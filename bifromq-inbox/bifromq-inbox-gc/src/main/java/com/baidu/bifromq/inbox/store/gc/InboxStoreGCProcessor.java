/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.baidu.bifromq.inbox.store.gc;

import static com.baidu.bifromq.basekv.client.KVRangeRouterUtil.findByBoundary;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static com.baidu.bifromq.inbox.store.schema.KVSchemaUtil.tenantBeginKeyPrefix;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.KVRangeSetting;
import com.baidu.bifromq.basekv.client.exception.BadRequestException;
import com.baidu.bifromq.basekv.client.exception.BadVersionException;
import com.baidu.bifromq.basekv.client.exception.InternalErrorException;
import com.baidu.bifromq.basekv.client.exception.TryLaterException;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.rpc.proto.DetachRequest;
import com.baidu.bifromq.inbox.storage.proto.GCReply;
import com.baidu.bifromq.inbox.storage.proto.GCRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;
import com.google.protobuf.ByteString;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxStoreGCProcessor implements IInboxStoreGCProcessor {
    protected final IBaseKVStoreClient storeClient;
    private final IInboxClient inboxClient;
    private final String localServerId;

    public InboxStoreGCProcessor(IInboxClient inboxClient, IBaseKVStoreClient storeClient) {
        this(inboxClient, storeClient, null);
    }

    public InboxStoreGCProcessor(IInboxClient inboxClient, IBaseKVStoreClient storeClient, String localStoreId) {
        this.inboxClient = inboxClient;
        this.storeClient = storeClient;
        this.localServerId = localStoreId;
    }

    @Override
    public final CompletableFuture<Result> gc(long reqId,
                                              @Nullable String tenantId,
                                              @Nullable Integer expirySeconds,
                                              long now) {
        Collection<KVRangeSetting> rangeSettingList;
        if (tenantId != null) {
            ByteString tenantBeginKey = tenantBeginKeyPrefix(tenantId);
            rangeSettingList = findByBoundary(toBoundary(tenantBeginKey, upperBound(tenantBeginKey)),
                storeClient.latestEffectiveRouter());
        } else {
            rangeSettingList = findByBoundary(FULL_BOUNDARY, storeClient.latestEffectiveRouter());
            if (localServerId != null) {
                rangeSettingList.removeIf(rangeSetting -> !rangeSetting.leader.equals(localServerId));
            }
        }
        if (rangeSettingList.isEmpty()) {
            return CompletableFuture.completedFuture(Result.OK);
        }
        CompletableFuture<?>[] gcResults = rangeSettingList.stream().map(
            setting -> doGC(reqId, setting, tenantId, expirySeconds, now)).toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(gcResults)
            .handle((v, e) -> {
                if (e != null) {
                    log.debug("[InboxGC] Failed to do gc: reqId={}", reqId, e);
                    return Result.ERROR;
                }
                return Result.OK;
            });
    }

    private CompletableFuture<Void> doGC(long reqId,
                                         KVRangeSetting rangeSetting,
                                         @Nullable String tenantId,
                                         @Nullable Integer expirySeconds,
                                         long now) {
        return scan(rangeSetting, tenantId, expirySeconds, now)
            .thenCompose(gcReply -> {
                List<GCReply.GCCandidate> inboxList = gcReply.getCandidateList();
                log.debug("[InboxGC] scan success: reqId={}, rangeId={}, size={}",
                    reqId, KVRangeIdUtil.toString(rangeSetting.id), inboxList.size());
                return CompletableFuture.allOf(inboxList.stream()
                    .map(inbox -> inboxClient.detach(DetachRequest.newBuilder()
                            .setReqId(System.nanoTime())
                            .setInboxId(inbox.getInboxId())
                            .setIncarnation(inbox.getIncarnation())
                            .setVersion(inbox.getVersion())
                            .setExpirySeconds(expirySeconds != null ? 0 : inbox.getExpirySeconds())
                            .setDiscardLWT(false)
                            .setClient(inbox.getClient())
                            .setNow(now)
                            .build())
                        .thenAccept(v -> {
                            switch (v.getCode()) {
                                case OK, NO_INBOX -> log.debug("[InboxGC] detach success: reqId={}, inboxId={}",
                                    reqId, inbox.getInboxId());
                                case TRY_LATER, CONFLICT ->
                                    log.debug("[InboxGC] detach inbox needs retry: reqId={}, inboxId={}, reason={}",
                                        reqId, inbox.getInboxId(), v.getCode());
                                default ->
                                    log.debug("[InboxGC] Failed to detach inbox: reqId={}, inboxId={}, reason={}",
                                        reqId, inbox.getInboxId(), v.getCode());
                            }
                        }))
                    .toArray(CompletableFuture[]::new));
            });
    }

    private CompletableFuture<GCReply> scan(KVRangeSetting rangeSetting,
                                            @Nullable String tenantId,
                                            @Nullable Integer expirySeconds,
                                            long now) {
        long reqId = System.nanoTime();
        return storeClient.query(rangeSetting.leader, KVRangeRORequest.newBuilder()
                .setReqId(reqId)
                .setKvRangeId(rangeSetting.id)
                .setVer(rangeSetting.ver)
                .setRoCoProc(ROCoProcInput.newBuilder()
                    .setInboxService(buildGCRequest(reqId, tenantId, expirySeconds, now))
                    .build())
                .build())
            .thenApply(v -> {
                switch (v.getCode()) {
                    case Ok -> {
                        return v.getRoCoProcResult().getInboxService().getGc();
                    }
                    case BadRequest -> throw new BadRequestException();
                    case BadVersion -> throw new BadVersionException();
                    case TryLater -> throw new TryLaterException();
                    default -> throw new InternalErrorException();
                }
            });
    }

    private InboxServiceROCoProcInput buildGCRequest(long reqId,
                                                     String tenantId,
                                                     Integer expirySeconds,
                                                     long now) {
        GCRequest.Builder reqBuilder = GCRequest.newBuilder().setNow(now);
        if (tenantId != null) {
            reqBuilder.setTenantId(tenantId);
        }
        if (expirySeconds != null) {
            reqBuilder.setExpirySeconds(expirySeconds);
        }
        return InboxServiceROCoProcInput.newBuilder()
            .setReqId(reqId)
            .setGc(reqBuilder.build())
            .build();
    }
}
