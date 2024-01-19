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

import static com.baidu.bifromq.inbox.util.MessageUtil.buildGCRequest;

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.rpc.proto.DetachReply;
import com.baidu.bifromq.inbox.rpc.proto.DetachRequest;
import com.baidu.bifromq.inbox.storage.proto.GCReply;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxGCProcessor implements IInboxGCProcessor {
    private final IInboxClient inboxClient;

    protected final IBaseKVStoreClient storeClient;

    public InboxGCProcessor(IInboxClient inboxClient, IBaseKVStoreClient storeClient) {
        this.inboxClient = inboxClient;
        this.storeClient = storeClient;
    }

    @Override
    public final CompletableFuture<Result> gcRange(KVRangeId rangeId,
                                                   @Nullable String tenantId,
                                                   @Nullable Integer expirySeconds,
                                                   long now,
                                                   int limit) {
        CompletableFuture<Result> onDone = new CompletableFuture<>();
        AtomicInteger count = new AtomicInteger();
        doGC(rangeId, tenantId, expirySeconds, null, now, limit, onDone, count);
        onDone.whenComplete((v, e) -> log.debug("[InboxGC] tenant[{}] gc {} inboxes from range[{}]",
            tenantId, count.get(), KVRangeIdUtil.toString(rangeId)));
        return onDone;
    }

    private void doGC(KVRangeId rangeId,
                      @Nullable String tenantId,
                      @Nullable Integer expirySeconds,
                      @Nullable ByteString cursor,
                      long now,
                      int limit,
                      CompletableFuture<Result> onDone,
                      AtomicInteger count) {
        scan(rangeId, tenantId, expirySeconds, cursor, now, limit)
            .thenCompose(gcReply -> {
                if (gcReply.getCode() == GCReply.Code.ERROR) {
                    return CompletableFuture.completedFuture(gcReply);
                }
                List<GCReply.Inbox> inboxList = gcReply.getInboxList();
                count.addAndGet(inboxList.size());
                log.debug("[InboxGC] scan success: rangeId={}, size={}", KVRangeIdUtil.toString(rangeId),
                    inboxList.size());
                return CompletableFuture.allOf(inboxList.stream()
                        .map(inbox -> inboxClient.detach(DetachRequest.newBuilder()
                                .setReqId(System.nanoTime())
                                .setInboxId(inbox.getInboxId())
                                .setIncarnation(inbox.getIncarnation())
                                .setVersion(inbox.getVersion())
                                .setExpirySeconds(0)
                                .setDiscardLWT(false)
                                .setClient(inbox.getClient())
                                .setNow(now)
                                .build())
                            .thenAccept(v -> {
                                if (v.getCode() != DetachReply.Code.OK && v.getCode() != DetachReply.Code.NO_INBOX) {
                                    log.error("Failed to clean expired inbox: tenantId={}, inboxId={}",
                                        tenantId, inbox.getInboxId());
                                } else {
                                    log.debug("[InboxGC] clean success: tenantId={}, inboxId={}",
                                        tenantId, inbox.getInboxId());
                                }
                            }))
                        .toArray(CompletableFuture[]::new))
                    .thenApply(v -> gcReply);
            })
            .whenComplete((gcReply, e) -> {
                if (e != null || gcReply.getCode() == GCReply.Code.ERROR) {
                    log.debug("Failed to gc inboxes");
                    onDone.complete(Result.ERROR);
                } else {
                    if (gcReply.hasCursor()) {
                        doGC(rangeId, tenantId, expirySeconds, gcReply.getCursor(), now, limit, onDone, count);
                    } else {
                        onDone.complete(Result.OK);
                    }
                }
            });
    }

    private CompletableFuture<GCReply> scan(KVRangeId rangeId,
                                            @Nullable String tenantId,
                                            @Nullable Integer expirySeconds,
                                            @Nullable ByteString cursor,
                                            long now,
                                            int limit) {
        Optional<KVRangeSetting> settingOptional = storeClient.findById(rangeId);
        if (settingOptional.isEmpty()) {
            return CompletableFuture.completedFuture(GCReply.newBuilder().setCode(GCReply.Code.ERROR).build());
        }
        KVRangeSetting replica = settingOptional.get();
        long reqId = System.nanoTime();
        return storeClient.query(replica.leader, KVRangeRORequest.newBuilder()
                .setReqId(reqId)
                .setKvRangeId(replica.id)
                .setVer(replica.ver)
                .setRoCoProc(ROCoProcInput.newBuilder()
                    .setInboxService(buildGCRequest(reqId, tenantId, expirySeconds, cursor, now, limit))
                    .build())
                .build())
            .thenApply(v -> {
                if (v.getCode() == ReplyCode.Ok) {
                    return v.getRoCoProcResult().getInboxService().getGc();
                }

                throw new RuntimeException("BaseKV Query failed:" + v.getCode().name());
            })
            .exceptionally(e -> {
                log.error("[InboxGC] scan failed: tenantId={}, rangeId={}", tenantId, KVRangeIdUtil.toString(rangeId),
                    e);
                return GCReply.newBuilder().setCode(GCReply.Code.ERROR).build();
            });
    }
}
