/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
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

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.inbox.storage.proto.GCReply;
import com.baidu.bifromq.inbox.util.KeyUtil;
import com.baidu.bifromq.inbox.util.MessageUtil;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class InboxGCProc {

    protected final IBaseKVStoreClient storeClient;

    private final Executor gcExecutor;


    public InboxGCProc(IBaseKVStoreClient storeClient,
                       Executor gcExecutor) {
        this.storeClient = storeClient;
        this.gcExecutor = gcExecutor;
    }

    /**
     * gc specific KVRange
     *
     * @param rangeId
     * @param tenantId      gc inboxes belong to the specific tenantId, gc all tenants if null
     * @param expirySeconds gc inboxes exceed the given expirySeconds
     * @param limit         max inboxes per gc
     * @return
     */
    public CompletableFuture<Void> gcRange(KVRangeId rangeId, String tenantId, Integer expirySeconds, int limit) {
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        AtomicInteger count = new AtomicInteger();
        gcExecutor.execute(() -> doGC(rangeId, tenantId, expirySeconds, null, limit, onDone, count));
        onDone.whenComplete((v, e) -> log.debug("[InboxGC] tenant[{}] gc {} inboxes from range[{}]",
            tenantId, count.get(), KVRangeIdUtil.toString(rangeId)));
        return onDone;
    }

    protected abstract CompletableFuture<Void> gcInbox(ByteString scopedInboxId);

    private void doGC(KVRangeId rangeId,
                      String tenantId,
                      Integer expirySeconds,
                      ByteString fromInboxId,
                      int limit,
                      CompletableFuture<Void> onDone,
                      AtomicInteger count) {
        scan(rangeId, tenantId, expirySeconds, fromInboxId, limit)
            .thenComposeAsync(gcReply -> {
                List<ByteString> scopedInboxIdList = gcReply.getScopedInboxIdList();
                count.addAndGet(scopedInboxIdList.size());
                log.debug("[InboxGC] scan succeed: rangeId={}, size={}", KVRangeIdUtil.toString(rangeId),
                    scopedInboxIdList.size());
                ByteString nextInboxId = gcReply.hasNextScopedInboxId() ? gcReply.getNextScopedInboxId() : null;
                if (scopedInboxIdList.isEmpty()) {
                    return CompletableFuture.completedFuture(nextInboxId);
                } else {
                    return CompletableFuture.allOf(scopedInboxIdList.stream()
                            .map(scopedInboxIdToGc -> gcInbox(scopedInboxIdToGc)
                                .handle((v, e) -> {
                                    String inboxId = KeyUtil.parseInboxId(scopedInboxIdToGc);
                                    if (e != null) {
                                        log.error("Failed to clean expired inbox: tenantId={}, inboxId={}", tenantId,
                                            inboxId, e);
                                    } else {
                                        log.debug("[InboxGC] clean success: tenantId={}, inboxId={}", tenantId, inboxId);
                                    }
                                    return null;
                                }))
                            .toArray(CompletableFuture[]::new))
                        .thenApply(v -> nextInboxId);
                }
            }, gcExecutor)
            .thenAcceptAsync(startFromScopedInboxId -> {
                if (startFromScopedInboxId != null) {
                    doGC(rangeId, tenantId, expirySeconds, startFromScopedInboxId, limit, onDone, count);
                } else {
                    onDone.complete(null);
                }
            }, gcExecutor);
    }

    private CompletableFuture<GCReply> scan(KVRangeId rangeId,
                                            String tenantId,
                                            Integer expirySeconds,
                                            ByteString fromInboxId,
                                            int limit) {
        Optional<KVRangeSetting> settingOptional = storeClient.findById(rangeId);
        if (settingOptional.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("KVRange not found:" + KVRangeIdUtil.toString(rangeId)));
        }
        KVRangeSetting replica = settingOptional.get();
        long reqId = System.nanoTime();
        return storeClient.query(replica.leader, KVRangeRORequest.newBuilder()
                .setReqId(reqId)
                .setKvRangeId(replica.id)
                .setVer(replica.ver)
                .setRoCoProc(ROCoProcInput.newBuilder()
                    .setInboxService(MessageUtil.buildGCRequest(reqId, fromInboxId, tenantId, expirySeconds, limit))
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
                return GCReply.newBuilder().setReqId(reqId).build();
            });
    }
}
