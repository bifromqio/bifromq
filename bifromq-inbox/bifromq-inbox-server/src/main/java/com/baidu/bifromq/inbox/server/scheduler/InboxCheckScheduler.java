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

package com.baidu.bifromq.inbox.server.scheduler;

import static com.baidu.bifromq.inbox.util.KeyUtil.scopedInboxId;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.INBOX_CHECK_QUEUES_PER_RANGE;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.inbox.rpc.proto.HasInboxReply;
import com.baidu.bifromq.inbox.rpc.proto.HasInboxRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchCheckRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcOutput;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxCheckScheduler extends InboxReadScheduler<HasInboxRequest, HasInboxReply>
    implements IInboxCheckScheduler {
    public InboxCheckScheduler(IBaseKVStoreClient inboxStoreClient) {
        super(INBOX_CHECK_QUEUES_PER_RANGE.get(), inboxStoreClient, "inbox_server_check");
    }

    @Override
    protected Batcher<HasInboxRequest, HasInboxReply, InboxReadBatcherKey> newBatcher(String name,
                                                                                      long tolerableLatencyNanos,
                                                                                      long burstLatencyNanos,
                                                                                      InboxReadBatcherKey batcherKey) {
        return new InboxCheckBatcher(batcherKey, name, tolerableLatencyNanos, burstLatencyNanos, inboxStoreClient);
    }

    @Override
    protected int selectQueue(int queueNum, HasInboxRequest request) {
        return ThreadLocalRandom.current().nextInt(0, queueNum);
    }

    @Override
    protected ByteString rangeKey(HasInboxRequest request) {
        return scopedInboxId(request.getTenantId(), request.getInboxId());
    }

    private static class InboxCheckBatcher extends Batcher<HasInboxRequest, HasInboxReply, InboxReadBatcherKey> {
        private class BatchInboxCheck implements IBatchCall<HasInboxRequest, HasInboxReply> {
            private Set<ByteString> checkInboxes = new HashSet<>();
            private Map<HasInboxRequest, CompletableFuture<HasInboxReply>> onInboxChecked = new HashMap<>();

            @Override
            public void reset() {
                checkInboxes = new HashSet<>();
                onInboxChecked = new HashMap<>();
            }

            @Override
            public void add(CallTask<HasInboxRequest, HasInboxReply> callTask) {
                checkInboxes.add(
                    scopedInboxId(callTask.call.getTenantId(), callTask.call.getInboxId()));
                onInboxChecked.put(callTask.call, callTask.callResult);
            }

            @Override
            public CompletableFuture<Void> execute() {
                long reqId = System.nanoTime();
                return inboxStoreClient.linearizedQuery(batcherKey.storeId,
                        KVRangeRORequest.newBuilder()
                            .setReqId(reqId)
                            .setVer(batcherKey.ver)
                            .setKvRangeId(batcherKey.id)
                            .setRoCoProcInput(InboxServiceROCoProcInput.newBuilder()
                                .setReqId(reqId)
                                .setBatchCheck(BatchCheckRequest.newBuilder()
                                    .addAllScopedInboxId(checkInboxes)
                                    .build())
                                .build()
                                .toByteString())
                            .build(), orderKey)
                    .thenApply(v -> {
                        switch (v.getCode()) {
                            case Ok:
                                try {
                                    InboxServiceROCoProcOutput reply = InboxServiceROCoProcOutput.parseFrom(
                                        v.getRoCoProcResult());
                                    assert reply.getReqId() == reqId;
                                    return reply.getBatchCheck();
                                } catch (InvalidProtocolBufferException e) {
                                    log.error("Unable to parse rw co-proc output", e);
                                    throw new RuntimeException("Unable to parse rw co-proc output", e);
                                }
                            default:
                                log.warn("Failed to exec rw co-proc[code={}]", v.getCode());
                                throw new RuntimeException("Failed to exec rw co-proc");
                        }
                    })
                    .handle((v, e) -> {
                        if (e != null) {
                            log.error("Inbox Check failed", e);
                            onInboxChecked.forEach((req, f) -> f.completeExceptionally(e));
                        } else {
                            onInboxChecked.forEach((req, f) -> {
                                Boolean exists = v.getExistsMap()
                                    .get(scopedInboxId(req.getTenantId(),
                                        req.getInboxId()).toStringUtf8());
                                // if query result doesn't contain the scoped inboxId, reply error
                                if (exists == null) {
                                    f.completeExceptionally(new RuntimeException("Inbox not found"));
                                } else {
                                    f.complete(HasInboxReply.newBuilder()
                                        .setReqId(req.getReqId())
                                        .setResult(exists ? HasInboxReply.Result.EXIST : HasInboxReply.Result.NO_INBOX)
                                        .build());
                                }
                            });
                        }
                        return null;
                    });
            }
        }

        private final String orderKey;
        private final IBaseKVStoreClient inboxStoreClient;

        InboxCheckBatcher(InboxReadBatcherKey batcherKey,
                          String name,
                          long tolerableLatencyNanos,
                          long burstLatencyNanos,
                          IBaseKVStoreClient inboxStoreClient) {
            super(batcherKey, name, tolerableLatencyNanos, burstLatencyNanos);
            this.inboxStoreClient = inboxStoreClient;
            orderKey = this.hashCode() + "";
        }

        @Override
        protected IBatchCall<HasInboxRequest, HasInboxReply> newBatch() {
            return new BatchInboxCheck();
        }
    }
}
