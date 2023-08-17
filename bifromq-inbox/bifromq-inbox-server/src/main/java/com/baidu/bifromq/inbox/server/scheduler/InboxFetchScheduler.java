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

import static com.baidu.bifromq.sysprops.BifroMQSysProp.INBOX_FETCH_QUEUES_PER_RANGE;

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.inbox.storage.proto.FetchParams;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import com.baidu.bifromq.inbox.storage.proto.InboxFetchRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcOutput;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxFetchScheduler extends InboxReadScheduler<IInboxFetchScheduler.InboxFetch, Fetched>
    implements IInboxFetchScheduler {
    public InboxFetchScheduler(IBaseKVStoreClient inboxStoreClient) {
        super(INBOX_FETCH_QUEUES_PER_RANGE.get(), inboxStoreClient, "inbox_server_fetch");
    }

    @Override
    protected Batcher<IInboxFetchScheduler.InboxFetch, Fetched, InboxReadBatcherKey> newBatcher(String name,
                                                                                                long tolerableLatencyNanos,
                                                                                                long burstLatencyNanos,
                                                                                                InboxReadBatcherKey inboxReadBatcherKey) {
        return new InboxFetchBatcher(inboxReadBatcherKey, name, tolerableLatencyNanos, burstLatencyNanos,
            inboxStoreClient);
    }

    @Override
    protected int selectQueue(int queueNum, IInboxFetchScheduler.InboxFetch request) {
        int idx = request.scopedInboxId.hashCode() % queueNum;
        if (idx < 0) {
            idx += queueNum;
        }
        return idx;
    }

    @Override
    protected ByteString rangeKey(IInboxFetchScheduler.InboxFetch request) {
        return request.scopedInboxId;
    }

    private static class InboxFetchBatcher
        extends Batcher<IInboxFetchScheduler.InboxFetch, Fetched, InboxReadBatcherKey> {
        private class InboxFetchBatch implements IBatchCall<InboxFetch, Fetched> {
            // key: scopedInboxIdUtf8
            private Map<String, FetchParams> inboxFetches = new HashMap<>(128);

            // key: scopedInboxIdUtf8
            private Map<String, CompletableFuture<Fetched>> onInboxFetched = new HashMap<>(128);

            @Override
            public void reset() {
                inboxFetches = new HashMap<>(128);
                onInboxFetched = new HashMap<>(128);
            }

            @Override
            public void add(CallTask<InboxFetch, Fetched> callTask) {
                InboxFetch request = callTask.call;
                inboxFetches.compute(request.scopedInboxId.toStringUtf8(), (k, v) -> {
                    if (v == null) {
                        return request.params;
                    }
                    FetchParams.Builder b = v.toBuilder();
                    if (request.params.hasQos0StartAfter()) {
                        b.setQos0StartAfter(request.params.getQos1StartAfter());
                    }
                    if (request.params.hasQos1StartAfter()) {
                        b.setQos1StartAfter(request.params.getQos1StartAfter());
                    }
                    if (request.params.hasQos2StartAfter()) {
                        b.setQos2StartAfter(request.params.getQos2StartAfter());
                    }
                    b.setMaxFetch(request.params.getMaxFetch());
                    return b.build();
                });
                onInboxFetched.put(request.scopedInboxId.toStringUtf8(), callTask.callResult);
            }

            @Override
            public CompletableFuture<Void> execute() {
                long reqId = System.nanoTime();
                return inboxStoreClient.linearizedQuery(rangeStoreId,
                        KVRangeRORequest.newBuilder()
                            .setReqId(reqId)
                            .setVer(range.ver)
                            .setKvRangeId(range.id)
                            .setRoCoProcInput(InboxServiceROCoProcInput.newBuilder()
                                .setReqId(reqId)
                                .setFetch(InboxFetchRequest.newBuilder()
                                    .putAllInboxFetch(inboxFetches)
                                    .build())
                                .build()
                                .toByteString())
                            .build(), orderKey)
                    .thenApply(v -> {
                        switch (v.getCode()) {
                            case Ok:
                                try {
                                    InboxServiceROCoProcOutput output = InboxServiceROCoProcOutput.parseFrom(
                                        v.getRoCoProcResult());
                                    assert output.getReqId() == reqId;
                                    return output.getFetch();
                                } catch (InvalidProtocolBufferException e) {
                                    log.error("Unable to parse rw co-proc output", e);
                                    throw new RuntimeException("Unable to parse rw co-proc output", e);
                                }
                            default:
                                throw new RuntimeException(
                                    String.format("Failed to exec ro co-proc[code=%s]", v.getCode()));
                        }
                    })
                    .handle((v, e) -> {
                        if (e != null) {
                            for (String scopedInboxIdUtf8 : onInboxFetched.keySet()) {
                                onInboxFetched.get(scopedInboxIdUtf8).completeExceptionally(e);
                            }
                        } else {
                            for (String scopedInboxIdUtf8 : onInboxFetched.keySet()) {
                                onInboxFetched.get(scopedInboxIdUtf8)
                                    .complete(v.getResultMap().get(scopedInboxIdUtf8));
                            }
                        }
                        return null;
                    });
            }
        }

        private final String rangeStoreId;
        private final String orderKey;
        private final KVRangeSetting range;
        private final IBaseKVStoreClient inboxStoreClient;

        InboxFetchBatcher(InboxReadBatcherKey batcherKey,
                          String name,
                          long tolerableLatencyNanos,
                          long burstLatencyNanos,
                          IBaseKVStoreClient inboxStoreClient) {
            super(batcherKey, name, tolerableLatencyNanos, burstLatencyNanos);
            this.range = batcherKey.range;
            this.inboxStoreClient = inboxStoreClient;
            rangeStoreId = selectStore(range);
            orderKey = String.valueOf(this.hashCode());
        }

        @Override
        protected IBatchCall<InboxFetch, Fetched> newBatch() {
            return new InboxFetchBatch();
        }

        private String selectStore(KVRangeSetting setting) {
            return setting.allReplicas.get(ThreadLocalRandom.current().nextInt(setting.allReplicas.size()));
        }
    }
}
