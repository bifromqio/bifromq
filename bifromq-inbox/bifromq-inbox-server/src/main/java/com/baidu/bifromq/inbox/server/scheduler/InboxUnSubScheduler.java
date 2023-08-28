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
import static com.baidu.bifromq.inbox.util.KeyUtil.scopedTopicFilter;

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.inbox.rpc.proto.UnsubReply;
import com.baidu.bifromq.inbox.rpc.proto.UnsubRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchUnsubRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import com.google.protobuf.ByteString;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxUnSubScheduler extends InboxMutateScheduler<UnsubRequest, UnsubReply>
    implements IInboxUnsubScheduler {
    public InboxUnSubScheduler(IBaseKVStoreClient inboxStoreClient) {
        super(inboxStoreClient, "inbox_server_unsub");
    }

    @Override
    protected Batcher<UnsubRequest, UnsubReply, KVRangeSetting> newBatcher(String name,
                                                                           long tolerableLatencyNanos,
                                                                           long burstLatencyNanos,
                                                                           KVRangeSetting range) {
        return new InboxUnSubBatcher(name, tolerableLatencyNanos, burstLatencyNanos, range, inboxStoreClient);
    }

    @Override
    protected ByteString rangeKey(UnsubRequest request) {
        return scopedInboxId(request.getTenantId(), request.getInboxId());
    }

    private static class InboxUnSubBatcher extends InboxMutateBatcher<UnsubRequest, UnsubReply> {
        private class InboxBatchUnSub implements IBatchCall<UnsubRequest, UnsubReply> {
            private final Queue<CallTask<UnsubRequest, UnsubReply>> batchTasks = new ArrayDeque<>();
            private BatchUnsubRequest.Builder reqBuilder = BatchUnsubRequest.newBuilder();

            @Override
            public void add(CallTask<UnsubRequest, UnsubReply> task) {
                batchTasks.add(task);
                UnsubRequest request = task.call;
                reqBuilder.addTopicFilters(
                    scopedTopicFilter(request.getTenantId(), request.getInboxId(),
                        request.getTopicFilter()));
            }

            @Override
            public void reset() {
                reqBuilder = BatchUnsubRequest.newBuilder();
            }

            @Override
            public CompletableFuture<Void> execute() {
                long reqId = System.nanoTime();
                return mutate(InboxServiceRWCoProcInput.newBuilder()
                    .setReqId(reqId)
                    .setBatchUnsub(reqBuilder.setReqId(reqId).build())
                    .build())
                    .thenApply(InboxServiceRWCoProcOutput::getBatchUnsub)
                    .handle((v, e) -> {
                        if (e != null) {
                            CallTask<UnsubRequest, UnsubReply> task;
                            while ((task = batchTasks.poll()) != null) {
                                task.callResult.complete(UnsubReply.newBuilder()
                                    .setReqId(task.call.getReqId())
                                    .setResult(UnsubReply.Result.ERROR)
                                    .build());
                            }
                        } else {
                            CallTask<UnsubRequest, UnsubReply> task;
                            while ((task = batchTasks.poll()) != null) {
                                task.callResult.complete(UnsubReply.newBuilder()
                                    .setReqId(task.call.getReqId())
                                    .setResult(UnsubReply.Result.forNumber(v
                                        .getResultsMap()
                                        .get(scopedTopicFilter(task.call.getTenantId(),
                                            task.call.getInboxId(), task.call.getTopicFilter()).toStringUtf8())
                                        .getNumber()))
                                    .build());
                            }
                        }
                        return null;
                    });
            }
        }

        InboxUnSubBatcher(String name,
                          long tolerableLatencyNanos,
                          long burstLatencyNanos,
                          KVRangeSetting range,
                          IBaseKVStoreClient inboxStoreClient) {
            super(name, tolerableLatencyNanos, burstLatencyNanos, range, inboxStoreClient);
        }

        @Override
        protected IBatchCall<UnsubRequest, UnsubReply> newBatch() {
            return new InboxBatchUnSub();
        }
    }
}
