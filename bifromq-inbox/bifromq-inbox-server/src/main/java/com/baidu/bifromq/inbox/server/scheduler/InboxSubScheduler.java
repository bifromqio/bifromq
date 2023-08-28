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
import com.baidu.bifromq.inbox.rpc.proto.SubReply;
import com.baidu.bifromq.inbox.rpc.proto.SubRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchSubRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import com.google.protobuf.ByteString;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxSubScheduler extends InboxMutateScheduler<SubRequest, SubReply> implements IInboxSubScheduler {
    public InboxSubScheduler(IBaseKVStoreClient inboxStoreClient) {
        super(inboxStoreClient, "inbox_server_sub");
    }

    @Override
    protected Batcher<SubRequest, SubReply, KVRangeSetting> newBatcher(String name,
                                                                       long tolerableLatencyNanos,
                                                                       long burstLatencyNanos,
                                                                       KVRangeSetting range) {
        return new InboxSubBatcher(name, tolerableLatencyNanos, burstLatencyNanos, range, inboxStoreClient);
    }

    @Override
    protected ByteString rangeKey(SubRequest request) {
        return scopedInboxId(request.getTenantId(), request.getInboxId());
    }

    private static class InboxSubBatcher extends InboxMutateBatcher<SubRequest, SubReply> {
        private class InboxBatchSub implements IBatchCall<SubRequest, SubReply> {
            private final Queue<CallTask<SubRequest, SubReply>> batchTasks = new ArrayDeque<>();
            private BatchSubRequest.Builder reqBuilder = BatchSubRequest.newBuilder();

            @Override
            public void add(CallTask<SubRequest, SubReply> task) {
                batchTasks.add(task);
                SubRequest request = task.call;
                reqBuilder.putTopicFilters(
                    scopedTopicFilter(request.getTenantId(), request.getInboxId(),
                        request.getTopicFilter()).toStringUtf8(), request.getSubQoS());
            }

            @Override
            public void reset() {
                reqBuilder = BatchSubRequest.newBuilder();
            }

            @Override
            public CompletableFuture<Void> execute() {
                long reqId = System.nanoTime();
                reqBuilder.setReqId(reqId);
                return mutate(InboxServiceRWCoProcInput.newBuilder()
                    .setReqId(reqId)
                    .setBatchSub(reqBuilder.build())
                    .build())
                    .thenApply(InboxServiceRWCoProcOutput::getBatchSub)
                    .handle((v, e) -> {
                        if (e != null) {
                            CallTask<SubRequest, SubReply> task;
                            while ((task = batchTasks.poll()) != null) {
                                task.callResult.complete(SubReply.newBuilder()
                                    .setReqId(task.call.getReqId())
                                    .setResult(SubReply.Result.ERROR)
                                    .build());
                            }
                        } else {
                            CallTask<SubRequest, SubReply> task;
                            while ((task = batchTasks.poll()) != null) {
                                task.callResult.complete(SubReply.newBuilder()
                                    .setReqId(task.call.getReqId())
                                    .setResult(SubReply.Result.forNumber(v
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

        InboxSubBatcher(String name,
                        long tolerableLatencyNanos,
                        long burstLatencyNanos,
                        KVRangeSetting range,
                        IBaseKVStoreClient inboxStoreClient) {
            super(name, tolerableLatencyNanos, burstLatencyNanos, range, inboxStoreClient);
        }

        @Override
        protected IBatchCall<SubRequest, SubReply> newBatch() {
            return new InboxBatchSub();
        }
    }
}
