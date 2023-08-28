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

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.inbox.storage.proto.BatchTouchRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import com.baidu.bifromq.inbox.storage.proto.TopicFilterList;
import com.google.protobuf.ByteString;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxTouchScheduler extends InboxMutateScheduler<IInboxTouchScheduler.Touch, List<String>>
    implements IInboxTouchScheduler {
    public InboxTouchScheduler(IBaseKVStoreClient inboxStoreClient) {
        super(inboxStoreClient, "inbox_server_touch");
    }

    @Override
    protected Batcher<Touch, List<String>, KVRangeSetting> newBatcher(String name,
                                                                      long tolerableLatencyNanos,
                                                                      long burstLatencyNanos,
                                                                      KVRangeSetting range) {
        return new InboxTouchBatcher(name, tolerableLatencyNanos, burstLatencyNanos, range, inboxStoreClient);
    }

    @Override
    protected ByteString rangeKey(Touch request) {
        return ByteString.copyFromUtf8(request.scopedInboxIdUtf8);
    }

    private static class InboxTouchBatcher extends InboxMutateBatcher<Touch, List<String>> {
        private class InboxTouchBatch implements IBatchCall<Touch, List<String>> {
            private final Queue<CallTask<Touch, List<String>>> batchedTasks = new ArrayDeque<>();
            private BatchTouchRequest.Builder reqBuilder = BatchTouchRequest.newBuilder();

            @Override
            public void reset() {
                reqBuilder = BatchTouchRequest.newBuilder();
            }

            @Override
            public void add(CallTask<Touch, List<String>> callTask) {
                Touch request = callTask.call;
                batchedTasks.add(callTask);
                reqBuilder.putScopedInboxId(request.scopedInboxIdUtf8,
                    request.keep && reqBuilder.getScopedInboxIdOrDefault(request.scopedInboxIdUtf8, true));
            }

            @Override
            public CompletableFuture<Void> execute() {
                long reqId = System.nanoTime();
                return mutate(InboxServiceRWCoProcInput.newBuilder()
                    .setReqId(reqId)
                    .setBatchTouch(reqBuilder.build())
                    .build())
                    .thenApply(InboxServiceRWCoProcOutput::getBatchTouch)
                    .handle((v, e) -> {
                        if (e != null) {
                            CallTask<Touch, List<String>> callTask;
                            while ((callTask = batchedTasks.poll()) != null) {
                                callTask.callResult.completeExceptionally(e);
                            }
                        } else {
                            CallTask<Touch, List<String>> callTask;
                            while ((callTask = batchedTasks.poll()) != null) {
                                callTask.callResult.complete(v.getSubsOrDefault(callTask.call.scopedInboxIdUtf8,
                                    TopicFilterList.getDefaultInstance()).getTopicFiltersList());
                            }
                        }
                        return null;
                    });
            }
        }

        InboxTouchBatcher(String name,
                          long tolerableLatencyNanos,
                          long burstLatencyNanos,
                          KVRangeSetting range,
                          IBaseKVStoreClient inboxStoreClient) {
            super(name, tolerableLatencyNanos, burstLatencyNanos, range, inboxStoreClient);
        }

        @Override
        protected IBatchCall<Touch, List<String>> newBatch() {
            return new InboxTouchBatch();
        }
    }
}
