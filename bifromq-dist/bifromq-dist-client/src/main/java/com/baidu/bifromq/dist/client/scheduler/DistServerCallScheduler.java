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

package com.baidu.bifromq.dist.client.scheduler;

import static java.util.Collections.emptyMap;

import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.basescheduler.BatchCallScheduler;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.basescheduler.ICallTask;
import com.baidu.bifromq.dist.client.DistResult;
import com.baidu.bifromq.dist.rpc.proto.DistReply;
import com.baidu.bifromq.dist.rpc.proto.DistRequest;
import com.baidu.bifromq.dist.rpc.proto.DistServiceGrpc;
import com.baidu.bifromq.sysprops.props.DataPlaneBurstLatencyMillis;
import com.baidu.bifromq.sysprops.props.DataPlaneTolerableLatencyMillis;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.PublisherMessagePack;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DistServerCallScheduler extends BatchCallScheduler<DistServerCall, DistResult, BatcherKey>
    implements IDistServerCallScheduler {
    private final IRPCClient rpcClient;

    public DistServerCallScheduler(IRPCClient rpcClient) {
        super("dist_client_send_batcher", Duration.ofMillis(DataPlaneTolerableLatencyMillis.INSTANCE.get()),
            Duration.ofMillis(DataPlaneBurstLatencyMillis.INSTANCE.get()));
        this.rpcClient = rpcClient;
    }

    @Override
    protected Batcher<DistServerCall, DistResult, BatcherKey> newBatcher(String name,
                                                                         long tolerableLatencyNanos,
                                                                         long burstLatencyNanos,
                                                                         BatcherKey batcherKey) {
        return new DistServerCallBatcher(batcherKey, name, tolerableLatencyNanos, burstLatencyNanos,
            rpcClient.createRequestPipeline(batcherKey.tenantId(), null, null, emptyMap(),
                DistServiceGrpc.getDistMethod()));
    }

    @Override
    protected Optional<BatcherKey> find(DistServerCall clientCall) {
        return Optional.of(new BatcherKey(clientCall.publisher.getTenantId(), Thread.currentThread().getId()));
    }

    private static class DistServerCallBatcher extends Batcher<DistServerCall, DistResult, BatcherKey> {
        private final IRPCClient.IRequestPipeline<DistRequest, DistReply> ppln;

        private class DistServerBatchCall implements IBatchCall<DistServerCall, DistResult, BatcherKey> {
            private final Queue<ICallTask<DistServerCall, DistResult, BatcherKey>> tasks = new ArrayDeque<>(64);
            private Map<ClientInfo, Map<String, PublisherMessagePack.TopicPack.Builder>> clientMsgPack =
                new HashMap<>(128);

            @Override
            public void reset() {
                clientMsgPack = new HashMap<>(128);
            }

            @Override
            public void add(ICallTask<DistServerCall, DistResult, BatcherKey> callTask) {
                tasks.add(callTask);
                clientMsgPack.computeIfAbsent(callTask.call().publisher, k -> new HashMap<>())
                    .computeIfAbsent(callTask.call().topic,
                        k -> PublisherMessagePack.TopicPack.newBuilder().setTopic(k))
                    .addMessage(callTask.call().message);
            }

            @Override
            public CompletableFuture<Void> execute() {
                DistRequest.Builder requestBuilder = DistRequest.newBuilder().setReqId(System.nanoTime());
                clientMsgPack.forEach((k, v) -> {
                    PublisherMessagePack.Builder senderMsgPackBuilder =
                        PublisherMessagePack.newBuilder().setPublisher(k);
                    for (PublisherMessagePack.TopicPack.Builder packBuilder : v.values()) {
                        senderMsgPackBuilder.addMessagePack(packBuilder);
                    }
                    requestBuilder.addMessages(senderMsgPackBuilder.build());
                });
                DistRequest request = requestBuilder.build();
                return ppln.invoke(request).handle((v, e) -> {
                    if (e != null) {
                        ICallTask<DistServerCall, DistResult, BatcherKey> task;
                        while ((task = tasks.poll()) != null) {
                            task.resultPromise().completeExceptionally(e);
                        }
                    } else {
                        Map<ClientInfo, Map<String, DistReply.Code>> resultMap = new HashMap<>();
                        for (int i = 0; i < request.getMessagesCount(); i++) {
                            PublisherMessagePack pubMsgPack = request.getMessages(i);
                            DistReply.Result result = v.getResults(i);
                            resultMap.put(pubMsgPack.getPublisher(), result.getTopicMap());
                        }
                        ICallTask<DistServerCall, DistResult, BatcherKey> task;
                        while ((task = tasks.poll()) != null) {
                            ClientInfo publisher = task.call().publisher;
                            String topic = task.call().topic;
                            switch (resultMap.get(publisher).get(topic)) {
                                case OK -> task.resultPromise().complete(DistResult.OK);
                                case NO_MATCH -> task.resultPromise().complete(DistResult.NO_MATCH);
                                case BACK_PRESSURE_REJECTED ->
                                    task.resultPromise().complete(DistResult.BACK_PRESSURE_REJECTED);
                                case ERROR -> task.resultPromise().complete(DistResult.ERROR);
                            }
                        }
                    }
                    return null;
                });
            }
        }

        DistServerCallBatcher(BatcherKey batcherKey, String name,
                              long tolerableLatencyNanos,
                              long burstLatencyNanos,
                              IRPCClient.IRequestPipeline<DistRequest, DistReply> ppln) {
            super(batcherKey, name, tolerableLatencyNanos, burstLatencyNanos);
            this.ppln = ppln;
        }

        @Override
        protected IBatchCall<DistServerCall, DistResult, BatcherKey> newBatch() {
            return new DistServerBatchCall();
        }

        @Override
        public void close() {
            super.close();
            ppln.close();
        }
    }
}
