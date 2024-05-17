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

package com.baidu.bifromq.dist.server;

import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static com.baidu.bifromq.plugin.eventcollector.distservice.DistError.DistErrorCode.DROP_EXCEED_LIMIT;
import static com.baidu.bifromq.plugin.eventcollector.distservice.DistError.DistErrorCode.RPC_FAILURE;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DIST_WORKER_CALL_QUEUES;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.INGRESS_SLOWDOWN_DIRECT_MEMORY_USAGE;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.INGRESS_SLOWDOWN_HEAP_MEMORY_USAGE;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.MAX_SLOWDOWN_TIMEOUT_SECONDS;

import com.baidu.bifromq.baserpc.ResponsePipeline;
import com.baidu.bifromq.baserpc.utils.MemInfo;
import com.baidu.bifromq.basescheduler.exception.BackPressureException;
import com.baidu.bifromq.dist.rpc.proto.DistReply;
import com.baidu.bifromq.dist.rpc.proto.DistRequest;
import com.baidu.bifromq.dist.server.scheduler.DistWorkerCall;
import com.baidu.bifromq.dist.server.scheduler.IDistCallScheduler;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.eventcollector.distservice.DistError;
import com.baidu.bifromq.plugin.eventcollector.distservice.Disted;
import com.baidu.bifromq.type.PublisherMessagePack;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class DistResponsePipeline extends ResponsePipeline<DistRequest, DistReply> {
    private static final double SLOWDOWN_DIRECT_MEM_USAGE = INGRESS_SLOWDOWN_DIRECT_MEMORY_USAGE.get();
    private static final double SLOWDOWN_HEAP_MEM_USAGE = INGRESS_SLOWDOWN_HEAP_MEMORY_USAGE.get();
    private static final Duration SLOWDOWN_TIMEOUT =
        Duration.ofSeconds(((Integer) MAX_SLOWDOWN_TIMEOUT_SECONDS.get()).longValue());
    private final IEventCollector eventCollector;
    private final IDistCallScheduler distCallScheduler;
    private final LoadingCache<String, RunningAverage> tenantFanouts;
    private final Integer callQueueIdx;

    DistResponsePipeline(IDistCallScheduler distCallScheduler,
                         StreamObserver<DistReply> responseObserver,
                         IEventCollector eventCollector,
                         LoadingCache<String, RunningAverage> tenantFanouts) {
        super(responseObserver, () -> MemInfo.directMemoryUsage() > SLOWDOWN_DIRECT_MEM_USAGE
            || MemInfo.heapMemoryUsage() > SLOWDOWN_HEAP_MEM_USAGE, SLOWDOWN_TIMEOUT);
        this.distCallScheduler = distCallScheduler;
        this.eventCollector = eventCollector;
        this.tenantFanouts = tenantFanouts;
        this.callQueueIdx = DistQueueAllocator.allocate();
    }

    @Override
    protected CompletableFuture<DistReply> handleRequest(String tenantId, DistRequest request) {
        return distCallScheduler.schedule(new DistWorkerCall(tenantId, request.getMessagesList(),
                callQueueIdx, tenantFanouts.get(tenantId).estimate()))
            .handle((v, e) -> {
                DistReply.Builder replyBuilder = DistReply.newBuilder().setReqId(request.getReqId());
                if (e != null) {
                    if (e instanceof BackPressureException || e.getCause() instanceof BackPressureException) {
                        for (PublisherMessagePack publisherMsgPack : request.getMessagesList()) {
                            DistReply.Result.Builder resultBuilder = DistReply.Result.newBuilder();
                            for (PublisherMessagePack.TopicPack topicPack : publisherMsgPack.getMessagePackList()) {
                                resultBuilder.putTopic(topicPack.getTopic(), DistReply.Code.BACK_PRESSURE_REJECTED);
                            }
                            replyBuilder.addResults(resultBuilder.build());
                        }
                        eventCollector.report(getLocal(DistError.class)
                            .reqId(request.getReqId())
                            .messages(request.getMessagesList())
                            .code(DROP_EXCEED_LIMIT));
                    } else {
                        for (PublisherMessagePack publisherMsgPack : request.getMessagesList()) {
                            DistReply.Result.Builder resultBuilder = DistReply.Result.newBuilder();
                            for (PublisherMessagePack.TopicPack topicPack : publisherMsgPack.getMessagePackList()) {
                                resultBuilder.putTopic(topicPack.getTopic(), DistReply.Code.ERROR);
                            }
                            replyBuilder.addResults(resultBuilder.build());
                        }
                        eventCollector.report(getLocal(DistError.class)
                            .reqId(request.getReqId())
                            .messages(request.getMessagesList())
                            .code(RPC_FAILURE));
                    }
                } else {
                    tenantFanouts.get(tenantId).log(v.values().stream().reduce(0, Integer::sum) / v.size());
                    for (PublisherMessagePack publisherMsgPack : request.getMessagesList()) {
                        DistReply.Result.Builder resultBuilder = DistReply.Result.newBuilder();
                        for (PublisherMessagePack.TopicPack topicPack : publisherMsgPack.getMessagePackList()) {
                            int fanout = v.get(topicPack.getTopic());
                            resultBuilder.putTopic(topicPack.getTopic(),
                                fanout > 0 ? DistReply.Code.OK : DistReply.Code.NO_MATCH);
                        }
                        replyBuilder.addResults(resultBuilder.build());
                    }
                    eventCollector.report(getLocal(Disted.class)
                        .reqId(request.getReqId())
                        .messages(request.getMessagesList())
                        .fanout(v.values().stream().reduce(0, Integer::sum)));
                }
                return replyBuilder.build();
            });
    }

    private static class DistQueueAllocator {
        private static final int QUEUE_NUMS = DIST_WORKER_CALL_QUEUES.get();
        private static final AtomicInteger IDX = new AtomicInteger(0);

        public static int allocate() {
            return IDX.getAndIncrement() % QUEUE_NUMS;
        }
    }
}
