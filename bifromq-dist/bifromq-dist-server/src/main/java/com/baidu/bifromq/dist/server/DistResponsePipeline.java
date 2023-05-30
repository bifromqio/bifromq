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

package com.baidu.bifromq.dist.server;

import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DIST_WORKER_CALL_QUEUES;

import com.baidu.bifromq.baserpc.ResponsePipeline;
import com.baidu.bifromq.basescheduler.IBatchCallScheduler;
import com.baidu.bifromq.basescheduler.exception.DropException;
import com.baidu.bifromq.dist.rpc.proto.DistReply;
import com.baidu.bifromq.dist.rpc.proto.DistRequest;
import com.baidu.bifromq.dist.server.scheduler.DistCall;
import com.baidu.bifromq.plugin.eventcollector.EventType;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.eventcollector.distservice.DistError;
import com.baidu.bifromq.plugin.eventcollector.distservice.Disted;
import com.baidu.bifromq.plugin.eventcollector.distservice.DropExceedLimit;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class DistResponsePipeline extends ResponsePipeline<DistRequest, DistReply> {
    private final IEventCollector eventCollector;
    private final IBatchCallScheduler<DistCall, Map<String, Integer>> distCallScheduler;
    private final LoadingCache<String, RunningAverage> trafficFanouts;
    private final Integer callQueueIdx;

    DistResponsePipeline(IBatchCallScheduler<DistCall, Map<String, Integer>> distCallScheduler,
                         StreamObserver<DistReply> responseObserver,
                         IEventCollector eventCollector,
                         LoadingCache<String, RunningAverage> trafficFanouts) {
        super(responseObserver);
        this.distCallScheduler = distCallScheduler;
        this.eventCollector = eventCollector;
        this.trafficFanouts = trafficFanouts;
        this.callQueueIdx = DistQueueAllocator.allocate();
    }

    @Override
    protected CompletableFuture<DistReply> handleRequest(String trafficId, DistRequest request) {
        return distCallScheduler.schedule(new DistCall(trafficId, request.getMessagesList(),
                callQueueIdx, trafficFanouts.get(trafficId).estimate()))
            .handle((v, e) -> {
                if (e != null) {
                    if (e.getCause() == DropException.EXCEED_LIMIT) {
                        eventCollector.report(getLocal(EventType.DROP_EXCEED_LIMIT, DropExceedLimit.class)
                            .trafficId(trafficId));
                    } else {
                        log.error("Failed to exec DistRequest, trafficId={}, req={}", trafficId, request, e);
                        eventCollector.report(getLocal(EventType.DIST_ERROR, DistError.class)
                            .reqId(request.getReqId())
                            .messages(request.getMessagesList()));
                    }
                    return DistReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setResult(DistReply.Result.ERROR)
                        .build();
                } else {
                    trafficFanouts.get(trafficId).log(v.values().stream().reduce(0, Integer::sum) / v.size());
                    eventCollector.report(getLocal(EventType.DISTED, Disted.class)
                        .reqId(request.getReqId())
                        .messages(request.getMessagesList())
                        .fanout(v.values().stream().reduce(0, Integer::sum)));
                    return DistReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setResult(DistReply.Result.SUCCEED)
                        .build();
                }
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
