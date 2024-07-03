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

package com.baidu.bifromq.mqtt.service;

import static com.baidu.bifromq.sysprops.BifroMQSysProp.INGRESS_SLOWDOWN_DIRECT_MEMORY_USAGE;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.INGRESS_SLOWDOWN_HEAP_MEMORY_USAGE;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.MAX_SLOWDOWN_TIMEOUT_SECONDS;

import com.baidu.bifromq.baseenv.MemUsage;
import com.baidu.bifromq.baserpc.ResponsePipeline;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.WriteReply;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.WriteRequest;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class LocalSessionWritePipeline extends ResponsePipeline<WriteRequest, WriteReply> {
    private static final double SLOWDOWN_DIRECT_MEM_USAGE = INGRESS_SLOWDOWN_DIRECT_MEMORY_USAGE.get();
    private static final double SLOWDOWN_HEAP_MEM_USAGE = INGRESS_SLOWDOWN_HEAP_MEMORY_USAGE.get();
    private static final Duration SLOWDOWN_TIMEOUT =
        Duration.ofSeconds(((Integer) MAX_SLOWDOWN_TIMEOUT_SECONDS.get()).longValue());
    private final ILocalDistService localDistService;

    public LocalSessionWritePipeline(ILocalDistService localDistService, StreamObserver<WriteReply> responseObserver) {
        super(responseObserver, () -> MemUsage.local().nettyDirectMemoryUsage() > SLOWDOWN_DIRECT_MEM_USAGE
            || MemUsage.local().heapMemoryUsage() > SLOWDOWN_HEAP_MEM_USAGE, SLOWDOWN_TIMEOUT);
        this.localDistService = localDistService;
    }

    @Override
    protected CompletableFuture<WriteReply> handleRequest(String tenantId, WriteRequest request) {
        log.trace("Handle inbox write request: \n{}", request);
        return localDistService.dist(request.getRequest())
            .thenApply(reply -> WriteReply.newBuilder().setReqId(request.getReqId()).setReply(reply).build());
    }
}
