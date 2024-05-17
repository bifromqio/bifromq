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

package com.baidu.bifromq.inbox.server;

import static com.baidu.bifromq.sysprops.BifroMQSysProp.INGRESS_SLOWDOWN_DIRECT_MEMORY_USAGE;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.INGRESS_SLOWDOWN_HEAP_MEMORY_USAGE;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.MAX_SLOWDOWN_TIMEOUT_SECONDS;

import com.baidu.bifromq.baserpc.RPCContext;
import com.baidu.bifromq.baserpc.ResponsePipeline;
import com.baidu.bifromq.baserpc.utils.MemInfo;
import com.baidu.bifromq.inbox.records.ScopedInbox;
import com.baidu.bifromq.inbox.rpc.proto.SendReply;
import com.baidu.bifromq.inbox.rpc.proto.SendRequest;
import com.baidu.bifromq.plugin.subbroker.DeliveryResult;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class InboxWriterPipeline extends ResponsePipeline<SendRequest, SendReply> {
    private static final double SLOWDOWN_DIRECT_MEM_USAGE = INGRESS_SLOWDOWN_DIRECT_MEMORY_USAGE.get();
    private static final double SLOWDOWN_HEAP_MEM_USAGE = INGRESS_SLOWDOWN_HEAP_MEMORY_USAGE.get();
    private static final Duration SLOWDOWN_TIMEOUT =
        Duration.ofSeconds(((Integer) MAX_SLOWDOWN_TIMEOUT_SECONDS.get()).longValue());

    interface IWriteCallback {
        void afterWrite(ScopedInbox scopedInbox, String delivererKey);
    }

    interface ISendRequestHandler {
        CompletableFuture<SendReply> handle(SendRequest request);
    }

    private final IWriteCallback writeCallback;
    private final ISendRequestHandler handler;
    private final String delivererKey;

    public InboxWriterPipeline(IWriteCallback writeCallback,
                               ISendRequestHandler handler,
                               StreamObserver<SendReply> responseObserver) {
        super(responseObserver, () -> MemInfo.directMemoryUsage() > SLOWDOWN_DIRECT_MEM_USAGE
            || MemInfo.heapMemoryUsage() > SLOWDOWN_HEAP_MEM_USAGE, SLOWDOWN_TIMEOUT);
        this.writeCallback = writeCallback;
        this.handler = handler;
        this.delivererKey = RPCContext.WCH_HASH_KEY_CTX_KEY.get();
    }

    @Override
    protected CompletableFuture<SendReply> handleRequest(String ignore, SendRequest request) {
        log.trace("Received inbox write request: deliverer={}, \n{}", delivererKey, request);
        return handler.handle(request).thenApply(v -> {
            v.getReply().getResultMap().forEach((tenantId, deliveryResults) -> {
                deliveryResults.getResultList().forEach(result -> {
                    if (result.getCode() == DeliveryResult.Code.OK) {
                        writeCallback.afterWrite(ScopedInbox.from(tenantId, result.getMatchInfo()), delivererKey);
                    }
                });
            });
            return v;
        });
    }
}
