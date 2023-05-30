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

package com.baidu.bifromq.inbox.server;

import com.baidu.bifromq.baserpc.RPCContext;
import com.baidu.bifromq.baserpc.ResponsePipeline;
import com.baidu.bifromq.inbox.rpc.proto.SendReply;
import com.baidu.bifromq.inbox.rpc.proto.SendRequest;
import com.baidu.bifromq.inbox.rpc.proto.SendResult;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxWriterPipeline extends ResponsePipeline<SendRequest, SendReply> {
    private final InboxFetcherRegistry registry;
    private final RequestHandler handler;
    private final String inboxGroupKey;

    public InboxWriterPipeline(InboxFetcherRegistry registry, RequestHandler handler,
                               StreamObserver<SendReply> responseObserver) {
        super(responseObserver);
        this.registry = registry;
        this.handler = handler;
        this.inboxGroupKey = RPCContext.WCH_HASH_KEY_CTX_KEY.get();
        // ensure fetch triggered when receive pipeline rebalanced
        registry.signalFetch(inboxGroupKey);
    }

    @Override
    protected CompletableFuture<SendReply> handleRequest(String ignore, SendRequest request) {
        log.trace("Received inbox write request: inboxGroupKey={}, \n{}", inboxGroupKey, request);
        return handler.handle(request).thenApply(v -> {
            for (SendResult result : v.getResultList()) {
                if (result.getResult() == SendResult.Result.OK) {
                    IInboxQueueFetcher f =
                        registry.get(result.getSubInfo().getTrafficId(), result.getSubInfo().getInboxId(),
                            inboxGroupKey);
                    if (f != null) {
                        f.signalFetch();
                    }
                }
            }
            return v;
        });
    }

    interface RequestHandler {
        CompletableFuture<SendReply> handle(SendRequest request);
    }
}
