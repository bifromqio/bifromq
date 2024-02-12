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

import static com.baidu.bifromq.baserpc.UnaryResponse.response;

import com.baidu.bifromq.mqtt.inbox.rpc.proto.OnlineInboxBrokerGrpc;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.SubReply;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.SubRequest;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.UnsubReply;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.UnsubRequest;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.WriteReply;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.WriteRequest;
import com.baidu.bifromq.mqtt.session.IMQTTSession;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class LocalSessionBrokerService extends OnlineInboxBrokerGrpc.OnlineInboxBrokerImplBase {
    private final ILocalSessionRegistry localSessionRegistry;
    private final ILocalDistService localDistService;

    public LocalSessionBrokerService(ILocalSessionRegistry registry, ILocalDistService service) {
        this.localSessionRegistry = registry;
        this.localDistService = service;
    }

    @Override
    public StreamObserver<WriteRequest> write(StreamObserver<WriteReply> responseObserver) {
        return new LocalSessionWritePipeline(localDistService, responseObserver);
    }

    @Override
    public void sub(SubRequest request, StreamObserver<SubReply> responseObserver) {
        response(tenantId -> {
            IMQTTSession session = localSessionRegistry.get(request.getSessionId());
            if (session != null) {
                SubReply.Builder builder = SubReply.newBuilder();
                builder.setReqId(request.getReqId());
                return session.subscribe(request.getReqId(), request.getTopicFilter(), request.getSubQoS())
                    .thenApply(v -> SubReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setResult(v)
                        .build());
            } else {
                return CompletableFuture.completedFuture(SubReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(SubReply.Result.NO_INBOX)
                    .build());
            }
        }, responseObserver);
    }

    @Override
    public void unsub(UnsubRequest request, StreamObserver<UnsubReply> responseObserver) {
        response(tenantId -> {
            IMQTTSession session = localSessionRegistry.get(request.getSessionId());
            if (session != null) {
                return session.unsubscribe(request.getReqId(), request.getTopicFilter())
                    .thenApply(v -> UnsubReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setResult(v)
                        .build());
            } else {
                return CompletableFuture.completedFuture(UnsubReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(UnsubReply.Result.NO_INBOX)
                    .build());
            }
        }, responseObserver);
    }

    public void close() {
    }
}
