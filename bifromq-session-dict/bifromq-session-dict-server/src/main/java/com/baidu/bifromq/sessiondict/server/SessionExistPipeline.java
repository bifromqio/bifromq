/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.sessiondict.server;

import com.baidu.bifromq.baserpc.server.ResponsePipeline;
import com.baidu.bifromq.sessiondict.rpc.proto.ExistReply;
import com.baidu.bifromq.sessiondict.rpc.proto.ExistRequest;
import com.baidu.bifromq.type.ClientInfo;
import io.grpc.stub.StreamObserver;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

class SessionExistPipeline extends ResponsePipeline<ExistRequest, ExistReply> {
    private final ISessionRegistry sessionRegistry;

    public SessionExistPipeline(ISessionRegistry sessionRegistry, StreamObserver<ExistReply> responseObserver) {
        super(responseObserver);
        this.sessionRegistry = sessionRegistry;
    }

    @Override
    protected CompletableFuture<ExistReply> handleRequest(String tenantId, ExistRequest request) {
        ExistReply.Builder respBuilder = ExistReply.newBuilder()
            .setReqId(request.getReqId())
            .setCode(ExistReply.Code.OK);
        for (ExistRequest.Client client : request.getClientList()) {
            Optional<ClientInfo> clientOpt = sessionRegistry.get(tenantId, client.getUserId(), client.getClientId());
            respBuilder.addExist(clientOpt.isPresent());
        }
        return CompletableFuture.completedFuture(respBuilder.build());
    }
}
