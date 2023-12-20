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

package com.baidu.bifromq.retain.server;

import static com.baidu.bifromq.baserpc.UnaryResponse.response;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.retain.rpc.proto.MatchReply;
import com.baidu.bifromq.retain.rpc.proto.MatchRequest;
import com.baidu.bifromq.retain.rpc.proto.RetainReply;
import com.baidu.bifromq.retain.rpc.proto.RetainRequest;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceGrpc;
import com.baidu.bifromq.retain.server.scheduler.IMatchCallScheduler;
import com.baidu.bifromq.retain.server.scheduler.IRetainCallScheduler;
import com.baidu.bifromq.retain.server.scheduler.MatchCallScheduler;
import com.baidu.bifromq.retain.server.scheduler.RetainCallScheduler;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RetainService extends RetainServiceGrpc.RetainServiceImplBase {
    private final IMatchCallScheduler matchCallScheduler;
    private final IRetainCallScheduler retainCallScheduler;

    RetainService(IBaseKVStoreClient retainStoreClient) {
        this.matchCallScheduler = new MatchCallScheduler(retainStoreClient);
        this.retainCallScheduler = new RetainCallScheduler(retainStoreClient);
    }

    @Override
    public void retain(RetainRequest request, StreamObserver<RetainReply> responseObserver) {
        log.trace("Handling retain request:\n{}", request);
        response((tenantId, metadata) -> retainCallScheduler
            .schedule(request)
            .exceptionally(e -> {
                log.error("Retain failed", e);
                return RetainReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(RetainReply.Result.ERROR)
                    .build();

            }), responseObserver);
    }

    @Override
    public void match(MatchRequest request, StreamObserver<MatchReply> responseObserver) {
        log.trace("Handling match request:\n{}", request);
        response((tenantId, metadata) -> matchCallScheduler
            .schedule(request)
            .exceptionally(e -> {
                log.error("Match failed", e);
                return MatchReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(MatchReply.Result.ERROR)
                    .build();
            }), responseObserver);
    }
}
