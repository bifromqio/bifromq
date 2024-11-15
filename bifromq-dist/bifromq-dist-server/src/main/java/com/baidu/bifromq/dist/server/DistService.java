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

import static com.baidu.bifromq.baserpc.server.UnaryResponse.response;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.dist.rpc.proto.DistReply;
import com.baidu.bifromq.dist.rpc.proto.DistRequest;
import com.baidu.bifromq.dist.rpc.proto.DistServiceGrpc;
import com.baidu.bifromq.dist.rpc.proto.MatchReply;
import com.baidu.bifromq.dist.rpc.proto.MatchRequest;
import com.baidu.bifromq.dist.rpc.proto.UnmatchReply;
import com.baidu.bifromq.dist.rpc.proto.UnmatchRequest;
import com.baidu.bifromq.dist.server.handler.MatchReqHandler;
import com.baidu.bifromq.dist.server.handler.UnmatchReqHandler;
import com.baidu.bifromq.dist.server.scheduler.DistWorkerCallScheduler;
import com.baidu.bifromq.dist.server.scheduler.IDistWorkerCallScheduler;
import com.baidu.bifromq.dist.server.scheduler.IMatchCallScheduler;
import com.baidu.bifromq.dist.server.scheduler.IUnmatchCallScheduler;
import com.baidu.bifromq.dist.server.scheduler.MatchCallScheduler;
import com.baidu.bifromq.dist.server.scheduler.UnmatchCallScheduler;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DistService extends DistServiceGrpc.DistServiceImplBase {
    private final IEventCollector eventCollector;
    private final IDistWorkerCallScheduler distCallScheduler;
    private final MatchReqHandler matchReqHandler;
    private final UnmatchReqHandler unmatchReqHandler;

    DistService(IBaseKVStoreClient distWorkerClient,
                ISettingProvider settingProvider,
                IEventCollector eventCollector) {
        this.eventCollector = eventCollector;

        IMatchCallScheduler matchCallScheduler = new MatchCallScheduler(distWorkerClient, settingProvider);
        matchReqHandler = new MatchReqHandler(eventCollector, matchCallScheduler);

        IUnmatchCallScheduler unmatchCallScheduler = new UnmatchCallScheduler(distWorkerClient);
        unmatchReqHandler = new UnmatchReqHandler(eventCollector, unmatchCallScheduler);

        this.distCallScheduler = new DistWorkerCallScheduler(distWorkerClient, settingProvider);
    }

    @Override
    public void match(MatchRequest request, StreamObserver<MatchReply> responseObserver) {
        response(tenantId -> matchReqHandler.handle(request), responseObserver);
    }

    public void unmatch(UnmatchRequest request, StreamObserver<UnmatchReply> responseObserver) {
        response(tenantId -> unmatchReqHandler.handle(request), responseObserver);
    }

    @Override
    public StreamObserver<DistRequest> dist(StreamObserver<DistReply> responseObserver) {
        return new DistResponsePipeline(distCallScheduler, responseObserver, eventCollector);
    }

    public void stop() {
        log.debug("stop dist worker call scheduler");
        distCallScheduler.close();
        log.debug("Stop match call handler");
        matchReqHandler.close();
        log.debug("Stop unmatch call handler");
        unmatchReqHandler.close();
    }
}
