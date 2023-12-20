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

package com.baidu.bifromq.baserpc.benchmark;

import com.baidu.bifromq.basecluster.AgentHostOptions;
import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.CRDTServiceOptions;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.baserpc.BluePrint;
import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.baserpc.IRPCServer;
import com.baidu.bifromq.baserpc.ResponsePipeline;
import com.baidu.bifromq.baserpc.test.RPCTestGrpc;
import com.baidu.bifromq.baserpc.test.Request;
import com.baidu.bifromq.baserpc.test.Response;
import com.baidu.bifromq.baserpc.utils.NettyUtil;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@Slf4j
@State(Scope.Benchmark)
public class NonSSLPipelineUnaryState {
    private IAgentHost agentHost;
    private ICRDTService crdtService;
    private Executor executor;

    private BluePrint bluePrint = BluePrint.builder()
        .serviceDescriptor(RPCTestGrpc.getServiceDescriptor())
        .methodSemantic(RPCTestGrpc.getPipelineReqMethod(), BluePrint.WRPipelineUnaryMethod.getInstance())
        .build();
    private IRPCClient client;
    private IRPCServer server;
    private IRPCClient.IRequestPipeline<Request, Response> ppln;
    private AtomicInteger seq = new AtomicInteger();

    public NonSSLPipelineUnaryState() {
        AgentHostOptions agentHostOpts = AgentHostOptions.builder()
            .addr("127.0.0.1")
            .baseProbeInterval(Duration.ofSeconds(10))
            .joinRetryInSec(5)
            .joinTimeout(Duration.ofMinutes(5))
            .build();
        agentHost = IAgentHost.newInstance(agentHostOpts);
        agentHost.start();
        crdtService = ICRDTService.newInstance(CRDTServiceOptions.builder().build());
        crdtService.start(agentHost);
        executor = Executors.newFixedThreadPool(4);
        this.server = IRPCServer.newBuilder()
            .host("127.0.0.1")
            .bossEventLoopGroup(NettyUtil.createEventLoopGroup(1))
            .workerEventLoopGroup(NettyUtil.createEventLoopGroup())
            .executor(executor)
            .crdtService(crdtService)
            .bindService(new RPCTestGrpc.RPCTestImplBase() {
                private ExecutorService executor = Executors.newSingleThreadExecutor();

                @Override
                public StreamObserver<Request> pipelineReq(StreamObserver<Response> responseObserver) {
                    return new ResponsePipeline<>(responseObserver) {
                        @Override
                        protected CompletableFuture<Response> handleRequest(String tenantId, Request request) {
                            return CompletableFuture.completedFuture(Response.newBuilder()
                                .setId(request.getId())
                                .setValue(request.getValue())
                                .setBin(request.getBin())
                                .build());
//                                CompletableFuture<Response> resp = new CompletableFuture<>();
//                                executor.execute(() -> resp.complete(Response.newBuilder()
//                                        .setId(request.getId())
//                                        .setValue(request.getValue())
//                                        .build()));
//                                return resp;
                        }
                    };
                }
            }.bindService(), bluePrint)
            .build();
    }

    @Setup(Level.Trial)
    public void setup() {
        server.start();
        client = IRPCClient.newBuilder()
            .eventLoopGroup(NettyUtil.createEventLoopGroup())
            .crdtService(crdtService)
            .bluePrint(bluePrint)
            .executor(MoreExecutors.directExecutor())
            .build();
        ppln = client.createRequestPipeline("abc", null, null,
            Collections.emptyMap(), RPCTestGrpc.getPipelineReqMethod());
    }

    @TearDown(Level.Trial)
    public void teardown() {
        client.stop();
        server.shutdown();
        if (executor instanceof ExecutorService) {
            ((ExecutorService) executor).shutdownNow();
        }
    }

    public Response request() {
        int reqId = seq.incrementAndGet();
        Response resp = ppln.invoke(Request.newBuilder()
            .setId(reqId)
            .setValue(reqId + "_value")
            .setBin(ByteString.copyFromUtf8("Hello: " + reqId))
            .build()).join();
        if (resp.getId() != reqId) {
            log.error("ReqId mismatch: send={}, recv={}", reqId, resp.getId());
        }
        return resp;
    }
}
