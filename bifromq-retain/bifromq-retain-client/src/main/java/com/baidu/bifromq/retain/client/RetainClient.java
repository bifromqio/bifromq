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

package com.baidu.bifromq.retain.client;

import static com.baidu.bifromq.retain.utils.PipelineUtil.PIPELINE_ATTR_KEY_CLIENT_INFO;
import static com.baidu.bifromq.retain.utils.PipelineUtil.encode;
import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.retain.RPCBluePrint;
import com.baidu.bifromq.retain.rpc.proto.MatchReply;
import com.baidu.bifromq.retain.rpc.proto.MatchRequest;
import com.baidu.bifromq.retain.rpc.proto.RetainReply;
import com.baidu.bifromq.retain.rpc.proto.RetainRequest;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceGrpc;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.QoS;
import io.reactivex.rxjava3.core.Observable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class RetainClient implements IRetainClient {
    private final IRPCClient rpcClient;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    RetainClient(RetainClientBuilder builder) {
        this.rpcClient = IRPCClient.newBuilder()
            .bluePrint(RPCBluePrint.INSTANCE)
            .executor(builder.executor)
            .eventLoopGroup(builder.eventLoopGroup)
            .sslContext(builder.sslContext)
            .crdtService(builder.crdtService)
            .build();
    }

    @Override
    public void stop() {
        if (closed.compareAndSet(false, true)) {
            log.info("Stopping retain client");
            log.debug("Stopping rpc client");
            rpcClient.stop();
            log.debug("Retain client stopped");
        }
    }

    @Override
    public Observable<IRPCClient.ConnState> connState() {
        return rpcClient.connState();
    }

    @Override
    public IClientPipeline open(ClientInfo clientInfo) {
        Map<String, String> pipelineAttrs = new HashMap<>() {{
            put(PIPELINE_ATTR_KEY_CLIENT_INFO, encode(clientInfo));
        }};
        return new IClientPipeline() {
            private final IRPCClient.IRequestPipeline<RetainRequest, RetainReply> ppln =
                rpcClient.createRequestPipeline(clientInfo.getTenantId(), null,
                    null, pipelineAttrs, RetainServiceGrpc.getRetainMethod());

            @Override
            public CompletableFuture<RetainReply> retain(long reqId, String topic,
                                                         QoS qos, ByteBuffer payload, int expirySeconds) {
                long now = System.currentTimeMillis();
                long expiry = expirySeconds == Integer.MAX_VALUE ? Long.MAX_VALUE : now +
                    TimeUnit.MILLISECONDS.convert(expirySeconds, TimeUnit.SECONDS);
                return ppln.invoke(RetainRequest.newBuilder()
                        .setReqId(reqId)
                        .setQos(qos)
                        .setTopic(topic)
                        .setTimestamp(now)
                        .setExpireTimestamp(expiry)
                        .setPayload(unsafeWrap(payload))
                        .build())
                    .exceptionally(e -> RetainReply.newBuilder()
                        .setReqId(reqId)
                        .setResult(RetainReply.Result.ERROR)
                        .build());
            }

            @Override
            public void close() {
                ppln.close();
            }
        };
    }

    @Override
    public CompletableFuture<MatchReply> match(long reqId, String tenantId,
                                               String topicFilter, int limit, ClientInfo clientInfo) {
        Map<String, String> pipelineAttrs = new HashMap<>() {{
            put(PIPELINE_ATTR_KEY_CLIENT_INFO, encode(clientInfo));
        }};
        log.trace("Handling match request: reqId={}, topicFilter={}", reqId, topicFilter);
        return rpcClient.invoke(tenantId, null, MatchRequest.newBuilder()
                .setReqId(reqId)
                .setTopicFilter(topicFilter)
                .setLimit(limit)
                .build(), pipelineAttrs, RetainServiceGrpc.getMatchMethod())
            .whenComplete((v, e) -> {
                if (e != null) {
                    log.trace("Finish handling match request with error: reqId={}, topicFilter={}",
                        reqId, topicFilter, e);
                } else {
                    log.trace("Finish handling match request: reqId={}, topicFilter={}, reply={}",
                        reqId, topicFilter, v);
                }
            });
    }
}
