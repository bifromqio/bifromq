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

package com.baidu.bifromq.mqtt.service;

import static com.baidu.bifromq.baserpc.UnaryResponse.response;

import com.baidu.bifromq.mqtt.inbox.rpc.proto.HasInboxReply;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.HasInboxRequest;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.OnlineInboxBrokerGrpc;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.WriteReply;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.WriteRequest;
import com.baidu.bifromq.mqtt.session.IMQTTSession;
import com.baidu.bifromq.mqtt.session.v3.IMQTT3TransientSession;
import com.google.common.util.concurrent.RateLimiter;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class LocalSessionBrokerService extends OnlineInboxBrokerGrpc.OnlineInboxBrokerImplBase {
    private final ConcurrentMap<String, IMQTTSession> sessionMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, IMQTT3TransientSession> transientSessionMap = new ConcurrentHashMap<>();

    private final Gauge connCountGauge;

    public LocalSessionBrokerService() {
        connCountGauge = Gauge.builder("mqtt.server.connection.gauge", sessionMap::size)
            .register(Metrics.globalRegistry);
    }

    @Override
    public StreamObserver<WriteRequest> write(StreamObserver<WriteReply> responseObserver) {
        return new LocalSessionWritePipeline(transientSessionMap, responseObserver);
    }

    @Override
    public void hasInbox(HasInboxRequest request, StreamObserver<HasInboxReply> responseObserver) {
        response(trafficId -> CompletableFuture.completedFuture(HasInboxReply
            .newBuilder()
            .setReqId(request.getReqId())
            .setResult(sessionMap.containsKey(request.getInboxId()))
            .build()), responseObserver);
    }

    void reg(String sessionId, IMQTTSession session) {
        sessionMap.putIfAbsent(sessionId, session);
        if (session instanceof IMQTT3TransientSession) {
            transientSessionMap.putIfAbsent(sessionId, (IMQTT3TransientSession) session);
        }
    }

    boolean unreg(String sessionId, IMQTTSession session) {
        transientSessionMap.remove(sessionId);
        return sessionMap.remove(sessionId, session);
    }

    public CompletableFuture<Void> disconnectAll(int disconnectRate) {
        RateLimiter limiter = RateLimiter.create(Math.max(1, disconnectRate));
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (String sessionId : sessionMap.keySet()) {
            limiter.acquire();
            futures.add(disconnect(sessionId));
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    public void close() {
        Metrics.globalRegistry.remove(connCountGauge);
    }

    private CompletableFuture<Void> disconnect(String sessionId) {
        IMQTTSession session = sessionMap.remove(sessionId);
        transientSessionMap.remove(sessionId);
        if (session != null) {
            return session.disconnect();
        }
        return CompletableFuture.completedFuture(null);
    }
}
