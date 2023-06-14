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

package com.baidu.bifromq.sessiondict.server;

import static com.baidu.bifromq.baserpc.UnaryResponse.response;
import static com.baidu.bifromq.metrics.TrafficMeter.gauging;
import static com.baidu.bifromq.metrics.TrafficMeter.stopGauging;
import static com.baidu.bifromq.metrics.TrafficMetric.MqttConnectionGauge;
import static com.github.benmanes.caffeine.cache.RemovalCause.EXPIRED;
import static com.github.benmanes.caffeine.cache.RemovalCause.SIZE;

import com.baidu.bifromq.baserpc.AckStream;
import com.baidu.bifromq.sessiondict.PipelineUtil;
import com.baidu.bifromq.sessiondict.rpc.proto.KillReply;
import com.baidu.bifromq.sessiondict.rpc.proto.KillRequest;
import com.baidu.bifromq.sessiondict.rpc.proto.Ping;
import com.baidu.bifromq.sessiondict.rpc.proto.Quit;
import com.baidu.bifromq.sessiondict.rpc.proto.SessionDictionaryServiceGrpc;
import com.baidu.bifromq.type.ClientInfo;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SessionDictionaryService extends SessionDictionaryServiceGrpc.SessionDictionaryServiceImplBase {

    private final Cache<AckStream<Ping, Quit>, ClientInfo> kickedPipelines = Caffeine.newBuilder()
        .expireAfterWrite(5, TimeUnit.SECONDS)
        .maximumSize(1000_000)
        .removalListener((RemovalListener<AckStream<Ping, Quit>, ClientInfo>) (key, value, cause) -> {
            if (cause == EXPIRED || cause == SIZE) {
                key.send(Quit.newBuilder().setKiller(value).build());
                key.onCompleted();
            }
        })
        .build();
    // trafficId -> userId/clientId -> AckPipeline<Ping, Quit>
    private final Map<String, Map<String, Registration>> registry = new ConcurrentHashMap<>();

    @Override
    public StreamObserver<Ping> join(StreamObserver<Quit> responseObserver) {
        return new Registration(responseObserver);
    }

    @Override
    public void kill(KillRequest request, StreamObserver<KillReply> responseObserver) {
        response(trafficId -> {
            Registration reg = registry.getOrDefault(trafficId, Collections.emptyMap())
                .get(toRegKey(request.getUserId(), request.getClientId()));
            if (reg != null) {
                reg.quit(request.getKiller());
            }
            return CompletableFuture.completedFuture(KillReply.newBuilder()
                .setReqId(request.getReqId())
                .setResult(KillReply.Result.OK)
                .build());
        }, responseObserver);
    }

    void close() {
        registry.forEach((trafficId, sessions) ->
            sessions.forEach((sessionId, ackPipeline) -> ackPipeline.onCompleted()));
    }

    private class Registration extends AckStream<Ping, Quit> {
        private final String regKey;
        private final ClientInfo clientInfo;

        protected Registration(StreamObserver<Quit> responseObserver) {
            super(responseObserver);
            clientInfo = PipelineUtil.decode(metadata.get(PipelineUtil.CLIENT_INFO));
            assert clientInfo.hasMqtt3ClientInfo();
            log.trace("Receive session registering, trafficId={}, userId={}, clientId={}, addr={}:{}",
                trafficId, clientInfo.getUserId(), clientInfo.getMqtt3ClientInfo().getClientId(),
                clientInfo.getMqtt3ClientInfo().getIp(), clientInfo.getMqtt3ClientInfo().getPort());
            regKey = toRegKey(clientInfo.getUserId(), clientInfo.getMqtt3ClientInfo().getClientId());
            registry.compute(trafficId, (t, m) -> {
                if (m == null) {
                    m = new HashMap<>();
                    gauging(trafficId, MqttConnectionGauge,
                        () -> registry.getOrDefault(trafficId, Collections.EMPTY_MAP).size());
                }
                m.compute(regKey, (r, oldPipeline) -> {
                    if (oldPipeline != null) {
                        oldPipeline.quit(clientInfo);
                        kickedPipelines.put(oldPipeline, clientInfo);
                    }
                    return this;
                });
                return m;
            });

            this.ack().doOnComplete(this::leave).subscribe();
        }

        public void quit(ClientInfo killer) {
            long reqId = System.nanoTime();
            if (log.isTraceEnabled()) {
                log.trace("Quit pipeline: reqId={}, trafficId={}, userId={}, clientId={}, address={}:{}",
                    reqId, trafficId, clientInfo.getUserId(),
                    clientInfo.getMqtt3ClientInfo().getClientId(),
                    clientInfo.getMqtt3ClientInfo().getIp(),
                    clientInfo.getMqtt3ClientInfo().getPort());
            }
            send(Quit.newBuilder().setKiller(killer).build());
        }

        private void leave() {
            registry.compute(trafficId, (t, m) -> {
                if (m == null) {
                    stopGauging(trafficId, MqttConnectionGauge);
                    return null;
                } else {
                    m.remove(regKey, this);
                    if (m.size() == 0) {
                        stopGauging(trafficId, MqttConnectionGauge);
                        return null;
                    }
                    return m;
                }
            });
            kickedPipelines.invalidate(this);
        }
    }

    private String toRegKey(String userId, String clientId) {
        return userId + "/" + clientId;
    }
}
