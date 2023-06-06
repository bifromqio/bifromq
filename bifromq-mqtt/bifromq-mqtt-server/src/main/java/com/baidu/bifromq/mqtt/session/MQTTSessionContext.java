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

package com.baidu.bifromq.mqtt.session;

import com.baidu.bifromq.baserpc.utils.FutureTracker;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.inbox.client.IInboxReaderClient;
import com.baidu.bifromq.mqtt.service.ILocalSessionBrokerServer;
import com.baidu.bifromq.mqtt.service.ILocalSessionRegistry;
import com.baidu.bifromq.plugin.authprovider.ActionInfo;
import com.baidu.bifromq.plugin.authprovider.AuthData;
import com.baidu.bifromq.plugin.authprovider.AuthResult;
import com.baidu.bifromq.plugin.authprovider.CheckResult;
import com.baidu.bifromq.plugin.authprovider.IAuthProvider;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.retain.client.IRetainServiceClient;
import com.baidu.bifromq.sessiondict.client.ISessionDictionaryClient;
import com.baidu.bifromq.type.ClientInfo;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Timer;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

@Slf4j
public final class MQTTSessionContext {
    private final IAuthProvider authProvider;
    public final ILocalSessionRegistry localSessionRegistry;
    public final IEventCollector eventCollector;
    public final ISettingProvider settingsProvider;
    public final IDistClient distClient;
    public final IInboxReaderClient inboxClient;
    public final IRetainServiceClient retainClient;
    public final ISessionDictionaryClient sessionDictClient;
    public final String serverId;
    public final int maxResendTimes;
    public final int resendDelayMillis;
    public final int defaultKeepAliveTimeSeconds;
    // track under confirming id count per trafficId
    private final InUseQoS2MessageIds unreleasedQoS2MessageIds;
    // cache for client dist pipeline
    private final LoadingCache<ClientInfo, IRetainServiceClient.IClientPipeline> clientRetainPipelines;
    private final FutureTracker bgTaskTracker;
    private final Gauge retainPplnNumGauge;

    @Builder
    MQTTSessionContext(ILocalSessionBrokerServer brokerServer,
                       IAuthProvider authProvider,
                       IDistClient distClient,
                       IInboxReaderClient inboxClient,
                       IRetainServiceClient retainClient,
                       ISessionDictionaryClient sessionDictClient,
                       int maxResendTimes,
                       int resendDelayMillis,
                       int defaultKeepAliveTimeSeconds,
                       int qos2ConfirmWindowSeconds,
                       IEventCollector eventCollector,
                       ISettingProvider settingProvider) {
        this.localSessionRegistry = brokerServer;
        this.serverId = brokerServer.id();
        this.authProvider = authProvider;
        this.eventCollector = eventCollector;
        this.settingsProvider = settingProvider;
        this.distClient = distClient;
        this.inboxClient = inboxClient;
        this.retainClient = retainClient;
        this.sessionDictClient = sessionDictClient;
        this.unreleasedQoS2MessageIds = new InUseQoS2MessageIds(Duration.ofSeconds(qos2ConfirmWindowSeconds));
        this.maxResendTimes = maxResendTimes;
        this.resendDelayMillis = resendDelayMillis;
        this.defaultKeepAliveTimeSeconds = defaultKeepAliveTimeSeconds;
        this.clientRetainPipelines = Caffeine.newBuilder()
                .scheduler(Scheduler.systemScheduler())
                .expireAfterAccess(Duration.ofSeconds(30))
                .removalListener((RemovalListener<ClientInfo, IRetainServiceClient.IClientPipeline>)
                        (key, value, cause) -> {
                            if (value != null) {
                                log.trace("Close client retain pipeline: clientInfo={}, cause={}", key, cause);
                                value.close();
                            }
                        })
                .build(clientInfo -> retainClient.open(clientInfo));
        retainPplnNumGauge = Gauge.builder("mqtt.server.ppln.retain.gauge", clientRetainPipelines::estimatedSize)
                .register(Metrics.globalRegistry);
        this.bgTaskTracker = new FutureTracker();
    }

    public IAuthProvider authProvider(ChannelHandlerContext ctx) {
        // a wrapper to ensure async fifo semantic for check call
        return new IAuthProvider() {
            private final LinkedHashMap<CompletableFuture<? extends CheckResult>,
                    CompletableFuture<? extends CheckResult>> checkTaskQueue = new LinkedHashMap<>();

            @Override
            public <T extends AuthData, R extends AuthResult> CompletableFuture<R> auth(T authData) {
                return authProvider.auth(authData);
            }

            @Override
            public <A extends ActionInfo, R extends CheckResult>
            CompletableFuture<R> check(ClientInfo clientInfo, A actionInfo) {
                CompletableFuture<R> onDone = new CompletableFuture<>();
                ctx.channel().eventLoop().execute(() -> {
                    CompletableFuture<R> task = (CompletableFuture<R>) authProvider.check(clientInfo, actionInfo)
                            .thenApply(v -> v); // in case authProvider returns same future object
                    // add it to queue for fifo semantic
                    checkTaskQueue.put(task, onDone);
                    task.whenCompleteAsync((_v, _e) -> {
                        Iterator<CompletableFuture<? extends CheckResult>> itr = checkTaskQueue.keySet().iterator();
                        while (itr.hasNext()) {
                            CompletableFuture<? extends CheckResult> k = itr.next();
                            if (k.isDone()) {
                                CompletableFuture r = checkTaskQueue.get(k);
                                k.whenComplete((v, e) -> {
                                    if (e != null) {
                                        r.completeExceptionally(e);
                                    } else {
                                        r.complete(v);
                                    }
                                });
                                itr.remove();
                            } else {
                                break;
                            }
                        }
                    }, ctx.channel().eventLoop());
                });
                return onDone;
            }
        };
    }

    public void addForConfirming(String trafficId, String channelId, int qos2MessageId) {
        unreleasedQoS2MessageIds.use(trafficId, channelId, qos2MessageId);
    }

    public boolean isConfirming(String trafficId, String channelId, int qos2MessageId) {
        return unreleasedQoS2MessageIds.inUse(trafficId, channelId, qos2MessageId);
    }

    public void confirm(String trafficId, String channelId, int qos2MessageId) {
        unreleasedQoS2MessageIds.release(trafficId, channelId, qos2MessageId);
    }

    public IRetainServiceClient.IClientPipeline getClientRetainPipeline(ClientInfo clientInfo) {
        return clientRetainPipelines.get(clientInfo);
    }

    public void closeClientRetainPipeline(ClientInfo clientInfo) {
        clientRetainPipelines.invalidate(clientInfo);
    }

    public void addBgTask(Supplier<CompletableFuture<Void>> taskSupplier) {
        bgTaskTracker.track(taskSupplier.get());
    }

    public void awaitBgTaskDone() {
        bgTaskTracker.whenComplete((v, e) -> log.debug("All bg tasks done")).join();
    }
}
