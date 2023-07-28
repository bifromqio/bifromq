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
import com.baidu.bifromq.mqtt.service.ILocalSessionRegistry;
import com.baidu.bifromq.plugin.authprovider.IAuthProvider;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthData;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import com.baidu.bifromq.plugin.authprovider.type.MQTTAction;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.retain.client.IRetainClient;
import com.baidu.bifromq.sessiondict.client.ISessionDictClient;
import com.baidu.bifromq.type.ClientInfo;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.base.Ticker;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.netty.channel.ChannelHandlerContext;
import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class MQTTSessionContext {
    private final IAuthProvider authProvider;
    public final ILocalSessionRegistry localSessionRegistry;
    public final IEventCollector eventCollector;
    public final ISettingProvider settingProvider;
    public final IDistClient distClient;
    public final IInboxReaderClient inboxClient;
    public final IRetainClient retainClient;
    public final ISessionDictClient sessionDictClient;
    public final String serverId;

    public final int maxResendTimes;
    public final int resendDelayMillis;
    public final int defaultKeepAliveTimeSeconds;
    // track under confirming id count per tenantId
    private final InUseQoS2MessageIds unreleasedQoS2MessageIds;
    // cache for client dist pipeline
    private final LoadingCache<ClientInfo, IRetainClient.IClientPipeline> clientRetainPipelines;
    private final FutureTracker bgTaskTracker;
    private final Ticker ticker;
    private final Gauge retainPplnNumGauge;

    @Builder
    MQTTSessionContext(String serverId,
                       ILocalSessionRegistry sessionRegistry,
                       IAuthProvider authProvider,
                       IDistClient distClient,
                       IInboxReaderClient inboxClient,
                       IRetainClient retainClient,
                       ISessionDictClient sessionDictClient,
                       int maxResendTimes,
                       int resendDelayMillis,
                       int defaultKeepAliveTimeSeconds,
                       int qos2ConfirmWindowSeconds,
                       IEventCollector eventCollector,
                       ISettingProvider settingProvider,
                       Ticker ticker) {
        this.serverId = serverId;
        this.localSessionRegistry = sessionRegistry;
        this.authProvider = authProvider;
        this.eventCollector = eventCollector;
        this.settingProvider = settingProvider;
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
            .removalListener((RemovalListener<ClientInfo, IRetainClient.IClientPipeline>)
                (key, value, cause) -> {
                    if (value != null) {
                        log.trace("Close client retain pipeline: clientInfo={}, cause={}", key, cause);
                        value.close();
                    }
                })
            .build(retainClient::open);
        retainPplnNumGauge = Gauge.builder("mqtt.server.ppln.retain.gauge", clientRetainPipelines::estimatedSize)
            .register(Metrics.globalRegistry);
        this.bgTaskTracker = new FutureTracker();
        this.ticker = ticker == null ? Ticker.systemTicker() : ticker;
    }

    public long nanoTime() {
        return ticker.read();
    }

    public IAuthProvider authProvider(ChannelHandlerContext ctx) {
        // a wrapper to ensure async fifo semantic for check call
        return new IAuthProvider() {
            private final LinkedHashMap<CompletableFuture<Boolean>, CompletableFuture<Boolean>> checkTaskQueue =
                new LinkedHashMap<>();

            @Override
            public CompletableFuture<MQTT3AuthResult> auth(MQTT3AuthData authData) {
                return authProvider.auth(authData);
            }

            @Override
            public CompletableFuture<Boolean> check(ClientInfo client, MQTTAction action) {
                CompletableFuture<Boolean> task = authProvider.check(client, action);
                if (task.isDone()) {
                    return task;
                } else {
                    // queue it for fifo semantic
                    CompletableFuture<Boolean> onDone = new CompletableFuture<>();
                    // in case authProvider returns same future object;
                    task = task.thenApply(v -> v);
                    checkTaskQueue.put(task, onDone);
                    task.whenCompleteAsync((_v, _e) -> {
                        Iterator<CompletableFuture<Boolean>> itr = checkTaskQueue.keySet().iterator();
                        while (itr.hasNext()) {
                            CompletableFuture<Boolean> k = itr.next();
                            if (k.isDone()) {
                                CompletableFuture<Boolean> r = checkTaskQueue.get(k);
                                try {
                                    r.complete(k.join());
                                } catch (Throwable e) {
                                    r.completeExceptionally(e);
                                }
                                itr.remove();
                            } else {
                                break;
                            }
                        }
                    }, ctx.channel().eventLoop());
                    return onDone;
                }
            }
        };
    }

    public void addForConfirming(String tenantId, String channelId, int qos2MessageId) {
        unreleasedQoS2MessageIds.use(tenantId, channelId, qos2MessageId);
    }

    public boolean isConfirming(String tenantId, String channelId, int qos2MessageId) {
        return unreleasedQoS2MessageIds.inUse(tenantId, channelId, qos2MessageId);
    }

    public void confirm(String tenantId, String channelId, int qos2MessageId) {
        unreleasedQoS2MessageIds.release(tenantId, channelId, qos2MessageId);
    }

    public IRetainClient.IClientPipeline getClientRetainPipeline(ClientInfo clientInfo) {
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
