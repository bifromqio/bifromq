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

package com.baidu.bifromq.mqtt.session;

import com.baidu.bifromq.baserpc.utils.FutureTracker;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.mqtt.service.ILocalDistService;
import com.baidu.bifromq.mqtt.service.ILocalSessionRegistry;
import com.baidu.bifromq.plugin.authprovider.IAuthProvider;
import com.baidu.bifromq.plugin.authprovider.type.CheckResult;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthData;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import com.baidu.bifromq.plugin.authprovider.type.MQTTAction;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.retain.client.IRetainClient;
import com.baidu.bifromq.sessiondict.client.ISessionDictClient;
import com.baidu.bifromq.type.ClientInfo;
import com.google.common.base.Ticker;
import io.netty.channel.ChannelHandlerContext;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.concurrent.CompletableFuture;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class MQTTSessionContext {
    private final IAuthProvider authProvider;
    public final ILocalSessionRegistry localSessionRegistry;
    public final ILocalDistService localDistService;
    public final IEventCollector eventCollector;
    public final ISettingProvider settingProvider;
    public final IDistClient distClient;
    public final IInboxClient inboxClient;
    public final IRetainClient retainClient;
    public final ISessionDictClient sessionDictClient;
    public final String serverId;
    public final int defaultKeepAliveTimeSeconds;
    private final Ticker ticker;
    private final FutureTracker futureTracker = new FutureTracker();

    @Builder
    MQTTSessionContext(String serverId,
                       ILocalSessionRegistry localSessionRegistry,
                       ILocalDistService localDistService,
                       IAuthProvider authProvider,
                       IDistClient distClient,
                       IInboxClient inboxClient,
                       IRetainClient retainClient,
                       ISessionDictClient sessionDictClient,
                       int defaultKeepAliveTimeSeconds,
                       IEventCollector eventCollector,
                       ISettingProvider settingProvider,
                       Ticker ticker) {
        this.serverId = serverId;
        this.localSessionRegistry = localSessionRegistry;
        this.localDistService = localDistService;
        this.authProvider = authProvider;
        this.eventCollector = eventCollector;
        this.settingProvider = settingProvider;
        this.distClient = distClient;
        this.inboxClient = inboxClient;
        this.retainClient = retainClient;
        this.sessionDictClient = sessionDictClient;
        this.defaultKeepAliveTimeSeconds = defaultKeepAliveTimeSeconds;
        this.ticker = ticker == null ? Ticker.systemTicker() : ticker;
    }

    public long nanoTime() {
        return ticker.read();
    }

    public IAuthProvider authProvider(ChannelHandlerContext ctx) {
        // a wrapper to ensure async fifo semantic for check call
        return new IAuthProvider() {
            private final LinkedHashMap<CompletableFuture<CheckResult>, CompletableFuture<CheckResult>>
                checkPermissionTaskQueue =
                new LinkedHashMap<>();

            private final LinkedHashMap<CompletableFuture<Boolean>, CompletableFuture<Boolean>>
                checkTaskQueue = new LinkedHashMap<>();

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

            @Override
            public CompletableFuture<CheckResult> checkPermission(ClientInfo client, MQTTAction action) {
                CompletableFuture<CheckResult> task = authProvider.checkPermission(client, action);
                if (task.isDone()) {
                    return task;
                } else {
                    // queue it for fifo semantic
                    CompletableFuture<CheckResult> onDone = new CompletableFuture<>();
                    // in case authProvider returns same future object;
                    task = task.thenApply(v -> v);
                    checkPermissionTaskQueue.put(task, onDone);
                    task.whenCompleteAsync((_v, _e) -> {
                        Iterator<CompletableFuture<CheckResult>> itr = checkPermissionTaskQueue.keySet().iterator();
                        while (itr.hasNext()) {
                            CompletableFuture<CheckResult> k = itr.next();
                            if (k.isDone()) {
                                CompletableFuture<CheckResult> r = checkPermissionTaskQueue.get(k);
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
                    }, ctx.executor());
                    return onDone;
                }
            }
        };
    }

    public <T> CompletableFuture<T> trackBgTask(CompletableFuture<T> task) {
        return futureTracker.track(task);
    }

    public CompletableFuture<Void> awaitBgTasksFinish() {
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        futureTracker.whenComplete((v, e) -> onDone.complete(null));
        return onDone;
    }
}
