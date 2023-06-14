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

package com.baidu.bifromq.plugin.authprovider;

import static com.baidu.bifromq.baseutils.ThreadUtil.forkJoinThreadFactory;
import static com.baidu.bifromq.plugin.eventcollector.EventType.ACCESS_CONTROL_ERROR;
import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static com.baidu.bifromq.plugin.settingprovider.Setting.ByPassPermCheckError;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.MAX_CACHED_AUTH_RESULTS;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.AUTH_RESULT_EXPIRY_SECONDS;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.AUTH_CALL_PARALLELISM;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.MAX_CACHED_CHECK_RESULTS;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.CHECK_RESULT_EXPIRY_SECONDS;

import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthData;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import com.baidu.bifromq.plugin.authprovider.type.MQTTAction;
import com.baidu.bifromq.plugin.authprovider.type.Reject;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.accessctrl.AccessControlError;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.type.ClientInfo;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.base.Preconditions;
import com.netflix.concurrency.limits.limit.Gradient2Limit;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.PluginManager;

@Slf4j
public class AuthProviderManager implements IAuthProvider {
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final IAuthProvider delegate;
    private final ISettingProvider settingProvider;
    private final IEventCollector eventCollector;
    private final Gradient2Limit authCallLimit = Gradient2Limit.newBuilder().build();
    private final AtomicInteger authCalls = new AtomicInteger();
    private final Gradient2Limit checkCallLimit = Gradient2Limit.newBuilder().build();
    private final AtomicInteger checkCalls = new AtomicInteger();
    private ExecutorService executor;
    private AsyncLoadingCache<MQTT3AuthData, MQTT3AuthResult> authCache;
    private AsyncLoadingCache<CheckCacheKey, Boolean> checkCache;
    private MetricManager metricMgr;

    public AuthProviderManager(String authProviderFQN,
                               PluginManager pluginMgr,
                               ISettingProvider settingProvider,
                               IEventCollector eventCollector) {
        this.settingProvider = settingProvider;
        this.eventCollector = eventCollector;
        Map<String, IAuthProvider> availAuthProviders = pluginMgr.getExtensions(IAuthProvider.class)
            .stream().collect(Collectors.toMap(e -> e.getClass().getName(), e -> e));
        if (availAuthProviders.isEmpty()) {
            log.warn("No auth provider plugin available, use DEV ONLY one instead");
            delegate = new DevOnlyAuthProvider();
        } else {
            if (authProviderFQN == null) {
                log.warn("Auth provider plugin type are not specified, use DEV ONLY one instead");
                delegate = new DevOnlyAuthProvider();
            } else {
                Preconditions.checkArgument(availAuthProviders.containsKey(authProviderFQN),
                    String.format("Auth Provider Plugin '%s' not found", authProviderFQN));
                log.debug("Auth provider plugin type: {}", authProviderFQN);
                delegate = availAuthProviders.get(authProviderFQN);
            }
        }
        init();
    }

    private void init() {
        executor = ExecutorServiceMetrics
            .monitor(Metrics.globalRegistry,
                new ForkJoinPool(AUTH_CALL_PARALLELISM.get(), forkJoinThreadFactory("auth-%d"), null, true), "auth");

        authCache = Caffeine.newBuilder()
            .maximumSize(MAX_CACHED_AUTH_RESULTS.get())
            .expireAfterAccess(Duration.ofSeconds(AUTH_RESULT_EXPIRY_SECONDS.get()))
            .refreshAfterWrite(Duration.ofSeconds(AUTH_RESULT_EXPIRY_SECONDS.get()))
            .scheduler(Scheduler.systemScheduler())
            .executor(executor)
            .buildAsync((authData, executor) -> doAuth(authData));

        checkCache = Caffeine.newBuilder()
            .maximumSize(MAX_CACHED_CHECK_RESULTS.get())
            .expireAfterAccess(Duration.ofSeconds(CHECK_RESULT_EXPIRY_SECONDS.get()))
            .refreshAfterWrite(Duration.ofSeconds(CHECK_RESULT_EXPIRY_SECONDS.get()))
            .scheduler(Scheduler.systemScheduler())
            .executor(executor)
            .buildAsync((key, executor) -> doCheck(key.client, key.action));
        metricMgr = new MetricManager(delegate.getClass().getName());
    }

    @Override
    public CompletableFuture<MQTT3AuthResult> auth(MQTT3AuthData authData) {
        checkState();
        return authCache.get(authData);
    }

    @Override
    public CompletableFuture<Boolean> check(ClientInfo client, MQTTAction action) {
        checkState();
        return checkCache.get(new CheckCacheKey(client, action));
    }

    private CompletableFuture<MQTT3AuthResult> doAuth(MQTT3AuthData authData) {
        if (authCallLimit.getLimit() < authCalls.get()) {
            return CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                .setReject(Reject.newBuilder()
                    .setCode(Reject.Code.Error)
                    .setReason("Auth request throttled")
                    .build())
                .build());
        }
        long start = System.nanoTime();
        try {
            long timeout = 2 * authCallLimit.getRttNoLoad(TimeUnit.MILLISECONDS);
            if (timeout == 0) {
                timeout = 5000;
            }
            authCalls.incrementAndGet();
            timeout = Math.max(2000, timeout);
            return delegate.auth(authData)
                .orTimeout(timeout, TimeUnit.MILLISECONDS)
                .handle((v, e) -> {
                    authCalls.decrementAndGet();
                    if (e != null) {
                        return MQTT3AuthResult.newBuilder()
                            .setReject(Reject.newBuilder()
                                .setCode(Reject.Code.Error)
                                .setReason(e.getMessage() != null ? e.getMessage() : e.toString())
                                .build())
                            .build();
                    } else {
                        long latency = System.nanoTime() - start;
                        metricMgr.authCallTimer.record(latency, TimeUnit.NANOSECONDS);
                        authCallLimit.onSample(start, latency, authCalls.get(), false);
                        return v;
                    }
                });
        } catch (Throwable e) {
            metricMgr.authCallErrorCounter.increment();
            authCalls.decrementAndGet();
            log.warn("Unexpected error", e);
            Reject.Builder rb = Reject.newBuilder().setCode(Reject.Code.Error);
            if (e.getMessage() != null) {
                rb.setReason(e.getMessage());
            }
            return CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                .setReject(Reject.newBuilder()
                    .setCode(Reject.Code.Error)
                    .setReason(e.getMessage() != null ? e.getMessage() : e.toString())
                    .build())

                .build());
        }
    }

    private CompletableFuture<Boolean> doCheck(ClientInfo client, MQTTAction action) {
        if (checkCallLimit.getLimit() < checkCalls.get()) {
            log.debug("Check throttled: limit={}, current={}", checkCallLimit.getLimit(), checkCalls.get());
            return CompletableFuture.completedFuture(false);
        }
        long start = System.nanoTime();
        long timeout = 2 * checkCallLimit.getRttNoLoad(TimeUnit.MILLISECONDS);
        if (timeout == 0) {
            timeout = 5000;
        }
        timeout = Math.max(2000, timeout);
        try {
            checkCalls.incrementAndGet();
            return delegate.check(client, action)
                .orTimeout(timeout, TimeUnit.MILLISECONDS)
                .exceptionally(e -> {
                    eventCollector.report(getLocal(ACCESS_CONTROL_ERROR, AccessControlError.class)
                        .clientInfo(client).cause(e));
                    return settingProvider.provide(ByPassPermCheckError, client);
                })
                .thenApply(v -> {
                    checkCalls.decrementAndGet();
                    long latency = System.nanoTime() - start;
                    metricMgr.checkCallTimer.record(latency, TimeUnit.NANOSECONDS);
                    checkCallLimit.onSample(start, latency, checkCalls.get(), false);
                    return v;
                });
        } catch (Throwable e) {
            checkCalls.decrementAndGet();
            metricMgr.checkCallErrorCounter.increment();
            return CompletableFuture.failedFuture(e);
        }
    }

    public void close() {
        if (stopped.compareAndSet(false, true)) {
            log.info("Closing auth provider manager");
            authCache.synchronous().invalidateAll();
            checkCache.synchronous().invalidateAll();
            executor.shutdown();
            delegate.close();
            metricMgr.close();
            log.info("Auth provider manager stopped");
        }
    }

    private void checkState() {
        Preconditions.checkState(!stopped.get(), "AuthPluginManager has stopped");
    }

    @EqualsAndHashCode
    @AllArgsConstructor
    private static class CheckCacheKey {
        final ClientInfo client;
        final MQTTAction action;
    }

    private class MetricManager {

        private final Gauge cachedAuthResultsGauge;
        private final Timer authCallTimer;
        private final Counter authCallErrorCounter;
        private final Gauge authCallsLimitGauge;
        private final Gauge authCallsGauge;
        private final Gauge cachedCheckResultsGauge;
        private final Timer checkCallTimer;
        private final Counter checkCallErrorCounter;
        private final Gauge checkCallsLimitGauge;
        private final Gauge checkCallsGauge;

        MetricManager(String id) {
            cachedAuthResultsGauge = Gauge.builder("calls.cache.gauge", authCache.synchronous()::estimatedSize)
                .tag("method", "AuthProvider/auth")
                .tag("type", id)
                .register(Metrics.globalRegistry);

            authCallTimer = Timer.builder("call.exec.timer")
                .tag("method", "AuthProvider/auth")
                .tag("type", id)
                .register(Metrics.globalRegistry);

            authCallErrorCounter = Counter.builder("call.exec.fail.count")
                .tag("method", "AuthProvider/auth")
                .tag("type", id)
                .register(Metrics.globalRegistry);

            authCallsLimitGauge = Gauge.builder("calls.limit.gauge", authCallLimit::getLimit)
                .tag("method", "AuthProvider/auth")
                .tag("type", id)
                .register(Metrics.globalRegistry);

            authCallsGauge = Gauge.builder("calls.gauge", authCalls::get)
                .tag("method", "AuthProvider/auth")
                .tag("type", id)
                .register(Metrics.globalRegistry);

            cachedCheckResultsGauge = Gauge.builder("calls.cache.gauge", checkCache.synchronous()::estimatedSize)
                .tag("method", "AuthProvider/check")
                .tag("type", id)
                .register(Metrics.globalRegistry);

            checkCallTimer = Timer.builder("call.exec.timer")
                .tag("method", "AuthProvider/check")
                .tag("type", id)
                .register(Metrics.globalRegistry);

            checkCallErrorCounter = Counter.builder("call.exec.fail.count")
                .tag("method", "AuthProvider/check")
                .tag("type", id)
                .register(Metrics.globalRegistry);

            checkCallsLimitGauge = Gauge.builder("calls.limit.gauge", checkCallLimit::getLimit)
                .tag("method", "AuthProvider/check")
                .tag("type", id)
                .register(Metrics.globalRegistry);

            checkCallsGauge = Gauge.builder("calls.gauge", checkCalls::get)
                .tag("method", "AuthProvider/check")
                .tag("type", id)
                .register(Metrics.globalRegistry);
        }

        void close() {
            Metrics.globalRegistry.remove(cachedAuthResultsGauge);
            Metrics.globalRegistry.remove(authCallTimer);
            Metrics.globalRegistry.remove(authCallErrorCounter);
            Metrics.globalRegistry.remove(authCallsLimitGauge);
            Metrics.globalRegistry.remove(authCallsGauge);

            Metrics.globalRegistry.remove(cachedCheckResultsGauge);
            Metrics.globalRegistry.remove(checkCallTimer);
            Metrics.globalRegistry.remove(checkCallErrorCounter);
            Metrics.globalRegistry.remove(checkCallsLimitGauge);
            Metrics.globalRegistry.remove(checkCallsGauge);
        }
    }
}
