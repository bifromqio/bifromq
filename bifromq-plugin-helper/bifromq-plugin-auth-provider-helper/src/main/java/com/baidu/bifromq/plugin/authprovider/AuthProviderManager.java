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
import static com.baidu.bifromq.sysprops.BifroMQSysProp.AUTH_AUTH_RESULT_CACHE_LIMIT;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.AUTH_AUTH_RESULT_EXPIRY_SECONDS;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.AUTH_CALL_PARALLELISM;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.AUTH_CHECK_RESULT_CACHE_LIMIT;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.AUTH_CHECK_RESULT_EXPIRY_SECONDS;

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
    private final IAuthProvider provider;
    private final Gradient2Limit authCallLimit = Gradient2Limit.newBuilder().build();
    private final AtomicInteger authCalls = new AtomicInteger();
    private final Gradient2Limit checkCallLimit = Gradient2Limit.newBuilder().build();
    private final AtomicInteger checkCalls = new AtomicInteger();
    private ExecutorService executor;
    private AsyncLoadingCache<AuthData<?>, AuthResult> authCache;
    private AsyncLoadingCache<CheckCacheKey, CheckResult> checkCache;
    private MetricManager metricMgr;

    public AuthProviderManager(String authProviderFQN, PluginManager pluginMgr) {
        Map<String, IAuthProvider> availAuthProviders = pluginMgr.getExtensions(IAuthProvider.class).stream()
            .collect(Collectors.toMap(e -> e.getClass().getName(), e -> e));
        if (availAuthProviders.isEmpty()) {
            log.warn("No auth provider plugin available, use DEV ONLY one instead");
            provider = new DevOnlyAuthProvider();
        } else {
            if (authProviderFQN == null) {
                log.warn("Auth provider plugin type are not specified, use DEV ONLY one instead");
                provider = new DevOnlyAuthProvider();
            } else {
                Preconditions.checkArgument(availAuthProviders.containsKey(authProviderFQN),
                    String.format("Auth Provider Plugin '%s' not found", authProviderFQN));
                log.debug("Auth provider plugin type: {}", authProviderFQN);
                provider = availAuthProviders.get(authProviderFQN);
            }
        }
        init();
    }

    public AuthProviderManager(IAuthProvider provider) {
        this.provider = provider;
        init();
    }

    private void init() {
        executor = ExecutorServiceMetrics
            .monitor(Metrics.globalRegistry,
                new ForkJoinPool(AUTH_CALL_PARALLELISM.get(), forkJoinThreadFactory("auth-%d"), null, true), "auth");

        authCache = Caffeine.newBuilder()
            .maximumSize(AUTH_AUTH_RESULT_CACHE_LIMIT.get())
            .expireAfterAccess(Duration.ofSeconds(AUTH_AUTH_RESULT_EXPIRY_SECONDS.get()))
            .refreshAfterWrite(Duration.ofSeconds(AUTH_AUTH_RESULT_EXPIRY_SECONDS.get()))
            .scheduler(Scheduler.systemScheduler())
            .executor(executor)
            .buildAsync((authData, executor) -> doAuth(authData));

        checkCache = Caffeine.newBuilder()
            .maximumSize(AUTH_CHECK_RESULT_CACHE_LIMIT.get())
            .expireAfterAccess(Duration.ofSeconds(AUTH_CHECK_RESULT_EXPIRY_SECONDS.get()))
            .refreshAfterWrite(Duration.ofSeconds(AUTH_CHECK_RESULT_EXPIRY_SECONDS.get()))
            .scheduler(Scheduler.systemScheduler())
            .executor(executor)
            .buildAsync((key, executor) -> doCheck(key.clientInfo, key.actionInfo));
        metricMgr = new MetricManager(provider.getClass().getName());
    }

    @Override
    public <T extends AuthData<?>> CompletableFuture<AuthResult> auth(T authData) {
        checkState();
        return authCache.get(authData);
    }

    @Override
    public <A extends ActionInfo<?>> CompletableFuture<CheckResult> check(ClientInfo clientInfo, A actionInfo) {
        checkState();
        return checkCache.get(new CheckCacheKey(clientInfo, actionInfo));
    }

    public IAuthProvider get() {
        return provider;
    }

    private CompletableFuture<AuthResult> doAuth(AuthData<?> authData) {
        if (authCallLimit.getLimit() < authCalls.get()) {
            return CompletableFuture.completedFuture(
                AuthResult.error(new ThrottledException("Auth request throttled")));
        }
        long start = System.nanoTime();
        try {
            long timeout = 2 * authCallLimit.getRttNoLoad(TimeUnit.MILLISECONDS);
            if (timeout == 0) {
                timeout = 5000;
            }
            authCalls.incrementAndGet();
            timeout = Math.max(2000, timeout);
            return provider.auth(authData)
                .orTimeout(timeout, TimeUnit.MILLISECONDS)
                .exceptionally(AuthResult::error)
                .handle((v, e) -> {
                    authCalls.decrementAndGet();
                    if (e != null) {
                        return AuthResult.error(e);
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
            return CompletableFuture.completedFuture(AuthResult.error(e));
        }
    }

    private CompletableFuture<CheckResult> doCheck(ClientInfo clientInfo, ActionInfo<?> actionInfo) {
        if (checkCallLimit.getLimit() < checkCalls.get()) {
            log.debug("Check throttled: limit={}, current={}", checkCallLimit.getLimit(), checkCalls.get());
            return CompletableFuture.completedFuture(
                CheckResult.error(new ThrottledException("Check request throttled")));
        }
        long start = System.nanoTime();
        long timeout = 2 * checkCallLimit.getRttNoLoad(TimeUnit.MILLISECONDS);
        if (timeout == 0) {
            timeout = 5000;
        }
        timeout = Math.max(2000, timeout);
        try {
            checkCalls.incrementAndGet();
            return provider.check(clientInfo, actionInfo)
                .orTimeout(timeout, TimeUnit.MILLISECONDS)
                .exceptionally(CheckResult::error)
                .handle((v, e) -> {
                    checkCalls.decrementAndGet();
                    if (e != null) {
                        return CheckResult.error(e);
                    } else {
                        long latency = System.nanoTime() - start;
                        metricMgr.checkCallTimer.record(latency, TimeUnit.NANOSECONDS);
                        checkCallLimit.onSample(start, latency, checkCalls.get(), false);
                        return v;
                    }
                });
        } catch (Throwable e) {
            checkCalls.decrementAndGet();
            metricMgr.checkCallErrorCounter.increment();
            return CompletableFuture.completedFuture(CheckResult.error(e));
        }
    }

    public void close() {
        if (stopped.compareAndSet(false, true)) {
            log.info("Closing auth provider manager");
            authCache.synchronous().invalidateAll();
            checkCache.synchronous().invalidateAll();
            executor.shutdown();
            provider.close();
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
        final ClientInfo clientInfo;
        final ActionInfo<?> actionInfo;
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
