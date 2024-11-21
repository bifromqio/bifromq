/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.starter.module;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.baserpc.utils.NettyUtil;
import com.baidu.bifromq.starter.config.StandaloneConfig;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Names;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.micrometer.core.instrument.binder.netty4.NettyEventExecutorMetrics;
import io.netty.channel.EventLoopGroup;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class ExecutorsModule extends AbstractModule {
    private static class RPCClientEventLoopProvider implements Provider<EventLoopGroup> {
        private final StandaloneConfig config;

        @Inject
        private RPCClientEventLoopProvider(StandaloneConfig config) {
            this.config = config;
        }

        @Override
        public EventLoopGroup get() {
            EventLoopGroup eventLoopGroup = NettyUtil.createEventLoopGroup(
                config.getRpcConfig().getClientEventLoopThreads(),
                EnvProvider.INSTANCE.newThreadFactory("rpc-client-worker-elg"));
            new NettyEventExecutorMetrics(eventLoopGroup).bindTo(Metrics.globalRegistry);
            return eventLoopGroup;
        }
    }

    private static class BackgroundTaskSchedulerProvider implements Provider<ScheduledExecutorService> {
        private final StandaloneConfig config;

        @Inject
        private BackgroundTaskSchedulerProvider(StandaloneConfig config) {
            this.config = config;
        }

        @Override
        public ScheduledExecutorService get() {
            return ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
                new ScheduledThreadPoolExecutor(config.getBgTaskThreads(),
                    EnvProvider.INSTANCE.newThreadFactory("bg-task-executor")), "bg-task-executor");
        }
    }

    @Override
    protected void configure() {
        bind(EventLoopGroup.class).annotatedWith(Names.named("rpcClientEventLoop"))
            .toProvider(RPCClientEventLoopProvider.class)
            .asEagerSingleton();
        bind(ScheduledExecutorService.class).annotatedWith(Names.named("bgTaskScheduler"))
            .toProvider(BackgroundTaskSchedulerProvider.class)
            .asEagerSingleton();
    }
}
