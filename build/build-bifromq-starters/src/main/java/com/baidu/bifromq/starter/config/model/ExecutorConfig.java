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

package com.baidu.bifromq.starter.config.model;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.baserpc.utils.NettyUtil;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.netty.channel.EventLoopGroup;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ExecutorConfig {
    private int mqttBossThreads = 1;
    private int mqttWorkerThreads = Runtime.getRuntime().availableProcessors();
    private int rpcBossThreads = 1;
    private int rpcWorkerThreads = Runtime.getRuntime().availableProcessors();
    private int ioClientParallelism = Math.max(2, Runtime.getRuntime().availableProcessors() / 3);
    private int ioServerParallelism = Math.max(2, Runtime.getRuntime().availableProcessors() / 3);
    private int queryThreads = Math.max(2, Runtime.getRuntime().availableProcessors() / 4);
    private int mutationThreads = 3;
    private int tickerThreads = Math.max(1, Runtime.getRuntime().availableProcessors() / 20);
    private int bgWorkerThreads = Math.max(1, Runtime.getRuntime().availableProcessors() / 20);

    public EventLoopGroup mqttBossGroup() {
        return NettyUtil.createEventLoopGroup(mqttBossThreads,
            EnvProvider.INSTANCE.newThreadFactory("mqtt-boss"));
    }

    public EventLoopGroup mqttWorkerGroup() {
        return NettyUtil.createEventLoopGroup(mqttWorkerThreads,
            EnvProvider.INSTANCE.newThreadFactory("mqtt-worker"));
    }

    public EventLoopGroup rpcBossGroup() {
        return NettyUtil.createEventLoopGroup(mqttBossThreads,
            EnvProvider.INSTANCE.newThreadFactory("rpc-boss"));
    }

    public EventLoopGroup rpcWorkerGroup() {
        return NettyUtil.createEventLoopGroup(mqttWorkerThreads,
            EnvProvider.INSTANCE.newThreadFactory("rpc-worker"));
    }

    public ExecutorService ioClientExecutor() {
        return ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ThreadPoolExecutor(ioClientParallelism, ioClientParallelism, 0L,
                TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
                EnvProvider.INSTANCE.newThreadFactory("io-client-executor")), "io-client-executor");
    }

    public ExecutorService ioServerExecutor() {
        return ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ThreadPoolExecutor(ioServerParallelism, ioServerParallelism, 0L,
                TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
                EnvProvider.INSTANCE.newThreadFactory("io-server-executor")), "io-server-executor");
    }

    public ExecutorService queryExecutor() {
        return ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ThreadPoolExecutor(queryThreads, queryThreads, 0L,
                TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
                EnvProvider.INSTANCE.newThreadFactory("query-executor")), "query-executor");
    }

    public ExecutorService mutationExecutor() {
        return ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ThreadPoolExecutor(mutationThreads, mutationThreads, 0L,
                TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
                EnvProvider.INSTANCE.newThreadFactory("mutation-executor")), "mutation-executor");
    }

    public ScheduledExecutorService tickTaskExecutor() {
        return ExecutorServiceMetrics
            .monitor(Metrics.globalRegistry, new ScheduledThreadPoolExecutor(tickerThreads,
                EnvProvider.INSTANCE.newThreadFactory("tick-task-executor")), "tick-task-executor");
    }

    public ScheduledExecutorService bgTaskExecutor() {
        return ExecutorServiceMetrics
            .monitor(Metrics.globalRegistry, new ScheduledThreadPoolExecutor(bgWorkerThreads,
                EnvProvider.INSTANCE.newThreadFactory("bg-task-executor")), "bg-task-executor");
    }

}
