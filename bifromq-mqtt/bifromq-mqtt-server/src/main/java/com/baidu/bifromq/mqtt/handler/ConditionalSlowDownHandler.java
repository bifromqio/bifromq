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

package com.baidu.bifromq.mqtt.handler;

import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.MAX_SLOWDOWN_TIMEOUT_SECONDS;

import com.baidu.bifromq.mqtt.handler.condition.Condition;
import com.baidu.bifromq.mqtt.handler.condition.InboundResourceCondition;
import com.baidu.bifromq.baserpc.utils.MemInfo;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.eventcollector.OutOfTenantResource;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ResourceThrottled;
import com.baidu.bifromq.type.ClientInfo;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConditionalSlowDownHandler extends ChannelInboundHandlerAdapter {
    public static final String NAME = "SlowDownHandler";
    private static final long MAX_SLOWDOWN_TIME =
        Duration.ofSeconds(((Integer) MAX_SLOWDOWN_TIMEOUT_SECONDS.get()).longValue()).toNanos();
    private final Set<Condition> slowDownConditions;
    private final Supplier<Long> nanoProvider;
    private final IEventCollector eventCollector;
    private final ClientInfo clientInfo;
    private ChannelHandlerContext ctx;
    private ScheduledFuture<?> resumeTask;
    private long slowDownAt = Long.MAX_VALUE;

    public ConditionalSlowDownHandler(Set<Condition> slowDownConditions,
                                      IEventCollector eventCollector,
                                      Supplier<Long> nanoProvider,
                                      ClientInfo clientInfo) {
        this.slowDownConditions = slowDownConditions;
        this.eventCollector = eventCollector;
        this.nanoProvider = nanoProvider;
        this.clientInfo = clientInfo;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        super.handlerAdded(ctx);
        this.ctx = ctx;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        if (resumeTask != null) {
            resumeTask.cancel(true);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        for (Condition slowDownCondition : slowDownConditions) {
            if (slowDownCondition.meet()) {
                log.debug("Stop read: directMemoryUsage={}, heapMemoryUsage={}, remote={}",
                    MemInfo.directMemoryUsage(), MemInfo.heapMemoryUsage(), ctx.channel().remoteAddress());
                ctx.channel().config().setAutoRead(false);
                slowDownAt = nanoProvider.get();
                scheduleResumeRead();
                break;
            }
        }
        ctx.fireChannelRead(msg);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        if (slowDownConditions.stream().noneMatch(Condition::meet)) {
            ctx.channel().config().setAutoRead(true);
            ctx.read();
            slowDownAt = Long.MAX_VALUE;
        } else {
            closeIfNeeded();
        }
        ctx.fireChannelReadComplete();
    }

    private void scheduleResumeRead() {
        if (resumeTask == null || resumeTask.isDone()) {
            resumeTask = ctx.executor()
                .schedule(this::resumeRead, ThreadLocalRandom.current().nextLong(100, 1001), TimeUnit.MILLISECONDS);
        }
    }

    private void resumeRead() {
        if (slowDownConditions.stream().noneMatch(Condition::meet)) {
            if (!ctx.channel().config().isAutoRead()) {
                ctx.channel().config().setAutoRead(true);
                log.debug("Resume read: directMemoryUsage={}, heapMemoryUsage={}, remote={}",
                    MemInfo.directMemoryUsage(), MemInfo.heapMemoryUsage(), ctx.channel().remoteAddress());
                ctx.read();
                slowDownAt = Long.MAX_VALUE;
            }
        } else {
            if (!closeIfNeeded()) {
                resumeTask = null;
                scheduleResumeRead();
            }
        }
    }

    private boolean closeIfNeeded() {
        if (nanoProvider.get() - slowDownAt > MAX_SLOWDOWN_TIME) {
            ctx.close();
            for (Condition slowDownCondition : slowDownConditions) {
                if (slowDownCondition.meet()) {
                    if (slowDownCondition instanceof InboundResourceCondition) {
                        eventCollector.report(getLocal(OutOfTenantResource.class)
                            .reason(slowDownCondition.toString())
                            .clientInfo(clientInfo));
                    }
                    eventCollector.report(getLocal(ResourceThrottled.class)
                        .reason(slowDownCondition.toString())
                        .clientInfo(clientInfo));
                }
            }
            return true;
        }
        return false;
    }
}
