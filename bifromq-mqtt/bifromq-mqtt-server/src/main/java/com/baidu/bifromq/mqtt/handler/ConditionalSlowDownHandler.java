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

import com.baidu.bifromq.mqtt.utils.MemInfo;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConditionalSlowDownHandler extends ChannelInboundHandlerAdapter {
    public static final String NAME = "SlowDownHandler";
    private final Supplier<Boolean> slowDownCondition;
    private ChannelHandlerContext ctx;
    private ScheduledFuture<?> resumeTask;

    public ConditionalSlowDownHandler(Supplier<Boolean> slowDownCondition) {
        this.slowDownCondition = slowDownCondition;
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
        if (slowDownCondition.get()) {
            log.debug("Stop read: directMemoryUsage={}, heapMemoryUsage={}, remote={}",
                MemInfo.directMemoryUsage(), MemInfo.heapMemoryUsage(), ctx.channel().remoteAddress());
            ctx.channel().config().setAutoRead(false);
            scheduleResumeRead();
        }
        ctx.fireChannelRead(msg);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        if (!slowDownCondition.get()) {
            ctx.channel().config().setAutoRead(true);
            ctx.read();
        }
        ctx.fireChannelReadComplete();
    }

    private void scheduleResumeRead() {
        if (resumeTask == null || resumeTask.isDone()) {
            resumeTask = ctx.executor()
                .schedule(this::resumeRead, ThreadLocalRandom.current().nextLong(100, 1000), TimeUnit.MILLISECONDS);
        }
    }

    private void resumeRead() {
        if (!slowDownCondition.get()) {
            if (!ctx.channel().config().isAutoRead()) {
                ctx.channel().config().setAutoRead(true);
                log.debug("Resume read: directMemoryUsage={}, heapMemoryUsage={}, remote={}",
                    MemInfo.directMemoryUsage(), MemInfo.heapMemoryUsage(), ctx.channel().remoteAddress());
                ctx.read();
            }
        } else {
            resumeTask = null;
            scheduleResumeRead();
        }
    }
}
