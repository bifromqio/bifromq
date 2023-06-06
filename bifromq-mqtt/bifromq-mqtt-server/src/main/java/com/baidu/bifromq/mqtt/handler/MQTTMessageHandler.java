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

package com.baidu.bifromq.mqtt.handler;

import com.baidu.bifromq.baserpc.utils.FutureTracker;
import com.baidu.bifromq.mqtt.handler.event.ConnectionWillClose;
import com.baidu.bifromq.mqtt.session.MQTTSessionContext;
import com.baidu.bifromq.plugin.authprovider.IAuthProvider;
import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.util.concurrent.Future;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@Slf4j
public abstract class MQTTMessageHandler extends ChannelDuplexHandler {
    private static final int DEFAULT_FLUSH_AFTER_FLUSHES = 64;
    private final int explicitFlushAfterFlushes;
    private final FutureTracker cancelOnInactiveTasks = new FutureTracker();
    private final FutureTracker tearDownTasks = new FutureTracker();
    private final Runnable flushTask;
    private ScheduledFuture<?> scheduledClose;
    private Event closeReason;
    private int flushPendingCount;
    private Future<?> nextScheduledFlush;
    protected MQTTSessionContext sessionCtx;

    protected IAuthProvider authProvider;

    protected IEventCollector eventCollector;

    protected ISettingProvider settingProvider;

    protected ChannelHandlerContext ctx;

    protected MQTTMessageHandler() {
        this(DEFAULT_FLUSH_AFTER_FLUSHES);
    }

    protected MQTTMessageHandler(int explicitFlushAfterFlushes) {
        this.explicitFlushAfterFlushes = explicitFlushAfterFlushes;
        this.flushTask = () -> {
            if (flushPendingCount > 0) {
                flushPendingCount = 0;
                nextScheduledFlush = null;
                ctx.flush();
            }
        };
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.ctx = ctx;
        sessionCtx = ChannelAttrs.mqttSessionContext(ctx);
        authProvider = sessionCtx.authProvider(ctx);
        eventCollector = sessionCtx.eventCollector;
        settingProvider = sessionCtx.settingsProvider;
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        flushIfNeeded(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        cancelOnInactiveTasks.stop();
        cancelIfUndone(scheduledClose);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // To ensure we not miss to flush anything, do it now.
        flushIfNeeded(ctx);
        ctx.fireExceptionCaught(cause);
    }

    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        flushIfNeeded(ctx);
        ctx.disconnect(promise);
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        flushIfNeeded(ctx);
        ctx.close(promise);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        if (!ctx.channel().isWritable()) {
            flushIfNeeded(ctx);
        }
        ctx.fireChannelWritabilityChanged();
    }

    protected ChannelFuture writeAndFlush(Object msg) {
        ChannelFuture future = ctx.write(msg);
        flush(false);
        return future;
    }

    protected void flush(boolean immediately) {
        if (++flushPendingCount == explicitFlushAfterFlushes || immediately) {
            flushNow(ctx);
        } else {
            scheduleFlush(ctx);
        }
    }

    protected final void resumeChannelRead() {
        // resume reading
        ctx.channel().config().setAutoRead(true);
        ctx.read();
    }

    protected final void addTearDownHook(Runnable runnable) {
        tearDownTasks.whenComplete((v, e) -> runnable.run());
    }

    protected final <T> CompletableFuture<T> cancelOnInactive(CompletableFuture<T> trackedFuture) {
        return cancelOnInactiveTasks.track(trackedFuture);
    }

    protected final <T> CompletableFuture<T> tearDownTasks(CompletableFuture<T> trackedFuture) {
        return tearDownTasks.track(trackedFuture);
    }

    protected final void submitBgTask(Supplier<CompletableFuture<Void>> bgTaskSupplier) {
        sessionCtx.addBgTask(bgTaskSupplier);
    }

    protected void cancelIfUndone(ScheduledFuture<?> task) {
        if (task != null && !task.isDone() && !task.isCancelled()) {
            task.cancel(true);
        }
    }

    protected boolean closeNotScheduled() {
        return scheduledClose == null;
    }

    protected void closeConnectionWithSomeDelay(@NonNull Event reason) {
        closeConnectionWithSomeDelay(null, reason);
    }

    protected void closeConnectionWithSomeDelay(MqttMessage farewell, @NonNull Event reason) {
        // must be called in event loop
        assert ctx.channel().eventLoop().inEventLoop();
        if (closeNotScheduled() && ctx.channel().isActive()) {
            // stop reading messages
            ctx.channel().config().setAutoRead(false);
            closeReason = reason;
            eventCollector.report(reason);
            ctx.pipeline().fireUserEventTriggered(new ConnectionWillClose(reason));
            scheduledClose = ctx.channel().eventLoop().schedule(() ->
                    farewellAndClose(farewell), randomDelay(), TimeUnit.MILLISECONDS);
        }
    }

    protected void closeConnectionNow(MqttMessage farewell, @NonNull Event reason) {
        assert ctx.channel().eventLoop().inEventLoop();
        if (ctx.channel().isActive()) {
            // stop reading messages
            ctx.channel().config().setAutoRead(false);
            cancelIfUndone(scheduledClose);
            // don't override first close reason
            if (closeReason == null) {
                closeReason = reason;
                eventCollector.report(reason);
                ctx.pipeline().fireUserEventTriggered(new ConnectionWillClose(reason));
            }
            farewellAndClose(farewell);
        }
    }

    protected void closeConnectionNow(@NonNull Event reason) {
        closeConnectionNow(null, reason);
    }

    private void farewellAndClose(MqttMessage farewell) {
        if (farewell != null) {
            ctx.writeAndFlush(farewell).addListener(ChannelFutureListener.CLOSE);
        } else if (ctx.channel().isActive()) {
            ctx.channel().close();
        }
    }

    private long randomDelay() {
        return ThreadLocalRandom.current().nextLong(1000, 4000);
    }

    private void flushIfNeeded(ChannelHandlerContext ctx) {
        if (flushPendingCount > 0) {
            flushNow(ctx);
        }
    }

    private void flushNow(ChannelHandlerContext ctx) {
        cancelScheduledFlush();
        flushPendingCount = 0;
        ctx.flush();
    }

    private void scheduleFlush(final ChannelHandlerContext ctx) {
        if (nextScheduledFlush == null) {
            // Run as soon as possible, but still yield to give a chance for additional writes to enqueue.
            nextScheduledFlush = ctx.channel().eventLoop().submit(flushTask);
        }
    }

    private void cancelScheduledFlush() {
        if (nextScheduledFlush != null) {
            nextScheduledFlush.cancel(false);
            nextScheduledFlush = null;
        }
    }
}
