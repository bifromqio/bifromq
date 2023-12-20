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

package com.baidu.bifromq.mqtt.handler;

import com.google.common.util.concurrent.RateLimiter;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ChannelHandler.Sharable
public class ConnectionRateLimitHandler extends ChannelDuplexHandler {

    private final RateLimiter rateLimiter;

    public ConnectionRateLimitHandler(int rate) {
        rateLimiter = RateLimiter.create(rate);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        if (rateLimiter.tryAcquire()) {
            ctx.fireChannelActive();
        } else {
            log.warn("Connection dropped due to exceed limit");
            // close the connection randomly
            ctx.channel().config().setAutoRead(false);
            ctx.executor().schedule(() -> {
                if (ctx.channel().isActive()) {
                    ctx.close();
                }
            }, ThreadLocalRandom.current().nextLong(3000, 5000), TimeUnit.MILLISECONDS);
        }
    }
}
