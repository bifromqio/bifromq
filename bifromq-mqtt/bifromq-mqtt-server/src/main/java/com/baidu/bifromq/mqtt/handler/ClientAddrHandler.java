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

import com.google.common.base.Strings;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpRequest;
import java.net.InetSocketAddress;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ChannelHandler.Sharable
public class ClientAddrHandler extends ChannelInboundHandlerAdapter {

    private static final String X_REAL_IP = "X-Real-IP";

    private static final String X_REAL_PORT = "X-Real-Port";


    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof HttpRequest) {
            HttpRequest req = (HttpRequest) msg;
            String realIP = req.headers().get(X_REAL_IP);
            String realPort = req.headers().get(X_REAL_PORT);

            if (!Strings.isNullOrEmpty(realIP) && !Strings.isNullOrEmpty(realPort)) {
                int port = 0;
                try {
                    port = Integer.parseInt(realPort);
                } catch (Exception e) {
                    log.warn("parseInt port fail, realPort: {}, use default port: 0", realPort);
                }
                ChannelAttrs.socketAddress(ctx, new InetSocketAddress(realIP, port));
            }
        }
        ctx.fireChannelRead(msg);
    }
}
