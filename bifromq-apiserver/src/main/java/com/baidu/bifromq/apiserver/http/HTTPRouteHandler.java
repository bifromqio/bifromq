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

package com.baidu.bifromq.apiserver.http;

import static com.baidu.bifromq.apiserver.Headers.HEADER_REQ_ID;
import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpHeaderValues.KEEP_ALIVE;
import static io.netty.handler.codec.http.HttpResponseStatus.CONTINUE;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_0;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import com.baidu.bifromq.apiserver.http.handler.HTTPHeaderUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpUtil;

class HTTPRouteHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    private final String tenantId;
    private final IHTTPRouteMap routeMap;

    HTTPRouteHandler(String tenantId, IHTTPRouteMap routeMap) {
        this.tenantId = tenantId;
        this.routeMap = routeMap;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) {
        if (HttpUtil.is100ContinueExpected(req)) {
            ctx.write(new DefaultFullHttpResponse(HTTP_1_1, CONTINUE, Unpooled.EMPTY_BUFFER));
        }
        req.retain();
        long reqId = HTTPHeaderUtils.getOptionalReqId(req);
        routeMap.getHandler(req)
            .handle(reqId, tenantId, req)
            .whenComplete((v, e) -> {
                FullHttpResponse response;
                if (e != null) {
                    ByteBuf content = ctx.alloc().buffer();
                    content.writeBytes(e.getMessage().getBytes());
                    response = new DefaultFullHttpResponse(HTTP_1_1, INTERNAL_SERVER_ERROR, content);
                    response.headers().set(CONTENT_TYPE, "text/plain; charset=UTF-8");
                } else {
                    response = v;
                }
                response.headers().set(HEADER_REQ_ID.header, reqId);
                response.headers().setInt(CONTENT_LENGTH, response.content().readableBytes());
                boolean keepAlive = HttpUtil.isKeepAlive(req);
                if (keepAlive) {
                    if (req.protocolVersion().equals(HTTP_1_0)) {
                        response.headers().set(CONNECTION, KEEP_ALIVE);
                    }
                    ctx.writeAndFlush(response);
                } else {
                    // Tell the client we're going to close the connection.
                    response.headers().set(CONNECTION, CLOSE);
                    ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
                }
                req.release();
            });
    }
}
