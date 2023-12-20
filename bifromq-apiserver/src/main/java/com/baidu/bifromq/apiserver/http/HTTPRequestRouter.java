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

import static com.baidu.bifromq.apiserver.Headers.HEADER_TENANT_ID;
import static com.baidu.bifromq.apiserver.http.handler.HTTPHeaderUtils.getHeader;
import static io.netty.buffer.Unpooled.EMPTY_BUFFER;
import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpHeaderValues.KEEP_ALIVE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_0;

import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerUpgradeHandler;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HTTPRequestRouter extends SimpleChannelInboundHandler<HttpRequest> {
    private final IHTTPRouteMap routeMap;
    private final ISettingProvider settingProvider;

    private String lastTenantId;

    public HTTPRequestRouter(IHTTPRouteMap routeMap, ISettingProvider settingProvider) {
        this.routeMap = routeMap;
        this.settingProvider = settingProvider;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof HttpServerUpgradeHandler.UpgradeEvent) {
            ctx.pipeline().remove(this);
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpRequest req) {
        ChannelPipeline pipeline = ctx.pipeline();
        String tenantId = getHeader(HEADER_TENANT_ID, req, false);
        if (tenantId != null && (lastTenantId == null || !lastTenantId.equals(tenantId))) {
            lastTenantId = tenantId;
            // setup aggregator at the first time or tenant change
            pipeline.addAfter(ctx.name(), null, new HTTPRouteHandler(tenantId, routeMap));
            pipeline.addAfter(ctx.name(), null,
                new HttpObjectAggregator(settingProvider.provide(Setting.MaxUserPayloadBytes, tenantId)));
        }
        if (tenantId != null || lastTenantId != null) {
            ReferenceCountUtil.retain(req);
            ctx.fireChannelRead(req);
        } else {
            FullHttpResponse response =
                new DefaultFullHttpResponse(req.protocolVersion(), HttpResponseStatus.BAD_REQUEST, EMPTY_BUFFER);
            response.headers().set(CONTENT_TYPE, "text/plain; charset=UTF-8");
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
        }
    }
}
