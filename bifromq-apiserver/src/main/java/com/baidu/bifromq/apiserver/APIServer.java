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

package com.baidu.bifromq.apiserver;

import com.baidu.bifromq.apiserver.http.HTTPRouteMap;
import com.baidu.bifromq.apiserver.http.IHTTPRequestHandler;
import com.baidu.bifromq.apiserver.http.IHTTPRequestHandlersFactory;
import com.baidu.bifromq.apiserver.http.IHTTPRouteMap;
import com.baidu.bifromq.apiserver.http.handler.RequestHandlersFactory;
import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basekv.metaservice.IBaseKVMetaService;
import com.baidu.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import com.baidu.bifromq.baserpc.utils.NettyUtil;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.retain.client.IRetainClient;
import com.baidu.bifromq.sessiondict.client.ISessionDictClient;
import com.google.common.base.Preconditions;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollMode;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class APIServer implements IAPIServer {
    enum State {
        INIT, STARTING, STARTED, STARTING_FAILED, STOPPING, STOPPED
    }

    private final String host;
    private final int port;
    private final int tlsPort;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final ServerBootstrap serverBootstrap;
    private final ServerBootstrap tlsServerBootstrap;
    private final Collection<IHTTPRequestHandler> handlers;
    private final AtomicReference<State> state = new AtomicReference<>(State.INIT);
    private Channel serverChannel;
    private Channel tlsServerChannel;

    @Builder
    private APIServer(String host,
                      int port,
                      int tlsPort,
                      int maxContentLength,
                      int workerThreads,
                      SslContext sslContext,
                      IRPCServiceTrafficService trafficService,
                      IBaseKVMetaService metaService,
                      IAgentHost agentHost,
                      IDistClient distClient,
                      IInboxClient inboxClient,
                      ISessionDictClient sessionDictClient,
                      IRetainClient retainClient,
                      ISettingProvider settingProvider) {
        Preconditions.checkArgument(port >= 0);
        Preconditions.checkArgument(tlsPort >= 0);
        this.host = host;
        this.port = port;
        this.tlsPort = tlsPort;
        this.bossGroup = NettyUtil.createEventLoopGroup(1,
            EnvProvider.INSTANCE.newThreadFactory("api-server-boss-elg"));
        this.workerGroup = NettyUtil.createEventLoopGroup(workerThreads,
            EnvProvider.INSTANCE.newThreadFactory("api-server-worker-elg"));
        IHTTPRequestHandlersFactory handlersFactory = new RequestHandlersFactory(agentHost,
            trafficService,
            metaService,
            sessionDictClient,
            distClient,
            inboxClient,
            retainClient,
            settingProvider);
        this.handlers = handlersFactory.build();
        IHTTPRouteMap routeMap = new HTTPRouteMap(handlers);
        this.serverBootstrap =
            buildServerChannel(new NonTLSServerInitializer(routeMap, settingProvider, maxContentLength));
        if (sslContext != null) {
            this.tlsServerBootstrap = buildServerChannel(
                new TLSServerInitializer(sslContext, routeMap, settingProvider, maxContentLength));
        } else {
            this.tlsServerBootstrap = null;
        }
    }

    @Override
    public String host() {
        checkStarted();
        return host;
    }

    @Override
    public int listeningPort() {
        checkStarted();
        return ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    @Override
    public Optional<Integer> listeningTlsPort() {
        checkStarted();
        if (tlsServerChannel != null) {
            return Optional.of(((InetSocketAddress) tlsServerChannel.localAddress()).getPort());
        }
        return Optional.empty();
    }

    @Override
    public void start() {
        if (state.compareAndSet(State.INIT, State.STARTING)) {
            try {
                handlers.forEach(IHTTPRequestHandler::start);
                log.info("Starting API server");
                this.serverChannel = serverBootstrap.bind(host, port).sync().channel();
                log.debug("Accepting API request at {}", serverChannel.localAddress());
                if (tlsServerBootstrap != null) {
                    this.tlsServerChannel = tlsServerBootstrap.bind(host, tlsPort).sync().channel();
                    log.debug("Accepting API request at {}", tlsServerChannel.localAddress());
                }
                log.info("API server started");
                state.set(State.STARTED);
            } catch (InterruptedException e) {
                state.set(State.STARTING_FAILED);
                throw new IllegalStateException(e);
            }
        }
    }

    @Override
    public void close() {
        if (state.compareAndSet(State.STARTED, State.STOPPING)) {
            log.info("Stopping API server");
            serverChannel.close().syncUninterruptibly();
            if (tlsServerChannel != null) {
                serverChannel.close().syncUninterruptibly();
            }
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
            handlers.forEach(IHTTPRequestHandler::close);
            log.debug("API server stopped");
            state.set(State.STOPPED);
        }
    }

    private ServerBootstrap buildServerChannel(ChannelInitializer<SocketChannel> channelInitializer) {
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
            // TODO - externalize following configs if needed
            .option(ChannelOption.SO_BACKLOG, 128)
            .option(ChannelOption.SO_REUSEADDR, true)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .channel(NettyUtil.determineServerSocketChannelClass(bossGroup))
            .childHandler(channelInitializer);
        if (Epoll.isAvailable()) {
            b.option(EpollChannelOption.EPOLL_MODE, EpollMode.EDGE_TRIGGERED);
        }
        return b;
    }

    private void checkStarted() {
        Preconditions.checkState(state.get() == State.STARTED, "APIServer not started");
    }
}
