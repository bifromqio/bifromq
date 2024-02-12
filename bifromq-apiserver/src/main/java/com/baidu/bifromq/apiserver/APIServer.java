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
import com.baidu.bifromq.apiserver.http.IHTTPRouteMap;
import com.baidu.bifromq.apiserver.http.handler.HTTPRequestHandlersFactory;
import com.baidu.bifromq.baserpc.utils.NettyUtil;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.retain.client.IRetainClient;
import com.baidu.bifromq.sessiondict.client.ISessionDictClient;
import com.google.common.base.Preconditions;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollMode;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class APIServer implements IAPIServer {
    enum State {
        INIT, STARTING, STARTED, STARTING_FAILED, STOPPING, STOPPED
    }

    private final String host;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final ChannelFuture serverChannel;
    private final ChannelFuture tlsServerChannel;
    private final AtomicReference<State> state = new AtomicReference<>(State.INIT);

    public APIServer(String host,
                     int port,
                     int tlsPort,
                     EventLoopGroup bossGroup,
                     EventLoopGroup workerGroup,
                     SslContext sslContext,
                     IDistClient distClient,
                     IInboxClient inboxClient,
                     ISessionDictClient sessionDictClient,
                     IRetainClient retainClient,
                     ISettingProvider settingProvider) {
        Preconditions.checkArgument(port >= 0);
        Preconditions.checkArgument(tlsPort >= 0);
        this.host = host;
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        IHTTPRouteMap routeMap = new HTTPRouteMap(new HTTPRequestHandlersFactory(sessionDictClient,
            distClient, inboxClient, retainClient, settingProvider));
        this.serverChannel =
            buildServerChannel(port, new NonTLSServerInitializer(routeMap, settingProvider));
        if (sslContext != null) {
            this.tlsServerChannel =
                buildServerChannel(tlsPort, new TLSServerInitializer(sslContext, routeMap, settingProvider));
        } else {
            this.tlsServerChannel = null;
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
        return ((InetSocketAddress) serverChannel.channel().localAddress()).getPort();
    }

    @Override
    public Optional<Integer> listeningTlsPort() {
        checkStarted();
        if (tlsServerChannel != null) {
            return Optional.of(((InetSocketAddress) tlsServerChannel.channel().localAddress()).getPort());
        }
        return Optional.empty();
    }

    @Override
    public void start() {
        if (state.compareAndSet(State.INIT, State.STARTING)) {
            try {
                log.info("Starting API server");
                Channel channel = serverChannel.sync().channel();
                log.debug("Accepting API request at {}", channel.localAddress());
                if (tlsServerChannel != null) {
                    channel = tlsServerChannel.sync().channel();
                    log.debug("Accepting API request at {}", channel.localAddress());
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
    public void shutdown() {
        if (state.compareAndSet(State.STARTED, State.STOPPING)) {
            log.info("Shutting down API server");
            serverChannel.channel().close().syncUninterruptibly();
            if (tlsServerChannel != null) {
                serverChannel.channel().close().syncUninterruptibly();
            }
            log.info("API server shutdown");
            state.set(State.STOPPED);
        }
    }

    private ChannelFuture buildServerChannel(int port, ChannelInitializer<SocketChannel> channelInitializer) {
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
        // Bind and start to accept incoming connections.
        return b.bind(host, port);
    }

    private void checkStarted() {
        Preconditions.checkState(state.get() == State.STARTED, "APIServer not started");
    }
}
