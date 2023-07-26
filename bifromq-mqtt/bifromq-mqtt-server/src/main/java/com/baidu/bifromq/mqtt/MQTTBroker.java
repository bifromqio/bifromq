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

package com.baidu.bifromq.mqtt;

import com.baidu.bifromq.baserpc.utils.NettyUtil;
import com.baidu.bifromq.mqtt.handler.ByteBufToWebSocketFrameEncoder;
import com.baidu.bifromq.mqtt.handler.ChannelAttrs;
import com.baidu.bifromq.mqtt.handler.ClientAddrHandler;
import com.baidu.bifromq.mqtt.handler.ConnectionRateLimitHandler;
import com.baidu.bifromq.mqtt.handler.MQTTConnectHandler;
import com.baidu.bifromq.mqtt.handler.MQTTMessageDebounceHandler;
import com.baidu.bifromq.mqtt.handler.ws.WebSocketFrameToByteBufDecoder;
import com.baidu.bifromq.mqtt.service.ILocalSessionBrokerServer;
import com.baidu.bifromq.mqtt.session.MQTTSessionContext;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class MQTTBroker implements IMQTTBroker {
    private static final String MQTT_SUBPROTOCOL_CSV_LIST = "mqtt, mqttv3.1, mqttv3.1.1";
    private final MQTTBrokerBuilder builder;
    private final String mqttHost;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    // fields of the returned anonymous MQTTBroker instance
    private final ConnectionRateLimitHandler connRateLimitHandler;
    private final ClientAddrHandler remoteAddrHandler;
    private final MQTTSessionContext sessionContext;
    private final ILocalSessionBrokerServer subBroker;
    private final Optional<ChannelFuture> tcpChannelF;
    private final Optional<ChannelFuture> tlsChannelF;
    private final Optional<ChannelFuture> wsChannelF;
    private final Optional<ChannelFuture> wssChannelF;

    public MQTTBroker(MQTTBrokerBuilder builder) {
        this.builder = builder;
        this.mqttHost = builder.mqttHost;
        this.bossGroup = builder.bossGroup;
        this.workerGroup = builder.workerGroup;
        connRateLimitHandler = new ConnectionRateLimitHandler(builder.connectRateLimit);
        remoteAddrHandler = new ClientAddrHandler();
        subBroker = ILocalSessionBrokerServer.newBuilder()
            .id(builder.serverId)
            .host(builder.rpcHost)
            .port(builder.rpcPort)
            .executor(builder.ioExecutor)
            .bossEventLoopGroup(bossGroup)
            .workerEventLoopGroup(builder.rpcWorkerGroup)
            .crdtService(builder.crdtService)
            .build();
        sessionContext = MQTTSessionContext.builder()
            .authProvider(builder.authProvider)
            .eventCollector(builder.eventCollector)
            .settingProvider(builder.settingProvider)
            .distClient(builder.distClient)
            .inboxClient(builder.inboxClient)
            .retainClient(builder.retainClient)
            .sessionDictClient(builder.sessionDictClient)
            .brokerServer(subBroker)
            .maxResendTimes(builder.maxResendTimes)
            .resendDelayMillis(builder.resendDelayMillis)
            .defaultKeepAliveTimeSeconds(builder.defaultKeepAliveSeconds)
            .qos2ConfirmWindowSeconds(builder.qos2ConfirmWindowSeconds)
            .build();
        tcpChannelF = builder.tcpListenerBuilder.map(this::bindTCPChannel);
        tlsChannelF = builder.tlsListenerBuilder.map(this::bindTLSChannel);
        wsChannelF = builder.wsListenerBuilder.map(this::bindWSChannel);
        wssChannelF = builder.wssListenerBuilder.map(this::bindWSSChannel);
    }

    @Override
    public void start() {
        try {
            log.info("Starting MQTT broker");
            log.debug("Starting sub broker server");
            subBroker.start();
            log.debug("Starting server channel");
            if (tcpChannelF.isPresent()) {
                Channel channel = tcpChannelF.get().sync().channel();
                log.debug("Accepting mqtt connection over tcp channel at {}", channel.localAddress());
            }
            if (tlsChannelF.isPresent()) {
                Channel channel = tlsChannelF.get().sync().channel();
                log.debug("Accepting mqtt connection over tls channel at {}", channel.localAddress());
            }
            if (wsChannelF.isPresent()) {
                Channel channel = wsChannelF.get().sync().channel();
                log.debug("Accepting mqtt connection over ws channel at {}", channel.localAddress());
            }
            if (wssChannelF.isPresent()) {
                Channel channel = wssChannelF.get().sync().channel();
                log.debug("Accepting mqtt connection over wss channel at {}", channel.localAddress());
            }
            log.info("MQTT broker started");
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void shutdown() {
        log.info("Shutting down MQTT broker");
        if (tcpChannelF.isPresent()) {
            tcpChannelF.get().channel().close().syncUninterruptibly();
            log.debug("Stopped accepting mqtt connection over tcp channel");
        }
        if (tlsChannelF.isPresent()) {
            tlsChannelF.get().channel().close().syncUninterruptibly();
            log.debug("Stopped accepting mqtt connection over tls channel");
        }
        if (wsChannelF.isPresent()) {
            wsChannelF.get().channel().close().syncUninterruptibly();
            log.debug("Stopped accepting mqtt connection over ws channel");
        }
        if (wssChannelF.isPresent()) {
            wssChannelF.get().channel().close().syncUninterruptibly();
            log.debug("Stopped accepting mqtt connection over wss channel");
        }
        subBroker.disconnectAll(builder.disconnectRate).join();
        log.debug("All mqtt connection closed");

        sessionContext.awaitBgTaskDone();
        log.debug("All background tasks done");

        subBroker.shutdown();
        log.debug("Transient session broker shutdown");

        bossGroup.shutdownGracefully().syncUninterruptibly();
        log.debug("Boss group shutdown");
        workerGroup.shutdownGracefully().syncUninterruptibly();
        log.debug("Worker group shutdown");
        log.info("MQTT broker shutdown");
    }

    private ChannelFuture bindTCPChannel(ConnListenerBuilder.TCPConnListenerBuilder connBuilder) {
        return buildChannel(connBuilder, new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast("connRateLimiter", connRateLimitHandler);
                pipeline.addLast("trafficShaper",
                    new ChannelTrafficShapingHandler(builder.writeLimit, builder.readLimit));
                pipeline.addLast("decoder", new MqttDecoder(builder.maxBytesInMessage));
                pipeline.addLast("encoder", MqttEncoder.INSTANCE);
                pipeline.addLast(MQTTMessageDebounceHandler.NAME, new MQTTMessageDebounceHandler());
                pipeline.addLast(MQTTConnectHandler.NAME, new MQTTConnectHandler(builder.connectTimeoutSeconds));
            }
        });
    }

    private ChannelFuture bindTLSChannel(ConnListenerBuilder.TLSConnListenerBuilder connBuilder) {
        return buildChannel(connBuilder, new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast("connRateLimiter", connRateLimitHandler);
                pipeline.addLast("ssl", connBuilder.sslContext.newHandler(ch.alloc()));
                pipeline.addLast("trafficShaper",
                    new ChannelTrafficShapingHandler(builder.writeLimit, builder.readLimit));
                pipeline.addLast("decoder", new MqttDecoder(builder.maxBytesInMessage));
                pipeline.addLast("encoder", MqttEncoder.INSTANCE);
                pipeline.addLast(MQTTMessageDebounceHandler.NAME, new MQTTMessageDebounceHandler());
                pipeline.addLast(MQTTConnectHandler.NAME, new MQTTConnectHandler(builder.connectTimeoutSeconds));
            }
        });
    }

    private ChannelFuture bindWSChannel(ConnListenerBuilder.WSConnListenerBuilder connBuilder) {
        return buildChannel(connBuilder, new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast("connRateLimiter", connRateLimitHandler);
                pipeline.addLast("trafficShaper",
                    new ChannelTrafficShapingHandler(builder.writeLimit, builder.readLimit));
                pipeline.addLast("httpEncoder", new HttpResponseEncoder());
                pipeline.addLast("httpDecoder", new HttpRequestDecoder());
                pipeline.addLast("remoteAddr", remoteAddrHandler);
                pipeline.addLast("aggregator", new HttpObjectAggregator(65536));
                pipeline.addLast("webSocketHandler", new WebSocketServerProtocolHandler(connBuilder.path(),
                    MQTT_SUBPROTOCOL_CSV_LIST));
                pipeline.addLast("ws2bytebufDecoder", new WebSocketFrameToByteBufDecoder());
                pipeline.addLast("bytebuf2wsEncoder", new ByteBufToWebSocketFrameEncoder());
                pipeline.addLast("decoder", new MqttDecoder(builder.maxBytesInMessage));
                pipeline.addLast("encoder", MqttEncoder.INSTANCE);
                pipeline.addLast(MQTTMessageDebounceHandler.NAME, new MQTTMessageDebounceHandler());
                pipeline.addLast(MQTTConnectHandler.NAME, new MQTTConnectHandler(builder.connectTimeoutSeconds));
            }
        });
    }

    private ChannelFuture bindWSSChannel(ConnListenerBuilder.WSSConnListenerBuilder connBuilder) {
        return buildChannel(connBuilder, new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast("connRateLimiter", connRateLimitHandler);
                pipeline.addLast("ssl", connBuilder.sslContext.newHandler(ch.alloc()));
                pipeline.addLast("trafficShaper",
                    new ChannelTrafficShapingHandler(builder.writeLimit, builder.readLimit));
                pipeline.addLast("httpEncoder", new HttpResponseEncoder());
                pipeline.addLast("httpDecoder", new HttpRequestDecoder());
                pipeline.addLast("remoteAddr", remoteAddrHandler);
                pipeline.addLast("aggregator", new HttpObjectAggregator(65536));
                pipeline.addLast("webSocketHandler", new WebSocketServerProtocolHandler(connBuilder.path(),
                    MQTT_SUBPROTOCOL_CSV_LIST));
                pipeline.addLast("ws2bytebufDecoder", new WebSocketFrameToByteBufDecoder());
                pipeline.addLast("bytebuf2wsEncoder", new ByteBufToWebSocketFrameEncoder());
                pipeline.addLast("decoder", new MqttDecoder(builder.maxBytesInMessage));
                pipeline.addLast("encoder", MqttEncoder.INSTANCE);
                pipeline.addLast(MQTTMessageDebounceHandler.NAME, new MQTTMessageDebounceHandler());
                pipeline.addLast(MQTTConnectHandler.NAME, new MQTTConnectHandler(builder.connectTimeoutSeconds));
            }
        });
    }

    private ChannelFuture buildChannel(ConnListenerBuilder<?> builder, final ChannelInitializer<?> channelInitializer) {
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
            .channel(NettyUtil.determineServerSocketChannelClass(bossGroup))
            .childHandler(channelInitializer)
            .childAttr(ChannelAttrs.MQTT_SESSION_CTX, sessionContext);
        builder.options.forEach((k, v) -> b.option((ChannelOption<? super Object>) k, v));
        builder.childOptions.forEach((k, v) -> b.childOption((ChannelOption<? super Object>) k, v));
        // Bind and start to accept incoming connections.
        return b.bind(mqttHost, builder.port);
    }
}
