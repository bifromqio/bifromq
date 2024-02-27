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

package com.baidu.bifromq.mqtt;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.baserpc.utils.NettyUtil;
import com.baidu.bifromq.mqtt.handler.ByteBufToWebSocketFrameEncoder;
import com.baidu.bifromq.mqtt.handler.ChannelAttrs;
import com.baidu.bifromq.mqtt.handler.ClientAddrHandler;
import com.baidu.bifromq.mqtt.handler.ConnectionRateLimitHandler;
import com.baidu.bifromq.mqtt.handler.MQTTMessageDebounceHandler;
import com.baidu.bifromq.mqtt.handler.MQTTPreludeHandler;
import com.baidu.bifromq.mqtt.handler.SlowDownOnMemPressureHandler;
import com.baidu.bifromq.mqtt.handler.ws.WebSocketFrameToByteBufDecoder;
import com.baidu.bifromq.mqtt.session.MQTTSessionContext;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.netty4.NettyEventExecutorMetrics;
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
import lombok.extern.slf4j.Slf4j;

@Slf4j
abstract class AbstractMQTTBroker<T extends AbstractMQTTBrokerBuilder<T>> implements IMQTTBroker {
    private static final String MQTT_SUBPROTOCOL_CSV_LIST = "mqtt, mqttv3.1, mqttv3.1.1";
    private final T builder;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final ConnectionRateLimitHandler connRateLimitHandler;
    private final ClientAddrHandler remoteAddrHandler;
    private MQTTSessionContext sessionContext;
    private ChannelFuture tcpChannelF;
    private ChannelFuture tlsChannelF;
    private ChannelFuture wsChannelF;
    private ChannelFuture wssChannelF;

    public AbstractMQTTBroker(T builder) {
        this.builder = builder;
        this.bossGroup = NettyUtil.createEventLoopGroup(builder.mqttBossELGThreads,
            EnvProvider.INSTANCE.newThreadFactory("mqtt-boss-elg"));
        this.workerGroup = NettyUtil.createEventLoopGroup(builder.mqttWorkerELGThreads,
            EnvProvider.INSTANCE.newThreadFactory("mqtt-worker-elg"));
        connRateLimitHandler = new ConnectionRateLimitHandler(builder.connectRateLimit);
        remoteAddrHandler = new ClientAddrHandler();
        new NettyEventExecutorMetrics(bossGroup).bindTo(Metrics.globalRegistry);
        new NettyEventExecutorMetrics(workerGroup).bindTo(Metrics.globalRegistry);
    }

    protected void beforeBrokerStart() {

    }

    protected void afterBrokerStop() {

    }

    @Override
    public final void start() {
        try {
            sessionContext = MQTTSessionContext.builder()
                .serverId(builder.brokerId())
                .localSessionRegistry(builder.sessionRegistry)
                .localDistService(builder.distService)
                .authProvider(builder.authProvider)
                .resourceThrottler(builder.resourceThrottler)
                .eventCollector(builder.eventCollector)
                .settingProvider(builder.settingProvider)
                .distClient(builder.distClient)
                .inboxClient(builder.inboxClient)
                .retainClient(builder.retainClient)
                .sessionDictClient(builder.sessionDictClient)
                .defaultKeepAliveTimeSeconds(builder.defaultKeepAliveSeconds)
                .build();
            log.info("Starting MQTT broker");
            beforeBrokerStart();
            log.debug("Starting server channel");
            if (builder.tcpListenerBuilder != null) {
                tcpChannelF = this.bindTCPChannel(builder.tcpListenerBuilder);
                Channel channel = tcpChannelF.sync().channel();
                log.debug("Accepting mqtt connection over tcp channel at {}", channel.localAddress());
            }
            if (builder.tlsListenerBuilder != null) {
                tlsChannelF = this.bindTLSChannel(builder.tlsListenerBuilder);
                Channel channel = tlsChannelF.sync().channel();
                log.debug("Accepting mqtt connection over tls channel at {}", channel.localAddress());
            }
            if (builder.wsListenerBuilder != null) {
                wsChannelF = this.bindWSChannel(builder.wsListenerBuilder);
                Channel channel = wsChannelF.sync().channel();
                log.debug("Accepting mqtt connection over ws channel at {}", channel.localAddress());
            }
            if (builder.wssListenerBuilder != null) {
                wssChannelF = this.bindWSSChannel(builder.wssListenerBuilder);
                Channel channel = wssChannelF.sync().channel();
                log.debug("Accepting mqtt connection over wss channel at {}", channel.localAddress());
            }
            log.info("MQTT broker started");
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public final void shutdown() {
        log.info("Shutting down MQTT broker");
        if (tcpChannelF != null) {
            tcpChannelF.channel().close().syncUninterruptibly();
            log.debug("Stopped accepting mqtt connection over tcp channel");
        }
        if (tlsChannelF != null) {
            tlsChannelF.channel().close().syncUninterruptibly();
            log.debug("Stopped accepting mqtt connection over tls channel");
        }
        if (wsChannelF != null) {
            wsChannelF.channel().close().syncUninterruptibly();
            log.debug("Stopped accepting mqtt connection over ws channel");
        }
        if (wssChannelF != null) {
            wssChannelF.channel().close().syncUninterruptibly();
            log.debug("Stopped accepting mqtt connection over wss channel");
        }
        sessionContext.localSessionRegistry.disconnectAll(builder.disconnectRate).join();
        log.debug("All mqtt connection closed");

        sessionContext.awaitBgTasksFinish().join();
        log.debug("All background tasks done");

        bossGroup.shutdownGracefully().syncUninterruptibly();
        log.debug("Boss group shutdown");
        workerGroup.shutdownGracefully().syncUninterruptibly();
        log.debug("Worker group shutdown");
        afterBrokerStop();
        log.info("MQTT broker shutdown");
    }

    private ChannelFuture bindTCPChannel(ConnListenerBuilder.TCPConnListenerBuilder<T> connBuilder) {
        return buildChannel(connBuilder, new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast("connRateLimiter", connRateLimitHandler);
                pipeline.addLast("trafficShaper",
                    new ChannelTrafficShapingHandler(builder.writeLimit, builder.readLimit));
                pipeline.addLast(MqttEncoder.class.getName(), MqttEncoder.INSTANCE);
                // insert PacketFilter here
                pipeline.addLast(MqttDecoder.class.getName(), new MqttDecoder(builder.maxBytesInMessage));
                pipeline.addLast(MQTTMessageDebounceHandler.NAME, new MQTTMessageDebounceHandler());
                pipeline.addLast(SlowDownOnMemPressureHandler.NAME, new SlowDownOnMemPressureHandler());
                pipeline.addLast(MQTTPreludeHandler.NAME, new MQTTPreludeHandler(builder.connectTimeoutSeconds));
            }
        });
    }

    private ChannelFuture bindTLSChannel(ConnListenerBuilder.TLSConnListenerBuilder<T> connBuilder) {
        return buildChannel(connBuilder, new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast("connRateLimiter", connRateLimitHandler);
                pipeline.addLast("ssl", connBuilder.sslContext.newHandler(ch.alloc()));
                pipeline.addLast("trafficShaper",
                    new ChannelTrafficShapingHandler(builder.writeLimit, builder.readLimit));
                pipeline.addLast(MqttEncoder.class.getName(), MqttEncoder.INSTANCE);
                // insert PacketFilter here
                pipeline.addLast(MqttDecoder.class.getName(), new MqttDecoder(builder.maxBytesInMessage));
                pipeline.addLast(MQTTMessageDebounceHandler.NAME, new MQTTMessageDebounceHandler());
                pipeline.addLast(SlowDownOnMemPressureHandler.NAME, new SlowDownOnMemPressureHandler());
                pipeline.addLast(MQTTPreludeHandler.NAME, new MQTTPreludeHandler(builder.connectTimeoutSeconds));
            }
        });
    }

    private ChannelFuture bindWSChannel(ConnListenerBuilder.WSConnListenerBuilder<T> connBuilder) {
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
                pipeline.addLast(MqttEncoder.class.getName(), MqttEncoder.INSTANCE);
                // insert PacketFilter here
                pipeline.addLast(MqttDecoder.class.getName(), new MqttDecoder(builder.maxBytesInMessage));
                pipeline.addLast(MQTTMessageDebounceHandler.NAME, new MQTTMessageDebounceHandler());
                pipeline.addLast(SlowDownOnMemPressureHandler.NAME, new SlowDownOnMemPressureHandler());
                pipeline.addLast(MQTTPreludeHandler.NAME, new MQTTPreludeHandler(builder.connectTimeoutSeconds));
            }
        });
    }

    private ChannelFuture bindWSSChannel(ConnListenerBuilder.WSSConnListenerBuilder<T> connBuilder) {
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
                pipeline.addLast(MqttEncoder.class.getName(), MqttEncoder.INSTANCE);
                // insert PacketFilter between Encoder
                pipeline.addLast(MqttDecoder.class.getName(), new MqttDecoder(builder.maxBytesInMessage));
                pipeline.addLast(MQTTMessageDebounceHandler.NAME, new MQTTMessageDebounceHandler());
                pipeline.addLast(SlowDownOnMemPressureHandler.NAME, new SlowDownOnMemPressureHandler());
                pipeline.addLast(MQTTPreludeHandler.NAME, new MQTTPreludeHandler(builder.connectTimeoutSeconds));
            }
        });
    }

    private ChannelFuture buildChannel(ConnListenerBuilder<?, T> builder,
                                       final ChannelInitializer<?> channelInitializer) {
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
            .channel(NettyUtil.determineServerSocketChannelClass(bossGroup))
            .childHandler(channelInitializer)
            .childAttr(ChannelAttrs.MQTT_SESSION_CTX, sessionContext);
        builder.options.forEach((k, v) -> b.option((ChannelOption<? super Object>) k, v));
        builder.childOptions.forEach((k, v) -> b.childOption((ChannelOption<? super Object>) k, v));
        // Bind and start to accept incoming connections.
        return b.bind(builder.host, builder.port);
    }
}
