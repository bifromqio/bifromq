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

import com.google.common.base.Preconditions;
import io.netty.channel.ChannelOption;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollMode;
import io.netty.handler.ssl.SslContext;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.NonNull;

abstract class ConnListenerBuilder<C extends ConnListenerBuilder> {
    protected final Map<ChannelOption<?>, Object> options = new LinkedHashMap<>();
    protected final Map<ChannelOption<?>, Object> childOptions = new LinkedHashMap<>();
    private final MQTTBrokerBuilder serverBuilder;
    protected int port;

    ConnListenerBuilder(MQTTBrokerBuilder builder) {
        serverBuilder = builder;
        options.put(ChannelOption.SO_BACKLOG, 128);
        options.put(ChannelOption.SO_REUSEADDR, true);
        if (Epoll.isAvailable()) {
            options.put(EpollChannelOption.EPOLL_MODE, EpollMode.EDGE_TRIGGERED);
        }
        childOptions.put(ChannelOption.TCP_NODELAY, true);

    }

    public C port(int port) {
        Preconditions.checkArgument(port > 0, "port");
        this.port = port;
        return (C) this;
    }

    public <T> C option(ChannelOption<T> option, T value) {
        Preconditions.checkNotNull(option, "option");
        if (value == null) {
            options.remove(option);
        } else {
            options.put(option, value);
        }
        return (C) this;
    }

    public <T> C childOption(ChannelOption<T> option, T value) {
        Preconditions.checkNotNull(option, "option");
        if (value == null) {
            childOptions.remove(option);
        } else {
            childOptions.put(option, value);
        }
        return (C) this;
    }

    public <M extends MQTTBrokerBuilder> M buildListener() {
        return (M) serverBuilder;
    }

    public static class TCPConnListenerBuilder extends ConnListenerBuilder<TCPConnListenerBuilder> {
        TCPConnListenerBuilder(MQTTBrokerBuilder builder) {
            super(builder);
            port(1883);
        }
    }

    private abstract static class SecuredConnListenerBuilder
        <L extends SecuredConnListenerBuilder> extends ConnListenerBuilder<L> {
        protected SslContext sslContext;

        SecuredConnListenerBuilder(MQTTBrokerBuilder builder) {
            super(builder);
        }

        public L sslContext(@NonNull SslContext sslContext) {
            Preconditions.checkArgument(sslContext.isServer());
            this.sslContext = sslContext;
            return (L) this;
        }
    }

    public static final class TLSConnListenerBuilder extends SecuredConnListenerBuilder<TLSConnListenerBuilder> {

        TLSConnListenerBuilder(MQTTBrokerBuilder builder) {
            super(builder);
            port(8883);
        }
    }

    public static final class WSConnListenerBuilder extends ConnListenerBuilder<WSConnListenerBuilder> {
        private String path = "mqtt";

        WSConnListenerBuilder(MQTTBrokerBuilder builder) {
            super(builder);
        }

        public String path() {
            return path;
        }

        public WSConnListenerBuilder path(String path) {
            this.path = path;
            return this;
        }
    }

    public static final class WSSConnListenerBuilder extends SecuredConnListenerBuilder<WSSConnListenerBuilder> {
        private String path;

        WSSConnListenerBuilder(MQTTBrokerBuilder builder) {
            super(builder);
        }

        public String path() {
            return path;
        }

        public WSSConnListenerBuilder path(String path) {
            this.path = path;
            return this;
        }
    }
}
