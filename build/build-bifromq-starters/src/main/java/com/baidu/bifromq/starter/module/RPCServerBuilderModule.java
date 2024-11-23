/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.starter.module;

import static com.baidu.bifromq.starter.module.SSLUtil.defaultSslProvider;
import static com.baidu.bifromq.starter.module.SSLUtil.findJdkProvider;
import static com.baidu.bifromq.starter.utils.ResourceUtil.loadFile;

import com.baidu.bifromq.baserpc.server.IRPCServer;
import com.baidu.bifromq.baserpc.server.RPCServerBuilder;
import com.baidu.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import com.baidu.bifromq.starter.config.StandaloneConfig;
import com.baidu.bifromq.starter.config.model.ServerSSLContextConfig;
import com.google.common.base.Strings;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.grpc.netty.GrpcSslContexts;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import javax.inject.Singleton;

public class RPCServerBuilderModule extends AbstractModule {
    private static class RPCServerBuilderProvider implements Provider<RPCServerBuilder> {
        private final StandaloneConfig config;
        private final IRPCServiceTrafficService trafficService;

        @Inject
        private RPCServerBuilderProvider(StandaloneConfig config, IRPCServiceTrafficService trafficService) {
            this.config = config;
            this.trafficService = trafficService;
        }

        @Override
        public RPCServerBuilder get() {
            RPCServerBuilder rpcServerBuilder = IRPCServer.newBuilder()
                .host(config.getRpcConfig().getHost())
                .port(config.getRpcConfig().getPort())
                .workerThreads(config.getRpcConfig().getServerEventLoopThreads())
                .trafficService(trafficService);
            if (config.getRpcConfig().isEnableSSL()) {
                rpcServerBuilder.sslContext(buildRPCServerSslContext(config.getRpcConfig().getServerSSLConfig()));
            }
            return rpcServerBuilder;
        }

        private SslContext buildRPCServerSslContext(ServerSSLContextConfig config) {
            try {
                SslProvider sslProvider = defaultSslProvider();
                SslContextBuilder sslCtxBuilder = GrpcSslContexts
                    .forServer(loadFile(config.getCertFile()), loadFile(config.getKeyFile()))
                    .clientAuth(config.getClientAuth())
                    .sslProvider(sslProvider);
                if (Strings.isNullOrEmpty(config.getTrustCertsFile())) {
                    sslCtxBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
                } else {
                    sslCtxBuilder.trustManager(loadFile(config.getTrustCertsFile()));
                }
                if (sslProvider == SslProvider.JDK) {
                    sslCtxBuilder.sslContextProvider(findJdkProvider());
                }
                return sslCtxBuilder.build();
            } catch (Throwable e) {
                throw new RuntimeException("Fail to initialize server SSLContext", e);
            }
        }
    }

    @Override
    protected void configure() {
        bind(RPCServerBuilder.class).toProvider(RPCServerBuilderProvider.class).in(Singleton.class);
    }
}
