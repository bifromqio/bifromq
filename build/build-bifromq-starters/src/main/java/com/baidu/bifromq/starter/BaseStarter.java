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

package com.baidu.bifromq.starter;

import com.baidu.bifromq.starter.config.StarterConfig;
import com.baidu.bifromq.starter.config.standalone.model.SSLContextConfig;
import com.baidu.bifromq.starter.config.standalone.model.ServerSSLContextConfig;
import com.baidu.bifromq.starter.utils.ConfigUtil;
import com.baidu.bifromq.starter.utils.ResourceUtil;
import com.google.common.base.Strings;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmHeapPressureMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmInfoMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;
import java.io.File;
import java.security.Provider;
import java.security.Security;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class BaseStarter<T extends StarterConfig> implements IStarter {

    static {
        RxJavaPlugins.setErrorHandler(e -> log.error("Uncaught RxJava exception", e));
    }

    public static final String CONF_DIR_PROP = "CONF_DIR";

    protected abstract void init(T config);

    protected abstract Class<T> configClass();

    protected T buildConfig(File configFile) {
        return ConfigUtil.build(configFile, configClass());
    }

    protected SslContext buildServerSslContext(ServerSSLContextConfig config) {
        try {
            SslProvider sslProvider = defaultSslProvider();
            SslContextBuilder sslCtxBuilder = SslContextBuilder
                .forServer(loadFromConfDir(config.getCertFile()), loadFromConfDir(config.getKeyFile()))
                .clientAuth(ClientAuth.valueOf(config.getClientAuth()))
                .sslProvider(sslProvider);
            if (Strings.isNullOrEmpty(config.getTrustCertsFile())) {
                sslCtxBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
            } else {
                sslCtxBuilder.trustManager(loadFromConfDir(config.getTrustCertsFile()));
            }
            if (sslProvider == SslProvider.JDK) {
                sslCtxBuilder.sslContextProvider(findJdkProvider());
            }
            return sslCtxBuilder.build();
        } catch (Throwable e) {
            throw new RuntimeException("Fail to initialize server SSLContext", e);
        }
    }

    protected SslContext buildClientSslContext(SSLContextConfig config) {
        try {
            SslProvider sslProvider = defaultSslProvider();
            SslContextBuilder sslCtxBuilder = SslContextBuilder
                .forClient()
                .trustManager(loadFromConfDir(config.getTrustCertsFile()))
                .keyManager(loadFromConfDir(config.getCertFile()), loadFromConfDir(config.getKeyFile()))
                .sslProvider(sslProvider);
            if (sslProvider == SslProvider.JDK) {
                sslCtxBuilder.sslContextProvider(findJdkProvider());
            }
            return sslCtxBuilder.build();
        } catch (Throwable e) {
            throw new RuntimeException("Fail to initialize client SSLContext", e);
        }
    }

    protected void setupMetrics() {
        new JvmInfoMetrics().bindTo(Metrics.globalRegistry);
        new JvmMemoryMetrics().bindTo(Metrics.globalRegistry);
        new JvmGcMetrics().bindTo(Metrics.globalRegistry);
        new JvmHeapPressureMetrics().bindTo(Metrics.globalRegistry);
        new JvmThreadMetrics().bindTo(Metrics.globalRegistry);
        new UptimeMetrics().bindTo(Metrics.globalRegistry);
        new ClassLoaderMetrics().bindTo(Metrics.globalRegistry);
        new ProcessorMetrics().bindTo(Metrics.globalRegistry);
    }

    private SslProvider defaultSslProvider() {
        if (OpenSsl.isAvailable()) {
            return SslProvider.OPENSSL;
        }
        Provider jdkProvider = findJdkProvider();
        if (jdkProvider != null) {
            return SslProvider.JDK;
        }
        throw new IllegalStateException("Could not find TLS provider");
    }

    private Provider findJdkProvider() {
        Provider[] providers = Security.getProviders("SSLContext.TLS");
        if (providers.length > 0) {
            return providers[0];
        }
        return null;
    }

    public static File loadFromConfDir(String fileName) {
        return ResourceUtil.getFile(fileName, CONF_DIR_PROP);
    }

}
