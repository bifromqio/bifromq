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

package com.baidu.demo.plugin;

import com.baidu.bifromq.plugin.BifroMQPlugin;
import com.baidu.bifromq.plugin.BifroMQPluginDescriptor;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DemoPlugin extends BifroMQPlugin<DemoPluginContext> {
    private static final String PLUGIN_PROMETHEUS_PORT = "plugin.prometheus.port";
    private static final String PLUGIN_PROMETHEUS_CONTEXT = "plugin.prometheus.context";
    private final PrometheusMeterRegistry registry;
    private final HttpServer prometheusExportServer;
    private final Thread serverThread;

    /**
     * Constructor to be used by plugin manager for plugin instantiation. Your plugins have to provide constructor with
     * this exact signature to be successfully loaded by manager.
     *
     * @param context the context object
     */
    public DemoPlugin(BifroMQPluginDescriptor context) {
        super(context);
        registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        registry.config().meterFilter(new MeterFilter() {
            @Override
            public DistributionStatisticConfig configure(Meter.Id id, DistributionStatisticConfig config) {
                switch (id.getType()) {
                    case TIMER:
                    case DISTRIBUTION_SUMMARY:
                        // following config will cause huge performance penalty
                        // don't enable it unless you accept the consequence
//                        if (id.getTag("tenantId") != null) {
//                            return DistributionStatisticConfig.builder()
//                                .percentiles(0.5, 0.99, 0.999)
//                                .expiry(Duration.ofSeconds(5))
//                                .build()
//                                .merge(config);
//                        }
                }
                return DistributionStatisticConfig.builder()
                    .expiry(Duration.ofSeconds(5))
                    .build().merge(config);
            }
        });
        Metrics.addRegistry(registry);
        try {
            prometheusExportServer = HttpServer.create(new InetSocketAddress(port()), 0);
            prometheusExportServer.createContext(contextPath(), httpExchange -> {
                String response = registry.scrape();
                httpExchange.sendResponseHeaders(200, response.getBytes().length);
                try (OutputStream os = httpExchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            });
            serverThread = new Thread(prometheusExportServer::start);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void doStart() {
        serverThread.start();
        log.debug("Prometheus exporter started");
    }

    @Override
    protected void doStop() {
        prometheusExportServer.stop(0);
        Metrics.removeRegistry(registry);
        log.debug("Prometheus exporter stopped");
    }

    private int port() {
        String prometheusPort = System.getProperty(PLUGIN_PROMETHEUS_PORT, "9090");
        try {
            return Integer.parseUnsignedInt(prometheusPort);
        } catch (Throwable e) {
            return 9090;
        }
    }

    private String contextPath() {
        String ctx = System.getProperty(PLUGIN_PROMETHEUS_CONTEXT, "/metrics");
        if (ctx.startsWith("/")) {
            return ctx;
        }
        return "/" + ctx;
    }
}
