package com.baidu.demo.plugin.util;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class VariableUtil {
    private static final String PLUGIN_PROMETHEUS_PORT = "PLUGIN_PROMETHEUS_PORT";
    private static final String PLUGIN_PROMETHEUS_CONTEXT = "PLUGIN_PROMETHEUS_CONTEXT";

    private static final int DEFAULT_PORT = 9090;
    private static final String DEFAULT_CONTEXT = "/metrics";

    public static int getPort() {
        String prometheusPort = System.getenv(PLUGIN_PROMETHEUS_PORT);
        if (prometheusPort == null) {
            prometheusPort = System.getProperty(PLUGIN_PROMETHEUS_PORT, "9090");
        }
        try {
            return Integer.parseUnsignedInt(prometheusPort);
        } catch (Throwable e) {
            log.error("Parse prometheus port: {} error, use default port", prometheusPort, e);
            return DEFAULT_PORT;
        }
    }

    public static String getContext() {
        String ctx = System.getenv(PLUGIN_PROMETHEUS_CONTEXT);
        if (ctx == null) {
            ctx = System.getProperty(PLUGIN_PROMETHEUS_CONTEXT, DEFAULT_CONTEXT);
        }
        return ctx.startsWith("/") ? ctx : "/" + ctx;
    }
}
