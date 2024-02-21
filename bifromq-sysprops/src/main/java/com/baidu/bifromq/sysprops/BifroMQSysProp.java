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

package com.baidu.bifromq.sysprops;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.sysprops.parser.BooleanParser;
import com.baidu.bifromq.sysprops.parser.DoubleParser;
import com.baidu.bifromq.sysprops.parser.IntegerParser;
import com.baidu.bifromq.sysprops.parser.LongParser;
import com.baidu.bifromq.sysprops.parser.PropParser;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public enum BifroMQSysProp {
    INGRESS_SLOWDOWN_DIRECT_MEMORY_USAGE("ingress_slowdown_direct_memory_usage", 0.8,
        DoubleParser.from(0.1, 1.0, false)),
    // further check if utf8 string contains any control character or non character according to [MQTT-1.5.3]
    MQTT_UTF8_SANITY_CHECK("mqtt_utf8_sanity_check", false, BooleanParser.INSTANCE),
    MAX_MQTT3_CLIENT_ID_LENGTH("max_mqtt3_client_id_length", 65535, IntegerParser.from(23, 65536)),
    MAX_MQTT5_CLIENT_ID_LENGTH("max_mqtt5_client_id_length", 65535, IntegerParser.from(23, 65536)),
    SESSION_REGISTER_NUM("session_register_num", 1000, IntegerParser.POSITIVE),
    DATA_PLANE_TOLERABLE_LATENCY_MS("data_plane_tolerable_latency_ms", 1000L, LongParser.POSITIVE),
    DATA_PLANE_BURST_LATENCY_MS("data_plane_burst_latency_ms", 5000L, LongParser.POSITIVE),
    CONTROL_PLANE_TOLERABLE_LATENCY_MS("control_plane_tolerant_latency_ms", 2000L, LongParser.POSITIVE),
    CONTROL_PLANE_BURST_LATENCY_MS("control_plane_burst_latency_ms", 5000L, LongParser.POSITIVE),
    DIST_WORKER_CALL_QUEUES("dist_server_dist_worker_call_queues", EnvProvider.INSTANCE.availableProcessors(),
        IntegerParser.POSITIVE),
    DIST_FAN_OUT_PARALLELISM("dist_worker_fanout_parallelism",
        Math.max(2, EnvProvider.INSTANCE.availableProcessors()), IntegerParser.POSITIVE),
    DIST_MAX_CACHED_SUBS_PER_TENANT("dist_worker_max_cached_subs_per_tenant", 200_000L, LongParser.POSITIVE),
    DIST_TOPIC_MATCH_EXPIRY("dist_worker_topic_match_expiry_seconds", 5, IntegerParser.POSITIVE),
    DIST_MATCH_PARALLELISM("dist_worker_match_parallelism",
        Math.max(2, EnvProvider.INSTANCE.availableProcessors() / 2), IntegerParser.POSITIVE),
    DIST_WORKER_FANOUT_SPLIT_THRESHOLD("dist_worker_fanout_split_threshold", 100000, IntegerParser.POSITIVE),
    DIST_WORKER_SPLIT_MAX_CPU_USAGE("dist_worker_split_max_cpu_usage", 0.8,
        DoubleParser.from(0.0, 1.0, true)),
    DIST_WORKER_LOAD_EST_WINDOW_SECONDS("dist_worker_load_estimation_window_seconds", 5L, LongParser.POSITIVE),
    DIST_WORKER_SPLIT_IO_NANOS_LIMIT("dist_worker_split_io_nanos_limit", 30_000L, LongParser.POSITIVE),
    DIST_WORKER_SPLIT_MAX_IO_DENSITY("dist_worker_split_max_io_density", 100, IntegerParser.POSITIVE),
    DIST_WORKER_RANGE_VOTER_COUNT("dist_worker_range_voter_count", 3, IntegerParser.POSITIVE),
    DIST_WORKER_RECOVERY_TIMEOUT_MILLIS("dist_worker_recovery_timeout_millis", 10000L, LongParser.NON_NEGATIVE),
    INBOX_DELIVERERS("inbox_deliverers", 100, IntegerParser.POSITIVE),
    INBOX_FETCH_PIPELINE_CREATION_RATE_LIMIT("inbox_fetch_pipeline_creation_rate_limit", 5000.0,
        DoubleParser.from(0.0, Double.MAX_VALUE, true)),
    INBOX_FETCH_QUEUES_PER_RANGE("inbox_fetch_queues_per_range",
        Math.max(1, EnvProvider.INSTANCE.availableProcessors() / 4), IntegerParser.POSITIVE),
    INBOX_CHECK_QUEUES_PER_RANGE("inbox_check_queues_per_range", 1, IntegerParser.POSITIVE),
    INBOX_STORE_TOLERABLE_QUERY_LATENCY_NANOS("inbox_store_tolerable_query_latency_nanos",
        Duration.ofMillis(2).toNanos(), LongParser.POSITIVE),
    INBOX_STORE_LOAD_EST_WINDOW_SECONDS("inbox_store_load_estimation_window_seconds", 5L, LongParser.POSITIVE),
    INBOX_STORE_RANGE_SPLIT_MAX_CPU_USAGE("inbox_store_range_split_max_cpu_usage", 0.8,
        DoubleParser.from(0.0, 1.0, true)),
    INBOX_STORE_RANGE_SPLIT_IO_NANOS_LIMIT("inbox_store_range_split_io_nanos_limit", 30_000L, LongParser.POSITIVE),
    INBOX_STORE_RANGE_SPLIT_MAX_IO_DENSITY("inbox_store_range_split_max_io_density", 100, IntegerParser.POSITIVE),
    INBOX_STORE_RANGE_VOTER_COUNT("inbox_store_range_voter_count", 1, IntegerParser.POSITIVE),
    INBOX_STORE_RECOVERY_TIMEOUT_MILLIS("inbox_store_recovery_timeout_millis", 10000L, LongParser.NON_NEGATIVE),
    MQTT_DELIVERERS_PER_SERVER("mqtt_deliverers_per_server",
        EnvProvider.INSTANCE.availableProcessors(), IntegerParser.POSITIVE),
    RETAIN_STORE_RANGE_SPLIT_MAX_CPU_USAGE("retain_store_range_split_max_cpu_usage", 0.8,
        DoubleParser.from(0.0, 1.0, true)),
    RETAIN_STORE_RANGE_SPLIT_IO_NANOS_LIMIT("retain_store_range_split_io_nanos_limit", 30_000L, LongParser.POSITIVE),
    RETAIN_STORE_RANGE_SPLIT_MAX_IO_DENSITY("retain_store_range_split_max_io_density", 100, IntegerParser.POSITIVE),
    RETAIN_STORE_RANGE_VOTER_COUNT("retain_store_range_voter_count", 3, IntegerParser.POSITIVE),
    RETAIN_STORE_LOAD_EST_WINDOW_SECONDS("retain_store_load_estimation_window_seconds", 5L, LongParser.POSITIVE),
    RETAIN_STORE_RECOVERY_TIMEOUT_MILLIS("retain_store_recovery_timeout_millis", 10000L, LongParser.NON_NEGATIVE);

    public final String propKey;
    private final Object propDefValue;
    private final PropParser<?> parser;

    BifroMQSysProp(String propKey, Object propDefValue, PropParser<?> parser) {
        this.propKey = propKey;
        this.propDefValue = propDefValue;
        this.parser = parser;
    }

    private String sysPropValue(final String key) {
        String value = null;
        try {
            value = System.getProperty(key);
        } catch (SecurityException e) {
            log.warn("Failed to retrieve a system property '{}'", key, e);
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    public <T> T defVal() {
        return (T) propDefValue;
    }

    @SuppressWarnings("unchecked")
    public <T> T get() {
        String value = sysPropValue(propKey);
        if (value == null) {
            return defVal();
        }

        value = value.trim().toLowerCase();
        if (value.isEmpty()) {
            return defVal();
        }
        try {
            return (T) parser.parse(value);
        } catch (Throwable e) {
            log.warn("Failed to parse system prop '{}':{} - using the default value: {}", propKey, value, defVal());
            return defVal();
        }
    }
}
