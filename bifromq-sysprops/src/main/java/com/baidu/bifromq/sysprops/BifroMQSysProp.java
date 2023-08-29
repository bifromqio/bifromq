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

package com.baidu.bifromq.sysprops;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.sysprops.parser.BooleanParser;
import com.baidu.bifromq.sysprops.parser.DoubleParser;
import com.baidu.bifromq.sysprops.parser.IntegerParser;
import com.baidu.bifromq.sysprops.parser.LongParser;
import com.baidu.bifromq.sysprops.parser.PropParser;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public enum BifroMQSysProp {
    // further check if utf8 string contains any control character or non character according to [MQTT-1.5.3]
    MQTT_UTF8_SANITY_CHECK("mqtt_utf8_sanity_check", false, BooleanParser.INSTANCE),
    MAX_CLIENT_ID_LENGTH("max_client_id_length", 65535, IntegerParser.from(23, 65536)),
    DATA_PLANE_TOLERABLE_LATENCY_MS("data_plane_tolerable_latency_ms", 50L, LongParser.POSITIVE),
    DATA_PLANE_BURST_LATENCY_MS("data_plane_burst_latency_ms", 1000L, LongParser.POSITIVE),
    CONTROL_PLANE_TOLERABLE_LATENCY_MS("control_plane_tolerant_latency_ms", 100L, LongParser.POSITIVE),
    CONTROL_PLANE_BURST_LATENCY_MS("control_plane_burst_latency_ms", 5000L, LongParser.POSITIVE),
    DIST_WORKER_CALL_QUEUES("dist_server_dist_worker_call_queues", 16, IntegerParser.POSITIVE),
    DIST_FAN_OUT_PARALLELISM("dist_worker_fanout_parallelism",
        Math.max(2, EnvProvider.INSTANCE.availableProcessors() / 2), IntegerParser.POSITIVE),
    DIST_MAX_CACHED_SUBS_PER_TENANT("dist_worker_max_cached_subs_per_tenant", 100_000L, LongParser.POSITIVE),
    DIST_TOPIC_MATCH_EXPIRY("dist_worker_topic_match_expiry_seconds", 5, IntegerParser.POSITIVE),
    DIST_MATCH_PARALLELISM("dist_worker_match_parallelism",
        Math.max(2, EnvProvider.INSTANCE.availableProcessors() / 2), IntegerParser.POSITIVE),
    DIST_WORKER_VOTER_COUNT("dist_worker_replica_voter_count", 3, IntegerParser.POSITIVE),
    DIST_WORKER_RECOVERY_TIMEOUT_MILLIS("dist_worker_recovery_timeout_millis", 10000L, LongParser.NON_NEGATIVE),
    INBOX_DELIVERERS("inbox_deliverers", 100, IntegerParser.POSITIVE),
    INBOX_FETCH_PIPELINE_CREATION_RATE_LIMIT("inbox_fetch_pipeline_creation_rate_limit", 5000.0,
        DoubleParser.from(0.0, Double.MAX_VALUE, true)),
    INBOX_FETCH_QUEUES_PER_RANGE("inbox_fetch_queues_per_range",
        Math.max(1, EnvProvider.INSTANCE.availableProcessors() / 4), IntegerParser.POSITIVE),
    INBOX_CHECK_QUEUES_PER_RANGE("inbox_check_queues_per_range", 1, IntegerParser.POSITIVE),
    INBOX_MAX_RANGE_LOAD("inbox_store_max_range_load", 2_000_000, IntegerParser.POSITIVE),
    INBOX_SPLIT_KEY_EST_THRESHOLD("inbox_store_split_key_threshold", 0.7D, DoubleParser.from(0.0, 1.0, true)),
    INBOX_LOAD_TRACKING_SECONDS("inbox_store_load_tracking_seconds", 5, IntegerParser.POSITIVE),
    INBOX_STORE_VOTER_COUNT("inbox_store_replica_voter_count", 3, IntegerParser.POSITIVE),
    INBOX_STORE_RECOVERY_TIMEOUT_MILLIS("inbox_store_recovery_timeout_millis", 10000L, LongParser.NON_NEGATIVE),
    MQTT_DELIVERERS_PER_SERVER("mqtt_deliverers_per_server", 4, IntegerParser.POSITIVE),
    RETAIN_STORE_VOTER_COUNT("retain_store_replica_voter_count", 3, IntegerParser.POSITIVE),
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
