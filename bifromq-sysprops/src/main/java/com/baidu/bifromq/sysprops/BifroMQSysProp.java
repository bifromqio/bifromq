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

import com.baidu.bifromq.sysprops.parser.DoubleParser;
import com.baidu.bifromq.sysprops.parser.IntegerParser;
import com.baidu.bifromq.sysprops.parser.LongParser;
import com.baidu.bifromq.sysprops.parser.PropParser;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public enum BifroMQSysProp {
    AUTH_CALL_PARALLELISM("auth_call_parallelism", 4,
        IntegerParser.from(0, 2 * Runtime.getRuntime().availableProcessors())),
    AUTH_AUTH_RESULT_CACHE_LIMIT("auth_auth_result_cache_limit", 100000L, LongParser.NON_NEGATIVE),
    AUTH_CHECK_RESULT_CACHE_LIMIT("auth_check_result_cache_limit", 100000L, LongParser.NON_NEGATIVE),
    AUTH_AUTH_RESULT_EXPIRY_SECONDS("auth_auth_result_expiry_seconds", 5L, LongParser.NON_NEGATIVE),
    AUTH_CHECK_RESULT_EXPIRY_SECONDS("auth_check_result_expiry_seconds", 5L, LongParser.NON_NEGATIVE),
    DIST_CLIENT_MAX_INFLIGHT_CALLS_PER_QUEUE("dist_client_max_calls_per_queue", 1, IntegerParser.POSITIVE),
    DIST_SERVER_MAX_INFLIGHT_CALLS_PER_QUEUE("dist_server_max_calls_per_queue", 1, IntegerParser.POSITIVE),
    DIST_MAX_TOPICS_IN_BATCH("dist_server_max_topics_in_batch", 200, IntegerParser.POSITIVE),
    DIST_MAX_UPDATES_IN_BATCH("dist_server_max_updates_in_batch", 100, IntegerParser.POSITIVE),
    DIST_WORKER_CALL_QUEUES("dist_server_dist_worker_call_queues", 16, IntegerParser.POSITIVE),
    DIST_MAX_INFLIGHT_SEND("dist_worker_max_inflight_send", 2, IntegerParser.POSITIVE),
    DIST_PARALLEL_FAN_OUT_SIZE("dist_worker_parallel_fanout_size", 5000, IntegerParser.POSITIVE),
    DIST_FAN_OUT_PARALLELISM("dist_worker_fanout_parallelism",
        Math.max(2, Runtime.getRuntime().availableProcessors() / 2), IntegerParser.POSITIVE),
    DIST_MAX_BATCH_SEND_MESSAGES("dist_worker_max_batch_send_messages", 10_000, IntegerParser.POSITIVE),
    DIST_MAX_CACHED_SUBS_PER_TRAFFIC("dist_worker_max_cached_subs_per_traffic", 100_000L, LongParser.POSITIVE),
    DIST_TOPIC_MATCH_EXPIRY("dist_worker_topic_match_expiry_seconds", 5, IntegerParser.POSITIVE),
    DIST_MATCH_PARALLELISM("dist_worker_match_parallelism",
        Math.max(2, Runtime.getRuntime().availableProcessors() / 2), IntegerParser.POSITIVE),
    DIST_MAX_RANGE_LOAD("dist_worker_max_range_load", 300_000L, IntegerParser.POSITIVE),
    DIST_SPLIT_KEY_EST_THRESHOLD("dist_worker_split_key_threshold", 0.7D, DoubleParser.from(0.0, 1.0, true)),
    DIST_LOAD_TRACKING_SECONDS("dist_worker_load_tracking_seconds", 5, IntegerParser.POSITIVE),
    DIST_WORKER_VOTER_COUNT("dist_worker_replica_voter_count", 3, IntegerParser.POSITIVE),
    DIST_WORKER_LEARNER_COUNT("dist_worker_replica_learner_count", 3, IntegerParser.POSITIVE),
    DIST_WORKER_RECOVERY_TIMEOUT_MILLIS("dist_worker_recovery_timeout_millis", 10000L, LongParser.NON_NEGATIVE),
    INBOX_INBOX_GROUPS("inbox_inbox_groups", 100, IntegerParser.POSITIVE),
    INBOX_FETCH_PIPELINE_CREATION_RATE_LIMIT("inbox_fetch_pipeline_creation_rate_limit", 5000.0,
        DoubleParser.from(0.0, Double.MAX_VALUE, true)),
    INBOX_MAX_BATCHED_UPDATES("inbox_max_batched_updates", 2000, IntegerParser.POSITIVE),
    INBOX_MAX_INBOXES_PER_INSERT("inbox_max_inboxes_per_insert", 500, IntegerParser.POSITIVE),
    INBOX_MAX_INBOXES_PER_COMMIT("inbox_max_inboxes_per_commit", 500, IntegerParser.POSITIVE),
    INBOX_MAX_INBOXES_PER_CREATE("inbox_max_inboxes_per_create", 500, IntegerParser.POSITIVE),
    INBOX_MAX_INBOXES_PER_TOUCH("inbox_max_inboxes_per_touch", 500, IntegerParser.POSITIVE),
    INBOX_MAX_INBOXES_PER_FETCH("inbox_max_inboxes_per_fetch", 500, IntegerParser.POSITIVE),
    INBOX_MAX_INBOXES_PER_CHECK("inbox_max_inboxes_per_check", 500, IntegerParser.POSITIVE),
    INBOX_MAX_BYTES_PER_INSERT("inbox_max_bytes_per_insert", 1024 * 1024 * 1024,
        Integer::parseUnsignedInt), // 1MB
    INBOX_FETCH_QUEUES_PER_RANGE("inbox_fetch_queues_per_range",
        Math.max(1, Runtime.getRuntime().availableProcessors() / 4), IntegerParser.POSITIVE),
    INBOX_CHECK_QUEUES_PER_RANGE("inbox_check_queues_per_range", 1, IntegerParser.POSITIVE),
    MQTT_INBOXGROUPS_PER_SERVER("mqtt_inboxgroups_per_server", 4, IntegerParser.POSITIVE);

    public final String propKey;
    private final Object propDefValue;
    private final PropParser parser;

    BifroMQSysProp(String propKey, Object propDefValue, PropParser parser) {
        this.propKey = propKey;
        this.propDefValue = propDefValue;
        this.parser = parser;
    }

    private String sysPropValue(String key) {
        return sysPropValue(key, null);
    }

    private String sysPropValue(final String key, String def) {
        String value = null;
        try {
            value = System.getProperty(key);
        } catch (SecurityException e) {
            log.warn("Failed to retrieve a system property '{}'; use default value '{}'.", key, def, e);
        }
        if (value == null) {
            return def;
        }
        return value;
    }

    public <T> T defVal() {
        return (T) propDefValue;
    }

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
