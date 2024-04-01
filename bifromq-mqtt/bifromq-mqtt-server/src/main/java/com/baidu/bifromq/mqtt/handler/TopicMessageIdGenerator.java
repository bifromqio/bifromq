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

package com.baidu.bifromq.mqtt.handler;

import static com.baidu.bifromq.mqtt.utils.MessageIdUtil.messageId;
import static com.baidu.bifromq.mqtt.utils.MessageIdUtil.messageSequence;
import static com.baidu.bifromq.mqtt.utils.MessageIdUtil.syncWindowSequence;

import com.baidu.bifromq.metrics.ITenantMeter;
import com.baidu.bifromq.metrics.TenantMetric;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
 * Generate message id for each topic. The message id is a 64-bit integer, where the high 32 bits are the sync window
 * sequence number and the low 32 bits are the message sequence number within the sync window.
 */
public class TopicMessageIdGenerator {
    private final long syncWindowIntervalMillis;
    private final TopicMessageIdCache topicMessageIdCache;
    private final PrematureEvictionChecker checker;

    public TopicMessageIdGenerator(Duration syncWindowInterval, int maxActiveTopics, ITenantMeter meter) {
        this.syncWindowIntervalMillis = syncWindowInterval.toMillis();
        this.checker = new PrematureEvictionChecker(syncWindowIntervalMillis);
        this.topicMessageIdCache = new TopicMessageIdCache(meter, checker, maxActiveTopics);
    }

    public long nextMessageId(String topic, long nowMillis) {
        checker.updateNowMillis(nowMillis);
        return topicMessageIdCache.computeIfAbsent(topic, k -> new AtomicLong(0xFFFFFFFE00000000L))
            .updateAndGet(msgId -> {
                long currentSWS = syncWindowSequence(nowMillis, syncWindowIntervalMillis);
                long lastSWS = syncWindowSequence(msgId);
                if (currentSWS == lastSWS || currentSWS == lastSWS + 1) {
                    return messageId(currentSWS, messageSequence(msgId) + 1);
                } else {
                    return messageId(currentSWS, 0);
                }
            });
    }

    private static class PrematureEvictionChecker implements Predicate<Long> {
        private final long syncWindowIntervalMillis;
        private long nowMillis;

        private PrematureEvictionChecker(long syncWindowIntervalMillis) {
            this.syncWindowIntervalMillis = syncWindowIntervalMillis;
        }

        public void updateNowMillis(long nowMillis) {
            this.nowMillis = nowMillis;
        }

        @Override
        public boolean test(Long messageId) {
            return syncWindowSequence(nowMillis, syncWindowIntervalMillis) - syncWindowSequence(messageId) < 1;
        }
    }

    private static class TopicMessageIdCache extends LinkedHashMap<String, AtomicLong> {
        private final ITenantMeter meter;
        private final Predicate<Long> isPrematureEviction;
        private final int maxSize;

        private TopicMessageIdCache(ITenantMeter meter,
                                    Predicate<Long> isPrematureEviction,
                                    int maxSize) {
            super(maxSize, 0.75f, true);
            this.meter = meter;
            this.isPrematureEviction = isPrematureEviction;
            this.maxSize = maxSize;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, AtomicLong> eldest) {
            if (size() > maxSize) {
                if (isPrematureEviction.test(eldest.getValue().get())) {
                    meter.recordCount(TenantMetric.MqttTopicSeqAbortCount);
                }
                return true;
            }
            return false;
        }
    }
}
