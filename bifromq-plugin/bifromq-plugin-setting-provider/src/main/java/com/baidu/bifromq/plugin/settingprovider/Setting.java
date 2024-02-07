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

package com.baidu.bifromq.plugin.settingprovider;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import java.time.Duration;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public enum Setting {
    MQTT3Enabled(Boolean.class, val -> true, true),
    MQTT4Enabled(Boolean.class, val -> true, true),
    MQTT5Enabled(Boolean.class, val -> true, true),
    DebugModeEnabled(Boolean.class, val -> true, false),
    ForceTransient(Boolean.class, val -> true, false),
    ByPassPermCheckError(Boolean.class, val -> true, true),
    PayloadFormatValidationEnabled(Boolean.class, val -> true, true),
    RetainEnabled(Boolean.class, val -> true, true),
    WildcardSubscriptionEnabled(Boolean.class, val -> true, true),
    SubscriptionIdentifierEnabled(Boolean.class, val -> true, true),
    SharedSubscriptionEnabled(Boolean.class, val -> true, true),
    MaximumQoS(Integer.class, val -> (int) val == 0 || (int) val == 1 || (int) val == 2, 2),
    MaxTopicLevelLength(Integer.class, val -> (int) val > 0, 40),

    MaxTopicLevels(Integer.class, val -> (int) val > 0, 16),

    MaxTopicLength(Integer.class, val -> (int) val > 0 && (int) val < 65536, 255),

    MaxTopicAlias(Integer.class, val -> (int) val >= 0 && (int) val < 65536, 10),

    MaxSharedGroupMembers(Integer.class, val -> (int) val > 0, 200),

    MaxTopicFiltersPerInbox(Integer.class, val -> (int) val > 0, 100),

    MsgPubPerSec(Integer.class, val -> (int) val > 0 && (int) val <= 1000, 200),

    ReceivingMaximum(Integer.class, val -> (int) val > 0 && (int) val <= 65535, 200),

    InBoundBandWidth(Long.class, val -> (long) val >= 0, 512 * 1024L),

    OutBoundBandWidth(Long.class, val -> (long) val >= 0, 512 * 1024L),

    MaxUserPayloadBytes(Integer.class, val -> (int) val > 0 && (int) val <= 1024 * 1024, 256 * 1024),

    MaxTopicFiltersPerSub(Integer.class, val -> (int) val > 0 && (int) val <= 100, 10),

    OfflineExpireTimeSeconds(Integer.class, val -> (int) val > 0, 24 * 60 * 60),

    OfflineQueueSize(Integer.class, val -> (int) val > 0 && (int) val <= 65535, 1000),

    OfflineOverflowDropOldest(Boolean.class, val -> true, false),

    RetainedTopicLimit(Integer.class, val -> (int) val >= 0, 10),

    RetainMessageMatchLimit(Integer.class, val -> (int) val >= 0, 10),

    DistReservedUnitInterval(Long.class, val -> (long) val > 0 && (long) val <= 0xFFFFFFFFL, 0xFFFFFFFFL),

    DistLimitUnitInterval(Long.class, val -> (long) val >= 0 && (long) val <= 0xFFFFFFFFL, 0L);

    public final Class<?> valueType;
    final Predicate<Object> validator;
    final Object initial;
    final Cache<String, Object> currentVals;

    Setting(Class<?> valueType, Predicate<Object> validator, Object initial) {
        this.valueType = valueType;
        this.validator = validator;
        initial = resolve(initial);
        assert isValid(initial);
        this.initial = initial;
        currentVals = Caffeine.newBuilder()
            .expireAfterWrite(Duration.ofSeconds(5))
            .scheduler(Scheduler.systemScheduler())
            .build();
    }

    /**
     * The current effective setting's value for given tenant
     *
     * @param tenantId the id of the calling tenant
     * @return The effective value of the setting for the client
     */
    public <R> R current(String tenantId) {
        return (R) currentVals.asMap().getOrDefault(tenantId, initial);
    }

    /**
     * Validate if provided value is a valid for the setting
     *
     * @param val the setting value to be verified
     * @return true if the value is valid
     */
    public <R> boolean isValid(R val) {
        if (!valueType.isInstance(val)) {
            return false;
        }
        return this.validator.test(val);
    }

    // intentionally package level access
    void current(String tenantId, Object newVal) {
        assert isValid(newVal);
        if (!newVal.equals(initial)) {
            // only cache changed value
            currentVals.put(tenantId, newVal);
        } else {
            // revert to initial
            currentVals.invalidate(tenantId);
        }
    }

    Object resolve(Object initial) {
        String override = System.getProperty(name());
        if (override != null) {
            try {
                if (valueType == Integer.class) {
                    return Integer.parseInt(override);
                }
                if (valueType == Long.class) {
                    return Long.parseLong(override);
                }
                if (valueType == Boolean.class) {
                    return Boolean.parseBoolean(override);
                }
            } catch (Throwable e) {
                log.error("Unable to parse setting value from system property: setting={}, value={}",
                    name(), override);
                return initial;
            }
        }
        return initial;
    }
}