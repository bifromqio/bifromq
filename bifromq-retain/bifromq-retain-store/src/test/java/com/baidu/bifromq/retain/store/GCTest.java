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

package com.baidu.bifromq.retain.store;

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.metrics.TenantMetric;
import com.baidu.bifromq.retain.rpc.proto.MatchResult;
import com.baidu.bifromq.retain.rpc.proto.Matched;
import com.baidu.bifromq.retain.rpc.proto.RetainResult;
import com.baidu.bifromq.type.TopicMessage;
import io.micrometer.core.instrument.Gauge;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class GCTest extends RetainStoreTest {
    private String tenantId;

    @BeforeMethod(alwaysRun = true)
    public void reset() {
        tenantId = "tenantA-" + System.nanoTime();
    }


    @Test(groups = "integration")
    public void retainAlreadyExpired() {
        String topic = "/a";
        TopicMessage message = message(topic, "hello", 0, 1);
        // make it expired
        assertEquals(requestRetain(tenantId, 1100L, message, 1), RetainResult.Code.ERROR);
    }

    @Test(groups = "integration")
    public void inlineGCDuringRetain() {
        String topic1 = "/a";
        String topic2 = "/b";
        TopicMessage message1 = message(topic1, "hello", 0, 1);

        requestRetain(tenantId, 0, message1, 1);
        MatchResult matchReply = requestMatch(tenantId, 0, topic1, 10);
        Matched matched = matchReply.getOk();
        assertEquals(matched.getMessagesCount(), 1);
        assertEquals(matched.getMessages(0), message1);

        // message1 has expired
        assertEquals(requestMatch(tenantId, 1100L, topic1, 10).getOk().getMessagesCount(), 0);

        TopicMessage message2 = message(topic2, "world", 1000, 1);

        assertEquals(requestRetain(tenantId, 1100L, message2, 1), RetainResult.Code.RETAINED);

        assertEquals(requestMatch(tenantId, 1100L, topic1, 10).getOk().getMessagesCount(), 0);

        matchReply = requestMatch(tenantId, 1100L, topic2, 10);
        assertEquals(matchReply.getOk().getMessagesCount(), 1);
        assertEquals(matchReply.getOk().getMessages(0), message2);

    }

    @Test(groups = "integration")
    public void inlineGCDuringDelete() {
        String topic = "/a";
        TopicMessage message1 = message(topic, "hello", 0, 1);

        requestRetain(tenantId, message1, 1);

        assertEquals(requestRetain(tenantId, 1100L, message(topic, ""), 1), RetainResult.Code.CLEARED);
        assertEquals(requestMatch(tenantId, 1100L, topic, 10).getOk().getMessagesCount(), 0);
    }

    @Test(groups = "integration")
    public void inlineGCDuringReplace() {
        String topic1 = "/a";
        String topic2 = "/b";
        TopicMessage message1 = message(topic1, "hello", 0, 1);
        TopicMessage message2 = message(topic2, "world", 0, 1);
        TopicMessage message3 = message(topic2, "world", 1000, 1);

        requestRetain(tenantId, 0, message1, 2);
        requestRetain(tenantId, 0, message2, 2);

        assertEquals(requestRetain(tenantId, 1100L, message3, 2), RetainResult.Code.RETAINED);

        assertEquals(requestMatch(tenantId, 1100L, topic1, 10).getOk().getMessagesCount(), 0);
        assertEquals(requestMatch(tenantId, 1100L, topic2, 10).getOk().getMessagesCount(), 1);
        assertEquals(requestMatch(tenantId, 1100L, topic2, 10).getOk().getMessages(0), message3);

        // message1 will be removed as well, so retain set size should be 1
        assertEquals(requestRetain(tenantId, 1100L, message("/c", "abc"), 2), RetainResult.Code.RETAINED);
        // no room
        assertEquals(requestRetain(tenantId, 1100L, message("/d", "abc"), 2), RetainResult.Code.ERROR);
    }

    @Test(groups = "integration")
    public void estExpiryTimeUpdateByRetainNew() {
        String topic1 = "/a";
        String topic2 = "/b";
        TopicMessage message1 = message(topic1, "hello", 0, 2);
        TopicMessage message2 = message(topic2, "world", 0, 1);


        requestRetain(tenantId, 0, message1, 2);
        requestRetain(tenantId, 0, message2, 2);

        // message2 expired
        assertEquals(requestMatch(tenantId, 1100L, topic1, 10).getOk().getMessagesCount(), 1);
        assertEquals(requestMatch(tenantId, 1100L, topic2, 10).getOk().getMessagesCount(), 0);

        assertEquals(requestRetain(tenantId, 1100L, message("/c", "abc"), 2), RetainResult.Code.RETAINED);
        assertEquals(requestRetain(tenantId, 1100L, message("/d", "abc"), 2), RetainResult.Code.EXCEED_LIMIT);

        assertEquals(requestRetain(tenantId, 2100L, message("/d", "abc"), 2), RetainResult.Code.RETAINED);
    }

    @Test(groups = "integration")
    public void estExpiryTimeUpdatedByReplaceNew() {
        String tenantId = "tenantA";
        String topic = "/a";
        TopicMessage message1 = message(topic, "hello", 0, 2);
        TopicMessage message2 = message(topic, "world", 0, 1);

        requestRetain(tenantId, 0, message1, 1);
        requestRetain(tenantId, 0, message2, 1);

        // no room for new message
        assertEquals(requestRetain(tenantId, 0, message("/d", "abc"), 1), RetainResult.Code.EXCEED_LIMIT);
        assertEquals(requestRetain(tenantId, 1100L, message("/d", "abc"), 1), RetainResult.Code.RETAINED);
        assertEquals(requestMatch(tenantId, 1100L, topic, 10).getOk().getMessagesCount(), 0);
    }

    @Test(groups = "integration")
    public void gc() {
        String topic = "/a";
        TopicMessage message = message(topic, "hello", 0, 1);

        requestRetain(tenantId, 0, message, 1);

        requestGC(0, null, null);
        assertEquals(requestRetain(tenantId, 0L, message("/d", "abc"), 1), RetainResult.Code.EXCEED_LIMIT);

        requestGC(1100L, null, null);

        assertEquals(requestRetain(tenantId, 1100L, message("/d", "abc"), 1), RetainResult.Code.RETAINED);
    }

    @Test(groups = "integration")
    public void gcTenant() {
        String tenantId1 = "tenantB-" + System.nanoTime();
        String topic = "/a";
        requestRetain(tenantId, 0, message(topic, "hello", 0, 1), 1);
        requestRetain(tenantId1, 0, message(topic, "hello", 0, 1), 1);

        requestGC(1100L, tenantId, null);

        assertNoGauge(tenantId, TenantMetric.MqttRetainNumGauge);
        assertNoGauge(tenantId, TenantMetric.MqttRetainSpaceGauge);

        getRetainCountGauge(tenantId1);
        getSpaceUsageGauge(tenantId1);
    }

    @Test(groups = "integration")
    public void gcTenantWithExpirySeconds() {
        requestRetain(tenantId, 0, message("/a", "hello", 0, 2), 2);
        requestRetain(tenantId, 0, message("/b", "hello", Duration.ofSeconds(1).toMillis(), 3), 2);
        Gauge retainCountGauge = getRetainCountGauge(tenantId);
        await().until(() -> retainCountGauge.value() == 2);
        requestGC(1100L, tenantId, 1);
        await().until(() -> retainCountGauge.value() == 1);
    }
}
