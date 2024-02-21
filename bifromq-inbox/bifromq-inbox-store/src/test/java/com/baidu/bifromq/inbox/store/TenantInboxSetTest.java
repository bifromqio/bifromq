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

package com.baidu.bifromq.inbox.store;

import static com.baidu.bifromq.metrics.TenantMetric.MqttPersistentSessionNumGauge;
import static com.baidu.bifromq.metrics.TenantMetric.MqttPersistentSessionSpaceGauge;
import static com.baidu.bifromq.metrics.TenantMetric.MqttPersistentSubCountGauge;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.inbox.storage.proto.InboxMetadata;
import com.baidu.bifromq.inbox.storage.proto.TopicFilterOption;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.mockito.Mockito;
import org.testng.annotations.Test;

@Slf4j
public class TenantInboxSetTest extends MeterTest {
    @Test
    public void testSetupMeterAfterCreate() {
        String tenantId = "tenantId-" + System.nanoTime();
        assertNoGauge(tenantId, MqttPersistentSubCountGauge);
        assertNoGauge(tenantId, MqttPersistentSessionNumGauge);
        assertNoGauge(tenantId, MqttPersistentSessionSpaceGauge);
        // mock Supplier<Number> using Mockito
        Supplier<Number> usedSpaceGetter = () -> 1;
        TenantInboxSet inboxSet = new TenantInboxSet(tenantId, usedSpaceGetter);

        assertGaugeValue(tenantId, MqttPersistentSubCountGauge, 0);
        assertGaugeValue(tenantId, MqttPersistentSessionNumGauge, 0);
        assertGaugeValue(tenantId, MqttPersistentSessionSpaceGauge, 1);
        inboxSet.destroy();
    }

    @Test
    public void testMeterClearedAfterDestroy() {
        String tenantId = "tenantId-" + System.nanoTime();
        Supplier<Number> usedSpaceGetter = () -> 1;
        TenantInboxSet inboxSet = new TenantInboxSet(tenantId, usedSpaceGetter, "tag1", "value1");
        inboxSet.destroy();
        assertNoGauge(tenantId, MqttPersistentSubCountGauge);
        assertNoGauge(tenantId, MqttPersistentSessionNumGauge);
        assertNoGauge(tenantId, MqttPersistentSessionSpaceGauge);
    }

    @Test
    public void testUpsert() {
        String tenantId = "tenantId-" + System.nanoTime();
        Supplier<Number> usedSpaceGetter = Mockito.mock(Supplier.class);
        when(usedSpaceGetter.get()).thenReturn(1);
        InboxMetadata inboxMetadata = InboxMetadata.newBuilder()
            .setInboxId("testInboxId")
            .setIncarnation(1)
            .build();
        TenantInboxSet inboxSet = new TenantInboxSet(tenantId, usedSpaceGetter);
        inboxSet.upsert(inboxMetadata);
        assertGaugeValue(tenantId, MqttPersistentSubCountGauge, 0);
        assertGaugeValue(tenantId, MqttPersistentSessionNumGauge, 1);
        assertGaugeValue(tenantId, MqttPersistentSessionSpaceGauge, 1);

        inboxMetadata = InboxMetadata.newBuilder()
            .setInboxId("testInboxId")
            .setIncarnation(1)
            .putTopicFilters("topic1", TopicFilterOption.getDefaultInstance())
            .build();

        inboxSet.upsert(inboxMetadata);
        assertGaugeValue(tenantId, MqttPersistentSubCountGauge, 1);
        assertGaugeValue(tenantId, MqttPersistentSessionNumGauge, 1);
        assertGaugeValue(tenantId, MqttPersistentSessionSpaceGauge, 1);

        when(usedSpaceGetter.get()).thenReturn(2);
        inboxMetadata = InboxMetadata.newBuilder()
            .setInboxId("testInboxId1")
            .setIncarnation(1)
            .putTopicFilters("topic1", TopicFilterOption.getDefaultInstance())
            .build();

        inboxSet.upsert(inboxMetadata);
        assertGaugeValue(tenantId, MqttPersistentSubCountGauge, 2);
        assertGaugeValue(tenantId, MqttPersistentSessionNumGauge, 2);
        assertGaugeValue(tenantId, MqttPersistentSessionSpaceGauge, 2);
    }

    @Test
    public void testRemove() {
        String tenantId = "tenantId-" + System.nanoTime();
        Supplier<Number> usedSpaceGetter = Mockito.mock(Supplier.class);
        when(usedSpaceGetter.get()).thenReturn(1);
        InboxMetadata inboxMetadata = InboxMetadata.newBuilder()
            .setInboxId("testInboxId")
            .setIncarnation(1)
            .putTopicFilters("topic1", TopicFilterOption.getDefaultInstance())
            .build();
        InboxMetadata inboxMetadata1 = InboxMetadata.newBuilder()
            .setInboxId("testInboxId1")
            .setIncarnation(1)
            .putTopicFilters("topic1", TopicFilterOption.getDefaultInstance())
            .build();
        TenantInboxSet inboxSet = new TenantInboxSet(tenantId, usedSpaceGetter);
        inboxSet.upsert(inboxMetadata);
        inboxSet.upsert(inboxMetadata1);
        assertGaugeValue(tenantId, MqttPersistentSubCountGauge, 2);
        assertGaugeValue(tenantId, MqttPersistentSessionNumGauge, 2);

        inboxSet.remove(inboxMetadata.getInboxId(), inboxMetadata.getIncarnation());
        assertGaugeValue(tenantId, MqttPersistentSubCountGauge, 1);
        assertGaugeValue(tenantId, MqttPersistentSessionNumGauge, 1);

        assertFalse(inboxSet.isEmpty());
        inboxSet.remove(inboxMetadata1.getInboxId(), inboxMetadata1.getIncarnation());
        assertGaugeValue(tenantId, MqttPersistentSubCountGauge, 0);
        assertGaugeValue(tenantId, MqttPersistentSessionNumGauge, 0);
        assertTrue(inboxSet.isEmpty());
    }

    @Test
    public void testGetInboxMetadata() {
        String tenantId = "tenantId-" + System.nanoTime();
        Supplier<Number> usedSpaceGetter = Mockito.mock(Supplier.class);
        when(usedSpaceGetter.get()).thenReturn(1);
        InboxMetadata inboxMetadata = InboxMetadata.newBuilder()
            .setInboxId("testInboxId")
            .setIncarnation(1)
            .putTopicFilters("topic1", TopicFilterOption.getDefaultInstance())
            .build();
        TenantInboxSet inboxSet = new TenantInboxSet(tenantId, usedSpaceGetter);
        inboxSet.upsert(inboxMetadata);
        assertTrue(inboxSet.get(inboxMetadata.getInboxId(), inboxMetadata.getIncarnation()).isPresent());
        assertEquals(inboxSet.get(inboxMetadata.getInboxId(), inboxMetadata.getIncarnation()).get(), inboxMetadata);
        inboxSet.remove(inboxMetadata.getInboxId(), inboxMetadata.getIncarnation());
        assertFalse(inboxSet.get(inboxMetadata.getInboxId(), inboxMetadata.getIncarnation()).isPresent());
    }

    @Test
    public void testGetAll() {
        String tenantId = "tenantId-" + System.nanoTime();
        Supplier<Number> usedSpaceGetter = Mockito.mock(Supplier.class);
        when(usedSpaceGetter.get()).thenReturn(1);
        InboxMetadata inboxMetadata = InboxMetadata.newBuilder()
            .setInboxId("testInboxId")
            .setIncarnation(1)
            .putTopicFilters("topic1", TopicFilterOption.getDefaultInstance())
            .build();

        InboxMetadata inboxMetadata1 = InboxMetadata.newBuilder()
            .setInboxId("testInboxId")
            .setIncarnation(2)
            .putTopicFilters("topic1", TopicFilterOption.getDefaultInstance())
            .build();
        InboxMetadata inboxMetadata2 = InboxMetadata.newBuilder()
            .setInboxId("testInboxId1")
            .setIncarnation(1)
            .putTopicFilters("topic1", TopicFilterOption.getDefaultInstance())
            .build();
        TenantInboxSet inboxSet = new TenantInboxSet(tenantId, usedSpaceGetter);
        inboxSet.upsert(inboxMetadata);
        inboxSet.upsert(inboxMetadata1);
        inboxSet.upsert(inboxMetadata2);
        assertEquals(inboxSet.getAll().size(), 3);
        assertEquals(inboxSet.getAll(inboxMetadata.getInboxId()).size(), 2);
        assertEquals(inboxSet.getAll(inboxMetadata2.getInboxId()).size(), 1);
        inboxSet.destroy();
    }
}
