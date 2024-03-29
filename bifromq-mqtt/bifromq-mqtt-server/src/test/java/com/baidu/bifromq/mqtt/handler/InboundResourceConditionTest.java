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

import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.mqtt.MockableTest;
import com.baidu.bifromq.mqtt.utils.MemInfo;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.type.ClientInfo;
import com.bifromq.plugin.resourcethrottler.IResourceThrottler;
import com.bifromq.plugin.resourcethrottler.TenantResourceType;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.testng.annotations.Test;

public class InboundResourceConditionTest extends MockableTest {
    @Mock
    public IEventCollector eventCollector;
    @Mock
    public IResourceThrottler resourceThrottler;
    public ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId("tenantA").build();

    @Test
    public void testNoInboundResource() {
        InboundResourceCondition condition =
            new InboundResourceCondition(resourceThrottler, eventCollector, clientInfo);
        when(resourceThrottler.hasResource(clientInfo.getTenantId(),
            TenantResourceType.TotalInboundBytesPerSecond)).thenReturn(false);
        assertTrue(condition.get());

        when(resourceThrottler.hasResource(clientInfo.getTenantId(),
            TenantResourceType.TotalInboundBytesPerSecond)).thenReturn(true);
        assertFalse(condition.get());
    }

    @Test
    public void testNoMemPressure() {
        try (MockedStatic<MemInfo> mockedStatic = mockStatic(MemInfo.class)) {
            mockedStatic.when(MemInfo::directMemoryUsage).thenReturn(0.9);
            InboundResourceCondition condition =
                new InboundResourceCondition(resourceThrottler, eventCollector, clientInfo);
            when(resourceThrottler.hasResource(clientInfo.getTenantId(),
                TenantResourceType.TotalInboundBytesPerSecond)).thenReturn(true);
            assertTrue(condition.get());

            mockedStatic.when(MemInfo::directMemoryUsage).thenReturn(0.2);
            assertFalse(condition.get());

            when(resourceThrottler.hasResource(clientInfo.getTenantId(),
                TenantResourceType.TotalInboundBytesPerSecond)).thenReturn(false);
            assertTrue(condition.get());
        }
    }
}
