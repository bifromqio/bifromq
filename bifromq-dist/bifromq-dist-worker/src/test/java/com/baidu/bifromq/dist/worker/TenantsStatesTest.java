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

package com.baidu.bifromq.dist.worker;

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static com.baidu.bifromq.metrics.TenantMetric.MqttRouteSpaceGauge;
import static com.baidu.bifromq.metrics.TenantMetric.MqttSharedSubNumGauge;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.basekv.store.api.IKVReader;
import lombok.SneakyThrows;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TenantsStatesTest extends MeterTest {
    @Mock
    private IKVReader reader;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        super.setup();
        closeable = MockitoAnnotations.openMocks(this);
        when(reader.boundary()).thenReturn(FULL_BOUNDARY);
    }

    @SneakyThrows
    @AfterMethod
    public void tearDown() {
        closeable.close();
        super.tearDown();
    }

    @Test
    public void incShareRoute() {
        when(reader.size(any())).thenReturn(1L);
        String tenantId = "tenant" + System.nanoTime();
        TenantsState tenantsState = new TenantsState(reader);
        tenantsState.incSharedRoutes(tenantId);
        assertGaugeValue(tenantId, MqttSharedSubNumGauge, 1);
    }

    @Test
    public void testRemove() {
        when(reader.size(any())).thenReturn(1L);
        String tenantId = "tenant" + System.nanoTime();
        TenantsState tenantsState = new TenantsState(reader);
        tenantsState.incSharedRoutes(tenantId);
        tenantsState.incNormalRoutes(tenantId);
        assertGaugeValue(tenantId, MqttSharedSubNumGauge, 1);
        assertGauge(tenantId, MqttRouteSpaceGauge);

        tenantsState.decSharedRoutes(tenantId);
        assertGaugeValue(tenantId, MqttSharedSubNumGauge, 0);
        assertGauge(tenantId, MqttRouteSpaceGauge);
        tenantsState.decNormalRoutes(tenantId);
        assertNoGauge(tenantId, MqttRouteSpaceGauge);
        assertNoGauge(tenantId, MqttSharedSubNumGauge);
    }

    @Test
    public void testReset() {
        when(reader.size(any())).thenReturn(1L);
        String tenantId = "tenant" + System.nanoTime();
        TenantsState tenantsState = new TenantsState(reader);
        tenantsState.incNormalRoutes(tenantId);
        assertGauge(tenantId, MqttRouteSpaceGauge);
        assertGauge(tenantId, MqttSharedSubNumGauge);

        tenantsState.reset();
        assertNoGauge(tenantId, MqttRouteSpaceGauge);
        assertNoGauge(tenantId, MqttSharedSubNumGauge);
    }
}
