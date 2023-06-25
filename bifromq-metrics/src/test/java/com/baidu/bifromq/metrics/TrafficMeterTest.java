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

package com.baidu.bifromq.metrics;

import static com.baidu.bifromq.metrics.TrafficMeter.TAG_TRAFFIC_ID;
import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertTrue;

import io.micrometer.core.instrument.Metrics;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class TrafficMeterTest {
    @Test
    public void get() throws InterruptedException {
        String trafficId = "testing_traffic";
        TrafficMeter meter = TrafficMeter.get(trafficId);
        meter.recordCount(TrafficMetric.MqttConnectCount);
        assertTrue(Metrics.globalRegistry.getMeters().stream()
            .anyMatch(m -> trafficId.equals(m.getId().getTag(TAG_TRAFFIC_ID))));
        meter = null;
        System.gc();
        TrafficMeter.cleanUp();
        System.gc();
        Thread.sleep(100);
        await().until(() -> {
            Thread.sleep(100);
            return Metrics.globalRegistry.getMeters().stream()
                .noneMatch(m -> trafficId.equals(m.getId().getTag(TAG_TRAFFIC_ID)));
        });
    }
}
