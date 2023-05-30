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

package com.baidu.bifromq.dist.worker;

import static com.baidu.bifromq.metrics.TrafficMetric.DistSubInfoSizeGauge;
import static com.baidu.bifromq.type.QoS.AT_MOST_ONCE;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.dist.client.ClearResult;
import com.baidu.bifromq.plugin.inboxbroker.HasResult;
import com.baidu.bifromq.type.ClientInfo;
import io.micrometer.core.instrument.Meter;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class GCAndStatsTest extends DistWorkerTest {
    @Test
    public void gc() {
        when(receiverManager.hasInbox(anyLong(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(HasResult.NO));

        when(distClient.clear(anyLong(), anyString(), anyString(), anyInt(), any(ClientInfo.class)))
            .thenReturn(CompletableFuture.completedFuture(ClearResult.OK));

        addTopicFilter("trafficA", "/a/b/c", AT_MOST_ONCE, MqttBroker, "inbox1", "server1");
        insertMatchRecord("trafficA", "/a/b/c", AT_MOST_ONCE, MqttBroker, "inbox1", "server1");

        addTopicFilter("trafficB", "/#", AT_MOST_ONCE, InboxService, "inbox2", "server2");
        insertMatchRecord("trafficB", "/#", AT_MOST_ONCE, InboxService, "inbox2", "server2");

        await().until(() -> {
            try {
                verify(distClient, times(2)).clear(anyLong(), anyString(), anyString(), anyInt(),
                    any(ClientInfo.class));
                return true;
            } catch (Throwable e) {
                return false;
            }
        });
    }

    @SneakyThrows
    @Test
    public void reportRangeMetrics() {
        addTopicFilter("trafficA", "/a/b/c", AT_MOST_ONCE, MqttBroker, "inbox1", "server1");
        insertMatchRecord("trafficA", "/a/b/c", AT_MOST_ONCE, MqttBroker, "inbox1", "server1");

        addTopicFilter("trafficB", "/#", AT_MOST_ONCE, InboxService, "inbox2", "server2");
        insertMatchRecord("trafficB", "/#", AT_MOST_ONCE, InboxService, "inbox2", "server2");

        await().until(() -> {
            for (Meter meter : meterRegistry.getMeters()) {
                if (meter.getId().getType() == Meter.Type.GAUGE &&
                    meter.getId().getName().equals(DistSubInfoSizeGauge.metricName)) {
                    return true;
                }
            }
            return false;
        });
    }
}
