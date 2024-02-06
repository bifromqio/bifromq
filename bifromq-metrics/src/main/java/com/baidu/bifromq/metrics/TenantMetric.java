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

package com.baidu.bifromq.metrics;

import io.micrometer.core.instrument.Meter;

public enum TenantMetric {
    MqttConnectionGauge("mqtt.connection.num.gauge", Meter.Type.GAUGE),
    MqttConnectCount("mqtt.connect.count", Meter.Type.COUNTER),
    MqttDisconnectCount("mqtt.disconnect.count", Meter.Type.COUNTER),
    MqttIngressBytes("mqtt.ingress.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttEgressBytes("mqtt.egress.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttQoS0IngressBytes("mqtt.ingress.qos0.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttQoS0DistBytes("mqtt.dist.qos0.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttQoS1IngressBytes("mqtt.ingress.qos1.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttQoS1DistBytes("mqtt.dist.qos1.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttQoS2IngressBytes("mqtt.ingress.qos2.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttQoS2DistBytes("mqtt.dist.qos2.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttQoS0EgressBytes("mqtt.egress.qos0.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttQoS1EgressBytes("mqtt.egress.qos1.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttQoS1DeliverBytes("mqtt.deliver.qos1.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttQoS2EgressBytes("mqtt.egress.qos2.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttQoS2DeliverBytes("mqtt.deliver.qos2.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttQoS0InternalLatency("mqtt.in.qos0.latency", Meter.Type.TIMER),
    MqttQoS1InternalLatency("mqtt.in.qos1.latency", Meter.Type.TIMER),
    MqttQoS1ExternalLatency("mqtt.ex.qos1.latency", Meter.Type.TIMER),
    MqttQoS2InternalLatency("mqtt.in.qos2.latency", Meter.Type.TIMER),
    MqttQoS2ExternalLatency("mqtt.ex.qos2.latency", Meter.Type.TIMER),
    MqttChannelLatency("mqtt.channel.latency", Meter.Type.TIMER),
    DistUsedSpaceGauge("dist.used.space.gauge", Meter.Type.GAUGE),
    InboxUsedSpaceGauge("inbox.used.space.gauge", Meter.Type.GAUGE),
    RetainUsedSpaceGauge("retain.used.space.gauge", Meter.Type.GAUGE),
    InboxFetcherGauge("inbox.fetcher.num.gauge", Meter.Type.GAUGE);

    public final String metricName;
    public final Meter.Type meterType;

    TenantMetric(String metricName, Meter.Type meterType) {
        this.metricName = metricName;
        this.meterType = meterType;
    }
}
