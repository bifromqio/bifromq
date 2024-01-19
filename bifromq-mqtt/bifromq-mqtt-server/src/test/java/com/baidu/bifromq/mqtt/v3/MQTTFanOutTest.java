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

package com.baidu.bifromq.mqtt.v3;

import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.reset;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.mqtt.v3.client.MqttMsg;
import com.baidu.bifromq.mqtt.v3.client.MqttTestClient;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthData;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import com.baidu.bifromq.plugin.authprovider.type.MQTTAction;
import com.baidu.bifromq.plugin.authprovider.type.Ok;
import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.type.ClientInfo;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class MQTTFanOutTest {
    private final MQTTTest mqttTest = MQTTTest.getInstance();
    private final String tenantId = "testFanOutTraffic";
    private final String deviceKey = "testDevice";

    @BeforeClass(alwaysRun = true)
    public void setup() {
        when(mqttTest.authProvider.auth(any(MQTT3AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                .setOk(Ok.newBuilder()
                    .setTenantId(tenantId)
                    .setUserId(deviceKey)
                    .build())
                .build()));
        when(mqttTest.authProvider.check(any(ClientInfo.class), any(MQTTAction.class)))
            .thenReturn(CompletableFuture.completedFuture(true));
        doAnswer(invocationOnMock -> {
//            Event event = invocationOnMock.getArgument(0);
//            log.info("event: {}", event);
            return null;
        }).when(mqttTest.eventCollector).report(any(Event.class));
    }

    @AfterClass(alwaysRun = true)
    public void resetMocks() {
        reset(mqttTest.authProvider, mqttTest.eventCollector);
        clearInvocations(mqttTest.eventCollector);
    }

    @Test(groups = "integration")
    public void fanout() {
        fanout(0);
        fanout(1);
        fanout(2);
    }

    public void fanout(int pubQoS) {
        String topic = "/a/" + pubQoS;
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        connOpts.setUserName(tenantId + "/" + deviceKey);

        MqttTestClient pubClient = new MqttTestClient(MQTTTest.brokerURI, "pubClient");
        pubClient.connect(connOpts);

        MqttTestClient subClient1 = new MqttTestClient(MQTTTest.brokerURI, "subClient1");
        subClient1.connect(connOpts);
        MqttTestClient subClient2 = new MqttTestClient(MQTTTest.brokerURI, "subClient2");
        subClient2.connect(connOpts);
        MqttTestClient subClient3 = new MqttTestClient(MQTTTest.brokerURI, "subClient3");
        subClient3.connect(connOpts);

        Observable<MqttMsg> topicSub1 = subClient1.subscribe("#", 0);
        Observable<MqttMsg> topicSub2 = subClient2.subscribe(topic, 1);
        Observable<MqttMsg> topicSub3 = subClient3.subscribe("/a/+", 2);

        pubClient.publish(topic, pubQoS, ByteString.copyFromUtf8("hello"), false);

        MqttMsg msg1 = topicSub1.blockingFirst();
        assertEquals(msg1.topic, topic);
        assertEquals(msg1.qos, Math.min(0, pubQoS));
        assertFalse(msg1.isDup);
        assertFalse(msg1.isRetain);
        assertEquals(msg1.payload, ByteString.copyFromUtf8("hello"));

        MqttMsg msg2 = topicSub2.blockingFirst();
        assertEquals(msg2.topic, topic);
        assertEquals(msg2.qos, Math.min(1, pubQoS));
        assertFalse(msg2.isDup);
        assertFalse(msg2.isRetain);
        assertEquals(msg2.payload, ByteString.copyFromUtf8("hello"));

        MqttMsg msg3 = topicSub3.blockingFirst();
        assertEquals(msg3.topic, topic);
        assertEquals(msg3.qos, Math.min(2, pubQoS));
        assertFalse(msg3.isDup);
        assertFalse(msg3.isRetain);
        assertEquals(msg3.payload, ByteString.copyFromUtf8("hello"));

        // TODO: verify event collected

        pubClient.disconnect();
        subClient1.disconnect();
        subClient2.disconnect();
        subClient3.disconnect();

        pubClient.close();
        subClient1.close();
        subClient2.close();
        subClient3.close();
    }
}
