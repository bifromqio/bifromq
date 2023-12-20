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

package com.baidu.bifromq.mqtt;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.reset;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.mqtt.client.MqttMsg;
import com.baidu.bifromq.mqtt.client.MqttTestClient;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthData;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import com.baidu.bifromq.plugin.authprovider.type.MQTTAction;
import com.baidu.bifromq.plugin.authprovider.type.Ok;
import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.type.ClientInfo;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.observers.TestObserver;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class MQTTFanInTest {
    private final MQTTTest mqttTest = MQTTTest.getInstance();
    private final String tenantId = "testFanInTraffic";
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
    public void fanin() {
        fanin(0);
        fanin(1);
        fanin(2);
    }

    public void fanin(int subQoS) {
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        connOpts.setUserName(tenantId + "/" + deviceKey);

        MqttTestClient pubClient1 = new MqttTestClient(MQTTTest.brokerURI, "pubClient1");
        pubClient1.connect(connOpts);

        MqttTestClient pubClient2 = new MqttTestClient(MQTTTest.brokerURI, "pubClient2");
        pubClient2.connect(connOpts);

        MqttTestClient pubClient3 = new MqttTestClient(MQTTTest.brokerURI, "pubClient3");
        pubClient3.connect(connOpts);

        MqttTestClient subClient = new MqttTestClient(MQTTTest.brokerURI, "subClient");
        subClient.connect(connOpts);

        Observable<MqttMsg> topicSub = subClient.subscribe("#", subQoS);
        TestObserver<MqttMsg> testObserver = TestObserver.create();
        topicSub.subscribe(testObserver);

        pubClient1.publish("/" + subQoS, 0, ByteString.copyFromUtf8("hello"), false);
        pubClient2.publish("/a/" + subQoS, 1, ByteString.copyFromUtf8("world"), false);
        pubClient3.publish("/a/b" + subQoS, 2, ByteString.copyFromUtf8("greeting"), false);

        await().atMost(Duration.ofSeconds(10)).until(() -> testObserver.values().size() >= 2);

        for (MqttMsg m : testObserver.values()) {
            if (m.topic.equals("/" + subQoS)) {
                assertEquals(m.qos, Math.min(0, subQoS));
                assertFalse(m.isDup);
                assertFalse(m.isRetain);
                assertEquals(m.payload, ByteString.copyFromUtf8("hello"));
            }
            if (m.topic.equals("/a/" + subQoS)) {
                assertEquals(m.qos, Math.min(1, subQoS));
                assertFalse(m.isDup);
                assertFalse(m.isRetain);
                assertEquals(m.payload, ByteString.copyFromUtf8("world"));
            }
            if (m.topic.equals("/a/b" + subQoS)) {
                assertEquals(m.qos, Math.min(2, subQoS));
                assertFalse(m.isDup);
                assertFalse(m.isRetain);
                assertEquals(m.payload, ByteString.copyFromUtf8("greeting"));
            }
        }

        // TODO: verify event collected

        pubClient1.disconnect();
        pubClient2.disconnect();
        pubClient3.disconnect();
        subClient.disconnect();

        pubClient1.close();
        pubClient2.close();
        pubClient3.close();
        subClient.close();
    }
}
