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

package com.baidu.bifromq.mqtt;

import static org.testng.AssertJUnit.assertEquals;
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
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class MQTTFanOutTest extends MQTTTest {
    private String trafficId = "testTraffic";
    private String deviceKey = "testDevice";

    @BeforeMethod(alwaysRun = true)
    public void setup() {
        super.setup();
        when(authProvider.auth(any(MQTT3AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                .setOk(Ok.newBuilder()
                    .setTrafficId(trafficId)
                    .setUserId(deviceKey)
                    .build())
                .build()));
        when(authProvider.check(any(ClientInfo.class), any(MQTTAction.class)))
            .thenReturn(CompletableFuture.completedFuture(true));
        doAnswer(invocationOnMock -> {
//            Event event = invocationOnMock.getArgument(0);
//            log.info("event: {}", event);
            return null;
        }).when(eventCollector).report(any(Event.class));
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
        connOpts.setUserName(trafficId + "/" + deviceKey);

        MqttTestClient pubClient = new MqttTestClient(brokerURI, "pubClient");
        pubClient.connect(connOpts);

        MqttTestClient subClient1 = new MqttTestClient(brokerURI, "subClient1");
        subClient1.connect(connOpts);
        MqttTestClient subClient2 = new MqttTestClient(brokerURI, "subClient2");
        subClient2.connect(connOpts);
        MqttTestClient subClient3 = new MqttTestClient(brokerURI, "subClient3");
        subClient3.connect(connOpts);

        Observable<MqttMsg> topicSub1 = subClient1.subscribe("#", 0);
        Observable<MqttMsg> topicSub2 = subClient2.subscribe(topic, 1);
        Observable<MqttMsg> topicSub3 = subClient3.subscribe("/a/+", 2);

        pubClient.publish(topic, pubQoS, ByteString.copyFromUtf8("hello"), false);

        MqttMsg msg1 = topicSub1.blockingFirst();
        assertEquals(topic, msg1.topic);
        assertEquals(Math.min(0, pubQoS), msg1.qos);
        assertFalse(msg1.isDup);
        assertFalse(msg1.isRetain);
        assertEquals(ByteString.copyFromUtf8("hello"), msg1.payload);

        MqttMsg msg2 = topicSub2.blockingFirst();
        assertEquals(topic, msg2.topic);
        assertEquals(Math.min(1, pubQoS), msg2.qos);
        assertFalse(msg2.isDup);
        assertFalse(msg2.isRetain);
        assertEquals(ByteString.copyFromUtf8("hello"), msg2.payload);

        MqttMsg msg3 = topicSub3.blockingFirst();
        assertEquals(topic, msg3.topic);
        assertEquals(Math.min(2, pubQoS), msg3.qos);
        assertFalse(msg3.isDup);
        assertFalse(msg3.isRetain);
        assertEquals(ByteString.copyFromUtf8("hello"), msg3.payload);

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
