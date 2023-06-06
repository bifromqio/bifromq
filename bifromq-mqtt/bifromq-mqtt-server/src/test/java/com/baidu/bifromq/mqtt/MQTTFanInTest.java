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

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.mqtt.client.MqttMsg;
import com.baidu.bifromq.mqtt.client.MqttTestClient;
import com.baidu.bifromq.plugin.authprovider.ActionInfo;
import com.baidu.bifromq.plugin.authprovider.AuthData;
import com.baidu.bifromq.plugin.authprovider.AuthResult;
import com.baidu.bifromq.plugin.authprovider.CheckResult;
import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.type.ClientInfo;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.observers.TestObserver;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class MQTTFanInTest extends MQTTTest {
    private String trafficId = "testTraffic";
    private String deviceKey = "testDevice";

    @Before
    public void setup() {
        super.setup();
        when(authProvider.auth(any(AuthData.class)))
            .thenReturn(CompletableFuture.completedFuture(AuthResult.pass()
                .trafficId(trafficId)
                .userId(deviceKey)
                .build()));
        when(authProvider.check(any(ClientInfo.class), any(ActionInfo.class)))
            .thenReturn(CompletableFuture.completedFuture(CheckResult.ALLOW));
        doAnswer(invocationOnMock -> {
//            Event event = invocationOnMock.getArgument(0);
//            log.info("event: {}", event);
            return null;
        }).when(eventCollector).report(any(Event.class));
    }

    @Test
    public void fanin() {
        fanin(0);
        fanin(1);
        fanin(2);
    }

    public void fanin(int subQoS) {
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        connOpts.setUserName(trafficId + "/" + deviceKey);

        MqttTestClient pubClient1 = new MqttTestClient(brokerURI, "pubClient1");
        pubClient1.connect(connOpts);

        MqttTestClient pubClient2 = new MqttTestClient(brokerURI, "pubClient2");
        pubClient2.connect(connOpts);

        MqttTestClient pubClient3 = new MqttTestClient(brokerURI, "pubClient3");
        pubClient3.connect(connOpts);

        MqttTestClient subClient = new MqttTestClient(brokerURI, "subClient");
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
                assertEquals(Math.min(0, subQoS), m.qos);
                assertFalse(m.isDup);
                assertFalse(m.isRetain);
                assertEquals(ByteString.copyFromUtf8("hello"), m.payload);
            }
            if (m.topic.equals("/a/" + subQoS)) {
                assertEquals(Math.min(1, subQoS), m.qos);
                assertFalse(m.isDup);
                assertFalse(m.isRetain);
                assertEquals(ByteString.copyFromUtf8("world"), m.payload);
            }
            if (m.topic.equals("/a/b" + subQoS)) {
                assertEquals(Math.min(2, subQoS), m.qos);
                assertFalse(m.isDup);
                assertFalse(m.isRetain);
                assertEquals(ByteString.copyFromUtf8("greeting"), m.payload);
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
