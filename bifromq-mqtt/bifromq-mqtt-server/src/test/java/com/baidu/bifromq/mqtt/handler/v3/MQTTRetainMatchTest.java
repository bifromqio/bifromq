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

package com.baidu.bifromq.mqtt.handler.v3;


import static com.baidu.bifromq.plugin.eventcollector.EventType.CLIENT_CONNECTED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.MATCH_RETAIN_ERROR;
import static com.baidu.bifromq.plugin.eventcollector.EventType.QOS0_PUSHED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.QOS1_PUSHED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.QOS2_PUSHED;
import static com.baidu.bifromq.plugin.eventcollector.EventType.SUB_ACKED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.dist.client.ClearResult;
import com.baidu.bifromq.mqtt.handler.BaseMQTTTest;
import com.baidu.bifromq.mqtt.utils.MQTTMessageUtils;
import com.baidu.bifromq.retain.rpc.proto.MatchReply;
import com.baidu.bifromq.retain.rpc.proto.MatchReply.Builder;
import com.baidu.bifromq.retain.rpc.proto.MatchReply.Result;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.TopicMessage;
import com.google.protobuf.ByteString;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Slf4j
public class MQTTRetainMatchTest extends BaseMQTTTest {

    @AfterMethod
    public void clean() {
        when(distClient.clear(anyLong(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(ClearResult.OK));
        channel.close();
        verify(distClient).clear(anyLong(), anyString(), anyString(), anyString(), anyInt());
    }

    @Test
    public void qoS0Match() {
        connectAndVerify(true);
        mockAuthCheck(true);
        mockDistSub(QoS.AT_MOST_ONCE, true);
        mockRetainMatch(1, QoS.EXACTLY_ONCE);
        int[] qos = {0, 0, 0};
        MqttSubscribeMessage subMessage = MQTTMessageUtils.qoSMqttSubMessages(qos);
        channel.writeInbound(subMessage);
        for (int i = 0; i < qos.length; i++) {
            MqttPublishMessage publishMessage = channel.readOutbound();
            assertEquals(publishMessage.variableHeader().topicName(), "testTopic");
            assertTrue(publishMessage.fixedHeader().isRetain());
        }
        MqttSubAckMessage subAckMessage = channel.readOutbound();
        verifySubAck(subAckMessage, qos);
        verifyEvent(5, CLIENT_CONNECTED, QOS0_PUSHED, QOS0_PUSHED, QOS0_PUSHED, SUB_ACKED);
    }

    @Test
    public void qoS1Match() {
        connectAndVerify(true);
        mockAuthCheck(true);
        mockDistSub(QoS.AT_LEAST_ONCE, true);
        mockRetainMatch(1, QoS.EXACTLY_ONCE);
        int[] qos = {1, 1, 1};
        MqttSubscribeMessage subMessage = MQTTMessageUtils.qoSMqttSubMessages(qos);
        channel.writeInbound(subMessage);
        for (int i = 0; i < qos.length; i++) {
            MqttPublishMessage publishMessage = channel.readOutbound();
            assertEquals(publishMessage.variableHeader().topicName(), "testTopic");
            assertTrue(publishMessage.fixedHeader().isRetain());
        }
        MqttSubAckMessage subAckMessage = channel.readOutbound();
        verifySubAck(subAckMessage, qos);
        verifyEvent(5, CLIENT_CONNECTED, QOS1_PUSHED, QOS1_PUSHED, QOS1_PUSHED, SUB_ACKED);
    }

    @Test
    public void qoS2Match() {
        connectAndVerify(true);
        mockAuthCheck(true);
        mockDistSub(QoS.EXACTLY_ONCE, true);
        mockRetainMatch(1, QoS.EXACTLY_ONCE);
        int[] qos = {2, 2, 2};
        MqttSubscribeMessage subMessage = MQTTMessageUtils.qoSMqttSubMessages(qos);
        channel.writeInbound(subMessage);
        for (int i = 0; i < qos.length; i++) {
            MqttPublishMessage publishMessage = channel.readOutbound();
            assertEquals(publishMessage.variableHeader().topicName(), "testTopic");
            assertTrue(publishMessage.fixedHeader().isRetain());
        }
        MqttSubAckMessage subAckMessage = channel.readOutbound();
        verifySubAck(subAckMessage, qos);
        verifyEvent(5, CLIENT_CONNECTED, QOS2_PUSHED, QOS2_PUSHED, QOS2_PUSHED, SUB_ACKED);
    }

    @Test
    public void mixedMatch() {
        connectAndVerify(true);
        mockAuthCheck(true);
        mockDistSub(QoS.AT_MOST_ONCE, true);
        mockDistSub(QoS.AT_LEAST_ONCE, true);
        mockDistSub(QoS.EXACTLY_ONCE, true);
        mockRetainMatch(1, QoS.EXACTLY_ONCE);
        int[] qos = {0, 1, 2};
        MqttSubscribeMessage subMessage = MQTTMessageUtils.qoSMqttSubMessages(qos);
        channel.writeInbound(subMessage);
        for (int i = 0; i < qos.length; i++) {
            MqttPublishMessage publishMessage = channel.readOutbound();
            assertEquals(publishMessage.variableHeader().topicName(), "testTopic");
            assertTrue(publishMessage.fixedHeader().isRetain());
        }
        MqttSubAckMessage subAckMessage = channel.readOutbound();
        verifySubAck(subAckMessage, qos);
        verifyEvent(5, CLIENT_CONNECTED, QOS2_PUSHED, QOS1_PUSHED, QOS0_PUSHED, SUB_ACKED);
    }

    @Test
    public void qoS0MatchFailed() {
        connectAndVerify(true);
        mockAuthCheck(true);
        mockDistSub(QoS.AT_MOST_ONCE, true);
        when(retainClient.match(anyLong(), anyString(), anyString(), anyInt(), any(ClientInfo.class)))
            .thenReturn(CompletableFuture.completedFuture(
                MatchReply.newBuilder().setResult(Result.ERROR).build()
            ));
        int[] qos = {0, 0, 0};
        MqttSubscribeMessage subMessage = MQTTMessageUtils.qoSMqttSubMessages(qos);
        channel.writeInbound(subMessage);
        MqttSubAckMessage subAckMessage = channel.readOutbound();
        verifySubAck(subAckMessage, new int[] {128, 128, 128});
        verifyEvent(5, CLIENT_CONNECTED, MATCH_RETAIN_ERROR, MATCH_RETAIN_ERROR, MATCH_RETAIN_ERROR, SUB_ACKED);
    }

    private void verifySubAck(MqttSubAckMessage subAckMessage, int[] expectedQos) {
        assertEquals(subAckMessage.payload().grantedQoSLevels().size(), expectedQos.length);
        for (int i = 0; i < expectedQos.length; i++) {
            assertEquals(expectedQos[i], (int) subAckMessage.payload().grantedQoSLevels().get(i));
        }
    }

    private void mockRetainMatch(int count, QoS qos) {
        Builder builder = MatchReply.newBuilder();
        for (int i = 0; i < count; i++) {
            builder.setResult(Result.OK)
                .addMessages(
                    TopicMessage.newBuilder()
                        .setTopic("testTopic")
                        .setMessage(
                            Message.newBuilder()
                                .setMessageId(1)
                                .setPayload(ByteString.copyFromUtf8("payload"))
                                .setTimestamp(System.currentTimeMillis())
                                .setPubQoS(QoS.EXACTLY_ONCE)
                                .build()
                        )
                        .setPublisher(ClientInfo.newBuilder().build())
                        .build()
                );
        }
        when(retainClient.match(anyLong(), anyString(), anyString(), anyInt(), any(ClientInfo.class)))
            .thenReturn(CompletableFuture.completedFuture(builder.build()));
    }

}
