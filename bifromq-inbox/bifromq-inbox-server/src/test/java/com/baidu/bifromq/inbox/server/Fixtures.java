/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.inbox.server;

import com.baidu.bifromq.inbox.rpc.proto.SendRequest;
import com.baidu.bifromq.plugin.subbroker.DeliveryPack;
import com.baidu.bifromq.plugin.subbroker.DeliveryPackage;
import com.baidu.bifromq.plugin.subbroker.DeliveryRequest;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.MatchInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.StringPair;
import com.baidu.bifromq.type.TopicMessagePack;
import com.baidu.bifromq.type.UserProperties;
import com.google.protobuf.ByteString;

import static com.baidu.bifromq.inbox.util.InboxServiceUtil.receiverId;

public class Fixtures {

    static SendRequest sendRequest() {
        Message message = Message.newBuilder()
            .setMessageId(1L)
            .setPubQoS(QoS.AT_MOST_ONCE)
            .setPayload(ByteString.EMPTY)
            .setTimestamp(System.currentTimeMillis())
            .setIsRetain(false)
            .setIsRetained(false)
            .setIsUTF8String(false)
            .setUserProperties(UserProperties.newBuilder()
                .addUserProperties(StringPair.newBuilder()
                    .setKey("foo_key")
                    .setValue("foo_val")
                    .build())
                .build())
            .build();
        TopicMessagePack.PublisherPack publisherPack = TopicMessagePack.PublisherPack.newBuilder()
            .setPublisher(ClientInfo
                .newBuilder()
                .setTenantId("iot_bar")
                .setType("type")
                .build())
            .addMessage(message)
            .build();
        TopicMessagePack topicMessagePack = TopicMessagePack.newBuilder()
            .setTopic("/foo/bar/baz")
            .addMessage(publisherPack)
            .build();
        DeliveryPack deliveryPack = DeliveryPack.newBuilder()
            .addMatchInfo(matchInfo())
            .setMessagePack(topicMessagePack)
            .build();
        DeliveryRequest deliveryRequest = DeliveryRequest.newBuilder()
            .putPackage("_", DeliveryPackage.newBuilder()
                .addPack(deliveryPack)
                .build())
            .build();

        return SendRequest.newBuilder()
            .setRequest(deliveryRequest)
            .build();
    }

    static MatchInfo matchInfo() {
        return MatchInfo.newBuilder()
            .setTopicFilter("/foo/+")
            .setIncarnation(1L)
            .setReceiverId(receiverId("foo", 1L))
            .build();
    }

}