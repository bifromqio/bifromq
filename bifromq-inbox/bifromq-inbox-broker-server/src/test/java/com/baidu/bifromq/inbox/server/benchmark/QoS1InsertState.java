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

package com.baidu.bifromq.inbox.server.benchmark;

import static com.baidu.bifromq.type.QoS.AT_LEAST_ONCE;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;

import com.baidu.bifromq.plugin.subbroker.DeliveryPack;
import com.baidu.bifromq.plugin.subbroker.IDeliverer;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.protobuf.ByteString;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

@Slf4j
@State(Scope.Benchmark)
public class QoS1InsertState extends InboxServiceState {
    private static final String tenantId = "testTraffic";
    private TopicMessagePack msgs;
    private static final int inboxCount = 100;
    private IDeliverer inboxWriter;

    @Override
    protected void afterSetup() {
        int i = 0;
        while (i < inboxCount) {
            inboxReaderClient.create(System.nanoTime(), String.valueOf(i), ClientInfo.newBuilder()
                .setTenantId(tenantId)
                .build()).join();
            i++;
        }
        inboxWriter = inboxBrokerClient.open("deliverer1");
        TopicMessagePack.Builder builder = TopicMessagePack.newBuilder().setTopic("greeting");
        IntStream.range(0, 10).forEach(j -> builder
            .addMessage(TopicMessagePack.PublisherPack.newBuilder()
                .addMessage(Message.newBuilder()
                    .setPubQoS(AT_LEAST_ONCE)
                    .setPayload(ByteString.copyFromUtf8("hello"))
                    .build())
                .build())
        );
        msgs = builder.build();
    }

    @Override
    protected void beforeTeardown() {
        inboxWriter.close();
    }

    public void insert() {
        inboxWriter.deliver(singleton(new DeliveryPack(msgs, singletonList(SubInfo.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(String.valueOf(ThreadLocalRandom.current().nextInt(0, inboxCount)))
            .setTopicFilter("greeting")
            .setSubQoS(AT_LEAST_ONCE)
            .build())))).join();
    }
}
