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

package com.baidu.bifromq.deliverer;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import com.baidu.bifromq.plugin.subbroker.DeliveryResult;
import com.baidu.bifromq.plugin.subbroker.IDeliverer;
import com.baidu.bifromq.plugin.subbroker.ISubBroker;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DeliverySchedulerTest {
    @Mock
    private ISubBrokerManager subBrokerManager;
    @Mock
    private ISubBroker subBroker;
    @Mock
    private IDeliverer groupWriter;
    private IMessageDeliverer testDeliverer;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        when(subBrokerManager.get(0)).thenReturn(subBroker);
        when(subBroker.open(anyString())).thenReturn(groupWriter);
        when(subBroker.id()).thenReturn(0);
        testDeliverer = new MessageDeliverer(subBrokerManager);
    }

    @SneakyThrows
    @AfterMethod
    public void teardown() {
        closeable.close();
    }

    @Test
    public void writeSucceed() {
        SubInfo subInfo = SubInfo.newBuilder().build();
        DeliveryRequest request = new DeliveryRequest(subInfo, 0, "group1", TopicMessagePack.newBuilder().build());
        when(groupWriter.deliver(anyList())).thenReturn(
            CompletableFuture.completedFuture(Collections.singletonMap(subInfo, DeliveryResult.OK)));
        DeliveryResult result = testDeliverer.schedule(request).join();
        assertEquals(result, DeliveryResult.OK);
    }

    @Test
    public void writeIncompleteResult() {
        SubInfo subInfo = SubInfo.newBuilder().build();
        DeliveryRequest request = new DeliveryRequest(subInfo, 0, "group1", TopicMessagePack.newBuilder().build());
        when(groupWriter.deliver(anyList())).thenReturn(CompletableFuture.completedFuture(Collections.emptyMap()));
        DeliveryResult result = testDeliverer.schedule(request).join();
        assertEquals(result, DeliveryResult.OK);
    }

    @Test
    public void writeNoInbox() {
        SubInfo subInfo = SubInfo.newBuilder().build();
        DeliveryRequest request = new DeliveryRequest(subInfo, 0, "group1", TopicMessagePack.newBuilder().build());
        when(groupWriter.deliver(anyList())).thenReturn(
            CompletableFuture.completedFuture(Collections.singletonMap(subInfo, DeliveryResult.NO_INBOX)));
        DeliveryResult result = testDeliverer.schedule(request).join();
        assertEquals(result, DeliveryResult.NO_INBOX);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void writeFail() {
        SubInfo subInfo = SubInfo.newBuilder().build();
        DeliveryRequest request = new DeliveryRequest(subInfo, 0, "group1", TopicMessagePack.newBuilder().build());
        when(groupWriter.deliver(anyList())).thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("Mock Exception")));
        testDeliverer.schedule(request).join();
        fail();
    }
}
