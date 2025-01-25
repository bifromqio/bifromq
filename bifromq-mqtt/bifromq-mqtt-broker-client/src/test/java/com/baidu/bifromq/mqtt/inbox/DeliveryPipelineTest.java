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

package com.baidu.bifromq.mqtt.inbox;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.baserpc.client.IRPCClient;
import com.baidu.bifromq.baserpc.client.exception.ServiceUnavailableException;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.WriteReply;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.WriteRequest;
import com.baidu.bifromq.plugin.subbroker.DeliveryPack;
import com.baidu.bifromq.plugin.subbroker.DeliveryPackage;
import com.baidu.bifromq.plugin.subbroker.DeliveryReply;
import com.baidu.bifromq.plugin.subbroker.DeliveryRequest;
import com.baidu.bifromq.plugin.subbroker.DeliveryResult;
import com.baidu.bifromq.plugin.subbroker.DeliveryResults;
import com.baidu.bifromq.type.MatchInfo;
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DeliveryPipelineTest {
    private AutoCloseable closeable;
    @Mock
    private IRPCClient.IRequestPipeline<WriteRequest, WriteReply> pipeline;
    private DeliveryPipeline deliveryPipeline;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        deliveryPipeline = new DeliveryPipeline(pipeline);
    }

    @SneakyThrows
    @AfterMethod
    public void tearDown(Method method) {
        deliveryPipeline.close();
        closeable.close();
    }

    @Test
    public void testDeliverSuccess() {
        DeliveryRequest request = buildSampleDeliveryRequest();
        WriteReply successReply = WriteReply.newBuilder()
            .setReply(DeliveryReply.newBuilder()
                .putResult("tenant1", DeliveryResults.newBuilder()
                    .addResult(DeliveryResult.newBuilder()
                        .setCode(DeliveryResult.Code.OK)
                        .build())
                    .build())
                .build())
            .build();

        when(pipeline.invoke(any())).thenReturn(CompletableFuture.completedFuture(successReply));

        DeliveryReply reply = deliveryPipeline.deliver(request).join();

        assertEquals(reply.getResultMap().size(), 1);
        assertEquals(reply.getResultMap().get("tenant1").getResultList().get(0).getCode(), DeliveryResult.Code.OK);
    }

    @Test
    public void testDeliverWithServiceUnavailable() {
        DeliveryRequest request = buildSampleDeliveryRequest();

        when(pipeline.invoke(any())).thenReturn(CompletableFuture.failedFuture(
            new ServiceUnavailableException("Service unavailable")));

        DeliveryReply reply = deliveryPipeline.deliver(request).join();

        assertEquals(reply.getResultMap().get("tenant1").getResultList().get(0).getCode(),
            DeliveryResult.Code.NO_RECEIVER);
    }

    @Test
    public void testDeliverWithGeneralError() {
        DeliveryRequest request = buildSampleDeliveryRequest();

        when(pipeline.invoke(any())).thenReturn(CompletableFuture.failedFuture(new RuntimeException("Generic error")));

        DeliveryReply reply = deliveryPipeline.deliver(request).join();

        assertEquals(reply.getResultMap().get("tenant1").getResultList().get(0).getCode(), DeliveryResult.Code.ERROR);
    }

    private DeliveryRequest buildSampleDeliveryRequest() {
        return DeliveryRequest.newBuilder()
            .putPackage("tenant1", DeliveryPackage.newBuilder()
                .addPack(DeliveryPack.newBuilder()
                    .addMatchInfo(MatchInfo.newBuilder().build())
                    .build())
                .build())
            .build();
    }
}
