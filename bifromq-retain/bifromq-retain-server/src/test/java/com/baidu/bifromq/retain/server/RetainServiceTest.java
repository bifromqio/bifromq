/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.retain.server;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.baserpc.RPCContext;
import com.baidu.bifromq.baserpc.metrics.RPCMeters;
import com.baidu.bifromq.deliverer.DeliveryRequest;
import com.baidu.bifromq.deliverer.IMessageDeliverer;
import com.baidu.bifromq.plugin.subbroker.DeliveryResult;
import com.baidu.bifromq.retain.rpc.proto.MatchReply;
import com.baidu.bifromq.retain.rpc.proto.MatchRequest;
import com.baidu.bifromq.retain.rpc.proto.RetainReply;
import com.baidu.bifromq.retain.rpc.proto.RetainRequest;
import com.baidu.bifromq.retain.server.scheduler.IMatchCallScheduler;
import com.baidu.bifromq.retain.server.scheduler.IRetainCallScheduler;
import com.baidu.bifromq.retain.server.scheduler.MatchCallResult;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.TopicMessage;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class RetainServiceTest {
    private String serviceName = "retainService";
    private String methodName = "testMethod";
    private String tenantId = "testTenantId";
    @Mock
    private IMessageDeliverer messageDeliverer;
    @Mock
    private IMatchCallScheduler matchCallScheduler;
    @Mock
    private IRetainCallScheduler retainCallScheduler;
    @Mock
    StreamObserver<RetainReply> retainResponseObserver;
    @Mock
    StreamObserver<MatchReply> matchResponseObserver;
    private AutoCloseable closeable;
    private RetainService service;

    @BeforeMethod
    public void setup(Method method) {
        log.info("Test case[{}.{}] start", method.getDeclaringClass().getName(), method.getName());
        Context.current()
            .withValue(RPCContext.METER_KEY_CTX_KEY, RPCMeters.MeterKey.builder()
                .service(serviceName)
                .method(methodName)
                .tenantId(tenantId)
                .build())
            .withValue(RPCContext.TENANT_ID_CTX_KEY, tenantId)
            .attach();
        closeable = MockitoAnnotations.openMocks(this);
        service = new RetainService(messageDeliverer, matchCallScheduler, retainCallScheduler);
    }

    @AfterMethod
    public void teardown(Method method) throws Exception {
        log.info("Test case[{}.{}] finished", method.getDeclaringClass().getName(), method.getName());
        closeable.close();
    }

    @Test
    public void testPutRetainWithException() {
        when(retainCallScheduler.schedule(any())).thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("Mocked")));
        long reqId = 1;
        service.retain(RetainRequest.newBuilder().setReqId(reqId).build(), retainResponseObserver);
        verify(retainResponseObserver)
            .onNext(argThat(r -> r.getReqId() == reqId && r.getResult() == RetainReply.Result.ERROR));
    }

    @Test
    public void testMatchNothing() {
        when(matchCallScheduler.schedule(any())).thenReturn(
            CompletableFuture.completedFuture(new MatchCallResult(MatchReply.Result.OK, Collections.emptyList())));
        long reqId = 1;
        service.match(MatchRequest.newBuilder().setReqId(reqId).build(), matchResponseObserver);
        verify(matchResponseObserver)
            .onNext(argThat(r -> r.getReqId() == reqId && r.getResult() == MatchReply.Result.OK));
        verify(messageDeliverer, never()).schedule(any());
    }

    @Test
    public void testDeliverRetainMessages() {
        TopicMessage retainMsg1 = TopicMessage.newBuilder()
            .setTopic("topic1")
            .setMessage(Message.newBuilder().build())
            .setPublisher(ClientInfo.newBuilder().build())
            .build();
        TopicMessage retainMsg2 = TopicMessage.newBuilder()
            .setTopic("topic2")
            .setMessage(Message.newBuilder().build())
            .setPublisher(ClientInfo.newBuilder().build())
            .build();
        when(matchCallScheduler.schedule(any())).thenReturn(CompletableFuture.completedFuture(
            new MatchCallResult(MatchReply.Result.OK, List.of(retainMsg1, retainMsg2))));
        when(messageDeliverer.schedule(any())).thenReturn(CompletableFuture.completedFuture(DeliveryResult.OK));
        MatchRequest matchRequest = MatchRequest.newBuilder()
            .setReqId(1)
            .setTenantId("tenant")
            .setInboxId("inbox")
            .setDelivererKey("delivererKey")
            .setBrokerId(1)
            .setTopicFilter("#")
            .setQos(QoS.EXACTLY_ONCE)
            .build();
        service.match(matchRequest, matchResponseObserver);
        verify(matchResponseObserver)
            .onNext(argThat(r -> r.getReqId() == matchRequest.getReqId() && r.getResult() == MatchReply.Result.OK));
        ArgumentCaptor<DeliveryRequest> reqCaptor = ArgumentCaptor.forClass(DeliveryRequest.class);
        verify(messageDeliverer, times(2)).schedule(reqCaptor.capture());
        List<DeliveryRequest> requestList = reqCaptor.getAllValues();
        DeliveryRequest req1 = requestList.get(0);
        assertEquals(req1.subInfo.getTenantId(), matchRequest.getTenantId());
        assertEquals(req1.subInfo.getInboxId(), matchRequest.getInboxId());
        assertEquals(req1.subInfo.getTopicFilter(), matchRequest.getTopicFilter());
        assertEquals(req1.subInfo.getSubQoS(), matchRequest.getQos());

        assertEquals(req1.msgPackWrapper.messagePack.getTopic(), retainMsg1.getTopic());
        assertEquals(req1.msgPackWrapper.messagePack.getMessage(0).getPublisher(), retainMsg1.getPublisher());

        assertEquals(req1.writerKey.delivererKey(), matchRequest.getDelivererKey());
        assertEquals(req1.writerKey.subBrokerId(), matchRequest.getBrokerId());
    }

    @Test
    public void testDeliverToNoInbox() {
        TopicMessage retainMsg1 = TopicMessage.newBuilder()
            .setTopic("topic1")
            .setMessage(Message.newBuilder().build())
            .setPublisher(ClientInfo.newBuilder().build())
            .build();
        TopicMessage retainMsg2 = TopicMessage.newBuilder()
            .setTopic("topic2")
            .setMessage(Message.newBuilder().build())
            .setPublisher(ClientInfo.newBuilder().build())
            .build();
        when(matchCallScheduler.schedule(any())).thenReturn(CompletableFuture.completedFuture(
            new MatchCallResult(MatchReply.Result.OK, List.of(retainMsg1, retainMsg2))));
        when(messageDeliverer.schedule(any())).thenReturn(CompletableFuture.completedFuture(DeliveryResult.NO_INBOX));
        MatchRequest matchRequest = MatchRequest.newBuilder()
            .setReqId(1)
            .setTenantId("tenant")
            .setInboxId("inbox")
            .setDelivererKey("delivererKey")
            .setBrokerId(1)
            .setTopicFilter("#")
            .setQos(QoS.EXACTLY_ONCE)
            .build();
        service.match(matchRequest, matchResponseObserver);
        verify(matchResponseObserver)
            .onNext(argThat(r -> r.getReqId() == matchRequest.getReqId() && r.getResult() == MatchReply.Result.ERROR));
        verify(messageDeliverer, times(2)).schedule(any());
    }

    @Test
    public void testDeliverFailed() {
        TopicMessage retainMsg1 = TopicMessage.newBuilder()
            .setTopic("topic1")
            .setMessage(Message.newBuilder().build())
            .setPublisher(ClientInfo.newBuilder().build())
            .build();
        TopicMessage retainMsg2 = TopicMessage.newBuilder()
            .setTopic("topic2")
            .setMessage(Message.newBuilder().build())
            .setPublisher(ClientInfo.newBuilder().build())
            .build();
        when(matchCallScheduler.schedule(any())).thenReturn(CompletableFuture.completedFuture(
            new MatchCallResult(MatchReply.Result.OK, List.of(retainMsg1, retainMsg2))));
        when(messageDeliverer.schedule(any())).thenReturn(CompletableFuture.completedFuture(DeliveryResult.FAILED));
        MatchRequest matchRequest = MatchRequest.newBuilder()
            .setReqId(1)
            .setTenantId("tenant")
            .setInboxId("inbox")
            .setDelivererKey("delivererKey")
            .setBrokerId(1)
            .setTopicFilter("#")
            .setQos(QoS.EXACTLY_ONCE)
            .build();
        service.match(matchRequest, matchResponseObserver);
        verify(matchResponseObserver)
            .onNext(argThat(r -> r.getReqId() == matchRequest.getReqId() && r.getResult() == MatchReply.Result.ERROR));
        verify(messageDeliverer, times(2)).schedule(any());
    }

    @Test
    public void testMatchRetainWithErrorCode() {
        when(matchCallScheduler.schedule(any())).thenReturn(
            CompletableFuture.completedFuture(new MatchCallResult(MatchReply.Result.ERROR, Collections.emptyList())));
        long reqId = 1;
        service.match(MatchRequest.newBuilder().setReqId(reqId).build(), matchResponseObserver);
        verify(matchResponseObserver)
            .onNext(argThat(r -> r.getReqId() == reqId && r.getResult() == MatchReply.Result.ERROR));
        verify(messageDeliverer, never()).schedule(any());
    }

    @Test
    public void testMatchRetainWithException() {
        when(matchCallScheduler.schedule(any())).thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("Mocked")));
        long reqId = 1;
        service.match(MatchRequest.newBuilder().setReqId(reqId).build(), matchResponseObserver);
        verify(matchResponseObserver)
            .onNext(argThat(r -> r.getReqId() == reqId && r.getResult() == MatchReply.Result.ERROR));
        verify(messageDeliverer, never()).schedule(any());
    }
}
