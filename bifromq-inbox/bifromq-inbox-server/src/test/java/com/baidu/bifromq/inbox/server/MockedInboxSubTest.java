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

package com.baidu.bifromq.inbox.server;

import static com.baidu.bifromq.inbox.records.ScopedInbox.distInboxId;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.basescheduler.exception.BatcherUnavailableException;
import com.baidu.bifromq.dist.client.MatchResult;
import com.baidu.bifromq.inbox.rpc.proto.SubReply;
import com.baidu.bifromq.inbox.rpc.proto.SubRequest;
import com.baidu.bifromq.type.QoS;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import org.testng.annotations.Test;

public class MockedInboxSubTest extends MockedInboxService {
    @Test
    public void subOK() {
        sub(SubReply.Code.OK);
    }

    @Test
    public void subExists() {
        sub(SubReply.Code.EXISTS);
    }

    @Test
    public void subExceedLimit() {
        long reqId = HLC.INST.getPhysical();
        long now = HLC.INST.getPhysical();
        String tenantId = "tenant";
        String inboxId = "inbox";
        long incarnation = 1;
        long version = 1;
        String topicFilter = "/a/b/c";
        when(subScheduler.schedule(any())).thenReturn(
            CompletableFuture.completedFuture(SubReply.newBuilder()
                .setReqId(reqId)
                .setCode(SubReply.Code.OK)
                .build()));

        when(distClient.match(anyLong(), anyString(), anyString(), any(), anyString(), anyString(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.EXCEED_LIMIT));

        StreamObserver<SubReply> streamObserver = mock(StreamObserver.class);
        inboxService.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setSubQoS(QoS.AT_LEAST_ONCE)
            .setIncarnation(incarnation)
            .setVersion(version)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build(), streamObserver);

        verify(distClient).match(eq(reqId), eq(tenantId), eq(topicFilter), eq(QoS.AT_LEAST_ONCE),
            eq(distInboxId(inboxId, incarnation)), anyString(), eq(1));
        verify(streamObserver).onNext(argThat(reply ->
            reply.getReqId() == reqId && reply.getCode() == SubReply.Code.EXCEED_LIMIT));
        verify(streamObserver).onCompleted();
    }

    private void sub(SubReply.Code code) {
        long reqId = HLC.INST.getPhysical();
        long now = HLC.INST.getPhysical();
        String tenantId = "tenant";
        String inboxId = "inbox";
        long incarnation = 1;
        long version = 1;
        String topicFilter = "/a/b/c";
        when(subScheduler.schedule(any())).thenReturn(
            CompletableFuture.completedFuture(SubReply.newBuilder()
                .setReqId(reqId)
                .setCode(code)
                .build()));

        when(distClient.match(anyLong(), anyString(), anyString(), any(), anyString(), anyString(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.OK));

        StreamObserver<SubReply> streamObserver = mock(StreamObserver.class);
        inboxService.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setSubQoS(QoS.AT_LEAST_ONCE)
            .setIncarnation(incarnation)
            .setVersion(version)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build(), streamObserver);

        verify(distClient).match(eq(reqId), eq(tenantId), eq(topicFilter), eq(QoS.AT_LEAST_ONCE),
            eq(distInboxId(inboxId, incarnation)), anyString(), eq(1));
        verify(streamObserver).onNext(argThat(reply ->
            reply.getReqId() == reqId && reply.getCode() == code));
        verify(streamObserver).onCompleted();
    }

    @Test
    public void subThrowsException() {
        long reqId = HLC.INST.getPhysical();
        when(subScheduler.schedule(any())).thenReturn(
            CompletableFuture.failedFuture(new BatcherUnavailableException("Mocked")));

        StreamObserver<SubReply> streamObserver = mock(StreamObserver.class);
        inboxService.sub(SubRequest.newBuilder().setReqId(reqId).build(), streamObserver);

        verify(streamObserver).onNext(argThat(reply ->
            reply.getReqId() == reqId && reply.getCode() == SubReply.Code.ERROR));
        verify(streamObserver).onCompleted();
    }
}
