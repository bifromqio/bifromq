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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.basescheduler.exception.BatcherUnavailableException;
import com.baidu.bifromq.inbox.rpc.proto.DetachReply;
import com.baidu.bifromq.inbox.rpc.proto.ExpireReply;
import com.baidu.bifromq.inbox.rpc.proto.ExpireRequest;
import com.baidu.bifromq.inbox.rpc.proto.GetReply;
import com.baidu.bifromq.inbox.storage.proto.InboxVersion;
import com.baidu.bifromq.type.ClientInfo;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import org.testng.annotations.Test;

public class MockedInboxExpireTest extends MockedInboxService {
    @Test
    public void expireThrowsException() {
        long reqId = HLC.INST.getPhysical();
        when(getScheduler.schedule(any())).thenReturn(
            CompletableFuture.failedFuture(new BatcherUnavailableException("Mocked")));

        StreamObserver<ExpireReply> streamObserver = mock(StreamObserver.class);
        inboxService.expire(ExpireRequest.newBuilder().setReqId(reqId).build(), streamObserver);

        verify(streamObserver).onNext(argThat(reply ->
            reply.getReqId() == reqId && reply.getCode() == ExpireReply.Code.ERROR));
        verify(streamObserver).onCompleted();
    }

    @Test
    public void expireThrowsException1() {
        long reqId = HLC.INST.getPhysical();
        long now = HLC.INST.getPhysical();
        String tenantId = "tenant";
        String inboxId = "inbox";
        InboxVersion inboxVersion = InboxVersion.newBuilder()
            .setIncarnation(123)
            .setVersion(1)
            .setClient(ClientInfo.newBuilder()
                .setTenantId("tenantId")
                .build())
            .build();
        when(getScheduler.schedule(any()))
            .thenReturn(CompletableFuture.completedFuture(GetReply.newBuilder()
                .setReqId(reqId)
                .setCode(GetReply.Code.EXIST)
                .addInbox(inboxVersion)
                .build()));
        when(detachScheduler.schedule(any()))
            .thenReturn(CompletableFuture.failedFuture(new BatcherUnavailableException("Mocked")));

        StreamObserver<ExpireReply> streamObserver = mock(StreamObserver.class);
        inboxService.expire(ExpireRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setNow(now)
            .build(), streamObserver);

        verify(streamObserver).onNext(argThat(reply ->
            reply.getReqId() == reqId && reply.getCode() == ExpireReply.Code.ERROR));
        verify(streamObserver).onCompleted();

        verify(detachScheduler).schedule(argThat(req ->
            req.getClient().equals(inboxVersion.getClient()) &&
                req.getInboxId().equals(inboxId) &&
                req.getIncarnation() == inboxVersion.getIncarnation() &&
                req.getVersion() == inboxVersion.getVersion() &&
                req.getExpirySeconds() == 0 &&
                !req.getDiscardLWT() &&
                req.getNow() == now
        ));
    }

    @Test
    public void expireThrowsException2() {
        long reqId = HLC.INST.getPhysical();
        InboxVersion inboxVersion = InboxVersion.newBuilder()
            .setIncarnation(123)
            .setVersion(1)
            .setClient(ClientInfo.newBuilder()
                .setTenantId("tenantId")
                .build())
            .build();
        InboxVersion inboxVersion1 = InboxVersion.newBuilder()
            .setIncarnation(123)
            .setVersion(1)
            .setClient(ClientInfo.newBuilder()
                .setTenantId("tenantId")
                .build())
            .build();
        when(getScheduler.schedule(any()))
            .thenReturn(CompletableFuture.completedFuture(GetReply.newBuilder()
                .setReqId(reqId)
                .setCode(GetReply.Code.EXIST)
                .addInbox(inboxVersion)
                .addInbox(inboxVersion1)
                .build()));
        when(detachScheduler.schedule(any()))
            .thenReturn(
                CompletableFuture.completedFuture(DetachReply.newBuilder()
                    .setReqId(reqId)
                    .setCode(DetachReply.Code.OK)
                    .build()),
                CompletableFuture.completedFuture(DetachReply.newBuilder()
                    .setReqId(reqId)
                    .setCode(DetachReply.Code.ERROR)
                    .build()));
        StreamObserver<ExpireReply> streamObserver = mock(StreamObserver.class);
        inboxService.expire(ExpireRequest.newBuilder().setReqId(reqId).build(), streamObserver);

        verify(detachScheduler, times(2)).schedule(any());
        verify(streamObserver).onNext(argThat(reply ->
            reply.getReqId() == reqId && reply.getCode() == ExpireReply.Code.ERROR));
        verify(streamObserver).onCompleted();
    }

    @Test
    public void expire() {
        long reqId = HLC.INST.getPhysical();
        InboxVersion inboxVersion = InboxVersion.newBuilder()
            .setIncarnation(123)
            .setVersion(1)
            .setClient(ClientInfo.newBuilder()
                .setTenantId("tenantId")
                .build())
            .build();
        InboxVersion inboxVersion1 = InboxVersion.newBuilder()
            .setIncarnation(123)
            .setVersion(1)
            .setClient(ClientInfo.newBuilder()
                .setTenantId("tenantId")
                .build())
            .build();
        when(getScheduler.schedule(any()))
            .thenReturn(CompletableFuture.completedFuture(GetReply.newBuilder()
                .setReqId(reqId)
                .setCode(GetReply.Code.EXIST)
                .addInbox(inboxVersion)
                .addInbox(inboxVersion1)
                .build()));
        when(detachScheduler.schedule(any()))
            .thenReturn(CompletableFuture.completedFuture(DetachReply.newBuilder()
                .setReqId(reqId)
                .setCode(DetachReply.Code.OK)
                .build()));
        StreamObserver<ExpireReply> streamObserver = mock(StreamObserver.class);
        inboxService.expire(ExpireRequest.newBuilder().setReqId(reqId).build(), streamObserver);

        verify(streamObserver).onNext(argThat(reply ->
            reply.getReqId() == reqId && reply.getCode() == ExpireReply.Code.OK));
        verify(streamObserver).onCompleted();
    }
}
