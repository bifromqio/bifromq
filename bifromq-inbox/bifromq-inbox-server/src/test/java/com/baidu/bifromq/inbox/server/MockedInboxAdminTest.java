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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.basescheduler.exception.BatcherUnavailableException;
import com.baidu.bifromq.inbox.rpc.proto.AttachReply;
import com.baidu.bifromq.inbox.rpc.proto.AttachRequest;
import com.baidu.bifromq.inbox.rpc.proto.CreateReply;
import com.baidu.bifromq.inbox.rpc.proto.CreateRequest;
import com.baidu.bifromq.inbox.rpc.proto.DetachReply;
import com.baidu.bifromq.inbox.rpc.proto.DetachRequest;
import com.baidu.bifromq.inbox.rpc.proto.GetReply;
import com.baidu.bifromq.inbox.rpc.proto.GetRequest;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import org.testng.annotations.Test;

public class MockedInboxAdminTest extends MockedInboxService {

    @Test
    public void getInboxThrowsException() {
        long reqId = HLC.INST.getPhysical();
        when(getScheduler.schedule(any())).thenReturn(
            CompletableFuture.failedFuture(new BatcherUnavailableException("Mocked")));
        StreamObserver<GetReply> streamObserver = mock(StreamObserver.class);

        inboxService.get(GetRequest.newBuilder().setReqId(reqId).build(), streamObserver);

        verify(streamObserver).onNext(argThat(reply ->
            reply.getReqId() == reqId && reply.getCode() == GetReply.Code.TRY_LATER));
        verify(streamObserver).onCompleted();
    }

    @Test
    public void createInboxThrowsException() {
        long reqId = HLC.INST.getPhysical();
        when(createScheduler.schedule(any())).thenReturn(
            CompletableFuture.failedFuture(new BatcherUnavailableException("Mocked")));

        StreamObserver<CreateReply> streamObserver = mock(StreamObserver.class);
        inboxService.create(CreateRequest.newBuilder().setReqId(reqId).build(), streamObserver);

        verify(streamObserver).onNext(argThat(reply ->
            reply.getReqId() == reqId && reply.getCode() == CreateReply.Code.TRY_LATER));
        verify(streamObserver).onCompleted();
    }

    @Test
    public void attachInboxThrowsException() {
        long reqId = HLC.INST.getPhysical();
        when(attachScheduler.schedule(any())).thenReturn(
            CompletableFuture.failedFuture(new BatcherUnavailableException("Mocked")));

        StreamObserver<AttachReply> streamObserver = mock(StreamObserver.class);
        inboxService.attach(AttachRequest.newBuilder().setReqId(reqId).build(), streamObserver);

        verify(streamObserver).onNext(argThat(reply ->
            reply.getReqId() == reqId && reply.getCode() == AttachReply.Code.TRY_LATER));
        verify(streamObserver).onCompleted();
    }

    @Test
    public void detachInboxThrowsException() {
        long reqId = HLC.INST.getPhysical();
        when(detachScheduler.schedule(any())).thenReturn(
            CompletableFuture.failedFuture(new BatcherUnavailableException("Mocked")));

        StreamObserver<DetachReply> streamObserver = mock(StreamObserver.class);
        inboxService.detach(DetachRequest.newBuilder().setReqId(reqId).build(), streamObserver);

        verify(streamObserver).onNext(argThat(reply ->
            reply.getReqId() == reqId && reply.getCode() == DetachReply.Code.TRY_LATER));
        verify(streamObserver).onCompleted();
    }
}
