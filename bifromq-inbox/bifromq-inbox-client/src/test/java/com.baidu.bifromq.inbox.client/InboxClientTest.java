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

package com.baidu.bifromq.inbox.client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.inbox.rpc.proto.AttachReply;
import com.baidu.bifromq.inbox.rpc.proto.AttachRequest;
import com.baidu.bifromq.inbox.rpc.proto.CommitReply;
import com.baidu.bifromq.inbox.rpc.proto.CommitRequest;
import com.baidu.bifromq.inbox.rpc.proto.CreateReply;
import com.baidu.bifromq.inbox.rpc.proto.CreateRequest;
import com.baidu.bifromq.inbox.rpc.proto.DetachReply;
import com.baidu.bifromq.inbox.rpc.proto.DetachRequest;
import com.baidu.bifromq.inbox.rpc.proto.ExpireAllReply;
import com.baidu.bifromq.inbox.rpc.proto.ExpireAllRequest;
import com.baidu.bifromq.inbox.rpc.proto.ExpireReply;
import com.baidu.bifromq.inbox.rpc.proto.ExpireRequest;
import com.baidu.bifromq.inbox.rpc.proto.GetReply;
import com.baidu.bifromq.inbox.rpc.proto.GetRequest;
import com.baidu.bifromq.inbox.rpc.proto.SubReply;
import com.baidu.bifromq.inbox.rpc.proto.SubRequest;
import com.baidu.bifromq.inbox.rpc.proto.TouchReply;
import com.baidu.bifromq.inbox.rpc.proto.TouchRequest;
import com.baidu.bifromq.inbox.rpc.proto.UnsubReply;
import com.baidu.bifromq.inbox.rpc.proto.UnsubRequest;
import com.baidu.bifromq.type.ClientInfo;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class InboxClientTest {
    private AutoCloseable closeable;
    @Mock
    private IRPCClient rpcClient;

    private InboxClient inboxClient;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        inboxClient = new InboxClient(rpcClient);
    }

    @AfterMethod
    @SneakyThrows
    public void teardown() {
        closeable.close();
    }

    @Test
    public void commitRPCException() {
        CommitRequest commitRequest = CommitRequest.newBuilder().setTenantId("TenantId").build();

        when(rpcClient.invoke(anyString(), isNull(), any(), any()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Mocked")));
        CommitReply reply = inboxClient.commit(commitRequest).join();
        verify(rpcClient).invoke(eq(commitRequest.getTenantId()), isNull(), eq(commitRequest), any());
        assertEquals(reply.getCode(), CommitReply.Code.ERROR);
    }

    @Test
    public void getRPCException() {
        GetRequest getRequest = GetRequest.newBuilder().setTenantId("TenantId").build();

        when(rpcClient.invoke(anyString(), isNull(), any(), any()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Mocked")));
        GetReply reply = inboxClient.get(getRequest).join();
        verify(rpcClient).invoke(eq(getRequest.getTenantId()), isNull(), eq(getRequest), any());
        assertEquals(reply.getCode(), GetReply.Code.ERROR);
    }

    @Test
    public void createRPCException() {
        CreateRequest createRequest = CreateRequest.newBuilder()
            .setClient(ClientInfo.newBuilder()
                .setTenantId("TenantId")
                .build()).build();

        when(rpcClient.invoke(anyString(), isNull(), any(), any()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Mocked")));
        CreateReply reply = inboxClient.create(createRequest).join();
        verify(rpcClient).invoke(eq(createRequest.getClient().getTenantId()), isNull(), eq(createRequest), any());
        assertEquals(reply.getCode(), CreateReply.Code.ERROR);
    }

    @Test
    public void attachRPCException() {
        AttachRequest attachRequest = AttachRequest.newBuilder()
            .setClient(ClientInfo.newBuilder()
                .setTenantId("TenantId")
                .build()).build();

        when(rpcClient.invoke(anyString(), isNull(), any(), any()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Mocked")));
        AttachReply reply = inboxClient.attach(attachRequest).join();
        verify(rpcClient).invoke(eq(attachRequest.getClient().getTenantId()), isNull(), eq(attachRequest), any());
        assertEquals(reply.getCode(), AttachReply.Code.ERROR);
    }

    @Test
    public void detachRPCException() {
        DetachRequest detachRequest = DetachRequest.newBuilder()
            .setClient(ClientInfo.newBuilder()
                .setTenantId("TenantId")
                .build()).build();

        when(rpcClient.invoke(anyString(), isNull(), any(), any()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Mocked")));
        DetachReply reply = inboxClient.detach(detachRequest).join();
        verify(rpcClient).invoke(eq(detachRequest.getClient().getTenantId()), isNull(), eq(detachRequest), any());
        assertEquals(reply.getCode(), DetachReply.Code.ERROR);
    }

    @Test
    public void subRPCException() {
        SubRequest subRequest = SubRequest.newBuilder().setTenantId("TenantId").build();

        when(rpcClient.invoke(anyString(), isNull(), any(), any()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Mocked")));
        SubReply reply = inboxClient.sub(subRequest).join();
        verify(rpcClient).invoke(eq(subRequest.getTenantId()), isNull(), eq(subRequest), any());
        assertEquals(reply.getCode(), SubReply.Code.ERROR);
    }

    @Test
    public void unsubRPCException() {
        UnsubRequest unsubRequest = UnsubRequest.newBuilder().setTenantId("TenantId").build();

        when(rpcClient.invoke(anyString(), isNull(), any(), any()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Mocked")));
        UnsubReply reply = inboxClient.unsub(unsubRequest).join();
        verify(rpcClient).invoke(eq(unsubRequest.getTenantId()), isNull(), eq(unsubRequest), any());
        assertEquals(reply.getCode(), UnsubReply.Code.ERROR);
    }

    @Test
    public void touchRPCException() {
        TouchRequest touchRequest = TouchRequest.newBuilder().setTenantId("TenantId").build();

        when(rpcClient.invoke(anyString(), isNull(), any(), any()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Mocked")));
        TouchReply reply = inboxClient.touch(touchRequest).join();
        verify(rpcClient).invoke(eq(touchRequest.getTenantId()), isNull(), eq(touchRequest), any());
        assertEquals(reply.getCode(), TouchReply.Code.ERROR);
    }

    @Test
    public void expireRPCException() {
        ExpireRequest expireRequest = ExpireRequest.newBuilder().setTenantId("TenantId").build();

        when(rpcClient.invoke(anyString(), isNull(), any(), any()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Mocked")));
        ExpireReply reply = inboxClient.expire(expireRequest).join();
        verify(rpcClient).invoke(eq(expireRequest.getTenantId()), isNull(), eq(expireRequest), any());
        assertEquals(reply.getCode(), ExpireReply.Code.ERROR);
    }

    @Test
    public void expireAllRPCException() {
        ExpireAllRequest expireAllRequest = ExpireAllRequest.newBuilder().setTenantId("TenantId").build();

        when(rpcClient.invoke(anyString(), isNull(), any(), any()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Mocked")));
        ExpireAllReply reply = inboxClient.expireAll(expireAllRequest).join();
        verify(rpcClient).invoke(eq(expireAllRequest.getTenantId()), isNull(), eq(expireAllRequest), any());
        assertEquals(reply.getCode(), ExpireAllReply.Code.ERROR);
    }
}
