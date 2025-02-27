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

import static com.baidu.bifromq.inbox.server.Fixtures.matchInfo;
import static com.baidu.bifromq.inbox.server.Fixtures.sendRequest;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.inbox.rpc.proto.SendReply;
import com.baidu.bifromq.inbox.rpc.proto.SendRequest;
import com.baidu.bifromq.inbox.server.scheduler.IInboxInsertScheduler;
import com.baidu.bifromq.inbox.storage.proto.BatchInsertReply;
import com.baidu.bifromq.inbox.storage.proto.InboxSubMessagePack;
import com.baidu.bifromq.plugin.subbroker.DeliveryReply;
import com.baidu.bifromq.plugin.subbroker.DeliveryResult;
import com.baidu.bifromq.plugin.subbroker.DeliveryResults;
import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class InboxWriterTest {

    @Mock
    private IInboxInsertScheduler insertScheduler;

    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @SneakyThrows
    @AfterMethod
    public void teardown() {
        closeable.close();
    }

    @Test
    public void insertScheduleErr() {
        when(insertScheduler.schedule(any(InboxSubMessagePack.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("err")));
        SendRequest request = sendRequest();
        SendReply sendReply = new InboxWriter(insertScheduler).handle(request).join();
        assertEquals(sendReply, sendReply(request.getReqId(), DeliveryResult.Code.ERROR));
    }

    @Test
    public void insertScheduleNoInbox() {
        when(insertScheduler.schedule(any(InboxSubMessagePack.class))).thenReturn(
            CompletableFuture.completedFuture(BatchInsertReply.Result.newBuilder()
                .setCode(BatchInsertReply.Code.NO_INBOX)
                .build()));
        SendRequest request = sendRequest();
        SendReply sendReply = new InboxWriter(insertScheduler).handle(request).join();
        assertEquals(sendReply, sendReply(request.getReqId(), DeliveryResult.Code.NO_RECEIVER));
    }

    @Test
    public void insertScheduleRejected() {
        when(insertScheduler.schedule(any(InboxSubMessagePack.class)))
            .thenReturn(CompletableFuture.completedFuture(
                BatchInsertReply.Result.newBuilder()
                    .addInsertionResult(BatchInsertReply.InsertionResult.newBuilder()
                        .setRejected(true)
                        .setIncarnation(1L)
                        .setTopicFilter("/foo/+")
                        .build())
                    .setCode(BatchInsertReply.Code.OK)
                    .build()));
        SendRequest request = sendRequest();
        SendReply sendReply = new InboxWriter(insertScheduler).handle(request).join();
        assertEquals(sendReply, sendReply(request.getReqId(), DeliveryResult.Code.NO_SUB));
    }

    @Test
    public void insertScheduleOk() {
        when(insertScheduler.schedule(any(InboxSubMessagePack.class))).thenReturn(
            CompletableFuture.completedFuture(BatchInsertReply.Result.newBuilder()
                .addInsertionResult(BatchInsertReply.InsertionResult.newBuilder()
                    .setRejected(false)
                    .setTopicFilter("/foo/+")
                    .setIncarnation(1L)
                    .build())
                .setCode(BatchInsertReply.Code.OK).build()));
        SendRequest request = sendRequest();
        SendReply sendReply = new InboxWriter(insertScheduler).handle(request).join();
        assertEquals(sendReply, sendReply(request.getReqId(), DeliveryResult.Code.OK));
    }

    @Test
    public void insertScheduleUnexpectedCode() {
        BatchInsertReply.Result result = BatchInsertReply.Result.newBuilder()
            .addInsertionResult(BatchInsertReply.InsertionResult.newBuilder()
                .setRejected(false)
                .build())
            .build();
        setUnknownCode(result);
        when(insertScheduler.schedule(any(InboxSubMessagePack.class))).thenReturn(
            CompletableFuture.completedFuture(result));
        SendRequest request = sendRequest();
        SendReply sendReply = new InboxWriter(insertScheduler).handle(request).join();
        assertEquals(sendReply, sendReply(request.getReqId(), DeliveryResult.Code.ERROR));
    }

    private void setUnknownCode(BatchInsertReply.Result result) {
        Field codeField;
        try {
            codeField = result.getClass().getDeclaredField("code_");
            codeField.setAccessible(true);
            codeField.set(result, 3);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private SendReply sendReply(long reqId, DeliveryResult.Code code) {
        return SendReply.newBuilder()
            .setReqId(reqId)
            .setReply(DeliveryReply.newBuilder()
                .putResult("_",
                    DeliveryResults.newBuilder()
                        .addResult(DeliveryResult.newBuilder()
                            .setCode(code)
                            .setMatchInfo(matchInfo())
                            .build())
                        .build())
                .build())
            .build();
    }

}