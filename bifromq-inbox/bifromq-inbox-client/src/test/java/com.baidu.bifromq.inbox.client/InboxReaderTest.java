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

package com.baidu.bifromq.inbox.client;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.inbox.rpc.proto.CommitReply;
import com.baidu.bifromq.inbox.rpc.proto.CommitRequest;
import com.baidu.bifromq.inbox.rpc.proto.InboxFetchHint;
import com.baidu.bifromq.inbox.rpc.proto.InboxFetched;
import com.baidu.bifromq.inbox.rpc.proto.InboxServiceGrpc;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import com.baidu.bifromq.inbox.storage.proto.Fetched.Result;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class InboxReaderTest {
    private AutoCloseable closeable;
    @Mock
    private Consumer<Fetched> onFetched;
    @Mock
    private IRPCClient rpcClient;
    @Mock
    private IRPCClient.IMessageStream<InboxFetched, InboxFetchHint> messageStream;
    private InboxFetchPipeline fetchPipeline;
    private final String tenantId = "tenantId";
    private final String delivererKey = "delivererKey";
    private final String inboxId = "inboxId";
    private final AtomicReference<Subject<InboxFetched>> fetchSubject = new AtomicReference<>();

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        when(rpcClient.createMessageStream(eq(tenantId), any(), eq(delivererKey), anyMap(),
            eq(InboxServiceGrpc.getFetchInboxMethod()))).thenReturn(messageStream);
        when(messageStream.msg()).thenAnswer((Answer<Observable<InboxFetched>>) invocationOnMock -> {
            fetchSubject.set(PublishSubject.create());
            return fetchSubject.get();
        });
        fetchPipeline = new InboxFetchPipeline(tenantId, delivererKey, rpcClient);
    }

    @AfterMethod
    @SneakyThrows
    public void teardown() {
        closeable.close();
    }

    @Test
    public void fetch() {
        when(rpcClient.invoke(eq(tenantId), any(), any(CommitRequest.class), eq(InboxServiceGrpc.getCommitMethod())))
            .thenReturn(CompletableFuture.completedFuture(CommitReply.newBuilder().build()));
        Fetched fetched = Fetched.newBuilder()
            .setResult(Result.OK)
            .addQos0Seq(1L)
            .build();
        InboxFetched inboxFetched = InboxFetched.newBuilder().setInboxId(inboxId).setFetched(fetched).build();
        InboxReader inboxReader = new InboxReader(inboxId, fetchPipeline);
        inboxReader.fetch(onFetched);
        fetchSubject.get().onNext(inboxFetched);
        verify(onFetched, times(1)).accept(fetched);
        verify(rpcClient, times(1)).invoke(eq(tenantId), any(), any(CommitRequest.class),
            eq(InboxServiceGrpc.getCommitMethod()));
    }

    @Test
    public void reFetchAfterError() {
        Fetched fetchedError = Fetched.newBuilder().setResult(Result.ERROR).build();
        InboxReader inboxReader = new InboxReader(inboxId, fetchPipeline);
        inboxReader.fetch(onFetched);
        fetchSubject.get().onError(new RuntimeException("Test Exception"));
        verify(onFetched, times(1)).accept(fetchedError);
        verify(messageStream, times(2)).msg();
    }

    @Test
    public void reFetchAfterComplete() {
        Fetched fetchedError = Fetched.newBuilder().setResult(Result.ERROR).build();
        InboxReader inboxReader = new InboxReader(inboxId, fetchPipeline);
        inboxReader.fetch(onFetched);
        fetchSubject.get().onComplete();
        verify(onFetched, times(1)).accept(fetchedError);
        verify(messageStream, times(2)).msg();
    }

    @Test
    public void registerGC() throws InterruptedException {
        InboxReader inboxReader = new InboxReader(inboxId, new InboxFetchPipeline(tenantId, delivererKey, rpcClient));
        inboxReader.close();
        inboxReader = null;
        System.gc();
        Thread.sleep(10);
        await().until(() -> {
            verify(messageStream, times(1)).close();
            return true;
        });
    }

    @Test
    public void fetchPipelineClose() throws InterruptedException {
        InboxFetchPipeline inboxFetchPipeline = new InboxFetchPipeline(tenantId, delivererKey, rpcClient);
        inboxFetchPipeline.close();
        Thread.sleep(10);
        await().until(() -> {
            verify(messageStream, times(1)).close();
            return true;
        });
    }
}
