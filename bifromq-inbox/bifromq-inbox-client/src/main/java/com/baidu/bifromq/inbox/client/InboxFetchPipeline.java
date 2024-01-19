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

import static com.baidu.bifromq.inbox.util.PipelineUtil.PIPELINE_ATTR_KEY_ID;
import static java.util.Collections.singletonMap;

import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.baserpc.IRPCClient.IMessageStream;
import com.baidu.bifromq.inbox.rpc.proto.InboxFetchHint;
import com.baidu.bifromq.inbox.rpc.proto.InboxFetched;
import com.baidu.bifromq.inbox.rpc.proto.InboxServiceGrpc;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.observers.DisposableObserver;
import java.lang.ref.Cleaner;
import java.lang.ref.Cleaner.Cleanable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class InboxFetchPipeline {
    private static final Cleaner CLEANER = Cleaner.create();

    private final IMessageStream<InboxFetched, InboxFetchHint> messageStream;
    private final String tenantId;
    private final Map<InboxFetchSessionId, Consumer<Fetched>> fetcherMap = new ConcurrentHashMap<>();
    private final Cleanable cleanable;

    InboxFetchPipeline(String tenantId, String delivererKey, IRPCClient rpcClient) {
        this.tenantId = tenantId;
        this.messageStream = rpcClient.createMessageStream(tenantId, null, delivererKey,
            singletonMap(PIPELINE_ATTR_KEY_ID, UUID.randomUUID().toString()),
            InboxServiceGrpc.getFetchMethod());
        this.cleanable = CLEANER.register(this, new PipelineCloseAction(messageStream));
        new FetchObserver(messageStream, fetcherMap).observe();
    }

    public void fetch(long sessionId, String inboxId, long incarnation, Consumer<Fetched> consumer) {
        fetcherMap.put(new InboxFetchSessionId(sessionId, inboxId, incarnation), consumer);
    }

    public void stopFetch(long sessionId, String inboxId, long incarnation) {
        fetcherMap.remove(new InboxFetchSessionId(sessionId, inboxId, incarnation));
    }

    public void hint(long sessionId,
                     String inboxId,
                     long incarnation,
                     int bufferCapacity,
                     long lastFetchQoS0Seq,
                     long lastFetchQoS1Seq,
                     long lastFetchQoS2Seq) {
        log.trace("Send hint: inboxId={}, incarnation={}, capacity={}, client={}", inboxId, incarnation, bufferCapacity,
            tenantId);
        messageStream.ack(InboxFetchHint.newBuilder()
            .setSessionId(sessionId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setCapacity(bufferCapacity)
            .setLastFetchQoS0Seq(lastFetchQoS0Seq)
            .setLastFetchQoS1Seq(lastFetchQoS1Seq)
            .setLastFetchQoS2Seq(lastFetchQoS2Seq)
            .build());
    }

    public void close() {
        if (cleanable != null) {
            cleanable.clean();
        }
    }

    private record FetchObserver(IMessageStream<InboxFetched, InboxFetchHint> stream,
                                 Map<InboxFetchSessionId, Consumer<Fetched>> fetcherMap) {
        private void observe() {
            if (!stream.isClosed()) {
                stream.msg()
                    .subscribeWith(new DisposableObserver<InboxFetched>() {
                        @Override
                        public void onNext(@NonNull InboxFetched inboxFetched) {
                            Consumer<Fetched> fetcher = fetcherMap.get(
                                new InboxFetchSessionId(inboxFetched.getSessionId(),
                                    inboxFetched.getInboxId(),
                                    inboxFetched.getIncarnation()));
                            if (fetcher != null) {
                                Fetched fetched = inboxFetched.getFetched();
                                fetcher.accept(fetched);
                            }
                        }

                        @Override
                        public void onError(@NonNull Throwable e) {
                            dispose();
                            fetcherMap.values().forEach(consumer -> consumer.accept(Fetched.newBuilder()
                                .setResult(Fetched.Result.ERROR)
                                .build()));
                            observe();
                        }

                        @Override
                        public void onComplete() {
                            dispose();
                            fetcherMap.values().forEach(consumer -> consumer.accept(Fetched.newBuilder()
                                .setResult(Fetched.Result.ERROR)
                                .build()));
                            observe();
                        }
                    });
            }
        }
    }

    private record PipelineCloseAction(IMessageStream<InboxFetched, InboxFetchHint> messageStream)
        implements Runnable {
        @Override
        public void run() {
            messageStream.close();
        }
    }

    private record InboxFetchSessionId(long sessionId, String inboxId, long incarnation) {
    }
}
