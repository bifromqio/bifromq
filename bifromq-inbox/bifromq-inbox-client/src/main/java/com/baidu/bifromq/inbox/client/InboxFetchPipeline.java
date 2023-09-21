/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
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
import com.baidu.bifromq.inbox.rpc.proto.CommitReply;
import com.baidu.bifromq.inbox.rpc.proto.CommitRequest;
import com.baidu.bifromq.inbox.rpc.proto.InboxFetchHint;
import com.baidu.bifromq.inbox.rpc.proto.InboxFetched;
import com.baidu.bifromq.inbox.rpc.proto.InboxServiceGrpc;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import com.baidu.bifromq.type.QoS;
import io.reactivex.rxjava3.annotations.NonNull;
import java.lang.ref.Cleaner;
import java.lang.ref.Cleaner.Cleanable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class InboxFetchPipeline {
    private static final Cleaner CLEANER = Cleaner.create();

    private final IRPCClient.IMessageStream<InboxFetched, InboxFetchHint> messageStream;
    private final String tenantId;
    private final IRPCClient rpcClient;
    private final Map<String, Consumer<Fetched>> fetcherMap = new ConcurrentHashMap<>();
    private final Cleanable cleanable;

    InboxFetchPipeline(String tenantId, String delivererKey, IRPCClient rpcClient) {
        this.tenantId = tenantId;
        this.rpcClient = rpcClient;
        this.messageStream = rpcClient.createMessageStream(tenantId, null, delivererKey,
            singletonMap(PIPELINE_ATTR_KEY_ID, UUID.randomUUID().toString()),
            InboxServiceGrpc.getFetchInboxMethod());
        this.cleanable = CLEANER.register(this, new PipelineCloseAction(messageStream));
        new FetchObserver(messageStream, fetcherMap).observe();
    }

    public void fetch(String inboxId, Consumer<Fetched> consumer) {
        fetcherMap.put(inboxId, consumer);
    }

    public void stopFetch(String inboxId) {
        fetcherMap.remove(inboxId);
    }

    public void hint(long incarnation, String inboxId, int bufferCapacity, long lastFetchQoS0Seq, long lastFetchQoS1Seq,
                     long lastFetchQoS2Seq) {
        log.trace("Send hint: inboxId={}, capacity={}, client={}", inboxId, bufferCapacity, tenantId);
        messageStream.ack(InboxFetchHint.newBuilder()
            .setIncarnation(incarnation)
            .setInboxId(inboxId)
            .setCapacity(bufferCapacity)
            .setLastFetchQoS0Seq(lastFetchQoS0Seq)
            .setLastFetchQoS1Seq(lastFetchQoS1Seq)
            .setLastFetchQoS2Seq(lastFetchQoS2Seq)
            .build());
    }

    public CompletableFuture<CommitReply> commit(long reqId, String inboxId, QoS qos, long upToSeq) {
        log.trace("Commit: tenantId={}, inbox={}, qos={}, seq={}", tenantId, inboxId, qos, upToSeq);
        return rpcClient.invoke(tenantId, null,
                CommitRequest.newBuilder()
                    .setReqId(reqId)
                    .setTenantId(tenantId)
                    .setQos(qos)
                    .setUpToSeq(upToSeq)
                    .setInboxId(inboxId)
                    .build(),
                InboxServiceGrpc.getCommitMethod())
            .exceptionally(e -> {
                log.error("Failed to commit inbox: {}", inboxId, e);
                return CommitReply.newBuilder()
                    .setReqId(reqId)
                    .setResult(CommitReply.Result.ERROR)
                    .build();
            });
    }

    public void close() {
        if (cleanable != null) {
            cleanable.clean();
        }
    }

    private static class FetchObserver {
        private final IRPCClient.IMessageStream<InboxFetched, InboxFetchHint> stream;
        private final Map<String, Consumer<Fetched>> fetcherMap;

        public FetchObserver(IMessageStream<InboxFetched, InboxFetchHint> stream,
                             Map<String, Consumer<Fetched>> fetcherMap) {
            this.stream = stream;
            this.fetcherMap = fetcherMap;
        }

        private void observe() {
            if (!stream.isClosed()) {
                stream.msg()
                    .subscribeWith(new io.reactivex.rxjava3.observers.DisposableObserver<InboxFetched>() {
                        @Override
                        public void onNext(@NonNull InboxFetched inboxFetched) {
                            Consumer<Fetched> fetcher = fetcherMap.get(inboxFetched.getInboxId());
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

    private static class PipelineCloseAction implements Runnable {
        private final IRPCClient.IMessageStream<InboxFetched, InboxFetchHint> messageStream;

        private PipelineCloseAction(IRPCClient.IMessageStream<InboxFetched, InboxFetchHint> messageStream) {
            this.messageStream = messageStream;
        }

        @Override
        public void run() {
            messageStream.close();
        }
    }
}
