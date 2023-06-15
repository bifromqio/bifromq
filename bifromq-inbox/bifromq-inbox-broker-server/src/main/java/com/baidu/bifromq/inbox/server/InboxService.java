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

package com.baidu.bifromq.inbox.server;

import static com.baidu.bifromq.baserpc.UnaryResponse.response;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.INBOX_FETCH_PIPELINE_CREATION_RATE_LIMIT;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.inbox.rpc.proto.CommitReply;
import com.baidu.bifromq.inbox.rpc.proto.CommitRequest;
import com.baidu.bifromq.inbox.rpc.proto.CreateInboxReply;
import com.baidu.bifromq.inbox.rpc.proto.CreateInboxRequest;
import com.baidu.bifromq.inbox.rpc.proto.DeleteInboxReply;
import com.baidu.bifromq.inbox.rpc.proto.DeleteInboxRequest;
import com.baidu.bifromq.inbox.rpc.proto.FetchHint;
import com.baidu.bifromq.inbox.rpc.proto.HasInboxReply;
import com.baidu.bifromq.inbox.rpc.proto.HasInboxRequest;
import com.baidu.bifromq.inbox.rpc.proto.InboxServiceGrpc;
import com.baidu.bifromq.inbox.rpc.proto.SendReply;
import com.baidu.bifromq.inbox.rpc.proto.SendRequest;
import com.baidu.bifromq.inbox.rpc.proto.SendResult;
import com.baidu.bifromq.inbox.server.scheduler.InboxCheckScheduler;
import com.baidu.bifromq.inbox.server.scheduler.InboxCommitScheduler;
import com.baidu.bifromq.inbox.server.scheduler.InboxCreateScheduler;
import com.baidu.bifromq.inbox.server.scheduler.InboxFetchScheduler;
import com.baidu.bifromq.inbox.server.scheduler.InboxInsertScheduler;
import com.baidu.bifromq.inbox.server.scheduler.InboxTouchScheduler;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import com.baidu.bifromq.inbox.storage.proto.MessagePack;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class InboxService extends InboxServiceGrpc.InboxServiceImplBase {
    private enum State {
        INIT,
        STARTING,
        STARTED,
        STOPPING,
        STOPPED
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.INIT);
    private final IBaseKVStoreClient kvStoreClient;
    private final ScheduledExecutorService bgTaskExecutor;
    private final boolean bgTaskExecutorOwner;
    private final InboxFetcherRegistry registry = new InboxFetcherRegistry();
    private final InboxFetchScheduler fetchScheduler;
    private final InboxCheckScheduler checkScheduler;
    private final InboxInsertScheduler insertScheduler;
    private final InboxCommitScheduler commitScheduler;
    private final InboxTouchScheduler touchScheduler;
    private final InboxCreateScheduler createScheduler;
    private final RateLimiter fetcherCreationLimiter;
    private final Duration touchIdle = Duration.ofMinutes(5);
    private ScheduledFuture<?> touchTask;

    InboxService(ISettingProvider settingProvider,
                 IBaseKVStoreClient kvStoreClient,
                 ScheduledExecutorService bgTaskExecutor) {
        this.kvStoreClient = kvStoreClient;
        this.fetchScheduler = new InboxFetchScheduler(kvStoreClient);
        this.checkScheduler = new InboxCheckScheduler(kvStoreClient);
        this.insertScheduler = new InboxInsertScheduler(kvStoreClient);
        this.commitScheduler = new InboxCommitScheduler(kvStoreClient);
        this.touchScheduler = new InboxTouchScheduler(kvStoreClient);
        this.createScheduler = new InboxCreateScheduler(kvStoreClient, settingProvider);
        fetcherCreationLimiter = RateLimiter.create(INBOX_FETCH_PIPELINE_CREATION_RATE_LIMIT.get());
        this.bgTaskExecutorOwner = bgTaskExecutor == null;
        if (bgTaskExecutorOwner) {
            this.bgTaskExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
                new ScheduledThreadPoolExecutor(1,
                    new ThreadFactoryBuilder().setNameFormat("inboxservice-default-bg-executor").build()),
                "inboxservice-default-bg-executor");
        } else {
            this.bgTaskExecutor = bgTaskExecutor;
        }
    }

    @Override
    public void hasInbox(HasInboxRequest request, StreamObserver<HasInboxReply> responseObserver) {
        response(trafficId -> checkScheduler.schedule(request), responseObserver);
    }

    @Override
    public void createInbox(CreateInboxRequest request, StreamObserver<CreateInboxReply> responseObserver) {
        response(trafficId -> createScheduler.schedule(request).exceptionally(e -> CreateInboxReply.newBuilder()
            .setReqId(request.getReqId())
            .setResult(CreateInboxReply.Result.ERROR)
            .build()), responseObserver);
    }

    @Override
    public void deleteInbox(DeleteInboxRequest request, StreamObserver<DeleteInboxReply> responseObserver) {
        response(trafficId -> touchScheduler.schedule(new InboxTouchScheduler.Touch(request))
            .handle((v, e) -> DeleteInboxReply.newBuilder()
                .setReqId(request.getReqId())
                .setResult(e == null ? DeleteInboxReply.Result.OK : DeleteInboxReply.Result.ERROR)
                .build()), responseObserver);
    }

    @Override
    public StreamObserver<SendRequest> receive(StreamObserver<SendReply> responseObserver) {
        return new InboxWriterPipeline(registry, request -> {
            Map<SubInfo, List<TopicMessagePack>> msgsByInbox = new HashMap<>();
            request.getInboxMsgPackList().forEach(inboxMsgPack ->
                inboxMsgPack.getSubInfoList().forEach(subInfo ->
                    msgsByInbox.computeIfAbsent(subInfo, k -> new LinkedList<>()).add(inboxMsgPack.getMessages())));
            List<CompletableFuture<SendResult>> replyFutures = msgsByInbox.entrySet().stream()
                .map(e -> insertScheduler.schedule(MessagePack.newBuilder()
                        .setSubInfo(e.getKey())
                        .addAllMessages(e.getValue())
                        .build())
                    .thenApply(v -> SendResult.newBuilder()
                        .setSubInfo(e.getKey())
                        .setResult(v)
                        .build()))
                .collect(Collectors.toList());
            return CompletableFuture.allOf(replyFutures.toArray(new CompletableFuture[0]))
                .thenApply(v -> replyFutures.stream().map(CompletableFuture::join).collect(Collectors.toList()))
                .thenApply(v -> SendReply.newBuilder()
                    .setReqId(request.getReqId())
                    .addAllResult(v)
                    .build());
        }, responseObserver);
    }

    @Override
    public StreamObserver<FetchHint> fetch(StreamObserver<Fetched> responseObserver) {
        return new InboxFetchPipeline(responseObserver,
            fetchScheduler::schedule,
            scopedInboxId -> touchScheduler.schedule(new InboxTouchScheduler.Touch(scopedInboxId)),
            kvStoreClient, registry,
            fetcherCreationLimiter);
    }

    @Override
    public void commit(CommitRequest request, StreamObserver<CommitReply> responseObserver) {
        response(trafficId -> commitScheduler.schedule(request)
            .exceptionally(e -> CommitReply.newBuilder()
                .setReqId(request.getReqId())
                .setResult(CommitReply.Result.ERROR)
                .build()), responseObserver);
    }

    public void start() {
        if (state.compareAndSet(State.INIT, State.STARTING)) {
            state.set(State.STARTED);
            scheduleTouchIdle();
        }
    }

    public void stop() {
        if (state.compareAndSet(State.STARTED, State.STOPPING)) {
            if (touchTask != null) {
                touchTask.cancel(true);
                try {
                    if (!touchTask.isCancelled()) {
                        touchTask.get();
                    }
                } catch (InterruptedException | ExecutionException e) {
                    log.error("Failed to stop touch task");
                }
            }
            for (IInboxQueueFetcher fetcher : registry) {
                fetcher.close();
            }
            if (bgTaskExecutorOwner) {
                bgTaskExecutor.shutdownNow();
            }
            state.set(State.STOPPED);
        }
    }

    private void scheduleTouchIdle() {
        if (state.get() != State.STARTED) {
            return;
        }
        touchTask = bgTaskExecutor.schedule(this::touchIdle, touchIdle.toMinutes(), TimeUnit.MINUTES);
    }

    private void touchIdle() {
        if (state.get() != State.STARTED) {
            return;
        }
        long now = System.nanoTime();
        Set<String> touched = Sets.newHashSet();
        for (IInboxQueueFetcher fetcher : registry) {
            if (Duration.ofNanos(now - fetcher.lastFetchTS()).compareTo(touchIdle) > 0) {
                if (!touched.contains(fetcher.trafficId() + fetcher.inboxId())) {
                    log.debug("Touch inbox: trafficId={}, inboxId={}", fetcher.trafficId(), fetcher.inboxId());
                    fetcher.touch();
                    touched.add(fetcher.trafficId() + fetcher.inboxId());
                }
            }
        }
        scheduleTouchIdle();
    }
}
