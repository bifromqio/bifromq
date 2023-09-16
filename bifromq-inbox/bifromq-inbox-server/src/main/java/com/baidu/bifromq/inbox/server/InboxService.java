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
import static com.baidu.bifromq.inbox.util.DelivererKeyUtil.getDelivererKey;
import static com.baidu.bifromq.inbox.util.KeyUtil.parseInboxId;
import static com.baidu.bifromq.inbox.util.KeyUtil.parseTenantId;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.INBOX_FETCH_PIPELINE_CREATION_RATE_LIMIT;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.client.UnmatchResult;
import com.baidu.bifromq.inbox.rpc.proto.CommitReply;
import com.baidu.bifromq.inbox.rpc.proto.CommitRequest;
import com.baidu.bifromq.inbox.rpc.proto.CreateInboxReply;
import com.baidu.bifromq.inbox.rpc.proto.CreateInboxRequest;
import com.baidu.bifromq.inbox.rpc.proto.DeleteInboxReply;
import com.baidu.bifromq.inbox.rpc.proto.DeleteInboxRequest;
import com.baidu.bifromq.inbox.rpc.proto.HasInboxReply;
import com.baidu.bifromq.inbox.rpc.proto.HasInboxRequest;
import com.baidu.bifromq.inbox.rpc.proto.InboxFetchHint;
import com.baidu.bifromq.inbox.rpc.proto.InboxFetched;
import com.baidu.bifromq.inbox.rpc.proto.InboxServiceGrpc;
import com.baidu.bifromq.inbox.rpc.proto.SendReply;
import com.baidu.bifromq.inbox.rpc.proto.SendRequest;
import com.baidu.bifromq.inbox.rpc.proto.SendResult;
import com.baidu.bifromq.inbox.rpc.proto.SubReply;
import com.baidu.bifromq.inbox.rpc.proto.SubRequest;
import com.baidu.bifromq.inbox.rpc.proto.TouchInboxReply;
import com.baidu.bifromq.inbox.rpc.proto.TouchInboxRequest;
import com.baidu.bifromq.inbox.rpc.proto.UnsubReply;
import com.baidu.bifromq.inbox.rpc.proto.UnsubRequest;
import com.baidu.bifromq.inbox.server.scheduler.IInboxCheckScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxCommitScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxCreateScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxFetchScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxInsertScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxSubScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxTouchScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxUnsubScheduler;
import com.baidu.bifromq.inbox.server.scheduler.InboxCheckScheduler;
import com.baidu.bifromq.inbox.server.scheduler.InboxCommitScheduler;
import com.baidu.bifromq.inbox.server.scheduler.InboxCreateScheduler;
import com.baidu.bifromq.inbox.server.scheduler.InboxFetchScheduler;
import com.baidu.bifromq.inbox.server.scheduler.InboxInsertScheduler;
import com.baidu.bifromq.inbox.server.scheduler.InboxSubScheduler;
import com.baidu.bifromq.inbox.server.scheduler.InboxTouchScheduler;
import com.baidu.bifromq.inbox.server.scheduler.InboxUnSubScheduler;
import com.baidu.bifromq.inbox.storage.proto.MessagePack;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.common.util.concurrent.RateLimiter;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    private final ScheduledExecutorService bgTaskExecutor;
    private final boolean bgTaskExecutorOwner;
    private final IDistClient distClient;
    private final InboxFetcherRegistry registry = new InboxFetcherRegistry();
    private final IInboxFetchScheduler fetchScheduler;
    private final IInboxCheckScheduler checkScheduler;
    private final IInboxInsertScheduler insertScheduler;
    private final IInboxCommitScheduler commitScheduler;
    //    private final IInboxComSertScheduler comSertScheduler;
    private final IInboxTouchScheduler touchScheduler;
    private final IInboxCreateScheduler createScheduler;
    private final IInboxSubScheduler subScheduler;
    private final IInboxUnsubScheduler unsubScheduler;
    private final RateLimiter fetcherCreationLimiter;
    private final Duration touchIdle = Duration.ofMinutes(5);
    private ScheduledFuture<?> touchTask;

    InboxService(ISettingProvider settingProvider,
                 IDistClient distClient,
                 IBaseKVStoreClient inboxStoreClient,
                 ScheduledExecutorService bgTaskExecutor) {
        this.distClient = distClient;
        this.checkScheduler = new InboxCheckScheduler(inboxStoreClient);
        this.fetchScheduler = new InboxFetchScheduler(inboxStoreClient);
        this.insertScheduler = new InboxInsertScheduler(inboxStoreClient);
        this.commitScheduler = new InboxCommitScheduler(inboxStoreClient);
//        this.comSertScheduler = new InboxComSertScheduler2(inboxStoreClient);
        this.createScheduler = new InboxCreateScheduler(inboxStoreClient, settingProvider);
        this.subScheduler = new InboxSubScheduler(inboxStoreClient);
        this.unsubScheduler = new InboxUnSubScheduler(inboxStoreClient);
        this.touchScheduler = new InboxTouchScheduler(inboxStoreClient);
        fetcherCreationLimiter = RateLimiter.create(INBOX_FETCH_PIPELINE_CREATION_RATE_LIMIT.get());
        this.bgTaskExecutorOwner = bgTaskExecutor == null;
        if (bgTaskExecutorOwner) {
            this.bgTaskExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
                new ScheduledThreadPoolExecutor(1, EnvProvider.INSTANCE.newThreadFactory("inboxservice-bg-executor")),
                "inboxservice-default-bg-executor");
        } else {
            this.bgTaskExecutor = bgTaskExecutor;
        }
    }

    @Override
    public void hasInbox(HasInboxRequest request, StreamObserver<HasInboxReply> responseObserver) {
        response(tenantId -> checkScheduler.schedule(request), responseObserver);
    }

    @Override
    public void createInbox(CreateInboxRequest request, StreamObserver<CreateInboxReply> responseObserver) {
        response(tenantId -> createScheduler.schedule(request)
            .thenCompose(topicFilters -> {
                if (topicFilters.isEmpty()) {
                    return CompletableFuture.completedFuture(CreateInboxReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setResult(CreateInboxReply.Result.OK)
                        .build());
                } else {
                    List<CompletableFuture<UnmatchResult>> unsubFutures = topicFilters.stream().map(
                        topicFilter -> distClient.unmatch(request.getReqId(),
                            request.getClientInfo().getTenantId(),
                            topicFilter,
                            request.getInboxId(),
                            getDelivererKey(request.getInboxId()), 1
                        )).toList();
                    return CompletableFuture.allOf(unsubFutures.toArray(CompletableFuture[]::new))
                        .thenApply(v -> unsubFutures.stream().map(CompletableFuture::join).toList())
                        .handle((unsubResults, e) -> {
                            if (e != null) {
                                log.debug("Failed to create inbox", e);
                                return CreateInboxReply.newBuilder()
                                    .setReqId(request.getReqId())
                                    .setResult(CreateInboxReply.Result.ERROR)
                                    .build();
                            } else {
                                if (unsubResults.stream().allMatch(result -> result == UnmatchResult.OK)) {
                                    return CreateInboxReply.newBuilder()
                                        .setReqId(request.getReqId())
                                        .setResult(CreateInboxReply.Result.OK)
                                        .build();
                                } else {
                                    return CreateInboxReply.newBuilder()
                                        .setReqId(request.getReqId())
                                        .setResult(CreateInboxReply.Result.ERROR)
                                        .build();
                                }
                            }
                        });
                }
            })
            .exceptionally(e -> {
                log.debug("Failed to create inbox", e);
                return CreateInboxReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(CreateInboxReply.Result.ERROR)
                    .build();
            }), responseObserver);
    }

    @Override
    public void deleteInbox(DeleteInboxRequest request, StreamObserver<DeleteInboxReply> responseObserver) {
        response(tenantId -> touchScheduler.schedule(new IInboxTouchScheduler.Touch(request))
            .exceptionally(e -> Collections.emptyList())
            .thenCompose(topicFilters -> {
                if (topicFilters.isEmpty()) {
                    return CompletableFuture.completedFuture(null);
                } else {
                    long reqId = System.nanoTime();
                    List<CompletableFuture<UnmatchResult>> unsubFutures = topicFilters.stream().map(
                        topicFilter -> distClient.unmatch(reqId,
                            tenantId,
                            topicFilter,
                            request.getInboxId(),
                            getDelivererKey(request.getInboxId()), 1
                        )).toList();
                    return CompletableFuture.allOf(unsubFutures.toArray(CompletableFuture[]::new))
                        .thenApply(v -> unsubFutures.stream().map(CompletableFuture::join).toList())
                        .handle((unsubResults, e) -> {
                            if (e != null) {
                                log.error("Touch inbox error", e);
                            }
                            return null;
                        });
                }
            })
            .handle((v, e) -> DeleteInboxReply.newBuilder()
                .setReqId(request.getReqId())
                .setResult(e == null ? DeleteInboxReply.Result.OK : DeleteInboxReply.Result.ERROR)
                .build()), responseObserver);
    }

    @Override
    public void touchInbox(TouchInboxRequest request, StreamObserver<TouchInboxReply> responseObserver) {
        response(tenantId -> touchScheduler.schedule(new IInboxTouchScheduler.Touch(request))
            .exceptionally(e -> Collections.emptyList())
            .thenCompose(topicFilters -> {
                if (topicFilters.isEmpty()) {
                    return CompletableFuture.completedFuture(null);
                } else {
                    long reqId = System.nanoTime();
                    List<CompletableFuture<UnmatchResult>> unsubFutures = topicFilters.stream().map(
                        topicFilter -> distClient.unmatch(reqId,
                            tenantId,
                            topicFilter,
                            request.getInboxId(),
                            getDelivererKey(request.getInboxId()), 1
                        )).toList();
                    return CompletableFuture.allOf(unsubFutures.toArray(CompletableFuture[]::new))
                        .thenApply(v -> unsubFutures.stream().map(CompletableFuture::join).toList())
                        .handle((unsubResults, e) -> {
                            if (e != null) {
                                log.error("Touch inbox error", e);
                            }
                            return null;
                        });
                }
            })
            .handle((v, e) -> {
                if (e != null) {
                    log.error("Touch inbox failed: inboxId={}", request.getInboxId(), e);
                }
                return TouchInboxReply.newBuilder()
                    .setReqId(request.getReqId())
                    .build();
            }), responseObserver);
    }

    @Override
    public void sub(SubRequest request, StreamObserver<SubReply> responseObserver) {
        response(tenantId -> distClient.match(request.getReqId(),
                request.getTenantId(),
                request.getTopicFilter(),
                request.getSubQoS(),
                request.getInboxId(),
                getDelivererKey(request.getInboxId()), 1)
            .thenCompose(matchResult -> {
                switch (matchResult) {
                    case OK -> {
                        return subScheduler.schedule(request);
                    }
                    case EXCEED_LIMIT -> {
                        return CompletableFuture.completedFuture(SubReply.newBuilder()
                            .setReqId(request.getReqId())
                            .setResult(SubReply.Result.EXCEED_LIMIT)
                            .build());
                    }
                    default -> {
                        return CompletableFuture.completedFuture(SubReply.newBuilder()
                            .setReqId(request.getReqId())
                            .setResult(SubReply.Result.ERROR)
                            .build());
                    }
                }
            })
            .exceptionally(e -> {
                log.error("Failed to subscribe", e);
                return SubReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(SubReply.Result.ERROR)
                    .build();

            }), responseObserver);
    }

    @Override
    public void unsub(UnsubRequest request, StreamObserver<UnsubReply> responseObserver) {
        response(tenantId -> distClient.unmatch(request.getReqId(),
                request.getTenantId(),
                request.getTopicFilter(),
                request.getInboxId(),
                getDelivererKey(request.getInboxId()), 1)
            .thenCompose(unmatchResult -> {
                if (Objects.requireNonNull(unmatchResult) == UnmatchResult.OK) {
                    return unsubScheduler.schedule(request);
                }
                return CompletableFuture.completedFuture(UnsubReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(UnsubReply.Result.ERROR)
                    .build());
            })
            .exceptionally(e -> {
                log.error("Failed to unsubscribe", e);
                return UnsubReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(UnsubReply.Result.ERROR)
                    .build();
            }), responseObserver);
    }

    @Override
    public StreamObserver<SendRequest> receive(StreamObserver<SendReply> responseObserver) {
        return new InboxWriterPipeline(registry, request -> {
            Map<SubInfo, List<TopicMessagePack>> msgsByInbox = new HashMap<>();
            request.getInboxMsgPackList().forEach(inboxMsgPack ->
                inboxMsgPack.getSubInfoList().forEach(subInfo ->
                    msgsByInbox.computeIfAbsent(subInfo, k -> new LinkedList<>()).add(inboxMsgPack.getMessages())));
            List<CompletableFuture<SendResult>> replyFutures = msgsByInbox.entrySet()
                .stream()
                .map(entry -> insertScheduler.schedule(MessagePack.newBuilder()
                        .setSubInfo(entry.getKey())
                        .addAllMessages(entry.getValue())
                        .build())
                    .handle((v, e) -> SendResult.newBuilder()
                        .setSubInfo(entry.getKey())
                        .setResult(e != null ? SendResult.Result.ERROR : v)
                        .build()))
                .toList();
            return CompletableFuture.allOf(replyFutures.toArray(new CompletableFuture[0]))
                .thenApply(v -> replyFutures.stream().map(CompletableFuture::join).collect(Collectors.toList()))
                .thenApply(v -> SendReply.newBuilder()
                    .setReqId(request.getReqId())
                    .addAllResult(v)
                    .build());
        }, responseObserver);
    }

    @Override
    public StreamObserver<InboxFetchHint> fetchInbox(StreamObserver<InboxFetched> responseObserver) {
        return new InboxFetchPipeline(responseObserver, fetchScheduler::schedule,
            scopedInboxId -> touchScheduler.schedule(new IInboxTouchScheduler.Touch(scopedInboxId))
                .exceptionally(e -> Collections.emptyList())
                .thenCompose(topicFilters -> {
                    if (topicFilters.isEmpty()) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        long reqId = System.nanoTime();
                        String tenantId = parseTenantId(scopedInboxId);
                        String inboxId = parseInboxId(scopedInboxId);
                        List<CompletableFuture<UnmatchResult>> unsubFutures = topicFilters.stream().map(
                            topicFilter -> distClient.unmatch(reqId,
                                tenantId,
                                topicFilter,
                                inboxId,
                                getDelivererKey(inboxId), 1
                            )).toList();
                        return CompletableFuture.allOf(unsubFutures.toArray(CompletableFuture[]::new))
                            .thenApply(v -> unsubFutures.stream().map(CompletableFuture::join).toList())
                            .handle((unsubResults, e) -> {
                                if (e != null) {
                                    log.error("Touch inbox error", e);
                                }
                                return null;
                            });
                    }
                }), registry);
    }

    @Override
    public void commit(CommitRequest request, StreamObserver<CommitReply> responseObserver) {
        response(tenantId -> commitScheduler.schedule(request)
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
            for (IInboxFetcher fetcher : registry) {
                fetcher.close();
            }
            checkScheduler.close();
            checkScheduler.close();
            fetchScheduler.close();
            insertScheduler.close();
            commitScheduler.close();
            createScheduler.close();
            subScheduler.close();
            unsubScheduler.close();
            touchScheduler.close();
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
        for (IInboxFetcher fetcher : registry) {
            log.debug("Touch inbox: tenantId={}, delivererKey={}", fetcher.tenantId(), fetcher.delivererKey());
            fetcher.touch();
        }
        scheduleTouchIdle();
    }
}
