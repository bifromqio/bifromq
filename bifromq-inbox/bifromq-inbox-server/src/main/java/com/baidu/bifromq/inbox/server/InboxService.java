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

package com.baidu.bifromq.inbox.server;

import static com.baidu.bifromq.baserpc.server.UnaryResponse.response;
import static com.baidu.bifromq.inbox.util.InboxServiceUtil.getDelivererKey;
import static com.baidu.bifromq.inbox.util.InboxServiceUtil.receiverId;

import com.baidu.bifromq.basescheduler.exception.BackPressureException;
import com.baidu.bifromq.basescheduler.exception.BatcherUnavailableException;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.client.UnmatchResult;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.rpc.proto.AttachReply;
import com.baidu.bifromq.inbox.rpc.proto.AttachRequest;
import com.baidu.bifromq.inbox.rpc.proto.CommitReply;
import com.baidu.bifromq.inbox.rpc.proto.CommitRequest;
import com.baidu.bifromq.inbox.rpc.proto.CreateReply;
import com.baidu.bifromq.inbox.rpc.proto.CreateRequest;
import com.baidu.bifromq.inbox.rpc.proto.DeleteReply;
import com.baidu.bifromq.inbox.rpc.proto.DeleteRequest;
import com.baidu.bifromq.inbox.rpc.proto.DetachReply;
import com.baidu.bifromq.inbox.rpc.proto.DetachRequest;
import com.baidu.bifromq.inbox.rpc.proto.ExpireAllReply;
import com.baidu.bifromq.inbox.rpc.proto.ExpireAllRequest;
import com.baidu.bifromq.inbox.rpc.proto.ExpireReply;
import com.baidu.bifromq.inbox.rpc.proto.ExpireRequest;
import com.baidu.bifromq.inbox.rpc.proto.GetReply;
import com.baidu.bifromq.inbox.rpc.proto.GetRequest;
import com.baidu.bifromq.inbox.rpc.proto.InboxFetchHint;
import com.baidu.bifromq.inbox.rpc.proto.InboxFetched;
import com.baidu.bifromq.inbox.rpc.proto.InboxServiceGrpc;
import com.baidu.bifromq.inbox.rpc.proto.SendLWTReply;
import com.baidu.bifromq.inbox.rpc.proto.SendLWTRequest;
import com.baidu.bifromq.inbox.rpc.proto.SendReply;
import com.baidu.bifromq.inbox.rpc.proto.SendRequest;
import com.baidu.bifromq.inbox.rpc.proto.SubReply;
import com.baidu.bifromq.inbox.rpc.proto.SubRequest;
import com.baidu.bifromq.inbox.rpc.proto.UnsubReply;
import com.baidu.bifromq.inbox.rpc.proto.UnsubRequest;
import com.baidu.bifromq.inbox.server.scheduler.IInboxAttachScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxCheckSubScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxCommitScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxCreateScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxDeleteScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxDetachScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxFetchScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxGetScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxInsertScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxSendLWTScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxSubScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxUnsubScheduler;
import com.baidu.bifromq.inbox.storage.proto.TopicFilterOption;
import com.baidu.bifromq.plugin.subbroker.CheckReply;
import com.baidu.bifromq.plugin.subbroker.CheckRequest;
import com.baidu.bifromq.util.TopicUtil;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class InboxService extends InboxServiceGrpc.InboxServiceImplBase {
    private final AtomicReference<State> state = new AtomicReference<>(State.INIT);
    private final IInboxClient inboxClient;
    private final IDistClient distClient;
    private final InboxFetcherRegistry registry = new InboxFetcherRegistry();
    private final IInboxFetchScheduler fetchScheduler;
    private final IInboxGetScheduler getScheduler;
    private final IInboxSendLWTScheduler sendLWTScheduler;
    private final IInboxCheckSubScheduler checkSubScheduler;
    private final IInboxInsertScheduler insertScheduler;
    private final IInboxCommitScheduler commitScheduler;
    private final IInboxCreateScheduler createScheduler;
    private final IInboxAttachScheduler attachScheduler;
    private final IInboxDetachScheduler detachScheduler;
    private final IInboxDeleteScheduler deleteScheduler;
    private final IInboxSubScheduler subScheduler;
    private final IInboxUnsubScheduler unsubScheduler;
    private final ITenantGCRunner tenantGCRunner;

    @Builder
    InboxService(IInboxClient inboxClient,
                 IDistClient distClient,
                 IInboxGetScheduler getScheduler,
                 IInboxSendLWTScheduler sendLWTScheduler,
                 IInboxCheckSubScheduler checkSubScheduler,
                 IInboxFetchScheduler fetchScheduler,
                 IInboxInsertScheduler insertScheduler,
                 IInboxCommitScheduler commitScheduler,
                 IInboxCreateScheduler createScheduler,
                 IInboxAttachScheduler attachScheduler,
                 IInboxDetachScheduler detachScheduler,
                 IInboxDeleteScheduler deleteScheduler,
                 IInboxSubScheduler subScheduler,
                 IInboxUnsubScheduler unsubScheduler,
                 ITenantGCRunner tenantGCRunner) {
        this.inboxClient = inboxClient;
        this.distClient = distClient;
        this.getScheduler = getScheduler;
        this.sendLWTScheduler = sendLWTScheduler;
        this.checkSubScheduler = checkSubScheduler;
        this.fetchScheduler = fetchScheduler;
        this.insertScheduler = insertScheduler;
        this.commitScheduler = commitScheduler;
        this.createScheduler = createScheduler;
        this.attachScheduler = attachScheduler;
        this.detachScheduler = detachScheduler;
        this.deleteScheduler = deleteScheduler;
        this.subScheduler = subScheduler;
        this.unsubScheduler = unsubScheduler;
        this.tenantGCRunner = tenantGCRunner;
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetReply> responseObserver) {
        log.trace("Handling get {}", request);
        response(tenantId -> getScheduler.schedule(request)
            .exceptionally(e -> {
                if (e instanceof BatcherUnavailableException || e.getCause() instanceof BatcherUnavailableException) {
                    return GetReply.newBuilder().setReqId(request.getReqId()).setCode(GetReply.Code.TRY_LATER).build();
                }
                if (e instanceof BackPressureException || e.getCause() instanceof BackPressureException) {
                    return GetReply.newBuilder().setReqId(request.getReqId())
                        .setCode(GetReply.Code.BACK_PRESSURE_REJECTED).build();
                }
                log.debug("Failed to get inbox", e);
                return GetReply.newBuilder().setReqId(request.getReqId()).setCode(GetReply.Code.ERROR).build();
            }), responseObserver);
    }

    @Override
    public void create(CreateRequest request, StreamObserver<CreateReply> responseObserver) {
        log.trace("Handling create {}", request);
        assert !request.hasLwt() || request.getLwt().getDelaySeconds() > 0;
        response(tenantId -> createScheduler.schedule(request)
            .exceptionally(e -> {
                if (e instanceof BatcherUnavailableException || e.getCause() instanceof BatcherUnavailableException) {
                    return CreateReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(CreateReply.Code.TRY_LATER)
                        .build();
                }
                if (e instanceof BackPressureException || e.getCause() instanceof BackPressureException) {
                    return CreateReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(CreateReply.Code.BACK_PRESSURE_REJECTED)
                        .build();
                }
                log.debug("Failed to create inbox", e);
                return CreateReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(CreateReply.Code.ERROR)
                    .build();
            }), responseObserver);
    }

    @Override
    public void attach(AttachRequest request, StreamObserver<AttachReply> responseObserver) {
        log.trace("Handling attach {}", request);
        assert !request.hasLwt() || request.getLwt().getDelaySeconds() > 0;
        response(tenantId -> attachScheduler.schedule(request)
            .exceptionally(e -> {
                if (e instanceof BatcherUnavailableException || e.getCause() instanceof BatcherUnavailableException) {
                    return AttachReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(AttachReply.Code.TRY_LATER)
                        .build();
                }
                if (e instanceof BackPressureException || e.getCause() instanceof BackPressureException) {
                    return AttachReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(AttachReply.Code.BACK_PRESSURE_REJECTED)
                        .build();
                }
                log.debug("Failed to attach inbox", e);
                return AttachReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(AttachReply.Code.ERROR)
                    .build();
            }), responseObserver);
    }

    @Override
    public void detach(DetachRequest request, StreamObserver<DetachReply> responseObserver) {
        log.trace("Handling detach {}", request);
        response(tenantId -> detachScheduler.schedule(request)
            .exceptionally(e -> {
                if (e instanceof BatcherUnavailableException || e.getCause() instanceof BatcherUnavailableException) {
                    return DetachReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(DetachReply.Code.TRY_LATER)
                        .build();
                }
                if (e instanceof BackPressureException || e.getCause() instanceof BackPressureException) {
                    return DetachReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(DetachReply.Code.BACK_PRESSURE_REJECTED)
                        .build();
                }
                log.debug("Failed to detach inbox", e);
                return DetachReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(DetachReply.Code.ERROR)
                    .build();
            }), responseObserver);
    }

    @Override
    public void sub(SubRequest request, StreamObserver<SubReply> responseObserver) {
        log.trace("Handling sub {}", request);
        response(tenantId -> subScheduler.schedule(request)
            .thenCompose(subReply -> {
                if (subReply.getCode() == SubReply.Code.OK || subReply.getCode() == SubReply.Code.EXISTS) {
                    return distClient.addRoute(request.getReqId(),
                            request.getTenantId(),
                            TopicUtil.from(request.getTopicFilter()),
                            receiverId(request.getInboxId(), request.getIncarnation()),
                            getDelivererKey(request.getTenantId(), request.getInboxId()),
                            inboxClient.id(),
                            request.getOption().getIncarnation())
                        .thenApply(matchResult -> {
                            switch (matchResult) {
                                case OK -> {
                                    return subReply;
                                }
                                case EXCEED_LIMIT -> {
                                    return SubReply.newBuilder()
                                        .setReqId(request.getReqId())
                                        .setCode(SubReply.Code.EXCEED_LIMIT).build();
                                }
                                case BACK_PRESSURE_REJECTED -> {
                                    return SubReply.newBuilder()
                                        .setReqId(request.getReqId())
                                        .setCode(SubReply.Code.BACK_PRESSURE_REJECTED).build();
                                }
                                case TRY_LATER -> {
                                    return SubReply.newBuilder()
                                        .setReqId(request.getReqId())
                                        .setCode(SubReply.Code.TRY_LATER).build();
                                }
                                default -> {
                                    return SubReply.newBuilder()
                                        .setReqId(request.getReqId())
                                        .setCode(SubReply.Code.ERROR).build();
                                }
                            }
                        });
                }
                return CompletableFuture.completedFuture(subReply);
            })
            .exceptionally(e -> {
                log.debug("Failed to subscribe", e);
                if (e instanceof BatcherUnavailableException || e.getCause() instanceof BatcherUnavailableException) {
                    return SubReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(SubReply.Code.TRY_LATER)
                        .build();
                }
                if (e instanceof BackPressureException || e.getCause() instanceof BackPressureException) {
                    return SubReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(SubReply.Code.BACK_PRESSURE_REJECTED)
                        .build();
                }
                return SubReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(SubReply.Code.ERROR)
                    .build();
            }), responseObserver);
    }

    @Override
    public void unsub(UnsubRequest request, StreamObserver<UnsubReply> responseObserver) {
        log.trace("Handling unsub {}", request);
        response(tenantId -> unsubScheduler.schedule(request)
            .thenCompose(v -> {
                if (v.getCode() == UnsubReply.Code.OK) {
                    return unmatch(request.getReqId(),
                        request.getTenantId(),
                        request.getInboxId(),
                        request.getIncarnation(),
                        request.getTopicFilter(),
                        v.getOption())
                        .thenApply(unmatchResult -> switch (unmatchResult) {
                            case OK -> v;
                            case NOT_EXISTED -> UnsubReply.newBuilder()
                                .setReqId(request.getReqId())
                                .setCode(UnsubReply.Code.NO_SUB)
                                .build();
                            case BACK_PRESSURE_REJECTED -> UnsubReply.newBuilder()
                                .setReqId(request.getReqId())
                                .setCode(UnsubReply.Code.BACK_PRESSURE_REJECTED)
                                .build();
                            case TRY_LATER -> UnsubReply.newBuilder()
                                .setReqId(request.getReqId())
                                .setCode(UnsubReply.Code.TRY_LATER)
                                .build();
                            case ERROR -> UnsubReply.newBuilder()
                                .setReqId(request.getReqId())
                                .setCode(UnsubReply.Code.ERROR)
                                .build();
                        });
                }
                return CompletableFuture.completedFuture(v);
            })
            .exceptionally(e -> {
                if (e instanceof BatcherUnavailableException || e.getCause() instanceof BatcherUnavailableException) {
                    return UnsubReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(UnsubReply.Code.TRY_LATER)
                        .build();
                }
                if (e instanceof BackPressureException || e.getCause() instanceof BackPressureException) {
                    return UnsubReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(UnsubReply.Code.BACK_PRESSURE_REJECTED)
                        .build();
                }
                log.debug("Failed to unsubscribe", e);
                return UnsubReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(UnsubReply.Code.ERROR)
                    .build();
            }), responseObserver);
    }

    private CompletableFuture<UnmatchResult> unmatch(long reqId,
                                                     String tenantId,
                                                     String inboxId,
                                                     long incarnation,
                                                     String topicFilter,
                                                     TopicFilterOption option) {
        return distClient.removeRoute(reqId, tenantId, TopicUtil.from(topicFilter), receiverId(inboxId, incarnation),
            getDelivererKey(tenantId, inboxId), inboxClient.id(), option.getIncarnation());
    }

    @Override
    public void expire(ExpireRequest request, StreamObserver<ExpireReply> responseObserver) {
        log.trace("Handling expire {}", request);
        response(tenantId -> getScheduler.schedule(GetRequest.newBuilder()
                .setReqId(request.getReqId())
                .setTenantId(request.getTenantId())
                .setInboxId(request.getInboxId())
                .setNow(request.getNow())
                .build())
            .exceptionally(e -> {
                if (e instanceof BatcherUnavailableException || e.getCause() instanceof BatcherUnavailableException) {
                    return GetReply.newBuilder().setReqId(request.getReqId()).setCode(GetReply.Code.TRY_LATER).build();
                }
                if (e instanceof BackPressureException || e.getCause() instanceof BackPressureException) {
                    return GetReply.newBuilder().setReqId(request.getReqId())
                        .setCode(GetReply.Code.BACK_PRESSURE_REJECTED).build();
                }
                log.debug("Failed to expire", e);
                return GetReply.newBuilder().setReqId(request.getReqId()).setCode(GetReply.Code.ERROR).build();
            })
            .thenCompose(reply -> {
                switch (reply.getCode()) {
                    case EXIST -> {
                        List<CompletableFuture<DeleteReply>> deleteFutures = reply.getInboxList().stream()
                            .map(inboxVersion -> delete(DeleteRequest.newBuilder()
                                .setReqId(request.getReqId())
                                .setTenantId(request.getTenantId())
                                .setInboxId(request.getInboxId())
                                .setIncarnation(inboxVersion.getIncarnation())
                                .setVersion(inboxVersion.getVersion())
                                .build()))
                            .toList();
                        return CompletableFuture.allOf(deleteFutures.toArray(CompletableFuture[]::new))
                            .thenApply(v -> deleteFutures.stream().map(CompletableFuture::join).toList())
                            .thenApply((deleteReplies) -> {
                                if (deleteReplies.stream().anyMatch(r ->
                                    r.getCode() != DeleteReply.Code.OK && r.getCode() != DeleteReply.Code.NO_INBOX)) {
                                    if (deleteReplies.stream()
                                        .anyMatch(r -> r.getCode() == DeleteReply.Code.TRY_LATER)) {
                                        // Any detach reply is TRY_LATER
                                        return ExpireReply.newBuilder()
                                            .setReqId(request.getReqId())
                                            .setCode(ExpireReply.Code.TRY_LATER)
                                            .build();
                                    }
                                    return ExpireReply.newBuilder()
                                        .setReqId(request.getReqId())
                                        .setCode(ExpireReply.Code.ERROR)
                                        .build();
                                } else {
                                    // OK or NO_INBOX
                                    return ExpireReply.newBuilder()
                                        .setReqId(request.getReqId())
                                        .setCode(ExpireReply.Code.OK)
                                        .build();
                                }
                            });
                    }
                    case NO_INBOX -> {
                        return CompletableFuture.completedFuture(ExpireReply.newBuilder()
                            .setReqId(request.getReqId())
                            .setCode(ExpireReply.Code.OK)
                            .build());
                    }
                    case TRY_LATER -> {
                        return CompletableFuture.completedFuture(ExpireReply.newBuilder()
                            .setReqId(request.getReqId())
                            .setCode(ExpireReply.Code.TRY_LATER)
                            .build());
                    }
                    case BACK_PRESSURE_REJECTED -> {
                        return CompletableFuture.completedFuture(ExpireReply.newBuilder()
                            .setReqId(request.getReqId())
                            .setCode(ExpireReply.Code.BACK_PRESSURE_REJECTED)
                            .build());
                    }
                    default -> {
                        return CompletableFuture.completedFuture(ExpireReply.newBuilder()
                            .setReqId(request.getReqId())
                            .setCode(ExpireReply.Code.ERROR)
                            .build());
                    }
                }
            }), responseObserver);
    }

    @Override
    public void expireAll(ExpireAllRequest request, StreamObserver<ExpireAllReply> responseObserver) {
        log.trace("Handling expireAll {}", request);
        response(tenantId -> tenantGCRunner.expire(request), responseObserver);
    }

    @Override
    public void checkSubscriptions(CheckRequest request, StreamObserver<CheckReply> responseObserver) {
        response(tenantId -> {
            List<CompletableFuture<CheckReply.Code>> futures = request.getMatchInfoList().stream()
                .map(matchInfo -> checkSubScheduler.schedule(
                        new IInboxCheckSubScheduler.CheckMatchInfo(request.getTenantId(), matchInfo))
                    .exceptionally(e -> {
                        if (e instanceof BatcherUnavailableException
                            || e.getCause() instanceof BatcherUnavailableException) {
                            return CheckReply.Code.TRY_LATER;
                        }
                        log.debug("Failed to check subscription", e);
                        return CheckReply.Code.ERROR;
                    }))
                .toList();
            return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .thenApply(v -> futures.stream().map(CompletableFuture::join).toList())
                .thenApply(codes -> CheckReply.newBuilder()
                    .addAllCode(codes)
                    .build());
        }, responseObserver);
    }

    @Override
    public StreamObserver<SendRequest> receive(StreamObserver<SendReply> responseObserver) {
        return new InboxWriterPipeline(new FetcherSignaler(registry),
            new InboxWriter(insertScheduler), responseObserver);
    }

    @Override
    public StreamObserver<InboxFetchHint> fetch(StreamObserver<InboxFetched> responseObserver) {
        return new InboxFetchPipeline(responseObserver, fetchScheduler::schedule, registry);
    }

    @Override
    public void commit(CommitRequest request, StreamObserver<CommitReply> responseObserver) {
        log.trace("Handling commit {}", request);
        response(tenantId -> commitScheduler.schedule(request)
            .exceptionally(e -> {
                if (e instanceof BatcherUnavailableException || e.getCause() instanceof BatcherUnavailableException) {
                    return CommitReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(CommitReply.Code.TRY_LATER)
                        .build();
                }
                if (e instanceof BackPressureException || e.getCause() instanceof BackPressureException) {
                    return CommitReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(CommitReply.Code.BACK_PRESSURE_REJECTED)
                        .build();
                }
                log.debug("Failed to commit", e);
                return CommitReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(CommitReply.Code.ERROR)
                    .build();

            }), responseObserver);
    }

    @Override
    public void sendLWT(SendLWTRequest request, StreamObserver<SendLWTReply> responseObserver) {
        log.trace("Handling send lwt {}", request);
        response(tenantId -> sendLWTScheduler.schedule(request)
            .exceptionally(e -> {
                if (e instanceof BatcherUnavailableException || e.getCause() instanceof BatcherUnavailableException) {
                    return SendLWTReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(SendLWTReply.Code.TRY_LATER)
                        .build();
                }
                if (e instanceof BackPressureException || e.getCause() instanceof BackPressureException) {
                    return SendLWTReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(SendLWTReply.Code.BACK_PRESSURE_REJECTED)
                        .build();
                }
                log.debug("Failed to send LWT", e);
                return SendLWTReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(SendLWTReply.Code.ERROR)
                    .build();
            }), responseObserver);
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteReply> responseObserver) {
        log.trace("Handling delete {}", request);
        response(tenantId -> delete(request), responseObserver);
    }

    private CompletableFuture<DeleteReply> delete(DeleteRequest request) {
        return deleteScheduler.schedule(request)
            .thenCompose(result -> {
                if (result.getCode() == DeleteReply.Code.OK) {
                    List<CompletableFuture<UnmatchResult>> unmatchFutures =
                        result.getTopicFiltersMap().entrySet().stream()
                            .map(e -> unmatch(System.nanoTime(),
                                request.getTenantId(),
                                request.getInboxId(),
                                request.getIncarnation(),
                                e.getKey(),
                                e.getValue()))
                            .toList();
                    return CompletableFuture.allOf(unmatchFutures.toArray(CompletableFuture[]::new))
                        .thenApply(v -> result);
                }
                return CompletableFuture.completedFuture(result);
            })
            .exceptionally(e -> {
                if (e instanceof BatcherUnavailableException || e.getCause() instanceof BatcherUnavailableException) {
                    return DeleteReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(DeleteReply.Code.TRY_LATER)
                        .build();
                }
                if (e instanceof BackPressureException || e.getCause() instanceof BackPressureException) {
                    return DeleteReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(DeleteReply.Code.BACK_PRESSURE_REJECTED)
                        .build();
                }
                log.debug("Failed to delete", e);
                return DeleteReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(DeleteReply.Code.ERROR)
                    .build();
            });
    }

    public void start() {
        if (state.compareAndSet(State.INIT, State.STARTING)) {
            state.set(State.STARTED);
        }
    }

    public void stop() {
        if (state.compareAndSet(State.STARTED, State.STOPPING)) {
            for (IInboxFetcher fetcher : registry) {
                fetcher.close();
            }
            getScheduler.close();
            attachScheduler.close();
            detachScheduler.close();
            deleteScheduler.close();
            createScheduler.close();
            subScheduler.close();
            unsubScheduler.close();

            fetchScheduler.close();
            insertScheduler.close();
            commitScheduler.close();

            state.set(State.STOPPED);
        }
    }

    private enum State {
        INIT,
        STARTING,
        STARTED,
        STOPPING,
        STOPPED
    }
}
