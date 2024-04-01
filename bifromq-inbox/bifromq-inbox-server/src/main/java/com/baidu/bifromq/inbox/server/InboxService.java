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

import static com.baidu.bifromq.baserpc.UnaryResponse.response;
import static com.baidu.bifromq.inbox.records.ScopedInbox.distInboxId;
import static com.baidu.bifromq.inbox.util.DelivererKeyUtil.getDelivererKey;
import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static com.baidu.bifromq.plugin.settingprovider.Setting.RetainEnabled;
import static com.bifromq.plugin.resourcethrottler.TenantResourceType.TotalRetainMessageSpaceBytes;
import static com.bifromq.plugin.resourcethrottler.TenantResourceType.TotalRetainTopics;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.dist.client.DistResult;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.client.UnmatchResult;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.records.ScopedInbox;
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
import com.baidu.bifromq.inbox.rpc.proto.InboxFetchHint;
import com.baidu.bifromq.inbox.rpc.proto.InboxFetched;
import com.baidu.bifromq.inbox.rpc.proto.InboxServiceGrpc;
import com.baidu.bifromq.inbox.rpc.proto.SendReply;
import com.baidu.bifromq.inbox.rpc.proto.SendRequest;
import com.baidu.bifromq.inbox.rpc.proto.SubReply;
import com.baidu.bifromq.inbox.rpc.proto.SubRequest;
import com.baidu.bifromq.inbox.rpc.proto.TouchReply;
import com.baidu.bifromq.inbox.rpc.proto.TouchRequest;
import com.baidu.bifromq.inbox.rpc.proto.UnsubReply;
import com.baidu.bifromq.inbox.rpc.proto.UnsubRequest;
import com.baidu.bifromq.inbox.server.scheduler.IInboxAttachScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxCommitScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxCreateScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxDeleteScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxDetachScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxFetchScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxGetScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxInsertScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxSubScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxTouchScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxUnsubScheduler;
import com.baidu.bifromq.inbox.storage.proto.BatchDeleteReply;
import com.baidu.bifromq.inbox.storage.proto.BatchDeleteRequest;
import com.baidu.bifromq.inbox.storage.proto.LWT;
import com.baidu.bifromq.inbox.store.gc.IInboxStoreGCProcessor;
import com.baidu.bifromq.inbox.store.gc.InboxStoreGCProcessor;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ResourceThrottled;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.disthandling.WillDistError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.disthandling.WillDisted;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.retainhandling.MsgRetained;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.retainhandling.MsgRetainedError;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.retainhandling.RetainMsgCleared;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.retain.client.IRetainClient;
import com.baidu.bifromq.retain.rpc.proto.RetainReply;
import com.baidu.bifromq.type.ClientInfo;
import com.bifromq.plugin.resourcethrottler.IResourceThrottler;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import lombok.Builder;
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
    private final ISettingProvider settingProvider;
    private final IEventCollector eventCollector;
    private final IResourceThrottler resourceThrottler;
    private final IInboxClient inboxClient;
    private final IDistClient distClient;
    private final IRetainClient retainClient;
    private final IBaseKVStoreClient inboxStoreClient;
    private final InboxFetcherRegistry registry = new InboxFetcherRegistry();
    private final IInboxFetchScheduler fetchScheduler;
    private final IInboxGetScheduler getScheduler;
    private final IInboxInsertScheduler insertScheduler;
    private final IInboxCommitScheduler commitScheduler;
    private final IInboxTouchScheduler touchScheduler;
    private final IInboxCreateScheduler createScheduler;
    private final IInboxAttachScheduler attachScheduler;
    private final IInboxDetachScheduler detachScheduler;
    private final IInboxDeleteScheduler deleteScheduler;
    private final IInboxSubScheduler subScheduler;
    private final IInboxUnsubScheduler unsubScheduler;
    private final IInboxStoreGCProcessor inboxGCProc;
    private final DelayTaskRunner<ScopedInbox, ExpireSessionTask> delayTaskRunner;

    @Builder
    InboxService(IEventCollector eventCollector,
                 IResourceThrottler resourceThrottler,
                 ISettingProvider settingProvider,
                 IInboxClient inboxClient,
                 IDistClient distClient,
                 IRetainClient retainClient,
                 IBaseKVStoreClient inboxStoreClient,
                 IInboxGetScheduler getScheduler,
                 IInboxFetchScheduler fetchScheduler,
                 IInboxInsertScheduler insertScheduler,
                 IInboxCommitScheduler commitScheduler,
                 IInboxCreateScheduler createScheduler,
                 IInboxAttachScheduler attachScheduler,
                 IInboxDetachScheduler detachScheduler,
                 IInboxDeleteScheduler deleteScheduler,
                 IInboxSubScheduler subScheduler,
                 IInboxUnsubScheduler unsubScheduler,
                 IInboxTouchScheduler touchScheduler) {
        this.eventCollector = eventCollector;
        this.resourceThrottler = resourceThrottler;
        this.settingProvider = settingProvider;
        this.inboxClient = inboxClient;
        this.distClient = distClient;
        this.retainClient = retainClient;
        this.inboxStoreClient = inboxStoreClient;
        this.getScheduler = getScheduler;
        this.fetchScheduler = fetchScheduler;
        this.insertScheduler = insertScheduler;
        this.commitScheduler = commitScheduler;
        this.createScheduler = createScheduler;
        this.attachScheduler = attachScheduler;
        this.detachScheduler = detachScheduler;
        this.deleteScheduler = deleteScheduler;
        this.subScheduler = subScheduler;
        this.unsubScheduler = unsubScheduler;
        this.touchScheduler = touchScheduler;
        this.inboxGCProc = new InboxStoreGCProcessor(inboxClient, inboxStoreClient);
        this.delayTaskRunner = new DelayTaskRunner<>(ScopedInbox::compareTo, HLC.INST::getPhysical);
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetReply> responseObserver) {
        log.trace("Handling get {}", request);
        response(tenantId -> getScheduler.schedule(request)
                .exceptionally(e -> {
                    log.debug("Failed to get inbox", e);
                    return GetReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(GetReply.Code.ERROR)
                        .build();
                })
            , responseObserver);
    }

    @Override
    public void create(CreateRequest request, StreamObserver<CreateReply> responseObserver) {
        log.trace("Handling create {}", request);
        assert !request.hasLwt() || request.getLwt().getDelaySeconds() > 0;
        response(tenantId -> createScheduler.schedule(request)
            .exceptionally(e -> {
                log.debug("Failed to create inbox", e);
                return CreateReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(CreateReply.Code.ERROR)
                    .build();
            })
            .whenComplete((v, e) -> {
                //reg a deadline for this inbox
                if (v != null && v.getCode() == CreateReply.Code.OK) {
                    ScopedInbox scopedInbox = new ScopedInbox(
                        request.getClient().getTenantId(),
                        request.getInboxId(),
                        request.getIncarnation());
                    LWT lwt = request.hasLwt() ? request.getLwt() : null;
                    if (lwt != null) {
                        delayTaskRunner.reg(scopedInbox,
                            idleTimeout(request.getKeepAliveSeconds()).plusSeconds(lwt.getDelaySeconds()),
                            new ExpireSessionTask(scopedInbox, 0,
                                request.getExpirySeconds(),
                                request.getClient(),
                                lwt));
                    } else {
                        delayTaskRunner.reg(scopedInbox,
                            idleTimeout(request.getKeepAliveSeconds()).plusSeconds(request.getExpirySeconds()),
                            new ExpireSessionTask(scopedInbox, 0,
                                request.getExpirySeconds(),
                                request.getClient(),
                                null));
                    }
                }
            }), responseObserver);
    }

    @Override
    public void attach(AttachRequest request, StreamObserver<AttachReply> responseObserver) {
        log.trace("Handling attach {}", request);
        assert !request.hasLwt() || request.getLwt().getDelaySeconds() > 0;
        response(tenantId -> attachScheduler.schedule(request)
            .exceptionally(e -> {
                log.debug("Failed to attach inbox", e);
                return AttachReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(AttachReply.Code.ERROR)
                    .build();
            })
            .whenComplete((v, e) -> {
                //reg a deadline for this inbox
                if (v != null && v.getCode() == AttachReply.Code.OK) {
                    ScopedInbox scopedInbox = new ScopedInbox(
                        request.getClient().getTenantId(),
                        request.getInboxId(),
                        request.getIncarnation());
                    LWT lwt = request.hasLwt() ? request.getLwt() : null;
                    if (lwt != null) {
                        delayTaskRunner.reg(scopedInbox,
                            idleTimeout(request.getKeepAliveSeconds()).plusSeconds(lwt.getDelaySeconds()),
                            new ExpireSessionTask(scopedInbox,
                                request.getVersion() + 1,
                                request.getExpirySeconds(), request.getClient(),
                                lwt));
                    } else {
                        delayTaskRunner.reg(scopedInbox,
                            idleTimeout(request.getKeepAliveSeconds()).plusSeconds(request.getExpirySeconds()),
                            new ExpireSessionTask(scopedInbox,
                                request.getVersion() + 1,
                                request.getExpirySeconds(),
                                request.getClient(),
                                null));
                    }
                }
            }), responseObserver);
    }

    private Duration idleTimeout(int keepAliveSeconds) {
        return Duration.ofMillis((long) (Duration.ofSeconds(keepAliveSeconds).toMillis() * 1.5));
    }

    @Override
    public void detach(DetachRequest request, StreamObserver<DetachReply> responseObserver) {
        response(tenantId -> detach(request), responseObserver);
    }

    private CompletableFuture<DetachReply> detach(DetachRequest request) {
        log.trace("Handling detach {}", request);
        return detachScheduler.schedule(request)
            .exceptionally(e -> {
                log.debug("Failed to detach inbox", e);
                return DetachReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(DetachReply.Code.ERROR)
                    .build();
            })
            .whenComplete((reply, e) -> {
                if (reply == null || reply.getCode() != DetachReply.Code.OK) {
                    return;
                }
                ScopedInbox scopedInbox = new ScopedInbox(
                    request.getClient().getTenantId(),
                    request.getInboxId(),
                    request.getIncarnation());
                delayTaskRunner.unreg(scopedInbox);
                LWT lwt = reply.hasLwt() ? reply.getLwt() : null;
                if (lwt != null) {
                    assert lwt.getDelaySeconds() > 0;
                    delayTaskRunner.reg(scopedInbox,
                        Duration.ofSeconds(Math.min(lwt.getDelaySeconds(), request.getExpirySeconds())),
                        new ExpireSessionTask(scopedInbox, request.getVersion() + 1, request.getExpirySeconds(),
                            request.getClient(), lwt));
                } else {
                    delayTaskRunner.reg(scopedInbox, Duration.ofSeconds(request.getExpirySeconds()),
                        new ExpireSessionTask(scopedInbox, request.getVersion() + 1, request.getExpirySeconds(),
                            request.getClient(), null));
                }
            });
    }

    @Override
    public void touch(TouchRequest request, StreamObserver<TouchReply> responseObserver) {
        log.trace("Handling touch {}", request);
        response(tenantId -> touchScheduler.schedule(request)
            .exceptionally(e -> {
                log.debug("Failed to touch inbox", e);
                return TouchReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(TouchReply.Code.ERROR)
                    .build();
            })
            .whenComplete((v, e) -> {
                //update monitored deadline record
                if (v != null && v.getCode() == TouchReply.Code.OK) {
                    ScopedInbox scopedInbox = new ScopedInbox(
                        request.getTenantId(),
                        request.getInboxId(),
                        request.getIncarnation());
                    delayTaskRunner.touch(scopedInbox);
                }
            }), responseObserver);
    }

    @Override
    public void sub(SubRequest request, StreamObserver<SubReply> responseObserver) {
        log.trace("Handling sub {}", request);
        response(tenantId -> subScheduler.schedule(request)
                .thenCompose(subReply -> {
                    if (subReply.getCode() == SubReply.Code.OK || subReply.getCode() == SubReply.Code.EXISTS) {
                        return distClient.match(request.getReqId(),
                                request.getTenantId(),
                                request.getTopicFilter(),
                                distInboxId(request.getInboxId(), request.getIncarnation()),
                                getDelivererKey(request.getInboxId()), inboxClient.id())
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
                    return SubReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(SubReply.Code.ERROR)
                        .build();
                })
                .whenComplete((v, e) -> {
                    //update monitored deadline record
                    if (v != null &&
                        (v.getCode() == SubReply.Code.OK
                            || v.getCode() == SubReply.Code.EXISTS
                            || v.getCode() == SubReply.Code.EXCEED_LIMIT)) {
                        ScopedInbox scopedInbox = new ScopedInbox(
                            request.getTenantId(),
                            request.getInboxId(),
                            request.getIncarnation());
                        delayTaskRunner.touch(scopedInbox);
                    }
                })
            , responseObserver);
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
                        request.getTopicFilter())
                        .thenApply(unmatchResult -> {
                            if (unmatchResult == UnmatchResult.OK) {
                                return v;
                            } else {
                                return UnsubReply.newBuilder()
                                    .setReqId(request.getReqId())
                                    .setCode(UnsubReply.Code.ERROR)
                                    .build();
                            }
                        });
                }
                return CompletableFuture.completedFuture(v);
            })
            .exceptionally(e -> {
                log.debug("Failed to unsubscribe", e);
                return UnsubReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(UnsubReply.Code.ERROR)
                    .build();
            })
            .whenComplete((v, e) -> {
                //update monitored deadline record
                if (v != null && v.getCode() == UnsubReply.Code.OK) {
                    ScopedInbox scopedInbox = new ScopedInbox(
                        request.getTenantId(),
                        request.getInboxId(),
                        request.getIncarnation());
                    delayTaskRunner.touch(scopedInbox);
                }
            }), responseObserver);
    }

    private CompletableFuture<UnmatchResult> unmatch(long reqId,
                                                     String tenantId,
                                                     String inboxId,
                                                     long incarnation,
                                                     String topicFilter) {
        return distClient.unmatch(reqId, tenantId, topicFilter, distInboxId(inboxId, incarnation),
            getDelivererKey(inboxId), 1);
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
                log.debug("Failed to expire", e);
                return GetReply.newBuilder().setReqId(request.getReqId()).setCode(GetReply.Code.ERROR).build();
            })
            .thenCompose(reply -> {
                switch (reply.getCode()) {
                    case EXIST -> {
                        List<CompletableFuture<DetachReply>> detachTasks =
                            reply.getInboxList().stream().map(inboxVersion -> detach(DetachRequest.newBuilder()
                                .setReqId(request.getReqId())
                                .setInboxId(request.getInboxId())
                                .setIncarnation(inboxVersion.getIncarnation())
                                .setVersion(inboxVersion.getVersion())
                                .setExpirySeconds(0) // detach now
                                .setDiscardLWT(false)
                                .setClient(inboxVersion.getClient())
                                .setNow(request.getNow())
                                .build())).toList();
                        return CompletableFuture.allOf(detachTasks.toArray(CompletableFuture[]::new))
                            .thenApply(v -> detachTasks.stream().map(CompletableFuture::join).toList())
                            .handle((detachReplies, e) -> {
                                if (e != null ||
                                    detachReplies.stream().anyMatch(r -> r.getCode() != DetachReply.Code.OK)) {
                                    return ExpireReply.newBuilder()
                                        .setReqId(request.getReqId())
                                        .setCode(ExpireReply.Code.ERROR)
                                        .build();
                                } else {
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
        response(tenantId ->
            inboxGCProc.gc(request.getReqId(), request.getTenantId(), request.getExpirySeconds(), request.getNow())
                .thenApply(result -> ExpireAllReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(result == IInboxStoreGCProcessor.Result.OK ?
                        ExpireAllReply.Code.OK : ExpireAllReply.Code.ERROR)
                    .build())
                .exceptionally(e -> {
                    log.debug("Failed to expire all", e);
                    return ExpireAllReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(ExpireAllReply.Code.ERROR)
                        .build();
                }), responseObserver);
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
                log.debug("Failed to commit", e);
                return CommitReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(CommitReply.Code.ERROR)
                    .build();
            })
            .whenComplete((v, e) -> {
                // update monitored deadline record
                if (v != null && v.getCode() == CommitReply.Code.OK) {
                    ScopedInbox scopedInbox = new ScopedInbox(
                        request.getTenantId(),
                        request.getInboxId(),
                        request.getIncarnation());
                    delayTaskRunner.touch(scopedInbox);
                }
            }), responseObserver);
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
            touchScheduler.close();
            subScheduler.close();
            unsubScheduler.close();

            fetchScheduler.close();
            insertScheduler.close();
            commitScheduler.close();

            state.set(State.STOPPED);
        }
    }

    private class ExpireSessionTask implements Runnable {
        private final ScopedInbox scopedInbox;
        private final int expireSeconds;
        private final LWT lwt;
        private final ClientInfo client;
        private final long version;

        ExpireSessionTask(ScopedInbox scopedInbox,
                          long version,
                          int expireSeconds,
                          ClientInfo client,
                          @Nullable LWT lwt) {
            this.scopedInbox = scopedInbox;
            this.expireSeconds = expireSeconds;
            this.client = client;
            this.lwt = lwt;
            this.version = version;
        }

        @Override
        public void run() {
            if (lwt != null) {
                long reqId = HLC.INST.getPhysical();
                CompletableFuture<DistResult> distLWTFuture = distClient.pub(reqId,
                    lwt.getTopic(),
                    lwt.getMessage().toBuilder()
                        .setMessageId(0)
                        .setTimestamp(reqId) // refresh the timestamp
                        .build(),
                    client);
                CompletableFuture<RetainReply.Result> retainLWTFuture;
                boolean willRetain = lwt.getMessage().getIsRetain();
                boolean retainEnabled = settingProvider.provide(RetainEnabled, client.getTenantId());
                if (willRetain) {
                    if (!retainEnabled) {
                        eventCollector.report(getLocal(MsgRetainedError.class)
                            .reqId(reqId)
                            .topic(lwt.getTopic())
                            .qos(lwt.getMessage().getPubQoS())
                            .payload(lwt.getMessage().getPayload().asReadOnlyByteBuffer())
                            .size(lwt.getMessage().getPayload().size())
                            .reason("Retain Disabled")
                            .clientInfo(client));
                        retainLWTFuture = CompletableFuture.completedFuture(RetainReply.Result.ERROR);
                    } else {
                        retainLWTFuture = retain(reqId, lwt, client)
                            .thenApply(v -> {
                                switch (v) {
                                    case RETAINED -> eventCollector.report(getLocal(MsgRetained.class)
                                        .topic(lwt.getTopic())
                                        .qos(lwt.getMessage().getPubQoS())
                                        .isLastWill(true)
                                        .size(lwt.getMessage().getPayload().size())
                                        .clientInfo(client));
                                    case CLEARED -> eventCollector.report(getLocal(RetainMsgCleared.class)
                                        .topic(lwt.getTopic())
                                        .isLastWill(true)
                                        .clientInfo(client));
                                    case BACK_PRESSURE_REJECTED ->
                                        eventCollector.report(getLocal(MsgRetainedError.class)
                                            .topic(lwt.getTopic())
                                            .qos(lwt.getMessage().getPubQoS())
                                            .isLastWill(true)
                                            .payload(lwt.getMessage().getPayload().asReadOnlyByteBuffer())
                                            .size(lwt.getMessage().getPayload().size())
                                            .reason("Server Busy")
                                            .clientInfo(client));
                                    case EXCEED_LIMIT -> eventCollector.report(getLocal(MsgRetainedError.class)
                                        .topic(lwt.getTopic())
                                        .qos(lwt.getMessage().getPubQoS())
                                        .isLastWill(true)
                                        .payload(lwt.getMessage().getPayload().asReadOnlyByteBuffer())
                                        .size(lwt.getMessage().getPayload().size())
                                        .reason("Exceed Limit")
                                        .clientInfo(client));
                                    case ERROR -> eventCollector.report(getLocal(MsgRetainedError.class)
                                        .topic(lwt.getTopic())
                                        .qos(lwt.getMessage().getPubQoS())
                                        .isLastWill(true)
                                        .payload(lwt.getMessage().getPayload().asReadOnlyByteBuffer())
                                        .size(lwt.getMessage().getPayload().size())
                                        .reason("Internal Error")
                                        .clientInfo(client));
                                }
                                return v;
                            });
                    }
                } else {
                    retainLWTFuture = CompletableFuture.completedFuture(null);
                }
                CompletableFuture.allOf(distLWTFuture, retainLWTFuture)
                    .thenAccept(v -> {
                        DistResult distResult = distLWTFuture.join();
                        boolean retry = distResult == DistResult.ERROR;
                        if (!retry) {
                            if (willRetain && retainEnabled) {
                                retry = retainLWTFuture.join() == RetainReply.Result.ERROR;
                            }
                        }
                        if (retry) {
                            // Delay some time and retry
                            delayTaskRunner.reg(scopedInbox, Duration.ofSeconds(lwt.getDelaySeconds()),
                                new ExpireSessionTask(scopedInbox, version, expireSeconds, client, lwt));
                        } else {
                            switch (distLWTFuture.join()) {
                                case OK, NO_MATCH -> {
                                    eventCollector.report(getLocal(WillDisted.class)
                                        .reqId(reqId)
                                        .topic(lwt.getTopic())
                                        .qos(lwt.getMessage().getPubQoS())
                                        .size(lwt.getMessage().getPayload().size())
                                        .clientInfo(client));
                                    if (lwt.getDelaySeconds() >= expireSeconds) {
                                        delayTaskRunner.reg(scopedInbox, Duration.ZERO,
                                            new ExpireSessionTask(scopedInbox, version, 0, client, null));
                                    } else {
                                        delayTaskRunner.reg(scopedInbox,
                                            Duration.ofSeconds(expireSeconds - lwt.getDelaySeconds()),
                                            new ExpireSessionTask(scopedInbox, version, 0, client, null));
                                    }
                                }
                                case BACK_PRESSURE_REJECTED -> {
                                    eventCollector.report(getLocal(WillDistError.class)
                                        .reqId(reqId)
                                        .topic(lwt.getTopic())
                                        .qos(lwt.getMessage().getPubQoS())
                                        .size(lwt.getMessage().getPayload().size())
                                        .reason("Server Busy")
                                        .clientInfo(client));
                                    if (lwt.getDelaySeconds() >= expireSeconds) {
                                        delayTaskRunner.reg(scopedInbox, Duration.ZERO,
                                            new ExpireSessionTask(scopedInbox, version, 0, client, null));
                                    } else {
                                        delayTaskRunner.reg(scopedInbox,
                                            Duration.ofSeconds(expireSeconds - lwt.getDelaySeconds()),
                                            new ExpireSessionTask(scopedInbox, version, 0, client, null));
                                    }
                                }
                            }
                        }
                    });
            } else {
                deleteScheduler.schedule(BatchDeleteRequest.Params.newBuilder()
                        .setTenantId(scopedInbox.tenantId())
                        .setInboxId(scopedInbox.inboxId())
                        .setIncarnation(scopedInbox.incarnation())
                        .setVersion(version)
                        .build())
                    .thenCompose(result -> {
                        if (result.getCode() == BatchDeleteReply.Code.OK) {
                            List<CompletableFuture<UnmatchResult>> unmatchFutures =
                                result.getTopicFiltersList().stream()
                                    .map(topicFilter -> unmatch(System.nanoTime(), scopedInbox.tenantId(),
                                        scopedInbox.inboxId(), scopedInbox.incarnation(), topicFilter)).toList();
                            return CompletableFuture.allOf(unmatchFutures.toArray(CompletableFuture[]::new));
                        }
                        return CompletableFuture.completedFuture(null);
                    });
            }
        }

        private CompletableFuture<RetainReply.Result> retain(long reqId, LWT lwt, ClientInfo publisher) {
            if (!resourceThrottler.hasResource(publisher.getTenantId(), TotalRetainTopics)) {
                eventCollector.report(getLocal(ResourceThrottled.class)
                    .reason(TotalRetainTopics.name())
                    .clientInfo(publisher));
                return CompletableFuture.completedFuture(RetainReply.Result.EXCEED_LIMIT);
            }
            if (!resourceThrottler.hasResource(publisher.getTenantId(), TotalRetainMessageSpaceBytes)) {
                eventCollector.report(getLocal(ResourceThrottled.class)
                    .reason(TotalRetainMessageSpaceBytes.name())
                    .clientInfo(publisher));
                return CompletableFuture.completedFuture(RetainReply.Result.EXCEED_LIMIT);
            }

            return retainClient.retain(reqId, lwt.getTopic(),
                lwt.getMessage().getPubQoS(),
                lwt.getMessage().getPayload(),
                lwt.getMessage().getExpiryInterval(),
                publisher).thenApply(RetainReply::getResult);
        }
    }
}
