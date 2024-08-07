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

package com.baidu.bifromq.basekv.server;

import static com.baidu.bifromq.baserpc.UnaryResponse.response;

import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basekv.store.IKVRangeStore;
import com.baidu.bifromq.basekv.store.IKVRangeStoreDescriptorReporter;
import com.baidu.bifromq.basekv.store.KVRangeStore;
import com.baidu.bifromq.basekv.store.KVRangeStoreDescriptorReporter;
import com.baidu.bifromq.basekv.store.exception.KVRangeException.BadRequest;
import com.baidu.bifromq.basekv.store.exception.KVRangeException.BadVersion;
import com.baidu.bifromq.basekv.store.exception.KVRangeException.TryLater;
import com.baidu.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc;
import com.baidu.bifromq.basekv.store.proto.BootstrapReply;
import com.baidu.bifromq.basekv.store.proto.BootstrapRequest;
import com.baidu.bifromq.basekv.store.proto.ChangeReplicaConfigReply;
import com.baidu.bifromq.basekv.store.proto.ChangeReplicaConfigRequest;
import com.baidu.bifromq.basekv.store.proto.KVRangeMergeReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeMergeRequest;
import com.baidu.bifromq.basekv.store.proto.KVRangeROReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.store.proto.KVRangeSplitReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeSplitRequest;
import com.baidu.bifromq.basekv.store.proto.RecoverReply;
import com.baidu.bifromq.basekv.store.proto.RecoverRequest;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basekv.store.proto.TransferLeadershipReply;
import com.baidu.bifromq.basekv.store.proto.TransferLeadershipRequest;
import com.baidu.bifromq.logger.SiftLogger;
import com.google.common.collect.Sets;
import io.grpc.stub.StreamObserver;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;

class BaseKVStoreService extends BaseKVStoreServiceGrpc.BaseKVStoreServiceImplBase {
    private static final int DEAD_STORE_CLEANUP_TIME_FACTOR = 10;
    private final Logger log;
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final IKVRangeStore kvRangeStore;
    private final IKVRangeStoreDescriptorReporter storeDescriptorReporter;
    private final IAgentHost agentHost;
    private final String clusterId;
    private final boolean bootstrap;

    BaseKVStoreService(BaseKVStoreServiceBuilder<?> builder) {
        kvRangeStore = new KVRangeStore(
            builder.clusterId,
            builder.storeOptions,
            builder.coProcFactory,
            builder.queryExecutor,
            builder.tickerThreads,
            builder.bgTaskExecutor);
        this.bootstrap = builder.bootstrap;
        this.clusterId = builder.clusterId;
        this.agentHost = builder.agentHost;
        log = SiftLogger.getLogger(BaseKVStoreService.class, "clusterId", clusterId, "storeId", kvRangeStore.id());
        storeDescriptorReporter = new KVRangeStoreDescriptorReporter(clusterId, kvRangeStore.id(),
            builder.serverBuilder.crdtService,
            Duration.ofSeconds(builder.storeOptions.getStatsCollectIntervalSec()).toMillis()
                * DEAD_STORE_CLEANUP_TIME_FACTOR);
    }

    public String clusterId() {
        return clusterId;
    }

    public String storeId() {
        return kvRangeStore.id();
    }

    public void start() {
        log.debug("Starting BaseKVStore service: bootstrap={}", bootstrap);
        kvRangeStore.start(new AgentHostStoreMessenger(agentHost, clusterId, kvRangeStore.id()));
        if (bootstrap) {
            kvRangeStore.bootstrap();
        }
        // sync store descriptor via crdt
        disposables.add(kvRangeStore.describe().subscribe(storeDescriptorReporter::report));
        storeDescriptorReporter.start();
        log.debug("BaseKVStore service started");
    }

    public void stop() {
        log.debug("Stopping BaseKVStore service");
        disposables.dispose();
        kvRangeStore.stop();
        storeDescriptorReporter.stop();
        log.debug("BaseKVStore service stopped");
    }

    @Override
    public void bootstrap(BootstrapRequest request, StreamObserver<BootstrapReply> responseObserver) {
        response(tenantId -> {
            try {
                boolean ret = kvRangeStore.bootstrap();
                if (ret) {
                    return CompletableFuture.completedFuture(BootstrapReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setResult(BootstrapReply.Result.Ok)
                        .build());
                } else {
                    return CompletableFuture.completedFuture(BootstrapReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setResult(BootstrapReply.Result.NotEmpty)
                        .build());
                }
            } catch (IllegalStateException e) {
                return CompletableFuture.completedFuture(BootstrapReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(BootstrapReply.Result.NotRunning)
                    .build());
            }
        }, responseObserver);
    }

    @Override
    public void recover(RecoverRequest request, StreamObserver<RecoverReply> responseObserver) {
        response(tenantId -> kvRangeStore.recover()
            .handle((v, e) -> RecoverReply.newBuilder().setReqId(request.getReqId()).build()), responseObserver);
    }

    @Override
    public void transferLeadership(TransferLeadershipRequest request,
                                   StreamObserver<TransferLeadershipReply> responseObserver) {
        response(tenantId -> kvRangeStore.transferLeadership(request.getVer(), request.getKvRangeId(),
                request.getNewLeaderStore())
            .thenApply(result -> TransferLeadershipReply.newBuilder()
                .setReqId(request.getReqId())
                .setCode(ReplyCode.Ok)
                .build())
            .exceptionally(e -> TransferLeadershipReply.newBuilder()
                .setReqId(request.getReqId())
                .setCode(convertKVRangeException(e))
                .build()), responseObserver);
    }

    @Override
    public void changeReplicaConfig(ChangeReplicaConfigRequest request,
                                    StreamObserver<ChangeReplicaConfigReply> responseObserver) {
        response(tenantId -> kvRangeStore.changeReplicaConfig(request.getVer(), request.getKvRangeId(),
                Sets.newHashSet(request.getNewVotersList()),
                Sets.newHashSet(request.getNewLearnersList()))
            .thenApply(result -> ChangeReplicaConfigReply.newBuilder()
                .setReqId(request.getReqId())
                .setCode(ReplyCode.Ok)
                .build())
            .exceptionally(e -> ChangeReplicaConfigReply.newBuilder()
                .setReqId(request.getReqId())
                .setCode(convertKVRangeException(e))
                .build()), responseObserver);
    }

    @Override
    public void split(KVRangeSplitRequest request, StreamObserver<KVRangeSplitReply> responseObserver) {
        response(tenantId -> kvRangeStore.split(request.getVer(), request.getKvRangeId(), request.getSplitKey())
                .thenApply(result -> KVRangeSplitReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(ReplyCode.Ok)
                    .build())
                .exceptionally(e -> KVRangeSplitReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(convertKVRangeException(e))
                    .build()),
            responseObserver);
    }

    @Override
    public void merge(KVRangeMergeRequest request, StreamObserver<KVRangeMergeReply> responseObserver) {
        response(tenantId -> kvRangeStore.merge(request.getVer(), request.getMergerId(), request.getMergeeId())
            .thenApply(result -> KVRangeMergeReply.newBuilder()
                .setReqId(request.getReqId())
                .setCode(ReplyCode.Ok)
                .build())
            .exceptionally(e -> KVRangeMergeReply.newBuilder()
                .setReqId(request.getReqId())
                .setCode(convertKVRangeException(e))
                .build()), responseObserver);
    }

    @Override
    public StreamObserver<KVRangeRWRequest> execute(StreamObserver<KVRangeRWReply> responseObserver) {
        return new MutatePipeline(kvRangeStore, responseObserver);
    }

    @Override
    public StreamObserver<KVRangeRORequest> query(StreamObserver<KVRangeROReply> responseObserver) {
        return new QueryPipeline(kvRangeStore, false, responseObserver);
    }

    @Override
    public StreamObserver<KVRangeRORequest> linearizedQuery(StreamObserver<KVRangeROReply> responseObserver) {
        return new QueryPipeline(kvRangeStore, true, responseObserver);
    }

    private ReplyCode convertKVRangeException(Throwable e) {
        if (e instanceof BadVersion || e.getCause() instanceof BadVersion) {
            return ReplyCode.BadVersion;
        }
        if (e instanceof TryLater || e.getCause() instanceof TryLater) {
            return ReplyCode.TryLater;
        }
        if (e instanceof BadRequest || e.getCause() instanceof BadRequest) {
            return ReplyCode.BadRequest;
        }
        log.error("Internal Error", e);
        return ReplyCode.InternalError;
    }
}
