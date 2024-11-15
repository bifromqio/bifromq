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

package com.baidu.bifromq.basekv.client;

import static com.baidu.bifromq.basekv.RPCBluePrint.toScopedFullMethodName;
import static com.baidu.bifromq.basekv.RPCServerMetadataUtil.RPC_METADATA_STORE_ID;
import static com.baidu.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getBootstrapMethod;
import static com.baidu.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getChangeReplicaConfigMethod;
import static com.baidu.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getExecuteMethod;
import static com.baidu.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getLinearizedQueryMethod;
import static com.baidu.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getMergeMethod;
import static com.baidu.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getQueryMethod;
import static com.baidu.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getRecoverMethod;
import static com.baidu.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getSplitMethod;
import static com.baidu.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getTransferLeadershipMethod;
import static com.baidu.bifromq.basekv.utils.DescriptorUtil.getEffectiveEpoch;
import static com.baidu.bifromq.basekv.utils.DescriptorUtil.toLeaderRanges;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptyNavigableMap;

import com.baidu.bifromq.basekv.metaservice.IBaseKVClusterMetadataManager;
import com.baidu.bifromq.basekv.metaservice.IBaseKVMetaService;
import com.baidu.bifromq.basekv.RPCBluePrint;
import com.baidu.bifromq.basekv.exception.BaseKVException;
import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus;
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
import com.baidu.bifromq.basekv.utils.DescriptorUtil;
import com.baidu.bifromq.basekv.utils.KeySpaceDAG;
import com.baidu.bifromq.baserpc.BluePrint;
import com.baidu.bifromq.baserpc.client.IRPCClient;
import com.baidu.bifromq.baserpc.client.exception.ServerNotFoundException;
import com.baidu.bifromq.logger.SiftLogger;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.grpc.MethodDescriptor;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import org.slf4j.Logger;

final class BaseKVStoreClient implements IBaseKVStoreClient {
    private record ClusterInfo(Map<String, KVRangeStoreDescriptor> storeDescriptors,
                               Map<String, String> serverToStoreMap) {
    }

    private final Logger log;
    private final String clusterId;
    private final IRPCClient rpcClient;
    private final IBaseKVMetaService metaService;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final int queryPipelinesPerStore;
    private final IBaseKVClusterMetadataManager metadataManager;
    private final MethodDescriptor<BootstrapRequest, BootstrapReply> bootstrapMethod;
    private final MethodDescriptor<RecoverRequest, RecoverReply> recoverMethod;
    private final MethodDescriptor<TransferLeadershipRequest, TransferLeadershipReply> transferLeadershipMethod;
    private final MethodDescriptor<ChangeReplicaConfigRequest, ChangeReplicaConfigReply> changeReplicaConfigMethod;
    private final MethodDescriptor<KVRangeSplitRequest, KVRangeSplitReply> splitMethod;
    private final MethodDescriptor<KVRangeMergeRequest, KVRangeMergeReply> mergeMethod;
    private final MethodDescriptor<KVRangeRWRequest, KVRangeRWReply> executeMethod;
    private final MethodDescriptor<KVRangeRORequest, KVRangeROReply> linearizedQueryMethod;
    private final MethodDescriptor<KVRangeRORequest, KVRangeROReply> queryMethod;
    private final Subject<Map<String, String>> storeToServerSubject = BehaviorSubject.createDefault(Maps.newHashMap());
    private final Observable<ClusterInfo> clusterInfoObservable;
    private final BehaviorSubject<NavigableMap<Boundary, KVRangeSetting>> effectiveRouterSubject =
        BehaviorSubject.createDefault(emptyNavigableMap());

    // key: serverId, val: storeId
    private volatile Map<String, String> serverToStoreMap = Maps.newHashMap();
    // key: storeId, value serverId
    private volatile Map<String, String> storeToServerMap = Maps.newHashMap();
    // key: storeId, subKey: KVRangeId
    private volatile Map<String, Map<KVRangeId, IMutationPipeline>> mutPplns = Maps.newHashMap();
    // key: storeId
    private volatile Map<String, List<IQueryPipeline>> queryPplns = Maps.newHashMap();
    // key: storeId
    private volatile Map<String, List<IQueryPipeline>> lnrQueryPplns = Maps.newHashMap();

    BaseKVStoreClient(BaseKVStoreClientBuilder builder) {
        this.clusterId = builder.clusterId;
        log = SiftLogger.getLogger(BaseKVStoreClient.class, "clusterId", clusterId);
        BluePrint bluePrint = RPCBluePrint.build(clusterId);
        this.bootstrapMethod = bluePrint.methodDesc(
            toScopedFullMethodName(clusterId, getBootstrapMethod().getFullMethodName()));
        this.recoverMethod =
            bluePrint.methodDesc(toScopedFullMethodName(clusterId, getRecoverMethod().getFullMethodName()));
        this.transferLeadershipMethod =
            bluePrint.methodDesc(toScopedFullMethodName(clusterId, getTransferLeadershipMethod().getFullMethodName()));
        this.changeReplicaConfigMethod =
            bluePrint.methodDesc(toScopedFullMethodName(clusterId, getChangeReplicaConfigMethod().getFullMethodName()));
        this.splitMethod =
            bluePrint.methodDesc(toScopedFullMethodName(clusterId, getSplitMethod().getFullMethodName()));
        this.mergeMethod =
            bluePrint.methodDesc(toScopedFullMethodName(clusterId, getMergeMethod().getFullMethodName()));
        this.executeMethod =
            bluePrint.methodDesc(toScopedFullMethodName(clusterId, getExecuteMethod().getFullMethodName()));
        this.linearizedQueryMethod =
            bluePrint.methodDesc(toScopedFullMethodName(clusterId, getLinearizedQueryMethod().getFullMethodName()));
        this.queryMethod =
            bluePrint.methodDesc(toScopedFullMethodName(clusterId, getQueryMethod().getFullMethodName()));
        this.metaService = builder.metaService;
        this.queryPipelinesPerStore = builder.queryPipelinesPerStore <= 0 ? 5 : builder.queryPipelinesPerStore;
        this.rpcClient = IRPCClient.newBuilder()
            .trafficService(builder.trafficService)
            .bluePrint(bluePrint)
            .executor(builder.executor)
            .eventLoopGroup(builder.eventLoopGroup)
            .sslContext(builder.sslContext)
            .idleTimeoutInSec(builder.idleTimeoutInSec)
            .keepAliveInSec(builder.keepAliveInSec)
            .build();
        metadataManager = metaService.metadataManager(clusterId);
        clusterInfoObservable = Observable.combineLatest(
                metadataManager.landscape(),
                rpcClient.serverList()
                    .map(servers -> Maps.transformValues(servers, metadata -> metadata.get(RPC_METADATA_STORE_ID))),
                ClusterInfo::new)
            .filter(clusterInfo -> {
                boolean complete = Sets.newHashSet(clusterInfo.serverToStoreMap.values())
                    .equals(clusterInfo.storeDescriptors.keySet());
                if (!complete) {
                    log.debug("Incomplete cluster[{}] info filtered: storeDescriptors={}, serverToStoreMap={}",
                        clusterId, clusterInfo.storeDescriptors, clusterInfo.serverToStoreMap);
                }
                return complete;
            });
        disposables.add(clusterInfoObservable.subscribe(this::refresh));
        disposables.add(effectiveRouterSubject.subscribe(router -> {
            if (!router.isEmpty()) {
                synchronized (this) {
                    this.notifyAll();
                }
            }
        }));
    }

    @Override
    public Observable<IRPCClient.ConnState> connState() {
        return rpcClient.connState();
    }

    @Override
    public String clusterId() {
        return clusterId;
    }

    @Override
    public Observable<Set<KVRangeStoreDescriptor>> describe() {
        return clusterInfoObservable.map(clusterInfo -> Sets.newHashSet(clusterInfo.storeDescriptors.values()));
    }

    @Override
    public Observable<NavigableMap<Boundary, KVRangeSetting>> effectiveRouter() {
        return effectiveRouterSubject.distinctUntilChanged();
    }

    @Override
    public NavigableMap<Boundary, KVRangeSetting> latestEffectiveRouter() {
        return effectiveRouterSubject.getValue();
    }

    @Override
    public CompletableFuture<BootstrapReply> bootstrap(String storeId, BootstrapRequest request) {
        String serverId = storeToServerMap.get(storeId);
        if (serverId == null) {
            return CompletableFuture.failedFuture(
                new ServerNotFoundException("BaseKVStore Server not available for storeId: " + storeId));
        }
        return rpcClient.invoke("", serverId, request, bootstrapMethod);
    }

    @Override
    public CompletableFuture<RecoverReply> recover(String storeId, RecoverRequest request) {
        String serverId = storeToServerMap.get(storeId);
        if (serverId == null) {
            return CompletableFuture.failedFuture(
                new ServerNotFoundException("BaseKVStore Server not available for storeId: " + storeId));
        }
        return rpcClient.invoke("", serverId, request, recoverMethod);
    }

    @Override
    public CompletableFuture<TransferLeadershipReply> transferLeadership(String storeId,
                                                                         TransferLeadershipRequest request) {
        String serverId = storeToServerMap.get(storeId);
        if (serverId == null) {
            return CompletableFuture.failedFuture(
                new ServerNotFoundException("BaseKVStore Server not available for storeId: " + storeId));
        }
        return rpcClient.invoke("", serverId, request, transferLeadershipMethod)
            .exceptionally(e -> {
                log.error("Failed to transfer leader", e);
                return TransferLeadershipReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(ReplyCode.InternalError)
                    .build();
            });
    }

    @Override
    public CompletableFuture<ChangeReplicaConfigReply> changeReplicaConfig(String storeId,
                                                                           ChangeReplicaConfigRequest request) {
        String serverId = storeToServerMap.get(storeId);
        if (serverId == null) {
            return CompletableFuture.failedFuture(
                new ServerNotFoundException("BaseKVStore Server not available for storeId: " + storeId));
        }
        return rpcClient.invoke("", serverId, request, changeReplicaConfigMethod)
            .exceptionally(e -> {
                log.error("Failed to change config", e);
                return ChangeReplicaConfigReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(ReplyCode.InternalError)
                    .build();
            });
    }

    @Override
    public CompletableFuture<KVRangeSplitReply> splitRange(String storeId, KVRangeSplitRequest request) {
        String serverId = storeToServerMap.get(storeId);
        if (serverId == null) {
            return CompletableFuture.failedFuture(
                new ServerNotFoundException("BaseKVStore Server not available for storeId: " + storeId));
        }
        return rpcClient.invoke("", serverId, request, splitMethod)
            .exceptionally(e -> {
                log.error("Failed to split", e);
                return KVRangeSplitReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(ReplyCode.InternalError)
                    .build();
            });
    }

    @Override
    public CompletableFuture<KVRangeMergeReply> mergeRanges(String storeId, KVRangeMergeRequest request) {
        String serverId = storeToServerMap.get(storeId);
        if (serverId == null) {
            return CompletableFuture.failedFuture(
                new ServerNotFoundException("BaseKVStore Server not available for storeId: " + storeId));
        }
        return rpcClient.invoke("", serverId, request, mergeMethod)
            .exceptionally(e -> {
                log.error("Failed to merge", e);
                return KVRangeMergeReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(ReplyCode.InternalError)
                    .build();
            });
    }

    @Override
    public CompletableFuture<KVRangeRWReply> execute(String storeId, KVRangeRWRequest request) {
        return execute(storeId, request, String.valueOf(Thread.currentThread().getId()));
    }

    @Override
    public CompletableFuture<KVRangeRWReply> execute(String storeId, KVRangeRWRequest request, String orderKey) {
        IMutationPipeline mutPpln = mutPplns.getOrDefault(storeId, emptyMap()).get(request.getKvRangeId());
        if (mutPpln == null) {
            return CompletableFuture.failedFuture(BaseKVException.serverNotFound());
        }
        return mutPpln.execute(request);
    }

    @Override
    public CompletableFuture<KVRangeROReply> query(String storeId, KVRangeRORequest request) {
        return query(storeId, request, String.valueOf(Thread.currentThread().getId()));
    }

    @Override
    public CompletableFuture<KVRangeROReply> query(String storeId, KVRangeRORequest request, String orderKey) {
        List<IQueryPipeline> pipelines = queryPplns.get(storeId);
        if (pipelines == null) {
            return CompletableFuture.failedFuture(BaseKVException.serverNotFound());
        }
        return pipelines.get((orderKey.hashCode() % pipelines.size() + pipelines.size()) % pipelines.size())
            .query(request);
    }

    @Override
    public CompletableFuture<KVRangeROReply> linearizedQuery(String storeId, KVRangeRORequest request) {
        return linearizedQuery(storeId, request, String.valueOf(Thread.currentThread().getId()));
    }

    @Override
    public CompletableFuture<KVRangeROReply> linearizedQuery(String storeId, KVRangeRORequest request,
                                                             String orderKey) {
        List<IQueryPipeline> pipelines = lnrQueryPplns.get(storeId);
        if (pipelines == null) {
            return CompletableFuture.failedFuture(BaseKVException.serverNotFound());
        }
        return pipelines.get((orderKey.hashCode() % pipelines.size() + pipelines.size()) % pipelines.size())
            .query(request);
    }

    @Override
    public IMutationPipeline createMutationPipeline(String storeId) {
        return new ManagedMutationPipeline(storeToServerSubject
            .map(m -> Optional.ofNullable(m.get(storeId)))
            .distinctUntilChanged()
            .map(serverIdOpt -> {
                if (serverIdOpt.isEmpty()) {
                    return new IRPCClient.IRequestPipeline<>() {
                        @Override
                        public boolean isClosed() {
                            return false;
                        }

                        @Override
                        public CompletableFuture<KVRangeRWReply> invoke(KVRangeRWRequest req) {
                            return CompletableFuture.failedFuture(
                                new ServerNotFoundException("No hosting server found for store: " + storeId));
                        }

                        @Override
                        public void close() {

                        }
                    };
                }
                return rpcClient.createRequestPipeline("", serverIdOpt.get(), null, emptyMap(), executeMethod);
            }));
    }

    @Override
    public IQueryPipeline createQueryPipeline(String storeId) {
        return createQueryPipeline(storeId, false);
    }

    @Override
    public IQueryPipeline createLinearizedQueryPipeline(String storeId) {
        return createQueryPipeline(storeId, true);
    }

    private IQueryPipeline createQueryPipeline(String storeId, boolean linearized) {
        return new ManagedQueryPipeline(storeToServerSubject
            .map(m -> Optional.ofNullable(m.get(storeId)))
            .distinctUntilChanged()
            .map(serverIdOpt -> {
                if (serverIdOpt.isEmpty()) {
                    return new IRPCClient.IRequestPipeline<>() {
                        @Override
                        public boolean isClosed() {
                            return false;
                        }

                        @Override
                        public CompletableFuture<KVRangeROReply> invoke(KVRangeRORequest req) {
                            return CompletableFuture.failedFuture(
                                new ServerNotFoundException("No hosting server found for store: " + storeId));
                        }

                        @Override
                        public void close() {

                        }
                    };
                }
                if (linearized) {
                    return rpcClient.createRequestPipeline("", serverIdOpt.get(), null, emptyMap(),
                        linearizedQueryMethod);
                } else {
                    return rpcClient.createRequestPipeline("", serverIdOpt.get(), null, emptyMap(), queryMethod);
                }
            }));
    }

    @Override
    public void join() {
        // wait for router covering full range
        if (effectiveRouterSubject.getValue() == null || effectiveRouterSubject.getValue().isEmpty()) {
            synchronized (this) {
                try {
                    if (effectiveRouterSubject.getValue().isEmpty()) {
                        this.wait();
                    }
                } catch (InterruptedException e) {
                    // nothing to do
                }
            }
        }
        rpcClient.connState()
            .filter(connState -> connState == IRPCClient.ConnState.READY || connState == IRPCClient.ConnState.SHUTDOWN)
            .blockingFirst();
    }

    @Override
    public void stop() {
        if (closed.compareAndSet(false, true)) {
            log.info("Stopping BaseKVStore client: cluster[{}]", clusterId);
            disposables.dispose();
            log.debug("Closing execution pipelines: cluster[{}]", clusterId);
            mutPplns.values().forEach(pplns -> pplns.values().forEach(IMutationPipeline::close));
            log.debug("Closing query pipelines: cluster[{}]", clusterId);
            queryPplns.values().forEach(pplns -> pplns.forEach(IQueryPipeline::close));
            log.debug("Closing linearizable query pipelines: cluster[{}]", clusterId);
            lnrQueryPplns.values().forEach(pplns -> pplns.forEach(IQueryPipeline::close));
            log.debug("Stopping rpc client: cluster[{}]", clusterId);
            rpcClient.stop();
            log.info("BaseKVStore client stopped: cluster[{}]", clusterId);
        }
    }

    private void refresh(ClusterInfo clusterInfo) {
        log.debug("Cluster[{}] info update\n{}", clusterId, clusterInfo);
        boolean rangeRouteUpdated = refreshRangeRoute(clusterInfo);
        boolean storeRouteUpdated = refreshStoreRoute(clusterInfo);
        if (storeRouteUpdated) {
            refreshQueryPipelines(clusterInfo.storeDescriptors.keySet());
        }
        if (rangeRouteUpdated || storeRouteUpdated) {
            refreshMutPipelines(clusterInfo.storeDescriptors);
        }
    }

    private boolean refreshRangeRoute(ClusterInfo clusterInfo) {
        Optional<DescriptorUtil.EffectiveEpoch> effectiveEpoch =
            getEffectiveEpoch(Sets.newHashSet(clusterInfo.storeDescriptors.values()));
        if (effectiveEpoch.isEmpty()) {
            return false;
        }
        Map<String, Map<KVRangeId, KVRangeDescriptor>> leaderRanges =
            toLeaderRanges(effectiveEpoch.get().storeDescriptors());
        KeySpaceDAG dag = new KeySpaceDAG(leaderRanges);
        NavigableMap<Boundary, KVRangeSetting> router = Maps.transformValues(dag.getEffectiveFullCoveredRoute(),
            leaderRange -> new KVRangeSetting(clusterId, leaderRange.storeId(), leaderRange.descriptor()));
        if (router.isEmpty()) {
            return false;
        }
        NavigableMap<Boundary, KVRangeSetting> last = effectiveRouterSubject.getValue();
        effectiveRouterSubject.onNext(router);
        return !router.equals(last);
    }

    private boolean refreshStoreRoute(ClusterInfo clusterInfo) {
        Map<String, String> oldServerToStoreMap = serverToStoreMap;
        if (clusterInfo.serverToStoreMap.equals(oldServerToStoreMap)) {
            return false;
        }
        Map<String, String> newStoreToServerMap = new HashMap<>();
        clusterInfo.serverToStoreMap.forEach((server, store) -> newStoreToServerMap.put(store, server));
        serverToStoreMap = clusterInfo.serverToStoreMap;
        storeToServerMap = newStoreToServerMap;
        storeToServerSubject.onNext(newStoreToServerMap);
        return true;
    }

    private void refreshMutPipelines(Map<String, KVRangeStoreDescriptor> storeDescriptors) {
        Map<String, Map<KVRangeId, IMutationPipeline>> nextMutPplns = new HashMap<>();
        Map<String, Map<KVRangeId, IMutationPipeline>> currentMutPplns = mutPplns;

        for (KVRangeStoreDescriptor storeDescriptor : storeDescriptors.values()) {
            String storeId = storeDescriptor.getId();
            for (KVRangeDescriptor rangeDescriptor : storeDescriptor.getRangesList()) {
                if (rangeDescriptor.getRole() != RaftNodeStatus.Leader) {
                    continue;
                }
                KVRangeId rangeId = rangeDescriptor.getId();
                Map<KVRangeId, IMutationPipeline> currentRanges =
                    currentMutPplns.getOrDefault(storeId, Collections.emptyMap());
                IMutationPipeline existingPpln = currentRanges.get(rangeId);
                if (existingPpln != null) {
                    nextMutPplns.computeIfAbsent(storeId, k -> new HashMap<>()).put(rangeId, existingPpln);
                } else {
                    nextMutPplns.computeIfAbsent(storeId, k -> new HashMap<>())
                        .put(rangeId, createMutationPipeline(storeId));
                }
            }
        }
        mutPplns = nextMutPplns;
        // clear mut pipelines targeting non-exist storeId;
        for (String storeId : Sets.difference(currentMutPplns.keySet(), nextMutPplns.keySet())) {
            currentMutPplns.get(storeId).values().forEach(IMutationPipeline::close);
        }
    }

    private void refreshQueryPipelines(Set<String> allStoreIds) {
        Map<String, List<IQueryPipeline>> nextQueryPplns = new HashMap<>();
        Map<String, List<IQueryPipeline>> currentQueryPplns = queryPplns;
        for (String storeId : allStoreIds) {
            if (currentQueryPplns.containsKey(storeId)) {
                nextQueryPplns.put(storeId, currentQueryPplns.get(storeId));
            } else {
                List<IQueryPipeline> queryPipelines = new ArrayList<>(queryPipelinesPerStore);
                IntStream.range(0, queryPipelinesPerStore)
                    .forEach(i -> queryPipelines.add(createQueryPipeline(storeId)));
                nextQueryPplns.put(storeId, queryPipelines);
            }
        }
        queryPplns = nextQueryPplns;
        // clear query pipelines targeting non-exist storeId;
        for (String storeId : Sets.difference(currentQueryPplns.keySet(), allStoreIds)) {
            currentQueryPplns.get(storeId).forEach(IQueryPipeline::close);
        }
    }
}
