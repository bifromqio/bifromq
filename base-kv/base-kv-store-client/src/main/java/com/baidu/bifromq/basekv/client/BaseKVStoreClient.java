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
import static com.baidu.bifromq.basekv.store.CRDTUtil.getDescriptorFromCRDT;
import static com.baidu.bifromq.basekv.store.CRDTUtil.storeDescriptorMapCRDTURI;
import static com.baidu.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getBootstrapMethod;
import static com.baidu.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getChangeReplicaConfigMethod;
import static com.baidu.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getExecuteMethod;
import static com.baidu.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getLinearizedQueryMethod;
import static com.baidu.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getMergeMethod;
import static com.baidu.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getQueryMethod;
import static com.baidu.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getRecoverMethod;
import static com.baidu.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getSplitMethod;
import static com.baidu.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getTransferLeadershipMethod;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static java.util.Collections.emptyMap;

import com.baidu.bifromq.basecrdt.core.api.IORMap;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.basekv.RPCBluePrint;
import com.baidu.bifromq.basekv.exception.BaseKVException;
import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
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
import com.baidu.bifromq.baserpc.BluePrint;
import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.baserpc.exception.ServerNotFoundException;
import com.baidu.bifromq.baserpc.utils.BehaviorSubject;
import com.baidu.bifromq.logger.SiftLogger;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import io.grpc.MethodDescriptor;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.subjects.Subject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;

final class BaseKVStoreClient implements IBaseKVStoreClient {
    private record ClusterInfo(Map<String, KVRangeStoreDescriptor> storeDescriptors,
                               Map<String, String> serverToStoreMap) {
    }

    private final Logger log;
    private final String clusterId;
    private final IRPCClient rpcClient;
    private final ICRDTService crdtService;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final KVRangeRouter rangeRouter;
    private final int queryPipelinesPerStore;
    private final IORMap storeDescriptorCRDT;
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
        rangeRouter = new KVRangeRouter(clusterId);
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
        this.crdtService = builder.crdtService;
        this.queryPipelinesPerStore = builder.queryPipelinesPerStore <= 0 ? 5 : builder.queryPipelinesPerStore;
        this.rpcClient = IRPCClient.newBuilder()
            .bluePrint(bluePrint)
            .executor(builder.executor)
            .eventLoopGroup(builder.eventLoopGroup)
            .sslContext(builder.sslContext)
            .idleTimeoutInSec(builder.idleTimeoutInSec)
            .keepAliveInSec(builder.keepAliveInSec)
            .crdtService(crdtService)
            .build();
        crdtService.host(storeDescriptorMapCRDTURI(clusterId));
        Optional<IORMap> crdtOpt = crdtService.get(storeDescriptorMapCRDTURI(clusterId));
        assert crdtOpt.isPresent();
        storeDescriptorCRDT = crdtOpt.get();
        clusterInfoObservable = Observable.combineLatest(
                storeDescriptorCRDT.inflation().map(this::currentStoreDescriptors),
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
    public Optional<KVRangeSetting> findByKey(ByteString key) {
        return rangeRouter.findByKey(key);
    }

    @Override
    public List<KVRangeSetting> findByBoundary(Boundary boundary) {
        return rangeRouter.findByBoundary(boundary);
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
        return new ManagedMutationPipeline(storeToServerSubject.map(m -> {
            String serverId = m.get(storeId);
            if (serverId == null) {
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
            return rpcClient.createRequestPipeline("", serverId, null, emptyMap(), executeMethod);
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
        return new ManagedQueryPipeline(storeToServerSubject.map(m -> {
            String serverId = m.get(storeId);
            if (serverId == null) {
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
                return rpcClient.createRequestPipeline("", serverId, null, emptyMap(), linearizedQueryMethod);
            } else {
                return rpcClient.createRequestPipeline("", serverId, null, emptyMap(), queryMethod);
            }
        }));
    }

    @Override
    public void join() {
        // wait for router covering full range
        if (!rangeRouter.isFullRangeCovered()) {
            synchronized (this) {
                try {
                    if (!rangeRouter.isFullRangeCovered()) {
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
            log.debug("Stopping hosting crdt: cluster[{}]", clusterId);
            crdtService.stopHosting(storeDescriptorMapCRDTURI(clusterId)).join();
            log.debug("Stopping rpc client: cluster[{}]", clusterId);
            rpcClient.stop();
            log.info("BaseKVStore client stopped: cluster[{}]", clusterId);
        }
    }

    private Map<String, KVRangeStoreDescriptor> currentStoreDescriptors(long ts) {
        log.trace("StoreDescriptor CRDT updated at {}: clusterId={}\n{}", ts, clusterId, storeDescriptorCRDT);
        Iterator<ByteString> keyItr = Iterators.transform(storeDescriptorCRDT.keys(), IORMap.ORMapKey::key);

        Map<String, KVRangeStoreDescriptor> storeDescriptors = new HashMap<>();
        while (keyItr.hasNext()) {
            Optional<KVRangeStoreDescriptor> storeDescOpt = getDescriptorFromCRDT(storeDescriptorCRDT, keyItr.next());
            storeDescOpt.ifPresent(storeDesc -> storeDescriptors.put(storeDesc.getId(), storeDesc));
        }
        return storeDescriptors;
    }

    private void refresh(ClusterInfo clusterInfo) {
        log.debug("Cluster[{}] info update\n{}", clusterId, clusterInfo);
        boolean rangeRouteUpdated = refreshRangeRoute(clusterInfo);
        boolean storeRouteUpdated = refreshStoreRoute(clusterInfo);
        if (storeRouteUpdated) {
            refreshQueryPipelines(storeToServerMap);
        }
        if (rangeRouteUpdated || storeRouteUpdated) {
            refreshMutPipelines(storeToServerMap, clusterInfo.storeDescriptors);
        }
        if (rangeRouteUpdated) {
            if (rangeRouter.isFullRangeCovered()) {
                synchronized (this) {
                    this.notifyAll();
                }
            }
        }
    }

    private boolean refreshRangeRoute(ClusterInfo clusterInfo) {
        return rangeRouter.reset(Sets.newHashSet(clusterInfo.storeDescriptors.values()));
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

    private void refreshMutPipelines(Map<String, String> newStoreToServerMap,
                                     Map<String, KVRangeStoreDescriptor> storeDescriptorMap) {
        Map<String, Map<KVRangeId, IMutationPipeline>> nextMutPplns = Maps.newHashMap(mutPplns);
        nextMutPplns.forEach((k, v) -> nextMutPplns.put(k, Maps.newHashMap(v)));
        Set<String> oldStoreIds = Sets.newHashSet(mutPplns.keySet());
        for (String existingStoreId : Sets.intersection(newStoreToServerMap.keySet(), oldStoreIds)) {
            Set<KVRangeId> newRangeIds = storeDescriptorMap.get(existingStoreId).getRangesList().stream()
                .map(KVRangeDescriptor::getId).collect(Collectors.toSet());
            Set<KVRangeId> oldRangeIds = Sets.newHashSet(nextMutPplns.get(existingStoreId).keySet());
            for (KVRangeId newRangeId : Sets.difference(newRangeIds, oldRangeIds)) {
                nextMutPplns.get(existingStoreId)
                    .put(newRangeId, new MutPipeline(newStoreToServerMap.get(existingStoreId)));
            }
            for (KVRangeId deadRangeId : Sets.difference(oldRangeIds, newRangeIds)) {
                nextMutPplns.get(existingStoreId).remove(deadRangeId).close();
            }
        }
        Set<KVRangeId> effectiveKVRangeIds = rangeRouter.findByBoundary(FULL_BOUNDARY).stream()
            .map(m -> m.id).collect(Collectors.toSet());
        for (String newStoreId : Sets.difference(newStoreToServerMap.keySet(), oldStoreIds)) {
            Map<KVRangeId, IMutationPipeline> rangeExecPplns =
                nextMutPplns.computeIfAbsent(newStoreId, k -> new HashMap<>());
            for (KVRangeDescriptor rangeDesc : storeDescriptorMap.get(newStoreId).getRangesList()) {
                if (effectiveKVRangeIds.contains(rangeDesc.getId())) {
                    // only create mutation pipelines for effective ranges
                    rangeExecPplns.put(rangeDesc.getId(), new MutPipeline(newStoreToServerMap.get(newStoreId)));
                }
            }
        }

        for (String deadStoreId : Sets.difference(oldStoreIds, newStoreToServerMap.keySet())) {
            nextMutPplns.remove(deadStoreId).values().forEach(IMutationPipeline::close);
        }
        mutPplns = nextMutPplns;
    }

    private void refreshQueryPipelines(Map<String, String> newStoreToServerMap) {
        if (newStoreToServerMap.keySet().equals(queryPplns.keySet())) {
            return;
        }
        Set<String> oldStoreIds = Sets.newHashSet(queryPplns.keySet());
        // query pipelines
        Map<String, List<IQueryPipeline>> nextQueryPplns = Maps.newHashMap(queryPplns);
        nextQueryPplns.forEach((k, v) -> nextQueryPplns.put(k, Lists.newArrayList(v)));
        // lnr query pipelines
        Map<String, List<IQueryPipeline>> nextLnrQueryPplns = Maps.newHashMap(lnrQueryPplns);
        nextQueryPplns.forEach((k, v) -> nextLnrQueryPplns.put(k, Lists.newArrayList(v)));

        for (String newStoreId : Sets.difference(newStoreToServerMap.keySet(), oldStoreIds)) {
            // setup new query pipelines
            List<IQueryPipeline> storeQueryPplns = nextQueryPplns.computeIfAbsent(newStoreId, k -> new ArrayList<>());
            IntStream.range(0, queryPipelinesPerStore)
                .forEach(i -> storeQueryPplns.add(new QueryPipeline(newStoreToServerMap.get(newStoreId))));

            // setup new linear query pipelines
            List<IQueryPipeline> storeLnrQueryPplns =
                nextLnrQueryPplns.computeIfAbsent(newStoreId, k -> new ArrayList<>());
            IntStream.range(0, queryPipelinesPerStore)
                .forEach(i -> storeLnrQueryPplns.add(new QueryPipeline(newStoreToServerMap.get(newStoreId), true)));
        }
        for (String deadStoreId : Sets.difference(oldStoreIds, newStoreToServerMap.keySet())) {
            // close store pipelines
            nextQueryPplns.remove(deadStoreId).forEach(IQueryPipeline::close);
            nextLnrQueryPplns.remove(deadStoreId).forEach(IQueryPipeline::close);
        }
        queryPplns = nextQueryPplns;
        lnrQueryPplns = nextLnrQueryPplns;
    }

    private class MutPipeline implements IMutationPipeline {
        private final IRPCClient.IRequestPipeline<KVRangeRWRequest, KVRangeRWReply> ppln;

        MutPipeline(String serverId) {
            ppln = rpcClient.createRequestPipeline("", serverId, null, emptyMap(), executeMethod);
        }

        @Override
        public CompletableFuture<KVRangeRWReply> execute(KVRangeRWRequest request) {
            log.trace("Requesting rw range:req={}", request);
            return ppln.invoke(request);
        }

        @Override
        public void close() {
            ppln.close();
        }
    }

    private class QueryPipeline implements IQueryPipeline {
        private final IRPCClient.IRequestPipeline<KVRangeRORequest, KVRangeROReply> ppln;
        private final boolean linearized;

        QueryPipeline(String serverId) {
            this(serverId, false);
        }

        QueryPipeline(String serverId, boolean linearized) {
            this.linearized = linearized;
            if (linearized) {
                ppln = rpcClient.createRequestPipeline("", serverId, null, emptyMap(), linearizedQueryMethod);
            } else {
                ppln = rpcClient.createRequestPipeline("", serverId, null,
                    emptyMap(), queryMethod);
            }
        }

        @Override
        public CompletableFuture<KVRangeROReply> query(KVRangeRORequest request) {
            log.trace("Invoke ro range request: linearized={} \n{}", linearized, request);
            return ppln.invoke(request);
        }

        @Override
        public void close() {
            ppln.close();
        }
    }
}
