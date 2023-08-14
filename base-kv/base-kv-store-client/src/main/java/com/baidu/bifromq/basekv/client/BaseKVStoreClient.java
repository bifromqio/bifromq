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
import static java.util.Collections.emptyMap;

import com.baidu.bifromq.basecrdt.core.api.IORMap;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.basekv.KVRangeRouter;
import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.RPCBluePrint;
import com.baidu.bifromq.basekv.exception.BaseKVException;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.proto.Range;
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
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import io.grpc.MethodDescriptor;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class BaseKVStoreClient implements IBaseKVStoreClient {
    private final String clusterId;
    private final IRPCClient rpcClient;
    private final ICRDTService crdtService;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final KVRangeRouter router = new KVRangeRouter();
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

    // key: serverId, val: storeId
    private volatile Map<String, String> serverToStoreMap = Maps.newHashMap();
    // key: storeId, value serverId
    private volatile Map<String, String> storeToServerMap = Maps.newHashMap();
    // key: storeId, subKey: KVRangeId
    private volatile Map<String, Map<KVRangeId, IExecutionPipeline>> execPplns = Maps.newHashMap();
    // key: storeId
    private volatile Map<String, List<IQueryPipeline>> queryPplns = Maps.newHashMap();
    // key: storeId
    private volatile Map<String, List<IQueryPipeline>> lnrQueryPplns = Maps.newHashMap();

    private record ClusterInfo(Set<KVRangeStoreDescriptor> storeDescriptors, Map<String, String> serverToStoreMap) {
    }

    BaseKVStoreClient(BaseKVStoreClientBuilder builder) {
        this.clusterId = builder.clusterId;
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
        storeDescriptorCRDT = (IORMap) crdtService.get(storeDescriptorMapCRDTURI(clusterId)).get();
        disposables.add(Observable.combineLatest(storeDescriptorCRDT.inflation().map(this::currentStoreDescriptors),
                rpcClient.serverList()
                    .map(servers -> Maps.transformValues(servers, metadata -> metadata.get(RPC_METADATA_STORE_ID))),
                ClusterInfo::new)
            .subscribe(this::refresh));
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
        return storeDescriptorCRDT.inflation().mapOptional(l -> {
            Set<KVRangeStoreDescriptor> descriptors = Sets.newHashSet();
            Iterator<ByteString> keyItr = Iterators.transform(storeDescriptorCRDT.keys(), IORMap.ORMapKey::key);
            while (keyItr.hasNext()) {
                Optional<KVRangeStoreDescriptor> descriptor = getDescriptorFromCRDT(storeDescriptorCRDT, keyItr.next());
                descriptor.ifPresent(descriptors::add);
            }
            return descriptors.isEmpty() ? Optional.empty() : Optional.of(descriptors);
        });
    }

    @Override
    public Optional<KVRangeSetting> findById(KVRangeId id) {
        return router.findById(id);
    }

    @Override
    public Optional<KVRangeSetting> findByKey(ByteString key) {
        return router.findByKey(key);
    }

    @Override
    public List<KVRangeSetting> findByRange(Range range) {
        return router.findByRange(range);
    }

    @Override
    public CompletableFuture<BootstrapReply> bootstrap(String storeId) {
        String serverId = storeToServerMap.get(storeId);
        if (serverId == null) {
            return CompletableFuture.failedFuture(
                new ServerNotFoundException("BaseKVStore Server not available for storeId: " + storeId));
        }
        return rpcClient.invoke("", serverId,
            BootstrapRequest.newBuilder().setReqId(System.nanoTime()).build(), bootstrapMethod);
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
            .exceptionally(e -> TransferLeadershipReply.newBuilder()
                .setReqId(request.getReqId())
                .setCode(ReplyCode.InternalError)
                .build());
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
            .exceptionally(e -> ChangeReplicaConfigReply.newBuilder()
                .setReqId(request.getReqId())
                .setCode(ReplyCode.InternalError)
                .build());
    }

    @Override
    public CompletableFuture<KVRangeSplitReply> splitRange(String storeId, KVRangeSplitRequest request) {
        String serverId = storeToServerMap.get(storeId);
        if (serverId == null) {
            return CompletableFuture.failedFuture(
                new ServerNotFoundException("BaseKVStore Server not available for storeId: " + storeId));
        }
        return rpcClient.invoke("", serverId, request, splitMethod)
            .exceptionally(e -> KVRangeSplitReply.newBuilder()
                .setReqId(request.getReqId())
                .setCode(ReplyCode.InternalError)
                .build());
    }

    @Override
    public CompletableFuture<KVRangeMergeReply> mergeRanges(String storeId, KVRangeMergeRequest request) {
        String serverId = storeToServerMap.get(storeId);
        if (serverId == null) {
            return CompletableFuture.failedFuture(
                new ServerNotFoundException("BaseKVStore Server not available for storeId: " + storeId));
        }
        return rpcClient.invoke("", serverId, request, mergeMethod)
            .exceptionally(e -> KVRangeMergeReply.newBuilder()
                .setReqId(request.getReqId())
                .setCode(ReplyCode.InternalError)
                .build());
    }

    @Override
    public CompletableFuture<KVRangeRWReply> execute(String storeId, KVRangeRWRequest request) {
        return execute(storeId, request, String.valueOf(Thread.currentThread().getId()));
    }

    @Override
    public CompletableFuture<KVRangeRWReply> execute(String storeId, KVRangeRWRequest request, String orderKey) {
        IExecutionPipeline execPpln = execPplns.getOrDefault(storeId, emptyMap()).get(request.getKvRangeId());
        if (execPpln == null) {
            return CompletableFuture.failedFuture(BaseKVException.serverNotFound());
        }
        return execPpln.execute(request);
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
    public IExecutionPipeline createExecutionPipeline(String storeId) {
        String serverId = storeToServerMap.get(storeId);
        if (serverId == null) {
            throw new NullPointerException("No hosting server found for store: " + storeId);
        }
        return new ExecPipeline(serverId);
    }

    @Override
    public IQueryPipeline createQueryPipeline(String storeId) {
        String serverId = storeToServerMap.get(storeId);
        if (serverId == null) {
            throw new NullPointerException("No hosting server found for store: " + storeId);
        }

        return new QueryPipeline(serverId, false);
    }

    @Override
    public IQueryPipeline createLinearizedQueryPipeline(String storeId) {
        String serverId = storeToServerMap.get(storeId);
        if (serverId == null) {
            throw new NullPointerException("No hosting server found for store: " + storeId);
        }
        return new QueryPipeline(serverId, true);
    }

    @Override
    public void join() {
        // wait for router covering full range
        if (!router.isFullRangeCovered()) {
            synchronized (this) {
                try {
                    if (!router.isFullRangeCovered()) {
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
            execPplns.values().forEach(pplns -> pplns.values().forEach(IExecutionPipeline::close));
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

    private Set<KVRangeStoreDescriptor> currentStoreDescriptors(long ts) {
        log.trace("StoreDescriptor updated at {}", ts);
        Iterator<ByteString> keyItr = Iterators.transform(storeDescriptorCRDT.keys(), IORMap.ORMapKey::key);

        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        while (keyItr.hasNext()) {
            Optional<KVRangeStoreDescriptor> storeDescOpt = getDescriptorFromCRDT(storeDescriptorCRDT, keyItr.next());
            storeDescOpt.ifPresent(storeDescriptors::add);
        }
        return storeDescriptors;
    }

    private void refresh(ClusterInfo clusterInfo) {
        boolean rangeRouteUpdated = refreshRangeRoute(clusterInfo);
        boolean storeRouteUpdated = refreshStoreRoute(clusterInfo);
        if (storeRouteUpdated) {
            refreshQueryPipelines(storeToServerMap);
        }
        if (rangeRouteUpdated || storeRouteUpdated) {
            refreshExecPipelines(storeToServerMap);
        }
    }

    private boolean refreshRangeRoute(ClusterInfo clusterInfo) {
        boolean changed = false;
        for (KVRangeStoreDescriptor storeDesc : clusterInfo.storeDescriptors) {
            changed |= router.upsert(storeDesc);
        }
        if (changed) {
            if (router.isFullRangeCovered()) {
                synchronized (this) {
                    this.notifyAll();
                }
            }
        }
        return changed;
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
        return true;
    }

    private void refreshExecPipelines(Map<String, String> newStoreToServerMap) {
        Map<String, Map<KVRangeId, IExecutionPipeline>> nextExecPplns = Maps.newHashMap(execPplns);
        nextExecPplns.forEach((k, v) -> nextExecPplns.put(k, Maps.newHashMap(v)));
        Set<String> oldStoreIds = Sets.newHashSet(execPplns.keySet());

        for (String existingStoreId : Sets.intersection(newStoreToServerMap.keySet(), oldStoreIds)) {
            Set<KVRangeId> newRangeIds = router.findByStore(existingStoreId).stream()
                .map(setting -> setting.id).collect(Collectors.toSet());
            Set<KVRangeId> oldRangeIds = nextExecPplns.get(existingStoreId).keySet();
            for (KVRangeId newRangeId : Sets.difference(newRangeIds, oldRangeIds)) {
                nextExecPplns.get(existingStoreId)
                    .put(newRangeId, new ExecPipeline(newStoreToServerMap.get(existingStoreId)));
            }
            for (KVRangeId deadRangeId : Sets.difference(oldRangeIds, newRangeIds)) {
                nextExecPplns.get(existingStoreId).remove(deadRangeId).close();
            }
        }
        for (String newStoreId : Sets.difference(newStoreToServerMap.keySet(), oldStoreIds)) {
            Map<KVRangeId, IExecutionPipeline> rangeExecPplns =
                nextExecPplns.computeIfAbsent(newStoreId, k -> new HashMap<>());
            for (KVRangeSetting setting : router.findByStore(newStoreId)) {
                rangeExecPplns.put(setting.id, new ExecPipeline(newStoreToServerMap.get(newStoreId)));
            }
        }

        for (String deadStoreId : Sets.difference(oldStoreIds, newStoreToServerMap.keySet())) {
            nextExecPplns.remove(deadStoreId).values().forEach(IExecutionPipeline::close);
        }
        execPplns = nextExecPplns;
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

    private class ExecPipeline implements IExecutionPipeline {
        private final IRPCClient.IRequestPipeline<KVRangeRWRequest, KVRangeRWReply> ppln;

        ExecPipeline(String serverId) {
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
