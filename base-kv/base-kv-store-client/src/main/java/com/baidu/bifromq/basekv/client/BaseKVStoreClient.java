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

import static com.baidu.bifromq.basekv.store.CRDTUtil.getDescriptorFromCRDT;
import static com.baidu.bifromq.basekv.store.CRDTUtil.storeDescriptorMapCRDTURI;

import com.baidu.bifromq.basecrdt.core.api.IORMap;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.basekv.KVRangeRouter;
import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.exception.BaseKVException;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.proto.Range;
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
import com.baidu.bifromq.baserpc.IRPCClient;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BaseKVStoreClient implements IBaseKVStoreClient {
    protected final String clusterId;
    private final IRPCClient rpcClient;
    private final ICRDTService crdtService;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final KVRangeRouter router = new KVRangeRouter();
    private final int execPipelinesPerServer;
    private final int queryPipelinesPerServer;
    private final IORMap storeDescriptorCRDT;
    private volatile Map<String, List<IExecutionPipeline>> execPplns = Maps.newHashMap();
    private volatile Map<String, List<IQueryPipeline>> queryPplns = Maps.newHashMap();
    private volatile Map<String, List<IQueryPipeline>> lnrQueryPplns = Maps.newHashMap();

    BaseKVStoreClient(@lombok.NonNull String clusterId,
                      @lombok.NonNull ICRDTService crdtService,
                      @lombok.NonNull IRPCClient rpcClient,
                      int execPipelinesPerServer,
                      int queryPipelinesPerServer) {
        this.clusterId = clusterId;
        this.crdtService = crdtService;
        this.execPipelinesPerServer = execPipelinesPerServer <= 0 ? 5 : execPipelinesPerServer;
        this.queryPipelinesPerServer = queryPipelinesPerServer <= 0 ? 5 : queryPipelinesPerServer;
        this.rpcClient = rpcClient;
        crdtService.host(storeDescriptorMapCRDTURI(clusterId));
        storeDescriptorCRDT = (IORMap) crdtService.get(storeDescriptorMapCRDTURI(clusterId)).get();

        disposables.add(storeDescriptorCRDT.inflation().subscribe(this::updateRouter));
        disposables.add(rpcClient.serverList().subscribe(this::refreshPipelinePool));
        updateRouter(System.currentTimeMillis());
    }

    @Override
    public String clusterId() {
        return clusterId;
    }

    @Override
    public Observable<Set<String>> storeServers() {
        return rpcClient.serverList();
    }

    @Override
    public Observable<Set<KVRangeStoreDescriptor>> describe() {
        return storeDescriptorCRDT.inflation().mapOptional(l -> {
            Set<KVRangeStoreDescriptor> descriptors = Sets.newHashSet();
            Iterator<ByteString> keyItr = Iterators.transform(storeDescriptorCRDT.keys(), k -> k.key());
            while (keyItr.hasNext()) {
                Optional<KVRangeStoreDescriptor> descriptor = getDescriptorFromCRDT(storeDescriptorCRDT, keyItr.next());
                if (descriptor.isPresent()) {
                    descriptors.add(descriptor.get());
                }
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
        return rpcClient.invoke("", storeId,
            BootstrapRequest.newBuilder().setReqId(System.nanoTime()).build(),
            BaseKVStoreServiceGrpc.getBootstrapMethod());
    }

    @Override
    public CompletableFuture<RecoverReply> recover(String storeId, RecoverRequest request) {
        return rpcClient.invoke("", storeId, request, BaseKVStoreServiceGrpc.getRecoverMethod());
    }

    @Override
    public CompletableFuture<TransferLeadershipReply> transferLeadership(String storeId,
                                                                         TransferLeadershipRequest request) {
        return rpcClient.invoke("", storeId, request, BaseKVStoreServiceGrpc.getTransferLeadershipMethod())
            .exceptionally(e -> TransferLeadershipReply.newBuilder()
                .setReqId(request.getReqId())
                .setCode(ReplyCode.InternalError)
                .build());
    }

    @Override
    public CompletableFuture<ChangeReplicaConfigReply> changeReplicaConfig(String storeId,
                                                                           ChangeReplicaConfigRequest request) {
        return rpcClient.invoke("", storeId, request, BaseKVStoreServiceGrpc.getChangeReplicaConfigMethod())
            .exceptionally(e -> ChangeReplicaConfigReply.newBuilder()
                .setReqId(request.getReqId())
                .setCode(ReplyCode.InternalError)
                .build());
    }

    @Override
    public CompletableFuture<KVRangeSplitReply> splitRange(String storeId, KVRangeSplitRequest request) {
        return rpcClient.invoke("", storeId, request, BaseKVStoreServiceGrpc.getSplitMethod())
            .exceptionally(e -> KVRangeSplitReply.newBuilder()
                .setReqId(request.getReqId())
                .setCode(ReplyCode.InternalError)
                .build());
    }

    @Override
    public CompletableFuture<KVRangeMergeReply> mergeRanges(String storeId, KVRangeMergeRequest request) {
        return rpcClient.invoke("", storeId, request, BaseKVStoreServiceGrpc.getMergeMethod())
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
        List<IExecutionPipeline> pipelines = execPplns.get(storeId);
        if (pipelines == null) {
            return CompletableFuture.failedFuture(BaseKVException.SERVER_NOT_FOUND);
        }
        return pipelines.get((orderKey.hashCode() % pipelines.size() + pipelines.size()) % pipelines.size())
            .execute(request);
    }

    @Override
    public CompletableFuture<KVRangeROReply> query(String storeId, KVRangeRORequest request) {
        return query(storeId, request, String.valueOf(Thread.currentThread().getId()));
    }

    @Override
    public CompletableFuture<KVRangeROReply> query(String storeId, KVRangeRORequest request, String orderKey) {
        List<IQueryPipeline> pipelines = queryPplns.get(storeId);
        if (pipelines == null) {
            return CompletableFuture.failedFuture(BaseKVException.SERVER_NOT_FOUND);
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
            return CompletableFuture.failedFuture(BaseKVException.SERVER_NOT_FOUND);
        }
        return pipelines.get((orderKey.hashCode() % pipelines.size() + pipelines.size()) % pipelines.size())
            .query(request);
    }

    @Override
    public IExecutionPipeline createExecutionPipeline(String storeId) {
        return new ExecPipeline(storeId);
    }

    @Override
    public IQueryPipeline createQueryPipeline(String storeId) {
        return new QueryPipeline(storeId, false);
    }

    @Override
    public IQueryPipeline createLinearizedQueryPipeline(String storeId) {
        return new QueryPipeline(storeId, true);
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
                }
            }
        }
        rpcClient.connState()
            .filter(connState -> connState == IRPCClient.ConnState.READY || connState == IRPCClient.ConnState.SHUTDOWN)
            .blockingFirst();
    }

    @Override
    public final void stop() {
        if (closed.compareAndSet(false, true)) {
            log.info("Stopping BaseKVStore client: cluster[{}]", clusterId);
            disposables.dispose();
            log.debug("Closing execution pipelines: cluster[{}]", clusterId);
            execPplns.values().forEach(pplns -> pplns.forEach(p -> p.close()));
            log.debug("Closing query pipelines: cluster[{}]", clusterId);
            queryPplns.values().forEach(pplns -> pplns.forEach(p -> p.close()));
            log.debug("Closing linearizable query pipelines: cluster[{}]", clusterId);
            lnrQueryPplns.values().forEach(pplns -> pplns.forEach(p -> p.close()));
            log.debug("Stopping hosting crdt: cluster[{}]", clusterId);
            crdtService.stopHosting(storeDescriptorMapCRDTURI(clusterId)).join();
            log.debug("Stopping rpc client: cluster[{}]", clusterId);
            rpcClient.stop();
            log.info("BaseKVStore client stopped: cluster[{}]", clusterId);
        }
    }

    private void updateRouter(long ts) {
        Iterator<ByteString> keyItr = Iterators.transform(storeDescriptorCRDT.keys(), k -> k.key());
        while (keyItr.hasNext()) {
            Optional<KVRangeStoreDescriptor> storeDescOpt = getDescriptorFromCRDT(storeDescriptorCRDT, keyItr.next());
            if (storeDescOpt.isPresent()) {
                KVRangeStoreDescriptor storeDesc = storeDescOpt.get();
                updateRouter(storeDesc);
            }
        }
    }

    private void updateRouter(KVRangeStoreDescriptor storeDescriptor) {
        log.debug("Update router with store descriptor\n{}", storeDescriptor);
        router.upsert(storeDescriptor);
        if (router.isFullRangeCovered()) {
            synchronized (this) {
                this.notifyAll();
            }
        }
    }

    private void refreshPipelinePool(Set<String> serverIds) {
        Map<String, List<IExecutionPipeline>> nextExecPplns = Maps.newHashMap(execPplns);
        for (String newServer : Sets.difference(serverIds, execPplns.keySet())) {
            nextExecPplns.put(newServer, new ArrayList<>());
            IntStream.range(0, execPipelinesPerServer)
                .forEach(i -> nextExecPplns.get(newServer).add(new ExecPipeline(newServer)));
        }
        for (String deadServer : Sets.difference(execPplns.keySet(), serverIds)) {
            nextExecPplns.remove(deadServer).forEach(IExecutionPipeline::close);
        }

        Map<String, List<IQueryPipeline>> nextQueryPplns = Maps.newHashMap(queryPplns);
        for (String newServer : Sets.difference(serverIds, queryPplns.keySet())) {
            nextQueryPplns.put(newServer, new ArrayList<>());
            IntStream.range(0, queryPipelinesPerServer)
                .forEach(i -> nextQueryPplns.get(newServer).add(new QueryPipeline(newServer)));
        }
        for (String deadServer : Sets.difference(queryPplns.keySet(), serverIds)) {
            nextQueryPplns.remove(deadServer).forEach(IQueryPipeline::close);
        }

        Map<String, List<IQueryPipeline>> nextLnrQueryPplns = Maps.newHashMap(lnrQueryPplns);
        for (String newServer : Sets.difference(serverIds, lnrQueryPplns.keySet())) {
            nextLnrQueryPplns.put(newServer, new ArrayList<>());
            IntStream.range(0, queryPipelinesPerServer)
                .forEach(i -> nextLnrQueryPplns.get(newServer).add(new QueryPipeline(newServer, true)));
        }
        for (String deadServer : Sets.difference(lnrQueryPplns.keySet(), serverIds)) {
            nextLnrQueryPplns.remove(deadServer).forEach(IQueryPipeline::close);
        }

        execPplns = nextExecPplns;
        queryPplns = nextQueryPplns;
        lnrQueryPplns = nextLnrQueryPplns;
    }

    private class ExecPipeline implements IExecutionPipeline {
        private final IRPCClient.IRequestPipeline<KVRangeRWRequest, KVRangeRWReply> ppln;

        ExecPipeline(String serverId) {
            ppln = rpcClient.createRequestPipeline("", serverId, null,
                Collections.emptyMap(), BaseKVStoreServiceGrpc.getExecuteMethod());
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
                ppln = rpcClient.createRequestPipeline("", serverId, null,
                    Collections.emptyMap(), BaseKVStoreServiceGrpc.getLinearizedQueryMethod());
            } else {
                ppln = rpcClient.createRequestPipeline("", serverId, null,
                    Collections.emptyMap(), BaseKVStoreServiceGrpc.getQueryMethod());
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
