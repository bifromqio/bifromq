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

package com.baidu.bifromq.basekv.store;

import static java.util.Collections.emptySet;
import static org.awaitility.Awaitility.await;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.TestCoProcFactory;
import com.baidu.bifromq.basekv.TestUtil;
import com.baidu.bifromq.basekv.localengine.InMemoryKVEngineConfigurator;
import com.baidu.bifromq.basekv.localengine.RocksDBKVEngineConfigurator;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.proto.State;
import com.baidu.bifromq.basekv.proto.StoreMessage;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.baidu.bifromq.basekv.store.exception.KVRangeException;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KVRangeStoreTestCluster {
    private static final String DB_NAME = "testDB";
    private static final String DB_CHECKPOINT_DIR_NAME = "testDB_cp";
    private static final String DB_WAL_NAME = "testWAL";
    private static final String DB_WAL_CHECKPOINT_DIR = "testWAL_cp";
    private static final String CLUSTER = "test_cluster";
    private final String bootstrapStore;
    private final KVRangeId genesisKVRangeId;
    private final Map<String, String> storePathMap = Maps.newConcurrentMap();
    private final Map<String, KVRangeStore> rangeStoreMap = Maps.newConcurrentMap();
    private final Map<String, PublishSubject<StoreMessage>> rangeStoreMsgSourceMap = Maps.newConcurrentMap();
    private final Map<String, KVRangeStoreDescriptor> storeDescriptorMap = Maps.newConcurrentMap();
    private final Map<KVRangeId, KVRangeSetting> rangeSettingMap = Maps.newConcurrentMap();
    private final Map<String, Set<String>> cutMap = Maps.newConcurrentMap();
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final KVRangeStoreOptions optionsTpl;
    private final ExecutorService queryExecutor = new ThreadPoolExecutor(2, 2, 0L,
        TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
        EnvProvider.INSTANCE.newThreadFactory("query-executor"));
    private final ExecutorService mutationExecutor = new ThreadPoolExecutor(2, 2, 0L,
        TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
        EnvProvider.INSTANCE.newThreadFactory("mutation-executor"));
    private final ScheduledExecutorService tickTaskExecutor = new ScheduledThreadPoolExecutor(2,
        EnvProvider.INSTANCE.newThreadFactory("tick-task-executor"));
    private final ScheduledExecutorService bgTaskExecutor = new ScheduledThreadPoolExecutor(1,
        EnvProvider.INSTANCE.newThreadFactory("bg-task-executor"));

    private Path dbRootDir;

    public KVRangeStoreTestCluster(KVRangeStoreOptions options) {
        this.optionsTpl = options.toBuilder().build();
        try {
            dbRootDir = Files.createTempDirectory("");
        } catch (IOException e) {
        }
        bootstrapStore = buildStore(true);
        List<KVRangeDescriptor> list = rangeStoreMap.get(bootstrapStore).describe().blockingFirst().getRangesList();
        genesisKVRangeId = list.get(0).getId();
    }

    public String addStore() {
        return buildStore(false);
    }

    public void startStore(String storeId) {
        Preconditions.checkArgument(storePathMap.containsKey(storeId), "Unknown store %s", storeId);
        loadStore(storeId);
    }

    public void shutdownStore(String storeId) {
        checkStore(storeId);
        rangeStoreMap.remove(storeId).stop();
        rangeStoreMsgSourceMap.remove(storeId).onComplete();
    }

    public CompletionStage<Void> recover(String storeId) {
        checkStore(storeId);
        return rangeStoreMap.get(storeId).recover();
    }

    public String bootstrapStore() {
        return bootstrapStore;
    }

    public KVRangeId genesisKVRangeId() {
        return genesisKVRangeId;
    }

    public List<String> allStoreIds() {
        return Lists.newArrayList(rangeStoreMap.keySet());
    }

    public boolean hasKVRange(String storeId, KVRangeId kvRangeId) {
        checkStore(storeId);
        return storeDescriptorMap.getOrDefault(storeId, KVRangeStoreDescriptor.getDefaultInstance())
            .getRangesList()
            .stream()
            .map(r -> r.getId())
            .collect(Collectors.toList())
            .contains(kvRangeId);
    }

    public KVRangeDescriptor getKVRange(String storeId, KVRangeId kvRangeId) {
        checkStore(storeId);
        return storeDescriptorMap
            .getOrDefault(storeId, KVRangeStoreDescriptor.getDefaultInstance())
            .getRangesList()
            .stream()
            .filter(r -> r.getId().equals(kvRangeId))
            .findFirst()
            .orElse(null);
    }

    public List<KVRangeId> allKVRangeIds() {
        return Lists.newArrayList(rangeSettingMap.keySet());
    }

    public KVRangeSetting kvRangeSetting(KVRangeId kvRangeId) {
        checkKVRangeId(kvRangeId);
        return rangeSettingMap.get(kvRangeId);
    }

    public void awaitKVRangeReady(String storeId, KVRangeId kvRangeId) {
        await().atMost(Duration.ofSeconds(10)).until(() -> {
            KVRangeSetting kvRangeSetting = rangeSettingMap.get(kvRangeId);
            return hasKVRange(storeId, kvRangeId) && kvRangeSetting != null;
        });
    }

    public KVRangeSetting awaitAllKVRangeReady(KVRangeId kvRangeId, long atLeastVer, long timeoutInMS) {
        await().atMost(Duration.ofMillis(timeoutInMS)).until(() -> {
            boolean allReady = true;
            for (KVRangeStoreDescriptor storeDescriptor : storeDescriptorMap.values()) {
                boolean ready = false;
                for (KVRangeDescriptor rangeDescriptor : storeDescriptor.getRangesList()) {
                    if (rangeDescriptor.getId().equals(kvRangeId) && rangeDescriptor.getVer() >= atLeastVer) {
                        ready = true;
                    }
                }
                allReady &= ready;
            }
            return allReady;
        });
        return rangeSettingMap.get(kvRangeId);
    }

    public CompletionStage<Void> transferLeader(String storeId, long ver, KVRangeId kvRangeId, String newLeader) {
        checkStore(storeId);
        return rangeStoreMap.get(storeId).transferLeadership(ver, kvRangeId, newLeader);
    }

    public CompletionStage<Void> changeReplicaConfig(String storeId, long ver, KVRangeId kvRangeId,
                                                     Set<String> newVoters, Set<String> newLearners) {
        checkStore(storeId);
        return rangeStoreMap.get(storeId).changeReplicaConfig(ver, kvRangeId, newVoters, newLearners);
    }

    public void cut(String fromStoreId, String toStoreId) {
        cutMap.computeIfAbsent(fromStoreId, k -> Sets.newConcurrentHashSet()).add(toStoreId);
    }

    public void uncut(String fromStoreId, String toStoreId) {
        cutMap.computeIfAbsent(fromStoreId, k -> Sets.newConcurrentHashSet()).remove(toStoreId);
    }

    public void isolate(String storeId) {
        Set<String> peers = allStoreIds().stream().filter(s -> !s.equals(storeId)).collect(Collectors.toSet());
        peers.forEach(peer -> {
            cut(storeId, peer);
            cut(peer, storeId);
        });
    }

    public void integrate(String storeId) {
        Set<String> peers = allStoreIds().stream().filter(s -> !s.equals(storeId)).collect(Collectors.toSet());
        peers.forEach(peer -> {
            uncut(storeId, peer);
            uncut(peer, storeId);
        });
    }

    public CompletionStage<Void> split(String storeId, long ver, KVRangeId kvRangeId, ByteString splitKey) {
        checkStore(storeId);
        return rangeStoreMap.get(storeId).split(ver, kvRangeId, splitKey);
    }

    public CompletionStage<Void> merge(String storeId, long ver, KVRangeId mergerId, KVRangeId mergeeId) {
        checkStore(storeId);
        return rangeStoreMap.get(storeId).merge(ver, mergerId, mergeeId);
    }

    public boolean exist(String storeId, KVRangeId kvRangeId, ByteString key) {
        checkStore(storeId);
        CompletableFuture<Boolean> onDone = new CompletableFuture<>();
        exist(storeId, kvRangeId, key, onDone);
        return onDone.join();
    }

    private void exist(String storeId, KVRangeId kvRangeId, ByteString key, CompletableFuture<Boolean> onDone) {
        checkStore(storeId);
        rangeStoreMap.get(storeId).exist(kvRangeSetting(kvRangeId).ver, kvRangeId, key, true)
            .whenComplete((v, e) -> {
                if (e != null) {
                    if (shouldRetry(e)) {
                        exist(storeId, kvRangeId, key, onDone);
                    } else {
                        onDone.completeExceptionally(e);
                    }
                } else {
                    onDone.complete(v);
                }
            });
    }

    public Optional<ByteString> get(String storeId, KVRangeId kvRangeId, ByteString key) {
        checkStore(storeId);
        CompletableFuture<Optional<ByteString>> onDone = new CompletableFuture<>();
        get(storeId, kvRangeId, key, onDone);
        return onDone.join();
    }

    private void get(String storeId, KVRangeId kvRangeId, ByteString key,
                     CompletableFuture<Optional<ByteString>> onDone) {
        checkStore(storeId);
        rangeStoreMap.get(storeId).get(kvRangeSetting(kvRangeId).ver, kvRangeId, key, true)
            .whenComplete((v, e) -> {
                if (e != null) {
                    if (shouldRetry(e)) {
                        get(storeId, kvRangeId, key, onDone);
                    } else {
                        onDone.completeExceptionally(e);
                    }
                } else {
                    onDone.complete(v);
                }
            });
    }

    public ByteString queryCoProc(String storeId, KVRangeId kvRangeId, ByteString key) {
        checkStore(storeId);
        CompletableFuture<ByteString> onDone = new CompletableFuture<>();
        queryCoProc(storeId, kvRangeId, key, onDone);
        return onDone.join();
    }

    private void queryCoProc(String storeId, KVRangeId kvRangeId, ByteString coProc,
                             CompletableFuture<ByteString> onDone) {
        checkStore(storeId);
        rangeStoreMap.get(storeId).queryCoProc(kvRangeSetting(kvRangeId).ver, kvRangeId, coProc, true)
            .whenComplete((v, e) -> {
                if (e != null) {
                    if (shouldRetry(e)) {
                        queryCoProc(storeId, kvRangeId, coProc, onDone);
                    } else {
                        onDone.completeExceptionally(e);
                    }
                } else {
                    onDone.complete(v);
                }
            });
    }

    public CompletionStage<ByteString> put(String storeId, long ver, KVRangeId kvRangeId, ByteString key,
                                           ByteString value) {
        return rangeStoreMap.get(storeId).put(ver, kvRangeId, key, value);
    }

    public ByteString put(String storeId, KVRangeId kvRangeId, ByteString key, ByteString value) {
        CompletableFuture<ByteString> onDone = new CompletableFuture<>();
        put(storeId, kvRangeId, key, value, onDone);
        return onDone.join();
    }

    private void put(String storeId, KVRangeId kvRangeId, ByteString key, ByteString value,
                     CompletableFuture<ByteString> onDone) {
        checkStore(storeId);
        rangeStoreMap.get(storeId).put(kvRangeSetting(kvRangeId).ver, kvRangeId, key, value)
            .whenComplete((v, e) -> {
                if (e != null) {
                    if (shouldRetry(e)) {
                        put(storeId, kvRangeId, key, value, onDone);
                    } else {
                        onDone.completeExceptionally(e);
                    }
                } else {
                    onDone.complete(v);
                }
            });
    }

    public ByteString delete(String storeId, KVRangeId kvRangeId, ByteString key) {
        CompletableFuture<ByteString> onDone = new CompletableFuture<>();
        delete(storeId, kvRangeId, key, onDone);
        return onDone.join();
    }

    private void delete(String storeId, KVRangeId kvRangeId, ByteString key, CompletableFuture<ByteString> onDone) {
        checkStore(storeId);
        rangeStoreMap.get(storeId).delete(kvRangeSetting(kvRangeId).ver, kvRangeId, key)
            .whenComplete((v, e) -> {
                if (e != null) {
                    if (shouldRetry(e)) {
                        delete(storeId, kvRangeId, key, onDone);
                    } else {
                        onDone.completeExceptionally(e);
                    }
                } else {
                    onDone.complete(v);
                }
            });
    }

    public ByteString mutateCoProc(String storeId, KVRangeId kvRangeId, ByteString key) {
        CompletableFuture<ByteString> onDone = new CompletableFuture<>();
        mutateCoProc(storeId, kvRangeId, key, onDone);
        return onDone.join();
    }

    private void mutateCoProc(String storeId, KVRangeId kvRangeId, ByteString key,
                              CompletableFuture<ByteString> onDone) {
        checkStore(storeId);
        rangeStoreMap.get(storeId).mutateCoProc(kvRangeSetting(kvRangeId).ver, kvRangeId, key)
            .whenComplete((v, e) -> {
                if (e != null) {
                    if (shouldRetry(e)) {
                        mutateCoProc(storeId, kvRangeId, key, onDone);
                    } else {
                        onDone.completeExceptionally(e);
                    }
                } else {
                    onDone.complete(v);
                }
            });
    }


    public void shutdown() {
        disposables.dispose();
        rangeStoreMap.values().forEach(IKVRangeStore::stop);
        TestUtil.deleteDir(dbRootDir.toString());
        dbRootDir.toFile().delete();
    }

    private String buildStore(boolean isBootstrap) {
        String uuid = UUID.randomUUID().toString();
        KVRangeStoreOptions options = optionsTpl.toBuilder().build();
        if (options.getDataEngineConfigurator() instanceof RocksDBKVEngineConfigurator) {
            ((RocksDBKVEngineConfigurator) options.getDataEngineConfigurator())
                .setDbCheckpointRootDir(Paths.get(dbRootDir.toString(), DB_CHECKPOINT_DIR_NAME, uuid)
                    .toString())
                .setDbRootDir(Paths.get(dbRootDir.toString(), DB_NAME, uuid).toString());
        }
        if (options.getWalEngineConfigurator() instanceof RocksDBKVEngineConfigurator) {
            ((RocksDBKVEngineConfigurator) options
                .getWalEngineConfigurator())
                .setDbCheckpointRootDir(Paths.get(dbRootDir.toString(), DB_WAL_CHECKPOINT_DIR, uuid)
                    .toString())
                .setDbRootDir(Paths.get(dbRootDir.toString(), DB_WAL_NAME, uuid).toString());
        }
        KVRangeStore store = initStore(options);
        if (isBootstrap) {
            store.bootstrap();
        }
        storePathMap.put(store.id(), uuid);
        return store.id();
    }

    private void loadStore(String storeId) {
        String uuid = storePathMap.get(storeId);
        KVRangeStoreOptions options = optionsTpl.toBuilder().build();
        if (options.getWalEngineConfigurator() instanceof InMemoryKVEngineConfigurator) {
            options.setOverrideIdentity(storeId);
        }
        if (options.getDataEngineConfigurator() instanceof RocksDBKVEngineConfigurator) {
            ((RocksDBKVEngineConfigurator) options.getDataEngineConfigurator())
                .setDbCheckpointRootDir(Paths.get(dbRootDir.toString(), DB_CHECKPOINT_DIR_NAME, uuid)
                    .toString())
                .setDbRootDir(Paths.get(dbRootDir.toString(), DB_NAME, uuid).toString());
            ((RocksDBKVEngineConfigurator) options.getWalEngineConfigurator())
                .setDbCheckpointRootDir(Paths.get(dbRootDir.toString(), DB_WAL_CHECKPOINT_DIR, uuid)
                    .toString())
                .setDbRootDir(Paths.get(dbRootDir.toString(), DB_WAL_NAME, uuid).toString());
        }
        initStore(options);
    }

    private KVRangeStore initStore(KVRangeStoreOptions options) {
        KVRangeStore store =
            new KVRangeStore(CLUSTER,
                options,
                new TestCoProcFactory(),
                queryExecutor,
                mutationExecutor,
                tickTaskExecutor,
                bgTaskExecutor);
        PublishSubject<StoreMessage> storeMsgSource = PublishSubject.create();
        store.start(new IStoreMessenger() {
            @Override
            public void send(StoreMessage message) {
                if (message.getPayload().hasHostStoreId()) {
                    if (rangeStoreMsgSourceMap.containsKey(message.getPayload().getHostStoreId()) &&
                        !cutMap.getOrDefault(message.getFrom(), emptySet())
                            .contains(message.getPayload().getHostStoreId())) {
                        rangeStoreMsgSourceMap.get(message.getPayload().getHostStoreId()).onNext(message);
                    }
                } else {
                    // broadcast
                    rangeStoreMsgSourceMap.forEach((storeId, msgSubject) ->
                        msgSubject.onNext(message.toBuilder()
                            .setPayload(message.getPayload().toBuilder()
                                // fill the target store
                                .setHostStoreId(storeId)
                                .build())
                            .build()));
                }
            }

            @Override
            public Observable<StoreMessage> receive() {
                return storeMsgSource.subscribeOn(Schedulers.io());
            }

            @Override
            public void close() {

            }
        });
        rangeStoreMap.put(store.id(), store);
        rangeStoreMsgSourceMap.put(store.id(), storeMsgSource);
        disposables.add(store.describe().subscribe(this::handleStoreDescriptor));
        return store;
    }

    private void handleStoreDescriptor(KVRangeStoreDescriptor storeDescriptor) {
        storeDescriptorMap.put(storeDescriptor.getId(), storeDescriptor);
        storeDescriptor.getRangesList().forEach(rangeDescriptor -> {
            if (rangeDescriptor.getRole() == RaftNodeStatus.Leader &&
                rangeDescriptor.getState() == State.StateType.Normal &&
                rangeDescriptor.getConfig().getNextVotersCount() == 0 &&
                rangeDescriptor.getConfig().getNextLearnersCount() == 0) {
                KVRangeSetting settings = new KVRangeSetting(storeDescriptor.getId(), rangeDescriptor);
                rangeSettingMap.compute(rangeDescriptor.getId(), (id, oldSettings) -> {
                    if (oldSettings != null) {
                        if (oldSettings.ver <= rangeDescriptor.getVer()) {
                            return settings;
                        }
                        return oldSettings;
                    }
                    return settings;
                });
            }
        });
    }

    private void checkStore(String storeId) {
        Preconditions.checkArgument(rangeStoreMap.containsKey(storeId));
    }

    private void checkKVRangeId(KVRangeId kvRangeId) {
        Preconditions.checkArgument(rangeSettingMap.containsKey(kvRangeId));
    }

    private boolean shouldRetry(Throwable e) {
        return e instanceof KVRangeException.TryLater ||
            e instanceof KVRangeException.BadVersion ||
            e.getCause() instanceof KVRangeException.TryLater ||
            e.getCause() instanceof KVRangeException.BadVersion;
    }

    private long reqId() {
        return System.nanoTime();
    }
}
