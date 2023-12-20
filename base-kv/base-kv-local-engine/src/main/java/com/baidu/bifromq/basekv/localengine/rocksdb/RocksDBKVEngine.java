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

package com.baidu.bifromq.basekv.localengine.rocksdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;

import com.baidu.bifromq.basekv.localengine.AbstractKVEngine;
import com.baidu.bifromq.basekv.localengine.KVEngineException;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

@Slf4j
public abstract class RocksDBKVEngine<
    E extends RocksDBKVEngine<E, T, C>,
    T extends RocksDBKVSpace<E, T, C>,
    C extends RocksDBKVEngineConfigurator<C>
    > extends AbstractKVEngine<T> {
    private final File dbRootDir;
    private final DBOptions dbOptions;
    private final C configurator;
    private final Map<ColumnFamilyDescriptor, ColumnFamilyHandle> existingColumnFamilies = new HashMap<>();
    private final ConcurrentMap<String, T> kvSpaceMap = new ConcurrentHashMap<>();
    private final String identity;
    private final RocksDB db;
    private final ColumnFamilyDescriptor defaultCFDesc;
    private final ColumnFamilyHandle defaultCFHandle;
    private String[] metricTags;
    private MetricManager metricManager;

    public RocksDBKVEngine(String overrideIdentity, C configurator) {
        super(overrideIdentity);
        this.configurator = configurator;
        dbOptions = configurator.dbOptions();
        dbRootDir = new File(configurator.dbRootDir());
        try (Options options = new Options()) {
            Files.createDirectories(dbRootDir.getAbsoluteFile().toPath());
            boolean isCreation = isEmpty(dbRootDir.toPath());
            if (isCreation) {
                defaultCFDesc = new ColumnFamilyDescriptor(DEFAULT_NS.getBytes());
                List<ColumnFamilyDescriptor> cfDescs = singletonList(defaultCFDesc);
                List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
                db = RocksDB.open(dbOptions, dbRootDir.getAbsolutePath(), cfDescs, cfHandles);
                assert cfHandles.size() == 1;
                defaultCFHandle = cfHandles.get(0);
            } else {
                List<ColumnFamilyDescriptor> cfDescs = RocksDB.listColumnFamilies(options, dbRootDir.getAbsolutePath())
                    .stream()
                    .map(nameBytes -> new ColumnFamilyDescriptor(nameBytes,
                        configurator.cfOptions(new String(nameBytes, UTF_8))))
                    .toList();
                List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
                db = RocksDB.open(dbOptions, dbRootDir.getAbsolutePath(), cfDescs, cfHandles);
                assert Arrays.equals(cfDescs.get(0).getName(), DEFAULT_NS.getBytes(UTF_8));
                assert cfDescs.size() == cfHandles.size();
                defaultCFDesc = cfDescs.get(0);
                defaultCFHandle = cfHandles.get(0);
                for (int i = 1; i < cfDescs.size(); i++) {
                    ColumnFamilyDescriptor cfDesc = cfDescs.get(i);
                    ColumnFamilyHandle cfHandle = cfHandles.get(i);
                    existingColumnFamilies.put(cfDesc, cfHandle);
                }
            }

            identity = loadIdentity(isCreation);
            log.info("RocksDBKVEngine[{}] {} at path[{}]", identity, isCreation ? "initialized" : "loaded",
                db.getName());
        } catch (Throwable e) {
            throw new KVEngineException("Failed to initialize RocksDB", e);
        }
    }

    @Override
    public T createIfMissing(String spaceId) {
        assertStarted();
        return kvSpaceMap.computeIfAbsent(spaceId,
            k -> {
                try {
                    ColumnFamilyDescriptor cfDesc =
                        new ColumnFamilyDescriptor(spaceId.getBytes(UTF_8), configurator.cfOptions(spaceId));
                    ColumnFamilyHandle cfHandle = db.createColumnFamily(cfDesc);
                    return buildKVSpace(spaceId, cfDesc, cfHandle, db, () -> kvSpaceMap.remove(spaceId),
                        metricTags).open();
                } catch (RocksDBException e) {
                    throw new KVEngineException("Create key range error", e);
                }
            });
    }

    protected abstract T buildKVSpace(String spaceId,
                                      ColumnFamilyDescriptor cfDesc,
                                      ColumnFamilyHandle cfHandle,
                                      RocksDB db,
                                      Runnable onDestroy,
                                      String... tags);

    @Override
    protected void doStart(String... metricTags) {
        loadExisting(metricTags);
        metricManager = new MetricManager(metricTags);
        this.metricTags = metricTags;
    }

    @Override
    protected void afterStart() {
        super.afterStart();
    }

    @Override
    public Map<String, T> spaces() {
        assertStarted();
        return Collections.unmodifiableMap(kvSpaceMap);
    }

    @Override
    protected void doStop() {
        log.info("Stopping RocksDBKVEngine[{}]", identity);
        metricManager.close();
        kvSpaceMap.values().forEach(RocksDBKVSpace::close);
        db.destroyColumnFamilyHandle(defaultCFHandle);
        defaultCFDesc.getOptions().close();
        db.close();
        dbOptions.close();
    }

    @Override
    public String id() {
        return identity;
    }

    private void loadExisting(String... metricTags) {
        existingColumnFamilies.forEach((cfDesc, cfHandle) -> {
            String rangeId = new String(cfDesc.getName());
            kvSpaceMap.put(rangeId,
                buildKVSpace(rangeId, cfDesc, cfHandle, db, () -> kvSpaceMap.remove(rangeId), metricTags).open());
        });
        existingColumnFamilies.clear();
    }

    private String loadIdentity(boolean isCreation) {
        try {
            Path overrideIdentityFilePath = Paths.get(dbRootDir.getAbsolutePath(), "OVERRIDEIDENTITY");
            if (isCreation && (overrideIdentity != null && !overrideIdentity.trim().isEmpty())) {
                Files.writeString(overrideIdentityFilePath, overrideIdentity, StandardOpenOption.CREATE);
            }
            if (overrideIdentityFilePath.toFile().exists()) {
                List<String> lines = Files.readAllLines(overrideIdentityFilePath);
                if (!lines.isEmpty()) {
                    return lines.get(0);
                }
            }
            List<String> lines = Files.readAllLines(Paths.get(dbRootDir.getAbsolutePath(), "IDENTITY"));
            return lines.get(0);
        } catch (IndexOutOfBoundsException | IOException e) {
            throw new KVEngineException("Failed to read IDENTITY file", e);
        }
    }

    private static boolean isEmpty(Path path) throws IOException {
        if (Files.isDirectory(path)) {
            try (Stream<Path> entries = Files.list(path)) {
                return entries.findFirst().isEmpty();
            }
        }
        return false;
    }

    private class MetricManager {
        private final Gauge dataTotalSpaceGauge;
        private final Gauge dataUsableSpaceGauge;

        MetricManager(String... metricTags) {
            Tags tags = Tags.of(metricTags);
            dataTotalSpaceGauge = Gauge.builder("basekv.le.rocksdb.total.data", dbRootDir::getTotalSpace)
                .tags(tags)
                .register(Metrics.globalRegistry);
            dataUsableSpaceGauge = Gauge.builder("basekv.le.rocksdb.usable.data", dbRootDir::getUsableSpace)
                .tags(tags)
                .register(Metrics.globalRegistry);
        }

        void close() {
            Metrics.globalRegistry.remove(dataTotalSpaceGauge);
            Metrics.globalRegistry.remove(dataUsableSpaceGauge);
        }
    }
}
