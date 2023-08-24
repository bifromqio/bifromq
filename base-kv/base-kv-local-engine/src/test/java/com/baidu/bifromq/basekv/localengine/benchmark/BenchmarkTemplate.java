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

package com.baidu.bifromq.basekv.localengine.benchmark;

import static com.baidu.bifromq.basekv.localengine.IKVEngine.DEFAULT_NS;
import static java.lang.Math.max;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basekv.localengine.IKVEngine;
import com.baidu.bifromq.basekv.localengine.KVEngineFactory;
import com.baidu.bifromq.basekv.localengine.RocksDBKVEngineConfigurator;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ColumnFamilyOptionsInterface;
import org.rocksdb.CompactionStyle;
import org.rocksdb.DBOptions;
import org.rocksdb.DBOptionsInterface;
import org.rocksdb.Env;
import org.rocksdb.IndexType;
import org.rocksdb.LRUCache;
import org.rocksdb.MutableColumnFamilyOptionsInterface;
import org.rocksdb.MutableDBOptionsInterface;
import org.rocksdb.Statistics;
import org.rocksdb.util.SizeUnit;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

@Slf4j
public abstract class BenchmarkTemplate {
    private Path dbRootDir;
    protected IKVEngine kvEngine;

    @Setup
    public void setup() {
        try {
            dbRootDir = Files.createTempDirectory("");
        } catch (IOException e) {
            log.error("Failed to create temp dir", e);
        }
        String DB_NAME = "testDB";
        String DB_CHECKPOINT_DIR = "testDB_cp";
        String uuid = UUID.randomUUID().toString();
        RocksDBKVEngineConfigurator configurator = new RocksDBKVEngineConfigurator(new RocksDBKVEngineConfigurator
            .DBOptionsConfigurator() {
            @Override
            public void config(DBOptionsInterface<DBOptions> targetOption) {
                targetOption.setEnv(Env.getDefault())
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true)
                    .setManualWalFlush(true)
                    .setRecycleLogFileNum(10)
                    .setAvoidUnnecessaryBlockingIO(true)
                    .setStatistics(gcable(new Statistics()));
            }

            @Override
            public void config(MutableDBOptionsInterface<DBOptions> targetOption) {
                targetOption
                    .setMaxOpenFiles(20)
                    .setStatsDumpPeriodSec(5)
                    .setMaxBackgroundJobs(max(EnvProvider.INSTANCE.availableProcessors(), 2));
            }
        }, new RocksDBKVEngineConfigurator.CFOptionsConfigurator() {
            @Override
            public void config(String name, ColumnFamilyOptionsInterface<ColumnFamilyOptions> targetOption) {
                targetOption.setMergeOperatorName("uint64add")
                    .setTableFormatConfig(
                        new BlockBasedTableConfig() //
                            // Begin to use partitioned index filters
                            // https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters#how-to-use-it
                            .setIndexType(IndexType.kTwoLevelIndexSearch) //
                            .setFilterPolicy(
                                gcable(new BloomFilter(16, false)))
                            .setPartitionFilters(true) //
                            .setMetadataBlockSize(8 * SizeUnit.KB) //
                            .setCacheIndexAndFilterBlocks(true) //
                            .setPinTopLevelIndexAndFilter(true)
                            .setCacheIndexAndFilterBlocksWithHighPriority(true) //
                            .setPinL0FilterAndIndexBlocksInCache(true) //
                            // End of partitioned index filters settings.
                            .setBlockSize(4 * SizeUnit.KB)//
                            .setBlockCache(
                                gcable(new LRUCache(512 * SizeUnit.MB, 8))))
                    .optimizeLevelStyleCompaction()
                    .setCompactionStyle(CompactionStyle.LEVEL) //
                    // Flushing options:
                    // min_write_buffer_number_to_merge is the minimum number of mem_tables to be
                    // merged before flushing to storage. For example, if this option is set to 2,
                    // immutable mem_tables are only flushed when there are two of them - a single
                    // immutable mem_table will never be flushed.  If multiple mem_tables are merged
                    // together, less data may be written to storage since two updates are merged to
                    // a single key. However, every Get() must traverse all immutable mem_tables
                    // linearly to check if the key is there. Setting this option too high may hurt
                    // read performance.
                    .setMinWriteBufferNumberToMerge(2)
                    // https://github.com/facebook/rocksdb/pull/5744
                    .setForceConsistencyChecks(true);

            }

            @Override
            public void config(String name, MutableColumnFamilyOptionsInterface<ColumnFamilyOptions> targetOption) {

            }
        }).setDbCheckpointRootDir(Paths.get(dbRootDir.toString(), uuid, DB_CHECKPOINT_DIR).toString())
            .setDbRootDir(Paths.get(dbRootDir.toString(), uuid, DB_NAME).toString());
        kvEngine = KVEngineFactory.create(null, List.of(DEFAULT_NS), cpId -> true, configurator);
        kvEngine.start();
        doSetup();
    }

    @TearDown
    public void tearDown() {
        kvEngine.stop();
        try {
            Files.walk(dbRootDir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
            Files.delete(dbRootDir);
        } catch (IOException e) {
            log.error("Failed to delete db root dir", e);
        }
    }

    protected void doSetup() {
    }


    @Test
    @Ignore
    public void test() {
        Options opt = new OptionsBuilder()
            .include(this.getClass().getSimpleName())
            .warmupIterations(5)
            .measurementIterations(10)
            .forks(1)
            .build();
        try {
            new Runner(opt).run();
        } catch (RunnerException e) {
            throw new RuntimeException(e);
        }
    }
}
