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

import static java.lang.Math.max;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.basekv.localengine.IKVEngine;
import com.baidu.bifromq.basekv.localengine.KVEngineFactory;
import com.baidu.bifromq.basekv.localengine.rocksdb.RocksDBKVEngineConfigurator;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ColumnFamilyOptionsInterface;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.DBOptionsInterface;
import org.rocksdb.Env;
import org.rocksdb.IndexType;
import org.rocksdb.LRUCache;
import org.rocksdb.MutableColumnFamilyOptionsInterface;
import org.rocksdb.MutableDBOptionsInterface;
import org.rocksdb.Statistics;
import org.rocksdb.util.SizeUnit;

@Slf4j
abstract class BenchmarkState {
    protected IKVEngine kvEngine;
    private Path dbRootDir;

    BenchmarkState() {
        try {
            dbRootDir = Files.createTempDirectory("");
        } catch (IOException e) {
            log.error("Failed to create temp dir", e);
        }
        String DB_NAME = "testDB";
        String DB_CHECKPOINT_DIR = "testDB_cp";
        String uuid = UUID.randomUUID().toString();
        RocksDBKVEngineConfigurator configurator =
            new RocksDBKVEngineConfigurator(new RocksDBKVEngineConfigurator.DBOptionsConfigurator() {
                @Override
                public void config(DBOptionsInterface<DBOptions> targetOption) {
                    targetOption.setEnv(Env.getDefault())
                        .setCreateIfMissing(true)
                        .setCreateMissingColumnFamilies(true)
                        .setManualWalFlush(true)
                        .setRecycleLogFileNum(10)
                        .setAvoidUnnecessaryBlockingIO(true)
                        .setStatistics(autoRelease(new Statistics(), targetOption));
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
                                    autoRelease(new BloomFilter(16, false), targetOption))
                                .setPartitionFilters(true) //
                                .setMetadataBlockSize(8 * SizeUnit.KB) //
                                .setCacheIndexAndFilterBlocks(true) //
                                .setPinTopLevelIndexAndFilter(true)
                                .setCacheIndexAndFilterBlocksWithHighPriority(true) //
                                .setPinL0FilterAndIndexBlocksInCache(true) //
                                // End of partitioned index filters settings.
                                .setBlockSize(4 * SizeUnit.KB)//
                                .setBlockCache(
                                    autoRelease(new LRUCache(512 * SizeUnit.MB, 8), targetOption)))
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
                    targetOption
                        // Flushing options:
                        // write_buffer_size sets the size of a single mem_table. Once mem_table exceeds
                        // this size, it is marked immutable and a new one is created.
                        .setWriteBufferSize(128 * SizeUnit.MB)
                        // Level Style Compaction:
                        // level0_file_num_compaction_trigger -- Once level 0 reaches this number of
                        // files, L0->L1 compaction is triggered. We can therefore estimate level 0
                        // size in stable state as
                        // write_buffer_size * min_write_buffer_number_to_merge * level0_file_num_compaction_trigger.
                        .setLevel0FileNumCompactionTrigger(10)
                        // Level Style Compaction:
                        // max_bytes_for_level_base and max_bytes_for_level_multiplier
                        //  -- max_bytes_for_level_base is total size of level 1. As mentioned, we
                        // recommend that this be around the size of level 0. Each subsequent level
                        // is max_bytes_for_level_multiplier larger than previous one. The default
                        // is 10 and we do not recommend changing that.
                        .setMaxBytesForLevelBase(128 * SizeUnit.MB)
                        .setCompressionType(CompressionType.LZ4_COMPRESSION)

                        // Below methods are defined in AdvancedMutableColumnFamilyOptionsInterface

                        // Level Style Compaction:
                        // target_file_size_base and target_file_size_multiplier
                        //  -- Files in level 1 will have target_file_size_base bytes. Each next
                        // level's file size will be target_file_size_multiplier bigger than previous
                        // one. However, by default target_file_size_multiplier is 1, so files in all
                        // L1..LMax levels are equal. Increasing target_file_size_base will reduce total
                        // number of database files, which is generally a good thing. We recommend setting
                        // target_file_size_base to be max_bytes_for_level_base / 10, so that there are
                        // 10 files in level 1.
                        .setTargetFileSizeBase(64 * SizeUnit.MB)
                        // If prefix_extractor is set and memtable_prefix_bloom_size_ratio is not 0,
                        // create prefix bloom for memtable with the size of
                        // write_buffer_size * memtable_prefix_bloom_size_ratio.
                        // If it is larger than 0.25, it is santinized to 0.25.
                        .setMemtablePrefixBloomSizeRatio(0.125)
                        // Soft limit on number of level-0 files. We start slowing down writes at this
                        // point. A value 0 means that no writing slow down will be triggered by number
                        // of files in level-0.
                        .setLevel0SlowdownWritesTrigger(80)
                        // Maximum number of level-0 files.  We stop writes at this point.
                        .setLevel0StopWritesTrigger(100)
                        // Flushing options:
                        // max_write_buffer_number sets the maximum number of mem_tables, both active
                        // and immutable.  If the active mem_table fills up and the total number of
                        // mem_tables is larger than max_write_buffer_number we stall further writes.
                        // This may happen if the flush process is slower than the write rate.
                        .setMaxWriteBufferNumber(4)
                        .setMinWriteBufferNumberToMerge(3);
                }
            }).setDbCheckpointRootDir(Paths.get(dbRootDir.toString(), uuid, DB_CHECKPOINT_DIR).toString())
                .setDbRootDir(Paths.get(dbRootDir.toString(), uuid, DB_NAME).toString());
        kvEngine = KVEngineFactory.create(null, configurator);
    }

    @Setup(Level.Trial)
    public void setup() {
        kvEngine.start();
        afterSetup();
        log.info("Setup finished, and start testing");
    }

    protected abstract void afterSetup();

    @TearDown(Level.Trial)
    public void teardown() {
        beforeTeardown();
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

    protected abstract void beforeTeardown();
}
