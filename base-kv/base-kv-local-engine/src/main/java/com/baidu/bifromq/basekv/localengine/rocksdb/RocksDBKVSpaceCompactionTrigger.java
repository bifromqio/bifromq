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

package com.baidu.bifromq.basekv.localengine.rocksdb;

import java.util.concurrent.atomic.AtomicInteger;

public class RocksDBKVSpaceCompactionTrigger {
    private final AtomicInteger totalKeyCount = new AtomicInteger();
    private final AtomicInteger totalTombstoneKeyCount = new AtomicInteger();
    private final AtomicInteger totalTombstoneRangeCount = new AtomicInteger();

    private final int minTombstoneKeysTrigger;
    private final int minTombstoneRangesTrigger;
    private final double minTombstoneKeysRatioTrigger;
    private final Runnable compact;

    public RocksDBKVSpaceCompactionTrigger(int minTombstoneKeysTrigger,
                                           int minTombstoneRangesTrigger,
                                           double minTombstoneKeysRatioTrigger,
                                           Runnable compact) {
        this.minTombstoneKeysTrigger = minTombstoneKeysTrigger;
        this.minTombstoneRangesTrigger = minTombstoneRangesTrigger;
        this.minTombstoneKeysRatioTrigger = minTombstoneKeysRatioTrigger;
        this.compact = compact;
    }

    WriteStatsRecorder newRecorder() {
        return new WriteStatsRecorder();
    }

    void reset() {
        totalTombstoneKeyCount.set(0);
        totalKeyCount.set(0);
    }

    class WriteStatsRecorder {
        private final AtomicInteger keyCount = new AtomicInteger();
        private final AtomicInteger tombstoneKeyCount = new AtomicInteger();
        private final AtomicInteger tombstoneRangeCount = new AtomicInteger();


        void recordPut() {
            keyCount.incrementAndGet();
            tombstoneKeyCount.incrementAndGet();
        }

        void recordInsert() {
            keyCount.incrementAndGet();
        }

        void recordDelete() {
            tombstoneKeyCount.incrementAndGet();
        }

        void recordDeleteRange() {
            tombstoneRangeCount.incrementAndGet();
        }

        void stop() {
            int totalKeys = totalKeyCount.addAndGet(keyCount.get());
            int totalTombstones = totalTombstoneKeyCount.addAndGet(tombstoneKeyCount.get());
            int totalRangeTombstones = totalTombstoneRangeCount.addAndGet(tombstoneRangeCount.get());
            if (totalRangeTombstones > minTombstoneRangesTrigger || (totalTombstones > minTombstoneKeysTrigger &&
                (double) totalTombstones / (totalKeys + totalTombstones) >= minTombstoneKeysRatioTrigger)) {
                totalKeyCount.addAndGet(-keyCount.get());
                totalTombstoneKeyCount.addAndGet(-tombstoneKeyCount.get());
                totalTombstoneRangeCount.addAndGet(-tombstoneRangeCount.get());
                compact.run();
            }
        }
    }
}
