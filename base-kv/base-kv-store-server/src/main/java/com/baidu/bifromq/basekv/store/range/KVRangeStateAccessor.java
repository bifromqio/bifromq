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

package com.baidu.bifromq.basekv.store.range;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KVRangeStateAccessor {
    private final AtomicLong stateModVer = new AtomicLong();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    KVRangeWriterMutator mutator() {
        return new KVRangeWriterMutator();
    }

    KVRangeReaderRefresher refresher() {
        return new KVRangeReaderRefresher();
    }

    public class KVRangeReaderRefresher {
        private final Lock rLock;
        private long readVer;

        KVRangeReaderRefresher() {
            this.rLock = rwLock.readLock();
            this.readVer = stateModVer.get();
        }

        void run(Runnable refresh) {
            rLock.lock();
            try {
                if (readVer == stateModVer.get()) {
                    return;
                }
                readVer = stateModVer.get();
                refresh.run();
            } finally {
                rLock.unlock();
            }
        }

        void lock() {
            rLock.lock();
        }

        void unlock() {
            rLock.unlock();
        }
    }

    public class KVRangeWriterMutator {
        private final Lock wLock;

        KVRangeWriterMutator() {
            this.wLock = rwLock.writeLock();
        }

        void run(Runnable mutation) {
            wLock.lock();
            try {
                mutation.run();
                stateModVer.incrementAndGet();
            } finally {
                wLock.unlock();
            }
        }

    }
}
