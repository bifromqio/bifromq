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

import com.baidu.bifromq.basekv.localengine.IKVEngineIterator;
import com.google.protobuf.ByteString;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

@Slf4j
@State(Scope.Benchmark)
public class HybridWorkloadState extends BenchmarkState {
    int rangeId;
    ConcurrentHashMap<Long, IKVEngineIterator> itrMap = new ConcurrentHashMap<>();

    @Override
    protected void afterSetup() {
        rangeId = kvEngine.registerKeyRange(DEFAULT_NS, null, null);
//        itr = kvEngine.newIterator(rangeId);
    }


    @Override
    protected void beforeTeardown() {
//        itr.close();
        itrMap.values().forEach(IKVEngineIterator::close);
        kvEngine.unregisterKeyRange(rangeId);
    }

    public void randomPut() {
        kvEngine.put(rangeId, randomBS(), randomBS());
    }

    public void randomDelete() {
        kvEngine.delete(rangeId, randomBS());
    }

    public void randomPutAndDelete() {
        ByteString key = randomBS();
        int batchId = kvEngine.startBatch();
        kvEngine.put(batchId, rangeId, key, randomBS());
        kvEngine.delete(batchId, rangeId, key);
        kvEngine.endBatch(batchId);
    }

    public Optional<ByteString> randomGet() {
        return kvEngine.get(rangeId, randomBS());
    }

    public boolean randomExist() {
        return kvEngine.exist(rangeId, randomBS());
    }

    public void seekToFirst() {
        IKVEngineIterator itr = itrMap.computeIfAbsent(Thread.currentThread().getId(),
            k -> kvEngine.newIterator(rangeId));
        itr.refresh();
        itr.seekToFirst();
    }

    public void randomSeek() {
        IKVEngineIterator itr = itrMap.computeIfAbsent(Thread.currentThread().getId(),
            k -> kvEngine.newIterator(rangeId));
        itr.refresh();
        itr.seek(randomBS());
    }

    private ByteString randomBS() {
        return ByteString.copyFromUtf8(ThreadLocalRandom.current().nextInt(0, 1000000000) + "");
    }
}
