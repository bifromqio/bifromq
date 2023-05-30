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


import static com.baidu.bifromq.basekv.localengine.TestUtil.toByteString;

import com.baidu.bifromq.basekv.localengine.IKVEngine;
import com.baidu.bifromq.basekv.localengine.IKVEngineIterator;
import com.google.protobuf.ByteString;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

@State(Scope.Benchmark)
public class ContinuousKeySingleDeleteAndSeekState extends BenchmarkState {
    int keyCount = 1000000;
    ByteString key = ByteString.copyFromUtf8("key");
    IKVEngineIterator itr;
    int rangeId;

    @Override
    protected void afterSetup() {
        int rangeId = kvEngine.registerKeyRange(IKVEngine.DEFAULT_NS, null, null);
        int batchId = kvEngine.startBatch();
        for (int i = 0; i < keyCount; i++) {
            kvEngine.put(batchId, rangeId, key.concat(toByteString(i)), ByteString.EMPTY);
            kvEngine.delete(batchId, rangeId, key.concat(toByteString(i)));
        }
        kvEngine.put(batchId, rangeId, key.concat(toByteString(keyCount)), ByteString.EMPTY);
        kvEngine.endBatch(batchId);
        itr = kvEngine.newIterator(rangeId);
    }

    @Override
    protected void beforeTeardown() {

    }
}
