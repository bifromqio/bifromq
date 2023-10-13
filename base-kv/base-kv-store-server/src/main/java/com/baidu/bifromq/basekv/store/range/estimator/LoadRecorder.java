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

package com.baidu.bifromq.basekv.store.range.estimator;

import com.baidu.bifromq.basekv.store.range.ILoadTracker;
import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

class LoadRecorder implements ILoadTracker.ILoadRecorder {
    private final long startNanos;
    private final Map<ByteString, Long> loadDistribution = new HashMap<>();
    private long kvIONanos = 0L;
    private int kvIOs = 0;
    private final Consumer<LoadRecorder> onStop;

    public LoadRecorder(Supplier<Long> nanoSource, Consumer<LoadRecorder> onStop) {
        startNanos = nanoSource.get();
        this.onStop = onStop;
    }

    @Override
    public long startNanos() {
        return startNanos;
    }

    @Override
    public int getKVIOs() {
        return kvIOs;
    }

    @Override
    public long getKVIONanos() {
        return kvIONanos;
    }

    @Override
    public Map<ByteString, Long> keyDistribution() {
        return loadDistribution;
    }

    @Override
    public void record(ByteString key, long latencyNanos) {
        loadDistribution.compute(key, (k, v) -> v == null ? latencyNanos : v + latencyNanos);
        kvIOs++;
        kvIONanos += latencyNanos;
    }

    @Override
    public void record(long latencyNanos) {
        kvIONanos += latencyNanos;
        kvIOs++;
    }

    @Override
    public void stop() {
        onStop.accept(this);
    }
}
