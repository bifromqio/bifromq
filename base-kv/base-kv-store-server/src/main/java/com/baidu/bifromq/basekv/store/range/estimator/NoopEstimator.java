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

import com.baidu.bifromq.basekv.proto.SplitHint;
import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class NoopEstimator implements ISplitKeyEstimator {
    public static final ISplitKeyEstimator INSTANCE = new NoopEstimator();
    private static final ILoadRecorder DUMMY_RECORDER = new ILoadRecorder() {
        @Override
        public long startNanos() {
            return 0;
        }

        @Override
        public int getKVIOs() {
            return 0;
        }

        @Override
        public long getKVIONanos() {
            return 0;
        }

        @Override
        public Map<ByteString, Long> keyDistribution() {
            return Collections.emptyMap();
        }

        @Override
        public void record(ByteString key, long latencyNanos) {

        }

        @Override
        public void record(long latencyNanos) {

        }

        @Override
        public void stop() {

        }
    };

    @Override
    public ILoadRecorder start() {
        return DUMMY_RECORDER;
    }

    @Override
    public SplitHint estimate() {
        return SplitHint.getDefaultInstance();
    }
}
