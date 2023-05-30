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

package com.baidu.bifromq.dist.server;

public class RunningAverage {
    private final int[] window;
    private int total;
    private int count;
    private int estimate;

    public RunningAverage(int windowSize) {
        assert windowSize > 0;
        window = new int[windowSize];
    }

    public synchronized void log(int value) {
        int idx = count++ % window.length;
        int dropped = window[idx];
        window[idx] = value;
        total += value - dropped;
        estimate = total / Math.min(count, window.length);
    }

    public int estimate() {
        return estimate;
    }

}
