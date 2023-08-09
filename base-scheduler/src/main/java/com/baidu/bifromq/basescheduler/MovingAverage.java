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

package com.baidu.bifromq.basescheduler;

import com.netflix.concurrency.limits.internal.Preconditions;
import java.time.Duration;

public class MovingAverage {
    private final long freshness;
    private final Stat[] observation;
    private long total;
    private int head;
    private int tail;
    private volatile long lastObserveTs;
    private volatile long avg;
    private volatile long max;

    public MovingAverage(int precision, Duration freshness) {
        Preconditions.checkArgument(precision > 0, "Precision must be positive");
        observation = new Stat[precision + 1]; // one more for sentinel
        for (int i = 0; i < observation.length; i++) {
            observation[i] = new Stat();
        }
        this.freshness = freshness.toNanos();
    }

    public synchronized void observe(long read) {
        lastObserveTs = System.nanoTime();
        observation[head].ts = lastObserveTs;
        observation[head].read = read;
        total += read;
        head = (head + 1) % observation.length;
        if (head == tail) {
            // make room for head
            total -= observation[head].read;
            tail = (tail + 1) % observation.length;
        }
        while (lastObserveTs - observation[tail].ts > freshness) {
            // remove staled observation
            total -= observation[tail].read;
            tail = (tail + 1) % observation.length;
        }
        avg = total / ((head - tail + observation.length) % observation.length);
        max = Math.max(max, read);
    }

    public long max() {
        long now = System.nanoTime();
        if (now - lastObserveTs > freshness) {
            return 0;
        } else {
            return max;
        }
    }

    public long estimate() {
        long now = System.nanoTime();
        if (now - lastObserveTs > freshness) {
            return 0;
        } else {
            return avg;
        }
    }

    private static class Stat {
        private long ts;
        private long read;
    }
}
