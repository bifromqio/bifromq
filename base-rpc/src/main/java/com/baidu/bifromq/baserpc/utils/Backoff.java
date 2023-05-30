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

package com.baidu.bifromq.baserpc.utils;

import java.util.concurrent.ThreadLocalRandom;

public class Backoff {
    private final int multiplier;
    private final long base;
    private final long max;
    private int n = 0;

    public Backoff(int multiplier, long base, long max) {
        assert multiplier > 0;
        assert base > 0;
        this.multiplier = multiplier;
        this.base = base;
        this.max = max;
    }

    public synchronized long backoff() {
        double c = Math.pow(multiplier, n++);
        if (Double.isInfinite(c) || Double.isNaN(c)) {
            return max;
        }
        long b = (long) (base * c) + ThreadLocalRandom.current().nextLong(0, base);
        if (b <= 0) {
            return max;
        }
        return Math.min(b, max);
    }

    public synchronized void reset() {
        n = 0;
    }
}
