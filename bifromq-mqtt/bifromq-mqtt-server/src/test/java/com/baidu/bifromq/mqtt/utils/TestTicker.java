/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.mqtt.utils;

import com.baidu.bifromq.mqtt.session.ITicker;
import java.util.concurrent.TimeUnit;

public class TestTicker implements ITicker {
    private long nanos;
    private long millis;

    public TestTicker() {
        reset();
    }

    public void reset() {
        nanos = System.nanoTime();
        millis = System.currentTimeMillis();
    }

    @Override
    public long systemNanos() {
        // read different timestamp
        return ++nanos;
    }

    @Override
    public long nowMillis() {
        return ++millis;
    }

    public void advanceTimeBy(long duration, TimeUnit unit) {
        nanos += unit.toNanos(duration);
        millis += unit.toMillis(duration);
    }
}
