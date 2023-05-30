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

package com.baidu.bifromq.mqtt.utils;

import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import java.time.Clock;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

public final class TestTimer implements Timer {
    private static class TimeoutImpl implements Timeout {
        final TimerTask timerTask;
        final Timer timer;
        volatile boolean expired = false;
        volatile boolean cancelled = false;

        TimeoutImpl(TimerTask timerTask, Timer timer) {
            this.timerTask = timerTask;
            this.timer = timer;
        }

        @Override
        public Timer timer() {
            return timer;
        }

        @Override
        public TimerTask task() {
            return timerTask;
        }

        @Override
        public boolean isExpired() {
            return expired;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public boolean cancel() {
            return cancelled = true;
        }

        public void expire() {
            try {
                timerTask.run(this);
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                expired = true;
            }
        }
    }

    TreeMap<Long, TimeoutImpl> tasks = new TreeMap<>();
    volatile long timeShift = 0;

    @Override
    public Timeout newTimeout(TimerTask task, long delay, TimeUnit unit) {
        TimeoutImpl timeout = new TimeoutImpl(task, this);
        tasks.put(Clock.systemUTC().millis() + timeShift + unit.toMillis(delay), timeout);
        return timeout;
    }

    @Override
    public Set<Timeout> stop() {
        throw new UnsupportedOperationException();
    }

    public void advanceBy(long duration, TimeUnit unit) {
        long deadline = Clock.systemUTC().millis() + timeShift + unit.toMillis(duration);
        timeShift += unit.toMillis(duration);
        Set<Long> expired = new HashSet<>(tasks.navigableKeySet().headSet(deadline));
        for (Long key : expired) {
            TimeoutImpl timeout = tasks.get(key);
            if (!timeout.isCancelled() && !timeout.isExpired()) {
                tasks.get(key).expire();
            }
        }
        tasks.keySet().removeAll(expired);
    }
}
