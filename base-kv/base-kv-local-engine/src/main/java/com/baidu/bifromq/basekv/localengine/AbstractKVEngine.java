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

package com.baidu.bifromq.basekv.localengine;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractKVEngine<T extends IKVSpace> implements IKVEngine<T> {
    protected enum State {
        INIT, STARTING, STARTED, FATAL_FAILURE, STOPPING, STOPPED
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.INIT);
    protected final String overrideIdentity;
    private Gauge gauge;

    public AbstractKVEngine(String overrideIdentity) {
        this.overrideIdentity = overrideIdentity;
    }

    protected State state() {
        return state.get();
    }

    @Override
    public void start(String... metricTags) {
        if (state.compareAndSet(State.INIT, State.STARTING)) {
            try {
                doStart(metricTags);
                state.set(State.STARTED);
                gauge = Gauge.builder("basekv.le.ranges", this.spaces()::size)
                    .tags(metricTags)
                    .register(Metrics.globalRegistry);
                afterStart();
            } catch (Throwable e) {
                state.set(State.FATAL_FAILURE);
                throw e;
            }
        }
    }

    protected abstract void doStart(String... metricTags);

    protected void afterStart() {

    }

    @Override
    public void stop() {
        assertStarted();
        if (state.compareAndSet(State.STARTED, State.STOPPING)) {
            try {
                doStop();
                Metrics.globalRegistry.remove(gauge);
            } finally {
                state.set(State.STOPPED);
            }
        }
    }

    protected abstract void doStop();

    protected void assertStarted() {
        assert state.get() == State.STARTED : "Not started";
    }
}
