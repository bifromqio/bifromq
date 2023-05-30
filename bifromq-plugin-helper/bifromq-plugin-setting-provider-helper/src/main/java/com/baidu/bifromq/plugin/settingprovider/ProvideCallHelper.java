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

package com.baidu.bifromq.plugin.settingprovider;

import static com.baidu.bifromq.baseutils.ThreadUtil.threadFactory;

import com.baidu.bifromq.type.ClientInfo;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class ProvideCallHelper {
    private final Disruptor<ProvideCallHolder> disruptor;
    private final RingBuffer<ProvideCallHolder> ringBuffer;
    private final ProvideCallHandler[] provideCallHandlers;
    private final Counter callInvokeCounter;
    private final Gauge callBufferCapacityGauge;

    public ProvideCallHelper(int bufferSize, int workerThreads, ISettingProvider provider) {
        assert bufferSize > 0 && ((bufferSize & (bufferSize - 1)) == 0);
        disruptor = new Disruptor<>(ProvideCallHolder::new, bufferSize,
            threadFactory("provide-call-helper", true),
            ProducerType.MULTI, new BlockingWaitStrategy());
        provideCallHandlers = new ProvideCallHandler[workerThreads];
        for (int i = 0; i < workerThreads; i++) {
            provideCallHandlers[i] = new ProvideCallHandler(provider);
        }
        disruptor.handleEventsWithWorkerPool(provideCallHandlers);
        ringBuffer = disruptor.start();

        callInvokeCounter = Counter.builder("call.invoke.count")
            .tag("method", "SettingProvider/provide")
            .register(Metrics.globalRegistry);
        callBufferCapacityGauge = Gauge.builder("call.buffer.capacity.gauge",
                ringBuffer::remainingCapacity)
            .tag("method", "SettingProvider/provide")
            .register(Metrics.globalRegistry);
    }

    public <R> R provide(Setting setting, ClientInfo clientInfo) {
        R current = setting.current(clientInfo);
        // trigger async refresh
        callInvokeCounter.increment();
        if (!ringBuffer.tryPublishEvent((holder, seq, arg0, arg1) -> holder.hold(arg0, arg1), setting, clientInfo)) {
            setting.current(clientInfo, current);
        }
        return current;
    }

    public void close() {
        disruptor.shutdown();
        Metrics.globalRegistry.remove(callInvokeCounter);
        Metrics.globalRegistry.remove(callBufferCapacityGauge);
    }
}
