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

import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.WorkHandler;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class ProvideCallHandler implements WorkHandler<ProvideCallHolder>, LifecycleAware {
    private final String id;
    private final ISettingProvider provider;

    private final Timer provideCallTimer;
    private final Counter provideCallErrorCounter;

    ProvideCallHandler(ISettingProvider provider) {
        this.id = provider.getClass().getName();
        this.provider = provider;

        provideCallTimer = Timer.builder("call.exec.timer")
            .tag("method", "SettingProvider/provide")
            .tag("type", id)
            .register(Metrics.globalRegistry);
        provideCallErrorCounter = Counter.builder("call.exec.fail.count")
            .tag("method", "SettingProvider/provide")
            .tag("type", id)
            .register(Metrics.globalRegistry);
    }

    @Override
    public void onStart() {
        log.debug("ProvideSetting call handler start");
    }

    @Override
    public void onShutdown() {
        log.debug("ProvideSetting call handler shutdown");
        provider.close();
        Metrics.globalRegistry.remove(provideCallTimer);
        Metrics.globalRegistry.remove(provideCallErrorCounter);
    }

    @Override
    public void onEvent(ProvideCallHolder holder) {
        Object current = holder.setting.current(holder.clientInfo);
        try {
            Timer.Sample sample = Timer.start();
            Object newVal = provider.provide(holder.setting, holder.clientInfo);
            sample.stop(provideCallTimer);
            if (holder.setting.isValid(newVal)) {
                // update the value
                holder.setting.current(holder.clientInfo, newVal);
            } else {
                log.error("Invalid setting value: setting={}, value={}", holder.setting.name(), newVal);
            }
        } catch (Throwable e) {
            log.error("Setting provider throws exception: setting={}", holder.setting.name(), e);
            // keep current value in case provider throws
            holder.setting.current(holder.clientInfo, current);
            provideCallErrorCounter.increment();
        } finally {
            holder.reset();
        }
    }
}
