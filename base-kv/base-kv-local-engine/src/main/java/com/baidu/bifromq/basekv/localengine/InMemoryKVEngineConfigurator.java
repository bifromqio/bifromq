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

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(chain = true)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class InMemoryKVEngineConfigurator implements KVEngineConfigurator<InMemoryKVEngineConfigurator> {
    private long gcIntervalInSec = 300; // ms

    public static InMemoryKVEngineConfiguratorBuilder builder() {
        return new InMemoryKVEngineConfigurator().toBuilder();
    }

    public InMemoryKVEngineConfiguratorBuilder toBuilder() {
        return new InMemoryKVEngineConfiguratorBuilder().gcInterval(this.gcIntervalInSec);
    }

    public static class InMemoryKVEngineConfiguratorBuilder implements
        KVEngineConfiguratorBuilder<InMemoryKVEngineConfigurator> {
        private long gcInterval;

        InMemoryKVEngineConfiguratorBuilder() {
        }

        public InMemoryKVEngineConfiguratorBuilder gcInterval(long gcInterval) {
            this.gcInterval = gcInterval;
            return this;
        }

        public InMemoryKVEngineConfigurator build() {
            return new InMemoryKVEngineConfigurator(gcInterval);
        }

        public String toString() {
            return "InMemoryKVEngineConfigurator.InMemoryKVEngineConfiguratorBuilder(gcInterval="
                + this.gcInterval + ")";
        }
    }
}
