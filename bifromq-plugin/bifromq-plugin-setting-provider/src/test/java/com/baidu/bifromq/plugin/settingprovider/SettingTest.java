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

package com.baidu.bifromq.plugin.settingprovider;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.Test;

public class SettingTest {
    private final String tenantId = "tenantA";

    @Test
    public void enumInitialValue() {
        for (Setting setting : Setting.values()) {
            assertTrue(setting.isValid(setting.current(tenantId)));
        }
    }

    @Test
    public void customValueExpiry() {
        assertEquals((int) Setting.MaxTopicLevels.current(tenantId), 16);
        AtomicInteger calls = new AtomicInteger();
        AtomicReference<String> tenantIdRef = new AtomicReference<>(null);
        Setting.MaxTopicLevels.setProvider(new ISettingProvider() {
            @Override
            public <R> R provide(Setting setting, String tenantId) {
                tenantIdRef.set(tenantId);
                if (calls.getAndIncrement() == 0) {
                    Integer levels = 32;
                    return (R) levels;
                } else {
                    Integer levels = 48;
                    return (R) levels;
                }
            }
        });
        Setting.MaxTopicLevels.currentVals.invalidateAll();

        assertEquals((int) Setting.MaxTopicLevels.current(tenantId), 32);
        assertEquals(tenantIdRef.get(), tenantId);

        Setting.MaxTopicLevels.currentVals.invalidateAll();

        assertEquals((int) Setting.MaxTopicLevels.current(tenantId), 48);
        assertEquals(tenantIdRef.get(), tenantId);
    }

    @Test
    public void systemPropertyOverride() {
        System.setProperty("MsgPubPerSec", "100");
        assertEquals((int) Setting.MsgPubPerSec.resolve(200), 100);

        // invalid value should be ignored
        System.setProperty("MsgPubPerSec", "sdfa");
        assertEquals((int) Setting.MsgPubPerSec.resolve(200), 200);
    }
}
