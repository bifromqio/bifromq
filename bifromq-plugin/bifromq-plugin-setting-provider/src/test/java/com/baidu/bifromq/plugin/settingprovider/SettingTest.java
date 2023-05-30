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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.baidu.bifromq.type.ClientInfo;
import org.junit.Test;

public class SettingTest {
    @Test
    public void enumInitialValue() {
        for (Setting setting : Setting.values()) {
            assertTrue(setting.isValid(setting.current(ClientInfo.getDefaultInstance())));
        }
    }

    @Test
    public void customClassifier() {
        ClientInfo clientInfo = ClientInfo.newBuilder().setTrafficId("abc").setUserId("123").build();
        Setting.MaxTopicLevels.current(clientInfo, 32);
        assertTrue(Setting.MaxTopicLevels.current(clientInfo).equals(32));

        Setting.MaxTopicLevels.setClientClassifier(c -> c.getTrafficId());
        assertTrue(Setting.MaxTopicLevels.current(clientInfo).equals(16));

        Setting.MaxTopicLevels.current(clientInfo, 32);
        assertTrue(Setting.MaxTopicLevels.current(clientInfo).equals(32));
    }

    @Test
    public void customValueExpiry() {
        ClientInfo clientInfo = ClientInfo.newBuilder().setTrafficId("abc").setUserId("123").build();
        Setting.MaxTopicLevels.current(clientInfo, 32);

        Setting.MaxTopicLevels.currentVals.invalidateAll();

        assertTrue(Setting.MaxTopicLevels.current(clientInfo).equals(16));
    }

    @Test
    public void systemPropertyOverride() {
        System.setProperty("MsgPubPerSec", "100");
        assertEquals(100, (int) Setting.MsgPubPerSec.resolve(200));

        System.setProperty("MsgPubPerSec", "sdfa");
        assertEquals(200, (int) Setting.MsgPubPerSec.resolve(200));
    }
}
