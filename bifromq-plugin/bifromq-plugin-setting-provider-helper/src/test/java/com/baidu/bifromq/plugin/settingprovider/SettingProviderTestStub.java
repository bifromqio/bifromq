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

import com.baidu.bifromq.type.ClientInfo;
import java.util.HashMap;
import java.util.Map;
import org.pf4j.Extension;

@Extension
public class SettingProviderTestStub implements ISettingProvider {
    private final Map<Setting, Object> settings = new HashMap<>();

    @Override
    public <R> R provide(Setting setting, ClientInfo clientInfo) {
        return settings.containsKey(setting) ? (R) settings.get(setting) : setting.current(clientInfo);
    }

    public void setValue(Setting setting, Object newVal) {
        settings.put(setting, newVal);
    }
}
