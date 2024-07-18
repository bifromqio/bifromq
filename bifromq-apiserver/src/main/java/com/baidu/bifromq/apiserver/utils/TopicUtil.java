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

package com.baidu.bifromq.apiserver.utils;

import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxTopicLength;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxTopicLevelLength;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxTopicLevels;
import static com.baidu.bifromq.util.TopicUtil.isValidTopicFilter;

import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.sysprops.props.SanityCheckMqttUtf8String;
import com.baidu.bifromq.util.UTF8Util;

public class TopicUtil {
    public static boolean checkTopicFilter(String topic, String tenantId, ISettingProvider settingProvider) {
        int maxTopicLevelLength = settingProvider.provide(MaxTopicLevelLength, tenantId);
        int maxTopicLevels = settingProvider.provide(MaxTopicLevels, tenantId);
        int maxTopicLength = settingProvider.provide(MaxTopicLength, tenantId);
        return UTF8Util.isWellFormed(topic, SanityCheckMqttUtf8String.INSTANCE.get())
            && isValidTopicFilter(topic, maxTopicLevelLength, maxTopicLevels, maxTopicLength);
    }
}
