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
import org.checkerframework.checker.nullness.qual.Nullable;
import org.pf4j.ExtensionPoint;

public interface ISettingProvider extends ExtensionPoint {
    /**
     * Provide a value of the setting for given client. The method will be called in the same thread of delivering message,
     * so it's expected to be performant and non-blocking otherwise the messaging performance will be greatly impacted.
     * It's allowed to return null to reuse the current setting value, in case the value could not be determined in timely manner.
     *
     * @param setting    the setting for the client
     * @param clientInfo the client
     * @return the setting value for the client or null
     */
    @Nullable <R> R provide(Setting setting, ClientInfo clientInfo);

    /**
     * This method will be called during broker shutdown
     */
    default void close() {

    }
}
