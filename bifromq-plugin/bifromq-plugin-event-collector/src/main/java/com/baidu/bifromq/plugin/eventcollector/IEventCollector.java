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

package com.baidu.bifromq.plugin.eventcollector;

import org.pf4j.ExtensionPoint;

public interface IEventCollector extends ExtensionPoint {
    /**
     * Implement this method to receive various events at runtime.
     * <p/>
     * Note: To reduce memory pressure, the passed event object is shared in the calling thread, and will be reused
     * afterwards. Make a clone if implementation's logic needs to pass it to another thread for processing.
     *
     * @param event
     * @param <T>
     */
    <T extends Event> void report(T event);

    /**
     * This method will be called during broker shutdown
     */
    default void close() {
    }
}
