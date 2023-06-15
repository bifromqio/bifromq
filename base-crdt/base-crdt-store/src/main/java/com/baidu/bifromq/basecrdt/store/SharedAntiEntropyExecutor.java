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

package com.baidu.bifromq.basecrdt.store;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

class SharedAntiEntropyExecutor {
    private static ScheduledExecutorService instance;

    static synchronized ScheduledExecutorService getInstance() {
        if (instance == null) {
            instance = new ScheduledThreadPoolExecutor(
                Math.max(2, Runtime.getRuntime().availableProcessors() / 10),
                new ThreadFactoryBuilder().setNameFormat("shared-crdt-store-%d").setDaemon(true).build());
        }
        return instance;
    }
}
