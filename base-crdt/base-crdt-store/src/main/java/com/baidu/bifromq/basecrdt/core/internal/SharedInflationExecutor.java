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

package com.baidu.bifromq.basecrdt.core.internal;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.baseenv.IEnvProvider;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

class SharedInflationExecutor {
    private static ScheduledExecutorService instance;

    static synchronized ScheduledExecutorService getInstance() {
        if (instance == null) {
            IEnvProvider envProvider = EnvProvider.INSTANCE;
            instance = new ScheduledThreadPoolExecutor(Math.max(2, envProvider.availableProcessors() / 20),
                envProvider.newThreadFactory("shared-crdt-inflater", true));
        }
        return instance;
    }
}
