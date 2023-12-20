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

package com.baidu.bifromq.inbox.server;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
abstract class AbstractInboxServer implements IInboxServer {
    protected final InboxService inboxService;

    AbstractInboxServer(AbstractInboxServerBuilder<?> builder) {
        this.inboxService = new InboxService(builder.settingProvider, builder.distClient,
            builder.inboxStoreClient, builder.bgTaskExecutor);
    }

    @Override
    public void start() {
        log.debug("Starting inbox service");
        inboxService.start();
    }

    @SneakyThrows
    @Override
    public void shutdown() {
        log.debug("Stopping inbox service");
        inboxService.stop();
    }
}
