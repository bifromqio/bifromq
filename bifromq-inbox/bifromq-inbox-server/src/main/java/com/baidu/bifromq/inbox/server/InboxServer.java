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

import com.baidu.bifromq.inbox.RPCBluePrint;
import com.baidu.bifromq.inbox.server.scheduler.InboxAttachScheduler;
import com.baidu.bifromq.inbox.server.scheduler.InboxCommitScheduler;
import com.baidu.bifromq.inbox.server.scheduler.InboxCreateScheduler;
import com.baidu.bifromq.inbox.server.scheduler.InboxDeleteScheduler;
import com.baidu.bifromq.inbox.server.scheduler.InboxDetachScheduler;
import com.baidu.bifromq.inbox.server.scheduler.InboxFetchScheduler;
import com.baidu.bifromq.inbox.server.scheduler.InboxGetScheduler;
import com.baidu.bifromq.inbox.server.scheduler.InboxInsertScheduler;
import com.baidu.bifromq.inbox.server.scheduler.InboxSubScheduler;
import com.baidu.bifromq.inbox.server.scheduler.InboxTouchScheduler;
import com.baidu.bifromq.inbox.server.scheduler.InboxUnSubScheduler;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class InboxServer implements IInboxServer {
    protected final InboxService inboxService;

    InboxServer(InboxServerBuilder builder) {
        this.inboxService = InboxService.builder()
            .eventCollector(builder.eventCollector)
            .resourceThrottler(builder.resourceThrottler)
            .settingProvider(builder.settingProvider)
            .inboxClient(builder.inboxClient)
            .distClient(builder.distClient)
            .retainClient(builder.retainClient)
            .inboxStoreClient(builder.inboxStoreClient)
            .getScheduler(new InboxGetScheduler(builder.inboxStoreClient))
            .fetchScheduler(new InboxFetchScheduler(builder.inboxStoreClient))
            .insertScheduler(new InboxInsertScheduler(builder.inboxStoreClient))
            .commitScheduler(new InboxCommitScheduler(builder.inboxStoreClient))
            .createScheduler(new InboxCreateScheduler(builder.inboxStoreClient))
            .attachScheduler(new InboxAttachScheduler(builder.inboxStoreClient))
            .detachScheduler(new InboxDetachScheduler(builder.inboxStoreClient))
            .deleteScheduler(new InboxDeleteScheduler(builder.inboxStoreClient))
            .subScheduler(new InboxSubScheduler(builder.inboxStoreClient))
            .unsubScheduler(new InboxUnSubScheduler(builder.inboxStoreClient))
            .touchScheduler(new InboxTouchScheduler(builder.inboxStoreClient))
            .build();
        builder.rpcServerBuilder.bindService(inboxService.bindService(),
            RPCBluePrint.INSTANCE,
            builder.attrs,
            builder.defaultGroupTags,
            builder.rpcExecutor);
        start();
    }

    private void start() {
        log.debug("Starting inbox service");
        inboxService.start();
    }

    @SneakyThrows
    @Override
    public void close() {
        log.debug("Stopping inbox service");
        inboxService.stop();
    }
}
