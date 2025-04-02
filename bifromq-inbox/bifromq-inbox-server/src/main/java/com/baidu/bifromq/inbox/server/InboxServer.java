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

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.inbox.RPCBluePrint;
import com.baidu.bifromq.inbox.server.scheduler.InboxAttachScheduler;
import com.baidu.bifromq.inbox.server.scheduler.InboxCheckSubScheduler;
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
import com.google.common.util.concurrent.MoreExecutors;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class InboxServer implements IInboxServer {
    private final InboxService inboxService;
    private final ExecutorService rpcExecutor;

    InboxServer(InboxServerBuilder builder) {
        this.inboxService = InboxService.builder()
            .idleSeconds(builder.idleSeconds)
            .eventCollector(builder.eventCollector)
            .resourceThrottler(builder.resourceThrottler)
            .settingProvider(builder.settingProvider)
            .inboxClient(builder.inboxClient)
            .distClient(builder.distClient)
            .retainClient(builder.retainClient)
            .inboxStoreClient(builder.inboxStoreClient)
            .getScheduler(new InboxGetScheduler(builder.inboxStoreClient))
            .checkSubScheduler(new InboxCheckSubScheduler(builder.inboxStoreClient))
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
        if (builder.workerThreads == 0) {
            rpcExecutor = MoreExecutors.newDirectExecutorService();
        } else {
            rpcExecutor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
                new ThreadPoolExecutor(builder.workerThreads,
                    builder.workerThreads, 0L,
                    TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
                    EnvProvider.INSTANCE.newThreadFactory("inbox-server-executor")), "inbox-server-executor");
        }
        builder.rpcServerBuilder.bindService(inboxService.bindService(),
            RPCBluePrint.INSTANCE,
            builder.attributes,
            builder.defaultGroupTags,
            rpcExecutor);
        start();
    }

    private void start() {
        log.debug("Starting inbox service");
        inboxService.start();
    }

    @SneakyThrows
    @Override
    public void close() {
        log.info("Stopping InboxService");
        inboxService.stop();
        MoreExecutors.shutdownAndAwaitTermination(rpcExecutor, 5, TimeUnit.SECONDS);
        log.info("InboxService stopped");
    }
}
