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

package com.baidu.bifromq.plugin.inboxbroker;

import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.common.base.Preconditions;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class InboxBrokerSender implements IInboxBroker {
    private final AtomicBoolean hasStopped = new AtomicBoolean();
    private final IInboxBroker delegate;
    private final Timer hasInboxCallTimer;
    private final Timer writeCallTimer;

    InboxBrokerSender(IInboxBroker delegate) {
        this.delegate = delegate;
        hasInboxCallTimer = Timer.builder("ib.hasinbox.finish.time")
            .tag("type", delegate.getClass().getName())
            .register(Metrics.globalRegistry);
        writeCallTimer = Timer.builder("ib.write.finish.time")
            .tag("type", delegate.getClass().getName())
            .register(Metrics.globalRegistry);
    }

    @Override
    public int id() {
        return delegate.id();
    }

    @Override
    public IInboxWriter openInboxWriter(String inboxGroupKey) {
        Preconditions.checkState(!hasStopped.get());
        return new MonitoredInboxWriter(inboxGroupKey);
    }

    @Override
    public CompletableFuture<HasResult> hasInbox(long reqId,
                                                 @NonNull String trafficId,
                                                 @NonNull String inboxId,
                                                 @Nullable String inboxGroupKey) {
        Preconditions.checkState(!hasStopped.get());
        try {
            Timer.Sample start = Timer.start();
            return delegate.hasInbox(reqId, trafficId, inboxId, inboxGroupKey)
                .exceptionally(HasResult::error)
                .whenComplete((v, e) -> start.stop(hasInboxCallTimer));
        } catch (Throwable e) {
            return CompletableFuture.completedFuture(HasResult.error(e));
        }
    }


    @Override
    public void close() {
        if (hasStopped.compareAndSet(false, true)) {
            delegate.close();
            Metrics.globalRegistry.remove(hasInboxCallTimer);
            Metrics.globalRegistry.remove(writeCallTimer);
        }
    }

    private class MonitoredInboxWriter implements IInboxWriter {
        private final IInboxWriter inboxWriter;

        MonitoredInboxWriter(String inboxGroupKey) {
            inboxWriter = delegate.openInboxWriter(inboxGroupKey);
        }

        @Override
        public CompletableFuture<Map<SubInfo, WriteResult>> write(Map<TopicMessagePack, List<SubInfo>> messages) {
            try {
                Timer.Sample start = Timer.start();
                return inboxWriter.write(messages).whenComplete((v, e) -> start.stop(writeCallTimer));
            } catch (Throwable e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        @Override
        public void close() {
            inboxWriter.close();
        }
    }
}
