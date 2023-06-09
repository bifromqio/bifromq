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

package com.baidu.bifromq.dist.worker.scheduler;

import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DIST_MAX_BATCH_SEND_MESSAGES;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DIST_WORKER_MAX_INFLIGHT_CALLS_PER_QUEUE;

import com.baidu.bifromq.basescheduler.BatchCallBuilder;
import com.baidu.bifromq.basescheduler.BatchCallScheduler;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.plugin.eventcollector.EventType;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.eventcollector.distservice.DeliverError;
import com.baidu.bifromq.plugin.eventcollector.distservice.DeliverNoInbox;
import com.baidu.bifromq.plugin.eventcollector.distservice.Delivered;
import com.baidu.bifromq.plugin.inboxbroker.IInboxBrokerManager;
import com.baidu.bifromq.plugin.inboxbroker.IInboxWriter;
import com.baidu.bifromq.plugin.inboxbroker.InboxPack;
import com.baidu.bifromq.plugin.inboxbroker.WriteResult;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.SysClientInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxWriteScheduler
    extends BatchCallScheduler<InboxWriteRequest, Void, InboxWriteRequest.InboxWriterKey> {
    private static final int MAX_BATCH_MESSAGES = DIST_MAX_BATCH_SEND_MESSAGES.get();
    private final IInboxBrokerManager inboxBrokerManager;
    private final IEventCollector eventCollector;
    private final IDistClient distClient;

    public InboxWriteScheduler(IInboxBrokerManager inboxBrokerManager,
                               IEventCollector eventCollector,
                               IDistClient distClient) {
        super("dist_worker_send_batcher", DIST_WORKER_MAX_INFLIGHT_CALLS_PER_QUEUE.get());
        this.inboxBrokerManager = inboxBrokerManager;
        this.eventCollector = eventCollector;
        this.distClient = distClient;
    }

    @Override
    protected BatchCallBuilder<InboxWriteRequest, Void> newBuilder(String name, int maxInflights,
                                                                   InboxWriteRequest.InboxWriterKey inboxWriterKey) {
        return new InboxWriteCallBuilder(name, maxInflights, inboxWriterKey);
    }

    @Override
    protected Optional<InboxWriteRequest.InboxWriterKey> find(InboxWriteRequest request) {
        return Optional.of(request.writerKey);
    }

    private class InboxWriteCallBuilder extends BatchCallBuilder<InboxWriteRequest, Void> {
        private class BatchInboxWriteCall implements IBatchCall<InboxWriteRequest, Void> {
            private final AtomicInteger msgCount = new AtomicInteger();
            private final Map<MessagePackWrapper, Set<SubInfo>> batch = new ConcurrentHashMap<>();
            private CompletableFuture<Void> onDone = new CompletableFuture<>();

            @Override
            public boolean isEmpty() {
                return batch.isEmpty();
            }

            @Override
            public boolean isEnough() {
                return msgCount.get() > MAX_BATCH_MESSAGES;
            }

            @Override
            public CompletableFuture<Void> add(InboxWriteRequest request) {
                if (batch.computeIfAbsent(request.msgPackWrapper, k -> ConcurrentHashMap.newKeySet())
                    .add(request.matching.subInfo)) {
                    request.msgPackWrapper.messagePack.getMessageList()
                        .forEach(senderMsgPack -> msgCount.addAndGet(senderMsgPack.getMessageCount()));
                }
                return onDone;
            }

            @Override
            public void reset() {
                msgCount.set(0);
                batch.clear();
                onDone = new CompletableFuture<>();
            }

            @Override
            public CompletableFuture<Void> execute() {
                msgCountSummary.record(msgCount.get());
                inboxWriter.write(batch.entrySet().stream()
                        .map(e -> new InboxPack(e.getKey().messagePack, e.getValue()))
                        .collect(Collectors.toList()))
                    .whenComplete((reply, e) -> {
                        if (e != null) {
                            log.warn("Message deliver failed", e);
                        } else {
                            Map<SubInfo, TopicMessagePack> okMap = new HashMap<>();
                            Map<SubInfo, TopicMessagePack> noInboxMap = new HashMap<>();
                            Map<SubInfo, TopicMessagePack> errorMap = new HashMap<>();
                            Map<SubInfo, Throwable> causeMap = new HashMap<>();
                            for (MessagePackWrapper msgPackWrapper : batch.keySet()) {
                                Set<SubInfo> subInfoList = batch.get(msgPackWrapper);
                                for (SubInfo subInfo : subInfoList) {
                                    WriteResult v = reply.get(subInfo);
                                    switch (v.type()) {
                                        case OK:
                                            okMap.put(subInfo, msgPackWrapper.messagePack);
                                            break;
                                        case NO_INBOX:
                                            // clear all subs from the missing inbox
                                            distClient.clear(System.nanoTime(), subInfo.getInboxId(), inboxGroupKey,
                                                brokerId,
                                                ClientInfo.newBuilder()
                                                    .setTrafficId(subInfo.getTrafficId())
                                                    .setUserId("distworker")
                                                    .setSysClientInfo(SysClientInfo
                                                        .newBuilder()
                                                        .setType("distservice")
                                                        .build())
                                                    .build());
                                            noInboxMap.put(subInfo, msgPackWrapper.messagePack);
                                            break;
                                        case ERROR:
                                            errorMap.put(subInfo, msgPackWrapper.messagePack);
                                            causeMap.put(subInfo, ((WriteResult.Error) v).cause);
                                            break;
                                    }
                                }
                            }
                            if (!okMap.isEmpty()) {
                                eventCollector.report(getLocal(EventType.DELIVERED, Delivered.class)
                                    .brokerId(brokerId)
                                    .inboxGroupKey(inboxGroupKey)
                                    .messages(okMap));
                            }
                            if (!noInboxMap.isEmpty()) {
                                eventCollector.report(getLocal(EventType.DELIVER_NO_INBOX, DeliverNoInbox.class)
                                    .brokerId(brokerId)
                                    .inboxGroupKey(inboxGroupKey)
                                    .messages(noInboxMap));
                            }
                            if (!errorMap.isEmpty()) {
                                eventCollector.report(getLocal(EventType.DELIVER_ERROR, DeliverError.class)
                                    .brokerId(brokerId)
                                    .inboxGroupKey(inboxGroupKey)
                                    .messages(errorMap)
                                    .causes(causeMap));
                            }
                        }
                        onDone.complete(null);
                    });
                return onDone;
            }
        }

        private final int brokerId;
        private final String inboxGroupKey;
        private final IInboxWriter inboxWriter;
        private final DistributionSummary msgCountSummary;

        InboxWriteCallBuilder(String name, int maxInflights, InboxWriteRequest.InboxWriterKey key) {
            super(name, maxInflights);
            this.brokerId = key.brokerId();
            this.inboxGroupKey = key.inboxGroupKey();
            this.inboxWriter = inboxBrokerManager.openWriter(inboxGroupKey, brokerId);
            Tags tags = Tags.of("brokerId", String.valueOf(brokerId));
            msgCountSummary = DistributionSummary.builder("dist.server.send.messages")
                .tags(tags)
                .register(Metrics.globalRegistry);
        }

        @Override
        public BatchInboxWriteCall newBatch() {
            return new BatchInboxWriteCall();
        }

        @Override
        public void close() {
            inboxWriter.close();
            Metrics.globalRegistry.remove(msgCountSummary);
        }
    }
}
