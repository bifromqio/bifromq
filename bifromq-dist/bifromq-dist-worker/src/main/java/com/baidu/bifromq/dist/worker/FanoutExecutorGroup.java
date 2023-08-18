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

package com.baidu.bifromq.dist.worker;

import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.entity.NormalMatching;
import com.baidu.bifromq.dist.worker.scheduler.DeliveryRequest;
import com.baidu.bifromq.dist.worker.scheduler.IDeliveryScheduler;
import com.baidu.bifromq.dist.worker.scheduler.MessagePackWrapper;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.eventcollector.distservice.DeliverError;
import com.baidu.bifromq.plugin.eventcollector.distservice.DeliverNoInbox;
import com.baidu.bifromq.plugin.eventcollector.distservice.Delivered;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.jctools.queues.MpscBlockingConsumerArrayQueue;

@Slf4j
class FanoutExecutorGroup {
    private final IEventCollector eventCollector;
    private final IDistClient distClient;
    private final IDeliveryScheduler scheduler;
    private final ExecutorService[] phaseOneExecutorGroup;
    private final ExecutorService[] phaseTwoExecutorGroup;

    FanoutExecutorGroup(ISubBrokerManager subBrokerMgr,
                        IDeliveryScheduler scheduler,
                        IEventCollector eventCollector,
                        IDistClient distClient,
                        int groupSize) {
        this.eventCollector = eventCollector;
        this.distClient = distClient;
        this.scheduler = scheduler;
        phaseOneExecutorGroup = new ExecutorService[groupSize];
        phaseTwoExecutorGroup = new ExecutorService[groupSize];
        for (int i = 0; i < groupSize; i++) {
            phaseOneExecutorGroup[i] = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new MpscBlockingConsumerArrayQueue<>(2000),
                EnvProvider.INSTANCE.newThreadFactory("fanout-p1-executor-" + i));
            phaseTwoExecutorGroup[i] = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new MpscBlockingConsumerArrayQueue<>(2000),
                EnvProvider.INSTANCE.newThreadFactory("fanout-p2-executor-" + i));
        }
    }

    public void shutdown() {
        for (ExecutorService executorService : phaseOneExecutorGroup) {
            executorService.shutdown();
        }
        for (ExecutorService executorService : phaseTwoExecutorGroup) {
            executorService.shutdown();
        }
    }

    public void submit(int hash, Map<NormalMatching, Set<ClientInfo>> routeMap, MessagePackWrapper msgPackWrapper,
                       Map<ClientInfo, TopicMessagePack.PublisherPack> senderMsgPackMap) {
        int idx = hash % phaseOneExecutorGroup.length;
        if (idx < 0) {
            idx += phaseOneExecutorGroup.length;
        }
        try {
            phaseOneExecutorGroup[idx].submit(() -> send(routeMap, msgPackWrapper, senderMsgPackMap));
        } catch (RejectedExecutionException ree) {
            log.warn("Message drop due to fan-out queue is full");
        }
    }

    private void send(Map<NormalMatching, Set<ClientInfo>> routeMap,
                      MessagePackWrapper msgPackWrapper,
                      Map<ClientInfo, TopicMessagePack.PublisherPack> senderMsgPackMap) {
        if (routeMap.size() == 1) {
            routeMap.forEach((route, senders) -> send(route, senders, msgPackWrapper, senderMsgPackMap));
        } else {
            List<List<Runnable>> fanoutTasksPerIdx = new ArrayList<>(phaseTwoExecutorGroup.length);
            for (int i = 0; i < phaseTwoExecutorGroup.length; i++) {
                fanoutTasksPerIdx.add(new LinkedList<>());
            }
            routeMap.forEach((route, senders) -> {
                int idx = route.hashCode() % phaseTwoExecutorGroup.length;
                if (idx < 0) {
                    idx += phaseTwoExecutorGroup.length;
                }
                List<Runnable> fanoutTasks = fanoutTasksPerIdx.get(idx);
                fanoutTasks.add(() -> send(route, senders, msgPackWrapper, senderMsgPackMap));
            });
            for (int i = 0; i < phaseTwoExecutorGroup.length; i++) {
                List<Runnable> fanoutTasks = fanoutTasksPerIdx.get(i);
                if (!fanoutTasks.isEmpty()) {
                    try {
                        phaseTwoExecutorGroup[i].submit(() -> fanoutTasks.forEach(Runnable::run));
                    } catch (RejectedExecutionException ree) {
                        log.warn("Message drop due to fan-out queue is full");
                    }
                }
            }
        }
    }

    private void send(NormalMatching route, Set<ClientInfo> senders,
                      MessagePackWrapper msgPackWrapper,
                      Map<ClientInfo, TopicMessagePack.PublisherPack> senderMsgPackMap) {
        if (senders.size() == senderMsgPackMap.size()) {
            send(msgPackWrapper, route);
        } else {
            // ordered share sub
            TopicMessagePack.Builder subMsgPackBuilder = TopicMessagePack.newBuilder()
                .setTopic(msgPackWrapper.messagePack.getTopic());
            senders.forEach(sender -> subMsgPackBuilder.addMessage(senderMsgPackMap.get(sender)));
            send(MessagePackWrapper.wrap(subMsgPackBuilder.build()), route);
        }
    }

    private void send(MessagePackWrapper msgPack, NormalMatching matched) {
        int subBrokerId = matched.subBrokerId;
        String delivererKey = matched.delivererKey;
        SubInfo sub = matched.subInfo;
        DeliveryRequest request = new DeliveryRequest(sub, subBrokerId, delivererKey, msgPack);
        scheduler.schedule(request).whenComplete((result, e) -> {
            if (e != null) {
                eventCollector.report(getLocal(DeliverError.class)
                    .brokerId(subBrokerId)
                    .delivererKey(delivererKey)
                    .subInfo(sub)
                    .messages(msgPack.messagePack));
            } else {
                switch (result) {
                    case OK -> eventCollector.report(getLocal(Delivered.class)
                        .brokerId(subBrokerId)
                        .delivererKey(delivererKey)
                        .subInfo(sub)
                        .messages(msgPack.messagePack));
                    case NO_INBOX -> {
                        // unsub as side effect
                        SubInfo subInfo = matched.subInfo;
                        distClient.unmatch(System.nanoTime(),
                            subInfo.getTenantId(),
                            subInfo.getTopicFilter(),
                            subInfo.getInboxId(),
                            delivererKey,
                            subBrokerId);
                        eventCollector.report(getLocal(DeliverNoInbox.class)
                            .brokerId(subBrokerId)
                            .delivererKey(delivererKey)
                            .subInfo(sub)
                            .messages(msgPack.messagePack));
                    }
                    case FAILED -> eventCollector.report(getLocal(DeliverError.class)
                        .brokerId(subBrokerId)
                        .delivererKey(delivererKey)
                        .subInfo(sub)
                        .messages(msgPack.messagePack));
                }
            }
        });
    }
}
