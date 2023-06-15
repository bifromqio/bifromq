package com.baidu.bifromq.dist.worker;

import com.baidu.bifromq.dist.entity.NormalMatching;
import com.baidu.bifromq.dist.worker.scheduler.InboxWriteRequest;
import com.baidu.bifromq.dist.worker.scheduler.InboxWriteScheduler;
import com.baidu.bifromq.dist.worker.scheduler.MessagePackWrapper;
import com.baidu.bifromq.plugin.inboxbroker.IInboxBrokerManager;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
    private final IInboxBrokerManager inboxBrokerManager;
    private final InboxWriteScheduler scheduler;
    private final ExecutorService[] phaseOneExecutorGroup;
    private final ExecutorService[] phaseTwoExecutorGroup;

    FanoutExecutorGroup(IInboxBrokerManager inboxBrokerManager, InboxWriteScheduler scheduler, int groupSize) {
        this.inboxBrokerManager = inboxBrokerManager;
        this.scheduler = scheduler;
        phaseOneExecutorGroup = new ExecutorService[groupSize];
        phaseTwoExecutorGroup = new ExecutorService[groupSize];
        for (int i = 0; i < groupSize; i++) {
            phaseOneExecutorGroup[i] = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new MpscBlockingConsumerArrayQueue<>(2000),
                new ThreadFactoryBuilder().setNameFormat("fanout-p1-executor" + i).build());
            phaseTwoExecutorGroup[i] = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new MpscBlockingConsumerArrayQueue<>(2000),
                new ThreadFactoryBuilder().setNameFormat("fanout-p2-executor" + i).build());
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
                       Map<ClientInfo, TopicMessagePack.SenderMessagePack> senderMsgPackMap) {
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
                      Map<ClientInfo, TopicMessagePack.SenderMessagePack> senderMsgPackMap) {
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
                      Map<ClientInfo, TopicMessagePack.SenderMessagePack> senderMsgPackMap) {
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
        SubInfo sub = matched.subInfo;
        if (!inboxBrokerManager.hasBroker(matched.brokerId)) {
            log.error("Invalid inbox broker[{}] for sub[inboxId={}, qos={}, topicFilter={}]",
                matched.brokerId,
                sub.getInboxId(),
                sub.getSubQoS(),
                sub.getTopicFilter());
            return;
        }
        scheduler.schedule(new InboxWriteRequest(matched, msgPack));
    }
}
