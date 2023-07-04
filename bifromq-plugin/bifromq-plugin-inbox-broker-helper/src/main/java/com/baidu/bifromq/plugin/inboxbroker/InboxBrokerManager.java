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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.PluginManager;

@Slf4j
public class InboxBrokerManager implements IInboxBrokerManager {
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final Map<Integer, IInboxBroker> receivers = new HashMap<>();

    public InboxBrokerManager(PluginManager pluginMgr, IInboxBroker... builtInReceivers) {
        for (IInboxBroker receiver : builtInReceivers) {
            log.info("Register built-in receiver[{}] with id[{}]", receiver.getClass().getSimpleName(), receiver.id());
            receivers.put(receiver.id(), new InboxBrokerSender(receiver));
        }
        List<IInboxBroker> customReceivers = pluginMgr.getExtensions(IInboxBroker.class);
        for (IInboxBroker customReceiver : customReceivers) {
            if (receivers.containsKey(customReceiver.id())) {
                log.warn("Id[{}] is reserved for receiver[{}], skip registering custom receiver[{}]",
                    customReceiver.id(),
                    receivers.get(customReceiver.id()).getClass().getName(),
                    customReceiver.getClass().getName());
            } else {
                log.info("Register custom receiver[{}] with id[{}]",
                    customReceiver.getClass().getSimpleName(), customReceiver.id());
                receivers.put(customReceiver.id(), new InboxBrokerSender(customReceiver));
            }
        }
    }

    @Override
    public boolean hasBroker(int brokerId) {
        return receivers.containsKey(brokerId);
    }

    @Override
    public IInboxGroupWriter openWriter(String inboxGroupKey, int brokerId) {
        IInboxBroker receiver = receivers.get(brokerId);
        if (receiver == null) {
            return null;
        }
        return receiver.open(inboxGroupKey);
    }

    @Override
    public CompletableFuture<Boolean> hasInbox(long reqId, String trafficId,
                                               String inboxId, String inboxGroupKey, int brokerId) {
        IInboxBroker receiver = receivers.get(brokerId);
        if (receiver == null) {
            return CompletableFuture.completedFuture(false);
        }
        return receiver
            .hasInbox(reqId, trafficId, inboxId, inboxGroupKey)
            .exceptionally(e -> {
                log.warn("Unexpected error during inbox existence check", e);
                return true;
            });
    }

    @Override
    public void stop() {
        if (stopped.compareAndSet(false, true)) {
            log.info("Stopping inbox broker manager");
            receivers.values().forEach(IInboxBroker::close);
            log.info("Inbox broker manager stopped");
        }
    }
}
