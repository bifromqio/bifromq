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

package com.baidu.demo.plugin;

import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.channelclosed.ChannelClosedEvent;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ClientDisconnectEvent;
import org.pf4j.Extension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Extension
public final class EventLogger implements IEventCollector {
    private static final Logger LOG = LoggerFactory.getLogger("DemoEventLogger");

    @Override
    public void report(Event<?> event) {
        if (LOG.isDebugEnabled()) {
            switch (event.type()) {
                case DISCARD,
                    WILL_DIST_ERROR,
                    QOS0_DIST_ERROR,
                    QOS1_DIST_ERROR,
                    QOS2_DIST_ERROR,
                    OVERFLOWED,
                    QOS0_DROPPED,
                    QOS1_DROPPED,
                    QOS2_DROPPED,
                    OVERSIZE_PACKET_DROPPED,
                    MSG_RETAINED_ERROR,
                    DELIVER_ERROR -> LOG.debug("Message dropped due to {}", event.type());
                default -> {
                    if (event instanceof ChannelClosedEvent || event instanceof ClientDisconnectEvent) {
                        LOG.debug("Channel closed due to {}", event.type());
                    }
                }
            }
        }
    }
}
