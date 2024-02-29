/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.mqtt.handler;

import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static com.bifromq.plugin.resourcethrottler.TenantResourceType.TotalInboundBytesPerSecond;

import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ResourceThrottled;
import com.baidu.bifromq.type.ClientInfo;
import com.bifromq.plugin.resourcethrottler.IResourceThrottler;

public class InboundResourceCondition extends MemPressureCondition {
    private final IResourceThrottler resourceThrottler;
    private final IEventCollector eventCollector;
    private final ClientInfo clientInfo;

    public InboundResourceCondition(IResourceThrottler resourceThrottler,
                                    IEventCollector eventCollector,
                                    ClientInfo clientInfo) {
        this.resourceThrottler = resourceThrottler;
        this.eventCollector = eventCollector;
        this.clientInfo = clientInfo;
    }

    @Override
    public Boolean get() {
        if (super.get()) {
            eventCollector.report(getLocal(ResourceThrottled.class)
                .type("High DirectMemory Usage")
                .clientInfo(clientInfo));
            return true;
        }
        if (!resourceThrottler.hasResource(clientInfo.getTenantId(), TotalInboundBytesPerSecond)) {
            eventCollector.report(getLocal(ResourceThrottled.class)
                .type(TotalInboundBytesPerSecond.name())
                .clientInfo(clientInfo));
            return true;
        }
        return false;
    }
}
