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

package com.baidu.demo.plugin;

import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import org.pf4j.Extension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Extension
public final class EventLogger implements IEventCollector {
    private static final Logger LOG = LoggerFactory.getLogger("DemoEventLogger");

    @Override
    public void report(Event<?> event) {
        if (LOG.isWarnEnabled()) {
            switch (event.type()) {
                case DIST_ERROR:
                case OVERFLOWED:
                case QOS1_DROPPED:
                case DELIVER_ERROR:
                    LOG.warn("Message dropped due to {}", event.type());
            }
        }
    }
}
