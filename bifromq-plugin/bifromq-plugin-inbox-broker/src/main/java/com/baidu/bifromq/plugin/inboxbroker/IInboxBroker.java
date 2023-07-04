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

import java.util.concurrent.CompletableFuture;
import org.pf4j.ExtensionPoint;

public interface IInboxBroker extends ExtensionPoint {
    /**
     * A static id representing the message receiver(brokerId) in subscription
     *
     * @return the statically assigned id for the downstream inbox broker
     */
    int id();

    /**
     * Open a group writer
     *
     * @param inboxGroupKey the key of the inbox group
     * @return a writer object
     */
    IInboxGroupWriter open(String inboxGroupKey);

    /**
     * Check the existence of an inbox in given inbox group asynchronously.
     *
     * @param reqId         the request id
     * @param trafficId     the id of the traffic to which the inbox belongs
     * @param inboxId       the inbox id
     * @param inboxGroupKey the key of the inbox group under which the inbox belongs
     * @return boolean indicating if the inbox still exists
     */
    CompletableFuture<Boolean> hasInbox(long reqId, String trafficId, String inboxId, String inboxGroupKey);

    /**
     * Close the inbox broker
     */
    void close();
}
