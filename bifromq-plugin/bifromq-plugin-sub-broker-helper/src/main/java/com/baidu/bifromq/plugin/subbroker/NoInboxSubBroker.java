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

package com.baidu.bifromq.plugin.subbroker;

import com.baidu.bifromq.type.SubInfo;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class NoInboxSubBroker implements ISubBroker {
    private static final CompletableFuture<CheckResult> NO_INBOX =
        CompletableFuture.completedFuture(CheckResult.NO_INBOX);
    public static final ISubBroker INSTANCE = new NoInboxSubBroker();

    @Override
    public int id() {
        return Integer.MIN_VALUE;
    }

    @Override
    public IDeliverer open(String delivererKey) {
        return new IDeliverer() {
            @Override
            public CompletableFuture<Map<SubInfo, DeliveryResult>> deliver(Iterable<DeliveryPack> packs) {
                Map<SubInfo, DeliveryResult> deliveryResults = new HashMap<>();
                for (DeliveryPack pack : packs) {
                    pack.inboxes.forEach(subInfo -> deliveryResults.put(subInfo, DeliveryResult.NO_INBOX));
                }
                return CompletableFuture.completedFuture(deliveryResults);
            }

            @Override
            public void close() {

            }
        };
    }

    @Override
    public void close() {

    }
}
