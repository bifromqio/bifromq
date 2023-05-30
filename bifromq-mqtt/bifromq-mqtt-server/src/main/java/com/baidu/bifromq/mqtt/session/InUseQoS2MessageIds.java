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

package com.baidu.bifromq.mqtt.session;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.time.Duration;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;

public class InUseQoS2MessageIds {
    private final LoadingCache<String, Cache<MessageId, MessageId>> msgIdCache;

    InUseQoS2MessageIds(Duration lifetime) {
        msgIdCache = Caffeine.newBuilder()
            .expireAfterAccess(lifetime.multipliedBy(2))
            .build(k -> Caffeine.newBuilder()
                .expireAfterWrite(lifetime)
                .build());
    }

    public void use(String trafficId, String channelId, int messageId) {
        MessageId msgId = new MessageId(channelId, messageId);
        msgIdCache.get(trafficId).put(msgId, msgId);
    }

    public boolean inUse(String trafficId, String channelId, int messageId) {
        return msgIdCache.get(trafficId).getIfPresent(new MessageId(channelId, messageId)) != null;
    }

    public void release(String trafficId, String channelId, int messageId) {
        msgIdCache.get(trafficId).invalidate(new MessageId(channelId, messageId));
    }

    @AllArgsConstructor
    @EqualsAndHashCode
    private static final class MessageId {
        final String channelId;
        final int messageId;
    }
}
