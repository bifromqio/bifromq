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

package com.baidu.bifromq.inbox.records;

import com.baidu.bifromq.type.MatchInfo;
import java.util.Comparator;

public record ScopedInbox(String tenantId, String inboxId, long incarnation) implements Comparable<ScopedInbox> {
    private static final String SEPARATOR = "_";
    private static final Comparator<ScopedInbox> COMPARATOR = Comparator.comparing(ScopedInbox::tenantId)
        .thenComparing(ScopedInbox::inboxId)
        .thenComparing(ScopedInbox::incarnation);

    public static String distInboxId(String inboxId, long incarnation) {
        return inboxId + SEPARATOR + incarnation;
    }

    public static ScopedInbox from(String tenantId, MatchInfo subInfo) {
        int splitAt = subInfo.getReceiverId().lastIndexOf(SEPARATOR);
        return new ScopedInbox(tenantId,
            subInfo.getReceiverId().substring(0, splitAt),
            Long.parseUnsignedLong(subInfo.getReceiverId().substring(splitAt + 1)));
    }

    public MatchInfo convertTo(String topicFilter) {
        return MatchInfo.newBuilder()
            .setReceiverId(inboxId + SEPARATOR + incarnation)
            .setTopicFilter(topicFilter)
            .build();
    }

    @Override
    public int compareTo(ScopedInbox o) {
        return COMPARATOR.compare(this, o);
    }
}
