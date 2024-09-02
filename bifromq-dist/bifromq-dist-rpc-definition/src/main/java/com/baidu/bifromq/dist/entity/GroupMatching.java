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

package com.baidu.bifromq.dist.entity;

import static com.baidu.bifromq.util.TopicConst.DELIMITER_CHAR;
import static com.baidu.bifromq.util.TopicConst.ORDERED_SHARE;
import static com.baidu.bifromq.util.TopicConst.UNORDERED_SHARE;
import static com.baidu.bifromq.dist.util.TopicUtil.unescape;

import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode(callSuper = true)
@ToString
public class GroupMatching extends Matching {
    @EqualsAndHashCode.Exclude
    public final String group;
    @EqualsAndHashCode.Exclude
    public final boolean ordered;
    @EqualsAndHashCode.Exclude
    public final List<NormalMatching> receiverList;

    public final Set<String> receiverIds;
    private final String origTopicFilter;

    GroupMatching(ByteString key, String group, boolean ordered, List<String> scopedReceiverIds) {
        super(key);
        this.group = group;
        this.ordered = ordered;
        this.receiverIds = Sets.newHashSet(scopedReceiverIds);
        if (ordered) {
            origTopicFilter =
                ORDERED_SHARE + DELIMITER_CHAR + group + DELIMITER_CHAR + unescape(escapedTopicFilter);
        } else {
            origTopicFilter =
                UNORDERED_SHARE + DELIMITER_CHAR + group + DELIMITER_CHAR + unescape(escapedTopicFilter);
        }
        this.receiverList = Sets.newLinkedHashSet(scopedReceiverIds).stream()
            .map(receiverId -> new NormalMatching(key, origTopicFilter, receiverId))
            .collect(Collectors.toList());
    }

    @Override
    public Type type() {
        return Type.Group;
    }

    @Override
    public String originalTopicFilter() {
        return origTopicFilter;
    }

    public void addAll(Set<String> scopedReceiverIds) {
        scopedReceiverIds.forEach(receiverId -> {
            if (!receiverIds.contains(receiverId)) {
                receiverIds.add(receiverId);
                receiverList.add(new NormalMatching(key, origTopicFilter, receiverId));
            }
        });
    }

    public void removeAll(Set<String> scopedReceiverIds) {
        receiverIds.removeIf(receiverId -> {
            if (scopedReceiverIds.contains(receiverId)) {
                receiverList.removeIf(receiver -> receiver.scopedInboxId.equals(receiverId));
                return true;
            }
            return false;
        });
    }
}
