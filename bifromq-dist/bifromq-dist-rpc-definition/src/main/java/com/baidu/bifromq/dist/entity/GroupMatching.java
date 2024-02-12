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

import static com.baidu.bifromq.dist.util.TopicUtil.ORDERED_SHARE;
import static com.baidu.bifromq.dist.util.TopicUtil.TOPIC_SEPARATOR;
import static com.baidu.bifromq.dist.util.TopicUtil.UNORDERED_SHARE;
import static com.baidu.bifromq.dist.util.TopicUtil.unescape;

import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
public class GroupMatching extends Matching {
    @EqualsAndHashCode.Exclude
    public final String group;
    @EqualsAndHashCode.Exclude
    public final boolean ordered;
    @EqualsAndHashCode.Exclude
    public final List<NormalMatching> receiverList;

    public final List<String> receiverIds;
    private final String origTopicFilter;

    GroupMatching(ByteString key, String group, boolean ordered, List<String> scopedReceiverIds) {
        super(key);
        this.group = group;
        this.ordered = ordered;
        this.receiverIds = scopedReceiverIds;
        if (ordered) {
            origTopicFilter =
                ORDERED_SHARE + TOPIC_SEPARATOR + group + TOPIC_SEPARATOR + unescape(escapedTopicFilter);
        } else {
            origTopicFilter =
                UNORDERED_SHARE + TOPIC_SEPARATOR + group + TOPIC_SEPARATOR + unescape(escapedTopicFilter);
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
}
