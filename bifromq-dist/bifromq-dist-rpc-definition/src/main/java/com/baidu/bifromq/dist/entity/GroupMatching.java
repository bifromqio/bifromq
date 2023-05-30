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

package com.baidu.bifromq.dist.entity;

import static com.baidu.bifromq.dist.util.TopicUtil.ORDERED_SHARE;
import static com.baidu.bifromq.dist.util.TopicUtil.TOPIC_SEPARATOR;
import static com.baidu.bifromq.dist.util.TopicUtil.UNORDERED_SHARE;
import static com.baidu.bifromq.dist.util.TopicUtil.unescape;

import com.baidu.bifromq.type.QoS;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
public class GroupMatching extends Matching {
    public final String group;
    public final boolean ordered;
    public final List<NormalMatching> inboxList;
    public final Map<String, QoS> inboxMap;
    private final String origTopicFilter;

    GroupMatching(ByteString key, String group, boolean ordered, Map<String, QoS> inboxes) {
        super(key);
        this.group = group;
        this.ordered = ordered;
        this.inboxMap = inboxes;
        if (ordered) {
            origTopicFilter =
                ORDERED_SHARE + TOPIC_SEPARATOR + group + TOPIC_SEPARATOR + unescape(escapedTopicFilter);
        } else {
            origTopicFilter =
                UNORDERED_SHARE + TOPIC_SEPARATOR + group + TOPIC_SEPARATOR + unescape(escapedTopicFilter);
        }
        this.inboxList = inboxes.entrySet().stream()
            .map(e -> new NormalMatching(key, origTopicFilter, e.getKey(), e.getValue()))
            .collect(Collectors.toList());
    }

    @Override
    public String originalTopicFilter() {
        return origTopicFilter;
    }
}
