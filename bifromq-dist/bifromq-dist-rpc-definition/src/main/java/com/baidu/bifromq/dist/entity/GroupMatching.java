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
import static com.baidu.bifromq.util.TopicUtil.unescape;

import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
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

    public final Map<String, Long> receiverIds;
    private final String origTopicFilter;

    GroupMatching(ByteString key, String group, boolean ordered, Map<String, Long> scopedReceiverIds) {
        super(key);
        this.group = group;
        this.ordered = ordered;
        this.receiverIds = scopedReceiverIds;
        if (ordered) {
            origTopicFilter =
                ORDERED_SHARE + DELIMITER_CHAR + group + DELIMITER_CHAR + unescape(escapedTopicFilter);
        } else {
            origTopicFilter =
                UNORDERED_SHARE + DELIMITER_CHAR + group + DELIMITER_CHAR + unescape(escapedTopicFilter);
        }
        this.receiverList = scopedReceiverIds.entrySet().stream()
            .map(e -> new NormalMatching(key, origTopicFilter, e.getKey(), e.getValue()))
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
