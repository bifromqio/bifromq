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

package com.baidu.bifromq.dist.worker.schema;

import static com.baidu.bifromq.util.TopicUtil.unescape;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode(callSuper = true)
@ToString
public class GroupMatching extends Matching {
    @EqualsAndHashCode.Exclude
    public final boolean ordered;
    @EqualsAndHashCode.Exclude
    public final List<NormalMatching> receiverList;

    public final Map<String, Long> receivers;
    private final String origTopicFilter;

    GroupMatching(RouteDetail detail, Map<String, Long> members) {
        super(detail);
        assert detail.type() != RouteDetail.RouteType.NormalReceiver;
        this.ordered = detail.type() == RouteDetail.RouteType.OrderedReceiverGroup;
        this.receivers = members;
        this.origTopicFilter = detail.originalTopicFilter();
        this.receiverList = members.entrySet().stream()
            .map(e -> new NormalMatching(detail, origTopicFilter, e.getKey(), e.getValue()))
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

    public String topicFilter() {
        return unescape(escapedTopicFilter);
    }
}
