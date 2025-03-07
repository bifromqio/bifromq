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

import com.baidu.bifromq.type.MatchInfo;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode(callSuper = true)
@ToString
public class NormalMatching extends Matching {
    public final String receiverUrl;
    private final String originalTopicFilter;
    private final long incarnation;

    @EqualsAndHashCode.Exclude
    private final KVSchemaUtil.Receiver receiver;
    @EqualsAndHashCode.Exclude
    public final MatchInfo matchInfo;

    NormalMatching(RouteDetail routeDetail, long incarnation) {
        this(routeDetail, unescape(routeDetail.escapedTopicFilter()), routeDetail.receiverInfo(), incarnation);
    }

    NormalMatching(RouteDetail routeDetail, String originalTopicFilter, String receiverUrl, long incarnation) {
        super(routeDetail);
        this.receiverUrl = receiverUrl;
        receiver = KVSchemaUtil.parseReceiver(receiverUrl);
        this.originalTopicFilter = originalTopicFilter;
        this.incarnation = incarnation;

        matchInfo = MatchInfo.newBuilder()
            .setReceiverId(receiver.receiverId())
            .setTopicFilter(originalTopicFilter)
            .setIncarnation(incarnation)
            .build();
    }

    @Override
    public Type type() {
        return Type.Normal;
    }

    @Override
    public String originalTopicFilter() {
        return originalTopicFilter;
    }

    public MatchInfo matchInfo() {
        return matchInfo;
    }

    public int subBrokerId() {
        return receiver.subBrokerId();
    }

    public String delivererKey() {
        return receiver.delivererKey();
    }

    public long incarnation() {
        return incarnation;
    }
}
