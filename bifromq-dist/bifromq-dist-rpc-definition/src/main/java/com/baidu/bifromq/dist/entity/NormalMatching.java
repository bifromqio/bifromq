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


import static com.baidu.bifromq.dist.util.TopicUtil.NUL;
import static com.baidu.bifromq.dist.util.TopicUtil.unescape;

import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.SubInfo;
import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
public class NormalMatching extends Matching {
    public final String scopedInboxId;

    public final SubInfo subInfo;

    public final String inboxGroupKey;
    public final int brokerId;
    private final String originalTopicFilter;

    NormalMatching(ByteString key, String scopedInboxId, QoS subQoS) {
        super(key);
        this.originalTopicFilter = unescape(escapedTopicFilter);
        this.scopedInboxId = scopedInboxId;

        scopedInboxId = new String(Base64.getDecoder().decode(scopedInboxId), StandardCharsets.UTF_8);
        String[] parts = scopedInboxId.split(NUL);
        brokerId = Integer.parseInt(parts[0]);
        inboxGroupKey = Strings.isNullOrEmpty(parts[2]) ? null : parts[2];
        subInfo = SubInfo.newBuilder()
            .setTrafficId(trafficId)
            .setInboxId(parts[1])
            .setSubQoS(subQoS)
            .setTopicFilter(originalTopicFilter)
            .build();

    }

    NormalMatching(ByteString key, String originalTopicFilter, String scopedInboxId, QoS subQoS) {
        super(key);
        this.scopedInboxId = scopedInboxId;
        this.originalTopicFilter = originalTopicFilter;

        scopedInboxId = new String(Base64.getDecoder().decode(scopedInboxId), StandardCharsets.UTF_8);
        String[] parts = scopedInboxId.split(NUL);
        brokerId = Integer.parseInt(parts[0]);
        inboxGroupKey = Strings.isNullOrEmpty(parts[2]) ? null : parts[2];
        subInfo = SubInfo.newBuilder()
            .setTrafficId(trafficId)
            .setInboxId(parts[1])
            .setSubQoS(subQoS)
            .setTopicFilter(originalTopicFilter)
            .build();
    }

    @Override
    public String originalTopicFilter() {
        return originalTopicFilter;
    }
}
