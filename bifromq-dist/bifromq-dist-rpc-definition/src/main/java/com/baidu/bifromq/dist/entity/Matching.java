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

import static com.baidu.bifromq.dist.util.TopicUtil.NUL;

import com.google.protobuf.ByteString;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public abstract class Matching {
    public enum Type {
        Normal, Group
    }

    @EqualsAndHashCode.Exclude
    public final ByteString key;
    @EqualsAndHashCode.Exclude
    public final String escapedTopicFilter;

    public final String tenantId;

    protected Matching(ByteString matchRecordKey) {
        this.key = matchRecordKey;
        String matchRecordKeyStr = matchRecordKey.toStringUtf8();
        int firstSplit = matchRecordKeyStr.indexOf(NUL);
        tenantId = matchRecordKeyStr.substring(0, firstSplit);
        int lastSplit = matchRecordKeyStr.lastIndexOf(NUL);
        escapedTopicFilter = matchRecordKeyStr.substring(firstSplit + 2, lastSplit);
    }

    public abstract Type type();

    public abstract String originalTopicFilter();
}
