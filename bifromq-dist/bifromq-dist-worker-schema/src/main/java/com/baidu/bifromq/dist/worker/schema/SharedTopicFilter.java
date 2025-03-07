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

package com.baidu.bifromq.dist.worker.schema;

import static com.baidu.bifromq.util.TopicConst.DELIMITER_CHAR;
import static com.baidu.bifromq.util.TopicConst.ORDERED_SHARE;
import static com.baidu.bifromq.util.TopicConst.UNORDERED_SHARE;
import static com.baidu.bifromq.util.TopicUtil.isNormalTopicFilter;

/**
 * The object contains the information parsed from a shared subscription topic filter.
 * <br>
 * Currently two formats are supported:
 * <ul>Unordered share: {@code $share/<groupName>/<topicFilter>}</ul>
 * <ul>Ordered share: {@code $oshare/<groupName>/<topicFilter>}</ul>
 */
public class SharedTopicFilter {
    public String originTopicFilter;
    public boolean ordered;
    public String shareGroup;
    public String topicFilter;

    private SharedTopicFilter(String originTopicFilter, boolean ordered, String shareName, String filter) {
        this.originTopicFilter = originTopicFilter;
        this.ordered = ordered;
        this.shareGroup = shareName;
        this.topicFilter = filter;
    }

    public static SharedTopicFilter from(String topicFilter) {
        assert !isNormalTopicFilter(topicFilter);
        String sharePrefix = topicFilter.startsWith(UNORDERED_SHARE) ? UNORDERED_SHARE : ORDERED_SHARE;
        boolean ordered = !topicFilter.startsWith(UNORDERED_SHARE);
        String rest = topicFilter.substring((sharePrefix + DELIMITER_CHAR).length());
        int firstTopicSeparatorIndex = rest.indexOf(DELIMITER_CHAR);
        String shareGroup = rest.substring(0, firstTopicSeparatorIndex);
        return new SharedTopicFilter(topicFilter, ordered, shareGroup, rest.substring(firstTopicSeparatorIndex + 1));
    }
}
