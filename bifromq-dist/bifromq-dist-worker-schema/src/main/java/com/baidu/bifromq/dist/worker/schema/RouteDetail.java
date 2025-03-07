/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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
import static com.baidu.bifromq.util.TopicConst.NUL;
import static com.baidu.bifromq.util.TopicConst.ORDERED_SHARE;
import static com.baidu.bifromq.util.TopicConst.UNORDERED_SHARE;
import static com.baidu.bifromq.util.TopicUtil.unescape;

/**
 * RouteDetail parsed from a match record key.
 *
 * @param tenantId           the tenantId
 * @param escapedTopicFilter the topic filter in escaped form
 * @param type               the route type
 * @param receiverInfo       the receiverUrl or receiverGroup
 */
public record RouteDetail(String tenantId,
                          String escapedTopicFilter,
                          RouteDetail.RouteType type,
                          String receiverInfo) { // receiverUrl or receiverGroup depends on RouteType
    public enum RouteType {
        NormalReceiver,
        OrderedReceiverGroup,
        UnorderedReceiverGroup
    }

    /**
     * The topic filter in unescaped form.
     *
     * @return the topic filter
     */
    public String topicFilter() {
        return unescape(escapedTopicFilter);
    }

    /**
     * The original topic filter.
     *
     * @return the original topic filter
     */
    public String originalTopicFilter() {
        switch (type) {
            case NormalReceiver -> {
                return unescape(escapedTopicFilter);
            }
            case UnorderedReceiverGroup -> {
                return UNORDERED_SHARE + DELIMITER_CHAR + receiverInfo + DELIMITER_CHAR + unescape(escapedTopicFilter);
            }
            case OrderedReceiverGroup -> {
                return ORDERED_SHARE + DELIMITER_CHAR + receiverInfo + DELIMITER_CHAR + unescape(escapedTopicFilter);
            }
            default -> throw new UnsupportedOperationException("Unknown route type: " + type);
        }
    }

    public String globalEscapedTopicFilter() {
        return tenantId + NUL + escapedTopicFilter;
    }
}
