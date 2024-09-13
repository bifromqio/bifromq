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

package com.baidu.bifromq.dist.util;

import static com.baidu.bifromq.util.TopicConst.DELIMITER;
import static com.baidu.bifromq.util.TopicConst.DELIMITER_CHAR;
import static com.baidu.bifromq.util.TopicConst.MULTI_WILDCARD;
import static com.baidu.bifromq.util.TopicConst.NUL;
import static com.baidu.bifromq.util.TopicConst.NUL_CHAR;
import static com.baidu.bifromq.util.TopicConst.ORDERED_SHARE;
import static com.baidu.bifromq.util.TopicConst.SINGLE_WILDCARD;
import static com.baidu.bifromq.util.TopicConst.UNORDERED_SHARE;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public class TopicUtil {
    public static String escape(String topicFilter) {
        assert !topicFilter.contains(NUL);
        return topicFilter.replace(DELIMITER, NUL);
    }

    public static String unescape(String topicFilter) {
        return topicFilter.replace(NUL, DELIMITER);
    }

    public static List<String> parse(String tenantId, String topic, boolean isEscaped) {
        List<String> topicLevels = new ArrayList<>();
        topicLevels.add(tenantId);
        return parse(topic, isEscaped, topicLevels);
    }

    // parse a topic or topic filter string into a list of topic levels
    // eg. "/" -> ["",""], "/a" -> ["",a], "a/" -> [a,""]
    public static List<String> parse(String topic, boolean isEscaped) {
        return parse(topic, isEscaped, new ArrayList<>());
    }

    // parse a topic or topic filter string into a list of topic levels
    // eg. "/" -> ["",""], "/a" -> ["",a], "a/" -> [a,""]
    private static List<String> parse(String topic, boolean isEscaped, List<String> topicLevels) {
        char splitter = isEscaped ? NUL_CHAR : DELIMITER_CHAR;
        StringBuilder tl = new StringBuilder();
        for (int i = 0; i < topic.length(); i++) {
            if (topic.charAt(i) == splitter) {
                topicLevels.add(tl.toString());
                tl.delete(0, tl.length());
            } else {
                tl.append(topic.charAt(i));
            }
        }
        topicLevels.add(tl.toString());
        return topicLevels;
    }

    public static boolean isWildcardTopicFilter(String topicFilter) {
        return topicFilter.contains(SINGLE_WILDCARD) || topicFilter.contains(MULTI_WILDCARD);
    }

    public static boolean isNormalTopicFilter(String topicFilter) {
        return !isUnorderedShared(topicFilter) && !isOrderedShared(topicFilter);
    }

    public static boolean isUnorderedShared(String topicFilter) {
        return topicFilter.startsWith(UNORDERED_SHARE);
    }

    public static boolean isOrderedShared(String topicFilter) {
        return topicFilter.startsWith(ORDERED_SHARE);
    }

    public static String fastJoin(CharSequence delimiter, Iterable<? extends CharSequence> strings) {
        StringBuilder sb = new StringBuilder();
        Iterator<? extends CharSequence> itr = strings.iterator();
        while (itr.hasNext()) {
            sb.append(itr.next());
            if (itr.hasNext()) {
                sb.append(delimiter);
            }
        }
        return sb.toString();
    }

    public static <T> String fastJoin(CharSequence delimiter, Iterable<T> items,
                                      Function<T, ? extends CharSequence> toCharSequence) {
        StringBuilder sb = new StringBuilder();
        Iterator<T> itr = items.iterator();
        while (itr.hasNext()) {
            sb.append(toCharSequence.apply(itr.next()));
            if (itr.hasNext()) {
                sb.append(delimiter);
            }
        }
        return sb.toString();
    }
}
