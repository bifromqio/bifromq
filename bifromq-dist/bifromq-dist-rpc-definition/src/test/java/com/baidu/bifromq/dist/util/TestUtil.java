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

package com.baidu.bifromq.dist.util;

import static com.baidu.bifromq.dist.util.TopicUtil.SYS_PREFIX;

import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;

public class TestUtil {
    public static String randomTopic() {
        return randomTopic(16);
    }

    public static String randomTopicFilter() {
        return randomTopicFilter(16);
    }

    public static String randomTopic(int maxLevel) {
        int levels = ThreadLocalRandom.current().nextInt(1, maxLevel + 1);
        String[] topicLevels = new String[levels];
        for (int i = 0; i < levels; i++) {
            topicLevels[i] = RandomTopicName.nextString(8);
        }
        if (ThreadLocalRandom.current().nextFloat() > 0.5) {
            topicLevels[0] = SYS_PREFIX + topicLevels[0];// make
        }
        String topic = String.join("/", topicLevels);
        return ThreadLocalRandom.current().nextFloat() > 0.5 ? "/" + topic : topic;
    }

    public static String randomTopicFilter(int maxLevel) {
        int levels = ThreadLocalRandom.current().nextInt(1, maxLevel + 1);
        String[] filterLevels = new String[levels];
        for (int i = 0; i < levels; i++) {
            filterLevels[i] = RandomTopicName.nextString(8, true, i < levels - 1);
        }
        return String.join("/", filterLevels);
    }

    private static class RandomTopicName {
        public static final String upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

        public static final String lower = upper.toLowerCase(Locale.ROOT);

        public static final String digits = "0123456789";

        public static final String puncs = " !\"$%&'()*,-.";

        private static final ThreadLocalRandom random = ThreadLocalRandom.current();

        private static final char[] symbols = (upper + lower + digits + puncs).toCharArray();

        public static String nextString(int maxLength) {
            int strLen = random.nextInt(1, maxLength);
            char[] buf = new char[strLen];
            for (int idx = 0; idx < buf.length; ++idx) {
                buf[idx] = symbols[random.nextInt(symbols.length)];
            }
            return new String(buf);
        }

        public static String nextString(int maxLength, boolean includeWildcard, boolean singleWildcardOnly) {
            if (includeWildcard && random.nextFloat() > 0.5) {
                if (singleWildcardOnly) {
                    return "+";
                } else {
                    return "#";
                }
            } else {
                return nextString(maxLength);
            }
        }
    }
}
