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


import static com.baidu.bifromq.dist.util.TopicUtil.escape;
import static com.baidu.bifromq.dist.util.TopicUtil.isWildcardTopicFilter;
import static com.baidu.bifromq.dist.util.TopicUtil.parse;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class TopicUtilTest {
    @Test
    public void check() {
        assertTrue(isWildcardTopicFilter("#"));
        assertTrue(isWildcardTopicFilter("+"));
        assertTrue(isWildcardTopicFilter("/#"));
        assertTrue(isWildcardTopicFilter("/+"));
        assertFalse(isWildcardTopicFilter("/"));
    }

    @Test
    public void testParse() {
        assertEquals(parse("/", false), Lists.newArrayList("", ""));
        assertEquals(parse(escape("/"), true), Lists.newArrayList("", ""));
        assertEquals(parse("//", false), Lists.newArrayList("", "", ""));
        assertEquals(parse(escape("//"), true), Lists.newArrayList("", "", ""));
        assertEquals(parse(" //", false), Lists.newArrayList(" ", "", ""));
        assertEquals(parse(escape(" //"), true), Lists.newArrayList(" ", "", ""));
        assertEquals(parse(" / / ", false), Lists.newArrayList(" ", " ", " "));
        assertEquals(parse(escape(" / / "), true), Lists.newArrayList(" ", " ", " "));
        assertEquals(parse("a/", false), Lists.newArrayList("a", ""));
        assertEquals(parse(escape("a/"), true), Lists.newArrayList("a", ""));
        assertEquals(parse("a/b", false), Lists.newArrayList("a", "b"));
        assertEquals(parse(escape("a/b"), true), Lists.newArrayList("a", "b"));
        assertEquals(parse("a/b/", false), Lists.newArrayList("a", "b", ""));
        assertEquals(parse(escape("a/b/"), true), Lists.newArrayList("a", "b", ""));
    }

    @Test
    public void testExpand() {
        List<String> topicFilters = TopicUtil.expand(TestUtil.randomTopic());
        List<String> copy = Lists.newArrayList(topicFilters);
        topicFilters.sort(String::compareTo);
        assertEquals(topicFilters, copy);
    }

    @Test
    public void testExpandSysTopic() {
        String topic = "$sys/a/b/c";
        List<String> topicFilters = TopicUtil.expand(topic);
        List<String> copy = Lists.newArrayList(topicFilters);
        topicFilters.sort(String::compareTo);
        assertEquals(topicFilters, copy);
    }

    @Test
    public void testMatch() {
        String topic = TestUtil.randomTopic();
        TopicMatcher matcher = new TopicMatcher(topic);
        List<String> topicFilters = TopicUtil.expand(topic);
        log.debug("Topic '{}' matches {} TopicFilters", topic, topicFilters.size());
        for (int i = topicFilters.size() - 1; i >= 0; i--) {
            String topicFilter = topicFilters.get(i);
            assertTrue(matcher.match(topicFilter));
            if (i < topicFilters.size() - 1) {
                assertEquals(matcher.next(topicFilter).get(), topicFilters.get(i + 1));
            } else {
                assertFalse(matcher.next(topicFilter).isPresent());
            }
        }
    }

    @Test
    public void testMatchSysTopic() {
        String topic = "$sys/baidu/user/event/abc";
        TopicMatcher matcher = new TopicMatcher(topic);
        List<String> topicFilters = TopicUtil.expand(topic);
        for (int i = topicFilters.size() - 1; i >= 0; i--) {
            String topicFilter = topicFilters.get(i);
            assertTrue(matcher.match(topicFilter));
            if (i < topicFilters.size() - 1) {
                assertEquals(matcher.next(topicFilter).get(), topicFilters.get(i + 1));
            } else {
                assertFalse(matcher.next(topicFilter).isPresent());
            }
        }
        assertFalse(matcher.match("#"));
        assertFalse(matcher.match("+"));
        assertFalse(matcher.match("+/+/+/+/+"));
    }

    @Test
    public void testMatchRandomly() {
        int j = 100;
        while (j-- > 0) {
            String topic = TestUtil.randomTopic();
            TopicMatcher matcher = new TopicMatcher(topic);
            NavigableSet<String> topicFilters = Sets.newTreeSet(TopicUtil.expand(topic));
            int i = 100;
            while (i-- > 0) {
                String topicFilter = TestUtil.randomTopicFilter();
                boolean matched = matcher.match(escape(topicFilter));
                if (!Optional.ofNullable(topicFilters.higher(escape(topicFilter)))
                    .equals(matcher.next(escape(topicFilter)))) {
                    log.info("'{}' matches '{}'? {}, next '{}'",
                        topicFilter, topic, matched, matcher.next(escape(topicFilter)).orElse(null));
                    fail();
                }
            }
        }
    }
}
