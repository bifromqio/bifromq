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


import static com.baidu.bifromq.dist.util.TopicUtil.escape;
import static com.baidu.bifromq.dist.util.TopicUtil.isWildcardTopicFilter;
import static com.baidu.bifromq.dist.util.TopicUtil.parse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

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
        assertEquals(Lists.newArrayList("", ""), parse("/", false));
        assertEquals(Lists.newArrayList("", ""), parse(escape("/"), true));
        assertEquals(Lists.newArrayList("", "", ""), parse("//", false));
        assertEquals(Lists.newArrayList("", "", ""), parse(escape("//"), true));
        assertEquals(Lists.newArrayList(" ", "", ""), parse(" //", false));
        assertEquals(Lists.newArrayList(" ", "", ""), parse(escape(" //"), true));
        assertEquals(Lists.newArrayList(" ", " ", " "), parse(" / / ", false));
        assertEquals(Lists.newArrayList(" ", " ", " "), parse(escape(" / / "), true));
        assertEquals(Lists.newArrayList("a", ""), parse("a/", false));
        assertEquals(Lists.newArrayList("a", ""), parse(escape("a/"), true));
        assertEquals(Lists.newArrayList("a", "b"), parse("a/b", false));
        assertEquals(Lists.newArrayList("a", "b"), parse(escape("a/b"), true));
        assertEquals(Lists.newArrayList("a", "b", ""), parse("a/b/", false));
        assertEquals(Lists.newArrayList("a", "b", ""), parse(escape("a/b/"), true));
    }

    @Test
    public void testExpand() {
        long s = System.nanoTime();
        String topic = TestUtil.randomTopic();
        printExpand(topic);
        log.debug("{} in {}ms", TopicUtil.expand(topic).size(), Duration.ofNanos(System.nanoTime() - s).toMillis());
    }

    @Test
    public void testExpandSysTopic() {
        long s = System.nanoTime();
        String topic = "$sys/a/b/c";
        printExpand(topic);
        log.debug("{} in {}ms", TopicUtil.expand(topic).size(), Duration.ofNanos(System.nanoTime() - s).toMillis());
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
                assertEquals(topicFilters.get(i + 1), matcher.next(topicFilter).get());
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
                assertEquals(topicFilters.get(i + 1), matcher.next(topicFilter).get());
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

    private void printExpand(String topic) {
        TopicUtil.expand(topic).forEach(tf -> log.debug("{}", tf));
    }
}
