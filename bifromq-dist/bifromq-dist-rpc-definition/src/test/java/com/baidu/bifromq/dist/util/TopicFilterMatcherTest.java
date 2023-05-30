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
import static com.baidu.bifromq.dist.util.TopicUtil.parse;
import static com.baidu.bifromq.dist.util.TopicUtil.unescape;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.baidu.bifromq.type.TopicMessage;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public class TopicFilterMatcherTest {

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

//    @Test
//    public void testExpand() {
//        TopicTrieBuilder builder = new TopicTrieBuilder();
//        builder.add("a/", List.of(ClientMessages.getDefaultInstance()));
//        TopicTrieNode root = builder.build();
//
//        TopicUtil.expand(root).forEach(tf -> log.info("TopicFilter {}", TopicUtil.unescape(tf)));
//    }

    @Test
    public void testEmptyMatcher() {
        TopicTrie trie = new TopicTrie();
        TopicFilterMatcher topicFilterMatcher = new TopicFilterMatcher(trie);
        assertTrue(TopicUtil.expand(trie).isEmpty());
        assertFalse(topicFilterMatcher.match(TopicUtil.escape(TestUtil.randomTopicFilter())).isPresent());
    }

    @Test
    public void testMatch() {
        long s = System.nanoTime();
        List<String> topics = new ArrayList<>();
        int topicCount = ThreadLocalRandom.current().nextInt(1, 11);
        log.info("Expanding {} topics", topicCount);
        for (int i = 0; i < topicCount; i++) {
            String topic = TestUtil.randomTopic();
            topics.add(topic);
        }
        TopicTrie trie = new TopicTrie();
        topics.forEach(
            topic -> trie.add(topic, Collections.singleton(TopicMessage.newBuilder().setTopic(topic).build())));
        List<String> topicFilters = TopicUtil.expand(trie);
        log.info("Expand {} topics into {} topic filters costs {}ms", topicCount, topicFilters.size(),
            Duration.ofNanos(System.nanoTime() - s).toMillis());
        TreeSet<String> verifySet = Sets.newTreeSet(topicFilters);
        assertEquals(Lists.newArrayList(verifySet), topicFilters);

        TopicFilterMatcher matcher = new TopicFilterMatcher(trie);
        for (String topicFilter : topicFilters) {
            Optional<Map<String, Iterable<TopicMessage>>> matched = matcher.match(topicFilter);
            assertTrue(matched.isPresent());
            assertTrue(matched.get().keySet().stream().anyMatch(t -> topics.contains(t)));
            assertTrue(verifySet.contains(topicFilter));
        }
    }

    @Test
    public void testFindingHigherTopicFilter() {
        long s = System.nanoTime();
        List<String> topics = new ArrayList<>();
//        int topicCount = ThreadLocalRandom.current().nextInt(1, 11);
        int topicCount = 10;
        log.info("Expanding {} topics", topicCount);
        for (int i = 0; i < topicCount; i++) {
            String topic = TestUtil.randomTopic();
            topics.add(topic);
        }
        TopicTrie trie = new TopicTrie();
        topics.forEach(
            topic -> trie.add(topic, Collections.singleton(TopicMessage.newBuilder().setTopic(topic).build())));
        List<String> topicFilters = TopicUtil.expand(trie);
        log.info("Expand {} topics into {} topic filters costs {}ms", topicCount, topicFilters.size(),
            Duration.ofNanos(System.nanoTime() - s).toMillis());
        TreeSet<String> verifySet = Sets.newTreeSet(topicFilters);
        assertEquals(Lists.newArrayList(verifySet), topicFilters);


        // test set includes additional random topic filters which may not match
        TreeSet<String> testSet = Sets.newTreeSet(topicFilters);
        for (int i = 0; i < verifySet.size(); i++) {
            testSet.add(TestUtil.randomTopicFilter());
        }

        TopicFilterMatcher matcher = new TopicFilterMatcher(trie);
        for (String topicFilter : testSet) {
            Optional<String> nextTopicFilter = matcher.higher(topicFilter);
            if (!Optional.ofNullable(verifySet.higher(topicFilter)).equals(nextTopicFilter)) {
                log.error("Expect {} after matching {}, but is {}",
                    unescape(verifySet.higher(topicFilter)),
                    unescape(topicFilter),
                    unescape(nextTopicFilter.orElse("")));
                fail();
            }
        }
    }

}
