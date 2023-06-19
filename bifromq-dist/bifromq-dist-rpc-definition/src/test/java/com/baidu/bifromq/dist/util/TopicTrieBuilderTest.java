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

import static org.testng.AssertJUnit.assertEquals;

import com.baidu.bifromq.type.TopicMessage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class TopicTrieBuilderTest {
    @Test
    public void testStats() {
        TopicTrie trie = new TopicTrie();
        assertEquals(0, trie.topicCount());

        trie.add("a", Collections.singleton(TopicMessage.newBuilder().setTopic("a").build()));
        assertEquals(1, trie.topicCount());

        trie.add("a", Collections.singleton(TopicMessage.newBuilder().setTopic("a").build()));
        assertEquals(1, trie.topicCount());

        trie.add("a/", Collections.singleton(TopicMessage.newBuilder().setTopic("a/").build()));
        assertEquals(2, trie.topicCount());
    }

    @Test
    public void testIterate() {
        TopicTrie trie = new TopicTrie();
        int topicCount = 3;
        List<String> topics = new ArrayList<>(topicCount);
        for (int i = 0; i < topicCount; i++) {
            topics.add("" + i);
        }
        topics.sort(Comparator.comparing(TopicUtil::escape));
        topics.forEach(
            topic -> trie.add(topic, Collections.singleton(TopicMessage.newBuilder().setTopic(topic).build())));
        TrieNode root = trie.root();

        List<String> toVerify = new ArrayList<>();
        TopicTrieIterator.iterate(root, (topic, msgs) -> toVerify.add(topic));
        for (int i = 0; i < topics.size(); i++) {
            if (!topics.get(i).equals(toVerify.get(i))) {
                log.info("{}: {}", i, topics.get(i));
            }
        }
        assertEquals(topics, toVerify);
    }
}
