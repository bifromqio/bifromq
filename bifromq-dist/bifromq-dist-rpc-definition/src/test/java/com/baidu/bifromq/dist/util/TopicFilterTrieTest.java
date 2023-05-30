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

import static com.baidu.bifromq.dist.util.TestUtil.randomTopic;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.baidu.bifromq.type.TopicMessage;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public class TopicFilterTrieTest {

    @Test
    public void iterateOverEmptyTrie() {
        TopicFilterTrieIterator itr = new TopicFilterTrieIterator(new TopicTrie());
        assertFalse(itr.hasNext());
    }

    @Test
    public void expandTopic() {
        TopicTrie topicTrie = new TopicTrie();
        String topic = randomTopic();
        topicTrie.add(topic, Collections.singleton(TopicMessage.newBuilder().setTopic(topic).build()));
        TopicFilterTrieIterator itr = new TopicFilterTrieIterator(topicTrie);
        List<String> allFilters = TopicUtil.expand(topicTrie);
        assertEquals(allFilters, Lists.newArrayList(itr));
    }
}
