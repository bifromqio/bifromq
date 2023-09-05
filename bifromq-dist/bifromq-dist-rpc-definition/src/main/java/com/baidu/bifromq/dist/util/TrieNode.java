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

import com.baidu.bifromq.type.TopicMessage;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TrieNode {
    public static final TrieNode MULTI = new TrieNode("#");
    public static final TrieNode SINGLE = new TrieNode("+");
    private final String levelName;
    private final Map<String, TrieNode> childMap = new HashMap<>();
    private boolean isLastTopicLevel;
    private Iterable<TopicMessage> messages;
    private List<TrieNode> sortedChildList;

    public TrieNode() {
        this(TopicUtil.NUL);
    }

    public TrieNode(String levelName) {
        this(levelName, new LinkedList<>(), Collections.emptyList());
    }

    public TrieNode(String levelName,
                    Iterable<TopicMessage> msgs,
                    Iterable<TrieNode> childList) {
        this.levelName = levelName;
        messages = msgs;
        isLastTopicLevel = messages.iterator().hasNext();
        childList.forEach(child -> childMap.put(child.levelName, child));
    }

    public String levelName() {
        return levelName;
    }

    public boolean isLastTopicLevel() {
        return isLastTopicLevel;
    }

    public Iterable messages() {
        return messages;
    }

    public List<TrieNode> children() {
        if (sortedChildList == null) {
            sortedChildList = Lists.newArrayList(childMap.values());
            sortedChildList.sort(Comparator.comparing(TrieNode::levelName));
        }
        return sortedChildList;
    }

    public boolean add(List<String> topicLevels, Iterable<TopicMessage> messages) {
        assert !topicLevels.isEmpty();
        String childLevelName = topicLevels.get(0);
        TrieNode child = childMap.computeIfAbsent(childLevelName, k -> {
            // new child added, reset the sorted list
            sortedChildList = null;
            return new TrieNode(k);
        });
        if (topicLevels.size() > 1) {
            return child.add(topicLevels.subList(1, topicLevels.size()), messages);
        } else {
            boolean newTopic = !child.isLastTopicLevel();
            child.addMessages(messages);
            assert child.isLastTopicLevel();
            return newTopic;
        }
    }

    public TrieNode duplicate(String levelName) {
        return new TrieNode(levelName, this.messages, this.childMap.values());
    }

    void addMessages(Iterable<TopicMessage> msgs) {
        messages = Iterables.concat(messages, msgs);
        isLastTopicLevel = messages.iterator().hasNext();
    }
}
