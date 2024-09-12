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

package com.baidu.bifromq.dist.trie;

import static com.baidu.bifromq.util.TopicConst.MULTI_WILDCARD;
import static com.baidu.bifromq.util.TopicConst.SINGLE_WILDCARD;

import com.google.common.collect.Sets;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Single level topic filter trie node.
 *
 * @param <V> value type
 */
final class STopicFilterTrieNode<V> extends TopicFilterTrieNode<V> {
    private final Set<TopicTrieNode<V>> siblingTopicTrieNodes;
    private final NavigableSet<String> subLevelNames;
    private final NavigableMap<String, Set<TopicTrieNode<V>>> subTopicTrieNodes;
    private final Set<TopicTrieNode<V>> subWildcardMatchableTopicTrieNodes;

    // point to the sub node during iteration
    private String subLevelName;

    STopicFilterTrieNode(TopicFilterTrieNode<V> parent,
                         Set<TopicTrieNode<V>> siblingTopicTrieNodes) {
        super(parent);
        this.siblingTopicTrieNodes = siblingTopicTrieNodes;
        this.subLevelNames = new TreeSet<>();
        this.subTopicTrieNodes = new TreeMap<>();
        this.subWildcardMatchableTopicTrieNodes = new HashSet<>();
        for (TopicTrieNode<V> sibling : siblingTopicTrieNodes) {
            for (Map.Entry<String, TopicTrieNode<V>> entry : sibling.children().entrySet()) {
                TopicTrieNode<V> subNode = entry.getValue();
                if (subNode.wildcardMatchable()) {
                    subWildcardMatchableTopicTrieNodes.add(subNode);
                }
                subTopicTrieNodes.computeIfAbsent(subNode.levelName(), k -> new HashSet<>()).add(subNode);
                subLevelNames.add(subNode.levelName());
            }

        }
        // # match parent
        if (!backingTopics().isEmpty()) {
            subLevelNames.add(MULTI_WILDCARD);
        }
        if (!subWildcardMatchableTopicTrieNodes.isEmpty()) {
            subLevelNames.add(MULTI_WILDCARD);
            subLevelNames.add(SINGLE_WILDCARD);
        }
        // point to first child after init
        seekChild("");
    }

    @Override
    String levelName() {
        return SINGLE_WILDCARD;
    }

    @Override
    Set<TopicTrieNode<V>> backingTopics() {
        Set<TopicTrieNode<V>> topics = Sets.newHashSet();
        for (TopicTrieNode<V> sibling : siblingTopicTrieNodes) {
            if (sibling.isUserTopic()) {
                topics.add(sibling);
            }
        }
        return topics;
    }

    @Override
    void seekChild(String childLevelName) {
        if (!subLevelNames.isEmpty()) {
            subLevelName = subLevelNames.ceiling(childLevelName);
        }
    }

    @Override
    void seekPrevChild(String childLevelName) {
        if (!subLevelNames.isEmpty()) {
            subLevelName = subLevelNames.floor(childLevelName);
        }
    }

    @Override
    void seekToFirstChild() {
        if (!subLevelNames.isEmpty()) {
            subLevelName = subLevelNames.first();
        }
    }

    @Override
    void seekToLastChild() {
        if (!subLevelNames.isEmpty()) {
            subLevelName = subLevelNames.last();
        }
    }

    @Override
    boolean atValidChild() {
        return subLevelName != null;
    }

    @Override
    void nextChild() {
        if (subLevelName != null) {
            subLevelName = subLevelNames.higher(subLevelName);
        }
    }

    @Override
    void prevChild() {
        if (subLevelName != null) {
            subLevelName = subLevelNames.lower(subLevelName);
        }
    }

    @Override
    TopicFilterTrieNode<V> childNode() {
        if (subLevelName == null) {
            throw new NoSuchElementException();
        }
        return switch (subLevelName) {
            case MULTI_WILDCARD -> new MTopicFilterTrieNode<>(this, subWildcardMatchableTopicTrieNodes);
            case SINGLE_WILDCARD -> new STopicFilterTrieNode<>(this, subWildcardMatchableTopicTrieNodes);
            default -> new NTopicFilterTrieNode<>(this, subLevelName, subTopicTrieNodes.get(subLevelName));
        };
    }
}
