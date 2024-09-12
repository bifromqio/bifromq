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

import com.google.common.collect.Sets;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Multi-level topic filter trie node.
 *
 * @param <V> value type
 */
final class MTopicFilterTrieNode<V> extends TopicFilterTrieNode<V> {
    private final Set<TopicTrieNode<V>> siblingTopicTrieNodes;

    MTopicFilterTrieNode(TopicFilterTrieNode<V> parent,
                         Set<TopicTrieNode<V>> siblingTopicTrieNodes) {
        super(parent);
        this.siblingTopicTrieNodes = siblingTopicTrieNodes;
    }

    @Override
    String levelName() {
        return MULTI_WILDCARD;
    }

    @Override
    Set<TopicTrieNode<V>> backingTopics() {
        Set<TopicTrieNode<V>> topics = Sets.newHashSet();
        if (parent != null) {
            topics.addAll(parent.backingTopics());
        }
        for (TopicTrieNode<V> sibling : siblingTopicTrieNodes) {
            collectTopics(sibling, topics);
        }
        return topics;
    }

    private void collectTopics(TopicTrieNode<V> node, Set<TopicTrieNode<V>> topics) {
        if (node.isUserTopic()) {
            topics.add(node);
        }
        for (TopicTrieNode<V> child : node.children().values()) {
            collectTopics(child, topics);
        }
    }

    @Override
    void seekChild(String childLevelName) {

    }

    @Override
    void seekPrevChild(String childLevelName) {

    }

    @Override
    void seekToFirstChild() {

    }

    @Override
    void seekToLastChild() {

    }

    @Override
    boolean atValidChild() {
        return false;
    }

    @Override
    void nextChild() {

    }

    @Override
    void prevChild() {

    }

    @Override
    TopicFilterTrieNode<V> childNode() {
        throw new NoSuchElementException();
    }
}
