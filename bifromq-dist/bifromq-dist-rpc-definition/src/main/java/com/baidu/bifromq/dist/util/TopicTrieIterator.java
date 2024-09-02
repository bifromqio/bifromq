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

import static com.baidu.bifromq.dist.util.TopicUtil.fastJoin;
import static com.baidu.bifromq.util.TopicConst.DELIMITER;
import static java.util.Collections.singleton;

import com.baidu.bifromq.type.TopicMessage;
import com.google.common.collect.Lists;
import java.util.LinkedList;
import java.util.List;

public interface TopicTrieIterator {
    static void iterate(TrieNode root, TopicTrieIterator iterator) {
        LinkedList<LinkedList<TrieNode>> toVisit = Lists.newLinkedList();
        toVisit.add(Lists.newLinkedList(singleton(root)));
        while (!toVisit.isEmpty()) {
            LinkedList<TrieNode> current = toVisit.poll();
            TrieNode lastTopicLevel = current.getLast();
            if (lastTopicLevel.isLastTopicLevel()) {
                TrieNode first = current.removeFirst();
                iterator.next(fastJoin(DELIMITER, current, TrieNode::levelName), lastTopicLevel.messages());
                current.addFirst(first);
            }
            List<TrieNode> children = lastTopicLevel.children();
            for (int i = children.size() - 1; i >= 0; i--) {
                LinkedList<TrieNode> next = Lists.newLinkedList(current);
                next.addLast(children.get(i));
                toVisit.addFirst(next);
            }
        }
    }

    void next(String topic, Iterable<TopicMessage> messages);
}
