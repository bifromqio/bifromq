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

import static com.baidu.bifromq.dist.util.TopicUtil.NUL;
import static com.baidu.bifromq.dist.util.TopicUtil.fastJoin;

import com.google.common.collect.AbstractIterator;
import java.util.Iterator;
import javax.annotation.CheckForNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.map.LinkedMap;

@Slf4j
public class TopicFilterTrieIterator extends AbstractIterator<String> {
    private final TopicFilterTrie root;
    private final LinkedMap<TopicFilterTrie, Iterator<TopicFilterTrie>> current;

    public TopicFilterTrieIterator(TopicTrie topicTrie) {
        root = TopicFilterTrie.build(topicTrie);
        this.current = new LinkedMap<>();
        this.current.put(root, root.children());
    }

    public void forward(String escapedTopicFilter) {
        root.forward(TopicUtil.parse(escapedTopicFilter, true));
        this.current.clear();
        this.current.put(root, root.children());
    }

    @CheckForNull
    @Override
    protected String computeNext() {
        findNext();
        if (current.isEmpty()) {
            return endOfData();
        }
        return fastJoin(NUL, current.keySet(), TopicFilterTrie::levelName).substring(2);
    }

    private void findNext() {
        if (current.isEmpty()) {
            return;
        }
        TopicFilterTrie node = current.lastKey();
        Iterator<TopicFilterTrie> childItr = current.get(node);
        if (childItr.hasNext()) {
            TopicFilterTrie child = childItr.next();
            current.put(child, child.children());
            if (!child.isMatchTopic()) {
                findNext();
            }
        } else {
            current.remove(node);
            findNext();
        }
    }
}
