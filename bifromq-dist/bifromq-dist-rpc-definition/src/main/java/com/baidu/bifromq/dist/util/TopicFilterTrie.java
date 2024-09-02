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

import static com.baidu.bifromq.util.TopicConst.MULTI_WILDCARD;
import static com.baidu.bifromq.util.TopicConst.NUL;
import static com.baidu.bifromq.util.TopicConst.SINGLE_WILDCARD;
import static com.baidu.bifromq.util.TopicConst.SYS_PREFIX;
import static com.google.common.collect.Lists.newLinkedList;
import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptyList;

import com.google.common.collect.AbstractIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import javax.annotation.CheckForNull;

public abstract class TopicFilterTrie {
    private static final List<TrieNode> MULTI_FILTER = List.of(TrieNode.MULTI);
    private static final List<TrieNode> MULTI_SINGLE_FILTER = List.of(TrieNode.MULTI, TrieNode.SINGLE);

    public static TopicFilterTrie build(TopicTrie trie) {
        return new TopicFilterTrieRoot(trie.root());
    }

    abstract String levelName();

    abstract boolean isMatchTopic();

    abstract Iterator<TopicFilterTrie> children();

    abstract void forward(List<String> filterLevels);

    private static class TopicFilterTrieRoot extends TopicFilterTrie {
        private final TrieNode root;
        private final boolean allSySTopics;
        private final IChildren children;

        TopicFilterTrieRoot(TrieNode trieRoot) {
            assert trieRoot.levelName().equals(NUL);
            this.root = trieRoot;
            allSySTopics = root.children().stream().allMatch(child -> child.levelName().startsWith(SYS_PREFIX));
            if (root.children().isEmpty()) {
                children = NoChildren.INSTANCE;
            } else {
                if (allSySTopics) {
                    children = new MergedChildren(List.of(root.children()), true);
                } else {
                    List<List<TrieNode>> mergingChildren = List.of(MULTI_SINGLE_FILTER, root.children());
                    children = new MergedChildren(mergingChildren, true);
                }
            }
        }

        String levelName() {
            return root.levelName();
        }

        @Override
        boolean isMatchTopic() {
            return false;
        }

        @Override
        Iterator<TopicFilterTrie> children() {
            return children.iterator();
        }

        @Override
        void forward(List<String> filterLevels) {
            // filterLevels must be no less than floorFilterLevels in byte-wise order
            children.forward(filterLevels);
        }
    }

    private static class TopicFilterTrieMultiLevel extends TopicFilterTrie {
        static final TopicFilterTrie INSTANCE = new TopicFilterTrieMultiLevel();

        @Override
        String levelName() {
            return MULTI_WILDCARD;
        }

        @Override
        boolean isMatchTopic() {
            return true;
        }

        @Override
        Iterator<TopicFilterTrie> children() {
            return emptyIterator();
        }

        @Override
        void forward(List<String> filterLevels) {

        }
    }

    private static class TopicFilterTrieMergedLevel extends TopicFilterTrie {
        private final String levelName;
        private final boolean isMatchTopic;

        private final IChildren children;

        TopicFilterTrieMergedLevel(String levelName, List<List<TrieNode>> childrenList, boolean isMatchTopic) {
            this.levelName = levelName;
            this.isMatchTopic = isMatchTopic;
            List<List<TrieNode>> mergingChildren = new ArrayList<>(childrenList.size() + 1);
            if (!levelName.isEmpty()) {
                // # matching parent level
                mergingChildren.add(childrenList.isEmpty() ? MULTI_FILTER : MULTI_SINGLE_FILTER);
                mergingChildren.addAll(childrenList);
            } else if (!childrenList.isEmpty()) {
                mergingChildren.add(MULTI_SINGLE_FILTER);
                mergingChildren.addAll(childrenList);
            }
            children = new MergedChildren(mergingChildren);
        }

        @Override
        String levelName() {
            return levelName;
        }

        @Override
        boolean isMatchTopic() {
            return isMatchTopic;
        }

        @Override
        Iterator<TopicFilterTrie> children() {
            return children.iterator();
        }

        @Override
        void forward(List<String> filterLevels) {
            children.forward(filterLevels);
        }
    }

    private interface IChildren extends Iterable<TopicFilterTrie> {
        void forward(List<String> filterLevels);
    }

    private static class NoChildren implements IChildren {
        public static final IChildren INSTANCE = new NoChildren();

        @Override
        public void forward(List<String> filterLevels) {

        }

        @Override
        public Iterator<TopicFilterTrie> iterator() {
            return emptyIterator();
        }
    }

    private static class MergedChildren implements IChildren {
        private final List<List<TrieNode>> mergingChildren;
        private final boolean skipSysTopic;
        private final ChildAccessor childAccessor;


        MergedChildren(List<List<TrieNode>> mergingChildren) {
            this(mergingChildren, false);
        }

        MergedChildren(List<List<TrieNode>> mergingChildren, boolean skipSysTopic) {
            this.mergingChildren = mergingChildren;
            this.skipSysTopic = skipSysTopic;
            this.childAccessor = new ChildAccessor();
            forward(emptyList());
        }

        public void forward(List<String> filterLevels) {
            childAccessor.forward(filterLevels);
        }

        @Override
        public Iterator<TopicFilterTrie> iterator() {
            return new AbstractIterator<>() {
                private final ChildAccessor childIterAccessor = childAccessor.duplicate();

                @CheckForNull
                @Override
                protected TopicFilterTrie computeNext() {
                    TopicFilterTrie next = childIterAccessor.currentFilterTrie();
                    if (next == null) {
                        return endOfData();
                    }
                    // find next
                    childIterAccessor.next();
                    return next;
                }
            };
        }

        private class ChildAccessor {
            private final int[] pos;
            private int next = -1;
            private TopicFilterTrie filterTrie = null; // the generated filter trie corresponding to current trie node
            private boolean done;

            ChildAccessor() {
                pos = new int[mergingChildren.size()];
                findNext();
            }

            ChildAccessor(int[] pos, int next, boolean done, TopicFilterTrie filterTrie) {
                this.pos = Arrays.copyOf(pos, pos.length);
                this.next = next;
                this.done = done;
                this.filterTrie = filterTrie;
            }

            ChildAccessor duplicate() {
                return new ChildAccessor(pos, next, done, filterTrie);
            }

            TopicFilterTrie currentFilterTrie() {
                return filterTrie;
            }

            void forward(List<String> filterLevels) {
                Optional<TrieNode> currentChild = current();
                if (!currentChild.isPresent()) {
                    return;
                }
                List<String> childFloorFilterLevels = emptyList();
                if (!filterLevels.isEmpty()) {
                    String floorLevelName = filterLevels.get(0);
                    int c = currentChild.get().levelName().compareTo(floorLevelName);
                    if (c == 0) {
                        // level name match
                        if (filterLevels.size() > 1) {
                            // more filter levels to compare
                            childFloorFilterLevels = filterLevels.subList(1, filterLevels.size());
                        } else {
                            // current level equals or greater
                            childFloorFilterLevels = emptyList();
                        }
                    } else if (c < 0) {
                        // forward to next smallest
                        advance();
                        currentChild = current();
                        while (currentChild.isPresent()) {
                            if (currentChild.get().levelName().compareTo(floorLevelName) < 0) {
                                advance();
                                currentChild = current();
                            } else {
                                break;
                            }
                        }
                        if (currentChild.isPresent()) {
                            if (currentChild.get().levelName().compareTo(floorLevelName) == 0) {
                                // level name match
                                if (filterLevels.size() > 1) {
                                    // more filter levels to compare
                                    childFloorFilterLevels = filterLevels.subList(1, filterLevels.size());
                                } else {
                                    // current level equals or greater
                                    childFloorFilterLevels = emptyList();
                                }
                            } else {
                                // level name is greater, no more filter levels to compare
                                childFloorFilterLevels = emptyList();
                            }
                        }
                    }
                } else {
                    childFloorFilterLevels = emptyList();
                }
                if (currentChild.isPresent()) {
                    genFilterTrie(childFloorFilterLevels.isEmpty());
                    currentFilterTrie().forward(childFloorFilterLevels);
                }
            }

            void next() {
                if (advance()) {
                    genFilterTrie(true);
                }
            }

            // if has next
            private boolean advance() {
                if (next >= 0) {
                    pos[next]++;
                    findNext();
                }
                filterTrie = null;
                return next >= 0;
            }

            private void findNext() {
                if (done) {
                    return;
                }
                next = -1;
                for (int i = 0; i < pos.length; i++) {
                    if (pos[i] < mergingChildren.get(i).size()) {
                        if (next < 0) {
                            next = i;
                        } else {
                            if (mergingChildren.get(i).get(pos[i]).levelName()
                                .compareTo(mergingChildren.get(next).get(pos[next]).levelName()) < 0) {
                                next = i;
                            }
                        }
                    }
                }
                done = next < 0;
            }

            private Optional<TrieNode> current() {
                return next >= 0 ? Optional.of(mergingChildren.get(next).get(pos[next])) : Optional.empty();
            }

            private void genFilterTrie(boolean startMatching) {
                assert current().isPresent();
                TrieNode node = current().get();
                switch (node.levelName()) {
                    case MULTI_WILDCARD: {
                        filterTrie = TopicFilterTrie.TopicFilterTrieMultiLevel.INSTANCE;
                        break;
                    }
                    case SINGLE_WILDCARD: {
                        // merge children's children
                        LinkedList<List<TrieNode>> grandChildren = newLinkedList();
                        boolean isMatchTopic = false;
                        for (int i = 1; i < mergingChildren.size(); i++) {
                            for (TrieNode child : mergingChildren.get(i)) {
                                if (skipSysTopic && child.levelName().startsWith(SYS_PREFIX)) {
                                    continue;
                                }
                                if (child.isLastTopicLevel()) {
                                    isMatchTopic = true;
                                }
                                if (!child.children().isEmpty()) {
                                    grandChildren.add(child.children());
                                }
                            }
                        }
                        filterTrie = new TopicFilterTrie.TopicFilterTrieMergedLevel(SINGLE_WILDCARD, grandChildren,
                            startMatching && isMatchTopic);
                        break;
                    }
                    default: {
                        LinkedList<List<TrieNode>> grandChildren = newLinkedList();
                        if (!node.children().isEmpty()) {
                            grandChildren.add(node.children());
                        }
                        boolean isMatchTopic = node.isLastTopicLevel();

                        // merge all children of trie node with same level name
                        ChildAccessor childAccessor = duplicate();
                        childAccessor.advance();
                        Optional<TrieNode> nextNode = childAccessor.current();
                        while (nextNode.isPresent()) {
                            if (nextNode.get().levelName().equals(node.levelName())) {
                                isMatchTopic |= nextNode.get().isLastTopicLevel();
                                grandChildren.add(nextNode.get().children());
                                childAccessor.advance();
                                nextNode = childAccessor.current();
                            } else {
                                break;
                            }
                        }
                        filterTrie = new TopicFilterTrie.TopicFilterTrieMergedLevel(node.levelName(), grandChildren,
                            startMatching && isMatchTopic);
                        break;
                    }
                }
            }
        }
    }
}

