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

import static com.baidu.bifromq.util.TopicConst.DELIMITER;
import static com.baidu.bifromq.util.TopicConst.DELIMITER_CHAR;
import static com.baidu.bifromq.util.TopicConst.MULTI_WILDCARD;
import static com.baidu.bifromq.util.TopicConst.NUL;
import static com.baidu.bifromq.util.TopicConst.NUL_CHAR;
import static com.baidu.bifromq.util.TopicConst.ORDERED_SHARE;
import static com.baidu.bifromq.util.TopicConst.SINGLE_WILDCARD;
import static com.baidu.bifromq.util.TopicConst.SYS_PREFIX;
import static com.baidu.bifromq.util.TopicConst.UNORDERED_SHARE;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newLinkedList;
import static java.util.Collections.singleton;

import com.baidu.bifromq.type.TopicMessage;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TopicUtil {
    public static List<String> expand(String topic) {
        List<String> topicLevels = parse(topic, false);
        List<String> topicFilters = newArrayList();
        LinkedList<LinkedList<String>> toVisit = newLinkedList();
        String rootLevel = topicLevels.get(0);
        if (rootLevel.startsWith(SYS_PREFIX)) {
            // sys topic don't match "#" and "+"
            toVisit.add(newLinkedList(singleton(rootLevel)));
        } else {
            toVisit.addAll(toFilters(rootLevel));
        }
        while (!toVisit.isEmpty()) {
            LinkedList<String> currentFilterLevels = toVisit.pollFirst();
            switch (currentFilterLevels.getLast()) {
                case MULTI_WILDCARD:
                    topicFilters.add(fastJoin(NUL, currentFilterLevels));
                    break;
                case SINGLE_WILDCARD:
                default:
                    if (currentFilterLevels.size() == topicLevels.size()) {
                        String tf = fastJoin(NUL, currentFilterLevels);
                        topicFilters.add(tf);
                        if (!currentFilterLevels.getLast().isEmpty()) {
                            // "#" also matches parent level if current level name is not empty string
                            topicFilters.add(fastJoin(NUL, List.of(tf, MULTI_WILDCARD)));
                        }
                    } else {
                        toFilters(topicLevels.get(currentFilterLevels.size()))
                            .descendingIterator()
                            .forEachRemaining(f -> {
                                currentFilterLevels.descendingIterator().forEachRemaining(f::addFirst);
                                toVisit.addFirst(f);
                            });
                    }
            }
        }
        return topicFilters;
    }

    public static List<String> expand(TopicTrie trie) {
        List<String> topicFilters = newArrayList();
        LinkedList<LinkedList<TrieNode>> toVisit = newLinkedList();
        toVisit.addAll(genFilters(trie.root()));
        while (!toVisit.isEmpty()) {
            LinkedList<TrieNode> currentFilterLevels = toVisit.pollFirst();
            TrieNode lastFilter = currentFilterLevels.getLast();
            switch (lastFilter.levelName()) {
                case MULTI_WILDCARD:
                    topicFilters.add(fastJoin(NUL, currentFilterLevels, TrieNode::levelName));
                    break;
                case SINGLE_WILDCARD:
                default:
                    TrieNode lastFilterLevel = currentFilterLevels.getLast();
                    if (lastFilterLevel.isLastTopicLevel()) {
                        String tf = fastJoin(NUL, currentFilterLevels, TrieNode::levelName);
                        topicFilters.add(tf);
                    }
                    genFilters(lastFilterLevel).descendingIterator()
                        .forEachRemaining(f -> {
                            currentFilterLevels.descendingIterator().forEachRemaining(f::addFirst);
                            toVisit.addFirst(f);
                        });
            }
        }
        return topicFilters;
    }

    public static Optional<String> findNext(List<String> topicLevels, String escapedTopicFilter) {
        // the algorithm is O(log(n))
        String rootLevel = topicLevels.get(0);
        LinkedList<LinkedList<String>> toVisit = newLinkedList();
        if (rootLevel.startsWith(SYS_PREFIX)) {
            // sys topic don't match "#" and "+"
            toVisit.add(newLinkedList(singleton(rootLevel)));
        } else {
            toVisit.addAll(toFilters(rootLevel));
        }

        String nextFilter = null;
        out:
        while (!toVisit.isEmpty()) {
            LinkedList<String> currentFilterLevels = toVisit.pollFirst();
            switch (currentFilterLevels.getLast()) {
                case MULTI_WILDCARD: {
                    String current = fastJoin(NUL, currentFilterLevels);
                    if (escapedTopicFilter.compareTo(current) < 0) {
                        nextFilter = current;
                        break out;
                    } else {
                        while (!toVisit.isEmpty()) {
                            String next = fastJoin(NUL, toVisit.peekFirst());
                            if (escapedTopicFilter.compareTo(next) <= 0 || escapedTopicFilter.startsWith(next)) {
                                break;
                            } else {
                                toVisit.pollFirst();
                            }
                        }
                    }
                    break;
                }
                case SINGLE_WILDCARD:
                default: {
                    if (currentFilterLevels.size() == topicLevels.size()) {
                        String current = fastJoin(NUL, currentFilterLevels);
                        if (escapedTopicFilter.compareTo(current) < 0) {
                            nextFilter = current;
                            break out;
                        }
                        current = fastJoin(NUL, List.of(current, MULTI_WILDCARD));
                        if (escapedTopicFilter.compareTo(current) < 0) {
                            nextFilter = current;
                            break out;
                        }
                        while (!toVisit.isEmpty()) {
                            String next = fastJoin(NUL, toVisit.peekFirst());
                            if (escapedTopicFilter.compareTo(next) <= 0 || escapedTopicFilter.startsWith(next)) {
                                break;
                            } else {
                                toVisit.pollFirst();
                            }
                        }
                    } else {
                        toFilters(topicLevels.get(currentFilterLevels.size())).descendingIterator()
                            .forEachRemaining(f -> {
                                currentFilterLevels.descendingIterator().forEachRemaining(f::addFirst);
                                toVisit.addFirst(f);
                            });
                    }

                }
            }
        }
        return Optional.ofNullable(nextFilter);
    }

    private static LinkedList<LinkedList<TrieNode>> genFilters(TrieNode node) {
        LinkedList<LinkedList<TrieNode>> filters = newLinkedList();
        if (node.levelName().equals(NUL)
            && node.children().stream().allMatch(child -> child.levelName().startsWith(SYS_PREFIX))) {
            // SYS topics are not matched by # and +
            if (!node.children().isEmpty()) {
                node.children().forEach(c -> filters.add(newLinkedList(singleton(c))));
            }
        } else {
            if (!node.children().isEmpty()) {
                // # and + matches all non SYS topics
                TrieNode singleLevelFilter = merge(node.children().stream()
                    .filter(child -> !node.levelName().equals(NUL) || !child.levelName().startsWith(SYS_PREFIX))
                    .map(child -> child.duplicate(SINGLE_WILDCARD)).collect(Collectors.toList()))
                    .iterator().next();
                merge(Iterables.concat(newArrayList(singleLevelFilter, TrieNode.MULTI), node.children()))
                    .forEach(n -> filters.add(newLinkedList(singleton(n))));
            } else if (!node.levelName().isEmpty()) {
                // # also match parent level if current level name is not empty string
                filters.add(newLinkedList(singleton(TrieNode.MULTI)));
            }
        }
        return filters;
    }

    private static Iterable<TrieNode> merge(Iterable<TrieNode> nodes) {
        class MergedState {
            Iterable<TopicMessage> messages = new LinkedList<>();
            final List<TrieNode> childList = new ArrayList<>();
        }
        TreeMap<String, MergedState> merged = new TreeMap<>();
        for (TrieNode node : nodes) {
            merged.compute(node.levelName(), (k, b) -> {
                if (b == null) {
                    b = new MergedState();
                }
                b.messages = Iterables.concat(b.messages, node.messages());
                Iterable<TrieNode> mergedChildren = merge(Iterables.concat(b.childList, node.children()));
                b.childList.clear();
                b.childList.addAll(Lists.newLinkedList(mergedChildren));
                return b;
            });
        }
        return Iterables.transform(merged.entrySet(),
            entry -> new TrieNode(entry.getKey(), newArrayList(entry.getValue().messages),
                entry.getValue().childList));
    }

    private static LinkedList<LinkedList<String>> toFilters(String topicLevel) {
        LinkedList<LinkedList<String>> filters = newLinkedList();
        if (MULTI_WILDCARD.compareTo(topicLevel) > 0) {
            filters.add(newLinkedList(singleton(topicLevel)));
            filters.add(newLinkedList(singleton(MULTI_WILDCARD)));
            filters.add(newLinkedList(singleton(SINGLE_WILDCARD)));
        } else if (SINGLE_WILDCARD.compareTo(topicLevel) > 0) {
            filters.add(newLinkedList(singleton(MULTI_WILDCARD)));
            filters.add(newLinkedList(singleton(topicLevel)));
            filters.add(newLinkedList(singleton(SINGLE_WILDCARD)));
        } else {
            filters.add(newLinkedList(singleton(MULTI_WILDCARD)));
            filters.add(newLinkedList(singleton(SINGLE_WILDCARD)));
            filters.add(newLinkedList(singleton(topicLevel)));
        }
        return filters;
    }

    public static String escape(String topicFilter) {
        assert !topicFilter.contains(NUL);
        return topicFilter.replace(DELIMITER, NUL);
    }

    public static String unescape(String topicFilter) {
        return topicFilter.replace(NUL, DELIMITER);
    }

    // parse a topic or topic filter string into a list of topic levels
    // eg. "/" -> ["",""], "/a" -> ["",a], "a/" -> [a,""]
    public static List<String> parse(String topic, boolean isEscaped) {
        List<String> topicLevels = new ArrayList<>();
        char splitter = isEscaped ? NUL_CHAR : DELIMITER_CHAR;
        StringBuilder tl = new StringBuilder();
        for (int i = 0; i < topic.length(); i++) {
            if (topic.charAt(i) == splitter) {
                topicLevels.add(tl.toString());
                tl.delete(0, tl.length());
            } else {
                tl.append(topic.charAt(i));
            }
        }
        topicLevels.add(tl.toString());
        return topicLevels;
    }

    public static boolean isWildcardTopicFilter(String topicFilter) {
        return topicFilter.contains(SINGLE_WILDCARD) || topicFilter.contains(MULTI_WILDCARD);
    }

    public static boolean isNormalTopicFilter(String topicFilter) {
        return !isUnorderedShared(topicFilter) && !isOrderedShared(topicFilter);
    }

    public static boolean isUnorderedShared(String topicFilter) {
        return topicFilter.startsWith(UNORDERED_SHARE);
    }

    public static boolean isOrderedShared(String topicFilter) {
        return topicFilter.startsWith(ORDERED_SHARE);
    }

    public static SharedTopicFilter parseSharedTopic(String topicFilter) {
        assert !isNormalTopicFilter(topicFilter);
        String sharePrefix = topicFilter.startsWith(UNORDERED_SHARE) ? UNORDERED_SHARE : ORDERED_SHARE;
        boolean ordered = !topicFilter.startsWith(UNORDERED_SHARE);
        String rest = topicFilter.substring((sharePrefix + DELIMITER_CHAR).length());
        int firstTopicSeparatorIndex = rest.indexOf(DELIMITER_CHAR);
        String shareGroup = rest.substring(0, firstTopicSeparatorIndex);
        return new SharedTopicFilter(topicFilter, ordered, shareGroup,
            rest.substring(firstTopicSeparatorIndex + 1));
    }

    public static class SharedTopicFilter {
        public String originTopicFilter;
        public boolean ordered;
        public String shareGroup;
        public String topicFilter;

        SharedTopicFilter(String originTopicFilter, boolean ordered, String shareName, String filter) {
            this.originTopicFilter = originTopicFilter;
            this.ordered = ordered;
            this.shareGroup = shareName;
            this.topicFilter = filter;
        }
    }

    public static String fastJoin(CharSequence delimiter, Iterable<? extends CharSequence> strings) {
        StringBuilder sb = new StringBuilder();
        Iterator<? extends CharSequence> itr = strings.iterator();
        while (itr.hasNext()) {
            sb.append(itr.next());
            if (itr.hasNext()) {
                sb.append(delimiter);
            }
        }
        return sb.toString();
    }

    public static <T> String fastJoin(CharSequence delimiter, Iterable<T> items,
                                      Function<T, ? extends CharSequence> toCharSequence) {
        StringBuilder sb = new StringBuilder();
        Iterator<T> itr = items.iterator();
        while (itr.hasNext()) {
            sb.append(toCharSequence.apply(itr.next()));
            if (itr.hasNext()) {
                sb.append(delimiter);
            }
        }
        return sb.toString();
    }
}
