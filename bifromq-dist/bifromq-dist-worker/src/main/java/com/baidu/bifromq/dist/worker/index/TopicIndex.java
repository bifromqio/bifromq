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

package com.baidu.bifromq.dist.worker.index;

import static com.baidu.bifromq.util.TopicConst.MULTI_WILDCARD;
import static com.baidu.bifromq.util.TopicConst.SINGLE_WILDCARD;
import static com.baidu.bifromq.util.TopicConst.SYS_PREFIX;

import com.baidu.bifromq.dist.util.TopicUtil;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Concurrent Index for searching Topics against TopicFilter.
 */
public final class TopicIndex<V> extends TopicLevelTrie<V> {
    private static final BranchSelector BranchSelector = new BranchSelector() {
        @Override
        public <T> Map<Branch<T>, Action> selectBranch(Map<String, Branch<T>> branches,
                                                       List<String> topicLevels,
                                                       int currentLevel) {
            if (currentLevel < topicLevels.size() - 1) {
                // not last level
                String topicLevelToMatch = topicLevels.get(currentLevel);
                switch (topicLevelToMatch) {
                    case SINGLE_WILDCARD -> {
                        Map<Branch<T>, Action> result = new HashMap<>();
                        for (Map.Entry<String, Branch<T>> entry : branches.entrySet()) {
                            Branch<T> branch = entry.getValue();
                            if (currentLevel == 0 && entry.getKey().startsWith(SYS_PREFIX)) {
                                // + skip SYS topic
                                continue;
                            }
                            result.put(branch, Action.CONTINUE);
                        }
                        return result;
                    }
                    case MULTI_WILDCARD -> {
                        Map<Branch<T>, Action> result = new HashMap<>();
                        for (Map.Entry<String, Branch<T>> entry : branches.entrySet()) {
                            Branch<T> branch = entry.getValue();
                            if (currentLevel == 0 && entry.getKey().startsWith(SYS_PREFIX)) {
                                // # skip SYS topic
                                continue;
                            }
                            result.put(branch, Action.MATCH_AND_CONTINUE);
                        }
                        return result;
                    }
                    default -> {
                        if (branches.containsKey(topicLevelToMatch)) {
                            if (currentLevel + 1 < topicLevels.size()
                                && topicLevels.get(currentLevel + 1).equals(MULTI_WILDCARD)) {
                                // # match parent level
                                return Map.of(branches.get(topicLevelToMatch), Action.MATCH_AND_CONTINUE);
                            }
                            return Map.of(branches.get(topicLevelToMatch), Action.CONTINUE);
                        }
                        return Collections.emptyMap();
                    }
                }
            } else if (currentLevel == topicLevels.size() - 1) {
                // last level
                String topicLevelToMatch = topicLevels.get(currentLevel);
                switch (topicLevelToMatch) {
                    case SINGLE_WILDCARD -> {
                        Map<Branch<T>, Action> result = new HashMap<>();
                        for (Map.Entry<String, Branch<T>> entry : branches.entrySet()) {
                            Branch<T> branch = entry.getValue();
                            if (currentLevel == 0 && entry.getKey().startsWith(SYS_PREFIX)) {
                                // + skip SYS topic
                                continue;
                            }
                            result.put(branch, Action.MATCH_AND_STOP);
                        }
                        return result;
                    }
                    case MULTI_WILDCARD -> {
                        Map<Branch<T>, Action> result = new HashMap<>();
                        for (Map.Entry<String, Branch<T>> entry : branches.entrySet()) {
                            Branch<T> branch = entry.getValue();
                            if (currentLevel == 0 && entry.getKey().startsWith(SYS_PREFIX)) {
                                // # skip SYS topic
                                continue;
                            }
                            result.put(branch, Action.MATCH_AND_CONTINUE);
                        }
                        return result;
                    }
                    default -> {
                        if (branches.containsKey(topicLevelToMatch)) {
                            return Map.of(branches.get(topicLevelToMatch), Action.MATCH_AND_STOP);
                        }
                        return Collections.emptyMap();
                    }
                }
            } else {
                // # matches all descendant levels
                Map<Branch<T>, Action> result = new HashMap<>();
                for (Map.Entry<String, Branch<T>> entry : branches.entrySet()) {
                    Branch<T> branch = entry.getValue();
                    result.put(branch, Action.MATCH_AND_CONTINUE);
                }
                return result;
            }
        }
    };

    public TopicIndex() {
        super(BranchSelector);
    }

    public void add(String topic, V value) {
        add(TopicUtil.parse(topic, false), value);
    }

    public void remove(String topic, V value) {
        remove(TopicUtil.parse(topic, false), value);
    }

    public List<V> match(String topicFilter) {
        return lookup(TopicUtil.parse(topicFilter, false));
    }
}
