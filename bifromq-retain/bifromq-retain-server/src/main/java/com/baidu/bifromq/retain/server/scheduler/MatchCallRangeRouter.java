/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.retain.server.scheduler;

import static com.baidu.bifromq.basekv.client.KVRangeRouterUtil.findByBoundary;
import static com.baidu.bifromq.basekv.client.KVRangeRouterUtil.findByKey;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.compare;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.compareEndKeys;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.compareStartKey;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.endKey;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.startKey;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static com.baidu.bifromq.retain.store.schema.KVSchemaUtil.filterPrefix;
import static com.baidu.bifromq.retain.store.schema.KVSchemaUtil.parseLevelHash;
import static com.baidu.bifromq.retain.store.schema.KVSchemaUtil.retainKeyPrefix;
import static com.baidu.bifromq.retain.store.schema.KVSchemaUtil.tenantBeginKey;
import static com.baidu.bifromq.util.TopicUtil.isMultiWildcardTopicFilter;
import static com.baidu.bifromq.util.TopicUtil.isNormalTopicFilter;
import static com.baidu.bifromq.util.TopicUtil.isWildcardTopicFilter;
import static com.baidu.bifromq.util.TopicUtil.parse;

import com.baidu.bifromq.basekv.client.KVRangeSetting;
import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.retain.store.schema.KVSchemaUtil;
import com.baidu.bifromq.retain.store.schema.LevelHash;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;

class MatchCallRangeRouter {
    public static Map<KVRangeSetting, Set<String>> rangeLookup(String tenantId, Set<String> topicFilters,
                                                               NavigableMap<Boundary, KVRangeSetting> effectiveRouter) {
        Map<KVRangeSetting, Set<String>> topicFiltersByRange = new HashMap<>();
        for (String topicFilter : topicFilters) {
            // not shared subscription
            assert isNormalTopicFilter(topicFilter);
            if (isWildcardTopicFilter(topicFilter)) {
                boolean isFixedLevelMatch = !isMultiWildcardTopicFilter(topicFilter);
                List<String> filterLevels = parse(topicFilter, false);
                List<String> filterPrefix = filterPrefix(filterLevels);
                short levels = (short) (isFixedLevelMatch ? filterLevels.size() : filterLevels.size() - 1);
                Collection<KVRangeSetting> rangeSettingList;
                if (isFixedLevelMatch) {
                    ByteString startKey = retainKeyPrefix(tenantId, levels, filterPrefix);
                    Boundary topicBoundary = toBoundary(startKey, upperBound(startKey));
                    rangeSettingList = findByBoundary(topicBoundary, effectiveRouter);
                } else {
                    // topic filter end with "#"
                    if (filterPrefix.isEmpty()) {
                        // topic filters start with wildcard
                        Boundary topicBoundary = Boundary.newBuilder()
                            .setStartKey(retainKeyPrefix(tenantId, levels, filterPrefix))
                            .setEndKey(upperBound(tenantBeginKey(tenantId)))
                            .build();
                        rangeSettingList = findByBoundary(topicBoundary, effectiveRouter);
                    } else {
                        rangeSettingList = findCandidates(tenantId, levels, filterPrefix, effectiveRouter);
                    }
                }
                for (KVRangeSetting rangeSetting : rangeSettingList) {
                    topicFiltersByRange.computeIfAbsent(rangeSetting, k -> new HashSet<>()).add(topicFilter);
                }
            } else {
                ByteString retainKey = KVSchemaUtil.retainMessageKey(tenantId, topicFilter);
                Optional<KVRangeSetting> rangeSetting = findByKey(retainKey, effectiveRouter);
                assert rangeSetting.isPresent();
                topicFiltersByRange.computeIfAbsent(rangeSetting.get(), k -> new HashSet<>()).add(topicFilter);
            }
        }
        return topicFiltersByRange;
    }

    // find the candidates that overlap with each [retainKeyPrefixStart, retainKeyPrefixStop)
    // retainKeyPrefixStart defined retainKeyPrefix with levels vary from filterPrefix.size() to maxLevel(0xFF)
    // retainKeyPrefixStop defined upperBound of retainKeyPrefixStart with levels vary from filterPrefix.size() to maxLevel(0xFF)
    // retainKeyPrefixStart is the retainKeyPrefixStart when levels = filterPrefix.size()
    // retainKeyPrefixEnd is the upper bound of tenant range
    private static List<KVRangeSetting> findCandidates(String tenantId,
                                                       short levels,
                                                       List<String> filterPrefix,
                                                       NavigableMap<Boundary, KVRangeSetting> effectiveRouter) {
        ByteString retainKeyPrefixBegin = retainKeyPrefix(tenantId, levels, filterPrefix);
        ByteString levelHash = LevelHash.hash(filterPrefix);
        ByteString retainKeyPrefixEnd = upperBound(tenantBeginKey(tenantId));
        Boundary topicBoundary = Boundary.newBuilder()
            .setStartKey(retainKeyPrefixBegin)
            .setEndKey(retainKeyPrefixEnd)
            .build();
        Collection<KVRangeSetting> allCandidates = findByBoundary(topicBoundary, effectiveRouter);
        List<KVRangeSetting> candidates = new ArrayList<>(allCandidates.size());
        for (KVRangeSetting rangeSetting : allCandidates) {
            Boundary candidateBoundary = rangeSetting.boundary;
            ByteString candidateStartKey = startKey(candidateBoundary);
            ByteString candidateEndKey = endKey(candidateBoundary);
            if (compareStartKey(candidateStartKey, retainKeyPrefixBegin) > 0
                && compare(upperBound(levelHash), parseLevelHash(candidateStartKey)) <= 0) {
                // skip: retainKeyPrefixBegin < (some)retainKeyPrefixStop <= [candidateStartKey,candidateEndKey)
                continue;
            }
            if (compareEndKeys(candidateEndKey, retainKeyPrefixEnd) <= 0
                && compare(parseLevelHash(candidateEndKey), levelHash) <= 0) {
                // skip: retainKeyPrefixBegin < [candidateStartKey,candidateEndKey) <= (some)retainKeyPrefixStart
                continue;
            }
            candidates.add(rangeSetting);
        }
        return candidates;
    }
}
