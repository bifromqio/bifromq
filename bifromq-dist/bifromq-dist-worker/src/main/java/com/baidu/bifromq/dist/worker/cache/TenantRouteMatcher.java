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

package com.baidu.bifromq.dist.worker.cache;

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.compare;
import static com.baidu.bifromq.dist.entity.EntityUtil.matchRecordPrefixWithEscapedTopicFilter;
import static com.baidu.bifromq.dist.entity.EntityUtil.parseMatchRecord;
import static com.baidu.bifromq.util.TopicConst.NUL;

import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.baidu.bifromq.basekv.store.api.IKVReader;
import com.baidu.bifromq.dist.entity.Matching;
import com.baidu.bifromq.dist.trie.TopicFilterIterator;
import com.baidu.bifromq.dist.trie.TopicTrieNode;
import com.baidu.bifromq.dist.util.TopicUtil;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Timer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

class TenantRouteMatcher implements ITenantRouteMatcher {
    private final String tenantId;
    private final Timer timer;
    private final Supplier<IKVReader> kvReaderSupplier;

    public TenantRouteMatcher(String tenantId, Supplier<IKVReader> kvReaderSupplier, Timer timer) {
        this.tenantId = tenantId;
        this.timer = timer;
        this.kvReaderSupplier = kvReaderSupplier;
    }

    @Override
    public Set<Matching> match(String topic, Boundary matchRecordBoundary) {
        final Timer.Sample sample = Timer.start();
        IKVReader rangeReader = kvReaderSupplier.get();
        rangeReader.refresh();

        TopicTrieNode<String> topicTrie = TopicTrieNode.<String>builder(false)
            .addTopic(TopicUtil.parse(topic, false), topic)
            .build();
        TopicFilterIterator<String> expansionSetItr = new TopicFilterIterator<>(topicTrie);

        Set<Matching> matchedRoutes = new HashSet<>();

        Set<String> matchedTopicFilters = Sets.newHashSet();
        int probe = 0;
        IKVIterator itr = rangeReader.iterator();
        // track seek
        itr.seek(matchRecordBoundary.getStartKey());
        while (itr.isValid() && compare(itr.key(), matchRecordBoundary.getEndKey()) < 0) {
            // track itr.key()
            Matching matching = parseMatchRecord(itr.key(), itr.value());
            // key: topic
            if (!matchedTopicFilters.contains(matching.escapedTopicFilter)) {
                List<String> seekTopicFilter = TopicUtil.parse(matching.escapedTopicFilter, true);
                expansionSetItr.seek(seekTopicFilter);
                if (expansionSetItr.isValid()) {
                    List<String> topicFilterToMatch = expansionSetItr.key();
                    if (topicFilterToMatch.equals(seekTopicFilter)) {
                        matchedTopicFilters.add(matching.escapedTopicFilter);
                        matchedRoutes.add(matching);
                        itr.next();
                        probe = 0;
                    } else {
                        // next() is much cheaper than seek(), we probe following 20 entries
                        if (probe++ < 20) {
                            // probe next
                            itr.next();
                        } else {
                            // seek to match next topic filter
                            ByteString nextMatch = matchRecordPrefixWithEscapedTopicFilter(tenantId,
                                TopicUtil.fastJoin(NUL, topicFilterToMatch));
                            if (ByteString.unsignedLexicographicalComparator().compare(nextMatch, itr.key()) <= 0) {
                                // TODO: THIS IS ONLY FOR BYPASSING THE HANG ISSUE CAUSED BY PREVIOUS BUGGY MATCH_RECORD_KEY ENCODING
                                // AND WILL BE REMOVED IN LATER VERSION
                                itr.next();
                            } else {
                                itr.seek(nextMatch);
                            }
                        }
                    }
                } else {
                    break; // no more topic filter to match, stop here
                }
            } else {
                itr.next();
                matchedRoutes.add(matching);
            }
        }
        sample.stop(timer);
        return matchedRoutes;
    }
}
