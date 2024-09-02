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
import static java.util.Collections.singleton;

import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.baidu.bifromq.basekv.store.api.IKVReader;
import com.baidu.bifromq.dist.entity.Matching;
import com.baidu.bifromq.dist.util.TopicFilterMatcher;
import com.baidu.bifromq.dist.util.TopicTrie;
import com.baidu.bifromq.type.TopicMessage;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Timer;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
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
        Set<Matching> routes = new HashSet<>();
        IKVReader rangeReader = kvReaderSupplier.get();
        rangeReader.refresh();

        TopicTrie topicTrie = new TopicTrie();
        topicTrie.add(topic, singleton(TopicMessage.getDefaultInstance()));
        // key: topicFilter, value: set of matched topics
        Map<String, Set<String>> matched = Maps.newHashMap();
        TopicFilterMatcher matcher = new TopicFilterMatcher(topicTrie);
        int probe = 0;
        IKVIterator itr = rangeReader.iterator();
        // track seek
        itr.seek(matchRecordBoundary.getStartKey());
        while (itr.isValid() && compare(itr.key(), matchRecordBoundary.getEndKey()) < 0) {
            // track itr.key()
            Matching matching = parseMatchRecord(itr.key(), itr.value());
            // key: topic
            Set<String> matchedTopics = matched.get(matching.escapedTopicFilter);
            if (matchedTopics == null) {
                Optional<Map<String, Iterable<TopicMessage>>> clientMsgs =
                    matcher.match(matching.escapedTopicFilter);
                if (clientMsgs.isPresent()) {
                    matchedTopics = clientMsgs.get().keySet();
                    matched.put(matching.escapedTopicFilter, matchedTopics);
                    itr.next();
                    probe = 0;
                } else {
                    Optional<String> higherFilter = matcher.higher(matching.escapedTopicFilter);
                    if (higherFilter.isPresent()) {
                        // next() is much cheaper than seek(), we probe following 20 entries
                        if (probe++ < 20) {
                            // probe next
                            itr.next();
                        } else {
                            // seek to match next topic filter
                            ByteString nextMatch =
                                matchRecordPrefixWithEscapedTopicFilter(tenantId, higherFilter.get());
                            if (ByteString.unsignedLexicographicalComparator().compare(nextMatch, itr.key()) <= 0) {
                                // TODO: THIS IS ONLY FOR BYPASSING THE HANG ISSUE CAUSED BY PREVIOUS BUGGY MATCH_RECORD_KEY ENCODING
                                // AND WILL BE REMOVED IN LATER VERSION
                                itr.next();
                            } else {
                                itr.seek(nextMatch);
                            }
                        }
                        continue;
                    } else {
                        break; // no more topic filter to match, stop here
                    }
                }
            } else {
                itr.next();
            }
            matchedTopics.forEach(t -> routes.add(matching));
        }
        sample.stop(timer);
        return routes;
    }
}
