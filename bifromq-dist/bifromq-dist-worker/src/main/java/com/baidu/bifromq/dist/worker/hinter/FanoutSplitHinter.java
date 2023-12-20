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

package com.baidu.bifromq.dist.worker.hinter;

import static com.baidu.bifromq.dist.entity.EntityUtil.matchRecordSize;
import static com.baidu.bifromq.dist.entity.EntityUtil.parseQInboxIdFromScopedTopicFilter;
import static com.baidu.bifromq.dist.entity.EntityUtil.parseTenantIdFromScopedTopicFilter;
import static com.baidu.bifromq.dist.entity.EntityUtil.parseTopicFilterFromScopedTopicFilter;
import static com.baidu.bifromq.dist.entity.EntityUtil.toMatchRecordKeyPrefix;

import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.SplitHint;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.baidu.bifromq.basekv.store.api.IKVLoadRecord;
import com.baidu.bifromq.basekv.store.api.IKVRangeSplitHinter;
import com.baidu.bifromq.basekv.store.api.IKVReader;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.utils.BoundaryUtil;
import com.baidu.bifromq.dist.rpc.proto.BatchMatchRequest;
import com.baidu.bifromq.dist.rpc.proto.BatchUnmatchRequest;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FanoutSplitHinter implements IKVRangeSplitHinter {
    public static final String TYPE = "fanout_split_hinter";
    public static final String LOAD_TYPE_FANOUT_TOPIC_FILTERS = "fanout_topicfilters";
    public static final String LOAD_TYPE_FANOUT_SCALE = "fanout_scale";
    private final int splitAtScale;
    private final Supplier<IKVReader> readerSupplier;
    private final ThreadLocal<IKVReader> threadLocalKVReader;
    // key: matchRecordKeyPrefix, value: splitKey
    private final Map<ByteString, FanoutSplit> fanoutSplitKeys = new ConcurrentHashMap<>();
    private final Gauge fanoutTopicFiltersGuage;
    private final Gauge fanoutScaleGuage;
    private volatile Boundary boundary;


    public FanoutSplitHinter(Supplier<IKVReader> readerSupplier, int splitAtScale, String... tags) {
        this.splitAtScale = splitAtScale;
        this.readerSupplier = readerSupplier;
        threadLocalKVReader = ThreadLocal.withInitial(readerSupplier);
        boundary = readerSupplier.get().boundary();
        fanoutTopicFiltersGuage = Gauge.builder("dist.fanout.topicfilters", fanoutSplitKeys::size)
            .tags(tags)
            .register(Metrics.globalRegistry);
        fanoutScaleGuage = Gauge.builder("dist.fanout.scale",
                () -> fanoutSplitKeys.values()
                    .stream()
                    .map(f -> f.estimation.dataSize / f.estimation.recordSize)
                    .reduce(Long::sum).orElse(0L))
            .tags(tags)
            .register(Metrics.globalRegistry);
    }

    @Override
    public void recordQuery(ROCoProcInput input, IKVLoadRecord ioRecord) {
    }

    @Override
    public void recordMutate(RWCoProcInput input, IKVLoadRecord ioRecord) {
        assert input.hasDistService();
        switch (input.getDistService().getTypeCase()) {
            case BATCHMATCH -> {
                BatchMatchRequest request = input.getDistService().getBatchMatch();
                Map<ByteString, RecordEstimation> matchRecordKeyPrefixMap = new HashMap<>();
                request.getScopedTopicFilterMap().forEach((stf_str, subQoS) -> {
                    String tenantId = parseTenantIdFromScopedTopicFilter(stf_str);
                    String topicFilter = parseTopicFilterFromScopedTopicFilter(stf_str);
                    String qInboxId = parseQInboxIdFromScopedTopicFilter(stf_str);
                    matchRecordKeyPrefixMap.computeIfAbsent(toMatchRecordKeyPrefix(tenantId, topicFilter),
                            k -> new RecordEstimation(false))
                        .addRecordSize(matchRecordSize(tenantId, topicFilter, qInboxId));
                });
                doEstimate(matchRecordKeyPrefixMap);
            }
            case BATCHUNMATCH -> {
                BatchUnmatchRequest request = input.getDistService().getBatchUnmatch();
                Map<ByteString, RecordEstimation> matchRecordKeyPrefixMap = new HashMap<>();
                request.getScopedTopicFilterList().forEach((stf_str) -> {
                    String tenantId = parseTenantIdFromScopedTopicFilter(stf_str);
                    String topicFilter = parseTopicFilterFromScopedTopicFilter(stf_str);
                    String qInboxId = parseQInboxIdFromScopedTopicFilter(stf_str);
                    matchRecordKeyPrefixMap.computeIfAbsent(toMatchRecordKeyPrefix(tenantId, topicFilter),
                            k -> new RecordEstimation(true))
                        .addRecordSize(matchRecordSize(tenantId, topicFilter, qInboxId));
                });
                doEstimate(matchRecordKeyPrefixMap);
            }
        }
    }

    @Override
    public void reset(Boundary boundary) {
        this.boundary = boundary;
        Map<ByteString, RecordEstimation> finished = new HashMap<>();
        for (Map.Entry<ByteString, FanoutSplit> entry : fanoutSplitKeys.entrySet()) {
            ByteString matchRecordKeyPrefix = entry.getKey();
            FanoutSplit fanoutSplit = entry.getValue();
            if (!BoundaryUtil.inRange(fanoutSplit.splitKey, boundary)) {
                fanoutSplitKeys.remove(matchRecordKeyPrefix);
                RecordEstimation recordEst = new RecordEstimation(false);
                recordEst.addRecordSize(fanoutSplit.estimation.recordSize);
                finished.put(matchRecordKeyPrefix, recordEst);
            }
        }
        // check if the finished still needs more split
        doEstimate(finished);
    }

    @Override
    public SplitHint estimate() {
        Optional<Map.Entry<ByteString, FanoutSplit>> firstSplit = fanoutSplitKeys.entrySet().stream().findFirst();
        SplitHint.Builder hintBuilder = SplitHint.newBuilder().setType(TYPE)
            .putLoad(LOAD_TYPE_FANOUT_TOPIC_FILTERS, fanoutSplitKeys.size())
            .putLoad(LOAD_TYPE_FANOUT_SCALE, 0);
        firstSplit.ifPresent(s -> {
            if (!BoundaryUtil.isSplittable(boundary, s.getValue().splitKey)) {
                fanoutSplitKeys.remove(s.getKey());
            } else {
                hintBuilder.setSplitKey(s.getValue().splitKey);
                hintBuilder.putLoad(LOAD_TYPE_FANOUT_SCALE,
                    s.getValue().estimation.dataSize / (double) s.getValue().estimation.recordSize);
            }
        });
        return hintBuilder.build();
    }

    @Override
    public void close() {
        Metrics.globalRegistry.remove(fanoutTopicFiltersGuage);
        Metrics.globalRegistry.remove(fanoutScaleGuage);
    }

    private void doEstimate(Map<ByteString, RecordEstimation> matchRecordKeyPrefixMap) {
        Map<ByteString, RangeEstimation> splitCandidate = new HashMap<>();
        matchRecordKeyPrefixMap.forEach((matchRecordKeyPrefix, recordEst) -> {
            long dataSize = (threadLocalKVReader.get()
                .size(Boundary.newBuilder()
                    .setStartKey(matchRecordKeyPrefix)
                    .setEndKey(BoundaryUtil.upperBound(matchRecordKeyPrefix))
                    .build())) - recordEst.tombstoneSize();
            long fanoutScale = dataSize / recordEst.avgRecordSize();
            if (fanoutScale >= splitAtScale) {
                splitCandidate.put(matchRecordKeyPrefix, new RangeEstimation(dataSize, recordEst.avgRecordSize()));
            } else if (fanoutSplitKeys.containsKey(matchRecordKeyPrefix) && fanoutScale < 0.5 * splitAtScale) {
                fanoutSplitKeys.remove(matchRecordKeyPrefix);
            }
        });
        if (!splitCandidate.isEmpty()) {
            IKVReader reader = readerSupplier.get();
            for (ByteString matchKeyPrefix : splitCandidate.keySet()) {
                RangeEstimation recEst = splitCandidate.get(matchKeyPrefix);
                fanoutSplitKeys.computeIfAbsent(matchKeyPrefix, k -> {
                    IKVIterator itr = reader.iterator();
                    int i = 0;
                    for (itr.seek(matchKeyPrefix); itr.isValid(); itr.next()) {
                        if (i++ >= splitAtScale) {
                            return new FanoutSplit(recEst, itr.key());
                        }
                    }
                    return null;
                });
            }
        }
    }

    private record RangeEstimation(long dataSize, int recordSize) {
    }

    private record FanoutSplit(RangeEstimation estimation, ByteString splitKey) {
    }
}
