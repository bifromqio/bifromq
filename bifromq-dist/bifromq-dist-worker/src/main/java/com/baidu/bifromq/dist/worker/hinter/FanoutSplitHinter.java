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

import static com.baidu.bifromq.dist.worker.schema.KVSchemaUtil.toGroupRouteKey;
import static com.baidu.bifromq.dist.worker.schema.KVSchemaUtil.toNormalRouteKey;
import static com.baidu.bifromq.dist.worker.schema.KVSchemaUtil.toReceiverUrl;
import static com.baidu.bifromq.util.TopicUtil.isNormalTopicFilter;

import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.SplitHint;
import com.baidu.bifromq.basekv.store.api.IKVCloseableReader;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.baidu.bifromq.basekv.store.api.IKVLoadRecord;
import com.baidu.bifromq.basekv.store.api.IKVRangeSplitHinter;
import com.baidu.bifromq.basekv.store.api.IKVReader;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.utils.BoundaryUtil;
import com.baidu.bifromq.dist.rpc.proto.BatchMatchRequest;
import com.baidu.bifromq.dist.rpc.proto.BatchUnmatchRequest;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FanoutSplitHinter implements IKVRangeSplitHinter {
    public static final String TYPE = "fanout_split_hinter";
    public static final String LOAD_TYPE_FANOUT_TOPIC_FILTERS = "fanout_topicfilters";
    public static final String LOAD_TYPE_FANOUT_SCALE = "fanout_scale";
    private final int splitAtScale;
    private final Supplier<IKVCloseableReader> readerSupplier;
    private final Set<IKVCloseableReader> threadReaders = Sets.newConcurrentHashSet();
    private final ThreadLocal<IKVReader> threadLocalKVReader;
    // key: matchRecordKeyPrefix, value: splitKey
    private final Map<ByteString, FanOutSplit> fanoutSplitKeys = new ConcurrentHashMap<>();
    private final Gauge fanOutTopicFiltersGauge;
    private final Gauge fanOutScaleGauge;
    private volatile Boundary boundary;


    public FanoutSplitHinter(Supplier<IKVCloseableReader> readerSupplier, int splitAtScale, String... tags) {
        this.splitAtScale = splitAtScale;
        this.readerSupplier = readerSupplier;
        threadLocalKVReader = ThreadLocal.withInitial(() -> {
            IKVCloseableReader reader = readerSupplier.get();
            threadReaders.add(reader);
            return reader;
        });
        boundary = readerSupplier.get().boundary();
        fanOutTopicFiltersGauge = Gauge.builder("dist.fanout.topicfilters", fanoutSplitKeys::size)
            .tags(tags)
            .register(Metrics.globalRegistry);
        fanOutScaleGauge = Gauge.builder("dist.fanout.scale",
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
                Map<ByteString, RecordEstimation> routeKeyLoads = new HashMap<>();
                request.getRequestsMap().forEach((tenantId, records) ->
                    records.getRouteList().forEach(route -> {
                        String topicFilter = route.getTopicFilter();
                        if (isNormalTopicFilter(topicFilter)) {
                            ByteString routeKey = toNormalRouteKey(tenantId, topicFilter, toReceiverUrl(route));
                            routeKeyLoads.computeIfAbsent(routeKey,
                                k -> new RecordEstimation(false)).addRecordSize(routeKey.size());
                        } else {
                            ByteString routeKey = toGroupRouteKey(tenantId, route.getTopicFilter());
                            routeKeyLoads.computeIfAbsent(
                                    toGroupRouteKey(tenantId, route.getTopicFilter()),
                                    k -> new RecordEstimation(false))
                                .addRecordSize(routeKey.size());
                        }
                    }));
                doEstimate(routeKeyLoads);
            }
            case BATCHUNMATCH -> {
                BatchUnmatchRequest request = input.getDistService().getBatchUnmatch();
                Map<ByteString, RecordEstimation> routeKeyLoads = new HashMap<>();
                request.getRequestsMap().forEach((tenantId, records) ->
                    records.getRouteList().forEach(route -> {
                        String topicFilter = route.getTopicFilter();
                        if (isNormalTopicFilter(topicFilter)) {
                            ByteString routeKey = toNormalRouteKey(tenantId, topicFilter, toReceiverUrl(route));
                            routeKeyLoads.computeIfAbsent(routeKey,
                                k -> new RecordEstimation(true)).addRecordSize(routeKey.size());
                        } else {
                            ByteString routeKey = toGroupRouteKey(tenantId, route.getTopicFilter());
                            routeKeyLoads.computeIfAbsent(routeKey,
                                k -> new RecordEstimation(true)).addRecordSize(routeKey.size());
                        }
                    }));
                doEstimate(routeKeyLoads);
            }
            default -> {
                // ignore
            }
        }
    }

    @Override
    public void reset(Boundary boundary) {
        this.boundary = boundary;
        Map<ByteString, RecordEstimation> finished = new HashMap<>();
        for (Map.Entry<ByteString, FanOutSplit> entry : fanoutSplitKeys.entrySet()) {
            ByteString matchRecordKeyPrefix = entry.getKey();
            FanOutSplit fanoutSplit = entry.getValue();
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
        Optional<Map.Entry<ByteString, FanOutSplit>> firstSplit = fanoutSplitKeys.entrySet().stream().findFirst();
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
        threadReaders.forEach(IKVCloseableReader::close);
        Metrics.globalRegistry.remove(fanOutTopicFiltersGauge);
        Metrics.globalRegistry.remove(fanOutScaleGauge);
    }

    private void doEstimate(Map<ByteString, RecordEstimation> routeKeyLoads) {
        Map<ByteString, RangeEstimation> splitCandidate = new HashMap<>();
        routeKeyLoads.forEach((matchRecordKeyPrefix, recordEst) -> {
            long dataSize = (threadLocalKVReader.get()
                .size(Boundary.newBuilder()
                    .setStartKey(matchRecordKeyPrefix)
                    .setEndKey(BoundaryUtil.upperBound(matchRecordKeyPrefix))
                    .build())) - recordEst.tombstoneSize();
            long fanOutScale = dataSize / recordEst.avgRecordSize();
            if (fanOutScale >= splitAtScale) {
                splitCandidate.put(matchRecordKeyPrefix, new RangeEstimation(dataSize, recordEst.avgRecordSize()));
            } else if (fanoutSplitKeys.containsKey(matchRecordKeyPrefix) && fanOutScale < 0.5 * splitAtScale) {
                fanoutSplitKeys.remove(matchRecordKeyPrefix);
            }
        });
        if (!splitCandidate.isEmpty()) {
            try (IKVCloseableReader reader = readerSupplier.get()) {
                for (ByteString routeKey : splitCandidate.keySet()) {
                    RangeEstimation recEst = splitCandidate.get(routeKey);
                    fanoutSplitKeys.computeIfAbsent(routeKey, k -> {
                        IKVIterator itr = reader.iterator();
                        int i = 0;
                        for (itr.seek(routeKey); itr.isValid(); itr.next()) {
                            if (i++ >= splitAtScale) {
                                return new FanOutSplit(recEst, itr.key());
                            }
                        }
                        return null;
                    });
                }
            }
        }
    }

    private record RangeEstimation(long dataSize, int recordSize) {
    }

    private record FanOutSplit(RangeEstimation estimation, ByteString splitKey) {
    }
}
