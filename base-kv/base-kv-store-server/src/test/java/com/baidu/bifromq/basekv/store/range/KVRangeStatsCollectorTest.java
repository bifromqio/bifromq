/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
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

package com.baidu.bifromq.basekv.store.range;

import static com.baidu.bifromq.basekv.Constants.FULL_RANGE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.basekv.store.api.IKVRangeReader;
import com.baidu.bifromq.basekv.store.api.IKVReader;
import com.baidu.bifromq.basekv.store.wal.IKVRangeWAL;
import com.google.common.util.concurrent.MoreExecutors;
import io.reactivex.rxjava3.observers.TestObserver;
import java.time.Duration;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KVRangeStatsCollectorTest {
    @Mock
    private IKVRangeWAL rangeWAL;
    @Mock
    private IKVRangeState accessor;
    @Mock
    private IKVRangeReader rangeReader;
    @Mock
    private IKVReader kvReader;

    @Test
    public void testScrap() {
        when(accessor.getReader()).thenReturn(rangeReader);
        when(rangeReader.kvReader()).thenReturn(kvReader);
        when(kvReader.range()).thenReturn(FULL_RANGE);
        doNothing().when(rangeReader).refresh();
        when(kvReader.size(FULL_RANGE)).thenReturn(0L);
        when(rangeWAL.logDataSize()).thenReturn(0L);
        KVRangeStatsCollector statsCollector = new KVRangeStatsCollector(accessor, rangeWAL,
            Duration.ofSeconds(1), MoreExecutors.directExecutor());
        TestObserver<Map<String, Double>> statsObserver = TestObserver.create();
        statsCollector.collect().subscribe(statsObserver);
        statsObserver.awaitCount(1);
        Map<String, Double> stats = statsObserver.values().get(0);
        assertEquals(0.0, stats.get("dataSize"), 0.0);
        assertEquals(0.0, stats.get("walSize"), 0.0);
    }
}
