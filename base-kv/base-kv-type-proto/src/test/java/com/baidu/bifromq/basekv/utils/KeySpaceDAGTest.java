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

package com.baidu.bifromq.basekv.utils;

import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.google.protobuf.ByteString;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.MIN_KEY;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.NULL_BOUNDARY;
import static org.testng.Assert.*;

public class KeySpaceDAGTest {
    @Test
    public void getEffectiveFullCoveredRoute() {
        Map<String, Map<KVRangeId, KVRangeDescriptor>> rangeDescriptorsByStoreId;
        rangeDescriptorsByStoreId = new HashMap<>();

        // Create KVRangeDescriptors that cover the entire key space
        KVRangeDescriptor range1 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(1).setEpoch(1).build())
            .setBoundary(Boundary.newBuilder()
                .setEndKey(ByteString.copyFromUtf8("a")) // (null, "a")
                .build())
            .build();

        KVRangeDescriptor range2 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(2).setEpoch(1).build())
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("a")) // ["a", "b")
                .setEndKey(ByteString.copyFromUtf8("b"))
                .build())
            .build();

        KVRangeDescriptor range3 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(3).setEpoch(1).build())
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("b")) // ["b", "c")
                .setEndKey(ByteString.copyFromUtf8("c"))
                .build())
            .build();

        KVRangeDescriptor range4 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(4).setEpoch(1).build())
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("c")) // ["c", "d")
                .setEndKey(ByteString.copyFromUtf8("d"))
                .build())
            .build();

        KVRangeDescriptor range5 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(5).setEpoch(1).build())
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("d")) // ["d", null)
                .build())
            .build();

        Map<KVRangeId, KVRangeDescriptor> store1 = new HashMap<>() {{
            put(range1.getId(), range1);
            put(range2.getId(), range2);
            put(range3.getId(), range3);
            put(range4.getId(), range4);
            put(range5.getId(), range5);
        }};

        rangeDescriptorsByStoreId.put("store1", store1);

        KeySpaceDAG dag = new KeySpaceDAG(rangeDescriptorsByStoreId);

        NavigableMap<Boundary, KeySpaceDAG.LeaderRange> result = dag.getEffectiveFullCoveredRoute();

        // Verify the route contains the expected KVRangeDescriptors with the correct boundaries
        assertEquals(result.size(), 5);

        Boundary boundary1 = Boundary.newBuilder()
            .setEndKey(ByteString.copyFromUtf8("a")) // (null, "a")
            .build();
        Boundary boundary2 = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("a"))
            .setEndKey(ByteString.copyFromUtf8("b")) // ["a", "b")
            .build();
        Boundary boundary3 = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("b"))
            .setEndKey(ByteString.copyFromUtf8("c")) // ["b", "c")
            .build();
        Boundary boundary4 = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("c"))
            .setEndKey(ByteString.copyFromUtf8("d")) // ["c", "d")
            .build();
        Boundary boundary5 = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("d")) // ["d", null)
            .build();

        assertEquals(result.get(boundary1).descriptor().getId().getId(), 1L);
        assertEquals(result.get(boundary2).descriptor().getId().getId(), 2L);
        assertEquals(result.get(boundary3).descriptor().getId().getId(), 3L);
        assertEquals(result.get(boundary4).descriptor().getId().getId(), 4L);
        assertEquals(result.get(boundary5).descriptor().getId().getId(), 5L);
    }

    @Test
    public void filterOutEmptyRangeFromEffectiveRoute() {
        Map<String, Map<KVRangeId, KVRangeDescriptor>> rangeDescriptorsByStoreId;
        rangeDescriptorsByStoreId = new HashMap<>();

        // Create KVRangeDescriptors that cover the entire key space
        KVRangeDescriptor range1 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(1).setEpoch(1).build())
            .setBoundary(Boundary.newBuilder()
                .setEndKey(ByteString.copyFromUtf8("a")) // (null, "a")
                .build())
            .build();

        KVRangeDescriptor range2 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(2).setEpoch(1).build())
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("a")) // ["a", "b")
                .setEndKey(ByteString.copyFromUtf8("b"))
                .build())
            .build();

        KVRangeDescriptor range3 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(3).setEpoch(1).build())
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("b")) // ["b", "c")
                .setEndKey(ByteString.copyFromUtf8("c"))
                .build())
            .build();

        KVRangeDescriptor range4 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(4).setEpoch(1).build())
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("c")) // ["c", "d")
                .setEndKey(ByteString.copyFromUtf8("d"))
                .build())
            .build();

        KVRangeDescriptor range5 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(5).setEpoch(1).build())
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("d")) // ["d", null)
                .build())
            .build();

        KVRangeDescriptor range00 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(-1).setEpoch(1).build())
            .setBoundary(NULL_BOUNDARY)
            .build();
        KVRangeDescriptor range01 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(-2).setEpoch(1).build())
            .setBoundary(Boundary.newBuilder()
                .setStartKey(MIN_KEY)
                .build())
            .build();


        Map<KVRangeId, KVRangeDescriptor> store1 = new HashMap<>() {{
            put(range00.getId(), range00);
            put(range01.getId(), range01);
            put(range1.getId(), range1);
            put(range2.getId(), range2);
            put(range3.getId(), range3);
            put(range4.getId(), range4);
            put(range5.getId(), range5);
        }};

        rangeDescriptorsByStoreId.put("store1", store1);

        KeySpaceDAG dag = new KeySpaceDAG(rangeDescriptorsByStoreId);

        NavigableMap<Boundary, KeySpaceDAG.LeaderRange> result = dag.getEffectiveFullCoveredRoute();

        // Verify the route contains the expected KVRangeDescriptors with the correct boundaries
        assertEquals(result.size(), 5);

        Boundary boundary1 = Boundary.newBuilder()
            .setEndKey(ByteString.copyFromUtf8("a")) // (null, "a")
            .build();
        Boundary boundary2 = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("a"))
            .setEndKey(ByteString.copyFromUtf8("b")) // ["a", "b")
            .build();
        Boundary boundary3 = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("b"))
            .setEndKey(ByteString.copyFromUtf8("c")) // ["b", "c")
            .build();
        Boundary boundary4 = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("c"))
            .setEndKey(ByteString.copyFromUtf8("d")) // ["c", "d")
            .build();
        Boundary boundary5 = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("d")) // ["d", null)
            .build();

        assertEquals(result.get(boundary1).descriptor().getId().getId(), 1L);
        assertEquals(result.get(boundary2).descriptor().getId().getId(), 2L);
        assertEquals(result.get(boundary3).descriptor().getId().getId(), 3L);
        assertEquals(result.get(boundary4).descriptor().getId().getId(), 4L);
        assertEquals(result.get(boundary5).descriptor().getId().getId(), 5L);

    }

    @Test
    public void overlappingKeySpace() {
        Map<String, Map<KVRangeId, KVRangeDescriptor>> rangeDescriptorsByStoreId = new HashMap<>();

        KVRangeDescriptor range1 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(1).setEpoch(1).build())
            .setBoundary(Boundary.newBuilder()
                .setEndKey(ByteString.copyFromUtf8("a")) // (null, "a")
                .build())
            .build();

        KVRangeDescriptor range2 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(2).setEpoch(1).build())
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("a")) // ["a", "b")
                .setEndKey(ByteString.copyFromUtf8("b"))
                .build())
            .build();

        // Overlapping range with higher id
        KVRangeDescriptor range3 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(3).setEpoch(1).build())
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("a")) // ["a", "b") overlapping
                .setEndKey(ByteString.copyFromUtf8("b"))
                .build())
            .build();

        KVRangeDescriptor range4 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(4).setEpoch(1).build())
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("b")) // ["b", "c")
                .setEndKey(ByteString.copyFromUtf8("c"))
                .build())
            .build();

        KVRangeDescriptor range5 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(5).setEpoch(1).build())
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("c")) // ["c", null)
                .build())
            .build();

        Map<KVRangeId, KVRangeDescriptor> store1 = new HashMap<>() {{
            put(range1.getId(), range1);
            put(range2.getId(), range2);
            put(range3.getId(), range3);
            put(range4.getId(), range4);
            put(range5.getId(), range5);
        }};
        rangeDescriptorsByStoreId.put("store1", store1);

        KeySpaceDAG dag = new KeySpaceDAG(rangeDescriptorsByStoreId);

        NavigableMap<Boundary, KeySpaceDAG.LeaderRange> result = dag.getEffectiveFullCoveredRoute();

        // Verify the route contains the expected KVRangeDescriptors with the correct boundaries
        assertEquals(result.size(), 4);

        Boundary boundary1 = Boundary.newBuilder()
            .setEndKey(ByteString.copyFromUtf8("a")) // (null, "a")
            .build();
        Boundary boundary2 = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("a"))
            .setEndKey(ByteString.copyFromUtf8("b")) // ["a", "b")
            .build();
        Boundary boundary3 = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("b"))
            .setEndKey(ByteString.copyFromUtf8("c")) // ["b", "c")
            .build();
        Boundary boundary4 = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("c")) // ["c", null)
            .build();

        assertEquals(result.get(boundary1).descriptor().getId().getId(), 1L);
        assertEquals(result.get(boundary2).descriptor().getId().getId(),
            2L); // Should choose id 2 over id 3 due to lower id
        assertEquals(result.get(boundary3).descriptor().getId().getId(), 4L);
        assertEquals(result.get(boundary4).descriptor().getId().getId(), 5L);
    }

    @Test
    public void differentStoreId() {
        Map<String, Map<KVRangeId, KVRangeDescriptor>> rangeDescriptorsByStoreId = new HashMap<>();

        KVRangeDescriptor range1 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(1).setEpoch(1).build())
            .setBoundary(Boundary.newBuilder()
                .setEndKey(ByteString.copyFromUtf8("a")) // (null, "a")
                .build())
            .build();

        KVRangeDescriptor range2 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(2).setEpoch(1).build())
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("a")) // ["a", "b")
                .setEndKey(ByteString.copyFromUtf8("b"))
                .build())
            .build();

        KVRangeDescriptor range3 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(3).setEpoch(1).build())
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("b")) // ["b", "c")
                .setEndKey(ByteString.copyFromUtf8("c"))
                .build())
            .build();

        KVRangeDescriptor range4Store2 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(4).setEpoch(1).build())
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("c")) // ["c", null)
                .build())
            .build();

        KVRangeDescriptor range5Store1 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(5).setEpoch(1).build())
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("c")) // ["c", null)
                .build())
            .build();

        Map<KVRangeId, KVRangeDescriptor> store1 = new HashMap<>() {{
            put(range1.getId(), range1);
            put(range2.getId(), range2);
            put(range3.getId(), range3);
            put(range5Store1.getId(), range5Store1);
        }};
        Map<KVRangeId, KVRangeDescriptor> store2 = new HashMap<>() {{
            put(range4Store2.getId(), range4Store2);
        }};

        rangeDescriptorsByStoreId.put("store1", store1);
        rangeDescriptorsByStoreId.put("store2", store2);

        KeySpaceDAG dag = new KeySpaceDAG(rangeDescriptorsByStoreId);

        NavigableMap<Boundary, KeySpaceDAG.LeaderRange> result = dag.getEffectiveFullCoveredRoute();

        // Verify the route contains the expected KVRangeDescriptors with the correct boundaries
        assertEquals(result.size(), 4);

        Boundary boundary1 = Boundary.newBuilder()
            .setEndKey(ByteString.copyFromUtf8("a")) // (null, "a")
            .build();
        Boundary boundary2 = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("a"))
            .setEndKey(ByteString.copyFromUtf8("b")) // ["a", "b")
            .build();
        Boundary boundary3 = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("b"))
            .setEndKey(ByteString.copyFromUtf8("c")) // ["b", "c")
            .build();
        Boundary boundary4 = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("c")) // ["c", null)
            .build();

        assertEquals(result.get(boundary1).descriptor().getId().getId(), 1L);
        assertEquals(result.get(boundary2).descriptor().getId().getId(), 2L);
        assertEquals(result.get(boundary3).descriptor().getId().getId(), 3L);
        assertEquals(result.get(boundary4).descriptor().getId().getId(),
            4L); // Should choose from store2 due to storeId sorting
    }

    @Test
    public void keySpaceWithGaps() {
        Map<String, Map<KVRangeId, KVRangeDescriptor>> rangeDescriptorsByStoreId = new HashMap<>();

        KVRangeDescriptor range1 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(1).setEpoch(1).build())
            .setBoundary(Boundary.newBuilder()
                .setEndKey(ByteString.copyFromUtf8("a")) // (null, "a")
                .build())
            .build();

        KVRangeDescriptor range2 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(2).setEpoch(1).build())
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("b")) // ["b", "c")
                .setEndKey(ByteString.copyFromUtf8("c"))
                .build())
            .build();

        KVRangeDescriptor range3 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(3).setEpoch(1).build())
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("c")) // [“c”, null)
                .build())
            .build();
        Map<KVRangeId, KVRangeDescriptor> ranges = new HashMap<>() {{
            put(range1.getId(), range1);
            put(range2.getId(), range2);
            put(range3.getId(), range3);
        }};

        rangeDescriptorsByStoreId.put("store1", ranges);

        KeySpaceDAG dag = new KeySpaceDAG(rangeDescriptorsByStoreId);

        NavigableMap<Boundary, KeySpaceDAG.LeaderRange> result = dag.getEffectiveFullCoveredRoute();

        assertTrue(result.isEmpty());
    }

    @Test
    public void noGapsFullCoverage() {
        // Create KVRangeDescriptors that cover the entire key space without gaps
        KVRangeDescriptor range1 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(1).build())
            .setBoundary(Boundary.newBuilder()
                .setEndKey(ByteString.copyFromUtf8("a")) // (null, "a")
                .build())
            .build();

        KVRangeDescriptor range2 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(2).build())
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("a")) // ["a", "b")
                .setEndKey(ByteString.copyFromUtf8("b"))
                .build())
            .build();

        KVRangeDescriptor range3 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(3).build())
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("b")) // ["b", null)
                .build())
            .build();

        Map<KVRangeId, KVRangeDescriptor> ranges = new HashMap<>() {{
            put(range1.getId(), range1);
            put(range2.getId(), range2);
            put(range3.getId(), range3);
        }};

        Map<String, Map<KVRangeId, KVRangeDescriptor>> storeMap = new HashMap<>();
        storeMap.put("store1", ranges);

        KeySpaceDAG dag = new KeySpaceDAG(storeMap);

        // Since the ranges cover the full key space, there should be no gaps
        List<Boundary> gaps = dag.findGaps();
        assertEquals(gaps.size(), 0);
    }

    @Test
    public void gapsInKeySpace() {
        // Create KVRangeDescriptors with gaps in the key space
        KVRangeDescriptor range1 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(1).build())
            .setBoundary(Boundary.newBuilder()
                .setEndKey(ByteString.copyFromUtf8("a")) // (null, "a")
                .build())
            .build();

        KVRangeDescriptor range2 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(2).build())
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("c")) // ["c", "d")
                .setEndKey(ByteString.copyFromUtf8("d"))
                .build())
            .build();

        KVRangeDescriptor range3 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(3).build())
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("e")) // ["e", null)
                .build())
            .build();

        Map<KVRangeId, KVRangeDescriptor> ranges = new HashMap<>() {{
            put(range1.getId(), range1);
            put(range2.getId(), range2);
            put(range3.getId(), range3);
        }};
        Map<String, Map<KVRangeId, KVRangeDescriptor>> storeMap = new HashMap<>();
        storeMap.put("store1", ranges);

        KeySpaceDAG dag = new KeySpaceDAG(storeMap);

        // Gaps should be identified between "a" and "c", and between "d" and "e"
        List<Boundary> gaps = dag.findGaps();
        assertEquals(gaps.size(), 2);

        // Expected gaps: ["a", "c"), ["d", "e")
        Boundary gap1 = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("a"))
            .setEndKey(ByteString.copyFromUtf8("c"))
            .build();

        Boundary gap2 = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("d"))
            .setEndKey(ByteString.copyFromUtf8("e"))
            .build();

        assertEquals(gaps.get(0), gap1);
        assertEquals(gaps.get(1), gap2);
    }

    @Test
    public void gapsWithOverlappingRanges() {
        // Create KVRangeDescriptors with overlapping ranges
        KVRangeDescriptor range1 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(1).build())
            .setBoundary(Boundary.newBuilder()
                .setEndKey(ByteString.copyFromUtf8("b")) // (null, "b")
                .build())
            .build();

        KVRangeDescriptor range2 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(2).build())
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("a")) // ["a", "c")
                .setEndKey(ByteString.copyFromUtf8("c"))
                .build())
            .build();

        KVRangeDescriptor range3 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(3).build())
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("c")) // ["c", null)
                .build())
            .build();

        Map<KVRangeId, KVRangeDescriptor> ranges = new HashMap<>() {{
            put(range1.getId(), range1);
            put(range2.getId(), range2);
            put(range3.getId(), range3);
        }};
        Map<String, Map<KVRangeId, KVRangeDescriptor>> storeMap = new HashMap<>();
        storeMap.put("store1", ranges);

        KeySpaceDAG dag = new KeySpaceDAG(storeMap);

        // Gaps should be identified correctly considering overlapping ranges
        List<Boundary> gaps = dag.findGaps();
        assertEquals(gaps.size(), 1);

        // Expected gaps: ["b", "c")
        Boundary gap1 = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("b"))
            .setEndKey(ByteString.copyFromUtf8("c"))
            .build();
        assertEquals(gaps.get(0), gap1);
    }

    @Test
    public void leftOpenEndGap() {
        Map<String, Map<KVRangeId, KVRangeDescriptor>> rangeDescriptorsByStoreId = new HashMap<>();

        KVRangeDescriptor range1 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(1).setEpoch(1).build())
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("a")) // (null, "a")
                .build())
            .build();

        Map<KVRangeId, KVRangeDescriptor> ranges = new HashMap<>() {{
            put(range1.getId(), range1);
        }};

        rangeDescriptorsByStoreId.put("store1", ranges);

        KeySpaceDAG dag = new KeySpaceDAG(rangeDescriptorsByStoreId);

        List<Boundary> result = dag.findGaps();

        assertEquals(result.size(), 1);
        assertEquals(result.get(0).getEndKey(), ByteString.copyFromUtf8("a"));
        assertFalse(result.get(0).hasStartKey());
    }

    @Test
    public void rightOpenEndGap() {
        Map<String, Map<KVRangeId, KVRangeDescriptor>> rangeDescriptorsByStoreId = new HashMap<>();

        KVRangeDescriptor range1 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(1).setEpoch(1).build())
            .setBoundary(Boundary.newBuilder()
                .setEndKey(ByteString.copyFromUtf8("a")) // (null, "a")
                .build())
            .build();

        Map<KVRangeId, KVRangeDescriptor> ranges = new HashMap<>() {{
            put(range1.getId(), range1);
        }};

        rangeDescriptorsByStoreId.put("store1", ranges);

        KeySpaceDAG dag = new KeySpaceDAG(rangeDescriptorsByStoreId);

        List<Boundary> result = dag.findGaps();

        assertEquals(result.size(), 1);
        assertEquals(result.get(0).getStartKey(), ByteString.copyFromUtf8("a"));
        assertFalse(result.get(0).hasEndKey());
    }
}