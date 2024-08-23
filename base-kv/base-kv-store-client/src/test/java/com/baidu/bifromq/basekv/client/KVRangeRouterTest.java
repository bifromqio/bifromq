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

package com.baidu.bifromq.basekv.client;

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.EMPTY_BOUNDARY;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class KVRangeRouterTest {

    private KVRangeRouter router;

    @BeforeMethod
    public void setUp() {
        router = new KVRangeRouter("testCluster");
    }

    @Test
    public void reset() {

    }

    @Test
    public void emptyRouter() {
        assertFalse(router.findByKey(ByteString.copyFromUtf8("a")).isPresent());
        assertTrue(router.findByBoundary(FULL_BOUNDARY).isEmpty());
    }

    @Test
    public void findByKey() {
        // Prepare test data with full space coverage
        KVRangeDescriptor range1 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(1).build())
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder().setEndKey(ByteString.copyFromUtf8("b")).build()) // (null, "b")
            .build();

        KVRangeDescriptor range2 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(2).build())
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("b"))
                .setEndKey(ByteString.copyFromUtf8("c"))
                .build()) // ["b", "c")
            .build();

        KVRangeDescriptor range3 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(3).build())
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("c"))
                .setEndKey(ByteString.copyFromUtf8("d"))
                .build()) // ["c", "d")
            .build();

        KVRangeDescriptor range4 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(4).build())
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("d"))
                .build()) // ["d", null)
            .build();

        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("store1")
            .addRanges(range1)
            .addRanges(range2)
            .addRanges(range3)
            .addRanges(range4)
            .build();

        assertTrue(router.reset(Set.of(storeDescriptor)));

        // Test find by key within the first range
        Optional<KVRangeSetting> result1 = router.findByKey(ByteString.copyFromUtf8("a"));
        assertTrue(result1.isPresent());
        assertEquals(result1.get().id.getId(), 1L);

        // Test find by key within the second range
        Optional<KVRangeSetting> result2 = router.findByKey(ByteString.copyFromUtf8("b"));
        assertTrue(result2.isPresent());
        assertEquals(result2.get().id.getId(), 2L);

        // Test find by key within the third range
        Optional<KVRangeSetting> result3 = router.findByKey(ByteString.copyFromUtf8("c"));
        assertTrue(result3.isPresent());
        assertEquals(result3.get().id.getId(), 3L);

        // Test find by key within the fourth range
        Optional<KVRangeSetting> result4 = router.findByKey(ByteString.copyFromUtf8("d"));
        assertTrue(result4.isPresent());
        assertEquals(result4.get().id.getId(), 4L);

        // Test find by key not in any range
        Optional<KVRangeSetting> result5 = router.findByKey(ByteString.copyFromUtf8("z"));
        assertTrue(result5.isPresent());
        assertEquals(result5.get().id.getId(), 4L);
    }

    @Test
    public void findByBoundary() {
        // Prepare test data with full space coverage
        KVRangeDescriptor range1 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(1).build())
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder().setEndKey(ByteString.copyFromUtf8("b")).build()) // (null, "b")
            .build();

        KVRangeDescriptor range2 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(2).build())
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("b"))
                .setEndKey(ByteString.copyFromUtf8("c"))
                .build()) // ["b", "c")
            .build();

        KVRangeDescriptor range3 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(3).build())
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("c"))
                .setEndKey(ByteString.copyFromUtf8("d"))
                .build()) // ["c", "d")
            .build();

        KVRangeDescriptor range4 = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setId(4).build())
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFromUtf8("d"))
                .build()) // ["d", null)
            .build();

        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("store1")
            .addRanges(range1)
            .addRanges(range2)
            .addRanges(range3)
            .addRanges(range4)
            .build();

        router.reset(Set.of(storeDescriptor));
        List<KVRangeSetting> result0 = router.findByBoundary(
            Boundary.newBuilder().setEndKey(ByteString.copyFromUtf8("a")).build()); // (null, "a")
        assertEquals(result0.size(), 1);
        assertEquals(result0.get(0).id.getId(), 1L);

        // Test find by exact boundary of the first range
        List<KVRangeSetting> result1 = router.findByBoundary(
            Boundary.newBuilder().setEndKey(ByteString.copyFromUtf8("b")).build()); // (null, "b")
        assertEquals(result1.size(), 1);
        assertEquals(result1.get(0).id.getId(), 1L);

        // Test find by overlapping boundary with the second range
        List<KVRangeSetting> result2 = router.findByBoundary(
            Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("b")).build()); // ["b", null)
        assertEquals(result2.size(), 3); // Covers ranges 2, 3, and 4
        assertEquals(result2.get(0).boundary, range2.getBoundary());
        assertEquals(result2.get(1).boundary, range3.getBoundary());
        assertEquals(result2.get(2).boundary, range4.getBoundary());

        // Test find by a boundary that overlaps multiple ranges
        List<KVRangeSetting> result3 = router.findByBoundary(
            Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("b"))
                .setEndKey(ByteString.copyFromUtf8("d"))
                .build()); // ["b", "d")
        assertEquals(result3.size(), 2); // Covers ranges 2 and 3
        assertEquals(result3.get(0).boundary, range2.getBoundary());
        assertEquals(result3.get(1).boundary, range3.getBoundary());

        List<KVRangeSetting> result4 = router.findByBoundary(
            Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("x"))
                .setEndKey(ByteString.copyFromUtf8("y"))
                .build()); // ["x", "y")
        assertEquals(result4.size(), 1);
        assertEquals(result4.get(0).boundary, range4.getBoundary());

        List<KVRangeSetting> result5 = router.findByBoundary(FULL_BOUNDARY);
        assertEquals(result5.size(), 4);
        assertEquals(result5.get(0).boundary, range1.getBoundary());
        assertEquals(result5.get(1).boundary, range2.getBoundary());
        assertEquals(result5.get(2).boundary, range3.getBoundary());
        assertEquals(result5.get(3).boundary, range4.getBoundary());

        List<KVRangeSetting> result6 = router.findByBoundary(EMPTY_BOUNDARY);
        assertEquals(result6.size(), 1);
        assertEquals(result6.get(0).boundary, range1.getBoundary());
    }
}