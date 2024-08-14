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

package com.baidu.bifromq.basekv;

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;

public class KVRangeRouterTest {
    private String clusterId = "test_cluster";
    private KVRangeStoreDescriptor bucket_full_range = KVRangeStoreDescriptor.newBuilder()
        .setId("bucket_full_range")
        .addRanges(KVRangeDescriptor.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setBoundary(FULL_BOUNDARY)
            .build())
        .build();
    private KVRangeStoreDescriptor bucket__a = KVRangeStoreDescriptor.newBuilder()
        .setId("bucket__a")
        .addRanges(KVRangeDescriptor.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(1)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder()
                .setEndKey(copyFromUtf8("a"))
                .build())
            .build())
        .build();

    private KVRangeStoreDescriptor bucket_a_c = KVRangeStoreDescriptor.newBuilder()
        .setId("bucket_a_c")
        .addRanges(KVRangeDescriptor.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(1)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(copyFromUtf8("a"))
                .setEndKey(copyFromUtf8("c"))
                .build())
            .build())
        .build();
    private KVRangeStoreDescriptor bucket_c_e = KVRangeStoreDescriptor.newBuilder()
        .setId("bucket_c_e")
        .addRanges(KVRangeDescriptor.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(1)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(copyFromUtf8("c"))
                .setEndKey(copyFromUtf8("e"))
                .build())
            .build())
        .build();
    private KVRangeStoreDescriptor bucket_e_ = KVRangeStoreDescriptor.newBuilder()
        .setId("bucket_e_")
        .addRanges(KVRangeDescriptor.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(1)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(Boundary.newBuilder()
                .setStartKey(copyFromUtf8("e"))
                .build())
            .build())
        .build();

    @Test
    public void testUpsert() {
        KVRangeRouter router = new KVRangeRouter(clusterId);
        router.upsert(bucket__a);
        router.upsert(bucket_a_c);
        router.upsert(bucket_c_e);
        router.upsert(bucket_e_);

        KVRangeStoreDescriptor bucket_a_e_oldVer = KVRangeStoreDescriptor.newBuilder()
            .setId("bucket_a_e_oldVer")
            .addRanges(KVRangeDescriptor.newBuilder()
                .setId(KVRangeIdUtil.generate())
                .setVer(0) // older version
                .setRole(RaftNodeStatus.Leader)
                .setBoundary(Boundary.newBuilder()
                    .setStartKey(copyFromUtf8("a"))
                    .setEndKey(copyFromUtf8("e"))
                    .build())
                .build())
            .build();

        router.upsert(bucket_a_e_oldVer);
        assertEquals(router.findByKey(copyFromUtf8("a")).get(), convert(bucket_a_c));
        assertEquals(router.findByKey(copyFromUtf8("c")).get(), convert(bucket_c_e));
        assertEquals(router.findByKey(copyFromUtf8("e")).get(), convert(bucket_e_));

        KVRangeStoreDescriptor bucket_a_e_newVer = bucket_a_e_oldVer.toBuilder()
            .setRanges(0, bucket_a_e_oldVer.getRanges(0).toBuilder().setVer(1).build())
            .build();
        router.upsert(bucket_a_e_newVer);
        assertEquals(router.findByKey(copyFromUtf8("a")).get(), convert(bucket_a_e_newVer));
        assertEquals(router.findByKey(copyFromUtf8("c")).get(), convert(bucket_a_e_newVer));
        assertEquals(router.findByKey(copyFromUtf8("e")).get(), convert(bucket_e_));
    }

    @Test
    public void testUpsertWithFullRange() {
        KVRangeRouter router = new KVRangeRouter(clusterId);
        router.upsert(bucket_e_);
        router.upsert(bucket_full_range);
        assertEquals(router.findByBoundary(FULL_BOUNDARY).size(), 1);
        assertEquals(router.findByBoundary(FULL_BOUNDARY).get(0), convert(bucket_full_range));
    }

    @Test
    public void testFindByKey() {
        KVRangeRouter router = new KVRangeRouter(clusterId);
        assertFalse(router.findByKey(copyFromUtf8("abc")).isPresent());
        router.upsert(bucket__a);
        router.upsert(bucket_a_c);
        router.upsert(bucket_c_e);
        router.upsert(bucket_e_);
        assertEquals(router.findByKey(ByteString.EMPTY).get(), convert(bucket__a));
        assertEquals(router.findByKey(copyFromUtf8("a")).get(), convert(bucket_a_c));
        assertEquals(router.findByKey(copyFromUtf8("a1")).get(), convert(bucket_a_c));
        assertEquals(router.findByKey(copyFromUtf8("c")).get(), convert(bucket_c_e));
    }

    @Test
    public void testFindByRange() {
        KVRangeRouter router = new KVRangeRouter(clusterId);
        router.upsert(bucket__a);
        router.upsert(bucket_a_c);
        router.upsert(bucket_c_e);
        router.upsert(bucket_e_);

        List<KVRangeSetting> overlapped = router.findByBoundary(bucket__a.getRanges(0).getBoundary());
        assertTrue(overlapped.size() == 1 && overlapped.contains(convert(bucket__a)));

        overlapped = router.findByBoundary(Boundary.getDefaultInstance()); // default instance is full range
        assertEquals(overlapped.size(), 4);

        overlapped = router.findByBoundary(FULL_BOUNDARY);
        assertEquals(overlapped.size(), 4);
        assertTrue(overlapped.size() == 4 &&
            overlapped.containsAll(
                Arrays.asList(convert(bucket__a), convert(bucket_a_c), convert(bucket_c_e), convert(bucket_e_))));

        overlapped = router.findByBoundary(Boundary.newBuilder()
            .setStartKey(copyFromUtf8("c"))
            .setEndKey(copyFromUtf8("a"))
            .build());
        assertTrue(overlapped.isEmpty());

        overlapped = router.findByBoundary(Boundary.newBuilder()
            .setStartKey(copyFromUtf8("a1"))
            .setEndKey(copyFromUtf8("d"))
            .build());
        assertEquals(overlapped.size(), 2);
        assertEquals(new ArrayList<>(overlapped), Arrays.asList(convert(bucket_a_c), convert(bucket_c_e)));

        overlapped = router.findByBoundary(Boundary.newBuilder()
            .setStartKey(copyFromUtf8("c"))
            .setEndKey(copyFromUtf8("c"))
            .build());
        assertEquals(overlapped.size(), 0);

        overlapped = router.findByBoundary(Boundary.newBuilder()
            .setStartKey(copyFromUtf8("c"))
            .setEndKey(copyFromUtf8("c1"))
            .build());
        assertEquals(overlapped.size(), 1);
        assertEquals(overlapped.get(0), convert(bucket_c_e));


        overlapped = router.findByBoundary(Boundary.newBuilder()
            .setEndKey(copyFromUtf8("c"))
            .build());
        assertEquals(overlapped.size(), 2);
        assertEquals(new ArrayList<>(overlapped), Arrays.asList(convert(bucket__a), convert(bucket_a_c)));
    }

    @Test
    public void testFullRangeCoverCheck() {
        KVRangeRouter router = new KVRangeRouter(clusterId);
        assertFalse(router.isFullRangeCovered());
        router.upsert(bucket__a);
        assertFalse(router.isFullRangeCovered());
        router.upsert(bucket_c_e);
        assertFalse(router.isFullRangeCovered());
        router.upsert(bucket_a_c);
        assertFalse(router.isFullRangeCovered());
        router.upsert(bucket_e_);
        assertTrue(router.isFullRangeCovered());
    }

    @Test
    public void testUpsertWithFullOverlap() {
        KVRangeRouter router = new KVRangeRouter(clusterId);

        KVRangeStoreDescriptor bucket_a_c_initial = KVRangeStoreDescriptor.newBuilder()
            .setId("bucket_a_c_initial")
            .addRanges(KVRangeDescriptor.newBuilder()
                .setId(KVRangeId.newBuilder().setEpoch(1).setId(100).build())  // epoch 1, id 100
                .setVer(1)
                .setRole(RaftNodeStatus.Leader)
                .setBoundary(Boundary.newBuilder()
                    .setStartKey(copyFromUtf8("a"))
                    .setEndKey(copyFromUtf8("c"))
                    .build())
                .build())
            .build();

        router.upsert(bucket_a_c_initial);

        KVRangeStoreDescriptor bucket_a_c_largerId = KVRangeStoreDescriptor.newBuilder()
            .setId("bucket_a_c_largerId")
            .addRanges(KVRangeDescriptor.newBuilder()
                .setId(KVRangeId.newBuilder().setEpoch(1).setId(200).build())  // epoch 1, id 200
                .setVer(1)
                .setRole(RaftNodeStatus.Leader)
                .setBoundary(Boundary.newBuilder()
                    .setStartKey(copyFromUtf8("a"))
                    .setEndKey(copyFromUtf8("c"))
                    .build())
                .build())
            .build();

        router.upsert(bucket_a_c_largerId);

        assertEquals(router.findByKey(copyFromUtf8("a")).get(), convert(bucket_a_c_initial));
        assertEquals(router.findByKey(copyFromUtf8("b")).get(), convert(bucket_a_c_initial));

        KVRangeStoreDescriptor bucket_a_c_smallerId = KVRangeStoreDescriptor.newBuilder()
            .setId("bucket_a_c_smallerId")
            .addRanges(KVRangeDescriptor.newBuilder()
                .setId(KVRangeId.newBuilder().setEpoch(1).setId(50).build())  // epoch 1, id 50
                .setVer(1)
                .setRole(RaftNodeStatus.Leader)
                .setBoundary(Boundary.newBuilder()
                    .setStartKey(copyFromUtf8("a"))
                    .setEndKey(copyFromUtf8("c"))
                    .build())
                .build())
            .build();

        router.upsert(bucket_a_c_smallerId);

        assertEquals(router.findByKey(copyFromUtf8("a")).get(), convert(bucket_a_c_smallerId));
        assertEquals(router.findByKey(copyFromUtf8("b")).get(), convert(bucket_a_c_smallerId));
    }

    private KVRangeSetting convert(KVRangeStoreDescriptor storeDescriptor) {
        return new KVRangeSetting(clusterId, storeDescriptor.getId(), storeDescriptor.getRanges(0));
    }
}
