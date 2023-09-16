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

package com.baidu.bifromq.basekv;

import static com.baidu.bifromq.basekv.Constants.FULL_RANGE;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.proto.Range;
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
            .setRange(FULL_RANGE)
            .build())
        .build();
    private KVRangeStoreDescriptor bucket__a = KVRangeStoreDescriptor.newBuilder()
        .setId("bucket__a")
        .addRanges(KVRangeDescriptor.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(1)
            .setRole(RaftNodeStatus.Leader)
            .setRange(Range.newBuilder()
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
            .setRange(Range.newBuilder()
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
            .setRange(Range.newBuilder()
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
            .setRange(Range.newBuilder()
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
                .setRange(Range.newBuilder()
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
        assertEquals(router.findByRange(FULL_RANGE).size(), 1);
        assertEquals(router.findByRange(FULL_RANGE).get(0), convert(bucket_full_range));
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

        List<KVRangeSetting> overlapped = router.findByRange(bucket__a.getRanges(0).getRange());
        assertTrue(overlapped.size() == 1 && overlapped.contains(convert(bucket__a)));

        overlapped = router.findByRange(Range.getDefaultInstance()); // default instance is full range
        assertEquals(overlapped.size(), 4);

        overlapped = router.findByRange(FULL_RANGE);
        assertEquals(overlapped.size(), 4);
        assertTrue(overlapped.size() == 4 &&
            overlapped.containsAll(
                Arrays.asList(convert(bucket__a), convert(bucket_a_c), convert(bucket_c_e), convert(bucket_e_))));

        overlapped = router.findByRange(Range.newBuilder()
            .setStartKey(copyFromUtf8("c"))
            .setEndKey(copyFromUtf8("a"))
            .build());
        assertTrue(overlapped.isEmpty());

        overlapped = router.findByRange(Range.newBuilder()
            .setStartKey(copyFromUtf8("a1"))
            .setEndKey(copyFromUtf8("d"))
            .build());
        assertEquals(overlapped.size(), 2);
        assertEquals(new ArrayList<>(overlapped), Arrays.asList(convert(bucket_a_c), convert(bucket_c_e)));

        overlapped = router.findByRange(Range.newBuilder()
            .setStartKey(copyFromUtf8("c"))
            .setEndKey(copyFromUtf8("c"))
            .build());
        assertEquals(overlapped.size(), 0);

        overlapped = router.findByRange(Range.newBuilder()
            .setStartKey(copyFromUtf8("c"))
            .setEndKey(copyFromUtf8("c1"))
            .build());
        assertEquals(overlapped.size(), 1);
        assertEquals(overlapped.get(0), convert(bucket_c_e));


        overlapped = router.findByRange(Range.newBuilder()
            .setEndKey(copyFromUtf8("c"))
            .build());
        assertEquals(overlapped.size(), 2);
        assertEquals(new ArrayList<>(overlapped), Arrays.asList(convert(bucket__a), convert(bucket_a_c)));
    }

    @Test
    public void testFindByStore() {
        KVRangeRouter router = new KVRangeRouter(clusterId);
        router.upsert(bucket__a);
        router.upsert(bucket_a_c);
        router.upsert(bucket_c_e);
        router.upsert(bucket_e_);
        assertEquals(router.findByStore(bucket__a.getId()).size(), 1);
        assertEquals(router.findByStore(bucket_a_c.getId()).size(), 1);
        assertEquals(router.findByStore(bucket_c_e.getId()).size(), 1);
        assertEquals(router.findByStore(bucket_e_.getId()).size(), 1);
        assertTrue(
            router.findByStore(bucket__a.getId()).contains(router.findById(bucket__a.getRanges(0).getId()).get()));
        assertTrue(
            router.findByStore(bucket_a_c.getId()).contains(router.findById(bucket_a_c.getRanges(0).getId()).get()));
        assertTrue(
            router.findByStore(bucket_c_e.getId()).contains(router.findById(bucket_c_e.getRanges(0).getId()).get()));
        assertTrue(
            router.findByStore(bucket_e_.getId()).contains(router.findById(bucket_e_.getRanges(0).getId()).get()));
    }

    @Test
    public void testReset() {
        KVRangeRouter router = new KVRangeRouter(clusterId);
        router.upsert(bucket__a);
        router.upsert(bucket_a_c);
        router.upsert(bucket_c_e);
        router.upsert(bucket_e_);
        assertTrue(router.isFullRangeCovered());
        router.reset(bucket_full_range);
        assertEquals(router.findByKey(copyFromUtf8("a")).get(), convert(bucket_full_range));
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
        router.reset(bucket_full_range);
        assertTrue(router.isFullRangeCovered());
    }

    private KVRangeSetting convert(KVRangeStoreDescriptor storeDescriptor) {
        return new KVRangeSetting(clusterId, storeDescriptor.getId(), storeDescriptor.getRanges(0));
    }
}
