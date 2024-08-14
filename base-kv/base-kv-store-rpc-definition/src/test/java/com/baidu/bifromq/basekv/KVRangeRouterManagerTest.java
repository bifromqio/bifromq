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

package com.baidu.bifromq.basekv;

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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class KVRangeRouterManagerTest {

    private KVRangeRouterManager manager;

    @BeforeMethod
    public void setUp() {
        manager = new KVRangeRouterManager("testCluster");
    }

    @Test
    public void testUpsertAndFindById() {
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        Boundary boundary = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("a"))
            .setEndKey(ByteString.copyFromUtf8("z"))
            .build();
        KVRangeDescriptor descriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(boundary)
            .build();
        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("store1")
            .addRanges(descriptor)
            .build();

        boolean changed = manager.upsert(storeDescriptor);
        assertTrue(changed);

        Optional<KVRangeSetting> result = manager.findById(kvRangeId);
        assertTrue(result.isPresent());

        KVRangeSetting setting = result.get();
        assertEquals(setting.boundary, boundary);
    }

    @Test
    public void testFindByKey() {
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        Boundary boundary = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("a"))
            .setEndKey(ByteString.copyFromUtf8("z"))
            .build();
        KVRangeDescriptor descriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(boundary)
            .build();
        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("store1")
            .addRanges(descriptor)
            .build();

        manager.upsert(storeDescriptor);

        Optional<KVRangeSetting> result = manager.findByKey(ByteString.copyFromUtf8("b"));
        assertTrue(result.isPresent());
    }

    @Test
    public void testFindByBoundary() {
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        Boundary boundary = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("a"))
            .setEndKey(ByteString.copyFromUtf8("z"))
            .build();
        KVRangeDescriptor descriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(boundary)
            .build();
        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("store1")
            .addRanges(descriptor)
            .build();

        manager.upsert(storeDescriptor);

        List<KVRangeSetting> result = manager.findByBoundary(boundary);
        assertEquals(result.size(), 1);
        assertEquals(result.get(0).boundary, boundary);
    }

    @Test
    public void testIsFullRangeCovered() {
        KVRangeId kvRangeId = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        Boundary boundary = Boundary.newBuilder().build();
        KVRangeDescriptor descriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(boundary)
            .build();
        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("store1")
            .addRanges(descriptor)
            .build();

        manager.upsert(storeDescriptor);

        boolean fullRangeCovered = manager.isFullRangeCovered();
        assertTrue(fullRangeCovered);
    }

    @Test
    public void testEmptyManager() {
        Optional<KVRangeSetting> resultById = manager.findById(KVRangeId.newBuilder().setEpoch(1).setId(1).build());
        assertFalse(resultById.isPresent());

        Optional<KVRangeSetting> resultByKey = manager.findByKey(ByteString.copyFromUtf8("a"));
        assertFalse(resultByKey.isPresent());

        List<KVRangeSetting> resultByBoundary = manager.findByBoundary(Boundary.newBuilder().build());
        assertTrue(resultByBoundary.isEmpty());

        boolean fullRangeCovered = manager.isFullRangeCovered();
        assertFalse(fullRangeCovered);
    }

    @Test
    public void testUpsertWithMultipleEpochs() {
        KVRangeId kvRangeId1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
        Boundary boundary1 = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("a"))
            .setEndKey(ByteString.copyFromUtf8("m"))
            .build();
        KVRangeDescriptor descriptor1 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId1)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(boundary1)
            .build();
        KVRangeStoreDescriptor storeDescriptor1 = KVRangeStoreDescriptor.newBuilder()
            .setId("store1")
            .addRanges(descriptor1)
            .build();

        KVRangeId kvRangeId2 = KVRangeId.newBuilder().setEpoch(2).setId(1).build();
        Boundary boundary2 = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("n"))
            .setEndKey(ByteString.copyFromUtf8("z"))
            .build();
        KVRangeDescriptor descriptor2 = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId2)
            .setRole(RaftNodeStatus.Leader)
            .setBoundary(boundary2)
            .build();
        KVRangeStoreDescriptor storeDescriptor2 = KVRangeStoreDescriptor.newBuilder()
            .setId("store2")
            .addRanges(descriptor2)
            .build();

        boolean changed1 = manager.upsert(storeDescriptor1);
        boolean changed2 = manager.upsert(storeDescriptor2);

        assertTrue(changed1);
        assertTrue(changed2);

        Optional<KVRangeSetting> result = manager.findByKey(ByteString.copyFromUtf8("a"));
        assertTrue(result.isPresent());
        assertEquals(result.get().boundary, boundary1);

        result = manager.findByKey(ByteString.copyFromUtf8("n"));
        assertFalse(result.isPresent());
    }
}