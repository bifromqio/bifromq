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

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

import com.baidu.bifromq.basekv.localengine.IKVSpace;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeSnapshot;
import com.baidu.bifromq.basekv.proto.State;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import io.reactivex.rxjava3.core.Maybe;
import org.testng.annotations.Test;

public class KVRangeMetadataTest extends AbstractKVRangeTest {
    @Test
    public void initWithNoData() {
        KVRangeId id = KVRangeIdUtil.generate();
        IKVSpace keyRange = kvEngine.createIfMissing(KVRangeIdUtil.toString(id));
        IKVRange accessor = new KVRange(keyRange);
        assertEquals(accessor.id(), id);
        assertEquals(accessor.version(), -1);
        assertEquals(accessor.lastAppliedIndex(), -1);
        assertEquals(accessor.state().getType(), State.StateType.NoUse);
    }

    @Test
    public void initExistingRange() {
        KVRangeSnapshot snapshot = KVRangeSnapshot.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(0)
            .setLastAppliedIndex(0)
            .setState(State.newBuilder().setType(State.StateType.Normal).build())
            .setBoundary(FULL_BOUNDARY)
            .build();
        IKVSpace keyRange = kvEngine.createIfMissing(KVRangeIdUtil.toString(snapshot.getId()));
        IKVRange accessor = new KVRange(keyRange).toReseter(snapshot).done();

        assertEquals(accessor.version(), snapshot.getVer());
        assertEquals(accessor.boundary(), snapshot.getBoundary());
        assertEquals(accessor.lastAppliedIndex(), snapshot.getLastAppliedIndex());
        assertEquals(accessor.state(), snapshot.getState());
    }

    @Test
    public void initWithNoDataAndDestroy() {
        try {
            KVRangeId rangeId = KVRangeIdUtil.generate();
            IKVSpace keyRange = kvEngine.createIfMissing(KVRangeIdUtil.toString(rangeId));
            IKVRange kvRange = new KVRange(keyRange);
            Maybe<IKVRange.KVRangeMeta> metaMayBe = kvRange.metadata().firstElement();
            kvRange = null;
            keyRange.destroy();
            assertNull(metaMayBe.blockingGet());
        } catch (Throwable e) {
            fail();
        }
    }

    @Test
    public void lastAppliedIndex() {
        KVRangeId id = KVRangeIdUtil.generate();
        long ver = 10;
        long lastAppliedIndex = 10;
        State state = State.newBuilder().setType(State.StateType.Normal).build();
        KVRangeSnapshot snapshot = KVRangeSnapshot.newBuilder()
            .setId(id)
            .setVer(ver)
            .setLastAppliedIndex(lastAppliedIndex)
            .setState(state)
            .setBoundary(FULL_BOUNDARY)
            .build();
        IKVSpace keyRange = kvEngine.createIfMissing(KVRangeIdUtil.toString(snapshot.getId()));
        IKVRange accessor = new KVRange(keyRange).toReseter(snapshot).done();

        lastAppliedIndex = 11;
        accessor.toWriter().lastAppliedIndex(lastAppliedIndex).done();
        assertEquals(accessor.lastAppliedIndex(), lastAppliedIndex);
    }
}

