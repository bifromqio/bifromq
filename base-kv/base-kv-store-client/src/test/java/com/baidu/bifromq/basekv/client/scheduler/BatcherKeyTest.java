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

package com.baidu.bifromq.basekv.client.scheduler;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import org.testng.annotations.Test;

public class BatcherKeyTest {
    @Test
    public void mutationCallBatcherKeyEquality() {
        KVRangeId id1 = KVRangeIdUtil.generate();
        KVRangeId id2 = KVRangeIdUtil.generate();
        MutationCallBatcherKey id1_key1 = new MutationCallBatcherKey(id1, "V1", 0);
        MutationCallBatcherKey id1_key2 = new MutationCallBatcherKey(id1, "V1", 1);
        MutationCallBatcherKey id1_key3 = new MutationCallBatcherKey(id1, "V2", 0);
        MutationCallBatcherKey id2_key = new MutationCallBatcherKey(id2, "V2", 0);
        assertEquals(id1_key1, id1_key2);
        assertEquals(id1_key2, id1_key3);
        assertNotEquals(id1_key1, id2_key);
    }

    @Test
    public void queryCallBatcherKeyEquality() {
        KVRangeId id1 = KVRangeIdUtil.generate();
        KVRangeId id2 = KVRangeIdUtil.generate();
        QueryCallBatcherKey id1_key1 = new QueryCallBatcherKey(id1, "V1", 0, 0);
        QueryCallBatcherKey id1_key2 = new QueryCallBatcherKey(id1, "V1", 1, 0);
        QueryCallBatcherKey id1_key3 = new QueryCallBatcherKey(id1, "V2", 0, 0);
        QueryCallBatcherKey id1_key4 = new QueryCallBatcherKey(id1, "V2", 0, 1);
        QueryCallBatcherKey id2_key = new QueryCallBatcherKey(id2, "V2", 0, 0);
        assertNotEquals(id1_key1, id1_key2);
        assertNotEquals(id1_key2, id1_key3);
        assertNotEquals(id1_key1, id1_key3);

        assertEquals(id1_key3, id1_key4);

        assertNotEquals(id1_key1, id2_key);
    }
}
