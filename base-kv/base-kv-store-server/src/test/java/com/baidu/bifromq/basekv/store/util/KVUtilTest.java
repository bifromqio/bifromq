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

package com.baidu.bifromq.basekv.store.util;

import static com.baidu.bifromq.basekv.store.util.KVUtil.cap;
import static com.baidu.bifromq.basekv.store.util.KVUtil.toByteString;
import static com.baidu.bifromq.basekv.store.util.KVUtil.toInt;
import static com.baidu.bifromq.basekv.store.util.KVUtil.toKVRangeId;
import static com.baidu.bifromq.basekv.store.util.KVUtil.toLong;
import static com.baidu.bifromq.basekv.store.util.KVUtil.toLongNativeOrder;
import static org.testng.AssertJUnit.assertEquals;

import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class KVUtilTest {
    @Test
    public void testToBytes() {
        assertEquals(100L, toLong(KVUtil.toByteString(100L)));
        assertEquals(100, toInt(KVUtil.toByteString(100)));
        assertEquals(100L, toLongNativeOrder(KVUtil.toByteStringNativeOrder(100L)));
    }

    @Test
    public void testConcat() {
        assertEquals(ByteString.copyFrom(new byte[] {1, 2, 3, 4, 5, 6, 7}),
            KVUtil.concat(ByteString.copyFrom(new byte[] {1, 2}),
                ByteString.copyFrom(new byte[] {3, 4, 5}),
                ByteString.copyFrom(new byte[0]),
                ByteString.copyFrom(new byte[] {6, 7})));
        assertEquals(ByteString.EMPTY, KVUtil.concat(ByteString.EMPTY, ByteString.EMPTY));
        assertEquals(ByteString.copyFrom(new byte[] {1, 2, 3}),
            KVUtil.concat(ByteString.copyFrom(new byte[] {1, 2, 3})));
    }

    @Test
    public void testUpperBound() {
        KVRangeId bucketId = cap(KVRangeId.newBuilder().build());
        assertEquals(bucketId.getEpoch(), 0L);
        assertEquals(bucketId.getId(), 1L);
    }

    @Test
    public void testKVRangeIdCodec() {
        KVRangeId id = KVRangeIdUtil.generate();
        assertEquals(id, toKVRangeId(toByteString(id)));
    }
}
