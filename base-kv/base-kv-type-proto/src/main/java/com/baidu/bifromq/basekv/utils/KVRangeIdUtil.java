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

package com.baidu.bifromq.basekv.utils;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.basekv.proto.KVRangeId;


public final class KVRangeIdUtil {
    public static KVRangeId generate() {
        long hlc = HLC.INST.get();
        return KVRangeId.newBuilder()
            .setEpoch(hlc)
            .setId(0)
            .build();
    }

    public static KVRangeId next(KVRangeId from) {
        return KVRangeId.newBuilder()
            .setEpoch(from.getEpoch()) // under same epoch
            .setId(HLC.INST.get())
            .build();
    }

    public static String toString(KVRangeId kvRangeId) {
        return Long.toUnsignedString(kvRangeId.getEpoch()) + "_" + Long.toUnsignedString(kvRangeId.getId());
    }

    public static KVRangeId fromString(String id) {
        String[] parts = id.split("_");
        assert parts.length == 2;
        return KVRangeId.newBuilder()
            .setEpoch(Long.parseUnsignedLong(parts[0]))
            .setId(Long.parseUnsignedLong(parts[1]))
            .build();
    }
}
