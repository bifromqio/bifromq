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

package com.baidu.bifromq.basekv.localengine.memory;

import com.baidu.bifromq.basekv.localengine.IKVSpaceCheckpoint;
import com.google.protobuf.ByteString;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

class InMemKVSpaceCheckpoint extends InMemKVSpaceReader implements IKVSpaceCheckpoint {
    private final String cpId;
    private final Map<ByteString, ByteString> metadataMap;
    private final ConcurrentSkipListMap<ByteString, ByteString> rangeData;

    protected InMemKVSpaceCheckpoint(String id,
                                     String cpId,
                                     Map<ByteString, ByteString> metadataMap,
                                     ConcurrentSkipListMap<ByteString, ByteString> rangeData,
                                     String... tags) {
        super(id, tags);
        this.cpId = cpId;
        this.metadataMap = metadataMap;
        this.rangeData = rangeData;
    }

    @Override
    public String cpId() {
        return cpId;
    }

    @Override
    protected Map<ByteString, ByteString> metadataMap() {
        return metadataMap;
    }

    @Override
    protected ConcurrentSkipListMap<ByteString, ByteString> rangeData() {
        return rangeData;
    }
}
