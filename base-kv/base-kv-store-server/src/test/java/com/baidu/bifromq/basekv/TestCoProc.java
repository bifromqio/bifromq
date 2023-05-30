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

import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.api.IKVRangeCoProc;
import com.baidu.bifromq.basekv.store.api.IKVRangeReader;
import com.baidu.bifromq.basekv.store.api.IKVReader;
import com.baidu.bifromq.basekv.store.api.IKVWriter;
import com.google.protobuf.ByteString;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class TestCoProc implements IKVRangeCoProc {
    private final Supplier<IKVRangeReader> rangeReaderProvider;

    public TestCoProc(KVRangeId id, Supplier<IKVRangeReader> rangeReaderProvider) {
        this.rangeReaderProvider = rangeReaderProvider;
    }

    @Override
    public CompletableFuture<ByteString> query(ByteString input, IKVReader reader) {
        // get
        return CompletableFuture.completedFuture(reader.get(input).orElse(ByteString.EMPTY));
    }

    @Override
    public Supplier<ByteString> mutate(ByteString input, IKVReader reader, IKVWriter client) {
        String[] str = input.toStringUtf8().split("_");
        ByteString key = ByteString.copyFromUtf8(str[0]);
        ByteString value = ByteString.copyFromUtf8(str[1]);
        // update
        Optional<ByteString> existing = reader.get(key);
        client.put(key, value);
        return () -> existing.orElse(ByteString.EMPTY);
    }

    @Override
    public void close() {

    }
}
