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

package com.baidu.bifromq.basekv.store.api;

import com.google.protobuf.ByteString;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public interface IKVRangeCoProc {
    /**
     * Execute a query co-proc
     *
     * @param input
     * @return
     */
    CompletableFuture<ByteString> query(ByteString input, IKVReader client);

    /**
     * Execute a mutation co-proc, returns a supplier of mutation output. The supplier will be called after mutation is
     * persisted successfully.
     *
     * @param input
     * @param reader
     * @param writer
     * @return
     */
    Supplier<ByteString> mutate(ByteString input, IKVReader reader, IKVWriter writer);

    /**
     * Close the coproc instance, and release all related resources
     */
    void close();
}
