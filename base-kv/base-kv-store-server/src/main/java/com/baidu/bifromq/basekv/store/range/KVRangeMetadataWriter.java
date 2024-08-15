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

package com.baidu.bifromq.basekv.store.range;

import com.baidu.bifromq.basekv.localengine.IKVSpaceMetadataUpdatable;
import com.baidu.bifromq.basekv.localengine.IKVSpaceMetadataWriter;

public class KVRangeMetadataWriter extends AbstractKVRangeMetadataUpdatable<KVRangeMetadataWriter>
    implements IKVRangeMetadataWriter<KVRangeMetadataWriter> {
    private final IKVSpaceMetadataWriter keyRangeMetadataWriter;

    KVRangeMetadataWriter(IKVSpaceMetadataWriter keyRangeMetadataWriter) {
        super(keyRangeMetadataWriter);
        this.keyRangeMetadataWriter = keyRangeMetadataWriter;
    }

    @Override
    protected IKVSpaceMetadataUpdatable<?> keyRangeWriter() {
        return keyRangeMetadataWriter;
    }

    @Override
    public void done() {
        keyRangeMetadataWriter.done();
    }

    @Override
    public void abort() {
        keyRangeMetadataWriter.abort();
    }

    @Override
    public int count() {
        return keyRangeMetadataWriter.count();
    }
}
