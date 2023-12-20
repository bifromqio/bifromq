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

package com.baidu.bifromq.basekv.store;

import com.baidu.bifromq.basecrdt.core.api.CRDTURI;
import com.baidu.bifromq.basecrdt.core.api.CausalCRDTType;
import com.baidu.bifromq.basecrdt.core.api.IORMap;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CRDTUtil {
    public static String storeDescriptorMapCRDTURI(String clusterId) {
        return CRDTURI.toURI(CausalCRDTType.ormap, clusterId + "-store-descriptor-map");
    }

    public static Optional<KVRangeStoreDescriptor> getDescriptorFromCRDT(IORMap descriptorMapCRDT,
                                                                         ByteString key) {
        ArrayList<KVRangeStoreDescriptor> l = Lists.newArrayList(
            Iterators.filter(Iterators.transform(descriptorMapCRDT.getMVReg(key).read(), b -> {
                try {
                    return KVRangeStoreDescriptor.parseFrom(b);
                } catch (InvalidProtocolBufferException e) {
                    log.error("Unable to parse KVRangeStoreDescriptor", e);
                    return null;
                }
            }), Objects::nonNull));
        l.sort((a, b) -> Long.compareUnsigned(b.getHlc(), a.getHlc()));
        return Optional.ofNullable(l.isEmpty() ? null : l.get(0));
    }
}
