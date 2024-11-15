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

package com.baidu.bifromq.basecluster.memberlist.agent;

import static org.mockito.Mockito.when;

import com.baidu.bifromq.basecluster.agent.proto.AgentEndpoint;
import com.baidu.bifromq.basecluster.agent.proto.AgentMemberAddr;
import com.baidu.bifromq.basecluster.agent.proto.AgentMemberMetadata;
import com.baidu.bifromq.basecluster.membership.proto.HostEndpoint;
import com.baidu.bifromq.basecrdt.core.api.CausalCRDTType;
import com.baidu.bifromq.basecrdt.core.api.IMVReg;
import com.baidu.bifromq.basecrdt.core.api.IORMap;
import com.baidu.bifromq.basecrdt.core.api.MVRegOperation;
import com.baidu.bifromq.basecrdt.proto.Replica;
import com.baidu.bifromq.basehlc.HLC;
import com.google.common.collect.Iterators;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class MockUtil {
    public static AgentMemberMetadata toAgentMemberMetadata(ByteString value) {
        return AgentMemberMetadata.newBuilder().setValue(value).setHlc(HLC.INST.get()).build();
    }

    public static AgentMemberAddr toAgentMemberAddr(String name, AgentEndpoint endpoint) {
        return AgentMemberAddr.newBuilder()
            .setName(name)
            .setEndpoint(endpoint.getEndpoint())
            .setIncarnation(endpoint.getIncarnation())
            .build();
    }

    public static void mockAgentMemberCRDT(IORMap orMap, Map<AgentMemberAddr, AgentMemberMetadata> members) {
        IORMap.ORMapKey[] keys =
            members.keySet().stream().map(memberAddr -> mvRegKey(memberAddr.toByteString()))
                .toArray(IORMap.ORMapKey[]::new);
        Map<ByteString, AgentMemberMetadata> memberMap =
            members.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().toByteString(), e -> e.getValue()));
        when(orMap.keys()).thenReturn(Iterators.forArray(keys));
        for (IORMap.ORMapKey key : keys) {
            when(orMap.getMVReg(key.key())).thenReturn(mvRegValue(memberMap.get(key.key()).toByteString()));
        }
    }

    private static IORMap.ORMapKey mvRegKey(ByteString key) {
        return new IORMap.ORMapKey() {
            @Override
            public ByteString key() {
                return key;
            }

            @Override
            public CausalCRDTType valueType() {
                return CausalCRDTType.mvreg;
            }
        };
    }

    private static IMVReg mvRegValue(ByteString value) {
        return new IMVReg() {
            @Override
            public Iterator<ByteString> read() {
                return Iterators.forArray(value);
            }

            @Override
            public Replica id() {
                return null;
            }

            @Override
            public CompletableFuture<Void> execute(MVRegOperation op) {
                return null;
            }

            @Override
            public Observable<Long> inflation() {
                return null;
            }
        };
    }
}
