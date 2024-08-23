/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.basekv.balance.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basekv.balance.command.ChangeConfigCommand;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.raft.proto.ClusterConfig;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import org.testng.annotations.Test;

public class CommandUtilTest {
    @Test
    public void quitWithMultipleVoters() {
        String localStoreId = "localStore";
        KVRangeId kvRangeId = KVRangeId.newBuilder().setId(1).setEpoch(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setVer(1)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localStoreId)
                .addVoters("otherStore")
                .build())
            .build();

        Optional<ChangeConfigCommand> commandOptional = CommandUtil.quit(localStoreId, kvRangeDescriptor);

        assertTrue(commandOptional.isPresent());
        ChangeConfigCommand command = commandOptional.get();

        assertEquals(command.getToStore(), localStoreId);
        assertEquals(command.getKvRangeId(), kvRangeId);
        assertEquals(command.getVoters(), Set.of(localStoreId));
        assertEquals(command.getLearners(), Collections.emptySet());
    }

    @Test
    public void quitWithLearners() {
        String localStoreId = "localStore";
        KVRangeId kvRangeId = KVRangeId.newBuilder().setId(1).setEpoch(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setVer(1)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localStoreId)
                .addLearners("learnerStore")
                .build())
            .build();

        Optional<ChangeConfigCommand> commandOptional = CommandUtil.quit(localStoreId, kvRangeDescriptor);

        assertTrue(commandOptional.isPresent());
        ChangeConfigCommand command = commandOptional.get();

        assertEquals(command.getToStore(), localStoreId);
        assertEquals(command.getKvRangeId(), kvRangeId);
        assertEquals(command.getVoters(), Set.of(localStoreId));
        assertEquals(command.getLearners(), Collections.emptySet());
    }

    @Test
    public void quitWithSingleVoterNoLearners() {
        String localStoreId = "localStore";
        KVRangeId kvRangeId = KVRangeId.newBuilder().setId(1).setEpoch(1).build();
        KVRangeDescriptor kvRangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(kvRangeId)
            .setVer(1)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localStoreId)
                .build())
            .build();

        Optional<ChangeConfigCommand> commandOptional = CommandUtil.quit(localStoreId, kvRangeDescriptor);

        assertTrue(commandOptional.isPresent());
        ChangeConfigCommand command = commandOptional.get();

        assertEquals(command.getToStore(), localStoreId);
        assertEquals(command.getKvRangeId(), kvRangeId);
        assertEquals(command.getVoters(), Collections.emptySet());
        assertEquals(command.getLearners(), Collections.emptySet());
    }
}
