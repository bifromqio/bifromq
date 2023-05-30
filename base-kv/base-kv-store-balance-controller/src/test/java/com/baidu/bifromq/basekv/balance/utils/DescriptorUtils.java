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

package com.baidu.bifromq.basekv.balance.utils;

import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.LoadHint;
import com.baidu.bifromq.basekv.proto.State.StateType;
import com.baidu.bifromq.basekv.raft.proto.ClusterConfig;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class DescriptorUtils {

    public static List<KVRangeDescriptor> generateRangeDesc(KVRangeId id, Set<String> voters, Set<String> learner) {
        List<KVRangeDescriptor> descriptors = new ArrayList<>();
        for (int i = 0; i < voters.size() + learner.size(); i++) {
            RaftNodeStatus raftNodeStatus = i == 0 ? RaftNodeStatus.Leader : RaftNodeStatus.Follower;
            KVRangeDescriptor rangeDescriptor = KVRangeDescriptor.newBuilder()
                .setId(id)
                .setState(StateType.Normal)
                .setRole(raftNodeStatus)
                .setLoadHint(LoadHint.newBuilder()
                    .setLoad(100 - i)
                    .build()
                )
                .setVer(1)
                .setConfig(ClusterConfig.newBuilder()
                    .addAllVoters(voters)
                    .addAllLearners(learner)
                    .build()
                ).build();
            descriptors.add(rangeDescriptor);
        }
        return descriptors;
    }
}
