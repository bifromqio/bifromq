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

import static com.baidu.bifromq.basekv.Constants.FULL_RANGE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.raft.proto.ClusterConfig;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeSyncState;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import org.testng.annotations.Test;

public class KVRangeSettingTest {
    private String clusterId = "test_cluster";
    private String localVoter = "localVoter";
    private String remoteVoter1 = "remoteVoter1";
    private String remoteVoter2 = "remoteVoter2";
    private String remoteLearner1 = "remoteLearner1";
    private String remoteLearner2 = "remoteLearner2";

    @Test
    public void preferInProc() {
        KVRangeSetting.regInProcStore(clusterId, localVoter);
        KVRangeSetting setting = new KVRangeSetting(clusterId, remoteVoter1, KVRangeDescriptor.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setRange(FULL_RANGE)
            .putSyncState(localVoter, RaftNodeSyncState.Replicating)
            .putSyncState(remoteVoter1, RaftNodeSyncState.Replicating)
            .putSyncState(remoteVoter2, RaftNodeSyncState.Replicating)
            .putSyncState(remoteLearner1, RaftNodeSyncState.Replicating)
            .putSyncState(remoteLearner2, RaftNodeSyncState.Replicating)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localVoter)
                .addVoters(remoteVoter1)
                .addVoters(remoteVoter2)
                .addLearners(remoteLearner1)
                .addLearners(remoteLearner2)
                .build())
            .build());

        assertEquals(setting.randomReplica(), localVoter);
        assertEquals(setting.randomVoters(), localVoter);
    }

    @Test
    public void skipNonReplicating() {
        KVRangeSetting setting = new KVRangeSetting(clusterId, localVoter, KVRangeDescriptor.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setRole(RaftNodeStatus.Leader)
            .setVer(1)
            .setRange(FULL_RANGE)
            .putSyncState(localVoter, RaftNodeSyncState.Replicating)
            .putSyncState(remoteVoter1, RaftNodeSyncState.Replicating)
            .putSyncState(remoteVoter2, RaftNodeSyncState.Probing)
            .putSyncState(remoteLearner1, RaftNodeSyncState.Replicating)
            .putSyncState(remoteLearner2, RaftNodeSyncState.Probing)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters(localVoter)
                .addVoters(remoteVoter1)
                .addVoters(remoteVoter2)
                .addLearners(remoteLearner1)
                .addLearners(remoteLearner2)
                .build())
            .build());
        assertFalse(setting.followers.contains(remoteVoter2));
        assertFalse(setting.allReplicas.contains(remoteLearner2));
    }
}
