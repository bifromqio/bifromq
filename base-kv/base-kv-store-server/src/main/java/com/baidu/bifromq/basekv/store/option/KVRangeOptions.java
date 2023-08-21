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

package com.baidu.bifromq.basekv.store.option;

import com.baidu.bifromq.basekv.raft.RaftConfig;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(chain = true)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder(toBuilder = true)
public class KVRangeOptions {
    private int maxRangeLoad = 300_000;
    private double splitKeyThreshold = 0.6;
    private int loadTrackingWindowSec = 5;
    private int snapshotSyncBytesPerSec = 1024 * 1024;
    private int compactWALThresholdBytes = 256 * 1024 * 1024; // 256MB
    private int compactLingerTimeSec = 5;
    private long tickUnitInMS = 100;
    private int maxWALFatchBatchSize = 64 * 1024; // 64KB
    private int snapshotSyncIdleTimeoutSec = 30;
    private int statsCollectIntervalSec = 5;
    private RaftConfig walRaftConfig = new RaftConfig()
        .setPreVote(true)
        .setInstallSnapshotTimeoutTick(300)
        .setMaxSizePerAppend(2 * 1024 * 1024); // 2MB;
}
