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

package com.baidu.bifromq.basekv.raft;


import com.baidu.bifromq.basekv.raft.proto.RaftNodeSyncState;

interface IPeerLogReplicator {

    /**
     * Current matching index
     *
     * @return
     */
    long matchIndex();

    /**
     * Next index to send
     *
     * @return
     */
    long nextIndex();

    /**
     * Current status
     *
     * @return
     */
    RaftNodeSyncState status();

    /**
     * an external clock signal to drive the state machine forward in case no other stimuli available
     *
     * @return true if the replicator has changed its state after tick
     */
    boolean tick();

    /**
     * the amount of matchIndex advanced per tick always non-negative
     *
     * @return
     */
    long catchupRate();

    /**
     * a flag indicating whether the append entries for given peer should be paused
     *
     * @return
     */
    boolean pauseReplicating();

    /**
     * a flag indicating whether the given peer need a heartbeat due to heartbeatTimeoutTick exceed
     *
     * @return true if peer need a heartbeat
     */
    boolean needHeartbeat();

    /**
     * backoff the next index when peer follower rejected the append entries request
     *
     * @param peerRejectedIndex the index of mismatched log which is literally the prevLogIndex in appendEntries rpc
     * @param peerLastIndex     the index of last log entry in peer's raft log
     * @return true if the replicator has changed its state after calling this method
     */
    boolean backoff(long peerRejectedIndex, long peerLastIndex);

    /**
     * update the match index when peer follower accepted the append entries request
     *
     * @param peerLastIndex the index of last log entry in peer's raft log
     * @return true if the replicator has changed its state after calling this method
     */
    boolean confirmMatch(long peerLastIndex);

    /**
     * advance the next index after sending log entries up to endIndex(inclusively) to follower
     *
     * @param endIndex
     * @return true if the replicator has changed its state after calling this method
     */
    boolean replicateBy(long endIndex);
}
