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

package com.baidu.bifromq.basekv.raft.exception;

public class ClusterConfigChangeException extends RuntimeException {
    public static final ClusterConfigChangeException CONCURRENT_CHANGE =
        new ClusterConfigChangeException("Only one on-going change is allowed");
    public static final ClusterConfigChangeException EMPTY_VOTERS =
        new ClusterConfigChangeException("Voters can not be empty");
    public static final ClusterConfigChangeException LEARNERS_OVERLAP =
        new ClusterConfigChangeException("Learners must not overlap voters");
    public static final ClusterConfigChangeException SLOW_LEARNER =
        new ClusterConfigChangeException("Some new added servers are too slow to catch up leader's progress");
    public static final ClusterConfigChangeException LEADER_STEP_DOWN =
        new ClusterConfigChangeException("Leader has stepped down");
    public static final ClusterConfigChangeException NOT_LEADER =
        new ClusterConfigChangeException("Cluster change can only do via leader");
    public static final ClusterConfigChangeException NO_LEADER =
        new ClusterConfigChangeException("No leader elected");

    private ClusterConfigChangeException(String message) {
        super(message);
    }
}
