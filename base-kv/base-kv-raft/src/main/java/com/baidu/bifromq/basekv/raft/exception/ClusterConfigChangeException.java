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

package com.baidu.bifromq.basekv.raft.exception;

public class ClusterConfigChangeException extends RuntimeException {
    public static ConcurrentChangeException concurrentChange() {
        return new ConcurrentChangeException();
    }

    public static EmptyVotersException emptyVoters() {
        return new EmptyVotersException();
    }

    public static LearnersOverlapException learnersOverlap() {
        return new LearnersOverlapException();
    }

    public static SlowLearnerException slowLearner() {
        return new SlowLearnerException();
    }

    public static LeaderStepDownException leaderStepDown() {
        return new LeaderStepDownException();
    }

    public static NotLeaderException notLeader() {
        return new NotLeaderException();
    }

    public static NoLeaderException noLeader() {
        return new NoLeaderException();
    }

    protected ClusterConfigChangeException(String message) {
        super(message);
    }

    public static class ConcurrentChangeException extends ClusterConfigChangeException {
        private ConcurrentChangeException() {
            super("Only one on-going change is allowed");
        }
    }

    public static class LeaderStepDownException extends ClusterConfigChangeException {
        private LeaderStepDownException() {
            super("Leader has stepped down");
        }
    }

    public static class EmptyVotersException extends ClusterConfigChangeException {
        private EmptyVotersException() {
            super("Voters can not be empty");
        }
    }

    public static class LearnersOverlapException extends ClusterConfigChangeException {
        private LearnersOverlapException() {
            super("Learners must not overlap voters");
        }
    }

    public static class SlowLearnerException extends ClusterConfigChangeException {
        private SlowLearnerException() {
            super("Some new added servers are too slow to catch up leader's progress");
        }
    }

    public static class NotLeaderException extends ClusterConfigChangeException {

        private NotLeaderException() {
            super("Cluster change can only do via leader");
        }
    }

    public static class NoLeaderException extends ClusterConfigChangeException {
        private NoLeaderException() {
            super("No leader elected");
        }
    }
}
