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

public class DropProposalException extends RuntimeException {
    public static TransferringLeaderException transferringLeader() {
        return new TransferringLeaderException();
    }

    public static ThrottleByThresholdException throttledByThreshold() {
        return new ThrottleByThresholdException();
    }

    public static NoLeaderException NoLeader() {
        return new NoLeaderException();
    }

    public static LeaderForwardDisabledException leaderForwardDisabled() {
        return new LeaderForwardDisabledException();
    }

    public static ForwardTimeoutException forwardTimeout() {
        return new ForwardTimeoutException();
    }

    public static OverriddenException overridden() {
        return new OverriddenException();
    }

    public static SupersededBySnapshotException superseded() {
        return new SupersededBySnapshotException();
    }

    protected DropProposalException(String message) {
        super(message);
    }

    public static class TransferringLeaderException extends DropProposalException {
        private TransferringLeaderException() {
            super("Proposal dropped due to on-going transferring leader");
        }
    }

    public static class ThrottleByThresholdException extends DropProposalException {
        private ThrottleByThresholdException() {
            super("Proposal dropped due to too many uncommitted logs");
        }
    }

    public static class NoLeaderException extends DropProposalException {
        private NoLeaderException() {
            super("No leader elected");
        }
    }

    public static class LeaderForwardDisabledException extends DropProposalException {
        private LeaderForwardDisabledException() {
            super("Proposal forward feature is disabled");
        }
    }

    public static class ForwardTimeoutException extends DropProposalException {
        private ForwardTimeoutException() {
            super("Doesn't receive propose reply from leader within timeout");
        }
    }

    public static class OverriddenException extends DropProposalException {
        private OverriddenException() {
            super("Proposal dropped due to overridden by proposal from newer leader");
        }
    }

    public static class SupersededBySnapshotException extends DropProposalException {
        private SupersededBySnapshotException() {
            super("Proposal dropped due to superseded by snapshot");
        }
    }
}
