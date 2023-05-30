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

public class DropProposalException extends RuntimeException {
    public static final DropProposalException TRANSFERRING_LEADER =
        new DropProposalException("Proposal dropped due to on-going transferring leader");

    public static final DropProposalException THROTTLED_BY_THRESHOLD =
        new DropProposalException("Proposal dropped due to too many uncommitted logs");

    public static final DropProposalException NO_LEADER = new DropProposalException("No leader elected");

    public static final DropProposalException LEADER_FORWARD_DISABLED
        = new DropProposalException("Proposal forward feature is disabled");

    public static final DropProposalException FORWARD_TIMEOUT =
        new DropProposalException("Doesn't receive propose reply from leader within timeout");

    public static final DropProposalException OVERRIDDEN
        = new DropProposalException("Proposal dropped due to overridden by proposal from newer leader");

    private DropProposalException(String message) {
        super(message);
    }
}
