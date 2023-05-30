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

public class LeaderTransferException extends RuntimeException {
    public static final LeaderTransferException LEADER_NOT_READY =
        new LeaderTransferException("Leader has not been ready " +
            "due to commit index of its term has not been confirmed");

    public static final LeaderTransferException TRANSFERRING_IN_PROGRESS =
        new LeaderTransferException("There is transferring in progress");

    public static final LeaderTransferException SELF_TRANSFER = new LeaderTransferException("Cannot transfer to self");

    public static final LeaderTransferException STEP_DOWN_BY_OTHER =
        new LeaderTransferException("Step down by another candidate");

    public static final LeaderTransferException NOT_FOUND_OR_QUALIFIED =
        new LeaderTransferException("Transferee not found or not qualified");

    public static final LeaderTransferException TRANSFER_TIMEOUT =
        new LeaderTransferException("Cannot finish transfer within one election timeout");

    public static final LeaderTransferException NOT_LEADER = new LeaderTransferException("Only leader can do transfer");

    public static final LeaderTransferException NO_LEADER = new LeaderTransferException("No leader elected");

    private LeaderTransferException(String message) {
        super(message);
    }
}
