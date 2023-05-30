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

public class RecoveryException extends RuntimeException {
    public static final RecoveryException NOT_LOST_QUORUM = new RecoveryException("Not lost quorum");

    public static final RecoveryException ABORT = new RecoveryException("Aborted");

    public static final RecoveryException NOT_VOTER = new RecoveryException("Not voter");

    public static final RecoveryException NOT_QUALIFY = new RecoveryException("Not qualify");

    public static final RecoveryException RECOVERY_IN_PROGRESS = new RecoveryException("There is recovery in progress");

    private RecoveryException(String message) {
        super(message);
    }
}
