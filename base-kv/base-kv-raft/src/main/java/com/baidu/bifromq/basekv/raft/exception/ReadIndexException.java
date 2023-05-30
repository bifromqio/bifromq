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

public class ReadIndexException extends RuntimeException {
    public static final ReadIndexException COMMIT_INDEX_NOT_CONFIRMED =
        new ReadIndexException("Leader has not confirmed the commit index of its term");

    public static final ReadIndexException LEADER_STEP_DOWN =
        new ReadIndexException("Leader has been stepped down");

    public static final ReadIndexException NO_LEADER = new ReadIndexException("No leader elected");

    public static final ReadIndexException FORWARD_TIMEOUT =
        new ReadIndexException("Doesn't receive read index from leader within timeout");

    private ReadIndexException(String message) {
        super(message);
    }
}
