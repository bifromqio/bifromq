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

package com.baidu.bifromq.dist.server.scheduler;

import com.baidu.bifromq.dist.rpc.proto.AddTopicFilterReply;
import com.baidu.bifromq.dist.rpc.proto.InboxSubInfo;
import com.baidu.bifromq.dist.rpc.proto.JoinMatchGroupReply;

public abstract class SubCallResult {
    abstract SubCallType type();

    public static class AddTopicFilterResult extends SubCallResult {
        public final AddTopicFilterReply.Result result;

        public AddTopicFilterResult(AddTopicFilterReply.Result result) {
            this.result = result;
        }

        @Override
        final SubCallType type() {
            return SubCallType.ADD_TOPIC_FILTER;
        }
    }

    public static class InsertMatchRecordResult extends SubCallResult {
        @Override
        final SubCallType type() {
            return SubCallType.INSERT_MATCH_RECORD;
        }

    }

    public static class JoinMatchGroupResult extends SubCallResult {
        public final JoinMatchGroupReply.Result result;

        public JoinMatchGroupResult(JoinMatchGroupReply.Result result) {
            this.result = result;
        }

        @Override
        final SubCallType type() {
            return SubCallType.JOIN_MATCH_GROUP;
        }

    }

    public static class RemoveTopicFilterResult extends SubCallResult {
        public final boolean exist;

        public RemoveTopicFilterResult(boolean exist) {
            this.exist = exist;
        }

        @Override
        final SubCallType type() {
            return SubCallType.REMOVE_TOPIC_FILTER;
        }


    }

    public static class DeleteMatchRecordResult extends SubCallResult {
        public final boolean exist;

        public DeleteMatchRecordResult(boolean exist) {
            this.exist = exist;
        }

        @Override
        final SubCallType type() {
            return SubCallType.DELETE_MATCH_RECORD;
        }
    }

    public static class LeaveJoinGroupResult extends SubCallResult {
        @Override
        final SubCallType type() {
            return SubCallType.LEAVE_JOIN_GROUP;
        }
    }

    public static class ClearResult extends SubCallResult {
        public final InboxSubInfo subInfo;

        public ClearResult(InboxSubInfo subInfo) {
            this.subInfo = subInfo;
        }

        @Override
        final SubCallType type() {
            return SubCallType.CLEAR;
        }
    }
}
