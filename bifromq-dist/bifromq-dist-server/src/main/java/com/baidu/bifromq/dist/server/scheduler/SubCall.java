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

import com.baidu.bifromq.dist.rpc.proto.ClearRequest;
import com.baidu.bifromq.dist.rpc.proto.SubRequest;
import com.baidu.bifromq.dist.rpc.proto.UnsubRequest;

public abstract class SubCall {

    abstract SubCallType type();

    public static class AddTopicFilter extends SubCall {
        public final SubRequest request;

        public AddTopicFilter(SubRequest request) {
            this.request = request;
        }

        @Override
        final SubCallType type() {
            return SubCallType.ADD_TOPIC_FILTER;
        }
    }

    public static class InsertMatchRecord extends SubCall {
        public final SubRequest request;

        public InsertMatchRecord(SubRequest request) {
            this.request = request;
        }

        @Override
        final SubCallType type() {
            return SubCallType.INSERT_MATCH_RECORD;
        }

    }

    public static class JoinMatchGroup extends SubCall {
        public final SubRequest request;

        public JoinMatchGroup(SubRequest request) {
            this.request = request;
        }

        @Override
        final SubCallType type() {
            return SubCallType.JOIN_MATCH_GROUP;
        }

    }

    public static class RemoveTopicFilter extends SubCall {
        public final UnsubRequest request;

        public RemoveTopicFilter(UnsubRequest request) {
            this.request = request;
        }

        @Override
        final SubCallType type() {
            return SubCallType.REMOVE_TOPIC_FILTER;
        }


    }

    public static class DeleteMatchRecord extends SubCall {
        public final UnsubRequest request;

        public DeleteMatchRecord(UnsubRequest request) {
            this.request = request;
        }

        @Override
        final SubCallType type() {
            return SubCallType.DELETE_MATCH_RECORD;
        }


    }

    public static class LeaveJoinGroup extends SubCall {
        public final UnsubRequest request;

        public LeaveJoinGroup(UnsubRequest request) {
            this.request = request;
        }

        @Override
        final SubCallType type() {
            return SubCallType.LEAVE_JOIN_GROUP;
        }
    }

    public static class Clear extends SubCall {
        public final ClearRequest request;

        public Clear(ClearRequest request) {
            this.request = request;
        }

        @Override
        final SubCallType type() {
            return SubCallType.CLEAR;
        }
    }
}
