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

package com.baidu.bifromq.inbox.server.scheduler;

import com.baidu.bifromq.basescheduler.IBatchCallScheduler;
import com.baidu.bifromq.inbox.rpc.proto.CommitReply;
import com.baidu.bifromq.inbox.rpc.proto.CommitRequest;
import com.baidu.bifromq.inbox.rpc.proto.SendResult;
import com.baidu.bifromq.inbox.storage.proto.MessagePack;

public interface IInboxComSertScheduler
    extends IBatchCallScheduler<IInboxComSertScheduler.ComSertCall, IInboxComSertScheduler.ComSertResult> {

    enum ComSertCallType {
        INSERT, COMMIT
    }

    abstract class ComSertCall {
        abstract ComSertCallType type();
    }

    class InsertCall extends ComSertCall {
        public final MessagePack messagePack;

        public InsertCall(MessagePack messagePack) {
            this.messagePack = messagePack;
        }

        @Override
        final ComSertCallType type() {
            return ComSertCallType.INSERT;
        }
    }

    class CommitCall extends ComSertCall {
        public final CommitRequest request;

        public CommitCall(CommitRequest request) {
            this.request = request;
        }

        @Override
        ComSertCallType type() {
            return ComSertCallType.COMMIT;
        }
    }

    abstract class ComSertResult {
        abstract ComSertCallType type();
    }

    class InsertResult extends ComSertResult {
        public final SendResult.Result result;

        public InsertResult(SendResult.Result result) {
            this.result = result;
        }

        @Override
        ComSertCallType type() {
            return ComSertCallType.INSERT;
        }
    }

    class CommitResult extends ComSertResult {
        public final CommitReply result;

        public CommitResult(CommitReply result) {
            this.result = result;
        }

        @Override
        ComSertCallType type() {
            return ComSertCallType.COMMIT;
        }
    }
}
