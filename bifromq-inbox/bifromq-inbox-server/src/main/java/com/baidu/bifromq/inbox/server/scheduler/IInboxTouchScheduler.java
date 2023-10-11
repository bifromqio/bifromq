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

package com.baidu.bifromq.inbox.server.scheduler;

import static com.baidu.bifromq.inbox.util.KeyUtil.scopedInboxId;

import com.baidu.bifromq.basescheduler.IBatchCallScheduler;
import com.baidu.bifromq.inbox.rpc.proto.DeleteInboxRequest;
import com.baidu.bifromq.inbox.rpc.proto.TouchInboxRequest;
import com.google.protobuf.ByteString;
import java.util.List;

public interface IInboxTouchScheduler extends IBatchCallScheduler<IInboxTouchScheduler.Touch, List<String>> {
    class Touch {
        final String scopedInboxIdUtf8;
        final boolean keep;

        public Touch(DeleteInboxRequest req) {
            scopedInboxIdUtf8 = scopedInboxId(req.getTenantId(), req.getInboxId()).toStringUtf8();
            keep = false;
        }

        public Touch(TouchInboxRequest req) {
            scopedInboxIdUtf8 = scopedInboxId(req.getTenantId(), req.getInboxId()).toStringUtf8();
            keep = true;

        }

        public Touch(ByteString scopedInboxId) {
            scopedInboxIdUtf8 = scopedInboxId.toStringUtf8();
            keep = true;
        }

        public Touch(ByteString scopedInboxId, boolean keep) {
            scopedInboxIdUtf8 = scopedInboxId.toStringUtf8();
            this.keep = keep;
        }
    }
}
