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

import static com.baidu.bifromq.inbox.store.schema.KVSchemaUtil.inboxStartKeyPrefix;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.inbox.rpc.proto.SendLWTReply;
import com.baidu.bifromq.inbox.rpc.proto.SendLWTRequest;
import com.baidu.bifromq.sysprops.props.InboxCheckQueuesPerRange;
import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxSendLWTScheduler extends InboxReadScheduler<SendLWTRequest, SendLWTReply, BatchSendLWTCall>
    implements IInboxSendLWTScheduler {
    public InboxSendLWTScheduler(IBaseKVStoreClient inboxStoreClient) {
        super(BatchSendLWTCall::new, InboxCheckQueuesPerRange.INSTANCE.get(), inboxStoreClient);
    }

    @Override
    protected boolean isLinearizable(SendLWTRequest request) {
        return true;
    }

    @Override
    protected ByteString rangeKey(SendLWTRequest request) {
        return inboxStartKeyPrefix(request.getTenantId(), request.getInboxId());
    }
}
