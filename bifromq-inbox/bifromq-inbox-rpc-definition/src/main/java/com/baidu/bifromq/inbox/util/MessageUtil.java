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

package com.baidu.bifromq.inbox.util;

import com.baidu.bifromq.inbox.storage.proto.BatchAddSubRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchRemoveSubRequest;
import com.baidu.bifromq.inbox.storage.proto.CollectMetricsRequest;
import com.baidu.bifromq.inbox.storage.proto.CreateRequest;
import com.baidu.bifromq.inbox.storage.proto.GCRequest;
import com.baidu.bifromq.inbox.storage.proto.HasRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxCommitRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxFetchRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxInsertRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.TouchRequest;
import com.google.protobuf.ByteString;

public class MessageUtil {
    public static InboxServiceROCoProcInput buildGCRequest(long reqId, ByteString scopedInboxId, int limit) {
        GCRequest.Builder reqBuilder = GCRequest.newBuilder()
            .setReqId(reqId)
            .setLimit(limit);
        if (scopedInboxId != null) {
            reqBuilder.setScopedInboxId(scopedInboxId);
        }
        return InboxServiceROCoProcInput.newBuilder()
            .setReqId(reqId)
            .setGc(reqBuilder.build())
            .build();
    }

    public static InboxServiceROCoProcInput buildCollectMetricsRequest(long reqId) {
        return InboxServiceROCoProcInput.newBuilder()
            .setReqId(reqId)
            .setCollectMetrics(CollectMetricsRequest.newBuilder().setReqId(reqId).build())
            .build();
    }

    public static InboxServiceRWCoProcInput buildCreateRequest(long reqId, CreateRequest request) {
        return InboxServiceRWCoProcInput.newBuilder()
            .setReqId(reqId)
            .setCreateInbox(request)
            .build();
    }

    public static InboxServiceRWCoProcInput buildBatchAddSubRequest(long reqId, BatchAddSubRequest request) {
        return InboxServiceRWCoProcInput.newBuilder()
            .setReqId(reqId)
            .setAddTopicFilter(request)
            .build();
    }

    public static InboxServiceRWCoProcInput buildBatchRemoveSubRequest(long reqId, BatchRemoveSubRequest request) {
        return InboxServiceRWCoProcInput.newBuilder()
            .setReqId(reqId)
            .setRemoveTopicFilter(request)
            .build();
    }


    public static InboxServiceROCoProcInput buildHasRequest(long reqId, HasRequest request) {
        return InboxServiceROCoProcInput.newBuilder()
            .setReqId(reqId)
            .setHas(request)
            .build();
    }

    public static InboxServiceRWCoProcInput buildBatchInboxInsertRequest(long reqId, InboxInsertRequest request) {
        return InboxServiceRWCoProcInput.newBuilder()
            .setReqId(reqId)
            .setInsert(request)
            .build();
    }

    public static InboxServiceROCoProcInput buildInboxFetchRequest(long reqId, InboxFetchRequest request) {
        return InboxServiceROCoProcInput.newBuilder()
            .setReqId(reqId)
            .setFetch(request)
            .build();
    }

    public static InboxServiceRWCoProcInput buildTouchRequest(long reqId, TouchRequest request) {
        return InboxServiceRWCoProcInput.newBuilder()
            .setReqId(reqId)
            .setTouch(request)
            .build();
    }

    public static InboxServiceRWCoProcInput buildBatchCommitRequest(long reqId, InboxCommitRequest request) {
        return InboxServiceRWCoProcInput.newBuilder()
            .setReqId(reqId)
            .setCommit(request)
            .build();
    }
}
