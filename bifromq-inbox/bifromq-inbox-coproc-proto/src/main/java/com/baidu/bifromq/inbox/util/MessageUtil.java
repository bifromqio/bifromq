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

import com.baidu.bifromq.inbox.storage.proto.BatchCheckRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchCommitRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchCreateRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchFetchRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchInsertRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchSubRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchTouchRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchUnsubRequest;
import com.baidu.bifromq.inbox.storage.proto.CollectMetricsRequest;
import com.baidu.bifromq.inbox.storage.proto.GCRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
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

    public static InboxServiceRWCoProcInput buildCreateRequest(long reqId, BatchCreateRequest request) {
        return InboxServiceRWCoProcInput.newBuilder()
            .setReqId(reqId)
            .setBatchCreate(request)
            .build();
    }

    public static InboxServiceRWCoProcInput buildBatchSubRequest(long reqId, BatchSubRequest request) {
        return InboxServiceRWCoProcInput.newBuilder()
            .setReqId(reqId)
            .setBatchSub(request)
            .build();
    }

    public static InboxServiceRWCoProcInput buildBatchUnsubRequest(long reqId, BatchUnsubRequest request) {
        return InboxServiceRWCoProcInput.newBuilder()
            .setReqId(reqId)
            .setBatchUnsub(request)
            .build();
    }


    public static InboxServiceROCoProcInput buildHasRequest(long reqId, BatchCheckRequest request) {
        return InboxServiceROCoProcInput.newBuilder()
            .setReqId(reqId)
            .setBatchCheck(request)
            .build();
    }

    public static InboxServiceRWCoProcInput buildBatchInboxInsertRequest(long reqId, BatchInsertRequest request) {
        return InboxServiceRWCoProcInput.newBuilder()
            .setReqId(reqId)
            .setBatchInsert(request)
            .build();
    }

    public static InboxServiceROCoProcInput buildInboxFetchRequest(long reqId, BatchFetchRequest request) {
        return InboxServiceROCoProcInput.newBuilder()
            .setReqId(reqId)
            .setBatchFetch(request)
            .build();
    }

    public static InboxServiceRWCoProcInput buildTouchRequest(long reqId, BatchTouchRequest request) {
        return InboxServiceRWCoProcInput.newBuilder()
            .setReqId(reqId)
            .setBatchTouch(request)
            .build();
    }

    public static InboxServiceRWCoProcInput buildBatchCommitRequest(long reqId, BatchCommitRequest request) {
        return InboxServiceRWCoProcInput.newBuilder()
            .setReqId(reqId)
            .setBatchCommit(request)
            .build();
    }
}
