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

package com.baidu.bifromq.inbox.store;

import com.baidu.bifromq.inbox.storage.proto.BatchAttachRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchCommitRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchDeleteRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchDetachRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchExistRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchFetchRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchInsertRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchSendLWTRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchSubRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchUnsubRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;

public class MessageUtil {
    public static InboxServiceRWCoProcInput buildAttachRequest(long reqId, BatchAttachRequest request) {
        return InboxServiceRWCoProcInput.newBuilder()
            .setReqId(reqId)
            .setBatchAttach(request)
            .build();
    }

    public static InboxServiceRWCoProcInput buildDetachRequest(long reqId, BatchDetachRequest request) {
        return InboxServiceRWCoProcInput.newBuilder()
            .setReqId(reqId)
            .setBatchDetach(request)
            .build();
    }

    public static InboxServiceRWCoProcInput buildSubRequest(long reqId, BatchSubRequest request) {
        return InboxServiceRWCoProcInput.newBuilder()
            .setReqId(reqId)
            .setBatchSub(request)
            .build();
    }

    public static InboxServiceRWCoProcInput buildUnsubRequest(long reqId, BatchUnsubRequest request) {
        return InboxServiceRWCoProcInput.newBuilder()
            .setReqId(reqId)
            .setBatchUnsub(request)
            .build();
    }

    public static InboxServiceROCoProcInput buildExistRequest(long reqId, BatchExistRequest request) {
        return InboxServiceROCoProcInput.newBuilder()
            .setReqId(reqId)
            .setBatchExist(request)
            .build();
    }

    public static InboxServiceROCoProcInput buildSendLWTRequest(long reqId, BatchSendLWTRequest request) {
        return InboxServiceROCoProcInput.newBuilder()
            .setReqId(reqId)
            .setBatchSendLWT(request)
            .build();
    }

    public static InboxServiceRWCoProcInput buildInsertRequest(long reqId, BatchInsertRequest request) {
        return InboxServiceRWCoProcInput.newBuilder()
            .setReqId(reqId)
            .setBatchInsert(request)
            .build();
    }

    public static InboxServiceRWCoProcInput buildCommitRequest(long reqId, BatchCommitRequest request) {
        return InboxServiceRWCoProcInput.newBuilder()
            .setReqId(reqId)
            .setBatchCommit(request)
            .build();
    }

    public static InboxServiceROCoProcInput buildFetchRequest(long reqId, BatchFetchRequest request) {
        return InboxServiceROCoProcInput.newBuilder()
            .setReqId(reqId)
            .setBatchFetch(request)
            .build();
    }

    public static InboxServiceRWCoProcInput buildDeleteRequest(long reqId, BatchDeleteRequest request) {
        return InboxServiceRWCoProcInput.newBuilder()
            .setReqId(reqId)
            .setBatchDelete(request)
            .build();
    }
}
