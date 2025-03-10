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

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.scheduler.BatchQueryCall;
import com.baidu.bifromq.basekv.client.scheduler.QueryCallBatcherKey;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.proto.ROCoProcOutput;
import com.baidu.bifromq.basescheduler.ICallTask;
import com.baidu.bifromq.inbox.record.TenantInboxInstance;
import com.baidu.bifromq.inbox.storage.proto.BatchCheckSubReply;
import com.baidu.bifromq.inbox.storage.proto.BatchCheckSubRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;
import com.baidu.bifromq.plugin.subbroker.CheckReply;
import java.time.Duration;
import java.util.Iterator;
import java.util.Queue;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BatchCheckSubCall extends BatchQueryCall<IInboxCheckSubScheduler.CheckMatchInfo, CheckReply.Code> {
    protected BatchCheckSubCall(KVRangeId rangeId, IBaseKVStoreClient storeClient, Duration pipelineExpiryTime) {
        super(rangeId, storeClient, true, pipelineExpiryTime);
    }

    @Override
    protected ROCoProcInput makeBatch(Iterator<IInboxCheckSubScheduler.CheckMatchInfo> reqIterator) {
        BatchCheckSubRequest.Builder reqBuilder = BatchCheckSubRequest.newBuilder().setNow(HLC.INST.getPhysical());
        reqIterator.forEachRemaining(request -> {
            TenantInboxInstance tenantInboxInstance = TenantInboxInstance.from(request.tenantId(), request.matchInfo());
            reqBuilder.addParams(BatchCheckSubRequest.Params.newBuilder().setTenantId(tenantInboxInstance.tenantId())
                .setInboxId(tenantInboxInstance.instance().inboxId())
                .setIncarnation(tenantInboxInstance.instance().incarnation())
                .setTopicFilter(request.matchInfo().getTopicFilter())
                .build());
        });
        long reqId = System.nanoTime();
        return ROCoProcInput.newBuilder().setInboxService(
                InboxServiceROCoProcInput.newBuilder().setReqId(reqId).setBatchCheckSub(reqBuilder.build()).build())
            .build();
    }

    @Override
    protected void handleOutput(
        Queue<ICallTask<IInboxCheckSubScheduler.CheckMatchInfo, CheckReply.Code, QueryCallBatcherKey>> batchedTasks,
        ROCoProcOutput output) {
        ICallTask<IInboxCheckSubScheduler.CheckMatchInfo, CheckReply.Code, QueryCallBatcherKey> task;
        assert batchedTasks.size() == output.getInboxService().getBatchCheckSub().getCodeCount();
        int i = 0;
        while ((task = batchedTasks.poll()) != null) {
            BatchCheckSubReply.Code code = output.getInboxService().getBatchCheckSub().getCode(i++);
            switch (code) {
                case OK -> task.resultPromise().complete(CheckReply.Code.OK);
                case NO_MATCH -> task.resultPromise().complete(CheckReply.Code.NO_SUB);
                case NO_INBOX -> task.resultPromise().complete(CheckReply.Code.NO_RECEIVER);
                default -> task.resultPromise().complete(CheckReply.Code.ERROR);
            }
        }
    }

    @Override
    protected void handleException(
        ICallTask<IInboxCheckSubScheduler.CheckMatchInfo, CheckReply.Code, QueryCallBatcherKey> callTask, Throwable e) {
        callTask.resultPromise().complete(CheckReply.Code.ERROR);
    }
}
