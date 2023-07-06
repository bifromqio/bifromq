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

package com.baidu.bifromq.dist.util;

import static com.baidu.bifromq.dist.entity.EntityUtil.matchRecordKey;

import com.baidu.bifromq.dist.entity.EntityUtil;
import com.baidu.bifromq.dist.rpc.proto.AddTopicFilter;
import com.baidu.bifromq.dist.rpc.proto.BatchDist;
import com.baidu.bifromq.dist.rpc.proto.ClearSubInfo;
import com.baidu.bifromq.dist.rpc.proto.CollectMetricsRequest;
import com.baidu.bifromq.dist.rpc.proto.DeleteMatchRecord;
import com.baidu.bifromq.dist.rpc.proto.DistServiceROCoProcInput;
import com.baidu.bifromq.dist.rpc.proto.DistServiceRWCoProcInput;
import com.baidu.bifromq.dist.rpc.proto.GCRequest;
import com.baidu.bifromq.dist.rpc.proto.GroupMatchRecord;
import com.baidu.bifromq.dist.rpc.proto.InboxSubInfo;
import com.baidu.bifromq.dist.rpc.proto.InsertMatchRecord;
import com.baidu.bifromq.dist.rpc.proto.JoinMatchGroup;
import com.baidu.bifromq.dist.rpc.proto.LeaveMatchGroup;
import com.baidu.bifromq.dist.rpc.proto.QInboxIdList;
import com.baidu.bifromq.dist.rpc.proto.RemoveTopicFilter;
import com.baidu.bifromq.dist.rpc.proto.SubRequest;
import com.baidu.bifromq.dist.rpc.proto.TopicFilterList;
import com.baidu.bifromq.dist.rpc.proto.UnsubRequest;
import com.baidu.bifromq.dist.rpc.proto.UpdateRequest;
import com.google.protobuf.ByteString;

public class MessageUtil {
    public static DistServiceRWCoProcInput buildAddTopicFilterRequest(SubRequest request) {
        return DistServiceRWCoProcInput.newBuilder()
            .setUpdateRequest(UpdateRequest.newBuilder()
                .setReqId(request.getReqId())
                .setAddTopicFilter(AddTopicFilter.newBuilder()
                    .putTopicFilter(
                        EntityUtil.subInfoKey(request.getClient().getTenantId(),
                            EntityUtil.toQualifiedInboxId(request.getBroker(), request.getInboxId(),
                                request.getDelivererKey())).toStringUtf8(),
                        InboxSubInfo.newBuilder()
                            .putTopicFilters(request.getTopicFilter(), request.getSubQoS())
                            .build())
                    .build())
                .build())
            .build();
    }

    public static DistServiceRWCoProcInput buildInsertMatchRecordRequest(SubRequest request) {
        assert TopicUtil.isNormalTopicFilter(request.getTopicFilter());
        return DistServiceRWCoProcInput.newBuilder()
            .setUpdateRequest(UpdateRequest.newBuilder()
                .setReqId(request.getReqId())
                .setInsertMatchRecord(InsertMatchRecord.newBuilder().putRecord(
                    matchRecordKey(request.getClient().getTenantId(),
                        request.getTopicFilter(),
                        EntityUtil.toQualifiedInboxId(request.getBroker(), request.getInboxId(),
                            request.getDelivererKey())).toStringUtf8(),
                    request.getSubQoS()).build())
                .build())
            .build();
    }

    public static DistServiceRWCoProcInput buildJoinMatchGroupRequest(SubRequest request) {
        assert !TopicUtil.isNormalTopicFilter(request.getTopicFilter());
        return DistServiceRWCoProcInput.newBuilder()
            .setUpdateRequest(UpdateRequest.newBuilder()
                .setReqId(request.getReqId())
                .setJoinMatchGroup(JoinMatchGroup.newBuilder()
                    .putRecord(
                        matchRecordKey(request.getClient().getTenantId(), request.getTopicFilter(),
                            EntityUtil.toQualifiedInboxId(request.getBroker(), request.getInboxId(),
                                request.getDelivererKey())).toStringUtf8(),
                        GroupMatchRecord.newBuilder()
                            .putEntry(EntityUtil.toQualifiedInboxId(request.getBroker(),
                                request.getInboxId(),
                                request.getDelivererKey()), request.getSubQoS())
                            .build())
                    .build())
                .build())
            .build();

    }

    public static DistServiceRWCoProcInput buildRemoveTopicFilterRequest(UnsubRequest request) {
        return DistServiceRWCoProcInput.newBuilder()
            .setUpdateRequest(UpdateRequest.newBuilder()
                .setReqId(request.getReqId())
                .setRemoveTopicFilter(RemoveTopicFilter.newBuilder()
                    .putTopicFilter(
                        EntityUtil.subInfoKey(request.getClient().getTenantId(),
                            EntityUtil.toQualifiedInboxId(request.getBroker(), request.getInboxId(),
                                request.getDelivererKey())).toStringUtf8(),
                        TopicFilterList.newBuilder().addTopicFilter(request.getTopicFilter()).build())
                    .build())
                .build())
            .build();
    }

    public static DistServiceRWCoProcInput buildDeleteMatchRecordRequest(long reqId, String tenantId,
                                                                         String scopedInboxId,
                                                                         String topicFilter) {
        return DistServiceRWCoProcInput.newBuilder()
            .setUpdateRequest(UpdateRequest.newBuilder()
                .setReqId(reqId)
                .setDeleteMatchRecord(DeleteMatchRecord.newBuilder()
                    .addMatchRecordKey(matchRecordKey(tenantId, topicFilter, scopedInboxId).toStringUtf8())
                    .build())
                .build())
            .build();
    }

    public static DistServiceRWCoProcInput buildLeaveMatchGroupRequest(long reqId, String tenantId,
                                                                       String scopedInboxId,
                                                                       String topicFilter) {
        return DistServiceRWCoProcInput.newBuilder()
            .setUpdateRequest(UpdateRequest.newBuilder()
                .setReqId(reqId)
                .setLeaveMatchGroup(LeaveMatchGroup.newBuilder()
                    .putRecord(matchRecordKey(tenantId, topicFilter, scopedInboxId).toStringUtf8(),
                        QInboxIdList.newBuilder().addQInboxId(scopedInboxId).build())
                    .build())
                .build())
            .build();
    }

    public static DistServiceRWCoProcInput buildClearSubInfoRequest(long reqId, ByteString scopedInboxId) {
        return DistServiceRWCoProcInput.newBuilder()
            .setUpdateRequest(UpdateRequest.newBuilder()
                .setReqId(reqId)
                .setClearSubInfo(ClearSubInfo.newBuilder().addSubInfoKey(scopedInboxId).build())
                .build())
            .build();
    }

    public static DistServiceROCoProcInput buildBatchDistRequest(BatchDist request) {
        return DistServiceROCoProcInput.newBuilder().setDist(request).build();
    }

    public static DistServiceROCoProcInput buildGCRequest(long reqId) {
        return DistServiceROCoProcInput.newBuilder()
            .setGcRequest(GCRequest.newBuilder().setReqId(reqId).build())
            .build();
    }

    public static DistServiceROCoProcInput buildCollectMetricsRequest(long reqId) {
        return DistServiceROCoProcInput.newBuilder()
            .setCollectMetricsRequest(CollectMetricsRequest.newBuilder().setReqId(reqId).build())
            .build();
    }
}
