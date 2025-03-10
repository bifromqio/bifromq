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

package com.baidu.bifromq.inbox;

import static com.baidu.bifromq.inbox.util.InboxServiceUtil.getDelivererKey;

import com.baidu.bifromq.baserpc.BluePrint;
import com.baidu.bifromq.inbox.rpc.proto.AttachRequest;
import com.baidu.bifromq.inbox.rpc.proto.CommitRequest;
import com.baidu.bifromq.inbox.rpc.proto.CreateRequest;
import com.baidu.bifromq.inbox.rpc.proto.DetachRequest;
import com.baidu.bifromq.inbox.rpc.proto.ExpireRequest;
import com.baidu.bifromq.inbox.rpc.proto.GetRequest;
import com.baidu.bifromq.inbox.rpc.proto.InboxServiceGrpc;
import com.baidu.bifromq.inbox.rpc.proto.SubRequest;
import com.baidu.bifromq.inbox.rpc.proto.TouchRequest;
import com.baidu.bifromq.inbox.rpc.proto.UnsubRequest;
import com.baidu.bifromq.plugin.subbroker.CheckRequest;

public class RPCBluePrint {
    public static final BluePrint INSTANCE = BluePrint.builder()
        .serviceDescriptor(InboxServiceGrpc.getServiceDescriptor())
        // inbox related rpc must be routed using WCH mode
        // broker client rpc
        .methodSemantic(InboxServiceGrpc.getReceiveMethod(), BluePrint.WCHPipelineUnaryMethod.getInstance())
        .methodSemantic(InboxServiceGrpc.getFetchMethod(), BluePrint.WCHStreamingMethod.getInstance())
        .methodSemantic(InboxServiceGrpc.getCheckSubscriptionsMethod(), BluePrint.WCHUnaryMethod.<CheckRequest>builder()
            .keyHashFunc(CheckRequest::getDelivererKey).build())
        // both broker and reader client rpc
        .methodSemantic(InboxServiceGrpc.getGetMethod(), BluePrint.WCHUnaryMethod.<GetRequest>builder()
            .keyHashFunc(request -> getDelivererKey(request.getTenantId(), request.getInboxId())).build())
        // reader client rpc
        .methodSemantic(InboxServiceGrpc.getAttachMethod(), BluePrint.WCHUnaryMethod.<AttachRequest>builder()
            .keyHashFunc(request -> getDelivererKey(request.getClient().getTenantId(), request.getInboxId())).build())
        .methodSemantic(InboxServiceGrpc.getDetachMethod(), BluePrint.WCHUnaryMethod.<DetachRequest>builder()
            .keyHashFunc(request -> getDelivererKey(request.getClient().getTenantId(), request.getInboxId())).build())
        .methodSemantic(InboxServiceGrpc.getExpireMethod(), BluePrint.WCHUnaryMethod.<ExpireRequest>builder()
            .keyHashFunc(request -> getDelivererKey(request.getTenantId(), request.getInboxId())).build())
        .methodSemantic(InboxServiceGrpc.getCreateMethod(), BluePrint.WCHUnaryMethod.<CreateRequest>builder()
            .keyHashFunc(request -> getDelivererKey(request.getClient().getTenantId(), request.getInboxId())).build())
        .methodSemantic(InboxServiceGrpc.getTouchMethod(), BluePrint.WCHUnaryMethod.<TouchRequest>builder()
            .keyHashFunc(request -> getDelivererKey(request.getTenantId(), request.getInboxId())).build())
        .methodSemantic(InboxServiceGrpc.getSubMethod(), BluePrint.WCHUnaryMethod.<SubRequest>builder()
            .keyHashFunc(request -> getDelivererKey(request.getTenantId(), request.getInboxId())).build())
        .methodSemantic(InboxServiceGrpc.getUnsubMethod(), BluePrint.WCHUnaryMethod.<UnsubRequest>builder()
            .keyHashFunc(request -> getDelivererKey(request.getTenantId(), request.getInboxId())).build())
        .methodSemantic(InboxServiceGrpc.getCommitMethod(), BluePrint.WCHUnaryMethod
            .<CommitRequest>builder()
            .keyHashFunc(request -> getDelivererKey(request.getTenantId(), request.getInboxId()))
            .build())
        // expire all
        .methodSemantic(InboxServiceGrpc.getExpireAllMethod(), BluePrint.WRUnaryMethod.getInstance())
        .build();
}
