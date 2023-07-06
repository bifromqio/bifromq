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

package com.baidu.bifromq.basekv;

import static com.baidu.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getBootstrapMethod;
import static com.baidu.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getChangeReplicaConfigMethod;
import static com.baidu.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getExecuteMethod;
import static com.baidu.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getLinearizedQueryMethod;
import static com.baidu.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getMergeMethod;
import static com.baidu.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getQueryMethod;
import static com.baidu.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getRecoverMethod;
import static com.baidu.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getSplitMethod;
import static com.baidu.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getTransferLeadershipMethod;

import com.baidu.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc;
import com.baidu.bifromq.baserpc.BluePrint;

public class RPCBluePrint {
    public static final BluePrint INSTANCE = BluePrint.builder()
        .serviceDescriptor(BaseKVStoreServiceGrpc.getServiceDescriptor())
        .methodSemantic(getBootstrapMethod(), BluePrint.DDUnaryMethod.getInstance())
        .methodSemantic(getRecoverMethod(), BluePrint.DDUnaryMethod.getInstance())
        .methodSemantic(getChangeReplicaConfigMethod(), BluePrint.DDUnaryMethod.getInstance())
        .methodSemantic(getSplitMethod(), BluePrint.DDUnaryMethod.getInstance())
        .methodSemantic(getMergeMethod(), BluePrint.DDUnaryMethod.getInstance())
        .methodSemantic(getTransferLeadershipMethod(), BluePrint.DDUnaryMethod.getInstance())
        .methodSemantic(getExecuteMethod(), BluePrint.DDPipelineUnaryMethod.getInstance())
        .methodSemantic(getQueryMethod(), BluePrint.DDPipelineUnaryMethod.getInstance())
        .methodSemantic(getLinearizedQueryMethod(), BluePrint.DDPipelineUnaryMethod.getInstance())
        .build();
}
