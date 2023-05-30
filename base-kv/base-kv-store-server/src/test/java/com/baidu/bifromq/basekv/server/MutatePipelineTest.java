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

package com.baidu.bifromq.basekv.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.Put;
import com.baidu.bifromq.basekv.store.IKVRangeStore;
import com.baidu.bifromq.basekv.store.exception.KVRangeException;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.protobuf.ByteString;
import io.grpc.stub.ServerCallStreamObserver;
import java.util.concurrent.CompletableFuture;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MutatePipelineTest {
    @Mock
    private IKVRangeStore rangeStore;

    @Mock
    private ServerCallStreamObserver streamObserver;

    @Test
    public void put() {
        MutatePipeline pipeline = new MutatePipeline(rangeStore, streamObserver);
        KVRangeId rangeId = KVRangeIdUtil.generate();
        ByteString putKey = ByteString.copyFromUtf8("put");
        ByteString putVal = ByteString.copyFromUtf8("val");
        Put put = Put.newBuilder()
            .setKey(putKey)
            .setValue(putVal)
            .build();
        KVRangeRWRequest putRequest = KVRangeRWRequest.newBuilder()
            .setReqId(1)
            .setVer(1)
            .setKvRangeId(rangeId)
            .setPut(put)
            .build();

        when(rangeStore.put(1, rangeId, putKey, putVal)).thenReturn(
            CompletableFuture.completedFuture(ByteString.empty()));

        KVRangeRWReply putReply = pipeline.handleRequest("_", putRequest).join();

        assertEquals(1, putReply.getReqId());
        assertEquals(ReplyCode.Ok, putReply.getCode());
        assertTrue(putReply.getPutResult().isEmpty());
    }

    @Test
    public void delete() {
        MutatePipeline pipeline = new MutatePipeline(rangeStore, streamObserver);
        KVRangeId rangeId = KVRangeIdUtil.generate();
        ByteString delKey = ByteString.copyFromUtf8("del");
        KVRangeRWRequest delRequest = KVRangeRWRequest.newBuilder()
            .setReqId(1)
            .setVer(1)
            .setKvRangeId(rangeId)
            .setDelete(delKey)
            .build();

        when(rangeStore.delete(1, rangeId, delKey)).thenReturn(CompletableFuture.completedFuture(ByteString.empty()));

        KVRangeRWReply delReply = pipeline.handleRequest("_", delRequest).join();

        assertEquals(1, delReply.getReqId());
        assertEquals(ReplyCode.Ok, delReply.getCode());
        assertTrue(delReply.getDeleteResult().isEmpty());
    }

    @Test
    public void mutateCoProc() {
        MutatePipeline pipeline = new MutatePipeline(rangeStore, streamObserver);
        KVRangeId rangeId = KVRangeIdUtil.generate();
        ByteString mutateCoProcInput = ByteString.copyFromUtf8("mutate");
        KVRangeRWRequest mutateRequest = KVRangeRWRequest.newBuilder()
            .setReqId(1)
            .setVer(1)
            .setKvRangeId(rangeId)
            .setRwCoProc(mutateCoProcInput)
            .build();

        when(rangeStore.mutateCoProc(1, rangeId, mutateCoProcInput)).thenReturn(
            CompletableFuture.completedFuture(ByteString.empty()));

        KVRangeRWReply mutateReply = pipeline.handleRequest("_", mutateRequest).join();

        assertEquals(1, mutateReply.getReqId());
        assertEquals(ReplyCode.Ok, mutateReply.getCode());
        assertTrue(mutateReply.getRwCoProcResult().isEmpty());
    }

    @Test
    public void errorCodeConversion() {
        MutatePipeline pipeline = new MutatePipeline(rangeStore, streamObserver);
        KVRangeId rangeId = KVRangeIdUtil.generate();
        ByteString putKey = ByteString.copyFromUtf8("put");
        ByteString putVal = ByteString.copyFromUtf8("val");
        Put put = Put.newBuilder()
            .setKey(putKey)
            .setValue(putVal)
            .build();
        // bad version
        KVRangeRWRequest putRequest = KVRangeRWRequest.newBuilder()
            .setReqId(1)
            .setVer(1)
            .setKvRangeId(rangeId)
            .setPut(put)
            .build();
        when(rangeStore.put(1, rangeId, putKey, putVal)).thenReturn(
            CompletableFuture.failedFuture(new KVRangeException.BadVersion("bad version")));
        KVRangeRWReply putReply = pipeline.handleRequest("_", putRequest).join();
        assertEquals(ReplyCode.BadVersion, putReply.getCode());

        // bad request
        putRequest = KVRangeRWRequest.newBuilder()
            .setReqId(1)
            .setVer(2)
            .setKvRangeId(rangeId)
            .setPut(put)
            .build();
        when(rangeStore.put(2, rangeId, putKey, putVal)).thenReturn(
            CompletableFuture.failedFuture(new KVRangeException.BadRequest("bad request")));
        putReply = pipeline.handleRequest("_", putRequest).join();
        assertEquals(ReplyCode.BadRequest, putReply.getCode());

        // try later
        putRequest = KVRangeRWRequest.newBuilder()
            .setReqId(1)
            .setVer(3)
            .setKvRangeId(rangeId)
            .setPut(put)
            .build();
        when(rangeStore.put(3, rangeId, putKey, putVal)).thenReturn(
            CompletableFuture.failedFuture(new KVRangeException.TryLater("try later")));
        putReply = pipeline.handleRequest("_", putRequest).join();
        assertEquals(ReplyCode.TryLater, putReply.getCode());

        putRequest = KVRangeRWRequest.newBuilder()
            .setReqId(1)
            .setVer(4)
            .setKvRangeId(rangeId)
            .setPut(put)
            .build();
        when(rangeStore.put(4, rangeId, putKey, putVal)).thenReturn(
            CompletableFuture.failedFuture(new KVRangeException.InternalException("internal error")));
        putReply = pipeline.handleRequest("_", putRequest).join();
        assertEquals(ReplyCode.InternalError, putReply.getCode());
    }
}
