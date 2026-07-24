/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bifromq.apiserver.http.handler;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import io.reactivex.rxjava3.core.Observable;
import java.util.Optional;
import java.util.Set;
import lombok.SneakyThrows;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.testng.annotations.Test;

public class GetStoreRangesHandlerTest extends AbstractHTTPRequestHandlerTest<GetStoreRangesHandler> {
    @Override
    protected Class<GetStoreRangesHandler> handlerClass() {
        return GetStoreRangesHandler.class;
    }

    @SneakyThrows
    @Test
    public void encodeBoundaryKeysAsCanonicalHex() {
        KVRangeDescriptor collisionRange = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setEpoch(1).setId(1).build())
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFrom(new byte[] {0x00, 0x00, 0x00}))
                .setEndKey(ByteString.copyFrom(new byte[] {0x30, 0x78, 0x30, 0x30, 0x00, 0x00}))
                .build())
            .build();
        KVRangeDescriptor emptyRange = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setEpoch(1).setId(2).build())
            .setBoundary(Boundary.newBuilder().setStartKey(ByteString.EMPTY).build())
            .build();
        KVRangeDescriptor binaryRange = KVRangeDescriptor.newBuilder()
            .setId(KVRangeId.newBuilder().setEpoch(1).setId(3).build())
            .setBoundary(Boundary.newBuilder()
                .setStartKey(ByteString.copyFrom(new byte[] {0x7F, (byte) 0x80, (byte) 0xFF}))
                .build())
            .build();
        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("store1")
            .addRanges(collisionRange)
            .addRanges(emptyRange)
            .addRanges(binaryRange)
            .build();
        when(metaService.clusterIds()).thenReturn(Observable.just(Set.of("dist.worker")));
        when(landscapeObserver.getStoreDescriptor("store1")).thenReturn(Optional.of(storeDescriptor));

        GetStoreRangesHandler handler = new GetStoreRangesHandler(metaService);
        handler.start();
        DefaultFullHttpRequest req = buildRequest(HttpMethod.GET);
        req.headers().set("store_name", "dist.worker");
        req.headers().set("store_id", "store1");
        FullHttpResponse resp = handler.handle(111, req).join();

        assertEquals(resp.protocolVersion(), req.protocolVersion());
        assertEquals(resp.status(), HttpResponseStatus.OK);
        assertEquals(resp.headers().get("Content-Type"), "application/json");

        ArrayNode ranges = (ArrayNode) new ObjectMapper().readTree(resp.content().toString(CharsetUtil.UTF_8));
        assertEquals(ranges.size(), 3);
        ObjectNode collisionBoundary = (ObjectNode) ranges.get(0).get("boundary");
        assertEquals(collisionBoundary.get("startKey").asText(), "000000");
        assertEquals(collisionBoundary.get("endKey").asText(), "307830300000");
        ObjectNode emptyBoundary = (ObjectNode) ranges.get(1).get("boundary");
        assertEquals(emptyBoundary.get("startKey").asText(), "");
        assertTrue(emptyBoundary.get("endKey").isNull());
        ObjectNode binaryBoundary = (ObjectNode) ranges.get(2).get("boundary");
        assertEquals(binaryBoundary.get("startKey").asText(), "7f80ff");
    }
}
