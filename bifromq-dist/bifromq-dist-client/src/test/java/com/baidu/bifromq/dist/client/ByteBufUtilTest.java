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

package com.baidu.bifromq.dist.client;

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class ByteBufUtilTest {
    @Test
    public void releaseWhenGC() {
        ByteBuf nettyBuffer = Unpooled.directBuffer(10);
        assertEquals(nettyBuffer.refCnt(), 1);
        ByteString byteString = ByteBufUtil.toRetainedByteBuffer(nettyBuffer);
        assertEquals(nettyBuffer.refCnt(), 2);
        byteString = null;
        await().until(() -> {
            System.gc();
            return nettyBuffer.refCnt() == 1;
        });
    }
}
