/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.mqtt.handler;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.mqtt.MockableTest;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class ConditionalSlowDownHandlerTest extends MockableTest {
    @Mock
    private Supplier<Boolean> slowDownCondition;

    @Test
    public void testAutoReadDisable() {
        when(slowDownCondition.get()).thenReturn(true);
        EmbeddedChannel embeddedChannel = new EmbeddedChannel(new ConditionalSlowDownHandler(slowDownCondition));
        embeddedChannel.writeInbound("1");
        assertEquals(embeddedChannel.readInbound(), "1");
        assertFalse(embeddedChannel.config().isAutoRead());

        embeddedChannel.advanceTimeBy(1, TimeUnit.SECONDS);
        embeddedChannel.runScheduledPendingTasks();
        assertFalse(embeddedChannel.config().isAutoRead());

        when(slowDownCondition.get()).thenReturn(false);
        embeddedChannel.advanceTimeBy(1, TimeUnit.SECONDS);
        embeddedChannel.runScheduledPendingTasks();
        assertTrue(embeddedChannel.config().isAutoRead());
    }
}
