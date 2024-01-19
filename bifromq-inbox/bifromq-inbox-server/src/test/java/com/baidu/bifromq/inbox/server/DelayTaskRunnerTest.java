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

package com.baidu.bifromq.inbox.server;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.fail;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DelayTaskRunnerTest {

    @Mock
    private Runnable deadLineTask1;
    @Mock
    private Runnable deadLineTask2;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @SneakyThrows
    public void teardown() {
        closeable.close();
    }

    @Test
    public void reg() {
        DelayTaskRunner<String, Runnable> trigger =
            new DelayTaskRunner<>(String::compareTo, System::currentTimeMillis);
        trigger.reg("key1", Duration.ofSeconds(1), deadLineTask1);
        verify(deadLineTask1, timeout(1100).times(1)).run();
    }

    @Test
    public void regWithNoDelay() {
        DelayTaskRunner<String, Runnable> trigger =
            new DelayTaskRunner<>(String::compareTo, System::currentTimeMillis);
        trigger.reg("key1", Duration.ZERO, deadLineTask1);
        verify(deadLineTask1, timeout(1000).times(1)).run();
    }

    @Test
    public void touchWithoutReg() {
        DelayTaskRunner<String, Runnable> trigger =
            new DelayTaskRunner<>(String::compareTo, System::currentTimeMillis);
        try {
            trigger.touch("inbox");
        } catch (Throwable e) {
            fail();
        }
    }

    @Test
    public void touchAndTrigger() {
        DelayTaskRunner<String, Runnable> trigger =
            new DelayTaskRunner<>(String::compareTo, System::currentTimeMillis);
        trigger.reg("key1", Duration.ofSeconds(1), deadLineTask1);
        trigger.touch("key1");
        verify(deadLineTask1, timeout(1100).times(0)).run();
    }

    @Test
    public void regTwoInboxes() {
        DelayTaskRunner<String, Runnable> trigger =
            new DelayTaskRunner<>(String::compareTo, System::currentTimeMillis);
        trigger.reg("inbox1", Duration.ofSeconds(2), deadLineTask1);
        trigger.reg("inbox2", Duration.ofSeconds(1), deadLineTask2);
        verify(deadLineTask1, timeout(2100).times(1)).run();
        verify(deadLineTask2, timeout(1100).times(1)).run();
    }

    @SneakyThrows
    @Test
    public void touchAndReschedule() {
        DelayTaskRunner<String, Runnable> trigger =
            new DelayTaskRunner<>(String::compareTo, System::currentTimeMillis);
        AtomicInteger counter = new AtomicInteger();
        trigger.reg("inbox1", Duration.ofSeconds(1), counter::incrementAndGet);
        Thread.sleep(800);
        trigger.touch("inbox1");
        Thread.sleep(800);
        trigger.touch("inbox1");
        await().atMost(Duration.ofSeconds(3)).until(() -> counter.get() == 1);
    }

    @Test
    public void unreg() {
        DelayTaskRunner<String, Runnable> trigger =
            new DelayTaskRunner<>(String::compareTo, System::currentTimeMillis);
        trigger.reg("inbox1", Duration.ofSeconds(1), deadLineTask1);
        trigger.unreg("inbox1");
        verify(deadLineTask1, timeout(1100).times(0)).run();
    }
}
