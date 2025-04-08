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

package com.baidu.bifromq.inbox.store.delay;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DelayTaskRunnerTest {
    private DelayTaskRunner<String> runner;
    private Supplier<Long> currentMillisSupplier;
    private BiFunction<String, IDelayTaskRunner<String>, CompletableFuture<Void>> taskFunction1;
    private BiFunction<String, IDelayTaskRunner<String>, CompletableFuture<Void>> taskFunction2;

    @BeforeMethod
    public void setUp() {
        currentMillisSupplier = System::currentTimeMillis;
        runner = new DelayTaskRunner<>(KVRangeIdUtil.generate(), "store1", String::compareTo, currentMillisSupplier,
            1000);
        taskFunction1 = mock(BiFunction.class);
        taskFunction2 = mock(BiFunction.class);
    }

    @Test
    public void testImmediateExecution() {
        TestDelayedTask task = new TestDelayedTask(Duration.ZERO, taskFunction1);

        Supplier<TestDelayedTask> supplier = () -> task;
        runner.reschedule("immediateKey", supplier, TestDelayedTask.class);
        verify(taskFunction1, timeout(100).times(1)).apply("immediateKey", runner);
        assertFalse(runner.hasTask("immidateKey"));
    }

    @Test
    public void testDelayedExecution() {
        TestDelayedTask task = new TestDelayedTask(Duration.ofMillis(50), taskFunction1);
        Supplier<TestDelayedTask> supplier = () -> task;

        runner.reschedule("delayedKey", supplier, TestDelayedTask.class);

        verify(taskFunction1, timeout(40).times(0)).apply(Mockito.anyString(), Mockito.any());
        verify(taskFunction1, timeout(100).times(1)).apply("delayedKey", runner);
        assertFalse(runner.hasTask("delayedKey"));
    }

    @Test
    public void testRescheduleSameType() throws Exception {
        TestDelayedTask task = new TestDelayedTask(Duration.ofMillis(50), taskFunction1);

        Supplier<TestDelayedTask> supplier = Mockito.mock(Supplier.class);
        when(supplier.get()).thenReturn(task);

        runner.reschedule("rescheduleKey", supplier, TestDelayedTask.class);
        Thread.sleep(10);
        runner.reschedule("rescheduleKey", supplier, TestDelayedTask.class);

        verify(supplier, timeout(100).times(1)).get();
        verify(taskFunction1, timeout(200).times(1)).apply("rescheduleKey", runner);
    }

    @Test
    public void testScheduleAnyway() {
        TestDelayedTask task1 = new TestDelayedTask(Duration.ofMillis(50), taskFunction1);
        TestDelayedTask task2 = new TestDelayedTask(Duration.ofMillis(50), taskFunction2);

        runner.schedule("ifAbsentKey", task1);
        runner.schedule("ifAbsentKey", task2);

        verify(taskFunction1, timeout(200).times(0)).apply("ifAbsentKey", runner);
        verify(taskFunction2, timeout(200).times(1)).apply("ifAbsentKey", runner);
    }

    @Test
    public void testScheduleIfAbsent() {
        TestDelayedTask task = new TestDelayedTask(Duration.ofMillis(50), taskFunction1);

        Supplier<TestDelayedTask> supplier = Mockito.mock(Supplier.class);
        when(supplier.get()).thenReturn(task);

        runner.scheduleIfAbsent("ifAbsentKey", supplier);
        runner.scheduleIfAbsent("ifAbsentKey", supplier);

        verify(supplier, timeout(100).times(1)).get();
        verify(taskFunction1, timeout(200).times(1)).apply("ifAbsentKey", runner);
    }

    @Test
    public void testHasTask() {
        TestDelayedTask task = new TestDelayedTask(Duration.ofMillis(1000), taskFunction1);

        Supplier<TestDelayedTask> supplier = Mockito.mock(Supplier.class);
        when(supplier.get()).thenReturn(task);

        runner.reschedule("hasTaskKey", supplier, TestDelayedTask.class);
        await().until(() -> runner.hasTask("hasTaskKey"));
        await().until(() -> runner.hasTask("hasTaskKey"));
    }

    @Test
    public void testRateLimiterBehavior() {
        int rateLimit = 2;
        DelayTaskRunner<String> rateLimitedRunner = new DelayTaskRunner<>(KVRangeIdUtil.generate(), "store1",
            String::compareTo, System::currentTimeMillis, rateLimit);

        List<Long> executionTimes = Collections.synchronizedList(new ArrayList<>());

        for (int i = 1; i <= 6; i++) {
            String key = "task" + i;
            Supplier<RecordingTask> supplier = () -> new RecordingTask(Duration.ofMillis(10), executionTimes);
            rateLimitedRunner.reschedule(key, supplier, RecordingTask.class);
        }

        await().atMost(Duration.ofSeconds(10)).until(() -> executionTimes.size() == 6);

        List<Long> times = new ArrayList<>(executionTimes);
        Collections.sort(times);
        long firstTime = times.get(0);
        long thirdTime = times.get(2);
        long fifthTime = times.get(4);

        long diff1 = thirdTime - firstTime;
        long diff2 = fifthTime - firstTime;

        assertTrue(diff1 >= 800);
        assertTrue(diff2 >= 1800);

        rateLimitedRunner.shutdown();

        await().atMost(Duration.ofSeconds(5))
            .until(() -> !rateLimitedRunner.hasTask("task1") && !rateLimitedRunner.hasTask("task2") &&
                !rateLimitedRunner.hasTask("task3") && !rateLimitedRunner.hasTask("task4") &&
                !rateLimitedRunner.hasTask("task5") && !rateLimitedRunner.hasTask("task6"));
    }

    @Test
    public void testShutdown() {
        TestDelayedTask task = new TestDelayedTask(Duration.ofMillis(50), taskFunction1);
        runner.reschedule("shutdownKey2", () -> task, TestDelayedTask.class);
        runner.shutdown();
        verify(taskFunction1, timeout(100).times(0)).apply("shutdownKey2", runner);
    }

    private static class RecordingTask implements IDelayedTask<String> {
        private final Duration delay;
        private final List<Long> executionTimes;

        RecordingTask(Duration delay, List<Long> executionTimes) {
            this.delay = delay;
            this.executionTimes = executionTimes;
        }

        @Override
        public Duration getDelay() {
            return delay;
        }

        @Override
        public CompletableFuture<Void> run(String key, IDelayTaskRunner<String> runner) {
            executionTimes.add(System.currentTimeMillis());
            return CompletableFuture.completedFuture(null);
        }
    }

    private static class TestDelayedTask implements IDelayedTask<String> {
        private final Duration delay;
        private final BiFunction<String, IDelayTaskRunner<String>, CompletableFuture<Void>> taskFunction;

        private TestDelayedTask(Duration delay,
                                BiFunction<String, IDelayTaskRunner<String>, CompletableFuture<Void>> taskFunction) {
            this.delay = delay;
            this.taskFunction = taskFunction;
        }

        @Override
        public Duration getDelay() {
            return delay;
        }

        @Override
        public CompletableFuture<Void> run(String key, IDelayTaskRunner<String> runner) {
            return taskFunction.apply(key, runner);
        }
    }
}
