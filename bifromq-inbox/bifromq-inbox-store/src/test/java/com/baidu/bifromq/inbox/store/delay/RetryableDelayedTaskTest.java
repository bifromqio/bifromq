/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.baidu.bifromq.inbox.record.InboxInstance;
import com.baidu.bifromq.inbox.record.TenantInboxInstance;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RetryableDelayedTaskTest {

    private TenantInboxInstance tenantInboxInstance;
    private IDelayTaskRunner<TenantInboxInstance> runner;

    @BeforeMethod
    public void setUp() {
        tenantInboxInstance = new TenantInboxInstance("tenant1", new InboxInstance("inbox1", 1L));
        runner = mock(IDelayTaskRunner.class);
    }

    @Test
    public void testRetryCountExceeded() throws Exception {
        TestTask task = new TestTask(Duration.ofMillis(100), 1L, 6, false);
        CompletableFuture<Void> future = task.run(tenantInboxInstance, runner);
        future.get();

        Assert.assertFalse(task.callOperationInvoked);
        verify(runner, Mockito.never()).scheduleIfAbsent(Mockito.any(), Mockito.any());
    }

    @Test
    public void testShouldRetry() throws Exception {
        TestTask task = new TestTask(Duration.ofMillis(100), 1L, 1, true);
        CompletableFuture<Void> future = task.run(tenantInboxInstance, runner);
        future.get();
        Assert.assertTrue(task.callOperationInvoked);

        verify(runner, Mockito.times(1)).scheduleIfAbsent(Mockito.eq(tenantInboxInstance), Mockito.any());
        ArgumentCaptor<Supplier> supplierCaptor = ArgumentCaptor.forClass(Supplier.class);
        verify(runner).scheduleIfAbsent(Mockito.eq(tenantInboxInstance), supplierCaptor.capture());
        Supplier supplier = supplierCaptor.getValue();
        RetryableDelayedTask<Boolean> retryTask = (RetryableDelayedTask<Boolean>) supplier.get();
        Assert.assertEquals(retryTask.retryCount, task.retryCount + 1);
    }

    @Test
    public void testNoRetry() throws Exception {
        TestTask task = new TestTask(Duration.ofMillis(100), 1L, 1, false);
        CompletableFuture<Void> future = task.run(tenantInboxInstance, runner);
        future.get();  // 等待任务执行完成
        Assert.assertTrue(task.callOperationInvoked);

        verify(runner, Mockito.never()).scheduleIfAbsent(Mockito.any(), Mockito.any());
    }

    private static class TestTask extends RetryableDelayedTask<Boolean> {
        private final Boolean response;
        boolean callOperationInvoked = false;

        public TestTask(Duration delay, long version, int retryCount, Boolean response) {
            super(delay, version, retryCount);
            this.response = response;
        }

        @Override
        protected CompletableFuture<Boolean> callOperation(TenantInboxInstance key,
                                                           IDelayTaskRunner<TenantInboxInstance> runner) {
            callOperationInvoked = true;
            return CompletableFuture.completedFuture(response);
        }

        @Override
        protected boolean shouldRetry(Boolean reply) {
            return reply;
        }

        @Override
        protected RetryableDelayedTask<Boolean> createRetryTask(Duration newDelay) {
            return new TestTask(newDelay, version, retryCount + 1, response);
        }

        @Override
        protected String getTaskName() {
            return "TestTask";
        }
    }
}