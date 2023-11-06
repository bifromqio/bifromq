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

package com.baidu.bifromq.basekv.store.wal;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basekv.MockableTest;
import com.baidu.bifromq.basekv.proto.KVRangeSnapshot;
import com.baidu.bifromq.basekv.raft.proto.LogEntry;
import com.baidu.bifromq.basekv.store.exception.KVRangeException;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.MoreExecutors;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

@Slf4j
public class KVRangeWALSubscriptionTest extends MockableTest {
    private long maxSize = 1024;
    @Mock
    private IKVRangeWAL wal;
    private PublishSubject<IKVRangeWAL.SnapshotInstallTask> snapshotSource = PublishSubject.create();
    private BehaviorSubject<Long> commitIndexSource = BehaviorSubject.create();
    @Mock
    private IKVRangeWALSubscriber subscriber;
    private ExecutorService executor;

    protected void doSetup(Method method) {
        executor = Executors.newSingleThreadScheduledExecutor();
        when(wal.snapshotInstallTask()).thenReturn(snapshotSource);
        when(wal.rangeId()).thenReturn(KVRangeIdUtil.generate());
    }

    protected void doTeardown(Method method) {
        MoreExecutors.shutdownAndAwaitTermination(executor, Duration.ofSeconds(5));
    }

    @SneakyThrows
    @Test
    public void retrieveFailAndRetry() {
        when(wal.retrieveCommitted(0, maxSize))
            .thenReturn(
                CompletableFuture.failedFuture(new IllegalArgumentException()),
                CompletableFuture.completedFuture(Iterators.forArray(LogEntry.newBuilder()
                    .setTerm(0)
                    .setIndex(0)
                    .build())));
        CountDownLatch latch = new CountDownLatch(1);
        when(subscriber.apply(any(LogEntry.class))).thenAnswer(
            (Answer<CompletableFuture<Void>>) invocationOnMock -> {
                latch.countDown();
                return CompletableFuture.completedFuture(null);
            });

        KVRangeWALSubscription walSub =
            new KVRangeWALSubscription(maxSize, wal, commitIndexSource, -1, subscriber, executor);
        commitIndexSource.onNext(0L);
        latch.await();
        verify(wal, times(2)).retrieveCommitted(0, maxSize);
    }

    @SneakyThrows
    @Test
    public void stopRetryWhenStop() {
        CountDownLatch latch = new CountDownLatch(2);

        when(wal.retrieveCommitted(0, maxSize))
            .thenAnswer((Answer<CompletableFuture<Iterator<LogEntry>>>) invocationOnMock -> {
                latch.countDown();
                return CompletableFuture.failedFuture(
                    new IllegalArgumentException());
            });

        KVRangeWALSubscription walSub =
            new KVRangeWALSubscription(maxSize, wal, commitIndexSource, -1, subscriber, executor);
        commitIndexSource.onNext(0L);
        latch.await();
        walSub.stop();
        verify(wal, atLeast(2)).retrieveCommitted(0, maxSize);
    }

    @SneakyThrows
    @Test
    public void stopRetryWhenSnapshot() {
        AtomicInteger retryCount = new AtomicInteger();
        KVRangeSnapshot snapshot = KVRangeSnapshot.getDefaultInstance();
        when(wal.retrieveCommitted(0, maxSize))
            .thenAnswer((Answer<CompletableFuture<Iterator<LogEntry>>>) invocationOnMock -> {
                retryCount.incrementAndGet();
                return CompletableFuture.failedFuture(
                    new IllegalArgumentException("For Testing"));
            });
        CountDownLatch latch = new CountDownLatch(1);
        when(subscriber.install(any(KVRangeSnapshot.class), anyString())).thenAnswer(
            (Answer<CompletableFuture<KVRangeSnapshot>>) invocationOnMock -> {
                latch.countDown();
                return CompletableFuture.completedFuture(snapshot);
            });

        KVRangeWALSubscription walSub =
            new KVRangeWALSubscription(maxSize, wal, commitIndexSource, -1, subscriber, executor);
        commitIndexSource.onNext(0L);
        await().until(() -> retryCount.get() > 2);
        snapshotSource.onNext(
            new IKVRangeWAL.SnapshotInstallTask(snapshot.toByteString(), "leader"));
        latch.await();
        await().until(() -> {
            int c = retryCount.get();
            Thread.sleep(100);
            return retryCount.get() == c;
        });
        verify(wal, atLeast(2)).retrieveCommitted(0, maxSize);
    }

    @SneakyThrows
    @Test
    public void reapplyLog() {
        when(wal.retrieveCommitted(0, maxSize)).thenReturn(CompletableFuture.completedFuture(
            Iterators.forArray(
                LogEntry.newBuilder().setTerm(0).setIndex(0).build(),
                LogEntry.newBuilder().setTerm(0).setIndex(1).build()))
        );
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger applyCount = new AtomicInteger();
        when(subscriber.apply(any(LogEntry.class)))
            .thenAnswer((Answer<CompletableFuture<Void>>) invocationOnMock -> {
                log.info("invoke");
                if (applyCount.getAndIncrement() == 0) {
                    return CompletableFuture.failedFuture(new KVRangeException.TryLater("try again"));
                }
                latch.countDown();
                return CompletableFuture.completedFuture(null);
            });
        KVRangeWALSubscription walSub =
            new KVRangeWALSubscription(maxSize, wal, commitIndexSource, -1, subscriber, executor);
        commitIndexSource.onNext(0L);
        latch.await();
        log.info("{}", applyCount.get());
        assertTrue(1 < applyCount.get());
        ArgumentCaptor<LogEntry> logEntryCap = ArgumentCaptor.forClass(LogEntry.class);
        await().until(() -> {
            verify(subscriber, atLeast(2)).apply(logEntryCap.capture());
            return logEntryCap.getAllValues().get(0).getIndex() == 0 &&
                logEntryCap.getAllValues().get(logEntryCap.getAllValues().size() - 1).getIndex() == 1;
        });
    }

    @SneakyThrows
    @Test
    public void cancelApplyLogWhenSnapshot() {
        when(wal.retrieveCommitted(0, maxSize)).thenReturn(CompletableFuture.completedFuture(
            Iterators.forArray(
                LogEntry.newBuilder().setTerm(0).setIndex(0).build(),
                LogEntry.newBuilder().setTerm(0).setIndex(1).build()))
        );
        CountDownLatch latch = new CountDownLatch(1);
        CompletableFuture<Void> applyLogFuture = new CompletableFuture<>();
        when(subscriber.apply(any(LogEntry.class)))
            .thenAnswer((Answer<CompletableFuture<Void>>) invocationOnMock -> {
                latch.countDown();
                return applyLogFuture;
            });
        KVRangeWALSubscription walSub =
            new KVRangeWALSubscription(maxSize, wal, commitIndexSource, -1, subscriber, executor);
        commitIndexSource.onNext(0L);
        latch.await();
        snapshotSource.onNext(
            new IKVRangeWAL.SnapshotInstallTask(KVRangeSnapshot.getDefaultInstance().toByteString(), "leader"));
        await().until(applyLogFuture::isCancelled);
    }

    @SneakyThrows
    @Test
    public void cancelReapplyWhenSnapshot() {
        when(wal.retrieveCommitted(0, maxSize)).thenReturn(CompletableFuture.completedFuture(
            Iterators.forArray(
                LogEntry.newBuilder().setTerm(0).setIndex(0).build(),
                LogEntry.newBuilder().setTerm(0).setIndex(1).build()))
        );
        KVRangeSnapshot snapshot = KVRangeSnapshot.getDefaultInstance();
        AtomicInteger retryCount = new AtomicInteger();
        when(subscriber.apply(any(LogEntry.class)))
            .thenAnswer((Answer<CompletableFuture<Void>>) invocationOnMock -> {
                retryCount.incrementAndGet();
                return CompletableFuture.failedFuture(new KVRangeException.TryLater("Try again"));
            });
        CountDownLatch latch = new CountDownLatch(1);
        when(subscriber.install(any(KVRangeSnapshot.class), anyString()))
            .thenAnswer((Answer<CompletableFuture<KVRangeSnapshot>>) invocationOnMock -> {
                latch.countDown();
                return CompletableFuture.completedFuture(snapshot);
            });
        KVRangeWALSubscription walSub =
            new KVRangeWALSubscription(maxSize, wal, commitIndexSource, -1, subscriber, executor);
        commitIndexSource.onNext(0L);
        await().until(() -> retryCount.get() > 2);
        snapshotSource.onNext(
            new IKVRangeWAL.SnapshotInstallTask(snapshot.toByteString(), "leader"));
        latch.await();
        int c = retryCount.get();
        Thread.sleep(100);
        assertEquals(retryCount.get(), c);
    }

    @SneakyThrows
    @Test
    public void cancelApplySnapshot() {
        CountDownLatch latch = new CountDownLatch(1);
        CompletableFuture<KVRangeSnapshot> applySnapshotFuture = new CompletableFuture<>();
        when(subscriber.install(any(KVRangeSnapshot.class), eq("leader")))
            .thenAnswer((Answer<CompletableFuture<KVRangeSnapshot>>) invocationOnMock -> {
                latch.countDown();
                return applySnapshotFuture;
            });
        KVRangeWALSubscription walSub =
            new KVRangeWALSubscription(maxSize, wal, commitIndexSource, 0, subscriber, executor);
        snapshotSource.onNext(
            new IKVRangeWAL.SnapshotInstallTask(KVRangeSnapshot.getDefaultInstance().toByteString(), "leader"));
        latch.await();
        snapshotSource.onNext(
            new IKVRangeWAL.SnapshotInstallTask(KVRangeSnapshot.getDefaultInstance().toByteString(), "leader"));
        await().until(applySnapshotFuture::isCancelled);
    }

    @SneakyThrows
    @Test
    public void cancelApplySnapshotWhenStop() {
        CountDownLatch latch = new CountDownLatch(1);
        CompletableFuture<KVRangeSnapshot> applySnapshotFuture = new CompletableFuture<>();
        when(subscriber.install(any(KVRangeSnapshot.class), anyString()))
            .thenAnswer((Answer<CompletableFuture<KVRangeSnapshot>>) invocationOnMock -> {
                latch.countDown();
                return applySnapshotFuture;
            });
        KVRangeWALSubscription walSub =
            new KVRangeWALSubscription(maxSize, wal, commitIndexSource, 0, subscriber, executor);
        snapshotSource.onNext(
            new IKVRangeWAL.SnapshotInstallTask(KVRangeSnapshot.getDefaultInstance().toByteString(), "leader"));
        latch.await();
        walSub.stop();
        await().until(applySnapshotFuture::isCancelled);
    }

    @SneakyThrows
    @Test
    public void applyLogsAndSnapshot() {
        LogEntry entry1 = LogEntry.newBuilder().setTerm(0).setIndex(0).build();
        LogEntry entry2 = LogEntry.newBuilder().setTerm(0).setIndex(1).build();
        when(wal.retrieveCommitted(0, maxSize))
            .thenReturn(CompletableFuture.completedFuture(Iterators.forArray(entry1, entry2)));
        CountDownLatch latch = new CountDownLatch(1);
        CompletableFuture<Void> applyLogFuture1 = new CompletableFuture<>();
        when(subscriber.apply(entry1))
            .thenAnswer((Answer<CompletableFuture<Void>>) invocationOnMock -> {
                latch.countDown();
                return applyLogFuture1;
            });
        KVRangeWALSubscription walSub =
            new KVRangeWALSubscription(maxSize, wal, commitIndexSource, -1, subscriber, executor);
        commitIndexSource.onNext(0L);
        latch.await();
        snapshotSource.onNext(
            new IKVRangeWAL.SnapshotInstallTask(KVRangeSnapshot.getDefaultInstance().toByteString(), "leader"));
        await().until(applyLogFuture1::isCancelled);
        verify(subscriber, times(1)).apply(any(LogEntry.class));
        verify(subscriber, times(1)).install(KVRangeSnapshot.getDefaultInstance(), "leader");
    }
}
