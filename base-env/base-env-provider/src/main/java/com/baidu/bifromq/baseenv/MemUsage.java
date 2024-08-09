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

package com.baidu.bifromq.baseenv;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufAllocatorMetric;
import io.netty.buffer.ByteBufAllocatorMetricProvider;
import io.netty.buffer.PoolArenaMetric;
import io.netty.buffer.PoolChunkListMetric;
import io.netty.buffer.PoolChunkMetric;
import io.netty.buffer.PooledByteBufAllocatorMetric;
import io.netty.util.internal.PlatformDependent;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public class MemUsage {
    private static final double INLINE_REFRESH_THRESHOLD = 0.6;
    private static final long UPDATE_INTERVAL = Duration.ofMillis(10).toNanos();
    private static final boolean MAY_BE_BLOCKING = maybeBlocking();
    private static final long JVM_MAX_DIRECT_MEMORY = PlatformDependent.estimateMaxDirectMemory();
    private static final ThreadLocal<MemUsage> THREAD_LOCAL = ThreadLocal.withInitial(MemUsage::new);

    public static MemUsage local() {
        return THREAD_LOCAL.get();
    }

    private final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    private double nettyDirectMemoryUsage = 0;
    private double heapMemoryUsage = 0;
    private long refreshNettyDirectMemoryUsageAt = 0;
    private long refreshHeapMemoryUsageAt = 0;

    public double nettyDirectMemoryUsage() {
        scheduleNettyDirectMemoryUsage();
        return nettyDirectMemoryUsage;
    }

    public double heapMemoryUsage() {
        scheduleHeapMemoryUsage();
        return heapMemoryUsage;
    }

    private void scheduleNettyDirectMemoryUsage() {
        long now = System.nanoTime();
        if (now - refreshNettyDirectMemoryUsageAt > UPDATE_INTERVAL) {
            if (MAY_BE_BLOCKING && nettyDirectMemoryUsage > INLINE_REFRESH_THRESHOLD) {
                // if netty direct memory usage is high, reading pool allocator stats directly may be blocking
                // we read it in a separate thread
                nettyDirectMemoryUsage = NonblockingNettyDirectMemoryUsage.usage();
            } else {
                // calculating inline
                nettyDirectMemoryUsage = calculateNettyDirectMemoryUsage();
            }
            refreshNettyDirectMemoryUsageAt = System.nanoTime();
        }
    }

    private static boolean maybeBlocking() {
        return !PlatformDependent.useDirectBufferNoCleaner() && ByteBufAllocator.DEFAULT.isDirectBufferPooled();
    }

    private void scheduleHeapMemoryUsage() {
        long now = System.nanoTime();
        if (now - refreshHeapMemoryUsageAt > UPDATE_INTERVAL) {
            heapMemoryUsage = calculateHeapMemoryUsage();
            refreshHeapMemoryUsageAt = System.nanoTime();
        }
    }

    private static double calculateNettyDirectMemoryUsage() {
        if (PlatformDependent.useDirectBufferNoCleaner()) {
            if (ByteBufAllocator.DEFAULT.isDirectBufferPooled()) {
                long pooledDirectMemory = PlatformDependent.usedDirectMemory();
                double pooledDirectMemoryUsage = pooledDirectMemoryUsage();
                double jvmDirectMemoryUsage = pooledDirectMemory / (double) JVM_MAX_DIRECT_MEMORY;
                return Math.min(pooledDirectMemoryUsage, jvmDirectMemoryUsage);
            } else {
                return (PlatformDependent.usedDirectMemory()) / (double) PlatformDependent.maxDirectMemory();
            }
        } else {
            ByteBufAllocatorMetric allocatorMetric =
                ((ByteBufAllocatorMetricProvider) ByteBufAllocator.DEFAULT).metric();
            long usedDirectMemory = allocatorMetric.usedDirectMemory();
            if (ByteBufAllocator.DEFAULT.isDirectBufferPooled()) {
                double pooledDirectMemoryUsage = pooledDirectMemoryUsage();
                double jvmDirectMemoryUsage = usedDirectMemory / (double) JVM_MAX_DIRECT_MEMORY;
                return Math.min(pooledDirectMemoryUsage, jvmDirectMemoryUsage);
            } else {
                return usedDirectMemory / (double) Math.min(PlatformDependent.maxDirectMemory(), JVM_MAX_DIRECT_MEMORY);
            }
        }
    }

    private double calculateHeapMemoryUsage() {
        try {
            MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();
            long usedHeapMemory = memoryUsage.getUsed();
            long maxHeapMemory = memoryUsage.getMax();
            return (double) usedHeapMemory / maxHeapMemory;
        } catch (IllegalArgumentException e) {
            // there is an unresolved issue in open jdk17: https://bugs.openjdk.org/browse/JDK-8207200
            return 0;
        }
    }

    private static double pooledDirectMemoryUsage() {
        PooledByteBufAllocatorMetric allocatorMetric =
            (PooledByteBufAllocatorMetric) ((ByteBufAllocatorMetricProvider) ByteBufAllocator.DEFAULT).metric();

        int[] buckets = new int[101];
        int totalChunks = 0;

        for (PoolArenaMetric arenaMetric : allocatorMetric.directArenas()) {
            totalChunks += collectChunkUsages(arenaMetric, buckets);
        }

        return calculatePercentile(buckets, totalChunks, 90) / 100.0;
    }

    private static int collectChunkUsages(PoolArenaMetric arenaMetric, int[] buckets) {
        int totalChunks = 0;
        for (PoolChunkListMetric chunkListMetric : arenaMetric.chunkLists()) {
            for (PoolChunkMetric chunkMetric : chunkListMetric) {
                int usage = chunkMetric.usage();
                buckets[usage]++;
                totalChunks++;
            }
        }
        return totalChunks;
    }

    private static double calculatePercentile(int[] buckets, int totalChunks, double percentile) {
        if (totalChunks == 0) {
            return 0;
        }
        int threshold = (int) Math.ceil(percentile / 100.0 * totalChunks);
        int cumulativeCount = 0;
        for (int i = 0; i < buckets.length; i++) {
            cumulativeCount += buckets[i];
            if (cumulativeCount >= threshold) {
                return i;
            }
        }
        return 100;
    }

    private static double maxPooledMemoryUsage() {
        PooledByteBufAllocatorMetric allocatorMetric =
            (PooledByteBufAllocatorMetric) ((ByteBufAllocatorMetricProvider) ByteBufAllocator.DEFAULT).metric();
        int maxUsage = 0;
        for (PoolArenaMetric arenaMetric : allocatorMetric.directArenas()) {
            maxUsage = Math.max(maxUsage, pooledArenaUsage(arenaMetric));
        }
        return maxUsage / 100.0;
    }

    private static int pooledArenaUsage(PoolArenaMetric arenaMetric) {
        int maxUsage = 0;
        for (PoolChunkListMetric chunkListMetric : arenaMetric.chunkLists()) {
            for (PoolChunkMetric chunkMetric : chunkListMetric) {
                maxUsage = Math.max(chunkMetric.usage(), maxUsage);
            }
        }
        return maxUsage;
    }

    private static long calculateTotalFreeBytes() {
        PooledByteBufAllocatorMetric allocatorMetric =
            (PooledByteBufAllocatorMetric) ((ByteBufAllocatorMetricProvider) ByteBufAllocator.DEFAULT).metric();
        long totalFreeBytes = 0;
        for (PoolArenaMetric arenaMetric : allocatorMetric.directArenas()) {
            totalFreeBytes += getFreeBytesFromArena(arenaMetric);
        }
        return totalFreeBytes;
    }

    private static long getFreeBytesFromArena(PoolArenaMetric arenaMetric) {
        long freeBytes = 0;
        for (PoolChunkListMetric chunkListMetric : arenaMetric.chunkLists()) {
            for (PoolChunkMetric chunkMetric : chunkListMetric) {
                freeBytes += chunkMetric.freeBytes();
            }
        }
        return freeBytes;
    }

    private static class NonblockingNettyDirectMemoryUsage {
        private static final Executor executor =
            newSingleThreadExecutor(EnvProvider.INSTANCE.newThreadFactory("netty-pool-usage-reader", true));
        private static final AtomicBoolean isRefreshing = new AtomicBoolean(false);
        private static volatile double nettyDirectMemoryUsage = 0;

        static double usage() {
            scheduleRefresh();
            return nettyDirectMemoryUsage;
        }

        static void scheduleRefresh() {
            if (isRefreshing.compareAndSet(false, true)) {
                executor.execute(() -> {
                    nettyDirectMemoryUsage = MemUsage.calculateNettyDirectMemoryUsage();
                    isRefreshing.set(false);
                });
            }
        }
    }
}
