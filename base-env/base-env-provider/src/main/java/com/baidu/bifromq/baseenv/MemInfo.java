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
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class MemInfo {
    public record MemUsage(double nettyDirectMemoryUsage, double heapMemoryUsage) {

    }

    private static final long UPDATE_INTERVAL = Duration.ofMillis(100).toNanos();
    private static final Executor executor =
        Executors.newSingleThreadExecutor(EnvProvider.INSTANCE.newThreadFactory("memusage-reader", true));
    private static final AtomicBoolean isCalculating = new AtomicBoolean(false);
    private static final long JVM_MAX_DIRECT_MEMORY = PlatformDependent.estimateMaxDirectMemory();
    private static final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    private static volatile MemUsage usage = new MemUsage(0, 0);
    private static volatile long refreshAt = 0;

    public static MemUsage usage() {
        scheduleUpdate();
        return usage;
    }

    public static double nettyDirectMemoryUsage() {
        scheduleUpdate();
        return usage.nettyDirectMemoryUsage();
    }

    public static double heapMemoryUsage() {
        scheduleUpdate();
        return usage.heapMemoryUsage();
    }

    private static void scheduleUpdate() {
        if (isCalculating.compareAndSet(false, true)) {
            executor.execute(() -> {
                refresh();
                isCalculating.set(false);
            });
        }
    }

    private static void refresh() {
        long now = System.nanoTime();
        if (now - refreshAt > UPDATE_INTERVAL) {
            usage = new MemUsage(calculateDirectMemoryUsage(), calculateHeapMemoryUsage());
            refreshAt = now;
        }
    }

    private static double calculateDirectMemoryUsage() {
        if (PlatformDependent.useDirectBufferNoCleaner()) {
            if (ByteBufAllocator.DEFAULT.isDirectBufferPooled()) {
                long pooledDirectMemory = PlatformDependent.usedDirectMemory();
                long freeDirectMemoryInPool = calculateTotalFreeBytes();
                double pooledDirectMemoryUsage =
                    (pooledDirectMemory - freeDirectMemoryInPool) / (double) pooledDirectMemory;
                double jvmDirectMemoryUsage = pooledDirectMemory / (double) JVM_MAX_DIRECT_MEMORY;
                return Math.min(pooledDirectMemoryUsage, jvmDirectMemoryUsage);
            } else {
                return (PlatformDependent.usedDirectMemory())
                    / (double) PlatformDependent.maxDirectMemory();
            }
        } else {
            ByteBufAllocatorMetric allocatorMetric =
                ((ByteBufAllocatorMetricProvider) ByteBufAllocator.DEFAULT).metric();
            long usedDirectMemory = allocatorMetric.usedDirectMemory();
            if (ByteBufAllocator.DEFAULT.isDirectBufferPooled()) {
                long freeDirectMemoryInPool = calculateTotalFreeBytes();
                double pooledDirectMemoryUsage =
                    (usedDirectMemory - freeDirectMemoryInPool) / (double) usedDirectMemory;
                double jvmDirectMemoryUsage = usedDirectMemory / (double) JVM_MAX_DIRECT_MEMORY;
                return Math.min(pooledDirectMemoryUsage, jvmDirectMemoryUsage);
            } else {
                return usedDirectMemory / (double) Math.min(PlatformDependent.maxDirectMemory(), JVM_MAX_DIRECT_MEMORY);
            }
        }
    }

    private static double calculateHeapMemoryUsage() {
        MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();
        long usedHeapMemory = memoryUsage.getUsed();
        long maxHeapMemory = memoryUsage.getMax();
        return (double) usedHeapMemory / maxHeapMemory;
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
}
