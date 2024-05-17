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

package com.baidu.bifromq.baserpc.utils;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.internal.PlatformDependent;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.time.Duration;

public class MemInfo {
    private static final long READ_GAP_NANOS = Duration.ofMillis(100).toNanos();
    private static volatile long lastDirectUsageReadNanos;
    private static volatile long lastHeapUsageReadNanos;
    private static volatile double directMemoryUsage;
    private static volatile double heapMemoryUsage;

    public static double directMemoryUsage() {
        long now = System.nanoTime();
        if ((now - lastDirectUsageReadNanos) > READ_GAP_NANOS) {
            directMemoryUsage = PooledByteBufAllocator.DEFAULT.metric().usedDirectMemory() /
                (double) PlatformDependent.maxDirectMemory();
            lastDirectUsageReadNanos = now;
        }
        return directMemoryUsage;
    }

    public static double heapMemoryUsage() {
        long now = System.nanoTime();
        if ((now - lastHeapUsageReadNanos) > READ_GAP_NANOS) {
            MemoryUsage memoryUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
            long usedHeapMemory = memoryUsage.getUsed();
            long maxHeapMemory = memoryUsage.getMax();
            heapMemoryUsage = (double) usedHeapMemory / maxHeapMemory;
            lastHeapUsageReadNanos = now;
        }
        return heapMemoryUsage;
    }
}
