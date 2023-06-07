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

package com.baidu.bifromq.baserpc.utils;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public final class FutureTracker {
    private final Set<CompletableFuture<?>> track = ConcurrentHashMap.newKeySet();

    public <T> CompletableFuture<T> track(CompletableFuture<T> trackedFuture) {
        track.add(trackedFuture);
        trackedFuture.whenComplete((v, e) -> track.remove(trackedFuture));
        return trackedFuture;
    }

    public <T> CompletableFuture<T> track(Supplier<CompletableFuture<T>> futureSupplier) {
        return track(futureSupplier.get());
    }

    public void stop() {
        for (CompletableFuture<?> tracked : track) {
            tracked.cancel(true);
        }
    }

    public CompletableFuture<Void> whenComplete(BiConsumer<Void, Throwable> biConsumer) {
        return CompletableFuture.allOf(track.toArray(new CompletableFuture[0]))
            .whenComplete(biConsumer);
    }

    public CompletableFuture<Void> whenCompleteAsync(BiConsumer<Void, Throwable> biConsumer, Executor executor) {
        return CompletableFuture.allOf(track.toArray(new CompletableFuture[0]))
            .whenCompleteAsync(biConsumer, executor);
    }
}
