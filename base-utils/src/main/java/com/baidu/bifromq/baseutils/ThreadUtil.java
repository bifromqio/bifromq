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

package com.baidu.bifromq.baseutils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ThreadUtil {
    public static UncaughtExceptionHandler uncaughtExceptionHandler() {
        return (t, e) -> log.error("Caught an exception in {}", t, e);
    }

    public static ThreadFactory threadFactory(String nameFormat) {
        return threadFactory(nameFormat, false);
    }

    public static ThreadFactory threadFactory(String nameFormat, boolean daemon) {
        return new ThreadFactoryBuilder()
            .setNameFormat(nameFormat)
            .setUncaughtExceptionHandler(uncaughtExceptionHandler())
            .setDaemon(daemon)
            .build();
    }

    public static ForkJoinWorkerThreadFactory forkJoinThreadFactory(String nameFormat) {
        return forkJoinThreadFactory(nameFormat, false);
    }

    public static ForkJoinWorkerThreadFactory forkJoinThreadFactory(String nameFormat, boolean daemon) {
        return new ForkJoinWorkerThreadFactory() {
            final AtomicInteger index = new AtomicInteger(0);

            @Override
            public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
                ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                worker.setName(String.format(nameFormat, index.incrementAndGet()));
                worker.setDaemon(daemon);
                return worker;
            }
        };
    }

}
