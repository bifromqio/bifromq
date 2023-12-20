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

package com.baidu.bifromq.basescheduler;

import java.util.concurrent.CompletableFuture;

/**
 * A call scheduler is used for scheduling call before actually being invoked
 *
 * @param <Req> the request type
 */
public interface ICallScheduler<Req> {
    /**
     * Schedule a call to be called in the future when the turned future completed.
     * The returned future could be completed with DropException
     *
     * @param request the request to be scheduled
     * @return the future of the request to be invoked
     */
    default CompletableFuture<Req> submit(Req request) {
        return CompletableFuture.completedFuture(request);
    }

    /**
     * Close the scheduler
     */
    default void close() {
    }
}
