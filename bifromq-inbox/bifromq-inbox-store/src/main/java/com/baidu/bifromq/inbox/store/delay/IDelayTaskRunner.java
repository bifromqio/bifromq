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

import com.baidu.bifromq.inbox.storage.proto.Replica;
import java.util.function.Supplier;

/**
 * The interface of DelayTask Runner which is used to schedule tasks to be executed after a specified delay.
 *
 * @param <KeyT> the type of the key used to identify the task.
 */
public interface IDelayTaskRunner<KeyT> {
    /**
     * The owner replica of this runner.
     *
     * @return the owner replica
     */
    Replica owner();

    /**
     * Schedule a new task or postpone existing task of same type to be triggered after the specified delay. If there is
     * a task registered with the same key, and of different type. The previous task will be cancelled and a new task
     * will be scheduled.
     *
     * @param key       the key under monitoring
     * @param supplier  the task to be triggered
     * @param taskClass the class of the task to be triggered
     * @param <TaskT>   the type of the task
     */
    <TaskT extends IDelayedTask<KeyT>> void reschedule(KeyT key, Supplier<TaskT> supplier, Class<TaskT> taskClass);

    /**
     * Schedule a new delayedTask.
     *
     * @param key         the key under monitoring
     * @param delayedTask the delayedTask to be triggered
     * @param <TaskT>     the type of the delayedTask
     */
    <TaskT extends IDelayedTask<KeyT>> void schedule(KeyT key, TaskT delayedTask);

    /**
     * Schedule a new task to be triggered after the specified delay.
     *
     * @param key      the key under monitoring
     * @param supplier the task to be triggered
     * @param <TaskT>  the type of the task
     */
    <TaskT extends IDelayedTask<KeyT>> void scheduleIfAbsent(KeyT key, Supplier<TaskT> supplier);
}
