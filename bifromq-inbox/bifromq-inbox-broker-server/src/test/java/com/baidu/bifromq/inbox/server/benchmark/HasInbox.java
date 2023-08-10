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

package com.baidu.bifromq.inbox.server.benchmark;

import com.baidu.bifromq.plugin.subbroker.CheckResult;
import java.util.concurrent.ThreadLocalRandom;
import lombok.SneakyThrows;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class HasInbox {

    @SneakyThrows
    public static void main(String[] args) {
        Options opt = new OptionsBuilder()
            .include(HasInbox.class.getSimpleName())
            .build();
        new Runner(opt).run();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Warmup(iterations = 3)
    @Measurement(iterations = 8)
    @Threads(4)
    @Fork(1)
    public void testHasInbox(HasInboxState state, Blackhole blackhole) {
        String tenantId = "DevOnly";
        String inboxId = "Inbox_" + ThreadLocalRandom.current().nextInt();
        long reqId = System.nanoTime();
        CheckResult reply = state.inboxBrokerClient.hasInbox(reqId, tenantId, inboxId, null).join();
        blackhole.consume(reply);
    }
}
