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

package com.baidu.bifromq.basecrdt.core.benchmark;

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import com.baidu.bifromq.basecrdt.core.api.CRDTEngineOptions;
import com.baidu.bifromq.basecrdt.core.api.ICRDTEngine;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import lombok.extern.slf4j.Slf4j;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@Slf4j
public abstract class CRDTBenchmarkTemplate {
    protected ICRDTEngine engine;
    private TemporaryFolder dbRootDir = new TemporaryFolder();

    @Setup
    public void setup() {
        try {
            dbRootDir.create();
        } catch (IOException e) {
        }

        engine = ICRDTEngine.newInstance(new CRDTEngineOptions());
        engine.start();
        dbRootDir = new TemporaryFolder();
        doSetup();
    }

    @TearDown
    public void tearDown() {
        log.info("Stop engine");
        engine.stop();
        dbRootDir.delete();
    }

    protected abstract void doSetup();

    @Test
    @Ignore
    public void test() {
        Options opt = new OptionsBuilder()
            .include(this.getClass().getSimpleName())
            .warmupIterations(0)
            .measurementIterations(100)
            .forks(1)
            .build();
        try {
            new Runner(opt).run();
        } catch (RunnerException e) {
            throw new RuntimeException(e);
        }
    }

    protected ByteString toByteString(long l) {
        return unsafeWrap(ByteBuffer.allocate(Long.BYTES).putLong(l).array());
    }
}
