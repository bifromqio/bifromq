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

package com.baidu.bifromq.dist.server;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class RunningAverageTest {
    @Test
    public void test() {
        RunningAverage runningAverage = new RunningAverage(1);
        assertEquals(runningAverage.estimate(), 0);

        runningAverage.log(1);
        assertEquals(runningAverage.estimate(), 1);

        runningAverage.log(2);
        assertEquals(runningAverage.estimate(), 2);

        runningAverage = new RunningAverage(2);
        runningAverage.log(1);
        runningAverage.log(3);
        assertEquals(runningAverage.estimate(), 2);
    }
}
