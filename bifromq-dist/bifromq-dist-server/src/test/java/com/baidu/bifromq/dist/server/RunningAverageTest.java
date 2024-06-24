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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RunningAverageTest {
    private RunningAverage avg;

    @BeforeMethod
    public void setUp() {
        avg = new RunningAverage(5); // Example window size of 5 for testing
    }

    @Test
    public void testInitialEstimate() {
        assertEquals(0, avg.estimate());
    }

    @Test
    public void testSingleValue() {
        avg.log(10);
        assertEquals(10, avg.estimate());
    }

    @Test
    public void testWindowSizeReached() {
        int[] values = {1, 2, 3, 4, 5};
        for (int value : values) {
            avg.log(value);
        }
        assertEquals(3, avg.estimate());
    }

    @Test
    public void testWindowSizeExceeded() {
        int[] values = {1, 2, 3, 4, 5, 6};
        for (int value : values) {
            avg.log(value);
        }
        assertEquals(4, avg.estimate());
    }

    @Test
    public void testHighValueInput() {
        avg.log(Integer.MAX_VALUE);
        avg.log(Integer.MAX_VALUE);
        avg.log(Integer.MAX_VALUE);
        avg.log(Integer.MAX_VALUE);
        avg.log(Integer.MAX_VALUE);
        long expected = Integer.MAX_VALUE;
        assertEquals(expected, avg.estimate());
    }

    @Test
    public void testVaryingValues() {
        avg.log(10);
        avg.log(20);
        avg.log(30);
        avg.log(40);
        avg.log(50);
        avg.log(60);  // Should drop the first (10)
        assertEquals(40, avg.estimate());
    }

    @Test(expectedExceptions = AssertionError.class)
    public void testZeroWindowSize() {
        new RunningAverage(0);
    }

    @Test
    public void testNegativeValues() {
        avg.log(-5);
        avg.log(-10);
        avg.log(-15);
        avg.log(-20);
        avg.log(-25);
        assertEquals(-15, avg.estimate());
    }
}
