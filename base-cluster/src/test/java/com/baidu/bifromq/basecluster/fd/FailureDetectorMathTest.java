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

package com.baidu.bifromq.basecluster.fd;

import static com.baidu.bifromq.basecluster.fd.FailureDetectorMath.scale;
import static org.testng.AssertJUnit.assertEquals;

import java.time.Duration;
import org.testng.annotations.Test;

public class FailureDetectorMathTest {
    @Test
    public void scaleDuration() {
        assertEquals(Duration.ofMillis(100), scale(Duration.ofMillis(100), -1));
        assertEquals(Duration.ofMillis(100), scale(Duration.ofMillis(100), 0));
        assertEquals(Duration.ofMillis(200), scale(Duration.ofMillis(100), 1));
    }
}
