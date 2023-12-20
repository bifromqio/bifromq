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

package com.baidu.bifromq.basekv.store.util;

import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.basekv.MockableTest;
import org.testng.annotations.Test;

public class VerUtilTest extends MockableTest {
    @Test
    public void bumpTest() {
        assertEquals(VerUtil.bump(0, false), 2);
        assertEquals(VerUtil.bump(1, false), 2);

        assertEquals(VerUtil.bump(0, true), 1);
        assertEquals(VerUtil.bump(1, true), 3);
    }
}
