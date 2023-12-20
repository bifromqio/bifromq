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

package com.baidu.bifromq.sysprops;

import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class BifroMQSysPropTest {
    @Test
    public void defaultValue() {
        for (BifroMQSysProp prop : BifroMQSysProp.values()) {
            assertTrue(prop.get().equals(prop.defVal()));
        }
    }

    @Test
    public void emptyPropFallbackToDefaultValue() {
        for (BifroMQSysProp prop : BifroMQSysProp.values()) {
            System.setProperty(prop.propKey, "");
            assertTrue(prop.get().equals(prop.defVal()));
        }
    }

    @Test
    public void unparseablePropFallbackToDefaultValue() {
        for (BifroMQSysProp prop : BifroMQSysProp.values()) {
            if (!(prop.defVal() instanceof String)) {
                System.setProperty(prop.propKey, "bad prop");
                assertTrue(prop.get().equals(prop.defVal()));
            }
        }
    }
}
