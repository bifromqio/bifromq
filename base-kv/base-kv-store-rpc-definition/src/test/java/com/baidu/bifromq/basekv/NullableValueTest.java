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

package com.baidu.bifromq.basekv;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.baidu.bifromq.basekv.proto.NullableValue;
import com.google.protobuf.ByteString;
import org.junit.Test;

public class NullableValueTest {
    @Test
    public void testNullable() {
        NullableValue nullable = NullableValue.newBuilder().build();
        assertFalse(nullable.hasValue());
        assertEquals(ByteString.EMPTY, nullable.getValue());
        nullable = NullableValue.newBuilder().setValue(ByteString.EMPTY).build();
        assertTrue(nullable.hasValue());
    }
}
