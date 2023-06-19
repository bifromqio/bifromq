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

package com.baidu.bifromq.basekv.localengine;

import static com.google.protobuf.ByteString.EMPTY;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.protobuf.ByteString;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RangeUtilTest {

    @Test
    public void rangeValidation() {
        assertTrue(RangeUtil.isValid(null, null));
        assertTrue(RangeUtil.isValid(null, ByteString.EMPTY));
        assertTrue(RangeUtil.isValid(ByteString.EMPTY, null));
        assertTrue(RangeUtil.isValid(ByteString.EMPTY, ByteString.EMPTY));

        assertTrue(RangeUtil.isValid(ByteString.EMPTY, copyFromUtf8("a")));

        assertFalse(RangeUtil.isValid(copyFromUtf8("a"), ByteString.EMPTY));
        assertFalse(RangeUtil.isValid(copyFromUtf8("b"), copyFromUtf8("a")));
    }

    @Test
    public void keyInRange() {
        assertTrue(RangeUtil.inRange(copyFromUtf8("a"), null, null));

        assertTrue(RangeUtil.inRange(copyFromUtf8("a"), copyFromUtf8("a"), null));

        assertFalse(RangeUtil.inRange(copyFromUtf8("a"), copyFromUtf8("b"), null));

        assertFalse(RangeUtil.inRange(copyFromUtf8("a"), null, copyFromUtf8("a")));

        assertFalse(RangeUtil.inRange(copyFromUtf8("a"), copyFromUtf8("b"), copyFromUtf8("c")));

        assertTrue(RangeUtil.inRange(copyFromUtf8("b"), copyFromUtf8("a"), copyFromUtf8("c")));

        assertFalse(RangeUtil.inRange(copyFromUtf8("d"), copyFromUtf8("b"), copyFromUtf8("c")));

        try {
            assertFalse(RangeUtil.inRange(copyFromUtf8("b"), copyFromUtf8("c"), copyFromUtf8("a")));
            fail();
        } catch (AssertionError error) {

        }
    }

    @Test
    public void rangeInRange() {

        assertTrue(RangeUtil.inRange(copyFromUtf8("a"), copyFromUtf8("b"), null, null));
        assertTrue(RangeUtil.inRange(null, EMPTY, null, null));

        assertTrue(RangeUtil.inRange(copyFromUtf8("a"), copyFromUtf8("b"), copyFromUtf8("a"), null));
        assertTrue(RangeUtil.inRange(null, EMPTY, copyFromUtf8("a"), null));

        assertTrue(RangeUtil.inRange(copyFromUtf8("a"), copyFromUtf8("b"), null, copyFromUtf8("b")));
        assertTrue(RangeUtil.inRange(null, EMPTY, null, copyFromUtf8("b")));

        assertTrue(RangeUtil.inRange(copyFromUtf8("a"), copyFromUtf8("b"), copyFromUtf8("a"), copyFromUtf8("c")));
        assertTrue(RangeUtil.inRange(null, EMPTY, copyFromUtf8("a"), copyFromUtf8("c")));

        assertTrue(RangeUtil.inRange(copyFromUtf8("b"), copyFromUtf8("c"), copyFromUtf8("a"), copyFromUtf8("c")));

        assertFalse(RangeUtil.inRange(copyFromUtf8("a"), copyFromUtf8("c"), copyFromUtf8("a"), copyFromUtf8("b")));

        assertFalse(RangeUtil.inRange(copyFromUtf8("a"), copyFromUtf8("c"), copyFromUtf8("b"), copyFromUtf8("d")));
    }

    @Test
    public void findUpperBound() {
        Assert.assertEquals(ByteString.copyFrom(new byte[] {1, 2, 4}), RangeUtil.upperBound(ByteString.copyFrom(new byte[] {1, 2, 3})));
        Assert.assertEquals(ByteString.copyFrom(new byte[] {1, 3, (byte) 0xFF}),
            RangeUtil.upperBound(ByteString.copyFrom(new byte[] {1, 2, (byte) 0xFF})));
        Assert.assertEquals(ByteString.copyFrom(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF}),
            RangeUtil.upperBound(ByteString.copyFrom(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF})));
    }
}
