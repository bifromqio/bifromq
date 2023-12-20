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

package com.baidu.bifromq.mqtt.utils;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.UUID;
import org.testng.annotations.Test;

public class MQTTUtf8UtilTest {
    @Test
    public void clientId() {
        for (int i = 0; i < 100; i++) {
            String s = UUID.randomUUID().toString();
            assertTrue(MQTTUtf8Util.isWellFormed(s, false));
        }
    }

    @Test
    public void emptyClientId() {
        assertTrue(MQTTUtf8Util.isWellFormed("", false));
        assertTrue(MQTTUtf8Util.isWellFormed(" ", false)); // whitespace is acceptable according to MQTT spec
    }

    @Test
    public void mustNotChars() {
        assertFalse(MQTTUtf8Util.isWellFormed("hello\u0000world", false)); // null character U+0000
        assertFalse(MQTTUtf8Util.isWellFormed("hello\uD83D\uDE0Aworld", false)); // surrogate pairs
    }

    @Test
    public void shouldNotChars() {
        for (int i = '\u0001'; i <= '\u001F'; i++) {
            assertFalse(MQTTUtf8Util.isWellFormed("hello" + (char) i, true)); // control character
        }
        for (int i = '\u007F'; i <= '\u009F'; i++) {
            assertFalse(MQTTUtf8Util.isWellFormed("hello" + (char) i, true)); // control character
        }
        assertFalse(MQTTUtf8Util.isWellFormed("hello\uFFFF", true)); // non character
    }

    @Test
    public void noSanityCheck() {
        for (int i = '\u0001'; i <= '\u001F'; i++) {
            assertTrue(MQTTUtf8Util.isWellFormed("hello" + (char) i, false)); // control character
        }
        for (int i = '\u007F'; i <= '\u009F'; i++) {
            assertTrue(MQTTUtf8Util.isWellFormed("hello" + (char) i, false)); // control character
        }
        assertTrue(MQTTUtf8Util.isWellFormed("hello\uFFFF", false)); // non character
    }
}
