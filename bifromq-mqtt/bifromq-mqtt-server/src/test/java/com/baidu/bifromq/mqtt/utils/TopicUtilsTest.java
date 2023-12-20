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

import org.testng.Assert;
import org.testng.annotations.Test;

public class TopicUtilsTest {

    @Test
    public void testIsValidTopic() {
        assertTrue(TopicUtil.isValidTopic("/", 40, 16, 255));
        assertTrue(TopicUtil.isValidTopic("//", 40, 16, 255));
        assertFalse(TopicUtil.isValidTopic("", 40, 16, 255));
        assertTrue(TopicUtil.isValidTopic(" ", 40, 16, 255));
        assertTrue(TopicUtil.isValidTopic("/ ", 40, 16, 255));
        assertTrue(TopicUtil.isValidTopic("/ /", 40, 16, 255));
        assertFalse(TopicUtil.isValidTopic("/\u0000abc/", 40, 16, 255));
        assertFalse(TopicUtil.isValidTopic("/abc/", 2, 16, 255));
        assertTrue(TopicUtil.isValidTopic("abc", 4, 1, 255));
        assertFalse(TopicUtil.isValidTopic("/", 4, 1, 255));
        assertTrue(TopicUtil.isValidTopic("/", 4, 2, 255));
        assertFalse(TopicUtil.isValidTopic("/abcde/fghij", 5, 4, 10));
        assertFalse(TopicUtil.isValidTopic("/+/#", 5, 4, 10));

        assertFalse(TopicUtil.isValidTopic("$share/a/", 5, 4, 10));
        assertFalse(TopicUtil.isValidTopic("$share/a//", 5, 4, 10));

        assertTrue(TopicUtil.isValidTopic("$share", 10, 4, 20));
        assertTrue(TopicUtil.isValidTopic("$shared/a//", 10, 4, 20));
    }

    @Test
    public void testIsValidTopicFilter() {
        assertTrue(TopicUtil.isValidTopicFilter("/", 40, 16, 255));
        assertTrue(TopicUtil.isValidTopicFilter("//", 40, 16, 255));
        assertFalse(TopicUtil.isValidTopicFilter("", 40, 16, 255));
        assertTrue(TopicUtil.isValidTopicFilter(" ", 40, 16, 255));
        assertTrue(TopicUtil.isValidTopicFilter("/ ", 40, 16, 255));
        assertTrue(TopicUtil.isValidTopicFilter("/ /", 40, 16, 255));
        assertFalse(TopicUtil.isValidTopicFilter("/\u0000abc/", 40, 16, 255));
        assertFalse(TopicUtil.isValidTopicFilter("/abc/", 2, 16, 255));
        assertTrue(TopicUtil.isValidTopicFilter("abc", 4, 1, 255));
        assertFalse(TopicUtil.isValidTopicFilter("/", 4, 1, 255));
        assertTrue(TopicUtil.isValidTopicFilter("/", 4, 2, 255));
        assertFalse(TopicUtil.isValidTopicFilter("/abcde/fghij", 5, 4, 10));

        assertTrue(TopicUtil.isValidTopicFilter("#", 40, 16, 255));
        assertTrue(TopicUtil.isValidTopicFilter("a/#", 40, 16, 255));
        assertTrue(TopicUtil.isValidTopicFilter("+", 40, 16, 255));
        assertTrue(TopicUtil.isValidTopicFilter("+/", 40, 16, 255));
        assertTrue(TopicUtil.isValidTopicFilter("+/+", 40, 16, 255));
        assertTrue(TopicUtil.isValidTopicFilter("/+/#", 40, 16, 255));
        assertTrue(TopicUtil.isValidTopicFilter("+/a/#", 40, 16, 255));

        assertFalse(TopicUtil.isValidTopicFilter("#a", 40, 16, 255));
        assertFalse(TopicUtil.isValidTopicFilter("#/a", 40, 16, 255));
        assertFalse(TopicUtil.isValidTopicFilter("/a#", 40, 16, 255));
        assertFalse(TopicUtil.isValidTopicFilter("/a#a", 40, 16, 255));
        assertFalse(TopicUtil.isValidTopicFilter("/a+/", 40, 16, 255));
        assertFalse(TopicUtil.isValidTopicFilter("/+a/", 40, 16, 255));
        assertFalse(TopicUtil.isValidTopicFilter("/a+a/", 40, 16, 255));
        assertFalse(TopicUtil.isValidTopicFilter("a+", 40, 16, 255));
        assertFalse(TopicUtil.isValidTopicFilter("+a", 40, 16, 255));
        assertFalse(TopicUtil.isValidTopicFilter("/a/+#", 40, 16, 255));


        assertFalse(TopicUtil.isValidTopicFilter("$share/", 5, 4, 10));
        assertFalse(TopicUtil.isValidTopicFilter("$share/a", 5, 4, 10));
        assertFalse(TopicUtil.isValidTopicFilter("$share/\u0000/", 5, 4, 10));
        assertFalse(TopicUtil.isValidTopicFilter("$share//", 5, 4, 10));

        assertFalse(TopicUtil.isValidTopicFilter("$oshare/", 5, 4, 10));
        assertFalse(TopicUtil.isValidTopicFilter("$oshare//", 5, 4, 10));
        assertFalse(TopicUtil.isValidTopicFilter("$oshare/a", 5, 4, 10));
        assertFalse(TopicUtil.isValidTopicFilter("$oshare/\u0000/", 5, 4, 10));

        assertTrue(TopicUtil.isValidTopicFilter("$share", 10, 4, 100));
        assertTrue(TopicUtil.isValidTopicFilter("$oshare", 10, 4, 100));
        assertTrue(TopicUtil.isValidTopicFilter("$shared/", 10, 4, 10));
        assertTrue(TopicUtil.isValidTopicFilter("$oshared/", 10, 4, 100));
        assertTrue(TopicUtil.isValidTopicFilter("$share/g/", 10, 4, 100));
        assertTrue(TopicUtil.isValidTopicFilter("$share/g//", 10, 4, 100));
        assertFalse(TopicUtil.isValidTopicFilter("$share/g/abcdef/", 5, 4, 10));
        assertFalse(TopicUtil.isValidTopicFilter("$share/g/1/2/3/4/5", 5, 4, 255));
        assertFalse(TopicUtil.isValidTopicFilter("$share/g//1/2/3/4", 5, 4, 255));
        assertFalse(TopicUtil.isValidTopicFilter("$share/g//1/2/3/", 5, 4, 255));

        assertTrue(TopicUtil.isValidTopicFilter("$share/g/+/a", 10, 4, 100));
        assertTrue(TopicUtil.isValidTopicFilter("$share/g/#", 10, 4, 100));
        assertTrue(TopicUtil.isValidTopicFilter("$share/g//#", 10, 4, 100));
        assertTrue(TopicUtil.isValidTopicFilter("$share/g//+/a/#", 10, 4, 100));

        assertFalse(TopicUtil.isValidTopicFilter("$share/g//a+", 10, 4, 100));
        assertFalse(TopicUtil.isValidTopicFilter("$share/g/+a", 10, 4, 100));
        assertFalse(TopicUtil.isValidTopicFilter("$share/g/#/a", 10, 4, 100));

    }

    @Test
    public void testParseTopicFilter() {
        Assert.assertEquals("/a/b/c", TopicUtil.parseTopicFilter("$share/g1//a/b/c"));
        Assert.assertEquals("/a/b/c", TopicUtil.parseTopicFilter("$oshare/g1//a/b/c"));
        Assert.assertEquals("a/b/c", TopicUtil.parseTopicFilter("$share/g1/a/b/c"));
        Assert.assertEquals("a/b/c", TopicUtil.parseTopicFilter("$oshare/g1/a/b/c"));
        Assert.assertEquals("/a/b/c", TopicUtil.parseTopicFilter("/a/b/c"));
        Assert.assertEquals("$share", TopicUtil.parseTopicFilter("$share"));
        Assert.assertEquals("$oshare", TopicUtil.parseTopicFilter("$oshare"));
        Assert.assertEquals("$oshared/a", TopicUtil.parseTopicFilter("$oshared/a"));
    }
}
