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

package com.baidu.bifromq.dist.util;


import static com.baidu.bifromq.dist.util.TopicUtil.escape;
import static com.baidu.bifromq.dist.util.TopicUtil.isWildcardTopicFilter;
import static com.baidu.bifromq.dist.util.TopicUtil.parse;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class TopicUtilTest {
    @Test
    public void check() {
        assertTrue(isWildcardTopicFilter("#"));
        assertTrue(isWildcardTopicFilter("+"));
        assertTrue(isWildcardTopicFilter("/#"));
        assertTrue(isWildcardTopicFilter("/+"));
        assertFalse(isWildcardTopicFilter("/"));
    }

    @Test
    public void testParse() {
        assertEquals(parse("", false), Lists.newArrayList(""));
        assertEquals(parse(" ", false), Lists.newArrayList(" "));
        assertEquals(parse("/", false), Lists.newArrayList("", ""));
        assertEquals(parse(escape("/"), true), Lists.newArrayList("", ""));
        assertEquals(parse("//", false), Lists.newArrayList("", "", ""));
        assertEquals(parse(escape("//"), true), Lists.newArrayList("", "", ""));
        assertEquals(parse(" //", false), Lists.newArrayList(" ", "", ""));
        assertEquals(parse(escape(" //"), true), Lists.newArrayList(" ", "", ""));
        assertEquals(parse(" / / ", false), Lists.newArrayList(" ", " ", " "));
        assertEquals(parse(escape(" / / "), true), Lists.newArrayList(" ", " ", " "));
        assertEquals(parse("a/", false), Lists.newArrayList("a", ""));
        assertEquals(parse(escape("a/"), true), Lists.newArrayList("a", ""));
        assertEquals(parse("a/b", false), Lists.newArrayList("a", "b"));
        assertEquals(parse(escape("a/b"), true), Lists.newArrayList("a", "b"));
        assertEquals(parse("a/b/", false), Lists.newArrayList("a", "b", ""));
        assertEquals(parse(escape("a/b/"), true), Lists.newArrayList("a", "b", ""));
    }

}
