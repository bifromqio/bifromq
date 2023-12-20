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

package com.baidu.bifromq.dist.util.benchmark;

import com.baidu.bifromq.dist.util.TestUtil;
import com.baidu.bifromq.dist.util.TopicTrie;
import com.baidu.bifromq.type.TopicMessage;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@Slf4j
@State(Scope.Thread)
public class TopicTrieBuilderBenchmarkState {
    public TopicTrie topicTrie;
    public final List<TopicMessage> distMessages = List.of(TopicMessage.getDefaultInstance());

    @Setup(Level.Iteration)
    public void setup() {
        topicTrie = new TopicTrie();
    }

    /*
     * And, check the benchmark went fine afterwards:
     */

    @TearDown(Level.Iteration)
    public void teardown() {
        log.info("topic count: {}", topicTrie.topicCount());
    }

    public String randomTopic() {
        return TestUtil.randomTopic();
    }
}
