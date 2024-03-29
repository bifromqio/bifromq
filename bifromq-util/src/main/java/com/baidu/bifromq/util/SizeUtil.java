/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.util;

import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.TopicMessagePack;

public class SizeUtil {
    public static int estSizeOf(TopicMessagePack topicMsgPack) {
        int size = 2; // smallest fixed header size
        int topicLength = topicMsgPack.getTopic().length();
        for (TopicMessagePack.PublisherPack publisherPack : topicMsgPack.getMessageList()) {
            for (Message message : publisherPack.getMessageList()) {
                size += topicLength;
                size += message.getPayload().size();
            }
        }
        return size;
    }
}
