package com.baidu.bifromq.plugin.inboxbroker;

import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;

public class InboxPack {
    public final TopicMessagePack messagePack;
    public final Iterable<SubInfo> inboxes;

    public InboxPack(TopicMessagePack messagePack, Iterable<SubInfo> inboxes) {
        this.messagePack = messagePack;
        this.inboxes = inboxes;
    }
}
