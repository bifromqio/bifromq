package com.baidu.bifromq.plugin.subbroker;

import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;

public class DeliveryPack {
    public final TopicMessagePack messagePack;
    public final Iterable<SubInfo> inboxes;

    public DeliveryPack(TopicMessagePack messagePack, Iterable<SubInfo> inboxes) {
        this.messagePack = messagePack;
        this.inboxes = inboxes;
    }
}
