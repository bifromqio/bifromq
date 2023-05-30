package com.baidu.bifromq.inbox.client;

import com.baidu.bifromq.plugin.inboxbroker.IInboxBroker;

public interface IInboxBrokerClient extends IInboxBroker {
    static InboxBrokerClientBuilder.InProcOfflineInboxClientBuilder inProcClientBuilder() {
        return new InboxBrokerClientBuilder.InProcOfflineInboxClientBuilder();
    }

    static InboxBrokerClientBuilder.NonSSLOfflineInboxClientBuilder nonSSLClientBuilder() {
        return new InboxBrokerClientBuilder.NonSSLOfflineInboxClientBuilder();
    }

    static InboxBrokerClientBuilder.SSLOfflineInboxWriterClientBuilder sslClientBuilder() {
        return new InboxBrokerClientBuilder.SSLOfflineInboxWriterClientBuilder();
    }

    @Override
    default int id() {
        return 1;
    }
}
