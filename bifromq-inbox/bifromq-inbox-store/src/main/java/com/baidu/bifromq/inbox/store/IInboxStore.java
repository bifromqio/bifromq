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

package com.baidu.bifromq.inbox.store;

public interface IInboxStore {
    String CLUSTER_NAME = "inbox.store";

    static InboxStoreBuilder.InProcStore inProcBuilder() {
        return new InboxStoreBuilder.InProcStore();
    }

    static InboxStoreBuilder.NonSSLInboxBuilder nonSSLBuilder() {
        return new InboxStoreBuilder.NonSSLInboxBuilder();
    }

    static InboxStoreBuilder.SSLInboxBuilder sslBuilder() {
        return new InboxStoreBuilder.SSLInboxBuilder();
    }


    String id();

    void start(boolean bootstrap);

    void stop();
}
