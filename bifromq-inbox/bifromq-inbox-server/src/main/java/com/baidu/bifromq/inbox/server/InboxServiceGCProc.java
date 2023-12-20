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

package com.baidu.bifromq.inbox.server;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.inbox.server.scheduler.IInboxTouchScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxTouchScheduler.Touch;
import com.baidu.bifromq.inbox.store.gc.InboxGCProc;
import com.google.protobuf.ByteString;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxServiceGCProc extends InboxGCProc {

    private final IInboxTouchScheduler touchScheduler;


    public InboxServiceGCProc(IBaseKVStoreClient storeClient,
                              IInboxTouchScheduler touchScheduler,
                              ExecutorService gcExecutor) {
        super(storeClient, gcExecutor);
        this.touchScheduler = touchScheduler;
    }


    @Override
    protected CompletableFuture<Void> gcInbox(ByteString scopedInboxId) {
        return touchScheduler.schedule(new Touch(scopedInboxId, false)).thenApply(r -> null);
    }

}
