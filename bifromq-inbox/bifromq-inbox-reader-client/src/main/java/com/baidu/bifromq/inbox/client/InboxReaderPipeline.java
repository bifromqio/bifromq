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

package com.baidu.bifromq.inbox.client;

import static com.baidu.bifromq.inbox.util.PipelineUtil.PIPELINE_ATTR_KEY_CLIENT_INFO;
import static com.baidu.bifromq.inbox.util.PipelineUtil.PIPELINE_ATTR_KEY_INBOX_ID;
import static com.baidu.bifromq.inbox.util.PipelineUtil.PIPELINE_ATTR_KEY_QOS0_LAST_FETCH_SEQ;
import static com.baidu.bifromq.inbox.util.PipelineUtil.PIPELINE_ATTR_KEY_QOS2_LAST_FETCH_SEQ;
import static com.baidu.bifromq.inbox.util.PipelineUtil.encode;

import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.inbox.rpc.proto.CommitReply;
import com.baidu.bifromq.inbox.rpc.proto.CommitRequest;
import com.baidu.bifromq.inbox.rpc.proto.FetchHint;
import com.baidu.bifromq.inbox.rpc.proto.InboxServiceGrpc;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.QoS;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class InboxReaderPipeline implements IInboxReaderClient.IInboxReader {
    private final IRPCClient.IMessageStream<Fetched, FetchHint> ppln;
    private final CompositeDisposable consumptions = new CompositeDisposable();
    private final ClientInfo clientInfo;
    private final String inboxId;
    private final IRPCClient rpcClient;
    private volatile long lastFetchQoS0Seq = -1;
    private volatile long lastFetchQoS2Seq = -1;

    InboxReaderPipeline(String inboxId, String inboxGroupKey, ClientInfo clientInfo, IRPCClient rpcClient) {
        this.clientInfo = clientInfo;
        this.inboxId = inboxId;
        this.rpcClient = rpcClient;
        Map<String, String> metadata = new HashMap<>() {{
            put(PIPELINE_ATTR_KEY_INBOX_ID, inboxId);
            put(PIPELINE_ATTR_KEY_CLIENT_INFO, encode(clientInfo));
        }};
        ppln = rpcClient.createMessageStream(clientInfo.getTrafficId(), null, inboxGroupKey, () -> {
                metadata.put(PIPELINE_ATTR_KEY_QOS0_LAST_FETCH_SEQ, lastFetchQoS0Seq + "");
                metadata.put(PIPELINE_ATTR_KEY_QOS2_LAST_FETCH_SEQ, lastFetchQoS2Seq + "");
                return metadata;
            },
            InboxServiceGrpc.getFetchMethod());
    }

    @Override
    public void fetch(Consumer<Fetched> consumer) {
        consumptions.add(ppln.msg()
            .doOnNext(fetched -> {
                if (fetched.getQos0SeqCount() > 0) {
                    lastFetchQoS0Seq = fetched.getQos0Seq(fetched.getQos0SeqCount() - 1);
                    // commit immediately
                    commit(System.nanoTime(), QoS.AT_MOST_ONCE, lastFetchQoS0Seq);
                }
                if (fetched.getQos2SeqCount() > 0) {
                    lastFetchQoS2Seq = fetched.getQos2Seq(fetched.getQos2SeqCount() - 1);
                }
            })
            .subscribe(consumer::accept));
    }

    @Override
    public void hint(int bufferCapacity) {
        log.trace("Send hint: inboxId={}, capacity={}, client={}", inboxId, bufferCapacity, clientInfo);
        ppln.ack(FetchHint.newBuilder().setCapacity(bufferCapacity).build());
    }

    @Override
    public CompletableFuture<CommitReply> commit(long reqId, QoS qos, long upToSeq) {
        log.trace("Commit: inbox={}, qos={}, seq={}, client={}", inboxId, qos, upToSeq, clientInfo);
        return rpcClient.invoke(clientInfo.getTrafficId(), null,
                CommitRequest.newBuilder()
                    .setReqId(reqId)
                    .setQos(qos)
                    .setUpToSeq(upToSeq)
                    .setInboxId(inboxId)
                    .setClientInfo(clientInfo)
                    .build(),
                InboxServiceGrpc.getCommitMethod())
            .exceptionally(e -> {
                log.error("Failed to commit inbox: {}", inboxId, e);
                return CommitReply.newBuilder()
                    .setReqId(reqId)
                    .setResult(CommitReply.Result.ERROR)
                    .build();
            });
    }

    @Override
    public void close() {
        consumptions.dispose();
        ppln.close();
    }
}
