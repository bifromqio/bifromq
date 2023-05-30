package com.baidu.bifromq.inbox.client;

import static java.util.Collections.emptyMap;

import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.inbox.rpc.proto.InboxMessagePack;
import com.baidu.bifromq.inbox.rpc.proto.InboxServiceGrpc;
import com.baidu.bifromq.inbox.rpc.proto.SendReply;
import com.baidu.bifromq.inbox.rpc.proto.SendRequest;
import com.baidu.bifromq.plugin.inboxbroker.IInboxWriter;
import com.baidu.bifromq.plugin.inboxbroker.WriteResult;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.common.collect.Iterables;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

class InboxPipeline implements IInboxWriter {
    private final IRPCClient.IRequestPipeline<SendRequest, SendReply> ppln;

    InboxPipeline(String inboxGroupKey, IRPCClient rpcClient) {
        ppln = rpcClient.createRequestPipeline("", null, inboxGroupKey,
            emptyMap(), InboxServiceGrpc.getReceiveMethod());
    }

    @Override
    public CompletableFuture<Map<SubInfo, WriteResult>> write(Map<TopicMessagePack, List<SubInfo>> messages) {
        long reqId = System.nanoTime();
        return ppln.invoke(SendRequest.newBuilder()
                .setReqId(reqId)
                .addAllInboxMsgPack(Iterables.transform(messages.entrySet(), e -> InboxMessagePack.newBuilder()
                    .setMessages(e.getKey())
                    .addAllSubInfo(e.getValue())
                    .build()))
                .build())
            .thenApply(sendReply -> sendReply.getResultList().stream()
                .collect(Collectors.toMap(e -> e.getSubInfo(), e -> {
                    switch (e.getResult()) {
                        case OK:
                            return WriteResult.OK;
                        case NO_INBOX:
                            return WriteResult.NO_INBOX;
                        case ERROR:
                        default:
                            return WriteResult.error(new RuntimeException("inbox server internal error"));
                    }
                })))
            .exceptionally(e -> {
                WriteResult result = WriteResult.error(e);
                return messages.values().stream().flatMap(l -> l.stream())
                    .collect(Collectors.toMap(s -> s, s -> result));
            });
    }

    @Override
    public void close() {
        ppln.close();
    }
}
