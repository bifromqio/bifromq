package com.baidu.bifromq.inbox.client;

import static java.util.Collections.emptyMap;

import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.inbox.rpc.proto.InboxMessagePack;
import com.baidu.bifromq.inbox.rpc.proto.InboxServiceGrpc;
import com.baidu.bifromq.inbox.rpc.proto.SendReply;
import com.baidu.bifromq.inbox.rpc.proto.SendRequest;
import com.baidu.bifromq.inbox.rpc.proto.SendResult;
import com.baidu.bifromq.plugin.inboxbroker.IInboxGroupWriter;
import com.baidu.bifromq.plugin.inboxbroker.InboxPack;
import com.baidu.bifromq.plugin.inboxbroker.WriteResult;
import com.baidu.bifromq.type.SubInfo;
import com.google.common.collect.Iterables;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

class InboxPipeline implements IInboxGroupWriter {
    private final IRPCClient.IRequestPipeline<SendRequest, SendReply> ppln;

    InboxPipeline(String inboxGroupKey, IRPCClient rpcClient) {
        ppln = rpcClient.createRequestPipeline("", null, inboxGroupKey,
            emptyMap(), InboxServiceGrpc.getReceiveMethod());
    }

    @Override
    public CompletableFuture<Map<SubInfo, WriteResult>> write(Iterable<InboxPack> messagePacks) {
        long reqId = System.nanoTime();
        return ppln.invoke(SendRequest.newBuilder()
                .setReqId(reqId)
                .addAllInboxMsgPack(Iterables.transform(messagePacks,
                    e -> InboxMessagePack.newBuilder()
                        .setMessages(e.messagePack)
                        .addAllSubInfo(e.inboxes)
                        .build()))
                .build())
            .thenApply(sendReply -> sendReply.getResultList().stream()
                .collect(Collectors.toMap(SendResult::getSubInfo, e -> {
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
                WriteResult error = WriteResult.error(e);
                Map<SubInfo, WriteResult> resultMap = new HashMap<>();
                for (InboxPack inboxWrite : messagePacks) {
                    for (SubInfo subInfo : inboxWrite.inboxes) {
                        resultMap.put(subInfo, error);
                    }
                }
                return resultMap;
            });
    }

    @Override
    public void close() {
        ppln.close();
    }
}
