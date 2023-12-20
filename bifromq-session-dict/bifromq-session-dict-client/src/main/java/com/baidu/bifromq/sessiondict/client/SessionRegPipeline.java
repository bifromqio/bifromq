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

package com.baidu.bifromq.sessiondict.client;

import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.baserpc.IRPCClient.IMessageStream;
import com.baidu.bifromq.sessiondict.rpc.proto.Quit;
import com.baidu.bifromq.sessiondict.rpc.proto.Session;
import com.baidu.bifromq.sessiondict.rpc.proto.SessionDictServiceGrpc;
import com.baidu.bifromq.type.ClientInfo;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.observers.DisposableObserver;
import java.lang.ref.Cleaner;
import java.lang.ref.Cleaner.Cleanable;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SessionRegPipeline {
    private static final Cleaner CLEANER = Cleaner.create();

    private final IRPCClient.IMessageStream<Quit, Session> messageStream;
    private final Map<ClientInfo, Consumer<Quit>> sessions = new ConcurrentHashMap<>();
    private final Cleanable cleanable;

    public SessionRegPipeline(String registerKey, IRPCClient rpcClient) {
        this.messageStream = rpcClient.createMessageStream("", null, registerKey,
            Collections.emptyMap(), SessionDictServiceGrpc.getDictMethod());
        this.cleanable = CLEANER.register(this, new PipelineCloseAction(messageStream));
        new QuitObserver(messageStream, sessions).initObserve();
    }

    public void sendRegInfo(ClientInfo owner, boolean keep) {
        messageStream.ack(Session.newBuilder()
            .setReqId(System.nanoTime())
            .setOwner(owner)
            .setKeep(keep)
            .build());
    }

    public void reg(ClientInfo owner, Consumer<Quit> kickConsumer) {
        sessions.put(owner, kickConsumer);
    }

    public void unReg(ClientInfo owner) {
        sessions.remove(owner);
    }

    public void close() {
        if (cleanable != null) {
            cleanable.clean();
        }
    }

    private static class QuitObserver {
        private final IRPCClient.IMessageStream<Quit, Session> stream;
        private final Map<ClientInfo, Consumer<Quit>> sessions;

        public QuitObserver(IRPCClient.IMessageStream<Quit, Session> stream,
                            Map<ClientInfo, Consumer<Quit>> sessions) {
            this.stream = stream;
            this.sessions = sessions;
        }

        private void initObserve() {
            if (!stream.isClosed()) {
                for (ClientInfo owner : sessions.keySet()) {
                    stream.ack(Session.newBuilder()
                        .setReqId(System.nanoTime())
                        .setOwner(owner)
                        .setKeep(true)
                        .build());
                }
                stream.msg()
                    .subscribeWith(new DisposableObserver<Quit>() {
                        @Override
                        public void onNext(@NonNull Quit quit) {
                            sessions.computeIfPresent(quit.getOwner(), (k, v) -> {
                                v.accept(quit);
                                return v;
                            });
                        }

                        @Override
                        public void onError(@NonNull Throwable e) {
                            dispose();
                            initObserve();
                        }

                        @Override
                        public void onComplete() {
                            dispose();
                            initObserve();
                        }
                    });
            }
        }
    }

    private static class PipelineCloseAction implements Runnable {
        private final IMessageStream<Quit, Session> messageStream;

        private PipelineCloseAction(IMessageStream<Quit, Session> messageStream) {
            this.messageStream = messageStream;
        }

        @Override
        public void run() {
            messageStream.close();
        }
    }
}
