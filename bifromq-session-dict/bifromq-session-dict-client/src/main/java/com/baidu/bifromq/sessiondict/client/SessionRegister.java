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

package com.baidu.bifromq.sessiondict.client;

import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.sessiondict.rpc.proto.Quit;
import com.baidu.bifromq.sessiondict.rpc.proto.Session;
import com.baidu.bifromq.type.ClientInfo;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.observers.DisposableObserver;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SessionRegister implements ISessionRegister {
    private enum State {
        Registered, Kicked, Quit
    }

    private final AtomicReference<State> state;
    private final ClientInfo owner;
    private final IRPCClient.IMessageStream<Quit, Session> regPipeline;
    private final Consumer<ClientInfo> onKick;
    private final CompositeDisposable consumptions = new CompositeDisposable();

    public SessionRegister(ClientInfo owner, Consumer<ClientInfo> onKick,
                           IRPCClient.IMessageStream<Quit, Session> regPipeline) {
        this.owner = owner;
        this.regPipeline = regPipeline;
        this.onKick = onKick;
        this.state = new AtomicReference<>(State.Registered);
        reg();
    }

    private void reg() {
        if (state.get() != State.Registered) {
            return;
        }
        regPipeline.ack(Session.newBuilder()
            .setReqId(System.nanoTime())
            .setOwner(owner)
            .setKeep(true)
            .build());
        consumptions.add(regPipeline.msg()
            .subscribeWith(new DisposableObserver<Quit>() {
                @Override
                public void onNext(@NonNull Quit quit) {
                    if (quit.getOwner().equals(owner)) {
                        if (log.isTraceEnabled()) {
                            log.trace("Received quit request:reqId={},killer={}", quit.getReqId(), quit.getKiller());
                        }
                        onKick.accept(quit.getKiller());
                        state.set(State.Kicked);
                        stop();
                    }
                }

                @Override
                public void onError(@NonNull Throwable e) {
                    consumptions.remove(this);
                    reg();
                }

                @Override
                public void onComplete() {
                    consumptions.remove(this);
                    reg();
                }
            })
        );
    }

    @Override
    public void stop() {
        if (state.get() == State.Quit) {
            return;
        }
        if (state.get() == State.Registered) {
            regPipeline.ack(Session.newBuilder()
                .setReqId(System.nanoTime())
                .setOwner(owner)
                .setKeep(false)
                .build());
        }
        consumptions.dispose();
        state.set(State.Quit);
    }
}
