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

import com.baidu.bifromq.type.ClientInfo;
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
    private final SessionRegPipeline regPipeline;

    public SessionRegister(ClientInfo owner, Consumer<ClientInfo> onKick,
                           SessionRegPipeline regPipeline) {
        this.owner = owner;
        this.regPipeline = regPipeline;
        this.state = new AtomicReference<>(State.Registered);
        this.regPipeline.reg(owner, quit -> {
            if (log.isTraceEnabled()) {
                log.trace("Received quit request:reqId={},killer={}", quit.getReqId(), quit.getKiller());
            }
            onKick.accept(quit.getKiller());
            state.set(State.Kicked);
            stop();
        });
        this.regPipeline.sendRegInfo(owner, true);
    }

    @Override
    public void stop() {
        if (state.get() == State.Quit) {
            return;
        }
        if (state.get() == State.Registered) {
            this.regPipeline.sendRegInfo(owner, false);
        }
        regPipeline.unReg(owner);
        state.set(State.Quit);
    }
}
