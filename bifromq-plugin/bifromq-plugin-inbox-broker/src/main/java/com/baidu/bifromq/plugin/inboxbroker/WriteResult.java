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

package com.baidu.bifromq.plugin.inboxbroker;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

public abstract class WriteResult {
    public enum Type {
        OK, NO_INBOX, ERROR
    }

    public static final OK OK = new OK();
    public static final NoInbox NO_INBOX = new NoInbox();

    public static Error error(Throwable e) {
        return new Error(e);
    }

    public abstract Type type();

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class OK extends WriteResult {

        @Override
        public Type type() {
            return Type.OK;
        }
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class NoInbox extends WriteResult {

        @Override
        public Type type() {
            return Type.NO_INBOX;
        }
    }

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class Error extends WriteResult {
        public final Throwable cause;

        @Override
        public Type type() {
            return Type.ERROR;
        }
    }
}
