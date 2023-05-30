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

public abstract class HasResult {
    public enum Type {
        YES, NO, ERROR
    }

    public static final YES YES = new YES();

    public static final NO NO = new NO();

    public static Error error(Throwable e) {
        return new Error(e);
    }

    public abstract Type type();

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class YES extends HasResult {

        @Override
        public Type type() {
            return Type.YES;
        }
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class NO extends HasResult {

        @Override
        public Type type() {
            return Type.NO;
        }
    }

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class Error extends HasResult {
        public final Throwable cause;

        @Override
        public Type type() {
            return Type.ERROR;
        }
    }
}
