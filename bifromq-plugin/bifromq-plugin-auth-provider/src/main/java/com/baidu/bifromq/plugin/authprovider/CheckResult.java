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

package com.baidu.bifromq.plugin.authprovider;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;

public abstract class CheckResult {
    public enum Type {
        ALLOW, DISALLOW, ERROR
    }

    public static final Allow ALLOW = new Allow();
    public static final Disallow DISALLOW = new Disallow();

    public static Error error(@NonNull Throwable cause) {
        return new Error(cause);
    }

    public abstract Type type();

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class Allow extends CheckResult {

        @Override
        public Type type() {
            return Type.ALLOW;
        }
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class Disallow extends CheckResult {

        @Override
        public Type type() {
            return Type.DISALLOW;
        }
    }

    public static final class Error extends CheckResult {
        public final Throwable cause;

        private Error(Throwable cause) {
            this.cause = cause;
        }

        @Override
        public Type type() {
            return Type.ERROR;
        }
    }
}
