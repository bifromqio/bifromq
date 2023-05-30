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

import java.nio.ByteBuffer;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.NonNull;

public abstract class AuthResult {
    public enum Type {
        PASS, NO_PASS, BANNED, BAD_AUTH_METHOD, AUTH_CONTINUE, ERROR
    }

    public static final Banned BANNED = new Banned();
    public static final NoPass NO_PASS = new NoPass();
    public static final BadAuthMethod BAD_AUTH_METHOD = new BadAuthMethod();

    public static Pass.PassBuilder pass() {
        return Pass.builder();
    }

    public static AuthContinue authContinue(ByteBuffer authData) {
        return new AuthContinue(authData);
    }

    public static Error error(@NonNull Throwable cause) {
        return new Error(cause);
    }

    public abstract Type type();

    public static final class Pass extends AuthResult {
        public final String trafficId;
        public final String userId;
        public final Optional<String> token;

        @Builder
        private Pass(String trafficId, String userId, String token) {
            this.trafficId = trafficId;
            this.userId = userId;
            this.token = Optional.ofNullable(token);
        }

        @Override
        public Type type() {
            return Type.PASS;
        }
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class Banned extends AuthResult {
        @Override
        public Type type() {
            return Type.BANNED;
        }
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class NoPass extends AuthResult {
        @Override
        public Type type() {
            return Type.NO_PASS;
        }
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class BadAuthMethod extends AuthResult {
        @Override
        public Type type() {
            return Type.BAD_AUTH_METHOD;
        }
    }

    public static final class AuthContinue extends AuthResult {
        public final ByteBuffer authData;

        private AuthContinue(ByteBuffer authData) {
            assert authData.isReadOnly();
            this.authData = authData;
        }

        @Override
        public Type type() {
            return Type.AUTH_CONTINUE;
        }
    }

    public static final class Error extends AuthResult {
        public final Throwable cause;

        @Override
        public Type type() {
            return Type.ERROR;
        }

        private Error(Throwable cause) {
            this.cause = cause;
        }
    }
}
