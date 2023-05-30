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

package com.baidu.bifromq.dist.client;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;

public abstract class DistResult {
    public enum Type {
        SUCCEED, ERROR
    }

    public abstract Type type();

    public static final Error InternalError = new Error(new RuntimeException("Internal Error"));

    public static final Succeed Succeed = new Succeed();


    public static Error error(Throwable e) {
        return new Error(e);
    }

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class Succeed extends DistResult {
        public Type type() {
            return Type.SUCCEED;
        }
    }

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class Error extends DistResult {
        public final Throwable cause;

        @Override
        public Type type() {
            return Type.ERROR;
        }
    }
}
