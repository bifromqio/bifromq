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

package com.baidu.bifromq.dist.client;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;

public abstract class ClearResult {
    public enum Type {
        OK, ERROR
    }

    public abstract ClearResult.Type type();

    public static final ClearResult.OK OK = new ClearResult.OK();

    public static final ClearResult.Error INTERNAL_ERROR =
        new ClearResult.Error(new RuntimeException("Internal Error"));

    public static ClearResult.Error error(Throwable e) {
        return new ClearResult.Error(e);
    }

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class OK extends ClearResult {
        @Override
        public Type type() {
            return Type.OK;
        }
    }

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class Error extends ClearResult {
        public final Throwable cause;

        @Override
        public Type type() {
            return Type.ERROR;
        }
    }

}
