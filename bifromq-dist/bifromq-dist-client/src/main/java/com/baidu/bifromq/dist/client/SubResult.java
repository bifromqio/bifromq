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

public abstract class SubResult {
    public enum Type {
        OK_QoS0, OK_QoS1, OK_QoS2, ERROR
    }

    public abstract SubResult.Type type();

    public static final SubResult.QoS0 QoS0 = new QoS0();

    public static final SubResult.QoS1 QoS1 = new QoS1();

    public static final SubResult.QoS2 QoS2 = new QoS2();

    public static final SubResult.Error InternalError = new Error(new RuntimeException("Internal Error"));

    public static SubResult.Error error(Throwable e) {
        return new SubResult.Error(e);
    }

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class QoS0 extends SubResult {
        @Override
        public Type type() {
            return Type.OK_QoS0;
        }
    }

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class QoS1 extends SubResult {
        @Override
        public Type type() {
            return Type.OK_QoS1;
        }
    }

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class QoS2 extends SubResult {
        @Override
        public Type type() {
            return Type.OK_QoS2;
        }
    }


    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class Error extends SubResult {
        public final Throwable cause;

        @Override
        public Type type() {
            return Type.ERROR;
        }
    }

}
