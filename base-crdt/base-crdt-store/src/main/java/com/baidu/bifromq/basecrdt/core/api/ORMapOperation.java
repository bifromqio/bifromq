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

package com.baidu.bifromq.basecrdt.core.api;

import com.google.protobuf.ByteString;
import lombok.ToString;

@ToString
public abstract class ORMapOperation implements ICRDTOperation {
    public interface ORMapUpdater {
        ORMapUpdate with(AWORSetOperation op);

        ORMapUpdate with(RWORSetOperation op);

        ORMapUpdate with(DWFlagOperation op);

        ORMapUpdate with(EWFlagOperation op);

        ORMapUpdate with(MVRegOperation op);

        ORMapUpdate with(CCounterOperation op);

        ORMapUpdate with(ORMapOperation op);
    }

    public interface ORMapRemover {
        ORMapRemove of(CausalCRDTType valueType);
    }

    public enum Type {
        UpdateKey,
        RemoveKey,
        Clear
    }

    public final Type type;
    public final ByteString[] keyPath;

    ORMapOperation(Type type, ByteString[] keyPath) {
        this.type = type;
        this.keyPath = keyPath;
    }

    public static ORMapRemover remove(ByteString... keyPath) {
        return valueType -> new ORMapRemove(Type.RemoveKey, keyPath, valueType);
    }

    public static ORMapUpdater update(ByteString... keyPath) {
        return new ORMapUpdater() {
            @Override
            public ORMapUpdate with(AWORSetOperation op) {
                return new ORMapUpdate(Type.UpdateKey, keyPath, op);
            }

            @Override
            public ORMapUpdate with(RWORSetOperation op) {
                return new ORMapUpdate(Type.UpdateKey, keyPath, op);
            }

            @Override
            public ORMapUpdate with(DWFlagOperation op) {
                return new ORMapUpdate(Type.UpdateKey, keyPath, op);
            }

            @Override
            public ORMapUpdate with(EWFlagOperation op) {
                return new ORMapUpdate(Type.UpdateKey, keyPath, op);
            }

            @Override
            public ORMapUpdate with(MVRegOperation op) {
                return new ORMapUpdate(Type.UpdateKey, keyPath, op);
            }

            @Override
            public ORMapUpdate with(CCounterOperation op) {
                return new ORMapUpdate(Type.UpdateKey, keyPath, op);
            }

            @Override
            public ORMapUpdate with(ORMapOperation op) {
                return new ORMapUpdate(Type.UpdateKey, keyPath, op);
            }

        };
    }

    public static class ORMapUpdate extends ORMapOperation {
        public final ICRDTOperation valueOp;

        private ORMapUpdate(Type type, ByteString[] keyPath, ICRDTOperation valueOp) {
            super(type, keyPath);
            this.valueOp = valueOp;
        }
    }

    public static class ORMapRemove extends ORMapOperation {
        public CausalCRDTType valueType;

        private ORMapRemove(Type type, ByteString[] keyPath, CausalCRDTType valueType) {
            super(type, keyPath);
            this.valueType = valueType;
        }
    }
}
