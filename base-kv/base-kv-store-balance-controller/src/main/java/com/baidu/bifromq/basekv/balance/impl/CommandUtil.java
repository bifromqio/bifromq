/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.basekv.balance.impl;

import com.baidu.bifromq.basekv.balance.command.ChangeConfigCommand;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.raft.proto.ClusterConfig;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

public class CommandUtil {
    public static Optional<ChangeConfigCommand> quit(String localStoreId, KVRangeDescriptor rangeDescriptor) {
        ClusterConfig config = rangeDescriptor.getConfig();
        if (config.getVotersCount() > 1 || config.getLearnersCount() > 0) {
            return Optional.of(ChangeConfigCommand.builder()
                .toStore(localStoreId)
                .kvRangeId(rangeDescriptor.getId())
                .expectedVer(rangeDescriptor.getVer())
                .voters(Set.of(localStoreId))
                .learners(Collections.emptySet())
                .build());
        } else {
            return Optional.of(ChangeConfigCommand.builder()
                .toStore(localStoreId)
                .kvRangeId(rangeDescriptor.getId())
                .expectedVer(rangeDescriptor.getVer())
                .voters(Collections.emptySet())
                .learners(Collections.emptySet())
                .build());
        }
    }

}
