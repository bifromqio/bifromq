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

package com.baidu.bifromq.starter.config.standalone.model;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class StateStoreConfig {
    private int tickerThreads = Math.max(1, Runtime.getRuntime().availableProcessors() / 20);
    private int bgWorkerThreads = Math.max(1, EnvProvider.INSTANCE.availableProcessors() / 4);

    @JsonSetter(nulls = Nulls.SKIP)
    private DistWorkerConfig distWorkerConfig = new DistWorkerConfig();
    @JsonSetter(nulls = Nulls.SKIP)
    private InboxStoreConfig inboxStoreConfig = new InboxStoreConfig();
    @JsonSetter(nulls = Nulls.SKIP)
    private RetainStoreConfig retainStoreConfig = new RetainStoreConfig();

    @Getter
    @Setter
    public static class BalancerOptions {
        private long retryDelayInMS = 5000;
        @JsonSetter(nulls = Nulls.SKIP)
        @JsonSerialize(using = BalancerFactorySerializer.class)
        @JsonDeserialize(using = BalancerFactoryDeserializer.class)
        private Map<String, Struct> balancers = new HashMap<>();
    }

    @Getter
    @Setter
    public static class DistWorkerConfig {
        private int queryPipelinePerStore = 1000;
        private int compactWALThreshold = 2500;
        @JsonSetter(nulls = Nulls.SKIP)
        private StorageEngineConfig dataEngineConfig = new RocksDBEngineConfig();
        @JsonSetter(nulls = Nulls.SKIP)
        private StorageEngineConfig walEngineConfig = new RocksDBEngineConfig()
            .setManualCompaction(true)
            .setCompactMinTombstoneKeys(2500)
            .setCompactMinTombstoneRanges(2);
        @JsonSetter(nulls = Nulls.SKIP)
        private BalancerOptions balanceConfig = new BalancerOptions();

        public DistWorkerConfig() {
            // DO not enable DistWorker split by default
//            balanceConfig.balancers.add("com.baidu.bifromq.dist.worker.balance.DistWorkerSplitBalancerFactory");
            balanceConfig.balancers.put("com.baidu.bifromq.dist.worker.balance.RangeLeaderBalancerFactory",
                Struct.getDefaultInstance());
            balanceConfig.balancers.put("com.baidu.bifromq.dist.worker.balance.ReplicaCntBalancerFactory",
                Struct.getDefaultInstance());
            balanceConfig.balancers.put(
                "com.baidu.bifromq.dist.worker.balance.UnreachableReplicaRemovalBalancerFactory",
                Struct.getDefaultInstance());
            balanceConfig.balancers.put("com.baidu.bifromq.dist.worker.balance.RangeBootstrapBalancerFactory",
                Struct.getDefaultInstance());
            balanceConfig.balancers.put("com.baidu.bifromq.dist.worker.balance.RedundantEpochRemovalBalancerFactory",
                Struct.getDefaultInstance());
        }
    }

    @Getter
    @Setter
    public static class InboxStoreConfig {
        private int queryPipelinePerStore = 100;
        private int compactWALThreshold = 2500;
        private int gcIntervalSeconds = 600;
        @JsonSetter(nulls = Nulls.SKIP)
        private StorageEngineConfig dataEngineConfig = new RocksDBEngineConfig();
        @JsonSetter(nulls = Nulls.SKIP)
        private StorageEngineConfig walEngineConfig = new RocksDBEngineConfig()
            .setManualCompaction(true)
            .setCompactMinTombstoneKeys(2500)
            .setCompactMinTombstoneRanges(2);
        @JsonSetter(nulls = Nulls.SKIP)
        private BalancerOptions balanceConfig = new BalancerOptions();

        public InboxStoreConfig() {
            balanceConfig.balancers.put("com.baidu.bifromq.inbox.store.balance.ReplicaCntBalancerFactory",
                Struct.getDefaultInstance());
            balanceConfig.balancers.put("com.baidu.bifromq.inbox.store.balance.RangeSplitBalancerFactory",
                Struct.getDefaultInstance());
            balanceConfig.balancers.put("com.baidu.bifromq.inbox.store.balance.RangeLeaderBalancerFactory",
                Struct.getDefaultInstance());
            balanceConfig.balancers.put(
                "com.baidu.bifromq.inbox.store.balance.UnreachableReplicaRemovalBalancerFactory",
                Struct.getDefaultInstance());
            balanceConfig.balancers.put("com.baidu.bifromq.inbox.store.balance.RangeBootstrapBalancerFactory",
                Struct.getDefaultInstance());
            balanceConfig.balancers.put("com.baidu.bifromq.inbox.store.balance.RedundantEpochRemovalBalancerFactory",
                Struct.getDefaultInstance());
        }
    }

    @Getter
    @Setter
    public static class RetainStoreConfig {
        private int queryPipelinePerStore = 100;
        private int compactWALThreshold = 2500;
        private int gcIntervalSeconds = 600;
        @JsonSetter(nulls = Nulls.SKIP)
        private StorageEngineConfig dataEngineConfig = new RocksDBEngineConfig();
        @JsonSetter(nulls = Nulls.SKIP)
        private StorageEngineConfig walEngineConfig = new RocksDBEngineConfig()
            .setManualCompaction(true)
            .setCompactMinTombstoneKeys(2500)
            .setCompactMinTombstoneRanges(2);
        @JsonSetter(nulls = Nulls.SKIP)
        private BalancerOptions balanceConfig = new BalancerOptions();

        public RetainStoreConfig() {
            balanceConfig.balancers.put("com.baidu.bifromq.retain.store.balance.ReplicaCntBalancerFactory",
                Struct.getDefaultInstance());
            balanceConfig.balancers.put(
                "com.baidu.bifromq.retain.store.balance.UnreachableReplicaRemovalBalancerFactory",
                Struct.getDefaultInstance());
            balanceConfig.balancers.put("com.baidu.bifromq.retain.store.balance.RangeBootstrapBalancerFactory",
                Struct.getDefaultInstance());
            balanceConfig.balancers.put("com.baidu.bifromq.retain.store.balance.RedundantEpochRemovalBalancerFactory",
                Struct.getDefaultInstance());
        }
    }

    private static class BalancerFactorySerializer extends JsonSerializer<Map<String, Struct>> {
        @Override
        public void serialize(Map<String, Struct> value, JsonGenerator gen, SerializerProvider serializers)
            throws IOException {
            gen.writeStartObject();
            for (Map.Entry<String, Struct> entry : value.entrySet()) {
                String balancerName = entry.getKey();
                Struct balancerConfig = entry.getValue();
                gen.writeFieldName(balancerName);
                String jsonString = JsonFormat.printer().print(balancerConfig);
                JsonNode jsonNode = gen.getCodec().readTree(gen.getCodec().getFactory().createParser(jsonString));
                gen.writeTree(jsonNode);
            }
            gen.writeEndObject();
        }
    }

    private static class BalancerFactoryDeserializer extends JsonDeserializer<Map<String, Struct>> {
        @Override
        public Map<String, Struct> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            JsonNode node = p.getCodec().readTree(p);
            Map<String, Struct> balancerConfigMap = new HashMap<>();

            if (node.isArray()) {
                for (JsonNode balancerNode : node) {
                    String balancerName = balancerNode.asText();
                    balancerConfigMap.put(balancerName, Struct.getDefaultInstance());
                }
            } else if (node.isObject()) {
                Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> field = fields.next();
                    String balancerName = field.getKey();
                    JsonNode balancerConfigNode = field.getValue();
                    String jsonString = balancerConfigNode.toString();
                    Struct.Builder structBuilder = Struct.newBuilder();
                    JsonFormat.parser().merge(jsonString, structBuilder);
                    Struct balancerConfig = structBuilder.build();
                    balancerConfigMap.put(balancerName, balancerConfig);
                }
            }
            return balancerConfigMap;
        }
    }
}
