/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kareldb.kafka;

import io.kareldb.avro.AvroSchema;
import io.kareldb.avro.AvroUtils;
import io.kareldb.kafka.serialization.KafkaSchemaKeySerde;
import io.kareldb.kafka.serialization.KafkaSchemaValueSerde;
import io.kareldb.schema.ColumnDef;
import io.kareldb.schema.ColumnStrategy;
import io.kareldb.schema.RelDef;
import io.kareldb.schema.Schema;
import io.kareldb.schema.Table;
import io.kcache.Cache;
import io.kcache.CacheUpdateHandler;
import io.kcache.KafkaCache;
import io.kcache.KafkaCacheConfig;
import io.kcache.KeyValueIterator;
import io.kcache.utils.Caches;
import io.kcache.utils.InMemoryCache;
import io.kcache.utils.Streams;
import org.apache.avro.SchemaValidationException;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.sql.ddl.SqlAlterTableExtension;
import org.apache.calcite.util.Pair;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class KafkaSchema extends Schema {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSchema.class);

    public static final int MIN_VERSION = 1;
    public static final int MAX_VERSION = Integer.MAX_VALUE;

    private final Map<String, org.apache.calcite.schema.Table> tableMap;
    private Cache<KafkaSchemaKey, KafkaSchemaValue> schemaMap;

    public KafkaSchema() {
        tableMap = new ConcurrentHashMap<>();
    }

    @Override
    public Map<String, org.apache.calcite.schema.Table> getTableMap() {
        return tableMap;
    }

    @Override
    public void configure(Map<String, ?> operand) {
        super.configure(operand);
        Map<String, Object> configs = new HashMap<>(operand);
        String groupId = (String) configs.getOrDefault(KafkaCacheConfig.KAFKACACHE_GROUP_ID_CONFIG, "kareldb-1");
        String topic = "_tables";
        configs.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, topic);
        configs.put(KafkaCacheConfig.KAFKACACHE_GROUP_ID_CONFIG, groupId);
        configs.put(KafkaCacheConfig.KAFKACACHE_CLIENT_ID_CONFIG, groupId + "-" + topic);
        // Always reload all tables on startup as they need to be initialized properly
        Cache<KafkaSchemaKey, KafkaSchemaValue> schemaMap = new KafkaCache<>(
            new KafkaCacheConfig(configs), new KafkaSchemaKeySerde(), new KafkaSchemaValueSerde(),
            new TableUpdateHandler(), new InMemoryCache<>());
        this.schemaMap = Caches.concurrentCache(schemaMap);
    }

    @Override
    public void init() {
        schemaMap.init();
        // Initialize tables in parallel
        CompletableFuture
            .allOf(tableMap.values().stream()
                .map(t -> CompletableFuture.runAsync(((Table) t)::init))
                .toArray(CompletableFuture[]::new))
            .join();
    }

    @Override
    public void sync() {
        schemaMap.sync();
        // Sync tables in parallel
        CompletableFuture
            .allOf(tableMap.values().stream()
                .map(t -> CompletableFuture.runAsync(((Table) t)::sync))
                .toArray(CompletableFuture[]::new))
            .join();
    }

    public KafkaSchemaValue getSchemaValue(String name, int version) {
        KafkaSchemaKey key = new KafkaSchemaKey(name, version);
        return schemaMap.get(key);
    }

    public List<KafkaSchemaValue> getLatestSchemaValuesDescending(String name) {
        KafkaSchemaKey key1 = new KafkaSchemaKey(name, MIN_VERSION);
        KafkaSchemaKey key2 = new KafkaSchemaKey(name, MAX_VERSION);
        // Get all schemas with the same epoch in descending order
        try (KeyValueIterator<KafkaSchemaKey, KafkaSchemaValue> iter =
                 schemaMap.descendingCache().range(key2, false, key1, true)) {
            List<KafkaSchemaValue> schemas = Streams.streamOf(iter)
                .map(kv -> kv.value)
                .collect(Collectors.toList());
            if (schemas.isEmpty()) {
                return schemas;
            }
            int epoch = schemas.get(0).getEpoch();
            return schemas.stream()
                .filter(s -> s.getEpoch() == epoch)
                .collect(Collectors.toList());
        }
    }

    public KafkaSchemaValue getLatestSchemaValue(String name) {
        KafkaSchemaKey key1 = new KafkaSchemaKey(name, MIN_VERSION);
        KafkaSchemaKey key2 = new KafkaSchemaKey(name, MAX_VERSION);
        try (KeyValueIterator<KafkaSchemaKey, KafkaSchemaValue> iter =
                 schemaMap.range(key1, true, key2, false)) {
            return Streams.streamOf(iter)
                .reduce((e1, e2) -> e2)
                .map(kv -> kv.value)
                .orElse(null);
        }
    }

    @Override
    public Table createTable(String tableName,
                             Map<String, Object> operand,
                             RelDef rowType) {
        KafkaSchemaValue latest = getLatestSchemaValue(tableName);
        // Verify the previous version is null or deleted
        if (latest != null && latest.getAction() != Action.DROP) {
            throw new IllegalStateException("Table " + tableName + " already exists");
        }
        int version = latest != null ? latest.getVersion() + 1 : 1;
        org.apache.avro.Schema avroSchema = AvroSchema.toAvroSchema(tableName, rowType);
        schemaMap.put(new KafkaSchemaKey(tableName, version),
            new KafkaSchemaValue(tableName, version, avroSchema.toString(), Action.CREATE, version));
        schemaMap.flush();
        // Initialize the table
        Table table = (Table) tableMap.get(tableName);
        table.init();
        return table;
    }

    @Override
    public void alterTable(String tableName,
                           List<SqlAlterTableExtension.Action> actions,
                           RelDef relDef) {
        if (relDef.getKeyFields().size() > 0) {
            throw new IllegalArgumentException("Key fields cannot be altered");
        }
        KafkaSchemaValue latest = getLatestSchemaValue(tableName);
        if (latest == null || latest.getAction() == Action.DROP) {
            throw new IllegalStateException("Table " + tableName + " does not exist");
        }
        int version = latest.getVersion();
        org.apache.avro.Schema avroSchema = AvroUtils.parseSchema(latest.getSchema());
        Pair<LinkedHashMap<String, ColumnDef>, List<String>> columnTypes = AvroSchema.toColumnDefs(avroSchema);
        LinkedHashMap<String, ColumnDef> oldColumnDefs = columnTypes.left;
        List<String> oldKeyFields = columnTypes.right;
        LinkedHashMap<String, ColumnDef> newColumnDefs = Schema.toColumnDefs(relDef.getRowType());
        for (Ord<Map.Entry<String, ColumnDef>> ordColumnType : Ord.zip(newColumnDefs.entrySet())) {
            int index = ordColumnType.i;
            Map.Entry<String, ColumnDef> entry = ordColumnType.e;
            ColumnDef columnDef = entry.getValue();
            SqlAlterTableExtension.Action action = actions.get(index);
            ColumnStrategy strategy = relDef.getStrategies().get(index);
            if (action == SqlAlterTableExtension.Action.DROP) {
                oldColumnDefs.remove(entry.getKey());
            } else {
                oldColumnDefs.put(entry.getKey(), new ColumnDef(columnDef.getColumnType(), strategy,
                    columnDef.getPrecision(), columnDef.getScale()));
            }
        }
        RelDef newRowType = Schema.toRowType(oldColumnDefs, oldKeyFields);
        org.apache.avro.Schema newSchema = AvroSchema.toAvroSchema(tableName, newRowType);
        try {
            AvroUtils.checkCompatibility(
                newSchema,
                getLatestSchemaValuesDescending(tableName).stream()
                    .map(v -> AvroUtils.parseSchema(v.getSchema()))
                    .collect(Collectors.toList()));
        } catch (SchemaValidationException e) {
            LOG.error(e.getMessage());
            throw new IllegalArgumentException(e);
        }
        schemaMap.put(new KafkaSchemaKey(tableName, version + 1),
            new KafkaSchemaValue(tableName, version + 1, newSchema.toString(), Action.ALTER, latest.getEpoch()));
        schemaMap.flush();
    }

    @Override
    public boolean dropTable(String tableName) {
        KafkaSchemaValue latest = getLatestSchemaValue(tableName);
        if (latest == null || latest.getAction() == Action.DROP) {
            return false;
        }
        boolean exists = tableMap.get(tableName) != null;
        int version = latest.getVersion();
        schemaMap.put(new KafkaSchemaKey(tableName, version + 1),
            new KafkaSchemaValue(tableName, version + 1, null, Action.DROP, latest.getEpoch()));
        schemaMap.flush();
        return exists;
    }

    @Override
    public void close() throws IOException {
        for (org.apache.calcite.schema.Table table : tableMap.values()) {
            ((Table) table).close();
        }
        schemaMap.close();
    }

    private class TableUpdateHandler implements CacheUpdateHandler<KafkaSchemaKey, KafkaSchemaValue> {
        @Override
        public void handleUpdate(KafkaSchemaKey schemaKey,
                                 KafkaSchemaValue schemaValue,
                                 KafkaSchemaValue oldSchemaValue,
                                 TopicPartition tp,
                                 long offset,
                                 long timestamp) {
            String tableName = schemaKey.getTableName();
            org.apache.avro.Schema avroSchema;
            RelDef rowType;
            Table table;
            switch (schemaValue.getAction()) {
                case CREATE:
                    avroSchema = AvroUtils.parseSchema(schemaValue.getSchema());
                    rowType = AvroSchema.toRowType(avroSchema);
                    Map<String, Object> configs = new HashMap<>(getConfigs());
                    configs.put("avroSchema", avroSchema);
                    configs.put("epoch", schemaValue.getEpoch());
                    table = new KafkaTable(KafkaSchema.this, avroSchema.getName(), rowType);
                    table.configure(configs);
                    tableMap.put(tableName, table);
                    break;
                case ALTER:
                    avroSchema = AvroUtils.parseSchema(schemaValue.getSchema());
                    rowType = AvroSchema.toRowType(avroSchema);
                    table = (Table) tableMap.get(tableName);
                    table.setRelDef(rowType);
                    break;
                case DROP:
                    table = (Table) tableMap.get(tableName);
                    if (table != null) {
                        try {
                            table.close();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        tableMap.remove(tableName);
                    }
                    break;
            }
        }
    }

    public enum Action {
        CREATE,
        ALTER,
        DROP
    }
}
