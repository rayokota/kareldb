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

import com.google.common.collect.ImmutableMap;
import io.kareldb.avro.AvroKeyComparator;
import io.kareldb.avro.AvroUtils;
import io.kareldb.kafka.serialization.KafkaKeySerde;
import io.kareldb.kafka.serialization.KafkaValueSerde;
import io.kareldb.schema.FilterableTable;
import io.kareldb.schema.RelDef;
import io.kareldb.schema.Schema;
import io.kareldb.version.VersionedCache;
import io.kareldb.version.VersionedValue;
import io.kcache.Cache;
import io.kcache.KafkaCache;
import io.kcache.KafkaCacheConfig;
import io.kcache.utils.TransformedRawCache;
import org.apache.avro.Conversions;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.calcite.util.Pair;
import org.apache.kafka.common.serialization.Serdes;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

import static io.kareldb.avro.AvroSchema.SQL_KEY_INDEX_PROP;

/**
 * Table that reads Kafka topics.
 */
public class KafkaTable extends FilterableTable {
    private KafkaCache<byte[], byte[]> rows;

    public final static GenericData GENERIC = new GenericData();

    static {
        GENERIC.addLogicalTypeConversion(new Conversions.DecimalConversion());
    }

    public KafkaTable(Schema schema, String name, RelDef rowType) {
        super(schema, name, rowType);
    }

    @Override
    public VersionedCache getRows() {
        KafkaSchema schema = (KafkaSchema) getSchema();
        KafkaSchemaValue schemaValue = schema.getLatestSchemaValue(getName());
        org.apache.avro.Schema avroSchema = AvroUtils.parseSchema(schemaValue.getSchema());
        Pair<org.apache.avro.Schema, org.apache.avro.Schema> schemas = getKeyValueSchemas(avroSchema);
        KafkaKeySerde keySerde = new KafkaKeySerde();
        KafkaValueSerde valueSerde = new KafkaValueSerde();
        keySerde.configure(ImmutableMap.of(
            "table", this,
            "avroSchema", schemas.left,
            "version", schemaValue.getVersion()), true);
        valueSerde.configure(ImmutableMap.of(
            "table", this,
            "avroSchema", schemas.right,
            "version", schemaValue.getVersion()), false);
        Cache<Comparable[], NavigableMap<Long, VersionedValue>> transformedCache = new TransformedRawCache<>(
            keySerde, valueSerde, rows);
        return new VersionedCache(getName(), transformedCache, keySerde, valueSerde);
    }

    @Override
    public void configure(Map<String, ?> operand) {
        super.configure(operand);
        if (getRowType() == null) {
            throw new IllegalStateException("Custom tables not yet supported for Kafka");
        }
        Map<String, Object> configs = new HashMap<>(operand);
        String groupId = (String) configs.getOrDefault(KafkaCacheConfig.KAFKACACHE_GROUP_ID_CONFIG, "kareldb-1");
        int epoch = (Integer) configs.get("epoch");
        org.apache.avro.Schema avroSchema = (org.apache.avro.Schema) configs.get("avroSchema");
        Pair<org.apache.avro.Schema, org.apache.avro.Schema> schemas = getKeyValueSchemas(avroSchema);
        String topic = getName() + "_" + epoch;
        configs.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, topic);
        configs.put(KafkaCacheConfig.KAFKACACHE_GROUP_ID_CONFIG, groupId);
        configs.put(KafkaCacheConfig.KAFKACACHE_CLIENT_ID_CONFIG, groupId + "-" + topic);
        Comparator<byte[]> cmp = new AvroKeyComparator(schemas.left);
        this.rows = new KafkaCache<>(
            new KafkaCacheConfig(configs), Serdes.ByteArray(), Serdes.ByteArray(), null, topic, cmp);
    }

    @Override
    public void init() {
        rows.init();
    }

    @Override
    public void sync() {
        rows.sync();
    }

    @Override
    public void close() throws IOException {
        rows.close();
    }

    public static Pair<org.apache.avro.Schema, org.apache.avro.Schema> getKeyValueSchemas(org.apache.avro.Schema schema) {
        SchemaBuilder.FieldAssembler<org.apache.avro.Schema> keySchemaBuilder =
            SchemaBuilder.record(schema.getName() + "_key").fields();
        SchemaBuilder.FieldAssembler<org.apache.avro.Schema> valueSchemaBuilder =
            SchemaBuilder.record(schema.getName() + "_value").fields();
        int size = schema.getFields().size();
        Field[] keyFields = new Field[size];
        Field[] valueFields = new Field[size];
        int valueIndex = 0;
        for (Field field : schema.getFields()) {
            Integer keyIndex = (Integer) field.getObjectProp(SQL_KEY_INDEX_PROP);
            if (keyIndex != null) {
                keyFields[keyIndex] = field;
            } else {
                valueFields[valueIndex++] = field;
            }
        }
        int keyCount = 0;
        for (Field field : keyFields) {
            if (field == null) {
                break;
            }
            keySchemaBuilder = keySchemaBuilder.name(field.name()).type(field.schema()).noDefault();
            keyCount++;
        }
        valueIndex = 0;
        if (keyCount == 0) {
            // Use first value field as key
            Field field = valueFields[valueIndex++];
            keySchemaBuilder = keySchemaBuilder.name(field.name()).type(field.schema()).noDefault();
        }
        valueSchemaBuilder = valueSchemaBuilder
            .name("_version").type().longType().noDefault()
            .name("_commit").type().longType().noDefault()
            .name("_deleted").type().booleanType().noDefault();
        for (; valueIndex < valueFields.length; valueIndex++) {
            Field field = valueFields[valueIndex];
            if (field == null) {
                break;
            }
            SchemaBuilder.GenericDefault<org.apache.avro.Schema> builder
                = valueSchemaBuilder.name(field.name()).type(field.schema());
            if (field.hasDefaultValue()) {
                Object defaultVal = field.defaultVal();
                valueSchemaBuilder = builder.withDefault(defaultVal == JsonProperties.NULL_VALUE ? null : defaultVal);
            } else {
                valueSchemaBuilder = builder.noDefault();
            }
        }
        org.apache.avro.Schema keySchema = keySchemaBuilder.endRecord();
        org.apache.avro.Schema valueSchema = SchemaBuilder.array().items(valueSchemaBuilder.endRecord());
        return Pair.of(keySchema, valueSchema);
    }
}
