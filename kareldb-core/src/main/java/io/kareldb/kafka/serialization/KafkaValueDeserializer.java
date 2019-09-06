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
package io.kareldb.kafka.serialization;

import io.kareldb.avro.AvroSchema;
import io.kareldb.avro.AvroUtils;
import io.kareldb.kafka.KafkaSchema;
import io.kareldb.kafka.KafkaSchemaValue;
import io.kareldb.kafka.KafkaTable;
import io.kareldb.version.VersionedValue;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.calcite.util.Pair;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static io.kareldb.kafka.KafkaTable.getKeyValueSchemas;
import static io.kareldb.kafka.serialization.KafkaValueSerde.MAGIC_BYTE;
import static io.kareldb.kafka.serialization.KafkaValueSerde.VERSION_SIZE;

public class KafkaValueDeserializer implements Deserializer<NavigableMap<Long, VersionedValue>> {
    private final static Logger LOG = LoggerFactory.getLogger(KafkaValueDeserializer.class);

    private final DecoderFactory decoderFactory = DecoderFactory.get();
    private KafkaTable table;
    private Schema avroSchema;
    private int version;
    private final Map<Integer, DatumReader<GenericArray<GenericRecord>>> readers = new HashMap<>();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        table = (KafkaTable) configs.get("table");
        avroSchema = (Schema) configs.get("avroSchema");
        version = (Integer) configs.get("version");
    }

    @Override
    public NavigableMap<Long, VersionedValue> deserialize(String topic, byte[] payload) throws SerializationException {
        if (payload == null) {
            return null;
        }
        try {
            ByteBuffer buffer = getByteBuffer(payload);
            int version = buffer.getInt();
            int length = buffer.limit() - 1 - VERSION_SIZE;
            int start = buffer.position() + buffer.arrayOffset();
            DatumReader<GenericArray<GenericRecord>> reader = readers.get(version);
            if (reader == null) {
                KafkaSchema schema = (KafkaSchema) table.getSchema();
                KafkaSchemaValue schemaValue = schema.getSchemaValue(table.getName(), version);
                Schema writerSchema = AvroUtils.parseSchema(schemaValue.getSchema());
                Pair<Schema, Schema> schemas = getKeyValueSchemas(writerSchema);
                Schema valueSchema = schemas.right;
                reader = new GenericDatumReader<>(valueSchema, avroSchema, KafkaTable.GENERIC);
                readers.put(version, reader);
            }
            GenericArray<GenericRecord> array = reader.read(
                null, decoderFactory.binaryDecoder(buffer.array(), start, length, null));
            return toValue(array);
        } catch (IOException | RuntimeException e) {
            // avro deserialization may throw AvroRuntimeException, NullPointerException, etc
            LOG.error("Error deserializing Avro value " + e.getMessage());
            throw new SerializationException("Error deserializing Avro value", e);
        }
    }

    private ByteBuffer getByteBuffer(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        if (buffer.get() != MAGIC_BYTE) {
            throw new SerializationException("Unknown magic byte!");
        }
        return buffer;
    }

    private NavigableMap<Long, VersionedValue> toValue(GenericArray<GenericRecord> array) {
        NavigableMap<Long, VersionedValue> map = new TreeMap<>();
        Schema recordSchema = avroSchema.getElementType();
        List<Schema.Field> fields = recordSchema.getFields();
        int size = fields.size();
        for (GenericRecord record : array) {
            Long version = (Long) record.get(0);
            Long commit = (Long) record.get(1);
            boolean deleted = (Boolean) record.get(2);
            Comparable[] row = new Comparable[size - 3];
            for (int i = 0; i < row.length; i++) {
                Schema schema = fields.get(i + 3).schema();
                Comparable value = (Comparable) record.get(i + 3);
                row[i] = AvroSchema.fromAvroValue(schema, value);
            }
            map.put(version, new VersionedValue(version, commit, deleted, row));
        }
        return map;
    }

    @Override
    public void close() {
    }
}
