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
import io.kareldb.kafka.KafkaTable;
import io.kareldb.version.VersionedValue;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.calcite.linq4j.Ord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import static io.kareldb.kafka.serialization.KafkaValueSerde.MAGIC_BYTE;
import static io.kareldb.kafka.serialization.KafkaValueSerde.VERSION_SIZE;

public class KafkaValueSerializer implements Serializer<NavigableMap<Long, VersionedValue>> {
    private final static Logger LOG = LoggerFactory.getLogger(KafkaValueSerializer.class);

    private final EncoderFactory encoderFactory = EncoderFactory.get();
    private KafkaTable table;
    private Schema avroSchema;
    private int version;
    private DatumWriter<Object> writer;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        table = (KafkaTable) configs.get("table");
        avroSchema = (Schema) configs.get("avroSchema");
        version = (Integer) configs.get("version");
        writer = new GenericDatumWriter<>(avroSchema, KafkaTable.GENERIC);
    }

    @Override
    public byte[] serialize(String topic, NavigableMap<Long, VersionedValue> object) {
        if (object == null) {
            return null;
        }
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(MAGIC_BYTE);
            out.write(ByteBuffer.allocate(VERSION_SIZE).putInt(version).array());
            BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
            writer.write(toArray(object), encoder);
            encoder.flush();
            byte[] bytes = out.toByteArray();
            out.close();
            return bytes;
        } catch (IOException | RuntimeException e) {
            // avro serialization can throw AvroRuntimeException, NullPointerException,
            // ClassCastException, etc
            LOG.error("Error serializing Avro value " + e.getMessage());
            throw new SerializationException("Error serializing Avro value", e);
        }
    }

    private List<GenericRecord> toArray(NavigableMap<Long, VersionedValue> object) {
        List<GenericRecord> records = new ArrayList<>();
        Schema recordSchema = avroSchema.getElementType();
        for (VersionedValue versionedValue : object.values()) {
            Comparable[] value = versionedValue.getValue();
            GenericRecordBuilder builder = new GenericRecordBuilder(recordSchema);
            for (Ord<Field> field : Ord.zip(recordSchema.getFields())) {
                if (field.i == 0) {
                    builder.set(field.e, versionedValue.getVersion());
                } else if (field.i == 1) {
                    builder.set(field.e, versionedValue.getCommit());
                } else if (field.i == 2) {
                    builder.set(field.e, versionedValue.isDeleted());
                } else {
                    if (!versionedValue.isDeleted()) {
                        Object v = AvroSchema.toAvroValue(field.e.schema(), value[field.i - 3]);
                        if (v != null) {
                            builder.set(field.e, v);
                        }
                    }
                }
            }
            records.add(builder.build());
        }
        return records;
    }

    public Comparable[] toAvroValues(Comparable[] values) {
        Schema recordSchema = avroSchema.getElementType();
        List<Schema.Field> fields = recordSchema.getFields();
        Comparable[] result = new Comparable[values.length];
        for (int i = 0; i < values.length; i++) {
            result[i] = AvroSchema.toAvroValue(fields.get(i + 3).schema(), values[i]);
        }
        return result;
    }

    @Override
    public void close() {
    }
}
