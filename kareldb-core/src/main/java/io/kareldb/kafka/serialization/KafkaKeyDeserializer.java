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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class KafkaKeyDeserializer implements Deserializer<Comparable[]> {
    private final static Logger LOG = LoggerFactory.getLogger(KafkaKeyDeserializer.class);

    private final DecoderFactory decoderFactory = DecoderFactory.get();
    private Schema avroSchema;
    private DatumReader<GenericRecord> reader;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        avroSchema = (Schema) configs.get("avroSchema");
        reader = new GenericDatumReader<>(avroSchema, avroSchema, KafkaTable.GENERIC);
    }

    @Override
    public Comparable[] deserialize(String topic, byte[] payload) throws SerializationException {
        if (payload == null) {
            return null;
        }
        try {
            GenericRecord record = reader.read(null, decoderFactory.binaryDecoder(payload, null));
            return toKey(record);
        } catch (IOException | RuntimeException e) {
            // avro deserialization may throw AvroRuntimeException, NullPointerException, etc
            LOG.error("Error deserializing Avro key " + e.getMessage());
            throw new SerializationException("Error deserializing Avro key", e);
        }
    }

    private Comparable[] toKey(GenericRecord record) {
        List<Schema.Field> fields = avroSchema.getFields();
        int size = fields.size();
        Comparable[] row = new Comparable[size];
        for (int i = 0; i < size; i++) {
            Schema schema = fields.get(i).schema();
            Comparable value = (Comparable) record.get(i);
            row[i] = AvroSchema.fromAvroValue(schema, value);
        }
        return row;
    }

    @Override
    public void close() {
    }
}
