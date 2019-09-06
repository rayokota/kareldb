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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kareldb.kafka.KafkaSchemaValue;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class KafkaSchemaValueSerializer implements Serializer<KafkaSchemaValue> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, KafkaSchemaValue value) {
        try {
            return new ObjectMapper().writeValueAsBytes(value);
        } catch (IOException e) {
            throw new SerializationException("Error while serializing schema value " + value.toString(),
                e);
        }
    }
}
