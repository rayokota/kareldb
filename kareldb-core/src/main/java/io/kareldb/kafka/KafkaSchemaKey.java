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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.Objects;

@JsonPropertyOrder(value = {"keytype", "subject", "version", "magic"})
public class KafkaSchemaKey implements Comparable<KafkaSchemaKey> {

    private final String tableName;
    private final Integer version;

    public KafkaSchemaKey(@JsonProperty("tableName") String tableName,
                          @JsonProperty("version") int version) {
        this.tableName = tableName;
        this.version = version;
    }

    @JsonProperty("tableName")
    public String getTableName() {
        return tableName;
    }

    @JsonProperty("version")
    public int getVersion() {
        return this.version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaSchemaKey kafkaSchemaKey = (KafkaSchemaKey) o;
        return tableName.equals(kafkaSchemaKey.tableName) &&
            version.equals(kafkaSchemaKey.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, version);
    }

    @Override
    public String toString() {
        return "SchemaKey{" +
            "tableName='" + tableName + '\'' +
            ", version=" + version +
            '}';
    }

    @Override
    public int compareTo(KafkaSchemaKey that) {
        if (this.tableName.compareTo(that.tableName) < 0) {
            return -1;
        } else if (this.tableName.compareTo(that.tableName) > 0) {
            return 1;
        }

        if (this.version.compareTo(that.version) < 0) {
            return -1;
        } else if (this.version.compareTo(that.version) > 0) {
            return 1;
        }
        return 0;
    }
}