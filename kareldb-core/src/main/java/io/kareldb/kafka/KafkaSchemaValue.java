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

import java.util.Objects;

public class KafkaSchemaValue implements Comparable<KafkaSchemaValue> {

    private final String tableName;
    private final Integer version;
    private final String schema;
    private final KafkaSchema.Action action;
    private final Integer epoch;

    public KafkaSchemaValue(@JsonProperty("tableName") String tableName,
                            @JsonProperty("version") Integer version,
                            @JsonProperty("schema") String schema,
                            @JsonProperty("action") KafkaSchema.Action action,
                            @JsonProperty("epoch") Integer epoch) {
        this.tableName = tableName;
        this.version = version;
        this.schema = schema;
        this.action = action;
        this.epoch = epoch;
    }

    @JsonProperty("tableName")
    public String getTableName() {
        return tableName;
    }

    @JsonProperty("version")
    public Integer getVersion() {
        return this.version;
    }

    @JsonProperty("schema")
    public String getSchema() {
        return this.schema;
    }

    @JsonProperty("action")
    public KafkaSchema.Action getAction() {
        return action;
    }

    @JsonProperty("epoch")
    public Integer getEpoch() {
        return this.epoch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaSchemaValue that = (KafkaSchemaValue) o;
        return tableName.equals(that.tableName) &&
            version.equals(that.version) &&
            Objects.equals(schema, that.schema) &&
            action == that.action &&
            epoch.equals(that.epoch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, version, schema, action, epoch);
    }

    @Override
    public String toString() {
        return "KafkaSchemaValue{" +
            "tableName='" + tableName + '\'' +
            ", version=" + version +
            ", schema='" + schema + '\'' +
            ", action=" + action +
            ", epoch=" + epoch +
            '}';
    }


    @Override
    public int compareTo(KafkaSchemaValue that) {
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

        if (this.schema.compareTo(that.schema) < 0) {
            return -1;
        } else if (this.schema.compareTo(that.schema) > 0) {
            return 1;
        }

        if (this.action.compareTo(that.action) < 0) {
            return -1;
        } else if (this.action.compareTo(that.action) > 0) {
            return 1;
        }

        if (this.epoch.compareTo(that.epoch) < 0) {
            return -1;
        } else if (this.epoch.compareTo(that.epoch) > 0) {
            return 1;
        }
        return 0;
    }
}
