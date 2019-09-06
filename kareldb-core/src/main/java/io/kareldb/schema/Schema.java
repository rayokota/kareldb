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
package io.kareldb.schema;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.ddl.SqlAlterTableExtension;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.kafka.common.Configurable;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class Schema extends AbstractSchema implements Configurable, Closeable {
    private Map<String, Object> configs;

    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs) {
        this.configs = (Map<String, Object>) configs;
    }

    public abstract void init();

    public Map<String, Object> getConfigs() {
        return configs;
    }

    @Override
    public abstract Map<String, org.apache.calcite.schema.Table> getTableMap();

    public abstract void createTable(String tableName, Map<String, Object> operand, RelDef rowType);

    public abstract void alterTable(String tableName,
                                    List<SqlAlterTableExtension.Action> actions,
                                    RelDef rowType);

    public abstract boolean dropTable(String tableName);

    public static RelDef toRowType(LinkedHashMap<String, ColumnDef> columnTypes, List<String> keyFields) {
        JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
        List<RelDataType> types = new ArrayList<>();
        for (ColumnDef columnDef : columnTypes.values()) {
            final RelDataType type;
            if (columnDef == null) {
                type = typeFactory.createSqlType(SqlTypeName.VARCHAR);
            } else {
                type = columnDef.toType(typeFactory);
            }
            types.add(type);
        }
        RelDataType rowType = typeFactory.createStructType(Pair.zip(new ArrayList<>(columnTypes.keySet()), types));
        List<ColumnStrategy> strategies = columnTypes.values().stream()
            .map(ColumnDef::getColumnStrategy)
            .collect(Collectors.toList());
        return new RelDef(rowType, keyFields, strategies);
    }

    public static LinkedHashMap<String, ColumnDef> toColumnDefs(RelDataType rowType) {
        final LinkedHashMap<String, ColumnDef> columnDefs = new LinkedHashMap<>();
        for (RelDataTypeField field : rowType.getFieldList()) {
            columnDefs.put(field.getName(), toColumnDef(field.getType()));
        }
        return columnDefs;
    }

    private static ColumnDef toColumnDef(RelDataType fieldType) {
        SqlTypeName sqlTypeName = fieldType.getSqlTypeName();
        ColumnType columnType = ColumnType.of(sqlTypeName);
        return sqlTypeName == SqlTypeName.DECIMAL
            ? new ColumnDef(columnType, fieldType.getPrecision(), fieldType.getScale())
            : new ColumnDef(columnType);
    }
}
