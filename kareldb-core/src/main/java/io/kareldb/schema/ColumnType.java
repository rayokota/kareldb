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
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public enum ColumnType {
    BOOLEAN(Primitive.BOOLEAN, SqlTypeName.BOOLEAN),
    INT(Primitive.INT, SqlTypeName.INTEGER),
    LONG(Primitive.LONG, SqlTypeName.BIGINT),
    FLOAT(Primitive.FLOAT, SqlTypeName.REAL),
    DOUBLE(Primitive.DOUBLE, SqlTypeName.DOUBLE),
    BYTES(byte[].class, "bytes", SqlTypeName.VARBINARY),
    STRING(String.class, "string", SqlTypeName.VARCHAR),
    DECIMAL(BigDecimal.class, "decimal", SqlTypeName.DECIMAL),
    DATE(java.sql.Date.class, "date", SqlTypeName.DATE),
    TIME(java.sql.Time.class, "time", SqlTypeName.TIME),
    TIMESTAMP(java.sql.Timestamp.class, "timestamp", SqlTypeName.TIMESTAMP);

    private final Class clazz;
    private final String simpleName;
    private final SqlTypeName sqlType;

    private static final Map<String, ColumnType> NAMES = new HashMap<>();
    private static final Map<SqlTypeName, ColumnType> TYPES = new HashMap<>();

    static {
        for (ColumnType value : values()) {
            NAMES.put(value.simpleName, value);
            TYPES.put(value.sqlType, value);
        }
        // Handle both int and Integer
        NAMES.put(Integer.class.getSimpleName(), INT);
    }

    ColumnType(Primitive primitive, SqlTypeName sqlType) {
        this(primitive.boxClass, primitive.primitiveName, sqlType);
    }

    ColumnType(Class clazz, String simpleName, SqlTypeName sqlType) {
        this.clazz = clazz;
        this.simpleName = simpleName;
        this.sqlType = sqlType;
    }

    // See org.apache.calcite.sql.type.JavaToSqlTypeConversionRules
    public Class<?> getType() {
        return clazz;
    }

    public SqlTypeName toType(JavaTypeFactory typeFactory) {
        RelDataType javaType = typeFactory.createJavaType(clazz);
        return javaType.getSqlTypeName();
    }

    public static ColumnType of(String typeString) {
        return NAMES.get(typeString);
    }

    public static ColumnType of(SqlTypeName sqlType) {
        return TYPES.get(sqlType);
    }
}
