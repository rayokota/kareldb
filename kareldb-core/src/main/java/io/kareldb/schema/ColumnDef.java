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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

public class ColumnDef {
    private final ColumnType columnType;
    private final ColumnStrategy columnStrategy;
    private final int precision;
    private final int scale;

    public ColumnDef(ColumnType columnType) {
        this(columnType, ColumnStrategy.NULL_STRATEGY, 0, 0);
    }

    public ColumnDef(ColumnType columnType, int precision, int scale) {
        this(columnType, ColumnStrategy.NULL_STRATEGY, precision, scale);
    }

    public ColumnDef(ColumnType columnType, ColumnStrategy columnStrategy, int precision, int scale) {
        this.columnType = columnType;
        this.columnStrategy = columnStrategy;
        this.precision = precision;
        this.scale = scale;
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    public ColumnStrategy getColumnStrategy() {
        return columnStrategy;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public RelDataType toType(JavaTypeFactory typeFactory) {
        SqlTypeName sqlTypeName = getColumnType().toType(typeFactory);
        RelDataType sqlType = columnType == ColumnType.DECIMAL
            ? typeFactory.createSqlType(sqlTypeName, precision, scale)
            : typeFactory.createSqlType(sqlTypeName);
        return typeFactory.createTypeWithNullability(sqlType, true);
    }
}
