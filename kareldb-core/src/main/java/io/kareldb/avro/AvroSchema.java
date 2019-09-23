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
package io.kareldb.avro;

import io.kareldb.schema.ColumnDef;
import io.kareldb.schema.ColumnStrategy;
import io.kareldb.schema.ColumnType;
import io.kareldb.schema.RelDef;
import io.kareldb.schema.Schema;
import io.kareldb.schema.Table;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.util.Utf8;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.ddl.SqlAlterTableExtension;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AvroSchema extends Schema {

    public static final String AVRO_LOGICAL_TYPE_PROP = "logicalType";
    public static final String AVRO_LOGICAL_TIMESTAMP_MILLIS = "timestamp-millis";
    public static final String AVRO_LOGICAL_TIME_MILLIS = "time-millis";
    public static final String AVRO_LOGICAL_DATE = "date";
    public static final String AVRO_LOGICAL_DECIMAL = "decimal";
    public static final String AVRO_LOGICAL_DECIMAL_SCALE_PROP = "scale";
    public static final String AVRO_LOGICAL_DECIMAL_PRECISION_PROP = "precision";
    public static final String SQL_KEY_INDEX_PROP = "sql.key.index";

    private final Map<String, org.apache.calcite.schema.Table> tableMap;
    private File directoryFile;

    /**
     * Creates an Avro schema.
     */
    public AvroSchema() {
        this.tableMap = new HashMap<>();
    }

    @Override
    public Map<String, org.apache.calcite.schema.Table> getTableMap() {
        return tableMap;
    }

    @Override
    public void configure(Map<String, ?> operand) {
        super.configure(operand);
        final String directory = (String) operand.get("directory");
        final File base = (File) operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);
        File directoryFile = new File(directory);
        if (base != null && !directoryFile.isAbsolute()) {
            directoryFile = new File(base, directory);
        }
        this.directoryFile = directoryFile;
    }

    @Override
    public void init() {
        try {
            // Look for files in the directory ending in ".avsc"
            File[] files = directoryFile.listFiles((dir, name) -> name.endsWith(".avsc"));
            if (files == null) {
                System.out.println("directory " + directoryFile + " not found");
                files = new File[0];
            }
            Map<String, Object> configs = new HashMap<>(getConfigs());
            // Build a map from table name to table; each file becomes a table.
            for (File file : files) {
                Source source = Sources.of(file);
                org.apache.avro.Schema avroSchema = AvroUtils.parseSchema(source.file());
                configs.put("avroSchema", avroSchema);
                String name = avroSchema.getName();
                RelDef rowType = toRowType(avroSchema);
                createTable(name, configs, rowType);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void sync() {
    }

    @Override
    public Table createTable(String tableName,
                             Map<String, Object> operand,
                             RelDef rowType) {
        AvroTable table = new AvroTable(this, tableName, rowType);
        table.configure(operand != null ? operand : getConfigs());
        table.init();
        tableMap.put(tableName, table);
        return table;
    }

    @Override
    public void alterTable(String tableName,
                           List<SqlAlterTableExtension.Action> actions,
                           RelDef rowType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean dropTable(String tableName) {
        return tableMap.remove(tableName) != null;
    }

    @Override
    public void close() {
    }

    public static RelDef toRowType(org.apache.avro.Schema schema) {
        Pair<LinkedHashMap<String, ColumnDef>, List<String>> types = toColumnDefs(schema);
        return Schema.toRowType(types.left, types.right);
    }

    public static Pair<LinkedHashMap<String, ColumnDef>, List<String>> toColumnDefs(org.apache.avro.Schema schema) {
        int size = schema.getFields().size();
        final LinkedHashMap<String, ColumnDef> columnDefs = new LinkedHashMap<>();
        String[] keyNames = new String[size];
        for (org.apache.avro.Schema.Field field : schema.getFields()) {
            org.apache.avro.Schema fieldSchema = field.schema();
            Integer keyIndex = (Integer) field.getObjectProp(SQL_KEY_INDEX_PROP);
            if (keyIndex != null) {
                keyNames[keyIndex] = field.name();
            }
            ColumnDef columnDef = toColumnDef(fieldSchema);
            if (columnDef == null) {
                throw new IllegalArgumentException("Unsupported type " + fieldSchema.getType());
            }
            if (field.hasDefaultValue()) {
                Object defaultVal = field.defaultVal();
                // Already handled null strategy
                if (defaultVal != JsonProperties.NULL_VALUE) {
                    columnDef = new ColumnDef(columnDef.getColumnType(),
                        new ColumnStrategy.DefaultStrategy(defaultVal),
                        columnDef.getPrecision(), columnDef.getScale());
                }
            }
            columnDefs.put(field.name(), columnDef);
        }
        List<String> keyFields = new ArrayList<>(size);
        for (String keyName : keyNames) {
            if (keyName == null) {
                break;
            }
            keyFields.add(keyName);
        }
        return Pair.of(columnDefs, keyFields);
    }

    private static ColumnDef toColumnDef(org.apache.avro.Schema schema) {
        String logicalType = schema.getProp(AVRO_LOGICAL_TYPE_PROP);
        switch (schema.getType()) {
            case BOOLEAN:
                return new ColumnDef(ColumnType.BOOLEAN);
            case INT:
                if (AVRO_LOGICAL_DATE.equalsIgnoreCase(logicalType)) {
                    return new ColumnDef(ColumnType.DATE);
                } else if (AVRO_LOGICAL_TIME_MILLIS.equalsIgnoreCase(logicalType)) {
                    return new ColumnDef(ColumnType.TIME);
                }
                return new ColumnDef(ColumnType.INT);
            case LONG:
                if (AVRO_LOGICAL_TIMESTAMP_MILLIS.equalsIgnoreCase(logicalType)) {
                    return new ColumnDef(ColumnType.TIMESTAMP);
                }
                return new ColumnDef(ColumnType.LONG);
            case FLOAT:
                return new ColumnDef(ColumnType.FLOAT);
            case DOUBLE:
                return new ColumnDef(ColumnType.DOUBLE);
            case BYTES:
            case FIXED:
                if (AVRO_LOGICAL_DECIMAL.equalsIgnoreCase(logicalType)) {
                    Object scaleObj = schema.getObjectProp(AVRO_LOGICAL_DECIMAL_SCALE_PROP);
                    if (!(scaleObj instanceof Number)) {
                        throw new IllegalArgumentException("scale must be specified and must be a number.");
                    }
                    int scale = ((Number) scaleObj).intValue();

                    Object precisionObj = schema.getObjectProp(AVRO_LOGICAL_DECIMAL_PRECISION_PROP);
                    int precision = 0;
                    if (precisionObj != null) {
                        if (!(precisionObj instanceof Number)) {
                            throw new IllegalArgumentException(AVRO_LOGICAL_DECIMAL_PRECISION_PROP
                                + " property must be an Integer."
                                + " https://avro.apache.org/docs/1.7.7/spec.html#Decimal");
                        }
                        precision = ((Number) precisionObj).intValue();
                    }
                    return new ColumnDef(ColumnType.DECIMAL, precision, scale);
                }
                return new ColumnDef(ColumnType.BYTES);
            case STRING:
                return new ColumnDef(ColumnType.STRING);
            case UNION:
                for (org.apache.avro.Schema subtype : schema.getTypes()) {
                    ColumnDef columnDef = toColumnDef(subtype);
                    if (columnDef != null) {
                        return new ColumnDef(columnDef.getColumnType(), ColumnStrategy.NULL_STRATEGY,
                            columnDef.getPrecision(), columnDef.getScale());
                    }
                }
                return null;
            default:
                return null;
        }
    }

    public static org.apache.avro.Schema toAvroSchema(String tableName, RelDef relDef) {
        SchemaBuilder.FieldAssembler<org.apache.avro.Schema> schemaBuilder =
            SchemaBuilder.record(tableName).fields();
        Map<String, Integer> keyIndices = Ord.zip(relDef.getKeyFields()).stream()
            .collect(Collectors.toMap(o -> o.e, o -> o.i));
        for (Pair<RelDataTypeField, ColumnStrategy> entry
            : Pair.zip(relDef.getRowType().getFieldList(), relDef.getStrategies())) {
            RelDataTypeField field = entry.left;
            ColumnStrategy strategy = entry.right;
            RelDataType type = field.getType();
            SchemaBuilder.FieldBuilder<org.apache.avro.Schema> fieldBuilder = schemaBuilder.name(field.getName());
            Integer keyIndex = keyIndices.get(field.getName());
            if (keyIndex != null) {
                fieldBuilder = fieldBuilder.prop(SQL_KEY_INDEX_PROP, keyIndex);
                // Keys are always NOT NULL
                strategy = ColumnStrategy.NOT_NULL_STRATEGY;
            }
            schemaBuilder = toAvroType(fieldBuilder, type, strategy);
        }
        return schemaBuilder.endRecord();
    }

    private static SchemaBuilder.FieldAssembler<org.apache.avro.Schema> toAvroType(
        SchemaBuilder.FieldBuilder<org.apache.avro.Schema> fieldBuilder,
        RelDataType type, ColumnStrategy strategy) {
        SchemaBuilder.FieldAssembler<org.apache.avro.Schema> schemaBuilder = null;
        switch (type.getSqlTypeName()) {
            case BOOLEAN:
                switch (strategy.getType()) {
                    case NOT_NULL:
                        schemaBuilder = fieldBuilder.type().booleanType().noDefault();
                        break;
                    case NULL:
                        schemaBuilder = fieldBuilder.type().optional().booleanType();
                        break;
                    case DEFAULT:
                        schemaBuilder = fieldBuilder.type().booleanType()
                            .booleanDefault((Boolean) strategy.getDefaultValue());
                        break;
                }
                break;
            case INTEGER:
                switch (strategy.getType()) {
                    case NOT_NULL:
                        schemaBuilder = fieldBuilder.type().intType().noDefault();
                        break;
                    case NULL:
                        schemaBuilder = fieldBuilder.type().optional().intType();
                        break;
                    case DEFAULT:
                        schemaBuilder = fieldBuilder.type().intType()
                            .intDefault(((Number) strategy.getDefaultValue()).intValue());
                        break;
                }
                break;
            case BIGINT:
                switch (strategy.getType()) {
                    case NOT_NULL:
                        schemaBuilder = fieldBuilder.type().longType().noDefault();
                        break;
                    case NULL:
                        schemaBuilder = fieldBuilder.type().optional().longType();
                        break;
                    case DEFAULT:
                        schemaBuilder = fieldBuilder.type().longType()
                            .longDefault(((Number) strategy.getDefaultValue()).longValue());
                        break;
                }
                break;
            case REAL:
                switch (strategy.getType()) {
                    case NOT_NULL:
                        schemaBuilder = fieldBuilder.type().floatType().noDefault();
                        break;
                    case NULL:
                        schemaBuilder = fieldBuilder.type().optional().floatType();
                        break;
                    case DEFAULT:
                        schemaBuilder = fieldBuilder.type().floatType()
                            .floatDefault(((Number) strategy.getDefaultValue()).floatValue());
                        break;
                }
                break;
            case DOUBLE:
                switch (strategy.getType()) {
                    case NOT_NULL:
                        schemaBuilder = fieldBuilder.type().doubleType().noDefault();
                        break;
                    case NULL:
                        schemaBuilder = fieldBuilder.type().optional().doubleType();
                        break;
                    case DEFAULT:
                        schemaBuilder = fieldBuilder.type().doubleType()
                            .doubleDefault(((Number) strategy.getDefaultValue()).doubleValue());
                        break;
                }
                break;
            case VARBINARY:
                switch (strategy.getType()) {
                    case NOT_NULL:
                        schemaBuilder = fieldBuilder.type().bytesType().noDefault();
                        break;
                    case NULL:
                        schemaBuilder = fieldBuilder.type().optional().bytesType();
                        break;
                    case DEFAULT:
                        schemaBuilder = fieldBuilder.type().bytesType()
                            .bytesDefault(strategy.getDefaultValue().toString());
                        break;
                }
                break;
            case VARCHAR:
                switch (strategy.getType()) {
                    case NOT_NULL:
                        schemaBuilder = fieldBuilder.type().stringType().noDefault();
                        break;
                    case NULL:
                        schemaBuilder = fieldBuilder.type().optional().stringType();
                        break;
                    case DEFAULT:
                        schemaBuilder = fieldBuilder.type().stringType()
                            .stringDefault(strategy.getDefaultValue().toString());
                        break;
                }
                break;
            case DECIMAL:
                int precision = type.getPrecision();
                int scale = type.getScale();
                LogicalType decimal = LogicalTypes.decimal(precision, Math.max(scale, 0));
                org.apache.avro.Schema bytesSchema = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES);
                org.apache.avro.Schema decimalSchema = decimal.addToSchema(bytesSchema);
                switch (strategy.getType()) {
                    case NOT_NULL:
                        schemaBuilder = fieldBuilder.type(decimalSchema).noDefault();
                        break;
                    case NULL:
                        schemaBuilder = fieldBuilder.type().optional().type(decimalSchema);
                        break;
                    case DEFAULT:
                        schemaBuilder = fieldBuilder.type(decimalSchema).withDefault(strategy.getDefaultValue());
                        break;
                }
                break;
            case DATE:
                LogicalType date = LogicalTypes.date();
                org.apache.avro.Schema intSchema = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT);
                org.apache.avro.Schema dateSchema = date.addToSchema(intSchema);
                switch (strategy.getType()) {
                    case NOT_NULL:
                        schemaBuilder = fieldBuilder.type(dateSchema).noDefault();
                        break;
                    case NULL:
                        schemaBuilder = fieldBuilder.type().optional().type(dateSchema);
                        break;
                    case DEFAULT:
                        schemaBuilder = fieldBuilder.type(dateSchema).withDefault(strategy.getDefaultValue());
                        break;
                }
                break;
            case TIME:
                LogicalType time = LogicalTypes.timeMillis();
                org.apache.avro.Schema int2Schema = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT);
                org.apache.avro.Schema timeSchema = time.addToSchema(int2Schema);
                switch (strategy.getType()) {
                    case NOT_NULL:
                        schemaBuilder = fieldBuilder.type(timeSchema).noDefault();
                        break;
                    case NULL:
                        schemaBuilder = fieldBuilder.type().optional().type(timeSchema);
                        break;
                    case DEFAULT:
                        schemaBuilder = fieldBuilder.type(timeSchema).withDefault(strategy.getDefaultValue());
                        break;
                }
                break;
            case TIMESTAMP:
                LogicalType timestamp = LogicalTypes.timestampMillis();
                org.apache.avro.Schema longSchema = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG);
                org.apache.avro.Schema timestampSchema = timestamp.addToSchema(longSchema);
                switch (strategy.getType()) {
                    case NOT_NULL:
                        schemaBuilder = fieldBuilder.type(timestampSchema).noDefault();
                        break;
                    case NULL:
                        schemaBuilder = fieldBuilder.type().optional().type(timestampSchema);
                        break;
                    case DEFAULT:
                        schemaBuilder = fieldBuilder.type(timestampSchema).withDefault(strategy.getDefaultValue());
                        break;
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported type " + type);
        }
        return schemaBuilder;
    }

    public static Comparable toAvroValue(org.apache.avro.Schema schema, Comparable value) {
        if (value == null) {
            return null;
        }
        String logicalType = schema.getProp(AVRO_LOGICAL_TYPE_PROP);
        switch (schema.getType()) {
            case BOOLEAN:
                return value;
            case INT:
                if (AVRO_LOGICAL_DATE.equalsIgnoreCase(logicalType)) {
                    return value;
                } else if (AVRO_LOGICAL_TIME_MILLIS.equalsIgnoreCase(logicalType)) {
                    return value;
                }
                return ((Number) value).intValue();
            case LONG:
                if (AVRO_LOGICAL_TIMESTAMP_MILLIS.equalsIgnoreCase(logicalType)) {
                    return value;
                }
                return ((Number) value).longValue();
            case FLOAT:
                return ((Number) value).floatValue();
            case DOUBLE:
                return ((Number) value).doubleValue();
            case BYTES:
            case FIXED:
                if (AVRO_LOGICAL_DECIMAL.equalsIgnoreCase(logicalType)) {
                    return value;
                }
                return ByteBuffer.wrap(((ByteString) value).getBytes());
            case STRING:
                return value;
            case UNION:
                for (org.apache.avro.Schema subtype : schema.getTypes()) {
                    Comparable v = toAvroValue(subtype, value);
                    if (v != null) {
                        return v;
                    }
                }
                return null;
            default:
                return null;
        }
    }

    public static Comparable fromAvroValue(org.apache.avro.Schema schema, Comparable value) {
        if (value instanceof Utf8) {
            value = ((Utf8) value).toString();
        } else if (value instanceof ByteBuffer) {
            value = new ByteString(((ByteBuffer) value).array());
        }
        return value;
    }
}
