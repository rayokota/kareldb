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
package io.kareldb.csv;

import au.com.bytecode.opencsv.CSVReader;
import io.kareldb.KarelDbEngine;
import io.kareldb.schema.ColumnDef;
import io.kareldb.schema.ColumnType;
import io.kareldb.schema.FilterableTable;
import io.kareldb.schema.RelDef;
import io.kareldb.schema.Schema;
import io.kareldb.schema.Table;
import io.kareldb.version.VersionedCache;
import org.apache.calcite.avatica.util.Base64;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.omid.transaction.Transaction;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

/**
 * Base class for table that reads CSV files.
 */
public class CsvTable extends FilterableTable {
    private final VersionedCache rows;

    private static final FastDateFormat TIME_FORMAT_DATE;
    private static final FastDateFormat TIME_FORMAT_TIME;
    private static final FastDateFormat TIME_FORMAT_TIMESTAMP;

    static {
        final TimeZone gmt = TimeZone.getTimeZone("GMT");
        TIME_FORMAT_DATE = FastDateFormat.getInstance("yyyy-MM-dd", gmt);
        TIME_FORMAT_TIME = FastDateFormat.getInstance("HH:mm:ss", gmt);
        TIME_FORMAT_TIMESTAMP =
            FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", gmt);
    }

    /**
     * Creates a CsvTable.
     */
    public CsvTable(Schema schema, String name, RelDef rowType) {
        super(schema, name, rowType);
        this.rows = new VersionedCache(name);
    }

    @Override
    public VersionedCache getRows() {
        return rows;
    }

    @Override
    public void init() {
        final String fileName = (String) getConfigs().get("file");
        final Source source = getSource(getConfigs(), fileName);
        if (source == null) {
            return;
        }
        RelDataType rowType = getRowType();
        Collection modifiableCollection = getModifiableCollection();
        try (CSVReader reader = openCsv(source)) {
            Transaction tx = KarelDbEngine.getInstance().beginTx();
            reader.readNext(); // skip header row
            List<ColumnDef> columnDefs = new ArrayList<>(Schema.toColumnDefs(rowType).values());
            List<ColumnType> columnTypes = columnDefs.stream().map(ColumnDef::getColumnType).collect(Collectors.toList());
            RowConverter<?> rowConverter = converter(columnTypes);
            String[] strings = reader.readNext();
            while (strings != null) {
                Object row = rowConverter.convertRow(strings);
                //noinspection unchecked
                modifiableCollection.add(row);
                strings = reader.readNext();
            }
            KarelDbEngine.getInstance().commitTx(tx);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void sync() {
    }

    private Source getSource(Map<String, ?> operand, String fileName) {
        if (fileName == null) {
            return null;
        }
        Path path = Paths.get(fileName);
        final String directory = (String) operand.get("directory");
        if (directory != null) {
            path = Paths.get(directory, path.toString());
        }
        final File base = (File) operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);
        if (base != null) {
            path = Paths.get(base.getPath(), path.toString());
        }
        File file = path.toFile();
        return file.exists() ? Sources.of(path.toFile()) : null;
    }

    private static CSVReader openCsv(Source source) throws IOException {
        final Reader fileReader = source.reader();
        return new CSVReader(fileReader);
    }

    private static RowConverter<?> converter(List<ColumnType> fieldTypes) {
        final int[] fields = Table.identityList(fieldTypes.size());
        if (fields.length == 1) {
            final int field = fields[0];
            return new SingleColumnRowConverter(fieldTypes.get(field), field);
        } else {
            return new ArrayRowConverter(fieldTypes, fields);
        }
    }

    @Override
    public void close() {
    }

    /**
     * Row converter.
     *
     * @param <E> element type
     */
    abstract static class RowConverter<E> {
        abstract E convertRow(String[] rows);

        protected Object convert(ColumnType fieldType, String string) {
            if (fieldType == null) {
                return string;
            }
            switch (fieldType) {
                case BOOLEAN:
                    if (string.length() == 0) {
                        return null;
                    }
                    return Boolean.parseBoolean(string);
                case INT:
                    if (string.length() == 0) {
                        return null;
                    }
                    return Integer.parseInt(string);
                case LONG:
                    if (string.length() == 0) {
                        return null;
                    }
                    return Long.parseLong(string);
                case FLOAT:
                    if (string.length() == 0) {
                        return null;
                    }
                    return Float.parseFloat(string);
                case DOUBLE:
                    if (string.length() == 0) {
                        return null;
                    }
                    return Double.parseDouble(string);
                case BYTES:
                    if (string.length() == 0) {
                        return new ByteString(new byte[0]);
                    }
                    try {
                        return new ByteString(Base64.decode(string));
                    } catch (IOException e) {
                        return null;
                    }
                case DECIMAL:
                    if (string.length() == 0) {
                        return null;
                    }
                    return new BigDecimal(string);
                case DATE:
                    if (string.length() == 0) {
                        return null;
                    }
                    try {
                        Date date = TIME_FORMAT_DATE.parse(string);
                        return (int) (date.getTime() / DateTimeUtils.MILLIS_PER_DAY);
                    } catch (ParseException e) {
                        return null;
                    }
                case TIME:
                    if (string.length() == 0) {
                        return null;
                    }
                    try {
                        Date date = TIME_FORMAT_TIME.parse(string);
                        return (int) date.getTime();
                    } catch (ParseException e) {
                        return null;
                    }
                case TIMESTAMP:
                    if (string.length() == 0) {
                        return null;
                    }
                    try {
                        Date date = TIME_FORMAT_TIMESTAMP.parse(string);
                        return date.getTime();
                    } catch (ParseException e) {
                        return null;
                    }
                case STRING:
                default:
                    return string;
            }
        }
    }

    /**
     * Array row converter.
     */
    private static class ArrayRowConverter extends RowConverter<Object[]> {
        private final ColumnType[] fieldTypes;
        private final int[] fields;

        ArrayRowConverter(List<ColumnType> fieldTypes, int[] fields) {
            this.fieldTypes = fieldTypes.toArray(new ColumnType[0]);
            this.fields = fields;
        }

        public Object[] convertRow(String[] strings) {
            final Object[] objects = new Object[fields.length];
            for (int i = 0; i < fields.length; i++) {
                int field = fields[i];
                objects[i] = convert(fieldTypes[field], strings[field]);
            }
            return objects;
        }
    }

    /**
     * Single column row converter.
     */
    private static class SingleColumnRowConverter extends RowConverter<Object> {
        private final ColumnType fieldType;
        private final int fieldIndex;

        private SingleColumnRowConverter(ColumnType fieldType, int fieldIndex) {
            this.fieldType = fieldType;
            this.fieldIndex = fieldIndex;
        }

        public Object convertRow(String[] strings) {
            return convert(fieldType, strings[fieldIndex]);
        }
    }
}
