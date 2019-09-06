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
import io.kareldb.schema.ColumnDef;
import io.kareldb.schema.ColumnType;
import io.kareldb.schema.RelDef;
import io.kareldb.schema.Schema;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.ddl.SqlAlterTableExtension;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Schema mapped onto a directory of CSV files. Each table in the schema
 * is a CSV file in that directory.
 */
public class CsvSchema extends Schema {
    private final Map<String, org.apache.calcite.schema.Table> tableMap;
    private File directoryFile;

    /**
     * Creates a CSV schema.
     */
    public CsvSchema() {
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
        // Look for files in the directory ending in ".csv", ".csv.gz"
        final Source baseSource = Sources.of(directoryFile);
        File[] files = directoryFile.listFiles((dir, name) -> {
            final String nameSansGz = trim(name, ".gz");
            return nameSansGz.endsWith(".csv");
        });
        if (files == null) {
            System.out.println("directory " + directoryFile + " not found");
            files = new File[0];
        }
        Map<String, Object> configs = new HashMap<>(getConfigs());
        // Build a map from table name to table; each file becomes a table.
        for (File file : files) {
            Source source = Sources.of(file);
            Source sourceSansGz = source.trim(".gz");
            final Source sourceSansCsv = sourceSansGz.trimOrNull(".csv");
            if (sourceSansCsv != null) {
                configs.put("file", source.file().getName());
                String name = sourceSansCsv.relative(baseSource).path();
                RelDataType rowType = getRowType(source);
                List<String> keyFields = Collections.singletonList(rowType.getFieldNames().get(0));
                RelDef relDef = new RelDef(rowType, keyFields, Collections.emptyList());
                createTable(name, configs, relDef);
            }
        }
    }

    @Override
    public void createTable(String tableName,
                            Map<String, Object> operand,
                            RelDef rowType) {
        CsvTable table = new CsvTable(this, tableName, rowType);
        table.configure(operand != null ? operand : getConfigs());
        table.init();
        tableMap.put(tableName, table);
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

    /**
     * Looks for a suffix on a string and returns
     * either the string with the suffix removed
     * or the original string.
     */
    private static String trim(String s, String suffix) {
        String trimmed = trimOrNull(s, suffix);
        return trimmed != null ? trimmed : s;
    }

    /**
     * Looks for a suffix on a string and returns
     * either the string with the suffix removed
     * or null.
     */
    private static String trimOrNull(String s, String suffix) {
        return s.endsWith(suffix)
            ? s.substring(0, s.length() - suffix.length())
            : null;
    }

    public static RelDataType getRowType(Source source) {
        try (CSVReader reader = openCsv(source)) {
            String[] strings = reader.readNext(); // get header row
            LinkedHashMap<String, ColumnDef> types = toColumnDefs(strings);
            return Schema.toRowType(types, Collections.emptyList()).getRowType();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static CSVReader openCsv(Source source) throws IOException {
        final Reader fileReader = source.reader();
        return new CSVReader(fileReader);
    }

    private static LinkedHashMap<String, ColumnDef> toColumnDefs(String[] strings) {
        final LinkedHashMap<String, ColumnDef> columnDefs = new LinkedHashMap<>();
        if (strings == null) {
            strings = new String[]{"EmptyFileHasNoColumns:boolean"};
        }
        for (String string : strings) {
            final String name;
            final ColumnDef columnDef;
            final int colon = string.indexOf(':');
            if (colon >= 0) {
                name = string.substring(0, colon);
                String typeString = string.substring(colon + 1);
                columnDef = new ColumnDef(ColumnType.of(typeString));
                if (columnDef == null) {
                    System.out.println("WARNING: Found unknown type: "
                        + typeString + " in file: "
                        + " for column: " + name
                        + ". Will assume the type of column is string");
                }
            } else {
                name = string;
                columnDef = null;
            }
            columnDefs.put(name, columnDef);
        }
        if (columnDefs.isEmpty()) {
            columnDefs.put("line", null);
        }
        return columnDefs;
    }
}
