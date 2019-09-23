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

import io.kareldb.KarelDbEngine;
import io.kareldb.schema.FilterableTable;
import io.kareldb.schema.RelDef;
import io.kareldb.schema.Schema;
import io.kareldb.version.VersionedCache;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;
import org.apache.omid.transaction.Transaction;

import java.io.BufferedReader;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class AvroTable extends FilterableTable {
    private final VersionedCache rows;
    private final DecoderFactory decoderFactory = DecoderFactory.get();

    public AvroTable(Schema schema, String name, RelDef rowType) {
        super(schema, name, rowType);
        this.rows = new VersionedCache(name);
    }

    @Override
    public VersionedCache getRows() {
        return rows;
    }

    @Override
    public void init() {
        try {
            Transaction tx = KarelDbEngine.getInstance().beginTx();
            org.apache.avro.Schema schema = (org.apache.avro.Schema) getConfigs().get("avroSchema");
            if (schema == null) {
                schema = AvroSchema.toAvroSchema(getName(), getRelDef());
            }
            Collection modifiableCollection = getModifiableCollection();
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
            Source json = getSource(getConfigs(), schema.getName() + ".json");
            if (json != null) {
                BufferedReader reader = Files.newBufferedReader(Paths.get(json.path()));
                String line;
                while ((line = reader.readLine()) != null) {
                    Object row = toRow(datumReader.read(null, decoderFactory.jsonDecoder(schema, line)));
                    //noinspection unchecked
                    modifiableCollection.add(row);
                }
            }
            Source avro = getSource(getConfigs(), schema.getName() + ".avro");
            if (avro != null) {
                DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(avro.file(), datumReader);
                for (GenericRecord record : dataFileReader) {
                    Object row = toRow(record);
                    //noinspection unchecked
                    modifiableCollection.add(row);
                }
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

    private Object toRow(GenericRecord record) {
        List<org.apache.avro.Schema.Field> fields = record.getSchema().getFields();
        int size = fields.size();
        Object[] result = new Object[size];
        for (int i = 0; i < size; i++) {
            org.apache.avro.Schema schema = fields.get(i).schema();
            Comparable value = (Comparable) record.get(i);
            result[i] = AvroSchema.fromAvroValue(schema, value);
        }
        return size == 1 ? result[0] : result;
    }

    @Override
    public void close() {
    }
}
