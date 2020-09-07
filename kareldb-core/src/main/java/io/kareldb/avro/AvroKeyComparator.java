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

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryData;

import java.io.Serializable;
import java.util.Comparator;

public class AvroKeyComparator implements Comparator<byte[]>, Serializable {
    private static final long serialVersionUID = -6393807366276536251L;

    private final Schema schema;

    public AvroKeyComparator(Schema schema) {
        this.schema = schema;
    }

    @Override
    public int compare(byte[] b1, byte[] b2) {
        return BinaryData.compare(b1, 0, b1.length, b2, 0, b2.length, schema);
    }
}
