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

import org.apache.calcite.linq4j.Enumerator;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

class TableEnumerator<E> implements Enumerator<E> {
    private final Iterator<?> rows;
    private final AtomicBoolean cancelFlag;
    private final boolean alwaysReturnArray;
    private E current;

    TableEnumerator(Iterator<?> rows, AtomicBoolean cancelFlag, boolean alwaysReturnArray) {
        this.rows = rows;
        this.cancelFlag = cancelFlag;
        this.alwaysReturnArray = alwaysReturnArray;
    }

    public E current() {
        return current;
    }

    public boolean moveNext() {
        if (cancelFlag.get()) {
            return false;
        }
        final Object[] row = rows.hasNext() ? Table.toArray(rows.next()) : null;
        if (row == null) {
            current = null;
            return false;
        }
        current = convertRow(row);
        return true;
    }

    @SuppressWarnings("unchecked")
    private E convertRow(Object[] row) {
        return alwaysReturnArray || row.length > 1 ? (E) row : (E) row[0];
    }

    public void reset() {
        throw new UnsupportedOperationException();
    }

    public void close() {
    }
}
