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

import com.google.common.collect.Iterators;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This table implements the {@link org.apache.calcite.schema.FilterableTable} interface, so Calcite gets
 * data by calling the {@link #scan(DataContext, List)} method.
 */
public abstract class FilterableTable extends Table
    implements org.apache.calcite.schema.FilterableTable {

    public FilterableTable(Schema schema, String name, RelDef rowType) {
        super(schema, name, rowType);
    }

    public String toString() {
        return "FilterableTable";
    }

    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
        CollectionWrapper rows = (CollectionWrapper) getModifiableCollection();
        for (RexNode filter : filters) {
            rows = scanFilterForKeyFields(root, filter, rows);
        }
        final Collection coll = rows;
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
        return new AbstractEnumerable<Object[]>() {
            @SuppressWarnings("unchecked")
            public Enumerator<Object[]> enumerator() {
                return new TableEnumerator<>(Iterators.<Object, Object[]>transform(
                    coll.iterator(), Table::toArray), cancelFlag, true);
            }
        };
    }

    private CollectionWrapper scanFilterForKeyFields(DataContext root, RexNode filter, CollectionWrapper rows) {
        Comparable[] keyValues = findKeyFieldAccess(root, filter, SqlKind.EQUALS);
        if (keyValues != null) {
            return rows.singleton(keyValues);
        } else {
            boolean fromInclusive = false;
            Comparable[] rangeMin = findKeyFieldAccess(root, filter, SqlKind.GREATER_THAN_OR_EQUAL);
            if (rangeMin == null) {
                rangeMin = findKeyFieldAccess(root, filter, SqlKind.GREATER_THAN);
            } else {
                fromInclusive = true;
            }

            boolean toInclusive = false;
            Comparable[] rangeMax = findKeyFieldAccess(root, filter, SqlKind.LESS_THAN_OR_EQUAL);
            if (rangeMax == null) {
                rangeMax = findKeyFieldAccess(root, filter, SqlKind.LESS_THAN);
            } else {
                toInclusive = true;
            }
            if (rangeMin != null || rangeMax != null) {
                return rows.subCollection(rangeMin, fromInclusive, rangeMax, toInclusive);
            }
        }
        return rows;
    }

    private Comparable[] findKeyFieldAccess(DataContext root, RexNode filter, SqlKind operator) {
        int[] keyIndices = getKeyIndices();
        List<String> keyFields = getRelDef().getKeyFields();
        Comparable[] keyValues = new Comparable[keyIndices.length];
        for (int i = 0; i < keyIndices.length; i++) {
            int keyIndex = keyIndices[i];
            String keyField = keyFields.get(i);
            ColumnType type = ColumnType.of(getRowType().getField(keyField, true, false).getType().getSqlTypeName());
            List<Comparable> values = scanFilterForKeyField(root, filter, keyIndex, operator, type);
            if (values.isEmpty()) {
                return null;
            }
            keyValues[i] = values.get(0);
        }
        return keyValues;
    }

    private List<Comparable> scanFilterForKeyField(DataContext root, RexNode filter, int keyIndex,
                                                   SqlKind operator, ColumnType type) {
        List<Comparable> values = new ArrayList<>();
        if (filter.isA(SqlKind.AND)) {
            ((RexCall) filter).getOperands().forEach(
                subFilter -> values.addAll(scanFilterForKeyField(root, subFilter, keyIndex, operator, type)));
        } else if (filter.isA(operator)) {
            final RexCall call = (RexCall) filter;
            RexNode left = call.getOperands().get(0);
            if (left.isA(SqlKind.CAST)) {
                left = ((RexCall) left).operands.get(0);
            }
            final RexNode right = call.getOperands().get(1);
            if (left instanceof RexInputRef) {
                if (right instanceof RexLiteral) {
                    final int index = ((RexInputRef) left).getIndex();
                    if (index == keyIndex) {
                        Comparable value = (Comparable) ((RexLiteral) right).getValueAs(type.getType());
                        return Collections.singletonList(value);
                    }
                } else if (right instanceof RexDynamicParam) {
                    Comparable value = (Comparable) root.get(((RexDynamicParam) right).getName());
                    return Collections.singletonList(value);
                }
            }
        }
        return values;
    }
}
