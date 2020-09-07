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

import com.google.common.collect.ForwardingSortedSet;
import com.google.common.collect.Iterators;
import io.kareldb.version.TxVersionedCache;
import io.kareldb.version.VersionedCache;
import io.kareldb.version.VersionedValue;
import io.kcache.KeyValue;
import io.kcache.KeyValueIterator;
import io.kcache.KeyValueIterators;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.util.Pair;

import java.io.Closeable;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

@SuppressWarnings("unchecked")
public abstract class Table extends AbstractQueryableTable implements ModifiableTable, Closeable {

    public static final Comparable[] EMPTY_VALUE = new Comparable[0];

    private static final KeyValue<Comparable[], VersionedValue> EMPTY_KEY_VALUE =
        new KeyValue<>(null, null);

    private final Schema schema;
    private final String name;
    private RelDef relDef;
    private Map<String, Object> configs;
    private int[] permutationIndices;
    private int[] inverseIndices;

    Table(Schema schema, String name, RelDef relDef) {
        super(Object[].class);
        this.schema = schema;
        this.name = name;
        setRelDef(relDef);
    }

    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs) {
        this.configs = (Map<String, Object>) configs;
    }

    public abstract void init();

    public abstract void sync();

    public Schema getSchema() {
        return schema;
    }

    public Map<String, Object> getConfigs() {
        return configs;
    }

    public int[] getKeyIndices() {
        return Arrays.copyOf(getPermutationIndices(), getRelDef().getKeyFields().size());
    }

    // Defines how to permute the rowType so that the keyFields are stored as a prefix.
    private int[] getPermutationIndices() {
        if (permutationIndices == null) {
            int size = size();
            int[] result = new int[size];
            Set<Integer> keyIndices = new HashSet<>();
            int index = 0;
            for (String keyField : getRelDef().getKeyFields()) {
                keyIndices.add(index);
                result[index++] = getRelDef().getRowType().getField(keyField, true, false).getIndex();
            }
            for (int i = 0; i < size; i++) {
                if (!keyIndices.contains(i)) {
                    result[index++] = i;
                }
            }
            permutationIndices = result;
        }
        return permutationIndices;
    }

    // Defines how to permute the stored row to match the rowType.
    private int[] getInverseIndices() {
        if (inverseIndices == null) {
            int[] permutation = getPermutationIndices();
            int size = size();
            int[] result = new int[size];
            for (int i = 0; i < size; i++) {
                result[permutation[i]] = i;
            }
            inverseIndices = result;
        }
        return inverseIndices;
    }

    public int size() {
        return getRowType().getFieldCount();
    }

    @Override
    public TableModify toModificationRel(
        RelOptCluster cluster,
        RelOptTable table,
        Prepare.CatalogReader catalogReader,
        RelNode child,
        TableModify.Operation operation,
        List<String> updateColumnList,
        List<RexNode> sourceExpressionList,
        boolean flattened) {
        return LogicalTableModify.create(table, catalogReader, child, operation,
            updateColumnList, sourceExpressionList, flattened);
    }

    @Override
    public Collection getModifiableCollection() {
        return new CacheWrapper(getRows());
    }

    protected abstract VersionedCache getRows();

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
                                        SchemaPlus schema, String tableName) {
        return new AbstractTableQueryable<T>(queryProvider, schema, this, tableName) {
            public Enumerator<T> enumerator() {
                //noinspection unchecked
                return (Enumerator<T>) Linq4j.iterableEnumerator(
                    () -> Iterators.transform(getModifiableCollection().iterator(), Table::toArray));
            }
        };
    }

    public static Object[] toArray(Object o) {
        return o.getClass().isArray() ? (Comparable[]) o : new Comparable[]{(Comparable) o};
    }

    public String getName() {
        return name;
    }

    public RelDef getRelDef() {
        return relDef;
    }

    public void setRelDef(RelDef relDef) {
        this.relDef = relDef;
        this.permutationIndices = null;
        this.inverseIndices = null;
    }

    public RelDataType getRowType() {
        return getRelDef().getRowType();
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (getRelDef() == null) {
            return null;
        }
        RelProtoDataType protoRowType = RelDataTypeImpl.proto(getRowType());
        return protoRowType.apply(typeFactory);
    }

    /**
     * Returns an array of integers {0, ..., n - 1}.
     */
    public static int[] identityList(int n) {
        int[] integers = new int[n];
        for (int i = 0; i < n; i++) {
            integers[i] = i;
        }
        return integers;
    }

    public Pair<Comparable[], Comparable[]> toKeyValue(Object o) {
        if (!o.getClass().isArray()) {
            return new Pair<>(new Comparable[]{(Comparable) o}, EMPTY_VALUE);
        }
        Object[] objs = (Object[]) o;
        int size = size();
        int keySize = getRelDef().getKeyFields().size();
        int valueSize = size - keySize;
        Comparable[] keys = new Comparable[keySize];
        Comparable[] values = new Comparable[valueSize];
        int[] permutation = getPermutationIndices();
        int index = 0;
        for (int i = 0; i < keySize; i++) {
            keys[index++] = (Comparable) objs[permutation[i]];
        }
        index = 0;
        for (int i = keySize; i < size; i++) {
            values[index++] = (Comparable) objs[permutation[i]];
        }
        return new Pair<>(keys, values);
    }

    public Object toRow(KeyValue<Comparable[], VersionedValue> entry) {
        Comparable[] key = entry.key;
        Comparable[] values = entry.value.getValue();
        if (key.length == 1 && values.length == 0) {
            return key[0];
        }
        int[] inverse = getInverseIndices();
        Comparable[] row = new Comparable[inverse.length];
        for (int i = 0; i < inverse.length; i++) {
            int index = inverse[i];
            int valueIndex = index - key.length;
            row[i] = index < key.length
                ? key[index]
                : valueIndex < values.length ? values[valueIndex] : null;
        }
        return row;
    }

    public static class ComparableArrayComparator implements Comparator<Comparable[]>, Serializable {
        private static final long serialVersionUID = 6469375761922827109L;

        // Avro only supports nulls first since the default must correspond to the
        // first schema in the union.
        private final Comparator<Comparable> defaultComparator =
            Comparator.<Comparable>nullsFirst(Comparator.<Comparable>naturalOrder());

        @Override
        public int compare(Comparable[] o1, Comparable[] o2) {
            for (int i = 0; i < Math.min(o1.length, o2.length); i++) {
                int c = defaultComparator.compare(o1[i], o2[i]);
                if (c != 0) {
                    return c;
                }
            }
            return Integer.compare(o1.length, o2.length);
        }
    }

    interface CollectionWrapper extends Collection {

        CollectionWrapper singleton(Comparable[] key);

        CollectionWrapper subCollection(Comparable[] from, boolean fromInclusive, Comparable[] to, boolean toInclusive);
    }

    class CacheWrapper implements CollectionWrapper {

        private final TxVersionedCache cache;
        private int rowsAffected = 0;

        public CacheWrapper(VersionedCache cache) {
            this(new TxVersionedCache(cache));
        }

        protected CacheWrapper(TxVersionedCache cache) {
            this.cache = cache;
        }

        @Override
        public int size() {
            // size is currently called to find how many rows were affected by a DML operation
            return rowsAffected;
        }

        @Override
        public boolean isEmpty() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean contains(Object o) {
            Pair<Comparable[], Comparable[]> keyValue = toKeyValue(o);
            return cache.get(keyValue.left) != null;
        }

        @Override
        public Iterator iterator() {
            KeyValueIterator<Comparable[], VersionedValue> iter = cache.all();
            return KeyValueIterators.flatMap(
                KeyValueIterators.concat(iter,
                    KeyValueIterators.singletonIterator(EMPTY_KEY_VALUE)),
                keyValue -> {
                    if (keyValue == EMPTY_KEY_VALUE) {
                        iter.close();
                        return Collections.emptyIterator();
                    }
                    return Iterators.singletonIterator(toRow(keyValue));
                });
        }

        @Override
        public Object[] toArray() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object[] toArray(Object[] a) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean add(Object o) {
            if (o.getClass().isArray() && ((Object[]) o).length != Table.this.size()) {
                return update(o);
            } else {
                return insert(o);
            }
        }

        private boolean insert(Object o) {
            Pair<Comparable[], Comparable[]> keyValue = toKeyValue(o);
            cache.put(keyValue.left, keyValue.right);
            rowsAffected++;
            return true;
        }

        private boolean update(Object o) {
            // Handle UPDATE from EnumerableTableModifyExtension
            Pair<Comparable[], Comparable[]> keyValue = toKeyValue(o);
            Comparable[] oldKey = Arrays.copyOf(keyValue.left, keyValue.left.length);
            Comparable[] oldValue = Arrays.copyOf(keyValue.right, keyValue.right.length);

            int fieldCount = Table.this.size();
            Object[] values = (Object[]) o;
            int len = (values.length - fieldCount) / 2;
            Object[] putValues = Arrays.copyOfRange(values, fieldCount, fieldCount + len);
            Object[] colValues = Arrays.copyOfRange(values, fieldCount + len, values.length);

            int[] inverseIndices = getInverseIndices();
            int keySize = getRelDef().getKeyFields().size();
            for (int i = 0; i < putValues.length; i++) {
                Object colValue = putValues[i];
                String colName = colValues[i].toString();
                int fieldIndex = getRelDef().getRowType().getField(colName, true, false).getIndex();
                int inverseIndex = inverseIndices[fieldIndex];
                if (inverseIndex < keySize) {
                    keyValue.left[inverseIndex] = (Comparable) colValue;
                } else {
                    keyValue.right[inverseIndex - keySize] = (Comparable) colValue;
                }
            }
            boolean replaced = cache.replace(oldKey, oldValue, keyValue.left, keyValue.right);
            if (replaced) {
                rowsAffected++;
            }
            return replaced;
        }

        @Override
        public boolean remove(Object o) {
            Pair<Comparable[], Comparable[]> keyValue = toKeyValue(o);
            cache.remove(keyValue.left);
            rowsAffected++;
            return true;
        }

        @Override
        public boolean containsAll(Collection c) {
            for (Object o : c) {
                if (!contains(o)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public boolean addAll(Collection c) {
            for (Object o : c) {
                add(o);
            }
            return true;
        }

        @Override
        public boolean removeAll(Collection c) {
            for (Object o : c) {
                remove(o);
            }
            return true;
        }

        @Override
        public boolean retainAll(Collection c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }

        @Override
        public CollectionWrapper singleton(Comparable[] key) {
            CollectionWrapper singleton = new SetWrapper();
            VersionedValue value = cache.get(key);
            if (value != null) {
                Object row = toRow(new KeyValue<>(key, value));
                if (!row.getClass().isArray()) {
                    row = new Comparable[]{(Comparable) row};
                }
                singleton.add(row);
            }
            return singleton;
        }

        @Override
        public CollectionWrapper subCollection(Comparable[] from, boolean fromInclusive, Comparable[] to, boolean toInclusive) {
            return new CacheWrapper(cache.subCache(from, fromInclusive, to, toInclusive));
        }
    }

    class SetWrapper extends ForwardingSortedSet implements CollectionWrapper {

        private final SortedSet delegate;

        public SetWrapper() {
            this(new TreeSet(new ComparableArrayComparator()));
        }

        protected SetWrapper(SortedSet delegate) {
            this.delegate = delegate;
        }

        @Override
        protected SortedSet delegate() {
            return delegate;
        }

        @Override
        public Iterator iterator() {
            return Iterators.transform(delegate.iterator(), o ->
                o.getClass().isArray() && ((Comparable[]) o).length == 1 ? ((Comparable[]) o)[0] : o);
        }

        @Override
        public CollectionWrapper singleton(Comparable[] key) {
            SetWrapper singleton = new SetWrapper();
            if (contains(key)) {
                singleton.add(key);
            }
            return singleton;
        }

        @Override
        public CollectionWrapper subCollection(Comparable[] from, boolean fromInclusive, Comparable[] to, boolean toInclusive) {
            return new SetWrapper(subSet(from, to));
        }
    }
}
