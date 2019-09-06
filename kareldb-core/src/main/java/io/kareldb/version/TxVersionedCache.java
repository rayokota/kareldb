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

package io.kareldb.version;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Striped;
import io.kareldb.transaction.client.KarelDbCellId;
import io.kareldb.transaction.client.KarelDbTransaction;
import io.kareldb.transaction.client.SnapshotFilter;
import io.kareldb.transaction.client.SnapshotFilterImpl;
import io.kcache.KeyValue;
import io.kcache.KeyValueIterator;
import io.kcache.utils.Streams;
import org.apache.omid.committable.CommitTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Collectors;

public class TxVersionedCache implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(TxVersionedCache.class);

    public static final long INVALID_TX = CommitTable.INVALID_TRANSACTION_MARKER;
    public static final long PENDING_TX = 0L;

    private final VersionedCache cache;
    private final boolean conflictFree;
    private final SnapshotFilter snapshotFilter;

    private final transient Striped<ReadWriteLock> striped;

    public TxVersionedCache(VersionedCache cache) {
        this(cache, false);
    }

    public TxVersionedCache(VersionedCache cache, boolean conflictFree) {
        this.cache = cache;
        this.conflictFree = conflictFree;
        this.snapshotFilter = new SnapshotFilterImpl(cache);
        this.striped = Striped.readWriteLock(128);
    }

    public String getName() {
        return cache.getName();
    }

    @VisibleForTesting
    public int size() {
        return Iterators.size(all());
    }

    @VisibleForTesting
    public boolean isEmpty() {
        return size() == 0;
    }

    public VersionedValue get(Comparable[] key) {
        List<VersionedValue> values = getAll(key);
        return values.size() > 0 ? values.get(0) : null;
    }

    public List<VersionedValue> getAll(Comparable[] key) {
        Lock lock = striped.get(key).readLock();
        lock.lock();
        try {
            KarelDbTransaction tx = KarelDbTransaction.currentTransaction();
            List<VersionedValue> values = snapshotFilter.get(tx, key);
            return values;
        } finally {
            lock.unlock();
        }
    }

    public void put(Comparable[] key, Comparable[] value) {
        Lock lock = striped.get(key).writeLock();
        lock.lock();
        try {
            KarelDbTransaction tx = KarelDbTransaction.currentTransaction();
            List<VersionedValue> values = snapshotFilter.get(tx, key);
            if (values.size() > 0) {
                throw new IllegalStateException("Primary key constraint violation" + Arrays.toString(key));
            }
            addWriteSetElement(tx, new KarelDbCellId(cache, key, tx.getWriteTimestamp()));
            cache.put(key, tx.getWriteTimestamp(), value);
        } finally {
            lock.unlock();
        }
    }

    public boolean replace(Comparable[] key, Comparable[] oldValue, Comparable[] newValue) {
        return replace(key, oldValue, key, newValue);
    }

    public boolean replace(Comparable[] oldKey, Comparable[] oldValue,
                           Comparable[] newKey, Comparable[] newValue) {
        Iterable<ReadWriteLock> locks = striped.bulkGet(ImmutableList.of(oldKey, newKey));
        List<Lock> writeLocks = Streams.streamOf(locks)
            .map(ReadWriteLock::writeLock)
            .collect(Collectors.toList());
        writeLocks.forEach(Lock::lock);
        try {
            KarelDbTransaction tx = KarelDbTransaction.currentTransaction();
            // Ensure the value hasn't changed
            List<VersionedValue> oldValues = snapshotFilter.get(tx, oldKey);
            VersionedValue oldVersionedValue = oldValues.size() > 0 ? oldValues.get(0) : null;
            if (oldVersionedValue == null || !Arrays.equals(oldValue, oldVersionedValue.getValue())) {
                throw new IllegalStateException("Previous value has changed");
            }
            if (Arrays.equals(oldKey, newKey)) {
                addWriteSetElement(tx, new KarelDbCellId(cache, newKey, tx.getWriteTimestamp()));
                cache.put(newKey, tx.getWriteTimestamp(), newValue);
                return true;
            } else {
                List<VersionedValue> newValues = snapshotFilter.get(tx, newKey);
                if (newValues.size() > 0) {
                    throw new IllegalStateException("Primary key constraint violation: " + Arrays.toString(newKey));
                }
                addWriteSetElement(tx, new KarelDbCellId(cache, oldKey, tx.getWriteTimestamp()));
                addWriteSetElement(tx, new KarelDbCellId(cache, newKey, tx.getWriteTimestamp()));
                cache.remove(oldKey, tx.getWriteTimestamp());
                cache.put(newKey, tx.getWriteTimestamp(), newValue);
                return true;
            }
        } finally {
            writeLocks.forEach(Lock::unlock);
        }
    }

    public void remove(Comparable[] key) {
        Lock lock = striped.get(key).writeLock();
        lock.lock();
        try {
            KarelDbTransaction tx = KarelDbTransaction.currentTransaction();
            addWriteSetElement(tx, new KarelDbCellId(cache, key, tx.getWriteTimestamp()));
            cache.remove(key, tx.getWriteTimestamp());
        } finally {
            lock.unlock();
        }
    }

    public TxVersionedCache subCache(Comparable[] from, boolean fromInclusive, Comparable[] to, boolean toInclusive) {
        return new TxVersionedCache(cache.subCache(from, fromInclusive, to, toInclusive));
    }

    public KeyValueIterator<Comparable[], VersionedValue> range(
        Comparable[] from, boolean fromInclusive, Comparable[] to, boolean toInclusive) {
        KarelDbTransaction tx = KarelDbTransaction.currentTransaction();
        return new FlattenedKeyValueIterator(snapshotFilter.range(tx, from, fromInclusive, to, toInclusive));
    }

    public KeyValueIterator<Comparable[], VersionedValue> all() {
        KarelDbTransaction tx = KarelDbTransaction.currentTransaction();
        return new FlattenedKeyValueIterator(snapshotFilter.all(tx));
    }

    private void addWriteSetElement(KarelDbTransaction transaction, KarelDbCellId cellId) {
        if (conflictFree) {
            transaction.addConflictFreeWriteSetElement(cellId);
        } else {
            transaction.addWriteSetElement(cellId);
        }
    }

    public void flush() {
        cache.flush();
    }

    public void close() throws IOException {
        cache.close();
    }

    private static class FlattenedKeyValueIterator implements KeyValueIterator<Comparable[], VersionedValue> {
        private final KeyValueIterator<Comparable[], List<VersionedValue>> rawIterator;
        private final Iterator<KeyValue<Comparable[], VersionedValue>> iterator;

        FlattenedKeyValueIterator(
            KeyValueIterator<Comparable[], List<VersionedValue>> iter) {
            this.rawIterator = iter;
            this.iterator = Streams.<KeyValue<Comparable[], List<VersionedValue>>>streamOf(iter)
                .flatMap(kv -> kv.value.stream().map(value -> new KeyValue<>(kv.key, value)))
                .filter(kv -> !kv.value.isDeleted())
                .iterator();
        }

        public final boolean hasNext() {
            return iterator.hasNext();
        }

        public final KeyValue<Comparable[], VersionedValue> next() {
            return iterator.next();
        }

        public final void remove() {
            throw new UnsupportedOperationException();
        }

        public final void close() {
            rawIterator.close();
        }
    }
}
