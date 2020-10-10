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

import io.kareldb.KarelDbEngine;
import io.kareldb.kafka.serialization.KafkaKeySerde;
import io.kareldb.kafka.serialization.KafkaValueSerde;
import io.kareldb.kafka.serialization.KafkaValueSerializer;
import io.kareldb.schema.Table;
import io.kareldb.transaction.client.KarelDbTransactionManager;
import io.kcache.Cache;
import io.kcache.KeyValue;
import io.kcache.KeyValueIterator;
import io.kcache.utils.InMemoryCache;
import io.kcache.utils.Streams;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.omid.transaction.TransactionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

import static io.kareldb.schema.Table.EMPTY_VALUE;
import static io.kareldb.version.TxVersionedCache.INVALID_TX;
import static io.kareldb.version.TxVersionedCache.PENDING_TX;

public class VersionedCache implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(VersionedCache.class);

    private final String name;
    private final Cache<Comparable[], NavigableMap<Long, VersionedValue>> cache;
    private final KafkaKeySerde keySerde;
    private final KafkaValueSerde valueSerde;

    public VersionedCache(String name) {
        this(name, new InMemoryCache<>(new Table.ComparableArrayComparator()), null, null);
    }

    public VersionedCache(String name,
                          Cache<Comparable[], NavigableMap<Long, VersionedValue>> cache,
                          KafkaKeySerde keySerde,
                          KafkaValueSerde valueSerde) {
        this.name = name;
        this.cache = cache;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    public String getName() {
        return name;
    }

    public boolean keysEqual(Comparable[] key1, Comparable[] key2) {
        if (keySerde != null) {
            Serializer<Comparable[]> serializer = keySerde.serializer();
            return Arrays.equals(serializer.serialize(null, key1), serializer.serialize(null, key2));
        } else {
            return Arrays.equals(key1, key2);
        }
    }

    public boolean valuesEqual(Comparable[] value1, Comparable[] value2) {
        if (valueSerde != null) {
            KafkaValueSerializer serializer = (KafkaValueSerializer) valueSerde.serializer();
            return Arrays.equals(serializer.toAvroValues(value1), serializer.toAvroValues(value2));
        } else {
            return Arrays.equals(value1, value2);
        }
    }

    public VersionedValue get(Comparable[] key, long version) {
        NavigableMap<Long, VersionedValue> rowData = cache.get(key);
        return rowData != null ? rowData.get(version) : null;
    }

    public List<VersionedValue> get(Comparable[] key, long minVersion, long maxVersion) {
        NavigableMap<Long, VersionedValue> rowData = cache.get(key);
        return rowData != null ? getAll(rowData, minVersion, maxVersion) : Collections.emptyList();
    }

    private static List<VersionedValue> getAll(
        NavigableMap<Long, VersionedValue> rowdata, long minVersion, long maxVersion) {
        List<VersionedValue> all = new ArrayList<>(rowdata.subMap(minVersion, true, maxVersion, true)
            .descendingMap()
            .values());
        return all;
    }

    public void put(Comparable[] key, long version, Comparable[] value) {
        NavigableMap<Long, VersionedValue> rowData = cache.getOrDefault(key, new ConcurrentSkipListMap<>());
        rowData.put(version, new VersionedValue(version, PENDING_TX, false, value));
        garbageCollect(rowData);
        cache.put(key, rowData);
    }

    public boolean setCommit(Comparable[] key, long version, long commit) {
        NavigableMap<Long, VersionedValue> rowData = cache.getOrDefault(key, new ConcurrentSkipListMap<>());
        VersionedValue value = rowData.get(version);
        if (value == null) {
            return false;
        }
        if (commit == INVALID_TX) {
            rowData.remove(version);
        } else {
            rowData.put(version, new VersionedValue(version, commit, value.isDeleted(), value.getValue()));
        }
        garbageCollect(rowData);
        cache.put(key, rowData);
        return true;
    }

    public void remove(Comparable[] key, long version) {
        NavigableMap<Long, VersionedValue> rowData = cache.getOrDefault(key, new ConcurrentSkipListMap<>());
        rowData.put(version, new VersionedValue(version, PENDING_TX, true, EMPTY_VALUE));
        garbageCollect(rowData);
        cache.put(key, rowData);
    }

    private void garbageCollect(NavigableMap<Long, VersionedValue> rowData) {
        // Discard all entries strictly older than the low water mark except the most recent
        try {
            KarelDbTransactionManager txManager = KarelDbTransactionManager.getInstance();
            if (txManager != null) {  // allow null for tests
                long lowWaterMark = txManager.getLowWatermark();
                List<Long> oldVersions = new ArrayList<>(rowData.headMap(lowWaterMark).keySet());
                if (oldVersions.size() > 1) {
                    for (int i = 0; i < oldVersions.size() - 1; i++) {
                        rowData.remove(oldVersions.get(i));
                    }
                }
            }
        } catch (TransactionException e) {
            throw new RuntimeException(e);
        }
    }

    public VersionedCache subCache(Comparable[] from, boolean fromInclusive, Comparable[] to, boolean toInclusive) {
        return new VersionedCache(name, cache.subCache(from, fromInclusive, to, toInclusive), keySerde, valueSerde);
    }

    public KeyValueIterator<Comparable[], List<VersionedValue>> range(
        Comparable[] from, boolean fromInclusive, Comparable[] to, boolean toInclusive, long minVersion, long maxVersion) {
        return new VersionedKeyValueIterator(cache.range(from, fromInclusive, to, toInclusive), minVersion, maxVersion);
    }

    public KeyValueIterator<Comparable[], List<VersionedValue>> all(long minVersion, long maxVersion) {
        return new VersionedKeyValueIterator(cache.all(), minVersion, maxVersion);
    }

    public void flush() {
        cache.flush();
    }

    public void close() throws IOException {
        cache.close();
    }

    private static class VersionedKeyValueIterator implements KeyValueIterator<Comparable[], List<VersionedValue>> {
        private final KeyValueIterator<Comparable[], NavigableMap<Long, VersionedValue>> rawIterator;
        private final Iterator<KeyValue<Comparable[], List<VersionedValue>>> iterator;

        VersionedKeyValueIterator(
            KeyValueIterator<Comparable[], NavigableMap<Long, VersionedValue>> iter,
            long minVersion, long maxVersion) {
            this.rawIterator = iter;
            this.iterator = Streams.<KeyValue<Comparable[], NavigableMap<Long, VersionedValue>>>streamOf(iter)
                .flatMap(kv -> {
                    List<VersionedValue> values = getAll(kv.value, minVersion, maxVersion);
                    return values.isEmpty() ? Stream.empty() : Stream.of(new KeyValue<>(kv.key, values));
                })
                .iterator();
        }

        public final boolean hasNext() {
            return iterator.hasNext();
        }

        public final KeyValue<Comparable[], List<VersionedValue>> next() {
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
