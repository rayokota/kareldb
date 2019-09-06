/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kareldb.transaction.client;

import com.google.common.hash.Funnel;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.hash.PrimitiveSink;
import io.kareldb.version.VersionedCache;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.omid.tso.client.CellId;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;

import static com.google.common.base.Charsets.UTF_8;

public class KarelDbCellId implements CellId {

    private final VersionedCache cache;
    private final Comparable[] key;
    private final long timestamp;

    public KarelDbCellId(VersionedCache cache, Comparable[] key, long timestamp) {
        this.timestamp = timestamp;
        this.cache = cache;
        this.key = key;
    }

    public VersionedCache getCache() {
        return cache;
    }

    public Comparable[] getKey() {
        return key;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return cache.getName()
            + ":" + Arrays.toString(key)
            + ":" + timestamp;
    }

    @Override
    public long getCellId() {
        return getHasher()
            .putBytes(cache.getName().getBytes(UTF_8))
            .putObject(key, new ComparableArrayFunnel())
            .hash().asLong();
    }

    @Override
    public long getTableId() {
        return getHasher()
            .putBytes(cache.getName().getBytes(UTF_8))
            .hash().asLong();
    }

    @Override
    public long getRowId() {
        return getHasher()
            .putBytes(cache.getName().getBytes(UTF_8))
            .putObject(key, new ComparableArrayFunnel())
            .hash().asLong();
    }

    public static Hasher getHasher() {
        return Hashing.murmur3_128().newHasher();
    }

    static class ComparableArrayFunnel implements Funnel<Comparable[]> {
        @Override
        public void funnel(Comparable[] array, PrimitiveSink into) {
            for (Comparable c : array) {
                if (c instanceof Boolean) {
                    into.putBoolean((Boolean) c);
                } else if (c instanceof Integer) {
                    into.putInt((Integer) c);
                } else if (c instanceof Long) {
                    into.putLong((Long) c);
                } else if (c instanceof Float) {
                    into.putFloat((Float) c);
                } else if (c instanceof Double) {
                    into.putDouble((Double) c);
                } else if (c instanceof ByteString) {
                    into.putBytes(((ByteString) c).getBytes());
                } else if (c instanceof String) {
                    into.putString((String) c, UTF_8);
                } else if (c instanceof BigDecimal) {
                    into.putDouble(((BigDecimal) c).doubleValue());
                } else if (c instanceof Date) {
                    into.putLong(((Date) c).getTime());
                } else if (c instanceof Time) {
                    into.putLong(((Time) c).getTime());
                } else if (c instanceof Timestamp) {
                    into.putLong(((Timestamp) c).getTime());
                } else {
                    throw new IllegalArgumentException("Unsupported object of type " + c.getClass().getName());
                }
            }
        }
    }
}
