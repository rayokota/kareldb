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

import io.kareldb.version.TxVersionedCache;
import io.kareldb.version.VersionedCache;
import io.kcache.utils.Streams;
import org.apache.omid.transaction.Transaction;
import org.apache.omid.transaction.TransactionManager;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class UpdateScanTest {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateScanTest.class);

    private static final String TEST_TABLE = "test-table";

    private Comparable[] dataValue1 = {"testWrite-1"};

    @Test
    public void testGet() throws Exception {
        TransactionManager tm = KarelDbTransactionManager.newInstance();
        TxVersionedCache versionedCache = new TxVersionedCache(new VersionedCache(TEST_TABLE));

        Transaction t = tm.begin();

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t);
        int[] lInts = new int[]{100, 243, 2342, 22, 1, 5, 43, 56};
        for (int lInt : lInts) {
            versionedCache.put(new Comparable[]{lInt}, dataValue1);
        }

        Comparable[] key = {22};
        int count = (int) Streams.streamOf(versionedCache.range(key, true, key, true)).count();
        assertEquals("Count is wrong", count, 1);

        tm.commit(t);

        t = tm.begin();

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t);
        count = (int) Streams.streamOf(versionedCache.range(key, true, key, true)).count();
        assertEquals("Count is wrong", count, 1);

        tm.commit(t);
    }

    @Test
    public void testScan() throws Exception {
        TransactionManager tm = KarelDbTransactionManager.newInstance();
        TxVersionedCache versionedCache = new TxVersionedCache(new VersionedCache(TEST_TABLE));

        Transaction t = tm.begin();

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t);
        int[] lInts = new int[]{100, 243, 2342, 22, 1, 5, 43, 56};
        for (int lInt : lInts) {
            versionedCache.put(new Comparable[]{lInt}, dataValue1);
        }

        int count = (int) Streams.streamOf(versionedCache.all()).count();
        assertEquals("Count is wrong", count, lInts.length);

        tm.commit(t);

        t = tm.begin();

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t);
        count = (int) Streams.streamOf(versionedCache.all()).count();
        assertEquals("Count is wrong", count, lInts.length);

        tm.commit(t);
    }

    @Test
    public void testScanUncommitted() throws Exception {
        TransactionManager tm = KarelDbTransactionManager.newInstance();
        TxVersionedCache versionedCache = new TxVersionedCache(new VersionedCache(TEST_TABLE));

        Transaction t = tm.begin();

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t);
        int[] lIntsA = new int[]{100, 243, 2342, 22, 1, 5, 43, 56};
        for (int element : lIntsA) {
            versionedCache.put(new Comparable[]{element}, dataValue1);
        }

        tm.commit(t);

        Transaction tu = tm.begin();

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tu);
        int[] lIntsB = new int[]{105, 24, 4342, 32, 7, 3, 30, 40};
        for (int item : lIntsB) {
            versionedCache.put(new Comparable[]{item}, dataValue1);
        }

        t = tm.begin();

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t);
        int[] lIntsC = new int[]{109, 224, 242, 2, 16, 59, 23, 26};
        for (int value : lIntsC) {
            versionedCache.put(new Comparable[]{value}, dataValue1);
        }

        tm.commit(t);

        t = tm.begin();

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t);
        int count = (int) Streams.streamOf(versionedCache.all()).count();
        assertEquals("Count is wrong", count, lIntsA.length + lIntsC.length);
    }
}
