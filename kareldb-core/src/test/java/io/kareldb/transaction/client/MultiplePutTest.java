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
import org.apache.omid.transaction.Transaction;
import org.apache.omid.transaction.TransactionManager;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

public class MultiplePutTest {

    private static final Logger LOG = LoggerFactory.getLogger(MultiplePutTest.class);

    private static final String TEST_TABLE = "test-table";

    @Test
    public void testManyManyPutsInDifferentRowsAreInTheTableAfterCommit() throws Exception {
        final int NUM_ROWS_TO_ADD = 50;

        TransactionManager tm = KarelDbTransactionManager.newInstance();
        TxVersionedCache versionedCache = new TxVersionedCache(new VersionedCache(TEST_TABLE));

        Transaction tx = tm.begin();

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx);
        for (int i = 0; i <= NUM_ROWS_TO_ADD; i++) {
            versionedCache.put(new Comparable[]{i}, new Comparable[]{"testData" + i});
        }

        tm.commit(tx);

        tx = tm.begin();
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx);
        assertArrayEquals(new Comparable[]{"testData" + 0}, versionedCache.get(new Comparable[]{0}).getValue());
        assertArrayEquals(new Comparable[]{"testData" + NUM_ROWS_TO_ADD}, versionedCache.get(new Comparable[]{NUM_ROWS_TO_ADD}).getValue());
    }

    @Test
    public void testGetFromNonExistentRowAfterMultiplePutsReturnsNoResult() throws Exception {
        final int NUM_ROWS_TO_ADD = 10;

        TransactionManager tm = KarelDbTransactionManager.newInstance();
        TxVersionedCache versionedCache = new TxVersionedCache(new VersionedCache(TEST_TABLE));

        Transaction tx = tm.begin();

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx);
        for (int i = 0; i <= NUM_ROWS_TO_ADD; i++) {
            versionedCache.put(new Comparable[]{i}, new Comparable[]{"testData" + i});
        }

        tm.commit(tx);

        tx = tm.begin();
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx);
        assertNull(versionedCache.get(new Comparable[]{NUM_ROWS_TO_ADD + 5}));
    }
}
