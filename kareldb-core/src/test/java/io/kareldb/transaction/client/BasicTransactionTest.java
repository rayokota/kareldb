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
import io.kareldb.version.VersionedValue;
import io.kcache.KeyValue;
import org.apache.omid.transaction.RollbackException;
import org.apache.omid.transaction.Transaction;
import org.apache.omid.transaction.TransactionManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class BasicTransactionTest {

    private static final Logger LOG = LoggerFactory.getLogger(BasicTransactionTest.class);

    private static final String TEST_TABLE = "test-table";

    private Comparable[] rowId1 = {"row1"};
    private Comparable[] rowId2 = {"row2"};

    private Comparable[] dataValue1 = {"testWrite-1"};
    private Comparable[] dataValue2 = {"testWrite-2"};

    private TransactionManager tm;
    private TxVersionedCache versionedCache;

    @Before
    public void setUp() throws Exception {
        tm = KarelDbTransactionManager.newInstance();
        versionedCache = new TxVersionedCache(new VersionedCache(TEST_TABLE));
    }

    @After
    public void tearDown() throws Exception {
        tm.close();
    }

    @Test
    public void testTimestampsOfTwoRowsInsertedAfterCommitOfSingleTransactionAreEquals() throws Exception {

        Transaction tx1 = tm.begin();

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx1);
        versionedCache.put(rowId1, dataValue1);
        versionedCache.put(rowId2, dataValue2);

        tm.commit(tx1);

        Transaction tx2 = tm.begin();

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx2);
        VersionedValue v1 = versionedCache.get(rowId1);
        VersionedValue v2 = versionedCache.get(rowId2);

        assertArrayEquals(dataValue1, v1.getValue());
        assertArrayEquals(dataValue2, v2.getValue());

        assertEquals(v1.getCommit(), v2.getCommit());
    }

    @Test
    public void runTestSimple() throws Exception {

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t1);
        versionedCache.put(rowId1, dataValue1);

        tm.commit(t1);

        Transaction tread = tm.begin();
        Transaction t2 = tm.begin();

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t2);
        versionedCache.replace(rowId1, dataValue1, dataValue2);

        tm.commit(t2);

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tread);
        VersionedValue v1 = versionedCache.get(rowId1);
        assertArrayEquals(dataValue1, v1.getValue());
    }

    @Test
    public void runTestManyVersions() throws Exception {

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t1);
        versionedCache.put(rowId1, dataValue1);

        tm.commit(t1);

        for (int i = 0; i < 5; ++i) {
            Transaction t2 = tm.begin();
            KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t2);
            versionedCache.replace(rowId1, dataValue1, dataValue2);
        }

        Transaction tread = tm.begin();

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tread);
        VersionedValue v1 = versionedCache.get(rowId1);
        assertArrayEquals(dataValue1, v1.getValue());
    }

    @Test
    public void runTestInterleave() throws Exception {

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t1);
        versionedCache.put(rowId1, dataValue1);

        tm.commit(t1);

        Transaction t2 = tm.begin();

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t2);
        versionedCache.replace(rowId1, dataValue1, dataValue2);

        Transaction tread = tm.begin();

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tread);
        VersionedValue v1 = versionedCache.get(rowId1);
        assertArrayEquals(dataValue1, v1.getValue());

        try {
            tm.commit(t2);
        } catch (RollbackException e) {
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSameCommitRaisesException() throws Exception {
        Transaction t1 = tm.begin();
        tm.commit(t1);
        tm.commit(t1);
    }

    @Test
    public void testInterleavedScanReturnsTheRightSnapshotResults() throws Exception {

        Comparable[] startRow = {"row-to-scan" + 0};
        Comparable[] stopRow = {"row-to-scan" + 9};
        Comparable[] randomRow = {"row-to-scan" + 3};

        // Add some data transactionally to have an initial state for the test
        Transaction tx1 = tm.begin();
        for (int i = 0; i < 10; i++) {
            Comparable[] row = {"row-to-scan" + i};
            KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx1);
            versionedCache.put(row, dataValue1);
        }
        tm.commit(tx1);

        // Start a second transaction -Tx2- modifying a random row and check that a concurrent transactional context
        // that scans the table, gets the proper snapshot with the stuff written by Tx1
        Transaction tx2 = tm.begin();
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx2);
        versionedCache.replace(randomRow, dataValue1, dataValue2);

        Transaction scanTx = tm.begin(); // This is the concurrent transactional scanner
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) scanTx);

        Iterator<KeyValue<Comparable[], VersionedValue>> iter = versionedCache.range(startRow, true, stopRow, true);
        while (iter.hasNext()) {
            KeyValue<Comparable[], VersionedValue> kv = iter.next();
            assertArrayEquals(dataValue1, kv.value.getValue());
        }

        // Commit the Tx2 and then check that under a new transactional context, the scanner gets the right snapshot,
        // which must include the row modified by Tx2
        tm.commit(tx2);

        int modifiedRows = 0;
        Transaction newScanTx = tm.begin();
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) newScanTx);
        Iterator<KeyValue<Comparable[], VersionedValue>> newIter = versionedCache.range(startRow, true, stopRow, true);
        while (newIter.hasNext()) {
            KeyValue<Comparable[], VersionedValue> kv = newIter.next();
            if (Arrays.equals(dataValue2, kv.value.getValue())) {
                modifiedRows++;
            }
        }
        assertEquals("Expected 1 row modified, but " + modifiedRows + " are.", modifiedRows, 1);

        // Finally, check that the Scanner Iterator does not implement the remove method
        newIter = versionedCache.range(startRow, true, stopRow, true);
        try {
            newIter.remove();
            fail();
        } catch (RuntimeException re) {
            // Expected
        }
    }

    @Test
    public void testInterleavedScanReturnsTheRightSnapshotResultsWhenATransactionAborts()
        throws Exception {

        Comparable[] startRow = {"row-to-scan" + 0};
        Comparable[] stopRow = {"row-to-scan" + 9};
        Comparable[] randomRow = {"row-to-scan" + 3};

        // Add some data transactionally to have an initial state for the test
        Transaction tx1 = tm.begin();
        for (int i = 0; i < 10; i++) {
            Comparable[] row = {"row-to-scan" + i};
            KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx1);
            versionedCache.put(row, dataValue1);
        }
        tm.commit(tx1);

        // Start a second transaction modifying a random row and check that a transactional scanner in Tx2 gets the
        // right snapshot with the new value in the random row just written by Tx2
        Transaction tx2 = tm.begin();
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx2);
        versionedCache.replace(randomRow, dataValue1, dataValue2);

        int modifiedRows = 0;
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx2);
        Iterator<KeyValue<Comparable[], VersionedValue>> newIter = versionedCache.range(startRow, true, stopRow, true);
        while (newIter.hasNext()) {
            KeyValue<Comparable[], VersionedValue> kv = newIter.next();
            if (Arrays.equals(dataValue2, kv.value.getValue())) {
                modifiedRows++;
            }
        }
        assertEquals("Expected 1 row modified, but " + modifiedRows + " are.", modifiedRows, 1);

        // Rollback the second transaction and then check that under a new transactional scanner we get the snapshot
        // that includes the only the initial rows put by Tx1
        tm.rollback(tx2);

        Transaction txScan = tm.begin(); // This is the concurrent transactional scanner
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) txScan);

        Iterator<KeyValue<Comparable[], VersionedValue>> iter = versionedCache.range(startRow, true, stopRow, true);
        while (iter.hasNext()) {
            KeyValue<Comparable[], VersionedValue> kv = iter.next();
            assertArrayEquals(dataValue1, kv.value.getValue());
        }

        // Finally, check that the Scanner Iterator does not implement the remove method
        newIter = versionedCache.range(startRow, true, stopRow, true);
        try {
            newIter.remove();
            fail();
        } catch (RuntimeException re) {
            // Expected
        }
    }
}
