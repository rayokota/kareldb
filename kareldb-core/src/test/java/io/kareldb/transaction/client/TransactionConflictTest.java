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
import org.apache.omid.transaction.RollbackException;
import org.apache.omid.transaction.Transaction;
import org.apache.omid.transaction.TransactionManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class TransactionConflictTest {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionConflictTest.class);

    private static final String TEST_TABLE = "test-table";
    private static final String TEST_TABLE2 = "test-table2";

    private Comparable[] rowId1 = {"row1"};
    private Comparable[] rowId2 = {"row2"};

    private Comparable[] dataValue1 = {"testWrite-1"};
    private Comparable[] dataValue2 = {"testWrite-2"};

    private TransactionManager tm;
    private TxVersionedCache versionedCache;
    private TxVersionedCache versionedCache2;

    @Before
    public void setUp() throws Exception {
        tm = KarelDbTransactionManager.newInstance();
        versionedCache = new TxVersionedCache(new VersionedCache(TEST_TABLE));
        versionedCache2 = new TxVersionedCache(new VersionedCache(TEST_TABLE2));
    }

    @After
    public void tearDown() throws Exception {
        tm.close();
    }

    @Test
    public void runTestWriteWriteConflict() throws Exception {
        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        Transaction t2 = tm.begin();
        LOG.info("Transaction created " + t2);

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t1);
        versionedCache.put(rowId1, dataValue1);

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t2);
        versionedCache.put(rowId1, dataValue2);

        tm.commit(t2);

        try {
            tm.commit(t1);
            fail("Transaction should not commit successfully");
        } catch (RollbackException e) {
        }
    }

    @Test
    public void runTestMultiTableConflict() throws Exception {
        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        Transaction t2 = tm.begin();
        LOG.info("Transaction created " + t2);

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t1);
        versionedCache.put(rowId1, dataValue1);
        versionedCache2.put(rowId1, dataValue1);

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t2);
        versionedCache.put(rowId1, dataValue2);
        versionedCache2.put(rowId1, dataValue2);

        tm.commit(t2);

        boolean aborted = false;
        try {
            tm.commit(t1);
            fail("Transaction committed successfully");
        } catch (RollbackException e) {
            aborted = true;
        }
        assertTrue("Transaction didn't raise exception", aborted);

        Transaction t3 = tm.begin();
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t3);
        assertArrayEquals(dataValue2, versionedCache2.get(rowId1).getValue());
    }

    @Test
    public void runTestCleanupAfterConflict() throws Exception {
        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        Transaction t2 = tm.begin();
        LOG.info("Transaction created " + t2);

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t1);
        versionedCache.put(rowId1, dataValue1);

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t2);
        versionedCache.put(rowId1, dataValue2);

        tm.commit(t1);

        boolean aborted = false;
        try {
            tm.commit(t2);
            fail("Transaction committed successfully");
        } catch (RollbackException e) {
            aborted = true;
        }
        assertTrue("Transaction didn't raise exception", aborted);

        Transaction t3 = tm.begin();
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t3);
        assertArrayEquals(dataValue1, versionedCache.get(rowId1).getValue());
    }

    @Test
    public void testCleanupWithDeleteRow() throws Exception {
        int rowcount = 10;
        int count = 0;

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t1);
        for (int i = 0; i < rowcount; i++) {
            versionedCache.put(new Comparable[]{"test-del" + i}, dataValue1);
        }
        tm.commit(t1);

        Transaction t2 = tm.begin();
        LOG.info("Transaction created " + t2);

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t2);
        versionedCache.remove(new Comparable[]{"test-del" + 3});

        count = countRows(versionedCache);
        assertEquals("Wrong count", count, rowcount - 1);

        Transaction t3 = tm.begin();
        LOG.info("Transaction created " + t3);

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t3);
        versionedCache.replace(new Comparable[]{"test-del" + 3}, dataValue1, dataValue2);

        tm.commit(t3);

        boolean aborted = false;
        try {
            tm.commit(t2);
            fail("Didn't abort");
        } catch (RollbackException e) {
            aborted = true;
        }
        assertTrue("Didn't raise exception", aborted);

        Transaction tscan = tm.begin();

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tscan);
        count = countRows(versionedCache);
        assertEquals("Wrong count", count, rowcount);
    }

    public int countRows(TxVersionedCache cache) {
        return (int) Streams.streamOf(cache.all()).count();
    }

    @Test
    public void testBatchedCleanup() throws Exception {
        int rowcount = 10;
        int count = 0;

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        Transaction t2 = tm.begin();
        LOG.info("Transaction created " + t2);

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t1);
        versionedCache.put(rowId1, dataValue1);

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t2);
        versionedCache.put(rowId1, dataValue2);

        //Add more rows to hit batch
        for (int i = 0; i < rowcount; i++) {
            versionedCache.put(new Comparable[]{"test-del" + i}, dataValue2);
            versionedCache2.put(new Comparable[]{"test-del" + i}, dataValue2);
        }

        // validate rows are really written
        assertEquals("Unexpected size for read.", countRows(versionedCache), rowcount + 1);
        assertEquals("Unexpected size for read.", countRows(versionedCache2), rowcount);

        tm.commit(t1);

        boolean aborted = false;
        try {
            tm.commit(t2);
            fail("Transaction commited successfully");
        } catch (RollbackException e) {
            aborted = true;
        }
        assertTrue("Transaction didn't raise exception", aborted);

        Transaction tscan = tm.begin();

        // validate rows are cleaned
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tscan);
        assertEquals("Unexpected size for read.", countRows(versionedCache), 1);
        assertEquals("Unexpected size for read.", countRows(versionedCache2), 0);
    }
}
