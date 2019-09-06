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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * These tests try to analyze the transactional anomalies described by P. Bailis et al. in
 * http://arxiv.org/pdf/1302.0309.pdf
 * <p>
 * These tests try to model what project Hermitage is trying to do to compare the behavior of different DBMSs on these
 * anomalies depending on the different isolation levels they offer. For more info on the Hermitage project, please
 * refer to: https://github.com/ept/hermitage
 * <p>
 * Transactional histories have been translated to HBase from the ones done for Postgresql in the Hermitage project:
 * https://github.com/ept/hermitage/blob/master/postgres.md
 * <p>
 * The "repeatable read" Postgresql isolation level is equivalent to "snapshot isolation", so we include the experiments
 * for that isolation level
 */
public class BailisAnomaliesTest {

    private static final Logger LOG = getLogger(BailisAnomaliesTest.class);

    private static final String TEST_TABLE = "test-table";

    // Data used in the tests
    private Comparable[] rowId1 = {"row1"};
    private Comparable[] rowId2 = {"row2"};
    private Comparable[] rowId3 = {"row3"};

    private Comparable[] dataValue1 = {10};
    private Comparable[] dataValue2 = {20};
    private Comparable[] dataValue3 = {30};

    private TransactionManager tm;
    private TxVersionedCache versionedCache;

    /**
     * This translates the table initialization done in:
     * https://github.com/ept/hermitage/blob/master/postgres.md
     * <p>
     * create table test (id int primary key, value int);
     * insert into test (id, value) values (1, 10), (2, 20);
     */
    @Before
    public void setUp() throws Exception {
        tm = KarelDbTransactionManager.newInstance();
        versionedCache = new TxVersionedCache(new VersionedCache(TEST_TABLE));

        Transaction tx = tm.begin();

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx);
        versionedCache.put(rowId1, dataValue1);
        versionedCache.put(rowId2, dataValue2);

        tm.commit(tx);
    }

    @After
    public void tearDown() throws Exception {
        tm.close();
    }

    @Test
    public void testSIPreventsPredicateManyPrecedersForReadPredicates() throws Exception {
        // TX History for PMP for Read Predicate:
        // begin; set transaction isolation level repeatable read; -- T1
        // begin; set transaction isolation level repeatable read; -- T2
        // select * from test where value = 30; -- T1. Returns nothing
        // insert into test (id, value) values(3, 30); -- T2
        // commit; -- T2
        // select * from test where value % 3 = 0; -- T1. Still returns nothing
        // commit; -- T1

        // 0) Start transactions
        Transaction tx1 = tm.begin();
        Transaction tx2 = tm.begin();

        // 1) select * from test where value = 30; -- T1. Returns nothing
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx1);
        assertNull(versionedCache.get(rowId3));

        // 2) insert into test (id, value) values(3, 30); -- T2
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx2);
        versionedCache.put(rowId3, dataValue3);

        // 3) Commit TX 2
        tm.commit(tx2);

        // 4) select * from test where value % 3 = 0; -- T1. Still returns nothing
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx1);
        assertNull(versionedCache.get(rowId3));

        // 5) Commit TX 1
        tm.commit(tx1);
    }

    @Test
    public void testSIPreventsPredicateManyPrecedersForWritePredicates() throws Exception {
        // TX History for PMP for Write Predicate:
        // begin; set transaction isolation level repeatable read; -- T1
        // begin; set transaction isolation level repeatable read; -- T2
        // update test set value = value + 10; -- T1
        // delete from test where value = 20; -- T2, BLOCKS
        // commit; -- T1. T2 now prints out "ERROR: could not serialize access due to concurrent update"
        // abort; -- T2. There's nothing else we can do, this transaction has failed

        // 0) Start transactions
        Transaction tx1 = tm.begin();
        Transaction tx2 = tm.begin();

        // 1) update test set value = value + 10; -- T1
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx1);
        versionedCache.replace(rowId1, dataValue1, dataValue2);
        versionedCache.replace(rowId2, dataValue2, dataValue3);

        // 2) delete from test where value = 20; -- T2, BLOCKS
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx2);
        versionedCache.remove(rowId1);

        // 3) commit TX 1
        try {
            tm.commit(tx1);
        } catch (RollbackException e) {
        }

        // 4) commit TX 2 -> Should be rolled-back
        try {
            tm.commit(tx2);
            fail();
        } catch (RollbackException e) {
            // Expected
        }
    }

    @Test
    public void testSIPreventsLostUpdates() throws Exception {
        // TX History for P4:
        // begin; set transaction isolation level repeatable read; -- T1
        // begin; set transaction isolation level repeatable read; -- T2
        // select * from test where id = 1; -- T1
        // select * from test where id = 1; -- T2
        // update test set value = 11 where id = 1; -- T1
        // update test set value = 11 where id = 1; -- T2, BLOCKS
        // commit; -- T1. T2 now prints out "ERROR: could not serialize access due to concurrent update"
        // abort;  -- T2. There's nothing else we can do, this transaction has failed

        // 0) Start transactions
        Transaction tx1 = tm.begin();
        Transaction tx2 = tm.begin();

        // 1) select * from test where id = 1; -- T1
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx1);
        assertArrayEquals(dataValue1, versionedCache.get(rowId1).getValue());

        // 2) select * from test where id = 1; -- T2
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx2);
        assertArrayEquals(dataValue1, versionedCache.get(rowId1).getValue());

        // 3) update test set value = 11 where id = 1; -- T1
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx1);
        versionedCache.replace(rowId1, dataValue1, new Comparable[]{11});

        // 4) update test set value = 11 where id = 1; -- T2
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx2);
        versionedCache.replace(rowId1, dataValue1, new Comparable[]{11});

        // 5) commit -- T1
        tm.commit(tx1);

        // 6) commit -- T2 --> should be rolled-back
        try {
            tm.commit(tx2);
            fail();
        } catch (RollbackException e) {
            // Expected
        }
    }

    @Test
    public void testSIPreventsReadSkew() throws Exception {
        // TX History for G-single:
        // begin; set transaction isolation level repeatable read; -- T1
        // begin; set transaction isolation level repeatable read; -- T2
        // select * from test where id = 1; -- T1. Shows 1 => 10
        // select * from test where id = 1; -- T2
        // select * from test where id = 2; -- T2
        // update test set value = 12 where id = 1; -- T2
        // update test set value = 18 where id = 2; -- T2
        // commit; -- T2
        // select * from test where id = 2; -- T1. Shows 2 => 20
        // commit; -- T1

        // 0) Start transactions
        Transaction tx1 = tm.begin();
        Transaction tx2 = tm.begin();

        // 1) select * from test where id = 1; -- T1. Shows 1 => 10
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx1);
        assertArrayEquals(dataValue1, versionedCache.get(rowId1).getValue());

        // 2) select * from test where id = 1; -- T2
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx2);
        assertArrayEquals(dataValue1, versionedCache.get(rowId1).getValue());

        // 3) select * from test where id = 2; -- T2
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx2);
        assertArrayEquals(dataValue2, versionedCache.get(rowId2).getValue());

        // 4) update test set value = 12 where id = 1; -- T2
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx2);
        versionedCache.replace(rowId1, dataValue1, new Comparable[]{12});

        // 5) update test set value = 18 where id = 2; -- T2
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx2);
        versionedCache.replace(rowId2, dataValue2, new Comparable[]{18});

        // 6) commit -- T2
        tm.commit(tx2);

        // 7) select * from test where id = 2; -- T1. Shows 2 => 20
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx1);
        assertArrayEquals(dataValue2, versionedCache.get(rowId2).getValue());

        // 8) commit -- T1
        tm.commit(tx1);
    }

    @Test
    public void testSIPreventsReadSkewUsingWritePredicate() throws Exception {
        // TX History for G-single:
        // begin; set transaction isolation level repeatable read; -- T1
        // begin; set transaction isolation level repeatable read; -- T2
        // select * from test where id = 1; -- T1. Shows 1 => 10
        // select * from test; -- T2
        // update test set value = 12 where id = 1; -- T2
        // update test set value = 18 where id = 2; -- T2
        // commit; -- T2
        // delete from test where value = 20; -- T1. Prints "ERROR: could not serialize access due to concurrent update"
        // abort; -- T1. There's nothing else we can do, this transaction has failed

        // 0) Start transactions
        Transaction tx1 = tm.begin();
        Transaction tx2 = tm.begin();

        // 1) select * from test; -- T1
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx1);
        assertEquals(2, versionedCache.size());

        // 2) select * from test; -- T2
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx2);
        assertEquals(2, versionedCache.size());

        // 3) update test set value = 12 where id = 1; -- T2
        // 4) update test set value = 18 where id = 2; -- T2
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx1);
        versionedCache.replace(rowId1, dataValue1, new Comparable[]{12});

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx2);
        versionedCache.replace(rowId2, dataValue2, new Comparable[]{18});

        // 5) commit; -- T2
        tm.commit(tx2);

        // 6) delete from test where value = 20; -- T1
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx1);
        versionedCache.remove(rowId2);

        // 7) abort; -- T1
        try {
            tm.commit(tx1);
            fail("Should be aborted");
        } catch (RollbackException e) {
            // Expected
        }
    }

    // this test shows that Omid does not provide serilizable level of isolation other wise last commit would have failed
    @Test
    public void testSIDoesNotPreventWriteSkew() throws Exception {
        // TX History for G2-item:
        // begin; set transaction isolation level repeatable read; -- T1
        // begin; set transaction isolation level repeatable read; -- T2
        // select * from test where id in (1,2); -- T1
        // select * from test where id in (1,2); -- T2
        // update test set value = 11 where id = 1; -- T1
        // update test set value = 21 where id = 2; -- T2
        // commit; -- T1
        // commit; -- T2

        // 0) Start transactions
        Transaction tx1 = tm.begin();
        Transaction tx2 = tm.begin();

        // 1) select * from test where id in (1,2); -- T1
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx1);
        assertEquals(2, Streams.streamOf(versionedCache.range(rowId1, true, rowId2, true)).count());

        // 2) select * from test where id in (1,2); -- T2
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx2);
        assertEquals(2, Streams.streamOf(versionedCache.range(rowId1, true, rowId2, true)).count());

        // 3) update test set value = 11 where id = 1; -- T1
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx1);
        versionedCache.replace(rowId1, dataValue1, new Comparable[]{11});

        // 4) update test set value = 21 where id = 2; -- T2
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx2);
        versionedCache.replace(rowId2, dataValue2, new Comparable[]{21});

        // 5) commit; -- T1
        tm.commit(tx1);

        // 6) commit; -- T2
        tm.commit(tx2);
    }

    // this test shows that Omid does not provide serilizable level of isolation other wise last commit would have failed
    @Test
    public void testSIDoesNotPreventAntiDependencyCycles() throws Exception {
        // TX History for G2:
        // begin; set transaction isolation level repeatable read; -- T1
        // begin; set transaction isolation level repeatable read; -- T2
        // select * from test where value % 3 = 0; -- T1
        // select * from test where value % 3 = 0; -- T2
        // insert into test (id, value) values(3, 30); -- T1
        // insert into test (id, value) values(4, 42); -- T2
        // commit; -- T1
        // commit; -- T2
        // select * from test where value % 3 = 0; -- Either. Returns 3 => 30, 4 => 42

        // 0) Start transactions
        Transaction tx1 = tm.begin();
        Transaction tx2 = tm.begin();

        // 1) select * from test where value % 3 = 0; -- T1
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx1);
        assertArrayEquals(dataValue1, versionedCache.get(rowId1).getValue());

        // 2) select * from test where value % 3 = 0; -- T2
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx2);
        assertArrayEquals(dataValue1, versionedCache.get(rowId1).getValue());

        // 3) insert into test (id, value) values(3, 30); -- T1
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx1);
        versionedCache.put(rowId3, dataValue3);

        // 4) insert into test (id, value) values(4, 42); -- T2
        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx2);
        versionedCache.put(new Comparable[]{"row4"}, new Comparable[]{42});

        // 5) commit; -- T1
        tm.commit(tx1);

        // 6) commit; -- T2
        tm.commit(tx2);

        // 7) select * from test where value % 3 = 0; -- Either. Returns 3 => 30, 4 => 42
    }
}
