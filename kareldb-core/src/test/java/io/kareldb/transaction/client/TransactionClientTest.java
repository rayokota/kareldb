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

import com.google.common.collect.Maps;
import io.kareldb.transaction.InMemoryCommitTable;
import io.kareldb.transaction.InMemoryTimestampStorage;
import io.kareldb.version.TxVersionedCache;
import io.kareldb.version.VersionedCache;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.committable.CommitTable.CommitTimestamp;
import org.apache.omid.metrics.NullMetricsProvider;
import org.apache.omid.timestamp.storage.TimestampStorage;
import org.apache.omid.transaction.CommitTimestampLocator;
import org.apache.omid.transaction.PostCommitActions;
import org.apache.omid.tso.RuntimeExceptionPanicker;
import org.apache.omid.tso.TimestampOracle;
import org.apache.omid.tso.TimestampOracleImpl;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.omid.committable.CommitTable.CommitTimestamp.Location.CACHE;
import static org.apache.omid.committable.CommitTable.CommitTimestamp.Location.COMMIT_TABLE;
import static org.apache.omid.committable.CommitTable.CommitTimestamp.Location.SHADOW_CELL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class TransactionClientTest {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionClientTest.class);

    private static final String TEST_TABLE = "test-table";

    private Comparable[] rowId1 = {"row1"};
    private Comparable[] rowId2 = {"row2"};

    private Comparable[] dataValue1 = {"testWrite-1"};

    @Test
    public void testIsCommitted() throws Exception {
        KarelDbTransactionManager tm = KarelDbTransactionManager.newInstance();
        VersionedCache cache = new VersionedCache(TEST_TABLE);
        TxVersionedCache txCache = new TxVersionedCache(cache);
        SnapshotFilterImpl snapshotFilter = new SnapshotFilterImpl(cache);

        KarelDbTransaction t1 = (KarelDbTransaction) tm.begin();

        KarelDbTransaction.setCurrentTransaction(t1);
        txCache.put(rowId1, dataValue1);

        tm.commit(t1);

        KarelDbTransaction t2 = (KarelDbTransaction) tm.begin();

        KarelDbTransaction.setCurrentTransaction(t2);
        txCache.put(rowId2, dataValue1);

        KarelDbTransaction t3 = (KarelDbTransaction) tm.begin();

        KarelDbTransaction.setCurrentTransaction(t3);
        txCache.put(rowId2, dataValue1);

        tm.commit(t3);

        KarelDbCellId cellId1 = new KarelDbCellId(cache, rowId1, t1.getStartTimestamp());
        KarelDbCellId cellId2 = new KarelDbCellId(cache, rowId2, t2.getStartTimestamp());
        KarelDbCellId cellId3 = new KarelDbCellId(cache, rowId2, t3.getStartTimestamp());

        assertTrue("row1 should be committed", snapshotFilter.isCommitted(t1, cellId1));
        assertFalse("row2 should not be committed for kv2", snapshotFilter.isCommitted(t2, cellId2));
        assertTrue("row2 should be committed for kv3", snapshotFilter.isCommitted(t3, cellId3));
    }

    // Tests step 1 in AbstractTransactionManager.locateCellCommitTimestamp()
    @Test
    public void testCellCommitTimestampIsLocatedInCache() throws Exception {
        KarelDbTransactionManager tm = KarelDbTransactionManager.newInstance();
        VersionedCache cache = new VersionedCache(TEST_TABLE);
        TxVersionedCache txCache = new TxVersionedCache(cache);
        SnapshotFilterImpl snapshotFilter = new SnapshotFilterImpl(cache);

        KarelDbTransaction t1 = (KarelDbTransaction) tm.begin();

        final long CELL_ST = 1L;
        final long CELL_CT = 2L;

        KarelDbCellId cellId = new KarelDbCellId(cache, rowId1, CELL_ST);
        Map<Long, Long> fakeCache = Maps.newHashMap();
        fakeCache.put(CELL_ST, CELL_CT);

        // Then test that locator finds it in the cache
        CommitTimestampLocator ctLocator =
            new KarelDbTransactionManager.CommitTimestampLocatorImpl(cellId, fakeCache, cache);
        CommitTimestamp ct = snapshotFilter.locateCellCommitTimestamp(t1, CELL_ST, ctLocator);
        assertTrue(ct.isValid());
        assertEquals(ct.getValue(), CELL_CT);
        assertTrue(ct.getLocation().compareTo(CACHE) == 0);
    }

    // Tests step 2 in AbstractTransactionManager.locateCellCommitTimestamp()
    @Test
    public void testCellCommitTimestampIsLocatedInCommitTable() throws Exception {
        CommitTable commitTable = new InMemoryCommitTable();
        TimestampStorage timestampStorage = new InMemoryTimestampStorage();
        TimestampOracle timestampOracle = new TimestampOracleImpl(
            new NullMetricsProvider(), timestampStorage, new RuntimeExceptionPanicker());
        timestampOracle.initialize();
        PostCommitActions postCommitter = spy(new KarelDbSyncPostCommitter(commitTable.getClient()));
        VersionedCache cache = new VersionedCache(TEST_TABLE);
        TxVersionedCache txCache = new TxVersionedCache(cache);
        SnapshotFilterImpl snapshotFilter = new SnapshotFilterImpl(cache);
        KarelDbTransactionManager tm = KarelDbTransactionManager.newInstance(commitTable, timestampOracle, postCommitter);

        // The following line emulates a crash after commit that is observed in (*) below
        doThrow(new RuntimeException()).when(postCommitter).updateShadowCells(any(KarelDbTransaction.class));

        // Commit a transaction that is broken on commit to avoid
        // write to the shadow cells and avoid cleaning the commit table
        KarelDbTransaction tx1 = (KarelDbTransaction) tm.begin();

        KarelDbTransaction.setCurrentTransaction(tx1);
        txCache.put(rowId1, dataValue1);

        try {
            tm.commit(tx1);
        } catch (Exception e) { // (*) crash
            // Do nothing
        }

        // Test the locator finds the appropriate data in the commit table
        KarelDbCellId cellId = new KarelDbCellId(cache, rowId1, tx1.getStartTimestamp());
        CommitTimestampLocator ctLocator = new KarelDbTransactionManager.CommitTimestampLocatorImpl(cellId,
            Maps.<Long, Long>newHashMap(), cache);
        CommitTimestamp ct = snapshotFilter.locateCellCommitTimestamp(tx1, tx1.getStartTimestamp(), ctLocator);
        assertTrue(ct.isValid());
        long expectedCommitTS = tx1.getStartTimestamp() + CommitTable.MAX_CHECKPOINTS_PER_TXN;
        assertEquals(ct.getValue(), expectedCommitTS);
        assertTrue(ct.getLocation().compareTo(COMMIT_TABLE) == 0);
    }

    // Tests step 3 in AbstractTransactionManager.locateCellCommitTimestamp()
    @Test
    public void testCellCommitTimestampIsLocatedInShadowCells() throws Exception {
        KarelDbTransactionManager tm = KarelDbTransactionManager.newInstance();
        VersionedCache cache = new VersionedCache(TEST_TABLE);
        TxVersionedCache txCache = new TxVersionedCache(cache);
        SnapshotFilterImpl snapshotFilter = new SnapshotFilterImpl(cache);

        KarelDbTransaction tx1 = (KarelDbTransaction) tm.begin();

        KarelDbTransaction.setCurrentTransaction(tx1);
        txCache.put(rowId1, dataValue1);

        tm.commit(tx1);

        // Test the locator finds the appropriate data in the shadow cells
        KarelDbCellId cellId = new KarelDbCellId(cache, rowId1, tx1.getStartTimestamp());
        CommitTimestampLocator ctLocator = new KarelDbTransactionManager.CommitTimestampLocatorImpl(cellId,
            Maps.<Long, Long>newHashMap(), cache);
        CommitTimestamp ct = snapshotFilter.locateCellCommitTimestamp(tx1, tx1.getStartTimestamp(), ctLocator);
        assertTrue(ct.isValid());
        assertEquals(ct.getValue(), tx1.getCommitTimestamp());
        assertTrue(ct.getLocation().compareTo(SHADOW_CELL) == 0);
    }

    // Tests step 4 in AbstractTransactionManager.locateCellCommitTimestamp()
    // Note: this test is questionable, it is missing a commit
    @Test
    public void testCellFromTransactionInPreviousEpochGetsInvalidCommitTimestamp() throws Exception {
        CommitTable commitTable = new InMemoryCommitTable();
        TimestampStorage timestampStorage = new InMemoryTimestampStorage();
        TimestampOracle timestampOracle = new TimestampOracleImpl(
            new NullMetricsProvider(), timestampStorage, new RuntimeExceptionPanicker());
        timestampOracle.initialize();
        PostCommitActions postCommitter = spy(new KarelDbSyncPostCommitter(commitTable.getClient()));
        VersionedCache cache = new VersionedCache(TEST_TABLE);
        TxVersionedCache txCache = new TxVersionedCache(cache);
        SnapshotFilterImpl snapshotFilter = new SnapshotFilterImpl(cache);
        KarelDbTransactionManager tm = KarelDbTransactionManager.newInstance(commitTable, timestampOracle, postCommitter);

        final long CURRENT_EPOCH_FAKE = (System.currentTimeMillis() + 10000) * CommitTable.MAX_CHECKPOINTS_PER_TXN;

        // Commit a transaction to addColumn ST/CT in commit table
        KarelDbTransaction tx1 = spy((KarelDbTransaction) tm.begin());
        // Fake the current epoch to simulate a newer TSO
        doReturn(CURRENT_EPOCH_FAKE).when(tx1).getEpoch();

        KarelDbTransaction.setCurrentTransaction(tx1);
        txCache.put(rowId1, dataValue1);
        // Upon commit, the commit data should be in the shadow cells

        // Test a transaction in the previous epoch gets an InvalidCommitTimestamp class
        KarelDbCellId cellId = new KarelDbCellId(cache, rowId1, tx1.getStartTimestamp());
        CommitTimestampLocator ctLocator = new KarelDbTransactionManager.CommitTimestampLocatorImpl(cellId,
            Maps.<Long, Long>newHashMap(), cache);
        CommitTimestamp ct = snapshotFilter.locateCellCommitTimestamp(tx1, tx1.getStartTimestamp(), ctLocator);
        assertFalse(ct.isValid());
        assertEquals(ct.getValue(), CommitTable.INVALID_TRANSACTION_MARKER);
        assertTrue(ct.getLocation().compareTo(COMMIT_TABLE) == 0);
    }
}
