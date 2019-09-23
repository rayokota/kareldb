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
package io.kareldb.transaction;

import com.google.common.collect.Sets;
import io.kcache.utils.InMemoryCache;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.metrics.NullMetricsProvider;
import org.apache.omid.timestamp.storage.TimestampStorage;
import org.apache.omid.tso.RuntimeExceptionPanicker;
import org.apache.omid.tso.TimestampOracle;
import org.apache.omid.tso.TimestampOracleImpl;
import org.apache.omid.tso.client.AbortException;
import org.apache.omid.tso.client.CellId;
import org.apache.omid.tso.client.TSOProtocol;
import org.apache.omid.tso.util.DummyCellIdImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TSOClientBasicTest {

    private static final Logger LOG = LoggerFactory.getLogger(TSOClientBasicTest.class);

    // Cells for tests
    private final static CellId c1 = new DummyCellIdImpl(0xdeadbeefL);
    private final static CellId c2 = new DummyCellIdImpl(0xfeedcafeL);

    private CommitTable commitTable;
    private CommitTable.Client commitTableClient;
    private TSOProtocol tsoClient;
    private TSOProtocol justAnotherTSOClient;

    @Before
    public void setUp() throws Exception {
        commitTable = new KarelDbCommitTable(new InMemoryCache<>());
        commitTableClient = commitTable.getClient();
        TimestampStorage timestampStorage = new KarelDbTimestampStorage(new InMemoryCache<>());
        TimestampOracle timestampOracle = new TimestampOracleImpl(
            new NullMetricsProvider(), timestampStorage, new RuntimeExceptionPanicker());
        timestampOracle.initialize();
        tsoClient = new KarelDbTimestampClient(timestampOracle, commitTable.getWriter());
        justAnotherTSOClient = new KarelDbTimestampClient(timestampOracle, commitTable.getWriter());
    }

    @Test
    public void testTimestampsOrderingGrowMonotonically() throws Exception {
        long referenceTimestamp;
        long startTsTx1 = tsoClient.getNewStartTimestamp().get();
        referenceTimestamp = startTsTx1;

        long startTsTx2 = tsoClient.getNewStartTimestamp().get();
        referenceTimestamp += CommitTable.MAX_CHECKPOINTS_PER_TXN;
        assertTrue("Should grow monotonically", startTsTx2 >= referenceTimestamp);
        assertTrue("Two timestamps obtained consecutively should grow", startTsTx2 > startTsTx1);

        long commitTsTx2 = tsoClient.commit(startTsTx2, Sets.newHashSet(c1)).get();
        referenceTimestamp += CommitTable.MAX_CHECKPOINTS_PER_TXN;
        assertTrue("Should grow monotonically", commitTsTx2 >= referenceTimestamp);

        long commitTsTx1 = tsoClient.commit(startTsTx1, Sets.newHashSet(c2)).get();
        referenceTimestamp += CommitTable.MAX_CHECKPOINTS_PER_TXN;
        assertTrue("Should grow monotonically", commitTsTx1 >= referenceTimestamp);

        long startTsTx3 = tsoClient.getNewStartTimestamp().get();
        referenceTimestamp += CommitTable.MAX_CHECKPOINTS_PER_TXN;
        assertTrue("Should grow monotonically", startTsTx3 >= referenceTimestamp);
    }

    @Test
    public void testSimpleTransactionWithNoWriteSetCanCommit() throws Exception {
        long startTsTx1 = tsoClient.getNewStartTimestamp().get();
        long commitTsTx1 = tsoClient.commit(startTsTx1, Sets.<CellId>newHashSet()).get();
        assertTrue(commitTsTx1 > startTsTx1);
    }

    @Test
    public void testTransactionWithMassiveWriteSetCanCommit() throws Exception {
        long startTs = tsoClient.getNewStartTimestamp().get();

        Set<CellId> cells = new HashSet<>();
        for (int i = 0; i < 1_000_000; i++) {
            cells.add(new DummyCellIdImpl(i));
        }

        long commitTs = tsoClient.commit(startTs, cells).get();
        assertTrue("Commit TS should be higher than Start TS", commitTs > startTs);
    }

    @Test
    public void testMultipleSerialCommitsDoNotConflict() throws Exception {
        long startTsTx1 = tsoClient.getNewStartTimestamp().get();
        long commitTsTx1 = tsoClient.commit(startTsTx1, Sets.newHashSet(c1)).get();
        assertTrue("Commit TS must be greater than Start TS", commitTsTx1 > startTsTx1);

        long startTsTx2 = tsoClient.getNewStartTimestamp().get();
        assertTrue("TS should grow monotonically", startTsTx2 > commitTsTx1);

        long commitTsTx2 = tsoClient.commit(startTsTx2, Sets.newHashSet(c1, c2)).get();
        assertTrue("Commit TS must be greater than Start TS", commitTsTx2 > startTsTx2);

        long startTsTx3 = tsoClient.getNewStartTimestamp().get();
        long commitTsTx3 = tsoClient.commit(startTsTx3, Sets.newHashSet(c2)).get();
        assertTrue("Commit TS must be greater than Start TS", commitTsTx3 > startTsTx3);
    }

    @Test
    public void testCommitWritesToCommitTable() throws Exception {

        long startTsForTx1 = tsoClient.getNewStartTimestamp().get();
        long startTsForTx2 = tsoClient.getNewStartTimestamp().get();
        assertTrue("Start TS should grow", startTsForTx2 > startTsForTx1);

        if (!tsoClient.isLowLatency())
            assertFalse("Commit TS for TX1 shouldn't appear in Commit Table",
                commitTableClient.getCommitTimestamp(startTsForTx1).get().isPresent());

        long commitTsForTx1 = tsoClient.commit(startTsForTx1, Sets.newHashSet(c1)).get();
        assertTrue("Commit TS should be higher than Start TS for the same tx", commitTsForTx1 > startTsForTx1);

        if (!tsoClient.isLowLatency()) {
            Long commitTs1InCommitTable = commitTableClient.getCommitTimestamp(startTsForTx1).get().get().getValue();
            assertNotNull("Tx is committed, should return as such from Commit Table", commitTs1InCommitTable);
            assertEquals("getCommitTimestamp() & commit() should report same Commit TS value for same tx",
                commitTsForTx1, (long) commitTs1InCommitTable);
            assertTrue("Commit TS should be higher than tx's Start TS", commitTs1InCommitTable > startTsForTx2);
        } else {
            assertTrue("Commit TS should be higher than tx's Start TS", commitTsForTx1 > startTsForTx2);
        }
    }

    @Test
    public void testTwoConcurrentTxWithOverlappingWritesetsHaveConflicts() throws Exception {
        long startTsTx1 = tsoClient.getNewStartTimestamp().get();
        long startTsTx2 = tsoClient.getNewStartTimestamp().get();
        assertTrue("Second TX should have higher TS", startTsTx2 > startTsTx1);

        long commitTsTx1 = tsoClient.commit(startTsTx1, Sets.newHashSet(c1)).get();
        assertTrue("Commit TS must be higher than Start TS for the same tx", commitTsTx1 > startTsTx1);

        try {
            tsoClient.commit(startTsTx2, Sets.newHashSet(c1, c2)).get();
            Assert.fail("Second TX should fail on commit");
        } catch (ExecutionException ee) {
            assertEquals("Should have aborted", AbortException.class, ee.getCause().getClass());
        }
    }

    @Test
    public void testTransactionStartedBeforeFenceAborts() throws Exception {

        long startTsTx1 = tsoClient.getNewStartTimestamp().get();

        long fenceID = tsoClient.getFence(c1.getTableId()).get();

        assertTrue("Fence ID should be higher thank Tx1ID", fenceID > startTsTx1);

        try {
            tsoClient.commit(startTsTx1, Sets.newHashSet(c1, c2)).get();
            Assert.fail("TX should fail on commit");
        } catch (ExecutionException ee) {
            assertEquals("Should have aborted", AbortException.class, ee.getCause().getClass());
        }
    }

    @Test
    public void testTransactionStartedBeforeNonOverlapFenceCommits() throws Exception {

        long startTsTx1 = tsoClient.getNewStartTimestamp().get();

        tsoClient.getFence(7).get();

        try {
            tsoClient.commit(startTsTx1, Sets.newHashSet(c1, c2)).get();
        } catch (ExecutionException ee) {
            Assert.fail("TX should successfully commit");
        }
    }

    @Test
    public void testTransactionStartedAfterFenceCommits() throws Exception {

        tsoClient.getFence(c1.getTableId()).get();

        long startTsTx1 = tsoClient.getNewStartTimestamp().get();

        try {
            tsoClient.commit(startTsTx1, Sets.newHashSet(c1, c2)).get();
        } catch (ExecutionException ee) {
            Assert.fail("TX should successfully commit");
        }
    }

    @Test
    public void testConflictsAndMonotonicallyTimestampGrowthWithTwoDifferentTSOClients() throws Exception {
        long startTsTx1Client1 = tsoClient.getNewStartTimestamp().get();
        long startTsTx2Client1 = tsoClient.getNewStartTimestamp().get();
        long startTsTx3Client1 = tsoClient.getNewStartTimestamp().get();

        Long commitTSTx1 = tsoClient.commit(startTsTx1Client1, Sets.newHashSet(c1)).get();
        try {
            tsoClient.commit(startTsTx3Client1, Sets.newHashSet(c1, c2)).get();
            Assert.fail("Second commit should fail as conflicts with the previous concurrent one");
        } catch (ExecutionException ee) {
            assertEquals("Should have aborted", AbortException.class, ee.getCause().getClass());
        }
        long startTsTx4Client2 = justAnotherTSOClient.getNewStartTimestamp().get();

        assertFalse("Tx3 didn't commit", commitTableClient.getCommitTimestamp(startTsTx3Client1).get().isPresent());
        if (!tsoClient.isLowLatency())
            commitTSTx1 = commitTableClient.getCommitTimestamp(startTsTx1Client1).get().get().getValue();
        assertTrue("Tx1 committed after Tx2 started", commitTSTx1 > startTsTx2Client1);
        assertTrue("Tx1 committed before Tx4 started on the other TSO client", commitTSTx1 < startTsTx4Client2);
    }
}
