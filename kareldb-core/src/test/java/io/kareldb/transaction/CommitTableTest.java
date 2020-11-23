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

import io.kcache.Cache;
import io.kcache.utils.InMemoryCache;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.committable.CommitTable.Client;
import org.apache.omid.committable.CommitTable.CommitTimestamp;
import org.apache.omid.committable.CommitTable.Writer;
import org.apache.phoenix.thirdparty.com.google.common.base.Optional;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CommitTableTest {

    private static final Logger LOG = LoggerFactory.getLogger(CommitTableTest.class);

    @Test
    public void testBasicBehaviour() throws Throwable {
        Cache<Long, Long> cache = new InMemoryCache<>();
        KarelDbCommitTable commitTable = new KarelDbCommitTable(cache);

        Writer writer = commitTable.getWriter();
        Client client = commitTable.getClient();

        // Test that the first time the table is empty
        assertEquals("Rows should be 0!", cache.size(), 0);

        // Test the successful creation of 1000 txs in the table
        for (int i = 0; i < 1000; i += CommitTable.MAX_CHECKPOINTS_PER_TXN) {
            writer.addCommittedTransaction(i, i + 1);
        }
        writer.flush();
        assertEquals("Rows should be 1000!", cache.size(), 1000 / CommitTable.MAX_CHECKPOINTS_PER_TXN);

        // Test the we get the right commit timestamps for each previously inserted tx
        for (long i = 0; i < 1000; i++) {
            Optional<CommitTimestamp> commitTimestamp = client.getCommitTimestamp(i).get();
            assertTrue(commitTimestamp.isPresent());
            assertTrue(commitTimestamp.get().isValid());
            long ct = commitTimestamp.get().getValue();
            long expected = i - (i % CommitTable.MAX_CHECKPOINTS_PER_TXN) + 1;
            assertEquals("Commit timestamp should be " + expected, ct, expected);
        }
        assertEquals("Rows should be 1000!", cache.size(), 1000 / CommitTable.MAX_CHECKPOINTS_PER_TXN);

        // Test the successful deletion of the 1000 txs
        Future<Void> f;
        for (long i = 0; i < 1000; i += CommitTable.MAX_CHECKPOINTS_PER_TXN) {
            f = client.deleteCommitEntry(i);
            f.get();
        }
        assertEquals("Rows should be 0!", cache.size(), 0);

        // Test we don't get a commit timestamp for a non-existent transaction id in the table
        Optional<CommitTimestamp> commitTimestamp = client.getCommitTimestamp(0).get();
        assertFalse("Commit timestamp should not be present", commitTimestamp.isPresent());

        // Test that the first time, the low watermark family in table is empty
        assertEquals("Rows should be 0!", cache.size(), 0);

        // Test the unsuccessful read of the low watermark the first time
        ListenableFuture<Long> lowWatermarkFuture = client.readLowWatermark();
        assertEquals("Low watermark should be 0", lowWatermarkFuture.get(), Long.valueOf(0));

        // Test the successful update of the low watermark
        for (int lowWatermark = 0; lowWatermark < 1000; lowWatermark++) {
            writer.updateLowWatermark(lowWatermark);
        }
        writer.flush();
        assertEquals("Should there be only row!", cache.size(), 1);

        // Test the successful read of the low watermark
        lowWatermarkFuture = client.readLowWatermark();
        long lowWatermark = lowWatermarkFuture.get();
        assertEquals("Low watermark should be 999", lowWatermark, 999);
        assertEquals("Should there be only one row", cache.size(), 1);
    }

    @Test
    public void testCheckpoints() throws Throwable {
        Cache<Long, Long> cache = new InMemoryCache<>();
        KarelDbCommitTable commitTable = new KarelDbCommitTable(cache);

        Writer writer = commitTable.getWriter();
        Client client = commitTable.getClient();

        // Test that the first time the table is empty
        assertEquals("Rows should be 0!", cache.size(), 0);

        long st = 0;
        long ct = 1;

        // Add a single commit that may represent many checkpoints
        writer.addCommittedTransaction(st, ct);
        writer.flush();

        for (int i = 0; i < CommitTable.MAX_CHECKPOINTS_PER_TXN; ++i) {
            Optional<CommitTimestamp> commitTimestamp = client.getCommitTimestamp(i).get();
            assertTrue(commitTimestamp.isPresent());
            assertTrue(commitTimestamp.get().isValid());
            assertEquals(ct, commitTimestamp.get().getValue());
        }

        // try invalidate based on start timestamp from a checkpoint
        assertFalse(client.tryInvalidateTransaction(st + 1).get());

        long st2 = 100;
        long ct2 = 101;

        // now invalidate a not committed transaction and then commit
        assertTrue(client.tryInvalidateTransaction(st2 + 1).get());
        assertFalse(writer.atomicAddCommittedTransaction(st2, ct2));

        //test delete
        client.deleteCommitEntry(st2 + 1).get();
        //now committing should work
        assertTrue(writer.atomicAddCommittedTransaction(st2, ct2));
    }

    @Test
    public void testTransactionInvalidation() throws Throwable {
        // Prepare test
        final int TX1_ST = 0;
        final int TX1_CT = TX1_ST + 1;
        final int TX2_ST = CommitTable.MAX_CHECKPOINTS_PER_TXN;
        final int TX2_CT = TX2_ST + 1;

        Cache<Long, Long> cache = new InMemoryCache<>();
        KarelDbCommitTable commitTable = new KarelDbCommitTable(cache);

        // Components under test
        Writer writer = commitTable.getWriter();
        Client client = commitTable.getClient();

        // Test that initially the table is empty
        assertEquals("Rows should be 0!", cache.size(), 0);

        // Test that a transaction can be added properly to the commit table
        writer.addCommittedTransaction(TX1_ST, TX1_CT);
        writer.flush();
        Optional<CommitTimestamp> commitTimestamp = client.getCommitTimestamp(TX1_ST).get();
        assertTrue(commitTimestamp.isPresent());
        assertTrue(commitTimestamp.get().isValid());
        long ct = commitTimestamp.get().getValue();
        assertEquals("Commit timestamp should be " + TX1_CT, ct, TX1_CT);

        // Test that a committed transaction cannot be invalidated and
        // preserves its commit timestamp after that
        boolean wasInvalidated = client.tryInvalidateTransaction(TX1_ST).get();
        assertFalse("Transaction should not be invalidated", wasInvalidated);

        commitTimestamp = client.getCommitTimestamp(TX1_ST).get();
        assertTrue(commitTimestamp.isPresent());
        assertTrue(commitTimestamp.get().isValid());
        ct = commitTimestamp.get().getValue();
        assertEquals("Commit timestamp should be " + TX1_CT, ct, TX1_CT);

        // Test that a non-committed transaction can be invalidated...
        wasInvalidated = client.tryInvalidateTransaction(TX2_ST).get();
        assertTrue("Transaction should be invalidated", wasInvalidated);
        commitTimestamp = client.getCommitTimestamp(TX2_ST).get();
        assertTrue(commitTimestamp.isPresent());
        assertFalse(commitTimestamp.get().isValid());
        ct = commitTimestamp.get().getValue();
        assertEquals("Commit timestamp should be " + CommitTable.INVALID_TRANSACTION_MARKER,
            ct, CommitTable.INVALID_TRANSACTION_MARKER);
        // ...and that if it has been already invalidated, it remains
        // invalidated when someone tries to commit it
        writer.addCommittedTransaction(TX2_ST, TX2_CT);
        writer.flush();
        commitTimestamp = client.getCommitTimestamp(TX2_ST).get();
        assertTrue(commitTimestamp.isPresent());
        assertFalse(commitTimestamp.get().isValid());
        ct = commitTimestamp.get().getValue();
        assertEquals("Commit timestamp should be " + CommitTable.INVALID_TRANSACTION_MARKER,
            ct, CommitTable.INVALID_TRANSACTION_MARKER);

        // Test that at the end of the test, the commit table contains 2
        // elements, which correspond to the two rows added in the test
        assertEquals("Rows should be 2!", cache.size(), 2);
    }
}
