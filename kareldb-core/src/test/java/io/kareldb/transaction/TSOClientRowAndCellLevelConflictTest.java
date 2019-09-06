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
import org.apache.omid.tso.WorldClockOracleImpl;
import org.apache.omid.tso.client.AbortException;
import org.apache.omid.tso.client.CellId;
import org.apache.omid.tso.client.TSOProtocol;
import org.apache.omid.tso.util.DummyCellIdImpl;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TSOClientRowAndCellLevelConflictTest {

    private static final Logger LOG = LoggerFactory.getLogger(TSOClientRowAndCellLevelConflictTest.class);

    private CommitTable commitTable;
    private TSOProtocol client;

    @Before
    public void setUp() throws IOException {
        commitTable = new KarelDbCommitTable(new InMemoryCache<>());
        TimestampStorage timestampStorage = new KarelDbTimestampStorage(new InMemoryCache<>());
        TimestampOracle timestampOracle = new WorldClockOracleImpl(
            new NullMetricsProvider(), timestampStorage, new RuntimeExceptionPanicker());
        timestampOracle.initialize();
        client = new KarelDbTimestampClient(timestampOracle, commitTable.getWriter());
    }

    @Test
    public void testCellLevelConflictAnalysisConflict() throws Exception {

        CellId c1 = new DummyCellIdImpl(0xdeadbeefL, 0xdeadbeeeL);
        CellId c2 = new DummyCellIdImpl(0xdeadbeefL, 0xdeadbeeeL);

        Set<CellId> testWriteSet1 = Sets.newHashSet(c1);
        Set<CellId> testWriteSet2 = Sets.newHashSet(c2);

        long ts1 = client.getNewStartTimestamp().get();
        long ts2 = client.getNewStartTimestamp().get();

        client.commit(ts1, testWriteSet1).get();

        try {
            client.commit(ts2, testWriteSet2).get();
        } catch (ExecutionException e) {
            assertTrue("Transaction should be aborted", e.getCause() instanceof AbortException);
            return;
        }

        assertTrue("Transaction should be aborted", false);
    }

    @Test
    public void testCellLevelConflictAnalysisCommit() throws Exception {

        CellId c1 = new DummyCellIdImpl(0xdeadbeefL, 0xdeadbeeeL);
        CellId c2 = new DummyCellIdImpl(0xfeedcafeL, 0xdeadbeefL);

        Set<CellId> testWriteSet1 = Sets.newHashSet(c1);
        Set<CellId> testWriteSet2 = Sets.newHashSet(c2);

        long ts1 = client.getNewStartTimestamp().get();
        long ts2 = client.getNewStartTimestamp().get();

        client.commit(ts1, testWriteSet1).get();

        try {
            client.commit(ts2, testWriteSet2).get();
        } catch (ExecutionException e) {
            assertFalse("Transaction should be committed", e.getCause() instanceof AbortException);
            return;
        }

        assertTrue("Transaction should be committed", true);
    }
}
