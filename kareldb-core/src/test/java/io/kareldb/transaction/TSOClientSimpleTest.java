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

import java.io.IOException;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TSOClientSimpleTest {

    private static final CellId c1 = new DummyCellIdImpl(0xdeadbeefL);
    private static final CellId c2 = new DummyCellIdImpl(-0xfeedcafeL);

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
    public void testConflicts() throws Exception {
        long tr1 = client.getNewStartTimestamp().get();
        long tr2 = client.getNewStartTimestamp().get();

        client.commit(tr1, Sets.newHashSet(c1), new HashSet<>()).get();

        try {
            client.commit(tr2, Sets.newHashSet(c1, c2), new HashSet<>()).get();
            fail("Shouldn't have committed");
        } catch (ExecutionException ee) {
            assertEquals("Should have aborted", ee.getCause().getClass(), AbortException.class);
        }
    }

    @Test
    public void testWatermarkUpdate() throws Exception {
        CommitTable.Client commitTableClient = commitTable.getClient();

        long tr1 = client.getNewStartTimestamp().get();
        client.commit(tr1, Sets.newHashSet(c1), new HashSet<>()).get();

        long initWatermark = commitTableClient.readLowWatermark().get();

        long tr2 = client.getNewStartTimestamp().get();
        client.commit(tr2, Sets.newHashSet(c1), new HashSet<>()).get();

        long newWatermark = commitTableClient.readLowWatermark().get();
        assertTrue("new low watermark should be bigger", newWatermark > initWatermark);
    }
}
