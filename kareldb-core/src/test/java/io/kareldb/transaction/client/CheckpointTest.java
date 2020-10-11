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
import org.apache.omid.committable.CommitTable;
import org.apache.omid.transaction.AbstractTransaction.VisibilityLevel;
import org.apache.omid.transaction.Transaction;
import org.apache.omid.transaction.TransactionException;
import org.apache.omid.transaction.TransactionManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class CheckpointTest {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointTest.class);

    private static final String TEST_TABLE = "test-table";

    private Comparable[] rowId1 = {"row1"};

    private Comparable[] dataValue0 = {"testWrite-0"};
    private Comparable[] dataValue1 = {"testWrite-1"};
    private Comparable[] dataValue2 = {"testWrite-2"};
    private Comparable[] dataValue3 = {"testWrite-3"};

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
    public void testFewCheckPoints() throws Exception {

        Transaction tx1 = tm.begin();
        KarelDbTransaction kdbTx1 = (KarelDbTransaction) tx1;

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx1);
        versionedCache.put(rowId1, dataValue1);

        VersionedValue v1 = versionedCache.get(rowId1);
        assertArrayEquals(dataValue1, v1.getValue());

        kdbTx1.checkpoint();

        versionedCache.replace(rowId1, dataValue1, dataValue2);

        v1 = versionedCache.get(rowId1);
        assertArrayEquals(dataValue1, v1.getValue());

        kdbTx1.setVisibilityLevel(VisibilityLevel.SNAPSHOT);

        v1 = versionedCache.get(rowId1);
        assertArrayEquals(dataValue2, v1.getValue());

        kdbTx1.checkpoint();

        versionedCache.replace(rowId1, dataValue2, dataValue3);

        v1 = versionedCache.get(rowId1);
        assertArrayEquals(dataValue2, v1.getValue());

        kdbTx1.checkpoint();

        v1 = versionedCache.get(rowId1);
        assertArrayEquals(dataValue3, v1.getValue());

        kdbTx1.setVisibilityLevel(VisibilityLevel.SNAPSHOT_ALL);

        List<VersionedValue> values = versionedCache.getVersions(rowId1);
        assertEquals("Expected 3 results and found " + values.size(), 3, values.size());

        assertArrayEquals(dataValue3, values.get(0).getValue());
        assertArrayEquals(dataValue2, values.get(1).getValue());
        assertArrayEquals(dataValue1, values.get(2).getValue());
    }

    @Test
    public void testSnapshot() throws Exception {
        Transaction tx1 = tm.begin();

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx1);
        versionedCache.put(rowId1, dataValue0);

        tm.commit(tx1);

        tx1 = tm.begin();
        KarelDbTransaction kdbTx1 = (KarelDbTransaction) tx1;

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx1);
        VersionedValue v1 = versionedCache.get(rowId1);
        assertArrayEquals(dataValue0, v1.getValue());

        versionedCache.replace(rowId1, dataValue0, dataValue1);

        v1 = versionedCache.get(rowId1);
        assertArrayEquals(dataValue1, v1.getValue());

        kdbTx1.checkpoint();

        versionedCache.replace(rowId1, dataValue1, dataValue2);

        v1 = versionedCache.get(rowId1);
        assertArrayEquals(dataValue1, v1.getValue());

        kdbTx1.setVisibilityLevel(VisibilityLevel.SNAPSHOT);

        v1 = versionedCache.get(rowId1);
        assertArrayEquals(dataValue2, v1.getValue());
    }

    @Test
    public void testSnapshotAll() throws Exception {
        Transaction tx1 = tm.begin();

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx1);
        versionedCache.put(rowId1, dataValue0);

        tm.commit(tx1);

        tx1 = tm.begin();
        KarelDbTransaction kdbTx1 = (KarelDbTransaction) tx1;

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx1);
        VersionedValue v1 = versionedCache.get(rowId1);
        assertArrayEquals(dataValue0, v1.getValue());

        versionedCache.replace(rowId1, dataValue0, dataValue1);

        v1 = versionedCache.get(rowId1);
        assertArrayEquals(dataValue1, v1.getValue());

        kdbTx1.checkpoint();

        versionedCache.replace(rowId1, dataValue1, dataValue2);

        v1 = versionedCache.get(rowId1);
        assertArrayEquals(dataValue1, v1.getValue());

        kdbTx1.setVisibilityLevel(VisibilityLevel.SNAPSHOT_ALL);

        List<VersionedValue> values = versionedCache.getVersions(rowId1);
        assertEquals("Expected 3 results and found " + values.size(), 3, values.size());

        assertArrayEquals(dataValue2, values.get(0).getValue());
        assertArrayEquals(dataValue1, values.get(1).getValue());
        assertArrayEquals(dataValue0, values.get(2).getValue());
    }

    @Test
    public void testSnapshotExcludeCurrent() throws Exception {
        Transaction tx1 = tm.begin();
        KarelDbTransaction kdbTx1 = (KarelDbTransaction) tx1;

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx1);
        versionedCache.put(rowId1, dataValue1);

        VersionedValue v1 = versionedCache.get(rowId1);
        assertArrayEquals(dataValue1, v1.getValue());

        kdbTx1.checkpoint();

        versionedCache.replace(rowId1, dataValue1, dataValue2);

        v1 = versionedCache.get(rowId1);
        assertArrayEquals(dataValue1, v1.getValue());

        kdbTx1.setVisibilityLevel(VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT);

        v1 = versionedCache.get(rowId1);
        assertArrayEquals(dataValue1, v1.getValue());
    }

    @Test
    public void testDeleteAfterCheckpoint() throws Exception {
        Transaction tx1 = tm.begin();

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tx1);
        versionedCache.put(rowId1, dataValue1);

        tm.commit(tx1);

        Transaction tx2 = tm.begin();

        KarelDbTransaction kdbTx2 = (KarelDbTransaction) tx2;

        kdbTx2.checkpoint();

        versionedCache.remove(rowId1);

        try {
            tm.commit(tx2);
        } catch (TransactionException e) {
            Assert.fail();
        }
    }

    @Test
    public void testOutOfCheckpoints() throws Exception {
        Transaction tx1 = tm.begin();
        KarelDbTransaction kdbTx1 = (KarelDbTransaction) tx1;

        for (int i = 0; i < CommitTable.MAX_CHECKPOINTS_PER_TXN - 1; ++i) {
            kdbTx1.checkpoint();
        }

        try {
            kdbTx1.checkpoint();
            Assert.fail();
        } catch (TransactionException e) {
            // expected
        }
    }
}
