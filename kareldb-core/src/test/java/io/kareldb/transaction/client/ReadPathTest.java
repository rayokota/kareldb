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
import org.apache.omid.transaction.Transaction;
import org.apache.omid.transaction.TransactionManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

public class ReadPathTest {

    private static final String TEST_TABLE = "test-table";

    private Comparable[] rowId1 = {"row1"};

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
    public void testReadInterleaved() throws Exception {
        // Put some data on the DB
        Transaction t1 = tm.begin();
        Transaction t2 = tm.begin();

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t1);
        versionedCache.put(rowId1, dataValue1);

        tm.commit(t1);

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t2);
        assertNull(versionedCache.get(rowId1));
    }

    @Test
    public void testReadWithSeveralUncommitted() throws Exception {
        // Put some data on the DB
        Transaction t = tm.begin();

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t);
        versionedCache.put(rowId1, dataValue1);

        tm.commit(t);
        List<Transaction> running = new ArrayList<>();

        // Shade the data with uncommitted data
        for (int i = 0; i < 10; ++i) {
            t = tm.begin();

            KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t);
            versionedCache.replace(rowId1, dataValue1, dataValue2);

            running.add(t);
        }

        // Try to read from row, it should ignore the uncommitted data and return the original committed value
        t = tm.begin();

        VersionedValue v1 = versionedCache.get(rowId1);
        assertArrayEquals(dataValue1, v1.getValue());

        tm.commit(t);

        for (Transaction r : running) {
            tm.rollback(r);
        }
    }
}
