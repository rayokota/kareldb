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
import io.kcache.utils.Streams;
import org.apache.omid.transaction.Transaction;
import org.apache.omid.transaction.TransactionManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;


public class DeletionTest {

    private static final Logger LOG = LoggerFactory.getLogger(DeletionTest.class);

    private static final String TEST_TABLE = "test-table";

    private Comparable[] dataValue1 = {"testWrite-1"};

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
    public void runTestDeleteRow() throws Exception {
        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        int rowsWritten = 10;

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t1);
        writeRows(versionedCache, rowsWritten);

        tm.commit(t1);

        Transaction t2 = tm.begin();

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) t2);
        versionedCache.remove(new Comparable[]{"test-del" + 0});

        Transaction tscan = tm.begin();

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tscan);
        Iterator<KeyValue<Comparable[], VersionedValue>> iter = versionedCache.range(
            new Comparable[]{"test-del" + 0}, true, new Comparable[]{"test-del" + 9}, true);
        int rowsRead = (int) Streams.streamOf(iter).count();
        assertEquals("Expected " + rowsWritten + " rows but " + rowsRead + " found", rowsRead, rowsWritten);

        tm.commit(t2);

        tscan = tm.begin();

        KarelDbTransaction.setCurrentTransaction((KarelDbTransaction) tscan);
        iter = versionedCache.range(
            new Comparable[]{"test-del" + 0}, true, new Comparable[]{"test-del" + 9}, true);
        rowsRead = (int) Streams.streamOf(iter).count();
        assertEquals("Expected " + (rowsWritten - 1) + " rows but " + rowsRead + " found", rowsRead, rowsWritten - 1);
    }

    private void writeRows(TxVersionedCache versionCache, int rowcount) {
        for (int i = 0; i < rowcount; i++) {
            Comparable[] row = {"test-del" + i};
            versionCache.put(row, dataValue1);
        }
    }
}
