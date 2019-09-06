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

import com.google.common.base.Preconditions;
import io.kareldb.transaction.client.KarelDbTransactionManager;
import io.kareldb.version.TxVersionedCache;
import io.kareldb.version.VersionedCache;
import io.kareldb.version.VersionedValue;
import org.apache.omid.transaction.RollbackException;
import org.apache.omid.transaction.Transaction;
import org.apache.omid.transaction.TransactionManager;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;

public class SnapshotIsolationTest {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotIsolationTest.class);

    @Test
    public void testSnapshotIsolation() throws Exception {
        String userTableName = "MY_TX_TABLE";
        Comparable[] initialData = new Comparable[]{0L};
        Comparable[] dataValue1 = new Comparable[]{1L};
        Comparable[] dataValue2 = new Comparable[]{2L};

        try (TransactionManager tm = KarelDbTransactionManager.newInstance()) {
            TxVersionedCache versionedCache = new TxVersionedCache(new VersionedCache(userTableName));
            Comparable[] rowId = new Comparable[]{100L};

            // A transaction Tx0 sets an initial value to a particular column in an specific row
            Transaction tx0 = tm.begin();
            versionedCache.put(rowId, initialData);
            tm.commit(tx0);
            LOG.info("Initial Transaction {} COMMITTED.", tx0);

            // Transaction Tx1 starts, creates its own snapshot of the current data in HBase and writes new data
            Transaction tx1 = tm.begin();
            LOG.info("Transaction {} STARTED", tx1);
            versionedCache.replace(rowId, initialData, dataValue1);
            LOG.info("Transaction {} updates base value in its own Snapshot", tx1);

            // A concurrent transaction Tx2 starts, creates its own snapshot and reads the column value
            Transaction tx2 = tm.begin();
            LOG.info("Concurrent Transaction {} STARTED", tx2);
            VersionedValue tx2GetResult = versionedCache.get(rowId);
            assertArrayEquals("As Tx1 is not yet committed, Tx2 should read the value set by Tx0 not the value written by Tx1",
                tx2GetResult.getValue(), initialData);

            // Transaction Tx1 tries to commit and as there're no conflicting changes, persists the new value in HBase
            tm.commit(tx1);
            LOG.info("Transaction {} COMMITTED.", tx1);

            // Tx2 reading again after Tx1 commit must read data from its snapshot...
            tx2GetResult = versionedCache.get(rowId);
            // ...so it must read the initial value written by Tx0
            LOG.info(
                "Concurrent Transaction {} should read again base value in its Snapshot | Value read = {}",
                tx2, Arrays.toString(tx2GetResult.getValue()));
            assertArrayEquals("Tx2 must read the initial value written by Tx0", tx2GetResult.getValue(), initialData);

            // Tx2 tries to write the column written by the committed concurrent transaction Tx1...
            versionedCache.replace(rowId, tx2GetResult.getValue(), dataValue2);

            // ... and when committing, Tx2 has to abort due to concurrent conflicts with committed transaction Tx1
            try {
                LOG.info("Concurrent Transaction {} TRYING TO COMMIT", tx2);
                tm.commit(tx2);
                // should throw an exception
                Preconditions.checkState(false, "Should have thrown RollbackException");
            } catch (RollbackException e) {
                LOG.info("Concurrent Transaction {} ROLLED-BACK : {}", tx2, e.getMessage());
            }
        }
    }
}

