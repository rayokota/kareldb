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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.kareldb.version.VersionedCache;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.transaction.AbstractTransaction;
import org.apache.omid.transaction.PostCommitActions;
import org.apache.omid.transaction.TransactionManagerException;
import org.apache.omid.tso.client.CellId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class KarelDbSyncPostCommitter implements PostCommitActions {

    private static final Logger LOG = LoggerFactory.getLogger(KarelDbSyncPostCommitter.class);

    private final CommitTable.Client commitTableClient;

    public KarelDbSyncPostCommitter(CommitTable.Client commitTableClient) {
        this.commitTableClient = commitTableClient;
    }

    @Override
    public ListenableFuture<Void> updateShadowCells(AbstractTransaction<? extends CellId> transaction) {

        SettableFuture<Void> updateSCFuture = SettableFuture.create();

        KarelDbTransaction tx = KarelDbTransactionManager.enforceKarelDbTransactionAsParam(transaction);

        try {
            Map<String, VersionedCache> caches = new HashMap<>();
            // Add shadow cells
            for (KarelDbCellId cell : tx.getWriteSet()) {
                VersionedCache cache = cell.getCache();
                caches.put(cache.getName(), cache);
                cache.setCommit(cell.getKey(), cell.getTimestamp(), tx.getCommitTimestamp());
            }

            for (KarelDbCellId cell : tx.getConflictFreeWriteSet()) {
                VersionedCache cache = cell.getCache();
                caches.put(cache.getName(), cache);
                cache.setCommit(cell.getKey(), cell.getTimestamp(), tx.getCommitTimestamp());
            }

            for (VersionedCache cache : caches.values()) {
                cache.flush();
            }

            //Only if all is well we set to null and delete commit entry from commit table
            updateSCFuture.set(null);
        } catch (Exception e) {
            LOG.warn("{}: Error inserting shadow cells", tx, e);
            updateSCFuture.setException(
                new TransactionManagerException(tx + ": Error inserting shadow cells ", e));
        }

        return updateSCFuture;
    }

    @Override
    public ListenableFuture<Void> removeCommitTableEntry(AbstractTransaction<? extends CellId> transaction) {

        SettableFuture<Void> updateSCFuture = SettableFuture.create();

        KarelDbTransaction tx = KarelDbTransactionManager.enforceKarelDbTransactionAsParam(transaction);

        try {
            commitTableClient.deleteCommitEntry(tx.getStartTimestamp()).get();
            updateSCFuture.set(null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("{}: interrupted during commit table entry delete", tx, e);
            updateSCFuture.setException(
                new TransactionManagerException(tx + ": interrupted during commit table entry delete"));
        } catch (ExecutionException e) {
            LOG.warn("{}: can't remove commit table entry", tx, e);
            updateSCFuture.setException(new TransactionManagerException(tx + ": can't remove commit table entry"));
        }

        return updateSCFuture;
    }
}
