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

import io.kareldb.version.VersionedCache;
import org.apache.omid.transaction.AbstractTransaction;
import org.apache.omid.transaction.AbstractTransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.kareldb.version.TxVersionedCache.INVALID_TX;

public class KarelDbTransaction extends AbstractTransaction<KarelDbCellId> {

    private static final Logger LOG = LoggerFactory.getLogger(KarelDbTransaction.class);

    private static final ThreadLocal<KarelDbTransaction> currentTransaction = new ThreadLocal<>();

    public static KarelDbTransaction currentTransaction() {
        KarelDbTransaction transaction = currentTransaction.get();
        if (transaction == null) {
            throw new IllegalStateException("No current transaction");
        } else if (transaction.getStatus() == Status.ROLLEDBACK) {
            throw new IllegalStateException("Transaction was already " + transaction.getStatus());
        }
        return transaction;
    }

    public static void setCurrentTransaction(KarelDbTransaction transaction) {
        if (transaction == null) {
            throw new IllegalArgumentException("No current transaction");
        } else if (transaction.getStatus() != Status.RUNNING) {
            throw new IllegalArgumentException("Transaction was already " + transaction.getStatus());
        }
        currentTransaction.set(transaction);
    }

    public KarelDbTransaction(long transactionId, long epoch, Set<KarelDbCellId> writeSet,
                              Set<KarelDbCellId> conflictFreeWriteSet, AbstractTransactionManager tm, boolean isLowLatency) {
        super(transactionId, epoch, writeSet, conflictFreeWriteSet, tm, isLowLatency);
    }

    public KarelDbTransaction(long transactionId, long epoch, Set<KarelDbCellId> writeSet,
                              Set<KarelDbCellId> conflictFreeWriteSet, AbstractTransactionManager tm,
                              long readTimestamp, long writeTimestamp, boolean isLowLatency) {
        super(transactionId, epoch, writeSet, conflictFreeWriteSet, tm, readTimestamp, writeTimestamp, isLowLatency);
    }

    public KarelDbTransaction(long transactionId, long readTimestamp, VisibilityLevel visibilityLevel, long epoch,
                              Set<KarelDbCellId> writeSet, Set<KarelDbCellId> conflictFreeWriteSet,
                              AbstractTransactionManager tm, boolean isLowLatency) {
        super(transactionId, readTimestamp, visibilityLevel, epoch, writeSet, conflictFreeWriteSet, tm, isLowLatency);
    }

    @Override
    public void cleanup() {
        try {
            Map<String, VersionedCache> caches = new HashMap<>();

            for (final KarelDbCellId cell : getWriteSet()) {
                VersionedCache cache = cell.getCache();
                caches.put(cache.getName(), cache);
                cache.setCommit(cell.getKey(), cell.getTimestamp(), INVALID_TX);
            }

            for (final KarelDbCellId cell : getConflictFreeWriteSet()) {
                VersionedCache cache = cell.getCache();
                caches.put(cache.getName(), cache);
                cache.setCommit(cell.getKey(), cell.getTimestamp(), INVALID_TX);
            }

            for (VersionedCache cache : caches.values()) {
                cache.flush();
            }
        } catch (Exception e) {
            LOG.warn("Failed cleanup for Tx {}. This issue has been ignored", getTransactionId(), e);
        }
    }
}
