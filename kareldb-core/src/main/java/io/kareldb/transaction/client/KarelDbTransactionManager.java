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

import com.google.common.base.Optional;
import io.kareldb.transaction.InMemoryCommitTable;
import io.kareldb.transaction.InMemoryTimestampStorage;
import io.kareldb.transaction.KarelDbTimestampClient;
import io.kareldb.version.VersionedCache;
import io.kareldb.version.VersionedValue;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.metrics.NullMetricsProvider;
import org.apache.omid.timestamp.storage.TimestampStorage;
import org.apache.omid.transaction.AbstractTransaction;
import org.apache.omid.transaction.AbstractTransactionManager;
import org.apache.omid.transaction.AbstractTransactionManagerShim;
import org.apache.omid.transaction.CommitTimestampLocator;
import org.apache.omid.transaction.PostCommitActions;
import org.apache.omid.transaction.TransactionException;
import org.apache.omid.transaction.TransactionManagerException;
import org.apache.omid.tso.RuntimeExceptionPanicker;
import org.apache.omid.tso.TimestampOracle;
import org.apache.omid.tso.TimestampOracleImpl;
import org.apache.omid.tso.client.CellId;
import org.apache.omid.tso.client.TSOProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class KarelDbTransactionManager extends AbstractTransactionManagerShim {

    private static final Logger LOG = LoggerFactory.getLogger(KarelDbTransactionManager.class);

    private static class KarelDbTransactionFactory implements TransactionFactory<KarelDbCellId> {
        @Override
        public KarelDbTransaction createTransaction(long transactionId, long epoch, AbstractTransactionManager tm) {
            return new KarelDbTransaction(transactionId, epoch, new HashSet<>(), new HashSet<>(),
                tm, tm.isLowLatency());
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Construction
    // ----------------------------------------------------------------------------------------------------------------

    public static KarelDbTransactionManager newInstance() {
        return newInstance(new InMemoryCommitTable(), new InMemoryTimestampStorage());
    }

    public static KarelDbTransactionManager newInstance(CommitTable commitTable,
                                                        TimestampStorage timestampStorage) {
        try {
            MetricsRegistry metricsRegistry = new NullMetricsProvider();
            TimestampOracle timestampOracle = new TimestampOracleImpl(
                metricsRegistry, timestampStorage, new RuntimeExceptionPanicker());
            timestampOracle.initialize();
            PostCommitActions postCommitter = new KarelDbSyncPostCommitter(commitTable.getClient());
            return newInstance(commitTable, timestampOracle, postCommitter);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static KarelDbTransactionManager newInstance(CommitTable commitTable,
                                                        TimestampStorage timestampStorage,
                                                        PostCommitActions postCommitter) {
        try {
            MetricsRegistry metricsRegistry = new NullMetricsProvider();
            CommitTable.Client commitTableClient = commitTable.getClient();
            CommitTable.Writer commitTableWriter = commitTable.getWriter();
            TimestampOracle timestampOracle = new TimestampOracleImpl(
                metricsRegistry, timestampStorage, new RuntimeExceptionPanicker());
            timestampOracle.initialize();
            TSOProtocol tsoClient = new KarelDbTimestampClient(timestampOracle, commitTableWriter);

            return new KarelDbTransactionManager(metricsRegistry,
                postCommitter,
                tsoClient,
                commitTableClient,
                commitTableWriter,
                new KarelDbTransactionFactory());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static KarelDbTransactionManager newInstance(CommitTable commitTable,
                                                        TimestampOracle timestampOracle,
                                                        PostCommitActions postCommitter) {
        try {
            MetricsRegistry metricsRegistry = new NullMetricsProvider();
            CommitTable.Client commitTableClient = commitTable.getClient();
            CommitTable.Writer commitTableWriter = commitTable.getWriter();
            TSOProtocol tsoClient = new KarelDbTimestampClient(timestampOracle, commitTableWriter);

            return new KarelDbTransactionManager(metricsRegistry,
                postCommitter,
                tsoClient,
                commitTableClient,
                commitTableWriter,
                new KarelDbTransactionFactory());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private KarelDbTransactionManager(MetricsRegistry metricsRegistry,
                                      PostCommitActions postCommitter,
                                      TSOProtocol tsoClient,
                                      CommitTable.Client commitTableClient,
                                      CommitTable.Writer commitTableWriter,
                                      KarelDbTransactionFactory transactionFactory) {
        super(metricsRegistry,
            postCommitter,
            tsoClient,
            commitTableClient,
            commitTableWriter,
            transactionFactory);
    }

    // ----------------------------------------------------------------------------------------------------------------
    // AbstractTransactionManager overwritten methods
    // ----------------------------------------------------------------------------------------------------------------
    @Override
    public void postBegin(AbstractTransaction<? extends CellId> transaction) throws TransactionManagerException {
        KarelDbTransaction.setCurrentTransaction(((KarelDbTransaction) transaction));
    }

    @Override
    public void closeResources() throws IOException {
    }

    @Override
    public long getHashForTable(byte[] tableName) {
        return KarelDbCellId.getHasher().putBytes(tableName).hash().asLong();
    }

    public long getLowWatermark() throws TransactionException {
        try {
            return commitTableClient.readLowWatermark().get();
        } catch (ExecutionException ee) {
            throw new TransactionException("Error reading low watermark", ee.getCause());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new TransactionException("Interrupted reading low watermark", ie);
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Helper methods
    // ----------------------------------------------------------------------------------------------------------------

    public static KarelDbTransaction enforceKarelDbTransactionAsParam(AbstractTransaction<? extends CellId> tx) {

        if (tx instanceof KarelDbTransaction) {
            return (KarelDbTransaction) tx;
        } else {
            throw new IllegalArgumentException(
                "The transaction object passed is not an instance of KarelDBTransaction");
        }
    }

    public static class CommitTimestampLocatorImpl implements CommitTimestampLocator {

        private final KarelDbCellId cellId;
        private final Map<Long, Long> commitCache;
        private final VersionedCache versionedCache;

        public CommitTimestampLocatorImpl(KarelDbCellId cellId, Map<Long, Long> commitCache, VersionedCache versionedCache) {
            this.cellId = cellId;
            this.commitCache = commitCache;
            this.versionedCache = versionedCache;
        }

        @Override
        public Optional<Long> readCommitTimestampFromCache(long startTimestamp) {
            return Optional.fromNullable(commitCache.get(startTimestamp));
        }

        @Override
        public Optional<Long> readCommitTimestampFromShadowCell(long startTimestamp) throws IOException {
            VersionedValue value = versionedCache.get(cellId.getKey(), startTimestamp);
            if (value == null) {
                return Optional.absent();
            }
            long commit = value.getCommit();
            return Optional.fromNullable(commit > 0 ? commit : null);
        }
    }
}
