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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import io.kareldb.transaction.client.KarelDbTransactionManager.CommitTimestampLocatorImpl;
import io.kareldb.version.VersionedCache;
import io.kareldb.version.VersionedValue;
import io.kcache.KeyValue;
import io.kcache.KeyValueIterator;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.committable.CommitTable.CommitTimestamp;
import org.apache.omid.transaction.AbstractTransaction.VisibilityLevel;
import org.apache.omid.transaction.CommitTimestampLocator;
import org.apache.omid.transaction.TransactionException;
import org.apache.phoenix.thirdparty.com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static io.kareldb.version.TxVersionedCache.INVALID_TX;
import static org.apache.omid.committable.CommitTable.CommitTimestamp.Location.CACHE;
import static org.apache.omid.committable.CommitTable.CommitTimestamp.Location.COMMIT_TABLE;
import static org.apache.omid.committable.CommitTable.CommitTimestamp.Location.NOT_PRESENT;
import static org.apache.omid.committable.CommitTable.CommitTimestamp.Location.SHADOW_CELL;

public class SnapshotFilterImpl implements SnapshotFilter {

    private static Logger LOG = LoggerFactory.getLogger(SnapshotFilterImpl.class);

    private final VersionedCache versionedCache;

    public SnapshotFilterImpl(VersionedCache versionedCache) {
        this.versionedCache = versionedCache;
    }

    public VersionedCache getVersionedCache() {
        return versionedCache;
    }

    private void healShadowCell(KeyValue<Comparable[], VersionedValue> kv, long commitTimestamp) {
        versionedCache.setCommit(kv.key, kv.value.getVersion(), commitTimestamp);
    }

    /**
     * Check if the transaction commit data is in the shadow cell
     *
     * @param cellStartTimestamp the transaction start timestamp
     *                           locator
     *                           the timestamp locator
     */
    public Optional<CommitTimestamp> readCommitTimestampFromShadowCell(long cellStartTimestamp, CommitTimestampLocator locator)
        throws IOException {

        Optional<CommitTimestamp> commitTS = Optional.absent();

        Optional<Long> commitTimestamp = locator.readCommitTimestampFromShadowCell(cellStartTimestamp);
        if (commitTimestamp.isPresent()) {
            commitTS = Optional.of(new CommitTimestamp(SHADOW_CELL, commitTimestamp.get(), true)); // Valid commit TS
        }

        return commitTS;
    }

    /**
     * This function returns the commit timestamp for a particular cell if the transaction was already committed in
     * the system. In case the transaction was not committed and the cell was written by transaction initialized by a
     * previous TSO server, an invalidation try occurs.
     * Otherwise the function returns a value that indicates that the commit timestamp was not found.
     *
     * @param cellStartTimestamp start timestamp of the cell to locate the commit timestamp for.
     * @param locator            a locator to find the commit timestamp in the system.
     * @return the commit timestamp joint with the location where it was found
     * or an object indicating that it was not found in the system
     * @throws IOException in case of any I/O issues
     */
    public CommitTimestamp locateCellCommitTimestamp(KarelDbTransaction transaction, long cellStartTimestamp,
                                                     CommitTimestampLocator locator) throws IOException {
        CommitTable.Client commitTableClient = transaction.getTransactionManager().getCommitTableClient();
        long epoch = transaction.getEpoch();
        boolean isLowLatency = transaction.isLowLatency();
        try {
            // 1) First check the cache
            Optional<Long> commitTimestamp = locator.readCommitTimestampFromCache(cellStartTimestamp);
            if (commitTimestamp.isPresent()) { // Valid commit timestamp
                return new CommitTimestamp(CACHE, commitTimestamp.get(), true);
            }

            // 2) Then check the commit table
            // If the data was written at a previous epoch, check whether the transaction was invalidated
            boolean invalidatedByOther = false;
            Optional<CommitTimestamp> commitTimestampFromCT = commitTableClient.getCommitTimestamp(cellStartTimestamp).get();
            if (commitTimestampFromCT.isPresent()) {
                if (isLowLatency && !commitTimestampFromCT.get().isValid())
                    invalidatedByOther = true;
                else
                    return commitTimestampFromCT.get();
            }

            // 3) Read from shadow cell
            Optional<CommitTimestamp> commitTimeStamp = readCommitTimestampFromShadowCell(cellStartTimestamp, locator);
            if (commitTimeStamp.isPresent()) {
                return commitTimeStamp.get();
            }

            // In case of LL, if found invalid ct cell, still must check sc in stage 3 then return
            if (invalidatedByOther) {
                assert (!commitTimestampFromCT.get().isValid());
                return commitTimestampFromCT.get();
            }

            // 4) Check the epoch and invalidate the entry
            // if the data was written by a transaction from a previous epoch (previous TSO)
            if (cellStartTimestamp < epoch || isLowLatency) {
                boolean invalidated = commitTableClient.tryInvalidateTransaction(cellStartTimestamp).get();
                if (invalidated) { // Invalid commit timestamp

                    // If we are running lowLatency Omid, we could have manged to invalidate a ct entry,
                    // but the committing client already wrote to shadow cells:
                    if (isLowLatency) {
                        commitTimeStamp = readCommitTimestampFromShadowCell(cellStartTimestamp, locator);
                        if (commitTimeStamp.isPresent()) {
                            // Remove false invalidation from commit table
                            commitTableClient.deleteCommitEntry(cellStartTimestamp);
                            return commitTimeStamp.get();
                        }
                    }

                    return new CommitTimestamp(COMMIT_TABLE, INVALID_TX, false);
                }
            }

            // 5) We did not manage to invalidate the transactions then check the commit table
            commitTimeStamp = commitTableClient.getCommitTimestamp(cellStartTimestamp).get();
            if (commitTimeStamp.isPresent()) {
                return commitTimeStamp.get();
            }

            // 6) Read from shadow cell
            commitTimeStamp = readCommitTimestampFromShadowCell(cellStartTimestamp, locator);
            if (commitTimeStamp.isPresent()) {
                return commitTimeStamp.get();
            }

            // *) Otherwise return not found
            return new CommitTimestamp(NOT_PRESENT, -1L /* TODO Check if we should return this */, true);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while finding commit timestamp", e);
        } catch (ExecutionException e) {
            throw new IOException("Problem finding commit timestamp", e);
        }
    }

    public Optional<Long> tryToLocateCellCommitTimestamp(KarelDbTransaction transaction,
                                                         KeyValue<Comparable[], VersionedValue> cell,
                                                         Map<Long, Long> commitCache) throws IOException {
        CommitTimestamp tentativeCommitTimestamp =
            locateCellCommitTimestamp(transaction, cell.value.getVersion(),
                new CommitTimestampLocatorImpl(new KarelDbCellId(versionedCache, cell.key, cell.value.getVersion()),
                    commitCache, versionedCache));

        // If transaction that added the cell was invalidated
        if (!tentativeCommitTimestamp.isValid()) {
            return Optional.absent();
        }

        switch (tentativeCommitTimestamp.getLocation()) {
            case COMMIT_TABLE:
                // If the commit timestamp is found in the persisted commit table,
                // that means the writing process of the shadow cell in the post
                // commit phase of the client probably failed, so we heal the shadow
                // cell with the right commit timestamp for avoiding further reads to
                // hit the storage
                healShadowCell(cell, tentativeCommitTimestamp.getValue());
                return Optional.of(tentativeCommitTimestamp.getValue());
            case CACHE:
            case SHADOW_CELL:
                return Optional.of(tentativeCommitTimestamp.getValue());
            case NOT_PRESENT:
                return Optional.absent();
            default:
                assert (false);
                return Optional.absent();
        }
    }

    private Optional<Long> getCommitTimestamp(KarelDbTransaction transaction, KeyValue<Comparable[], VersionedValue> kv,
                                              Map<Long, Long> commitCache)
        throws IOException {

        long startTimestamp = transaction.getStartTimestamp();

        if (kv.value.getVersion() == startTimestamp) {
            return Optional.of(startTimestamp);
        }

        return tryToLocateCellCommitTimestamp(transaction, kv, commitCache);
    }

    private Map<Long, Long> buildCommitCache(List<VersionedValue> rawCells) {

        Map<Long, Long> commitCache = new HashMap<>();

        for (VersionedValue value : rawCells) {
            long commit = value.getCommit();
            if (commit > 0) {
                commitCache.put(value.getVersion(), commit);
            }
        }

        return commitCache;
    }

    public Optional<Long> getTSIfInTransaction(KarelDbTransaction transaction,
                                               KeyValue<Comparable[], VersionedValue> kv) {
        long startTimestamp = transaction.getStartTimestamp();
        long readTimestamp = transaction.getReadTimestamp();

        // A cell was written by a transaction if its timestamp is larger than its startTimestamp and smaller or equal to its readTimestamp.
        // There also might be a case where the cell was written by the transaction and its timestamp equals to its writeTimestamp, however,
        // this case occurs after checkpoint and in this case we do not want to read this data.
        if (kv.value.getVersion() >= startTimestamp && kv.value.getVersion() <= readTimestamp) {
            return Optional.of(kv.value.getVersion());
        }

        return Optional.absent();
    }

    public Optional<Long> getTSIfInSnapshot(KarelDbTransaction transaction,
                                            KeyValue<Comparable[], VersionedValue> kv,
                                            Map<Long, Long> commitCache)
        throws IOException {

        Optional<Long> commitTimestamp = getCommitTimestamp(transaction, kv, commitCache);

        if (commitTimestamp.isPresent() && commitTimestamp.get() < transaction.getStartTimestamp())
            return commitTimestamp;

        return Optional.absent();
    }

    /**
     * Filters the raw results and returns only those belonging to the current snapshot, as defined
     * by the transaction object.
     *
     * @param transaction Defines the current snapshot
     * @param rawCells    Raw cells that we are going to filter
     * @return Filtered KVs belonging to the transaction snapshot
     */
    public List<VersionedValue> filterCellsForSnapshot(
        KarelDbTransaction transaction, Comparable[] key, List<VersionedValue> rawCells) throws IOException {

        assert (rawCells != null && transaction != null);

        List<VersionedValue> keyValuesInSnapshot = new ArrayList<>();

        Map<Long, Long> commitCache = buildCommitCache(rawCells);

        for (VersionedValue value : rawCells) {
            KeyValue<Comparable[], VersionedValue> kv = new KeyValue<>(key, value);
            if (getTSIfInTransaction(transaction, kv).isPresent() ||
                getTSIfInSnapshot(transaction, kv, commitCache).isPresent()) {

                if (transaction.getVisibilityLevel() == VisibilityLevel.SNAPSHOT_ALL) {
                    keyValuesInSnapshot.add(value);
                    if (getTSIfInTransaction(transaction, kv).isPresent()) {
                        continue;
                    } else {
                        break;
                    }
                } else {
                    keyValuesInSnapshot.add(value);
                    break;
                }
            }
        }

        // TODO remove?
        //Collections.sort(keyValuesInSnapshot, new Table.ComparableArrayComparator());

        return keyValuesInSnapshot;
    }

    @Override
    public List<VersionedValue> get(KarelDbTransaction transaction, Comparable[] key) {
        try {
            List<VersionedValue> result = versionedCache.get(key, 0, Long.MAX_VALUE);

            List<VersionedValue> filteredKeyValues = Collections.emptyList();
            if (!result.isEmpty()) {
                filteredKeyValues = filterCellsForSnapshot(transaction, key, result);
            }

            return filteredKeyValues;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public KeyValueIterator<Comparable[], List<VersionedValue>> range(
        KarelDbTransaction transaction, Comparable[] from, boolean fromInclusive, Comparable[] to, boolean toInclusive) {
        return new FilteredKeyValueIterator(transaction, versionedCache.range(
            from, fromInclusive, to, toInclusive, 0, Long.MAX_VALUE));
    }

    @Override
    public KeyValueIterator<Comparable[], List<VersionedValue>> all(KarelDbTransaction transaction) {
        return new FilteredKeyValueIterator(transaction, versionedCache.all(0, Long.MAX_VALUE));
    }

    @VisibleForTesting
    public boolean isCommitted(KarelDbTransaction transaction, KarelDbCellId cellId) throws TransactionException {
        try {
            long timestamp = cellId.getTimestamp();
            CommitTimestamp tentativeCommitTimestamp =
                locateCellCommitTimestamp(transaction, timestamp,
                    new CommitTimestampLocatorImpl(cellId, Maps.<Long, Long>newHashMap(), versionedCache));

            // If transaction that added the cell was invalidated
            if (!tentativeCommitTimestamp.isValid()) {
                return false;
            }

            switch (tentativeCommitTimestamp.getLocation()) {
                case COMMIT_TABLE:
                case SHADOW_CELL:
                    return true;
                case NOT_PRESENT:
                    return false;
                case CACHE: // cache was empty
                default:
                    return false;
            }
        } catch (IOException e) {
            throw new TransactionException("Failure while checking if a transaction was committed", e);
        }
    }

    private class FilteredKeyValueIterator implements KeyValueIterator<Comparable[], List<VersionedValue>> {
        private final KarelDbTransaction transaction;
        private final KeyValueIterator<Comparable[], List<VersionedValue>> iterator;

        FilteredKeyValueIterator(
            KarelDbTransaction transaction,
            KeyValueIterator<Comparable[], List<VersionedValue>> iter) {
            this.transaction = transaction;
            this.iterator = iter;
        }

        public final boolean hasNext() {
            return iterator.hasNext();
        }

        public final KeyValue<Comparable[], List<VersionedValue>> next() {
            try {
                KeyValue<Comparable[], List<VersionedValue>> next = iterator.next();
                return new KeyValue<>(next.key, filterCellsForSnapshot(transaction, next.key, next.value));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public final void remove() {
            throw new UnsupportedOperationException();
        }

        public final void close() {
            iterator.close();
        }
    }
}
