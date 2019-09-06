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

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.kcache.Cache;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.committable.CommitTable.CommitTimestamp.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KarelDbCommitTable implements CommitTable {

    private static final Logger LOG = LoggerFactory.getLogger(KarelDbCommitTable.class);

    private static final long LOW_WATERMARK_KEY = 0L;

    private final Cache<Long, Long> cache;

    public KarelDbCommitTable(Cache<Long, Long> cache) {
        this.cache = cache;
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Reader and Writer
    // ----------------------------------------------------------------------------------------------------------------

    private class KarelDbWriter implements Writer {

        private static final long INITIAL_LWM_VALUE = -1L;

        // Our own buffer for operations
        final Map<Long, Long> writeBuffer = new ConcurrentHashMap<>();
        volatile long lowWatermarkToStore = INITIAL_LWM_VALUE;

        KarelDbWriter() {
        }

        @Override
        public void addCommittedTransaction(long startTimestamp, long commitTimestamp) throws IOException {
            // Allow to be equal for fencing
            assert (startTimestamp <= commitTimestamp);
            writeBuffer.put(startTimestamp, commitTimestamp);
        }

        @Override
        public void updateLowWatermark(long lowWatermark) throws IOException {
            lowWatermarkToStore = lowWatermark;
        }

        @Override
        public void flush() throws IOException {
            addLowWatermarkToStoreToWriteBuffer();
            for (Map.Entry<Long, Long> entry : writeBuffer.entrySet()) {
                cache.merge(entry.getKey(), entry.getValue(),
                    (oldValue, newValue) -> oldValue != INVALID_TRANSACTION_MARKER ? newValue : oldValue);
            }
            cache.flush();
            writeBuffer.clear();
        }

        @Override
        public void clearWriteBuffer() {
            writeBuffer.clear();
        }

        @Override
        public boolean atomicAddCommittedTransaction(long startTimestamp, long commitTimestamp) throws IOException {
            assert (startTimestamp < commitTimestamp);
            long value = cache.merge(startTimestamp, commitTimestamp,
                (oldValue, newValue) -> oldValue != INVALID_TRANSACTION_MARKER ? newValue : oldValue);
            cache.flush();
            return value != INVALID_TRANSACTION_MARKER;
        }

        private void addLowWatermarkToStoreToWriteBuffer() {
            long lowWatermark = lowWatermarkToStore;
            if (lowWatermark != INITIAL_LWM_VALUE) {
                writeBuffer.put(LOW_WATERMARK_KEY, lowWatermark);
            }
        }
    }

    class KarelDbClient implements Client {

        KarelDbClient() {
        }

        @Override
        public ListenableFuture<Optional<CommitTimestamp>> getCommitTimestamp(long startTimestamp) {
            startTimestamp = removeCheckpointBits(startTimestamp);
            SettableFuture<Optional<CommitTimestamp>> f = SettableFuture.create();
            Long commitTimestamp = cache.get(startTimestamp);
            if (commitTimestamp == null) {
                f.set(Optional.absent());
            } else if (commitTimestamp == INVALID_TRANSACTION_MARKER) {
                CommitTimestamp invalidCT =
                    new CommitTimestamp(Location.COMMIT_TABLE, INVALID_TRANSACTION_MARKER, false);
                f.set(Optional.of(invalidCT));
                return f;
            } else {
                CommitTimestamp validCT = new CommitTimestamp(Location.COMMIT_TABLE, commitTimestamp, true);
                f.set(Optional.of(validCT));
            }
            return f;
        }

        @Override
        public ListenableFuture<Long> readLowWatermark() {
            SettableFuture<Long> f = SettableFuture.create();
            Long lowWatermark = cache.get(LOW_WATERMARK_KEY);
            if (lowWatermark == null) {
                f.set(0L);
            } else {
                f.set(lowWatermark);
            }
            return f;
        }

        // This function is only used to delete a CT entry and should be renamed
        @Override
        public ListenableFuture<Void> deleteCommitEntry(long startTimestamp) {
            startTimestamp = removeCheckpointBits(startTimestamp);
            cache.remove(startTimestamp);
            cache.flush();
            SettableFuture<Void> f = SettableFuture.create();
            f.set(null);
            return f;
        }

        @Override
        public ListenableFuture<Boolean> tryInvalidateTransaction(long startTimestamp) {
            startTimestamp = removeCheckpointBits(startTimestamp);
            SettableFuture<Boolean> f = SettableFuture.create();
            Long value = cache.putIfAbsent(startTimestamp, INVALID_TRANSACTION_MARKER);
            cache.flush();
            f.set(value == null || value == INVALID_TRANSACTION_MARKER);
            return f;
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Getters
    // ----------------------------------------------------------------------------------------------------------------

    @Override
    public Writer getWriter() throws IOException {
        return new KarelDbWriter();
    }

    @Override
    public Client getClient() throws IOException {
        return new KarelDbClient();
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Helper methods
    // ----------------------------------------------------------------------------------------------------------------
    static long removeCheckpointBits(long startTimestamp) {
        return startTimestamp - (startTimestamp % CommitTable.MAX_CHECKPOINTS_PER_TXN);
    }
}
