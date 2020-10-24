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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.metrics.Gauge;
import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.timestamp.storage.TimestampStorage;
import org.apache.omid.tso.Panicker;
import org.apache.omid.tso.TimestampOracle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.apache.omid.metrics.MetricsUtils.name;

/**
 * The Timestamp Oracle that gives monotonically increasing timestamps.
 */
@Singleton
public class KarelDbTimestampOracle implements TimestampOracle {

    private static final Logger LOG = LoggerFactory.getLogger(KarelDbTimestampOracle.class);

    static final long TIMESTAMP_BATCH = 10_000_000 * CommitTable.MAX_CHECKPOINTS_PER_TXN; // 10 million
    private static final long TIMESTAMP_REMAINING_THRESHOLD = 1_000_000 * CommitTable.MAX_CHECKPOINTS_PER_TXN; // 1 million

    private long lastTimestamp;

    private long maxTimestamp;

    private TimestampStorage storage;
    private Panicker panicker;

    private long nextAllocationThreshold;
    private volatile long maxAllocatedTimestamp;

    private Executor executor = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("ts-persist-%d").build());

    private Runnable allocateTimestampsBatchTask = new AllocateTimestampBatchTask(0);

    private class AllocateTimestampBatchTask implements Runnable {
        long previousMaxTimestamp;

        AllocateTimestampBatchTask(long previousMaxTimestamp) {
            this.previousMaxTimestamp = previousMaxTimestamp;
        }

        @Override
        public void run() {
            long newMaxTimestamp = previousMaxTimestamp + TIMESTAMP_BATCH;
            try {
                storage.updateMaxTimestamp(previousMaxTimestamp, newMaxTimestamp);
                LOG.info("Updating timestamp oracle with max timestamp: (previous:{}, new:{})",
                    previousMaxTimestamp, newMaxTimestamp);
                maxAllocatedTimestamp = newMaxTimestamp;
                previousMaxTimestamp = newMaxTimestamp;
            } catch (Throwable e) {
                panicker.panic("Can't store the new max timestamp in timestamp oracle", e);
            }
        }
    }

    @Inject
    public KarelDbTimestampOracle(MetricsRegistry metrics,
                                  TimestampStorage tsStorage,
                                  Panicker panicker) throws IOException {

        this.storage = tsStorage;
        this.panicker = panicker;

        metrics.gauge(name("tso", "maxTimestamp"), new Gauge<Long>() {
            @Override
            public Long getValue() {
                return maxTimestamp;
            }
        });

    }

    @Override
    public void initialize() throws IOException {

        this.lastTimestamp = this.maxTimestamp = storage.getMaxTimestamp();

        this.allocateTimestampsBatchTask = new AllocateTimestampBatchTask(lastTimestamp);

        allocateTimestampsBatchTask.run();

        LOG.info("Initializing timestamp oracle with timestamp {}", this.lastTimestamp);
    }

    /**
     * Returns the next timestamp if available. Otherwise spins till the ts-persist thread allocates a new timestamp.
     */
    @SuppressWarnings("StatementWithEmptyBody")
    @Override
    public long next() {
        lastTimestamp += CommitTable.MAX_CHECKPOINTS_PER_TXN;

        if (lastTimestamp >= nextAllocationThreshold) {
            // set the nextAllocationThread to max value of long in order to
            // make sure only one call to this function will execute a thread to extend the timestamp batch.
            nextAllocationThreshold = Long.MAX_VALUE; 
            executor.execute(allocateTimestampsBatchTask);
        }

        if (lastTimestamp >= maxTimestamp) {
            assert (maxTimestamp <= maxAllocatedTimestamp);
            while (maxAllocatedTimestamp == maxTimestamp) {
                // spin
            }
            assert (maxAllocatedTimestamp > maxTimestamp);
            maxTimestamp = maxAllocatedTimestamp;
            nextAllocationThreshold = maxTimestamp - TIMESTAMP_REMAINING_THRESHOLD;
            assert (nextAllocationThreshold > lastTimestamp && nextAllocationThreshold < maxTimestamp);
            assert (lastTimestamp < maxTimestamp);
        }

        return lastTimestamp;
    }

    @Override
    public long getLast() {
        return lastTimestamp;
    }

    @Override
    public String toString() {
        return String.format("TimestampOracle -> LastTimestamp: %d, MaxTimestamp: %d", lastTimestamp, maxTimestamp);
    }

}
