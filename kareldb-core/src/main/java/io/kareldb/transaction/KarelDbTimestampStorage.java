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

import io.kcache.Cache;
import org.apache.omid.timestamp.storage.TimestampStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class KarelDbTimestampStorage implements TimestampStorage {

    private static final long INITIAL_MAX_TS_VALUE = 0L;

    private static final Logger LOG = LoggerFactory.getLogger(KarelDbTimestampStorage.class);

    private static final long TSO_KEY = -1L;

    private final Cache<Long, Long> cache;

    public KarelDbTimestampStorage(Cache<Long, Long> cache) {
        this.cache = cache;
    }

    @Override
    public void updateMaxTimestamp(long previousMaxTimestamp, long newMaxTimestamp) throws IOException {
        if (newMaxTimestamp < 0) {
            LOG.error("Negative value received for maxTimestamp: {}", newMaxTimestamp);
            throw new IllegalArgumentException("Negative value received for maxTimestamp " + newMaxTimestamp);
        }
        boolean updated;
        if (previousMaxTimestamp == INITIAL_MAX_TS_VALUE) {
            updated = cache.putIfAbsent(TSO_KEY, newMaxTimestamp) == null;
        } else {
            updated = cache.replace(TSO_KEY, previousMaxTimestamp, newMaxTimestamp);
        }
        cache.flush();
        if (!updated) {
            throw new IOException("Previous max timestamp " + previousMaxTimestamp + " is incorrect");
        }
    }

    @Override
    public long getMaxTimestamp() {
        return cache.getOrDefault(TSO_KEY, INITIAL_MAX_TS_VALUE);
    }
}
