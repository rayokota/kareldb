/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kareldb.transaction;

import org.apache.omid.timestamp.storage.TimestampStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemoryTimestampStorage implements TimestampStorage {

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryTimestampStorage.class);

    private long maxTime = 0;

    @Override
    public void updateMaxTimestamp(long previousMaxTime, long nextMaxTime) {
        maxTime = nextMaxTime;
        LOG.info("Updating max timestamp: (previous:{}, new:{})", previousMaxTime, nextMaxTime);
    }

    @Override
    public long getMaxTimestamp() {
        return maxTime;
    }

}
