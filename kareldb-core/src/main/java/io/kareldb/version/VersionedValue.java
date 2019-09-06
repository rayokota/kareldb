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

package io.kareldb.version;

import static io.kareldb.version.TxVersionedCache.PENDING_TX;

public class VersionedValue {
    private final long version;
    private final long commit;
    private final boolean deleted;
    private final Comparable[] value;

    public VersionedValue(long version, Comparable[] value) {
        this.version = version;
        this.commit = PENDING_TX;
        this.deleted = false;
        this.value = value;
    }

    public VersionedValue(long version, long commit, Comparable[] value) {
        this.version = version;
        this.commit = commit;
        this.deleted = false;
        this.value = value;
    }

    public VersionedValue(long version, long commit, boolean deleted, Comparable[] value) {
        this.version = version;
        this.commit = commit;
        this.deleted = deleted;
        this.value = value;
    }

    public long getVersion() {
        return version;
    }

    public long getCommit() {
        return commit;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public Comparable[] getValue() {
        return value;
    }
}

