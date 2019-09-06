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

import io.kareldb.transaction.client.KarelDbTransactionManager;
import io.kareldb.version.TxVersionedCache;
import io.kareldb.version.VersionedCache;
import org.apache.omid.transaction.Transaction;
import org.apache.omid.transaction.TransactionManager;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicTest {

    private static final Logger LOG = LoggerFactory.getLogger(BasicTest.class);

    @Test
    public void testBasic() throws Exception {
        String userTableName = "MY_TX_TABLE";
        LOG.info("Creating access to Omid Transaction Manager & Transactional Table '{}'", userTableName);
        try (TransactionManager tm = KarelDbTransactionManager.newInstance()) {
            TxVersionedCache versionedCache = new TxVersionedCache(new VersionedCache(userTableName));
            Transaction tx = tm.begin();
            LOG.info("Transaction {} STARTED", tx);
            versionedCache.put(new Comparable[]{0L}, new Comparable[]{1L});
            tm.commit(tx);
            LOG.info("Transaction {} COMMITTED", tx);
        }
    }
}
