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
package io.kareldb.server.jdbc;

import io.kareldb.KarelDbConfig;
import io.kareldb.server.utils.RemoteClusterSslTestHarness;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SslDriverTest extends RemoteClusterSslTestHarness {
    private static final Logger LOG = LoggerFactory.getLogger(SslDriverTest.class);

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testReadWrite() throws Exception {
        final String tableName = "testReadWrite";
        try (Connection conn = DriverManager.getConnection(getJdbcUrl());
             Statement stmt = conn.createStatement()) {
            assertFalse(stmt.execute("DROP TABLE IF EXISTS " + tableName));
            assertFalse(stmt.execute("CREATE TABLE " + tableName + "(pk integer)"));
            assertEquals(1, stmt.executeUpdate("INSERT INTO " + tableName + " VALUES(1)"));
            assertEquals(1, stmt.executeUpdate("INSERT INTO " + tableName + " VALUES(2)"));
            assertEquals(1, stmt.executeUpdate("INSERT INTO " + tableName + " VALUES(3)"));

            ResultSet results = stmt.executeQuery("SELECT count(1) FROM " + tableName);
            assertTrue(results.next());
            assertEquals(3, results.getInt(1));
        }
    }

    public String getJdbcUrl() {
        final String serialization = "JSON";
        final String url = "jdbc:avatica:remote:url=https://localhost:" + serverPort
            + ";serialization=" + serialization + ";truststore=" + props.get(KarelDbConfig.SSL_TRUSTSTORE_LOCATION_CONFIG)
            + ";truststore_password=" + props.get(KarelDbConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        LOG.info("JDBC URL {}", url);
        return url;
    }
}
