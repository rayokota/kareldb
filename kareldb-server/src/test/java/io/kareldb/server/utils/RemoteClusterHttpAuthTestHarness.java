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
package io.kareldb.server.utils;

import com.google.common.io.Files;
import io.kareldb.KarelDbConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Common test logic for HTTP basic and digest auth
 */
public abstract class RemoteClusterHttpAuthTestHarness extends RemoteClusterTestHarness {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteClusterSslTestHarness.class);

    private File passwordFile;
    private File jaasConfigFile;

    public RemoteClusterHttpAuthTestHarness() {
        super();
    }

    public RemoteClusterHttpAuthTestHarness(int numBrokers) {
        super(numBrokers);
    }

    protected abstract String getAuthMethod();

    @Before
    public void setUp() throws Exception {
        tempDir = Files.createTempDir();
        passwordFile = new File(tempDir, "password-file");
        jaasConfigFile = new File(tempDir, "jaas_config.file");
        writePasswordFile(passwordFile);
        writeJaasFile(jaasConfigFile, passwordFile);
        System.setProperty("java.security.auth.login.config", jaasConfigFile.getAbsolutePath());
        super.setUp();
    }

    @Override
    protected void injectKarelDbProperties(Properties props) {
        super.injectKarelDbProperties(props);

        props.put(KarelDbConfig.AUTHENTICATION_METHOD_CONFIG, getAuthMethod());
        props.put(KarelDbConfig.AUTHENTICATION_ROLES_CONFIG, "admin,user");
        props.put(KarelDbConfig.AUTHENTICATION_REALM_CONFIG, "KarelDb-Props");
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testValidUser() throws Exception {
        // Allowed by avatica and hsqldb
        final Properties props = new Properties();
        props.put("avatica_user", "fred");
        props.put("avatica_password", "letmein");

        readWriteData(getJdbcUrl(), "VALID_USER", props);
    }

    @Test
    public void testInvalidUser() throws Exception {
        // Denied by avatica
        final Properties props = new Properties();
        props.put("avatica_user", "foo");
        props.put("avatica_password", "bar");

        try {
            readWriteData(getJdbcUrl(), "INVALID_USER", props);
            fail("Expected an exception");
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), containsString("HTTP/401"));
        }
    }

    @Test
    public void testUserWithDisallowedRole() throws Exception {
        // Disallowed by avatica
        final Properties props = new Properties();
        props.put("avatica_user", "harry");
        props.put("avatica_password", "changeme");

        try {
            readWriteData(getJdbcUrl(), "DISALLOWED_AVATICA_USER", props);
            fail("Expected an exception");
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), containsString("HTTP/403"));
        }
    }

    public String getJdbcUrl() {
        final String serialization = "JSON";
        final String url = "jdbc:avatica:remote:url=http://localhost:" + serverPort
            + ";authentication=" + getAuthMethod() + ";serialization=" + serialization;
        LOG.info("JDBC URL {}", url);
        return url;
    }

    protected void readWriteData(String url, String tableName, Properties props) throws Exception {
        try (Connection conn = DriverManager.getConnection(url, props);
             Statement stmt = conn.createStatement()) {
            assertFalse(stmt.execute("DROP TABLE IF EXISTS " + tableName));
            assertFalse(stmt.execute("CREATE TABLE " + tableName + " (pk integer, msg varchar(10))"));

            assertEquals(1, stmt.executeUpdate("INSERT INTO " + tableName + " VALUES(1, 'abcd')"));
            assertEquals(1, stmt.executeUpdate("INSERT INTO " + tableName + " VALUES(2, 'bcde')"));
            assertEquals(1, stmt.executeUpdate("INSERT INTO " + tableName + " VALUES(3, 'cdef')"));

            ResultSet results = stmt.executeQuery("SELECT count(1) FROM " + tableName);
            assertNotNull(results);
            assertTrue(results.next());
            assertEquals(3, results.getInt(1));
        }
    }

    public static void writeJaasFile(File configFile, File passwordFile)
        throws Exception {
        try (BufferedWriter writer =
                 new BufferedWriter(
                     new OutputStreamWriter(
                         new FileOutputStream(configFile),
                         StandardCharsets.UTF_8))) {
            writer.write("KarelDb-Props {\n");
            writer.write(" org.eclipse.jetty.jaas.spi.PropertyFileLoginModule required\n");
            writer.write(" file=\"" + passwordFile.getPath() + "\"\n");
            writer.write(" debug=\"false\";\n");
            writer.write("};\n");
        }
    }

    public static void writePasswordFile(File passwordFile)
        throws Exception {
        try (BufferedWriter writer =
                 new BufferedWriter(
                     new OutputStreamWriter(
                         new FileOutputStream(passwordFile),
                         StandardCharsets.UTF_8))) {
            writer.write("fred: OBF:1w8t1tvf1w261w8v1w1c1tvn1w8x,user,admin\n");
            writer.write("harry: changeme,developer\n");
            writer.write("tom: MD5:164c88b302622e17050af52c89945d44,user\n");
            writer.write("dick: CRYPT:adpexzg3FUZAk,admin\n");
            writer.write("};\n");
        }
    }
}
