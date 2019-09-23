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
package io.kareldb.jdbc;

import org.apache.calcite.util.TestUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class BasicTest extends BaseJDBCTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(BasicTest.class);

    @Test
    public void testBasicAutoCommitOff() throws SQLException {
        testBasic(false);
    }

    @Test
    public void testBasicAutoCommitOn() throws SQLException {
        testBasic(true);
    }

    private void testBasic(boolean autoCommit) throws SQLException {
        try (Connection connection = createConnection()) {
            connection.setAutoCommit(autoCommit);
            Statement s = connection.createStatement();
            boolean b = s.execute("create table t (i int not null, constraint pk primary key (i))");
            assertThat(b, is(false));
            int x = s.executeUpdate("insert into t values 1");
            assertThat(x, is(1));
            x = s.executeUpdate("insert into t values 3");
            assertThat(x, is(1));
            ResultSet resultSet = s.executeQuery("select * from t where i = 1");
            String[][] expectedRows = {
                {"1"}
            };
            JDBC.assertFullResultSet(resultSet, expectedRows);
            resultSet.close();

            PreparedStatement ps = connection.prepareStatement("select * from t where i = ?");
            ps.setInt(1, 1);
            resultSet = ps.executeQuery();
            expectedRows = new String[][]{
                {"1"}
            };
            JDBC.assertFullResultSet(resultSet, expectedRows);
            resultSet.close();

            resultSet = s.executeQuery("select * from t");
            expectedRows = new String[][]{
                {"1"},
                {"3"}
            };
            JDBC.assertFullResultSet(resultSet, expectedRows);
            resultSet.close();

            resultSet = s.executeQuery("select * from t where i > 1");
            expectedRows = new String[][]{
                {"3"}
            };
            JDBC.assertFullResultSet(resultSet, expectedRows);
            resultSet.close();

            resultSet = s.executeQuery("select * from t where i < 3");
            expectedRows = new String[][]{
                {"1"}
            };
            JDBC.assertFullResultSet(resultSet, expectedRows);
            resultSet.close();

            x = s.executeUpdate("delete from t where i = 3");
            assertThat(x, is(1));
            resultSet = s.executeQuery("select * from t");
            expectedRows = new String[][]{
                {"1"}
            };
            JDBC.assertFullResultSet(resultSet, expectedRows);
            resultSet.close();

            s = connection.createStatement();
            b = s.execute("create table t2 (i int not null, j int not null, k varchar, constraint pk primary key (j, i))");
            assertThat(b, is(false));
            x = s.executeUpdate("insert into t2 values (1, 2, 'hi')");
            assertThat(x, is(1));
            x = s.executeUpdate("insert into t2 values (3, 4, 'world')");
            assertThat(x, is(1));
            resultSet = s.executeQuery("select * from t2");
            expectedRows = new String[][]{
                {"1", "2", "hi"},
                {"3", "4", "world"}
            };
            JDBC.assertFullResultSet(resultSet, expectedRows);
            resultSet.close();

            x = s.executeUpdate("update t2 set i = 10 where i = 3");
            assertThat(x, is(1));
            resultSet = s.executeQuery("select * from t2");
            expectedRows = new String[][]{
                {"1", "2", "hi"},
                {"10", "4", "world"}
            };
            JDBC.assertFullResultSet(resultSet, expectedRows);
            resultSet.close();

            x = s.executeUpdate("delete from t2 where i = 10");
            assertThat(x, is(1));
            resultSet = s.executeQuery("select * from t2");
            expectedRows = new String[][]{
                {"1", "2", "hi"}
            };
            JDBC.assertFullResultSet(resultSet, expectedRows);
            resultSet.close();

            b = s.execute("alter table t2 add m varchar null, add n int null");
            assertThat(b, is(false));
            x = s.executeUpdate("insert into t2 values (5, 6, 'world', 'why', 7)");
            assertThat(x, is(1));
            resultSet = s.executeQuery("select * from t2");
            expectedRows = new String[][]{
                {"1", "2", "hi", null, null},
                {"5", "6", "world", "why", "7"}
            };
            JDBC.assertFullResultSet(resultSet, expectedRows);
            resultSet.close();

            b = s.execute("alter table t2 drop n");
            assertThat(b, is(false));
            resultSet = s.executeQuery("select * from t2");
            expectedRows = new String[][]{
                {"1", "2", "hi", null},
                {"5", "6", "world", "why"}
            };
            JDBC.assertFullResultSet(resultSet, expectedRows);
            resultSet.close();

            x = s.executeUpdate("update t2 set k = 'yall' where i = 5");
            assertThat(x, is(1));
            resultSet = s.executeQuery("select * from t2");
            expectedRows = new String[][]{
                {"1", "2", "hi", null},
                {"5", "6", "yall", "why"}
            };
            JDBC.assertFullResultSet(resultSet, expectedRows);
            resultSet.close();

            x = s.executeUpdate("update t2 set i = 7 where i = 5");
            assertThat(x, is(1));
            resultSet = s.executeQuery("select * from t2");
            expectedRows = new String[][]{
                {"1", "2", "hi", null},
                {"7", "6", "yall", "why"}
            };
            JDBC.assertFullResultSet(resultSet, expectedRows);
            resultSet.close();

            s.close();
        }
    }

    @Test
    public void testIsolation() throws SQLException {
        try (Connection connection = createConnection();
             Connection connection2 = createConnection()) {
            Statement s = connection.createStatement();
            boolean b = s.execute("create table t3 (i int not null, j int not null, k varchar, constraint pk primary key (j, i))");
            assertThat(b, is(false));
            connection.commit();

            int x = s.executeUpdate("insert into t3 values (1, 2, 'hi')");
            assertThat(x, is(1));
            x = s.executeUpdate("insert into t3 values (3, 4, 'world')");
            assertThat(x, is(1));
            ResultSet resultSet = s.executeQuery("select * from t3");
            String[][] expectedRows = {
                {"1", "2", "hi"},
                {"3", "4", "world"}
            };
            JDBC.assertFullResultSet(resultSet, expectedRows);
            resultSet.close();

            Statement s2 = connection2.createStatement();
            x = s2.executeUpdate("insert into t3 values (5, 6, 'bye')");
            resultSet = s2.executeQuery("select * from t3");
            expectedRows = new String[][]{
                {"5", "6", "bye"}
            };
            JDBC.assertFullResultSet(resultSet, expectedRows);
            resultSet.close();

            resultSet = s.executeQuery("select * from t3");
            expectedRows = new String[][]{
                {"1", "2", "hi"},
                {"3", "4", "world"}
            };
            JDBC.assertFullResultSet(resultSet, expectedRows);
            resultSet.close();

            x = s.executeUpdate("delete from t3 where i = 3");
            assertThat(x, is(1));
            resultSet = s.executeQuery("select * from t3");
            expectedRows = new String[][]{
                {"1", "2", "hi"}
            };
            JDBC.assertFullResultSet(resultSet, expectedRows);
            resultSet.close();
            connection2.commit();
            connection.commit();

            x = s2.executeUpdate("insert into t3 values (7, 8, 'bye')");
            resultSet = s2.executeQuery("select * from t3");
            expectedRows = new String[][]{
                {"1", "2", "hi"},
                {"5", "6", "bye"},
                {"7", "8", "bye"}
            };
            JDBC.assertFullResultSet(resultSet, expectedRows);
            resultSet.close();
        }
    }

    private void output(ResultSet resultSet, PrintStream out)
        throws SQLException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
            for (int i = 1; ; i++) {
                out.print(resultSet.getString(i));
                if (i < columnCount) {
                    out.print(", ");
                } else {
                    out.println();
                    break;
                }
            }
        }
    }

    private Void output(ResultSet resultSet) {
        try {
            output(resultSet, System.out);
        } catch (SQLException e) {
            throw TestUtil.rethrow(e);
        }
        return null;
    }
}
