/*
Derby - Class org.apache.derbyTesting.functionTests.tests.lang.AlterTableTest

Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

 */
package io.kareldb.jdbc;

import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import static org.junit.Assert.assertNotNull;

public final class AlterTableTest extends BaseJDBCTestCase {

    private static final String CANNOT_ALTER_NON_IDENTITY_COLUMN = "42Z29";
    private static final String CANNOT_MODIFY_ALWAYS_IDENTITY_COLUMN = "42Z23";
    private static final String DUPLICATE_KEY = "23505";
    private static final String EXHAUSTED_IDENTITY_COLUMN = "2200H";

    private void createTestObjects(Statement st) throws SQLException {
        Connection conn = getConnection();
        conn.setAutoCommit(false);

        st.executeUpdate(
            "create table t0(c1 int not null, constraint p1 primary key (c1))");

        st.executeUpdate("create table t0_1(c1 int)");
        st.executeUpdate("create table t0_2(c1 int)");
        st.executeUpdate("create table t0_3(c1 int)");
        st.executeUpdate("create table t1(c1 int)");
        st.executeUpdate("create table t1_1(c1 int)");
        st.executeUpdate("create table t2(c1 int)");
        st.executeUpdate("create table t3(c1 int)");
        st.executeUpdate("create table t4(c1 int not null)");

        // do some population

        st.executeUpdate("insert into t1 values 1");
        st.executeUpdate("insert into t1_1 values 1");
        st.executeUpdate("insert into t2 values 1");
        st.executeUpdate("insert into t2 values 2");
        st.executeUpdate("insert into t3 values 1");
        st.executeUpdate("insert into t3 values 2");
        st.executeUpdate("insert into t3 values 3");
        st.executeUpdate("insert into t4 values 1, 2, 3");
        st.executeUpdate("create schema emptyschema");
    }

    private void checkWarning(Statement st, String expectedWarning)
        throws Exception {
        SQLWarning sqlWarn = (st == null) ?
            getConnection().getWarnings() : st.getWarnings();
        assertNotNull("Expected warning but found none", sqlWarn);
        assertSQLState(expectedWarning, sqlWarn);
    }

    @Test
    public void testAddColumn() throws Exception {
        Statement st = createStatement();
        createTestObjects(st);
        commit();

        // cannot alter a table when there is an open cursor on it

        PreparedStatement ps_c1 = prepareStatement("select * from t1");

        ResultSet c1 = ps_c1.executeQuery();
        c1.close();
        ps_c1.close();

        // positive tests add a non-nullable column to a non-empty table
        st.executeUpdate(
            "alter table t1 add c2 int not null default 0");

        PreparedStatement pSt = prepareStatement("select * from t2");

        ResultSet rs = pSt.executeQuery();
        JDBC.assertColumnNames(rs, new String[]{"C1"});
        JDBC.assertFullResultSet(rs, new String[][]{{"1"}, {"2"}});

        st.executeUpdate("alter table t2 add c2 int");

        // select * prepared statements do see added columns after
        // alter table

        pSt = prepareStatement("select * from t2");
        rs = pSt.executeQuery();
        JDBC.assertColumnNames(rs, new String[]{"C1", "C2"});
        JDBC.assertFullResultSet(rs, new String[][]{
            {"1", null},
            {"2", null}
        });

        // add non-nullable column to 0 row table and verify
        st.executeUpdate("alter table t0 add c2 int not null default 0");
        st.executeUpdate("insert into t0 values (1, default)");

        rs = st.executeQuery("select * from t0");
        JDBC.assertColumnNames(rs, new String[]{"C1", "C2"});
        JDBC.assertFullResultSet(rs, new String[][]{{"1", "0"}});

        st.executeUpdate("drop table t0");

        rollback();
    }

    /**
     * Test cases for altering the nullability of a column. Derby supports
     * two different syntaxes: A legacy syntax for backwards compatibility
     * ({@code ALTER TABLE t ALTER COLUMN c [NOT] NULL}), and SQL standard
     * syntax ({@code ALTER TABLE t ALTER COLUMN c SET NOT NULL}, and
     * {@code ALTER TABLE t ALTER COLUMN c DROP NOT NULL}).
     *
     * @param st a statement to use for executing SQL statements
     *           otherwise, test the legacy syntax
     * @throws SQLException if a database error occurs
     */
    private void testAlterColumnNullability(
        Statement st) throws SQLException {
        final String setNotNull = "NOT NULL";
        final String dropNotNull = "NULL";

        st.executeUpdate(
            "create table atmcn_1 (a integer, b integer not null)");

        // should fail because b cannot be null
        assertStatementError("23502", st,
            "insert into atmcn_1 (a) values (1)");

        st.executeUpdate("insert into atmcn_1 values (1,1)");

        ResultSet rs = st.executeQuery("select * from atmcn_1");

        String[] expColNames = {"A", "B"};
        JDBC.assertColumnNames(rs, expColNames);

        String[][] expRS = {
            {"1", "1"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("alter table atmcn_1 alter a integer " + setNotNull);

        // should fail because a cannot be null

        assertStatementError("23502", st,
            "insert into atmcn_1 (b) values (2)");

        st.executeUpdate("insert into atmcn_1 values (2,2)");

        rs = st.executeQuery("select * from atmcn_1");

        expColNames = new String[]{"A", "B"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][]{
            {"1", "1"},
            {"2", "2"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("alter table atmcn_1 alter b integer " + dropNotNull);
        st.executeUpdate("insert into atmcn_1 (a) values (3)");

        rs = st.executeQuery("select * from atmcn_1");

        expColNames = new String[]{"A", "B"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][]{
            {"1", "1"},
            {"2", "2"},
            {"3", null}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Now that B has a null value, trying to modify it to NOT
        // NULL should fail

        assertStatementError("X0Y80", st,
            "alter table atmcn_1 alter b integer " + setNotNull);

        // show that a column which is part of the PRIMARY KEY
        // cannot be modified NULL

        st.executeUpdate(
            "create table atmcn_2 (a integer not null, " +
                "b integer not null, constraint pk primary key (a))");

        assertStatementError("42Z20", st,
            " alter table atmcn_2 alter a " + dropNotNull);

        st.executeUpdate(
            " create table atmcn_3 (a integer not null, b " +
                "integer not null)");

        rollback();
    }

    @Test
    public void testAlterColumn() throws Exception {
        setAutoCommit(false);
        Statement st = createStatement();
        createTestObjects(st);

        // tests for ALTER TABLE ALTER COLUMN [NOT] NULL, and the
        testAlterColumnNullability(st);

        // tests for ALTER TABLE ALTER COLUMN DEFAULT

        st.executeUpdate(
            "create table atmod_1 (a0 integer, a integer default 0, b varchar(10))");

        st.executeUpdate("insert into atmod_1 values (1, 1, 'one')");
        st.executeUpdate("alter table atmod_1 alter a integer default -1");
        st.executeUpdate("insert into atmod_1 values (2, default, 'minus one')");
        st.executeUpdate("insert into atmod_1 (a0, b) values (3, 'b')");

        ResultSet rs = st.executeQuery("select * from atmod_1");

        JDBC.assertColumnNames(rs, new String[]{"A0", "A", "B"});

        String[][] expRS = {
            {"1", "1", "one"},
            {"2", "-1", "minus one"},
            {"3", "-1", "b"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("alter table atmod_1 alter a integer default 42");
        st.executeUpdate("insert into atmod_1 values(4, 3, 'three')");
        st.executeUpdate("insert into atmod_1 values (5, default, 'forty two')");

        rs = st.executeQuery("select * from atmod_1");
        JDBC.assertColumnNames(rs, new String[]{"A0", "A", "B"});
        JDBC.assertFullResultSet(rs, new String[][]{
            {"1", "1", "one"},
            {"2", "-1", "minus one"},
            {"3", "-1", "b"},
            {"4", "3", "three"},
            {"5", "42", "forty two"}
        });
    }

    // alter table tests for ALTER TABLE DROP COLUMN.
    @Test
    public void testDropColumn() throws Exception {
        Statement st = createStatement();
        createTestObjects(st);

        st.executeUpdate("create table atdc_0 (a integer)");
        st.executeUpdate("create table atdc_1 (a integer, b varchar)");
        st.executeUpdate("insert into atdc_1 values (1, 'hi')");

        JDBC.assertFullResultSet(st.executeQuery(" select * from atdc_1"),
            new String[][]{{"1", "hi"}});

        st.executeUpdate("alter table atdc_1 drop b");

        ResultSet rs =
            st.executeQuery("select * from atdc_1");
        JDBC.assertColumnNames(rs, new String[]{"A"});
        JDBC.assertSingleValueResultSet(rs, "1");

        st.executeUpdate("alter table atdc_1 add b varchar (20)");
        st.executeUpdate("update atdc_1 set b = 'new val' where a = 1");
        st.executeUpdate("insert into atdc_1 (a, b) values (2, 'two val')");

        rs =
            st.executeQuery("select * from atdc_1");
        JDBC.assertColumnNames(rs, new String[]{"A", "B"});
        JDBC.assertFullResultSet(rs,
            new String[][]{
                {"1", "new val"},
                {"2", "two val"}
            });

        st.executeUpdate("alter table atdc_1 add c integer");
        st.executeUpdate("insert into atdc_1 values (3, null, 3)");

        rs =
            st.executeQuery("select * from atdc_1");
        JDBC.assertColumnNames(rs, new String[]{"A", "B", "C"});
        JDBC.assertFullResultSet(rs,
            new String[][]{
                {"1", "new val", null},
                {"2", "two val", null},
                {"3", null, "3"}
            });

        st.executeUpdate("alter table atdc_1 drop b");

        rs =
            st.executeQuery("select * from atdc_1");
        JDBC.assertColumnNames(rs, new String[]{"A", "C"});
        JDBC.assertFullResultSet(rs,
            new String[][]{
                {"1", null},
                {"2", null},
                {"3", "3"}
            });
    }
}
