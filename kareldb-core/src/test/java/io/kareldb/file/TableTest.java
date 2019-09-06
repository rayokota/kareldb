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
package io.kareldb.file;

import com.google.common.collect.Ordering;
import io.kareldb.KarelDbEngine;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.isA;
import static org.junit.Assert.assertThat;

/**
 * Unit test of the Calcite adapter for CSV.
 */
public class TableTest {

    @After
    public void tearDown() throws Exception {
        KarelDbEngine.closeInstance();
    }

    private void close(Connection connection, Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                // ignore
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    /**
     * Tests an inline schema with a non-existent directory.
     */
    @Test
    public void testBadDirectory() throws SQLException {
        Properties info = new Properties();
        info.put("model",
            "inline:"
                + "{\n"
                + "  version: '1.0',\n"
                + "   schemas: [\n"
                + "     {\n"
                + "       type: 'custom',\n"
                + "       name: 'bad',\n"
                + "       factory: 'io.kareldb.schema.SchemaFactory',\n"
                + "       operand: {\n"
                + "         kind: 'io.kareldb.csv.CsvSchema',\n"
                + "         directory: '/does/not/exist'\n"
                + "       }\n"
                + "     }\n"
                + "   ]\n"
                + "}");

        Connection connection =
            DriverManager.getConnection("jdbc:kareldb:", info);
        // must print "directory ... not found" to stdout, but not fail
        ResultSet tables =
            connection.getMetaData().getTables(null, null, null, null);
        tables.next();
        tables.close();
        connection.close();
    }

    /**
     * Reads from a table.
     */
    @Test
    public void testSelect() throws SQLException {
        sql("model", "select * from EMPS").ok();
    }

    @Test
    public void testCreateTable() throws SQLException {
        try (Connection connection =
                 DriverManager.getConnection("jdbc:kareldb:",
                     new PropBuilder()
                         .set(CalciteConnectionProperty.MODEL, jsonPath("model"))
                         .set(CalciteConnectionProperty.PARSER_FACTORY,
                             "org.apache.calcite.sql.parser.parserextension"
                                 + ".ExtensionSqlParserImpl#FACTORY")
                         .build())) {

            Statement s = connection.createStatement();
            boolean b = s.execute("create table t (i int not null, constraint pk primary key (i))");
            assertThat(b, is(false));
            int x = s.executeUpdate("insert into t values 1");
            assertThat(x, is(1));
            x = s.executeUpdate("insert into t values 3");
            assertThat(x, is(1));

            ResultSet resultSet = s.executeQuery("select * from t");
            output(resultSet);
            resultSet.close();

            x = s.executeUpdate("delete from t where i = 3");
            assertThat(x, is(1));
            resultSet = s.executeQuery("select * from t");
            output(resultSet);
            resultSet.close();

            s = connection.createStatement();
            b = s.execute("create table t2 (i int not null, j int not null, k varchar, constraint pk primary key (j, i))");
            assertThat(b, is(false));
            x = s.executeUpdate("insert into t2 values (1, 2, 'hi')");
            assertThat(x, is(1));
            x = s.executeUpdate("insert into t2 values (3, 4, 'world')");
            assertThat(x, is(1));

            resultSet = s.executeQuery("select * from t2");
            output(resultSet);
            resultSet.close();

            x = s.executeUpdate("delete from t2 where i = 3");
            assertThat(x, is(1));
            resultSet = s.executeQuery("select * from t2");
            output(resultSet);
            resultSet.close();

            String sql = "select * from EMPS";
            resultSet = s.executeQuery(sql);
            output(resultSet);
            resultSet.close();

            s.close();
        }
    }

    @Test
    public void testAvroSelect() throws SQLException {
        sql("avro", "select * from \"users\"").ok();
        sql("avro", "select \"name\" from \"users\"").ok();
    }

    @Test
    public void testAvroCreateTable() throws SQLException {
        try (Connection connection =
                 DriverManager.getConnection("jdbc:kareldb:",
                     new PropBuilder()
                         .set(CalciteConnectionProperty.MODEL, jsonPath("avro"))
                         .set(CalciteConnectionProperty.PARSER_FACTORY,
                             "org.apache.calcite.sql.parser.parserextension"
                                 + ".ExtensionSqlParserImpl#FACTORY")
                         .build())) {

            Statement s = connection.createStatement();
            boolean b = s.execute("create table t (i int not null, constraint pk primary key (i))");
            assertThat(b, is(false));
            int x = s.executeUpdate("insert into t values 1");
            assertThat(x, is(1));
            x = s.executeUpdate("insert into t values 3");
            assertThat(x, is(1));
            ResultSet resultSet = s.executeQuery("select * from t");
            output(resultSet);
            resultSet.close();

            x = s.executeUpdate("delete from t where i = 3");
            assertThat(x, is(1));
            resultSet = s.executeQuery("select * from t");
            output(resultSet);
            resultSet.close();

            s = connection.createStatement();
            b = s.execute("create table t2 (i int not null, j int not null, k varchar, constraint pk primary key (j, i))");
            assertThat(b, is(false));
            x = s.executeUpdate("insert into t2 values (1, 2, 'hi')");
            assertThat(x, is(1));
            x = s.executeUpdate("insert into t2 values (3, 4, 'world')");
            assertThat(x, is(1));
            resultSet = s.executeQuery("select * from t2");
            output(resultSet);
            resultSet.close();
            resultSet = s.executeQuery("select * from t2 where i = 1 and j = 2");
            output(resultSet);
            resultSet.close();
            resultSet = s.executeQuery("select * from t2 where i >= 1 and j >= 2 and i < 3 and j < 4");
            output(resultSet);
            resultSet.close();

            x = s.executeUpdate("delete from t2 where i = 3");
            assertThat(x, is(1));
            resultSet = s.executeQuery("select * from t2");
            output(resultSet);
            resultSet.close();

            String sql = "select * from \"users\"";
            resultSet = s.executeQuery(sql);
            output(resultSet);
            resultSet.close();

            s.close();
        }
    }

    @Test
    public void testInsertOne() throws SQLException {
        Properties info = new Properties();
        info.put("model", jsonPath("model"));

        try (Connection connection =
                 DriverManager.getConnection("jdbc:kareldb:", info)) {
            String sql = "insert into EMPS (empno, name, deptno, gender, city,\n"
                + "  empid, age, slacker, manager, joinedat)\n"
                + "values(140, 'Bob', 140, 'F', 'Belmont',\n"
                + "  140, 140, true, false, date '1970-01-01')";

            Statement statement = connection.createStatement();
            statement.executeUpdate(sql);

            sql = "select * from EMPS";
            ResultSet resultSet = statement.executeQuery(sql);
            output(resultSet);
            resultSet.close();
            statement.close();
        }
    }

    /**
     * Test case for
     * <a href="https://issues.apache.org/jira/browse/CALCITE-898">[CALCITE-898]
     * Type inference multiplying Java long by SQL INTEGER</a>.
     */
    @Test
    public void testSelectLongMultiplyInteger() throws SQLException {
        final String sql = "select empno * 3 as e3\n"
            + "from long_emps where empno = 100";

        sql("bug", sql).checking(resultSet -> {
            try {
                assertThat(resultSet.next(), is(true));
                Long o = (Long) resultSet.getObject(1);
                assertThat(o, is(300L));
                assertThat(resultSet.next(), is(false));
            } catch (SQLException e) {
                throw TestUtil.rethrow(e);
            }
        }).ok();
    }

    @Test
    public void testPushDownProjectDumb() throws SQLException {
        // rule does not fire, because we're using 'dumb' tables in simple model
        final String sql = "explain plan for select * from EMPS";
        //final String expected = "PLAN=EnumerableInterpreter\n"
        //    + "  BindableTableScan(table=[[SALES, EMPS]])\n";
        final String expected = "PLAN=EnumerableTableScan(table=[[SALES, EMPS]])\n";
        sql("model", sql).returns(expected).ok();
    }

    @Test
    public void testFilterableSelect() throws SQLException {
        sql("filterable-model", "select name from EMPS").ok();
    }

    @Test
    public void testFilterableSelectStar() throws SQLException {
        sql("filterable-model", "select * from EMPS").ok();
    }

    /**
     * Filter that can be fully handled by FilterableTable.
     */
    @Test
    public void testFilterableWhere() throws SQLException {
        final String sql =
            "select empno, gender, name from EMPS where name = 'Eric'";
        sql("filterable-model", sql)
            .returns("EMPNO=110; GENDER=M; NAME=Eric").ok();
    }

    /**
     * Filter that can be partly handled by FilterableTable.
     */
    @Test
    public void testFilterableWhere2() throws SQLException {
        final String sql = "select empno, gender, name from EMPS\n"
            + " where gender = 'F' and empno > 125";
        sql("filterable-model", sql)
            .returns("EMPNO=130; GENDER=F; NAME=Alice").ok();
    }

    /**
     * Filter that can be slightly handled by FilterableTable.
     */
    @Test
    public void testFilterableWhere3() throws SQLException {
        final String sql = "select empno, gender, name from EMPS\n"
            + " where gender <> 'M' and empno > 125";
        sql("filterable-model", sql)
            .returns("EMPNO=130; GENDER=F; NAME=Alice")
            .ok();
    }

    /**
     * Test case for
     * <a href="https://issues.apache.org/jira/browse/CALCITE-2272">[CALCITE-2272]
     * Incorrect result for {@code name like '%E%' and city not like '%W%'}</a>.
     */
    @Test
    public void testFilterableWhereWithNot1() throws SQLException {
        sql("filterable-model",
            "select name, empno from EMPS "
                + "where name like '%E%' and city not like '%W%' ")
            .returns("NAME=Eric; EMPNO=110")
            .ok();
    }

    /**
     * Similar to {@link #testFilterableWhereWithNot1()};
     * But use the same column.
     */
    @Test
    public void testFilterableWhereWithNot2() throws SQLException {
        sql("filterable-model",
            "select name, empno from EMPS "
                + "where name like '%i%' and name not like '%W%' ")
            .returns("NAME=Eric; EMPNO=110",
                "NAME=Alice; EMPNO=130")
            .ok();
    }

    private Fluent sql(String model, String sql) {
        return new Fluent(model, sql, this::output);
    }

    /**
     * Returns a function that checks the contents of a result set against an
     * expected string.
     */
    private static Consumer<ResultSet> expect(final String... expected) {
        return resultSet -> {
            try {
                final List<String> lines = new ArrayList<>();
                TableTest.collect(lines, resultSet);
                Assert.assertEquals(Arrays.asList(expected), lines);
            } catch (SQLException e) {
                throw TestUtil.rethrow(e);
            }
        };
    }

    /**
     * Returns a function that checks the contents of a result set against an
     * expected string.
     */
    private static Consumer<ResultSet> expectUnordered(String... expected) {
        final List<String> expectedLines =
            Ordering.natural().immutableSortedCopy(Arrays.asList(expected));
        return resultSet -> {
            try {
                final List<String> lines = new ArrayList<>();
                TableTest.collect(lines, resultSet);
                Collections.sort(lines);
                Assert.assertEquals(expectedLines, lines);
            } catch (SQLException e) {
                throw TestUtil.rethrow(e);
            }
        };
    }

    private void checkSql(String sql, String model, Consumer<ResultSet> fn)
        throws SQLException {
        Connection connection = null;
        Statement statement = null;
        try {
            Properties info = new Properties();
            info.put("model", jsonPath(model));
            connection = DriverManager.getConnection("jdbc:kareldb:", info);
            statement = connection.createStatement();
            final ResultSet resultSet =
                statement.executeQuery(
                    sql);
            fn.accept(resultSet);
        } finally {
            close(connection, statement);
        }
    }

    private String jsonPath(String model) {
        return resourcePath(model + ".json");
    }

    private String resourcePath(String path) {
        return Sources.of(TableTest.class.getResource("/" + path)).file().getAbsolutePath();
    }

    private static void collect(List<String> result, ResultSet resultSet)
        throws SQLException {
        final StringBuilder buf = new StringBuilder();
        while (resultSet.next()) {
            buf.setLength(0);
            int n = resultSet.getMetaData().getColumnCount();
            String sep = "";
            for (int i = 1; i <= n; i++) {
                buf.append(sep)
                    .append(resultSet.getMetaData().getColumnLabel(i))
                    .append("=")
                    .append(resultSet.getString(i));
                sep = "; ";
            }
            result.add(Util.toLinux(buf.toString()));
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

    @Test
    public void testWackyColumns() throws SQLException {
        final String sql = "select * from wacky_column_names where false";
        sql("bug", sql).returns().ok();

        final String sql2 = "select \"joined at\", \"naME\"\n"
            + "from wacky_column_names\n"
            + "where \"2gender\" = 'F'";
        sql("bug", sql2)
            .returns("joined at=2005-09-07; naME=Wilma",
                "joined at=2007-01-01; naME=Alice")
            .ok();
    }

    /**
     * Test case for
     * <a href="https://issues.apache.org/jira/browse/CALCITE-1754">[CALCITE-1754]
     * In Csv adapter, convert DATE and TIME values to int, and TIMESTAMP values
     * to long</a>.
     */
    @Test
    public void testGroupByTimestampAdd() throws SQLException {
        final String sql = "select count(*) as c,\n"
            + "  {fn timestampadd(SQL_TSI_DAY, 1, JOINEDAT) } as t\n"
            + "from EMPS group by {fn timestampadd(SQL_TSI_DAY, 1, JOINEDAT ) } ";
        sql("model", sql)
            .returnsUnordered("C=1; T=1996-08-04",
                "C=1; T=2005-09-08",
                "C=1; T=2007-01-02",
                "C=1; T=2001-01-02")
            .ok();

        final String sql2 = "select count(*) as c,\n"
            + "  {fn timestampadd(SQL_TSI_MONTH, 1, JOINEDAT) } as t\n"
            + "from EMPS group by {fn timestampadd(SQL_TSI_MONTH, 1, JOINEDAT ) } ";
        sql("model", sql2)
            .returnsUnordered("C=1; T=2005-10-07",
                "C=1; T=2007-02-01",
                "C=1; T=2001-02-01",
                "C=1; T=1996-09-03")
            .ok();
    }

    @Test
    public void testUnionGroupByWithoutGroupKey() {
        final String sql = "select count(*) as c1 from EMPS group by NAME\n"
            + "union\n"
            + "select count(*) as c1 from EMPS group by NAME";
        sql("model", sql).ok();
    }

    private String range(int first, int count) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(i == 0 ? "(" : ", ").append(first + i);
        }
        return sb.append(')').toString();
    }

    @Test
    public void testDateType() throws SQLException {
        Properties info = new Properties();
        info.put("model", jsonPath("bug"));

        try (Connection connection =
                 DriverManager.getConnection("jdbc:kareldb:", info)) {
            ResultSet res = connection.getMetaData().getColumns(null, null,
                "DATE", "JOINEDAT");
            res.next();
            Assert.assertEquals(res.getInt("DATA_TYPE"), java.sql.Types.DATE);

            res = connection.getMetaData().getColumns(null, null,
                "DATE", "JOINTIME");
            res.next();
            Assert.assertEquals(res.getInt("DATA_TYPE"), java.sql.Types.TIME);

            res = connection.getMetaData().getColumns(null, null,
                "DATE", "JOINTIMES");
            res.next();
            Assert.assertEquals(res.getInt("DATA_TYPE"), java.sql.Types.TIMESTAMP);

            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                "select \"JOINEDAT\", \"JOINTIME\", \"JOINTIMES\" from \"DATE\" where EMPNO = 100");
            resultSet.next();

            // date
            Assert.assertEquals(java.sql.Date.class, resultSet.getDate(1).getClass());
            Assert.assertEquals(java.sql.Date.valueOf("1996-08-03"),
                resultSet.getDate(1));

            // time
            Assert.assertEquals(java.sql.Time.class, resultSet.getTime(2).getClass());
            Assert.assertEquals(java.sql.Time.valueOf("00:01:02"),
                resultSet.getTime(2));

            // timestamp
            Assert.assertEquals(Timestamp.class,
                resultSet.getTimestamp(3).getClass());
            Assert.assertEquals(Timestamp.valueOf("1996-08-03 00:01:02"),
                resultSet.getTimestamp(3));

        }
    }

    /**
     * Test case for
     * <a href="https://issues.apache.org/jira/browse/CALCITE-1072">[CALCITE-1072]
     * CSV adapter incorrectly parses TIMESTAMP values after noon</a>.
     */
    @Test
    public void testDateType2() throws SQLException {
        Properties info = new Properties();
        info.put("model", jsonPath("bug"));

        try (Connection connection =
                 DriverManager.getConnection("jdbc:kareldb:", info)) {
            Statement statement = connection.createStatement();
            final String sql = "select * from \"DATE\"\n"
                + "where EMPNO >= 140 and EMPNO < 200";
            ResultSet resultSet = statement.executeQuery(sql);
            int n = 0;
            while (resultSet.next()) {
                ++n;
                final int empId = resultSet.getInt(1);
                final String date = resultSet.getString(2);
                final String time = resultSet.getString(3);
                final String timestamp = resultSet.getString(4);
                assertThat(date, is("2015-12-31"));
                switch (empId) {
                    case 140:
                        assertThat(time, is("07:15:56"));
                        assertThat(timestamp, is("2015-12-31 07:15:56"));
                        break;
                    case 150:
                        assertThat(time, is("13:31:21"));
                        assertThat(timestamp, is("2015-12-31 13:31:21"));
                        break;
                    default:
                        throw new AssertionError();
                }
            }
            assertThat(n, is(2));
            resultSet.close();
            statement.close();
        }
    }

    /**
     * Test case for
     * <a href="https://issues.apache.org/jira/browse/CALCITE-1673">[CALCITE-1673]
     * Query with ORDER BY or GROUP BY on TIMESTAMP column throws
     * CompileException</a>.
     */
    @Test
    public void testTimestampGroupBy() throws SQLException {
        Properties info = new Properties();
        info.put("model", jsonPath("bug"));
        // Use LIMIT to ensure that results are deterministic without ORDER BY
        final String sql = "select \"EMPNO\", \"JOINTIMES\"\n"
            + "from (select * from \"DATE\" limit 1)\n"
            + "group by \"EMPNO\",\"JOINTIMES\"";
        try (Connection connection =
                 DriverManager.getConnection("jdbc:kareldb:", info);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            assertThat(resultSet.next(), is(true));
            final Timestamp timestamp = resultSet.getTimestamp(2);
            Assert.assertThat(timestamp, isA(Timestamp.class));
            // Note: This logic is time zone specific, but the same time zone is
            // used in the CSV adapter and this test, so they should cancel out.
            Assert.assertThat(timestamp,
                is(Timestamp.valueOf("1996-08-03 00:01:02.0")));
        }
    }

    /**
     * As {@link #testTimestampGroupBy()} but with ORDER BY.
     */
    @Test
    public void testTimestampOrderBy() throws SQLException {
        Properties info = new Properties();
        info.put("model", jsonPath("bug"));
        final String sql = "select \"EMPNO\",\"JOINTIMES\" from \"DATE\"\n"
            + "order by \"JOINTIMES\"";
        try (Connection connection =
                 DriverManager.getConnection("jdbc:kareldb:", info);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            assertThat(resultSet.next(), is(true));
            final Timestamp timestamp = resultSet.getTimestamp(2);
            Assert.assertThat(timestamp,
                is(Timestamp.valueOf("1996-08-03 00:01:02")));
        }
    }

    /**
     * As {@link #testTimestampGroupBy()} but with ORDER BY as well as GROUP
     * BY.
     */
    @Test
    public void testTimestampGroupByAndOrderBy() throws SQLException {
        Properties info = new Properties();
        info.put("model", jsonPath("bug"));
        final String sql = "select \"EMPNO\", \"JOINTIMES\" from \"DATE\"\n"
            + "group by \"EMPNO\",\"JOINTIMES\" order by \"JOINTIMES\"";
        try (Connection connection =
                 DriverManager.getConnection("jdbc:kareldb:", info);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            assertThat(resultSet.next(), is(true));
            final Timestamp timestamp = resultSet.getTimestamp(2);
            Assert.assertThat(timestamp,
                is(Timestamp.valueOf("1996-08-03 00:01:02")));
        }
    }

    /**
     * Test case for
     * <a href="https://issues.apache.org/jira/browse/CALCITE-1054">[CALCITE-1054]
     * NPE caused by wrong code generation for Timestamp fields</a>.
     */
    @Test
    public void testFilterOnNullableTimestamp() throws Exception {
        Properties info = new Properties();
        info.put("model", jsonPath("bug"));

        try (Connection connection =
                 DriverManager.getConnection("jdbc:kareldb:", info)) {
            final Statement statement = connection.createStatement();

            // date
            final String sql1 = "select JOINEDAT from \"DATE\"\n"
                + "where JOINEDAT < {d '2000-01-01'}\n"
                + "or JOINEDAT >= {d '2017-01-01'}";
            final ResultSet joinedAt = statement.executeQuery(sql1);
            assertThat(joinedAt.next(), is(true));
            assertThat(joinedAt.getDate(1), is(java.sql.Date.valueOf("1996-08-03")));

            // time
            final String sql2 = "select JOINTIME from \"DATE\"\n"
                + "where JOINTIME >= {t '07:00:00'}\n"
                + "and JOINTIME < {t '08:00:00'}";
            final ResultSet joinTime = statement.executeQuery(sql2);
            assertThat(joinTime.next(), is(true));
            assertThat(joinTime.getTime(1), is(java.sql.Time.valueOf("07:15:56")));

            // timestamp
            final String sql3 = "select JOINTIMES,\n"
                + "  {fn timestampadd(SQL_TSI_DAY, 1, JOINTIMES)}\n"
                + "from \"DATE\"\n"
                + "where (JOINTIMES >= {ts '2003-01-01 00:00:00'}\n"
                + "and JOINTIMES < {ts '2006-01-01 00:00:00'})\n"
                + "or (JOINTIMES >= {ts '2003-01-01 00:00:00'}\n"
                + "and JOINTIMES < {ts '2007-01-01 00:00:00'})";
            final ResultSet joinTimes = statement.executeQuery(sql3);
            assertThat(joinTimes.next(), is(true));
            assertThat(joinTimes.getTimestamp(1),
                is(Timestamp.valueOf("2005-09-07 00:00:00")));
            assertThat(joinTimes.getTimestamp(2),
                is(Timestamp.valueOf("2005-09-08 00:00:00")));

            final String sql4 = "select JOINTIMES, extract(year from JOINTIMES)\n"
                + "from \"DATE\"";
            final ResultSet joinTimes2 = statement.executeQuery(sql4);
            assertThat(joinTimes2.next(), is(true));
            assertThat(joinTimes2.getTimestamp(1),
                is(Timestamp.valueOf("1996-08-03 00:01:02")));
        }
    }

    /**
     * Test case for
     * <a href="https://issues.apache.org/jira/browse/CALCITE-1118">[CALCITE-1118]
     * NullPointerException in EXTRACT with WHERE ... IN clause if field has null
     * value</a>.
     */
    @Test
    public void testFilterOnNullableTimestamp2() throws Exception {
        Properties info = new Properties();
        info.put("model", jsonPath("bug"));

        try (Connection connection =
                 DriverManager.getConnection("jdbc:kareldb:", info)) {
            final Statement statement = connection.createStatement();
            final String sql1 = "select extract(year from JOINTIMES)\n"
                + "from \"DATE\"\n"
                + "where extract(year from JOINTIMES) in (2006, 2007)";
            final ResultSet joinTimes = statement.executeQuery(sql1);
            assertThat(joinTimes.next(), is(true));
            assertThat(joinTimes.getInt(1), is(2007));

            final String sql2 = "select extract(year from JOINTIMES),\n"
                + "  count(0) from \"DATE\"\n"
                + "where extract(year from JOINTIMES) between 2007 and 2016\n"
                + "group by extract(year from JOINTIMES)";
            final ResultSet joinTimes2 = statement.executeQuery(sql2);
            assertThat(joinTimes2.next(), is(true));
            assertThat(joinTimes2.getInt(1), is(2007));
            assertThat(joinTimes2.getLong(2), is(1L));
            assertThat(joinTimes2.next(), is(true));
            assertThat(joinTimes2.getInt(1), is(2015));
            assertThat(joinTimes2.getLong(2), is(2L));
        }
    }

    /**
     * Test case for
     * <a href="https://issues.apache.org/jira/browse/CALCITE-1427">[CALCITE-1427]
     * Code generation incorrect (does not compile) for DATE, TIME and TIMESTAMP
     * fields</a>.
     */
    @Test
    public void testNonNullFilterOnDateType() throws SQLException {
        Properties info = new Properties();
        info.put("model", jsonPath("bug"));

        try (Connection connection =
                 DriverManager.getConnection("jdbc:kareldb:", info)) {
            final Statement statement = connection.createStatement();

            // date
            final String sql1 = "select JOINEDAT from \"DATE\"\n"
                + "where JOINEDAT is not null";
            final ResultSet joinedAt = statement.executeQuery(sql1);
            assertThat(joinedAt.next(), is(true));
            assertThat(joinedAt.getDate(1).getClass(), equalTo(java.sql.Date.class));
            assertThat(joinedAt.getDate(1), is(java.sql.Date.valueOf("1996-08-03")));

            // time
            final String sql2 = "select JOINTIME from \"DATE\"\n"
                + "where JOINTIME is not null";
            final ResultSet joinTime = statement.executeQuery(sql2);
            assertThat(joinTime.next(), is(true));
            assertThat(joinTime.getTime(1).getClass(), equalTo(java.sql.Time.class));
            assertThat(joinTime.getTime(1), is(java.sql.Time.valueOf("00:01:02")));

            // timestamp
            final String sql3 = "select JOINTIMES from \"DATE\"\n"
                + "where JOINTIMES is not null";
            final ResultSet joinTimes = statement.executeQuery(sql3);
            assertThat(joinTimes.next(), is(true));
            assertThat(joinTimes.getTimestamp(1).getClass(),
                equalTo(Timestamp.class));
            assertThat(joinTimes.getTimestamp(1),
                is(Timestamp.valueOf("1996-08-03 00:01:02")));
        }
    }

    /**
     * Test case for
     * <a href="https://issues.apache.org/jira/browse/CALCITE-1427">[CALCITE-1427]
     * Code generation incorrect (does not compile) for DATE, TIME and TIMESTAMP
     * fields</a>.
     */
    @Test
    public void testGreaterThanFilterOnDateType() throws SQLException {
        Properties info = new Properties();
        info.put("model", jsonPath("bug"));

        try (Connection connection =
                 DriverManager.getConnection("jdbc:kareldb:", info)) {
            final Statement statement = connection.createStatement();

            // date
            final String sql1 = "select JOINEDAT from \"DATE\"\n"
                + "where JOINEDAT > {d '1990-01-01'}";
            final ResultSet joinedAt = statement.executeQuery(sql1);
            assertThat(joinedAt.next(), is(true));
            assertThat(joinedAt.getDate(1).getClass(), equalTo(java.sql.Date.class));
            assertThat(joinedAt.getDate(1), is(java.sql.Date.valueOf("1996-08-03")));

            // time
            final String sql2 = "select JOINTIME from \"DATE\"\n"
                + "where JOINTIME > {t '00:00:00'}";
            final ResultSet joinTime = statement.executeQuery(sql2);
            assertThat(joinTime.next(), is(true));
            assertThat(joinTime.getTime(1).getClass(), equalTo(java.sql.Time.class));
            assertThat(joinTime.getTime(1), is(java.sql.Time.valueOf("00:01:02")));

            // timestamp
            final String sql3 = "select JOINTIMES from \"DATE\"\n"
                + "where JOINTIMES > {ts '1990-01-01 00:00:00'}";
            final ResultSet joinTimes = statement.executeQuery(sql3);
            assertThat(joinTimes.next(), is(true));
            assertThat(joinTimes.getTimestamp(1).getClass(),
                equalTo(Timestamp.class));
            assertThat(joinTimes.getTimestamp(1),
                is(Timestamp.valueOf("1996-08-03 00:01:02")));
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

    /**
     * Fluent API to perform test actions.
     */
    private class Fluent {
        private final String model;
        private final String sql;
        private final Consumer<ResultSet> expect;

        Fluent(String model, String sql, Consumer<ResultSet> expect) {
            this.model = model;
            this.sql = sql;
            this.expect = expect;
        }

        /**
         * Runs the test.
         */
        Fluent ok() {
            try {
                checkSql(sql, model, expect);
                return this;
            } catch (SQLException e) {
                throw TestUtil.rethrow(e);
            }
        }

        /**
         * Assigns a function to call to test whether output is correct.
         */
        Fluent checking(Consumer<ResultSet> expect) {
            return new Fluent(model, sql, expect);
        }

        /**
         * Sets the rows that are expected to be returned from the SQL query.
         */
        Fluent returns(String... expectedLines) {
            return checking(expect(expectedLines));
        }

        /**
         * Sets the rows that are expected to be returned from the SQL query,
         * in no particular order.
         */
        Fluent returnsUnordered(String... expectedLines) {
            return checking(expectUnordered(expectedLines));
        }
    }

    static class PropBuilder {
        final Properties properties = new Properties();

        PropBuilder set(CalciteConnectionProperty p, String v) {
            properties.setProperty(p.camelName(), v);
            return this;
        }

        Properties build() {
            return properties;
        }
    }
}
