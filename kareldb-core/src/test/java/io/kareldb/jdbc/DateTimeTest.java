/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.DateTimeTest

       Licensed to the Apache Software Foundation (ASF) under one
       or more contributor license agreements.  See the NOTICE file
       distributed with this work for additional information
       regarding copyright ownership.  The ASF licenses this file
       to you under the Apache License, Version 2.0 (the
       "License"); you may not use this file except in compliance
       with the License.  You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

       Unless required by applicable law or agreed to in writing,
       software distributed under the License is distributed on an
       "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
       KIND, either express or implied.  See the License for the
       specific language governing permissions and limitations
       under the License
*/
package io.kareldb.jdbc;

import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Test the builtin date/time types assumes these builtin types
 * exist: int, varchar, real other things we might test:
 * interaction with UUID and other user defined types
 * compatibility with dynamic parameters and JDBC getDate etc. methods.
 */
public final class DateTimeTest extends BaseJDBCTestCase {

    @Before
    public void setUp() throws Exception {
        super.setUp();

        Statement stmt = createStatement();
        createTableForArithmeticTest(stmt);
        createTableForSyntaxTest(stmt);
        createTableForConversionTest(stmt);
        createTableForISOFormatTest(stmt);
        stmt.close();
    }

    private static void createTableForISOFormatTest(Statement st)
        throws SQLException {
        st.executeUpdate(" create table ts (ts1 timestamp, ts2 timestamp)");
    }

    private static void createTableForConversionTest(Statement st) throws SQLException {
        st.executeUpdate(
            " create table convtest(i int, d date, t time, ts timestamp)");
        st.executeUpdate(" insert into convtest values(0, date'1932-03-21',  "
            + "time'23:49:52', timestamp'1832-09-24 10:11:43.32')");
        st.executeUpdate(" insert into convtest values(1, date'0001-03-21',  "
            + "time'5:22:59', timestamp'9999-12-31 23:59:59.999999')");
        st.executeUpdate(" insert into convtest values(2, null, null, null)");
    }

    private static void createTableForSyntaxTest(Statement stmt)
        throws SQLException {
        stmt.executeUpdate("create table source (i int, s int, c varchar(10), "
            + "v varchar(50), d double precision, r real, e date, "
            + "t time, p timestamp)");

        stmt.executeUpdate(" create table target (e date not null, t time not "
            + "null, p timestamp not null)");
    }

    private static void createTableForArithmeticTest(Statement stmt)
        throws SQLException {
        stmt.executeUpdate("create table t (i int, s int, " +
            "c varchar(10), v varchar(50), d double precision," +
            " r real, e date, t time, p timestamp)");

        stmt.executeUpdate(" insert into t values (100, null, " +
            "null, null, null, null, null, null, null)");

        stmt.executeUpdate(" insert into t values (0, 100, 'hello', " +
            "'everyone is here', 200.0e0, 300.0e0, " +
            "date'1992-01-01', time'12:30:30', " +
            "timestamp'1992-01-01 12:30:30')");

        stmt.executeUpdate(" insert into t values (-1, -100, " +
            "'goodbye', 'everyone is there', -200.0e0, " +
            "-300.0e0, date'1992-01-01', time'12:30:30', " +
            "timestamp'1992-01-01 12:30:45')");
    }

    /**
     * date/times don't support math, show each combination.
     */
    @Test
    public void testArithOpers_math() throws SQLException {
        Statement st = createStatement();

        assertStatementError("42Y95", st, "select e + e from t");

        assertStatementError("42Y95", st, " select i + e from t");

        assertStatementError("42Y95", st, " select p / p from t");

        assertStatementError("42Y95", st, " select p * s from t");

        assertStatementError("42Y95", st, " select t - t from t");

        assertStatementError("42X37", st, " select -t from t");

        assertStatementError("42X37", st, " select +e from t");

        st.close();
    }

    public void testArithOpers_Comarision() throws SQLException {
        ResultSet rs = null;
        Statement st = createStatement();

        rs = st.executeQuery("select e from t where e = date'1992-01-01'");
        JDBC.assertColumnNames(rs, new String[]{"E"});
        JDBC.assertFullResultSet(rs,
            new String[][]{{"1992-01-01"}, {"1992-01-01"}}, true);

        rs = st.executeQuery(" select e from t where date'1992-01-01' = e");
        JDBC.assertColumnNames(rs, new String[]{"E"});
        JDBC.assertFullResultSet(rs,
            new String[][]{{"1992-01-01"}, {"1992-01-01"}}, true);

        rs = st.executeQuery(" select t from t where t > time'09:30:15'");
        JDBC.assertColumnNames(rs, new String[]{"T"});
        JDBC.assertFullResultSet(rs,
            new String[][]{{"12:30:30"}, {"12:30:30"}}, true);

        rs = st.executeQuery(" select t from t where time'09:30:15' < t");
        JDBC.assertColumnNames(rs, new String[]{"T"});
        JDBC.assertFullResultSet(rs,
            new String[][]{{"12:30:30"}, {"12:30:30"}}, true);

        rs = st.executeQuery(
            "select p from t where p < timestamp'1997-06-30 01:01:01'");
        JDBC.assertColumnNames(rs, new String[]{"P"});
        JDBC.assertFullResultSet(rs,
            new String[][]{{"1992-01-01 12:30:30.0"},
                {"1992-01-01 12:30:45.0"}}, true);

        rs = st.executeQuery(
            "select p from t where timestamp'1997-06-30 01:01:01' )> p");
        JDBC.assertColumnNames(rs, new String[]{"P"});
        JDBC.assertFullResultSet(rs,
            new String[][]{{"1992-01-01 12:30:30.0"},
                {"1992-01-01 12:30:45.0"}}, true);

        rs = st.executeQuery("select e from t where e >= date'1990-01-01'");
        JDBC.assertColumnNames(rs, new String[]{"E"});
        JDBC.assertFullResultSet(rs,
            new String[][]{{"1992-01-01"}, {"1992-01-01"}}, true);

        rs = st.executeQuery(" select e from t where date'1990-01-01'<= e");
        JDBC.assertColumnNames(rs, new String[]{"E"});
        JDBC.assertFullResultSet(rs,
            new String[][]{{"1992-01-01"}, {"1992-01-01"}}, true);

        rs = st.executeQuery(" select t from t where t <= time'09:30:15'");
        JDBC.assertColumnNames(rs, new String[]{"T"});
        JDBC.assertDrainResults(rs, 0);

        rs = st.executeQuery(" select t from t where time'09:30:15' >= t");
        JDBC.assertColumnNames(rs, new String[]{"T"});
        JDBC.assertDrainResults(rs, 0);

        rs = st.executeQuery(
            "select p from t where p <> timestamp'1997-06-30 01:01:01'");
        JDBC.assertColumnNames(rs, new String[]{"P"});
        JDBC.assertFullResultSet(rs,
            new String[][]{{"1992-01-01 12:30:30.0"},
                {"1992-01-01 12:30:45.0"}}, true);

        rs = st.executeQuery(
            "select p from t where timestamp'1997-06-30 01:01:01' )<> p");
        JDBC.assertColumnNames(rs, new String[]{"P"});
        JDBC.assertFullResultSet(rs,
            new String[][]{{"1992-01-01 12:30:30.0"},
                {"1992-01-01 12:30:45.0"}}, true);

        st.close();
    }

    /**
     * Show comparisons with mixed types don't work.
     */
    public void testArithOpers_CompraionOnMixedTypes() throws SQLException {
        Statement st = createStatement();

        assertStatementError("42818", st, "select e from t where e <= i");

        assertStatementError("42818", st, " select e from t where t < s");

        assertStatementError("42818", st, " select e from t where p > d");

        assertStatementError("42818", st, " select e from t where e >= t");

        assertStatementError("42818", st, " select e from t where t <> p");

        assertStatementError("42818", st, " select e from t where p = e");

        st.close();
    }

    /**
     * Look for a value that isn't in the table.
     */
    @Test
    public void testArithOpers_CompraionOnNotExistingValue()
        throws SQLException {
        ResultSet rs = null;
        Statement st = createStatement();

        rs = st.executeQuery("select e from t where e <> date'1992-01-01'");
        JDBC.assertColumnNames(rs, new String[]{"E"});
        JDBC.assertDrainResults(rs, 0);

        rs = st.executeQuery("select e from t where date'1992-01-01' <> e");
        JDBC.assertColumnNames(rs, new String[]{"E"});
        JDBC.assertDrainResults(rs, 0);

        st.close();
    }

    /**
     * Show garbage in == errors out
     */
    @Test
    public void testArithOpers_ComparisonOnGarbage() throws SQLException {
        Statement st = createStatement();

        assertStatementError("22008", st,
            "select date( 'xxxx' from t where p is null");

        assertStatementError("22007", st,
            " select time( '' from t where p is null");

        assertStatementError("22008", st,
            " select timestamp( 'is there anything here?' )from " +
                "t where p is null");

        assertStatementError("22008", st,
            " select timestamp( '1992-01- there anything here?'" +
                "from t where p is null");

        assertStatementError("22008", st,
            " select timestamp( '--::' )from t where p is null");

        assertStatementError("22007", st,
            " select time'::::' from t where p is null");

        st.close();
    }

    /**
     * Check limit values.
     */
    @Test
    public void testArithOpers_ComparisonOnLimits() throws SQLException {
        ResultSet rs = null;
        Statement st = createStatement();

        rs = st.executeQuery("values( date'0001-1-1', date'9999-12-31', "
            + "date'2000-2-29', date'2004-02-29')");
        JDBC.assertFullResultSet(rs, new String[][]{{"0001-01-01",
            "9999-12-31", "2000-02-29", "2004-02-29"}}, true);

        rs = st.executeQuery(" values( time'00:00:00', time'23:59:59')");
        JDBC.assertFullResultSet(rs,
            new String[][]{{"00:00:00", "23:59:59"}}, true);

        rs = st.executeQuery(" values( timestamp'0001-1-1 00:00:00', "
            + "timestamp'9999-12-31 23:59:59')");
        JDBC.assertFullResultSet(rs, new String[][]{{
            "0001-01-01 00:00:00", "9999-12-31 23:59:59"}}, true);

        st.close();
    }

    /**
     * Show that overflow and underflow are not allowed
     * (SQL92 would have these report errors).
     */
    @Test
    public void testArithOpers_ComparisonOnBeyondLimits() throws SQLException {
        Statement st = createStatement();

        assertStatementError("22008", st, "values( date'0000-01-01')");

        assertStatementError("22008", st, " values( date'2000-00-01')");

        assertStatementError("22008", st, " values( date'2000-01-00')");

        assertStatementError("22008", st, " values( date'10000-01-01')");

        assertStatementError("22008", st, " values( date'2000-13-01')");

        assertStatementError("22008", st, " values( date'2000-01-32')");

        assertStatementError("22008", st, " values( date'1900-02-29')");

        assertStatementError("22008", st, " values( date'2001-02-29')");

        assertStatementError("22007", st, " values( time'25:00:00')");

        assertStatementError("22007", st, " values( time'24:00:01')");

        assertStatementError("22007", st, " values( time'00:60:00')");

        assertStatementError("22007", st, " values( time'00:00:60')");

        st.close();
    }

    @Test
    public void testArithOpers_ComparisonOnNullAndNonNull()
        throws SQLException {
        ResultSet rs = null;
        Statement st = createStatement();

        rs = st.executeQuery("select e, t, p from t " +
            "where e = e or t = t or p = p");
        JDBC.assertColumnNames(rs, new String[]{"E", "T", "P"});
        JDBC.assertFullResultSet(rs, new String[][]{{"1992-01-01",
            "12:30:30", "1992-01-01 12:30:45"}, {"1992-01-01",
            "12:30:30", "1992-01-01 12:30:30"}}, true);

        rs = st.executeQuery("select * from t where e is not null " +
            "and t is not " + "null and p is not null");
        JDBC.assertColumnNames(rs, new String[]{"I", "S", "C", "V",
            "D", "R", "E", "T", "P"});
        JDBC.assertFullResultSet(rs, new String[][]{
            {"-1", "-100", "goodbye", "everyone is there", "-200.0",
                "-300.0", "1992-01-01", "12:30:30",
                "1992-01-01 12:30:45"},
            {"0", "100", "hello", "everyone is here", "200.0", "300.0",
                "1992-01-01", "12:30:30", "1992-01-01 12:30:30"}}, true);
        st.close();
    }

    /**
     * Test =SQ .
     */
    @Test
    public void testArithOpers_ComparisonOnEqualSQ() throws SQLException {
        ResultSet rs = null;
        Statement st = createStatement();

        assertStatementError("21000", st,
            "select 'fail' from t where e = (select e from t)");

        rs = st.executeQuery("select 'pass' from t " +
            "where e = (select e from t where d=200)");
        JDBC.assertFullResultSet(rs,
            new String[][]{{"pass"}, {"pass"}}, true);

        assertStatementError("21000", st,
            "select 'fail' from t where t = (select t from t)");

        rs = st.executeQuery("select 'pass' from t " +
            "where t = (select t from t where d=200)");
        JDBC.assertFullResultSet(rs,
            new String[][]{{"pass"}, {"pass"}}, true);

        assertStatementError("21000", st,
            "select 'fail' from t where p = (select p from t)");

        rs = st.executeQuery("select 'pass' from t " +
            "where p = (select p from t where d=200)");
        JDBC.assertFullResultSet(rs, new String[][]{{"pass"}}, true);

        st.close();
    }

    /**
     * Test a variety of inserts.
     */
    @Test
    public void testSyntax_Insert() throws SQLException {
        Statement st = createStatement();

        st.executeUpdate("insert into source values (1, 2, '3', '4', 5, 6, "
            + "date'1997-07-07', "
            + "time'08:08:08',timestamp'1999-09-09 09:09:09')");

        st.executeUpdate("insert into target select e,t,p from source");

        //wrong columns should fail
        assertStatementError("42821", st,
            "insert into target select p,e,t from source");

        assertStatementError("42821", st,
            " insert into target select i,s,d from source");

        assertStatementError("42821", st,
            " insert into target (t,p) select c,r from source");

        assertUpdateCount(st, 1, " delete from source");


        st.executeUpdate(" insert into source values (1000, null, null, null, "
            + "null, null, null, null, null)");

        // these fail because the target won't take a null -- of any type
        assertStatementError("23502", st,
            "insert into target values(null, null, null)");

        assertStatementError("23502", st,
            " insert into target select e,t,p from source");

        //these still fail with type errors:
        assertStatementError("42821", st,
            "insert into target select p,e,t from source");

        assertStatementError("42821", st,
            " insert into target select i,s,d from source");

        assertStatementError("42821", st,
            " insert into target (t,p)select c,r from source");

        //expect 1 row in target.
        ResultSet rs = st.executeQuery("select * from target");
        JDBC.assertColumnNames(rs, new String[]{"E", "T", "P"});
        JDBC.assertFullResultSet(rs, new String[][]{{
            "1997-07-07", "08:08:08", "1999-09-09 09:09:09"}}, true);

        st.close();
    }

    @Test
    public void testSyntax_CurrentFunctions() throws SQLException {
        ResultSet rs = null;
        Statement st = createStatement();

        st.executeUpdate(" insert into source values (1, 2, '3', " +
            "'4', 5, 6, date'1997-06-07', time'08:08:08', " +
            "timestamp'9999-09-09 09:09:09')");

        // these tests are 'funny' so that the masters won't show 
        // a diff every time.
        rs = st.executeQuery("select 'pass' from source where current_date = "
            + "current_date and current_time = current_time and "
            + "current_timestamp = current_timestamp");
        JDBC.assertFullResultSet(rs, new String[][]{{"pass"}}, true);

        rs = st.executeQuery(" select 'pass' from source where current_date > "
            + "date'1996-12-31' and current_time <= "
            + "time'23:59:59' -- may oopsie on leap second days "
            + "and current_timestamp <> timestamp( -- this comment "
            + "is just more whitespace '1996-12-31 00:00:00'");
        JDBC.assertFullResultSet(rs, new String[][]{{"pass"}}, true);
    }

    @Test
    public void testSyntax_Extract() throws SQLException {
        Statement st = createStatement();

        st.executeUpdate(" insert into source values (1, 2, '3', " +
            "'4', 5, 6, date'1997-06-07', time'08:08:08', " +
            "timestamp'9999-09-09 09:09:09')");

        ResultSet rs = st.executeQuery("select year( e), month( e), dayofmonth( date "
            + "'1997-01-15'), hour( t), minute( t), second( time"
            + "'01:01:42'), year( p), month( p), dayofmonth( p), hour( "
            + "timestamp'1992-01-01 14:11:23'), minute( p), "
            + "second( p) from source");
        JDBC.assertFullResultSet(rs, new String[][]{{"1997", "6", "15",
            "8", "8", "42", "9999", "9", "9", "14", "9", "9"}}, true);

        // extract won't work on other types
        assertStatementError("42X25", st, "select month( i) from source");

        assertStatementError("42X25", st, " select hour( d) from source");

        assertUpdateCount(st, 1,
            " update source set i=month( e), s=minute( t), d=second( p)");

        // should be true and atomics should match field named as 
        // label in date/times
        rs = st.executeQuery("select i,e as \"month\",s,t " +
            "as \"minute\",d,p as \"second\" from source " +
            "where (i = month(e)) and (s = minute(t)) " +
            "and (d = second(p))");
        // TODO fix?
        //JDBC.assertColumnNames(rs, new String[] { "I", "month",
        //    "S", "minute", "D", "second" });
        JDBC.assertColumnNames(rs, new String[]{"I", "E",
            "S", "T", "D", "P"});
        JDBC.assertFullResultSet(rs, new String[][]{{"6", "1997-06-07",
            "8", "08:08:08", "9.0", "9999-09-09 09:09:09"}}, true);

        // fields should match the fields in the date (in order)
        rs = st.executeQuery("select p, year( p) as \"year\", month( p) as "
            + "\"month\", dayofmonth( p) as \"day\", hour( p) as "
            + "\"hour\", minute( p) as \"minute\", second( p) as "
            + "\"second\" from source");
        JDBC.assertColumnNames(rs, new String[]{"P", "year", "month",
            "day", "hour", "minute", "second"});
        JDBC.assertFullResultSet(rs, new String[][]{{
            "9999-09-09 09:09:09", "9999", "9", "9",
            "9", "9", "9"}}, true);


        // jdbc escape sequences
        rs = st.executeQuery("values ({d '1999-01-12'}, {t '11:26:35'}, {ts "
            + "'1999-01-12 11:26:51'})");
        JDBC.assertFullResultSet(rs, new String[][]{{"1999-01-12",
            "11:26:35", "1999-01-12 11:26:51"}}, true);

        rs = st.executeQuery(" values year( {d '1999-01-12'})");
        JDBC.assertFullResultSet(rs, new String[][]{{"1999"}}, true);

        rs = st.executeQuery(" values hour( {t '11:28:10'})");
        JDBC.assertFullResultSet(rs, new String[][]{{"11"}}, true);

        rs = st.executeQuery(" values dayofmonth( {ts '1999-01-12 11:28:23'})");
        JDBC.assertFullResultSet(rs, new String[][]{{"12"}}, true);

        st.close();
    }

    /**
     * Random tests for date.
     */
    @Test
    public void testRandom() throws SQLException {
        Statement st = createStatement();

        st.executeUpdate("create table sertest(d date, s Date, o Date)");
        st.executeUpdate(" insert into sertest values (date'1992-01-03', " +
            "null, null)");

        ResultSet rs = st.executeQuery(" select * from sertest");
        JDBC.assertColumnNames(rs, new String[]{"D", "S", "O"});
        JDBC.assertFullResultSet(rs,
            new String[][]{{"1992-01-03", null, null}}, true);

        assertUpdateCount(st, 1, " update sertest set s=d");
        assertUpdateCount(st, 1, " update sertest set o=d");
        st.executeUpdate(" insert into sertest values (date'3245-09-09', "
            + "date'1001-06-07', date'1999-01-05')");

        rs = st.executeQuery(" select * from sertest");
        JDBC.assertColumnNames(rs, new String[]{"D", "S", "O"});
        JDBC.assertFullResultSet(rs, new String[][]{{"1992-01-03",
            "1992-01-03", "1992-01-03"}, {"3245-09-09", "1001-06-07",
            "1999-01-05"}}, true);

        rs = st.executeQuery(" select * from sertest where d > s");
        JDBC.assertColumnNames(rs, new String[]{"D", "S", "O"});
        JDBC.assertFullResultSet(rs, new String[][]{{"3245-09-09",
            "1001-06-07", "1999-01-05"}}, true);

        assertUpdateCount(st, 1, " update sertest set d=s");

        // should get type errors:
        assertStatementError("42821", st,
            "insert into sertest values (date'3245-09-09', "
                + "time'09:30:25', null)");

        assertStatementError("42821", st,
            " insert into sertest values (null, null, time'09:30:25')");

        assertStatementError("42821", st,
            " insert into sertest values (null, null, "
                + "timestamp'1745-01-01 09:30:25')");

        rs = st.executeQuery(" select * from sertest");
        JDBC.assertColumnNames(rs, new String[]{"D", "S", "O"});
        JDBC.assertFullResultSet(rs, new String[][]{{"1001-06-07",
            "1001-06-07", "1999-01-05"}, {"1992-01-03",
            "1992-01-03", "1992-01-03"}}, true);

        assertUpdateCount(st, 1, "update sertest set d=o");

        rs = st.executeQuery(" select * from sertest");
        JDBC.assertColumnNames(rs, new String[]{"D", "S", "O"});
        JDBC.assertFullResultSet(rs, new String[][]{{"1992-01-03",
            "1992-01-03", "1992-01-03"}, {"1999-01-05",
            "1001-06-07", "1999-01-05"}}, true);

        rs = st.executeQuery(" select * from sertest where s is null " +
            "and o is not null");
        JDBC.assertColumnNames(rs, new String[]{"D", "S", "O"});
        JDBC.assertDrainResults(rs, 0);

        rs = st.executeQuery("select month(s) from sertest " +
            "where s is not null");
        JDBC.assertFullResultSet(rs, new String[][]{{"1"}, {"6"}},
            true);

        rs = st.executeQuery(" select dayofmonth(o) from sertest");
        JDBC.assertFullResultSet(rs, new String[][]{{"3"}, {"5"}},
            true);

        dropTable("sertest");
    }

    @Test
    public void testConvertFromString() throws SQLException {
        Statement st = createStatement();

        st.executeUpdate("create table convstrtest(i int, d varchar(30), t varchar(30), "
            + "ts varchar)");
        st.executeUpdate(" insert into convstrtest values(0, '1932-03-21',  "
            + "'23:49:52', '1832-09-24 10:11:43.32')");
        st.executeUpdate(" insert into convstrtest values(1, null, null, null)");

        ResultSet rs = st.executeQuery("select CAST (t AS time) from convstrtest");
        JDBC.assertFullResultSet(rs, new String[][]{{"23:49:52"},
            {null}}, true);

        rs = st.executeQuery(" select CAST (d AS date) from convstrtest");
        JDBC.assertFullResultSet(rs,
            new String[][]{{"1932-03-21"}, {null}}, true);

        dropTable("convstrtest");

        st.close();
    }

    @Test
    public void testConversion_Aggregates() throws SQLException {
        Statement st = createStatement();

        //test aggregates sum should fail
        assertStatementError("42Y22", st, "select sum(d) from convtest");

        assertStatementError("42Y22", st, " select sum(t) from convtest");

        assertStatementError("42Y22", st, " select sum(ts) from convtest");

        // these should work
        ResultSet rs = st.executeQuery("select count(d) from convtest");
        JDBC.assertFullResultSet(rs, new String[][]{{"2"},}, true);

        rs = st.executeQuery(" select count(t) from convtest");
        JDBC.assertFullResultSet(rs, new String[][]{{"2"},}, true);

        rs = st.executeQuery(" select count(ts) from convtest");
        JDBC.assertFullResultSet(rs, new String[][]{{"2"},}, true);

        st.executeUpdate(" insert into convtest values(4, date'0001-03-21',  "
            + "time'5:22:59', timestamp'9999-12-31 23:59:59.999999')");

        // distinct count should be 2 not 3
        rs = st.executeQuery("select count(distinct d) from convtest");
        JDBC.assertFullResultSet(rs, new String[][]{{"2"},}, true);

        rs = st.executeQuery(" select count(distinct t) from convtest");
        JDBC.assertFullResultSet(rs, new String[][]{{"2"},}, true);

        rs = st.executeQuery(" select count(distinct ts) from convtest");
        JDBC.assertFullResultSet(rs, new String[][]{{"2"},}, true);

        // min should not be null!!!!!!!!
        rs = st.executeQuery("select min(d) from convtest");
        JDBC.assertFullResultSet(rs, new String[][]{{"0001-03-21"},},
            true);

        rs = st.executeQuery(" select min(t) from convtest");
        JDBC.assertFullResultSet(rs,
            new String[][]{{"05:22:59"},}, true);

        // show time and date separately as timestamp will be 
        // filtered out
        rs = st.executeQuery("select " +
            "CAST(CAST (min(ts) AS timestamp) AS date), " +
            "CAST(CAST (min(ts) AS timestamp) AS time) from convtest");
        JDBC.assertFullResultSet(rs, new String[][]{{"1832-09-24",
            "10:11:43"},}, true);

        rs = st.executeQuery(" select max(d) from convtest");
        JDBC.assertFullResultSet(rs,
            new String[][]{{"1932-03-21"},}, true);

        rs = st.executeQuery(" select max(t) from convtest");
        JDBC.assertFullResultSet(rs,
            new String[][]{{"23:49:52"},}, true);

        // show time and date separately as timestamp will be 
        // filtered out
        rs = st.executeQuery("select " +
            "CAST(CAST (max(ts) AS timestamp) AS date), " +
            "CAST(CAST (max(ts) AS timestamp) AS time) from convtest");
        JDBC.assertFullResultSet(rs,
            new String[][]{{"9999-12-31", "23:59:59"},}, true);

        st.close();
    }

    @Test
    public void testConversion() throws SQLException {
        Statement st = createStatement();

        // these should fail
        assertStatementError("42846", st,
            "select CAST (d AS time) from convtest");

        assertStatementError("42846", st,
            " select CAST (t AS date) from convtest");

        // these should work
        ResultSet rs = st.executeQuery(
            "select CAST (t AS time) from convtest");
        JDBC.assertFullResultSet(rs, new String[][]{{"23:49:52"},
            {"05:22:59"}, {null}}, true);

        rs = st.executeQuery(" select CAST (d AS date) from convtest");
        JDBC.assertFullResultSet(rs, new String[][]{{"1932-03-21"},
            {"0001-03-21"}, {null}}, true);

        rs = st.executeQuery(" select CAST (ts AS time) from convtest");
        JDBC.assertFullResultSet(rs, new String[][]{{"10:11:43"},
            {"23:59:59"}, {null}}, true);

        rs = st.executeQuery(" select CAST (ts AS date) from convtest");
        JDBC.assertFullResultSet(rs, new String[][]{{"1832-09-24"},
            {"9999-12-31"}, {null}}, true);

        // show time and date separately as timestamp will be 
        // filtered out
        rs = st.executeQuery("select CAST(CAST (ts AS timestamp) AS date), " +
            "CAST(CAST (ts AS timestamp) AS time) from convtest");

        JDBC.assertFullResultSet(rs, new String[][]{{"1832-09-24",
            "10:11:43"}, {"9999-12-31", "23:59:59"},
            {null, null}}, true);

        st.close();
    }

    /**
     * Null values in datetime scalar functions.
     */
    @Test
    public void testNulls() throws SQLException {
        Statement st = createStatement();

        st.executeUpdate("create table nulls (i int, t time, d date, ts timestamp)");

        st.executeUpdate(" insert into nulls values (0, null,null,null)");

        commit();

        ResultSet rs = st.executeQuery(" select hour(t) from nulls");
        JDBC.assertFullResultSet(rs, new String[][]{{null}}, true);

        rs = st.executeQuery(" select minute(t) from nulls");
        JDBC.assertFullResultSet(rs, new String[][]{{null}}, true);

        rs = st.executeQuery(" select second(t) from nulls");
        JDBC.assertFullResultSet(rs, new String[][]{{null}}, true);

        rs = st.executeQuery(" select year(d) from nulls");
        JDBC.assertFullResultSet(rs, new String[][]{{null}}, true);

        rs = st.executeQuery(" select month(d) from nulls");
        JDBC.assertFullResultSet(rs, new String[][]{{null}}, true);

        rs = st.executeQuery(" select dayofmonth(d) from nulls");
        JDBC.assertFullResultSet(rs, new String[][]{{null}}, true);

        rs = st.executeQuery(" select year(ts) from nulls");
        JDBC.assertFullResultSet(rs, new String[][]{{null}}, true);

        rs = st.executeQuery(" select month(ts) from nulls");
        JDBC.assertFullResultSet(rs, new String[][]{{null}}, true);

        rs = st.executeQuery(" select dayofmonth(ts) from nulls");
        JDBC.assertFullResultSet(rs, new String[][]{{null}}, true);

        rs = st.executeQuery(" select hour(ts) from nulls");
        JDBC.assertFullResultSet(rs, new String[][]{{null}}, true);

        rs = st.executeQuery(" select minute(ts) from nulls");
        JDBC.assertFullResultSet(rs, new String[][]{{null}}, true);

        rs = st.executeQuery(" select second(ts) from nulls");
        JDBC.assertFullResultSet(rs, new String[][]{{null}}, true);

        st.executeUpdate(" drop table nulls");

        st.close();
    }

    /**
     * Execute an SQL statement and check that it returns a single, specific
     * value.
     *
     * @param sql           the statement to execute
     * @param expectedValue the expected value
     */
    private void assertSingleValue(String sql, String expectedValue)
        throws SQLException {
        JDBC.assertSingleValueResultSet(
            createStatement().executeQuery(sql), expectedValue);
    }
}
