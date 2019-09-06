/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.JoinTest

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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Test cases for JOINs.
 */
public class JoinTest extends BaseJDBCTestCase {
    private static final String SYNTAX_ERROR = "42X01";
    private static final String AMBIGUOUS_COLNAME = "42X03";
    private static final String COLUMN_NOT_IN_SCOPE = "42X04";
    private static final String NON_COMPARABLE = "42818";
    private static final String NO_COLUMNS = "42X81";
    private static final String TABLE_NAME_NOT_IN_SCOPE = "42X10";
    private static final String VALUES_WITH_NULL = "42X07";

    /**
     * Test the CROSS JOIN syntax
     */
    @Test
    public void testCrossJoins() throws SQLException {
        // No auto-commit to make it easier to clean up the test tables.
        setAutoCommit(false);

        final String[][] T1 = {
            {"1", "one"}, {"2", "two"}, {"3", null},
            {"5", "five"}, {"6", "six"}
        };

        final String[][] T2 = {
            {"1", null}, {"2", "II"}, {"4", "IV"}
        };

        Statement s = createStatement();
        s.execute("create table t1(c1 varchar, c2 varchar(10))");
        fillTable("insert into t1 values (?,?)", T1);
        s.execute("create table t2(c1 varchar, c2 varchar(10))");
        fillTable("insert into t2 values (?,?)", T2);

        // Simple join
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 cross join t2"),
            cross(T1, T2));

        // Cross Join does not allow USING clause
        assertStatementError(
            SYNTAX_ERROR, s, "select * from t1 cross join t1 USING(c1)");

        // Self join
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 a cross join t1 b"),
            cross(T1, T1));

        // Change order in select list
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select t2.*, t1.* from t1 cross join t2"),
            cross(T2, T1));

        // Multiple joins
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 cross join t2 cross join t1 t3"),
            cross(T1, cross(T2, T1)));

        // Project one column
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select t1.c2 from t1 cross join t2"),
            project(new int[]{1}, cross(T1, T2)));

        // Project more columns
        // TODO
        /*
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select t1.c1, t2.c2, t2.c2 from t1 cross join t2"),
            project(new int[]{0, 3, 3}, cross(T1, T2)));
         */

        // Aggregate function
        JDBC.assertSingleValueResultSet(
            s.executeQuery("select count(*) from t1 cross join t2"),
            Integer.toString(T1.length * T2.length));

        // INNER JOIN using CROSS JOIN + WHERE
        String[][] expectedInnerJoin = new String[][]{
            {"1", "one", "1", null}, {"2", "two", "2", "II"}
        };
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 cross join t2 where t1.c1=t2.c1"),
            expectedInnerJoin);
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 inner join t2 on t1.c1=t2.c1"),
            expectedInnerJoin);

        // ORDER BY
        // TODO
        /*
        JDBC.assertFullResultSet(
            s.executeQuery("select * from t1 cross join t2 " +
                           "order by t1.c1 desc"),
            reverse(cross(T1, T2)));
         */

        // GROUP BY
        JDBC.assertFullResultSet(
            s.executeQuery("select t1.c1, count(t1.c2) from t1 cross join t2 " +
                "group by t1.c1 order by t1.c1"),
            new String[][]{
                {"1", "3"}, {"2", "3"}, {"3", "0"}, {"5", "3"}, {"6", "3"}
            });

        // Join VALUES expressions
        JDBC.assertUnorderedResultSet(
            s.executeQuery(
                "select * from (values 1,2) v1 cross join (values 'a','b') v2"),
            new String[][]{{"1", "a"}, {"1", "b"}, {"2", "a"}, {"2", "b"}});

        // Mix INNER and CROSS
        JDBC.assertUnorderedResultSet(
            s.executeQuery(
                "select * from t1 a cross join t2 b inner join t2 c on 1=1"),
            cross(T1, cross(T2, T2)));
        JDBC.assertUnorderedResultSet(
            s.executeQuery(
                "select * from t1 a inner join t2 b on 1=1 cross join t2 c"),
            cross(T1, cross(T2, T2)));
        // TODO
        /*
        JDBC.assertUnorderedResultSet(
            s.executeQuery(
                "select * from t1 a inner join (t2 b cross join t2 c) on 1=1"),
            cross(T1, cross(T2, T2)));
        // RESOLVE: The syntax below should be allowed.
        JDBC.assertUnorderedResultSet(
            s.executeQuery(
                "select * from t1 a inner join t2 b cross join t2 c on 1=1"),
            cross(T1, cross(T2, T2)));
         */

        // Check that the implicit nesting is correct.
        // A CROSS B RIGHT C should nest as (A CROSS B) RIGHT C and
        // not as A CROSS (B RIGHT C).
        //
        // 1) Would have failed if nesting was incorrect because A.C1 would be
        //    out of scope for the join specification
        JDBC.assertSingleValueResultSet(
            s.executeQuery("select count(*) from t2 a cross join " +
                "t1 b right join t2 c on a.c1=c.c1"),
            Integer.toString(T1.length * T2.length));
        // 2) Would have returned returned wrong result if nesting was
        //    incorrect
        String[][] expectedCorrectlyNested =
            new String[][]{{null, null, null, null, "4", "IV"}};
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t2 a cross join t1 b " +
                "right join t2 c on b.c1=c.c1 where c.c1='4'"),
            expectedCorrectlyNested);
        // 3) An explicitly nested query, equivalent to (2), so expect the
        //    same result
        // TODO
        /*
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from (t2 a cross join t1 b) " +
                           "right join t2 c on b.c1=c.c1 where c.c1='4'"),
            expectedCorrectlyNested);
         */
        // 4) An explicitly nested query, not equivalent to (2) or (3), so
        //    expect different results
        // TODO
        /*
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t2 a cross join (t1 b " +
                           "right join t2 c on b.c1=c.c1) where c.c1='4'"),
            new String[][] {
                {"1", null, null, null, "4", "IV"},
                {"2", "II", null, null, "4", "IV"},
                {"4", "IV", null, null, "4", "IV"}});
         */

        // ***** Negative tests *****

        // Self join must have alias to disambiguate column names
        assertStatementError(
            AMBIGUOUS_COLNAME, s, "select * from t1 cross join t1");

        // Column name must be qualified if ambiguous
        assertStatementError(
            AMBIGUOUS_COLNAME, s, "select c1 from t1 cross join t2");

        // CROSS JOIN cannot have ON clause, expect syntax error
        assertStatementError(
            SYNTAX_ERROR, s,
            "select * from t1 cross join t2 on t1.c1 = t2.c2");

        // Mixed CROSS with INNER/LEFT/RIGHT still needs ON
        assertStatementError(
            SYNTAX_ERROR, s,
            "select * from t1 inner join t2 cross join t2 t3");
        assertStatementError(
            SYNTAX_ERROR, s,
            "select * from t1 left join t2 cross join t2 t3");
        assertStatementError(
            SYNTAX_ERROR, s,
            "select * from t1 right join t2 cross join t2 t3");
        assertStatementError(
            SYNTAX_ERROR, s,
            "select * from t1 cross join t2 inner join t2 t3");
    }

    /**
     * Fill a table with rows.
     *
     * @param sql  the insert statement used to populate the table
     * @param data the rows to insert into the table
     */
    private void fillTable(String sql, String[][] data) throws SQLException {
        int id = 0;
        PreparedStatement ins = prepareStatement(sql);
        for (String[] datum : data) {
            for (int j = 0; j < datum.length; j++) {
                ins.setString(j + 1, datum[j]);
            }
            ins.executeUpdate();
        }
        ins.close();
    }

    /**
     * Calculate the Cartesian product of two tables.
     *
     * @param t1 the rows in the table on the left side
     * @param t2 the rows in the table on the right side
     * @return a two-dimensional array containing the Cartesian product of the
     * two tables (primary ordering same as t1, secondary ordering same as t2)
     */
    private static String[][] cross(String[][] t1, String[][] t2) {
        String[][] result = new String[t1.length * t2.length][];
        for (int i = 0; i < result.length; i++) {
            String[] r1 = t1[i / t2.length];
            String[] r2 = t2[i % t2.length];
            result[i] = new String[r1.length + r2.length];
            System.arraycopy(r1, 0, result[i], 0, r1.length);
            System.arraycopy(r2, 0, result[i], r1.length, r2.length);
        }
        return result;
    }

    /**
     * Project columns from a table.
     *
     * @param cols the column indexes (0-based) to project
     * @param rows the rows in the table
     * @return the projected result
     */
    private static String[][] project(int[] cols, String[][] rows) {
        String[][] result = new String[rows.length][cols.length];
        for (int i = 0; i < rows.length; i++) {
            for (int j = 0; j < cols.length; j++) {
                result[i][j] = rows[i][cols[j]];
            }
        }
        return result;
    }

    /**
     * Reverse the order of rows in a table.
     *
     * @param rows the rows in the table
     * @return the rows in reverse order
     */
    private static String[][] reverse(String[][] rows) {
        String[][] result = new String[rows.length][];
        for (int i = 0; i < rows.length; i++) {
            result[i] = rows[rows.length - 1 - i];
        }
        return result;
    }

    /**
     * Tests for the USING clause added in DERBY-4370.
     */
    // TODO @Test
    public void testUsingClause() throws SQLException {
        // No auto-commit to make it easier to clean up the test tables.
        setAutoCommit(false);

        Statement s = createStatement();

        s.execute("create table t1(a int, b int, c int)");
        s.execute("create table t2(b int, c int, d int)");
        s.execute("create table t3(d int, e varchar(5), f int)");

        s.execute("insert into t1 values (1,2,3),(2,3,4),(4,4,4)");
        s.execute("insert into t2 values (1,2,3),(2,3,4),(5,5,5)");
        s.execute("insert into t3 values " +
            "(2,'abc',3),(4,'def',5),(6,null,null)");

        // Simple one-column USING clauses for the different joins. Expected
        // column order: First, the columns from the USING clause. Then,
        // non-join columns from left side followed by non-join columns from
        // right side.
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 join t2 using (b)"),
            new String[][]{{"2", "1", "3", "3", "4"}});
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 inner join t2 using (b)"),
            new String[][]{{"2", "1", "3", "3", "4"}});
        // TODO
        /*
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 left join t2 using (b)"),
            new String[][]{
                {"2", "1", "3", "3", "4"},
                {"3", "2", "4", null, null},
                {"4", "4", "4", null, null}});
         */
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 left outer join t2 using (b)"),
            new String[][]{
                {"2", "1", "3", "3", "4"},
                {"3", "2", "4", null, null},
                {"4", "4", "4", null, null}});
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 right join t2 using (b)"),
            new String[][]{
                {"2", "1", "3", "3", "4"},
                {"1", null, null, "2", "3"},
                {"5", null, null, "5", "5"}});
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 right outer join t2 using (b)"),
            new String[][]{
                {"2", "1", "3", "3", "4"},
                {"1", null, null, "2", "3"},
                {"5", null, null, "5", "5"}});

        // Two-column clauses
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 join t2 using (b, c)"),
            new String[][]{{"2", "3", "1", "4"}});
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 join t2 using (c, b)"),
            new String[][]{{"3", "2", "1", "4"}});

        // Qualified asterisks should expand to all non-join columns
        JDBC.assertSingleValueResultSet(
            s.executeQuery("select t1.* from t1 join t2 using (b, c)"),
            "1");
        JDBC.assertSingleValueResultSet(
            s.executeQuery("select t2.* from t1 join t2 using (b, c)"),
            "4");
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select t1.*, t2.* from t1 join t2 using (b, c)"),
            new String[][]{{"1", "4"}});
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select t1.* from t1 left join t2 using (b, c)"),
            new String[][]{{"1"}, {"2"}, {"4"}});
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select t1.* from t1 right join t2 using (b, c)"),
            new String[][]{{"1"}, {null}, {null}});

        // USING clause can be in between joins or at the end
        JDBC.assertSingleValueResultSet(
            s.executeQuery(
                "select t3.e from t1 join t2 using (b) join t3 using (d)"),
            "def");
        JDBC.assertSingleValueResultSet(
            s.executeQuery(
                "select t3.e from t1 join t2 join t3 using (d) using (b)"),
            "def");

        // USING can be placed in between or after outer joins as well, but
        // then the results are different (different nesting of the joins).
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 left join t2 using (b) " +
                "right join t3 using (d)"),
            new String[][]{
                {"2", null, null, null, null, "abc", "3"},
                {"4", "2", "1", "3", "3", "def", "5"},
                {null, null, null, null, null, null, null}});
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 left join t2 " +
                "right join t3 using (d) using (b)"),
            new String[][]{
                {"2", "1", "3", "4", "3", "def", "5"},
                {"3", "2", "4", null, null, null, null},
                {"4", "4", "4", null, null, null, null}});

        // Should be able to reference a non-join column without qualifier if
        // it's unambiguous.
        JDBC.assertSingleValueResultSet(
            s.executeQuery("select a from t1 join t2 using (b, c)"),
            "1");

        // USING clause should accept quoted identifiers.
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 join t2 using (\"B\")"),
            new String[][]{{"2", "1", "3", "3", "4"}});

        // When referencing a join column X without a table qualifier in an
        // outer join, the value should be coalesce(t1.x, t2.x). That is, the
        // value should be non-null if one of the qualified columns is non-null.
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select b from t1 left join t2 using (b)"),
            new String[][]{{"2"}, {"3"}, {"4"}});
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select b from t1 right join t2 using (b)"),
            new String[][]{{"1"}, {"2"}, {"5"}});
        JDBC.assertUnorderedResultSet(s.executeQuery(
            "select d, t2.d, t3.d from t2 left join t3 using (d)"),
            new String[][]{
                {"3", "3", null},
                {"4", "4", "4"},
                {"5", "5", null}});
        JDBC.assertUnorderedResultSet(s.executeQuery(
            "select d, t2.d, t3.d from t2 right join t3 using (d)"),
            new String[][]{
                {"2", null, "2"},
                {"4", "4", "4"},
                {null, null, null}});
        JDBC.assertEmpty(s.executeQuery(
            "select * from t2 left join t3 using (d) where d is null"));
        JDBC.assertUnorderedResultSet(s.executeQuery(
            "select * from t2 right join t3 using (d) where d is null"),
            new String[][]{{null, null, null, null, null}});

        // Verify that ORDER BY picks up the correct column.
        JDBC.assertFullResultSet(
            s.executeQuery("select c from t1 left join t2 using (b, c) " +
                "order by c desc nulls last"),
            new String[][]{{"4"}, {"4"}, {"3"}});
        JDBC.assertFullResultSet(
            s.executeQuery("select c from t1 left join t2 using (b, c) " +
                "order by t1.c desc nulls last"),
            new String[][]{{"4"}, {"4"}, {"3"}});
        JDBC.assertFullResultSet(
            s.executeQuery("select c from t1 left join t2 using (b, c) " +
                "order by t2.c desc nulls last"),
            new String[][]{{"3"}, {"4"}, {"4"}});
        JDBC.assertSingleValueResultSet(
            s.executeQuery("select c from t1 right join t2 using (b, c) " +
                "order by c desc nulls last fetch next row only"),
            "5");
        JDBC.assertSingleValueResultSet(
            s.executeQuery("select c from t1 right join t2 using (b, c) " +
                "order by t1.c desc nulls last fetch next row only"),
            "3");
        JDBC.assertSingleValueResultSet(
            s.executeQuery("select c from t1 right join t2 using (b, c) " +
                "order by t2.c desc nulls last fetch next row only"),
            "5");

        // Aggregate + GROUP BY
        JDBC.assertFullResultSet(
            s.executeQuery("select b, count(t2.b) from t1 left join t2 " +
                "using (b) group by b order by b"),
            new String[][]{{"2", "1"}, {"3", "0"}, {"4", "0"}});

        // Using aliases to construct common column names.
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 table_a(col1, col2, col3) " +
                "inner join t3 table_b(col1, col2, col3) " +
                "using (col1)"),
            new String[][]{
                {"2", "3", "4", "abc", "3"},
                {"4", "4", "4", "def", "5"}});

        // ***** Negative tests *****

        // Use of unqualified non-join columns should result in errors if
        // columns with that name exist in both tables.
        assertStatementError(AMBIGUOUS_COLNAME, s,
            "select b from t1 join t2 using (b) join t3 using(c)");
        assertStatementError(AMBIGUOUS_COLNAME, s,
            "select b from t1 join t2 using (c)");
        assertStatementError(AMBIGUOUS_COLNAME, s,
            "select * from t1 join t2 using (b) order by c");

        // Column names in USING should not be qualified.
        assertStatementError(SYNTAX_ERROR, s,
            "select * from t1 join t2 using (t1.b)");

        // USING needs parens even if only one column is specified.
        assertStatementError(SYNTAX_ERROR, s,
            "select * from t1 join t2 using b");

        // Empty column list is not allowed.
        assertStatementError(SYNTAX_ERROR, s,
            "select * from t1 join t2 using ()");

        // Join columns with non-comparable data types should fail (trying to
        // compare INT and VARCHAR).
        assertStatementError(NON_COMPARABLE, s,
            "select * from t2 a(x,y,z) join t3 b(x,y,z) using(y)");

        // The two using clauses come in the wrong order, so expect that
        // column B is not found.
        assertStatementError(COLUMN_NOT_IN_SCOPE, s,
            "select t3.e from t1 join t2 join t3 using (b) using (d)");

        // References to non-common or non-existent columns in the using clause
        // should result in an error.
        assertStatementError(COLUMN_NOT_IN_SCOPE, s,
            "select * from t1 join t2 using (a)");
        assertStatementError(COLUMN_NOT_IN_SCOPE, s,
            "select * from t1 join t2 using (d)");
        assertStatementError(COLUMN_NOT_IN_SCOPE, s,
            "select * from t1 join t2 using (a,d)");
        assertStatementError(COLUMN_NOT_IN_SCOPE, s,
            "select * from t1 join t2 using (a,b,c)");
        assertStatementError(COLUMN_NOT_IN_SCOPE, s,
            "select * from t1 join t2 using (x)");
        assertStatementError(COLUMN_NOT_IN_SCOPE, s,
            "select * from t1 join t2 using (b,c,x)");

        // If two columns in the left table are named B, we should get an
        // error when specifying B as a join column, since we don't know which
        // of the columns to use.
        assertStatementError(AMBIGUOUS_COLNAME, s,
            "select * from (t1 cross join t2) join t2 tt2 using(b)");

        // DERBY-4407: If all the columns of table X are in the USING clause,
        // X.* will expand to no columns. A result should always have at least
        // one column.
        assertStatementError(NO_COLUMNS, s,
            "select x.* from t1 x inner join t1 y using (a,b,c)");
        assertStatementError(NO_COLUMNS, s,
            "select x.* from t1 x left join t1 y using (a,b,c)");
        assertStatementError(NO_COLUMNS, s,
            "select x.* from t1 x right join t1 y using (a,b,c)");

        // DERBY-4410: If X.* expanded to no columns, the result column that
        // immediately followed it (Y.*) would not be expanded, which eventually
        // resulted in a NullPointerException.
        assertStatementError(NO_COLUMNS, s,
            "select x.*, y.* from t1 x inner join t1 y using (a, b, c)");

        // DERBY-4414: If the table name in an asterisked identifier chain does
        // not match the table names of either side in the join, the query
        // should fail gracefully and not throw a NullPointerException.
        assertStatementError(TABLE_NAME_NOT_IN_SCOPE, s,
            "select xyz.* from t1 join t2 using (b)");
    }

    /**
     * Tests for the NATURAL JOIN syntax added in DERBY-4495.
     */
    @Test
    public void testNaturalJoin() throws SQLException {
        // No auto-commit to make it easier to clean up the test tables.
        setAutoCommit(false);

        final String[][] T1 = {
            {"1", "2", "3"}, {"4", "5", "6"}, {"7", "8", "9"}
        };

        final String[][] T2 = {
            {"4", "3", "2"}, {"1", "2", "3"}, {"3", "2", "1"}
        };

        final String[][] T3 = {{"4", "100"}};

        Statement s = createStatement();
        s.execute("create table t1(a varchar, b varchar, c varchar)");
        s.execute("create table t2(d varchar, c varchar, b varchar)");
        s.execute("create table t3(d varchar, e varchar)");

        fillTable("insert into t1 values (?,?,?)", T1);
        fillTable("insert into t2 values (?,?,?)", T2);
        fillTable("insert into t3 values (?,?)", T3);

        // Join on single common column (D)
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t2 natural join t3"),
            new String[][]{{"4", "3", "2", "100"}});
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t3 natural join t2"),
            new String[][]{{"4", "100", "3", "2"}});

        // Join on two common columns (B and C). Expected column ordering:
        //    1) all common columns, same order as in left table
        //    2) all non-common columns from left table
        //    3) all non-common columns from right table
        ResultSet rs = s.executeQuery("select * from t1 natural join t2");
        JDBC.assertColumnNames(rs, new String[]{"B", "C", "A", "D"});
        JDBC.assertUnorderedResultSet(
            rs, new String[][]{{"2", "3", "1", "4"}});

        rs = s.executeQuery("select * from t2 natural join t1");
        JDBC.assertColumnNames(rs, new String[]{"C", "B", "D", "A"});
        JDBC.assertUnorderedResultSet(
            rs, new String[][]{{"3", "2", "4", "1"}});

        // No common column names means cross join
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 natural join t3"),
            cross(T1, T3));
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 as a(c1, c2, c3) " +
                "natural join t2 as b(c4, c5, c6)"),
            cross(T1, T2));
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from (values 1,2) v1(x) " +
                "natural join (values 'a','b') v2(y)"),
            new String[][]{{"1", "a"}, {"1", "b"}, {"2", "a"}, {"2", "b"}});

        // Join two sub-queries
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from (select * from t1) table1 " +
                "natural join (select * from t2) table2"),
            new String[][]{{"2", "3", "1", "4"}});

        // Expressions with no explicit names are not common columns because
        // we give them different implicit names (typically 1, 2, 3, etc...)
        // TODO
        /*
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select * from (select b+c from t1) as x " +
                               "natural join (select b+c from t2) as y"),
                cross(new String[][] {{"5"}, {"11"}, {"17"}}, // b+c in t1
                      new String[][] {{"5"}, {"5"}, {"3"}})); // b+c in t2

        // Expressions with explicit names may be common columns, if the
        // names are equal
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select * from (select b+c c1 from t1) as x " +
                               "natural join (select b+c c1 from t2) as y"),
                new String[][] {{"5"}, {"5"}});
         */

        // Multiple JOIN operators
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 natural join t2 " +
                "natural join t3"),
            new String[][]{{"4", "2", "3", "1", "100"}});
        // TODO
        /*
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select * from (t1 natural join t2) " +
                               "natural join t3"),

                new String[][] {{"4", "2", "3", "1", "100"}});
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select * from t1 natural join " +
                               "(t2 natural join t3)"),
                new String[][] {{"2", "3", "1", "4", "100"}});
         */
        JDBC.assertUnorderedResultSet(
            s.executeQuery(
                "select * from t1 natural join t2 cross join t3"),
            new String[][]{{"2", "3", "1", "4", "4", "100"}});
        JDBC.assertUnorderedResultSet(
            s.executeQuery(
                "select * from t1 natural join t2 inner join t3 on 1=1"),
            new String[][]{{"2", "3", "1", "4", "4", "100"}});
        JDBC.assertUnorderedResultSet(
            s.executeQuery(
                "select * from t1 cross join t2 natural join t3"),
            new String[][]{
                {"4", "1", "2", "3", "3", "2", "100"},
                {"4", "4", "5", "6", "3", "2", "100"},
                {"4", "7", "8", "9", "3", "2", "100"}});
        JDBC.assertUnorderedResultSet(
            s.executeQuery(
                "select * from t1 inner join t2 on 1=1 natural join t3"),
            new String[][]{
                {"4", "1", "2", "3", "3", "2", "100"},
                {"4", "4", "5", "6", "3", "2", "100"},
                {"4", "7", "8", "9", "3", "2", "100"}});
        // TODO
        /*
        JDBC.assertUnorderedResultSet(
                s.executeQuery(
                    "select * from t1 inner join t2 natural join t3 on 1=1"),
                new String[][] {
                    {"1", "2", "3", "4", "3", "2", "100"},
                    {"4", "5", "6", "4", "3", "2", "100"},
                    {"7", "8", "9", "4", "3", "2", "100"}});
         */

        // NATURAL JOIN in INSERT context
        s.execute("create table insert_src (c1 varchar, c2 varchar, c3 varchar, c4 varchar)");
        s.execute("insert into insert_src select * from t1 natural join t2");
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from insert_src"),
            new String[][]{{"2", "3", "1", "4"}});

        // Asterisked identifier chains (common columns should not be included)
        // TODO
        /*
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select t1.* from t1 natural join t2"),
                "1");
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select t2.* from t1 natural join t2"),
                "4");
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select t1.*, t2.* from t1 natural join t2"),
                new String[][] {{"1", "4"}});
         */

        // NATURAL INNER JOIN (same as NATURAL JOIN because INNER is default)
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 natural inner join t2"),
            new String[][]{{"2", "3", "1", "4"}});

        // NATURAL LEFT (OUTER) JOIN
        String[][] ljRows = {
            {"2", "3", "1", "4"},
            {"5", "6", "4", null},
            {"8", "9", "7", null}
        };
        // TODO
        /*
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select * from t1 natural left join t2"),
                ljRows);
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select * from t1 natural left outer join t2"),
                ljRows);
        JDBC.assertUnorderedResultSet(
                s.executeQuery(
                    "select b, t1.b, t2.b from t1 natural left join t2"),
                new String[][] {
                    {"2", "2", "2"},
                    {"5", "5", null},
                    {"8", "8", null}});

        // NATURAL RIGHT (OUTER) JOIN
        String[][] rjRows = {
            {"1", "2", null, "3"},
            {"2", "3",  "1", "4"},
            {"3", "2", null, "1"}
        };
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select * from t1 natural right join t2"),
                rjRows);
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select * from t1 natural right outer join t2"),
                rjRows);
        JDBC.assertUnorderedResultSet(
                s.executeQuery(
                    "select b, t1.b, t2.b from t1 natural right join t2"),
                new String[][] {
                    {"1", null, "1"},
                    {"2",  "2", "2"},
                    {"3", null, "3"}});
         */

        // ***** Negative tests *****

        // ON or USING clause not allowed with NATURAL
        assertStatementError(
            SYNTAX_ERROR, s,
            "select * from t1 natural join t2 on t1.b=t2.b");
        assertStatementError(
            SYNTAX_ERROR, s,
            "select * from t1 natural join t2 using (b)");

        // CROSS JOIN cannot be used together with NATURAL
        assertStatementError(
            SYNTAX_ERROR, s,
            "select * from t1 natural cross join t2");

        // T has one column named D, T2 CROSS JOIN T3 has two columns named D,
        // so it's not clear which columns to join on
        assertStatementError(
            AMBIGUOUS_COLNAME, s,
            "select * from t1 t(d,x,y) natural join (t2 cross join t3)");
    }

    /**
     * Test that ON clauses can contain subqueries (DERBY-4380).
     */
    // TODO @Test
    public void testSubqueryInON() throws SQLException {
        setAutoCommit(false);

        Statement s = createStatement();
        s.execute("create table t1(a varchar)");
        s.execute("insert into t1 values '1','2','3'");
        s.execute("create table t2(b varchar)");
        s.execute("insert into t2 values '1','2'");
        s.execute("create table t3(c varchar)");
        s.execute("insert into t3 values '2','3'");

        JDBC.assertUnorderedResultSet(
            s.executeQuery(
                "select * from t1 join t2 on a = some (select c from t3)"),
            new String[][]{{"2", "1"}, {"2", "2"}, {"3", "1"}, {"3", "2"}});

        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t1 left join t2 " +
                "on a = b and b not in (select c from t3)"),
            new String[][]{{"1", "1"}, {"2", null}, {"3", null}});

        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from t3 join t2 on exists " +
                "(select * from t2 join t1 on exists " +
                "(select * from t3 where c = a))"),
            new String[][]{{"2", "1"}, {"2", "2"}, {"3", "1"}, {"3", "2"}});

        JDBC.assertSingleValueResultSet(
            s.executeQuery("select a from t1 join t2 " +
                "on a = (select count(*) from t3) and a = b"),
            "2");

        // This query used to cause NullPointerException with early versions
        // of the DERBY-4380 patch.
        JDBC.assertEmpty(s.executeQuery(
            "select * from t1 join t2 on exists " +
                "(select * from t3 x left join t3 y on 1=0 where y.c=1)"));
    }

    static void insertTourRow(PreparedStatement ps, String a, String b)
        throws SQLException {
        ps.setString(1, a);
        ps.setString(2, b);
        ps.execute();
    }

    static void insertTourRow(PreparedStatement ps, int a, String b, String c)
        throws SQLException {
        ps.setInt(1, a);
        ps.setString(2, b);
        ps.setString(3, c);
        ps.execute();
    }
}
