/**
 * Derby - Class org.apache.derbyTesting.functionTests.tests.lang.SimpleTest
 * <p>
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.kareldb.jdbc;

import org.junit.Test;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class SimpleTest extends BaseJDBCTestCase {

    /**
     * converted from supersimple.sql.  Data limits seems a
     * more relevant name.
     *
     * @throws SQLException
     */
    @Test
    public void testBasicOperations() throws SQLException {
        Connection conn = getConnection();
        Statement s = conn.createStatement();
        s.executeUpdate("create table b (i int, bi bigint, r real, f double, d double precision, n5_2 numeric(5,2), dec10_3 decimal(10,3), ch20 varchar(20),vc varchar(20), blobCol varbinary(1000))");
        s.executeUpdate("insert into b values(3,4,5.3,5.3,5.3,31.13,123456.123, 'one','one',cast(X'01ABCD' as varbinary(1000)))");
        s.executeUpdate("insert into b values(-2147483648, -9223372036854775808 ,1.2E-37, 2.225E-307, +2.225E-307,-56.12, -123456.123,'one','one', cast(X'01ABCD' as varbinary(1000)))");
        s.executeUpdate("insert into b values(0,null,null,null,null,null,null,null,null,null)");
        s.executeUpdate("insert into b values(2147483647, 9223372036854775807 ,1.4 , 3.4028235E38 ,3.4028235E38  ,999.99, 9999999.999,'one','one', cast(X'01ABCD' as varbinary(1000)))");
        ResultSet rs = s.executeQuery("select * from b");
        String[][] expectedRows = {
            {"-2147483648", "-9223372036854775808", "1.2E-37", "2.225E-307", "2.225E-307", "-56.12", "-123456.123", "one                 ", "one", "01abcd"},
            {"0", null, null, null, null, null, null, null, null, null},
            {"3", "4", "5.3", "5.3", "5.3", "31.13", "123456.123", "one                 ", "one", "01abcd"},
            {"2147483647", "9223372036854775807", "1.4", "3.4028235E38", "3.4028235E38", "999.99", "9999999.999", "one                 ", "one", "01abcd"},
        };

        JDBC.assertFullResultSet(rs, expectedRows);
        s.executeUpdate("update b set i = 1, bi = 2, r = 3.0, f = 4.56, d = 7.89, n5_2 = 101.12, dec10_3 = 8888888.888, ch20 = 'two', vc = 'two', blobCol = cast(X'DCBA01' as varbinary(1000)) where i = 3");
        rs = s.executeQuery("select * from b where i = 1");
        expectedRows = new String[][]{
            {"1", "2", "3.0", "4.56", "7.89", "101.12", "8888888.888", "two", "two", "dcba01"}
        };

        JDBC.assertFullResultSet(rs, expectedRows);
        s.executeUpdate("drop table b");

        s.executeUpdate("create table c  (si int not null,i int not null , bi bigint not null, r real not null, f double not null, d double precision not null, n5_2 numeric(5,2) not null , dec10_3 decimal(10,3) not null, ch20 varchar(20) not null ,vc varchar(20) not null, lvc varchar not null,  blobCol varbinary(1000) not null,  clobCol varchar(1000) not null)");
        s.executeUpdate("insert into c values(2,3,4,5.3,5.3,5.3,31.13,123456.123, 'one','one','one', cast(X'01ABCD' as varbinary(1000)), 'one')");
        s.executeUpdate("insert into c values(-32768,-2147483648, -9223372036854775808 ,1.2E-37, 2.225E-307, +2.225E-307,-56.12, -123456.123,'one','one','one', cast(X'01ABCD' as varbinary(1000)),'one')");
        rs = s.executeQuery("select * from c");
        expectedRows = new String[][]{
            {"-32768", "-2147483648", "-9223372036854775808", "1.2E-37", "2.225E-307", "2.225E-307", "-56.12", "-123456.123", "one                 ", "one", "one", "01abcd", "one"},
            {"2", "3", "4", "5.3", "5.3", "5.3", "31.13", "123456.123", "one                 ", "one", "one", "01abcd", "one"}};
        JDBC.assertFullResultSet(rs, expectedRows);
        s.executeUpdate("drop table c");

        s.executeUpdate("create table d  (i int not null, b boolean, dt date, t time, ts timestamp)");
        s.executeUpdate("insert into d values(1, true, date'1992-01-01', time'12:30:30', timestamp'1992-01-01 12:30:30')");
        rs = s.executeQuery("select * from d");
        expectedRows = new String[][]{
            {"1", "true", "1992-01-01", "12:30:30", "1992-01-01 12:30:30"}};
        JDBC.assertFullResultSet(rs, expectedRows);
        s.executeUpdate("update d set b = false, dt = date'1993-02-02', t = time'1:15:15', ts = timestamp'1993-02-02 1:15:15' where i = 1");
        rs = s.executeQuery("select * from d");
        expectedRows = new String[][]{
            {"1", "false", "1993-02-02", "01:15:15", "1993-02-02 01:15:15"}};
        JDBC.assertFullResultSet(rs, expectedRows);
        s.executeUpdate("drop table d");

        // test large number of columns
        rs = s.executeQuery("values ( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84)");
        expectedRows = new String[][]{{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "50", "51", "52", "53", "54", "55", "56", "57", "58", "59", "60", "61", "62", "63", "64", "65", "66", "67", "68", "69", "70", "71", "72", "73", "74", "75", "76", "77", "78", "79", "80", "81", "82", "83", "84"}};
        JDBC.assertFullResultSet(rs, expectedRows);
        rs = s.executeQuery("values(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90,91, 92, 93, 94, 95, 96, 97, 98, 99, 100)");
        expectedRows = new String[][]{{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "50", "51", "52", "53", "54", "55", "56", "57", "58", "59", "60", "61", "62", "63", "64", "65", "66", "67", "68", "69", "70", "71", "72", "73", "74", "75", "76", "77", "78", "79", "80", "81", "82", "83", "84", "85", "86", "87", "88", "89", "90", "91", "92", "93", "94", "95", "96", "97", "98", "99", "100"}};
        JDBC.assertFullResultSet(rs, expectedRows);
        // test commit and rollback
        conn.setAutoCommit(false);
        s.executeUpdate("create table a (a int)");
        s.executeUpdate("insert into a values(1)");
        rs = s.executeQuery("select * from a");
        JDBC.assertFullResultSet(rs, new String[][]{{"1"}});
        conn.commit();
        s.executeUpdate("drop table a");
        conn.commit();
    }

    @Test
    public void testBasic() throws Exception {
        Connection conn = getConnection();
        Statement s = conn.createStatement();
        s.execute("create table books (id int, name varchar, author varchar)");
        s.executeUpdate("insert into books values(1, 'The Trial', 'Franz Kafka')");
        ResultSet rs = s.executeQuery("select * from books");
        JDBC.assertFullResultSet(rs, new String[][]{{"1", "The Trial", "Franz Kafka"}});
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
}
