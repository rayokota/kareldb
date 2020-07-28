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
package org.apache.calcite.sql.ddl;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Objects;

/**
 * Simple test example of a CREATE TABLE statement.
 */
public class SqlCreateTableExtension extends SqlCreateTable {
    public final SqlIdentifier name;
    public final SqlNodeList columnList;
    public final SqlNode query;

    /**
     * Creates a SqlCreateTable.
     */
    public SqlCreateTableExtension(SqlParserPos pos, boolean replace, boolean ifNotExists,
                                   SqlIdentifier name, SqlNodeList columnList, SqlNode query) {
        super(pos, replace, ifNotExists, name, columnList, query);
        this.name = Objects.requireNonNull(name);
        this.columnList = columnList; // may be null
        this.query = query; // for "CREATE TABLE ... AS query"; may be null
    }
}
