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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlAlter;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.Objects;

/**
 * Simple test example of a CREATE TABLE statement.
 */
public class SqlAlterTableExtension extends SqlAlter {
    public static final SqlOperator OPERATOR =
        new SqlSpecialOperator("ALTER TABLE", SqlKind.OTHER_DDL);

    public final boolean ifExists;
    public final SqlIdentifier name;
    public final List<Action> actions;
    public final SqlNodeList columnList;

    /**
     * Creates a SqlCreateTable.
     */
    public SqlAlterTableExtension(SqlParserPos pos, boolean ifExists,
                                  SqlIdentifier name, List<Action> actions, SqlNodeList columnList) {
        super(pos, "TABLE");
        this.ifExists = ifExists;
        this.name = Objects.requireNonNull(name);
        this.actions = actions;
        this.columnList = columnList;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(name, columnList);
    }

    @Override
    protected void unparseAlterOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        if (ifExists) {
            writer.keyword("IF NOT EXISTS");
        }
        name.unparse(writer, leftPrec, rightPrec);
        if (columnList != null) {
            SqlWriter.Frame frame = writer.startList("", "");
            for (int i = 0; i < columnList.size(); i++) {
                Action a = actions.get(i);
                SqlNode c = columnList.get(i);
                writer.sep(",");
                writer.keyword(a.name());
                c.unparse(writer, 0, 0);
            }
            writer.endList(frame);
        }
    }

    public enum Action {
        ADD, ALTER, DROP
    }
}
