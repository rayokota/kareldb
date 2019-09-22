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

import io.kareldb.schema.Schema;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;

import java.io.IOException;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Parse tree for {@code DROP TABLE} statement.
 */
public class SqlDropTableExtension extends SqlDropObject {
    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("DROP TABLE", SqlKind.DROP_TABLE);

    /**
     * Creates a SqlDropTable.
     */
    public SqlDropTableExtension(SqlParserPos pos, boolean ifExists, SqlIdentifier name) {
        super(OPERATOR, pos, ifExists, name);
    }

    @Override
    public void execute(CalcitePrepare.Context context) {
        final List<String> path = context.getDefaultSchemaPath();
        CalciteSchema schema = context.getRootSchema();
        for (String p : path) {
            schema = schema.getSubSchema(p, true);
        }

        final Pair<CalciteSchema, String> pair =
            SqlDdlNodes.schema(context, true, name);
        switch (getKind()) {
            case DROP_TABLE:
                Schema schemaPlus = schema.plus().unwrap(Schema.class);
                schemaPlus.dropTable(name.getSimple());
                boolean existed = pair.left.removeTable(name.getSimple());
                if (!existed && !ifExists) {
                    throw SqlUtil.newContextException(name.getParserPosition(),
                        RESOURCE.tableNotFound(name.getSimple()));
                }
                break;
            default:
                throw new AssertionError(getKind());
        }
    }
}
