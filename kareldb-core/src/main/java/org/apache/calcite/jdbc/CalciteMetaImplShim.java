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
package org.apache.calcite.jdbc;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.jdbc.CalcitePrepare.Context;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.Pair;

import java.sql.SQLException;

public class CalciteMetaImplShim extends CalciteMetaImpl {
    public CalciteMetaImplShim(AvaticaConnection connection) {
        super((CalciteConnectionImpl) connection);
    }

    // Temporary fix for CALCITE-3324
    // See https://issues.apache.org/jira/browse/CALCITE-3324
    @Override
    public ExecuteResult prepareAndExecute(StatementHandle h,
                                           String sql, long maxRowCount, int maxRowsInFirstFrame,
                                           PrepareCallback callback) throws NoSuchStatementException {
        final CalcitePrepare.CalciteSignature<Object> signature;
        try {
            final int updateCount;
            synchronized (callback.getMonitor()) {
                callback.clear();
                final CalciteConnectionImpl calciteConnection = getConnection();
                final CalciteServerStatement statement =
                    calciteConnection.server.getStatement(h);
                final Context context = statement.createPrepareContext();
                final CalcitePrepare.Query<Object> query = toQuery(context, sql);
                signature = calciteConnection.parseQuery(query, context, maxRowCount);
                statement.setSignature(signature);
                switch (signature.statementType) {
                    case CREATE:
                    case DROP:
                    case ALTER:
                    case OTHER_DDL:
                        updateCount = 0; // DDL produces no result set
                        break;
                    default:
                        updateCount = -1; // SELECT and DML produces result set
                        break;
                }
                callback.assign(signature, null, updateCount);
            }
            callback.execute();
            final MetaResultSet metaResultSet =
                new MetaResultSetImpl(h.connectionId, h.id, false, signature, null, updateCount);
            return new ExecuteResult(ImmutableList.of(metaResultSet));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        // TODO: share code with prepare and createIterable
    }

    public static class MetaResultSetImpl extends MetaResultSet {
        protected MetaResultSetImpl(String connectionId, int statementId, boolean ownStatement, Meta.Signature signature, Meta.Frame firstFrame, long updateCount) {
            super(connectionId, statementId, ownStatement, signature, firstFrame, updateCount);
        }
    }

    /**
     * Wraps the SQL string in a
     * {@link org.apache.calcite.jdbc.CalcitePrepare.Query} object, giving the
     * {@link Hook#STRING_TO_QUERY} hook chance to override.
     */
    private CalcitePrepare.Query<Object> toQuery(
        Context context, String sql) {
        final Holder<CalcitePrepare.Query<Object>> queryHolder =
            Holder.of(CalcitePrepare.Query.of(sql));
        final FrameworkConfig config = Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.Config.DEFAULT)
            .defaultSchema(context.getRootSchema().plus())
            .build();
        Hook.STRING_TO_QUERY.run(Pair.of(config, queryHolder));
        return queryHolder.get();
    }
}
