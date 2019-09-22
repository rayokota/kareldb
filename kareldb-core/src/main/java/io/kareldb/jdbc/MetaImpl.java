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

import com.google.common.annotations.VisibleForTesting;
import io.kareldb.KarelDbEngine;
import io.kareldb.transaction.client.KarelDbTransaction;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteMetaImplShim;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.omid.transaction.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Helper for implementing the {@code getXxx} methods such as
 * {@link org.apache.calcite.avatica.AvaticaDatabaseMetaData#getTables}.
 */
public class MetaImpl extends CalciteMetaImplShim {

    private static final Logger log = LoggerFactory.getLogger(MetaImpl.class);

    static final Driver DRIVER = new Driver();

    private KarelDbTransaction transaction;

    public MetaImpl(AvaticaConnection connection) {
        super(connection);
        this.connProps
            .setAutoCommit(false)
            .setReadOnly(false)
            .setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        this.connProps.setDirty(false);
    }

    public AvaticaConnection getConnection() {
        return connection;
    }

    public KarelDbTransaction getTransaction() {
        return transaction;
    }

    public boolean isAutoCommit() {
        try {
            return connection.getAutoCommit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ExecuteResult prepareAndExecute(StatementHandle h,
                                           String sql, long maxRowCount, int maxRowsInFirstFrame,
                                           PrepareCallback callback) throws NoSuchStatementException {
        begin();
        try {
            return super.prepareAndExecute(h, sql, maxRowCount, maxRowsInFirstFrame, callback);
        } finally {
            if (isAutoCommit()) {
                commit(connection.handle);
            }
        }
    }

    @Override
    public Frame fetch(StatementHandle h, long offset,
                       int fetchMaxRowCount) throws NoSuchStatementException {
        begin();
        try {
            return super.fetch(h, offset, fetchMaxRowCount);
        } finally {
            if (isAutoCommit()) {
                commit(connection.handle);
            }
        }
    }

    @Override
    public ExecuteResult execute(StatementHandle h,
                                 List<TypedValue> parameterValues, int maxRowsInFirstFrame)
        throws NoSuchStatementException {
        begin();
        try {
            return super.execute(h, parameterValues, maxRowsInFirstFrame);
        } finally {
            if (isAutoCommit()) {
                commit(connection.handle);
            }
        }
    }

    @Override
    public ExecuteBatchResult executeBatch(StatementHandle h,
                                           List<List<TypedValue>> parameterValueLists) throws NoSuchStatementException {
        begin();
        try {
            return super.executeBatch(h, parameterValueLists);
        } finally {
            if (isAutoCommit()) {
                commit(connection.handle);
            }
        }
    }

    @Override
    public ExecuteBatchResult prepareAndExecuteBatch(
        final StatementHandle h,
        List<String> sqlCommands) throws NoSuchStatementException {
        begin();
        try {
            return super.prepareAndExecuteBatch(h, sqlCommands);
        } finally {
            if (isAutoCommit()) {
                commit(connection.handle);
            }
        }
    }

    /**
     * A trojan-horse method, subject to change without notice.
     */
    @VisibleForTesting
    public static CalciteConnection connect(CalciteSchema schema,
                                            JavaTypeFactory typeFactory) {
        return DRIVER.connect(schema, typeFactory);
    }

    public boolean syncResults(StatementHandle h, QueryState state, long offset)
        throws NoSuchStatementException {
        // Doesn't have application in Calcite itself.
        throw new UnsupportedOperationException();
    }

    public void begin() {
        try {
            if (transaction == null || transaction.getStatus() != Transaction.Status.RUNNING) {
                transaction = (KarelDbTransaction) KarelDbEngine.getInstance().beginTx();
            }
            KarelDbTransaction.setCurrentTransaction(transaction);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void commit(ConnectionHandle ch) {
        try {
            if (transaction != null && transaction.getStatus() == Transaction.Status.RUNNING) {
                KarelDbEngine.getInstance().commitTx(transaction);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void rollback(ConnectionHandle ch) {
        try {
            if (transaction != null && transaction.getStatus() == Transaction.Status.RUNNING) {
                KarelDbEngine.getInstance().rollbackTx(transaction);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
