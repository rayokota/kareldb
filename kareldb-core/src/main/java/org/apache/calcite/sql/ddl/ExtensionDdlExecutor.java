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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.kareldb.schema.ColumnStrategy.DefaultStrategy;
import io.kareldb.schema.RelDef;
import io.kareldb.schema.Schema;
import io.kareldb.schema.Table;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.ContextSqlValidator;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.schema.impl.ViewTableMacro;
import org.apache.calcite.server.DdlExecutor;
import org.apache.calcite.server.DdlExecutorImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.parserextension.ExtensionSqlParserImpl;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Util;

import java.io.Reader;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Executes the few DDL commands supported by
 * {@link ExtensionSqlParserImpl}.
 */
public class ExtensionDdlExecutor extends DdlExecutorImpl {
    static final ExtensionDdlExecutor INSTANCE = new ExtensionDdlExecutor();

    /**
     * Parser factory.
     */
    @SuppressWarnings("unused") // used via reflection
    public static final SqlParserImplFactory PARSER_FACTORY =
        new SqlParserImplFactory() {
            @Override
            public SqlAbstractParserImpl getParser(Reader stream) {
                return ExtensionSqlParserImpl.FACTORY.getParser(stream);
            }

            @Override
            public DdlExecutor getDdlExecutor() {
                return ExtensionDdlExecutor.INSTANCE;
            }
        };

    /**
     * Executes a {@code CREATE TABLE} command. Called via reflection.
     */
    public void execute(SqlCreateTableExtension create, CalcitePrepare.Context context) {
        final CalciteSchema schema =
            Schemas.subSchema(context.getRootSchema(),
                context.getDefaultSchemaPath());
        final JavaTypeFactory typeFactory = context.getTypeFactory();
        final RelDataType queryRowType;
        if (create.query != null) {
            // A bit of a hack: pretend it's a view, to get its row type
            final String sql =
                create.query.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
            final ViewTableMacro viewTableMacro =
                ViewTable.viewMacro(schema.plus(), sql, schema.path(null),
                    context.getObjectPath(), false);
            final TranslatableTable x = viewTableMacro.apply(ImmutableList.of());
            queryRowType = x.getRowType(typeFactory);

            if (create.columnList != null
                && queryRowType.getFieldCount() != create.columnList.size()) {
                throw SqlUtil.newContextException(create.columnList.getParserPosition(),
                    RESOURCE.columnCountMismatch());
            }
        } else {
            queryRowType = null;
        }
        final List<SqlNode> columnList;
        if (create.columnList != null) {
            columnList = create.columnList.getList();
        } else {
            if (queryRowType == null) {
                // "CREATE TABLE t" is invalid; because there is no "AS query" we need
                // a list of column names and types, "CREATE TABLE t (INT c)".
                throw SqlUtil.newContextException(create.name.getParserPosition(),
                    RESOURCE.createTableRequiresColumnList());
            }
            columnList = new ArrayList<>();
            for (String name : queryRowType.getFieldNames()) {
                columnList.add(new SqlIdentifier(name, SqlParserPos.ZERO));
            }
        }
        final ImmutableList.Builder<ColumnDef> b = ImmutableList.builder();
        final RelDataTypeFactory.Builder builder = typeFactory.builder();
        final RelDataTypeFactory.Builder storedBuilder = typeFactory.builder();
        final SqlValidator validator = new ContextSqlValidator(context, false);
        final List<String> keyFields = new ArrayList<>();
        final List<io.kareldb.schema.ColumnStrategy> strategies = new ArrayList<>();
        for (Ord<SqlNode> c : Ord.zip(columnList)) {
            if (c.e instanceof SqlColumnDeclaration) {
                final SqlColumnDeclaration d = (SqlColumnDeclaration) c.e;
                final RelDataType type = d.dataType.deriveType(validator, true);
                builder.add(d.name.getSimple(), type);
                if (d.strategy != ColumnStrategy.VIRTUAL) {
                    storedBuilder.add(d.name.getSimple(), type);
                }
                b.add(ColumnDef.of(d.expression, type, d.strategy));
                addStrategy(strategies, d.strategy, d.expression);
            } else if (c.e instanceof SqlIdentifier) {
                final SqlIdentifier id = (SqlIdentifier) c.e;
                if (queryRowType == null) {
                    throw SqlUtil.newContextException(id.getParserPosition(),
                        RESOURCE.createTableRequiresColumnTypes(id.getSimple()));
                }
                final RelDataTypeField f = queryRowType.getFieldList().get(c.i);
                final ColumnStrategy strategy = f.getType().isNullable()
                    ? ColumnStrategy.NULLABLE
                    : ColumnStrategy.NOT_NULLABLE;
                b.add(ColumnDef.of(c.e, f.getType(), strategy));
                builder.add(id.getSimple(), f.getType());
                storedBuilder.add(id.getSimple(), f.getType());
                addStrategy(strategies, strategy, null);
            } else if (c.e instanceof SqlKeyConstraint) {
                final SqlKeyConstraint keyConstraint = (SqlKeyConstraint) c.e;
                if (keyConstraint.getOperator() == SqlKeyConstraint.PRIMARY) {
                    List<SqlNode> operands = keyConstraint.getOperandList();
                    SqlNodeList sqlNodeList = (SqlNodeList) operands.get(1);
                    List<SqlNode> keyNodes = sqlNodeList.getList();
                    for (SqlNode keyNode : keyNodes) {
                        keyFields.add(((SqlIdentifier) keyNode).getSimple());
                    }
                }
            } else {
                throw new AssertionError(c.e.getClass());
            }
        }
        final RelDataType rowType = builder.build();
        final RelDataType storedRowType = storedBuilder.build();
        final List<ColumnDef> columns = b.build();
        final InitializerExpressionFactory ief =
            new NullInitializerExpressionFactory() {
                @Override
                public ColumnStrategy generationStrategy(RelOptTable table,
                                                         int iColumn) {
                    return columns.get(iColumn).strategy;
                }

                @Override
                public RexNode newColumnDefaultValue(RelOptTable table,
                                                     int iColumn, InitializerContext context) {
                    final ColumnDef c = columns.get(iColumn);
                    if (c.expr != null) {
                        return context.convertExpression(c.expr);
                    }
                    return super.newColumnDefaultValue(table, iColumn, context);
                }
            };

        Schema schemaPlus = schema.plus().unwrap(Schema.class);
        String tableName = create.name.getSimple();
        if (schemaPlus.getTable(tableName) != null) {
            // Table exists.
            if (!create.ifNotExists) {
                // They did not specify IF NOT EXISTS, so give error.
                throw SqlUtil.newContextException(create.name.getParserPosition(),
                    RESOURCE.tableExists(tableName));
            }
            return;
        }
        if (keyFields.isEmpty()) {
            // Use first column as primary key
            keyFields.add(rowType.getFieldList().get(0).getName());
        }
        // Table does not exist. Create it.
        Table table = schemaPlus.createTable(tableName, null, new RelDef(rowType, keyFields, strategies));
        schema.add(tableName, table);
        if (create.query != null) {
            populate(create.name, create.query, context);
        }
    }

    public void execute(SqlAlterTableExtension alter, CalcitePrepare.Context context) {
        final CalciteSchema schema =
            Schemas.subSchema(context.getRootSchema(),
                context.getDefaultSchemaPath());
        final JavaTypeFactory typeFactory = context.getTypeFactory();
        final List<SqlNode> columnList = alter.columnList.getList();
        final ImmutableList.Builder<ColumnDef> b = ImmutableList.builder();
        final RelDataTypeFactory.Builder builder = typeFactory.builder();
        final RelDataTypeFactory.Builder storedBuilder = typeFactory.builder();
        final SqlValidator validator = new ContextSqlValidator(context, false);
        final List<String> keyFields = new ArrayList<>();
        final List<io.kareldb.schema.ColumnStrategy> strategies = new ArrayList<>();
        for (Ord<SqlNode> c : Ord.zip(columnList)) {
            if (c.e instanceof SqlColumnDeclaration) {
                final SqlColumnDeclaration d = (SqlColumnDeclaration) c.e;
                final RelDataType type = d.dataType.deriveType(validator, true);
                builder.add(d.name.getSimple(), type);
                if (d.strategy != ColumnStrategy.VIRTUAL) {
                    storedBuilder.add(d.name.getSimple(), type);
                }
                b.add(ColumnDef.of(d.expression, type, d.strategy));
                addStrategy(strategies, d.strategy, d.expression);
            } else if (c.e instanceof SqlIdentifier) {
                final SqlIdentifier id = (SqlIdentifier) c.e;
                final RelDataType nullType = typeFactory.createSqlType(SqlTypeName.NULL);
                builder.add(id.getSimple(), nullType);
                addStrategy(strategies, ColumnStrategy.NULLABLE, null);
            } else {
                throw new AssertionError(c.e.getClass());
            }
        }
        final RelDataType rowType = builder.build();
        final RelDataType storedRowType = storedBuilder.build();
        final List<ColumnDef> columns = b.build();
        final InitializerExpressionFactory ief =
            new NullInitializerExpressionFactory() {
                @Override
                public ColumnStrategy generationStrategy(RelOptTable table,
                                                         int iColumn) {
                    return columns.get(iColumn).strategy;
                }

                @Override
                public RexNode newColumnDefaultValue(RelOptTable table,
                                                     int iColumn, InitializerContext context) {
                    final ColumnDef c = columns.get(iColumn);
                    if (c.expr != null) {
                        return context.convertExpression(c.expr);
                    }
                    return super.newColumnDefaultValue(table, iColumn, context);
                }
            };
        Schema schemaPlus = schema.plus().unwrap(Schema.class);
        String tableName = alter.name.getSimple();
        if (schemaPlus.getTable(tableName) == null) {
            // Table does not exists.
            if (!alter.ifExists) {
                // They did not specify IF EXISTS, so give error.
                throw SqlUtil.newContextException(alter.name.getParserPosition(),
                    RESOURCE.tableNameNotFound(tableName));
            }
            return;
        }
        // Table does not exist. Create it.
        schemaPlus.alterTable(tableName, alter.actions, new RelDef(rowType, keyFields,
            strategies));
    }

    public void execute(SqlDropTableExtension drop, CalcitePrepare.Context context) {
        final CalciteSchema schema =
            Schemas.subSchema(context.getRootSchema(),
                context.getDefaultSchemaPath());
        String tableName = drop.name.getSimple();
        switch (drop.getKind()) {
            case DROP_TABLE:
                Schema schemaPlus = schema.plus().unwrap(Schema.class);
                boolean existed = schemaPlus.dropTable(tableName);
                schema.removeTable(tableName);
                if (!existed && !drop.ifExists) {
                    throw SqlUtil.newContextException(drop.name.getParserPosition(),
                        RESOURCE.tableNotFound(tableName));
                }
                break;
            default:
                throw new AssertionError(drop.getKind());
        }
    }

    private void addStrategy(List<io.kareldb.schema.ColumnStrategy> strategies, ColumnStrategy strategy, SqlNode expression) {
        switch (strategy) {
            case NULLABLE:
                strategies.add(io.kareldb.schema.ColumnStrategy.NULL_STRATEGY);
                break;
            case NOT_NULLABLE:
                strategies.add(io.kareldb.schema.ColumnStrategy.NOT_NULL_STRATEGY);
                break;
            case DEFAULT:
                if (expression instanceof SqlLiteral) {
                    strategies.add(new DefaultStrategy(((SqlLiteral) expression).getValue()));
                } else {
                    strategies.add(io.kareldb.schema.ColumnStrategy.NOT_NULL_STRATEGY);
                }
                break;
            default:
                strategies.add(io.kareldb.schema.ColumnStrategy.NOT_NULL_STRATEGY);
                break;
        }
    }

    /**
     * Column definition.
     */
    private static class ColumnDef {
        final SqlNode expr;
        final RelDataType type;
        final ColumnStrategy strategy;

        private ColumnDef(SqlNode expr, RelDataType type,
                          ColumnStrategy strategy) {
            this.expr = expr;
            this.type = type;
            this.strategy = Objects.requireNonNull(strategy);
            Preconditions.checkArgument(
                strategy == ColumnStrategy.NULLABLE
                    || strategy == ColumnStrategy.NOT_NULLABLE
                    || expr != null);
        }

        static ColumnDef of(SqlNode expr, RelDataType type,
                            ColumnStrategy strategy) {
            return new ColumnDef(expr, type, strategy);
        }
    }
   
    /**
     * Populates the table called {@code name} by executing {@code query}.
     */
    protected static void populate(SqlIdentifier name, SqlNode query,
                                   CalcitePrepare.Context context) {
        // Generate, prepare and execute an "INSERT INTO table query" statement.
        // (It's a bit inefficient that we convert from SqlNode to SQL and back
        // again.)
        final FrameworkConfig config = Frameworks.newConfigBuilder()
            .defaultSchema(
                Objects.requireNonNull(
                    Schemas.subSchema(context.getRootSchema(),
                        context.getDefaultSchemaPath())).plus())
            .build();
        final Planner planner = Frameworks.getPlanner(config);
        try {
            final StringBuilder buf = new StringBuilder();
            final SqlPrettyWriter w =
                new SqlPrettyWriter(
                    SqlPrettyWriter.config()
                        .withDialect(CalciteSqlDialect.DEFAULT)
                        .withAlwaysUseParentheses(false),
                    buf);
            buf.append("INSERT INTO ");
            name.unparse(w, 0, 0);
            buf.append(" ");
            query.unparse(w, 0, 0);
            final String sql = buf.toString();
            final SqlNode query1 = planner.parse(sql);
            final SqlNode query2 = planner.validate(query1);
            final RelRoot r = planner.rel(query2);
            final PreparedStatement prepare = context.getRelRunner().prepare(r.rel);
            int rowCount = prepare.executeUpdate();
            Util.discard(rowCount);
            prepare.close();
        } catch (SqlParseException | ValidationException
            | RelConversionException | SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
