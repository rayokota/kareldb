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
package io.kareldb.jdbc.rules;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

/**
 * Planner rule that converts a
 * {@link org.apache.calcite.rel.logical.LogicalTableModify}
 * relational expression
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}.
 */
public class EnumerableTableModifyExtensionRule extends ConverterRule {
    public static final ConverterRule INSTANCE = Config.INSTANCE
        .withConversion(
            LogicalTableModify.class,
            (Predicate<RelNode>) r -> true,
            Convention.NONE,
            EnumerableConvention.INSTANCE,
            "EnumerableTableModificationExtensionRule")
        .withRuleFactory(EnumerableTableModifyExtensionRule::new)
        .toRule(EnumerableTableModifyExtensionRule.class);

    private EnumerableTableModifyExtensionRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalTableModify modify =
            (LogicalTableModify) rel;
        final ModifiableTable modifiableTable =
            modify.getTable().unwrap(ModifiableTable.class);
        if (modifiableTable == null) {
            return null;
        }
        final RelTraitSet traitSet =
            modify.getTraitSet().replace(EnumerableConvention.INSTANCE);
        return new EnumerableTableModifyExtension(
            modify.getCluster(), traitSet,
            modify.getTable(),
            modify.getCatalogReader(),
            convert(modify.getInput(), traitSet),
            modify.getOperation(),
            modify.getUpdateColumnList(),
            modify.getSourceExpressionList(),
            modify.isFlattened());
    }
}
