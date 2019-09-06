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

import io.kareldb.jdbc.rules.EnumerableTableModifyExtensionRule;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;

public class CalcitePrepareImpl extends org.apache.calcite.prepare.CalcitePrepareImpl {

    public CalcitePrepareImpl() {
        super();
    }

    @Override
    protected RelOptPlanner createPlanner(
        final CalcitePrepare.Context prepareContext,
        org.apache.calcite.plan.Context externalContext,
        RelOptCostFactory costFactory) {
        RelOptPlanner planner = super.createPlanner(prepareContext, externalContext, costFactory);
        planner.removeRule(EnumerableRules.ENUMERABLE_TABLE_MODIFICATION_RULE);
        planner.addRule(EnumerableTableModifyExtensionRule.INSTANCE);
        return planner;
    }
}
