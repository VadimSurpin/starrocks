// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector.flight;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateToExternalTableScanRule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlightPushdownRuleTest {
    private final Column idColumn = new Column("id", Type.BIGINT);
    private final ColumnRefOperator id = new ColumnRefOperator(1, Type.BIGINT, "id", true);

    private OptExpression filterOverScan(ScalarOperator filter) {
        FlightTable table = new FlightTable(1, "orders", List.of(idColumn), "sales", "flight",
                FlightConfig.from(Map.of("flight.uri", "grpc://localhost:8815")), null);
        Map<ColumnRefOperator, Column> refs = new HashMap<>();
        refs.put(id, idColumn);
        Map<Column, ColumnRefOperator> columns = new HashMap<>();
        columns.put(idColumn, id);
        LogicalJDBCScanOperator scan = new LogicalJDBCScanOperator(table, refs, columns, -1, null, null);
        return OptExpression.create(new LogicalFilterOperator(filter), OptExpression.create(scan));
    }

    @Test
    public void flightTableKeepsUnrenderablePredicatesInAFilterAboveTheScan() {
        ScalarOperator pushable = BinaryPredicateOperator.eq(id, ConstantOperator.createBigint(1));
        ScalarOperator keep = BinaryPredicateOperator.eq(
                new CallOperator("abs", Type.BIGINT, List.of(id)), ConstantOperator.createBigint(2));
        List<OptExpression> result = new PushDownPredicateToExternalTableScanRule().transform(
                filterOverScan(CompoundPredicateOperator.and(pushable, keep)), null);

        Assertions.assertEquals(1, result.size());
        OptExpression project = result.get(0);
        OptExpression filter = project.inputAt(0);
        Assertions.assertEquals(OperatorType.LOGICAL_FILTER, filter.getOp().getOpType());
        Assertions.assertEquals(keep, ((LogicalFilterOperator) filter.getOp()).getPredicate());
        LogicalJDBCScanOperator scan = (LogicalJDBCScanOperator) filter.inputAt(0).getOp();
        Assertions.assertEquals(pushable, scan.getPredicate());
        Assertions.assertTrue(scan.getTable() instanceof FlightTable);
        Assertions.assertTrue(scan.getTable() instanceof JDBCTable);
    }

    @Test
    public void flightTablePushesEverythingItCanRender() {
        ScalarOperator pushable = BinaryPredicateOperator.eq(id, ConstantOperator.createBigint(1));
        List<OptExpression> result = new PushDownPredicateToExternalTableScanRule().transform(
                filterOverScan(pushable), null);
        Assertions.assertEquals(1, result.size());
        LogicalJDBCScanOperator scan = (LogicalJDBCScanOperator) result.get(0).inputAt(0).getOp();
        Assertions.assertEquals(pushable, scan.getPredicate());
    }
}
