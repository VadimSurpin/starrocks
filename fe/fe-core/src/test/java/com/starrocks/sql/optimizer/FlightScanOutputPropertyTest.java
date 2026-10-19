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

package com.starrocks.sql.optimizer;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.connector.flight.FlightConfig;
import com.starrocks.connector.flight.FlightTable;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A Flight scan runs one fragment instance per endpoint. If it claimed a gathered distribution the optimizer would
 * plan one-phase aggregates and skip exchanges, and every instance would finalize only its own slice.
 */
public class FlightScanOutputPropertyTest {
    @Test
    public void flightScanDoesNotClaimAGatheredDistribution() {
        ColumnRefFactory factory = new ColumnRefFactory();
        Column column = new Column("k", Type.INT);
        ColumnRefOperator ref = factory.create("k", Type.INT, true);
        FlightTable table = new FlightTable(1, "orders", List.of(column), "sales", "flight",
                FlightConfig.from(Map.of("flight.uri", "grpc://localhost:8815")), null);
        Map<ColumnRefOperator, Column> refs = new LinkedHashMap<>();
        refs.put(ref, column);
        Map<Column, ColumnRefOperator> columns = new LinkedHashMap<>();
        columns.put(column, ref);
        PhysicalJDBCScanOperator scan =
                new PhysicalJDBCScanOperator(new LogicalJDBCScanOperator(table, refs, columns, -1, null, null));

        GroupExpression expression = new GroupExpression(scan, List.of());
        expression.setGroup(new Group(0));

        PhysicalPropertySet output =
                new OutputPropertyDeriver(expression, PhysicalPropertySet.EMPTY, List.of()).getOutputProperty();

        Assertions.assertFalse(output.getDistributionProperty().isGather(),
                "a multi-endpoint Flight scan must not claim to be gathered");
    }
}
