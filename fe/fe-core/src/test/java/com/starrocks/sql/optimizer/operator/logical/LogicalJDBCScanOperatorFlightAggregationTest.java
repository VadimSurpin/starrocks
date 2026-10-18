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

package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.connector.flight.FlightConfig;
import com.starrocks.connector.flight.FlightTable;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJDBCScanOperator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.starrocks.ScanAggregate;

import java.util.List;
import java.util.Map;

public class LogicalJDBCScanOperatorFlightAggregationTest {
    private static LogicalJDBCScanOperator scan() {
        FlightConfig config = FlightConfig.from(Map.of("flight.uri", "grpc://localhost:8815"));
        FlightTable table = new FlightTable(1, "orders", List.of(new Column("id", Type.BIGINT)), "sales", "flight",
                config, null);
        return new LogicalJDBCScanOperator(table, Map.of(), Map.of(), -1, null, null);
    }

    private static ScanAggregate aggregate() {
        return new ScanAggregate(List.of(), List.of(
                new ScanAggregate.Partial(ScanAggregate.Func.COUNT_STAR, null, "__agg0")));
    }

    @Test
    public void aggregationSurvivesTheBuilderAndReachesThePhysicalOperator() {
        LogicalJDBCScanOperator plain = scan();
        Assertions.assertNull(plain.getFlightAggregation());

        LogicalJDBCScanOperator pushed = new LogicalJDBCScanOperator.Builder().withOperator(plain)
                .setFlightAggregation(aggregate()).build();
        Assertions.assertEquals(aggregate(), pushed.getFlightAggregation());

        LogicalJDBCScanOperator copy = new LogicalJDBCScanOperator.Builder().withOperator(pushed).build();
        Assertions.assertEquals(aggregate(), copy.getFlightAggregation());
        Assertions.assertEquals(aggregate(), new PhysicalJDBCScanOperator(copy).getFlightAggregation());
    }

    @Test
    public void aggregationParticipatesInEquality() {
        LogicalJDBCScanOperator plain = scan();
        LogicalJDBCScanOperator pushed = new LogicalJDBCScanOperator.Builder().withOperator(plain)
                .setFlightAggregation(aggregate()).build();
        Assertions.assertNotEquals(plain, pushed);
        Assertions.assertNotEquals(plain.hashCode(), pushed.hashCode());
    }
}
