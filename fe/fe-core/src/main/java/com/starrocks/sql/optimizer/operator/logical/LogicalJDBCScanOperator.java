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

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import pro.surpin.data.arrowflight.starrocks.ScanAggregate;

import java.util.Map;
import java.util.Objects;

public class LogicalJDBCScanOperator extends LogicalScanOperator {
    // Set only for Flight tables: partial aggregation evaluated by the Flight server (see PushDownAggToFlightScanRule)
    private ScanAggregate flightAggregation;

    public LogicalJDBCScanOperator(Table table,
                                   Map<ColumnRefOperator, Column> columnRefOperatorColumnMap,
                                   Map<Column, ColumnRefOperator> columnMetaToColRefMap,
                                   long limit,
                                   ScalarOperator predicate,
                                   Projection projection) {
        super(OperatorType.LOGICAL_JDBC_SCAN,
                table,
                columnRefOperatorColumnMap,
                columnMetaToColRefMap,
                limit, predicate, projection);
        Preconditions.checkState(table instanceof JDBCTable);
    }

    private LogicalJDBCScanOperator() {
        super(OperatorType.LOGICAL_JDBC_SCAN);
    }

    public ScanAggregate getFlightAggregation() {
        return flightAggregation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return super.equals(o) && Objects.equals(flightAggregation, ((LogicalJDBCScanOperator) o).flightAggregation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), flightAggregation);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalJDBCScan(this, context);
    }

    public static class Builder
            extends LogicalScanOperator.Builder<LogicalJDBCScanOperator, LogicalJDBCScanOperator.Builder> {
        @Override
        protected LogicalJDBCScanOperator newInstance() {
            return new LogicalJDBCScanOperator();
        }

        @Override
        public Builder withOperator(LogicalJDBCScanOperator scanOperator) {
            super.withOperator(scanOperator);
            builder.flightAggregation = scanOperator.flightAggregation;
            return this;
        }

        public Builder setFlightAggregation(ScanAggregate flightAggregation) {
            builder.flightAggregation = flightAggregation;
            return this;
        }
    }
}
