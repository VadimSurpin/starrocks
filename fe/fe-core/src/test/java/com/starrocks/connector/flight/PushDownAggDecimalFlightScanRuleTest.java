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

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.PushDownAggToFlightScanRule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.starrocks.ScanSql;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Decimal and expression aggregates pushed into a Flight scan. */
public class PushDownAggDecimalFlightScanRuleTest {
    private static final Type D15_2 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 15, 2);
    private static final Type D16_2 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 16, 2);
    private static final Type D31_4 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 31, 4);
    private static final Type D38_2 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 2);
    private static final Type D38_4 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 4);
    private static final Type D18_2 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 2);

    private final ColumnRefFactory factory = new ColumnRefFactory();
    private final Column kCol = new Column("k", Type.INT);
    private final Column priceCol = new Column("price", D15_2);
    private final Column discCol = new Column("disc", D15_2);
    private final ColumnRefOperator k = factory.create("k", Type.INT, true);
    private final ColumnRefOperator price = factory.create("price", D15_2, true);
    private final ColumnRefOperator disc = factory.create("disc", D15_2, true);

    private LogicalJDBCScanOperator scan() {
        Map<String, String> props = new HashMap<>();
        props.put("flight.uri", "grpc://localhost:8815");
        FlightTable table = new FlightTable(1, "orders", List.of(kCol, priceCol, discCol), "sales", "flight",
                FlightConfig.from(props), null);
        Map<ColumnRefOperator, Column> refs = new LinkedHashMap<>();
        refs.put(k, kCol);
        refs.put(price, priceCol);
        refs.put(disc, discCol);
        Map<Column, ColumnRefOperator> cols = new LinkedHashMap<>();
        refs.forEach((r, c) -> cols.put(c, r));
        return new LogicalJDBCScanOperator(table, refs, cols, -1, null, null);
    }

    private CallOperator call(String name, Type returnType, ScalarOperator... args) {
        Type[] types = java.util.Arrays.stream(args).map(ScalarOperator::getType).toArray(Type[]::new);
        Function fn = Expr.getBuiltinFunction(name, types, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        return new CallOperator(name, returnType, Lists.newArrayList(args), fn, false);
    }

    private OptExpression aggOver(Map<ColumnRefOperator, CallOperator> aggs, Map<ColumnRefOperator, ScalarOperator> project) {
        OptExpression scanExpr = OptExpression.create(scan());
        LogicalAggregationOperator agg = new LogicalAggregationOperator(AggType.GLOBAL, List.of(k), aggs);
        if (project == null) {
            return OptExpression.create(agg, scanExpr);
        }
        return OptExpression.create(agg, OptExpression.create(new LogicalProjectOperator(project), scanExpr));
    }

    private List<OptExpression> run(OptExpression input, boolean withProject) {
        PushDownAggToFlightScanRule rule = withProject
                ? PushDownAggToFlightScanRule.WITH_PROJECT : PushDownAggToFlightScanRule.WITHOUT_PROJECT;
        if (!rule.check(input, OptimizerFactory.mockContext(factory))) {
            return List.of();
        }
        return rule.transform(input, OptimizerFactory.mockContext(factory));
    }

    /** price * (1 - disc) the way StarRocks types it: operands widened to decimals, scales adding up. */
    private ScalarOperator discountedPrice(Type productType) {
        ScalarOperator oneMinus = call("subtract", D16_2, ConstantOperator.createInt(1),
                new CastOperator(D16_2, disc, true));
        return call("multiply", productType, new CastOperator(D15_2, price, true),
                new CastOperator(D16_2, oneMinus, true));
    }

    private Map<ColumnRefOperator, ScalarOperator> projectOf(ColumnRefOperator expr, ScalarOperator value) {
        Map<ColumnRefOperator, ScalarOperator> project = new LinkedHashMap<>();
        project.put(k, k);
        project.put(expr, value);
        return project;
    }

    @Test
    public void decimalSumOverAProjectedExpressionIsPushedExactly() {
        ColumnRefOperator revenue = factory.create("revenue", D31_4, true);
        ColumnRefOperator total = factory.create("total", D38_4, true);
        List<OptExpression> result = run(aggOver(Map.of(total, call(FunctionSet.SUM, D38_4, revenue)),
                projectOf(revenue, discountedPrice(D31_4))), true);

        Assertions.assertEquals(1, result.size());
        LogicalAggregationOperator merge = (LogicalAggregationOperator) result.get(0).getOp();
        CallOperator sum = merge.getAggregations().get(total);
        Assertions.assertEquals("sum", sum.getFunction().functionName());
        ColumnRefOperator partial = (ColumnRefOperator) sum.getChild(0);
        Assertions.assertEquals(D38_4, partial.getType());
        // The exchange between the local and merge phase takes its type from the function, so a wildcard DECIMAL128
        // (no precision and scale) would corrupt the column and crash the backend's result writer.
        Assertions.assertEquals(D38_4, sum.getFunction().getArgs()[0]);
        Assertions.assertEquals(D38_4, sum.getFunction().getReturnType());
        Assertions.assertEquals(D38_4, sum.getType());

        LogicalJDBCScanOperator pushed = (LogicalJDBCScanOperator) result.get(0).inputAt(0).getOp();
        Assertions.assertEquals("SELECT k, SUM(CAST((price * (1 - disc)) AS DECIMAL(38,4))) AS __agg0 "
                + "FROM sales.orders GROUP BY k",
                ScanSql.select("sales", "orders", pushed.getFlightAggregation(), null));
    }

    @Test
    public void decimalSumAndMinMaxOverPlainColumnsArePushed() {
        ColumnRefOperator total = factory.create("total", D38_2, true);
        ColumnRefOperator lo = factory.create("lo", D15_2, true);
        ColumnRefOperator hi = factory.create("hi", D15_2, true);
        Map<ColumnRefOperator, CallOperator> aggs = new LinkedHashMap<>();
        aggs.put(total, call(FunctionSet.SUM, D38_2, price));
        aggs.put(lo, call(FunctionSet.MIN, D15_2, price));
        aggs.put(hi, call(FunctionSet.MAX, D15_2, price));

        List<OptExpression> result = run(aggOver(aggs, null), false);

        Assertions.assertEquals(1, result.size());
        LogicalAggregationOperator merge = (LogicalAggregationOperator) result.get(0).getOp();
        Assertions.assertEquals("min", merge.getAggregations().get(lo).getFnName());
        Assertions.assertEquals(D15_2, merge.getAggregations().get(lo).getType());
        LogicalJDBCScanOperator pushed = (LogicalJDBCScanOperator) result.get(0).inputAt(0).getOp();
        Assertions.assertEquals("SELECT k, SUM(CAST(price AS DECIMAL(38,2))) AS __agg0, MIN(price) AS __agg1, "
                + "MAX(price) AS __agg2 FROM sales.orders GROUP BY k",
                ScanSql.select("sales", "orders", pushed.getFlightAggregation(), null));
    }

    @Test
    public void declinesDecimalSumsThatWouldRoundOrNarrow() {
        ColumnRefOperator revenue = factory.create("revenue", D31_4, true);
        ColumnRefOperator total = factory.create("total", D38_4, true);
        // the product's StarRocks scale (3) is not the exact one (4)
        Type roundedProduct = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 31, 3);
        Assertions.assertTrue(run(aggOver(Map.of(total, call(FunctionSet.SUM, D38_4, revenue)),
                projectOf(revenue, discountedPrice(roundedProduct))), true).isEmpty());

        // SUM result narrower than DECIMAL(38, s) can overflow in StarRocks differently
        ColumnRefOperator narrow = factory.create("narrow", D18_2, true);
        Assertions.assertTrue(run(aggOver(Map.of(narrow, call(FunctionSet.SUM, D18_2, price)), null), false)
                .isEmpty());

        // an expression the converter cannot render
        ColumnRefOperator odd = factory.create("odd", D15_2, true);
        Assertions.assertTrue(run(aggOver(Map.of(total, call(FunctionSet.SUM, D38_2, odd)),
                projectOf(odd, call("abs", D15_2, price))), true).isEmpty());
    }

    @Test
    public void decimalAvgSplitsIntoAnExactSumAndACountDividedAbove() {
        Type avgType = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 8);
        ColumnRefOperator avg = factory.create("av", avgType, true);
        List<OptExpression> result = run(aggOver(Map.of(avg, call(FunctionSet.AVG, avgType, price)), null), false);

        Assertions.assertEquals(1, result.size());
        OptExpression top = result.get(0);
        ScalarOperator division = ((LogicalProjectOperator) top.getOp()).getColumnRefMap().get(avg);
        Assertions.assertEquals(FunctionSet.DIVIDE, ((CallOperator) division).getFnName());
        Assertions.assertEquals(avgType, division.getType());
        LogicalAggregationOperator merge = (LogicalAggregationOperator) top.inputAt(0).getOp();
        Assertions.assertEquals(2, merge.getAggregations().size());
        LogicalJDBCScanOperator pushed = (LogicalJDBCScanOperator) top.inputAt(0).inputAt(0).getOp();
        Assertions.assertEquals("SELECT k, SUM(CAST(price AS DECIMAL(38,2))) AS __agg0, COUNT(price) AS __agg1 "
                + "FROM sales.orders GROUP BY k",
                ScanSql.select("sales", "orders", pushed.getFlightAggregation(), null));
    }
}
