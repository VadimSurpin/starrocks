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
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
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

public class PushDownAggToFlightScanRuleTest {
    private final ColumnRefFactory factory = new ColumnRefFactory();
    private final Column kCol = new Column("k", Type.INT);
    private final Column iCol = new Column("i", Type.INT);
    private final Column dCol = new Column("d", Type.DOUBLE);
    private final Column fCol = new Column("f", Type.FLOAT);
    private final Column sCol = new Column("s", Type.VARCHAR);
    private final Column idCol = new Column("id", Type.BIGINT);
    private final ColumnRefOperator k = factory.create("k", Type.INT, true);
    private final ColumnRefOperator i = factory.create("i", Type.INT, true);
    private final ColumnRefOperator d = factory.create("d", Type.DOUBLE, true);
    private final ColumnRefOperator f = factory.create("f", Type.FLOAT, true);
    private final ColumnRefOperator s = factory.create("s", Type.VARCHAR, true);
    private final ColumnRefOperator id = factory.create("id", Type.BIGINT, true);

    private LogicalJDBCScanOperator scan(Map<String, String> extra, ScalarOperator predicate, long limit) {
        Map<String, String> props = new HashMap<>(extra);
        props.put("flight.uri", "grpc://localhost:8815");
        FlightTable table = new FlightTable(1, "orders", List.of(kCol, iCol, dCol, fCol, sCol, idCol), "sales", "flight",
                FlightConfig.from(props), null);
        Map<ColumnRefOperator, Column> refs = new LinkedHashMap<>();
        refs.put(k, kCol);
        refs.put(i, iCol);
        refs.put(d, dCol);
        refs.put(f, fCol);
        refs.put(s, sCol);
        refs.put(id, idCol);
        Map<Column, ColumnRefOperator> cols = new LinkedHashMap<>();
        refs.forEach((r, c) -> cols.put(c, r));
        return new LogicalJDBCScanOperator(table, refs, cols, limit, predicate, null);
    }

    private CallOperator call(String name, Type returnType, boolean distinct, ScalarOperator... args) {
        Type[] types = java.util.Arrays.stream(args).map(ScalarOperator::getType).toArray(Type[]::new);
        Function fn = Expr.getBuiltinFunction(name, types, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        return new CallOperator(name, returnType, Lists.newArrayList(args), fn, distinct);
    }

    private OptExpression aggOver(LogicalJDBCScanOperator scan, List<ColumnRefOperator> keys,
                                  Map<ColumnRefOperator, CallOperator> aggs) {
        return OptExpression.create(new LogicalAggregationOperator(AggType.GLOBAL, keys, aggs),
                OptExpression.create(scan));
    }

    private List<OptExpression> run(OptExpression input) {
        PushDownAggToFlightScanRule rule = PushDownAggToFlightScanRule.WITHOUT_PROJECT;
        if (!rule.check(input, OptimizerFactory.mockContext(factory))) {
            return List.of();
        }
        return rule.transform(input, OptimizerFactory.mockContext(factory));
    }

    @Test
    public void groupedCountSumMinMaxBecomePartialsMergedAbove() {
        Map<ColumnRefOperator, CallOperator> aggs = new LinkedHashMap<>();
        ColumnRefOperator cnt = factory.create("cnt", Type.BIGINT, true);
        ColumnRefOperator sum = factory.create("sm", Type.DOUBLE, true);
        ColumnRefOperator min = factory.create("mn", Type.INT, true);
        ColumnRefOperator max = factory.create("mx", Type.INT, true);
        aggs.put(cnt, call(FunctionSet.COUNT, Type.BIGINT, false));
        aggs.put(sum, call(FunctionSet.SUM, Type.DOUBLE, false, d));
        aggs.put(min, call(FunctionSet.MIN, Type.INT, false, i));
        aggs.put(max, call(FunctionSet.MAX, Type.INT, false, i));

        List<OptExpression> result = run(aggOver(scan(Map.of(), null, -1), List.of(k), aggs));

        Assertions.assertEquals(1, result.size());
        OptExpression top = result.get(0);
        Assertions.assertEquals(OperatorType.LOGICAL_AGGR, top.getOp().getOpType());
        LogicalAggregationOperator merge = (LogicalAggregationOperator) top.getOp();
        Assertions.assertEquals(List.of(k), merge.getGroupingKeys());
        Assertions.assertEquals(FunctionSet.SUM, merge.getAggregations().get(cnt).getFnName());
        Assertions.assertEquals(Type.BIGINT, merge.getAggregations().get(cnt).getType());
        Assertions.assertEquals(FunctionSet.SUM, merge.getAggregations().get(sum).getFnName());
        Assertions.assertEquals(FunctionSet.MIN, merge.getAggregations().get(min).getFnName());
        Assertions.assertEquals(FunctionSet.MAX, merge.getAggregations().get(max).getFnName());

        // The merge must run the right function over the partial's own type, not the original call's function.
        for (CallOperator merge1 : merge.getAggregations().values()) {
            ColumnRefOperator partial = (ColumnRefOperator) merge1.getChild(0);
            Assertions.assertEquals(merge1.getFnName(), merge1.getFunction().functionName(), merge1.toString());
            Assertions.assertEquals(partial.getType(), merge1.getFunction().getArgs()[0], merge1.toString());
        }

        LogicalJDBCScanOperator pushed = (LogicalJDBCScanOperator) top.inputAt(0).getOp();
        Assertions.assertNull(pushed.getPredicate());
        Assertions.assertEquals("SELECT k, COUNT(*) AS __agg0, SUM(CAST(d AS DOUBLE)) AS __agg1, MIN(i) AS __agg2, "
                + "MAX(i) AS __agg3 "
                + "FROM sales.orders GROUP BY k",
                ScanSql.select("sales", "orders", pushed.getFlightAggregation(), null));
        // the scan exposes the key and one column per partial, and nothing else
        Assertions.assertEquals(5, pushed.getColRefToColumnMetaMap().size());
        Assertions.assertTrue(pushed.getColRefToColumnMetaMap().containsKey(k));
        Assertions.assertFalse(pushed.getColRefToColumnMetaMap().containsKey(s));
        Assertions.assertEquals(Type.DOUBLE, pushed.getColRefToColumnMetaMap().values().stream()
                .filter(c -> c.getName().equals("__agg1")).findFirst().orElseThrow().getType());
    }

    @Test
    public void floatSumMergesAsADoubleSum() {
        ColumnRefOperator sum = factory.create("sf", Type.DOUBLE, true);
        List<OptExpression> result = run(aggOver(scan(Map.of(), null, -1), List.of(),
                Map.of(sum, call(FunctionSet.SUM, Type.DOUBLE, false, f))));
        CallOperator merged = ((LogicalAggregationOperator) result.get(0).getOp()).getAggregations().get(sum);
        Assertions.assertEquals("sum", merged.getFunction().functionName());
        Assertions.assertEquals(Type.DOUBLE, merged.getFunction().getArgs()[0]);
    }

    @Test
    public void avgSplitsIntoSumAndCountWithADivisionAbove() {
        ColumnRefOperator avg = factory.create("av", Type.DOUBLE, true);
        Map<ColumnRefOperator, CallOperator> aggs = new LinkedHashMap<>();
        aggs.put(avg, call(FunctionSet.AVG, Type.DOUBLE, false, i));

        List<OptExpression> result = run(aggOver(scan(Map.of(), null, -1), List.of(), aggs));

        Assertions.assertEquals(1, result.size());
        OptExpression top = result.get(0);
        Assertions.assertEquals(OperatorType.LOGICAL_PROJECT, top.getOp().getOpType());
        ScalarOperator division = ((LogicalProjectOperator) top.getOp()).getColumnRefMap().get(avg);
        Assertions.assertTrue(division instanceof CallOperator);
        Assertions.assertEquals(FunctionSet.DIVIDE, ((CallOperator) division).getFnName());
        LogicalAggregationOperator merge = (LogicalAggregationOperator) top.inputAt(0).getOp();
        Assertions.assertEquals(2, merge.getAggregations().size());
        for (CallOperator merge1 : merge.getAggregations().values()) {
            ColumnRefOperator partial = (ColumnRefOperator) merge1.getChild(0);
            Assertions.assertEquals("sum", merge1.getFunction().functionName(), merge1.toString());
            Assertions.assertEquals(partial.getType(), merge1.getFunction().getArgs()[0], merge1.toString());
        }
        LogicalJDBCScanOperator pushed = (LogicalJDBCScanOperator) top.inputAt(0).inputAt(0).getOp();
        Assertions.assertEquals("SELECT SUM(CAST(i AS DOUBLE)) AS __agg0, COUNT(i) AS __agg1 FROM sales.orders",
                ScanSql.select("sales", "orders", pushed.getFlightAggregation(), null));
    }

    @Test
    public void filterIsCarriedOnTheScanBecauseItsColumnsLeaveTheTuple() {
        ScalarOperator filter = BinaryPredicateOperator.gt(id, ConstantOperator.createBigint(5));
        Map<ColumnRefOperator, CallOperator> aggs = new LinkedHashMap<>();
        aggs.put(factory.create("cnt", Type.BIGINT, true), call(FunctionSet.COUNT, Type.BIGINT, false));

        List<OptExpression> result = run(aggOver(scan(Map.of(), filter, -1), List.of(k), aggs));

        LogicalJDBCScanOperator pushed = (LogicalJDBCScanOperator) result.get(0).inputAt(0).getOp();
        Assertions.assertNull(pushed.getPredicate());
        Assertions.assertEquals("(id > 5)", pushed.getFlightPredicate().toSql());
        Assertions.assertFalse(pushed.getColRefToColumnMetaMap().containsKey(id));
    }

    @Test
    public void declinesWhatTheServerCannotComputeExactly() {
        ColumnRefOperator out = factory.create("o", Type.BIGINT, true);
        // distinct
        Assertions.assertTrue(run(aggOver(scan(Map.of(), null, -1), List.of(k),
                Map.of(out, call(FunctionSet.COUNT, Type.BIGINT, true, i)))).isEmpty());
        // integral SUM is DOUBLE on the server: inexact above 2^53
        Assertions.assertTrue(run(aggOver(scan(Map.of(), null, -1), List.of(k),
                Map.of(out, call(FunctionSet.SUM, Type.BIGINT, false, i)))).isEmpty());
        // string MIN: collation may differ
        Assertions.assertTrue(run(aggOver(scan(Map.of(), null, -1), List.of(k),
                Map.of(out, call(FunctionSet.MIN, Type.VARCHAR, false, s)))).isEmpty());
        // a filter the converter cannot render must stay with StarRocks
        ScalarOperator abs = BinaryPredicateOperator.eq(call("abs", Type.BIGINT, false, id),
                ConstantOperator.createBigint(2));
        Assertions.assertTrue(run(aggOver(scan(Map.of(), abs, -1), List.of(k),
                Map.of(out, call(FunctionSet.COUNT, Type.BIGINT, false)))).isEmpty());
        // limit on the scan
        Assertions.assertTrue(run(aggOver(scan(Map.of(), null, 10), List.of(k),
                Map.of(out, call(FunctionSet.COUNT, Type.BIGINT, false)))).isEmpty());
    }

    @Test
    public void declinesWhenSwitchedOffAndNeverReappliesToAnAggregatedScan() {
        ColumnRefOperator out = factory.create("o", Type.BIGINT, true);
        Map<ColumnRefOperator, CallOperator> aggs = Map.of(out, call(FunctionSet.COUNT, Type.BIGINT, false));
        Assertions.assertTrue(run(aggOver(scan(Map.of("flight.aggregation_pushdown", "false"), null, -1),
                List.of(k), aggs)).isEmpty());

        List<OptExpression> once = run(aggOver(scan(Map.of(), null, -1), List.of(k), aggs));
        Assertions.assertEquals(1, once.size());
        LogicalAggregationOperator merge = (LogicalAggregationOperator) once.get(0).getOp();
        Assertions.assertTrue(run(OptExpression.create(merge, once.get(0).inputAt(0))).isEmpty());
    }
}
