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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.connector.flight.FlightExprConverter;
import com.starrocks.connector.flight.FlightPredicateConverter;
import com.starrocks.connector.flight.FlightTable;
import com.starrocks.sql.analyzer.DecimalV3FunctionAnalyzer;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.materialization.common.AggregatePushDownUtils;
import pro.surpin.data.arrowflight.starrocks.ScanAggregate;
import pro.surpin.data.arrowflight.starrocks.ScanExpr;
import pro.surpin.data.arrowflight.starrocks.ScanPredicate;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Pushes plain-column aggregates into a Flight scan. A table has several Flight endpoints, so the server can only
 * return one partial aggregate per endpoint; the original aggregate stays above the scan and merges the partials:
 * COUNT merges as SUM, SUM as SUM, MIN as MIN, MAX as MAX and AVG becomes SUM(sum) / SUM(count).
 *
 * <p>Only what the server computes exactly is pushed. SUM is pushed over FLOAT/DOUBLE inputs (summed as DOUBLE) and
 * over exact DECIMAL inputs, including arithmetic and CASE expressions {@link FlightExprConverter} accepts; a decimal
 * sum is cast to DECIMAL(38, scale) on the server so the partials add up exactly. AVG accepts any numeric column
 * because its result is a DOUBLE anyway. MIN/MAX are limited to numeric and decimal columns. DISTINCT, strings and
 * integral SUM stay in StarRocks.
 */
public class PushDownAggToFlightScanRule extends TransformationRule {
    public static final PushDownAggToFlightScanRule WITH_PROJECT = new PushDownAggToFlightScanRule(true);
    public static final PushDownAggToFlightScanRule WITHOUT_PROJECT = new PushDownAggToFlightScanRule(false);

    private static final Set<PrimitiveType> INTEGRAL = Set.of(PrimitiveType.TINYINT, PrimitiveType.SMALLINT,
            PrimitiveType.INT, PrimitiveType.BIGINT);
    private static final Set<PrimitiveType> FLOATING = Set.of(PrimitiveType.FLOAT, PrimitiveType.DOUBLE);

    private final boolean withProject;

    private PushDownAggToFlightScanRule(boolean withProject) {
        super(RuleType.TF_PUSH_DOWN_AGG_TO_FLIGHT_SCAN, withProject
                ? Pattern.create(OperatorType.LOGICAL_AGGR).addChildren(
                        Pattern.create(OperatorType.LOGICAL_PROJECT, OperatorType.LOGICAL_JDBC_SCAN))
                : Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.LOGICAL_JDBC_SCAN));
        this.withProject = withProject;
    }

    private static boolean numeric(Type type) {
        return INTEGRAL.contains(type.getPrimitiveType()) || FLOATING.contains(type.getPrimitiveType());
    }

    private static boolean decimal(Type type) {
        return type.isDecimalV3();
    }

    private LogicalJDBCScanOperator scanOf(OptExpression input) {
        OptExpression child = input.inputAt(0);
        return (LogicalJDBCScanOperator) (withProject ? child.inputAt(0) : child).getOp();
    }

    /** Resolves a column of the aggregate to a scan column; null when it is anything but a plain scan column. */
    private ColumnRefOperator scanColumn(OptExpression input, ColumnRefOperator ref) {
        LogicalJDBCScanOperator scan = scanOf(input);
        if (withProject) {
            ScalarOperator value = ((LogicalProjectOperator) input.inputAt(0).getOp()).getColumnRefMap().get(ref);
            if (!ref.equals(value)) {
                return null;
            }
        }
        return scan.getColRefToColumnMetaMap().containsKey(ref) ? ref : null;
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        return plan(input, null) != null;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        Plan plan = plan(input, context.getColumnRefFactory());
        return plan == null ? Lists.newArrayList() : Lists.newArrayList(plan.result);
    }

    private static final class Plan {
        private OptExpression result;
    }

    /** Builds the rewrite, or returns null when this aggregate cannot be pushed. With a null factory it only checks. */
    private Plan plan(OptExpression input, ColumnRefFactory factory) {
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.getOp();
        LogicalJDBCScanOperator scan = scanOf(input);
        if (!(scan.getTable() instanceof FlightTable)
                || !((FlightTable) scan.getTable()).getConfig().isAggregationPushdown()
                || scan.getFlightAggregation() != null || scan.getLimit() != -1 || scan.getProjection() != null
                || agg.getType() != AggType.GLOBAL || agg.isSplit()
                || agg.getLimit() != -1 || agg.getProjection() != null || agg.getAggregations().isEmpty()) {
            return null;
        }

        // The whole filter must render for the server: the aggregated scan no longer exposes its columns.
        Map<ColumnRefOperator, Column> scanColumns = scan.getColRefToColumnMetaMap();
        FlightPredicateConverter converter = new FlightPredicateConverter(ref -> {
            Column column = scanColumns.get(ref);
            return column == null ? null : column.getName();
        });
        List<ScanPredicate> terms = new ArrayList<>();
        for (ScalarOperator conjunct : Utils.extractConjuncts(scan.getPredicate())) {
            Optional<ScanPredicate> converted = converter.convert(conjunct);
            if (converted.isEmpty()) {
                return null;
            }
            terms.add(converted.get());
        }
        ScanPredicate where = terms.isEmpty() ? null : terms.size() == 1 ? terms.get(0) : new ScanPredicate.And(terms);

        List<ColumnRefOperator> keys = new ArrayList<>();
        for (ColumnRefOperator key : agg.getGroupingKeys()) {
            ColumnRefOperator column = scanColumn(input, key);
            if (column == null) {
                return null;
            }
            keys.add(column);
        }

        // Describe every pushed partial first so unsupported calls decline before anything is created.
        List<Spec> specs = new ArrayList<>();
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : agg.getAggregations().entrySet()) {
            Spec spec = spec(input, entry.getKey(), entry.getValue(), scanColumns);
            if (spec == null) {
                return null;
            }
            specs.add(spec);
        }
        // A predicate on the aggregate (HAVING, or a join condition against an aggregate) is evaluated after the
        // merge, where the aggregate outputs are exactly what it saw. An AVG output only exists above a projection.
        ScalarOperator having = agg.getPredicate();
        if (having != null && specs.stream().anyMatch(spec -> spec.avg)) {
            return null;
        }
        if (factory == null) {
            return new Plan();
        }

        Map<ColumnRefOperator, Column> newColumns = new LinkedHashMap<>();
        Map<Column, ColumnRefOperator> newRefs = new LinkedHashMap<>();
        for (ColumnRefOperator key : keys) {
            newColumns.put(key, scanColumns.get(key));
            newRefs.put(scanColumns.get(key), key);
        }
        List<ScanAggregate.Partial> partials = new ArrayList<>();
        Map<ColumnRefOperator, CallOperator> merged = new LinkedHashMap<>();
        Map<ColumnRefOperator, ScalarOperator> above = new LinkedHashMap<>();
        for (ColumnRefOperator key : agg.getGroupingKeys()) {
            above.put(key, key);
        }
        boolean needsProject = false;
        for (Spec spec : specs) {
            ColumnRefOperator[] partialRefs = new ColumnRefOperator[spec.parts.size()];
            for (int p = 0; p < spec.parts.size(); p++) {
                Part part = spec.parts.get(p);
                String alias = "__agg" + partials.size();
                partials.add(new ScanAggregate.Partial(part.func, part.input, alias));
                Column column = new Column(alias, part.type);
                column.setIsAllowNull(true);
                ColumnRefOperator ref = factory.create(alias, part.type, true);
                newColumns.put(ref, column);
                newRefs.put(column, ref);
                partialRefs[p] = ref;
            }
            if (spec.avg) {
                Type sumType = spec.parts.get(0).type;
                ColumnRefOperator sum = factory.create("avg_sum", sumType, true);
                ColumnRefOperator count = factory.create("avg_count", Type.BIGINT, true);
                merged.put(sum, mergeCall(FunctionSet.SUM, sumType, partialRefs[0], null));
                merged.put(count, mergeCall(FunctionSet.SUM, Type.BIGINT, partialRefs[1], null));
                if (decimal(sumType)) {
                    // StarRocks' own sum/count rewrite for decimal averages, so scale and rounding are its own
                    above.put(spec.output, AggregatePushDownUtils.createAvgBySumCount(spec.original, sum, count));
                } else {
                    Function divide = Expr.getBuiltinFunction(FunctionSet.DIVIDE,
                            new Type[] {Type.DOUBLE, Type.DOUBLE}, Function.CompareMode.IS_IDENTICAL);
                    above.put(spec.output, new CallOperator(FunctionSet.DIVIDE, Type.DOUBLE,
                            Lists.newArrayList(sum, new CastOperator(Type.DOUBLE, count)), divide));
                }
                needsProject = true;
            } else {
                Part part = spec.parts.get(0);
                String mergeFn = part.func == ScanAggregate.Func.COUNT || part.func == ScanAggregate.Func.COUNT_STAR
                        ? FunctionSet.SUM : spec.original.getFnName();
                // Only MIN/MAX keep their input type, so only they can reuse the original function; COUNT and SUM
                // merge with SUM over the partial's own type.
                boolean sameFunction = part.func == ScanAggregate.Func.MIN || part.func == ScanAggregate.Func.MAX;
                merged.put(spec.output, mergeCall(mergeFn, spec.output.getType(), partialRefs[0],
                        sameFunction ? spec.original.getFunction() : null));
                above.put(spec.output, spec.output);
            }
        }

        ScanAggregate aggregate = new ScanAggregate(keys.stream().map(k -> scanColumns.get(k).getName()).toList(),
                partials);
        LogicalJDBCScanOperator pushed = new LogicalJDBCScanOperator.Builder().withOperator(scan)
                .setColRefToColumnMetaMap(newColumns).setColumnMetaToColRefMap(newRefs)
                .setPredicate(null).setFlightAggregation(aggregate).setFlightPredicate(where).build();
        LogicalAggregationOperator mergeAgg = new LogicalAggregationOperator(AggType.GLOBAL, keys, merged);
        if (having != null) {
            mergeAgg = new LogicalAggregationOperator.Builder().withOperator(mergeAgg).setPredicate(having).build();
        }
        OptExpression mergeExpr = OptExpression.create(mergeAgg, OptExpression.create(pushed));
        Plan plan = new Plan();
        plan.result = needsProject ? OptExpression.create(new LogicalProjectOperator(above), mergeExpr) : mergeExpr;
        return plan;
    }

    private CallOperator mergeCall(String name, Type type, ColumnRefOperator partial, Function reuse) {
        Function fn = reuse != null ? reuse : Expr.getBuiltinFunction(name, new Type[] {type},
                Function.CompareMode.IS_IDENTICAL);
        if (fn == null) {
            fn = Expr.getBuiltinFunction(name, new Type[] {type}, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        }
        if (decimal(type) && fn instanceof AggregateFunction) {
            // The built-in decimal function is declared over a wildcard DECIMAL128; the exchange between the local and
            // merge phase takes its column type from the function, so it needs the concrete precision and scale.
            fn = DecimalV3FunctionAnalyzer.rectifyAggregationFunction((AggregateFunction) fn, type, type);
        }
        return new CallOperator(name, type, Lists.newArrayList(partial), fn);
    }

    private static final class Part {
        private final ScanAggregate.Func func;
        private final ScanExpr input;
        private final Type type;

        private Part(ScanAggregate.Func func, ScanExpr input, Type type) {
            this.func = func;
            this.input = input;
            this.type = type;
        }

        private static Part column(ScanAggregate.Func func, String column, Type type) {
            return new Part(func, new ScanExpr.Column(column), type);
        }
    }

    private static final class Spec {
        private final ColumnRefOperator output;
        private final CallOperator original;
        private final List<Part> parts;
        private final boolean avg;

        private Spec(ColumnRefOperator output, CallOperator original, List<Part> parts, boolean avg) {
            this.output = output;
            this.original = original;
            this.parts = parts;
            this.avg = avg;
        }
    }

    private Spec spec(OptExpression input, ColumnRefOperator output, CallOperator call,
                      Map<ColumnRefOperator, Column> scanColumns) {
        if (call.isDistinct()) {
            return null;
        }
        String fn = call.getFnName().toLowerCase();
        List<ScalarOperator> args = call.getChildren();
        if (fn.equals(FunctionSet.COUNT) && (args.isEmpty() || isConstant(input, args))) {
            return new Spec(output, call, List.of(new Part(ScanAggregate.Func.COUNT_STAR, null, Type.BIGINT)), false);
        }
        if (args.size() != 1 || !(args.get(0) instanceof ColumnRefOperator)) {
            return null;
        }
        ColumnRefOperator argument = (ColumnRefOperator) args.get(0);
        if (fn.equals(FunctionSet.SUM)) {
            return sumSpec(input, output, call, argument, scanColumns);
        }
        ColumnRefOperator column = scanColumn(input, argument);
        if (column == null) {
            return null;
        }
        Type type = scanColumns.get(column).getType();
        String name = scanColumns.get(column).getName();
        switch (fn) {
            case FunctionSet.COUNT:
                return new Spec(output, call, List.of(Part.column(ScanAggregate.Func.COUNT, name, Type.BIGINT)),
                        false);
            case FunctionSet.MIN:
            case FunctionSet.MAX:
                return numeric(type) || decimal(type)
                        ? new Spec(output, call, List.of(Part.column(
                                fn.equals(FunctionSet.MIN) ? ScanAggregate.Func.MIN : ScanAggregate.Func.MAX,
                                name, type)), false)
                        : null;
            case FunctionSet.AVG:
                if (decimal(type) && decimal(call.getType())) {
                    int scale = ((ScalarType) type).getScalarScale();
                    return new Spec(output, call, List.of(
                            new Part(ScanAggregate.Func.SUM, ScanExpr.Cast.toDecimal(new ScanExpr.Column(name), 38, scale),
                                    ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, scale)),
                            Part.column(ScanAggregate.Func.COUNT, name, Type.BIGINT)), true);
                }
                return numeric(type)
                        ? new Spec(output, call, List.of(
                                new Part(ScanAggregate.Func.SUM, ScanExpr.Cast.toDouble(new ScanExpr.Column(name)),
                                        Type.DOUBLE),
                                Part.column(ScanAggregate.Func.COUNT, name, Type.BIGINT)), true)
                        : null;
            default:
                return null;
        }
    }

    /**
     * SUM over a floating column (summed as DOUBLE), or over an exact decimal column or expression, which is cast to
     * DECIMAL(38, scale) on the server so that the per-endpoint partials add up with no rounding.
     */
    private Spec sumSpec(OptExpression input, ColumnRefOperator output, CallOperator call, ColumnRefOperator argument,
                         Map<ColumnRefOperator, Column> scanColumns) {
        Type type = argument.getType();
        ColumnRefOperator plain = scanColumn(input, argument);
        if (plain != null && FLOATING.contains(type.getPrimitiveType())) {
            return new Spec(output, call, List.of(new Part(ScanAggregate.Func.SUM,
                    ScanExpr.Cast.toDouble(new ScanExpr.Column(scanColumns.get(plain).getName())), Type.DOUBLE)),
                    false);
        }
        boolean exactDecimal = decimal(type) && decimal(call.getType())
                && ((ScalarType) type).getScalarScale() == ((ScalarType) call.getType()).getScalarScale()
                && ((ScalarType) call.getType()).getScalarPrecision() == 38;
        if (!exactDecimal && !type.getPrimitiveType().equals(PrimitiveType.DOUBLE)) {
            return null;
        }
        ScalarOperator value = argumentValue(input, argument);
        Optional<ScanExpr> expr = new FlightExprConverter(ref -> {
            Column column = scanColumns.get(ref);
            return column == null ? null : column.getName();
        }).convert(value);
        if (expr.isEmpty()) {
            return null;
        }
        if (exactDecimal) {
            int scale = ((ScalarType) type).getScalarScale();
            return new Spec(output, call, List.of(new Part(ScanAggregate.Func.SUM,
                    ScanExpr.Cast.toDecimal(expr.get(), 38, scale), call.getType())), false);
        }
        return new Spec(output, call, List.of(new Part(ScanAggregate.Func.SUM, ScanExpr.Cast.toDouble(expr.get()),
                Type.DOUBLE)), false);
    }

    /** The expression an aggregate argument stands for: the projected value below the aggregate, if there is one. */
    private ScalarOperator argumentValue(OptExpression input, ColumnRefOperator argument) {
        if (!withProject) {
            return argument;
        }
        ScalarOperator value = ((LogicalProjectOperator) input.inputAt(0).getOp()).getColumnRefMap().get(argument);
        return value == null ? argument : value;
    }

    /** count(1) arrives as a count over a constant, possibly through the projection below the aggregate. */
    private boolean isConstant(OptExpression input, List<ScalarOperator> args) {
        if (args.size() != 1) {
            return false;
        }
        ScalarOperator arg = args.get(0);
        if (arg instanceof ConstantOperator) {
            return true;
        }
        if (withProject && arg instanceof ColumnRefOperator) {
            ScalarOperator value = ((LogicalProjectOperator) input.inputAt(0).getOp()).getColumnRefMap().get(arg);
            return value instanceof ConstantOperator;
        }
        return false;
    }
}
