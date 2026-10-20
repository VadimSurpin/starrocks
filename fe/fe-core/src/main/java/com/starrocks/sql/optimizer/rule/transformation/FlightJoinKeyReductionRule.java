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
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.connector.flight.FlightPredicateConverter;
import com.starrocks.connector.flight.FlightTable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pro.surpin.data.arrowflight.starrocks.ScanPredicate;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Semi-join reduction for two Flight scans: when one side of an equi-join has a selective filter that the Flight
 * server can evaluate completely, the distinct join keys of that side are fetched while planning and become
 * {@code key IN (...)} on the other side's scan, which is then filtered at the source instead of after it has been
 * shipped to StarRocks. The join itself stays, so the result cannot change: a row the IN list drops has no partner.
 *
 * <p>Applies to inner joins (both directions) and to semi joins (the preserved side only); outer and anti joins keep
 * rows without a partner and are left alone. Only plain integer key columns are used, only when the filtered side's
 * whole predicate can be pushed to the server (otherwise the keys would not match what the join sees), and only when
 * there are at most {@code flight.join_key_reduction.max_keys} keys. Any failure to fetch the keys leaves the plan
 * as it is.
 */
public class FlightJoinKeyReductionRule extends TransformationRule {
    private static final Logger LOG = LogManager.getLogger(FlightJoinKeyReductionRule.class);

    public static final FlightJoinKeyReductionRule INSTANCE = new FlightJoinKeyReductionRule();

    private FlightJoinKeyReductionRule() {
        super(RuleType.TF_FLIGHT_JOIN_KEY_REDUCTION, Pattern.create(OperatorType.LOGICAL_JOIN)
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF))
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    /** One equi-join condition between a column of the left scan and a column of the right scan. */
    private static final class KeyPair {
        private final ColumnRefOperator left;
        private final ColumnRefOperator right;

        private KeyPair(ColumnRefOperator left, ColumnRefOperator right) {
            this.left = left;
            this.right = right;
        }
    }

    /** Restricts the {@code target} scan (index 0 or 1 of the join) by the keys of the other one. */
    private static final class Direction {
        private final int target;
        private final ScanPredicate sourceFilter;

        private Direction(int target, ScanPredicate sourceFilter) {
            this.target = target;
            this.sourceFilter = sourceFilter;
        }
    }

    private static boolean isIntegral(PrimitiveType type) {
        return type == PrimitiveType.TINYINT || type == PrimitiveType.SMALLINT || type == PrimitiveType.INT
                || type == PrimitiveType.BIGINT;
    }

    private static LogicalJDBCScanOperator flightScan(OptExpression expression) {
        if (!(expression.getOp() instanceof LogicalJDBCScanOperator)) {
            return null;
        }
        LogicalJDBCScanOperator scan = (LogicalJDBCScanOperator) expression.getOp();
        if (!(scan.getTable() instanceof FlightTable)
                || !((FlightTable) scan.getTable()).getConfig().isJoinKeyReduction()
                || scan.getFlightAggregation() != null || scan.getLimit() != -1) {
            return null;
        }
        return scan;
    }

    private static List<KeyPair> equiKeys(LogicalJoinOperator join, LogicalJDBCScanOperator left,
                                          LogicalJDBCScanOperator right) {
        List<KeyPair> pairs = new ArrayList<>();
        if (join.getOnPredicate() == null) {
            return pairs;
        }
        for (ScalarOperator conjunct : Utils.extractConjuncts(join.getOnPredicate())) {
            if (!(conjunct instanceof BinaryPredicateOperator)
                    || ((BinaryPredicateOperator) conjunct).getBinaryType() != BinaryType.EQ
                    || !(conjunct.getChild(0) instanceof ColumnRefOperator)
                    || !(conjunct.getChild(1) instanceof ColumnRefOperator)) {
                continue;
            }
            ColumnRefOperator a = (ColumnRefOperator) conjunct.getChild(0);
            ColumnRefOperator b = (ColumnRefOperator) conjunct.getChild(1);
            boolean aLeft = left.getColRefToColumnMetaMap().containsKey(a);
            boolean bLeft = left.getColRefToColumnMetaMap().containsKey(b);
            boolean aRight = right.getColRefToColumnMetaMap().containsKey(a);
            boolean bRight = right.getColRefToColumnMetaMap().containsKey(b);
            if (!(aLeft && bRight) && !(aRight && bLeft)) {
                continue;
            }
            ColumnRefOperator leftRef = aLeft ? a : b;
            ColumnRefOperator rightRef = aLeft ? b : a;
            if (isIntegral(leftRef.getType().getPrimitiveType()) && isIntegral(rightRef.getType().getPrimitiveType())) {
                pairs.add(new KeyPair(leftRef, rightRef));
            }
        }
        return pairs;
    }

    /** The scan's whole predicate as a server-side filter, or null when it has none or cannot be pushed entirely. */
    private static ScanPredicate pushableFilter(LogicalJDBCScanOperator scan) {
        if (scan.getPredicate() == null) {
            return null;
        }
        FlightPredicateConverter converter = new FlightPredicateConverter(ref -> {
            Column column = scan.getColRefToColumnMetaMap().get(ref);
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
        return terms.isEmpty() ? null : terms.size() == 1 ? terms.get(0) : new ScanPredicate.And(terms);
    }

    private static List<Direction> directions(OptExpression input) {
        LogicalJoinOperator join = (LogicalJoinOperator) input.getOp();
        JoinOperator type = join.getJoinType();
        boolean inner = type == JoinOperator.INNER_JOIN;
        if (!inner && type != JoinOperator.LEFT_SEMI_JOIN && type != JoinOperator.RIGHT_SEMI_JOIN) {
            return List.of();
        }
        LogicalJDBCScanOperator left = flightScan(input.inputAt(0));
        LogicalJDBCScanOperator right = flightScan(input.inputAt(1));
        if (left == null || right == null || equiKeys(join, left, right).isEmpty()) {
            return List.of();
        }
        List<Direction> result = new ArrayList<>();
        // keys come from the right side and restrict the left one
        if (inner || type == JoinOperator.LEFT_SEMI_JOIN) {
            ScanPredicate filter = pushableFilter(right);
            if (filter != null) {
                result.add(new Direction(0, filter));
            }
        }
        // keys come from the left side and restrict the right one
        if (inner || type == JoinOperator.RIGHT_SEMI_JOIN) {
            ScanPredicate filter = pushableFilter(left);
            if (filter != null) {
                result.add(new Direction(1, filter));
            }
        }
        return result;
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        return !directions(input).isEmpty();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalJoinOperator join = (LogicalJoinOperator) input.getOp();
        LogicalJDBCScanOperator[] scans = {flightScan(input.inputAt(0)), flightScan(input.inputAt(1))};
        List<KeyPair> pairs = equiKeys(join, scans[0], scans[1]);
        ScalarOperator[] reduced = {scans[0].getPredicate(), scans[1].getPredicate()};
        boolean changed = false;
        for (Direction direction : directions(input)) {
            int source = 1 - direction.target;
            FlightTable sourceTable = (FlightTable) scans[source].getTable();
            for (KeyPair pair : pairs) {
                ColumnRefOperator sourceRef = source == 0 ? pair.left : pair.right;
                ColumnRefOperator targetRef = source == 0 ? pair.right : pair.left;
                Optional<List<Long>> keys = fetchKeys(sourceTable, scans[source], sourceRef, direction.sourceFilter);
                if (keys.isEmpty() || keys.get().isEmpty()) {
                    continue;
                }
                Optional<InPredicateOperator> in = inList(targetRef, keys.get());
                if (in.isEmpty()) {
                    continue;
                }
                reduced[direction.target] = reduced[direction.target] == null
                        ? in.get() : Utils.compoundAnd(reduced[direction.target], in.get());
                changed = true;
            }
        }
        if (!changed) {
            return Lists.newArrayList();
        }
        List<OptExpression> children = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            children.add(reduced[i] == scans[i].getPredicate() ? input.inputAt(i)
                    : OptExpression.create(new LogicalJDBCScanOperator.Builder().withOperator(scans[i])
                            .setPredicate(reduced[i]).build()));
        }
        return Lists.newArrayList(OptExpression.create(join, children));
    }

    private static Optional<List<Long>> fetchKeys(FlightTable table, LogicalJDBCScanOperator scan,
                                                  ColumnRefOperator keyRef, ScanPredicate filter) {
        String column = scan.getColRefToColumnMetaMap().get(keyRef).getName();
        try {
            return table.getService().distinctLongKeys(table.getCatalogDBName(), table.getName(), column, filter,
                    table.getConfig().getJoinKeyReductionMaxKeys());
        } catch (RuntimeException e) {
            LOG.warn("Flight join key reduction skipped, cannot fetch the keys of {}.{}: {}",
                    table.getCatalogDBName(), table.getName(), e.getMessage());
            return Optional.empty();
        }
    }

    /** {@code key IN (values)} typed like the key column, or empty when a value does not fit its type. */
    private static Optional<InPredicateOperator> inList(ColumnRefOperator key, List<Long> values) {
        PrimitiveType type = key.getType().getPrimitiveType();
        List<ScalarOperator> arguments = new ArrayList<>();
        arguments.add(key);
        for (long value : values) {
            switch (type) {
                case TINYINT:
                    if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
                        return Optional.empty();
                    }
                    arguments.add(ConstantOperator.createTinyInt((byte) value));
                    break;
                case SMALLINT:
                    if (value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
                        return Optional.empty();
                    }
                    arguments.add(ConstantOperator.createSmallInt((short) value));
                    break;
                case INT:
                    if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
                        return Optional.empty();
                    }
                    arguments.add(ConstantOperator.createInt((int) value));
                    break;
                default:
                    arguments.add(ConstantOperator.createBigint(value));
                    break;
            }
        }
        return Optional.of(new InPredicateOperator(false, arguments));
    }
}
