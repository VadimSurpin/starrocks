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

import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import pro.surpin.data.arrowflight.starrocks.ScanExpr;
import pro.surpin.data.arrowflight.starrocks.ScanPredicate;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Converts a StarRocks predicate (implicit casts already stripped) into a {@link ScanPredicate} the Flight
 * server can evaluate. Anything that cannot be rendered with exactly StarRocks' semantics yields empty, and the
 * caller must then keep the predicate inside StarRocks.
 */
public final class FlightPredicateConverter {
    private final Function<ColumnRefOperator, String> columnName;

    /** @param columnName maps a scan output column to the remote column name, or null when it is not one */
    public FlightPredicateConverter(Function<ColumnRefOperator, String> columnName) {
        this.columnName = columnName;
    }

    public Optional<ScanPredicate> convert(ScalarOperator op) {
        if (op instanceof BinaryPredicateOperator) {
            return binary((BinaryPredicateOperator) op);
        }
        if (op instanceof InPredicateOperator) {
            return in((InPredicateOperator) op);
        }
        if (op instanceof IsNullPredicateOperator) {
            IsNullPredicateOperator isNull = (IsNullPredicateOperator) op;
            return column(isNull.getChild(0)).map(c -> new ScanPredicate.IsNull(c, isNull.isNotNull()));
        }
        if (op instanceof LikePredicateOperator) {
            return like((LikePredicateOperator) op);
        }
        if (op instanceof BetweenPredicateOperator) {
            return between((BetweenPredicateOperator) op);
        }
        if (op instanceof CompoundPredicateOperator) {
            return compound((CompoundPredicateOperator) op);
        }
        return Optional.empty();
    }

    private Optional<ScanPredicate> binary(BinaryPredicateOperator op) {
        ScalarOperator left = op.getChild(0);
        ScalarOperator right = op.getChild(1);
        boolean flipped = !(left instanceof ColumnRefOperator);
        if (!(left instanceof ColumnRefOperator && right instanceof ConstantOperator)
                && !(right instanceof ColumnRefOperator && left instanceof ConstantOperator)) {
            return expressionComparison(op);
        }
        if (!sameFamily(flipped ? right : left, flipped ? left : right)) {
            return Optional.empty();
        }
        Optional<String> column = column(flipped ? right : left);
        Optional<ScanPredicate.Literal> literal = literal(flipped ? left : right);
        Optional<ScanPredicate.Op> comparison = comparison(op.getBinaryType(), flipped);
        if (column.isEmpty() || literal.isEmpty() || comparison.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new ScanPredicate.Compare(column.get(), comparison.get(), literal.get()));
    }

    /** Column against column, or a whitelisted function of a column against a constant or another column. */
    private Optional<ScanPredicate> expressionComparison(BinaryPredicateOperator op) {
        ScalarOperator left = op.getChild(0);
        ScalarOperator right = op.getChild(1);
        Optional<ScanExpr> l = value(left);
        Optional<ScanExpr> r = value(right);
        Optional<ScanPredicate.Op> comparison = comparison(op.getBinaryType(), false);
        if (l.isEmpty() || r.isEmpty() || comparison.isEmpty() || !sameFamily(left, right)
                || (left instanceof ConstantOperator && right instanceof ConstantOperator)) {
            return Optional.empty();
        }
        return Optional.of(new ScanPredicate.ExprCompare(l.get(), comparison.get(), r.get()));
    }

    /** A column, a whitelisted function of a column, or a constant. */
    private Optional<ScanExpr> value(ScalarOperator op) {
        if (op instanceof ConstantOperator) {
            return literal(op).map(ScanExpr.Constant::new);
        }
        return operand(op);
    }

    /** A column or a whitelisted function of a column. */
    private Optional<ScanExpr> operand(ScalarOperator op) {
        try {
            if (op instanceof ColumnRefOperator) {
                return column(op).map(ScanExpr.Column::new);
            }
            if (op instanceof CallOperator) {
                return call((CallOperator) op);
            }
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
        return Optional.empty();
    }

    private Optional<ScanExpr> call(CallOperator call) {
        List<ScalarOperator> args = call.getChildren();
        switch (call.getFnName().toLowerCase()) {
            case "substring":
            case "substr": {
                if (args.size() != 3 || !(args.get(0) instanceof ColumnRefOperator)
                        || family(args.get(0)) != Family.STRING) {
                    return Optional.empty();
                }
                Optional<Long> from = integerConstant(args.get(1));
                Optional<Long> length = integerConstant(args.get(2));
                Optional<ScanExpr> text = operand(args.get(0));
                if (from.isEmpty() || length.isEmpty() || text.isEmpty() || from.get() < 1 || length.get() < 0) {
                    return Optional.empty();
                }
                return Optional.of(ScanExpr.Call.substring(text.get(), from.get(), length.get()));
            }
            case "year":
            case "month":
            case "day":
            case "dayofmonth": {
                if (args.size() != 1 || !(args.get(0) instanceof ColumnRefOperator)) {
                    return Optional.empty();
                }
                Family argument = family(args.get(0));
                Optional<ScanExpr> date = operand(args.get(0));
                if ((argument != Family.DATE && argument != Family.DATETIME) || date.isEmpty()) {
                    return Optional.empty();
                }
                String name = call.getFnName().toLowerCase();
                return Optional.of(name.equals("year") ? ScanExpr.Call.year(date.get())
                        : name.equals("month") ? ScanExpr.Call.month(date.get()) : ScanExpr.Call.day(date.get()));
            }
            default:
                return Optional.empty();
        }
    }

    private static Optional<Long> integerConstant(ScalarOperator op) {
        if (!(op instanceof ConstantOperator) || ((ConstantOperator) op).isNull()) {
            return Optional.empty();
        }
        ConstantOperator constant = (ConstantOperator) op;
        switch (constant.getType().getPrimitiveType()) {
            case TINYINT:
                return Optional.of((long) constant.getTinyInt());
            case SMALLINT:
                return Optional.of((long) constant.getSmallint());
            case INT:
                return Optional.of((long) constant.getInt());
            case BIGINT:
                return Optional.of(constant.getBigint());
            default:
                return Optional.empty();
        }
    }

    private Optional<ScanPredicate> like(LikePredicateOperator op) {
        if (op.isRegexp() || family(op.getChild(0)) != Family.STRING || !(op.getChild(1) instanceof ConstantOperator)
                || ((ConstantOperator) op.getChild(1)).isNull() || family(op.getChild(1)) != Family.STRING) {
            return Optional.empty();
        }
        Optional<ScanExpr> subject = operand(op.getChild(0));
        if (subject.isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(new ScanPredicate.Like(subject.get(), ((ConstantOperator) op.getChild(1)).getVarchar()));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    private static Optional<ScanPredicate.Op> comparison(BinaryType type, boolean flipped) {
        switch (type) {
            case EQ:
                return Optional.of(ScanPredicate.Op.EQ);
            case NE:
                return Optional.of(ScanPredicate.Op.NE);
            case LT:
                return Optional.of(flipped ? ScanPredicate.Op.GT : ScanPredicate.Op.LT);
            case LE:
                return Optional.of(flipped ? ScanPredicate.Op.GE : ScanPredicate.Op.LE);
            case GT:
                return Optional.of(flipped ? ScanPredicate.Op.LT : ScanPredicate.Op.GT);
            case GE:
                return Optional.of(flipped ? ScanPredicate.Op.LE : ScanPredicate.Op.GE);
            default:
                return Optional.empty();
        }
    }

    private Optional<ScanPredicate> in(InPredicateOperator op) {
        if (op.isSubquery()) {
            return Optional.empty();
        }
        Optional<String> column = column(op.getChild(0));
        Optional<ScanExpr> subject = column.isPresent() ? Optional.empty() : operand(op.getChild(0));
        if (column.isEmpty() && subject.isEmpty()) {
            return Optional.empty();
        }
        List<ScanPredicate.Literal> values = new ArrayList<>();
        for (int i = 1; i < op.getChildren().size(); i++) {
            Optional<ScanPredicate.Literal> value = literal(op.getChild(i));
            if (value.isEmpty() || !sameFamily(op.getChild(0), op.getChild(i))) {
                return Optional.empty();
            }
            values.add(value.get());
        }
        ScanPredicate in = column.isPresent() ? new ScanPredicate.In(column.get(), values)
                : new ScanPredicate.ExprIn(subject.get(), values);
        return Optional.of(op.isNotIn() ? new ScanPredicate.Not(in) : in);
    }

    private Optional<ScanPredicate> between(BetweenPredicateOperator op) {
        Optional<String> column = column(op.getChild(0));
        Optional<ScanPredicate.Literal> low = literal(op.getChild(1));
        Optional<ScanPredicate.Literal> high = literal(op.getChild(2));
        if (column.isEmpty() || low.isEmpty() || high.isEmpty()
                || !sameFamily(op.getChild(0), op.getChild(1)) || !sameFamily(op.getChild(0), op.getChild(2))) {
            return Optional.empty();
        }
        ScanPredicate range = new ScanPredicate.And(List.of(
                new ScanPredicate.Compare(column.get(), ScanPredicate.Op.GE, low.get()),
                new ScanPredicate.Compare(column.get(), ScanPredicate.Op.LE, high.get())));
        return Optional.of(op.isNotBetween() ? new ScanPredicate.Not(range) : range);
    }

    private Optional<ScanPredicate> compound(CompoundPredicateOperator op) {
        List<ScanPredicate> terms = new ArrayList<>();
        for (ScalarOperator child : op.getChildren()) {
            Optional<ScanPredicate> term = convert(child);
            if (term.isEmpty()) {
                return Optional.empty();
            }
            terms.add(term.get());
        }
        switch (op.getCompoundType()) {
            case AND:
                return Optional.of(new ScanPredicate.And(terms));
            case OR:
                return Optional.of(new ScanPredicate.Or(terms));
            case NOT:
                return Optional.of(new ScanPredicate.Not(terms.get(0)));
            default:
                return Optional.empty();
        }
    }

    /** Type families whose values the Flight server compares the way StarRocks does. */
    private enum Family { BOOLEAN, NUMERIC, STRING, DATE, DATETIME, OTHER }

    private static Family family(ScalarOperator op) {
        switch (op.getType().getPrimitiveType()) {
            case BOOLEAN:
                return Family.BOOLEAN;
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return Family.NUMERIC;
            case CHAR:
            case VARCHAR:
                return Family.STRING;
            case DATE:
                return Family.DATE;
            case DATETIME:
                return Family.DATETIME;
            default:
                return Family.OTHER;
        }
    }

    /**
     * Implicit casts were stripped before pushdown, so a column compared with a constant of another family
     * (text against a number, a date against a timestamp) would be coerced by the Flight server under its own
     * rules. Such comparisons stay in StarRocks.
     */
    private static boolean sameFamily(ScalarOperator column, ScalarOperator constant) {
        Family family = family(column);
        return family != Family.OTHER && family == family(constant);
    }

    private Optional<String> column(ScalarOperator op) {
        if (!(op instanceof ColumnRefOperator)) {
            return Optional.empty();
        }
        return Optional.ofNullable(columnName.apply((ColumnRefOperator) op));
    }

    private static Optional<ScanPredicate.Literal> literal(ScalarOperator op) {
        if (!(op instanceof ConstantOperator) || ((ConstantOperator) op).isNull()) {
            return Optional.empty();
        }
        ConstantOperator constant = (ConstantOperator) op;
        PrimitiveType type = constant.getType().getPrimitiveType();
        switch (type) {
            case BOOLEAN:
                return Optional.of(new ScanPredicate.BoolLit(constant.getBoolean()));
            case TINYINT:
                return Optional.of(new ScanPredicate.IntLit(constant.getTinyInt()));
            case SMALLINT:
                return Optional.of(new ScanPredicate.IntLit(constant.getSmallint()));
            case INT:
                return Optional.of(new ScanPredicate.IntLit(constant.getInt()));
            case BIGINT:
                return Optional.of(new ScanPredicate.IntLit(constant.getBigint()));
            case FLOAT:
            case DOUBLE: {
                double value = type == PrimitiveType.FLOAT ? constant.getFloat() : constant.getDouble();
                return Double.isNaN(value) || Double.isInfinite(value)
                        ? Optional.empty() : Optional.of(new ScanPredicate.DoubleLit(value));
            }
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return Optional.of(new ScanPredicate.DecimalLit(constant.getDecimal()));
            case CHAR:
            case VARCHAR:
                return Optional.of(new ScanPredicate.StringLit(constant.getVarchar()));
            case DATE:
                return Optional.of(new ScanPredicate.DateLit(constant.getDate().toLocalDate()));
            case DATETIME:
                return Optional.of(new ScanPredicate.TimestampLit(constant.getDatetime()));
            default:
                return Optional.empty();
        }
    }
}
