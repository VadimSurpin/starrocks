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

import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import pro.surpin.data.arrowflight.starrocks.ScanExpr;
import pro.surpin.data.arrowflight.starrocks.ScanPredicate;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.function.Function;

/**
 * Converts a numeric StarRocks scalar expression into a {@link ScanExpr} the Flight server evaluates inside an
 * aggregate. The server must produce exactly what StarRocks would, so the converter only accepts:
 * <ul>
 *   <li>{@code + - *} on exact types (integers and decimals) whose StarRocks result scale is the exact one
 *       (scales add up for products and take the maximum for sums), so no rounding happens on either side;</li>
 *   <li>{@code + - *} where every operand is DOUBLE;</li>
 *   <li>value-preserving widening casts (integer or decimal to a decimal with at least the same scale and integer
 *       digits), which are dropped because the server widens implicitly;</li>
 *   <li>{@code CASE WHEN cond THEN a ELSE b END} with a predicate {@link FlightPredicateConverter} accepts.</li>
 * </ul>
 * Anything else (division, FLOAT, functions, lossy casts) yields empty and stays inside StarRocks.
 */
public final class FlightExprConverter {
    private final Function<ColumnRefOperator, String> columnName;
    private final FlightPredicateConverter predicates;

    /** @param columnName maps a scan output column to the remote column name, or null when it is not one */
    public FlightExprConverter(Function<ColumnRefOperator, String> columnName) {
        this.columnName = columnName;
        this.predicates = new FlightPredicateConverter(columnName);
    }

    public Optional<ScanExpr> convert(ScalarOperator op) {
        if (op instanceof ColumnRefOperator) {
            String name = columnName.apply((ColumnRefOperator) op);
            return name != null && usable(op.getType()) ? Optional.of(new ScanExpr.Column(name)) : Optional.empty();
        }
        if (op instanceof ConstantOperator) {
            return constant((ConstantOperator) op);
        }
        if (op instanceof CastOperator) {
            return cast((CastOperator) op);
        }
        if (op instanceof CaseWhenOperator) {
            return caseWhen((CaseWhenOperator) op);
        }
        if (op instanceof CallOperator) {
            return arithmetic((CallOperator) op);
        }
        return Optional.empty();
    }

    private static boolean isDouble(Type type) {
        return type.getPrimitiveType() == PrimitiveType.DOUBLE;
    }

    private static boolean isInteger(Type type) {
        switch (type.getPrimitiveType()) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                return true;
            default:
                return false;
        }
    }

    private static boolean isDecimal(Type type) {
        return type.isDecimalV3();
    }

    private static boolean exact(Type type) {
        return isInteger(type) || isDecimal(type);
    }

    private static boolean usable(Type type) {
        return isDouble(type) || exact(type);
    }

    private static int scale(Type type) {
        return isDecimal(type) ? ((ScalarType) type).getScalarScale() : 0;
    }

    private static int integerDigits(Type type) {
        if (isDecimal(type)) {
            ScalarType decimal = (ScalarType) type;
            return decimal.getScalarPrecision() - decimal.getScalarScale();
        }
        switch (type.getPrimitiveType()) {
            case TINYINT:
                return 3;
            case SMALLINT:
                return 5;
            case INT:
                return 10;
            default:
                return 19;
        }
    }

    private Optional<ScanExpr> constant(ConstantOperator constant) {
        if (constant.isNull()) {
            return Optional.empty();
        }
        Type type = constant.getType();
        switch (type.getPrimitiveType()) {
            case TINYINT:
                return Optional.of(new ScanExpr.Constant(new ScanPredicate.IntLit(constant.getTinyInt())));
            case SMALLINT:
                return Optional.of(new ScanExpr.Constant(new ScanPredicate.IntLit(constant.getSmallint())));
            case INT:
                return Optional.of(new ScanExpr.Constant(new ScanPredicate.IntLit(constant.getInt())));
            case BIGINT:
                return Optional.of(new ScanExpr.Constant(new ScanPredicate.IntLit(constant.getBigint())));
            case DOUBLE: {
                double value = constant.getDouble();
                return Double.isNaN(value) || Double.isInfinite(value)
                        ? Optional.empty() : Optional.of(new ScanExpr.Constant(new ScanPredicate.DoubleLit(value)));
            }
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128: {
                BigDecimal value = constant.getDecimal();
                int scale = scale(type);
                if (value.scale() < scale) {
                    value = value.setScale(scale);
                }
                return Optional.of(new ScanExpr.Constant(new ScanPredicate.DecimalLit(value)));
            }
            default:
                return Optional.empty();
        }
    }

    /** A cast the server reproduces by implicit widening, so the cast itself is dropped. */
    private Optional<ScanExpr> cast(CastOperator cast) {
        Type from = cast.getChild(0).getType();
        Type to = cast.getType();
        boolean widening = isDouble(from) && isDouble(to)
                || exact(from) && isDecimal(to) && scale(to) >= scale(from)
                && integerDigits(to) >= integerDigits(from)
                || isInteger(from) && isInteger(to) && integerDigits(to) >= integerDigits(from);
        return widening ? convert(cast.getChild(0)) : Optional.empty();
    }

    private Optional<ScanExpr> arithmetic(CallOperator call) {
        ScanExpr.Op op;
        switch (call.getFnName().toLowerCase()) {
            case "add":
                op = ScanExpr.Op.ADD;
                break;
            case "subtract":
                op = ScanExpr.Op.SUB;
                break;
            case "multiply":
                op = ScanExpr.Op.MUL;
                break;
            default:
                return Optional.empty();
        }
        if (call.getChildren().size() != 2) {
            return Optional.empty();
        }
        ScalarOperator left = call.getChild(0);
        ScalarOperator right = call.getChild(1);
        if (!exactResult(call.getType(), op, left.getType(), right.getType())) {
            return Optional.empty();
        }
        Optional<ScanExpr> l = convert(left);
        Optional<ScanExpr> r = convert(right);
        if (l.isEmpty() || r.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new ScanExpr.Arithmetic(op, l.get(), r.get()));
    }

    private static boolean exactResult(Type result, ScanExpr.Op op, Type left, Type right) {
        if (isDouble(result)) {
            return isDouble(left) && isDouble(right);
        }
        if (!isDecimal(result) || !exact(left) || !exact(right)) {
            return false;
        }
        int expected = op == ScanExpr.Op.MUL ? scale(left) + scale(right) : Math.max(scale(left), scale(right));
        return scale(result) == expected;
    }

    private Optional<ScanExpr> caseWhen(CaseWhenOperator op) {
        if (op.hasCase() || !op.hasElse() || op.getWhenClauseSize() != 1 || !usable(op.getType())) {
            return Optional.empty();
        }
        Optional<ScanPredicate> condition = predicates.convert(op.getWhenClause(0));
        Optional<ScanExpr> then = convert(op.getThenClause(0));
        Optional<ScanExpr> otherwise = convert(op.getElseClause());
        if (condition.isEmpty() || then.isEmpty() || otherwise.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new ScanExpr.Case(condition.get(), then.get(), otherwise.get()));
    }
}
