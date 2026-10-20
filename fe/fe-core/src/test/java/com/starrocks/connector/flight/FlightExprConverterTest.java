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
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

public class FlightExprConverterTest {
    private static final Type D15_2 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 15, 2);
    private static final Type D16_2 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 16, 2);
    private static final Type D10_1 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 10, 1);
    private static final Type D31_4 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 31, 4);
    private static final Type D38_6 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 6);

    private final ColumnRefOperator price = new ColumnRefOperator(1, D15_2, "price", true);
    private final ColumnRefOperator disc = new ColumnRefOperator(2, D15_2, "disc", true);
    private final ColumnRefOperator tax = new ColumnRefOperator(3, D15_2, "tax", true);
    private final ColumnRefOperator mode = new ColumnRefOperator(4, Type.VARCHAR, "mode", true);
    private final ColumnRefOperator x = new ColumnRefOperator(5, Type.DOUBLE, "x", true);
    private final ColumnRefOperator y = new ColumnRefOperator(6, Type.DOUBLE, "y", true);
    private final ColumnRefOperator f = new ColumnRefOperator(7, Type.FLOAT, "f", true);
    private final ColumnRefOperator ghost = new ColumnRefOperator(8, D15_2, "ghost", true);
    private final FlightExprConverter converter = new FlightExprConverter(
            ref -> ref.equals(ghost) ? null : ref.getName());

    private static CallOperator call(String name, Type type, ScalarOperator... args) {
        return new CallOperator(name, type, Lists.newArrayList(args));
    }

    private String sql(ScalarOperator op) {
        Optional<String> rendered = converter.convert(op).map(e -> e.toSql());
        Assertions.assertTrue(rendered.isPresent(), "expected pushable: " + op);
        return rendered.get();
    }

    private void assertNotPushable(ScalarOperator op) {
        Assertions.assertTrue(converter.convert(op).isEmpty(), "expected not pushable: " + op);
    }

    /** price * (1 - discount), the way StarRocks types it: operands widened to decimals, scales adding up. */
    private ScalarOperator discountedPrice(Type productType) {
        ScalarOperator oneMinus = call("subtract", D16_2, ConstantOperator.createInt(1),
                new CastOperator(D16_2, disc, true));
        return call("multiply", productType, new CastOperator(D15_2, price, true),
                new CastOperator(D16_2, oneMinus, true));
    }

    @Test
    public void convertsExactDecimalArithmeticWithWideningCasts() {
        Assertions.assertEquals("(price * (1 - disc))", sql(discountedPrice(D31_4)));
        ScalarOperator charge = call("multiply", D38_6, discountedPrice(D31_4),
                call("add", D16_2, ConstantOperator.createInt(1), new CastOperator(D16_2, tax, true)));
        Assertions.assertEquals("((price * (1 - disc)) * (1 + tax))", sql(charge));
    }

    @Test
    public void rendersDecimalConstantsAtTheirScale() {
        ScalarOperator half = ConstantOperator.createDecimal(new BigDecimal("0.50"), D15_2);
        Assertions.assertEquals("(price * 0.50)", sql(call("multiply", D31_4, price, half)));
    }

    @Test
    public void declinesDecimalArithmeticWhoseResultScaleIsNotExact() {
        // StarRocks would round to scale 3 where the exact product has scale 4
        assertNotPushable(discountedPrice(ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 31, 3)));
    }

    @Test
    public void convertsDoubleArithmeticOnlyBetweenDoubles() {
        Assertions.assertEquals("(x * y)", sql(call("multiply", Type.DOUBLE, x, y)));
        Assertions.assertEquals("(x - y)", sql(call("subtract", Type.DOUBLE, x, y)));
        assertNotPushable(call("multiply", Type.DOUBLE, x, price));
        assertNotPushable(call("multiply", Type.DOUBLE, f, y));
    }

    @Test
    public void declinesNarrowingAndLossyCasts() {
        assertNotPushable(new CastOperator(D10_1, price));
        assertNotPushable(new CastOperator(Type.DOUBLE, price));
        assertNotPushable(new CastOperator(Type.BIGINT, price));
    }

    @Test
    public void convertsCaseWhenWithAPushablePredicate() {
        ScalarOperator when = BinaryPredicateOperator.eq(mode, ConstantOperator.createVarchar("MAIL"));
        ScalarOperator elseZero = new CastOperator(D15_2, ConstantOperator.createInt(0), true);
        ScalarOperator caseWhen = new CaseWhenOperator(D15_2, null, elseZero, List.of(when, price));
        Assertions.assertEquals("CASE WHEN (mode = 'MAIL') THEN price ELSE 0 END", sql(caseWhen));
    }

    @Test
    public void declinesCaseWithoutElseOrWithSeveralBranches() {
        ScalarOperator when = BinaryPredicateOperator.eq(mode, ConstantOperator.createVarchar("MAIL"));
        assertNotPushable(new CaseWhenOperator(D15_2, null, null, List.of(when, price)));
        ScalarOperator zero = new CastOperator(D15_2, ConstantOperator.createInt(0), true);
        assertNotPushable(new CaseWhenOperator(D15_2, null, zero, List.of(when, price, when, disc)));
    }

    @Test
    public void declinesUnsupportedShapes() {
        assertNotPushable(call("divide", Type.DOUBLE, x, y));
        assertNotPushable(call("upper", Type.VARCHAR, mode));
        assertNotPushable(ghost);
        assertNotPushable(f);
        assertNotPushable(mode);
        assertNotPushable(ConstantOperator.createNull(D15_2));
        assertNotPushable(call("add", D16_2, price, ghost));
    }
}
