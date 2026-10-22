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
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ExternalTablePredicateExtractor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public class FlightPredicateConverterTest {
    private final ColumnRefOperator a = new ColumnRefOperator(1, Type.BIGINT, "a", true);
    private final ColumnRefOperator b = new ColumnRefOperator(2, Type.VARCHAR, "b", true);
    private final ColumnRefOperator d = new ColumnRefOperator(3, Type.DATE, "d", true);
    private final ColumnRefOperator t = new ColumnRefOperator(4, Type.DATETIME, "t", true);
    private final ColumnRefOperator x = new ColumnRefOperator(5, ScalarType.createDecimalV3NarrowestType(10, 2), "x", true);
    private final FlightPredicateConverter converter = new FlightPredicateConverter(
            ref -> "col_" + ref.getName());

    private String sql(ScalarOperator op) {
        Optional<String> rendered = converter.convert(op).map(p -> p.toSql());
        Assertions.assertTrue(rendered.isPresent(), "expected pushable: " + op);
        return rendered.get();
    }

    private void assertNotPushable(ScalarOperator op) {
        Assertions.assertTrue(converter.convert(op).isEmpty(), "expected not pushable: " + op);
    }

    @Test
    public void convertsComparisonsInEitherOrder() {
        Assertions.assertEquals("(col_a = 5)", sql(BinaryPredicateOperator.eq(a, ConstantOperator.createBigint(5))));
        Assertions.assertEquals("(col_a > 5)", sql(BinaryPredicateOperator.lt(ConstantOperator.createBigint(5), a)));
        Assertions.assertEquals("(col_a <> 5)", sql(BinaryPredicateOperator.ne(a, ConstantOperator.createBigint(5))));
        Assertions.assertEquals("(col_b = 'x''y')",
                sql(BinaryPredicateOperator.eq(b, ConstantOperator.createVarchar("x'y"))));
    }

    @Test
    public void convertsTemporalAndDecimalConstants() {
        Assertions.assertEquals("(col_d >= DATE '2026-10-17')", sql(BinaryPredicateOperator.ge(d,
                ConstantOperator.createDate(LocalDateTime.of(2026, 10, 17, 0, 0)))));
        Assertions.assertEquals("(col_t < TIMESTAMP '2026-10-17 01:02:03.0')", sql(BinaryPredicateOperator.lt(t,
                ConstantOperator.createDatetime(LocalDateTime.of(2026, 10, 17, 1, 2, 3)))));
        Assertions.assertEquals("(col_x = 1.50)", sql(BinaryPredicateOperator.eq(x,
                ConstantOperator.createDecimal(new BigDecimal("1.50"), x.getType()))));
    }

    @Test
    public void convertsInIsNullBetweenAndBooleanLogic() {
        Assertions.assertEquals("(col_a IN (1, 2))", sql(new InPredicateOperator(false, a,
                ConstantOperator.createBigint(1), ConstantOperator.createBigint(2))));
        Assertions.assertEquals("(NOT (col_a IN (1, 2)))", sql(new InPredicateOperator(true, a,
                ConstantOperator.createBigint(1), ConstantOperator.createBigint(2))));
        Assertions.assertEquals("(col_a IS NOT NULL)", sql(new IsNullPredicateOperator(true, a)));
        Assertions.assertEquals("((col_a >= 1) AND (col_a <= 9))", sql(new BetweenPredicateOperator(false, a,
                ConstantOperator.createBigint(1), ConstantOperator.createBigint(9))));
        Assertions.assertEquals("(NOT ((col_a >= 1) AND (col_a <= 9)))", sql(new BetweenPredicateOperator(true, a,
                ConstantOperator.createBigint(1), ConstantOperator.createBigint(9))));
        ScalarOperator or = CompoundPredicateOperator.or(
                BinaryPredicateOperator.eq(a, ConstantOperator.createBigint(1)),
                new IsNullPredicateOperator(b));
        Assertions.assertEquals("((col_a = 1) OR (col_b IS NULL))", sql(or));
        Assertions.assertEquals("(NOT (col_a = 1))",
                sql(CompoundPredicateOperator.not(BinaryPredicateOperator.eq(a, ConstantOperator.createBigint(1)))));
    }

    @Test
    public void refusesWhatItCannotRenderExactly() {
        // NULL constants, null-safe equality, explicit casts and unsupported functions stay in StarRocks.
        assertNotPushable(BinaryPredicateOperator.eq(a, ConstantOperator.createNull(Type.BIGINT)));
        assertNotPushable(new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL, a, ConstantOperator.createBigint(1)));
        assertNotPushable(BinaryPredicateOperator.eq(new CastOperator(Type.VARCHAR, a, false),
                ConstantOperator.createVarchar("1")));
        assertNotPushable(BinaryPredicateOperator.eq(
                new CallOperator("abs", Type.BIGINT, List.of(a)), ConstantOperator.createBigint(1)));
        assertNotPushable(new InPredicateOperator(false, a, ConstantOperator.createBigint(1),
                ConstantOperator.createNull(Type.BIGINT)));
        // One bad branch poisons a disjunction: nothing partial is ever returned for OR/NOT.
        assertNotPushable(CompoundPredicateOperator.or(
                BinaryPredicateOperator.eq(a, ConstantOperator.createBigint(1)),
                new CallOperator("abs", Type.BOOLEAN, List.of(a))));
    }

    @Test
    public void unmappedColumnIsNotPushable() {
        FlightPredicateConverter noColumns = new FlightPredicateConverter(ref -> null);
        Assertions.assertTrue(noColumns.convert(
                BinaryPredicateOperator.eq(a, ConstantOperator.createBigint(5))).isEmpty());
    }

    private ExternalTablePredicateExtractor flightExtractor() {
        return new ExternalTablePredicateExtractor(false, op -> converter.convert(op).isPresent());
    }

    @Test
    public void extractorKeepsUnrenderablePredicatesInStarRocks() {
        ScalarOperator pushable = BinaryPredicateOperator.eq(a, ConstantOperator.createBigint(1));
        ScalarOperator keep = BinaryPredicateOperator.eq(
                new CallOperator("abs", Type.BIGINT, List.of(a)), ConstantOperator.createBigint(2));
        ExternalTablePredicateExtractor extractor = flightExtractor();
        extractor.extract(CompoundPredicateOperator.and(pushable, keep));
        Assertions.assertEquals(pushable, extractor.getPushPredicate());
        Assertions.assertEquals(keep, extractor.getReservePredicate());
    }

    @Test
    public void extractorStripsImplicitCastsBeforePushing() {
        ScalarOperator withCast = BinaryPredicateOperator.gt(new CastOperator(Type.DOUBLE, a, true),
                ConstantOperator.createDouble(1.5));
        ExternalTablePredicateExtractor extractor = flightExtractor();
        extractor.extract(withCast);
        Assertions.assertEquals(BinaryPredicateOperator.gt(a, ConstantOperator.createDouble(1.5)),
                extractor.getPushPredicate());
        Assertions.assertNull(extractor.getReservePredicate());
    }

    @Test
    public void extractorReservesWholeDisjunctionWhenOneBranchIsNotRenderable() {
        ScalarOperator or = CompoundPredicateOperator.or(
                BinaryPredicateOperator.eq(a, ConstantOperator.createBigint(1)),
                new CallOperator("abs", Type.BOOLEAN, List.of(a)));
        ExternalTablePredicateExtractor extractor = flightExtractor();
        extractor.extract(or);
        Assertions.assertNull(extractor.getPushPredicate());
        Assertions.assertEquals(or, extractor.getReservePredicate());
    }

    @Test
    public void refusesComparisonsAcrossTypeFamilies() {
        // After implicit casts are stripped these would be compared by the Flight server with its own coercion
        // rules, which differ from StarRocks' (for example text against a number), so they stay in StarRocks.
        assertNotPushable(BinaryPredicateOperator.eq(b, ConstantOperator.createBigint(1)));
        assertNotPushable(BinaryPredicateOperator.eq(a, ConstantOperator.createVarchar("1")));
        assertNotPushable(BinaryPredicateOperator.eq(d,
                ConstantOperator.createDatetime(LocalDateTime.of(2026, 10, 17, 1, 2, 3))));
        assertNotPushable(new InPredicateOperator(false, b, ConstantOperator.createBigint(1)));
        assertNotPushable(new BetweenPredicateOperator(false, b, ConstantOperator.createBigint(1),
                ConstantOperator.createBigint(2)));
        assertNotPushable(BinaryPredicateOperator.eq(a, ConstantOperator.createBoolean(true)));
    }

    @Test
    public void numericFamiliesCompareWithEachOther() {
        Assertions.assertEquals("(col_a > 1.5)",
                sql(BinaryPredicateOperator.gt(a, ConstantOperator.createDouble(1.5))));
        Assertions.assertEquals("(col_x = 7)",
                sql(BinaryPredicateOperator.eq(x, ConstantOperator.createBigint(7))));
    }

    private final ColumnRefOperator other = new ColumnRefOperator(9, Type.BIGINT, "other", true);
    private final ColumnRefOperator s = new ColumnRefOperator(10, Type.VARCHAR, "s", true);

    private CallOperator substring(ScalarOperator text, int from, int length) {
        return new CallOperator("substring", Type.VARCHAR,
                List.of(text, ConstantOperator.createInt(from), ConstantOperator.createInt(length)));
    }

    @Test
    public void convertsColumnToColumnComparisonsOfOneFamily() {
        Assertions.assertEquals("(col_a < col_other)", sql(BinaryPredicateOperator.lt(a, other)));
        Assertions.assertEquals("(col_a <> col_other)", sql(BinaryPredicateOperator.ne(a, other)));
        Assertions.assertEquals("(col_b >= col_s)", sql(BinaryPredicateOperator.ge(b, s)));
        ColumnRefOperator d2 = new ColumnRefOperator(11, Type.DATE, "d2", true);
        Assertions.assertEquals("(col_d < col_d2)", sql(BinaryPredicateOperator.lt(d, d2)));
    }

    @Test
    public void refusesColumnComparisonsAcrossFamiliesAndUnmappedColumns() {
        assertNotPushable(BinaryPredicateOperator.lt(a, b));
        assertNotPushable(BinaryPredicateOperator.lt(d, t));
        assertNotPushable(new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL, a, other));
        FlightPredicateConverter onlyA = new FlightPredicateConverter(ref -> ref == a ? "col_a" : null);
        Assertions.assertTrue(onlyA.convert(BinaryPredicateOperator.lt(a, other)).isEmpty());
    }

    @Test
    public void convertsLikeAndNotLikeOnStringColumns() {
        Assertions.assertEquals("(col_b LIKE '%BRASS')",
                sql(new LikePredicateOperator(b, ConstantOperator.createVarchar("%BRASS"))));
        Assertions.assertEquals("(NOT (col_b LIKE '%special%requests%'))", sql(CompoundPredicateOperator.not(
                new LikePredicateOperator(b, ConstantOperator.createVarchar("%special%requests%")))));
        Assertions.assertEquals("(substring(col_b, 1, 2) LIKE '1_')",
                sql(new LikePredicateOperator(substring(b, 1, 2), ConstantOperator.createVarchar("1_"))));
    }

    @Test
    public void refusesLikeThatServerCouldEvaluateDifferently() {
        assertNotPushable(new LikePredicateOperator(LikePredicateOperator.LikeType.REGEXP, b,
                ConstantOperator.createVarchar("a.*")));
        assertNotPushable(new LikePredicateOperator(b, ConstantOperator.createVarchar("a\\%b")));
        assertNotPushable(new LikePredicateOperator(a, ConstantOperator.createVarchar("1%")));
        assertNotPushable(new LikePredicateOperator(b, s));
        assertNotPushable(new LikePredicateOperator(b, ConstantOperator.createNull(Type.VARCHAR)));
    }

    @Test
    public void convertsSubstringAndDatePartPredicates() {
        Assertions.assertEquals("(substring(col_b, 1, 2) IN ('13', '31'))", sql(new InPredicateOperator(false,
                substring(b, 1, 2), ConstantOperator.createVarchar("13"), ConstantOperator.createVarchar("31"))));
        Assertions.assertEquals("(NOT (substring(col_b, 1, 2) IN ('13')))", sql(new InPredicateOperator(true,
                substring(b, 1, 2), ConstantOperator.createVarchar("13"))));
        Assertions.assertEquals("(substring(col_b, 2, 3) = 'abc')", sql(BinaryPredicateOperator.eq(
                substring(b, 2, 3), ConstantOperator.createVarchar("abc"))));
        CallOperator year = new CallOperator("year", Type.SMALLINT, List.of(d));
        Assertions.assertEquals("(year(col_d) = 1995)", sql(BinaryPredicateOperator.eq(year,
                ConstantOperator.createInt(1995))));
        Assertions.assertEquals("(year(col_t) >= 1995)", sql(BinaryPredicateOperator.ge(
                new CallOperator("year", Type.SMALLINT, List.of(t)), ConstantOperator.createInt(1995))));
    }

    @Test
    public void refusesFunctionsOutsideTheWhitelistAndBadArguments() {
        assertNotPushable(BinaryPredicateOperator.eq(substring(b, 0, 2), ConstantOperator.createVarchar("13")));
        assertNotPushable(BinaryPredicateOperator.eq(substring(b, -3, 2), ConstantOperator.createVarchar("13")));
        assertNotPushable(BinaryPredicateOperator.eq(new CallOperator("substring", Type.VARCHAR,
                List.of(b, ConstantOperator.createInt(1))), ConstantOperator.createVarchar("13")));
        assertNotPushable(BinaryPredicateOperator.eq(new CallOperator("substring", Type.VARCHAR,
                List.of(b, a, ConstantOperator.createInt(2))), ConstantOperator.createVarchar("13")));
        assertNotPushable(BinaryPredicateOperator.eq(new CallOperator("year", Type.SMALLINT, List.of(a)),
                ConstantOperator.createInt(1995)));
        assertNotPushable(BinaryPredicateOperator.eq(new CallOperator("upper", Type.VARCHAR, List.of(b)),
                ConstantOperator.createVarchar("X")));
        assertNotPushable(BinaryPredicateOperator.eq(substring(b, 1, 2), ConstantOperator.createBigint(13)));
    }

    @Test
    public void extractorPushesExpressionPredicatesAndKeepsOthers() {
        ScalarOperator colCol = BinaryPredicateOperator.lt(a, other);
        ScalarOperator keep = BinaryPredicateOperator.eq(
                new CallOperator("abs", Type.BIGINT, List.of(a)), ConstantOperator.createBigint(2));
        ExternalTablePredicateExtractor extractor = flightExtractor();
        extractor.extract(CompoundPredicateOperator.and(colCol, keep));
        Assertions.assertEquals(colCol, extractor.getPushPredicate());
        Assertions.assertEquals(keep, extractor.getReservePredicate());
    }
}
