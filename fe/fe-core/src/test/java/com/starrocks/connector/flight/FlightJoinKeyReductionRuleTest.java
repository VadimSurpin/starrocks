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
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.FlightJoinKeyReductionRule;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.starrocks.ScanAggregate;
import pro.surpin.data.arrowflight.starrocks.ScanPredicate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class FlightJoinKeyReductionRuleTest {
    /** Records key requests and answers from a table of canned keys. */
    private static final class FakeService implements FlightService {
        private final Map<String, List<Long>> keysByTable = new HashMap<>();
        private final List<String> requests = new ArrayList<>();
        private int lastMaxKeys;

        @Override
        public List<String> schemas() {
            return List.of();
        }

        @Override
        public List<String> tables(String schema) {
            return List.of();
        }

        @Override
        public Schema tableSchema(String schema, String table) {
            return null;
        }

        @Override
        public FlightInfo planScan(String schema, String table, List<String> columns, ScanPredicate where) {
            return null;
        }

        @Override
        public FlightInfo planScan(String schema, String table, ScanPredicate where, ScanAggregate aggregate) {
            return null;
        }

        @Override
        public Optional<List<Long>> distinctLongKeys(String schema, String table, String column,
                                                     ScanPredicate where, int maxKeys) {
            requests.add(schema + "." + table + "." + column + " where " + (where == null ? "-" : where.toSql()));
            lastMaxKeys = maxKeys;
            return Optional.ofNullable(keysByTable.get(table));
        }

        @Override
        public void close() {
        }
    }

    private final ColumnRefFactory factory = new ColumnRefFactory();
    private final FakeService service = new FakeService();
    private final Column fkCol = new Column("fk", Type.BIGINT);
    private final Column vCol = new Column("v", Type.BIGINT);
    private final Column dkCol = new Column("dk", Type.BIGINT);
    private final Column flagCol = new Column("flag", Type.BIGINT);
    private final Column nameCol = new Column("name", Type.VARCHAR);
    private final ColumnRefOperator fk = factory.create("fk", Type.BIGINT, true);
    private final ColumnRefOperator v = factory.create("v", Type.BIGINT, true);
    private final ColumnRefOperator dk = factory.create("dk", Type.BIGINT, true);
    private final ColumnRefOperator flag = factory.create("flag", Type.BIGINT, true);
    private final ColumnRefOperator name = factory.create("name", Type.VARCHAR, true);

    private FlightTable table(String tableName, List<Column> columns, Map<String, String> extra) {
        Map<String, String> props = new HashMap<>(extra);
        props.put("flight.uri", "grpc://localhost:8815");
        return new FlightTable(1, tableName, columns, "sales", "flight", FlightConfig.from(props), service);
    }

    private LogicalJDBCScanOperator facts(ScalarOperator predicate, Map<String, String> extra) {
        Map<ColumnRefOperator, Column> refs = new LinkedHashMap<>();
        refs.put(fk, fkCol);
        refs.put(v, vCol);
        Map<Column, ColumnRefOperator> cols = new LinkedHashMap<>();
        refs.forEach((r, c) -> cols.put(c, r));
        return new LogicalJDBCScanOperator(table("facts", List.of(fkCol, vCol), extra), refs, cols, -1, predicate, null);
    }

    private LogicalJDBCScanOperator dims(ScalarOperator predicate, Map<String, String> extra) {
        Map<ColumnRefOperator, Column> refs = new LinkedHashMap<>();
        refs.put(dk, dkCol);
        refs.put(flag, flagCol);
        refs.put(name, nameCol);
        Map<Column, ColumnRefOperator> cols = new LinkedHashMap<>();
        refs.forEach((r, c) -> cols.put(c, r));
        return new LogicalJDBCScanOperator(table("dims", List.of(dkCol, flagCol, nameCol), extra), refs, cols, -1,
                predicate, null);
    }

    private ScalarOperator flagIs(long value) {
        return BinaryPredicateOperator.eq(flag, ConstantOperator.createBigint(value));
    }

    private OptExpression join(JoinOperator type, ScalarOperator on, LogicalJDBCScanOperator left,
                               LogicalJDBCScanOperator right) {
        return OptExpression.create(new LogicalJoinOperator(type, on), OptExpression.create(left),
                OptExpression.create(right));
    }

    private ScalarOperator onKeys() {
        return BinaryPredicateOperator.eq(fk, dk);
    }

    private List<OptExpression> run(OptExpression input) {
        FlightJoinKeyReductionRule rule = FlightJoinKeyReductionRule.INSTANCE;
        if (!rule.check(input, OptimizerFactory.mockContext(factory))) {
            return List.of();
        }
        return rule.transform(input, OptimizerFactory.mockContext(factory));
    }

    private static ScalarOperator predicateOf(OptExpression scan) {
        return ((LogicalJDBCScanOperator) scan.getOp()).getPredicate();
    }

    @Test
    public void innerJoinReducesTheBigSideByTheFilteredSmallSide() {
        service.keysByTable.put("dims", List.of(10L, 20L, 30L));
        List<OptExpression> result = run(join(JoinOperator.INNER_JOIN, onKeys(), facts(null, Map.of()),
                dims(flagIs(5), Map.of())));

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(List.of("sales.dims.dk where (flag = 5)"), service.requests);
        Assertions.assertEquals(100_000, service.lastMaxKeys);
        ScalarOperator pushed = predicateOf(result.get(0).inputAt(0));
        Assertions.assertTrue(pushed instanceof InPredicateOperator, String.valueOf(pushed));
        InPredicateOperator in = (InPredicateOperator) pushed;
        Assertions.assertFalse(in.isNotIn());
        Assertions.assertEquals(fk, in.getChild(0));
        Assertions.assertEquals(List.of(10L, 20L, 30L), in.getChildren().subList(1, 4).stream()
                .map(c -> ((ConstantOperator) c).getBigint()).toList());
        Assertions.assertEquals(flagIs(5), predicateOf(result.get(0).inputAt(1)));
    }

    @Test
    public void existingPredicateOfTheReducedSideIsKept() {
        service.keysByTable.put("dims", List.of(1L, 2L));
        ScalarOperator vPositive = BinaryPredicateOperator.gt(v, ConstantOperator.createBigint(0));
        List<OptExpression> result = run(join(JoinOperator.INNER_JOIN, onKeys(), facts(vPositive, Map.of()),
                dims(flagIs(5), Map.of())));
        Assertions.assertEquals(1, result.size());
        String rendered = String.valueOf(predicateOf(result.get(0).inputAt(0)));
        Assertions.assertTrue(rendered.contains("IN") && rendered.contains(">"), rendered);
    }

    @Test
    public void bothSidesAreReducedWhenBothAreFiltered() {
        service.keysByTable.put("dims", List.of(1L, 2L));
        service.keysByTable.put("facts", List.of(2L, 3L));
        ScalarOperator factFilter = BinaryPredicateOperator.gt(v, ConstantOperator.createBigint(0));
        List<OptExpression> result = run(join(JoinOperator.INNER_JOIN, onKeys(), facts(factFilter, Map.of()),
                dims(flagIs(5), Map.of())));
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(2, service.requests.size());
        Assertions.assertTrue(String.valueOf(predicateOf(result.get(0).inputAt(0))).contains("IN"));
        Assertions.assertTrue(String.valueOf(predicateOf(result.get(0).inputAt(1))).contains("IN"));
    }

    @Test
    public void semiJoinReducesTheProbeSideOnly() {
        service.keysByTable.put("dims", List.of(7L));
        List<OptExpression> result = run(join(JoinOperator.LEFT_SEMI_JOIN, onKeys(), facts(null, Map.of()),
                dims(flagIs(5), Map.of())));
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(predicateOf(result.get(0).inputAt(0)) instanceof InPredicateOperator);
        Assertions.assertEquals(flagIs(5), predicateOf(result.get(0).inputAt(1)));
    }

    @Test
    public void declinesWithoutASelectiveFilterOnTheSourceSide() {
        service.keysByTable.put("dims", List.of(1L));
        Assertions.assertTrue(run(join(JoinOperator.INNER_JOIN, onKeys(), facts(null, Map.of()),
                dims(null, Map.of()))).isEmpty());
        Assertions.assertTrue(service.requests.isEmpty());
    }

    @Test
    public void declinesWhenTheFilterCannotBeFullyPushedToTheServer() {
        service.keysByTable.put("dims", List.of(1L));
        CallOperator abs = new CallOperator("abs", Type.BIGINT, new ArrayList<>(List.of(flag)));
        ScalarOperator unpushable = BinaryPredicateOperator.eq(abs, ConstantOperator.createBigint(2));
        Assertions.assertTrue(run(join(JoinOperator.INNER_JOIN, onKeys(), facts(null, Map.of()),
                dims(unpushable, Map.of()))).isEmpty());
        Assertions.assertTrue(service.requests.isEmpty());
    }

    @Test
    public void declinesWhenThereAreTooManyKeys() {
        // the fake has no entry for "dims": the server reported more keys than the limit
        Assertions.assertTrue(run(join(JoinOperator.INNER_JOIN, onKeys(), facts(null, Map.of()),
                dims(flagIs(5), Map.of()))).isEmpty());
        Assertions.assertEquals(1, service.requests.size());
    }

    @Test
    public void declinesJoinsWhereTheOtherSideCannotBeRestricted() {
        service.keysByTable.put("dims", List.of(1L));
        for (JoinOperator type : List.of(JoinOperator.FULL_OUTER_JOIN, JoinOperator.LEFT_ANTI_JOIN,
                JoinOperator.LEFT_OUTER_JOIN)) {
            Assertions.assertTrue(run(join(type, onKeys(), facts(null, Map.of()), dims(flagIs(5), Map.of()))).isEmpty(),
                    type.toString());
        }
        Assertions.assertTrue(service.requests.isEmpty());
    }

    @Test
    public void declinesNonEquiAndNonColumnJoinConditions() {
        service.keysByTable.put("dims", List.of(1L));
        ScalarOperator lessThan = new BinaryPredicateOperator(BinaryType.LT, fk, dk);
        Assertions.assertTrue(run(join(JoinOperator.INNER_JOIN, lessThan, facts(null, Map.of()),
                dims(flagIs(5), Map.of()))).isEmpty());
        ScalarOperator nullSafe = new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL, fk, dk);
        Assertions.assertTrue(run(join(JoinOperator.INNER_JOIN, nullSafe, facts(null, Map.of()),
                dims(flagIs(5), Map.of()))).isEmpty());
        Assertions.assertTrue(service.requests.isEmpty());
    }

    @Test
    public void declinesNonIntegerKeys() {
        service.keysByTable.put("dims", List.of(1L));
        ColumnRefOperator textKey = name;
        ScalarOperator onText = BinaryPredicateOperator.eq(textKey, textKey);
        Assertions.assertTrue(run(join(JoinOperator.INNER_JOIN, onText, facts(null, Map.of()),
                dims(flagIs(5), Map.of()))).isEmpty());
        Assertions.assertTrue(service.requests.isEmpty());
    }

    @Test
    public void honoursTheCatalogSwitchAndKeyLimit() {
        service.keysByTable.put("dims", List.of(1L));
        Assertions.assertTrue(run(join(JoinOperator.INNER_JOIN, onKeys(),
                facts(null, Map.of("flight.join_key_reduction", "false")),
                dims(flagIs(5), Map.of("flight.join_key_reduction", "false")))).isEmpty());
        Assertions.assertTrue(service.requests.isEmpty());

        run(join(JoinOperator.INNER_JOIN, onKeys(),
                facts(null, Map.of("flight.join_key_reduction.max_keys", "25")),
                dims(flagIs(5), Map.of("flight.join_key_reduction.max_keys", "25"))));
        Assertions.assertEquals(25, service.lastMaxKeys);
    }

    @Test
    public void declinesKeysThatDoNotFitTheReducedColumnType() {
        // the reduced side's key is INT, the dimension keys exceed its range
        Column smallFk = new Column("fk", Type.INT);
        ColumnRefOperator smallRef = factory.create("sfk", Type.INT, true);
        Map<ColumnRefOperator, Column> refs = new LinkedHashMap<>();
        refs.put(smallRef, smallFk);
        Map<Column, ColumnRefOperator> cols = new LinkedHashMap<>();
        cols.put(smallFk, smallRef);
        LogicalJDBCScanOperator small = new LogicalJDBCScanOperator(
                table("facts", List.of(smallFk), Map.of()), refs, cols, -1, null, null);
        service.keysByTable.put("dims", List.of(1L, 5_000_000_000L));
        Assertions.assertTrue(run(join(JoinOperator.INNER_JOIN, BinaryPredicateOperator.eq(smallRef, dk), small,
                dims(flagIs(5), Map.of()))).isEmpty());
    }
}
