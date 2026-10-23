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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.connector.TableVersionRange;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.starrocks.ScanAggregate;
import pro.surpin.data.arrowflight.starrocks.ScanPredicate;
import pro.surpin.data.arrowflight.starrocks.TableStats;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class FlightStatisticsTest {
    private static final long ROWS = 3000;
    private static final long EPOCH_DAY = LocalDate.of(2020, 1, 1).toEpochDay();

    private static Field field(String name, ArrowType type) {
        return new Field(name, FieldType.nullable(type), null);
    }

    private static class FakeService implements FlightService {
        final AtomicInteger statisticCalls = new AtomicInteger();
        final List<List<String>> requestedColumns = new ArrayList<>();
        Optional<TableStats> answer = Optional.empty();
        RuntimeException failure;

        @Override
        public List<String> schemas() {
            return List.of("sales");
        }

        @Override
        public List<String> tables(String schema) {
            return List.of("orders");
        }

        @Override
        public Schema tableSchema(String schema, String table) {
            return new Schema(List.of(
                    field("id", new ArrowType.Int(64, true)),
                    field("d", new ArrowType.Date(DateUnit.DAY)),
                    field("name", ArrowType.Utf8.INSTANCE),
                    field("qty", new ArrowType.Int(32, true))));
        }

        @Override
        public Optional<TableStats> tableStatistics(String schema, String table, List<String> columns) {
            statisticCalls.incrementAndGet();
            requestedColumns.add(columns);
            if (failure != null) {
                throw failure;
            }
            // like the real server, answer only for the columns asked
            return answer.map(all -> {
                Map<String, TableStats.ColumnStats> asked = new LinkedHashMap<>();
                all.columns().forEach((name, stats) -> {
                    if (columns.contains(name)) {
                        asked.put(name, stats);
                    }
                });
                return new TableStats(all.rows(), asked);
            });
        }

        @Override
        public FlightInfo planScan(String schema, String table, List<String> columns, ScanPredicate where) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FlightInfo planScan(String schema, String table, ScanPredicate where, ScanAggregate aggregate) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }

    private static TableStats stats(Long idNdv) {
        Map<String, TableStats.ColumnStats> columns = new LinkedHashMap<>();
        columns.put("id", new TableStats.ColumnStats(1.0, 3000.0, 0, 24000, idNdv));
        columns.put("d", new TableStats.ColumnStats((double) EPOCH_DAY, (double) EPOCH_DAY + 364, 0, 12000, 365L));
        columns.put("name", new TableStats.ColumnStats(null, null, 300, 9000, 7L));
        columns.put("qty", new TableStats.ColumnStats(null, null, -1, 12000, null));
        return new TableStats(ROWS, columns);
    }

    private final FakeService service = new FakeService();
    private final FlightMetadata metadata = new FlightMetadata("flight", service,
            FlightConfig.from(Map.of("flight.uri", "grpc://localhost:8815")));
    private final Table table = metadata.getTable(null, "sales", "orders");

    private Map<ColumnRefOperator, Column> refs(String... names) {
        Map<ColumnRefOperator, Column> map = new LinkedHashMap<>();
        int id = 1;
        for (String name : names) {
            Column column = table.getColumn(name);
            map.put(new ColumnRefOperator(id++, column.getType(), name, true), column);
        }
        return map;
    }

    private Statistics ask(Map<ColumnRefOperator, Column> columns) {
        return metadata.getTableStatistics(null, table, columns, List.of(), null, -1, TableVersionRange.empty());
    }

    private static ColumnStatistic of(Statistics statistics, String name) {
        return statistics.getColumnStatistics().entrySet().stream()
                .filter(e -> e.getKey().getName().equals(name)).findFirst().orElseThrow().getValue();
    }

    @Test
    public void reportsRowCountAndColumnStatistics() {
        service.answer = Optional.of(stats(2990L));
        Statistics statistics = ask(refs("id", "d", "name"));
        Assertions.assertEquals(ROWS, statistics.getOutputRowCount(), 0);
        ColumnStatistic id = of(statistics, "id");
        Assertions.assertEquals(1.0, id.getMinValue(), 0);
        Assertions.assertEquals(3000.0, id.getMaxValue(), 0);
        Assertions.assertEquals(2990.0, id.getDistinctValuesCount(), 0);
        Assertions.assertEquals(0.0, id.getNullsFraction(), 0);
        Assertions.assertEquals(8.0, id.getAverageRowSize(), 0.5);
        ColumnStatistic name = of(statistics, "name");
        Assertions.assertEquals(0.1, name.getNullsFraction(), 1e-9);
        Assertions.assertEquals(7.0, name.getDistinctValuesCount(), 0);
        Assertions.assertTrue(Double.isInfinite(name.getMinValue()), "strings have no numeric bounds");
        Assertions.assertEquals(3.0, name.getAverageRowSize(), 0.01, "bytes per row");
    }

    @Test
    public void dateBoundsAreConvertedTheWayThePlannerConvertsDateConstants() {
        service.answer = Optional.of(stats(2990L));
        ColumnStatistic d = of(ask(refs("d")), "d");
        Assertions.assertEquals(Utils.getLongFromDateTime(LocalDate.of(2020, 1, 1).atStartOfDay()), d.getMinValue(), 0);
        Assertions.assertEquals(Utils.getLongFromDateTime(LocalDate.of(2020, 1, 1).plusDays(364).atStartOfDay()),
                d.getMaxValue(), 0);
    }

    @Test
    public void pendingDistinctCountIsEstimatedFromBoundsForIntegersAndFromRowsOtherwise() {
        service.answer = Optional.of(stats(null));
        Statistics statistics = ask(refs("id", "qty"));
        Assertions.assertEquals(3000.0, of(statistics, "id").getDistinctValuesCount(), 0, "dense key range");
        double qty = of(statistics, "qty").getDistinctValuesCount();
        Assertions.assertTrue(qty > 1 && qty <= ROWS, "unknown column gets a plausible count, got " + qty);
        Assertions.assertEquals(0.0, of(statistics, "qty").getNullsFraction(), 0, "unknown null count counts as none");
    }

    @Test
    public void serverWithoutStatisticsGivesNoStatistics() {
        service.answer = Optional.empty();
        Assertions.assertNull(ask(refs("id")));
    }

    @Test
    public void statisticsSwitchedOffAreNeitherAskedForNorUsed() {
        service.answer = Optional.of(stats(2990L));
        FlightMetadata off = new FlightMetadata("flight", service,
                FlightConfig.from(Map.of("flight.uri", "grpc://localhost:8815", "flight.statistics", "false")));
        Table offTable = off.getTable(null, "sales", "orders");
        Column id = offTable.getColumn("id");
        Map<ColumnRefOperator, Column> columns = Map.of(new ColumnRefOperator(1, id.getType(), "id", true), id);
        Assertions.assertNull(off.getTableStatistics(null, offTable, columns, List.of(), null, -1,
                TableVersionRange.empty()));
        Assertions.assertEquals(0, service.statisticCalls.get());
    }

    @Test
    public void aFailingServerNeverFailsPlanning() {
        service.failure = new IllegalStateException("boom");
        Assertions.assertNull(ask(refs("id")));
    }

    @Test
    public void completeAnswersAreCachedAndNewColumnsTriggerAFreshRequest() {
        service.answer = Optional.of(stats(2990L));
        ask(refs("id", "d"));
        ask(refs("id", "d"));
        ask(refs("id"));
        Assertions.assertEquals(1, service.statisticCalls.get());
        ask(refs("id", "name"));
        Assertions.assertEquals(2, service.statisticCalls.get());
        Assertions.assertTrue(service.requestedColumns.get(1).containsAll(List.of("id", "d", "name")),
                "a refresh asks for every column seen so far: " + service.requestedColumns.get(1));
    }

    @Test
    public void incompleteAnswersAreAskedAgainSoThePendingEstimateArrives() throws Exception {
        service.answer = Optional.of(stats(null));
        FlightMetadata quick = new FlightMetadata("flight", service,
                FlightConfig.from(Map.of("flight.uri", "grpc://localhost:8815")), 0L);
        Table quickTable = quick.getTable(null, "sales", "orders");
        Map<ColumnRefOperator, Column> columns = new LinkedHashMap<>();
        columns.put(new ColumnRefOperator(1, Type.BIGINT, "id", true), quickTable.getColumn("id"));
        quick.getTableStatistics(null, quickTable, columns, List.of(), null, -1, TableVersionRange.empty());
        service.answer = Optional.of(stats(2990L));
        Statistics later = quick.getTableStatistics(null, quickTable, columns, List.of(), null, -1,
                TableVersionRange.empty());
        Assertions.assertEquals(2, service.statisticCalls.get());
        Assertions.assertEquals(2990.0, later.getColumnStatistics().values().iterator().next()
                .getDistinctValuesCount(), 0);
    }
}
