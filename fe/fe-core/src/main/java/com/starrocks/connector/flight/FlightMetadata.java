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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.TableVersionRange;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pro.surpin.data.arrowflight.starrocks.ScanPredicate;
import pro.surpin.data.arrowflight.starrocks.TableStats;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/** Exposes the schemas and tables of a Flight SQL server as a StarRocks catalog. */
public class FlightMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(FlightMetadata.class);
    private static final long COMPLETE_STATISTICS_TTL_NANOS = TimeUnit.MINUTES.toNanos(10);
    private static final long DEFAULT_PENDING_REFRESH_MILLIS = 15_000L;

    /** A server answer with the moment it arrived; one per table, covering every column asked for so far. */
    private static final class CachedStatistics {
        private final TableStats stats;
        private final long loadedNanos;

        private CachedStatistics(TableStats stats, long loadedNanos) {
            this.stats = stats;
            this.loadedNanos = loadedNanos;
        }

        boolean complete() {
            return stats.columns().values().stream().allMatch(column -> column.ndv() != null);
        }
    }

    private final long pendingRefreshNanos;
    private final Map<String, CachedStatistics> statistics = new ConcurrentHashMap<>();
    private final String catalogName;
    private final FlightService service;
    private final FlightConfig config;
    private final Cache<String, Table> tableCache;
    private final Map<String, Integer> tableIds = new ConcurrentHashMap<>();

    public FlightMetadata(String catalogName, FlightService service, FlightConfig config) {
        this(catalogName, service, config, DEFAULT_PENDING_REFRESH_MILLIS);
    }

    /**
     * @param pendingRefreshMillis how long an answer whose distinct counts are still pending is reused before the
     *                             server is asked again
     */
    FlightMetadata(String catalogName, FlightService service, FlightConfig config, long pendingRefreshMillis) {
        this.pendingRefreshNanos = TimeUnit.MILLISECONDS.toNanos(pendingRefreshMillis);
        this.catalogName = catalogName;
        this.service = service;
        this.config = config;
        this.tableCache = CacheBuilder.newBuilder()
                .expireAfterWrite(config.getCacheTtlSeconds(), TimeUnit.SECONDS).build();
    }

    @Override
    public Table.TableType getTableType() {
        return Table.TableType.JDBC;
    }

    @Override
    public List<String> listDbNames(ConnectContext context) {
        return service.schemas();
    }

    @Override
    public Database getDb(ConnectContext context, String name) {
        return service.schemas().contains(name) ? new Database(0, name) : null;
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        return service.tables(dbName);
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        String key = dbName + "." + tblName;
        Table cached = tableCache.getIfPresent(key);
        if (cached != null) {
            return cached;
        }
        Schema schema = service.tableSchema(dbName, tblName);
        if (schema == null) {
            return null;
        }
        List<Column> columns = new ArrayList<>();
        for (Field field : schema.getFields()) {
            if (!ScanPredicate.IDENTIFIER.matcher(field.getName()).matches()) {
                throw new StarRocksConnectorException(
                        "Column '%s' of %s cannot be queried through Flight: only unquoted ASCII identifiers "
                                + "([A-Za-z_][A-Za-z0-9_]*) are supported", field.getName(), key);
            }
            columns.add(new Column(field.getName(), FlightTypeMapper.toStarRocks(field), true));
        }
        int id = tableIds.computeIfAbsent(key, k -> ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt());
        Table table = new FlightTable(id, tblName, columns, dbName, catalogName, config, service);
        tableCache.put(key, table);
        return table;
    }

    /**
     * Row count and column statistics from the Flight server, so the optimizer can order joins and pick broadcast
     * or shuffle instead of treating every scan as one row. Returns null when the server offers none, in which
     * case the caller keeps its defaults; a failing server never fails planning.
     */
    @Override
    public Statistics getTableStatistics(OptimizerContext session, Table table,
                                         Map<ColumnRefOperator, Column> columns, List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate, long limit, TableVersionRange versionRange) {
        if (!(table instanceof FlightTable)) {
            return null;
        }
        Optional<TableStats> stats = tableStats((FlightTable) table, columns.values());
        if (stats.isEmpty() || stats.get().rows() <= 0) {
            return null;
        }
        Statistics.Builder builder = Statistics.builder().setOutputRowCount(stats.get().rows());
        for (Map.Entry<ColumnRefOperator, Column> entry : columns.entrySet()) {
            TableStats.ColumnStats column = stats.get().columns().get(entry.getValue().getName());
            if (column != null) {
                builder.addColumnStatistic(entry.getKey(),
                        columnStatistic(entry.getValue().getType(), stats.get().rows(), column));
            }
        }
        return builder.build();
    }

    private Optional<TableStats> tableStats(FlightTable table, java.util.Collection<Column> columns) {
        String key = table.getCatalogDBName() + "." + table.getName();
        Set<String> wanted = new LinkedHashSet<>();
        columns.forEach(column -> wanted.add(column.getName()));
        long now = System.nanoTime();
        CachedStatistics cached = statistics.get(key);
        if (cached != null && cached.stats.columns().keySet().containsAll(wanted)) {
            long age = now - cached.loadedNanos;
            if (age < (cached.complete() ? COMPLETE_STATISTICS_TTL_NANOS : pendingRefreshNanos)) {
                return Optional.of(cached.stats);
            }
        }
        if (cached != null) {
            wanted.addAll(cached.stats.columns().keySet());
        }
        try {
            Optional<TableStats> fresh = service.tableStatistics(table.getCatalogDBName(), table.getName(),
                    new ArrayList<>(wanted));
            fresh.ifPresent(stats -> statistics.put(key, new CachedStatistics(stats, System.nanoTime())));
            return fresh;
        } catch (RuntimeException e) {
            LOG.warn("Flight statistics of {} are unavailable: {}", key, e.getMessage());
            return Optional.empty();
        }
    }

    private static ColumnStatistic columnStatistic(Type type, long rows, TableStats.ColumnStats stats) {
        double nonNull = Math.max(1, rows - Math.max(0, stats.nulls()));
        ColumnStatistic.Builder builder = ColumnStatistic.builder();
        Double min = stats.min();
        Double max = stats.max();
        if (min != null && max != null) {
            builder.setMinValue(bound(type, min)).setMaxValue(bound(type, max));
        }
        builder.setNullsFraction(stats.nulls() < 0 ? 0 : (double) stats.nulls() / rows);
        builder.setAverageRowSize(stats.bytes() > 0 ? (double) stats.bytes() / rows : Math.max(1, type.getTypeSize()));
        builder.setDistinctValuesCount(stats.ndv() != null ? Math.min(stats.ndv(), nonNull)
                : guessDistinct(type, nonNull, min, max));
        return builder.build();
    }

    /** Statistics of a date or timestamp are seconds, the way the planner converts constants. */
    private static double bound(Type type, double value) {
        if (type.getPrimitiveType() == PrimitiveType.DATE) {
            return Utils.getLongFromDateTime(LocalDate.ofEpochDay((long) value).atStartOfDay());
        }
        if (type.getPrimitiveType() == PrimitiveType.DATETIME) {
            return Utils.getLongFromDateTime(LocalDateTime.ofEpochSecond((long) value, 0, ZoneOffset.UTC));
        }
        return value;
    }

    /** Until the server's estimate arrives: dense integer and date ranges are keys, anything else a tenth of the rows. */
    private static double guessDistinct(Type type, double nonNull, Double min, Double max) {
        boolean integral = type.isIntegerType();
        boolean date = type.getPrimitiveType() == PrimitiveType.DATE;
        if ((integral || date) && min != null && max != null) {
            return Math.max(1, Math.min(nonNull, max - min + 1));
        }
        return Math.max(1, Math.min(nonNull, nonNull / 10));
    }

    @Override
    public void shutdown() {
        service.close();
    }
}
