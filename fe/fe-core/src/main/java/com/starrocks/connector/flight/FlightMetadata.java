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
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import pro.surpin.data.arrowflight.starrocks.ScanPredicate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/** Exposes the schemas and tables of a Flight SQL server as a StarRocks catalog. */
public class FlightMetadata implements ConnectorMetadata {
    private final String catalogName;
    private final FlightService service;
    private final FlightConfig config;
    private final Cache<String, Table> tableCache;
    private final Map<String, Integer> tableIds = new ConcurrentHashMap<>();

    public FlightMetadata(String catalogName, FlightService service, FlightConfig config) {
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
        Table table = new FlightTable(id, tblName, columns, dbName, catalogName, config);
        tableCache.put(key, table);
        return table;
    }

    @Override
    public void shutdown() {
        service.close();
    }
}
