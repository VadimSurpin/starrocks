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

import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.starrocks.ScanPredicate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class FlightMetadataTest {
    private static Field field(String name, ArrowType type) {
        return new Field(name, FieldType.nullable(type), null);
    }

    private static class FakeService implements FlightService {
        final Map<String, Schema> schemas = new HashMap<>();
        final AtomicInteger schemaCalls = new AtomicInteger();

        @Override
        public List<String> schemas() {
            return List.of("sales", "other");
        }

        @Override
        public List<String> tables(String schema) {
            return "sales".equals(schema) ? List.of("orders", "bad") : List.of();
        }

        @Override
        public Schema tableSchema(String schema, String table) {
            schemaCalls.incrementAndGet();
            return schemas.get(table);
        }

        @Override
        public FlightInfo planScan(String schema, String table, List<String> columns, ScanPredicate where) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FlightInfo planScan(String schema, String table, ScanPredicate where,
                                   pro.surpin.data.arrowflight.starrocks.ScanAggregate aggregate) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }

    private static FlightMetadata metadata(FakeService service) {
        FakeService s = service;
        s.schemas.put("orders", new Schema(List.of(
                field("id", new ArrowType.Int(64, true)),
                field("name", ArrowType.Utf8.INSTANCE))));
        s.schemas.put("bad", new Schema(List.of(field("order date", ArrowType.Utf8.INSTANCE))));
        return new FlightMetadata("flight", service,
                FlightConfig.from(Map.of("flight.uri", "grpc://localhost:8815")));
    }

    @Test
    public void listsDatabasesAndTables() {
        FlightMetadata metadata = metadata(new FakeService());
        Assertions.assertEquals(List.of("sales", "other"), metadata.listDbNames(null));
        Assertions.assertEquals(List.of("orders", "bad"), metadata.listTableNames(null, "sales"));
        Assertions.assertNotNull(metadata.getDb(null, "sales"));
        Assertions.assertNull(metadata.getDb(null, "missing"));
    }

    @Test
    public void getTableMapsColumnsAndCachesTheSchema() {
        FakeService service = new FakeService();
        FlightMetadata metadata = metadata(service);
        Table first = metadata.getTable(null, "sales", "orders");
        Table second = metadata.getTable(null, "sales", "orders");
        Assertions.assertInstanceOf(FlightTable.class, first);
        Assertions.assertEquals(2, first.getFullSchema().size());
        Assertions.assertEquals(Type.BIGINT, first.getColumn("id").getType());
        Assertions.assertTrue(first.getColumn("name").getType().isVarchar());
        Assertions.assertSame(first, second);
        Assertions.assertEquals(1, service.schemaCalls.get());
    }

    @Test
    public void rejectsColumnNamesTheServerCannotBeQueriedWith() {
        FlightMetadata metadata = metadata(new FakeService());
        StarRocksConnectorException e = Assertions.assertThrows(StarRocksConnectorException.class,
                () -> metadata.getTable(null, "sales", "bad"));
        Assertions.assertTrue(e.getMessage().contains("order date"), e.getMessage());
    }

    @Test
    public void missingTableReturnsNull() {
        FlightMetadata metadata = metadata(new FakeService());
        Assertions.assertNull(metadata.getTable(null, "sales", "nope"));
    }
}
