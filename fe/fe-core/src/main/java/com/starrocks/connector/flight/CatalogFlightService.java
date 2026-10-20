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

import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.BearerCredentialWriter;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.vector.types.pojo.Schema;
import pro.surpin.data.arrowflight.starrocks.FlightCatalog;
import pro.surpin.data.arrowflight.starrocks.ScanAggregate;
import pro.surpin.data.arrowflight.starrocks.ScanPredicate;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** {@link FlightService} backed by the Flight SQL metadata client from the connector module. */
public class CatalogFlightService implements FlightService {
    private final FlightCatalog catalog;
    private final String uri;

    private CatalogFlightService(FlightCatalog catalog, String uri) {
        this.catalog = catalog;
        this.uri = uri;
    }

    public static CatalogFlightService connect(FlightConfig config) {
        CallOption[] options = config.getAuthType() == FlightConfig.AuthType.TOKEN
                ? new CallOption[] {new CredentialCallOption(new BearerCredentialWriter(config.getToken()))}
                : new CallOption[0];
        try {
            return new CatalogFlightService(FlightCatalog.connect(new Location(URI.create(config.getUri())), options),
                    config.getUri());
        } catch (RuntimeException e) {
            throw new StarRocksConnectorException("Cannot connect to Flight server %s: %s",
                    config.getUri(), e.getMessage());
        }
    }

    /** Runs one Flight call; failures name the server and never include credentials. */
    private <T> T call(Supplier<T> action) {
        try {
            return action.get();
        } catch (StarRocksConnectorException e) {
            throw e;
        } catch (RuntimeException e) {
            Throwable cause = e.getCause();
            String detail = e.getMessage() + (cause == null || cause.getMessage() == null
                    ? "" : " (" + cause.getMessage() + ")");
            throw new StarRocksConnectorException("Flight server %s request failed: %s", uri, detail);
        }
    }

    @Override
    public List<String> schemas() {
        return call(() -> catalog.schemas(null));
    }

    @Override
    public List<String> tables(String schema) {
        return call(() -> catalog.tables(null, schema).stream()
                .map(FlightCatalog.Table::name).collect(Collectors.toList()));
    }

    @Override
    public Schema tableSchema(String schema, String table) {
        return call(() -> {
            try {
                return catalog.tableSchema(schema, table);
            } catch (FlightRuntimeException e) {
                if (e.status().code() == CallStatus.NOT_FOUND.code()) {
                    return null;
                }
                throw e;
            }
        });
    }

    @Override
    public FlightInfo planScan(String schema, String table, List<String> columns, ScanPredicate where) {
        return call(() -> catalog.planScan(schema, table, columns, where));
    }

    @Override
    public FlightInfo planScan(String schema, String table, ScanPredicate where, ScanAggregate aggregate) {
        return call(() -> catalog.planScan(schema, table, where, aggregate));
    }

    @Override
    public Optional<List<Long>> distinctLongKeys(String schema, String table, String column, ScanPredicate where,
                                                 int maxKeys) {
        return call(() -> catalog.distinctLongKeys(schema, table, column, where, maxKeys));
    }

    @Override
    public void close() {
        try {
            catalog.close();
        } catch (Exception e) {
            throw new StarRocksConnectorException("Failed to close Flight client: %s", e.getMessage());
        }
    }
}
