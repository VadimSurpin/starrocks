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

import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;

/** Connector for the {@code flight} external catalog type. */
public class FlightConnector implements Connector {
    private final String catalogName;
    private final FlightConfig config;
    private FlightMetadata metadata;

    public FlightConnector(ConnectorContext context) {
        this.catalogName = context.getCatalogName();
        this.config = FlightConfig.from(context.getProperties());
    }

    @Override
    public synchronized ConnectorMetadata getMetadata() {
        if (metadata == null) {
            metadata = new FlightMetadata(catalogName, CatalogFlightService.connect(config), config);
        }
        return metadata;
    }

    @Override
    public synchronized void shutdown() {
        if (metadata != null) {
            metadata.shutdown();
            metadata = null;
        }
    }
}
