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

import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.vector.types.pojo.Schema;
import pro.surpin.data.arrowflight.starrocks.ScanPredicate;

import java.util.List;

/** The Flight SQL operations the catalog needs; a seam so metadata can be tested without a server. */
public interface FlightService extends AutoCloseable {
    List<String> schemas();

    List<String> tables(String schema);

    /** Returns the table's Arrow schema, or null when the table does not exist. */
    Schema tableSchema(String schema, String table);

    FlightInfo planScan(String schema, String table, List<String> columns, ScanPredicate where);

    @Override
    void close();
}
