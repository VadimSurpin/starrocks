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

import com.starrocks.analysis.DescriptorTable;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.thrift.TJDBCTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;

import java.util.List;

/**
 * A table of a Flight SQL catalog. It is typed as a JDBC table so the optimizer reuses the external
 * JDBC scan operators (column pruning, predicate and limit pushdown); the plan builder turns it into a
 * {@code FlightScanNode} instead of a JDBC scan node.
 */
public class FlightTable extends JDBCTable {
    private final transient FlightConfig config;

    public FlightTable(long id, String name, List<Column> schema, String dbName, String catalogName,
                       FlightConfig config) {
        super(id, name, schema, dbName, catalogName);
        this.config = config;
    }

    public FlightConfig getConfig() {
        return config;
    }

    @Override
    public String getCatalogTableName() {
        return getName();
    }

    @Override
    public ProtocolType getProtocolType() {
        return ProtocolType.UNKNOWN;
    }

    /** The backend needs only the tuple layout; connection details travel in the scan node, not here. */
    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        TTableDescriptor descriptor = new TTableDescriptor(getId(), TTableType.JDBC_TABLE,
                getFullSchema().size(), 0, getName(), "");
        descriptor.setJdbcTable(new TJDBCTable());
        return descriptor;
    }
}
