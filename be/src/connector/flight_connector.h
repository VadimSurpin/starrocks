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

#pragma once

#include <deque>
#include <memory>

#include "connector/connector.h"
#include "connector/flight_scan_reader.h"

namespace starrocks::connector {

class FlightConnector final : public Connector {
public:
    ~FlightConnector() override = default;

    DataSourceProviderPtr create_data_source_provider(ConnectorScanNode* scan_node,
                                                      const TPlanNode& plan_node) const override;

    ConnectorType connector_type() const override { return ConnectorType::FLIGHT; }
};

class FlightDataSource;

class FlightDataSourceProvider final : public DataSourceProvider {
public:
    ~FlightDataSourceProvider() override = default;
    friend class FlightDataSource;
    FlightDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node);
    DataSourcePtr create_data_source(const TScanRange& scan_range) override;
    const TupleDescriptor* tuple_descriptor(RuntimeState* state) const override;

protected:
    ConnectorScanNode* _scan_node;
    const TFlightScanNode _flight_scan_node;
};

// Reads one Flight endpoint: redeems the frontend-planned ticket and streams Arrow batches into chunks.
class FlightDataSource final : public DataSource {
public:
    ~FlightDataSource() override = default;

    FlightDataSource(const FlightDataSourceProvider* provider, const TScanRange& scan_range);
    std::string name() const override;
    Status open(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk) override;

    int64_t raw_rows_read() const override;
    int64_t num_rows_read() const override;
    int64_t num_bytes_read() const override;
    int64_t cpu_time_spent() const override;

private:
    // Splits a converted batch into chunks of at most the runtime's chunk size.
    void _enqueue(const ChunkPtr& chunk, size_t chunk_size);

    const FlightDataSourceProvider* _provider;
    const TFlightScanRange _scan_range;

    std::unique_ptr<FlightScanReader> _reader;
    std::unique_ptr<FlightTicketStream> _stream;
    std::deque<ChunkPtr> _pending;
    bool _eos = false;
    int64_t _rows_read = 0;
    int64_t _bytes_read = 0;
    int64_t _cpu_time_ns = 0;
};

} // namespace starrocks::connector
