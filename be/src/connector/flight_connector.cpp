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

#include "connector/flight_connector.h"

#include <arrow/flight/types.h>

#include "column/chunk.h"
#include "connector/flight_chunk_converter.h"
#include "exec/exec_node.h"
#include "fmt/format.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/stopwatch.hpp"

namespace starrocks::connector {

DataSourceProviderPtr FlightConnector::create_data_source_provider(ConnectorScanNode* scan_node,
                                                                   const TPlanNode& plan_node) const {
    return std::make_unique<FlightDataSourceProvider>(scan_node, plan_node);
}

// ================================

FlightDataSourceProvider::FlightDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node)
        : _scan_node(scan_node), _flight_scan_node(plan_node.flight_scan_node) {}

DataSourcePtr FlightDataSourceProvider::create_data_source(const TScanRange& scan_range) {
    return std::make_unique<FlightDataSource>(this, scan_range);
}

const TupleDescriptor* FlightDataSourceProvider::tuple_descriptor(RuntimeState* state) const {
    return state->desc_tbl().get_tuple_descriptor(_flight_scan_node.tuple_id);
}

// ================================

FlightDataSource::FlightDataSource(const FlightDataSourceProvider* provider, const TScanRange& scan_range)
        : _provider(provider), _scan_range(scan_range.flight_scan_range) {}

std::string FlightDataSource::name() const {
    return "FlightDataSource";
}

Status FlightDataSource::open(RuntimeState* state) {
    const TFlightScanNode& node = _provider->_flight_scan_node;
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(node.tuple_id);

    // An endpoint without a location, or one that asks to reuse the connection, is served by the
    // Flight server the catalog was created with.
    std::string uri = node.flight_uri;
    if (!_scan_range.locations.empty() &&
        _scan_range.locations.front() != arrow::flight::Location::ReuseConnection().ToString()) {
        uri = _scan_range.locations.front();
    }
    auto location = arrow::flight::Location::Parse(uri);
    if (!location.ok()) {
        return Status::InvalidArgument(fmt::format("Invalid Flight location '{}': {}", uri, location.status().message()));
    }

    // The token is only ever handed to the Flight client; it is not part of any message built here.
    auto reader = FlightScanReader::Connect(*location, node.__isset.auth_token ? node.auth_token : std::string());
    if (!reader.ok()) {
        return Status::InternalError(fmt::format("Cannot connect to Flight endpoint {}: {}", uri, reader.status().message()));
    }
    _reader = std::move(*reader);
    auto stream = _reader->OpenTicket(arrow::flight::Ticket{_scan_range.ticket});
    if (!stream.ok()) {
        return Status::InternalError(fmt::format("Cannot open Flight stream at {}: {}", uri, stream.status().message()));
    }
    _stream = std::move(*stream);
    return Status::OK();
}

void FlightDataSource::close(RuntimeState* state) {
    if (_stream != nullptr) {
        _stream->Cancel();
        _stream.reset();
    }
    _reader.reset();
}

void FlightDataSource::_enqueue(const ChunkPtr& chunk, size_t chunk_size) {
    const size_t rows = chunk->num_rows();
    if (rows <= chunk_size) {
        _pending.push_back(chunk);
        return;
    }
    for (size_t offset = 0; offset < rows; offset += chunk_size) {
        ChunkPtr part = chunk->clone_empty(chunk_size);
        part->append(*chunk, offset, std::min(chunk_size, rows - offset));
        _pending.push_back(std::move(part));
    }
}

Status FlightDataSource::get_next(RuntimeState* state, ChunkPtr* chunk) {
    SCOPED_RAW_TIMER(&_cpu_time_ns);
    while (_pending.empty()) {
        if (_eos) {
            return Status::EndOfFile("");
        }
        auto batch = _stream->Next();
        if (!batch.ok()) {
            return Status::InternalError(batch.status().message());
        }
        if (*batch == nullptr) {
            _eos = true;
            continue;
        }
        if ((*batch)->num_rows() == 0) {
            continue;
        }
        ASSIGN_OR_RETURN(ChunkPtr converted, convert_flight_batch(**batch, *_tuple_desc));
        _enqueue(converted, state->chunk_size());
    }
    *chunk = std::move(_pending.front());
    _pending.pop_front();

    // Predicates the frontend could not push down are still evaluated here; pushed ones were already
    // applied by the Flight server and removed from the plan.
    if (!_conjunct_ctxs.empty()) {
        RETURN_IF_ERROR(ExecNode::eval_conjuncts(_conjunct_ctxs, chunk->get()));
    }
    _rows_read += (*chunk)->num_rows();
    _bytes_read += (*chunk)->bytes_usage();
    return Status::OK();
}

int64_t FlightDataSource::raw_rows_read() const {
    return _rows_read;
}
int64_t FlightDataSource::num_rows_read() const {
    return _rows_read;
}
int64_t FlightDataSource::num_bytes_read() const {
    return _bytes_read;
}
int64_t FlightDataSource::cpu_time_spent() const {
    return _cpu_time_ns;
}

} // namespace starrocks::connector
