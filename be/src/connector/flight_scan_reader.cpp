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

#include "connector/flight_scan_reader.h"

#include <utility>

namespace starrocks::connector {

FlightTicketStream::FlightTicketStream(std::unique_ptr<arrow::flight::FlightStreamReader> reader)
        : _reader(std::move(reader)) {}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> FlightTicketStream::Next() {
    while (true) {
        auto chunk = _reader->Next();
        if (!chunk.ok()) {
            return arrow::Status::IOError("Flight stream failed: ", chunk.status().message());
        }
        if (!chunk->data && !chunk->app_metadata) {
            return std::shared_ptr<arrow::RecordBatch>();
        }
        if (chunk->data) {
            return chunk->data;
        }
    }
}

void FlightTicketStream::Cancel() {
    _reader->Cancel();
}

FlightScanReader::FlightScanReader(arrow::flight::Location location, std::unique_ptr<arrow::flight::FlightClient> client,
                                   arrow::flight::FlightCallOptions options)
        : _location(std::move(location)), _client(std::move(client)), _options(std::move(options)) {}

arrow::Result<std::unique_ptr<FlightScanReader>> FlightScanReader::Connect(const arrow::flight::Location& location,
                                                                           const std::string& bearer_token) {
    ARROW_ASSIGN_OR_RAISE(auto client, arrow::flight::FlightClient::Connect(location));
    arrow::flight::FlightCallOptions options;
    if (!bearer_token.empty()) {
        options.headers.emplace_back("authorization", "Bearer " + bearer_token);
    }
    return std::unique_ptr<FlightScanReader>(new FlightScanReader(location, std::move(client), std::move(options)));
}

arrow::Result<std::unique_ptr<FlightTicketStream>> FlightScanReader::OpenTicket(const arrow::flight::Ticket& ticket) {
    auto reader = _client->DoGet(_options, ticket);
    if (!reader.ok()) {
        return arrow::Status::IOError("Flight DoGet failed at ", _location.ToString(), ": ",
                                      reader.status().message());
    }
    return std::make_unique<FlightTicketStream>(std::move(*reader));
}

} // namespace starrocks::connector
