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

#include <arrow/flight/client.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include <memory>
#include <string>

namespace starrocks::connector {

// Pull-style stream over one redeemed Flight ticket.
class FlightTicketStream {
public:
    explicit FlightTicketStream(std::unique_ptr<arrow::flight::FlightStreamReader> reader);

    // Returns the next batch, or a null batch at the end of the stream.
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> Next();

    // Cancels the server-side stream; safe to call more than once.
    void Cancel();

private:
    std::unique_ptr<arrow::flight::FlightStreamReader> _reader;
};

// Redeems tickets planned by the frontend. Vendored from the arrow-flight-server repository
// (flight-starrocks/cpp); keep the two in sync.
class FlightScanReader {
public:
    // bearer_token may be empty. Neither it nor ticket bytes ever appear in returned errors.
    static arrow::Result<std::unique_ptr<FlightScanReader>> Connect(const arrow::flight::Location& location,
                                                                     const std::string& bearer_token);

    arrow::Result<std::unique_ptr<FlightTicketStream>> OpenTicket(const arrow::flight::Ticket& ticket);

private:
    FlightScanReader(arrow::flight::Location location, std::unique_ptr<arrow::flight::FlightClient> client,
                     arrow::flight::FlightCallOptions options);

    arrow::flight::Location _location;
    std::unique_ptr<arrow::flight::FlightClient> _client;
    arrow::flight::FlightCallOptions _options;
};

} // namespace starrocks::connector
