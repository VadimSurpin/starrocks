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

#include <cstdint>
#include <functional>
#include <cstddef>
#include <string>
#include <vector>

#include "common/global_types.h"

namespace starrocks {
class ExprContext;
}

namespace starrocks::connector {

// A runtime IN-filter a hash join built from its build side: `column IN (values)`.
struct FlightInFilter {
    std::string column;
    std::vector<int64_t> values;
};

// Picks the filters that can be sent to a Flight server out of a scan's conjuncts: a join-built positive IN on one
// integer column without NULL in the set. `column_of` names the table column of a slot (empty when it has none).
// Everything else is left out; StarRocks still applies every conjunct to the rows it receives, so leaving a
// filter out only costs speed.
std::vector<FlightInFilter> collect_flight_in_filters(const std::vector<ExprContext*>& conjuncts,
                                                      const std::function<std::string(SlotId)>& column_of);

// Most values the server accepts across the filters of one request; filters past this are left out.
constexpr size_t kFlightMaxFilterValues = 1'000'000;

// The filter block a Flight server reads: little-endian magic "ARF1", filter count, then per filter the column
// name (length, bytes) and its values (count, int64 each). Filters beyond kFlightMaxFilterValues values in total
// are dropped. Returns an empty string when nothing is left to send.
std::string encode_flight_filter_block(const std::vector<FlightInFilter>& filters);

// A Flight ticket is a protobuf message; field 15, which it does not use, carries the filter block, and
// appending a field is plain protobuf concatenation, so the signed ticket inside is untouched.
std::string attach_flight_filter_block(const std::string& ticket, const std::string& block);

} // namespace starrocks::connector
