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
class RuntimeFilterProbeCollector;
class SimdBlockFilter;
} // namespace starrocks

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

// A runtime bloom filter a hash join built: a split-block bloom filter over the integer join key. A join that was
// partitioned has one filter per hash partition; a row can pass any of them, and the server tests all.
struct FlightBloomFilter {
    // A key and whether the filter accepts it. The server tests its own implementation of the filter against
    // these answers and does not use the filter when one differs, so a server that hashes differently can only
    // lose speed, never rows.
    struct Witness {
        int64_t key;
        bool accepted;
    };

    std::string column;
    std::vector<const SimdBlockFilter*> partitions;
    std::vector<Witness> witnesses;
};

// Picks the bloom filters of a scan's runtime filters that a Flight server can apply: a join-built bloom filter
// on one integer column that holds no NULL and is still usable. `driver_sequence` selects the filter of a
// colocate join. Everything else is left out; StarRocks still applies every filter to the rows it receives.
std::vector<FlightBloomFilter> collect_flight_bloom_filters(const RuntimeFilterProbeCollector& collector,
                                                            int32_t driver_sequence,
                                                            const std::function<std::string(SlotId)>& column_of);

// Most values the server accepts across the filters of one request; filters past this are left out.
constexpr size_t kFlightMaxFilterValues = 1'000'000;

// The filter block a Flight server reads: little-endian magic "ARF1", filter count, then per filter the column
// name (length, bytes) and its values (count, int64 each). Filters beyond kFlightMaxFilterValues values in total
// are dropped. Returns an empty string when nothing is left to send.
std::string encode_flight_filter_block(const std::vector<FlightInFilter>& filters);

// The same block extended with bloom filters, magic "ARF2": the IN-filters exactly as above, then the bloom
// filter count and per filter the column name, the partition count and per partition the log2 of its bucket
// count (int32), the directory length (uint64) and the directory itself, then the witness count and per witness
// the key (int64) and its answer (uint8). A filter whose directories would take
// the total past `max_bloom_bytes` is dropped. Returns an empty string when nothing is left to send, and the
// "ARF1" block when only IN-filters are left.
std::string encode_flight_filter_block(const std::vector<FlightInFilter>& filters,
                                       const std::vector<FlightBloomFilter>& blooms, size_t max_bloom_bytes);

// A Flight ticket is a protobuf message; field 15, which it does not use, carries the filter block, and
// appending a field is plain protobuf concatenation, so the signed ticket inside is untouched.
std::string attach_flight_filter_block(const std::string& ticket, const std::string& block);

} // namespace starrocks::connector
