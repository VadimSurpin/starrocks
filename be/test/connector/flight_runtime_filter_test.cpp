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


#include "connector/flight_runtime_filter.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>

#include "exprs/column_ref.h"
#include "exprs/in_const_predicate.hpp"
#include "exprs/runtime_filter.h"
#include "runtime/runtime_state.h"
#include "testutil/column_test_helper.h"

namespace starrocks::connector {

class FlightRuntimeFilterTest : public ::testing::Test {
protected:
    template <typename T>
    ExprContext* in_filter(const TypeDescriptor& type, SlotId slot, const std::vector<T>& values, bool not_in = false,
                           bool null_in_set = false) {
        ColumnRef* key = _pool.add(new ColumnRef(type, slot));
        VectorizedInConstPredicateBuilder builder(&_state, &_pool, key);
        builder.set_is_not_in(not_in);
        builder.set_null_in_set(null_in_set);
        builder.use_as_join_runtime_filter();
        EXPECT_TRUE(builder.create().ok());
        builder.add_values(ColumnTestHelper::build_column(values), 0);
        return builder.get_in_const_predicate();
    }

    static std::string name_of(SlotId slot) { return slot == 1 ? "l_partkey" : slot == 2 ? "l_suppkey" : ""; }

    ObjectPool _pool;
    RuntimeState _state;
};

TEST_F(FlightRuntimeFilterTest, collects_a_bigint_in_filter_with_its_column_name) {
    auto filters = collect_flight_in_filters({in_filter<int64_t>(TypeDescriptor(TYPE_BIGINT), 1, {7, 3, 5})},
                                             name_of);
    ASSERT_EQ(1, filters.size());
    EXPECT_EQ("l_partkey", filters[0].column);
    std::sort(filters[0].values.begin(), filters[0].values.end());
    EXPECT_EQ((std::vector<int64_t>{3, 5, 7}), filters[0].values);
}

TEST_F(FlightRuntimeFilterTest, widens_int_values_to_int64) {
    auto filters = collect_flight_in_filters({in_filter<int32_t>(TypeDescriptor(TYPE_INT), 2, {-1, 4})}, name_of);
    ASSERT_EQ(1, filters.size());
    EXPECT_EQ("l_suppkey", filters[0].column);
    std::sort(filters[0].values.begin(), filters[0].values.end());
    EXPECT_EQ((std::vector<int64_t>{-1, 4}), filters[0].values);
}

TEST_F(FlightRuntimeFilterTest, skips_filters_the_server_cannot_apply) {
    std::vector<ExprContext*> conjuncts{
            in_filter<int64_t>(TypeDescriptor(TYPE_BIGINT), 1, {1, 2}, /*not_in=*/true),
            in_filter<int64_t>(TypeDescriptor(TYPE_BIGINT), 1, {1, 2}, false, /*null_in_set=*/true),
            in_filter<int64_t>(TypeDescriptor(TYPE_BIGINT), 99, {1, 2}),
    };
    EXPECT_TRUE(collect_flight_in_filters(conjuncts, name_of).empty());
}

static uint32_t read_u32(const std::string& s, size_t offset) {
    uint32_t v;
    memcpy(&v, s.data() + offset, sizeof(v));
    return v;
}

TEST_F(FlightRuntimeFilterTest, filter_block_layout_matches_what_the_server_reads) {
    std::string block = encode_flight_filter_block({FlightInFilter{"k", {5, -1}}});
    ASSERT_EQ(4 + 4 + 4 + 1 + 4 + 16, block.size());
    EXPECT_EQ(0x41524631u, read_u32(block, 0));
    EXPECT_EQ(1u, read_u32(block, 4));
    EXPECT_EQ(1u, read_u32(block, 8));
    EXPECT_EQ('k', block[12]);
    EXPECT_EQ(2u, read_u32(block, 13));
    int64_t first;
    int64_t second;
    memcpy(&first, block.data() + 17, 8);
    memcpy(&second, block.data() + 25, 8);
    EXPECT_EQ(5, first);
    EXPECT_EQ(-1, second);
}

TEST_F(FlightRuntimeFilterTest, filters_beyond_the_value_limit_are_left_out) {
    FlightInFilter big{"big", std::vector<int64_t>(kFlightMaxFilterValues + 1, 1)};
    FlightInFilter small{"small", {1, 2, 3}};
    std::string block = encode_flight_filter_block({big, small});
    EXPECT_EQ(1u, read_u32(block, 4));
    EXPECT_EQ('s', block[12]);
    EXPECT_TRUE(encode_flight_filter_block({big}).empty());
    EXPECT_TRUE(encode_flight_filter_block({}).empty());
}

TEST_F(FlightRuntimeFilterTest, block_is_appended_to_the_ticket_as_protobuf_field_15) {
    std::string ticket = "ticket-bytes";
    std::string block(200, 'x');
    std::string result = attach_flight_filter_block(ticket, block);
    ASSERT_EQ(ticket.size() + 1 + 2 + block.size(), result.size());
    EXPECT_EQ(ticket, result.substr(0, ticket.size()));
    EXPECT_EQ(static_cast<char>(0x7A), result[ticket.size()]);
    // 200 as a protobuf varint
    EXPECT_EQ(static_cast<char>(0xC8), result[ticket.size() + 1]);
    EXPECT_EQ(static_cast<char>(0x01), result[ticket.size() + 2]);
    EXPECT_EQ(block, result.substr(ticket.size() + 3));
    EXPECT_EQ(ticket, attach_flight_filter_block(ticket, ""));
}

TEST_F(FlightRuntimeFilterTest, bloom_filters_are_sent_as_raw_directories_with_witnesses) {
    SimdBlockFilter bloom;
    bloom.init(1000); // 2^5 buckets of 32 bytes
    bloom.insert_hash(12345);
    FlightBloomFilter filter{"l_orderkey", {&bloom}, {{7, true}, {-8, false}}};

    std::string block = encode_flight_filter_block({}, {filter}, 1 << 20);

    ASSERT_FALSE(block.empty());
    EXPECT_EQ(0x41524632u, read_u32(block, 0)); // "ARF2"
    EXPECT_EQ(0u, read_u32(block, 4));          // no IN-filters
    EXPECT_EQ(1u, read_u32(block, 8));          // one bloom filter
    EXPECT_EQ(10u, read_u32(block, 12));        // name length
    EXPECT_EQ("l_orderkey", block.substr(16, 10));
    EXPECT_EQ(1u, read_u32(block, 26));         // partitions
    EXPECT_EQ(5u, read_u32(block, 30));         // log2 of the bucket count
    uint64_t length = 0;
    memcpy(&length, block.data() + 34, 8);
    EXPECT_EQ(1024u, length);
    EXPECT_EQ(0, memcmp(bloom.directory(), block.data() + 42, 1024));
    EXPECT_EQ(2u, read_u32(block, 42 + 1024)); // witnesses
    int64_t key = 0;
    memcpy(&key, block.data() + 42 + 1024 + 4, 8);
    EXPECT_EQ(7, key);
    EXPECT_EQ(1, block[42 + 1024 + 4 + 8]);
    EXPECT_EQ(42u + 1024 + 4 + 2 * 9, block.size());
}

TEST_F(FlightRuntimeFilterTest, a_bloom_filter_over_the_byte_budget_is_left_out) {
    SimdBlockFilter bloom;
    bloom.init(1000);
    FlightBloomFilter filter{"k", {&bloom}, {{1, true}}};
    EXPECT_TRUE(encode_flight_filter_block({}, {filter}, 512).empty());

    // the IN-filters still go, in the older block
    std::string block = encode_flight_filter_block({FlightInFilter{"a", {1}}}, {filter}, 512);
    EXPECT_EQ(0x41524631u, read_u32(block, 0));
}

TEST_F(FlightRuntimeFilterTest, a_partitioned_bloom_filter_sends_every_partition) {
    SimdBlockFilter first;
    SimdBlockFilter second;
    first.init(100);
    second.init(100);
    FlightBloomFilter filter{"k", {&first, &second}, {{1, false}}};

    std::string block = encode_flight_filter_block({}, {filter}, 1 << 20);

    EXPECT_EQ(2u, read_u32(block, 8 + 4 + 1 + 4)); // after the count, name length and name
}

} // namespace starrocks::connector
