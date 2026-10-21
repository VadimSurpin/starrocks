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

#include "exprs/column_ref.h"
#include "exprs/in_const_predicate.hpp"
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

} // namespace starrocks::connector
