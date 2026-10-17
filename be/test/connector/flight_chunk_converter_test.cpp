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

#include "connector/flight_chunk_converter.h"

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "runtime/types.h"
#include "types/date_value.hpp"

namespace starrocks::connector {

class FlightChunkConverterTest : public ::testing::Test {
public:
    void SetUp() override { date::init_date_cache(); }
};

static std::shared_ptr<arrow::Array> int64_array(const std::vector<int64_t>& values) {
    arrow::Int64Builder builder;
    EXPECT_TRUE(builder.AppendValues(values).ok());
    return builder.Finish().ValueOrDie();
}

TEST_F(FlightChunkConverterTest, int64_into_bigint) {
    auto array = int64_array({1, 2, 3});
    auto result = convert_flight_array("id", *array, TypeDescriptor(TYPE_BIGINT), false);
    ASSERT_TRUE(result.ok()) << result.status().to_string();
    ColumnPtr column = result.value();
    ASSERT_EQ(3, column->size());
    EXPECT_EQ(2, column->get(1).get_int64());
}

TEST_F(FlightChunkConverterTest, strings_with_nulls_into_nullable_varchar) {
    arrow::StringBuilder builder;
    ASSERT_TRUE(builder.Append("a").ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    ASSERT_TRUE(builder.Append("ccc").ok());
    auto array = builder.Finish().ValueOrDie();

    auto result = convert_flight_array("name", *array, TypeDescriptor::create_varchar_type(1024), true);
    ASSERT_TRUE(result.ok()) << result.status().to_string();
    ColumnPtr column = result.value();
    ASSERT_EQ(3, column->size());
    EXPECT_FALSE(column->is_null(0));
    EXPECT_TRUE(column->is_null(1));
    EXPECT_EQ("ccc", column->get(2).get_slice().to_string());
}

TEST_F(FlightChunkConverterTest, date32_and_timestamp) {
    arrow::Date32Builder dates;
    ASSERT_TRUE(dates.Append(0).ok()); // 1970-01-01
    auto date_array = dates.Finish().ValueOrDie();
    auto date_result = convert_flight_array("d", *date_array, TypeDescriptor(TYPE_DATE), true);
    ASSERT_TRUE(date_result.ok()) << date_result.status().to_string();
    EXPECT_EQ("1970-01-01", date_result.value()->get(0).get_date().to_string());

    arrow::TimestampBuilder stamps(arrow::timestamp(arrow::TimeUnit::MICRO), arrow::default_memory_pool());
    ASSERT_TRUE(stamps.Append(1'000'000).ok()); // 1970-01-01 00:00:01
    auto ts_array = stamps.Finish().ValueOrDie();
    auto ts_result = convert_flight_array("t", *ts_array, TypeDescriptor(TYPE_DATETIME), true);
    ASSERT_TRUE(ts_result.ok()) << ts_result.status().to_string();
    EXPECT_EQ("1970-01-01 00:00:01", ts_result.value()->get(0).get_timestamp().to_string());
}

TEST_F(FlightChunkConverterTest, decimal128_into_decimal64) {
    arrow::Decimal128Builder builder(arrow::decimal128(15, 2));
    ASSERT_TRUE(builder.Append(arrow::Decimal128(12345)).ok()); // 123.45
    auto array = builder.Finish().ValueOrDie();
    auto result = convert_flight_array("x", *array, TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 15, 2), true);
    ASSERT_TRUE(result.ok()) << result.status().to_string();
    EXPECT_EQ(12345, result.value()->get(0).get_int64());
}

TEST_F(FlightChunkConverterTest, type_mismatch_is_an_error_naming_the_column) {
    arrow::StringBuilder builder;
    ASSERT_TRUE(builder.Append("not a number").ok());
    auto array = builder.Finish().ValueOrDie();
    auto result = convert_flight_array("weird_col", *array, TypeDescriptor(TYPE_BIGINT), true);
    ASSERT_FALSE(result.ok());
    EXPECT_NE(std::string::npos, result.status().message().find("weird_col")) << result.status().message();
}

TEST_F(FlightChunkConverterTest, null_in_non_nullable_column_is_an_error) {
    arrow::Int64Builder builder;
    ASSERT_TRUE(builder.Append(1).ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    auto array = builder.Finish().ValueOrDie();
    auto result = convert_flight_array("id", *array, TypeDescriptor(TYPE_BIGINT), false);
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_data_quality_error()) << result.status().to_string();
}

TEST_F(FlightChunkConverterTest, empty_array_gives_empty_column) {
    auto array = int64_array({});
    auto result = convert_flight_array("id", *array, TypeDescriptor(TYPE_BIGINT), true);
    ASSERT_TRUE(result.ok()) << result.status().to_string();
    EXPECT_EQ(0, result.value()->size());
}

} // namespace starrocks::connector
