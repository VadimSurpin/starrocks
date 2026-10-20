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

#include <algorithm>
#include <cctype>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/decimalv3_column.h"
#include "column/nullable_column.h"
#include "exec/arrow_to_starrocks_converter.h"
#include "fmt/format.h"
#include "runtime/descriptors.h"

namespace starrocks::connector {

// The server's narrow wire form: a DECIMAL(p, s) column arrives as its unscaled value in an INT32/INT64 array. The
// slot's declared type gives the scale, so the values are copied (widened when needed) after a range check.
static bool is_unscaled_decimal(const arrow::Array& array, const TypeDescriptor& type) {
    const bool integer = array.type_id() == arrow::Type::INT64 || array.type_id() == arrow::Type::INT32;
    return integer && (type.type == TYPE_DECIMAL32 || type.type == TYPE_DECIMAL64 || type.type == TYPE_DECIMAL128);
}

template <typename CppType, typename ArrowArray>
static Status copy_unscaled(const std::string& column_name, const ArrowArray& array, const TypeDescriptor& type,
                            Column* data_column) {
    const size_t num_rows = array.length();
    auto* decimal_column = down_cast<DecimalV3Column<CppType>*>(data_column);
    auto& data = decimal_column->get_data();
    data.resize(num_rows);
    int128_t limit = 1;
    for (int i = 0; i < type.precision; ++i) {
        limit *= 10;
    }
    const auto* values = array.raw_values();
    for (size_t i = 0; i < num_rows; ++i) {
        const int128_t value = array.IsNull(i) ? 0 : static_cast<int128_t>(values[i]);
        if (value >= limit || value <= -limit) {
            return Status::DataQualityError(fmt::format(
                    "Flight column '{}' has the unscaled value {} which does not fit {}", column_name,
                    static_cast<int64_t>(value), type.debug_string()));
        }
        data[i] = static_cast<CppType>(value);
    }
    return Status::OK();
}

static Status copy_unscaled_decimal(const std::string& column_name, const arrow::Array& array,
                                    const TypeDescriptor& type, Column* data_column) {
    const bool wide = array.type_id() == arrow::Type::INT64;
    switch (type.type) {
    case TYPE_DECIMAL32:
        return wide ? copy_unscaled<int32_t>(column_name, static_cast<const arrow::Int64Array&>(array), type,
                                             data_column)
                    : copy_unscaled<int32_t>(column_name, static_cast<const arrow::Int32Array&>(array), type,
                                             data_column);
    case TYPE_DECIMAL64:
        return wide ? copy_unscaled<int64_t>(column_name, static_cast<const arrow::Int64Array&>(array), type,
                                             data_column)
                    : copy_unscaled<int64_t>(column_name, static_cast<const arrow::Int32Array&>(array), type,
                                             data_column);
    default:
        return wide ? copy_unscaled<int128_t>(column_name, static_cast<const arrow::Int64Array&>(array), type,
                                              data_column)
                    : copy_unscaled<int128_t>(column_name, static_cast<const arrow::Int32Array&>(array), type,
                                              data_column);
    }
}

StatusOr<ColumnPtr> convert_flight_array(const std::string& column_name, const arrow::Array& array,
                                         const TypeDescriptor& type, bool nullable) {
    if (!nullable && array.null_count() > 0) {
        return Status::DataQualityError(
                fmt::format("Flight column '{}' contains NULL but the StarRocks column is not nullable", column_name));
    }
    if (is_unscaled_decimal(array, type)) {
        ColumnPtr column = ColumnHelper::create_column(type, nullable);
        Column* data_column = column.get();
        if (nullable) {
            auto* nullable_column = down_cast<NullableColumn*>(column.get());
            NullColumn* null_column = nullable_column->mutable_null_column();
            size_t null_count = fill_null_column(&array, 0, array.length(), null_column, 0);
            nullable_column->set_has_null(null_count != 0);
            data_column = nullable_column->data_column().get();
        }
        RETURN_IF_ERROR(copy_unscaled_decimal(column_name, array, type, data_column));
        if (nullable) {
            down_cast<NullableColumn*>(column.get())->update_has_null();
        }
        return column;
    }
    ConvertFunc func = get_arrow_converter(array.type_id(), type.type, nullable, /*is_strict=*/true);
    if (func == nullptr) {
        return Status::NotSupported(fmt::format("Flight column '{}' has Arrow type {} which cannot be read as {}",
                                                column_name, array.type()->ToString(), type.debug_string()));
    }

    const size_t num_rows = array.length();
    ColumnPtr column = ColumnHelper::create_column(type, nullable);
    // The converters read the declared length limit from the slot, so they need one even without a tuple.
    SlotDescriptor slot(SlotId(0), column_name, type);
    ArrowConvertContext ctx{nullptr, &slot, column_name};
    // Filter semantics follow the load path: 1 keeps a row, 0 marks one that could not be represented.
    Filter filter(num_rows, 1);

    Column* data_column = column.get();
    uint8_t* null_data = nullptr;
    if (nullable) {
        auto* nullable_column = down_cast<NullableColumn*>(column.get());
        NullColumn* null_column = nullable_column->mutable_null_column();
        size_t null_count = fill_null_column(&array, 0, num_rows, null_column, 0);
        nullable_column->set_has_null(null_count != 0);
        null_data = num_rows == 0 ? nullptr : &null_column->get_data().front();
        data_column = nullable_column->data_column().get();
    }

    RETURN_IF_ERROR(func(&array, 0, num_rows, data_column, 0, null_data, &filter, &ctx, nullptr));
    // In strict mode a value that cannot be represented is marked in the filter instead of being nulled.
    if (std::any_of(filter.begin(), filter.end(), [](uint8_t keep) { return keep == 0; })) {
        return Status::DataQualityError(fmt::format(
                "Flight column '{}' has values that cannot be represented as {}", column_name, type.debug_string()));
    }
    if (nullable) {
        down_cast<NullableColumn*>(column.get())->update_has_null();
    }
    return column;
}

static std::shared_ptr<arrow::Array> find_column(const arrow::RecordBatch& batch, const std::string& name) {
    if (auto exact = batch.GetColumnByName(name)) {
        return exact;
    }
    auto lower = [](std::string s) {
        std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });
        return s;
    };
    const std::string wanted = lower(name);
    for (int i = 0; i < batch.num_columns(); ++i) {
        if (lower(batch.column_name(i)) == wanted) {
            return batch.column(i);
        }
    }
    return nullptr;
}

StatusOr<ChunkPtr> convert_flight_batch(const arrow::RecordBatch& batch, const TupleDescriptor& tuple_desc) {
    auto chunk = std::make_shared<Chunk>();
    for (const SlotDescriptor* slot : tuple_desc.slots()) {
        if (!slot->is_materialized()) {
            continue;
        }
        auto array = find_column(batch, slot->col_name());
        if (array == nullptr) {
            return Status::InternalError(fmt::format("Flight result has no column '{}'", slot->col_name()));
        }
        ASSIGN_OR_RETURN(ColumnPtr column,
                         convert_flight_array(slot->col_name(), *array, slot->type(), slot->is_nullable()));
        chunk->append_column(std::move(column), slot->id());
    }
    return chunk;
}

} // namespace starrocks::connector
