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
#include "column/nullable_column.h"
#include "exec/arrow_to_starrocks_converter.h"
#include "fmt/format.h"
#include "runtime/descriptors.h"

namespace starrocks::connector {

StatusOr<ColumnPtr> convert_flight_array(const std::string& column_name, const arrow::Array& array,
                                         const TypeDescriptor& type, bool nullable) {
    if (!nullable && array.null_count() > 0) {
        return Status::DataQualityError(
                fmt::format("Flight column '{}' contains NULL but the StarRocks column is not nullable", column_name));
    }
    ConvertFunc func = get_arrow_converter(array.type_id(), type.type, nullable, /*is_strict=*/true);
    if (func == nullptr) {
        return Status::NotSupported(fmt::format("Flight column '{}' has Arrow type {} which cannot be read as {}",
                                                column_name, array.type()->ToString(), type.debug_string()));
    }

    const size_t num_rows = array.length();
    ColumnPtr column = ColumnHelper::create_column(type, nullable);
    Filter filter(num_rows, 0);
    ArrowConvertContext ctx{nullptr, nullptr, column_name};

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
    if (std::any_of(filter.begin(), filter.end(), [](uint8_t flagged) { return flagged != 0; })) {
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
