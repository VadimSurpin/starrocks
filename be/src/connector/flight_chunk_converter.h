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

#include <arrow/array.h>
#include <arrow/record_batch.h>

#include <string>

#include "column/column.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "runtime/types.h"

namespace starrocks {
class TupleDescriptor;
class RuntimeState;
} // namespace starrocks

namespace starrocks::connector {

// Converts one Arrow array into a column of the given type. There is no silent cast: when no converter exists for
// the (Arrow type, StarRocks type) pair, or a value cannot be represented, the result is an error naming the column.
StatusOr<ColumnPtr> convert_flight_array(const std::string& column_name, const arrow::Array& array,
                                         const TypeDescriptor& type, bool nullable);

// Builds one chunk holding the tuple's materialized slots from a batch. Arrow columns are matched to slots by name,
// falling back to a case-insensitive match.
StatusOr<ChunkPtr> convert_flight_batch(const arrow::RecordBatch& batch, const TupleDescriptor& tuple_desc);

} // namespace starrocks::connector
