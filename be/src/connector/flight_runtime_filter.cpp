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

#include "column/column_viewer.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "exprs/in_const_predicate.hpp"
#include "gutil/casts.h"

namespace starrocks::connector {

namespace {

template <LogicalType Type>
bool collect_values(Expr* root, std::vector<int64_t>* values) {
    auto* predicate = down_cast<VectorizedInConstPredicate<Type>*>(root);
    // Only filters a join built: other IN lists are query predicates the server already applies.
    if (!predicate->is_join_runtime_filter() || predicate->is_not_in() || predicate->null_in_set()) {
        return false;
    }
    ColumnPtr all = predicate->get_all_values();
    ColumnViewer<Type> viewer(all);
    values->reserve(viewer.size());
    for (size_t i = 0; i < viewer.size(); ++i) {
        if (viewer.is_null(i)) {
            return false;
        }
        values->push_back(static_cast<int64_t>(viewer.value(i)));
    }
    return true;
}

} // namespace

std::vector<FlightInFilter> collect_flight_in_filters(const std::vector<ExprContext*>& conjuncts,
                                                      const std::function<std::string(SlotId)>& column_of) {
    std::vector<FlightInFilter> filters;
    for (ExprContext* ctx : conjuncts) {
        Expr* root = ctx->root();
        if (root->node_type() != TExprNodeType::IN_PRED || root->get_num_children() < 1) {
            continue;
        }
        Expr* key = root->get_child(0);
        if (!key->is_slotref()) {
            continue;
        }
        std::string column = column_of(down_cast<ColumnRef*>(key)->slot_id());
        if (column.empty()) {
            continue;
        }
        std::vector<int64_t> values;
        bool ok = false;
        switch (key->type().type) {
        case TYPE_TINYINT:
            ok = collect_values<TYPE_TINYINT>(root, &values);
            break;
        case TYPE_SMALLINT:
            ok = collect_values<TYPE_SMALLINT>(root, &values);
            break;
        case TYPE_INT:
            ok = collect_values<TYPE_INT>(root, &values);
            break;
        case TYPE_BIGINT:
            ok = collect_values<TYPE_BIGINT>(root, &values);
            break;
        default:
            break;
        }
        if (ok) {
            filters.push_back(FlightInFilter{std::move(column), std::move(values)});
        }
    }
    return filters;
}

} // namespace starrocks::connector
