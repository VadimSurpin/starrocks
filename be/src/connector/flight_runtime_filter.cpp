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
#include "exprs/runtime_filter.h"
#include "exprs/runtime_filter_bank.h"
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

namespace {

constexpr size_t kWitnessesPerAnswer = 32;
constexpr size_t kWitnessCandidates = 200'000;

uint64_t splitmix64(uint64_t* state) {
    uint64_t z = (*state += 0x9e3779b97f4a7c15ULL);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}

// Up to kWitnessesPerAnswer keys the filter accepts and as many it rejects, from a fixed pseudo-random sequence
// over the whole range of the key type, so negative keys are covered.
template <LogicalType Type>
std::vector<FlightBloomFilter::Witness> pick_witnesses(const TRuntimeBloomFilter<Type>& bloom) {
    using CppType = RunTimeCppType<Type>;
    std::vector<FlightBloomFilter::Witness> witnesses;
    size_t accepted = 0;
    size_t rejected = 0;
    uint64_t state = 42;
    for (size_t i = 0; i < kWitnessCandidates && (accepted < kWitnessesPerAnswer || rejected < kWitnessesPerAnswer);
         ++i) {
        const CppType key = static_cast<CppType>(splitmix64(&state));
        const size_t hash = bloom.compute_hash(key);
        bool answer = false;
        if (bloom.partition_blooms().empty()) {
            answer = bloom.bloom().test_hash(hash);
        } else {
            for (const SimdBlockFilter& partition : bloom.partition_blooms()) {
                answer = answer || partition.test_hash(hash);
            }
        }
        size_t& count = answer ? accepted : rejected;
        if (count < kWitnessesPerAnswer) {
            ++count;
            witnesses.push_back({static_cast<int64_t>(key), answer});
        }
    }
    return witnesses;
}

template <LogicalType Type>
bool collect_partitions(const RuntimeMembershipFilter* membership, std::vector<const SimdBlockFilter*>* partitions,
                        std::vector<FlightBloomFilter::Witness>* witnesses) {
    const auto* bloom = down_cast<const TRuntimeBloomFilter<Type>*>(membership);
    if (bloom->partition_blooms().empty()) {
        partitions->push_back(&bloom->bloom());
    } else {
        for (const SimdBlockFilter& partition : bloom->partition_blooms()) {
            partitions->push_back(&partition);
        }
    }
    for (const SimdBlockFilter* partition : *partitions) {
        // A filter that gave up its bits (it grew past the global size limit) accepts everything.
        if (!partition->can_use()) {
            return false;
        }
    }
    *witnesses = pick_witnesses<Type>(*bloom);
    return true;
}

} // namespace

std::vector<FlightBloomFilter> collect_flight_bloom_filters(const RuntimeFilterProbeCollector& collector,
                                                            int32_t driver_sequence,
                                                            const std::function<std::string(SlotId)>& column_of) {
    std::vector<FlightBloomFilter> filters;
    for (const auto& item : collector.descriptors()) {
        const RuntimeFilterProbeDescriptor* probe = item.second;
        const RuntimeFilter* filter = probe->runtime_filter(driver_sequence);
        // A filter that holds NULL lets NULL keys through (outer joins), which the server could not reproduce.
        if (filter == nullptr || filter->always_true() || filter->has_null() ||
            filter->type() != RuntimeFilterSerializeType::BLOOM_FILTER) {
            continue;
        }
        SlotId slot_id;
        if (!probe->is_probe_slot_ref(&slot_id)) {
            continue;
        }
        std::string column = column_of(slot_id);
        if (column.empty()) {
            continue;
        }
        const RuntimeMembershipFilter* membership = filter->get_membership_filter();
        std::vector<const SimdBlockFilter*> partitions;
        std::vector<FlightBloomFilter::Witness> witnesses;
        bool ok = false;
        switch (membership->logical_type()) {
        case TYPE_TINYINT:
            ok = collect_partitions<TYPE_TINYINT>(membership, &partitions, &witnesses);
            break;
        case TYPE_SMALLINT:
            ok = collect_partitions<TYPE_SMALLINT>(membership, &partitions, &witnesses);
            break;
        case TYPE_INT:
            ok = collect_partitions<TYPE_INT>(membership, &partitions, &witnesses);
            break;
        case TYPE_BIGINT:
            ok = collect_partitions<TYPE_BIGINT>(membership, &partitions, &witnesses);
            break;
        default:
            break;
        }
        if (ok && !partitions.empty()) {
            filters.push_back(FlightBloomFilter{std::move(column), std::move(partitions), std::move(witnesses)});
        }
    }
    return filters;
}

namespace {

constexpr uint32_t kBlockMagic = 0x41524631; // "ARF1"
constexpr uint32_t kBloomBlockMagic = 0x41524632; // "ARF2"
constexpr uint32_t kTicketFilterTag = (15 << 3) | 2;

template <typename T>
void put(std::string* out, T value) {
    // The block is little-endian; every platform StarRocks builds for is too.
    out->append(reinterpret_cast<const char*>(&value), sizeof(T));
}

void put_varint(std::string* out, uint64_t value) {
    while (value >= 0x80) {
        out->push_back(static_cast<char>(value | 0x80));
        value >>= 7;
    }
    out->push_back(static_cast<char>(value));
}

} // namespace

namespace {

std::vector<const FlightInFilter*> within_value_limit(const std::vector<FlightInFilter>& filters) {
    std::vector<const FlightInFilter*> kept;
    size_t total = 0;
    for (const FlightInFilter& filter : filters) {
        if (total + filter.values.size() > kFlightMaxFilterValues) {
            continue;
        }
        total += filter.values.size();
        kept.push_back(&filter);
    }
    return kept;
}

void put_in_filters(std::string* block, const std::vector<const FlightInFilter*>& kept) {
    put<uint32_t>(block, static_cast<uint32_t>(kept.size()));
    for (const FlightInFilter* filter : kept) {
        put<uint32_t>(block, static_cast<uint32_t>(filter->column.size()));
        block->append(filter->column);
        put<uint32_t>(block, static_cast<uint32_t>(filter->values.size()));
        for (int64_t value : filter->values) {
            put<int64_t>(block, value);
        }
    }
}

} // namespace

std::string encode_flight_filter_block(const std::vector<FlightInFilter>& filters) {
    std::vector<const FlightInFilter*> kept = within_value_limit(filters);
    if (kept.empty()) {
        return {};
    }
    std::string block;
    put<uint32_t>(&block, kBlockMagic);
    put_in_filters(&block, kept);
    return block;
}

std::string encode_flight_filter_block(const std::vector<FlightInFilter>& filters,
                                       const std::vector<FlightBloomFilter>& blooms, size_t max_bloom_bytes) {
    std::vector<const FlightBloomFilter*> kept_blooms;
    size_t bloom_bytes = 0;
    for (const FlightBloomFilter& bloom : blooms) {
        size_t bytes = 0;
        for (const SimdBlockFilter* partition : bloom.partitions) {
            bytes += partition->get_alloc_size();
        }
        if (bytes == 0 || bloom_bytes + bytes > max_bloom_bytes) {
            continue;
        }
        bloom_bytes += bytes;
        kept_blooms.push_back(&bloom);
    }
    if (kept_blooms.empty()) {
        return encode_flight_filter_block(filters);
    }
    std::vector<const FlightInFilter*> kept = within_value_limit(filters);
    std::string block;
    block.reserve(bloom_bytes + 64);
    put<uint32_t>(&block, kBloomBlockMagic);
    put_in_filters(&block, kept);
    put<uint32_t>(&block, static_cast<uint32_t>(kept_blooms.size()));
    for (const FlightBloomFilter* bloom : kept_blooms) {
        put<uint32_t>(&block, static_cast<uint32_t>(bloom->column.size()));
        block.append(bloom->column);
        put<uint32_t>(&block, static_cast<uint32_t>(bloom->partitions.size()));
        for (const SimdBlockFilter* partition : bloom->partitions) {
            put<int32_t>(&block, partition->log_num_buckets());
            put<uint64_t>(&block, partition->get_alloc_size());
            block.append(static_cast<const char*>(partition->directory()), partition->get_alloc_size());
        }
        put<uint32_t>(&block, static_cast<uint32_t>(bloom->witnesses.size()));
        for (const FlightBloomFilter::Witness& witness : bloom->witnesses) {
            put<int64_t>(&block, witness.key);
            put<uint8_t>(&block, witness.accepted ? 1 : 0);
        }
    }
    return block;
}

std::string attach_flight_filter_block(const std::string& ticket, const std::string& block) {
    if (block.empty()) {
        return ticket;
    }
    std::string result = ticket;
    put_varint(&result, kTicketFilterTag);
    put_varint(&result, block.size());
    result.append(block);
    return result;
}

} // namespace starrocks::connector
