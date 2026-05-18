// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");

pub const Slice = extern struct {
    ptr: ?[*]const u8 = null,
    len: usize = 0,

    pub fn bytes(self: Slice) []const u8 {
        if (self.ptr == null or self.len == 0) return "";
        return self.ptr.?[0..self.len];
    }
};

pub const WriteIntent = extern struct {
    key: Slice,
    value: Slice,
    is_delete: bool = false,
};

pub const VersionPredicate = extern struct {
    key: Slice,
    expected_version: u64,
};

pub const Buffer = extern struct {
    ptr: ?[*]u8 = null,
    len: usize = 0,
};

pub const DenseSearchHit = extern struct {
    id_ptr: ?[*]u8 = null,
    id_len: usize = 0,
    score: f32 = 0,
};

pub const DenseSearchResult = extern struct {
    hits_ptr: ?[*]DenseSearchHit = null,
    hit_count: usize = 0,
    total_hits: u32 = 0,
};

pub const PackedDenseSearchHit = extern struct {
    id_offset: usize = 0,
    id_len: usize = 0,
    score: f32 = 0,
};

pub const PackedDenseSearchResult = extern struct {
    hits_ptr: ?[*]PackedDenseSearchHit = null,
    hit_count: usize = 0,
    total_hits: u32 = 0,
    ids_ptr: ?[*]u8 = null,
    ids_len: usize = 0,
};

pub const DenseSearchProfile = extern struct {
    total_ns: u64 = 0,
    index_lookup_ns: u64 = 0,
    search_ns: u64 = 0,
    hits_ns: u64 = 0,
    fallback_ns: u64 = 0,
    hbc_total_ns: u64 = 0,
    hbc_setup_ns: u64 = 0,
    hbc_root_load_ns: u64 = 0,
    hbc_node_cache_miss_ns: u64 = 0,
    hbc_node_cache_misses: u64 = 0,
    hbc_quantized_cache_miss_ns: u64 = 0,
    hbc_quantized_cache_misses: u64 = 0,
    hbc_child_expand_ns: u64 = 0,
    hbc_leaf_score_ns: u64 = 0,
    hbc_rerank_ns: u64 = 0,
    hbc_rerank_vector_load_ns: u64 = 0,
    hbc_rerank_distance_ns: u64 = 0,
    hbc_nodes_visited: u64 = 0,
    hbc_leaves_explored: u64 = 0,
    hbc_reranked_vectors: u64 = 0,
    hit_count: u32 = 0,
    total_hits: u32 = 0,
    used_fast_path: bool = false,
};

pub const DenseWireSearchProfile = extern struct {
    total_ns: u64 = 0,
    decode_ns: u64 = 0,
    search_ns: u64 = 0,
    resolve_ns: u64 = 0,
    encode_ns: u64 = 0,
    fallback_ns: u64 = 0,
    hbc_total_ns: u64 = 0,
    hbc_setup_ns: u64 = 0,
    hbc_root_load_ns: u64 = 0,
    hbc_node_cache_miss_ns: u64 = 0,
    hbc_node_cache_misses: u64 = 0,
    hbc_quantized_cache_miss_ns: u64 = 0,
    hbc_quantized_cache_misses: u64 = 0,
    hbc_child_expand_ns: u64 = 0,
    hbc_leaf_score_ns: u64 = 0,
    hbc_rerank_ns: u64 = 0,
    hbc_rerank_vector_load_ns: u64 = 0,
    hbc_rerank_distance_ns: u64 = 0,
    hbc_nodes_visited: u64 = 0,
    hbc_leaves_explored: u64 = 0,
    hbc_reranked_vectors: u64 = 0,
    hit_count: u32 = 0,
    total_hits: u32 = 0,
    used_fast_path: bool = false,
};

pub const ScanHashEntry = extern struct {
    id_ptr: ?[*]u8 = null,
    id_len: usize = 0,
    hash: u64 = 0,
};

pub const ScanHashResult = extern struct {
    entries_ptr: ?[*]ScanHashEntry = null,
    entry_count: usize = 0,
};

pub const ErrorCode = enum(c_int) {
    ok = 0,
    invalid_argument = 1,
    not_found = 2,
    version_conflict = 3,
    intent_conflict = 4,
    txn_not_found = 5,
    internal = 255,
};

pub fn mapError(err: anyerror) ErrorCode {
    return switch (err) {
        error.VersionConflict => .version_conflict,
        error.IntentConflict, error.DecisionConflict => .intent_conflict,
        error.TxnNotFound, error.NotFound => .txn_not_found,
        error.InvalidArgument, error.InvalidAggregation, error.UnsupportedAggregation => .invalid_argument,
        else => .internal,
    };
}
