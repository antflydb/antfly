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

pub const lite_open_mode_writer: u32 = 0;
pub const lite_open_mode_readonly: u32 = 1;
pub const lite_open_mode_status_only: u32 = 2;

pub const open_mode_writer: u32 = 0;
pub const open_mode_readonly: u32 = 1;
pub const open_mode_status_only: u32 = 2;

pub const storage_kind_directory: u32 = 0;
pub const storage_kind_lite: u32 = 1;

pub const profile_native: u32 = 0;
pub const profile_hosted: u32 = 1;

pub const lite_profile_native: u32 = 0;
pub const lite_profile_hosted: u32 = 1;

pub const open_flag_no_sync: u32 = 1 << 0;
pub const open_flag_ttl_cleanup: u32 = 1 << 1;
pub const open_flag_remote_provider_configured: u32 = 1 << 2;
pub const open_flag_local_runtime_configured: u32 = 1 << 3;
pub const open_flag_generated_enrichment_replay: u32 = 1 << 4;

pub const lite_open_flag_no_sync: u32 = 1 << 0;
pub const lite_open_flag_ttl_cleanup: u32 = 1 << 1;
pub const lite_open_flag_remote_provider_configured: u32 = 1 << 2;
pub const lite_open_flag_local_runtime_configured: u32 = 1 << 3;
pub const lite_open_flag_generated_enrichment_replay: u32 = 1 << 4;

pub const OpenOptions = extern struct {
    abi_size: u32 = @sizeOf(OpenOptions),
    storage_kind: u32 = storage_kind_directory,
    open_mode: u32 = open_mode_writer,
    profile: u32 = profile_native,
    flags: u32 = 0,
    reserved0: u32 = 0,
    map_size: u64 = 0,
    ttl_cleanup_enabled: bool = false,
    ttl_cleanup_lease_owned: bool = false,
    ttl_cleanup_batch_size: u32 = 0,
    ttl_cleanup_owner_id: Slice = .{},
    ttl_cleanup_lease_ttl_ms: u64 = 0,
    ttl_cleanup_interval_ms: u64 = 0,
    ttl_cleanup_grace_period_ns: u64 = 0,
    reserved: [8]u64 = .{0} ** 8,
};

pub const LiteOpenOptions = extern struct {
    abi_size: u32 = @sizeOf(LiteOpenOptions),
    open_mode: u32 = lite_open_mode_writer,
    profile: u32 = lite_profile_native,
    flags: u32 = 0,
    map_size: u64 = 0,
    ttl_cleanup_enabled: bool = false,
    ttl_cleanup_lease_owned: bool = false,
    ttl_cleanup_batch_size: u32 = 0,
    ttl_cleanup_owner_id: Slice = .{},
    ttl_cleanup_lease_ttl_ms: u64 = 0,
    ttl_cleanup_interval_ms: u64 = 0,
    ttl_cleanup_grace_period_ns: u64 = 0,
    reserved: [8]u64 = .{0} ** 8,
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
    identity_read_generation: u64 = 0,
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
    identity_read_generation: u64 = 0,
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
    busy = 6,
    internal = 255,
};

pub fn errorCodeName(code: c_int) [*:0]const u8 {
    return switch (code) {
        @intFromEnum(ErrorCode.ok) => "ANTFLY_OK",
        @intFromEnum(ErrorCode.invalid_argument) => "ANTFLY_INVALID_ARGUMENT",
        @intFromEnum(ErrorCode.not_found) => "ANTFLY_NOT_FOUND",
        @intFromEnum(ErrorCode.version_conflict) => "ANTFLY_VERSION_CONFLICT",
        @intFromEnum(ErrorCode.intent_conflict) => "ANTFLY_INTENT_CONFLICT",
        @intFromEnum(ErrorCode.txn_not_found) => "ANTFLY_TXN_NOT_FOUND",
        @intFromEnum(ErrorCode.busy) => "ANTFLY_BUSY",
        @intFromEnum(ErrorCode.internal) => "ANTFLY_INTERNAL",
        else => "ANTFLY_UNKNOWN_ERROR",
    };
}

pub fn errorCodeDescription(code: c_int) [*:0]const u8 {
    return switch (code) {
        @intFromEnum(ErrorCode.ok) => "operation completed successfully",
        @intFromEnum(ErrorCode.invalid_argument) => "an argument, request, path, or open mode is invalid",
        @intFromEnum(ErrorCode.not_found) => "the requested database object was not found",
        @intFromEnum(ErrorCode.version_conflict) => "a version predicate did not match the current document version",
        @intFromEnum(ErrorCode.intent_conflict) => "a transaction intent conflicts with the requested operation",
        @intFromEnum(ErrorCode.txn_not_found) => "the requested transaction was not found",
        @intFromEnum(ErrorCode.busy) => "the database is temporarily busy or a writer is already active",
        @intFromEnum(ErrorCode.internal) => "an internal error occurred",
        else => "unknown Antfly error code",
    };
}

pub fn mapError(err: anyerror) ErrorCode {
    return switch (err) {
        error.VersionConflict => .version_conflict,
        error.IntentConflict, error.DecisionConflict => .intent_conflict,
        error.TxnNotFound => .txn_not_found,
        error.NotFound => .not_found,
        error.InvalidArgument,
        error.InvalidBatchRequest,
        error.UnsupportedBatchRequestEncoding,
        error.ValueTooLong,
        error.InvalidQueryRequest,
        error.UnsupportedQueryRequest,
        error.InvalidFilterQueryRequest,
        error.InvalidExclusionQueryRequest,
        error.UnsupportedFilterQueryRequest,
        error.UnsupportedExclusionQueryRequest,
        error.IdentityReadGenerationChanged,
        error.InvalidAggregation,
        error.UnsupportedAggregation,
        error.ReadOnly,
        error.InvalidNativeSnapshotPath,
        error.InvalidNativeMagic,
        error.TruncatedNativeHeader,
        error.UnsupportedNativeFormatVersion,
        error.InvalidNativeHeaderSize,
        error.NativeHeaderChecksumMismatch,
        error.InvalidNativePageSize,
        error.InvalidNativeCheckpoint,
        error.TruncatedNativeFile,
        error.PathAlreadyExists,
        error.EndOfStream,
        error.Truncated,
        error.InvalidMagic,
        error.HeaderCrcMismatch,
        error.UnsupportedVersion,
        error.BlockCrcMismatch,
        error.InvalidBackupRequest,
        => .invalid_argument,
        error.FileNotFound => .not_found,
        error.WouldBlock,
        error.WriterLocked,
        error.FileBusy,
        => .busy,
        else => .internal,
    };
}
