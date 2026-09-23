// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Raw, hand-written FFI declarations for `libantfly`, the embedded Antfly
//! C ABI.
//!
//! This mirrors `zig/pkg/antfly/include/antfly.h` field-for-field and
//! function-for-function. There is no `bindgen`/`libclang` dependency: the
//! header is small and stable enough that a hand-written mirror is easier to
//! audit and does not require a C toolchain to build this crate.
//!
//! Nothing in this crate is safe to call directly: every function takes raw
//! pointers and untyped handles exactly as the C ABI defines them. Use the
//! `antfly-lite` crate for a safe API.
//!
//! Linking against the actual `libantfly` dylib only happens when the
//! `libantfly` Cargo feature is enabled (see `build.rs`). Without it, this
//! crate compiles (and its types can be named) but must not be linked into
//! any binary that references these `extern "C"` items, or linking will
//! fail.
//!
//! Naming (mirrors the header's own convention): `antfly_*` functions are
//! library-level and take no database handle; `antfly_db_*` functions take
//! an `antfly_db` handle of any storage kind; `antfly_lite_*` functions
//! operate on the `.aflite` single-file format itself, plus shortcuts for
//! opening one.
#![allow(non_camel_case_types)]

use std::ffi::c_char;

// ---------------------------------------------------------------------
// antfly_error_code
// ---------------------------------------------------------------------

/// A stable Antfly C ABI error code. Represented as `i32` to match a plain
/// C `enum`'s default underlying type on every target this crate supports.
pub type antfly_error_code = i32;

pub const ANTFLY_OK: antfly_error_code = 0;
pub const ANTFLY_INVALID_ARGUMENT: antfly_error_code = 1;
pub const ANTFLY_NOT_FOUND: antfly_error_code = 2;
pub const ANTFLY_VERSION_CONFLICT: antfly_error_code = 3;
pub const ANTFLY_INTENT_CONFLICT: antfly_error_code = 4;
pub const ANTFLY_TXN_NOT_FOUND: antfly_error_code = 5;
pub const ANTFLY_BUSY: antfly_error_code = 6;
pub const ANTFLY_OUTCOME_UNKNOWN: antfly_error_code = 7;
pub const ANTFLY_UNSUPPORTED: antfly_error_code = 8;
pub const ANTFLY_STALLED: antfly_error_code = 9;
pub const ANTFLY_INTERNAL: antfly_error_code = 255;

// ---------------------------------------------------------------------
// antfly_db (opaque handle)
// ---------------------------------------------------------------------

/// An open database. Opaque; see `antfly_db_open_with_options`. Every
/// handle-taking function in this crate takes `*mut antfly_db` /
/// `*mut *mut antfly_db`, matching the header's typed opaque pointer (ABI
/// version 2 replaced the untyped `void *` handle with this type).
#[repr(C)]
pub struct antfly_db {
    _private: [u8; 0],
}

// ---------------------------------------------------------------------
// antfly_txn_status
// ---------------------------------------------------------------------

/// Transaction intent lifecycle state. Every C ABI function that carries a
/// transaction status uses a raw `uint8_t`, not this enum type, so this
/// alias exists only for documentation/parity with the header.
pub type antfly_txn_status = u8;

pub const ANTFLY_TXN_PENDING: antfly_txn_status = 0;
pub const ANTFLY_TXN_COMMITTED: antfly_txn_status = 1;
pub const ANTFLY_TXN_ABORTED: antfly_txn_status = 2;

// ---------------------------------------------------------------------
// Open mode / profile / storage kind / flag constants
// ---------------------------------------------------------------------

pub const ANTFLY_OPEN_MODE_WRITER: u32 = 0;
pub const ANTFLY_OPEN_MODE_READONLY: u32 = 1;
pub const ANTFLY_OPEN_MODE_STATUS_ONLY: u32 = 2;

pub const ANTFLY_STORAGE_KIND_DIRECTORY: u32 = 0;
pub const ANTFLY_STORAGE_KIND_LITE: u32 = 1;

pub const ANTFLY_PROFILE_NATIVE: u32 = 0;
pub const ANTFLY_PROFILE_HOSTED: u32 = 1;

pub const ANTFLY_OPEN_FLAG_NO_SYNC: u32 = 1 << 0;
pub const ANTFLY_OPEN_FLAG_TTL_CLEANUP: u32 = 1 << 1;
pub const ANTFLY_OPEN_FLAG_REMOTE_PROVIDER_CONFIGURED: u32 = 1 << 2;
pub const ANTFLY_OPEN_FLAG_LOCAL_RUNTIME_CONFIGURED: u32 = 1 << 3;
pub const ANTFLY_OPEN_FLAG_GENERATED_ENRICHMENT_REPLAY: u32 = 1 << 4;

/// `inference.mode` values reported by `antfly_db_status_json`.
pub const ANTFLY_INFERENCE_MODE_CALLER_SUPPLIED_OR_DISABLED: &str = "caller_supplied_or_disabled";
pub const ANTFLY_INFERENCE_MODE_CALLER_SUPPLIED_ARTIFACTS: &str = "caller_supplied_artifacts";
pub const ANTFLY_INFERENCE_MODE_REMOTE_PROVIDER: &str = "remote_provider";
pub const ANTFLY_INFERENCE_MODE_LOCAL_EMBEDDED: &str = "local_embedded";
pub const ANTFLY_INFERENCE_MODE_MANUAL_MAINTENANCE: &str = "manual_maintenance";
pub const ANTFLY_INFERENCE_MODE_DISABLED_DEFERRED: &str = "disabled_deferred";

/// Threading contract, like `sqlite3_threadsafe()`. See `antfly_threading_mode`.
pub const ANTFLY_THREADING_SERIALIZED: u32 = 1;

/// Minimum native stack, in bytes, for any thread that calls into libantfly.
/// See `ANTFLY_MIN_THREAD_STACK_SIZE` in antfly.h.
pub const ANTFLY_MIN_THREAD_STACK_SIZE: usize = 8 * 1024 * 1024;

/// `direction` values for `antfly_db_get_edges_json` and
/// `antfly_db_get_neighbors_json`.
pub const ANTFLY_GRAPH_DIRECTION_OUT: u8 = 0;
pub const ANTFLY_GRAPH_DIRECTION_IN: u8 = 1;
pub const ANTFLY_GRAPH_DIRECTION_BOTH: u8 = 2;

// ---------------------------------------------------------------------
// Plain-old-data structs (repr(C), field order matches antfly.h exactly)
// ---------------------------------------------------------------------

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct antfly_slice {
    pub ptr: *const u8,
    pub len: usize,
}

impl Default for antfly_slice {
    fn default() -> Self {
        antfly_slice {
            ptr: std::ptr::null(),
            len: 0,
        }
    }
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct antfly_buffer {
    pub ptr: *mut u8,
    pub len: usize,
}

impl Default for antfly_buffer {
    fn default() -> Self {
        antfly_buffer {
            ptr: std::ptr::null_mut(),
            len: 0,
        }
    }
}

/// One options struct for every storage kind. `storage_kind` (an
/// `ANTFLY_STORAGE_KIND_*` value) picks a `.aflite` file or a normal
/// directory; the inference budget fields, `busy_timeout_ms`, and
/// `reserved[8]` layout must mirror the header exactly -- `tests/abi_sizes.rs`
/// checks `size_of::<antfly_open_options>()` against
/// `antfly_open_options_size()`.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct antfly_open_options {
    pub abi_size: u32,
    pub storage_kind: u32,
    pub open_mode: u32,
    pub profile: u32,
    pub flags: u32,
    pub reserved0: u32,
    pub map_size: u64,
    pub ttl_cleanup_enabled: bool,
    pub ttl_cleanup_lease_owned: bool,
    pub ttl_cleanup_batch_size: u32,
    pub ttl_cleanup_owner_id: antfly_slice,
    pub ttl_cleanup_lease_ttl_ms: u64,
    pub ttl_cleanup_interval_ms: u64,
    pub ttl_cleanup_grace_period_ns: u64,
    /// Explicit embedded-inference resource-budget overrides in MiB, 0
    /// meaning automatic/host-detected sizing. Only consulted when `flags`
    /// carries `ANTFLY_OPEN_FLAG_LOCAL_RUNTIME_CONFIGURED`.
    pub inference_host_budget_mb: u32,
    pub inference_backend_budget_mb: u32,
    pub inference_process_memory_budget_mb: u32,
    pub inference_combined_budget_mb: u32,
    pub inference_kv_budget_mb: u32,
    pub inference_scratch_budget_mb: u32,
    /// Milliseconds to keep retrying while another writer holds the writer
    /// lock (`ANTFLY_BUSY`), like `sqlite3_busy_timeout`. 0 fails immediately.
    pub busy_timeout_ms: u64,
    pub reserved: [u64; 8],
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct antfly_write_intent {
    pub key: antfly_slice,
    pub value: antfly_slice,
    pub is_delete: bool,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct antfly_version_predicate {
    pub key: antfly_slice,
    pub expected_version: u64,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct antfly_dense_search_hit {
    pub id_ptr: *mut u8,
    pub id_len: usize,
    pub score: f32,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct antfly_dense_search_result {
    pub hits_ptr: *mut antfly_dense_search_hit,
    pub hit_count: usize,
    pub total_hits: u32,
    pub identity_read_generation: u64,
}

impl Default for antfly_dense_search_result {
    fn default() -> Self {
        antfly_dense_search_result {
            hits_ptr: std::ptr::null_mut(),
            hit_count: 0,
            total_hits: 0,
            identity_read_generation: 0,
        }
    }
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct antfly_packed_dense_search_hit {
    pub id_offset: usize,
    pub id_len: usize,
    pub score: f32,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct antfly_packed_dense_search_result {
    pub hits_ptr: *mut antfly_packed_dense_search_hit,
    pub hit_count: usize,
    pub total_hits: u32,
    pub ids_ptr: *mut u8,
    pub ids_len: usize,
    pub identity_read_generation: u64,
}

impl Default for antfly_packed_dense_search_result {
    fn default() -> Self {
        antfly_packed_dense_search_result {
            hits_ptr: std::ptr::null_mut(),
            hit_count: 0,
            total_hits: 0,
            ids_ptr: std::ptr::null_mut(),
            ids_len: 0,
            identity_read_generation: 0,
        }
    }
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Default)]
pub struct antfly_dense_search_profile {
    pub total_ns: u64,
    pub index_lookup_ns: u64,
    pub search_ns: u64,
    pub hits_ns: u64,
    pub fallback_ns: u64,
    pub hbc_total_ns: u64,
    pub hbc_setup_ns: u64,
    pub hbc_root_load_ns: u64,
    pub hbc_node_cache_miss_ns: u64,
    pub hbc_node_cache_misses: u64,
    pub hbc_quantized_cache_miss_ns: u64,
    pub hbc_quantized_cache_misses: u64,
    pub hbc_child_expand_ns: u64,
    pub hbc_leaf_score_ns: u64,
    pub hbc_rerank_ns: u64,
    pub hbc_rerank_vector_load_ns: u64,
    pub hbc_rerank_distance_ns: u64,
    pub hbc_nodes_visited: u64,
    pub hbc_leaves_explored: u64,
    pub hbc_reranked_vectors: u64,
    pub hit_count: u32,
    pub total_hits: u32,
    pub used_fast_path: bool,
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Default)]
pub struct antfly_dense_wire_search_profile {
    pub total_ns: u64,
    pub decode_ns: u64,
    pub search_ns: u64,
    pub resolve_ns: u64,
    pub encode_ns: u64,
    pub fallback_ns: u64,
    pub hbc_total_ns: u64,
    pub hbc_setup_ns: u64,
    pub hbc_root_load_ns: u64,
    pub hbc_node_cache_miss_ns: u64,
    pub hbc_node_cache_misses: u64,
    pub hbc_quantized_cache_miss_ns: u64,
    pub hbc_quantized_cache_misses: u64,
    pub hbc_child_expand_ns: u64,
    pub hbc_leaf_score_ns: u64,
    pub hbc_rerank_ns: u64,
    pub hbc_rerank_vector_load_ns: u64,
    pub hbc_rerank_distance_ns: u64,
    pub hbc_nodes_visited: u64,
    pub hbc_leaves_explored: u64,
    pub hbc_reranked_vectors: u64,
    pub hit_count: u32,
    pub total_hits: u32,
    pub used_fast_path: bool,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct antfly_scan_hash_entry {
    pub id_ptr: *mut u8,
    pub id_len: usize,
    pub hash: u64,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct antfly_scan_hash_result {
    pub entries_ptr: *mut antfly_scan_hash_entry,
    pub entry_count: usize,
}

impl Default for antfly_scan_hash_result {
    fn default() -> Self {
        antfly_scan_hash_result {
            entries_ptr: std::ptr::null_mut(),
            entry_count: 0,
        }
    }
}

// ---------------------------------------------------------------------
// extern "C" functions -- order matches antfly.h
// ---------------------------------------------------------------------

unsafe extern "C" {
    pub fn antfly_abi_version() -> u32;
    pub fn antfly_open_options_size() -> u32;
    pub fn antfly_error_code_name(code: antfly_error_code) -> *const c_char;
    pub fn antfly_error_code_description(code: antfly_error_code) -> *const c_char;
    pub fn antfly_open_options_init(options: *mut antfly_open_options) -> antfly_error_code;

    pub fn antfly_threading_mode() -> u32;

    pub fn antfly_db_open(path: *const c_char, out_db: *mut *mut antfly_db) -> antfly_error_code;
    pub fn antfly_db_open_with_options(
        path: *const c_char,
        options: *const antfly_open_options,
        out_db: *mut *mut antfly_db,
    ) -> antfly_error_code;
    pub fn antfly_db_create_with_options(
        path: *const c_char,
        options: *const antfly_open_options,
        out_db: *mut *mut antfly_db,
    ) -> antfly_error_code;
    pub fn antfly_db_close(db: *mut antfly_db);

    pub fn antfly_lite_open(path: *const c_char, out_db: *mut *mut antfly_db) -> antfly_error_code;
    pub fn antfly_lite_create(
        path: *const c_char,
        out_db: *mut *mut antfly_db,
    ) -> antfly_error_code;
    pub fn antfly_lite_open_hosted(
        path: *const c_char,
        out_db: *mut *mut antfly_db,
    ) -> antfly_error_code;
    pub fn antfly_lite_create_hosted(
        path: *const c_char,
        out_db: *mut *mut antfly_db,
    ) -> antfly_error_code;
    pub fn antfly_lite_open_readonly(
        path: *const c_char,
        out_db: *mut *mut antfly_db,
    ) -> antfly_error_code;
    pub fn antfly_lite_open_status_only(
        path: *const c_char,
        out_db: *mut *mut antfly_db,
    ) -> antfly_error_code;

    pub fn antfly_db_status_json(db: *mut antfly_db, out: *mut antfly_buffer) -> antfly_error_code;
    pub fn antfly_db_capabilities_json(
        db: *mut antfly_db,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;

    pub fn antfly_db_backup(db: *mut antfly_db, out: *mut antfly_buffer) -> antfly_error_code;
    pub fn antfly_db_import_backup(db: *mut antfly_db, backup: antfly_slice) -> antfly_error_code;
    pub fn antfly_restore_backup_json(
        dest_path: *const c_char,
        options: *const antfly_open_options,
        backup: antfly_slice,
        replace: bool,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;
    pub fn antfly_restore_backup_file_json(
        dest_path: *const c_char,
        options: *const antfly_open_options,
        backup_path: *const c_char,
        replace: bool,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;

    pub fn antfly_lite_check_json(db: *mut antfly_db, out: *mut antfly_buffer)
    -> antfly_error_code;
    pub fn antfly_lite_check_file_json(
        path: *const c_char,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;
    pub fn antfly_lite_copy_stable_snapshot_json(
        db: *mut antfly_db,
        dest_path: *const c_char,
        replace: bool,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;
    pub fn antfly_lite_copy_stable_snapshot_file_json(
        src_path: *const c_char,
        dest_path: *const c_char,
        replace: bool,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;
    pub fn antfly_lite_compact_json(
        db: *mut antfly_db,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;
    pub fn antfly_lite_vacuum_json(
        db: *mut antfly_db,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;

    pub fn antfly_buffer_free(buffer: *mut antfly_buffer);
    pub fn antfly_buffer_free_zero(buffer: *mut antfly_buffer);
    pub fn antfly_dense_search_result_free(result: *mut antfly_dense_search_result);
    pub fn antfly_packed_dense_search_result_free(result: *mut antfly_packed_dense_search_result);
    pub fn antfly_scan_hash_result_free(result: *mut antfly_scan_hash_result);

    pub fn antfly_db_batch(
        db: *mut antfly_db,
        writes: *const antfly_write_intent,
        write_count: usize,
        predicates: *const antfly_version_predicate,
        predicate_count: usize,
        timestamp_ns: u64,
        sync_level: u8,
    ) -> antfly_error_code;
    pub fn antfly_db_batch_json(
        db: *mut antfly_db,
        request_json: antfly_slice,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;
    pub fn antfly_db_begin_transaction_with_id(
        db: *mut antfly_db,
        txn_id: *const [u8; 16],
        timestamp_ns: u64,
        participants: *const antfly_slice,
        participant_count: usize,
    ) -> antfly_error_code;
    pub fn antfly_db_write_transaction(
        db: *mut antfly_db,
        txn_id: *const [u8; 16],
        writes: *const antfly_write_intent,
        write_count: usize,
        predicates: *const antfly_version_predicate,
        predicate_count: usize,
    ) -> antfly_error_code;
    pub fn antfly_db_resolve_intents(
        db: *mut antfly_db,
        txn_id: *const [u8; 16],
        status: u8,
        commit_version: u64,
    ) -> antfly_error_code;
    pub fn antfly_db_get_transaction_status(
        db: *mut antfly_db,
        txn_id: *const [u8; 16],
        out_status: *mut u8,
    ) -> antfly_error_code;
    pub fn antfly_db_get_commit_version(
        db: *mut antfly_db,
        txn_id: *const [u8; 16],
        out_commit_version: *mut u64,
    ) -> antfly_error_code;
    pub fn antfly_db_get_timestamp(
        db: *mut antfly_db,
        key: antfly_slice,
        out_timestamp: *mut u64,
    ) -> antfly_error_code;
    pub fn antfly_db_lookup_json(
        db: *mut antfly_db,
        key: antfly_slice,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;
    pub fn antfly_db_get_raw(
        db: *mut antfly_db,
        key: antfly_slice,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;
    pub fn antfly_db_get_schema_json(
        db: *mut antfly_db,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;
    pub fn antfly_db_set_schema_json(
        db: *mut antfly_db,
        schema_json: antfly_slice,
    ) -> antfly_error_code;
    pub fn antfly_db_run_until_idle(db: *mut antfly_db) -> antfly_error_code;
    pub fn antfly_db_run_until_idle_json(
        db: *mut antfly_db,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;
    pub fn antfly_db_pending_work_stats_json(
        db: *mut antfly_db,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;
    pub fn antfly_db_replay_generated_enrichments_json(
        db: *mut antfly_db,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;
    pub fn antfly_db_list_indexes_json(
        db: *mut antfly_db,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;
    pub fn antfly_db_add_index_json(
        db: *mut antfly_db,
        config_json: antfly_slice,
    ) -> antfly_error_code;
    pub fn antfly_db_delete_index(
        db: *mut antfly_db,
        name: antfly_slice,
        out_deleted: *mut bool,
    ) -> antfly_error_code;
    pub fn antfly_db_list_enrichments_json(
        db: *mut antfly_db,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;
    pub fn antfly_db_add_enrichment_json(
        db: *mut antfly_db,
        config_json: antfly_slice,
    ) -> antfly_error_code;
    pub fn antfly_db_delete_enrichment(
        db: *mut antfly_db,
        kind: antfly_slice,
        name: antfly_slice,
        out_deleted: *mut bool,
    ) -> antfly_error_code;
    pub fn antfly_db_scan_json(
        db: *mut antfly_db,
        request_json: antfly_slice,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;
    pub fn antfly_db_scan_hashes(
        db: *mut antfly_db,
        request_json: antfly_slice,
        out_result: *mut antfly_scan_hash_result,
    ) -> antfly_error_code;
    pub fn antfly_db_stats_json(db: *mut antfly_db, out: *mut antfly_buffer) -> antfly_error_code;
    pub fn antfly_db_search_json(
        db: *mut antfly_db,
        request_json: antfly_slice,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;
    pub fn antfly_db_search_dense(
        db: *mut antfly_db,
        index_name: antfly_slice,
        vector_ptr: *const f32,
        vector_len: usize,
        k: u32,
        limit: u32,
        offset: u32,
        out_result: *mut antfly_packed_dense_search_result,
    ) -> antfly_error_code;
    pub fn antfly_db_search_dense_profile(
        db: *mut antfly_db,
        index_name: antfly_slice,
        vector_ptr: *const f32,
        vector_len: usize,
        k: u32,
        limit: u32,
        offset: u32,
        out_profile: *mut antfly_dense_search_profile,
    ) -> antfly_error_code;
    pub fn antfly_db_search_dense_wire(
        db: *mut antfly_db,
        request_buf: antfly_slice,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;
    pub fn antfly_db_search_dense_wire_profile(
        db: *mut antfly_db,
        request_buf: antfly_slice,
        out: *mut antfly_buffer,
        out_profile: *mut antfly_dense_wire_search_profile,
    ) -> antfly_error_code;
    pub fn antfly_db_search_text_match(
        db: *mut antfly_db,
        index_name: antfly_slice,
        field: antfly_slice,
        text: antfly_slice,
        limit: u32,
        offset: u32,
        out_result: *mut antfly_dense_search_result,
    ) -> antfly_error_code;
    pub fn antfly_db_search_text_match_wire(
        db: *mut antfly_db,
        request_buf: antfly_slice,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;
    pub fn antfly_db_search_text_term_wire(
        db: *mut antfly_db,
        request_buf: antfly_slice,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;
    pub fn antfly_db_search_text_match_phrase_wire(
        db: *mut antfly_db,
        request_buf: antfly_slice,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;
    pub fn antfly_db_search_hits_json(
        db: *mut antfly_db,
        request_json: antfly_slice,
        out_result: *mut antfly_dense_search_result,
    ) -> antfly_error_code;
    pub fn antfly_db_aggregate_hits_json(
        db: *mut antfly_db,
        request_json: antfly_slice,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;
    pub fn antfly_db_lookup_artifact_json(
        db: *mut antfly_db,
        artifact_id_b64: antfly_slice,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;
    pub fn antfly_decode_artifact_id_json(
        artifact_id_b64: antfly_slice,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;
    pub fn antfly_db_extract_enrichments_json(
        db: *mut antfly_db,
        request_json: antfly_slice,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;
    pub fn antfly_db_compute_enrichments_json(
        db: *mut antfly_db,
        request_json: antfly_slice,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;

    pub fn antfly_db_get_edges_json(
        db: *mut antfly_db,
        index_name: antfly_slice,
        key: antfly_slice,
        edge_type: antfly_slice,
        direction: u8,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;
    pub fn antfly_db_traverse_edges_json(
        db: *mut antfly_db,
        request_json: antfly_slice,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;
    pub fn antfly_db_execute_graph_queries_json(
        db: *mut antfly_db,
        request_json: antfly_slice,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;
    pub fn antfly_db_get_neighbors_json(
        db: *mut antfly_db,
        index_name: antfly_slice,
        key: antfly_slice,
        edge_type: antfly_slice,
        direction: u8,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;
    pub fn antfly_db_find_shortest_path_json(
        db: *mut antfly_db,
        request_json: antfly_slice,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;
    pub fn antfly_db_find_k_shortest_paths_json(
        db: *mut antfly_db,
        request_json: antfly_slice,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;
    pub fn antfly_db_match_pattern_json(
        db: *mut antfly_db,
        request_json: antfly_slice,
        out: *mut antfly_buffer,
    ) -> antfly_error_code;
}
