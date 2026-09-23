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

#ifndef ANTFLY_H
#define ANTFLY_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum antfly_error_code {
    ANTFLY_OK = 0,
    ANTFLY_INVALID_ARGUMENT = 1,
    ANTFLY_NOT_FOUND = 2,
    ANTFLY_VERSION_CONFLICT = 3,
    ANTFLY_INTENT_CONFLICT = 4,
    ANTFLY_TXN_NOT_FOUND = 5,
    ANTFLY_BUSY = 6,
    /* The operation crossed its publication point, but crash durability could
     * not be confirmed. Inspect the destination; do not retry automatically. */
    ANTFLY_OUTCOME_UNKNOWN = 7,
    /* The operation requires a capability unavailable on this platform or
     * filesystem. Retrying unchanged will not succeed. */
    ANTFLY_UNSUPPORTED = 8,
    /* A bounded drain such as run-until-idle found a managed index making no
     * forward progress for its stall window and gave up. */
    ANTFLY_STALLED = 9,
    ANTFLY_INTERNAL = 255,
} antfly_error_code;

/*
 * C ABI conventions:
 *
 * - Functions return ANTFLY_OK on success. Any other antfly_error_code is an
 *   error and should be handled with the stable strings returned by
 *   antfly_error_code_name and antfly_error_code_description.
 * - ANTFLY_OUTCOME_UNKNOWN is not safe to retry automatically: publication
 *   completed in the running process, but crash durability was not confirmed.
 *   Inspect the caller-supplied destination before deciding how to proceed.
 * - ANTFLY_UNSUPPORTED is not transient. For file restore operations it can
 *   mean the source filesystem lacks required advisory locking; copy the
 *   archive to a supported local filesystem before retrying.
 * - antfly_slice is borrowed input. The caller owns the memory and must keep it
 *   valid for the duration of the call.
 * - antfly_buffer is owned output. On success, the caller owns the returned
 *   memory and must release it with antfly_buffer_free, or
 *   antfly_buffer_free_zero when the bytes may contain sensitive data.
 * - Status, maintenance, backup, restore, check, and snapshot calls reset
 *   antfly_buffer outputs to {NULL, 0} before validating arguments. Other
 *   buffer-returning calls require a valid output pointer and only transfer
 *   ownership after ANTFLY_OK.
 * - antfly_db handles are owned by the caller after a successful open and
 *   must be closed with antfly_db_close. Closing NULL, an already closed
 *   handle, or a handle closed concurrently is allowed.
 * - Search result structs own nested allocations only after ANTFLY_OK and must
 *   be released with their matching *_free function.
 *
 * Naming: antfly_* functions are library-level and take no database handle;
 * antfly_db_* functions take an antfly_db handle of any storage kind;
 * antfly_lite_* functions operate on the .aflite single-file format itself,
 * plus shortcuts for opening one.
 */

/* An open database. Opaque; see antfly_db_open_with_options. */
typedef struct antfly_db antfly_db;

typedef enum antfly_txn_status {
    ANTFLY_TXN_PENDING = 0,
    ANTFLY_TXN_COMMITTED = 1,
    ANTFLY_TXN_ABORTED = 2,
} antfly_txn_status;

typedef struct antfly_slice {
    const uint8_t *ptr;
    size_t len;
} antfly_slice;

typedef struct antfly_buffer {
    uint8_t *ptr;
    size_t len;
} antfly_buffer;

#define ANTFLY_OPEN_MODE_WRITER 0u
#define ANTFLY_OPEN_MODE_READONLY 1u
#define ANTFLY_OPEN_MODE_STATUS_ONLY 2u

#define ANTFLY_STORAGE_KIND_DIRECTORY 0u
#define ANTFLY_STORAGE_KIND_LITE 1u

#define ANTFLY_PROFILE_NATIVE 0u
#define ANTFLY_PROFILE_HOSTED 1u

#define ANTFLY_OPEN_FLAG_NO_SYNC (1u << 0)
#define ANTFLY_OPEN_FLAG_TTL_CLEANUP (1u << 1)
#define ANTFLY_OPEN_FLAG_REMOTE_PROVIDER_CONFIGURED (1u << 2)
#define ANTFLY_OPEN_FLAG_LOCAL_RUNTIME_CONFIGURED (1u << 3)
#define ANTFLY_OPEN_FLAG_GENERATED_ENRICHMENT_REPLAY (1u << 4)

/* `inference.mode` values reported by antfly_db_status_json. */
#define ANTFLY_INFERENCE_MODE_CALLER_SUPPLIED_OR_DISABLED "caller_supplied_or_disabled"
#define ANTFLY_INFERENCE_MODE_CALLER_SUPPLIED_ARTIFACTS "caller_supplied_artifacts"
#define ANTFLY_INFERENCE_MODE_REMOTE_PROVIDER "remote_provider"
#define ANTFLY_INFERENCE_MODE_LOCAL_EMBEDDED "local_embedded"
#define ANTFLY_INFERENCE_MODE_MANUAL_MAINTENANCE "manual_maintenance"
#define ANTFLY_INFERENCE_MODE_DISABLED_DEFERRED "disabled_deferred"

typedef struct antfly_open_options {
    uint32_t abi_size;
    uint32_t storage_kind;
    uint32_t open_mode;
    uint32_t profile;
    uint32_t flags;
    uint32_t reserved0;
    uint64_t map_size;
    bool ttl_cleanup_enabled;
    bool ttl_cleanup_lease_owned;
    uint32_t ttl_cleanup_batch_size;
    antfly_slice ttl_cleanup_owner_id;
    uint64_t ttl_cleanup_lease_ttl_ms;
    uint64_t ttl_cleanup_interval_ms;
    uint64_t ttl_cleanup_grace_period_ns;
    /* Explicit embedded-inference resource-budget overrides in MiB, 0
     * meaning automatic/host-detected sizing. Only consulted when `flags`
     * carries ANTFLY_OPEN_FLAG_LOCAL_RUNTIME_CONFIGURED; mirror the antfly
     * CLI's --inference-host-budget-mb/--inference-backend-budget-mb/
     * --process-memory-budget-mb. */
    uint32_t inference_host_budget_mb;
    uint32_t inference_backend_budget_mb;
    uint32_t inference_process_memory_budget_mb;
    uint32_t inference_combined_budget_mb;
    uint32_t inference_kv_budget_mb;
    uint32_t inference_scratch_budget_mb;
    /* Milliseconds to keep retrying while another writer holds the writer
     * lock (ANTFLY_BUSY), like sqlite3_busy_timeout. 0 fails immediately. */
    uint64_t busy_timeout_ms;
    uint64_t reserved[8];
} antfly_open_options;

/*
 * Call antfly_open_options_init before setting fields manually. The abi_size
 * field must remain the value returned by antfly_open_options_size so future
 * library versions can detect the caller's struct layout.
 */


typedef struct antfly_write_intent {
    antfly_slice key;
    antfly_slice value;
    bool is_delete;
} antfly_write_intent;

typedef struct antfly_version_predicate {
    antfly_slice key;
    uint64_t expected_version;
} antfly_version_predicate;

typedef struct antfly_dense_search_hit {
    uint8_t *id_ptr;
    size_t id_len;
    float score;
} antfly_dense_search_hit;

typedef struct antfly_dense_search_result {
    antfly_dense_search_hit *hits_ptr;
    size_t hit_count;
    uint32_t total_hits;
    uint64_t identity_read_generation;
} antfly_dense_search_result;

typedef struct antfly_packed_dense_search_hit {
    size_t id_offset;
    size_t id_len;
    float score;
} antfly_packed_dense_search_hit;

typedef struct antfly_packed_dense_search_result {
    antfly_packed_dense_search_hit *hits_ptr;
    size_t hit_count;
    uint32_t total_hits;
    uint8_t *ids_ptr;
    size_t ids_len;
    uint64_t identity_read_generation;
} antfly_packed_dense_search_result;

typedef struct antfly_dense_search_profile {
    uint64_t total_ns;
    uint64_t index_lookup_ns;
    uint64_t search_ns;
    uint64_t hits_ns;
    uint64_t fallback_ns;
    uint64_t hbc_total_ns;
    uint64_t hbc_setup_ns;
    uint64_t hbc_root_load_ns;
    uint64_t hbc_node_cache_miss_ns;
    uint64_t hbc_node_cache_misses;
    uint64_t hbc_quantized_cache_miss_ns;
    uint64_t hbc_quantized_cache_misses;
    uint64_t hbc_child_expand_ns;
    uint64_t hbc_leaf_score_ns;
    uint64_t hbc_rerank_ns;
    uint64_t hbc_rerank_vector_load_ns;
    uint64_t hbc_rerank_distance_ns;
    uint64_t hbc_nodes_visited;
    uint64_t hbc_leaves_explored;
    uint64_t hbc_reranked_vectors;
    uint32_t hit_count;
    uint32_t total_hits;
    bool used_fast_path;
} antfly_dense_search_profile;

typedef struct antfly_dense_wire_search_profile {
    uint64_t total_ns;
    uint64_t decode_ns;
    uint64_t search_ns;
    uint64_t resolve_ns;
    uint64_t encode_ns;
    uint64_t fallback_ns;
    uint64_t hbc_total_ns;
    uint64_t hbc_setup_ns;
    uint64_t hbc_root_load_ns;
    uint64_t hbc_node_cache_miss_ns;
    uint64_t hbc_node_cache_misses;
    uint64_t hbc_quantized_cache_miss_ns;
    uint64_t hbc_quantized_cache_misses;
    uint64_t hbc_child_expand_ns;
    uint64_t hbc_leaf_score_ns;
    uint64_t hbc_rerank_ns;
    uint64_t hbc_rerank_vector_load_ns;
    uint64_t hbc_rerank_distance_ns;
    uint64_t hbc_nodes_visited;
    uint64_t hbc_leaves_explored;
    uint64_t hbc_reranked_vectors;
    uint32_t hit_count;
    uint32_t total_hits;
    bool used_fast_path;
} antfly_dense_wire_search_profile;

typedef struct antfly_scan_hash_entry {
    uint8_t *id_ptr;
    size_t id_len;
    uint64_t hash;
} antfly_scan_hash_entry;

typedef struct antfly_scan_hash_result {
    antfly_scan_hash_entry *entries_ptr;
    size_t entry_count;
} antfly_scan_hash_result;

uint32_t antfly_abi_version(void);
uint32_t antfly_open_options_size(void);
const char *antfly_error_code_name(antfly_error_code code);
const char *antfly_error_code_description(antfly_error_code code);
antfly_error_code antfly_open_options_init(antfly_open_options *options);

/*
 * Threading contract, like sqlite3_threadsafe(). ANTFLY_THREADING_SERIALIZED
 * means any thread may call any function on any handle concurrently: reads
 * run in parallel, writes on one handle queue behind each other, and schema
 * or admin changes wait for in-flight calls. See zig/CAPI.md "Thread Safety".
 */
#define ANTFLY_THREADING_SERIALIZED 1u
/*
 * Minimum native stack, in bytes, for any thread that calls into libantfly.
 * The storage engine keeps sizable buffers on the stack; release builds use
 * about 2 MiB at peak and debug builds more. 8 MiB is the Linux and macOS
 * main-thread default, but secondary threads are often smaller (512 KiB for
 * macOS pthreads, 2 MiB for Rust std threads): size them explicitly.
 */
#define ANTFLY_MIN_THREAD_STACK_SIZE (8u * 1024u * 1024u)
uint32_t antfly_threading_mode(void);

/*
 * Storage-neutral opens. ANTFLY_STORAGE_KIND_DIRECTORY opens a normal
 * single-node Antfly directory; ANTFLY_STORAGE_KIND_LITE opens a .aflite
 * single-file database. antfly_db_create_with_options has exclusive-create
 * semantics for ANTFLY_STORAGE_KIND_LITE; directory storage is opened (and
 * created if missing) with antfly_db_open_with_options. NULL options open
 * directory storage with defaults.
 */
antfly_error_code antfly_db_open(const char *path, antfly_db **out_db);
antfly_error_code antfly_db_open_with_options(
    const char *path,
    const antfly_open_options *options,
    antfly_db **out_db
);
antfly_error_code antfly_db_create_with_options(
    const char *path,
    const antfly_open_options *options,
    antfly_db **out_db
);
void antfly_db_close(antfly_db *db);

/*
 * Shortcuts for ANTFLY_STORAGE_KIND_LITE with default options.
 * antfly_lite_create* creates a new .aflite database; antfly_lite_open*
 * opens an existing one and never creates a missing file or migrates a
 * pre-release layout.
 */
antfly_error_code antfly_lite_open(const char *path, antfly_db **out_db);
antfly_error_code antfly_lite_create(const char *path, antfly_db **out_db);
antfly_error_code antfly_lite_open_hosted(const char *path, antfly_db **out_db);
antfly_error_code antfly_lite_create_hosted(const char *path, antfly_db **out_db);
antfly_error_code antfly_lite_open_readonly(const char *path, antfly_db **out_db);
antfly_error_code antfly_lite_open_status_only(const char *path, antfly_db **out_db);

/* Storage identity, stats, pending work, inference state, and capabilities. */
antfly_error_code antfly_db_status_json(antfly_db *db, antfly_buffer *out);
antfly_error_code antfly_db_capabilities_json(antfly_db *db, antfly_buffer *out);

/*
 * Portable .afb backups work across storage kinds: a backup of either kind
 * restores or imports into either kind. antfly_db_import_backup requires an
 * empty database. antfly_restore_backup_json and
 * antfly_restore_backup_file_json create a new database at dest_path whose
 * storage kind comes from `options` (NULL means directory storage); other
 * option fields apply when the restored database is opened. `replace`
 * atomically replaces an existing destination.
 */
antfly_error_code antfly_db_backup(antfly_db *db, antfly_buffer *out);
antfly_error_code antfly_db_import_backup(antfly_db *db, antfly_slice backup);
antfly_error_code antfly_restore_backup_json(
    const char *dest_path,
    const antfly_open_options *options,
    antfly_slice backup,
    bool replace,
    antfly_buffer *out
);
antfly_error_code antfly_restore_backup_file_json(
    const char *dest_path,
    const antfly_open_options *options,
    const char *backup_path,
    bool replace,
    antfly_buffer *out
);

/* Operations on the .aflite file format. */
antfly_error_code antfly_lite_check_json(antfly_db *db, antfly_buffer *out);
antfly_error_code antfly_lite_check_file_json(const char *path, antfly_buffer *out);
antfly_error_code antfly_lite_copy_stable_snapshot_json(
    antfly_db *db,
    const char *dest_path,
    bool replace,
    antfly_buffer *out
);
antfly_error_code antfly_lite_copy_stable_snapshot_file_json(
    const char *src_path,
    const char *dest_path,
    bool replace,
    antfly_buffer *out
);
antfly_error_code antfly_lite_compact_json(antfly_db *db, antfly_buffer *out);
antfly_error_code antfly_lite_vacuum_json(antfly_db *db, antfly_buffer *out);

void antfly_buffer_free(antfly_buffer *buffer);
void antfly_buffer_free_zero(antfly_buffer *buffer);
void antfly_dense_search_result_free(antfly_dense_search_result *result);
void antfly_packed_dense_search_result_free(antfly_packed_dense_search_result *result);
void antfly_scan_hash_result_free(antfly_scan_hash_result *result);

antfly_error_code antfly_db_batch(
    antfly_db *db,
    const antfly_write_intent *writes,
    size_t write_count,
    const antfly_version_predicate *predicates,
    size_t predicate_count,
    uint64_t timestamp_ns,
    uint8_t sync_level
);
antfly_error_code antfly_db_batch_json(
    antfly_db *db,
    antfly_slice request_json,
    antfly_buffer *out
);
antfly_error_code antfly_db_begin_transaction_with_id(
    antfly_db *db,
    const uint8_t (*txn_id)[16],
    uint64_t timestamp_ns,
    const antfly_slice *participants,
    size_t participant_count
);
antfly_error_code antfly_db_write_transaction(
    antfly_db *db,
    const uint8_t (*txn_id)[16],
    const antfly_write_intent *writes,
    size_t write_count,
    const antfly_version_predicate *predicates,
    size_t predicate_count
);
antfly_error_code antfly_db_resolve_intents(
    antfly_db *db,
    const uint8_t (*txn_id)[16],
    uint8_t status,
    uint64_t commit_version
);
antfly_error_code antfly_db_get_transaction_status(
    antfly_db *db,
    const uint8_t (*txn_id)[16],
    uint8_t *out_status
);
antfly_error_code antfly_db_get_commit_version(
    antfly_db *db,
    const uint8_t (*txn_id)[16],
    uint64_t *out_commit_version
);
antfly_error_code antfly_db_get_timestamp(antfly_db *db, antfly_slice key, uint64_t *out_timestamp);
antfly_error_code antfly_db_lookup_json(antfly_db *db, antfly_slice key, antfly_buffer *out);
antfly_error_code antfly_db_get_raw(antfly_db *db, antfly_slice key, antfly_buffer *out);
antfly_error_code antfly_db_get_schema_json(antfly_db *db, antfly_buffer *out);
antfly_error_code antfly_db_set_schema_json(antfly_db *db, antfly_slice schema_json);
antfly_error_code antfly_db_run_until_idle(antfly_db *db);
antfly_error_code antfly_db_run_until_idle_json(antfly_db *db, antfly_buffer *out);
antfly_error_code antfly_db_pending_work_stats_json(antfly_db *db, antfly_buffer *out);
antfly_error_code antfly_db_replay_generated_enrichments_json(antfly_db *db, antfly_buffer *out);
antfly_error_code antfly_db_list_indexes_json(antfly_db *db, antfly_buffer *out);
antfly_error_code antfly_db_add_index_json(antfly_db *db, antfly_slice config_json);
antfly_error_code antfly_db_delete_index(antfly_db *db, antfly_slice name, bool *out_deleted);
antfly_error_code antfly_db_list_enrichments_json(antfly_db *db, antfly_buffer *out);
antfly_error_code antfly_db_add_enrichment_json(antfly_db *db, antfly_slice config_json);
antfly_error_code antfly_db_delete_enrichment(
    antfly_db *db,
    antfly_slice kind,
    antfly_slice name,
    bool *out_deleted
);
antfly_error_code antfly_db_scan_json(antfly_db *db, antfly_slice request_json, antfly_buffer *out);
antfly_error_code antfly_db_scan_hashes(
    antfly_db *db,
    antfly_slice request_json,
    antfly_scan_hash_result *out_result
);
antfly_error_code antfly_db_stats_json(antfly_db *db, antfly_buffer *out);
antfly_error_code antfly_db_search_json(antfly_db *db, antfly_slice request_json, antfly_buffer *out);
antfly_error_code antfly_db_search_dense(
    antfly_db *db,
    antfly_slice index_name,
    const float *vector_ptr,
    size_t vector_len,
    uint32_t k,
    uint32_t limit,
    uint32_t offset,
    antfly_packed_dense_search_result *out_result
);
antfly_error_code antfly_db_search_dense_profile(
    antfly_db *db,
    antfly_slice index_name,
    const float *vector_ptr,
    size_t vector_len,
    uint32_t k,
    uint32_t limit,
    uint32_t offset,
    antfly_dense_search_profile *out_profile
);
antfly_error_code antfly_db_search_dense_wire(
    antfly_db *db,
    antfly_slice request_buf,
    antfly_buffer *out
);
antfly_error_code antfly_db_search_dense_wire_profile(
    antfly_db *db,
    antfly_slice request_buf,
    antfly_buffer *out,
    antfly_dense_wire_search_profile *out_profile
);
antfly_error_code antfly_db_search_text_match(
    antfly_db *db,
    antfly_slice index_name,
    antfly_slice field,
    antfly_slice text,
    uint32_t limit,
    uint32_t offset,
    antfly_dense_search_result *out_result
);
antfly_error_code antfly_db_search_text_match_wire(antfly_db *db, antfly_slice request_buf, antfly_buffer *out);
antfly_error_code antfly_db_search_text_term_wire(antfly_db *db, antfly_slice request_buf, antfly_buffer *out);
antfly_error_code antfly_db_search_text_match_phrase_wire(antfly_db *db, antfly_slice request_buf, antfly_buffer *out);
antfly_error_code antfly_db_search_hits_json(
    antfly_db *db,
    antfly_slice request_json,
    antfly_dense_search_result *out_result
);
antfly_error_code antfly_db_aggregate_hits_json(antfly_db *db, antfly_slice request_json, antfly_buffer *out);
antfly_error_code antfly_db_lookup_artifact_json(antfly_db *db, antfly_slice artifact_id_b64, antfly_buffer *out);
antfly_error_code antfly_decode_artifact_id_json(antfly_slice artifact_id_b64, antfly_buffer *out);
antfly_error_code antfly_db_extract_enrichments_json(antfly_db *db, antfly_slice request_json, antfly_buffer *out);
antfly_error_code antfly_db_compute_enrichments_json(antfly_db *db, antfly_slice request_json, antfly_buffer *out);

/* `direction` values for antfly_db_get_edges_json and
 * antfly_db_get_neighbors_json. */
#define ANTFLY_GRAPH_DIRECTION_OUT 0u
#define ANTFLY_GRAPH_DIRECTION_IN 1u
#define ANTFLY_GRAPH_DIRECTION_BOTH 2u
antfly_error_code antfly_db_get_edges_json(
    antfly_db *db,
    antfly_slice index_name,
    antfly_slice key,
    antfly_slice edge_type,
    uint8_t direction,
    antfly_buffer *out
);
antfly_error_code antfly_db_traverse_edges_json(antfly_db *db, antfly_slice request_json, antfly_buffer *out);
antfly_error_code antfly_db_execute_graph_queries_json(antfly_db *db, antfly_slice request_json, antfly_buffer *out);
antfly_error_code antfly_db_get_neighbors_json(
    antfly_db *db,
    antfly_slice index_name,
    antfly_slice key,
    antfly_slice edge_type,
    uint8_t direction,
    antfly_buffer *out
);
antfly_error_code antfly_db_find_shortest_path_json(antfly_db *db, antfly_slice request_json, antfly_buffer *out);
antfly_error_code antfly_db_find_k_shortest_paths_json(antfly_db *db, antfly_slice request_json, antfly_buffer *out);
antfly_error_code antfly_db_match_pattern_json(antfly_db *db, antfly_slice request_json, antfly_buffer *out);

#ifdef __cplusplus
}
#endif

#endif
