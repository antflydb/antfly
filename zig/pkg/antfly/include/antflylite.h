// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license

#ifndef ANTFLY_LITE_H
#define ANTFLY_LITE_H

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
    ANTFLY_INTERNAL = 255,
} antfly_error_code;

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

typedef struct antfly_write_intent {
    antfly_slice key;
    antfly_slice value;
    bool is_delete;
} antfly_write_intent;

typedef struct antfly_version_predicate {
    antfly_slice key;
    uint64_t expected_version;
} antfly_version_predicate;

uint32_t antfly_lite_abi_version(void);

antfly_error_code antfly_lite_open(const char *path, void **out_handle);
antfly_error_code antfly_lite_open_hosted(const char *path, void **out_handle);
antfly_error_code antfly_lite_open_readonly(const char *path, void **out_handle);
antfly_error_code antfly_lite_open_status_only(const char *path, void **out_handle);

antfly_error_code antfly_lite_capabilities_json(void *handle, antfly_buffer *out);
antfly_error_code antfly_lite_backup(void *handle, antfly_buffer *out);
antfly_error_code antfly_lite_import_backup(void *handle, antfly_slice backup);
antfly_error_code antfly_lite_check_json(void *handle, antfly_buffer *out);
antfly_error_code antfly_lite_copy_stable_snapshot_json(
    void *handle,
    const char *dest_path,
    bool replace,
    antfly_buffer *out
);
antfly_error_code antfly_lite_vacuum_json(void *handle, antfly_buffer *out);

void antfly_db_close(void *handle);
void antfly_db_buffer_free(uint8_t *ptr, size_t len);

antfly_error_code antfly_db_batch(
    void *handle,
    const antfly_write_intent *writes,
    size_t write_count,
    const antfly_version_predicate *predicates,
    size_t predicate_count,
    uint64_t timestamp_ns,
    uint8_t sync_level
);
antfly_error_code antfly_db_begin_transaction_with_id(
    void *handle,
    const uint8_t (*txn_id)[16],
    uint64_t timestamp_ns,
    const antfly_slice *participants,
    size_t participant_count
);
antfly_error_code antfly_db_write_transaction(
    void *handle,
    const uint8_t (*txn_id)[16],
    const antfly_write_intent *writes,
    size_t write_count,
    const antfly_version_predicate *predicates,
    size_t predicate_count
);
antfly_error_code antfly_db_resolve_intents(
    void *handle,
    const uint8_t (*txn_id)[16],
    uint8_t status,
    uint64_t commit_version
);
antfly_error_code antfly_db_get_transaction_status(
    void *handle,
    const uint8_t (*txn_id)[16],
    uint8_t *out_status
);
antfly_error_code antfly_db_get_commit_version(
    void *handle,
    const uint8_t (*txn_id)[16],
    uint64_t *out_commit_version
);
antfly_error_code antfly_db_lookup_json(void *handle, antfly_slice key, antfly_buffer *out);
antfly_error_code antfly_db_get_raw(void *handle, antfly_slice key, antfly_buffer *out);
antfly_error_code antfly_db_get_schema_json(void *handle, antfly_buffer *out);
antfly_error_code antfly_db_set_schema_json(void *handle, antfly_slice schema_json);
antfly_error_code antfly_db_run_until_idle(void *handle);
antfly_error_code antfly_db_pending_work_stats_json(void *handle, antfly_buffer *out);
antfly_error_code antfly_db_list_indexes_json(void *handle, antfly_buffer *out);
antfly_error_code antfly_db_add_index_json(void *handle, antfly_slice config_json);
antfly_error_code antfly_db_delete_index(void *handle, antfly_slice name, bool *out_deleted);
antfly_error_code antfly_db_list_enrichments_json(void *handle, antfly_buffer *out);
antfly_error_code antfly_db_add_enrichment_json(void *handle, antfly_slice config_json);
antfly_error_code antfly_db_delete_enrichment(
    void *handle,
    antfly_slice kind,
    antfly_slice name,
    bool *out_deleted
);
antfly_error_code antfly_db_scan_json(void *handle, antfly_slice request_json, antfly_buffer *out);
antfly_error_code antfly_db_stats_json(void *handle, antfly_buffer *out);
antfly_error_code antfly_db_search_json(void *handle, antfly_slice request_json, antfly_buffer *out);
antfly_error_code antfly_db_lookup_artifact_json(void *handle, antfly_slice artifact_id_b64, antfly_buffer *out);
antfly_error_code antfly_db_decode_artifact_id_json(antfly_slice artifact_id_b64, antfly_buffer *out);
antfly_error_code antfly_db_extract_enrichments_json(void *handle, antfly_slice request_json, antfly_buffer *out);
antfly_error_code antfly_db_compute_enrichments_json(void *handle, antfly_slice request_json, antfly_buffer *out);

antfly_error_code antfly_db_get_edges_json(
    void *handle,
    antfly_slice index_name,
    antfly_slice key,
    antfly_slice edge_type,
    uint8_t direction,
    antfly_buffer *out
);
antfly_error_code antfly_db_traverse_edges_json(void *handle, antfly_slice request_json, antfly_buffer *out);
antfly_error_code antfly_db_get_neighbors_json(
    void *handle,
    antfly_slice index_name,
    antfly_slice key,
    antfly_slice edge_type,
    uint8_t direction,
    antfly_buffer *out
);
antfly_error_code antfly_db_find_shortest_path_json(void *handle, antfly_slice request_json, antfly_buffer *out);
antfly_error_code antfly_db_find_k_shortest_paths_json(void *handle, antfly_slice request_json, antfly_buffer *out);
antfly_error_code antfly_db_match_pattern_json(void *handle, antfly_slice request_json, antfly_buffer *out);

#ifdef __cplusplus
}
#endif

#endif
