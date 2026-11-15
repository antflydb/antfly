// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Stable error transport for independently generated runtime archives.
//!
//! Zig error integers and error-union layouts are compilation-local.  Runtime
//! archives therefore exchange only these explicit C-layout values.  `Code`
//! carries the language-neutral behavior class; `Detail` preserves the small
//! set of domain outcomes that callers intentionally distinguish.

const std = @import("std");

// The merged graph/document detail inventories share the original prefix and
// append both extensions under one new independently compiled boundary.
pub const abi_version: u32 = 2;

test "proved aborted graph policy denial retains its stable boundary identity" {
    const value = statusFromError(error.TransactionPrepareAbortedRowPolicyDenied);
    try std.testing.expectEqual(@backingInt(Code.forbidden), value.code);
    try std.testing.expectEqual(error.TransactionPrepareAbortedRowPolicyDenied, errorFromStatus(value));
}

test "OCR truncation preserves terminal identity across runtime callbacks" {
    const transported = statusFromError(error.ReadOutputTruncated);
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), transported.code);
    try std.testing.expectEqual(error.ReadOutputTruncated, errorFromStatus(transported));
    try std.testing.expect(errorHasStableDetail(error.ReadOutputTruncated));
}

test "FK publication progress and corrupt retirement proofs retain distinct boundary semantics" {
    const pending = statusFromError(error.GenerationAdmissionAcknowledgementPending);
    const changed = statusFromError(error.GenerationRetirementChanged);
    const corrupted = statusFromError(error.InvalidRetirementSummary);
    try std.testing.expectEqual(@backingInt(Code.retryable), pending.code);
    try std.testing.expectEqual(@backingInt(Code.conflict), changed.code);
    try std.testing.expectEqual(@backingInt(Code.corrupt), corrupted.code);
    try std.testing.expect(pending.detail != changed.detail and changed.detail != corrupted.detail);
    const publication_changed = statusFromError(error.GenerationPublicationChanged);
    try std.testing.expectEqual(error.GenerationPublicationChanged, errorFromStatus(publication_changed));
    const duplicate_child = statusFromError(error.InitialChildProvisionAlreadyCommitted);
    try std.testing.expectEqual(error.InitialChildProvisionAlreadyCommitted, errorFromStatus(duplicate_child));
}

test "document extraction failures retain terminal identity across callbacks" {
    for ([_]anyerror{ error.DocumentExtractionWorkingSetTooLarge, error.DocumentArtifactPublicationFailed }) |err| {
        const transported = statusFromError(err);
        try std.testing.expect(transported.code != @backingInt(Code.retryable));
        try std.testing.expectEqual(err, errorFromStatus(transported));
    }
}

test "metadata head races remain retryable across runtime callbacks" {
    const transported = statusFromError(error.MetadataSnapshotHeadMismatch);
    try std.testing.expectEqual(@backingInt(Code.retryable), transported.code);
    try std.testing.expectEqual(error.MetadataSnapshotHeadMismatch, errorFromStatus(transported));
    try std.testing.expect(errorHasStableDetail(error.MetadataSnapshotHeadMismatch));
}

test "hierarchy cursor conflicts retain identity across runtime callbacks" {
    const transported = statusFromError(error.HierarchyCursorStale);
    try std.testing.expectEqual(@backingInt(Code.conflict), transported.code);
    try std.testing.expectEqual(error.HierarchyCursorStale, errorFromStatus(transported));
    try std.testing.expect(errorHasStableDetail(error.HierarchyCursorStale));
}

pub const Code = enum(c_int) {
    ok = 0,
    out_of_memory,
    invalid_argument,
    not_found,
    already_exists,
    conflict,
    unauthorized,
    forbidden,
    unavailable,
    retryable,
    timeout,
    cancelled,
    unsupported,
    corrupt,
    internal,
};

pub const Detail = enum(c_int) {
    // Preserve published main identities; append unreleased SQL identities.
    none = 0,
    out_of_memory = 1,
    invalid_argument = 2,
    invalid_arguments = 3,
    invalid_request = 4,
    invalid_query_request = 5,
    invalid_filter_query_request = 6,
    invalid_exclusion_query_request = 7,
    invalid_batch_request = 8,
    invalid_create_table_request = 9,
    unsupported_create_table_request = 10,
    invalid_schema_update_request = 11,
    invalid_manifest = 12,
    invalid_table_file = 13,
    invalid_txn_record = 14,
    not_found = 15,
    file_not_found = 16,
    table_not_found = 17,
    table_already_exists = 18,
    index_not_found = 19,
    model_not_found = 20,
    secret_not_found = 21,
    user_not_found = 22,
    unknown_group = 23,
    txn_not_found = 24,
    conflict = 25,
    decision_conflict = 26,
    table_transition_active = 27,
    extension_owned_object = 28,
    restore_intent_conflict = 29,
    unauthorized = 30,
    forbidden = 31,
    unavailable = 32,
    write_unavailable = 33,
    read_unavailable = 34,
    read_requires_primary = 35,
    storage_read_temporarily_unavailable = 36,
    resource_request_too_large = 37,
    resource_temporarily_unavailable = 38,
    restore_job_persistence_unavailable = 39,
    backpressured = 40,
    dense_repair_backpressure = 41,
    outcome_unknown = 42,
    commit_propagation_incomplete = 43,
    commit_decision_unknown = 44,
    committed_pending = 45,
    write_outcome_unknown = 46,
    doc_identity_unavailable = 47,
    ha_read_only_standby = 48,
    ha_promoted_standby_requires_primary_open = 49,
    ha_fenced_primary = 50,
    internal_failure = 51,
    not_leader = 52,
    topology_changed = 53,
    identity_read_generation_changed = 54,
    doc_identity_namespace_mismatch = 55,
    table_generation_changed = 56,
    generation_durability_uncertain = 57,
    index_rebuilding = 58,
    table_visibility_timeout = 59,
    writer_locked = 60,
    lsm_root_writer_already_open = 61,
    unexpected_http_status = 62,
    method_not_allowed = 63,
    unsupported = 64,
    unsupported_operation = 65,
    unsupported_query_request = 66,
    unsupported_filter_query_request = 67,
    unsupported_exclusion_query_request = 68,
    unsupported_sync_level = 69,
    unsupported_exact_sort = 70,
    unsupported_version = 71,
    timeout = 72,
    connection_timeout = 73,
    connection_timed_out = 74,
    cancelled = 75,
    canceled = 76,
    connection_refused = 77,
    connection_reset_by_peer = 78,
    network_unreachable = 79,
    host_unreachable = 80,
    temporary_name_server_failure = 81,
    name_server_failure = 82,
    query_candidate_budget_exceeded = 83,
    query_embedding_input_too_large = 84,
    query_embedding_overloaded = 85,
    embed_rate_limited = 86,
    embed_transient_failure = 87,
    embed_upstream_failure = 88,
    embedding_probe_unavailable = 89,
    invalid_embedding_response = 90,
    invalid_embedding_dimensions = 91,
    backup_already_exists = 92,
    backup_manifest_too_large = 93,
    backup_integrity_failure = 94,
    backup_artifact_integrity_mismatch = 95,
    backup_repository_busy = 96,
    invalid_backup_request = 97,
    restore_durability_pending = 98,
    restore_durability_confirmed = 99,
    corrupt_input = 100,
    corrupted = 101,
    corrupt_restore_job_store = 102,
    table_block_checksum_mismatch = 103,
    value_too_long = 104,
    unsupported_platform = 105,
    unsupported_transform_operation = 106,
    ha_read_requires_primary = 107,
    ha_read_wait_for_apply = 108,
    ha_read_wait_for_metadata = 109,
    persistent_descriptor_admission_exhausted = 110,
    invalid_graph_edges = 111,
    invalid_table_index_metadata = 112,
    transaction_begin_failed = 113,
    abort_decision_not_durable = 114,
    commit_visibility_not_satisfied = 115,
    leader_unavailable = 116,
    raft_batch_write_outcome_unknown = 117,
    raft_batch_write_partial_outcome = 118,
    enrichment_retry_in_progress = 119,
    resource_budget_exceeded = 120,
    text_merge_backpressure_timeout = 121,
    text_merge_backpressure_unavailable = 122,
    text_merge_runtime_shutdown = 123,
    background_owner_closing = 124,
    invalid_enrichment_config = 125,
    restore_identity_mismatch = 126,
    table_read_churn = 127,
    unsupported_multi_range_table = 128,
    proposal_dropped = 129,
    leader_transfer_in_progress = 130,
    metadata_linearizable_read_timeout = 131,
    reconcile_lease_not_held = 132,
    enrichment_not_found = 133,
    invalid_extension_enrichment = 134,
    conflicting_enrichment_config = 135,
    invalid_create_index_request = 136,
    unsupported_backup_migration_state = 137,
    unsupported_backup_format = 138,
    extension_already_installed = 139,
    extension_not_installed = 140,
    extension_disabled = 141,
    extension_lifecycle_busy = 142,
    extension_visibility_timeout = 143,
    dependent_extension_exists = 144,
    required_extension_not_installed = 145,
    missing_extension_package = 146,
    package_not_found = 147,
    package_version_mismatch = 148,
    unsupported_package_kind = 149,
    invalid_extension_shape = 150,
    unsupported_extension_scope = 151,
    invalid_extension_lifecycle_request = 152,
    restore_job_fenced = 153,
    restore_job_capacity_exceeded = 154,
    restore_job_record_too_large = 155,
    too_many_restore_tables = 156,
    duplicate_restore_table_name = 157,
    idempotency_conflict = 158,
    invalid_idempotency_key = 159,
    async_restore_unavailable = 160,
    background_runtime_unavailable = 161,
    backup_staging_unavailable = 162,
    backup_artifact_missing = 163,
    backup_integrity_missing = 164,
    backup_attempt_lease_lost = 165,
    background_owner_closed = 166,
    restore_scheduling_capacity = 167,
    restore_wait_interrupted = 168,
    api_key_expired = 169,
    api_key_invalid = 170,
    api_key_not_found = 171,
    auth_row_filter_no_match = 172,
    backup_maintenance_queue_full = 173,
    backup_maintenance_unavailable = 174,
    connection_not_found = 175,
    embed_request_failed = 176,
    empty_embedding_response = 177,
    empty_name = 178,
    end_of_stream = 179,
    foreign_query_failed = 180,
    invalid_api_key_request = 181,
    invalid_api_server_config = 182,
    invalid_ard_agents_request = 183,
    invalid_ard_search_request = 184,
    invalid_backup_id = 185,
    invalid_backup_list_limit = 186,
    invalid_character = 187,
    invalid_create_user_request = 188,
    invalid_detail = 189,
    invalid_duration = 190,
    invalid_enrichment_request = 191,
    invalid_eval_request = 192,
    invalid_identifier = 193,
    invalid_index_request = 194,
    invalid_job_id = 195,
    invalid_json_object = 196,
    invalid_linear_merge_request = 197,
    invalid_metadata = 198,
    invalid_password = 199,
    invalid_password_update_request = 200,
    invalid_path_parameter = 201,
    invalid_permission_type = 202,
    invalid_query_builder_request = 203,
    invalid_read_consistency = 204,
    invalid_resource_type = 205,
    invalid_restore_job_list_cursor = 206,
    invalid_restore_job_list_limit = 207,
    invalid_restore_job_list_phase = 208,
    invalid_restore_job_list_scope = 209,
    invalid_retrieval_agent_request = 210,
    invalid_role = 211,
    invalid_secret_key = 212,
    invalid_transaction_commit_request = 213,
    invalid_transaction_session_record = 214,
    maintenance_busy = 215,
    maintenance_history_full = 216,
    maintenance_job_id_exhausted = 217,
    member_table_outside_scope = 218,
    missing_field = 219,
    missing_location = 220,
    missing_resource = 221,
    missing_resource_type = 222,
    missing_role = 223,
    no_tables = 224,
    path_already_exists = 225,
    privilege_escalation = 226,
    probe_unavailable = 227,
    role_not_found = 228,
    row_filter_not_found = 229,
    savepoint_limit_exceeded = 230,
    session_lease_lost = 231,
    session_limit_exceeded = 232,
    session_record_too_large = 233,
    stale_index_repair_control = 234,
    syntax_error = 235,
    too_many_runtime_statuses = 236,
    transaction_commit_request_mismatch = 237,
    transaction_commit_sealed = 238,
    tree_root_set_too_large = 239,
    unrequested_capability_grant = 240,
    unsupported_aggregate = 241,
    unsupported_backup_location = 242,
    unsupported_manifest_api_version = 243,
    unsupported_object_kind_for_v1 = 244,
    unsupported_retrieval_agent_request = 245,
    unsupported_source_kind = 246,
    update_path_not_found = 247,
    user_exists = 248,
    queue_full = 249,
    resource_limit_exceeded = 250,
    body_too_large = 251,
    body_capacity_exceeded = 252,
    body_read_failed = 253,
    inference_provider_failure = 254,
    kernel_jit_required_dynamic_load = 255,
    unexpected_token = 256,
    enrichment_wait_canceled = 257,
    enrichment_wait_timeout = 258,
    enrichment_worker_failed = 259,
    ha_sync_commit_would_block = 260,
    ha_sync_commit_wait_limit_exceeded = 261,
    ha_sync_commit_wait_missing_context = 262,
    ha_sync_commit_wait_standby_not_in_policy = 263,
    deadline_exceeded = 264,
    pre_decision_deadline_exceeded = 265,
    graph_distinct_budget_exceeded = 266,
    graph_anchor_filter_requires_index = 267,
    graph_match_operation_limit_exceeded = 268,
    graph_cross_range_mode_unsupported = 269,
    graph_work_budget_exceeded = 270,
    graph_min_weight_domain_violation = 271,
    graph_max_weight_domain_violation = 272,
    graph_path_weight_overflow = 273,
    metadata_mutation_outcome_unknown = 274,
    missing_embedding_artifact_producer = 275,
    invalid_embedding_artifact_producer = 276,
    embedding_artifact_dimension_required = 277,
    conflicting_embedding_artifact_dimensions = 278,
    native_backup_repair_state_not_quiescent = 279,
    native_backup_projection_not_quiescent = 280,
    native_backup_projection_repair_failed = 281,
    native_backup_projection_repair_paused = 282,
    reranker_candidate_limit_exceeded = 283,
    rerank_rate_limited = 284,
    rerank_transient_failure = 285,
    rerank_upstream_failure = 286,
    invalid_rate_limit_policy = 287,
    conflicting_rate_limit_policy = 288,
    provider_token_budget_exceeded = 289,
    provider_quota_registry_full = 290,
    unsupported_media_token_budget = 291,
    unsupported_local_rate_limit = 292,
    incompatible_model = 293,
    unsupported_generator_provider = 294,
    generate_request_failed = 295,
    generation_rate_limit = 296,
    unsupported_tensor_type = 297,
    generation_capacity_unavailable = 298,
    schema_in_use = 299,
    transaction_too_large = 300,
    invalid_table_storage_settings = 301,
    vector_store_requires_local_single_shard_table = 302,
    vector_store_requires_empty_table = 303,
    immutable_table_storage_settings = 304,
    vector_store_lifecycle_unsupported = 305,
    vector_store_reference_format_required = 306,
    backup_outcome_ambiguous = 307,
    metadata_mutation_not_applied = 308,
    graph_metric_action_partial_outcome = 309,
    index_generation_mismatch = 310,
    generation_transition_active = 311,
    storage_busy = 312,
    storage_kernel_failure = 313,
    invalid_vector_migration_budget = 314,
    invalid_vector_migration_id = 315,
    invalid_vector_migration_state = 316,
    unsupported_vector_migration_direction = 317,
    unsupported_vector_migration_version = 318,
    vector_migration_active = 319,
    vector_migration_already_exists = 320,
    vector_migration_already_published = 321,
    vector_migration_configuration_changed = 322,
    vector_migration_coverage_mismatch = 323,
    vector_migration_disk_reserve = 324,
    vector_migration_idempotency_conflict = 325,
    vector_migration_identity_mismatch = 326,
    vector_migration_inline_payload_remains = 327,
    vector_migration_not_found = 328,
    vector_migration_not_ready = 329,
    vector_migration_read_epoch_changed = 330,
    vector_migration_recovery_required = 331,
    vector_migration_row_exceeds_budget = 332,
    vector_migration_temporary_budget_exceeded = 333,
    vector_migration_offline_admission = 334,
    vector_migration_catalog_in_use = 335,
    vector_migration_copy_mismatch = 336,
    vector_migration_unsupported_file = 337,
    vector_store_requires_offline_command = 338,
    read_index_timeout = 339,
    incomplete_published_snapshot = 340,
    distributed_query_unavailable = 341,
    database_not_found = 342,
    namespace_not_found = 343,
    tablespace_not_found = 344,
    catalog_not_found = 345,
    catalog_already_exists = 346,
    catalog_generation_changed = 347,
    tablespace_in_use = 348,
    namespace_not_empty = 349,
    database_not_empty = 350,
    protected_catalog_resource = 351,
    invalid_catalog_name = 352,
    invalid_catalog_mutation = 353,
    invalid_tablespace_location = 354,
    invalid_tablespace_placement_policy = 355,
    catalog_command_too_large = 356,
    invalid_catalog_record = 357,
    catalog_id_exhausted = 358,
    table_topology_protocol_upgrade_required = 359,
    ha_seed_snapshot_runtime_busy = 360,
    ha_seed_capture_already_in_progress = 361,
    storage_kernel_owner_unavailable = 362,
    catalog_routing_snapshot_timeout = 363,
    catalog_routing_unavailable = 364,
    catalog_projection_refresh_required = 365,
    metadata_incarnation_unavailable = 366,
    invalid_metadata_incarnation = 367,
    metadata_incarnation_mismatch = 368,
    metadata_proposal_superseded = 369,
    metadata_proposal_apply_timeout = 370,
    backup_cohort_already_committed = 371,
    backup_cohort_cancelled = 372,
    backup_cohort_changed = 373,
    backup_cohort_fence_lost = 374,
    backup_seal_mismatch = 375,
    backup_seal_source_changed = 376,
    restore_staging_canceled = 377,
    restore_staging_progress_changed = 378,
    restore_staging_scope_changed = 379,
    integrity_topology_changed = 380,
    integrity_topology_completed = 381,
    integrity_topology_fence_missing = 382,
    integrity_catalog_changed = 383,
    integrity_catalog_incarnation_mismatch = 384,
    constraint_activation_changed = 385,
    constraint_activation_owner_changed = 386,
    constraint_retirement_changed = 387,
    foreign_key_parent_missing = 388,
    foreign_key_referenced = 389,
    unique_constraint_violation = 390,
    prepared_generation_changed = 391,
    prepared_read_set_changed = 392,
    foreign_key_action_mismatch = 393,
    foreign_key_action_not_validated = 394,
    constraint_activation_failed = 395,
    foreign_key_action_failed = 396,
    restore_staging_target_not_empty = 397,
    backup_pin_source_unavailable = 398,
    restore_source_durability_uncertain = 399,
    restore_staging_in_progress = 400,
    restore_validation_pending = 401,
    integrity_topology_busy = 402,
    transaction_topology_busy = 403,
    relational_topology_protocol_upgrade_required = 404,
    integrity_catalog_unavailable = 405,
    constraint_retirement_in_progress = 406,
    constraint_activation_in_progress = 407,
    foreign_key_action_in_progress = 408,
    coordinated_ttl_backpressure = 409,
    metadata_capability_unavailable = 410,
    backup_cohort_too_large = 411,
    incomplete_backup_cohort = 412,
    invalid_backup_cohort = 413,
    backup_seal_inventory_too_large = 414,
    invalid_backup_seal = 415,
    restore_dependency_missing = 416,
    restore_source_proof_missing = 417,
    invalid_restore_source_checkpoint = 418,
    invalid_restore_staging = 419,
    invalid_restore_staging_command = 420,
    invalid_restore_staging_record = 421,
    invalid_integrity_topology_fence = 422,
    invalid_metadata_ha_effect = 423,
    invalid_metadata_ha_chunk = 424,
    metadata_ha_checkpoint_too_large = 425,
    metadata_ha_effect_too_large = 426,
    invalid_constraint_activation_command = 427,
    invalid_constraint_retirement_command = 428,
    key_out_of_range = 429,
    invalid_integrity_command = 430,
    invalid_integrity_operation = 431,
    invalid_integrity_continuation = 432,
    integrity_claim_guard_required = 433,
    integrity_action_job_required = 434,
    invalid_relational_rows_request = 435,
    constraint_retirement_required = 436,
    invalid_integrity_key = 437,
    integrity_record_too_large = 438,
    invalid_range = 439,
    backup_seal_backend_unsupported = 440,
    relational_topology_migration_unsupported = 441,
    foreign_key_coordination_required = 442,
    constraint_not_found = 443,
    integrity_topology_epoch_exhausted = 444,
    backup_seal_released = 445,
    integrity_address_mismatch = 446,
    integrity_handoff_collision = 447,
    integrity_handoff_destination_reset_required = 448,
    integrity_handoff_incomplete = 449,
    integrity_handoff_missing = 450,
    integrity_handoff_sequence_changed = 451,
    missing_integrity_binding = 452,
    missing_integrity_catalog = 453,
    integrity_catalog_too_large = 454,
    integrity_handoff_too_large = 455,
    invalid_integrity_address = 456,
    invalid_integrity_budget = 457,
    invalid_integrity_catalog = 458,
    invalid_integrity_definition = 459,
    invalid_integrity_record = 460,
    coordinated_constraints_require_table_identity = 461,
    integrity_retirement_backlog_full = 462,
    restore_projection_catch_up_pending = 463,
    integrity_checksum_mismatch = 464,
    integrity_missing_companion = 465,
    coordinated_constraint_portable_backup_unsupported = 466,
    coordinated_constraint_restore_required = 467,
    coordinated_constraint_topology_unsupported = 468,
    integrity_generation_exhausted = 469,
    invalid_restore_terminal = 470,
    invalid_standalone_metadata_checkpoint = 471,
    seed_below_ha_replay_floor = 472,
    seed_metadata_topology_mismatch = 473,
    integrity_topology_cutover_required = 474,
    metadata_ha_binding_busy = 475,
    metadata_ha_outbox_pending = 476,
    metadata_ha_checkpoint_target_not_empty = 477,
    invalid_metadata_ha_checkpoint = 478,
    metadata_ha_migration_after_binding = 479,
    metadata_ha_incomplete_effect = 480,
    metadata_ha_source_changed = 481,
    metadata_ha_sequence_gap = 482,
    invalid_metadata_ha_effect_chunk = 483,
    ha_primary_not_configured = 484,
    relational_index_not_ready = 485,
    invalid_relational_index_bound = 486,
    relational_index_column_not_found = 487,
    unsupported_relational_index_column = 488,
    relational_rows_output_budget_exceeded = 489,
    relational_row_result_too_large = 490,
    relational_index_column_type_mismatch = 491,
    relational_table_required = 492,
    invalid_relational_index_forward_key = 493,
    invalid_online_source_command = 494,
    online_source_corrupt = 495,
    online_source_pin_missing = 496,
    online_source_pin_pending = 497,
    online_source_scope_changed = 498,
    invalid_source_snapshot = 499,
    source_snapshot_corrupt = 500,
    source_snapshot_cut_mismatch = 501,
    source_snapshot_incomplete = 502,
    source_snapshot_too_large = 503,
    source_copy_restore_unsupported = 504,
    invalid_merge_page = 505,
    merge_copy_fenced = 506,
    merge_page_chunk_required = 507,
    merge_page_incomplete = 508,
    merge_page_sequence_gap = 509,
    retained_effects_consumer_limit = 510,
    retained_effects_corrupt = 511,
    retained_effects_cursor_mismatch = 512,
    retained_effects_full = 513,
    retained_effects_identity_required = 514,
    retained_effects_namespace_mismatch = 515,
    backend_runtime_io_unavailable = 516,
    corrupt_raft_applied_entry = 517,
    unknown_schema_version = 518,
    invalid_retained_effects_admission = 519,
    retained_effects_fence_mismatch = 520,
    retained_effects_mixed_control = 521,
    retained_effects_transaction_failed = 522,
    transition_operation_busy = 523,
    unknown_split_runtime = 524,
    unknown_merge_runtime = 525,
    missing_split_runtime = 526,
    missing_merge_runtime = 527,
    metadata_snapshot_unavailable = 528,
    split_source_projection_not_ready = 529,
    split_source_projection_advanced = 530,
    durable_root_incarnation_unavailable = 531,
    auto_bulk_ingest_busy = 532,
    apply_store_group_retired = 533,
    apply_store_shutting_down = 534,
    group_leader_unavailable = 535,
    transition_operations_retired = 536,
    merge_transition_not_ready = 537,
    merge_receiver_projection_not_ready = 538,
    merge_source_projection_not_ready = 539,
    merge_source_projection_advanced = 540,
    restore_job_commit_not_applied = 541,
    coordinated_standalone_ha_metadata_required = 542,
    relational_rewrite_type_change = 543,
    relational_rewrite_column_drop = 544,
    relational_rewrite_requires_relational = 545,
    relational_rewrite_budget_exceeded = 546,
    relational_expression_overflow = 547,
    relational_expression_division_by_zero = 548,
    relational_expression_budget_exceeded = 549,
    invalid_relational_expression_input = 550,
    invalid_relational_generated_value = 551,
    generated_column_rewrite_required = 552,
    intent_conflict = 553,
    version_conflict = 554,
    merge_page_required = 555,
    invalid_response = 556,
    relational_index_key_too_large = 557,
    table_lifecycle_conflict = 558,
    http_connection_closing = 559,
    raft_batch_write_transport_outcome_unknown = 560,
    concurrency_unavailable = 561,
    sql_range_tracking_required = 562,
    sql_statement_snapshot_required = 563,
    invalid_range_tracking_state = 564,
    range_tracking_generation_exhausted = 565,
    row_policy_catalog_changed = 566,
    online_merge_artifact_catalog_uncoordinated = 567,
    artifact_catalog_drift = 568,
    invalid_artifact_catalog_command = 569,
    artifact_catalog_epoch_changed = 570,
    artifact_catalog_scope_changed = 571,
    online_merge_artifact_catalog_changed = 572,
    store_root_enrollment_changed = 573,
    invalid_store_root_enrollment = 574,
    initial_child_root_receipt_changed = 575,
    invalid_initial_fk_retirement_signature = 576,
    initial_fk_retirement_signing_key_unavailable = 577,
    initial_fk_retirement_reporter_changed = 578,
    initial_fk_retirement_work_changed = 579,
    initial_fk_retirement_publication_changed = 580,
    initial_fk_retirement_reservation_changed = 581,
    invalid_initial_fk_retirement_ack = 582,
    invalid_initial_fk_retirement_page = 583,
    initial_fk_retirement_root_changed = 584,
    replica_retirement_recovery_in_progress = 585,
    membership_change_fenced = 586,
    initial_child_publication_changed = 587,
    invalid_initial_child_publication = 588,
    initial_fk_retirement_proof_unavailable = 589,
    invalid_initial_fk_retirement_ticket = 590,
    invalid_initial_fk_retirement_intent = 591,
    initial_fk_retirement_path_changed = 592,
    artifact_catalog_corrupt = 593,
    artifact_catalog_epoch_exhausted = 594,
    online_merge_artifact_tails_unsupported = 595,
    setting_authority_unavailable = 596,
    invalid_generation_publication = 597,
    invalid_retirement_summary = 598,
    generation_admission_acknowledgement_pending = 599,
    generation_admission_changed = 600,
    generation_admission_pending = 601,
    generation_admission_revision_exhausted = 602,
    invalid_generation_admission = 603,
    generation_admission_activation_required = 604,
    generation_retirement_acknowledgement_pending = 605,
    generation_retirement_changed = 606,
    generation_retirement_handoff_required = 607,
    generation_retirement_pending = 608,
    generation_retirement_revision_exhausted = 609,
    invalid_generation_retirement = 610,
    initial_child_publication_missing = 611,
    initial_child_not_published = 612,
    invalid_control_receipt_position = 613,
    invalid_graph_transfer = 614,
    graph_generation_mismatch = 615,
    generation_publication_changed = 616,
    initial_child_provision_already_committed = 617,
    external_lake_read_only = 618,
    external_lake_snapshot_mismatch = 619,
    external_lake_schema_mismatch = 620,
    external_lake_schema_unavailable = 621,
    external_lake_field_identity_changed = 622,
    external_lake_object_changed = 623,
    empty_external_source_snapshot = 624,
    unsupported_external_lake_schema_type = 625,
    invalid_parquet_metadata = 626,
    invalid_parquet_footer = 627,
    invalid_parquet_footer_magic = 628,
    invalid_parquet_page = 629,
    unsupported_parquet_page = 630,
    parquet_page_too_large = 631,
    parquet_column_not_found = 632,
    unsupported_iceberg_schema_evolution = 633,
    unsupported_iceberg_delete_file = 634,
    invalid_sql_spill = 635,
    metric_not_ready = 636,
    metric_stale = 637,
    invalid_generated_tool_arguments = 638,
    sql_feature_not_supported = 639,
    catalog_publication_proof_pending = 640,
    warm_inference_server_unavailable = 641,
    unsupported_server_generate_option = 642,
    missing_inference_listener = 643,
    runtime_failed = 644,
    runtime_stopping = 645,
    runtime_already_ready = 646,
    document_extraction_working_set_too_large = 647,
    document_artifact_publication_failed = 648,
    crc_mismatch = 649,
    invalid_data = 650,
    mapped_artifact_unsupported = 651,
    immutable_read_source_unsupported = 652,
    directory_listing_unsupported = 653,
    unsupported_storage_lease = 654,
    durable_atomic_write_unsupported = 655,
    durable_atomic_rename_unsupported = 656,
    durable_file_sync_unsupported = 657,
    durable_directory_sync_unsupported = 658,
    stable_file_lease_unsupported = 659,
    read_only = 660,
    storage_closed = 661,
    cache_budget_exceeded = 662,
    buffer_too_small = 663,
    file_too_big = 664,
    invalid_scratch = 665,
    invalid_atomic_write_offset = 666,
    canceled_range_read = 667,
    source_visitor_failed = 668,
    mapped_artifact_unavailable = 669,
    read_scope_reset_unsupported = 670,
    read_snapshot_fork_unsupported = 671,
    file_busy = 672,
    would_block = 673,
    inference_worker_executable_not_configured = 674,
    inference_worker_unavailable = 675,
    model_chunk_audio_too_large = 676,
    model_chunk_chunk_output_too_large = 677,
    model_chunk_chunk_request_failed = 678,
    model_chunk_chunk_unit_too_large = 679,
    model_chunk_embedding_identity_mismatch = 680,
    model_chunk_empty_embedding_input = 681,
    model_chunk_empty_response = 682,
    model_chunk_empty_source = 683,
    model_chunk_empty_video = 684,
    model_chunk_immutable_version_required = 685,
    model_chunk_incompatible_chunk_embedding_target = 686,
    model_chunk_incomplete_chunk_coverage = 687,
    model_chunk_incomplete_video_picture = 688,
    model_chunk_inference_admission_queue_full = 689,
    model_chunk_inference_capabilities_stale = 690,
    model_chunk_invalid_audio_window = 691,
    model_chunk_invalid_chunk_continuation = 692,
    model_chunk_invalid_chunk_coordinates = 693,
    model_chunk_invalid_chunk_geometry = 694,
    model_chunk_invalid_chunk_model = 695,
    model_chunk_invalid_chunk_policy = 696,
    model_chunk_invalid_chunk_provider = 697,
    model_chunk_invalid_chunk_replay = 698,
    model_chunk_invalid_chunk_source = 699,
    model_chunk_invalid_chunker_config = 700,
    model_chunk_invalid_chunker_response = 701,
    model_chunk_invalid_embedding_output = 702,
    model_chunk_invalid_input_shape = 703,
    model_chunk_invalid_sampling_policy = 704,
    model_chunk_invalid_source_reference = 705,
    model_chunk_invalid_utf8 = 706,
    model_chunk_memory_budget_exceeded = 707,
    model_chunk_missing_attachment_envelope = 708,
    model_chunk_missing_chunk_stream_context = 709,
    model_chunk_model_aware_chunk_provider_required = 710,
    model_chunk_shared_admission_exceeded = 711,
    model_chunk_source_version_changed = 712,
    model_chunk_stale_chunk_continuation = 713,
    model_chunk_stream_too_long = 714,
    model_chunk_timestamp_overflow = 715,
    model_chunk_unknown_video_duration = 716,
    model_chunk_unresolved_chunk_source = 717,
    model_chunk_unsupported_audio_format = 718,
    model_chunk_unsupported_chunk_media_type = 719,
    model_chunk_working_budget = 720,
    model_chunk_dependency_budget = 721,
    model_chunk_coded_window_budget = 722,
    model_chunk_event_budget = 723,
    model_chunk_remote_bytes_budget = 724,
    model_chunk_remote_requests_budget = 725,
    model_chunk_invalid_limits = 726,
    model_chunk_streaming_profile = 727,
    model_chunk_incomplete_audio_stream = 728,
    decision_limit_exceeded = 729,
    grouped_source_owner_changed = 730,
    grouped_source_topology_unavailable = 731,
    apple_intelligence_provider_unavailable = 732,
    artifact_publication_pending = 733,
    chatgpt_billing_fallback_forbidden = 734,
    chatgpt_disabled = 735,
    chatgpt_interactive_only = 736,
    constrained_input_too_large = 737,
    decide_request_limit_exceeded = 738,
    enrichment_source_changed = 739,
    generated_extraction_limit_exceeded = 740,
    grouped_prior_context_limit_exceeded = 741,
    grouped_source_baseline_pending = 742,
    grouped_source_busy = 743,
    grouped_source_corrupt = 744,
    grouped_source_earlier_work_pending = 745,
    grouped_source_identity_conflict = 746,
    grouped_source_limit_exceeded = 747,
    grouped_source_member_limit_exceeded = 748,
    grouped_source_member_too_large = 749,
    grouped_source_partition_too_large = 750,
    grouped_source_retirement_pending = 751,
    invalid_constrained_generation_config = 752,
    invalid_constrained_output = 753,
    invalid_constrained_probabilities = 754,
    invalid_constrained_schema = 755,
    invalid_decide_request = 756,
    invalid_decision_output = 757,
    invalid_generated_extractor_config = 758,
    invalid_generator_config = 759,
    invalid_generator_tool_config = 760,
    invalid_grouped_prior_context = 761,
    invalid_grouped_source_command = 762,
    invalid_grouped_source_config = 763,
    invalid_grouped_source_member = 764,
    invalid_grouped_source_mutation = 765,
    invalid_grouped_source_query = 766,
    invalid_grouped_source_transfer = 767,
    invalid_structured_output = 768,
    missing_generator_credentials = 769,
    unsupported_apple_generation_options = 770,
    unsupported_constrained_output = 771,
    unsupported_constrained_probabilities = 772,
    unsupported_constrained_schema = 773,
    unsupported_generated_extraction_schema = 774,
    resolver_curation_conflict = 775,
    value_verification_requires_scan = 776,
    value_verification_limit_exceeded = 777,
    invalid_embedding_identity = 778,
    graph_start_identity_required = 779,
    metadata_snapshot_head_mismatch = 780,
    inference_endpoint_access_denied = 781,
    document_storage_not_configured = 782,
    document_run_busy = 783,
    document_run_exists = 784,
    document_run_expired = 785,
    document_run_lease_lost = 786,
    document_run_not_found = 787,
    document_run_retention_active = 788,
    document_run_terminal = 789,
    document_run_store_needs_recovery = 790,
    document_model_revision_mismatch = 791,
    document_budget_exhausted = 792,
    document_output_too_large = 793,
    document_stage_in_flight = 794,
    invalid_document_checkpoint = 795,
    too_many_active_jobs = 796,
    invalid_document_source = 797,
    unsupported_document_format = 798,
    document_source_limit_exceeded = 799,
    document_page_limit_exceeded = 800,
    document_pixel_limit_exceeded = 801,
    document_media_type_mismatch = 802,
    invalid_document_selection = 803,
    duplicate_document_source = 804,
    document_unresolved = 805,
    document_capability_mismatch = 806,
    unsupported_document_request_field = 807,
    remote_document_model_denied = 808,
    cloudflare_credential_missing = 809,
    invalid_cloudflare_config = 810,
    unsupported_cloudflare_model = 811,
    unsupported_cloudflare_schema = 812,
    cloudflare_authentication_failed = 813,
    invalid_extractor_response = 814,
    unsupported_extraction_schema_version = 815,
    invalid_inference_request = 816,
    document_artifact_not_found = 817,
    document_artifact_expired = 818,
    cloudflare_rate_limited = 819,
    inference_upstream_rate_limited = 820,
    inference_capacity_unavailable = 821,
    inference_transport_unavailable = 822,
    cloudflare_inference_failed = 823,
    invalid_cloudflare_response = 824,
    document_token_limit_exceeded = 825,
    hierarchy_cursor_stale = 826,
    read_transient_failure = 827,
    read_request_failed = 828,
    invalid_gcs_url = 829,
    gcs_connection_not_found_or_out_of_scope = 830,
    ambiguous_gcs_connection = 831,
    invalid_connection_credentials = 832,
    pioneer_credential_missing = 833,
    unsupported_pioneer_schema = 834,
    unsupported_pioneer_input = 835,
    standalone_replication_sources_unsupported = 836,
    ha_catalog_replication_sources_unsupported = 837,
    embed_admission_refused = 838,
    read_output_truncated = 839,
    invalid_document_extraction_config = 840,
    document_extraction_model_required = 841,
    conflicting_privacy_producer_options = 842,
    conflicting_privacy_producer_schema = 843,
    invalid_privacy_catalog = 844,
    invalid_privacy_categories = 845,
    invalid_privacy_category = 846,
    invalid_privacy_confidence = 847,
    invalid_privacy_document = 848,
    invalid_privacy_execution = 849,
    invalid_privacy_field_type = 850,
    invalid_privacy_fields = 851,
    invalid_privacy_offset_unit = 852,
    invalid_privacy_output = 853,
    invalid_privacy_output_cardinality = 854,
    invalid_privacy_producer = 855,
    invalid_privacy_receipt = 856,
    invalid_privacy_scan = 857,
    invalid_privacy_source = 858,
    invalid_privacy_span = 859,
    invalid_privacy_threshold = 860,
    invalid_privacy_token_geometry = 861,
    invalid_privacy_window_receipt = 862,
    privacy_embedding_producer_required = 863,
    privacy_evidence_mismatch = 864,
    privacy_external_embeddings_unsupported = 865,
    privacy_external_vectors_unsupported = 866,
    privacy_inspection_failed = 867,
    privacy_literal_credential = 868,
    privacy_managed_graph = 869,
    privacy_pending = 870,
    privacy_precondition_failed = 871,
    privacy_precondition_required = 872,
    privacy_producer_revision_mismatch = 873,
    privacy_producer_revision_required = 874,
    privacy_publication_pending = 875,
    privacy_read_fence_unavailable = 876,
    privacy_relational_storage_unsupported = 877,
    privacy_schema_exceeds_window = 878,
    privacy_stale = 879,
    privacy_synchronous_ingestion_required = 880,
    privacy_unknown_artifact_source = 881,
    privacy_unsupported_artifact_input = 882,
    privacy_unsupported_index_type = 883,
    privacy_unsupported_mutation = 884,
    privacy_unsupported_projection = 885,
    privacy_window_limit_exceeded = 886,
    privacy_window_token_limit_exceeded = 887,
    unknown_privacy_category = 888,
    sql_statement_read_unavailable = 889,
    unsupported_sql_execution = 890,
    cypher_aggregate_scope = 891,
    cypher_aggregate_wildcard = 892,
    cypher_ambiguous_search_table = 893,
    cypher_anchor_budget_exceeded = 894,
    cypher_create_requires_label = 895,
    cypher_deleted_entity = 896,
    cypher_derived_relationship_mutation = 897,
    cypher_division_by_zero = 898,
    cypher_duplicate_identity = 899,
    cypher_edge_budget_exceeded = 900,
    cypher_exists_mutation = 901,
    cypher_function_arity = 902,
    cypher_invalid_assignment = 903,
    cypher_invalid_backend_cursor = 904,
    cypher_invalid_backend_page = 905,
    cypher_invalid_backend_relationship = 906,
    cypher_invalid_create_relationship = 907,
    cypher_invalid_document = 908,
    cypher_invalid_grouping = 909,
    cypher_invalid_owner = 910,
    cypher_invalid_property_value = 911,
    cypher_invalid_range = 912,
    cypher_invalid_relationship = 913,
    cypher_invalid_relationship_metadata = 914,
    cypher_invalid_search_limit = 915,
    cypher_invalid_search_result = 916,
    cypher_invalid_shortest_path = 917,
    cypher_invalid_substring = 918,
    cypher_label_predicate_mismatch = 919,
    cypher_label_predicate_violation = 920,
    cypher_legacy_id_unavailable = 921,
    cypher_limit_exceeded = 922,
    cypher_memory_budget_exceeded = 923,
    cypher_mutations_unavailable = 924,
    cypher_node_has_relationships = 925,
    cypher_numeric_overflow = 926,
    cypher_observations_unavailable = 927,
    cypher_overlapping_owners = 928,
    cypher_owner_budget_exceeded = 929,
    cypher_path_budget_exceeded = 930,
    cypher_query_requires_return = 931,
    cypher_recursion_budget_exceeded = 932,
    cypher_regex_unavailable = 933,
    cypher_relationship_endpoint_mismatch = 934,
    cypher_remove_requires_property = 935,
    cypher_request_too_large = 936,
    cypher_response_too_large = 937,
    cypher_return_must_be_last = 938,
    cypher_return_predicate = 939,
    cypher_row_policy_unsupported = 940,
    cypher_search_after_write = 941,
    cypher_search_preparation_required = 942,
    cypher_search_table_outside_graph = 943,
    cypher_search_unavailable = 944,
    cypher_snapshot_not_ready = 945,
    cypher_snapshot_required = 946,
    cypher_state_budget_exceeded = 947,
    cypher_transaction_already_active = 948,
    cypher_transaction_conflict = 949,
    cypher_transaction_required = 950,
    cypher_type_mismatch = 951,
    cypher_union_column_mismatch = 952,
    cypher_value_depth_exceeded = 953,
    cypher_variable_conflict = 954,
    cypher_variable_type_conflict = 955,
    cypher_write_union_unsupported = 956,
    row_policy_unsupported = 957,
    row_policy_denied = 958,
    row_policy_authentication_required = 959,
    row_policy_authority_unavailable = 960,
    transaction_prepare_aborted_row_policy_denied = 961,
    retained_read_restart_required = 962,
    row_policy_mutation_unsupported = 963,
    cypher_invalid_regex = 964,
    schema_version_changed = 965,
    cypher_write_request_already_used = 966,
    invalid_cypher_parameters = 967,
    missing_cypher_parameter = 968,
    sql_write_capacity_unavailable = 969,
    invalid_external_source_inventory = 970,
    invalid_external_source_snapshot = 971,
    external_source_snapshot_too_large = 972,
    storage_owner_admission_pending = 973,
    reembed_budget_exceeded = 974,
    reembed_setup_retry = 975,
    unsupported_node_filter_query = 976,
    process_fd_quota_exceeded = 977,
    system_fd_quota_exceeded = 978,
    storage_descriptor_pressure = 979,
    input_output = 980,
    catalog_changed = 981,
    index_referenced_by_alias = 982,
    index_alias_graph_conflict = 983,
    index_alias_precondition_required = 984,
    index_alias_precondition_failed = 985,
    sql_index_already_exists = 986,
    sql_constraint_already_exists = 987,
    program_limit_exceeded = 988,
    catalog_occupancy_not_ready = 989,
    unsupported_native_index_owner = 990,
    invalid_private_index_claim_inventory = 991,
    invalid_private_index_claim_receipt = 992,
    private_index_owner_job_changed = 993,
    private_index_publication_canceled = 994,
    private_index_job_required = 995,
    private_index_publication_required = 996,
    invalid_private_index_owner_job = 997,
    invalid_private_index_inputs = 998,
    relational_index_catalog_schema_mismatch = 999,
    invalid_index_publication_candidate = 1000,
    immutable_relational_index_definition_conflict = 1001,
    private_index_cancellation_pending = 1002,
    private_index_cancellation_required = 1003,
    invalid_private_index_cancellation = 1004,
    invalid_private_index_native_receipt = 1005,
    invalid_relational_index_progress = 1006,
    missing_relational_index_definitions = 1007,
    index_owner_receipt_changed = 1008,
    invalid_index_owner_receipt = 1009,
    sql_invalid_power_argument = 1010,
    invalid_schema_reader_requirements = 1011,
    private_index_root_recovery_required = 1012,
    invalid_relational_index_budget = 1013,
    private_index_claim_backlog_full = 1014,
    partial_index_predicate_not_implied = 1015,
    relational_check_violation = 1016,
    check_constraint_validation_required = 1017,
    foreign_key_match_full_violation = 1018,
    invalid_schema_validation_job = 1019,
    invalid_schema_validation_page_command = 1020,
    invalid_schema_validation_token = 1021,
    schema_validation_canceled = 1022,
    schema_validation_cancellation_required = 1023,
    schema_validation_command_rejected = 1024,
    schema_validation_inputs_required = 1025,
    schema_validation_job_changed = 1026,
    schema_validation_job_required = 1027,
    schema_validation_not_ready = 1028,
    schema_validation_preparation_changed = 1029,
    schema_validation_publication_required = 1030,
    schema_validation_runtime_recovery_required = 1031,
    store_registration_required = 1032,
    catalog_publication_fence_unavailable = 1033,
    undefined_column = 1034,
    foreign_key_validation_required = 1035,
    materialized_view_read_only = 1036,
    raft_apply_writer_unavailable = 1037,
    sql_database_in_use = 1038,
    invalid_scope_retirement = 1039,
    scope_retirement_changed = 1040,
    sequence_admission_conflict = 1041,
    invalid_table_retirement_manifest = 1042,
    sql_program_limit_exceeded = 1043,
    snapshot_build_cancelled = 1044,
    catalog_generation_exhausted = 1045,
    invalid_sql_relation_persistence = 1046,
    sql_relation_owner_changed = 1047,
    sql_temporary_relation_not_owned = 1048,
    invalid_sql_temporary_namespace = 1049,
    sql_relation_persistence_foreign_key_mismatch = 1050,
    sql_relation_recovery_incomplete = 1051,
    sql_relation_owner_unavailable = 1052,
    invalid_sql_trigger_definition = 1053,
    invalid_sql_trigger_manifest = 1054,
    sql_trigger_context_required = 1055,
    sql_trigger_returned_no_value = 1056,
    sql_trigger_return_value_required = 1057,
    sql_trigger_declared_arguments = 1058,
    sql_trigger_sql_language = 1059,
    sql_trigger_coordination_required = 1060,
    invalid_generation_transaction = 1061,
    generation_transaction_changed = 1062,
    generation_transaction_decision_required = 1063,
    invalid_prepared_graph_effects = 1064,
    sql_prepared_durability_unavailable = 1065,
    invalid_participant = 1066,
    catalog_revision_exhausted = 1067,
    duplicate_sql_extension_export = 1068,
    duplicate_sql_extension_version = 1069,
    inconsistent_sql_extension_control = 1070,
    invalid_package_digest = 1071,
    invalid_sql_extension_artifact = 1072,
    invalid_sql_extension_control = 1073,
    invalid_sql_extension_dependency = 1074,
    invalid_sql_extension_record = 1075,
    invalid_sql_extension_runtime = 1076,
    invalid_sql_extension_package_mutation = 1077,
    duplicate_sql_extension_control = 1078,
    sql_dependent_extension_object = 1079,
    sql_duplicate_extension = 1080,
    sql_duplicate_function = 1081,
    sql_extension_control_conflict = 1082,
    sql_extension_dependency_cycle = 1083,
    sql_extension_promotion_nonce_conflict = 1084,
    sql_extension_schema_mismatch = 1085,
    sql_extension_unavailable = 1086,
    sql_extension_update_path_unavailable = 1087,
    sql_extension_version_unavailable = 1088,
    sql_function_return_type_changed = 1089,
    sql_required_extension_not_installed = 1090,
    sql_undefined_extension = 1091,
    unsupported_sql_extension_object = 1092,
    sql_extension_owned_object = 1093,
    duplicate_shape_name = 1094,
    generated_artifact_shape_required = 1095,
    no_supported_scopes = 1096,
    scope_must_not_name_table = 1097,
    unknown_shape_reference = 1098,
};

test "SQL relation persistence callback failures preserve exact identity and behavior" {
    const errors = [_]anyerror{ error.InvalidSqlRelationPersistence, error.SqlRelationOwnerChanged, error.SqlTemporaryRelationNotOwned, error.InvalidSqlTemporaryNamespace, error.SqlRelationPersistenceForeignKeyMismatch, error.SqlRelationRecoveryIncomplete, error.SqlRelationOwnerUnavailable };
    const codes = [_]Code{ .invalid_argument, .conflict, .forbidden, .invalid_argument, .invalid_argument, .unavailable, .unavailable };
    for (errors, codes, 1033..) |err, code, detail| {
        const wire = statusFromError(err);
        try std.testing.expectEqual(@backingInt(code), wire.code);
        try std.testing.expectEqual(@as(c_int, @intCast(detail)), wire.detail);
        try std.testing.expectEqual(err, errorFromStatus(wire));
    }
}

test "partial index proof rejection preserves exact callback error identity" {
    const wire = statusFromError(error.PartialIndexPredicateNotImplied);
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), wire.code);
    try std.testing.expectEqual(error.PartialIndexPredicateNotImplied, errorFromStatus(wire));
}

test "schema reader requirement validation preserves exact callback error identity" {
    const wire = statusFromError(error.InvalidSchemaReaderRequirements);
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), wire.code);
    try std.testing.expectEqual(error.InvalidSchemaReaderRequirements, errorFromStatus(wire));
}

test "native index claim failures preserve typed callback boundary identities" {
    const cases = .{
        .{ error.UnsupportedNativeIndexOwner, Code.unsupported },
        .{ error.InvalidPrivateIndexClaimInventory, Code.invalid_argument },
        .{ error.InvalidPrivateIndexClaimReceipt, Code.corrupt },
        .{ error.PrivateIndexOwnerJobChanged, Code.conflict },
        .{ error.PrivateIndexPublicationCanceled, Code.conflict },
        .{ error.PrivateIndexJobRequired, Code.retryable },
        .{ error.PrivateIndexRootRecoveryRequired, Code.retryable },
        .{ error.InvalidRelationalIndexBudget, Code.invalid_argument },
        .{ error.PrivateIndexClaimBacklogFull, Code.retryable },
        .{ error.PrivateIndexPublicationRequired, Code.retryable },
        .{ error.InvalidPrivateIndexOwnerJob, Code.corrupt },
        .{ error.InvalidPrivateIndexInputs, Code.corrupt },
        .{ error.RelationalIndexCatalogSchemaMismatch, Code.conflict },
        .{ error.InvalidIndexPublicationCandidate, Code.invalid_argument },
        .{ error.ImmutableRelationalIndexDefinitionConflict, Code.conflict },
        .{ error.PrivateIndexCancellationPending, Code.retryable },
        .{ error.PrivateIndexCancellationRequired, Code.retryable },
        .{ error.InvalidPrivateIndexCancellation, Code.corrupt },
        .{ error.InvalidPrivateIndexNativeReceipt, Code.corrupt },
        .{ error.InvalidRelationalIndexProgress, Code.corrupt },
        .{ error.MissingRelationalIndexDefinitions, Code.corrupt },
        .{ error.IndexOwnerReceiptChanged, Code.conflict },
        .{ error.InvalidIndexOwnerReceipt, Code.invalid_argument },
    };
    inline for (cases) |entry| {
        const result = statusFromError(entry[0]);
        try std.testing.expectEqual(@backingInt(entry[1]), result.code);
        try std.testing.expectEqual(entry[0], errorFromStatus(result));
    }
}

pub const Status = extern struct {
    // Keep the wire fields as integers so a newer peer's enum value can be
    // rejected without constructing an invalid exhaustive Zig enum.
    code: c_int = @backingInt(Code.ok),
    detail: c_int = @backingInt(Detail.none),

    pub const ok: Status = .{};

    pub fn isOk(self: Status) bool {
        return self.code == @backingInt(Code.ok);
    }
};

pub fn statusFromError(err: anyerror) Status {
    return switch (err) {
        error.IndexAliasPreconditionRequired => status(.conflict, .index_alias_precondition_required),
        error.IndexAliasPreconditionFailed => status(.conflict, .index_alias_precondition_failed),
        error.IndexReferencedByAlias => status(.conflict, .index_referenced_by_alias),
        error.IndexAliasGraphConflict => status(.conflict, .index_alias_graph_conflict),
        error.CatalogChanged => status(.conflict, .catalog_changed),
        error.SqlStatementReadUnavailable => status(.retryable, .sql_statement_read_unavailable),
        error.UnsupportedSqlExecution => status(.unsupported, .unsupported_sql_execution),
        error.ConflictingPrivacyProducerOptions => status(.invalid_argument, .conflicting_privacy_producer_options),
        error.ConflictingPrivacyProducerSchema => status(.invalid_argument, .conflicting_privacy_producer_schema),
        error.InvalidPrivacyCatalog => status(.corrupt, .invalid_privacy_catalog),
        error.InvalidPrivacyCategories => status(.invalid_argument, .invalid_privacy_categories),
        error.InvalidPrivacyCategory => status(.invalid_argument, .invalid_privacy_category),
        error.InvalidPrivacyConfidence => status(.invalid_argument, .invalid_privacy_confidence),
        error.InvalidPrivacyDocument => status(.invalid_argument, .invalid_privacy_document),
        error.InvalidPrivacyExecution => status(.invalid_argument, .invalid_privacy_execution),
        error.InvalidPrivacyFieldType => status(.invalid_argument, .invalid_privacy_field_type),
        error.InvalidPrivacyFields => status(.invalid_argument, .invalid_privacy_fields),
        error.InvalidPrivacyOffsetUnit => status(.invalid_argument, .invalid_privacy_offset_unit),
        error.InvalidPrivacyOutput => status(.invalid_argument, .invalid_privacy_output),
        error.InvalidPrivacyOutputCardinality => status(.invalid_argument, .invalid_privacy_output_cardinality),
        error.InvalidPrivacyProducer => status(.invalid_argument, .invalid_privacy_producer),
        error.InvalidPrivacyReceipt => status(.corrupt, .invalid_privacy_receipt),
        error.InvalidPrivacyScan => status(.invalid_argument, .invalid_privacy_scan),
        error.InvalidPrivacySource => status(.invalid_argument, .invalid_privacy_source),
        error.InvalidPrivacySpan => status(.invalid_argument, .invalid_privacy_span),
        error.InvalidPrivacyThreshold => status(.invalid_argument, .invalid_privacy_threshold),
        error.InvalidPrivacyTokenGeometry => status(.invalid_argument, .invalid_privacy_token_geometry),
        error.InvalidPrivacyWindowReceipt => status(.invalid_argument, .invalid_privacy_window_receipt),
        error.PrivacyEmbeddingProducerRequired => status(.invalid_argument, .privacy_embedding_producer_required),
        error.PrivacyEvidenceMismatch => status(.conflict, .privacy_evidence_mismatch),
        error.PrivacyExternalEmbeddingsUnsupported => status(.unsupported, .privacy_external_embeddings_unsupported),
        error.PrivacyExternalVectorsUnsupported => status(.unsupported, .privacy_external_vectors_unsupported),
        error.PrivacyInspectionFailed => status(.retryable, .privacy_inspection_failed),
        error.PrivacyLiteralCredential => status(.invalid_argument, .privacy_literal_credential),
        error.PrivacyManagedGraph => status(.invalid_argument, .privacy_managed_graph),
        error.PrivacyPending => status(.retryable, .privacy_pending),
        error.PrivacyPreconditionFailed => status(.conflict, .privacy_precondition_failed),
        error.PrivacyPreconditionRequired => status(.invalid_argument, .privacy_precondition_required),
        error.PrivacyProducerRevisionMismatch => status(.invalid_argument, .privacy_producer_revision_mismatch),
        error.PrivacyProducerRevisionRequired => status(.invalid_argument, .privacy_producer_revision_required),
        error.PrivacyPublicationPending => status(.retryable, .privacy_publication_pending),
        error.PrivacyReadFenceUnavailable => status(.retryable, .privacy_read_fence_unavailable),
        error.PrivacyRelationalStorageUnsupported => status(.unsupported, .privacy_relational_storage_unsupported),
        error.PrivacySchemaExceedsWindow => status(.invalid_argument, .privacy_schema_exceeds_window),
        error.PrivacyStale => status(.retryable, .privacy_stale),
        error.PrivacySynchronousIngestionRequired => status(.unsupported, .privacy_synchronous_ingestion_required),
        error.PrivacyUnknownArtifactSource => status(.invalid_argument, .privacy_unknown_artifact_source),
        error.PrivacyUnsupportedArtifactInput => status(.unsupported, .privacy_unsupported_artifact_input),
        error.PrivacyUnsupportedIndexType => status(.unsupported, .privacy_unsupported_index_type),
        error.PrivacyUnsupportedMutation => status(.unsupported, .privacy_unsupported_mutation),
        error.PrivacyUnsupportedProjection => status(.unsupported, .privacy_unsupported_projection),
        error.PrivacyWindowLimitExceeded => status(.invalid_argument, .privacy_window_limit_exceeded),
        error.PrivacyWindowTokenLimitExceeded => status(.invalid_argument, .privacy_window_token_limit_exceeded),
        error.UnknownPrivacyCategory => status(.invalid_argument, .unknown_privacy_category),
        error.InvalidDocumentExtractionConfig => status(.invalid_argument, .invalid_document_extraction_config),
        error.DocumentExtractionModelRequired => status(.invalid_argument, .document_extraction_model_required),
        error.ReadTransientFailure => status(.retryable, .read_transient_failure),
        error.ReadRequestFailed => status(.invalid_argument, .read_request_failed),
        error.InvalidGcsUrl => status(.invalid_argument, .invalid_gcs_url),
        error.GcsConnectionNotFoundOrOutOfScope => status(.forbidden, .gcs_connection_not_found_or_out_of_scope),
        error.AmbiguousGcsConnection => status(.invalid_argument, .ambiguous_gcs_connection),
        error.InvalidConnectionCredentials => status(.invalid_argument, .invalid_connection_credentials),
        error.InvalidDocumentSource => status(.invalid_argument, .invalid_document_source),
        error.UnsupportedDocumentFormat => status(.invalid_argument, .unsupported_document_format),
        error.DocumentSourceLimitExceeded => status(.invalid_argument, .document_source_limit_exceeded),
        error.DocumentPageLimitExceeded => status(.invalid_argument, .document_page_limit_exceeded),
        error.DocumentPixelLimitExceeded => status(.invalid_argument, .document_pixel_limit_exceeded),
        error.DocumentTokenLimitExceeded => status(.invalid_argument, .document_token_limit_exceeded),
        error.DocumentMediaTypeMismatch => status(.invalid_argument, .document_media_type_mismatch),
        error.InvalidDocumentSelection => status(.invalid_argument, .invalid_document_selection),
        error.DuplicateDocumentSource => status(.invalid_argument, .duplicate_document_source),
        error.DocumentUnresolved => status(.invalid_argument, .document_unresolved),
        error.DocumentCapabilityMismatch => status(.invalid_argument, .document_capability_mismatch),
        error.UnsupportedDocumentRequestField => status(.invalid_argument, .unsupported_document_request_field),
        error.RemoteDocumentModelDenied => status(.forbidden, .remote_document_model_denied),
        error.CloudflareCredentialMissing => status(.invalid_argument, .cloudflare_credential_missing),
        error.InvalidCloudflareConfig => status(.invalid_argument, .invalid_cloudflare_config),
        error.UnsupportedCloudflareModel => status(.invalid_argument, .unsupported_cloudflare_model),
        error.UnsupportedCloudflareSchema => status(.invalid_argument, .unsupported_cloudflare_schema),
        error.PioneerCredentialMissing => status(.invalid_argument, .pioneer_credential_missing),
        error.UnsupportedPioneerSchema => status(.invalid_argument, .unsupported_pioneer_schema),
        error.UnsupportedPioneerInput => status(.invalid_argument, .unsupported_pioneer_input),
        error.StandaloneReplicationSourcesUnsupported => status(.invalid_argument, .standalone_replication_sources_unsupported),
        error.HACatalogReplicationSourcesUnsupported => status(.invalid_argument, .ha_catalog_replication_sources_unsupported),
        error.CloudflareAuthenticationFailed => status(.unauthorized, .cloudflare_authentication_failed),
        error.InvalidExtractorResponse => status(.invalid_argument, .invalid_extractor_response),
        error.UnsupportedExtractionSchemaVersion => status(.invalid_argument, .unsupported_extraction_schema_version),
        error.InvalidInferenceRequest => status(.invalid_argument, .invalid_inference_request),
        error.DocumentArtifactNotFound => status(.not_found, .document_artifact_not_found),
        error.DocumentArtifactExpired => status(.not_found, .document_artifact_expired),
        error.CloudflareRateLimited => status(.retryable, .cloudflare_rate_limited),
        error.InferenceUpstreamRateLimited => status(.retryable, .inference_upstream_rate_limited),
        error.InferenceCapacityUnavailable => status(.unavailable, .inference_capacity_unavailable),
        error.InferenceTransportUnavailable => status(.unavailable, .inference_transport_unavailable),
        error.CloudflareInferenceFailed => status(.unavailable, .cloudflare_inference_failed),
        error.InvalidCloudflareResponse => status(.unavailable, .invalid_cloudflare_response),
        error.TooManyActiveJobs => status(.retryable, .too_many_active_jobs),
        error.DocumentStorageNotConfigured => status(.unavailable, .document_storage_not_configured),
        error.DocumentRunBusy => status(.conflict, .document_run_busy),
        error.DocumentRunExists => status(.already_exists, .document_run_exists),
        error.DocumentRunExpired => status(.not_found, .document_run_expired),
        error.DocumentRunLeaseLost => status(.conflict, .document_run_lease_lost),
        error.DocumentRunNotFound => status(.not_found, .document_run_not_found),
        error.DocumentRunRetentionActive => status(.conflict, .document_run_retention_active),
        error.DocumentRunTerminal => status(.conflict, .document_run_terminal),
        error.DocumentRunStoreNeedsRecovery => status(.unavailable, .document_run_store_needs_recovery),
        error.DocumentModelRevisionMismatch => status(.conflict, .document_model_revision_mismatch),
        error.DocumentBudgetExhausted => status(.invalid_argument, .document_budget_exhausted),
        error.DocumentOutputTooLarge => status(.invalid_argument, .document_output_too_large),
        error.DocumentStageInFlight => status(.conflict, .document_stage_in_flight),
        error.InvalidDocumentCheckpoint => status(.invalid_argument, .invalid_document_checkpoint),

        error.ResolverCurationConflict => status(.conflict, .resolver_curation_conflict),
        error.GroupedSourceOwnerChanged => status(.conflict, .grouped_source_owner_changed),
        error.GroupedSourceTopologyUnavailable => status(.unavailable, .grouped_source_topology_unavailable),
        error.AppleIntelligenceProviderUnavailable => status(.unavailable, .apple_intelligence_provider_unavailable),
        error.ArtifactPublicationPending => status(.retryable, .artifact_publication_pending),
        error.ChatGPTBillingFallbackForbidden => status(.forbidden, .chatgpt_billing_fallback_forbidden),
        error.ChatGPTDisabled => status(.invalid_argument, .chatgpt_disabled),
        error.ChatGPTInteractiveOnly => status(.invalid_argument, .chatgpt_interactive_only),
        error.ConstrainedInputTooLarge => status(.invalid_argument, .constrained_input_too_large),
        error.DecideRequestLimitExceeded => status(.invalid_argument, .decide_request_limit_exceeded),
        error.EnrichmentSourceChanged => status(.conflict, .enrichment_source_changed),
        error.GeneratedExtractionLimitExceeded => status(.invalid_argument, .generated_extraction_limit_exceeded),
        error.GroupedPriorContextLimitExceeded => status(.invalid_argument, .grouped_prior_context_limit_exceeded),
        error.GroupedSourceBaselinePending => status(.retryable, .grouped_source_baseline_pending),
        error.GroupedSourceBusy => status(.retryable, .grouped_source_busy),
        error.GroupedSourceCorrupt => status(.corrupt, .grouped_source_corrupt),
        error.GroupedSourceEarlierWorkPending => status(.retryable, .grouped_source_earlier_work_pending),
        error.GroupedSourceIdentityConflict => status(.conflict, .grouped_source_identity_conflict),
        error.GroupedSourceLimitExceeded => status(.invalid_argument, .grouped_source_limit_exceeded),
        error.GroupedSourceMemberLimitExceeded => status(.invalid_argument, .grouped_source_member_limit_exceeded),
        error.GroupedSourceMemberTooLarge => status(.invalid_argument, .grouped_source_member_too_large),
        error.GroupedSourcePartitionTooLarge => status(.invalid_argument, .grouped_source_partition_too_large),
        error.GroupedSourceRetirementPending => status(.retryable, .grouped_source_retirement_pending),
        error.InvalidConstrainedGenerationConfig => status(.invalid_argument, .invalid_constrained_generation_config),
        error.InvalidConstrainedOutput => status(.invalid_argument, .invalid_constrained_output),
        error.InvalidConstrainedProbabilities => status(.invalid_argument, .invalid_constrained_probabilities),
        error.InvalidConstrainedSchema => status(.invalid_argument, .invalid_constrained_schema),
        error.InvalidDecideRequest => status(.invalid_argument, .invalid_decide_request),
        error.InvalidDecisionOutput => status(.invalid_argument, .invalid_decision_output),
        error.InvalidGeneratedExtractorConfig => status(.invalid_argument, .invalid_generated_extractor_config),
        error.InvalidGeneratorConfig => status(.invalid_argument, .invalid_generator_config),
        error.InvalidGeneratorToolConfig => status(.invalid_argument, .invalid_generator_tool_config),
        error.InvalidGroupedPriorContext => status(.invalid_argument, .invalid_grouped_prior_context),
        error.InvalidGroupedSourceCommand => status(.invalid_argument, .invalid_grouped_source_command),
        error.InvalidGroupedSourceConfig => status(.invalid_argument, .invalid_grouped_source_config),
        error.InvalidGroupedSourceMember => status(.invalid_argument, .invalid_grouped_source_member),
        error.InvalidGroupedSourceMutation => status(.invalid_argument, .invalid_grouped_source_mutation),
        error.InvalidGroupedSourceQuery => status(.invalid_argument, .invalid_grouped_source_query),
        error.InvalidGroupedSourceTransfer => status(.invalid_argument, .invalid_grouped_source_transfer),
        error.InvalidStructuredOutput => status(.invalid_argument, .invalid_structured_output),
        error.MissingGeneratorCredentials => status(.invalid_argument, .missing_generator_credentials),
        error.UnsupportedAppleGenerationOptions => status(.unsupported, .unsupported_apple_generation_options),
        error.UnsupportedConstrainedOutput => status(.unsupported, .unsupported_constrained_output),
        error.UnsupportedConstrainedProbabilities => status(.unsupported, .unsupported_constrained_probabilities),
        error.UnsupportedConstrainedSchema => status(.unsupported, .unsupported_constrained_schema),
        error.UnsupportedGeneratedExtractionSchema => status(.unsupported, .unsupported_generated_extraction_schema),
        error.SchemaVersionChanged => status(.conflict, .schema_version_changed),
        error.CypherWriteRequestAlreadyUsed => status(.conflict, .cypher_write_request_already_used),
        error.InvalidCypherParameters => status(.invalid_argument, .invalid_cypher_parameters),
        error.MissingCypherParameter => status(.invalid_argument, .missing_cypher_parameter),
        error.SqlWriteCapacityUnavailable => status(.unavailable, .sql_write_capacity_unavailable),
        error.CypherAggregateScope => status(.invalid_argument, .cypher_aggregate_scope),
        error.CypherAggregateWildcard => status(.invalid_argument, .cypher_aggregate_wildcard),
        error.CypherAmbiguousSearchTable => status(.invalid_argument, .cypher_ambiguous_search_table),
        error.RowPolicyUnsupported => status(.invalid_argument, .row_policy_unsupported),
        error.RowPolicyMutationUnsupported => status(.invalid_argument, .row_policy_mutation_unsupported),
        error.CypherInvalidRegex => status(.invalid_argument, .cypher_invalid_regex),
        error.RowPolicyDenied => status(.forbidden, .row_policy_denied),
        error.RowPolicyAuthenticationRequired => status(.unauthorized, .row_policy_authentication_required),
        error.RowPolicyAuthorityUnavailable => status(.unavailable, .row_policy_authority_unavailable),
        error.TransactionPrepareAbortedRowPolicyDenied => status(.forbidden, .transaction_prepare_aborted_row_policy_denied),
        error.RetainedReadRestartRequired => status(.retryable, .retained_read_restart_required),
        error.CypherAnchorBudgetExceeded => status(.invalid_argument, .cypher_anchor_budget_exceeded),
        error.CypherCreateRequiresLabel => status(.invalid_argument, .cypher_create_requires_label),
        error.CypherDeletedEntity => status(.invalid_argument, .cypher_deleted_entity),
        error.CypherDerivedRelationshipMutation => status(.invalid_argument, .cypher_derived_relationship_mutation),
        error.CypherDivisionByZero => status(.invalid_argument, .cypher_division_by_zero),
        error.CypherDuplicateIdentity => status(.invalid_argument, .cypher_duplicate_identity),
        error.CypherEdgeBudgetExceeded => status(.invalid_argument, .cypher_edge_budget_exceeded),
        error.CypherExistsMutation => status(.invalid_argument, .cypher_exists_mutation),
        error.CypherFunctionArity => status(.invalid_argument, .cypher_function_arity),
        error.CypherInvalidAssignment => status(.invalid_argument, .cypher_invalid_assignment),
        error.CypherInvalidBackendCursor => status(.invalid_argument, .cypher_invalid_backend_cursor),
        error.CypherInvalidBackendPage => status(.invalid_argument, .cypher_invalid_backend_page),
        error.CypherInvalidBackendRelationship => status(.invalid_argument, .cypher_invalid_backend_relationship),
        error.CypherInvalidCreateRelationship => status(.invalid_argument, .cypher_invalid_create_relationship),
        error.CypherInvalidDocument => status(.invalid_argument, .cypher_invalid_document),
        error.CypherInvalidGrouping => status(.invalid_argument, .cypher_invalid_grouping),
        error.CypherInvalidOwner => status(.invalid_argument, .cypher_invalid_owner),
        error.CypherInvalidPropertyValue => status(.invalid_argument, .cypher_invalid_property_value),
        error.CypherInvalidRange => status(.invalid_argument, .cypher_invalid_range),
        error.CypherInvalidRelationship => status(.invalid_argument, .cypher_invalid_relationship),
        error.CypherInvalidRelationshipMetadata => status(.invalid_argument, .cypher_invalid_relationship_metadata),
        error.CypherInvalidSearchLimit => status(.invalid_argument, .cypher_invalid_search_limit),
        error.CypherInvalidSearchResult => status(.invalid_argument, .cypher_invalid_search_result),
        error.CypherInvalidShortestPath => status(.invalid_argument, .cypher_invalid_shortest_path),
        error.CypherInvalidSubstring => status(.invalid_argument, .cypher_invalid_substring),
        error.CypherLabelPredicateMismatch => status(.invalid_argument, .cypher_label_predicate_mismatch),
        error.CypherLabelPredicateViolation => status(.invalid_argument, .cypher_label_predicate_violation),
        error.CypherLegacyIdUnavailable => status(.invalid_argument, .cypher_legacy_id_unavailable),
        error.CypherLimitExceeded => status(.invalid_argument, .cypher_limit_exceeded),
        error.CypherMemoryBudgetExceeded => status(.invalid_argument, .cypher_memory_budget_exceeded),
        error.CypherMutationsUnavailable => status(.unavailable, .cypher_mutations_unavailable),
        error.CypherNodeHasRelationships => status(.invalid_argument, .cypher_node_has_relationships),
        error.CypherNumericOverflow => status(.invalid_argument, .cypher_numeric_overflow),
        error.CypherObservationsUnavailable => status(.invalid_argument, .cypher_observations_unavailable),
        error.CypherOverlappingOwners => status(.invalid_argument, .cypher_overlapping_owners),
        error.CypherOwnerBudgetExceeded => status(.invalid_argument, .cypher_owner_budget_exceeded),
        error.CypherPathBudgetExceeded => status(.invalid_argument, .cypher_path_budget_exceeded),
        error.CypherQueryRequiresReturn => status(.invalid_argument, .cypher_query_requires_return),
        error.CypherRecursionBudgetExceeded => status(.invalid_argument, .cypher_recursion_budget_exceeded),
        error.CypherRegexUnavailable => status(.invalid_argument, .cypher_regex_unavailable),
        error.CypherRelationshipEndpointMismatch => status(.invalid_argument, .cypher_relationship_endpoint_mismatch),
        error.CypherRemoveRequiresProperty => status(.invalid_argument, .cypher_remove_requires_property),
        error.CypherRequestTooLarge => status(.invalid_argument, .cypher_request_too_large),
        error.CypherResponseTooLarge => status(.invalid_argument, .cypher_response_too_large),
        error.CypherReturnMustBeLast => status(.invalid_argument, .cypher_return_must_be_last),
        error.CypherReturnPredicate => status(.invalid_argument, .cypher_return_predicate),
        error.CypherRowPolicyUnsupported => status(.invalid_argument, .cypher_row_policy_unsupported),
        error.CypherSearchAfterWrite => status(.invalid_argument, .cypher_search_after_write),
        error.CypherSearchPreparationRequired => status(.invalid_argument, .cypher_search_preparation_required),
        error.CypherSearchTableOutsideGraph => status(.invalid_argument, .cypher_search_table_outside_graph),
        error.CypherSearchUnavailable => status(.unavailable, .cypher_search_unavailable),
        error.CypherSnapshotNotReady => status(.unavailable, .cypher_snapshot_not_ready),
        error.CypherSnapshotRequired => status(.unavailable, .cypher_snapshot_required),
        error.CypherStateBudgetExceeded => status(.invalid_argument, .cypher_state_budget_exceeded),
        error.CypherTransactionAlreadyActive => status(.invalid_argument, .cypher_transaction_already_active),
        error.CypherTransactionConflict => status(.conflict, .cypher_transaction_conflict),
        error.CypherTransactionRequired => status(.invalid_argument, .cypher_transaction_required),
        error.CypherTypeMismatch => status(.invalid_argument, .cypher_type_mismatch),
        error.CypherUnionColumnMismatch => status(.invalid_argument, .cypher_union_column_mismatch),
        error.CypherValueDepthExceeded => status(.invalid_argument, .cypher_value_depth_exceeded),
        error.CypherVariableConflict => status(.invalid_argument, .cypher_variable_conflict),
        error.CypherVariableTypeConflict => status(.invalid_argument, .cypher_variable_type_conflict),
        error.CypherWriteUnionUnsupported => status(.invalid_argument, .cypher_write_union_unsupported),

        error.ChunkWorkingBudgetExceeded => status(.invalid_argument, .model_chunk_working_budget),
        error.ChunkDependencyBudgetExceeded => status(.invalid_argument, .model_chunk_dependency_budget),
        error.ChunkCodedWindowBudgetExceeded => status(.invalid_argument, .model_chunk_coded_window_budget),
        error.ChunkEventBudgetExceeded => status(.invalid_argument, .model_chunk_event_budget),
        error.RemoteReadBudgetExceeded => status(.invalid_argument, .model_chunk_remote_bytes_budget),
        error.RemoteRequestBudgetExceeded => status(.invalid_argument, .model_chunk_remote_requests_budget),
        error.InvalidChunkLimits => status(.invalid_argument, .model_chunk_invalid_limits),
        error.UnsupportedStreamingAudioProfile => status(.unsupported, .model_chunk_streaming_profile),
        error.IncompleteAudioStream => status(.unavailable, .model_chunk_incomplete_audio_stream),

        error.AudioTooLarge => status(.invalid_argument, .model_chunk_audio_too_large),
        error.ChunkOutputTooLarge => status(.invalid_argument, .model_chunk_chunk_output_too_large),
        error.ChunkRequestFailed => status(.unavailable, .model_chunk_chunk_request_failed),
        error.ChunkUnitTooLarge => status(.invalid_argument, .model_chunk_chunk_unit_too_large),
        error.EmbeddingIdentityMismatch => status(.conflict, .model_chunk_embedding_identity_mismatch),
        error.InvalidEmbeddingIdentity => status(.invalid_argument, .invalid_embedding_identity),
        error.EmptyEmbeddingInput => status(.invalid_argument, .model_chunk_empty_embedding_input),
        error.EmptyResponse => status(.unavailable, .model_chunk_empty_response),
        error.EmptySource => status(.invalid_argument, .model_chunk_empty_source),
        error.EmptyVideo => status(.invalid_argument, .model_chunk_empty_video),
        error.ImmutableVersionRequired => status(.invalid_argument, .model_chunk_immutable_version_required),
        error.IncompatibleChunkEmbeddingTarget => status(.invalid_argument, .model_chunk_incompatible_chunk_embedding_target),
        error.IncompleteChunkCoverage => status(.invalid_argument, .model_chunk_incomplete_chunk_coverage),
        error.IncompleteVideoPicture => status(.corrupt, .model_chunk_incomplete_video_picture),
        error.InferenceAdmissionQueueFull => status(.retryable, .model_chunk_inference_admission_queue_full),
        error.InferenceCapabilitiesStale => status(.conflict, .model_chunk_inference_capabilities_stale),
        error.InvalidAudioWindow => status(.invalid_argument, .model_chunk_invalid_audio_window),
        error.InvalidChunkContinuation => status(.invalid_argument, .model_chunk_invalid_chunk_continuation),
        error.InvalidChunkCoordinates => status(.invalid_argument, .model_chunk_invalid_chunk_coordinates),
        error.InvalidChunkGeometry => status(.invalid_argument, .model_chunk_invalid_chunk_geometry),
        error.InvalidChunkModel => status(.invalid_argument, .model_chunk_invalid_chunk_model),
        error.InvalidChunkPolicy => status(.invalid_argument, .model_chunk_invalid_chunk_policy),
        error.InvalidChunkProvider => status(.invalid_argument, .model_chunk_invalid_chunk_provider),
        error.InvalidChunkReplay => status(.conflict, .model_chunk_invalid_chunk_replay),
        error.InvalidChunkSource => status(.invalid_argument, .model_chunk_invalid_chunk_source),
        error.InvalidChunkerConfig => status(.invalid_argument, .model_chunk_invalid_chunker_config),
        error.InvalidChunkerResponse => status(.invalid_argument, .model_chunk_invalid_chunker_response),
        error.InvalidEmbeddingOutput => status(.internal, .model_chunk_invalid_embedding_output),
        error.InvalidInputShape => status(.invalid_argument, .model_chunk_invalid_input_shape),
        error.InvalidSamplingPolicy => status(.invalid_argument, .model_chunk_invalid_sampling_policy),
        error.InvalidSourceReference => status(.invalid_argument, .model_chunk_invalid_source_reference),
        error.InvalidUtf8 => status(.invalid_argument, .model_chunk_invalid_utf8),
        error.MemoryBudgetExceeded => status(.retryable, .model_chunk_memory_budget_exceeded),
        error.MissingAttachmentEnvelope => status(.invalid_argument, .model_chunk_missing_attachment_envelope),
        error.MissingChunkStreamContext => status(.internal, .model_chunk_missing_chunk_stream_context),
        error.ModelAwareChunkProviderRequired => status(.invalid_argument, .model_chunk_model_aware_chunk_provider_required),
        error.SharedAdmissionExceeded => status(.retryable, .model_chunk_shared_admission_exceeded),
        error.SourceVersionChanged => status(.conflict, .model_chunk_source_version_changed),
        error.StaleChunkContinuation => status(.conflict, .model_chunk_stale_chunk_continuation),
        error.StreamTooLong => status(.unavailable, .model_chunk_stream_too_long),
        error.TimestampOverflow => status(.invalid_argument, .model_chunk_timestamp_overflow),
        error.UnknownVideoDuration => status(.invalid_argument, .model_chunk_unknown_video_duration),
        error.UnresolvedChunkSource => status(.invalid_argument, .model_chunk_unresolved_chunk_source),
        error.UnsupportedAudioFormat => status(.unsupported, .model_chunk_unsupported_audio_format),
        error.UnsupportedChunkMediaType => status(.unsupported, .model_chunk_unsupported_chunk_media_type),
        error.ExternalLakeReadOnly => status(.forbidden, .external_lake_read_only),
        error.MaterializedViewReadOnly => status(.forbidden, .materialized_view_read_only),
        error.ExternalLakeSnapshotMismatch => status(.conflict, .external_lake_snapshot_mismatch),
        error.ExternalLakeSchemaMismatch => status(.conflict, .external_lake_schema_mismatch),
        error.ExternalLakeSchemaUnavailable => status(.unavailable, .external_lake_schema_unavailable),
        error.InvalidExternalSourceInventory => status(.invalid_argument, .invalid_external_source_inventory),
        error.InvalidExternalSourceSnapshot => status(.invalid_argument, .invalid_external_source_snapshot),
        error.ExternalSourceSnapshotTooLarge => status(.invalid_argument, .external_source_snapshot_too_large),
        error.ExternalLakeFieldIdentityChanged => status(.conflict, .external_lake_field_identity_changed),
        error.ExternalLakeObjectChanged => status(.conflict, .external_lake_object_changed),
        error.EmptyExternalSourceSnapshot => status(.invalid_argument, .empty_external_source_snapshot),
        error.UnsupportedExternalLakeSchemaType => status(.unsupported, .unsupported_external_lake_schema_type),
        error.InvalidParquetMetadata => status(.corrupt, .invalid_parquet_metadata),
        error.InvalidParquetFooter => status(.corrupt, .invalid_parquet_footer),
        error.InvalidParquetFooterMagic => status(.corrupt, .invalid_parquet_footer_magic),
        error.InvalidParquetPage => status(.corrupt, .invalid_parquet_page),
        error.UnsupportedParquetPage => status(.unsupported, .unsupported_parquet_page),
        error.ParquetPageTooLarge => status(.invalid_argument, .parquet_page_too_large),
        error.ParquetColumnNotFound => status(.not_found, .parquet_column_not_found),
        error.UnsupportedIcebergSchemaEvolution => status(.unsupported, .unsupported_iceberg_schema_evolution),
        error.UnsupportedIcebergDeleteFile => status(.unsupported, .unsupported_iceberg_delete_file),
        error.InvalidSqlSpill => status(.corrupt, .invalid_sql_spill),

        error.SettingAuthorityUnavailable => status(.unavailable, .setting_authority_unavailable),
        error.InvalidGenerationPublication => status(.invalid_argument, .invalid_generation_publication),
        error.GenerationPublicationChanged => status(.conflict, .generation_publication_changed),
        error.InitialChildProvisionAlreadyCommitted => status(.conflict, .initial_child_provision_already_committed),
        error.InvalidRetirementSummary => status(.corrupt, .invalid_retirement_summary),
        error.InvalidControlReceiptPosition => status(.corrupt, .invalid_control_receipt_position),
        error.InvalidGraphTransfer => status(.invalid_argument, .invalid_graph_transfer),
        error.GraphGenerationMismatch => status(.conflict, .graph_generation_mismatch),
        error.GenerationAdmissionAcknowledgementPending => status(.retryable, .generation_admission_acknowledgement_pending),
        error.GenerationAdmissionChanged => status(.conflict, .generation_admission_changed),
        error.GenerationAdmissionPending => status(.retryable, .generation_admission_pending),
        error.GenerationAdmissionRevisionExhausted => status(.conflict, .generation_admission_revision_exhausted),
        error.InvalidGenerationAdmission => status(.invalid_argument, .invalid_generation_admission),
        error.GenerationAdmissionActivationRequired => status(.conflict, .generation_admission_activation_required),
        error.GenerationRetirementAcknowledgementPending => status(.retryable, .generation_retirement_acknowledgement_pending),
        error.GenerationRetirementChanged => status(.conflict, .generation_retirement_changed),
        error.GenerationRetirementHandoffRequired => status(.conflict, .generation_retirement_handoff_required),
        error.GenerationRetirementPending => status(.retryable, .generation_retirement_pending),
        error.GenerationRetirementRevisionExhausted => status(.conflict, .generation_retirement_revision_exhausted),
        error.InvalidGenerationRetirement => status(.invalid_argument, .invalid_generation_retirement),
        error.InitialChildPublicationMissing => status(.not_found, .initial_child_publication_missing),
        error.InitialChildNotPublished => status(.retryable, .initial_child_not_published),
        error.InitialChildPublicationChanged => status(.conflict, .initial_child_publication_changed),
        error.InvalidInitialChildPublication => status(.invalid_argument, .invalid_initial_child_publication),
        error.InitialFkRetirementProofUnavailable => status(.unavailable, .initial_fk_retirement_proof_unavailable),
        error.InvalidInitialFkRetirementTicket => status(.invalid_argument, .invalid_initial_fk_retirement_ticket),
        error.InvalidInitialFkRetirementIntent => status(.corrupt, .invalid_initial_fk_retirement_intent),
        error.InitialFkRetirementPathChanged => status(.conflict, .initial_fk_retirement_path_changed),
        error.ArtifactCatalogCorrupt => status(.corrupt, .artifact_catalog_corrupt),
        error.ArtifactCatalogEpochExhausted => status(.internal, .artifact_catalog_epoch_exhausted),
        error.OnlineMergeArtifactTailsUnsupported => status(.unsupported, .online_merge_artifact_tails_unsupported),
        error.ArtifactCatalogDrift => status(.retryable, .artifact_catalog_drift),
        error.InvalidArtifactCatalogCommand => status(.invalid_argument, .invalid_artifact_catalog_command),
        error.ArtifactCatalogEpochChanged => status(.conflict, .artifact_catalog_epoch_changed),
        error.ArtifactCatalogScopeChanged => status(.conflict, .artifact_catalog_scope_changed),
        error.OnlineMergeArtifactCatalogChanged => status(.conflict, .online_merge_artifact_catalog_changed),
        error.StoreRootEnrollmentChanged => status(.forbidden, .store_root_enrollment_changed),
        error.InvalidSchemaValidationJob => status(.invalid_argument, .invalid_schema_validation_job),
        error.InvalidSchemaValidationPageCommand => status(.invalid_argument, .invalid_schema_validation_page_command),
        error.InvalidSchemaValidationToken => status(.invalid_argument, .invalid_schema_validation_token),
        error.SchemaValidationCanceled => status(.conflict, .schema_validation_canceled),
        error.SchemaValidationCancellationRequired => status(.conflict, .schema_validation_cancellation_required),
        error.SchemaValidationCommandRejected => status(.invalid_argument, .schema_validation_command_rejected),
        error.SchemaValidationInputsRequired => status(.invalid_argument, .schema_validation_inputs_required),
        error.SchemaValidationJobChanged => status(.conflict, .schema_validation_job_changed),
        error.SchemaValidationJobRequired => status(.conflict, .schema_validation_job_required),
        error.SchemaValidationNotReady => status(.retryable, .schema_validation_not_ready),
        error.SchemaValidationPreparationChanged => status(.conflict, .schema_validation_preparation_changed),
        error.SchemaValidationPublicationRequired => status(.unsupported, .schema_validation_publication_required),
        error.SchemaValidationRuntimeRecoveryRequired => status(.retryable, .schema_validation_runtime_recovery_required),
        error.StoreRegistrationRequired => status(.unavailable, .store_registration_required),
        error.CatalogPublicationFenceUnavailable => status(.unavailable, .catalog_publication_fence_unavailable),
        error.InvalidStoreRootEnrollment => status(.invalid_argument, .invalid_store_root_enrollment),
        error.InitialChildRootReceiptChanged => status(.conflict, .initial_child_root_receipt_changed),
        error.InvalidInitialFkRetirementSignature => status(.forbidden, .invalid_initial_fk_retirement_signature),
        error.InitialFkRetirementSigningKeyUnavailable => status(.unavailable, .initial_fk_retirement_signing_key_unavailable),
        error.InitialFkRetirementReporterChanged => status(.conflict, .initial_fk_retirement_reporter_changed),
        error.InitialFkRetirementWorkChanged => status(.conflict, .initial_fk_retirement_work_changed),
        error.InitialFkRetirementPublicationChanged => status(.conflict, .initial_fk_retirement_publication_changed),
        error.InitialFkRetirementReservationChanged => status(.conflict, .initial_fk_retirement_reservation_changed),
        error.InvalidInitialFkRetirementAck => status(.invalid_argument, .invalid_initial_fk_retirement_ack),
        error.InvalidInitialFkRetirementPage => status(.invalid_argument, .invalid_initial_fk_retirement_page),
        error.InitialFkRetirementRootChanged => status(.conflict, .initial_fk_retirement_root_changed),
        error.ReplicaRetirementRecoveryInProgress => status(.retryable, .replica_retirement_recovery_in_progress),
        error.MembershipChangeFenced => status(.conflict, .membership_change_fenced),
        error.SqlRangeTrackingRequired => status(.unsupported, .sql_range_tracking_required),
        error.SqlStatementSnapshotRequired => status(.unsupported, .sql_statement_snapshot_required),
        error.InvalidRangeTrackingState => status(.corrupt, .invalid_range_tracking_state),
        error.RangeTrackingGenerationExhausted => status(.unavailable, .range_tracking_generation_exhausted),
        error.HASeedSnapshotRuntimeBusy => status(.unavailable, .ha_seed_snapshot_runtime_busy),
        error.HASeedCaptureAlreadyInProgress => status(.unavailable, .ha_seed_capture_already_in_progress),
        error.StorageKernelOwnerUnavailable => status(.unavailable, .storage_kernel_owner_unavailable),
        error.TableTopologyProtocolUpgradeRequired => status(.unavailable, .table_topology_protocol_upgrade_required),
        error.DatabaseNotFound => status(.not_found, .database_not_found),
        error.NamespaceNotFound => status(.not_found, .namespace_not_found),
        error.TablespaceNotFound => status(.not_found, .tablespace_not_found),
        error.CatalogRoutingSnapshotTimeout => status(.timeout, .catalog_routing_snapshot_timeout),
        error.MetadataIncarnationUnavailable => status(.unavailable, .metadata_incarnation_unavailable),
        error.InvalidMetadataIncarnation => status(.unavailable, .invalid_metadata_incarnation),
        error.MetadataIncarnationMismatch => status(.unavailable, .metadata_incarnation_mismatch),
        error.CatalogRoutingUnavailable => status(.unavailable, .catalog_routing_unavailable),
        error.CatalogProjectionRefreshRequired => status(.unavailable, .catalog_projection_refresh_required),
        error.CatalogNotFound => status(.not_found, .catalog_not_found),
        error.CatalogAlreadyExists => status(.conflict, .catalog_already_exists),
        error.CatalogGenerationChanged => status(.conflict, .catalog_generation_changed),
        error.TablespaceInUse => status(.conflict, .tablespace_in_use),
        error.NamespaceNotEmpty => status(.conflict, .namespace_not_empty),
        error.DatabaseNotEmpty => status(.conflict, .database_not_empty),
        error.ProtectedCatalogResource => status(.conflict, .protected_catalog_resource),
        error.InvalidCatalogName => status(.invalid_argument, .invalid_catalog_name),
        error.InvalidCatalogMutation => status(.invalid_argument, .invalid_catalog_mutation),
        error.InvalidTablespaceLocation => status(.invalid_argument, .invalid_tablespace_location),
        error.InvalidTablespacePlacementPolicy => status(.invalid_argument, .invalid_tablespace_placement_policy),
        error.CatalogCommandTooLarge => status(.invalid_argument, .catalog_command_too_large),
        error.InvalidCatalogRecord => status(.corrupt, .invalid_catalog_record),
        error.RowPolicyCatalogChanged => status(.conflict, .row_policy_catalog_changed),
        error.CatalogIdExhausted => status(.internal, .catalog_id_exhausted),

        error.RelationalRewriteTypeChange => status(.invalid_argument, .relational_rewrite_type_change),
        error.RelationalRewriteColumnDrop => status(.invalid_argument, .relational_rewrite_column_drop),
        error.RelationalRewriteRequiresRelational => status(.invalid_argument, .relational_rewrite_requires_relational),
        error.RelationalRewriteBudgetExceeded => status(.invalid_argument, .relational_rewrite_budget_exceeded),
        error.RelationalExpressionOverflow => status(.invalid_argument, .relational_expression_overflow),
        error.RelationalExpressionDivisionByZero => status(.invalid_argument, .relational_expression_division_by_zero),
        error.SqlInvalidPowerArgument => status(.invalid_argument, .sql_invalid_power_argument),
        error.InvalidSchemaReaderRequirements => status(.invalid_argument, .invalid_schema_reader_requirements),
        error.SqlFeatureNotSupported => status(.unsupported, .sql_feature_not_supported),
        error.CatalogPublicationProofPending => status(.retryable, .catalog_publication_proof_pending),
        error.SqlIndexAlreadyExists => status(.already_exists, .sql_index_already_exists),
        error.SqlConstraintAlreadyExists => status(.already_exists, .sql_constraint_already_exists),
        error.UndefinedColumn => status(.invalid_argument, .undefined_column),
        error.ForeignKeyValidationRequired => status(.unsupported, .foreign_key_validation_required),
        error.ProgramLimitExceeded => status(.invalid_argument, .program_limit_exceeded),
        error.CatalogOccupancyNotReady => status(.retryable, .catalog_occupancy_not_ready),
        error.RelationalExpressionBudgetExceeded => status(.invalid_argument, .relational_expression_budget_exceeded),
        error.RelationalIndexKeyTooLarge => status(.invalid_argument, .relational_index_key_too_large),
        error.InvalidRelationalExpressionInput => status(.invalid_argument, .invalid_relational_expression_input),
        error.InvalidRelationalGeneratedValue => status(.invalid_argument, .invalid_relational_generated_value),
        error.GeneratedColumnRewriteRequired => status(.conflict, .generated_column_rewrite_required),
        error.CheckConstraintValidationRequired => status(.conflict, .check_constraint_validation_required),
        error.RestoreJobCommitNotApplied => status(.retryable, .restore_job_commit_not_applied),
        error.CoordinatedStandaloneHAMetadataRequired => status(.unsupported, .coordinated_standalone_ha_metadata_required),
        error.StorageBusy => status(.retryable, .storage_busy),
        error.StorageOwnerAdmissionPending => status(.retryable, .storage_owner_admission_pending),
        error.MergeTransitionNotReady => status(.retryable, .merge_transition_not_ready),
        error.MergeReceiverProjectionNotReady => status(.retryable, .merge_receiver_projection_not_ready),
        error.MergeSourceProjectionNotReady => status(.retryable, .merge_source_projection_not_ready),
        error.MergeSourceProjectionAdvanced => status(.retryable, .merge_source_projection_advanced),
        error.GroupLeaderUnavailable => status(.retryable, .group_leader_unavailable),
        error.IntentConflict => status(.conflict, .intent_conflict),
        error.VersionConflict => status(.conflict, .version_conflict),
        error.MergePageRequired => status(.conflict, .merge_page_required),
        error.InvalidResponse, error.UnexpectedEof => status(.internal, .invalid_response),
        error.TransitionOperationsRetired => status(.retryable, .transition_operations_retired),
        error.TransitionOperationBusy => status(.retryable, .transition_operation_busy),
        error.UnknownSplitRuntime => status(.not_found, .unknown_split_runtime),
        error.UnknownMergeRuntime => status(.not_found, .unknown_merge_runtime),
        error.MissingSplitRuntime => status(.not_found, .missing_split_runtime),
        error.MissingMergeRuntime => status(.not_found, .missing_merge_runtime),
        error.MetadataSnapshotUnavailable => status(.retryable, .metadata_snapshot_unavailable),
        error.MetadataSnapshotHeadMismatch => status(.retryable, .metadata_snapshot_head_mismatch),
        error.SplitSourceProjectionNotReady => status(.retryable, .split_source_projection_not_ready),
        error.SplitSourceProjectionAdvanced => status(.retryable, .split_source_projection_advanced),
        error.DurableRootIncarnationUnavailable => status(.retryable, .durable_root_incarnation_unavailable),
        error.AutoBulkIngestBusy => status(.retryable, .auto_bulk_ingest_busy),
        error.ApplyStoreGroupRetired => status(.retryable, .apply_store_group_retired),
        error.ApplyStoreShuttingDown => status(.retryable, .apply_store_shutting_down),
        error.InvalidRetainedEffectsAdmission => status(.invalid_argument, .invalid_retained_effects_admission),
        error.RetainedEffectsFenceMismatch => status(.conflict, .retained_effects_fence_mismatch),
        error.RetainedEffectsMixedControl => status(.invalid_argument, .retained_effects_mixed_control),
        error.RetainedEffectsTransactionFailed => status(.conflict, .retained_effects_transaction_failed),
        error.InvalidOnlineSourceCommand => status(.invalid_argument, .invalid_online_source_command),
        error.OnlineSourceCorrupt => status(.corrupt, .online_source_corrupt),
        error.OnlineSourcePinMissing => status(.not_found, .online_source_pin_missing),
        error.OnlineSourcePinPending => status(.retryable, .online_source_pin_pending),
        error.OnlineSourceScopeChanged => status(.conflict, .online_source_scope_changed),
        error.OnlineMergeArtifactCatalogUncoordinated => status(.conflict, .online_merge_artifact_catalog_uncoordinated),
        error.InvalidSourceSnapshot => status(.invalid_argument, .invalid_source_snapshot),
        error.SourceSnapshotCorrupt => status(.corrupt, .source_snapshot_corrupt),
        error.SourceSnapshotCutMismatch => status(.conflict, .source_snapshot_cut_mismatch),
        error.SourceSnapshotIncomplete => status(.retryable, .source_snapshot_incomplete),
        error.SourceSnapshotTooLarge => status(.invalid_argument, .source_snapshot_too_large),
        error.SourceCopyRestoreUnsupported => status(.unsupported, .source_copy_restore_unsupported),
        error.InvalidMergePage => status(.invalid_argument, .invalid_merge_page),
        error.MergeCopyFenced => status(.conflict, .merge_copy_fenced),
        error.MergePageChunkRequired => status(.invalid_argument, .merge_page_chunk_required),
        error.MergePageIncomplete => status(.retryable, .merge_page_incomplete),
        error.MergePageSequenceGap => status(.conflict, .merge_page_sequence_gap),
        error.RetainedEffectsConsumerLimit => status(.retryable, .retained_effects_consumer_limit),
        error.RetainedEffectsCorrupt => status(.corrupt, .retained_effects_corrupt),
        error.RetainedEffectsCursorMismatch => status(.conflict, .retained_effects_cursor_mismatch),
        error.RetainedEffectsFull => status(.retryable, .retained_effects_full),
        error.RetainedEffectsIdentityRequired => status(.conflict, .retained_effects_identity_required),
        error.RetainedEffectsNamespaceMismatch => status(.conflict, .retained_effects_namespace_mismatch),
        error.BackendRuntimeIoUnavailable => status(.unavailable, .backend_runtime_io_unavailable),
        error.CorruptRaftAppliedEntry, error.CorruptOrderedApplyReceipt => status(.corrupt, .corrupt_raft_applied_entry),
        error.UnknownSchemaVersion => status(.conflict, .unknown_schema_version),
        error.MetadataHABindingBusy => status(.retryable, .metadata_ha_binding_busy),
        error.MetadataHAOutboxPending => status(.retryable, .metadata_ha_outbox_pending),
        error.MetadataHACheckpointTargetNotEmpty => status(.conflict, .metadata_ha_checkpoint_target_not_empty),
        error.InvalidMetadataHACheckpoint => status(.conflict, .invalid_metadata_ha_checkpoint),
        error.MetadataHAMigrationAfterBinding => status(.conflict, .metadata_ha_migration_after_binding),
        error.MetadataHAIncompleteEffect => status(.conflict, .metadata_ha_incomplete_effect),
        error.MetadataHASourceChanged => status(.conflict, .metadata_ha_source_changed),
        error.MetadataHASequenceGap => status(.conflict, .metadata_ha_sequence_gap),
        error.InvalidMetadataHAEffectChunk => status(.conflict, .invalid_metadata_ha_effect_chunk),
        error.HAPrimaryNotConfigured => status(.unavailable, .ha_primary_not_configured),
        error.RelationalIndexNotReady => status(.conflict, .relational_index_not_ready),
        error.InvalidRelationalIndexBound => status(.invalid_argument, .invalid_relational_index_bound),
        error.RelationalIndexColumnNotFound => status(.invalid_argument, .relational_index_column_not_found),
        error.UnsupportedRelationalIndexColumn => status(.unsupported, .unsupported_relational_index_column),
        error.RelationalRowsOutputBudgetExceeded => status(.invalid_argument, .relational_rows_output_budget_exceeded),
        error.RelationalRowResultTooLarge => status(.invalid_argument, .relational_row_result_too_large),
        error.RelationalIndexColumnTypeMismatch => status(.conflict, .relational_index_column_type_mismatch),
        error.RelationalTableRequired => status(.invalid_argument, .relational_table_required),
        error.InvalidRelationalIndexForwardKey => status(.corrupt, .invalid_relational_index_forward_key),
        error.IntegrityTopologyCutoverRequired => status(.conflict, .integrity_topology_cutover_required),
        error.InvalidRestoreTerminal => status(.conflict, .invalid_restore_terminal),
        error.InvalidStandaloneMetadataCheckpoint => status(.conflict, .invalid_standalone_metadata_checkpoint),
        error.SeedBelowHAReplayFloor => status(.conflict, .seed_below_ha_replay_floor),
        error.SeedMetadataTopologyMismatch => status(.conflict, .seed_metadata_topology_mismatch),
        error.BackupSealReleased => status(.conflict, .backup_seal_released),
        error.IntegrityAddressMismatch => status(.conflict, .integrity_address_mismatch),
        error.IntegrityHandoffCollision => status(.conflict, .integrity_handoff_collision),
        error.IntegrityHandoffDestinationResetRequired => status(.conflict, .integrity_handoff_destination_reset_required),
        error.IntegrityHandoffIncomplete => status(.conflict, .integrity_handoff_incomplete),
        error.IntegrityHandoffMissing => status(.conflict, .integrity_handoff_missing),
        error.IntegrityHandoffSequenceChanged => status(.conflict, .integrity_handoff_sequence_changed),
        error.MissingIntegrityBinding => status(.conflict, .missing_integrity_binding),
        error.MissingIntegrityCatalog => status(.conflict, .missing_integrity_catalog),
        error.IntegrityCatalogTooLarge => status(.invalid_argument, .integrity_catalog_too_large),
        error.IntegrityHandoffTooLarge => status(.invalid_argument, .integrity_handoff_too_large),
        error.InvalidIntegrityAddress => status(.invalid_argument, .invalid_integrity_address),
        error.InvalidIntegrityBudget => status(.invalid_argument, .invalid_integrity_budget),
        error.InvalidIntegrityCatalog => status(.invalid_argument, .invalid_integrity_catalog),
        error.InvalidIntegrityDefinition => status(.invalid_argument, .invalid_integrity_definition),
        error.InvalidIntegrityRecord => status(.invalid_argument, .invalid_integrity_record),
        error.CoordinatedConstraintsRequireTableIdentity => status(.invalid_argument, .coordinated_constraints_require_table_identity),
        error.IntegrityRetirementBacklogFull => status(.retryable, .integrity_retirement_backlog_full),
        error.RestoreProjectionCatchUpPending => status(.retryable, .restore_projection_catch_up_pending),
        error.IntegrityChecksumMismatch => status(.corrupt, .integrity_checksum_mismatch),
        error.IntegrityMissingCompanion => status(.corrupt, .integrity_missing_companion),
        error.CoordinatedConstraintPortableBackupUnsupported => status(.unsupported, .coordinated_constraint_portable_backup_unsupported),
        error.CoordinatedConstraintRestoreRequired => status(.unsupported, .coordinated_constraint_restore_required),
        error.CoordinatedConstraintTopologyUnsupported => status(.unsupported, .coordinated_constraint_topology_unsupported),
        error.IntegrityGenerationExhausted => status(.unavailable, .integrity_generation_exhausted),
        error.BackupCohortAlreadyCommitted => status(.conflict, .backup_cohort_already_committed),
        error.BackupCohortCancelled => status(.conflict, .backup_cohort_cancelled),
        error.BackupCohortChanged => status(.conflict, .backup_cohort_changed),
        error.BackupCohortFenceLost => status(.conflict, .backup_cohort_fence_lost),
        error.BackupSealMismatch => status(.conflict, .backup_seal_mismatch),
        error.BackupSealSourceChanged => status(.conflict, .backup_seal_source_changed),
        error.RestoreStagingCanceled => status(.conflict, .restore_staging_canceled),
        error.RestoreStagingProgressChanged => status(.conflict, .restore_staging_progress_changed),
        error.RestoreStagingScopeChanged => status(.conflict, .restore_staging_scope_changed),
        error.IntegrityTopologyChanged => status(.conflict, .integrity_topology_changed),
        error.IntegrityTopologyCompleted => status(.conflict, .integrity_topology_completed),
        error.IntegrityTopologyFenceMissing => status(.conflict, .integrity_topology_fence_missing),
        error.IntegrityCatalogChanged => status(.conflict, .integrity_catalog_changed),
        error.IntegrityCatalogIncarnationMismatch => status(.conflict, .integrity_catalog_incarnation_mismatch),
        error.ConstraintActivationChanged => status(.conflict, .constraint_activation_changed),
        error.ConstraintActivationOwnerChanged => status(.conflict, .constraint_activation_owner_changed),
        error.ConstraintRetirementChanged => status(.conflict, .constraint_retirement_changed),
        error.ForeignKeyParentMissing => status(.conflict, .foreign_key_parent_missing),
        error.ForeignKeyReferenced => status(.conflict, .foreign_key_referenced),
        error.ForeignKeyMatchFullViolation => status(.conflict, .foreign_key_match_full_violation),
        error.UniqueConstraintViolation => status(.conflict, .unique_constraint_violation),
        error.RelationalCheckViolation => status(.invalid_argument, .relational_check_violation),
        error.UnsupportedNativeIndexOwner => status(.unsupported, .unsupported_native_index_owner),
        error.InvalidPrivateIndexClaimInventory => status(.invalid_argument, .invalid_private_index_claim_inventory),
        error.InvalidPrivateIndexClaimReceipt => status(.corrupt, .invalid_private_index_claim_receipt),
        error.PrivateIndexOwnerJobChanged => status(.conflict, .private_index_owner_job_changed),
        error.PrivateIndexPublicationCanceled => status(.conflict, .private_index_publication_canceled),
        error.PrivateIndexJobRequired => status(.retryable, .private_index_job_required),
        error.PrivateIndexRootRecoveryRequired => status(.retryable, .private_index_root_recovery_required),
        error.InvalidRelationalIndexBudget => status(.invalid_argument, .invalid_relational_index_budget),
        error.PrivateIndexClaimBacklogFull => status(.retryable, .private_index_claim_backlog_full),
        error.PrivateIndexPublicationRequired => status(.retryable, .private_index_publication_required),
        error.InvalidPrivateIndexOwnerJob => status(.corrupt, .invalid_private_index_owner_job),
        error.InvalidPrivateIndexInputs => status(.corrupt, .invalid_private_index_inputs),
        error.RelationalIndexCatalogSchemaMismatch => status(.conflict, .relational_index_catalog_schema_mismatch),
        error.InvalidIndexPublicationCandidate => status(.invalid_argument, .invalid_index_publication_candidate),
        error.ImmutableRelationalIndexDefinitionConflict => status(.conflict, .immutable_relational_index_definition_conflict),
        error.PrivateIndexCancellationPending => status(.retryable, .private_index_cancellation_pending),
        error.PrivateIndexCancellationRequired => status(.retryable, .private_index_cancellation_required),
        error.InvalidPrivateIndexCancellation => status(.corrupt, .invalid_private_index_cancellation),
        error.InvalidPrivateIndexNativeReceipt => status(.corrupt, .invalid_private_index_native_receipt),
        error.InvalidRelationalIndexProgress => status(.corrupt, .invalid_relational_index_progress),
        error.MissingRelationalIndexDefinitions => status(.corrupt, .missing_relational_index_definitions),
        error.IndexOwnerReceiptChanged => status(.conflict, .index_owner_receipt_changed),
        error.PartialIndexPredicateNotImplied => status(.invalid_argument, .partial_index_predicate_not_implied),
        error.InvalidIndexOwnerReceipt => status(.invalid_argument, .invalid_index_owner_receipt),
        error.PreparedGenerationChanged => status(.conflict, .prepared_generation_changed),
        error.PreparedReadSetChanged => status(.conflict, .prepared_read_set_changed),
        error.ForeignKeyActionMismatch => status(.conflict, .foreign_key_action_mismatch),
        error.ForeignKeyActionNotValidated => status(.conflict, .foreign_key_action_not_validated),
        error.ConstraintActivationFailed => status(.conflict, .constraint_activation_failed),
        error.ForeignKeyActionFailed => status(.conflict, .foreign_key_action_failed),
        error.RestoreStagingTargetNotEmpty => status(.conflict, .restore_staging_target_not_empty),
        error.BackupPinSourceUnavailable => status(.retryable, .backup_pin_source_unavailable),
        error.RestoreSourceDurabilityUncertain => status(.retryable, .restore_source_durability_uncertain),
        error.RestoreStagingInProgress => status(.retryable, .restore_staging_in_progress),
        error.RestoreValidationPending => status(.retryable, .restore_validation_pending),
        error.IntegrityTopologyBusy => status(.retryable, .integrity_topology_busy),
        error.TransactionTopologyBusy => status(.retryable, .transaction_topology_busy),
        error.RelationalTopologyProtocolUpgradeRequired => status(.retryable, .relational_topology_protocol_upgrade_required),
        error.IntegrityCatalogUnavailable => status(.retryable, .integrity_catalog_unavailable),
        error.ConstraintRetirementInProgress => status(.retryable, .constraint_retirement_in_progress),
        error.ConstraintActivationInProgress => status(.retryable, .constraint_activation_in_progress),
        error.ForeignKeyActionInProgress => status(.retryable, .foreign_key_action_in_progress),
        error.CoordinatedTtlBackpressure => status(.retryable, .coordinated_ttl_backpressure),
        error.MetadataCapabilityUnavailable => status(.retryable, .metadata_capability_unavailable),
        error.BackupCohortTooLarge => status(.invalid_argument, .backup_cohort_too_large),
        error.IncompleteBackupCohort => status(.invalid_argument, .incomplete_backup_cohort),
        error.InvalidBackupCohort => status(.invalid_argument, .invalid_backup_cohort),
        error.BackupSealInventoryTooLarge => status(.invalid_argument, .backup_seal_inventory_too_large),
        error.InvalidBackupSeal => status(.invalid_argument, .invalid_backup_seal),
        error.RestoreDependencyMissing => status(.invalid_argument, .restore_dependency_missing),
        error.RestoreSourceProofMissing => status(.invalid_argument, .restore_source_proof_missing),
        error.InvalidRestoreSourceCheckpoint => status(.invalid_argument, .invalid_restore_source_checkpoint),
        error.InvalidRestoreStaging => status(.invalid_argument, .invalid_restore_staging),
        error.InvalidRestoreStagingCommand => status(.invalid_argument, .invalid_restore_staging_command),
        error.InvalidRestoreStagingRecord => status(.invalid_argument, .invalid_restore_staging_record),
        error.InvalidIntegrityTopologyFence => status(.invalid_argument, .invalid_integrity_topology_fence),
        error.InvalidMetadataHAEffect => status(.invalid_argument, .invalid_metadata_ha_effect),
        error.InvalidMetadataHAChunk => status(.invalid_argument, .invalid_metadata_ha_chunk),
        error.MetadataHACheckpointTooLarge => status(.invalid_argument, .metadata_ha_checkpoint_too_large),
        error.MetadataHAEffectTooLarge => status(.invalid_argument, .metadata_ha_effect_too_large),
        error.InvalidConstraintActivationCommand => status(.invalid_argument, .invalid_constraint_activation_command),
        error.InvalidConstraintRetirementCommand => status(.invalid_argument, .invalid_constraint_retirement_command),
        error.KeyOutOfRange => status(.invalid_argument, .key_out_of_range),
        error.InvalidIntegrityCommand => status(.invalid_argument, .invalid_integrity_command),
        error.InvalidIntegrityOperation => status(.invalid_argument, .invalid_integrity_operation),
        error.InvalidIntegrityContinuation => status(.invalid_argument, .invalid_integrity_continuation),
        error.IntegrityClaimGuardRequired => status(.invalid_argument, .integrity_claim_guard_required),
        error.IntegrityActionJobRequired => status(.invalid_argument, .integrity_action_job_required),
        error.InvalidRelationalRowsRequest => status(.invalid_argument, .invalid_relational_rows_request),
        error.ConstraintRetirementRequired => status(.invalid_argument, .constraint_retirement_required),
        error.InvalidIntegrityKey => status(.invalid_argument, .invalid_integrity_key),
        error.IntegrityRecordTooLarge => status(.invalid_argument, .integrity_record_too_large),
        error.InvalidRange => status(.invalid_argument, .invalid_range),
        error.BackupSealBackendUnsupported => status(.unsupported, .backup_seal_backend_unsupported),
        error.RelationalTopologyMigrationUnsupported => status(.unsupported, .relational_topology_migration_unsupported),
        error.ForeignKeyCoordinationRequired => status(.unsupported, .foreign_key_coordination_required),
        error.ConstraintNotFound => status(.not_found, .constraint_not_found),
        error.IntegrityTopologyEpochExhausted => status(.unavailable, .integrity_topology_epoch_exhausted),
        error.OutOfMemory => status(.out_of_memory, .out_of_memory),
        error.InvalidArgument => status(.invalid_argument, .invalid_argument),
        error.InvalidArguments => status(.invalid_argument, .invalid_arguments),
        error.InvalidRequest => status(.invalid_argument, .invalid_request),
        // Aggregations may run in either an owner or a coordinator. Transport
        // their public query classes using existing details, including when
        // they cross a callback before the HTTP boundary can normalize them.
        error.InvalidQueryRequest, error.InvalidAggregation => status(.invalid_argument, .invalid_query_request),
        error.InvalidFilterQueryRequest => status(.invalid_argument, .invalid_filter_query_request),
        error.InvalidExclusionQueryRequest => status(.invalid_argument, .invalid_exclusion_query_request),
        error.InvalidBatchRequest => status(.invalid_argument, .invalid_batch_request),
        error.InvalidCreateTableRequest => status(.invalid_argument, .invalid_create_table_request),
        error.UnsupportedCreateTableRequest => status(.unsupported, .unsupported_create_table_request),
        error.InvalidSchemaUpdateRequest => status(.invalid_argument, .invalid_schema_update_request),
        error.InvalidManifest => status(.invalid_argument, .invalid_manifest),
        error.InvalidTableFile => status(.invalid_argument, .invalid_table_file),
        error.InvalidTxnRecord => status(.invalid_argument, .invalid_txn_record),
        error.NotFound => status(.not_found, .not_found),
        error.FileNotFound => status(.not_found, .file_not_found),
        error.TableNotFound => status(.not_found, .table_not_found),
        error.TableAlreadyExists => status(.already_exists, .table_already_exists),
        error.IndexNotFound => status(.not_found, .index_not_found),
        error.ModelNotFound => status(.not_found, .model_not_found),
        error.SecretNotFound => status(.not_found, .secret_not_found),
        error.UserNotFound => status(.not_found, .user_not_found),
        error.UnknownGroup => status(.not_found, .unknown_group),
        error.TxnNotFound => status(.not_found, .txn_not_found),
        error.Conflict => status(.conflict, .conflict),
        error.DecisionConflict => status(.conflict, .decision_conflict),
        error.TableTransitionActive => status(.conflict, .table_transition_active),
        error.SchemaInUse => status(.conflict, .schema_in_use),
        error.TransactionTooLarge => status(.invalid_argument, .transaction_too_large),
        error.ExtensionOwnedObject => status(.conflict, .extension_owned_object),
        error.RestoreIntentConflict => status(.conflict, .restore_intent_conflict),
        error.Unauthorized => status(.unauthorized, .unauthorized),
        error.Forbidden => status(.forbidden, .forbidden),
        // Capability barriers can be unavailable during provisioning or a
        // rolling upgrade. They fail before batch proposal admission.
        error.Unavailable, error.RaftBatchMergeProtocolUnavailable, error.RaftBatchSplitDeltaProtocolUnavailable => status(.unavailable, .unavailable),
        error.WriteUnavailable => status(.unavailable, .write_unavailable),
        error.ReadUnavailable => status(.unavailable, .read_unavailable),
        error.ReadRequiresPrimary => status(.unavailable, .read_requires_primary),
        error.HAReadRequiresPrimary => status(.unavailable, .ha_read_requires_primary),
        error.HAReadWaitForApply => status(.retryable, .ha_read_wait_for_apply),
        error.HAReadWaitForMetadata => status(.retryable, .ha_read_wait_for_metadata),
        error.DistributedQueryUnavailable => status(.retryable, .distributed_query_unavailable),
        error.MetricNotReady => status(.retryable, .metric_not_ready),
        error.MetricStale => status(.retryable, .metric_stale),
        error.StorageReadTemporarilyUnavailable => status(.retryable, .storage_read_temporarily_unavailable),
        error.PersistentDescriptorAdmissionExhausted => status(.retryable, .persistent_descriptor_admission_exhausted),
        error.ProcessFdQuotaExceeded => status(.retryable, .process_fd_quota_exceeded),
        error.SystemFdQuotaExceeded => status(.retryable, .system_fd_quota_exceeded),
        error.StorageDescriptorPressure => status(.retryable, .storage_descriptor_pressure),
        error.InputOutput => status(.internal, .input_output),
        error.ResourceRequestTooLarge => status(.invalid_argument, .resource_request_too_large),
        error.ResourceTemporarilyUnavailable => status(.retryable, .resource_temporarily_unavailable),
        error.ConcurrencyUnavailable => status(.retryable, .concurrency_unavailable),
        error.RestoreJobPersistenceUnavailable => status(.unavailable, .restore_job_persistence_unavailable),
        error.Backpressured => status(.retryable, .backpressured),
        error.DenseRepairBackpressure => status(.retryable, .dense_repair_backpressure),
        error.OutcomeUnknown => status(.retryable, .outcome_unknown),
        error.CommitPropagationIncomplete => status(.retryable, .commit_propagation_incomplete),
        error.CommitDecisionUnknown => status(.retryable, .commit_decision_unknown),
        error.CommitVisibilityNotSatisfied => status(.retryable, .commit_visibility_not_satisfied),
        error.TransactionBeginFailed => status(.unavailable, .transaction_begin_failed),
        error.AbortDecisionNotDurable => status(.unavailable, .abort_decision_not_durable),
        error.CommittedPending => status(.retryable, .committed_pending),
        error.WriteOutcomeUnknown => status(.retryable, .write_outcome_unknown),
        error.RaftBatchWriteOutcomeUnknown => status(.retryable, .raft_batch_write_outcome_unknown),
        error.RaftApplyWriterUnavailable => status(.retryable, .raft_apply_writer_unavailable),
        error.SqlDatabaseInUse => status(.conflict, .sql_database_in_use),
        error.InvalidSqlRelationPersistence => status(.invalid_argument, .invalid_sql_relation_persistence),
        error.SqlRelationOwnerChanged => status(.conflict, .sql_relation_owner_changed),
        error.SqlTemporaryRelationNotOwned => status(.forbidden, .sql_temporary_relation_not_owned),
        error.InvalidSqlTemporaryNamespace => status(.invalid_argument, .invalid_sql_temporary_namespace),
        error.SqlRelationPersistenceForeignKeyMismatch => status(.invalid_argument, .sql_relation_persistence_foreign_key_mismatch),
        error.SqlRelationRecoveryIncomplete => status(.unavailable, .sql_relation_recovery_incomplete),
        error.SqlRelationOwnerUnavailable => status(.unavailable, .sql_relation_owner_unavailable),
        error.InvalidScopeRetirement => status(.invalid_argument, .invalid_scope_retirement),
        error.ScopeRetirementChanged => status(.conflict, .scope_retirement_changed),
        error.SequenceAdmissionConflict => status(.conflict, .sequence_admission_conflict),
        error.InvalidTableRetirementManifest => status(.invalid_argument, .invalid_table_retirement_manifest),
        error.CatalogRevisionExhausted => status(.invalid_argument, .catalog_revision_exhausted),
        error.DuplicateSqlExtensionExport => status(.invalid_argument, .duplicate_sql_extension_export),
        error.DuplicateSqlExtensionVersion => status(.invalid_argument, .duplicate_sql_extension_version),
        error.InconsistentSqlExtensionControl => status(.corrupt, .inconsistent_sql_extension_control),
        error.InvalidPackageDigest => status(.corrupt, .invalid_package_digest),
        error.InvalidSqlExtensionArtifact => status(.unsupported, .invalid_sql_extension_artifact),
        error.InvalidSqlExtensionControl => status(.invalid_argument, .invalid_sql_extension_control),
        error.InvalidSqlExtensionDependency => status(.invalid_argument, .invalid_sql_extension_dependency),
        error.InvalidSqlExtensionRecord => status(.corrupt, .invalid_sql_extension_record),
        error.InvalidSqlExtensionRuntime => status(.unsupported, .invalid_sql_extension_runtime),
        error.InvalidSqlExtensionPackageMutation => status(.invalid_argument, .invalid_sql_extension_package_mutation),
        error.DuplicateSqlExtensionControl => status(.invalid_argument, .duplicate_sql_extension_control),
        error.SqlDependentExtensionObject => status(.conflict, .sql_dependent_extension_object),
        error.SqlDuplicateExtension => status(.already_exists, .sql_duplicate_extension),
        error.SqlDuplicateFunction => status(.already_exists, .sql_duplicate_function),
        error.SqlExtensionControlConflict => status(.conflict, .sql_extension_control_conflict),
        error.SqlExtensionDependencyCycle => status(.invalid_argument, .sql_extension_dependency_cycle),
        error.SqlExtensionPromotionNonceConflict => status(.conflict, .sql_extension_promotion_nonce_conflict),
        error.SqlExtensionSchemaMismatch => status(.invalid_argument, .sql_extension_schema_mismatch),
        error.SqlExtensionUnavailable => status(.unsupported, .sql_extension_unavailable),
        error.SqlExtensionUpdatePathUnavailable => status(.invalid_argument, .sql_extension_update_path_unavailable),
        error.SqlExtensionVersionUnavailable => status(.not_found, .sql_extension_version_unavailable),
        error.SqlFunctionReturnTypeChanged => status(.invalid_argument, .sql_function_return_type_changed),
        error.SqlRequiredExtensionNotInstalled => status(.not_found, .sql_required_extension_not_installed),
        error.SqlUndefinedExtension => status(.not_found, .sql_undefined_extension),
        error.UnsupportedSqlExtensionObject => status(.unsupported, .unsupported_sql_extension_object),
        error.SqlExtensionOwnedObject => status(.conflict, .sql_extension_owned_object),
        error.DuplicateShapeName => status(.invalid_argument, .duplicate_shape_name),
        error.GeneratedArtifactShapeRequired => status(.invalid_argument, .generated_artifact_shape_required),
        error.NoSupportedScopes => status(.invalid_argument, .no_supported_scopes),
        error.ScopeMustNotNameTable => status(.invalid_argument, .scope_must_not_name_table),
        error.UnknownShapeReference => status(.invalid_argument, .unknown_shape_reference),
        error.SqlProgramLimitExceeded => status(.invalid_argument, .sql_program_limit_exceeded),
        error.SnapshotBuildCancelled => status(.cancelled, .snapshot_build_cancelled),
        error.CatalogGenerationExhausted => status(.invalid_argument, .catalog_generation_exhausted),
        error.InvalidSqlTriggerDefinition => status(.invalid_argument, .invalid_sql_trigger_definition),
        error.InvalidSqlTriggerManifest => status(.invalid_argument, .invalid_sql_trigger_manifest),
        error.SqlTriggerContextRequired => status(.invalid_argument, .sql_trigger_context_required),
        error.SqlTriggerReturnedNoValue => status(.invalid_argument, .sql_trigger_returned_no_value),
        error.SqlTriggerReturnValueRequired => status(.invalid_argument, .sql_trigger_return_value_required),
        error.SqlTriggerDeclaredArguments => status(.invalid_argument, .sql_trigger_declared_arguments),
        error.SqlTriggerSqlLanguage => status(.invalid_argument, .sql_trigger_sql_language),
        error.SqlTriggerCoordinationRequired => status(.invalid_argument, .sql_trigger_coordination_required),
        error.InvalidGenerationTransaction => status(.invalid_argument, .invalid_generation_transaction),
        error.GenerationTransactionChanged => status(.conflict, .generation_transaction_changed),
        error.GenerationTransactionDecisionRequired => status(.conflict, .generation_transaction_decision_required),
        error.InvalidPreparedGraphEffects => status(.invalid_argument, .invalid_prepared_graph_effects),
        error.SqlPreparedDurabilityUnavailable => status(.invalid_argument, .sql_prepared_durability_unavailable),
        error.InvalidParticipant => status(.invalid_argument, .invalid_participant),

        error.RaftBatchWriteTransportOutcomeUnknown => status(.retryable, .raft_batch_write_transport_outcome_unknown),
        error.HttpConnectionClosing => status(.unavailable, .http_connection_closing),
        error.RaftBatchWritePartialOutcome => status(.retryable, .raft_batch_write_partial_outcome),
        error.GraphMetricActionPartialOutcome => status(.retryable, .graph_metric_action_partial_outcome),
        error.DocIdentityUnavailable => status(.retryable, .doc_identity_unavailable),
        error.HAReadOnlyStandby => status(.unavailable, .ha_read_only_standby),
        error.HAPromotedStandbyRequiresPrimaryOpen => status(.unavailable, .ha_promoted_standby_requires_primary_open),
        error.HAFencedPrimary => status(.conflict, .ha_fenced_primary),
        error.HASyncCommitWouldBlock => status(.unavailable, .ha_sync_commit_would_block),
        error.HASyncCommitWaitLimitExceeded => status(.unavailable, .ha_sync_commit_wait_limit_exceeded),
        error.HASyncCommitWaitMissingContext => status(.unavailable, .ha_sync_commit_wait_missing_context),
        error.HASyncCommitWaitStandbyNotInPolicy => status(.unavailable, .ha_sync_commit_wait_standby_not_in_policy),
        error.InternalFailure => status(.internal, .internal_failure),
        error.NotLeader => status(.retryable, .not_leader),
        error.LeaderUnavailable => status(.unavailable, .leader_unavailable),
        error.TopologyChanged => status(.retryable, .topology_changed),
        error.IndexGenerationMismatch => status(.retryable, .index_generation_mismatch),
        error.IdentityReadGenerationChanged => status(.conflict, .identity_read_generation_changed),
        error.DocIdentityNamespaceMismatch => status(.conflict, .doc_identity_namespace_mismatch),
        error.TableGenerationChanged => status(.conflict, .table_generation_changed),
        error.TableLifecycleConflict => status(.conflict, .table_lifecycle_conflict),
        error.GenerationDurabilityUncertain => status(.retryable, .generation_durability_uncertain),
        error.GenerationTransitionActive => status(.retryable, .generation_transition_active),
        error.IndexRebuilding => status(.retryable, .index_rebuilding),
        error.ReembedBudgetExceeded => status(.unavailable, .reembed_budget_exceeded),
        error.ReembedSetupRetry => status(.unavailable, .reembed_setup_retry),
        error.IncompletePublishedSnapshot => status(.retryable, .incomplete_published_snapshot),
        error.TableVisibilityTimeout => status(.timeout, .table_visibility_timeout),
        error.FileBusy => status(.retryable, .file_busy),
        error.WouldBlock => status(.retryable, .would_block),
        error.WriterLocked => status(.retryable, .writer_locked),
        error.LsmRootWriterAlreadyOpen => status(.retryable, .lsm_root_writer_already_open),
        error.UnexpectedHttpStatus => status(.retryable, .unexpected_http_status),
        error.MethodNotAllowed => status(.unsupported, .method_not_allowed),
        error.Unsupported => status(.unsupported, .unsupported),
        error.UnsupportedOperation => status(.unsupported, .unsupported_operation),
        error.UnsupportedTensorType => status(.unsupported, .unsupported_tensor_type),
        error.UnsupportedTransformOperation => status(.invalid_argument, .unsupported_transform_operation),
        error.InvalidGraphEdges => status(.invalid_argument, .invalid_graph_edges),
        error.InvalidTableIndexMetadata => status(.invalid_argument, .invalid_table_index_metadata),
        error.InvalidEnrichmentConfig => status(.invalid_argument, .invalid_enrichment_config),
        error.UnsupportedQueryRequest, error.UnsupportedAggregation => status(.unsupported, .unsupported_query_request),
        error.UnsupportedHierarchyGrouping => status(.unsupported, .unsupported_query_request),
        error.UnsupportedNodeFilterQuery => status(.invalid_argument, .unsupported_node_filter_query),
        error.UnsupportedFilterQueryRequest => status(.unsupported, .unsupported_filter_query_request),
        error.UnsupportedExclusionQueryRequest => status(.unsupported, .unsupported_exclusion_query_request),
        error.UnsupportedSyncLevel => status(.unsupported, .unsupported_sync_level),
        error.UnsupportedExactSort => status(.unsupported, .unsupported_exact_sort),
        error.UnsupportedVersion => status(.unsupported, .unsupported_version),
        error.UnsupportedPlatform => status(.unsupported, .unsupported_platform),
        error.Timeout => status(.timeout, .timeout),
        error.ReadIndexTimeout => status(.timeout, .read_index_timeout),
        error.DeadlineExceeded => status(.timeout, .deadline_exceeded),
        error.PreDecisionDeadlineExceeded => status(.timeout, .pre_decision_deadline_exceeded),
        error.ConnectionTimeout => status(.timeout, .connection_timeout),
        error.ConnectionTimedOut => status(.timeout, .connection_timed_out),
        error.Cancelled => status(.cancelled, .cancelled),
        error.Canceled => status(.cancelled, .canceled),
        error.ConnectionRefused => status(.unavailable, .connection_refused),
        error.ConnectionResetByPeer => status(.unavailable, .connection_reset_by_peer),
        error.NetworkUnreachable => status(.unavailable, .network_unreachable),
        error.HostUnreachable => status(.unavailable, .host_unreachable),
        error.TemporaryNameServerFailure => status(.retryable, .temporary_name_server_failure),
        error.NameServerFailure => status(.unavailable, .name_server_failure),
        error.QueryCandidateBudgetExceeded => status(.unavailable, .query_candidate_budget_exceeded),
        error.ValueSearchRequiresScan => status(.unsupported, .value_verification_requires_scan),
        error.ValueSearchLimitExceeded => status(.invalid_argument, .value_verification_limit_exceeded),
        error.DecisionLimitExceeded => status(.invalid_argument, .decision_limit_exceeded),
        error.GraphWorkBudgetExceeded => status(.invalid_argument, .graph_work_budget_exceeded),
        error.GraphMinWeightDomainViolation => status(.invalid_argument, .graph_min_weight_domain_violation),
        error.GraphMaxWeightDomainViolation => status(.invalid_argument, .graph_max_weight_domain_violation),
        error.GraphPathWeightOverflow => status(.invalid_argument, .graph_path_weight_overflow),
        error.GraphDistinctBudgetExceeded => status(.invalid_argument, .graph_distinct_budget_exceeded),
        error.GraphAnchorFilterRequiresIndex => status(.unsupported, .graph_anchor_filter_requires_index),
        error.GraphMatchOperationLimitExceeded => status(.invalid_argument, .graph_match_operation_limit_exceeded),
        // Preserve the append-only runtime detail code while the public error
        // name reflects that unsupported exact modes are not topology-specific.
        error.GraphQueryModeUnsupported => status(.unsupported, .graph_cross_range_mode_unsupported),
        error.GraphStartIdentityRequired => status(.unsupported, .graph_start_identity_required),
        error.QueryEmbeddingInputTooLarge => status(.invalid_argument, .query_embedding_input_too_large),
        error.QueryEmbeddingOverloaded => status(.unavailable, .query_embedding_overloaded),
        error.EmbedRateLimited => status(.retryable, .embed_rate_limited),
        error.EmbedTransientFailure => status(.retryable, .embed_transient_failure),
        error.EmbedUpstreamFailure => status(.unavailable, .embed_upstream_failure),
        error.EmbeddingProbeUnavailable => status(.unavailable, .embedding_probe_unavailable),
        error.InferenceEndpointAccessDenied => status(.forbidden, .inference_endpoint_access_denied),
        error.InvalidEmbeddingResponse => status(.invalid_argument, .invalid_embedding_response),
        error.InvalidEmbeddingDimensions => status(.invalid_argument, .invalid_embedding_dimensions),
        error.BackupAlreadyExists => status(.already_exists, .backup_already_exists),
        error.BackupManifestTooLarge => status(.invalid_argument, .backup_manifest_too_large),
        // Reuse the stable wire detail: both errors tell a remote caller that
        // the archive's metadata exceeds the accepted manifest envelope.
        error.BackupSchemaHistoryTooLarge => status(.invalid_argument, .backup_manifest_too_large),
        error.BackupIntegrityFailure => status(.corrupt, .backup_integrity_failure),
        error.BackupArtifactIntegrityMismatch => status(.corrupt, .backup_artifact_integrity_mismatch),
        error.BackupRepositoryBusy => status(.retryable, .backup_repository_busy),
        error.InvalidBackupRequest => status(.invalid_argument, .invalid_backup_request),
        error.RestoreDurabilityPending => status(.retryable, .restore_durability_pending),
        // This is still transported as an error-union failure by existing
        // callers.  Never encode it as `ok`: doing so would discard the
        // detail before the receiving unit can reconstruct the local error.
        error.RestoreDurabilityConfirmed => status(.retryable, .restore_durability_confirmed),
        error.CorruptInput => status(.corrupt, .corrupt_input),
        error.Corrupted => status(.corrupt, .corrupted),
        error.CorruptRestoreJobStore => status(.corrupt, .corrupt_restore_job_store),
        error.TableBlockChecksumMismatch => status(.corrupt, .table_block_checksum_mismatch),
        error.ValueTooLong => status(.invalid_argument, .value_too_long),
        error.EnrichmentRetryInProgress => status(.retryable, .enrichment_retry_in_progress),
        error.ResourceBudgetExceeded => status(.retryable, .resource_budget_exceeded),
        error.TextMergeBackpressureTimeout => status(.retryable, .text_merge_backpressure_timeout),
        error.TextMergeBackpressureUnavailable => status(.retryable, .text_merge_backpressure_unavailable),
        error.TextMergeRuntimeShutdown => status(.unavailable, .text_merge_runtime_shutdown),
        error.BackgroundOwnerClosing => status(.unavailable, .background_owner_closing),
        error.RestoreIdentityMismatch => status(.conflict, .restore_identity_mismatch),
        error.TableReadChurn => status(.retryable, .table_read_churn),
        error.UnsupportedMultiRangeTable => status(.unsupported, .unsupported_multi_range_table),
        error.ProposalDropped => status(.retryable, .proposal_dropped),
        error.LeaderTransferInProgress => status(.retryable, .leader_transfer_in_progress),
        error.MetadataLinearizableReadTimeout => status(.timeout, .metadata_linearizable_read_timeout),
        error.ReconcileLeaseNotHeld => status(.retryable, .reconcile_lease_not_held),
        error.MetadataProposalSuperseded => status(.conflict, .metadata_proposal_superseded),
        error.MetadataProposalApplyTimeout => status(.timeout, .metadata_proposal_apply_timeout),
        error.MetadataMutationOutcomeUnknown => status(.conflict, .metadata_mutation_outcome_unknown),
        error.MissingEmbeddingArtifactProducer => status(.invalid_argument, .missing_embedding_artifact_producer),
        error.InvalidEmbeddingArtifactProducer => status(.invalid_argument, .invalid_embedding_artifact_producer),
        error.EmbeddingArtifactDimensionRequired => status(.invalid_argument, .embedding_artifact_dimension_required),
        error.ConflictingEmbeddingArtifactDimensions => status(.invalid_argument, .conflicting_embedding_artifact_dimensions),
        error.NativeBackupRepairStateNotQuiescent => status(.retryable, .native_backup_repair_state_not_quiescent),
        error.NativeBackupProjectionNotQuiescent => status(.retryable, .native_backup_projection_not_quiescent),
        error.NativeBackupProjectionRepairFailed => status(.corrupt, .native_backup_projection_repair_failed),
        error.NativeBackupProjectionRepairPaused => status(.conflict, .native_backup_projection_repair_paused),
        error.RerankerCandidateLimitExceeded => status(.invalid_argument, .reranker_candidate_limit_exceeded),
        error.RerankRateLimited => status(.retryable, .rerank_rate_limited),
        error.RerankTransientFailure => status(.retryable, .rerank_transient_failure),
        error.RerankUpstreamFailure => status(.unavailable, .rerank_upstream_failure),
        error.IncompatibleModel => status(.invalid_argument, .incompatible_model),
        error.UnsupportedGeneratorProvider => status(.unsupported, .unsupported_generator_provider),
        error.GenerateRequestFailed => status(.unavailable, .generate_request_failed),
        error.GenerationCapacityUnavailable => status(.retryable, .generation_capacity_unavailable),
        error.InvalidGeneratedToolArguments => status(.retryable, .invalid_generated_tool_arguments),
        error.RateLimit => status(.retryable, .generation_rate_limit),
        error.InvalidRateLimitPolicy => status(.invalid_argument, .invalid_rate_limit_policy),
        error.ConflictingRateLimitPolicy => status(.conflict, .conflicting_rate_limit_policy),
        error.ProviderTokenBudgetExceeded => status(.invalid_argument, .provider_token_budget_exceeded),
        error.ProviderQuotaRegistryFull => status(.retryable, .provider_quota_registry_full),
        error.UnsupportedMediaTokenBudget => status(.unsupported, .unsupported_media_token_budget),
        error.UnsupportedLocalRateLimit => status(.unsupported, .unsupported_local_rate_limit),
        error.EnrichmentNotFound => status(.not_found, .enrichment_not_found),
        error.InvalidExtensionEnrichment => status(.invalid_argument, .invalid_extension_enrichment),
        error.ConflictingEnrichmentConfig => status(.invalid_argument, .conflicting_enrichment_config),
        error.InvalidCreateIndexRequest => status(.invalid_argument, .invalid_create_index_request),
        error.UnsupportedBackupMigrationState => status(.invalid_argument, .unsupported_backup_migration_state),
        error.UnsupportedBackupFormat => status(.unsupported, .unsupported_backup_format),
        error.ExtensionAlreadyInstalled => status(.already_exists, .extension_already_installed),
        error.ExtensionNotInstalled => status(.not_found, .extension_not_installed),
        error.ExtensionDisabled => status(.conflict, .extension_disabled),
        error.ExtensionLifecycleBusy => status(.retryable, .extension_lifecycle_busy),
        error.ExtensionVisibilityTimeout => status(.timeout, .extension_visibility_timeout),
        error.DependentExtensionExists => status(.conflict, .dependent_extension_exists),
        error.RequiredExtensionNotInstalled => status(.conflict, .required_extension_not_installed),
        error.MissingExtensionPackage => status(.not_found, .missing_extension_package),
        error.PackageNotFound => status(.not_found, .package_not_found),
        error.PackageVersionMismatch => status(.conflict, .package_version_mismatch),
        error.UnsupportedPackageKind => status(.unsupported, .unsupported_package_kind),
        error.InvalidExtensionShape => status(.invalid_argument, .invalid_extension_shape),
        error.UnsupportedExtensionScope => status(.unsupported, .unsupported_extension_scope),
        error.InvalidExtensionLifecycleRequest => status(.invalid_argument, .invalid_extension_lifecycle_request),
        error.RestoreJobFenced => status(.conflict, .restore_job_fenced),
        error.RestoreJobCapacityExceeded => status(.unavailable, .restore_job_capacity_exceeded),
        error.RestoreJobRecordTooLarge => status(.invalid_argument, .restore_job_record_too_large),
        error.TooManyRestoreTables => status(.invalid_argument, .too_many_restore_tables),
        error.DuplicateRestoreTableName => status(.invalid_argument, .duplicate_restore_table_name),
        error.IdempotencyConflict => status(.conflict, .idempotency_conflict),
        error.InvalidIdempotencyKey => status(.invalid_argument, .invalid_idempotency_key),
        error.AsyncRestoreUnavailable => status(.unavailable, .async_restore_unavailable),
        error.BackgroundRuntimeUnavailable => status(.unavailable, .background_runtime_unavailable),
        error.BackupStagingUnavailable => status(.unavailable, .backup_staging_unavailable),
        error.BackupArtifactMissing => status(.not_found, .backup_artifact_missing),
        error.BackupIntegrityMissing => status(.corrupt, .backup_integrity_missing),
        error.BackupOutcomeAmbiguous => status(.conflict, .backup_outcome_ambiguous),
        error.MetadataMutationNotApplied => status(.retryable, .metadata_mutation_not_applied),
        error.BackupAttemptLeaseLost => status(.conflict, .backup_attempt_lease_lost),
        error.BackgroundOwnerClosed => status(.unavailable, .background_owner_closed),
        error.RestoreSchedulingCapacity => status(.retryable, .restore_scheduling_capacity),
        error.RestoreWaitInterrupted => status(.cancelled, .restore_wait_interrupted),
        error.ApiKeyExpired => status(.unauthorized, .api_key_expired),
        error.ApiKeyInvalid => status(.unauthorized, .api_key_invalid),
        error.ApiKeyNotFound => status(.not_found, .api_key_not_found),
        error.AuthRowFilterNoMatch => status(.forbidden, .auth_row_filter_no_match),
        error.BackupMaintenanceQueueFull => status(.retryable, .backup_maintenance_queue_full),
        error.QueueFull => status(.retryable, .queue_full),
        error.ResourceLimitExceeded => status(.unavailable, .resource_limit_exceeded),
        error.BodyTooLarge => status(.invalid_argument, .body_too_large),
        error.BodyCapacityExceeded => status(.retryable, .body_capacity_exceeded),
        error.BodyReadFailed => status(.unavailable, .body_read_failed),
        error.InferenceProviderFailure => status(.internal, .inference_provider_failure),
        error.InferenceWorkerExecutableNotConfigured => status(.invalid_argument, .inference_worker_executable_not_configured),
        error.InferenceWorkerUnavailable => status(.retryable, .inference_worker_unavailable),
        error.KernelJitRequiredDynamicLoad => status(.unsupported, .kernel_jit_required_dynamic_load),
        error.UnexpectedToken => status(.internal, .unexpected_token),
        error.EnrichmentWaitCanceled => status(.cancelled, .enrichment_wait_canceled),
        error.EnrichmentWaitTimeout => status(.timeout, .enrichment_wait_timeout),
        // This can be reported only after a write's primary mutation is
        // durable. Preserve it across the API/storage partition so ingress
        // can return committed_repair_required instead of an unsafe 500.
        error.EnrichmentWorkerFailed => status(.internal, .enrichment_worker_failed),
        error.BackupMaintenanceUnavailable => status(.unavailable, .backup_maintenance_unavailable),
        error.ConnectionNotFound => status(.not_found, .connection_not_found),
        error.EmbedRequestFailed => status(.unavailable, .embed_request_failed),
        error.EmptyEmbeddingResponse => status(.unavailable, .empty_embedding_response),
        error.EmptyName => status(.invalid_argument, .empty_name),
        error.EndOfStream => status(.retryable, .end_of_stream),
        error.ForeignQueryFailed => status(.unavailable, .foreign_query_failed),
        error.InvalidApiKeyRequest => status(.invalid_argument, .invalid_api_key_request),
        error.InvalidApiServerConfig => status(.invalid_argument, .invalid_api_server_config),
        error.InvalidArdAgentsRequest => status(.invalid_argument, .invalid_ard_agents_request),
        error.InvalidArdSearchRequest => status(.invalid_argument, .invalid_ard_search_request),
        error.InvalidBackupId => status(.invalid_argument, .invalid_backup_id),
        error.InvalidBackupListLimit => status(.invalid_argument, .invalid_backup_list_limit),
        error.InvalidCharacter => status(.invalid_argument, .invalid_character),
        error.InvalidCreateUserRequest => status(.invalid_argument, .invalid_create_user_request),
        error.InvalidDetail => status(.invalid_argument, .invalid_detail),
        error.InvalidDuration => status(.invalid_argument, .invalid_duration),
        error.InvalidEnrichmentRequest => status(.invalid_argument, .invalid_enrichment_request),
        error.InvalidEvalRequest => status(.invalid_argument, .invalid_eval_request),
        error.InvalidIdentifier => status(.invalid_argument, .invalid_identifier),
        error.InvalidIndexRequest => status(.invalid_argument, .invalid_index_request),
        error.InvalidJobId => status(.invalid_argument, .invalid_job_id),
        error.InvalidJsonObject => status(.invalid_argument, .invalid_json_object),
        error.InvalidLinearMergeRequest => status(.invalid_argument, .invalid_linear_merge_request),
        error.InvalidMetadata => status(.invalid_argument, .invalid_metadata),
        error.InvalidPassword => status(.invalid_argument, .invalid_password),
        error.InvalidPasswordUpdateRequest => status(.invalid_argument, .invalid_password_update_request),
        error.InvalidPathParameter => status(.invalid_argument, .invalid_path_parameter),
        error.InvalidPermissionType => status(.invalid_argument, .invalid_permission_type),
        error.InvalidQueryBuilderRequest => status(.invalid_argument, .invalid_query_builder_request),
        error.InvalidReadConsistency => status(.invalid_argument, .invalid_read_consistency),
        error.InvalidResourceType => status(.invalid_argument, .invalid_resource_type),
        error.InvalidRestoreJobListCursor => status(.invalid_argument, .invalid_restore_job_list_cursor),
        error.InvalidRestoreJobListLimit => status(.invalid_argument, .invalid_restore_job_list_limit),
        error.InvalidRestoreJobListPhase => status(.invalid_argument, .invalid_restore_job_list_phase),
        error.InvalidRestoreJobListScope => status(.invalid_argument, .invalid_restore_job_list_scope),
        error.InvalidRetrievalAgentRequest => status(.invalid_argument, .invalid_retrieval_agent_request),
        error.InvalidRole => status(.invalid_argument, .invalid_role),
        error.InvalidSecretKey => status(.invalid_argument, .invalid_secret_key),
        error.InvalidTransactionCommitRequest => status(.invalid_argument, .invalid_transaction_commit_request),
        error.InvalidTransactionSessionRecord => status(.invalid_argument, .invalid_transaction_session_record),
        error.MaintenanceBusy => status(.retryable, .maintenance_busy),
        error.MaintenanceHistoryFull => status(.unavailable, .maintenance_history_full),
        error.MaintenanceJobIdExhausted => status(.unavailable, .maintenance_job_id_exhausted),
        error.MemberTableOutsideScope => status(.invalid_argument, .member_table_outside_scope),
        error.MissingField => status(.invalid_argument, .missing_field),
        error.MissingLocation => status(.invalid_argument, .missing_location),
        error.MissingResource => status(.not_found, .missing_resource),
        error.MissingResourceType => status(.invalid_argument, .missing_resource_type),
        error.MissingRole => status(.invalid_argument, .missing_role),
        error.NoTables => status(.invalid_argument, .no_tables),
        error.PathAlreadyExists => status(.already_exists, .path_already_exists),
        error.PrivilegeEscalation => status(.forbidden, .privilege_escalation),
        error.ProbeUnavailable => status(.unavailable, .probe_unavailable),
        error.RoleNotFound => status(.not_found, .role_not_found),
        error.RowFilterNotFound => status(.not_found, .row_filter_not_found),
        error.SavepointLimitExceeded => status(.unavailable, .savepoint_limit_exceeded),
        error.SessionLeaseLost => status(.conflict, .session_lease_lost),
        error.SessionLimitExceeded => status(.unavailable, .session_limit_exceeded),
        error.SessionRecordTooLarge => status(.invalid_argument, .session_record_too_large),
        error.StaleIndexRepairControl => status(.conflict, .stale_index_repair_control),
        error.SyntaxError => status(.invalid_argument, .syntax_error),
        error.TooManyRuntimeStatuses => status(.unavailable, .too_many_runtime_statuses),
        error.TransactionCommitRequestMismatch => status(.conflict, .transaction_commit_request_mismatch),
        error.TransactionCommitSealed => status(.conflict, .transaction_commit_sealed),
        error.TreeRootSetTooLarge => status(.invalid_argument, .tree_root_set_too_large),
        error.UnrequestedCapabilityGrant => status(.forbidden, .unrequested_capability_grant),
        error.UnsupportedAggregate => status(.unsupported, .unsupported_aggregate),
        error.UnsupportedBackupLocation => status(.unsupported, .unsupported_backup_location),
        error.UnsupportedManifestApiVersion => status(.unsupported, .unsupported_manifest_api_version),
        error.UnsupportedObjectKindForV1 => status(.unsupported, .unsupported_object_kind_for_v1),
        error.UnsupportedRetrievalAgentRequest => status(.unsupported, .unsupported_retrieval_agent_request),
        error.UnsupportedSourceKind => status(.unsupported, .unsupported_source_kind),
        error.UpdatePathNotFound => status(.not_found, .update_path_not_found),
        error.UserExists => status(.already_exists, .user_exists),
        error.InvalidTableStorageSettings => status(.invalid_argument, .invalid_table_storage_settings),
        error.VectorStoreRequiresLocalSingleShardTable => status(.invalid_argument, .vector_store_requires_local_single_shard_table),
        error.VectorStoreRequiresEmptyTable => status(.conflict, .vector_store_requires_empty_table),
        error.ImmutableTableStorageSettings => status(.conflict, .immutable_table_storage_settings),
        error.VectorStoreLifecycleUnsupported => status(.unsupported, .vector_store_lifecycle_unsupported),
        error.VectorStoreReferenceFormatRequired => status(.unsupported, .vector_store_reference_format_required),
        error.InvalidVectorMigrationBudget => status(.invalid_argument, .invalid_vector_migration_budget),
        error.InvalidVectorMigrationId => status(.invalid_argument, .invalid_vector_migration_id),
        error.InvalidVectorMigrationState => status(.corrupt, .invalid_vector_migration_state),
        error.UnsupportedVectorMigrationDirection => status(.unsupported, .unsupported_vector_migration_direction),
        error.UnsupportedVectorMigrationVersion => status(.unsupported, .unsupported_vector_migration_version),
        error.VectorMigrationActive => status(.conflict, .vector_migration_active),
        error.VectorMigrationAlreadyExists => status(.conflict, .vector_migration_already_exists),
        error.VectorMigrationAlreadyPublished => status(.conflict, .vector_migration_already_published),
        error.VectorMigrationConfigurationChanged => status(.conflict, .vector_migration_configuration_changed),
        error.VectorMigrationCoverageMismatch => status(.corrupt, .vector_migration_coverage_mismatch),
        error.VectorMigrationDiskReserve => status(.retryable, .vector_migration_disk_reserve),
        error.VectorMigrationIdempotencyConflict => status(.conflict, .vector_migration_idempotency_conflict),
        error.VectorMigrationIdentityMismatch => status(.corrupt, .vector_migration_identity_mismatch),
        error.VectorMigrationInlinePayloadRemains => status(.corrupt, .vector_migration_inline_payload_remains),
        error.VectorMigrationNotFound => status(.not_found, .vector_migration_not_found),
        error.VectorMigrationNotReady => status(.retryable, .vector_migration_not_ready),
        error.VectorMigrationReadEpochChanged => status(.retryable, .vector_migration_read_epoch_changed),
        error.VectorMigrationRecoveryRequired => status(.retryable, .vector_migration_recovery_required),
        error.VectorMigrationRowExceedsBudget => status(.invalid_argument, .vector_migration_row_exceeds_budget),
        error.VectorMigrationTemporaryBudgetExceeded => status(.retryable, .vector_migration_temporary_budget_exceeded),
        error.VectorMigrationOfflineAdmission => status(.conflict, .vector_migration_offline_admission),
        error.VectorMigrationCatalogInUse => status(.conflict, .vector_migration_catalog_in_use),
        error.VectorMigrationCopyMismatch => status(.conflict, .vector_migration_copy_mismatch),
        error.VectorMigrationUnsupportedFile => status(.conflict, .vector_migration_unsupported_file),
        error.VectorStoreRequiresOfflineCommand => status(.conflict, .vector_store_requires_offline_command),
        error.StorageKernelFailure => status(.internal, .storage_kernel_failure),
        error.WarmInferenceServerUnavailable => status(.unavailable, .warm_inference_server_unavailable),
        error.UnsupportedServerGenerateOption => status(.unsupported, .unsupported_server_generate_option),
        error.MissingInferenceListener => status(.internal, .missing_inference_listener),
        error.RuntimeFailed => status(.internal, .runtime_failed),
        error.RuntimeStopping => status(.cancelled, .runtime_stopping),
        error.RuntimeAlreadyReady => status(.conflict, .runtime_already_ready),
        error.DocumentExtractionWorkingSetTooLarge => status(.invalid_argument, .document_extraction_working_set_too_large),
        error.DocumentArtifactPublicationFailed => status(.conflict, .document_artifact_publication_failed),
        error.HierarchyCursorStale => status(.conflict, .hierarchy_cursor_stale),
        error.CrcMismatch => status(.corrupt, .crc_mismatch),
        error.InvalidData => status(.corrupt, .invalid_data),
        error.MappedArtifactUnsupported => status(.unsupported, .mapped_artifact_unsupported),
        error.ImmutableReadSourceUnsupported => status(.unsupported, .immutable_read_source_unsupported),
        error.DirectoryListingUnsupported => status(.unsupported, .directory_listing_unsupported),
        error.UnsupportedStorageLease => status(.unsupported, .unsupported_storage_lease),
        error.DurableAtomicWriteUnsupported => status(.unsupported, .durable_atomic_write_unsupported),
        error.DurableAtomicRenameUnsupported => status(.unsupported, .durable_atomic_rename_unsupported),
        error.DurableFileSyncUnsupported => status(.unsupported, .durable_file_sync_unsupported),
        error.DurableDirectorySyncUnsupported => status(.unsupported, .durable_directory_sync_unsupported),
        error.StableFileLeaseUnsupported => status(.unsupported, .stable_file_lease_unsupported),
        error.ReadOnly => status(.forbidden, .read_only),
        error.StorageClosed => status(.unavailable, .storage_closed),
        error.CacheBudgetExceeded => status(.unavailable, .cache_budget_exceeded),
        error.BufferTooSmall => status(.invalid_argument, .buffer_too_small),
        error.FileTooBig => status(.invalid_argument, .file_too_big),
        error.InvalidScratch => status(.invalid_argument, .invalid_scratch),
        error.InvalidAtomicWriteOffset => status(.invalid_argument, .invalid_atomic_write_offset),
        error.CanceledRangeRead => status(.cancelled, .canceled_range_read),
        error.MappedArtifactUnavailable => status(.unsupported, .mapped_artifact_unavailable),
        error.ReadScopeResetUnsupported => status(.unsupported, .read_scope_reset_unsupported),
        error.ReadSnapshotForkUnsupported => status(.unsupported, .read_snapshot_fork_unsupported),
        error.SourceVisitorFailed => status(.internal, .source_visitor_failed),
        // A local provider's admission refusal (the request never ran)
        // distinct from a real provider transient failure. Keep this
        // retryable like EmbedTransientFailure so callers that only check
        // the Code still back off, while callers that need the distinct
        // identity (the enrichment retry-budget exemption) can tell them
        // apart by name.
        error.EmbedAdmissionRefused => status(.retryable, .embed_admission_refused),
        error.ReadOutputTruncated => status(.invalid_argument, .read_output_truncated),
        else => blk: {
            // Private error identities cannot cross this stable ABI. Retain
            // the owning side's name in diagnostics instead of leaving an
            // otherwise opaque RuntimeBoundaryFailure impossible to trace.
            if (!@inComptime()) std.log.debug("runtime boundary unclassified error={s}", .{@errorName(err)});
            break :blk status(.internal, .none);
        },
    };
}

/// Preserve an explicitly classified error and replace an untransportable,
/// compilation-local error with a stable boundary-specific outcome.
///
/// Callers should log `err` on the owning side before using this helper: the
/// fallback intentionally carries behavior and a diagnosable boundary name,
/// not an arbitrary error string across the stable ABI.
pub fn statusFromErrorWithFallback(err: anyerror, fallback: anyerror) Status {
    const value = statusFromError(err);
    if (value.detail != @backingInt(Detail.none)) return value;
    const fallback_value = statusFromError(fallback);
    std.debug.assert(fallback_value.detail != @backingInt(Detail.none));
    return fallback_value;
}

pub fn errorHasStableDetail(err: anyerror) bool {
    return statusFromError(err).detail != @backingInt(Detail.none);
}

fn status(code: Code, detail: Detail) Status {
    return .{ .code = @backingInt(code), .detail = @backingInt(detail) };
}

pub fn errorFromStatus(value: Status) anyerror {
    @setEvalBranchQuota(30_000);
    const code = std.enums.fromInt(Code, value.code) orelse return error.RuntimeBoundaryFailure;
    if (code == .ok) return error.RuntimeBoundaryFailure;
    const detail = std.enums.fromInt(Detail, value.detail) orelse return error.RuntimeBoundaryFailure;
    const err: anyerror = switch (detail) {
        .none => error.RuntimeBoundaryFailure,
        inline else => |known_detail| @field(anyerror, detailErrorName(known_detail)),
    };
    if (err == error.RuntimeBoundaryFailure) return err;
    const canonical = statusFromError(err);
    if (canonical.code != value.code or canonical.detail != value.detail)
        return error.RuntimeBoundaryFailure;
    return err;
}

fn detailErrorName(comptime detail: Detail) []const u8 {
    return switch (detail) {
        .relational_check_violation => "RelationalCheckViolation",
        .invalid_document_source => "InvalidDocumentSource",
        .unsupported_document_format => "UnsupportedDocumentFormat",
        .document_source_limit_exceeded => "DocumentSourceLimitExceeded",
        .document_page_limit_exceeded => "DocumentPageLimitExceeded",
        .document_pixel_limit_exceeded => "DocumentPixelLimitExceeded",
        .document_token_limit_exceeded => "DocumentTokenLimitExceeded",
        .document_media_type_mismatch => "DocumentMediaTypeMismatch",
        .invalid_document_selection => "InvalidDocumentSelection",
        .duplicate_document_source => "DuplicateDocumentSource",
        .document_unresolved => "DocumentUnresolved",
        .document_capability_mismatch => "DocumentCapabilityMismatch",
        .unsupported_document_request_field => "UnsupportedDocumentRequestField",
        .remote_document_model_denied => "RemoteDocumentModelDenied",
        .cloudflare_credential_missing => "CloudflareCredentialMissing",
        .invalid_cloudflare_config => "InvalidCloudflareConfig",
        .unsupported_cloudflare_model => "UnsupportedCloudflareModel",
        .unsupported_cloudflare_schema => "UnsupportedCloudflareSchema",
        .pioneer_credential_missing => "PioneerCredentialMissing",
        .unsupported_pioneer_schema => "UnsupportedPioneerSchema",
        .unsupported_pioneer_input => "UnsupportedPioneerInput",
        .standalone_replication_sources_unsupported => "StandaloneReplicationSourcesUnsupported",
        .ha_catalog_replication_sources_unsupported => "HACatalogReplicationSourcesUnsupported",
        .cloudflare_authentication_failed => "CloudflareAuthenticationFailed",
        .invalid_extractor_response => "InvalidExtractorResponse",
        .unsupported_extraction_schema_version => "UnsupportedExtractionSchemaVersion",
        .invalid_inference_request => "InvalidInferenceRequest",
        .document_artifact_not_found => "DocumentArtifactNotFound",
        .document_artifact_expired => "DocumentArtifactExpired",
        .cloudflare_rate_limited => "CloudflareRateLimited",
        .inference_upstream_rate_limited => "InferenceUpstreamRateLimited",
        .inference_capacity_unavailable => "InferenceCapacityUnavailable",
        .inference_transport_unavailable => "InferenceTransportUnavailable",
        .cloudflare_inference_failed => "CloudflareInferenceFailed",
        .invalid_cloudflare_response => "InvalidCloudflareResponse",

        .too_many_active_jobs => "TooManyActiveJobs",
        .document_storage_not_configured => "DocumentStorageNotConfigured",
        .document_run_busy => "DocumentRunBusy",
        .document_run_exists => "DocumentRunExists",
        .document_run_expired => "DocumentRunExpired",
        .document_run_lease_lost => "DocumentRunLeaseLost",
        .document_run_not_found => "DocumentRunNotFound",
        .document_run_retention_active => "DocumentRunRetentionActive",
        .document_run_terminal => "DocumentRunTerminal",
        .document_run_store_needs_recovery => "DocumentRunStoreNeedsRecovery",
        .document_model_revision_mismatch => "DocumentModelRevisionMismatch",
        .document_budget_exhausted => "DocumentBudgetExhausted",
        .document_output_too_large => "DocumentOutputTooLarge",
        .document_stage_in_flight => "DocumentStageInFlight",
        .invalid_document_checkpoint => "InvalidDocumentCheckpoint",

        .resolver_curation_conflict => "ResolverCurationConflict",
        .grouped_source_owner_changed => "GroupedSourceOwnerChanged",
        .grouped_source_topology_unavailable => "GroupedSourceTopologyUnavailable",
        .apple_intelligence_provider_unavailable => "AppleIntelligenceProviderUnavailable",
        .artifact_publication_pending => "ArtifactPublicationPending",
        .chatgpt_billing_fallback_forbidden => "ChatGPTBillingFallbackForbidden",
        .chatgpt_disabled => "ChatGPTDisabled",
        .chatgpt_interactive_only => "ChatGPTInteractiveOnly",
        .constrained_input_too_large => "ConstrainedInputTooLarge",
        .decide_request_limit_exceeded => "DecideRequestLimitExceeded",
        .enrichment_source_changed => "EnrichmentSourceChanged",
        .generated_extraction_limit_exceeded => "GeneratedExtractionLimitExceeded",
        .grouped_prior_context_limit_exceeded => "GroupedPriorContextLimitExceeded",
        .grouped_source_baseline_pending => "GroupedSourceBaselinePending",
        .grouped_source_busy => "GroupedSourceBusy",
        .grouped_source_corrupt => "GroupedSourceCorrupt",
        .grouped_source_earlier_work_pending => "GroupedSourceEarlierWorkPending",
        .grouped_source_identity_conflict => "GroupedSourceIdentityConflict",
        .grouped_source_limit_exceeded => "GroupedSourceLimitExceeded",
        .grouped_source_member_limit_exceeded => "GroupedSourceMemberLimitExceeded",
        .grouped_source_member_too_large => "GroupedSourceMemberTooLarge",
        .grouped_source_partition_too_large => "GroupedSourcePartitionTooLarge",
        .grouped_source_retirement_pending => "GroupedSourceRetirementPending",
        .invalid_constrained_generation_config => "InvalidConstrainedGenerationConfig",
        .invalid_constrained_output => "InvalidConstrainedOutput",
        .invalid_constrained_probabilities => "InvalidConstrainedProbabilities",
        .invalid_constrained_schema => "InvalidConstrainedSchema",
        .invalid_decide_request => "InvalidDecideRequest",
        .invalid_decision_output => "InvalidDecisionOutput",
        .invalid_generated_extractor_config => "InvalidGeneratedExtractorConfig",
        .invalid_generator_config => "InvalidGeneratorConfig",
        .invalid_generator_tool_config => "InvalidGeneratorToolConfig",
        .invalid_grouped_prior_context => "InvalidGroupedPriorContext",
        .invalid_grouped_source_command => "InvalidGroupedSourceCommand",
        .invalid_grouped_source_config => "InvalidGroupedSourceConfig",
        .invalid_grouped_source_member => "InvalidGroupedSourceMember",
        .invalid_grouped_source_mutation => "InvalidGroupedSourceMutation",
        .invalid_grouped_source_query => "InvalidGroupedSourceQuery",
        .invalid_grouped_source_transfer => "InvalidGroupedSourceTransfer",
        .invalid_structured_output => "InvalidStructuredOutput",
        .missing_generator_credentials => "MissingGeneratorCredentials",
        .unsupported_apple_generation_options => "UnsupportedAppleGenerationOptions",
        .unsupported_constrained_output => "UnsupportedConstrainedOutput",
        .unsupported_constrained_probabilities => "UnsupportedConstrainedProbabilities",
        .unsupported_constrained_schema => "UnsupportedConstrainedSchema",
        .unsupported_generated_extraction_schema => "UnsupportedGeneratedExtractionSchema",
        .model_chunk_audio_too_large => "AudioTooLarge",
        .model_chunk_chunk_output_too_large => "ChunkOutputTooLarge",
        .model_chunk_chunk_request_failed => "ChunkRequestFailed",
        .model_chunk_chunk_unit_too_large => "ChunkUnitTooLarge",
        .model_chunk_embedding_identity_mismatch => "EmbeddingIdentityMismatch",
        .invalid_embedding_identity => "InvalidEmbeddingIdentity",
        .model_chunk_empty_embedding_input => "EmptyEmbeddingInput",
        .model_chunk_empty_response => "EmptyResponse",
        .model_chunk_empty_source => "EmptySource",
        .model_chunk_empty_video => "EmptyVideo",
        .model_chunk_immutable_version_required => "ImmutableVersionRequired",
        .model_chunk_incompatible_chunk_embedding_target => "IncompatibleChunkEmbeddingTarget",
        .model_chunk_incomplete_chunk_coverage => "IncompleteChunkCoverage",
        .model_chunk_incomplete_video_picture => "IncompleteVideoPicture",
        .model_chunk_inference_admission_queue_full => "InferenceAdmissionQueueFull",
        .model_chunk_inference_capabilities_stale => "InferenceCapabilitiesStale",
        .model_chunk_invalid_audio_window => "InvalidAudioWindow",
        .model_chunk_invalid_chunk_continuation => "InvalidChunkContinuation",
        .model_chunk_invalid_chunk_coordinates => "InvalidChunkCoordinates",
        .model_chunk_invalid_chunk_geometry => "InvalidChunkGeometry",
        .model_chunk_invalid_chunk_model => "InvalidChunkModel",
        .model_chunk_invalid_chunk_policy => "InvalidChunkPolicy",
        .model_chunk_invalid_chunk_provider => "InvalidChunkProvider",
        .model_chunk_invalid_chunk_replay => "InvalidChunkReplay",
        .model_chunk_invalid_chunk_source => "InvalidChunkSource",
        .model_chunk_invalid_chunker_config => "InvalidChunkerConfig",
        .model_chunk_invalid_chunker_response => "InvalidChunkerResponse",
        .model_chunk_invalid_embedding_output => "InvalidEmbeddingOutput",
        .model_chunk_invalid_input_shape => "InvalidInputShape",
        .model_chunk_invalid_sampling_policy => "InvalidSamplingPolicy",
        .model_chunk_invalid_source_reference => "InvalidSourceReference",
        .model_chunk_invalid_utf8 => "InvalidUtf8",
        .model_chunk_memory_budget_exceeded => "MemoryBudgetExceeded",
        .model_chunk_missing_attachment_envelope => "MissingAttachmentEnvelope",
        .model_chunk_missing_chunk_stream_context => "MissingChunkStreamContext",
        .model_chunk_model_aware_chunk_provider_required => "ModelAwareChunkProviderRequired",
        .model_chunk_shared_admission_exceeded => "SharedAdmissionExceeded",
        .model_chunk_source_version_changed => "SourceVersionChanged",
        .model_chunk_stale_chunk_continuation => "StaleChunkContinuation",
        .model_chunk_stream_too_long => "StreamTooLong",
        .model_chunk_timestamp_overflow => "TimestampOverflow",
        .model_chunk_unknown_video_duration => "UnknownVideoDuration",
        .model_chunk_unresolved_chunk_source => "UnresolvedChunkSource",
        .model_chunk_unsupported_audio_format => "UnsupportedAudioFormat",
        .model_chunk_unsupported_chunk_media_type => "UnsupportedChunkMediaType",
        .model_chunk_working_budget => "ChunkWorkingBudgetExceeded",
        .model_chunk_dependency_budget => "ChunkDependencyBudgetExceeded",
        .model_chunk_coded_window_budget => "ChunkCodedWindowBudgetExceeded",
        .model_chunk_event_budget => "ChunkEventBudgetExceeded",
        .model_chunk_remote_bytes_budget => "RemoteReadBudgetExceeded",
        .model_chunk_remote_requests_budget => "RemoteRequestBudgetExceeded",
        .model_chunk_invalid_limits => "InvalidChunkLimits",
        .model_chunk_streaming_profile => "UnsupportedStreamingAudioProfile",
        .model_chunk_incomplete_audio_stream => "IncompleteAudioStream",

        .document_extraction_working_set_too_large => "DocumentExtractionWorkingSetTooLarge",
        .document_artifact_publication_failed => "DocumentArtifactPublicationFailed",
        .index_alias_precondition_required => "IndexAliasPreconditionRequired",
        .index_alias_precondition_failed => "IndexAliasPreconditionFailed",
        .index_referenced_by_alias => "IndexReferencedByAlias",
        .index_alias_graph_conflict => "IndexAliasGraphConflict",
        .hierarchy_cursor_stale => "HierarchyCursorStale",
        .read_transient_failure => "ReadTransientFailure",
        .read_request_failed => "ReadRequestFailed",
        .invalid_gcs_url => "InvalidGcsUrl",
        .gcs_connection_not_found_or_out_of_scope => "GcsConnectionNotFoundOrOutOfScope",
        .ambiguous_gcs_connection => "AmbiguousGcsConnection",
        .invalid_connection_credentials => "InvalidConnectionCredentials",

        .schema_version_changed => "SchemaVersionChanged",
        .cypher_write_request_already_used => "CypherWriteRequestAlreadyUsed",
        .invalid_cypher_parameters => "InvalidCypherParameters",
        .missing_cypher_parameter => "MissingCypherParameter",
        .sql_write_capacity_unavailable => "SqlWriteCapacityUnavailable",
        .cypher_aggregate_scope => "CypherAggregateScope",
        .cypher_aggregate_wildcard => "CypherAggregateWildcard",
        .cypher_ambiguous_search_table => "CypherAmbiguousSearchTable",
        .row_policy_unsupported => "RowPolicyUnsupported",
        .row_policy_mutation_unsupported => "RowPolicyMutationUnsupported",
        .cypher_invalid_regex => "CypherInvalidRegex",
        .row_policy_denied => "RowPolicyDenied",
        .row_policy_authentication_required => "RowPolicyAuthenticationRequired",
        .row_policy_authority_unavailable => "RowPolicyAuthorityUnavailable",
        .transaction_prepare_aborted_row_policy_denied => "TransactionPrepareAbortedRowPolicyDenied",
        .retained_read_restart_required => "RetainedReadRestartRequired",
        .cypher_anchor_budget_exceeded => "CypherAnchorBudgetExceeded",
        .cypher_create_requires_label => "CypherCreateRequiresLabel",
        .cypher_deleted_entity => "CypherDeletedEntity",
        .cypher_derived_relationship_mutation => "CypherDerivedRelationshipMutation",
        .cypher_division_by_zero => "CypherDivisionByZero",
        .cypher_duplicate_identity => "CypherDuplicateIdentity",
        .cypher_edge_budget_exceeded => "CypherEdgeBudgetExceeded",
        .cypher_exists_mutation => "CypherExistsMutation",
        .cypher_function_arity => "CypherFunctionArity",
        .cypher_invalid_assignment => "CypherInvalidAssignment",
        .cypher_invalid_backend_cursor => "CypherInvalidBackendCursor",
        .cypher_invalid_backend_page => "CypherInvalidBackendPage",
        .cypher_invalid_backend_relationship => "CypherInvalidBackendRelationship",
        .cypher_invalid_create_relationship => "CypherInvalidCreateRelationship",
        .cypher_invalid_document => "CypherInvalidDocument",
        .cypher_invalid_grouping => "CypherInvalidGrouping",
        .cypher_invalid_owner => "CypherInvalidOwner",
        .cypher_invalid_property_value => "CypherInvalidPropertyValue",
        .cypher_invalid_range => "CypherInvalidRange",
        .cypher_invalid_relationship => "CypherInvalidRelationship",
        .cypher_invalid_relationship_metadata => "CypherInvalidRelationshipMetadata",
        .cypher_invalid_search_limit => "CypherInvalidSearchLimit",
        .cypher_invalid_search_result => "CypherInvalidSearchResult",
        .cypher_invalid_shortest_path => "CypherInvalidShortestPath",
        .cypher_invalid_substring => "CypherInvalidSubstring",
        .cypher_label_predicate_mismatch => "CypherLabelPredicateMismatch",
        .cypher_label_predicate_violation => "CypherLabelPredicateViolation",
        .cypher_legacy_id_unavailable => "CypherLegacyIdUnavailable",
        .cypher_limit_exceeded => "CypherLimitExceeded",
        .cypher_memory_budget_exceeded => "CypherMemoryBudgetExceeded",
        .cypher_mutations_unavailable => "CypherMutationsUnavailable",
        .cypher_node_has_relationships => "CypherNodeHasRelationships",
        .cypher_numeric_overflow => "CypherNumericOverflow",
        .cypher_observations_unavailable => "CypherObservationsUnavailable",
        .cypher_overlapping_owners => "CypherOverlappingOwners",
        .cypher_owner_budget_exceeded => "CypherOwnerBudgetExceeded",
        .cypher_path_budget_exceeded => "CypherPathBudgetExceeded",
        .cypher_query_requires_return => "CypherQueryRequiresReturn",
        .cypher_recursion_budget_exceeded => "CypherRecursionBudgetExceeded",
        .cypher_regex_unavailable => "CypherRegexUnavailable",
        .cypher_relationship_endpoint_mismatch => "CypherRelationshipEndpointMismatch",
        .cypher_remove_requires_property => "CypherRemoveRequiresProperty",
        .cypher_request_too_large => "CypherRequestTooLarge",
        .cypher_response_too_large => "CypherResponseTooLarge",
        .cypher_return_must_be_last => "CypherReturnMustBeLast",
        .cypher_return_predicate => "CypherReturnPredicate",
        .cypher_row_policy_unsupported => "CypherRowPolicyUnsupported",
        .cypher_search_after_write => "CypherSearchAfterWrite",
        .cypher_search_preparation_required => "CypherSearchPreparationRequired",
        .cypher_search_table_outside_graph => "CypherSearchTableOutsideGraph",
        .cypher_search_unavailable => "CypherSearchUnavailable",
        .cypher_snapshot_not_ready => "CypherSnapshotNotReady",
        .cypher_snapshot_required => "CypherSnapshotRequired",
        .cypher_state_budget_exceeded => "CypherStateBudgetExceeded",
        .cypher_transaction_already_active => "CypherTransactionAlreadyActive",
        .cypher_transaction_conflict => "CypherTransactionConflict",
        .cypher_transaction_required => "CypherTransactionRequired",
        .cypher_type_mismatch => "CypherTypeMismatch",
        .cypher_union_column_mismatch => "CypherUnionColumnMismatch",
        .cypher_value_depth_exceeded => "CypherValueDepthExceeded",
        .cypher_variable_conflict => "CypherVariableConflict",
        .cypher_variable_type_conflict => "CypherVariableTypeConflict",
        .cypher_write_union_unsupported => "CypherWriteUnionUnsupported",
        .crc_mismatch => "CrcMismatch",
        .invalid_data => "InvalidData",
        .mapped_artifact_unsupported => "MappedArtifactUnsupported",
        .mapped_artifact_unavailable => "MappedArtifactUnavailable",
        .read_scope_reset_unsupported => "ReadScopeResetUnsupported",
        .read_snapshot_fork_unsupported => "ReadSnapshotForkUnsupported",
        .immutable_read_source_unsupported => "ImmutableReadSourceUnsupported",
        .directory_listing_unsupported => "DirectoryListingUnsupported",
        .unsupported_storage_lease => "UnsupportedStorageLease",
        .durable_atomic_write_unsupported => "DurableAtomicWriteUnsupported",
        .durable_atomic_rename_unsupported => "DurableAtomicRenameUnsupported",
        .durable_file_sync_unsupported => "DurableFileSyncUnsupported",
        .durable_directory_sync_unsupported => "DurableDirectorySyncUnsupported",
        .stable_file_lease_unsupported => "StableFileLeaseUnsupported",
        .read_only => "ReadOnly",
        .storage_closed => "StorageClosed",
        .cache_budget_exceeded => "CacheBudgetExceeded",
        .buffer_too_small => "BufferTooSmall",
        .file_too_big => "FileTooBig",
        .invalid_scratch => "InvalidScratch",
        .invalid_atomic_write_offset => "InvalidAtomicWriteOffset",
        .canceled_range_read => "CanceledRangeRead",
        .source_visitor_failed => "SourceVisitorFailed",

        .external_lake_read_only => "ExternalLakeReadOnly",
        .materialized_view_read_only => "MaterializedViewReadOnly",
        .external_lake_snapshot_mismatch => "ExternalLakeSnapshotMismatch",
        .external_lake_schema_mismatch => "ExternalLakeSchemaMismatch",
        .external_lake_schema_unavailable => "ExternalLakeSchemaUnavailable",
        .invalid_external_source_inventory => "InvalidExternalSourceInventory",
        .invalid_external_source_snapshot => "InvalidExternalSourceSnapshot",
        .external_source_snapshot_too_large => "ExternalSourceSnapshotTooLarge",
        .external_lake_field_identity_changed => "ExternalLakeFieldIdentityChanged",
        .external_lake_object_changed => "ExternalLakeObjectChanged",
        .empty_external_source_snapshot => "EmptyExternalSourceSnapshot",
        .unsupported_external_lake_schema_type => "UnsupportedExternalLakeSchemaType",
        .invalid_parquet_metadata => "InvalidParquetMetadata",
        .invalid_parquet_footer => "InvalidParquetFooter",
        .invalid_parquet_footer_magic => "InvalidParquetFooterMagic",
        .invalid_parquet_page => "InvalidParquetPage",
        .unsupported_parquet_page => "UnsupportedParquetPage",
        .parquet_page_too_large => "ParquetPageTooLarge",
        .parquet_column_not_found => "ParquetColumnNotFound",
        .unsupported_iceberg_schema_evolution => "UnsupportedIcebergSchemaEvolution",
        .unsupported_iceberg_delete_file => "UnsupportedIcebergDeleteFile",
        .invalid_sql_spill => "InvalidSqlSpill",
        .metric_not_ready => "MetricNotReady",
        .metric_stale => "MetricStale",

        .generation_publication_changed => "GenerationPublicationChanged",
        .initial_child_provision_already_committed => "InitialChildProvisionAlreadyCommitted",
        .initial_child_publication_changed => "InitialChildPublicationChanged",
        .invalid_initial_child_publication => "InvalidInitialChildPublication",
        .initial_fk_retirement_proof_unavailable => "InitialFkRetirementProofUnavailable",
        .invalid_initial_fk_retirement_ticket => "InvalidInitialFkRetirementTicket",
        .invalid_initial_fk_retirement_intent => "InvalidInitialFkRetirementIntent",
        .initial_fk_retirement_path_changed => "InitialFkRetirementPathChanged",
        .artifact_catalog_corrupt => "ArtifactCatalogCorrupt",
        .artifact_catalog_epoch_exhausted => "ArtifactCatalogEpochExhausted",
        .online_merge_artifact_tails_unsupported => "OnlineMergeArtifactTailsUnsupported",
        .setting_authority_unavailable => "SettingAuthorityUnavailable",
        .invalid_generation_publication => "InvalidGenerationPublication",
        .invalid_retirement_summary => "InvalidRetirementSummary",
        .invalid_control_receipt_position => "InvalidControlReceiptPosition",
        .invalid_graph_transfer => "InvalidGraphTransfer",
        .graph_generation_mismatch => "GraphGenerationMismatch",
        .generation_admission_acknowledgement_pending => "GenerationAdmissionAcknowledgementPending",
        .generation_admission_changed => "GenerationAdmissionChanged",
        .generation_admission_pending => "GenerationAdmissionPending",
        .generation_admission_revision_exhausted => "GenerationAdmissionRevisionExhausted",
        .invalid_generation_admission => "InvalidGenerationAdmission",
        .generation_admission_activation_required => "GenerationAdmissionActivationRequired",
        .generation_retirement_acknowledgement_pending => "GenerationRetirementAcknowledgementPending",
        .generation_retirement_changed => "GenerationRetirementChanged",
        .generation_retirement_handoff_required => "GenerationRetirementHandoffRequired",
        .generation_retirement_pending => "GenerationRetirementPending",
        .generation_retirement_revision_exhausted => "GenerationRetirementRevisionExhausted",
        .invalid_generation_retirement => "InvalidGenerationRetirement",
        .initial_child_publication_missing => "InitialChildPublicationMissing",
        .initial_child_not_published => "InitialChildNotPublished",
        .artifact_catalog_drift => "ArtifactCatalogDrift",
        .invalid_artifact_catalog_command => "InvalidArtifactCatalogCommand",
        .artifact_catalog_epoch_changed => "ArtifactCatalogEpochChanged",
        .artifact_catalog_scope_changed => "ArtifactCatalogScopeChanged",
        .online_merge_artifact_catalog_changed => "OnlineMergeArtifactCatalogChanged",
        .store_root_enrollment_changed => "StoreRootEnrollmentChanged",
        .invalid_schema_validation_job => "InvalidSchemaValidationJob",
        .invalid_schema_validation_page_command => "InvalidSchemaValidationPageCommand",
        .invalid_schema_validation_token => "InvalidSchemaValidationToken",
        .schema_validation_canceled => "SchemaValidationCanceled",
        .schema_validation_cancellation_required => "SchemaValidationCancellationRequired",
        .schema_validation_command_rejected => "SchemaValidationCommandRejected",
        .schema_validation_inputs_required => "SchemaValidationInputsRequired",
        .schema_validation_job_changed => "SchemaValidationJobChanged",
        .schema_validation_job_required => "SchemaValidationJobRequired",
        .schema_validation_not_ready => "SchemaValidationNotReady",
        .schema_validation_preparation_changed => "SchemaValidationPreparationChanged",
        .schema_validation_publication_required => "SchemaValidationPublicationRequired",
        .schema_validation_runtime_recovery_required => "SchemaValidationRuntimeRecoveryRequired",
        .store_registration_required => "StoreRegistrationRequired",
        .catalog_publication_fence_unavailable => "CatalogPublicationFenceUnavailable",
        .invalid_store_root_enrollment => "InvalidStoreRootEnrollment",
        .initial_child_root_receipt_changed => "InitialChildRootReceiptChanged",
        .invalid_initial_fk_retirement_signature => "InvalidInitialFkRetirementSignature",
        .initial_fk_retirement_signing_key_unavailable => "InitialFkRetirementSigningKeyUnavailable",
        .initial_fk_retirement_reporter_changed => "InitialFkRetirementReporterChanged",
        .initial_fk_retirement_work_changed => "InitialFkRetirementWorkChanged",
        .initial_fk_retirement_publication_changed => "InitialFkRetirementPublicationChanged",
        .initial_fk_retirement_reservation_changed => "InitialFkRetirementReservationChanged",
        .invalid_initial_fk_retirement_ack => "InvalidInitialFkRetirementAck",
        .invalid_initial_fk_retirement_page => "InvalidInitialFkRetirementPage",
        .initial_fk_retirement_root_changed => "InitialFkRetirementRootChanged",
        .replica_retirement_recovery_in_progress => "ReplicaRetirementRecoveryInProgress",
        .membership_change_fenced => "MembershipChangeFenced",
        .sql_range_tracking_required => "SqlRangeTrackingRequired",
        .sql_statement_snapshot_required => "SqlStatementSnapshotRequired",
        .invalid_range_tracking_state => "InvalidRangeTrackingState",
        .range_tracking_generation_exhausted => "RangeTrackingGenerationExhausted",
        .concurrency_unavailable => "ConcurrencyUnavailable",
        .table_topology_protocol_upgrade_required => "TableTopologyProtocolUpgradeRequired",
        .database_not_found => "DatabaseNotFound",
        .namespace_not_found => "NamespaceNotFound",
        .tablespace_not_found => "TablespaceNotFound",
        .catalog_routing_snapshot_timeout => "CatalogRoutingSnapshotTimeout",
        .metadata_incarnation_unavailable => "MetadataIncarnationUnavailable",
        .invalid_metadata_incarnation => "InvalidMetadataIncarnation",
        .metadata_incarnation_mismatch => "MetadataIncarnationMismatch",
        .catalog_routing_unavailable => "CatalogRoutingUnavailable",
        .catalog_projection_refresh_required => "CatalogProjectionRefreshRequired",
        .catalog_not_found => "CatalogNotFound",
        .catalog_already_exists => "CatalogAlreadyExists",
        .catalog_generation_changed => "CatalogGenerationChanged",
        .tablespace_in_use => "TablespaceInUse",
        .namespace_not_empty => "NamespaceNotEmpty",
        .database_not_empty => "DatabaseNotEmpty",
        .protected_catalog_resource => "ProtectedCatalogResource",
        .invalid_catalog_name => "InvalidCatalogName",
        .invalid_catalog_mutation => "InvalidCatalogMutation",
        .invalid_tablespace_location => "InvalidTablespaceLocation",
        .invalid_tablespace_placement_policy => "InvalidTablespacePlacementPolicy",
        .catalog_command_too_large => "CatalogCommandTooLarge",
        .invalid_catalog_record => "InvalidCatalogRecord",
        .row_policy_catalog_changed => "RowPolicyCatalogChanged",
        .catalog_id_exhausted => "CatalogIdExhausted",

        .restore_job_commit_not_applied => "RestoreJobCommitNotApplied",
        .coordinated_standalone_ha_metadata_required => "CoordinatedStandaloneHAMetadataRequired",
        .relational_rewrite_type_change => "RelationalRewriteTypeChange",
        .relational_rewrite_column_drop => "RelationalRewriteColumnDrop",
        .relational_rewrite_requires_relational => "RelationalRewriteRequiresRelational",
        .relational_rewrite_budget_exceeded => "RelationalRewriteBudgetExceeded",
        .relational_expression_overflow => "RelationalExpressionOverflow",
        .relational_expression_division_by_zero => "RelationalExpressionDivisionByZero",
        .sql_invalid_power_argument => "SqlInvalidPowerArgument",
        .invalid_schema_reader_requirements => "InvalidSchemaReaderRequirements",
        .sql_feature_not_supported => "SqlFeatureNotSupported",
        .catalog_publication_proof_pending => "CatalogPublicationProofPending",
        .sql_index_already_exists => "SqlIndexAlreadyExists",
        .sql_constraint_already_exists => "SqlConstraintAlreadyExists",
        .undefined_column => "UndefinedColumn",
        .foreign_key_validation_required => "ForeignKeyValidationRequired",
        .program_limit_exceeded => "ProgramLimitExceeded",
        .catalog_occupancy_not_ready => "CatalogOccupancyNotReady",
        .relational_expression_budget_exceeded => "RelationalExpressionBudgetExceeded",
        .relational_index_key_too_large => "RelationalIndexKeyTooLarge",
        .invalid_relational_expression_input => "InvalidRelationalExpressionInput",
        .invalid_relational_generated_value => "InvalidRelationalGeneratedValue",
        .generated_column_rewrite_required => "GeneratedColumnRewriteRequired",
        .check_constraint_validation_required => "CheckConstraintValidationRequired",
        .invalid_retained_effects_admission => "InvalidRetainedEffectsAdmission",
        .retained_effects_fence_mismatch => "RetainedEffectsFenceMismatch",
        .retained_effects_mixed_control => "RetainedEffectsMixedControl",
        .retained_effects_transaction_failed => "RetainedEffectsTransactionFailed",
        .transition_operation_busy => "TransitionOperationBusy",
        .unknown_split_runtime => "UnknownSplitRuntime",
        .unknown_merge_runtime => "UnknownMergeRuntime",
        .missing_split_runtime => "MissingSplitRuntime",
        .missing_merge_runtime => "MissingMergeRuntime",
        .metadata_snapshot_unavailable => "MetadataSnapshotUnavailable",
        .metadata_snapshot_head_mismatch => "MetadataSnapshotHeadMismatch",
        .split_source_projection_not_ready => "SplitSourceProjectionNotReady",
        .split_source_projection_advanced => "SplitSourceProjectionAdvanced",
        .durable_root_incarnation_unavailable => "DurableRootIncarnationUnavailable",
        .auto_bulk_ingest_busy => "AutoBulkIngestBusy",
        .apply_store_group_retired => "ApplyStoreGroupRetired",
        .apply_store_shutting_down => "ApplyStoreShuttingDown",
        .group_leader_unavailable => "GroupLeaderUnavailable",
        .intent_conflict => "IntentConflict",
        .version_conflict => "VersionConflict",
        .merge_page_required => "MergePageRequired",
        .invalid_response => "InvalidResponse",
        .transition_operations_retired => "TransitionOperationsRetired",
        .merge_transition_not_ready => "MergeTransitionNotReady",
        .merge_receiver_projection_not_ready => "MergeReceiverProjectionNotReady",
        .merge_source_projection_not_ready => "MergeSourceProjectionNotReady",
        .merge_source_projection_advanced => "MergeSourceProjectionAdvanced",
        .storage_busy => "StorageBusy",
        .storage_owner_admission_pending => "StorageOwnerAdmissionPending",
        .invalid_online_source_command => "InvalidOnlineSourceCommand",
        .online_source_corrupt => "OnlineSourceCorrupt",
        .online_source_pin_missing => "OnlineSourcePinMissing",
        .online_source_pin_pending => "OnlineSourcePinPending",
        .online_source_scope_changed => "OnlineSourceScopeChanged",
        .online_merge_artifact_catalog_uncoordinated => "OnlineMergeArtifactCatalogUncoordinated",
        .invalid_source_snapshot => "InvalidSourceSnapshot",
        .source_snapshot_corrupt => "SourceSnapshotCorrupt",
        .source_snapshot_cut_mismatch => "SourceSnapshotCutMismatch",
        .source_snapshot_incomplete => "SourceSnapshotIncomplete",
        .source_snapshot_too_large => "SourceSnapshotTooLarge",
        .source_copy_restore_unsupported => "SourceCopyRestoreUnsupported",
        .invalid_merge_page => "InvalidMergePage",
        .merge_copy_fenced => "MergeCopyFenced",
        .merge_page_chunk_required => "MergePageChunkRequired",
        .merge_page_incomplete => "MergePageIncomplete",
        .merge_page_sequence_gap => "MergePageSequenceGap",
        .retained_effects_consumer_limit => "RetainedEffectsConsumerLimit",
        .retained_effects_corrupt => "RetainedEffectsCorrupt",
        .retained_effects_cursor_mismatch => "RetainedEffectsCursorMismatch",
        .retained_effects_full => "RetainedEffectsFull",
        .retained_effects_identity_required => "RetainedEffectsIdentityRequired",
        .retained_effects_namespace_mismatch => "RetainedEffectsNamespaceMismatch",
        .backend_runtime_io_unavailable => "BackendRuntimeIoUnavailable",
        .corrupt_raft_applied_entry => "CorruptRaftAppliedEntry",
        .unknown_schema_version => "UnknownSchemaVersion",
        .metadata_ha_binding_busy => "MetadataHABindingBusy",
        .metadata_ha_outbox_pending => "MetadataHAOutboxPending",
        .metadata_ha_checkpoint_target_not_empty => "MetadataHACheckpointTargetNotEmpty",
        .invalid_metadata_ha_checkpoint => "InvalidMetadataHACheckpoint",
        .metadata_ha_migration_after_binding => "MetadataHAMigrationAfterBinding",
        .metadata_ha_incomplete_effect => "MetadataHAIncompleteEffect",
        .metadata_ha_source_changed => "MetadataHASourceChanged",
        .metadata_ha_sequence_gap => "MetadataHASequenceGap",
        .invalid_metadata_ha_effect_chunk => "InvalidMetadataHAEffectChunk",
        .ha_primary_not_configured => "HAPrimaryNotConfigured",
        .relational_index_not_ready => "RelationalIndexNotReady",
        .invalid_relational_index_bound => "InvalidRelationalIndexBound",
        .relational_index_column_not_found => "RelationalIndexColumnNotFound",
        .unsupported_relational_index_column => "UnsupportedRelationalIndexColumn",
        .relational_rows_output_budget_exceeded => "RelationalRowsOutputBudgetExceeded",
        .relational_row_result_too_large => "RelationalRowResultTooLarge",
        .relational_index_column_type_mismatch => "RelationalIndexColumnTypeMismatch",
        .relational_table_required => "RelationalTableRequired",
        .invalid_relational_index_forward_key => "InvalidRelationalIndexForwardKey",
        .integrity_topology_cutover_required => "IntegrityTopologyCutoverRequired",
        .invalid_restore_terminal => "InvalidRestoreTerminal",
        .invalid_standalone_metadata_checkpoint => "InvalidStandaloneMetadataCheckpoint",
        .seed_below_ha_replay_floor => "SeedBelowHAReplayFloor",
        .seed_metadata_topology_mismatch => "SeedMetadataTopologyMismatch",
        .backup_seal_released => "BackupSealReleased",
        .integrity_address_mismatch => "IntegrityAddressMismatch",
        .integrity_handoff_collision => "IntegrityHandoffCollision",
        .integrity_handoff_destination_reset_required => "IntegrityHandoffDestinationResetRequired",
        .integrity_handoff_incomplete => "IntegrityHandoffIncomplete",
        .integrity_handoff_missing => "IntegrityHandoffMissing",
        .integrity_handoff_sequence_changed => "IntegrityHandoffSequenceChanged",
        .missing_integrity_binding => "MissingIntegrityBinding",
        .missing_integrity_catalog => "MissingIntegrityCatalog",
        .integrity_catalog_too_large => "IntegrityCatalogTooLarge",
        .integrity_handoff_too_large => "IntegrityHandoffTooLarge",
        .invalid_integrity_address => "InvalidIntegrityAddress",
        .invalid_integrity_budget => "InvalidIntegrityBudget",
        .invalid_integrity_catalog => "InvalidIntegrityCatalog",
        .invalid_integrity_definition => "InvalidIntegrityDefinition",
        .invalid_integrity_record => "InvalidIntegrityRecord",
        .coordinated_constraints_require_table_identity => "CoordinatedConstraintsRequireTableIdentity",
        .integrity_retirement_backlog_full => "IntegrityRetirementBacklogFull",
        .restore_projection_catch_up_pending => "RestoreProjectionCatchUpPending",
        .integrity_checksum_mismatch => "IntegrityChecksumMismatch",
        .integrity_missing_companion => "IntegrityMissingCompanion",
        .coordinated_constraint_portable_backup_unsupported => "CoordinatedConstraintPortableBackupUnsupported",
        .coordinated_constraint_restore_required => "CoordinatedConstraintRestoreRequired",
        .coordinated_constraint_topology_unsupported => "CoordinatedConstraintTopologyUnsupported",
        .integrity_generation_exhausted => "IntegrityGenerationExhausted",
        .backup_cohort_already_committed => "BackupCohortAlreadyCommitted",
        .backup_cohort_cancelled => "BackupCohortCancelled",
        .backup_cohort_changed => "BackupCohortChanged",
        .backup_cohort_fence_lost => "BackupCohortFenceLost",
        .backup_seal_mismatch => "BackupSealMismatch",
        .backup_seal_source_changed => "BackupSealSourceChanged",
        .restore_staging_canceled => "RestoreStagingCanceled",
        .restore_staging_progress_changed => "RestoreStagingProgressChanged",
        .restore_staging_scope_changed => "RestoreStagingScopeChanged",
        .integrity_topology_changed => "IntegrityTopologyChanged",
        .integrity_topology_completed => "IntegrityTopologyCompleted",
        .integrity_topology_fence_missing => "IntegrityTopologyFenceMissing",
        .integrity_catalog_changed => "IntegrityCatalogChanged",
        .integrity_catalog_incarnation_mismatch => "IntegrityCatalogIncarnationMismatch",
        .constraint_activation_changed => "ConstraintActivationChanged",
        .constraint_activation_owner_changed => "ConstraintActivationOwnerChanged",
        .constraint_retirement_changed => "ConstraintRetirementChanged",
        .foreign_key_parent_missing => "ForeignKeyParentMissing",
        .foreign_key_referenced => "ForeignKeyReferenced",
        .foreign_key_match_full_violation => "ForeignKeyMatchFullViolation",
        .unique_constraint_violation => "UniqueConstraintViolation",
        .unsupported_native_index_owner => "UnsupportedNativeIndexOwner",
        .invalid_private_index_claim_inventory => "InvalidPrivateIndexClaimInventory",
        .invalid_private_index_claim_receipt => "InvalidPrivateIndexClaimReceipt",
        .private_index_owner_job_changed => "PrivateIndexOwnerJobChanged",
        .private_index_publication_canceled => "PrivateIndexPublicationCanceled",
        .private_index_job_required => "PrivateIndexJobRequired",
        .private_index_root_recovery_required => "PrivateIndexRootRecoveryRequired",
        .invalid_relational_index_budget => "InvalidRelationalIndexBudget",
        .private_index_claim_backlog_full => "PrivateIndexClaimBacklogFull",
        .private_index_publication_required => "PrivateIndexPublicationRequired",
        .invalid_private_index_owner_job => "InvalidPrivateIndexOwnerJob",
        .invalid_private_index_inputs => "InvalidPrivateIndexInputs",
        .relational_index_catalog_schema_mismatch => "RelationalIndexCatalogSchemaMismatch",
        .invalid_index_publication_candidate => "InvalidIndexPublicationCandidate",
        .immutable_relational_index_definition_conflict => "ImmutableRelationalIndexDefinitionConflict",
        .private_index_cancellation_pending => "PrivateIndexCancellationPending",
        .private_index_cancellation_required => "PrivateIndexCancellationRequired",
        .invalid_private_index_cancellation => "InvalidPrivateIndexCancellation",
        .invalid_private_index_native_receipt => "InvalidPrivateIndexNativeReceipt",
        .invalid_relational_index_progress => "InvalidRelationalIndexProgress",
        .missing_relational_index_definitions => "MissingRelationalIndexDefinitions",
        .index_owner_receipt_changed => "IndexOwnerReceiptChanged",
        .partial_index_predicate_not_implied => "PartialIndexPredicateNotImplied",
        .invalid_index_owner_receipt => "InvalidIndexOwnerReceipt",
        .prepared_generation_changed => "PreparedGenerationChanged",
        .prepared_read_set_changed => "PreparedReadSetChanged",
        .foreign_key_action_mismatch => "ForeignKeyActionMismatch",
        .foreign_key_action_not_validated => "ForeignKeyActionNotValidated",
        .constraint_activation_failed => "ConstraintActivationFailed",
        .foreign_key_action_failed => "ForeignKeyActionFailed",
        .restore_staging_target_not_empty => "RestoreStagingTargetNotEmpty",
        .backup_pin_source_unavailable => "BackupPinSourceUnavailable",
        .restore_source_durability_uncertain => "RestoreSourceDurabilityUncertain",
        .restore_staging_in_progress => "RestoreStagingInProgress",
        .restore_validation_pending => "RestoreValidationPending",
        .integrity_topology_busy => "IntegrityTopologyBusy",
        .transaction_topology_busy => "TransactionTopologyBusy",
        .relational_topology_protocol_upgrade_required => "RelationalTopologyProtocolUpgradeRequired",
        .integrity_catalog_unavailable => "IntegrityCatalogUnavailable",
        .constraint_retirement_in_progress => "ConstraintRetirementInProgress",
        .constraint_activation_in_progress => "ConstraintActivationInProgress",
        .foreign_key_action_in_progress => "ForeignKeyActionInProgress",
        .coordinated_ttl_backpressure => "CoordinatedTtlBackpressure",
        .metadata_capability_unavailable => "MetadataCapabilityUnavailable",
        .backup_cohort_too_large => "BackupCohortTooLarge",
        .incomplete_backup_cohort => "IncompleteBackupCohort",
        .invalid_backup_cohort => "InvalidBackupCohort",
        .backup_seal_inventory_too_large => "BackupSealInventoryTooLarge",
        .invalid_backup_seal => "InvalidBackupSeal",
        .restore_dependency_missing => "RestoreDependencyMissing",
        .restore_source_proof_missing => "RestoreSourceProofMissing",
        .invalid_restore_source_checkpoint => "InvalidRestoreSourceCheckpoint",
        .invalid_restore_staging => "InvalidRestoreStaging",
        .invalid_restore_staging_command => "InvalidRestoreStagingCommand",
        .invalid_restore_staging_record => "InvalidRestoreStagingRecord",
        .invalid_integrity_topology_fence => "InvalidIntegrityTopologyFence",
        .invalid_metadata_ha_effect => "InvalidMetadataHAEffect",
        .invalid_metadata_ha_chunk => "InvalidMetadataHAChunk",
        .metadata_ha_checkpoint_too_large => "MetadataHACheckpointTooLarge",
        .metadata_ha_effect_too_large => "MetadataHAEffectTooLarge",
        .invalid_constraint_activation_command => "InvalidConstraintActivationCommand",
        .invalid_constraint_retirement_command => "InvalidConstraintRetirementCommand",
        .key_out_of_range => "KeyOutOfRange",
        .invalid_integrity_command => "InvalidIntegrityCommand",
        .invalid_integrity_operation => "InvalidIntegrityOperation",
        .invalid_integrity_continuation => "InvalidIntegrityContinuation",
        .integrity_claim_guard_required => "IntegrityClaimGuardRequired",
        .integrity_action_job_required => "IntegrityActionJobRequired",
        .invalid_relational_rows_request => "InvalidRelationalRowsRequest",
        .constraint_retirement_required => "ConstraintRetirementRequired",
        .invalid_integrity_key => "InvalidIntegrityKey",
        .integrity_record_too_large => "IntegrityRecordTooLarge",
        .invalid_range => "InvalidRange",
        .backup_seal_backend_unsupported => "BackupSealBackendUnsupported",
        .relational_topology_migration_unsupported => "RelationalTopologyMigrationUnsupported",
        .foreign_key_coordination_required => "ForeignKeyCoordinationRequired",
        .constraint_not_found => "ConstraintNotFound",
        .integrity_topology_epoch_exhausted => "IntegrityTopologyEpochExhausted",
        .none => "RuntimeBoundaryFailure",
        .ha_seed_snapshot_runtime_busy => "HASeedSnapshotRuntimeBusy",
        .ha_seed_capture_already_in_progress => "HASeedCaptureAlreadyInProgress",
        .storage_kernel_owner_unavailable => "StorageKernelOwnerUnavailable",
        .invalid_table_storage_settings => "InvalidTableStorageSettings",
        .vector_store_requires_local_single_shard_table => "VectorStoreRequiresLocalSingleShardTable",
        .vector_store_requires_empty_table => "VectorStoreRequiresEmptyTable",
        .immutable_table_storage_settings => "ImmutableTableStorageSettings",
        .vector_store_lifecycle_unsupported => "VectorStoreLifecycleUnsupported",
        .vector_store_reference_format_required => "VectorStoreReferenceFormatRequired",
        .invalid_vector_migration_budget => "InvalidVectorMigrationBudget",
        .invalid_vector_migration_id => "InvalidVectorMigrationId",
        .invalid_vector_migration_state => "InvalidVectorMigrationState",
        .unsupported_vector_migration_direction => "UnsupportedVectorMigrationDirection",
        .unsupported_vector_migration_version => "UnsupportedVectorMigrationVersion",
        .vector_migration_active => "VectorMigrationActive",
        .vector_migration_already_exists => "VectorMigrationAlreadyExists",
        .vector_migration_already_published => "VectorMigrationAlreadyPublished",
        .vector_migration_configuration_changed => "VectorMigrationConfigurationChanged",
        .vector_migration_coverage_mismatch => "VectorMigrationCoverageMismatch",
        .vector_migration_disk_reserve => "VectorMigrationDiskReserve",
        .vector_migration_idempotency_conflict => "VectorMigrationIdempotencyConflict",
        .vector_migration_identity_mismatch => "VectorMigrationIdentityMismatch",
        .vector_migration_inline_payload_remains => "VectorMigrationInlinePayloadRemains",
        .vector_migration_not_found => "VectorMigrationNotFound",
        .vector_migration_not_ready => "VectorMigrationNotReady",
        .vector_migration_read_epoch_changed => "VectorMigrationReadEpochChanged",
        .vector_migration_recovery_required => "VectorMigrationRecoveryRequired",
        .vector_migration_row_exceeds_budget => "VectorMigrationRowExceedsBudget",
        .vector_migration_temporary_budget_exceeded => "VectorMigrationTemporaryBudgetExceeded",
        .vector_migration_offline_admission => "VectorMigrationOfflineAdmission",
        .vector_migration_catalog_in_use => "VectorMigrationCatalogInUse",
        .vector_migration_copy_mismatch => "VectorMigrationCopyMismatch",
        .vector_migration_unsupported_file => "VectorMigrationUnsupportedFile",
        .vector_store_requires_offline_command => "VectorStoreRequiresOfflineCommand",

        .out_of_memory => "OutOfMemory",
        .invalid_argument => "InvalidArgument",
        .invalid_arguments => "InvalidArguments",
        .invalid_request => "InvalidRequest",
        .invalid_query_request => "InvalidQueryRequest",
        .invalid_filter_query_request => "InvalidFilterQueryRequest",
        .invalid_exclusion_query_request => "InvalidExclusionQueryRequest",
        .invalid_batch_request => "InvalidBatchRequest",
        .invalid_create_table_request => "InvalidCreateTableRequest",
        .unsupported_create_table_request => "UnsupportedCreateTableRequest",
        .invalid_schema_update_request => "InvalidSchemaUpdateRequest",
        .invalid_manifest => "InvalidManifest",
        .invalid_table_file => "InvalidTableFile",
        .invalid_txn_record => "InvalidTxnRecord",
        .not_found => "NotFound",
        .file_not_found => "FileNotFound",
        .table_not_found => "TableNotFound",
        .table_already_exists => "TableAlreadyExists",
        .index_not_found => "IndexNotFound",
        .model_not_found => "ModelNotFound",
        .secret_not_found => "SecretNotFound",
        .user_not_found => "UserNotFound",
        .unknown_group => "UnknownGroup",
        .txn_not_found => "TxnNotFound",
        .conflict => "Conflict",
        .decision_conflict => "DecisionConflict",
        .table_transition_active => "TableTransitionActive",
        .schema_in_use => "SchemaInUse",
        .transaction_too_large => "TransactionTooLarge",
        .extension_owned_object => "ExtensionOwnedObject",
        .restore_intent_conflict => "RestoreIntentConflict",
        .unauthorized => "Unauthorized",
        .forbidden => "Forbidden",
        .unavailable => "Unavailable",
        .write_unavailable => "WriteUnavailable",
        .read_unavailable => "ReadUnavailable",
        .read_requires_primary => "ReadRequiresPrimary",
        .distributed_query_unavailable => "DistributedQueryUnavailable",
        .storage_read_temporarily_unavailable => "StorageReadTemporarilyUnavailable",
        .resource_request_too_large => "ResourceRequestTooLarge",
        .resource_temporarily_unavailable => "ResourceTemporarilyUnavailable",
        .restore_job_persistence_unavailable => "RestoreJobPersistenceUnavailable",
        .backpressured => "Backpressured",
        .dense_repair_backpressure => "DenseRepairBackpressure",
        .outcome_unknown => "OutcomeUnknown",
        .commit_propagation_incomplete => "CommitPropagationIncomplete",
        .commit_decision_unknown => "CommitDecisionUnknown",
        .committed_pending => "CommittedPending",
        .write_outcome_unknown => "WriteOutcomeUnknown",
        .doc_identity_unavailable => "DocIdentityUnavailable",
        .ha_read_only_standby => "HAReadOnlyStandby",
        .ha_promoted_standby_requires_primary_open => "HAPromotedStandbyRequiresPrimaryOpen",
        .ha_fenced_primary => "HAFencedPrimary",
        .internal_failure => "InternalFailure",
        .not_leader => "NotLeader",
        .topology_changed => "TopologyChanged",
        .index_generation_mismatch => "IndexGenerationMismatch",
        .identity_read_generation_changed => "IdentityReadGenerationChanged",
        .doc_identity_namespace_mismatch => "DocIdentityNamespaceMismatch",
        .table_generation_changed => "TableGenerationChanged",
        .table_lifecycle_conflict => "TableLifecycleConflict",
        .generation_durability_uncertain => "GenerationDurabilityUncertain",
        .generation_transition_active => "GenerationTransitionActive",
        .reembed_budget_exceeded => "ReembedBudgetExceeded",
        .reembed_setup_retry => "ReembedSetupRetry",
        .incomplete_published_snapshot => "IncompletePublishedSnapshot",
        .table_visibility_timeout => "TableVisibilityTimeout",
        .file_busy => "FileBusy",
        .would_block => "WouldBlock",
        .writer_locked => "WriterLocked",
        .lsm_root_writer_already_open => "LsmRootWriterAlreadyOpen",
        .unexpected_http_status => "UnexpectedHttpStatus",
        .method_not_allowed => "MethodNotAllowed",
        .unsupported => "Unsupported",
        .unsupported_operation => "UnsupportedOperation",
        .unsupported_tensor_type => "UnsupportedTensorType",
        .unsupported_query_request => "UnsupportedQueryRequest",
        .unsupported_node_filter_query => "UnsupportedNodeFilterQuery",
        .catalog_changed => "CatalogChanged",
        .unsupported_filter_query_request => "UnsupportedFilterQueryRequest",
        .unsupported_exclusion_query_request => "UnsupportedExclusionQueryRequest",
        .unsupported_sync_level => "UnsupportedSyncLevel",
        .unsupported_exact_sort => "UnsupportedExactSort",
        .unsupported_version => "UnsupportedVersion",
        .timeout => "Timeout",
        .read_index_timeout => "ReadIndexTimeout",
        .connection_timeout => "ConnectionTimeout",
        .connection_timed_out => "ConnectionTimedOut",
        .cancelled => "Cancelled",
        .canceled => "Canceled",
        .connection_refused => "ConnectionRefused",
        .connection_reset_by_peer => "ConnectionResetByPeer",
        .network_unreachable => "NetworkUnreachable",
        .host_unreachable => "HostUnreachable",
        .temporary_name_server_failure => "TemporaryNameServerFailure",
        .name_server_failure => "NameServerFailure",
        .query_candidate_budget_exceeded => "QueryCandidateBudgetExceeded",
        .value_verification_requires_scan => "ValueSearchRequiresScan",
        .value_verification_limit_exceeded => "ValueSearchLimitExceeded",
        .decision_limit_exceeded => "DecisionLimitExceeded",
        .query_embedding_input_too_large => "QueryEmbeddingInputTooLarge",
        .query_embedding_overloaded => "QueryEmbeddingOverloaded",
        .embed_rate_limited => "EmbedRateLimited",
        .embed_transient_failure => "EmbedTransientFailure",
        .embed_upstream_failure => "EmbedUpstreamFailure",
        .embedding_probe_unavailable => "EmbeddingProbeUnavailable",
        .inference_endpoint_access_denied => "InferenceEndpointAccessDenied",
        .invalid_embedding_response => "InvalidEmbeddingResponse",
        .invalid_embedding_dimensions => "InvalidEmbeddingDimensions",
        .backup_already_exists => "BackupAlreadyExists",
        .backup_manifest_too_large => "BackupManifestTooLarge",
        .backup_integrity_failure => "BackupIntegrityFailure",
        .backup_artifact_integrity_mismatch => "BackupArtifactIntegrityMismatch",
        .backup_repository_busy => "BackupRepositoryBusy",
        .invalid_backup_request => "InvalidBackupRequest",
        .restore_durability_pending => "RestoreDurabilityPending",
        .restore_durability_confirmed => "RestoreDurabilityConfirmed",
        .corrupt_input => "CorruptInput",
        .corrupted => "Corrupted",
        .corrupt_restore_job_store => "CorruptRestoreJobStore",
        .table_block_checksum_mismatch => "TableBlockChecksumMismatch",
        .value_too_long => "ValueTooLong",
        .unsupported_platform => "UnsupportedPlatform",
        .unsupported_transform_operation => "UnsupportedTransformOperation",
        .ha_read_requires_primary => "HAReadRequiresPrimary",
        .ha_read_wait_for_apply => "HAReadWaitForApply",
        .ha_read_wait_for_metadata => "HAReadWaitForMetadata",
        .persistent_descriptor_admission_exhausted => "PersistentDescriptorAdmissionExhausted",
        .process_fd_quota_exceeded => "ProcessFdQuotaExceeded",
        .system_fd_quota_exceeded => "SystemFdQuotaExceeded",
        .storage_descriptor_pressure => "StorageDescriptorPressure",
        .input_output => "InputOutput",
        .invalid_graph_edges => "InvalidGraphEdges",
        .invalid_table_index_metadata => "InvalidTableIndexMetadata",
        .transaction_begin_failed => "TransactionBeginFailed",
        .abort_decision_not_durable => "AbortDecisionNotDurable",
        .commit_visibility_not_satisfied => "CommitVisibilityNotSatisfied",
        .leader_unavailable => "LeaderUnavailable",
        .raft_batch_write_outcome_unknown => "RaftBatchWriteOutcomeUnknown",
        .raft_apply_writer_unavailable => "RaftApplyWriterUnavailable",
        .sql_database_in_use => "SqlDatabaseInUse",
        .invalid_sql_relation_persistence => "InvalidSqlRelationPersistence",
        .sql_relation_owner_changed => "SqlRelationOwnerChanged",
        .sql_temporary_relation_not_owned => "SqlTemporaryRelationNotOwned",
        .invalid_sql_temporary_namespace => "InvalidSqlTemporaryNamespace",
        .sql_relation_persistence_foreign_key_mismatch => "SqlRelationPersistenceForeignKeyMismatch",
        .sql_relation_recovery_incomplete => "SqlRelationRecoveryIncomplete",
        .sql_relation_owner_unavailable => "SqlRelationOwnerUnavailable",
        .invalid_scope_retirement => "InvalidScopeRetirement",
        .scope_retirement_changed => "ScopeRetirementChanged",
        .sequence_admission_conflict => "SequenceAdmissionConflict",
        .invalid_table_retirement_manifest => "InvalidTableRetirementManifest",
        .catalog_revision_exhausted => "CatalogRevisionExhausted",
        .duplicate_sql_extension_export => "DuplicateSqlExtensionExport",
        .duplicate_sql_extension_version => "DuplicateSqlExtensionVersion",
        .inconsistent_sql_extension_control => "InconsistentSqlExtensionControl",
        .invalid_package_digest => "InvalidPackageDigest",
        .invalid_sql_extension_artifact => "InvalidSqlExtensionArtifact",
        .invalid_sql_extension_control => "InvalidSqlExtensionControl",
        .invalid_sql_extension_dependency => "InvalidSqlExtensionDependency",
        .invalid_sql_extension_record => "InvalidSqlExtensionRecord",
        .invalid_sql_extension_runtime => "InvalidSqlExtensionRuntime",
        .invalid_sql_extension_package_mutation => "InvalidSqlExtensionPackageMutation",
        .duplicate_sql_extension_control => "DuplicateSqlExtensionControl",
        .sql_dependent_extension_object => "SqlDependentExtensionObject",
        .sql_duplicate_extension => "SqlDuplicateExtension",
        .sql_duplicate_function => "SqlDuplicateFunction",
        .sql_extension_control_conflict => "SqlExtensionControlConflict",
        .sql_extension_dependency_cycle => "SqlExtensionDependencyCycle",
        .sql_extension_promotion_nonce_conflict => "SqlExtensionPromotionNonceConflict",
        .sql_extension_schema_mismatch => "SqlExtensionSchemaMismatch",
        .sql_extension_unavailable => "SqlExtensionUnavailable",
        .sql_extension_update_path_unavailable => "SqlExtensionUpdatePathUnavailable",
        .sql_extension_version_unavailable => "SqlExtensionVersionUnavailable",
        .sql_function_return_type_changed => "SqlFunctionReturnTypeChanged",
        .sql_required_extension_not_installed => "SqlRequiredExtensionNotInstalled",
        .sql_undefined_extension => "SqlUndefinedExtension",
        .unsupported_sql_extension_object => "UnsupportedSqlExtensionObject",
        .sql_extension_owned_object => "SqlExtensionOwnedObject",
        .duplicate_shape_name => "DuplicateShapeName",
        .generated_artifact_shape_required => "GeneratedArtifactShapeRequired",
        .no_supported_scopes => "NoSupportedScopes",
        .scope_must_not_name_table => "ScopeMustNotNameTable",
        .unknown_shape_reference => "UnknownShapeReference",
        .sql_program_limit_exceeded => "SqlProgramLimitExceeded",
        .snapshot_build_cancelled => "SnapshotBuildCancelled",
        .catalog_generation_exhausted => "CatalogGenerationExhausted",
        .invalid_sql_trigger_definition => "InvalidSqlTriggerDefinition",
        .invalid_sql_trigger_manifest => "InvalidSqlTriggerManifest",
        .sql_trigger_context_required => "SqlTriggerContextRequired",
        .sql_trigger_returned_no_value => "SqlTriggerReturnedNoValue",
        .sql_trigger_return_value_required => "SqlTriggerReturnValueRequired",
        .sql_trigger_declared_arguments => "SqlTriggerDeclaredArguments",
        .sql_trigger_sql_language => "SqlTriggerSqlLanguage",
        .sql_trigger_coordination_required => "SqlTriggerCoordinationRequired",
        .invalid_generation_transaction => "InvalidGenerationTransaction",
        .generation_transaction_changed => "GenerationTransactionChanged",
        .generation_transaction_decision_required => "GenerationTransactionDecisionRequired",
        .invalid_prepared_graph_effects => "InvalidPreparedGraphEffects",
        .sql_prepared_durability_unavailable => "SqlPreparedDurabilityUnavailable",
        .invalid_participant => "InvalidParticipant",

        .raft_batch_write_transport_outcome_unknown => "RaftBatchWriteTransportOutcomeUnknown",
        .http_connection_closing => "HttpConnectionClosing",
        .raft_batch_write_partial_outcome => "RaftBatchWritePartialOutcome",
        .enrichment_retry_in_progress => "EnrichmentRetryInProgress",
        .resource_budget_exceeded => "ResourceBudgetExceeded",
        .text_merge_backpressure_timeout => "TextMergeBackpressureTimeout",
        .text_merge_backpressure_unavailable => "TextMergeBackpressureUnavailable",
        .text_merge_runtime_shutdown => "TextMergeRuntimeShutdown",
        .background_owner_closing => "BackgroundOwnerClosing",
        .invalid_enrichment_config => "InvalidEnrichmentConfig",
        .restore_identity_mismatch => "RestoreIdentityMismatch",
        .table_read_churn => "TableReadChurn",
        .unsupported_multi_range_table => "UnsupportedMultiRangeTable",
        .proposal_dropped => "ProposalDropped",
        .leader_transfer_in_progress => "LeaderTransferInProgress",
        .metadata_linearizable_read_timeout => "MetadataLinearizableReadTimeout",
        .reconcile_lease_not_held => "ReconcileLeaseNotHeld",
        .enrichment_not_found => "EnrichmentNotFound",
        .invalid_extension_enrichment => "InvalidExtensionEnrichment",
        .conflicting_enrichment_config => "ConflictingEnrichmentConfig",
        .invalid_create_index_request => "InvalidCreateIndexRequest",
        .unsupported_backup_migration_state => "UnsupportedBackupMigrationState",
        .unsupported_backup_format => "UnsupportedBackupFormat",
        .extension_already_installed => "ExtensionAlreadyInstalled",
        .extension_not_installed => "ExtensionNotInstalled",
        .extension_disabled => "ExtensionDisabled",
        .extension_lifecycle_busy => "ExtensionLifecycleBusy",
        .extension_visibility_timeout => "ExtensionVisibilityTimeout",
        .dependent_extension_exists => "DependentExtensionExists",
        .required_extension_not_installed => "RequiredExtensionNotInstalled",
        .missing_extension_package => "MissingExtensionPackage",
        .package_not_found => "PackageNotFound",
        .package_version_mismatch => "PackageVersionMismatch",
        .unsupported_package_kind => "UnsupportedPackageKind",
        .invalid_extension_shape => "InvalidExtensionShape",
        .unsupported_extension_scope => "UnsupportedExtensionScope",
        .invalid_extension_lifecycle_request => "InvalidExtensionLifecycleRequest",
        .restore_job_fenced => "RestoreJobFenced",
        .restore_job_capacity_exceeded => "RestoreJobCapacityExceeded",
        .restore_job_record_too_large => "RestoreJobRecordTooLarge",
        .too_many_restore_tables => "TooManyRestoreTables",
        .duplicate_restore_table_name => "DuplicateRestoreTableName",
        .idempotency_conflict => "IdempotencyConflict",
        .invalid_idempotency_key => "InvalidIdempotencyKey",
        .async_restore_unavailable => "AsyncRestoreUnavailable",
        .background_runtime_unavailable => "BackgroundRuntimeUnavailable",
        .backup_staging_unavailable => "BackupStagingUnavailable",
        .backup_artifact_missing => "BackupArtifactMissing",
        .backup_integrity_missing => "BackupIntegrityMissing",
        .backup_outcome_ambiguous => "BackupOutcomeAmbiguous",
        .metadata_mutation_not_applied => "MetadataMutationNotApplied",
        .backup_attempt_lease_lost => "BackupAttemptLeaseLost",
        .background_owner_closed => "BackgroundOwnerClosed",
        .restore_scheduling_capacity => "RestoreSchedulingCapacity",
        .restore_wait_interrupted => "RestoreWaitInterrupted",
        .api_key_expired => "ApiKeyExpired",
        .api_key_invalid => "ApiKeyInvalid",
        .api_key_not_found => "ApiKeyNotFound",
        .auth_row_filter_no_match => "AuthRowFilterNoMatch",
        .backup_maintenance_queue_full => "BackupMaintenanceQueueFull",
        .backup_maintenance_unavailable => "BackupMaintenanceUnavailable",
        .connection_not_found => "ConnectionNotFound",
        .embed_request_failed => "EmbedRequestFailed",
        .empty_embedding_response => "EmptyEmbeddingResponse",
        .empty_name => "EmptyName",
        .end_of_stream => "EndOfStream",
        .foreign_query_failed => "ForeignQueryFailed",
        .invalid_api_key_request => "InvalidApiKeyRequest",
        .invalid_api_server_config => "InvalidApiServerConfig",
        .invalid_ard_agents_request => "InvalidArdAgentsRequest",
        .invalid_ard_search_request => "InvalidArdSearchRequest",
        .invalid_backup_id => "InvalidBackupId",
        .invalid_backup_list_limit => "InvalidBackupListLimit",
        .invalid_character => "InvalidCharacter",
        .invalid_create_user_request => "InvalidCreateUserRequest",
        .invalid_detail => "InvalidDetail",
        .invalid_duration => "InvalidDuration",
        .invalid_enrichment_request => "InvalidEnrichmentRequest",
        .invalid_eval_request => "InvalidEvalRequest",
        .invalid_identifier => "InvalidIdentifier",
        .invalid_index_request => "InvalidIndexRequest",
        .invalid_job_id => "InvalidJobId",
        .invalid_json_object => "InvalidJsonObject",
        .invalid_linear_merge_request => "InvalidLinearMergeRequest",
        .invalid_metadata => "InvalidMetadata",
        .invalid_password => "InvalidPassword",
        .invalid_password_update_request => "InvalidPasswordUpdateRequest",
        .invalid_path_parameter => "InvalidPathParameter",
        .invalid_permission_type => "InvalidPermissionType",
        .invalid_query_builder_request => "InvalidQueryBuilderRequest",
        .invalid_read_consistency => "InvalidReadConsistency",
        .invalid_resource_type => "InvalidResourceType",
        .invalid_restore_job_list_cursor => "InvalidRestoreJobListCursor",
        .invalid_restore_job_list_limit => "InvalidRestoreJobListLimit",
        .invalid_restore_job_list_phase => "InvalidRestoreJobListPhase",
        .invalid_restore_job_list_scope => "InvalidRestoreJobListScope",
        .invalid_retrieval_agent_request => "InvalidRetrievalAgentRequest",
        .invalid_role => "InvalidRole",
        .invalid_secret_key => "InvalidSecretKey",
        .invalid_transaction_commit_request => "InvalidTransactionCommitRequest",
        .invalid_transaction_session_record => "InvalidTransactionSessionRecord",
        .maintenance_busy => "MaintenanceBusy",
        .maintenance_history_full => "MaintenanceHistoryFull",
        .maintenance_job_id_exhausted => "MaintenanceJobIdExhausted",
        .member_table_outside_scope => "MemberTableOutsideScope",
        .missing_field => "MissingField",
        .missing_location => "MissingLocation",
        .missing_resource => "MissingResource",
        .missing_resource_type => "MissingResourceType",
        .missing_role => "MissingRole",
        .no_tables => "NoTables",
        .path_already_exists => "PathAlreadyExists",
        .privilege_escalation => "PrivilegeEscalation",
        .probe_unavailable => "ProbeUnavailable",
        .role_not_found => "RoleNotFound",
        .row_filter_not_found => "RowFilterNotFound",
        .savepoint_limit_exceeded => "SavepointLimitExceeded",
        .session_lease_lost => "SessionLeaseLost",
        .session_limit_exceeded => "SessionLimitExceeded",
        .session_record_too_large => "SessionRecordTooLarge",
        .stale_index_repair_control => "StaleIndexRepairControl",
        .syntax_error => "SyntaxError",
        .too_many_runtime_statuses => "TooManyRuntimeStatuses",
        .transaction_commit_request_mismatch => "TransactionCommitRequestMismatch",
        .transaction_commit_sealed => "TransactionCommitSealed",
        .tree_root_set_too_large => "TreeRootSetTooLarge",
        .unrequested_capability_grant => "UnrequestedCapabilityGrant",
        .unsupported_aggregate => "UnsupportedAggregate",
        .unsupported_backup_location => "UnsupportedBackupLocation",
        .unsupported_manifest_api_version => "UnsupportedManifestApiVersion",
        .unsupported_object_kind_for_v1 => "UnsupportedObjectKindForV1",
        .unsupported_retrieval_agent_request => "UnsupportedRetrievalAgentRequest",
        .unsupported_source_kind => "UnsupportedSourceKind",
        .update_path_not_found => "UpdatePathNotFound",
        .user_exists => "UserExists",
        .queue_full => "QueueFull",
        .resource_limit_exceeded => "ResourceLimitExceeded",
        .body_too_large => "BodyTooLarge",
        .body_capacity_exceeded => "BodyCapacityExceeded",
        .body_read_failed => "BodyReadFailed",
        .inference_provider_failure => "InferenceProviderFailure",
        .inference_worker_executable_not_configured => "InferenceWorkerExecutableNotConfigured",
        .inference_worker_unavailable => "InferenceWorkerUnavailable",
        .kernel_jit_required_dynamic_load => "KernelJitRequiredDynamicLoad",
        .unexpected_token => "UnexpectedToken",
        .enrichment_wait_canceled => "EnrichmentWaitCanceled",
        .enrichment_wait_timeout => "EnrichmentWaitTimeout",
        .enrichment_worker_failed => "EnrichmentWorkerFailed",
        .ha_sync_commit_would_block => "HASyncCommitWouldBlock",
        .ha_sync_commit_wait_limit_exceeded => "HASyncCommitWaitLimitExceeded",
        .ha_sync_commit_wait_missing_context => "HASyncCommitWaitMissingContext",
        .ha_sync_commit_wait_standby_not_in_policy => "HASyncCommitWaitStandbyNotInPolicy",
        .deadline_exceeded => "DeadlineExceeded",
        .pre_decision_deadline_exceeded => "PreDecisionDeadlineExceeded",
        .graph_metric_action_partial_outcome => "GraphMetricActionPartialOutcome",
        .graph_distinct_budget_exceeded => "GraphDistinctBudgetExceeded",
        .graph_anchor_filter_requires_index => "GraphAnchorFilterRequiresIndex",
        .graph_match_operation_limit_exceeded => "GraphMatchOperationLimitExceeded",
        .graph_cross_range_mode_unsupported => "GraphQueryModeUnsupported",
        .graph_start_identity_required => "GraphStartIdentityRequired",
        .graph_work_budget_exceeded => "GraphWorkBudgetExceeded",
        .graph_min_weight_domain_violation => "GraphMinWeightDomainViolation",
        .graph_max_weight_domain_violation => "GraphMaxWeightDomainViolation",
        .graph_path_weight_overflow => "GraphPathWeightOverflow",
        .metadata_proposal_superseded => "MetadataProposalSuperseded",
        .metadata_proposal_apply_timeout => "MetadataProposalApplyTimeout",
        .metadata_mutation_outcome_unknown => "MetadataMutationOutcomeUnknown",
        .missing_embedding_artifact_producer => "MissingEmbeddingArtifactProducer",
        .invalid_embedding_artifact_producer => "InvalidEmbeddingArtifactProducer",
        .embedding_artifact_dimension_required => "EmbeddingArtifactDimensionRequired",
        .conflicting_embedding_artifact_dimensions => "ConflictingEmbeddingArtifactDimensions",
        .native_backup_repair_state_not_quiescent => "NativeBackupRepairStateNotQuiescent",
        .native_backup_projection_not_quiescent => "NativeBackupProjectionNotQuiescent",
        .native_backup_projection_repair_failed => "NativeBackupProjectionRepairFailed",
        .native_backup_projection_repair_paused => "NativeBackupProjectionRepairPaused",
        .reranker_candidate_limit_exceeded => "RerankerCandidateLimitExceeded",
        .rerank_rate_limited => "RerankRateLimited",
        .rerank_transient_failure => "RerankTransientFailure",
        .rerank_upstream_failure => "RerankUpstreamFailure",
        .incompatible_model => "IncompatibleModel",
        .unsupported_generator_provider => "UnsupportedGeneratorProvider",
        .generate_request_failed => "GenerateRequestFailed",
        .generation_capacity_unavailable => "GenerationCapacityUnavailable",
        .invalid_generated_tool_arguments => "InvalidGeneratedToolArguments",
        .generation_rate_limit => "RateLimit",
        .invalid_rate_limit_policy => "InvalidRateLimitPolicy",
        .conflicting_rate_limit_policy => "ConflictingRateLimitPolicy",
        .provider_token_budget_exceeded => "ProviderTokenBudgetExceeded",
        .provider_quota_registry_full => "ProviderQuotaRegistryFull",
        .unsupported_media_token_budget => "UnsupportedMediaTokenBudget",
        .unsupported_local_rate_limit => "UnsupportedLocalRateLimit",
        .storage_kernel_failure => "StorageKernelFailure",
        .warm_inference_server_unavailable => "WarmInferenceServerUnavailable",
        .unsupported_server_generate_option => "UnsupportedServerGenerateOption",
        .missing_inference_listener => "MissingInferenceListener",
        .runtime_failed => "RuntimeFailed",
        .runtime_stopping => "RuntimeStopping",
        .runtime_already_ready => "RuntimeAlreadyReady",
        .embed_admission_refused => "EmbedAdmissionRefused",
        .read_output_truncated => "ReadOutputTruncated",
        .invalid_document_extraction_config => "InvalidDocumentExtractionConfig",
        .document_extraction_model_required => "DocumentExtractionModelRequired",
        .index_rebuilding => "IndexRebuilding",
        .sql_statement_read_unavailable => "SqlStatementReadUnavailable",
        .unsupported_sql_execution => "UnsupportedSqlExecution",
        .conflicting_privacy_producer_options => "ConflictingPrivacyProducerOptions",
        .conflicting_privacy_producer_schema => "ConflictingPrivacyProducerSchema",
        .invalid_privacy_catalog => "InvalidPrivacyCatalog",
        .invalid_privacy_categories => "InvalidPrivacyCategories",
        .invalid_privacy_category => "InvalidPrivacyCategory",
        .invalid_privacy_confidence => "InvalidPrivacyConfidence",
        .invalid_privacy_document => "InvalidPrivacyDocument",
        .invalid_privacy_execution => "InvalidPrivacyExecution",
        .invalid_privacy_field_type => "InvalidPrivacyFieldType",
        .invalid_privacy_fields => "InvalidPrivacyFields",
        .invalid_privacy_offset_unit => "InvalidPrivacyOffsetUnit",
        .invalid_privacy_output => "InvalidPrivacyOutput",
        .invalid_privacy_output_cardinality => "InvalidPrivacyOutputCardinality",
        .invalid_privacy_producer => "InvalidPrivacyProducer",
        .invalid_privacy_receipt => "InvalidPrivacyReceipt",
        .invalid_privacy_scan => "InvalidPrivacyScan",
        .invalid_privacy_source => "InvalidPrivacySource",
        .invalid_privacy_span => "InvalidPrivacySpan",
        .invalid_privacy_threshold => "InvalidPrivacyThreshold",
        .invalid_privacy_token_geometry => "InvalidPrivacyTokenGeometry",
        .invalid_privacy_window_receipt => "InvalidPrivacyWindowReceipt",
        .privacy_embedding_producer_required => "PrivacyEmbeddingProducerRequired",
        .privacy_evidence_mismatch => "PrivacyEvidenceMismatch",
        .privacy_external_embeddings_unsupported => "PrivacyExternalEmbeddingsUnsupported",
        .privacy_external_vectors_unsupported => "PrivacyExternalVectorsUnsupported",
        .privacy_inspection_failed => "PrivacyInspectionFailed",
        .privacy_literal_credential => "PrivacyLiteralCredential",
        .privacy_managed_graph => "PrivacyManagedGraph",
        .privacy_pending => "PrivacyPending",
        .privacy_precondition_failed => "PrivacyPreconditionFailed",
        .privacy_precondition_required => "PrivacyPreconditionRequired",
        .privacy_producer_revision_mismatch => "PrivacyProducerRevisionMismatch",
        .privacy_producer_revision_required => "PrivacyProducerRevisionRequired",
        .privacy_publication_pending => "PrivacyPublicationPending",
        .privacy_read_fence_unavailable => "PrivacyReadFenceUnavailable",
        .privacy_relational_storage_unsupported => "PrivacyRelationalStorageUnsupported",
        .privacy_schema_exceeds_window => "PrivacySchemaExceedsWindow",
        .privacy_stale => "PrivacyStale",
        .privacy_synchronous_ingestion_required => "PrivacySynchronousIngestionRequired",
        .privacy_unknown_artifact_source => "PrivacyUnknownArtifactSource",
        .privacy_unsupported_artifact_input => "PrivacyUnsupportedArtifactInput",
        .privacy_unsupported_index_type => "PrivacyUnsupportedIndexType",
        .privacy_unsupported_mutation => "PrivacyUnsupportedMutation",
        .privacy_unsupported_projection => "PrivacyUnsupportedProjection",
        .privacy_window_limit_exceeded => "PrivacyWindowLimitExceeded",
        .privacy_window_token_limit_exceeded => "PrivacyWindowTokenLimitExceeded",
        .unknown_privacy_category => "UnknownPrivacyCategory",
    };
}

test "schema epoch conflicts retain a retryable public status" {
    // main published this value before the relational-only tail. Preserve it
    // when both branches append details independently.
    try std.testing.expectEqual(@as(c_int, 297), @backingInt(Detail.unsupported_tensor_type));
    try std.testing.expectEqual(@as(c_int, 306), @backingInt(Detail.vector_store_reference_format_required));
    try std.testing.expectEqual(@as(c_int, 307), @backingInt(Detail.backup_outcome_ambiguous));
    try std.testing.expectEqual(@as(c_int, 308), @backingInt(Detail.metadata_mutation_not_applied));
    const value = statusFromError(error.SchemaInUse);
    try std.testing.expectEqual(@backingInt(Code.conflict), value.code);
    try std.testing.expectEqual(error.SchemaInUse, errorFromStatus(value));
}

test "transaction capacity rejection retains a permanent public status" {
    const value = statusFromError(error.TransactionTooLarge);
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), value.code);
    try std.testing.expectEqual(error.TransactionTooLarge, errorFromStatus(value));
}

test "stable status preserves deterministic raft rejection and malformed response identity" {
    inline for (.{ error.IntentConflict, error.VersionConflict, error.MergePageRequired }) |err| {
        const value = statusFromError(err);
        try std.testing.expectEqual(@backingInt(Code.conflict), value.code);
        try std.testing.expectEqual(err, errorFromStatus(value));
    }
    // A malformed response is not proof that a mutation failed to commit.
    // Preserve its identity without granting automatic retry authority.
    const malformed = statusFromError(error.InvalidResponse);
    try std.testing.expectEqualDeep(malformed, statusFromError(error.UnexpectedEof));
    try std.testing.expectEqual(@backingInt(Code.internal), malformed.code);
    try std.testing.expectEqual(error.InvalidResponse, errorFromStatus(malformed));
}

test "stable status preserves public boundary semantics" {
    for ([_]anyerror{ error.IndexRebuilding, error.IncompletePublishedSnapshot, error.DistributedQueryUnavailable, error.MetricNotReady, error.MetricStale }) |err| {
        const readiness = statusFromError(err);
        try std.testing.expectEqual(@backingInt(Code.retryable), readiness.code);
        try std.testing.expectEqual(err, errorFromStatus(readiness));
    }
    try std.testing.expectEqual(error.ReembedBudgetExceeded, errorFromStatus(statusFromError(error.ReembedBudgetExceeded)));
    try std.testing.expectEqual(error.ReembedSetupRetry, errorFromStatus(statusFromError(error.ReembedSetupRetry)));
    try std.testing.expectEqual(@backingInt(Code.unavailable), statusFromError(error.ReembedBudgetExceeded).code);
    try std.testing.expectEqual(error.ReadIndexTimeout, errorFromStatus(statusFromError(error.ReadIndexTimeout)));
    try std.testing.expectEqual(@backingInt(Code.timeout), statusFromError(error.ReadIndexTimeout).code);
    try std.testing.expectEqual(error.GenerationTransitionActive, errorFromStatus(statusFromError(error.GenerationTransitionActive)));
    try std.testing.expectEqual(@backingInt(Code.retryable), statusFromError(error.GenerationTransitionActive).code);
    try std.testing.expectEqual(error.IndexGenerationMismatch, errorFromStatus(statusFromError(error.IndexGenerationMismatch)));
    try std.testing.expectEqual(@backingInt(Code.retryable), statusFromError(error.IndexGenerationMismatch).code);
    try std.testing.expect(Status.ok.isOk());
    try std.testing.expectEqual(error.TableNotFound, errorFromStatus(statusFromError(error.TableNotFound)));
    try std.testing.expectEqual(error.TableVisibilityTimeout, errorFromStatus(statusFromError(error.TableVisibilityTimeout)));
    try std.testing.expectEqual(error.ReadIndexTimeout, errorFromStatus(statusFromError(error.ReadIndexTimeout)));
    try std.testing.expectEqual(error.CatalogRoutingSnapshotTimeout, errorFromStatus(statusFromError(error.CatalogRoutingSnapshotTimeout)));
    try std.testing.expectEqual(error.ExtensionOwnedObject, errorFromStatus(statusFromError(error.ExtensionOwnedObject)));
    try std.testing.expectEqual(error.ResourceRequestTooLarge, errorFromStatus(statusFromError(error.ResourceRequestTooLarge)));
    try std.testing.expectEqual(error.ResourceTemporarilyUnavailable, errorFromStatus(statusFromError(error.ResourceTemporarilyUnavailable)));
    try std.testing.expectEqual(error.ConcurrencyUnavailable, errorFromStatus(statusFromError(error.ConcurrencyUnavailable)));
    try std.testing.expectEqual(error.QueueFull, errorFromStatus(statusFromError(error.QueueFull)));
    try std.testing.expectEqual(error.ResourceLimitExceeded, errorFromStatus(statusFromError(error.ResourceLimitExceeded)));
    try std.testing.expectEqual(error.GraphQueryModeUnsupported, errorFromStatus(statusFromError(error.GraphQueryModeUnsupported)));
    try std.testing.expectEqual(error.InferenceProviderFailure, errorFromStatus(statusFromError(error.InferenceProviderFailure)));
    try std.testing.expectEqual(error.KernelJitRequiredDynamicLoad, errorFromStatus(statusFromError(error.KernelJitRequiredDynamicLoad)));
    try std.testing.expectEqual(error.UnexpectedToken, errorFromStatus(statusFromError(error.UnexpectedToken)));
    try std.testing.expectEqual(error.EnrichmentWaitCanceled, errorFromStatus(statusFromError(error.EnrichmentWaitCanceled)));
    try std.testing.expectEqual(error.EnrichmentWaitTimeout, errorFromStatus(statusFromError(error.EnrichmentWaitTimeout)));
    try std.testing.expectEqual(error.EnrichmentWorkerFailed, errorFromStatus(statusFromError(error.EnrichmentWorkerFailed)));
    try std.testing.expectEqual(error.DeadlineExceeded, errorFromStatus(statusFromError(error.DeadlineExceeded)));
    try std.testing.expectEqual(error.PreDecisionDeadlineExceeded, errorFromStatus(statusFromError(error.PreDecisionDeadlineExceeded)));
    try std.testing.expectEqual(error.MetadataMutationOutcomeUnknown, errorFromStatus(statusFromError(error.MetadataMutationOutcomeUnknown)));
    try std.testing.expectEqual(error.MetadataMutationNotApplied, errorFromStatus(statusFromError(error.MetadataMutationNotApplied)));
    try std.testing.expectEqual(error.InvalidEmbeddingArtifactProducer, errorFromStatus(statusFromError(error.InvalidEmbeddingArtifactProducer)));
    try std.testing.expectEqual(error.NativeBackupRepairStateNotQuiescent, errorFromStatus(statusFromError(error.NativeBackupRepairStateNotQuiescent)));
    try std.testing.expectEqual(error.NativeBackupProjectionNotQuiescent, errorFromStatus(statusFromError(error.NativeBackupProjectionNotQuiescent)));
    try std.testing.expectEqual(error.RerankerCandidateLimitExceeded, errorFromStatus(statusFromError(error.RerankerCandidateLimitExceeded)));
    try std.testing.expectEqual(error.RerankRateLimited, errorFromStatus(statusFromError(error.RerankRateLimited)));
    try std.testing.expectEqual(error.RerankTransientFailure, errorFromStatus(statusFromError(error.RerankTransientFailure)));
    try std.testing.expectEqual(error.RerankUpstreamFailure, errorFromStatus(statusFromError(error.RerankUpstreamFailure)));
    try std.testing.expectEqual(error.IncompatibleModel, errorFromStatus(statusFromError(error.IncompatibleModel)));
    try std.testing.expectEqual(error.UnsupportedGeneratorProvider, errorFromStatus(statusFromError(error.UnsupportedGeneratorProvider)));
    try std.testing.expectEqual(error.GenerateRequestFailed, errorFromStatus(statusFromError(error.GenerateRequestFailed)));
    try std.testing.expectEqual(error.RateLimit, errorFromStatus(statusFromError(error.RateLimit)));
    try std.testing.expectEqual(error.UnsupportedPlatform, errorFromStatus(statusFromError(error.UnsupportedPlatform)));
    try std.testing.expectEqual(error.UnsupportedTransformOperation, errorFromStatus(statusFromError(error.UnsupportedTransformOperation)));
    const schema_history_status = statusFromError(error.BackupSchemaHistoryTooLarge);
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), schema_history_status.code);
    try std.testing.expectEqual(@backingInt(Detail.backup_manifest_too_large), schema_history_status.detail);
    try std.testing.expectEqual(error.BackupManifestTooLarge, errorFromStatus(schema_history_status));
    try std.testing.expectEqual(error.HAReadRequiresPrimary, errorFromStatus(statusFromError(error.HAReadRequiresPrimary)));
    try std.testing.expectEqual(error.PersistentDescriptorAdmissionExhausted, errorFromStatus(statusFromError(error.PersistentDescriptorAdmissionExhausted)));
    try std.testing.expectEqual(error.CommitVisibilityNotSatisfied, errorFromStatus(statusFromError(error.CommitVisibilityNotSatisfied)));
    try std.testing.expectEqual(error.AbortDecisionNotDurable, errorFromStatus(statusFromError(error.AbortDecisionNotDurable)));
    try std.testing.expectEqual(error.LeaderUnavailable, errorFromStatus(statusFromError(error.LeaderUnavailable)));
    try std.testing.expectEqual(error.HASyncCommitWouldBlock, errorFromStatus(statusFromError(error.HASyncCommitWouldBlock)));
    try std.testing.expectEqual(error.HASyncCommitWaitLimitExceeded, errorFromStatus(statusFromError(error.HASyncCommitWaitLimitExceeded)));
    try std.testing.expectEqual(error.HASyncCommitWaitMissingContext, errorFromStatus(statusFromError(error.HASyncCommitWaitMissingContext)));
    try std.testing.expectEqual(error.HASyncCommitWaitStandbyNotInPolicy, errorFromStatus(statusFromError(error.HASyncCommitWaitStandbyNotInPolicy)));
    try std.testing.expectEqual(error.RuntimeBoundaryFailure, errorFromStatus(statusFromError(error.UnitPrivateError)));
}

test "stable status replaces private provider errors with a named fallback" {
    try std.testing.expectEqual(
        error.InferenceProviderFailure,
        errorFromStatus(statusFromErrorWithFallback(error.UnitPrivateInferenceError, error.InferenceProviderFailure)),
    );
    try std.testing.expectEqual(
        error.ResourceTemporarilyUnavailable,
        errorFromStatus(statusFromErrorWithFallback(error.ResourceTemporarilyUnavailable, error.InferenceProviderFailure)),
    );
}

test "stable detail detection distinguishes private errors" {
    try std.testing.expect(errorHasStableDetail(error.UnexpectedToken));
    try std.testing.expect(!errorHasStableDetail(error.UnitPrivateInferenceError));
}

test "every classified boundary outcome retains its identity" {
    @setEvalBranchQuota(@intCast(32 * (comptime std.meta.fieldNames(Detail)).len));
    const classified = comptime blk: {
        @setEvalBranchQuota(@typeInfo(Detail).@"enum".field_names.len * 8);
        const details = std.meta.tags(Detail)[1..];
        var errors: [details.len]anyerror = undefined;
        for (details, 0..) |detail, index| {
            errors[index] = @field(anyerror, detailErrorName(detail));
        }
        break :blk errors;
    };
    for (classified) |err| {
        try std.testing.expectEqual(err, errorFromStatus(statusFromError(err)));
    }
}

test "stable status has a C layout" {
    try std.testing.expectEqual(@sizeOf(c_int) * 2, @sizeOf(Status));
    try std.testing.expectEqual(@as(usize, 0), @offsetOf(Status, "code"));
    try std.testing.expectEqual(@sizeOf(c_int), @offsetOf(Status, "detail"));
}

test "scheduler admission detail appends without renumbering existing wire outcomes" {
    try std.testing.expectEqual(@as(c_int, 559), @backingInt(Detail.http_connection_closing));
    try std.testing.expectEqual(@as(c_int, 560), @backingInt(Detail.raft_batch_write_transport_outcome_unknown));
    try std.testing.expectEqual(@as(c_int, 561), @backingInt(Detail.concurrency_unavailable));
    const wire = statusFromError(error.ConcurrencyUnavailable);
    try std.testing.expectEqual(@backingInt(Code.retryable), wire.code);
    try std.testing.expectEqual(error.ConcurrencyUnavailable, errorFromStatus(wire));
}

test "unknown wire values fail closed" {
    try std.testing.expectEqual(error.RuntimeBoundaryFailure, errorFromStatus(.{ .code = 999, .detail = 999 }));
    try std.testing.expectEqual(error.RuntimeBoundaryFailure, errorFromStatus(.{ .code = @backingInt(Code.internal), .detail = 999 }));
    try std.testing.expectEqual(error.RuntimeBoundaryFailure, errorFromStatus(.{
        .code = @backingInt(Code.invalid_argument),
        .detail = @backingInt(Detail.table_not_found),
    }));
}

test "table vector storage errors retain their runtime boundary classification" {
    try std.testing.expectEqual(error.InvalidTableStorageSettings, errorFromStatus(statusFromError(error.InvalidTableStorageSettings)));
    try std.testing.expectEqual(error.VectorStoreRequiresLocalSingleShardTable, errorFromStatus(statusFromError(error.VectorStoreRequiresLocalSingleShardTable)));
    try std.testing.expectEqual(error.VectorStoreRequiresEmptyTable, errorFromStatus(statusFromError(error.VectorStoreRequiresEmptyTable)));
    try std.testing.expectEqual(error.ImmutableTableStorageSettings, errorFromStatus(statusFromError(error.ImmutableTableStorageSettings)));
    try std.testing.expectEqual(error.VectorStoreLifecycleUnsupported, errorFromStatus(statusFromError(error.VectorStoreLifecycleUnsupported)));
    try std.testing.expectEqual(error.VectorStoreReferenceFormatRequired, errorFromStatus(statusFromError(error.VectorStoreReferenceFormatRequired)));
}

test "provider quota errors retain stable boundary details" {
    try std.testing.expectEqual(error.InvalidRateLimitPolicy, errorFromStatus(statusFromError(error.InvalidRateLimitPolicy)));
    try std.testing.expectEqual(error.ConflictingRateLimitPolicy, errorFromStatus(statusFromError(error.ConflictingRateLimitPolicy)));
    try std.testing.expectEqual(error.ProviderTokenBudgetExceeded, errorFromStatus(statusFromError(error.ProviderTokenBudgetExceeded)));
    try std.testing.expectEqual(error.ProviderQuotaRegistryFull, errorFromStatus(statusFromError(error.ProviderQuotaRegistryFull)));
    try std.testing.expectEqual(error.UnsupportedMediaTokenBudget, errorFromStatus(statusFromError(error.UnsupportedMediaTokenBudget)));
    try std.testing.expectEqual(error.UnsupportedLocalRateLimit, errorFromStatus(statusFromError(error.UnsupportedLocalRateLimit)));
}

test "generation capacity retains retryability across the runtime boundary" {
    const result = statusFromError(error.GenerationCapacityUnavailable);
    try std.testing.expectEqual(@backingInt(Code.retryable), result.code);
    try std.testing.expectEqual(error.GenerationCapacityUnavailable, errorFromStatus(result));
    const malformed = statusFromError(error.InvalidGeneratedToolArguments);
    try std.testing.expectEqual(@backingInt(Code.retryable), malformed.code);
    try std.testing.expectEqual(error.InvalidGeneratedToolArguments, errorFromStatus(malformed));
}

test "system catalog errors retain their stable runtime boundary classification" {
    const unavailable = statusFromError(error.SettingAuthorityUnavailable);
    try std.testing.expectEqual(@backingInt(Code.unavailable), unavailable.code);
    try std.testing.expectEqual(error.SettingAuthorityUnavailable, errorFromStatus(unavailable));
    const errors = [_]anyerror{ error.TableTopologyProtocolUpgradeRequired, error.DatabaseNotFound, error.NamespaceNotFound, error.TablespaceNotFound, error.CatalogNotFound, error.CatalogAlreadyExists, error.CatalogGenerationChanged, error.TablespaceInUse, error.NamespaceNotEmpty, error.DatabaseNotEmpty, error.ProtectedCatalogResource, error.InvalidCatalogName, error.InvalidCatalogMutation, error.InvalidTablespaceLocation, error.InvalidTablespacePlacementPolicy, error.CatalogCommandTooLarge, error.InvalidCatalogRecord, error.CatalogIdExhausted };
    for (errors) |err| try std.testing.expectEqual(err, errorFromStatus(statusFromError(err)));
}

test "ambiguous backup outcome survives runtime transport without rollback authorization" {
    const wire = statusFromError(error.BackupOutcomeAmbiguous);
    try std.testing.expectEqual(@backingInt(Code.conflict), wire.code);
    try std.testing.expectEqual(error.BackupOutcomeAmbiguous, errorFromStatus(wire));
}

test "HA capture availability survives runtime callback transport" {
    inline for (.{ error.HASeedSnapshotRuntimeBusy, error.HASeedCaptureAlreadyInProgress, error.StorageKernelOwnerUnavailable }) |err| {
        try std.testing.expectEqual(err, errorFromStatus(statusFromError(err)));
    }
}

test "storage owner contention retains retryability and exact identity" {
    const wire = statusFromError(error.StorageBusy);
    try std.testing.expectEqual(@backingInt(Code.retryable), wire.code);
    try std.testing.expect(errorHasStableDetail(error.StorageBusy));
    try std.testing.expectEqual(error.StorageBusy, errorFromStatus(wire));
}

test "catalog publication preparation preserves explicit retry identity across archives" {
    const pending = statusFromError(error.CatalogPublicationProofPending);
    try std.testing.expectEqual(@backingInt(Code.retryable), pending.code);
    try std.testing.expectEqual(error.CatalogPublicationProofPending, errorFromStatus(pending));
    try std.testing.expect(pending.detail != statusFromError(error.ResourceTemporarilyUnavailable).detail);
    try std.testing.expect(pending.detail != statusFromError(error.InvalidCatalogRecord).detail);
    var forged = pending;
    forged.code = @backingInt(Code.corrupt);
    try std.testing.expectEqual(error.RuntimeBoundaryFailure, errorFromStatus(forged));
    inline for (.{ error.SqlIndexAlreadyExists, error.SqlConstraintAlreadyExists, error.ProgramLimitExceeded, error.CatalogOccupancyNotReady, error.UndefinedColumn, error.ForeignKeyValidationRequired }) |err| {
        const encoded = statusFromError(err);
        try std.testing.expect(errorHasStableDetail(err));
        try std.testing.expectEqual(err, errorFromStatus(encoded));
        var mismatched = encoded;
        mismatched.code = @backingInt(Code.corrupt);
        try std.testing.expectEqual(error.RuntimeBoundaryFailure, errorFromStatus(mismatched));
    }
}

test "released main Detail identifiers retain their exact names and numeric values" {
    @setEvalBranchQuota(20_000);
    const std_test = @import("std");
    var fingerprint: u64 = 14695981039346656037;
    var count: usize = 0;
    inline for (@typeInfo(Detail).@"enum".field_names, @typeInfo(Detail).@"enum".field_values) |reflected_name, field_value| {
        if (field_value <= 781) {
            for (reflected_name) |byte| fingerprint = (fingerprint ^ byte) *% 1099511628211;
            var encoded: [4]u8 = undefined;
            std_test.mem.writeInt(u32, &encoded, @intCast(field_value), .little);
            for (encoded) |byte| fingerprint = (fingerprint ^ byte) *% 1099511628211;
            count += 1;
        }
    }
    try std_test.testing.expectEqual(@as(usize, 782), count);
    try std_test.testing.expectEqual(@as(u64, 0xf7de08bbd998f30a), fingerprint);
}

test "metadata proposal recovery errors preserve identity across runtime archives" {
    for ([_]anyerror{ error.MetadataProposalSuperseded, error.MetadataProposalApplyTimeout, error.MetadataMutationOutcomeUnknown }) |err| {
        try std.testing.expect(errorHasStableDetail(err));
        try std.testing.expectEqual(err, errorFromStatus(statusFromErrorWithFallback(err, error.RestoreJobPersistenceUnavailable)));
    }
}

test "SQL snapshot and range protection errors preserve runtime capability semantics" {
    for ([_]anyerror{ error.SqlRangeTrackingRequired, error.SqlStatementSnapshotRequired, error.InvalidRangeTrackingState, error.RangeTrackingGenerationExhausted }) |err| {
        try std.testing.expect(errorHasStableDetail(err));
        try std.testing.expectEqual(err, errorFromStatus(statusFromError(err)));
    }
}

test "online merge admission barrier preserves exact identity across runtime archives" {
    const err = error.OnlineMergeArtifactCatalogUncoordinated;
    try std.testing.expect(errorHasStableDetail(err));
    try std.testing.expectEqual(err, errorFromStatus(statusFromError(err)));
}

test "ordered receipt corruption preserves the released runtime status" {
    const old = statusFromError(error.CorruptRaftAppliedEntry);
    const renamed = statusFromError(error.CorruptOrderedApplyReceipt);
    try std.testing.expectEqual(old, renamed);
    try std.testing.expectEqual(error.CorruptRaftAppliedEntry, errorFromStatus(renamed));
}

test "external lake and spill errors retain exact identity across runtime archives" {
    for ([_]anyerror{
        error.ExternalLakeReadOnly,
        error.ExternalLakeSnapshotMismatch,
        error.ExternalLakeSchemaMismatch,
        error.ExternalLakeSchemaUnavailable,
        error.InvalidExternalSourceInventory,
        error.InvalidExternalSourceSnapshot,
        error.ExternalSourceSnapshotTooLarge,
        error.ExternalLakeFieldIdentityChanged,
        error.ExternalLakeObjectChanged,
        error.EmptyExternalSourceSnapshot,
        error.UnsupportedExternalLakeSchemaType,
        error.InvalidParquetMetadata,
        error.InvalidParquetFooter,
        error.InvalidParquetFooterMagic,
        error.InvalidParquetPage,
        error.UnsupportedParquetPage,
        error.ParquetPageTooLarge,
        error.ParquetColumnNotFound,
        error.UnsupportedIcebergSchemaEvolution,
        error.UnsupportedIcebergDeleteFile,
        error.InvalidSqlSpill,
    }) |err| {
        try std.testing.expect(errorHasStableDetail(err));
        try std.testing.expectEqual(err, errorFromStatus(statusFromError(err)));
    }
}

test "Cypher errors retain stable callback details" {
    inline for (.{ error.CypherAnchorBudgetExceeded, error.CypherTransactionConflict, error.CypherMemoryBudgetExceeded, error.CypherObservationsUnavailable, error.SchemaVersionChanged, error.CypherWriteRequestAlreadyUsed, error.InvalidCypherParameters, error.MissingCypherParameter, error.SqlWriteCapacityUnavailable }) |err| {
        try std.testing.expect(errorHasStableDetail(err));
        try std.testing.expectEqual(err, errorFromStatus(statusFromError(err)));
    }
}

test "external lake evaluation limits retain their exact boundary identity" {
    const value = statusFromError(error.DecisionLimitExceeded);
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), value.code);
    try std.testing.expect(errorHasStableDetail(error.DecisionLimitExceeded));
    try std.testing.expectEqual(error.DecisionLimitExceeded, errorFromStatus(value));
}

test "inference command and lifecycle errors retain their identities across engine calls" {
    inline for (.{ error.WarmInferenceServerUnavailable, error.UnsupportedServerGenerateOption, error.MissingInferenceListener, error.RuntimeFailed, error.RuntimeStopping, error.RuntimeAlreadyReady }) |err| {
        try std.testing.expectEqual(err, errorFromStatus(statusFromError(err)));
    }
}

test "storage callback boundaries retain capability integrity and bounded-read outcomes" {
    for ([_]anyerror{ error.CrcMismatch, error.InvalidData, error.MappedArtifactUnavailable, error.ReadScopeResetUnsupported, error.ReadSnapshotForkUnsupported, error.MappedArtifactUnsupported, error.ImmutableReadSourceUnsupported, error.DirectoryListingUnsupported, error.UnsupportedStorageLease, error.DurableAtomicWriteUnsupported, error.DurableAtomicRenameUnsupported, error.DurableFileSyncUnsupported, error.DurableDirectorySyncUnsupported, error.StableFileLeaseUnsupported, error.ReadOnly, error.StorageClosed, error.CacheBudgetExceeded, error.BufferTooSmall, error.FileTooBig, error.InvalidScratch, error.InvalidAtomicWriteOffset, error.CanceledRangeRead }) |err| {
        const encoded = statusFromError(err);
        try std.testing.expect(errorHasStableDetail(err));
        try std.testing.expectEqual(err, errorFromStatus(encoded));
    }
}

test "temporary SQL statement capture failure preserves retryable boundary identity" {
    const transported = statusFromError(error.SqlStatementReadUnavailable);
    try std.testing.expectEqual(@backingInt(Code.retryable), transported.code);
    try std.testing.expectEqual(error.SqlStatementReadUnavailable, errorFromStatus(transported));
    try std.testing.expect(errorHasStableDetail(error.SqlStatementReadUnavailable));
}

test "provisioning races retain metadata identity and canonical protocol availability" {
    const metadata = statusFromError(error.MetadataSnapshotHeadMismatch);
    try std.testing.expectEqual(@backingInt(Code.retryable), metadata.code);
    try std.testing.expectEqual(error.MetadataSnapshotHeadMismatch, errorFromStatus(metadata));
    for ([_]anyerror{ error.RaftBatchMergeProtocolUnavailable, error.RaftBatchSplitDeltaProtocolUnavailable }) |err| {
        const wire = statusFromError(err);
        try std.testing.expectEqual(@backingInt(Code.unavailable), wire.code);
        try std.testing.expectEqual(error.Unavailable, errorFromStatus(wire));
    }
}

test "model-aware chunk failures preserve retry and source fencing semantics" {
    const Case = struct { err: anyerror, code: Code };
    for ([_]Case{
        .{ .err = error.StaleChunkContinuation, .code = .conflict },
        .{ .err = error.SourceVersionChanged, .code = .conflict },
        .{ .err = error.EmbeddingIdentityMismatch, .code = .conflict },
        .{ .err = error.InvalidEmbeddingIdentity, .code = .invalid_argument },
        .{ .err = error.IncompleteChunkCoverage, .code = .invalid_argument },
        .{ .err = error.ChunkOutputTooLarge, .code = .invalid_argument },
        .{ .err = error.MemoryBudgetExceeded, .code = .retryable },
        .{ .err = error.UnsupportedChunkMediaType, .code = .unsupported },
    }) |case| {
        const wire = statusFromError(case.err);
        try std.testing.expectEqual(@backingInt(case.code), wire.code);
        try std.testing.expectEqual(case.err, errorFromStatus(wire));
    }
}

test "inference worker setup and transport failures retain distinct boundary semantics" {
    const missing = statusFromError(error.InferenceWorkerExecutableNotConfigured);
    const unavailable = statusFromError(error.InferenceWorkerUnavailable);
    const capacity = statusFromError(error.ResourceTemporarilyUnavailable);
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), missing.code);
    try std.testing.expectEqual(@backingInt(Code.retryable), unavailable.code);
    try std.testing.expect(missing.detail != unavailable.detail and unavailable.detail != capacity.detail);
    for ([_]anyerror{ error.InferenceWorkerExecutableNotConfigured, error.InferenceWorkerUnavailable }) |err| {
        try std.testing.expectEqual(err, errorFromStatus(statusFromErrorWithFallback(err, error.InferenceProviderFailure)));
    }
}

/// Exact lookup for authenticated textual compatibility transports. Never
/// construct compilation-local errors from untrusted or arbitrary names.
pub fn errorFromStableName(name: []const u8) ?anyerror {
    const lookup = comptime blk: {
        @setEvalBranchQuota(1_000_000);
        const details = std.meta.tags(Detail);
        var entries: [details.len]struct { []const u8, anyerror } = undefined;
        for (details, 0..) |detail, index| {
            const spelling = detailErrorName(detail);
            entries[index] = .{ spelling, @field(anyerror, spelling) };
        }
        break :blk std.StaticStringMap(anyerror).initComptime(entries);
    };
    const err = lookup.get(name) orelse return null;
    return if (errorHasStableDetail(err)) err else null;
}

test "native normalization failures retain exact stable identities without accepting arbitrary names" {
    for ([_]anyerror{ error.RelationalCheckViolation, error.RowPolicyDenied, error.RowPolicyAuthenticationRequired, error.InvalidBatchRequest, error.UniqueConstraintViolation, error.OutOfMemory, error.DeadlineExceeded }) |err| {
        try std.testing.expectEqual(err, errorFromStatus(statusFromError(err)));
        try std.testing.expectEqual(err, errorFromStableName(@errorName(err)).?);
    }
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), statusFromError(error.RelationalCheckViolation).code);
    try std.testing.expectEqual(@backingInt(Code.forbidden), statusFromError(error.RowPolicyDenied).code);
    try std.testing.expectEqual(@backingInt(Code.unauthorized), statusFromError(error.RowPolicyAuthenticationRequired).code);
    try std.testing.expectEqual(@backingInt(Detail.partial_index_predicate_not_implied) + 1, @backingInt(Detail.relational_check_violation));
    for ([_][]const u8{ "RelationalCheckViolation\n", "relationalcheckviolation", "PrivateProviderSecret", "RuntimeBoundaryFailure" }) |name| try std.testing.expect(errorFromStableName(name) == null);
}

test "grouped and constrained producer failures preserve callback identity and behavior" {
    try std.testing.expectEqual(error.GroupedSourceOwnerChanged, errorFromStatus(statusFromError(error.GroupedSourceOwnerChanged)));
    try std.testing.expectEqual(@backingInt(Code.conflict), statusFromError(error.GroupedSourceOwnerChanged).code);
    try std.testing.expectEqual(error.GroupedSourceTopologyUnavailable, errorFromStatus(statusFromError(error.GroupedSourceTopologyUnavailable)));
    try std.testing.expectEqual(@backingInt(Code.unavailable), statusFromError(error.GroupedSourceTopologyUnavailable).code);
    try std.testing.expectEqual(error.AppleIntelligenceProviderUnavailable, errorFromStatus(statusFromError(error.AppleIntelligenceProviderUnavailable)));
    try std.testing.expectEqual(@backingInt(Code.unavailable), statusFromError(error.AppleIntelligenceProviderUnavailable).code);
    try std.testing.expectEqual(error.ArtifactPublicationPending, errorFromStatus(statusFromError(error.ArtifactPublicationPending)));
    try std.testing.expectEqual(@backingInt(Code.retryable), statusFromError(error.ArtifactPublicationPending).code);
    try std.testing.expectEqual(error.ChatGPTBillingFallbackForbidden, errorFromStatus(statusFromError(error.ChatGPTBillingFallbackForbidden)));
    try std.testing.expectEqual(@backingInt(Code.forbidden), statusFromError(error.ChatGPTBillingFallbackForbidden).code);
    try std.testing.expectEqual(error.ChatGPTDisabled, errorFromStatus(statusFromError(error.ChatGPTDisabled)));
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), statusFromError(error.ChatGPTDisabled).code);
    try std.testing.expectEqual(error.ChatGPTInteractiveOnly, errorFromStatus(statusFromError(error.ChatGPTInteractiveOnly)));
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), statusFromError(error.ChatGPTInteractiveOnly).code);
    try std.testing.expectEqual(error.ConstrainedInputTooLarge, errorFromStatus(statusFromError(error.ConstrainedInputTooLarge)));
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), statusFromError(error.ConstrainedInputTooLarge).code);
    try std.testing.expectEqual(error.DecideRequestLimitExceeded, errorFromStatus(statusFromError(error.DecideRequestLimitExceeded)));
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), statusFromError(error.DecideRequestLimitExceeded).code);
    try std.testing.expectEqual(error.EnrichmentSourceChanged, errorFromStatus(statusFromError(error.EnrichmentSourceChanged)));
    try std.testing.expectEqual(@backingInt(Code.conflict), statusFromError(error.EnrichmentSourceChanged).code);
    try std.testing.expectEqual(error.GeneratedExtractionLimitExceeded, errorFromStatus(statusFromError(error.GeneratedExtractionLimitExceeded)));
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), statusFromError(error.GeneratedExtractionLimitExceeded).code);
    try std.testing.expectEqual(error.GroupedPriorContextLimitExceeded, errorFromStatus(statusFromError(error.GroupedPriorContextLimitExceeded)));
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), statusFromError(error.GroupedPriorContextLimitExceeded).code);
    try std.testing.expectEqual(error.GroupedSourceBaselinePending, errorFromStatus(statusFromError(error.GroupedSourceBaselinePending)));
    try std.testing.expectEqual(@backingInt(Code.retryable), statusFromError(error.GroupedSourceBaselinePending).code);
    try std.testing.expectEqual(error.GroupedSourceBusy, errorFromStatus(statusFromError(error.GroupedSourceBusy)));
    try std.testing.expectEqual(@backingInt(Code.retryable), statusFromError(error.GroupedSourceBusy).code);
    try std.testing.expectEqual(error.GroupedSourceCorrupt, errorFromStatus(statusFromError(error.GroupedSourceCorrupt)));
    try std.testing.expectEqual(@backingInt(Code.corrupt), statusFromError(error.GroupedSourceCorrupt).code);
    try std.testing.expectEqual(error.GroupedSourceEarlierWorkPending, errorFromStatus(statusFromError(error.GroupedSourceEarlierWorkPending)));
    try std.testing.expectEqual(@backingInt(Code.retryable), statusFromError(error.GroupedSourceEarlierWorkPending).code);
    try std.testing.expectEqual(error.GroupedSourceIdentityConflict, errorFromStatus(statusFromError(error.GroupedSourceIdentityConflict)));
    try std.testing.expectEqual(@backingInt(Code.conflict), statusFromError(error.GroupedSourceIdentityConflict).code);
    try std.testing.expectEqual(error.GroupedSourceLimitExceeded, errorFromStatus(statusFromError(error.GroupedSourceLimitExceeded)));
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), statusFromError(error.GroupedSourceLimitExceeded).code);
    try std.testing.expectEqual(error.GroupedSourceMemberLimitExceeded, errorFromStatus(statusFromError(error.GroupedSourceMemberLimitExceeded)));
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), statusFromError(error.GroupedSourceMemberLimitExceeded).code);
    try std.testing.expectEqual(error.GroupedSourceMemberTooLarge, errorFromStatus(statusFromError(error.GroupedSourceMemberTooLarge)));
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), statusFromError(error.GroupedSourceMemberTooLarge).code);
    try std.testing.expectEqual(error.GroupedSourcePartitionTooLarge, errorFromStatus(statusFromError(error.GroupedSourcePartitionTooLarge)));
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), statusFromError(error.GroupedSourcePartitionTooLarge).code);
    try std.testing.expectEqual(error.GroupedSourceRetirementPending, errorFromStatus(statusFromError(error.GroupedSourceRetirementPending)));
    try std.testing.expectEqual(@backingInt(Code.retryable), statusFromError(error.GroupedSourceRetirementPending).code);
    try std.testing.expectEqual(error.InvalidConstrainedGenerationConfig, errorFromStatus(statusFromError(error.InvalidConstrainedGenerationConfig)));
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), statusFromError(error.InvalidConstrainedGenerationConfig).code);
    try std.testing.expectEqual(error.InvalidConstrainedOutput, errorFromStatus(statusFromError(error.InvalidConstrainedOutput)));
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), statusFromError(error.InvalidConstrainedOutput).code);
    try std.testing.expectEqual(error.InvalidConstrainedProbabilities, errorFromStatus(statusFromError(error.InvalidConstrainedProbabilities)));
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), statusFromError(error.InvalidConstrainedProbabilities).code);
    try std.testing.expectEqual(error.InvalidConstrainedSchema, errorFromStatus(statusFromError(error.InvalidConstrainedSchema)));
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), statusFromError(error.InvalidConstrainedSchema).code);
    try std.testing.expectEqual(error.InvalidDecideRequest, errorFromStatus(statusFromError(error.InvalidDecideRequest)));
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), statusFromError(error.InvalidDecideRequest).code);
    try std.testing.expectEqual(error.InvalidDecisionOutput, errorFromStatus(statusFromError(error.InvalidDecisionOutput)));
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), statusFromError(error.InvalidDecisionOutput).code);
    try std.testing.expectEqual(error.InvalidGeneratedExtractorConfig, errorFromStatus(statusFromError(error.InvalidGeneratedExtractorConfig)));
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), statusFromError(error.InvalidGeneratedExtractorConfig).code);
    try std.testing.expectEqual(error.InvalidGeneratorConfig, errorFromStatus(statusFromError(error.InvalidGeneratorConfig)));
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), statusFromError(error.InvalidGeneratorConfig).code);
    try std.testing.expectEqual(error.InvalidGeneratorToolConfig, errorFromStatus(statusFromError(error.InvalidGeneratorToolConfig)));
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), statusFromError(error.InvalidGeneratorToolConfig).code);
    try std.testing.expectEqual(error.InvalidGroupedPriorContext, errorFromStatus(statusFromError(error.InvalidGroupedPriorContext)));
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), statusFromError(error.InvalidGroupedPriorContext).code);
    try std.testing.expectEqual(error.InvalidGroupedSourceCommand, errorFromStatus(statusFromError(error.InvalidGroupedSourceCommand)));
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), statusFromError(error.InvalidGroupedSourceCommand).code);
    try std.testing.expectEqual(error.InvalidGroupedSourceConfig, errorFromStatus(statusFromError(error.InvalidGroupedSourceConfig)));
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), statusFromError(error.InvalidGroupedSourceConfig).code);
    try std.testing.expectEqual(error.InvalidGroupedSourceMember, errorFromStatus(statusFromError(error.InvalidGroupedSourceMember)));
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), statusFromError(error.InvalidGroupedSourceMember).code);
    try std.testing.expectEqual(error.InvalidGroupedSourceMutation, errorFromStatus(statusFromError(error.InvalidGroupedSourceMutation)));
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), statusFromError(error.InvalidGroupedSourceMutation).code);
    try std.testing.expectEqual(error.InvalidGroupedSourceQuery, errorFromStatus(statusFromError(error.InvalidGroupedSourceQuery)));
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), statusFromError(error.InvalidGroupedSourceQuery).code);
    try std.testing.expectEqual(error.InvalidGroupedSourceTransfer, errorFromStatus(statusFromError(error.InvalidGroupedSourceTransfer)));
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), statusFromError(error.InvalidGroupedSourceTransfer).code);
    try std.testing.expectEqual(error.InvalidStructuredOutput, errorFromStatus(statusFromError(error.InvalidStructuredOutput)));
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), statusFromError(error.InvalidStructuredOutput).code);
    try std.testing.expectEqual(error.MissingGeneratorCredentials, errorFromStatus(statusFromError(error.MissingGeneratorCredentials)));
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), statusFromError(error.MissingGeneratorCredentials).code);
    try std.testing.expectEqual(error.UnsupportedAppleGenerationOptions, errorFromStatus(statusFromError(error.UnsupportedAppleGenerationOptions)));
    try std.testing.expectEqual(@backingInt(Code.unsupported), statusFromError(error.UnsupportedAppleGenerationOptions).code);
    try std.testing.expectEqual(error.UnsupportedConstrainedOutput, errorFromStatus(statusFromError(error.UnsupportedConstrainedOutput)));
    try std.testing.expectEqual(@backingInt(Code.unsupported), statusFromError(error.UnsupportedConstrainedOutput).code);
    try std.testing.expectEqual(error.UnsupportedConstrainedProbabilities, errorFromStatus(statusFromError(error.UnsupportedConstrainedProbabilities)));
    try std.testing.expectEqual(@backingInt(Code.unsupported), statusFromError(error.UnsupportedConstrainedProbabilities).code);
    try std.testing.expectEqual(error.UnsupportedConstrainedSchema, errorFromStatus(statusFromError(error.UnsupportedConstrainedSchema)));
    try std.testing.expectEqual(@backingInt(Code.unsupported), statusFromError(error.UnsupportedConstrainedSchema).code);
    try std.testing.expectEqual(error.UnsupportedGeneratedExtractionSchema, errorFromStatus(statusFromError(error.UnsupportedGeneratedExtractionSchema)));
    try std.testing.expectEqual(@backingInt(Code.unsupported), statusFromError(error.UnsupportedGeneratedExtractionSchema).code);
}

test "released document reader and inference Detail identifiers retain main wire numbers" {
    const published = .{
        .{ Detail.document_storage_not_configured, @as(c_int, 782) },
        .{ Detail.document_run_busy, @as(c_int, 783) },
        .{ Detail.document_run_exists, @as(c_int, 784) },
        .{ Detail.document_run_expired, @as(c_int, 785) },
        .{ Detail.document_run_lease_lost, @as(c_int, 786) },
        .{ Detail.document_run_not_found, @as(c_int, 787) },
        .{ Detail.document_run_retention_active, @as(c_int, 788) },
        .{ Detail.document_run_terminal, @as(c_int, 789) },
        .{ Detail.document_run_store_needs_recovery, @as(c_int, 790) },
        .{ Detail.document_model_revision_mismatch, @as(c_int, 791) },
        .{ Detail.document_budget_exhausted, @as(c_int, 792) },
        .{ Detail.document_output_too_large, @as(c_int, 793) },
        .{ Detail.document_stage_in_flight, @as(c_int, 794) },
        .{ Detail.invalid_document_checkpoint, @as(c_int, 795) },
        .{ Detail.too_many_active_jobs, @as(c_int, 796) },
        .{ Detail.invalid_document_source, @as(c_int, 797) },
        .{ Detail.unsupported_document_format, @as(c_int, 798) },
        .{ Detail.document_source_limit_exceeded, @as(c_int, 799) },
        .{ Detail.document_page_limit_exceeded, @as(c_int, 800) },
        .{ Detail.document_pixel_limit_exceeded, @as(c_int, 801) },
        .{ Detail.document_media_type_mismatch, @as(c_int, 802) },
        .{ Detail.invalid_document_selection, @as(c_int, 803) },
        .{ Detail.duplicate_document_source, @as(c_int, 804) },
        .{ Detail.document_unresolved, @as(c_int, 805) },
        .{ Detail.document_capability_mismatch, @as(c_int, 806) },
        .{ Detail.unsupported_document_request_field, @as(c_int, 807) },
        .{ Detail.remote_document_model_denied, @as(c_int, 808) },
        .{ Detail.cloudflare_credential_missing, @as(c_int, 809) },
        .{ Detail.invalid_cloudflare_config, @as(c_int, 810) },
        .{ Detail.unsupported_cloudflare_model, @as(c_int, 811) },
        .{ Detail.unsupported_cloudflare_schema, @as(c_int, 812) },
        .{ Detail.cloudflare_authentication_failed, @as(c_int, 813) },
        .{ Detail.invalid_extractor_response, @as(c_int, 814) },
        .{ Detail.unsupported_extraction_schema_version, @as(c_int, 815) },
        .{ Detail.invalid_inference_request, @as(c_int, 816) },
        .{ Detail.document_artifact_not_found, @as(c_int, 817) },
        .{ Detail.document_artifact_expired, @as(c_int, 818) },
        .{ Detail.cloudflare_rate_limited, @as(c_int, 819) },
        .{ Detail.inference_upstream_rate_limited, @as(c_int, 820) },
        .{ Detail.inference_capacity_unavailable, @as(c_int, 821) },
        .{ Detail.inference_transport_unavailable, @as(c_int, 822) },
        .{ Detail.cloudflare_inference_failed, @as(c_int, 823) },
        .{ Detail.invalid_cloudflare_response, @as(c_int, 824) },
        .{ Detail.document_token_limit_exceeded, @as(c_int, 825) },
        .{ Detail.hierarchy_cursor_stale, @as(c_int, 826) },
        .{ Detail.read_transient_failure, @as(c_int, 827) },
        .{ Detail.read_request_failed, @as(c_int, 828) },
        .{ Detail.invalid_gcs_url, @as(c_int, 829) },
        .{ Detail.gcs_connection_not_found_or_out_of_scope, @as(c_int, 830) },
        .{ Detail.ambiguous_gcs_connection, @as(c_int, 831) },
        .{ Detail.invalid_connection_credentials, @as(c_int, 832) },
        .{ Detail.pioneer_credential_missing, @as(c_int, 833) },
        .{ Detail.unsupported_pioneer_schema, @as(c_int, 834) },
        .{ Detail.unsupported_pioneer_input, @as(c_int, 835) },
        .{ Detail.standalone_replication_sources_unsupported, @as(c_int, 836) },
        .{ Detail.ha_catalog_replication_sources_unsupported, @as(c_int, 837) },
        .{ Detail.embed_admission_refused, @as(c_int, 838) },
        .{ Detail.read_output_truncated, @as(c_int, 839) },
        .{ Detail.invalid_document_extraction_config, @as(c_int, 840) },
        .{ Detail.document_extraction_model_required, @as(c_int, 841) },
    };
    inline for (published) |entry| try std.testing.expectEqual(entry[1], @backingInt(entry[0]));
    try std.testing.expect(@backingInt(Detail.sql_index_already_exists) > 841);
}

test "merged runtime error details preserve main wire identifiers" {
    try std.testing.expectEqual(@as(c_int, 674), @backingInt(Detail.inference_worker_executable_not_configured));
    try std.testing.expectEqual(@as(c_int, 675), @backingInt(Detail.inference_worker_unavailable));
    try std.testing.expectEqual(@as(c_int, 676), @backingInt(Detail.model_chunk_audio_too_large));
    try std.testing.expectEqual(@as(c_int, 729), @backingInt(Detail.decision_limit_exceeded));
    try std.testing.expectEqual(@as(c_int, 778), @backingInt(Detail.invalid_embedding_identity));
    try std.testing.expectEqual(@as(c_int, 779), @backingInt(Detail.graph_start_identity_required));
    try std.testing.expectEqual(@as(c_int, 780), @backingInt(Detail.metadata_snapshot_head_mismatch));
    try std.testing.expectEqual(@as(c_int, 781), @backingInt(Detail.inference_endpoint_access_denied));
    try std.testing.expectEqual(@as(c_int, 782), @backingInt(Detail.document_storage_not_configured));
    try std.testing.expectEqual(@as(c_int, 825), @backingInt(Detail.document_token_limit_exceeded));
    try std.testing.expectEqual(@as(c_int, 826), @backingInt(Detail.hierarchy_cursor_stale));
    try std.testing.expectEqual(@as(c_int, 827), @backingInt(Detail.read_transient_failure));
    try std.testing.expectEqual(@as(c_int, 832), @backingInt(Detail.invalid_connection_credentials));
    try std.testing.expectEqual(@as(c_int, 833), @backingInt(Detail.pioneer_credential_missing));
    try std.testing.expectEqual(@as(c_int, 835), @backingInt(Detail.unsupported_pioneer_input));
    try std.testing.expectEqual(@as(c_int, 836), @backingInt(Detail.standalone_replication_sources_unsupported));
    try std.testing.expectEqual(@as(c_int, 837), @backingInt(Detail.ha_catalog_replication_sources_unsupported));
    try std.testing.expectEqual(@as(c_int, 838), @backingInt(Detail.embed_admission_refused));
    try std.testing.expectEqual(@as(c_int, 839), @backingInt(Detail.read_output_truncated));
    try std.testing.expectEqual(@as(c_int, 840), @backingInt(Detail.invalid_document_extraction_config));
    try std.testing.expectEqual(@as(c_int, 841), @backingInt(Detail.document_extraction_model_required));
    try std.testing.expectEqual(@as(c_int, 891), @backingInt(Detail.cypher_aggregate_scope));
}

test "logical query work and scan policy failures retain their identities" {
    for ([_]anyerror{ error.ValueSearchRequiresScan, error.ValueSearchLimitExceeded }) |err| {
        try std.testing.expectEqual(err, errorFromStatus(statusFromError(err)));
    }
}

test "resolver namespace rejection retains its conflict identity across runtime callbacks" {
    const value = statusFromError(error.ResolverCurationConflict);
    try std.testing.expectEqual(@backingInt(Code.conflict), value.code);
    try std.testing.expectEqual(error.ResolverCurationConflict, errorFromStatus(value));
}

test "graph start identity requirement retains its semantic identity" {
    const value = statusFromError(error.GraphStartIdentityRequired);
    try std.testing.expectEqual(@backingInt(Code.unsupported), value.code);
    try std.testing.expectEqual(error.GraphStartIdentityRequired, errorFromStatus(value));
}

test "durable document policy errors retain identity across native runtime boundaries" {
    for ([_]anyerror{
        error.DocumentStorageNotConfigured,
        error.DocumentModelRevisionMismatch,
        error.DocumentBudgetExhausted,
        error.UnsupportedMediaTokenBudget,
        error.InvalidDocumentCheckpoint,
        error.InvalidDocumentSource,
        error.UnsupportedDocumentFormat,
        error.DocumentSourceLimitExceeded,
        error.DocumentOutputTooLarge,
        error.DocumentPageLimitExceeded,
        error.DocumentPixelLimitExceeded,
        error.DocumentTokenLimitExceeded,
        error.DocumentMediaTypeMismatch,
        error.InvalidDocumentSelection,
        error.DuplicateDocumentSource,
        error.DocumentUnresolved,
        error.DocumentCapabilityMismatch,
        error.UnsupportedDocumentRequestField,
        error.ModelNotFound,
        error.RemoteDocumentModelDenied,
        error.CloudflareCredentialMissing,
        error.InvalidCloudflareConfig,
        error.UnsupportedCloudflareModel,
        error.UnsupportedCloudflareSchema,
        error.PioneerCredentialMissing,
        error.UnsupportedPioneerSchema,
        error.UnsupportedPioneerInput,
        error.StandaloneReplicationSourcesUnsupported,
        error.HACatalogReplicationSourcesUnsupported,
        error.CloudflareAuthenticationFailed,
        error.InvalidExtractorResponse,
        error.UnsupportedExtractionSchemaVersion,
        error.InvalidInferenceRequest,
        error.InferenceEndpointAccessDenied,
        error.DocumentArtifactNotFound,
        error.DocumentArtifactExpired,
        error.CloudflareRateLimited,
        error.InferenceUpstreamRateLimited,
        error.InferenceCapacityUnavailable,
        error.InferenceTransportUnavailable,
        error.DocumentRunLeaseLost,
        error.DocumentRunStoreNeedsRecovery,
        error.DocumentRunBusy,
        error.DocumentStageInFlight,
        error.Timeout,
        error.ResourceTemporarilyUnavailable,
        error.CloudflareInferenceFailed,
        error.InvalidCloudflareResponse,
        error.OutOfMemory,
    }) |err| {
        try std.testing.expectEqual(err, errorFromStatus(statusFromError(err)));
    }
}

test "materialized view mutation rejection retains exact runtime boundary identity" {
    const result = statusFromError(error.MaterializedViewReadOnly);
    try std.testing.expectEqual(@backingInt(Code.forbidden), result.code);
    try std.testing.expectEqual(error.MaterializedViewReadOnly, errorFromStatus(result));
    try std.testing.expectEqual(error.MaterializedViewReadOnly, errorFromStableName("MaterializedViewReadOnly").?);
}

test "reader failures preserve retry semantics and GCS failures preserve scope semantics" {
    try std.testing.expectEqual(@backingInt(Code.retryable), statusFromError(error.ReadTransientFailure).code);
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), statusFromError(error.ReadRequestFailed).code);
    try std.testing.expectEqual(@backingInt(Code.forbidden), statusFromError(error.GcsConnectionNotFoundOrOutOfScope).code);
    inline for (.{ error.ReadTransientFailure, error.ReadRequestFailed, error.InvalidGcsUrl, error.GcsConnectionNotFoundOrOutOfScope, error.AmbiguousGcsConnection, error.InvalidConnectionCredentials }) |expected| {
        try std.testing.expectEqual(expected, errorFromStatus(statusFromError(expected)));
    }
}

test "invalid persisted extraction policy retains identity across storage callbacks" {
    inline for (.{ error.InvalidDocumentExtractionConfig, error.DocumentExtractionModelRequired }) |expected| {
        const transported = comptime statusFromError(expected);
        try std.testing.expectEqual(@backingInt(Code.invalid_argument), transported.code);
        try std.testing.expectEqual(expected, errorFromStatus(transported));
    }
}

test "native locking callback admission preserves retryable ordered apply identity" {
    const transported = statusFromError(error.RaftApplyWriterUnavailable);
    try std.testing.expectEqual(@backingInt(Code.retryable), transported.code);
    try std.testing.expectEqual(error.RaftApplyWriterUnavailable, errorFromStatus(transported));
    try std.testing.expectEqual(@as(c_int, 1024), @backingInt(Detail.raft_apply_writer_unavailable));
}

test "scope retirement callback descriptors preserve exact lifecycle conflict and limit identities" {
    const cases = .{
        .{ error.SqlDatabaseInUse, Code.conflict, Detail.sql_database_in_use },
        .{ error.InvalidScopeRetirement, Code.invalid_argument, Detail.invalid_scope_retirement },
        .{ error.ScopeRetirementChanged, Code.conflict, Detail.scope_retirement_changed },
        .{ error.SequenceAdmissionConflict, Code.conflict, Detail.sequence_admission_conflict },
        .{ error.InvalidTableRetirementManifest, Code.invalid_argument, Detail.invalid_table_retirement_manifest },
        .{ error.SqlProgramLimitExceeded, Code.invalid_argument, Detail.sql_program_limit_exceeded },
        .{ error.SnapshotBuildCancelled, Code.cancelled, Detail.snapshot_build_cancelled },
        .{ error.CatalogGenerationExhausted, Code.invalid_argument, Detail.catalog_generation_exhausted },
    };
    inline for (cases) |entry| {
        const transported = statusFromError(entry[0]);
        try std.testing.expectEqual(@backingInt(entry[1]), transported.code);
        try std.testing.expectEqual(@backingInt(entry[2]), transported.detail);
        try std.testing.expectEqual(entry[0], errorFromStatus(transported));
        try std.testing.expectEqual(entry[0], errorFromStableName(@errorName(entry[0])).?);
        try std.testing.expect(errorHasStableDetail(entry[0]));
    }
    // Public enum numbers remain independent from the embedded owner ABI.
    try std.testing.expectEqual(@as(c_int, 1024), @backingInt(Detail.raft_apply_writer_unavailable));
    try std.testing.expectEqual(@as(c_int, 1025), @backingInt(Detail.sql_database_in_use));
    try std.testing.expectEqual(@as(c_int, 1032), @backingInt(Detail.catalog_generation_exhausted));
    try std.testing.expect(errorFromStableName("SqlDatabaseInUse\n") == null);
    try std.testing.expect(errorFromStableName("ScopeRetirementChangedPrivate") == null);
}

test "generation graph and durability errors survive an actual C callback status boundary without retry promotion" {
    const Callback = *const fn (*anyopaque) callconv(.c) Status;
    const State = struct {
        failure: anyerror,
        fn invoke(raw: *anyopaque) callconv(.c) Status {
            const state: *@This() = @ptrCast(@alignCast(raw));
            return statusFromError(state.failure);
        }
    };
    const callback: Callback = State.invoke;
    const failures = [_]anyerror{ error.InvalidGenerationTransaction, error.GenerationTransactionChanged, error.GenerationTransactionDecisionRequired, error.InvalidPreparedGraphEffects, error.SqlPreparedDurabilityUnavailable };
    const codes = [_]Code{ .invalid_argument, .conflict, .conflict, .invalid_argument, .invalid_argument };
    for (failures, codes, 1048..) |failure, code, detail| {
        var state: State = .{ .failure = failure };
        const transported = callback(&state);
        try std.testing.expectEqual(@backingInt(code), transported.code);
        try std.testing.expectEqual(@as(c_int, @intCast(detail)), transported.detail);
        try std.testing.expectEqual(failure, errorFromStatus(transported));
        try std.testing.expect(transported.code != @backingInt(Code.retryable));
    }
}

test "privacy failures retain identity and retry classes across runtime callbacks" {
    for ([_]anyerror{ error.IndexRebuilding, error.SqlStatementReadUnavailable, error.UnsupportedSqlExecution }) |err| {
        try std.testing.expect(errorHasStableDetail(err));
        try std.testing.expectEqual(err, errorFromStatus(statusFromError(err)));
    }
    for ([_]anyerror{ error.ConflictingPrivacyProducerOptions, error.ConflictingPrivacyProducerSchema, error.InvalidPrivacyCatalog, error.InvalidPrivacyCategories, error.InvalidPrivacyCategory, error.InvalidPrivacyConfidence, error.InvalidPrivacyDocument, error.InvalidPrivacyExecution, error.InvalidPrivacyFieldType, error.InvalidPrivacyFields, error.InvalidPrivacyOffsetUnit, error.InvalidPrivacyOutput, error.InvalidPrivacyOutputCardinality, error.InvalidPrivacyProducer, error.InvalidPrivacyReceipt, error.InvalidPrivacyScan, error.InvalidPrivacySource, error.InvalidPrivacySpan, error.InvalidPrivacyThreshold, error.InvalidPrivacyTokenGeometry, error.InvalidPrivacyWindowReceipt, error.PrivacyEmbeddingProducerRequired, error.PrivacyEvidenceMismatch, error.PrivacyExternalEmbeddingsUnsupported, error.PrivacyExternalVectorsUnsupported, error.PrivacyInspectionFailed, error.PrivacyLiteralCredential, error.PrivacyManagedGraph, error.PrivacyPending, error.PrivacyPreconditionFailed, error.PrivacyPreconditionRequired, error.PrivacyProducerRevisionMismatch, error.PrivacyProducerRevisionRequired, error.PrivacyPublicationPending, error.PrivacyReadFenceUnavailable, error.PrivacyRelationalStorageUnsupported, error.PrivacySchemaExceedsWindow, error.PrivacyStale, error.PrivacySynchronousIngestionRequired, error.PrivacyUnknownArtifactSource, error.PrivacyUnsupportedArtifactInput, error.PrivacyUnsupportedIndexType, error.PrivacyUnsupportedMutation, error.PrivacyUnsupportedProjection, error.PrivacyWindowLimitExceeded, error.PrivacyWindowTokenLimitExceeded, error.UnknownPrivacyCategory }) |err| {
        try std.testing.expect(errorHasStableDetail(err));
        try std.testing.expectEqual(err, errorFromStatus(statusFromError(err)));
    }
    try std.testing.expectEqual(@backingInt(Code.retryable), statusFromError(error.PrivacyStale).code);
    try std.testing.expectEqual(@backingInt(Code.corrupt), statusFromError(error.InvalidPrivacyReceipt).code);
}

test "external terminal membership failure crosses callbacks without retry promotion" {
    const Callback = *const fn () callconv(.c) Status;
    const Producer = struct {
        fn call() callconv(.c) Status {
            return statusFromError(error.InvalidParticipant);
        }
    };
    const callback: Callback = Producer.call;
    const result = callback();
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), result.code);
    try std.testing.expectEqual(@backingInt(Detail.invalid_participant), result.detail);
    try std.testing.expectEqual(error.InvalidParticipant, errorFromStatus(result));
    try std.testing.expectEqual(error.InvalidParticipant, errorFromStableName("InvalidParticipant").?);
    try std.testing.expect(errorHasStableDetail(error.InvalidParticipant));
}

test "SQL extension lifecycle callback identities retain exact behavior" {
    const errors = [_]anyerror{ error.CatalogRevisionExhausted, error.DuplicateSqlExtensionExport, error.DuplicateSqlExtensionVersion, error.InconsistentSqlExtensionControl, error.InvalidPackageDigest, error.InvalidSqlExtensionArtifact, error.InvalidSqlExtensionControl, error.InvalidSqlExtensionDependency, error.InvalidSqlExtensionRecord, error.InvalidSqlExtensionRuntime, error.InvalidSqlExtensionPackageMutation, error.DuplicateSqlExtensionControl, error.SqlDependentExtensionObject, error.SqlDuplicateExtension, error.SqlDuplicateFunction, error.SqlExtensionControlConflict, error.SqlExtensionDependencyCycle, error.SqlExtensionPromotionNonceConflict, error.SqlExtensionSchemaMismatch, error.SqlExtensionUnavailable, error.SqlExtensionUpdatePathUnavailable, error.SqlExtensionVersionUnavailable, error.SqlFunctionReturnTypeChanged, error.SqlRequiredExtensionNotInstalled, error.SqlUndefinedExtension, error.UnsupportedSqlExtensionObject, error.SqlExtensionOwnedObject, error.DuplicateShapeName, error.GeneratedArtifactShapeRequired, error.NoSupportedScopes, error.ScopeMustNotNameTable, error.UnknownShapeReference };
    const codes = [_]Code{ .invalid_argument, .invalid_argument, .invalid_argument, .corrupt, .corrupt, .unsupported, .invalid_argument, .invalid_argument, .corrupt, .unsupported, .invalid_argument, .invalid_argument, .conflict, .already_exists, .already_exists, .conflict, .invalid_argument, .conflict, .invalid_argument, .unsupported, .invalid_argument, .not_found, .invalid_argument, .not_found, .not_found, .unsupported, .conflict, .invalid_argument, .invalid_argument, .invalid_argument, .invalid_argument, .invalid_argument };
    for (errors, codes, 1054..) |err, code, ordinal| {
        const wire = statusFromError(err);
        try std.testing.expectEqual(@backingInt(code), wire.code);
        try std.testing.expectEqual(@as(c_int, @intCast(ordinal)), wire.detail);
        try std.testing.expectEqual(err, errorFromStatus(wire));
    }
    for ([_]anyerror{ error.UnsupportedManifestApiVersion, error.UnsupportedPackageKind }) |err| {
        try std.testing.expectEqual(err, errorFromStatus(statusFromError(err)));
    }
}

test "merged runtime error details preserve the common prefix under ABI two" {
    try std.testing.expectEqual(@as(u32, 2), abi_version);
    try std.testing.expectEqual(@as(c_int, 674), @backingInt(Detail.inference_worker_executable_not_configured));
    try std.testing.expectEqual(@as(c_int, 675), @backingInt(Detail.inference_worker_unavailable));
    try std.testing.expectEqual(@as(c_int, 676), @backingInt(Detail.model_chunk_audio_too_large));
    try std.testing.expectEqual(@as(c_int, 729), @backingInt(Detail.decision_limit_exceeded));
    try std.testing.expectEqual(@as(c_int, 778), @backingInt(Detail.invalid_embedding_identity));
    try std.testing.expectEqual(@as(c_int, 779), @backingInt(Detail.graph_start_identity_required));
    try std.testing.expectEqual(@as(c_int, 780), @backingInt(Detail.metadata_snapshot_head_mismatch));
    try std.testing.expectEqual(@as(c_int, 781), @backingInt(Detail.inference_endpoint_access_denied));
    try std.testing.expectEqual(@as(c_int, 782), @backingInt(Detail.document_storage_not_configured));
    try std.testing.expectEqual(@as(c_int, 825), @backingInt(Detail.document_token_limit_exceeded));
    try std.testing.expectEqual(@as(c_int, 826), @backingInt(Detail.hierarchy_cursor_stale));
    try std.testing.expectEqual(@as(c_int, 827), @backingInt(Detail.read_transient_failure));
    try std.testing.expectEqual(@as(c_int, 832), @backingInt(Detail.invalid_connection_credentials));
    try std.testing.expectEqual(@as(c_int, 833), @backingInt(Detail.pioneer_credential_missing));
    try std.testing.expectEqual(@as(c_int, 835), @backingInt(Detail.unsupported_pioneer_input));
    try std.testing.expectEqual(@as(c_int, 836), @backingInt(Detail.standalone_replication_sources_unsupported));
    try std.testing.expectEqual(@as(c_int, 837), @backingInt(Detail.ha_catalog_replication_sources_unsupported));
    try std.testing.expectEqual(@as(c_int, 838), @backingInt(Detail.embed_admission_refused));
    try std.testing.expectEqual(@as(c_int, 839), @backingInt(Detail.read_output_truncated));
    try std.testing.expectEqual(@as(c_int, 840), @backingInt(Detail.invalid_document_extraction_config));
    try std.testing.expectEqual(@as(c_int, 841), @backingInt(Detail.document_extraction_model_required));
    try std.testing.expectEqual(@as(c_int, 891), @backingInt(Detail.cypher_aggregate_scope));
}

test "manifest client failures append stable details without changing released ordinals" {
    try std.testing.expectEqual(@backingInt(Detail.sql_write_capacity_unavailable) + 1, @backingInt(Detail.invalid_external_source_inventory));
    for ([_]anyerror{ error.InvalidExternalSourceInventory, error.InvalidExternalSourceSnapshot, error.ExternalSourceSnapshotTooLarge }) |err| {
        const transported = statusFromError(err);
        try std.testing.expectEqual(@backingInt(Code.invalid_argument), transported.code);
        try std.testing.expectEqual(err, errorFromStatus(transported));
    }
}

test "latest main error detail prefix remains append only across graph integration" {
    @setEvalBranchQuota(30000);
    var hash: u64 = 14695981039346656037;
    var count: usize = 0;
    inline for (@typeInfo(Detail).@"enum".field_names, @typeInfo(Detail).@"enum".field_values) |name, value| {
        if (value <= 890) {
            for (name) |byte| hash = (hash ^ byte) *% 1099511628211;
            var encoded: [4]u8 = undefined;
            std.mem.writeInt(u32, &encoded, @intCast(value), .little);
            for (encoded) |byte| hash = (hash ^ byte) *% 1099511628211;
            count += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 891), count);
    try std.testing.expectEqual(@as(u64, 16819601552131841023), hash);
}

test "published main Detail inventory remains stable after SQL integration" {
    @setEvalBranchQuota(100000);
    var hash: u64 = 14695981039346656037;
    var count: usize = 0;
    inline for (@typeInfo(Detail).@"enum".field_names, @typeInfo(Detail).@"enum".field_values) |field_name, value| {
        if (value <= 972) {
            for (field_name) |byte| hash = (hash ^ byte) *% 1099511628211;
            var bytes: [4]u8 = undefined;
            std.mem.writeInt(u32, &bytes, @intCast(value), .little);
            for (bytes) |byte| hash = (hash ^ byte) *% 1099511628211;
            count += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 973), count);
    try std.testing.expectEqual(@as(u64, 10976950832745621870), hash);
}

test "time series aggregation callback failures use existing query identities" {
    const invalid = statusFromError(error.InvalidAggregation);
    try std.testing.expectEqualDeep(statusFromError(error.InvalidQueryRequest), invalid);
    try std.testing.expectEqual(error.InvalidQueryRequest, errorFromStatus(invalid));
    try std.testing.expect(errorHasStableDetail(error.InvalidAggregation));
    const unsupported = statusFromError(error.UnsupportedAggregation);
    try std.testing.expectEqualDeep(statusFromError(error.UnsupportedQueryRequest), unsupported);
    try std.testing.expectEqual(error.UnsupportedQueryRequest, errorFromStatus(unsupported));
    try std.testing.expect(errorHasStableDetail(error.UnsupportedAggregation));
}

test "storage owner lease admission retains retryability across runtime callbacks" {
    const wire = statusFromError(error.StorageOwnerAdmissionPending);
    try std.testing.expectEqual(@backingInt(Code.retryable), wire.code);
    try std.testing.expect(errorHasStableDetail(error.StorageOwnerAdmissionPending));
    try std.testing.expectEqual(error.StorageOwnerAdmissionPending, errorFromStatus(wire));
}

test "unsupported graph node filter survives the runtime boundary as a client error" {
    const wire = statusFromError(error.UnsupportedNodeFilterQuery);
    try std.testing.expectEqual(@backingInt(Code.invalid_argument), wire.code);
    try std.testing.expectEqual(error.UnsupportedNodeFilterQuery, errorFromStatus(wire));
    try std.testing.expect(errorHasStableDetail(error.UnsupportedNodeFilterQuery));
}

test "backup catalog conflicts retain their identity across runtime archives" {
    const wire = statusFromError(error.CatalogChanged);
    try std.testing.expectEqual(@backingInt(Code.conflict), wire.code);
    try std.testing.expectEqual(error.CatalogChanged, errorFromStatus(wire));
    try std.testing.expect(errorHasStableDetail(error.CatalogChanged));
}

test "descriptor exhaustion retains retryable public runtime identity" {
    inline for (.{ error.ProcessFdQuotaExceeded, error.SystemFdQuotaExceeded, error.StorageDescriptorPressure }) |err| {
        const value = statusFromError(err);
        try std.testing.expectEqual(@backingInt(Code.retryable), value.code);
        try std.testing.expectEqual(err, errorFromStatus(value));
    }
}

test "positional IO errors preserve identity without becoming retryable pressure" {
    const value = statusFromError(error.InputOutput);
    try std.testing.expectEqual(@backingInt(Code.internal), value.code);
    try std.testing.expectEqual(error.InputOutput, errorFromStatus(value));
}

test "index dependency conflicts preserve identity across owner callbacks" {
    for ([_]anyerror{ error.IndexReferencedByAlias, error.IndexAliasGraphConflict, error.IndexAliasPreconditionRequired, error.IndexAliasPreconditionFailed }) |err| {
        const result = statusFromError(err);
        try std.testing.expectEqual(@backingInt(Code.conflict), result.code);
        try std.testing.expectEqual(err, errorFromStatus(result));
    }
}
