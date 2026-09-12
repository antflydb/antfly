// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Compiled owner of the storage_kernel runtime entry points.

pub const antfly_sources = @import("source_owner_storage.zig");

const std = @import("std");

const bridge = @import("runtime_bridge.zig");

const process = @import("runtime_process.zig");

const runtimeEntry = process.runtimeEntry;

const exportInternal = process.exportInternal;

pub const runtime_impl = @import("capi_root.zig");

pub const storage_backend_erased = runtime_impl.storage_backend_erased;

pub const lsm_backend = runtime_impl.lsm_backend;

const restore_staging_exports = @import("standalone/restore_staging_exports.zig");

const storage_kernel_exports = @import("capi/db.zig");

const local_query_exports = @import("storage/local_query_provider.zig");

const lite_runtime = @import("cmd/lite.zig");

fn runLite(init: std.process.Init, _: []const u8, args: *std.process.Args.Iterator) !void {
    return lite_runtime.runFromIterator(init, "antfly", args);
}

fn liteEntry(context: *const bridge.Context) callconv(.c) c_int {
    return runtimeEntry(context, "lite", runLite);
}

comptime {
    // The kernel owns physical DB and local-query compilation plus
    // the C API. Product-mode orchestration stays in the distributed
    // control unit and reaches these implementations through opaque
    // owner and restore-staging entry points.
    _ = storage_kernel_exports;
    exportInternal(&storage_kernel_exports.storageOwnerMergeArtifactsPage, "antfly_storage_owner_merge_artifacts_page");
    exportInternal(&liteEntry, "antfly_runtime_lite");
    exportInternal(&restore_staging_exports.create, "antfly_restore_staging_create");
    exportInternal(&restore_staging_exports.destroy, "antfly_restore_staging_destroy");
    exportInternal(&@import("storage/db/enrichment/enrichment_types.zig").interactiveActivity, "antfly_storage_interactive_activity");
    exportInternal(&storage_kernel_exports.storageOwnerContextCreate, "antfly_storage_context_create");
    exportInternal(&storage_kernel_exports.storageOwnerContextCreateWithRuntime, "antfly_storage_context_create_with_runtime");
    exportInternal(&storage_kernel_exports.storageOwnerContextDestroy, "antfly_storage_context_destroy");
    exportInternal(&storage_kernel_exports.storageContextAttachInferenceProvider, "antfly_storage_context_attach_inference_provider");
    exportInternal(&storage_kernel_exports.storageOwnerContextConfigureRemoteContentSecurity, "antfly_storage_context_configure_remote_content_security");
    exportInternal(&storage_kernel_exports.storageOwnerContextMetrics, "antfly_storage_context_metrics");
    exportInternal(&storage_kernel_exports.storageOwnerContextInvalidateCaches, "antfly_storage_context_invalidate_caches");
    exportInternal(&storage_kernel_exports.storageContextSystemStoreOpen, "antfly_storage_context_system_store_open");
    exportInternal(&storage_kernel_exports.storageSystemStoreClose, "antfly_storage_system_store_close");
    exportInternal(&storage_kernel_exports.storageSystemStoreSync, "antfly_storage_system_store_sync");
    exportInternal(&storage_kernel_exports.storageSystemStoreBeginRead, "antfly_storage_system_store_begin_read");
    exportInternal(&storage_kernel_exports.storageSystemStoreBeginCurrentScan, "antfly_storage_system_store_begin_current_scan");
    exportInternal(&storage_kernel_exports.storageSystemStoreBeginWrite, "antfly_storage_system_store_begin_write");
    exportInternal(&storage_kernel_exports.storageSystemReadGet, "antfly_storage_system_read_get");
    exportInternal(&storage_kernel_exports.storageSystemReadOpenCursor, "antfly_storage_system_read_open_cursor");
    exportInternal(&storage_kernel_exports.storageSystemReadAbort, "antfly_storage_system_read_abort");
    exportInternal(&storage_kernel_exports.storageSystemCurrentScanOpenCursor, "antfly_storage_system_current_scan_open_cursor");
    exportInternal(&storage_kernel_exports.storageSystemCurrentScanAbort, "antfly_storage_system_current_scan_abort");
    exportInternal(&storage_kernel_exports.storageSystemWriteGet, "antfly_storage_system_write_get");
    exportInternal(&storage_kernel_exports.storageSystemWritePut, "antfly_storage_system_write_put");
    exportInternal(&storage_kernel_exports.storageSystemWriteDelete, "antfly_storage_system_write_delete");
    exportInternal(&storage_kernel_exports.storageSystemWriteCommit, "antfly_storage_system_write_commit");
    exportInternal(&storage_kernel_exports.storageSystemWriteAbort, "antfly_storage_system_write_abort");
    exportInternal(&storage_kernel_exports.storageSystemCursorMove, "antfly_storage_system_cursor_move");
    exportInternal(&storage_kernel_exports.storageSystemCursorClose, "antfly_storage_system_cursor_close");
    exportInternal(&storage_kernel_exports.storageContextLiteAdoptionProbe, "antfly_storage_context_lite_adoption_probe");
    exportInternal(&storage_kernel_exports.storageContextLiteAdoptAndVerify, "antfly_storage_context_lite_adopt_and_verify");
    exportInternal(&storage_kernel_exports.storageContextLiteMarkStandalone, "antfly_storage_context_lite_mark_standalone");
    exportInternal(&storage_kernel_exports.storageContextMaintenanceStatus, "antfly_storage_context_maintenance_status");
    exportInternal(&storage_kernel_exports.storageContextMaintenanceRun, "antfly_storage_context_maintenance_run");
    exportInternal(&storage_kernel_exports.metadataApplyStoreOpen, "antfly_metadata_apply_store_open");
    exportInternal(&storage_kernel_exports.metadataApplyStoreClose, "antfly_metadata_apply_store_close");
    exportInternal(&storage_kernel_exports.metadataApplyStoreApplyBatch, "antfly_metadata_apply_store_apply_batch");
    exportInternal(&storage_kernel_exports.metadataApplyStoreBuildSnapshot, "antfly_metadata_apply_store_build_snapshot");
    exportInternal(&storage_kernel_exports.metadataApplyStoreInstallSnapshot, "antfly_metadata_apply_store_install_snapshot");
    exportInternal(&storage_kernel_exports.metadataApplyStorePrepareSnapshot, "antfly_metadata_apply_store_prepare_snapshot");
    exportInternal(&storage_kernel_exports.metadataApplyPreparedSnapshotMaterialize, "antfly_metadata_apply_prepared_snapshot_materialize");
    exportInternal(&storage_kernel_exports.metadataApplyPreparedSnapshotCancel, "antfly_metadata_apply_prepared_snapshot_cancel");
    exportInternal(&storage_kernel_exports.metadataApplyPreparedSnapshotDestroy, "antfly_metadata_apply_prepared_snapshot_destroy");
    exportInternal(&storage_kernel_exports.metadataApplyStoreProjection, "antfly_metadata_apply_store_projection");
    exportInternal(&storage_kernel_exports.metadataApplyStoreAddListeners, "antfly_metadata_apply_store_add_listeners");
    exportInternal(&storage_kernel_exports.metadataApplyStoreRemoveListeners, "antfly_metadata_apply_store_remove_listeners");
    exportInternal(&storage_kernel_exports.metadataReconcileReplicaRoot, "antfly_metadata_reconcile_replica_root");
    exportInternal(&storage_kernel_exports.dataApplyStoreOpen, "antfly_data_apply_store_open");
    exportInternal(&storage_kernel_exports.dataApplyStoreClose, "antfly_data_apply_store_close");
    exportInternal(&storage_kernel_exports.dataApplyStoreApplyBatch, "antfly_data_apply_store_apply_batch");
    exportInternal(&storage_kernel_exports.dataApplyStoreBuildSnapshot, "antfly_data_apply_store_build_snapshot");
    exportInternal(&storage_kernel_exports.dataApplyStoreInstallSnapshot, "antfly_data_apply_store_install_snapshot");
    exportInternal(&storage_kernel_exports.dataApplyStorePrepareSnapshot, "antfly_data_apply_store_prepare_snapshot");
    exportInternal(&storage_kernel_exports.dataApplyPreparedSnapshotMaterialize, "antfly_data_apply_prepared_snapshot_materialize");
    exportInternal(&storage_kernel_exports.dataApplyPreparedSnapshotCancel, "antfly_data_apply_prepared_snapshot_cancel");
    exportInternal(&storage_kernel_exports.dataApplyPreparedSnapshotDestroy, "antfly_data_apply_prepared_snapshot_destroy");
    exportInternal(&storage_kernel_exports.dataApplyStoreLatest, "antfly_data_apply_store_latest");
    exportInternal(&storage_kernel_exports.dataApplyStoreLatestForTransition, "antfly_data_apply_store_latest_for_transition");
    exportInternal(&storage_kernel_exports.dataApplyStoreRaftBatchProtocolVersion, "antfly_data_apply_store_raft_batch_protocol_version");
    exportInternal(&storage_kernel_exports.dataApplyStoreProjection, "antfly_data_apply_store_projection");
    exportInternal(&storage_kernel_exports.dataApplyStoreReconcileOwner, "antfly_data_apply_store_reconcile_owner");
    exportInternal(&storage_kernel_exports.dataApplyStoreRetainGroups, "antfly_data_apply_store_retain_groups");
    exportInternal(&storage_kernel_exports.dataApplyStoreBeginGroupTransition, "antfly_data_apply_store_begin_group_transition");
    exportInternal(&storage_kernel_exports.dataApplyStoreCommitGroupTransition, "antfly_data_apply_store_commit_group_transition");
    exportInternal(&storage_kernel_exports.dataApplyStoreAbortGroupTransition, "antfly_data_apply_store_abort_group_transition");
    exportInternal(&storage_kernel_exports.dataApplyStoreDestroyGroupTransition, "antfly_data_apply_store_destroy_group_transition");
    exportInternal(&storage_kernel_exports.storageOwnerLocalTransition, "antfly_storage_owner_local_transition");
    exportInternal(&storage_kernel_exports.storageOwnerOpen, "antfly_storage_owner_open");
    exportInternal(&storage_kernel_exports.storageOwnerClose, "antfly_storage_owner_close");
    exportInternal(&storage_kernel_exports.storageHASeedActivateJson, "antfly_storage_ha_seed_activate_json");
    exportInternal(&storage_kernel_exports.storageHASeedValidateJson, "antfly_storage_ha_seed_validate_json");
    exportInternal(&storage_kernel_exports.storageHASeedPruneJson, "antfly_storage_ha_seed_prune_json");
    exportInternal(&storage_kernel_exports.storageOwnerConfigure, "antfly_storage_owner_configure");
    exportInternal(&storage_kernel_exports.storageOwnerReconcile, "antfly_storage_owner_reconcile");
    exportInternal(&storage_kernel_exports.storageOwnerPreflightWriteAdmission, "antfly_storage_owner_preflight_write_admission");
    exportInternal(&storage_kernel_exports.storageOwnerFindMedianKey, "antfly_storage_owner_find_median_key");
    exportInternal(&storage_kernel_exports.storageOwnerBulkBegin, "antfly_storage_owner_bulk_begin");
    exportInternal(&storage_kernel_exports.storageOwnerBulkFinish, "antfly_storage_owner_bulk_finish");
    exportInternal(&storage_kernel_exports.storageOwnerBulkAbort, "antfly_storage_owner_bulk_abort");
    exportInternal(&storage_kernel_exports.storageOwnerBatchJson, "antfly_storage_owner_batch_json");
    exportInternal(&storage_kernel_exports.storageOwnerReplicatedBatchJson, "antfly_storage_owner_replicated_batch_json");
    exportInternal(&storage_kernel_exports.storageOwnerReplicatedBatchAtRaftEntryJson, "antfly_storage_owner_replicated_batch_at_raft_entry_json");
    exportInternal(&storage_kernel_exports.storageOwnerTransactionStatus, "antfly_storage_owner_transaction_status");
    exportInternal(&storage_kernel_exports.storageOwnerWaitForSync, "antfly_storage_owner_wait_for_sync");
    exportInternal(&storage_kernel_exports.storageOwnerApplyHAReplicationRecord, "antfly_storage_owner_apply_ha_replication_record");
    exportInternal(&storage_kernel_exports.storageOwnerBackupJson, "antfly_storage_owner_backup_json");
    exportInternal(&storage_kernel_exports.storageSnapshotPrepare, "antfly_storage_snapshot_prepare");
    exportInternal(&storage_kernel_exports.storageRestorePrepare, "antfly_storage_restore_prepare");
    exportInternal(&storage_kernel_exports.storageRestoreReconcile, "antfly_storage_restore_reconcile");
    exportInternal(&storage_kernel_exports.storageRestoreApplyBootstrap, "antfly_storage_restore_apply_bootstrap");
    exportInternal(&storage_kernel_exports.storageOwnerRestoreRepair, "antfly_storage_owner_restore_repair");
    exportInternal(&storage_kernel_exports.storageSnapshotPromote, "antfly_storage_snapshot_promote");
    exportInternal(&storage_kernel_exports.storageSnapshotPublishPrepared, "antfly_storage_snapshot_publish_prepared");
    exportInternal(&storage_kernel_exports.storageSnapshotCommit, "antfly_storage_snapshot_commit");
    exportInternal(&storage_kernel_exports.storageSnapshotRollback, "antfly_storage_snapshot_rollback");
    exportInternal(&storage_kernel_exports.storageSnapshotDestroy, "antfly_storage_snapshot_destroy");
    exportInternal(&storage_kernel_exports.storageOwnerQueryJson, "antfly_storage_owner_query_json");
    exportInternal(&storage_kernel_exports.storageOwnerLookupJson, "antfly_storage_owner_lookup_json");
    exportInternal(&storage_kernel_exports.storageOwnerScanNdjson, "antfly_storage_owner_scan_ndjson");
    exportInternal(&storage_kernel_exports.storageOwnerPreflightJson, "antfly_storage_owner_preflight_json");
    exportInternal(&storage_kernel_exports.storageOwnerTextStatsJson, "antfly_storage_owner_text_stats_json");
    exportInternal(&storage_kernel_exports.storageOwnerAlgebraicPartialsJson, "antfly_storage_owner_algebraic_partials_json");
    exportInternal(&storage_kernel_exports.storageAggregateJson, "antfly_storage_aggregate_json");
    exportInternal(&storage_kernel_exports.storageOwnerGraphExpandJson, "antfly_storage_owner_graph_expand_json");
    exportInternal(&storage_kernel_exports.storageOwnerGraphHydrateJson, "antfly_storage_owner_graph_hydrate_json");
    exportInternal(&storage_kernel_exports.storageOwnerGraphEdgesJson, "antfly_storage_owner_graph_edges_json");
    exportInternal(&storage_kernel_exports.storageOwnerDocumentArtifactManifestJson, "antfly_storage_owner_document_artifact_manifest_json");
    exportInternal(&storage_kernel_exports.storageOwnerDocumentArtifactManifestsJson, "antfly_storage_owner_document_artifact_manifests_json");
    exportInternal(&storage_kernel_exports.storageOwnerArtifactOperationJson, "antfly_storage_owner_artifact_operation_json");
    exportInternal(&storage_kernel_exports.storageOwnerRuntimeStatusJson, "antfly_storage_owner_runtime_status_json");
    exportInternal(&storage_kernel_exports.storageOwnerObservedDynamicFieldCapabilitySetsJson, "antfly_storage_owner_observed_dynamic_field_capability_sets_json");
    exportInternal(&storage_kernel_exports.storageOwnerRestoreStateJson, "antfly_storage_owner_restore_state_json");
    exportInternal(&storage_kernel_exports.storageOwnerTextMemoryJson, "antfly_storage_owner_text_memory_json");
    exportInternal(&storage_kernel_exports.storageOwnerMaintenance, "antfly_storage_owner_maintenance");
    exportInternal(&storage_kernel_exports.storageWalOpen, "antfly_storage_wal_open");
    exportInternal(&storage_kernel_exports.storageWalClose, "antfly_storage_wal_close");
    exportInternal(&storage_kernel_exports.storageWalAppend, "antfly_storage_wal_append");
    exportInternal(&storage_kernel_exports.storageWalAppendIdempotent, "antfly_storage_wal_append_idempotent");
    exportInternal(&storage_kernel_exports.storageWalSync, "antfly_storage_wal_sync");
    exportInternal(&storage_kernel_exports.storageWalTruncatePrefix, "antfly_storage_wal_truncate_prefix");
    exportInternal(&storage_kernel_exports.storageWalTruncateSuffix, "antfly_storage_wal_truncate_suffix");
    exportInternal(&storage_kernel_exports.storageWalIterate, "antfly_storage_wal_iterate");
    exportInternal(&storage_kernel_exports.storageWalRead, "antfly_storage_wal_read");
    exportInternal(&storage_kernel_exports.storageWalStatsSnapshot, "antfly_storage_wal_stats_snapshot");
    exportInternal(&storage_kernel_exports.storageWalLastLsn, "antfly_storage_wal_last_lsn");
    exportInternal(&storage_kernel_exports.storageOwnerBufferDestroy, "antfly_storage_owner_buffer_destroy");
    exportInternal(&local_query_exports.execute, "antfly_local_query_execute");
    exportInternal(&local_query_exports.bufferDestroy, "antfly_local_query_buffer_destroy");
}
