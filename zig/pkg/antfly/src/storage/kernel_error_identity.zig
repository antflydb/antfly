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

//! Lossless semantic-error registry for the compiled storage boundary.
//!
//! `Status` values are stable identities, not broad error classes. Provider and
//! consumer must both use this table so adding an ABI boundary cannot silently
//! turn an expected domain error into `StorageKernelFailure`. Only errors that
//! are absent from this registry are unexpected provider failures and become
//! `.internal`.

const std = @import("std");
const abi = @import("kernel_owner_abi");

const Mapping = struct {
    status: abi.Status,
    err: anyerror,
};

const mappings = [_]Mapping{
    .{ .status = .invalid_abi, .err = error.InvalidAbiVersion },
    .{ .status = .invalid_argument, .err = error.InvalidArgument },
    .{ .status = .invalid_arguments, .err = error.InvalidArguments },
    .{ .status = .not_found, .err = error.NotFound },
    .{ .status = .file_not_found, .err = error.FileNotFound },
    .{ .status = .busy, .err = error.StorageBusy },
    .{ .status = .file_busy, .err = error.FileBusy },
    .{ .status = .version_conflict, .err = error.VersionConflict },
    .{ .status = .intent_conflict, .err = error.IntentConflict },
    .{ .status = .decision_conflict, .err = error.DecisionConflict },
    .{ .status = .transaction_not_found, .err = error.TxnNotFound },
    .{ .status = .read_only, .err = error.ReadOnly },
    .{ .status = .ha_read_only_standby, .err = error.HAReadOnlyStandby },
    .{ .status = .out_of_memory, .err = error.OutOfMemory },
    .{ .status = .corrupted, .err = error.Corrupted },
    .{ .status = .identity_namespace_mismatch, .err = error.DocIdentityNamespaceMismatch },
    .{ .status = .invalid_query, .err = error.InvalidQueryRequest },
    .{ .status = .unsupported_query, .err = error.UnsupportedQueryRequest },
    .{ .status = .index_not_found, .err = error.IndexNotFound },
    .{ .status = .identity_read_generation_changed, .err = error.IdentityReadGenerationChanged },
    .{ .status = .timeout, .err = error.Timeout },
    .{ .status = .table_visibility_timeout, .err = error.TableVisibilityTimeout },
    .{ .status = .cancelled, .err = error.Cancelled },
    .{ .status = .canceled, .err = error.Canceled },
    .{ .status = .snapshot_build_cancelled, .err = error.SnapshotBuildCancelled },
    .{ .status = .restore_identity_mismatch, .err = error.RestoreIdentityMismatch },
    .{ .status = .invalid_backup, .err = error.InvalidBackupRequest },
    .{ .status = .backup_integrity_missing, .err = error.BackupIntegrityMissing },
    .{ .status = .backup_artifact_missing, .err = error.BackupArtifactMissing },
    .{ .status = .backup_artifact_format_mismatch, .err = error.BackupArtifactFormatMismatch },
    .{ .status = .backup_artifact_integrity_mismatch, .err = error.BackupArtifactIntegrityMismatch },
    .{ .status = .invalid_backup_artifact_path, .err = error.InvalidBackupArtifactPath },
    .{ .status = .backup_artifact_too_large, .err = error.BackupArtifactTooLarge },
    .{ .status = .unsupported_backup_artifact, .err = error.UnsupportedBackupArtifact },
    .{ .status = .unsupported_backup_migration, .err = error.UnsupportedBackupMigrationState },
    .{ .status = .restore_identity_namespace_mismatch, .err = error.IdentityNamespaceMismatch },
    .{ .status = .invalid_aggregation, .err = error.InvalidAggregation },
    .{ .status = .unsupported_aggregation, .err = error.UnsupportedAggregation },
    .{ .status = .query_candidate_budget_exceeded, .err = error.QueryCandidateBudgetExceeded },
    .{ .status = .invalid_index_config, .err = error.InvalidIndexConfig },
    .{ .status = .algebraic_planner_scan_too_large, .err = error.AlgebraicPlannerScanTooLarge },
    .{ .status = .algebraic_result_bucket_limit, .err = error.AlgebraicResultBucketLimit },
    .{ .status = .invalid_algebraic_tensor_expr, .err = error.InvalidAlgebraicTensorExpr },
    .{ .status = .invalid_algebraic_tensor_row, .err = error.InvalidAlgebraicTensorRow },
    .{ .status = .lsm_root_writer_already_open, .err = error.LsmRootWriterAlreadyOpen },
    .{ .status = .generation_transition_active, .err = error.GenerationTransitionActive },
    .{ .status = .would_block, .err = error.WouldBlock },
    .{ .status = .path_already_exists, .err = error.PathAlreadyExists },
    .{ .status = .snapshot_too_large, .err = error.SnapshotTooLarge },
    .{ .status = .truncated_native_header, .err = error.TruncatedNativeHeader },
    .{ .status = .unsupported_native_format_version, .err = error.UnsupportedNativeFormatVersion },
    .{ .status = .missing_participant_resolver, .err = error.MissingParticipantResolver },
    .{ .status = .missing_replicated_recovery_hooks, .err = error.MissingReplicatedRecoveryHooks },
    .{ .status = .storage_kernel_callback_failed, .err = error.StorageKernelCallbackFailed },
    .{ .status = .storage_kernel_recovery_callback_failed, .err = error.StorageKernelRecoveryCallbackFailed },
    .{ .status = .resource_budget_exceeded, .err = error.ResourceBudgetExceeded },
    .{ .status = .table_not_found, .err = error.TableNotFound },
    .{ .status = .conflicting_enrichment_config, .err = error.ConflictingEnrichmentConfig },
    .{ .status = .invalid_table_index_metadata, .err = error.InvalidTableIndexMetadata },
    .{ .status = .invalid_table_schema, .err = error.InvalidTableSchema },
    .{ .status = .invalid_create_table_request, .err = error.InvalidCreateTableRequest },
    .{ .status = .unsupported_create_table_request, .err = error.UnsupportedCreateTableRequest },
    .{ .status = .storage_kernel_owner_unavailable, .err = error.StorageKernelOwnerUnavailable },
    .{ .status = .invalid_batch_request, .err = error.InvalidBatchRequest },
    .{ .status = .unsupported_batch_request_encoding, .err = error.UnsupportedBatchRequestEncoding },
    .{ .status = .value_too_long, .err = error.ValueTooLong },
    .{ .status = .invalid_filter_query_request, .err = error.InvalidFilterQueryRequest },
    .{ .status = .invalid_exclusion_query_request, .err = error.InvalidExclusionQueryRequest },
    .{ .status = .unsupported_filter_query_request, .err = error.UnsupportedFilterQueryRequest },
    .{ .status = .unsupported_exclusion_query_request, .err = error.UnsupportedExclusionQueryRequest },
    .{ .status = .invalid_native_snapshot_path, .err = error.InvalidNativeSnapshotPath },
    .{ .status = .invalid_native_magic, .err = error.InvalidNativeMagic },
    .{ .status = .invalid_native_header_size, .err = error.InvalidNativeHeaderSize },
    .{ .status = .native_header_checksum_mismatch, .err = error.NativeHeaderChecksumMismatch },
    .{ .status = .invalid_native_page_size, .err = error.InvalidNativePageSize },
    .{ .status = .invalid_native_checkpoint, .err = error.InvalidNativeCheckpoint },
    .{ .status = .truncated_native_file, .err = error.TruncatedNativeFile },
    .{ .status = .end_of_stream, .err = error.EndOfStream },
    .{ .status = .truncated, .err = error.Truncated },
    .{ .status = .invalid_magic, .err = error.InvalidMagic },
    .{ .status = .header_crc_mismatch, .err = error.HeaderCrcMismatch },
    .{ .status = .unsupported_version, .err = error.UnsupportedVersion },
    .{ .status = .block_crc_mismatch, .err = error.BlockCrcMismatch },
    .{ .status = .writer_locked, .err = error.WriterLocked },
    .{ .status = .active_node_finalize_rejected, .err = error.ActiveNodeFinalizeRejected },
    .{ .status = .applied_snapshot_index_mismatch, .err = error.AppliedSnapshotIndexMismatch },
    .{ .status = .invalid_committed_entries_encoding, .err = error.InvalidCommittedEntriesEncoding },
    .{ .status = .invalid_metadata_apply_batch, .err = error.InvalidMetadataApplyBatch },
    .{ .status = .invalid_metadata_incarnation, .err = error.InvalidMetadataIncarnation },
    .{ .status = .invalid_metadata_record, .err = error.InvalidMetadataRecord },
    .{ .status = .invalid_metadata_snapshot, .err = error.InvalidMetadataSnapshot },
    .{ .status = .invalid_metadata_transition_encoding, .err = error.InvalidMetadataTransitionEncoding },
    .{ .status = .invalid_replication_cutover_intent, .err = error.InvalidReplicationCutoverIntent },
    .{ .status = .invalid_restore_intent_identity, .err = error.InvalidRestoreIntentIdentity },
    .{ .status = .invalid_restore_job_record, .err = error.InvalidRestoreJobRecord },
    .{ .status = .invalid_restore_progress_record, .err = error.InvalidRestoreProgressRecord },
    .{ .status = .invalid_split_admission, .err = error.InvalidSplitAdmission },
    .{ .status = .invalid_table_definition_replacement, .err = error.InvalidTableDefinitionReplacement },
    .{ .status = .invalid_table_id, .err = error.InvalidTableId },
    .{ .status = .invalid_table_transition_fence, .err = error.InvalidTableTransitionFence },
    .{ .status = .metadata_snapshot_too_large, .err = error.MetadataSnapshotTooLarge },
    .{ .status = .missing_metadata_batch, .err = error.MissingMetadataBatch },
    .{ .status = .missing_metadata_snapshot_source, .err = error.MissingMetadataSnapshotSource },
    .{ .status = .no_space_left, .err = error.NoSpaceLeft },
    .{ .status = .reserved_group_id, .err = error.ReservedGroupId },
    .{ .status = .table_transition_count_exhausted, .err = error.TableTransitionCountExhausted },
    .{ .status = .table_transition_generation_exhausted, .err = error.TableTransitionGenerationExhausted },
    .{ .status = .unexpected_metadata_snapshot_artifact, .err = error.UnexpectedMetadataSnapshotArtifact },
    .{ .status = .provider_internal, .err = error.Internal },
};

pub fn statusFromError(err: anyerror) abi.Status {
    inline for (mappings) |mapping| {
        if (err == mapping.err) return mapping.status;
    }
    return .internal;
}

pub fn statusToError(status: abi.Status) !void {
    if (status == .ok) return;
    // ABI 27 used this broad status for all backup-integrity failures. Keep it
    // readable for mixed-version diagnostics; ABI 28 providers emit the exact
    // identities above.
    if (status == .backup_integrity) return error.BackupArtifactIntegrityMismatch;
    inline for (mappings) |mapping| {
        if (status == mapping.status) return mapping.err;
    }
    return error.StorageKernelFailure;
}

fn hasRegisteredIdentity(status: abi.Status) bool {
    inline for (mappings) |mapping| {
        if (status == mapping.status) return true;
    }
    return false;
}

pub fn validateForTest() !void {
    inline for (mappings) |mapping| {
        try std.testing.expectEqual(mapping.status, statusFromError(mapping.err));
        try std.testing.expectError(mapping.err, statusToError(mapping.status));
    }
    try std.testing.expectEqual(abi.Status.internal, statusFromError(error.UnregisteredKernelError));
    try std.testing.expectError(error.StorageKernelFailure, statusToError(.internal));

    // Adding a Status without adding its inverse mapping must break this test.
    // The three exceptions are protocol sentinels rather than domain-error
    // identities: success, the ABI-27 compatibility status, and the explicit
    // unexpected-provider-failure sentinel.
    inline for (std.meta.fields(abi.Status)) |field| {
        const status: abi.Status = @enumFromInt(field.value);
        if (status == .ok or status == .backup_integrity or status == .internal) continue;
        try std.testing.expect(hasRegisteredIdentity(status));
    }

    @setEvalBranchQuota(20_000);
    inline for (mappings, 0..) |lhs, i| {
        inline for (mappings[i + 1 ..]) |rhs| {
            try std.testing.expect(lhs.status != rhs.status);
            try std.testing.expect(lhs.err != rhs.err);
        }
    }
}

test "registered storage-kernel errors are unique and round trip without losing identity" {
    try validateForTest();
}
