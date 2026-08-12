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

//! Lossless semantic-error registry for every compiled runtime boundary.
//!
//! `Status` values are stable identities, not broad error classes. Provider and
//! consumer must both use this table so adding an ABI boundary cannot silently
//! turn an expected domain error into `StorageKernelFailure`. Only errors that
//! are absent from this registry are unexpected provider failures and become
//! `.internal`.

const std = @import("std");
const abi = @import("runtime_failure_abi");

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
    .{ .status = .corrupt_wal, .err = error.CorruptWal },
    .{ .status = .unsupported_kernel_wal_options, .err = error.UnsupportedKernelWalOptions },
    .{ .status = .wal_lsn_mismatch, .err = error.WalLsnMismatch },
    .{ .status = .read_only_transaction, .err = error.ReadOnlyTransaction },
    .{ .status = .arithmetic_overflow, .err = error.Overflow },
    // Expected operating-system and physical-WAL failures are domain-visible
    // too.  Do not collapse them into `.internal`: callers use these exact
    // identities for retry, read-only failover, and operator diagnostics.
    .{ .status = .access_denied, .err = error.AccessDenied },
    .{ .status = .disk_quota, .err = error.DiskQuota },
    .{ .status = .file_too_big, .err = error.FileTooBig },
    .{ .status = .input_output, .err = error.InputOutput },
    .{ .status = .is_dir, .err = error.IsDir },
    .{ .status = .link_quota_exceeded, .err = error.LinkQuotaExceeded },
    .{ .status = .name_too_long, .err = error.NameTooLong },
    .{ .status = .no_device, .err = error.NoDevice },
    .{ .status = .not_dir, .err = error.NotDir },
    .{ .status = .read_only_file_system, .err = error.ReadOnlyFileSystem },
    .{ .status = .sym_link_loop, .err = error.SymLinkLoop },
    .{ .status = .system_resources, .err = error.SystemResources },
    .{ .status = .write_zero, .err = error.WriteZero },
    .{ .status = .broken_pipe, .err = error.BrokenPipe },
    .{ .status = .storage_closed, .err = error.StorageClosed },
    .{ .status = .backend_closing, .err = error.BackendClosing },
    .{ .status = .wal_record_too_large, .err = error.WalRecordTooLarge },
    .{ .status = .wal_retention_limit_exceeded, .err = error.WalRetentionLimitExceeded },
    .{ .status = .write_pressure_exceeded, .err = error.WritePressureExceeded },
    .{ .status = .corrupt_lsm_wal, .err = error.CorruptLsmWal },
    .{ .status = .corrupt_lsm_wal_index, .err = error.CorruptLsmWalIndex },
    .{ .status = .truncated_lsm_wal_sparse_hole, .err = error.TruncatedLsmWalSparseHole },
    .{ .status = .truncated_lsm_wal_tail_junk, .err = error.TruncatedLsmWalTailJunk },
    .{ .status = .unsupported_lsm_wal_header, .err = error.UnsupportedLsmWalHeader },
    .{ .status = .unsupported_lsm_wal_version, .err = error.UnsupportedLsmWalVersion },
    .{ .status = .durable_atomic_rename_unsupported, .err = error.DurableAtomicRenameUnsupported },
    .{ .status = .durable_atomic_write_unsupported, .err = error.DurableAtomicWriteUnsupported },
    .{ .status = .durable_directory_sync_unsupported, .err = error.DurableDirectorySyncUnsupported },
    .{ .status = .durable_file_sync_unsupported, .err = error.DurableFileSyncUnsupported },
    .{ .status = .unsupported_platform, .err = error.UnsupportedPlatform },
    .{ .status = .unsupported_evented_io_runtime, .err = error.UnsupportedEventedIoRuntime },
    .{ .status = .dense_repair_backpressure, .err = error.DenseRepairBackpressure },
    .{ .status = .invalid_document_extraction_config, .err = error.InvalidDocumentExtractionConfig },
    .{ .status = .bad_unit_input, .err = error.BadUnitInput },
    .{ .status = .document_extraction_chunk_range_missing, .err = error.DocumentExtractionChunkRangeMissing },
    .{ .status = .document_extraction_working_set_too_large, .err = error.DocumentExtractionWorkingSetTooLarge },
    .{ .status = .invalid_document_extraction_manifest, .err = error.InvalidDocumentExtractionManifest },
    .{ .status = .invalid_document_extraction_state, .err = error.InvalidDocumentExtractionState },
    .{ .status = .invalid_graph_asset_state, .err = error.InvalidGraphAssetState },
    .{ .status = .missing_docx_document_xml, .err = error.MissingDocxDocumentXml },
    .{ .status = .pdf_extraction_unavailable, .err = error.PdfExtractionUnavailable },
    .{ .status = .unsupported_compression_method, .err = error.UnsupportedCompressionMethod },
    .{ .status = .zip64_unsupported, .err = error.Zip64Unsupported },
    .{ .status = .zip_bad_cd_offset, .err = error.ZipBadCdOffset },
    .{ .status = .zip_bad_file_offset, .err = error.ZipBadFileOffset },
    .{ .status = .zip_cd_size_mismatch, .err = error.ZipCdSizeMismatch },
    .{ .status = .zip_decompress_size_mismatch, .err = error.ZipDecompressSizeMismatch },
    .{ .status = .zip_encryption_unsupported, .err = error.ZipEncryptionUnsupported },
    .{ .status = .zip_no_end_record, .err = error.ZipNoEndRecord },
    .{ .status = .zip_truncated, .err = error.ZipTruncated },
    .{ .status = .invalid_boundary_failure_identity, .err = error.InvalidBoundaryFailureIdentity },
    .{ .status = .invalid_boundary_query_response, .err = error.InvalidBoundaryQueryResponse },
    .{ .status = .invalid_config, .err = error.InvalidConfig },
    .{ .status = .invalid_inference_model_cache_config, .err = error.InvalidInferenceModelCacheConfig },
    .{ .status = .resource_limit_exceeded, .err = error.ResourceLimitExceeded },
    .{ .status = .resource_temporarily_unavailable, .err = error.ResourceTemporarilyUnavailable },
    .{ .status = .unsupported_generator_provider, .err = error.UnsupportedGeneratorProvider },
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

/// Build the wire identity for a provider error. Declared errors retain their
/// stable semantic status; undeclared defects retain their bounded diagnostic
/// name while deliberately using `.internal` for control flow.
pub fn failureFromError(
    err: anyerror,
    boundary: abi.FailureBoundary,
    boundary_version: u32,
    operation: u32,
) abi.FailureIdentity {
    var result = abi.FailureIdentity{
        .status = statusFromError(err),
        .boundary = boundary,
        .boundary_version = boundary_version,
        .operation = operation,
    };
    const name = @errorName(err);
    const len = @min(name.len, abi.failure_error_name_capacity);
    @memcpy(result.error_name[0..len], name[0..len]);
    result.error_name_len = @intCast(len);
    result.error_name_truncated = @intFromBool(name.len > len);
    result.error_name_hash = stableErrorNameHash(name);
    return result;
}

/// Verify that the provider's control-flow status and diagnostic envelope
/// describe the same failure. Keep the returned envelope intact when this
/// reports a protocol defect so logs can show both conflicting identities.
pub fn validateFailureEnvelope(
    status: abi.Status,
    failure: *const abi.FailureIdentity,
    expected_boundary_version: u32,
) !void {
    const zero_name: [abi.failure_error_name_capacity]u8 = @splat(0);
    if (status == .ok) {
        if (failure.status != .ok or
            failure.boundary != .none or
            failure.boundary_version != expected_boundary_version or
            failure.operation != 0 or
            failure.error_name_len != 0 or
            failure.error_name_truncated != 0 or
            failure.error_name_hash != 0 or
            !std.mem.eql(u8, &failure._reserved0, &@as([2]u8, @splat(0))) or
            !std.mem.eql(u8, &failure.error_name, &zero_name))
        {
            return error.InvalidBoundaryFailureIdentity;
        }
        return;
    }
    if (failure.status != status or
        failure.boundary == .none or
        failure.boundary_version != expected_boundary_version or
        failure.operation == 0 or
        failure.error_name_len == 0 or
        failure.error_name_len > abi.failure_error_name_capacity or
        failure.error_name_hash == 0 or
        failure.error_name_truncated > 1 or
        !std.mem.eql(u8, &failure._reserved0, &@as([2]u8, @splat(0))) or
        (failure.error_name_truncated != 0 and
            failure.error_name_len != abi.failure_error_name_capacity) or
        (failure.error_name_truncated == 0 and
            stableErrorNameHash(failure.errorName()) != failure.error_name_hash) or
        !std.mem.eql(
            u8,
            failure.error_name[failure.error_name_len..],
            zero_name[failure.error_name_len..],
        ))
    {
        return error.InvalidBoundaryFailureIdentity;
    }
}

fn stableErrorNameHash(name: []const u8) u64 {
    var hash: u64 = 14_695_981_039_346_656_037;
    for (name) |byte| {
        hash ^= byte;
        hash *%= 1_099_511_628_211;
    }
    return hash;
}

/// Consumer-owned relay for callbacks invoked across a provider boundary.
/// The ABI sees only a protocol sentinel so it can unwind; the originating
/// consumer receives the exact error value after the provider returns.
pub const CallbackErrorRelay = struct {
    exact_error: ?anyerror = null,

    pub fn capture(self: *CallbackErrorRelay, err: anyerror) abi.Status {
        if (self.exact_error == null) self.exact_error = err;
        return .storage_kernel_callback_failed;
    }

    pub fn finish(self: *const CallbackErrorRelay, provider_status: abi.Status) !void {
        if (self.exact_error) |err| return err;
        return statusToError(provider_status);
    }
};

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

    @setEvalBranchQuota(100_000);
    inline for (mappings, 0..) |lhs, i| {
        inline for (mappings[i + 1 ..]) |rhs| {
            try std.testing.expect(lhs.status != rhs.status);
            try std.testing.expect(lhs.err != rhs.err);
        }
    }

    const declared = failureFromError(error.HAReadOnlyStandby, .storage_owner, 7, 41);
    try std.testing.expectEqual(abi.Status.ha_read_only_standby, declared.status);
    try std.testing.expectEqual(abi.FailureBoundary.storage_owner, declared.boundary);
    try std.testing.expectEqual(@as(u32, 7), declared.boundary_version);
    try std.testing.expectEqual(@as(u32, 41), declared.operation);
    try std.testing.expectEqualStrings("HAReadOnlyStandby", declared.errorName());
    try std.testing.expectEqual(@as(u8, 0), declared.error_name_truncated);
    try std.testing.expect(declared.error_name_hash != 0);
    try std.testing.expectError(error.HAReadOnlyStandby, statusToError(declared.status));

    const inference_config = failureFromError(
        error.InvalidInferenceModelCacheConfig,
        .inference_runtime,
        abi.abi_version,
        1,
    );
    try std.testing.expectEqual(
        abi.Status.invalid_inference_model_cache_config,
        inference_config.status,
    );
    try std.testing.expectEqual(abi.FailureBoundary.inference_runtime, inference_config.boundary);
    try std.testing.expectEqualStrings("InvalidInferenceModelCacheConfig", inference_config.errorName());
    try validateFailureEnvelope(inference_config.status, &inference_config, abi.abi_version);
    try std.testing.expectError(
        error.InvalidInferenceModelCacheConfig,
        statusToError(inference_config.status),
    );

    const defect = failureFromError(error.UnregisteredProviderDefect, .local_query, 8, 42);
    try std.testing.expectEqual(abi.Status.internal, defect.status);
    try std.testing.expectEqualStrings("UnregisteredProviderDefect", defect.errorName());
    try std.testing.expectError(error.StorageKernelFailure, statusToError(defect.status));

    try validateFailureEnvelope(declared.status, &declared, 7);
    try validateFailureEnvelope(defect.status, &defect, 8);
    const success: abi.FailureIdentity = .{};
    try validateFailureEnvelope(.ok, &success, abi.abi_version);
    var mismatched = declared;
    mismatched.status = .busy;
    try std.testing.expectError(
        error.InvalidBoundaryFailureIdentity,
        validateFailureEnvelope(.ha_read_only_standby, &mismatched, 7),
    );
    try std.testing.expectError(
        error.InvalidBoundaryFailureIdentity,
        validateFailureEnvelope(.ok, &declared, 7),
    );
    var corrupted_hash = declared;
    corrupted_hash.error_name_hash +%= 1;
    try std.testing.expectError(
        error.InvalidBoundaryFailureIdentity,
        validateFailureEnvelope(.ha_read_only_standby, &corrupted_hash, 7),
    );
    var noncanonical_success: abi.FailureIdentity = .{};
    noncanonical_success.error_name[0] = 'x';
    try std.testing.expectError(
        error.InvalidBoundaryFailureIdentity,
        validateFailureEnvelope(.ok, &noncanonical_success, abi.abi_version),
    );
    var noncanonical_failure = declared;
    noncanonical_failure.error_name[noncanonical_failure.error_name_len] = 'x';
    try std.testing.expectError(
        error.InvalidBoundaryFailureIdentity,
        validateFailureEnvelope(.ha_read_only_standby, &noncanonical_failure, 7),
    );
    var oversized_name = declared;
    oversized_name.error_name_len = abi.failure_error_name_capacity + 1;
    try std.testing.expectError(
        error.InvalidBoundaryFailureIdentity,
        validateFailureEnvelope(.ha_read_only_standby, &oversized_name, 7),
    );

    var relay = CallbackErrorRelay{};
    try std.testing.expectEqual(
        abi.Status.storage_kernel_callback_failed,
        relay.capture(error.CallbackReadOnly),
    );
    _ = relay.capture(error.LaterCallbackFailure);
    try std.testing.expectError(error.CallbackReadOnly, relay.finish(.storage_kernel_callback_failed));
    try std.testing.expectError(error.CallbackReadOnly, relay.finish(.ha_read_only_standby));
}

test "registered storage-kernel errors are unique and round trip without losing identity" {
    try validateForTest();
}
