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

//! Storage-owned batch, enrichment configuration, restore, and backup operations.

const builtin = @import("builtin");
const std = @import("std");
const scraping = @import("antfly_scraping");
const common_secrets = @import("../common/secrets.zig");
const fs_paths = @import("../common/fs_paths.zig");
const backups_api = @import("../api/backups.zig");
const metadata_table_provisioner = @import("../metadata/table_provisioner.zig");
const backup_restore = @import("../raft/storage/backup_restore.zig");
const transactions_mod = @import("transactions.zig");
const doc_identity = @import("db/doc_identity.zig");
const hbc_mod = @import("hbc_adapter.zig");
const lsm_backend = @import("lsm_backend/mod.zig");
const portable_backup = @import("portable_backup.zig");
const resource_manager_mod = @import("resource_manager.zig");
const storage_schema = @import("schema.zig");
const tables_api = @import("../api/tables.zig");
const stored_destination_authorization = @import("../api/stored_destination_authorization.zig");
const managed_embedder = @import("../inference/managed_embedder.zig");
const remote_capabilities = @import("../inference/remote_capabilities.zig");
const db_embedder = @import("db/enrichment/embedder.zig");
const asset_producer_runtime = @import("../asset_producer_runtime.zig");
const asset_producer_mod = @import("db/enrichment/asset_producer.zig");
const document_extraction_mod = @import("db/enrichment/document_extraction.zig");
const distributed_txn = @import("../api/distributed_txn.zig");
const platform_time = @import("antfly_platform").time;
const db_mod = @import("antfly_source_root").antfly_sources.selected_db;
const control_only_storage_sources = false;
const contract = @import("../api/local_write_contract.zig");
pub const ManagedDbOpenMode = contract.ManagedDbOpenMode;
pub const StartupCatchUpMetadata = contract.StartupCatchUpMetadata;
pub const StorageKernelArtifactChildRangeBatchRequest = contract.StorageKernelArtifactChildRangeBatchRequest;
pub const StorageKernelArtifactDocumentRequest = contract.StorageKernelArtifactDocumentRequest;
pub const StorageKernelArtifactPlacementRequest = contract.StorageKernelArtifactPlacementRequest;
pub const StorageKernelArtifactRangeRequest = contract.StorageKernelArtifactRangeRequest;
pub const StorageKernelEmbeddingCorruptionRequest = contract.StorageKernelEmbeddingCorruptionRequest;
pub const StorageKernelReconcileResult = contract.StorageKernelReconcileResult;
pub const StorageKernelReconcileState = contract.StorageKernelReconcileState;
pub const encodeStorageKernelArtifactChildRangeBatchRequest = contract.encodeStorageKernelArtifactChildRangeBatchRequest;
pub const freeBackupShards = contract.freeBackupShards;
pub const freeStorageKernelBackupShards = contract.freeStorageKernelBackupShards;
pub const indexesJsonHasGeneratedEnrichment = contract.indexesJsonHasGeneratedEnrichment;
pub const indexesJsonNeedsAssetProducer = contract.indexesJsonNeedsAssetProducer;
pub const jsonStringHasGeneratedEnrichment = contract.jsonStringHasGeneratedEnrichment;
pub const jsonStringLooksStructured = contract.jsonStringLooksStructured;
pub const jsonStringNeedsAssetProducer = contract.jsonStringNeedsAssetProducer;
pub const jsonValueHasGeneratedEnrichment = contract.jsonValueHasGeneratedEnrichment;
pub const jsonValueNeedsAssetProducer = contract.jsonValueNeedsAssetProducer;
pub const objectIsModelBackedAssetEnrichment = contract.objectIsModelBackedAssetEnrichment;

pub fn nativeSnapshotAttemptTokenAlloc(
    alloc: std.mem.Allocator,
    io: std.Io,
    backup_id: []const u8,
    shard_label: []const u8,
) ![]u8 {
    var entropy: [16]u8 = undefined;
    try io.randomSecure(&entropy);
    const nonce = std.fmt.bytesToHex(entropy, .lower);
    return try std.fmt.allocPrint(alloc, "{s}-{s}-attempt-{s}", .{ backup_id, shard_label, &nonce });
}

pub const native_snapshot_attempt_marker_suffix = ".native-backup-attempt.json";

pub const native_snapshot_attempt_marker_directory = ".native-backup-attempts";

pub const native_snapshot_attempt_stale_ns: i128 = 24 * std.time.ns_per_hour;

pub const native_snapshot_attempt_reclaim_limit: usize = 8;

pub const native_snapshot_attempt_scan_limit: usize = 8192;

pub const NativeSnapshotAttemptMarker = struct {
    snapshot_token: []const u8,
    created_at_unix_ns: i128,
};

/// Process/crash ownership for one native snapshot export. The kernel lease is
/// the authority that an attempt is still active; wall-clock age is used only
/// to bound how often abandoned attempts are considered for reclamation.
pub const NativeSnapshotAttempt = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    marker_path: []u8,
    lease: std.Io.File,
    active: bool = true,

    pub fn deinit(self: *@This()) void {
        if (!self.active) return;
        self.lease.close(self.io);
        deleteNativeSnapshotAttemptMarker(self.io, self.marker_path);
        self.alloc.free(self.marker_path);
        self.active = false;
    }

    fn abandonForTest(self: *@This()) void {
        std.debug.assert(builtin.is_test and self.active);
        self.lease.close(self.io);
        self.alloc.free(self.marker_path);
        self.active = false;
    }
};

pub fn nativeSnapshotAttemptMarkerPathAlloc(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    snapshot_token: []const u8,
) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}.snapshots/{s}/{s}{s}", .{
        db_path,
        native_snapshot_attempt_marker_directory,
        snapshot_token,
        native_snapshot_attempt_marker_suffix,
    });
}

pub fn createNativeSnapshotAttemptMarker(
    alloc: std.mem.Allocator,
    io: std.Io,
    db_path: []const u8,
    snapshot_token: []const u8,
    created_at_unix_ns: i128,
) !NativeSnapshotAttempt {
    const snapshot_parent = try std.fmt.allocPrint(alloc, "{s}.snapshots", .{db_path});
    defer alloc.free(snapshot_parent);
    try fs_paths.createDirPathPortable(io, snapshot_parent);
    const marker_directory = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ snapshot_parent, native_snapshot_attempt_marker_directory });
    defer alloc.free(marker_directory);
    try fs_paths.createDirPathPortable(io, marker_directory);
    const marker_path = try nativeSnapshotAttemptMarkerPathAlloc(alloc, db_path, snapshot_token);
    errdefer alloc.free(marker_path);
    errdefer deleteNativeSnapshotAttemptMarker(io, marker_path);
    const body = try std.json.Stringify.valueAlloc(alloc, NativeSnapshotAttemptMarker{
        .snapshot_token = snapshot_token,
        .created_at_unix_ns = created_at_unix_ns,
    }, .{});
    defer alloc.free(body);
    var file = try std.Io.Dir.cwd().createFile(io, marker_path, .{
        .read = true,
        .truncate = true,
        .exclusive = true,
        .lock = .exclusive,
        .lock_nonblocking = true,
    });
    errdefer file.close(io);
    var buffer: [4096]u8 = undefined;
    var writer = file.writer(io, &buffer);
    try writer.interface.writeAll(body);
    try writer.end();
    try file.sync(io);
    try fs_paths.syncDirPortable(io, marker_directory);
    try fs_paths.syncDirPortable(io, snapshot_parent);
    return .{
        .alloc = alloc,
        .io = io,
        .marker_path = marker_path,
        .lease = file,
    };
}

pub fn deleteNativeSnapshotAttemptMarker(io: std.Io, marker_path: []const u8) void {
    std.Io.Dir.cwd().deleteFile(io, marker_path) catch |err| switch (err) {
        error.FileNotFound => return,
        else => {
            std.log.warn("failed to remove native snapshot attempt marker path={s} err={s}", .{ marker_path, @errorName(err) });
            return;
        },
    };
    const parent = std.fs.path.dirname(marker_path) orelse return;
    fs_paths.syncDirPortable(io, parent) catch |err| {
        std.log.warn("failed to sync native snapshot attempt marker deletion path={s} err={s}", .{ marker_path, @errorName(err) });
    };
}

pub fn reclaimStaleNativeSnapshotAttempts(
    alloc: std.mem.Allocator,
    io: std.Io,
    db_path: []const u8,
) !void {
    const snapshot_parent = try std.fmt.allocPrint(alloc, "{s}.snapshots", .{db_path});
    defer alloc.free(snapshot_parent);
    const marker_directory = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ snapshot_parent, native_snapshot_attempt_marker_directory });
    defer alloc.free(marker_directory);
    var stale_tokens = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (stale_tokens.items) |token| alloc.free(token);
        stale_tokens.deinit(alloc);
    }
    const now = platform_time.realtimeNs();
    {
        var dir = std.Io.Dir.cwd().openDir(io, marker_directory, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };
        defer dir.close(io);
        var iterator = dir.iterate();
        var examined: usize = 0;
        while (try iterator.next(io)) |entry| {
            examined += 1;
            // Reclamation is bounded maintenance, not backup admission. A
            // large marker directory may defer some cleanup, but must not
            // turn historical cleanup debt into a foreground backup outage.
            if (examined > native_snapshot_attempt_scan_limit) break;
            if (stale_tokens.items.len == native_snapshot_attempt_reclaim_limit) break;
            if (entry.kind != .file or entry.name.len <= native_snapshot_attempt_marker_suffix.len or
                !std.mem.endsWith(u8, entry.name, native_snapshot_attempt_marker_suffix))
            {
                continue;
            }
            const token = entry.name[0 .. entry.name.len - native_snapshot_attempt_marker_suffix.len];
            if (std.mem.indexOfAny(u8, token, "/\\\x00") != null or
                std.mem.indexOf(u8, token, "-attempt-") == null)
            {
                continue;
            }
            // Age identifies candidates; the nonblocking kernel lease below is
            // the only proof that their owner is gone.
            var lease = dir.openFile(io, entry.name, .{
                .mode = .read_write,
                .lock = .exclusive,
                .lock_nonblocking = true,
            }) catch |err| switch (err) {
                error.WouldBlock => continue,
                error.FileNotFound => continue,
                error.FileLocksUnsupported => return error.FileLocksUnsupported,
                else => return err,
            };
            defer lease.close(io);
            var created_at_unix_ns: i128 = @intCast((try lease.stat(io)).mtime.toNanoseconds());
            const marker_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ marker_directory, entry.name });
            defer alloc.free(marker_path);
            if (std.Io.Dir.cwd().readFileAlloc(io, marker_path, alloc, .limited(4096)) catch null) |raw| {
                defer alloc.free(raw);
                if (std.json.parseFromSlice(NativeSnapshotAttemptMarker, alloc, raw, .{})) |parsed_value| {
                    var parsed = parsed_value;
                    defer parsed.deinit();
                    if (std.mem.eql(u8, parsed.value.snapshot_token, token))
                        created_at_unix_ns = parsed.value.created_at_unix_ns;
                } else |_| {}
            }
            if (created_at_unix_ns > now or
                now - created_at_unix_ns < native_snapshot_attempt_stale_ns)
            {
                continue;
            }
            try stale_tokens.append(alloc, try alloc.dupe(u8, token));
        }
    }

    for (stale_tokens.items) |token| {
        const snapshot_root = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ snapshot_parent, token });
        defer alloc.free(snapshot_root);
        // `deleteTree` is already idempotent for an absent path. Propagate
        // type/permission failures so a non-directory collision cannot be
        // silently orphaned while its durable marker is removed.
        try std.Io.Dir.cwd().deleteTree(io, snapshot_root);

        var cleanup_dir = std.Io.Dir.cwd().openDir(io, snapshot_parent, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => continue,
            else => return err,
        };
        defer cleanup_dir.close(io);
        const staging_prefix = try std.fmt.allocPrint(alloc, ".{s}.staging-", .{token});
        defer alloc.free(staging_prefix);
        var cleanup_iterator = cleanup_dir.iterate();
        while (try cleanup_iterator.next(io)) |entry| {
            if (entry.kind != .directory or !std.mem.startsWith(u8, entry.name, staging_prefix)) continue;
            const staging_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ snapshot_parent, entry.name });
            defer alloc.free(staging_path);
            try std.Io.Dir.cwd().deleteTree(io, staging_path);
        }
        const marker_path = try nativeSnapshotAttemptMarkerPathAlloc(alloc, db_path, token);
        defer alloc.free(marker_path);
        deleteNativeSnapshotAttemptMarker(io, marker_path);
    }
    try fs_paths.syncDirPortable(io, snapshot_parent);
}

pub const local_schema_json_key = "\x00\x00__metadata__:schema_json";

pub fn applyStorageKernelReplicatedBatch(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    table_name: []const u8,
    group_id: u64,
    req: db_mod.types.BatchRequest,
) !void {
    try validateTableBatchAgainstLocalSchema(alloc, db, req.writes, req.deletes, req.transforms);
    runTestBeforeBatchExecutionHook();
    if (req.transaction != null)
        try applyReplicatedTransactionMutation(alloc, db, table_name, group_id, req)
    else
        try db.batchReplicatedApply(req);
}

pub fn applyStorageKernelReplicatedBatchAtRaftEntry(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    table_name: []const u8,
    group_id: u64,
    req: db_mod.types.BatchRequest,
    raft_entry: db_mod.RaftAppliedEntryIdentity,
) !void {
    try validateTableBatchAgainstLocalSchema(alloc, db, req.writes, req.deletes, req.transforms);
    runTestBeforeBatchExecutionHook();
    if (req.transaction != null)
        try applyReplicatedTransactionMutationAtRaftEntry(alloc, db, table_name, group_id, req, raft_entry)
    else
        try db.batchRaftReplicatedApply(req, raft_entry);
}

pub fn applyReplicatedTransactionMutation(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    table_name: []const u8,
    group_id: u64,
    req: db_mod.types.BatchRequest,
) !void {
    try applyReplicatedTransactionMutationInternal(alloc, db, table_name, group_id, req, .none, null);
}

pub fn applyReplicatedTransactionMutationAtRaftEntry(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    table_name: []const u8,
    group_id: u64,
    req: db_mod.types.BatchRequest,
    raft_entry: db_mod.RaftAppliedEntryIdentity,
) !void {
    try applyReplicatedTransactionMutationInternal(alloc, db, table_name, group_id, req, .none, raft_entry);
}

pub fn applyReplicatedTransactionMutationInternal(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    table_name: []const u8,
    group_id: u64,
    req: db_mod.types.BatchRequest,
    visibility_cancellation: db_mod.types.CancellationToken,
    raft_entry: ?db_mod.RaftAppliedEntryIdentity,
) !void {
    const mutation = req.transaction orelse return error.InvalidBatchRequest;
    switch (mutation) {
        .begin => |begin| {
            const local_participant = try distributed_txn.participantIdForGroup(alloc, table_name, group_id);
            defer alloc.free(local_participant);
            if (begin.participants.len == 0) return error.InvalidBatchRequest;
            var seen = std.StringHashMapUnmanaged(void).empty;
            defer seen.deinit(alloc);
            var local_present = false;
            for (begin.participants) |participant| {
                if (distributed_txn.parseParticipantRef(participant) == null) return error.InvalidBatchRequest;
                const entry = try seen.getOrPut(alloc, participant);
                if (entry.found_existing) return error.InvalidBatchRequest;
                if (std.mem.eql(u8, participant, local_participant)) local_present = true;
            }
            if (!local_present) return error.InvalidBatchRequest;
            const coordinator = std.mem.eql(u8, begin.participants[0], local_participant);
            const local_only = [_][]const u8{local_participant};
            // Only the coordinator owns the full participant fan-out. A
            // follower tracks itself, making successful cleanup O(N) rather
            // than every participant retrying every other participant.
            const durable_participants: []const []const u8 = if (coordinator) begin.participants else &local_only;
            if (raft_entry) |entry|
                _ = try db.beginReplicatedTransactionAtRaftEntry(
                    begin.txn_id,
                    begin.begin_timestamp,
                    begin.created_at_ns,
                    durable_participants,
                    coordinator,
                    begin.retain_terminal,
                    entry,
                )
            else
                _ = try db.beginTransactionWithIdAndParticipantsCreatedAtRoleAndRetention(
                    begin.txn_id,
                    begin.begin_timestamp,
                    begin.created_at_ns,
                    durable_participants,
                    coordinator,
                    begin.retain_terminal,
                );
        },
        .prepare => |prepare| {
            const intents: db_mod.types.TransactionIntentRequest = .{
                .writes = batchWritesAsTransactionWrites(req.writes),
                .deletes = req.deletes,
                .transforms = req.transforms,
                .predicates = req.predicates,
            };
            if (raft_entry) |entry|
                try db.writeReplicatedTransactionAtRaftEntry(prepare.txn_id, intents, entry)
            else
                try db.writeTransaction(prepare.txn_id, intents);
        },
        .resolve => |resolve| {
            const local_participant = try distributed_txn.participantIdForGroup(alloc, table_name, group_id);
            defer alloc.free(local_participant);
            if (raft_entry) |entry| {
                // Retained coordinators keep their own acknowledgement pending
                // until the API session registry durably records the response.
                // Everyone else records resolution, acknowledgement, and the
                // Raft receipt in one backend batch.
                const defer_coordinator_ack = db.transactionRetainsCoordinatorAcknowledgement(resolve.txn_id) catch |err| switch (err) {
                    transactions_mod.TxnError.TxnNotFound => if (resolve.status == .aborted) false else return err,
                    else => return err,
                };
                try db.resolveReplicatedTransactionAtRaftEntry(
                    resolve.txn_id,
                    resolve.status,
                    resolve.commit_version,
                    req.sync_level,
                    visibility_cancellation,
                    entry,
                    if (defer_coordinator_ack) null else local_participant,
                );
            } else {
                try db.resolveTransactionIntentsWithSyncLevelAndCancellation(
                    resolve.txn_id,
                    resolve.status,
                    resolve.commit_version,
                    req.sync_level,
                    visibility_cancellation,
                );
                const defer_coordinator_ack = db.transactionDefersCoordinatorAcknowledgement(resolve.txn_id) catch |err| switch (err) {
                    transactions_mod.TxnError.TxnNotFound => if (resolve.status == .aborted) false else return err,
                    else => return err,
                };
                if (!defer_coordinator_ack) {
                    db.markTransactionParticipantResolved(resolve.txn_id, local_participant) catch |err| switch (err) {
                        transactions_mod.TxnError.TxnNotFound => if (resolve.status != .aborted) return err,
                        else => return err,
                    };
                }
            }
        },
        .acknowledge => |ack| (if (raft_entry) |entry|
            db.markReplicatedTransactionParticipantResolvedAtRaftEntry(ack.txn_id, ack.participant, entry)
        else
            db.markTransactionParticipantResolved(ack.txn_id, ack.participant)) catch |err| switch (err) {
            // Cleanup and acknowledgements are independently retryable Raft
            // commands. Once cleanup wins, a late acknowledgement is a safe
            // no-op and must not recreate coordinator sidecar metadata.
            transactions_mod.TxnError.TxnNotFound => {},
            else => return err,
        },
        .cleanup => |cleanup| {
            if (raft_entry) |entry|
                _ = try db.cleanupReplicatedTransactionAtRaftEntry(
                    cleanup.txn_id,
                    cleanup.cutoff_timestamp,
                    cleanup.retained_cutoff_timestamp,
                    entry,
                )
            else
                _ = try db.cleanupTransactionMetadataIfEligible(
                    cleanup.txn_id,
                    cleanup.cutoff_timestamp,
                    cleanup.retained_cutoff_timestamp,
                );
        },
    }
}

pub fn batchWritesAsTransactionWrites(writes: []const db_mod.types.BatchWrite) []const db_mod.types.TransactionWrite {
    comptime std.debug.assert(@sizeOf(db_mod.types.BatchWrite) == @sizeOf(db_mod.types.TransactionWrite));
    comptime std.debug.assert(@alignOf(db_mod.types.BatchWrite) == @alignOf(db_mod.types.TransactionWrite));
    return @ptrCast(writes);
}

pub fn corruptEmbeddingArtifactInDb(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    doc_key: []const u8,
    index_name: []const u8,
) !bool {
    const dense_name = db.core.index_manager.denseEmbeddingName(index_name);
    const sparse_name = db.core.index_manager.sparseEmbeddingName(index_name);
    const candidate_names = [_]?[]const u8{
        index_name,
        dense_name,
        sparse_name,
    };
    const prefix = try db_mod.internal_keys.artifactTypePrefixAlloc(alloc, doc_key, "embedding");
    defer alloc.free(prefix);
    const artifacts = try db.core.scanStorePrefix(alloc, prefix);
    defer db_mod.docstore.DocStore.freeResults(alloc, artifacts);

    var fallback_key: ?[]const u8 = null;
    for (artifacts) |entry| {
        if (!db_mod.internal_keys.isEmbeddingArtifactKey(entry.key)) continue;
        if (fallback_key == null) fallback_key = entry.key;
        for (candidate_names) |candidate_opt| {
            const candidate = candidate_opt orelse continue;
            if (!db_mod.internal_keys.matchesEmbeddingArtifactName(entry.key, candidate)) continue;
            try db.core.store.put(entry.key, "bad-artifact");
            return true;
        }
    }

    if (fallback_key) |artifact_key| {
        try db.core.store.put(artifact_key, "bad-artifact");
        return true;
    }

    const injection_names = [_]?[]const u8{
        dense_name,
        sparse_name,
        index_name,
    };
    for (injection_names) |candidate_opt| {
        const candidate = candidate_opt orelse continue;
        const artifact_key = try db_mod.internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, doc_key, candidate);
        defer alloc.free(artifact_key);
        try db.core.store.put(artifact_key, "bad-artifact");
        return true;
    }

    return false;
}

pub const ManagedDbOpenOptions = struct {
    /// Identity captured with the route that selected the group. Supplying it
    /// prevents a cache miss from performing a second, potentially older,
    /// catalog read before creating the database.
    identity_namespace_override: ?doc_identity.Namespace = null,
    drain_resolver_backfill: bool = false,
    defer_resolver_workers: bool = false,
    source_table: []const u8 = "",
    destination_authorizer: ?stored_destination_authorization.Authorizer = null,
    schema_json_before_index_load: ?[]const u8 = null,
    /// Request-owned Raft progress must never sleep inside writer acquisition:
    /// the single cadence lane retries on its next tick and remains available
    /// to drive elections, heartbeats, and unrelated groups.
    retry_transient_writer_open: bool = true,
    /// HA replay must reconcile catalog-driven indexes while the node remains a
    /// read-only standby. Perform that structural reconciliation in an isolated
    /// workerless open, then reopen with the live HA gate before publishing the
    /// DB to any cache or caller.
    reconcile_for_replicated_apply: bool = false,
    inference_api_url: ?[]const u8 = null,
    remote_capability_cache: ?*remote_capabilities.Cache = null,
    ha_write_gate: ?db_mod.HAWriteGate = null,
    ha_async_effect_mirror: ?db_mod.HAAsyncEffectMirror = null,
    ha_async_batch_mirror: ?db_mod.HAAsyncBatchMirror = null,
    ha_async_metadata_mirror: ?db_mod.HAAsyncMetadataMirror = null,
    staged_generation: ?*const (if (control_only_storage_sources) anyopaque else db_mod.generation_lifecycle.StagedGeneration) = null,
    /// Immutable native backend decision retained by PreparedRestore. Only
    /// repair execution policy and owned enrichment providers may be layered
    /// over this plan; storage topology remains the one admitted at the live
    /// target path.
    native_restore_open_plan: ?*const (if (control_only_storage_sources) anyopaque else db_mod.NativeRestoreOpenPlan) = null,
    identity_validation: StartupCatchUpMetadata.IdentityValidation = .exact,
    transaction_recovery: @import("db/transaction_recovery_contract.zig").Config = .{},
    dense_native_migration_policy_source: ?db_mod.DenseNativeMigrationPolicySource = null,
    /// Restrict metadata reconciliation during a cold open to one index. The
    /// full JSON remains available to construct managed producer runtimes, but
    /// sibling index and resolver catalogs are left untouched.
    reconcile_target_index_name: ?[]const u8 = null,
    /// A structural owner may reconfigure a resident writer under its own
    /// admission fence. Cache lookup must not retire that writer merely
    /// because the supplied desired metadata is newer; cold misses still open
    /// directly with the supplied configuration.
    reuse_cached_writer_for_metadata_reconcile: bool = false,
};

pub const ManagedDbEnrichmentSet = struct {
    dense: ?db_embedder.DenseEmbedder = null,
    sparse: ?db_embedder.SparseEmbedder = null,
    asset_runtime: ?*asset_producer_runtime.Runtime = null,
    chunk_provider: ?db_mod.enrichment_runtime.ChunkProvider = null,
    generated: bool = false,

    pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
        if (self.dense) |owned| owned.deinit(allocator);
        if (self.sparse) |owned| owned.deinit(allocator);
        if (self.asset_runtime) |runtime| {
            runtime.deinit();
            allocator.destroy(runtime);
        }
        if (self.chunk_provider) |*provider| provider.deinit();
    }

    fn enabled(self: @This()) bool {
        return self.dense != null or self.sparse != null or self.asset_runtime != null or self.generated;
    }

    fn config(self: @This()) db_mod.enrichment_runtime.Config {
        return .{
            .dense_embedder = self.dense,
            .sparse_embedder = self.sparse,
            .asset_producer = if (self.asset_runtime) |runtime| runtime.ownedProducer() else null,
            .chunk_provider = self.chunk_provider,
            .enable_without_producers = self.generated,
        };
    }

    fn forgetTransferred(self: *@This()) void {
        self.dense = null;
        self.sparse = null;
        self.asset_runtime = null;
        self.chunk_provider = null;
        self.generated = false;
    }

    fn takeConfig(self: *@This()) db_mod.enrichment_runtime.Config {
        const owned = self.config();
        self.forgetTransferred();
        return owned;
    }
};

pub fn chunkProviderForRuntime(
    alloc: std.mem.Allocator,
    provider: ?managed_embedder.AntflyProvider,
    io: ?std.Io,
    remote_capability_cache: ?*remote_capabilities.Cache,
    inference_api_url: ?[]const u8,
    source_table: []const u8,
) !?db_mod.enrichment_runtime.ChunkProvider {
    const resolved_cache = if (provider) |resolved|
        resolved.remote_capability_cache orelse remote_capability_cache
    else
        remote_capability_cache;
    const callback = if (provider) |resolved| resolved.chunk_input else null;
    const context_callback = if (provider) |resolved| resolved.chunk_input_with_context else null;
    if (callback == null and context_callback == null and io == null and resolved_cache == null and inference_api_url == null)
        return null;

    var result = db_mod.enrichment_runtime.ChunkProvider{
        .execution = .{
            .default_endpoint = inference_api_url,
            .capability_cache = resolved_cache,
            .io = io,
            .routing = .{ .source_table = source_table },
        },
    };
    if (provider) |resolved| if (callback) |chunk_input| {
        result.ptr = resolved.ptr;
        result.boundary_dispatch = resolved.boundary_dispatch;
        result.chunk_input_callback = @ptrCast(chunk_input);
    };
    if (provider) |resolved| if (context_callback) |chunk_input| {
        result.ptr = resolved.ptr;
        result.boundary_dispatch = resolved.boundary_dispatch;
        result.chunk_input_with_context_callback = @ptrCast(chunk_input);
    };
    var owned = try result.ownExecutionStrings(alloc);
    errdefer owned.deinit();
    if (io) |runtime_io| try owned.attachOwnedHttpClient(runtime_io);
    return owned;
}

pub fn createManagedDbEnrichments(
    allocator: std.mem.Allocator,
    raw_indexes_json: []const u8,
    runtime: ?*db_mod.background_runtime.BackendRuntime,
    local_provider: ?managed_embedder.AntflyProvider,
    remote_capability_cache: ?*remote_capabilities.Cache,
    inference_api_url: ?[]const u8,
    source_table: []const u8,
    store: ?*common_secrets.FileStore,
    remote: ?*const scraping.RemoteContentConfig,
) !ManagedDbEnrichmentSet {
    // Managed HTTP providers use a concurrent watchdog to enforce the
    // whole-request deadline. Reuse the table's backend executor so write
    // enrichment never falls back to the process-global single-threaded I/O,
    // where starting that watchdog correctly returns ConcurrencyUnavailable.
    const managed_io = if (runtime) |backend| backend.io() else null;
    var result = ManagedDbEnrichmentSet{};
    errdefer result.deinit(allocator);
    result.asset_runtime = if (try indexesJsonNeedsAssetProducer(allocator, raw_indexes_json)) blk: {
        const io = managed_io orelse return error.MissingBackendRuntimeIo;
        break :blk try asset_producer_runtime.Runtime.createOwned(allocator, io, .{
            .antfly_provider = local_provider,
            .inference_api_url = inference_api_url,
            .secret_store = store,
            .remote_capability_cache = remote_capability_cache,
            .source_table = source_table,
        });
    } else null;
    result.dense = try managed_embedder.ManagedEmbedder.createDenseEmbedderWithOptions(allocator, raw_indexes_json, .{ .antfly_provider = local_provider, .remote_capability_cache = remote_capability_cache, .io = managed_io, .bounded_http_request = managed_io != null, .secret_store = store, .remote_content = remote, .inference_api_url = inference_api_url, .source_table = source_table });
    result.sparse = try managed_embedder.ManagedEmbedder.createSparseEmbedderWithOptions(allocator, raw_indexes_json, .{ .antfly_provider = local_provider, .remote_capability_cache = remote_capability_cache, .io = managed_io, .bounded_http_request = managed_io != null, .secret_store = store, .remote_content = remote, .inference_api_url = inference_api_url, .source_table = source_table });
    result.chunk_provider = try chunkProviderForRuntime(
        allocator,
        local_provider,
        managed_io,
        remote_capability_cache,
        inference_api_url,
        source_table,
    );
    result.generated = try indexesJsonHasGeneratedEnrichment(allocator, raw_indexes_json);
    return result;
}

pub fn reconfigureManagedDbEnrichmentRuntime(
    _: std.mem.Allocator,
    db: *db_mod.DB,
    indexes_json: []const u8,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    antfly_provider: ?managed_embedder.AntflyProvider,
    remote_capability_cache: ?*remote_capabilities.Cache,
    inference_api_url: ?[]const u8,
    source_table: []const u8,
    secret_store: ?*common_secrets.FileStore,
    remote_content: ?*const scraping.RemoteContentConfig,
) !void {
    var enrichments = try createManagedDbEnrichments(
        db.runtime_alloc,
        indexes_json,
        backend_runtime,
        antfly_provider,
        remote_capability_cache,
        inference_api_url,
        source_table,
        secret_store,
        remote_content,
    );
    defer enrichments.deinit(db.runtime_alloc);
    // An empty replacement is meaningful: dropping the last managed producer
    // must retire the old provider instead of leaving an unused runtime alive.
    try db.reconfigureEnrichmentRuntime(enrichments.takeConfig());
}

pub fn managedIndexBackends(
    source: ?db_mod.DenseNativeMigrationPolicySource,
) @TypeOf((db_mod.OpenOptions{}).index_backends) {
    return .{ .dense_native_migration_policy_source = source };
}

pub fn openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentityWithOptions(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: []const u8,
    lsm_cache: ?*lsm_backend.Cache,
    hbc_cache: ?*hbc_mod.Cache,
    lsm_root_generation: u64,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    mode: ManagedDbOpenMode,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    antfly_provider: ?managed_embedder.AntflyProvider,
    secret_store: ?*common_secrets.FileStore,
    remote_content: ?*const scraping.RemoteContentConfig,
    identity_namespace: ?doc_identity.Namespace,
    options: ManagedDbOpenOptions,
) !db_mod.DB {
    const reconcile_mode: ManagedDbOpenMode = if (options.reconcile_for_replicated_apply) .restore_repair else mode;
    var reconcile_options = options;
    if (options.reconcile_for_replicated_apply) {
        reconcile_options.ha_write_gate = null;
        reconcile_options.ha_async_effect_mirror = null;
        reconcile_options.ha_async_batch_mirror = null;
        reconcile_options.ha_async_metadata_mirror = null;
    }
    var enrichments = try createManagedDbEnrichments(alloc, indexes_json, backend_runtime, antfly_provider, options.remote_capability_cache, options.inference_api_url, options.source_table, secret_store, remote_content);
    // takeConfig() clears every transferred owner. An unconditional defer is
    // therefore both the success-path cleanup for an unused provider-only set
    // and the error-path cleanup for a partially constructed replacement.
    defer enrichments.deinit(alloc);

    const openDb = struct {
        fn run(
            allocator: std.mem.Allocator,
            db_path: []const u8,
            enrichment_cfg: ?db_mod.enrichment_runtime.Config,
            cache: ?*lsm_backend.Cache,
            vector_cache: ?*hbc_mod.Cache,
            root_generation: u64,
            manager: ?*resource_manager_mod.ResourceManager,
            open_mode: ManagedDbOpenMode,
            runtime: ?*db_mod.background_runtime.BackendRuntime,
            store: ?*common_secrets.FileStore,
            remote: ?*const scraping.RemoteContentConfig,
            namespace: ?doc_identity.Namespace,
            open_options: ManagedDbOpenOptions,
        ) !db_mod.DB {
            const schema_before_index_load: ?db_mod.SchemaBeforeIndexLoad = if (open_mode == .query_readonly or open_mode == .status_only) null else if (open_options.schema_json_before_index_load) |schema_json| blk: {
                if (schema_json.len == 0) break :blk null;
                var parsed_schema = try tables_api.parseValidatedTableSchema(allocator, schema_json);
                defer parsed_schema.deinit(allocator);
                break :blk .{
                    .runtime_schema = try tables_api.deriveRuntimeTableSchema(allocator, parsed_schema),
                    .public_schema_json = schema_json,
                };
            } else null;
            defer if (schema_before_index_load) |schema| storage_schema.freeSchema(allocator, schema.runtime_schema);

            if (open_options.native_restore_open_plan) |native_plan| {
                if (open_mode != .restore_repair) return error.InvalidNativeRestoreOpenMode;
                const staged_generation = open_options.staged_generation orelse
                    return error.InvalidGenerationTransition;
                var resolved = try native_plan.optionsForStagedGeneration(staged_generation);
                // The plan owns every storage/root decision. These overlays
                // are request-scoped runtime policy or move-only providers and
                // cannot redirect candidate I/O outside the staged generation.
                resolved.secret_store = store;
                resolved.remote_content = remote;
                resolved.identity_namespace = namespace;
                resolved.prefer_existing_identity_namespace = namespace != null;
                resolved.enrichment = enrichment_cfg;
                resolved.ha_write_gate = open_options.ha_write_gate;
                resolved.ha_async_effect_mirror = null;
                resolved.ha_async_batch_mirror = null;
                resolved.ha_async_metadata_mirror = null;
                resolved.schema_before_index_load = schema_before_index_load;
                resolved.open_mode = .writer_no_replay;
                resolved.start_index_workers = false;
                resolved.start_optional_runtimes = enrichment_cfg != null;
                resolved.start_optional_runtime_workers = false;
                resolved.ttl_cleanup = .{ .enabled = false };
                resolved.transaction_recovery = .{ .enabled = false };
                resolved.text_merge = .{ .enabled = false };
                resolved.index_backends.dense_native_migration_policy_source = open_options.dense_native_migration_policy_source;
                return try db_mod.DB.open(allocator, db_path, resolved);
            }

            const base: db_mod.OpenOptions = .{
                .lsm_cache = cache,
                .hbc_cache = vector_cache,
                .index_backends = managedIndexBackends(open_options.dense_native_migration_policy_source),
                .lsm_root_generation = root_generation,
                .resource_manager = manager,
                .backend_runtime = runtime,
                .secret_store = store,
                .remote_content = remote,
                .identity_namespace = namespace,
                .prefer_existing_identity_namespace = namespace != null,
                .enrichment = enrichment_cfg,
                .ha_write_gate = open_options.ha_write_gate,
                .ha_async_effect_mirror = open_options.ha_async_effect_mirror,
                .ha_async_batch_mirror = open_options.ha_async_batch_mirror,
                .ha_async_metadata_mirror = open_options.ha_async_metadata_mirror,
                .transaction_recovery = open_options.transaction_recovery,
                .schema_before_index_load = schema_before_index_load,
                .start_resolver_workers = !open_options.defer_resolver_workers,
            };
            return switch (open_mode) {
                .default => if (enrichment_cfg != null)
                    try db_mod.DB.open(allocator, db_path, base)
                else
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .index_backends = managedIndexBackends(open_options.dense_native_migration_policy_source),
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .ha_write_gate = open_options.ha_write_gate,
                        .ha_async_effect_mirror = open_options.ha_async_effect_mirror,
                        .ha_async_batch_mirror = open_options.ha_async_batch_mirror,
                        .ha_async_metadata_mirror = open_options.ha_async_metadata_mirror,
                        .transaction_recovery = open_options.transaction_recovery,
                        .schema_before_index_load = schema_before_index_load,
                        .start_resolver_workers = !open_options.defer_resolver_workers,
                    }),
                .default_async, .writer_no_replay => if (enrichment_cfg != null)
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .index_backends = managedIndexBackends(open_options.dense_native_migration_policy_source),
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .enrichment = enrichment_cfg,
                        .ha_write_gate = open_options.ha_write_gate,
                        .ha_async_effect_mirror = open_options.ha_async_effect_mirror,
                        .ha_async_batch_mirror = open_options.ha_async_batch_mirror,
                        .ha_async_metadata_mirror = open_options.ha_async_metadata_mirror,
                        .transaction_recovery = open_options.transaction_recovery,
                        .schema_before_index_load = schema_before_index_load,
                        .start_resolver_workers = !open_options.defer_resolver_workers,
                        .open_mode = .writer_no_replay,
                        // The managed write cache opens DBs synchronously while
                        // table/index metadata can still be settling. Keep
                        // index catalog opens serial on this no-replay path
                        // until the parallel index opener is allocator-safe.
                        .index_open_parallelism = 1,
                    })
                else
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .index_backends = managedIndexBackends(open_options.dense_native_migration_policy_source),
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .ha_write_gate = open_options.ha_write_gate,
                        .ha_async_effect_mirror = open_options.ha_async_effect_mirror,
                        .ha_async_batch_mirror = open_options.ha_async_batch_mirror,
                        .ha_async_metadata_mirror = open_options.ha_async_metadata_mirror,
                        .transaction_recovery = open_options.transaction_recovery,
                        .schema_before_index_load = schema_before_index_load,
                        .start_resolver_workers = !open_options.defer_resolver_workers,
                        .open_mode = .writer_no_replay,
                        .index_open_parallelism = 1,
                    }),
                .startup_catch_up => try db_mod.DB.open(allocator, db_path, .{
                    .lsm_cache = cache,
                    .hbc_cache = vector_cache,
                    .index_backends = managedIndexBackends(open_options.dense_native_migration_policy_source),
                    .lsm_root_generation = root_generation,
                    .resource_manager = manager,
                    .backend_runtime = runtime,
                    .secret_store = store,
                    .remote_content = remote,
                    .identity_namespace = namespace,
                    .prefer_existing_identity_namespace = namespace != null,
                    .ha_write_gate = open_options.ha_write_gate,
                    .schema_before_index_load = schema_before_index_load,
                    .start_resolver_workers = !open_options.defer_resolver_workers,
                    .open_mode = .writer_no_replay,
                    .start_index_workers = false,
                    .enrichment = if (enrichment_cfg) |configured| blk: {
                        var bounded = configured;
                        // Loaded-state startup must not wait through the
                        // normal external-provider inline retry budget. One
                        // attempt records durable retry state; the startup
                        // scheduler owns later attempts.
                        bounded.inline_retry_max_attempts = 1;
                        // Startup is a short-lived foreground owner. If a
                        // retry budget was nearly exhausted before shutdown,
                        // do not convert one startup probe into terminal
                        // coverage before the long-lived worker can resume it.
                        // The persisted attempt count is retained, so the
                        // normal worker still enforces its bounded budget.
                        bounded.worker_retry_max_attempts = std.math.maxInt(u32);
                        break :blk bounded;
                    } else null,
                    // Startup catch-up drives enrichment synchronously below.
                    // Construct the runtime with metadata-owned providers, but
                    // do not leave workers attached to this short-lived owner.
                    .start_optional_runtimes = enrichment_cfg != null,
                    .start_optional_runtime_workers = false,
                    .ttl_cleanup = .{ .enabled = false },
                    .transaction_recovery = .{ .enabled = false },
                    .text_merge = .{ .enabled = false },
                    .staged_generation = open_options.staged_generation,
                }),
                .restore_repair => if (enrichment_cfg != null)
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .index_backends = managedIndexBackends(open_options.dense_native_migration_policy_source),
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .enrichment = enrichment_cfg,
                        .ha_write_gate = open_options.ha_write_gate,
                        .schema_before_index_load = schema_before_index_load,
                        .start_resolver_workers = !open_options.defer_resolver_workers,
                        .open_mode = .writer_no_replay,
                        .start_index_workers = false,
                        .start_optional_runtime_workers = false,
                        .ttl_cleanup = .{ .enabled = false },
                        .transaction_recovery = .{ .enabled = false },
                        .text_merge = .{ .enabled = false },
                        .staged_generation = open_options.staged_generation,
                    })
                else
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .index_backends = managedIndexBackends(open_options.dense_native_migration_policy_source),
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .ha_write_gate = open_options.ha_write_gate,
                        .schema_before_index_load = schema_before_index_load,
                        .start_resolver_workers = !open_options.defer_resolver_workers,
                        .open_mode = .writer_no_replay,
                        .start_index_workers = false,
                        .start_optional_runtimes = false,
                        .ttl_cleanup = .{ .enabled = false },
                        .transaction_recovery = .{ .enabled = false },
                        .text_merge = .{ .enabled = false },
                        .staged_generation = open_options.staged_generation,
                    }),
                .query_readonly => if (enrichment_cfg != null)
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .index_backends = managedIndexBackends(open_options.dense_native_migration_policy_source),
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .enrichment = enrichment_cfg,
                        .ha_write_gate = open_options.ha_write_gate,
                        .open_mode = .query_readonly,
                        .start_index_workers = false,
                        .ttl_cleanup = .{ .enabled = false },
                        .transaction_recovery = .{ .enabled = false },
                        .text_merge = .{ .enabled = false },
                    })
                else
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .index_backends = managedIndexBackends(open_options.dense_native_migration_policy_source),
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .ha_write_gate = open_options.ha_write_gate,
                        .open_mode = .query_readonly,
                        .start_index_workers = false,
                        .ttl_cleanup = .{ .enabled = false },
                        .transaction_recovery = .{ .enabled = false },
                        .text_merge = .{ .enabled = false },
                    }),
                .status_only => if (enrichment_cfg != null)
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .index_backends = managedIndexBackends(open_options.dense_native_migration_policy_source),
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .enrichment = enrichment_cfg,
                        .ha_write_gate = open_options.ha_write_gate,
                        .open_mode = .status_only,
                        .start_index_workers = false,
                        .ttl_cleanup = .{ .enabled = false },
                        .transaction_recovery = .{ .enabled = false },
                        .text_merge = .{ .enabled = false },
                    })
                else
                    try db_mod.DB.open(allocator, db_path, .{
                        .lsm_cache = cache,
                        .hbc_cache = vector_cache,
                        .index_backends = managedIndexBackends(open_options.dense_native_migration_policy_source),
                        .lsm_root_generation = root_generation,
                        .resource_manager = manager,
                        .backend_runtime = runtime,
                        .secret_store = store,
                        .remote_content = remote,
                        .identity_namespace = namespace,
                        .prefer_existing_identity_namespace = namespace != null,
                        .ha_write_gate = open_options.ha_write_gate,
                        .open_mode = .status_only,
                        .start_index_workers = false,
                        .ttl_cleanup = .{ .enabled = false },
                        .transaction_recovery = .{ .enabled = false },
                        .text_merge = .{ .enabled = false },
                    }),
            };
        }
    }.run;

    var db = blk: {
        const enrichment_cfg = if (enrichments.enabled()) enrichments.takeConfig() else null;
        const opened = try openDb(alloc, path, enrichment_cfg, lsm_cache, hbc_cache, lsm_root_generation, resource_manager, reconcile_mode, backend_runtime, secret_store, remote_content, identity_namespace, reconcile_options);
        break :blk opened;
    };
    var db_open = true;
    errdefer if (db_open) db.close();

    try validateProvisionedDbIdentityNamespaceWithPolicy(
        identity_namespace,
        options.identity_validation,
        &db,
    );
    if (mode == .status_only or mode == .query_readonly) return db;

    if ((mode == .startup_catch_up or mode == .restore_repair) and db.core.index_manager.hasLoadFailures()) {
        // Startup maintenance must preserve the failed generation so status
        // publication and repair discovery can report the original failure.
        // Catalog reconciliation is a structural-owner responsibility; doing
        // it here could delete a quarantined index and immediately collide
        // with the durable same-name cleanup fence.
        return db;
    }

    const index_reconcile_options: metadata_table_provisioner.ReconcileDbIndexOptions = .{
        .drain_resolver_backfill = options.drain_resolver_backfill,
        .embedding_options = .{
            .antfly_provider = antfly_provider,
            .inference_api_url = options.inference_api_url,
        },
        .source_table = options.source_table,
        .destination_authorizer = options.destination_authorizer,
    };
    const summary = if (options.reconcile_target_index_name) |target_index_name|
        try metadata_table_provisioner.reconcileDbIndexTargetWithOptions(alloc, &db, indexes_json, target_index_name, index_reconcile_options)
    else
        try metadata_table_provisioner.reconcileDbIndexesWithOptions(alloc, &db, indexes_json, index_reconcile_options);
    if (summary.indexManagerCatalogChanged() or options.reconcile_for_replicated_apply) {
        // First-open provisioning can mutate the live index manager. Reopen so
        // request work runs against the stabilized post-reconcile state.
        db.close();
        db_open = false;
        // `enrichments` is a move-only owner. The initial open consumes and
        // clears it when producers are enabled, but a provider-only set keeps
        // its chunk provider until this scope releases it. Do that before
        // replacing the aggregate; assignment would otherwise discard its
        // owned routing strings and HTTP client.
        enrichments.deinit(alloc);
        enrichments = try createManagedDbEnrichments(
            alloc,
            indexes_json,
            backend_runtime,
            antfly_provider,
            options.remote_capability_cache,
            options.inference_api_url,
            options.source_table,
            secret_store,
            remote_content,
        );
        db = blk: {
            const enrichment_cfg = if (enrichments.enabled()) enrichments.takeConfig() else null;
            const opened = try openDb(alloc, path, enrichment_cfg, lsm_cache, hbc_cache, lsm_root_generation, resource_manager, mode, backend_runtime, secret_store, remote_content, identity_namespace, options);
            break :blk opened;
        };
        db_open = true;
        try validateProvisionedDbIdentityNamespaceWithPolicy(
            identity_namespace,
            options.identity_validation,
            &db,
        );
    }

    // Metadata-driven index reconciliation happens during open/reopen rather
    // than through DB.addIndex(), so managed enrichment replay must be re-armed
    // here for pre-existing documents after an index is added.
    if ((mode == .default or mode == .default_async) and summary.indexes_added > 0) {
        if (db.enrichment_runtime != null) {
            _ = try db.replayGeneratedEnrichmentsFromStoredDocs(alloc);
        }
    }
    return db;
}

pub fn loadLocalTableSchemaJson(alloc: std.mem.Allocator, db: *db_mod.DB) !?[]u8 {
    return db.core.store.get(alloc, local_schema_json_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}

pub fn freeOwnedBatchWrites(alloc: std.mem.Allocator, writes: []const db_mod.types.BatchWrite) void {
    for (writes) |write| {
        alloc.free(@constCast(write.key));
        alloc.free(@constCast(write.value));
    }
    if (writes.len > 0) alloc.free(@constCast(writes));
}

pub const SchemaValidationWriteState = struct {
    const Kind = enum { write, delete };

    const Entry = struct {
        key: []u8,
        kind: Kind,
        value: ?[]u8 = null,

        pub fn deinit(self: *Entry, alloc: std.mem.Allocator) void {
            alloc.free(self.key);
            if (self.value) |value| alloc.free(value);
            self.* = undefined;
        }
    };

    entries: std.ArrayListUnmanaged(Entry) = .empty,

    pub fn deinit(self: *SchemaValidationWriteState, alloc: std.mem.Allocator) void {
        for (self.entries.items) |*entry| entry.deinit(alloc);
        self.entries.deinit(alloc);
        self.* = undefined;
    }

    fn findIndex(self: *const SchemaValidationWriteState, key: []const u8) ?usize {
        for (self.entries.items, 0..) |entry, i| {
            if (std.mem.eql(u8, entry.key, key)) return i;
        }
        return null;
    }

    fn applyBorrowedWrite(self: *SchemaValidationWriteState, alloc: std.mem.Allocator, key: []const u8, value: []const u8) !void {
        const owned_value = try alloc.dupe(u8, value);
        errdefer alloc.free(owned_value);
        if (self.findIndex(key)) |idx| {
            const entry = &self.entries.items[idx];
            if (entry.value) |old_value| alloc.free(old_value);
            entry.kind = .write;
            entry.value = owned_value;
            return;
        }

        const owned_key = try alloc.dupe(u8, key);
        errdefer alloc.free(owned_key);
        try self.entries.append(alloc, .{
            .key = owned_key,
            .kind = .write,
            .value = owned_value,
        });
    }

    fn applyDelete(self: *SchemaValidationWriteState, alloc: std.mem.Allocator, key: []const u8) !void {
        if (self.findIndex(key)) |idx| {
            const entry = &self.entries.items[idx];
            if (entry.value) |old_value| alloc.free(old_value);
            entry.kind = .delete;
            entry.value = null;
            return;
        }

        const owned_key = try alloc.dupe(u8, key);
        errdefer alloc.free(owned_key);
        try self.entries.append(alloc, .{
            .key = owned_key,
            .kind = .delete,
        });
    }

    fn applyOwnedWrite(self: *SchemaValidationWriteState, alloc: std.mem.Allocator, key: []const u8, value: []u8) !void {
        if (self.findIndex(key)) |idx| {
            const entry = &self.entries.items[idx];
            if (entry.value) |old_value| alloc.free(old_value);
            entry.kind = .write;
            entry.value = value;
            return;
        }

        const owned_key = try alloc.dupe(u8, key);
        errdefer alloc.free(owned_key);
        try self.entries.append(alloc, .{
            .key = owned_key,
            .kind = .write,
            .value = value,
        });
    }

    fn baseValue(self: *const SchemaValidationWriteState, key: []const u8) ?[]const u8 {
        const idx = self.findIndex(key) orelse return null;
        const entry = self.entries.items[idx];
        return switch (entry.kind) {
            .write => entry.value.?,
            .delete => null,
        };
    }

    fn hasRequestState(self: *const SchemaValidationWriteState, key: []const u8) bool {
        return self.findIndex(key) != null;
    }

    fn toOwnedWrites(self: *const SchemaValidationWriteState, alloc: std.mem.Allocator) ![]db_mod.types.BatchWrite {
        var count: usize = 0;
        for (self.entries.items) |entry| {
            if (entry.kind == .write) count += 1;
        }

        var out = try alloc.alloc(db_mod.types.BatchWrite, count);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |write| {
                alloc.free(@constCast(write.key));
                alloc.free(@constCast(write.value));
            }
            alloc.free(out);
        }

        var i: usize = 0;
        for (self.entries.items) |entry| {
            if (entry.kind != .write) continue;
            {
                const owned_key = try alloc.dupe(u8, entry.key);
                errdefer alloc.free(owned_key);
                const owned_value = try alloc.dupe(u8, entry.value.?);
                errdefer alloc.free(owned_value);
                out[i] = .{
                    .key = owned_key,
                    .value = owned_value,
                };
            }
            filled += 1;
            i += 1;
        }
        return out;
    }
};

pub fn portableBackupShardRelPath(alloc: std.mem.Allocator, backup_id: []const u8, group_id: u64) ![]u8 {
    if (group_id == 0) return try std.fmt.allocPrint(alloc, "{s}.afb", .{backup_id});
    return try std.fmt.allocPrint(alloc, "{s}/groups/{d}.afb", .{ backup_id, group_id });
}

pub fn exportPortableBackupShard(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    backup_root: []const u8,
    backup_id: []const u8,
    group_id: u64,
    shared_io: ?std.Io,
) ![]backups_api.ShardSnapshot {
    const rel_path = try portableBackupShardRelPath(alloc, backup_id, group_id);
    errdefer alloc.free(rel_path);

    const dest_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ backup_root, rel_path });
    defer alloc.free(dest_path);
    try exportPortableBackupFile(alloc, db.core.store, dest_path, shared_io);

    const byte_range = db.getRange();
    const shards = try alloc.alloc(backups_api.ShardSnapshot, 1);
    shards[0] = .{
        .group_id = group_id,
        .start_key = try alloc.dupe(u8, byte_range.start),
        .end_key = if (byte_range.end.len > 0) try alloc.dupe(u8, byte_range.end) else null,
        .snapshot_path = rel_path,
    };
    errdefer shards[0].deinit(alloc);
    try backups_api.populateShardArtifactIntegrity(alloc, shared_io, .portable, dest_path, &shards[0]);
    return shards;
}

pub const NativeBackupShardSnapshot = struct {
    snapshot_root: []const u8,
    snapshot_attempt: NativeSnapshotAttempt,
    dest_root: []const u8,
    io: std.Io,
    cancellation: db_mod.types.CancellationToken,
    shard: backups_api.ShardSnapshot,
    owns_shard: bool = true,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        deleteLocalNativeSnapshot(self.io, self.snapshot_root);
        self.snapshot_attempt.deinit();
        alloc.free(@constCast(self.snapshot_root));
        alloc.free(@constCast(self.dest_root));
        if (self.owns_shard) self.shard.deinit(alloc);
        self.* = undefined;
    }

    pub fn toShardsAlloc(self: *@This(), alloc: std.mem.Allocator) ![]backups_api.ShardSnapshot {
        const shards = try alloc.alloc(backups_api.ShardSnapshot, 1);
        shards[0] = self.shard;
        self.owns_shard = false;
        return shards;
    }
};

pub fn deleteLocalNativeSnapshot(io: std.Io, snapshot_root: []const u8) void {
    std.Io.Dir.cwd().deleteTree(io, snapshot_root) catch |err| {
        std.log.warn("failed to remove exported native snapshot staging root={s} err={s}", .{ snapshot_root, @errorName(err) });
    };
    if (std.fs.path.dirname(snapshot_root)) |parent| {
        fs_paths.syncDirPortable(io, parent) catch |err| {
            std.log.warn("failed to sync exported native snapshot deletion root={s} err={s}", .{ snapshot_root, @errorName(err) });
        };
    }
}

pub fn prepareNativeBackupShardSnapshot(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    db_path: []const u8,
    group_id: u64,
    plan: backups_api.TableBackupPlan,
) !NativeBackupShardSnapshot {
    std.debug.assert(plan.format == .native);

    const snapshot_io = plan.io orelse db.backend_runtime.filesystemIo() orelse
        return error.BackendRuntimeIoUnavailable;
    try reclaimStaleNativeSnapshotAttempts(alloc, snapshot_io, db_path);
    const group_label = try std.fmt.allocPrint(alloc, "g{d}", .{group_id});
    defer alloc.free(group_label);
    const snapshot_token = try nativeSnapshotAttemptTokenAlloc(alloc, snapshot_io, plan.backup_id, group_label);
    defer alloc.free(snapshot_token);
    var snapshot_attempt = try createNativeSnapshotAttemptMarker(
        alloc,
        snapshot_io,
        db_path,
        snapshot_token,
        platform_time.realtimeNs(),
    );
    errdefer snapshot_attempt.deinit();
    _ = try db.snapshotNativeWithCancellation(snapshot_token, plan.cancellation);

    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/{s}", .{ db_path, snapshot_token });
    errdefer alloc.free(snapshot_root);
    const dest_root = try backups_api.shardSnapshotPath(alloc, plan.backup_root, plan.backup_id, group_id);
    errdefer alloc.free(dest_root);
    const rel_path = try backups_api.shardSnapshotRelPath(alloc, plan.backup_id, group_id);
    errdefer alloc.free(rel_path);

    const byte_range = db.getRange();
    const start_key = try alloc.dupe(u8, byte_range.start);
    errdefer alloc.free(start_key);
    const end_key = if (byte_range.end.len > 0) try alloc.dupe(u8, byte_range.end) else null;
    errdefer if (end_key) |value| alloc.free(value);

    return .{
        .snapshot_root = snapshot_root,
        .snapshot_attempt = snapshot_attempt,
        .dest_root = dest_root,
        .io = snapshot_io,
        .cancellation = plan.cancellation,
        .shard = .{
            .group_id = group_id,
            .start_key = start_key,
            .end_key = end_key,
            .snapshot_path = rel_path,
        },
    };
}

pub fn copyPreparedNativeBackupShardSnapshot(
    alloc: std.mem.Allocator,
    native_snapshot: *NativeBackupShardSnapshot,
) ![]backups_api.ShardSnapshot {
    runTestBeforeNativeBackupCopyHook();
    var integrity = try backups_api.copyNativeDirectoryWithIntegrityUsingIo(
        alloc,
        native_snapshot.io,
        native_snapshot.snapshot_root,
        native_snapshot.dest_root,
        native_snapshot.cancellation,
    );
    native_snapshot.shard.artifact_size_bytes = integrity.size_bytes;
    native_snapshot.shard.artifact_sha256 = integrity.sha256;
    integrity = undefined;
    var native_manifest_integrity = try backups_api.nativeGenerationManifestIntegrityAllocWithCancellation(
        alloc,
        native_snapshot.io,
        native_snapshot.dest_root,
        native_snapshot.cancellation,
    );
    native_snapshot.shard.native_manifest_size_bytes = native_manifest_integrity.size_bytes;
    native_snapshot.shard.native_manifest_sha256 = native_manifest_integrity.sha256;
    native_manifest_integrity = undefined;
    return try native_snapshot.toShardsAlloc(alloc);
}

pub fn backupStorageKernelOwnerDb(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    db_path: []const u8,
    group_id: u64,
    backup_root: []const u8,
    backup_id: []const u8,
    format: backups_api.BackupFormat,
) ![]backups_api.ShardSnapshot {
    const plan: backups_api.TableBackupPlan = .{
        .backup_root = backup_root,
        .backup_id = backup_id,
        .format = format,
        // std.Io is intentionally not an ABI type. The storage unit creates
        // and owns the filesystem scheduler used by this coarse operation.
        .io = null,
    };
    if (format == .portable)
        return try exportPortableBackupShard(alloc, db, backup_root, backup_id, group_id, null);
    var native_snapshot = try prepareNativeBackupShardSnapshot(
        alloc,
        db,
        db_path,
        group_id,
        plan,
    );
    defer native_snapshot.deinit(alloc);
    return try copyPreparedNativeBackupShardSnapshot(alloc, &native_snapshot);
}

pub fn exportPortableBackupFile(alloc: std.mem.Allocator, store: *db_mod.docstore.DocStore, path: []const u8, shared_io: ?std.Io) !void {
    if (shared_io) |io| return try exportPortableBackupFileWithIo(alloc, store, path, io);
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    return try exportPortableBackupFileWithIo(alloc, store, path, io_impl.io());
}

pub fn exportPortableBackupFileWithIo(alloc: std.mem.Allocator, store: *db_mod.docstore.DocStore, path: []const u8, io: std.Io) !void {
    if (std.fs.path.dirname(path)) |parent| try fs_paths.createDirPathPortable(io, parent);
    const tmp_path = try std.fmt.allocPrint(alloc, "{s}.tmp-{d}", .{ path, platform_time.monotonicNs() });
    defer alloc.free(tmp_path);
    errdefer if (std.fs.path.isAbsolute(tmp_path))
        std.Io.Dir.deleteFileAbsolute(io, tmp_path) catch {}
    else
        std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};
    var file = try fs_paths.createFilePortable(io, tmp_path, .{ .truncate = true });
    var file_open = true;
    defer if (file_open) file.close(io);
    const spool_path = try std.fmt.allocPrint(alloc, "{s}.spool", .{tmp_path});
    defer alloc.free(spool_path);
    defer if (std.fs.path.isAbsolute(spool_path))
        std.Io.Dir.deleteFileAbsolute(io, spool_path) catch {}
    else
        std.Io.Dir.cwd().deleteFile(io, spool_path) catch {};
    var spool_file = try fs_paths.createFilePortable(io, spool_path, .{ .read = true, .truncate = true });
    defer spool_file.close(io);
    var buf: [64 * 1024]u8 = undefined;
    var writer = file.writer(io, &buf);
    try portable_backup.exportPortableToWriterWithOptions(alloc, store, &writer.interface, .{
        .spool = .{ .io = io, .file = spool_file },
    });
    try writer.end();
    try file.sync(io);
    file.close(io);
    file_open = false;
    if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.renameAbsolute(tmp_path, path, io)
    else
        try std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io);
}

pub fn resolveWritesForSchemaValidation(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    base_writes: []const db_mod.types.BatchWrite,
    deletes: []const []const u8,
    transforms: []const db_mod.types.DocumentTransform,
) ![]db_mod.types.BatchWrite {
    var state = SchemaValidationWriteState{};
    defer state.deinit(alloc);

    for (base_writes) |write| {
        try state.applyBorrowedWrite(alloc, write.key, write.value);
    }

    for (deletes) |key| {
        try state.applyDelete(alloc, key);
    }

    for (transforms) |transform| {
        const has_request_state = state.hasRequestState(transform.key);
        const existing_from_request = state.baseValue(transform.key);
        const existing_from_db = if (!has_request_state) try db.get(alloc, transform.key) else null;
        defer if (existing_from_db) |body| alloc.free(body);
        const existing = existing_from_request orelse existing_from_db;
        const resolved = db_mod.transform.resolveDocumentTransform(alloc, existing, transform) catch |err| switch (err) {
            error.InvalidArgument, error.UnsupportedTransformOperation => return error.InvalidBatchRequest,
            else => return err,
        } orelse continue;

        state.applyOwnedWrite(alloc, transform.key, resolved) catch |err| {
            alloc.free(resolved);
            return err;
        };
    }

    return try state.toOwnedWrites(alloc);
}

pub fn validateTableBatchAgainstLocalSchema(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    writes: []const db_mod.types.BatchWrite,
    deletes: []const []const u8,
    transforms: []const db_mod.types.DocumentTransform,
) !void {
    if (writes.len == 0 and deletes.len == 0 and transforms.len == 0) return;
    // Relational writes are validated authoritatively by DB.batch after its
    // final transform resolution and while holding the schema generation's
    // apply lock. Repeating the API-level load/parse/transform pass doubles
    // CPU and allocation cost without improving error timing: both paths are
    // synchronous and return InvalidBatchRequest before any durable mutation.
    if (db.usesRelationalStorage()) return;
    const schema_json = (try loadLocalTableSchemaJson(alloc, db)) orelse return;
    defer alloc.free(schema_json);
    if (schema_json.len == 0) return;

    const effective_writes = try resolveWritesForSchemaValidation(alloc, db, writes, deletes, transforms);
    defer freeOwnedBatchWrites(alloc, effective_writes);
    if (effective_writes.len == 0) return;

    var parsed_schema = try tables_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    try tables_api.validateWritesAgainstTableSchema(alloc, parsed_schema, effective_writes);
}

pub fn applyLocalTableSchemaJson(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    schema_json: []const u8,
) !void {
    // The absent/empty catalog contract has the same canonical schema as
    // table creation. Never overwrite only the public marker: its validator
    // and the durable runtime layout must be committed in the same epoch.
    const effective_schema_json = if (schema_json.len == 0) tables_api.default_schema_json else schema_json;
    // Install the public and runtime forms together so storage-boundary writes
    // immediately use the same authoritative validator as API writes.
    try db.setSchemaJson(alloc, effective_schema_json);
    // Propagate schema-derived changes to live algebraic indexes so dynamic
    // template updates take effect without a reopen.
    try db.reloadAlgebraicSchemaConfigs(effective_schema_json);
}

/// Installed producer configuration for a live compiled owner. Reconciliation
/// of unchanged catalog JSON must not cancel/join the same runtime again.
pub const OwnerManagedConfig = struct {
    fingerprint: ?[32]u8 = null,

    fn digest(indexes_json: []const u8) [32]u8 {
        var result: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(indexes_json, &result, .{});
        return result;
    }
    fn matches(self: *const @This(), indexes_json: []const u8) bool {
        const installed = self.fingerprint orelse return false;
        return std.mem.eql(u8, &installed, &digest(indexes_json));
    }
    fn publish(self: *@This(), indexes_json: []const u8) void {
        self.fingerprint = digest(indexes_json);
    }
};

pub fn prepareOwnerSchemaBeforeIndexLoad(alloc: std.mem.Allocator, schema_json: []const u8) !?db_mod.SchemaBeforeIndexLoad {
    if (schema_json.len == 0) return null;
    var parsed = try tables_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    return .{ .runtime_schema = try tables_api.deriveRuntimeTableSchema(alloc, parsed), .public_schema_json = schema_json };
}

pub fn freeOwnerSchemaBeforeIndexLoad(alloc: std.mem.Allocator, prepared: ?db_mod.SchemaBeforeIndexLoad) void {
    if (prepared) |schema| storage_schema.freeSchema(alloc, schema.runtime_schema);
}

pub fn configureStorageKernelOwnerDb(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    table_name: []const u8,
    schema_json: []const u8,
    indexes_json: []const u8,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    antfly_provider: ?managed_embedder.AntflyProvider,
    remote_content: ?*const scraping.RemoteContentConfig,
    installed: ?*OwnerManagedConfig,
) !void {
    if (schema_json.len > 0) try applyLocalTableSchemaJson(alloc, db, schema_json);
    if (indexes_json.len > 0) {
        const replace = backend_runtime != null and !(if (installed) |state| state.matches(indexes_json) else false);
        if (replace) try reconfigureManagedDbEnrichmentRuntimePaused(
            alloc,
            db,
            indexes_json,
            backend_runtime,
            antfly_provider,
            null,
            null,
            table_name,
            null,
            remote_content,
        );
        _ = try metadata_table_provisioner.reconcileDbIndexesWithOptions(alloc, db, indexes_json, .{
            .drain_resolver_backfill = false,
        });
        if (replace) try db.resumeEnrichmentRuntimeAfterReconfigure("owner configuration", "*");
        if (installed) |state| state.publish(indexes_json);
    }
}

pub fn openStorageKernelRestoreDb(
    alloc: std.mem.Allocator,
    path: []const u8,
    indexes_json: []const u8,
    lsm_root_generation: u64,
    backend_runtime: *db_mod.background_runtime.BackendRuntime,
    identity_namespace: ?doc_identity.Namespace,
    prepared_restore: *const backup_restore.PreparedRestore,
) !db_mod.DB {
    return try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentityWithOptions(
        alloc,
        path,
        indexes_json,
        null,
        null,
        lsm_root_generation,
        null,
        .restore_repair,
        backend_runtime,
        null,
        null,
        null,
        identity_namespace,
        .{
            .staged_generation = prepared_restore.stagedGeneration(),
            .native_restore_open_plan = prepared_restore.nativeOpenPlan(),
        },
    );
}

pub fn repairStorageKernelRestoreDb(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    group_id: u64,
    schema_json: []const u8,
    indexes_json: []const u8,
    cancellation: db_mod.types.CancellationToken,
) !void {
    try configureStorageKernelOwnerDb(alloc, db, "", schema_json, indexes_json, null, null, null, null);
    const io = db.backend_runtime.filesystemIo() orelse std.Io.Threaded.global_single_threaded.io();
    var repair_cancellation = db_mod.types.RepairCancellation{ .token = cancellation };
    var attempts: usize = 0;
    std.log.info("storage-kernel restore repair begin group_id={d}", .{group_id});
    while (try db.restoreRuntimeRepairNeeded()) {
        try cancellation.check();
        attempts += 1;
        const repaired = db.repairRestoreRuntimeStateStepIfNeededWithIoAndRepairOptions(
            alloc,
            io,
            .{ .cancel_check = repair_cancellation.check() },
        ) catch |err| switch (err) {
            // A staged owner has no serving runtime to finish deferred index
            // work after this call. Drive its bounded repair steps until every
            // durable completion proof holds, preserving caller cancellation.
            error.RestoreRuntimeRepairIncomplete,
            error.RestoreDenseArtifactRebuildIncomplete,
            error.RestoreDenseConfigProofIncomplete,
            error.RestoreDenseCounterProofIncomplete,
            error.RestoreDenseIndexProofIncomplete,
            error.RestoreDenseCoverageProofIncomplete,
            error.RestoreDenseCheckpointIncomplete,
            error.RestoreIndexAvailabilityIncomplete,
            => blk: {
                const repair = try db.repairRecoverableStartupIndexFailures(alloc, 1, .{ .cancel_check = repair_cancellation.check() });
                if (repair.attempted == 0 and repair.repaired == 0) try io.sleep(.fromMilliseconds(100), .awake);
                break :blk repair.repaired != 0;
            },
            else => return err,
        };
        if (repaired) {
            db.clearDenseHbcCaches();
        }
    }
    try db.sync(true);
    try db.syncIndexes(true);
    std.log.info("storage-kernel restore repair complete group_id={d} attempts={d}", .{
        group_id,
        attempts,
    });
}

pub fn reconcileStorageKernelOwnerDb(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    table_name: []const u8,
    schema_json: []const u8,
    indexes_json: []const u8,
    target_index_name: ?[]const u8,
    advance_index_repair: bool,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    antfly_provider: ?managed_embedder.AntflyProvider,
    installed: ?*OwnerManagedConfig,
) !StorageKernelReconcileResult {
    if (target_index_name == null and schema_json.len > 0) try applyLocalTableSchemaJson(alloc, db, schema_json);
    const replace = indexes_json.len > 0 and backend_runtime != null and !(if (installed) |state| state.matches(indexes_json) else false);
    if (replace) try reconfigureManagedDbEnrichmentRuntimePaused(
        alloc,
        db,
        indexes_json,
        backend_runtime,
        antfly_provider,
        null,
        null,
        table_name,
        null,
        null,
    );
    const provisioned = if (indexes_json.len > 0) blk: {
        const options: metadata_table_provisioner.ReconcileDbIndexOptions = .{ .drain_resolver_backfill = false };
        break :blk if (target_index_name) |target|
            try metadata_table_provisioner.reconcileDbIndexTargetWithOptions(alloc, db, indexes_json, target, options)
        else
            try metadata_table_provisioner.reconcileDbIndexesWithOptions(alloc, db, indexes_json, options);
    } else metadata_table_provisioner.ProvisionSummary{};
    if (replace) try db.resumeEnrichmentRuntimeAfterReconfigure("owner reconciliation", target_index_name orelse "*");
    if (indexes_json.len > 0) if (installed) |state| state.publish(indexes_json);

    var result = StorageKernelReconcileResult{
        .indexes_added = provisioned.indexes_added,
        .indexes_removed = provisioned.indexes_removed,
        .indexes_pending = provisioned.indexes_pending,
    };
    if (provisioned.indexes_pending != 0) {
        _ = try db.advanceGeneratedArtifactCleanupPage(target_index_name);
    }

    // A published restore generation carries a durable phase marker whose
    // completion is part of table visibility. Advance exactly one phase per
    // structural pass so graph/artifact reconstruction remains bounded and
    // observable through the same compiled owner that serves the generation.
    // This work is independent of operator-controlled index-repair scheduling.
    if (try db.restoreRuntimeRepairNeeded()) {
        result.restore_repair_attempted = 1;
        const progressed = db.repairRestoreRuntimeStateStepIfNeeded(alloc) catch |err| switch (err) {
            error.RestoreRuntimeRepairIncomplete,
            error.RestoreDenseArtifactRebuildIncomplete,
            error.RestoreDenseConfigProofIncomplete,
            error.RestoreDenseCounterProofIncomplete,
            error.RestoreDenseIndexProofIncomplete,
            error.RestoreDenseCoverageProofIncomplete,
            error.RestoreDenseCheckpointIncomplete,
            error.RestoreIndexAvailabilityIncomplete,
            => false,
            else => return err,
        };
        if (progressed) {
            result.restore_repair_progressed = 1;
            db.clearDenseHbcCaches();
        }
        result.restore_repair_pending = @intFromBool(try db.restoreRuntimeRepairNeeded());
        if (result.restore_repair_pending == 0) {
            try db.sync(true);
            try db.syncIndexes(true);
        }
    }

    var repair_summary = db.indexRepairIntentSummary(alloc) catch |err| switch (err) {
        error.DurableIndexRepairStateUnavailable => db_mod.DB.IndexRepairIntentSummary{},
        else => return err,
    };
    if (advance_index_repair and repair_summary.runnable != 0) {
        const repair = try db.repairRecoverableStartupIndexFailures(alloc, 1, .{});
        result.repair_discovered = repair.discovered;
        result.repair_attempted = repair.attempted;
        result.repair_repaired = repair.repaired;
        result.repair_remaining = repair.remaining;
        result.repair_terminal = repair.terminal;
        result.repair_busy = repair.busy;
        result.repair_disk_waits = repair.disk_waits;
        result.next_retry_at_ms = repair.next_retry_at_ms;
        repair_summary = try db.indexRepairIntentSummary(alloc);
    } else {
        result.repair_remaining = repair_summary.runnable + repair_summary.paused + repair_summary.terminal;
        result.repair_terminal = repair_summary.terminal;
        result.next_retry_at_ms = repair_summary.earliest_retry_at_ms;
    }

    result.state = if (repair_summary.terminal != 0)
        .degraded
    else if (result.repair_busy != 0)
        .busy
    else if (result.restore_repair_pending != 0)
        .restore_repair_pending
    else if (repair_summary.runnable != 0 or repair_summary.paused != 0)
        .repair_pending
    else if (provisioned.indexes_pending != 0)
        .busy
    else
        .complete;
    return result;
}

pub fn validateProvisionedDbIdentityNamespaceWithPolicy(
    expected: ?doc_identity.Namespace,
    validation: StartupCatchUpMetadata.IdentityValidation,
    db: *const db_mod.DB,
) !void {
    const namespace = expected orelse return;
    const valid = switch (validation) {
        .exact => db.core.identity_namespace.eql(namespace),
        .reassign_same_table => db.core.identity_namespace.table_id ==
            namespace.table_id,
    };
    if (!valid) return error.DocIdentityNamespaceMismatch;
}

fn runTestBeforeBatchExecutionHook() void {
    if (comptime builtin.is_test) @import("../api/local_write_test_hooks.zig").runTestBeforeBatchExecutionHook();
}

fn runTestBeforeNativeBackupCopyHook() void {
    if (comptime builtin.is_test) @import("../api/local_write_test_hooks.zig").runTestBeforeNativeBackupCopyHook();
}

test "native backup local attempt tokens are retry unique" {
    const first = try nativeSnapshotAttemptTokenAlloc(std.testing.allocator, std.testing.io, "backup", "g7");
    defer std.testing.allocator.free(first);
    const second = try nativeSnapshotAttemptTokenAlloc(std.testing.allocator, std.testing.io, "backup", "g7");
    defer std.testing.allocator.free(second);
    try std.testing.expect(!std.mem.eql(u8, first, second));
    try std.testing.expect(std.mem.startsWith(u8, first, "backup-g7-attempt-"));
}

test "native backup reclaims crash-left snapshot attempts from durable markers" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/table-db", .{tmp.sub_path});
    defer alloc.free(db_path);
    const token = "backup-g7-attempt-00000000000000000000000000000000";
    var attempt = try createNativeSnapshotAttemptMarker(
        alloc,
        std.testing.io,
        db_path,
        token,
        platform_time.realtimeNs() - 25 * std.time.ns_per_hour,
    );
    defer attempt.deinit();
    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/{s}", .{ db_path, token });
    defer alloc.free(snapshot_root);
    try fs_paths.createDirPathPortable(std.testing.io, snapshot_root);
    const staging_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/.{s}.staging-deadbeef", .{ db_path, token });
    defer alloc.free(staging_root);
    try fs_paths.createDirPathPortable(std.testing.io, staging_root);

    try reclaimStaleNativeSnapshotAttempts(alloc, std.testing.io, db_path);
    const marker_path = try alloc.dupe(u8, attempt.marker_path);
    defer alloc.free(marker_path);
    attempt.abandonForTest();
    try reclaimStaleNativeSnapshotAttempts(alloc, std.testing.io, db_path);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, marker_path, .{}));
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, snapshot_root, .{}));
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, staging_root, .{}));
}

test "native backup reclaims a crash marker before snapshot root creation" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/table-db", .{tmp.sub_path});
    defer alloc.free(db_path);
    const token = "backup-g7-attempt-11111111111111111111111111111111";
    var attempt = try createNativeSnapshotAttemptMarker(
        alloc,
        std.testing.io,
        db_path,
        token,
        platform_time.realtimeNs() - 25 * std.time.ns_per_hour,
    );
    const marker_path = try alloc.dupe(u8, attempt.marker_path);
    defer alloc.free(marker_path);
    attempt.abandonForTest();

    try reclaimStaleNativeSnapshotAttempts(alloc, std.testing.io, db_path);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, marker_path, .{}));
}

test "native backup never reclaims an old attempt with a live lease" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const db_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/table-db", .{tmp.sub_path});
    defer alloc.free(db_path);
    const token = "backup-g7-attempt-22222222222222222222222222222222";
    var attempt = try createNativeSnapshotAttemptMarker(
        alloc,
        std.testing.io,
        db_path,
        token,
        platform_time.realtimeNs() - 25 * std.time.ns_per_hour,
    );
    defer attempt.deinit();
    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/{s}", .{ db_path, token });
    defer alloc.free(snapshot_root);
    try fs_paths.createDirPathPortable(std.testing.io, snapshot_root);

    try reclaimStaleNativeSnapshotAttempts(alloc, std.testing.io, db_path);
    try std.Io.Dir.cwd().access(std.testing.io, attempt.marker_path, .{});
    try std.Io.Dir.cwd().access(std.testing.io, snapshot_root, .{});
}

pub fn batchUsesDurableTransactionContract(alloc: std.mem.Allocator, db: *db_mod.DB, req: db_mod.types.BatchRequest) !bool {
    const mutation = req.transaction orelse return false;
    return switch (mutation) {
        .prepare => |prepare| try transactionUsesDurableContract(alloc, db, prepare.txn_id),
        else => false,
    };
}

pub fn transactionUsesDurableContract(alloc: std.mem.Allocator, db: *db_mod.DB, txn_id: db_mod.types.TxnId) !bool {
    if (try db.core.transactionSchemaBinding(alloc, txn_id) != null) return true;
    const status = db.getTransactionStatus(txn_id) catch |err| switch (err) {
        error.TxnNotFound => return false,
        else => return err,
    };
    return status != .pending;
}

pub fn applyGraphMetricActionToDb(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    index_name: []const u8,
    metric_name: []const u8,
    action: []const u8,
) !db_mod.types.GraphMetricStatus {
    if (std.mem.eql(u8, action, "refresh")) return try db.scheduleGraphMetricBuild(alloc, index_name, metric_name, false);
    if (std.mem.eql(u8, action, "rebuild")) return try db.scheduleGraphMetricBuild(alloc, index_name, metric_name, true);
    if (std.mem.eql(u8, action, "delete")) return try db.deleteGraphMetricMaterialization(alloc, index_name, metric_name);
    if (std.mem.eql(u8, action, "pause")) return try db.pauseGraphMetricMaintenance(alloc, index_name, metric_name);
    if (std.mem.eql(u8, action, "resume")) return try db.resumeGraphMetricMaintenance(alloc, index_name, metric_name);
    return error.InvalidGraphMetricAction;
}

const GraphMetricGroupActionRequest = contract.GraphMetricGroupActionRequest;
const graph_metric_group_action_operation = contract.graph_metric_group_action_operation;

pub fn runGraphMetricMaintenanceOrActionJsonAlloc(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    body: []const u8,
) ![]u8 {
    var parsed = std.json.parseFromSlice(GraphMetricGroupActionRequest, alloc, body, .{ .ignore_unknown_fields = true }) catch
        return try db.runGraphMetricServiceMaintenanceJsonAlloc(alloc, body);
    defer parsed.deinit();
    const operation = parsed.value.operation orelse return try db.runGraphMetricServiceMaintenanceJsonAlloc(alloc, body);
    if (!std.mem.eql(u8, operation, graph_metric_group_action_operation)) {
        return try db.runGraphMetricServiceMaintenanceJsonAlloc(alloc, body);
    }
    if (parsed.value.index_name.len == 0 or parsed.value.metric_name.len == 0 or parsed.value.action.len == 0) {
        return error.InvalidGraphMetricAction;
    }
    var status = try applyGraphMetricActionToDb(
        alloc,
        db,
        parsed.value.index_name,
        parsed.value.metric_name,
        parsed.value.action,
    );
    defer status.deinit(alloc);
    return try std.json.Stringify.valueAlloc(alloc, status, .{ .emit_null_optional_fields = false });
}

pub fn reconfigureManagedDbEnrichmentRuntimePaused(
    _: std.mem.Allocator,
    db: *db_mod.DB,
    indexes_json: []const u8,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    antfly_provider: ?managed_embedder.AntflyProvider,
    remote_capability_cache: ?*remote_capabilities.Cache,
    inference_api_url: ?[]const u8,
    source_table: []const u8,
    secret_store: ?*common_secrets.FileStore,
    remote_content: ?*const scraping.RemoteContentConfig,
) !void {
    var enrichments = try createManagedDbEnrichments(
        db.runtime_alloc,
        indexes_json,
        backend_runtime,
        antfly_provider,
        remote_capability_cache,
        inference_api_url,
        source_table,
        secret_store,
        remote_content,
    );
    defer enrichments.deinit(db.runtime_alloc);
    try db.reconfigureEnrichmentRuntimePaused(enrichments.takeConfig());
}
