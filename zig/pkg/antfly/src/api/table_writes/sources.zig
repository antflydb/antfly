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

const builtin = @import("builtin");
const std = @import("std");
const platform_sync = @import("antfly_platform").sync;
const platform_time_lib = @import("antfly_platform").time;
const metadata_openapi = @import("antfly_metadata_openapi");
const scraping = @import("antfly_scraping");

const common_secrets = @import("../../common/secrets.zig");
const fs_paths = @import("../../common/fs_paths.zig");
const metadata_admin = @import("../../metadata/admin.zig");
const metadata_mod = @import("../../metadata/mod.zig");
const metadata_api = @import("../../metadata/api.zig");
const metadata_table_manager = @import("../../metadata/table_manager.zig");
const metadata_table_provisioner = @import("../../metadata/table_provisioner.zig");
const metadata_transition_state = @import("../../metadata/transition_state.zig");
const raft_mod = @import("../../raft/mod.zig");
const raft_reconciler = @import("../../raft/reconciler.zig");
const platform_time = @import("../../platform/time.zig");
const backend_types = @import("../../storage/backend_types.zig");
const doc_identity = @import("../../storage/db/doc_identity.zig");
const hbc_mod = @import("../../storage/hbc_adapter.zig");
const lsm_backend = @import("../../storage/lsm_backend/mod.zig");
const portable_backup = @import("../../storage/portable_backup.zig");
const resource_manager_mod = @import("../../storage/resource_manager.zig");
const ha_write_gate_mod = @import("../../storage/ha/write_gate.zig");
const schema_mod = @import("../../schema/mod.zig");
const build_options = @import("build_options");
const tracing = @import("../../tracing/mod.zig");
const backups_api = @import("../backups.zig");
const catalog_resources = @import("../catalog_resources.zig");
const db_mod = @import("../../storage/db/mod.zig");
const distributed_txn = @import("../distributed_txn.zig");
const http_client = @import("../http_client.zig");
const http_server = @import("../http_server.zig");
const indexes_api = @import("../indexes.zig");
const query_api = @import("../query.zig");
const relational_rows_api = @import("../relational_rows.zig");
const runtime_status = @import("../runtime_status.zig");
const sql_adapter = @import("../../sql/mod.zig");
const storage_schema = @import("../../storage/schema.zig");
const table_catalog = @import("../table_catalog.zig");
const table_read_cache = @import("../table_reads/cache.zig");
const table_read_core = @import("../table_reads/core.zig");
const table_read_sources = @import("../table_reads/sources.zig");
const table_router = @import("../table_router.zig");
const tables_api = @import("../tables.zig");
const http_common = @import("../../raft/transport/http_common.zig");
const std_http_listener = @import("../../raft/transport/std_http_listener.zig");
const table_write_backup_restore = @import("backup_restore.zig");
const table_write_bulk_ingest = @import("bulk_ingest.zig");
const table_write_cache = @import("cache.zig");
const table_write_core = @import("core.zig");
const table_write_index_config = @import("index_config.zig");
const table_write_integrity = @import("integrity.zig");
const table_write_integrity_types = @import("integrity_types.zig");
const table_write_managed_db = @import("managed_db.zig");
const table_write_relational_mutation = @import("relational_mutation.zig");
const table_write_remote_wire = @import("remote_wire.zig");
const table_write_schema_jobs = @import("schema_jobs.zig");
const managed_embedder = @import("../../inference/managed_embedder.zig");
const Io = std.Io;

const TableWriteSource = table_write_core.TableWriteSource;
const RaftBatcher = table_write_core.RaftBatcher;
const GroupBatch = table_write_core.GroupBatch;
const WriteCoalesceQueue = table_write_bulk_ingest.WriteCoalesceQueue;
const max_cached_write_tables = 64;
const freeBackupShards = table_write_backup_restore.freeBackupShards;
const ProvisionedTableWriteCache = table_write_cache.ProvisionedTableWriteCache;
const HostedManagedDbCache = table_write_cache.HostedManagedDbCache;
const hostedManagedDbCacheForRoot = table_write_cache.hostedManagedDbCacheForRoot;
const hostedManagedDbCacheForRootIfPresent = table_write_cache.hostedManagedDbCacheForRootIfPresent;
const backend_current_root_generation = table_write_core.backend_current_root_generation;
const nativeCatalogTableNameAlloc = table_catalog.nativeTableNameForCatalogTargetAlloc;
const nativeCatalogTableNameForCreateAlloc = table_catalog.nativeTableNameForCatalogCreateTargetAlloc;
const auto_bulk_ingest_min_batch_ops = table_write_bulk_ingest.min_batch_ops;
const auto_bulk_ingest_max_window_ops = table_write_bulk_ingest.max_window_ops;
const auto_bulk_ingest_max_idle_ns = table_write_bulk_ingest.max_idle_ns;
const auto_bulk_ingest_finish_options = table_write_bulk_ingest.finish_options;
const shouldDrainCachedManagedDbAfterBatch = table_write_bulk_ingest.shouldDrainCachedManagedDbAfterBatch;
const autoBulkIngestBatchOps = table_write_bulk_ingest.autoBulkIngestBatchOps;
const autoBulkIngestGroupBatchOps = table_write_bulk_ingest.autoBulkIngestGroupBatchOps;
const ensureGroupBatch = table_write_bulk_ingest.ensureGroupBatch;
const WriteCoalesceEntry = table_write_bulk_ingest.WriteCoalesceEntry;
const freeWriteCoalesceGroupBatch = table_write_bulk_ingest.freeWriteCoalesceGroupBatch;
const totalCoalescedWrites = table_write_bulk_ingest.totalCoalescedWrites;
const totalCoalescedDeletes = table_write_bulk_ingest.totalCoalescedDeletes;
const coalescedEntryBatchRequest = table_write_bulk_ingest.coalescedEntryBatchRequest;
const cloneWriteCoalesceGroupBatch = table_write_bulk_ingest.cloneWriteCoalesceGroupBatch;
const accumulateTextMemoryAttributionStats = table_write_cache.accumulateTextMemoryAttributionStats;
const overlayDenseHbcCacheStatsFromDb = table_write_cache.overlayDenseHbcCacheStatsFromDb;
const overlayRuntimeStatusReplayTargetFromDb = table_write_cache.overlayRuntimeStatusReplayTargetFromDb;
const startupCatchUpStatsForPhase = table_write_cache.startupCatchUpStatsForPhase;
const startupCatchUpStatsForPath = table_write_cache.startupCatchUpStatsForPath;
const applyStartupCatchUpAsyncOverlay = table_write_cache.applyStartupCatchUpAsyncOverlay;
const snapshotLocalTableRuntimeStatusesUncached = table_write_cache.snapshotLocalTableRuntimeStatusesUncached;
const moveDroppedGroupPathToTrash = table_write_backup_restore.moveDroppedGroupPathToTrash;
const deleteGroupPathIfPresent = table_write_backup_restore.deleteGroupPathIfPresent;
const readBackupFileAlloc = table_write_backup_restore.readBackupFileAlloc;
const cloneShardSnapshots = table_write_backup_restore.cloneShardSnapshots;
const ManagedDbOpenMode = table_write_managed_db.ManagedDbOpenMode;
const haMirrorForManagedDbOpenMode = table_write_managed_db.haMirrorForManagedDbOpenMode;
const catchUpManagedIndexCreate = table_write_managed_db.catchUpManagedIndexCreate;
const rebuildEmptyVersionedFullTextIndexesAfterSchemaUpdate = table_write_managed_db.rebuildEmptyVersionedFullTextIndexesAfterSchemaUpdate;
const seedManagedIndexReplayFromStoredDocsIfNeeded = table_write_managed_db.seedManagedIndexReplayFromStoredDocsIfNeeded;
const dropLocalTableIndex = table_write_managed_db.dropLocalTableIndex;
const drainManagedDbBeforeClose = table_write_managed_db.drainManagedDbBeforeClose;
const isTransientReplayVisibilityError = table_write_managed_db.isTransientReplayVisibilityError;
const loadTableIndexesJson = table_write_managed_db.loadTableIndexesJson;
const loadTableIdentityNamespaceForGroup = table_write_managed_db.loadTableIdentityNamespaceForGroup;
const findTableRecord = table_write_managed_db.findTableRecord;
const findRangeRecord = table_write_managed_db.findRangeRecord;
const loadTableSchemaJson = table_write_managed_db.loadTableSchemaJson;
const loadTableManagedMetadata = table_write_managed_db.loadTableManagedMetadata;
const validateProvisionedDbIdentityNamespaceExpected = table_write_managed_db.validateProvisionedDbIdentityNamespaceExpected;
const validateProvisionedDbIdentityNamespace = table_write_managed_db.validateProvisionedDbIdentityNamespace;
const validateTableBatchAgainstSchemaJson = table_write_managed_db.validateTableBatchAgainstSchemaJson;
const validateTableBatchAgainstCatalogSchema = table_write_managed_db.validateTableBatchAgainstCatalogSchema;
const validateTransactionAgainstCatalogSchema = table_write_managed_db.validateTransactionAgainstCatalogSchema;
const openManagedDbWithIndexesJson = table_write_managed_db.openManagedDbWithIndexesJson;
const openManagedDbWithIndexesJsonAndCacheMode = table_write_managed_db.openManagedDbWithIndexesJsonAndCacheMode;
const openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndIdentity = table_write_managed_db.openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndIdentity;
const openManagedDbForTableGroupWithRuntime = table_write_managed_db.openManagedDbForTableGroupWithRuntime;
const openManagedDbForTableGroupWithRuntimeAndHAWriteGate = table_write_managed_db.openManagedDbForTableGroupWithRuntimeAndHAWriteGate;
const openManagedDbForTableGroupWithCacheAndRuntime = table_write_managed_db.openManagedDbForTableGroupWithCacheAndRuntime;
const openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentity = table_write_managed_db.openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentity;
const openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentityWithOptions = table_write_managed_db.openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentityWithOptions;
const resolveForeignKeyIntegrityGroupsEventually = table_write_integrity.resolveForeignKeyIntegrityGroupsEventually;
const planForeignKeyIntegrityWorkUnits = table_write_integrity.planForeignKeyIntegrityWorkUnits;
const planForeignKeyIntegrityWorkerWorkUnits = table_write_integrity.planForeignKeyIntegrityWorkerWorkUnits;
const appendUniqueConstraintIntegrityResult = table_write_integrity.appendUniqueConstraintIntegrityResult;
const cloneForeignKeyIntegrityResultForWorkerExecution = table_write_integrity.cloneForeignKeyIntegrityResultForWorkerExecution;
const appendForeignKeyActionJobStatuses = table_write_integrity.appendForeignKeyActionJobStatuses;
const appendForeignKeyActionScheduleStatuses = table_write_integrity.appendForeignKeyActionScheduleStatuses;
const cloneForeignKeyActionJobStatus = table_write_integrity.cloneForeignKeyActionJobStatus;
const cloneForeignKeyActionScheduleStatus = table_write_integrity.cloneForeignKeyActionScheduleStatus;
const foreignKeyActionCanRunGroupDbLocal = table_write_integrity.foreignKeyActionCanRunGroupDbLocal;
const runCatalogForeignKeyIntegritySchemaControllerMaintenancePass = table_write_integrity.runCatalogForeignKeyIntegritySchemaControllerMaintenancePass;
const runCatalogUniqueConstraintIntegritySchemaControllerMaintenancePass = table_write_integrity.runCatalogUniqueConstraintIntegritySchemaControllerMaintenancePass;
const foreignKeyIntegrityResultFromRoutedExplain = table_write_integrity.foreignKeyIntegrityResultFromRoutedExplain;
const foreignKeyActionOwnerParentTableNameAlloc = table_write_integrity.foreignKeyActionOwnerParentTableNameAlloc;
const collectForeignKeyActionJobProgressGroupIds = table_write_integrity.collectForeignKeyActionJobProgressGroupIds;
const stableForeignKeyActionPageTxnId = table_write_integrity.stableForeignKeyActionPageTxnId;
const runSecondaryIndexRebuildRangeGroupLocal = table_write_schema_jobs.runSecondaryIndexRebuildRangeGroupLocal;
const runSecondaryIndexRebuildWorkerPassForCatalog = table_write_schema_jobs.runSecondaryIndexRebuildWorkerPassForCatalog;
const runSchemaRewriteJobGroupLocal = table_write_schema_jobs.runSchemaRewriteJobGroupLocal;
const runSchemaRewriteWorkerPassForCatalog = table_write_schema_jobs.runSchemaRewriteWorkerPassForCatalog;
const SecondaryIndexRebuildWorkerResult = table_write_schema_jobs.SecondaryIndexRebuildWorkerResult;
const SecondaryIndexRebuildWorkerPassResult = table_write_schema_jobs.SecondaryIndexRebuildWorkerPassResult;
const SecondaryIndexRebuildGroupRequest = table_write_schema_jobs.SecondaryIndexRebuildGroupRequest;
const SchemaRewriteWorkerResult = table_write_schema_jobs.SchemaRewriteWorkerResult;
const SchemaRewriteWorkerPassResult = table_write_schema_jobs.SchemaRewriteWorkerPassResult;
const SchemaRewriteGroupRequest = table_write_schema_jobs.SchemaRewriteGroupRequest;
const ForeignKeyIntegrityAction = table_write_integrity_types.ForeignKeyIntegrityAction;
const ForeignKeyIntegrityRequest = table_write_integrity_types.ForeignKeyIntegrityRequest;
const UniqueConstraintIntegrityRequest = table_write_integrity_types.UniqueConstraintIntegrityRequest;
const ForeignKeyIntegrityProgress = table_write_integrity_types.ForeignKeyIntegrityProgress;
const ForeignKeyIntegrityWorkClaim = table_write_integrity_types.ForeignKeyIntegrityWorkClaim;
const ForeignKeyIntegritySchemaControllerOptions = table_write_integrity_types.ForeignKeyIntegritySchemaControllerOptions;
const ForeignKeyIntegritySchemaControllerResult = table_write_integrity_types.ForeignKeyIntegritySchemaControllerResult;
const UniqueConstraintIntegrityAction = table_write_integrity_types.UniqueConstraintIntegrityAction;
const UniqueConstraintIntegritySchemaControllerOptions = table_write_integrity_types.UniqueConstraintIntegritySchemaControllerOptions;
const UniqueConstraintIntegritySchemaControllerResult = table_write_integrity_types.UniqueConstraintIntegritySchemaControllerResult;
const UniqueConstraintIntegrityGroupReport = table_write_integrity_types.UniqueConstraintIntegrityGroupReport;
const UniqueConstraintIntegrityProgress = table_write_integrity_types.UniqueConstraintIntegrityProgress;
const UniqueConstraintIntegrityResult = table_write_integrity_types.UniqueConstraintIntegrityResult;
const ForeignKeyIntegrityWorkUnit = table_write_integrity_types.ForeignKeyIntegrityWorkUnit;
const ForeignKeyIntegrityResult = table_write_integrity_types.ForeignKeyIntegrityResult;
const ForeignKeyIntegrityGroupReport = table_write_integrity_types.ForeignKeyIntegrityGroupReport;
const ForeignKeyIntegrityViolation = table_write_integrity_types.ForeignKeyIntegrityViolation;
const ForeignKeyIntegrityJobStatus = table_write_integrity_types.ForeignKeyIntegrityJobStatus;
const ForeignKeyActionJobStatus = table_write_integrity_types.ForeignKeyActionJobStatus;
const ForeignKeyActionJobResult = table_write_integrity_types.ForeignKeyActionJobResult;
const ForeignKeyActionJobProgressResult = table_write_integrity_types.ForeignKeyActionJobProgressResult;
const ForeignKeyActionScheduleStatus = table_write_integrity_types.ForeignKeyActionScheduleStatus;
const ForeignKeyActionScheduleProgressResult = table_write_integrity_types.ForeignKeyActionScheduleProgressResult;

const normalizeRelationalConstraintError = table_write_core.normalizeRelationalConstraintError;
const nextTxnTimestamp = table_write_core.nextTxnTimestamp;
const nextTxnId = table_write_core.nextTxnId;
const boundConflict = table_write_core.boundConflict;
const loadLocalTableSchemaJson = table_write_managed_db.loadLocalTableSchemaJson;
const applyLocalTableSchemaJson = table_write_managed_db.applyLocalTableSchemaJson;
const validateTableBatchAgainstLocalSchema = table_write_managed_db.validateTableBatchAgainstLocalSchema;
const validateTransactionAgainstLocalSchema = table_write_managed_db.validateTransactionAgainstLocalSchema;
const corruptEmbeddingArtifactInDb = table_write_managed_db.corruptEmbeddingArtifactInDb;
const mutateRowsFromSourceAutocommitOnDb = table_write_relational_mutation.mutateRowsFromSourceAutocommitOnDb;
const mutateRowsJoinedFromSourceRowsOnDb = table_write_relational_mutation.mutateRowsJoinedFromSourceRowsOnDb;
const mutateRowsJoinedFromSourceRowsAutocommitOnDb = table_write_relational_mutation.mutateRowsJoinedFromSourceRowsAutocommitOnDb;
const mergeRowsFromSourceRowsOnDb = table_write_relational_mutation.mergeRowsFromSourceRowsOnDb;
const cloneForeignKeyIntegrityResultViolation = table_write_integrity.cloneForeignKeyIntegrityResultViolation;
const foreignKeyActionJobStatusFromDbRecord = table_write_integrity.foreignKeyActionJobStatusFromDbRecord;
const foreignKeyActionScheduleStatusFromDbRecord = table_write_integrity.foreignKeyActionScheduleStatusFromDbRecord;
const foreignKeyActionJobProgressFromDbRecords = table_write_integrity.foreignKeyActionJobProgressFromDbRecords;
const foreignKeyActionScheduleProgressFromDbRecords = table_write_integrity.foreignKeyActionScheduleProgressFromDbRecords;
const emptyForeignKeyIntegrityControllerResult = table_write_integrity.emptyForeignKeyIntegrityControllerResult;
const foreignKeyIntegritySchemaControllerPassWithSchemaJson = table_write_integrity.foreignKeyIntegritySchemaControllerPassWithSchemaJson;
const foreignKeyIntegrityWorkerActionSupported = table_write_integrity.foreignKeyIntegrityWorkerActionSupported;
const startForeignKeyIntegrityJobOnDb = table_write_integrity.startForeignKeyIntegrityJobOnDb;
const finishForeignKeyIntegrityJobOnDb = table_write_integrity.finishForeignKeyIntegrityJobOnDb;
const attachForeignKeyIntegrityJobId = table_write_integrity.attachForeignKeyIntegrityJobId;
const hydrateForeignKeyIntegrityResultDiagnosticsFromJobOnDb = table_write_integrity.hydrateForeignKeyIntegrityResultDiagnosticsFromJobOnDb;
const buildForeignKeyIntegrityWorkStatuses = table_write_integrity.buildForeignKeyIntegrityWorkStatuses;
const foreignKeyIntegritySingleWorkUnit = table_write_integrity.foreignKeyIntegritySingleWorkUnit;
const foreignKeyIntegrityPlannedUnitsContainGroupBefore = table_write_integrity.foreignKeyIntegrityPlannedUnitsContainGroupBefore;
const foreignKeyIntegrityNowNs = table_write_integrity.foreignKeyIntegrityNowNs;
const foreignKeyIntegrityWorkStatusClaimable = table_write_integrity.foreignKeyIntegrityWorkStatusClaimable;
const foreignKeyIntegrityWorkStatusesHaveClaimable = table_write_integrity.foreignKeyIntegrityWorkStatusesHaveClaimable;
const foreignKeyIntegrityWorkStatusesValid = table_write_integrity.foreignKeyIntegrityWorkStatusesValid;
const cloneForeignKeyIntegrityWorkUnit = table_write_integrity.cloneForeignKeyIntegrityWorkUnit;
const cloneForeignKeyIntegrityWorkUnits = table_write_integrity.cloneForeignKeyIntegrityWorkUnits;
const cloneForeignKeyIntegrityProgressSlice = table_write_integrity.cloneForeignKeyIntegrityProgressSlice;
const cloneForeignKeyIntegrityWorkClaimSlice = table_write_integrity.cloneForeignKeyIntegrityWorkClaimSlice;
const cloneForeignKeyIntegrityResultForWorkerSnapshot = table_write_integrity.cloneForeignKeyIntegrityResultForWorkerSnapshot;
const mergeForeignKeyIntegrityReport = table_write_integrity.mergeForeignKeyIntegrityReport;
const appendForeignKeyIntegrityResult = table_write_integrity.appendForeignKeyIntegrityResult;
const appendForeignKeyIntegrityProgressAndClaims = table_write_integrity.appendForeignKeyIntegrityProgressAndClaims;
const appendForeignKeyIntegrityExecutedResult = table_write_integrity.appendForeignKeyIntegrityExecutedResult;
const runForeignKeyIntegrityOnDb = table_write_integrity.runForeignKeyIntegrityOnDb;
const runForeignKeyIntegrityClaimedWorkUnitOnDb = table_write_integrity.runForeignKeyIntegrityClaimedWorkUnitOnDb;
const runUniqueConstraintIntegrityOnDb = table_write_integrity.runUniqueConstraintIntegrityOnDb;

pub const TestExecutionHook = struct {
    ptr: *anyopaque,
    run: *const fn (ptr: *anyopaque) void,
};

pub var test_before_batch_execution_hook: ?TestExecutionHook = null;
pub var test_before_drop_table_delete_hook: ?table_write_backup_restore.DroppedTableDeleteHook = null;
pub var test_before_drop_index_work_hook: ?TestExecutionHook = null;
pub var test_before_restore_work_hook: ?TestExecutionHook = null;

fn runTestBeforeBatchExecutionHook() void {
    if (comptime builtin.is_test) {
        if (test_before_batch_execution_hook) |hook| hook.run(hook.ptr);
    }
}

fn runTestBeforeDropIndexWorkHook() void {
    if (comptime builtin.is_test) {
        if (test_before_drop_index_work_hook) |hook| hook.run(hook.ptr);
    }
}

fn runTestBeforeRestoreWorkHook() void {
    if (comptime builtin.is_test) {
        if (test_before_restore_work_hook) |hook| hook.run(hook.ptr);
    }
}

fn mergeDocumentArtifactTableReprocessResult(
    alloc: std.mem.Allocator,
    dst: *db_mod.types.DocumentArtifactTableReprocessResult,
    dst_failures: *std.ArrayListUnmanaged(db_mod.types.DocumentArtifactReprocessFailure),
    dst_shard_cursors: *std.ArrayListUnmanaged(db_mod.types.DocumentArtifactReprocessShardCursor),
    group_id: ?u64,
    src: db_mod.types.DocumentArtifactTableReprocessResult,
) !void {
    dst.scanned += src.scanned;
    dst.reprocessed += src.reprocessed;
    dst.skipped += src.skipped;
    dst.failed += src.failed;
    dst.limit += src.limit;
    if (dst.next_key == null) {
        if (src.next_key) |next_key| dst.next_key = try alloc.dupe(u8, next_key);
    }
    if (src.shard_cursors.len > 0) {
        for (src.shard_cursors) |cursor| {
            const next_key_copy = try alloc.dupe(u8, cursor.next_key);
            errdefer alloc.free(next_key_copy);
            try dst_shard_cursors.append(alloc, .{
                .group_id = cursor.group_id orelse group_id,
                .next_key = next_key_copy,
                .scanned = cursor.scanned,
                .reprocessed = cursor.reprocessed,
                .skipped = cursor.skipped,
                .failed = cursor.failed,
                .limit = cursor.limit,
            });
        }
    } else if (src.next_key) |next_key| {
        const next_key_copy = try alloc.dupe(u8, next_key);
        errdefer alloc.free(next_key_copy);
        try dst_shard_cursors.append(alloc, .{
            .group_id = group_id,
            .next_key = next_key_copy,
            .scanned = src.scanned,
            .reprocessed = src.reprocessed,
            .skipped = src.skipped,
            .failed = src.failed,
            .limit = src.limit,
        });
    }
    for (src.failures) |failure| {
        const key = try alloc.dupe(u8, failure.key);
        errdefer alloc.free(key);
        const error_code = try alloc.dupe(u8, failure.error_code);
        errdefer alloc.free(error_code);
        try dst_failures.append(alloc, .{
            .key = key,
            .error_code = error_code,
        });
    }
}

fn documentArtifactReprocessRequestForCursor(
    req: db_mod.types.DocumentArtifactTableReprocessRequest,
    cursor: db_mod.types.DocumentArtifactReprocessShardResume,
) db_mod.types.DocumentArtifactTableReprocessRequest {
    return .{
        .from_key = cursor.next_key,
        .to_key = req.to_key,
        .limit = if (cursor.limit != 0) cursor.limit else req.limit,
    };
}

fn uniqueTestTmpPathAlloc(alloc: std.mem.Allocator, prefix: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "/tmp/{s}-{d}", .{ prefix, platform_time.monotonicNs() });
}

fn testingVisibleRootGenerationSource(value: *u64) table_read_core.GroupVisibleRootGenerationSource {
    return .{
        .ptr = value,
        .visible_root_generation_for_group = testingVisibleRootGenerationForGroup,
    };
}

fn testingVisibleRootGenerationForGroup(ptr: *anyopaque, _: u64) u64 {
    return (@as(*u64, @ptrCast(@alignCast(ptr)))).*;
}

pub const ProvisionedTableWriteSource = struct {
    pub const StartupCatchUpResult = struct {
        had_debt: bool = false,
        cleared_debt: bool = false,
        busy: bool = false,
    };

    pub const LocalChangeKind = enum {
        data,
        structural,
    };

    pub const LocalChangeHook = struct {
        ptr: *anyopaque,
        on_change: *const fn (ptr: *anyopaque, table_name: []const u8, kind: LocalChangeKind) void,
    };

    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    local_db_mutex: std.atomic.Mutex = .unlocked,
    table_activity_threaded: Io.Threaded,
    table_activity_mutex: Io.Mutex = .init,
    table_activity_ready: Io.Condition = .init,
    write_coalesce_mutex: Io.Mutex = .init,
    write_coalesce_ready: Io.Condition = .init,
    write_coalesce_queues: std.ArrayListUnmanaged(WriteCoalesceQueue) = .empty,
    read_cache: ?*table_read_cache.ProvisionedTableReadCache = null,
    write_cache: ?*ProvisionedTableWriteCache = null,
    startup_write_cache: ?*ProvisionedTableWriteCache = null,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime = null,
    dropped_table_delete_owner_id: u64 = 0,
    restore_repair_work_group: Io.Group = .init,
    restore_repair_completion_mutex: Io.Mutex = .init,
    restore_repair_completion_group: Io.Group = .init,
    restore_repair_completion_scheduled: std.atomic.Value(bool) = .init(false),
    restore_repair_completions: std.ArrayListUnmanaged([]u8) = .empty,
    runtime_status_cache: ?*runtime_status.TableRuntimeSnapshotCache = null,
    local_change_hook: ?LocalChangeHook = null,
    raft_batcher: ?RaftBatcher = null,
    group_visible_root_generation: ?table_read_core.GroupVisibleRootGenerationSource = null,
    antfly_provider: ?managed_embedder.AntflyProvider = null,
    secret_store: ?*common_secrets.FileStore = null,
    remote_content: ?*const scraping.RemoteContentConfig = null,
    resolution_candidate_source: ?db_mod.CandidateSource = null,
    entity_sink: ?db_mod.EntitySink = null,
    promotion_leadership_source: ?ProvisionedTableWriteCache.PromotionLeadershipSource = null,
    ha_write_gate: ?db_mod.HAWriteGate = null,
    ha_async_mirror: ?db_mod.HAAsyncEffectMirror = null,
    dirty_write_tables_mutex: std.atomic.Mutex = .unlocked,
    dirty_write_table_count: std.atomic.Value(u32) = .init(0),
    startup_catch_up_active: std.atomic.Value(bool) = .init(false),
    dirty_write_table_hashes: [max_cached_write_tables]u64 = [_]u64{0} ** max_cached_write_tables,
    dirty_write_table_hashes_len: usize = 0,
    active_table_activities: std.ArrayListUnmanaged(TableActivity) = .empty,

    const TableActivity = struct {
        // Owned by active_table_activities; entries must be created via activityEntryLocked.
        table_name: []const u8,
        group_id: ?u64 = null,
        table_request_active: usize = 0,
        operation_active: bool = false,
        structural_active: bool = false,
    };

    pub fn init(replica_root_dir: []const u8, catalog: table_catalog.CatalogSource) ProvisionedTableWriteSource {
        return .{
            .replica_root_dir = replica_root_dir,
            .catalog = catalog,
            .table_activity_threaded = Io.Threaded.init(std.heap.page_allocator, .{}),
        };
    }

    pub fn withAntflyProvider(
        self: *ProvisionedTableWriteSource,
        provider: ?managed_embedder.AntflyProvider,
    ) *ProvisionedTableWriteSource {
        self.antfly_provider = provider;
        if (self.write_cache) |cache| cache.antfly_provider = provider;
        if (self.startup_write_cache) |cache| cache.antfly_provider = provider;
        return self;
    }

    pub fn withSecretStore(
        self: *ProvisionedTableWriteSource,
        secret_store: ?*common_secrets.FileStore,
    ) *ProvisionedTableWriteSource {
        self.secret_store = secret_store;
        if (self.write_cache) |cache| cache.secret_store = secret_store;
        if (self.startup_write_cache) |cache| cache.secret_store = secret_store;
        return self;
    }

    pub fn withRemoteContent(
        self: *ProvisionedTableWriteSource,
        remote_content: ?*const scraping.RemoteContentConfig,
    ) *ProvisionedTableWriteSource {
        self.remote_content = remote_content;
        if (self.write_cache) |cache| cache.remote_content = remote_content;
        if (self.startup_write_cache) |cache| cache.remote_content = remote_content;
        return self;
    }

    pub fn withGroupVisibleRootGeneration(
        self: *ProvisionedTableWriteSource,
        generation_source: ?table_read_core.GroupVisibleRootGenerationSource,
    ) *ProvisionedTableWriteSource {
        self.group_visible_root_generation = generation_source;
        return self;
    }

    fn groupVisibleRootGenerationSource(self: *const ProvisionedTableWriteSource) ?table_read_core.GroupVisibleRootGenerationSource {
        return self.group_visible_root_generation;
    }

    pub fn withResolutionCandidateSource(
        self: *ProvisionedTableWriteSource,
        resolution_candidate_source: ?db_mod.CandidateSource,
    ) *ProvisionedTableWriteSource {
        self.resolution_candidate_source = resolution_candidate_source;
        self.syncRuntimeHooksToCaches();
        return self;
    }

    pub fn withEntitySink(
        self: *ProvisionedTableWriteSource,
        entity_sink: ?db_mod.EntitySink,
    ) *ProvisionedTableWriteSource {
        self.entity_sink = entity_sink;
        self.syncRuntimeHooksToCaches();
        return self;
    }

    pub fn withPromotionLeadershipSource(
        self: *ProvisionedTableWriteSource,
        leadership_source: ?ProvisionedTableWriteCache.PromotionLeadershipSource,
    ) *ProvisionedTableWriteSource {
        self.promotion_leadership_source = leadership_source;
        self.syncRuntimeHooksToCaches();
        return self;
    }

    pub fn withHAWriteGate(
        self: *ProvisionedTableWriteSource,
        gate: ?db_mod.HAWriteGate,
    ) *ProvisionedTableWriteSource {
        self.ha_write_gate = gate;
        if (self.write_cache) |cache| cache.setHAWriteGate(gate);
        if (self.startup_write_cache) |cache| cache.setHAWriteGate(gate);
        return self;
    }

    pub fn withHAMirror(
        self: *ProvisionedTableWriteSource,
        mirror: ?db_mod.HAAsyncEffectMirror,
    ) *ProvisionedTableWriteSource {
        self.ha_async_mirror = mirror;
        if (self.write_cache) |cache| cache.setHAMirror(mirror);
        if (self.startup_write_cache) |cache| cache.setHAMirror(mirror);
        return self;
    }

    fn syncRuntimeHooksToCaches(self: *ProvisionedTableWriteSource) void {
        if (self.write_cache) |cache| self.syncRuntimeHooksToCache(cache);
        if (self.startup_write_cache) |cache| {
            if (self.write_cache != cache) self.syncRuntimeHooksToCache(cache);
        }
    }

    fn syncRuntimeHooksToCache(
        self: *ProvisionedTableWriteSource,
        cache: *ProvisionedTableWriteCache,
    ) void {
        lockAtomic(&cache.open_mutex);
        defer cache.open_mutex.unlock();
        lockAtomic(&self.local_db_mutex);
        defer self.local_db_mutex.unlock();
        cache.setRuntimeHooksLocked(
            self.resolution_candidate_source,
            self.entity_sink,
            self.promotion_leadership_source,
        );
    }

    fn applyRuntimeHooksToUncachedDb(
        self: *ProvisionedTableWriteSource,
        db: *db_mod.DB,
        group_id: u64,
        owner_state: *ProvisionedTableWriteCache.PromotionOwnerState,
    ) void {
        db.setResolutionCandidateSource(self.resolution_candidate_source);
        db.setEntitySink(self.entity_sink);
        owner_state.* = .{
            .group_id = group_id,
            .leadership_source = self.promotion_leadership_source,
        };
        db.setPromotionOwner(owner_state.owner());
    }

    pub fn withRaftBatcher(self: *ProvisionedTableWriteSource, batcher: ?RaftBatcher) *ProvisionedTableWriteSource {
        self.raft_batcher = batcher;
        return self;
    }

    pub fn deinit(self: *ProvisionedTableWriteSource) void {
        self.drainDroppedTableDeletes();
        const io = self.table_activity_threaded.io();
        self.restore_repair_work_group.await(io) catch {};
        self.restore_repair_completion_group.await(io) catch {};
        self.freeRestoreRepairCompletions();
        self.freeWriteCoalesceQueues();
        self.table_activity_mutex.lockUncancelable(io);
        for (self.active_table_activities.items) |entry| {
            std.heap.page_allocator.free(entry.table_name);
        }
        self.active_table_activities.deinit(std.heap.page_allocator);
        self.active_table_activities = .empty;
        self.table_activity_mutex.unlock(io);
        self.table_activity_threaded.deinit();
        self.* = undefined;
    }

    pub fn localDbMutex(self: *ProvisionedTableWriteSource) *std.atomic.Mutex {
        return &self.local_db_mutex;
    }

    fn freeWriteCoalesceQueues(self: *ProvisionedTableWriteSource) void {
        const io = self.table_activity_threaded.io();
        const alloc = std.heap.page_allocator;
        self.write_coalesce_mutex.lockUncancelable(io);
        defer self.write_coalesce_mutex.unlock(io);
        table_write_bulk_ingest.deinitWriteCoalesceQueues(alloc, &self.write_coalesce_queues);
    }

    fn findWriteCoalesceQueueLocked(
        self: *ProvisionedTableWriteSource,
        table_name: []const u8,
        group_id: u64,
    ) ?usize {
        return table_write_bulk_ingest.findWriteCoalesceQueue(self.write_coalesce_queues.items, table_name, group_id);
    }

    fn pruneWriteCoalesceQueueLocked(self: *ProvisionedTableWriteSource, index: usize) void {
        table_write_bulk_ingest.pruneWriteCoalesceQueue(std.heap.page_allocator, &self.write_coalesce_queues, index);
    }

    fn writeCoalesceQueueLocked(
        self: *ProvisionedTableWriteSource,
        table_name: []const u8,
        group_id: u64,
    ) !*WriteCoalesceQueue {
        return try table_write_bulk_ingest.ensureWriteCoalesceQueue(std.heap.page_allocator, &self.write_coalesce_queues, table_name, group_id);
    }

    fn visibleRootGeneration(self: *const ProvisionedTableWriteSource, group_id: u64) u64 {
        return if (self.group_visible_root_generation) |generation_source| generation_source.visibleRootGenerationForGroup(group_id) else backend_current_root_generation;
    }

    fn droppedTableDeleteOwnerId(self: *ProvisionedTableWriteSource, runtime: *db_mod.background_runtime.BackendRuntime) u64 {
        if (self.dropped_table_delete_owner_id == 0) {
            self.dropped_table_delete_owner_id = runtime.allocOwnerId();
        }
        return self.dropped_table_delete_owner_id;
    }

    pub fn drainDroppedTableDeletes(self: *ProvisionedTableWriteSource) void {
        if (self.dropped_table_delete_owner_id == 0) return;
        if (self.backend_runtime) |runtime| {
            runtime.durable_jobs.drainOwner(self.dropped_table_delete_owner_id);
        }
    }

    fn deleteDroppedGroupPath(self: *ProvisionedTableWriteSource, alloc: std.mem.Allocator, path: []u8) !void {
        const runtime = self.backend_runtime;
        const owner_id = if (runtime) |job_runtime| self.droppedTableDeleteOwnerId(job_runtime) else null;
        try table_write_backup_restore.deleteDroppedGroupPath(alloc, path, runtime, owner_id, test_before_drop_table_delete_hook);
    }

    fn findTableActivityLocked(self: *ProvisionedTableWriteSource, table_name: []const u8, group_id: ?u64) ?usize {
        for (self.active_table_activities.items, 0..) |entry, i| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.group_id != group_id) continue;
            return i;
        }
        return null;
    }

    fn activityEntryLocked(self: *ProvisionedTableWriteSource, table_name: []const u8, group_id: ?u64) *TableActivity {
        if (self.findTableActivityLocked(table_name, group_id)) |index| return &self.active_table_activities.items[index];
        const index = self.active_table_activities.items.len;
        const owned_table_name = std.heap.page_allocator.dupe(u8, table_name) catch {
            @panic("failed to allocate table activity name");
        };
        self.active_table_activities.append(std.heap.page_allocator, .{ .table_name = owned_table_name, .group_id = group_id }) catch {
            std.heap.page_allocator.free(owned_table_name);
            @panic("failed to allocate table activity entry");
        };
        return &self.active_table_activities.items[index];
    }

    fn pruneTableActivityLocked(self: *ProvisionedTableWriteSource, table_name: []const u8, group_id: ?u64) void {
        const index = self.findTableActivityLocked(table_name, group_id) orelse return;
        const entry = self.active_table_activities.items[index];
        if (entry.table_request_active > 0 or entry.operation_active or entry.structural_active) return;
        const removed = self.active_table_activities.swapRemove(index);
        std.heap.page_allocator.free(removed.table_name);
        if (self.active_table_activities.items.len == 0) {
            self.active_table_activities.deinit(std.heap.page_allocator);
            self.active_table_activities = .empty;
        }
    }

    fn hasAnyActiveGroupOperationLocked(self: *ProvisionedTableWriteSource, table_name: []const u8) bool {
        for (self.active_table_activities.items) |entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.group_id == null) continue;
            if (entry.operation_active) return true;
        }
        return false;
    }

    fn waitForNoStructuralActivityLocked(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        const io = self.table_activity_threaded.io();
        while (true) {
            if (self.findTableActivityLocked(table_name, null)) |index| {
                if (self.active_table_activities.items[index].structural_active) {
                    self.table_activity_ready.waitUncancelable(io, &self.table_activity_mutex);
                    continue;
                }
            }
            return;
        }
    }

    fn beginTableRequestLocked(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        const io = self.table_activity_threaded.io();
        while (true) {
            if (self.findTableActivityLocked(table_name, null)) |index| {
                if (self.active_table_activities.items[index].structural_active) {
                    self.table_activity_ready.waitUncancelable(io, &self.table_activity_mutex);
                    continue;
                }
            }
            const entry = self.activityEntryLocked(table_name, null);
            entry.table_request_active += 1;
            return;
        }
    }

    fn endTableRequestLocked(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        const io = self.table_activity_threaded.io();
        const index = self.findTableActivityLocked(table_name, null) orelse {
            self.table_activity_ready.broadcast(io);
            return;
        };
        if (self.active_table_activities.items[index].table_request_active == 0) {
            self.table_activity_ready.broadcast(io);
            return;
        }
        self.active_table_activities.items[index].table_request_active -= 1;
        self.pruneTableActivityLocked(table_name, null);
        self.table_activity_ready.broadcast(io);
    }

    fn beginGroupOperationLocked(self: *ProvisionedTableWriteSource, table_name: []const u8, group_id: u64) void {
        const io = self.table_activity_threaded.io();
        while (true) {
            if (self.findTableActivityLocked(table_name, null)) |index| {
                const entry = self.active_table_activities.items[index];
                if (entry.structural_active) {
                    self.table_activity_ready.waitUncancelable(io, &self.table_activity_mutex);
                    continue;
                }
            }
            if (self.findTableActivityLocked(table_name, group_id)) |index| {
                const entry = self.active_table_activities.items[index];
                if (entry.operation_active) {
                    self.table_activity_ready.waitUncancelable(io, &self.table_activity_mutex);
                    continue;
                }
            }
            const entry = self.activityEntryLocked(table_name, group_id);
            entry.operation_active = true;
            return;
        }
    }

    fn tryBeginGroupOperationLocked(self: *ProvisionedTableWriteSource, table_name: []const u8, group_id: u64) bool {
        if (self.findTableActivityLocked(table_name, null)) |index| {
            const entry = self.active_table_activities.items[index];
            if (entry.structural_active) return false;
        }
        if (self.findTableActivityLocked(table_name, group_id)) |index| {
            const entry = self.active_table_activities.items[index];
            if (entry.operation_active) return false;
        }
        const entry = self.activityEntryLocked(table_name, group_id);
        entry.operation_active = true;
        return true;
    }

    fn endGroupOperationLocked(self: *ProvisionedTableWriteSource, table_name: []const u8, group_id: u64) void {
        const io = self.table_activity_threaded.io();
        const index = self.findTableActivityLocked(table_name, group_id) orelse unreachable;
        std.debug.assert(self.active_table_activities.items[index].operation_active);
        self.active_table_activities.items[index].operation_active = false;
        self.pruneTableActivityLocked(table_name, group_id);
        self.table_activity_ready.broadcast(io);
    }

    fn tryBeginStructuralTableActivityLocked(self: *ProvisionedTableWriteSource, table_name: []const u8) bool {
        if (self.findTableActivityLocked(table_name, null)) |index| {
            const entry = self.active_table_activities.items[index];
            if (entry.structural_active or entry.table_request_active > 0) return false;
        }
        if (self.hasAnyActiveGroupOperationLocked(table_name)) return false;
        const entry = self.activityEntryLocked(table_name, null);
        entry.structural_active = true;
        return true;
    }

    fn beginStructuralTableActivityLocked(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        const io = self.table_activity_threaded.io();
        while (true) {
            if (self.findTableActivityLocked(table_name, null)) |index| {
                const entry = self.active_table_activities.items[index];
                if (entry.structural_active or entry.table_request_active > 0) {
                    self.table_activity_ready.waitUncancelable(io, &self.table_activity_mutex);
                    continue;
                }
            }
            if (self.hasAnyActiveGroupOperationLocked(table_name)) {
                self.table_activity_ready.waitUncancelable(io, &self.table_activity_mutex);
                continue;
            }
            const entry = self.activityEntryLocked(table_name, null);
            entry.structural_active = true;
            return;
        }
    }

    fn endStructuralTableActivityLocked(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        const io = self.table_activity_threaded.io();
        const index = self.findTableActivityLocked(table_name, null) orelse unreachable;
        self.active_table_activities.items[index].structural_active = false;
        self.pruneTableActivityLocked(table_name, null);
        self.table_activity_ready.broadcast(io);
    }

    fn waitForNoStructuralActivity(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        const io = self.table_activity_threaded.io();
        self.table_activity_mutex.lockUncancelable(io);
        defer self.table_activity_mutex.unlock(io);
        self.waitForNoStructuralActivityLocked(table_name);
    }

    fn waitForNoReadBlockingActivityLocked(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        const io = self.table_activity_threaded.io();
        while (true) {
            if (self.findTableActivityLocked(table_name, null)) |index| {
                const entry = self.active_table_activities.items[index];
                if (entry.structural_active or entry.table_request_active > 0) {
                    self.table_activity_ready.waitUncancelable(io, &self.table_activity_mutex);
                    continue;
                }
            }
            if (self.hasAnyActiveGroupOperationLocked(table_name)) {
                self.table_activity_ready.waitUncancelable(io, &self.table_activity_mutex);
                continue;
            }
            return;
        }
    }

    fn waitForNoReadBlockingActivity(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        const io = self.table_activity_threaded.io();
        self.table_activity_mutex.lockUncancelable(io);
        defer self.table_activity_mutex.unlock(io);
        self.waitForNoReadBlockingActivityLocked(table_name);
    }

    pub fn beginTableRequest(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        const io = self.table_activity_threaded.io();
        self.table_activity_mutex.lockUncancelable(io);
        defer self.table_activity_mutex.unlock(io);
        self.beginTableRequestLocked(table_name);
    }

    pub fn endTableRequest(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        const io = self.table_activity_threaded.io();
        self.table_activity_mutex.lockUncancelable(io);
        defer self.table_activity_mutex.unlock(io);
        self.endTableRequestLocked(table_name);
    }

    pub fn beginGroupOperation(self: *ProvisionedTableWriteSource, table_name: []const u8, group_id: u64) void {
        const io = self.table_activity_threaded.io();
        self.table_activity_mutex.lockUncancelable(io);
        defer self.table_activity_mutex.unlock(io);
        self.beginGroupOperationLocked(table_name, group_id);
    }

    pub fn testingMarkTableRequestActive(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        if (!builtin.is_test) @compileError("testingMarkTableRequestActive is test-only");
        const io = self.table_activity_threaded.io();
        self.table_activity_mutex.lockUncancelable(io);
        defer self.table_activity_mutex.unlock(io);
        const entry = self.activityEntryLocked(table_name, null);
        entry.table_request_active += 1;
    }

    pub fn testingMarkGroupOperationActive(self: *ProvisionedTableWriteSource, table_name: []const u8, group_id: u64) void {
        if (!builtin.is_test) @compileError("testingMarkGroupOperationActive is test-only");
        const io = self.table_activity_threaded.io();
        self.table_activity_mutex.lockUncancelable(io);
        defer self.table_activity_mutex.unlock(io);
        const entry = self.activityEntryLocked(table_name, group_id);
        std.debug.assert(!entry.operation_active);
        entry.operation_active = true;
    }

    pub fn tryBeginGroupOperation(self: *ProvisionedTableWriteSource, table_name: []const u8, group_id: u64) bool {
        const io = self.table_activity_threaded.io();
        self.table_activity_mutex.lockUncancelable(io);
        defer self.table_activity_mutex.unlock(io);
        return self.tryBeginGroupOperationLocked(table_name, group_id);
    }

    pub fn endGroupOperation(self: *ProvisionedTableWriteSource, table_name: []const u8, group_id: u64) void {
        const io = self.table_activity_threaded.io();
        self.table_activity_mutex.lockUncancelable(io);
        defer self.table_activity_mutex.unlock(io);
        self.endGroupOperationLocked(table_name, group_id);
    }

    pub fn tryBeginStructuralTableActivity(self: *ProvisionedTableWriteSource, table_name: []const u8) bool {
        const io = self.table_activity_threaded.io();
        self.table_activity_mutex.lockUncancelable(io);
        defer self.table_activity_mutex.unlock(io);
        return self.tryBeginStructuralTableActivityLocked(table_name);
    }

    fn beginStructuralTableActivity(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        const io = self.table_activity_threaded.io();
        self.table_activity_mutex.lockUncancelable(io);
        defer self.table_activity_mutex.unlock(io);
        self.beginStructuralTableActivityLocked(table_name);
    }

    pub fn endStructuralTableActivity(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        const io = self.table_activity_threaded.io();
        self.table_activity_mutex.lockUncancelable(io);
        defer self.table_activity_mutex.unlock(io);
        self.endStructuralTableActivityLocked(table_name);
    }

    pub fn beginLocalStructuralMutation(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        self.beginStructuralTableActivity(table_name);
        lockAtomic(&self.local_db_mutex);
        self.invalidateWriteCache(table_name);
        self.invalidateReadCache(table_name);
        self.invalidateRuntimeStatusCache(table_name);
    }

    pub fn finishLocalStructuralMutation(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        self.invalidateReadCache(table_name);
        self.local_db_mutex.unlock();
        self.endStructuralTableActivity(table_name);
    }

    fn abortLocalStructuralMutation(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        self.invalidateReadCache(table_name);
        self.invalidateWriteCache(table_name);
        self.invalidateRuntimeStatusCache(table_name);
        self.local_db_mutex.unlock();
        self.endStructuralTableActivity(table_name);
    }

    pub fn getOrOpenCachedDbMode(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        cache: *ProvisionedTableWriteCache,
        path: []const u8,
        group_id: u64,
        table_name: []const u8,
        mode: ManagedDbOpenMode,
        finish_expired_auto_bulk_now_ns: ?u64,
        ensure_auto_bulk_now_ns: ?u64,
    ) !ProvisionedTableWriteCache.CachedDb {
        return try self.getOrOpenCachedDbModeAtGeneration(
            alloc,
            cache,
            path,
            group_id,
            self.visibleRootGeneration(group_id),
            table_name,
            mode,
            finish_expired_auto_bulk_now_ns,
            ensure_auto_bulk_now_ns,
        );
    }

    fn getOrOpenCachedDbModeAtGeneration(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        cache: *ProvisionedTableWriteCache,
        path: []const u8,
        group_id: u64,
        lsm_root_generation: u64,
        table_name: []const u8,
        mode: ManagedDbOpenMode,
        finish_expired_auto_bulk_now_ns: ?u64,
        ensure_auto_bulk_now_ns: ?u64,
    ) !ProvisionedTableWriteCache.CachedDb {
        _ = alloc;
        if (cache.backend_runtime == null) cache.backend_runtime = self.backend_runtime;
        cache.antfly_provider = self.antfly_provider;
        cache.remote_content = self.remote_content;
        self.syncRuntimeHooksToCache(cache);
        const identity_namespace = try loadTableIdentityNamespaceForGroup(cache.alloc, self.catalog, table_name, group_id);
        const expected_identity_namespace = if (mode == .startup_catch_up or mode == .restore_repair)
            null
        else
            identity_namespace;
        if (mode == .status_only) {
            lockAtomic(&self.local_db_mutex);
            defer self.local_db_mutex.unlock();
            if (finish_expired_auto_bulk_now_ns) |now_ns| {
                _ = try cache.finishExpiredAutoBulkIngestLocked(now_ns);
            }
            const cached = try cache.getOrOpenLockedMode(path, self.catalog, group_id, lsm_root_generation, table_name, .status_only);
            try validateProvisionedDbIdentityNamespaceExpected(expected_identity_namespace, cached.db);
            return cached;
        }

        var prepared_open: ?ProvisionedTableWriteCache.PreparedOpen = null;
        defer if (prepared_open) |*prepared| prepared.deinit(cache.alloc);

        {
            lockAtomic(&self.local_db_mutex);
            defer self.local_db_mutex.unlock();
            if (finish_expired_auto_bulk_now_ns) |now_ns| {
                _ = cache.finishExpiredAutoBulkIngestLocked(now_ns) catch |err| {
                    if (!isTransientReplayVisibilityError(err)) return err;
                    std.log.warn("auto bulk ingest expired finish deferred table={s} group_id={} err={s}", .{
                        table_name,
                        group_id,
                        @errorName(err),
                    });
                };
            }
            switch (try cache.getOrPrepareOpenLocked(group_id, lsm_root_generation, table_name)) {
                .cached => |cached| {
                    try validateProvisionedDbIdentityNamespaceExpected(expected_identity_namespace, cached.db);
                    if (mode == .default or mode == .default_async) {
                        cached.db.setQueryVisibilityHook(self.managedDerivedVisibilityHook(cached.entry.?.table_name, group_id, cached.db));
                    }
                    if (ensure_auto_bulk_now_ns) |now_ns| try cache.ensureAutoBulkIngestLocked(group_id, table_name, now_ns);
                    return cached;
                },
                .prepared => |prepared| prepared_open = prepared,
            }
        }

        lockAtomic(&cache.open_mutex);
        defer cache.open_mutex.unlock();

        {
            lockAtomic(&self.local_db_mutex);
            defer self.local_db_mutex.unlock();
            switch (try cache.getOrPrepareOpenLocked(group_id, lsm_root_generation, table_name)) {
                .cached => |cached| {
                    try validateProvisionedDbIdentityNamespaceExpected(expected_identity_namespace, cached.db);
                    prepared_open.?.deinit(cache.alloc);
                    prepared_open = null;
                    if (mode == .default or mode == .default_async) {
                        cached.db.setQueryVisibilityHook(self.managedDerivedVisibilityHook(cached.entry.?.table_name, group_id, cached.db));
                    }
                    if (ensure_auto_bulk_now_ns) |now_ns| try cache.ensureAutoBulkIngestLocked(group_id, table_name, now_ns);
                    return cached;
                },
                .prepared => |prepared| {
                    prepared_open.?.deinit(cache.alloc);
                    prepared_open = prepared;
                },
            }
        }

        if (try loadTableManagedMetadata(cache.alloc, self.catalog, table_name)) |metadata| {
            prepared_open.?.indexes_json = metadata.indexes_json;
            prepared_open.?.schema_json = metadata.schema_json;
        }

        const effective_ha_mirror = haMirrorForManagedDbOpenMode(mode, self.ha_async_mirror);
        var opened: ?db_mod.DB = if (prepared_open.?.indexes_json) |value|
            try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentityWithOptions(
                cache.alloc,
                path,
                value,
                cache.lsm_cache,
                cache.hbc_cache,
                lsm_root_generation,
                cache.resource_manager,
                mode,
                cache.backend_runtime,
                self.antfly_provider,
                self.secret_store,
                cache.remote_content,
                identity_namespace,
                .{
                    .drain_resolver_backfill = false,
                    .ha_write_gate = self.ha_write_gate,
                    .ha_async_effect_mirror = effective_ha_mirror,
                    .ha_async_batch_mirror = effective_ha_mirror,
                    .ha_async_metadata_mirror = effective_ha_mirror,
                },
            )
        else
            try db_mod.DB.open(cache.alloc, path, .{
                .lsm_cache = cache.lsm_cache,
                .hbc_cache = cache.hbc_cache,
                .lsm_root_generation = lsm_root_generation,
                .resource_manager = cache.resource_manager,
                .backend_runtime = cache.backend_runtime,
                .identity_namespace = identity_namespace,
                .prefer_existing_identity_namespace = identity_namespace != null,
                .ha_write_gate = self.ha_write_gate,
                .ha_async_effect_mirror = effective_ha_mirror,
                .ha_async_batch_mirror = effective_ha_mirror,
                .ha_async_metadata_mirror = effective_ha_mirror,
                .open_mode = switch (mode) {
                    .default => .writer,
                    .default_async, .writer_no_replay => .writer_no_replay,
                    .startup_catch_up, .restore_repair => .writer_no_replay,
                    .query_readonly => .query_readonly,
                    .status_only => .status_only,
                },
                .index_open_parallelism = if (mode == .default_async or mode == .writer_no_replay) 1 else null,
                .start_index_workers = if (mode == .startup_catch_up) false else true,
                .start_optional_runtimes = mode != .startup_catch_up,
                .ttl_cleanup = if (mode == .startup_catch_up or mode == .restore_repair) .{ .enabled = false } else .{},
                .transaction_recovery = if (mode == .startup_catch_up or mode == .restore_repair) .{ .enabled = false } else .{},
                .text_merge = if (mode == .startup_catch_up or mode == .restore_repair) .{ .enabled = false } else .{},
            });
        defer if (opened) |*db| db.close();
        try validateProvisionedDbIdentityNamespaceExpected(identity_namespace, &opened.?);
        if (prepared_open.?.schema_json) |schema_json| {
            if (schema_json.len > 0) try applyLocalTableSchemaJson(cache.alloc, &opened.?, schema_json);
        }

        var cached = blk: {
            lockAtomic(&self.local_db_mutex);
            defer self.local_db_mutex.unlock();
            try cache.retired_entries.ensureUnusedCapacity(cache.alloc, 1);
            const adopted = try cache.adoptPreparedOpenLocked(&opened, group_id, lsm_root_generation, table_name, mode, &prepared_open.?);
            if (mode == .default or mode == .default_async) {
                adopted.db.setQueryVisibilityHook(self.managedDerivedVisibilityHook(adopted.entry.?.table_name, group_id, adopted.db));
            }
            if (ensure_auto_bulk_now_ns) |now_ns| try cache.ensureAutoBulkIngestLocked(group_id, table_name, now_ns);
            break :blk adopted;
        };
        errdefer {
            lockAtomic(&self.local_db_mutex);
            defer self.local_db_mutex.unlock();
            cache.retireFailedOpenLocked(&cached);
        }
        // .default_async opens run on the raft apply thread
        // (applyReplicatedBatchGroupLocal). Draining resolver backfill there
        // blocks the raft loop on the promotion pipeline, whose cross-shard
        // entity upserts need raft applies that are queued behind this very
        // open — observed as a full apply wedge (batch writes timing out
        // cluster-wide) in the multinode autograph e2e. The promotion and
        // resolution workers started by this open drain the same backlog
        // asynchronously instead.
        if (mode != .default_async) try cached.db.drainResolverBackfill();
        return cached;
    }

    pub fn setLocalChangeHook(self: *ProvisionedTableWriteSource, hook: ?LocalChangeHook) void {
        self.local_change_hook = hook;
    }

    fn invalidateReadCache(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        if (self.read_cache) |cache| cache.invalidateTable(table_name);
    }

    fn invalidateWriteCacheForTable(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        lockAtomic(&self.local_db_mutex);
        defer self.local_db_mutex.unlock();
        if (self.write_cache) |cache| cache.invalidateTable(table_name);
    }

    fn invalidateRuntimeStatusCache(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        if (self.runtime_status_cache) |snapshot_cache| snapshot_cache.invalidateTable(table_name);
    }

    pub fn managedDerivedVisibilityHook(
        self: *ProvisionedTableWriteSource,
        table_name: []const u8,
        group_id: u64,
        db: *db_mod.DB,
    ) db_mod.QueryVisibilityHook {
        return .{
            .ptr = self,
            .table_name = table_name,
            .group_id = group_id,
            .db = db,
            .on_change = onManagedDerivedVisibilityChanged,
        };
    }

    pub fn publishManagedRuntimeStatusBestEffort(
        self: *ProvisionedTableWriteSource,
        table_name: []const u8,
        group_id: u64,
        db: *db_mod.DB,
    ) bool {
        const snapshot_cache = self.runtime_status_cache orelse return false;
        publishRuntimeStatusSnapshot(self, snapshot_cache.alloc, table_name, group_id, db) catch |err| {
            std.log.warn("managed runtime status publish failed table={s} group_id={} err={s}", .{
                table_name,
                group_id,
                @errorName(err),
            });
            return false;
        };
        return true;
    }

    fn overlayCachedManagedRuntimeStatusBestEffort(
        self: *ProvisionedTableWriteSource,
        table_name: []const u8,
        group_id: u64,
        db: *db_mod.DB,
    ) bool {
        const snapshot_cache = self.runtime_status_cache orelse return false;
        var status = (snapshot_cache.snapshotGroupStatus(snapshot_cache.alloc, table_name, group_id) catch |err| {
            std.log.warn("managed runtime status cached snapshot lookup failed table={s} group_id={} err={s}", .{
                table_name,
                group_id,
                @errorName(err),
            });
            return false;
        }) orelse return false;
        defer status.deinit(snapshot_cache.alloc);

        db.overlayRuntimeStatusBestEffort(snapshot_cache.alloc, &status.stats);
        snapshot_cache.upsertGroupStatus(table_name, status) catch |err| {
            std.log.warn("managed runtime status overlay publish failed table={s} group_id={} err={s}", .{
                table_name,
                group_id,
                @errorName(err),
            });
            return false;
        };
        return true;
    }

    pub fn onManagedDerivedVisibilityChanged(
        ptr: *anyopaque,
        table_name: []const u8,
        group_id: u64,
        db: ?*db_mod.DB,
        change: db_mod.QueryVisibilityChange,
    ) void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        switch (change) {
            .publish => {
                if (db) |managed_db| {
                    if (self.publishManagedRuntimeStatusBestEffort(table_name, group_id, managed_db)) {
                        self.invalidateReadCache(table_name);
                        self.clearDirtyWriteTable(table_name);
                        self.notifyLocalChange(table_name, .data);
                        return;
                    }
                }
                lockAtomic(&self.local_db_mutex);
                self.markWriteCacheDirty(table_name);
                self.local_db_mutex.unlock();
            },
            .publish_consistent => {
                if (db) |managed_db| {
                    if (self.runtime_status_cache) |snapshot_cache| {
                        const published = tryPublishRuntimeStatusSnapshotConsistent(self, snapshot_cache.alloc, table_name, group_id, managed_db) catch |err| {
                            std.log.warn("managed runtime status consistent publish failed table={s} group_id={} err={s}", .{
                                table_name,
                                group_id,
                                @errorName(err),
                            });
                            lockAtomic(&self.local_db_mutex);
                            self.markWriteCacheDirty(table_name);
                            self.local_db_mutex.unlock();
                            return;
                        };
                        if (published) {
                            self.invalidateReadCache(table_name);
                            self.clearDirtyWriteTable(table_name);
                            self.notifyLocalChange(table_name, .data);
                            return;
                        }
                    }
                }
                lockAtomic(&self.local_db_mutex);
                self.markWriteCacheDirty(table_name);
                self.local_db_mutex.unlock();
            },
            .invalidate => {},
        }
        self.invalidateReadCache(table_name);
        self.notifyLocalChange(table_name, .data);
    }

    pub fn invalidateWriteCache(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        _ = self.publishWriteCacheStatusBeforeInvalidate(table_name);
        self.detachWriteCacheVisibilityHooksBeforeInvalidate(table_name);
        if (self.write_cache) |cache| cache.invalidateTable(table_name);
        if (self.startup_write_cache) |cache| cache.invalidateTable(table_name);
        self.clearDirtyWriteTable(table_name);
    }

    fn publishWriteCacheStatusBeforeInvalidate(self: *ProvisionedTableWriteSource, table_name: []const u8) bool {
        if (self.runtime_status_cache == null) return true;
        var published = true;
        published = self.publishCacheStatusBeforeInvalidate(table_name, self.write_cache) and published;
        published = self.publishCacheStatusBeforeInvalidate(table_name, self.startup_write_cache) and published;
        return published;
    }

    fn detachWriteCacheVisibilityHooksBeforeInvalidate(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        self.detachCacheVisibilityHooksBeforeInvalidate(table_name, self.write_cache);
        self.detachCacheVisibilityHooksBeforeInvalidate(table_name, self.startup_write_cache);
    }

    fn detachCacheVisibilityHooksBeforeInvalidate(
        self: *ProvisionedTableWriteSource,
        table_name: []const u8,
        maybe_cache: ?*ProvisionedTableWriteCache,
    ) void {
        const cache = maybe_cache orelse return;
        for (cache.entries.items) |entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.lsm_root_generation != self.visibleRootGeneration(entry.group_id)) continue;
            entry.db.setQueryVisibilityHook(null);
        }
    }

    fn publishCacheStatusBeforeInvalidate(
        self: *ProvisionedTableWriteSource,
        table_name: []const u8,
        maybe_cache: ?*ProvisionedTableWriteCache,
    ) bool {
        const cache = maybe_cache orelse return true;
        var published = true;
        for (cache.entries.items) |entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.lsm_root_generation != self.visibleRootGeneration(entry.group_id)) continue;
            self.finishEntryAutoBulkIngestForForegroundVisibility(cache, entry) catch |err| {
                if (!isTransientReplayVisibilityError(err)) {
                    std.log.warn("managed writer auto bulk finish before status publish failed table={s} group_id={} err={s}", .{
                        entry.table_name,
                        entry.group_id,
                        @errorName(err),
                    });
                }
                published = false;
                continue;
            };
            published = self.publishManagedRuntimeStatusBestEffort(entry.table_name, entry.group_id, &entry.db) and published;
        }
        return published;
    }

    fn finishEntryAutoBulkIngestForForegroundVisibility(
        self: *ProvisionedTableWriteSource,
        cache: *ProvisionedTableWriteCache,
        entry: *ProvisionedTableWriteCache.Entry,
    ) !void {
        if (!entry.auto_bulk_ingest_session_open) return;
        try entry.db.finishDenseAutoBulkIngestSessionWithOptions(auto_bulk_ingest_finish_options);
        entry.bulk_ingest_session_open = false;
        entry.auto_bulk_ingest_session_open = false;
        entry.auto_bulk_ingest_ops = 0;
        entry.auto_bulk_ingest_started_ns = 0;
        entry.auto_bulk_ingest_last_ns = 0;
        entry.auto_bulk_ingest_finish_requested = false;
        cache.removeInactiveBulkIngestSessionLocked(entry.table_name);
        _ = self;
    }

    pub fn pruneStaleWriteCacheLocked(self: *ProvisionedTableWriteSource) void {
        const pruneCache = struct {
            fn run(write_source: *ProvisionedTableWriteSource, cache: *ProvisionedTableWriteCache) void {
                var leased_retirements: usize = 0;
                for (cache.entries.items) |entry| {
                    const current_generation = write_source.visibleRootGeneration(entry.group_id);
                    if (entry.lsm_root_generation == current_generation) continue;
                    if (entry.active_leases > 0) leased_retirements += 1;
                }
                cache.retired_entries.ensureUnusedCapacity(cache.alloc, leased_retirements) catch return;

                var i: usize = 0;
                while (i < cache.entries.items.len) {
                    const entry = cache.entries.items[i];
                    const current_generation = write_source.visibleRootGeneration(entry.group_id);
                    if (entry.lsm_root_generation == current_generation) {
                        i += 1;
                        continue;
                    }
                    if (cache.adoptSeededEntryGenerationLocked(entry, current_generation)) {
                        i += 1;
                        continue;
                    }
                    _ = cache.entries.orderedRemove(i);
                    cache.retireEntryLocked(entry);
                }

                var session_index: usize = 0;
                while (session_index < cache.active_bulk_ingest_sessions.items.len) {
                    const table_name = cache.active_bulk_ingest_sessions.items[session_index].table_name;
                    for (cache.entries.items) |entry| {
                        if (std.mem.eql(u8, entry.table_name, table_name)) break;
                    } else {
                        var removed = cache.active_bulk_ingest_sessions.orderedRemove(session_index);
                        removed.deinit(cache.alloc);
                        continue;
                    }
                    session_index += 1;
                }
            }
        }.run;

        if (self.write_cache) |cache| pruneCache(self, cache);
        if (self.startup_write_cache) |cache| pruneCache(self, cache);
    }

    pub fn reconcileReplicaRootTablesWithWriteCacheLocked(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        metadata_group_id: u64,
        hosted_group_ids: []const u64,
        tables: []const metadata_table_manager.TableRecord,
        ranges: []const metadata_table_manager.RangeRecord,
        backend_runtime: ?*db_mod.background_runtime.BackendRuntime,
    ) !metadata_table_provisioner.ProvisionSummary {
        const cache = self.write_cache orelse return try metadata_table_provisioner.reconcileReplicaRootWithOptions(
            alloc,
            self.replica_root_dir,
            metadata_group_id,
            hosted_group_ids,
            tables,
            ranges,
            .{ .backend_runtime = backend_runtime },
        );

        if (cache.backend_runtime == null) cache.backend_runtime = backend_runtime orelse self.backend_runtime;
        cache.antfly_provider = self.antfly_provider;
        cache.secret_store = self.secret_store;
        cache.remote_content = self.remote_content;

        var summary: metadata_table_provisioner.ProvisionSummary = .{};
        for (hosted_group_ids) |group_id| {
            if (group_id == metadata_group_id) continue;
            const range = findRangeRecord(ranges, group_id) orelse continue;
            const table = findTableRecord(tables, range.table_id) orelse continue;
            summary.groups_considered += 1;

            const path = try metadata_table_provisioner.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
            defer alloc.free(path);

            var io_impl = std.Io.Threaded.init(alloc, .{});
            defer io_impl.deinit();
            try fs_paths.createDirPathPortable(io_impl.io(), path);
            try metadata_table_provisioner.applyRestoreIntentIfNeeded(alloc, path, group_id, table, range);

            const lsm_root_generation = self.visibleRootGeneration(group_id);
            const identity_namespace = doc_identity.Namespace{
                .table_id = table.table_id,
                .shard_id = metadata_table_manager.rangeDocIdentityShardId(range),
                .range_id = metadata_table_manager.rangeDocIdentityRangeId(range),
            };
            if (cache.getLocked(group_id, lsm_root_generation, table.name)) |db| {
                try validateProvisionedDbIdentityNamespaceExpected(identity_namespace, db);
                try applyLocalTableSchemaJson(alloc, db, table.schema_json);
                const index_summary = try metadata_table_provisioner.reconcileDbIndexes(alloc, db, table.indexes_json);
                summary.indexes_removed += index_summary.indexes_removed;
                summary.indexes_added += index_summary.indexes_added;
                summary.enrichments_added += index_summary.enrichments_added;
                summary.enrichments_updated += index_summary.enrichments_updated;
                summary.enrichments_removed += index_summary.enrichments_removed;
                summary.resolvers_added += index_summary.resolvers_added;
                summary.resolvers_updated += index_summary.resolvers_updated;
                summary.resolvers_removed += index_summary.resolvers_removed;
                try cache.replaceTableMetadataLocked(table.name, table.indexes_json, table.schema_json);
                continue;
            }
            var opened: ?db_mod.DB = try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentity(
                alloc,
                path,
                table.indexes_json,
                cache.lsm_cache,
                cache.hbc_cache,
                lsm_root_generation,
                cache.resource_manager,
                .default,
                cache.backend_runtime,
                self.antfly_provider,
                self.secret_store,
                self.remote_content,
                identity_namespace,
            );
            defer if (opened) |*db| db.close();
            summary.dbs_opened += 1;

            try applyLocalTableSchemaJson(alloc, &opened.?, table.schema_json);
            try cache.seedCreatedDbLocked(&opened, group_id, lsm_root_generation, table.name, table.indexes_json, table.schema_json);
        }
        return summary;
    }

    pub fn transferAdoptableWriteCacheEntriesTo(
        self: *ProvisionedTableWriteSource,
        dest: *ProvisionedTableWriteSource,
        table_name: []const u8,
    ) !usize {
        if (self == dest) return 0;
        const source_cache = self.write_cache orelse return 0;
        const dest_cache = dest.write_cache orelse return 0;
        if (source_cache == dest_cache) return 0;

        const self_mutex = self.localDbMutex();
        const dest_mutex = dest.localDbMutex();
        if (@intFromPtr(self_mutex) < @intFromPtr(dest_mutex)) {
            lockAtomic(self_mutex);
            defer self_mutex.unlock();
            lockAtomic(dest_mutex);
            defer dest_mutex.unlock();
        } else {
            lockAtomic(dest_mutex);
            defer dest_mutex.unlock();
            lockAtomic(self_mutex);
            defer self_mutex.unlock();
        }
        const CacheGenerationAdapter = struct {
            const vtable = table_write_cache.VisibleRootGenerationSource.VTable{
                .visible_root_generation_for_group = visibleRootGenerationForGroup,
            };

            fn visibleRootGenerationForGroup(ptr: *anyopaque, group_id: u64) u64 {
                const generation_source: *table_read_core.GroupVisibleRootGenerationSource = @ptrCast(@alignCast(ptr));
                return generation_source.visibleRootGenerationForGroup(group_id);
            }
        };
        var read_generation_source = dest.groupVisibleRootGenerationSource();
        const cache_generation_source: ?table_write_cache.VisibleRootGenerationSource = if (read_generation_source) |*generation_source|
            .{ .ptr = generation_source, .vtable = &CacheGenerationAdapter.vtable }
        else
            null;
        return try source_cache.transferAdoptableEntriesForTableLocked(dest_cache, table_name, cache_generation_source);
    }

    pub fn clearStartupWriteCacheLocked(self: *ProvisionedTableWriteSource) void {
        if (self.startup_write_cache) |cache| cache.clear();
    }

    pub fn clearStartupWriteCache(self: *ProvisionedTableWriteSource) void {
        lockAtomic(&self.local_db_mutex);
        defer self.local_db_mutex.unlock();
        self.clearStartupWriteCacheLocked();
    }

    pub fn clearWriteCacheLocked(self: *ProvisionedTableWriteSource) void {
        if (self.write_cache) |cache| cache.clear();
        self.clearStartupWriteCacheLocked();
        self.clearAllDirtyWriteTables();
    }

    pub fn clearWriteCache(self: *ProvisionedTableWriteSource) void {
        lockAtomic(&self.local_db_mutex);
        defer self.local_db_mutex.unlock();
        self.clearWriteCacheLocked();
    }

    pub fn warmTableGroup(self: *ProvisionedTableWriteSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8) !void {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);

        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);

        if (self.write_cache) |cache| {
            var cached = try self.getOrOpenCachedDbMode(alloc, cache, path, group_id, table_name, .default, null, null);
            defer cached.deinit(alloc);
            return;
        }

        var db = try openManagedDbForTableGroupWithRuntimeAndHAWriteGate(alloc, path, self.catalog, table_name, group_id, self.backend_runtime, self.ha_write_gate, self.ha_async_mirror);
        db.close();
    }

    pub fn findMedianKeyForGroup(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        lsm_root_generation: u64,
    ) !?[]u8 {
        var snapshot = try self.catalog.adminSnapshot();
        defer self.catalog.freeAdminSnapshot(&snapshot);

        const range = metadata_mod.findAdminRange(&snapshot, group_id) orelse return error.UnknownGroup;
        const table = metadata_mod.findAdminTable(&snapshot, range.table_id) orelse return error.TableNotFound;
        return try self.findMedianKeyForTableGroup(alloc, group_id, lsm_root_generation, table.name);
    }

    pub fn findMedianKeyForTableGroup(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        lsm_root_generation: u64,
        table_name: []const u8,
    ) !?[]u8 {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);

        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);

        if (self.write_cache) |cache| {
            var cached = try self.getOrOpenCachedDbModeAtGeneration(alloc, cache, path, group_id, lsm_root_generation, table_name, .default_async, null, null);
            defer cached.deinit(alloc);
            if (cached.entry) |entry| {
                try self.finishEntryAutoBulkIngestForForegroundVisibility(cache, entry);
            }
            try drainManagedDbBeforeClose(cached.db);
            const median = cached.db.findMedianKey(alloc) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            };
            return median;
        }

        var db = try openManagedDbForTableGroupWithCacheAndRuntime(alloc, path, self.catalog, table_name, group_id, null, null, lsm_root_generation, null, self.backend_runtime);
        defer db.close();
        const median = db.findMedianKey(alloc) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        return median;
    }

    pub fn catchUpTableGroupBestEffort(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
    ) !StartupCatchUpResult {
        return try self.catchUpTableGroupBestEffortWithIndexesJson(alloc, group_id, table_name, null);
    }

    pub fn catchUpTableGroupBestEffortWithIndexesJson(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        cached_indexes_json: ?[]const u8,
    ) !StartupCatchUpResult {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const lsm_root_generation = self.visibleRootGeneration(group_id);

        if (!self.tryBeginGroupOperation(table_name, group_id)) return .{ .busy = true };
        if (!self.local_db_mutex.tryLock()) {
            self.endGroupOperation(table_name, group_id);
            return .{ .busy = true };
        }
        if (self.hasDirtyWriteTableWithLocalDbLocked(table_name)) {
            self.local_db_mutex.unlock();
            self.endGroupOperation(table_name, group_id);
            return .{ .busy = true };
        }
        if (self.write_cache) |cache| {
            if (cache.hasForegroundStateForGroupTableLocked(group_id, table_name)) {
                self.local_db_mutex.unlock();
                self.endGroupOperation(table_name, group_id);
                return .{ .busy = true };
            }
        }
        self.local_db_mutex.unlock();
        defer {
            lockAtomic(&self.local_db_mutex);
            self.clearStartupWriteCacheLocked();
            self.local_db_mutex.unlock();
            self.endGroupOperation(table_name, group_id);
        }
        self.startup_catch_up_active.store(true, .monotonic);
        defer self.startup_catch_up_active.store(false, .monotonic);

        const owned_indexes_json = if (cached_indexes_json == null) try loadTableIndexesJson(alloc, self.catalog, table_name) else null;
        defer if (owned_indexes_json) |value| alloc.free(value);
        const indexes_json = cached_indexes_json orelse owned_indexes_json;
        var configured_indexes_storage: ?StartupConfiguredIndexes = null;
        if (indexes_json) |value| configured_indexes_storage = try parseStartupConfiguredIndexes(alloc, value);
        defer if (configured_indexes_storage) |*summary| summary.deinit(alloc);
        const configured_indexes = if (configured_indexes_storage) |*summary| summary else null;
        const opening_db_startup = startupCatchUpStatsForPath(path, .opening_db, configured_indexes) catch db_mod.types.StartupCatchUpStats{
            .active = true,
            .phase = .opening_db,
        };
        try publishStartupCatchUpRuntimeStatusSnapshot(self, alloc, table_name, group_id, opening_db_startup, null, configured_indexes);
        errdefer publishStartupCatchUpRuntimeStatusSnapshot(self, alloc, table_name, group_id, .{}, null, null) catch {};
        _ = db_mod.DB.recoverIncompleteRestoreImportIfNeeded(alloc, path, .{}) catch |err| {
            std.log.warn("managed startup catch-up restore import recovery failed table={s} err={}", .{ table_name, err });
            return err;
        };
        const restore_repair_needed = db_mod.DB.restoreRuntimeRepairNeededForPath(alloc, path) catch |err| {
            std.log.warn("managed startup catch-up restore repair probe failed table={s} err={}", .{ table_name, err });
            return err;
        };
        const startup_open_mode: ManagedDbOpenMode = if (restore_repair_needed) .restore_repair else .startup_catch_up;
        const startup_cache = self.startup_write_cache;
        var cached_db: ?ProvisionedTableWriteCache.CachedDb = null;
        defer if (cached_db) |*cached| cached.deinit(alloc);
        var uncached_db: ?db_mod.DB = null;
        var uncached_promotion_owner_state: ProvisionedTableWriteCache.PromotionOwnerState = .{};
        const db = db_blk: {
            if (startup_cache) |cache| {
                cached_db = try self.getOrOpenCachedDbMode(alloc, cache, path, group_id, table_name, startup_open_mode, null, null);
                break :db_blk cached_db.?.db;
            }

            const identity_namespace = if (cached_indexes_json == null)
                try loadTableIdentityNamespaceForGroup(alloc, self.catalog, table_name, group_id)
            else
                null;
            const effective_ha_mirror = haMirrorForManagedDbOpenMode(startup_open_mode, self.ha_async_mirror);
            uncached_db = if (indexes_json) |value|
                try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentityWithOptions(
                    alloc,
                    path,
                    value,
                    null,
                    null,
                    lsm_root_generation,
                    null,
                    startup_open_mode,
                    self.backend_runtime,
                    self.antfly_provider,
                    self.secret_store,
                    self.remote_content,
                    identity_namespace,
                    .{
                        .drain_resolver_backfill = false,
                        .ha_write_gate = self.ha_write_gate,
                        .ha_async_effect_mirror = effective_ha_mirror,
                        .ha_async_batch_mirror = effective_ha_mirror,
                        .ha_async_metadata_mirror = effective_ha_mirror,
                    },
                )
            else
                try db_mod.DB.open(alloc, path, .{
                    .open_mode = .writer_no_replay,
                    .lsm_root_generation = lsm_root_generation,
                    .backend_runtime = self.backend_runtime,
                    .start_index_workers = false,
                    .start_optional_runtimes = false,
                    .ttl_cleanup = .{ .enabled = false },
                    .transaction_recovery = .{ .enabled = false },
                    .text_merge = .{ .enabled = false },
                    .identity_namespace = identity_namespace,
                    .prefer_existing_identity_namespace = identity_namespace != null,
                    .ha_write_gate = self.ha_write_gate,
                    .ha_async_effect_mirror = effective_ha_mirror,
                    .ha_async_batch_mirror = effective_ha_mirror,
                    .ha_async_metadata_mirror = effective_ha_mirror,
                });
            errdefer if (uncached_db) |*owned| owned.close();
            try validateProvisionedDbIdentityNamespaceExpected(identity_namespace, &uncached_db.?);
            self.applyRuntimeHooksToUncachedDb(&uncached_db.?, group_id, &uncached_promotion_owner_state);
            try uncached_db.?.drainResolverBackfill();
            break :db_blk &uncached_db.?;
        };
        defer if (uncached_db) |*owned| owned.close();
        try publishStartupCatchUpRuntimeStatusSnapshot(
            self,
            alloc,
            table_name,
            group_id,
            startupCatchUpStatsForPhase(.opening_db, db),
            db,
            configured_indexes,
        );
        const result = try catchUpManagedDb(self, alloc, group_id, table_name, db);
        try publishRuntimeStatusSnapshotWithStartupPhase(self, alloc, table_name, group_id, .idle, db);
        return result;
    }

    pub fn runLsmMaintenanceRound(self: *ProvisionedTableWriteSource) !bool {
        var leased = blk: {
            lockAtomic(&self.local_db_mutex);
            defer self.local_db_mutex.unlock();
            const cache = self.write_cache orelse return false;
            if (cache.maxLsmMaintenanceScoreLocked() == 0) return false;
            break :blk cache.leaseLsmMaintenanceRoundLocked() orelse return false;
        };
        defer {
            const release_alloc = if (leased.cache) |cache| cache.alloc else std.heap.page_allocator;
            leased.deinit(release_alloc);
        }
        return try leased.db.runLsmMaintenanceStep();
    }

    pub fn runLsmMaintenanceRoundBestEffort(self: *ProvisionedTableWriteSource) !bool {
        if (!self.local_db_mutex.tryLock()) return false;
        var leased = blk: {
            defer self.local_db_mutex.unlock();
            const cache = self.write_cache orelse return false;
            if (cache.maxLsmMaintenanceScoreLocked() == 0) return false;
            break :blk cache.leaseLsmMaintenanceRoundBestEffortLocked() orelse return false;
        };
        defer {
            const release_alloc = if (leased.cache) |cache| cache.alloc else std.heap.page_allocator;
            leased.deinit(release_alloc);
        }
        return try leased.db.runLsmMaintenanceStepBestEffort();
    }

    pub fn runDensePostingMaintenanceRoundBestEffort(self: *ProvisionedTableWriteSource) !usize {
        if (!self.local_db_mutex.tryLock()) return 0;
        var leased = blk: {
            defer self.local_db_mutex.unlock();
            const cache = self.write_cache orelse return 0;
            for (cache.entries.items) |entry| {
                if (entry.bulk_ingest_session_open) continue;
                if (entry.db.hasActiveDenseBulkWork()) continue;
                var leases = std.ArrayListUnmanaged(ProvisionedTableWriteCache.CachedDb).empty;
                defer leases.deinit(std.heap.page_allocator);
                try cache.appendRuntimeStatusLeaseForEntryLocked(std.heap.page_allocator, entry, &leases);
                break :blk leases.items[0];
            }
            return 0;
        };
        defer {
            const release_alloc = if (leased.cache) |cache| cache.alloc else std.heap.page_allocator;
            leased.deinit(release_alloc);
        }
        return try leased.db.runDensePostingMaintenanceForIdleBestEffort();
    }

    pub fn finishExpiredAutoBulkIngestBestEffort(self: *ProvisionedTableWriteSource) bool {
        return self.tryFinishExpiredAutoBulkIngest() orelse false;
    }

    pub fn tryFinishExpiredAutoBulkIngestAndPublishStatus(self: *ProvisionedTableWriteSource, alloc: std.mem.Allocator) ?bool {
        var leases = std.ArrayListUnmanaged(ProvisionedTableWriteCache.CachedDb).empty;
        defer {
            for (leases.items) |*lease| {
                const release_alloc = if (lease.cache) |cache| cache.alloc else std.heap.page_allocator;
                lease.deinit(release_alloc);
            }
            leases.deinit(alloc);
        }

        if (!self.local_db_mutex.tryLock()) return null;
        const finished = self.finishExpiredAutoBulkIngestLockedCollectingStatusLeases(alloc, &leases);
        self.local_db_mutex.unlock();

        self.publishRuntimeStatusLeaseSnapshots(alloc, leases.items);
        return finished;
    }

    pub fn tryFinishExpiredAutoBulkIngest(self: *ProvisionedTableWriteSource) ?bool {
        if (!self.local_db_mutex.tryLock()) return null;
        defer self.local_db_mutex.unlock();

        return self.finishExpiredAutoBulkIngestLocked();
    }

    pub fn finishExpiredAutoBulkIngest(self: *ProvisionedTableWriteSource) bool {
        lockAtomic(&self.local_db_mutex);
        defer self.local_db_mutex.unlock();

        return self.finishExpiredAutoBulkIngestLocked();
    }

    fn finishExpiredAutoBulkIngestLocked(self: *ProvisionedTableWriteSource) bool {
        return self.finishExpiredAutoBulkIngestLockedCollectingStatusLeases(std.heap.page_allocator, null);
    }

    fn finishExpiredAutoBulkIngestLockedCollectingStatusLeases(
        self: *ProvisionedTableWriteSource,
        lease_alloc: std.mem.Allocator,
        finished_leases: ?*std.ArrayListUnmanaged(ProvisionedTableWriteCache.CachedDb),
    ) bool {
        const now_ns = platform_time.monotonicNs();
        var finished_any = false;
        if (self.write_cache) |cache| {
            finished_any = (cache.finishExpiredAutoBulkIngestLockedWithStatusLeases(now_ns, lease_alloc, finished_leases) catch |err| {
                if (!isTransientReplayVisibilityError(err)) {
                    std.log.warn("auto bulk ingest background finish failed err={s}", .{@errorName(err)});
                }
                return false;
            }) or finished_any;
        }
        if (self.startup_write_cache) |cache| {
            finished_any = (cache.finishExpiredAutoBulkIngestLockedWithStatusLeases(now_ns, lease_alloc, finished_leases) catch |err| {
                if (!isTransientReplayVisibilityError(err)) {
                    std.log.warn("startup auto bulk ingest background finish failed err={s}", .{@errorName(err)});
                }
                return finished_any;
            }) or finished_any;
        }
        return finished_any;
    }

    pub fn publishCachedWriterRuntimeStatusesBestEffort(self: *ProvisionedTableWriteSource, alloc: std.mem.Allocator) void {
        var leases = std.ArrayListUnmanaged(ProvisionedTableWriteCache.CachedDb).empty;
        defer {
            for (leases.items) |*lease| {
                const release_alloc = if (lease.cache) |cache| cache.alloc else std.heap.page_allocator;
                lease.deinit(release_alloc);
            }
            leases.deinit(alloc);
        }

        if (!self.local_db_mutex.tryLock()) return;
        self.collectAllRuntimeStatusLeasesFromCacheLocked(alloc, self.write_cache, &leases) catch {
            self.local_db_mutex.unlock();
            return;
        };
        self.collectAllRuntimeStatusLeasesFromCacheLocked(alloc, self.startup_write_cache, &leases) catch {
            self.local_db_mutex.unlock();
            return;
        };
        self.local_db_mutex.unlock();

        self.publishRuntimeStatusLeaseSnapshots(alloc, leases.items);
    }

    fn publishDirtyWriteCacheRuntimeStatusesBestEffort(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) void {
        if (!self.isWriteCacheDirtyForTable(table_name)) return;
        self.publishWriteCacheRuntimeStatusesForTableBestEffort(alloc, table_name);
    }

    fn publishWriteCacheRuntimeStatusesForTableBestEffort(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) void {
        var leases = std.ArrayListUnmanaged(ProvisionedTableWriteCache.CachedDb).empty;
        defer {
            for (leases.items) |*lease| {
                const release_alloc = if (lease.cache) |cache| cache.alloc else std.heap.page_allocator;
                lease.deinit(release_alloc);
            }
            leases.deinit(alloc);
        }

        lockAtomic(&self.local_db_mutex);
        const has_leases = self.collectRuntimeStatusLeasesFromWriteCacheLocked(alloc, table_name, &leases) catch {
            self.local_db_mutex.unlock();
            return;
        };
        self.local_db_mutex.unlock();
        if (!has_leases) return;

        self.publishRuntimeStatusLeaseSnapshots(alloc, leases.items);
    }

    fn publishRuntimeStatusLeaseSnapshots(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        leases: []const ProvisionedTableWriteCache.CachedDb,
    ) void {
        for (leases) |lease| {
            const entry = lease.entry orelse continue;
            publishRuntimeStatusSnapshot(self, alloc, entry.table_name, entry.group_id, lease.db) catch |err| {
                std.log.warn("cached writer runtime status publish failed table={s} group_id={} err={s}", .{
                    entry.table_name,
                    entry.group_id,
                    @errorName(err),
                });
            };
        }
    }

    fn collectAllRuntimeStatusLeasesFromCacheLocked(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        maybe_cache: ?*ProvisionedTableWriteCache,
        out: *std.ArrayListUnmanaged(ProvisionedTableWriteCache.CachedDb),
    ) !void {
        const cache = maybe_cache orelse return;
        for (cache.entries.items) |entry| {
            if (entry.lsm_root_generation != self.visibleRootGeneration(entry.group_id)) continue;
            if (entry.bulk_ingest_session_open or entry.auto_bulk_ingest_session_open) continue;
            try cache.appendRuntimeStatusLeaseForEntryLocked(alloc, entry, out);
        }
    }

    pub fn lsmMaintenanceScore(self: *ProvisionedTableWriteSource) u64 {
        lockAtomic(&self.local_db_mutex);
        defer self.local_db_mutex.unlock();
        return if (self.write_cache) |cache| cache.maxLsmMaintenanceScoreLocked() else 0;
    }

    pub fn lsmMaintenanceScoreBestEffort(self: *ProvisionedTableWriteSource) u64 {
        if (!self.local_db_mutex.tryLock()) return 0;
        defer self.local_db_mutex.unlock();
        return if (self.write_cache) |cache| cache.maxLsmMaintenanceScoreLocked() else 0;
    }

    pub fn nextLsmMaintenanceWakeDelayNsBestEffort(self: *ProvisionedTableWriteSource) ?u64 {
        if (!self.local_db_mutex.tryLock()) return null;
        defer self.local_db_mutex.unlock();
        var best: ?u64 = null;
        if (self.write_cache) |cache| {
            for (cache.entries.items) |entry| {
                if (entry.db.nextLsmMaintenanceWakeDelayNsBestEffort()) |candidate| {
                    best = if (best) |current| @min(current, candidate) else candidate;
                }
            }
        }
        if (self.startup_write_cache) |cache| {
            for (cache.entries.items) |entry| {
                if (entry.db.nextLsmMaintenanceWakeDelayNsBestEffort()) |candidate| {
                    best = if (best) |current| @min(current, candidate) else candidate;
                }
            }
        }
        return best;
    }

    pub fn hasActiveBulkIngestSession(self: *ProvisionedTableWriteSource) bool {
        if (!self.local_db_mutex.tryLock()) return true;
        defer self.local_db_mutex.unlock();
        const cache = self.write_cache orelse return false;
        for (cache.entries.items) |entry| {
            if (entry.bulk_ingest_session_open) return true;
        }
        return false;
    }

    fn hasActiveBulkIngestSessionForTableBestEffort(
        self: *ProvisionedTableWriteSource,
        table_name: []const u8,
    ) bool {
        if (!self.local_db_mutex.tryLock()) return true;
        defer self.local_db_mutex.unlock();
        const cache = self.write_cache orelse return false;
        return cache.bulkIngestSessionOpenForTable(table_name);
    }

    pub const ManagedWriterGroupProbe = union(enum) {
        absent,
        unknown,
        leased: ProvisionedTableWriteCache.CachedDb,
    };

    pub fn probeManagedWriterGroupBestEffort(
        self: *ProvisionedTableWriteSource,
        table_name: []const u8,
        group_id: u64,
    ) ManagedWriterGroupProbe {
        if (!self.local_db_mutex.tryLock()) return .unknown;
        const lsm_root_generation = self.visibleRootGeneration(group_id);
        var leased: ?ProvisionedTableWriteCache.CachedDb = null;
        if (self.write_cache) |cache| {
            leased = cache.snapshotLeaseLocked(group_id, lsm_root_generation, table_name);
        }
        if (leased == null) {
            if (self.startup_write_cache) |cache| {
                leased = cache.snapshotLeaseLocked(group_id, lsm_root_generation, table_name);
            }
        }
        self.local_db_mutex.unlock();
        if (leased) |cached| return .{ .leased = cached };
        return .absent;
    }

    pub fn snapshotManagedWriterGroupStatusBestEffort(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_id: u64,
    ) !?runtime_status.LocalTableRuntimeStatus {
        return switch (self.probeManagedWriterGroupBestEffort(table_name, group_id)) {
            .absent, .unknown => null,
            .leased => |cached| blk: {
                var owned = cached;
                const release_alloc = if (owned.cache) |cache| cache.alloc else std.heap.page_allocator;
                defer owned.deinit(release_alloc);
                if (self.runtime_status_cache) |snapshot_cache| {
                    if (try snapshot_cache.snapshotGroupStatus(alloc, table_name, group_id)) |cached_status| {
                        var status = cached_status;
                        errdefer status.deinit(alloc);
                        owned.db.overlayRuntimeStatusBestEffort(alloc, &status.stats);
                        self.markManagedWriterRuntimeStatus(&status);
                        if (status.created_at_millis == 0) {
                            status.created_at_millis = (owned.db.getGroupCreatedAtMillis(alloc, group_id) catch null) orelse 0;
                        }
                        break :blk status;
                    }
                }
                var status = runtime_status.LocalTableRuntimeStatus{
                    .group_id = group_id,
                    .created_at_millis = (owned.db.getGroupCreatedAtMillis(alloc, group_id) catch null) orelse 0,
                    .stats = try owned.db.runtimeStatusStatsConsistent(alloc),
                };
                self.markManagedWriterRuntimeStatus(&status);
                break :blk status;
            },
        };
    }

    pub fn overlayManagedWriterGroupStatusBestEffort(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_id: u64,
        status: *runtime_status.LocalTableRuntimeStatus,
    ) void {
        switch (self.probeManagedWriterGroupBestEffort(table_name, group_id)) {
            .absent, .unknown => {},
            .leased => |cached| {
                var owned = cached;
                const release_alloc = if (owned.cache) |cache| cache.alloc else std.heap.page_allocator;
                defer owned.deinit(release_alloc);
                owned.db.overlayRuntimeStatusBestEffort(alloc, &status.stats);
                self.markManagedWriterRuntimeStatus(status);
                if (status.created_at_millis == 0) {
                    status.created_at_millis = (owned.db.getGroupCreatedAtMillis(std.heap.page_allocator, group_id) catch null) orelse 0;
                }
            },
        }
    }

    fn markManagedWriterRuntimeStatus(
        self: *ProvisionedTableWriteSource,
        status: *runtime_status.LocalTableRuntimeStatus,
    ) void {
        status.metadata = .{
            .updated_at_ns = platform_time.monotonicNs(),
            .source = if (self.startup_catch_up_active.load(.monotonic)) .startup_catch_up else .live_writer_publish,
            .freshness = .fresh,
        };
    }

    pub fn hasGroupActivityBestEffort(self: *ProvisionedTableWriteSource, table_name: []const u8, group_id: u64) bool {
        const io = self.table_activity_threaded.io();
        self.table_activity_mutex.lockUncancelable(io);
        defer self.table_activity_mutex.unlock(io);
        if (self.findTableActivityLocked(table_name, null)) |index| {
            if (self.active_table_activities.items[index].structural_active) return true;
        }
        if (self.findTableActivityLocked(table_name, group_id)) |index| {
            if (self.active_table_activities.items[index].operation_active) return true;
        }
        return false;
    }

    pub fn hasReadBlockingActivityBestEffort(self: *ProvisionedTableWriteSource, table_name: []const u8, group_id: u64) bool {
        const io = self.table_activity_threaded.io();
        self.table_activity_mutex.lockUncancelable(io);
        defer self.table_activity_mutex.unlock(io);
        if (self.findTableActivityLocked(table_name, null)) |index| {
            const entry = self.active_table_activities.items[index];
            if (entry.structural_active or entry.table_request_active > 0) return true;
        }
        if (self.findTableActivityLocked(table_name, group_id)) |index| {
            if (self.active_table_activities.items[index].operation_active) return true;
        }
        return false;
    }

    fn activeOperationGroupsForTable(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !struct {
        structural_busy: bool,
        request_busy: bool,
        groups: []u64,
    } {
        const io = self.table_activity_threaded.io();
        self.table_activity_mutex.lockUncancelable(io);
        defer self.table_activity_mutex.unlock(io);

        var structural_busy = false;
        var request_busy = false;
        if (self.findTableActivityLocked(table_name, null)) |index| {
            const entry = self.active_table_activities.items[index];
            if (entry.structural_active) structural_busy = true;
            if (entry.table_request_active > 0) request_busy = true;
        }

        var groups = std.ArrayListUnmanaged(u64).empty;
        errdefer groups.deinit(alloc);
        for (self.active_table_activities.items) |activity| {
            if (!std.mem.eql(u8, activity.table_name, table_name)) continue;
            const group_id = activity.group_id orelse continue;
            if (!activity.operation_active) continue;
            try groups.append(alloc, group_id);
        }

        return .{
            .structural_busy = structural_busy,
            .request_busy = request_busy,
            .groups = try groups.toOwnedSlice(alloc),
        };
    }

    pub fn takeStatusesWithoutActiveGroups(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        statuses: *runtime_status.LocalTableRuntimeStatuses,
        active_groups: []const u64,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        _ = self;
        if (active_groups.len == 0) {
            const owned = statuses.*;
            statuses.* = .{};
            return owned;
        }

        var keep_count: usize = 0;
        for (statuses.items) |item| {
            for (active_groups) |group_id| {
                if (item.group_id == group_id) break;
            } else {
                keep_count += 1;
            }
        }
        if (keep_count == statuses.items.len) {
            const owned = statuses.*;
            statuses.* = .{};
            return owned;
        }
        if (keep_count == 0) {
            statuses.deinit(alloc);
            statuses.* = .{};
            return null;
        }

        const original_items = statuses.items;
        const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, keep_count);
        var write_index: usize = 0;
        for (original_items) |*item| {
            for (active_groups) |group_id| {
                if (item.group_id == group_id) break;
            } else {
                items[write_index] = item.*;
                item.* = undefined;
                write_index += 1;
                continue;
            }
            item.deinit(alloc);
        }

        alloc.free(original_items);
        statuses.* = .{};
        return .{ .items = items };
    }

    fn statusesWithoutActiveGroups(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        statuses: runtime_status.LocalTableRuntimeStatuses,
        active_groups: []const u64,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        var owned = statuses;
        defer owned.deinit(alloc);
        return try self.takeStatusesWithoutActiveGroups(alloc, &owned, active_groups);
    }

    fn appendOrReplaceRuntimeStatusClone(
        alloc: std.mem.Allocator,
        items: *std.ArrayListUnmanaged(runtime_status.LocalTableRuntimeStatus),
        item: runtime_status.LocalTableRuntimeStatus,
    ) !void {
        for (items.items) |*existing| {
            if (existing.group_id != item.group_id) continue;
            var cloned = try item.clone(alloc);
            errdefer cloned.deinit(alloc);
            existing.deinit(alloc);
            existing.* = cloned;
            return;
        }
        var cloned = try item.clone(alloc);
        errdefer cloned.deinit(alloc);
        try items.append(alloc, cloned);
    }

    fn appendOrReplaceRuntimeStatusClones(
        alloc: std.mem.Allocator,
        items: *std.ArrayListUnmanaged(runtime_status.LocalTableRuntimeStatus),
        statuses: *const runtime_status.LocalTableRuntimeStatuses,
    ) !void {
        for (statuses.items) |item| {
            try appendOrReplaceRuntimeStatusClone(alloc, items, item);
        }
    }

    fn mergedRuntimeStatusReplacement(
        alloc: std.mem.Allocator,
        current: *const runtime_status.LocalTableRuntimeStatuses,
        refresh: *const runtime_status.LocalTableRuntimeStatuses,
    ) !runtime_status.LocalTableRuntimeStatuses {
        var merged = std.ArrayListUnmanaged(runtime_status.LocalTableRuntimeStatus).empty;
        errdefer {
            for (merged.items) |*item| item.deinit(alloc);
            merged.deinit(alloc);
        }

        try merged.ensureTotalCapacity(alloc, current.items.len + refresh.items.len);
        try appendOrReplaceRuntimeStatusClones(alloc, &merged, current);
        try appendOrReplaceRuntimeStatusClones(alloc, &merged, refresh);
        return .{ .items = try merged.toOwnedSlice(alloc) };
    }

    pub fn replaceRuntimeStatusesWithMergedRefresh(
        alloc: std.mem.Allocator,
        statuses: *runtime_status.LocalTableRuntimeStatuses,
        refresh: *const runtime_status.LocalTableRuntimeStatuses,
    ) !void {
        var replacement = try mergedRuntimeStatusReplacement(alloc, statuses, refresh);
        errdefer replacement.deinit(alloc);
        statuses.deinit(alloc);
        statuses.* = replacement;
    }

    pub fn snapshotRuntimeStatusesBestEffort(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        // Keep HTTP status reads on the cached status plane. See STATUS.md.
        const snapshot_cache = self.runtime_status_cache orelse return null;
        var statuses = (try snapshot_cache.snapshot(alloc, table_name)) orelse return null;
        errdefer statuses.deinit(alloc);
        self.overlayManagedWriterReplayTargetsBestEffort(table_name, &statuses);
        return statuses;
    }

    fn refreshRuntimeStatusesFromDirtyWriteCache(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        statuses: *runtime_status.LocalTableRuntimeStatuses,
    ) !void {
        if (!self.isWriteCacheDirtyForTable(table_name)) return;

        const now_ns = platform_time.monotonicNs();
        if (!runtimeStatusesNeedWriterRefresh(statuses, now_ns)) return;

        var leases = std.ArrayListUnmanaged(ProvisionedTableWriteCache.CachedDb).empty;
        defer {
            for (leases.items) |*lease| {
                const release_alloc = if (lease.cache) |cache| cache.alloc else std.heap.page_allocator;
                lease.deinit(release_alloc);
            }
            leases.deinit(alloc);
        }

        lockAtomic(&self.local_db_mutex);
        const has_leases = self.collectRuntimeStatusLeasesFromWriteCacheLocked(alloc, table_name, &leases) catch |err| {
            self.local_db_mutex.unlock();
            return err;
        };
        self.local_db_mutex.unlock();
        if (!has_leases) return;

        var live_statuses = (try self.runtimeStatusesFromCachedDbLeasesBestEffort(alloc, table_name, leases.items)) orelse return;
        defer live_statuses.deinit(alloc);

        try replaceRuntimeStatusesWithMergedRefresh(alloc, statuses, &live_statuses);
    }

    fn refreshStaleRuntimeStatusesFromStorage(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        statuses: *runtime_status.LocalTableRuntimeStatuses,
    ) !void {
        if (self.isWriteCacheDirtyForTable(table_name)) return;

        const now_ns = platform_time.monotonicNs();
        if (!runtimeStatusesNeedWriterRefresh(statuses, now_ns)) return;

        var uncached = (try self.snapshotUncachedRuntimeStatusesAndUpdateCache(alloc, table_name)) orelse return;
        defer uncached.deinit(alloc);

        try replaceRuntimeStatusesWithMergedRefresh(alloc, statuses, &uncached);
    }

    fn runtimeStatusesNeedWriterRefresh(statuses: *const runtime_status.LocalTableRuntimeStatuses, now_ns: u64) bool {
        const min_refresh_interval_ns = std.time.ns_per_s;
        if (statuses.items.len == 0) return true;
        for (statuses.items) |status| {
            if (!runtime_status.statusRuntimeFresh(status)) return true;
            if (status.metadata.updated_at_ns == 0) return true;
            if (now_ns -| status.metadata.updated_at_ns >= min_refresh_interval_ns) return true;
        }
        return false;
    }

    fn snapshotUncachedRuntimeStatusesAndUpdateCache(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        const snapshot_cache = self.runtime_status_cache orelse return null;
        var uncached = (try snapshotLocalTableRuntimeStatusesUncached(alloc, self.catalog, self.replica_root_dir, self.backend_runtime, table_name)) orelse return null;
        errdefer uncached.deinit(alloc);
        for (uncached.items) |item| {
            try snapshot_cache.upsertGroupStatus(table_name, item);
        }
        return uncached;
    }

    fn overlayManagedWriterReplayTargetsBestEffort(
        self: *ProvisionedTableWriteSource,
        table_name: []const u8,
        statuses: *runtime_status.LocalTableRuntimeStatuses,
    ) void {
        if (!self.local_db_mutex.tryLock()) return;
        defer self.local_db_mutex.unlock();
        self.overlayManagedWriterReplayTargetsFromCacheLocked(table_name, self.write_cache, statuses);
        self.overlayManagedWriterReplayTargetsFromCacheLocked(table_name, self.startup_write_cache, statuses);
    }

    fn overlayManagedWriterReplayTargetsFromCacheLocked(
        self: *ProvisionedTableWriteSource,
        table_name: []const u8,
        maybe_cache: ?*ProvisionedTableWriteCache,
        statuses: *runtime_status.LocalTableRuntimeStatuses,
    ) void {
        const cache = maybe_cache orelse return;
        for (cache.entries.items) |entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.lsm_root_generation != self.visibleRootGeneration(entry.group_id)) continue;
            for (statuses.items) |*status| {
                if (status.group_id != entry.group_id) continue;
                overlayRuntimeStatusReplayTargetFromDb(status, &entry.db);
                break;
            }
        }
    }

    pub fn lsmMaintenanceStats(self: *ProvisionedTableWriteSource) lsm_backend.Backend.MaintenanceStats {
        lockAtomic(&self.local_db_mutex);
        defer self.local_db_mutex.unlock();
        var stats = lsm_backend.Backend.MaintenanceStats{};
        if (self.write_cache) |cache| {
            for (cache.entries.items) |entry| {
                lsm_backend.Backend.accumulateMaintenanceStats(&stats, entry.db.snapshotLsmMaintenanceStats());
            }
        }
        return stats;
    }

    pub fn lsmMaintenanceStatsBestEffort(self: *ProvisionedTableWriteSource) lsm_backend.Backend.MaintenanceStats {
        if (!self.local_db_mutex.tryLock()) return .{};
        defer self.local_db_mutex.unlock();
        var stats = lsm_backend.Backend.MaintenanceStats{};
        if (self.write_cache) |cache| {
            for (cache.entries.items) |entry| {
                if (entry.db.trySnapshotLsmMaintenanceStats()) |entry_stats| {
                    lsm_backend.Backend.accumulateMaintenanceStats(&stats, entry_stats);
                }
            }
        }
        return stats;
    }

    pub fn lsmWriteStatsBestEffort(self: *ProvisionedTableWriteSource) lsm_backend.Backend.WriteStats {
        if (!self.local_db_mutex.tryLock()) return .{};
        defer self.local_db_mutex.unlock();
        var stats = lsm_backend.Backend.WriteStats{};
        if (self.write_cache) |cache| {
            for (cache.entries.items) |entry| {
                if (entry.db.trySnapshotLsmWriteStats()) |entry_stats| {
                    lsm_backend.Backend.accumulateWriteStats(&stats, entry_stats);
                }
            }
        }
        return stats;
    }

    pub fn lsmNativeStorageStatsBestEffort(self: *ProvisionedTableWriteSource) ?lsm_backend.NativeStorageStats {
        if (!self.local_db_mutex.tryLock()) return null;
        defer self.local_db_mutex.unlock();
        const cache = self.write_cache orelse return null;
        var stats = lsm_backend.NativeStorageStats{};
        var observed = false;
        for (cache.entries.items) |entry| {
            const entry_stats = entry.db.trySnapshotLsmNativeStorageStats() orelse continue;
            observed = true;
            stats.fd_cache_entries +|= entry_stats.fd_cache_entries;
            stats.fd_cache_capacity +|= entry_stats.fd_cache_capacity;
        }
        if (!observed) return null;
        return stats;
    }

    pub fn asyncIndexingStats(self: *ProvisionedTableWriteSource) db_mod.types.AsyncIndexingStats {
        lockAtomic(&self.local_db_mutex);
        defer self.local_db_mutex.unlock();
        const cache = self.write_cache orelse return .{};
        var stats = db_mod.types.AsyncIndexingStats{};
        for (cache.entries.items) |entry| {
            db_mod.types.accumulateAsyncIndexingStats(&stats, entry.db.snapshotAsyncIndexingStats());
        }
        return stats;
    }

    pub fn asyncIndexingStatsBestEffort(self: *ProvisionedTableWriteSource) db_mod.types.AsyncIndexingStats {
        if (!self.local_db_mutex.tryLock()) {
            if (self.startup_catch_up_active.load(.monotonic)) {
                if (self.runtime_status_cache) |snapshot_cache| return snapshot_cache.summary().async_indexing;
            }
            return .{};
        }
        defer self.local_db_mutex.unlock();
        const cache = self.write_cache orelse return .{};
        var stats = db_mod.types.AsyncIndexingStats{};
        for (cache.entries.items) |entry| {
            db_mod.types.accumulateAsyncIndexingStats(&stats, entry.db.snapshotAsyncIndexingStats());
        }
        return stats;
    }

    pub fn textMemoryAttributionStatsBestEffort(self: *ProvisionedTableWriteSource) db_mod.TextMemoryAttributionStats {
        if (!self.local_db_mutex.tryLock()) return .{};
        defer self.local_db_mutex.unlock();
        const cache = self.write_cache orelse return .{};
        var stats = db_mod.TextMemoryAttributionStats{};
        for (cache.entries.items) |entry| {
            const entry_stats = entry.db.trySnapshotTextMemoryAttributionStats() orelse continue;
            accumulateTextMemoryAttributionStats(&stats, entry_stats);
        }
        return stats;
    }

    pub fn textMergeStatsBestEffort(self: *ProvisionedTableWriteSource) db_mod.types.TextMergeStats {
        if (!self.local_db_mutex.tryLock()) return .{};
        defer self.local_db_mutex.unlock();
        const cache = self.write_cache orelse return .{};
        var stats = db_mod.types.TextMergeStats{};
        for (cache.entries.items) |entry| {
            const entry_stats = entry.db.trySnapshotTextMergeStats() orelse continue;
            db_mod.types.accumulateTextMergeStats(&stats, entry_stats);
        }
        return stats;
    }

    pub fn autoBulkIngestStatsBestEffort(self: *ProvisionedTableWriteSource) ProvisionedTableWriteCache.AutoBulkIngestStats {
        if (!self.local_db_mutex.tryLock()) return .{};
        defer self.local_db_mutex.unlock();
        const now_ns = platform_time.monotonicNs();
        var stats = ProvisionedTableWriteCache.AutoBulkIngestStats{};
        if (self.write_cache) |cache| stats.merge(cache.autoBulkIngestStatsLocked(now_ns));
        if (self.startup_write_cache) |cache| stats.merge(cache.autoBulkIngestStatsLocked(now_ns));
        return stats;
    }

    pub fn cachedWriteDbCount(self: *ProvisionedTableWriteSource) usize {
        lockAtomic(&self.local_db_mutex);
        defer self.local_db_mutex.unlock();
        const cache = self.write_cache orelse return 0;
        return cache.entries.items.len;
    }

    pub fn cachedWriteDbCountBestEffort(self: *ProvisionedTableWriteSource) usize {
        if (!self.local_db_mutex.tryLock()) return 0;
        defer self.local_db_mutex.unlock();
        const cache = self.write_cache orelse return 0;
        return cache.entries.items.len;
    }

    pub fn readPreparation(self: *ProvisionedTableWriteSource) table_read_core.ReadPreparation {
        return .{
            .ptr = self,
            .vtable = &.{
                .prepare_for_read = prepareForRead,
            },
        };
    }

    pub fn primaryLookupDbSource(self: *ProvisionedTableWriteSource) table_read_core.PrimaryLookupDbSource {
        return .{
            .ptr = self,
            .lease_group = leasePrimaryLookupDb,
        };
    }

    fn prepareForRead(ptr: *anyopaque, table_name: []const u8, kind: table_read_core.ReadPreparation.Kind) void {
        _ = kind;
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        self.waitForNoStructuralActivity(table_name);
        if (!self.isWriteCacheDirtyForTable(table_name)) return;

        // Data writes and derived catch-up are intentionally eventually visible
        // to queries. A read can discard its cached reader and reopen the latest
        // published view, but it must not wait behind writer-cache maintenance
        // or close the live writer cache from the query path.
        self.invalidateReadCache(table_name);
        if (self.hasActiveBulkIngestSessionForTableBestEffort(table_name)) return;
        self.clearDirtyWriteTable(table_name);
    }

    fn leasePrimaryLookupDb(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_id: u64,
        lsm_root_generation: u64,
    ) !?table_read_core.PrimaryLookupDbLease {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.local_db_mutex);
        var cached: ?ProvisionedTableWriteCache.CachedDb = null;
        if (self.write_cache) |cache| {
            cached = cache.snapshotLeaseOrAdoptSeededLocked(group_id, lsm_root_generation, table_name);
        }
        if (cached == null) {
            if (self.startup_write_cache) |cache| {
                cached = cache.snapshotLeaseOrAdoptSeededLocked(group_id, lsm_root_generation, table_name);
            }
        }
        self.local_db_mutex.unlock();

        var cached_value = cached orelse return null;
        const lease_ctx = alloc.create(ProvisionedTableWriteCache.CachedDb) catch |err| {
            cached_value.deinit(alloc);
            return err;
        };
        lease_ctx.* = cached_value;
        return .{
            .ptr = lease_ctx,
            .db = lease_ctx.db,
            .release_fn = releasePrimaryLookupDb,
        };
    }

    fn releasePrimaryLookupDb(ptr: *anyopaque, alloc: std.mem.Allocator) void {
        const lease_ctx: *ProvisionedTableWriteCache.CachedDb = @ptrCast(@alignCast(ptr));
        lease_ctx.deinit(alloc);
        alloc.destroy(lease_ctx);
    }

    fn writeCacheTableHash(table_name: []const u8) u64 {
        return std.hash.Wyhash.hash(0, table_name);
    }

    fn hasDirtyWriteTableLocked(self: *ProvisionedTableWriteSource, table_name: []const u8) bool {
        const table_hash = writeCacheTableHash(table_name);
        for (self.dirty_write_table_hashes[0..self.dirty_write_table_hashes_len]) |candidate| {
            if (candidate == table_hash) return true;
        }
        return false;
    }

    pub fn isWriteCacheDirtyForTable(self: *ProvisionedTableWriteSource, table_name: []const u8) bool {
        if (self.dirty_write_table_count.load(.acquire) == 0) return false;
        lockAtomic(&self.dirty_write_tables_mutex);
        defer self.dirty_write_tables_mutex.unlock();
        return self.hasDirtyWriteTableLocked(table_name);
    }

    fn clearAllDirtyWriteTablesLocked(self: *ProvisionedTableWriteSource) void {
        self.dirty_write_table_hashes_len = 0;
        self.dirty_write_table_count.store(0, .release);
    }

    fn clearAllDirtyWriteTables(self: *ProvisionedTableWriteSource) void {
        lockAtomic(&self.dirty_write_tables_mutex);
        defer self.dirty_write_tables_mutex.unlock();
        self.clearAllDirtyWriteTablesLocked();
    }

    fn clearDirtyWriteTable(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        lockAtomic(&self.dirty_write_tables_mutex);
        defer self.dirty_write_tables_mutex.unlock();
        const table_hash = writeCacheTableHash(table_name);
        var i: usize = 0;
        while (i < self.dirty_write_table_hashes_len) {
            if (self.dirty_write_table_hashes[i] != table_hash) {
                i += 1;
                continue;
            }
            self.dirty_write_table_hashes_len -= 1;
            self.dirty_write_table_hashes[i] = self.dirty_write_table_hashes[self.dirty_write_table_hashes_len];
            break;
        }
        self.dirty_write_table_count.store(@intCast(self.dirty_write_table_hashes_len), .release);
    }

    fn hasDirtyWriteTableWithLocalDbLocked(self: *ProvisionedTableWriteSource, table_name: []const u8) bool {
        if (self.dirty_write_table_count.load(.acquire) == 0) return false;
        const dirty = dirty_blk: {
            lockAtomic(&self.dirty_write_tables_mutex);
            defer self.dirty_write_tables_mutex.unlock();
            break :dirty_blk self.hasDirtyWriteTableLocked(table_name);
        };
        if (!dirty) return false;

        const cache = self.write_cache orelse {
            self.clearDirtyWriteTable(table_name);
            return false;
        };
        if (cache.bulkIngestSessionOpenForTable(table_name)) return true;
        if (cache.hasLiveEntryForTableLocked(table_name)) return true;

        self.clearDirtyWriteTable(table_name);
        return false;
    }

    pub fn markWriteCacheDirty(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        if (self.write_cache == null) return;
        lockAtomic(&self.dirty_write_tables_mutex);
        const table_hash = writeCacheTableHash(table_name);
        for (self.dirty_write_table_hashes[0..self.dirty_write_table_hashes_len]) |candidate| {
            if (candidate == table_hash) {
                self.dirty_write_table_count.store(@intCast(self.dirty_write_table_hashes_len), .release);
                self.dirty_write_tables_mutex.unlock();
                return;
            }
        }
        if (self.dirty_write_table_hashes_len >= self.dirty_write_table_hashes.len) {
            self.dirty_write_tables_mutex.unlock();
            self.clearWriteCacheLocked();
            return;
        }
        self.dirty_write_table_hashes[self.dirty_write_table_hashes_len] = table_hash;
        self.dirty_write_table_hashes_len += 1;
        self.dirty_write_table_count.store(@intCast(self.dirty_write_table_hashes_len), .release);
        self.dirty_write_tables_mutex.unlock();
    }

    fn invalidateDirtyWriteCacheForRead(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        lockAtomic(&self.dirty_write_tables_mutex);
        const dirty = self.hasDirtyWriteTableLocked(table_name);
        self.dirty_write_tables_mutex.unlock();
        if (!dirty) return;
        self.invalidateWriteCache(table_name);
    }

    fn notifyLocalChange(self: *ProvisionedTableWriteSource, table_name: []const u8, kind: LocalChangeKind) void {
        if (self.local_change_hook) |hook| hook.on_change(hook.ptr, table_name, kind);
    }

    fn changeKindForHARecord(record: db_mod.HAReplicationRecordView) LocalChangeKind {
        return switch (record.kind) {
            .metadata_mutation => .structural,
            else => .data,
        };
    }

    fn publishRestoreRepairComplete(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        self.invalidateReadCache(table_name);
        self.invalidateWriteCacheForTable(table_name);
        self.clearDirtyWriteTable(table_name);
        self.notifyLocalChange(table_name, .data);
    }

    pub fn enqueueRestoreRepairComplete(self: *ProvisionedTableWriteSource, table_name: []const u8) void {
        const alloc = std.heap.page_allocator;
        const owned_table_name = alloc.dupe(u8, table_name) catch |err| {
            std.log.warn("restore repair completion allocation failed table={s} err={}", .{ table_name, err });
            return;
        };

        const io = self.table_activity_threaded.io();
        self.restore_repair_completion_mutex.lockUncancelable(io);
        self.restore_repair_completions.append(alloc, owned_table_name) catch |err| {
            self.restore_repair_completion_mutex.unlock(io);
            alloc.free(owned_table_name);
            std.log.warn("restore repair completion enqueue failed table={s} err={}", .{ table_name, err });
            return;
        };
        self.restore_repair_completion_mutex.unlock(io);
        self.scheduleRestoreRepairCompletionDrain();
    }

    fn scheduleRestoreRepairCompletionDrain(self: *ProvisionedTableWriteSource) void {
        if (self.restore_repair_completion_scheduled.cmpxchgStrong(false, true, .acq_rel, .acquire) != null) return;
        const io = self.table_activity_threaded.io();
        self.restore_repair_completion_group.concurrent(io, drainRestoreRepairCompletionsTask, .{self}) catch |err| {
            self.restore_repair_completion_scheduled.store(false, .release);
            std.log.warn("restore repair completion drain schedule failed err={}", .{err});
        };
    }

    fn drainRestoreRepairCompletionsTask(self: *ProvisionedTableWriteSource) !void {
        self.drainRestoreRepairCompletionsScheduled();
    }

    fn drainRestoreRepairCompletionsScheduled(self: *ProvisionedTableWriteSource) void {
        const alloc = std.heap.page_allocator;
        const io = self.table_activity_threaded.io();
        while (true) {
            self.restore_repair_completion_mutex.lockUncancelable(io);
            if (self.restore_repair_completions.items.len == 0) {
                self.restore_repair_completion_scheduled.store(false, .release);
                self.restore_repair_completion_mutex.unlock(io);
                return;
            }
            var pending = self.restore_repair_completions;
            self.restore_repair_completions = .empty;
            self.restore_repair_completion_mutex.unlock(io);

            for (pending.items) |table_name| {
                self.publishRestoreRepairComplete(table_name);
                alloc.free(table_name);
            }
            pending.deinit(alloc);
        }
    }

    fn freeRestoreRepairCompletions(self: *ProvisionedTableWriteSource) void {
        const alloc = std.heap.page_allocator;
        const io = self.table_activity_threaded.io();
        self.restore_repair_completion_mutex.lockUncancelable(io);
        defer self.restore_repair_completion_mutex.unlock(io);
        for (self.restore_repair_completions.items) |table_name| alloc.free(table_name);
        self.restore_repair_completions.deinit(alloc);
        self.restore_repair_completions = .empty;
    }

    pub fn openRestoreRepairDbForGroup(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        path: []const u8,
        group_id: u64,
        table_name: []const u8,
        indexes_json: []const u8,
    ) !db_mod.DB {
        const identity_namespace = try loadTableIdentityNamespaceForGroup(alloc, self.catalog, table_name, group_id);
        var db = try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentity(
            alloc,
            path,
            indexes_json,
            null,
            null,
            self.visibleRootGeneration(group_id),
            null,
            .restore_repair,
            self.backend_runtime,
            self.antfly_provider,
            self.secret_store,
            self.remote_content,
            identity_namespace,
        );
        errdefer db.close();
        try validateProvisionedDbIdentityNamespaceExpected(identity_namespace, &db);
        return db;
    }

    const RestoreRepairCatchUpWork = struct {
        alloc: std.mem.Allocator,
        source: *ProvisionedTableWriteSource,
        group_id: u64,
        table_name: []u8,

        fn sleepRetry(self: *@This()) void {
            self.source.table_activity_threaded.io().sleep(Io.Duration.fromMilliseconds(100), .awake) catch {};
        }

        fn runAndDeinit(work: *@This()) Io.Cancelable!void {
            defer RestoreRepairCatchUpWork.deinit(work);
            work.run() catch |err| {
                std.log.warn("restore background catch-up failed table={s} group_id={d} err={s}", .{
                    work.table_name,
                    work.group_id,
                    @errorName(err),
                });
            };
        }

        fn run(work: *@This()) !void {
            const path = try metadata_mod.groupDbPathFromReplicaRoot(work.alloc, work.source.replica_root_dir, work.group_id);
            defer work.alloc.free(path);

            var attempts: usize = 0;
            std.log.info("restore background catch-up begin table={s} group_id={d}", .{ work.table_name, work.group_id });
            while (true) {
                attempts += 1;
                const busy = try work.repairOnce(path);
                const still_needed = try db_mod.DB.restoreRuntimeRepairNeededForPath(work.alloc, path);
                if (!busy and !still_needed) {
                    work.source.enqueueRestoreRepairComplete(work.table_name);
                    std.log.info("restore background catch-up complete table={s} group_id={d} attempts={d}", .{
                        work.table_name,
                        work.group_id,
                        attempts,
                    });
                    return;
                }
                work.sleepRetry();
            }
        }

        fn repairOnce(self: *@This(), path: []const u8) !bool {
            if (!try db_mod.DB.restoreRuntimeRepairNeededForPath(self.alloc, path)) return false;

            if (!self.source.tryBeginGroupOperation(self.table_name, self.group_id)) return true;
            defer self.source.endGroupOperation(self.table_name, self.group_id);

            if (!self.source.local_db_mutex.tryLock()) return true;
            if (self.source.hasDirtyWriteTableWithLocalDbLocked(self.table_name)) {
                self.source.local_db_mutex.unlock();
                return true;
            }
            if (self.source.write_cache) |cache| {
                if (cache.hasForegroundStateForGroupTableLocked(self.group_id, self.table_name)) {
                    self.source.local_db_mutex.unlock();
                    return true;
                }
            }
            self.source.local_db_mutex.unlock();

            const indexes_json = (try loadTableIndexesJson(self.alloc, self.source.catalog, self.table_name)) orelse return true;
            defer self.alloc.free(indexes_json);

            var db = try self.source.openRestoreRepairDbForGroup(
                self.alloc,
                path,
                self.group_id,
                self.table_name,
                indexes_json,
            );
            defer db.close();

            if (try db.repairRestoreRuntimeStateStepIfNeeded(self.alloc)) {
                db.clearDenseHbcCaches();
            }
            return false;
        }

        fn deinit(ptr: *anyopaque) void {
            const work: *@This() = @ptrCast(@alignCast(ptr));
            const alloc = work.alloc;
            alloc.free(work.table_name);
            alloc.destroy(work);
        }
    };

    fn requestRestoreRepairCatchUp(self: *ProvisionedTableWriteSource, table_name: []const u8, group_id: u64) void {
        const alloc = std.heap.page_allocator;
        const work = alloc.create(RestoreRepairCatchUpWork) catch |err| {
            std.log.warn("restore background catch-up allocation failed table={s} group_id={d} err={}", .{ table_name, group_id, err });
            return;
        };
        const owned_table_name = alloc.dupe(u8, table_name) catch |err| {
            alloc.destroy(work);
            std.log.warn("restore background catch-up table name allocation failed table={s} group_id={d} err={}", .{ table_name, group_id, err });
            return;
        };
        work.* = .{
            .alloc = alloc,
            .source = self,
            .group_id = group_id,
            .table_name = owned_table_name,
        };
        self.restore_repair_work_group.concurrent(self.table_activity_threaded.io(), RestoreRepairCatchUpWork.runAndDeinit, .{work}) catch |err| {
            RestoreRepairCatchUpWork.deinit(work);
            std.log.warn("restore background catch-up submit failed table={s} group_id={d} err={}", .{ table_name, group_id, err });
            return;
        };
    }

    fn beginBulkIngest(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        table_name: []const u8,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        try enforceHAWriteGateOptional(self.ha_write_gate);
        lockAtomic(&self.local_db_mutex);
        defer self.local_db_mutex.unlock();
        const cache = self.write_cache orelse return null;
        try cache.beginBulkIngestLocked(table_name);
    }

    fn finishBulkIngest(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        table_name: []const u8,
        options: backend_types.BulkIngestFinishOptions,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        try enforceHAWriteGateOptional(self.ha_write_gate);
        lockAtomic(&self.local_db_mutex);
        defer self.local_db_mutex.unlock();
        const cache = self.write_cache orelse return null;
        try cache.finishBulkIngestLocked(table_name, options);
    }

    fn abortBulkIngest(ptr: *anyopaque, table_name: []const u8) void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.local_db_mutex);
        defer self.local_db_mutex.unlock();
        const cache = self.write_cache orelse return;
        cache.abortBulkIngestLocked(table_name);
    }

    fn graphMetricMaintenanceGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?[]u8 {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);

        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);

        var db = openManagedDbForTableGroupWithRuntime(alloc, path, self.catalog, table_name, group_id, self.backend_runtime) catch |err| switch (err) {
            error.FileNotFound => return error.UnknownGroup,
            else => return err,
        };
        defer db.close();
        return try db.runGraphMetricServiceMaintenanceJsonAlloc(alloc, body);
    }

    pub fn source(self: *ProvisionedTableWriteSource) TableWriteSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .create_table = createTable,
                .create_catalog_table = createCatalogTableNative,
                .update_schema = updateSchema,
                .create_index = createIndex,
                .create_catalog_index = createCatalogIndexNative,
                .drop_index = dropIndex,
                .drop_catalog_index = dropCatalogIndexNative,
                .drop_table = dropTable,
                .drop_catalog_table = dropCatalogTableNative,
                .graph_metric_maintenance_group_local = graphMetricMaintenanceGroupLocal,
                .commit_transaction = commitTransaction,
                .commit_transaction_with_id = commitTransactionWithId,
                .backup_table = backupTable,
                .backup_catalog_table = backupCatalogTableNative,
                .restore_table = restoreTable,
                .restore_catalog_table = restoreCatalogTableNative,
                .batch = batch,
                .batch_catalog = batchCatalogNative,
                .reprocess_document_artifact = reprocessDocumentArtifact,
                .reprocess_document_artifact_range = reprocessDocumentArtifactRange,
                .reprocess_document_artifact_group_local = reprocessDocumentArtifactGroupLocal,
                .begin_bulk_ingest = beginBulkIngest,
                .finish_bulk_ingest = finishBulkIngest,
                .abort_bulk_ingest = abortBulkIngest,
                .batch_group_local = batchGroupLocal,
                .txn_begin_group_local = txnBeginGroupLocal,
                .txn_prepare_group_local = txnPrepareGroupLocal,
                .txn_resolve_group_local = txnResolveGroupLocal,
                .txn_status_group_local = txnStatusGroupLocal,
                .corrupt_embedding_artifact = corruptEmbeddingArtifact,
                .local_runtime_statuses = localRuntimeStatuses,
                .local_runtime_statuses_catalog = ProvisionedTableWriteSource.localRuntimeStatusesCatalogNative,
                .foreign_key_integrity = foreignKeyIntegrity,
                .foreign_key_integrity_worker_pass = foreignKeyIntegrityWorkerPass,
                .foreign_key_integrity_schema_controller_pass = foreignKeyIntegritySchemaControllerPass,
                .foreign_key_integrity_schema_controller_maintenance_pass = foreignKeyIntegritySchemaControllerMaintenancePass,
                .foreign_key_action_job_page = foreignKeyActionJobPage,
                .foreign_key_action_job_schedule = foreignKeyActionJobSchedule,
                .foreign_key_action_job_requeue = foreignKeyActionJobRequeue,
                .foreign_key_action_job_group_local = foreignKeyActionJobGroupLocal,
                .foreign_key_action_job_group_local_schedule = foreignKeyActionJobGroupLocalSchedule,
                .foreign_key_action_job_group_local_requeue = foreignKeyActionJobGroupLocalRequeue,
                .foreign_key_action_job_progress = foreignKeyActionJobProgress,
                .foreign_key_action_job_group_local_progress = foreignKeyActionJobGroupLocalProgress,
                .foreign_key_action_schedule_progress = foreignKeyActionScheduleProgress,
                .foreign_key_action_schedule_group_local_progress = foreignKeyActionScheduleGroupLocalProgress,
                .foreign_key_action_schedule_mark_seeded = foreignKeyActionScheduleMarkSeeded,
                .foreign_key_action_schedule_requeue = foreignKeyActionScheduleRequeue,
                .foreign_key_action_schedule_group_local_mark_seeded = foreignKeyActionScheduleGroupLocalMarkSeeded,
                .foreign_key_action_schedule_group_local_requeue = foreignKeyActionScheduleGroupLocalRequeue,
                .unique_constraint_integrity = uniqueConstraintIntegrity,
                .unique_constraint_integrity_schema_controller_maintenance_pass = uniqueConstraintIntegritySchemaControllerMaintenancePass,
                .secondary_index_rebuild_worker_pass = secondaryIndexRebuildWorkerPass,
                .secondary_index_rebuild_group_local = secondaryIndexRebuildGroupLocal,
                .schema_rewrite_worker_pass = ProvisionedTableWriteSource.schemaRewriteWorkerPass,
                .schema_rewrite_group_local = ProvisionedTableWriteSource.schemaRewriteGroupLocal,
                .foreign_key_integrity_group_local = foreignKeyIntegrityGroupLocal,
                .foreign_key_integrity_work_unit_group_local = ProvisionedTableWriteSource.foreignKeyIntegrityWorkUnitGroupLocal,
                .unique_constraint_integrity_group_local = uniqueConstraintIntegrityGroupLocal,
                .foreign_key_ref_children_group_local = foreignKeyRefChildrenGroupLocal,
                .foreign_key_ref_children_page_group_local = foreignKeyRefChildrenPageGroupLocal,
            },
        };
    }

    fn foreignKeyIntegrityWorkerPass(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        action: ForeignKeyIntegrityAction,
        job_id: ?[]const u8,
        worker_id: []const u8,
        lease_ms: u64,
        max_work_units: usize,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?ForeignKeyIntegrityResult {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try runProvisionedForeignKeyIntegrityWorkerPass(
            self,
            alloc,
            table_name,
            action,
            job_id,
            worker_id,
            lease_ms,
            max_work_units,
            constraint_name,
            lower_doc_key,
            upper_doc_key,
            violation_limit,
        );
    }

    fn foreignKeyIntegritySchemaControllerPass(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        action: ForeignKeyIntegrityAction,
        worker_id: []const u8,
        lease_ms: u64,
        max_work_units: usize,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?ForeignKeyIntegrityResult {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const schema_json = (try loadTableSchemaJson(alloc, self.catalog, table_name)) orelse return null;
        defer alloc.free(schema_json);
        return try foreignKeyIntegritySchemaControllerPassWithSchemaJson(
            alloc,
            self.source(),
            table_name,
            schema_json,
            action,
            worker_id,
            lease_ms,
            max_work_units,
            constraint_name,
            lower_doc_key,
            upper_doc_key,
            violation_limit,
        );
    }

    fn foreignKeyIntegritySchemaControllerMaintenancePass(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        options: ForeignKeyIntegritySchemaControllerOptions,
    ) !?ForeignKeyIntegritySchemaControllerResult {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try runCatalogForeignKeyIntegritySchemaControllerMaintenancePass(alloc, self.source(), self.catalog, options);
    }

    fn uniqueConstraintIntegritySchemaControllerMaintenancePass(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        options: UniqueConstraintIntegritySchemaControllerOptions,
    ) !?UniqueConstraintIntegritySchemaControllerResult {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try runCatalogUniqueConstraintIntegritySchemaControllerMaintenancePass(alloc, self.source(), self.catalog, options);
    }

    fn createTable(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: tables_api.CreateTableRequest,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        try enforceHAWriteGateOptional(self.ha_write_gate);
        std.log.info("provisioned create table local begin table={s}", .{table_name});
        const group_ids = try table_catalog.resolveGroupsForSpanEventually(
            alloc,
            self.catalog,
            table_name,
            "",
            "",
            5 * std.time.ns_per_s,
            10,
        );
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;

        self.beginLocalStructuralMutation(table_name);
        errdefer self.abortLocalStructuralMutation(table_name);

        const raw_indexes_json = req.indexes_json orelse tables_api.default_indexes_json;
        const schema_json = tables_api.effectiveSchemaJson(req.schema_json);
        const indexes_json = try tables_api.prepareTableIndexesForSchemaAlloc(alloc, table_name, raw_indexes_json, schema_json);
        defer alloc.free(indexes_json);
        for (group_ids) |group_id| {
            std.log.info("provisioned create table local group begin table={s} group_id={d}", .{ table_name, group_id });
            try deleteGroupPathIfPresent(alloc, self.replica_root_dir, group_id);
            const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
            defer alloc.free(path);

            const identity_namespace = try loadTableIdentityNamespaceForGroup(alloc, self.catalog, table_name, group_id);
            const lsm_root_generation = self.visibleRootGeneration(group_id);
            if (self.write_cache) |cache| {
                if (cache.backend_runtime == null) cache.backend_runtime = self.backend_runtime;
                cache.antfly_provider = self.antfly_provider;
                cache.secret_store = self.secret_store;
                cache.remote_content = self.remote_content;
            }
            var opened: ?db_mod.DB = try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentity(
                alloc,
                path,
                indexes_json,
                if (self.write_cache) |cache| cache.lsm_cache else null,
                if (self.write_cache) |cache| cache.hbc_cache else null,
                lsm_root_generation,
                if (self.write_cache) |cache| cache.resource_manager else null,
                .default,
                if (self.write_cache) |cache| cache.backend_runtime else self.backend_runtime,
                self.antfly_provider,
                self.secret_store,
                self.remote_content,
                identity_namespace,
            );
            defer if (opened) |*db| db.close();
            try applyLocalTableSchemaJson(alloc, &opened.?, schema_json);
            // Register entity resolvers declared in the index config. Indexes
            // and enrichments are provisioned through the managed-open path, but
            // resolvers are not, so do it here (idempotent: addResolver skips
            // names that already exist on reopen).
            _ = try metadata_table_provisioner.ensureResolvers(alloc, &opened.?, indexes_json);
            if (self.write_cache) |cache| {
                try cache.seedCreatedDbLocked(&opened, group_id, lsm_root_generation, table_name, indexes_json, schema_json);
            }
            std.log.info("provisioned create table local group ready table={s} group_id={d}", .{ table_name, group_id });
        }

        self.finishLocalStructuralMutation(table_name);
        std.log.info("provisioned create table local notify table={s}", .{table_name});
        self.notifyLocalChange(table_name, .structural);
        std.log.info("provisioned create table local done table={s}", .{table_name});
    }

    fn createCatalogTableNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        req: tables_api.CreateTableRequest,
    ) !?void {
        const table_name = try nativeCatalogTableNameForCreateAlloc(alloc, target);
        defer alloc.free(table_name);
        return try createTable(ptr, alloc, table_name, req);
    }

    fn updateSchema(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        schema_json: []const u8,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        try enforceHAWriteGateOptional(self.ha_write_gate);
        const group_ids = try table_catalog.resolveGroupsForSpanEventually(
            alloc,
            self.catalog,
            table_name,
            "",
            "",
            5 * std.time.ns_per_s,
            10,
        );
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;
        const indexes_json = try loadTableIndexesJson(alloc, self.catalog, table_name);
        defer if (indexes_json) |value| alloc.free(value);

        self.beginLocalStructuralMutation(table_name);
        errdefer self.abortLocalStructuralMutation(table_name);

        for (group_ids) |group_id| {
            const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
            defer alloc.free(path);

            var db = try openManagedDbForTableGroupWithRuntimeAndHAWriteGate(alloc, path, self.catalog, table_name, group_id, self.backend_runtime, self.ha_write_gate, self.ha_async_mirror);
            defer db.close();
            try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group_id, &db);
            try applyLocalTableSchemaJson(alloc, &db, schema_json);
            if (indexes_json) |value| try rebuildEmptyVersionedFullTextIndexesAfterSchemaUpdate(alloc, &db, value);
            try drainManagedDbBeforeClose(&db);
            try publishRuntimeStatusSnapshotConsistent(self, alloc, table_name, group_id, &db);
        }

        self.finishLocalStructuralMutation(table_name);
        self.notifyLocalChange(table_name, .structural);
    }

    fn createIndex(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
        index_json: []const u8,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        try table_write_index_config.validateIndexConfig(alloc, index_name, index_json);
        try enforceHAWriteGateOptional(self.ha_write_gate);
        self.beginLocalStructuralMutation(table_name);
        errdefer self.abortLocalStructuralMutation(table_name);
        const managed_visibility_changed = try reconcileLocalTableIndexCreate(self, alloc, table_name, index_name);
        self.finishLocalStructuralMutation(table_name);
        self.notifyLocalChange(table_name, .structural);
        if (managed_visibility_changed) {
            self.publishWriteCacheRuntimeStatusesForTableBestEffort(alloc, table_name);
            self.notifyLocalChange(table_name, .data);
        }
    }

    fn createCatalogIndexNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        index_name: []const u8,
        index_json: []const u8,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const table_name = try nativeCatalogTableNameAlloc(alloc, self.catalog, target);
        defer alloc.free(table_name);
        return try createIndex(ptr, alloc, table_name, index_name, index_json);
    }

    fn dropIndex(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        try enforceHAWriteGateOptional(self.ha_write_gate);
        self.beginStructuralTableActivity(table_name);
        var structural_active = true;
        errdefer if (structural_active) self.endStructuralTableActivity(table_name);
        lockAtomic(&self.local_db_mutex);
        self.invalidateWriteCache(table_name);
        self.invalidateReadCache(table_name);
        self.invalidateRuntimeStatusCache(table_name);
        self.local_db_mutex.unlock();
        runTestBeforeDropIndexWorkHook();
        try dropLocalTableIndex(alloc, self.catalog, self.replica_root_dir, self.backend_runtime, table_name, index_name);
        lockAtomic(&self.local_db_mutex);
        self.invalidateReadCache(table_name);
        self.invalidateRuntimeStatusCache(table_name);
        self.local_db_mutex.unlock();
        self.endStructuralTableActivity(table_name);
        structural_active = false;
        self.notifyLocalChange(table_name, .structural);
    }

    fn dropCatalogIndexNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        index_name: []const u8,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const table_name = try nativeCatalogTableNameAlloc(alloc, self.catalog, target);
        defer alloc.free(table_name);
        return try dropIndex(ptr, alloc, table_name, index_name);
    }

    fn dropTable(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_ids: []const u64,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        try enforceHAWriteGateOptional(self.ha_write_gate);
        if (group_ids.len == 0) return null;

        self.beginLocalStructuralMutation(table_name);
        errdefer self.abortLocalStructuralMutation(table_name);

        for (group_ids) |group_id| {
            const trash_path = try moveDroppedGroupPathToTrash(alloc, self.replica_root_dir, table_name, group_id);
            if (trash_path) |path| {
                try self.deleteDroppedGroupPath(alloc, path);
            }
        }
        self.finishLocalStructuralMutation(table_name);
        self.notifyLocalChange(table_name, .structural);
    }

    fn dropCatalogTableNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        group_ids: []const u64,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const table_name = try nativeCatalogTableNameAlloc(alloc, self.catalog, target);
        defer alloc.free(table_name);
        return try dropTable(ptr, alloc, table_name, group_ids);
    }

    fn canCoalesceProvisionedGroupBatch(self: *ProvisionedTableWriteSource, group: GroupBatch, req: db_mod.types.BatchRequest) bool {
        if (self.write_cache == null) return false;
        if (group.transforms.items.len != 0) return false;
        if (group.relational_identity_rewrites.items.len != 0) return false;
        if (req.graph_writes.len != 0 or req.graph_deletes.len != 0 or req.predicates.len != 0) return false;
        return switch (req.sync_level) {
            .propose, .write => true,
            .enrichments, .full_text, .aknn, .full_index => false,
        };
    }

    fn writeCoalescerActive(self: *ProvisionedTableWriteSource, table_name: []const u8, group_id: u64) bool {
        const io = self.table_activity_threaded.io();
        self.write_coalesce_mutex.lockUncancelable(io);
        defer self.write_coalesce_mutex.unlock(io);
        const queue_index = self.findWriteCoalesceQueueLocked(table_name, group_id) orelse return false;
        return table_write_bulk_ingest.writeCoalesceQueueActive(&self.write_coalesce_queues.items[queue_index]);
    }

    pub fn testingWaitForWriteCoalesceQueueEntries(
        self: *ProvisionedTableWriteSource,
        table_name: []const u8,
        group_id: u64,
        expected_entries: usize,
    ) !void {
        if (!builtin.is_test) @compileError("testingWaitForWriteCoalesceQueueEntries is test-only");
        const deadline_ns = platform_time.monotonicNs() + 5 * std.time.ns_per_s;
        while (true) {
            const count = self.testingWriteCoalesceQueueEntryCount(table_name, group_id);
            if (count >= expected_entries) return;
            if (platform_time.monotonicNs() >= deadline_ns) return error.Timeout;
            sleepNs(std.time.ns_per_ms);
        }
    }

    fn testingWriteCoalesceQueueEntryCount(
        self: *ProvisionedTableWriteSource,
        table_name: []const u8,
        group_id: u64,
    ) usize {
        if (!builtin.is_test) @compileError("testingWriteCoalesceQueueEntryCount is test-only");
        const io = self.table_activity_threaded.io();
        self.write_coalesce_mutex.lockUncancelable(io);
        defer self.write_coalesce_mutex.unlock(io);
        return table_write_bulk_ingest.writeCoalesceQueueEntryCount(self.write_coalesce_queues.items, table_name, group_id);
    }

    fn enqueueProvisionedGroupBatchCoalesced(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group: GroupBatch,
        req: db_mod.types.BatchRequest,
    ) !void {
        var entry = WriteCoalesceEntry{
            .group = try cloneWriteCoalesceGroupBatch(alloc, group),
            .sync_level = req.sync_level,
            .timestamp_ns = req.timestamp_ns,
        };
        defer freeWriteCoalesceGroupBatch(alloc, &entry.group);

        const io = self.table_activity_threaded.io();
        var should_drain = false;
        self.write_coalesce_mutex.lockUncancelable(io);
        {
            errdefer self.write_coalesce_mutex.unlock(io);
            const queue = try self.writeCoalesceQueueLocked(table_name, group.group_id);
            try queue.entries.append(std.heap.page_allocator, &entry);
            if (!queue.draining) {
                queue.draining = true;
                should_drain = true;
            }
        }
        self.write_coalesce_mutex.unlock(io);

        if (should_drain) self.drainProvisionedGroupBatchCoalescer(alloc, table_name, group.group_id);

        self.write_coalesce_mutex.lockUncancelable(io);
        defer self.write_coalesce_mutex.unlock(io);
        while (!entry.done) {
            self.write_coalesce_ready.waitUncancelable(io, &self.write_coalesce_mutex);
        }
        if (entry.err) |err| return err;
    }

    fn drainProvisionedGroupBatchCoalescer(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_id: u64,
    ) void {
        const io = self.table_activity_threaded.io();
        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);

        while (true) {
            var entries = std.ArrayListUnmanaged(*WriteCoalesceEntry).empty;
            defer entries.deinit(alloc);

            self.write_coalesce_mutex.lockUncancelable(io);
            {
                const queue_index = self.findWriteCoalesceQueueLocked(table_name, group_id) orelse {
                    self.write_coalesce_mutex.unlock(io);
                    return;
                };
                const queue = &self.write_coalesce_queues.items[queue_index];
                if (queue.entries.items.len == 0) {
                    queue.draining = false;
                    self.write_coalesce_ready.broadcast(io);
                    self.pruneWriteCoalesceQueueLocked(queue_index);
                    self.write_coalesce_mutex.unlock(io);
                    return;
                }

                table_write_bulk_ingest.takeWriteCoalesceEntries(alloc, queue, &entries) catch |err| {
                    table_write_bulk_ingest.failAndClearWriteCoalesceQueue(queue, err);
                    self.write_coalesce_ready.broadcast(io);
                    self.pruneWriteCoalesceQueueLocked(queue_index);
                    self.write_coalesce_mutex.unlock(io);
                    return;
                };
            }
            self.write_coalesce_mutex.unlock(io);

            self.applyCoalescedEntriesWithIsolation(alloc, table_name, group_id, entries.items);
        }
    }

    fn applyCoalescedEntriesWithIsolation(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_id: u64,
        entries: []const *WriteCoalesceEntry,
    ) void {
        std.debug.assert(entries.len > 0);
        if (entries.len == 1) {
            const entry = entries[0];
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const apply_err: ?anyerror = blk: {
                self.applyProvisionedGroupBatchLocked(arena.allocator(), table_name, entry.group, coalescedEntryBatchRequest(entry)) catch |err| break :blk err;
                break :blk null;
            };
            self.finishCoalescedEntry(entry, apply_err);
            return;
        }

        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const arena_alloc = arena.allocator();
        var merged = GroupBatch{ .group_id = group_id };
        merged.writes.ensureTotalCapacity(arena_alloc, totalCoalescedWrites(entries)) catch |err| {
            self.finishCoalescedEntries(entries, err);
            return;
        };
        merged.deletes.ensureTotalCapacity(arena_alloc, totalCoalescedDeletes(entries)) catch |err| {
            self.finishCoalescedEntries(entries, err);
            return;
        };
        for (entries) |entry| {
            std.debug.assert(entry.group.relational_identity_rewrites.items.len == 0);
            for (entry.group.writes.items) |write| merged.writes.appendAssumeCapacity(write);
            for (entry.group.deletes.items) |key| merged.deletes.appendAssumeCapacity(key);
        }

        const merged_req = coalescedEntryBatchRequest(entries[0]);
        // Default timestamp_ns=0 intentionally remains a DB-batch timestamp:
        // coalescing preserves ordinary batch semantics and excludes CAS,
        // graph, transform, and derived-visibility writes.
        self.applyProvisionedGroupBatchLocked(arena_alloc, table_name, merged, merged_req) catch |merged_err| {
            std.log.warn("coalesced provisioned batch failed; retrying waiters individually table={s} group_id={} waiters={} err={s}", .{
                table_name,
                group_id,
                entries.len,
                @errorName(merged_err),
            });
            self.applyCoalescedEntriesIndividually(alloc, table_name, entries);
            return;
        };
        self.finishCoalescedEntries(entries, null);
    }

    fn applyCoalescedEntriesIndividually(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        entries: []const *WriteCoalesceEntry,
    ) void {
        for (entries) |entry| {
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const apply_err: ?anyerror = blk: {
                self.applyProvisionedGroupBatchLocked(arena.allocator(), table_name, entry.group, coalescedEntryBatchRequest(entry)) catch |err| break :blk err;
                break :blk null;
            };
            self.finishCoalescedEntry(entry, apply_err);
        }
    }

    fn finishCoalescedEntries(self: *ProvisionedTableWriteSource, entries: []const *WriteCoalesceEntry, err: ?anyerror) void {
        const io = self.table_activity_threaded.io();
        self.write_coalesce_mutex.lockUncancelable(io);
        defer self.write_coalesce_mutex.unlock(io);
        for (entries) |entry| {
            entry.err = err;
            entry.done = true;
        }
        self.write_coalesce_ready.broadcast(io);
    }

    fn finishCoalescedEntry(self: *ProvisionedTableWriteSource, entry: *WriteCoalesceEntry, err: ?anyerror) void {
        const io = self.table_activity_threaded.io();
        self.write_coalesce_mutex.lockUncancelable(io);
        defer self.write_coalesce_mutex.unlock(io);
        entry.err = err;
        entry.done = true;
        self.write_coalesce_ready.broadcast(io);
    }

    fn applyProvisionedGroupBatchLocked(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group: GroupBatch,
        req: db_mod.types.BatchRequest,
    ) !void {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group.group_id);
        defer alloc.free(path);
        const group_auto_bulk_ops = autoBulkIngestGroupBatchOps(group, req.sync_level);
        const auto_bulk_now_ns = platform_time.monotonicNs();
        if (self.write_cache) |cache| {
            var cached = try self.getOrOpenCachedDbMode(
                alloc,
                cache,
                path,
                group.group_id,
                table_name,
                .default_async,
                if (group_auto_bulk_ops > 0) auto_bulk_now_ns else null,
                if (group_auto_bulk_ops > 0) auto_bulk_now_ns else null,
            );
            defer cached.deinit(alloc);
            try table_write_bulk_ingest.applyGroupBatchWithSchemaJson(alloc, cached.db, cached.schema_json, group, req, runTestBeforeBatchExecutionHook);
            lockAtomic(&self.local_db_mutex);
            defer self.local_db_mutex.unlock();
            if (group_auto_bulk_ops > 0) {
                const record_now_ns = platform_time.monotonicNs();
                cache.recordAutoBulkIngestOpsLocked(group.group_id, table_name, group_auto_bulk_ops, record_now_ns) catch |err| {
                    std.log.err("provisioned batch auto bulk accounting failed table={s} group_id={} ops={} err={s}", .{
                        table_name,
                        group.group_id,
                        group_auto_bulk_ops,
                        @errorName(err),
                    });
                    return err;
                };
                const rolled = try cache.rollRequestedAutoBulkIngestLocked(group.group_id, table_name, platform_time.monotonicNs());
                if (rolled) {
                    self.local_db_mutex.unlock();
                    publishRuntimeStatusSnapshot(self, alloc, table_name, group.group_id, cached.db) catch |err| {
                        std.log.warn("auto bulk roll runtime status publish failed table={s} group_id={} err={s}", .{
                            table_name,
                            group.group_id,
                            @errorName(err),
                        });
                    };
                    lockAtomic(&self.local_db_mutex);
                }
            }
        } else {
            var db = try openManagedDbForTableGroupWithRuntimeAndHAWriteGate(alloc, path, self.catalog, table_name, group.group_id, self.backend_runtime, self.ha_write_gate, self.ha_async_mirror);
            defer db.close();
            try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group.group_id, &db);
            try table_write_bulk_ingest.applyGroupBatch(alloc, self.catalog, &db, table_name, group, req, runTestBeforeBatchExecutionHook);
            self.finishTransientManagedDbWriteBeforeClose(table_name, group.group_id, &db);
        }
    }

    fn batch(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        try enforceHAWriteGateOptional(self.ha_write_gate);
        self.beginTableRequest(table_name);
        defer self.endTableRequest(table_name);
        lockAtomic(&self.local_db_mutex);
        self.invalidateReadCache(table_name);
        self.markWriteCacheDirty(table_name);
        self.local_db_mutex.unlock();
        errdefer {
            lockAtomic(&self.local_db_mutex);
            defer self.local_db_mutex.unlock();
            self.invalidateReadCache(table_name);
            self.invalidateWriteCache(table_name);
        }
        var grouped = std.ArrayListUnmanaged(GroupBatch).empty;
        defer {
            for (grouped.items) |*group| group.deinit(alloc);
            grouped.deinit(alloc);
        }

        var snapshot = try self.catalog.adminSnapshot();
        defer self.catalog.freeAdminSnapshot(&snapshot);
        const table = tables_api.findTableByName(&snapshot, table_name) orelse return null;
        const ranges = try metadata_admin.listTableRanges(alloc, &snapshot, table.table_id);
        defer metadata_admin.freeRangeRefs(alloc, ranges);
        if (ranges.len == 0) return null;

        for (req.writes) |write| {
            const group_id = table_catalog.resolveGroupForKeyFromRanges(ranges, write.key) orelse return null;
            const group = try ensureGroupBatch(alloc, &grouped, group_id);
            try group.writes.append(alloc, write);
        }
        for (req.deletes) |key| {
            const group_id = table_catalog.resolveGroupForKeyFromRanges(ranges, key) orelse return null;
            const group = try ensureGroupBatch(alloc, &grouped, group_id);
            try group.deletes.append(alloc, key);
        }
        for (req.relational_identity_rewrites) |rewrite| {
            const old_group_id = table_catalog.resolveGroupForKeyFromRanges(ranges, rewrite.old_key) orelse return null;
            const new_group_id = table_catalog.resolveGroupForKeyFromRanges(ranges, rewrite.new_key) orelse return null;
            if (old_group_id != new_group_id) return error.UnsupportedOperation;
            const group = try ensureGroupBatch(alloc, &grouped, old_group_id);
            try group.relational_identity_rewrites.append(alloc, rewrite);
        }
        for (req.transforms) |transform| {
            const group_id = table_catalog.resolveGroupForKeyFromRanges(ranges, transform.key) orelse return null;
            const group = try ensureGroupBatch(alloc, &grouped, group_id);
            try group.transforms.append(alloc, transform);
        }

        if (self.raft_batcher) |batcher| {
            for (grouped.items) |group| {
                try batcher.batchGroup(alloc, group.group_id, table_name, .{
                    .writes = group.writes.items,
                    .deletes = group.deletes.items,
                    .relational_identity_rewrites = group.relational_identity_rewrites.items,
                    .transforms = group.transforms.items,
                    .sync_level = req.sync_level,
                });
            }
            return {};
        }

        for (grouped.items) |group| {
            if (self.canCoalesceProvisionedGroupBatch(group, req)) {
                if (self.writeCoalescerActive(table_name, group.group_id)) {
                    try self.enqueueProvisionedGroupBatchCoalesced(alloc, table_name, group, req);
                } else if (self.tryBeginGroupOperation(table_name, group.group_id)) {
                    defer self.endGroupOperation(table_name, group.group_id);
                    try self.applyProvisionedGroupBatchLocked(alloc, table_name, group, req);
                } else {
                    try self.enqueueProvisionedGroupBatchCoalesced(alloc, table_name, group, req);
                }
            } else {
                self.beginGroupOperation(table_name, group.group_id);
                defer self.endGroupOperation(table_name, group.group_id);
                try self.applyProvisionedGroupBatchLocked(alloc, table_name, group, req);
            }
        }
        lockAtomic(&self.local_db_mutex);
        self.markWriteCacheDirty(table_name);
        self.local_db_mutex.unlock();
        self.publishDirtyWriteCacheRuntimeStatusesBestEffort(alloc, table_name);
        self.notifyLocalChange(table_name, .data);
    }

    fn batchCatalogNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        req: db_mod.types.BatchRequest,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const table_name = try nativeCatalogTableNameAlloc(alloc, self.catalog, target);
        defer alloc.free(table_name);
        return try batch(ptr, alloc, table_name, req);
    }

    fn backupTable(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        plan: backups_api.TableBackupPlan,
    ) !?[]backups_api.ShardSnapshot {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const group_id = (try table_catalog.resolveSingleRangeGroup(alloc, self.catalog, table_name)) orelse return null;
        self.beginGroupOperation(table_name, group_id);
        lockAtomic(&self.local_db_mutex);
        self.invalidateWriteCache(table_name);
        self.local_db_mutex.unlock();
        defer {
            self.endGroupOperation(table_name, group_id);
        }
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        var db = try openManagedDbForTableGroupWithRuntimeAndHAWriteGate(alloc, path, self.catalog, table_name, group_id, self.backend_runtime, self.ha_write_gate, self.ha_async_mirror);
        defer db.close();

        const snapshot_token = try std.fmt.allocPrint(alloc, "{s}-g{d}", .{ plan.backup_id, group_id });
        defer alloc.free(snapshot_token);
        return try table_write_backup_restore.backupOpenDbShard(alloc, &db, plan.backup_root, plan.backup_id, group_id, snapshot_token, plan.format);
    }

    fn backupCatalogTableNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        plan: backups_api.TableBackupPlan,
    ) !?[]backups_api.ShardSnapshot {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const table_name = try nativeCatalogTableNameAlloc(alloc, self.catalog, target);
        defer alloc.free(table_name);
        return try backupTable(ptr, alloc, table_name, plan);
    }

    fn restoreTable(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        plan: backups_api.TableRestorePlan,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        try enforceHAWriteGateOptional(self.ha_write_gate);
        if (plan.manifest.shards.len == 0) return error.UnsupportedBackupFormat;

        const group_id = (try table_catalog.resolveSingleRangeGroup(alloc, self.catalog, table_name)) orelse return null;
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const identity_namespace = try loadTableIdentityNamespaceForGroup(alloc, self.catalog, table_name, group_id);
        self.beginStructuralTableActivity(table_name);
        var structural_active = true;
        errdefer if (structural_active) self.endStructuralTableActivity(table_name);
        lockAtomic(&self.local_db_mutex);
        self.invalidateWriteCache(table_name);
        self.invalidateReadCache(table_name);
        self.invalidateRuntimeStatusCache(table_name);
        self.local_db_mutex.unlock();
        try table_write_backup_restore.restoreProvisionedTableGroupDeferredRuntimeRepair(
            alloc,
            path,
            table_name,
            group_id,
            identity_namespace,
            plan,
            runTestBeforeRestoreWorkHook,
        );

        lockAtomic(&self.local_db_mutex);
        self.invalidateReadCache(table_name);
        self.invalidateWriteCache(table_name);
        self.invalidateRuntimeStatusCache(table_name);
        self.local_db_mutex.unlock();
        self.endStructuralTableActivity(table_name);
        structural_active = false;
        self.notifyLocalChange(table_name, .structural);
        self.notifyLocalChange(table_name, .data);
        self.requestRestoreRepairCatchUp(table_name, group_id);
    }

    fn restoreCatalogTableNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        plan: backups_api.TableRestorePlan,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const table_name = try nativeCatalogTableNameAlloc(alloc, self.catalog, target);
        defer alloc.free(table_name);
        return try restoreTable(ptr, alloc, table_name, plan);
    }

    fn commitTransaction(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        tables: []const distributed_txn.TableCommitRequest,
        sync_level: db_mod.types.SyncLevel,
    ) !?distributed_txn.CommitOutcome {
        const txn_id = nextTxnId();
        return try commitTransactionWithId(ptr, alloc, txn_id, nextTxnTimestamp(), tables, sync_level);
    }

    fn commitTransactionWithId(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        begin_timestamp: u64,
        tables: []const distributed_txn.TableCommitRequest,
        _: db_mod.types.SyncLevel,
    ) !?distributed_txn.CommitOutcome {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        try enforceHAWriteGateOptional(self.ha_write_gate);
        var worker_impl = distributed_txn.LocalTableWriteParticipantWorker.init(self.source());
        var read_source = table_read_sources.ProvisionedTableReadSource.init(self.replica_root_dir, self.catalog, raft_mod.read_gate.noopReadableLeaseRequester());
        read_source.cache = self.read_cache;
        read_source.backend_runtime = self.backend_runtime;
        read_source.primary_lookup_db = self.primaryLookupDbSource();
        read_source.secret_store = self.secret_store;
        read_source.remote_content = self.remote_content;
        _ = worker_impl.withReads(read_source.source());
        const commit_version = begin_timestamp + 1;
        return try distributed_txn.executeMultiTableCommit(
            alloc,
            self.catalog,
            worker_impl.worker(),
            txn_id,
            begin_timestamp,
            commit_version,
            tables,
            if (comptime build_options.with_tla) tracing.stderrAntflyTraceWriter() else null,
        );
    }

    fn batchGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        try enforceHAWriteGateOptional(self.ha_write_gate);
        if (self.raft_batcher) |batcher| {
            try batcher.batchGroupLocal(alloc, group_id, table_name, req);
            return {};
        }
        return try self.applyReplicatedBatchGroupLocal(alloc, group_id, table_name, req);
    }

    pub fn applyReplicatedBatchGroupLocal(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) !?void {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const apply_req = req;
        const auto_bulk_ops = autoBulkIngestBatchOps(apply_req);
        const auto_bulk_now_ns = platform_time.monotonicNs();
        self.beginGroupOperation(table_name, group_id);
        lockAtomic(&self.local_db_mutex);
        self.invalidateReadCache(table_name);
        self.markWriteCacheDirty(table_name);
        self.local_db_mutex.unlock();
        defer {
            self.endGroupOperation(table_name, group_id);
        }
        errdefer {
            lockAtomic(&self.local_db_mutex);
            defer self.local_db_mutex.unlock();
            self.invalidateReadCache(table_name);
            self.invalidateWriteCache(table_name);
        }
        if (self.write_cache) |cache| {
            var cached = try self.getOrOpenCachedDbMode(
                alloc,
                cache,
                path,
                group_id,
                table_name,
                .default_async,
                if (auto_bulk_ops > 0) auto_bulk_now_ns else null,
                if (auto_bulk_ops > 0) auto_bulk_now_ns else null,
            );
            defer cached.deinit(alloc);
            try validateTableBatchAgainstSchemaJson(alloc, cached.db, cached.schema_json, apply_req.writes, apply_req.deletes, apply_req.transforms);
            runTestBeforeBatchExecutionHook();
            cached.db.batchReplicatedApply(apply_req) catch |err| return normalizeRelationalConstraintError(err);
            {
                lockAtomic(&self.local_db_mutex);
                defer self.local_db_mutex.unlock();
                if (auto_bulk_ops > 0) {
                    const record_now_ns = platform_time.monotonicNs();
                    try cache.recordAutoBulkIngestOpsLocked(group_id, table_name, auto_bulk_ops, record_now_ns);
                    const rolled = try cache.rollRequestedAutoBulkIngestLocked(group_id, table_name, platform_time.monotonicNs());
                    if (rolled) {
                        self.local_db_mutex.unlock();
                        publishRuntimeStatusSnapshot(self, alloc, table_name, group_id, cached.db) catch |err| {
                            std.log.warn("auto bulk roll runtime status publish failed table={s} group_id={} err={s}", .{
                                table_name,
                                group_id,
                                @errorName(err),
                            });
                        };
                        lockAtomic(&self.local_db_mutex);
                    }
                }
                self.markWriteCacheDirty(table_name);
            }
            self.publishDirtyWriteCacheRuntimeStatusesBestEffort(alloc, table_name);
            self.notifyLocalChange(table_name, .data);
        } else {
            var db = try openManagedDbForTableGroupWithRuntimeAndHAWriteGate(alloc, path, self.catalog, table_name, group_id, self.backend_runtime, self.ha_write_gate, self.ha_async_mirror);
            defer db.close();
            try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group_id, &db);
            try validateTableBatchAgainstCatalogSchema(alloc, self.catalog, &db, table_name, apply_req.writes, apply_req.deletes, apply_req.transforms);
            runTestBeforeBatchExecutionHook();
            db.batchReplicatedApply(apply_req) catch |err| return normalizeRelationalConstraintError(err);
            self.finishTransientManagedDbWriteBeforeClose(table_name, group_id, &db);
            lockAtomic(&self.local_db_mutex);
            self.markWriteCacheDirty(table_name);
            self.local_db_mutex.unlock();
            self.notifyLocalChange(table_name, .data);
        }
    }

    pub fn applyHAReplicationRecordGroupLocal(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        record: db_mod.HAReplicationRecordView,
    ) !void {
        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);

        if (self.write_cache) |cache| {
            var cached = try self.getOrOpenCachedDbMode(
                alloc,
                cache,
                path,
                group_id,
                table_name,
                .default_async,
                null,
                null,
            );
            defer cached.deinit(alloc);
            try cached.db.applyHAReplicationRecord(record);
            lockAtomic(&self.local_db_mutex);
            self.markWriteCacheDirty(table_name);
            self.local_db_mutex.unlock();
            self.publishDirtyWriteCacheRuntimeStatusesBestEffort(alloc, table_name);
            self.notifyLocalChange(table_name, changeKindForHARecord(record));
        } else {
            var db = try openManagedDbForTableGroupWithRuntimeAndHAWriteGate(alloc, path, self.catalog, table_name, group_id, self.backend_runtime, self.ha_write_gate, self.ha_async_mirror);
            defer db.close();
            try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group_id, &db);
            try db.applyHAReplicationRecord(record);
            self.finishTransientManagedDbWriteBeforeClose(table_name, group_id, &db);
            lockAtomic(&self.local_db_mutex);
            self.markWriteCacheDirty(table_name);
            self.local_db_mutex.unlock();
            self.notifyLocalChange(table_name, changeKindForHARecord(record));
        }
    }

    pub fn syncReplicatedBatchGroupLocal(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        sync_level: db_mod.types.SyncLevel,
    ) !void {
        switch (sync_level) {
            .propose, .write => return,
            .enrichments, .full_text, .aknn, .full_index => {},
        }

        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);

        if (self.write_cache) |cache| {
            var cached = try self.getOrOpenCachedDbMode(
                alloc,
                cache,
                path,
                group_id,
                table_name,
                .default_async,
                null,
                null,
            );
            defer cached.deinit(alloc);
            try cached.db.waitForCurrentSyncLevel(sync_level);
            self.publishDirtyWriteCacheRuntimeStatusesBestEffort(alloc, table_name);
        } else {
            var db = try openManagedDbForTableGroupWithRuntimeAndHAWriteGate(alloc, path, self.catalog, table_name, group_id, self.backend_runtime, self.ha_write_gate, self.ha_async_mirror);
            defer db.close();
            try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group_id, &db);
            try db.waitForCurrentSyncLevel(sync_level);
            self.finishTransientManagedDbWriteBeforeClose(table_name, group_id, &db);
        }
        self.notifyLocalChange(table_name, .data);
    }

    fn finishTransientManagedDbWriteBeforeClose(
        self: *ProvisionedTableWriteSource,
        table_name: []const u8,
        group_id: u64,
        db: *db_mod.DB,
    ) void {
        if (self.runtime_status_cache != null) {
            drainManagedDbBeforeClose(db) catch |err| {
                if (!isTransientReplayVisibilityError(err)) {
                    std.log.warn("transient managed writer drain before status publish failed table={s} group_id={} err={s}", .{
                        table_name,
                        group_id,
                        @errorName(err),
                    });
                }
            };
            _ = self.publishManagedRuntimeStatusBestEffort(table_name, group_id, db);
        }
    }

    fn txnBeginGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        begin_timestamp: u64,
        topology_epoch: u64,
        participants: []const []const u8,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        try enforceHAWriteGateOptional(self.ha_write_gate);
        try table_catalog.validateGroupTopologyEpoch(alloc, self.catalog, table_name, group_id, topology_epoch);
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        var db = try openManagedDbForTableGroupWithRuntimeAndHAWriteGate(alloc, path, self.catalog, table_name, group_id, self.backend_runtime, self.ha_write_gate, self.ha_async_mirror);
        defer db.close();
        try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group_id, &db);
        // Recovery can notify this same table/group through the participant
        // worker, so run it before taking the group activity gate.
        try recoverProvisionedTransactionsOnce(self, alloc, &db);
        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);
        _ = try db.beginTransactionWithIdAndParticipants(txn_id, begin_timestamp, participants);
    }

    fn txnPrepareGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        topology_epoch: u64,
        req: db_mod.types.TransactionIntentRequest,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        try enforceHAWriteGateOptional(self.ha_write_gate);
        try table_catalog.validateGroupTopologyEpoch(alloc, self.catalog, table_name, group_id, topology_epoch);
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        var db = try openManagedDbForTableGroupWithRuntimeAndHAWriteGate(alloc, path, self.catalog, table_name, group_id, self.backend_runtime, self.ha_write_gate, self.ha_async_mirror);
        defer db.close();
        try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group_id, &db);
        // Recovery can notify this same table/group through the participant
        // worker, so run it before taking the group activity gate.
        try recoverProvisionedTransactionsOnce(self, alloc, &db);
        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);
        try validateTransactionAgainstCatalogSchema(alloc, self.catalog, &db, table_name, req.writes, req.deletes, req.transforms);
        db.writeTransaction(txn_id, req) catch |err| return normalizeRelationalConstraintError(err);
    }

    fn txnResolveGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        status: db_mod.types.TxnStatus,
        commit_version: u64,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        try enforceHAWriteGateOptional(self.ha_write_gate);
        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);
        if (status == .committed) {
            lockAtomic(&self.local_db_mutex);
            self.invalidateReadCache(table_name);
            self.markWriteCacheDirty(table_name);
            self.local_db_mutex.unlock();
            errdefer {
                lockAtomic(&self.local_db_mutex);
                defer self.local_db_mutex.unlock();
                self.invalidateReadCache(table_name);
                self.invalidateWriteCache(table_name);
            }
        }
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        var db = try openManagedDbForTableGroupWithRuntimeAndHAWriteGate(alloc, path, self.catalog, table_name, group_id, self.backend_runtime, self.ha_write_gate, self.ha_async_mirror);
        defer db.close();
        try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group_id, &db);
        db.resolveTransactionIntents(txn_id, status, commit_version) catch |err| {
            return normalizeRelationalConstraintError(err);
        };
        const participant = try std.fmt.allocPrint(alloc, "group:{d}", .{group_id});
        defer alloc.free(participant);
        try db.markTransactionParticipantResolved(txn_id, participant);
        // Keep the resolved participant marker in the same drain window as the
        // committed row/index effects, so the next open does not recover it.
        if (status == .committed) try drainManagedDbBeforeClose(&db);
        if (status == .committed) {
            lockAtomic(&self.local_db_mutex);
            self.invalidateWriteCache(table_name);
            self.invalidateReadCache(table_name);
            self.local_db_mutex.unlock();
            self.notifyLocalChange(table_name, .data);
        }
    }

    fn txnStatusGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
    ) !?db_mod.types.TxnStatus {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        var db = try openManagedDbForTableGroupWithRuntimeAndHAWriteGate(alloc, path, self.catalog, table_name, group_id, self.backend_runtime, self.ha_write_gate, self.ha_async_mirror);
        defer db.close();
        try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group_id, &db);
        try recoverProvisionedTransactionsOnce(self, alloc, &db);
        return try db.getTransactionStatus(txn_id);
    }

    fn localRuntimeStatuses(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try self.snapshotRuntimeStatusesBestEffort(alloc, table_name);
    }

    fn localRuntimeStatusesCatalogNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const table_name = try nativeCatalogTableNameAlloc(alloc, self.catalog, target);
        defer alloc.free(table_name);
        return try localRuntimeStatuses(ptr, alloc, table_name);
    }

    fn foreignKeyIntegrity(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        action: ForeignKeyIntegrityAction,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?ForeignKeyIntegrityResult {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        if (action == .explain_delete) {
            var read_source = table_read_sources.ProvisionedTableReadSource.init(
                self.replica_root_dir,
                self.catalog,
                raft_mod.read_gate.noopReadableLeaseRequester(),
            );
            var worker_impl = distributed_txn.LocalTableWriteParticipantWorker.init(self.source());
            _ = worker_impl.withReads(read_source.source());
            if (try distributed_txn.explainRoutedForeignKeyParentDelete(
                alloc,
                self.catalog,
                worker_impl.worker(),
                table_name,
                constraint_name,
                lower_doc_key,
            )) |explain| {
                return try foreignKeyIntegrityResultFromRoutedExplain(alloc, action, violation_limit, explain);
            }
        }
        const group_ids = try resolveForeignKeyIntegrityGroupsEventually(
            alloc,
            self.catalog,
            table_name,
            action,
            lower_doc_key,
            upper_doc_key,
        );
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;

        const planned_units: []ForeignKeyIntegrityWorkUnit = if (action == .explain_delete)
            &.{}
        else
            try planForeignKeyIntegrityWorkUnits(alloc, self.catalog, table_name, action, constraint_name, lower_doc_key, upper_doc_key);
        var planned_units_owned = true;
        errdefer if (planned_units_owned) {
            for (planned_units) |*unit| unit.deinit(alloc);
            if (planned_units.len > 0) alloc.free(planned_units);
        };
        if (action == .plan) {
            const groups = try alloc.alloc(ForeignKeyIntegrityGroupReport, 0);
            errdefer alloc.free(groups);
            const violations_empty = try alloc.alloc(ForeignKeyIntegrityViolation, 0);
            errdefer alloc.free(violations_empty);
            const work_statuses = try buildForeignKeyIntegrityWorkStatuses(alloc, planned_units, &.{}, &.{}, .planned);
            errdefer {
                for (work_statuses) |*status| status.deinit(alloc);
                if (work_statuses.len > 0) alloc.free(work_statuses);
            }
            const result: ForeignKeyIntegrityResult = .{
                .action = action,
                .valid = true,
                .complete = true,
                .violation_limit = violation_limit,
                .violations_truncated = false,
                .report = .{},
                .delete_plan = null,
                .groups = groups,
                .progress = &.{},
                .work_units = planned_units,
                .work_claims = &.{},
                .work_statuses = work_statuses,
                .violations = violations_empty,
            };
            planned_units_owned = false;
            return result;
        }
        planned_units_owned = false;
        defer {
            for (planned_units) |*unit| unit.deinit(alloc);
            if (planned_units.len > 0) alloc.free(planned_units);
        }

        var group_reports = std.ArrayListUnmanaged(ForeignKeyIntegrityGroupReport).empty;
        var progress_entries = std.ArrayListUnmanaged(ForeignKeyIntegrityProgress).empty;
        var work_units = std.ArrayListUnmanaged(ForeignKeyIntegrityWorkUnit).empty;
        var work_claims = std.ArrayListUnmanaged(ForeignKeyIntegrityWorkClaim).empty;
        var jobs = std.ArrayListUnmanaged(ForeignKeyIntegrityJobStatus).empty;
        var violations = std.ArrayListUnmanaged(ForeignKeyIntegrityViolation).empty;
        errdefer {
            group_reports.deinit(alloc);
            for (progress_entries.items) |*progress| progress.deinit(alloc);
            progress_entries.deinit(alloc);
            for (work_units.items) |*unit| unit.deinit(alloc);
            work_units.deinit(alloc);
            for (work_claims.items) |*claim| claim.deinit(alloc);
            work_claims.deinit(alloc);
            for (jobs.items) |*job| job.deinit(alloc);
            jobs.deinit(alloc);
            for (violations.items) |*violation| violation.deinit(alloc);
            violations.deinit(alloc);
        }
        try work_units.ensureUnusedCapacity(alloc, planned_units.len);
        for (planned_units) |unit| {
            work_units.appendAssumeCapacity(try cloneForeignKeyIntegrityWorkUnit(alloc, unit));
        }

        var aggregate: db_mod.relational_store.ForeignKeyIntegrityReport = .{};
        var delete_plan: ?db_mod.relational_store.ForeignKeyDeletePlan = null;
        var truncated = false;
        var valid = true;
        var complete = true;
        if (action == .progress or action == .explain_delete) {
            for (group_ids) |group_id| {
                var one = (try runProvisionedForeignKeyIntegrityGroupLocal(
                    self,
                    alloc,
                    group_id,
                    table_name,
                    action,
                    constraint_name,
                    lower_doc_key,
                    upper_doc_key,
                    violation_limit -| violations.items.len,
                )) orelse return null;
                defer one.deinit(alloc);
                try appendForeignKeyIntegrityResult(
                    alloc,
                    one,
                    violation_limit,
                    &aggregate,
                    &group_reports,
                    &progress_entries,
                    &work_units,
                    &work_claims,
                    &jobs,
                    &violations,
                    &delete_plan,
                    &truncated,
                    &valid,
                    &complete,
                );
            }
        } else {
            for (planned_units) |unit| {
                var one = (try runProvisionedForeignKeyIntegrityGroupLocal(
                    self,
                    alloc,
                    unit.group_id,
                    table_name,
                    action,
                    constraint_name,
                    unit.lower_doc_key,
                    unit.upper_doc_key,
                    violation_limit -| violations.items.len,
                )) orelse return null;
                defer one.deinit(alloc);
                try appendForeignKeyIntegrityResult(
                    alloc,
                    one,
                    violation_limit,
                    &aggregate,
                    &group_reports,
                    &progress_entries,
                    &work_units,
                    &work_claims,
                    &jobs,
                    &violations,
                    &delete_plan,
                    &truncated,
                    &valid,
                    &complete,
                );
            }
        }

        const work_statuses = try buildForeignKeyIntegrityWorkStatuses(alloc, work_units.items, progress_entries.items, work_claims.items, .pending);
        errdefer {
            for (work_statuses) |*status| status.deinit(alloc);
            if (work_statuses.len > 0) alloc.free(work_statuses);
        }

        return .{
            .action = action,
            .valid = valid,
            .complete = complete,
            .violation_limit = violation_limit,
            .violations_truncated = truncated,
            .report = aggregate,
            .delete_plan = delete_plan,
            .groups = try group_reports.toOwnedSlice(alloc),
            .progress = try progress_entries.toOwnedSlice(alloc),
            .work_units = try work_units.toOwnedSlice(alloc),
            .work_claims = try work_claims.toOwnedSlice(alloc),
            .work_statuses = work_statuses,
            .jobs = try jobs.toOwnedSlice(alloc),
            .violations = try violations.toOwnedSlice(alloc),
        };
    }

    fn foreignKeyIntegrityGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        action: ForeignKeyIntegrityAction,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?ForeignKeyIntegrityResult {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try runProvisionedForeignKeyIntegrityGroupLocal(
            self,
            alloc,
            group_id,
            table_name,
            action,
            constraint_name,
            lower_doc_key,
            upper_doc_key,
            violation_limit,
        );
    }

    fn foreignKeyIntegrityWorkUnitGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        action: ForeignKeyIntegrityAction,
        phase: []const u8,
        job_id: ?[]const u8,
        claim_key: []const u8,
        worker_id: []const u8,
        lease_ms: u64,
        max_work_units: usize,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?ForeignKeyIntegrityResult {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try runProvisionedForeignKeyIntegrityClaimedWorkUnitGroupLocal(
            self,
            alloc,
            group_id,
            table_name,
            action,
            phase,
            job_id,
            claim_key,
            worker_id,
            lease_ms,
            max_work_units,
            constraint_name,
            lower_doc_key,
            upper_doc_key,
            violation_limit,
        );
    }

    fn secondaryIndexRebuildWorkerPass(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        worker_id: []const u8,
        lease_ms: u64,
        max_work_units: usize,
    ) !?SecondaryIndexRebuildWorkerPassResult {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try runSecondaryIndexRebuildWorkerPassForCatalog(alloc, self.source(), self.catalog, table_name, worker_id, lease_ms, max_work_units);
    }

    fn secondaryIndexRebuildGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        record: metadata_table_manager.SecondaryIndexRebuildRangeRecord,
        worker_id: []const u8,
        lease_ms: u64,
    ) !?SecondaryIndexRebuildWorkerResult {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try runProvisionedSecondaryIndexRebuildGroupLocal(self, alloc, group_id, table_name, record, worker_id, lease_ms);
    }

    fn schemaRewriteWorkerPass(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        worker_id: []const u8,
        lease_ms: u64,
        max_work_units: usize,
    ) !?SchemaRewriteWorkerPassResult {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try runSchemaRewriteWorkerPassForCatalog(alloc, self.source(), self.catalog, table_name, worker_id, lease_ms, max_work_units);
    }

    fn schemaRewriteGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        record: metadata_table_manager.SchemaRewriteJobRecord,
        worker_id: []const u8,
        lease_ms: u64,
    ) !?SchemaRewriteWorkerResult {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try runProvisionedSchemaRewriteGroupLocal(self, alloc, group_id, table_name, record, worker_id, lease_ms);
    }

    fn uniqueConstraintIntegrity(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        action: UniqueConstraintIntegrityAction,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
    ) !?UniqueConstraintIntegrityResult {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const group_ids = try table_catalog.resolveGroupsForSpanEventually(
            alloc,
            self.catalog,
            table_name,
            lower_doc_key,
            upper_doc_key,
            5 * std.time.ns_per_s,
            10,
        );
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;

        var group_reports = std.ArrayListUnmanaged(UniqueConstraintIntegrityGroupReport).empty;
        var progress_entries = std.ArrayListUnmanaged(UniqueConstraintIntegrityProgress).empty;
        errdefer {
            group_reports.deinit(alloc);
            for (progress_entries.items) |*progress| progress.deinit(alloc);
            progress_entries.deinit(alloc);
        }

        var aggregate: db_mod.relational_store.UniqueConstraintIntegrityReport = .{};
        var valid = true;
        var complete = true;
        for (group_ids) |group_id| {
            var one = (try runProvisionedUniqueConstraintIntegrityGroupLocal(
                self,
                alloc,
                group_id,
                table_name,
                action,
                lower_doc_key,
                upper_doc_key,
            )) orelse return null;
            defer one.deinit(alloc);
            try appendUniqueConstraintIntegrityResult(alloc, one, &aggregate, &group_reports, &progress_entries, &valid, &complete);
        }

        var owner_topology = try table_write_integrity.inspectUniqueConstraintOwnerTopology(alloc, self.catalog, table_name);
        errdefer if (owner_topology) |*topology| topology.deinit(alloc);

        return .{
            .action = action,
            .valid = valid,
            .complete = complete,
            .report = aggregate,
            .groups = try group_reports.toOwnedSlice(alloc),
            .owner_topology = owner_topology,
            .progress = try progress_entries.toOwnedSlice(alloc),
        };
    }

    fn uniqueConstraintIntegrityGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        action: UniqueConstraintIntegrityAction,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
    ) !?UniqueConstraintIntegrityResult {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try runProvisionedUniqueConstraintIntegrityGroupLocal(
            self,
            alloc,
            group_id,
            table_name,
            action,
            lower_doc_key,
            upper_doc_key,
        );
    }

    fn foreignKeyRefChildrenGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        limit: usize,
    ) !?[]db_mod.types.ForeignKeyRefChild {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);

        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);

        if (self.write_cache) |cache| {
            var cached = try self.getOrOpenCachedDbMode(alloc, cache, path, group_id, table_name, .status_only, null, null);
            defer cached.deinit(alloc);
            return try cached.db.listForeignKeyRefChildrenForParent(alloc, constraint_name, parent_table, parent_key, limit);
        }

        var db = try openManagedDbForTableGroupWithRuntime(alloc, path, self.catalog, table_name, group_id, self.backend_runtime);
        defer db.close();
        return try db.listForeignKeyRefChildrenForParent(alloc, constraint_name, parent_table, parent_key, limit);
    }

    fn foreignKeyRefChildrenPageGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        start_after_child_table: ?[]const u8,
        start_after_child_key: ?[]const u8,
        limit: usize,
    ) !?db_mod.types.ForeignKeyRefChildrenPage {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);

        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);

        if (self.write_cache) |cache| {
            var cached = try self.getOrOpenCachedDbMode(alloc, cache, path, group_id, table_name, .status_only, null, null);
            defer cached.deinit(alloc);
            return try cached.db.listForeignKeyRefChildrenPageForParent(alloc, constraint_name, parent_table, parent_key, start_after_child_table, start_after_child_key, limit);
        }

        var db = try openManagedDbForTableGroupWithRuntime(alloc, path, self.catalog, table_name, group_id, self.backend_runtime);
        defer db.close();
        return try db.listForeignKeyRefChildrenPageForParent(alloc, constraint_name, parent_table, parent_key, start_after_child_table, start_after_child_key, limit);
    }

    fn foreignKeyActionJobPage(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
        lease_ms: u64,
    ) !?ForeignKeyActionJobResult {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const owner_parent_table = try foreignKeyActionOwnerParentTableNameAlloc(
            alloc,
            self.catalog,
            table_name,
            constraint_name,
            parent_table,
        );
        defer alloc.free(owner_parent_table);
        var resolution = try table_catalog.resolveForeignKeyRefOwnerGroups(
            alloc,
            self.catalog,
            table_name,
            constraint_name,
            owner_parent_table,
            parent_key,
        );
        defer resolution.deinit(alloc);
        if (!resolution.configured) return error.UnsupportedOperation;
        if (resolution.groups.len == 0) return error.UnknownGroup;

        var groups = std.ArrayListUnmanaged(ForeignKeyActionJobStatus).empty;
        errdefer {
            for (groups.items) |*group| group.deinit(alloc);
            groups.deinit(alloc);
        }
        var complete = true;
        for (resolution.groups) |group_id| {
            var status = try self.runProvisionedForeignKeyActionJobGroupLocal(
                alloc,
                group_id,
                table_name,
                job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                updated_parent_key,
                page_limit,
                lease_ms,
                0,
                64,
            );
            errdefer status.deinit(alloc);
            if (!status.completed) complete = false;
            try groups.append(alloc, status);
        }
        return .{
            .complete = complete,
            .groups = try groups.toOwnedSlice(alloc),
        };
    }

    fn foreignKeyActionJobGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
        lease_ms: u64,
        cascade_depth: u32,
        cascade_max_depth: u32,
    ) !?ForeignKeyActionJobStatus {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try self.runProvisionedForeignKeyActionJobGroupLocal(
            alloc,
            group_id,
            table_name,
            job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
            lease_ms,
            cascade_depth,
            cascade_max_depth,
        );
    }

    fn foreignKeyActionJobSchedule(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
        cascade_depth: u32,
        cascade_max_depth: u32,
    ) !?ForeignKeyActionJobResult {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const owner_parent_table = try foreignKeyActionOwnerParentTableNameAlloc(
            alloc,
            self.catalog,
            table_name,
            constraint_name,
            parent_table,
        );
        defer alloc.free(owner_parent_table);
        var resolution = try table_catalog.resolveForeignKeyRefOwnerGroups(
            alloc,
            self.catalog,
            table_name,
            constraint_name,
            owner_parent_table,
            parent_key,
        );
        defer resolution.deinit(alloc);
        if (!resolution.configured) return error.UnsupportedOperation;
        if (resolution.groups.len == 0) return error.UnknownGroup;

        var groups = std.ArrayListUnmanaged(ForeignKeyActionJobStatus).empty;
        errdefer {
            for (groups.items) |*group| group.deinit(alloc);
            groups.deinit(alloc);
        }
        var complete = true;
        for (resolution.groups) |group_id| {
            var status = try self.scheduleProvisionedForeignKeyActionJobGroupLocal(
                alloc,
                group_id,
                table_name,
                job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                updated_parent_key,
                page_limit,
                cascade_depth,
                cascade_max_depth,
            );
            errdefer status.deinit(alloc);
            if (!status.completed) complete = false;
            try groups.append(alloc, status);
        }
        return .{
            .complete = complete,
            .groups = try groups.toOwnedSlice(alloc),
        };
    }

    fn foreignKeyActionJobRequeue(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
    ) !?ForeignKeyActionJobResult {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const owner_parent_table = try foreignKeyActionOwnerParentTableNameAlloc(
            alloc,
            self.catalog,
            table_name,
            constraint_name,
            parent_table,
        );
        defer alloc.free(owner_parent_table);
        var resolution = try table_catalog.resolveForeignKeyRefOwnerGroups(
            alloc,
            self.catalog,
            table_name,
            constraint_name,
            owner_parent_table,
            parent_key,
        );
        defer resolution.deinit(alloc);
        if (!resolution.configured) return error.UnsupportedOperation;
        if (resolution.groups.len == 0) return error.UnknownGroup;

        var groups = std.ArrayListUnmanaged(ForeignKeyActionJobStatus).empty;
        errdefer {
            for (groups.items) |*group| group.deinit(alloc);
            groups.deinit(alloc);
        }
        for (resolution.groups) |group_id| {
            var status = try self.requeueProvisionedForeignKeyActionJobGroupLocal(
                alloc,
                group_id,
                table_name,
                job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                updated_parent_key,
                page_limit,
            );
            errdefer status.deinit(alloc);
            try groups.append(alloc, status);
        }
        return .{
            .complete = false,
            .groups = try groups.toOwnedSlice(alloc),
        };
    }

    fn foreignKeyActionJobGroupLocalSchedule(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
        cascade_depth: u32,
        cascade_max_depth: u32,
    ) !?ForeignKeyActionJobStatus {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try self.scheduleProvisionedForeignKeyActionJobGroupLocal(
            alloc,
            group_id,
            table_name,
            job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
            cascade_depth,
            cascade_max_depth,
        );
    }

    fn foreignKeyActionJobGroupLocalRequeue(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
    ) !?ForeignKeyActionJobStatus {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try self.requeueProvisionedForeignKeyActionJobGroupLocal(
            alloc,
            group_id,
            table_name,
            job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
        );
    }

    fn foreignKeyActionJobProgress(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?ForeignKeyActionJobProgressResult {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const group_ids = try collectForeignKeyActionJobProgressGroupIds(alloc, self.catalog, table_name);
        defer alloc.free(group_ids);

        var jobs = std.ArrayListUnmanaged(ForeignKeyActionJobStatus).empty;
        errdefer {
            for (jobs.items) |*job| job.deinit(alloc);
            jobs.deinit(alloc);
        }
        for (group_ids) |group_id| {
            var progress = try self.runProvisionedForeignKeyActionJobGroupLocalProgress(alloc, group_id, table_name);
            defer progress.deinit(alloc);
            try appendForeignKeyActionJobStatuses(alloc, &jobs, progress.jobs);
        }
        return .{ .jobs = try jobs.toOwnedSlice(alloc) };
    }

    fn foreignKeyActionJobGroupLocalProgress(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
    ) !?ForeignKeyActionJobProgressResult {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try self.runProvisionedForeignKeyActionJobGroupLocalProgress(alloc, group_id, table_name);
    }

    fn foreignKeyActionScheduleProgress(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?ForeignKeyActionScheduleProgressResult {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const group_ids = try collectForeignKeyActionJobProgressGroupIds(alloc, self.catalog, table_name);
        defer alloc.free(group_ids);

        var schedules = std.ArrayListUnmanaged(ForeignKeyActionScheduleStatus).empty;
        errdefer {
            for (schedules.items) |*schedule| schedule.deinit(alloc);
            schedules.deinit(alloc);
        }
        for (group_ids) |group_id| {
            var progress = try self.runProvisionedForeignKeyActionScheduleGroupLocalProgress(alloc, group_id, table_name);
            defer progress.deinit(alloc);
            try appendForeignKeyActionScheduleStatuses(alloc, &schedules, progress.schedules);
        }
        return .{ .schedules = try schedules.toOwnedSlice(alloc) };
    }

    fn foreignKeyActionScheduleMarkSeeded(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        schedule_id: []const u8,
        scheduled_groups: u64,
    ) !?ForeignKeyActionScheduleStatus {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const group_ids = try collectForeignKeyActionJobProgressGroupIds(alloc, self.catalog, table_name);
        defer alloc.free(group_ids);
        for (group_ids) |group_id| {
            if (try self.markProvisionedForeignKeyActionScheduleGroupLocalSeeded(alloc, group_id, table_name, schedule_id, scheduled_groups)) |status| {
                return status;
            }
        }
        return null;
    }

    fn foreignKeyActionScheduleRequeue(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        schedule_id: []const u8,
        action_job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
    ) !?ForeignKeyActionScheduleStatus {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const group_ids = try collectForeignKeyActionJobProgressGroupIds(alloc, self.catalog, table_name);
        defer alloc.free(group_ids);
        for (group_ids) |group_id| {
            if (try self.requeueProvisionedForeignKeyActionScheduleGroupLocal(alloc, group_id, table_name, schedule_id, action_job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit)) |status| {
                return status;
            }
        }
        return null;
    }

    fn foreignKeyActionScheduleGroupLocalProgress(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
    ) !?ForeignKeyActionScheduleProgressResult {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try self.runProvisionedForeignKeyActionScheduleGroupLocalProgress(alloc, group_id, table_name);
    }

    fn foreignKeyActionScheduleGroupLocalMarkSeeded(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        schedule_id: []const u8,
        scheduled_groups: u64,
    ) !?ForeignKeyActionScheduleStatus {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try self.markProvisionedForeignKeyActionScheduleGroupLocalSeeded(alloc, group_id, table_name, schedule_id, scheduled_groups);
    }

    fn foreignKeyActionScheduleGroupLocalRequeue(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        schedule_id: []const u8,
        action_job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
    ) !?ForeignKeyActionScheduleStatus {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try self.requeueProvisionedForeignKeyActionScheduleGroupLocal(alloc, group_id, table_name, schedule_id, action_job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit);
    }

    fn runProvisionedForeignKeyActionJobGroupLocalProgress(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
    ) !ForeignKeyActionJobProgressResult {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);

        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);

        if (self.write_cache) |cache| {
            var cached = try self.getOrOpenCachedDbMode(alloc, cache, path, group_id, table_name, .status_only, null, null);
            defer cached.deinit(alloc);
            const records = try cached.db.listForeignKeyActionJobRecords();
            defer cached.db.freeForeignKeyActionJobRecords(records);
            return try foreignKeyActionJobProgressFromDbRecords(alloc, group_id, records);
        }

        var db = openManagedDbForTableGroupWithRuntime(alloc, path, self.catalog, table_name, group_id, self.backend_runtime) catch |err| switch (err) {
            error.FileNotFound => return .{ .jobs = &.{} },
            else => return err,
        };
        defer db.close();
        const records = try db.listForeignKeyActionJobRecords();
        defer db.freeForeignKeyActionJobRecords(records);
        return try foreignKeyActionJobProgressFromDbRecords(alloc, group_id, records);
    }

    fn runProvisionedForeignKeyActionScheduleGroupLocalProgress(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
    ) !ForeignKeyActionScheduleProgressResult {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);

        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);

        if (self.write_cache) |cache| {
            var cached = try self.getOrOpenCachedDbMode(alloc, cache, path, group_id, table_name, .status_only, null, null);
            defer cached.deinit(alloc);
            const records = try cached.db.listForeignKeyActionScheduleRecords();
            defer cached.db.freeForeignKeyActionScheduleRecords(records);
            return try foreignKeyActionScheduleProgressFromDbRecords(alloc, group_id, records);
        }

        var db = openManagedDbForTableGroupWithRuntime(alloc, path, self.catalog, table_name, group_id, self.backend_runtime) catch |err| switch (err) {
            error.FileNotFound => return .{ .schedules = &.{} },
            else => return err,
        };
        defer db.close();
        const records = try db.listForeignKeyActionScheduleRecords();
        defer db.freeForeignKeyActionScheduleRecords(records);
        return try foreignKeyActionScheduleProgressFromDbRecords(alloc, group_id, records);
    }

    fn markProvisionedForeignKeyActionScheduleGroupLocalSeeded(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        schedule_id: []const u8,
        scheduled_groups: u64,
    ) !?ForeignKeyActionScheduleStatus {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);

        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);

        if (self.write_cache) |cache| {
            var cached = try self.getOrOpenCachedDbMode(alloc, cache, path, group_id, table_name, .default, null, null);
            errdefer cached.deinit(alloc);
            const record = cached.db.markForeignKeyActionScheduleSeeded(schedule_id, scheduled_groups) catch |err| switch (err) {
                error.ForeignKeyActionScheduleNotFound => {
                    cached.deinit(alloc);
                    return null;
                },
                else => return err,
            };
            defer cached.db.freeForeignKeyActionScheduleRecord(record);
            var status = try foreignKeyActionScheduleStatusFromDbRecord(alloc, group_id, record);
            errdefer status.deinit(alloc);
            try drainManagedDbBeforeClose(cached.db);
            lockAtomic(&self.local_db_mutex);
            self.markWriteCacheDirty(table_name);
            self.invalidateReadCache(table_name);
            self.local_db_mutex.unlock();
            self.publishDirtyWriteCacheRuntimeStatusesBestEffort(alloc, table_name);
            self.notifyLocalChange(table_name, .data);
            cached.deinit(alloc);
            return status;
        }

        var db = openManagedDbForTableGroupWithRuntime(alloc, path, self.catalog, table_name, group_id, self.backend_runtime) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer db.close();
        try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group_id, &db);
        const record = db.markForeignKeyActionScheduleSeeded(schedule_id, scheduled_groups) catch |err| switch (err) {
            error.ForeignKeyActionScheduleNotFound => return null,
            else => return err,
        };
        defer db.freeForeignKeyActionScheduleRecord(record);
        var status = try foreignKeyActionScheduleStatusFromDbRecord(alloc, group_id, record);
        errdefer status.deinit(alloc);
        self.finishTransientManagedDbWriteBeforeClose(table_name, group_id, &db);
        self.notifyLocalChange(table_name, .data);
        return status;
    }

    fn requeueProvisionedForeignKeyActionScheduleGroupLocal(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        schedule_id: []const u8,
        action_job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
    ) !?ForeignKeyActionScheduleStatus {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);

        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);

        if (self.write_cache) |cache| {
            var cached = try self.getOrOpenCachedDbMode(alloc, cache, path, group_id, table_name, .default, null, null);
            errdefer cached.deinit(alloc);
            const record = cached.db.requeueForeignKeyActionScheduleWithUpdatedParentKeyAt(
                schedule_id,
                action_job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                updated_parent_key,
                page_limit,
                nextTxnTimestamp(),
            ) catch |err| switch (err) {
                error.ForeignKeyActionScheduleNotFound => {
                    cached.deinit(alloc);
                    return null;
                },
                else => return err,
            };
            defer cached.db.freeForeignKeyActionScheduleRecord(record);
            var status = try foreignKeyActionScheduleStatusFromDbRecord(alloc, group_id, record);
            errdefer status.deinit(alloc);
            try drainManagedDbBeforeClose(cached.db);
            lockAtomic(&self.local_db_mutex);
            self.markWriteCacheDirty(table_name);
            self.invalidateReadCache(table_name);
            self.local_db_mutex.unlock();
            self.publishDirtyWriteCacheRuntimeStatusesBestEffort(alloc, table_name);
            self.notifyLocalChange(table_name, .data);
            cached.deinit(alloc);
            return status;
        }

        var db = openManagedDbForTableGroupWithRuntime(alloc, path, self.catalog, table_name, group_id, self.backend_runtime) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer db.close();
        try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group_id, &db);
        const record = db.requeueForeignKeyActionScheduleWithUpdatedParentKeyAt(
            schedule_id,
            action_job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
            nextTxnTimestamp(),
        ) catch |err| switch (err) {
            error.ForeignKeyActionScheduleNotFound => return null,
            else => return err,
        };
        defer db.freeForeignKeyActionScheduleRecord(record);
        var status = try foreignKeyActionScheduleStatusFromDbRecord(alloc, group_id, record);
        errdefer status.deinit(alloc);
        self.finishTransientManagedDbWriteBeforeClose(table_name, group_id, &db);
        self.notifyLocalChange(table_name, .data);
        return status;
    }

    fn scheduleProvisionedForeignKeyActionJobGroupLocal(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
        cascade_depth: u32,
        cascade_max_depth: u32,
    ) !ForeignKeyActionJobStatus {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);

        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);

        if (self.write_cache) |cache| {
            var cached = try self.getOrOpenCachedDbMode(alloc, cache, path, group_id, table_name, .default, null, null);
            errdefer cached.deinit(alloc);
            const record = try cached.db.scheduleForeignKeyActionJobWithUpdatedParentKeyAndCascadeLineageAt(
                job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                updated_parent_key,
                page_limit,
                cascade_depth,
                cascade_max_depth,
                nextTxnTimestamp(),
            );
            defer cached.db.freeForeignKeyActionJobRecord(record);
            var status = try foreignKeyActionJobStatusFromDbRecord(alloc, group_id, record);
            errdefer status.deinit(alloc);
            try drainManagedDbBeforeClose(cached.db);
            lockAtomic(&self.local_db_mutex);
            self.markWriteCacheDirty(table_name);
            self.invalidateReadCache(table_name);
            self.local_db_mutex.unlock();
            self.publishDirtyWriteCacheRuntimeStatusesBestEffort(alloc, table_name);
            self.notifyLocalChange(table_name, .data);
            cached.deinit(alloc);
            return status;
        }

        var db = try openManagedDbForTableGroupWithRuntime(alloc, path, self.catalog, table_name, group_id, self.backend_runtime);
        defer db.close();
        try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group_id, &db);
        const record = try db.scheduleForeignKeyActionJobWithUpdatedParentKeyAndCascadeLineageAt(
            job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
            cascade_depth,
            cascade_max_depth,
            nextTxnTimestamp(),
        );
        defer db.freeForeignKeyActionJobRecord(record);
        var status = try foreignKeyActionJobStatusFromDbRecord(alloc, group_id, record);
        errdefer status.deinit(alloc);
        self.finishTransientManagedDbWriteBeforeClose(table_name, group_id, &db);
        self.notifyLocalChange(table_name, .data);
        return status;
    }

    fn requeueProvisionedForeignKeyActionJobGroupLocal(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
    ) !ForeignKeyActionJobStatus {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);

        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);

        if (self.write_cache) |cache| {
            var cached = try self.getOrOpenCachedDbMode(alloc, cache, path, group_id, table_name, .default, null, null);
            defer cached.deinit(alloc);
            const record = try cached.db.requeueForeignKeyActionJobWithUpdatedParentKey(
                job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                updated_parent_key,
                page_limit,
            );
            defer cached.db.freeForeignKeyActionJobRecord(record);
            var status = try foreignKeyActionJobStatusFromDbRecord(alloc, group_id, record);
            errdefer status.deinit(alloc);
            return status;
        }

        var db = try openManagedDbForTableGroupWithRuntime(alloc, path, self.catalog, table_name, group_id, self.backend_runtime);
        defer db.close();
        const record = try db.requeueForeignKeyActionJobWithUpdatedParentKey(
            job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
        );
        defer db.freeForeignKeyActionJobRecord(record);
        var status = try foreignKeyActionJobStatusFromDbRecord(alloc, group_id, record);
        errdefer status.deinit(alloc);
        return status;
    }

    fn runProvisionedForeignKeyActionJobGroupLocal(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
        lease_ms: u64,
        cascade_depth: u32,
        cascade_max_depth: u32,
    ) !ForeignKeyActionJobStatus {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);

        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);

        if (self.write_cache) |cache| {
            var cached = try self.getOrOpenCachedDbMode(alloc, cache, path, group_id, table_name, .default, null, null);
            errdefer cached.deinit(alloc);
            const record = try self.claimRunAndFinishProvisionedForeignKeyActionJobGroupLocal(
                alloc,
                group_id,
                table_name,
                cached.db,
                job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                updated_parent_key,
                page_limit,
                lease_ms,
                cascade_depth,
                cascade_max_depth,
            );
            defer cached.db.freeForeignKeyActionJobRecord(record);
            var status = try foreignKeyActionJobStatusFromDbRecord(alloc, group_id, record);
            errdefer status.deinit(alloc);
            try drainManagedDbBeforeClose(cached.db);
            lockAtomic(&self.local_db_mutex);
            self.markWriteCacheDirty(table_name);
            self.invalidateReadCache(table_name);
            self.local_db_mutex.unlock();
            self.publishDirtyWriteCacheRuntimeStatusesBestEffort(alloc, table_name);
            self.notifyLocalChange(table_name, .data);
            cached.deinit(alloc);
            return status;
        }

        var db = try openManagedDbForTableGroupWithRuntime(alloc, path, self.catalog, table_name, group_id, self.backend_runtime);
        defer db.close();
        try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group_id, &db);
        const record = try self.claimRunAndFinishProvisionedForeignKeyActionJobGroupLocal(
            alloc,
            group_id,
            table_name,
            &db,
            job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
            lease_ms,
            cascade_depth,
            cascade_max_depth,
        );
        defer db.freeForeignKeyActionJobRecord(record);
        var status = try foreignKeyActionJobStatusFromDbRecord(alloc, group_id, record);
        errdefer status.deinit(alloc);
        self.finishTransientManagedDbWriteBeforeClose(table_name, group_id, &db);
        self.notifyLocalChange(table_name, .data);
        return status;
    }

    fn claimRunAndFinishProvisionedForeignKeyActionJobGroupLocal(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        db: *db_mod.DB,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
        lease_ms: u64,
        cascade_depth: u32,
        cascade_max_depth: u32,
    ) !db_mod.DB.ForeignKeyActionJobRecord {
        if (db.core.schema != null and try foreignKeyActionCanRunGroupDbLocal(
            alloc,
            self.catalog,
            group_id,
            table_name,
            constraint_name,
            parent_table,
            parent_key,
        )) {
            return try db.claimAndRunForeignKeyActionJobPageWithUpdatedParentKeyAndCascadeLineageAt(
                job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                updated_parent_key,
                page_limit,
                lease_ms,
                cascade_depth,
                cascade_max_depth,
                nextTxnTimestamp(),
            );
        }

        const claimed = try db.claimForeignKeyActionJobPageWithUpdatedParentKeyAndCascadeLineageAt(
            job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
            lease_ms,
            cascade_depth,
            cascade_max_depth,
            nextTxnTimestamp(),
        );
        var claimed_owned = true;
        defer if (claimed_owned) db.freeForeignKeyActionJobRecord(claimed);
        if (claimed.completed) {
            claimed_owned = false;
            return claimed;
        }

        var worker = distributed_txn.LocalTableWriteParticipantWorker.init(self.source());
        const begin_timestamp = nextTxnTimestamp();
        var execution = distributed_txn.executeForeignKeyActionPage(
            alloc,
            self.catalog,
            worker.worker(),
            stableForeignKeyActionPageTxnId(claimed),
            begin_timestamp,
            begin_timestamp +| 1,
            table_name,
            group_id,
            action,
            constraint_name,
            parent_table,
            parent_key,
            claimed.updated_parent_key,
            claimed.next_child_table,
            claimed.next_child_key,
            page_limit,
            claimed.cascade_depth,
            claimed.cascade_max_depth,
            null,
        ) catch |err| {
            const failed = db.finishClaimedForeignKeyActionJobPage(
                claimed,
                0,
                false,
                claimed.next_child_table,
                claimed.next_child_key,
                @errorName(err),
            ) catch null;
            if (failed) |record| db.freeForeignKeyActionJobRecord(record);
            return err;
        };
        defer execution.deinit(alloc);

        return try db.finishClaimedForeignKeyActionJobPage(
            claimed,
            execution.applied_children,
            execution.complete,
            execution.next_child_table,
            execution.next_child_key,
            null,
        );
    }

    fn runProvisionedForeignKeyIntegrityGroupLocal(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        action: ForeignKeyIntegrityAction,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?ForeignKeyIntegrityResult {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);

        const mode: ManagedDbOpenMode = switch (action) {
            .plan, .validate, .dry_run, .list, .explain_delete, .progress => .status_only,
            .repair => .default,
        };

        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);

        if (self.write_cache) |cache| {
            var cached = try self.getOrOpenCachedDbMode(alloc, cache, path, group_id, table_name, mode, null, null);
            defer cached.deinit(alloc);
            var result = try runForeignKeyIntegrityOnDb(alloc, cached.db, group_id, action, constraint_name, lower_doc_key, upper_doc_key, violation_limit);
            errdefer result.deinit(alloc);
            if (action == .repair) {
                try drainManagedDbBeforeClose(cached.db);
                lockAtomic(&self.local_db_mutex);
                self.markWriteCacheDirty(table_name);
                self.invalidateReadCache(table_name);
                self.local_db_mutex.unlock();
                self.publishDirtyWriteCacheRuntimeStatusesBestEffort(alloc, table_name);
                self.notifyLocalChange(table_name, .data);
            }
            return result;
        }

        var db = try openManagedDbForTableGroupWithRuntime(alloc, path, self.catalog, table_name, group_id, self.backend_runtime);
        defer db.close();
        try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group_id, &db);
        var result = try runForeignKeyIntegrityOnDb(alloc, &db, group_id, action, constraint_name, lower_doc_key, upper_doc_key, violation_limit);
        errdefer result.deinit(alloc);
        if (action == .repair) {
            self.finishTransientManagedDbWriteBeforeClose(table_name, group_id, &db);
            self.notifyLocalChange(table_name, .data);
        }
        return result;
    }

    fn collectProvisionedForeignKeyIntegrityStatusSnapshot(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        planned_units: []const ForeignKeyIntegrityWorkUnit,
        progress_entries: *std.ArrayListUnmanaged(ForeignKeyIntegrityProgress),
        work_claims: *std.ArrayListUnmanaged(ForeignKeyIntegrityWorkClaim),
    ) !void {
        for (planned_units, 0..) |unit, i| {
            if (foreignKeyIntegrityPlannedUnitsContainGroupBefore(planned_units, i)) continue;
            var one = (try runProvisionedForeignKeyIntegrityGroupLocal(
                self,
                alloc,
                unit.group_id,
                table_name,
                .progress,
                null,
                "",
                "",
                0,
            )) orelse return error.UnknownGroup;
            defer one.deinit(alloc);
            var jobs = std.ArrayListUnmanaged(ForeignKeyIntegrityJobStatus).empty;
            defer {
                for (jobs.items) |*job| job.deinit(alloc);
                jobs.deinit(alloc);
            }
            try appendForeignKeyIntegrityProgressAndClaims(alloc, one, progress_entries, work_claims, &jobs);
        }
    }

    fn runProvisionedForeignKeyIntegrityWorkerPass(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        action: ForeignKeyIntegrityAction,
        job_id: ?[]const u8,
        worker_id: []const u8,
        lease_ms: u64,
        max_work_units: usize,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?ForeignKeyIntegrityResult {
        if (!foreignKeyIntegrityWorkerActionSupported(action)) return error.InvalidForeignKeyIntegrityRequest;
        if (worker_id.len == 0 or lease_ms == 0) return error.InvalidForeignKeyIntegrityRequest;
        if (job_id) |id| {
            if (id.len == 0) return error.InvalidForeignKeyIntegrityRequest;
        }

        const planned_units = try planForeignKeyIntegrityWorkerWorkUnits(alloc, self.catalog, table_name, action, constraint_name, lower_doc_key, upper_doc_key);
        defer {
            for (planned_units) |*unit| unit.deinit(alloc);
            if (planned_units.len > 0) alloc.free(planned_units);
        }
        if (planned_units.len == 0) return null;
        try recordProvisionedForeignKeyIntegrityJobStart(self, alloc, table_name, planned_units, job_id, action, worker_id, constraint_name, lower_doc_key, upper_doc_key, lease_ms, max_work_units);

        var initial_progress = std.ArrayListUnmanaged(ForeignKeyIntegrityProgress).empty;
        var initial_claims = std.ArrayListUnmanaged(ForeignKeyIntegrityWorkClaim).empty;
        defer {
            for (initial_progress.items) |*progress| progress.deinit(alloc);
            initial_progress.deinit(alloc);
            for (initial_claims.items) |*claim| claim.deinit(alloc);
            initial_claims.deinit(alloc);
        }
        try collectProvisionedForeignKeyIntegrityStatusSnapshot(self, alloc, table_name, planned_units, &initial_progress, &initial_claims);
        const initial_statuses = try buildForeignKeyIntegrityWorkStatuses(alloc, planned_units, initial_progress.items, initial_claims.items, .pending);
        defer {
            for (initial_statuses) |*status| status.deinit(alloc);
            if (initial_statuses.len > 0) alloc.free(initial_statuses);
        }

        var aggregate: db_mod.relational_store.ForeignKeyIntegrityReport = .{};
        var group_reports = std.ArrayListUnmanaged(ForeignKeyIntegrityGroupReport).empty;
        var violations = std.ArrayListUnmanaged(ForeignKeyIntegrityViolation).empty;
        errdefer {
            group_reports.deinit(alloc);
            for (violations.items) |*violation| violation.deinit(alloc);
            violations.deinit(alloc);
        }
        var truncated = false;
        var executed: usize = 0;
        const now_ns = foreignKeyIntegrityNowNs();
        for (planned_units, initial_statuses) |unit, status| {
            if (executed >= max_work_units) break;
            if (!foreignKeyIntegrityWorkStatusClaimable(status, now_ns)) continue;
            var one = (runProvisionedForeignKeyIntegrityClaimedWorkUnitGroupLocal(
                self,
                alloc,
                unit.group_id,
                table_name,
                action,
                unit.phase,
                null,
                status.claim_key,
                worker_id,
                lease_ms,
                1,
                unit.constraint_name,
                unit.lower_doc_key,
                unit.upper_doc_key,
                violation_limit -| violations.items.len,
            ) catch |err| switch (err) {
                error.ForeignKeyIntegrityClaimBusy => continue,
                else => return err,
            }) orelse return error.UnknownGroup;
            defer one.deinit(alloc);
            try appendForeignKeyIntegrityExecutedResult(alloc, one, violation_limit, &aggregate, &group_reports, &violations, &truncated);
            executed += 1;
        }

        var final_progress = std.ArrayListUnmanaged(ForeignKeyIntegrityProgress).empty;
        var final_claims = std.ArrayListUnmanaged(ForeignKeyIntegrityWorkClaim).empty;
        errdefer {
            for (final_progress.items) |*progress| progress.deinit(alloc);
            final_progress.deinit(alloc);
            for (final_claims.items) |*claim| claim.deinit(alloc);
            final_claims.deinit(alloc);
        }
        try collectProvisionedForeignKeyIntegrityStatusSnapshot(self, alloc, table_name, planned_units, &final_progress, &final_claims);
        const work_statuses = try buildForeignKeyIntegrityWorkStatuses(alloc, planned_units, final_progress.items, final_claims.items, .pending);
        errdefer {
            for (work_statuses) |*status| status.deinit(alloc);
            if (work_statuses.len > 0) alloc.free(work_statuses);
        }
        const work_units = try cloneForeignKeyIntegrityWorkUnits(alloc, planned_units);
        errdefer {
            for (work_units) |*unit| unit.deinit(alloc);
            if (work_units.len > 0) alloc.free(work_units);
        }

        const valid = foreignKeyIntegrityWorkStatusesValid(work_statuses);
        const complete = !foreignKeyIntegrityWorkStatusesHaveClaimable(work_statuses, foreignKeyIntegrityNowNs());
        var result: ForeignKeyIntegrityResult = .{
            .action = action,
            .valid = valid,
            .complete = complete,
            .violation_limit = violation_limit,
            .violations_truncated = truncated,
            .report = aggregate,
            .groups = try group_reports.toOwnedSlice(alloc),
            .progress = try final_progress.toOwnedSlice(alloc),
            .work_units = work_units,
            .work_claims = try final_claims.toOwnedSlice(alloc),
            .work_statuses = work_statuses,
            .violations = try violations.toOwnedSlice(alloc),
        };
        errdefer {
            var cleanup = result;
            cleanup.deinit(alloc);
        }
        try recordProvisionedForeignKeyIntegrityJobFinish(self, alloc, table_name, planned_units, job_id, result);
        try attachForeignKeyIntegrityJobId(alloc, &result, job_id);
        return result;
    }

    fn foreignKeyIntegrityGroupSeenBefore(planned_units: []const ForeignKeyIntegrityWorkUnit, index: usize, group_id: u64) bool {
        for (planned_units[0..index]) |unit| {
            if (unit.group_id == group_id) return true;
        }
        return false;
    }

    fn recordProvisionedForeignKeyIntegrityJobStart(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        planned_units: []const ForeignKeyIntegrityWorkUnit,
        job_id: ?[]const u8,
        action: ForeignKeyIntegrityAction,
        worker_id: []const u8,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        lease_ms: u64,
        max_work_units: usize,
    ) !void {
        if (job_id == null) return;
        for (planned_units, 0..) |unit, index| {
            if (foreignKeyIntegrityGroupSeenBefore(planned_units, index, unit.group_id)) continue;
            try self.recordProvisionedForeignKeyIntegrityJobOnGroup(
                alloc,
                table_name,
                unit.group_id,
                job_id,
                action,
                worker_id,
                constraint_name,
                lower_doc_key,
                upper_doc_key,
                lease_ms,
                max_work_units,
                null,
            );
        }
    }

    fn recordProvisionedForeignKeyIntegrityJobFinish(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        planned_units: []const ForeignKeyIntegrityWorkUnit,
        job_id: ?[]const u8,
        result: ForeignKeyIntegrityResult,
    ) !void {
        if (job_id == null or !result.complete) return;
        for (planned_units, 0..) |unit, index| {
            if (foreignKeyIntegrityGroupSeenBefore(planned_units, index, unit.group_id)) continue;
            try self.recordProvisionedForeignKeyIntegrityJobOnGroup(
                alloc,
                table_name,
                unit.group_id,
                job_id,
                result.action,
                "",
                null,
                "",
                "",
                0,
                0,
                result,
            );
        }
    }

    fn recordProvisionedForeignKeyIntegrityJobOnGroup(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_id: u64,
        job_id: ?[]const u8,
        action: ForeignKeyIntegrityAction,
        worker_id: []const u8,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        lease_ms: u64,
        max_work_units: usize,
        complete_result: ?ForeignKeyIntegrityResult,
    ) !void {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);

        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);

        if (self.write_cache) |cache| {
            var cached = try self.getOrOpenCachedDbMode(alloc, cache, path, group_id, table_name, .default, null, null);
            defer cached.deinit(alloc);
            if (complete_result) |result| {
                try finishForeignKeyIntegrityJobOnDb(alloc, cached.db, job_id, result);
            } else {
                try startForeignKeyIntegrityJobOnDb(cached.db, job_id, table_name, action, worker_id, constraint_name, lower_doc_key, upper_doc_key, lease_ms, max_work_units);
            }
            try drainManagedDbBeforeClose(cached.db);
            lockAtomic(&self.local_db_mutex);
            self.markWriteCacheDirty(table_name);
            self.invalidateReadCache(table_name);
            self.local_db_mutex.unlock();
            self.publishDirtyWriteCacheRuntimeStatusesBestEffort(alloc, table_name);
            return;
        }

        var db = try openManagedDbForTableGroupWithRuntime(alloc, path, self.catalog, table_name, group_id, self.backend_runtime);
        defer db.close();
        try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group_id, &db);
        if (complete_result) |result| {
            try finishForeignKeyIntegrityJobOnDb(alloc, &db, job_id, result);
        } else {
            try startForeignKeyIntegrityJobOnDb(&db, job_id, table_name, action, worker_id, constraint_name, lower_doc_key, upper_doc_key, lease_ms, max_work_units);
        }
        self.finishTransientManagedDbWriteBeforeClose(table_name, group_id, &db);
    }

    fn runProvisionedForeignKeyIntegrityClaimedWorkUnitGroupLocal(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        action: ForeignKeyIntegrityAction,
        phase: []const u8,
        job_id: ?[]const u8,
        claim_key: []const u8,
        worker_id: []const u8,
        lease_ms: u64,
        max_work_units: usize,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?ForeignKeyIntegrityResult {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);

        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);

        if (self.write_cache) |cache| {
            var cached = try self.getOrOpenCachedDbMode(alloc, cache, path, group_id, table_name, .default, null, null);
            defer cached.deinit(alloc);
            try startForeignKeyIntegrityJobOnDb(cached.db, job_id, table_name, action, worker_id, constraint_name, lower_doc_key, upper_doc_key, lease_ms, max_work_units);
            var result = try runForeignKeyIntegrityClaimedWorkUnitOnDb(
                alloc,
                cached.db,
                group_id,
                action,
                phase,
                claim_key,
                worker_id,
                lease_ms,
                constraint_name,
                lower_doc_key,
                upper_doc_key,
                violation_limit,
            );
            errdefer result.deinit(alloc);
            try finishForeignKeyIntegrityJobOnDb(alloc, cached.db, job_id, result);
            try attachForeignKeyIntegrityJobId(alloc, &result, job_id);
            try hydrateForeignKeyIntegrityResultDiagnosticsFromJobOnDb(alloc, cached.db, group_id, &result);
            try drainManagedDbBeforeClose(cached.db);
            lockAtomic(&self.local_db_mutex);
            self.markWriteCacheDirty(table_name);
            self.invalidateReadCache(table_name);
            self.local_db_mutex.unlock();
            self.publishDirtyWriteCacheRuntimeStatusesBestEffort(alloc, table_name);
            if (action == .repair) self.notifyLocalChange(table_name, .data);
            return result;
        }

        var db = try openManagedDbForTableGroupWithRuntime(alloc, path, self.catalog, table_name, group_id, self.backend_runtime);
        defer db.close();
        try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group_id, &db);
        try startForeignKeyIntegrityJobOnDb(&db, job_id, table_name, action, worker_id, constraint_name, lower_doc_key, upper_doc_key, lease_ms, max_work_units);
        var result = try runForeignKeyIntegrityClaimedWorkUnitOnDb(
            alloc,
            &db,
            group_id,
            action,
            phase,
            claim_key,
            worker_id,
            lease_ms,
            constraint_name,
            lower_doc_key,
            upper_doc_key,
            violation_limit,
        );
        errdefer result.deinit(alloc);
        try finishForeignKeyIntegrityJobOnDb(alloc, &db, job_id, result);
        try attachForeignKeyIntegrityJobId(alloc, &result, job_id);
        try hydrateForeignKeyIntegrityResultDiagnosticsFromJobOnDb(alloc, &db, group_id, &result);
        self.finishTransientManagedDbWriteBeforeClose(table_name, group_id, &db);
        if (action == .repair) self.notifyLocalChange(table_name, .data);
        return result;
    }

    fn runProvisionedUniqueConstraintIntegrityGroupLocal(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        action: UniqueConstraintIntegrityAction,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
    ) !?UniqueConstraintIntegrityResult {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);

        const mode: ManagedDbOpenMode = switch (action) {
            .progress => .status_only,
            .validate, .dry_run, .repair => .default,
        };

        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);

        if (self.write_cache) |cache| {
            var cached = try self.getOrOpenCachedDbMode(alloc, cache, path, group_id, table_name, mode, null, null);
            defer cached.deinit(alloc);
            var result = try runUniqueConstraintIntegrityOnDb(alloc, cached.db, group_id, action, lower_doc_key, upper_doc_key);
            errdefer result.deinit(alloc);
            if (action == .validate or action == .dry_run or action == .repair) {
                try drainManagedDbBeforeClose(cached.db);
                lockAtomic(&self.local_db_mutex);
                self.markWriteCacheDirty(table_name);
                self.invalidateReadCache(table_name);
                self.local_db_mutex.unlock();
                self.publishDirtyWriteCacheRuntimeStatusesBestEffort(alloc, table_name);
                self.notifyLocalChange(table_name, .data);
            }
            return result;
        }

        var db = try openManagedDbForTableGroupWithRuntime(alloc, path, self.catalog, table_name, group_id, self.backend_runtime);
        defer db.close();
        try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group_id, &db);
        var result = try runUniqueConstraintIntegrityOnDb(alloc, &db, group_id, action, lower_doc_key, upper_doc_key);
        errdefer result.deinit(alloc);
        if (action == .validate or action == .dry_run or action == .repair) {
            self.finishTransientManagedDbWriteBeforeClose(table_name, group_id, &db);
            self.notifyLocalChange(table_name, .data);
        }
        return result;
    }

    fn runProvisionedSecondaryIndexRebuildGroupLocal(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        record: metadata_table_manager.SecondaryIndexRebuildRangeRecord,
        worker_id: []const u8,
        lease_ms: u64,
    ) !?SecondaryIndexRebuildWorkerResult {
        if (record.group_id != group_id) return error.InvalidSecondaryIndexRebuildRequest;
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const now_ms = platform_time.monotonicNs() / std.time.ns_per_ms;

        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);

        if (self.write_cache) |cache| {
            var cached = try self.getOrOpenCachedDbMode(alloc, cache, path, group_id, table_name, .default, null, null);
            defer cached.deinit(alloc);
            const result = try runSecondaryIndexRebuildRangeGroupLocal(cached.db, self.catalog, record, worker_id, now_ms, lease_ms);
            if (result.claimed) {
                try drainManagedDbBeforeClose(cached.db);
                lockAtomic(&self.local_db_mutex);
                self.markWriteCacheDirty(table_name);
                self.invalidateReadCache(table_name);
                self.local_db_mutex.unlock();
                self.publishDirtyWriteCacheRuntimeStatusesBestEffort(alloc, table_name);
                self.notifyLocalChange(table_name, .data);
            }
            return result;
        }

        var db = try openManagedDbForTableGroupWithRuntime(alloc, path, self.catalog, table_name, group_id, self.backend_runtime);
        defer db.close();
        try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group_id, &db);
        const result = try runSecondaryIndexRebuildRangeGroupLocal(&db, self.catalog, record, worker_id, now_ms, lease_ms);
        if (result.claimed) {
            self.finishTransientManagedDbWriteBeforeClose(table_name, group_id, &db);
            self.notifyLocalChange(table_name, .data);
        }
        return result;
    }

    fn runProvisionedSchemaRewriteGroupLocal(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        record: metadata_table_manager.SchemaRewriteJobRecord,
        worker_id: []const u8,
        lease_ms: u64,
    ) !?SchemaRewriteWorkerResult {
        if (record.group_id != group_id) return error.InvalidSchemaRewriteJobRange;
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const now_ms = platform_time.monotonicNs() / std.time.ns_per_ms;

        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);

        if (self.write_cache) |cache| {
            var cached = try self.getOrOpenCachedDbMode(alloc, cache, path, group_id, table_name, .default, null, null);
            defer cached.deinit(alloc);
            const result = try runSchemaRewriteJobGroupLocal(alloc, cached.db, self.catalog, record, worker_id, now_ms, lease_ms);
            if (result.claimed) {
                try drainManagedDbBeforeClose(cached.db);
                lockAtomic(&self.local_db_mutex);
                self.markWriteCacheDirty(table_name);
                self.invalidateReadCache(table_name);
                self.local_db_mutex.unlock();
                self.publishDirtyWriteCacheRuntimeStatusesBestEffort(alloc, table_name);
                self.notifyLocalChange(table_name, .data);
            }
            return result;
        }

        var db = try openManagedDbForTableGroupWithRuntime(alloc, path, self.catalog, table_name, group_id, self.backend_runtime);
        defer db.close();
        try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group_id, &db);
        const result = try runSchemaRewriteJobGroupLocal(alloc, &db, self.catalog, record, worker_id, now_ms, lease_ms);
        if (result.claimed) {
            self.finishTransientManagedDbWriteBeforeClose(table_name, group_id, &db);
            self.notifyLocalChange(table_name, .data);
        }
        return result;
    }

    fn collectRuntimeStatusLeasesFromWriteCacheLocked(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        out: *std.ArrayListUnmanaged(ProvisionedTableWriteCache.CachedDb),
    ) !bool {
        const cache = self.write_cache orelse return false;
        out.clearRetainingCapacity();

        if (self.isWriteCacheDirtyForTable(table_name)) {
            _ = try cache.finishExpiredAutoBulkIngestLocked(platform_time.monotonicNs());
        }

        var leased_retirements: usize = 0;
        for (cache.entries.items) |entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.lsm_root_generation == self.visibleRootGeneration(entry.group_id)) continue;
            if (entry.active_leases > 0) leased_retirements += 1;
        }
        try cache.retired_entries.ensureUnusedCapacity(cache.alloc, leased_retirements);

        var i: usize = 0;
        while (i < cache.entries.items.len) {
            const entry = cache.entries.items[i];
            if (!std.mem.eql(u8, entry.table_name, table_name)) {
                i += 1;
                continue;
            }
            if (entry.lsm_root_generation == self.visibleRootGeneration(entry.group_id)) {
                i += 1;
                continue;
            }
            _ = cache.entries.orderedRemove(i);
            cache.retireEntryLocked(entry);
        }

        var matching_entries: usize = 0;
        for (cache.entries.items) |entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.lsm_root_generation != self.visibleRootGeneration(entry.group_id)) continue;
            if (entry.bulk_ingest_session_open or entry.auto_bulk_ingest_session_open) continue;
            matching_entries += 1;
        }
        try out.ensureTotalCapacity(alloc, matching_entries);

        for (cache.entries.items) |entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.lsm_root_generation != self.visibleRootGeneration(entry.group_id)) continue;
            if (entry.bulk_ingest_session_open or entry.auto_bulk_ingest_session_open) continue;
            lockAtomic(&cache.entry_lifecycle_mutex);
            entry.active_leases += 1;
            cache.entry_lifecycle_mutex.unlock();
            out.appendAssumeCapacity(.{
                .cache = cache,
                .entry = entry,
                .db = &entry.db,
                .schema_json = entry.schema_json,
            });
        }
        return out.items.len != 0;
    }

    fn runtimeStatusesFromCachedDbLeasesBestEffort(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        leases: []const ProvisionedTableWriteCache.CachedDb,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        const snapshot_cache = self.runtime_status_cache orelse return null;
        if (self.isWriteCacheDirtyForTable(table_name)) {
            if (self.write_cache) |cache| {
                lockAtomic(&self.local_db_mutex);
                defer self.local_db_mutex.unlock();
                _ = try cache.finishExpiredAutoBulkIngestLocked(platform_time.monotonicNs());
            }
        }
        var items = std.ArrayListUnmanaged(runtime_status.LocalTableRuntimeStatus).empty;
        errdefer {
            for (items.items) |*item| item.deinit(alloc);
            items.deinit(alloc);
        }

        try items.ensureTotalCapacity(alloc, leases.len);
        for (leases) |lease| {
            const group_id = lease.entry.?.group_id;
            try publishRuntimeStatusSnapshotConsistent(self, alloc, table_name, group_id, lease.db);
            var owned_status = (try snapshot_cache.snapshotGroupStatus(alloc, table_name, group_id)) orelse continue;
            errdefer owned_status.deinit(alloc);
            try self.overlayReadCacheIndexVisibilityBestEffort(alloc, table_name, group_id, &owned_status);
            items.appendAssumeCapacity(owned_status);
        }
        if (items.items.len == 0) {
            items.deinit(alloc);
            return try snapshot_cache.snapshot(alloc, table_name);
        }
        return .{ .items = try items.toOwnedSlice(alloc) };
    }

    fn overlayReadCacheHbcStatsBestEffort(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_id: u64,
        status: *runtime_status.LocalTableRuntimeStatus,
    ) !void {
        const read_cache = self.read_cache orelse return;
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);

        var read_lease = read_cache.getOrOpen(
            path,
            self.catalog,
            group_id,
            self.visibleRootGeneration(group_id),
            table_name,
        ) catch return;
        defer read_lease.release();

        overlayDenseHbcCacheStatsFromDb(&status.stats, read_lease.db);
        if (self.runtime_status_cache) |snapshot_cache| {
            try snapshot_cache.upsertGroupStatus(table_name, status.*);
        }
    }

    pub fn overlayReadCacheIndexVisibilityBestEffort(
        self: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_id: u64,
        status: *runtime_status.LocalTableRuntimeStatus,
    ) !void {
        if (self.isWriteCacheDirtyForTable(table_name)) return;
        if (self.hasActiveBulkIngestSessionForTableBestEffort(table_name)) return;

        const read_cache = self.read_cache orelse return;
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);

        var read_lease = read_cache.getOrOpen(
            path,
            self.catalog,
            group_id,
            self.visibleRootGeneration(group_id),
            table_name,
        ) catch return;
        defer read_lease.release();

        const visible_stats = try read_lease.db.stats(alloc);
        defer db_mod.types.freeDBStats(alloc, visible_stats);

        for (status.stats.indexes) |*item| {
            const visible = for (visible_stats.indexes) |candidate| {
                if (std.mem.eql(u8, candidate.name, item.name)) break candidate;
            } else continue;

            item.doc_count = visible.doc_count;
            item.term_count = visible.term_count;
            item.edge_count = visible.edge_count;
            item.node_count = visible.node_count;
            item.hbc_cache = visible.hbc_cache;
        }

        if (self.runtime_status_cache) |snapshot_cache| {
            try snapshot_cache.upsertGroupStatus(table_name, status.*);
        }
    }

    fn corruptEmbeddingArtifact(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        index_name: []const u8,
    ) !?void {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        try enforceHAWriteGateOptional(self.ha_write_gate);
        self.beginTableRequest(table_name);
        defer self.endTableRequest(table_name);
        lockAtomic(&self.local_db_mutex);
        self.invalidateReadCache(table_name);
        self.markWriteCacheDirty(table_name);
        self.local_db_mutex.unlock();
        const group_ids = try table_catalog.resolveGroupsForSpanEventually(
            alloc,
            self.catalog,
            table_name,
            "",
            "",
            5 * std.time.ns_per_s,
            10,
        );
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;

        for (group_ids) |group_id| {
            self.beginGroupOperation(table_name, group_id);
            {
                defer self.endGroupOperation(table_name, group_id);
                const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
                defer alloc.free(path);

                if (self.write_cache) |cache| {
                    var cached = try self.getOrOpenCachedDbMode(alloc, cache, path, group_id, table_name, .default, null, null);
                    defer cached.deinit(alloc);
                    if (try corruptEmbeddingArtifactInDb(alloc, cached.db, doc_key, index_name)) {
                        lockAtomic(&self.local_db_mutex);
                        self.invalidateReadCache(table_name);
                        self.markWriteCacheDirty(table_name);
                        self.local_db_mutex.unlock();
                        self.notifyLocalChange(table_name, .data);
                        return;
                    }
                } else {
                    var db = try openManagedDbForTableGroupWithRuntimeAndHAWriteGate(alloc, path, self.catalog, table_name, group_id, self.backend_runtime, self.ha_write_gate, self.ha_async_mirror);
                    defer db.close();
                    try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group_id, &db);
                    if (try corruptEmbeddingArtifactInDb(alloc, &db, doc_key, index_name)) {
                        lockAtomic(&self.local_db_mutex);
                        self.invalidateReadCache(table_name);
                        self.markWriteCacheDirty(table_name);
                        self.local_db_mutex.unlock();
                        self.notifyLocalChange(table_name, .data);
                        return;
                    }
                }
            }
        }

        return error.NotFound;
    }

    fn reprocessDocumentArtifact(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
    ) !?bool {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const group_id = (try table_catalog.resolveGroupForKey(alloc, self.catalog, table_name, doc_key)) orelse return null;
        return try reprocessDocumentArtifactGroupLocal(ptr, alloc, group_id, table_name, doc_key, artifact_name);
    }

    fn reprocessDocumentArtifactRange(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        artifact_name: []const u8,
        req: db_mod.types.DocumentArtifactTableReprocessRequest,
    ) !?db_mod.types.DocumentArtifactTableReprocessResult {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        var result = db_mod.types.DocumentArtifactTableReprocessResult{};
        errdefer result.deinit(alloc);
        var failures = std.ArrayListUnmanaged(db_mod.types.DocumentArtifactReprocessFailure).empty;
        var shard_cursors = std.ArrayListUnmanaged(db_mod.types.DocumentArtifactReprocessShardCursor).empty;
        errdefer {
            for (failures.items) |*failure| failure.deinit(alloc);
            failures.deinit(alloc);
            for (shard_cursors.items) |*cursor| cursor.deinit(alloc);
            shard_cursors.deinit(alloc);
        }

        var handled_any = false;
        if (req.shard_cursors.len > 0) {
            for (req.shard_cursors) |cursor| {
                const group_id = cursor.group_id orelse return error.InvalidArgument;
                const group_req = documentArtifactReprocessRequestForCursor(req, cursor);
                var group_result = (try reprocessDocumentArtifactRangeGroupLocal(ptr, alloc, group_id, table_name, artifact_name, group_req)) orelse continue;
                defer group_result.deinit(alloc);
                handled_any = true;
                try mergeDocumentArtifactTableReprocessResult(alloc, &result, &failures, &shard_cursors, group_id, group_result);
            }
        } else {
            const group_ids = try table_catalog.resolveGroupsForSpanEventually(
                alloc,
                self.catalog,
                table_name,
                req.from_key,
                req.to_key,
                5 * std.time.ns_per_s,
                10,
            );
            defer alloc.free(group_ids);
            if (group_ids.len == 0) return null;

            for (group_ids) |group_id| {
                var group_result = (try reprocessDocumentArtifactRangeGroupLocal(ptr, alloc, group_id, table_name, artifact_name, req)) orelse continue;
                defer group_result.deinit(alloc);
                handled_any = true;
                try mergeDocumentArtifactTableReprocessResult(alloc, &result, &failures, &shard_cursors, group_id, group_result);
            }
        }
        if (!handled_any) return null;
        result.failures = try failures.toOwnedSlice(alloc);
        result.shard_cursors = try shard_cursors.toOwnedSlice(alloc);
        return result;
    }

    fn updateDocumentArtifactChildRangePlacement(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        update: db_mod.types.DocumentArtifactChildRangePlacementUpdate,
    ) !?bool {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const group_id = (try table_catalog.resolveGroupForKey(alloc, self.catalog, table_name, doc_key)) orelse return null;
        return try updateDocumentArtifactChildRangePlacementGroupLocal(ptr, alloc, group_id, table_name, doc_key, artifact_name, update);
    }

    fn applyDocumentArtifactChildRangeBatch(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        child_batch: db_mod.DocumentArtifactChildRangeApplyBatch,
    ) !?u64 {
        return try applyDocumentArtifactChildRangeBatchGroupLocal(ptr, alloc, group_id, table_name, doc_key, artifact_name, child_batch);
    }

    fn reprocessDocumentArtifactGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
    ) !?bool {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        try enforceHAWriteGateOptional(self.ha_write_gate);
        self.beginTableRequest(table_name);
        defer self.endTableRequest(table_name);
        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);

        lockAtomic(&self.local_db_mutex);
        self.invalidateReadCache(table_name);
        self.markWriteCacheDirty(table_name);
        self.local_db_mutex.unlock();

        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const handled = if (self.write_cache) |cache| blk: {
            var cached = try self.getOrOpenCachedDbMode(alloc, cache, path, group_id, table_name, .default, null, null);
            defer cached.deinit(alloc);
            break :blk try cached.db.reprocessDocumentArtifact(alloc, doc_key, artifact_name);
        } else blk: {
            const indexes_json = try loadTableIndexesJson(alloc, self.catalog, table_name);
            defer if (indexes_json) |value| alloc.free(value);
            var db = if (indexes_json) |value|
                try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentity(
                    alloc,
                    path,
                    value,
                    null,
                    null,
                    self.visibleRootGeneration(group_id),
                    null,
                    .default,
                    self.backend_runtime,
                    self.antfly_provider,
                    self.secret_store,
                    self.remote_content,
                    try loadTableIdentityNamespaceForGroup(alloc, self.catalog, table_name, group_id),
                )
            else
                try openManagedDbForTableGroupWithRuntimeAndHAWriteGate(alloc, path, self.catalog, table_name, group_id, self.backend_runtime, self.ha_write_gate, self.ha_async_mirror);
            defer db.close();
            try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group_id, &db);
            const result = try db.reprocessDocumentArtifact(alloc, doc_key, artifact_name);
            self.finishTransientManagedDbWriteBeforeClose(table_name, group_id, &db);
            break :blk result;
        };
        if (handled) {
            lockAtomic(&self.local_db_mutex);
            self.invalidateReadCache(table_name);
            self.markWriteCacheDirty(table_name);
            self.local_db_mutex.unlock();
            self.notifyLocalChange(table_name, .data);
        }
        return handled;
    }

    fn updateDocumentArtifactChildRangePlacementGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        update: db_mod.types.DocumentArtifactChildRangePlacementUpdate,
    ) !?bool {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        try enforceHAWriteGateOptional(self.ha_write_gate);
        self.beginTableRequest(table_name);
        defer self.endTableRequest(table_name);
        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);

        lockAtomic(&self.local_db_mutex);
        self.invalidateReadCache(table_name);
        self.markWriteCacheDirty(table_name);
        self.local_db_mutex.unlock();

        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const handled = if (self.write_cache) |cache| blk: {
            var cached = try self.getOrOpenCachedDbMode(alloc, cache, path, group_id, table_name, .default, null, null);
            defer cached.deinit(alloc);
            break :blk try cached.db.updateDocumentArtifactChildRangePlacement(alloc, doc_key, artifact_name, update);
        } else blk: {
            const indexes_json = try loadTableIndexesJson(alloc, self.catalog, table_name);
            defer if (indexes_json) |value| alloc.free(value);
            var db = if (indexes_json) |value|
                try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentity(
                    alloc,
                    path,
                    value,
                    null,
                    null,
                    self.visibleRootGeneration(group_id),
                    null,
                    .default,
                    self.backend_runtime,
                    self.antfly_provider,
                    self.secret_store,
                    self.remote_content,
                    try loadTableIdentityNamespaceForGroup(alloc, self.catalog, table_name, group_id),
                )
            else
                try openManagedDbForTableGroupWithRuntimeAndHAWriteGate(alloc, path, self.catalog, table_name, group_id, self.backend_runtime, self.ha_write_gate, self.ha_async_mirror);
            defer db.close();
            try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group_id, &db);
            const result = try db.updateDocumentArtifactChildRangePlacement(alloc, doc_key, artifact_name, update);
            self.finishTransientManagedDbWriteBeforeClose(table_name, group_id, &db);
            break :blk result;
        };
        if (handled) {
            lockAtomic(&self.local_db_mutex);
            self.invalidateReadCache(table_name);
            self.markWriteCacheDirty(table_name);
            self.local_db_mutex.unlock();
            self.notifyLocalChange(table_name, .data);
        }
        return handled;
    }

    fn applyDocumentArtifactChildRangeBatchGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        _: []const u8,
        _: []const u8,
        child_batch: db_mod.DocumentArtifactChildRangeApplyBatch,
    ) !?u64 {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        try enforceHAWriteGateOptional(self.ha_write_gate);
        self.beginTableRequest(table_name);
        defer self.endTableRequest(table_name);
        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);

        lockAtomic(&self.local_db_mutex);
        self.invalidateReadCache(table_name);
        self.markWriteCacheDirty(table_name);
        self.local_db_mutex.unlock();

        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const sequence = if (self.write_cache) |cache| blk: {
            var cached = try self.getOrOpenCachedDbMode(alloc, cache, path, group_id, table_name, .default_async, null, null);
            defer cached.deinit(alloc);
            break :blk try cached.db.applyDocumentArtifactChildRangeBatch(child_batch);
        } else blk: {
            const indexes_json = try loadTableIndexesJson(alloc, self.catalog, table_name);
            defer if (indexes_json) |value| alloc.free(value);
            var db = if (indexes_json) |value|
                try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentity(
                    alloc,
                    path,
                    value,
                    null,
                    null,
                    self.visibleRootGeneration(group_id),
                    null,
                    .default_async,
                    self.backend_runtime,
                    self.antfly_provider,
                    self.secret_store,
                    self.remote_content,
                    try loadTableIdentityNamespaceForGroup(alloc, self.catalog, table_name, group_id),
                )
            else
                try openManagedDbForTableGroupWithRuntimeAndHAWriteGate(alloc, path, self.catalog, table_name, group_id, self.backend_runtime, self.ha_write_gate, self.ha_async_mirror);
            defer db.close();
            try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group_id, &db);
            const result = try db.applyDocumentArtifactChildRangeBatch(child_batch);
            self.finishTransientManagedDbWriteBeforeClose(table_name, group_id, &db);
            break :blk result;
        };
        if (sequence > 0) {
            lockAtomic(&self.local_db_mutex);
            self.invalidateReadCache(table_name);
            self.markWriteCacheDirty(table_name);
            self.local_db_mutex.unlock();
            self.notifyLocalChange(table_name, .data);
        }
        return sequence;
    }

    fn reprocessDocumentArtifactRangeGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        artifact_name: []const u8,
        req: db_mod.types.DocumentArtifactTableReprocessRequest,
    ) !?db_mod.types.DocumentArtifactTableReprocessResult {
        const self: *ProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        try enforceHAWriteGateOptional(self.ha_write_gate);
        self.beginTableRequest(table_name);
        defer self.endTableRequest(table_name);
        self.beginGroupOperation(table_name, group_id);
        defer self.endGroupOperation(table_name, group_id);

        lockAtomic(&self.local_db_mutex);
        self.invalidateReadCache(table_name);
        self.markWriteCacheDirty(table_name);
        self.local_db_mutex.unlock();

        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        var result = if (self.write_cache) |cache| blk: {
            var cached = try self.getOrOpenCachedDbMode(alloc, cache, path, group_id, table_name, .default, null, null);
            defer cached.deinit(alloc);
            break :blk try cached.db.reprocessDocumentArtifactRange(alloc, artifact_name, req);
        } else blk: {
            const indexes_json = try loadTableIndexesJson(alloc, self.catalog, table_name);
            defer if (indexes_json) |value| alloc.free(value);
            var db = if (indexes_json) |value|
                try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentity(
                    alloc,
                    path,
                    value,
                    null,
                    null,
                    self.visibleRootGeneration(group_id),
                    null,
                    .default,
                    self.backend_runtime,
                    self.antfly_provider,
                    self.secret_store,
                    self.remote_content,
                    try loadTableIdentityNamespaceForGroup(alloc, self.catalog, table_name, group_id),
                )
            else
                try openManagedDbForTableGroupWithRuntimeAndHAWriteGate(alloc, path, self.catalog, table_name, group_id, self.backend_runtime, self.ha_write_gate, self.ha_async_mirror);
            defer db.close();
            try validateProvisionedDbIdentityNamespace(alloc, self.catalog, table_name, group_id, &db);
            const range_result = try db.reprocessDocumentArtifactRange(alloc, artifact_name, req);
            self.finishTransientManagedDbWriteBeforeClose(table_name, group_id, &db);
            break :blk range_result;
        };
        errdefer result.deinit(alloc);
        if (result.reprocessed > 0 or result.failed > 0) {
            lockAtomic(&self.local_db_mutex);
            self.invalidateReadCache(table_name);
            self.markWriteCacheDirty(table_name);
            self.local_db_mutex.unlock();
            self.notifyLocalChange(table_name, .data);
        }
        return result;
    }
};

fn enforceHAWriteGateOptional(gate: ?db_mod.HAWriteGate) !void {
    const configured = gate orelse return;
    const decision = switch (configured) {
        .primary => |primary| try ha_write_gate_mod.evaluatePrimary(primary, .{}),
        .fenced_primary => |gate_value| try ha_write_gate_mod.evaluateFencedPrimary(gate_value, .{}),
        .standby => |standby| try ha_write_gate_mod.evaluateStandby(standby, .{}),
    };
    switch (decision.action) {
        .allow_write => return,
        .reject_read_only_standby => return error.HAReadOnlyStandby,
        .open_promoted_primary => return error.HAPromotedStandbyRequiresPrimaryOpen,
        .reject_fenced_primary => return error.HAFencedPrimary,
    }
}

pub const HostedProvisionedTableWriteSource = struct {
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    router: table_router.HostedGroupRouter,
    executor: http_common.RequestExecutor,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime = null,
    group_visible_root_generation: ?table_read_core.GroupVisibleRootGenerationSource = null,
    secret_store: ?*common_secrets.FileStore = null,
    remote_content: ?*const scraping.RemoteContentConfig = null,
    foreground_derived_progress: bool = false,

    pub fn init(
        replica_root_dir: []const u8,
        catalog: table_catalog.CatalogSource,
        router: table_router.HostedGroupRouter,
        executor: http_common.RequestExecutor,
    ) HostedProvisionedTableWriteSource {
        return .{
            .replica_root_dir = replica_root_dir,
            .catalog = catalog,
            .router = router,
            .executor = executor,
        };
    }

    pub fn withBackendRuntime(self: *HostedProvisionedTableWriteSource, backend_runtime: *db_mod.background_runtime.BackendRuntime) *HostedProvisionedTableWriteSource {
        self.backend_runtime = backend_runtime;
        return self;
    }

    pub fn withGroupVisibleRootGeneration(
        self: *HostedProvisionedTableWriteSource,
        generation_source: ?table_read_core.GroupVisibleRootGenerationSource,
    ) *HostedProvisionedTableWriteSource {
        self.group_visible_root_generation = generation_source;
        return self;
    }

    pub fn withSecretStore(
        self: *HostedProvisionedTableWriteSource,
        secret_store: ?*common_secrets.FileStore,
    ) *HostedProvisionedTableWriteSource {
        self.secret_store = secret_store;
        if (hostedManagedDbCacheForRootIfPresent(self.replica_root_dir)) |cache| {
            cache.write_cache.secret_store = secret_store;
        }
        return self;
    }

    pub fn withRemoteContent(
        self: *HostedProvisionedTableWriteSource,
        remote_content: ?*const scraping.RemoteContentConfig,
    ) *HostedProvisionedTableWriteSource {
        self.remote_content = remote_content;
        if (hostedManagedDbCacheForRootIfPresent(self.replica_root_dir)) |cache| {
            cache.write_cache.remote_content = remote_content;
        }
        return self;
    }

    pub fn withForegroundDerivedProgress(self: *HostedProvisionedTableWriteSource) *HostedProvisionedTableWriteSource {
        self.foreground_derived_progress = true;
        return self;
    }

    fn shouldDrainAfterBatch(self: *const HostedProvisionedTableWriteSource, sync_level: db_mod.types.SyncLevel) bool {
        return self.foreground_derived_progress or shouldDrainCachedManagedDbAfterBatch(sync_level);
    }

    fn visibleRootGeneration(self: *const HostedProvisionedTableWriteSource, group_id: u64) u64 {
        return if (self.group_visible_root_generation) |generation_source| generation_source.visibleRootGenerationForGroup(group_id) else backend_current_root_generation;
    }

    pub fn invalidateManagedCache(self: *HostedProvisionedTableWriteSource, table_name: []const u8) void {
        const hosted_cache = hostedManagedDbCacheForRootIfPresent(self.replica_root_dir) orelse return;
        lockAtomic(&hosted_cache.mutex);
        defer hosted_cache.mutex.unlock();
        hosted_cache.write_cache.invalidateTable(table_name);
    }

    pub fn getOrOpenCachedDbMode(
        self: *HostedProvisionedTableWriteSource,
        cache: *HostedManagedDbCache,
        path: []const u8,
        group_id: u64,
        table_name: []const u8,
        mode: ManagedDbOpenMode,
    ) !ProvisionedTableWriteCache.CachedDb {
        const lsm_root_generation = self.visibleRootGeneration(group_id);
        if (cache.write_cache.backend_runtime == null) cache.write_cache.backend_runtime = self.backend_runtime;
        cache.write_cache.secret_store = self.secret_store;
        cache.write_cache.remote_content = self.remote_content;
        const identity_namespace = try loadTableIdentityNamespaceForGroup(cache.write_cache.alloc, self.catalog, table_name, group_id);
        const expected_identity_namespace = if (mode == .startup_catch_up or mode == .restore_repair)
            null
        else
            identity_namespace;
        if (mode == .status_only) {
            lockAtomic(&cache.mutex);
            defer cache.mutex.unlock();
            const cached = try cache.write_cache.getOrOpenLockedMode(path, self.catalog, group_id, lsm_root_generation, table_name, .status_only);
            try validateProvisionedDbIdentityNamespaceExpected(expected_identity_namespace, cached.db);
            return cached;
        }

        var prepared_open: ?ProvisionedTableWriteCache.PreparedOpen = null;
        defer if (prepared_open) |*prepared| prepared.deinit(cache.write_cache.alloc);

        {
            lockAtomic(&cache.mutex);
            defer cache.mutex.unlock();
            switch (try cache.write_cache.getOrPrepareOpenLocked(group_id, lsm_root_generation, table_name)) {
                .cached => |cached| {
                    try validateProvisionedDbIdentityNamespaceExpected(expected_identity_namespace, cached.db);
                    return cached;
                },
                .prepared => |prepared| prepared_open = prepared,
            }
        }

        lockAtomic(&cache.write_cache.open_mutex);
        defer cache.write_cache.open_mutex.unlock();

        {
            lockAtomic(&cache.mutex);
            defer cache.mutex.unlock();
            switch (try cache.write_cache.getOrPrepareOpenLocked(group_id, lsm_root_generation, table_name)) {
                .cached => |cached| {
                    try validateProvisionedDbIdentityNamespaceExpected(expected_identity_namespace, cached.db);
                    return cached;
                },
                .prepared => |prepared| {
                    prepared_open.?.deinit(cache.write_cache.alloc);
                    prepared_open = prepared;
                },
            }
        }

        if (try loadTableManagedMetadata(cache.write_cache.alloc, self.catalog, table_name)) |metadata| {
            prepared_open.?.indexes_json = metadata.indexes_json;
            prepared_open.?.schema_json = metadata.schema_json;
        }

        const effective_ha_mirror = haMirrorForManagedDbOpenMode(mode, cache.write_cache.ha_async_mirror);
        var opened: ?db_mod.DB = if (prepared_open.?.indexes_json) |value|
            try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentityWithOptions(
                cache.write_cache.alloc,
                path,
                value,
                cache.write_cache.lsm_cache,
                cache.write_cache.hbc_cache,
                lsm_root_generation,
                cache.write_cache.resource_manager,
                mode,
                cache.write_cache.backend_runtime,
                cache.write_cache.antfly_provider,
                cache.write_cache.secret_store,
                cache.write_cache.remote_content,
                identity_namespace,
                .{
                    .drain_resolver_backfill = false,
                    .ha_write_gate = cache.write_cache.ha_write_gate,
                    .ha_async_effect_mirror = effective_ha_mirror,
                    .ha_async_batch_mirror = effective_ha_mirror,
                    .ha_async_metadata_mirror = effective_ha_mirror,
                },
            )
        else
            try db_mod.DB.open(cache.write_cache.alloc, path, .{
                .lsm_cache = cache.write_cache.lsm_cache,
                .hbc_cache = cache.write_cache.hbc_cache,
                .lsm_root_generation = lsm_root_generation,
                .resource_manager = cache.write_cache.resource_manager,
                .backend_runtime = cache.write_cache.backend_runtime,
                .identity_namespace = identity_namespace,
                .prefer_existing_identity_namespace = identity_namespace != null,
                .ha_write_gate = cache.write_cache.ha_write_gate,
                .ha_async_effect_mirror = effective_ha_mirror,
                .ha_async_batch_mirror = effective_ha_mirror,
                .ha_async_metadata_mirror = effective_ha_mirror,
                .open_mode = switch (mode) {
                    .default => .writer,
                    .default_async, .writer_no_replay => .writer_no_replay,
                    .startup_catch_up, .restore_repair => .writer_no_replay,
                    .query_readonly => .query_readonly,
                    .status_only => .status_only,
                },
                .start_index_workers = if (mode == .startup_catch_up) false else true,
                .start_optional_runtimes = mode != .startup_catch_up,
                .ttl_cleanup = if (mode == .startup_catch_up or mode == .restore_repair) .{ .enabled = false } else .{},
                .transaction_recovery = if (mode == .startup_catch_up or mode == .restore_repair) .{ .enabled = false } else .{},
                .text_merge = if (mode == .startup_catch_up or mode == .restore_repair) .{ .enabled = false } else .{},
            });
        defer if (opened) |*db| db.close();
        try validateProvisionedDbIdentityNamespaceExpected(identity_namespace, &opened.?);
        if (prepared_open.?.schema_json) |schema_json| {
            if (schema_json.len > 0) try applyLocalTableSchemaJson(cache.write_cache.alloc, &opened.?, schema_json);
        }

        var cached = blk: {
            lockAtomic(&cache.mutex);
            defer cache.mutex.unlock();
            try cache.write_cache.retired_entries.ensureUnusedCapacity(cache.write_cache.alloc, 1);
            break :blk try cache.write_cache.adoptPreparedOpenLocked(&opened, group_id, lsm_root_generation, table_name, mode, &prepared_open.?);
        };
        errdefer {
            lockAtomic(&cache.mutex);
            defer cache.mutex.unlock();
            cache.write_cache.retireFailedOpenLocked(&cached);
        }
        try cached.db.drainResolverBackfill();
        return cached;
    }

    fn reconcileCachedIndexCreate(
        self: *HostedProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
    ) !void {
        const group_ids = try table_catalog.resolveGroupsForSpanEventually(
            alloc,
            self.catalog,
            table_name,
            "",
            "",
            5 * std.time.ns_per_s,
            10,
        );
        defer alloc.free(group_ids);

        var hosted_cache: ?*HostedManagedDbCache = null;
        for (group_ids) |group_id| {
            const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
            defer alloc.free(path);

            // Public routes can run on nodes with stale on-disk group paths.
            // Reconcile only groups the hosted router says this process actively owns.
            if (self.router.localStatus(group_id) != .active) continue;

            const cache = hosted_cache orelse blk: {
                const cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
                hosted_cache = cache;
                break :blk cache;
            };

            var cached = try self.getOrOpenCachedDbMode(cache, path, group_id, table_name, .default_async);
            defer cached.deinit(cache.write_cache.alloc);

            if (try cached.db.core.indexRequiresEnrichmentReplay(index_name)) {
                _ = try seedManagedIndexReplayFromStoredDocsIfNeeded(alloc, cached.db, index_name);
            }

            if (try cached.db.hasPendingDenseArtifactRebuild(alloc)) {
                _ = try cached.db.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc);
            }
            try drainManagedDbBeforeClose(cached.db);
        }
    }

    pub fn source(self: *HostedProvisionedTableWriteSource) TableWriteSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .create_index = createIndex,
                .create_catalog_index = createCatalogIndexNative,
                .drop_index = dropIndex,
                .drop_catalog_index = dropCatalogIndexNative,
                .commit_transaction = commitTransaction,
                .commit_transaction_with_id = commitTransactionWithId,
                .backup_table = backupTable,
                .backup_catalog_table = HostedProvisionedTableWriteSource.backupCatalogTableNative,
                .backup_table_to_location = backupTableToLocation,
                .restore_table = restoreTable,
                .restore_catalog_table = HostedProvisionedTableWriteSource.restoreCatalogTableNative,
                .batch = batch,
                .batch_catalog = HostedProvisionedTableWriteSource.batchCatalogNative,
                .mutate_rows_from_source = mutateRowsFromSource,
                .mutate_rows_from_source_autocommit = mutateRowsFromSourceAutocommit,
                .mutate_rows_joined_from_source_rows_autocommit = mutateRowsJoinedFromSourceRowsAutocommit,
                .batch_group_local = batchGroupLocal,
                .txn_begin_group_local = txnBeginGroupLocal,
                .txn_prepare_group_local = txnPrepareGroupLocal,
                .txn_resolve_group_local = txnResolveGroupLocal,
                .txn_status_group_local = txnStatusGroupLocal,
                .corrupt_embedding_artifact = corruptEmbeddingArtifact,
                .local_runtime_statuses = localRuntimeStatuses,
                .local_runtime_statuses_catalog = HostedProvisionedTableWriteSource.localRuntimeStatusesCatalogNative,
                .foreign_key_integrity = foreignKeyIntegrity,
                .foreign_key_integrity_worker_pass = foreignKeyIntegrityWorkerPass,
                .foreign_key_integrity_schema_controller_pass = foreignKeyIntegritySchemaControllerPass,
                .foreign_key_integrity_schema_controller_maintenance_pass = foreignKeyIntegritySchemaControllerMaintenancePass,
                .foreign_key_action_job_page = foreignKeyActionJobPage,
                .foreign_key_action_job_schedule = foreignKeyActionJobSchedule,
                .foreign_key_action_job_requeue = foreignKeyActionJobRequeue,
                .foreign_key_action_job_group_local = foreignKeyActionJobGroupLocal,
                .foreign_key_action_job_group_local_schedule = foreignKeyActionJobGroupLocalSchedule,
                .foreign_key_action_job_group_local_requeue = foreignKeyActionJobGroupLocalRequeue,
                .foreign_key_action_job_progress = foreignKeyActionJobProgress,
                .foreign_key_action_job_group_local_progress = foreignKeyActionJobGroupLocalProgress,
                .foreign_key_action_schedule_progress = foreignKeyActionScheduleProgress,
                .foreign_key_action_schedule_group_local_progress = foreignKeyActionScheduleGroupLocalProgress,
                .foreign_key_action_schedule_mark_seeded = foreignKeyActionScheduleMarkSeeded,
                .foreign_key_action_schedule_requeue = foreignKeyActionScheduleRequeue,
                .foreign_key_action_schedule_group_local_mark_seeded = foreignKeyActionScheduleGroupLocalMarkSeeded,
                .foreign_key_action_schedule_group_local_requeue = foreignKeyActionScheduleGroupLocalRequeue,
                .unique_constraint_integrity = uniqueConstraintIntegrity,
                .unique_constraint_integrity_schema_controller_maintenance_pass = uniqueConstraintIntegritySchemaControllerMaintenancePass,
                .secondary_index_rebuild_worker_pass = secondaryIndexRebuildWorkerPass,
                .secondary_index_rebuild_group_local = secondaryIndexRebuildGroupLocal,
                .schema_rewrite_worker_pass = HostedProvisionedTableWriteSource.schemaRewriteWorkerPass,
                .schema_rewrite_group_local = HostedProvisionedTableWriteSource.schemaRewriteGroupLocal,
                .foreign_key_integrity_group_local = foreignKeyIntegrityGroupLocal,
                .foreign_key_integrity_work_unit_group_local = foreignKeyIntegrityWorkUnitGroupLocal,
                .unique_constraint_integrity_group_local = uniqueConstraintIntegrityGroupLocal,
                .foreign_key_ref_children_group_local = foreignKeyRefChildrenGroupLocal,
                .foreign_key_ref_children_page_group_local = foreignKeyRefChildrenPageGroupLocal,
            },
        };
    }

    fn secondaryIndexRebuildWorkerPass(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        worker_id: []const u8,
        lease_ms: u64,
        max_work_units: usize,
    ) !?SecondaryIndexRebuildWorkerPassResult {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try runSecondaryIndexRebuildWorkerPassForCatalog(alloc, self.source(), self.catalog, table_name, worker_id, lease_ms, max_work_units);
    }

    fn secondaryIndexRebuildGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        record: metadata_table_manager.SecondaryIndexRebuildRangeRecord,
        worker_id: []const u8,
        lease_ms: u64,
    ) !?SecondaryIndexRebuildWorkerResult {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        if (record.group_id != group_id) return error.InvalidSecondaryIndexRebuildRequest;
        var resolved_route = try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, .prefer_leader);
        if (resolved_route) |*route| {
            defer route.deinit(alloc);
            switch (route.*) {
                .local => return try runHostedSecondaryIndexRebuildGroupLocal(self, alloc, group_id, table_name, record, worker_id, lease_ms),
                .remote => |remote| {
                    var client = http_client.ApiHttpClient.init(alloc, self.executor);
                    const body = try std.json.Stringify.valueAlloc(alloc, SecondaryIndexRebuildGroupRequest{
                        .record = record,
                        .worker_id = worker_id,
                        .lease_ms = lease_ms,
                    }, .{});
                    defer alloc.free(body);
                    var response = try client.fetchGroupSecondaryIndexRebuild(remote.base_uri, group_id, table_name, body);
                    defer response.deinit(alloc);
                    var parsed = try std.json.parseFromSlice(SecondaryIndexRebuildWorkerResult, alloc, response.body, .{
                        .ignore_unknown_fields = true,
                    });
                    defer parsed.deinit();
                    return parsed.value;
                },
            }
        }
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().access(io_impl.io(), path, .{}) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        return try runHostedSecondaryIndexRebuildGroupLocal(self, alloc, group_id, table_name, record, worker_id, lease_ms);
    }

    fn schemaRewriteWorkerPass(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        worker_id: []const u8,
        lease_ms: u64,
        max_work_units: usize,
    ) !?SchemaRewriteWorkerPassResult {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try runSchemaRewriteWorkerPassForCatalog(alloc, self.source(), self.catalog, table_name, worker_id, lease_ms, max_work_units);
    }

    fn schemaRewriteGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        record: metadata_table_manager.SchemaRewriteJobRecord,
        worker_id: []const u8,
        lease_ms: u64,
    ) !?SchemaRewriteWorkerResult {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        if (record.group_id != group_id) return error.InvalidSchemaRewriteJobRange;
        var resolved_route = try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, .prefer_leader);
        if (resolved_route) |*route| {
            defer route.deinit(alloc);
            switch (route.*) {
                .local => return try runHostedSchemaRewriteGroupLocal(self, alloc, group_id, table_name, record, worker_id, lease_ms),
                .remote => |remote| {
                    var client = http_client.ApiHttpClient.init(alloc, self.executor);
                    const body = try std.json.Stringify.valueAlloc(alloc, SchemaRewriteGroupRequest{
                        .record = record,
                        .worker_id = worker_id,
                        .lease_ms = lease_ms,
                    }, .{});
                    defer alloc.free(body);
                    var response = try client.fetchGroupSchemaRewrite(remote.base_uri, group_id, table_name, body);
                    defer response.deinit(alloc);
                    var parsed = try std.json.parseFromSlice(SchemaRewriteWorkerResult, alloc, response.body, .{
                        .ignore_unknown_fields = true,
                    });
                    defer parsed.deinit();
                    return parsed.value;
                },
            }
        }
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().access(io_impl.io(), path, .{}) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        return try runHostedSchemaRewriteGroupLocal(self, alloc, group_id, table_name, record, worker_id, lease_ms);
    }

    fn foreignKeyIntegrityWorkerPass(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        action: ForeignKeyIntegrityAction,
        job_id: ?[]const u8,
        worker_id: []const u8,
        lease_ms: u64,
        max_work_units: usize,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?ForeignKeyIntegrityResult {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try self.runHostedForeignKeyIntegrityWorkerPass(
            alloc,
            table_name,
            action,
            job_id,
            worker_id,
            lease_ms,
            max_work_units,
            constraint_name,
            lower_doc_key,
            upper_doc_key,
            violation_limit,
        );
    }

    fn foreignKeyIntegritySchemaControllerPass(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        action: ForeignKeyIntegrityAction,
        worker_id: []const u8,
        lease_ms: u64,
        max_work_units: usize,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?ForeignKeyIntegrityResult {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const schema_json = (try loadTableSchemaJson(alloc, self.catalog, table_name)) orelse return null;
        defer alloc.free(schema_json);
        return try foreignKeyIntegritySchemaControllerPassWithSchemaJson(
            alloc,
            self.source(),
            table_name,
            schema_json,
            action,
            worker_id,
            lease_ms,
            max_work_units,
            constraint_name,
            lower_doc_key,
            upper_doc_key,
            violation_limit,
        );
    }

    fn foreignKeyIntegritySchemaControllerMaintenancePass(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        options: ForeignKeyIntegritySchemaControllerOptions,
    ) !?ForeignKeyIntegritySchemaControllerResult {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try runCatalogForeignKeyIntegritySchemaControllerMaintenancePass(alloc, self.source(), self.catalog, options);
    }

    fn uniqueConstraintIntegritySchemaControllerMaintenancePass(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        options: UniqueConstraintIntegritySchemaControllerOptions,
    ) !?UniqueConstraintIntegritySchemaControllerResult {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try runCatalogUniqueConstraintIntegritySchemaControllerMaintenancePass(alloc, self.source(), self.catalog, options);
    }

    fn createIndex(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
        index_json: []const u8,
    ) !?void {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        try table_write_index_config.validateIndexConfig(alloc, index_name, index_json);
        self.invalidateManagedCache(table_name);
        try self.reconcileCachedIndexCreate(alloc, table_name, index_name);
    }

    fn dropIndex(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
    ) !?void {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        self.invalidateManagedCache(table_name);
        try dropLocalTableIndex(alloc, self.catalog, self.replica_root_dir, self.backend_runtime, table_name, index_name);
        self.invalidateManagedCache(table_name);
    }

    fn createCatalogIndexNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        index_name: []const u8,
        index_json: []const u8,
    ) !?void {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const table_name = try nativeCatalogTableNameAlloc(alloc, self.catalog, target);
        defer alloc.free(table_name);
        return try createIndex(ptr, alloc, table_name, index_name, index_json);
    }

    fn dropCatalogIndexNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        index_name: []const u8,
    ) !?void {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const table_name = try nativeCatalogTableNameAlloc(alloc, self.catalog, target);
        defer alloc.free(table_name);
        return try dropIndex(ptr, alloc, table_name, index_name);
    }

    fn batch(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) !?void {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        var grouped = std.ArrayListUnmanaged(GroupBatch).empty;
        defer {
            for (grouped.items) |*group| group.deinit(alloc);
            grouped.deinit(alloc);
        }

        var snapshot = try self.catalog.adminSnapshot();
        defer self.catalog.freeAdminSnapshot(&snapshot);
        const table = tables_api.findTableByName(&snapshot, table_name) orelse return null;
        const ranges = try metadata_admin.listTableRanges(alloc, &snapshot, table.table_id);
        defer metadata_admin.freeRangeRefs(alloc, ranges);
        if (ranges.len == 0) return null;

        for (req.writes) |write| {
            const group_id = table_catalog.resolveGroupForKeyFromRanges(ranges, write.key) orelse return null;
            const group = try ensureGroupBatch(alloc, &grouped, group_id);
            try group.writes.append(alloc, write);
        }
        for (req.deletes) |key| {
            const group_id = table_catalog.resolveGroupForKeyFromRanges(ranges, key) orelse return null;
            const group = try ensureGroupBatch(alloc, &grouped, group_id);
            try group.deletes.append(alloc, key);
        }
        for (req.relational_identity_rewrites) |rewrite| {
            const old_group_id = table_catalog.resolveGroupForKeyFromRanges(ranges, rewrite.old_key) orelse return null;
            const new_group_id = table_catalog.resolveGroupForKeyFromRanges(ranges, rewrite.new_key) orelse return null;
            if (old_group_id != new_group_id) return error.UnsupportedOperation;
            const group = try ensureGroupBatch(alloc, &grouped, old_group_id);
            try group.relational_identity_rewrites.append(alloc, rewrite);
        }
        for (req.transforms) |transform| {
            const group_id = table_catalog.resolveGroupForKeyFromRanges(ranges, transform.key) orelse return null;
            const group = try ensureGroupBatch(alloc, &grouped, group_id);
            try group.transforms.append(alloc, transform);
        }

        for (grouped.items) |group| {
            var resolved_route = try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group.group_id, .prefer_leader);
            if (resolved_route) |*route| {
                defer route.deinit(alloc);

                switch (route.*) {
                    .local => {
                        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group.group_id);
                        defer alloc.free(path);
                        const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
                        var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group.group_id, table_name, .default_async);
                        defer cached.deinit(hosted_cache.write_cache.alloc);
                        try validateTableBatchAgainstSchemaJson(alloc, cached.db, cached.schema_json, group.writes.items, group.deletes.items, group.transforms.items);
                        try cached.db.batch(.{
                            .writes = group.writes.items,
                            .deletes = group.deletes.items,
                            .relational_identity_rewrites = group.relational_identity_rewrites.items,
                            .transforms = group.transforms.items,
                            .graph_writes = req.graph_writes,
                            .graph_deletes = req.graph_deletes,
                            .predicates = req.predicates,
                            .timestamp_ns = req.timestamp_ns,
                            .sync_level = req.sync_level,
                        });
                        if (self.shouldDrainAfterBatch(req.sync_level)) try drainManagedDbBeforeClose(cached.db);
                    },
                    .remote => |remote| {
                        if (group.relational_identity_rewrites.items.len != 0) return error.UnsupportedOperation;
                        var client = http_client.ApiHttpClient.init(alloc, self.executor);
                        const body = try encodeRemoteBatchRequest(alloc, .{
                            .writes = group.writes.items,
                            .deletes = group.deletes.items,
                            .transforms = group.transforms.items,
                            .graph_writes = req.graph_writes,
                            .graph_deletes = req.graph_deletes,
                            .predicates = req.predicates,
                            .timestamp_ns = req.timestamp_ns,
                            .sync_level = req.sync_level,
                        });
                        defer alloc.free(body);
                        var response = try client.fetchGroupBatch(remote.base_uri, group.group_id, table_name, body);
                        response.deinit(alloc);
                    },
                }
            } else {
                const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group.group_id);
                defer alloc.free(path);
                var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
                defer io_impl.deinit();
                std.Io.Dir.cwd().access(io_impl.io(), path, .{}) catch |err| switch (err) {
                    error.FileNotFound => return null,
                    else => return err,
                };
                const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
                var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group.group_id, table_name, .default_async);
                defer cached.deinit(hosted_cache.write_cache.alloc);
                try validateTableBatchAgainstSchemaJson(alloc, cached.db, cached.schema_json, group.writes.items, group.deletes.items, group.transforms.items);
                try cached.db.batch(.{
                    .writes = group.writes.items,
                    .deletes = group.deletes.items,
                    .relational_identity_rewrites = group.relational_identity_rewrites.items,
                    .transforms = group.transforms.items,
                    .graph_writes = req.graph_writes,
                    .graph_deletes = req.graph_deletes,
                    .predicates = req.predicates,
                    .timestamp_ns = req.timestamp_ns,
                    .sync_level = req.sync_level,
                });
                if (self.shouldDrainAfterBatch(req.sync_level)) try drainManagedDbBeforeClose(cached.db);
            }
        }
    }

    fn batchCatalogNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        req: db_mod.types.BatchRequest,
    ) !?void {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const table_name = try nativeCatalogTableNameAlloc(alloc, self.catalog, target);
        defer alloc.free(table_name);
        return try batch(ptr, alloc, table_name, req);
    }

    fn mutateRowsFromSource(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        schema: storage_schema.TableSchema,
        req: db_mod.types.RelationalRowsMutationSourceRequest,
    ) !?db_mod.types.RelationalRowsMutationSourceResult {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        var snapshot = try self.catalog.adminSnapshot();
        defer self.catalog.freeAdminSnapshot(&snapshot);
        const table = tables_api.findTableByName(&snapshot, table_name) orelse return null;
        const ranges = try metadata_admin.listTableRanges(alloc, &snapshot, table.table_id);
        defer metadata_admin.freeRangeRefs(alloc, ranges);
        if (ranges.len != 1) return error.UnsupportedOperation;

        const group_id = ranges[0].group_id;
        var resolved_route = try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, .prefer_leader);
        if (resolved_route) |*route| {
            defer route.deinit(alloc);
            switch (route.*) {
                .local => return try self.mutateRowsFromSourceGroupLocal(alloc, group_id, table_name, schema, req),
                .remote => return error.UnsupportedOperation,
            }
        }

        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().access(io_impl.io(), path, .{}) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        return try self.mutateRowsFromSourceGroupLocal(alloc, group_id, table_name, schema, req);
    }

    fn mutateRowsFromSourceGroupLocal(
        self: *HostedProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        schema: storage_schema.TableSchema,
        req: db_mod.types.RelationalRowsMutationSourceRequest,
    ) !?db_mod.types.RelationalRowsMutationSourceResult {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
        var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group_id, table_name, .default);
        defer cached.deinit(hosted_cache.write_cache.alloc);
        try recoverHostedTransactionsOnce(self, alloc, cached.db);
        return cached.db.mutateRelationalRowsFromSource(alloc, schema, req) catch |err| switch (err) {
            error.TxnNotFound => {
                const claim = req.source.row_claim orelse return err;
                _ = try cached.db.beginTransactionWithIdAndParticipants(claim.txn_id orelse return err, nextTxnTimestamp(), &.{});
                return try cached.db.mutateRelationalRowsFromSource(alloc, schema, req);
            },
            else => return err,
        };
    }

    fn mutateRowsFromSourceAutocommit(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        schema: storage_schema.TableSchema,
        req: db_mod.types.RelationalRowsMutationSourceRequest,
        sync_level: db_mod.types.SyncLevel,
    ) !?db_mod.types.RelationalRowsMutationSourceResult {
        _ = sync_level;
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        var snapshot = try self.catalog.adminSnapshot();
        defer self.catalog.freeAdminSnapshot(&snapshot);
        const table = tables_api.findTableByName(&snapshot, table_name) orelse return null;
        const ranges = try metadata_admin.listTableRanges(alloc, &snapshot, table.table_id);
        defer metadata_admin.freeRangeRefs(alloc, ranges);
        if (ranges.len != 1) return error.UnsupportedOperation;

        const group_id = ranges[0].group_id;
        var resolved_route = try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, .prefer_leader);
        if (resolved_route) |*route| {
            defer route.deinit(alloc);
            switch (route.*) {
                .local => return try self.mutateRowsFromSourceAutocommitGroupLocal(alloc, group_id, table_name, schema, req),
                .remote => return error.UnsupportedOperation,
            }
        }

        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().access(io_impl.io(), path, .{}) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        return try self.mutateRowsFromSourceAutocommitGroupLocal(alloc, group_id, table_name, schema, req);
    }

    fn mutateRowsFromSourceAutocommitGroupLocal(
        self: *HostedProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        schema: storage_schema.TableSchema,
        req: db_mod.types.RelationalRowsMutationSourceRequest,
    ) !?db_mod.types.RelationalRowsMutationSourceResult {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
        var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group_id, table_name, .default);
        defer cached.deinit(hosted_cache.write_cache.alloc);
        try recoverHostedTransactionsOnce(self, alloc, cached.db);
        return try mutateRowsFromSourceAutocommitOnDb(alloc, cached.db, schema, req);
    }

    fn mutateRowsJoinedFromSourceRowsAutocommit(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        target_schema: storage_schema.TableSchema,
        source_schema: storage_schema.TableSchema,
        req: db_mod.types.RelationalRowsJoinedMutationSourceRequest,
        source_rows: []const []const u8,
        sync_level: db_mod.types.SyncLevel,
    ) !?db_mod.types.RelationalRowsMutationSourceResult {
        _ = sync_level;
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        var snapshot = try self.catalog.adminSnapshot();
        defer self.catalog.freeAdminSnapshot(&snapshot);
        const table = tables_api.findTableByName(&snapshot, table_name) orelse return null;
        const ranges = try metadata_admin.listTableRanges(alloc, &snapshot, table.table_id);
        defer metadata_admin.freeRangeRefs(alloc, ranges);
        if (ranges.len != 1) return error.UnsupportedOperation;

        const group_id = ranges[0].group_id;
        var resolved_route = try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, .prefer_leader);
        if (resolved_route) |*route| {
            defer route.deinit(alloc);
            switch (route.*) {
                .local => return try self.mutateRowsJoinedFromSourceRowsAutocommitGroupLocal(alloc, group_id, table_name, target_schema, source_schema, req, source_rows),
                .remote => return error.UnsupportedOperation,
            }
        }

        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().access(io_impl.io(), path, .{}) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        return try self.mutateRowsJoinedFromSourceRowsAutocommitGroupLocal(alloc, group_id, table_name, target_schema, source_schema, req, source_rows);
    }

    fn mutateRowsJoinedFromSourceRowsAutocommitGroupLocal(
        self: *HostedProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        target_schema: storage_schema.TableSchema,
        source_schema: storage_schema.TableSchema,
        req: db_mod.types.RelationalRowsJoinedMutationSourceRequest,
        source_rows: []const []const u8,
    ) !?db_mod.types.RelationalRowsMutationSourceResult {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
        var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group_id, table_name, .default);
        defer cached.deinit(hosted_cache.write_cache.alloc);
        try recoverHostedTransactionsOnce(self, alloc, cached.db);
        return try mutateRowsJoinedFromSourceRowsAutocommitOnDb(alloc, cached.db, target_schema, source_schema, req, source_rows);
    }

    fn commitTransaction(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        tables: []const distributed_txn.TableCommitRequest,
        sync_level: db_mod.types.SyncLevel,
    ) !?distributed_txn.CommitOutcome {
        const txn_id = nextTxnId();
        return try commitTransactionWithId(ptr, alloc, txn_id, nextTxnTimestamp(), tables, sync_level);
    }

    fn commitTransactionWithId(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        begin_timestamp: u64,
        tables: []const distributed_txn.TableCommitRequest,
        _: db_mod.types.SyncLevel,
    ) !?distributed_txn.CommitOutcome {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        var worker_impl = distributed_txn.HostedParticipantWorker.init(self.catalog, self.router, self.source(), self.executor);
        var read_source = table_read_sources.HostedProvisionedTableReadSource.init(
            self.replica_root_dir,
            self.catalog,
            raft_mod.read_gate.noopReadableLeaseRequester(),
            self.router,
            self.executor,
        );
        if (self.backend_runtime) |runtime| _ = read_source.withBackendRuntime(runtime);
        _ = worker_impl.withReads(read_source.source());
        const commit_version = begin_timestamp + 1;
        return try distributed_txn.executeMultiTableCommit(
            alloc,
            self.catalog,
            worker_impl.worker(),
            txn_id,
            begin_timestamp,
            commit_version,
            tables,
            if (comptime build_options.with_tla) tracing.stderrAntflyTraceWriter() else null,
        );
    }

    fn backupTable(
        _: *anyopaque,
        _: std.mem.Allocator,
        _: []const u8,
        _: backups_api.TableBackupPlan,
    ) !?[]backups_api.ShardSnapshot {
        return error.UnsupportedOperation;
    }

    fn backupCatalogTableNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        plan: backups_api.TableBackupPlan,
    ) !?[]backups_api.ShardSnapshot {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const table_name = try nativeCatalogTableNameAlloc(alloc, self.catalog, target);
        defer alloc.free(table_name);
        return try backupTable(ptr, alloc, table_name, plan);
    }

    fn backupTableToLocation(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        backup_id: []const u8,
        format: backups_api.BackupFormat,
        location_uri: []const u8,
        location: *backups_api.BackupLocation,
    ) !?[]backups_api.ShardSnapshot {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const group_id = (try table_catalog.resolveSingleRangeGroup(alloc, self.catalog, table_name)) orelse return null;
        const resolved_route = try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, .prefer_leader);
        var route = resolved_route orelse return null;
        defer route.deinit(alloc);
        switch (route) {
            .local => return error.UnsupportedOperation,
            .remote => |remote| {
                const format_name = switch (format) {
                    .native => "native",
                    .portable => "portable",
                };
                const body = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(.{
                    .backup_id = backup_id,
                    .location = location_uri,
                    .format = format_name,
                }, .{})});
                defer alloc.free(body);

                var client = http_client.ApiHttpClient.init(alloc, self.executor);
                var response = try client.fetchBackupTable(remote.base_uri, table_name, body);
                response.deinit(alloc);

                var manifest = try backups_api.readManifestFromLocation(alloc, location, backup_id);
                defer manifest.deinit(alloc);
                return try cloneShardSnapshots(alloc, manifest.shards);
            },
        }
    }

    fn restoreTable(
        _: *anyopaque,
        _: std.mem.Allocator,
        _: []const u8,
        _: backups_api.TableRestorePlan,
    ) !?void {
        return error.UnsupportedOperation;
    }

    fn restoreCatalogTableNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        plan: backups_api.TableRestorePlan,
    ) !?void {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const table_name = try nativeCatalogTableNameAlloc(alloc, self.catalog, target);
        defer alloc.free(table_name);
        return try restoreTable(ptr, alloc, table_name, plan);
    }

    fn batchGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) !?void {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
        var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group_id, table_name, .default_async);
        defer cached.deinit(hosted_cache.write_cache.alloc);
        try validateTableBatchAgainstSchemaJson(alloc, cached.db, cached.schema_json, req.writes, req.deletes, req.transforms);
        cached.db.batchReplicatedApply(req) catch |err| return normalizeRelationalConstraintError(err);
        if (self.shouldDrainAfterBatch(req.sync_level)) try drainManagedDbBeforeClose(cached.db);
    }

    fn txnBeginGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        begin_timestamp: u64,
        topology_epoch: u64,
        participants: []const []const u8,
    ) !?void {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        try table_catalog.validateGroupTopologyEpoch(alloc, self.catalog, table_name, group_id, topology_epoch);
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
        var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group_id, table_name, .default);
        defer cached.deinit(hosted_cache.write_cache.alloc);
        // Do not run recovery while serving a participant begin RPC. Hosted
        // recovery sends participant RPCs through the same HTTP executor; doing
        // that here can cycle with the coordinator waiting for this response.
        _ = try cached.db.beginTransactionWithIdAndParticipants(txn_id, begin_timestamp, participants);
    }

    fn txnPrepareGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        topology_epoch: u64,
        req: db_mod.types.TransactionIntentRequest,
    ) !?void {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        try table_catalog.validateGroupTopologyEpoch(alloc, self.catalog, table_name, group_id, topology_epoch);
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
        var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group_id, table_name, .default);
        defer cached.deinit(hosted_cache.write_cache.alloc);
        // Participant RPC handlers must not synchronously run hosted recovery:
        // recovery may call back into another participant over HTTP while the
        // coordinator is waiting on this handler.
        try validateTransactionAgainstCatalogSchema(alloc, self.catalog, cached.db, table_name, req.writes, req.deletes, req.transforms);
        cached.db.writeTransaction(txn_id, req) catch |err| return normalizeRelationalConstraintError(err);
    }

    fn txnResolveGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        status: db_mod.types.TxnStatus,
        commit_version: u64,
    ) !?void {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
        var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group_id, table_name, .default);
        defer cached.deinit(hosted_cache.write_cache.alloc);
        cached.db.resolveTransactionIntents(txn_id, status, commit_version) catch |err| {
            return normalizeRelationalConstraintError(err);
        };
        const participant = try std.fmt.allocPrint(alloc, "group:{d}", .{group_id});
        defer alloc.free(participant);
        try cached.db.markTransactionParticipantResolved(txn_id, participant);
        // Keep the resolved participant marker in the same drain window as the
        // committed row/index effects, so the next open does not recover it.
        if (status == .committed) try drainManagedDbBeforeClose(cached.db);
    }

    fn txnStatusGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
    ) !?db_mod.types.TxnStatus {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
        var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group_id, table_name, .default);
        defer cached.deinit(hosted_cache.write_cache.alloc);
        // Status is itself a participant RPC and must stay non-reentrant for
        // the same reason as begin/prepare.
        return try cached.db.getTransactionStatus(txn_id);
    }

    fn localRuntimeStatuses(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        if (hostedManagedDbCacheForRootIfPresent(self.replica_root_dir)) |hosted_cache| {
            lockAtomic(&hosted_cache.mutex);
            defer hosted_cache.mutex.unlock();
            const statuses = try hosted_cache.write_cache.snapshotRuntimeStatusesLocked(alloc, table_name);
            if (statuses) |owned| return owned;
        }
        return try snapshotLocalTableRuntimeStatusesUncached(alloc, self.catalog, self.replica_root_dir, self.backend_runtime, table_name);
    }

    fn localRuntimeStatusesCatalogNative(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const table_name = try nativeCatalogTableNameAlloc(alloc, self.catalog, target);
        defer alloc.free(table_name);
        return try localRuntimeStatuses(ptr, alloc, table_name);
    }

    fn foreignKeyIntegrity(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        action: ForeignKeyIntegrityAction,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?ForeignKeyIntegrityResult {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        if (action == .explain_delete) {
            var worker_impl = distributed_txn.HostedParticipantWorker.init(self.catalog, self.router, self.source(), self.executor);
            var read_source = table_read_sources.HostedProvisionedTableReadSource.init(
                self.replica_root_dir,
                self.catalog,
                raft_mod.read_gate.noopReadableLeaseRequester(),
                self.router,
                self.executor,
            );
            if (self.backend_runtime) |runtime| _ = read_source.withBackendRuntime(runtime);
            _ = worker_impl.withReads(read_source.source());
            if (try distributed_txn.explainRoutedForeignKeyParentDelete(
                alloc,
                self.catalog,
                worker_impl.worker(),
                table_name,
                constraint_name,
                lower_doc_key,
            )) |explain| {
                return try foreignKeyIntegrityResultFromRoutedExplain(alloc, action, violation_limit, explain);
            }
        }
        const group_ids = try resolveForeignKeyIntegrityGroupsEventually(
            alloc,
            self.catalog,
            table_name,
            action,
            lower_doc_key,
            upper_doc_key,
        );
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;

        const planned_units: []ForeignKeyIntegrityWorkUnit = if (action == .explain_delete)
            &.{}
        else
            try planForeignKeyIntegrityWorkUnits(alloc, self.catalog, table_name, action, constraint_name, lower_doc_key, upper_doc_key);
        var planned_units_owned = true;
        errdefer if (planned_units_owned) {
            for (planned_units) |*unit| unit.deinit(alloc);
            if (planned_units.len > 0) alloc.free(planned_units);
        };
        if (action == .plan) {
            const groups = try alloc.alloc(ForeignKeyIntegrityGroupReport, 0);
            errdefer alloc.free(groups);
            const violations_empty = try alloc.alloc(ForeignKeyIntegrityViolation, 0);
            errdefer alloc.free(violations_empty);
            const work_statuses = try buildForeignKeyIntegrityWorkStatuses(alloc, planned_units, &.{}, &.{}, .planned);
            errdefer {
                for (work_statuses) |*status| status.deinit(alloc);
                if (work_statuses.len > 0) alloc.free(work_statuses);
            }
            const result: ForeignKeyIntegrityResult = .{
                .action = action,
                .valid = true,
                .complete = true,
                .violation_limit = violation_limit,
                .violations_truncated = false,
                .report = .{},
                .delete_plan = null,
                .groups = groups,
                .progress = &.{},
                .work_units = planned_units,
                .work_claims = &.{},
                .work_statuses = work_statuses,
                .violations = violations_empty,
            };
            planned_units_owned = false;
            return result;
        }
        planned_units_owned = false;
        defer {
            for (planned_units) |*unit| unit.deinit(alloc);
            if (planned_units.len > 0) alloc.free(planned_units);
        }

        var group_reports = std.ArrayListUnmanaged(ForeignKeyIntegrityGroupReport).empty;
        var progress_entries = std.ArrayListUnmanaged(ForeignKeyIntegrityProgress).empty;
        var work_units = std.ArrayListUnmanaged(ForeignKeyIntegrityWorkUnit).empty;
        var work_claims = std.ArrayListUnmanaged(ForeignKeyIntegrityWorkClaim).empty;
        var jobs = std.ArrayListUnmanaged(ForeignKeyIntegrityJobStatus).empty;
        var violations = std.ArrayListUnmanaged(ForeignKeyIntegrityViolation).empty;
        errdefer {
            group_reports.deinit(alloc);
            for (progress_entries.items) |*progress| progress.deinit(alloc);
            progress_entries.deinit(alloc);
            for (work_units.items) |*unit| unit.deinit(alloc);
            work_units.deinit(alloc);
            for (work_claims.items) |*claim| claim.deinit(alloc);
            work_claims.deinit(alloc);
            for (jobs.items) |*job| job.deinit(alloc);
            jobs.deinit(alloc);
            for (violations.items) |*violation| violation.deinit(alloc);
            violations.deinit(alloc);
        }
        try work_units.ensureUnusedCapacity(alloc, planned_units.len);
        for (planned_units) |unit| {
            work_units.appendAssumeCapacity(try cloneForeignKeyIntegrityWorkUnit(alloc, unit));
        }

        var aggregate: db_mod.relational_store.ForeignKeyIntegrityReport = .{};
        var delete_plan: ?db_mod.relational_store.ForeignKeyDeletePlan = null;
        var truncated = false;
        var valid = true;
        var complete = true;
        if (action == .progress or action == .explain_delete) {
            for (group_ids) |group_id| {
                var resolved_route = try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, .prefer_leader);
                if (resolved_route) |*route| {
                    defer route.deinit(alloc);
                    switch (route.*) {
                        .local => {
                            const maybe_one = runHostedForeignKeyIntegrityGroupLocal(
                                self,
                                alloc,
                                group_id,
                                table_name,
                                action,
                                constraint_name,
                                lower_doc_key,
                                upper_doc_key,
                                violation_limit -| violations.items.len,
                            ) catch |err| switch (err) {
                                error.UnknownGroup => if (action == .progress) continue else return err,
                                else => return err,
                            };
                            var one = maybe_one orelse {
                                if (action == .progress) continue;
                                return error.UnknownGroup;
                            };
                            defer one.deinit(alloc);
                            try appendForeignKeyIntegrityResult(alloc, one, violation_limit, &aggregate, &group_reports, &progress_entries, &work_units, &work_claims, &jobs, &violations, &delete_plan, &truncated, &valid, &complete);
                        },
                        .remote => |remote| {
                            var client = http_client.ApiHttpClient.init(alloc, self.executor);
                            const body = try std.json.Stringify.valueAlloc(alloc, ForeignKeyIntegrityRequest{
                                .action = action,
                                .constraint_name = constraint_name,
                                .doc_key = if (action == .explain_delete) lower_doc_key else null,
                                .lower_doc_key = lower_doc_key,
                                .upper_doc_key = upper_doc_key,
                                .violation_limit = violation_limit -| violations.items.len,
                            }, .{});
                            defer alloc.free(body);
                            var response = try client.fetchGroupForeignKeyIntegrity(remote.base_uri, group_id, table_name, body);
                            defer response.deinit(alloc);
                            var parsed = try std.json.parseFromSlice(ForeignKeyIntegrityResult, alloc, response.body, .{
                                .allocate = .alloc_always,
                                .ignore_unknown_fields = true,
                            });
                            defer parsed.deinit();
                            try appendForeignKeyIntegrityResult(alloc, parsed.value, violation_limit, &aggregate, &group_reports, &progress_entries, &work_units, &work_claims, &jobs, &violations, &delete_plan, &truncated, &valid, &complete);
                        },
                    }
                } else {
                    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
                    defer alloc.free(path);
                    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
                    defer io_impl.deinit();
                    std.Io.Dir.cwd().access(io_impl.io(), path, .{}) catch |err| switch (err) {
                        error.FileNotFound => if (action == .progress) continue else return error.UnknownGroup,
                        else => return err,
                    };
                    const maybe_one = runHostedForeignKeyIntegrityGroupLocal(
                        self,
                        alloc,
                        group_id,
                        table_name,
                        action,
                        constraint_name,
                        lower_doc_key,
                        upper_doc_key,
                        violation_limit -| violations.items.len,
                    ) catch |err| switch (err) {
                        error.UnknownGroup => if (action == .progress) continue else return err,
                        else => return err,
                    };
                    var one = maybe_one orelse {
                        if (action == .progress) continue;
                        return error.UnknownGroup;
                    };
                    defer one.deinit(alloc);
                    try appendForeignKeyIntegrityResult(alloc, one, violation_limit, &aggregate, &group_reports, &progress_entries, &work_units, &work_claims, &jobs, &violations, &delete_plan, &truncated, &valid, &complete);
                }
            }
        } else {
            for (planned_units) |unit| {
                var resolved_route = try table_router.resolveGroupRoute(alloc, self.catalog, self.router, unit.group_id, .prefer_leader);
                if (resolved_route) |*route| {
                    defer route.deinit(alloc);
                    switch (route.*) {
                        .local => {
                            var one = (try runHostedForeignKeyIntegrityGroupLocal(
                                self,
                                alloc,
                                unit.group_id,
                                table_name,
                                action,
                                constraint_name,
                                unit.lower_doc_key,
                                unit.upper_doc_key,
                                violation_limit -| violations.items.len,
                            )) orelse return error.UnknownGroup;
                            defer one.deinit(alloc);
                            try appendForeignKeyIntegrityResult(alloc, one, violation_limit, &aggregate, &group_reports, &progress_entries, &work_units, &work_claims, &jobs, &violations, &delete_plan, &truncated, &valid, &complete);
                        },
                        .remote => |remote| {
                            var client = http_client.ApiHttpClient.init(alloc, self.executor);
                            const body = try std.json.Stringify.valueAlloc(alloc, ForeignKeyIntegrityRequest{
                                .action = action,
                                .constraint_name = constraint_name,
                                .doc_key = if (action == .explain_delete) unit.lower_doc_key else null,
                                .lower_doc_key = unit.lower_doc_key,
                                .upper_doc_key = unit.upper_doc_key,
                                .violation_limit = violation_limit -| violations.items.len,
                            }, .{});
                            defer alloc.free(body);
                            var response = try client.fetchGroupForeignKeyIntegrity(remote.base_uri, unit.group_id, table_name, body);
                            defer response.deinit(alloc);
                            var parsed = try std.json.parseFromSlice(ForeignKeyIntegrityResult, alloc, response.body, .{
                                .allocate = .alloc_always,
                                .ignore_unknown_fields = true,
                            });
                            defer parsed.deinit();
                            try appendForeignKeyIntegrityResult(alloc, parsed.value, violation_limit, &aggregate, &group_reports, &progress_entries, &work_units, &work_claims, &jobs, &violations, &delete_plan, &truncated, &valid, &complete);
                        },
                    }
                } else {
                    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, unit.group_id);
                    defer alloc.free(path);
                    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
                    defer io_impl.deinit();
                    std.Io.Dir.cwd().access(io_impl.io(), path, .{}) catch |err| switch (err) {
                        error.FileNotFound => return error.UnknownGroup,
                        else => return err,
                    };
                    var one = (try runHostedForeignKeyIntegrityGroupLocal(
                        self,
                        alloc,
                        unit.group_id,
                        table_name,
                        action,
                        constraint_name,
                        unit.lower_doc_key,
                        unit.upper_doc_key,
                        violation_limit -| violations.items.len,
                    )) orelse return error.UnknownGroup;
                    defer one.deinit(alloc);
                    try appendForeignKeyIntegrityResult(alloc, one, violation_limit, &aggregate, &group_reports, &progress_entries, &work_units, &work_claims, &jobs, &violations, &delete_plan, &truncated, &valid, &complete);
                }
            }
        }

        const work_statuses = try buildForeignKeyIntegrityWorkStatuses(alloc, work_units.items, progress_entries.items, work_claims.items, .pending);
        errdefer {
            for (work_statuses) |*status| status.deinit(alloc);
            if (work_statuses.len > 0) alloc.free(work_statuses);
        }

        return .{
            .action = action,
            .valid = valid,
            .complete = complete,
            .violation_limit = violation_limit,
            .violations_truncated = truncated,
            .report = aggregate,
            .delete_plan = delete_plan,
            .groups = try group_reports.toOwnedSlice(alloc),
            .progress = try progress_entries.toOwnedSlice(alloc),
            .work_units = try work_units.toOwnedSlice(alloc),
            .work_claims = try work_claims.toOwnedSlice(alloc),
            .work_statuses = work_statuses,
            .jobs = try jobs.toOwnedSlice(alloc),
            .violations = try violations.toOwnedSlice(alloc),
        };
    }

    fn foreignKeyIntegrityGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        action: ForeignKeyIntegrityAction,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?ForeignKeyIntegrityResult {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try runHostedForeignKeyIntegrityGroupLocal(
            self,
            alloc,
            group_id,
            table_name,
            action,
            constraint_name,
            lower_doc_key,
            upper_doc_key,
            violation_limit,
        );
    }

    fn foreignKeyIntegrityWorkUnitGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        action: ForeignKeyIntegrityAction,
        phase: []const u8,
        job_id: ?[]const u8,
        claim_key: []const u8,
        worker_id: []const u8,
        lease_ms: u64,
        max_work_units: usize,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?ForeignKeyIntegrityResult {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try runHostedForeignKeyIntegrityClaimedWorkUnitGroupLocal(
            self,
            alloc,
            group_id,
            table_name,
            action,
            phase,
            job_id,
            claim_key,
            worker_id,
            lease_ms,
            max_work_units,
            constraint_name,
            lower_doc_key,
            upper_doc_key,
            violation_limit,
        );
    }

    fn uniqueConstraintIntegrity(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        action: UniqueConstraintIntegrityAction,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
    ) !?UniqueConstraintIntegrityResult {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const group_ids = try table_catalog.resolveGroupsForSpanEventually(
            alloc,
            self.catalog,
            table_name,
            lower_doc_key,
            upper_doc_key,
            5 * std.time.ns_per_s,
            10,
        );
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;

        var group_reports = std.ArrayListUnmanaged(UniqueConstraintIntegrityGroupReport).empty;
        var progress_entries = std.ArrayListUnmanaged(UniqueConstraintIntegrityProgress).empty;
        errdefer {
            group_reports.deinit(alloc);
            for (progress_entries.items) |*progress| progress.deinit(alloc);
            progress_entries.deinit(alloc);
        }

        var aggregate: db_mod.relational_store.UniqueConstraintIntegrityReport = .{};
        var valid = true;
        var complete = true;
        for (group_ids) |group_id| {
            var resolved_route = try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, .prefer_leader);
            if (resolved_route) |*route| {
                defer route.deinit(alloc);
                switch (route.*) {
                    .local => {
                        var one = (try runHostedUniqueConstraintIntegrityGroupLocal(
                            self,
                            alloc,
                            group_id,
                            table_name,
                            action,
                            lower_doc_key,
                            upper_doc_key,
                        )) orelse return error.UnknownGroup;
                        defer one.deinit(alloc);
                        try appendUniqueConstraintIntegrityResult(alloc, one, &aggregate, &group_reports, &progress_entries, &valid, &complete);
                    },
                    .remote => |remote| {
                        var client = http_client.ApiHttpClient.init(alloc, self.executor);
                        const body = try std.json.Stringify.valueAlloc(alloc, UniqueConstraintIntegrityRequest{
                            .action = action,
                            .lower_doc_key = lower_doc_key,
                            .upper_doc_key = upper_doc_key,
                        }, .{});
                        defer alloc.free(body);
                        var response = try client.fetchGroupUniqueIntegrity(remote.base_uri, group_id, table_name, body);
                        defer response.deinit(alloc);
                        var parsed = try std.json.parseFromSlice(UniqueConstraintIntegrityResult, alloc, response.body, .{
                            .ignore_unknown_fields = true,
                        });
                        defer parsed.deinit();
                        try appendUniqueConstraintIntegrityResult(alloc, parsed.value, &aggregate, &group_reports, &progress_entries, &valid, &complete);
                    },
                }
            } else {
                const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
                defer alloc.free(path);
                var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
                defer io_impl.deinit();
                std.Io.Dir.cwd().access(io_impl.io(), path, .{}) catch |err| switch (err) {
                    error.FileNotFound => return error.UnknownGroup,
                    else => return err,
                };
                var one = (try runHostedUniqueConstraintIntegrityGroupLocal(
                    self,
                    alloc,
                    group_id,
                    table_name,
                    action,
                    lower_doc_key,
                    upper_doc_key,
                )) orelse return error.UnknownGroup;
                defer one.deinit(alloc);
                try appendUniqueConstraintIntegrityResult(alloc, one, &aggregate, &group_reports, &progress_entries, &valid, &complete);
            }
        }

        var owner_topology = try table_write_integrity.inspectUniqueConstraintOwnerTopology(alloc, self.catalog, table_name);
        errdefer if (owner_topology) |*topology| topology.deinit(alloc);

        return .{
            .action = action,
            .valid = valid,
            .complete = complete,
            .report = aggregate,
            .groups = try group_reports.toOwnedSlice(alloc),
            .owner_topology = owner_topology,
            .progress = try progress_entries.toOwnedSlice(alloc),
        };
    }

    fn uniqueConstraintIntegrityGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        action: UniqueConstraintIntegrityAction,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
    ) !?UniqueConstraintIntegrityResult {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try runHostedUniqueConstraintIntegrityGroupLocal(
            self,
            alloc,
            group_id,
            table_name,
            action,
            lower_doc_key,
            upper_doc_key,
        );
    }

    fn foreignKeyRefChildrenGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        limit: usize,
    ) !?[]db_mod.types.ForeignKeyRefChild {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
        var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group_id, table_name, .status_only);
        defer cached.deinit(hosted_cache.write_cache.alloc);
        return try cached.db.listForeignKeyRefChildrenForParent(alloc, constraint_name, parent_table, parent_key, limit);
    }

    fn foreignKeyRefChildrenPageGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        start_after_child_table: ?[]const u8,
        start_after_child_key: ?[]const u8,
        limit: usize,
    ) !?db_mod.types.ForeignKeyRefChildrenPage {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
        var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group_id, table_name, .status_only);
        defer cached.deinit(hosted_cache.write_cache.alloc);
        return try cached.db.listForeignKeyRefChildrenPageForParent(alloc, constraint_name, parent_table, parent_key, start_after_child_table, start_after_child_key, limit);
    }

    fn collectHostedForeignKeyIntegrityStatusSnapshot(
        self: *HostedProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        planned_units: []const ForeignKeyIntegrityWorkUnit,
        progress_entries: *std.ArrayListUnmanaged(ForeignKeyIntegrityProgress),
        work_claims: *std.ArrayListUnmanaged(ForeignKeyIntegrityWorkClaim),
    ) !void {
        for (planned_units, 0..) |unit, i| {
            if (foreignKeyIntegrityPlannedUnitsContainGroupBefore(planned_units, i)) continue;
            var one: ForeignKeyIntegrityResult = blk: {
                var resolved_route = try table_router.resolveGroupRoute(alloc, self.catalog, self.router, unit.group_id, .prefer_leader);
                if (resolved_route) |*route| {
                    defer route.deinit(alloc);
                    switch (route.*) {
                        .local => break :blk (try runHostedForeignKeyIntegrityGroupLocal(
                            self,
                            alloc,
                            unit.group_id,
                            table_name,
                            .progress,
                            null,
                            "",
                            "",
                            0,
                        )) orelse return error.UnknownGroup,
                        .remote => |remote| {
                            var client = http_client.ApiHttpClient.init(alloc, self.executor);
                            const body = try std.json.Stringify.valueAlloc(alloc, ForeignKeyIntegrityRequest{
                                .action = .progress,
                            }, .{});
                            defer alloc.free(body);
                            var response = try client.fetchGroupForeignKeyIntegrity(remote.base_uri, unit.group_id, table_name, body);
                            defer response.deinit(alloc);
                            var parsed = try std.json.parseFromSlice(ForeignKeyIntegrityResult, alloc, response.body, .{
                                .allocate = .alloc_always,
                                .ignore_unknown_fields = true,
                            });
                            defer parsed.deinit();
                            break :blk try cloneForeignKeyIntegrityResultForWorkerSnapshot(alloc, parsed.value);
                        },
                    }
                }
                const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, unit.group_id);
                defer alloc.free(path);
                var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
                defer io_impl.deinit();
                std.Io.Dir.cwd().access(io_impl.io(), path, .{}) catch |err| switch (err) {
                    error.FileNotFound => return error.UnknownGroup,
                    else => return err,
                };
                break :blk (try runHostedForeignKeyIntegrityGroupLocal(
                    self,
                    alloc,
                    unit.group_id,
                    table_name,
                    .progress,
                    null,
                    "",
                    "",
                    0,
                )) orelse return error.UnknownGroup;
            };
            defer one.deinit(alloc);
            var jobs = std.ArrayListUnmanaged(ForeignKeyIntegrityJobStatus).empty;
            defer {
                for (jobs.items) |*job| job.deinit(alloc);
                jobs.deinit(alloc);
            }
            try appendForeignKeyIntegrityProgressAndClaims(alloc, one, progress_entries, work_claims, &jobs);
        }
    }

    fn runHostedForeignKeyIntegrityClaimedWorkUnitRouted(
        self: *HostedProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        unit: ForeignKeyIntegrityWorkUnit,
        action: ForeignKeyIntegrityAction,
        job_id: ?[]const u8,
        claim_key: []const u8,
        worker_id: []const u8,
        lease_ms: u64,
        max_work_units: usize,
        violation_limit: usize,
    ) !ForeignKeyIntegrityResult {
        var resolved_route = try table_router.resolveGroupRoute(alloc, self.catalog, self.router, unit.group_id, .prefer_leader);
        if (resolved_route) |*route| {
            defer route.deinit(alloc);
            switch (route.*) {
                .local => return (try runHostedForeignKeyIntegrityClaimedWorkUnitGroupLocal(
                    self,
                    alloc,
                    unit.group_id,
                    table_name,
                    action,
                    unit.phase,
                    job_id,
                    claim_key,
                    worker_id,
                    lease_ms,
                    max_work_units,
                    unit.constraint_name,
                    unit.lower_doc_key,
                    unit.upper_doc_key,
                    violation_limit,
                )) orelse error.UnknownGroup,
                .remote => |remote| {
                    var client = http_client.ApiHttpClient.init(alloc, self.executor);
                    const body = try std.json.Stringify.valueAlloc(alloc, ForeignKeyIntegrityRequest{
                        .action = action,
                        .phase = unit.phase,
                        .constraint_name = unit.constraint_name,
                        .lower_doc_key = unit.lower_doc_key,
                        .upper_doc_key = unit.upper_doc_key,
                        .violation_limit = violation_limit,
                        .job_id = job_id,
                        .claim_key = claim_key,
                        .worker_id = worker_id,
                        .lease_ms = lease_ms,
                        .max_work_units = max_work_units,
                    }, .{});
                    defer alloc.free(body);
                    var response = try client.fetchGroupForeignKeyIntegrity(remote.base_uri, unit.group_id, table_name, body);
                    defer response.deinit(alloc);
                    var parsed = try std.json.parseFromSlice(ForeignKeyIntegrityResult, alloc, response.body, .{
                        .allocate = .alloc_always,
                        .ignore_unknown_fields = true,
                    });
                    defer parsed.deinit();
                    return try cloneForeignKeyIntegrityResultForWorkerExecution(alloc, parsed.value);
                },
            }
        }

        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, unit.group_id);
        defer alloc.free(path);
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().access(io_impl.io(), path, .{}) catch |err| switch (err) {
            error.FileNotFound => return error.UnknownGroup,
            else => return err,
        };
        return (try runHostedForeignKeyIntegrityClaimedWorkUnitGroupLocal(
            self,
            alloc,
            unit.group_id,
            table_name,
            action,
            unit.phase,
            job_id,
            claim_key,
            worker_id,
            lease_ms,
            max_work_units,
            unit.constraint_name,
            unit.lower_doc_key,
            unit.upper_doc_key,
            violation_limit,
        )) orelse error.UnknownGroup;
    }

    fn foreignKeyActionJobPage(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
        lease_ms: u64,
    ) !?ForeignKeyActionJobResult {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const owner_parent_table = try foreignKeyActionOwnerParentTableNameAlloc(
            alloc,
            self.catalog,
            table_name,
            constraint_name,
            parent_table,
        );
        defer alloc.free(owner_parent_table);
        var resolution = try table_catalog.resolveForeignKeyRefOwnerGroups(
            alloc,
            self.catalog,
            table_name,
            constraint_name,
            owner_parent_table,
            parent_key,
        );
        defer resolution.deinit(alloc);
        if (!resolution.configured) return error.UnsupportedOperation;
        if (resolution.groups.len == 0) return error.UnknownGroup;

        var groups = std.ArrayListUnmanaged(ForeignKeyActionJobStatus).empty;
        errdefer {
            for (groups.items) |*group| group.deinit(alloc);
            groups.deinit(alloc);
        }
        var complete = true;
        for (resolution.groups) |group_id| {
            var route = try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, .prefer_leader);
            if (route) |*resolved| {
                defer resolved.deinit(alloc);
                switch (resolved.*) {
                    .local => {},
                    .remote => |remote| {
                        var client = http_client.ApiHttpClient.init(alloc, self.executor);
                        const body = try std.json.Stringify.valueAlloc(alloc, .{
                            .job_id = job_id,
                            .action = action,
                            .worker_id = worker_id,
                            .constraint_name = constraint_name,
                            .parent_table = parent_table,
                            .parent_key = parent_key,
                            .updated_parent_key = updated_parent_key,
                            .page_limit = page_limit,
                            .lease_ms = lease_ms,
                        }, .{});
                        defer alloc.free(body);
                        var response = client.fetchGroupForeignKeyActionJob(remote.base_uri, group_id, table_name, body) catch |err| switch (err) {
                            error.InvalidBatchRequest => return error.InvalidForeignKeyActionJob,
                            else => return err,
                        };
                        defer response.deinit(alloc);
                        var parsed = try std.json.parseFromSlice(ForeignKeyActionJobStatus, alloc, response.body, .{
                            .allocate = .alloc_always,
                            .ignore_unknown_fields = true,
                        });
                        defer parsed.deinit();
                        var status = try cloneForeignKeyActionJobStatus(alloc, parsed.value);
                        errdefer status.deinit(alloc);
                        if (!status.completed) complete = false;
                        try groups.append(alloc, status);
                        continue;
                    },
                }
            }
            var status = try self.runHostedForeignKeyActionJobGroupLocal(
                alloc,
                group_id,
                table_name,
                job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                updated_parent_key,
                page_limit,
                lease_ms,
                0,
                64,
            );
            errdefer status.deinit(alloc);
            if (!status.completed) complete = false;
            try groups.append(alloc, status);
        }
        return .{
            .complete = complete,
            .groups = try groups.toOwnedSlice(alloc),
        };
    }

    fn foreignKeyActionJobGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
        lease_ms: u64,
        cascade_depth: u32,
        cascade_max_depth: u32,
    ) !?ForeignKeyActionJobStatus {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try self.runHostedForeignKeyActionJobGroupLocal(
            alloc,
            group_id,
            table_name,
            job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
            lease_ms,
            cascade_depth,
            cascade_max_depth,
        );
    }

    fn foreignKeyActionJobSchedule(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
        cascade_depth: u32,
        cascade_max_depth: u32,
    ) !?ForeignKeyActionJobResult {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const owner_parent_table = try foreignKeyActionOwnerParentTableNameAlloc(
            alloc,
            self.catalog,
            table_name,
            constraint_name,
            parent_table,
        );
        defer alloc.free(owner_parent_table);
        var resolution = try table_catalog.resolveForeignKeyRefOwnerGroups(
            alloc,
            self.catalog,
            table_name,
            constraint_name,
            owner_parent_table,
            parent_key,
        );
        defer resolution.deinit(alloc);
        if (!resolution.configured) return error.UnsupportedOperation;
        if (resolution.groups.len == 0) return error.UnknownGroup;

        var groups = std.ArrayListUnmanaged(ForeignKeyActionJobStatus).empty;
        errdefer {
            for (groups.items) |*group| group.deinit(alloc);
            groups.deinit(alloc);
        }
        var complete = true;
        for (resolution.groups) |group_id| {
            var route = try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, .prefer_leader);
            if (route) |*resolved| {
                defer resolved.deinit(alloc);
                switch (resolved.*) {
                    .local => {},
                    .remote => |remote| {
                        var client = http_client.ApiHttpClient.init(alloc, self.executor);
                        const body = try std.json.Stringify.valueAlloc(alloc, .{
                            .job_id = job_id,
                            .action = action,
                            .worker_id = worker_id,
                            .constraint_name = constraint_name,
                            .parent_table = parent_table,
                            .parent_key = parent_key,
                            .updated_parent_key = updated_parent_key,
                            .page_limit = page_limit,
                            .cascade_depth = cascade_depth,
                            .cascade_max_depth = cascade_max_depth,
                            .schedule_only = true,
                        }, .{});
                        defer alloc.free(body);
                        var response = client.fetchGroupForeignKeyActionJob(remote.base_uri, group_id, table_name, body) catch |err| switch (err) {
                            error.InvalidBatchRequest => return error.InvalidForeignKeyActionJob,
                            else => return err,
                        };
                        defer response.deinit(alloc);
                        var parsed = try std.json.parseFromSlice(ForeignKeyActionJobStatus, alloc, response.body, .{
                            .allocate = .alloc_always,
                            .ignore_unknown_fields = true,
                        });
                        defer parsed.deinit();
                        var status = try cloneForeignKeyActionJobStatus(alloc, parsed.value);
                        errdefer status.deinit(alloc);
                        if (!status.completed) complete = false;
                        try groups.append(alloc, status);
                        continue;
                    },
                }
            }
            var status = try self.scheduleHostedForeignKeyActionJobGroupLocal(
                alloc,
                group_id,
                table_name,
                job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                updated_parent_key,
                page_limit,
                cascade_depth,
                cascade_max_depth,
            );
            errdefer status.deinit(alloc);
            if (!status.completed) complete = false;
            try groups.append(alloc, status);
        }
        return .{
            .complete = complete,
            .groups = try groups.toOwnedSlice(alloc),
        };
    }

    fn foreignKeyActionJobGroupLocalSchedule(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
        cascade_depth: u32,
        cascade_max_depth: u32,
    ) !?ForeignKeyActionJobStatus {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try self.scheduleHostedForeignKeyActionJobGroupLocal(
            alloc,
            group_id,
            table_name,
            job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
            cascade_depth,
            cascade_max_depth,
        );
    }

    fn foreignKeyActionJobRequeue(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
    ) !?ForeignKeyActionJobResult {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const owner_parent_table = try foreignKeyActionOwnerParentTableNameAlloc(
            alloc,
            self.catalog,
            table_name,
            constraint_name,
            parent_table,
        );
        defer alloc.free(owner_parent_table);
        var resolution = try table_catalog.resolveForeignKeyRefOwnerGroups(
            alloc,
            self.catalog,
            table_name,
            constraint_name,
            owner_parent_table,
            parent_key,
        );
        defer resolution.deinit(alloc);
        if (!resolution.configured) return error.UnsupportedOperation;
        if (resolution.groups.len == 0) return error.UnknownGroup;

        var groups = std.ArrayListUnmanaged(ForeignKeyActionJobStatus).empty;
        errdefer {
            for (groups.items) |*group| group.deinit(alloc);
            groups.deinit(alloc);
        }
        for (resolution.groups) |group_id| {
            var route = try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, .prefer_leader);
            if (route) |*resolved| {
                defer resolved.deinit(alloc);
                switch (resolved.*) {
                    .local => {},
                    .remote => |remote| {
                        var client = http_client.ApiHttpClient.init(alloc, self.executor);
                        const body = try std.json.Stringify.valueAlloc(alloc, .{
                            .job_id = job_id,
                            .action = action,
                            .worker_id = worker_id,
                            .constraint_name = constraint_name,
                            .parent_table = parent_table,
                            .parent_key = parent_key,
                            .updated_parent_key = updated_parent_key,
                            .page_limit = page_limit,
                            .requeue_only = true,
                        }, .{});
                        defer alloc.free(body);
                        var response = try client.fetchGroupForeignKeyActionJob(remote.base_uri, group_id, table_name, body);
                        defer response.deinit(alloc);
                        var parsed = try std.json.parseFromSlice(ForeignKeyActionJobStatus, alloc, response.body, .{
                            .allocate = .alloc_always,
                            .ignore_unknown_fields = true,
                        });
                        defer parsed.deinit();
                        var status = try cloneForeignKeyActionJobStatus(alloc, parsed.value);
                        errdefer status.deinit(alloc);
                        try groups.append(alloc, status);
                        continue;
                    },
                }
            }
            var status = try self.requeueHostedForeignKeyActionJobGroupLocal(
                alloc,
                group_id,
                table_name,
                job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                updated_parent_key,
                page_limit,
            );
            errdefer status.deinit(alloc);
            try groups.append(alloc, status);
        }
        return .{
            .complete = false,
            .groups = try groups.toOwnedSlice(alloc),
        };
    }

    fn foreignKeyActionJobGroupLocalRequeue(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
    ) !?ForeignKeyActionJobStatus {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try self.requeueHostedForeignKeyActionJobGroupLocal(
            alloc,
            group_id,
            table_name,
            job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
        );
    }

    fn foreignKeyActionJobProgress(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?ForeignKeyActionJobProgressResult {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const group_ids = try collectForeignKeyActionJobProgressGroupIds(alloc, self.catalog, table_name);
        defer alloc.free(group_ids);

        var jobs = std.ArrayListUnmanaged(ForeignKeyActionJobStatus).empty;
        errdefer {
            for (jobs.items) |*job| job.deinit(alloc);
            jobs.deinit(alloc);
        }
        for (group_ids) |group_id| {
            var route = try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, .prefer_leader);
            if (route) |*resolved| {
                defer resolved.deinit(alloc);
                switch (resolved.*) {
                    .local => {},
                    .remote => |remote| {
                        var client = http_client.ApiHttpClient.init(alloc, self.executor);
                        var response = try client.fetchGroupForeignKeyActionJobProgress(remote.base_uri, group_id, table_name);
                        defer response.deinit(alloc);
                        var parsed = try std.json.parseFromSlice(ForeignKeyActionJobProgressResult, alloc, response.body, .{
                            .allocate = .alloc_always,
                            .ignore_unknown_fields = true,
                        });
                        defer parsed.deinit();
                        try appendForeignKeyActionJobStatuses(alloc, &jobs, parsed.value.jobs);
                        continue;
                    },
                }
            }

            var progress = try self.runHostedForeignKeyActionJobGroupLocalProgress(alloc, group_id, table_name);
            defer progress.deinit(alloc);
            try appendForeignKeyActionJobStatuses(alloc, &jobs, progress.jobs);
        }
        return .{ .jobs = try jobs.toOwnedSlice(alloc) };
    }

    fn foreignKeyActionJobGroupLocalProgress(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
    ) !?ForeignKeyActionJobProgressResult {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try self.runHostedForeignKeyActionJobGroupLocalProgress(alloc, group_id, table_name);
    }

    fn foreignKeyActionScheduleProgress(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?ForeignKeyActionScheduleProgressResult {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const group_ids = try collectForeignKeyActionJobProgressGroupIds(alloc, self.catalog, table_name);
        defer alloc.free(group_ids);

        var schedules = std.ArrayListUnmanaged(ForeignKeyActionScheduleStatus).empty;
        errdefer {
            for (schedules.items) |*schedule| schedule.deinit(alloc);
            schedules.deinit(alloc);
        }
        for (group_ids) |group_id| {
            var route = try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, .prefer_leader);
            if (route) |*resolved| {
                defer resolved.deinit(alloc);
                switch (resolved.*) {
                    .local => {},
                    .remote => |remote| {
                        var client = http_client.ApiHttpClient.init(alloc, self.executor);
                        var response = try client.fetchGroupForeignKeyActionScheduleProgress(remote.base_uri, group_id, table_name);
                        defer response.deinit(alloc);
                        var parsed = try std.json.parseFromSlice(ForeignKeyActionScheduleProgressResult, alloc, response.body, .{
                            .allocate = .alloc_always,
                            .ignore_unknown_fields = true,
                        });
                        defer parsed.deinit();
                        try appendForeignKeyActionScheduleStatuses(alloc, &schedules, parsed.value.schedules);
                        continue;
                    },
                }
            }

            var progress = try self.runHostedForeignKeyActionScheduleGroupLocalProgress(alloc, group_id, table_name);
            defer progress.deinit(alloc);
            try appendForeignKeyActionScheduleStatuses(alloc, &schedules, progress.schedules);
        }
        return .{ .schedules = try schedules.toOwnedSlice(alloc) };
    }

    fn foreignKeyActionScheduleGroupLocalProgress(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
    ) !?ForeignKeyActionScheduleProgressResult {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try self.runHostedForeignKeyActionScheduleGroupLocalProgress(alloc, group_id, table_name);
    }

    fn foreignKeyActionScheduleMarkSeeded(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        schedule_id: []const u8,
        scheduled_groups: u64,
    ) !?ForeignKeyActionScheduleStatus {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const group_ids = try collectForeignKeyActionJobProgressGroupIds(alloc, self.catalog, table_name);
        defer alloc.free(group_ids);
        for (group_ids) |group_id| {
            var route = try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, .prefer_leader);
            if (route) |*resolved| {
                defer resolved.deinit(alloc);
                switch (resolved.*) {
                    .local => {},
                    .remote => |remote| {
                        var client = http_client.ApiHttpClient.init(alloc, self.executor);
                        const body = try std.json.Stringify.valueAlloc(alloc, .{
                            .schedule_id = schedule_id,
                            .scheduled_groups = scheduled_groups,
                        }, .{});
                        defer alloc.free(body);
                        var response = client.fetchGroupForeignKeyActionSchedule(remote.base_uri, group_id, table_name, body) catch |err| switch (err) {
                            error.InvalidBatchRequest => return error.InvalidForeignKeyActionJob,
                            error.UnknownGroup => continue,
                            else => return err,
                        };
                        defer response.deinit(alloc);
                        var parsed = try std.json.parseFromSlice(ForeignKeyActionScheduleStatus, alloc, response.body, .{
                            .allocate = .alloc_always,
                            .ignore_unknown_fields = true,
                        });
                        defer parsed.deinit();
                        return try cloneForeignKeyActionScheduleStatus(alloc, parsed.value);
                    },
                }
            }

            if (try self.markHostedForeignKeyActionScheduleGroupLocalSeeded(alloc, group_id, table_name, schedule_id, scheduled_groups)) |status| {
                return status;
            }
        }
        return null;
    }

    fn foreignKeyActionScheduleRequeue(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        schedule_id: []const u8,
        action_job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
    ) !?ForeignKeyActionScheduleStatus {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const group_ids = try collectForeignKeyActionJobProgressGroupIds(alloc, self.catalog, table_name);
        defer alloc.free(group_ids);
        for (group_ids) |group_id| {
            var route = try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, .prefer_leader);
            if (route) |*resolved| {
                defer resolved.deinit(alloc);
                switch (resolved.*) {
                    .local => {},
                    .remote => |remote| {
                        var client = http_client.ApiHttpClient.init(alloc, self.executor);
                        const body = try std.json.Stringify.valueAlloc(alloc, .{
                            .schedule_id = schedule_id,
                            .action_job_id = action_job_id,
                            .action = action,
                            .worker_id = worker_id,
                            .constraint_name = constraint_name,
                            .parent_table = parent_table,
                            .parent_key = parent_key,
                            .updated_parent_key = updated_parent_key,
                            .page_limit = page_limit,
                            .requeue_only = true,
                        }, .{});
                        defer alloc.free(body);
                        var response = client.fetchGroupForeignKeyActionSchedule(remote.base_uri, group_id, table_name, body) catch |err| switch (err) {
                            error.InvalidBatchRequest => return error.InvalidForeignKeyActionJob,
                            error.UnknownGroup => continue,
                            else => return err,
                        };
                        defer response.deinit(alloc);
                        var parsed = try std.json.parseFromSlice(ForeignKeyActionScheduleStatus, alloc, response.body, .{
                            .allocate = .alloc_always,
                            .ignore_unknown_fields = true,
                        });
                        defer parsed.deinit();
                        return try cloneForeignKeyActionScheduleStatus(alloc, parsed.value);
                    },
                }
            }

            if (try self.requeueHostedForeignKeyActionScheduleGroupLocal(alloc, group_id, table_name, schedule_id, action_job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit)) |status| {
                return status;
            }
        }
        return null;
    }

    fn foreignKeyActionScheduleGroupLocalMarkSeeded(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        schedule_id: []const u8,
        scheduled_groups: u64,
    ) !?ForeignKeyActionScheduleStatus {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try self.markHostedForeignKeyActionScheduleGroupLocalSeeded(alloc, group_id, table_name, schedule_id, scheduled_groups);
    }

    fn foreignKeyActionScheduleGroupLocalRequeue(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        schedule_id: []const u8,
        action_job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
    ) !?ForeignKeyActionScheduleStatus {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        return try self.requeueHostedForeignKeyActionScheduleGroupLocal(alloc, group_id, table_name, schedule_id, action_job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit);
    }

    fn runHostedForeignKeyActionJobGroupLocalProgress(
        self: *HostedProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
    ) !ForeignKeyActionJobProgressResult {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().access(io_impl.io(), path, .{}) catch |err| switch (err) {
            error.FileNotFound => return .{ .jobs = &.{} },
            else => return err,
        };
        const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
        var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group_id, table_name, .status_only);
        defer cached.deinit(hosted_cache.write_cache.alloc);
        const records = try cached.db.listForeignKeyActionJobRecords();
        defer cached.db.freeForeignKeyActionJobRecords(records);
        return try foreignKeyActionJobProgressFromDbRecords(alloc, group_id, records);
    }

    fn runHostedForeignKeyActionScheduleGroupLocalProgress(
        self: *HostedProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
    ) !ForeignKeyActionScheduleProgressResult {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().access(io_impl.io(), path, .{}) catch |err| switch (err) {
            error.FileNotFound => return .{ .schedules = &.{} },
            else => return err,
        };
        const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
        var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group_id, table_name, .status_only);
        defer cached.deinit(hosted_cache.write_cache.alloc);
        const records = try cached.db.listForeignKeyActionScheduleRecords();
        defer cached.db.freeForeignKeyActionScheduleRecords(records);
        return try foreignKeyActionScheduleProgressFromDbRecords(alloc, group_id, records);
    }

    fn requeueHostedForeignKeyActionJobGroupLocal(
        self: *HostedProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
    ) !ForeignKeyActionJobStatus {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().access(io_impl.io(), path, .{}) catch |err| switch (err) {
            error.FileNotFound => return error.UnknownGroup,
            else => return err,
        };

        const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
        var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group_id, table_name, .default);
        defer cached.deinit(hosted_cache.write_cache.alloc);
        const record = try cached.db.requeueForeignKeyActionJobWithUpdatedParentKey(
            job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
        );
        defer cached.db.freeForeignKeyActionJobRecord(record);
        var status = try foreignKeyActionJobStatusFromDbRecord(alloc, group_id, record);
        errdefer status.deinit(alloc);
        return status;
    }

    fn markHostedForeignKeyActionScheduleGroupLocalSeeded(
        self: *HostedProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        schedule_id: []const u8,
        scheduled_groups: u64,
    ) !?ForeignKeyActionScheduleStatus {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().access(io_impl.io(), path, .{}) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
        var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group_id, table_name, .default);
        defer cached.deinit(hosted_cache.write_cache.alloc);
        const record = cached.db.markForeignKeyActionScheduleSeeded(schedule_id, scheduled_groups) catch |err| switch (err) {
            error.ForeignKeyActionScheduleNotFound => return null,
            else => return err,
        };
        defer cached.db.freeForeignKeyActionScheduleRecord(record);
        var status = try foreignKeyActionScheduleStatusFromDbRecord(alloc, group_id, record);
        errdefer status.deinit(alloc);
        try drainManagedDbBeforeClose(cached.db);
        return status;
    }

    fn requeueHostedForeignKeyActionScheduleGroupLocal(
        self: *HostedProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        schedule_id: []const u8,
        action_job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
    ) !?ForeignKeyActionScheduleStatus {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().access(io_impl.io(), path, .{}) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
        var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group_id, table_name, .default);
        defer cached.deinit(hosted_cache.write_cache.alloc);
        const record = cached.db.requeueForeignKeyActionScheduleWithUpdatedParentKeyAt(
            schedule_id,
            action_job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
            nextTxnTimestamp(),
        ) catch |err| switch (err) {
            error.ForeignKeyActionScheduleNotFound => return null,
            else => return err,
        };
        defer cached.db.freeForeignKeyActionScheduleRecord(record);
        var status = try foreignKeyActionScheduleStatusFromDbRecord(alloc, group_id, record);
        errdefer status.deinit(alloc);
        try drainManagedDbBeforeClose(cached.db);
        return status;
    }

    fn scheduleHostedForeignKeyActionJobGroupLocal(
        self: *HostedProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
        cascade_depth: u32,
        cascade_max_depth: u32,
    ) !ForeignKeyActionJobStatus {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
        var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group_id, table_name, .default);
        defer cached.deinit(hosted_cache.write_cache.alloc);
        const record = try cached.db.scheduleForeignKeyActionJobWithUpdatedParentKeyAndCascadeLineageAt(
            job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
            cascade_depth,
            cascade_max_depth,
            nextTxnTimestamp(),
        );
        defer cached.db.freeForeignKeyActionJobRecord(record);
        var status = try foreignKeyActionJobStatusFromDbRecord(alloc, group_id, record);
        errdefer status.deinit(alloc);
        try drainManagedDbBeforeClose(cached.db);
        return status;
    }

    fn runHostedForeignKeyActionJobGroupLocal(
        self: *HostedProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
        lease_ms: u64,
        cascade_depth: u32,
        cascade_max_depth: u32,
    ) !ForeignKeyActionJobStatus {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
        var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group_id, table_name, .default);
        defer cached.deinit(hosted_cache.write_cache.alloc);
        const record = try self.claimRunAndFinishHostedForeignKeyActionJobGroupLocal(
            alloc,
            group_id,
            table_name,
            cached.db,
            job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
            lease_ms,
            cascade_depth,
            cascade_max_depth,
        );
        defer cached.db.freeForeignKeyActionJobRecord(record);
        var status = try foreignKeyActionJobStatusFromDbRecord(alloc, group_id, record);
        errdefer status.deinit(alloc);
        try drainManagedDbBeforeClose(cached.db);
        return status;
    }

    fn claimRunAndFinishHostedForeignKeyActionJobGroupLocal(
        self: *HostedProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        db: *db_mod.DB,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
        lease_ms: u64,
        cascade_depth: u32,
        cascade_max_depth: u32,
    ) !db_mod.DB.ForeignKeyActionJobRecord {
        if (db.core.schema != null and try foreignKeyActionCanRunGroupDbLocal(
            alloc,
            self.catalog,
            group_id,
            table_name,
            constraint_name,
            parent_table,
            parent_key,
        )) {
            return try db.claimAndRunForeignKeyActionJobPageWithUpdatedParentKeyAndCascadeLineageAt(
                job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                updated_parent_key,
                page_limit,
                lease_ms,
                cascade_depth,
                cascade_max_depth,
                nextTxnTimestamp(),
            );
        }

        const claimed = try db.claimForeignKeyActionJobPageWithUpdatedParentKeyAndCascadeLineageAt(
            job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
            lease_ms,
            cascade_depth,
            cascade_max_depth,
            nextTxnTimestamp(),
        );
        var claimed_owned = true;
        defer if (claimed_owned) db.freeForeignKeyActionJobRecord(claimed);
        if (claimed.completed) {
            claimed_owned = false;
            return claimed;
        }

        var worker = distributed_txn.HostedParticipantWorker.init(self.catalog, self.router, self.source(), self.executor);
        const begin_timestamp = nextTxnTimestamp();
        var execution = distributed_txn.executeForeignKeyActionPage(
            alloc,
            self.catalog,
            worker.worker(),
            stableForeignKeyActionPageTxnId(claimed),
            begin_timestamp,
            begin_timestamp +| 1,
            table_name,
            group_id,
            action,
            constraint_name,
            parent_table,
            parent_key,
            claimed.updated_parent_key,
            claimed.next_child_table,
            claimed.next_child_key,
            page_limit,
            claimed.cascade_depth,
            claimed.cascade_max_depth,
            null,
        ) catch |err| {
            const failed = db.finishClaimedForeignKeyActionJobPage(
                claimed,
                0,
                false,
                claimed.next_child_table,
                claimed.next_child_key,
                @errorName(err),
            ) catch null;
            if (failed) |record| db.freeForeignKeyActionJobRecord(record);
            return err;
        };
        defer execution.deinit(alloc);

        return try db.finishClaimedForeignKeyActionJobPage(
            claimed,
            execution.applied_children,
            execution.complete,
            execution.next_child_table,
            execution.next_child_key,
            null,
        );
    }

    fn runHostedForeignKeyIntegrityWorkerPass(
        self: *HostedProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        action: ForeignKeyIntegrityAction,
        job_id: ?[]const u8,
        worker_id: []const u8,
        lease_ms: u64,
        max_work_units: usize,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?ForeignKeyIntegrityResult {
        if (job_id) |id| {
            if (id.len == 0) return error.InvalidForeignKeyIntegrityRequest;
        }
        if (!foreignKeyIntegrityWorkerActionSupported(action)) return error.InvalidForeignKeyIntegrityRequest;
        if (worker_id.len == 0 or lease_ms == 0) return error.InvalidForeignKeyIntegrityRequest;

        const planned_units = try planForeignKeyIntegrityWorkerWorkUnits(alloc, self.catalog, table_name, action, constraint_name, lower_doc_key, upper_doc_key);
        defer {
            for (planned_units) |*unit| unit.deinit(alloc);
            if (planned_units.len > 0) alloc.free(planned_units);
        }
        if (planned_units.len == 0) return null;

        var initial_progress = std.ArrayListUnmanaged(ForeignKeyIntegrityProgress).empty;
        var initial_claims = std.ArrayListUnmanaged(ForeignKeyIntegrityWorkClaim).empty;
        defer {
            for (initial_progress.items) |*progress| progress.deinit(alloc);
            initial_progress.deinit(alloc);
            for (initial_claims.items) |*claim| claim.deinit(alloc);
            initial_claims.deinit(alloc);
        }
        try collectHostedForeignKeyIntegrityStatusSnapshot(self, alloc, table_name, planned_units, &initial_progress, &initial_claims);
        const initial_statuses = try buildForeignKeyIntegrityWorkStatuses(alloc, planned_units, initial_progress.items, initial_claims.items, .pending);
        defer {
            for (initial_statuses) |*status| status.deinit(alloc);
            if (initial_statuses.len > 0) alloc.free(initial_statuses);
        }

        var aggregate: db_mod.relational_store.ForeignKeyIntegrityReport = .{};
        var group_reports = std.ArrayListUnmanaged(ForeignKeyIntegrityGroupReport).empty;
        var violations = std.ArrayListUnmanaged(ForeignKeyIntegrityViolation).empty;
        errdefer {
            group_reports.deinit(alloc);
            for (violations.items) |*violation| violation.deinit(alloc);
            violations.deinit(alloc);
        }
        var truncated = false;
        var executed: usize = 0;
        const now_ns = foreignKeyIntegrityNowNs();
        for (planned_units, initial_statuses) |unit, status| {
            if (executed >= max_work_units) break;
            if (!foreignKeyIntegrityWorkStatusClaimable(status, now_ns)) continue;
            var one = (runHostedForeignKeyIntegrityClaimedWorkUnitRouted(
                self,
                alloc,
                table_name,
                unit,
                action,
                job_id,
                status.claim_key,
                worker_id,
                lease_ms,
                max_work_units,
                violation_limit -| violations.items.len,
            ) catch |err| switch (err) {
                error.ForeignKeyIntegrityClaimBusy => continue,
                else => return err,
            });
            defer one.deinit(alloc);
            try appendForeignKeyIntegrityExecutedResult(alloc, one, violation_limit, &aggregate, &group_reports, &violations, &truncated);
            executed += 1;
        }

        var final_progress = std.ArrayListUnmanaged(ForeignKeyIntegrityProgress).empty;
        var final_claims = std.ArrayListUnmanaged(ForeignKeyIntegrityWorkClaim).empty;
        errdefer {
            for (final_progress.items) |*progress| progress.deinit(alloc);
            final_progress.deinit(alloc);
            for (final_claims.items) |*claim| claim.deinit(alloc);
            final_claims.deinit(alloc);
        }
        try collectHostedForeignKeyIntegrityStatusSnapshot(self, alloc, table_name, planned_units, &final_progress, &final_claims);
        const work_statuses = try buildForeignKeyIntegrityWorkStatuses(alloc, planned_units, final_progress.items, final_claims.items, .pending);
        errdefer {
            for (work_statuses) |*status| status.deinit(alloc);
            if (work_statuses.len > 0) alloc.free(work_statuses);
        }
        const work_units = try cloneForeignKeyIntegrityWorkUnits(alloc, planned_units);
        errdefer {
            for (work_units) |*unit| unit.deinit(alloc);
            if (work_units.len > 0) alloc.free(work_units);
        }

        var result: ForeignKeyIntegrityResult = .{
            .action = action,
            .valid = foreignKeyIntegrityWorkStatusesValid(work_statuses),
            .complete = !foreignKeyIntegrityWorkStatusesHaveClaimable(work_statuses, foreignKeyIntegrityNowNs()),
            .violation_limit = violation_limit,
            .violations_truncated = truncated,
            .report = aggregate,
            .groups = try group_reports.toOwnedSlice(alloc),
            .progress = try final_progress.toOwnedSlice(alloc),
            .work_units = work_units,
            .work_claims = try final_claims.toOwnedSlice(alloc),
            .work_statuses = work_statuses,
            .violations = try violations.toOwnedSlice(alloc),
        };
        errdefer result.deinit(alloc);
        try attachForeignKeyIntegrityJobId(alloc, &result, job_id);
        return result;
    }

    fn runHostedForeignKeyIntegrityGroupLocal(
        self: *HostedProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        action: ForeignKeyIntegrityAction,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?ForeignKeyIntegrityResult {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const mode: ManagedDbOpenMode = switch (action) {
            .plan, .validate, .dry_run, .list, .explain_delete, .progress => .status_only,
            .repair => .default,
        };
        const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
        var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group_id, table_name, mode);
        defer cached.deinit(hosted_cache.write_cache.alloc);
        var result = try runForeignKeyIntegrityOnDb(alloc, cached.db, group_id, action, constraint_name, lower_doc_key, upper_doc_key, violation_limit);
        errdefer result.deinit(alloc);
        if (action == .repair) try drainManagedDbBeforeClose(cached.db);
        return result;
    }

    fn runHostedForeignKeyIntegrityClaimedWorkUnitGroupLocal(
        self: *HostedProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        action: ForeignKeyIntegrityAction,
        phase: []const u8,
        job_id: ?[]const u8,
        claim_key: []const u8,
        worker_id: []const u8,
        lease_ms: u64,
        max_work_units: usize,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?ForeignKeyIntegrityResult {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
        var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group_id, table_name, .default);
        defer cached.deinit(hosted_cache.write_cache.alloc);
        try startForeignKeyIntegrityJobOnDb(cached.db, job_id, table_name, action, worker_id, constraint_name, lower_doc_key, upper_doc_key, lease_ms, max_work_units);
        var result = try runForeignKeyIntegrityClaimedWorkUnitOnDb(
            alloc,
            cached.db,
            group_id,
            action,
            phase,
            claim_key,
            worker_id,
            lease_ms,
            constraint_name,
            lower_doc_key,
            upper_doc_key,
            violation_limit,
        );
        errdefer result.deinit(alloc);
        try finishForeignKeyIntegrityJobOnDb(alloc, cached.db, job_id, result);
        try attachForeignKeyIntegrityJobId(alloc, &result, job_id);
        try hydrateForeignKeyIntegrityResultDiagnosticsFromJobOnDb(alloc, cached.db, group_id, &result);
        try drainManagedDbBeforeClose(cached.db);
        return result;
    }

    fn runHostedUniqueConstraintIntegrityGroupLocal(
        self: *HostedProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        action: UniqueConstraintIntegrityAction,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
    ) !?UniqueConstraintIntegrityResult {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const mode: ManagedDbOpenMode = switch (action) {
            .progress => .status_only,
            .validate, .dry_run, .repair => .default,
        };
        const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
        var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group_id, table_name, mode);
        defer cached.deinit(hosted_cache.write_cache.alloc);
        var result = try runUniqueConstraintIntegrityOnDb(alloc, cached.db, group_id, action, lower_doc_key, upper_doc_key);
        errdefer result.deinit(alloc);
        if (action == .validate or action == .dry_run or action == .repair) try drainManagedDbBeforeClose(cached.db);
        return result;
    }

    fn runHostedSecondaryIndexRebuildGroupLocal(
        self: *HostedProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        record: metadata_table_manager.SecondaryIndexRebuildRangeRecord,
        worker_id: []const u8,
        lease_ms: u64,
    ) !?SecondaryIndexRebuildWorkerResult {
        if (record.group_id != group_id) return error.InvalidSecondaryIndexRebuildRequest;
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
        var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group_id, table_name, .default);
        defer cached.deinit(hosted_cache.write_cache.alloc);
        const now_ms = platform_time.monotonicNs() / std.time.ns_per_ms;
        const result = try runSecondaryIndexRebuildRangeGroupLocal(cached.db, self.catalog, record, worker_id, now_ms, lease_ms);
        if (result.claimed) try drainManagedDbBeforeClose(cached.db);
        return result;
    }

    fn runHostedSchemaRewriteGroupLocal(
        self: *HostedProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        record: metadata_table_manager.SchemaRewriteJobRecord,
        worker_id: []const u8,
        lease_ms: u64,
    ) !?SchemaRewriteWorkerResult {
        if (record.group_id != group_id) return error.InvalidSchemaRewriteJobRange;
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);
        const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
        var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group_id, table_name, .default);
        defer cached.deinit(hosted_cache.write_cache.alloc);
        const now_ms = platform_time.monotonicNs() / std.time.ns_per_ms;
        const result = try runSchemaRewriteJobGroupLocal(alloc, cached.db, self.catalog, record, worker_id, now_ms, lease_ms);
        if (result.claimed) try drainManagedDbBeforeClose(cached.db);
        return result;
    }

    fn corruptEmbeddingArtifact(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        index_name: []const u8,
    ) !?void {
        const self: *HostedProvisionedTableWriteSource = @ptrCast(@alignCast(ptr));
        const group_ids = try table_catalog.resolveGroupsForSpanEventually(
            alloc,
            self.catalog,
            table_name,
            "",
            "",
            5 * std.time.ns_per_s,
            10,
        );
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;

        for (group_ids) |group_id| {
            const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
            defer alloc.free(path);
            const hosted_cache = try hostedManagedDbCacheForRoot(self.replica_root_dir);
            var cached = try self.getOrOpenCachedDbMode(hosted_cache, path, group_id, table_name, .default);
            defer cached.deinit(hosted_cache.write_cache.alloc);
            if (try corruptEmbeddingArtifactInDb(alloc, cached.db, doc_key, index_name)) return;
        }

        return error.NotFound;
    }
};

const parseIndexKind = table_write_index_config.parseIndexKind;
const isReservedTableIndexMetadataEntry = table_write_index_config.isReservedTableIndexMetadataEntry;
const parseIndexConfig = table_write_index_config.parseIndexConfig;
const extractIndexConfigJson = table_write_index_config.extractIndexConfigJson;
const StartupConfiguredIndexes = table_write_index_config.StartupConfiguredIndexes;
const parseStartupConfiguredIndexes = table_write_index_config.parseStartupConfiguredIndexes;
const encodeRemoteBatchRequest = table_write_remote_wire.encodeRemoteBatchRequest;

fn reconcileCachedLocalTableIndexCreate(
    self: *ProvisionedTableWriteSource,
    alloc: std.mem.Allocator,
    cache: *ProvisionedTableWriteCache,
    table_name: []const u8,
    index_name: []const u8,
) !bool {
    const group_ids = try table_catalog.resolveGroupsForSpanEventually(
        alloc,
        self.catalog,
        table_name,
        "",
        "",
        5 * std.time.ns_per_s,
        10,
    );
    defer alloc.free(group_ids);

    var managed_visibility_changed = false;
    for (group_ids) |group_id| {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);

        var cached = try cache.getOrOpenLockedMode(path, self.catalog, group_id, self.visibleRootGeneration(group_id), table_name, .default_async);
        defer cached.deinit(alloc);
        cached.db.setQueryVisibilityHook(self.managedDerivedVisibilityHook(cached.entry.?.table_name, group_id, cached.db));

        try catchUpManagedIndexCreate(alloc, cached.db, index_name);
        try publishRuntimeStatusSnapshotConsistent(self, alloc, table_name, group_id, cached.db);
        managed_visibility_changed = true;
    }
    return managed_visibility_changed;
}

fn reconcileUncachedLocalTableIndexCreate(
    self: *ProvisionedTableWriteSource,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    index_name: []const u8,
) !bool {
    const group_ids = try table_catalog.resolveGroupsForSpanEventually(
        alloc,
        self.catalog,
        table_name,
        "",
        "",
        5 * std.time.ns_per_s,
        10,
    );
    defer alloc.free(group_ids);

    var managed_visibility_changed = false;
    for (group_ids) |group_id| {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);

        var db = try openManagedDbForTableGroupWithRuntimeAndHAWriteGate(alloc, path, self.catalog, table_name, group_id, self.backend_runtime, self.ha_write_gate, self.ha_async_mirror);
        defer db.close();

        try catchUpManagedIndexCreate(alloc, &db, index_name);
        try publishRuntimeStatusSnapshotConsistent(self, alloc, table_name, group_id, &db);
        managed_visibility_changed = true;
    }
    return managed_visibility_changed;
}

fn reconcileLocalTableIndexCreate(
    self: *ProvisionedTableWriteSource,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    index_name: []const u8,
) !bool {
    if (self.write_cache) |cache| {
        return try reconcileCachedLocalTableIndexCreate(self, alloc, cache, table_name, index_name);
    }
    return try reconcileUncachedLocalTableIndexCreate(self, alloc, table_name, index_name);
}

fn publishRuntimeStatusSnapshot(
    source: *ProvisionedTableWriteSource,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    group_id: u64,
    db: *db_mod.DB,
) !void {
    try table_write_cache.publishRuntimeStatusSnapshot(
        source.runtime_status_cache,
        source.startup_catch_up_active.load(.monotonic),
        alloc,
        table_name,
        group_id,
        db,
    );
}

fn publishRuntimeStatusSnapshotConsistent(
    source: *ProvisionedTableWriteSource,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    group_id: u64,
    db: *db_mod.DB,
) !void {
    try table_write_cache.publishRuntimeStatusSnapshotConsistent(
        source.runtime_status_cache,
        source.startup_catch_up_active.load(.monotonic),
        alloc,
        table_name,
        group_id,
        db,
    );
}

fn tryPublishRuntimeStatusSnapshotConsistent(
    source: *ProvisionedTableWriteSource,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    group_id: u64,
    db: *db_mod.DB,
) !bool {
    return try table_write_cache.tryPublishRuntimeStatusSnapshotConsistent(
        source.runtime_status_cache,
        source.startup_catch_up_active.load(.monotonic),
        alloc,
        table_name,
        group_id,
        db,
    );
}

fn publishRuntimeStatusSnapshotWithStartupPhase(
    source: *ProvisionedTableWriteSource,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    group_id: u64,
    phase: db_mod.types.StartupCatchUpPhase,
    db: *db_mod.DB,
) !void {
    try table_write_cache.publishRuntimeStatusSnapshotWithStartupPhase(
        source.runtime_status_cache,
        source.startup_catch_up_active.load(.monotonic),
        alloc,
        table_name,
        group_id,
        phase,
        db,
    );
}

fn publishStartupCatchUpRuntimeStatusSnapshot(
    source: *ProvisionedTableWriteSource,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    group_id: u64,
    startup: db_mod.types.StartupCatchUpStats,
    db: ?*db_mod.DB,
    configured_indexes: ?*const StartupConfiguredIndexes,
) !void {
    try table_write_cache.publishStartupCatchUpRuntimeStatusSnapshot(
        source.runtime_status_cache,
        alloc,
        table_name,
        group_id,
        startup,
        db,
        configured_indexes,
    );
}

fn catchUpManagedDb(
    source: *ProvisionedTableWriteSource,
    alloc: std.mem.Allocator,
    group_id: u64,
    table_name: []const u8,
    db: *db_mod.DB,
) !ProvisionedTableWriteSource.StartupCatchUpResult {
    const before = db.listDerivedReplayDebt(alloc) catch |err| {
        std.log.warn("managed startup catch-up list debt failed table={s} err={}", .{ table_name, err });
        return err;
    };
    defer {
        for (before) |*status| status.deinit(alloc);
        alloc.free(before);
    }

    var had_debt = false;
    for (before) |status| {
        if (!status.catch_up_required) continue;
        had_debt = true;
        break;
    }
    const ProgressCtx = struct {
        source: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        db: *db_mod.DB,
        phase: db_mod.types.StartupCatchUpPhase,

        fn run(ptr: *anyopaque, _: []const u8, _: db_mod.ReplayProgress) !void {
            const ctx: *@This() = @ptrCast(@alignCast(ptr));
            try publishRuntimeStatusSnapshotWithStartupPhase(ctx.source, ctx.alloc, ctx.table_name, ctx.group_id, ctx.phase, ctx.db);
        }
    };

    var progress_ctx = ProgressCtx{
        .source = source,
        .alloc = alloc,
        .group_id = group_id,
        .table_name = table_name,
        .db = db,
        .phase = .startup_catch_up,
    };

    const restore_repair_needed = db.restoreRuntimeRepairNeeded() catch |err| {
        std.log.warn("managed startup catch-up restore repair probe failed table={s} err={}", .{ table_name, err });
        return err;
    };
    const needs_dense_artifact_rebuild = db.hasPendingDenseArtifactRebuild(alloc) catch |err| {
        std.log.warn("managed startup catch-up dense rebuild probe failed table={s} err={}", .{ table_name, err });
        return err;
    };

    var repaired_restore_runtime = false;
    var repaired_dense_artifacts: usize = 0;
    if (restore_repair_needed) {
        std.log.info("managed restore repair begin table={s} group_id={d}", .{ table_name, group_id });
        progress_ctx.phase = .artifact_rebuild;
        try publishRuntimeStatusSnapshotWithStartupPhase(source, alloc, table_name, group_id, .artifact_rebuild, db);
        repaired_restore_runtime = db.repairRestoreRuntimeStateStepIfNeeded(alloc) catch |err| {
            std.log.warn("managed startup catch-up restore repair failed table={s} err={}", .{ table_name, err });
            return err;
        };
        try publishRuntimeStatusSnapshotWithStartupPhase(source, alloc, table_name, group_id, .artifact_rebuild, db);
        std.log.info("managed restore repair step complete table={s} group_id={d} repaired={}", .{ table_name, group_id, repaired_restore_runtime });
    } else if (had_debt) {
        try publishRuntimeStatusSnapshotWithStartupPhase(source, alloc, table_name, group_id, .startup_catch_up, db);
        db.catchUpPendingDerivedReplayWithProgress(&progress_ctx, ProgressCtx.run) catch |err| {
            std.log.warn("managed startup catch-up replay failed table={s} err={}", .{ table_name, err });
            if (err == error.WriterLocked or err == error.ReplayDocumentNotVisible) {
                return .{
                    .had_debt = true,
                    .cleared_debt = false,
                    .busy = true,
                };
            }
            return err;
        };
        db.runUntilIdle() catch |err| {
            std.log.warn("managed startup catch-up replay idle drain failed table={s} err={}", .{ table_name, err });
            if (err == error.WriterLocked or err == error.ReplayDocumentNotVisible) {
                return .{
                    .had_debt = true,
                    .cleared_debt = false,
                    .busy = true,
                };
            }
            return err;
        };
        try db.core.index_manager.syncAll(false);
    }

    if (!had_debt and !restore_repair_needed and !needs_dense_artifact_rebuild) {
        try publishRuntimeStatusSnapshotWithStartupPhase(source, alloc, table_name, group_id, .idle, db);
        return .{};
    }

    if (!restore_repair_needed and needs_dense_artifact_rebuild) {
        progress_ctx.phase = .artifact_rebuild;
        try publishRuntimeStatusSnapshotWithStartupPhase(source, alloc, table_name, group_id, .artifact_rebuild, db);
        repaired_dense_artifacts = db.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeededWithProgress(alloc, &progress_ctx, ProgressCtx.run) catch |err| {
            std.log.warn("managed startup catch-up dense rebuild failed table={s} err={}", .{ table_name, err });
            return err;
        };
        if (repaired_dense_artifacts > 0) {
            db.runUntilIdle() catch |err| {
                std.log.warn("managed startup catch-up dense rebuild idle drain failed table={s} err={}", .{ table_name, err });
                return err;
            };
            try db.core.index_manager.syncAll(false);
        }
        try publishRuntimeStatusSnapshotWithStartupPhase(source, alloc, table_name, group_id, .artifact_rebuild, db);
    }

    if (!had_debt and !repaired_restore_runtime and repaired_dense_artifacts == 0) {
        return .{};
    }

    if (repaired_restore_runtime) db.clearDenseHbcCaches();

    // Startup catch-up mutates on-disk index/runtime state through an isolated
    // DB instance. Drop shared cached handles for this table so future reads
    // and writer-side status probes reopen against the updated state. Do not
    // invalidate the startup cache here because `db` may still alias its live
    // entry until the caller's deferred startup-cache clear runs.
    source.invalidateReadCache(table_name);
    source.invalidateWriteCacheForTable(table_name);
    source.clearDirtyWriteTable(table_name);

    const after = db.listDerivedReplayDebt(alloc) catch |err| {
        std.log.warn("managed startup catch-up post-check debt failed table={s} err={}", .{ table_name, err });
        return err;
    };
    defer {
        for (after) |*status| status.deinit(alloc);
        alloc.free(after);
    }

    for (after) |status| {
        if (status.catch_up_required) {
            return .{
                .had_debt = had_debt or restore_repair_needed,
                .cleared_debt = false,
            };
        }
    }
    if (db.hasPendingDenseArtifactRebuild(alloc) catch |err| {
        std.log.warn("managed startup catch-up post-check dense rebuild probe failed table={s} err={}", .{ table_name, err });
        return err;
    }) {
        return .{
            .had_debt = had_debt or restore_repair_needed,
            .cleared_debt = false,
        };
    }
    if (db.restoreRuntimeRepairNeeded() catch |err| {
        std.log.warn("managed startup catch-up post-check restore repair probe failed table={s} err={}", .{ table_name, err });
        return err;
    }) {
        return .{
            .had_debt = had_debt or restore_repair_needed,
            .cleared_debt = false,
        };
    }
    return .{
        .had_debt = had_debt or restore_repair_needed,
        .cleared_debt = had_debt or repaired_restore_runtime,
    };
}

fn sleepNs(duration_ns: u64) void {
    platform_time_lib.sleepNs(duration_ns);
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    // Bounded spin, then yield (platform_sync): local_db_mutex guards cache
    // bookkeeping that can take a while under contention (opens,
    // invalidation), and a pure spin pins a core per waiter — on
    // CPU-constrained hosts (CI runners) that starves the very threads that
    // would release the lock.
    platform_sync.lockYielding(mutex);
}

fn recoverProvisionedTransactionsOnce(
    self: *ProvisionedTableWriteSource,
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
) !void {
    var worker_impl = distributed_txn.LocalTableWriteParticipantWorker.init(self.source());
    var resolver = distributed_txn.RecoveryResolver{
        .alloc = alloc,
        .worker = worker_impl.worker(),
        .owner_id = "api-provisioned",
        .lease_owned = true,
    };
    _ = try db.runTransactionRecoveryOnce(resolver.config());
}

fn recoverHostedTransactionsOnce(
    self: *HostedProvisionedTableWriteSource,
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
) !void {
    var worker_impl = distributed_txn.HostedParticipantWorker.init(self.catalog, self.router, self.source(), self.executor);
    var resolver = distributed_txn.RecoveryResolver{
        .alloc = alloc,
        .worker = worker_impl.worker(),
        .owner_id = "api-hosted",
        .lease_owned = true,
    };
    _ = try db.runTransactionRecoveryOnce(resolver.config());
}

pub const BoundTableWriteSource = struct {
    table_name: []const u8,
    db: *db_mod.DB,

    pub fn init(table_name: []const u8, db: *db_mod.DB) BoundTableWriteSource {
        return .{
            .table_name = table_name,
            .db = db,
        };
    }

    pub fn source(self: *BoundTableWriteSource) TableWriteSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .create_table = createTable,
                .update_schema = updateSchema,
                .create_index = createIndex,
                .drop_index = dropIndex,
                .graph_metric_action = graphMetricAction,
                .reprocess_document_artifact = reprocessDocumentArtifact,
                .reprocess_document_artifact_range = reprocessDocumentArtifactRange,
                .graph_metric_maintenance_group_local = graphMetricMaintenanceGroupLocal,
                .backup_table = backupTable,
                .restore_table = restoreTable,
                .commit_transaction = commitTransaction,
                .commit_transaction_with_id = commitTransactionWithId,
                .batch = batch,
                .mutate_rows_from_source = mutateRowsFromSource,
                .mutate_rows_from_source_autocommit = mutateRowsFromSourceAutocommit,
                .mutate_rows_joined_from_source_rows = mutateRowsJoinedFromSourceRows,
                .mutate_rows_joined_from_source_rows_autocommit = mutateRowsJoinedFromSourceRowsAutocommit,
                .merge_rows_from_source_rows = mergeRowsFromSourceRows,
                .begin_bulk_ingest = beginBulkIngest,
                .finish_bulk_ingest = finishBulkIngest,
                .abort_bulk_ingest = abortBulkIngest,
                .batch_group_local = batchGroupLocal,
                .txn_begin_group_local = txnBeginGroupLocal,
                .txn_prepare_group_local = txnPrepareGroupLocal,
                .txn_resolve_group_local = txnResolveGroupLocal,
                .txn_status_group_local = txnStatusGroupLocal,
                .corrupt_embedding_artifact = corruptEmbeddingArtifact,
                .local_runtime_statuses = localRuntimeStatuses,
                .foreign_key_integrity = foreignKeyIntegrity,
                .foreign_key_integrity_worker_pass = foreignKeyIntegrityWorkerPass,
                .foreign_key_integrity_schema_controller_pass = foreignKeyIntegritySchemaControllerPass,
                .foreign_key_integrity_schema_controller_maintenance_pass = foreignKeyIntegritySchemaControllerMaintenancePass,
                .foreign_key_action_job_page = foreignKeyActionJobPage,
                .foreign_key_action_job_schedule = foreignKeyActionJobSchedule,
                .foreign_key_action_job_requeue = foreignKeyActionJobRequeue,
                .foreign_key_action_job_group_local = foreignKeyActionJobGroupLocal,
                .foreign_key_action_job_group_local_schedule = foreignKeyActionJobGroupLocalSchedule,
                .foreign_key_action_job_group_local_requeue = foreignKeyActionJobGroupLocalRequeue,
                .foreign_key_action_job_progress = foreignKeyActionJobProgress,
                .foreign_key_action_job_group_local_progress = foreignKeyActionJobGroupLocalProgress,
                .foreign_key_action_schedule_progress = foreignKeyActionScheduleProgress,
                .foreign_key_action_schedule_group_local_progress = foreignKeyActionScheduleGroupLocalProgress,
                .foreign_key_action_schedule_mark_seeded = foreignKeyActionScheduleMarkSeeded,
                .foreign_key_action_schedule_requeue = foreignKeyActionScheduleRequeue,
                .foreign_key_action_schedule_group_local_mark_seeded = foreignKeyActionScheduleGroupLocalMarkSeeded,
                .foreign_key_action_schedule_group_local_requeue = foreignKeyActionScheduleGroupLocalRequeue,
                .unique_constraint_integrity = uniqueConstraintIntegrity,
                .unique_constraint_integrity_group_local = uniqueConstraintIntegrityGroupLocal,
                .unique_constraint_integrity_schema_controller_maintenance_pass = uniqueConstraintIntegritySchemaControllerMaintenancePass,
                .foreign_key_integrity_group_local = foreignKeyIntegrityGroupLocal,
                .foreign_key_integrity_work_unit_group_local = foreignKeyIntegrityWorkUnitGroupLocal,
                .foreign_key_ref_children_group_local = foreignKeyRefChildrenGroupLocal,
                .foreign_key_ref_children_page_group_local = foreignKeyRefChildrenPageGroupLocal,
            },
        };
    }

    fn localRuntimeStatuses(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
        items[0] = .{
            .group_id = 0,
            .stats = try self.db.runtimeStatusStatsConsistent(alloc),
        };
        return .{ .items = items };
    }

    fn graphMetricMaintenanceGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?[]u8 {
        _ = group_id;
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        return try self.db.runGraphMetricServiceMaintenanceJsonAlloc(alloc, body);
    }

    fn foreignKeyIntegrity(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        action: ForeignKeyIntegrityAction,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?ForeignKeyIntegrityResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        return try runForeignKeyIntegrityOnDb(alloc, self.db, 0, action, constraint_name, lower_doc_key, upper_doc_key, violation_limit);
    }

    fn foreignKeyIntegrityWorkerPass(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        action: ForeignKeyIntegrityAction,
        job_id: ?[]const u8,
        worker_id: []const u8,
        lease_ms: u64,
        max_work_units: usize,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?ForeignKeyIntegrityResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        if (!foreignKeyIntegrityWorkerActionSupported(action)) return error.InvalidForeignKeyIntegrityRequest;
        try startForeignKeyIntegrityJobOnDb(self.db, job_id, table_name, action, worker_id, constraint_name, lower_doc_key, upper_doc_key, lease_ms, max_work_units);

        const planned_units = try foreignKeyIntegritySingleWorkUnit(alloc, 0, action, constraint_name, lower_doc_key, upper_doc_key);
        defer {
            for (planned_units) |*unit| unit.deinit(alloc);
            alloc.free(planned_units);
        }

        var status_snapshot = try runForeignKeyIntegrityOnDb(alloc, self.db, 0, .progress, constraint_name, lower_doc_key, upper_doc_key, violation_limit);
        defer status_snapshot.deinit(alloc);
        const initial_statuses = try buildForeignKeyIntegrityWorkStatuses(alloc, planned_units, status_snapshot.progress, status_snapshot.work_claims, .pending);
        defer {
            for (initial_statuses) |*status| status.deinit(alloc);
            if (initial_statuses.len > 0) alloc.free(initial_statuses);
        }

        var aggregate: db_mod.relational_store.ForeignKeyIntegrityReport = .{};
        var group_reports = std.ArrayListUnmanaged(ForeignKeyIntegrityGroupReport).empty;
        var violations = std.ArrayListUnmanaged(ForeignKeyIntegrityViolation).empty;
        errdefer {
            group_reports.deinit(alloc);
            for (violations.items) |*violation| violation.deinit(alloc);
            violations.deinit(alloc);
        }
        var truncated = false;
        const now_ns = foreignKeyIntegrityNowNs();
        if (max_work_units > 0 and initial_statuses.len > 0 and foreignKeyIntegrityWorkStatusClaimable(initial_statuses[0], now_ns)) {
            var one = try runForeignKeyIntegrityClaimedWorkUnitOnDb(
                alloc,
                self.db,
                0,
                action,
                "child_range",
                initial_statuses[0].claim_key,
                worker_id,
                lease_ms,
                constraint_name,
                lower_doc_key,
                upper_doc_key,
                violation_limit,
            );
            defer one.deinit(alloc);
            mergeForeignKeyIntegrityReport(&aggregate, one.report);
            try group_reports.ensureUnusedCapacity(alloc, one.groups.len);
            for (one.groups) |group| group_reports.appendAssumeCapacity(group);
            const copy_count = @min(violation_limit, one.violations.len);
            try violations.ensureUnusedCapacity(alloc, copy_count);
            for (one.violations[0..copy_count]) |violation| {
                violations.appendAssumeCapacity(try cloneForeignKeyIntegrityResultViolation(alloc, violation));
            }
            truncated = one.violations_truncated or one.violations.len > violation_limit;
        }

        var final_snapshot = try runForeignKeyIntegrityOnDb(alloc, self.db, 0, .progress, constraint_name, lower_doc_key, upper_doc_key, violation_limit);
        defer final_snapshot.deinit(alloc);
        const work_units = try cloneForeignKeyIntegrityWorkUnits(alloc, planned_units);
        errdefer {
            for (work_units) |*unit| unit.deinit(alloc);
            if (work_units.len > 0) alloc.free(work_units);
        }
        const work_statuses = try buildForeignKeyIntegrityWorkStatuses(alloc, planned_units, final_snapshot.progress, final_snapshot.work_claims, .pending);
        errdefer {
            for (work_statuses) |*status| status.deinit(alloc);
            if (work_statuses.len > 0) alloc.free(work_statuses);
        }

        const valid = foreignKeyIntegrityWorkStatusesValid(work_statuses);
        const complete = !foreignKeyIntegrityWorkStatusesHaveClaimable(work_statuses, foreignKeyIntegrityNowNs());
        var result: ForeignKeyIntegrityResult = .{
            .action = action,
            .valid = valid,
            .complete = complete,
            .violation_limit = violation_limit,
            .violations_truncated = truncated,
            .report = aggregate,
            .groups = try group_reports.toOwnedSlice(alloc),
            .progress = try cloneForeignKeyIntegrityProgressSlice(alloc, final_snapshot.progress),
            .work_units = work_units,
            .work_claims = try cloneForeignKeyIntegrityWorkClaimSlice(alloc, final_snapshot.work_claims),
            .work_statuses = work_statuses,
            .violations = try violations.toOwnedSlice(alloc),
        };
        errdefer {
            var cleanup = result;
            cleanup.deinit(alloc);
        }
        try finishForeignKeyIntegrityJobOnDb(alloc, self.db, job_id, result);
        try attachForeignKeyIntegrityJobId(alloc, &result, job_id);
        try hydrateForeignKeyIntegrityResultDiagnosticsFromJobOnDb(alloc, self.db, 0, &result);
        return result;
    }

    fn foreignKeyIntegritySchemaControllerPass(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        action: ForeignKeyIntegrityAction,
        worker_id: []const u8,
        lease_ms: u64,
        max_work_units: usize,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?ForeignKeyIntegrityResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const schema_json = (try loadLocalTableSchemaJson(alloc, self.db)) orelse {
            return try emptyForeignKeyIntegrityControllerResult(alloc, action, violation_limit);
        };
        defer alloc.free(schema_json);
        return try foreignKeyIntegritySchemaControllerPassWithSchemaJson(
            alloc,
            self.source(),
            table_name,
            schema_json,
            action,
            worker_id,
            lease_ms,
            max_work_units,
            constraint_name,
            lower_doc_key,
            upper_doc_key,
            violation_limit,
        );
    }

    fn foreignKeyIntegritySchemaControllerMaintenancePass(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        options: ForeignKeyIntegritySchemaControllerOptions,
    ) !?ForeignKeyIntegritySchemaControllerResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        return try table_write_integrity.runLocalForeignKeyIntegritySchemaControllerMaintenancePass(
            alloc,
            self.source(),
            self.db,
            self.table_name,
            options,
        );
    }

    fn uniqueConstraintIntegritySchemaControllerMaintenancePass(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        options: UniqueConstraintIntegritySchemaControllerOptions,
    ) !?UniqueConstraintIntegritySchemaControllerResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        return try table_write_integrity.runLocalUniqueConstraintIntegritySchemaControllerMaintenancePass(
            alloc,
            self.source(),
            self.db,
            self.table_name,
            options,
        );
    }

    fn foreignKeyActionJobPage(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
        lease_ms: u64,
    ) !?ForeignKeyActionJobResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const record = try self.db.claimAndRunForeignKeyActionJobPageWithUpdatedParentKeyAt(
            job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
            lease_ms,
            nextTxnTimestamp(),
        );
        defer self.db.freeForeignKeyActionJobRecord(record);
        const groups = try alloc.alloc(ForeignKeyActionJobStatus, 1);
        errdefer alloc.free(groups);
        groups[0] = try foreignKeyActionJobStatusFromDbRecord(alloc, 0, record);
        errdefer groups[0].deinit(alloc);
        return .{
            .complete = groups[0].completed,
            .groups = groups,
        };
    }

    fn foreignKeyActionJobSchedule(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
        cascade_depth: u32,
        cascade_max_depth: u32,
    ) !?ForeignKeyActionJobResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const record = try self.db.scheduleForeignKeyActionJobWithUpdatedParentKeyAndCascadeLineageAt(
            job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
            cascade_depth,
            cascade_max_depth,
            nextTxnTimestamp(),
        );
        defer self.db.freeForeignKeyActionJobRecord(record);
        const groups = try alloc.alloc(ForeignKeyActionJobStatus, 1);
        errdefer alloc.free(groups);
        groups[0] = try foreignKeyActionJobStatusFromDbRecord(alloc, 0, record);
        errdefer groups[0].deinit(alloc);
        return .{
            .complete = groups[0].completed,
            .groups = groups,
        };
    }

    fn foreignKeyActionJobRequeue(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
    ) !?ForeignKeyActionJobResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const record = try self.db.requeueForeignKeyActionJobWithUpdatedParentKey(
            job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
        );
        defer self.db.freeForeignKeyActionJobRecord(record);
        const groups = try alloc.alloc(ForeignKeyActionJobStatus, 1);
        errdefer alloc.free(groups);
        groups[0] = try foreignKeyActionJobStatusFromDbRecord(alloc, 0, record);
        errdefer groups[0].deinit(alloc);
        return .{
            .complete = groups[0].completed,
            .groups = groups,
        };
    }

    fn foreignKeyActionJobGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
        lease_ms: u64,
        cascade_depth: u32,
        cascade_max_depth: u32,
    ) !?ForeignKeyActionJobStatus {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const record = try self.db.claimAndRunForeignKeyActionJobPageWithUpdatedParentKeyAndCascadeLineageAt(
            job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
            lease_ms,
            cascade_depth,
            cascade_max_depth,
            nextTxnTimestamp(),
        );
        defer self.db.freeForeignKeyActionJobRecord(record);
        return try foreignKeyActionJobStatusFromDbRecord(alloc, group_id, record);
    }

    fn foreignKeyActionJobGroupLocalRequeue(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
    ) !?ForeignKeyActionJobStatus {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const record = try self.db.requeueForeignKeyActionJobWithUpdatedParentKey(
            job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
        );
        defer self.db.freeForeignKeyActionJobRecord(record);
        return try foreignKeyActionJobStatusFromDbRecord(alloc, group_id, record);
    }

    fn foreignKeyActionJobGroupLocalSchedule(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
        cascade_depth: u32,
        cascade_max_depth: u32,
    ) !?ForeignKeyActionJobStatus {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const record = try self.db.scheduleForeignKeyActionJobWithUpdatedParentKeyAndCascadeLineageAt(
            job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
            cascade_depth,
            cascade_max_depth,
            nextTxnTimestamp(),
        );
        defer self.db.freeForeignKeyActionJobRecord(record);
        return try foreignKeyActionJobStatusFromDbRecord(alloc, group_id, record);
    }

    fn foreignKeyActionJobProgress(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?ForeignKeyActionJobProgressResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const records = try self.db.listForeignKeyActionJobRecords();
        defer self.db.freeForeignKeyActionJobRecords(records);
        return try foreignKeyActionJobProgressFromDbRecords(alloc, 0, records);
    }

    fn foreignKeyActionJobGroupLocalProgress(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
    ) !?ForeignKeyActionJobProgressResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const records = try self.db.listForeignKeyActionJobRecords();
        defer self.db.freeForeignKeyActionJobRecords(records);
        return try foreignKeyActionJobProgressFromDbRecords(alloc, group_id, records);
    }

    fn foreignKeyActionScheduleProgress(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?ForeignKeyActionScheduleProgressResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const records = try self.db.listForeignKeyActionScheduleRecords();
        defer self.db.freeForeignKeyActionScheduleRecords(records);
        return try foreignKeyActionScheduleProgressFromDbRecords(alloc, 0, records);
    }

    fn foreignKeyActionScheduleGroupLocalProgress(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
    ) !?ForeignKeyActionScheduleProgressResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const records = try self.db.listForeignKeyActionScheduleRecords();
        defer self.db.freeForeignKeyActionScheduleRecords(records);
        return try foreignKeyActionScheduleProgressFromDbRecords(alloc, group_id, records);
    }

    fn foreignKeyActionScheduleMarkSeeded(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        schedule_id: []const u8,
        scheduled_groups: u64,
    ) !?ForeignKeyActionScheduleStatus {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const record = try self.db.markForeignKeyActionScheduleSeeded(schedule_id, scheduled_groups);
        defer self.db.freeForeignKeyActionScheduleRecord(record);
        return try foreignKeyActionScheduleStatusFromDbRecord(alloc, 0, record);
    }

    fn foreignKeyActionScheduleRequeue(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        schedule_id: []const u8,
        action_job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
    ) !?ForeignKeyActionScheduleStatus {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const record = try self.db.requeueForeignKeyActionScheduleWithUpdatedParentKeyAt(
            schedule_id,
            action_job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
            nextTxnTimestamp(),
        );
        defer self.db.freeForeignKeyActionScheduleRecord(record);
        return try foreignKeyActionScheduleStatusFromDbRecord(alloc, 0, record);
    }

    fn foreignKeyActionScheduleGroupLocalMarkSeeded(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        schedule_id: []const u8,
        scheduled_groups: u64,
    ) !?ForeignKeyActionScheduleStatus {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const record = try self.db.markForeignKeyActionScheduleSeeded(schedule_id, scheduled_groups);
        defer self.db.freeForeignKeyActionScheduleRecord(record);
        return try foreignKeyActionScheduleStatusFromDbRecord(alloc, group_id, record);
    }

    fn foreignKeyActionScheduleGroupLocalRequeue(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        schedule_id: []const u8,
        action_job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
    ) !?ForeignKeyActionScheduleStatus {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const record = try self.db.requeueForeignKeyActionScheduleWithUpdatedParentKeyAt(
            schedule_id,
            action_job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
            nextTxnTimestamp(),
        );
        defer self.db.freeForeignKeyActionScheduleRecord(record);
        return try foreignKeyActionScheduleStatusFromDbRecord(alloc, group_id, record);
    }

    fn uniqueConstraintIntegrity(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        action: UniqueConstraintIntegrityAction,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
    ) !?UniqueConstraintIntegrityResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        return try runUniqueConstraintIntegrityOnDb(alloc, self.db, 0, action, lower_doc_key, upper_doc_key);
    }

    fn foreignKeyIntegrityGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        action: ForeignKeyIntegrityAction,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?ForeignKeyIntegrityResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        return try runForeignKeyIntegrityOnDb(alloc, self.db, group_id, action, constraint_name, lower_doc_key, upper_doc_key, violation_limit);
    }

    fn foreignKeyIntegrityWorkUnitGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        action: ForeignKeyIntegrityAction,
        phase: []const u8,
        job_id: ?[]const u8,
        claim_key: []const u8,
        worker_id: []const u8,
        lease_ms: u64,
        max_work_units: usize,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?ForeignKeyIntegrityResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        try startForeignKeyIntegrityJobOnDb(self.db, job_id, table_name, action, worker_id, constraint_name, lower_doc_key, upper_doc_key, lease_ms, max_work_units);
        var result = try runForeignKeyIntegrityClaimedWorkUnitOnDb(alloc, self.db, group_id, action, phase, claim_key, worker_id, lease_ms, constraint_name, lower_doc_key, upper_doc_key, violation_limit);
        errdefer result.deinit(alloc);
        try finishForeignKeyIntegrityJobOnDb(alloc, self.db, job_id, result);
        try attachForeignKeyIntegrityJobId(alloc, &result, job_id);
        try hydrateForeignKeyIntegrityResultDiagnosticsFromJobOnDb(alloc, self.db, group_id, &result);
        return result;
    }

    fn uniqueConstraintIntegrityGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        action: UniqueConstraintIntegrityAction,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
    ) !?UniqueConstraintIntegrityResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        return try runUniqueConstraintIntegrityOnDb(alloc, self.db, group_id, action, lower_doc_key, upper_doc_key);
    }

    fn foreignKeyRefChildrenGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        limit: usize,
    ) !?[]db_mod.types.ForeignKeyRefChild {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        return try self.db.listForeignKeyRefChildrenForParent(alloc, constraint_name, parent_table, parent_key, limit);
    }

    fn foreignKeyRefChildrenPageGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        start_after_child_table: ?[]const u8,
        start_after_child_key: ?[]const u8,
        limit: usize,
    ) !?db_mod.types.ForeignKeyRefChildrenPage {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        return try self.db.listForeignKeyRefChildrenPageForParent(alloc, constraint_name, parent_table, parent_key, start_after_child_table, start_after_child_key, limit);
    }

    fn corruptEmbeddingArtifact(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        index_name: []const u8,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        if (!try corruptEmbeddingArtifactInDb(alloc, self.db, doc_key, index_name)) return error.NotFound;
    }

    fn createTable(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: tables_api.CreateTableRequest,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;

        const raw_indexes_json = req.indexes_json orelse tables_api.default_indexes_json;
        const schema_json = tables_api.effectiveSchemaJson(req.schema_json);
        const expanded_indexes_json = try tables_api.prepareTableIndexesForSchemaAlloc(alloc, table_name, raw_indexes_json, schema_json);
        defer alloc.free(expanded_indexes_json);
        const indexes_json = expanded_indexes_json;
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
        defer parsed.deinit();
        const object = switch (parsed.value) {
            .object => |object| object,
            else => return error.InvalidCreateTableRequest,
        };

        var it = object.iterator();
        while (it.next()) |entry| {
            if (isReservedTableIndexMetadataEntry(entry.key_ptr.*)) continue;
            const kind = try parseIndexKind(entry.value_ptr.*);
            const config_json = try extractIndexConfigJson(alloc, entry.key_ptr.*, entry.value_ptr.*);
            defer alloc.free(config_json);
            try self.db.addIndex(.{
                .name = entry.key_ptr.*,
                .kind = kind,
                .config_json = config_json,
            });
        }

        try applyLocalTableSchemaJson(alloc, self.db, schema_json);
    }

    fn updateSchema(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        schema_json: []const u8,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try applyLocalTableSchemaJson(alloc, self.db, schema_json);
    }

    fn batch(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try validateTableBatchAgainstLocalSchema(alloc, self.db, req.writes, req.deletes, req.transforms);
        self.db.batch(req) catch |err| return normalizeRelationalConstraintError(err);
    }

    fn mutateRowsFromSource(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        schema: storage_schema.TableSchema,
        req: db_mod.types.RelationalRowsMutationSourceRequest,
    ) !?db_mod.types.RelationalRowsMutationSourceResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        return try self.db.mutateRelationalRowsFromSource(alloc, schema, req);
    }

    fn mutateRowsFromSourceAutocommit(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        schema: storage_schema.TableSchema,
        req: db_mod.types.RelationalRowsMutationSourceRequest,
        _: db_mod.types.SyncLevel,
    ) !?db_mod.types.RelationalRowsMutationSourceResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        return try mutateRowsFromSourceAutocommitOnDb(alloc, self.db, schema, req);
    }

    fn mutateRowsJoinedFromSourceRows(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        target_schema: storage_schema.TableSchema,
        source_schema: storage_schema.TableSchema,
        req: db_mod.types.RelationalRowsJoinedMutationSourceRequest,
        source_rows: []const []const u8,
    ) !?db_mod.types.RelationalRowsMutationSourceResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;

        return try mutateRowsJoinedFromSourceRowsOnDb(alloc, self.db, target_schema, source_schema, req, source_rows);
    }

    fn mutateRowsJoinedFromSourceRowsAutocommit(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        target_schema: storage_schema.TableSchema,
        source_schema: storage_schema.TableSchema,
        req: db_mod.types.RelationalRowsJoinedMutationSourceRequest,
        source_rows: []const []const u8,
        _: db_mod.types.SyncLevel,
    ) !?db_mod.types.RelationalRowsMutationSourceResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        return try mutateRowsJoinedFromSourceRowsAutocommitOnDb(alloc, self.db, target_schema, source_schema, req, source_rows);
    }

    fn mergeRowsFromSourceRows(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        target_schema: storage_schema.TableSchema,
        source_schema: storage_schema.TableSchema,
        plan: sql_adapter.LoweredMergeMutationPlan,
        source_rows: []const []const u8,
    ) !?relational_rows_api.OwnedRowsBatchRequest {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;

        return try mergeRowsFromSourceRowsOnDb(alloc, self.db, target_schema, source_schema, plan, source_rows, normalizeRelationalConstraintError);
    }

    fn beginBulkIngest(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        table_name: []const u8,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try self.db.beginBulkIngestSession();
    }

    fn finishBulkIngest(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        table_name: []const u8,
        options: backend_types.BulkIngestFinishOptions,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try self.db.finishBulkIngestSessionWithOptions(options);
    }

    fn abortBulkIngest(ptr: *anyopaque, table_name: []const u8) void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return;
        self.db.abortBulkIngestSession();
    }

    fn backupTable(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        plan: backups_api.TableBackupPlan,
    ) !?[]backups_api.ShardSnapshot {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        return try table_write_backup_restore.backupLocalTable(alloc, self.db, plan);
    }

    fn restoreTable(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        plan: backups_api.TableRestorePlan,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try table_write_backup_restore.restoreLocalTable(alloc, self.db, plan);
    }

    fn commitTransaction(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        tables: []const distributed_txn.TableCommitRequest,
        sync_level: db_mod.types.SyncLevel,
    ) !?distributed_txn.CommitOutcome {
        const txn_id = nextTxnId();
        return try commitTransactionWithId(ptr, alloc, txn_id, nextTxnTimestamp(), tables, sync_level);
    }

    fn commitTransactionWithId(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        begin_timestamp: u64,
        tables: []const distributed_txn.TableCommitRequest,
        _: db_mod.types.SyncLevel,
    ) !?distributed_txn.CommitOutcome {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (tables.len != 1) return error.UnsupportedOperation;
        const table = tables[0];
        if (!std.mem.eql(u8, self.table_name, table.table_name)) return null;
        try validateTransactionAgainstLocalSchema(alloc, self.db, table.writes, table.deletes, table.transforms);

        const commit_version = begin_timestamp + 1;

        _ = try self.db.beginTransactionWithIdAndParticipants(txn_id, begin_timestamp, &.{});
        self.db.writeTransaction(txn_id, .{
            .writes = table.writes,
            .deletes = table.deletes,
            .transforms = table.transforms,
            .predicates = table.predicates,
        }) catch |err| switch (err) {
            error.VersionConflict, error.IntentConflict => {
                self.db.resolveTransactionIntents(txn_id, .aborted, commit_version) catch {};
                return .{ .conflict = boundConflict(table, err) };
            },
            else => return normalizeRelationalConstraintError(err),
        };
        self.db.resolveTransactionIntents(txn_id, .committed, commit_version) catch |err| {
            return normalizeRelationalConstraintError(err);
        };
        return .{ .committed = .{ .participant_count = 1 } };
    }

    fn createIndex(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
        index_json: []const u8,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        const schema_json = try loadLocalTableSchemaJson(alloc, self.db);
        defer if (schema_json) |value| alloc.free(value);
        const expanded_index_json = try tables_api.expandSchemaDerivedAlgebraicIndexAlloc(alloc, table_name, index_json, tables_api.effectiveSchemaJson(schema_json));
        defer alloc.free(expanded_index_json);
        const cfg = try parseIndexConfig(alloc, index_name, expanded_index_json);
        defer {
            alloc.free(cfg.name);
            alloc.free(cfg.config_json);
        }
        try self.db.addIndex(cfg);
    }

    fn dropIndex(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        _ = try self.db.deleteIndex(index_name);
    }

    fn graphMetricAction(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
        metric_name: []const u8,
        action: []const u8,
    ) !?db_mod.types.GraphMetricStatus {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        if (std.mem.eql(u8, action, "refresh")) {
            return try self.db.refreshGraphMetric(alloc, index_name, metric_name);
        }
        if (std.mem.eql(u8, action, "rebuild")) {
            return try self.db.rebuildGraphMetric(alloc, index_name, metric_name);
        }
        if (std.mem.eql(u8, action, "delete")) {
            return try self.db.deleteGraphMetricMaterialization(alloc, index_name, metric_name);
        }
        if (std.mem.eql(u8, action, "pause")) {
            return try self.db.pauseGraphMetricMaintenance(alloc, index_name, metric_name);
        }
        if (std.mem.eql(u8, action, "resume")) {
            return try self.db.resumeGraphMetricMaintenance(alloc, index_name, metric_name);
        }
        return error.InvalidGraphMetricAction;
    }

    fn reprocessDocumentArtifact(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
    ) !?bool {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        return try self.db.reprocessDocumentArtifact(alloc, doc_key, artifact_name);
    }

    fn reprocessDocumentArtifactRange(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        artifact_name: []const u8,
        req: db_mod.types.DocumentArtifactTableReprocessRequest,
    ) !?db_mod.types.DocumentArtifactTableReprocessResult {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        return try self.db.reprocessDocumentArtifactRange(alloc, artifact_name, req);
    }

    fn batchGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) !?void {
        return try batch(ptr, alloc, table_name, req);
    }

    fn txnBeginGroupLocal(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        begin_timestamp: u64,
        _: u64,
        participants: []const []const u8,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        _ = try self.db.beginTransactionWithIdAndParticipants(txn_id, begin_timestamp, participants);
    }

    fn txnPrepareGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        _: u64,
        req: db_mod.types.TransactionIntentRequest,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        try validateTransactionAgainstLocalSchema(alloc, self.db, req.writes, req.deletes, req.transforms);
        self.db.writeTransaction(txn_id, req) catch |err| return normalizeRelationalConstraintError(err);
    }

    fn txnResolveGroupLocal(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
        status: db_mod.types.TxnStatus,
        commit_version: u64,
    ) !?void {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        self.db.resolveTransactionIntents(txn_id, status, commit_version) catch |err| {
            return normalizeRelationalConstraintError(err);
        };
        const participant = try std.fmt.allocPrint(self.db.alloc, "group:{d}", .{group_id});
        defer self.db.alloc.free(participant);
        try self.db.markTransactionParticipantResolved(txn_id, participant);
    }

    fn txnStatusGroupLocal(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        _: u64,
        table_name: []const u8,
        txn_id: db_mod.types.TxnId,
    ) !?db_mod.types.TxnStatus {
        const self: *BoundTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, self.table_name, table_name)) return null;
        return try self.db.getTransactionStatus(txn_id);
    }
};

test "provisioned table write source serializes same-table same-group operations" {
    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-table-activity", NoCatalog.iface());
    source.beginGroupOperation("docs", 7001);
    defer source.endGroupOperation("docs", 7001);

    try std.testing.expect(!source.tryBeginGroupOperation("docs", 7001));
}

test "api.table_writes.docid provisioned table write source routes same-owner identity rewrites and rejects cross-owner rewrites" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-identity-rewrite-routing";

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data" }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const Recorder = struct {
        calls: usize = 0,
        group_id: u64 = 0,
        identity_rewrites: usize = 0,

        fn batcher(self: *@This()) RaftBatcher {
            return .{
                .ptr = self,
                .vtable = &.{
                    .batch_group = batchGroup,
                    .batch_group_local = batchGroupLocal,
                },
            };
        }

        fn batchGroup(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: db_mod.types.BatchRequest) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqual(@as(usize, 0), req.writes.len);
            try std.testing.expectEqual(@as(usize, 0), req.deletes.len);
            try std.testing.expectEqual(@as(usize, 1), req.relational_identity_rewrites.len);
            try std.testing.expectEqualStrings("doc:a", req.relational_identity_rewrites[0].old_key);
            try std.testing.expectEqualStrings("doc:b", req.relational_identity_rewrites[0].new_key);
            self.calls += 1;
            self.group_id = group_id;
            self.identity_rewrites = req.relational_identity_rewrites.len;
        }

        fn batchGroupLocal(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.BatchRequest) anyerror!void {
            return error.TestUnexpectedResult;
        }
    };

    var recorder = Recorder{};
    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    defer source.deinit();
    _ = source.withRaftBatcher(recorder.batcher());

    _ = try source.source().batch(alloc, "docs", .{
        .relational_identity_rewrites = &.{.{
            .old_key = "doc:a",
            .new_key = "doc:b",
            .value = "{\"id\":\"doc:b\"}",
        }},
    });
    try std.testing.expectEqual(@as(usize, 1), recorder.calls);
    try std.testing.expectEqual(@as(u64, 7001), recorder.group_id);
    try std.testing.expectEqual(@as(usize, 1), recorder.identity_rewrites);

    try std.testing.expectError(error.UnsupportedOperation, source.source().batch(alloc, "docs", .{
        .relational_identity_rewrites = &.{.{
            .old_key = "doc:a",
            .new_key = "doc:z",
            .value = "{\"id\":\"doc:z\"}",
        }},
    }));
}

const ProvisionedWriteCoalesceTestCatalog = struct {
    fn iface() table_catalog.CatalogSource {
        return .{
            .ptr = undefined,
            .vtable = &.{
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
            },
        };
    }

    fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
        return .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                .table_id = 7,
                .name = "docs",
                .placement_role = "data",
                .indexes_json = tables_api.default_indexes_json,
            }})[0..]),
            .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                .group_id = 7001,
                .table_id = 7,
                .start_key = "",
                .end_key = null,
            }})[0..]),
            .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
            .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
            .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
            .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
        };
    }

    fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
};

const ProvisionedWriteCoalesceProbe = struct {
    calls: std.atomic.Value(u32) = .init(0),
    first_entered: std.atomic.Value(bool) = .init(false),
    release_first: std.atomic.Value(bool) = .init(false),

    fn beforeBatch(ptr: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const call = self.calls.fetchAdd(1, .acq_rel) + 1;
        if (call == 1) {
            self.first_entered.store(true, .release);
            while (!self.release_first.load(.acquire)) std.atomic.spinLoopHint();
        }
    }
};

const ProvisionedWriteCoalesceBatchWorker = struct {
    source: *ProvisionedTableWriteSource,
    key: []const u8,
    value: []const u8,
    err: ?anyerror = null,

    fn run(self: *@This()) void {
        _ = self.source.source().batch(std.heap.page_allocator, "docs", .{
            .writes = &.{.{ .key = self.key, .value = self.value }},
            .sync_level = .write,
        }) catch |err| {
            self.err = err;
        };
    }
};

test "api.table_writes.docid provisioned table write source coalesces same-group waiters" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-batch-coalesce-waiters");
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(path, ProvisionedWriteCoalesceTestCatalog.iface());
    defer source.deinit();
    source.write_cache = &write_cache;

    _ = try source.source().createTable(alloc, "docs", .{});

    var probe = ProvisionedWriteCoalesceProbe{};
    test_before_batch_execution_hook = .{ .ptr = &probe, .run = ProvisionedWriteCoalesceProbe.beforeBatch };
    defer test_before_batch_execution_hook = null;

    var first = ProvisionedWriteCoalesceBatchWorker{ .source = &source, .key = "doc:a", .value = "{\"title\":\"alpha\"}" };
    const first_thread = try std.Thread.spawn(.{}, ProvisionedWriteCoalesceBatchWorker.run, .{&first});
    while (!probe.first_entered.load(.acquire)) std.atomic.spinLoopHint();

    var second = ProvisionedWriteCoalesceBatchWorker{ .source = &source, .key = "doc:b", .value = "{\"title\":\"beta\"}" };
    var third = ProvisionedWriteCoalesceBatchWorker{ .source = &source, .key = "doc:c", .value = "{\"title\":\"gamma\"}" };
    const second_thread = try std.Thread.spawn(.{}, ProvisionedWriteCoalesceBatchWorker.run, .{&second});
    const third_thread = try std.Thread.spawn(.{}, ProvisionedWriteCoalesceBatchWorker.run, .{&third});

    try source.testingWaitForWriteCoalesceQueueEntries("docs", 7001, 2);
    probe.release_first.store(true, .release);

    first_thread.join();
    second_thread.join();
    third_thread.join();
    if (first.err) |err| return err;
    if (second.err) |err| return err;
    if (third.err) |err| return err;

    try std.testing.expectEqual(@as(u32, 2), probe.calls.load(.acquire));

    var cached = try source.getOrOpenCachedDbMode(alloc, &write_cache, db_path, 7001, "docs", .default, null, null);
    defer cached.deinit(alloc);
    try drainManagedDbBeforeClose(cached.db);
    var beta = (try cached.db.lookup(alloc, "doc:b", .{})).?;
    defer beta.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, beta.json, "\"beta\"") != null);
    var gamma = (try cached.db.lookup(alloc, "doc:c", .{})).?;
    defer gamma.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, gamma.json, "\"gamma\"") != null);
}

test "api.table_writes.docid provisioned table write coalescer isolates failed waiters" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-batch-coalesce-failure-isolation");
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(path, ProvisionedWriteCoalesceTestCatalog.iface());
    defer source.deinit();
    source.write_cache = &write_cache;

    _ = try source.source().createTable(alloc, "docs", .{});

    var probe = ProvisionedWriteCoalesceProbe{};
    test_before_batch_execution_hook = .{ .ptr = &probe, .run = ProvisionedWriteCoalesceProbe.beforeBatch };
    defer test_before_batch_execution_hook = null;

    var first = ProvisionedWriteCoalesceBatchWorker{ .source = &source, .key = "doc:a", .value = "{\"title\":\"alpha\"}" };
    const first_thread = try std.Thread.spawn(.{}, ProvisionedWriteCoalesceBatchWorker.run, .{&first});
    while (!probe.first_entered.load(.acquire)) std.atomic.spinLoopHint();

    var invalid = ProvisionedWriteCoalesceBatchWorker{ .source = &source, .key = "doc:b", .value = "{\"title\":" };
    var valid = ProvisionedWriteCoalesceBatchWorker{ .source = &source, .key = "doc:c", .value = "{\"title\":\"gamma\"}" };
    const invalid_thread = try std.Thread.spawn(.{}, ProvisionedWriteCoalesceBatchWorker.run, .{&invalid});
    const valid_thread = try std.Thread.spawn(.{}, ProvisionedWriteCoalesceBatchWorker.run, .{&valid});

    try source.testingWaitForWriteCoalesceQueueEntries("docs", 7001, 2);
    probe.release_first.store(true, .release);

    first_thread.join();
    invalid_thread.join();
    valid_thread.join();
    if (first.err) |err| return err;
    try std.testing.expect(invalid.err != null);
    if (valid.err) |err| return err;

    var cached = try source.getOrOpenCachedDbMode(alloc, &write_cache, db_path, 7001, "docs", .default, null, null);
    defer cached.deinit(alloc);
    try drainManagedDbBeforeClose(cached.db);
    try std.testing.expect((try cached.db.lookup(alloc, "doc:b", .{})) == null);
    var gamma = (try cached.db.lookup(alloc, "doc:c", .{})).?;
    defer gamma.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, gamma.json, "\"gamma\"") != null);
}

test "provisioned table write source rejects writes that violate enforced document schemas" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-batch-schema");
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);
    var db = try db_mod.DB.open(alloc, db_path, .{});
    defer db.close();

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data", .schema_json = "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}" }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    defer source.deinit();
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"unexpected\"}" }},
    }));
}

fn parseJsonBodyIgnoreUnknown(comptime T: type, alloc: std.mem.Allocator, body: []const u8) !std.json.Parsed(T) {
    return try std.json.parseFromSlice(T, alloc, body, .{ .ignore_unknown_fields = true });
}

fn jsonValueContainsText(value: std.json.Value, needle: []const u8) bool {
    switch (value) {
        .string => |text| return std.mem.indexOf(u8, text, needle) != null,
        .array => |items| {
            for (items.items) |item| {
                if (jsonValueContainsText(item, needle)) return true;
            }
            return false;
        },
        .object => |obj| {
            var it = obj.iterator();
            while (it.next()) |entry| {
                if (std.mem.indexOf(u8, entry.key_ptr.*, needle) != null) return true;
                if (jsonValueContainsText(entry.value_ptr.*, needle)) return true;
            }
            return false;
        },
        else => return false,
    }
}

const TestEmbeddingRequest = struct {
    model: std.json.Value,
    input: std.json.Value,
};

test "provisioned table write source drains managed dense enrichment before close" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-managed-dense-drain");
    defer alloc.free(path);

    const FakeEmbeddingProvider = struct {
        fn executor() http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, arena: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/v1/embeddings"));

            var parsed_req = try parseJsonBodyIgnoreUnknown(TestEmbeddingRequest, arena, req.body);
            defer parsed_req.deinit();

            const vector = if (jsonValueContainsText(parsed_req.value.input, "alpha body"))
                "[1,0,0]"
            else
                "[0,0,1]";

            const body = try std.fmt.allocPrint(
                arena,
                "{{\"object\":\"list\",\"data\":[{{\"object\":\"embedding\",\"index\":0,\"embedding\":{s}}}],\"model\":\"test-embed\",\"usage\":{{\"prompt_tokens\":1,\"total_tokens\":1}}}}",
                .{vector},
            );
            return .{
                .status = 200,
                .content_type = try arena.dupe(u8, "application/json"),
                .body = body,
            };
        }
    };

    const FakeCatalog = struct {
        var indexes_json_buf: []const u8 = "";
        var table_records = [_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .placement_role = "data",
        }};
        var range_records = [_]metadata_table_manager.RangeRecord{.{
            .group_id = 7001,
            .table_id = 7,
            .start_key = "",
            .end_key = null,
        }};

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            table_records[0].indexes_json = indexes_json_buf;
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = table_records[0..],
                .ranges = range_records[0..],
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var listener = std_http_listener.StdHttpListener.init(alloc, .{}, FakeEmbeddingProvider.executor());
    defer listener.deinit();
    try listener.start();
    const base_uri = try listener.baseUri(alloc);
    defer alloc.free(base_uri);

    FakeCatalog.indexes_json_buf = try std.fmt.allocPrint(alloc,
        \\{{"semantic_idx":{{"type":"embeddings","field":"body","dimension":3,"embedder":{{"provider":"openai","model":"test-embed","url":"{s}"}}}}}}
    , .{base_uri});
    defer alloc.free(FakeCatalog.indexes_json_buf);

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    defer source.deinit();
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha body\"}" }},
    });

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);
    var reopened = try db_mod.DB.open(alloc, db_path, .{});
    defer reopened.close();

    try std.testing.expect(reopened.core.index_manager.denseIndex("semantic_idx").?.index.metadata.active_count > 0);

    const query_vec = [_]f32{ 1, 0, 0 };
    var result = try reopened.search(alloc, .{
        .index_name = "semantic_idx",
        .dense = .{
            .vector = query_vec[0..],
            .k = 1,
        },
        .limit = 1,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "provisioned table write cache eventually runs managed dense enrichment for write sync" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-managed-dense-write-cache");
    defer alloc.free(path);

    const FakeEmbeddingProvider = struct {
        var request_count: std.atomic.Value(u32) = .init(0);

        fn executor() http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, arena: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/v1/embeddings"));
            _ = request_count.fetchAdd(1, .monotonic);

            var parsed_req = try parseJsonBodyIgnoreUnknown(TestEmbeddingRequest, arena, req.body);
            defer parsed_req.deinit();

            const vector = if (jsonValueContainsText(parsed_req.value.input, "alpha body"))
                "[1,0,0]"
            else
                "[0,0,1]";

            const body = try std.fmt.allocPrint(
                arena,
                "{{\"object\":\"list\",\"data\":[{{\"object\":\"embedding\",\"index\":0,\"embedding\":{s}}}],\"model\":\"test-embed\",\"usage\":{{\"prompt_tokens\":1,\"total_tokens\":1}}}}",
                .{vector},
            );
            return .{
                .status = 200,
                .content_type = try arena.dupe(u8, "application/json"),
                .body = body,
            };
        }
    };

    const FakeCatalog = struct {
        var indexes_json_buf: []const u8 = "";
        var table_records = [_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .placement_role = "data",
        }};
        var range_records = [_]metadata_table_manager.RangeRecord{.{
            .group_id = 7001,
            .table_id = 7,
            .start_key = "",
            .end_key = null,
        }};

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            table_records[0].indexes_json = indexes_json_buf;
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = table_records[0..],
                .ranges = range_records[0..],
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var listener = std_http_listener.StdHttpListener.init(alloc, .{}, FakeEmbeddingProvider.executor());
    defer listener.deinit();
    try listener.start();
    const base_uri = try listener.baseUri(alloc);
    defer alloc.free(base_uri);

    FakeCatalog.indexes_json_buf = try std.fmt.allocPrint(alloc,
        \\{{"semantic_idx":{{"type":"embeddings","field":"body","dimension":3,"embedder":{{"provider":"openai","model":"test-embed","url":"{s}"}}}}}}
    , .{base_uri});
    defer alloc.free(FakeCatalog.indexes_json_buf);

    FakeEmbeddingProvider.request_count.store(0, .monotonic);

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    defer source.deinit();
    source.write_cache = &write_cache;
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha body\"}" }},
        .sync_level = .write,
    });

    var attempts: usize = 0;
    while (attempts < 100 and FakeEmbeddingProvider.request_count.load(.monotonic) == 0) : (attempts += 1) {
        sleepNs(50 * std.time.ns_per_ms);
    }

    try std.testing.expect(FakeEmbeddingProvider.request_count.load(.monotonic) > 0);
}

test "provisioned write cache invalidation closes failed managed enrichment db without aborting" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-managed-dense-write-cache-failed-close");
    defer alloc.free(path);

    const FakeEmbeddingProvider = struct {
        var request_count: std.atomic.Value(u32) = .init(0);
        var failed_response_count: std.atomic.Value(u32) = .init(0);

        fn executor() http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, arena: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/v1/embeddings"));

            var parsed_req = try parseJsonBodyIgnoreUnknown(TestEmbeddingRequest, arena, req.body);
            defer parsed_req.deinit();

            _ = request_count.fetchAdd(1, .monotonic);
            _ = failed_response_count.fetchAdd(1, .monotonic);
            const body = try arena.dupe(u8,
                \\{"object":"list","data":[],"model":"test-embed","usage":{"prompt_tokens":1,"total_tokens":1}}
            );
            return .{
                .status = 200,
                .content_type = try arena.dupe(u8, "application/json"),
                .body = body,
            };
        }
    };

    const FakeCatalog = struct {
        var indexes_json_buf: []const u8 = "";
        var table_records = [_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .placement_role = "data",
        }};
        var range_records = [_]metadata_table_manager.RangeRecord{.{
            .group_id = 7001,
            .table_id = 7,
            .start_key = "",
            .end_key = null,
        }};

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            table_records[0].indexes_json = indexes_json_buf;
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = table_records[0..],
                .ranges = range_records[0..],
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var listener = std_http_listener.StdHttpListener.init(alloc, .{}, FakeEmbeddingProvider.executor());
    defer listener.deinit();
    try listener.start();
    const base_uri = try listener.baseUri(alloc);
    defer alloc.free(base_uri);

    FakeCatalog.indexes_json_buf = try std.fmt.allocPrint(alloc,
        \\{{"semantic_idx":{{"type":"embeddings","field":"body","dimension":3,"embedder":{{"provider":"openai","model":"test-embed","url":"{s}"}}}}}}
    , .{base_uri});
    defer alloc.free(FakeCatalog.indexes_json_buf);

    FakeEmbeddingProvider.request_count.store(0, .monotonic);
    FakeEmbeddingProvider.failed_response_count.store(0, .monotonic);

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    defer source.deinit();
    source.write_cache = &write_cache;
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"alpha body\"}" },
            .{ .key = "doc:b", .value = "{\"body\":\"beta body\"}" },
        },
        .sync_level = .write,
    });

    var attempts: usize = 0;
    while (attempts < 100 and FakeEmbeddingProvider.failed_response_count.load(.monotonic) == 0) : (attempts += 1) {
        sleepNs(50 * std.time.ns_per_ms);
    }

    try std.testing.expect(FakeEmbeddingProvider.failed_response_count.load(.monotonic) > 0);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);

    source.invalidateWriteCache("docs");

    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items.len);
}

test "api.table_writes.query_visibility table write source invalidates cached query db after managed dense replay becomes visible" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-managed-dense-query-visibility");
    defer alloc.free(path);

    const FakeEmbeddingProvider = struct {
        var request_count: std.atomic.Value(u32) = .init(0);
        var rate_limited_count: std.atomic.Value(u32) = .init(0);
        var allow_all: std.atomic.Value(bool) = .init(false);

        fn vectorForInput(input: std.json.Value) []const u8 {
            if (jsonValueContainsText(input, "alpha")) return "[1,0,0]";
            if (jsonValueContainsText(input, "beta")) return "[0,1,0]";
            return "[0,0,1]";
        }

        fn appendEmbedding(
            arena: std.mem.Allocator,
            out: *std.ArrayListUnmanaged(u8),
            index: usize,
            input: std.json.Value,
        ) !void {
            if (index != 0) try out.append(arena, ',');
            const entry = try std.fmt.allocPrint(
                arena,
                "{{\"object\":\"embedding\",\"index\":{d},\"embedding\":{s}}}",
                .{ index, vectorForInput(input) },
            );
            defer arena.free(entry);
            try out.appendSlice(arena, entry);
        }

        fn responseBody(arena: std.mem.Allocator, input: std.json.Value) ![]u8 {
            var out = std.ArrayListUnmanaged(u8).empty;
            try out.appendSlice(arena, "{\"object\":\"list\",\"data\":[");
            switch (input) {
                .array => |items| {
                    for (items.items, 0..) |item, index| {
                        try appendEmbedding(arena, &out, index, item);
                    }
                },
                else => try appendEmbedding(arena, &out, 0, input),
            }
            try out.appendSlice(arena, "],\"model\":\"test-embed\",\"usage\":{\"prompt_tokens\":1,\"total_tokens\":1}}");
            return try out.toOwnedSlice(arena);
        }

        fn executor() http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, arena: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/v1/embeddings"));

            var parsed_req = try parseJsonBodyIgnoreUnknown(TestEmbeddingRequest, arena, req.body);
            defer parsed_req.deinit();

            const request_index = request_count.fetchAdd(1, .monotonic);
            if (request_index != 0 and !allow_all.load(.acquire)) {
                _ = rate_limited_count.fetchAdd(1, .monotonic);
                const body = try arena.dupe(u8,
                    \\{"error":{"message":"rate limited","type":"rate_limit_exceeded"}}
                );
                return .{
                    .status = 429,
                    .content_type = try arena.dupe(u8, "application/json"),
                    .body = body,
                };
            }

            return .{
                .status = 200,
                .content_type = try arena.dupe(u8, "application/json"),
                .body = try responseBody(arena, parsed_req.value.input),
            };
        }

        fn allowAll() void {
            allow_all.store(true, .release);
        }
    };

    const FakeCatalog = struct {
        var indexes_json_buf: []const u8 = "";
        var table_records = [_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .placement_role = "data",
        }};
        var range_records = [_]metadata_table_manager.RangeRecord{.{
            .group_id = 7001,
            .table_id = 7,
            .start_key = "",
            .end_key = null,
        }};

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            table_records[0].indexes_json = indexes_json_buf;
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = table_records[0..],
                .ranges = range_records[0..],
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var listener = std_http_listener.StdHttpListener.init(alloc, .{}, FakeEmbeddingProvider.executor());
    defer listener.deinit();
    try listener.start();
    const base_uri = try listener.baseUri(alloc);
    defer alloc.free(base_uri);

    const managed_indexes_json_buf = try std.fmt.allocPrint(alloc,
        \\{{"semantic_idx":{{"type":"embeddings","field":"body","dimension":3,"embedder":{{"provider":"openai","model":"test-embed","url":"{s}"}}}}}}
    , .{base_uri});
    defer alloc.free(managed_indexes_json_buf);
    FakeCatalog.indexes_json_buf = managed_indexes_json_buf;

    FakeEmbeddingProvider.request_count.store(0, .monotonic);
    FakeEmbeddingProvider.rate_limited_count.store(0, .monotonic);
    FakeEmbeddingProvider.allow_all.store(false, .monotonic);

    var read_cache = table_read_cache.ProvisionedTableReadCache.init(alloc);
    defer read_cache.deinit();

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    defer source.deinit();
    source.read_cache = &read_cache;
    source.write_cache = &write_cache;

    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha body\"}" }},
        .sync_level = .write,
    });

    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{
            .{ .key = "doc:b", .value = "{\"body\":\"beta body\"}" },
            .{ .key = "doc:c", .value = "{\"body\":\"gamma body\"}" },
        },
        .sync_level = .write,
    });

    var attempts: usize = 0;
    while (attempts < 100 and FakeEmbeddingProvider.rate_limited_count.load(.monotonic) == 0) : (attempts += 1) {
        sleepNs(50 * std.time.ns_per_ms);
    }
    try std.testing.expect(FakeEmbeddingProvider.rate_limited_count.load(.monotonic) > 0);

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);
    FakeCatalog.indexes_json_buf = "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":3}}";
    {
        var read_lease = try read_cache.getOrOpen(db_path, FakeCatalog.iface(), 7001, 0, "docs");
        defer read_lease.release();

        var initial = read_lease.db.search(alloc, .{
            .index_name = "semantic_idx",
            .dense = .{
                .vector = &.{ 1.0, 0.0, 0.0 },
                .k = 3,
            },
            .limit = 3,
        }) catch |err| switch (err) {
            error.StoredDocMissing => null,
            else => return err,
        };
        if (initial) |*result| {
            defer result.deinit();
            try std.testing.expect(result.total_hits < 3);
        }
    }

    FakeEmbeddingProvider.allowAll();
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{
            .{ .key = "doc:b", .value = "{\"body\":\"beta body\"}" },
            .{ .key = "doc:c", .value = "{\"body\":\"gamma body\"}" },
        },
        .sync_level = .full_index,
    });
    source.readPreparation().prepareForRead("docs", .dense_query);

    var ready = false;
    attempts = 0;
    while (attempts < 800) : (attempts += 1) {
        {
            var read_lease = try read_cache.getOrOpen(db_path, FakeCatalog.iface(), 7001, 0, "docs");
            defer read_lease.release();

            var result = read_lease.db.search(alloc, .{
                .index_name = "semantic_idx",
                .dense = .{
                    .vector = &.{ 1.0, 0.0, 0.0 },
                    .k = 3,
                },
                .limit = 3,
            }) catch |err| switch (err) {
                error.StoredDocMissing => {
                    sleepNs(25 * std.time.ns_per_ms);
                    continue;
                },
                else => return err,
            };
            defer result.deinit();
            if (result.total_hits == 3 and result.hits.len == 3) {
                try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
                ready = true;
                break;
            }
        }

        sleepNs(25 * std.time.ns_per_ms);
    }

    try std.testing.expect(ready);
}

test "provisioned table write source persists chunk artifacts when chunker enables full text indexing" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-managed-chunk-full-text");
    defer alloc.free(path);

    const FakeEmbeddingProvider = struct {
        fn vectorForInput(input: std.json.Value) []const u8 {
            return if (jsonValueContainsText(input, "alpha")) "[1,0,0]" else "[0,0,1]";
        }

        fn appendEmbedding(
            arena: std.mem.Allocator,
            out: *std.ArrayListUnmanaged(u8),
            index: usize,
            input: std.json.Value,
        ) !void {
            if (index != 0) try out.append(arena, ',');
            const entry = try std.fmt.allocPrint(
                arena,
                "{{\"object\":\"embedding\",\"index\":{d},\"embedding\":{s}}}",
                .{ index, vectorForInput(input) },
            );
            defer arena.free(entry);
            try out.appendSlice(arena, entry);
        }

        fn responseBody(arena: std.mem.Allocator, input: std.json.Value) ![]u8 {
            var out = std.ArrayListUnmanaged(u8).empty;
            try out.appendSlice(arena, "{\"object\":\"list\",\"data\":[");
            switch (input) {
                .array => |items| {
                    for (items.items, 0..) |item, index| {
                        try appendEmbedding(arena, &out, index, item);
                    }
                },
                else => try appendEmbedding(arena, &out, 0, input),
            }
            try out.appendSlice(arena, "],\"model\":\"test-embed\",\"usage\":{\"prompt_tokens\":1,\"total_tokens\":1}}");
            return try out.toOwnedSlice(arena);
        }

        fn executor() http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, arena: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/v1/embeddings"));

            var parsed_req = try parseJsonBodyIgnoreUnknown(TestEmbeddingRequest, arena, req.body);
            defer parsed_req.deinit();

            return .{
                .status = 200,
                .content_type = try arena.dupe(u8, "application/json"),
                .body = try responseBody(arena, parsed_req.value.input),
            };
        }
    };

    const FakeCatalog = struct {
        var indexes_json_buf: []const u8 = "";
        var table_records = [_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .placement_role = "data",
        }};
        var range_records = [_]metadata_table_manager.RangeRecord{.{
            .group_id = 7001,
            .table_id = 7,
            .start_key = "",
            .end_key = null,
        }};

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            table_records[0].indexes_json = indexes_json_buf;
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = table_records[0..],
                .ranges = range_records[0..],
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var listener = std_http_listener.StdHttpListener.init(alloc, .{}, FakeEmbeddingProvider.executor());
    defer listener.deinit();
    try listener.start();
    const base_uri = try listener.baseUri(alloc);
    defer alloc.free(base_uri);

    FakeCatalog.indexes_json_buf = try std.fmt.allocPrint(alloc,
        \\{{"semantic_chunked_idx":{{"type":"embeddings","field":"body","dimension":3,"embedder":{{"provider":"openai","model":"test-embed","url":"{s}"}},"chunker":{{"provider":"mock","store_chunks":false,"full_text_index":{{}},"text":{{"target_tokens":4,"overlap_tokens":1,"separator":" "}}}}}}}}
    , .{base_uri});
    defer alloc.free(FakeCatalog.indexes_json_buf);

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    defer source.deinit();
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"Alpha with full text chunks\",\"body\":\"alpha alpha alpha alpha beta beta beta beta beta beta\"}" }},
        .sync_level = .full_text,
    });

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);
    var reopened = try db_mod.DB.open(alloc, db_path, .{});
    defer reopened.close();

    const chunk_prefix = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "semantic_chunked_idx_chunks");
    defer alloc.free(chunk_prefix);
    const artifacts = try reopened.core.store.scanPrefix(alloc, chunk_prefix);
    defer db_mod.docstore.DocStore.freeResults(alloc, artifacts);

    var chunk_count: usize = 0;
    for (artifacts) |entry| {
        if (db_mod.internal_keys.isChunkArtifactRecordKey(entry.key)) chunk_count += 1;
    }

    try std.testing.expect(chunk_count >= 2);
}

test "api.table_writes.query_visibility read preparation invalidates readers without closing dirty writer cache" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-write-cache-read-prep";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    defer source.deinit();
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    source.write_cache = &write_cache;

    _ = try source.source().createTable(alloc, "docs", .{});
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .timestamp_ns = 1,
    });

    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(source.isWriteCacheDirtyForTable("docs"));

    source.readPreparation().prepareForRead("other", .general);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(source.isWriteCacheDirtyForTable("docs"));

    source.readPreparation().prepareForRead("docs", .dense_query);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(!source.isWriteCacheDirtyForTable("docs"));

    {
        var cached = try source.getOrOpenCachedDbMode(alloc, &write_cache, path, 7001, "docs", .default, null, null);
        defer cached.deinit(alloc);
    }
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(!source.isWriteCacheDirtyForTable("docs"));

    const large_writes = try alloc.alloc(db_mod.types.BatchWrite, auto_bulk_ingest_min_batch_ops);
    defer {
        for (large_writes) |write| alloc.free(@constCast(write.key));
        alloc.free(large_writes);
    }
    for (large_writes, 0..) |*write, i| {
        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:bulk:{d}", .{i}),
            .value = "{\"title\":\"bulk\"}",
        };
    }
    _ = try source.source().batch(alloc, "docs", .{
        .writes = large_writes,
        .timestamp_ns = 2,
        .sync_level = .write,
    });

    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(!write_cache.entries.items[0].*.auto_bulk_ingest_session_open);
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].*.auto_bulk_ingest_ops);

    const next_writes = try alloc.alloc(db_mod.types.BatchWrite, auto_bulk_ingest_min_batch_ops);
    defer {
        for (next_writes) |write| alloc.free(@constCast(write.key));
        alloc.free(next_writes);
    }
    for (next_writes, 0..) |*write, i| {
        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:bulk-next:{d}", .{i}),
            .value = "{\"title\":\"bulk next\"}",
        };
    }
    _ = try source.source().batch(alloc, "docs", .{
        .writes = next_writes,
        .timestamp_ns = 3,
        .sync_level = .write,
    });

    try std.testing.expect(!write_cache.entries.items[0].*.auto_bulk_ingest_session_open);
    try std.testing.expect(!write_cache.entries.items[0].*.auto_bulk_ingest_finish_requested);
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].*.auto_bulk_ingest_ops);

    source.readPreparation().prepareForRead("docs", .general);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(!source.isWriteCacheDirtyForTable("docs"));
}

test "weak-sync group writes publish all docs after background dense catch-up" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/auto-bulk-threshold-visible", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = "{\"dense_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    defer source.deinit();
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    const batch_size = 250;
    const expected_docs = 1000;
    const writes = try alloc.alloc(db_mod.types.BatchWrite, batch_size);
    defer {
        for (writes) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        alloc.free(writes);
    }
    for (writes, 0..) |*write, i| {
        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{i}),
            .value = try std.fmt.allocPrint(alloc, "{{\"_embeddings\":{{\"dense_idx\":[1.0,0.0]}}}}", .{}),
        };
    }

    var offset: usize = 0;
    while (offset < expected_docs) : (offset += batch_size) {
        for (writes, 0..) |*write, i| {
            alloc.free(@constCast(write.key));
            write.key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{offset + i});
        }

        _ = try source.source().batchGroupLocal(alloc, 7001, "docs", .{
            .writes = writes,
            .timestamp_ns = @intCast(offset + 1),
            .sync_level = .write,
        });

        try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
        try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].active_leases);
    }

    try std.testing.expect(!write_cache.entries.items[0].auto_bulk_ingest_session_open);
    try std.testing.expect(!write_cache.entries.items[0].auto_bulk_ingest_finish_requested);
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].auto_bulk_ingest_ops);
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].active_leases);

    const replay_target = write_cache.entries.items[0].db.core.nextDerivedSequence();
    try write_cache.entries.items[0].db.executor.waitForAll(replay_target);
    const stats = try write_cache.entries.items[0].db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(u64, expected_docs), stats.indexes[0].doc_count);
    try std.testing.expectEqual(replay_target, stats.indexes[0].replay_applied_sequence);

    try std.testing.expect(source.publishManagedRuntimeStatusBestEffort("docs", 7001, &write_cache.entries.items[0].db));
    var statuses = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, expected_docs), statuses.items[0].stats.indexes[0].doc_count);
    try std.testing.expectEqual(replay_target, statuses.items[0].stats.indexes[0].replay_applied_sequence);
}

test "api.table_writes.query_visibility table write source runtime status does not inspect read cache hbc stats when dirty" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-read-cache-hbc", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    {
        var db = try openManagedDbWithIndexesJson(
            alloc,
            path,
            "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
        );
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"_embeddings\":{\"semantic_idx\":[1,2]}}" }},
            .sync_level = .write,
        });
    }

    var hbc_cache = hbc_mod.Cache.init(alloc);
    defer hbc_cache.deinit();

    var read_cache = table_read_cache.ProvisionedTableReadCache.init(alloc);
    defer read_cache.deinit();
    read_cache.hbc_cache = &hbc_cache;

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    defer source.deinit();
    source.read_cache = &read_cache;
    source.runtime_status_cache = &snapshot_cache;
    source.markWriteCacheDirty("docs");

    const read_cache_stats_before = read_cache.cacheStats();
    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    const read_cache_stats_after = read_cache.cacheStats();

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.indexes[0].doc_count);
    try std.testing.expectEqual(@as(u64, 0), statuses.items[0].stats.indexes[0].hbc_cache.total_bytes);
    try std.testing.expectEqual(@as(u64, 0), statuses.items[0].stats.indexes[0].hbc_cache.vector.used_bytes);
    try std.testing.expectEqual(read_cache_stats_before.hit_count, read_cache_stats_after.hit_count);
    try std.testing.expectEqual(read_cache_stats_before.miss_count, read_cache_stats_after.miss_count);
}

test "api.table_writes.query_visibility table write source read cache overlay preserves live replay status" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-read-cache-preserve-replay", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    {
        var db = try openManagedDbWithIndexesJson(
            alloc,
            path,
            "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
        );
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"_embeddings\":{\"semantic_idx\":[1,2]}}" }},
            .sync_level = .write,
        });
    }

    var hbc_cache = hbc_mod.Cache.init(alloc);
    defer hbc_cache.deinit();

    var read_cache = table_read_cache.ProvisionedTableReadCache.init(alloc);
    defer read_cache.deinit();
    read_cache.hbc_cache = &hbc_cache;

    {
        var read_lease = try read_cache.getOrOpen(path, NoCatalog.iface(), 7001, 0, "docs");
        defer read_lease.release();

        const query_vec = [_]f32{ 1.0, 2.0 };
        const req: db_mod.types.SearchRequest = .{
            .index_name = "semantic_idx",
            .dense = .{ .vector = query_vec[0..], .k = 1 },
            .limit = 1,
            .include_stored = false,
        };
        var profiled = try read_lease.db.searchDenseProfiled(alloc, req, req.dense.?);
        defer profiled.result.deinit();
        try std.testing.expect(profiled.result.hits.len >= 1);
    }

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    defer source.deinit();
    source.read_cache = &read_cache;

    var status = runtime_status.LocalTableRuntimeStatus{
        .group_id = 7001,
        .stats = .{
            .doc_count = 0,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    defer status.deinit(alloc);
    status.stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 0,
        .replay_applied_sequence = 42,
        .replay_target_sequence = 42,
        .replay_catch_up_required = false,
        .backfill_active = false,
    };

    try source.overlayReadCacheIndexVisibilityBestEffort(alloc, "docs", 7001, &status);

    try std.testing.expectEqual(@as(u64, 1), status.stats.indexes[0].doc_count);
    try std.testing.expect(status.stats.indexes[0].hbc_cache.total_bytes > 0);
    try std.testing.expect(status.stats.indexes[0].hbc_cache.vector.used_bytes > 0);
    try std.testing.expectEqual(@as(u64, 42), status.stats.indexes[0].replay_applied_sequence);
    try std.testing.expectEqual(@as(u64, 42), status.stats.indexes[0].replay_target_sequence);
    try std.testing.expect(!status.stats.indexes[0].replay_catch_up_required);
    try std.testing.expect(!status.stats.indexes[0].backfill_active);
}

test "provisioned table write source restore repair completion retires cached vector read state" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/restore-repair-read-state", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    {
        var db = try openManagedDbWithIndexesJson(
            alloc,
            path,
            "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
        );
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"_embeddings\":{\"semantic_idx\":[1,2]}}" }},
            .sync_level = .write,
        });
    }

    var hbc_cache = hbc_mod.Cache.init(alloc);
    defer hbc_cache.deinit();

    var read_cache = table_read_cache.ProvisionedTableReadCache.init(alloc);
    defer read_cache.deinit();
    read_cache.hbc_cache = &hbc_cache;

    {
        var read_lease = try read_cache.getOrOpen(path, NoCatalog.iface(), 7001, 0, "docs");
        defer read_lease.release();

        const query_vec = [_]f32{ 1.0, 2.0 };
        var result = try read_lease.db.search(alloc, .{
            .index_name = "semantic_idx",
            .dense = .{ .vector = query_vec[0..], .k = 1 },
            .limit = 1,
            .include_stored = false,
        });
        defer result.deinit();
        try std.testing.expect(result.hits.len >= 1);
    }

    try std.testing.expect(hbc_cache.global_stats.total_bytes > 0);
    const stats_before = read_cache.cacheStats();
    {
        var repair_db = try table_write_managed_db.openManagedDbWithIndexesJsonAndCache(
            alloc,
            path,
            "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
            null,
            &hbc_cache,
            0,
            null,
        );
        defer repair_db.close();
        repair_db.clearDenseHbcCaches();
    }

    const Hook = struct {
        calls: usize = 0,
        kind: ?ProvisionedTableWriteSource.LocalChangeKind = null,

        fn onChange(ptr: *anyopaque, _: []const u8, kind: ProvisionedTableWriteSource.LocalChangeKind) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.kind = kind;
        }
    };
    var hook = Hook{};
    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    defer source.deinit();
    source.read_cache = &read_cache;
    source.setLocalChangeHook(.{
        .ptr = &hook,
        .on_change = Hook.onChange,
    });

    source.enqueueRestoreRepairComplete("docs");
    source.restore_repair_completion_group.await(source.table_activity_threaded.io()) catch {};

    try std.testing.expectEqual(@as(u64, 0), hbc_cache.global_stats.total_bytes);
    try std.testing.expectEqual(@as(usize, 1), hook.calls);
    try std.testing.expectEqual(ProvisionedTableWriteSource.LocalChangeKind.data, hook.kind.?);

    {
        var reopened = try read_cache.getOrOpen(path, NoCatalog.iface(), 7001, 0, "docs");
        defer reopened.release();
    }
    const stats_after = read_cache.cacheStats();
    try std.testing.expectEqual(stats_before.miss_count + 1, stats_after.miss_count);
}

test "api.table_writes.docid provisioned restore repair open rejects stale doc identity namespace" {
    const alloc = std.testing.allocator;

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .range_id = 97001,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/restore-repair-stale-docid", .{tmp.sub_path});
    defer alloc.free(path);

    const stale_namespace = doc_identity.Namespace{ .table_id = 7, .shard_id = 7001, .range_id = 71001 };
    {
        var db = try db_mod.DB.open(alloc, path, .{ .identity_namespace = stale_namespace });
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"stale\"}" }},
            .sync_level = .write,
        });
    }

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-restore-repair-stale-docid", Catalog.iface());
    defer source.deinit();

    try std.testing.expectError(error.DocIdentityNamespaceMismatch, source.openRestoreRepairDbForGroup(
        alloc,
        path,
        7001,
        "docs",
        "{}",
    ));
}

test "provisioned table write source visibility hook publishes owner db status" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-visibility-hook", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    var db = try openManagedDbWithIndexesJson(
        alloc,
        path,
        "{\"search_idx\":{\"type\":\"full_text\",\"config\":{}}}",
    );
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .full_index,
    });

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    defer source.deinit();
    source.runtime_status_cache = &snapshot_cache;
    const Hook = struct {
        calls: usize = 0,
        kind: ?ProvisionedTableWriteSource.LocalChangeKind = null,
        table_name: []const u8 = "",

        fn onChange(ptr: *anyopaque, table_name: []const u8, kind: ProvisionedTableWriteSource.LocalChangeKind) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.kind = kind;
            self.table_name = table_name;
        }
    };
    var hook = Hook{};
    source.setLocalChangeHook(.{
        .ptr = &hook,
        .on_change = Hook.onChange,
    });

    ProvisionedTableWriteSource.onManagedDerivedVisibilityChanged(&source, "docs", 7001, &db, .publish);

    try std.testing.expectEqual(@as(usize, 1), hook.calls);
    try std.testing.expectEqual(ProvisionedTableWriteSource.LocalChangeKind.data, hook.kind.?);
    try std.testing.expectEqualStrings("docs", hook.table_name);
    var published = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer published.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), published.items.len);
    try std.testing.expectEqual(runtime_status.RuntimeStatusSource.live_writer_publish, published.items[0].metadata.source);
    try std.testing.expectEqual(runtime_status.RuntimeStatusFreshness.fresh, published.items[0].metadata.freshness);
    try std.testing.expectEqual(@as(u64, 1), published.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 1), published.items[0].stats.indexes[0].doc_count);

    ProvisionedTableWriteSource.onManagedDerivedVisibilityChanged(&source, "docs", 7001, null, .invalidate);

    try std.testing.expectEqual(@as(usize, 2), hook.calls);
    try std.testing.expectEqual(ProvisionedTableWriteSource.LocalChangeKind.data, hook.kind.?);
    try std.testing.expectEqualStrings("docs", hook.table_name);
    var retained = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer retained.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), retained.items.len);
    try std.testing.expectEqual(@as(u64, 1), retained.items[0].stats.doc_count);
}

test "provisioned replicated sync marks local runtime status dirty" {
    const alloc = std.testing.allocator;

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"search_idx\":{\"type\":\"full_text\",\"config\":{}}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const Hook = struct {
        calls: usize = 0,
        kind: ?ProvisionedTableWriteSource.LocalChangeKind = null,

        fn onChange(ptr: *anyopaque, _: []const u8, kind: ProvisionedTableWriteSource.LocalChangeKind) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.kind = kind;
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/replicated-sync-runtime-dirty", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    defer source.deinit();
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    var hook = Hook{};
    source.setLocalChangeHook(.{
        .ptr = &hook,
        .on_change = Hook.onChange,
    });

    _ = try source.applyReplicatedBatchGroupLocal(alloc, 7001, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .full_index,
    });
    try std.testing.expect(hook.calls > 0);
    try std.testing.expectEqual(ProvisionedTableWriteSource.LocalChangeKind.data, hook.kind.?);

    hook.calls = 0;
    hook.kind = null;
    try source.syncReplicatedBatchGroupLocal(alloc, 7001, "docs", .full_index);

    try std.testing.expect(hook.calls > 0);
    try std.testing.expectEqual(ProvisionedTableWriteSource.LocalChangeKind.data, hook.kind.?);
}

test "api.table_writes.docid provisioned table write source consistent visibility hook does not block on busy apply lock" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-consistent-hook-busy", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    var db = try openManagedDbWithIndexesJson(
        alloc,
        path,
        "{\"search_idx\":{\"type\":\"full_text\",\"config\":{}}}",
    );
    defer db.close();

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    defer source.deinit();
    source.runtime_status_cache = &snapshot_cache;

    db.core.lockApplyExclusive();
    ProvisionedTableWriteSource.onManagedDerivedVisibilityChanged(&source, "docs", 7001, &db, .publish_consistent);
    db.core.unlockApplyExclusive();

    const published = try snapshot_cache.snapshot(alloc, "docs");
    try std.testing.expect(published == null);
}

test "api.table_writes.docid provisioned table write source consistent visibility refreshes stale dense status" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-consistent-dense-refresh", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    var db = try openManagedDbWithIndexesJson(
        alloc,
        path,
        "{\"dense_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
    );
    defer db.close();

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, table_catalog.emptyCatalogSource());
    defer source.deinit();
    source.runtime_status_cache = &snapshot_cache;
    const hook = source.managedDerivedVisibilityHook("docs", 7001, &db);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"_embeddings\":{\"dense_idx\":[1,0]}}" }},
        .sync_level = .full_index,
    });
    hook.notify(.publish);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:b", .value = "{\"_embeddings\":{\"dense_idx\":[0,1]}}" }},
        .sync_level = .full_index,
    });
    hook.notify(.publish_consistent);

    var published = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer published.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), published.items.len);
    try std.testing.expectEqual(@as(usize, 1), published.items[0].stats.indexes.len);
    try std.testing.expectEqual(@as(u64, 2), published.items[0].stats.indexes[0].doc_count);
    try std.testing.expectEqual(@as(u64, 2), published.items[0].stats.doc_count);
}

test "provisioned table write source promotes synthetic placeholder when publishing live db status" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-promote-synthetic", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var placeholder = runtime_status.LocalTableRuntimeStatus{
        .group_id = 7001,
        .metadata = .{
            .updated_at_ns = 1,
            .source = .synthetic_config,
            .freshness = .stale,
        },
        .stats = .{
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    defer placeholder.deinit(alloc);
    placeholder.stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
    };
    try snapshot_cache.upsertGroupStatus("docs", placeholder);

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    defer source.deinit();
    source.runtime_status_cache = &snapshot_cache;

    var db = try openManagedDbWithIndexesJson(
        alloc,
        path,
        "{\"dense_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
    );
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"_embeddings\":{\"dense_idx\":[1,0]}}" }},
        .sync_level = .full_index,
    });

    try publishRuntimeStatusSnapshot(&source, alloc, "docs", 7001, &db);

    var published = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer published.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), published.items.len);
    try std.testing.expectEqual(runtime_status.RuntimeStatusSource.live_writer_publish, published.items[0].metadata.source);
    try std.testing.expectEqual(runtime_status.RuntimeStatusFreshness.fresh, published.items[0].metadata.freshness);
    try std.testing.expect(runtime_status.statusHasRuntimeFacts(published.items[0]));
    try std.testing.expectEqual(@as(u64, 1), published.items[0].stats.indexes[0].doc_count);
}

test "provisioned table write source publishes replay debt from owner db handle" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-replay-debt-publish", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    var db = try openManagedDbWithIndexesJsonAndCacheMode(
        alloc,
        path,
        "{\"dv_v1\":{\"type\":\"embeddings\",\"field\":\"embedding\",\"dimension\":2,\"embedder\":{\"provider\":\"openai\",\"model\":\"test-embed\",\"url\":\"http://127.0.0.1:9\"}}}",
        null,
        null,
        0,
        null,
        .writer_no_replay,
    );
    defer db.close();
    _ = try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"embedding\":[1,2]}" }},
        .sync_level = .write,
    });

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    defer source.deinit();
    source.runtime_status_cache = &snapshot_cache;
    try std.testing.expect(source.publishManagedRuntimeStatusBestEffort("docs", 7001, &db));

    var statuses = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(usize, 1), statuses.items[0].stats.indexes.len);
    try std.testing.expectEqualStrings("dv_v1", statuses.items[0].stats.indexes[0].name);
    try std.testing.expect(statuses.items[0].stats.indexes[0].replay_catch_up_required);
    try std.testing.expect(statuses.items[0].stats.indexes[0].replay_target_sequence > statuses.items[0].stats.indexes[0].replay_applied_sequence);
}

test "provisioned runtime status overlays live writer replay target without republishing stats" {
    const alloc = std.testing.allocator;
    const namespace = doc_identity.Namespace{ .table_id = 7, .shard_id = 7001, .range_id = 97001 };

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"dv_v1\":{\"type\":\"embeddings\",\"field\":\"embedding\",\"dimension\":2,\"embedder\":{\"provider\":\"openai\",\"model\":\"test-embed\",\"url\":\"http://127.0.0.1:9\"}}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .range_id = 97001, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-live-target-overlay", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    defer source.deinit();
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    _ = try source.source().batchGroupLocal(alloc, 7001, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"embedding\":[1,2]}" }},
        .sync_level = .write,
    });
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try validateProvisionedDbIdentityNamespaceExpected(namespace, &write_cache.entries.items[0].db);
    try std.testing.expect(source.publishManagedRuntimeStatusBestEffort("docs", 7001, &write_cache.entries.items[0].db));

    var initial_cached = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer initial_cached.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), initial_cached.items[0].stats.indexes[0].replay_target_sequence);
    var stale_status = try initial_cached.items[0].clone(alloc);
    defer stale_status.deinit(alloc);

    _ = try source.source().batchGroupLocal(alloc, 7001, "docs", .{
        .writes = &.{.{ .key = "doc:b", .value = "{\"title\":\"beta\",\"embedding\":[2,3]}" }},
        .sync_level = .write,
    });
    try snapshot_cache.upsertGroupStatus("docs", stale_status);

    var cached_only = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer cached_only.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), cached_only.items[0].stats.indexes[0].replay_target_sequence);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(usize, 1), statuses.items[0].stats.indexes.len);
    try std.testing.expectEqual(@as(u64, 2), statuses.items[0].stats.indexes[0].replay_target_sequence);
    try std.testing.expect(statuses.items[0].stats.indexes[0].replay_catch_up_required);
}

test "dirty auto bulk writer publishes runtime status before read preparation clears dirty state" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/antfly-api-provisioned-write-cache-publish-before-invalidate", .{tmp.sub_path});
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    defer source.deinit();
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    _ = try source.source().createTable(alloc, "docs", .{});

    const writes = try alloc.alloc(db_mod.types.BatchWrite, auto_bulk_ingest_min_batch_ops);
    defer {
        for (writes) |write| alloc.free(@constCast(write.key));
        alloc.free(writes);
    }
    for (writes, 0..) |*write, i| {
        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:bulk-status:{d}", .{i}),
            .value = "{\"title\":\"bulk status\"}",
        };
    }
    _ = try source.source().batch(alloc, "docs", .{
        .writes = writes,
        .timestamp_ns = 1,
        .sync_level = .write,
    });

    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(!write_cache.entries.items[0].*.auto_bulk_ingest_session_open);
    try std.testing.expect(source.isWriteCacheDirtyForTable("docs"));

    source.readPreparation().prepareForRead("docs", .general);

    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(!source.isWriteCacheDirtyForTable("docs"));

    var statuses = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(runtime_status.RuntimeStatusSource.live_writer_publish, statuses.items[0].metadata.source);
    try std.testing.expectEqual(runtime_status.RuntimeStatusFreshness.fresh, statuses.items[0].metadata.freshness);
    try std.testing.expect(runtime_status.statusHasRuntimeFacts(statuses.items[0]));
    try std.testing.expect(statuses.items[0].stats.index_count > 0);
}

test "api.table_writes.query_visibility managed publish hook updates runtime status cache from live writer" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/antfly-api-managed-visibility-publish-status", .{tmp.sub_path});
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var source = ProvisionedTableWriteSource.init(path, table_catalog.emptyCatalogSource());
    defer source.deinit();
    source.runtime_status_cache = &snapshot_cache;

    var db = try openManagedDbWithIndexesJson(
        alloc,
        path,
        "{\"dense_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
    );
    defer db.close();
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"_embeddings\":{\"dense_idx\":[1,0]}}" },
        },
        .sync_level = .full_index,
    });

    const hook = source.managedDerivedVisibilityHook("docs", 7001, &db);
    hook.notify(.publish);

    var statuses = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(runtime_status.RuntimeStatusSource.live_writer_publish, statuses.items[0].metadata.source);
    try std.testing.expectEqual(runtime_status.RuntimeStatusFreshness.fresh, statuses.items[0].metadata.freshness);
    try std.testing.expect(runtime_status.statusHasRuntimeFacts(statuses.items[0]));
    try std.testing.expectEqual(@as(u64, 1), statuses.items[0].stats.indexes[0].doc_count);
}

test "api.table_writes.query_visibility read preparation does not block on same-table batch after early dirty publication" {
    const alloc = std.testing.allocator;
    const replica_root_dir = "/tmp/antfly-api-provisioned-read-prep-active-batch";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root_dir) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root_dir) catch {};

    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const BatchProbe = struct {
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),

        fn beforeBatch(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) std.atomic.spinLoopHint();
        }
    };

    const BatchWorker = struct {
        source: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            _ = self.source.source().batch(self.alloc, "docs", .{
                .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
                .timestamp_ns = 1,
            }) catch |err| {
                self.err = err;
            };
        }
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    defer source.deinit();
    source.write_cache = &write_cache;

    _ = try source.source().createTable(alloc, "docs", .{});

    {
        var cached = try source.getOrOpenCachedDbMode(alloc, &write_cache, path, 7001, "docs", .default, null, null);
        defer cached.deinit(alloc);
    }

    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(!source.isWriteCacheDirtyForTable("docs"));

    var probe = BatchProbe{};
    test_before_batch_execution_hook = .{
        .ptr = &probe,
        .run = BatchProbe.beforeBatch,
    };
    defer test_before_batch_execution_hook = null;

    var batch_worker = BatchWorker{ .source = &source, .alloc = alloc };
    const batch_thread = try std.Thread.spawn(.{}, BatchWorker.run, .{&batch_worker});
    defer batch_thread.join();

    const ReadWorker = struct {
        source: *ProvisionedTableWriteSource,
        started: std.atomic.Value(bool) = .init(false),
        completed: std.atomic.Value(bool) = .init(false),

        fn run(self: *@This()) void {
            self.started.store(true, .release);
            self.source.readPreparation().prepareForRead("docs", .general);
            self.completed.store(true, .release);
        }
    };

    while (!probe.entered.load(.acquire)) std.atomic.spinLoopHint();

    var read_worker = ReadWorker{ .source = &source };
    const read_thread = try std.Thread.spawn(.{}, ReadWorker.run, .{&read_worker});
    defer read_thread.join();

    while (!read_worker.started.load(.acquire)) std.atomic.spinLoopHint();
    for (0..1000) |_| std.atomic.spinLoopHint();
    try std.testing.expect(read_worker.completed.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(!source.isWriteCacheDirtyForTable("docs"));

    probe.release.store(true, .release);
    if (batch_worker.err) |err| return err;
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
}

test "provisioned txn resolve invalidates cached writer state on commit" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-txn-resolve-invalidates-write-cache");
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    defer source.deinit();
    source.write_cache = &write_cache;

    _ = try source.source().createTable(alloc, "docs", .{});
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .timestamp_ns = 1,
    });

    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);

    const txn_id = try distributed_txn.parseTxnIdHex("00112233445566778899aabbccddeeff");
    const begin_timestamp = nextTxnTimestamp();
    const commit_timestamp = begin_timestamp + 1;
    _ = try source.source().txnBeginGroupLocal(alloc, 7001, "docs", txn_id, begin_timestamp, 0, &.{"group:7001"});
    _ = try source.source().txnPrepareGroupLocal(alloc, 7001, "docs", txn_id, 0, .{
        .writes = &.{.{ .key = "doc:b", .value = "{\"title\":\"beta\"}" }},
    });
    _ = try source.source().txnResolveGroupLocal(alloc, 7001, "docs", txn_id, .committed, commit_timestamp);

    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items.len);
    try std.testing.expect(!source.isWriteCacheDirtyForTable("docs"));
}

test "provisioned table write source routes batch writes across ranges" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-batch");
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const left_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(left_path);
    const right_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7002);
    defer alloc.free(right_path);

    var left_db = try db_mod.DB.open(alloc, left_path, .{});
    defer left_db.close();
    var right_db = try db_mod.DB.open(alloc, right_path, .{});
    defer right_db.close();

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data" }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    defer source.deinit();
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
            .{ .key = "doc:z", .value = "{\"title\":\"zeta\"}" },
        },
    });

    left_db.close();
    left_db = try db_mod.DB.open(alloc, left_path, .{});
    right_db.close();
    right_db = try db_mod.DB.open(alloc, right_path, .{});

    var left = (try left_db.lookup(alloc, "doc:a", .{})).?;
    defer left.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, left.json, "\"alpha\"") != null);

    var right = (try right_db.lookup(alloc, "doc:z", .{})).?;
    defer right.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, right.json, "\"zeta\"") != null);
}

test "provisioned table write source structural activity waits for table request lease" {
    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-table-request", NoCatalog.iface());
    source.beginTableRequest("docs");
    defer source.endTableRequest("docs");

    try std.testing.expect(!source.tryBeginStructuralTableActivity("docs"));
    try std.testing.expect(source.hasReadBlockingActivityBestEffort("docs", 7001));
}

test "provisioned table write source keeps structural activity blocked until last table request exits" {
    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-table-request-refcount", NoCatalog.iface());
    source.beginTableRequest("docs");
    source.beginTableRequest("docs");

    try std.testing.expect(!source.tryBeginStructuralTableActivity("docs"));

    source.endTableRequest("docs");
    try std.testing.expect(!source.tryBeginStructuralTableActivity("docs"));

    source.endTableRequest("docs");
    try std.testing.expect(source.tryBeginStructuralTableActivity("docs"));
    source.endStructuralTableActivity("docs");
}

test "provisioned table write source allows different groups of same table to proceed independently" {
    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-table-activity-different-groups", NoCatalog.iface());
    source.beginGroupOperation("docs", 7001);
    defer source.endGroupOperation("docs", 7001);

    try std.testing.expect(source.tryBeginGroupOperation("docs", 7002));
    source.endGroupOperation("docs", 7002);
}

test "provisioned table write source reports only same-group activity as busy" {
    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-group-activity", NoCatalog.iface());
    source.beginGroupOperation("docs", 7001);
    defer source.endGroupOperation("docs", 7001);

    try std.testing.expect(source.hasGroupActivityBestEffort("docs", 7001));
    try std.testing.expect(!source.hasGroupActivityBestEffort("docs", 7002));
    try std.testing.expect(source.hasReadBlockingActivityBestEffort("docs", 7001));
    try std.testing.expect(!source.hasReadBlockingActivityBestEffort("docs", 7002));
}

test "provisioned table write source managed writer probe is unknown while source mutex is busy" {
    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-managed-writer-state", NoCatalog.iface());
    lockAtomic(&source.local_db_mutex);
    defer source.local_db_mutex.unlock();

    try std.testing.expectEqual(@as(ProvisionedTableWriteSource.ManagedWriterGroupProbe, .unknown), source.probeManagedWriterGroupBestEffort("docs", 7001));
}

test "provisioned table write source runtime status is best effort when local db is busy" {
    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-status", FakeCatalog.iface());
    try std.testing.expect(source.local_db_mutex.tryLock());
    defer source.local_db_mutex.unlock();

    const statuses = try source.source().localRuntimeStatuses(std.testing.allocator, "docs");
    try std.testing.expect(statuses == null);
}

test "provisioned table write source runtime statuses reconcile empty embeddings indexes" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/antfly-api-provisioned-write-runtime-status-managed", .{tmp.sub_path});
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3,\"external\":true}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;
    lockAtomic(&source.local_db_mutex);
    var cached = try write_cache.getOrOpenLocked(group_path, FakeCatalog.iface(), 7001, 0, "docs");
    source.local_db_mutex.unlock();
    defer cached.deinit(alloc);
    try publishRuntimeStatusSnapshot(&source, alloc, "docs", 7001, cached.db);
    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(usize, 1), statuses.items[0].stats.indexes.len);
    try std.testing.expectEqualStrings("semantic_idx", statuses.items[0].stats.indexes[0].name);
    try std.testing.expectEqual(false, statuses.items[0].stats.indexes[0].backfill_active);
    try std.testing.expectEqual(@as(u64, 0), statuses.items[0].stats.indexes[0].doc_count);
}

test "provisioned table write source runtime status stays cache-only without shared snapshot" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-write-runtime-cache";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);

    const WarmCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3,\"external\":true}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(path, NoCatalog.iface());
    source.write_cache = &write_cache;

    lockAtomic(&source.local_db_mutex);
    var cached = try write_cache.getOrOpenLocked(group_path, WarmCatalog.iface(), 7001, 0, "docs");
    source.local_db_mutex.unlock();
    defer cached.deinit(alloc);

    try std.testing.expect((try source.source().localRuntimeStatuses(alloc, "docs")) == null);
}

test "provisioned table write source runtime status prefers shared snapshot cache" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-write-runtime-prefers-snapshot";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    var db = try db_mod.DB.open(alloc, group_path, .{});
    defer db.close();

    const WarmCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"cached_handle_idx\":{\"type\":\"embeddings\",\"dimension\":3,\"external\":true}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var cached = try write_cache.getOrOpenLocked(path, WarmCatalog.iface(), 7001, 0, "docs");
    defer cached.deinit(alloc);

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();
    var indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1);
    indexes[0] = .{
        .name = try alloc.dupe(u8, "snapshot_idx"),
        .kind = .dense_vector,
        .doc_count = 42,
    };
    var status = runtime_status.LocalTableRuntimeStatus{
        .group_id = 7001,
        .stats = .{
            .doc_count = 42,
            .index_count = 1,
            .indexes = indexes,
        },
    };
    defer status.deinit(alloc);
    try snapshot_cache.upsertGroupStatus("docs", status);

    var source = ProvisionedTableWriteSource.init(path, NoCatalog.iface());
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 7001), statuses.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 42), statuses.items[0].stats.doc_count);
    try std.testing.expectEqualStrings("snapshot_idx", statuses.items[0].stats.indexes[0].name);
}

test "provisioned table write source structural mutation invalidates shared runtime status cache" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();
    try snapshot_cache.upsertGroupStatus("docs", .{
        .group_id = 7001,
        .stats = .{},
    });
    {
        var before = (try snapshot_cache.snapshot(alloc, "docs")).?;
        defer before.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 1), before.items.len);
    }

    var source = ProvisionedTableWriteSource.init("/tmp/antfly-runtime-status-structural-invalidation", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    source.beginLocalStructuralMutation("docs");
    source.finishLocalStructuralMutation("docs");

    try std.testing.expect((try snapshot_cache.snapshot(alloc, "docs")) == null);
}

test "provisioned table write source runtime status falls back to shared snapshot cache" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-snapshot", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 7001), statuses.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
    try std.testing.expectEqualStrings("semantic_idx", statuses.items[0].stats.indexes[0].name);
}

test "provisioned table write source cached runtime status does not fetch catalog coverage" {
    const alloc = std.testing.allocator;

    const CountingCatalog = struct {
        calls: usize = 0,

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var cached_status = runtime_status.LocalTableRuntimeStatus{
        .group_id = 7001,
        .stats = .{
            .doc_count = 25_000,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    defer cached_status.deinit(alloc);
    cached_status.stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
        .doc_count = 25_000,
        .replay_applied_sequence = 100,
        .replay_target_sequence = 101,
        .replay_catch_up_required = true,
        .backfill_active = true,
    };
    try snapshot_cache.upsertGroupStatus("docs", cached_status);

    var catalog = CountingCatalog{};
    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-status-no-catalog-coverage", catalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 0), catalog.calls);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 25_000), statuses.items[0].stats.doc_count);
    try std.testing.expectEqualStrings("dense_idx", statuses.items[0].stats.indexes[0].name);
    try std.testing.expect(statuses.items[0].stats.indexes[0].replay_catch_up_required);
}

test "provisioned table write source runtime status does not cold-open uncached db" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-uncached-fallback", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    {
        var db = try openManagedDbWithIndexesJson(
            alloc,
            path,
            "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
        );
        defer db.close();
        _ = try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"_embeddings\":{\"semantic_idx\":[1,2]}}" }},
            .sync_level = .write,
        });
    }

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    try std.testing.expect((try source.source().localRuntimeStatuses(alloc, "docs")) == null);
}

test "provisioned table write source runtime status serves cached snapshot during active same-table work" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-snapshot-active", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    source.beginGroupOperation("docs", 7001);
    defer source.endGroupOperation("docs", 7001);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
}

test "provisioned table write source runtime status serves cached snapshot while dirty and request busy" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-request-busy-dirty", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;
    source.markWriteCacheDirty("docs");
    source.beginTableRequest("docs");
    defer source.endTableRequest("docs");

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
}

test "provisioned table write source runtime status still serves sibling groups while one group is active" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 2);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };
    items[1] = .{
        .group_id = 7002,
        .stats = .{
            .doc_count = 3,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[1].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 3,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-snapshot-active-sibling", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    source.beginGroupOperation("docs", 7001);
    defer source.endGroupOperation("docs", 7001);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 7001), statuses.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 7002), statuses.items[1].group_id);
    try std.testing.expectEqual(@as(u64, 3), statuses.items[1].stats.doc_count);
}

test "provisioned table write source runtime status filtering transfers owned statuses" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-status-filter-ownership", NoCatalog.iface());

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    errdefer alloc.free(items);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    errdefer items[0].deinit(alloc);
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    var owned: runtime_status.LocalTableRuntimeStatuses = .{ .items = items };
    defer owned.deinit(alloc);

    var filtered = (try source.takeStatusesWithoutActiveGroups(alloc, &owned, &.{7002})).?;
    defer filtered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 0), owned.items.len);
    try std.testing.expectEqual(@as(usize, 1), filtered.items.len);
    try std.testing.expectEqual(@as(u64, 7001), filtered.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 9), filtered.items[0].stats.doc_count);
}

test "provisioned table write source runtime status still serves unrelated table snapshot while source mutex is busy" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-snapshot-unrelated-busy", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    try std.testing.expect(source.local_db_mutex.tryLock());
    defer source.local_db_mutex.unlock();

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 7001), statuses.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
}

test "provisioned table write source runtime status serves cached snapshot while dirty and busy" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-snapshot-dirty", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;
    source.markWriteCacheDirty("docs");

    try std.testing.expect(source.local_db_mutex.tryLock());
    defer source.local_db_mutex.unlock();

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
}

test "provisioned table write source runtime status serves cached snapshot while dirty without source contention" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-snapshot-dirty-unlocked", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;
    source.markWriteCacheDirty("docs");

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
}

test "provisioned table write source runtime status still serves clean sibling table while another table is dirty" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const docs_items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    docs_items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    docs_items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const logs_items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    logs_items[0] = .{
        .group_id = 8001,
        .stats = .{
            .doc_count = 4,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    logs_items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 4,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 2);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = docs_items },
    };
    snapshots[1] = .{
        .table_name = try alloc.dupe(u8, "logs"),
        .statuses = .{ .items = logs_items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-snapshot-dirty-sibling", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;
    source.markWriteCacheDirty("docs");

    var statuses = (try source.source().localRuntimeStatuses(alloc, "logs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 8001), statuses.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 4), statuses.items[0].stats.doc_count);
}

test "provisioned table write source runtime status serves shared snapshot cache while clean and busy" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-snapshot-clean-busy", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    try std.testing.expect(source.local_db_mutex.tryLock());
    defer source.local_db_mutex.unlock();

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 7001), statuses.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
}

test "provisioned table write source runtime status stays null while dirty and busy during startup catch-up" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
            .async_indexing = .{
                .startup = .{
                    .active = true,
                    .phase = .artifact_rebuild,
                    .wal_retention_known = true,
                    .wal_retained_segments = 4,
                    .wal_retained_bytes = 99,
                },
                .dense_catch_up = .{
                    .active = true,
                    .current_sequence = 4,
                    .current_target_sequence = 9,
                    .current_scanned_entries = 12,
                    .current_applied_entries = 4,
                    .progress_updates = 3,
                },
            },
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-snapshot-startup", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;
    source.markWriteCacheDirty("docs");
    source.startup_catch_up_active.store(true, .monotonic);

    try std.testing.expect(source.local_db_mutex.tryLock());
    defer source.local_db_mutex.unlock();

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);

    const async_stats = source.asyncIndexingStatsBestEffort();
    try std.testing.expect(async_stats.startup.active);
    try std.testing.expectEqual(db_mod.types.StartupCatchUpPhase.artifact_rebuild, async_stats.startup.phase);
    try std.testing.expectEqual(@as(u64, 99), async_stats.startup.wal_retained_bytes);
    try std.testing.expect(async_stats.dense_catch_up.active);
    try std.testing.expectEqual(@as(u64, 9), async_stats.dense_catch_up.current_target_sequence);
}

test "provisioned table write source runtime status serves cached snapshot when request is idle" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-leased-snapshot", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    {
        var db = try openManagedDbWithIndexesJson(
            alloc,
            path,
            "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
        );
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"embedding\":[1,2]}" }},
            .sync_level = .write,
        });
    }

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.indexes[0].doc_count);
}

test "provisioned table write source runtime status remains cache-only when dirty and idle" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-leased-dirty", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    {
        var db = try openManagedDbWithIndexesJson(
            alloc,
            path,
            "{\"indexes\":[{\"name\":\"semantic_idx\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
        );
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"embedding\":[1,2]}" }},
            .sync_level = .write,
        });
    }

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;
    source.markWriteCacheDirty("docs");

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.indexes[0].doc_count);
}

test "provisioned table write source runtime status keeps cached snapshot while request is active" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-leased-request-busy", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    {
        var db = try openManagedDbWithIndexesJson(
            alloc,
            path,
            "{\"indexes\":[{\"name\":\"semantic_idx\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
        );
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"embedding\":[1,2]}" }},
            .sync_level = .write,
        });
    }

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;
    source.beginTableRequest("docs");
    defer source.endTableRequest("docs");

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.indexes[0].doc_count);
}

test "provisioned table write source runtime status remains cache-only while bulk ingest session is active" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-bulk-ingest-active", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    {
        var db = try openManagedDbWithIndexesJson(
            alloc,
            path,
            "{\"indexes\":[{\"name\":\"semantic_idx\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
        );
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"embedding\":[1,2]}" }},
            .sync_level = .write,
        });
    }

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    try write_cache.beginBulkIngestLocked("docs");

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.indexes[0].doc_count);
}

test "provisioned table write source runtime status does not lease writer during auto bulk ingest" {
    const alloc = std.testing.allocator;

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-auto-bulk-active", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    var cached = try source.getOrOpenCachedDbMode(alloc, &write_cache, path, 7001, "docs", .default_async, null, null);
    cached.deinit(alloc);

    try write_cache.ensureAutoBulkIngestLocked(7001, "docs", platform_time.monotonicNs());
    try std.testing.expect(write_cache.entries.items[0].auto_bulk_ingest_session_open);
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].active_leases);

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };
    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].active_leases);
    try write_cache.finishAutoBulkIngestLocked(7001, "docs");
}

test "write cache prunes stale visible root generations instead of clearing current entries" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/write-cache-generations", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var generation: u64 = 1;
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    _ = source.withGroupVisibleRootGeneration(testingVisibleRootGenerationSource(&generation));

    var cached_first = try source.getOrOpenCachedDbMode(alloc, &write_cache, path, 7001, "docs", .default_async, null, null);
    cached_first.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expectEqual(@as(u64, 1), write_cache.entries.items[0].lsm_root_generation);

    generation = 2;
    var cached_second = try source.getOrOpenCachedDbMode(alloc, &write_cache, path, 7001, "docs", .default_async, null, null);
    cached_second.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expectEqual(@as(u64, 2), write_cache.entries.items[0].lsm_root_generation);

    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].active_leases);
}

test "hosted write cache opens current visible root generation" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/hosted-write-cache-generation", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, 7001);
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const Router = struct {
        fn iface() table_router.HostedGroupRouter {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .local_node_id = localNodeId,
                    .local_status = localStatus,
                    .group_leader_node_id = groupLeaderNodeId,
                    .node_status = nodeStatus,
                    .node_base_uri = nodeBaseUri,
                },
            };
        }

        fn localNodeId(_: *anyopaque) u64 {
            return 1;
        }

        fn localStatus(_: *anyopaque, group_id: u64) raft_mod.HostedReplicaStatus {
            return if (group_id == 7001) .active else .absent;
        }

        fn groupLeaderNodeId(_: *anyopaque, group_id: u64) ?u64 {
            return if (group_id == 7001) 1 else null;
        }

        fn nodeStatus(_: *anyopaque, node_id: u64, group_id: u64) raft_mod.HostedReplicaStatus {
            _ = node_id;
            return if (group_id == 7001) .active else .absent;
        }

        fn nodeBaseUri(_: *anyopaque, _: std.mem.Allocator, _: u64) !?[]u8 {
            return null;
        }
    };

    const Executor = struct {
        fn iface() http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(_: *anyopaque, _: std.mem.Allocator, _: http_common.HttpRequest) !http_common.HttpResponse {
            return error.UnexpectedHttpRequest;
        }
    };

    var generation: u64 = 1;
    var source = HostedProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface(), Router.iface(), Executor.iface());
    _ = source.withGroupVisibleRootGeneration(testingVisibleRootGenerationSource(&generation));
    defer source.invalidateManagedCache("docs");

    const hosted_cache = try hostedManagedDbCacheForRoot(replica_root_dir);
    var cached_first = try source.getOrOpenCachedDbMode(hosted_cache, path, 7001, "docs", .default_async);
    cached_first.deinit(hosted_cache.write_cache.alloc);
    try std.testing.expectEqual(@as(usize, 1), hosted_cache.write_cache.entries.items.len);
    try std.testing.expectEqual(@as(u64, 1), hosted_cache.write_cache.entries.items[0].lsm_root_generation);

    generation = 2;
    var cached_second = try source.getOrOpenCachedDbMode(hosted_cache, path, 7001, "docs", .default_async);
    cached_second.deinit(hosted_cache.write_cache.alloc);
    try std.testing.expectEqual(@as(usize, 1), hosted_cache.write_cache.entries.items.len);
    try std.testing.expectEqual(@as(u64, 2), hosted_cache.write_cache.entries.items[0].lsm_root_generation);
}

test "write cache adopts just-created db across reconcile generation bump" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/write-cache-created-generation", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var generation: u64 = 1;
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    _ = source.withGroupVisibleRootGeneration(testingVisibleRootGenerationSource(&generation));

    var cached_seed = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, generation, "docs");
    cached_seed.deinit(alloc);
    write_cache.entries.items[0].allow_generation_adoption = true;
    try std.testing.expectEqual(@as(u64, 1), write_cache.entries.items[0].lsm_root_generation);
    const misses_before = write_cache.miss_count.load(.monotonic);

    generation = 2;
    source.pruneStaleWriteCacheLocked();

    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expectEqual(@as(u64, 2), write_cache.entries.items[0].lsm_root_generation);
    try std.testing.expect(!write_cache.entries.items[0].allow_generation_adoption);

    var cached_after_bump = try source.getOrOpenCachedDbMode(alloc, &write_cache, path, 7001, "docs", .default_async, null, null);
    cached_after_bump.deinit(alloc);
    try std.testing.expectEqual(misses_before, write_cache.miss_count.load(.monotonic));
    try std.testing.expect(write_cache.hit_count.load(.monotonic) > 0);
}

test "runtime status request does not finish expired auto bulk ingest" {
    const alloc = std.testing.allocator;

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-auto-bulk-expired", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    var cached = try source.getOrOpenCachedDbMode(alloc, &write_cache, path, 7001, "docs", .default_async, null, null);
    cached.deinit(alloc);

    const now_ns = platform_time.monotonicNs();
    try write_cache.ensureAutoBulkIngestLocked(7001, "docs", now_ns -| auto_bulk_ingest_max_idle_ns);
    write_cache.entries.items[0].auto_bulk_ingest_last_ns = now_ns -| auto_bulk_ingest_max_idle_ns;
    source.markWriteCacheDirty("docs");
    try std.testing.expect(write_cache.entries.items[0].auto_bulk_ingest_session_open);

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{},
    };
    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expect(write_cache.entries.items[0].auto_bulk_ingest_session_open);
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].active_leases);
    try write_cache.finishAutoBulkIngestLocked(7001, "docs");
}

test "runtime status refresh preserves current snapshot on allocation failure" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const alloc = failing.allocator();

    const current_items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    var current = runtime_status.LocalTableRuntimeStatuses{ .items = current_items };
    defer current.deinit(alloc);
    current.items[0] = .{
        .group_id = 7001,
        .stats = .{ .doc_count = 11 },
    };

    const refresh_items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    var refresh = runtime_status.LocalTableRuntimeStatuses{ .items = refresh_items };
    defer refresh.deinit(alloc);
    refresh.items[0] = .{
        .group_id = 7001,
        .stats = .{ .doc_count = 99 },
    };

    failing.fail_index = failing.alloc_index;
    failing.resize_fail_index = failing.resize_index;
    try std.testing.expectError(
        error.OutOfMemory,
        ProvisionedTableWriteSource.replaceRuntimeStatusesWithMergedRefresh(alloc, &current, &refresh),
    );

    failing.fail_index = std.math.maxInt(usize);
    failing.resize_fail_index = std.math.maxInt(usize);
    try std.testing.expectEqual(@as(usize, 1), current.items.len);
    try std.testing.expectEqual(@as(u64, 11), current.items[0].stats.doc_count);

    try ProvisionedTableWriteSource.replaceRuntimeStatusesWithMergedRefresh(alloc, &current, &refresh);
    try std.testing.expectEqual(@as(usize, 1), current.items.len);
    try std.testing.expectEqual(@as(u64, 99), current.items[0].stats.doc_count);
}

test "provisioned table write source startup snapshot preserves existing group status" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-startup-overlay", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    try publishStartupCatchUpRuntimeStatusSnapshot(&source, alloc, "docs", 7001, .{
        .active = true,
        .phase = .opening_db,
        .wal_retention_known = true,
        .wal_retained_segments = 5,
        .wal_retained_bytes = 123,
    }, null, null);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(usize, 1), statuses.items[0].stats.indexes.len);
    try std.testing.expectEqualStrings("semantic_idx", statuses.items[0].stats.indexes[0].name);
    try std.testing.expect(statuses.items[0].stats.async_indexing.startup.active);
    try std.testing.expectEqual(db_mod.types.StartupCatchUpPhase.opening_db, statuses.items[0].stats.async_indexing.startup.phase);
    try std.testing.expectEqual(@as(u64, 5), statuses.items[0].stats.async_indexing.startup.wal_retained_segments);
    try std.testing.expectEqual(@as(u64, 123), statuses.items[0].stats.async_indexing.startup.wal_retained_bytes);
}

test "startup async overlay replaces async stats while preserving cached table stats" {
    var status = runtime_status.LocalTableRuntimeStatus{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = &.{},
            .async_indexing = .{
                .startup = .{
                    .active = true,
                    .phase = .opening_db,
                    .wal_retention_known = true,
                    .wal_retained_segments = 5,
                    .wal_retained_bytes = 123,
                },
                .dense_catch_up = .{
                    .active = false,
                    .current_sequence = 1,
                    .current_target_sequence = 2,
                    .current_scanned_entries = 3,
                    .current_applied_entries = 4,
                    .progress_updates = 5,
                },
            },
        },
    };

    applyStartupCatchUpAsyncOverlay(&status, .{
        .startup = .{
            .active = true,
            .phase = .artifact_rebuild,
            .configured_indexes = 2,
            .opened_indexes = 2,
        },
        .dense_catch_up = .{
            .active = true,
            .current_sequence = 10880,
            .current_target_sequence = 1001001,
            .current_scanned_entries = 10880,
            .current_applied_entries = 10880,
            .progress_updates = 91,
        },
    }, .{
        .active = true,
        .phase = .artifact_rebuild,
        .wal_retention_known = true,
        .wal_retained_segments = 7,
        .wal_retained_bytes = 456,
    });

    try std.testing.expectEqual(@as(u64, 9), status.stats.doc_count);
    try std.testing.expectEqual(db_mod.types.StartupCatchUpPhase.artifact_rebuild, status.stats.async_indexing.startup.phase);
    try std.testing.expectEqual(@as(u64, 7), status.stats.async_indexing.startup.wal_retained_segments);
    try std.testing.expectEqual(@as(u64, 456), status.stats.async_indexing.startup.wal_retained_bytes);
    try std.testing.expect(status.stats.async_indexing.dense_catch_up.active);
    try std.testing.expectEqual(@as(u64, 10880), status.stats.async_indexing.dense_catch_up.current_applied_entries);
}

test "provisioned table write source maintenance probes are best effort when local db is busy" {
    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-maintenance-probe", FakeCatalog.iface());
    try std.testing.expect(source.local_db_mutex.tryLock());
    defer source.local_db_mutex.unlock();

    try std.testing.expectEqual(@as(u64, 0), source.lsmMaintenanceScoreBestEffort());
    try std.testing.expect(source.hasActiveBulkIngestSession());
    try std.testing.expect(!try source.runLsmMaintenanceRoundBestEffort());
}

test "auto bulk best-effort finish does not spin when writer cache lock is busy" {
    var source = ProvisionedTableWriteSource.init(
        "/tmp/unused-antfly-auto-bulk-finish-busy",
        table_catalog.emptyCatalogSource(),
    );

    try std.testing.expectEqual(@as(?bool, false), source.tryFinishExpiredAutoBulkIngest());

    try std.testing.expect(source.local_db_mutex.tryLock());
    defer source.local_db_mutex.unlock();

    try std.testing.expectEqual(@as(?bool, null), source.tryFinishExpiredAutoBulkIngest());
    try std.testing.expect(!source.finishExpiredAutoBulkIngestBestEffort());
}

test "auto bulk max-window request waits for idle finish" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-auto-bulk-roll-without-next-write";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    source.write_cache = &write_cache;

    _ = try source.source().createTable(alloc, "docs", .{});

    const writes = try alloc.alloc(db_mod.types.BatchWrite, auto_bulk_ingest_min_batch_ops);
    defer {
        for (writes) |write| alloc.free(@constCast(write.key));
        alloc.free(writes);
    }
    for (writes, 0..) |*write, i| {
        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:bulk-roll:{d}", .{i}),
            .value = "{\"title\":\"bulk roll\"}",
        };
    }

    _ = try source.source().batch(alloc, "docs", .{
        .writes = writes,
        .timestamp_ns = 1,
        .sync_level = .write,
    });

    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try write_cache.ensureAutoBulkIngestLocked(7001, "docs", platform_time.monotonicNs());
    try std.testing.expect(write_cache.entries.items[0].*.auto_bulk_ingest_session_open);

    const requested_finish_ns = write_cache.entries.items[0].*.auto_bulk_ingest_last_ns + 1;
    try write_cache.recordAutoBulkIngestOpsLocked(7001, "docs", auto_bulk_ingest_max_window_ops, requested_finish_ns);
    try std.testing.expect(write_cache.entries.items[0].*.auto_bulk_ingest_finish_requested);

    try std.testing.expect(!try write_cache.finishExpiredAutoBulkIngestLocked(requested_finish_ns));
    try std.testing.expect(write_cache.entries.items[0].*.auto_bulk_ingest_session_open);
    try std.testing.expect(write_cache.entries.items[0].*.auto_bulk_ingest_finish_requested);
    try std.testing.expect(write_cache.entries.items[0].*.auto_bulk_ingest_ops >= auto_bulk_ingest_max_window_ops);

    const idle_finish_ns = write_cache.entries.items[0].*.auto_bulk_ingest_last_ns + auto_bulk_ingest_max_idle_ns;
    try std.testing.expect(try write_cache.finishExpiredAutoBulkIngestLocked(idle_finish_ns));
    try std.testing.expect(!write_cache.entries.items[0].*.auto_bulk_ingest_session_open);
    try std.testing.expectEqual(@as(usize, 0), write_cache.active_bulk_ingest_sessions.items.len);
}

test "auto bulk background finish skips entries with active foreground leases" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/antfly-api-auto-bulk-active-lease-skip", .{tmp.sub_path});
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    source.write_cache = &write_cache;

    _ = try source.source().createTable(alloc, "docs", .{});

    const writes = try alloc.alloc(db_mod.types.BatchWrite, auto_bulk_ingest_min_batch_ops);
    defer {
        for (writes) |write| alloc.free(@constCast(write.key));
        alloc.free(writes);
    }
    for (writes, 0..) |*write, i| {
        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:auto-bulk-active-lease:{d}", .{i}),
            .value = "{\"title\":\"active lease\"}",
        };
    }

    _ = try source.source().batch(alloc, "docs", .{
        .writes = writes,
        .timestamp_ns = 1,
        .sync_level = .write,
    });

    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try write_cache.ensureAutoBulkIngestLocked(7001, "docs", platform_time.monotonicNs());
    const entry = write_cache.entries.items[0];
    try std.testing.expect(entry.auto_bulk_ingest_session_open);
    try write_cache.recordAutoBulkIngestOpsLocked(7001, "docs", auto_bulk_ingest_max_window_ops, platform_time.monotonicNs());
    try std.testing.expect(entry.auto_bulk_ingest_finish_requested);

    entry.active_leases += 1;
    const skipped = try write_cache.finishExpiredAutoBulkIngestLocked(platform_time.monotonicNs());
    try std.testing.expect(!skipped);
    try std.testing.expect(entry.auto_bulk_ingest_session_open);
    try std.testing.expect(entry.auto_bulk_ingest_finish_requested);

    const foreground_roll = try write_cache.rollRequestedAutoBulkIngestLocked(7001, "docs", platform_time.monotonicNs());
    try std.testing.expect(foreground_roll);
    try std.testing.expect(entry.auto_bulk_ingest_session_open);
    try std.testing.expect(!entry.auto_bulk_ingest_finish_requested);
    try std.testing.expectEqual(@as(usize, 0), entry.auto_bulk_ingest_ops);

    entry.active_leases -= 1;
}

test "auto bulk group writes release leases so idle finish can publish" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/auto-bulk-group-write-release", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = "{\"search_idx\":{\"type\":\"full_text\",\"config\":{}}}",
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    const batch_size = auto_bulk_ingest_min_batch_ops;
    const total_docs = batch_size;
    const writes = try alloc.alloc(db_mod.types.BatchWrite, batch_size);
    defer {
        for (writes) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        alloc.free(writes);
    }
    for (writes, 0..) |*write, i| {
        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{i}),
            .value = try std.fmt.allocPrint(alloc, "{{\"body\":\"auto bulk doc {d}\"}}", .{i}),
        };
    }

    var offset: usize = 0;
    while (offset < total_docs) : (offset += batch_size) {
        for (writes, 0..) |*write, i| {
            alloc.free(@constCast(write.key));
            write.key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{offset + i});
        }

        _ = try source.source().batchGroupLocal(alloc, 7001, "docs", .{
            .writes = writes,
            .timestamp_ns = @intCast(offset + 1),
            .sync_level = .full_index,
        });

        try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
        try std.testing.expect(!write_cache.entries.items[0].auto_bulk_ingest_session_open);
        try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].active_leases);

        if (try source.source().localRuntimeStatuses(alloc, "docs")) |statuses| {
            var owned = statuses;
            owned.deinit(alloc);
        }
        try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].active_leases);
    }

    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].auto_bulk_ingest_ops);
    const replay_target = write_cache.entries.items[0].db.core.nextDerivedSequence();
    try std.testing.expect(replay_target > 0);
    try std.testing.expect(!write_cache.entries.items[0].auto_bulk_ingest_session_open);
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].active_leases);

    try std.testing.expect(source.publishManagedRuntimeStatusBestEffort("docs", 7001, &write_cache.entries.items[0].db));
    var statuses = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(usize, 1), statuses.items[0].stats.indexes.len);
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].active_leases);
}

test "bound table write source resolves internal group transactions into visible documents" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-txn-group-local";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    const txn_id = try distributed_txn.parseTxnIdHex("00112233445566778899aabbccddeeff");

    _ = try source.source().txnBeginGroupLocal(alloc, 7, "docs", txn_id, 10_000, 0, &.{"group:7"});
    _ = try source.source().txnPrepareGroupLocal(alloc, 7, "docs", txn_id, 0, .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });
    _ = try source.source().txnResolveGroupLocal(alloc, 7, "docs", txn_id, .committed, 10_001);

    try std.testing.expectEqual(db_mod.types.TxnStatus.committed, (try source.source().txnStatusGroupLocal(alloc, 7, "docs", txn_id)).?);

    var result = (try db.lookup(alloc, "doc:a", .{})).?;
    defer result.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, result.json, "\"alpha\"") != null);
}

test "bound table write source provisions default full text index on create" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-create";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{};
    defer req.deinit(alloc);
    req.indexes_json = try alloc.dupe(u8, tables_api.default_indexes_json);

    _ = try source.source().createTable(alloc, "docs", req);
    try std.testing.expect(db.core.index_manager.textIndex("full_text_index_v0") != null);
}

test "bound table write source rejects invalid batch writes against persisted schema" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-batch-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(u8, "{\"default_type\":\"doc\",\"enforce_types\":true,\"dynamic_templates\":{\"meta\":{\"match\":\"meta_*\",\"mapping\":{\"type\":\"keyword\"}}},\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}"),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    try std.testing.expect(db.core.schema != null);
    try std.testing.expectEqual(storage_schema.AntflyType.keyword, storage_schema.resolveFieldType(db.core.schema.?, "meta_status").?.field_type);
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"unexpected\"}" }},
    }));
}

test "bound table write source enforces nested required fields and array items" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-nested-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"required\":[\"author\",\"tags\"],\"properties\":{\"author\":{\"type\":\"object\",\"required\":[\"name\"],\"properties\":{\"name\":{\"type\":\"text\"},\"active\":{\"type\":\"boolean\"}}},\"tags\":{\"type\":\"array\",\"items\":{\"type\":\"keyword\"}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"author\":{\"name\":\"ann\",\"active\":true},\"tags\":[\"a\",\"b\"]}" }},
    });

    var written = (try db.lookup(alloc, "doc:good", .{})).?;
    defer written.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, written.json, "\"name\":\"ann\"") != null);

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing", .value = "{\"author\":{\"active\":true},\"tags\":[\"a\"]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:wrong-tag", .value = "{\"author\":{\"name\":\"ann\"},\"tags\":[1]}" }},
    }));
}

test "bound table write source enforces enums numeric bounds and anyOf" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-enum-bounds-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"status\":{\"type\":\"keyword\",\"enum\":[\"draft\",\"published\"]},\"score\":{\"type\":\"numeric\",\"minimum\":0,\"maximum\":10},\"metric\":{\"anyOf\":[{\"type\":\"numeric\",\"minimum\":0},{\"type\":\"keyword\",\"enum\":[\"n/a\"]}]}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"status\":\"draft\",\"score\":8,\"metric\":\"n/a\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-status", .value = "{\"status\":\"archived\",\"score\":8,\"metric\":\"n/a\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-score", .value = "{\"status\":\"draft\",\"score\":11,\"metric\":\"n/a\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-metric", .value = "{\"status\":\"draft\",\"score\":8,\"metric\":\"bad\"}" }},
    }));
}

test "bound table write source enforces oneOf allOf pattern and item cardinality" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-pattern-compose-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"sku\":{\"type\":\"keyword\",\"pattern\":\"^[A-Z]{3}-[0-9]{2}$\"},\"tags\":{\"type\":\"array\",\"minItems\":1,\"maxItems\":2,\"items\":{\"type\":\"keyword\"}},\"code\":{\"oneOf\":[{\"type\":\"keyword\",\"enum\":[\"A\"]},{\"type\":\"keyword\",\"enum\":[\"B\"]}]},\"score\":{\"allOf\":[{\"type\":\"numeric\",\"minimum\":0},{\"type\":\"numeric\",\"maximum\":5}]}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"sku\":\"ABC-12\",\"tags\":[\"x\"],\"code\":\"A\",\"score\":4}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-pattern", .value = "{\"sku\":\"bad\",\"tags\":[\"x\"],\"code\":\"A\",\"score\":4}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-min-items", .value = "{\"sku\":\"ABC-12\",\"tags\":[],\"code\":\"A\",\"score\":4}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-max-items", .value = "{\"sku\":\"ABC-12\",\"tags\":[\"x\",\"y\",\"z\"],\"code\":\"A\",\"score\":4}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-oneof", .value = "{\"sku\":\"ABC-12\",\"tags\":[\"x\"],\"code\":\"C\",\"score\":4}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-allof", .value = "{\"sku\":\"ABC-12\",\"tags\":[\"x\"],\"code\":\"A\",\"score\":8}" }},
    }));
}

test "bound table write source enforces string length and object cardinality" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-length-cardinality-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"minProperties\":2,\"maxProperties\":3,\"properties\":{\"title\":{\"type\":\"text\",\"minLength\":3,\"maxLength\":5},\"meta\":{\"type\":\"object\",\"minProperties\":1,\"maxProperties\":2,\"properties\":{\"a\":{\"type\":\"keyword\"},\"b\":{\"type\":\"keyword\"},\"c\":{\"type\":\"keyword\"}}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"title\":\"alpha\",\"meta\":{\"a\":\"x\"}}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-min-length", .value = "{\"title\":\"hi\",\"meta\":{\"a\":\"x\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-max-length", .value = "{\"title\":\"alphabet\",\"meta\":{\"a\":\"x\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-root-cardinality", .value = "{\"title\":\"alpha\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-object-cardinality", .value = "{\"title\":\"alpha\",\"meta\":{\"a\":\"x\",\"b\":\"y\",\"c\":\"z\"}}" }},
    }));
}

test "bound table write source backs up and restores a local table" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-table-backup-restore");
    defer alloc.free(path);
    const backup_root = try uniqueTestTmpPathAlloc(alloc, "antfly-api-table-backup-restore-out");
    defer alloc.free(backup_root);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    }

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .timestamp_ns = 1,
    });

    var source = BoundTableWriteSource.init("docs", &db);
    const shards = (try source.source().backupTable(alloc, "docs", .{
        .backup_root = backup_root,
        .backup_id = "snap1",
    })).?;
    defer freeBackupShards(alloc, shards);

    var manifest = try backups_api.createManifest(alloc, "snap1", &.{
        .table_id = 1,
        .name = "docs",
        .description = "docs table",
        .schema_json = "",
        .read_schema_json = "",
        .indexes_json = tables_api.default_indexes_json,
        .replication_sources_json = "[]",
    }, shards);
    defer manifest.deinit(alloc);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"beta\"}" }},
        .timestamp_ns = 2,
    });

    _ = try source.source().restoreTable(alloc, "docs", .{
        .backup_root = backup_root,
        .manifest = &manifest,
    });

    db.close();
    db = try db_mod.DB.open(alloc, path, .{});

    var restored = (try db.lookup(alloc, "doc:a", .{})).?;
    defer restored.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, restored.json, "\"alpha\"") != null);
}

test "bound table write source backs up and restores a portable local table" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-table-portable-backup-restore");
    defer alloc.free(path);
    const backup_root = try uniqueTestTmpPathAlloc(alloc, "antfly-api-table-portable-backup-restore-out");
    defer alloc.free(backup_root);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    }

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .timestamp_ns = 1,
    });

    var source = BoundTableWriteSource.init("docs", &db);
    const shards = (try source.source().backupTable(alloc, "docs", .{
        .backup_root = backup_root,
        .backup_id = "portable-snap",
        .format = .portable,
    })).?;
    defer freeBackupShards(alloc, shards);
    try std.testing.expectEqual(@as(usize, 1), shards.len);
    try std.testing.expectEqualStrings("portable-snap.afb", shards[0].snapshot_path);

    const afb_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ backup_root, shards[0].snapshot_path });
    defer alloc.free(afb_path);
    var afb_file = try std.Io.Dir.cwd().openFile(io_impl.io(), afb_path, .{});
    defer afb_file.close(io_impl.io());
    try std.testing.expect((try afb_file.stat(io_impl.io())).size > 0);

    var manifest = try backups_api.createManifest(alloc, "portable-snap", &.{
        .table_id = 1,
        .name = "docs",
        .description = "docs table",
        .schema_json = "",
        .read_schema_json = "",
        .indexes_json = tables_api.default_indexes_json,
        .replication_sources_json = "[]",
    }, shards);
    defer manifest.deinit(alloc);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"beta\"}" }},
        .timestamp_ns = 2,
    });

    _ = try source.source().restoreTable(alloc, "docs", .{
        .backup_root = backup_root,
        .manifest = &manifest,
    });

    var restored = (try db.lookup(alloc, "doc:a", .{})).?;
    defer restored.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, restored.json, "\"alpha\"") != null);
}

test "bound table write source enforces root conditionals not and unique items" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-conditional-unique-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"if\":{\"required\":[\"kind\"],\"properties\":{\"kind\":{\"enum\":[\"story\"]}}},\"then\":{\"required\":[\"headline\"]},\"else\":{\"required\":[\"slug\"]},\"properties\":{\"kind\":{\"type\":\"keyword\",\"enum\":[\"story\",\"note\"]},\"headline\":{\"type\":\"text\"},\"slug\":{\"type\":\"keyword\"},\"tags\":{\"type\":\"array\",\"uniqueItems\":true,\"items\":{\"type\":\"keyword\"}},\"status\":{\"type\":\"keyword\",\"not\":{\"enum\":[\"archived\"]}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:story", .value = "{\"kind\":\"story\",\"headline\":\"alpha\",\"tags\":[\"a\",\"b\"],\"status\":\"draft\"}" }},
    });
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:note", .value = "{\"kind\":\"note\",\"slug\":\"alpha\",\"tags\":[\"a\",\"b\"],\"status\":\"draft\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing-headline", .value = "{\"kind\":\"story\",\"tags\":[\"a\",\"b\"],\"status\":\"draft\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing-slug", .value = "{\"kind\":\"note\",\"tags\":[\"a\",\"b\"],\"status\":\"draft\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:duplicate-tags", .value = "{\"kind\":\"story\",\"headline\":\"alpha\",\"tags\":[\"a\",\"a\"],\"status\":\"draft\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-status", .value = "{\"kind\":\"story\",\"headline\":\"alpha\",\"tags\":[\"a\",\"b\"],\"status\":\"archived\"}" }},
    }));
}

test "bound table write source enforces property names and dependent required" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-property-names-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":false,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"dependentRequired\":{\"kind\":[\"slug\"]},\"properties\":{\"kind\":{\"type\":\"keyword\"},\"slug\":{\"type\":\"keyword\"},\"attrs\":{\"type\":\"object\",\"propertyNames\":{\"pattern\":\"^meta_[a-z]+$\"}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"attrs\":{\"meta_color\":\"red\"}}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing-dependent", .value = "{\"kind\":\"story\",\"attrs\":{\"meta_color\":\"red\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-property-name", .value = "{\"slug\":\"alpha\",\"attrs\":{\"bad\":\"red\"}}" }},
    }));
}

test "bound table write source enforces dependent schemas" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-dependent-schemas";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":false,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"kind\":{\"type\":\"keyword\"},\"slug\":{\"type\":\"keyword\"},\"details\":{\"type\":\"text\"}},\"dependentSchemas\":{\"kind\":{\"required\":[\"slug\"],\"properties\":{\"kind\":{\"const\":\"story\"},\"slug\":{\"type\":\"keyword\"}}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"details\":\"ok\"}" }},
    });
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:no-trigger", .value = "{\"details\":\"ok\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing-slug", .value = "{\"kind\":\"story\",\"details\":\"ok\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-kind", .value = "{\"kind\":\"note\",\"slug\":\"alpha\",\"details\":\"ok\"}" }},
    }));
}

test "bound table write source enforces additional properties" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-additional-properties";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":false,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"title\":{\"type\":\"text\"},\"meta\":{\"type\":\"object\",\"additionalProperties\":{\"type\":\"keyword\"}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"title\":\"alpha\",\"meta\":{\"a\":\"x\",\"b\":\"y\"}}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-root-extra", .value = "{\"title\":\"alpha\",\"body\":\"unexpected\",\"meta\":{\"a\":\"x\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-meta-extra", .value = "{\"title\":\"alpha\",\"meta\":{\"a\":1}}" }},
    }));
}

test "bound table write source enforces contains semantics" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-contains";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"tags\":{\"type\":\"array\",\"contains\":{\"type\":\"keyword\",\"const\":\"hot\"},\"minContains\":1,\"maxContains\":2},\"scores\":{\"type\":\"array\",\"contains\":{\"type\":\"numeric\",\"minimum\":10}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"tags\":[\"hot\",\"warm\"],\"scores\":[1,10,20]}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing-contains", .value = "{\"tags\":[\"warm\"],\"scores\":[10]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:too-many-contains", .value = "{\"tags\":[\"hot\",\"hot\",\"hot\"],\"scores\":[10]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing-score-match", .value = "{\"tags\":[\"hot\"],\"scores\":[1,2,3]}" }},
    }));
}

test "bound table write source enforces prefix items and pattern properties" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-prefix-pattern";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":false,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"coords\":{\"type\":\"array\",\"prefixItems\":[{\"type\":\"keyword\",\"const\":\"point\"},{\"type\":\"numeric\"}],\"items\":{\"type\":\"numeric\"}},\"meta\":{\"type\":\"object\",\"patternProperties\":{\"^meta_[a-z]+$\":{\"type\":\"keyword\"},\"^flag_[a-z]+$\":{\"type\":\"boolean\"}},\"additionalProperties\":false}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"coords\":[\"point\",1,2,3],\"meta\":{\"meta_color\":\"red\",\"flag_ready\":true}}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-prefix-const", .value = "{\"coords\":[1,1,2],\"meta\":{\"meta_color\":\"red\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-prefix-type", .value = "{\"coords\":[\"point\",\"bad\"],\"meta\":{\"meta_color\":\"red\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-pattern-type", .value = "{\"coords\":[\"point\",1],\"meta\":{\"meta_color\":1}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-pattern-extra", .value = "{\"coords\":[\"point\",1],\"meta\":{\"other\":\"x\"}}" }},
    }));
}

test "bound table write source enforces exclusive numeric bounds and multipleOf" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-exclusive-multiple";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"score\":{\"type\":\"numeric\",\"exclusiveMinimum\":0,\"exclusiveMaximum\":10,\"multipleOf\":0.5}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"score\":5.5}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-exclusive-min", .value = "{\"score\":0}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-exclusive-max", .value = "{\"score\":10}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-multiple", .value = "{\"score\":5.25}" }},
    }));
}

test "bound table write source enforces nullable and type-array fields" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-nullable-types";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"},\"subtitle\":{\"type\":[\"text\",\"null\"]},\"score\":{\"type\":\"numeric\",\"nullable\":true},\"flag\":{\"type\":[\"boolean\"]}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good-nullable", .value = "{\"title\":\"alpha\",\"subtitle\":null,\"score\":null,\"flag\":true}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-title-null", .value = "{\"title\":null,\"subtitle\":\"beta\",\"score\":1,\"flag\":true}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-flag-null", .value = "{\"title\":\"alpha\",\"subtitle\":\"beta\",\"score\":1,\"flag\":null}" }},
    }));
}

test "bound table write source enforces local defs and refs" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-defs-refs";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"$defs\":{\"titleField\":{\"type\":\"text\"},\"metaField\":{\"type\":\"object\",\"properties\":{\"status\":{\"type\":\"keyword\"}}},\"scoreField\":{\"type\":\"numeric\",\"nullable\":true}},\"properties\":{\"title\":{\"$ref\":\"#/$defs/titleField\"},\"meta\":{\"$ref\":\"#/$defs/metaField\"},\"score\":{\"$ref\":\"#/$defs/scoreField\"}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"title\":\"alpha\",\"meta\":{\"status\":\"ready\"},\"score\":null}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-title", .value = "{\"title\":1,\"meta\":{\"status\":\"ready\"},\"score\":null}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-meta", .value = "{\"title\":\"alpha\",\"meta\":{\"status\":1},\"score\":null}" }},
    }));
}

test "bound table write source enforces ref siblings and nested local defs" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-ref-siblings-local-defs";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"$defs\":{\"titleField\":{\"type\":\"text\"},\"sharedText\":{\"type\":\"text\",\"minLength\":8}},\"properties\":{\"title\":{\"$ref\":\"#/$defs/titleField\",\"minLength\":3},\"meta\":{\"type\":\"object\",\"$defs\":{\"sharedText\":{\"type\":\"text\",\"minLength\":4}},\"properties\":{\"note\":{\"$ref\":\"#/$defs/sharedText\",\"maxLength\":6}}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"title\":\"alpha\",\"meta\":{\"note\":\"short\"}}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-title", .value = "{\"title\":\"ab\",\"meta\":{\"note\":\"short\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-local-def", .value = "{\"title\":\"alpha\",\"meta\":{\"note\":\"abc\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-sibling", .value = "{\"title\":\"alpha\",\"meta\":{\"note\":\"toolong\"}}" }},
    }));
}

test "bound table write source enforces recursive root refs" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-recursive-root-refs";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"node\",\"enforce_types\":true,\"document_schemas\":{\"node\":{\"schema\":{\"type\":\"object\",\"required\":[\"name\"],\"properties\":{\"name\":{\"type\":\"text\"},\"children\":{\"type\":\"array\",\"items\":{\"$ref\":\"#\"}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "node:good", .value = "{\"name\":\"root\",\"children\":[{\"name\":\"leaf\",\"children\":[]},{\"name\":\"branch\",\"children\":[{\"name\":\"twig\"}]}]}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "node:bad-child-type", .value = "{\"name\":\"root\",\"children\":[{\"name\":1}]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "node:bad-null-child", .value = "{\"name\":\"root\",\"children\":[null]}" }},
    }));
}

test "bound table write source enforces format and additionalItems" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-format-additional-items";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"email\":{\"type\":\"keyword\",\"format\":\"email\"},\"site\":{\"type\":\"keyword\",\"format\":\"uri\"},\"id\":{\"type\":\"keyword\",\"format\":\"uuid\"},\"coords\":{\"type\":\"array\",\"prefixItems\":[{\"type\":\"keyword\",\"const\":\"point\"},{\"type\":\"numeric\"}],\"additionalItems\":false},\"labels\":{\"type\":\"array\",\"prefixItems\":[{\"type\":\"keyword\"}],\"additionalItems\":{\"type\":\"keyword\",\"pattern\":\"^[a-z]+$\"}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"email\":\"a@example.com\",\"site\":\"https://example.com/docs\",\"id\":\"123e4567-e89b-12d3-a456-426614174000\",\"coords\":[\"point\",1],\"labels\":[\"seed\",\"alpha\",\"beta\"]}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-email", .value = "{\"email\":\"bad\",\"site\":\"https://example.com/docs\",\"id\":\"123e4567-e89b-12d3-a456-426614174000\",\"coords\":[\"point\",1],\"labels\":[\"seed\",\"alpha\"]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-uri", .value = "{\"email\":\"a@example.com\",\"site\":\"not a uri\",\"id\":\"123e4567-e89b-12d3-a456-426614174000\",\"coords\":[\"point\",1],\"labels\":[\"seed\",\"alpha\"]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-uuid", .value = "{\"email\":\"a@example.com\",\"site\":\"https://example.com/docs\",\"id\":\"bad-uuid\",\"coords\":[\"point\",1],\"labels\":[\"seed\",\"alpha\"]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-extra-items", .value = "{\"email\":\"a@example.com\",\"site\":\"https://example.com/docs\",\"id\":\"123e4567-e89b-12d3-a456-426614174000\",\"coords\":[\"point\",1,2],\"labels\":[\"seed\",\"alpha\"]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-additional-schema", .value = "{\"email\":\"a@example.com\",\"site\":\"https://example.com/docs\",\"id\":\"123e4567-e89b-12d3-a456-426614174000\",\"coords\":[\"point\",1],\"labels\":[\"seed\",1]}" }},
    }));
}

test "bound table write source enforces broader string formats" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-broader-formats";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"published_at\":{\"type\":\"keyword\",\"format\":\"date-time\"},\"birthday\":{\"type\":\"keyword\",\"format\":\"date\"},\"v4\":{\"type\":\"keyword\",\"format\":\"ipv4\"},\"v6\":{\"type\":\"keyword\",\"format\":\"ipv6\"},\"host\":{\"type\":\"keyword\",\"format\":\"hostname\"},\"ref\":{\"type\":\"keyword\",\"format\":\"uri-reference\"}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"published_at\":\"2024-01-02T03:04:05Z\",\"birthday\":\"2024-01-02\",\"v4\":\"192.168.1.10\",\"v6\":\"2001:db8::1\",\"host\":\"api.example.com\",\"ref\":\"/docs/intro\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-datetime", .value = "{\"published_at\":\"2024-01-02\",\"birthday\":\"2024-01-02\",\"v4\":\"192.168.1.10\",\"v6\":\"2001:db8::1\",\"host\":\"api.example.com\",\"ref\":\"/docs/intro\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-date", .value = "{\"published_at\":\"2024-01-02T03:04:05Z\",\"birthday\":\"2024-13-02\",\"v4\":\"192.168.1.10\",\"v6\":\"2001:db8::1\",\"host\":\"api.example.com\",\"ref\":\"/docs/intro\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-ipv4", .value = "{\"published_at\":\"2024-01-02T03:04:05Z\",\"birthday\":\"2024-01-02\",\"v4\":\"999.1.1.1\",\"v6\":\"2001:db8::1\",\"host\":\"api.example.com\",\"ref\":\"/docs/intro\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-ipv6", .value = "{\"published_at\":\"2024-01-02T03:04:05Z\",\"birthday\":\"2024-01-02\",\"v4\":\"192.168.1.10\",\"v6\":\"invalid\",\"host\":\"api.example.com\",\"ref\":\"/docs/intro\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-host", .value = "{\"published_at\":\"2024-01-02T03:04:05Z\",\"birthday\":\"2024-01-02\",\"v4\":\"192.168.1.10\",\"v6\":\"2001:db8::1\",\"host\":\"-bad-host\",\"ref\":\"/docs/intro\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-ref", .value = "{\"published_at\":\"2024-01-02T03:04:05Z\",\"birthday\":\"2024-01-02\",\"v4\":\"192.168.1.10\",\"v6\":\"2001:db8::1\",\"host\":\"api.example.com\",\"ref\":\"/docs bad\"}" }},
    }));
}

test "bound table write source enforces unevaluated properties and items" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-unevaluated";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"kind\":{\"type\":\"keyword\"},\"meta\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}},\"unevaluatedProperties\":{\"type\":\"keyword\"}},\"coords\":{\"type\":\"array\",\"prefixItems\":[{\"type\":\"keyword\",\"const\":\"point\"}],\"unevaluatedItems\":{\"type\":\"numeric\"}}},\"unevaluatedProperties\":false}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\",\"slug\":\"ok\"},\"coords\":[\"point\",1,2]}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-root-extra", .value = "{\"kind\":\"story\",\"extra\":\"bad\",\"meta\":{\"title\":\"alpha\"},\"coords\":[\"point\",1]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-meta-extra", .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\",\"slug\":1},\"coords\":[\"point\",1]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-unevaluated-item", .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\",\"slug\":\"ok\"},\"coords\":[\"point\",\"bad\"]}" }},
    }));
}

test "bound table write source enforces composed unevaluated coverage" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-unevaluated-composed";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"allOf\":[{\"properties\":{\"kind\":{\"type\":\"keyword\"}}},{\"properties\":{\"meta\":{\"type\":\"object\",\"allOf\":[{\"properties\":{\"title\":{\"type\":\"text\"}}}],\"unevaluatedProperties\":false}}},{\"properties\":{\"coords\":{\"type\":\"array\",\"anyOf\":[{\"prefixItems\":[{\"const\":\"point\"},{\"type\":\"numeric\"}],\"unevaluatedItems\":false},{\"prefixItems\":[{\"const\":\"line\"},{\"type\":\"numeric\"},{\"type\":\"numeric\"}],\"unevaluatedItems\":false}]}}}],\"unevaluatedProperties\":false}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good-point", .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\"},\"coords\":[\"point\",1]}" }},
    });
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good-line", .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\"},\"coords\":[\"line\",1,2]}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-root-extra", .value = "{\"kind\":\"story\",\"extra\":\"bad\",\"meta\":{\"title\":\"alpha\"},\"coords\":[\"point\",1]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-meta-extra", .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\",\"slug\":\"bad\"},\"coords\":[\"point\",1]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-array-extra", .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\"},\"coords\":[\"point\",1,2]}" }},
    }));
}

test "bound table write source enforces root unevaluated properties" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-root-unevaluated";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"kind\":{\"type\":\"keyword\"}},\"unevaluatedProperties\":{\"type\":\"keyword\"}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"kind\":\"story\",\"slug\":\"ok\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad", .value = "{\"kind\":\"story\",\"slug\":1}" }},
    }));
}

test "bound table write source enforces conditional and dependency unevaluated coverage" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-conditional-unevaluated";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"kind\":{\"type\":\"keyword\"}},\"if\":{\"properties\":{\"kind\":{\"const\":\"story\"}}},\"then\":{\"required\":[\"slug\"],\"properties\":{\"slug\":{\"type\":\"keyword\"}}},\"else\":{\"required\":[\"rating\"],\"properties\":{\"rating\":{\"type\":\"numeric\"}}},\"dependentSchemas\":{\"kind\":{\"properties\":{\"details\":{\"type\":\"text\"}}}},\"unevaluatedProperties\":false}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:story", .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"details\":\"body\"}" }},
    });
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:score", .value = "{\"kind\":\"score\",\"rating\":5,\"details\":\"body\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing-slug", .value = "{\"kind\":\"story\",\"details\":\"body\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing-rating", .value = "{\"kind\":\"score\",\"details\":\"body\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:extra", .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"details\":\"body\",\"extra\":\"bad\"}" }},
    }));
}

test "bound table write source enforces anyOf and oneOf branch evaluation coverage" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-branch-unevaluated";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"kind\":{\"type\":\"keyword\"}},\"allOf\":[{\"properties\":{\"meta\":{\"type\":\"object\",\"anyOf\":[{\"properties\":{\"mode\":{\"const\":\"alpha\"},\"a\":{\"type\":\"keyword\"}}},{\"properties\":{\"mode\":{\"const\":\"beta\"},\"b\":{\"type\":\"numeric\"}}}],\"unevaluatedProperties\":false}}},{\"properties\":{\"choice\":{\"type\":\"object\",\"oneOf\":[{\"properties\":{\"mode\":{\"const\":\"left\"},\"left\":{\"type\":\"keyword\"}},\"required\":[\"mode\",\"left\"]},{\"properties\":{\"mode\":{\"const\":\"right\"},\"right\":{\"type\":\"numeric\"}},\"required\":[\"mode\",\"right\"]}],\"unevaluatedProperties\":false}}}],\"unevaluatedProperties\":false}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:alpha-left", .value = "{\"kind\":\"story\",\"meta\":{\"mode\":\"alpha\",\"a\":\"ok\"},\"choice\":{\"mode\":\"left\",\"left\":\"x\"}}" }},
    });
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:beta-right", .value = "{\"kind\":\"story\",\"meta\":{\"mode\":\"beta\",\"b\":3},\"choice\":{\"mode\":\"right\",\"right\":9}}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:wrong-anyof-alpha", .value = "{\"kind\":\"story\",\"meta\":{\"mode\":\"alpha\",\"b\":3},\"choice\":{\"mode\":\"left\",\"left\":\"x\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:wrong-anyof-beta", .value = "{\"kind\":\"story\",\"meta\":{\"mode\":\"beta\",\"a\":\"oops\"},\"choice\":{\"mode\":\"right\",\"right\":9}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:wrong-oneof", .value = "{\"kind\":\"story\",\"meta\":{\"mode\":\"alpha\",\"a\":\"ok\"},\"choice\":{\"mode\":\"left\",\"right\":9}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:extra", .value = "{\"kind\":\"story\",\"meta\":{\"mode\":\"alpha\",\"a\":\"ok\",\"extra\":\"bad\"},\"choice\":{\"mode\":\"left\",\"left\":\"x\"}}" }},
    }));
}

test "bound table write source enforces anyOf and oneOf array evaluation coverage" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-array-branch-unevaluated";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"coords\":{\"type\":\"array\",\"anyOf\":[{\"minItems\":2,\"prefixItems\":[{\"const\":\"point\"},{\"type\":\"numeric\"}],\"unevaluatedItems\":false},{\"minItems\":3,\"prefixItems\":[{\"const\":\"line\"},{\"type\":\"numeric\"},{\"type\":\"numeric\"}],\"unevaluatedItems\":false}]},\"choice\":{\"type\":\"array\",\"oneOf\":[{\"minItems\":2,\"prefixItems\":[{\"const\":\"left\"},{\"type\":\"keyword\"}],\"unevaluatedItems\":false},{\"minItems\":2,\"prefixItems\":[{\"const\":\"right\"},{\"type\":\"numeric\"}],\"unevaluatedItems\":false}]}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:point-left", .value = "{\"coords\":[\"point\",1],\"choice\":[\"left\",\"ok\"]}" }},
    });
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:line-right", .value = "{\"coords\":[\"line\",1,2],\"choice\":[\"right\",9]}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:coords-extra", .value = "{\"coords\":[\"point\",1,2],\"choice\":[\"left\",\"ok\"]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:coords-short", .value = "{\"coords\":[\"line\",1],\"choice\":[\"right\",9]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:choice-wrong-branch", .value = "{\"coords\":[\"point\",1],\"choice\":[\"left\",9]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:choice-extra", .value = "{\"coords\":[\"point\",1],\"choice\":[\"right\",9,10]}" }},
    }));
}

test "bound table write source enforces composed contains-driven array evaluation coverage" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-array-contains-unevaluated";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"series\":{\"type\":\"array\",\"allOf\":[{\"minItems\":2,\"prefixItems\":[{\"const\":\"set\"}]},{\"contains\":{\"type\":\"numeric\",\"minimum\":10},\"minContains\":1}],\"unevaluatedItems\":false},\"selector\":{\"type\":\"array\",\"anyOf\":[{\"contains\":{\"const\":\"hot\"},\"minContains\":1,\"unevaluatedItems\":false},{\"contains\":{\"const\":\"cold\"},\"minContains\":1,\"unevaluatedItems\":false}]},\"exclusive\":{\"type\":\"array\",\"oneOf\":[{\"contains\":{\"const\":\"left\"},\"minContains\":1,\"unevaluatedItems\":false},{\"contains\":{\"const\":\"right\"},\"minContains\":1,\"unevaluatedItems\":false}]}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:hot-left", .value = "{\"series\":[\"set\",10,11],\"selector\":[\"hot\"],\"exclusive\":[\"left\"]}" }},
    });
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:cold-right", .value = "{\"series\":[\"set\",12],\"selector\":[\"cold\"],\"exclusive\":[\"right\"]}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:series-leftover", .value = "{\"series\":[\"set\",10,1],\"selector\":[\"hot\"],\"exclusive\":[\"left\"]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:selector-no-branch", .value = "{\"series\":[\"set\",12],\"selector\":[\"warm\"],\"exclusive\":[\"left\"]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:selector-overlap", .value = "{\"series\":[\"set\",12],\"selector\":[\"hot\",\"cold\"],\"exclusive\":[\"left\"]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:exclusive-overlap", .value = "{\"series\":[\"set\",12],\"selector\":[\"hot\"],\"exclusive\":[\"left\",\"right\"]}" }},
    }));
}

test "bound table write source enforces composed pattern and additional properties evaluation coverage" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-pattern-additional-unevaluated";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"meta\":{\"type\":\"object\",\"allOf\":[{\"patternProperties\":{\"^meta_[a-z]+$\":{\"type\":\"keyword\"}}},{\"properties\":{\"count\":{\"type\":\"numeric\"}}}],\"unevaluatedProperties\":false},\"choice\":{\"type\":\"object\",\"anyOf\":[{\"patternProperties\":{\"^flag_[a-z]+$\":{\"type\":\"boolean\"}}},{\"additionalProperties\":{\"type\":\"numeric\"}}],\"unevaluatedProperties\":false},\"exclusive\":{\"type\":\"object\",\"oneOf\":[{\"patternProperties\":{\"^name_[a-z]+$\":{\"type\":\"text\"}},\"unevaluatedProperties\":false},{\"additionalProperties\":{\"type\":\"numeric\"},\"unevaluatedProperties\":false}],\"unevaluatedProperties\":false}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:pattern", .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"flag_enabled\":true},\"exclusive\":{\"name_primary\":\"alpha\"}}" }},
    });
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:additional", .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"score\":7},\"exclusive\":{\"score\":9}}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:meta-extra", .value = "{\"meta\":{\"meta_title\":\"ok\",\"other\":\"bad\"},\"choice\":{\"flag_enabled\":true},\"exclusive\":{\"name_primary\":\"alpha\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:choice-typed-wrong", .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"flag_enabled\":\"bad\"},\"exclusive\":{\"name_primary\":\"alpha\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:choice-overlap", .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"flag_enabled\":true,\"score\":7},\"exclusive\":{\"name_primary\":\"alpha\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:exclusive-overlap", .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"score\":7},\"exclusive\":{\"name_primary\":\"alpha\",\"score\":9}}" }},
    }));
}

test "bound table write source enforces composed ref closure evaluation coverage" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-ref-pattern-additional";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"$defs\":{\"meta_patterns\":{\"patternProperties\":{\"^meta_[a-z]+$\":{\"type\":\"keyword\"}}},\"meta_count\":{\"properties\":{\"count\":{\"type\":\"numeric\"}}},\"choice_flags\":{\"patternProperties\":{\"^flag_[a-z]+$\":{\"type\":\"boolean\"}}},\"choice_numbers\":{\"additionalProperties\":{\"type\":\"numeric\"}},\"exclusive_names\":{\"patternProperties\":{\"^name_[a-z]+$\":{\"type\":\"text\"}},\"unevaluatedProperties\":false},\"exclusive_numbers\":{\"additionalProperties\":{\"type\":\"numeric\"},\"unevaluatedProperties\":false}},\"properties\":{\"meta\":{\"type\":\"object\",\"allOf\":[{\"$ref\":\"#/$defs/meta_patterns\"},{\"$ref\":\"#/$defs/meta_count\"}],\"unevaluatedProperties\":false},\"choice\":{\"type\":\"object\",\"anyOf\":[{\"$ref\":\"#/$defs/choice_flags\"},{\"$ref\":\"#/$defs/choice_numbers\"}],\"unevaluatedProperties\":false},\"exclusive\":{\"type\":\"object\",\"oneOf\":[{\"$ref\":\"#/$defs/exclusive_names\"},{\"$ref\":\"#/$defs/exclusive_numbers\"}],\"unevaluatedProperties\":false}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:pattern", .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"flag_enabled\":true},\"exclusive\":{\"name_primary\":\"alpha\"}}" }},
    });
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:additional", .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"score\":7},\"exclusive\":{\"score\":9}}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:meta-extra", .value = "{\"meta\":{\"meta_title\":\"ok\",\"other\":\"bad\"},\"choice\":{\"flag_enabled\":true},\"exclusive\":{\"name_primary\":\"alpha\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:choice-bad", .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"flag_enabled\":\"bad\"},\"exclusive\":{\"name_primary\":\"alpha\"}}" }},
    }));
}

test "bound table write source enforces nullable composed refs" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-nullable-composed-refs";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"$defs\":{\"nullable_keyword\":{\"type\":[\"keyword\",\"null\"]},\"null_or_x\":{\"anyOf\":[{\"const\":null},{\"type\":\"keyword\",\"enum\":[\"x\"]}]}},\"properties\":{\"maybe\":{\"allOf\":[{\"$ref\":\"#/$defs/nullable_keyword\"},{\"$ref\":\"#/$defs/null_or_x\"}]}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:null", .value = "{\"maybe\":null}" }},
    });
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:x", .value = "{\"maybe\":\"x\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:y", .value = "{\"maybe\":\"y\"}" }},
    }));
}

test "bound table write source enforces recursive ref closure semantics" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-recursive-closure";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("nodes", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"node\",\"enforce_types\":true,\"document_schemas\":{\"node\":{\"schema\":{\"type\":\"object\",\"required\":[\"name\"],\"properties\":{\"name\":{\"type\":\"text\"},\"meta\":{\"type\":\"object\",\"allOf\":[{\"patternProperties\":{\"^tag_[a-z]+$\":{\"type\":\"keyword\"}}},{\"properties\":{\"count\":{\"type\":\"numeric\"}}}],\"unevaluatedProperties\":false},\"children\":{\"type\":\"array\",\"items\":{\"$ref\":\"#\"}}},\"unevaluatedProperties\":false}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "nodes", req);
    _ = try source.source().batch(alloc, "nodes", .{
        .writes = &.{.{ .key = "node:root", .value = "{\"name\":\"root\",\"meta\":{\"tag_kind\":\"oak\",\"count\":2},\"children\":[{\"name\":\"leaf\",\"meta\":{\"tag_kind\":\"leaf\"},\"children\":[]},{\"name\":\"branch\",\"meta\":{\"tag_kind\":\"branch\",\"count\":1},\"children\":[{\"name\":\"twig\",\"meta\":{\"tag_kind\":\"twig\"}}]}]}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "nodes", .{
        .writes = &.{.{ .key = "node:extra", .value = "{\"name\":\"root\",\"meta\":{\"tag_kind\":\"oak\",\"count\":2},\"children\":[{\"name\":\"leaf\",\"extra\":\"bad\"}]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "nodes", .{
        .writes = &.{.{ .key = "node:meta-extra", .value = "{\"name\":\"root\",\"meta\":{\"tag_kind\":\"oak\",\"other\":\"bad\"},\"children\":[]}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "nodes", .{
        .writes = &.{.{ .key = "node:meta-type", .value = "{\"name\":\"root\",\"meta\":{\"tag_kind\":\"oak\",\"count\":2},\"children\":[{\"name\":\"leaf\",\"meta\":{\"tag_kind\":1}}]}" }},
    }));
}

test "bound table write source enforces escaped ref tokens and direct fragment refs" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-ref-escaped-hash";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"$defs\":{\"slash/name\":{\"type\":\"text\"},\"tilde~name\":{\"type\":\"keyword\"}},\"properties\":{\"title\":{\"$ref\":\"#/$defs/slash~1name\"},\"kind\":{\"$ref\":\"#/$defs/tilde~0name\"},\"meta\":{\"type\":\"object\",\"$defs\":{\"local/name\":{\"type\":\"text\"}},\"properties\":{\"note\":{\"$ref\":\"#/properties/meta/$defs/local~1name\"},\"shadow\":{\"$ref\":\"#/properties/title\"}},\"required\":[\"note\",\"shadow\"]}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"title\":\"alpha\",\"kind\":\"ready\",\"meta\":{\"note\":\"short\",\"shadow\":\"again\"}}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-slash-ref", .value = "{\"title\":1,\"kind\":\"ready\",\"meta\":{\"note\":\"short\",\"shadow\":\"again\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-tilde-ref", .value = "{\"title\":\"alpha\",\"kind\":true,\"meta\":{\"note\":\"short\",\"shadow\":\"again\"}}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-direct-fragment", .value = "{\"title\":\"alpha\",\"kind\":\"ready\",\"meta\":{\"note\":\"short\",\"shadow\":1}}" }},
    }));
}

test "bound table write source enforces legacy dependencies keyword" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-legacy-dependencies";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"enforce_types\":false,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"kind\":{\"type\":\"keyword\"},\"slug\":{\"type\":\"keyword\"},\"mode\":{\"type\":\"keyword\"},\"details\":{\"type\":\"text\"}},\"dependencies\":{\"kind\":[\"slug\"],\"mode\":{\"required\":[\"details\"],\"properties\":{\"mode\":{\"const\":\"long\"},\"details\":{\"type\":\"text\"}}}}}}}}",
        ),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:good", .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"mode\":\"long\",\"details\":\"ok\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing-slug", .value = "{\"kind\":\"story\",\"mode\":\"long\",\"details\":\"ok\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:missing-details", .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"mode\":\"long\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:bad-mode-const", .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"mode\":\"short\",\"details\":\"ok\"}" }},
    }));
}

test "bound table write source rejects invalid commit writes against persisted schema" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-commit-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(u8, "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}"),
    };
    defer req.deinit(alloc);
    _ = try source.source().createTable(alloc, "docs", req);

    try std.testing.expectError(error.InvalidBatchRequest, source.source().commitTransaction(alloc, &.{.{
        .table_name = "docs",
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"unexpected\"}" }},
    }}, .write));
}

test "bound table write source rejects invalid commit transforms against persisted schema" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-commit-transform-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(u8, "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}"),
    };
    defer req.deinit(alloc);
    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().commitTransaction(alloc, &.{.{
        .table_name = "docs",
        .transforms = &.{.{
            .key = "doc:a",
            .operations = &.{
                .{ .op = .set, .path = "body", .value_json = "\"unexpected\"" },
            },
        }},
    }}, .write));
}

test "bound table write source rejects invalid txn prepare writes against persisted schema" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-prepare-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(u8, "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}"),
    };
    defer req.deinit(alloc);
    _ = try source.source().createTable(alloc, "docs", req);

    const txn_id = try distributed_txn.parseTxnIdHex("11112222333344445555666677778888");
    _ = try source.source().txnBeginGroupLocal(alloc, 7, "docs", txn_id, 10_000, 0, &.{"group:7"});
    try std.testing.expectError(error.InvalidBatchRequest, source.source().txnPrepareGroupLocal(alloc, 7, "docs", txn_id, 0, .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"unexpected\"}" }},
    }));
}

test "bound table write source rejects invalid txn prepare transforms against persisted schema" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-prepare-transform-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(u8, "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}"),
    };
    defer req.deinit(alloc);
    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });

    const txn_id = try distributed_txn.parseTxnIdHex("11112222333344445555666677779999");
    _ = try source.source().txnBeginGroupLocal(alloc, 7, "docs", txn_id, 10_000, 0, &.{"group:7"});
    try std.testing.expectError(error.InvalidBatchRequest, source.source().txnPrepareGroupLocal(alloc, 7, "docs", txn_id, 0, .{
        .transforms = &.{.{
            .key = "doc:a",
            .operations = &.{
                .{ .op = .set, .path = "body", .value_json = "\"unexpected\"" },
            },
        }},
    }));
}

test "bound table write source rejects invalid batch transforms against persisted schema" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-batch-transform-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(u8, "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}"),
    };
    defer req.deinit(alloc);
    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .transforms = &.{.{
            .key = "doc:a",
            .operations = &.{
                .{ .op = .set, .path = "body", .value_json = "\"unexpected\"" },
            },
        }},
    }));
}

test "bound table write source validates transforms against same-batch writes" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-batch-transform-same-request-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(u8, "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"},\"aliases\":{\"type\":\"array\",\"items\":{\"type\":\"keyword\"}}}}}}}"),
    };
    defer req.deinit(alloc);
    _ = try source.source().createTable(alloc, "docs", req);

    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"curated\"}" }},
        .transforms = &.{.{
            .key = "doc:a",
            .upsert = true,
            .operations = &.{
                .{ .op = .set_on_insert, .path = "body", .value_json = "\"would-be-invalid-on-insert\"" },
                .{ .op = .add_to_set, .path = "aliases", .value_json = "\"alpha\"" },
            },
        }},
    });

    const stored = (try db.get(alloc, "doc:a")) orelse return error.TestExpectedEqual;
    defer alloc.free(stored);
    try std.testing.expect(std.mem.indexOf(u8, stored, "\"title\":\"curated\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, stored, "would-be-invalid-on-insert") == null);
    try std.testing.expect(std.mem.indexOf(u8, stored, "\"aliases\":[\"alpha\"]") != null);
}

test "bound table write source validates non-upsert transforms against same-batch deletes" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-batch-transform-delete-no-upsert-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(u8, "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}"),
    };
    defer req.deinit(alloc);
    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });

    _ = try source.source().batch(alloc, "docs", .{
        .deletes = &.{"doc:a"},
        .transforms = &.{.{
            .key = "doc:a",
            .operations = &.{
                .{ .op = .set, .path = "body", .value_json = "\"would-be-invalid\"" },
            },
        }},
    });

    const stored = try db.get(alloc, "doc:a");
    defer if (stored) |body| alloc.free(body);
    try std.testing.expect(stored == null);
}

test "bound table write source rejects upsert transforms against same-batch deletes" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-batch-transform-delete-upsert-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(u8, "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}"),
    };
    defer req.deinit(alloc);
    _ = try source.source().createTable(alloc, "docs", req);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .deletes = &.{"doc:a"},
        .transforms = &.{.{
            .key = "doc:a",
            .upsert = true,
            .operations = &.{
                .{ .op = .set_on_insert, .path = "body", .value_json = "\"invalid-insert\"" },
            },
        }},
    }));
}

test "bound table write source derives ttl timestamps from ttl_field values" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-ttl-field-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(
            u8,
            "{\"default_type\":\"doc\",\"ttl_duration_ns\":1000000000,\"ttl_field\":\"expires_at\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"expires_at\":{\"type\":\"datetime\"},\"title\":{\"type\":\"text\"}}}}}}",
        ),
    };
    defer req.deinit(alloc);
    _ = try source.source().createTable(alloc, "docs", req);

    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"expires_at\":5}" }},
        .timestamp_ns = 999,
    });
    try std.testing.expectEqual(@as(u64, 5), try db.getTimestamp(alloc, "doc:a"));

    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:b", .value = "{\"title\":\"beta\",\"expires_at\":\"2024-01-02T03:04:05Z\"}" }},
        .timestamp_ns = 999,
    });
    try std.testing.expectEqual(@as(u64, 1_704_164_645_000_000_000), try db.getTimestamp(alloc, "doc:b"));

    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"expires_at\":\"not-a-time\"}" }},
    }));
}

test "bound table write source applies batch writes" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-table-batch";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var source = BoundTableWriteSource.init("docs", &db);
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });

    var result = (try db.lookup(alloc, "doc:a", .{})).?;
    defer result.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, result.json, "\"alpha\"") != null);
}

test "api.table_writes.docid provisioned secondary index rebuild worker pass repairs projected catalog range" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/provisioned-secondary-index-rebuild-root", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, 9001);
    defer alloc.free(db_path);

    const building_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric","x-antfly-index-lifecycle":"building","x-antfly-index-generation":9,"x-antfly-index-where":{"all":[{"field":"status","op":"eq","value":"active"}]}},"status":{"type":"keyword"}},"required":["id","amount","status"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;

    const range: metadata_table_manager.RangeRecord = .{
        .group_id = 9001,
        .range_id = 9101,
        .table_id = 77,
        .start_key = "",
        .end_key = null,
    };
    const namespace: doc_identity.Namespace = .{
        .table_id = 77,
        .shard_id = metadata_table_manager.rangeDocIdentityShardId(range),
        .range_id = metadata_table_manager.rangeDocIdentityRangeId(range),
    };
    {
        var db = try db_mod.DB.open(alloc, db_path, .{ .identity_namespace = namespace });
        defer db.close();
        try db.applyTableSchemaJson(alloc, building_schema_json, .{});
        try db.batch(.{ .writes = &.{
            .{ .key = "row:active", .value = "{\"id\":\"active\",\"amount\":1,\"status\":\"active\"}" },
            .{ .key = "row:inactive", .value = "{\"id\":\"inactive\",\"amount\":2,\"status\":\"inactive\"}" },
        } });
        const inactive_amount_key = try db_mod.internal_keys.relationalColumnIndexKeyAlloc(alloc, "amount", "row:inactive");
        defer alloc.free(inactive_amount_key);
        try db.core.store.put(inactive_amount_key, "");
    }

    const Catalog = struct {
        const ready_schema_json =
            \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric","x-antfly-index-lifecycle":"ready","x-antfly-index-generation":9,"x-antfly-index-where":{"all":[{"field":"status","op":"eq","value":"active"}]}},"status":{"type":"keyword"}},"required":["id","amount","status"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
        ;

        table: metadata_table_manager.TableRecord = .{
            .table_id = 77,
            .name = "orders",
            .placement_role = "data",
            .indexes_json = "{}",
            .schema_json = building_schema_json,
        },
        range: metadata_table_manager.RangeRecord = range,
        rebuild: metadata_table_manager.SecondaryIndexRebuildRangeRecord = .{
            .table_id = 77,
            .index_name = "amount",
            .index_generation = 9,
            .start_row_key = "",
            .end_row_key = null,
            .group_id = 9001,
        },

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .begin_secondary_index_rebuild_range = beginSecondaryIndexRebuildRange,
                    .finish_secondary_index_rebuild_range = finishSecondaryIndexRebuildRange,
                    .invalidate_secondary_index_rebuild_range = invalidateSecondaryIndexRebuildRange,
                    .promote_secondary_index_ready = promoteSecondaryIndexReady,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @as([*]metadata_table_manager.TableRecord, @ptrCast(&self.table))[0..1],
                .ranges = @as([*]metadata_table_manager.RangeRecord, @ptrCast(&self.range))[0..1],
                .secondary_index_rebuild_ranges = @as([*]metadata_table_manager.SecondaryIndexRebuildRangeRecord, @ptrCast(&self.rebuild))[0..1],
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn selectorMatches(self: *@This(), selector: metadata_table_manager.SecondaryIndexRebuildRangeSelector) bool {
            return selector.table_id == self.rebuild.table_id and
                selector.index_generation == self.rebuild.index_generation and
                std.mem.eql(u8, selector.index_name, self.rebuild.index_name) and
                std.mem.eql(u8, selector.start_row_key, self.rebuild.start_row_key);
        }

        fn beginSecondaryIndexRebuildRange(ptr: *anyopaque, request: metadata_table_manager.SecondaryIndexRebuildRangeBeginRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!self.selectorMatches(request.selector)) return error.SecondaryIndexRebuildRangeNotFound;
            if (!std.mem.eql(u8, self.rebuild.state, metadata_table_manager.secondary_index_rebuild_declared)) return error.SecondaryIndexRebuildRangeClaimBusy;
            self.rebuild.state = metadata_table_manager.secondary_index_rebuild_building;
            self.rebuild.lease_owner = request.lease_owner;
            self.rebuild.lease_expires_at_ms = request.lease_expires_at_ms;
        }

        fn finishSecondaryIndexRebuildRange(ptr: *anyopaque, request: metadata_table_manager.SecondaryIndexRebuildRangeFinishRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!self.selectorMatches(request.selector)) return error.SecondaryIndexRebuildRangeNotFound;
            self.rebuild.state = metadata_table_manager.secondary_index_rebuild_ready;
            self.rebuild.completed_row_count = request.completed_row_count;
            self.rebuild.progress_row_key = request.progress_row_key;
        }

        fn invalidateSecondaryIndexRebuildRange(ptr: *anyopaque, request: metadata_table_manager.SecondaryIndexRebuildRangeInvalidateRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!self.selectorMatches(request.selector)) return error.SecondaryIndexRebuildRangeNotFound;
            self.rebuild.state = metadata_table_manager.secondary_index_rebuild_invalid;
            self.rebuild.last_error = request.last_error;
        }

        fn promoteSecondaryIndexReady(
            ptr: *anyopaque,
            allocator: std.mem.Allocator,
            table_name: []const u8,
            index_name: []const u8,
            expected_generation: u64,
        ) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!std.mem.eql(u8, table_name, self.table.name)) return false;
            const updated = tables_api.schemaWithSecondaryIndexReadyAlloc(
                allocator,
                self.table.schema_json,
                index_name,
                expected_generation,
            ) catch |err| switch (err) {
                error.SecondaryIndexNotBuilding,
                error.SecondaryIndexGenerationMismatch,
                error.SecondaryIndexNotFound,
                => return false,
                else => return err,
            };
            defer allocator.free(updated);
            self.table.schema_json = ready_schema_json;
            return true;
        }
    };

    var catalog = Catalog{};
    var source = ProvisionedTableWriteSource.init(replica_root_dir, catalog.iface());
    defer source.deinit();

    var pass = (try source.source().secondaryIndexRebuildWorkerPass(alloc, "orders", "worker-a", 500, 1)).?;
    defer pass.deinit(alloc);
    try std.testing.expect(pass.complete);
    try std.testing.expectEqual(@as(u64, 1), pass.ranges_claimed);
    try std.testing.expectEqual(@as(u64, 1), pass.ranges_completed);
    try std.testing.expectEqual(@as(u64, 1), pass.indexes_promoted);
    try std.testing.expectEqual(@as(u64, 2), pass.report.scanned_rows);
    try std.testing.expectEqual(@as(u64, 1), pass.report.indexed_rows);
    try std.testing.expectEqualStrings(metadata_table_manager.secondary_index_rebuild_ready, catalog.rebuild.state);
    try std.testing.expectEqual(@as(u64, 2), catalog.rebuild.completed_row_count);
    try std.testing.expect(std.mem.indexOf(u8, catalog.table.schema_json, "\"x-antfly-index-lifecycle\":\"ready\"") != null);

    var reopened = try db_mod.DB.open(alloc, db_path, .{ .identity_namespace = namespace, .prefer_existing_identity_namespace = true });
    defer reopened.close();
    const inactive_amount_key = try db_mod.internal_keys.relationalColumnIndexKeyAlloc(alloc, "amount", "row:inactive");
    defer alloc.free(inactive_amount_key);
    try std.testing.expectError(error.NotFound, reopened.core.store.get(alloc, inactive_amount_key));
}

test "api.table_writes.docid provisioned schema rewrite worker pass drains projected catalog range job" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/provisioned-schema-rewrite-root", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, 9001);
    defer alloc.free(db_path);

    const schema_v1 =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"keyword"},"status":{"type":"keyword"}},"required":["title","status"],"additionalProperties":false}}}}
    ;
    const schema_v2 =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"keyword"},"status":{"type":"keyword"},"status_key":{"type":"keyword"}},"required":["title","status"],"additionalProperties":false}}}}
    ;

    const range: metadata_table_manager.RangeRecord = .{
        .group_id = 9001,
        .range_id = 9101,
        .table_id = 77,
        .start_key = "",
        .end_key = null,
    };
    const namespace: doc_identity.Namespace = .{
        .table_id = 77,
        .shard_id = metadata_table_manager.rangeDocIdentityShardId(range),
        .range_id = metadata_table_manager.rangeDocIdentityRangeId(range),
    };
    {
        var db = try db_mod.DB.open(alloc, db_path, .{ .identity_namespace = namespace });
        defer db.close();
        try db.applyTableSchemaJson(alloc, schema_v1, .{});
        try db.batch(.{ .writes = &.{.{ .key = "row:a", .value = "{\"title\":\"one\",\"status\":\"ACTIVE\"}" }} });
        try db.applyTableSchemaJson(alloc, schema_v2, .{});
    }

    const Catalog = struct {
        table: metadata_table_manager.TableRecord = .{
            .table_id = 77,
            .name = "events",
            .placement_role = "data",
            .indexes_json = "{}",
            .schema_json = schema_v2,
        },
        range: metadata_table_manager.RangeRecord = range,
        job: metadata_table_manager.SchemaRewriteJobRecord = .{
            .job_id = 8101,
            .table_id = 77,
            .group_id = 9001,
            .schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(schema_v2),
            .action = "rewrite",
            .reason = "row_images",
            .start_row_key = "",
            .end_row_key = null,
            .target_column = "status_key",
            .expression = .{
                .kind = .lower,
                .operands = &.{.{ .kind = .field, .field = "status" }},
            },
        },

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .begin_schema_rewrite_job = beginSchemaRewriteJob,
                    .finish_schema_rewrite_job = finishSchemaRewriteJob,
                    .invalidate_schema_rewrite_job = invalidateSchemaRewriteJob,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @as([*]metadata_table_manager.TableRecord, @ptrCast(&self.table))[0..1],
                .ranges = @as([*]metadata_table_manager.RangeRecord, @ptrCast(&self.range))[0..1],
                .schema_rewrite_jobs = @as([*]metadata_table_manager.SchemaRewriteJobRecord, @ptrCast(&self.job))[0..1],
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn beginSchemaRewriteJob(ptr: *anyopaque, request: metadata_table_manager.SchemaRewriteJobBeginRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (request.job_id != self.job.job_id) return error.UnknownSchemaRewriteJob;
            if (!std.mem.eql(u8, self.job.state, metadata_table_manager.schema_rewrite_declared)) return error.SchemaRewriteJobClaimBusy;
            self.job.state = metadata_table_manager.schema_rewrite_running;
            self.job.lease_owner = request.lease_owner;
            self.job.lease_expires_at_ms = request.lease_expires_at_ms;
            self.job.attempts += 1;
        }

        fn finishSchemaRewriteJob(ptr: *anyopaque, request: metadata_table_manager.SchemaRewriteJobFinishRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (request.job_id != self.job.job_id) return error.UnknownSchemaRewriteJob;
            if (!std.mem.eql(u8, self.job.lease_owner, request.lease_owner)) return error.SchemaRewriteJobLeaseMismatch;
            self.job.state = metadata_table_manager.schema_rewrite_ready;
            self.job.lease_owner = "";
            self.job.lease_expires_at_ms = 0;
            self.job.completed_row_count = request.completed_row_count;
            self.job.progress_row_key = "";
        }

        fn invalidateSchemaRewriteJob(ptr: *anyopaque, request: metadata_table_manager.SchemaRewriteJobInvalidateRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (request.job_id != self.job.job_id) return error.UnknownSchemaRewriteJob;
            self.job.state = metadata_table_manager.schema_rewrite_invalid;
            self.job.lease_owner = "";
            self.job.lease_expires_at_ms = 0;
            self.job.last_error = request.last_error;
        }
    };

    var catalog = Catalog{};
    var source = ProvisionedTableWriteSource.init(replica_root_dir, catalog.iface());
    defer source.deinit();

    var pass = (try source.source().schemaRewriteWorkerPass(alloc, "events", "worker-a", 500, 1)).?;
    defer pass.deinit(alloc);
    try std.testing.expect(pass.complete);
    try std.testing.expectEqual(@as(u64, 1), pass.jobs_claimed);
    try std.testing.expectEqual(@as(u64, 1), pass.jobs_completed);
    try std.testing.expectEqualStrings(metadata_table_manager.schema_rewrite_ready, catalog.job.state);
    try std.testing.expectEqual(@as(u64, 1), catalog.job.completed_row_count);

    var reopened = try db_mod.DB.open(alloc, db_path, .{ .identity_namespace = namespace, .prefer_existing_identity_namespace = true });
    defer reopened.close();
    const materialized = (try reopened.get(alloc, "row:a")) orelse return error.TestUnexpectedResult;
    defer alloc.free(materialized);
    try std.testing.expect(std.mem.indexOf(u8, materialized, "\"status_key\":\"active\"") != null);
}

test "provisioned table write source create table clears stale local group state" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/provisioned-create-table-clears-stale-root", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, 7001);
    defer alloc.free(db_path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .range_id = 7101,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    {
        var stale_db = try db_mod.DB.open(alloc, db_path, .{});
        defer stale_db.close();
        try stale_db.batch(.{
            .writes = &.{.{ .key = "doc:stale", .value = "{\"title\":\"stale\"}" }},
        });
    }

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const stale_change_journal_dir = try std.fmt.allocPrint(alloc, "{s}/change_journal", .{db_path});
    defer alloc.free(stale_change_journal_dir);
    try fs_paths.createDirPathPortable(io_impl.io(), stale_change_journal_dir);
    const stale_marker_path = try std.fmt.allocPrint(alloc, "{s}/stale.marker", .{stale_change_journal_dir});
    defer alloc.free(stale_marker_path);
    try std.Io.Dir.cwd().writeFile(io_impl.io(), .{ .sub_path = stale_marker_path, .data = "stale" });

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    _ = try source.source().createTable(alloc, "docs", .{});

    var recreated_db = try db_mod.DB.open(alloc, db_path, .{});
    defer recreated_db.close();
    try std.testing.expect((try recreated_db.lookup(alloc, "doc:stale", .{})) == null);
    try std.testing.expect(recreated_db.core.index_manager.textIndex("full_text_index_v0") != null);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(io_impl.io(), stale_marker_path, .{}));
}

test "provisioned table write source seeds doc identity namespace from table range" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/provisioned-identity-namespace-root", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, 7001);
    defer alloc.free(db_path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .range_id = 7101,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    {
        var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
        defer source.deinit();
        _ = try source.source().batch(alloc, "docs", .{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        });
    }

    var db = try db_mod.DB.open(alloc, db_path, .{ .start_index_workers = false });
    defer db.close();
    try std.testing.expect(db.core.identity_namespace.eql(.{
        .table_id = 7,
        .shard_id = 7001,
        .range_id = 7101,
    }));
}

test "api.table_writes.docid provisioned table write source rejects stale doc identity namespace before write" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .range_id = 7101,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const stale_namespace = doc_identity.Namespace{
        .table_id = 7,
        .shard_id = 7001,
        .range_id = 9999,
    };

    const setupStaleDb = struct {
        fn run(allocator: std.mem.Allocator, path: []const u8, namespace: doc_identity.Namespace) !void {
            var db = try db_mod.DB.open(allocator, path, .{ .identity_namespace = namespace });
            defer db.close();
            try db.batch(.{
                .writes = &.{.{ .key = "doc:stale", .value = "{\"title\":\"stale\"}" }},
            });
        }
    }.run;

    const uncached_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/provisioned-stale-identity-uncached-root", .{tmp.sub_path});
    defer alloc.free(uncached_root);
    const uncached_db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, uncached_root, 7001);
    defer alloc.free(uncached_db_path);
    try setupStaleDb(alloc, uncached_db_path, stale_namespace);

    {
        var source = ProvisionedTableWriteSource.init(uncached_root, Catalog.iface());
        defer source.deinit();
        try std.testing.expectError(error.DocIdentityNamespaceMismatch, source.source().batch(alloc, "docs", .{
            .writes = &.{.{ .key = "doc:b", .value = "{\"title\":\"beta\"}" }},
        }));
    }

    var uncached_db = try db_mod.DB.open(alloc, uncached_db_path, .{ .start_index_workers = false });
    defer uncached_db.close();
    try std.testing.expect((try uncached_db.lookup(alloc, "doc:b", .{})) == null);

    const cached_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/provisioned-stale-identity-cached-root", .{tmp.sub_path});
    defer alloc.free(cached_root);
    const cached_db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, cached_root, 7001);
    defer alloc.free(cached_db_path);
    try setupStaleDb(alloc, cached_db_path, stale_namespace);

    {
        var source = ProvisionedTableWriteSource.init(cached_root, Catalog.iface());
        defer source.deinit();
        var write_cache = ProvisionedTableWriteCache.init(alloc);
        defer write_cache.deinit();
        source.write_cache = &write_cache;
        try std.testing.expectError(error.DocIdentityNamespaceMismatch, source.source().batch(alloc, "docs", .{
            .writes = &.{.{ .key = "doc:b", .value = "{\"title\":\"beta\"}" }},
        }));
    }

    var cached_db = try db_mod.DB.open(alloc, cached_db_path, .{ .start_index_workers = false });
    defer cached_db.close();
    try std.testing.expect((try cached_db.lookup(alloc, "doc:b", .{})) == null);
}

test "provisioned table write source backs up and restores a local table" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-table-backup-restore");
    defer alloc.free(path);
    const backup_root = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-table-backup-restore-out");
    defer alloc.free(backup_root);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    defer {
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    }

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);
    var db = try db_mod.DB.open(alloc, db_path, .{});
    defer db.close();

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    defer source.deinit();
    _ = try source.source().createTable(alloc, "docs", .{});

    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .timestamp_ns = 1,
    });

    const shards = (try source.source().backupTable(alloc, "docs", .{
        .backup_root = backup_root,
        .backup_id = "snap1",
    })).?;
    defer freeBackupShards(alloc, shards);

    var manifest = try backups_api.createManifest(alloc, "snap1", &.{
        .table_id = 7,
        .name = "docs",
        .description = "docs table",
        .schema_json = "",
        .read_schema_json = "",
        .indexes_json = tables_api.default_indexes_json,
        .replication_sources_json = "[]",
    }, shards);
    defer manifest.deinit(alloc);

    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"beta\"}" }},
        .timestamp_ns = 2,
    });

    _ = try source.source().restoreTable(alloc, "docs", .{
        .backup_root = backup_root,
        .manifest = &manifest,
    });

    db.close();
    db = try db_mod.DB.open(alloc, db_path, .{});

    var restored = (try db.lookup(alloc, "doc:a", .{})).?;
    defer restored.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, restored.json, "\"alpha\"") != null);
}

test "api.table_writes.docid provisioned table write source backs up a portable local table" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-table-portable-backup");
    defer alloc.free(path);
    const backup_root = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-table-portable-backup-out");
    defer alloc.free(backup_root);
    const restore_path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-table-portable-backup-restore");
    defer alloc.free(restore_path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), restore_path) catch {};
    defer {
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), restore_path) catch {};
    }

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);
    var db = try db_mod.DB.open(alloc, db_path, .{});
    defer db.close();

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    defer source.deinit();
    _ = try source.source().createTable(alloc, "docs", .{});
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .timestamp_ns = 1,
    });

    const shards = (try source.source().backupTable(alloc, "docs", .{
        .backup_root = backup_root,
        .backup_id = "portable-snap",
        .format = .portable,
    })).?;
    defer freeBackupShards(alloc, shards);
    try std.testing.expectEqual(@as(usize, 1), shards.len);
    try std.testing.expectEqualStrings("portable-snap.afb", shards[0].snapshot_path);

    const afb_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ backup_root, shards[0].snapshot_path });
    defer alloc.free(afb_path);
    const afb = try readBackupFileAlloc(alloc, afb_path);
    defer alloc.free(afb);

    var restored_db = try db_mod.DB.open(alloc, restore_path, .{});
    defer restored_db.close();
    try portable_backup.importPortable(alloc, restored_db.core.store, afb);
    var restored = (try restored_db.lookup(alloc, "doc:a", .{})).?;
    defer restored.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, restored.json, "\"alpha\"") != null);
}

test "api.table_writes.docid provisioned table restore rejects mismatched doc identity namespace" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-table-backup-restore-docid-mismatch");
    defer alloc.free(path);
    const backup_root = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-table-backup-restore-docid-mismatch-out");
    defer alloc.free(backup_root);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    defer {
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    }

    const source_namespace = doc_identity.Namespace{ .table_id = 7, .shard_id = 7001, .range_id = 97001 };
    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);
    {
        var db = try db_mod.DB.open(alloc, db_path, .{
            .identity_namespace = source_namespace,
        });
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
            .timestamp_ns = 1,
        });
        _ = try db.snapshot("snap1-local");
    }

    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/snap1-local", .{db_path});
    defer alloc.free(snapshot_root);
    const dest_root = try backups_api.shardSnapshotPath(alloc, backup_root, "snap1", 7001);
    defer alloc.free(dest_root);
    try backups_api.copyDirectoryRecursive(alloc, snapshot_root, dest_root);

    const shards = try alloc.alloc(backups_api.ShardSnapshot, 1);
    shards[0] = .{
        .group_id = 7001,
        .start_key = try alloc.dupe(u8, ""),
        .end_key = null,
        .snapshot_path = try backups_api.shardSnapshotRelPath(alloc, "snap1", 7001),
    };
    defer freeBackupShards(alloc, shards);

    var manifest = try backups_api.createManifest(alloc, "snap1", &.{
        .table_id = 7,
        .name = "docs",
        .description = "docs table",
        .schema_json = "",
        .read_schema_json = "",
        .indexes_json = tables_api.default_indexes_json,
        .replication_sources_json = "[]",
    }, shards);
    defer manifest.deinit(alloc);

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                    .range_id = 7001,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    defer source.deinit();
    try std.testing.expectError(error.IdentityNamespaceMismatch, source.source().restoreTable(alloc, "docs", .{
        .backup_root = backup_root,
        .manifest = &manifest,
    }));
}

test "provisioned table write source backs up and restores full_text writes from the write cache" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-write-cache-backup-restore");
    defer alloc.free(path);
    const backup_root = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-write-cache-backup-restore-out");
    defer alloc.free(backup_root);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    defer {
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    }

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);
    var db = try db_mod.DB.open(alloc, db_path, .{});
    defer db.close();

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    defer source.deinit();
    source.write_cache = &write_cache;

    _ = try source.source().createTable(alloc, "docs", .{});
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"content\":\"distributed consensus\"}" }},
        .timestamp_ns = 1,
        .sync_level = .full_text,
    });

    const shards = (try source.source().backupTable(alloc, "docs", .{
        .backup_root = backup_root,
        .backup_id = "snap1",
    })).?;
    defer freeBackupShards(alloc, shards);

    var manifest = try backups_api.createManifest(alloc, "snap1", &.{
        .table_id = 7,
        .name = "docs",
        .description = "docs table",
        .schema_json = "",
        .read_schema_json = "",
        .indexes_json = tables_api.default_indexes_json,
        .replication_sources_json = "[]",
    }, shards);
    defer manifest.deinit(alloc);

    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"beta\",\"content\":\"vector search\"}" }},
        .timestamp_ns = 2,
        .sync_level = .full_text,
    });

    _ = try source.source().restoreTable(alloc, "docs", .{
        .backup_root = backup_root,
        .manifest = &manifest,
    });

    var read_source = table_read_sources.ProvisionedTableReadSource.init(
        path,
        FakeCatalog.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
    );

    var restored_lookup = (try read_source.source().lookup(alloc, "docs", "doc:a", .{}, .read_index)).?;
    defer restored_lookup.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, restored_lookup.json, "\"alpha\"") != null);

    var restored_scan = (try read_source.source().scan(alloc, "docs", "", "", .{
        .limit = 10,
        .include_documents = true,
    }, .read_index)).?;
    defer restored_scan.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, restored_scan.ndjson, "\"doc:a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, restored_scan.ndjson, "\"alpha\"") != null);

    db.close();
    db = try db_mod.DB.open(alloc, db_path, .{});

    var restored = (try db.lookup(alloc, "doc:a", .{})).?;
    defer restored.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, restored.json, "\"alpha\"") != null);
}

test "api.table_writes.query_visibility read preparation keeps write cache dirty while auto bulk ingest is active" {
    const alloc = std.testing.allocator;

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/read-prep-auto-bulk-dirty", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;

    var cached = try source.getOrOpenCachedDbMode(alloc, &write_cache, path, 7001, "docs", .default_async, null, null);
    cached.deinit(alloc);

    try write_cache.ensureAutoBulkIngestLocked(7001, "docs", platform_time.monotonicNs());
    try std.testing.expect(write_cache.entries.items[0].auto_bulk_ingest_session_open);
    source.markWriteCacheDirty("docs");

    source.readPreparation().prepareForRead("docs", .dense_query);
    try std.testing.expect(source.isWriteCacheDirtyForTable("docs"));

    try write_cache.finishAutoBulkIngestLocked(7001, "docs");
    source.readPreparation().prepareForRead("docs", .dense_query);
    try std.testing.expect(!source.isWriteCacheDirtyForTable("docs"));
}

test "api.table_writes.docid provisioned foreign key action job drains owner range page" {
    const alloc = std.testing.allocator;
    const replica_root_dir = "/tmp/antfly-api-fk-action-job-owner-range";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root_dir) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root_dir) catch {};

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"},"status":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"set_null"}]}
    ;

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, 7001);
    defer alloc.free(db_path);
    {
        var db = try db_mod.DB.open(alloc, db_path, .{
            .identity_namespace = .{
                .table_id = 42,
                .shard_id = 7001,
                .range_id = 7101,
            },
        });
        defer db.close();
        try db.applyTableSchemaJson(alloc, schema_json, .{});
        try db.batch(.{
            .writes = &.{
                .{ .key = "customer:owner", .value = "{\"_type\":\"customers\"}" },
                .{ .key = "order:owner", .value = "{\"customer_id\":\"customer:owner\",\"status\":\"open\"}" },
            },
            .sync_level = .write,
        });
        _ = try db.repairForeignKeyRefsInRange("", "");
    }

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{
                        .table_id = 42,
                        .name = "orders",
                        .placement_role = "data",
                        .schema_json = schema_json,
                    },
                    .{
                        .table_id = 43,
                        .name = "customers",
                        .placement_role = "data",
                    },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .range_id = 7101,
                    .table_id = 42,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .foreign_key_ref_ranges = @constCast((&[_]metadata_table_manager.ForeignKeyReferenceRangeRecord{.{
                    .child_table_id = 42,
                    .constraint_name = "orders_customer_id_fkey",
                    .parent_table_id = 43,
                    .start_parent_key = "",
                    .end_parent_key = null,
                    .group_id = 7001,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    defer source.deinit();
    var result = (try source.source().foreignKeyActionJobPage(
        alloc,
        "orders",
        "fk-action:set-null:owner-range",
        "set_null",
        "worker:fk-action",
        "orders_customer_id_fkey",
        "customers",
        "customer:owner",
        null,
        1,
        60_000,
    )) orelse return error.TestUnexpectedResult;
    defer result.deinit(alloc);
    try std.testing.expect(result.complete);
    try std.testing.expectEqual(@as(usize, 1), result.groups.len);
    try std.testing.expectEqual(@as(u64, 7001), result.groups[0].group_id);
    try std.testing.expectEqual(@as(u64, 1), result.groups[0].applied_children);
    try std.testing.expectEqualStrings("complete", result.groups[0].status);

    var reopened = try db_mod.DB.open(alloc, db_path, .{ .start_index_workers = false });
    defer reopened.close();
    const child = (try reopened.get(alloc, "order:owner")) orelse return error.TestUnexpectedResult;
    defer alloc.free(child);
    try std.testing.expectEqualStrings("{\"status\":\"open\"}", child);
}

test "api.table_writes.docid provisioned same-table foreign key action job routes runtime parent through catalog owner range" {
    const alloc = std.testing.allocator;
    const replica_root_dir = "/tmp/antfly-api-fk-action-job-same-table-owner-range";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root_dir) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root_dir) catch {};

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"manager_id":{"type":"keyword"},"status":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"row_manager_id_fkey","columns":["manager_id"],"references":{"table":"row","columns":["_id"]},"on_delete":"set_null","validation_state":"enforced"}]}
    ;

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, 7001);
    defer alloc.free(db_path);
    {
        var db = try db_mod.DB.open(alloc, db_path, .{
            .identity_namespace = .{
                .table_id = 42,
                .shard_id = 7001,
                .range_id = 7101,
            },
        });
        defer db.close();
        try db.applyTableSchemaJson(alloc, schema_json, .{});
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:manager", .value = "{\"status\":\"lead\"}" },
                .{ .key = "doc:report", .value = "{\"manager_id\":\"doc:manager\",\"status\":\"open\"}" },
            },
            .sync_level = .write,
        });
        _ = try db.repairForeignKeyRefsInRange("", "");
    }

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 42,
                    .name = "docs",
                    .placement_role = "data",
                    .schema_json = schema_json,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .range_id = 7101,
                    .table_id = 42,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .foreign_key_ref_ranges = @constCast((&[_]metadata_table_manager.ForeignKeyReferenceRangeRecord{.{
                    .child_table_id = 42,
                    .constraint_name = "row_manager_id_fkey",
                    .parent_table_id = 42,
                    .start_parent_key = "",
                    .end_parent_key = null,
                    .group_id = 7001,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    defer source.deinit();
    var result = (try source.source().foreignKeyActionJobPage(
        alloc,
        "docs",
        "fk-action:set-null:same-table-owner-range",
        "set_null",
        "worker:fk-action",
        "row_manager_id_fkey",
        "row",
        "doc:manager",
        null,
        1,
        60_000,
    )) orelse return error.TestUnexpectedResult;
    defer result.deinit(alloc);
    try std.testing.expect(result.complete);
    try std.testing.expectEqual(@as(usize, 1), result.groups.len);
    try std.testing.expectEqual(@as(u64, 7001), result.groups[0].group_id);
    try std.testing.expectEqual(@as(u64, 1), result.groups[0].applied_children);
    try std.testing.expectEqualStrings("complete", result.groups[0].status);

    var reopened = try db_mod.DB.open(alloc, db_path, .{ .start_index_workers = false });
    defer reopened.close();
    const child = (try reopened.get(alloc, "doc:report")) orelse return error.TestUnexpectedResult;
    defer alloc.free(child);
    try std.testing.expectEqualStrings("{\"status\":\"open\"}", child);
}

test "managed startup catch-up uses provided indexes json without catalog fetch" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-startup-catch-up-provided-indexes", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);
    const indexes_json = "{\"indexes\":[{\"name\":\"dv_v1\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}";

    {
        var db = try openManagedDbWithIndexesJsonAndCacheMode(alloc, path, indexes_json, null, null, backend_current_root_generation, null, .default);
        defer db.close();
    }

    const CountingCatalog = struct {
        calls: usize = 0,

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var catalog = CountingCatalog{};
    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, catalog.iface());
    source.runtime_status_cache = &snapshot_cache;
    const result = try source.catchUpTableGroupBestEffortWithIndexesJson(alloc, 7001, "docs", indexes_json);

    try std.testing.expectEqual(@as(usize, 0), catalog.calls);
    try std.testing.expect(!result.busy);
    try std.testing.expect(!result.had_debt);
    try std.testing.expect(!result.cleared_debt);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expect(!statuses.items[0].stats.async_indexing.startup.active);
}

test "managed startup catch-up bypasses shared write cache" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-startup-catch-up-cache", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[{\"name\":\"dv_v1\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;

    var cached = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, 0, "docs");
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(write_cache.entries.items[0].db.start_index_workers);
    cached.deinit(alloc);

    const result = try source.catchUpTableGroupBestEffort(alloc, 7001, "docs");
    try std.testing.expect(result.busy);
    try std.testing.expect(!result.had_debt);
    try std.testing.expect(!result.cleared_debt);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(write_cache.entries.items[0].db.start_index_workers);
}

test "managed startup catch-up invalidates stale cached writer status after replay clears debt" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-startup-catch-up-invalidates-stale-cache", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[{\"name\":\"dv_v1\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    {
        var seeded = try openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentity(
            alloc,
            path,
            "{\"indexes\":[{\"name\":\"dv_v1\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
            null,
            null,
            0,
            null,
            .writer_no_replay,
            null,
            null,
            null,
            null,
            .{ .table_id = 7, .shard_id = 7001, .range_id = 7001 },
        );
        defer seeded.close();
        _ = try seeded.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"embedding\":[1,2]}" }},
            .sync_level = .write,
        });
        const before = try seeded.stats(alloc);
        defer db_mod.types.freeDBStats(alloc, before);
        try std.testing.expectEqual(@as(usize, 1), before.indexes.len);
        try std.testing.expect(before.indexes[0].replay_catch_up_required);
    }
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items.len);

    const result = try source.catchUpTableGroupBestEffort(alloc, 7001, "docs");
    try std.testing.expect(!result.busy);
    try std.testing.expect(!result.had_debt);
    try std.testing.expect(!result.cleared_debt);
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items.len);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(usize, 1), statuses.items[0].stats.indexes.len);
    try std.testing.expectEqual(@as(u64, statuses.items[0].stats.indexes[0].replay_target_sequence), statuses.items[0].stats.indexes[0].replay_applied_sequence);
    try std.testing.expect(!statuses.items[0].stats.indexes[0].replay_catch_up_required);
}

test "managed startup catch-up defers while shared writer cache owns the table" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-startup-catch-up-defers-shared-writer", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[{\"name\":\"dv_v1\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var startup_write_cache = ProvisionedTableWriteCache.init(alloc);
    defer startup_write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.startup_write_cache = &startup_write_cache;

    var cached = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, 0, "docs");
    defer cached.deinit(alloc);

    const result = try source.catchUpTableGroupBestEffort(alloc, 7001, "docs");
    try std.testing.expect(result.busy);
    try std.testing.expect(!result.had_debt);
    try std.testing.expect(!result.cleared_debt);
    try std.testing.expectEqual(@as(usize, 0), startup_write_cache.entries.items.len);
}

test "managed startup catch-up defers while foreground writer state is dirty" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-startup-catch-up-defers-dirty-writer", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[{\"name\":\"dv_v1\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var startup_write_cache = ProvisionedTableWriteCache.init(alloc);
    defer startup_write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.startup_write_cache = &startup_write_cache;

    var cached = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, 0, "docs");
    cached.deinit(alloc);
    source.markWriteCacheDirty("docs");

    const result = try source.catchUpTableGroupBestEffort(alloc, 7001, "docs");
    try std.testing.expect(result.busy);
    try std.testing.expect(!result.had_debt);
    try std.testing.expect(!result.cleared_debt);
    try std.testing.expectEqual(@as(usize, 0), startup_write_cache.entries.items.len);
}

test "provisioned table write source group batch does not hold local db mutex during db batch" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/provisioned-group-batch-mutex", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const BatchProbe = struct {
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),

        fn beforeBatch(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) std.atomic.spinLoopHint();
        }
    };

    const BatchWorker = struct {
        source: *ProvisionedTableWriteSource,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            _ = self.source.source().batchGroupLocal(std.heap.page_allocator, 7001, "docs", .{
                .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
            }) catch |err| {
                self.err = err;
            };
        }
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;

    var probe = BatchProbe{};
    test_before_batch_execution_hook = .{
        .ptr = &probe,
        .run = BatchProbe.beforeBatch,
    };
    defer test_before_batch_execution_hook = null;

    var worker = BatchWorker{ .source = &source };
    const thread = try std.Thread.spawn(.{}, BatchWorker.run, .{&worker});

    while (!probe.entered.load(.acquire)) std.atomic.spinLoopHint();
    try std.testing.expect(source.local_db_mutex.tryLock());
    source.local_db_mutex.unlock();

    probe.release.store(true, .release);
    thread.join();

    if (worker.err) |err| return err;

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    var result = (try db.lookup(alloc, "doc:a", .{})).?;
    defer result.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, result.json, "\"alpha\"") != null);
}

test "api.table_writes.docid provisioned table write source drop table does not hold local db mutex during background delete" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/provisioned-drop-table-mutex", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const DropProbe = struct {
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),

        fn beforeDeleteTree(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) std.atomic.spinLoopHint();
        }
    };

    const DropWorker = struct {
        source: *ProvisionedTableWriteSource,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            _ = self.source.source().dropTable(std.heap.page_allocator, "docs", &.{7001}) catch |err| {
                self.err = err;
            };
        }
    };

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    try std.Io.Dir.cwd().createDirPath(io_impl.io(), path);
    const marker_path = try std.fmt.allocPrint(alloc, "{s}/marker.txt", .{path});
    defer alloc.free(marker_path);
    var marker = try std.Io.Dir.cwd().createFile(io_impl.io(), marker_path, .{});
    marker.close(io_impl.io());

    var runtime = try db_mod.background_runtime.BackendRuntimeHandle.init(alloc, .{ .backend = .io_threaded });
    defer runtime.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    source.backend_runtime = runtime.ptr();
    var probe = DropProbe{};
    test_before_drop_table_delete_hook = .{
        .ptr = &probe,
        .run = DropProbe.beforeDeleteTree,
    };
    defer test_before_drop_table_delete_hook = null;

    var worker = DropWorker{ .source = &source };
    const thread = try std.Thread.spawn(.{}, DropWorker.run, .{&worker});
    thread.join();

    if (worker.err) |err| return err;

    while (!probe.entered.load(.acquire)) std.Thread.yield() catch {};
    try std.testing.expect(source.local_db_mutex.tryLock());
    source.local_db_mutex.unlock();

    probe.release.store(true, .release);
    source.drainDroppedTableDeletes();

    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(io_impl.io(), path, .{}));
}

test "api.table_writes.docid provisioned table write source drop table waits for in-flight group batch on same table" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/provisioned-drop-table-waits-for-batch", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const Probe = struct {
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),

        fn run(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) std.atomic.spinLoopHint();
        }
    };

    const BatchWorker = struct {
        source: *ProvisionedTableWriteSource,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            _ = self.source.source().batchGroupLocal(std.heap.page_allocator, 7001, "docs", .{
                .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
            }) catch |err| {
                self.err = err;
            };
        }
    };

    const DropWorker = struct {
        source: *ProvisionedTableWriteSource,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            _ = self.source.source().dropTable(std.heap.page_allocator, "docs", &.{7001}) catch |err| {
                self.err = err;
            };
        }
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;

    var batch_probe = Probe{};
    var drop_probe = Probe{};
    test_before_batch_execution_hook = .{ .ptr = &batch_probe, .run = Probe.run };
    defer test_before_batch_execution_hook = null;
    test_before_drop_table_delete_hook = .{ .ptr = &drop_probe, .run = Probe.run };
    defer test_before_drop_table_delete_hook = null;

    var batch_worker = BatchWorker{ .source = &source };
    const batch_thread = try std.Thread.spawn(.{}, BatchWorker.run, .{&batch_worker});
    while (!batch_probe.entered.load(.acquire)) std.atomic.spinLoopHint();

    var drop_worker = DropWorker{ .source = &source };
    const drop_thread = try std.Thread.spawn(.{}, DropWorker.run, .{&drop_worker});

    sleepNs(10 * std.time.ns_per_ms);
    try std.testing.expect(!drop_probe.entered.load(.acquire));

    batch_probe.release.store(true, .release);
    batch_thread.join();
    if (batch_worker.err) |err| return err;

    while (!drop_probe.entered.load(.acquire)) std.atomic.spinLoopHint();
    drop_probe.release.store(true, .release);
    drop_thread.join();
    if (drop_worker.err) |err| return err;
}

test "provisioned table write source drop index does not hold local db mutex during index deletion work" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/provisioned-drop-index-mutex", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = tables_api.default_indexes_json,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });

    const Probe = struct {
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),

        fn run(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) std.atomic.spinLoopHint();
        }
    };

    const Worker = struct {
        source: *ProvisionedTableWriteSource,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            _ = self.source.source().dropIndex(std.heap.page_allocator, "docs", "full_text_index_v0") catch |err| {
                self.err = err;
            };
        }
    };

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    var probe = Probe{};
    test_before_drop_index_work_hook = .{
        .ptr = &probe,
        .run = Probe.run,
    };
    defer test_before_drop_index_work_hook = null;

    var worker = Worker{ .source = &source };
    const thread = try std.Thread.spawn(.{}, Worker.run, .{&worker});

    while (!probe.entered.load(.acquire)) std.atomic.spinLoopHint();
    try std.testing.expect(source.local_db_mutex.tryLock());
    source.local_db_mutex.unlock();

    probe.release.store(true, .release);
    thread.join();

    if (worker.err) |err| return err;

    var reopened = try db_mod.DB.open(alloc, path, .{});
    defer reopened.close();
    try std.testing.expect(reopened.core.index_manager.textIndex("full_text_index_v0") == null);
}

test "provisioned table write source create table provisions local indexes and schema" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-create-schema";
    const schema_json =
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = schema_json,
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(path, Catalog.iface());
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(u8, schema_json),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);
    var db = try db_mod.DB.open(alloc, db_path, .{});
    defer db.close();

    try std.testing.expect(db.core.index_manager.textIndex("full_text_index_v0") != null);
    try std.testing.expect(db.core.schema != null);
}

test "provisioned table write source restore table does not hold local db mutex during restore work" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-table-restore-mutex";
    const backup_root = "/tmp/antfly-api-provisioned-table-restore-mutex-out";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    defer {
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    }

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);
    var db = try db_mod.DB.open(alloc, db_path, .{});
    defer db.close();

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const Probe = struct {
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),

        fn run(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) std.atomic.spinLoopHint();
        }
    };

    const Worker = struct {
        source: *ProvisionedTableWriteSource,
        manifest: *const backups_api.TableBackupManifest,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            _ = self.source.source().restoreTable(std.heap.page_allocator, "docs", .{
                .backup_root = backup_root,
                .manifest = self.manifest,
            }) catch |err| {
                self.err = err;
            };
        }
    };

    var source = ProvisionedTableWriteSource.init(path, Catalog.iface());
    _ = try source.source().createTable(alloc, "docs", .{});
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .timestamp_ns = 1,
    });

    const shards = (try source.source().backupTable(alloc, "docs", .{
        .backup_root = backup_root,
        .backup_id = "snap1",
    })).?;
    defer freeBackupShards(alloc, shards);

    var manifest = try backups_api.createManifest(alloc, "snap1", &.{
        .table_id = 7,
        .name = "docs",
        .description = "docs table",
        .schema_json = "",
        .read_schema_json = "",
        .indexes_json = tables_api.default_indexes_json,
        .replication_sources_json = "[]",
    }, shards);
    defer manifest.deinit(alloc);

    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"beta\"}" }},
        .timestamp_ns = 2,
    });

    var probe = Probe{};
    test_before_restore_work_hook = .{
        .ptr = &probe,
        .run = Probe.run,
    };
    defer test_before_restore_work_hook = null;

    var worker = Worker{
        .source = &source,
        .manifest = &manifest,
    };
    const thread = try std.Thread.spawn(.{}, Worker.run, .{&worker});
    var thread_joined = false;
    defer if (!thread_joined) {
        probe.release.store(true, .release);
        thread.join();
    };

    while (!probe.entered.load(.acquire)) std.atomic.spinLoopHint();
    try std.testing.expect(source.local_db_mutex.tryLock());
    source.local_db_mutex.unlock();

    probe.release.store(true, .release);
    thread.join();
    thread_joined = true;

    if (worker.err) |err| return err;

    db.close();
    db = try db_mod.DB.open(alloc, db_path, .{});
    var restored = (try db.lookup(alloc, "doc:a", .{})).?;
    defer restored.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, restored.json, "\"alpha\"") != null);
}

test "api.table_writes.query_visibility table write source deinit drains restore repair work group" {
    if (builtin.os.tag == .freestanding) return;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogFetch;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const DrainCtx = struct {
        started: std.atomic.Value(u32) = .init(0),
        finished: std.atomic.Value(u32) = .init(0),

        fn run(self: *@This()) !void {
            _ = self.started.fetchAdd(1, .release);
            sleepNs(20 * std.time.ns_per_ms);
            _ = self.finished.fetchAdd(1, .release);
        }
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-restore-repair-drain", NoCatalog.iface());

    var ctx = DrainCtx{};
    try source.restore_repair_work_group.concurrent(source.table_activity_threaded.io(), DrainCtx.run, .{&ctx});

    while (ctx.started.load(.acquire) == 0) std.Thread.yield() catch {};
    source.deinit();

    try std.testing.expectEqual(@as(u32, 1), ctx.started.load(.acquire));
    try std.testing.expectEqual(@as(u32, 1), ctx.finished.load(.acquire));
}

test "managed startup catch-up repairs external dense doc gaps from stored artifacts without replay debt" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-startup-catch-up-dense-artifacts", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"dense_idx\":{\"type\":\"embeddings\",\"dimension\":3,\"metric\":\"l2_squared\",\"external\":true},\"ft_v1\":{\"type\":\"full_text\"}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    {
        var db = try db_mod.DB.open(alloc, path, .{
            .identity_namespace = .{ .table_id = 7, .shard_id = 7001, .range_id = 7001 },
        });
        defer db.close();
        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
        });
        try db.addIndex(.{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{}",
        });
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}" },
                .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_embeddings\":{\"dense_idx\":[0.9,0.1,0]}}" },
            },
            .sync_level = .full_index,
        });
    }

    const dense_index_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_idx", .{path});
    defer alloc.free(dense_index_path);
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    try std.Io.Dir.cwd().deleteTree(io_impl.io(), dense_index_path);

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    const result = try source.catchUpTableGroupBestEffort(alloc, 7001, "docs");
    try std.testing.expect(!result.busy);
    try std.testing.expect(!result.had_debt);
    try std.testing.expect(!result.cleared_debt);

    var repaired = try db_mod.DB.open(alloc, path, .{
        .identity_namespace = .{ .table_id = 7, .shard_id = 7001, .range_id = 7001 },
        .prefer_existing_identity_namespace = true,
        .start_index_workers = false,
    });
    defer repaired.close();
    const repaired_stats = try repaired.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, repaired_stats);
    var dense_doc_count: ?u64 = null;
    for (repaired_stats.indexes) |index| {
        if (!std.mem.eql(u8, index.name, "dense_idx")) continue;
        dense_doc_count = index.doc_count;
    }
    try std.testing.expectEqual(@as(?u64, 3), dense_doc_count);
}

test "managed startup catch-up defers while shared bulk ingest state is active" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-startup-catch-up-bulk", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var startup_write_cache = ProvisionedTableWriteCache.init(alloc);
    defer startup_write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.startup_write_cache = &startup_write_cache;

    try write_cache.beginBulkIngestLocked("docs");
    try std.testing.expectEqual(@as(usize, 1), write_cache.active_bulk_ingest_sessions.items.len);
    var cached_seed = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, 0, "docs");
    defer cached_seed.deinit(alloc);
    try std.testing.expect(write_cache.entries.items[0].bulk_ingest_session_open);
    const hits_before = write_cache.hit_count.load(.monotonic);

    const first = try source.catchUpTableGroupBestEffort(alloc, 7001, "docs");
    try std.testing.expect(first.busy);
    try std.testing.expect(!first.had_debt);
    try std.testing.expectEqual(@as(usize, 0), startup_write_cache.entries.items.len);
    const startup_hits_before = startup_write_cache.hit_count.load(.monotonic);

    const second = try source.catchUpTableGroupBestEffort(alloc, 7001, "docs");
    try std.testing.expect(second.busy);
    try std.testing.expect(!second.had_debt);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(write_cache.entries.items[0].bulk_ingest_session_open);
    try std.testing.expectEqual(@as(usize, 1), write_cache.active_bulk_ingest_sessions.items.len);
    try std.testing.expectEqual(hits_before, write_cache.hit_count.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 0), startup_write_cache.entries.items.len);
    try std.testing.expectEqual(startup_hits_before, startup_write_cache.hit_count.load(.monotonic));
}

test "api.table_writes.query_visibility managed startup catch-up ignores stale dirty bit after writer cache entry is gone" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-startup-catch-up-stale-dirty", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    {
        var db = try db_mod.DB.open(alloc, path, .{});
        defer db.close();
    }

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var startup_write_cache = ProvisionedTableWriteCache.init(alloc);
    defer startup_write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.startup_write_cache = &startup_write_cache;
    source.markWriteCacheDirty("docs");
    try std.testing.expect(source.isWriteCacheDirtyForTable("docs"));

    const result = try source.catchUpTableGroupBestEffort(alloc, 7001, "docs");
    try std.testing.expect(!result.busy);
    try std.testing.expect(!source.isWriteCacheDirtyForTable("docs"));
}

test "managed source status-only open bypasses shared writer cache entry" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-source-status-only", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;

    var seeded = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, 0, "docs");
    defer seeded.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(write_cache.entries.items[0].db.start_index_workers);

    var status_only = try source.getOrOpenCachedDbMode(alloc, &write_cache, path, 7001, "docs", .status_only, null, null);
    defer status_only.deinit(alloc);

    try std.testing.expect(status_only.owned_db != null);
    try std.testing.expect(status_only.entry == null);
    try std.testing.expect(!status_only.db.start_index_workers);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(write_cache.entries.items[0].db.start_index_workers);
}

test "api.table_writes.docid primary lookup adopts seeded write cache across visible generation bump" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/primary-lookup-write-cache-generation", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var generation: u64 = 1;
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    _ = source.withGroupVisibleRootGeneration(testingVisibleRootGenerationSource(&generation));

    var seeded = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, generation, "docs");
    try seeded.db.batch(.{
        .writes = &.{.{ .key = "doc:gold", .value = "{\"title\":\"gold doc\"}" }},
        .sync_level = .write,
    });
    seeded.deinit(alloc);

    const primary_lookup = source.primaryLookupDbSource();
    generation = 2;
    try std.testing.expect((try primary_lookup.leaseGroup(alloc, "docs", 7001, generation)) == null);

    write_cache.entries.items[0].allow_generation_adoption = true;
    const misses_before = write_cache.miss_count.load(.monotonic);

    var lease = (try primary_lookup.leaseGroup(alloc, "docs", 7001, generation)).?;
    defer lease.release(alloc);

    try std.testing.expectEqual(@as(u64, 2), write_cache.entries.items[0].lsm_root_generation);
    try std.testing.expect(!write_cache.entries.items[0].allow_generation_adoption);
    try std.testing.expectEqual(misses_before, write_cache.miss_count.load(.monotonic));

    var result = (try lease.db.lookup(alloc, "doc:gold", .{})).?;
    defer result.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, result.json, "\"gold doc\"") != null);
}

test "replica root reconcile seeds write cache across generation bump" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/write-cache-reconcile-generation", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const tables = [_]metadata_table_manager.TableRecord{.{
        .table_id = 7,
        .name = "docs",
        .placement_role = "data",
        .indexes_json = "{\"indexes\":[]}",
    }};
    const ranges = [_]metadata_table_manager.RangeRecord{.{
        .group_id = 7001,
        .table_id = 7,
        .start_key = "",
        .end_key = null,
    }};
    const hosted_groups = [_]u64{7001};

    var generation: u64 = 1;
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    _ = source.withGroupVisibleRootGeneration(testingVisibleRootGenerationSource(&generation));

    const summary = try source.reconcileReplicaRootTablesWithWriteCacheLocked(
        alloc,
        1,
        &hosted_groups,
        &tables,
        &ranges,
        null,
    );
    try std.testing.expectEqual(@as(usize, 1), summary.groups_considered);
    try std.testing.expectEqual(@as(usize, 1), summary.dbs_opened);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(write_cache.entries.items[0].allow_generation_adoption);
    const misses_before = write_cache.miss_count.load(.monotonic);

    generation = 2;
    source.pruneStaleWriteCacheLocked();
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expectEqual(@as(u64, 2), write_cache.entries.items[0].lsm_root_generation);

    var cached_after_bump = try source.getOrOpenCachedDbMode(alloc, &write_cache, path, 7001, "docs", .default_async, null, null);
    cached_after_bump.deinit(alloc);
    try std.testing.expectEqual(misses_before, write_cache.miss_count.load(.monotonic));
}

test "provisioned table read source serves profiled dense query without runtime status warmup" {
    const alloc = std.testing.allocator;

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-warmed-read-cache-profiled-query", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    {
        var db = try openManagedDbWithIndexesJson(
            alloc,
            path,
            "{\"indexes\":[{\"name\":\"semantic_idx\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
        );
        defer db.close();
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"_embeddings\":{\"semantic_idx\":[1,2]}}" },
                .{ .key = "doc:b", .value = "{\"_embeddings\":{\"semantic_idx\":[2,1]}}" },
            },
            .sync_level = .full_index,
        });
    }

    var read_cache = table_read_cache.ProvisionedTableReadCache.init(alloc);
    defer read_cache.deinit();

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    _ = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, 0, "docs");

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var write_source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    write_source.read_cache = &read_cache;
    write_source.write_cache = &write_cache;
    write_source.runtime_status_cache = &snapshot_cache;
    write_source.markWriteCacheDirty("docs");

    try std.testing.expect((try write_source.source().localRuntimeStatuses(alloc, "docs")) == null);

    var read_source = table_read_sources.ProvisionedTableReadSource.init(replica_root_dir, Catalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());
    read_source.cache = &read_cache;

    var owned = try query_api.parseQueryRequest(alloc, null, "docs",
        \\{"embeddings":{"semantic_idx":[1.0,2.0]},"indexes":["semantic_idx"],"limit":2,"profile":true}
    );
    defer owned.deinit(alloc);

    var response = (try read_source.source().query(alloc, "docs", owned.req, .read_index)).?;
    defer response.deinit(alloc);
    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryResponses, alloc, response.json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expect(parsed.value.responses.?[0].hits != null);
    try std.testing.expect(parsed.value.responses.?[0].profile != null);
}

test "hosted provisioned table read source serves profiled dense query after external write-sync batch without index-not-found" {
    const alloc = std.testing.allocator;

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeRouter = struct {
        fn iface() table_router.HostedGroupRouter {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .local_node_id = localNodeId,
                    .local_status = localStatus,
                    .group_leader_node_id = groupLeaderNodeId,
                    .node_status = nodeStatus,
                    .node_base_uri = nodeBaseUri,
                },
            };
        }

        fn localNodeId(_: *anyopaque) u64 {
            return 1;
        }

        fn localStatus(_: *anyopaque, group_id: u64) raft_mod.HostedReplicaStatus {
            return if (group_id == 7001) .active else .absent;
        }

        fn groupLeaderNodeId(_: *anyopaque, group_id: u64) ?u64 {
            return if (group_id == 7001) 1 else null;
        }

        fn nodeStatus(_: *anyopaque, node_id: u64, group_id: u64) raft_mod.HostedReplicaStatus {
            _ = node_id;
            return if (group_id == 7001) .active else .absent;
        }

        fn nodeBaseUri(_: *anyopaque, _: std.mem.Allocator, _: u64) !?[]u8 {
            return null;
        }
    };

    const ExecutorState = struct {
        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(_: *anyopaque, _: std.mem.Allocator, _: http_common.HttpRequest) !http_common.HttpResponse {
            return error.UnexpectedHttpRequest;
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/hosted-profiled-external-write-sync", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);

    var write_source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    _ = try write_source.source().batch(alloc, "docs", .{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"_embeddings\":{\"semantic_idx\":[1,2]}}" },
            .{ .key = "doc:b", .value = "{\"_embeddings\":{\"semantic_idx\":[2,1]}}" },
        },
        .sync_level = .write,
    });

    var executor_state = ExecutorState{};
    var hosted = table_read_sources.HostedProvisionedTableReadSource.init(
        replica_root_dir,
        Catalog.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
        FakeRouter.iface(),
        executor_state.iface(),
    );

    var owned = try query_api.parseQueryRequest(alloc, null, "docs",
        \\{"embeddings":{"semantic_idx":[1.0,2.0]},"indexes":["semantic_idx"],"limit":2,"profile":true}
    );
    defer owned.deinit(alloc);

    var response = (try hosted.source().query(alloc, "docs", owned.req, .read_index)).?;
    defer response.deinit(alloc);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, response.json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expect(parsed.value.object.get("responses") != null);
}

test "provisioned table read source survives many external write-sync batches before first profiled dense query" {
    const alloc = std.testing.allocator;
    const total_docs: usize = 1_000;
    const batch_size: usize = 50;
    const dims: usize = 16;

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":16}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeStatusSource = struct {
        fn iface() http_server.StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return try Catalog.adminSnapshot(undefined);
        }

        fn freeAdminSnapshot(_: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
            Catalog.freeAdminSnapshot(undefined, snapshot);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/hosted-profiled-external-write-sync-many", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);

    var read_cache = table_read_cache.ProvisionedTableReadCache.init(alloc);
    defer read_cache.deinit();

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var write_source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    write_source.read_cache = &read_cache;
    write_source.write_cache = &write_cache;
    write_source.runtime_status_cache = &snapshot_cache;

    const dense_doc_json = blk: {
        var out: std.Io.Writer.Allocating = .init(alloc);
        defer out.deinit();
        try out.writer.writeAll("{\"_embeddings\":{\"semantic_idx\":[");
        for (0..dims) |i| {
            if (i != 0) try out.writer.writeByte(',');
            try out.writer.writeAll("1");
        }
        try out.writer.writeAll("]}}");
        break :blk try out.toOwnedSlice();
    };
    defer alloc.free(dense_doc_json);

    const query_json = blk: {
        var out: std.Io.Writer.Allocating = .init(alloc);
        defer out.deinit();
        try out.writer.writeAll("{\"embeddings\":{\"semantic_idx\":[");
        for (0..dims) |i| {
            if (i != 0) try out.writer.writeByte(',');
            try out.writer.writeAll("1.0");
        }
        try out.writer.writeAll("]},\"indexes\":[\"semantic_idx\"],\"limit\":10,\"profile\":true}");
        break :blk try out.toOwnedSlice();
    };
    defer alloc.free(query_json);

    {
        var cold_read_source = table_read_sources.ProvisionedTableReadSource.init(
            replica_root_dir,
            Catalog.iface(),
            raft_mod.read_gate.noopReadableLeaseRequester(),
        );
        cold_read_source.cache = &read_cache;

        var cold_owned = try query_api.parseQueryRequest(alloc, null, "docs", query_json);
        defer cold_owned.deinit(alloc);

        var cold_response = (try cold_read_source.source().query(alloc, "docs", cold_owned.req, .read_index)).?;
        defer cold_response.deinit(alloc);
    }

    for (0..(total_docs / batch_size)) |batch_idx| {
        const writes = try alloc.alloc(db_mod.types.BatchWrite, batch_size);
        defer {
            for (writes) |write| {
                alloc.free(@constCast(write.key));
                alloc.free(@constCast(write.value));
            }
            alloc.free(writes);
        }
        for (writes, 0..) |*write, i| {
            const doc_idx = batch_idx * batch_size + i;
            write.key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{doc_idx});
            write.value = try alloc.dupe(u8, dense_doc_json);
        }
        _ = try write_source.source().batch(alloc, "docs", .{
            .writes = writes,
            .sync_level = .write,
        });
    }

    var read_source = table_read_sources.ProvisionedTableReadSource.init(
        replica_root_dir,
        Catalog.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
    );
    read_source.cache = &read_cache;

    var server = http_server.ApiHttpServer.init(
        alloc,
        .{},
        FakeStatusSource.iface(),
        read_source.source(),
        write_source.source(),
    );

    const IndexDetail = struct {
        status: ?struct {
            doc_count: ?u64 = null,
            total_indexed: ?u64 = null,
            replay_target_sequence: ?u64 = null,
            replay_applied_sequence: ?u64 = null,
            replay_catch_up_required: ?bool = null,
            backfill_active: ?bool = null,
            rebuilding: ?bool = null,
        } = null,
    };

    var ready = false;
    for (0..1000) |_| {
        var detail = try server.handlePublicTableGetIndex("docs", "semantic_idx");
        defer detail.deinit(alloc);
        try std.testing.expectEqual(@as(u16, 200), detail.status);
        var parsed_detail = try std.json.parseFromSlice(IndexDetail, alloc, detail.body, .{ .ignore_unknown_fields = true });
        defer parsed_detail.deinit();
        if (parsed_detail.value.status) |idx| {
            if ((idx.doc_count orelse 0) == total_docs and
                (idx.total_indexed orelse 0) == total_docs and
                (idx.replay_applied_sequence orelse 0) == (idx.replay_target_sequence orelse 0) and
                !(idx.replay_catch_up_required orelse false) and
                !(idx.backfill_active orelse false) and
                !(idx.rebuilding orelse false))
            {
                ready = true;
                break;
            }
        }
        sleepNs(10 * std.time.ns_per_ms);
    }
    try std.testing.expect(ready);

    var response = try server.handlePublicTableQuery("docs", query_json, null);
    defer response.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, response.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expect(parsed.value.object.get("responses") != null);
}
