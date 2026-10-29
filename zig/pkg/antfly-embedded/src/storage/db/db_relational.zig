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

//! Relational operations of the local storage DB. Methods take `*DB` and
//! are re-exported from `DB` in db.zig, so callers keep writing `db.method()`.

const db_zig = @import("db.zig");
const DB = db_zig.DB;
const TestDb = db_zig.TestDb;
const RelationalProjectionPlan = @import("query/relational_projection.zig").Plan;
const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
const background_runtime_mod = @import("../background_runtime.zig");
const db_config = @import("config.zig");
const db_core = @import("core.zig");
const derived_types = @import("derived/derived_types.zig");
const doc_identity = @import("doc_identity.zig");
const doc_set = @import("doc_set.zig");
const docstore_mod = @import("../docstore.zig");
const empty_generation_handoff = @import("empty_generation_handoff.zig");
const graph_retirement_seal = @import("graph_retirement_seal.zig");
const internal_keys = @import("../internal_keys.zig");
const lsm_backend_mod = @import("../lsm_backend/mod.zig");
const mapper = @import("document/mapper.zig");
const merge_state_mod = @import("merge_state.zig");
const native_topology_receipt = @import("native/topology_receipt.zig");
const online_merge_io = @import("online_merge_io.zig");
const online_source_contract = @import("online_source_contract.zig");
const platform = @import("antfly_platform");
const portable_backup = @import("../portable_backup.zig");
const public_table_schema = @import("../../schema/mod.zig");
const receipt_position = @import("receipt_position.zig");
const relational_columns = @import("relational_columns.zig");
const relational_constraint_jobs = @import("relational_constraint_jobs.zig");
const relational_index_catalog = @import("relational_index_catalog.zig");
const relational_index_gc = @import("relational_index_gc.zig");
const relational_index_jobs = @import("relational_index_jobs.zig");
const relational_index_maintenance_contract = @import("relational_index_maintenance_contract.zig");
const relational_index_mod = @import("../relational_index.zig");
const relational_index_records = @import("relational_index_records.zig");
const relational_initial_child_publication = @import("relational_initial_child_publication.zig");
const relational_integrity_activation = @import("relational_integrity_activation.zig");
const relational_integrity_catalog = @import("relational_integrity_catalog.zig");
const relational_integrity_generation_admission = @import("relational_integrity_generation_admission.zig");
const relational_integrity_generation_retirement = @import("relational_integrity_generation_retirement.zig");
const relational_integrity_handoff = @import("relational_integrity_handoff.zig");
const relational_integrity_json = @import("relational_integrity_json.zig");
const relational_integrity_topology = @import("relational_integrity_topology.zig");
const relational_integrity_topology_contract = @import("relational_integrity_topology_contract.zig");
const relational_row_codec = @import("algebraic/relational_row_codec.zig");
const relational_store = @import("relational_store.zig");
const replication_effects_mod = @import("replication_effects.zig");
const resource_manager_mod = @import("../resource_manager.zig");
const restore_staging_contract = @import("restore_staging_contract.zig");
const row_policy_gate_mod = @import("row_policy_gate.zig");
const schema_mod = @import("../schema.zig");
const source_authority_mod = @import("../source_authority.zig");
const std = @import("std");
const storage_engine = @import("../engine.zig");
const table_catalog_mod = @import("table_catalog.zig");
const transactions_mod = @import("../transactions.zig");
const ttl_runtime_mod = @import("maintenance/ttl_runtime.zig");
const types = @import("types.zig");
const vopr_mod = @import("vopr");
const Allocator = db_zig.Allocator;
const BatchExecutionOptions = db_zig.BatchExecutionOptions;
const OpenOptions = db_zig.OpenOptions;
const OrderedApplyReceipt = db_zig.OrderedApplyReceipt;
const PrimaryBackend = db_zig.PrimaryBackend;
const RequestPreparationContext = db_zig.RequestPreparationContext;
const checkLookupOptionsActive = db_zig.checkLookupOptionsActive;
const collectTextDocumentWritesForIndex = db_zig.collectTextDocumentWritesForIndex;
const currentTimeNs = db_zig.currentTimeNs;
const deleteExpiredDocumentsFromCandidates = db_zig.deleteExpiredDocumentsFromCandidates;
const drainTestRelationalMaintenance = db_zig.drainTestRelationalMaintenance;
const ensureSnapshotActive = db_zig.ensureSnapshotActive;
const expectTestRelationalIndexRow = db_zig.expectTestRelationalIndexRow;
const initStoppedTtlRuntimeForTest = db_zig.initStoppedTtlRuntimeForTest;
const lockApply = db_zig.lockApply;
const lockApplyShared = db_zig.lockApplyShared;
const lockAtomicWithBackoff = db_zig.lockAtomicWithBackoff;
const missingAsNull = db_zig.missingAsNull;
const openModeRequiresReadOnlyBackends = db_zig.openModeRequiresReadOnlyBackends;
const orderedApplyDisposition = db_zig.orderedApplyDisposition;
const platform_time = db_zig.platform_time;
const productionLsmPhysicalChurnBenchmark = db_zig.productionLsmPhysicalChurnBenchmark;
const public_schema_json_key = db_zig.public_schema_json_key;
const readOrderedApplyReceipt = db_zig.readOrderedApplyReceipt;
const relationalColumns = db_zig.relationalColumns;
const relationalPreparationWorkers = db_zig.relationalPreparationWorkers;
const requireRelationalConsumerFields = db_zig.requireRelationalConsumerFields;
const seedColumnScanPlanTest = db_zig.seedColumnScanPlanTest;
const stageApplyMarkers = db_zig.stageApplyMarkers;
const testColumnarSelection = db_zig.testColumnarSelection;
const testRelationalBoundScan = db_zig.testRelationalBoundScan;
const testRelationalCleanupBackend = db_zig.testRelationalCleanupBackend;
const testRelationalDecodedReuse = db_zig.testRelationalDecodedReuse;
const ConstraintValidationStatus = DB.ConstraintValidationStatus;
const NativeTopologyContext = DB.NativeTopologyContext;
const RelationalIndexBuildBudget = DB.RelationalIndexBuildBudget;
const RelationalIndexBuildStatus = DB.RelationalIndexBuildStatus;
const RelationalReadSession = DB.RelationalReadSession;
const RelationalRows = DB.RelationalRows;
const RelationalStatementSnapshot = DB.RelationalStatementSnapshot;
const RelationalTopologyIdentity = DB.RelationalTopologyIdentity;
const RelationalTopologyStatus = DB.RelationalTopologyStatus;
const artifact_repair_metadata_poll_ns = DB.artifact_repair_metadata_poll_ns;
const hasCoordinatedConstraints = DB.hasCoordinatedConstraints;
const intersectResolvedFilterIncludeAlloc = DB.intersectResolvedFilterIncludeAlloc;
const nativeRestoreStoragePublicationCompatible = DB.nativeRestoreStoragePublicationCompatible;
const unionResolvedFilterExcludeAlloc = DB.unionResolvedFilterExcludeAlloc;

pub fn constraintValidationStatus(self: *DB) !ConstraintValidationStatus {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self});
    try self.lockApplySharedForPortableRuntime();
    defer self.core.unlockApplyShared();
    var view = self.core.acquireSchemaView() orelse return error.ConstraintNotFound;
    defer view.release();
    var read = try self.core.store.beginReadTxn();
    defer read.abort();
    const progress = try relational_constraint_jobs.status(&read, view);
    return .{ .schema_version = progress.schema_version, .state = progress.state, .rows_scanned = progress.rows_scanned, .failed_check = if (progress.state == .invalid) progress.failed_check else null };
}

pub fn validateConstraintsStep(self: *DB, budget: relational_constraint_jobs.Budget) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, budget });
    return self.validateConstraintsStepLocal(budget, false);
}

pub fn validateConstraintsStepLocal(self: *DB, budget: relational_constraint_jobs.Budget, replicated: bool) !bool {
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    {
        var active = self.core.acquireSchemaView() orelse return false;
        defer active.release();
        const validator = active.validator() orelse return false;
        if (validator.execution.checks == null) return false;
    }
    var replication_mutation = if (replicated) null else self.acquireReplicationMutationShared();
    defer if (replication_mutation) |*lease| lease.release();
    if (!replicated) try self.enforceReplicationWriteGate();
    var snapshot_mutation = self.core.snapshot_admission.acquireMutation();
    defer snapshot_mutation.release();
    var replay_mutation = try self.acquireSnapshotReplayMutation();
    defer replay_mutation.release();
    var page = (try relational_constraint_jobs.Page.prepare(self.alloc, self.backend_runtime.io(), self.core, budget)) orelse return false;
    defer page.deinit();
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    if (!replicated) try self.enforceReplicationWriteGate();
    try page.commit(self.core);
    return true;
}

/// Retry only an observed failed epoch, never reset a newer declaration.
pub fn retryConstraintValidation(self: *DB, schema_version: u32) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, schema_version });
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    var write_fence = try self.beginReplicatedWrite(.gate);
    defer write_fence.release();
    var snapshot_mutation = self.core.snapshot_admission.acquireMutation();
    defer snapshot_mutation.release();
    var replay_mutation = try self.acquireSnapshotReplayMutation();
    defer replay_mutation.release();
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    try self.enforceReplicationWriteGate();
    var view = self.core.acquireSchemaView() orelse return error.ConstraintNotFound;
    defer view.release();
    if (view.version() != schema_version) return error.PreparedGenerationChanged;
    var txn = try self.core.store.beginWriteTxn();
    errdefer txn.abort();
    if ((try relational_constraint_jobs.status(&txn, view)).state != .invalid) {
        txn.abort();
        return false;
    }
    try txn.delete(relational_constraint_jobs.progress_key);
    try txn.commit();
    self.local_execution.relational_index_maintenance_sweep.request();
    self.local_execution.relational_index_retry_after_ns.store(0, .release);
    return true;
}

/// A single bounded maintenance increment. Derived entries and local
/// coverage are reconstructed independently on every replica; progress is
/// never a hot-standby command and cannot make a different owner query-ready.
pub fn buildRelationalIndexStep(self: *DB, name: []const u8, budget: RelationalIndexBuildBudget) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, name, budget });
    return self.buildRelationalIndexStepLocal(name, budget, false);
}

pub fn buildRelationalIndexStepLocal(self: *DB, name: []const u8, budget: RelationalIndexBuildBudget, replicated: bool) !void {
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    var replication_mutation = if (replicated) null else self.acquireReplicationMutationShared();
    defer if (replication_mutation) |*lease| lease.release();
    if (!replicated) try self.enforceReplicationWriteGate();
    var snapshot_mutation = self.core.snapshot_admission.acquireMutation();
    defer snapshot_mutation.release();
    var replay_mutation = try self.acquireSnapshotReplayMutation();
    defer replay_mutation.release();
    var page = (try relational_index_jobs.Page.prepare(self.alloc, self.backend_runtime.io(), self.core, name, budget)) orelse return;
    defer page.deinit();
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    if (!replicated) try self.enforceReplicationWriteGate();
    try page.commit(self.core);
}

pub fn collectRelationalIndexGarbageStep(self: *DB) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self});
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    var write_fence = try self.beginReplicatedWrite(.gate);
    defer write_fence.release();
    var snapshot_mutation = self.core.snapshot_admission.acquireMutation();
    defer snapshot_mutation.release();
    var replay_mutation = try self.acquireSnapshotReplayMutation();
    defer replay_mutation.release();
    var page = (try relational_index_gc.Page.prepare(self.alloc, self.backend_runtime.io(), self.core)) orelse return false;
    defer page.deinit();
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    try self.enforceReplicationWriteGate();
    try page.commit(self.core);
    return true;
}

/// Retry only the observed failed generation. Healthy/building retries
/// are idempotent; an old admin request cannot reset a replacement index.
pub fn retryRelationalIndexBuild(self: *DB, name: []const u8, generation: u64) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, name, generation });
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    var write_fence = try self.beginReplicatedWrite(.gate);
    defer write_fence.release();
    var snapshot_mutation = self.core.snapshot_admission.acquireMutation();
    defer snapshot_mutation.release();
    var replay_mutation = try self.acquireSnapshotReplayMutation();
    defer replay_mutation.release();
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    try self.enforceReplicationWriteGate();
    var pinned = self.core.relational_indexes.acquire() orelse return error.IndexNotFound;
    defer pinned.deinit();
    const index = for (pinned.plan.boundIndexes()) |index| {
        if (std.mem.eql(u8, name, index.name)) break index;
    } else return error.IndexNotFound;
    if (index.generation != generation) return error.PreparedGenerationChanged;
    var maintenance_manager = try self.core.initTxnManager();
    defer maintenance_manager.deinit();
    try maintenance_manager.checkOrdinaryWriteConflict(&relational_index_maintenance_contract.controlKey(index.id()));
    var txn = try self.core.store.beginWriteTxn();
    errdefer txn.abort();
    const current_progress = try relational_index_jobs.status(&txn, index);
    if (current_progress.state != .failed) {
        txn.abort();
        return false;
    }
    const reset_progress = try relational_index_jobs.resetProgress(current_progress);
    const encoded_progress = try reset_progress.encode(self.alloc);
    defer self.alloc.free(encoded_progress);
    try txn.put(&relational_index_jobs.progressKey(index.id()), encoded_progress);
    try txn.commit();
    self.local_execution.relational_index_maintenance_sweep.request();
    self.local_execution.relational_index_retry_after_ns.store(0, .release);
    return true;
}

pub fn captureRelationalStatementSnapshot(self: *DB) !RelationalStatementSnapshot {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self});
    // Capturing a visibility cut does not expose row bytes. A protected
    // statement captures only the owner policy epoch; each later scan
    // must present an authenticated proof for that same epoch and retain
    // its own bounded policy lease. An unprotected cut instead pins a raw
    // lease so activation cannot overtake an in-flight statement.
    try self.maybeFinalizePendingRowPolicyPublication();
    const initial_phase = self.local_execution.row_policy_gate.currentPhase();
    var row_policy_lease: ?row_policy_gate_mod.Gate.Lease = if (initial_phase == .disabled)
        try self.local_execution.row_policy_gate.enterRawRead()
    else if (initial_phase == .active)
        null
    else
        return error.RowPolicyAuthenticationRequired;
    errdefer if (row_policy_lease) |*lease| lease.release();
    const policy_generation = self.local_execution.row_policy_gate.generation.load(.acquire);
    const policy_epoch = self.local_execution.row_policy_gate.catalog_epoch.load(.acquire);
    try self.lockApplySharedForPortableRuntime();
    defer self.core.unlockApplyShared();
    var view = self.core.acquireSchemaView() orelse return error.RelationalTableRequired;
    defer view.release();
    if (view.storageMode() != .relational) return error.RelationalTableRequired;
    var read = try self.core.store.beginReadTxn();
    errdefer read.abort();
    if (self.local_execution.row_policy_gate.currentPhase() != initial_phase or
        self.local_execution.row_policy_gate.generation.load(.acquire) != policy_generation or
        self.local_execution.row_policy_gate.catalog_epoch.load(.acquire) != policy_epoch)
        return error.RowPolicyCatalogChanged;
    return .{
        .row_policy_lease = row_policy_lease,
        .row_policy_phase = initial_phase,
        .row_policy_generation = policy_generation,
        .row_policy_catalog_epoch = policy_epoch,
        .read = read,
        .schema_version = view.version(),
    };
}

pub fn beginRelationalRows(self: *DB, alloc: Allocator, request: RelationalRows.Request) !RelationalRows.Reader {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, request });
    var row_policy_lease = try self.local_execution.row_policy_gate.enterRawRead();
    errdefer row_policy_lease.release();
    var reader = try self.beginRelationalRowsAtSnapshot(alloc, request, null);
    reader.row_policy_lease = row_policy_lease;
    return reader;
}

pub fn beginRelationalRowsAtSnapshot(self: *DB, alloc: Allocator, request: RelationalRows.Request, statement: ?*RelationalStatementSnapshot) !RelationalRows.Reader {
    // A delayed scan may use only the primary path. READY index state and
    // auto-index plans can change after the original visibility cut.
    if (statement != null and (request.index != null or request.auto_index)) return error.SqlStatementSnapshotRequired;
    try self.lockApplySharedForPortableRuntime();
    var locked = true;
    defer if (locked) self.core.unlockApplyShared();
    var view = self.core.acquireSchemaView() orelse return error.RelationalTableRequired;
    defer view.release();
    if (statement) |cut| if (view.version() != cut.schema_version) return error.PreparedGenerationChanged;
    var indexes = self.core.relational_indexes.acquire();
    defer if (indexes) |*pinned| pinned.deinit();
    const read_snapshot = if (statement) |cut| try cut.read.forkRead() else try self.core.store.beginReadTxn();
    const authenticated = self.core.store.valuesAreAuthenticated();
    const now = currentTimeNs();
    self.core.unlockApplyShared();
    locked = false;
    // Immutable pins and the read transaction were captured together.
    // Allocation, predicate compilation, readiness I/O and cardinality
    // probes must not hold the global apply publication fence.
    return try RelationalRows.Reader.openSnapshot(alloc, read_snapshot, authenticated, view, indexes, request, now);
}

pub fn relationalIndexBuildStatus(self: *DB, name: []const u8) !RelationalIndexBuildStatus {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, name });
    try self.lockApplySharedForPortableRuntime();
    defer self.core.unlockApplyShared();
    var pinned = self.core.relational_indexes.acquire() orelse return error.IndexNotFound;
    defer pinned.deinit();
    const index = for (pinned.plan.boundIndexes()) |index| {
        if (std.mem.eql(u8, name, index.name)) break index;
    } else return error.IndexNotFound;
    var read = try self.core.store.beginReadTxn();
    defer read.abort();
    const progress = try relational_index_jobs.status(&read, index);
    return .{ .generation = progress.id.generation, .slot = progress.id.slot, .state = progress.state, .rows_scanned = progress.rows_scanned, .failure = progress.failure };
}

pub fn lookupRelationalIntegrityCatalog(self: *DB, alloc: Allocator) !?types.LookupResult {
    const catalog_mod = relational_integrity_catalog;
    self.core.lockApplyShared();
    defer self.core.unlockApplyShared();
    var pinned = self.core.acquireSchemaView() orelse return error.InvalidRelationalRowsRequest;
    defer pinned.release();
    if (pinned.storageMode() != .relational) return error.InvalidRelationalRowsRequest;
    const bytes = (try self.core.getStoreValue(alloc, catalog_mod.key)) orelse return null;
    defer alloc.free(bytes);
    var catalog = try catalog_mod.decode(alloc, bytes);
    defer catalog.deinit();
    if (catalog.schema_version != pinned.version()) return error.IntegrityCatalogChanged;
    const incarnation = try catalog_mod.incarnationFromTableId(self.core.identity_namespace.table_id);
    if (!std.mem.eql(u8, &incarnation, &catalog.incarnation)) return error.IntegrityCatalogIncarnationMismatch;
    const layout = try schema_mod.serializeSchema(alloc, pinned.tableSchema().*);
    defer alloc.free(layout);
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(layout, &digest, .{});
    if (!std.mem.eql(u8, &digest, &catalog.schema_digest)) return error.IntegrityCatalogChanged;
    const encoded = try alloc.alloc(u8, std.base64.standard.Encoder.calcSize(bytes.len));
    defer alloc.free(encoded);
    _ = std.base64.standard.Encoder.encode(encoded, bytes);
    return .{ .json = try std.fmt.allocPrint(alloc, "{{\"catalog\":{f},\"schema_version\":{d},\"table_id\":\"{d}\"}}", .{
        std.json.fmt(encoded, .{}), pinned.version(), self.core.identity_namespace.table_id,
    }) };
}

pub fn lookupRelationalIndexStatus(self: *DB, alloc: Allocator, bytes: []const u8, opts: types.LookupOptions) !?types.LookupResult {
    const contract = @import("relational_index_status_contract.zig");
    if (bytes.len > 4096) return error.InvalidArgument;
    var request = try std.json.parseFromSlice(contract.Request, alloc, bytes, .{});
    defer request.deinit();
    if (request.value.name) |name| if (name.len == 0 or name.len > 1024) return error.InvalidArgument;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const temporary = arena.allocator();
    var statuses: std.ArrayList(contract.NamedStatus) = .empty;
    // Capture one owner snapshot, then release the apply lock and storage
    // snapshot before JSON encoding/network delivery. All escaped slices
    // below are request-owned, not borrowed from a retired schema plan.
    {
        try self.lockApplySharedForPortableRuntime();
        defer self.core.unlockApplyShared();
        var view = self.core.acquireSchemaView() orelse return error.RelationalTableRequired;
        defer view.release();
        if (view.storageMode() != .relational) return error.RelationalTableRequired;
        if (view.version() != request.value.schema_version) return error.PreparedGenerationChanged;
        var pinned = self.core.relational_indexes.acquire() orelse return error.IndexNotFound;
        defer pinned.deinit();
        if (pinned.plan.schemaView().epoch != view.epoch) return error.PreparedGenerationChanged;
        var read = try self.core.store.beginReadTxn();
        defer read.abort();
        const range = self.core.byteRange();
        var admitted_bytes: usize = 0;
        for (pinned.plan.boundIndexes()) |index| {
            if (request.value.name) |name| if (!std.mem.eql(u8, index.name, name)) continue;
            // JSON's worst case is six bytes per input byte. Admit before
            // materializing status arrays or encoding escaped range keys.
            admitted_bytes +|= 2048 +| (6 *| (index.name.len +| range.start.len +| range.end.len));
            if (admitted_bytes > contract.max_response_bytes) return error.ResourceBudgetExceeded;
        }
        const range_start = try temporary.dupe(u8, range.start);
        const range_end = try temporary.dupe(u8, range.end);
        const ownership = try relational_index_jobs.ownership(&read);
        for (pinned.plan.boundIndexes()) |index| {
            try checkLookupOptionsActive(opts);
            if (request.value.name) |name| if (!std.mem.eql(u8, index.name, name)) continue;
            const proof = try relational_index_jobs.statusProofWithOwnership(&read, index, ownership);
            const progress = proof.progress;
            const value = contract.Status{
                .table_id = self.core.identity_namespace.table_id,
                .schema_version = view.version(),
                .generation = index.generation,
                .slot = index.slot,
                .catalog = pinned.head.blob_digest,
                .comparison = progress.comparison,
                .owner = progress.owner,
                .range_start = range_start,
                .range_end = range_end,
                .state = switch (progress.state) {
                    .building => .building,
                    .ready => .ready,
                    .failed => .failed,
                },
                .rows_scanned = progress.rows_scanned,
                .progress_digest = proof.digest,
                .maintenance_epoch = proof.maintenance_epoch,
                .last_maintenance_request = proof.last_maintenance_request,
                .failure = switch (progress.failure) {
                    .none => .none,
                    .incompatible_schema => .incompatible_schema,
                    .invalid_row => .invalid_row,
                    .key_too_large => .key_too_large,
                },
            };
            try statuses.append(temporary, .{ .name = try temporary.dupe(u8, index.name), .status = value });
        }
    }
    if (request.value.name != null and statuses.items.len == 0) return error.IndexNotFound;
    try checkLookupOptionsActive(opts);
    const json = if (request.value.name != null)
        try std.json.Stringify.valueAlloc(alloc, statuses.items[0].status, .{})
    else
        try std.json.Stringify.valueAlloc(alloc, contract.Batch{ .statuses = statuses.items }, .{});
    errdefer alloc.free(json);
    if (json.len > contract.max_response_bytes) return error.ResourceBudgetExceeded;
    try checkLookupOptionsActive(opts);
    return .{ .json = json };
}

pub fn requireSupportedRelationalTopology(self: *DB) !void {
    if (self.local_execution.row_policy_gate.currentPhase() != .disabled) return error.RowPolicyTopologyUnsupported;
    var view = self.core.acquireSchemaView();
    defer if (view) |*pinned| pinned.release();
    // Initial physical transfer understands routed integrity records, but
    // the online delta/intent handoff does not yet carry those namespaces.
    if (hasCoordinatedConstraints(view)) return error.CoordinatedConstraintTopologyUnsupported;
}

/// Cohort capture verifies the exact durable owner fence and drained
/// participant set under the same apply lock that pins the native root.
pub fn snapshotRelationalCohort(self: *DB, id: []const u8, expected: relational_integrity_topology.Fence, cancellation: types.CancellationToken) !u64 {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, id, expected, cancellation });
    var row_policy_lease = try self.local_execution.row_policy_gate.enterRaw();
    defer row_policy_lease.release();
    if (expected.role != .backup_snapshot) return error.InvalidIntegrityTopologyFence;
    return try self.snapshotInternal(id, true, cancellation, null, expected, false, null);
}

pub fn refreshRelationalRuntimeMode(self: *DB) !void {
    const enabled = relationalColumns(self) != null;
    self.async_context.relational_base_rows = enabled;
    self.core.index_manager.setRelationalBaseRows(enabled);
    if (self.ttl_cleanup_context) |ctx| ctx.batch.relational_base_rows = enabled;
    if (self.enrichment_runtime) |runtime| runtime.setRelationalBaseRows(enabled);
    if (self.transaction_recovery_identity_context) |ctx| try ctx.updateRelationalColumns(
        relationalColumns(self),
        if (self.core.schema) |schema| schema.version else 0,
    );
}

pub fn publishRelationalRuntimeModePrepared(self: *DB, prepared: ?*db_core.PreparedRecoveryRelationalState) void {
    const enabled = relationalColumns(self) != null;
    self.async_context.relational_base_rows = enabled;
    self.core.index_manager.setRelationalBaseRows(enabled);
    if (self.ttl_cleanup_context) |ctx| ctx.batch.relational_base_rows = enabled;
    if (self.enrichment_runtime) |runtime| runtime.setRelationalBaseRows(enabled);
    if (self.transaction_recovery_identity_context) |ctx| {
        ctx.installPreparedRelationalState(prepared orelse unreachable);
    }
}

/// Storage mode is immutable once a DB contains a schema or user rows, but
/// take the shared apply lock so callers never race initial publication.
pub fn usesRelationalStorage(self: *DB) bool {
    lockApplyShared(self);
    defer self.core.unlockApplyShared();
    return relationalColumns(self) != null;
}

pub fn lookupRelationalTopology(self: *DB, alloc: Allocator, request_json: []const u8, opts: types.LookupOptions) !?types.LookupResult {
    var request = try std.json.parseFromSlice(struct {
        mode: enum { identity, status, completed, parent_activation, generation_publication, generation_handoff_summary, generation_handoff_seal, generation_handoff_install, generation_handoff_identity, graph_retirement, graph_endpoint_cleanup, initial_child_preflight, initial_child_publication, public_schema, generation_gc, handoff_progress, handoff_manifest, prune_progress, online_source_status, merge_copy_receipt },
        scope: ?online_source_contract.Scope = null,
    }, alloc, request_json, .{});
    defer request.deinit();
    if ((request.value.mode == .online_source_status) != (request.value.scope != null)) return error.InvalidOnlineSourceCommand;
    var output: std.Io.Writer.Allocating = .init(alloc);
    errdefer output.deinit();
    var stream: std.json.Stringify = .{ .writer = &output.writer, .options = .{} };
    switch (request.value.mode) {
        .identity => try relational_integrity_json.write(try self.relationalTopologyIdentity(), &stream),
        .generation_handoff_identity => {
            const scope = opts.restore_staging_scope orelse return error.RestoreStagingScopeChanged;
            const plan_id = opts.restore_staging_plan_id orelse return error.RestoreStagingScopeChanged;
            {
                var read = try self.core.store.beginProbeTxn();
                defer read.abort();
                const contract = restore_staging_contract;
                const raw = read.get(contract.bootstrap_key) catch return error.RestoreStagingScopeChanged;
                var bootstrap = try contract.OwnerBootstrap.decode(alloc, raw);
                defer bootstrap.deinit();
                try bootstrap.value.validate();
                if (bootstrap.value.empty_generation_handoff == null or
                    !bootstrap.value.scope.target_namespace.eql(self.core.identity_namespace) or
                    !std.mem.eql(u8, &scope, &bootstrap.value.scope.digest()) or
                    !std.mem.eql(u8, &plan_id, &bootstrap.value.scope.plan_id)) return error.RestoreStagingScopeChanged;
            }
            try relational_integrity_json.write(try self.relationalTopologyIdentity(), &stream);
        },
        .status => try relational_integrity_json.write(try self.relationalTopologyStatus(), &stream),
        .online_source_status => try relational_integrity_json.write(try self.onlineSourceStatus(request.value.scope.?), &stream),
        .merge_copy_receipt => {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            var read = try self.core.store.beginProbeTxn();
            defer read.abort();
            const raw = try missingAsNull(read.get(merge_state_mod.key));
            var state = if (raw) |value| try merge_state_mod.decodeAlloc(alloc, value) else null;
            defer if (state) |*value| value.deinit(alloc);
            const table = table_catalog_mod;
            const table_raw = try missingAsNull(read.get(table.key));
            const row_derived_document = if (table_raw) |value| blk: {
                const catalog = try table.Catalog.decode(value);
                break :blk catalog.mode_initialized and catalog.storage_mode == .document and
                    try online_merge_io.rowDerivedIndexesAssumeApply(self, alloc);
            } else false;
            try relational_integrity_json.write(@import("merge_contract.zig").CopyReceipt{
                .namespace = self.core.identity_namespace,
                .range = self.core.byteRange(),
                .state = state,
                .row_derived_document = row_derived_document,
            }, &stream);
        },
        .completed => {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            var read = try self.core.store.beginProbeTxn();
            defer read.abort();
            try relational_integrity_json.write(try relational_integrity_topology.completed(&read), &stream);
        },
        .parent_activation => {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            var read = try self.core.store.beginProbeTxn();
            defer read.abort();
            const retirement = relational_integrity_generation_retirement;
            const value = try retirement.ownerStatus(alloc, &read);
            defer if (value) |status| alloc.free(status.entries);
            try relational_integrity_json.write(value, &stream);
        },
        .generation_publication => {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            var read = try self.core.store.beginProbeTxn();
            defer read.abort();
            const value = try relational_integrity_generation_admission.ownerStatus(&read);
            try relational_integrity_json.write(value, &stream);
        },
        .generation_handoff_summary => {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            // This status scans bounded live scopes and streams the
            // historical retirement digest. Probe transactions cannot
            // open physical cursors; pin one coherent read snapshot.
            var read = try self.core.store.beginReadTxn();
            defer read.abort();
            var scan_options = opts;
            // A caller that omits a deadline must not pin source read
            // admission for an unbounded historical tombstone scan.
            const local_bound = opts.executionNowNs() +| 2 * std.time.ns_per_s;
            scan_options.execution_deadline_ns = @min(opts.execution_deadline_ns orelse local_bound, local_bound);
            const Scan = struct {
                fn check(ptr: *anyopaque) !void {
                    const options: *types.LookupOptions = @ptrCast(@alignCast(ptr));
                    try checkLookupOptionsActive(options.*);
                }
            };
            var value = try empty_generation_handoff.summaryAllocControlled(alloc, &read, self.core.identity_namespace, .{ .ptr = &scan_options, .check_fn = Scan.check });
            defer value.deinit(alloc);
            try relational_integrity_json.write(value, &stream);
        },
        .generation_handoff_seal => {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            var read = try self.core.store.beginProbeTxn();
            defer read.abort();
            try relational_integrity_json.write(try empty_generation_handoff.sealStatus(&read, self.core.identity_namespace), &stream);
        },
        .generation_handoff_install => {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            var read = try self.core.store.beginProbeTxn();
            defer read.abort();
            try relational_integrity_json.write(try empty_generation_handoff.loadInstallReceipt(&read), &stream);
        },
        .graph_endpoint_cleanup => {
            if (try self.prepareGraphEndpointCleanupBatch(alloc)) |owned| {
                var cleanup = owned;
                defer cleanup.deinit();
                try relational_integrity_json.write(types.GraphEndpointCleanupStatus{ .pending = true, .guards = cleanup.page.guards, .graph_deletes = cleanup.graph_deletes, .deletes = cleanup.page.deletes, .merge_artifacts = cleanup.replay_writes }, &stream);
            } else try relational_integrity_json.write(types.GraphEndpointCleanupStatus{}, &stream);
        },
        .graph_retirement => {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            var read = try self.core.store.beginProbeTxn();
            defer read.abort();
            try relational_integrity_json.write(try graph_retirement_seal.status(&read), &stream);
        },
        .initial_child_publication => {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            var read = try self.core.store.beginProbeTxn();
            defer read.abort();
            const value = try relational_initial_child_publication.load(&read);
            try relational_integrity_json.write(value, &stream);
        },
        .initial_child_preflight => {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            var read = try self.core.store.beginProbeTxn();
            defer read.abort();
            const table_raw = try missingAsNull(read.get(table_catalog_mod.key));
            const row_count: u64 = if (table_raw) |raw| (try table_catalog_mod.Catalog.decode(raw)).row_count else 0;
            const existing_schema = try missingAsNull(read.get(public_schema_json_key));
            const existing_catalog = try missingAsNull(read.get(relational_integrity_catalog.key));
            try relational_integrity_json.write(.{
                .namespace = self.core.identity_namespace,
                .row_count = row_count,
                .has_schema = existing_schema != null,
                .has_catalog = existing_catalog != null,
                .hidden = try relational_initial_child_publication.load(&read),
            }, &stream);
        },
        .public_schema => {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            var read = try self.core.store.beginProbeTxn();
            defer read.abort();
            const value = read.get(public_schema_json_key) catch return error.IntegrityCatalogChanged;
            try stream.write(std.json.Value{ .string = value });
        },
        .generation_gc => {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            // GC preparation scans a bounded key range. Probe transactions
            // support point reads only and cannot open the required cursor.
            var read = try self.core.store.beginReadTxn();
            defer read.abort();
            if (try relational_integrity_topology.current(&read) != null) {
                try relational_integrity_json.write(@as(?relational_integrity_generation_retirement.GcCommand, null), &stream);
            } else {
                var page = try relational_integrity_generation_retirement.prepareGcPage(alloc, &read, 64, 512 * 1024);
                defer if (page) |*owned_page| owned_page.deinit();
                const value: ?relational_integrity_generation_retirement.GcCommand = if (page) |*owned_page| owned_page.command(0, self.core.identity_namespace) else null;
                try relational_integrity_json.write(value, &stream);
            }
        },
        .handoff_progress => {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            var read = try self.core.store.beginProbeTxn();
            defer read.abort();
            var progress = try relational_integrity_handoff.loadProgress(alloc, &read);
            defer progress.deinit();
            try relational_integrity_json.write(progress.value, &stream);
        },
        .handoff_manifest => {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            var read = try self.core.store.beginProbeTxn();
            defer read.abort();
            const bytes = read.get(relational_integrity_handoff.manifest_key) catch |err| switch (err) {
                error.NotFound => "null",
                else => return err,
            };
            var manifest = try std.json.parseFromSlice(?relational_integrity_handoff.Manifest, alloc, bytes, .{});
            defer manifest.deinit();
            try relational_integrity_json.write(manifest.value, &stream);
        },
        .prune_progress => {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            var read = try self.core.store.beginProbeTxn();
            defer read.abort();
            const bytes = read.get(relational_integrity_handoff.prune_key) catch |err| switch (err) {
                error.NotFound => "null",
                else => return err,
            };
            var progress = try std.json.parseFromSlice(?relational_integrity_handoff.PruneProgress, alloc, bytes, .{});
            defer progress.deinit();
            try relational_integrity_json.write(progress.value, &stream);
        },
    }
    return .{ .json = try output.toOwnedSlice() };
}

pub fn relationalTopologyIdentity(self: *DB) !RelationalTopologyIdentity {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self});
    self.core.lockApplyShared();
    defer self.core.unlockApplyShared();
    var read = try self.core.store.beginProbeTxn();
    defer read.abort();
    const raw = read.get(relational_integrity_catalog.key) catch |err| switch (err) {
        error.NotFound => "",
        else => return err,
    };
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(raw, &digest, .{});
    const authority = try source_authority_mod.load(&read);
    const handoff_authority: relational_integrity_topology_contract.GenerationHandoffReceiptAuthority = if (authority) |value|
        if (std.mem.eql(u8, &value.namespace, &online_source_contract.namespaceBytes(self.core.identity_namespace)))
            if (value.kind == .raft) .raft else .native
        else
            .unsupported
    else
        .unsupported;
    return .{
        .namespace = self.core.identity_namespace,
        .catalog_digest = digest,
        .next_epoch = try relational_integrity_topology.nextEpoch(&read),
        .generation_handoff_receipt_authority = handoff_authority,
        .backup_seal_supported = self.primary_backend == .lsm and self.physical_root_mode == .filesystem_managed and
            nativeRestoreStoragePublicationCompatible(self.primary_lsm_storage) and self.backend_runtime.filesystemIo() != null and
            try self.sealedBackupSupportedLocked(),
    };
}

pub fn relationalTopologyStatus(self: *DB) !RelationalTopologyStatus {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self});
    self.core.lockApplyShared();
    defer self.core.unlockApplyShared();
    var read = try self.core.store.beginProbeTxn();
    defer read.abort();
    const fence = try relational_integrity_topology.current(&read);
    var manager = try self.core.initTxnManager();
    defer manager.deinit();
    return .{ .fence = fence, .drained = !try manager.hasTopologySensitiveTransactions() };
}

/// The allocator belongs to one bounded transfer request/arena. All slices
/// in this manifest are owned; none retain a source cursor or schema view.
pub fn relationalHandoffManifest(
    self: *DB,
    alloc: Allocator,
    source: relational_integrity_topology.Fence,
    destination: relational_integrity_topology.Fence,
    lower: []const u8,
    upper: []const u8,
    primary_sequence: u64,
) !relational_integrity_handoff.Manifest {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, source, destination, lower, upper, primary_sequence });
    self.core.lockApplyShared();
    defer self.core.unlockApplyShared();
    var read = try self.core.store.beginProbeTxn();
    defer read.abort();
    var manager = try self.core.initTxnManager();
    defer manager.deinit();
    try relational_integrity_topology.requireDrained(&read, &manager, source);
    const owned_range = self.core.byteRange();
    if (std.mem.order(u8, lower, owned_range.start) == .lt or
        (owned_range.end.len != 0 and (upper.len == 0 or std.mem.order(u8, upper, owned_range.end) == .gt))) return error.KeyOutOfRange;
    const raw_catalog = try read.get(relational_integrity_catalog.key);
    var compiled = try relational_integrity_catalog.decode(alloc, raw_catalog);
    defer compiled.deinit();
    try relational_integrity_activation.requireReady(&read, compiled);
    return .{
        .source = source,
        .destination = destination,
        .lower = try alloc.dupe(u8, lower),
        .upper = try alloc.dupe(u8, upper),
        .source_range_start = try alloc.dupe(u8, owned_range.start),
        .source_range_end = try alloc.dupe(u8, owned_range.end),
        .catalog_bytes = try alloc.dupe(u8, raw_catalog),
        .activation_bytes = try alloc.dupe(u8, try read.get(relational_integrity_activation.key)),
        .primary_sequence = primary_sequence,
    };
}

pub fn relationalHandoffPage(self: *DB, alloc: Allocator, manifest: relational_integrity_handoff.Manifest, progress: relational_integrity_handoff.Progress) !relational_integrity_handoff.Page {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, manifest, progress });
    self.core.lockApplyShared();
    defer self.core.unlockApplyShared();
    var read = try self.core.store.beginProbeTxn();
    defer read.abort();
    var manager = try self.core.initTxnManager();
    defer manager.deinit();
    try relational_integrity_topology.requireDrained(&read, &manager, manifest.source);
    return relational_integrity_handoff.readPage(alloc, self.core.store, manifest, progress);
}

/// Private replicated lifecycle control. Begin fences new work without
/// waiting for old participants: their later decisions must remain able to
/// apply. Callers separately poll drained before snapshot or cutover.
pub fn applyRelationalTopologyControl(
    self: *DB,
    command: relational_integrity_topology.Command,
    ordered_receipt: ?OrderedApplyReceipt,
) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, command, ordered_receipt });
    return self.applyRelationalTopologyControlWithReplication(command, ordered_receipt, null, null, null, null);
}

pub fn applyRelationalTopologyBatch(self: *DB, req: types.BatchRequest, opts: BatchExecutionOptions) !void {
    if (req.relational_index_maintenance != null) return error.InvalidBatchRequest;
    const hidden_handoff_install = if (req.relational_topology) |control| control.action == .install_generation_handoff else false;
    if (hidden_handoff_install) {
        const control = req.relational_topology.?;
        const install = control.generation_handoff_install orelse return error.InvalidBatchRequest;
        var expected_plan_id: [16]u8 = undefined;
        std.mem.writeInt(u64, expected_plan_id[0..8], control.fence.transition_id, .little);
        std.mem.writeInt(u64, expected_plan_id[8..16], control.fence.attempt, .little);
        if (req.restore_staging_scope == null or req.restore_staging_plan_id == null or
            !std.mem.eql(u8, &req.restore_staging_scope.?, &install.scope) or
            !std.mem.eql(u8, &req.restore_staging_plan_id.?, &expected_plan_id)) return error.InvalidBatchRequest;
    } else if (req.restore_staging_scope != null or req.restore_staging_plan_id != null) return error.InvalidBatchRequest;
    if (req.writes.len != 0 or req.deletes.len != 0 or req.transforms.len != 0 or
        req.graph_writes.len != 0 or req.graph_deletes.len != 0 or req.integrity.len != 0 or
        req.integrity_commands.len != 0 or req.predicates.len != 0 or req.transaction != null or
        req.restore_staging != null or
        req.relational_activation != null or req.relational_retirement != null or
        req.relational_repair or req.split_checkpoint != null or req.split_replication != null or
        req.merge_checkpoint != null or req.merge_replication != null or
        req.merge_source_transition != null or req.merge_artifacts.len != 0) return error.InvalidBatchRequest;
    if (req.relational_topology != null and req.split_transition != null) return error.InvalidBatchRequest;
    if (req.relational_topology) |command| if (command.fence.role == .backup_snapshot and (command.action != .begin and command.action != .release and command.action != .cancel)) return error.InvalidBatchRequest;
    if (req.relational_topology) |command| if ((command.action == .seal_graph_retirement and command.graph_retirement == null) or
        (command.graph_retirement != null and command.action != .begin and command.action != .seal_graph_retirement)) return error.InvalidBatchRequest;
    if (req.split_transition) |transition| if (transition.kind != .finalize) return error.InvalidBatchRequest;
    if (req.relational_topology) |command| if (command.action == .install_child_schema) {
        if ((command.fence.role != .child_generation_source and command.fence.role != .child_generation_dual) or command.child_schema_install == null or
            command.transfer != null or command.parent_retirement != null or command.parent_activation != null or
            command.child_generations != null) return error.InvalidBatchRequest;
        const install = command.child_schema_install.?;
        const entry = opts.ordered_apply_receipt orelse opts.native_fk_generation_entry orelse return error.InvalidBatchRequest;
        return self.installPublishedChildSchema(self.alloc, install.schema_json, .{
            .fence = command.fence,
            .before_schema_json_digest = install.before_schema_json_digest,
            .schema_json_digest = install.schema_json_digest,
            .before_catalog_digest = install.before_catalog_digest,
            .after_catalog_digest = install.after_catalog_digest,
            .ordered_receipt = entry,
            .native = opts.native_fk_generation_entry != null,
        });
    };
    if (req.relational_topology) |command| if (command.action == .provision_initial_child) {
        if (command.initial_child_provision == null or command.initial_child_control != null or
            command.child_schema_install != null or command.transfer != null or command.parent_retirement != null or
            command.parent_activation != null or command.child_generations != null) return error.InvalidBatchRequest;
        const provision = command.initial_child_provision.?;
        const entry = opts.ordered_apply_receipt orelse opts.native_initial_child_entry orelse return error.InvalidBatchRequest;
        var replication_mutation = if (opts.bypass_replication_write_gate) null else self.acquireReplicationMutationShared();
        defer if (replication_mutation) |*lease| lease.release();
        if (!opts.bypass_replication_write_gate) {
            if (self.local_execution.replication_async_metadata_mirror != null and self.local_execution.replication_async_batch_mirror == null) return error.ReplicationPublisherUnavailable;
            try self.enforceReplicationWriteGate();
            try self.ensureDurableReplicationStartupBarrier();
            try self.flushDurableReplicationOutboxes();
            try self.preflightReplicationBatchSyncCommit();
        }
        const payload = if (!opts.bypass_replication_write_gate and self.local_execution.replication_async_batch_mirror != null)
            try replication_effects_mod.encodeInitialChildMutationRequestAlloc(self.alloc, req, entry)
        else
            null;
        defer if (payload) |bytes| self.alloc.free(bytes);
        try self.provisionInitialHiddenChild(provision.schema_json, .{
            .fence = command.fence,
            .child_table_name = provision.child_table_name,
            .plan_id = provision.plan_id,
            .plan_digest = provision.plan_digest,
            .schema_digest = provision.schema_digest,
            .public_schema_json_digest = provision.public_schema_json_digest,
            .catalog_digest = provision.catalog_digest,
            .ordered_receipt = entry,
            .native = opts.native_initial_child_entry != null,
        }, opts.replication_applied_lsn_marker, payload);
        if (payload != null) try self.flushDurableReplicationOutboxes();
        return;
    };
    if (req.relational_topology) |command| if (command.action == .release_initial_child or command.action == .cancel_initial_child) {
        if (command.initial_child_control == null or command.initial_child_provision != null or
            command.child_schema_install != null or command.transfer != null or command.parent_retirement != null or
            command.parent_activation != null or command.child_generations != null) return error.InvalidBatchRequest;
        const entry = opts.ordered_apply_receipt orelse opts.native_initial_child_entry orelse return error.InvalidBatchRequest;
        var replication_mutation = if (opts.bypass_replication_write_gate) null else self.acquireReplicationMutationShared();
        defer if (replication_mutation) |*lease| lease.release();
        if (!opts.bypass_replication_write_gate) {
            if (self.local_execution.replication_async_metadata_mirror != null and self.local_execution.replication_async_batch_mirror == null) return error.ReplicationPublisherUnavailable;
            try self.enforceReplicationWriteGate();
            try self.ensureDurableReplicationStartupBarrier();
            try self.flushDurableReplicationOutboxes();
            try self.preflightReplicationBatchSyncCommit();
        }
        const payload = if (!opts.bypass_replication_write_gate and self.local_execution.replication_async_batch_mirror != null)
            try replication_effects_mod.encodeInitialChildMutationRequestAlloc(self.alloc, req, entry)
        else
            null;
        defer if (payload) |bytes| self.alloc.free(bytes);
        try self.applyInitialChildPhase(command.fence, command.initial_child_control.?, if (command.action == .release_initial_child) .released else .canceled, entry, opts.native_initial_child_entry != null, opts.replication_applied_lsn_marker, payload);
        if (payload != null) try self.flushDurableReplicationOutboxes();
        return;
    };
    var mutation = if (opts.bypass_replication_write_gate) null else self.acquireReplicationMutationShared();
    defer if (mutation) |*lease| lease.release();
    if (!opts.bypass_replication_write_gate) {
        try self.enforceReplicationWriteGate();
        try self.ensureDurableReplicationStartupBarrier();
        try self.flushDurableReplicationOutboxes();
        try self.preflightReplicationBatchSyncCommit();
    }
    const native_control = opts.ordered_apply_receipt == null and opts.native_fk_generation_entry == null and
        (if (req.relational_topology) |command| native_topology_receipt.supports(command) else false);
    if (native_control) try native_topology_receipt.validateRequest(self.alloc, req);
    const mirror_control = !opts.bypass_replication_write_gate and self.local_execution.replication_async_batch_mirror != null;
    const payload = if (mirror_control and !native_control)
        if (req.relational_topology) |command|
            if (command.action == .seal_graph_retirement)
                try replication_effects_mod.encodeGraphRetirementSealMutationRequestAlloc(self.alloc, req, opts.ordered_apply_receipt orelse return error.InvalidBatchRequest)
            else
                try replication_effects_mod.encodeBatchMutationRequestAlloc(self.alloc, req)
        else
            try replication_effects_mod.encodeBatchMutationRequestAlloc(self.alloc, req)
    else
        null;
    defer if (payload) |bytes| self.alloc.free(bytes);
    if (req.relational_topology) |command| {
        try self.applyRelationalTopologyControlWithReplication(command, opts.ordered_apply_receipt, opts.replication_applied_lsn_marker, payload, opts.native_fk_generation_entry, if (native_control) .{ .request = req, .replay = opts.native_topology_position, .mirror = mirror_control } else null);
    } else {
        try self.applyRangeFinalization(req.split_transition.?, opts.ordered_apply_receipt, opts.replication_applied_lsn_marker, payload);
    }
    if (mirror_control) try self.flushDurableReplicationOutboxes();
}

pub fn applyRelationalTopologyControlWithReplication(
    self: *DB,
    command: relational_integrity_topology.Command,
    ordered_receipt: ?OrderedApplyReceipt,
    replication_lsn: ?u64,
    replication_payload: ?[]const u8,
    native_receipt: ?OrderedApplyReceipt,
    native_control: ?NativeTopologyContext,
) !void {
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    const receipt_entry = ordered_receipt orelse native_receipt;
    var mutation = self.core.snapshot_admission.acquireMutation();
    defer mutation.release();
    const structural_held = command.action == .begin;
    if (structural_held) lockAtomicWithBackoff(&self.local_execution.index_structural_mutation_mutex);
    defer if (structural_held) self.local_execution.index_structural_mutation_mutex.unlock();
    try self.lockApplyForPortableRuntime();
    defer self.core.unlockApply();
    if (ordered_receipt) |entry| switch (try orderedApplyDisposition(try readOrderedApplyReceipt(self.alloc, self.core.store), entry)) {
        .already_applied => return,
        .apply => {},
    };
    const graph_catalog_lock_required = command.graph_retirement != null or
        (command.fence.role == .rewrite_source and (command.action == .cancel or command.action == .abort_transition));
    if (graph_catalog_lock_required and !self.core.index_manager.catalog_mutex.tryLockExclusive()) return error.StorageBusy;
    defer if (graph_catalog_lock_required) self.core.index_manager.catalog_mutex.unlockExclusive();
    var close_graph_after_commit = false;
    var open_graph_after_commit = false;
    if (!command.fence.namespace.eql(self.core.identity_namespace)) return error.IdentityNamespaceMismatch;
    var txn = try self.core.store.beginWriteTxn();
    errdefer txn.abort();
    if (try doc_identity.loadNamespaceTxn(&txn)) |stored| {
        if (!stored.eql(command.fence.namespace)) return error.IdentityNamespaceMismatch;
    } else {
        try doc_identity.writeNamespaceTxn(&txn, command.fence.namespace);
    }
    const topology = relational_integrity_topology;
    if (ordered_receipt != null) try source_authority_mod.requireRaftMarkerAllowed(&txn);
    if ((command.action == .transfer) != (command.transfer != null) or
        (command.action == .seal_graph_retirement and command.graph_retirement == null) or
        (command.graph_retirement != null and command.action != .begin and command.action != .seal_graph_retirement) or
        (command.generation_handoff != null and command.action != .begin) or
        ((command.action == .seal_generation_handoff) != (command.generation_handoff_seal != null)) or
        ((command.action == .install_generation_handoff) != (command.generation_handoff_install != null)) or
        (command.action == .stage_parent_retirement) != (command.parent_retirement != null) or
        ((command.action == .activate_parent_retirement or command.action == .acknowledge_parent_retirement) != (command.parent_activation != null)) or
        ((command.action == .stage_child_generation or command.action == .activate_child_generation or command.action == .acknowledge_child_generation or
            (command.action == .cancel and (command.fence.role == .child_generation_parent or command.fence.role == .child_generation_dual))) != (command.child_generations != null))) return error.InvalidBatchRequest;
    if (command.child_schema_install != null) return error.InvalidBatchRequest;
    const native_prepared: ?native_topology_receipt.Prepared = if (native_control) |control| blk: {
        if (ordered_receipt != null or native_receipt != null) return error.InvalidControlReceiptPosition;
        const owner = try source_authority_mod.load(&txn);
        if (owner == null or owner.?.kind != .native) {
            if (control.replay != null) return error.InvalidControlReceiptPosition;
            break :blk null;
        }
        break :blk try native_topology_receipt.stage(self.alloc, &txn, command, control.replay);
    } else null;
    const stamp: ?receipt_position.Position = if (native_prepared) |prepared|
        .{ .native = prepared.receipt }
    else if (ordered_receipt) |entry|
        .{ .raft = .{ .term = entry.term, .index = entry.index } }
    else
        null;
    const native_payload = if (native_control) |control|
        if (control.mirror)
            if (native_prepared) |prepared| try replication_effects_mod.encodeNativeTopologyMutationRequestAlloc(self.alloc, control.request, prepared.receipt) else try replication_effects_mod.encodeBatchMutationRequestAlloc(self.alloc, control.request)
        else
            null
    else
        null;
    defer if (native_payload) |bytes| self.alloc.free(bytes);
    if (native_prepared == null or !native_prepared.?.duplicate) switch (command.action) {
        .begin => {
            if (command.graph_retirement) |scope| {
                if (!scope.fence.eql(command.fence)) return error.InvalidBatchRequest;
                const loaded = (try self.core.index_manager.graphRetirementConfigDigestAssumeCatalogLock(self.alloc)) orelse return error.InvalidBatchRequest;
                if (!std.mem.eql(u8, &loaded, &scope.graph_config_digest)) return error.InvalidBatchRequest;
            }
            if (try topology.current(&txn) == null) {
                if (command.fence.role == .split_destination) {
                    const previous_manifest = try missingAsNull(txn.get(relational_integrity_handoff.manifest_key));
                    if (previous_manifest) |bytes| {
                        var previous = try std.json.parseFromSlice(relational_integrity_handoff.Manifest, self.alloc, bytes, .{});
                        defer previous.deinit();
                        if (!previous.value.destination.eql(command.fence)) return error.IntegrityHandoffDestinationResetRequired;
                    }
                }
                const enrichment = self.enrichmentStatsWithSupervisorState(.{ .enabled = true });
                const has_producers = enrichment.enabled or self.core.hasGeneratedEnrichmentTargets() or self.core.index_manager.enrichments.items.len != 0 or self.hasConfiguredResolvers();
                if (replication_lsn == null and has_producers) {
                    const target = self.currentMaintenanceTargetSequence();
                    if (enrichment.retrying or enrichment.worker_failed or enrichment.stalled or
                        ((enrichment.enabled or self.core.hasGeneratedEnrichmentTargets() or self.core.index_manager.enrichments.items.len != 0) and enrichment.applied_sequence < @max(target, enrichment.target_sequence))) return error.IntegrityTopologyBusy;
                    if (self.hasConfiguredResolvers()) {
                        const resolution = self.resolutionStageStats();
                        const promotion = self.promotionStageStats();
                        if (!resolution.enabled or !promotion.enabled or resolution.blocked or promotion.blocked or resolution.applied_sequence < target or promotion.applied_sequence < target) return error.IntegrityTopologyBusy;
                    }
                    // An old participant commit could enqueue a new
                    // primary-changing callback after this freeze. Drain
                    // those decisions before admitting a producer-bearing
                    // owner; ordinary producer-free tables may drain under
                    // the already-published fence without this restriction.
                    var manager = try self.core.initTxnManager();
                    defer manager.deinit();
                    if (try manager.hasTopologySensitiveTransactions()) return error.IntegrityTopologyBusy;
                }
            }
            try topology.stageBeginWithHandoff(&txn, command.fence, command.generation_handoff);
            if (command.graph_retirement) |scope| {
                try graph_retirement_seal.stageBegin(&txn, scope);
                close_graph_after_commit = true;
            }
            if (command.fence.role == .child_generation_source or command.fence.role == .child_generation_dual)
                try relational_integrity_generation_admission.stageSourceFencedReceipt(&txn, command.fence, if (receipt_entry) |entry| entry.term else 0, if (receipt_entry) |entry| entry.index else 0);
        },
        .seal_graph_retirement => {
            const scope = command.graph_retirement.?;
            if (!scope.fence.eql(command.fence)) return error.InvalidBatchRequest;
            if (!self.core.index_manager.graphRetirementPinsDrainedAssumeCatalogLock()) return error.StorageBusy;
            const loaded = (try self.core.index_manager.graphRetirementConfigDigestAssumeCatalogLock(self.alloc)) orelse return error.InvalidBatchRequest;
            if (!std.mem.eql(u8, &loaded, &scope.graph_config_digest)) return error.InvalidBatchRequest;
            _ = try graph_retirement_seal.stageSealPosition(&txn, scope, stamp orelse return error.InvalidBatchRequest);
        },
        .seal_generation_handoff => {
            try empty_generation_handoff.stageSealPosition(&txn, command.fence, command.generation_handoff_seal.?, stamp orelse return error.InvalidBatchRequest);
        },
        .install_generation_handoff => {
            if (command.fence.role != .rewrite_destination) return error.InvalidGenerationHandoff;
            const install_stamp = stamp orelse return error.InvalidBatchRequest;
            try install_stamp.requireNamespace(command.fence.namespace);
            const install = command.generation_handoff_install.?;
            const contract = restore_staging_contract;
            const logical = try empty_generation_handoff.installReceiptDigest(install);
            var expected_plan_id: [16]u8 = undefined;
            std.mem.writeInt(u64, expected_plan_id[0..8], command.fence.transition_id, .little);
            std.mem.writeInt(u64, expected_plan_id[8..16], command.fence.attempt, .little);
            const bootstrap_raw = txn.get(contract.bootstrap_key) catch return error.RestoreStagingScopeChanged;
            var bootstrap = try contract.OwnerBootstrap.decode(self.alloc, bootstrap_raw);
            defer bootstrap.deinit();
            const binding = bootstrap.value.empty_generation_handoff orelse return error.GenerationHandoffIntentMissing;
            const raw_progress = txn.get(contract.key) catch return error.RestoreStagingScopeChanged;
            var progress = try contract.Progress.decode(self.alloc, raw_progress);
            defer progress.deinit();
            if (!bootstrap.value.scope.target_namespace.eql(command.fence.namespace) or
                !std.mem.eql(u8, &bootstrap.value.scope.plan_id, &expected_plan_id) or
                !std.mem.eql(u8, &bootstrap.value.scope.digest(), &install.scope) or
                !std.mem.eql(u8, &progress.value.scope.digest(), &install.scope) or
                progress.value.phase != .validated or
                !std.mem.eql(u8, &binding.source_summary_digest, &install.source_summary_digest) or
                !std.mem.eql(u8, &binding.retired_digest, &install.retired_digest) or
                binding.retired_count != install.retired_count or
                !std.mem.eql(u8, &binding.expected_install_receipt_digest, &logical))
                return error.RestoreStagingScopeChanged;
            _ = try empty_generation_handoff.stageInstallPosition(self.alloc, &txn, install, bootstrap.value.scope.plan_id, install_stamp);
        },
        .release => {
            if (command.fence.role == .child_generation_parent or command.fence.role == .child_generation_source or command.fence.role == .child_generation_dual or command.fence.role == .truncate_parent)
                return error.GenerationAdmissionActivationRequired;
            if ((command.fence.role == .split_destination or command.fence.role == .merge_destination) and try topology.current(&txn) != null) {
                var progress = try relational_integrity_handoff.loadProgress(self.alloc, &txn);
                defer progress.deinit();
                if (!progress.value.ready) return error.IntegrityHandoffIncomplete;
                if (command.fence.role == .split_destination) {
                    const marker = (try self.core.loadSplitBootstrapMarker(self.alloc)) orelse return error.SplitBootstrapIncomplete;
                    if (!marker.bootstrap_complete or marker.transition_id != command.fence.transition_id or marker.attempt_epoch != command.fence.attempt) return error.SplitBootstrapIncomplete;
                } else {
                    var state = try merge_state_mod.decodeAlloc(self.alloc, try txn.get(merge_state_mod.key));
                    defer state.deinit(self.alloc);
                    if (state.phase != .finalized or state.transition_id != command.fence.transition_id or state.donor_group_id != command.fence.peer_group_id) return error.MergeTransitionNotReady;
                }
                var compiled = try relational_integrity_catalog.decode(self.alloc, try txn.get(relational_integrity_catalog.key));
                defer compiled.deinit();
                try relational_integrity_activation.requireReady(&txn, compiled);
            } else if (command.fence.role != .backup_snapshot and try topology.current(&txn) != null) return error.IntegrityTopologyCutoverRequired;
            try topology.stageRelease(&txn, command.fence);
            if (command.fence.role == .merge_destination) {
                try txn.delete(relational_integrity_handoff.manifest_key);
                try txn.delete(relational_integrity_handoff.progress_key);
                try txn.delete(relational_integrity_handoff.prune_key);
            }
        },
        .cancel => {
            // A merge receiver is already a published owner. Its imported
            // off-range records must be drained by rollback/prune before
            // admission resumes; generic cancellation is not that proof.
            if (command.fence.role == .merge_source or command.fence.role == .merge_destination) return error.InvalidBatchRequest;
            // A child-source fence protects the old FK generation while
            // parent owners transition. Only a metadata-authenticated
            // abort decision may lift it; generic cancellation could
            // admit a delayed old-generation attach after activation.
            if (command.fence.role == .child_generation_source) return error.GenerationAdmissionActivationRequired;
            if (command.fence.role == .child_generation_parent or command.fence.role == .child_generation_dual) {
                const admission = relational_integrity_generation_admission;
                const transitions = command.child_generations.?;
                try admission.validateTransitions(transitions);
                for (transitions) |transition| try admission.cancelTransition(self.alloc, &txn, transition);
                try admission.stageCanceledReceipt(&txn, command.fence, transitions, if (receipt_entry) |entry| entry.term else 0, if (receipt_entry) |entry| entry.index else 0);
            }
            if (command.fence.role == .child_generation_dual)
                try txn.delete(relational_integrity_generation_admission.dual_acknowledged_fence_key);
            try topology.stageCancel(&txn, command.fence);
            if (command.fence.role == .child_generation_dual) {
                const encoded_fence = try command.fence.encode();
                try txn.put(relational_integrity_generation_admission.dual_canceled_fence_key, &encoded_fence);
            }
            if (command.fence.role == .rewrite_source)
                open_graph_after_commit = try graph_retirement_seal.stageCancel(&txn, command.fence);
        },
        .abort_transition => {
            if (command.fence.role == .merge_destination and try topology.current(&txn) != null) {
                const active = (try topology.current(&txn)).?;
                if (active.role != command.fence.role or active.transition_id != command.fence.transition_id or
                    active.peer_group_id != command.fence.peer_group_id or active.attempt > command.fence.attempt) return error.IntegrityTopologyChanged;
                const handoff = relational_integrity_handoff;
                // An ambiguously delivered begin with no imported proof
                // can be canceled immediately. Once copying started,
                // rollback must first restore the range and drain all
                // off-owner integrity records under the same fence.
                const manifest = try missingAsNull(txn.get(handoff.manifest_key));
                if (manifest != null) {
                    const raw_progress = txn.get(handoff.prune_key) catch |err| switch (err) {
                        error.NotFound => return error.IntegrityHandoffIncomplete,
                        else => return err,
                    };
                    var progress = try std.json.parseFromSlice(handoff.PruneProgress, self.alloc, raw_progress, .{});
                    defer progress.deinit();
                    if (!progress.value.complete or !progress.value.fence.eql((try topology.current(&txn)).?)) return error.IntegrityHandoffIncomplete;
                    try txn.delete(handoff.manifest_key);
                    try txn.delete(handoff.progress_key);
                    try txn.delete(handoff.prune_key);
                }
            }
            try topology.stageAbortTransition(&txn, command.fence);
            if (command.fence.role == .rewrite_source)
                open_graph_after_commit = try graph_retirement_seal.stageCancel(&txn, command.fence);
        },
        .transfer => {
            var manager = try self.core.initTxnManager();
            defer manager.deinit();
            try topology.requireDrained(&txn, &manager, command.fence);
            try relational_integrity_handoff.apply(self.alloc, &txn, command.fence, command.transfer.?);
        },
        .stage_parent_retirement => {
            var manager = try self.core.initTxnManager();
            defer manager.deinit();
            const stage = command.parent_retirement.?;
            try relational_integrity_generation_retirement.stagePending(self.alloc, &txn, &manager, command.fence, stage.plan_digest, stage.entries);
        },
        .activate_parent_retirement => {
            const retirement = relational_integrity_generation_retirement;
            const activation = command.parent_activation.?;
            if (command.fence.role != .truncate_parent or std.mem.allEqual(u8, &activation.plan_id, 0) or
                !std.mem.eql(u8, &activation.publication_digest, &retirement.publicationDigest(activation.plan_id, activation.plan_digest)))
                return error.InvalidGenerationRetirement;
            if (try topology.current(&txn)) |actual| {
                if (!actual.eql(command.fence)) return error.IntegrityTopologyChanged;
                if (!try retirement.completedActivation(&txn, command.fence, activation.plan_digest, activation.publication_digest)) {
                    if (try retirement.completedPending(&txn)) |prior| {
                        if (prior.fence.eql(command.fence)) return error.GenerationRetirementChanged;
                    }
                    var manager = try self.core.initTxnManager();
                    defer manager.deinit();
                    try topology.requireDrained(&txn, &manager, command.fence);
                    try retirement.stageVerifiedActivation(self.alloc, &txn, command.fence, activation.plan_id, activation.plan_digest, activation.publication_digest);
                }
            } else {
                const completed = (try topology.completed(&txn)) orelse return error.IntegrityTopologyFenceMissing;
                if (!completed.eql(command.fence) or !try retirement.completedActivation(&txn, command.fence, activation.plan_digest, activation.publication_digest))
                    return error.GenerationRetirementChanged;
            }
        },
        .acknowledge_parent_retirement => {
            const retirement = relational_integrity_generation_retirement;
            const activation = command.parent_activation.?;
            if (command.fence.role != .truncate_parent or std.mem.allEqual(u8, &activation.plan_id, 0) or
                !std.mem.eql(u8, &activation.publication_digest, &retirement.publicationDigest(activation.plan_id, activation.plan_digest)))
                return error.InvalidGenerationRetirement;
            try retirement.stageAcknowledgement(&txn, command.fence, activation.plan_digest, activation.publication_digest);
        },
        .stage_child_generation => {
            if (command.fence.role != .child_generation_parent and command.fence.role != .child_generation_dual) return error.InvalidGenerationAdmission;
            var manager = try self.core.initTxnManager();
            defer manager.deinit();
            try topology.requireDrained(&txn, &manager, command.fence);
            const admission = relational_integrity_generation_admission;
            const transitions = command.child_generations.?;
            try admission.validateTransitions(transitions);
            for (transitions) |transition| try admission.stageTransition(self.alloc, &txn, transition);
            try admission.stageStagedReceipt(&txn, command.fence, transitions, if (receipt_entry) |entry| entry.term else 0, if (receipt_entry) |entry| entry.index else 0);
        },
        .activate_child_generation => {
            if (command.fence.role != .child_generation_parent and command.fence.role != .child_generation_dual) return error.InvalidGenerationAdmission;
            const admission = relational_integrity_generation_admission;
            const transitions = command.child_generations.?;
            try admission.validateTransitions(transitions);
            if (try topology.current(&txn)) |actual| {
                if (!actual.eql(command.fence)) return error.IntegrityTopologyChanged;
                var manager = try self.core.initTxnManager();
                defer manager.deinit();
                try topology.requireDrained(&txn, &manager, command.fence);
                for (transitions) |transition| try admission.activateTransition(self.alloc, &txn, transition);
                try relational_integrity_generation_retirement.stageChildGenerationRetirements(self.alloc, &txn, command.fence, transitions);
                try admission.stageCompletion(&txn, command.fence, transitions, if (receipt_entry) |entry| entry.term else 0, if (receipt_entry) |entry| entry.index else 0);
                // The same fence still protects the old child schema. A
                // dual-role owner releases it only with the published
                // schema/catalog install, never at parent activation.
                if (command.fence.role != .child_generation_dual) try topology.stageRelease(&txn, command.fence);
            } else {
                if (command.fence.role == .child_generation_dual) return error.IntegrityTopologyFenceMissing;
                const completed = (try topology.completed(&txn)) orelse return error.IntegrityTopologyFenceMissing;
                const expected = try admission.completionReceipt(command.fence, transitions);
                const stored = txn.get(admission.activation_receipt_key) catch return error.GenerationAdmissionChanged;
                const applied = try admission.AppliedReceipt.decode(stored);
                if (!completed.eql(command.fence) or !std.mem.eql(u8, &applied.digest, &expected)) return error.GenerationAdmissionChanged;
            }
        },
        .acknowledge_child_generation => {
            if (command.fence.role != .child_generation_parent and command.fence.role != .child_generation_dual) return error.InvalidGenerationAdmission;
            try relational_integrity_generation_admission.stageAcknowledgement(&txn, command.fence, command.child_generations.?, if (receipt_entry) |entry| entry.term else 0, if (receipt_entry) |entry| entry.index else 0);
        },
        .cancel_child_generation_source => {
            if (command.fence.role != .child_generation_source and command.fence.role != .child_generation_dual) return error.InvalidGenerationAdmission;
            // The private owner route must independently read the durable
            // metadata canceling decision before proposing this command.
            // The source has no accepted-generation state to undo; the
            // exact fence receipt consumes its admission epoch forever.
            if (command.fence.role == .child_generation_dual) {
                try relational_integrity_generation_admission.requireDualParentCanceled(&txn, command.fence);
                try txn.delete(relational_integrity_generation_admission.dual_acknowledged_fence_key);
                try txn.delete(relational_integrity_generation_admission.dual_canceled_fence_key);
            }
            try topology.stageCancel(&txn, command.fence);
            try relational_integrity_generation_admission.stageCanceledReceipt(&txn, command.fence, null, if (receipt_entry) |entry| entry.term else 0, if (receipt_entry) |entry| entry.index else 0);
        },
        .install_child_schema => unreachable, // Handled before generic topology transaction.
        .provision_initial_child, .release_initial_child, .cancel_initial_child => unreachable, // Handled before generic topology transaction.
        .prune => try relational_integrity_handoff.prune(self.alloc, &txn, command.fence, self.core.byteRange()),
    };
    try stageApplyMarkers(&txn, ordered_receipt, replication_lsn);
    try self.stageRestoreStagingReplicationOutbox(&txn, native_payload orelse replication_payload);
    try txn.commit();
    if (close_graph_after_commit) self.core.index_manager.setGraphRetirementAdmissionAssumeCatalogLock(false);
    if (open_graph_after_commit) self.core.index_manager.setGraphRetirementAdmissionAssumeCatalogLock(true);
}

/// Fair bounded dispatch on the existing std.Io maintenance scheduler.
/// Never scan every catalog entry or finish an entire index in one turn.
pub fn runRelationalIndexMaintenancePass(self: *DB) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self});
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return false;
    if (self.independent_maintenance.stopping.load(.acquire)) return error.Canceled;
    const started = platform_time.monotonicNs();
    if (started < self.local_execution.relational_index_retry_after_ns.load(.acquire)) return false;
    const sweep = &self.local_execution.relational_index_maintenance_sweep;
    if (!sweep.enter()) return false;
    defer sweep.leave();
    errdefer {
        sweep.failed();
        self.local_execution.relational_index_retry_after_ns.store(platform_time.monotonicNs() +| artifact_repair_metadata_poll_ns, .release);
    }
    const validated = try self.validateConstraintsStep(.{});
    var pinned = self.core.relational_indexes.acquire() orelse {
        sweep.empty(validated);
        return validated;
    };
    defer pinned.deinit();
    const collected = try self.collectRelationalIndexGarbageStep();
    const indexes = pinned.plan.boundIndexes();
    sweep.begin(pinned.head, self.core.schemaNamespaceGeneration(), indexes.len);
    if (indexes.len == 0) {
        sweep.empty(collected or validated);
        return collected or validated;
    }
    for (0..@min(indexes.len, 16)) |_| {
        const next = sweep.next() orelse break;
        const name = indexes[next].name;
        if ((try self.relationalIndexBuildStatus(name)).state == .building) {
            try self.buildRelationalIndexStep(name, .{});
            sweep.finish(true);
            return true;
        }
        if (platform_time.monotonicNs() -| started >= 5 * std.time.ns_per_ms) {
            sweep.finish(collected or validated);
            return false;
        }
    }
    sweep.finish(collected or validated);
    return collected or validated;
}

/// Foreground idle maintenance uses the same bounded, fair pages
/// as the native worker. A clean sweep, rather than one no-work slice,
/// proves that every current index has been considered.
pub fn runRelationalIndexMaintenanceUntilIdle(self: *DB, cancellation: types.CancellationToken, deadline_ns: ?u64) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, cancellation, deadline_ns });
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    const sweep = &self.local_execution.relational_index_maintenance_sweep;
    sweep.request();
    var last_progress = platform_time.monotonicNs();
    while (sweep.isPending()) {
        try ensureSnapshotActive(cancellation);
        const now = platform_time.monotonicNs();
        if (deadline_ns) |deadline| if (now >= deadline) return error.DeadlineExceeded;
        if (self.run_until_idle_no_progress_timeout_ns != 0 and now -| last_progress >= self.run_until_idle_no_progress_timeout_ns) return error.RunUntilIdleNoProgress;
        if (try self.runRelationalIndexMaintenancePass()) {
            last_progress = platform_time.monotonicNs();
        } else if (sweep.isPending()) {
            const io = self.backend_runtime.io() orelse return error.UnsupportedOperation;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
    }
}

/// Admission depends only on these index identities. Do not spend its
/// deadline on unrelated builds, CHECK validation, or index reclamation.
/// Each round gives every required building index one bounded page.
pub fn ensureRelationalIndexesReady(self: *DB, names: []const []const u8, cancellation: types.CancellationToken, deadline_ns: ?u64) !void {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, names, cancellation, deadline_ns });
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
    while (true) {
        try ensureSnapshotActive(cancellation);
        var pending = false;
        for (names) |name| {
            switch ((try self.relationalIndexBuildStatus(name)).state) {
                .ready => continue,
                .failed => return error.RelationalIndexNotReady,
                .building => {},
            }
            if (deadline_ns) |deadline| if (platform_time.monotonicNs() >= deadline) return error.DeadlineExceeded;
            try ensureSnapshotActive(cancellation);
            try self.buildRelationalIndexStep(name, .{});
            pending = true;
        }
        if (!pending) return;
    }
}

pub fn runRelationalColumnMaintenancePass(self: *DB) !usize {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self});
    const started = self.independentMaintenanceNowNs();
    // Artifact repair can keep the shared worker on its active cadence;
    // enforce columnar backoff independently of that worker's sleep.
    if (started < self.relational_column_maintenance.retry_after_ns.load(.acquire)) return 0;
    var completed: usize = 0;
    while (completed < 8) : (completed += 1) {
        const changed = rebuildRelationalColumnsWithPolicy(self, true) catch |err| {
            // Resource pressure backs off to the idle cadence; it must not
            // create a 100 ms retry storm or masquerade as a clean table.
            self.relational_column_maintenance.notePending(true);
            self.relational_column_maintenance.backing_off.store(true, .release);
            self.relational_column_maintenance.retry_after_ns.store(self.independentMaintenanceNowNs() +| artifact_repair_metadata_poll_ns, .release);
            return err;
        };
        if (!changed) break;
        if (self.independentMaintenanceNowNs() -| started >= 50 * std.time.ns_per_ms) return completed + 1;
    }
    return completed;
}

/// Build disposable table-owned column blocks without holding the apply
/// lock. The store's atomic primary-mutation fence governs publication.
pub fn rebuildRelationalColumns(self: *DB) !bool {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{self});
    return rebuildRelationalColumnsWithPolicy(self, false);
}

pub fn rebuildRelationalColumnsWithPolicy(self: *DB, adaptive: bool) !bool {
    if (openModeRequiresReadOnlyBackends(self.open_mode)) return false;
    if (self.independent_maintenance.stopping.load(.acquire)) return error.Canceled;
    var view = self.core.acquireSchemaView() orelse return false;
    defer view.release();
    if (view.storageMode() != .relational) return false;
    if (self.relational_columns_building.swap(true, .acq_rel)) return false;
    defer self.relational_columns_building.store(false, .release);
    const started = self.independentMaintenanceNowNs();
    _ = self.relational_column_maintenance.passes.fetchAdd(1, .monotonic);
    defer self.relational_column_maintenance.last_pass_ns.store(self.independentMaintenanceNowNs() -| started, .monotonic);
    errdefer _ = self.relational_column_maintenance.failures.fetchAdd(1, .monotonic);
    var preparation: RequestPreparationContext = undefined;
    preparation.init(self);
    defer preparation.deinit();
    const force = self.relational_columns_rebuild_requested.swap(false, .acq_rel);
    errdefer if (force) self.relational_columns_rebuild_requested.store(true, .release);
    const changed = relational_columns.rebuild(self, preparation.guard.allocator(), force, adaptive) catch |err| {
        switch (err) {
            error.InvalidColumnSegment, error.InvalidData, error.NotFound => self.relational_columns_rebuild_requested.store(true, .release),
            else => {},
        }
        return preparation.mapError(err);
    };
    self.relational_column_maintenance.notePending(changed or self.relational_column_maintenance.waiting_until_ns.load(.acquire) != 0);
    self.relational_column_maintenance.backing_off.store(false, .release);
    self.relational_column_maintenance.retry_after_ns.store(0, .release);
    return changed;
}

pub fn openRelationalReadSession(self: *DB, alloc: Allocator, from_key: []const u8, to_key: []const u8, opts: types.ScanOptions) !*RelationalReadSession {
    if (comptime storage_engine.enabled) return storage_engine.call(@src().fn_name, .{ self, alloc, from_key, to_key, opts });
    return self.openRelationalReadSessionAtSnapshot(alloc, from_key, to_key, opts, null);
}

pub fn scanRelationalRowsVisit(self: *DB, alloc: Allocator, from_key: []const u8, to_key: []const u8, opts: types.ScanOptions, visitor: types.ScanVisitor) !void {
    const session = try self.openRelationalReadSession(alloc, from_key, to_key, opts);
    defer session.deinit();
    const reader = &session.reader;
    const limit = if (opts.limit == 0) @as(u32, 128) else opts.limit;
    var delivered: usize = 0;
    var remaining_bytes: usize = 16 * 1024 * 1024;
    while (delivered < limit) {
        try session.checkpoint();
        if (opts.cancellation) |cancellation| if (cancellation.isCancelled()) return error.Canceled;
        if (opts.execution_deadline_ns) |deadline| if (platform_time.monotonicNs() >= deadline) return error.DeadlineExceeded;
        if (remaining_bytes == 0) return error.RelationalRowResultTooLarge;
        var page = try reader.nextPage(alloc, self.backend_runtime.io(), .{ .rows = @min(128, limit - delivered), .output_bytes = remaining_bytes });
        defer page.deinit();
        for (page.rows) |row| {
            try session.checkpoint();
            if (opts.cancellation) |cancellation| if (cancellation.isCancelled()) return error.Canceled;
            try visitor.visitEntry(.{
                .id = row.key,
                .hash = row.version,
                .content_hash = if (opts.include_content_hashes) row.semantic_hash else null,
                .relational_schema_version = reader.active.version(),
                .relational_cursor = row.cursor,
                .json_null_fields = row.json_null_fields,
                .document_json = if (opts.include_documents) row.json else null,
            });
            delivered += 1;
        }
        remaining_bytes -= page.output_bytes;
        if (!page.more) break;
        if (delivered < limit) if (self.backend_runtime.io()) |io| try io.sleep(.fromNanoseconds(1), .awake);
    }
}

pub fn hasNativeDocIdConstraints(req: types.SearchRequest) bool {
    return req.filter_doc_ids_positive or
        req.filter_doc_ids.len > 0 or
        req.exclude_doc_ids.len > 0 or
        req.resolved_doc_filter != null or
        req.doc_filter_bindings.len > 0;
}

pub fn resolvedDocFilterForRequestNativeConstraintsAlloc(
    self: *DB,
    alloc: Allocator,
    req: types.SearchRequest,
) !?doc_set.ResolvedDocFilter {
    var filter = doc_set.ResolvedDocFilter{};
    errdefer filter.deinit(alloc);
    var changed = false;

    if (req.resolved_doc_filter) |ptr| {
        const existing: *const doc_set.ResolvedDocFilter = @ptrCast(@alignCast(ptr));
        filter = try self.normalizeResolvedDocFilterDocKeysNoLockAtGenerationAlloc(alloc, existing, req.identity_read_generation);
        changed = true;
    }

    if (req.filter_doc_ids_positive or req.filter_doc_ids.len != 0) {
        var include = if (req.filter_doc_ids.len == 0)
            doc_set.ResolvedDocSet.none
        else
            try self.resolveDocSetForIdsNoLockAtGenerationAlloc(alloc, req.filter_doc_ids, req.identity_read_generation);
        defer include.deinit(alloc);
        if (!(try intersectResolvedFilterIncludeAlloc(alloc, &filter, &include))) {
            filter.deinit(alloc);
            return null;
        }
        changed = true;
    }

    if (req.exclude_doc_ids.len != 0) {
        var exclude = try self.resolveDocSetForIdsNoLockAtGenerationAlloc(alloc, req.exclude_doc_ids, req.identity_read_generation);
        defer exclude.deinit(alloc);
        if (!(try unionResolvedFilterExcludeAlloc(alloc, &filter, &exclude))) {
            filter.deinit(alloc);
            return null;
        }
        changed = true;
    }

    if (!changed) return null;
    return filter;
}

test "relational runtime-only and mixed schema epochs survive portable restore" {
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |upgrade| {
        var source_path_tmp = try TestDirectory.init("db");
        defer source_path_tmp.cleanup();
        const source_path = source_path_tmp.path().ptr;
        var target_path_tmp = try TestDirectory.init("db");
        defer target_path_tmp.cleanup();
        const target_path = target_path_tmp.path().ptr;
        var source = try DB.open(alloc, std.mem.span(source_path), .{ .start_index_workers = false });
        defer source.close();
        const columns = [_]schema_mod.RelationalColumn{.{ .name = "id", .path = "id", .column_type = .string, .required = true }};
        try source.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
        try source.batch(.{ .writes = &.{.{ .key = "old", .value = "{\"id\":\"old\"}" }} });
        if (upgrade) {
            try source.setSchemaJson(alloc,
                \\{"version":2,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
            );
            try source.batch(.{ .writes = &.{.{ .key = "new", .value = "{\"id\":\"new\"}" }} });
        }
        var archive = std.ArrayList(u8).empty;
        defer archive.deinit(alloc);
        try portable_backup.exportPortable(alloc, source.core.store, &archive);
        var target = try DB.open(alloc, std.mem.span(target_path), .{ .start_index_workers = false });
        defer target.close();
        try target.importPortableIntoUnpublishedEmpty(alloc, archive.items, doc_identity.default_namespace);
        const old = (try target.get(alloc, "old")).?;
        defer alloc.free(old);
        try std.testing.expectEqualStrings("{\"id\":\"old\"}", old);
        if (upgrade) {
            const new = (try target.get(alloc, "new")).?;
            defer alloc.free(new);
            try std.testing.expectEqualStrings("{\"id\":\"new\"}", new);
        }
    }
}

test "relational rows snapshot streams flushed companion families without point probes" {
    var checked_allocator = try @import("antfly_platform").allocator.CheckedTestAllocator.init();
    defer checked_allocator.deinit();
    const alloc = checked_allocator.allocator();
    var tmp = try TestDirectory.init("relational-primary-stream");
    defer tmp.cleanup();
    {
        var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false, .primary_backend = .{ .lsm = .{ .flush_threshold = 2 } } });
        defer db.close();
        try db.setSchemaJson(alloc,
            \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer"}},"additionalProperties":false}}}}
        );
        for (0..20) |batch| {
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const a = arena.allocator();
            const writes = try a.alloc(types.BatchWrite, 250);
            for (writes, 0..) |*write, index| {
                const ordinal = batch * 250 + index;
                write.* = .{ .key = try std.fmt.allocPrint(a, "row:{d:0>5}", .{ordinal}), .value = try std.fmt.allocPrint(a, "{{\"n\":{d}}}", .{ordinal}) };
            }
            try db.batch(.{ .timestamp_ns = 100, .writes = writes });
        }
        // A preceding document companion exercises the seek fallback; orphan
        // companions before and after the corpus must not lose continuation.
        const companion = try internal_keys.documentKeyAlloc(alloc, "row:00000");
        defer alloc.free(companion);
        try db.core.store.put(companion, "{}");
        for ([_][]const u8{ "orphan", "z-orphan" }) |id| {
            const key = try internal_keys.ttlKeyAlloc(alloc, id);
            defer alloc.free(key);
            const timestamp = @as([8]u8, @splat(0));
            try db.core.store.put(key, &timestamp);
        }
    }
    // Reopen so the test measures the persisted-run path, not a memtable.
    var db = try DB.open(alloc, tmp.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    var reader = try db.beginRelationalRows(alloc, .{ .fields = &.{"n"} });
    defer reader.deinit();
    var count: usize = 0;
    var pages: usize = 0;
    while (true) {
        var page = try reader.nextTypedPage(alloc, null, .{ .rows = 17, .records = 17 });
        defer page.deinit();
        try std.testing.expectEqual(@as(usize, 0), page.primary_lookups);
        for (page.rows) |row| {
            try std.testing.expectEqual(@as(i64, @intCast(count)), row.typed.?.object.get("n").?.integer);
            count += 1;
        }
        pages += 1;
        if (!page.more) break;
        try std.testing.expect(pages < 512);
    }
    try std.testing.expectEqual(@as(usize, 5000), count);
}

test "relational rows snapshot projects exact composite ranges through writes DDL and garbage collection" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("relational-rows");
    defer path_tmp.cleanup();
    var db = try DB.open(alloc, path_tmp.path(), .{ .start_optional_runtimes = false, .primary_backend = .{ .lsm = .{ .flush_threshold = 2 } } });
    defer db.close();
    try db.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant":{"type":"integer"},"id":{"type":"integer"},"payload":{"type":"string"}},"additionalProperties":false}}}}
    );
    const large = try alloc.alloc(u8, 1024 * 1024);
    defer alloc.free(large);
    @memset(large, 'x');
    const wide = try std.fmt.allocPrint(alloc, "{{\"tenant\":1,\"id\":9007199254740993,\"payload\":\"{s}\"}}", .{large});
    defer alloc.free(wide);
    try db.batch(.{ .timestamp_ns = 100, .writes = &.{
        .{ .key = "a", .value = "{\"tenant\":1,\"id\":9007199254740992}" },
        .{ .key = "b", .value = wide },
        .{ .key = "c", .value = "{\"tenant\":2,\"id\":0}" },
    } });
    try db.setSchemaJson(alloc,
        \\{"version":2,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"tenant_id","keys":[{"column":"tenant"},{"column":"id","direction":"desc"}]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant":{"type":"integer"},"id":{"type":"integer"},"payload":{"type":"string"}},"additionalProperties":false}}}}
    );
    const request = DB.RelationalRows.Request{
        .index = "tenant_id",
        .fields = &.{"id"},
        .lower = .{ .values = &.{.{ .integer = 1 }} },
        .upper = .{ .values = &.{.{ .integer = 1 }} },
    };
    try std.testing.expectError(error.RelationalIndexNotReady, db.beginRelationalRows(alloc, request));
    for (0..256) |_| {
        if ((try db.relationalIndexBuildStatus("tenant_id")).state == .ready) break;
        _ = try db.buildRelationalIndexStep("tenant_id", .{});
    }
    {
        // Native SQL scans hand the storage reader typed conjunctions and let
        // it choose a READY index. The two equality predicates must bind the
        // compound key, including its descending suffix, without widening the
        // read to a primary-table scan or exposing an index name to callers.
        const PlanningAllocator = struct {
            db: *DB,
            blocked: bool = false,
            allocations: usize = 0,

            fn allocate(ptr: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.allocations += 1;
                if (self.db.core.tryLockApplyExclusive()) self.db.core.unlockApplyExclusive() else self.blocked = true;
                return std.testing.allocator.rawAlloc(len, alignment, ret);
            }
            fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
                return false;
            }
            fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
                return null;
            }
            fn free(_: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret: usize) void {
                std.testing.allocator.rawFree(memory, alignment, ret);
            }
            fn allocator(self: *@This()) Allocator {
                return .{ .ptr = self, .vtable = &.{ .alloc = allocate, .resize = resize, .remap = remap, .free = free } };
            }
        };
        var planning: PlanningAllocator = .{ .db = &db };
        var automatic = try db.beginRelationalRows(planning.allocator(), .{
            .auto_index = true,
            .fields = &.{"id"},
            .conditions = &.{
                .{ .column = "tenant", .op = .eq, .value = .{ .integer = 1 } },
                .{ .column = "id", .op = .eq, .value = .{ .integer = 9007199254740993 } },
            },
        });
        defer automatic.deinit();
        try std.testing.expect(planning.allocations != 0);
        try std.testing.expect(!planning.blocked);
        try std.testing.expectEqualStrings("tenant_id", automatic.index.?.name);
        var page = try automatic.nextPage(alloc, null, .{});
        defer page.deinit();
        try std.testing.expectEqual(@as(usize, 1), page.rows.len);
        try std.testing.expectEqualStrings("b", page.rows[0].key);
        try std.testing.expectEqualStrings("{\"id\":9007199254740993}", page.rows[0].json);
    }
    var wrong = request;
    wrong.lower = .{ .values = &.{.{ .number = 1 }} };
    try std.testing.expectError(error.InvalidRelationalIndexBound, db.beginRelationalRows(alloc, wrong));
    {
        var filtered_request = request;
        filtered_request.fields = &.{"tenant"};
        filtered_request.conditions = &.{.{ .column = "id", .op = .gt, .value = .{ .integer = 9007199254740992 } }};
        var filtered = try db.beginRelationalRows(alloc, filtered_request);
        defer filtered.deinit();
        var page = try filtered.nextPage(alloc, null, .{});
        defer page.deinit();
        try std.testing.expectEqual(@as(usize, 1), page.rows.len);
        try std.testing.expectEqualStrings("b", page.rows[0].key);
        try std.testing.expectEqualStrings("{\"tenant\":1}", page.rows[0].json);
    }
    const parent_read = try db.beginTransaction(101);
    try db.writeIntents(parent_read, &.{}, &.{.{ .key = "b", .expected_version = 100 }});
    try std.testing.expectError(error.IntentConflict, db.batch(.{ .deletes = &.{"b"} }));
    try db.commitTransaction(parent_read, 102);
    {
        var soft_reader = try db.beginRelationalRows(alloc, .{ .fields = &.{"payload"} });
        defer soft_reader.deinit();
        var first = try soft_reader.nextTypedPage(alloc, null, .{ .target_bytes = 1, .output_bytes = 16 * 1024 * 1024 });
        defer first.deinit();
        try std.testing.expectEqual(@as(usize, 1), first.rows.len);
        var second = try soft_reader.nextTypedPage(alloc, null, .{ .target_bytes = 1, .output_bytes = 16 * 1024 * 1024 });
        defer second.deinit();
        try std.testing.expectEqual(@as(usize, 1), second.rows.len);
        try std.testing.expectEqualStrings("b", second.rows[0].key);
        try std.testing.expect(second.output_bytes > 1024 * 1024);
    }
    var reader = try db.beginRelationalRows(alloc, request);
    defer reader.deinit();
    try std.testing.expectError(error.RelationalRowResultTooLarge, reader.nextPage(alloc, null, .{ .output_bytes = 1 }));
    var memory: [64 * 1024]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&memory);
    {
        // A 1 MiB unselected column cannot fit in the execution arena. Success
        // proves that the reader does not materialize/parse the complete row.
        var page = try reader.nextPage(fixed.allocator(), null, .{ .rows = 1 });
        defer page.deinit();
        try std.testing.expectEqual(@as(usize, 1), page.rows.len);
        try std.testing.expectEqualStrings("b", page.rows[0].key);
        try std.testing.expectEqualStrings("{\"id\":9007199254740993}", page.rows[0].json);
        try std.testing.expectEqual(@as(u64, 100), page.rows[0].version);
        try std.testing.expect(page.more);
    }
    try db.batch(.{ .timestamp_ns = 200, .writes = &.{.{ .key = "b", .value = "{\"tenant\":1,\"id\":1}" }}, .deletes = &.{"a"} });
    try db.setSchemaJson(alloc,
        \\{"version":3,"storage_mode":"relational","default_type":"row","relational_indexes":[],"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant":{"type":"integer"},"id":{"type":"integer"},"payload":{"type":"string"}},"additionalProperties":false}}}}
    );
    for (0..256) |_| {
        if (!try db.collectRelationalIndexGarbageStep()) break;
    }
    {
        var page = try reader.nextPage(alloc, null, .{ .rows = 1 });
        defer page.deinit();
        try std.testing.expectEqual(@as(usize, 1), page.rows.len);
        try std.testing.expectEqualStrings("a", page.rows[0].key);
        try std.testing.expectEqualStrings("{\"id\":9007199254740992}", page.rows[0].json);
    }
    {
        var page = try reader.nextPage(alloc, null, .{});
        defer page.deinit();
        try std.testing.expectEqual(@as(usize, 0), page.rows.len);
        try std.testing.expect(!page.more);
    }
    try std.testing.expectError(error.IndexNotFound, db.beginRelationalRows(alloc, request));
    var primary = try db.beginRelationalRows(alloc, .{
        .fields = &.{"id"},
        .conditions = &.{.{ .column = "id", .op = .gt, .value = .{ .integer = 0 } }},
    });
    defer primary.deinit();
    var found: usize = 0;
    for (0..256) |_| {
        var page = try primary.nextPage(alloc, null, .{ .records = 2 });
        defer page.deinit();
        found += page.rows.len;
        if (!page.more) break;
    }
    try std.testing.expectEqual(@as(usize, 1), found);
    {
        // Exercise the routed public scan adapter, including exact integer
        // operands and authorization before projection.
        const query = "{\"fields\":[\"id\"],\"conditions\":[{\"column\":\"id\",\"op\":\"gt\",\"value\":0}],\"schema_version\":3}";
        var result = try db.scan(alloc, "", "", .{
            .relational_query_json = query,
            .include_documents = true,
            .limit = 16,
        });
        defer result.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 1), result.hashes.len);
        try std.testing.expectEqual(@as(?u32, 3), result.hashes[0].relational_schema_version);
        var excluded = try db.scan(alloc, "b", "", .{
            .relational_query_json = query,
            .include_documents = true,
            .inclusive_from = false,
            .limit = 16,
        });
        defer excluded.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 0), excluded.hashes.len);
        // Session owns compiled query/filter bytes and keeps the original
        // snapshot even after caller buffers and live row values change.
        const query_copy = try alloc.dupe(u8, query);
        defer alloc.free(query_copy);
        const filter_copy = try alloc.dupe(u8, "{\"doc_id\":[\"b\"]}");
        defer alloc.free(filter_copy);
        var cancel = std.atomic.Value(bool).init(false);
        const session = try db.openRelationalReadSession(alloc, "", "", .{
            .relational_query_json = query_copy,
            .filter_query_json = filter_copy,
            .limit = 16,
            .cancellation = types.CancellationToken.fromAtomic(&cancel),
        });
        defer session.deinit();
        @memset(query_copy, 'x');
        @memset(filter_copy, 'x');
        try db.batch(.{ .timestamp_ns = 300, .writes = &.{.{ .key = "b", .value = "{\"tenant\":1,\"id\":2}" }} });
        var typed = try session.nextTypedPage(alloc, null, .{});
        defer typed.deinit();
        try std.testing.expectEqual(@as(usize, 1), typed.rows.len);
        try std.testing.expectEqualStrings("b", typed.rows[0].key);
        try std.testing.expectEqual(@as(i64, 1), typed.rows[0].typed.?.object.get("id").?.integer);
        cancel.store(true, .release);
        try std.testing.expectError(error.Canceled, session.nextTypedPage(alloc, null, .{}));
        try std.testing.expectError(error.DeadlineExceeded, db.openRelationalReadSession(alloc, "", "", .{
            .relational_query_json = query,
            .execution_deadline_ns = 0,
        }));
    }
}

test "relational constraint activation fences prepared writes and resumes CHECK coverage" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    const initial =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const checked =
        \\{"version":2,"storage_mode":"relational","default_type":"row","checks":[{"name":"positive","column":"id","op":"gt","value":"9007199254740992"}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const options = OpenOptions{ .start_optional_runtimes = false, .primary_backend = .{ .lsm = .{ .flush_threshold = 2 } } };
    {
        var db = try DB.open(alloc, std.mem.span(path), options);
        defer db.close();
        try db.setSchemaJson(alloc, initial);
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"id\":0}" }} });
        const txn = try db.beginTransaction(100);
        try db.writeIntents(txn, &.{.{ .key = "b", .value = "{\"id\":9007199254740993}" }}, &.{});
        try std.testing.expectError(error.SchemaInUse, db.setSchemaJson(alloc, checked));
        try db.commitTransaction(txn, 200);
        try db.setSchemaJson(alloc, checked);
        try std.testing.expectEqual(.validating, (try db.constraintValidationStatus()).state);
        try std.testing.expectError(error.RelationalCheckViolation, db.batch(.{ .writes = &.{.{ .key = "bad", .value = "{\"id\":9007199254740992}" }} }));
        try std.testing.expectEqual(@as(?[]u8, null), try db.get(alloc, "bad"));
        // NULL/absent evaluates UNKNOWN and passes CHECK, not WHERE.
        try db.batch(.{ .writes = &.{.{ .key = "c", .value = "{}" }} });
        var failed = (try relational_constraint_jobs.Page.prepare(alloc, db.backend_runtime.io(), db.core, .{ .time_ns = std.time.ns_per_s })).?;
        defer failed.deinit();
        try std.testing.expectEqual(.invalid, failed.next.state);
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"id\":9007199254740993}" }} });
        db.core.lockApply();
        defer db.core.unlockApply();
        try std.testing.expectError(error.PreparedGenerationChanged, failed.commit(db.core));
    }
    {
        var db = try DB.open(alloc, std.mem.span(path), options);
        defer db.close();
        try std.testing.expect(try db.validateConstraintsStep(.{ .records = 1 }));
        try std.testing.expectError(error.RelationalCheckViolation, db.batch(.{ .writes = &.{.{ .key = "bad", .value = "{\"id\":0}" }} }));
        try std.testing.expectError(error.ReservedRelationalIndexMetadataKey, db.batch(.{ .deletes = &.{relational_constraint_jobs.progress_key} }));
    }
    {
        var db = try DB.open(alloc, std.mem.span(path), options);
        defer db.close();
        for (0..128) |_| {
            if ((try db.constraintValidationStatus()).state == .enforced) break;
            _ = try db.runRelationalIndexMaintenancePass();
        } else return error.ValidationDidNotConverge;
        try std.testing.expectEqual(@as(u64, 3), (try db.constraintValidationStatus()).rows_scanned);
        try std.testing.expect(!try db.retryConstraintValidation(2));
        try std.testing.expectError(error.PreparedGenerationChanged, db.retryConstraintValidation(1));
    }
}

test "relational constraint failed validation is durable and retry is epoch bound" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    const options = OpenOptions{ .start_optional_runtimes = false, .primary_backend = .{ .lsm = .{} } };
    {
        var db = try DB.open(alloc, std.mem.span(path), options);
        defer db.close();
        try db.setSchemaJson(alloc,
            \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
        );
        try db.batch(.{ .writes = &.{.{ .key = "bad", .value = "{\"id\":-1}" }} });
        try db.setSchemaJson(alloc,
            \\{"version":2,"storage_mode":"relational","default_type":"row","checks":[{"name":"positive","column":"id","op":"gt","value":0}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
        );
        for (0..128) |_| {
            if ((try db.constraintValidationStatus()).state == .invalid) break;
            _ = try db.validateConstraintsStep(.{});
        } else return error.ValidationDidNotConverge;
        try std.testing.expectEqual(@as(?u16, 0), (try db.constraintValidationStatus()).failed_check);
    }
    {
        var db = try DB.open(alloc, std.mem.span(path), options);
        defer db.close();
        try std.testing.expectEqual(.invalid, (try db.constraintValidationStatus()).state);
        try std.testing.expect(!try db.validateConstraintsStep(.{}));
        try std.testing.expectError(error.RelationalCheckViolation, db.batch(.{ .writes = &.{.{ .key = "new", .value = "{\"id\":-2}" }} }));
        const txn = try db.beginTransaction(100);
        defer db.abortTransaction(txn, 300) catch {};
        try std.testing.expectError(error.RelationalCheckViolation, db.writeIntents(txn, &.{.{ .key = "new", .value = "{\"id\":-2}" }}, &.{}));
        try db.batch(.{ .writes = &.{.{ .key = "bad", .value = "{\"id\":1}" }} });
        try std.testing.expect(try db.retryConstraintValidation(2));
        try std.testing.expect(!try db.retryConstraintValidation(2));
        try std.testing.expectError(error.PreparedGenerationChanged, db.retryConstraintValidation(1));
        for (0..128) |_| {
            if ((try db.constraintValidationStatus()).state == .enforced) break;
            _ = try db.validateConstraintsStep(.{});
        } else return error.ValidationDidNotConverge;
        try std.testing.expectEqual(@as(?u16, null), (try db.constraintValidationStatus()).failed_check);
    }
}

test "relational index schema declarations publish atomically and maintenance backfills" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = .{ .lsm = .{ .flush_threshold = 2 } } });
    defer db.close();
    try db.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant":{"type":"integer"},"id":{"type":"integer"}},"additionalProperties":false}}}}
    );
    const row = "{\"tenant\":1,\"id\":9007199254740993}";
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = row }} });
    const indexed =
        \\{"version":2,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"tenant_id","keys":[{"column":"tenant"},{"column":"id","direction":"desc"}]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant":{"type":"integer"},"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const txn = try db.beginTransaction(100);
    try db.writeIntents(txn, &.{.{ .key = "b", .value = row }}, &.{});
    try std.testing.expectError(error.SchemaInUse, db.setSchemaJson(alloc, indexed));
    try std.testing.expectError(error.IndexNotFound, db.relationalIndexBuildStatus("tenant_id"));
    try db.commitTransaction(txn, 200);
    try db.setSchemaJson(alloc, indexed);
    try std.testing.expectEqual(.building, (try db.relationalIndexBuildStatus("tenant_id")).state);
    for (0..128) |_| {
        if ((try db.relationalIndexBuildStatus("tenant_id")).state == .ready) break;
        _ = try db.runRelationalIndexMaintenancePass();
    } else return error.BuildDidNotConverge;
    try expectTestRelationalIndexRow(&db, "a", row, true);
    try expectTestRelationalIndexRow(&db, "b", row, true);
    const ready = try db.relationalIndexBuildStatus("tenant_id");
    try std.testing.expectError(error.ReservedRelationalIndexMetadataKey, db.batch(.{
        .writes = &.{.{ .key = relational_index_catalog.head_key, .value = "forged" }},
    }));
    try std.testing.expectError(error.ReservedRelationalIndexMetadataKey, db.batch(.{
        .deletes = &.{relational_index_jobs.progress_prefix},
    }));
    {
        var pinned = db.core.relational_indexes.acquire().?;
        defer pinned.deinit();
        var reverse = std.ArrayList(u8).empty;
        defer reverse.deinit(alloc);
        try relational_index_records.appendReverseKey(alloc, &reverse, pinned.plan.boundIndexes()[0].id(), "a");
        const original = (try db.core.getStoreValue(alloc, reverse.items)).?;
        defer alloc.free(original);
        try db.core.store.putBatch(&.{}, &.{reverse.items});
        try std.testing.expectError(error.MissingRelationalIndexReverse, db.batch(.{ .writes = &.{.{ .key = "a", .value = row }} }));
        try db.core.store.putBatch(&.{.{ .key = reverse.items, .value = original }}, &.{});
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = row }} });
    }
    try db.setSchemaJson(alloc, indexed);
    try std.testing.expectEqual(ready, try db.relationalIndexBuildStatus("tenant_id"));
    db.close();
    db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = .{ .lsm = .{ .flush_threshold = 2 } } });
    try std.testing.expectEqual(ready, try db.relationalIndexBuildStatus("tenant_id"));
    {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        var writes: [600]types.BatchWrite = undefined;
        for (&writes, 0..) |*write, i| write.* = .{
            .key = try std.fmt.allocPrint(arena.allocator(), "gc-{d:0>4}", .{i}),
            .value = row,
        };
        try db.batch(.{ .writes = &writes });
    }
    try db.setSchemaJson(alloc,
        \\{"version":3,"storage_mode":"relational","default_type":"row","relational_indexes":[],"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant":{"type":"integer"},"id":{"type":"integer"}},"additionalProperties":false}}}}
    );
    try std.testing.expectError(error.IndexNotFound, db.relationalIndexBuildStatus("tenant_id"));
    {
        // A retired value need not be readable to be safely deleted. The
        // generation ID and framed owner key are the cleanup authority.
        var reverse = std.ArrayList(u8).empty;
        defer reverse.deinit(alloc);
        try relational_index_records.appendReverseKey(alloc, &reverse, .{ .generation = ready.generation, .slot = ready.slot }, "a");
        try db.core.store.putBatch(&.{.{ .key = reverse.items, .value = "corrupt retired value" }}, &.{});
        var first = (try relational_index_gc.Page.prepare(alloc, db.backend_runtime.io(), db.core)).?;
        defer first.deinit();
        var rival = (try relational_index_gc.Page.prepare(alloc, db.backend_runtime.io(), db.core)).?;
        defer rival.deinit();
        try std.testing.expect(first.next != null);
        try std.testing.expect(first.deletes.len <= 512);
        db.core.lockApply();
        defer db.core.unlockApply();
        try first.commit(db.core);
        try std.testing.expectError(error.PreparedGenerationChanged, rival.commit(db.core));
    }
    db.close();
    db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = .{ .lsm = .{ .flush_threshold = 2 } } });
    // The time budget may yield after one cold record. Do not assume a
    // minimum records-per-time-slice throughput in this convergence test.
    for (0..1024) |_| {
        if (!try db.collectRelationalIndexGarbageStep()) break;
    } else return error.GcDidNotConverge;
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    var cursor = try read.openCursor();
    defer cursor.close();
    var blob_count: usize = 0;
    var entry = try cursor.seekAtOrAfter("");
    while (entry) |item| : (entry = try cursor.next()) {
        try std.testing.expect(!relational_index_records.isForwardKey(item.key));
        try std.testing.expect(!internal_keys.isRelationalIndexReverseKey(item.key));
        try std.testing.expect(!std.mem.startsWith(u8, item.key, relational_index_gc.prefix));
        try std.testing.expect(!std.mem.startsWith(u8, item.key, relational_index_jobs.progress_prefix));
        if (std.mem.startsWith(u8, item.key, relational_index_catalog.blob_prefix)) blob_count += 1;
    }
    try std.testing.expectEqual(@as(usize, 1), blob_count);
}

test "relational index build pages fence races resume after reopen and prove readiness" {
    const alloc = std.testing.allocator;
    const native = relational_index_mod;
    for ([_]PrimaryBackend{.{ .lsm = .{ .flush_threshold = 2 } }}) |backend| {
        var path_tmp = try TestDirectory.init("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        defer db.close();
        const columns = [_]schema_mod.RelationalColumn{
            .{ .name = "tenant", .path = "tenant", .column_type = .integer },
            .{ .name = "id", .path = "id", .column_type = .integer },
        };
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
        const before = "{\"tenant\":1,\"id\":9007199254740993}";
        const after = "{\"tenant\":2,\"id\":9007199254740994}";
        try db.batch(.{ .writes = &.{ .{ .key = "a", .value = before }, .{ .key = "b", .value = before }, .{ .key = "c", .value = before } } });
        {
            var view = db.core.acquireSchemaView().?;
            defer view.release();
            var definition = try db.core.relational_indexes.prepare(view, &.{.{
                .name = "tenant_id",
                .owner_kind = .table,
                .owner_name = native.relational_table_index_owner_name,
                .access_method = .ordered_tuple,
                .keys = &.{ .{ .column = "tenant" }, .{ .column = "id", .direction = .desc } },
            }});
            defer definition.deinit();
            db.core.lockApply();
            defer db.core.unlockApply();
            _ = try db.core.relational_indexes.commit(&definition, &.{});
        }
        try std.testing.expectEqual(.building, (try db.relationalIndexBuildStatus("tenant_id")).state);
        try std.testing.expectError(error.InvalidRelationalIndexBudget, db.buildRelationalIndexStep("tenant_id", .{ .records = 0 }));
        {
            const primary = try internal_keys.relationalRowKeyAlloc(alloc, "a");
            defer alloc.free(primary);
            const original = (try db.core.getStoreValue(alloc, primary)).?;
            defer alloc.free(original);
            try db.core.store.putBatch(&.{.{ .key = primary, .value = "corrupt" }}, &.{});
            {
                var failure = (try relational_index_jobs.Page.prepare(alloc, db.backend_runtime.io(), db.core, "tenant_id", .{ .time_ns = std.time.ns_per_s })).?;
                defer failure.deinit();
                try std.testing.expectEqual(.failed, failure.next.state);
                try db.core.store.putBatch(&.{.{ .key = primary, .value = original }}, &.{});
                db.core.lockApply();
                defer db.core.unlockApply();
                try std.testing.expectError(error.PreparedGenerationChanged, failure.commit(db.core));
            }
            try db.core.store.putBatch(&.{.{ .key = primary, .value = "corrupt" }}, &.{});
            try db.buildRelationalIndexStep("tenant_id", .{ .time_ns = std.time.ns_per_s });
            const failed = try db.relationalIndexBuildStatus("tenant_id");
            try std.testing.expectEqual(.failed, failed.state);
            try std.testing.expectEqual(.invalid_row, failed.failure);
            db.close();
            db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
            try std.testing.expectEqual(failed, try db.relationalIndexBuildStatus("tenant_id"));
            try db.core.store.putBatch(&.{.{ .key = primary, .value = original }}, &.{});
            try std.testing.expectError(error.PreparedGenerationChanged, db.retryRelationalIndexBuild("tenant_id", failed.generation + 1));
            try std.testing.expect(try db.retryRelationalIndexBuild("tenant_id", failed.generation));
            try std.testing.expect(!try db.retryRelationalIndexBuild("tenant_id", failed.generation));
            try std.testing.expectEqual(.building, (try db.relationalIndexBuildStatus("tenant_id")).state);
        }
        // Two builders prepared from the same durable cursor. A later writer
        // updates one source row and deletes another before either commits.
        {
            var first = (try relational_index_jobs.Page.prepare(alloc, db.backend_runtime.io(), db.core, "tenant_id", .{ .time_ns = std.time.ns_per_s })).?;
            defer first.deinit();
            var rival = (try relational_index_jobs.Page.prepare(alloc, db.backend_runtime.io(), db.core, "tenant_id", .{ .time_ns = std.time.ns_per_s })).?;
            defer rival.deinit();
            try db.batch(.{ .writes = &.{.{ .key = "a", .value = after }}, .deletes = &.{"b"} });
            db.core.lockApply();
            defer db.core.unlockApply();
            try first.commit(db.core);
            try std.testing.expectError(error.PreparedGenerationChanged, rival.commit(db.core));
            try std.testing.expectError(error.RelationalIndexPageConsumed, first.commit(db.core));
        }
        try expectTestRelationalIndexRow(&db, "a", before, false);
        try expectTestRelationalIndexRow(&db, "a", after, true);
        try expectTestRelationalIndexRow(&db, "b", before, false);
        try expectTestRelationalIndexRow(&db, "c", before, true);
        // Exhausting primary rows only starts verification. Readiness requires
        // bounded forward and reverse passes over the derived index as well.
        try std.testing.expectEqual(.building, (try db.relationalIndexBuildStatus("tenant_id")).state);
        for (0..128) |_| {
            if ((try db.relationalIndexBuildStatus("tenant_id")).state == .ready) break;
            try db.buildRelationalIndexStep("tenant_id", .{ .records = 1, .time_ns = std.time.ns_per_s });
        }
        try std.testing.expectEqual(.ready, (try db.relationalIndexBuildStatus("tenant_id")).state);
        // A compatible schema epoch preserves coverage; changing the index
        // definition creates a new generation that must rebuild in tiny pages.
        try db.setSchema(.{ .version = 2, .storage_mode = .relational, .relational_columns = &columns });
        try std.testing.expectEqual(.ready, (try db.relationalIndexBuildStatus("tenant_id")).state);
        {
            var view = db.core.acquireSchemaView().?;
            defer view.release();
            var definition = try db.core.relational_indexes.prepare(view, &.{.{
                .name = "tenant_id",
                .owner_kind = .table,
                .owner_name = native.relational_table_index_owner_name,
                .access_method = .ordered_tuple,
                .keys = &.{ .{ .column = "tenant" }, .{ .column = "id", .direction = .asc } },
            }});
            defer definition.deinit();
            db.core.lockApply();
            defer db.core.unlockApply();
            _ = try db.core.relational_indexes.commit(&definition, &.{});
        }
        try db.buildRelationalIndexStep("tenant_id", .{ .records = 1 });
        const saved = try db.relationalIndexBuildStatus("tenant_id");
        try std.testing.expectEqual(.building, saved.state);
        db.close();
        db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        try std.testing.expectEqual(saved, try db.relationalIndexBuildStatus("tenant_id"));
        for (0..128) |_| {
            if ((try db.relationalIndexBuildStatus("tenant_id")).state == .ready) break;
            try db.buildRelationalIndexStep("tenant_id", .{ .records = 1 });
        } else return error.BuildDidNotConverge;
        try expectTestRelationalIndexRow(&db, "a", after, true);
        try expectTestRelationalIndexRow(&db, "c", before, true);
        const ready = try db.relationalIndexBuildStatus("tenant_id");
        try std.testing.expectEqual(@as(u64, 2), ready.rows_scanned);
        try db.buildRelationalIndexStep("tenant_id", .{});
        try std.testing.expectEqual(ready, try db.relationalIndexBuildStatus("tenant_id"));
        // Ownership transfer invalidates readiness without reusing its cursor.
        var split_tmp = try TestDirectory.init("db");
        defer split_tmp.cleanup();
        const split_path = split_tmp.path().ptr;
        try db.split(db.getRange(), "b", "", std.mem.span(split_path), true);
        try db.finalizeSplit(.{ .start = "", .end = "b" });
        try std.testing.expectEqual(.building, (try db.relationalIndexBuildStatus("tenant_id")).state);
        for (0..128) |_| {
            if ((try db.relationalIndexBuildStatus("tenant_id")).state == .ready) break;
            try db.buildRelationalIndexStep("tenant_id", .{ .records = 1 });
        } else return error.BuildDidNotConverge;
        try std.testing.expectEqual(@as(u64, 1), (try db.relationalIndexBuildStatus("tenant_id")).rows_scanned);
    }
}

test "relational index mutations follow primary batches transactions deletes and reopen" {
    const alloc = std.testing.allocator;
    const native = relational_index_mod;
    for ([_]PrimaryBackend{.{ .lsm = .{ .flush_threshold = 2 } }}) |backend| {
        var path_tmp = try TestDirectory.init("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        defer db.close();
        const columns = [_]schema_mod.RelationalColumn{
            .{ .name = "tenant", .path = "tenant", .column_type = .integer },
            .{ .name = "id", .path = "id", .column_type = .integer },
        };
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
        var view = db.core.acquireSchemaView().?;
        defer view.release();
        var definition = try db.core.relational_indexes.prepare(view, &.{.{
            .name = "tenant_id",
            .owner_kind = .table,
            .owner_name = native.relational_table_index_owner_name,
            .access_method = .ordered_tuple,
            .keys = &.{ .{ .column = "tenant" }, .{ .column = "id", .direction = .desc } },
        }});
        defer definition.deinit();
        {
            db.core.lockApply();
            defer db.core.unlockApply();
            _ = try db.core.relational_indexes.commit(&definition, &.{});
        }
        const first = "{\"tenant\":1,\"id\":9007199254740993}";
        const second = "{\"tenant\":2,\"id\":9007199254740994}";
        const third = "{\"tenant\":3,\"id\":7}";
        try db.batch(.{ .writes = &.{.{ .key = "row\x00:1", .value = first }} });
        try expectTestRelationalIndexRow(&db, "row\x00:1", first, true);
        try db.batch(.{ .writes = &.{ .{ .key = "row\x00:1", .value = second }, .{ .key = "row\x00:1", .value = third } }, .deletes = &.{"row\x00:1"} });
        try expectTestRelationalIndexRow(&db, "row\x00:1", first, false);
        try expectTestRelationalIndexRow(&db, "row\x00:1", second, false);
        try expectTestRelationalIndexRow(&db, "row\x00:1", third, false);
        try std.testing.expect((try db.get(alloc, "row\x00:1")) == null);
        try db.batch(.{ .writes = &.{ .{ .key = "row\x00:1", .value = second }, .{ .key = "row\x00:1", .value = third } } });
        try expectTestRelationalIndexRow(&db, "row\x00:1", second, false);
        try expectTestRelationalIndexRow(&db, "row\x00:1", third, true);
        const txn = try db.beginTransaction(100);
        try db.writeIntents(txn, &.{.{ .key = "row\x00:1", .value = second }}, &.{});
        try std.testing.expectError(error.SchemaInUse, db.setSchema(.{ .version = 2, .storage_mode = .relational, .relational_columns = &columns }));
        try db.commitTransaction(txn, 200);
        try db.commitTransaction(txn, 200); // Idempotent replay cannot leave old keys.
        try expectTestRelationalIndexRow(&db, "row\x00:1", third, false);
        try expectTestRelationalIndexRow(&db, "row\x00:1", second, true);
        {
            var archive = std.ArrayList(u8).empty;
            defer archive.deinit(alloc);
            try portable_backup.exportPortable(alloc, db.core.store, &archive);
            for ([_]bool{ false, true }) |unpublished| {
                var restore_tmp = try TestDirectory.init("db");
                defer restore_tmp.cleanup();
                const restore_path = restore_tmp.path().ptr;
                var restored = try DB.open(alloc, std.mem.span(restore_path), .{ .start_optional_runtimes = false, .primary_backend = backend });
                defer restored.close();
                if (unpublished)
                    try restored.importPortableIntoUnpublishedEmpty(alloc, archive.items, doc_identity.default_namespace)
                else
                    try restored.importPortableIntoEmpty(alloc, archive.items, doc_identity.default_namespace);
                try expectTestRelationalIndexRow(&restored, "row\x00:1", second, true);
                try expectTestRelationalIndexRow(&restored, "row\x00:1", first, false);
                try expectTestRelationalIndexRow(&restored, "row\x00:1", third, false);
            }
        }
        db.close();
        db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        try expectTestRelationalIndexRow(&db, "row\x00:1", second, true);
        try db.batch(.{ .deletes = &.{"row\x00:1"} });
        try expectTestRelationalIndexRow(&db, "row\x00:1", second, false);
        try db.batch(.{ .deletes = &.{"row\x00:1"} });
        try db.batch(.{ .writes = &.{ .{ .key = "a\x00", .value = first }, .{ .key = "z\x00", .value = third } } });
        {
            const page = try db.mergeDocumentsPage(alloc, db.getRange(), "a\x00");
            defer {
                for (page) |item| {
                    alloc.free(item.key);
                    alloc.free(item.value);
                }
                alloc.free(page);
            }
            try std.testing.expectEqual(@as(usize, 1), page.len);
            try std.testing.expectEqualStrings("z\x00", page[0].key);
            try std.testing.expectEqualStrings(third, page[0].value);
        }
        var split_tmp = try TestDirectory.init("db");
        defer split_tmp.cleanup();
        const split_path = split_tmp.path().ptr;
        try db.split(db.getRange(), "m", "", std.mem.span(split_path), true);
        {
            var right = try DB.open(alloc, std.mem.span(split_path), .{ .start_optional_runtimes = false, .primary_backend = backend });
            defer right.close();
            try expectTestRelationalIndexRow(&right, "z\x00", third, true);
            try expectTestRelationalIndexRow(&right, "a\x00", first, false);
        }
        try db.finalizeSplit(.{ .start = "", .end = "m" });
        try expectTestRelationalIndexRow(&db, "a\x00", first, true);
        try expectTestRelationalIndexRow(&db, "z\x00", third, false);
        // Expiration uses its own stable runtime context. Cover both the
        // retired schema generation and newly written current-generation keys.
        const expired_at = currentTimeNs() - 3 * std.time.ns_per_s;
        try db.batch(.{ .writes = &.{.{ .key = "a\x00", .value = first }}, .timestamp_ns = expired_at });
        try db.setSchema(.{ .version = 2, .storage_mode = .relational, .relational_columns = &columns, .ttl_duration_ns = std.time.ns_per_s });
        try db.batch(.{ .writes = &.{.{ .key = "b\x00", .value = second }}, .timestamp_ns = expired_at });
        try initStoppedTtlRuntimeForTest(&db, .{ .enabled = false, .grace_period_ns = 0 });
        var expired_a = [_]u8{ 'a', 0 };
        var expired_b = [_]u8{ 'b', 0 };
        try std.testing.expectEqual(@as(u32, 2), try deleteExpiredDocumentsFromCandidates(db.ttl_cleanup_context.?, &.{
            .{ .key = &expired_a, .timestamp_ns = expired_at },
            .{ .key = &expired_b, .timestamp_ns = expired_at },
        }));
        const remaining_index_keys = try db.core.store.scanPrefix(alloc, relational_index_records.forward_namespace);
        defer docstore_mod.DocStore.freeResults(alloc, remaining_index_keys);
        try std.testing.expectEqual(@as(usize, 0), remaining_index_keys.len);
        try std.testing.expectEqual(@as(u64, 0), db.core.table_catalog.row_count);
        const persisted_catalog = (try table_catalog_mod.load(alloc, db.core.store)).?;
        try std.testing.expectEqual(@as(u64, 0), persisted_catalog.row_count);
        try db.batch(.{ .writes = &.{.{ .key = "b\x00", .value = second }} });
        try std.testing.expectEqual(@as(u64, 1), db.core.table_catalog.row_count);
        try db.batch(.{ .deletes = &.{"b\x00"} });
        try std.testing.expectEqual(@as(u64, 0), db.core.table_catalog.row_count);
    }
}

test "relational index restore projects historical rows without requiring unused history columns" {
    const alloc = std.testing.allocator;
    var source_tmp = try TestDirectory.init("db");
    defer source_tmp.cleanup();
    const source_path = source_tmp.path().ptr;
    var source = try DB.open(alloc, std.mem.span(source_path), .{ .start_optional_runtimes = false });
    defer source.close();
    try source.setSchema(.{ .version = 0, .storage_mode = .relational, .relational_columns = &.{.{ .name = "unused", .path = "unused", .column_type = .string }} });
    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "tenant", .path = "tenant", .column_type = .integer },
        .{ .name = "id", .path = "id", .column_type = .integer },
    };
    try source.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    const logical = "{\"tenant\":9,\"id\":9007199254740993}";
    try source.batch(.{ .writes = &.{.{ .key = "old", .value = logical }} });
    const reordered = [_]schema_mod.RelationalColumn{ columns[1], columns[0] };
    try source.setSchema(.{ .version = 2, .storage_mode = .relational, .relational_columns = &reordered });
    var view = source.core.acquireSchemaView().?;
    defer view.release();
    var candidate = try source.core.relational_indexes.prepare(view, &.{.{
        .name = "tenant_id",
        .owner_kind = .table,
        .owner_name = relational_index_mod.relational_table_index_owner_name,
        .access_method = .ordered_tuple,
        .keys = &.{ .{ .column = "tenant" }, .{ .column = "id", .direction = .desc } },
    }});
    defer candidate.deinit();
    {
        source.core.lockApply();
        defer source.core.unlockApply();
        _ = try source.core.relational_indexes.commit(&candidate, &.{});
    }
    var archive = std.ArrayList(u8).empty;
    defer archive.deinit(alloc);
    try portable_backup.exportPortable(alloc, source.core.store, &archive);
    for ([_]bool{ false, true }) |unpublished| {
        var target_tmp = try TestDirectory.init("db");
        defer target_tmp.cleanup();
        const target_path = target_tmp.path().ptr;
        var target = try DB.open(alloc, std.mem.span(target_path), .{ .start_optional_runtimes = false, .primary_backend = .{ .lsm = .{} } });
        defer target.close();
        if (unpublished)
            try target.importPortableIntoUnpublishedEmpty(alloc, archive.items, doc_identity.default_namespace)
        else
            try target.importPortableIntoEmpty(alloc, archive.items, doc_identity.default_namespace);
        try expectTestRelationalIndexRow(&target, "old", logical, true);
    }
}

test "relational index catalog core publishes schema bound plans and recovers on reopen" {
    const alloc = std.testing.allocator;
    const native_indexes = relational_index_mod;
    const catalog = relational_index_catalog;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    const initial_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
    ;
    const next_json =
        \\{"version":2,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"extra":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
    ;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
        defer db.close();
        try db.setSchemaJson(alloc, initial_json);
        try std.testing.expect(db.core.relational_indexes.acquire() == null);
        var view = db.core.acquireSchemaView().?;
        defer view.release();
        var candidate = try db.core.relational_indexes.prepare(view, &.{.{
            .name = "by_id",
            .owner_kind = .table,
            .owner_name = native_indexes.relational_table_index_owner_name,
            .access_method = .ordered_tuple,
            .keys = &.{.{ .column = "id" }},
        }});
        defer candidate.deinit();
        {
            db.core.lockApply();
            defer db.core.unlockApply();
            try std.testing.expect(try db.core.relational_indexes.commit(&candidate, &.{}));
        }
        var old = db.core.relational_indexes.acquire().?;
        defer old.deinit();
        try db.setSchemaJson(alloc, next_json);
        var current = db.core.relational_indexes.acquire().?;
        defer current.deinit();
        var schema_view = db.core.acquireSchemaView().?;
        defer schema_view.release();
        try std.testing.expectEqual(schema_view.epoch, current.plan.schemaView().epoch);
        try std.testing.expectEqual(@as(u32, 2), current.head.schema_version);
        try std.testing.expect(!db.core.relational_indexes.isCurrent(old));
        try std.testing.expectEqual(@as(u32, 1), old.plan.schemaView().version());
        try std.testing.expectError(error.RelationalIndexColumnNotFound, db.setSchemaJson(alloc,
            \\{"version":3,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"other":{"type":"keyword"}},"additionalProperties":false}}}}
        ));
        try std.testing.expect(db.core.relational_indexes.isCurrent(current));
        // Runtime metadata is compiled from the unpublished source, never by
        // reopening the target's old schema after durable publication.
        var target_tmp = try TestDirectory.init("db");
        defer target_tmp.cleanup();
        const target_path = target_tmp.path().ptr;
        var target = try DB.open(alloc, std.mem.span(target_path), .{ .start_optional_runtimes = false });
        defer target.close();
        {
            var restored = try target.preparePortableRuntimeMetadata(db.core.store, doc_identity.default_namespace);
            defer restored.deinit();
            try std.testing.expect(restored.relational_indexes.?.head.eql(current.head));
            try std.testing.expectEqual(restored.schema_replacement.current.?, restored.relational_indexes.?.plan.schemaView().epoch);
            try std.testing.expect(target.core.relational_indexes.acquire() == null);
        }
        try db.core.store.put(catalog.head_key, "corrupt");
        try std.testing.expectError(error.InvalidRelationalIndexCatalog, target.preparePortableRuntimeMetadata(db.core.store, doc_identity.default_namespace));
        try std.testing.expect(target.core.relational_indexes.acquire() == null);
        try std.testing.expect(target.core.acquireSchemaView() == null);
        try db.core.store.put(catalog.head_key, &current.head.encode());
        var archive = std.ArrayList(u8).empty;
        defer archive.deinit(alloc);
        try portable_backup.exportPortable(alloc, db.core.store, &archive);
        try target.importPortableIntoUnpublishedEmpty(alloc, archive.items, doc_identity.default_namespace);
        var restored = target.core.relational_indexes.acquire().?;
        defer restored.deinit();
        var target_schema = target.core.acquireSchemaView().?;
        defer target_schema.release();
        try std.testing.expectEqual(target_schema.epoch, restored.plan.schemaView().epoch);
        try std.testing.expect(restored.head.eql(current.head));
        var txn = try db.core.store.beginReadTxn();
        defer txn.abort();
        var durable = (try catalog.load(alloc, &txn)).?;
        defer durable.deinit();
        try std.testing.expect(durable.head.eql(current.head));
    }
    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
        defer reopened.close();
        var recovered = reopened.core.relational_indexes.acquire().?;
        defer recovered.deinit();
        var schema_view = reopened.core.acquireSchemaView().?;
        defer schema_view.release();
        try std.testing.expectEqual(schema_view.epoch, recovered.plan.schemaView().epoch);
        try std.testing.expectEqual(@as(u32, 2), recovered.head.schema_version);
        try std.testing.expectEqualStrings("by_id", recovered.plan.boundIndexes()[0].name);
    }
}

test "relational public schema provenance rejects missing validators on reopen" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
        defer db.close();
        try db.setSchemaJson(alloc,
            \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
        );
        try db.core.store.delete(public_schema_json_key);
    }
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false }));
}

test "relational prepared intents reject physical overflow before voting and persist canonical rows" {
    const alloc = std.testing.allocator;
    const t = try TestDb.open(alloc, .{ .start_optional_runtimes = false });
    defer t.deinit();
    const db = &t.db;
    try db.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"embedding":{"type":"embedding"}},"additionalProperties":false}}}}
    );
    const txn = try db.beginTransaction(100);
    try std.testing.expectError(error.InvalidBatchRequest, db.writeIntents(txn, &.{.{ .key = "bad", .value = "{\"embedding\":[1e100]}" }}, &.{}));
    try std.testing.expectEqual(@as(?transactions_mod.SchemaBinding, null), try db.core.transactionSchemaBinding(alloc, txn));
    try std.testing.expectError(error.InvalidBatchRequest, db.writeTransaction(txn, .{ .writes = &.{.{ .key = "bad", .value = "{\"embedding\":[-1e100]}" }} }));
    try db.writeIntents(txn, &.{.{ .key = "ok", .value = "{\"embedding\":[1,2]}" }}, &.{});
    var captured = try db.core.collectTransactionIntentBatch(alloc, txn);
    defer captured.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), captured.writes.len);
    const packed_bytes = captured.prepared_rows[0].?;
    try std.testing.expectEqual(@as(u32, 1), try mapper.relationalRowSchemaVersion(packed_bytes));
    const digest = try relational_row_codec.rowSemanticHash(packed_bytes);
    try db.commitTransaction(txn, 200);
    const row_key = try internal_keys.relationalRowKeyAlloc(alloc, "ok");
    defer alloc.free(row_key);
    const stored = try db.core.store.get(alloc, row_key);
    defer alloc.free(stored);
    try std.testing.expectEqual(digest, try relational_row_codec.rowSemanticHash(stored));
}

test "relational cumulative prepares remain committable within the preparation envelope" {
    const alloc = std.testing.allocator;
    var options = resource_manager_mod.Options{ .identity_allocator = alloc };
    options.budgets[@backingInt(resource_manager_mod.Slice.relational_preparation_working_set)] = .{ .hard_limit_bytes = 2 * 1024 * 1024 };
    var resources = resource_manager_mod.ResourceManager.init(options);
    defer resources.deinit(alloc);
    const t = try TestDb.open(alloc, .{ .resource_manager = &resources, .start_optional_runtimes = false });
    defer t.deinit();
    const db = &t.db;
    const columns = [_]schema_mod.RelationalColumn{.{ .name = "body", .path = "body", .column_type = .string }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    // Admission is table policy, deliberately independent of this process's
    // working-set budget. This fixture chooses a small logical transaction cap.
    db.core.table_catalog.transaction_admission_bytes = 32 * 1024;
    const payload: [1024]u8 = @splat('x');
    const document = try std.fmt.allocPrint(alloc, "{{\"body\":\"{s}\"}}", .{payload});
    defer alloc.free(document);
    const txn = try db.beginTransaction(100);
    var admitted: usize = 0;
    while (admitted < 64) : (admitted += 1) {
        var key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "row:{d}", .{admitted});
        db.writeIntents(txn, &.{.{ .key = key, .value = document }}, &.{}) catch |err| switch (err) {
            error.TransactionTooLarge => break,
            else => return err,
        };
    }
    try std.testing.expect(admitted > 1 and admitted < 64);
    var snapshot = try db.core.collectTransactionIntentBatch(alloc, txn);
    defer snapshot.deinit(alloc);
    try std.testing.expectEqual(admitted, snapshot.writes.len);
    try db.commitTransaction(txn, 200);
    try std.testing.expectEqual(@as(u64, 0), resources.sliceStats(.relational_preparation_working_set).used_bytes);
}

test "relational direct intents share preparation admission before the apply fence" {
    const alloc = std.testing.allocator;
    var manager = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = alloc });
    defer manager.deinit(alloc);
    const t = try TestDb.open(alloc, .{ .resource_manager = &manager, .start_index_workers = false, .start_optional_runtimes = false, .start_optional_runtime_workers = false, .ttl_cleanup = .{ .enabled = false } });
    defer t.deinit();
    const db = &t.db;
    const columns = [_]schema_mod.RelationalColumn{.{ .name = "id", .path = "id", .column_type = .string, .required = true }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    const txn_id = try db.beginTransaction(100);
    const hard_limit = manager.sliceStats(.relational_preparation_working_set).hard_limit_bytes;
    var reservation = try manager.reserve(.relational_preparation_working_set, hard_limit);
    defer reservation.release();
    const intents = [_]transactions_mod.WriteIntent{.{ .key = "row", .value = "{\"id\":\"ok\"}" }};
    const before = db.snapshotApplyLockStats();
    try std.testing.expectError(error.ResourceBudgetExceeded, db.writeIntents(txn_id, &intents, &.{}));
    try std.testing.expectEqualDeep(before, db.snapshotApplyLockStats());
    reservation.release();
    try db.writeIntents(txn_id, &intents, &.{});
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.relational_preparation_working_set).used_bytes);
    reservation = try manager.reserve(.relational_preparation_working_set, hard_limit);
    const before_commit = db.snapshotApplyLockStats();
    try std.testing.expectError(error.ResourceBudgetExceeded, db.commitTransaction(txn_id, 200));
    try std.testing.expectEqualDeep(before_commit, db.snapshotApplyLockStats());
    try std.testing.expectEqual(transactions_mod.TxnStatus.pending, try db.getTransactionStatus(txn_id));
    reservation.release();
    try db.commitTransaction(txn_id, 200);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.relational_preparation_working_set).used_bytes);
    const row = (try db.get(alloc, "row")).?;
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"id\":\"ok\"}", row);
}

test "relational transaction epoch survives schema publication and participant restart" {
    const alloc = std.testing.allocator;
    const v1 =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"amount":{"type":"integer","minimum":0}},"required":["amount"],"additionalProperties":false}}}}
    ;
    const v2 =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"amount":{"type":"string"}},"required":["amount"],"additionalProperties":false}}}}
    ;
    for ([_]bool{ false, true }) |public| {
        var path_tmp = try TestDirectory.init("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        var txn_ids: [2]transactions_mod.TxnId = undefined;
        {
            var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
            defer db.close();
            const columns = [_]schema_mod.RelationalColumn{.{ .name = "amount", .path = "amount", .column_type = .integer, .required = true }};
            if (public) try db.setSchemaJson(alloc, v1) else try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
            txn_ids[0] = try db.beginTransaction(100);
            txn_ids[1] = try db.beginTransaction(101);
            try db.writeIntents(txn_ids[0], &.{.{ .key = "direct", .value = "{\"amount\":3}" }}, &.{});
            try db.writeTransaction(txn_ids[1], .{ .writes = &.{.{ .key = "transaction", .value = "{\"amount\":4}" }} });
            const changed = [_]schema_mod.RelationalColumn{.{ .name = "amount", .path = "amount", .column_type = .string, .required = true }};
            if (public) try db.setSchemaJson(alloc, v2) else try db.setSchema(.{ .version = 2, .storage_mode = .relational, .relational_columns = &changed });
            // Retrying/expanding an already prepared participant keeps v1.
            if (public) try std.testing.expectError(error.InvalidBatchRequest, db.writeIntents(txn_ids[0], &.{.{ .key = "invalid", .value = "{\"amount\":-1}" }}, &.{}));
            try db.writeIntents(txn_ids[0], &.{.{ .key = "direct", .value = "{\"amount\":5}" }}, &.{});
            try db.writeTransaction(txn_ids[1], .{ .writes = &.{.{ .key = "transaction", .value = "{\"amount\":6}" }} });
        }
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
        defer db.close();
        try db.commitTransaction(txn_ids[0], 200);
        try db.commitTransaction(txn_ids[1], 201);
        // Idempotent terminal retries must not re-apply v1 over future rows.
        try db.commitTransaction(txn_ids[0], 200);
        try db.batch(.{ .writes = &.{.{ .key = "direct", .value = "{\"amount\":\"replaced\"}" }} });
        try db.commitTransaction(txn_ids[0], 200);
        const replaced = (try db.get(alloc, "direct")).?;
        defer alloc.free(replaced);
        try std.testing.expectEqualStrings("{\"amount\":\"replaced\"}", replaced);
        try db.batch(.{ .writes = &.{.{ .key = "current", .value = "{\"amount\":\"new\"}" }} });
        for ([_][]const u8{ "direct", "transaction", "current" }, 0..) |key, i| {
            const store_key = try relational_store.keyAlloc(alloc, key);
            defer alloc.free(store_key);
            const row = try db.core.store.get(alloc, store_key);
            defer alloc.free(row);
            try std.testing.expectEqual(@as(u32, if (i == 1) 1 else 2), try relational_store.rowSchemaVersion(row));
        }
        try std.testing.expectEqual(@as(u32, 2), db.core.schema.?.version);
    }
}

test "relational durable transaction epoch does not cross whole namespace replacement" {
    const alloc = std.testing.allocator;
    const t = try TestDb.open(alloc, .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer t.deinit();
    const db = &t.db;
    const columns = [_]schema_mod.RelationalColumn{.{ .name = "amount", .path = "amount", .column_type = .integer }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    const txn = try db.beginTransaction(100);
    const writes = [_]types.BatchWrite{.{ .key = "row", .value = "{\"amount\":3}" }};
    try db.writeIntents(txn, &.{.{ .key = "row", .value = writes[0].value }}, &.{});
    const namespace = db.core.schemaNamespaceGeneration();
    var snapshot = try db.core.collectTransactionIntentBatch(alloc, txn);
    defer snapshot.deinit(alloc);
    // Restore/reload may reuse schema versions. Namespace identity, not just
    // the durable version or intent revision, must invalidate old preparation.
    lockApply(db);
    db.core.reloadSchemaFromStore() catch |err| {
        db.core.unlockApply();
        return err;
    };
    db.core.unlockApply();
    try std.testing.expect(namespace != db.core.schemaNamespaceGeneration());
    try std.testing.expectError(error.PreparedGenerationChanged, db.batchInternal(.{ .writes = &writes }, null, .{
        .transaction_resolution = .{
            .txn_id = txn,
            .status = .committed,
            .commit_version = 200,
            .expected_intent_revision = snapshot.revision,
            .intent_keys = &.{"row"},
            .schema_binding = snapshot.schema_binding,
            .schema_namespace_generation = namespace,
        },
    }));
    try std.testing.expect((try db.get(alloc, "row")) == null);
    try std.testing.expectEqual(transactions_mod.TxnStatus.pending, try db.getTransactionStatus(txn));
    try db.commitTransaction(txn, 200);
}

test "relational initial storage mode cannot strand schemaless transaction intents" {
    const alloc = std.testing.allocator;
    const t = try TestDb.open(alloc, .{ .start_optional_runtimes = false });
    defer t.deinit();
    const db = &t.db;
    const txn = try db.beginTransaction(100);
    try db.writeIntents(txn, &.{.{ .key = "row", .value = "{\"amount\":3}" }}, &.{});
    const columns = [_]schema_mod.RelationalColumn{.{ .name = "amount", .path = "amount", .column_type = .integer }};
    const schema = schema_mod.TableSchema{ .version = 1, .storage_mode = .relational, .relational_columns = &columns };
    try std.testing.expectError(error.SchemaInUse, db.setSchema(schema));
    try std.testing.expect(db.core.schema == null);
    try db.abortTransaction(txn, 200);
    try db.abortTransaction(txn, 200);
    try db.setSchema(schema);
}

test "relational preparation budget rejects contention before commit and releases on retry" {
    const alloc = std.testing.allocator;
    var manager = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = alloc });
    defer manager.deinit(alloc);
    const t = try TestDb.open(alloc, .{ .resource_manager = &manager, .start_index_workers = false, .ttl_cleanup = .{ .enabled = false } });
    defer t.deinit();
    const db = &t.db;
    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "id", .path = "id", .column_type = .string, .required = true },
    };
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    const hard_limit = manager.sliceStats(.relational_preparation_working_set).hard_limit_bytes;
    var competing = try manager.reserve(.relational_preparation_working_set, hard_limit);
    defer competing.release();
    const request: types.BatchRequest = .{ .writes = &.{.{ .key = "row", .value = "{\"id\":\"ok\"}" }}, .sync_level = .write };
    try std.testing.expectError(error.ResourceBudgetExceeded, db.batch(request));
    try std.testing.expectEqual(hard_limit, manager.sliceStats(.relational_preparation_working_set).used_bytes);
    try std.testing.expectEqual(@as(?[]u8, null), try db.get(alloc, "row"));
    competing.release();
    try db.batch(request);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.relational_preparation_working_set).used_bytes);
    const stored = (try db.get(alloc, "row")).?;
    defer alloc.free(stored);
    try std.testing.expectEqualStrings("{\"id\":\"ok\"}", stored);
}

test "relational preparation budget includes expanded transform snapshots" {
    const alloc = std.testing.allocator;
    var manager = resource_manager_mod.ResourceManager.init(.{ .identity_allocator = alloc });
    defer manager.deinit(alloc);
    const t = try TestDb.open(alloc, .{ .resource_manager = &manager, .start_index_workers = false, .ttl_cleanup = .{ .enabled = false } });
    defer t.deinit();
    const db = &t.db;
    const columns = [_]schema_mod.RelationalColumn{.{ .name = "id", .path = "id", .column_type = .string, .required = true }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    const content = try alloc.alloc(u8, 256 * 1024);
    defer alloc.free(content);
    @memset(content, 'x');
    const document = try std.fmt.allocPrint(alloc, "{{\"id\":\"{s}\"}}", .{content});
    defer alloc.free(document);
    try db.batch(.{ .writes = &.{.{ .key = "row", .value = document }}, .sync_level = .write });
    const hard_limit = manager.sliceStats(.relational_preparation_working_set).hard_limit_bytes;
    var competing = try manager.reserve(.relational_preparation_working_set, hard_limit - 64 * 1024);
    defer competing.release();
    const request: types.BatchRequest = .{ .transforms = &.{.{
        .key = "row",
        .operations = &.{.{ .op = .set, .path = "id", .value_json = "\"small\"" }},
    }}, .sync_level = .write };
    // The final row is tiny: rejection must happen while reconstructing the
    // large base, before any prepared-row allocation or durable mutation.
    try std.testing.expectError(error.ResourceBudgetExceeded, db.batch(request));
    try std.testing.expectEqual(hard_limit - 64 * 1024, manager.sliceStats(.relational_preparation_working_set).used_bytes);
    const unchanged = (try db.get(alloc, "row")).?;
    defer alloc.free(unchanged);
    try std.testing.expectEqualStrings(document, unchanged);
    competing.release();
    try db.batch(request);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.relational_preparation_working_set).used_bytes);
    const changed = (try db.get(alloc, "row")).?;
    defer alloc.free(changed);
    try std.testing.expectEqualStrings("{\"id\":\"small\"}", changed);
}

test "relational transform preparation releases every failed allocation" {
    const Check = struct {
        fn run(alloc: Allocator) !void {
            // An explicit empty snapshot supplies absent durable bases. This
            // pure coalescing path must not touch DB state or its allocator.
            var db: DB = undefined;
            const snapshot: DB.TransformReadSnapshot = .{};
            var prepared = try db.coalesceKeyValueRequest(alloc, types.BatchWrite, &.{.{ .key = "existing", .value = "{\"count\":1}" }}, &.{}, &.{
                .{ .key = "new", .upsert = true, .operations = &.{.{ .op = .set, .path = "count", .value_json = "2" }} },
                .{ .key = "existing", .operations = &.{.{ .op = .inc, .path = "count", .value_json = "1" }} },
                .{ .key = "existing", .operations = &.{.{ .op = .inc, .path = "count", .value_json = "1" }} },
            }, &snapshot);
            defer prepared.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 2), prepared.writes.len);
        }
    };
    try platform.allocator.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "relational columnar dirty scans intersect query and shard bounds before decoding" {
    const alloc = std.testing.allocator;
    for ([_]PrimaryBackend{.{ .lsm = .{ .flush_threshold = 1 } }}) |backend| {
        const t = try TestDb.open(alloc, .{ .start_optional_runtimes = false, .primary_backend = backend });
        defer t.deinit();
        const db = &t.db;
        const columns = [_]schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
        try db.batch(.{ .writes = &.{
            .{ .key = "a", .value = "{\"n\":1}" },
            .{ .key = "b", .value = "{\"n\":2}" },
            .{ .key = "b\x00", .value = "{\"n\":3}" },
            .{ .key = "z", .value = "{\"n\":4}" },
        } });
        try std.testing.expect(try db.rebuildRelationalColumns());
        for ([_][]const u8{ "a", "z" }) |id| {
            const key = try internal_keys.relationalRowKeyAlloc(alloc, id);
            defer alloc.free(key);
            try db.core.store.put(key, "corrupt outside query");
        }
        var stats: types.ColumnarScanStats = .{};
        var result = try db.scan(alloc, "b", "b", .{ .inclusive_from = true, .columnar_stats = &stats });
        defer result.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 1), result.hashes.len);
        try std.testing.expectEqualStrings("b", result.hashes[0].id);
        try std.testing.expectEqual(@as(u64, 0), stats.primary_rows_read);
        // The shard end is exclusive; b\x00 must not be confused with b.
        try db.updateRange(.{ .start = "b", .end = "b\x00" });
        stats = .{};
        var shard = try db.scan(alloc, "", "", .{ .columnar_stats = &stats });
        defer shard.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 1), shard.hashes.len);
        try std.testing.expectEqualStrings("b", shard.hashes[0].id);
        try std.testing.expectEqual(@as(u64, 0), stats.primary_rows_read);
        var empty = try db.scan(alloc, "b", "b", .{ .inclusive_from = true, .exclusive_to = true });
        defer empty.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 0), empty.hashes.len);
    }
}

test "relational columnar existence and null projection do not fetch payload records" {
    // Assert payload/admission semantics independently of the real-time build
    // quantum. Bounded/resumable maintenance is tested separately.
    relational_columns.test_disable_deadline = true;
    defer relational_columns.test_disable_deadline = false;
    const alloc = std.testing.allocator;
    const t = try TestDb.open(alloc, .{ .start_optional_runtimes = false, .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } } });
    defer t.deinit();
    const db = &t.db;
    const columns = [_]schema_mod.RelationalColumn{.{ .name = "payload", .path = "payload", .column_type = .string, .allows_null = true }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    const payload = try alloc.alloc(u8, 64 * 1024);
    defer alloc.free(payload);
    @memset(payload, 'x');
    const json = try std.fmt.allocPrint(alloc, "{{\"payload\":\"{s}\"}}", .{payload});
    defer alloc.free(json);
    try db.batch(.{ .writes = &.{ .{ .key = "a", .value = json }, .{ .key = "b", .value = "{\"payload\":null}" }, .{ .key = "c", .value = "{}" } } });
    try std.testing.expect(try db.rebuildRelationalColumns());
    const payload_key = try relational_columns.payloadKeyForTest(db, alloc, 1, 0x200000000, 0);
    defer alloc.free(payload_key);
    try db.core.store.delete(payload_key);
    var stats: types.ColumnarScanStats = .{};
    var exists = try db.scan(alloc, "", "", .{ .filter_query_json = "{\"exists\":{\"field\":\"payload\"}}", .columnar_stats = &stats });
    defer exists.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), exists.hashes.len);
    try std.testing.expectEqual(@as(u64, 0), stats.payload_bytes_read);
    try std.testing.expectEqual(@as(u64, 0), stats.primary_rows_read);
    try std.testing.expect(stats.metadata_bytes_read < 1024);
    stats = .{};
    var null_row = try db.scan(alloc, "b", "b", .{ .inclusive_from = true, .include_documents = true, .include_all_fields = false, .fields = &.{"payload"}, .columnar_stats = &stats });
    defer null_row.deinit(alloc);
    try std.testing.expectEqualStrings("{\"payload\":null}", null_row.documents[0].json);
    try std.testing.expect(stats.used);
    try std.testing.expectEqual(@as(u64, 0), stats.logical_slots_initialized);
    try std.testing.expectEqual(@as(u64, 0), stats.payload_bytes_read);
    try std.testing.expectEqual(@as(u64, 0), stats.primary_rows_read);
    // A consumer that actually needs the missing payload still falls back.
    var projected = try db.scan(alloc, "a", "a", .{ .inclusive_from = true, .include_documents = true, .include_all_fields = false, .fields = &.{"payload"} });
    defer projected.deinit(alloc);
    try std.testing.expectEqualStrings(json, projected.documents[0].json);
    try std.testing.expect(db.relational_columns_rebuild_requested.load(.acquire));
}

test "relational columnar adaptive maintenance resumes after empty coverage" {
    const alloc = std.testing.allocator;
    const t = try TestDb.open(alloc, .{ .start_optional_runtimes = false });
    defer t.deinit();
    const db = &t.db;
    const columns = [_]schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    for (0..3) |cycle| {
        for (0..64) |_| {
            _ = try db.runRelationalColumnMaintenancePass();
            if (!db.relational_column_maintenance.pending.load(.acquire)) break;
        }
        try std.testing.expect(!db.relational_column_maintenance.pending.load(.acquire));
        if (cycle == 1) {
            try db.batch(.{ .deletes = &.{"row"} });
        } else try db.batch(.{ .writes = &.{.{ .key = "row", .value = "{\"n\":1}" }} });
    }
    for (0..64) |_| {
        _ = try db.runRelationalColumnMaintenancePass();
        if (!db.relational_column_maintenance.pending.load(.acquire)) break;
    }
    try std.testing.expect(!db.relational_column_maintenance.pending.load(.acquire));
    var scan_stats: types.ColumnarScanStats = .{};
    var result = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"}, .columnar_stats = &scan_stats });
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), result.documents.len);
    try std.testing.expectEqual(@as(u64, 0), scan_stats.primary_rows_read);
}

test "relational columnar range admission ignores unrelated mutation epochs" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    defer db.close();
    const columns = [_]schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const writes = try scratch.alloc(types.BatchWrite, 1024);
    for (writes, 0..) |*write, i| write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = "{\"n\":1}" };
    try db.batch(.{ .writes = writes });
    try drainTestRelationalMaintenance(&db);
    for (0..4) |i| try db.batch(.{ .writes = &.{.{ .key = writes[i * 256].key, .value = "{\"n\":2}" }} });
    relational_columns.test_now_ns = 100 * std.time.ns_per_s;
    defer relational_columns.test_now_ns = null;
    for (0..8) |_| _ = try db.runRelationalColumnMaintenancePass();
    const roots = db.relational_column_maintenance.admission_root_reads.load(.monotonic);
    const deferred = db.relational_column_maintenance.ranges_deferred.load(.monotonic);
    try std.testing.expect(roots >= 4);
    for (0..8) |i| {
        try db.batch(.{ .writes = &.{.{ .key = writes[0].key, .value = try std.fmt.allocPrint(scratch, "{{\"n\":{d}}}", .{i + 3}) }} });
        _ = try db.runRelationalColumnMaintenancePass();
    }
    try std.testing.expectEqual(roots, db.relational_column_maintenance.admission_root_reads.load(.monotonic));
    try std.testing.expectEqual(deferred + 8, db.relational_column_maintenance.ranges_deferred.load(.monotonic));
    db.close();
    db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    for (0..8) |_| _ = try db.runRelationalColumnMaintenancePass();
    try std.testing.expectEqual(@as(u64, 0), db.relational_column_maintenance.admission_root_reads.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), db.relational_column_maintenance.ranges_deferred.load(.monotonic));
}

test "relational columnar selected payload pages bound wide projection reads" {
    const alloc = std.testing.allocator;
    for ([_]PrimaryBackend{.{ .lsm = .{ .flush_threshold = 1 } }}) |backend| {
        var path_tmp = try TestDirectory.init("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        defer db.close();
        const columns = [_]schema_mod.RelationalColumn{
            .{ .name = "n", .path = "n", .column_type = .integer },
            .{ .name = "payload", .path = "payload", .column_type = .string },
        };
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        var random = std.Random.DefaultPrng.init(42);
        const writes = try scratch.alloc(types.BatchWrite, 64);
        const payloads = try scratch.alloc([]const u8, writes.len);
        for (writes, 0..) |*write, i| {
            const payload = try scratch.alloc(u8, 4096);
            for (payload) |*byte| byte.* = 'a' + random.random().uintLessThan(u8, 26);
            payloads[i] = payload;
            write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = try std.fmt.allocPrint(scratch, "{{\"n\":{d},\"payload\":\"{s}\"}}", .{ i, payload }) };
        }
        try db.batch(.{ .writes = writes });
        try drainTestRelationalMaintenance(&db);
        var all_stats: types.ColumnarScanStats = .{};
        var all = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"payload"}, .columnar_stats = &all_stats });
        defer all.deinit(alloc);
        var one_stats: types.ColumnarScanStats = .{};
        var one = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"payload"}, .limit = 1, .columnar_stats = &one_stats });
        defer one.deinit(alloc);
        try std.testing.expectEqualDeep(all.documents[0], one.documents[0]);
        try std.testing.expectEqual(@as(u64, 1), one_stats.payload_pages_read);
        try std.testing.expect(one_stats.payload_bytes_read * 8 < all_stats.payload_bytes_read);
        // The scalar predicate narrows the payload selection before wide pages
        // are decoded; OR leaves can request different pages of one column.
        const filter = try std.fmt.allocPrint(scratch, "{{\"disjuncts\":[{{\"conjuncts\":[{{\"term\":{{\"n\":0}}}},{{\"term\":{{\"payload\":\"{s}\"}}}}]}},{{\"conjuncts\":[{{\"term\":{{\"n\":63}}}},{{\"term\":{{\"payload\":\"{s}\"}}}}]}}]}}", .{ payloads[0], payloads[63] });
        var filtered_stats: types.ColumnarScanStats = .{};
        var filtered = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"payload"}, .filter_query_json = filter, .columnar_stats = &filtered_stats });
        defer filtered.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 2), filtered.documents.len);
        try std.testing.expectEqualDeep(all.documents[0], filtered.documents[0]);
        try std.testing.expectEqualDeep(all.documents[63], filtered.documents[1]);
        try std.testing.expect(filtered_stats.payload_bytes_read * 4 < all_stats.payload_bytes_read);
        // Typed predicates and borrowed text projection need neither logical
        // value materialization nor a block-owned logical cache.
        try std.testing.expectEqual(@as(u64, 0), filtered_stats.values_materialized);
        try std.testing.expectEqual(@as(u64, 0), filtered_stats.logical_slots_initialized);
        // Equivalent predicate order must not force wide payload reads.
        const reversed_filter = try std.fmt.allocPrint(scratch, "{{\"conjuncts\":[{{\"term\":{{\"payload\":\"{s}\"}}}},{{\"term\":{{\"n\":0}}}}]}}", .{payloads[0]});
        var reversed_stats: types.ColumnarScanStats = .{};
        var reversed = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"payload"}, .filter_query_json = reversed_filter, .columnar_stats = &reversed_stats });
        defer reversed.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 1), reversed.documents.len);
        try std.testing.expectEqualDeep(all.documents[0], reversed.documents[0]);
        try std.testing.expect(reversed_stats.payload_bytes_read * 8 < all_stats.payload_bytes_read);
        const payload_filter = try std.fmt.allocPrint(scratch, "{{\"term\":{{\"payload\":\"{s}\"}}}}", .{payloads[0]});
        var limited_stats: types.ColumnarScanStats = .{};
        var limited = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"payload"}, .filter_query_json = payload_filter, .limit = 1, .columnar_stats = &limited_stats });
        defer limited.deinit(alloc);
        try std.testing.expectEqualDeep(one.documents, limited.documents);
        try std.testing.expectEqual(@as(u64, 1), limited_stats.payload_pages_read);
        try std.testing.expect(limited_stats.payload_bytes_read * 8 < all_stats.payload_bytes_read);
        // Both bounded overlays and dense-plan admission must ignore obsolete
        // base predicates, including when no current row matches the filter.
        const replacements = try scratch.alloc(types.BatchWrite, writes.len);
        for (writes, replacements) |write, *replacement| replacement.* = .{ .key = write.key, .value = "{\"n\":100}" };
        try db.batch(.{ .writes = replacements });
        for ([_]u32{ 0, 1 }) |limit| {
            var replaced_stats: types.ColumnarScanStats = .{};
            var replaced = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"payload"}, .filter_query_json = payload_filter, .limit = limit, .columnar_stats = &replaced_stats });
            defer replaced.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 0), replaced.documents.len);
            try std.testing.expectEqual(@as(u64, 0), replaced_stats.payload_bytes_read);
        }
        try db.batch(.{ .writes = writes });
        // Coverage publication retires every payload page, including pages
        // never read by the point projection above.
        try db.batch(.{ .deletes = &.{ writes[0].key, writes[63].key } });
        try drainTestRelationalMaintenance(&db);
        var after = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"payload"} });
        defer after.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 62), after.documents.len);
        db.close();
        db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        var reopened_stats: types.ColumnarScanStats = .{};
        var reopened = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"payload"}, .columnar_stats = &reopened_stats });
        defer reopened.deinit(alloc);
        try std.testing.expectEqualDeep(after.documents, reopened.documents);
        try std.testing.expectEqual(@as(u64, 0), reopened_stats.primary_rows_read);
    }
}

test "relational columnar skew pages and sparse delta merges bound physical work" {
    const alloc = std.testing.allocator;
    for ([_]PrimaryBackend{.{ .lsm = .{ .flush_threshold = 1 } }}) |backend| {
        var path_tmp = try TestDirectory.init("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        defer db.close();
        const columns = [_]schema_mod.RelationalColumn{
            .{ .name = "n", .path = "n", .column_type = .integer },
            .{ .name = "payload", .path = "payload", .column_type = .string, .allows_null = true },
        };
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        const large = try scratch.alloc(u8, 512 * 1024);
        var random = std.Random.DefaultPrng.init(123);
        for (large) |*byte| byte.* = 'a' + random.random().uintLessThan(u8, 26);
        const writes = try scratch.alloc(types.BatchWrite, 128);
        for (writes, 0..) |*write, i| write.* = .{
            .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}),
            .value = try std.fmt.allocPrint(scratch, "{{\"n\":{d},\"payload\":\"{s}\"}}", .{ i, if (i == 1) large else "small" }),
        };
        try db.batch(.{ .writes = writes });
        try drainTestRelationalMaintenance(&db);
        for ([_][]const u8{ "", "{\"term\":{\"payload\":\"small\"}}" }) |filter| {
            var stats: types.ColumnarScanStats = .{};
            var one = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"payload"}, .limit = 1, .filter_query_json = filter, .columnar_stats = &stats });
            defer one.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 1), one.documents.len);
            try std.testing.expectEqualStrings("{\"payload\":\"small\"}", one.documents[0].json);
            try std.testing.expectEqual(@as(u64, 1), stats.payload_pages_read);
            try std.testing.expect(stats.payload_bytes_read < 1024);
        }
        const primary_before = db.relational_column_maintenance.primary_rows_read.load(.monotonic);
        const covered_before = db.relational_column_maintenance.covered_rows_read.load(.monotonic);
        try db.batch(.{ .writes = &.{
            .{ .key = "k0000", .value = "{\"n\":-1,\"payload\":\"updated\"}" },
            .{ .key = "k0010a", .value = "{\"n\":500,\"payload\":null}" },
        }, .deletes = &.{ "k0001", "k0127" } });
        const options: types.ScanOptions = .{ .include_documents = true, .include_all_fields = false, .fields = &.{ "n", "payload" }, .include_content_hashes = true };
        var before = try db.scan(alloc, "", "", options);
        defer before.deinit(alloc);
        try drainTestRelationalMaintenance(&db);
        try std.testing.expectEqual(@as(u64, 2), db.relational_column_maintenance.primary_rows_read.load(.monotonic) - primary_before);
        try std.testing.expect(db.relational_column_maintenance.covered_rows_read.load(.monotonic) > covered_before);
        var stats: types.ColumnarScanStats = .{};
        var measured = options;
        measured.columnar_stats = &stats;
        var after = try db.scan(alloc, "", "", measured);
        defer after.deinit(alloc);
        try std.testing.expectEqualDeep(before.documents, after.documents);
        try std.testing.expectEqualDeep(before.hashes, after.hashes);
        try std.testing.expectEqual(@as(u64, 0), stats.primary_rows_read);
        try std.testing.expectEqual(@as(u64, 2 * 127), stats.cell_slots_initialized);
        try std.testing.expect(stats.cell_cache_hits > 127);
        db.close();
        db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        var reopened = try db.scan(alloc, "", "", options);
        defer reopened.deinit(alloc);
        try std.testing.expectEqualDeep(after.documents, reopened.documents);
    }
}

test "relational columnar merge frontier skips tombstones and unread base pages" {
    const alloc = std.testing.allocator;
    for ([_]PrimaryBackend{.{ .lsm = .{ .flush_threshold = 1 } }}) |backend| {
        const t = try TestDb.open(alloc, .{ .start_optional_runtimes = false, .primary_backend = backend });
        defer t.deinit();
        const db = &t.db;
        const columns = [_]schema_mod.RelationalColumn{.{ .name = "payload", .path = "payload", .column_type = .string }};
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        const large = try scratch.alloc(u8, 512 * 1024);
        @memset(large, 'x');
        var writes: [201]types.BatchWrite = undefined;
        var deletes: [200][]const u8 = undefined;
        for (writes[0..200], 0..) |*write, i| {
            const key = try std.fmt.allocPrint(scratch, "{d:0>4}", .{i});
            write.* = .{ .key = key, .value = "{\"payload\":\"other\"}" };
            deletes[i] = key;
        }
        writes[200] = .{ .key = "z", .value = try std.fmt.allocPrint(scratch, "{{\"payload\":\"{s}\"}}", .{large}) };
        try db.batch(.{ .writes = &writes });
        try drainTestRelationalMaintenance(db);
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"payload\":\"match\"}" }}, .deletes = &deletes });
        var stats: types.ColumnarScanStats = .{};
        var result = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"payload"}, .limit = 1, .filter_query_json = "{\"term\":{\"payload\":\"match\"}}", .columnar_stats = &stats });
        defer result.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 1), result.documents.len);
        try std.testing.expectEqualStrings("a", result.documents[0].id);
        try std.testing.expectEqual(@as(u64, 0), stats.payload_pages_read);
        try std.testing.expectEqual(@as(u64, 1), stats.primary_rows_read);
        try std.testing.expectEqual(@as(u64, 200), stats.overlay_tombstones_skipped);
        try drainTestRelationalMaintenance(db);
        try relational_columns.validatePayloadOwnershipForTest(db, alloc);
    }
}

test "relational columnar tombstone costing preserves narrow projection" {
    const alloc = std.testing.allocator;
    relational_columns.test_disable_deadline = true;
    defer relational_columns.test_disable_deadline = false;
    for ([_]PrimaryBackend{.{ .lsm = .{ .flush_threshold = 1 } }}) |backend| {
        const t = try TestDb.open(alloc, .{ .start_optional_runtimes = false, .primary_backend = backend });
        defer t.deinit();
        const db = &t.db;
        const columns = [_]schema_mod.RelationalColumn{
            .{ .name = "n", .path = "n", .column_type = .integer },
            .{ .name = "payload", .path = "payload", .column_type = .string },
        };
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        const large = try scratch.alloc(u8, 200 * 1024);
        @memset(large, 'x');
        var writes: [101]types.BatchWrite = undefined;
        var deletes: [100][]const u8 = undefined;
        for (writes[0..100], &deletes, 0..) |*write, *key, i| {
            key.* = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i});
            write.* = .{ .key = key.*, .value = "{\"n\":0,\"payload\":\"small\"}" };
        }
        writes[100] = .{ .key = "z", .value = try std.fmt.allocPrint(scratch, "{{\"n\":1,\"payload\":\"{s}\"}}", .{large}) };
        try db.batch(.{ .writes = &writes });
        try drainTestRelationalMaintenance(db);
        try db.batch(.{ .deletes = &deletes });
        for ([_]u32{ 0, 64, 1 }) |limit| {
            var stats: types.ColumnarScanStats = .{};
            var result = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"}, .limit = limit, .columnar_stats = &stats });
            defer result.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 1), result.documents.len);
            try std.testing.expectEqualStrings("{\"n\":1}", result.documents[0].json);
            try std.testing.expectEqual(@as(u64, 0), stats.primary_rows_read);
            try std.testing.expectEqual(@as(u64, 0), stats.dense_delta_scans);
            try std.testing.expectEqual(@as(u64, 100), stats.overlay_tombstones_skipped);
            try std.testing.expect(stats.payload_bytes_read < 1024);
            if (limit != 1) try std.testing.expect(stats.estimated_overlay_bytes < stats.estimated_primary_bytes);
        }
    }
}

test "relational columnar churn bounds retained payload storage and reads" {
    const alloc = std.testing.allocator;
    relational_columns.test_disable_deadline = true;
    defer relational_columns.test_disable_deadline = false;
    for ([_]PrimaryBackend{.{ .lsm = .{ .flush_threshold = 1 } }}) |backend| {
        var path_tmp = try TestDirectory.init("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        defer db.close();
        const columns = [_]schema_mod.RelationalColumn{.{ .name = "payload", .path = "payload", .column_type = .string }};
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        var random = std.Random.DefaultPrng.init(801);
        var writes: [128]types.BatchWrite = undefined;
        for (&writes, 0..) |*write, i| {
            var value: [256]u8 = undefined;
            for (&value) |*byte| byte.* = 'a' + random.random().uintLessThan(u8, 26);
            write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = try std.fmt.allocPrint(scratch, "{{\"payload\":\"{s}\"}}", .{value}) };
        }
        try db.batch(.{ .writes = &writes });
        try drainTestRelationalMaintenance(&db);
        const original_payload = try relational_columns.payloadKeyForTest(&db, alloc, 1, 0x200000000, 0);
        defer alloc.free(original_payload);
        const Snapshot = struct {
            db: *DB,
            writes: []const types.BatchWrite,
            deletes: []const []const u8,
            stride: usize,
            index: usize = 0,
            fn visit(ptr: ?*anyopaque, row: types.ScanVisitEntry) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr.?));
                if (self.index == 0) {
                    try self.db.batch(.{ .deletes = self.deletes });
                    try drainTestRelationalMaintenance(self.db);
                }
                try std.testing.expectEqualStrings(self.writes[self.index * self.stride].value, row.document_json.?);
                self.index += 1;
            }
        };
        var previous_stride: usize = 1;
        for ([_]usize{ 2, 8, 32 }) |stride| {
            var deletes = std.ArrayListUnmanaged([]const u8).empty;
            for (writes, 0..) |write, i| if (i % previous_stride == 0 and i % stride != 0) try deletes.append(scratch, write.key);
            // Repack and reclaim while an old column scan is pinned. Every
            // old row must remain readable from its original payload snapshot.
            var snapshot: Snapshot = .{ .db = &db, .writes = &writes, .deletes = deletes.items, .stride = previous_stride };
            try db.scanVisit(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"payload"} }, .{ .context = &snapshot, .visit = Snapshot.visit });
            try std.testing.expectEqual(128 / previous_stride, snapshot.index);
            try relational_columns.validatePayloadOwnershipForTest(&db, alloc);
            var stats: types.ColumnarScanStats = .{};
            var result = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"payload"}, .columnar_stats = &stats });
            defer result.deinit(alloc);
            try std.testing.expectEqual(128 / stride, result.documents.len);
            for (result.documents, 0..) |row, i| try std.testing.expectEqualStrings(writes[i * stride].value, row.json);
            try std.testing.expectEqual(@as(u64, 0), stats.primary_rows_read);
            const live_bytes = result.documents.len * 256;
            try std.testing.expect(stats.payload_bytes_read < live_bytes * 2 + 1024);
            try std.testing.expect(try relational_columns.payloadStorageBytesForTest(&db, alloc) < live_bytes * 2 + 1024);
            previous_stride = stride;
            db.close();
            db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        }
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, original_payload));
        try relational_columns.validatePayloadOwnershipForTest(&db, alloc);
    }
}

test "relational columnar skewed partial payload reuse follows bytes not row count" {
    const alloc = std.testing.allocator;
    relational_columns.test_disable_deadline = true;
    defer relational_columns.test_disable_deadline = false;
    for ([_]PrimaryBackend{.{ .lsm = .{ .flush_threshold = 1 } }}) |backend| {
        const t = try TestDb.open(alloc, .{ .start_optional_runtimes = false, .primary_backend = backend });
        defer t.deinit();
        const db = &t.db;
        const columns = [_]schema_mod.RelationalColumn{.{ .name = "payload", .path = "payload", .column_type = .string }};
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        var random = std.Random.DefaultPrng.init(905);
        var large: [15 * 1024]u8 = undefined;
        for (&large) |*byte| byte.* = 'a' + random.random().uintLessThan(u8, 26);
        var writes: [64]types.BatchWrite = undefined;
        var deletes: [63][]const u8 = undefined;
        for (&writes, 0..) |*write, i| {
            write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = try std.fmt.allocPrint(scratch, "{{\"payload\":\"{s}\"}}", .{if (i == 0) &large else "x"}) };
            if (i != 0) deletes[i - 1] = write.key;
        }
        try db.batch(.{ .writes = &writes });
        try drainTestRelationalMaintenance(db);
        const encoded_before = db.relational_column_maintenance.payload_encoding_bytes.load(.monotonic);
        try db.batch(.{ .deletes = &deletes });
        try drainTestRelationalMaintenance(db);
        // One of 64 rows survives, but it owns almost all payload bytes.
        try std.testing.expectEqual(encoded_before, db.relational_column_maintenance.payload_encoding_bytes.load(.monotonic));
        try relational_columns.validatePayloadOwnershipForTest(db, alloc);
        var result = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"payload"} });
        defer result.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 1), result.documents.len);
        try std.testing.expectEqualStrings(writes[0].value, result.documents[0].json);
    }
}

test "relational columnar shared pages bound alternating merges and survive reclamation" {
    const alloc = std.testing.allocator;
    relational_columns.test_disable_deadline = true;
    defer relational_columns.test_disable_deadline = false;
    for ([_]PrimaryBackend{.{ .lsm = .{ .flush_threshold = 1 } }}) |backend| {
        var path_tmp = try TestDirectory.init("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        defer db.close();
        const columns = [_]schema_mod.RelationalColumn{
            .{ .name = "n", .path = "n", .column_type = .integer },
            .{ .name = "payload", .path = "payload", .column_type = .string },
        };
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        const large = try scratch.alloc(u8, 512 * 1024);
        var random = std.Random.DefaultPrng.init(312);
        for (large) |*byte| byte.* = 'a' + random.random().uintLessThan(u8, 26);
        var writes: [128]types.BatchWrite = undefined;
        var updates: [64]types.BatchWrite = undefined;
        for (&writes, 0..) |*write, i| {
            write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = try std.fmt.allocPrint(scratch, "{{\"n\":0,\"payload\":\"{s}\"}}", .{if (i == 1) large else "small"}) };
            if (i % 2 == 0) updates[i / 2] = .{ .key = write.key, .value = "{\"n\":1,\"payload\":\"small\"}" };
        }
        try db.batch(.{ .writes = &writes });
        try drainTestRelationalMaintenance(&db);
        const before = db.relational_column_maintenance.snapshot();
        try db.batch(.{ .writes = &updates });
        try drainTestRelationalMaintenance(&db);
        const after = db.relational_column_maintenance.snapshot();
        try std.testing.expect(after.cell_slots_examined - before.cell_slots_examined <= 2 * 128);
        try std.testing.expect(after.payload_bytes_written - before.payload_bytes_written < 4096);
        try std.testing.expect(after.payloads_reused > before.payloads_reused);
        try relational_columns.validatePayloadOwnershipForTest(&db, alloc);
        // A full-row scalar update must also deduplicate the unchanged wide
        // singleton payload instead of rewriting it under a new block key.
        const wide_update = try std.fmt.allocPrint(scratch, "{{\"n\":9,\"payload\":\"{s}\"}}", .{large});
        try db.batch(.{ .writes = &.{.{ .key = "k0001", .value = wide_update }} });
        try drainTestRelationalMaintenance(&db);
        try std.testing.expect(db.relational_column_maintenance.payload_bytes_written.load(.monotonic) - after.payload_bytes_written < 4096);
        try std.testing.expect(db.relational_column_maintenance.payload_encoding_bytes.load(.monotonic) - after.payload_encoding_bytes < 4096);
        // Canceled staging owns references until durable GC removes its column
        // metadata. Both the unpublished and drained states must balance.
        const Cancel = struct {
            fn run(_: *anyopaque) anyerror!void {
                return error.Canceled;
            }
        };
        try db.batch(.{ .writes = &.{.{ .key = "k0000", .value = "{\"n\":2,\"payload\":\"small\"}" }} });
        relational_columns.test_before_publish = .{ .context = &db, .run = Cancel.run };
        defer relational_columns.test_before_publish = null;
        try std.testing.expectError(error.Canceled, db.rebuildRelationalColumns());
        relational_columns.test_before_publish = null;
        try relational_columns.validatePayloadOwnershipForTest(&db, alloc);
        try drainTestRelationalMaintenance(&db);
        try relational_columns.validatePayloadOwnershipForTest(&db, alloc);
        const options: types.ScanOptions = .{ .include_documents = true, .include_all_fields = false, .fields = &.{ "n", "payload" } };
        var baseline = try db.scan(alloc, "", "", options);
        defer baseline.deinit(alloc);
        const Snapshot = struct {
            db: *DB,
            documents: []const types.ScanDocument,
            index: usize = 0,
            fn visit(ptr: ?*anyopaque, entry: types.ScanVisitEntry) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr.?));
                if (self.index == 0) {
                    try self.db.batch(.{ .deletes = &.{"k0001"} });
                    try drainTestRelationalMaintenance(self.db);
                }
                try std.testing.expectEqualStrings(self.documents[self.index].json, entry.document_json.?);
                self.index += 1;
            }
        };
        var snapshot = Snapshot{ .db = &db, .documents = baseline.documents };
        try db.scanVisit(alloc, "", "", options, .{ .context = &snapshot, .visit = Snapshot.visit });
        try std.testing.expectEqual(baseline.documents.len, snapshot.index);
        try relational_columns.validatePayloadOwnershipForTest(&db, alloc);
        db.close();
        db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        try relational_columns.validatePayloadOwnershipForTest(&db, alloc);
        var reopened = try db.scan(alloc, "", "", options);
        defer reopened.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 127), reopened.documents.len);
    }
}

test "relational columnar row cursor skips artifact fanout and preserves binary owners" {
    const alloc = std.testing.allocator;
    // Exercise both an uninterrupted bootstrap and deterministic partial
    // publication. Small published ranges may then be merged by maintenance.
    relational_columns.test_disable_deadline = true;
    defer relational_columns.test_disable_deadline = false;
    defer relational_columns.test_owner_limit = null;
    for ([_]?usize{ null, 2 }) |owner_limit| {
        for ([_]PrimaryBackend{.{ .lsm = .{ .flush_threshold = 1 } }}) |backend| {
            relational_columns.test_owner_limit = owner_limit;
            const t = try TestDb.open(alloc, .{ .start_optional_runtimes = false, .primary_backend = backend });
            defer t.deinit();
            const db = &t.db;
            const columns = [_]schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
            try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
            const owners = [_][]const u8{ "", "a", "a\x00", "a\xff", "orphan" };
            for (owners[0..4]) |owner| try db.batch(.{ .writes = &.{.{ .key = owner, .value = "{\"n\":1}" }} });
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const scratch = arena.allocator();
            var batch = try db.core.store.beginWriteBatch();
            var live = true;
            defer if (live) batch.abort();
            for (owners) |owner| {
                const prefix_key = try internal_keys.artifactRootPrefixAlloc(scratch, owner);
                for (0..2048) |i| try batch.asTxn().put(try std.fmt.allocPrint(scratch, "{s}{d:0>4}", .{ prefix_key, i }), "artifact payload is never a row");
            }
            try batch.commit();
            live = false;
            var stats: types.ColumnarScanStats = .{};
            var primary = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = true, .columnar_stats = &stats });
            defer primary.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 4), primary.documents.len);
            try std.testing.expectEqual(@as(u64, 5), stats.primary_owners_examined);
            for (primary.documents, owners[0..4]) |document, owner| try std.testing.expectEqualStrings(owner, document.id);
            stats = .{};
            var bounded = try db.scan(alloc, "a", "a\xff", .{ .include_documents = true, .include_all_fields = true, .inclusive_from = false, .exclusive_to = true, .columnar_stats = &stats });
            defer bounded.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 1), bounded.documents.len);
            try std.testing.expectEqualStrings("a\x00", bounded.documents[0].id);
            if (owner_limit) |limit| {
                try std.testing.expect(try db.rebuildRelationalColumns());
                try std.testing.expectEqual(@as(u64, limit), db.relational_column_maintenance.owners_examined.load(.monotonic));
                // Force a bootstrap split, then restore the normal merge budget.
                relational_columns.test_owner_limit = null;
            }
            try drainTestRelationalMaintenance(db);
            const maintenance = db.relational_column_maintenance.snapshot();
            try std.testing.expectEqual(@as(u64, 4), maintenance.primary_rows_read);
            // Owner visits include typed rows revisited by post-bootstrap merges;
            // only the five primary owners may be scanned, regardless of fanout.
            try std.testing.expectEqual(@as(u64, 5) + maintenance.covered_rows_read, maintenance.owners_examined);
            if (owner_limit == null) {
                try std.testing.expectEqual(@as(u64, 1), maintenance.bootstrap_quanta);
            } else {
                try std.testing.expect(maintenance.bootstrap_quanta > 1);
                try std.testing.expect(maintenance.covered_rows_read > 0);
            }
            var covered = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"} });
            defer covered.deinit(alloc);
            try std.testing.expectEqualDeep(primary.documents, covered.documents);
        }
    }
}

test "relational columnar bootstrap yields across artifact-only owners" {
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |leading_row| {
        var path_tmp = try TestDirectory.init("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
        defer db.close();
        const columns = [_]schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
        try db.batch(.{ .writes = &.{.{ .key = "z", .value = "{\"n\":1}" }} });
        if (leading_row) try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"n\":1}" }} });
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        var batch = try db.core.store.beginWriteBatch();
        var live = true;
        defer if (live) batch.abort();
        for (0..1500) |i| {
            const owner = try std.fmt.allocPrint(scratch, "o{d:0>4}", .{i});
            try batch.asTxn().put(try internal_keys.artifactRootPrefixAlloc(scratch, owner), "orphan");
        }
        try batch.commit();
        live = false;
        try std.testing.expect(try db.rebuildRelationalColumns());
        try std.testing.expect(db.relational_column_maintenance.owners_examined.load(.monotonic) <= 1024);
        try std.testing.expectEqual(@as(u64, @intFromBool(leading_row)), db.relational_column_maintenance.rows_written.load(.monotonic));
        var partial = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"} });
        defer partial.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 1) + @intFromBool(leading_row), partial.documents.len);
        try std.testing.expectEqualStrings("z", partial.documents[partial.documents.len - 1].id);
        db.close();
        db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
        try drainTestRelationalMaintenance(&db);
        var scan_stats: types.ColumnarScanStats = .{};
        var complete = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"}, .columnar_stats = &scan_stats });
        defer complete.deinit(alloc);
        try std.testing.expectEqualDeep(partial.documents, complete.documents);
        try std.testing.expectEqual(@as(u64, 0), scan_stats.primary_owners_examined);
    }
}

test "relational columnar scheduler batches deferred discovery before ready work and persists timers" {
    // Keep leak checks and failure injection; allocation backtraces are opt-in.
    var allocator_state: std.heap.SafeAllocator = .init(std.heap.page_allocator, .{ .stack_trace_frames = 0 });
    defer std.debug.assert(allocator_state.deinit() == 0);
    const alloc = if (platform.env.getenvBool("ANTFLY_TEST_ALLOCATOR_TRACES")) std.testing.allocator else allocator_state.allocator();
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    defer db.close();
    const columns = [_]schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const writes = try scratch.alloc(types.BatchWrite, 32 * 256);
    for (writes, 0..) |*write, i| write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>5}", .{i}), .value = "{\"n\":1}" };
    try db.batch(.{ .writes = writes });
    try drainTestRelationalMaintenance(&db);
    var updates = std.ArrayListUnmanaged(types.BatchWrite).empty;
    for (0..31) |i| try updates.append(scratch, .{ .key = writes[i * 256].key, .value = "{\"n\":2}" });
    for (writes[31 * 256 ..][0..64]) |write| try updates.append(scratch, .{ .key = write.key, .value = "{\"n\":2}" });
    try db.batch(.{ .writes = updates.items });
    relational_columns.test_now_ns = 100 * std.time.ns_per_s;
    defer relational_columns.test_now_ns = null;
    const before = db.relational_column_maintenance.blocks_written.load(.monotonic);
    var turns: usize = 0;
    while (db.relational_column_maintenance.blocks_written.load(.monotonic) == before) : (turns += 1) {
        try std.testing.expect(turns < 8);
        _ = try db.runRelationalColumnMaintenancePass();
    }
    try std.testing.expect(db.relational_column_maintenance.scheduler_candidates.load(.monotonic) >= 32);
    try std.testing.expect(db.relational_column_maintenance.scheduler_commits.load(.monotonic) < 16);
    // Finish discovery, but not the deferred builds. Repeated idle checks must
    // not rewrite the cursor or timer records for the unchanged backlog.
    for (0..8) |_| _ = try db.runRelationalColumnMaintenancePass();
    const commits = db.relational_column_maintenance.scheduler_commits.load(.monotonic);
    for (0..4) |_| _ = try db.runRelationalColumnMaintenancePass();
    try std.testing.expectEqual(commits, db.relational_column_maintenance.scheduler_commits.load(.monotonic));
    try std.testing.expect(db.relational_column_maintenance.pending.load(.acquire));
    db.close();
    db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    relational_columns.test_now_ns = 105 * std.time.ns_per_s;
    for (0..4) |_| _ = try db.runRelationalColumnMaintenancePass();
    try std.testing.expectEqual(@as(u64, 0), db.relational_column_maintenance.blocks_written.load(.monotonic));
    relational_columns.test_now_ns = 111 * std.time.ns_per_s;
    for (0..64) |_| {
        _ = try db.runRelationalColumnMaintenancePass();
        if (!db.relational_column_maintenance.pending.load(.acquire)) break;
    }
    try std.testing.expect(!db.relational_column_maintenance.pending.load(.acquire));
    try std.testing.expect(db.relational_column_maintenance.blocks_written.load(.monotonic) >= 31);
}

test "relational columnar scheduler keeps merge queue separate from due timers" {
    const alloc = std.testing.allocator;
    const t = try TestDb.open(alloc, .{ .start_optional_runtimes = false });
    defer t.deinit();
    const db = &t.db;
    const columns = [_]schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const writes = try scratch.alloc(types.BatchWrite, 300);
    for (writes, 0..) |*write, i| write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = "{\"n\":1}" };
    try db.batch(.{ .writes = writes });
    // Bootstrap leaves an underfilled tail on the merge queue. Adaptive
    // discovery must not decode that queue record as a deferred-range timer.
    for (0..64) |_| {
        _ = try db.runRelationalColumnMaintenancePass();
        if (!db.relational_column_maintenance.pending.load(.acquire)) break;
    }
    try std.testing.expect(!db.relational_column_maintenance.pending.load(.acquire));
    var result = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"} });
    defer result.deinit(alloc);
    try std.testing.expectEqual(writes.len, result.documents.len);
}

test "relational columnar bootstrap checkpoints coverage with fresh snapshots across restart" {
    const alloc = std.testing.allocator;
    for ([_]PrimaryBackend{.{ .lsm = .{ .flush_threshold = 1 } }}) |backend| {
        var path_tmp = try TestDirectory.init("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        defer db.close();
        const columns = [_]schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        const writes = try scratch.alloc(types.BatchWrite, 600);
        for (writes, 0..) |*write, i| write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = "{\"n\":1}" };
        try db.batch(.{ .writes = writes });
        // Model authoritative rows predating column coverage/dirty records.
        var batch = try db.core.store.beginWriteBatch();
        var live = true;
        defer if (live) batch.abort();
        for (writes) |write| try batch.asTxn().delete(try std.mem.concat(scratch, u8, &.{ internal_keys.relational_columnar_dirty_prefix, write.key }));
        try batch.commit();
        live = false;
        relational_columns.test_compaction_block_limit = 1;
        defer relational_columns.test_compaction_block_limit = null;
        const Race = struct {
            fn run(ptr: *anyopaque) !void {
                relational_columns.test_before_publish = null;
                const owner: *DB = @ptrCast(@alignCast(ptr));
                try owner.batch(.{ .writes = &.{ .{ .key = "aa", .value = "{\"n\":9}" }, .{ .key = "k0002", .value = "{\"n\":9}" }, .{ .key = "k0599", .value = "{\"n\":9}" } } });
            }
        };
        relational_columns.test_before_publish = .{ .context = &db, .run = Race.run };
        defer relational_columns.test_before_publish = null;
        try std.testing.expect(try db.rebuildRelationalColumns());
        const first_rows = db.relational_column_maintenance.rows_written.load(.monotonic);
        try std.testing.expect(first_rows > 0 and first_rows <= 256);
        var stats: types.ColumnarScanStats = .{};
        var partial = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"}, .columnar_stats = &stats });
        defer partial.deinit(alloc);
        var primary = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = true });
        defer primary.deinit(alloc);
        try std.testing.expectEqualDeep(primary.documents, partial.documents);
        try std.testing.expectEqual(@as(usize, 601), partial.documents.len);
        try std.testing.expectEqual(@as(u64, 1), stats.uncovered_ranges_read);
        const Cancel = struct {
            fn run(_: *anyopaque) !void {
                relational_columns.test_before_publish = null;
                return error.Canceled;
            }
        };
        relational_columns.test_before_publish = .{ .context = &db, .run = Cancel.run };
        try std.testing.expectError(error.Canceled, db.rebuildRelationalColumns());
        db.close();
        db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        // Reclaim canceled staging, then advance from the checkpoint, never
        // the prefix. The time budget may yield before the 256-row block cap.
        var recovery_passes: usize = 0;
        while (db.relational_column_maintenance.bootstrap_quanta.load(.monotonic) == 0) : (recovery_passes += 1) {
            try std.testing.expect(recovery_passes < 10);
            try std.testing.expect(try db.rebuildRelationalColumns());
        }
        const resumed_rows = db.relational_column_maintenance.rows_written.load(.monotonic);
        try std.testing.expect(resumed_rows > 0 and resumed_rows <= 256);
        try drainTestRelationalMaintenance(&db);
        stats = .{};
        var complete = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"}, .columnar_stats = &stats });
        defer complete.deinit(alloc);
        try std.testing.expectEqualDeep(primary.documents, complete.documents);
        try std.testing.expectEqual(@as(u64, 0), stats.uncovered_ranges_read);
        try std.testing.expectEqual(@as(u64, 0), stats.primary_rows_read);
    }
}

test "relational columnar missing bootstrap checkpoint cannot claim complete coverage" {
    const alloc = std.testing.allocator;
    const t = try TestDb.open(alloc, .{ .start_optional_runtimes = false });
    defer t.deinit();
    const db = &t.db;
    const columns = [_]schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const writes = try scratch.alloc(types.BatchWrite, 300);
    for (writes, 0..) |*write, i| write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = "{\"n\":1}" };
    try db.batch(.{ .writes = writes });
    relational_columns.test_compaction_block_limit = 1;
    defer relational_columns.test_compaction_block_limit = null;
    try std.testing.expect(try db.rebuildRelationalColumns());
    try db.core.store.delete("\x00\x00__columnar__:bootstrap");
    var result = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"} });
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 300), result.documents.len);
    try std.testing.expect(db.relational_columns_rebuild_requested.load(.acquire));
    try drainTestRelationalMaintenance(db);
    try std.testing.expect(!db.relational_columns_rebuild_requested.load(.acquire));
}

test "relational columnar byte costing retains wide clean rows and switches for dense replacement" {
    var checked_allocator = try @import("antfly_platform").allocator.CheckedTestAllocator.init();
    defer checked_allocator.deinit();
    const alloc = checked_allocator.allocator();
    const t = try TestDb.open(alloc, .{ .start_optional_runtimes = false });
    defer t.deinit();
    const db = &t.db;
    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "n", .path = "n", .column_type = .integer },
        .{ .name = "blob", .path = "blob", .column_type = .string },
    };
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const blob = try scratch.alloc(u8, 8192);
    @memset(blob, 'x');
    const json = try std.fmt.allocPrint(scratch, "{{\"n\":1,\"blob\":\"{s}\"}}", .{blob});
    const writes = try scratch.alloc(types.BatchWrite, 256);
    const inserts = try scratch.alloc(types.BatchWrite, 256);
    for (writes, inserts, 0..) |*write, *insert, i| {
        write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = json };
        insert.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}a", .{i}), .value = "{\"n\":2}" };
    }
    try db.batch(.{ .writes = writes });
    try drainTestRelationalMaintenance(db);
    try db.batch(.{ .writes = inserts });
    var stats: types.ColumnarScanStats = .{};
    var narrow = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"}, .columnar_stats = &stats });
    defer narrow.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 512), narrow.documents.len);
    try std.testing.expectEqual(@as(u64, 0), stats.dense_delta_scans);
    try std.testing.expectEqual(@as(u64, 256), stats.primary_rows_read);
    try std.testing.expect(stats.estimated_primary_bytes > stats.estimated_overlay_bytes);
    for (writes) |*write| write.value = "{\"n\":2}";
    try db.batch(.{ .writes = writes });
    stats = .{};
    var dense = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"}, .columnar_stats = &stats });
    defer dense.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 512), dense.documents.len);
    try std.testing.expect(stats.dense_delta_scans > 0);
    try std.testing.expect(stats.estimated_primary_bytes < stats.estimated_overlay_bytes);
    for (dense.documents) |document| try std.testing.expectEqualStrings("{\"n\":2}", document.json);
    try drainTestRelationalMaintenance(db);
    const burst = try scratch.alloc(types.BatchWrite, 2050);
    for (burst, 0..) |*write, i| write.* = .{ .key = try std.fmt.allocPrint(scratch, "a{d:0>4}", .{i}), .value = "{\"n\":3}" };
    try db.batch(.{ .writes = burst });
    stats = .{};
    var large = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"}, .columnar_stats = &stats });
    defer large.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2562), large.documents.len);
    try std.testing.expect(stats.dense_delta_scans > 0);
    try std.testing.expectEqual(@as(u64, 1024), stats.costed_dirty_records);
}

test "relational columnar adaptive admission preserves age across hot updates and restart" {
    // Assert payload/admission semantics independently of the real-time build
    // quantum. Bounded/resumable maintenance is tested separately.
    relational_columns.test_disable_deadline = true;
    defer relational_columns.test_disable_deadline = false;
    const alloc = std.testing.allocator;
    const Progress = struct {
        fn expectBuild(db: *DB, before: u64) !void {
            // Admission is time-sliced. A pass may checkpoint discovery before
            // it builds, especially under concurrent compiler/CI load. Keep
            // the synthetic age clock frozen while allowing bounded progress.
            for (0..16) |_| {
                _ = try db.runRelationalColumnMaintenancePass();
                if (db.relational_column_maintenance.blocks_written.load(.monotonic) > before) return;
            }
            try std.testing.expect(db.relational_column_maintenance.blocks_written.load(.monotonic) > before);
        }
    };
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    defer db.close();
    const columns = [_]schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const writes = try scratch.alloc(types.BatchWrite, 256);
    for (writes, 0..) |*write, i| write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = "{\"n\":1}" };
    try db.batch(.{ .writes = writes });
    try drainTestRelationalMaintenance(&db);
    relational_columns.test_now_ns = 100 * std.time.ns_per_s;
    defer relational_columns.test_now_ns = null;
    const before = db.relational_column_maintenance.blocks_written.load(.monotonic);
    for (0..4) |i| {
        try db.batch(.{ .writes = &.{.{ .key = "k0000", .value = try std.fmt.allocPrint(scratch, "{{\"n\":{d}}}", .{i + 2}) }} });
        _ = try db.runRelationalColumnMaintenancePass();
    }
    try std.testing.expectEqual(before, db.relational_column_maintenance.blocks_written.load(.monotonic));
    try std.testing.expect(db.relational_column_maintenance.ranges_deferred.load(.monotonic) > 0);
    db.close();
    db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    relational_columns.test_now_ns = 111 * std.time.ns_per_s;
    try Progress.expectBuild(&db, 0);
    // Read pressure can admit another sparse change before the age cap.
    try db.batch(.{ .writes = &.{.{ .key = "k0000", .value = "{\"n\":7}" }} });
    for (0..20) |_| {
        var result = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"}, .limit = 1 });
        result.deinit(alloc);
    }
    const warm_before = db.relational_column_maintenance.blocks_written.load(.monotonic);
    try Progress.expectBuild(&db, warm_before);
    // High density bypasses age admission without any read-pressure hint.
    for (writes[0..64]) |*write| write.value = "{\"n\":9}";
    try db.batch(.{ .writes = writes[0..64] });
    const dense_before = db.relational_column_maintenance.blocks_written.load(.monotonic);
    try Progress.expectBuild(&db, dense_before);
    // A backwards clock correction must not strand durable deferral records.
    // Use a fresh clean range: prior read pressure and density-driven partial
    // page rewrites may legitimately admit another write without deferring it.
    var skew_tmp = try TestDirectory.init("db");
    defer skew_tmp.cleanup();
    const skew_path = skew_tmp.path().ptr;
    var skew_db = try DB.open(alloc, std.mem.span(skew_path), .{ .start_optional_runtimes = false });
    defer skew_db.close();
    try skew_db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    for (writes) |*write| write.value = "{\"n\":1}";
    try skew_db.batch(.{ .writes = writes });
    try drainTestRelationalMaintenance(&skew_db);
    try skew_db.batch(.{ .writes = &.{.{ .key = "k0000", .value = "{\"n\":10}" }} });
    const deferred_before = skew_db.relational_column_maintenance.ranges_deferred.load(.monotonic);
    for (0..16) |_| {
        _ = try skew_db.runRelationalColumnMaintenancePass();
        if (skew_db.relational_column_maintenance.ranges_deferred.load(.monotonic) > deferred_before) break;
    }
    // Do not move the clock back until a deferral exists at the old time.
    // Otherwise a yielded pass can first create it at 110s, which is not skew.
    try std.testing.expect(skew_db.relational_column_maintenance.ranges_deferred.load(.monotonic) > deferred_before);
    const skew_before = skew_db.relational_column_maintenance.blocks_written.load(.monotonic);
    relational_columns.test_now_ns = 110 * std.time.ns_per_s;
    try Progress.expectBuild(&skew_db, skew_before);
}

test "relational columnar overlays merge mutations in order without scanning clean primary rows" {
    const alloc = std.testing.allocator;
    for ([_]PrimaryBackend{.{ .lsm = .{ .flush_threshold = 1 } }}) |backend| {
        var path_tmp = try TestDirectory.init("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        defer db.close();
        const columns = [_]schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        const writes = try scratch.alloc(types.BatchWrite, 300);
        for (writes, 0..) |*write, i| write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = "{\"n\":1}" };
        try db.batch(.{ .writes = writes });
        try drainTestRelationalMaintenance(&db);
        try db.batch(.{ .writes = &.{ .{ .key = "k0005", .value = "{\"n\":999}" }, .{ .key = "k0005a", .value = "{\"n\":888}" }, .{ .key = "", .value = "{\"n\":777}" }, .{ .key = "z", .value = "{\"n\":666}" } }, .deletes = &.{"k0006"} });
        for ([_][]const u8{ "", "{\"numeric_range\":{\"field\":\"n\",\"min\":500}}", "{\"term\":{\"n\":1}}" }) |filter| {
            var stats: types.ColumnarScanStats = .{};
            var primary = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = true, .filter_query_json = filter, .include_content_hashes = true });
            defer primary.deinit(alloc);
            var overlay = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"}, .filter_query_json = filter, .include_content_hashes = true, .columnar_stats = &stats });
            defer overlay.deinit(alloc);
            try std.testing.expectEqualDeep(primary.documents, overlay.documents);
            try std.testing.expectEqualDeep(primary.hashes, overlay.hashes);
            try std.testing.expectEqual(@as(u64, 4), stats.primary_rows_read);
            try std.testing.expectEqual(@as(u64, 5), stats.overlay_rows_read);
            try std.testing.expectEqual(@as(u64, 2), stats.blocks_read);
        }
        var page = try db.scan(alloc, "k0004", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"}, .limit = 2 });
        defer page.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 2), page.documents.len);
        try std.testing.expectEqualStrings("k0005", page.documents[0].id);
        try std.testing.expectEqualStrings("k0005a", page.documents[1].id);
        var next = try db.scan(alloc, page.documents[1].id, "k0008", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"}, .limit = 2 });
        defer next.deinit(alloc);
        try std.testing.expectEqualStrings("k0007", next.documents[0].id);
        try std.testing.expectEqualStrings("k0008", next.documents[1].id);
        const SnapshotProbe = struct {
            db: *DB,
            count: usize = 0,
            saw_old: bool = false,
            fn visit(ptr: ?*anyopaque, entry: types.ScanVisitEntry) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr.?));
                self.count += 1;
                if (self.count == 1) try self.db.batch(.{ .writes = &.{.{ .key = "k0005", .value = "{\"n\":1000}" }} });
                if (std.mem.eql(u8, entry.id, "k0005")) {
                    try std.testing.expectEqualStrings("{\"n\":999}", entry.document_json.?);
                    self.saw_old = true;
                }
            }
        };
        var probe = SnapshotProbe{ .db = &db };
        try db.scanVisit(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"} }, .{ .context = &probe, .visit = SnapshotProbe.visit });
        try std.testing.expect(probe.saw_old);
        try std.testing.expectEqual(@as(usize, 302), probe.count);
        try db.batch(.{ .writes = writes });
        var dense_stats: types.ColumnarScanStats = .{};
        var dense = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"}, .columnar_stats = &dense_stats });
        defer dense.deinit(alloc);
        var authoritative = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = true });
        defer authoritative.deinit(alloc);
        try std.testing.expectEqualDeep(authoritative.documents, dense.documents);
        try std.testing.expect(dense_stats.dense_delta_scans > 0);
        dense_stats = .{};
        var short = try db.scan(alloc, "", "", .{ .limit = 2, .columnar_stats = &dense_stats });
        defer short.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 2), short.hashes.len);
        try std.testing.expectEqual(@as(u64, 0), dense_stats.dense_delta_scans);
    }
}

test "relational columnar decoded cache preserves snapshots and releases visitor failures" {
    const alloc = std.testing.allocator;
    relational_columns.test_disable_deadline = true;
    defer relational_columns.test_disable_deadline = false;
    for ([_]PrimaryBackend{.{ .lsm = .{ .flush_threshold = 1 } }}) |backend| {
        var path_tmp = try TestDirectory.init("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        defer db.close();
        const columns = [_]schema_mod.RelationalColumn{.{ .name = "payload", .path = "payload", .column_type = .string }};
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        const writes = try scratch.alloc(types.BatchWrite, 1024);
        for (writes, 0..) |*write, i| write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = "{\"payload\":\"original\"}" };
        try db.batch(.{ .writes = writes });
        try drainTestRelationalMaintenance(&db);
        const Visitor = struct {
            db: *DB,
            seen: usize = 0,
            mutate: bool = false,
            fail: bool = false,
            cancel: ?*std.atomic.Value(bool) = null,
            fn visit(ptr: ?*anyopaque, row: types.ScanVisitEntry) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr.?));
                if (self.mutate and self.seen == 0) {
                    try self.db.batch(.{ .writes = &.{.{ .key = "k1023", .value = "{\"payload\":\"changed\"}" }} });
                    try drainTestRelationalMaintenance(self.db);
                }
                try std.testing.expectEqualStrings("{\"payload\":\"original\"}", row.document_json.?);
                self.seen += 1;
                if (self.fail and self.seen == 257) return error.TestVisitorFailed;
                if (self.seen == 257) if (self.cancel) |flag| flag.store(true, .release);
            }
        };
        var measured = std.testing.FailingAllocator.init(alloc, .{});
        var stats: types.ColumnarScanStats = .{};
        var visitor: Visitor = .{ .db = &db, .mutate = true };
        try db.scanVisit(measured.allocator(), "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"payload"}, .columnar_stats = &stats }, .{ .context = &visitor, .visit = Visitor.visit });
        try std.testing.expectEqual(@as(usize, 1024), visitor.seen);
        try std.testing.expectEqual(@as(u64, 3), stats.decoded_cache_hits);
        try std.testing.expectEqual(@as(u64, 1), stats.payload_pages_read);
        try std.testing.expectEqual(measured.allocated_bytes, measured.freed_bytes);
        // The next snapshot must observe the replacement, not the old cache.
        var latest = try db.scan(alloc, "k1023", "k1023", .{ .inclusive_from = true, .include_documents = true, .include_all_fields = false, .fields = &.{"payload"} });
        defer latest.deinit(alloc);
        try std.testing.expectEqualStrings("{\"payload\":\"changed\"}", latest.documents[0].json);
        visitor = .{ .db = &db, .fail = true };
        stats = .{};
        try std.testing.expectError(error.TestVisitorFailed, db.scanVisit(measured.allocator(), "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"payload"}, .columnar_stats = &stats }, .{ .context = &visitor, .visit = Visitor.visit }));
        try std.testing.expectEqual(@as(usize, 257), visitor.seen);
        try std.testing.expect(stats.decoded_cache_hits > 0);
        try std.testing.expectEqual(measured.allocated_bytes, measured.freed_bytes);
        var cancelled = std.atomic.Value(bool).init(false);
        visitor = .{ .db = &db, .cancel = &cancelled };
        stats = .{};
        try std.testing.expectError(error.Canceled, db.scanVisit(measured.allocator(), "", "", .{
            .include_documents = true,
            .include_all_fields = false,
            .fields = &.{"payload"},
            .columnar_stats = &stats,
            .cancellation = types.CancellationToken.fromAtomic(&cancelled),
        }, .{ .context = &visitor, .visit = Visitor.visit }));
        try std.testing.expectEqual(@as(usize, 257), visitor.seen);
        try std.testing.expect(stats.decoded_cache_hits > 0);
        try std.testing.expectEqual(measured.allocated_bytes, measured.freed_bytes);
        try relational_columns.validatePayloadOwnershipForTest(&db, alloc);
    }
}

test "relational columnar JSON numeric predicates preserve document semantics" {
    const alloc = std.testing.allocator;
    for ([_]PrimaryBackend{.{ .lsm = .{ .flush_threshold = 1 } }}) |backend| {
        const t = try TestDb.open(alloc, .{ .start_optional_runtimes = false, .primary_backend = backend });
        defer t.deinit();
        const db = &t.db;
        const columns = [_]schema_mod.RelationalColumn{.{ .name = "payload", .path = "payload", .column_type = .json, .is_json = true, .json_kind = .any }};
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
        try db.batch(.{ .writes = &.{
            .{ .key = "a", .value = "{\"payload\":0.10000000000000001}" },
            .{ .key = "b", .value = "{\"payload\":{\"n\":0.10000000000000001}}" },
            .{ .key = "c", .value = "{\"payload\":{\"n\":9007199254740993}}" },
        } });
        try drainTestRelationalMaintenance(db);
        for ([_][]const u8{
            "{\"term\":{\"payload\":0.1}}",
            "{\"term\":{\"payload.n\":0.1}}",
            "{\"term\":{\"/payload/n\":9007199254740993}}",
        }) |query| {
            var stats: types.ColumnarScanStats = .{};
            var opts = types.ScanOptions{ .include_documents = true, .include_all_fields = false, .fields = &.{"payload"}, .filter_query_json = query, .columnar_stats = &stats };
            var actual = try db.scan(alloc, "", "", opts);
            defer actual.deinit(alloc);
            try std.testing.expect(stats.used);
            try std.testing.expectEqual(@as(usize, 1), actual.documents.len);
            opts.disable_columnar_scan = true;
            opts.columnar_stats = null;
            var expected = try db.scan(alloc, "", "", opts);
            defer expected.deinit(alloc);
            try std.testing.expectEqualDeep(expected.documents, actual.documents);
        }
    }
}

test "relational columnar bound selection and late projection match primary semantics" {
    try testColumnarSelection(true);
}

test "relational columnar projection matrix matches primary semantics on small fixtures" {
    try testColumnarSelection(false);
}

test "relational columnar late materialization pins snapshots and releases visitor failures" {
    const alloc = std.testing.allocator;
    relational_columns.test_disable_deadline = true;
    defer relational_columns.test_disable_deadline = false;
    for ([_]PrimaryBackend{.{ .lsm = .{ .flush_threshold = 1 } }}) |backend| {
        var path_tmp = try TestDirectory.init("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        defer db.close();
        try seedColumnScanPlanTest(&db, alloc);
        const Visitor = struct {
            db: *DB,
            seen: usize = 0,
            mutate: bool = false,
            fail: bool = false,
            cancelled: ?*std.atomic.Value(bool) = null,
            fn visit(ptr: ?*anyopaque, entry: types.ScanVisitEntry) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr.?));
                if (self.mutate and self.seen == 0) {
                    try self.db.batch(.{ .writes = &.{.{ .key = "k0767", .value = "{\"n\":999,\"user-name\":\"changed\"}" }} });
                    try drainTestRelationalMaintenance(self.db);
                }
                try std.testing.expect(std.mem.indexOf(u8, entry.document_json.?, "changed") == null);
                self.seen += 1;
                if (self.seen == 2) {
                    if (self.fail) return error.NotFound;
                    if (self.cancelled) |flag| flag.store(true, .release);
                }
            }
        };
        var measured = std.testing.FailingAllocator.init(alloc, .{});
        var stats: types.ColumnarScanStats = .{};
        var opts = types.ScanOptions{ .include_documents = true, .limit = 8, .filter_query_json = "{\"numeric_range\":{\"field\":\"n\",\"min\":760,\"max\":768}}", .columnar_stats = &stats };
        var visitor = Visitor{ .db = &db, .mutate = true };
        try db.scanVisit(measured.allocator(), "", "", opts, .{ .context = &visitor, .visit = Visitor.visit });
        try std.testing.expectEqual(@as(usize, 8), visitor.seen);
        try std.testing.expectEqual(@as(u64, 8), stats.late_materialized_rows);
        try std.testing.expectEqual(measured.allocated_bytes, measured.freed_bytes);
        visitor = .{ .db = &db, .fail = true };
        try std.testing.expectError(error.NotFound, db.scanVisit(measured.allocator(), "", "", opts, .{ .context = &visitor, .visit = Visitor.visit }));
        try std.testing.expectEqual(@as(usize, 2), visitor.seen);
        try std.testing.expectEqual(measured.allocated_bytes, measured.freed_bytes);
        var cancelled = std.atomic.Value(bool).init(false);
        visitor = .{ .db = &db, .cancelled = &cancelled };
        opts.cancellation = types.CancellationToken.fromAtomic(&cancelled);
        try std.testing.expectError(error.Canceled, db.scanVisit(measured.allocator(), "", "", opts, .{ .context = &visitor, .visit = Visitor.visit }));
        try std.testing.expectEqual(@as(usize, 2), visitor.seen);
        try std.testing.expectEqual(measured.allocated_bytes, measured.freed_bytes);
    }
}

test "relational columnar sequential selection preserves dirty owners bounds and limits" {
    // Keep leak checks and failure injection; allocation backtraces are opt-in.
    var allocator_state: std.heap.SafeAllocator = .init(std.heap.page_allocator, .{ .stack_trace_frames = 0 });
    defer std.debug.assert(allocator_state.deinit() == 0);
    const alloc = if (platform.env.getenvBool("ANTFLY_TEST_ALLOCATOR_TRACES")) std.testing.allocator else allocator_state.allocator();
    relational_columns.test_disable_deadline = true;
    defer relational_columns.test_disable_deadline = false;
    for ([_]PrimaryBackend{.{ .lsm = .{ .flush_threshold = 1 } }}) |backend| {
        const t = try TestDb.open(alloc, .{ .start_optional_runtimes = false, .primary_backend = backend });
        defer t.deinit();
        const db = &t.db;
        try seedColumnScanPlanTest(db, alloc);
        try db.batch(.{ .writes = &.{
            .{ .key = "k0002", .value = "{\"n\":999}" },
            .{ .key = "k0765", .value = "{\"n\":1}" },
            .{ .key = "k0010a", .value = "{\"n\":3}" },
            .{ .key = "k0010b", .value = "{\"n\":999}" },
        }, .deletes = &.{"k0003"} });
        for ([_]u32{ 0, 16, 128, 500, 900 }) |limit| for ([_]bool{ false, true }) |exclusive_to| {
            var stats: types.ColumnarScanStats = .{};
            var opts = types.ScanOptions{
                .include_documents = true,
                .include_content_hashes = true,
                .filter_query_json = "{\"numeric_range\":{\"field\":\"n\",\"max\":750}}",
                .limit = limit,
                .exclusive_to = exclusive_to,
                .columnar_stats = &stats,
            };
            var actual = try db.scan(alloc, "k0001", "k0766", opts);
            defer actual.deinit(alloc);
            opts.disable_columnar_scan = true;
            var expected = try db.scan(alloc, "k0001", "k0766", opts);
            defer expected.deinit(alloc);
            try std.testing.expectEqualDeep(expected.documents, actual.documents);
            try std.testing.expectEqualDeep(expected.hashes, actual.hashes);
            if (limit == 0) {
                try std.testing.expect(stats.selection_reused_rows > 500);
                try std.testing.expectEqual(@as(u64, 4), stats.primary_predicate_rows);
                try std.testing.expectEqual(@as(u64, 1), stats.scan_plans_built);
            }
        };
    }
}

test "relational columnar sequential selection pins snapshots and releases failures" {
    const alloc = std.testing.allocator;
    relational_columns.test_disable_deadline = true;
    defer relational_columns.test_disable_deadline = false;
    for ([_]PrimaryBackend{.{ .lsm = .{ .flush_threshold = 1 } }}) |backend| {
        var path_tmp = try TestDirectory.init("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        defer db.close();
        const columns = [_]schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const writes = try arena.allocator().alloc(types.BatchWrite, 128);
        for (writes, 0..) |*write, i| write.* = .{ .key = try std.fmt.allocPrint(arena.allocator(), "k{d:0>4}", .{i}), .value = "{\"n\":1}" };
        try db.batch(.{ .writes = writes });
        try drainTestRelationalMaintenance(&db);
        const Visitor = struct {
            db: *DB,
            seen: usize = 0,
            mutate: bool = false,
            fail: bool = false,
            cancel: ?*std.atomic.Value(bool) = null,
            fn visit(ptr: ?*anyopaque, row: types.ScanVisitEntry) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr.?));
                if (self.mutate and self.seen == 0) {
                    try self.db.batch(.{ .writes = &.{.{ .key = "k0127", .value = "{\"n\":2}" }} });
                    try drainTestRelationalMaintenance(self.db);
                }
                try std.testing.expectEqualStrings("{\"n\":1}", row.document_json.?);
                self.seen += 1;
                if (self.seen == 2) {
                    if (self.fail) return error.NotFound;
                    if (self.cancel) |flag| flag.store(true, .release);
                }
            }
        };
        var measured = std.testing.FailingAllocator.init(alloc, .{});
        var stats: types.ColumnarScanStats = .{};
        var opts = types.ScanOptions{ .include_documents = true, .filter_query_json = "{\"term\":{\"n\":1}}", .columnar_stats = &stats };
        var visitor = Visitor{ .db = &db, .mutate = true };
        try db.scanVisit(measured.allocator(), "", "", opts, .{ .context = &visitor, .visit = Visitor.visit });
        try std.testing.expectEqual(@as(usize, 128), visitor.seen);
        try std.testing.expectEqual(@as(u64, 128), stats.selection_reused_rows);
        try std.testing.expectEqual(@as(u64, 0), stats.primary_predicate_rows);
        try std.testing.expectEqual(measured.allocated_bytes, measured.freed_bytes);
        visitor = .{ .db = &db, .fail = true };
        try std.testing.expectError(error.NotFound, db.scanVisit(measured.allocator(), "", "", opts, .{ .context = &visitor, .visit = Visitor.visit }));
        try std.testing.expectEqual(@as(usize, 2), visitor.seen);
        try std.testing.expectEqual(measured.allocated_bytes, measured.freed_bytes);
        var cancelled = std.atomic.Value(bool).init(false);
        visitor = .{ .db = &db, .cancel = &cancelled };
        opts.cancellation = types.CancellationToken.fromAtomic(&cancelled);
        try std.testing.expectError(error.Canceled, db.scanVisit(measured.allocator(), "", "", opts, .{ .context = &visitor, .visit = Visitor.visit }));
        try std.testing.expectEqual(@as(usize, 2), visitor.seen);
        try std.testing.expectEqual(measured.allocated_bytes, measured.freed_bytes);
    }
}

test "relational columnar bound scan has bounded plans and primary reads" {
    try testRelationalBoundScan(false);
}

test "relational columnar bound scan benchmark" {
    try testRelationalBoundScan(true);
}

test "relational columnar prepared ownership aggregates and aborts atomically" {
    const payloads = @import("column_payloads.zig");
    const alloc = std.testing.allocator;
    for ([_]PrimaryBackend{.{ .lsm = db_config.primary_lsm_options_default }}) |backend| {
        const t = try TestDb.open(alloc, .{ .start_optional_runtimes = false, .primary_backend = backend });
        defer t.deinit();
        const db = &t.db;
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        const digest = @as([32]u8, @splat(9));
        var encoded: [8]u8 = @splat(0);
        std.mem.writeInt(u32, encoded[4..8], std.hash.Crc32.hash(encoded[0..4]), .little);
        var deltas: [300]payloads.Delta = @splat(.{ .digest = digest, .bytes = encoded.len, .retains = 1 });
        deltas[299].encoded = &encoded;
        const Failure = struct {
            fn run(failing: Allocator, target: *docstore_mod.DocStore, changes: []const payloads.Delta) !void {
                var preparation = std.heap.ArenaAllocator.init(failing);
                defer preparation.deinit();
                _ = try payloads.prepare(target, preparation.allocator(), 1, changes);
            }
        };
        try platform.allocator.checkAllAllocationFailures(alloc, Failure.run, .{ db.core.store, @as([]const payloads.Delta, &deltas) });
        const prepared = try payloads.prepare(db.core.store, scratch, 1, &deltas);
        try std.testing.expectEqual(@as(usize, 2), prepared.writes.items.len);
        try std.testing.expectEqual(@as(u64, 299), prepared.shared);
        const count_key = try payloads.key(scratch, 1, digest, true);
        {
            var write = try db.core.store.beginWriteTxn();
            defer write.abort();
            try prepared.apply(&write);
        }
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, count_key));
        {
            var write = try db.core.store.beginWriteTxn();
            errdefer write.abort();
            try prepared.apply(&write);
            try write.commit();
        }
        const count_bytes = try db.core.store.get(alloc, count_key);
        defer alloc.free(count_bytes);
        try std.testing.expectEqual(@as(u64, 300), (try payloads.decodeCount(count_bytes)).references);
        try std.testing.expectError(error.InvalidColumnSegment, payloads.prepare(db.core.store, scratch, 1, &.{.{ .digest = digest, .releases = 301 }}));
        try std.testing.expectError(error.InvalidColumnSegment, payloads.prepare(db.core.store, scratch, 1, &.{.{ .digest = digest, .bytes = 900, .retains = 1 }}));
        const released = try payloads.prepare(db.core.store, scratch, 1, &.{.{ .digest = digest, .releases = 300 }});
        try std.testing.expectEqual(@as(usize, 2), released.deletes.items.len);
        {
            var write = try db.core.store.beginWriteTxn();
            errdefer write.abort();
            try released.apply(&write);
            try write.commit();
        }
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, count_key));
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, try payloads.key(scratch, 1, digest, false)));
    }
}

test "relational columnar production LSM staging snapshot benchmark" {
    var checked_allocator = try @import("antfly_platform").allocator.CheckedTestAllocator.init();
    defer checked_allocator.deinit();
    const alloc = checked_allocator.allocator();
    relational_columns.test_disable_deadline = true;
    defer relational_columns.test_disable_deadline = false;
    defer lsm_backend_mod.Backend.test_deep_mutable_snapshots = false;
    var clones: [2]u64 = undefined;
    var elapsed: [2]u64 = undefined;
    for (0..2) |mode| {
        lsm_backend_mod.Backend.test_deep_mutable_snapshots = mode == 0;
        const t = try TestDb.open(alloc, .{ .start_optional_runtimes = false });
        defer t.deinit();
        const db = &t.db;
        const columns = [_]schema_mod.RelationalColumn{.{ .name = "payload", .path = "payload", .column_type = .string }};
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        const wide = try scratch.alloc(u8, 4096);
        var random = std.Random.DefaultPrng.init(729);
        var writes: [1024]types.BatchWrite = undefined;
        for (&writes, 0..) |*write, i| {
            for (wide) |*byte| byte.* = 'a' + random.random().uintLessThan(u8, 26);
            write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = try std.fmt.allocPrint(scratch, "{{\"payload\":\"{s}\"}}", .{wide}) };
        }
        try db.batch(.{ .writes = &writes });
        const backend = db.core.primary_store_owner.lsmBackend().?;
        const before = backend.snapshotMaintenanceStats();
        const started = platform_time.monotonicNs();
        try drainTestRelationalMaintenance(db);
        elapsed[mode] = platform_time.monotonicNs() - started;
        clones[mode] = backend.snapshotMaintenanceStats().mutable_snapshot_clone_bytes_total - before.mutable_snapshot_clone_bytes_total;
        try std.testing.expect(db.relational_column_maintenance.blocks_written.load(.monotonic) >= 4);
        try relational_columns.validatePayloadOwnershipForTest(db, alloc);
        var result = try db.scan(alloc, "", "", .{});
        defer result.deinit(alloc);
        try std.testing.expectEqual(writes.len, result.hashes.len);
    }
    try std.testing.expect(clones[1] < clones[0]);
    std.debug.print("\nproduction LSM staging: deep/shared snapshot ns={d}/{d}, copied bytes={d}/{d}\n", .{ elapsed[0], elapsed[1], clones[0], clones[1] });
}

test "relational columnar production LSM physical churn benchmark" {
    try productionLsmPhysicalChurnBenchmark(0);
    try productionLsmPhysicalChurnBenchmark(50);
}

test "relational columnar production LSM batched cold block read benchmark" {
    const alloc = std.testing.allocator;
    const t = try TestDb.open(alloc, .{ .start_optional_runtimes = false });
    defer t.deinit();
    const db = &t.db;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    var columns: [32]schema_mod.RelationalColumn = undefined;
    var fields: [32][]const u8 = undefined;
    for (&columns, &fields, 0..) |*column, *field, i| {
        field.* = try std.fmt.allocPrint(scratch, "f{d:0>4}", .{i});
        column.* = .{ .name = field.*, .path = field.*, .column_type = .integer };
    }
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    var writes: [128]types.BatchWrite = undefined;
    for (&writes, 0..) |*write, row| {
        var object = std.json.ObjectMap.empty;
        for (fields, 0..) |field, i| try object.put(scratch, field, .{ .integer = @intCast(i * 10000 + row) });
        write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{row}), .value = try std.json.Stringify.valueAlloc(scratch, std.json.Value{ .object = object }, .{}) };
    }
    try db.batch(.{ .writes = &writes });
    try drainTestRelationalMaintenance(db);
    const backend = db.core.primary_store_owner.lsmBackend().?;
    try backend.sync(true);
    // No local/shared decoded-block cache. OS page-cache state is deliberately
    // unspecified; physical block loads/bytes are the deterministic gate.
    try std.testing.expect(!backend.options.local_block_cache_enabled and backend.options.cache == null);
    defer relational_columns.test_scalar_reads = false;
    var loads: [2]u64 = undefined;
    var bytes: [2]u64 = undefined;
    var times: [2][7]u64 = undefined;
    var expected: ?[]const u8 = null;
    for (0..7) |sample| for (0..2) |turn| {
        const mode = (sample + turn) % 2;
        relational_columns.test_scalar_reads = mode == 0;
        const before = backend.snapshotReadStats();
        const started = platform_time.monotonicNs();
        var stats: types.ColumnarScanStats = .{};
        var result = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &fields, .limit = 1, .columnar_stats = &stats });
        defer result.deinit(alloc);
        times[mode][sample] = platform_time.monotonicNs() - started;
        const after = backend.snapshotReadStats();
        loads[mode] = after.table_block_loads - before.table_block_loads;
        bytes[mode] = after.table_block_bytes - before.table_block_bytes;
        try std.testing.expectEqual(@as(usize, 1), result.documents.len);
        try std.testing.expectEqual(@as(u64, 0), stats.primary_rows_read);
        const encoded = try std.json.Stringify.valueAlloc(scratch, result.documents[0], .{});
        if (expected) |old| try std.testing.expectEqualStrings(old, encoded) else expected = encoded;
    };
    for (&times) |*samples| std.mem.sort(u64, samples, {}, std.sort.asc(u64));
    std.debug.print("\nLSM column batches scalar/batched: block loads={d}/{d}, bytes={d}/{d}, median ns={d}/{d}\n", .{ loads[0], loads[1], bytes[0], bytes[1], times[0][3], times[1][3] });
    try std.testing.expect(loads[1] < loads[0]);
    try std.testing.expect(bytes[1] < bytes[0]);
}

test "relational point projection lease benchmark" {
    const alloc = std.testing.allocator;
    for ([_]PrimaryBackend{.{ .lsm = .{ .flush_threshold = 1 } }}) |backend| {
        const t = try TestDb.open(alloc, .{ .start_optional_runtimes = false, .primary_backend = backend });
        defer t.deinit();
        const db = &t.db;
        const columns = [_]schema_mod.RelationalColumn{
            .{ .name = "n", .path = "n", .column_type = .integer },
            .{ .name = "wide", .path = "wide", .column_type = .string },
        };
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
        const wide = try alloc.alloc(u8, 1024 * 1024);
        defer alloc.free(wide);
        @memset(wide, 'x');
        const json = try std.fmt.allocPrint(alloc, "{{\"n\":7,\"wide\":\"{s}\"}}", .{wide});
        defer alloc.free(json);
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = json }} });
        // Measure the public API allocation, including schema and plan setup.
        var measured = std.testing.FailingAllocator.init(alloc, .{});
        const result = (try db.lookup(measured.allocator(), "a", .{ .fields = &.{"n"}, .include_all_fields = false })).?;
        try std.testing.expectEqualStrings("{\"n\":7}", result.json);
        measured.allocator().free(result.json);
        try std.testing.expect(measured.allocated_bytes < 64 * 1024);
        try std.testing.expectEqual(measured.allocated_bytes, measured.freed_bytes);

        var view = db.core.acquireSchemaView().?;
        defer view.release();
        const key = try internal_keys.relationalRowKeyAlloc(alloc, "a");
        defer alloc.free(key);
        var projection = try RelationalProjectionPlan.init(alloc, view.tableSchema().*, view.physicalLayout(), &.{"n"});
        defer projection.deinit();
        var samples: [4][7]u64 = undefined;
        // Same warmed store probe and compiled projection in all modes. The
        // fourth mode isolates the upper bound of finer-grained authentication;
        // bypassing LMDB integrity is diagnostic only, never a production path.
        for (0..8) |round| for (0..4) |step| {
            const mode = (round + step) % 4;
            const start = platform_time.monotonicNs();
            for (0..64) |_| {
                var probe = try db.core.store.beginProbeTxn();
                defer probe.abort();
                const authenticated = db.core.store.valuesAreAuthenticated();
                const borrowed = if (mode == 0) try probe.get(key) else try probe.getLeased(key);
                const owned = if (mode == 0 and !authenticated) try alloc.dupe(u8, borrowed) else null;
                defer if (owned) |bytes| alloc.free(bytes);
                const raw = owned orelse borrowed;
                const row = if (mode == 3 or db.core.store.valuesAreAuthenticated())
                    try relational_row_codec.ordinalRowViewTrusted(raw, view.tableSchema().*, view.physicalLayout())
                else if (mode == 2)
                    try relational_row_codec.ordinalRowViewSelective(raw, view.tableSchema().*, view.physicalLayout())
                else
                    try relational_row_codec.ordinalRowView(raw, view.tableSchema().*, view.physicalLayout());
                const projected = try projection.project(alloc, row);
                defer alloc.free(projected);
                try std.testing.expectEqualStrings("{\"n\":7}", projected);
            }
            if (round != 0) samples[mode][round - 1] = platform_time.monotonicNs() - start;
        };
        for (&samples) |*sample| std.mem.sort(u64, sample, {}, std.sort.asc(u64));
        std.debug.print("\npoint projection 1MiB/64 probes: backend={s}, copied ns={d}, leased ns={d}, grouped ns={d}, authenticated diagnostic ns={d}, API allocated bytes={d}\n", .{
            @tagName(backend), samples[0][3], samples[1][3], samples[2][3], samples[3][3], measured.allocated_bytes,
        });
    }
}

test "relational columnar dense nested predicate benchmark" {
    const alloc = std.testing.allocator;
    relational_columns.test_disable_deadline = true;
    defer relational_columns.test_disable_deadline = false;
    for ([_]PrimaryBackend{.{ .lsm = .{ .flush_threshold = 1 } }}) |backend| {
        const t = try TestDb.open(alloc, .{ .start_optional_runtimes = false, .primary_backend = backend });
        defer t.deinit();
        const db = &t.db;
        const columns = [_]schema_mod.RelationalColumn{.{ .name = "payload", .path = "payload", .column_type = .json, .is_json = true, .json_kind = .any }};
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        const items = try scratch.alloc(u8, 4096);
        for (items, 0..) |*byte, i| byte.* = if (i % 2 == 0) '0' else ',';
        const json = try std.fmt.allocPrint(scratch, "{{\"payload\":{{\"match\":1,\"items\":[{s}0]}}}}", .{items});
        const writes = try scratch.alloc(types.BatchWrite, 512);
        for (writes, 0..) |*write, i| write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = json };
        try db.batch(.{ .writes = writes });
        try drainTestRelationalMaintenance(db);
        var samples: [5]u64 = undefined;
        var allocated: usize = 0;
        var counters: types.ColumnarScanStats = .{};
        for (0..6) |round| {
            var measured = std.testing.FailingAllocator.init(alloc, .{});
            counters = .{};
            const start = platform_time.monotonicNs();
            var result = try db.scan(measured.allocator(), "", "", .{
                .include_documents = true,
                .filter_query_json = "{\"term\":{\"payload.match\":1}}",
                .columnar_stats = &counters,
            });
            try std.testing.expectEqual(writes.len, result.documents.len);
            result.deinit(measured.allocator());
            if (round > 0) samples[round - 1] = platform_time.monotonicNs() - start;
            try std.testing.expectEqual(measured.allocated_bytes, measured.freed_bytes);
            allocated = measured.allocated_bytes;
        }
        std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
        try std.testing.expectEqual(@as(u64, writes.len), counters.selection_reused_rows);
        try std.testing.expectEqual(@as(u64, 0), counters.primary_predicate_rows);
        std.debug.print("\ndense nested predicate: backend={s}, median ns={d}, allocated bytes={d}, primary rows={d}, sequential ranges={d}, reused decisions={d}, primary predicate rows={d}\n", .{
            @tagName(backend), samples[2], allocated, counters.primary_rows_read, counters.dense_delta_scans, counters.selection_reused_rows, counters.primary_predicate_rows,
        });
    }
}

test "relational columnar decoded reuse bounds decoding and allocations" {
    try testRelationalDecodedReuse(false);
}

test "relational columnar decoded reuse production scale benchmark" {
    try testRelationalDecodedReuse(true);
}

test "relational columnar wide metadata allocation benchmark" {
    const alloc = std.testing.allocator;
    const t = try TestDb.open(alloc, .{ .start_optional_runtimes = false });
    defer t.deinit();
    const db = &t.db;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const columns = try scratch.alloc(schema_mod.RelationalColumn, 1024);
    var object = std.json.ObjectMap.empty;
    var predicates = std.array_list.Managed(std.json.Value).init(scratch);
    for (columns, 0..) |*column, i| {
        const name = try std.fmt.allocPrint(scratch, "f{d:0>4}", .{i});
        column.* = .{ .name = name, .path = name, .column_type = .integer };
        try object.put(scratch, name, .{ .integer = @intCast(i) });
        var exists = std.json.ObjectMap.empty;
        try exists.put(scratch, "field", .{ .string = name });
        var predicate = std.json.ObjectMap.empty;
        try predicate.put(scratch, "exists", .{ .object = exists });
        try predicates.append(.{ .object = predicate });
    }
    var conjunction = std.json.ObjectMap.empty;
    try conjunction.put(scratch, "conjuncts", .{ .array = predicates });
    const filter = try std.json.Stringify.valueAlloc(scratch, std.json.Value{ .object = conjunction }, .{});
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = columns });
    const json = try std.json.Stringify.valueAlloc(scratch, std.json.Value{ .object = object }, .{});
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = json }} });
    try drainTestRelationalMaintenance(db);
    var measured = std.testing.FailingAllocator.init(alloc, .{});
    const started = platform_time.monotonicNs();
    for (0..12) |_| {
        var stats: types.ColumnarScanStats = .{};
        var result = try db.scan(measured.allocator(), "", "", .{ .filter_query_json = filter, .columnar_stats = &stats });
        defer result.deinit(measured.allocator());
        try std.testing.expectEqual(@as(usize, 1), result.hashes.len);
        try std.testing.expect(stats.used);
        try std.testing.expectEqual(@as(u64, 1024), stats.column_metadata_reads);
        try std.testing.expectEqual(@as(u64, 0), stats.payload_pages_read);
        try std.testing.expectEqual(@as(u64, 0), stats.values_materialized);
        try std.testing.expectEqual(@as(u64, 0), stats.logical_slots_initialized);
        try std.testing.expect(stats.column_view_bytes <= 256 * 1024);
        try std.testing.expectEqual(@as(u64, 0), stats.decoded_cache_peak_bytes);
    }
    std.debug.print("\ncolumnar wide metadata: ns/scan={d}, allocated bytes/scan={d}\n", .{ (platform_time.monotonicNs() - started) / 12, measured.allocated_bytes / 12 });
    try std.testing.expectEqual(measured.allocated_bytes, measured.freed_bytes);
    // Keep metadata-only scans independent of max_rows-sized value caches.
    try std.testing.expect(measured.allocated_bytes / 12 < 4 * 1024 * 1024);
}

test "relational columnar narrow projections load only selected metadata on wide rows" {
    const alloc = std.testing.allocator;
    const t = try TestDb.open(alloc, .{ .start_optional_runtimes = false });
    defer t.deinit();
    const db = &t.db;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const columns = try scratch.alloc(schema_mod.RelationalColumn, 1024);
    var object = std.json.ObjectMap.empty;
    for (columns, 0..) |*column, i| {
        const name = try std.fmt.allocPrint(scratch, "f{d:0>4}", .{i});
        column.* = .{ .name = name, .path = name, .column_type = .integer };
        try object.put(scratch, name, .{ .integer = @intCast(i) });
    }
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = columns });
    const json = try std.json.Stringify.valueAlloc(scratch, std.json.Value{ .object = object }, .{});
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = json }} });
    try std.testing.expect(try db.rebuildRelationalColumns());
    // An unrequested metadata record is not read or decoded.
    try db.core.store.put("\x00\x00__columnar__:blocks:0000000000000001:0000000200000000:p00000001", "corrupt unrequested metadata");
    var stats: types.ColumnarScanStats = .{};
    var result = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"f0000"}, .columnar_stats = &stats });
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("{\"f0000\":0}", result.documents[0].json);
    try std.testing.expectEqual(@as(u64, 1), stats.column_metadata_reads);
    try std.testing.expect(stats.metadata_bytes_read < 1024);
    try std.testing.expect(!db.relational_columns_rebuild_requested.load(.acquire));
    var corrupt = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"f0001"} });
    defer corrupt.deinit(alloc);
    try std.testing.expectEqualStrings("{\"f0001\":1}", corrupt.documents[0].json);
    try std.testing.expect(db.relational_columns_rebuild_requested.load(.acquire));
}

test "relational columnar coalescing progresses while a distant range stays hot" {
    const alloc = std.testing.allocator;
    // This fixture assumes four 256-row bootstrap blocks and a fixed number
    // of maintenance quanta; wall-clock scheduling must not change that shape.
    relational_columns.test_disable_deadline = true;
    defer relational_columns.test_disable_deadline = false;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    defer db.close();
    const columns = [_]schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const writes = try scratch.alloc(types.BatchWrite, 1024);
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    for (writes, 0..) |*write, i| {
        write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = "{\"n\":1}" };
        if (i < 768 and i % 256 != 0) try deletes.append(scratch, write.key);
    }
    try db.batch(.{ .writes = writes });
    try drainTestRelationalMaintenance(&db);
    try db.batch(.{ .deletes = deletes.items });
    for (0..3) |_| try std.testing.expect(try db.rebuildRelationalColumns());
    const merged_before_restart = db.relational_column_maintenance.ranges_merged.load(.monotonic);
    db.close();
    db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    const Hot = struct {
        fn run(ptr: *anyopaque) !void {
            const owner: *DB = @ptrCast(@alignCast(ptr));
            const key = try internal_keys.relationalRowKeyAlloc(owner.alloc, "k0768");
            defer owner.alloc.free(key);
            const bytes = try owner.core.store.get(owner.alloc, key);
            defer owner.alloc.free(bytes);
            // Even an identical committed rewrite receives a fresh mutation ID.
            try owner.core.store.put(key, bytes);
        }
    };
    try Hot.run(&db);
    relational_columns.test_before_publish = .{ .context = &db, .run = Hot.run };
    defer relational_columns.test_before_publish = null;
    // Retirement is now its own bounded quantum. Compare the same number of
    // publications while also bounding the intervening reclamation work.
    var turns: usize = 0;
    while (db.relational_column_maintenance.ranges_compacted.load(.monotonic) < 16) {
        _ = try db.rebuildRelationalColumns();
        turns += 1;
        try std.testing.expect(turns < 160);
    }
    var stats: types.ColumnarScanStats = .{};
    var cold = try db.scan(alloc, "", "k0768", .{ .exclusive_to = true, .columnar_stats = &stats });
    defer cold.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), cold.hashes.len);
    try std.testing.expectEqual(@as(u64, 1), stats.blocks_read);
    try std.testing.expectEqual(@as(u64, 0), stats.overlay_rows_read);
    try std.testing.expect(merged_before_restart + db.relational_column_maintenance.ranges_merged.load(.monotonic) >= 2);
}

test "relational durable preparation sizes workers from packed bytes" {
    const alloc = std.testing.allocator;
    const packed_bytes = try alloc.alloc(u8, 512 * 1024);
    defer alloc.free(packed_bytes);
    var rows = std.StringHashMapUnmanaged([]const u8).empty;
    defer rows.deinit(alloc);
    const writes = [_]types.BatchWrite{ .{ .key = "a", .value = "{}" }, .{ .key = "b", .value = "{}" }, .{ .key = "c", .value = "{}" } };
    try rows.put(alloc, "a", packed_bytes);
    try rows.put(alloc, "b", packed_bytes);
    try std.testing.expectEqual(@as(usize, 1), relationalPreparationWorkers(&writes, null));
    try std.testing.expectEqual(@as(usize, 3), relationalPreparationWorkers(&writes, &rows));
    try std.testing.expectEqual(@as(usize, 1), relationalPreparationWorkers(writes[0..1], &rows));
}

test "relational columnar cleanup resumes published pages without rebuilding or clearing racing writes" {
    for ([_]PrimaryBackend{.{ .lsm = .{ .flush_threshold = 1 } }}) |backend|
        try testRelationalCleanupBackend(backend);
}

test "relational columnar delete waves coalesce adjacent underfilled ranges" {
    var checked_allocator = try @import("antfly_platform").allocator.CheckedTestAllocator.init();
    defer checked_allocator.deinit();
    const alloc = checked_allocator.allocator();
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    defer db.close();
    const columns = [_]schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const writes = try scratch.alloc(types.BatchWrite, 1024);
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    for (writes, 0..) |*write, i| {
        write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = "{\"n\":1}" };
        if (i % 256 != 0) try deletes.append(scratch, write.key);
    }
    try db.batch(.{ .writes = writes });
    try drainTestRelationalMaintenance(&db);
    // Separate waves reach idle between deletes. Occupancy metadata must
    // survive queue consumption so future neighbors can still merge.
    for (0..4) |wave| {
        try db.batch(.{ .deletes = deletes.items[wave * 255 ..][0..255] });
        try drainTestRelationalMaintenance(&db);
        if (wave == 0) {
            db.close();
            db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
        }
    }
    var stats: types.ColumnarScanStats = .{};
    var result = try db.scan(alloc, "", "", .{ .include_documents = true, .fields = &.{"n"}, .include_all_fields = false, .columnar_stats = &stats });
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 4), result.documents.len);
    try std.testing.expectEqual(@as(u64, 1), stats.blocks_read);
    try std.testing.expectEqual(@as(u64, 0), stats.dirty_ranges_read);
    try std.testing.expect(db.relational_column_maintenance.ranges_merged.load(.monotonic) >= 3);
    try std.testing.expect(db.relational_column_maintenance.covered_rows_read.load(.monotonic) > 0);
}

test "relational index system NUMERIC cold column projections preserve precision scale and logical hashes" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.initFast("numeric-columns");
    defer directory.cleanup();
    const backend: PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };
    var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .primary_backend = backend });
    defer db.close();
    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "n", .path = "n", .column_type = .numeric, .sql_element_type = .numeric, .allows_null = true },
    };
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    var writes: [32]types.BatchWrite = undefined;
    const documents = [_][]const u8{
        "{\"n\":123456789012345678901234567890.00001}",
        "{\"n\":1.2000}",
        "{\"n\":\"NaN\"}",
        "{\"n\":null}",
    };
    for (&writes, 0..) |*write, i| write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = documents[i % documents.len] };
    try db.batch(.{ .writes = &writes });
    const options: types.ScanOptions = .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"}, .include_content_hashes = true };
    var before = try db.scan(alloc, "", "", options);
    defer before.deinit(alloc);
    try drainTestRelationalMaintenance(&db);
    db.close();
    db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .primary_backend = backend });
    var stats: types.ColumnarScanStats = .{};
    var projected_options = options;
    projected_options.columnar_stats = &stats;
    var after = try db.scan(alloc, "", "", projected_options);
    defer after.deinit(alloc);
    try std.testing.expectEqual(@as(usize, writes.len), after.documents.len);
    try std.testing.expectEqualDeep(before.documents, after.documents);
    try std.testing.expectEqualDeep(before.hashes, after.hashes);
    try std.testing.expectEqual(@as(u64, 0), stats.primary_rows_read);
}

test "relational columnar clean coalescing preserves typed cells without primary reads" {
    // Preserve leak checks; allocation backtraces are opt-in for diagnostics.
    var allocator_state: std.heap.SafeAllocator = .init(std.heap.page_allocator, .{ .stack_trace_frames = 0 });
    defer std.debug.assert(allocator_state.deinit() == 0);
    const alloc = if (platform.env.getenvBool("ANTFLY_TEST_ALLOCATOR_TRACES")) std.testing.allocator else allocator_state.allocator();
    for ([_]PrimaryBackend{.{ .lsm = .{ .flush_threshold = 1 } }}) |backend| {
        var path_tmp = try TestDirectory.initFast("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        defer db.close();
        const columns = [_]schema_mod.RelationalColumn{
            .{ .name = "n", .path = "n", .column_type = .integer },
            .{ .name = "payload", .path = "payload", .column_type = .string, .allows_null = true },
            .{ .name = "metadata", .path = "metadata", .column_type = .json, .is_json = true, .json_kind = .any, .allows_null = true },
        };
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        const writes = try scratch.alloc(types.BatchWrite, 512);
        var deletes = std.ArrayListUnmanaged([]const u8).empty;
        for (writes, 0..) |*write, i| {
            const json = switch (i % 4) {
                0 => "{\"n\":0,\"payload\":\"abc\",\"metadata\":{\"a\":[1,true,\"x\"]}}",
                1 => "{\"n\":1,\"payload\":null,\"metadata\":null}",
                2 => "{\"n\":2}",
                else => "{\"n\":3,\"payload\":\"xyz\",\"metadata\":[null,2]}",
            };
            write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = json };
            if (i % 256 >= 4) try deletes.append(scratch, write.key);
        }
        try db.batch(.{ .writes = writes });
        try drainTestRelationalMaintenance(&db);
        try db.batch(.{ .deletes = deletes.items });
        const fields: []const []const u8 = &.{ "n", "payload", "metadata" };
        relational_columns.test_owner_limit = 8;
        defer relational_columns.test_owner_limit = null;
        var baseline = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = fields, .include_content_hashes = true });
        defer baseline.deinit(alloc);
        // Stop once dirty publication is complete, leaving the clean merge
        // queue to run separately so its primary I/O can be measured exactly.
        var passes: usize = 0;
        while (true) {
            var stats: types.ColumnarScanStats = .{};
            var current = try db.scan(alloc, "", "", .{ .columnar_stats = &stats });
            current.deinit(alloc);
            if (stats.dirty_ranges_read == 0) break;
            try std.testing.expect(try db.rebuildRelationalColumns());
            passes += 1;
            try std.testing.expect(passes < 200);
        }
        const primary_before = db.relational_column_maintenance.primary_rows_read.load(.monotonic);
        try drainTestRelationalMaintenance(&db);
        try std.testing.expectEqual(primary_before, db.relational_column_maintenance.primary_rows_read.load(.monotonic));
        try std.testing.expect(db.relational_column_maintenance.covered_rows_read.load(.monotonic) > 0);
        var after = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = fields, .include_content_hashes = true });
        defer after.deinit(alloc);
        try std.testing.expectEqualDeep(baseline.documents, after.documents);
        try std.testing.expectEqualDeep(baseline.hashes, after.hashes);
        db.close();
        db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        var reopened_stats: types.ColumnarScanStats = .{};
        var reopened = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = fields, .include_content_hashes = true, .columnar_stats = &reopened_stats });
        defer reopened.deinit(alloc);
        try std.testing.expectEqualDeep(after.documents, reopened.documents);
        try std.testing.expectEqual(@as(u64, 0), reopened_stats.primary_rows_read);
    }
}

test "relational columnar deleted insertion burst has bounded cleanup and complete coverage" {
    var checked_allocator = try @import("antfly_platform").allocator.CheckedTestAllocator.init();
    defer checked_allocator.deinit();
    const alloc = checked_allocator.allocator();
    const t = try TestDb.open(alloc, .{ .start_optional_runtimes = false });
    defer t.deinit();
    const db = &t.db;
    const columns = [_]schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    try db.batch(.{ .writes = &.{.{ .key = "z", .value = "{\"n\":7}" }} });
    try drainTestRelationalMaintenance(db);
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const writes = try scratch.alloc(types.BatchWrite, 2050);
    const deletes = try scratch.alloc([]const u8, writes.len);
    for (writes, deletes, 0..) |*write, *key, i| {
        key.* = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i});
        write.* = .{ .key = key.*, .value = "{\"n\":1}" };
    }
    try db.batch(.{ .writes = writes });
    try db.batch(.{ .deletes = deletes });
    const before = db.relational_column_maintenance.dirty_markers_cleared.load(.monotonic);
    try std.testing.expect(try db.rebuildRelationalColumns());
    try std.testing.expect(db.relational_column_maintenance.dirty_markers_cleared.load(.monotonic) - before <= 1024);
    var passes: usize = 0;
    while (true) {
        var result = try db.scan(alloc, "", "", .{ .include_documents = true, .fields = &.{"n"}, .include_all_fields = false });
        defer result.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 1), result.documents.len);
        try std.testing.expectEqualStrings("z", result.documents[0].id);
        try std.testing.expect(!db.relational_columns_rebuild_requested.load(.acquire));
        if (!try db.rebuildRelationalColumns()) break;
        passes += 1;
        try std.testing.expect(passes < 50);
    }
}

test "relational columnar epoch boundaries respect a one block maintenance quantum" {
    const alloc = std.testing.allocator;
    const t = try TestDb.open(alloc, .{ .start_optional_runtimes = false });
    defer t.deinit();
    const db = &t.db;
    const columns = [_]schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    try db.batch(.{ .writes = &.{ .{ .key = "a", .value = "{\"n\":1}" }, .{ .key = "c", .value = "{\"n\":3}" } } });
    try db.setSchema(.{ .version = 2, .storage_mode = .relational, .relational_columns = &columns });
    try drainTestRelationalMaintenance(db);
    try db.batch(.{ .writes = &.{.{ .key = "b", .value = "{\"n\":2}" }} });
    relational_columns.test_compaction_block_limit = 1;
    defer relational_columns.test_compaction_block_limit = null;
    const before = db.relational_column_maintenance.blocks_written.load(.monotonic);
    try std.testing.expect(try db.rebuildRelationalColumns());
    try std.testing.expectEqual(@as(u64, 1), db.relational_column_maintenance.blocks_written.load(.monotonic) - before);
    var result = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"} });
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), result.documents.len);
    try drainTestRelationalMaintenance(db);
    try std.testing.expect(!db.relational_columns_rebuild_requested.load(.acquire));
}

test "relational columnar obsolete generation GC is bounded and resumes after restart" {
    const alloc = std.testing.allocator;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    defer db.close();
    const columns = [_]schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"n\":1}" }} });
    try drainTestRelationalMaintenance(&db);
    const already_deleted = db.relational_column_maintenance.gc_records_deleted.load(.monotonic);
    for (0..260) |i| {
        const key = try std.fmt.allocPrint(alloc, "\x00\x00__columnar__:blocks:0000000000000000:{d:0>4}", .{i});
        defer alloc.free(key);
        try db.core.store.put(key, "obsolete");
    }
    try std.testing.expect(try db.rebuildRelationalColumns());
    try std.testing.expectEqual(@as(u64, 256), db.relational_column_maintenance.gc_records_deleted.load(.monotonic) - already_deleted);
    db.close();
    db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    try drainTestRelationalMaintenance(&db);
    try std.testing.expectEqual(@as(u64, 4), db.relational_column_maintenance.gc_records_deleted.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), db.relational_column_maintenance.blocks_written.load(.monotonic));
}

test "relational columnar wide retirement is bounded durable and backpressures publication" {
    const alloc = std.testing.allocator;
    relational_columns.test_disable_deadline = true;
    defer relational_columns.test_disable_deadline = false;
    for ([_]PrimaryBackend{.{ .lsm = .{ .flush_threshold = 1 } }}) |backend| {
        var path_tmp = try TestDirectory.init("db");
        defer path_tmp.cleanup();
        const path = path_tmp.path().ptr;
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        defer db.close();
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        const columns = try scratch.alloc(schema_mod.RelationalColumn, 600);
        var object = std.json.ObjectMap.empty;
        for (columns, 0..) |*column, i| {
            const name = try std.fmt.allocPrint(scratch, "c{d:0>3}", .{i});
            column.* = .{ .name = name, .path = name, .column_type = .integer };
            try object.put(scratch, name, .{ .integer = 1 });
        }
        try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = columns });
        const first = try std.json.Stringify.valueAlloc(scratch, std.json.Value{ .object = object }, .{});
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = first }} });
        try drainTestRelationalMaintenance(&db);
        try object.put(scratch, "c000", .{ .integer = 2 });
        const second = try std.json.Stringify.valueAlloc(scratch, std.json.Value{ .object = object }, .{});
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = second }} });
        const before = db.relational_column_maintenance.gc_records_deleted.load(.monotonic);
        try std.testing.expect(try db.rebuildRelationalColumns());
        // Publication does no per-column retirement work at all.
        try std.testing.expectEqual(before, db.relational_column_maintenance.gc_records_deleted.load(.monotonic));
        try std.testing.expectEqual(@as(usize, 1), try relational_columns.retiredRootsForTest(&db, alloc));
        try relational_columns.validatePayloadOwnershipForTest(&db, alloc);
        const published = db.relational_column_maintenance.blocks_written.load(.monotonic);
        try db.batch(.{ .writes = &.{.{ .key = "b", .value = first }} });
        try std.testing.expect(try db.rebuildRelationalColumns());
        try std.testing.expect(db.relational_column_maintenance.gc_records_deleted.load(.monotonic) - before <= 256);
        try std.testing.expectEqual(published, db.relational_column_maintenance.blocks_written.load(.monotonic));
        try std.testing.expectEqual(@as(usize, 1), try relational_columns.retiredRootsForTest(&db, alloc));
        try relational_columns.validatePayloadOwnershipForTest(&db, alloc);
        db.close();
        db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false, .primary_backend = backend });
        try drainTestRelationalMaintenance(&db);
        try std.testing.expectEqual(@as(usize, 0), try relational_columns.retiredRootsForTest(&db, alloc));
        try relational_columns.validatePayloadOwnershipForTest(&db, alloc);
        var result = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"c000"} });
        defer result.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 2), result.documents.len);
        try std.testing.expectEqualStrings("{\"c000\":2}", result.documents[0].json);
        try std.testing.expectEqualStrings("{\"c000\":1}", result.documents[1].json);
    }
}

test "relational columnar maintenance survives unrelated artifact corruption and backoff" {
    const alloc = std.testing.allocator;
    const t = try TestDb.open(alloc, .{ .start_optional_runtimes = false });
    defer t.deinit();
    const db = &t.db;
    const columns = [_]schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"n\":1}" }} });
    const issue = try internal_keys.artifactRepairIssueKeyAlloc(alloc, "bad", "embedding", "bad");
    defer alloc.free(issue);
    try db.core.store.put(issue, "malformed");
    const ready = try internal_keys.artifactRepairSummaryReadyKeyAlloc(alloc);
    defer alloc.free(ready);
    try db.core.store.putBatch(&.{}, &.{ready});
    const started = db.independentMaintenanceNowNs();
    try std.testing.expect(db.artifactRepairMetadataWorkerStep() != null);
    try std.testing.expect(db.independent_maintenance.repair_retry_after_ns > started);
    try std.testing.expect(db.relational_column_maintenance.blocks_written.load(.monotonic) > 0);
    const retry = db.independent_maintenance.repair_retry_after_ns;
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"n\":2}" }} });
    try std.testing.expect(db.artifactRepairMetadataWorkerStep() != null);
    try std.testing.expectEqual(retry, db.independent_maintenance.repair_retry_after_ns);
    try std.testing.expect(db.relational_column_maintenance.ranges_compacted.load(.monotonic) > 0);
    db.independent_maintenance.stopping.store(true, .release);
    try std.testing.expect(db.artifactRepairMetadataWorkerStep() == null);
}

test "relational columnar artifact backoff uses the owner clock through suppression and expiry" {
    const alloc = std.testing.allocator;
    var clock = try vopr_mod.vopr_io.VoprIo.init(.{ .monotonic_ns = 100 * std.time.ns_per_s });
    defer clock.deinit();
    var runtime = try background_runtime_mod.BackendRuntimeHandle.init(alloc, .{
        .backend = .manual,
        .borrowed_io = .{ .general = clock.io() },
    });
    defer runtime.deinit();
    const t = try TestDb.open(alloc, .{
        .backend_runtime = runtime.ptr(),
        .executor = .{ .backend = .manual },
        .primary_backend = .{ .mem = .{} },
        .physical_root_mode = .external_backend,
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer t.deinit();
    const db = &t.db;
    const columns = [_]schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"n\":1}" }} });
    const issue = try internal_keys.artifactRepairIssueKeyAlloc(alloc, "bad", "embedding", "bad");
    defer alloc.free(issue);
    try db.core.store.put(issue, "malformed");
    const ready = try internal_keys.artifactRepairSummaryReadyKeyAlloc(alloc);
    defer alloc.free(ready);
    try db.core.store.putBatch(&.{}, &.{ready});
    try std.testing.expect(db.artifactRepairMetadataWorkerStep() != null);
    try std.testing.expect(db.independent_maintenance.repair_retry_after_ns > db.independentMaintenanceNowNs());
    try std.testing.expect(db.relational_column_maintenance.blocks_written.load(.monotonic) > 0);
    const retry = db.independent_maintenance.repair_retry_after_ns;
    try std.testing.expectEqual(clock.monotonic_ns + DB.artifact_repair_metadata_poll_ns, retry);
    clock.monotonic_ns = retry - 1;
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"n\":2}" }} });
    try std.testing.expect(db.artifactRepairMetadataWorkerStep() != null);
    try std.testing.expectEqual(retry, db.independent_maintenance.repair_retry_after_ns);
    try std.testing.expect(db.relational_column_maintenance.ranges_compacted.load(.monotonic) > 0);
    // At the exact deadline the malformed issue is retried and gets a new
    // deadline. No wall-clock sleeping or comparison across clock domains.
    clock.monotonic_ns = retry;
    try std.testing.expect(db.artifactRepairMetadataWorkerStep() != null);
    try std.testing.expectEqual(retry + DB.artifact_repair_metadata_poll_ns, db.independent_maintenance.repair_retry_after_ns);
    db.independent_maintenance.stopping.store(true, .release);
    try std.testing.expect(db.artifactRepairMetadataWorkerStep() == null);
}

test "relational columnar maintenance advances past hot ranges across restart" {
    const alloc = std.testing.allocator;
    // Durable deferred-range ages must use one clock across setup, restart,
    // and admission; switching from realtime after setup can strand timers.
    relational_columns.test_now_ns = 100 * std.time.ns_per_s;
    defer relational_columns.test_now_ns = null;
    // Assert scheduler fairness against fixed 256-row ranges, independently
    // of debug-build speed and the production wall-clock quantum.
    relational_columns.test_disable_deadline = true;
    defer relational_columns.test_disable_deadline = false;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    defer db.close();
    const columns = [_]schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const writes = try scratch.alloc(types.BatchWrite, 768);
    for (writes, 0..) |*write, i| write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = "{\"n\":1}" };
    try db.batch(.{ .writes = writes });
    try std.testing.expect(try db.rebuildRelationalColumns());
    try drainTestRelationalMaintenance(&db);
    try db.batch(.{ .writes = &.{ .{ .key = "k0000", .value = "{\"n\":2}" }, .{ .key = "k0256", .value = "{\"n\":2}" }, .{ .key = "k0512", .value = "{\"n\":2}" } } });
    const HotWriter = struct {
        db: *DB,
        version: usize = 3,
        fn run(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var buf: [64]u8 = undefined;
            const value = try std.fmt.bufPrint(&buf, "{{\"n\":{d}}}", .{self.version});
            self.version += 1;
            try self.db.batch(.{ .writes = &.{.{ .key = "k0000", .value = value }} });
        }
    };
    var hot = HotWriter{ .db = &db };
    relational_columns.test_before_publish = .{ .context = &hot, .run = HotWriter.run };
    defer relational_columns.test_before_publish = null;
    try std.testing.expect(try db.rebuildRelationalColumns());
    db.close();
    db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    for (0..8) |_| _ = try db.rebuildRelationalColumns();
    var stats: types.ColumnarScanStats = .{};
    var cold = try db.scan(alloc, "k0256", "", .{ .inclusive_from = true, .columnar_stats = &stats });
    defer cold.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 512), cold.hashes.len);
    try std.testing.expectEqual(@as(u64, 0), stats.dirty_ranges_read);
    try std.testing.expectEqual(@as(u64, 2), stats.blocks_read);
    // Remove the hot writer. Adaptive maintenance waits for the durable age cap.
    relational_columns.test_before_publish = null;
    try std.testing.expect(try db.runRelationalColumnMaintenancePass() > 0);
    relational_columns.test_now_ns = 111 * std.time.ns_per_s;
    _ = try db.runRelationalColumnMaintenancePass();
    // A bounded pass may discover a range after the clock advance and arm
    // another age timer. Drive its requested wakeup instead of polling a
    // frozen clock or assuming all cleanup fits into eight native quanta.
    for (0..200) |_| {
        if (!db.relational_column_maintenance.pending.load(.acquire)) break;
        relational_columns.test_now_ns = @max(
            relational_columns.test_now_ns.?,
            db.relational_column_maintenance.waiting_until_ns.load(.acquire),
        );
        _ = try db.runRelationalColumnMaintenancePass();
    }
    const maintenance = db.relational_column_maintenance.snapshot();
    try std.testing.expect(!maintenance.pending);
    try std.testing.expectEqual(@as(u64, 0), maintenance.pending_age_ns);
    try std.testing.expect(maintenance.ranges_compacted >= 3);
    try std.testing.expect(maintenance.blocks_written >= 3);
    // Unrelated active artifact work cannot bypass columnar resource backoff.
    db.relational_column_maintenance.retry_after_ns.store(std.math.maxInt(u64), .release);
    const passes = db.relational_column_maintenance.passes.load(.monotonic);
    try std.testing.expectEqual(@as(usize, 0), try db.runRelationalColumnMaintenancePass());
    try std.testing.expectEqual(passes, db.relational_column_maintenance.passes.load(.monotonic));
}

test "relational columnar generations preserve scans and compact dirty ranges" {
    const alloc = std.testing.allocator;
    // This fixture intentionally addresses exact 256-row physical blocks.
    relational_columns.test_disable_deadline = true;
    defer relational_columns.test_disable_deadline = false;
    const t = try TestDb.open(alloc, .{ .start_optional_runtimes = false });
    defer t.deinit();
    const db = &t.db;
    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "n", .path = "n", .column_type = .integer },
        .{ .name = "tag", .path = "tag", .column_type = .string, .allows_null = true },
    };
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const writes = try scratch.alloc(types.BatchWrite, 601);
    writes[0] = .{ .key = "", .value = "{\"n\":-1}" };
    for (writes[1..], 0..) |*write, i| write.* = .{
        .key = try std.fmt.allocPrint(scratch, "row:{d:0>4}", .{i}),
        .value = try std.fmt.allocPrint(scratch, "{{\"n\":{d}{s}}}", .{ i, switch (i % 3) {
            0 => ",\"tag\":null",
            1 => ",\"tag\":\"yes\"",
            else => "",
        } }),
    };
    try db.batch(.{ .writes = writes, .timestamp_ns = 100 });
    const filters = [_][]const u8{
        "",                                                                                                                      "{\"numeric_range\":{\"field\":\"n\",\"min\":597}}",
        "{\"term\":{\"tag\":\"yes\"}}",                                                                                          "{\"term\":{\"tag\":null}}",
        "{\"bool\":{\"must\":[{\"numeric_range\":{\"field\":\"n\",\"min\":250}}],\"must_not\":[{\"term\":{\"tag\":\"yes\"}}]}}",
    };
    const results = try alloc.alloc(types.ScanResult, filters.len);
    defer alloc.free(results);
    var initialized: usize = 0;
    defer for (results[0..initialized]) |*result| result.deinit(alloc);
    for (filters, 0..) |filter, i| {
        results[i] = try db.scan(alloc, "row:0000", "row:0599", .{ .include_documents = true, .include_all_fields = false, .fields = &.{ "n", "tag" }, .filter_query_json = filter, .inclusive_from = false, .exclusive_to = false });
        initialized += 1;
    }
    try std.testing.expect(try db.rebuildRelationalColumns());
    try drainTestRelationalMaintenance(db);
    var columnar_stats: types.ColumnarScanStats = .{};
    var pruned = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"}, .filter_query_json = "{\"numeric_range\":{\"field\":\"n\",\"min\":597}}", .columnar_stats = &columnar_stats });
    defer pruned.deinit(alloc);
    try std.testing.expect(columnar_stats.used);
    try std.testing.expectEqual(@as(u64, 3), columnar_stats.blocks_read);
    try std.testing.expectEqual(@as(u64, 2), columnar_stats.blocks_pruned);
    try std.testing.expectEqual(@as(u64, 1), columnar_stats.columns_read);
    try std.testing.expectEqual(@as(u64, 3), columnar_stats.rows_selected);
    var resumed_stats: types.ColumnarScanStats = .{};
    var resumed = try db.scan(alloc, "row:0597", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"}, .columnar_stats = &resumed_stats });
    defer resumed.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), resumed.documents.len);
    try std.testing.expect(resumed_stats.used);
    try std.testing.expectEqual(@as(u64, 1), resumed_stats.blocks_read);
    for (filters, 0..) |filter, i| {
        var result = try db.scan(alloc, "row:0000", "row:0599", .{ .include_documents = true, .include_all_fields = false, .fields = &.{ "n", "tag" }, .filter_query_json = filter, .inclusive_from = false, .exclusive_to = false });
        defer result.deinit(alloc);
        try std.testing.expectEqualDeep(results[i].documents, result.documents);
        try std.testing.expectEqualDeep(results[i].hashes, result.hashes);
    }
    for ([_][]const u8{
        "\x00\x00__columnar__:blocks:0000000000000001:r:",
        "\x00\x00__columnar__:blocks:0000000000000001:r:row:0255",
        "\x00\x00__columnar__:blocks:0000000000000001:r:row:0511",
    }) |directory_key| {
        const directory_value = try db.core.store.get(alloc, directory_key);
        defer alloc.free(directory_value);
        try db.core.store.delete(directory_key);
        var missing_range = try db.scan(alloc, "row:0000", "row:0599", .{ .include_documents = true, .include_all_fields = false, .fields = &.{ "n", "tag" }, .inclusive_from = false, .exclusive_to = false });
        defer missing_range.deinit(alloc);
        try std.testing.expectEqualDeep(results[0].documents, missing_range.documents);
        try db.core.store.put(directory_key, directory_value);
    }
    const broken_column = try relational_columns.payloadKeyForTest(db, alloc, 1, 0x200000001, 0);
    defer alloc.free(broken_column);
    const corrupt = try db.core.store.get(alloc, broken_column);
    defer alloc.free(corrupt);
    corrupt[corrupt.len - 1] ^= 1;
    try db.core.store.put(broken_column, corrupt);
    var empty_key_scan = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"}, .filter_query_json = "{\"term\":{\"n\":-1}}" });
    defer empty_key_scan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), empty_key_scan.documents.len);
    try std.testing.expectEqualStrings("", empty_key_scan.documents[0].id);
    var recovered_scan = try db.scan(alloc, "row:0000", "row:0599", .{ .include_documents = true, .include_all_fields = false, .fields = &.{ "n", "tag" }, .inclusive_from = false, .exclusive_to = false });
    defer recovered_scan.deinit(alloc);
    // Failure occurs after block zero was delivered. Fallback must resume,
    // not duplicate that prefix or lose rows at the block boundary.
    try std.testing.expectEqualDeep(results[0].documents, recovered_scan.documents);
    try std.testing.expect(db.relational_columns_rebuild_requested.load(.acquire));
    try std.testing.expect(try db.rebuildRelationalColumns());
    const Callback = struct {
        calls: usize = 0,
        fn visit(ptr: ?*anyopaque, _: types.ScanVisitEntry) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            self.calls += 1;
            return error.NotFound;
        }
    };
    var callback: Callback = .{};
    try std.testing.expectError(error.NotFound, db.scanVisit(alloc, "", "", .{}, .{ .context = &callback, .visit = Callback.visit }));
    try std.testing.expectEqual(@as(usize, 1), callback.calls);
    var snapshot = try db.core.store.beginReadTxn();
    defer snapshot.abort();
    _ = try snapshot.get(internal_keys.relational_columnar_manifest_key);
    try db.batch(.{ .writes = &.{.{ .key = "row:0000", .value = "{\"n\":900}" }}, .deletes = &.{"row:0001"} });
    const retained_manifest = try db.core.store.get(alloc, internal_keys.relational_columnar_manifest_key);
    defer alloc.free(retained_manifest);
    var dirty_stats: types.ColumnarScanStats = .{};
    var dirty_scan = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"}, .filter_query_json = "{\"numeric_range\":{\"field\":\"n\",\"min\":800}}", .columnar_stats = &dirty_stats });
    defer dirty_scan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), dirty_scan.documents.len);
    try std.testing.expectEqualStrings("{\"n\":900}", dirty_scan.documents[0].json);
    try std.testing.expectEqual(@as(u64, 1), dirty_stats.dirty_ranges_read);
    try std.testing.expectEqual(@as(u64, 3), dirty_stats.blocks_read);
    // Readers that already pinned the old generation keep its manifest and rows.
    _ = try snapshot.get(internal_keys.relational_columnar_manifest_key);
    try std.testing.expect(try db.rebuildRelationalColumns());
    var updated = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"}, .filter_query_json = "{\"numeric_range\":{\"field\":\"n\",\"min\":800}}" });
    defer updated.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), updated.documents.len);
    try std.testing.expectEqualStrings("{\"n\":900}", updated.documents[0].json);
    try db.setSchema(.{ .version = 2, .storage_mode = .relational, .relational_columns = &columns });
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, internal_keys.relational_columnar_manifest_key));
    const Race = struct {
        fn mutate(ptr: *anyopaque) anyerror!void {
            const owner: *DB = @ptrCast(@alignCast(ptr));
            try owner.batch(.{ .writes = &.{.{ .key = "row:0000", .value = "{\"n\":901}" }} });
        }
    };
    relational_columns.test_before_publish = .{ .context = db, .run = Race.mutate };
    defer relational_columns.test_before_publish = null;
    try std.testing.expect(try db.rebuildRelationalColumns());
    relational_columns.test_before_publish = null;
    var raced = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"}, .filter_query_json = "{\"numeric_range\":{\"field\":\"n\",\"min\":901}}" });
    defer raced.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), raced.documents.len);
    try std.testing.expectEqualStrings("{\"n\":901}", raced.documents[0].json);
    db.independent_maintenance.stopping.store(true, .release);
    try std.testing.expectError(error.Canceled, db.rebuildRelationalColumns());
    db.independent_maintenance.stopping.store(false, .release);
    try std.testing.expect(try db.rebuildRelationalColumns());
}

test "relational durable preparation materializes only requested consumer ordinals" {
    const alloc = std.testing.allocator;
    const t = try TestDb.open(alloc, .{ .start_optional_runtimes = false });
    defer t.deinit();
    const db = &t.db;
    try db.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer"},"embedding":{"type":"embedding"},"payload":{"type":"json"}},"additionalProperties":false}}}}
    );
    const txn = try db.beginTransaction(100);
    try db.writeIntents(txn, &.{.{ .key = "row", .value = "{\"n\":7,\"embedding\":[1,2,3],\"payload\":{\"large\":[1,2,3]}}" }}, &.{});
    var intents = try db.core.collectTransactionIntentBatch(alloc, txn);
    defer intents.deinit(alloc);
    var view = db.core.acquireSchemaView().?;
    defer view.release();
    var plan = try db.core.index_manager.acquireWritePlanSnapshot();
    defer plan.release();
    var prepared = try mapper.PreparedRelationalWrite.initFromIntent(alloc, "row", intents.writes[0].value, view.validator(), view.tableSchema().*, view.physicalLayout(), intents.prepared_rows[0]);
    defer prepared.deinit(alloc);
    try requireRelationalConsumerFields(&prepared, view.tableSchema().*, plan.plan().*, false);
    try std.testing.expect(prepared.extracted.hasDocument());
    try std.testing.expect(prepared.extracted.cleaned_value == null);
    try std.testing.expect(prepared.extracted.logical_source.?.json == null);
    try std.testing.expectEqual(@as(usize, 0), prepared.parsedValue().object.count());
    try prepared.requireLogicalField("n");
    try std.testing.expectEqual(@as(usize, 1), prepared.parsedValue().object.count());
    try std.testing.expectEqual(@as(i64, 7), prepared.parsedValue().object.get("n").?.integer);
    try std.testing.expect(prepared.extracted.logical_source.?.json == null);
    var selected_fields = [_][]u8{@constCast("n")};
    var selected_plan = plan.plan().*;
    selected_plan.has_text_consumers = true;
    selected_plan.text_requires_root = false;
    selected_plan.text_fields = &selected_fields;
    try requireRelationalConsumerFields(&prepared, view.tableSchema().*, selected_plan, true);
    try std.testing.expectEqual(@as(usize, 1), prepared.extracted.prepared_text_root.?.object.count());
    try std.testing.expect(!prepared.parsedValue().object.contains("embedding"));
    const field_json = (try prepared.extracted.logicalJsonForField("n")).?;
    try std.testing.expectEqualStrings("{\"n\":7}", field_json);
    try std.testing.expect(prepared.extracted.logical_source.?.json == null);
    const first = (try prepared.extracted.logicalJson()).?;
    const second = (try prepared.extracted.logicalJson()).?;
    try std.testing.expect(first.ptr == second.ptr);
    var decoded = try std.json.parseFromSlice(std.json.Value, alloc, first, .{});
    defer decoded.deinit();
    try std.testing.expectEqual(@as(usize, 3), decoded.value.object.count());
    var cloned = try prepared.extracted.clone(alloc);
    defer cloned.deinit(alloc);
    try std.testing.expect(cloned.logical_source == null);
    try std.testing.expectEqualStrings(first, cloned.cleaned_value.?);
    try db.commitTransaction(txn, 200);
    const stored = (try db.get(alloc, "row")).?;
    defer alloc.free(stored);
    try std.testing.expectEqualStrings(first, stored);
}

test "relational columnar typed masks avoid vector expansion and eliminated columns" {
    const alloc = std.testing.allocator;
    const t = try TestDb.open(alloc, .{ .start_optional_runtimes = false });
    defer t.deinit();
    const db = &t.db;
    try db.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer"},"embedding":{"type":"embedding"},"payload":{"type":"json"}},"additionalProperties":false}}}}
    );
    try db.batch(.{ .writes = &.{
        .{ .key = "", .value = "{\"n\":1,\"embedding\":[1,2,3],\"payload\":[1,2]}" },
        .{ .key = "a", .value = "{\"n\":2,\"embedding\":[4,5,6],\"payload\":[3]}" },
        .{ .key = "b", .value = "{\"n\":3}" },
    } });
    const filters = [_][]const u8{
        "{\"exists\":{\"field\":\"embedding\"}}",
        "{\"term\":{\"embedding\":5}}",
        "{\"conjuncts\":[{\"term\":{\"embedding\":5}},{\"numeric_range\":{\"field\":\"n\",\"min\":100}}]}",
        "{\"bool\":{\"must\":[{\"term\":{\"payload\":2}},{\"numeric_range\":{\"field\":\"n\",\"min\":100}}]}}",
        "{\"disjuncts\":[{\"exists\":{\"field\":\"n\"}},{\"term\":{\"payload\":2}}]}",
        "{\"bool\":{\"should\":[{\"term\":{\"embedding\":5}},{\"term\":{\"n\":2}}],\"minimum_should_match\":2}}",
        "{\"term\":{\"embedding.1\":5}}",
        "{\"term\":{\"/embedding/1\":5}}",
        "{\"term\":{\"/embedding/01\":5}}",
    };
    var baseline: [filters.len]types.ScanResult = undefined;
    var initialized: usize = 0;
    defer for (baseline[0..initialized]) |*result| result.deinit(alloc);
    for (filters, 0..) |filter, i| {
        baseline[i] = try db.scan(alloc, "", "", .{ .filter_query_json = filter });
        initialized += 1;
    }
    try std.testing.expect(try db.rebuildRelationalColumns());
    for (filters, 0..) |filter, i| {
        var stats: types.ColumnarScanStats = .{};
        var actual = try db.scan(alloc, "", "", .{ .filter_query_json = filter, .columnar_stats = &stats });
        defer actual.deinit(alloc);
        try std.testing.expectEqualDeep(baseline[i].hashes, actual.hashes);
        try std.testing.expect(stats.used);
        try std.testing.expectEqual(@as(u64, 0), stats.values_materialized);
        if (i == 2 or i == 3) try std.testing.expectEqual(@as(u64, 0), stats.columns_read);
        if (i == 0 or i == 4) {
            try std.testing.expectEqual(@as(u64, 0), stats.columns_read);
            try std.testing.expectEqual(@as(u64, 0), stats.payload_bytes_read);
        }
        for ([_]u32{ 1, 2 }) |limit| {
            var limited = try db.scan(alloc, "", "", .{ .filter_query_json = filter, .limit = limit });
            defer limited.deinit(alloc);
            try std.testing.expectEqualDeep(baseline[i].hashes[0..@min(limit, baseline[i].hashes.len)], limited.hashes);
        }
    }
    // Projecting an indexed component and a complete JSON column must borrow
    // the pinned payloads without filling block-owned logical-value slots.
    var projection_stats: types.ColumnarScanStats = .{};
    var projected = try db.scan(alloc, "", "", .{
        .include_documents = true,
        .include_all_fields = false,
        .fields = &.{ "embedding.0", "payload" },
        .columnar_stats = &projection_stats,
    });
    defer projected.deinit(alloc);
    try std.testing.expect(projection_stats.used);
    try std.testing.expectEqualStrings("{\"embedding\":[1],\"payload\":[1,2]}", projected.documents[0].json);
    try std.testing.expectEqual(@as(u64, 0), projection_stats.values_materialized);
    try std.testing.expectEqual(@as(u64, 0), projection_stats.logical_slots_initialized);
    const Stream = struct {
        expected: []const types.ScanDocument,
        count: usize = 0,
        fail: bool = false,
        fn visit(ptr: ?*anyopaque, entry: types.ScanVisitEntry) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            try std.testing.expect(entry.document_json == null);
            const borrowed = entry.document_stream orelse return error.TestUnexpectedResult;
            var writer = std.Io.Writer.Allocating.init(std.testing.allocator);
            defer writer.deinit();
            try borrowed.writeFields(&writer.writer);
            const expected = self.expected[self.count];
            try std.testing.expectEqualStrings(expected.id, entry.id);
            if (expected.json.len == 2) try std.testing.expectEqual(@as(usize, 0), writer.written().len) else {
                try std.testing.expectEqual(@as(u8, ','), writer.written()[0]);
                try std.testing.expectEqualStrings(expected.json[1 .. expected.json.len - 1], writer.written()[1..]);
            }
            self.count += 1;
            if (self.fail) return error.Canceled;
        }
    };
    var stream: Stream = .{ .expected = projected.documents };
    const streaming_opts: types.ScanOptions = .{ .include_documents = true, .include_all_fields = false, .fields = &.{ "embedding.0", "payload" } };
    try db.scanVisit(alloc, "", "", streaming_opts, .{ .context = &stream, .visit = Stream.visit, .borrow_documents = true });
    try std.testing.expectEqual(projected.documents.len, stream.count);
    stream.count = 0;
    stream.fail = true;
    try std.testing.expectError(error.Canceled, db.scanVisit(alloc, "", "", streaming_opts, .{ .context = &stream, .visit = Stream.visit, .borrow_documents = true }));
    try std.testing.expectEqual(@as(usize, 1), stream.count);
    // Dirty owners must use the same borrowed contract through the primary
    // fallback, with no stale page payload and no duplicate delivery.
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"n\":2,\"embedding\":[9],\"payload\":[4]}" }} });
    var dirty = try db.scan(alloc, "", "", streaming_opts);
    defer dirty.deinit(alloc);
    stream = .{ .expected = dirty.documents };
    var dirty_stats: types.ColumnarScanStats = .{};
    var dirty_opts = streaming_opts;
    dirty_opts.columnar_stats = &dirty_stats;
    try db.scanVisit(alloc, "", "", dirty_opts, .{ .context = &stream, .visit = Stream.visit, .borrow_documents = true });
    try std.testing.expectEqual(dirty.documents.len, stream.count);
    try std.testing.expect(dirty_stats.primary_rows_read > 0);
    for ([_]bool{ false, true }) |disable_columnar| {
        var whole = try db.scan(alloc, "", "", .{ .include_documents = true });
        defer whole.deinit(alloc);
        stream = .{ .expected = whole.documents };
        try db.scanVisit(alloc, "", "", .{ .include_documents = true, .disable_columnar_scan = disable_columnar }, .{ .context = &stream, .visit = Stream.visit, .borrow_documents = true });
        try std.testing.expectEqual(whole.documents.len, stream.count);
    }
    // A small LIMIT keeps the clean covered-row path in columnar execution;
    // its borrowed primary scope must stay alive through producer completion.
    try std.testing.expect(try db.rebuildRelationalColumns());
    var first = try db.scan(alloc, "", "", .{ .include_documents = true, .limit = 1 });
    defer first.deinit(alloc);
    stream = .{ .expected = first.documents };
    var full_stats: types.ColumnarScanStats = .{};
    try db.scanVisit(alloc, "", "", .{ .include_documents = true, .limit = 1, .columnar_stats = &full_stats }, .{ .context = &stream, .visit = Stream.visit, .borrow_documents = true });
    try std.testing.expectEqual(first.documents.len, stream.count);
    try std.testing.expect(full_stats.used);
    try std.testing.expectEqual(@as(u64, 1), full_stats.primary_rows_read);
}

test "relational columnar bounded compaction splits empty ranges and resumes canceled staging" {
    // Keep leak checks and failure injection; allocation backtraces are opt-in.
    var allocator_state: std.heap.SafeAllocator = .init(std.heap.page_allocator, .{ .stack_trace_frames = 0 });
    defer std.debug.assert(allocator_state.deinit() == 0);
    const alloc = if (platform.env.getenvBool("ANTFLY_TEST_ALLOCATOR_TRACES")) std.testing.allocator else allocator_state.allocator();
    // The explicit block limit below, not elapsed wall time, defines quanta.
    relational_columns.test_disable_deadline = true;
    defer relational_columns.test_disable_deadline = false;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    defer db.close();
    const columns = [_]schema_mod.RelationalColumn{.{ .name = "n", .path = "n", .column_type = .integer }};
    try db.setSchema(.{ .version = 1, .storage_mode = .relational, .relational_columns = &columns });
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const writes = try scratch.alloc(types.BatchWrite, 800);
    for (writes, 0..) |*write, i| write.* = .{ .key = try std.fmt.allocPrint(scratch, "k{d:0>4}", .{i}), .value = "{\"n\":1}" };
    try db.batch(.{ .writes = writes });
    try std.testing.expect(try db.rebuildRelationalColumns());
    const unchanged_key = try relational_columns.payloadKeyForTest(&db, alloc, 1, 0x200000002, 0);
    defer alloc.free(unchanged_key);
    const unchanged = try db.core.store.get(alloc, unchanged_key);
    defer alloc.free(unchanged);
    for (writes, 0..) |*write, i| write.* = .{ .key = try std.fmt.allocPrint(scratch, "k0001:{d:0>4}", .{i}), .value = "{\"n\":2}" };
    try db.batch(.{ .writes = writes });
    try db.batch(.{ .writes = &.{ .{ .key = "", .value = "{\"n\":3}" }, .{ .key = "\x00x", .value = "{\"n\":4}" } } });
    relational_columns.test_compaction_block_limit = 2;
    defer relational_columns.test_compaction_block_limit = null;
    var passes: usize = 0;
    while (try db.rebuildRelationalColumns()) {
        passes += 1;
        try std.testing.expect(passes < 20);
        var primary = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = true });
        defer primary.deinit(alloc);
        var covered = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"} });
        defer covered.deinit(alloc);
        try std.testing.expectEqualDeep(primary.documents, covered.documents);
    }
    try std.testing.expect(passes >= 2);
    const still_unchanged = try db.core.store.get(alloc, unchanged_key);
    defer alloc.free(still_unchanged);
    try std.testing.expectEqualSlices(u8, unchanged, still_unchanged);

    try db.batch(.{ .writes = &.{.{ .key = "k0700", .value = "{\"n\":9}" }} });
    const RawRace = struct {
        fn run(ptr: *anyopaque) anyerror!void {
            const owner: *DB = @ptrCast(@alignCast(ptr));
            var view = owner.core.acquireSchemaView().?;
            defer view.release();
            var row = try mapper.PreparedRelationalWrite.init(owner.alloc, "k0700", "{\"n\":10}", view.validator(), view.tableSchema().*, view.physicalLayout());
            defer row.deinit(owner.alloc);
            try row.finalizeMetadata(100);
            const key = try internal_keys.relationalRowKeyAlloc(owner.alloc, "k0700");
            defer owner.alloc.free(key);
            // Recovery-style mutation: no apply lock or replay append.
            try owner.core.store.put(key, row.packed_row);
        }
    };
    relational_columns.test_before_publish = .{ .context = &db, .run = RawRace.run };
    defer relational_columns.test_before_publish = null;
    try std.testing.expect(try db.rebuildRelationalColumns());
    relational_columns.test_before_publish = null;
    var raced_stats: types.ColumnarScanStats = .{};
    var raced = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"}, .filter_query_json = "{\"term\":{\"n\":10}}", .columnar_stats = &raced_stats });
    defer raced.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), raced.documents.len);
    try std.testing.expectEqual(@as(u64, 1), raced_stats.dirty_ranges_read);
    const Cancel = struct {
        fn run(ptr: *anyopaque) anyerror!void {
            const owner: *DB = @ptrCast(@alignCast(ptr));
            owner.independent_maintenance.stopping.store(true, .release);
        }
    };
    // Reach the next publication boundary before injecting cancellation;
    // retirement-only quanta intentionally never call the publication hook.
    while (try relational_columns.retiredRootsForTest(&db, alloc) != 0) try std.testing.expect(try db.rebuildRelationalColumns());
    relational_columns.test_before_publish = .{ .context = &db, .run = Cancel.run };
    try std.testing.expectError(error.Canceled, db.rebuildRelationalColumns());
    relational_columns.test_before_publish = null;
    db.close();
    db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    try std.testing.expect(try db.rebuildRelationalColumns());
    try drainTestRelationalMaintenance(&db);

    // Retire all rows, including entire directory ranges and its first range.
    var all = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = true });
    defer all.deinit(alloc);
    const deletes = try scratch.alloc([]const u8, all.documents.len);
    for (deletes, all.documents) |*key, row| key.* = row.id;
    try db.batch(.{ .deletes = deletes });
    passes = 0;
    while (try db.rebuildRelationalColumns()) {
        passes += 1;
        try std.testing.expect(passes < 30);
        // Every publication must preserve empty logical coverage, including
        // neighbors whose physical roots retain rows outside their live bounds.
        var empty = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"} });
        defer empty.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 0), empty.documents.len);
    }
    try db.batch(.{ .writes = &.{.{ .key = "", .value = "{\"n\":42}" }} });
    var inserted = try db.scan(alloc, "", "", .{ .include_documents = true, .include_all_fields = false, .fields = &.{"n"} });
    defer inserted.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), inserted.documents.len);
    try std.testing.expectEqualStrings("{\"n\":42}", inserted.documents[0].json);
    try std.testing.expect(try db.rebuildRelationalColumns());
}

test "db relational mode stores authoritative packed rows across reopen scan and delete" {
    const alloc = std.testing.allocator;

    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"count":{"type":"integer"},"status":{"type":"keyword","enum":["active"]},"title":{"type":"text"},"payload":{"type":"json"}},"required":["id","count","status","title"],"additionalProperties":false}}}}
    ;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
        defer db.close();
        try db.setSchemaJson(alloc, schema_json);
        // Exact retries are idempotent, but public validation semantics are
        // part of the immutable epoch even when their reduced runtime layout
        // is unchanged.
        try db.setSchemaJson(alloc, schema_json);
        const conflicting_schema_json =
            \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"count":{"type":"integer"},"status":{"type":"keyword","enum":["inactive"]},"title":{"type":"text"},"payload":{"type":"json"}},"required":["id","count","status","title"],"additionalProperties":false}}}}
        ;
        try std.testing.expectError(
            error.ImmutableSchemaVersionConflict,
            db.setSchemaJson(alloc, conflicting_schema_json),
        );
        const public_v1_key = try public_table_schema.versionedSchemaKeyAlloc(alloc, 1);
        defer alloc.free(public_v1_key);
        const stored_public_v1 = try db.core.store.get(alloc, public_v1_key);
        defer alloc.free(stored_public_v1);
        try std.testing.expectEqualStrings(schema_json, stored_public_v1);
        try db.batch(.{ .writes = &.{.{ .key = "\x00\x00__metadata__:local_test", .value = "opaque" }} });
        try db.addIndex(.{ .name = "ft_rows", .kind = .full_text, .config_json = "{}" });

        try std.testing.expectError(error.InvalidBatchRequest, db.batch(.{ .writes = &.{.{
            .key = "row:invalid-direct",
            .value = "{\"id\":\"invalid-direct\",\"count\":1,\"status\":\"inactive\",\"title\":\"bad\"}",
        }} }));

        try db.batch(.{ .writes = &.{.{
            .key = "row:a",
            .value = "{\"id\":\"a\",\"count\":9007199254740993,\"status\":\"active\",\"title\":\"replayed relational row\",\"payload\":{\"n\":0.10000000000000001}}",
        }}, .sync_level = .write });

        const row_key = try internal_keys.relationalRowKeyAlloc(alloc, "row:a");
        defer alloc.free(row_key);
        const packed_row = try db.core.store.get(alloc, row_key);
        defer alloc.free(packed_row);
        try std.testing.expect(mapper.isRelationalRowValue(packed_row));
        try std.testing.expectEqual(@as(u32, 1), try mapper.relationalRowSchemaVersion(packed_row));
        try std.testing.expectEqual(@as(u64, 1), db.core.table_catalog.row_count);
        try std.testing.expectEqual(@as(u32, 1), db.core.table_catalog.active_schema_version);
        try std.testing.expect(db.core.table_catalog.reconciled);
        const replay_documents = [_]derived_types.DerivedDocument{.{
            .key = "row:a",
            .targets = &.{.{ .kind = .full_text, .index_name = "ft_rows" }},
        }};
        var collected_text = try collectTextDocumentWritesForIndex(
            alloc,
            db.core.store,
            db.core.index_manager,
            &replay_documents,
            "ft_rows",
            false,
            db.core.index_manager.byte_range,
            .{ .relational_base_rows = true },
        );
        defer collected_text.deinit();
        try std.testing.expectEqual(@as(usize, 1), collected_text.docs.items.len);
        try std.testing.expect(collected_text.docs.items[0].root != null);
        try std.testing.expectEqual(@as(usize, 0), collected_text.docs.items[0].value.len);
        const document_key = try internal_keys.documentKeyAlloc(alloc, "row:a");
        defer alloc.free(document_key);
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, document_key));
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();
        // Publish a compatible epoch before reading the old row. The physical
        // row remains bound to v1 and must decode through the retained layout,
        // never through the newly active v2 shape.
        const schema_v2_json =
            \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"count":{"type":"integer"},"status":{"type":"keyword","enum":["active"]},"title":{"type":"text"},"payload":{"type":"json"},"note":{"type":"keyword"}},"required":["id","count","status","title"],"additionalProperties":false}}}}
        ;
        try db.setSchemaJson(alloc, schema_v2_json);
        const public_v2_key = try public_table_schema.versionedSchemaKeyAlloc(alloc, 2);
        defer alloc.free(public_v2_key);
        const stored_public_v2 = try db.core.store.get(alloc, public_v2_key);
        defer alloc.free(stored_public_v2);
        try std.testing.expectEqualStrings(schema_v2_json, stored_public_v2);
        try std.testing.expectEqual(@as(u32, 2), db.core.table_catalog.active_schema_version);
        try std.testing.expectEqual(@as(u64, 1), db.core.table_catalog.row_count);
        const raw = (try db.get(alloc, "row:a")) orelse return error.TestExpectedEqual;
        defer alloc.free(raw);
        const metadata = (try db.get(alloc, "\x00\x00__metadata__:local_test")).?;
        defer alloc.free(metadata);
        try std.testing.expectEqualStrings("opaque", metadata);
        try std.testing.expectEqualStrings(
            "{\"id\":\"a\",\"count\":9007199254740993,\"status\":\"active\",\"title\":\"replayed relational row\",\"payload\":{\"n\":0.10000000000000001}}",
            raw,
        );
        var projected_lookup = (try db.lookup(alloc, "row:a", .{
            .fields = &.{ "payload", "count" },
            .include_all_fields = false,
        })) orelse return error.TestExpectedEqual;
        defer projected_lookup.deinit(alloc);
        try std.testing.expectEqualStrings(
            "{\"payload\":{\"n\":0.10000000000000001},\"count\":9007199254740993}",
            projected_lookup.json,
        );

        try db.runUntilIdle();
        var search_result = try db.search(alloc, .{
            .index_name = "ft_rows",
            .query = .{ .match = .{ .field = "title", .text = "replayed" } },
        });
        defer search_result.deinit();
        try std.testing.expectEqual(@as(usize, 1), search_result.hits.len);
        try std.testing.expectEqualStrings("row:a", search_result.hits[0].id);

        var projected_search = try db.search(alloc, .{
            .index_name = "ft_rows",
            .query = .{ .match = .{ .field = "title", .text = "replayed" } },
            .fields = &.{ "id", "count" },
            .include_all_fields = false,
            .include_stored = true,
        });
        defer projected_search.deinit();
        try std.testing.expectEqualStrings(
            "{\"id\":\"a\",\"count\":9007199254740993}",
            projected_search.hits[0].stored_data.?,
        );

        var scanned = try db.scan(alloc, "", "", .{
            .include_documents = true,
            .fields = &.{ "id", "count" },
            .include_all_fields = false,
            .filter_query_json = "{\"term\":{\"status\":\"active\"}}",
        });
        defer scanned.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 1), scanned.documents.len);
        try std.testing.expectEqualStrings("row:a", scanned.documents[0].id);
        try std.testing.expectEqualStrings(
            "{\"id\":\"a\",\"count\":9007199254740993}",
            scanned.documents[0].json,
        );

        const rejected_txn = try db.beginTransaction(1_900);
        try std.testing.expectError(error.InvalidBatchRequest, db.writeTransaction(rejected_txn, .{ .writes = &.{.{
            .key = "row:invalid",
            .value = "{\"id\":\"invalid\",\"count\":1,\"status\":\"inactive\",\"title\":\"bad\"}",
        }} }));
        try db.resolveTransactionIntents(rejected_txn, .aborted, 1_901);
        try std.testing.expect((try db.get(alloc, "row:invalid")) == null);

        const rejected_unknown_txn = try db.beginTransaction(1_910);
        try std.testing.expectError(error.InvalidBatchRequest, db.writeIntents(rejected_unknown_txn, &.{.{
            .key = "row:unknown",
            .value = "{\"id\":\"unknown\",\"count\":1,\"status\":\"active\",\"title\":\"bad\",\"unknown\":true}",
        }}, &.{}));
        try db.resolveTransactionIntents(rejected_unknown_txn, .aborted, 1_911);
        try std.testing.expect((try db.get(alloc, "row:unknown")) == null);

        const txn_id = try db.beginTransaction(2_000);
        try db.writeIntents(txn_id, &.{.{
            .key = "row:b",
            .value = "{\"id\":\"b\",\"count\":2,\"status\":\"active\",\"title\":\"committed row\"}",
        }}, &.{});
        try db.commitTransaction(txn_id, 2_001);
        const committed = (try db.get(alloc, "row:b")) orelse return error.TestExpectedEqual;
        defer alloc.free(committed);
        try std.testing.expectEqualStrings(
            "{\"id\":\"b\",\"count\":2,\"status\":\"active\",\"title\":\"committed row\"}",
            committed,
        );
        const committed_legacy_key = try internal_keys.documentKeyAlloc(alloc, "row:b");
        defer alloc.free(committed_legacy_key);
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, committed_legacy_key));

        try db.batch(.{ .deletes = &.{"row:a"} });
        try std.testing.expect((try db.get(alloc, "row:a")) == null);
        const row_key = try internal_keys.relationalRowKeyAlloc(alloc, "row:a");
        defer alloc.free(row_key);
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, row_key));
    }
}

test "db relational ttl cleanup preserves physical mode across reopen" {
    const alloc = std.testing.allocator;
    const ttl_duration_ns: u64 = std.time.ns_per_s;
    var path_tmp = try TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
        defer db.close();
        try db.setSchema(.{
            .version = 1,
            .storage_mode = .relational,
            .ttl_duration_ns = ttl_duration_ns,
            .relational_columns = &.{.{
                .name = "title",
                .path = "title",
                .column_type = .string,
                .required = true,
            }},
        });
        try db.batch(.{
            .writes = &.{.{ .key = "row:expired", .value = "{\"title\":\"expired\"}" }},
            .timestamp_ns = currentTimeNs() - 2 * ttl_duration_ns,
        });
        // Read visibility must expire the packed row before background GC.
        try std.testing.expect((try db.get(alloc, "row:expired")) == null);
        var scan = try db.scan(alloc, "", "", .{ .include_documents = true });
        defer scan.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 0), scan.documents.len);
        const stored_key = try internal_keys.relationalRowKeyAlloc(alloc, "row:expired");
        defer alloc.free(stored_key);
        const stored_row = try db.core.store.get(alloc, stored_key);
        defer alloc.free(stored_row);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{ .start_optional_runtimes = false });
    defer reopened.close();
    const ttl_cfg: ttl_runtime_mod.Config = .{
        .enabled = true,
        .interval_ms = 10,
        .batch_size = 8,
        .grace_period_ns = 0,
    };
    try initStoppedTtlRuntimeForTest(&reopened, ttl_cfg);
    try std.testing.expect(reopened.ttl_cleanup_context.?.batch.relational_base_rows);
    try reopened.ttl_runtime.?.runOnce();

    try std.testing.expect((try reopened.get(alloc, "row:expired")) == null);
    const row_key = try internal_keys.relationalRowKeyAlloc(alloc, "row:expired");
    defer alloc.free(row_key);
    try std.testing.expectError(error.NotFound, reopened.core.store.get(alloc, row_key));
    const stats = try reopened.diagnosticStats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(u64, 0), stats.doc_identity.live_ordinals);
}

test "db relational semantic no-op refreshes authoritative row timestamp" {
    const alloc = std.testing.allocator;
    const ttl_duration_ns: u64 = 10 * std.time.ns_per_s;
    const t = try TestDb.open(alloc, .{ .start_optional_runtimes = false });
    defer t.deinit();
    const db = &t.db;
    try db.setSchema(.{
        .version = 1,
        .storage_mode = .relational,
        .ttl_duration_ns = ttl_duration_ns,
        .relational_columns = &.{.{
            .name = "title",
            .path = "title",
            .column_type = .string,
            .required = true,
        }},
    });

    const value = "{\"title\":\"same logical row\"}";
    const refresh_timestamp = currentTimeNs();
    const original_timestamp = refresh_timestamp - 2 * ttl_duration_ns;
    try db.batch(.{
        .writes = &.{.{ .key = "row:refresh", .value = value }},
        .timestamp_ns = original_timestamp,
    });
    try db.batch(.{
        .writes = &.{.{ .key = "row:refresh", .value = value }},
        .timestamp_ns = refresh_timestamp,
    });

    const row_key = try internal_keys.relationalRowKeyAlloc(alloc, "row:refresh");
    defer alloc.free(row_key);
    const packed_row = try db.core.store.get(alloc, row_key);
    defer alloc.free(packed_row);
    try std.testing.expectEqual(refresh_timestamp, try relational_store.rowWriteTimestampNs(packed_row));
    try std.testing.expectEqual(refresh_timestamp, try db.getTimestamp(alloc, "row:refresh"));
    const visible = (try db.get(alloc, "row:refresh")) orelse return error.TestExpectedEqual;
    defer alloc.free(visible);
    try std.testing.expectEqualStrings(value, visible);
}
