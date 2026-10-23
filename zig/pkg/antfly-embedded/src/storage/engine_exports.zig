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

const engine = @import("engine.zig");
const std = @import("std");
const native = @import("antfly_runtime_abi").native_abi;
const Exports = @This();
fn portableExportToWriterWithOptions(alloc: Allocator, store: *@import("docstore.zig").DocStore, writer: engine.WriterBorrow, options: engine.PortableExportOptions) engine.Outcome(void) {
    var receiving_writer = writer.receive();
    var spool_receiver: ?@import("antfly_runtime_abi").io_abi.Receiver = if (options.spool) |spool| spool.io.receive() catch |err| return .{ .failure = .init(@errorName(err)) } else null;
    var received_options = options.value;
    if (spool_receiver) |*receiver| received_options.spool = .{ .io = receiver.io(), .file = options.spool.?.file };
    @import("portable_backup.zig").exportPortableToWriterWithOptions(alloc, store, receiving_writer.writer(), received_options) catch |err| return .{ .failure = .init(@errorName(err)) };
    receiving_writer.finish() catch |err| return .{ .failure = .init(@errorName(err)) };
    return .{ .value = {} };
}
fn portableExport(alloc: std.mem.Allocator, store: *@import("docstore.zig").DocStore, out: *std.ArrayList(u8)) engine.Outcome(void) {
    return .{ .value = @import("portable_backup.zig").exportPortable(alloc, store, out) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
const db = @import("db/db.zig");
const DB = db.DB;
const Allocator = std.mem.Allocator;
const types = @import("db/types.zig");
const BatchProfile = db.BatchProfile;
const BatchExecutionOptions = @import("db/execution_resources.zig").BatchExecutionOptions;
const SearchWithDenseProfileResult = db.SearchWithDenseProfileResult;
const SearchWithCapturedRequestResult = db.SearchWithCapturedRequestResult;
const schema_mod = @import("schema.zig");
const OpenOptions = db.OpenOptions;
const capi = @import("../capi/types.zig");
fn open(alloc: Allocator, path: []const u8, requested_opts: OpenOptions) engine.Outcome(DB) {
    return .{ .value = DB.open(alloc, path, requested_opts) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn openOwned(alloc: Allocator, path: []const u8, opts: OpenOptions) engine.Outcome(*DB) {
    return .{ .value = DB.openOwned(alloc, path, opts) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn close(self: *DB) engine.Outcome(void) {
    return .{ .value = self.close() };
}

fn closeOwned(self: *DB) engine.Outcome(void) {
    return .{ .value = self.closeOwned() };
}

fn startResidentBackgroundWorkersIfNeeded(self: *DB) engine.Outcome(void) {
    return .{ .value = self.startResidentBackgroundWorkersIfNeeded() };
}

fn batchInternal(self: *DB, req: types.BatchRequest, profile: ?*BatchProfile, opts: BatchExecutionOptions) engine.Outcome(void) {
    return .{ .value = self.batchInternal(req, profile, opts) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn searchWithDenseProfile(self: *DB, alloc: Allocator, req: types.SearchRequest) engine.Outcome(SearchWithDenseProfileResult) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    var value = self.searchWithDenseProfile(alloc, received) catch |err| return .{ .failure = .init(@errorName(err)) };
    // Captured requests retain caller-owned hooks, never the stack proxy.
    value.request.native_key_predicate = req.native_key_predicate;
    return .{ .value = value };
}

fn searchWithCapturedRequestAndExecutionContext(
    self: *DB,
    alloc: Allocator,
    req: types.SearchRequest,
    exec_ctx: types.ExecutionContext,
) engine.Outcome(SearchWithCapturedRequestResult) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    var value = self.searchWithCapturedRequestAndExecutionContext(alloc, received, exec_ctx) catch |err| return .{ .failure = .init(@errorName(err)) };
    // Captured requests retain caller-owned hooks, never the stack proxy.
    value.request.native_key_predicate = req.native_key_predicate;
    return .{ .value = value };
}

fn searchLease(lease: DB.QueryReadLease, alloc: Allocator, req: types.SearchRequest) engine.Outcome(SearchWithDenseProfileResult) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    var value = lease.search(alloc, received) catch |err| return .{ .failure = .init(@errorName(err)) };
    // Captured requests retain caller-owned hooks, never the stack proxy.
    value.request.native_key_predicate = req.native_key_predicate;
    return .{ .value = value };
}

fn sync(self: *DB, full: bool) engine.Outcome(void) {
    return .{ .value = self.sync(full) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn syncIndexes(self: *DB, force: bool) engine.Outcome(void) {
    return .{ .value = self.syncIndexes(force) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn snapshot(self: *DB, id: []const u8) engine.Outcome(u64) {
    return .{ .value = self.snapshot(id) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn snapshotWithMaintenanceDeadline(self: *DB, id: []const u8, maintenance_deadline_ns: u64) engine.Outcome(u64) {
    return .{ .value = self.snapshotWithMaintenanceDeadline(id, maintenance_deadline_ns) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn snapshotNativeWithCancellation(self: *DB, id: []const u8, cancellation: types.CancellationToken) engine.Outcome(u64) {
    return .{ .value = self.snapshotNativeWithCancellation(id, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn snapshotRelationalCohort(self: *DB, id: []const u8, expected: @import("db/relational_integrity_topology.zig").Fence, cancellation: types.CancellationToken) engine.Outcome(u64) {
    return .{ .value = self.snapshotRelationalCohort(id, expected, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn setSchema(self: *DB, table_schema: schema_mod.TableSchema) engine.Outcome(void) {
    return .{ .value = self.setSchema(table_schema) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn setSchemaJson(self: *DB, alloc: Allocator, schema_json: []const u8) engine.Outcome(void) {
    return .{ .value = self.setSchemaJson(alloc, schema_json) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn addIndex(self: *DB, cfg: types.IndexConfig) engine.Outcome(void) {
    return .{ .value = self.addIndex(cfg) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn deleteIndex(self: *DB, name: []const u8) engine.Outcome(bool) {
    return .{ .value = self.deleteIndex(name) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn runUntilIdle(self: *DB) engine.Outcome(void) {
    return .{ .value = self.runUntilIdle() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn runUntilIdleWithoutWaitingForEnrichmentRetriesWithCancellation(
    self: *DB,
    cancellation: types.CancellationToken,
) engine.Outcome(void) {
    return .{ .value = self.runUntilIdleWithoutWaitingForEnrichmentRetriesWithCancellation(cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn finishBackgroundGraphReplacement(self: *DB, name: []const u8, generation: u64) engine.Outcome(bool) {
    return .{ .value = self.finishBackgroundGraphReplacement(name, generation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn runUntilIdleWithGroupSourceState(self: *DB, state: *DB.GroupSourceMaintenanceState) engine.Outcome(void) {
    return .{ .value = self.runUntilIdleWithGroupSourceState(state) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn runGroupSourceMaintenanceSlice(self: *DB, state: *DB.GroupSourceMaintenanceState) engine.Outcome(bool) {
    return .{ .value = self.runGroupSourceMaintenanceSlice(state) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn runBackgroundTurnWithCancellation(self: *DB, cancellation: types.CancellationToken, shadow: bool) engine.Outcome(bool) {
    return .{ .value = self.runBackgroundTurnWithCancellation(cancellation, shadow) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn runBackgroundMaintenanceWithCancellation(self: *DB, cancellation: types.CancellationToken) engine.Outcome(bool) {
    return .{ .value = self.runBackgroundMaintenanceWithCancellation(cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn registry() engine.Outcome(*anyopaque) {
    return .{ .value = &@import("../capi/handles.zig").handle_registry };
}
fn clearError() callconv(.c) void {
    capi.clearLastError();
}
fn errorCode() callconv(.c) capi.ErrorCode {
    return capi.lastErrorCode();
}
fn errorDetail() callconv(.c) [*:0]const u8 {
    return capi.lastErrorDetail();
}
fn setError(code: capi.ErrorCode, ptr: [*]const u8, len: usize) callconv(.c) void {
    _ = capi.recordError(code, ptr[0..len]);
}
fn get(self: *DB, alloc: Allocator, key: []const u8) engine.Outcome(?[]u8) {
    return .{ .value = self.get(alloc, key) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn getDocument(self: *DB, alloc: Allocator, key: []const u8, opts: types.LookupOptions) engine.Outcome(?types.LookupResult) {
    return .{ .value = self.getDocument(alloc, key, opts) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn lookup(self: *DB, alloc: Allocator, key: []const u8, opts: types.LookupOptions) engine.Outcome(?types.LookupResult) {
    return .{ .value = self.lookup(alloc, key, opts) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn scan(self: *DB, alloc: Allocator, from_key: []const u8, to_key: []const u8, opts: types.ScanOptions) engine.Outcome(types.ScanResult) {
    return .{ .value = self.scan(alloc, from_key, to_key, opts) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn diagnosticStats(self: *DB, alloc: Allocator) engine.Outcome(types.DBStats) {
    return .{ .value = self.diagnosticStats(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn getSchemaJson(self: *DB, alloc: Allocator) engine.Outcome(?[]u8) {
    return .{ .value = self.getSchemaJson(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn primaryDocCount(self: *DB, alloc: Allocator) engine.Outcome(u64) {
    return .{ .value = self.primaryDocCount(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn getEnrichment(self: *DB, alloc: Allocator, kind: types.EnrichmentKind, name: []const u8) engine.Outcome(?types.EnrichmentConfig) {
    return .{ .value = self.getEnrichment(alloc, kind, name) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn runLsmMaintenanceStep(self: *DB) engine.Outcome(bool) {
    return .{ .value = self.runLsmMaintenanceStep() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn constraintValidationStatus(self: *DB) engine.Outcome(DB.ConstraintValidationStatus) {
    return .{ .value = self.constraintValidationStatus() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn validateConstraintsStep(self: *DB, budget: @import("db/relational_constraint_jobs.zig").Budget) engine.Outcome(bool) {
    return .{ .value = self.validateConstraintsStep(budget) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn retryConstraintValidation(self: *DB, schema_version: u32) engine.Outcome(bool) {
    return .{ .value = self.retryConstraintValidation(schema_version) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn buildRelationalIndexStep(self: *DB, name: []const u8, budget: DB.RelationalIndexBuildBudget) engine.Outcome(void) {
    return .{ .value = self.buildRelationalIndexStep(name, budget) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn collectRelationalIndexGarbageStep(self: *DB) engine.Outcome(bool) {
    return .{ .value = self.collectRelationalIndexGarbageStep() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn retryRelationalIndexBuild(self: *DB, name: []const u8, generation: u64) engine.Outcome(bool) {
    return .{ .value = self.retryRelationalIndexBuild(name, generation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn relationalIndexBuildStatus(self: *DB, name: []const u8) engine.Outcome(DB.RelationalIndexBuildStatus) {
    return .{ .value = self.relationalIndexBuildStatus(name) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn reconfigureEnrichmentRuntime(self: *DB, cfg: @import("db/enrichment/enrichment_runtime.zig").Config) engine.Outcome(void) {
    return .{ .value = self.reconfigureEnrichmentRuntime(cfg) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn reconfigureEnrichmentRuntimePaused(self: *DB, cfg: @import("db/enrichment/enrichment_runtime.zig").Config) engine.Outcome(void) {
    return .{ .value = self.reconfigureEnrichmentRuntimePaused(cfg) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn resumeEnrichmentRuntimeAfterReconfigure(self: *DB, operation: []const u8, index_name: []const u8) engine.Outcome(void) {
    return .{ .value = self.resumeEnrichmentRuntimeAfterReconfigure(operation, index_name) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn ensureTransactionRecoveryRuntime(self: *DB, cfg: @import("db/maintenance/transaction_runtime.zig").Config) engine.Outcome(void) {
    return .{ .value = self.ensureTransactionRecoveryRuntime(cfg) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn activateResolverReplayRuntimes(self: *DB) engine.Outcome(void) {
    return .{ .value = self.activateResolverReplayRuntimes() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn deactivateResolverReplayRuntimes(self: *DB) engine.Outcome(void) {
    self.deactivateResolverReplayRuntimes();
    return .{ .value = {} };
}

fn batchPrepared(self: *DB.PreparedBatchOwner, req: types.BatchRequest, profile: ?*BatchProfile, opts: BatchExecutionOptions, memo: *@import("db/execution_resources.zig").GeneratedEmbeddingMemo, allocator: *@import("db/execution_resources.zig").PreparedRowAllocator) engine.Outcome(void) {
    return .{ .value = self.batchInternalPrepared(req, profile, opts, memo, allocator) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
fn searchDenseProfiled(self: *DB, alloc: Allocator, req: types.SearchRequest, dense: types.DenseKnnQuery) engine.Outcome(@import("db/query/search_exec.zig").ProfiledDenseSearchResult) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    return .{ .value = self.searchDenseProfiled(alloc, received, dense) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn searchDenseProfiledWithCapturedRequest(self: *DB, alloc: Allocator, req: types.SearchRequest, dense: types.DenseKnnQuery) engine.Outcome(db.ProfiledDenseSearchWithCapturedRequestResult) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    var value = self.searchDenseProfiledWithCapturedRequest(alloc, received, dense) catch |err| return .{ .failure = .init(@errorName(err)) };
    value.request.native_key_predicate = req.native_key_predicate;
    return .{ .value = value };
}

fn searchTextKernel(self: *DB, alloc: Allocator, index_name: []const u8, text_query: types.TextQuery, options: types.TextKernelSearchOptions) engine.Outcome(types.TextKernelResult) {
    return .{ .value = self.searchTextKernel(alloc, index_name, text_query, options) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn preflightSearchRequestWithExecutionContext(self: *DB, alloc: Allocator, req: types.SearchRequest, max_work: u32, exec_ctx: types.ExecutionContext) engine.Outcome(@import("db/query/search_exec.zig").RuntimePreflightSummary) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    return .{ .value = self.preflightSearchRequestWithExecutionContext(alloc, received, max_work, exec_ctx) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn collectPlanningStatsWithExecutionContext(self: *DB, alloc: Allocator, req: types.SearchRequest, max_work: u32, exec_ctx: types.ExecutionContext) engine.Outcome(@import("db/planning_stats.zig").PlanningStatsSummary) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    return .{ .value = self.collectPlanningStatsWithExecutionContext(alloc, received, max_work, exec_ctx) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn collectSearchRequestTextStats(self: *DB, alloc: Allocator, req: types.SearchRequest) engine.Outcome([]const @import("../search/distributed_stats.zig").TextFieldStats) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    return .{ .value = self.collectSearchRequestTextStats(alloc, received) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn configureTableStorage(self: *DB, settings: @import("../common/table_storage.zig").Settings) engine.Outcome(void) {
    return .{ .value = self.configureTableStorage(settings) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn trySnapshotDenseSearch(self: *DB, alloc: std.mem.Allocator, req: types.SearchRequest, dense: types.DenseKnnQuery, include_profile: bool) engine.Outcome(?db.SearchWithDenseProfileResult) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    var value = self.trySnapshotDenseSearch(alloc, received, dense, include_profile) catch |err| return .{ .failure = .init(@errorName(err)) };
    if (value) |*captured| captured.request.native_key_predicate = req.native_key_predicate;
    return .{ .value = value };
}

fn snapshotInternal(self: *DB, id: []const u8, include_generated: bool, cancellation: types.CancellationToken, maintenance_deadline_ns: ?u64, expected_topology_fence: ?@import("db/relational_integrity_topology.zig").Fence, backup_seal_only: bool, query_cut: ?@import("db/native_query_cut.zig").Request) engine.Outcome(u64) {
    return .{ .value = self.snapshotInternal(id, include_generated, cancellation, maintenance_deadline_ns, expected_topology_fence, backup_seal_only, query_cut) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn navigateHierarchyChildren(self: *DB, alloc: std.mem.Allocator, req: types.SearchRequest) engine.Outcome(types.SearchResult) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    return .{ .value = self.navigateHierarchyChildren(alloc, received) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn searchDense(self: *DB, alloc: std.mem.Allocator, req: types.SearchRequest, dense: types.DenseKnnQuery) engine.Outcome(types.SearchResult) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    return .{ .value = self.searchDense(alloc, received, dense) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn searchSparse(self: *DB, alloc: std.mem.Allocator, req: types.SearchRequest, sparse: types.SparseKnnQuery) engine.Outcome(types.SearchResult) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    return .{ .value = self.searchSparse(alloc, received, sparse) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn searchGraph(self: *DB, alloc: std.mem.Allocator, req: types.SearchRequest, graph_query: @import("../graph/query.zig").GraphQuery, base_hits: ?[]const types.SearchHit) engine.Outcome(types.SearchResult) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    return .{ .value = self.searchGraph(alloc, received, graph_query, base_hits) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn applyReplicatedBatch(self: *DB, mutation: @import("db/replicated_mutation.zig").Batch, sequence: u64) engine.Outcome(void) {
    return .{ .value = self.applyReplicatedBatch(mutation, sequence) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn reconcileReplicatedArtifactAdmission(self: *DB, request: types.BatchRequest, entry: db.OrderedApplyReceipt) engine.Outcome(void) {
    return .{ .value = self.reconcileReplicatedArtifactAdmission(request, entry) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn applyReplicatedSchema(self: *DB, mutation: @import("db/replication_effects.zig").SchemaMutation, sequence: u64) engine.Outcome(void) {
    return .{ .value = self.applyReplicatedSchema(mutation, sequence) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn applyReplicatedRowPolicy(self: *DB, mutation: @import("db/replication_effects.zig").RowPolicyMutation, sequence: u64) engine.Outcome(void) {
    return .{ .value = self.applyReplicatedRowPolicy(mutation, sequence) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn applyReplicatedDerivedEffect(self: *DB, change: @import("db/derived/change_journal.zig").Record, primary: ?@import("db/replication_effects.zig").primary_effect.View, sequence: u64) engine.Outcome(u64) {
    return .{ .value = self.applyReplicatedDerivedEffect(change, primary, sequence) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn applyDocumentArtifactChildRangeBatch(self: *DB, child_batch: db.DocumentArtifactChildRangeApplyBatch) engine.Outcome(u64) {
    return .{ .value = self.applyDocumentArtifactChildRangeBatch(child_batch) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn batchNativeInitialChildApply(self: *DB, req: types.BatchRequest, receipt: db.OrderedApplyReceipt) engine.Outcome(void) {
    return .{ .value = self.batchNativeInitialChildApply(req, receipt) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn captureQueryCut(self: *DB, request: @import("db/native_query_cut.zig").Request, cancellation: types.CancellationToken) engine.Outcome(void) {
    return .{ .value = self.captureQueryCut(request, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn openQueryCut(self: *DB, request: @import("db/native_query_cut.zig").Request, cancellation: types.CancellationToken) engine.Outcome(DB) {
    return .{ .value = self.openQueryCut(request, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn runSourcePinCleanupStep(self: *DB) engine.Outcome(bool) {
    return .{ .value = self.runSourcePinCleanupStep() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn runLsmMaintenanceStepBestEffort(self: *DB) engine.Outcome(bool) {
    return .{ .value = self.runLsmMaintenanceStepBestEffort() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn getDocumentArtifactManifest(self: *DB, alloc: std.mem.Allocator, doc_key: []const u8, artifact_name: []const u8) engine.Outcome(?types.DocumentArtifactManifest) {
    return .{ .value = self.getDocumentArtifactManifest(alloc, doc_key, artifact_name) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn listDocumentArtifactManifests(self: *DB, alloc: std.mem.Allocator, doc_key: []const u8) engine.Outcome(types.DocumentArtifactManifestList) {
    return .{ .value = self.listDocumentArtifactManifests(alloc, doc_key) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn updateDocumentArtifactChildRangePlacement(self: *DB, alloc: std.mem.Allocator, doc_key: []const u8, artifact_name: []const u8, update: types.DocumentArtifactChildRangePlacementUpdate) engine.Outcome(bool) {
    return .{ .value = self.updateDocumentArtifactChildRangePlacement(alloc, doc_key, artifact_name, update) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn listArtifactRepairIssuesPage(self: *DB, alloc: std.mem.Allocator, req: types.ArtifactRepairListRequest) engine.Outcome(types.ArtifactRepairListResult) {
    return .{ .value = self.listArtifactRepairIssuesPage(alloc, req) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn discoverRecoverableStartupIndexFailures(self: *DB, alloc: std.mem.Allocator, limit: usize) engine.Outcome(DB.StartupIndexRepairDiscovery) {
    return .{ .value = self.discoverRecoverableStartupIndexFailures(alloc, limit) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn repairRecoverableStartupIndexFailures(self: *DB, alloc: std.mem.Allocator, limit: usize, options: types.ArtifactRepairRunOptions) engine.Outcome(DB.StartupIndexRepairResult) {
    return .{ .value = self.repairRecoverableStartupIndexFailures(alloc, limit, options) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn reprocessDocumentArtifactRange(self: *DB, alloc: std.mem.Allocator, artifact_name: []const u8, req: types.DocumentArtifactTableReprocessRequest) engine.Outcome(types.DocumentArtifactTableReprocessResult) {
    return .{ .value = self.reprocessDocumentArtifactRange(alloc, artifact_name, req) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn updateRange(self: *DB, byte_range: types.ByteRange) engine.Outcome(void) {
    return .{ .value = self.updateRange(byte_range) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn setSplitDeltaFinalSeq(self: *DB, seq: u64) engine.Outcome(void) {
    return .{ .value = self.setSplitDeltaFinalSeq(seq) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn mergeArtifactsPage(self: *DB, alloc: std.mem.Allocator, byte_range: types.ByteRange, after_key: ?[]const u8) engine.Outcome([]types.BatchWrite) {
    return .{ .value = self.mergeArtifactsPage(alloc, byte_range, after_key) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn alignOrderedArtifactCatalog(self: *DB, command: @import("db/artifact_inventory.zig").Command, configs: []const types.IndexConfig, context: @import("db/artifact_reconcile_intent.zig").Context) engine.Outcome(void) {
    return .{ .value = self.alignOrderedArtifactCatalog(command, configs, context) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn advanceOrderedArtifactInitialBuild(self: *DB, configs: []const types.IndexConfig, context: @import("db/artifact_reconcile_intent.zig").Context) engine.Outcome(void) {
    return .{ .value = self.advanceOrderedArtifactInitialBuild(configs, context) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn reconcileArtifactDeleteIndex(self: *DB, name: []const u8, context: @import("db/artifact_reconcile_intent.zig").Context) engine.Outcome(bool) {
    return .{ .value = self.reconcileArtifactDeleteIndex(name, context) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn reconcileArtifactAddEnrichment(self: *DB, cfg: types.EnrichmentConfig, context: @import("db/artifact_reconcile_intent.zig").Context) engine.Outcome(void) {
    return .{ .value = self.reconcileArtifactAddEnrichment(cfg, context) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn artifactInventoryCommand(self: *DB, alloc: std.mem.Allocator) engine.Outcome(@import("db/artifact_inventory.zig").Command) {
    return .{ .value = self.artifactInventoryCommand(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn advanceDocumentAuthorityBackfillPage(self: *DB) engine.Outcome(bool) {
    return .{ .value = self.advanceDocumentAuthorityBackfillPage() catch |err| return .{ .failure = .init(@errorName(err)) } };
}
fn advanceArtifactFootprintPage(self: *DB) engine.Outcome(bool) {
    return .{ .value = self.advanceArtifactFootprintPage() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn runGraphMetricServiceMaintenanceJsonAlloc(self: *DB, alloc: std.mem.Allocator, body: []const u8) engine.Outcome([]u8) {
    return .{ .value = self.runGraphMetricServiceMaintenanceJsonAlloc(alloc, body) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn scheduleGraphMetricBuild(self: *DB, alloc: std.mem.Allocator, index_name: []const u8, metric_name: []const u8, force: bool) engine.Outcome(types.GraphMetricStatus) {
    return .{ .value = self.scheduleGraphMetricBuild(alloc, index_name, metric_name, force) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn rebuildDenseIndexesForTargetCoverage(self: *DB, alloc: std.mem.Allocator) engine.Outcome(usize) {
    return .{ .value = self.rebuildDenseIndexesForTargetCoverage(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn rebuildSparseIndexesForTargetCoverage(self: *DB, alloc: std.mem.Allocator) engine.Outcome(usize) {
    return .{ .value = self.rebuildSparseIndexesForTargetCoverage(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn scanVisit(self: *DB, alloc: Allocator, from_key: []const u8, to_key: []const u8, opts: types.ScanOptions, visitor: types.ScanVisitor) engine.Outcome(void) {
    return .{ .value = self.scanVisit(alloc, from_key, to_key, opts, visitor) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn createDetachedEnrichmentRuntime(self: *DB, enrichment_cfg: *@import("db/enrichment/enrichment_runtime.zig").Config) engine.Outcome(?DB.DetachedEnrichmentRuntime) {
    return .{ .value = self.createDetachedEnrichmentRuntime(enrichment_cfg) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn initResolutionRuntime(self: *DB) engine.Outcome(void) {
    return .{ .value = self.initResolutionRuntime() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn initPromotionRuntime(self: *DB) engine.Outcome(void) {
    return .{ .value = self.initPromotionRuntime() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn initOptionalTtlRuntime(self: *DB, cfg: @import("db/maintenance/ttl_runtime.zig").Config) engine.Outcome(void) {
    return .{ .value = self.initOptionalTtlRuntime(cfg) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn initOptionalTransactionRuntime(self: *DB, cfg: @import("db/maintenance/transaction_runtime.zig").Config) engine.Outcome(void) {
    return .{ .value = self.initOptionalTransactionRuntime(cfg) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn initOptionalTextMergeRuntime(self: *DB, cfg: @import("db/maintenance/text_merge_runtime.zig").Config) engine.Outcome(void) {
    return .{ .value = self.initOptionalTextMergeRuntime(cfg) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn initOptionalSparseCompactionRuntime(self: *DB, cfg: @import("db/maintenance/sparse_compaction_runtime.zig").Config) engine.Outcome(void) {
    return .{ .value = self.initOptionalSparseCompactionRuntime(cfg) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn initOptionalGraphMetricRuntime(self: *DB, cfg: @import("db/maintenance/graph_metric_runtime.zig").Config) engine.Outcome(void) {
    return .{ .value = self.initOptionalGraphMetricRuntime(cfg) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn initOptionalRuntimes(self: *DB, opts: *db.OpenOptions) engine.Outcome(void) {
    return .{ .value = self.initOptionalRuntimes(opts) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn startOptionalRuntimes(self: *DB) engine.Outcome(void) {
    return .{ .value = self.startOptionalRuntimes() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn rebuildIndexWithShadowReplacementOwned(self: *DB, alloc: std.mem.Allocator, cfg: types.IndexConfig, options: types.ArtifactRepairRunOptions, durable_repair_id: ?u128, yield_progress: *DB.ShadowIndexReplacementResult) engine.Outcome(DB.ShadowIndexReplacementResult) {
    return .{ .value = self.rebuildIndexWithShadowReplacementOwned(alloc, cfg, options, durable_repair_id, yield_progress) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn advanceIndexRepairIntent(self: *DB, alloc: std.mem.Allocator, repair_id: u128, options: types.ArtifactRepairRunOptions) engine.Outcome(DB.IndexRepairAdvanceResult) {
    return .{ .value = self.advanceIndexRepairIntent(alloc, repair_id, options) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn rehydrateSchemaJson(self: *DB, schema_json: []const u8) engine.Outcome(bool) {
    return .{ .value = self.rehydrateSchemaJson(schema_json) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn removeResolverWithoutDrain(self: *DB, name: []const u8) engine.Outcome(bool) {
    return .{ .value = self.removeResolverWithoutDrain(name) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn deleteEnrichment(self: *DB, kind: types.EnrichmentKind, name: []const u8) engine.Outcome(bool) {
    return .{ .value = self.deleteEnrichment(kind, name) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn startVectorMigration(self: *DB, request: db.vector_migration.contract.Request) engine.Outcome(void) {
    return .{ .value = self.startVectorMigration(request) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn advanceVectorMigration(self: *DB, job_id: []const u8) engine.Outcome(void) {
    return .{ .value = self.advanceVectorMigration(job_id) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn publishVectorMigration(self: *DB, job_id: []const u8) engine.Outcome(void) {
    return .{ .value = self.publishVectorMigration(job_id) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn observedDynamicFieldCapabilitySetsAlloc(self: *DB, alloc: std.mem.Allocator, observation: @import("db/catalog/index_manager.zig").IndexManager.DynamicFieldObservationQuery) engine.Outcome([]@import("db/catalog/index_manager.zig").IndexManager.ObservedDynamicFieldCapabilitySet) {
    return .{ .value = self.observedDynamicFieldCapabilitySetsAlloc(alloc, observation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn graphHydrateKeysForInternalRead(self: *DB, alloc: std.mem.Allocator, req: types.SearchRequest, keys: []const []const u8) engine.Outcome([]types.SearchHit) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    return .{ .value = self.graphHydrateKeysForInternalRead(alloc, received, keys) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn applyDerivedBatchToIndexAsync(ctx: *@import("db/execution_resources.zig").AsyncContext, batch: @import("db/derived/derived_types.zig").DerivedBatch, index_ref: @import("db/catalog/index_manager.zig").ManagedIndexRef, token: @import("db/derived/derived_executor.zig").CatchUpSessionToken) engine.Outcome(bool) {
    return .{ .value = db.applyDerivedBatchToIndexAsync(@ptrCast(ctx), batch, index_ref, token) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn enrichmentStart(self: *@import("db/enrichment/enrichment_runtime.zig").EnrichmentRuntime) engine.Outcome(void) {
    return .{ .value = self.start() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn enrichmentReloadDurableState(self: *@import("db/enrichment/enrichment_runtime.zig").EnrichmentRuntime) engine.Outcome(void) {
    return .{ .value = self.reloadDurableState() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn enrichmentResumeFrom(self: *@import("db/enrichment/enrichment_runtime.zig").EnrichmentRuntime, sequence: u64, target_sequence: u64) engine.Outcome(void) {
    return .{ .value = self.resumeFrom(sequence, target_sequence) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn enrichmentWaitForAppliedWithVisibilityDeadline(self: *@import("db/enrichment/enrichment_runtime.zig").EnrichmentRuntime, sequence: u64, cancellation: types.CancellationToken, deadline_ns: ?u64) engine.Outcome(void) {
    return .{ .value = self.waitForAppliedWithVisibilityDeadline(sequence, cancellation, deadline_ns) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn enrichmentCatchUpUntil(self: *@import("db/enrichment/enrichment_runtime.zig").EnrichmentRuntime, sequence: u64) engine.Outcome(void) {
    return .{ .value = self.catchUpUntil(sequence) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn enrichmentCatchUpUntilWithVisibilityDeadline(self: *@import("db/enrichment/enrichment_runtime.zig").EnrichmentRuntime, sequence: u64, cancellation: types.CancellationToken, deadline_ns: ?u64) engine.Outcome(void) {
    return .{ .value = self.catchUpUntilWithVisibilityDeadline(sequence, cancellation, deadline_ns) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn admitManagedIndex(self: *DB, cfg: types.IndexConfig) engine.Outcome(?u128) {
    return .{ .value = self.admitManagedIndex(cfg) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn waitForSyncLevelWithCancellation(self: *DB, sync_level: types.SyncLevel, sequence: u64, sync_targets: @import("db/execution_resources.zig").ManagedSyncTargets, cancellation: types.CancellationToken, skip_enrichment_runtime_wait: bool) engine.Outcome(void) {
    return .{ .value = self.waitForSyncLevelWithCancellation(sync_level, sequence, sync_targets, cancellation, skip_enrichment_runtime_wait) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn waitForResolvedTransactionSyncWithCancellation(self: *DB, sync_level: types.SyncLevel, sequence: u64, cancellation: types.CancellationToken) engine.Outcome(void) {
    return .{ .value = self.waitForResolvedTransactionSyncWithCancellation(sync_level, sequence, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn waitForResolvedTransactionSyncTargetWithCancellation(self: *DB, sync_level: types.SyncLevel, sequence: *u64, phase: *u8, cancellation: types.CancellationToken) engine.Outcome(void) {
    return .{ .value = self.waitForResolvedTransactionSyncTargetWithCancellation(sync_level, sequence, phase, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn captureRelationalStatementSnapshot(self: *DB) engine.Outcome(DB.RelationalStatementSnapshot) {
    return .{ .value = self.captureRelationalStatementSnapshot() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn beginRelationalRows(self: *DB, alloc: std.mem.Allocator, request: @import("db/relational_rows.zig").Request) engine.Outcome(@import("db/relational_rows.zig").Reader) {
    return .{ .value = self.beginRelationalRows(alloc, request) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn openDocumentReadSession(self: *DB, alloc: std.mem.Allocator, from_key: []const u8, to_key: []const u8, opts: types.ScanOptions) engine.Outcome(*DB.DocumentReadSession) {
    return .{ .value = self.openDocumentReadSession(alloc, from_key, to_key, opts) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn openRelationalReadSessionAtSnapshot(self: *DB, alloc: std.mem.Allocator, from_key: []const u8, to_key: []const u8, opts: types.ScanOptions, statement: ?*DB.RelationalStatementSnapshot) engine.Outcome(*DB.RelationalReadSession) {
    return .{ .value = self.openRelationalReadSessionAtSnapshot(alloc, from_key, to_key, opts, statement) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn tryStatementReadFence(self: *DB) engine.Outcome(?DB.StatementReadFence) {
    return .{ .value = self.tryStatementReadFence() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn tryEmbeddedBackupReadFence(self: *DB) engine.Outcome(?DB.StatementReadFence) {
    return .{ .value = self.tryEmbeddedBackupReadFence() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn catchUpPendingDerivedReplay(self: *DB) engine.Outcome(void) {
    return .{ .value = self.catchUpPendingDerivedReplay() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn derivedAsyncAppendDerivedBatchRecord(self: *DB, derived_batch: @import("db/derived/derived_types.zig").DerivedBatch) engine.Outcome(u64) {
    return .{ .value = self.derivedAsyncAppendDerivedBatchRecord(derived_batch) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn runDerivedUntil(self: *DB, sequence: u64) engine.Outcome(void) {
    return .{ .value = self.runDerivedUntil(sequence) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn resolvedFilterTextIndexNameAlloc(self: *DB, alloc: Allocator, primary: ?[]const u8, singleton: ?[]const u8) engine.Outcome(?[]u8) {
    return .{ .value = self.resolvedFilterTextIndexNameAlloc(alloc, primary, singleton) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn waitForCurrentIndexTargets(self: *DB, index_names: []const []const u8, deadline_ns: u64) engine.Outcome(u64) {
    return .{ .value = self.waitForCurrentIndexTargets(index_names, deadline_ns) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn runDerivedUntilTargets(self: *DB, sequence: u64, index_names: []const []const u8) engine.Outcome(void) {
    return .{ .value = self.runDerivedUntilTargets(sequence, index_names) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn runRelationalIndexMaintenancePass(self: *DB) engine.Outcome(bool) {
    return .{ .value = self.runRelationalIndexMaintenancePass() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn runRelationalIndexMaintenanceUntilIdle(self: *DB, cancellation: types.CancellationToken, deadline_ns: ?u64) engine.Outcome(void) {
    return .{ .value = self.runRelationalIndexMaintenanceUntilIdle(cancellation, deadline_ns) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn ensureRelationalIndexesReady(self: *DB, names: []const []const u8, cancellation: types.CancellationToken, deadline_ns: ?u64) engine.Outcome(void) {
    return .{ .value = self.ensureRelationalIndexesReady(names, cancellation, deadline_ns) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn runRelationalColumnMaintenancePass(self: *DB) engine.Outcome(usize) {
    return .{ .value = self.runRelationalColumnMaintenancePass() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn rebuildRelationalColumns(self: *DB) engine.Outcome(bool) {
    return .{ .value = self.rebuildRelationalColumns() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn replayGeneratedEnrichmentsFromStoredDocs(self: *DB, alloc: std.mem.Allocator) engine.Outcome(usize) {
    return .{ .value = self.replayGeneratedEnrichmentsFromStoredDocs(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn advanceGraphReplacement(self: *DB) engine.Outcome(bool) {
    return .{ .value = self.advanceGraphReplacement() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn replaceGraphIndex(self: *DB, old: []const u8, cfg: types.IndexConfig) engine.Outcome(u64) {
    return .{ .value = self.replaceGraphIndex(old, cfg) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn rebuildGraphIndexesForTargetCoverage(self: *DB, alloc: std.mem.Allocator) engine.Outcome(void) {
    return .{ .value = self.rebuildGraphIndexesForTargetCoverage(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn beginBulkIngestSession(self: *DB) engine.Outcome(void) {
    return .{ .value = self.beginBulkIngestSession() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn finishBulkIngestSessionWithOptions(self: *DB, options: @import("backend_types.zig").BulkIngestFinishOptions) engine.Outcome(void) {
    return .{ .value = self.finishBulkIngestSessionWithOptions(options) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn beginDenseAutoBulkIngestSession(self: *DB) engine.Outcome(void) {
    return .{ .value = self.beginDenseAutoBulkIngestSession() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn beginPrimaryStoreAutoBulkIngestSession(self: *DB) engine.Outcome(void) {
    return .{ .value = self.beginPrimaryStoreAutoBulkIngestSession() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn finishPrimaryStoreAutoBulkIngestSessionWithOptions(self: *DB, options: @import("backend_types.zig").BulkIngestFinishOptions) engine.Outcome(void) {
    return .{ .value = self.finishPrimaryStoreAutoBulkIngestSessionWithOptions(options) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn rollPrimaryStoreAutoBulkIngestSessionWithOptions(self: *DB, options: @import("backend_types.zig").BulkIngestFinishOptions) engine.Outcome(void) {
    return .{ .value = self.rollPrimaryStoreAutoBulkIngestSessionWithOptions(options) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn finishDenseAutoBulkIngestSessionWithOptions(self: *DB, options: @import("backend_types.zig").BulkIngestFinishOptions) engine.Outcome(void) {
    return .{ .value = self.finishDenseAutoBulkIngestSessionWithOptions(options) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn rollDenseAutoBulkIngestSessionWithOptions(self: *DB, options: @import("backend_types.zig").BulkIngestFinishOptions) engine.Outcome(void) {
    return .{ .value = self.rollDenseAutoBulkIngestSessionWithOptions(options) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn relationalReaderDeinit(self: *@import("db/relational_rows.zig").Reader) engine.Outcome(void) {
    return .{ .value = self.deinit() };
}

fn relationalReaderNextPage(self: *@import("db/relational_rows.zig").Reader, alloc: Allocator, io: ?native.IoBorrow, budget: @import("db/relational_rows.zig").Budget) engine.Outcome(@import("db/relational_rows.zig").Page) {
    var receiver: ?@import("antfly_runtime_abi").io_abi.Receiver = if (io) |borrow| borrow.receive() catch |err| return .{ .failure = .init(@errorName(err)) } else null;
    const received_io: ?std.Io = if (receiver) |*value| value.io() else null;
    return .{ .value = self.nextPage(alloc, received_io, budget) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn relationalReaderNextTypedPage(self: *@import("db/relational_rows.zig").Reader, alloc: Allocator, io: ?native.IoBorrow, budget: @import("db/relational_rows.zig").Budget) engine.Outcome(@import("db/relational_rows.zig").Page) {
    var receiver: ?@import("antfly_runtime_abi").io_abi.Receiver = if (io) |borrow| borrow.receive() catch |err| return .{ .failure = .init(@errorName(err)) } else null;
    const received_io: ?std.Io = if (receiver) |*value| value.io() else null;
    return .{ .value = self.nextTypedPage(alloc, received_io, budget) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn relationalReaderLookupSqlBatch(self: *@import("db/relational_rows.zig").Reader, alloc: Allocator, documents: []const []const u8, layout: *const @import("../sql/catalog.zig").Row.TypedLayout, max_bytes: usize) engine.Outcome(@import("db/relational_rows.zig").Reader.Hydrated) {
    return .{ .value = self.lookupSqlBatch(alloc, documents, layout, max_bytes) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn relationalReaderLookupTypedRow(self: *@import("db/relational_rows.zig").Reader, alloc: Allocator, document: []const u8, native_cells: bool, shared: ?*?*const @import("../sql/catalog.zig").Row.TypedLayout) engine.Outcome(?@import("db/relational_rows.zig").Row) {
    return .{ .value = (if (native_cells) if (shared) |layout| self.lookupSqlRowWithLayout(alloc, document, layout) else self.lookupSqlRow(alloc, document) else self.lookupTypedRow(alloc, document)) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn relationalSessionDeinit(session: *@import("db/relational_read_session.zig").Session) engine.Outcome(void) {
    return .{ .value = session.deinit() };
}

fn relationalSessionCheckpoint(session: *const @import("db/relational_read_session.zig").Session) engine.Outcome(void) {
    return .{ .value = session.checkpoint() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn relationalSessionRangeProofs(session: *@import("db/relational_read_session.zig").Session, alloc: Allocator) engine.Outcome([]@import("range_protection.zig").Proof) {
    return .{ .value = session.rangeProofs(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn relationalSessionNextTypedPage(session: *@import("db/relational_read_session.zig").Session, alloc: Allocator, io: ?native.IoBorrow, budget: @import("db/relational_rows.zig").Budget) engine.Outcome(@import("db/relational_rows.zig").Page) {
    var receiver: ?@import("antfly_runtime_abi").io_abi.Receiver = if (io) |borrow| borrow.receive() catch |err| return .{ .failure = .init(@errorName(err)) } else null;
    const received_io: ?std.Io = if (receiver) |*value| value.io() else null;
    return .{ .value = session.nextTypedPage(alloc, received_io, budget) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn relationalSessionNormalizeRows(session: *@import("db/relational_read_session.zig").Session, alloc: Allocator, writes: []const types.BatchWrite) engine.Outcome([]types.BatchWrite) {
    return .{ .value = session.normalizeRows(alloc, writes) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn documentSessionDeinit(self: *@import("db/document_rows.zig").Session) engine.Outcome(void) {
    return .{ .value = self.deinit() };
}

fn documentSessionRangeProofs(self: *@import("db/document_rows.zig").Session, alloc: std.mem.Allocator) engine.Outcome([]@import("range_protection.zig").Proof) {
    return .{ .value = self.rangeProofs(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn documentSessionNormalizeRows(self: *@import("db/document_rows.zig").Session, alloc: std.mem.Allocator, writes: []const types.BatchWrite) engine.Outcome([]types.BatchWrite) {
    return .{ .value = self.normalizeRows(alloc, writes) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn documentSessionNext(self: *@import("db/document_rows.zig").Session, alloc: std.mem.Allocator, limit: u32) engine.Outcome(@import("relational_read_view.zig").View.Page) {
    return .{ .value = self.next(alloc, limit) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn catchUpPendingDerivedReplayWithProgress(self: *DB, context: *anyopaque, hook: db.ReplayProgressHook, dispatch: @import("named_callback.zig").Dispatch) engine.Outcome(void) {
    const Proxy = struct {
        context: *anyopaque,
        hook: db.ReplayProgressHook,
        dispatch: @import("named_callback.zig").Dispatch,
        fn progress(raw: *anyopaque, index_name: []const u8, value: db.ReplayProgress) !void {
            const proxy: *@This() = @ptrCast(@alignCast(raw));
            return engine.ReplayBoundary.call("progress", proxy.dispatch, proxy.hook, .{ proxy.context, index_name, value });
        }
    };
    var proxy: Proxy = .{ .context = context, .hook = hook, .dispatch = dispatch };
    return .{ .value = self.catchUpPendingDerivedReplayWithProgress(&proxy, Proxy.progress) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
fn relationalSnapshotDeinit(cut: *DB.RelationalStatementSnapshot) engine.Outcome(void) {
    return .{ .value = cut.deinit() };
}
fn statementFenceRelease(fence: *DB.StatementReadFence) engine.Outcome(void) {
    return .{ .value = fence.release() };
}
fn documentRowsUnderLease(lease: *const DB.QueryReadLease, alloc: Allocator, version: u32) engine.Outcome(DB.DocumentRows) {
    return .{ .value = lease.documentRows(alloc, version) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
fn documentRowsLookupProjectedBatch(reader: *DB.DocumentRows, ids: []const []const u8, fields: []const []const u8, max_bytes: usize) engine.Outcome(DB.DocumentRows.ProjectedBatch) {
    return .{ .value = reader.lookupProjectedBatch(ids, fields, max_bytes) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
fn documentRowsLookupBatch(reader: *DB.DocumentRows, ids: []const []const u8, max_bytes: usize) engine.Outcome(DB.DocumentRows.Batch) {
    return .{ .value = reader.lookupBatch(ids, max_bytes) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
fn documentRowsLookup(reader: *DB.DocumentRows, key: []const u8) engine.Outcome([]const u8) {
    return .{ .value = reader.lookup(key) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
fn documentRowsReleasePage(reader: *DB.DocumentRows) engine.Outcome(void) {
    return .{ .value = reader.releasePage() };
}
fn documentRowsDeinit(reader: *DB.DocumentRows) engine.Outcome(void) {
    return .{ .value = reader.deinit() };
}
fn relationalRowsUnderLease(lease: *const DB.QueryReadLease, alloc: Allocator, fields: []const []const u8, version: u32) engine.Outcome(@import("db/relational_rows.zig").Reader) {
    return .{ .value = lease.relationalRows(alloc, fields, version) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
fn resolveNativeRestoreOpenPlan(target_path: []const u8, requested_options: db.OpenOptions) engine.Outcome(db.NativeRestoreOpenPlan) {
    return .{ .value = DB.resolveNativeRestoreOpenPlan(target_path, requested_options) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn vectorMigrationCommand(self: *DB, alloc: std.mem.Allocator, command: db.vector_migration.contract.Command) engine.Outcome([]u8) {
    return .{ .value = self.vectorMigrationCommand(alloc, command) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn vectorMigrationStatus(self: *DB, alloc: std.mem.Allocator) engine.Outcome(?[]u8) {
    return .{ .value = self.vectorMigrationStatus(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn admitTransactionRecovery(self: *DB) engine.Outcome(?db.MutationBarrier.SharedLease) {
    return .{ .value = self.admitTransactionRecovery() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn runPrimaryLsmMaintenanceStep(self: *DB) engine.Outcome(bool) {
    return .{ .value = self.runPrimaryLsmMaintenanceStep() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn runPrimaryLsmMaintenanceStepBestEffort(self: *DB) engine.Outcome(bool) {
    return .{ .value = self.runPrimaryLsmMaintenanceStepBestEffort() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn runDueLsmObsoleteReclaimUntilIdle(self: *DB, max_steps: usize) engine.Outcome(usize) {
    return .{ .value = self.runDueLsmObsoleteReclaimUntilIdle(max_steps) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn applyOrderedCommittedMutation(self: *DB, request: types.BatchRequest, identity: db.OrderedApplyReceipt, bypass_write_gate: bool) engine.Outcome(void) {
    return .{ .value = self.applyOrderedCommittedMutation(request, identity, bypass_write_gate) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn recoverOrderedSourceAdmission(self: *DB, request: types.BatchRequest, identity: db.OrderedApplyReceipt, bypass_write_gate: bool) engine.Outcome(void) {
    return .{ .value = self.recoverOrderedSourceAdmission(request, identity, bypass_write_gate) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn batchNativeFkGenerationApply(self: *DB, req: types.BatchRequest) engine.Outcome(void) {
    return .{ .value = self.batchNativeFkGenerationApply(req) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn pinTransferablePrimarySnapshot(self: *DB) engine.Outcome(DB.TransferablePrimarySnapshot) {
    return .{ .value = self.pinTransferablePrimarySnapshot() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn verifyTransferablePrimarySnapshot(self: *DB, namespace: [24]u8, receipt: ?db.OrderedApplyReceipt) engine.Outcome(void) {
    return .{ .value = self.verifyTransferablePrimarySnapshot(namespace, receipt) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn verifyTransferablePrimarySnapshotWithOrigin(self: *DB, namespace: [24]u8, receipt: ?db.OrderedApplyReceipt, origin: @import("db/native_backup.zig").Origin) engine.Outcome(void) {
    return .{ .value = self.verifyTransferablePrimarySnapshotWithOrigin(namespace, receipt, origin) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn stepSchemaOwnerRowAudit(self: *DB, owner_group: u64, binding: @import("db/relational_schema_owner_contract.zig").Binding, inputs: []const u8, budget: @import("db/relational_rows.zig").Budget, signing_root_dir: []const u8, signing_root_incarnation: u128, context: @import("../api/operation.zig").RequestContext) engine.Outcome(@import("db/relational_schema_audit_checkpoint.zig").Result) {
    return .{ .value = self.stepSchemaOwnerRowAudit(owner_group, binding, inputs, budget, signing_root_dir, signing_root_incarnation, context) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn orderedMutationAlreadyApplied(self: *DB, identity: db.OrderedApplyReceipt) engine.Outcome(bool) {
    return .{ .value = self.orderedMutationAlreadyApplied(identity) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn loadRowPolicyReceipt(self: *DB, generation: u64, phase: @import("../system_catalog/policies.zig").Publication.Phase) engine.Outcome(@import("db/row_policy_bundle.zig").Receipt) {
    return .{ .value = self.loadRowPolicyReceipt(generation, phase) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn listArtifactRepairIssues(self: *DB, alloc: std.mem.Allocator, artifact_kind: ?types.ArtifactRepairKind, index_name: ?[]const u8, limit: usize) engine.Outcome([]types.ArtifactRepairIssue) {
    return .{ .value = self.listArtifactRepairIssues(alloc, artifact_kind, index_name, limit) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn hasPendingIndexRepairIntents(self: *const DB, alloc: std.mem.Allocator) engine.Outcome(bool) {
    return .{ .value = self.hasPendingIndexRepairIntents(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn completedManagedDenseGenerationIsServiceable(self: *DB, alloc: std.mem.Allocator, index_name: []const u8) engine.Outcome(bool) {
    return .{ .value = self.completedManagedDenseGenerationIsServiceable(alloc, index_name) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn markDenseReplayArtifactRepairRequired(self: *DB, alloc: std.mem.Allocator, index_name: []const u8) engine.Outcome(bool) {
    return .{ .value = self.markDenseReplayArtifactRepairRequired(alloc, index_name) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn materializeManagedIndexAdmission(self: *DB, alloc: std.mem.Allocator, index_name: []const u8) engine.Outcome(?u128) {
    return .{ .value = self.materializeManagedIndexAdmission(alloc, index_name) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn repairArtifactIssuesWithRequestOptions(self: *DB, alloc: std.mem.Allocator, req: types.ArtifactRepairRunRequest, options: types.ArtifactRepairRunOptions) engine.Outcome(types.ArtifactRepairResult) {
    return .{ .value = self.repairArtifactIssuesWithRequestOptions(alloc, req, options) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn repairArtifactIssues(self: *DB, alloc: std.mem.Allocator, artifact_kind: ?types.ArtifactRepairKind, limit: usize) engine.Outcome(types.ArtifactRepairResult) {
    return .{ .value = self.repairArtifactIssues(alloc, artifact_kind, limit) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn getTimestamp(self: *DB, alloc: std.mem.Allocator, key: []const u8) engine.Outcome(u64) {
    return .{ .value = self.getTimestamp(alloc, key) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn findMedianKey(self: *DB, alloc: std.mem.Allocator) engine.Outcome([]u8) {
    return .{ .value = self.findMedianKey(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn clearSplitDeltaFinalSeq(self: *DB) engine.Outcome(void) {
    return .{ .value = self.clearSplitDeltaFinalSeq() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn appendStagedSnapshotDocuments(self: *DB, staged_generation: *const @import("db/generation_lifecycle.zig").StagedGeneration, byte_range: types.ByteRange, writes: []const types.BatchWrite) engine.Outcome(void) {
    return .{ .value = self.appendStagedSnapshotDocuments(staged_generation, byte_range, writes) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn finishStagedSnapshotRange(self: *DB, staged_generation: *const @import("db/generation_lifecycle.zig").StagedGeneration, byte_range: types.ByteRange) engine.Outcome(void) {
    return .{ .value = self.finishStagedSnapshotRange(staged_generation, byte_range) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn replaceSplitBootstrap(self: *DB, alloc: std.mem.Allocator, byte_range: types.ByteRange, writes: []const types.BatchWrite, base_delta_sequence: u64, marker: @import("db/range_state.zig").SplitBootstrapMarker) engine.Outcome(bool) {
    return .{ .value = self.replaceSplitBootstrap(alloc, byte_range, writes, base_delta_sequence, marker) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn completeSplitBootstrap(self: *DB, alloc: std.mem.Allocator, byte_range: types.ByteRange, base_delta_sequence: u64, marker: @import("db/range_state.zig").SplitBootstrapMarker) engine.Outcome(bool) {
    return .{ .value = self.completeSplitBootstrap(alloc, byte_range, base_delta_sequence, marker) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn split(self: *DB, curr_range: types.ByteRange, split_key: []const u8, dest_dir1: []const u8, dest_dir2: []const u8, prepare_only: bool) engine.Outcome(void) {
    return .{ .value = self.split(curr_range, split_key, dest_dir1, dest_dir2, prepare_only) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn drainSnapshotMaintenance(self: *DB, deadline_ns: u64) engine.Outcome(void) {
    return .{ .value = self.drainSnapshotMaintenance(deadline_ns) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn sealBackupCohort(self: *DB, id: []const u8, expected: @import("db/relational_integrity_topology.zig").Fence, cancellation: types.CancellationToken) engine.Outcome(@import("db/native_backup_seal.zig").Handle) {
    return .{ .value = self.sealBackupCohort(id, expected, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn exportBackupCohort(self: *DB, handle: @import("db/native_backup_seal.zig").Handle, id: []const u8, cancellation: types.CancellationToken) engine.Outcome(u64) {
    return .{ .value = self.exportBackupCohort(handle, id, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn exportBackupCohortPortable(self: *DB, handle: @import("db/native_backup_seal.zig").Handle, writer: engine.WriterBorrow, options: engine.PortableExportOptions, cancellation: types.CancellationToken) engine.Outcome(void) {
    var receiving_writer = writer.receive();
    var spool_receiver: ?@import("antfly_runtime_abi").io_abi.Receiver = if (options.spool) |spool| spool.io.receive() catch |err| return .{ .failure = .init(@errorName(err)) } else null;
    var received_options = options.value;
    if (spool_receiver) |*receiver| received_options.spool = .{ .io = receiver.io(), .file = options.spool.?.file };
    self.exportBackupCohortPortable(handle, receiving_writer.writer(), received_options, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) };
    receiving_writer.finish() catch |err| return .{ .failure = .init(@errorName(err)) };
    return .{ .value = {} };
}

fn mergeDocumentsPage(self: *DB, alloc: std.mem.Allocator, byte_range: types.ByteRange, after_key: ?[]const u8) engine.Outcome([]types.BatchWrite) {
    return .{ .value = self.mergeDocumentsPage(alloc, byte_range, after_key) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn mergeCleanupKeysPage(self: *DB, alloc: std.mem.Allocator, byte_range: types.ByteRange, after_key: ?[]const u8) engine.Outcome([]types.BatchWrite) {
    return .{ .value = self.mergeCleanupKeysPage(alloc, byte_range, after_key) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn repairVerifiedPrimarySnapshot(alloc: std.mem.Allocator, staged: *const @import("db/generation_lifecycle.zig").StagedGeneration, namespace: [24]u8, receipt: ?db.OrderedApplyReceipt, root_generation: u64) engine.Outcome(void) {
    return .{ .value = DB.repairVerifiedPrimarySnapshot(alloc, staged, namespace, receipt, root_generation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn restoreSnapshotToStagedGeneration(staged_generation: *const @import("db/generation_lifecycle.zig").StagedGeneration, alloc: std.mem.Allocator, snapshot_root: []const u8, path: []const u8, opts: db.OpenOptions) engine.Outcome(void) {
    return .{ .value = DB.restoreSnapshotToStagedGeneration(staged_generation, alloc, snapshot_root, path, opts) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn restoreIdentityPreservingSnapshotToStagedGeneration(staged_generation: *const @import("db/generation_lifecycle.zig").StagedGeneration, alloc: std.mem.Allocator, snapshot_root: []const u8, path: []const u8, opts: db.OpenOptions, expected_namespace: @import("db/doc_identity.zig").Namespace) engine.Outcome(void) {
    return .{ .value = DB.restoreIdentityPreservingSnapshotToStagedGeneration(staged_generation, alloc, snapshot_root, path, opts, expected_namespace) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn openVerifiedRestoreSourceWithCancellation(staged_generation: *const @import("db/generation_lifecycle.zig").StagedGeneration, alloc: std.mem.Allocator, snapshot_root: []const u8, path: []const u8, opts: db.OpenOptions, expected_namespace: @import("db/doc_identity.zig").Namespace, cancellation: types.CancellationToken) engine.Outcome(DB) {
    return .{ .value = DB.openVerifiedRestoreSourceWithCancellation(staged_generation, alloc, snapshot_root, path, opts, expected_namespace, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn recoverIncompleteRestoreImportIfNeededWithIo(alloc: std.mem.Allocator, io: native.IoBorrow, path: []const u8, opts: db.OpenOptions) engine.Outcome(bool) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = DB.recoverIncompleteRestoreImportIfNeededWithIo(alloc, received_io, path, opts) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn markRestorePrimaryRestoredForPathWithArtifact(alloc: std.mem.Allocator, path: []const u8, backup_id: []const u8, location: []const u8, artifact_sha256: []const u8, snapshot_path: []const u8, group_id: u64) engine.Outcome(void) {
    return .{ .value = DB.markRestorePrimaryRestoredForPathWithArtifact(alloc, path, backup_id, location, artifact_sha256, snapshot_path, group_id) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn markRestorePrimaryRestoredForPathWithIdentityWithIo(alloc: std.mem.Allocator, io: native.IoBorrow, path: []const u8, identity: db.RestoreIdentity) engine.Outcome(void) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = DB.markRestorePrimaryRestoredForPathWithIdentityWithIo(alloc, received_io, path, identity) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn restoreRuntimeRepairNeeded(self: *DB) engine.Outcome(bool) {
    return .{ .value = self.restoreRuntimeRepairNeeded() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn prepareRestoreDurabilityRetryIfNeeded(self: *DB, alloc: std.mem.Allocator) engine.Outcome(bool) {
    return .{ .value = self.prepareRestoreDurabilityRetryIfNeeded(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn repairRestoreRuntimeStateStepIfNeeded(self: *DB, alloc: std.mem.Allocator) engine.Outcome(bool) {
    return .{ .value = self.repairRestoreRuntimeStateStepIfNeeded(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn repairRestoreRuntimeStateStepIfNeededWithIoAndRepairOptions(self: *DB, alloc: std.mem.Allocator, io: native.IoBorrow, repair_options: types.ArtifactRepairRunOptions) engine.Outcome(bool) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = self.repairRestoreRuntimeStateStepIfNeededWithIoAndRepairOptions(alloc, received_io, repair_options) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn installPublishedChildSchema(self: *DB, alloc: std.mem.Allocator, schema_json: []const u8, publication: DB.PublishedChildSchema) engine.Outcome(void) {
    return .{ .value = self.installPublishedChildSchema(alloc, schema_json, publication) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn beginTransactionWithIdAndParticipantsCreatedAtAndRole(self: *DB, txn_id: @import("transactions.zig").TxnId, timestamp_ns: u64, created_at_ns: u64, participants: []const []const u8, coordinator: bool) engine.Outcome(@import("transactions.zig").TxnId) {
    return .{ .value = self.beginTransactionWithIdAndParticipantsCreatedAtAndRole(txn_id, timestamp_ns, created_at_ns, participants, coordinator) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn beginTransactionScoped(self: *DB, txn_id: @import("transactions.zig").TxnId, timestamp_ns: u64, created_at_ns: u64, participants: []const []const u8, coordinator: bool, retain_terminal: bool, scope: ?[32]u8) engine.Outcome(@import("transactions.zig").TxnId) {
    return .{ .value = self.beginTransactionScoped(txn_id, timestamp_ns, created_at_ns, participants, coordinator, retain_terminal, scope) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn beginReplicatedTransactionScoped(self: *DB, txn_id: @import("transactions.zig").TxnId, timestamp_ns: u64, created_at_ns: u64, participants: []const []const u8, coordinator: bool, retain_terminal: bool, identity: db.OrderedApplyReceipt, scope: ?[32]u8) engine.Outcome(@import("transactions.zig").TxnId) {
    return .{ .value = self.beginReplicatedTransactionScoped(txn_id, timestamp_ns, created_at_ns, participants, coordinator, retain_terminal, identity, scope) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeIntents(self: *DB, txn_id: @import("transactions.zig").TxnId, intents: []const @import("transactions.zig").WriteIntent, predicates: []const @import("transactions.zig").VersionPredicate) engine.Outcome(void) {
    return .{ .value = self.writeIntents(txn_id, intents, predicates) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeTransaction(self: *DB, txn_id: types.TxnId, req: types.TransactionIntentRequest) engine.Outcome(void) {
    return .{ .value = self.writeTransaction(txn_id, req) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn resolveTransactionIntentsWithSyncLevelAndCancellation(self: *DB, txn_id: @import("transactions.zig").TxnId, status: @import("transactions.zig").TxnStatus, commit_version: u64, sync_level: types.SyncLevel, visibility_cancellation: types.CancellationToken) engine.Outcome(void) {
    return .{ .value = self.resolveTransactionIntentsWithSyncLevelAndCancellation(txn_id, status, commit_version, sync_level, visibility_cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn resolveReplicatedTransactionAtOrderedReceipt(self: *DB, txn_id: @import("transactions.zig").TxnId, status: @import("transactions.zig").TxnStatus, commit_version: u64, sync_level: types.SyncLevel, visibility_cancellation: types.CancellationToken, identity: db.OrderedApplyReceipt, resolved_participant: ?[]const u8) engine.Outcome(void) {
    return .{ .value = self.resolveReplicatedTransactionAtOrderedReceipt(txn_id, status, commit_version, sync_level, visibility_cancellation, identity, resolved_participant) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn relationalTopologyIdentity(self: *DB) engine.Outcome(DB.RelationalTopologyIdentity) {
    return .{ .value = self.relationalTopologyIdentity() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn relationalHandoffManifest(self: *DB, alloc: std.mem.Allocator, source: @import("db/relational_integrity_topology.zig").Fence, destination: @import("db/relational_integrity_topology.zig").Fence, lower: []const u8, upper: []const u8, primary_sequence: u64) engine.Outcome(@import("db/relational_integrity_handoff.zig").Manifest) {
    return .{ .value = self.relationalHandoffManifest(alloc, source, destination, lower, upper, primary_sequence) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn relationalHandoffPage(self: *DB, alloc: std.mem.Allocator, manifest: @import("db/relational_integrity_handoff.zig").Manifest, progress: @import("db/relational_integrity_handoff.zig").Progress) engine.Outcome(@import("db/relational_integrity_handoff.zig").Page) {
    return .{ .value = self.relationalHandoffPage(alloc, manifest, progress) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn markTransactionParticipantResolved(self: *DB, txn_id: @import("transactions.zig").TxnId, participant: []const u8) engine.Outcome(void) {
    return .{ .value = self.markTransactionParticipantResolved(txn_id, participant) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn markTransactionParticipantsResolved(self: *DB, txn_id: @import("transactions.zig").TxnId, participants: []const []const u8) engine.Outcome(void) {
    return .{ .value = self.markTransactionParticipantsResolved(txn_id, participants) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn markReplicatedTransactionParticipantResolvedAtOrderedReceipt(self: *DB, txn_id: @import("transactions.zig").TxnId, participant: []const u8, identity: db.OrderedApplyReceipt) engine.Outcome(void) {
    return .{ .value = self.markReplicatedTransactionParticipantResolvedAtOrderedReceipt(txn_id, participant, identity) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn markReplicatedTransactionParticipantsResolvedAtOrderedReceipt(self: *DB, txn_id: @import("transactions.zig").TxnId, participants: []const []const u8, identity: db.OrderedApplyReceipt) engine.Outcome(void) {
    return .{ .value = self.markReplicatedTransactionParticipantsResolvedAtOrderedReceipt(txn_id, participants, identity) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn cleanupReplicatedTransactionAtOrderedReceipt(self: *DB, txn_id: @import("transactions.zig").TxnId, cutoff_timestamp: u64, retained_cutoff_timestamp: u64, identity: db.OrderedApplyReceipt) engine.Outcome(bool) {
    return .{ .value = self.cleanupReplicatedTransactionAtOrderedReceipt(txn_id, cutoff_timestamp, retained_cutoff_timestamp, identity) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn getEdges(self: *DB, alloc: std.mem.Allocator, index_name: []const u8, key: []const u8, edge_type: []const u8, direction: @import("../graph/graph.zig").EdgeDirection) engine.Outcome([]@import("../graph/graph.zig").Edge) {
    return .{ .value = self.getEdges(alloc, index_name, key, edge_type, direction) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn addEnrichment(self: *DB, cfg: types.EnrichmentConfig) engine.Outcome(void) {
    return .{ .value = self.addEnrichment(cfg) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn upsertEnrichment(self: *DB, cfg: types.EnrichmentConfig) engine.Outcome(@import("db/catalog/index_manager.zig").IndexManager.EnrichmentUpsertResult) {
    return .{ .value = self.upsertEnrichment(cfg) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn listDerivedReplayDebt(self: *DB, alloc: std.mem.Allocator) engine.Outcome([]db.DerivedReplayDebtStatus) {
    return .{ .value = self.listDerivedReplayDebt(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn waitForCurrentSyncLevelWithCancellation(self: *DB, sync_level: types.SyncLevel, cancellation: types.CancellationToken) engine.Outcome(void) {
    return .{ .value = self.waitForCurrentSyncLevelWithCancellation(sync_level, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn refreshGraphMetric(self: *DB, alloc: std.mem.Allocator, index_name: []const u8, metric_name: []const u8) engine.Outcome(types.GraphMetricStatus) {
    return .{ .value = self.refreshGraphMetric(alloc, index_name, metric_name) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn rebuildGraphMetric(self: *DB, alloc: std.mem.Allocator, index_name: []const u8, metric_name: []const u8) engine.Outcome(types.GraphMetricStatus) {
    return .{ .value = self.rebuildGraphMetric(alloc, index_name, metric_name) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn pauseGraphMetricMaintenance(self: *DB, alloc: std.mem.Allocator, index_name: []const u8, metric_name: []const u8) engine.Outcome(types.GraphMetricStatus) {
    return .{ .value = self.pauseGraphMetricMaintenance(alloc, index_name, metric_name) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn resumeGraphMetricMaintenance(self: *DB, alloc: std.mem.Allocator, index_name: []const u8, metric_name: []const u8) engine.Outcome(types.GraphMetricStatus) {
    return .{ .value = self.resumeGraphMetricMaintenance(alloc, index_name, metric_name) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn ensureGraphMetricPlannedBuild(self: *DB, alloc: std.mem.Allocator, index_name: []const u8, metric_name: []const u8, target_generation: u64) engine.Outcome(types.GraphMetricStatus) {
    return .{ .value = self.ensureGraphMetricPlannedBuild(alloc, index_name, metric_name, target_generation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn runGraphMetricPlannedWorkerPageStepAt(self: *DB, index_name: []const u8, metric_name: []const u8, worker_id: []const u8, now_ms: u64) engine.Outcome(@import("../graph/graph.zig").GraphIndex.GraphMetricBuildWorkerStepResult) {
    return .{ .value = self.runGraphMetricPlannedWorkerPageStepAt(index_name, metric_name, worker_id, now_ms) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn runGraphMetricPlannedCoordinatorStepAt(self: *DB, index_name: []const u8, metric_name: []const u8, now_ms: u64) engine.Outcome(@import("../graph/graph.zig").GraphIndex.GraphMetricBuildWorkerStepResult) {
    return .{ .value = self.runGraphMetricPlannedCoordinatorStepAt(index_name, metric_name, now_ms) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn runGraphMetricPlannedDrain(self: *DB, alloc: std.mem.Allocator, index_name: []const u8, metric_name: []const u8, target_generation: u64, options: @import("../graph/graph.zig").GraphIndex.GraphMetricPlannedDrainOptions) engine.Outcome(types.GraphMetricStatus) {
    return .{ .value = self.runGraphMetricPlannedDrain(alloc, index_name, metric_name, target_generation, options) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn refreshDensePostingPayloadPageBestEffort(self: *DB) engine.Outcome(DB.PostingRefreshProgress) {
    return .{ .value = self.refreshDensePostingPayloadPageBestEffort() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn publishCompletedDensePostingCheckpoints(self: *DB) engine.Outcome(DB.NativePublicationResult) {
    return .{ .value = self.publishCompletedDensePostingCheckpoints() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn publishVectorBlockBasesOnlineReported(self: *DB, options: @import("db/catalog/index_manager.zig").IndexManager.OnlineVectorBlockPublicationOptions) engine.Outcome(DB.NativePublicationResult) {
    return .{ .value = self.publishVectorBlockBasesOnlineReported(options) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn finalizeDenseProjectionLifecycleForIdle(self: *DB) engine.Outcome(bool) {
    return .{ .value = self.finalizeDenseProjectionLifecycleForIdle() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn publishVectorBlockBasesAtStableTip(self: *DB) engine.Outcome(usize) {
    return .{ .value = self.publishVectorBlockBasesAtStableTip() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn hasPendingDenseArtifactRebuild(self: *DB, alloc: std.mem.Allocator) engine.Outcome(bool) {
    return .{ .value = self.hasPendingDenseArtifactRebuild(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn denseArtifactRebuildMaintenanceNeeded(self: *DB, alloc: std.mem.Allocator) engine.Outcome(bool) {
    return .{ .value = self.denseArtifactRebuildMaintenanceNeeded(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn stats(self: *DB, alloc: std.mem.Allocator) engine.Outcome(types.DBStats) {
    return .{ .value = self.stats(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn runtimeStatusStatsConsistent(self: *DB, alloc: std.mem.Allocator) engine.Outcome(types.DBStats) {
    return .{ .value = self.runtimeStatusStatsConsistent(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn runtimeStatusStatsConsistentIfAvailable(self: *DB, alloc: std.mem.Allocator) engine.Outcome(?types.DBStats) {
    return .{ .value = self.runtimeStatusStatsConsistentIfAvailable(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn runtimeStatusObservationConsistentIfAvailable(self: *DB, alloc: std.mem.Allocator) engine.Outcome(?DB.RuntimeStatusObservation) {
    return .{ .value = self.runtimeStatusObservationConsistentIfAvailable(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn reassignIdentityNamespaceForInternalTransition(self: *DB, namespace: @import("db/doc_identity.zig").Namespace) engine.Outcome(void) {
    return .{ .value = self.reassignIdentityNamespaceForInternalTransition(namespace) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn attachRestoreStagingReplicationMirror(self: *DB, mirror: ?db.ReplicationAsyncEffectMirror) engine.Outcome(void) {
    return .{ .value = self.attachRestoreStagingReplicationMirror(mirror) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn reserveRestoreStagingScoped(self: *DB, alloc: std.mem.Allocator, scope: @import("db/restore_staging.zig").Scope) engine.Outcome(void) {
    return .{ .value = self.reserveRestoreStagingScoped(alloc, scope) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn installRestoreStagingReadSchema(self: *DB, alloc: std.mem.Allocator, scope: @import("db/restore_staging.zig").Scope, read_json: []const u8) engine.Outcome(void) {
    return .{ .value = self.installRestoreStagingReadSchema(alloc, scope, read_json) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn installRestoreStagingBootstrap(self: *DB, alloc: std.mem.Allocator, snapshot_descriptor: @import("db/restore_staging.zig").OwnerBootstrap) engine.Outcome(void) {
    return .{ .value = self.installRestoreStagingBootstrap(alloc, snapshot_descriptor) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn readRestoreStagingBootstrap(self: *DB, alloc: std.mem.Allocator) engine.Outcome(?std.json.Parsed(@import("db/restore_staging.zig").OwnerBootstrap)) {
    return .{ .value = self.readRestoreStagingBootstrap(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn cancelColdInitialChildForRetirement(self: *DB, expected: @import("db/relational_initial_child_publication.zig").Record, cancel_revision: u64) engine.Outcome(void) {
    return .{ .value = self.cancelColdInitialChildForRetirement(expected, cancel_revision) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn importRestoreStagingPage(self: *DB, alloc: std.mem.Allocator, scope: @import("db/restore_staging.zig").Scope, source: *DB, max_rows: usize, cancellation: types.CancellationToken) engine.Outcome(@import("db/restore_staging.zig").Phase) {
    return .{ .value = self.importRestoreStagingPage(alloc, scope, source, max_rows, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn importPortableIntoEmpty(self: *DB, alloc: std.mem.Allocator, backup: []const u8, target_identity: @import("db/doc_identity.zig").Namespace) engine.Outcome(void) {
    return .{ .value = self.importPortableIntoEmpty(alloc, backup, target_identity) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn currentIdentityReadGenerationForRequest(self: *DB, requested: ?u64) engine.Outcome(u64) {
    return .{ .value = self.currentIdentityReadGenerationForRequest(requested) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn generatedCoverageReplayNeeded(self: *DB, alloc: std.mem.Allocator) engine.Outcome(bool) {
    return .{ .value = self.generatedCoverageReplayNeeded(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn generatedCoverageRecoveryOwnedByInitialBuild(self: *DB, alloc: std.mem.Allocator) engine.Outcome(bool) {
    return .{ .value = self.generatedCoverageRecoveryOwnedByInitialBuild(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn ensureGeneratedCoverageRecoveryIntents(self: *DB, alloc: std.mem.Allocator) engine.Outcome(usize) {
    return .{ .value = self.ensureGeneratedCoverageRecoveryIntents(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn beginQueryReadLease(self: *DB) engine.Outcome(DB.QueryReadLease) {
    return .{ .value = self.beginQueryReadLease() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn searchRequestAtCurrentIdentityGeneration(self: *DB, req: types.SearchRequest) engine.Outcome(types.SearchRequest) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    var result = self.searchRequestAtCurrentIdentityGeneration(received) catch |err| return .{ .failure = .init(@errorName(err)) };
    // The receiving proxy borrows this stack frame. Returned requests must
    // retain the caller's predicate for subsequent searches.
    result.native_key_predicate = req.native_key_predicate;
    return .{ .value = result };
}

fn graphHasIncomingEdgesForInternalReadBoundedAt(self: *DB, alloc: std.mem.Allocator, index_name: []const u8, keys: []const []const u8, expected: @import("db/catalog/index_manager.zig").IndexManager.CoverageIdentity, identity_read_generation: ?u64, ttl_now_ns: u64, max_scanned_rows: usize) engine.Outcome(@import("../graph/graph.zig").GraphIndex.IncomingProbeResult) {
    return .{ .value = self.graphHasIncomingEdgesForInternalReadBoundedAt(alloc, index_name, keys, expected, identity_read_generation, ttl_now_ns, max_scanned_rows) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn authorizeOfflineVectorMigrationCandidate(self: *DB, stage: *const @import("db/generation_lifecycle.zig").StagedGeneration) engine.Outcome(void) {
    return .{ .value = self.authorizeOfflineVectorMigrationCandidate(stage) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn setQueryVisibilityHook(self: *DB, hook: ?db.QueryVisibilityHook) engine.Outcome(void) {
    return .{ .value = self.setQueryVisibilityHook(hook) };
}

fn setCoordinatedTtl(self: *DB, port: ?db.coordinated_ttl.Port) engine.Outcome(void) {
    return .{ .value = self.setCoordinatedTtl(port) };
}

fn beginTeardown(self: *DB) engine.Outcome(void) {
    return .{ .value = self.beginTeardown() };
}

fn recoverLocalTransactionRecords(self: *DB, cutoff_ns: u64, now_ns: u64) engine.Outcome(@import("transactions.zig").RecoveryStats) {
    return .{ .value = self.recoverLocalTransactionRecords(cutoff_ns, now_ns) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn abortPrimaryStoreAutoBulkIngestSession(self: *DB) engine.Outcome(void) {
    return .{ .value = self.abortPrimaryStoreAutoBulkIngestSession() };
}

fn abortBulkIngestSession(self: *DB) engine.Outcome(void) {
    return .{ .value = self.abortBulkIngestSession() };
}

fn lsmMaintenanceScore(self: *DB) engine.Outcome(u64) {
    return .{ .value = self.lsmMaintenanceScore() };
}

fn lsmMaintenanceDebtHint(self: *DB) engine.Outcome(u64) {
    return .{ .value = self.lsmMaintenanceDebtHint() };
}

fn primaryLsmMaintenanceDebtHint(self: *DB) engine.Outcome(u64) {
    return .{ .value = self.primaryLsmMaintenanceDebtHint() };
}

fn nextLsmMaintenanceWakeDelayNsBestEffort(self: *DB) engine.Outcome(?u64) {
    return .{ .value = self.nextLsmMaintenanceWakeDelayNsBestEffort() };
}

fn nextPrimaryLsmMaintenanceWakeDelayNsBestEffort(self: *DB) engine.Outcome(?u64) {
    return .{ .value = self.nextPrimaryLsmMaintenanceWakeDelayNsBestEffort() };
}

fn snapshotAsyncIndexingStats(self: *DB) engine.Outcome(types.AsyncIndexingStats) {
    return .{ .value = self.snapshotAsyncIndexingStats() };
}

fn snapshotLsmMaintenanceStats(self: *DB) engine.Outcome(@import("lsm_backend/mod.zig").Backend.MaintenanceStats) {
    return .{ .value = self.snapshotLsmMaintenanceStats() };
}

fn snapshotLsmOwnerStatsAlloc(self: *DB, alloc: std.mem.Allocator) engine.Outcome([]DB.LsmOwnerStats) {
    return .{ .value = self.snapshotLsmOwnerStatsAlloc(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn trySnapshotLsmOwnerStatsAlloc(self: *DB, alloc: std.mem.Allocator) engine.Outcome(?[]DB.LsmOwnerStats) {
    return .{ .value = self.trySnapshotLsmOwnerStatsAlloc(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn trySnapshotLsmMaintenanceStats(self: *DB) engine.Outcome(?@import("lsm_backend/mod.zig").Backend.MaintenanceStats) {
    return .{ .value = self.trySnapshotLsmMaintenanceStats() };
}

fn snapshotLsmWriteStats(self: *DB) engine.Outcome(@import("lsm_backend/mod.zig").Backend.WriteStats) {
    return .{ .value = self.snapshotLsmWriteStats() };
}

fn trySnapshotLsmWriteStats(self: *DB) engine.Outcome(?@import("lsm_backend/mod.zig").Backend.WriteStats) {
    return .{ .value = self.trySnapshotLsmWriteStats() };
}

fn trySnapshotTextMemoryAttributionStats(self: *DB) engine.Outcome(?@import("db/catalog/index_manager.zig").TextMemoryAttributionStats) {
    return .{ .value = self.trySnapshotTextMemoryAttributionStats() };
}

fn trySnapshotTextMergeStats(self: *DB) engine.Outcome(?types.TextMergeStats) {
    return .{ .value = self.trySnapshotTextMergeStats() };
}

fn hasActiveDenseBulkWork(self: *const DB) engine.Outcome(bool) {
    return .{ .value = self.hasActiveDenseBulkWork() };
}

fn orderedApplyReceipt(self: *DB) engine.Outcome(?db.OrderedApplyReceipt) {
    return .{ .value = self.orderedApplyReceipt() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn clearOrderedApplyReceipt(self: *DB) engine.Outcome(void) {
    return .{ .value = self.clearOrderedApplyReceipt() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn setResolutionCandidateSource(self: *DB, src: ?@import("db/resolution_runtime.zig").CandidateSource) engine.Outcome(void) {
    return .{ .value = self.setResolutionCandidateSource(src) };
}

fn setEntitySink(self: *DB, sink: ?@import("db/promotion_runtime.zig").EntitySink) engine.Outcome(void) {
    return .{ .value = self.setEntitySink(sink) };
}

fn setPromotionOwner(self: *DB, owner: ?@import("db/promotion_runtime.zig").PromotionOwner) engine.Outcome(void) {
    return .{ .value = self.setPromotionOwner(owner) };
}

fn advanceGeneratedArtifactCleanupPage(self: *DB, index_name: ?[]const u8) engine.Outcome(DB.GeneratedArtifactCleanupAdvanceResult) {
    return .{ .value = self.advanceGeneratedArtifactCleanupPage(index_name) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn loadIndexRepairState(self: *const DB, alloc: std.mem.Allocator) engine.Outcome(@import("db/derived/index_repair_state.zig").State) {
    return .{ .value = self.loadIndexRepairState(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn getRange(self: *DB) engine.Outcome(types.ByteRange) {
    return .{ .value = self.getRange() };
}

fn getSplitBootstrapMarker(self: *DB, alloc: std.mem.Allocator) engine.Outcome(?@import("db/range_state.zig").SplitBootstrapMarker) {
    return .{ .value = self.getSplitBootstrapMarker(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn setSplitBootstrapMarker(self: *DB, marker: @import("db/range_state.zig").SplitBootstrapMarker) engine.Outcome(void) {
    return .{ .value = self.setSplitBootstrapMarker(marker) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn readRestoreStateForPath(alloc: std.mem.Allocator, path: []const u8) engine.Outcome(?db.RestoreState) {
    return .{ .value = DB.readRestoreStateForPath(alloc, path) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn readRestoreStateForPathWithIo(alloc: std.mem.Allocator, io: native.IoBorrow, path: []const u8) engine.Outcome(?db.RestoreState) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = DB.readRestoreStateForPathWithIo(alloc, received_io, path) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn restoreRuntimeRepairNeededForPath(alloc: std.mem.Allocator, path: []const u8) engine.Outcome(bool) {
    return .{ .value = DB.restoreRuntimeRepairNeededForPath(alloc, path) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn restoreRuntimeRepairNeededForPathWithIo(alloc: std.mem.Allocator, io: native.IoBorrow, path: []const u8) engine.Outcome(bool) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = DB.restoreRuntimeRepairNeededForPathWithIo(alloc, received_io, path) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn beginTransaction(self: *DB, timestamp_ns: u64) engine.Outcome(@import("transactions.zig").TxnId) {
    return .{ .value = self.beginTransaction(timestamp_ns) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn beginTransactionWithParticipants(self: *DB, timestamp_ns: u64, participants: []const []const u8) engine.Outcome(@import("transactions.zig").TxnId) {
    return .{ .value = self.beginTransactionWithParticipants(timestamp_ns, participants) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeReplicatedTransactionAtOrderedReceipt(self: *DB, txn_id: types.TxnId, req: types.TransactionIntentRequest, identity: db.OrderedApplyReceipt) engine.Outcome(void) {
    return .{ .value = self.writeReplicatedTransactionAtOrderedReceipt(txn_id, req, identity) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn getTransactionStatus(self: *DB, txn_id: @import("transactions.zig").TxnId) engine.Outcome(@import("transactions.zig").TxnStatus) {
    return .{ .value = self.getTransactionStatus(txn_id) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn transactionDefersCoordinatorAcknowledgement(self: *DB, txn_id: @import("transactions.zig").TxnId) engine.Outcome(bool) {
    return .{ .value = self.transactionDefersCoordinatorAcknowledgement(txn_id) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn transactionRetainsCoordinatorAcknowledgement(self: *DB, txn_id: @import("transactions.zig").TxnId) engine.Outcome(bool) {
    return .{ .value = self.transactionRetainsCoordinatorAcknowledgement(txn_id) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn hasTopologySensitiveTransactions(self: *DB) engine.Outcome(bool) {
    return .{ .value = self.hasTopologySensitiveTransactions() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn onlineSourceStatus(self: *DB, scope: @import("db/online_source_contract.zig").Scope) engine.Outcome(@import("db/online_source.zig").Progress) {
    return .{ .value = self.onlineSourceStatus(scope) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn prepareOnlineSourcePublication(self: *DB, scope: @import("db/online_source_contract.zig").Scope, cancellation: types.CancellationToken) engine.Outcome(@import("source_snapshot.zig").Certificate) {
    return .{ .value = self.prepareOnlineSourcePublication(scope, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn cleanupTransactionMetadataIfEligible(self: *DB, txn_id: @import("transactions.zig").TxnId, cutoff_timestamp: u64, retained_cutoff_timestamp: u64) engine.Outcome(bool) {
    return .{ .value = self.cleanupTransactionMetadataIfEligible(txn_id, cutoff_timestamp, retained_cutoff_timestamp) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn getUnresolvedTransactionParticipants(self: *DB, alloc: std.mem.Allocator, txn_id: @import("transactions.zig").TxnId) engine.Outcome([][]u8) {
    return .{ .value = self.getUnresolvedTransactionParticipants(alloc, txn_id) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn admitManagedFullTextIndex(self: *DB, cfg: types.IndexConfig) engine.Outcome(?u128) {
    return .{ .value = self.admitManagedFullTextIndex(cfg) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn listIndexes(self: *DB, alloc: std.mem.Allocator) engine.Outcome([]types.IndexConfig) {
    return .{ .value = self.listIndexes(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn listEnrichments(self: *DB, alloc: std.mem.Allocator) engine.Outcome([]types.EnrichmentConfig) {
    return .{ .value = self.listEnrichments(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn listResolvers(self: *DB, alloc: std.mem.Allocator) engine.Outcome([]@import("db/catalog/index_manager.zig").ResolverConfig) {
    return .{ .value = self.listResolvers(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn curateResolverJsonAlloc(self: *DB, alloc: std.mem.Allocator, name: []const u8, request_json: []const u8) engine.Outcome([]u8) {
    return .{ .value = self.curateResolverJsonAlloc(alloc, name, request_json) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn resolverCurationJsonAlloc(self: *DB, alloc: std.mem.Allocator, name: []const u8) engine.Outcome([]u8) {
    return .{ .value = self.resolverCurationJsonAlloc(alloc, name) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn resolverReviewsJsonAlloc(self: *DB, alloc: std.mem.Allocator, request_json: []const u8) engine.Outcome([]u8) {
    return .{ .value = self.resolverReviewsJsonAlloc(alloc, request_json) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn listResolverIdentities(self: *DB, alloc: std.mem.Allocator) engine.Outcome([]@import("db/catalog/resolver_catalog.zig").ResolverConfig) {
    return .{ .value = self.listResolverIdentities(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn consumeResolverMetadataDrop(self: *DB, alloc: std.mem.Allocator, identity: @import("db/resolver_curation.zig").MetadataIdentity) engine.Outcome(void) {
    return .{ .value = self.consumeResolverMetadataDrop(alloc, identity) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn pendingWorkStats(self: *DB) engine.Outcome(@import("db/core.zig").PendingWorkStats) {
    return .{ .value = self.pendingWorkStats() };
}

fn runArtifactRepairMetadataMaintenanceUntilIdle(self: *DB) engine.Outcome(void) {
    return .{ .value = self.runArtifactRepairMetadataMaintenanceUntilIdle() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn publishVectorBlockBasesOnline(self: *DB, options: @import("db/catalog/index_manager.zig").IndexManager.OnlineVectorBlockPublicationOptions) engine.Outcome(usize) {
    return .{ .value = self.publishVectorBlockBasesOnline(options) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn nativeVectorProjectionMaintenanceNeeded(self: *DB) engine.Outcome(bool) {
    return .{ .value = self.nativeVectorProjectionMaintenanceNeeded() };
}

fn reprocessGeneratedEnrichmentFromStoredDocsWithProgress(self: *DB, alloc: std.mem.Allocator, artifact_name: ?[]const u8) engine.Outcome(DB.GeneratedEnrichmentReplayAppendResult) {
    return .{ .value = self.reprocessGeneratedEnrichmentFromStoredDocsWithProgress(alloc, artifact_name) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn overlayRuntimeStatusBestEffort(self: *DB, stats_alloc: std.mem.Allocator, runtime_stats: *types.DBStats) engine.Outcome(bool) {
    return .{ .value = self.overlayRuntimeStatusBestEffort(stats_alloc, runtime_stats) };
}

fn restoreGenerationAdmissionReceipt(self: *DB) engine.Outcome(?@import("db/restore_staging_contract.zig").GenerationAdmissionReceipt) {
    return .{ .value = self.restoreGenerationAdmissionReceipt() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn reserveRestoreStaging(self: *DB, alloc: std.mem.Allocator, plan_id: [16]u8, plan_digest: [32]u8, namespace: @import("db/doc_identity.zig").Namespace) engine.Outcome(void) {
    return .{ .value = self.reserveRestoreStaging(alloc, plan_id, plan_digest, namespace) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn validateRestoreStagingScope(self: *DB, alloc: std.mem.Allocator, scope: [32]u8) engine.Outcome(void) {
    return .{ .value = self.validateRestoreStagingScope(alloc, scope) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn readInitialChildPublicationRecord(self: *DB) engine.Outcome(?@import("db/relational_initial_child_publication.zig").Record) {
    return .{ .value = self.readInitialChildPublicationRecord() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn prepareRestoreStagingPage(self: *DB, alloc: std.mem.Allocator, scope: @import("db/restore_staging.zig").Scope, source: *DB, max_rows: usize, cancellation: types.CancellationToken) engine.Outcome(@import("db/restore_staging.zig").PreparedPage) {
    return .{ .value = self.prepareRestoreStagingPage(alloc, scope, source, max_rows, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn prepareRewriteStagingPage(self: *DB, alloc: std.mem.Allocator, scope: @import("db/restore_staging.zig").Scope, source: *DB, max_rows: usize, cancellation: types.CancellationToken, program: *const @import("db/relational_rewrite_staging.zig").ProgramSet) engine.Outcome(@import("db/restore_staging.zig").PreparedPage) {
    return .{ .value = self.prepareRewriteStagingPage(alloc, scope, source, max_rows, cancellation, program) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn sourceVectorStats(self: *DB) engine.Outcome(?@import("vector_payload_store.zig").Stats) {
    return .{ .value = self.sourceVectorStats() catch |err| return .{ .failure = .init(@errorName(err)) } };
}
fn relationalTopologyStatus(self: *DB) engine.Outcome(DB.RelationalTopologyStatus) {
    return .{ .value = self.relationalTopologyStatus() catch |err| return .{ .failure = .init(@errorName(err)) } };
}
fn failGraphMetricPlannedBuild(self: *DB, alloc: Allocator, index_name: []const u8, metric_name: []const u8, failure_name: []const u8) engine.Outcome(types.GraphMetricStatus) {
    const failure = @import("storage_engine_errors").fromName(failure_name);
    return .{ .value = self.failGraphMetricPlannedBuild(alloc, index_name, metric_name, failure) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
fn runDenseArtifactRebuildMaintenanceWithProgress(self: *DB, alloc: Allocator, context: ?*anyopaque, hook: ?db.ReplayProgressHook, dispatch: @import("named_callback.zig").Dispatch) engine.Outcome(bool) {
    const Proxy = struct {
        context: ?*anyopaque,
        hook: db.ReplayProgressHook,
        dispatch: @import("named_callback.zig").Dispatch,
        fn progress(raw: *anyopaque, index_name: []const u8, value: db.ReplayProgress) !void {
            const proxy: *@This() = @ptrCast(@alignCast(raw));
            return engine.ReplayBoundary.call("progress", proxy.dispatch, proxy.hook, .{ proxy.context.?, index_name, value });
        }
    };
    if (hook) |callback_hook| {
        var proxy: Proxy = .{ .context = context, .hook = callback_hook, .dispatch = dispatch };
        return .{ .value = self.runDenseArtifactRebuildMaintenanceWithProgress(alloc, &proxy, Proxy.progress) catch |err| return .{ .failure = .init(@errorName(err)) } };
    }
    return .{ .value = self.runDenseArtifactRebuildMaintenanceWithProgress(alloc, context, null) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
fn queryReadLeaseRelease(lease: *DB.QueryReadLease) engine.Outcome(void) {
    return .{ .value = lease.release() };
}

fn batchReplicatedApply(self: *DB, req: types.BatchRequest) engine.Outcome(void) {
    return .{ .value = self.batchReplicatedApply(req) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn applyReplicatedRowPolicyPublication(self: *DB, bundle_bytes: []const u8, request: @import("../system_catalog/policies.zig").InstallRequest, identity: db.OrderedApplyReceipt) engine.Outcome(?@import("db/row_policy_bundle.zig").Receipt) {
    return .{ .value = self.applyReplicatedRowPolicyPublication(bundle_bytes, request, identity) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn recoverReplicatedRestoreFinish(self: *DB, finish: @FieldType(@import("db/restore_staging_contract.zig").Control, "finish")) engine.Outcome(void) {
    return .{ .value = self.recoverReplicatedRestoreFinish(finish) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn reprocessDocumentArtifact(self: *DB, alloc: std.mem.Allocator, doc_key: []const u8, artifact_name: []const u8) engine.Outcome(bool) {
    return .{ .value = self.reprocessDocumentArtifact(alloc, doc_key, artifact_name) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn indexRepairIntentSummary(self: *DB, alloc: std.mem.Allocator) engine.Outcome(DB.IndexRepairIntentSummary) {
    return .{ .value = self.indexRepairIntentSummary(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn repairArtifactIssuesWithRequest(self: *DB, alloc: std.mem.Allocator, req: types.ArtifactRepairRunRequest) engine.Outcome(types.ArtifactRepairResult) {
    return .{ .value = self.repairArtifactIssuesWithRequest(alloc, req) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn beginTransactionWithId(self: *DB, txn_id: @import("transactions.zig").TxnId, timestamp_ns: u64) engine.Outcome(@import("transactions.zig").TxnId) {
    return .{ .value = self.beginTransactionWithId(txn_id, timestamp_ns) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn beginReplicatedTransactionAtOrderedReceipt(self: *DB, txn_id: @import("transactions.zig").TxnId, timestamp_ns: u64, created_at_ns: u64, participants: []const []const u8, coordinator: bool, retain_terminal: bool, identity: db.OrderedApplyReceipt) engine.Outcome(@import("transactions.zig").TxnId) {
    return .{ .value = self.beginReplicatedTransactionAtOrderedReceipt(txn_id, timestamp_ns, created_at_ns, participants, coordinator, retain_terminal, identity) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn commitTransaction(self: *DB, txn_id: @import("transactions.zig").TxnId, timestamp_ns: u64) engine.Outcome(void) {
    return .{ .value = self.commitTransaction(txn_id, timestamp_ns) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn resolveTransactionIntents(self: *DB, txn_id: @import("transactions.zig").TxnId, status: @import("transactions.zig").TxnStatus, commit_version: u64) engine.Outcome(void) {
    return .{ .value = self.resolveTransactionIntents(txn_id, status, commit_version) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn applyRelationalTopologyControl(self: *DB, command: @import("db/relational_integrity_topology.zig").Command, ordered_receipt: ?db.OrderedApplyReceipt) engine.Outcome(void) {
    return .{ .value = self.applyRelationalTopologyControl(command, ordered_receipt) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn runUntilIdleWithoutWaitingForEnrichmentRetries(self: *DB) engine.Outcome(void) {
    return .{ .value = self.runUntilIdleWithoutWaitingForEnrichmentRetries() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn runVectorBlockMaintenanceForIdle(self: *DB) engine.Outcome(usize) {
    return .{ .value = self.runVectorBlockMaintenanceForIdle() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn prepareRestoreStagingIndexesStep(self: *DB, alloc: std.mem.Allocator, scope: [32]u8) engine.Outcome(bool) {
    return .{ .value = self.prepareRestoreStagingIndexesStep(alloc, scope) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn beginRestoreStaging(self: *DB, alloc: std.mem.Allocator, scope: @import("db/restore_staging.zig").Scope) engine.Outcome(void) {
    return .{ .value = self.beginRestoreStaging(alloc, scope) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn finishRestoreStaging(self: *DB, alloc: std.mem.Allocator, scope: @import("db/restore_staging.zig").Digest, phase: @import("db/restore_staging.zig").Phase) engine.Outcome(@import("db/restore_staging.zig").Digest) {
    return .{ .value = self.finishRestoreStaging(alloc, scope, phase) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn importPortableRestoreFileIntoUnpublishedEmpty(self: *DB, alloc: std.mem.Allocator, io: native.IoBorrow, file: std.Io.File, file_size: u64, options: engine.PortableRestoreImportOptions) engine.Outcome(void) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    return .{ .value = self.importPortableRestoreFileIntoUnpublishedEmpty(alloc, receiver.io(), file, file_size, options) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn importPortableFileIntoUnpublishedEmpty(self: *DB, alloc: std.mem.Allocator, io: native.IoBorrow, file: std.Io.File, file_size: u64, target_identity: @import("db/doc_identity.zig").Namespace) engine.Outcome(void) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = self.importPortableFileIntoUnpublishedEmpty(alloc, received_io, file, file_size, target_identity) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn importCohortPortableFileIntoUnpublishedEmpty(self: *DB, alloc: std.mem.Allocator, io: native.IoBorrow, file: std.Io.File, file_size: u64, proof: @import("portable_backup.zig").CohortProof, cancellation: types.CancellationToken) engine.Outcome(void) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = self.importCohortPortableFileIntoUnpublishedEmpty(alloc, received_io, file, file_size, proof, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn openRelationalReadSession(self: *DB, alloc: std.mem.Allocator, from_key: []const u8, to_key: []const u8, opts: types.ScanOptions) engine.Outcome(*DB.RelationalReadSession) {
    return .{ .value = self.openRelationalReadSession(alloc, from_key, to_key, opts) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn search(self: *DB, alloc: std.mem.Allocator, req: types.SearchRequest) engine.Outcome(types.SearchResult) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    return .{ .value = self.search(alloc, received) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn searchWithCapturedRequest(self: *DB, alloc: std.mem.Allocator, req: types.SearchRequest) engine.Outcome(db.SearchWithCapturedRequestResult) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    var result = self.searchWithCapturedRequest(alloc, received) catch |err| return .{ .failure = .init(@errorName(err)) };
    result.request.native_key_predicate = req.native_key_predicate;
    return .{ .value = result };
}

fn preflightSearchRequest(self: *DB, alloc: std.mem.Allocator, req: types.SearchRequest, max_work: u32) engine.Outcome(@import("db/query/search_exec.zig").RuntimePreflightSummary) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    return .{ .value = self.preflightSearchRequest(alloc, received, max_work) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn queryValidateGraphHydrateResolvedDocFilterForDb(req: @import("../api/graph.zig").GraphHydrateRequest, database: *@import("db/mod.zig").DB) engine.Outcome(void) {
    return .{ .value = @import("query.zig").validateGraphHydrateResolvedDocFilterForDb(req, database) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn queryExecuteStorageKernelGraphExpand(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, table_name: []const u8, req: @import("../api/graph.zig").GraphExpandRequest) engine.Outcome(@import("../api/graph.zig").GraphExpandResponse) {
    return .{ .value = @import("query.zig").executeStorageKernelGraphExpand(alloc, database, table_name, req) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn queryExecuteStorageKernelGraphHydrate(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, req: @import("../api/graph.zig").GraphHydrateRequest) engine.Outcome(@import("../api/graph.zig").GraphHydrateResponse) {
    return .{ .value = @import("query.zig").executeStorageKernelGraphHydrate(alloc, database, req) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn queryExecuteStorageKernelGraphEdges(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, req: @import("../api/graph.zig").GraphEdgesRequest) engine.Outcome(@import("../api/graph.zig").GraphEdgesResponse) {
    return .{ .value = @import("query.zig").executeStorageKernelGraphEdges(alloc, database, req) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn queryValidateGraphHydrateIncomingIndexIdentity(req: @import("../api/graph.zig").GraphHydrateRequest, database: *@import("db/mod.zig").DB) engine.Outcome(void) {
    return .{ .value = @import("query.zig").validateGraphHydrateIncomingIndexIdentity(req, database) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn queryCurrentIdentityReadGenerationForDb(requested: ?u64, database: *@import("db/mod.zig").DB) engine.Outcome(u64) {
    return .{ .value = @import("query.zig").currentIdentityReadGenerationForDb(requested, database) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn queryAlgebraicIndexFreshEnoughForName(alloc: std.mem.Allocator, index_name_opt: ?[]const u8, database: *@import("db/mod.zig").DB) engine.Outcome(bool) {
    return .{ .value = @import("query.zig").algebraicIndexFreshEnoughForName(alloc, index_name_opt, database) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn queryCollectTextStatsFromDbForRequest(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, request: @import("../api/query_execution_contract.zig").ParsedTextStatsRequest) engine.Outcome([]const @import("../search/distributed_stats.zig").TextFieldStats) {
    return .{ .value = @import("query.zig").collectTextStatsFromDbForRequest(alloc, database, request) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn queryCollectBackgroundTextStatsFromDbForRequest(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, request: @import("../api/query_execution_contract.zig").ParsedTextStatsRequest) engine.Outcome([]const @import("db/mod.zig").aggregations.DistributedBackgroundTextStats) {
    return .{ .value = @import("query.zig").collectBackgroundTextStatsFromDbForRequest(alloc, database, request) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn queryCollectAlgebraicPartialsFromDbForRequest(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, request: @import("../api/query_execution_contract.zig").ParsedAlgebraicPartialsRequest) engine.Outcome([]@import("db/mod.zig").algebraic.distributed.Partial) {
    return .{ .value = @import("query.zig").collectAlgebraicPartialsFromDbForRequest(alloc, database, request) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn queryAlgebraicTensorProgramOutputExpressionsForIndexAlloc(alloc: std.mem.Allocator, index: ?*const @import("db/mod.zig").algebraic.index.Index, access_paths: []@import("../api/query_execution_contract.zig").OwnedAlgebraicTensorAccessPath, program: *const @import("../api/query_execution_contract.zig").OwnedAlgebraicTensorProgram) engine.Outcome([]@import("../api/query_execution_contract.zig").algebraic_ir.TensorExpr) {
    return .{ .value = @import("query.zig").algebraicTensorProgramOutputExpressionsForIndexAlloc(alloc, index, access_paths, program) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn queryAlgebraicTensorProgramOutputExpressionForStep(alloc: std.mem.Allocator, index: ?*const @import("db/mod.zig").algebraic.index.Index, path_values: []const @import("../api/query_execution_contract.zig").algebraic_ir.PhysicalAccessPath, expr: @import("../api/query_execution_contract.zig").algebraic_ir.TensorExpr) engine.Outcome(@import("../api/query_execution_contract.zig").algebraic_ir.TensorExpr) {
    return .{ .value = @import("query.zig").algebraicTensorProgramOutputExpressionForStep(alloc, index, path_values, expr) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn queryFindAlgebraicMaterialization(index: *const @import("db/mod.zig").algebraic.index.Index, name: []const u8) engine.Outcome(?@import("db/mod.zig").algebraic.index.MaterializationConfig) {
    return .{ .value = @import("query.zig").findAlgebraicMaterialization(index, name) };
}

fn queryExecuteStorageKernelTextStats(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, table_name: []const u8, body: []const u8) engine.Outcome([]u8) {
    return .{ .value = @import("query.zig").executeStorageKernelTextStats(alloc, database, table_name, body) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn queryExecuteStorageKernelAlgebraicPartials(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, body: []const u8) engine.Outcome([]u8) {
    return .{ .value = @import("query.zig").executeStorageKernelAlgebraicPartials(alloc, database, body) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn queryExecuteStorageKernelPreflight(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, table_name: []const u8, request_json: []const u8) engine.Outcome([]u8) {
    return .{ .value = @import("query.zig").executeStorageKernelPreflight(alloc, database, table_name, request_json) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn computeSearchAggregations(alloc: Allocator, requests: []const @import("db/aggregations.zig").SearchAggregationRequest, result: types.SearchResult, ctx: @import("db/aggregations.zig").Context) engine.Outcome([]@import("db/aggregations.zig").SearchAggregationResult) {
    return .{ .value = @import("db/aggregations.zig").computeSearchAggregations(alloc, requests, result, ctx) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn portableDecoderStep(alloc: Allocator, io: native.IoBorrow, file: std.Io.File, plan: @import("portable_decoder.zig").Plan, cancellation: types.CancellationToken) engine.Outcome(bool) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = @import("portable_decoder.zig").step(alloc, received_io, file, plan, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
fn portableDecoderBindRange(alloc: Allocator, store: *@import("docstore.zig").DocStore, range: types.ByteRange) engine.Outcome(void) {
    return .{ .value = @import("portable_decoder.zig").bindRange(alloc, store, range) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
fn requireRewriteSourceManifest(self: *DB, alloc: Allocator, program: *const @import("db/relational_rewrite_program.zig").ProgramSet, cancellation: types.CancellationToken) engine.Outcome(void) {
    return .{ .value = self.requireRewriteSourceManifest(alloc, program, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
fn verifyColdInitialChildSeed(self: *DB, expected: @import("db/relational_initial_child_publication.zig").Bootstrap) engine.Outcome(void) {
    return .{ .value = self.verifyColdInitialChildSeed(expected) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
fn syncPrimaryStore(self: *DB) engine.Outcome(void) {
    return .{ .value = self.syncPrimaryStore() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeCorruptEmbeddingArtifactInDb(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, doc_key: []const u8, index_name: []const u8) engine.Outcome(bool) {
    return .{ .value = @import("write.zig").corruptEmbeddingArtifactInDb(alloc, database, doc_key, index_name) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeConfigureRestoreOwnerDb(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, bootstrap: @import("db/restore_staging_contract.zig").OwnerBootstrap, allow_canceled: bool, hot_standby_replay: bool) engine.Outcome(void) {
    return .{ .value = @import("write.zig").configureRestoreOwnerDb(alloc, database, bootstrap, allow_canceled, hot_standby_replay) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeCreateManagedDbEnrichments(allocator: std.mem.Allocator, raw_indexes_json: []const u8, runtime: ?*@import("db/mod.zig").background_runtime.BackendRuntime, local_provider: ?@import("../inference/managed_embedder.zig").AntflyProvider, remote_capability_cache: ?*@import("antfly_inference_remote_capabilities").Cache, inference_api_url: ?[]const u8, source_table: []const u8, store: ?*@import("../common/secrets.zig").FileStore, remote: ?*const @import("write.zig").scraping.RemoteContentConfig) engine.Outcome(@import("write.zig").ManagedDbEnrichmentSet) {
    return .{ .value = @import("write.zig").createManagedDbEnrichments(allocator, raw_indexes_json, runtime, local_provider, remote_capability_cache, inference_api_url, source_table, store, remote) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeReconfigureManagedDbEnrichmentRuntime(ignored_allocator: std.mem.Allocator, database: *@import("db/mod.zig").DB, indexes_json: []const u8, backend_runtime: ?*@import("db/mod.zig").background_runtime.BackendRuntime, antfly_provider: ?@import("../inference/managed_embedder.zig").AntflyProvider, remote_capability_cache: ?*@import("antfly_inference_remote_capabilities").Cache, inference_api_url: ?[]const u8, source_table: []const u8, secret_store: ?*@import("../common/secrets.zig").FileStore, remote_content: ?*const @import("write.zig").scraping.RemoteContentConfig) engine.Outcome(void) {
    return .{ .value = @import("write.zig").reconfigureManagedDbEnrichmentRuntime(ignored_allocator, database, indexes_json, backend_runtime, antfly_provider, remote_capability_cache, inference_api_url, source_table, secret_store, remote_content) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writePrepareManagedSchemaBeforeIndexLoad(alloc: std.mem.Allocator, mode: @import("../api/write_contract.zig").ManagedDbOpenMode, schema_json: ?[]const u8) engine.Outcome(?@import("db/mod.zig").SchemaBeforeIndexLoad) {
    return .{ .value = @import("write.zig").prepareManagedSchemaBeforeIndexLoad(alloc, mode, schema_json) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeOpenManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentityWithOptions(alloc: std.mem.Allocator, path: []const u8, indexes_json: []const u8, lsm_cache: ?*@import("lsm_backend/mod.zig").Cache, hbc_cache: ?*@import("hbc_adapter.zig").Cache, lsm_root_generation: u64, resource_manager: ?*@import("resource_manager.zig").ResourceManager, mode: @import("../api/write_contract.zig").ManagedDbOpenMode, backend_runtime: ?*@import("db/mod.zig").background_runtime.BackendRuntime, antfly_provider: ?@import("../inference/managed_embedder.zig").AntflyProvider, secret_store: ?*@import("../common/secrets.zig").FileStore, remote_content: ?*const @import("write.zig").scraping.RemoteContentConfig, identity_namespace: ?@import("db/doc_identity.zig").Namespace, options: @import("write.zig").ManagedDbOpenOptions) engine.Outcome(@import("db/mod.zig").DB) {
    return .{ .value = @import("write.zig").openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentityWithOptions(alloc, path, indexes_json, lsm_cache, hbc_cache, lsm_root_generation, resource_manager, mode, backend_runtime, antfly_provider, secret_store, remote_content, identity_namespace, options) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeLoadLocalTableSchemaJson(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB) engine.Outcome(?[]u8) {
    return .{ .value = @import("write.zig").loadLocalTableSchemaJson(alloc, database) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeExportPortableBackupShard(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, backup_root: []const u8, backup_id: []const u8, group_id: u64, shared_io: ?native.IoBorrow) engine.Outcome([]@import("../api/backups.zig").ShardSnapshot) {
    var receiver: ?@import("antfly_runtime_abi").io_abi.Receiver = if (shared_io) |borrow| borrow.receive() catch |err| return .{ .failure = .init(@errorName(err)) } else null;
    const received_io: ?std.Io = if (receiver) |*value| value.io() else null;
    return .{ .value = @import("write.zig").exportPortableBackupShard(alloc, database, backup_root, backup_id, group_id, received_io) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeExportPortableBackupShardWithSeal(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, backup_root: []const u8, backup_id: []const u8, group_id: u64, shared_io: ?native.IoBorrow, sealed: ?@import("../api/backup_contract.zig").SealedHandle, cancellation: @import("db/mod.zig").types.CancellationToken) engine.Outcome([]@import("../api/backups.zig").ShardSnapshot) {
    var receiver: ?@import("antfly_runtime_abi").io_abi.Receiver = if (shared_io) |borrow| borrow.receive() catch |err| return .{ .failure = .init(@errorName(err)) } else null;
    const received_io: ?std.Io = if (receiver) |*value| value.io() else null;
    return .{ .value = @import("write.zig").exportPortableBackupShardWithSeal(alloc, database, backup_root, backup_id, group_id, received_io, sealed, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeCopyPreparedNativeBackupShardSnapshot(alloc: std.mem.Allocator, native_snapshot: *@import("write.zig").NativeBackupShardSnapshot) engine.Outcome([]@import("../api/backups.zig").ShardSnapshot) {
    return .{ .value = @import("write.zig").copyPreparedNativeBackupShardSnapshot(alloc, native_snapshot) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeBackupStorageKernelOwnerDb(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, db_path: []const u8, group_id: u64, backup_root: []const u8, backup_id: []const u8, format: @import("../api/backups.zig").BackupFormat) engine.Outcome([]@import("../api/backups.zig").ShardSnapshot) {
    return .{ .value = @import("write.zig").backupStorageKernelOwnerDb(alloc, database, db_path, group_id, backup_root, backup_id, format) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeBackupStorageKernelOwnerDbWithControl(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, db_path: []const u8, group_id: u64, backup_root: []const u8, backup_id: []const u8, format: @import("../api/backups.zig").BackupFormat, cohort: ?@import("db/relational_integrity_topology_contract.zig").Fence, sealed: ?@import("db/native_backup_seal_contract.zig").Handle, control: @import("../api/backups.zig").BackupOperationControl) engine.Outcome([]@import("../api/backups.zig").ShardSnapshot) {
    return .{ .value = @import("write.zig").backupStorageKernelOwnerDbWithControl(alloc, database, db_path, group_id, backup_root, backup_id, format, cohort, sealed, control) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeExportPortableBackupFile(alloc: std.mem.Allocator, store: *@import("db/mod.zig").docstore.DocStore, path: []const u8, shared_io: ?native.IoBorrow) engine.Outcome(void) {
    var receiver: ?@import("antfly_runtime_abi").io_abi.Receiver = if (shared_io) |borrow| borrow.receive() catch |err| return .{ .failure = .init(@errorName(err)) } else null;
    const received_io: ?std.Io = if (receiver) |*value| value.io() else null;
    return .{ .value = @import("write.zig").exportPortableBackupFile(alloc, store, path, received_io) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeExportPortableBackupFileWithIo(alloc: std.mem.Allocator, store: *@import("db/mod.zig").docstore.DocStore, path: []const u8, io: native.IoBorrow) engine.Outcome(void) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = @import("write.zig").exportPortableBackupFileWithIo(alloc, store, path, received_io) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeResolveWritesForSchemaValidation(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, base_writes: []const @import("db/mod.zig").types.BatchWrite, deletes: []const []const u8, transforms: []const @import("db/mod.zig").types.DocumentTransform) engine.Outcome([]@import("db/mod.zig").types.BatchWrite) {
    return .{ .value = @import("write.zig").resolveWritesForSchemaValidation(alloc, database, base_writes, deletes, transforms) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeValidateGroupedSourceBatch(alloc: Allocator, database: *DB, writes: []const types.BatchWrite, deletes: []const []const u8, transforms: []const types.DocumentTransform) engine.Outcome(void) {
    return .{ .value = @import("write.zig").validateGroupedSourceBatch(alloc, database, writes, deletes, transforms) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeValidateTableBatchAgainstLocalSchema(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, writes: []const @import("db/mod.zig").types.BatchWrite, deletes: []const []const u8, transforms: []const @import("db/mod.zig").types.DocumentTransform) engine.Outcome(void) {
    return .{ .value = @import("write.zig").validateTableBatchAgainstLocalSchema(alloc, database, writes, deletes, transforms) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeApplyLocalTableSchemaJson(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, schema_json: []const u8) engine.Outcome(void) {
    return .{ .value = @import("write.zig").applyLocalTableSchemaJson(alloc, database, schema_json) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writePrepareOwnerSchemaBeforeIndexLoad(alloc: std.mem.Allocator, schema_json: []const u8) engine.Outcome(?@import("db/mod.zig").SchemaBeforeIndexLoad) {
    return .{ .value = @import("write.zig").prepareOwnerSchemaBeforeIndexLoad(alloc, schema_json) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeConfigureStorageKernelOwnerDb(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, table_name: []const u8, schema_json: []const u8, indexes_json: []const u8, backend_runtime: ?*@import("db/mod.zig").background_runtime.BackendRuntime, antfly_provider: ?@import("../inference/managed_embedder.zig").AntflyProvider, secret_store: ?*@import("../common/secrets.zig").FileStore, remote_content: ?*const @import("write.zig").scraping.RemoteContentConfig, installed: ?*@import("write.zig").OwnerManagedConfig) engine.Outcome(void) {
    return .{ .value = @import("write.zig").configureStorageKernelOwnerDb(alloc, database, table_name, schema_json, indexes_json, backend_runtime, antfly_provider, secret_store, remote_content, installed) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeConfigureStorageKernelOwnerDbAtOpen(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, table_name: []const u8, schema_json: []const u8, indexes_json: []const u8, backend_runtime: ?*@import("db/mod.zig").background_runtime.BackendRuntime, antfly_provider: ?@import("../inference/managed_embedder.zig").AntflyProvider, secret_store: ?*@import("../common/secrets.zig").FileStore, remote_content: ?*const @import("write.zig").scraping.RemoteContentConfig, installed: ?*@import("write.zig").OwnerManagedConfig, historical_ordered_apply: bool) engine.Outcome(bool) {
    return .{ .value = @import("write.zig").configureStorageKernelOwnerDbAtOpen(alloc, database, table_name, schema_json, indexes_json, backend_runtime, antfly_provider, secret_store, remote_content, installed, historical_ordered_apply) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeOpenStorageKernelRestoreDb(alloc: std.mem.Allocator, path: []const u8, indexes_json: []const u8, lsm_root_generation: u64, backend_runtime: *@import("db/mod.zig").background_runtime.BackendRuntime, identity_namespace: ?@import("db/doc_identity.zig").Namespace, prepared_restore: *const @import("backup_restore.zig").PreparedRestore) engine.Outcome(@import("db/mod.zig").DB) {
    return .{ .value = @import("write.zig").openStorageKernelRestoreDb(alloc, path, indexes_json, lsm_root_generation, backend_runtime, identity_namespace, prepared_restore) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeRepairStorageKernelRestoreDb(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, group_id: u64, schema_json: []const u8, indexes_json: []const u8, cancellation: @import("db/mod.zig").types.CancellationToken) engine.Outcome(void) {
    return .{ .value = @import("write.zig").repairStorageKernelRestoreDb(alloc, database, group_id, schema_json, indexes_json, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeReconcileStorageKernelOwnerDb(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, table_name: []const u8, schema_json: []const u8, indexes_json: []const u8, target_index_name: ?[]const u8, advance_index_repair: bool, backend_runtime: ?*@import("db/mod.zig").background_runtime.BackendRuntime, antfly_provider: ?@import("../inference/managed_embedder.zig").AntflyProvider, installed: ?*@import("write.zig").OwnerManagedConfig) engine.Outcome(@import("../api/write_contract.zig").StorageKernelReconcileResult) {
    return .{ .value = @import("write.zig").reconcileStorageKernelOwnerDb(alloc, database, table_name, schema_json, indexes_json, target_index_name, advance_index_repair, backend_runtime, antfly_provider, installed) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeRepairStorageKernelOwnerDb(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, target_index_name: ?[]const u8, advance_index_repair: bool, options: @import("db/mod.zig").types.ArtifactRepairRunOptions) engine.Outcome(@import("../api/write_contract.zig").StorageKernelReconcileResult) {
    return .{ .value = @import("write.zig").repairStorageKernelOwnerDb(alloc, database, target_index_name, advance_index_repair, options) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeValidateProvisionedDbIdentityNamespaceWithPolicy(expected: ?@import("db/doc_identity.zig").Namespace, validation: @import("../api/write_contract.zig").StartupCatchUpMetadata.IdentityValidation, database: *const @import("db/mod.zig").DB) engine.Outcome(void) {
    return .{ .value = @import("write.zig").validateProvisionedDbIdentityNamespaceWithPolicy(expected, validation, database) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeBatchUsesDurableTransactionContract(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, req: @import("db/mod.zig").types.BatchRequest) engine.Outcome(bool) {
    return .{ .value = @import("write.zig").batchUsesDurableTransactionContract(alloc, database, req) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeTransactionUsesDurableContract(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, txn_id: @import("db/mod.zig").types.TxnId) engine.Outcome(bool) {
    return .{ .value = @import("write.zig").transactionUsesDurableContract(alloc, database, txn_id) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeApplyGraphMetricActionToDb(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, index_name: []const u8, metric_name: []const u8, action: []const u8) engine.Outcome(@import("db/mod.zig").types.GraphMetricStatus) {
    return .{ .value = @import("write.zig").applyGraphMetricActionToDb(alloc, database, index_name, metric_name, action) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeRunGraphMetricMaintenanceOrActionJsonAlloc(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, body: []const u8) engine.Outcome([]u8) {
    return .{ .value = @import("write.zig").runGraphMetricMaintenanceOrActionJsonAlloc(alloc, database, body) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeReconfigureManagedDbEnrichmentRuntimePaused(ignored_allocator: std.mem.Allocator, database: *@import("db/mod.zig").DB, indexes_json: []const u8, backend_runtime: ?*@import("db/mod.zig").background_runtime.BackendRuntime, antfly_provider: ?@import("../inference/managed_embedder.zig").AntflyProvider, remote_capability_cache: ?*@import("antfly_inference_remote_capabilities").Cache, inference_api_url: ?[]const u8, source_table: []const u8, secret_store: ?*@import("../common/secrets.zig").FileStore, remote_content: ?*const @import("write.zig").scraping.RemoteContentConfig) engine.Outcome(void) {
    return .{ .value = @import("write.zig").reconfigureManagedDbEnrichmentRuntimePaused(ignored_allocator, database, indexes_json, backend_runtime, antfly_provider, remote_capability_cache, inference_api_url, source_table, secret_store, remote_content) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn writeExportPortableBackupFileWithSource(alloc: std.mem.Allocator, store: *@import("db/mod.zig").docstore.DocStore, path: []const u8, io: native.IoBorrow, sealed: ?@import("write.zig").PortableCohortSource) engine.Outcome(void) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = @import("write.zig").exportPortableBackupFileWithSource(alloc, store, path, received_io, sealed) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn managedDbEnrichmentSetDeinit(value: *@import("write.zig").ManagedDbEnrichmentSet, alloc: Allocator) engine.Outcome(void) {
    return .{ .value = value.deinit(alloc) };
}
fn nativeBackupSnapshotDeinit(value: *@import("write.zig").NativeBackupShardSnapshot, alloc: Allocator) engine.Outcome(void) {
    return .{ .value = value.deinit(alloc) };
}
fn writePrepareNativeBackupShardSnapshot(alloc: Allocator, database: *DB, path: []const u8, group: u64, plan: engine.BackupPlan) engine.Outcome(@import("write.zig").NativeBackupShardSnapshot) {
    var received = plan.value;
    var receiver: ?*@import("antfly_runtime_abi").io_abi.Receiver = null;
    if (plan.io) |borrow| {
        const owned = alloc.create(@import("antfly_runtime_abi").io_abi.Receiver) catch |err| return .{ .failure = .init(@errorName(err)) };
        owned.* = borrow.receive() catch |err| {
            alloc.destroy(owned);
            return .{ .failure = .init(@errorName(err)) };
        };
        receiver = owned;
        received.io = owned.io();
    }
    var value = @import("write.zig").prepareNativeBackupShardSnapshot(alloc, database, path, group, received) catch |err| {
        if (receiver) |owned| alloc.destroy(owned);
        return .{ .failure = .init(@errorName(err)) };
    };
    value.owned_io_receiver = receiver;
    return .{ .value = value };
}

fn resolutionDeinit(self: *@import("db/resolution_runtime.zig").ResolutionRuntime) engine.Outcome(void) {
    return .{ .value = self.deinit() };
}

fn resolutionStart(self: *@import("db/resolution_runtime.zig").ResolutionRuntime) engine.Outcome(void) {
    return .{ .value = self.start() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn resolutionStop(self: *@import("db/resolution_runtime.zig").ResolutionRuntime) engine.Outcome(void) {
    return .{ .value = self.stop() };
}

fn resolutionCatchUp(self: *@import("db/resolution_runtime.zig").ResolutionRuntime) engine.Outcome(void) {
    return .{ .value = self.catchUp() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn resolutionCatchUpOnce(self: *@import("db/resolution_runtime.zig").ResolutionRuntime, deadline_ns: u64) engine.Outcome(void) {
    return .{ .value = self.catchUpOnce(deadline_ns) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn resolutionRequestReresolveBacklog(self: *@import("db/resolution_runtime.zig").ResolutionRuntime) engine.Outcome(void) {
    return .{ .value = self.requestReresolveBacklog() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn resolutionHasReresolveBacklog(self: *@import("db/resolution_runtime.zig").ResolutionRuntime) engine.Outcome(bool) {
    return .{ .value = self.hasReresolveBacklog() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn resolutionRunReresolveBacklogWindow(self: *@import("db/resolution_runtime.zig").ResolutionRuntime) engine.Outcome(@import("db/resolution_runtime.zig").ReresolveEnqueueResult) {
    return .{ .value = self.runReresolveBacklogWindow() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn resolutionPendingReviews(self: *@import("db/resolution_runtime.zig").ResolutionRuntime, alloc: std.mem.Allocator) engine.Outcome([]@import("db/resolution_runtime.zig").PendingReview) {
    return .{ .value = self.pendingReviews(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn promotionDeinit(self: *@import("db/promotion_runtime.zig").PromotionRuntime) engine.Outcome(void) {
    return .{ .value = self.deinit() };
}

fn promotionStart(self: *@import("db/promotion_runtime.zig").PromotionRuntime) engine.Outcome(void) {
    return .{ .value = self.start() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn promotionStop(self: *@import("db/promotion_runtime.zig").PromotionRuntime) engine.Outcome(void) {
    return .{ .value = self.stop() };
}

fn promotionCatchUp(self: *@import("db/promotion_runtime.zig").PromotionRuntime) engine.Outcome(void) {
    return .{ .value = self.catchUp() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn pinnedSnapshotDeinit(self: *@import("db/core.zig").PinnedStoreSnapshot) engine.Outcome(void) {
    return .{ .value = self.deinit() };
}

fn pinnedSnapshotMaterializeWithSink(self: *@import("db/core.zig").PinnedStoreSnapshot, alloc: Allocator, io: native.IoBorrow, snapshot_root: []const u8, cancellation: types.CancellationToken, sink: ?@import("native_artifact_sink.zig").Sink) engine.Outcome(u64) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = self.materializeWithSink(alloc, received_io, snapshot_root, cancellation, sink) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn sourcePinLocate(database: *DB, scope: @import("db/online_source.zig").Scope) engine.Outcome(@import("db/online_source.zig").Located) {
    return .{ .value = @import("db/source_pin.zig").locate(database, scope) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn sourcePinEnsureAssumeApply(database: *DB, scope: @import("db/online_source.zig").Scope) engine.Outcome(void) {
    return .{ .value = @import("db/source_pin.zig").ensureAssumeApply(database, scope) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn sourcePinPublicationCertificateIfPresent(database: *DB, scope: @import("db/online_source.zig").Scope, cancellation: types.CancellationToken) engine.Outcome(?@import("source_snapshot.zig").Certificate) {
    return .{ .value = @import("db/source_pin.zig").publicationCertificateIfPresent(database, scope, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn sourcePinPreparePublication(database: *DB, scope: @import("db/online_source.zig").Scope, cancellation: types.CancellationToken) engine.Outcome(@import("source_snapshot.zig").Certificate) {
    return .{ .value = @import("db/source_pin.zig").preparePublication(database, scope, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn sourcePinReclaimReleased(database: *DB, scope: @import("db/online_source.zig").Scope) engine.Outcome(void) {
    return .{ .value = @import("db/source_pin.zig").reclaimReleased(database, scope) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn sourcePinReconcileReleasedWithBudget(database: *DB, budget: @import("db/source_pin_gc.zig").Budget) engine.Outcome(@import("db/source_pin_gc.zig").Work) {
    return .{ .value = @import("db/source_pin.zig").reconcileReleasedWithBudget(database, budget) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn openVerifiedRestoreSource(staged_generation: *const @import("db/generation_lifecycle.zig").StagedGeneration, alloc: std.mem.Allocator, snapshot_root: []const u8, path: []const u8, opts: db.OpenOptions, expected_namespace: @import("db/doc_identity.zig").Namespace) engine.Outcome(DB) {
    return .{ .value = DB.openVerifiedRestoreSource(staged_generation, alloc, snapshot_root, path, opts, expected_namespace) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn restoreSnapshotToDeferredRuntimeRepairWithIoAndCancellation(staged_generation: *const @import("db/generation_lifecycle.zig").StagedGeneration, alloc: std.mem.Allocator, io: native.IoBorrow, snapshot_root: []const u8, path: []const u8, opts: db.OpenOptions, identity: db.RestoreIdentity, cancellation: types.CancellationToken) engine.Outcome(void) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = DB.restoreSnapshotToDeferredRuntimeRepairWithIoAndCancellation(staged_generation, alloc, received_io, snapshot_root, path, opts, identity, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn restoreMaterializedNativeGenerationToDeferredRuntimeRepairWithIoAndCancellation(staged_generation: *const @import("db/generation_lifecycle.zig").StagedGeneration, alloc: std.mem.Allocator, io: native.IoBorrow, primary_snapshot_root: []const u8, path: []const u8, opts: db.OpenOptions, identity: db.RestoreIdentity, native_generation: *@import("db/native_backup.zig").LoadedManifest, cancellation: types.CancellationToken) engine.Outcome(void) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = DB.restoreMaterializedNativeGenerationToDeferredRuntimeRepairWithIoAndCancellation(staged_generation, alloc, received_io, primary_snapshot_root, path, opts, identity, native_generation, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn restoreMaterializedNativePrimaryAndPlanProjectionsWithIoAndCancellation(staged_generation: *const @import("db/generation_lifecycle.zig").StagedGeneration, alloc: std.mem.Allocator, io: native.IoBorrow, primary_snapshot_root: []const u8, path: []const u8, opts: db.OpenOptions, identity: db.RestoreIdentity, native_generation: *@import("db/native_backup.zig").LoadedManifest, cancellation: types.CancellationToken) engine.Outcome(void) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = DB.restoreMaterializedNativePrimaryAndPlanProjectionsWithIoAndCancellation(staged_generation, alloc, received_io, primary_snapshot_root, path, opts, identity, native_generation, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn finishMaterializedNativeGenerationWithIo(staged_generation: *const @import("db/generation_lifecycle.zig").StagedGeneration, alloc: std.mem.Allocator, io: native.IoBorrow, path: []const u8, opts: db.OpenOptions, native_generation: *@import("db/native_backup.zig").LoadedManifest, complete_runtime_repair: bool) engine.Outcome(void) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = DB.finishMaterializedNativeGenerationWithIo(staged_generation, alloc, received_io, path, opts, native_generation, complete_runtime_repair) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn restoreSnapshotToLocalDeferredRuntimeRepairWithIoAndCancellation(staged_generation: *const @import("db/generation_lifecycle.zig").StagedGeneration, alloc: std.mem.Allocator, io: native.IoBorrow, snapshot_root: []const u8, path: []const u8, opts: db.OpenOptions, cancellation: types.CancellationToken) engine.Outcome(bool) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = DB.restoreSnapshotToLocalDeferredRuntimeRepairWithIoAndCancellation(staged_generation, alloc, received_io, snapshot_root, path, opts, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn markRestorePrimaryRestoredForPathWithArtifactWithIo(alloc: std.mem.Allocator, io: native.IoBorrow, path: []const u8, backup_id: []const u8, location: []const u8, artifact_sha256: []const u8, snapshot_path: []const u8, group_id: u64) engine.Outcome(void) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = DB.markRestorePrimaryRestoredForPathWithArtifactWithIo(alloc, received_io, path, backup_id, location, artifact_sha256, snapshot_path, group_id) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

const descriptor: engine.Descriptor = .{
    .table_layout = engine.table_layout,
    .db_layout = engine.db_layout,
    .core_layout = engine.core_layout,
    .handle_layout = engine.handle_layout,
    .registry_layout = engine.registry_layout,
    .backend_runtime_layout = engine.backend_runtime_layout,
    .resource_manager_layout = engine.resource_manager_layout,
    .prepared_owner_layout = engine.prepared_owner_layout,
    .prepared_allocator_layout = engine.prepared_allocator_layout,
    .embedding_memo_layout = engine.embedding_memo_layout,
    .async_context_layout = engine.async_context_layout,
    .enrichment_runtime_layout = engine.enrichment_runtime_layout,
    .relationalReader_layout = engine.relationalReader_layout,
    .relationalSession_layout = engine.relationalSession_layout,
    .documentSession_layout = engine.documentSession_layout,

    .index_manager_layout = engine.index_manager_layout,
    .index_manager_fields = engine.index_manager_fields.ptr,
    .index_manager_field_count = engine.index_manager_fields.len,
    .enrichment_set_layout = engine.enrichment_set_layout,
    .backup_snapshot_layout = engine.backup_snapshot_layout,
    .algebraic_index_layout = engine.algebraic_index_layout,
    .docstore_layout = engine.docstore_layout,
    .rewrite_program_layout = engine.rewrite_program_layout,
    .resolution_runtime_layout = engine.resolution_runtime_layout,
    .promotion_runtime_layout = engine.promotion_runtime_layout,
    .pinned_snapshot_layout = engine.pinned_snapshot_layout,
    .native_manifest_layout = engine.native_manifest_layout,
    .writer_layout = engine.writer_layout,
    .generation_manager_layout = engine.generation_manager_layout,
    .staged_generation_layout = engine.staged_generation_layout,
    .table_fields = engine.table_fields.ptr,
    .table_field_count = engine.table_fields.len,
    .backend_fields = engine.backend_fields.ptr,
    .backend_field_count = engine.backend_fields.len,
    .resource_fields = engine.resource_fields.ptr,
    .resource_field_count = engine.resource_fields.len,
    .db_fields = engine.db_fields.ptr,
    .db_field_count = engine.db_fields.len,
    .core_fields = engine.core_fields.ptr,
    .core_field_count = engine.core_fields.len,
    .dispatch = engine.Boundary.local_dispatch,
};
const aggregation_contract = @import("aggregation_engine.zig");
const aggregation_descriptor: aggregation_contract.Descriptor = .{
    .method_layout = aggregation_contract.method_layout,
    .context_layout = aggregation_contract.context_layout,
    .index_manager_layout = aggregation_contract.index_manager_layout,
    .docstore_layout = aggregation_contract.docstore_layout,
    .dispatch = engine.Boundary.local_dispatch,
};
fn getAggregationDescriptor() callconv(.c) *const aggregation_contract.Descriptor {
    return &aggregation_descriptor;
}
fn getCAllocator() callconv(.c) ?*const Allocator {
    if (comptime @import("builtin").link_libc) return &std.heap.c_allocator;
    return null;
}
fn getDescriptor() callconv(.c) *const engine.Descriptor {
    return &descriptor;
}
comptime {
    for (@typeInfo(engine.VTable).@"struct".field_names) |name| {
        const Accessor = struct {
            fn get() callconv(.c) *const anyopaque {
                return @ptrCast(&@field(Exports, name));
            }
        };
        @export(&Accessor.get, .{ .name = "antfly_storage_engine_" ++ name ++ "_v32", .visibility = .hidden });
    }
    @export(&clearError, .{ .name = "antfly_storage_error_clear_v1", .visibility = .hidden });
    @export(&errorCode, .{ .name = "antfly_storage_error_code_v1", .visibility = .hidden });
    @export(&errorDetail, .{ .name = "antfly_storage_error_detail_v1", .visibility = .hidden });
    @export(&setError, .{ .name = "antfly_storage_error_set_v1", .visibility = .hidden });
    @export(&getAggregationDescriptor, .{ .name = "antfly_storage_engine_get_aggregation_descriptor_v1", .visibility = .hidden });
    @export(&getDescriptor, .{ .name = "antfly_storage_engine_get_descriptor_v32", .visibility = .hidden });
    @export(&getCAllocator, .{ .name = "antfly_storage_engine_c_allocator_v1", .visibility = .hidden });
}
