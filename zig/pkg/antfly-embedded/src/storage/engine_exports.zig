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
pub fn portableExportToWriterWithOptions(alloc: Allocator, store: *@import("docstore.zig").DocStore, writer: engine.WriterBorrow, options: engine.PortableExportOptions) engine.Outcome(void) {
    var receiving_writer = writer.receive();
    var spool_receiver: ?@import("antfly_runtime_abi").io_abi.Receiver = if (options.spool) |spool| spool.io.receive() catch |err| return .{ .failure = .init(@errorName(err)) } else null;
    var received_options = options.value;
    if (spool_receiver) |*receiver| received_options.spool = .{ .io = receiver.io(), .file = options.spool.?.file };
    @import("portable_backup.zig").exportPortableToWriterWithOptions(alloc, store, receiving_writer.writer(), received_options) catch |err| return .{ .failure = .init(@errorName(err)) };
    receiving_writer.finish() catch |err| return .{ .failure = .init(@errorName(err)) };
    return .{ .value = {} };
}
pub fn portableExport(alloc: std.mem.Allocator, store: *@import("docstore.zig").DocStore, out: *std.ArrayList(u8)) engine.Outcome(void) {
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
pub fn searchWithDenseProfile(self: *DB, alloc: Allocator, req: types.SearchRequest) engine.Outcome(SearchWithDenseProfileResult) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    var value = self.searchWithDenseProfile(alloc, received) catch |err| return .{ .failure = .init(@errorName(err)) };
    // Captured requests retain caller-owned hooks, never the stack proxy.
    value.request.native_key_predicate = req.native_key_predicate;
    return .{ .value = value };
}

pub fn searchWithCapturedRequestAndExecutionContext(
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

pub fn searchLease(lease: DB.QueryReadLease, alloc: Allocator, req: types.SearchRequest) engine.Outcome(SearchWithDenseProfileResult) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    var value = lease.search(alloc, received) catch |err| return .{ .failure = .init(@errorName(err)) };
    // Captured requests retain caller-owned hooks, never the stack proxy.
    value.request.native_key_predicate = req.native_key_predicate;
    return .{ .value = value };
}

pub fn registry() engine.Outcome(*anyopaque) {
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
pub fn get(self: *DB, alloc: Allocator, key: []const u8) engine.Outcome(?[]u8) {
    return .{ .value = self.get(alloc, key) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn batchPrepared(self: *DB.PreparedBatchOwner, req: types.BatchRequest, profile: ?*BatchProfile, opts: BatchExecutionOptions, memo: *@import("db/execution_resources.zig").GeneratedEmbeddingMemo, allocator: *@import("db/execution_resources.zig").PreparedRowAllocator) engine.Outcome(void) {
    return .{ .value = self.batchInternalPrepared(req, profile, opts, memo, allocator) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
pub fn searchDenseProfiled(self: *DB, alloc: Allocator, req: types.SearchRequest, dense: types.DenseKnnQuery) engine.Outcome(@import("db/query/search_exec.zig").ProfiledDenseSearchResult) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    return .{ .value = self.searchDenseProfiled(alloc, received, dense) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn searchDenseProfiledWithCapturedRequest(self: *DB, alloc: Allocator, req: types.SearchRequest, dense: types.DenseKnnQuery) engine.Outcome(db.ProfiledDenseSearchWithCapturedRequestResult) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    var value = self.searchDenseProfiledWithCapturedRequest(alloc, received, dense) catch |err| return .{ .failure = .init(@errorName(err)) };
    value.request.native_key_predicate = req.native_key_predicate;
    return .{ .value = value };
}

pub fn preflightSearchRequestWithExecutionContext(self: *DB, alloc: Allocator, req: types.SearchRequest, max_work: u32, exec_ctx: types.ExecutionContext) engine.Outcome(@import("db/query/search_exec.zig").RuntimePreflightSummary) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    return .{ .value = self.preflightSearchRequestWithExecutionContext(alloc, received, max_work, exec_ctx) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn collectPlanningStatsWithExecutionContext(self: *DB, alloc: Allocator, req: types.SearchRequest, max_work: u32, exec_ctx: types.ExecutionContext) engine.Outcome(@import("db/planning_stats.zig").PlanningStatsSummary) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    return .{ .value = self.collectPlanningStatsWithExecutionContext(alloc, received, max_work, exec_ctx) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn collectSearchRequestTextStats(self: *DB, alloc: Allocator, req: types.SearchRequest) engine.Outcome([]const @import("../search/distributed_stats.zig").TextFieldStats) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    return .{ .value = self.collectSearchRequestTextStats(alloc, received) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn trySnapshotDenseSearch(self: *DB, alloc: std.mem.Allocator, req: types.SearchRequest, dense: types.DenseKnnQuery, include_profile: bool) engine.Outcome(?db.SearchWithDenseProfileResult) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    var value = self.trySnapshotDenseSearch(alloc, received, dense, include_profile) catch |err| return .{ .failure = .init(@errorName(err)) };
    if (value) |*captured| captured.request.native_key_predicate = req.native_key_predicate;
    return .{ .value = value };
}

pub fn navigateHierarchyChildren(self: *DB, alloc: std.mem.Allocator, req: types.SearchRequest) engine.Outcome(types.SearchResult) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    return .{ .value = self.navigateHierarchyChildren(alloc, received) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn searchDense(self: *DB, alloc: std.mem.Allocator, req: types.SearchRequest, dense: types.DenseKnnQuery) engine.Outcome(types.SearchResult) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    return .{ .value = self.searchDense(alloc, received, dense) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn searchSparse(self: *DB, alloc: std.mem.Allocator, req: types.SearchRequest, sparse: types.SparseKnnQuery) engine.Outcome(types.SearchResult) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    return .{ .value = self.searchSparse(alloc, received, sparse) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn searchGraph(self: *DB, alloc: std.mem.Allocator, req: types.SearchRequest, graph_query: @import("../graph/query.zig").GraphQuery, base_hits: ?[]const types.SearchHit) engine.Outcome(types.SearchResult) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    return .{ .value = self.searchGraph(alloc, received, graph_query, base_hits) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn graphHydrateKeysForInternalRead(self: *DB, alloc: std.mem.Allocator, req: types.SearchRequest, keys: []const []const u8) engine.Outcome([]types.SearchHit) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    return .{ .value = self.graphHydrateKeysForInternalRead(alloc, received, keys) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn applyDerivedBatchToIndexAsync(ctx: *@import("db/execution_resources.zig").AsyncContext, batch: @import("db/derived/derived_types.zig").DerivedBatch, index_ref: @import("db/catalog/index_manager.zig").ManagedIndexRef, token: @import("db/derived/derived_executor.zig").CatchUpSessionToken) engine.Outcome(bool) {
    return .{ .value = db.applyDerivedBatchToIndexAsync(@ptrCast(ctx), batch, index_ref, token) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn enrichmentStart(self: *@import("db/enrichment/enrichment_runtime.zig").EnrichmentRuntime) engine.Outcome(void) {
    return .{ .value = self.start() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn enrichmentReloadDurableState(self: *@import("db/enrichment/enrichment_runtime.zig").EnrichmentRuntime) engine.Outcome(void) {
    return .{ .value = self.reloadDurableState() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn enrichmentResumeFrom(self: *@import("db/enrichment/enrichment_runtime.zig").EnrichmentRuntime, sequence: u64, target_sequence: u64) engine.Outcome(void) {
    return .{ .value = self.resumeFrom(sequence, target_sequence) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn enrichmentWaitForAppliedWithVisibilityDeadline(self: *@import("db/enrichment/enrichment_runtime.zig").EnrichmentRuntime, sequence: u64, cancellation: types.CancellationToken, deadline_ns: ?u64) engine.Outcome(void) {
    return .{ .value = self.waitForAppliedWithVisibilityDeadline(sequence, cancellation, deadline_ns) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn enrichmentCatchUpUntil(self: *@import("db/enrichment/enrichment_runtime.zig").EnrichmentRuntime, sequence: u64) engine.Outcome(void) {
    return .{ .value = self.catchUpUntil(sequence) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn enrichmentCatchUpUntilWithVisibilityDeadline(self: *@import("db/enrichment/enrichment_runtime.zig").EnrichmentRuntime, sequence: u64, cancellation: types.CancellationToken, deadline_ns: ?u64) engine.Outcome(void) {
    return .{ .value = self.catchUpUntilWithVisibilityDeadline(sequence, cancellation, deadline_ns) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn relationalReaderDeinit(self: *@import("db/relational_rows.zig").Reader) engine.Outcome(void) {
    return .{ .value = self.deinit() };
}

pub fn relationalReaderNextPage(self: *@import("db/relational_rows.zig").Reader, alloc: Allocator, io: ?native.IoBorrow, budget: @import("db/relational_rows.zig").Budget) engine.Outcome(@import("db/relational_rows.zig").Page) {
    var receiver: ?@import("antfly_runtime_abi").io_abi.Receiver = if (io) |borrow| borrow.receive() catch |err| return .{ .failure = .init(@errorName(err)) } else null;
    const received_io: ?std.Io = if (receiver) |*value| value.io() else null;
    return .{ .value = self.nextPage(alloc, received_io, budget) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn relationalReaderNextTypedPage(self: *@import("db/relational_rows.zig").Reader, alloc: Allocator, io: ?native.IoBorrow, budget: @import("db/relational_rows.zig").Budget) engine.Outcome(@import("db/relational_rows.zig").Page) {
    var receiver: ?@import("antfly_runtime_abi").io_abi.Receiver = if (io) |borrow| borrow.receive() catch |err| return .{ .failure = .init(@errorName(err)) } else null;
    const received_io: ?std.Io = if (receiver) |*value| value.io() else null;
    return .{ .value = self.nextTypedPage(alloc, received_io, budget) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn relationalReaderLookupSqlBatch(self: *@import("db/relational_rows.zig").Reader, alloc: Allocator, documents: []const []const u8, layout: *const @import("../sql/catalog.zig").Row.TypedLayout, max_bytes: usize) engine.Outcome(@import("db/relational_rows.zig").Reader.Hydrated) {
    return .{ .value = self.lookupSqlBatch(alloc, documents, layout, max_bytes) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn relationalReaderLookupTypedRow(self: *@import("db/relational_rows.zig").Reader, alloc: Allocator, document: []const u8, native_cells: bool, shared: ?*?*const @import("../sql/catalog.zig").Row.TypedLayout) engine.Outcome(?@import("db/relational_rows.zig").Row) {
    return .{ .value = (if (native_cells) if (shared) |layout| self.lookupSqlRowWithLayout(alloc, document, layout) else self.lookupSqlRow(alloc, document) else self.lookupTypedRow(alloc, document)) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn relationalSessionDeinit(session: *@import("db/relational_read_session.zig").Session) engine.Outcome(void) {
    return .{ .value = session.deinit() };
}

pub fn relationalSessionCheckpoint(session: *const @import("db/relational_read_session.zig").Session) engine.Outcome(void) {
    return .{ .value = session.checkpoint() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn relationalSessionRangeProofs(session: *@import("db/relational_read_session.zig").Session, alloc: Allocator) engine.Outcome([]@import("range_protection.zig").Proof) {
    return .{ .value = session.rangeProofs(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn relationalSessionNextTypedPage(session: *@import("db/relational_read_session.zig").Session, alloc: Allocator, io: ?native.IoBorrow, budget: @import("db/relational_rows.zig").Budget) engine.Outcome(@import("db/relational_rows.zig").Page) {
    var receiver: ?@import("antfly_runtime_abi").io_abi.Receiver = if (io) |borrow| borrow.receive() catch |err| return .{ .failure = .init(@errorName(err)) } else null;
    const received_io: ?std.Io = if (receiver) |*value| value.io() else null;
    return .{ .value = session.nextTypedPage(alloc, received_io, budget) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn relationalSessionNormalizeRows(session: *@import("db/relational_read_session.zig").Session, alloc: Allocator, writes: []const types.BatchWrite) engine.Outcome([]types.BatchWrite) {
    return .{ .value = session.normalizeRows(alloc, writes) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn documentSessionDeinit(self: *@import("db/document/rows.zig").Session) engine.Outcome(void) {
    return .{ .value = self.deinit() };
}

pub fn documentSessionRangeProofs(self: *@import("db/document/rows.zig").Session, alloc: std.mem.Allocator) engine.Outcome([]@import("range_protection.zig").Proof) {
    return .{ .value = self.rangeProofs(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn documentSessionNormalizeRows(self: *@import("db/document/rows.zig").Session, alloc: std.mem.Allocator, writes: []const types.BatchWrite) engine.Outcome([]types.BatchWrite) {
    return .{ .value = self.normalizeRows(alloc, writes) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn documentSessionNext(self: *@import("db/document/rows.zig").Session, alloc: std.mem.Allocator, limit: u32) engine.Outcome(@import("relational_read_view.zig").View.Page) {
    return .{ .value = self.next(alloc, limit) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn catchUpPendingDerivedReplayWithProgress(self: *DB, context: *anyopaque, hook: db.ReplayProgressHook, dispatch: @import("named_callback.zig").Dispatch) engine.Outcome(void) {
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
pub fn relationalSnapshotDeinit(cut: *DB.RelationalStatementSnapshot) engine.Outcome(void) {
    return .{ .value = cut.deinit() };
}
pub fn statementFenceRelease(fence: *DB.StatementReadFence) engine.Outcome(void) {
    return .{ .value = fence.release() };
}
pub fn documentRowsUnderLease(lease: *const DB.QueryReadLease, alloc: Allocator, version: u32) engine.Outcome(DB.DocumentRows) {
    return .{ .value = lease.documentRows(alloc, version) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
pub fn documentRowsLookupProjectedBatch(reader: *DB.DocumentRows, ids: []const []const u8, fields: []const []const u8, max_bytes: usize) engine.Outcome(DB.DocumentRows.ProjectedBatch) {
    return .{ .value = reader.lookupProjectedBatch(ids, fields, max_bytes) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
pub fn documentRowsLookupBatch(reader: *DB.DocumentRows, ids: []const []const u8, max_bytes: usize) engine.Outcome(DB.DocumentRows.Batch) {
    return .{ .value = reader.lookupBatch(ids, max_bytes) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
pub fn documentRowsLookup(reader: *DB.DocumentRows, key: []const u8) engine.Outcome([]const u8) {
    return .{ .value = reader.lookup(key) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
pub fn documentRowsReleasePage(reader: *DB.DocumentRows) engine.Outcome(void) {
    return .{ .value = reader.releasePage() };
}
pub fn documentRowsDeinit(reader: *DB.DocumentRows) engine.Outcome(void) {
    return .{ .value = reader.deinit() };
}
pub fn relationalRowsUnderLease(lease: *const DB.QueryReadLease, alloc: Allocator, fields: []const []const u8, version: u32) engine.Outcome(@import("db/relational_rows.zig").Reader) {
    return .{ .value = lease.relationalRows(alloc, fields, version) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
pub fn exportBackupCohortPortable(self: *DB, handle: @import("db/native/backup_seal.zig").Handle, writer: engine.WriterBorrow, options: engine.PortableExportOptions, cancellation: types.CancellationToken) engine.Outcome(void) {
    var receiving_writer = writer.receive();
    var spool_receiver: ?@import("antfly_runtime_abi").io_abi.Receiver = if (options.spool) |spool| spool.io.receive() catch |err| return .{ .failure = .init(@errorName(err)) } else null;
    var received_options = options.value;
    if (spool_receiver) |*receiver| received_options.spool = .{ .io = receiver.io(), .file = options.spool.?.file };
    self.exportBackupCohortPortable(handle, receiving_writer.writer(), received_options, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) };
    receiving_writer.finish() catch |err| return .{ .failure = .init(@errorName(err)) };
    return .{ .value = {} };
}

pub fn recoverIncompleteRestoreImportIfNeededWithIo(alloc: std.mem.Allocator, io: native.IoBorrow, path: []const u8, opts: db.OpenOptions) engine.Outcome(bool) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = DB.recoverIncompleteRestoreImportIfNeededWithIo(alloc, received_io, path, opts) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn markRestorePrimaryRestoredForPathWithIdentityWithIo(alloc: std.mem.Allocator, io: native.IoBorrow, path: []const u8, identity: db.RestoreIdentity) engine.Outcome(void) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = DB.markRestorePrimaryRestoredForPathWithIdentityWithIo(alloc, received_io, path, identity) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn repairRestoreRuntimeStateStepIfNeededWithIoAndRepairOptions(self: *DB, alloc: std.mem.Allocator, io: native.IoBorrow, repair_options: types.ArtifactRepairRunOptions) engine.Outcome(bool) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = self.repairRestoreRuntimeStateStepIfNeededWithIoAndRepairOptions(alloc, received_io, repair_options) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn searchRequestAtCurrentIdentityGeneration(self: *DB, req: types.SearchRequest) engine.Outcome(types.SearchRequest) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    var result = self.searchRequestAtCurrentIdentityGeneration(received) catch |err| return .{ .failure = .init(@errorName(err)) };
    // The receiving proxy borrows this stack frame. Returned requests must
    // retain the caller's predicate for subsequent searches.
    result.native_key_predicate = req.native_key_predicate;
    return .{ .value = result };
}

pub fn readRestoreStateForPathWithIo(alloc: std.mem.Allocator, io: native.IoBorrow, path: []const u8) engine.Outcome(?db.RestoreState) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = DB.readRestoreStateForPathWithIo(alloc, received_io, path) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn restoreRuntimeRepairNeededForPathWithIo(alloc: std.mem.Allocator, io: native.IoBorrow, path: []const u8) engine.Outcome(bool) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = DB.restoreRuntimeRepairNeededForPathWithIo(alloc, received_io, path) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn failGraphMetricPlannedBuild(self: *DB, alloc: Allocator, index_name: []const u8, metric_name: []const u8, failure_name: []const u8) engine.Outcome(types.GraphMetricStatus) {
    const failure = @import("storage_engine_errors").fromName(failure_name);
    return .{ .value = self.failGraphMetricPlannedBuild(alloc, index_name, metric_name, failure) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
pub fn runDenseArtifactRebuildMaintenanceWithProgress(self: *DB, alloc: Allocator, context: ?*anyopaque, hook: ?db.ReplayProgressHook, dispatch: @import("named_callback.zig").Dispatch) engine.Outcome(bool) {
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
pub fn queryReadLeaseRelease(lease: *DB.QueryReadLease) engine.Outcome(void) {
    return .{ .value = lease.release() };
}

pub fn importPortableRestoreFileIntoUnpublishedEmpty(self: *DB, alloc: std.mem.Allocator, io: native.IoBorrow, file: std.Io.File, file_size: u64, options: engine.PortableRestoreImportOptions) engine.Outcome(void) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    return .{ .value = self.importPortableRestoreFileIntoUnpublishedEmpty(alloc, receiver.io(), file, file_size, options) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn importPortableFileIntoUnpublishedEmpty(self: *DB, alloc: std.mem.Allocator, io: native.IoBorrow, file: std.Io.File, file_size: u64, target_identity: @import("db/doc_identity.zig").Namespace) engine.Outcome(void) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = self.importPortableFileIntoUnpublishedEmpty(alloc, received_io, file, file_size, target_identity) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn importCohortPortableFileIntoUnpublishedEmpty(self: *DB, alloc: std.mem.Allocator, io: native.IoBorrow, file: std.Io.File, file_size: u64, proof: @import("portable_backup.zig").CohortProof, cancellation: types.CancellationToken) engine.Outcome(void) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = self.importCohortPortableFileIntoUnpublishedEmpty(alloc, received_io, file, file_size, proof, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn search(self: *DB, alloc: std.mem.Allocator, req: types.SearchRequest) engine.Outcome(types.SearchResult) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    return .{ .value = self.search(alloc, received) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn searchWithCapturedRequest(self: *DB, alloc: std.mem.Allocator, req: types.SearchRequest) engine.Outcome(db.SearchWithCapturedRequestResult) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    var result = self.searchWithCapturedRequest(alloc, received) catch |err| return .{ .failure = .init(@errorName(err)) };
    result.request.native_key_predicate = req.native_key_predicate;
    return .{ .value = result };
}

pub fn preflightSearchRequest(self: *DB, alloc: std.mem.Allocator, req: types.SearchRequest, max_work: u32) engine.Outcome(@import("db/query/search_exec.zig").RuntimePreflightSummary) {
    var source = req.native_key_predicate;
    var received = req;
    if (source) |*predicate| received.native_key_predicate = predicate.receiver();
    return .{ .value = self.preflightSearchRequest(alloc, received, max_work) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn queryValidateGraphHydrateResolvedDocFilterForDb(req: @import("../api/graph.zig").GraphHydrateRequest, database: *@import("db/mod.zig").DB) engine.Outcome(void) {
    return .{ .value = @import("query.zig").validateGraphHydrateResolvedDocFilterForDb(req, database) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn queryExecuteStorageKernelGraphExpand(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, table_name: []const u8, req: @import("../api/graph.zig").GraphExpandRequest) engine.Outcome(@import("../api/graph.zig").GraphExpandResponse) {
    return .{ .value = @import("query.zig").executeStorageKernelGraphExpand(alloc, database, table_name, req) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn queryExecuteStorageKernelGraphHydrate(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, req: @import("../api/graph.zig").GraphHydrateRequest) engine.Outcome(@import("../api/graph.zig").GraphHydrateResponse) {
    return .{ .value = @import("query.zig").executeStorageKernelGraphHydrate(alloc, database, req) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn queryExecuteStorageKernelGraphEdges(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, req: @import("../api/graph.zig").GraphEdgesRequest) engine.Outcome(@import("../api/graph.zig").GraphEdgesResponse) {
    return .{ .value = @import("query.zig").executeStorageKernelGraphEdges(alloc, database, req) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn queryValidateGraphHydrateIncomingIndexIdentity(req: @import("../api/graph.zig").GraphHydrateRequest, database: *@import("db/mod.zig").DB) engine.Outcome(void) {
    return .{ .value = @import("query.zig").validateGraphHydrateIncomingIndexIdentity(req, database) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn queryCurrentIdentityReadGenerationForDb(requested: ?u64, database: *@import("db/mod.zig").DB) engine.Outcome(u64) {
    return .{ .value = @import("query.zig").currentIdentityReadGenerationForDb(requested, database) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn queryAlgebraicIndexFreshEnoughForName(alloc: std.mem.Allocator, index_name_opt: ?[]const u8, database: *@import("db/mod.zig").DB) engine.Outcome(bool) {
    return .{ .value = @import("query.zig").algebraicIndexFreshEnoughForName(alloc, index_name_opt, database) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn queryCollectTextStatsFromDbForRequest(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, request: @import("../api/query_execution_contract.zig").ParsedTextStatsRequest) engine.Outcome([]const @import("../search/distributed_stats.zig").TextFieldStats) {
    return .{ .value = @import("query.zig").collectTextStatsFromDbForRequest(alloc, database, request) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn queryCollectBackgroundTextStatsFromDbForRequest(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, request: @import("../api/query_execution_contract.zig").ParsedTextStatsRequest) engine.Outcome([]const @import("db/mod.zig").aggregations.DistributedBackgroundTextStats) {
    return .{ .value = @import("query.zig").collectBackgroundTextStatsFromDbForRequest(alloc, database, request) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn queryCollectAlgebraicPartialsFromDbForRequest(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, request: @import("../api/query_execution_contract.zig").ParsedAlgebraicPartialsRequest) engine.Outcome([]@import("db/mod.zig").algebraic.distributed.Partial) {
    return .{ .value = @import("query.zig").collectAlgebraicPartialsFromDbForRequest(alloc, database, request) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn queryAlgebraicTensorProgramOutputExpressionsForIndexAlloc(alloc: std.mem.Allocator, index: ?*const @import("db/mod.zig").algebraic.index.Index, access_paths: []@import("../api/query_execution_contract.zig").OwnedAlgebraicTensorAccessPath, program: *const @import("../api/query_execution_contract.zig").OwnedAlgebraicTensorProgram) engine.Outcome([]@import("../api/query_execution_contract.zig").algebraic_ir.TensorExpr) {
    return .{ .value = @import("query.zig").algebraicTensorProgramOutputExpressionsForIndexAlloc(alloc, index, access_paths, program) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn queryAlgebraicTensorProgramOutputExpressionForStep(alloc: std.mem.Allocator, index: ?*const @import("db/mod.zig").algebraic.index.Index, path_values: []const @import("../api/query_execution_contract.zig").algebraic_ir.PhysicalAccessPath, expr: @import("../api/query_execution_contract.zig").algebraic_ir.TensorExpr) engine.Outcome(@import("../api/query_execution_contract.zig").algebraic_ir.TensorExpr) {
    return .{ .value = @import("query.zig").algebraicTensorProgramOutputExpressionForStep(alloc, index, path_values, expr) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn queryFindAlgebraicMaterialization(index: *const @import("db/mod.zig").algebraic.index.Index, name: []const u8) engine.Outcome(?@import("db/mod.zig").algebraic.index.MaterializationConfig) {
    return .{ .value = @import("query.zig").findAlgebraicMaterialization(index, name) };
}

pub fn queryExecuteStorageKernelTextStats(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, table_name: []const u8, body: []const u8) engine.Outcome([]u8) {
    return .{ .value = @import("query.zig").executeStorageKernelTextStats(alloc, database, table_name, body) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn queryExecuteStorageKernelAlgebraicPartials(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, body: []const u8) engine.Outcome([]u8) {
    return .{ .value = @import("query.zig").executeStorageKernelAlgebraicPartials(alloc, database, body) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn queryExecuteStorageKernelPreflight(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, table_name: []const u8, request_json: []const u8) engine.Outcome([]u8) {
    return .{ .value = @import("query.zig").executeStorageKernelPreflight(alloc, database, table_name, request_json) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn computeSearchAggregations(alloc: Allocator, requests: []const @import("db/aggregations.zig").SearchAggregationRequest, result: types.SearchResult, ctx: @import("db/aggregations.zig").Context) engine.Outcome([]@import("db/aggregations.zig").SearchAggregationResult) {
    return .{ .value = @import("db/aggregations.zig").computeSearchAggregations(alloc, requests, result, ctx) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn portableDecoderStep(alloc: Allocator, io: native.IoBorrow, file: std.Io.File, plan: @import("portable_decoder.zig").Plan, cancellation: types.CancellationToken) engine.Outcome(bool) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = @import("portable_decoder.zig").step(alloc, received_io, file, plan, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
pub fn portableDecoderBindRange(alloc: Allocator, store: *@import("docstore.zig").DocStore, range: types.ByteRange) engine.Outcome(void) {
    return .{ .value = @import("portable_decoder.zig").bindRange(alloc, store, range) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
pub fn writeCorruptEmbeddingArtifactInDb(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, doc_key: []const u8, index_name: []const u8) engine.Outcome(bool) {
    return .{ .value = @import("write.zig").corruptEmbeddingArtifactInDb(alloc, database, doc_key, index_name) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn writeConfigureRestoreOwnerDb(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, bootstrap: @import("db/restore_staging_contract.zig").OwnerBootstrap, allow_canceled: bool, hot_standby_replay: bool) engine.Outcome(void) {
    return .{ .value = @import("write.zig").configureRestoreOwnerDb(alloc, database, bootstrap, allow_canceled, hot_standby_replay) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn writeCreateManagedDbEnrichments(allocator: std.mem.Allocator, raw_indexes_json: []const u8, runtime: ?*@import("db/mod.zig").background_runtime.BackendRuntime, local_provider: ?@import("../inference/managed_embedder.zig").AntflyProvider, remote_capability_cache: ?*@import("antfly_inference_remote_capabilities").Cache, inference_api_url: ?[]const u8, source_table: []const u8, store: ?*@import("../common/secrets.zig").FileStore, remote: ?*const @import("write.zig").scraping.RemoteContentConfig) engine.Outcome(@import("write.zig").ManagedDbEnrichmentSet) {
    return .{ .value = @import("write.zig").createManagedDbEnrichments(allocator, raw_indexes_json, runtime, local_provider, remote_capability_cache, inference_api_url, source_table, store, remote) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn writeReconfigureManagedDbEnrichmentRuntime(ignored_allocator: std.mem.Allocator, database: *@import("db/mod.zig").DB, indexes_json: []const u8, backend_runtime: ?*@import("db/mod.zig").background_runtime.BackendRuntime, antfly_provider: ?@import("../inference/managed_embedder.zig").AntflyProvider, remote_capability_cache: ?*@import("antfly_inference_remote_capabilities").Cache, inference_api_url: ?[]const u8, source_table: []const u8, secret_store: ?*@import("../common/secrets.zig").FileStore, remote_content: ?*const @import("write.zig").scraping.RemoteContentConfig) engine.Outcome(void) {
    return .{ .value = @import("write.zig").reconfigureManagedDbEnrichmentRuntime(ignored_allocator, database, indexes_json, backend_runtime, antfly_provider, remote_capability_cache, inference_api_url, source_table, secret_store, remote_content) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn writePrepareManagedSchemaBeforeIndexLoad(alloc: std.mem.Allocator, mode: @import("../api/write_contract.zig").ManagedDbOpenMode, schema_json: ?[]const u8) engine.Outcome(?@import("db/mod.zig").SchemaBeforeIndexLoad) {
    return .{ .value = @import("write.zig").prepareManagedSchemaBeforeIndexLoad(alloc, mode, schema_json) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn writeOpenManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentityWithOptions(alloc: std.mem.Allocator, path: []const u8, indexes_json: []const u8, lsm_cache: ?*@import("lsm_backend/mod.zig").Cache, hbc_cache: ?*@import("hbc_adapter.zig").Cache, lsm_root_generation: u64, resource_manager: ?*@import("resource_manager.zig").ResourceManager, mode: @import("../api/write_contract.zig").ManagedDbOpenMode, backend_runtime: ?*@import("db/mod.zig").background_runtime.BackendRuntime, antfly_provider: ?@import("../inference/managed_embedder.zig").AntflyProvider, secret_store: ?*@import("../common/secrets.zig").FileStore, remote_content: ?*const @import("write.zig").scraping.RemoteContentConfig, identity_namespace: ?@import("db/doc_identity.zig").Namespace, options: @import("write.zig").ManagedDbOpenOptions) engine.Outcome(@import("db/mod.zig").DB) {
    return .{ .value = @import("write.zig").openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentityWithOptions(alloc, path, indexes_json, lsm_cache, hbc_cache, lsm_root_generation, resource_manager, mode, backend_runtime, antfly_provider, secret_store, remote_content, identity_namespace, options) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn writeLoadLocalTableSchemaJson(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB) engine.Outcome(?[]u8) {
    return .{ .value = @import("write.zig").loadLocalTableSchemaJson(alloc, database) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn writeExportPortableBackupShard(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, backup_root: []const u8, backup_id: []const u8, group_id: u64, shared_io: ?native.IoBorrow) engine.Outcome([]@import("../api/backups.zig").ShardSnapshot) {
    var receiver: ?@import("antfly_runtime_abi").io_abi.Receiver = if (shared_io) |borrow| borrow.receive() catch |err| return .{ .failure = .init(@errorName(err)) } else null;
    const received_io: ?std.Io = if (receiver) |*value| value.io() else null;
    return .{ .value = @import("write.zig").exportPortableBackupShard(alloc, database, backup_root, backup_id, group_id, received_io) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn writeExportPortableBackupShardWithSeal(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, backup_root: []const u8, backup_id: []const u8, group_id: u64, shared_io: ?native.IoBorrow, sealed: ?@import("../api/backup_contract.zig").SealedHandle, cancellation: @import("db/mod.zig").types.CancellationToken) engine.Outcome([]@import("../api/backups.zig").ShardSnapshot) {
    var receiver: ?@import("antfly_runtime_abi").io_abi.Receiver = if (shared_io) |borrow| borrow.receive() catch |err| return .{ .failure = .init(@errorName(err)) } else null;
    const received_io: ?std.Io = if (receiver) |*value| value.io() else null;
    return .{ .value = @import("write.zig").exportPortableBackupShardWithSeal(alloc, database, backup_root, backup_id, group_id, received_io, sealed, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn writeCopyPreparedNativeBackupShardSnapshot(alloc: std.mem.Allocator, native_snapshot: *@import("write.zig").NativeBackupShardSnapshot) engine.Outcome([]@import("../api/backups.zig").ShardSnapshot) {
    return .{ .value = @import("write.zig").copyPreparedNativeBackupShardSnapshot(alloc, native_snapshot) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn writeBackupStorageKernelOwnerDb(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, db_path: []const u8, group_id: u64, backup_root: []const u8, backup_id: []const u8, format: @import("../api/backups.zig").BackupFormat) engine.Outcome([]@import("../api/backups.zig").ShardSnapshot) {
    return .{ .value = @import("write.zig").backupStorageKernelOwnerDb(alloc, database, db_path, group_id, backup_root, backup_id, format) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn writeBackupStorageKernelOwnerDbWithControl(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, db_path: []const u8, group_id: u64, backup_root: []const u8, backup_id: []const u8, format: @import("../api/backups.zig").BackupFormat, cohort: ?@import("db/relational_integrity_topology_contract.zig").Fence, sealed: ?@import("db/native/backup_seal_contract.zig").Handle, control: @import("../api/backups.zig").BackupOperationControl) engine.Outcome([]@import("../api/backups.zig").ShardSnapshot) {
    return .{ .value = @import("write.zig").backupStorageKernelOwnerDbWithControl(alloc, database, db_path, group_id, backup_root, backup_id, format, cohort, sealed, control) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn writeExportPortableBackupFile(alloc: std.mem.Allocator, store: *@import("db/mod.zig").docstore.DocStore, path: []const u8, shared_io: ?native.IoBorrow) engine.Outcome(void) {
    var receiver: ?@import("antfly_runtime_abi").io_abi.Receiver = if (shared_io) |borrow| borrow.receive() catch |err| return .{ .failure = .init(@errorName(err)) } else null;
    const received_io: ?std.Io = if (receiver) |*value| value.io() else null;
    return .{ .value = @import("write.zig").exportPortableBackupFile(alloc, store, path, received_io) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn writeExportPortableBackupFileWithIo(alloc: std.mem.Allocator, store: *@import("db/mod.zig").docstore.DocStore, path: []const u8, io: native.IoBorrow) engine.Outcome(void) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = @import("write.zig").exportPortableBackupFileWithIo(alloc, store, path, received_io) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn writeResolveWritesForSchemaValidation(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, base_writes: []const @import("db/mod.zig").types.BatchWrite, deletes: []const []const u8, transforms: []const @import("db/mod.zig").types.DocumentTransform) engine.Outcome([]@import("db/mod.zig").types.BatchWrite) {
    return .{ .value = @import("write.zig").resolveWritesForSchemaValidation(alloc, database, base_writes, deletes, transforms) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn writeValidateGroupedSourceBatch(alloc: Allocator, database: *DB, writes: []const types.BatchWrite, deletes: []const []const u8, transforms: []const types.DocumentTransform) engine.Outcome(void) {
    return .{ .value = @import("write.zig").validateGroupedSourceBatch(alloc, database, writes, deletes, transforms) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn writeValidateTableBatchAgainstLocalSchema(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, writes: []const @import("db/mod.zig").types.BatchWrite, deletes: []const []const u8, transforms: []const @import("db/mod.zig").types.DocumentTransform) engine.Outcome(void) {
    return .{ .value = @import("write.zig").validateTableBatchAgainstLocalSchema(alloc, database, writes, deletes, transforms) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn writeApplyLocalTableSchemaJson(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, schema_json: []const u8) engine.Outcome(void) {
    return .{ .value = @import("write.zig").applyLocalTableSchemaJson(alloc, database, schema_json) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn writePrepareOwnerSchemaBeforeIndexLoad(alloc: std.mem.Allocator, schema_json: []const u8) engine.Outcome(?@import("db/mod.zig").SchemaBeforeIndexLoad) {
    return .{ .value = @import("write.zig").prepareOwnerSchemaBeforeIndexLoad(alloc, schema_json) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn writeConfigureStorageKernelOwnerDb(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, table_name: []const u8, schema_json: []const u8, indexes_json: []const u8, backend_runtime: ?*@import("db/mod.zig").background_runtime.BackendRuntime, antfly_provider: ?@import("../inference/managed_embedder.zig").AntflyProvider, secret_store: ?*@import("../common/secrets.zig").FileStore, remote_content: ?*const @import("write.zig").scraping.RemoteContentConfig, installed: ?*@import("write.zig").OwnerManagedConfig) engine.Outcome(void) {
    return .{ .value = @import("write.zig").configureStorageKernelOwnerDb(alloc, database, table_name, schema_json, indexes_json, backend_runtime, antfly_provider, secret_store, remote_content, installed) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn writeConfigureStorageKernelOwnerDbAtOpen(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, table_name: []const u8, schema_json: []const u8, indexes_json: []const u8, backend_runtime: ?*@import("db/mod.zig").background_runtime.BackendRuntime, antfly_provider: ?@import("../inference/managed_embedder.zig").AntflyProvider, secret_store: ?*@import("../common/secrets.zig").FileStore, remote_content: ?*const @import("write.zig").scraping.RemoteContentConfig, installed: ?*@import("write.zig").OwnerManagedConfig, historical_ordered_apply: bool) engine.Outcome(bool) {
    return .{ .value = @import("write.zig").configureStorageKernelOwnerDbAtOpen(alloc, database, table_name, schema_json, indexes_json, backend_runtime, antfly_provider, secret_store, remote_content, installed, historical_ordered_apply) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn writeOpenStorageKernelRestoreDb(alloc: std.mem.Allocator, path: []const u8, indexes_json: []const u8, lsm_root_generation: u64, backend_runtime: *@import("db/mod.zig").background_runtime.BackendRuntime, identity_namespace: ?@import("db/doc_identity.zig").Namespace, prepared_restore: *const @import("backup_restore.zig").PreparedRestore) engine.Outcome(@import("db/mod.zig").DB) {
    return .{ .value = @import("write.zig").openStorageKernelRestoreDb(alloc, path, indexes_json, lsm_root_generation, backend_runtime, identity_namespace, prepared_restore) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn writeRepairStorageKernelRestoreDb(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, group_id: u64, schema_json: []const u8, indexes_json: []const u8, cancellation: @import("db/mod.zig").types.CancellationToken) engine.Outcome(void) {
    return .{ .value = @import("write.zig").repairStorageKernelRestoreDb(alloc, database, group_id, schema_json, indexes_json, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn writeReconcileStorageKernelOwnerDb(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, table_name: []const u8, schema_json: []const u8, indexes_json: []const u8, target_index_name: ?[]const u8, advance_index_repair: bool, backend_runtime: ?*@import("db/mod.zig").background_runtime.BackendRuntime, antfly_provider: ?@import("../inference/managed_embedder.zig").AntflyProvider, installed: ?*@import("write.zig").OwnerManagedConfig) engine.Outcome(@import("../api/write_contract.zig").StorageKernelReconcileResult) {
    return .{ .value = @import("write.zig").reconcileStorageKernelOwnerDb(alloc, database, table_name, schema_json, indexes_json, target_index_name, advance_index_repair, backend_runtime, antfly_provider, installed) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn writeRepairStorageKernelOwnerDb(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, target_index_name: ?[]const u8, advance_index_repair: bool, options: @import("db/mod.zig").types.ArtifactRepairRunOptions) engine.Outcome(@import("../api/write_contract.zig").StorageKernelReconcileResult) {
    return .{ .value = @import("write.zig").repairStorageKernelOwnerDb(alloc, database, target_index_name, advance_index_repair, options) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn writeValidateProvisionedDbIdentityNamespaceWithPolicy(expected: ?@import("db/doc_identity.zig").Namespace, validation: @import("../api/write_contract.zig").StartupCatchUpMetadata.IdentityValidation, database: *const @import("db/mod.zig").DB) engine.Outcome(void) {
    return .{ .value = @import("write.zig").validateProvisionedDbIdentityNamespaceWithPolicy(expected, validation, database) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn writeBatchUsesDurableTransactionContract(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, req: @import("db/mod.zig").types.BatchRequest) engine.Outcome(bool) {
    return .{ .value = @import("write.zig").batchUsesDurableTransactionContract(alloc, database, req) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn writeTransactionUsesDurableContract(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, txn_id: @import("db/mod.zig").types.TxnId) engine.Outcome(bool) {
    return .{ .value = @import("write.zig").transactionUsesDurableContract(alloc, database, txn_id) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn writeApplyGraphMetricActionToDb(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, index_name: []const u8, metric_name: []const u8, action: []const u8) engine.Outcome(@import("db/mod.zig").types.GraphMetricStatus) {
    return .{ .value = @import("write.zig").applyGraphMetricActionToDb(alloc, database, index_name, metric_name, action) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn writeRunGraphMetricMaintenanceOrActionJsonAlloc(alloc: std.mem.Allocator, database: *@import("db/mod.zig").DB, body: []const u8) engine.Outcome([]u8) {
    return .{ .value = @import("write.zig").runGraphMetricMaintenanceOrActionJsonAlloc(alloc, database, body) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn writeReconfigureManagedDbEnrichmentRuntimePaused(ignored_allocator: std.mem.Allocator, database: *@import("db/mod.zig").DB, indexes_json: []const u8, backend_runtime: ?*@import("db/mod.zig").background_runtime.BackendRuntime, antfly_provider: ?@import("../inference/managed_embedder.zig").AntflyProvider, remote_capability_cache: ?*@import("antfly_inference_remote_capabilities").Cache, inference_api_url: ?[]const u8, source_table: []const u8, secret_store: ?*@import("../common/secrets.zig").FileStore, remote_content: ?*const @import("write.zig").scraping.RemoteContentConfig) engine.Outcome(void) {
    return .{ .value = @import("write.zig").reconfigureManagedDbEnrichmentRuntimePaused(ignored_allocator, database, indexes_json, backend_runtime, antfly_provider, remote_capability_cache, inference_api_url, source_table, secret_store, remote_content) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn writeExportPortableBackupFileWithSource(alloc: std.mem.Allocator, store: *@import("db/mod.zig").docstore.DocStore, path: []const u8, io: native.IoBorrow, sealed: ?@import("write.zig").PortableCohortSource) engine.Outcome(void) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = @import("write.zig").exportPortableBackupFileWithSource(alloc, store, path, received_io, sealed) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn managedDbEnrichmentSetDeinit(value: *@import("write.zig").ManagedDbEnrichmentSet, alloc: Allocator) engine.Outcome(void) {
    return .{ .value = value.deinit(alloc) };
}
pub fn nativeBackupSnapshotDeinit(value: *@import("write.zig").NativeBackupShardSnapshot, alloc: Allocator) engine.Outcome(void) {
    return .{ .value = value.deinit(alloc) };
}
pub fn writePrepareNativeBackupShardSnapshot(alloc: Allocator, database: *DB, path: []const u8, group: u64, plan: engine.BackupPlan) engine.Outcome(@import("write.zig").NativeBackupShardSnapshot) {
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

pub fn resolutionDeinit(self: *@import("db/resolution_runtime.zig").ResolutionRuntime) engine.Outcome(void) {
    return .{ .value = self.deinit() };
}

pub fn resolutionStart(self: *@import("db/resolution_runtime.zig").ResolutionRuntime) engine.Outcome(void) {
    return .{ .value = self.start() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn resolutionStop(self: *@import("db/resolution_runtime.zig").ResolutionRuntime) engine.Outcome(void) {
    return .{ .value = self.stop() };
}

pub fn resolutionCatchUp(self: *@import("db/resolution_runtime.zig").ResolutionRuntime) engine.Outcome(void) {
    return .{ .value = self.catchUp() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn resolutionCatchUpOnce(self: *@import("db/resolution_runtime.zig").ResolutionRuntime, deadline_ns: u64) engine.Outcome(void) {
    return .{ .value = self.catchUpOnce(deadline_ns) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn resolutionRequestReresolveBacklog(self: *@import("db/resolution_runtime.zig").ResolutionRuntime) engine.Outcome(void) {
    return .{ .value = self.requestReresolveBacklog() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn resolutionHasReresolveBacklog(self: *@import("db/resolution_runtime.zig").ResolutionRuntime) engine.Outcome(bool) {
    return .{ .value = self.hasReresolveBacklog() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn resolutionRunReresolveBacklogWindow(self: *@import("db/resolution_runtime.zig").ResolutionRuntime) engine.Outcome(@import("db/resolution_runtime.zig").ReresolveEnqueueResult) {
    return .{ .value = self.runReresolveBacklogWindow() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn resolutionPendingReviews(self: *@import("db/resolution_runtime.zig").ResolutionRuntime, alloc: std.mem.Allocator) engine.Outcome([]@import("db/resolution_runtime.zig").PendingReview) {
    return .{ .value = self.pendingReviews(alloc) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn promotionDeinit(self: *@import("db/promotion_runtime.zig").PromotionRuntime) engine.Outcome(void) {
    return .{ .value = self.deinit() };
}

pub fn promotionStart(self: *@import("db/promotion_runtime.zig").PromotionRuntime) engine.Outcome(void) {
    return .{ .value = self.start() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn promotionStop(self: *@import("db/promotion_runtime.zig").PromotionRuntime) engine.Outcome(void) {
    return .{ .value = self.stop() };
}

pub fn promotionCatchUp(self: *@import("db/promotion_runtime.zig").PromotionRuntime) engine.Outcome(void) {
    return .{ .value = self.catchUp() catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn pinnedSnapshotDeinit(self: *@import("db/core.zig").PinnedStoreSnapshot) engine.Outcome(void) {
    return .{ .value = self.deinit() };
}

pub fn pinnedSnapshotMaterializeWithSink(self: *@import("db/core.zig").PinnedStoreSnapshot, alloc: Allocator, io: native.IoBorrow, snapshot_root: []const u8, cancellation: types.CancellationToken, sink: ?@import("native_artifact_sink.zig").Sink) engine.Outcome(u64) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = self.materializeWithSink(alloc, received_io, snapshot_root, cancellation, sink) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn sourcePinLocate(database: *DB, scope: @import("db/online_source.zig").Scope) engine.Outcome(@import("db/online_source.zig").Located) {
    return .{ .value = @import("db/source_pin.zig").locate(database, scope) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn sourcePinEnsureAssumeApply(database: *DB, scope: @import("db/online_source.zig").Scope) engine.Outcome(void) {
    return .{ .value = @import("db/source_pin.zig").ensureAssumeApply(database, scope) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn sourcePinPublicationCertificateIfPresent(database: *DB, scope: @import("db/online_source.zig").Scope, cancellation: types.CancellationToken) engine.Outcome(?@import("source_snapshot.zig").Certificate) {
    return .{ .value = @import("db/source_pin.zig").publicationCertificateIfPresent(database, scope, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn sourcePinPreparePublication(database: *DB, scope: @import("db/online_source.zig").Scope, cancellation: types.CancellationToken) engine.Outcome(@import("source_snapshot.zig").Certificate) {
    return .{ .value = @import("db/source_pin.zig").preparePublication(database, scope, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn sourcePinReclaimReleased(database: *DB, scope: @import("db/online_source.zig").Scope) engine.Outcome(void) {
    return .{ .value = @import("db/source_pin.zig").reclaimReleased(database, scope) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn sourcePinReconcileReleasedWithBudget(database: *DB, budget: @import("db/source_pin_gc.zig").Budget) engine.Outcome(@import("db/source_pin_gc.zig").Work) {
    return .{ .value = @import("db/source_pin.zig").reconcileReleasedWithBudget(database, budget) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn restoreSnapshotToDeferredRuntimeRepairWithIoAndCancellation(staged_generation: *const @import("db/generation_lifecycle.zig").StagedGeneration, alloc: std.mem.Allocator, io: native.IoBorrow, snapshot_root: []const u8, path: []const u8, opts: db.OpenOptions, identity: db.RestoreIdentity, cancellation: types.CancellationToken) engine.Outcome(void) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = DB.restoreSnapshotToDeferredRuntimeRepairWithIoAndCancellation(staged_generation, alloc, received_io, snapshot_root, path, opts, identity, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn restoreMaterializedNativeGenerationToDeferredRuntimeRepairWithIoAndCancellation(staged_generation: *const @import("db/generation_lifecycle.zig").StagedGeneration, alloc: std.mem.Allocator, io: native.IoBorrow, primary_snapshot_root: []const u8, path: []const u8, opts: db.OpenOptions, identity: db.RestoreIdentity, native_generation: *@import("db/native/backup.zig").LoadedManifest, cancellation: types.CancellationToken) engine.Outcome(void) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = DB.restoreMaterializedNativeGenerationToDeferredRuntimeRepairWithIoAndCancellation(staged_generation, alloc, received_io, primary_snapshot_root, path, opts, identity, native_generation, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn restoreMaterializedNativePrimaryAndPlanProjectionsWithIoAndCancellation(staged_generation: *const @import("db/generation_lifecycle.zig").StagedGeneration, alloc: std.mem.Allocator, io: native.IoBorrow, primary_snapshot_root: []const u8, path: []const u8, opts: db.OpenOptions, identity: db.RestoreIdentity, native_generation: *@import("db/native/backup.zig").LoadedManifest, cancellation: types.CancellationToken) engine.Outcome(void) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = DB.restoreMaterializedNativePrimaryAndPlanProjectionsWithIoAndCancellation(staged_generation, alloc, received_io, primary_snapshot_root, path, opts, identity, native_generation, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn finishMaterializedNativeGenerationWithIo(staged_generation: *const @import("db/generation_lifecycle.zig").StagedGeneration, alloc: std.mem.Allocator, io: native.IoBorrow, path: []const u8, opts: db.OpenOptions, native_generation: *@import("db/native/backup.zig").LoadedManifest, complete_runtime_repair: bool) engine.Outcome(void) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = DB.finishMaterializedNativeGenerationWithIo(staged_generation, alloc, received_io, path, opts, native_generation, complete_runtime_repair) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn restoreSnapshotToLocalDeferredRuntimeRepairWithIoAndCancellation(staged_generation: *const @import("db/generation_lifecycle.zig").StagedGeneration, alloc: std.mem.Allocator, io: native.IoBorrow, snapshot_root: []const u8, path: []const u8, opts: db.OpenOptions, cancellation: types.CancellationToken) engine.Outcome(bool) {
    var receiver = io.receive() catch |err| return .{ .failure = .init(@errorName(err)) };
    const received_io = receiver.io();
    return .{ .value = DB.restoreSnapshotToLocalDeferredRuntimeRepairWithIoAndCancellation(staged_generation, alloc, received_io, snapshot_root, path, opts, cancellation) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

pub fn markRestorePrimaryRestoredForPathWithArtifactWithIo(alloc: std.mem.Allocator, io: native.IoBorrow, path: []const u8, backup_id: []const u8, location: []const u8, artifact_sha256: []const u8, snapshot_path: []const u8, group_id: u64) engine.Outcome(void) {
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
/// A VTable entry without a hand-written `pub` wrapper above forwards to the
/// same-named DB function and turns its error into an Outcome failure.
/// (`@hasDecl` only sees `pub` declarations.)
fn exportFor(comptime name: []const u8) *const anyopaque {
    if (@hasDecl(Exports, name)) return @ptrCast(&@field(Exports, name));
    return @ptrCast(forwarder(name));
}

fn forwarder(comptime name: []const u8) @FieldType(engine.VTable, name) {
    const Fn = @typeInfo(@FieldType(engine.VTable, name)).pointer.child;
    const info = @typeInfo(Fn).@"fn";
    const R = info.return_type.?;
    const T = info.param_types;
    const target = @field(DB, name);
    const S = struct {
        fn lift(result: anytype) R {
            if (comptime @typeInfo(@TypeOf(result)) == .error_union)
                return .{ .value = result catch |err| return .{ .failure = .init(@errorName(err)) } };
            return .{ .value = result };
        }
        fn f0() R {
            return lift(target());
        }
        fn f1(p0: T[0].?) R {
            return lift(target(p0));
        }
        fn f2(p0: T[0].?, p1: T[1].?) R {
            return lift(target(p0, p1));
        }
        fn f3(p0: T[0].?, p1: T[1].?, p2: T[2].?) R {
            return lift(target(p0, p1, p2));
        }
        fn f4(p0: T[0].?, p1: T[1].?, p2: T[2].?, p3: T[3].?) R {
            return lift(target(p0, p1, p2, p3));
        }
        fn f5(p0: T[0].?, p1: T[1].?, p2: T[2].?, p3: T[3].?, p4: T[4].?) R {
            return lift(target(p0, p1, p2, p3, p4));
        }
        fn f6(p0: T[0].?, p1: T[1].?, p2: T[2].?, p3: T[3].?, p4: T[4].?, p5: T[5].?) R {
            return lift(target(p0, p1, p2, p3, p4, p5));
        }
        fn f7(p0: T[0].?, p1: T[1].?, p2: T[2].?, p3: T[3].?, p4: T[4].?, p5: T[5].?, p6: T[6].?) R {
            return lift(target(p0, p1, p2, p3, p4, p5, p6));
        }
        fn f8(p0: T[0].?, p1: T[1].?, p2: T[2].?, p3: T[3].?, p4: T[4].?, p5: T[5].?, p6: T[6].?, p7: T[7].?) R {
            return lift(target(p0, p1, p2, p3, p4, p5, p6, p7));
        }
        fn f9(p0: T[0].?, p1: T[1].?, p2: T[2].?, p3: T[3].?, p4: T[4].?, p5: T[5].?, p6: T[6].?, p7: T[7].?, p8: T[8].?) R {
            return lift(target(p0, p1, p2, p3, p4, p5, p6, p7, p8));
        }
        fn f10(p0: T[0].?, p1: T[1].?, p2: T[2].?, p3: T[3].?, p4: T[4].?, p5: T[5].?, p6: T[6].?, p7: T[7].?, p8: T[8].?, p9: T[9].?) R {
            return lift(target(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9));
        }
        fn f11(p0: T[0].?, p1: T[1].?, p2: T[2].?, p3: T[3].?, p4: T[4].?, p5: T[5].?, p6: T[6].?, p7: T[7].?, p8: T[8].?, p9: T[9].?, p10: T[10].?) R {
            return lift(target(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10));
        }
        fn f12(p0: T[0].?, p1: T[1].?, p2: T[2].?, p3: T[3].?, p4: T[4].?, p5: T[5].?, p6: T[6].?, p7: T[7].?, p8: T[8].?, p9: T[9].?, p10: T[10].?, p11: T[11].?) R {
            return lift(target(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11));
        }
        fn f13(p0: T[0].?, p1: T[1].?, p2: T[2].?, p3: T[3].?, p4: T[4].?, p5: T[5].?, p6: T[6].?, p7: T[7].?, p8: T[8].?, p9: T[9].?, p10: T[10].?, p11: T[11].?, p12: T[12].?) R {
            return lift(target(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12));
        }
        fn f14(p0: T[0].?, p1: T[1].?, p2: T[2].?, p3: T[3].?, p4: T[4].?, p5: T[5].?, p6: T[6].?, p7: T[7].?, p8: T[8].?, p9: T[9].?, p10: T[10].?, p11: T[11].?, p12: T[12].?, p13: T[13].?) R {
            return lift(target(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13));
        }
    };
    const f = switch (T.len) {
        0 => S.f0,
        1 => S.f1,
        2 => S.f2,
        3 => S.f3,
        4 => S.f4,
        5 => S.f5,
        6 => S.f6,
        7 => S.f7,
        8 => S.f8,
        9 => S.f9,
        10 => S.f10,
        11 => S.f11,
        12 => S.f12,
        13 => S.f13,
        14 => S.f14,
        else => @compileError("engine method " ++ name ++ " needs a hand-written export"),
    };
    // The accessor erases the type, so prove the generated wrapper matches the VTable.
    comptime std.debug.assert(@TypeOf(f) == Fn);
    return &f;
}

comptime {
    for (@typeInfo(engine.VTable).@"struct".field_names) |name| {
        const Accessor = struct {
            fn get() callconv(.c) *const anyopaque {
                return exportFor(name);
            }
        };
        @export(&Accessor.get, .{ .name = "antfly_storage_engine_" ++ name ++ "_v28", .visibility = .hidden });
    }
    @export(&clearError, .{ .name = "antfly_storage_error_clear_v1", .visibility = .hidden });
    @export(&errorCode, .{ .name = "antfly_storage_error_code_v1", .visibility = .hidden });
    @export(&errorDetail, .{ .name = "antfly_storage_error_detail_v1", .visibility = .hidden });
    @export(&setError, .{ .name = "antfly_storage_error_set_v1", .visibility = .hidden });
    @export(&getAggregationDescriptor, .{ .name = "antfly_storage_engine_get_aggregation_descriptor_v1", .visibility = .hidden });
    @export(&getDescriptor, .{ .name = "antfly_storage_engine_get_descriptor_v28", .visibility = .hidden });
    @export(&getCAllocator, .{ .name = "antfly_storage_engine_c_allocator_v1", .visibility = .hidden });
}
