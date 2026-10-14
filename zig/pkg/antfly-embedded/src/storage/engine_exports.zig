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
const Exports = @This();
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

fn batchPrepared(self: *DB.PreparedBatchOwner, req: types.BatchRequest, profile: ?*BatchProfile, opts: BatchExecutionOptions, memo: *@import("db/execution_resources.zig").GeneratedEmbeddingMemo, allocator: *@import("db/execution_resources.zig").PreparedRowAllocator) engine.Outcome(void) {
    return .{ .value = self.batchInternalPrepared(req, profile, opts, memo, allocator) catch |err| return .{ .failure = .init(@errorName(err)) } };
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
        @export(&Accessor.get, .{ .name = "antfly_storage_engine_" ++ name ++ "_v4", .visibility = .hidden });
    }
    @export(&clearError, .{ .name = "antfly_storage_error_clear_v1", .visibility = .hidden });
    @export(&errorCode, .{ .name = "antfly_storage_error_code_v1", .visibility = .hidden });
    @export(&errorDetail, .{ .name = "antfly_storage_error_detail_v1", .visibility = .hidden });
    @export(&setError, .{ .name = "antfly_storage_error_set_v1", .visibility = .hidden });
    @export(&getDescriptor, .{ .name = "antfly_storage_engine_get_descriptor_v4", .visibility = .hidden });
}
