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
    return .{ .value = self.searchWithDenseProfile(alloc, req) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn searchWithCapturedRequestAndExecutionContext(
    self: *DB,
    alloc: Allocator,
    req: types.SearchRequest,
    exec_ctx: types.ExecutionContext,
) engine.Outcome(SearchWithCapturedRequestResult) {
    return .{ .value = self.searchWithCapturedRequestAndExecutionContext(alloc, req, exec_ctx) catch |err| return .{ .failure = .init(@errorName(err)) } };
}

fn searchLease(lease: DB.QueryReadLease, alloc: Allocator, req: types.SearchRequest) engine.Outcome(SearchWithDenseProfileResult) {
    return .{ .value = lease.search(alloc, req) catch |err| return .{ .failure = .init(@errorName(err)) } };
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
const table: engine.VTable = .{ .open = open, .openOwned = openOwned, .close = close, .closeOwned = closeOwned, .startResidentBackgroundWorkersIfNeeded = startResidentBackgroundWorkersIfNeeded, .batchInternal = batchInternal, .searchWithDenseProfile = searchWithDenseProfile, .searchWithCapturedRequestAndExecutionContext = searchWithCapturedRequestAndExecutionContext, .searchLease = searchLease, .sync = sync, .syncIndexes = syncIndexes, .snapshot = snapshot, .snapshotWithMaintenanceDeadline = snapshotWithMaintenanceDeadline, .snapshotNativeWithCancellation = snapshotNativeWithCancellation, .snapshotRelationalCohort = snapshotRelationalCohort, .setSchema = setSchema, .setSchemaJson = setSchemaJson, .addIndex = addIndex, .deleteIndex = deleteIndex, .runUntilIdle = runUntilIdle, .runUntilIdleWithoutWaitingForEnrichmentRetriesWithCancellation = runUntilIdleWithoutWaitingForEnrichmentRetriesWithCancellation, .runBackgroundMaintenanceWithCancellation = runBackgroundMaintenanceWithCancellation, .registry = registry };
const descriptor: engine.Descriptor = .{
    .table_layout = engine.table_layout,
    .db_layout = engine.db_layout,
    .core_layout = engine.core_layout,
    .handle_layout = engine.handle_layout,
    .registry_layout = engine.registry_layout,
    .db_fields = engine.db_fields.ptr,
    .db_field_count = engine.db_fields.len,
    .core_fields = engine.core_fields.ptr,
    .core_field_count = engine.core_fields.len,
    .dispatch = engine.Boundary.local_dispatch,
    .table = &table,
};
fn getFunctionTable() callconv(.c) *const engine.Descriptor {
    return &descriptor;
}
comptime {
    @export(&clearError, .{ .name = "antfly_storage_error_clear_v1", .visibility = .hidden });
    @export(&errorCode, .{ .name = "antfly_storage_error_code_v1", .visibility = .hidden });
    @export(&errorDetail, .{ .name = "antfly_storage_error_detail_v1", .visibility = .hidden });
    @export(&setError, .{ .name = "antfly_storage_error_set_v1", .visibility = .hidden });
    @export(&getFunctionTable, .{ .name = "antfly_storage_engine_get_function_table_v1", .visibility = .hidden });
}
