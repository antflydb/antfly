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

//! Private compiled local storage owner. Calls cover whole lifecycle, mutation,
//! query, schema, index and maintenance operations. Record loops stay in owner.
const std = @import("std");
pub const enabled = @import("engine_selection.zig").enabled;
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
const native = @import("antfly_runtime_abi").native_abi;
const callback = @import("antfly_runtime_abi").callback_abi;
pub const Failure = extern struct {
    ptr: ?[*]const u8 = null,
    len: usize = 0,
    pub fn init(name: []const u8) Failure {
        return .{ .ptr = name.ptr, .len = name.len };
    }
};
pub fn Outcome(comptime T: type) type {
    return struct { value: T = undefined, failure: Failure = .{} };
}
pub const VTable = struct {
    open: *const fn (Allocator, []const u8, OpenOptions) Outcome(DB),
    openOwned: *const fn (Allocator, []const u8, OpenOptions) Outcome(*DB),
    close: *const fn (*DB) Outcome(void),
    closeOwned: *const fn (*DB) Outcome(void),
    startResidentBackgroundWorkersIfNeeded: *const fn (*DB) Outcome(void),
    batchInternal: *const fn (*DB, types.BatchRequest, ?*BatchProfile, BatchExecutionOptions) Outcome(void),
    searchWithDenseProfile: *const fn (*DB, Allocator, types.SearchRequest) Outcome(SearchWithDenseProfileResult),
    searchWithCapturedRequestAndExecutionContext: *const fn (*DB, Allocator, types.SearchRequest, types.ExecutionContext) Outcome(SearchWithCapturedRequestResult),
    searchLease: *const fn (DB.QueryReadLease, Allocator, types.SearchRequest) Outcome(SearchWithDenseProfileResult),
    sync: *const fn (*DB, bool) Outcome(void),
    syncIndexes: *const fn (*DB, bool) Outcome(void),
    snapshot: *const fn (*DB, []const u8) Outcome(u64),
    snapshotWithMaintenanceDeadline: *const fn (*DB, []const u8, u64) Outcome(u64),
    snapshotNativeWithCancellation: *const fn (*DB, []const u8, types.CancellationToken) Outcome(u64),
    snapshotRelationalCohort: *const fn (*DB, []const u8, @import("db/relational_integrity_topology.zig").Fence, types.CancellationToken) Outcome(u64),
    setSchema: *const fn (*DB, schema_mod.TableSchema) Outcome(void),
    setSchemaJson: *const fn (*DB, Allocator, []const u8) Outcome(void),
    addIndex: *const fn (*DB, types.IndexConfig) Outcome(void),
    deleteIndex: *const fn (*DB, []const u8) Outcome(bool),
    runUntilIdle: *const fn (*DB) Outcome(void),
    runUntilIdleWithoutWaitingForEnrichmentRetriesWithCancellation: *const fn (*DB, types.CancellationToken) Outcome(void),
    runBackgroundMaintenanceWithCancellation: *const fn (*DB, types.CancellationToken) Outcome(bool),
    registry: *const fn () Outcome(*anyopaque),
};
pub const Boundary = callback.Boundary(VTable);
pub const FieldLayout = extern struct { name: [*:0]const u8, type_name: [*:0]const u8, offset: usize, size: usize, alignment: usize };
pub fn fieldsFor(comptime T: type) []const FieldLayout {
    const Holder = struct {
        const fields = blk: {
            const info = @typeInfo(T).@"struct";
            var value: [info.field_names.len]FieldLayout = undefined;
            for (info.field_names, info.field_types, 0..) |name, Field, index|
                value[index] = .{ .name = name ++ "", .type_name = @typeName(Field) ++ "", .offset = @offsetOf(T, name), .size = @sizeOf(Field), .alignment = @alignOf(Field) };
            break :blk value;
        };
    };
    return &Holder.fields;
}
pub const Descriptor = extern struct {
    version: u32 = 1,
    table_layout: native.TypeContract,
    db_layout: native.TypeContract,
    core_layout: native.TypeContract,
    handle_layout: native.TypeContract,
    registry_layout: native.TypeContract,
    db_fields: [*]const FieldLayout,
    db_field_count: usize,
    core_fields: [*]const FieldLayout,
    core_field_count: usize,
    dispatch: callback.CallbackDispatch,
    table: *const anyopaque,
};
pub const table_layout = blk: {
    @setEvalBranchQuota(100_000);
    break :blk native.TypeContract.of(VTable);
};
pub const db_layout = blk: {
    @setEvalBranchQuota(100_000);
    break :blk native.TypeContract.of(DB);
};
pub const core_layout = blk: {
    @setEvalBranchQuota(100_000);
    break :blk native.TypeContract.of(@typeInfo(@FieldType(DB, "core")).pointer.child);
};
pub const handle_layout = blk: {
    @setEvalBranchQuota(100_000);
    break :blk native.TypeContract.of(@import("../capi/handles.zig").Handle);
};
pub const registry_layout = blk: {
    @setEvalBranchQuota(100_000);
    break :blk native.TypeContract.of(@import("../capi/handles.zig").HandleRegistry);
};
extern fn antfly_storage_engine_get_function_table_v1() callconv(.c) *const Descriptor;
fn Result(comptime name: []const u8) type {
    const Function = @typeInfo(@FieldType(VTable, name)).pointer.child;
    const Wire = @typeInfo(Function).@"fn".return_type.?;
    return anyerror!@FieldType(Wire, "value");
}
pub fn call(comptime name: []const u8, args: anytype) Result(name) {
    const descriptor = antfly_storage_engine_get_function_table_v1();
    try validateDescriptor(descriptor);
    const table: *const VTable = @ptrCast(@alignCast(descriptor.table));
    const outcome = try Boundary.call(name, descriptor.dispatch, @field(table, name), args);
    if (outcome.failure.ptr) |ptr| return @import("storage_engine_errors").fromName(ptr[0..outcome.failure.len]);
    return outcome.value;
}

pub fn descriptorForValidation() *const Descriptor {
    return antfly_storage_engine_get_function_table_v1();
}
pub fn validateDescriptor(descriptor: *const Descriptor) !void {
    if (descriptor.version != 1 or !descriptor.table_layout.matches(table_layout) or
        !descriptor.db_layout.matches(db_layout) or !descriptor.core_layout.matches(core_layout) or
        !descriptor.handle_layout.matches(handle_layout) or !descriptor.registry_layout.matches(registry_layout))
        return error.UnsupportedVersion;
}

pub const db_fields = fieldsFor(DB);
pub const core_fields = fieldsFor(@typeInfo(@FieldType(DB, "core")).pointer.child);
