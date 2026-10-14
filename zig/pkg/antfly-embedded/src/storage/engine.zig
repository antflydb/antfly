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
    searchDenseProfiled: *const fn (self: *DB, alloc: Allocator, req: types.SearchRequest, dense: types.DenseKnnQuery) Outcome(@import("db/query/search_exec.zig").ProfiledDenseSearchResult),
    searchDenseProfiledWithCapturedRequest: *const fn (self: *DB, alloc: Allocator, req: types.SearchRequest, dense: types.DenseKnnQuery) Outcome(db.ProfiledDenseSearchWithCapturedRequestResult),
    searchTextKernel: *const fn (self: *DB, alloc: Allocator, index_name: []const u8, text_query: types.TextQuery, options: types.TextKernelSearchOptions) Outcome(types.TextKernelResult),
    preflightSearchRequestWithExecutionContext: *const fn (self: *DB, alloc: Allocator, req: types.SearchRequest, max_work: u32, exec_ctx: types.ExecutionContext) Outcome(@import("db/query/search_exec.zig").RuntimePreflightSummary),
    collectPlanningStatsWithExecutionContext: *const fn (self: *DB, alloc: Allocator, req: types.SearchRequest, max_work: u32, exec_ctx: types.ExecutionContext) Outcome(@import("db/planning_stats.zig").PlanningStatsSummary),
    collectSearchRequestTextStats: *const fn (self: *DB, alloc: Allocator, req: types.SearchRequest) Outcome([]const @import("../search/distributed_stats.zig").TextFieldStats),
    configureTableStorage: *const fn (self: *DB, settings: @import("../common/table_storage.zig").Settings) Outcome(void),
    batchPrepared: *const fn (*DB.PreparedBatchOwner, types.BatchRequest, ?*BatchProfile, BatchExecutionOptions, *@import("db/execution_resources.zig").GeneratedEmbeddingMemo, *@import("db/execution_resources.zig").PreparedRowAllocator) Outcome(void),
    reconfigureEnrichmentRuntime: *const fn (self: *DB, cfg: @import("db/enrichment/enrichment_runtime.zig").Config) Outcome(void),
    reconfigureEnrichmentRuntimePaused: *const fn (self: *DB, cfg: @import("db/enrichment/enrichment_runtime.zig").Config) Outcome(void),
    resumeEnrichmentRuntimeAfterReconfigure: *const fn (self: *DB, operation: []const u8, index_name: []const u8) Outcome(void),
    ensureTransactionRecoveryRuntime: *const fn (self: *DB, cfg: @import("db/maintenance/transaction_runtime.zig").Config) Outcome(void),
    activateResolverReplayRuntimes: *const fn (self: *DB) Outcome(void),
    get: *const fn (self: *DB, alloc: Allocator, key: []const u8) Outcome(?[]u8),
    getDocument: *const fn (self: *DB, alloc: Allocator, key: []const u8, opts: types.LookupOptions) Outcome(?types.LookupResult),
    lookup: *const fn (self: *DB, alloc: Allocator, key: []const u8, opts: types.LookupOptions) Outcome(?types.LookupResult),
    scan: *const fn (self: *DB, alloc: Allocator, from_key: []const u8, to_key: []const u8, opts: types.ScanOptions) Outcome(types.ScanResult),
    diagnosticStats: *const fn (self: *DB, alloc: Allocator) Outcome(types.DBStats),
    getSchemaJson: *const fn (self: *DB, alloc: Allocator) Outcome(?[]u8),
    primaryDocCount: *const fn (self: *DB, alloc: Allocator) Outcome(u64),
    getEnrichment: *const fn (self: *DB, alloc: Allocator, kind: types.EnrichmentKind, name: []const u8) Outcome(?types.EnrichmentConfig),
    runLsmMaintenanceStep: *const fn (self: *DB) Outcome(bool),
    constraintValidationStatus: *const fn (self: *DB) Outcome(DB.ConstraintValidationStatus),
    validateConstraintsStep: *const fn (self: *DB, budget: @import("db/relational_constraint_jobs.zig").Budget) Outcome(bool),
    retryConstraintValidation: *const fn (self: *DB, schema_version: u32) Outcome(bool),
    buildRelationalIndexStep: *const fn (self: *DB, name: []const u8, budget: DB.RelationalIndexBuildBudget) Outcome(void),
    collectRelationalIndexGarbageStep: *const fn (self: *DB) Outcome(bool),
    retryRelationalIndexBuild: *const fn (self: *DB, name: []const u8, generation: u64) Outcome(bool),
    relationalIndexBuildStatus: *const fn (self: *DB, name: []const u8) Outcome(DB.RelationalIndexBuildStatus),
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
    version: u32 = 4,
    table_layout: native.TypeContract,
    db_layout: native.TypeContract,
    core_layout: native.TypeContract,
    handle_layout: native.TypeContract,
    registry_layout: native.TypeContract,
    backend_runtime_layout: native.TypeContract,
    resource_manager_layout: native.TypeContract,
    prepared_owner_layout: native.TypeContract,
    prepared_allocator_layout: native.TypeContract,
    embedding_memo_layout: native.TypeContract,
    backend_fields: [*]const FieldLayout,
    backend_field_count: usize,
    resource_fields: [*]const FieldLayout,
    resource_field_count: usize,
    db_fields: [*]const FieldLayout,
    db_field_count: usize,
    core_fields: [*]const FieldLayout,
    core_field_count: usize,
    dispatch: callback.CallbackDispatch,
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
pub const backend_runtime_layout = blk: {
    @setEvalBranchQuota(100_000);
    break :blk native.TypeContract.of(@import("background_runtime.zig").BackendRuntime);
};
pub const resource_manager_layout = blk: {
    @setEvalBranchQuota(100_000);
    break :blk native.TypeContract.of(@import("resource_manager.zig").ResourceManager);
};
extern fn antfly_storage_engine_get_descriptor_v4() callconv(.c) *const Descriptor;
fn Result(comptime name: []const u8) type {
    const Function = @typeInfo(@FieldType(VTable, name)).pointer.child;
    const Wire = @typeInfo(Function).@"fn".return_type.?;
    return anyerror!@FieldType(Wire, "value");
}
pub fn call(comptime name: []const u8, args: anytype) Result(name) {
    const descriptor = antfly_storage_engine_get_descriptor_v4();
    try validateDescriptor(descriptor);
    // Individual hidden accessors let final links discard unused operations.
    // A global table would retain every physical operation in every consumer.
    const getter = @extern(*const fn () callconv(.c) *const anyopaque, .{ .name = "antfly_storage_engine_" ++ name ++ "_v4" });
    const method: @FieldType(VTable, name) = @ptrCast(@alignCast(getter()));
    const outcome = try Boundary.call(name, descriptor.dispatch, method, args);
    if (outcome.failure.ptr) |ptr| return @import("storage_engine_errors").fromName(ptr[0..outcome.failure.len]);
    return outcome.value;
}

pub fn descriptorForValidation() *const Descriptor {
    return antfly_storage_engine_get_descriptor_v4();
}
pub fn validateDescriptor(descriptor: *const Descriptor) !void {
    if (descriptor.version != 4 or !descriptor.table_layout.matches(table_layout) or
        !descriptor.db_layout.matches(db_layout) or !descriptor.core_layout.matches(core_layout) or
        !descriptor.handle_layout.matches(handle_layout) or !descriptor.registry_layout.matches(registry_layout) or
        !descriptor.backend_runtime_layout.matches(backend_runtime_layout) or
        !descriptor.resource_manager_layout.matches(resource_manager_layout) or
        !descriptor.prepared_owner_layout.matches(prepared_owner_layout) or
        !descriptor.prepared_allocator_layout.matches(prepared_allocator_layout) or
        !descriptor.embedding_memo_layout.matches(embedding_memo_layout))
        return error.UnsupportedVersion;
}

pub const db_fields = fieldsFor(DB);
pub const core_fields = fieldsFor(@typeInfo(@FieldType(DB, "core")).pointer.child);

pub const backend_fields = fieldsFor(@import("background_runtime.zig").BackendRuntime);
pub const resource_fields = fieldsFor(@import("resource_manager.zig").ResourceManager);

pub const prepared_owner_layout = blk: {
    @setEvalBranchQuota(100_000);
    break :blk native.TypeContract.of(DB.PreparedBatchOwner);
};
pub const prepared_allocator_layout = native.TypeContract.of(@import("db/execution_resources.zig").PreparedRowAllocator);
pub const embedding_memo_layout = native.TypeContract.of(@import("db/execution_resources.zig").GeneratedEmbeddingMemo);
