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

const std = @import("std");

const metadata_api = @import("../../metadata/api.zig");
const metadata_table_manager = @import("../../metadata/table_manager.zig");
const metadata_transition_state = @import("../../metadata/transition_state.zig");
const db_mod = @import("../../storage/db/mod.zig");
const storage_schema = @import("../../storage/schema.zig");
const document_sql_runtime = @import("document_sql_runtime.zig");
const sql_adapter_runtime = @import("../../sql/runtime.zig");
const raft_mod = @import("../../raft/mod.zig");
const raft_reconciler = @import("../../raft/reconciler.zig");
const schema_api = @import("../../schema/mod.zig");
const table_catalog = @import("../table_catalog.zig");
const catalog_resources = @import("../catalog_resources.zig");
const relational_rows_api = @import("../../sql/relational_rows.zig");
const query_api = @import("../query.zig");
const core = @import("core.zig");
const document_sql = @import("document_sql.zig");

const TableReadSource = core.TableReadSource;
const LookupResponse = core.LookupResponse;
const ScanResponse = core.ScanResponse;
const appendScanLine = core.appendScanLine;
const nativeCatalogTableNameAlloc = table_catalog.nativeTableNameForCatalogTargetAlloc;

pub const LoweredSqlReadPlanResult = union(enum) {
    query: db_mod.types.RelationalRowsQueryResult,
    document_query: db_mod.types.RelationalRowsQueryResult,
    set_operation: db_mod.types.RelationalRowsQueryResult,
    recursive_cte: db_mod.types.RelationalRowsQueryResult,
    aggregate: db_mod.types.RelationalRowsAggregateResult,
    window: db_mod.types.RelationalRowsWindowResult,
    join: db_mod.types.RelationalRowsJoinResult,
    lateral: db_mod.types.RelationalRowsJoinResult,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .query => |*result| result.deinit(alloc),
            .document_query => |*result| result.deinit(alloc),
            .set_operation => |*result| result.deinit(alloc),
            .recursive_cte => |*result| result.deinit(alloc),
            .aggregate => |*result| result.deinit(alloc),
            .window => |*result| result.deinit(alloc),
            .join => |*result| result.deinit(alloc),
            .lateral => |*result| result.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const LoweredRelationPopulationRowsResult = struct {
    mode: sql_adapter_runtime.RelationPopulationMode,
    target_table_name: []const u8,
    rows: [][]const u8 = &.{},
    total: u32 = 0,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.target_table_name));
        for (self.rows) |row| alloc.free(@constCast(row));
        if (self.rows.len > 0) alloc.free(self.rows);
        self.* = undefined;
    }
};

pub const TakenLoweredSqlReadRows = struct {
    rows: [][]const u8,
    total: u32,
};

pub fn takeLoweredSqlReadRows(result: *LoweredSqlReadPlanResult) TakenLoweredSqlReadRows {
    return switch (result.*) {
        .query => |*query| blk: {
            const rows = query.rows;
            query.rows = &.{};
            break :blk .{ .rows = rows, .total = query.total };
        },
        .document_query => |*query| blk: {
            const rows = query.rows;
            query.rows = &.{};
            break :blk .{ .rows = rows, .total = query.total };
        },
        .set_operation => |*query| blk: {
            const rows = query.rows;
            query.rows = &.{};
            break :blk .{ .rows = rows, .total = query.total };
        },
        .recursive_cte => |*query| blk: {
            const rows = query.rows;
            query.rows = &.{};
            break :blk .{ .rows = rows, .total = query.total };
        },
        .aggregate => |*aggregate| blk: {
            const rows = aggregate.rows;
            aggregate.rows = &.{};
            break :blk .{ .rows = rows, .total = aggregate.total_groups };
        },
        .window => |*window| blk: {
            const rows = window.rows;
            window.rows = &.{};
            break :blk .{ .rows = rows, .total = window.total_rows };
        },
        .join => |*join| blk: {
            const rows = join.rows;
            join.rows = &.{};
            break :blk .{ .rows = rows, .total = join.total_rows };
        },
        .lateral => |*lateral| blk: {
            const rows = lateral.rows;
            lateral.rows = &.{};
            break :blk .{ .rows = rows, .total = lateral.total_rows };
        },
    };
}

pub fn resolveSingleUniqueOwnerGroup(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    constraint_name: []const u8,
    encoded_value: []const u8,
) !u64 {
    var owner = try table_catalog.resolveUniqueConstraintOwnerGroups(alloc, catalog, table_name, constraint_name, encoded_value);
    defer owner.deinit(alloc);
    if (!owner.configured or owner.groups.len != 1) return error.UniqueOwnerTopologyUnavailable;
    return owner.groups[0];
}

test "relational unique owner lookup requires one active owner range" {
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
                    .name = "users",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .unique_constraint_ranges = @constCast((&[_]metadata_table_manager.UniqueConstraintRangeRecord{
                    .{
                        .table_id = 7,
                        .constraint_name = "users_email_key",
                        .start_encoded_value = "",
                        .end_encoded_value = "m",
                        .group_id = 7101,
                    },
                    .{
                        .table_id = 7,
                        .constraint_name = "users_email_key",
                        .start_encoded_value = "m",
                        .end_encoded_value = null,
                        .group_id = 7102,
                    },
                    .{
                        .table_id = 7,
                        .constraint_name = "users_phone_key",
                        .start_encoded_value = "",
                        .end_encoded_value = null,
                        .group_id = 7201,
                        .state = metadata_table_manager.unique_constraint_range_rebuilding,
                    },
                    .{
                        .table_id = 7,
                        .constraint_name = "users_handle_key",
                        .start_encoded_value = "",
                        .end_encoded_value = null,
                        .group_id = 7202,
                        .state = metadata_table_manager.unique_constraint_range_splitting,
                    },
                    .{
                        .table_id = 7,
                        .constraint_name = "users_username_key",
                        .start_encoded_value = "",
                        .end_encoded_value = null,
                        .group_id = 7203,
                        .state = metadata_table_manager.unique_constraint_range_merging,
                    },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    try std.testing.expectEqual(@as(u64, 7101), try resolveSingleUniqueOwnerGroup(std.testing.allocator, FakeCatalog.iface(), "users", "users_email_key", "a"));
    try std.testing.expectEqual(@as(u64, 7102), try resolveSingleUniqueOwnerGroup(std.testing.allocator, FakeCatalog.iface(), "users", "users_email_key", "z"));
    try std.testing.expectError(error.UniqueOwnerTopologyUnavailable, resolveSingleUniqueOwnerGroup(std.testing.allocator, FakeCatalog.iface(), "users", "users_phone_key", "p"));
    try std.testing.expectError(error.UniqueOwnerTopologyUnavailable, resolveSingleUniqueOwnerGroup(std.testing.allocator, FakeCatalog.iface(), "users", "users_handle_key", "h"));
    try std.testing.expectError(error.UniqueOwnerTopologyUnavailable, resolveSingleUniqueOwnerGroup(std.testing.allocator, FakeCatalog.iface(), "users", "users_username_key", "u"));
    try std.testing.expectError(error.UniqueOwnerTopologyUnavailable, resolveSingleUniqueOwnerGroup(std.testing.allocator, FakeCatalog.iface(), "users", "missing_key", "a"));
}

pub fn relationalUniqueOwnerKeyAlloc(
    alloc: std.mem.Allocator,
    constraint_name: []const u8,
    encoded_value: []const u8,
) ![]u8 {
    return try db_mod.internal_keys.relationalUniqueKeyAlloc(alloc, constraint_name, encoded_value);
}

pub fn lookupRelationalUniqueOwnerInDb(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    group_id: u64,
    reads: raft_mod.FeatureReads,
    constraint_name: []const u8,
    encoded_value: []const u8,
    consistency: raft_mod.ReadConsistency,
) !?[]u8 {
    const key = try relationalUniqueOwnerKeyAlloc(alloc, constraint_name, encoded_value);
    defer alloc.free(key);
    reads.prepareLookupWithConsistency(group_id, key, .{}, consistency) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    return db.getRawStoreValue(alloc, key) catch |err| switch (err) {
        error.NotFound => null,
        else => err,
    };
}

pub fn lookupRelationalTemporalUniqueOwnerInDb(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    group_id: u64,
    reads: raft_mod.FeatureReads,
    constraint_name: []const u8,
    encoded_value: []const u8,
    encoded_point: []const u8,
    consistency: raft_mod.ReadConsistency,
) !?[]u8 {
    const prefix = try db_mod.internal_keys.relationalTemporalUniquePrefixAlloc(alloc, constraint_name, encoded_value);
    defer alloc.free(prefix);
    const upper = try db_mod.internal_keys.relationalTemporalUniquePrefixUpperAlloc(alloc, constraint_name, encoded_value);
    defer if (upper) |buf| alloc.free(buf);
    reads.prepareScanWithConsistency(group_id, prefix, if (upper) |buf| buf else "", .{}, consistency) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    return try db.lookupRelationalTemporalUniqueOwner(alloc, constraint_name, encoded_value, encoded_point);
}

pub fn lookupRelationalTemporalUniqueOverlapOwnerInDb(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    group_id: u64,
    reads: raft_mod.FeatureReads,
    constraint_name: []const u8,
    encoded_value: []const u8,
    encoded_start: []const u8,
    encoded_end: []const u8,
    consistency: raft_mod.ReadConsistency,
) !?[]u8 {
    const prefix = try db_mod.internal_keys.relationalTemporalUniquePrefixAlloc(alloc, constraint_name, encoded_value);
    defer alloc.free(prefix);
    const upper = try db_mod.internal_keys.relationalTemporalUniquePrefixUpperAlloc(alloc, constraint_name, encoded_value);
    defer if (upper) |buf| alloc.free(buf);
    reads.prepareScanWithConsistency(group_id, prefix, if (upper) |buf| buf else "", .{}, consistency) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    return try db.lookupRelationalTemporalUniqueOverlapOwner(alloc, constraint_name, encoded_value, encoded_start, encoded_end);
}

pub const RecursiveCteMaterializedRows = struct {
    rows: []const []const u8 = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.rows) |row| alloc.free(@constCast(row));
        if (self.rows.len > 0) alloc.free(self.rows);
        self.* = undefined;
    }
};

pub fn executeLoweredSqlReadPlanAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    catalog: table_catalog.CatalogSource,
    default_table_name: []const u8,
    default_schema: storage_schema.TableSchema,
    plan: sql_adapter_runtime.LoweredReadPlan,
    consistency: raft_mod.ReadConsistency,
) !?LoweredSqlReadPlanResult {
    return try executeLoweredSqlReadPlanWithSessionAlloc(
        alloc,
        source,
        catalog,
        catalog_resources.SqlCatalogSession.default(),
        default_table_name,
        default_schema,
        plan,
        consistency,
    );
}

pub fn executeLoweredSqlReadPlanWithSessionAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
    default_table_name: []const u8,
    default_schema: storage_schema.TableSchema,
    plan: sql_adapter_runtime.LoweredReadPlan,
    consistency: raft_mod.ReadConsistency,
) !?LoweredSqlReadPlanResult {
    return switch (plan) {
        .query => |lowered| blk: {
            const owned_schema = try catalogRuntimeSchemaUnlessDefaultAlloc(alloc, catalog, default_table_name, lowered.table_name);
            defer if (owned_schema) |schema| storage_schema.freeSchema(alloc, schema);
            const runtime_schema = owned_schema orelse default_schema;
            var result = if (lowered.system_time_as_of_sequence) |commit_sequence| result_blk: {
                const target = try catalogTargetForLoweredSqlTable(session, default_table_name, lowered.table_name);
                break :result_blk (try source.rowsQueryPlanCatalogSystemTimeAsOfSequence(alloc, target, runtime_schema, commit_sequence, lowered.plan, consistency)) orelse break :blk null;
            } else result_blk: {
                break :result_blk (try source.rowsQueryPlan(alloc, lowered.table_name, runtime_schema, lowered.plan, consistency)) orelse break :blk null;
            };
            errdefer result.deinit(alloc);
            break :blk .{ .query = result };
        },
        .document_query => |lowered| blk: {
            const target = try catalogTargetForLoweredSqlTable(session, default_table_name, lowered.table_name);
            const native_table_name = try nativeCatalogTableNameAlloc(alloc, catalog, target);
            defer alloc.free(native_table_name);
            var adapter = document_sql.RuntimeSourceAdapter{
                .source = source,
                .target = target,
                .native_table_name = native_table_name,
                .public_table_name = target.table_name,
            };
            var runtime_result = (try document_sql_runtime.executeReadPlanAlloc(
                alloc,
                adapter.runtimeSource(),
                lowered,
                consistency,
            )) orelse break :blk null;
            var runtime_result_transferred = false;
            errdefer if (!runtime_result_transferred) runtime_result.deinit(alloc);
            var result = db_mod.types.RelationalRowsQueryResult{
                .rows = runtime_result.rows,
                .total = runtime_result.total,
            };
            runtime_result_transferred = true;
            errdefer result.deinit(alloc);
            break :blk .{ .document_query = result };
        },
        .document_aggregate => |lowered| blk: {
            const target = try catalogTargetForLoweredSqlTable(session, default_table_name, lowered.table_name);
            const native_table_name = try nativeCatalogTableNameAlloc(alloc, catalog, target);
            defer alloc.free(native_table_name);
            var adapter = document_sql.RuntimeSourceAdapter{
                .source = source,
                .target = target,
                .native_table_name = native_table_name,
                .public_table_name = target.table_name,
            };
            var runtime_result = (try document_sql_runtime.executeAggregatePlanAlloc(
                alloc,
                adapter.runtimeSource(),
                lowered,
                consistency,
            )) orelse break :blk null;
            var runtime_result_transferred = false;
            errdefer if (!runtime_result_transferred) runtime_result.deinit(alloc);
            var result = db_mod.types.RelationalRowsAggregateResult{
                .rows = runtime_result.rows,
                .total_groups = runtime_result.total_groups,
            };
            runtime_result_transferred = true;
            errdefer result.deinit(alloc);
            break :blk .{ .aggregate = result };
        },
        .set_operation => |lowered| blk: {
            var result = (try executeLoweredSqlSetOperationPlanAlloc(
                alloc,
                source,
                catalog,
                session,
                default_table_name,
                default_schema,
                lowered,
                consistency,
            )) orelse break :blk null;
            errdefer result.deinit(alloc);
            break :blk .{ .set_operation = result };
        },
        .recursive_cte => |lowered| blk: {
            var result = (try executeLoweredRecursiveCtePlanAlloc(
                alloc,
                source,
                catalog,
                session,
                default_table_name,
                default_schema,
                lowered,
                consistency,
            )) orelse break :blk null;
            errdefer result.deinit(alloc);
            break :blk .{ .recursive_cte = result };
        },
        .aggregate => |lowered| blk: {
            const owned_schema = try catalogRuntimeSchemaUnlessDefaultAlloc(alloc, catalog, default_table_name, lowered.table_name);
            defer if (owned_schema) |schema| storage_schema.freeSchema(alloc, schema);
            const runtime_schema = owned_schema orelse default_schema;
            var result = (try source.rowsAggregatePlan(alloc, lowered.table_name, runtime_schema, lowered.plan, consistency)) orelse break :blk null;
            errdefer result.deinit(alloc);
            break :blk .{ .aggregate = result };
        },
        .window => |lowered| blk: {
            const owned_schema = try catalogRuntimeSchemaUnlessDefaultAlloc(alloc, catalog, default_table_name, lowered.table_name);
            defer if (owned_schema) |schema| storage_schema.freeSchema(alloc, schema);
            const runtime_schema = owned_schema orelse default_schema;
            var result = (try source.rowsWindowPlan(alloc, lowered.table_name, runtime_schema, lowered.plan, consistency)) orelse break :blk null;
            errdefer result.deinit(alloc);
            break :blk .{ .window = result };
        },
        .join => |lowered| blk: {
            const cte_table_name = try loweredReadJoinCteTableName(
                default_table_name,
                lowered.left_table_name,
                lowered.right_table_name,
                lowered.ctes.len,
                lowered.join.left.source_cte,
                lowered.join.right.source_cte,
            );
            const owned_cte_schema = try catalogRuntimeSchemaUnlessDefaultAlloc(alloc, catalog, default_table_name, cte_table_name);
            defer if (owned_cte_schema) |schema| storage_schema.freeSchema(alloc, schema);
            const owned_left_schema = try catalogRuntimeSchemaUnlessDefaultAlloc(alloc, catalog, default_table_name, lowered.left_table_name);
            defer if (owned_left_schema) |schema| storage_schema.freeSchema(alloc, schema);
            const owned_right_schema = try catalogRuntimeSchemaUnlessDefaultAlloc(alloc, catalog, default_table_name, lowered.right_table_name);
            defer if (owned_right_schema) |schema| storage_schema.freeSchema(alloc, schema);

            const cte_schema = owned_cte_schema orelse default_schema;
            const left_schema = owned_left_schema orelse default_schema;
            const right_schema = owned_right_schema orelse default_schema;
            const same_table = std.mem.eql(u8, cte_table_name, lowered.left_table_name) and
                std.mem.eql(u8, cte_table_name, lowered.right_table_name);
            const join_plan = lowered.asPlan();
            var result = (try (if (same_table)
                source.rowsJoinPlan(alloc, cte_table_name, cte_schema, join_plan, consistency)
            else
                rowsJoinPlanFromRoutedScansWithSchemasAlloc(alloc, source, cte_table_name, lowered.left_table_name, lowered.right_table_name, cte_schema, left_schema, right_schema, join_plan, consistency))) orelse break :blk null;
            errdefer result.deinit(alloc);
            break :blk .{ .join = result };
        },
        .lateral => |lowered| blk: {
            const cte_table_name = try loweredReadJoinCteTableName(
                default_table_name,
                lowered.left_table_name,
                lowered.right_table_name,
                lowered.plan.ctes.len,
                lowered.plan.lateral.left.source_cte,
                lowered.plan.lateral.right.source_cte,
            );
            const owned_cte_schema = try catalogRuntimeSchemaUnlessDefaultAlloc(alloc, catalog, default_table_name, cte_table_name);
            defer if (owned_cte_schema) |schema| storage_schema.freeSchema(alloc, schema);
            const owned_left_schema = try catalogRuntimeSchemaUnlessDefaultAlloc(alloc, catalog, default_table_name, lowered.left_table_name);
            defer if (owned_left_schema) |schema| storage_schema.freeSchema(alloc, schema);
            const owned_right_schema = try catalogRuntimeSchemaUnlessDefaultAlloc(alloc, catalog, default_table_name, lowered.right_table_name);
            defer if (owned_right_schema) |schema| storage_schema.freeSchema(alloc, schema);

            const cte_schema = owned_cte_schema orelse default_schema;
            const left_schema = owned_left_schema orelse default_schema;
            const right_schema = owned_right_schema orelse default_schema;
            const same_table = std.mem.eql(u8, cte_table_name, lowered.left_table_name) and
                std.mem.eql(u8, cte_table_name, lowered.right_table_name);
            var result = (try (if (same_table)
                source.rowsLateralPlan(alloc, cte_table_name, cte_schema, lowered.plan, consistency)
            else
                rowsLateralPlanFromRoutedScansWithSchemasAlloc(alloc, source, cte_table_name, lowered.left_table_name, lowered.right_table_name, cte_schema, left_schema, right_schema, lowered.plan, consistency))) orelse break :blk null;
            errdefer result.deinit(alloc);
            break :blk .{ .lateral = result };
        },
    };
}

pub fn executeLoweredRelationPopulationPlanAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    catalog: table_catalog.CatalogSource,
    default_table_name: []const u8,
    default_schema: storage_schema.TableSchema,
    plan: sql_adapter_runtime.LoweredRelationPopulationPlan,
    consistency: raft_mod.ReadConsistency,
) !?LoweredRelationPopulationRowsResult {
    if (!plan.populate) {
        const target_table_name = try alloc.dupe(u8, plan.target_table_name);
        return .{
            .mode = plan.mode,
            .target_table_name = target_table_name,
        };
    }

    var read_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        source,
        catalog,
        default_table_name,
        default_schema,
        plan.source,
        consistency,
    )) orelse return null;
    defer read_result.deinit(alloc);

    const extracted = takeLoweredSqlReadRows(&read_result);
    errdefer {
        for (extracted.rows) |row| alloc.free(@constCast(row));
        if (extracted.rows.len > 0) alloc.free(extracted.rows);
    }
    const target_table_name = try alloc.dupe(u8, plan.target_table_name);
    return .{
        .mode = plan.mode,
        .target_table_name = target_table_name,
        .rows = extracted.rows,
        .total = extracted.total,
    };
}

fn executeLoweredSqlSetOperationPlanAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
    default_table_name: []const u8,
    default_schema: storage_schema.TableSchema,
    lowered: sql_adapter_runtime.LoweredSetOperationPlan,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsQueryResult {
    const owned_left_schema = try catalogRuntimeSchemaUnlessDefaultAlloc(alloc, catalog, default_table_name, lowered.left.table_name);
    defer if (owned_left_schema) |schema| storage_schema.freeSchema(alloc, schema);
    const owned_right_schema = try catalogRuntimeSchemaUnlessDefaultAlloc(alloc, catalog, default_table_name, lowered.right.table_name);
    defer if (owned_right_schema) |schema| storage_schema.freeSchema(alloc, schema);
    const left_schema = owned_left_schema orelse default_schema;
    const right_schema = owned_right_schema orelse default_schema;

    const operation = loweredSetOperationToRowsOperation(lowered.operation);
    const same_table = std.mem.eql(u8, lowered.left.table_name, lowered.right.table_name);
    if (same_table) {
        const target = try catalogTargetForLoweredSqlTable(session, default_table_name, lowered.left.table_name);
        return try source.rowsSetOperationPlanCatalog(alloc, target, left_schema, .{
            .operation = operation,
            .left = lowered.left.plan,
            .right = lowered.right.plan,
            .order_by = lowered.order_by,
            .limit = lowered.limit,
            .offset = lowered.offset,
            .max_rows = lowered.max_rows,
            .max_bytes = lowered.max_bytes,
            .spill_after_bytes = lowered.spill_after_bytes,
        }, consistency);
    }

    const left_target = try catalogTargetForLoweredSqlTable(session, default_table_name, lowered.left.table_name);
    const right_target = try catalogTargetForLoweredSqlTable(session, default_table_name, lowered.right.table_name);

    var left = (try source.rowsQueryPlanCatalog(alloc, left_target, left_schema, lowered.left.plan, consistency)) orelse return null;
    defer left.deinit(alloc);
    var right = (try source.rowsQueryPlanCatalog(alloc, right_target, right_schema, lowered.right.plan, consistency)) orelse return null;
    defer right.deinit(alloc);
    return try executeSetOperationOnQueryResultsAlloc(alloc, .{
        .operation = operation,
        .left = lowered.left.plan,
        .right = lowered.right.plan,
        .order_by = lowered.order_by,
        .limit = lowered.limit,
        .offset = lowered.offset,
        .max_rows = lowered.max_rows,
        .max_bytes = lowered.max_bytes,
        .spill_after_bytes = lowered.spill_after_bytes,
    }, left.rows, right.rows);
}

test "lowered sql insert source plans build batches from routed scans" {
    const alloc = std.testing.allocator;
    const target_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status_key":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const source_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;

    var parsed_target = try schema_api.parseValidatedTableSchema(alloc, target_schema_json);
    defer parsed_target.deinit(alloc);
    const target_schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_target);
    defer storage_schema.freeSchema(alloc, target_schema);

    var parsed_source = try schema_api.parseValidatedTableSchema(alloc, source_schema_json);
    defer parsed_source.deinit(alloc);
    const source_schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_source);
    defer storage_schema.freeSchema(alloc, source_schema);

    const FakeCatalog = struct {
        tables: [2]metadata_table_manager.TableRecord = .{
            .{ .table_id = 7, .name = "order_copies", .schema_json = target_schema_json, .placement_role = "data" },
            .{ .table_id = 8, .name = "orders", .schema_json = source_schema_json, .placement_role = "data" },
        },

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
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = self.tables[0..],
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeRoutedSource = struct {
        scan_calls: usize = 0,

        fn source(self: *@This()) TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                },
            };
        }

        fn lookup(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            return null;
        }

        fn query(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.SearchRequest,
            _: raft_mod.ReadConsistency,
        ) !?query_api.QueryResponse {
            return null;
        }

        fn scan(
            ptr: *anyopaque,
            scan_alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: db_mod.types.ScanOptions,
            _: raft_mod.ReadConsistency,
        ) !?ScanResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(opts.include_documents);
            try std.testing.expect(opts.include_all_fields);
            try std.testing.expectEqualStrings("", from_key);
            try std.testing.expectEqualStrings("", to_key);
            if (!std.mem.eql(u8, table_name, "orders")) return error.TableNotFound;
            self.scan_calls += 1;
            const ndjson =
                "{\"key\":\"o1\",\"id\":\"o1\",\"status\":\"OPEN\",\"amount\":10}\n" ++
                "{\"key\":\"o2\",\"id\":\"o2\",\"status\":\"closed\",\"amount\":5}\n" ++
                "{\"key\":\"o3\",\"id\":\"o3\",\"status\":\"OPEN\",\"amount\":7}\n";
            return .{ .ndjson = try scan_alloc.dupe(u8, ndjson) };
        }
    };

    const NoConflictResolver = struct {
        fn resolver() relational_rows_api.UniqueSelectorResolver {
            return .{
                .ptr = undefined,
                .resolve = resolve,
                .resolve_primary = resolvePrimary,
            };
        }

        fn resolve(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
        ) !?[]u8 {
            return null;
        }

        fn resolvePrimary(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
        ) !bool {
            return false;
        }
    };

    var catalog = FakeCatalog{};
    var fake = FakeRoutedSource{};
    var lowered = try sql_adapter_runtime.lowerWritePlanWithCatalogAlloc(
        alloc,
        "WITH open_orders AS (SELECT id, status, amount FROM orders WHERE status = 'OPEN') INSERT INTO order_copies (id, status_key, amount) SELECT id || '_copy' AS id, lower(status), amount FROM open_orders ORDER BY id ASC RETURNING id, status_key, amount",
        target_schema,
        &.{},
        .{ .unique_resolver = NoConflictResolver.resolver(), .sync_level = .full_index },
        catalog.iface(),
    );
    defer lowered.deinit(alloc);

    switch (lowered) {
        .insert_source => |insert_source| {
            try std.testing.expectEqualStrings("order_copies", insert_source.table_name);
            try std.testing.expectEqualStrings("orders", insert_source.insert_source.req.source_table);
            try std.testing.expectEqual(@as(usize, 1), insert_source.ctes.len);
            const plan: db_mod.types.RelationalRowsInsertSourcePlan = .{
                .ctes = insert_source.ctes,
                .insert_source = insert_source.insert_source.req,
                .sync_level = insert_source.sync_level,
            };
            var batch = (try rowsInsertSourcePlanBatchFromRoutedScansWithSchemasAlloc(
                alloc,
                fake.source(),
                insert_source.table_name,
                insert_source.insert_source.req.source_table,
                target_schema,
                source_schema,
                plan,
                .read_index,
                NoConflictResolver.resolver(),
            )).?;
            defer batch.deinit(alloc);

            try std.testing.expectEqual(@as(usize, 1), fake.scan_calls);
            try std.testing.expectEqual(db_mod.types.SyncLevel.full_index, insert_source.sync_level);
            try std.testing.expectEqual(db_mod.types.SyncLevel.full_index, batch.req.sync_level);
            try std.testing.expectEqual(@as(u32, 2), batch.inserted);
            try std.testing.expectEqual(@as(usize, 2), batch.returning_rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"o1_copy\",\"status_key\":\"open\",\"amount\":10}", batch.returning_rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"o3_copy\",\"status_key\":\"open\",\"amount\":7}", batch.returning_rows[1]);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "lowered sql merge mutation plans build batches from routed scans" {
    const alloc = std.testing.allocator;
    const target_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"organization_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const source_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"target_id":{"type":"keyword"},"status":{"type":"keyword"},"organization_id":{"type":"keyword"}},"required":["id","target_id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;

    var parsed_target = try schema_api.parseValidatedTableSchema(alloc, target_schema_json);
    defer parsed_target.deinit(alloc);
    const target_schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_target);
    defer storage_schema.freeSchema(alloc, target_schema);

    var parsed_source = try schema_api.parseValidatedTableSchema(alloc, source_schema_json);
    defer parsed_source.deinit(alloc);
    const source_schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_source);
    defer storage_schema.freeSchema(alloc, source_schema);
    const target_row_json = "{\"id\":\"t1\",\"status\":\"open\",\"organization_id\":\"org:1\"}";
    const target_key = try relational_rows_api.physicalPrimaryKeyFromRowJsonAlloc(alloc, target_schema, target_row_json);
    defer alloc.free(target_key);

    const FakeCatalog = struct {
        tables: [2]metadata_table_manager.TableRecord = .{
            .{ .table_id = 21, .name = "orders", .schema_json = target_schema_json, .placement_role = "data" },
            .{ .table_id = 22, .name = "imports", .schema_json = source_schema_json, .placement_role = "data" },
        },

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
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = self.tables[0..],
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeRoutedSource = struct {
        target_key: []const u8,
        target_row_json: []const u8,
        scan_calls: usize = 0,
        lookup_calls: usize = 0,

        fn source(self: *@This()) TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                },
            };
        }

        fn lookup(
            ptr: *anyopaque,
            lookup_alloc: std.mem.Allocator,
            table_name: []const u8,
            key: []const u8,
            opts: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(opts.include_all_fields);
            if (!std.mem.eql(u8, table_name, "orders")) return null;
            if (!std.mem.eql(u8, key, self.target_key)) return null;
            self.lookup_calls += 1;
            return .{
                .json = try lookup_alloc.dupe(u8, self.target_row_json),
                .version = 17,
            };
        }

        fn query(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.SearchRequest,
            _: raft_mod.ReadConsistency,
        ) !?query_api.QueryResponse {
            return null;
        }

        fn scan(
            ptr: *anyopaque,
            scan_alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: db_mod.types.ScanOptions,
            _: raft_mod.ReadConsistency,
        ) !?ScanResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(opts.include_documents);
            try std.testing.expect(opts.include_all_fields);
            try std.testing.expectEqualStrings("", from_key);
            try std.testing.expectEqualStrings("", to_key);
            self.scan_calls += 1;
            const ndjson = if (std.mem.eql(u8, table_name, "orders"))
                try std.fmt.allocPrint(scan_alloc, "{{\"key\":{f},\"id\":\"t1\",\"status\":\"open\",\"organization_id\":\"org:1\"}}\n", .{std.json.fmt(self.target_key, .{})})
            else if (std.mem.eql(u8, table_name, "imports"))
                try scan_alloc.dupe(u8, "{\"key\":\"import:s1\",\"id\":\"s1\",\"target_id\":\"t1\",\"status\":\"UPDATED\",\"organization_id\":\"org:1\"}\n{\"key\":\"import:s2\",\"id\":\"s2\",\"target_id\":\"new1\",\"status\":\"inserted\",\"organization_id\":\"org:2\"}\n")
            else if (std.mem.eql(u8, table_name, "oversized_imports")) blk: {
                var out = std.ArrayListUnmanaged(u8).empty;
                errdefer out.deinit(scan_alloc);
                const line = "{\"key\":\"import:s1\",\"id\":\"s1\",\"target_id\":\"t1\",\"status\":\"UPDATED\",\"organization_id\":\"org:1\"}\n";
                var index: usize = 0;
                while (index <= db_mod.types.default_relational_rows_cte_max_rows) : (index += 1) {
                    try out.appendSlice(scan_alloc, line);
                }
                break :blk try out.toOwnedSlice(scan_alloc);
            } else return error.TableNotFound;
            return .{ .ndjson = ndjson };
        }
    };

    var catalog = FakeCatalog{};
    var lowered = try sql_adapter_runtime.lowerWritePlanWithCatalogAlloc(
        alloc,
        "MERGE INTO orders AS target USING imports AS source ON target.id = source.target_id WHEN MATCHED THEN UPDATE SET status = lower(source.status) WHEN NOT MATCHED THEN INSERT (id, status, organization_id) VALUES (source.target_id, upper(source.status), source.organization_id) RETURNING id, status, lower(status) AS status_key",
        target_schema,
        &.{},
        .{},
        catalog.iface(),
    );
    defer lowered.deinit(alloc);

    var fake = FakeRoutedSource{ .target_key = target_key, .target_row_json = target_row_json };
    switch (lowered) {
        .merge_mutation => |merge| {
            var batch = (try rowsMergeMutationBatchFromRoutedScansWithSchemasAlloc(
                alloc,
                fake.source(),
                "orders",
                "imports",
                target_schema,
                source_schema,
                merge,
                .{ .select_all = true },
                &.{},
                .{ .select_all = true },
                &.{},
                .read_index,
            )).?;
            defer batch.deinit(alloc);

            try std.testing.expectEqual(@as(usize, 2), fake.scan_calls);
            try std.testing.expectEqual(@as(usize, 1), fake.lookup_calls);
            try std.testing.expectEqual(@as(u32, 1), batch.inserted);
            try std.testing.expectEqual(@as(u32, 1), batch.transformed);
            var saw_target_version_predicate = false;
            for (batch.predicates) |predicate| {
                if (!std.mem.eql(u8, predicate.key, target_key)) continue;
                try std.testing.expectEqual(@as(u64, 17), predicate.expected_version);
                saw_target_version_predicate = true;
            }
            try std.testing.expect(saw_target_version_predicate);
            try std.testing.expectEqual(@as(usize, 2), batch.returning_rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"t1\",\"status\":\"updated\",\"status_key\":\"updated\"}", batch.returning_rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"new1\",\"status\":\"INSERTED\",\"status_key\":\"inserted\"}", batch.returning_rows[1]);

            try std.testing.expectError(error.UnsupportedRowsQuery, rowsMergeMutationBatchFromRoutedScansWithSchemasAlloc(
                alloc,
                fake.source(),
                "orders",
                "oversized_imports",
                target_schema,
                source_schema,
                merge,
                .{ .select_all = true },
                &.{},
                .{ .select_all = true },
                &.{},
                .read_index,
            ));
        },
        else => return error.TestUnexpectedResult,
    }
}

test "lowered sql recursive merge mutation plans build batches from routed scans" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"organization_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer storage_schema.freeSchema(alloc, schema);

    const path = "/tmp/antfly-api-recursive-merge-routed-scans";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(alloc, schema_json, .{});
    const row_a = "{\"id\":\"a\",\"organization_id\":\"root\",\"status\":\"ROOT\"}";
    const row_b = "{\"id\":\"b\",\"organization_id\":\"a\",\"status\":\"CHILD\"}";
    const row_c = "{\"id\":\"c\",\"organization_id\":\"other\",\"status\":\"OTHER\"}";
    const key_a = try relational_rows_api.physicalPrimaryKeyFromRowJsonAlloc(alloc, schema, row_a);
    defer alloc.free(key_a);
    const key_b = try relational_rows_api.physicalPrimaryKeyFromRowJsonAlloc(alloc, schema, row_b);
    defer alloc.free(key_b);
    const key_c = try relational_rows_api.physicalPrimaryKeyFromRowJsonAlloc(alloc, schema, row_c);
    defer alloc.free(key_c);
    try db.batch(.{
        .writes = &.{
            .{ .key = key_a, .value = row_a },
            .{ .key = key_b, .value = row_b },
            .{ .key = key_c, .value = row_c },
        },
        .sync_level = .write,
    });

    const FakeCatalog = struct {
        tables: [1]metadata_table_manager.TableRecord = .{
            .{ .table_id = 23, .name = "usage_records", .schema_json = schema_json, .placement_role = "data" },
        },

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
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = self.tables[0..],
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeReadSource = struct {
        db: *db_mod.DB,
        scan_calls: usize = 0,
        lookup_calls: usize = 0,
        query_plan_calls: usize = 0,
        expected_database_name: []const u8 = catalog_resources.default_database_name,
        expected_namespace_name: []const u8 = catalog_resources.default_namespace_name,

        fn source(self: *@This()) TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .rows_query_plan_catalog = rowsQueryPlanCatalog,
                },
            };
        }

        fn lookup(
            ptr: *anyopaque,
            lookup_alloc: std.mem.Allocator,
            table_name: []const u8,
            key: []const u8,
            opts: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!std.mem.eql(u8, table_name, "usage_records")) return null;
            self.lookup_calls += 1;
            var result = (try self.db.lookup(lookup_alloc, key, opts)) orelse return null;
            defer result.deinit(lookup_alloc);
            return .{
                .json = try lookup_alloc.dupe(u8, result.json),
                .version = try self.db.getTimestamp(lookup_alloc, key),
            };
        }

        fn scan(
            ptr: *anyopaque,
            scan_alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: db_mod.types.ScanOptions,
            _: raft_mod.ReadConsistency,
        ) !?ScanResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!std.mem.eql(u8, table_name, "usage_records")) return null;
            self.scan_calls += 1;
            var result = try self.db.scan(scan_alloc, from_key, to_key, opts);
            defer result.deinit(scan_alloc);
            var out = std.ArrayListUnmanaged(u8).empty;
            errdefer out.deinit(scan_alloc);
            for (result.hashes, 0..) |entry, i| {
                const json = if (opts.include_documents) result.documents[i].json else null;
                try appendScanLine(scan_alloc, &out, entry.id, json);
            }
            return .{ .ndjson = try out.toOwnedSlice(scan_alloc) };
        }

        fn query(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.SearchRequest,
            _: raft_mod.ReadConsistency,
        ) !?query_api.QueryResponse {
            return error.UnsupportedOperation;
        }

        fn rowsQueryPlanCatalog(
            ptr: *anyopaque,
            query_alloc: std.mem.Allocator,
            target: catalog_resources.TableTarget,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsQueryPlan,
            _: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsQueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings(self.expected_database_name, target.database_name);
            try std.testing.expectEqualStrings(self.expected_namespace_name, target.namespace_name);
            try std.testing.expectEqualStrings("usage_records", target.table_name);
            self.query_plan_calls += 1;
            return try self.db.queryRelationalRowsPlan(query_alloc, runtime_schema, plan);
        }
    };

    var lowered = try sql_adapter_runtime.lowerWritePlanAlloc(
        alloc,
        "WITH RECURSIVE source_rows AS (SELECT id, status FROM usage_records WHERE organization_id = 'root' UNION ALL SELECT child.id, child.status FROM usage_records AS child JOIN source_rows AS parent ON child.organization_id = parent.id) MERGE INTO usage_records AS target USING source_rows AS source ON target.id = source.id WHEN MATCHED THEN UPDATE SET status = lower(source.status) RETURNING target.id, target.status",
        schema,
        &.{},
        .{},
    );
    defer lowered.deinit(alloc);

    var catalog = FakeCatalog{};
    var read_source = FakeReadSource{ .db = &db };
    switch (lowered) {
        .recursive_merge_mutation => |recursive_merge| {
            var batch = (try rowsRecursiveMergeMutationBatchFromRoutedScansWithSchemasAlloc(
                alloc,
                read_source.source(),
                catalog.iface(),
                "usage_records",
                "usage_records",
                schema,
                schema,
                recursive_merge,
                .{ .select_all = true },
                &.{},
                .read_index,
            )).?;
            defer batch.deinit(alloc);

            try std.testing.expectEqual(@as(usize, 1), read_source.scan_calls);
            try std.testing.expectEqual(@as(usize, 2), read_source.query_plan_calls);
            try std.testing.expectEqual(@as(usize, 3), read_source.lookup_calls);
            try std.testing.expectEqual(@as(u32, 2), batch.transformed);
            try std.testing.expectEqual(@as(usize, 2), batch.returning_rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"a\",\"status\":\"root\"}", batch.returning_rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"b\",\"status\":\"child\"}", batch.returning_rows[1]);

            var tenant_read_source = FakeReadSource{
                .db = &db,
                .expected_database_name = "tenant_ops",
                .expected_namespace_name = "analytics",
            };
            const tenant_session: catalog_resources.SqlCatalogSession = .{
                .current_database_name = "tenant_ops",
                .search_path = &.{ "analytics", "public" },
            };
            var tenant_batch = (try rowsRecursiveMergeMutationBatchFromRoutedScansWithSchemasAndSessionAlloc(
                alloc,
                tenant_read_source.source(),
                catalog.iface(),
                tenant_session,
                "usage_records",
                "usage_records",
                schema,
                schema,
                recursive_merge,
                .{ .select_all = true },
                &.{},
                .read_index,
            )).?;
            defer tenant_batch.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 2), tenant_read_source.query_plan_calls);
            try std.testing.expectEqual(@as(u32, 2), tenant_batch.transformed);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "lowered sql recursive cte plans execute bounded materialization" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"parent_id":{"type":"keyword"},"depth":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;

    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer storage_schema.freeSchema(alloc, schema);

    const FakeCatalog = struct {
        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeSource = struct {
        calls: usize = 0,
        expected_database_name: []const u8 = catalog_resources.default_database_name,
        expected_namespace_name: []const u8 = catalog_resources.default_namespace_name,

        fn source(self: *@This()) TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .rows_query_plan_catalog = rowsQueryPlanCatalog,
                },
            };
        }

        fn lookup(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            return null;
        }

        fn query(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.SearchRequest,
            _: raft_mod.ReadConsistency,
        ) !?query_api.QueryResponse {
            return null;
        }

        fn scan(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.ScanOptions,
            _: raft_mod.ReadConsistency,
        ) !?ScanResponse {
            return null;
        }

        fn rowsQueryPlanCatalog(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            target: catalog_resources.TableTarget,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsQueryPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsQueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(raft_mod.ReadConsistency.read_index, consistency);
            try std.testing.expectEqualStrings(self.expected_database_name, target.database_name);
            try std.testing.expectEqualStrings(self.expected_namespace_name, target.namespace_name);
            try std.testing.expectEqualStrings("nodes", target.table_name);
            self.calls += 1;
            const rows = [_][]const u8{
                "{\"id\":\"a\",\"parent_id\":\"root\",\"depth\":1}",
                "{\"id\":\"b\",\"parent_id\":\"root\",\"depth\":1}",
                "{\"id\":\"c\",\"parent_id\":\"a\",\"depth\":0}",
                "{\"id\":\"d\",\"parent_id\":\"c\",\"depth\":0}",
                "{\"id\":\"outside\",\"parent_id\":\"other\",\"depth\":0}",
            };
            return try relational_rows_api.executeRowsQueryPlanOnJsonRowsAlloc(plan_alloc, runtime_schema, plan, rows[0..]);
        }
    };

    var lowered = try sql_adapter_runtime.lowerReadPlanAlloc(
        alloc,
        "WITH RECURSIVE walk(id, depth) AS (SELECT id, depth FROM nodes WHERE parent_id = 'root' UNION ALL SELECT nodes.id, walk.depth + 1 FROM nodes JOIN walk ON nodes.parent_id = walk.id) SELECT id FROM walk WHERE depth > 1 ORDER BY id",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);
    switch (lowered) {
        .recursive_cte => |recursive| {
            try std.testing.expectEqualStrings("walk", recursive.final_query.source_cte);
            try std.testing.expectEqual(@as(usize, 1), recursive.final_query.select.len);
            try std.testing.expectEqualStrings("id", recursive.final_query.select[0]);
            try std.testing.expectEqual(@as(usize, 1), recursive.final_query.predicates.len);
            try std.testing.expectEqualStrings("depth", recursive.final_query.predicates[0].field);
        },
        else => return error.TestUnexpectedResult,
    }

    var catalog = FakeCatalog{};
    var materialized_fake = FakeSource{};
    var materialized = (try materializeLoweredRecursiveCteRowsAlloc(
        alloc,
        materialized_fake.source(),
        catalog.iface(),
        "nodes",
        schema,
        switch (lowered) {
            .recursive_cte => |recursive| recursive,
            else => return error.TestUnexpectedResult,
        },
        .read_index,
    )) orelse return error.TestUnexpectedResult;
    defer materialized.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), materialized_fake.calls);
    try std.testing.expectEqual(@as(usize, 4), materialized.rows.len);

    const tenant_session: catalog_resources.SqlCatalogSession = .{
        .current_database_name = "tenant_ops",
        .search_path = &.{ "analytics", "public" },
    };
    var tenant_materialized_fake = FakeSource{
        .expected_database_name = "tenant_ops",
        .expected_namespace_name = "analytics",
    };
    var tenant_materialized = (try materializeLoweredRecursiveCteRowsWithSessionAlloc(
        alloc,
        tenant_materialized_fake.source(),
        catalog.iface(),
        tenant_session,
        "nodes",
        schema,
        switch (lowered) {
            .recursive_cte => |recursive| recursive,
            else => return error.TestUnexpectedResult,
        },
        .read_index,
    )) orelse return error.TestUnexpectedResult;
    defer tenant_materialized.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), tenant_materialized_fake.calls);
    try std.testing.expectEqual(@as(usize, 4), tenant_materialized.rows.len);

    var fake = FakeSource{};
    var result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        fake.source(),
        catalog.iface(),
        "nodes",
        schema,
        lowered,
        .read_index,
    )).?;
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    switch (result) {
        .recursive_cte => |query_result| {
            try std.testing.expectEqual(@as(u32, 2), query_result.total);
            try std.testing.expectEqual(@as(usize, 2), query_result.rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"c\"}", query_result.rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"d\"}", query_result.rows[1]);
        },
        else => return error.TestUnexpectedResult,
    }

    var tenant_fake = FakeSource{
        .expected_database_name = "tenant_ops",
        .expected_namespace_name = "analytics",
    };
    var tenant_result = (try executeLoweredSqlReadPlanWithSessionAlloc(
        alloc,
        tenant_fake.source(),
        catalog.iface(),
        tenant_session,
        "nodes",
        schema,
        lowered,
        .read_index,
    )).?;
    defer tenant_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), tenant_fake.calls);
    switch (tenant_result) {
        .recursive_cte => |query_result| {
            try std.testing.expectEqual(@as(u32, 2), query_result.total);
            try std.testing.expectEqual(@as(usize, 2), query_result.rows.len);
        },
        else => return error.TestUnexpectedResult,
    }

    const recursive = switch (lowered) {
        .recursive_cte => |plan| plan,
        else => return error.TestUnexpectedResult,
    };
    const assignments = [_]db_mod.types.RelationalRowsExpressionAssignment{
        .{ .field = "id", .expression = .{ .kind = .field, .field = "id" } },
        .{ .field = "depth", .expression = .{ .kind = .field, .field = "depth" } },
    };
    const returning = [_][]const u8{ "id", "depth" };
    var insert_fake = FakeSource{};
    var batch = (try rowsInsertSourceBatchFromRecursiveCtePlanAlloc(
        alloc,
        insert_fake.source(),
        catalog.iface(),
        "nodes",
        schema,
        "walk_copies",
        schema,
        recursive,
        .{
            .source_table = "nodes",
            .source = recursive.final_query,
            .assignments = assignments[0..],
            .returning = returning[0..],
        },
        .read_index,
        null,
    )) orelse return error.TestUnexpectedResult;
    defer batch.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), insert_fake.calls);
    try std.testing.expectEqual(@as(u32, 2), batch.inserted);
    try std.testing.expectEqual(@as(usize, 2), batch.returning_rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"c\",\"depth\":2}", batch.returning_rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"d\",\"depth\":3}", batch.returning_rows[1]);

    var tenant_insert_fake = FakeSource{
        .expected_database_name = "tenant_ops",
        .expected_namespace_name = "analytics",
    };
    var tenant_batch = (try rowsInsertSourceBatchFromRecursiveCtePlanWithSessionAlloc(
        alloc,
        tenant_insert_fake.source(),
        catalog.iface(),
        tenant_session,
        "nodes",
        schema,
        "walk_copies",
        schema,
        recursive,
        .{
            .source_table = "nodes",
            .source = recursive.final_query,
            .assignments = assignments[0..],
            .returning = returning[0..],
        },
        .read_index,
        null,
    )) orelse return error.TestUnexpectedResult;
    defer tenant_batch.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), tenant_insert_fake.calls);
    try std.testing.expectEqual(@as(u32, 2), tenant_batch.inserted);
}

test "lowered relation population plans execute routed typed read sources" {
    const alloc = std.testing.allocator;
    const orders_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"customer_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const customers_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"name":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;

    var parsed_orders = try schema_api.parseValidatedTableSchema(alloc, orders_schema_json);
    defer parsed_orders.deinit(alloc);
    const orders_schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_orders);
    defer storage_schema.freeSchema(alloc, orders_schema);

    var parsed_customers = try schema_api.parseValidatedTableSchema(alloc, customers_schema_json);
    defer parsed_customers.deinit(alloc);
    const customers_schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_customers);
    defer storage_schema.freeSchema(alloc, customers_schema);

    const FakeCatalog = struct {
        tables: [2]metadata_table_manager.TableRecord = .{
            .{ .table_id = 7, .name = "orders", .schema_json = orders_schema_json, .placement_role = "data" },
            .{ .table_id = 8, .name = "customers", .schema_json = customers_schema_json, .placement_role = "data" },
        },

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
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = self.tables[0..],
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeRoutedSource = struct {
        scan_calls: usize = 0,

        fn source(self: *@This()) TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                },
            };
        }

        fn lookup(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            return null;
        }

        fn query(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.SearchRequest,
            _: raft_mod.ReadConsistency,
        ) !?query_api.QueryResponse {
            return null;
        }

        fn scan(
            ptr: *anyopaque,
            scan_alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: db_mod.types.ScanOptions,
            _: raft_mod.ReadConsistency,
        ) !?ScanResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(opts.include_documents);
            try std.testing.expect(opts.include_all_fields);
            try std.testing.expectEqualStrings("", from_key);
            try std.testing.expectEqualStrings("", to_key);
            self.scan_calls += 1;
            const ndjson = if (std.mem.eql(u8, table_name, "orders"))
                "{\"key\":\"o1\",\"id\":\"o1\",\"status\":\"open\",\"customer_id\":\"c1\"}\n{\"key\":\"o2\",\"id\":\"o2\",\"status\":\"closed\",\"customer_id\":\"c2\"}\n"
            else if (std.mem.eql(u8, table_name, "customers"))
                "{\"key\":\"c1\",\"id\":\"c1\",\"status\":\"open\",\"name\":\"Ada\"}\n{\"key\":\"c2\",\"id\":\"c2\",\"status\":\"closed\",\"name\":\"Grace\"}\n"
            else
                return error.TableNotFound;
            return .{ .ndjson = try scan_alloc.dupe(u8, ndjson) };
        }
    };

    var catalog = FakeCatalog{};
    var fake = FakeRoutedSource{};
    var lowered = try sql_adapter_runtime.lowerRelationPopulationPlanWithCatalogAlloc(
        alloc,
        "CREATE TABLE order_archive AS SELECT o.id AS order_id, c.name AS customer_name FROM orders AS o LEFT JOIN customers AS c ON o.status = c.status ORDER BY order_id ASC",
        orders_schema,
        &.{},
        catalog.iface(),
    );
    defer lowered.deinit(alloc);

    var result = (try executeLoweredRelationPopulationPlanAlloc(
        alloc,
        fake.source(),
        catalog.iface(),
        "orders",
        orders_schema,
        lowered,
        .read_index,
    )).?;
    defer result.deinit(alloc);

    try std.testing.expectEqual(sql_adapter_runtime.RelationPopulationMode.create_table_as, result.mode);
    try std.testing.expectEqualStrings("order_archive", result.target_table_name);
    try std.testing.expectEqual(@as(usize, 2), fake.scan_calls);
    try std.testing.expectEqual(@as(u32, 2), result.total);
    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expectEqualStrings("{\"order_id\":\"o1\",\"customer_name\":\"Ada\"}", result.rows[0]);
    try std.testing.expectEqualStrings("{\"order_id\":\"o2\",\"customer_name\":\"Grace\"}", result.rows[1]);

    var lowered_no_data = try sql_adapter_runtime.lowerRelationPopulationPlanWithCatalogAlloc(
        alloc,
        "CREATE TABLE order_archive_empty AS SELECT o.id AS order_id, c.name AS customer_name FROM orders AS o LEFT JOIN customers AS c ON o.status = c.status WITH NO DATA",
        orders_schema,
        &.{},
        catalog.iface(),
    );
    defer lowered_no_data.deinit(alloc);
    try std.testing.expect(!lowered_no_data.populate);

    var no_data_result = (try executeLoweredRelationPopulationPlanAlloc(
        alloc,
        fake.source(),
        catalog.iface(),
        "orders",
        orders_schema,
        lowered_no_data,
        .read_index,
    )).?;
    defer no_data_result.deinit(alloc);

    try std.testing.expectEqualStrings("order_archive_empty", no_data_result.target_table_name);
    try std.testing.expectEqual(@as(usize, 2), fake.scan_calls);
    try std.testing.expectEqual(@as(u32, 0), no_data_result.total);
    try std.testing.expectEqual(@as(usize, 0), no_data_result.rows.len);
}

test "lowered relation population row transfer empties source result" {
    const alloc = std.testing.allocator;
    const rows = try alloc.alloc([]const u8, 2);
    rows[0] = try alloc.dupe(u8, "{\"id\":1}");
    rows[1] = try alloc.dupe(u8, "{\"id\":2}");

    var result = LoweredSqlReadPlanResult{
        .query = .{
            .rows = rows,
            .total = 2,
        },
    };
    defer result.deinit(alloc);

    const taken = takeLoweredSqlReadRows(&result);
    defer {
        for (taken.rows) |row| alloc.free(@constCast(row));
        alloc.free(taken.rows);
    }

    try std.testing.expectEqual(@as(u32, 2), taken.total);
    try std.testing.expectEqual(@as(usize, 2), taken.rows.len);
    try std.testing.expectEqual(@as(usize, 0), result.query.rows.len);
}

pub fn executeLoweredRecursiveCtePlanAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
    default_table_name: []const u8,
    default_schema: storage_schema.TableSchema,
    lowered: sql_adapter_runtime.LoweredRecursiveCtePlan,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsQueryResult {
    var materialized = (try materializeLoweredRecursiveCteRowsWithSessionAlloc(
        alloc,
        source,
        catalog,
        session,
        default_table_name,
        default_schema,
        lowered,
        consistency,
    )) orelse return null;
    defer materialized.deinit(alloc);

    var cte_schema = default_schema;
    cte_schema.relational_columns = lowered.output_columns;
    cte_schema.primary_key = null;
    var final_query = lowered.final_query;
    final_query.source_cte = "";
    return try relational_rows_api.executeRowsQueryOnJsonRowsAlloc(alloc, cte_schema, final_query, materialized.rows);
}

pub fn materializeLoweredRecursiveCteRowsAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    catalog: table_catalog.CatalogSource,
    default_table_name: []const u8,
    default_schema: storage_schema.TableSchema,
    lowered: sql_adapter_runtime.LoweredRecursiveCtePlan,
    consistency: raft_mod.ReadConsistency,
) !?RecursiveCteMaterializedRows {
    return try materializeLoweredRecursiveCteRowsWithSessionAlloc(
        alloc,
        source,
        catalog,
        catalog_resources.SqlCatalogSession.default(),
        default_table_name,
        default_schema,
        lowered,
        consistency,
    );
}

pub fn materializeLoweredRecursiveCteRowsWithSessionAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
    default_table_name: []const u8,
    default_schema: storage_schema.TableSchema,
    lowered: sql_adapter_runtime.LoweredRecursiveCtePlan,
    consistency: raft_mod.ReadConsistency,
) !?RecursiveCteMaterializedRows {
    if (lowered.output_columns.len == 0) return error.UnsupportedRowsQuery;
    const join_member = switch (lowered.recursive_member) {
        .join => |join| join,
    };
    const left_is_cte = std.mem.eql(u8, join_member.left_table_name, lowered.cte_name);
    const right_is_cte = std.mem.eql(u8, join_member.right_table_name, lowered.cte_name);
    if (left_is_cte == right_is_cte or join_member.join_type != .inner) return error.UnsupportedRowsQuery;
    const base_table_name = if (left_is_cte) join_member.right_table_name else join_member.left_table_name;

    const owned_anchor_schema = try catalogRuntimeSchemaUnlessDefaultAlloc(alloc, catalog, default_table_name, lowered.anchor.table_name);
    defer if (owned_anchor_schema) |schema| storage_schema.freeSchema(alloc, schema);
    const owned_base_schema = try catalogRuntimeSchemaUnlessDefaultAlloc(alloc, catalog, default_table_name, base_table_name);
    defer if (owned_base_schema) |schema| storage_schema.freeSchema(alloc, schema);
    const anchor_schema = owned_anchor_schema orelse default_schema;
    const base_schema = owned_base_schema orelse default_schema;

    const anchor_target = try catalogTargetForLoweredSqlTable(session, default_table_name, lowered.anchor.table_name);
    var anchor = (try source.rowsQueryPlanCatalog(alloc, anchor_target, anchor_schema, lowered.anchor.plan, consistency)) orelse return null;
    defer anchor.deinit(alloc);

    const base_target = try catalogTargetForLoweredSqlTable(session, default_table_name, base_table_name);
    var base = (try source.rowsQueryPlanCatalog(alloc, base_target, base_schema, .{ .query = .{ .select_all = true } }, consistency)) orelse return null;
    defer base.deinit(alloc);

    var materialized = std.ArrayListUnmanaged([]const u8).empty;
    errdefer deinitRecursiveCteRows(alloc, &materialized);
    var frontier = std.ArrayListUnmanaged([]const u8).empty;
    defer frontier.deinit(alloc);
    var seen = std.StringHashMap(void).init(alloc);
    defer seen.deinit();
    const distinct = lowered.operation == .union_distinct;

    for (anchor.rows) |row| {
        const owned = try alloc.dupe(u8, row);
        errdefer alloc.free(owned);
        if (distinct) {
            if (seen.contains(owned)) {
                alloc.free(owned);
                continue;
            }
            try seen.put(owned, {});
        }
        try materialized.append(alloc, owned);
        try frontier.append(alloc, owned);
    }
    try admitRecursiveCteRows(lowered, materialized.items);

    while (frontier.items.len != 0) {
        var next_frontier = std.ArrayListUnmanaged([]const u8).empty;
        errdefer next_frontier.deinit(alloc);
        for (base.rows) |base_row| {
            for (frontier.items) |cte_row| {
                if (!try recursiveCteJoinRowsMatchAlloc(alloc, base_row, cte_row, join_member, !left_is_cte)) continue;
                const projected = try recursiveCteProjectedRowJsonAlloc(alloc, base_row, cte_row, join_member.projections);
                errdefer alloc.free(projected);
                if (distinct) {
                    if (seen.contains(projected)) {
                        alloc.free(projected);
                        continue;
                    }
                    try seen.put(projected, {});
                }
                try materialized.append(alloc, projected);
                try next_frontier.append(alloc, projected);
                try admitRecursiveCteRows(lowered, materialized.items);
            }
        }
        frontier.deinit(alloc);
        frontier = next_frontier;
    }

    return .{ .rows = try materialized.toOwnedSlice(alloc) };
}

fn deinitRecursiveCteRows(alloc: std.mem.Allocator, rows: *std.ArrayListUnmanaged([]const u8)) void {
    for (rows.items) |row| alloc.free(@constCast(row));
    rows.deinit(alloc);
}

fn admitRecursiveCteRows(
    lowered: sql_adapter_runtime.LoweredRecursiveCtePlan,
    rows: []const []const u8,
) !void {
    const materialized_bytes = db_mod.types.relationalRowsCteMaterializedJsonBytes(rows) orelse return error.UnsupportedRowsQuery;
    const cte: db_mod.types.RelationalRowsCte = .{
        .name = lowered.cte_name,
        .query = lowered.anchor.plan.query,
        .max_rows = lowered.max_rows,
        .max_bytes = lowered.max_bytes,
        .spill_after_bytes = lowered.spill_after_bytes,
    };
    try db_mod.DB.admitRelationalRowsCteMaterializationAllowSpill(cte, rows.len, materialized_bytes);
}

fn recursiveCteJoinRowsMatchAlloc(
    alloc: std.mem.Allocator,
    base_row_json: []const u8,
    cte_row_json: []const u8,
    join: sql_adapter_runtime.LoweredRecursiveCteJoinMemberPlan,
    base_on_left: bool,
) !bool {
    var parsed_base = std.json.parseFromSlice(std.json.Value, alloc, base_row_json, .{}) catch return error.InvalidRowsRequest;
    defer parsed_base.deinit();
    var parsed_cte = std.json.parseFromSlice(std.json.Value, alloc, cte_row_json, .{}) catch return error.InvalidRowsRequest;
    defer parsed_cte.deinit();

    for (join.on) |on| {
        const base_field = if (base_on_left) on.left_field else on.right_field;
        const cte_field = if (base_on_left) on.right_field else on.left_field;
        const base_value = try rowObjectFieldJsonAlloc(alloc, parsed_base.value, base_field);
        defer alloc.free(base_value);
        const cte_value = try rowObjectFieldJsonAlloc(alloc, parsed_cte.value, cte_field);
        defer alloc.free(cte_value);
        if (!std.mem.eql(u8, base_value, cte_value)) return false;
    }
    return true;
}

fn rowObjectFieldJsonAlloc(alloc: std.mem.Allocator, row: std.json.Value, field: []const u8) ![]u8 {
    if (row != .object) return error.InvalidRowsRequest;
    const value = row.object.get(field) orelse return try alloc.dupe(u8, "null");
    return try std.json.Stringify.valueAlloc(alloc, value, .{});
}

fn recursiveCteProjectedRowJsonAlloc(
    alloc: std.mem.Allocator,
    base_row_json: []const u8,
    cte_row_json: []const u8,
    projections: []const db_mod.types.RelationalRowsExpressionProjection,
) ![]u8 {
    var parsed_base = std.json.parseFromSlice(std.json.Value, alloc, base_row_json, .{}) catch return error.InvalidRowsRequest;
    defer parsed_base.deinit();
    var parsed_cte = std.json.parseFromSlice(std.json.Value, alloc, cte_row_json, .{}) catch return error.InvalidRowsRequest;
    defer parsed_cte.deinit();

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, '{');
    for (projections, 0..) |projection, i| {
        if (i != 0) try out.append(alloc, ',');
        const output_json = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(projection.output, .{})});
        defer alloc.free(output_json);
        try out.appendSlice(alloc, output_json);
        try out.append(alloc, ':');
        const value_json = try relational_rows_api.expressionValueJsonWithTargetSourceAlloc(alloc, parsed_base.value, parsed_cte.value, projection.expression);
        defer alloc.free(value_json);
        try out.appendSlice(alloc, value_json);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn catalogTargetForLoweredSqlTable(
    session: catalog_resources.SqlCatalogSession,
    default_table_name: []const u8,
    table_name: []const u8,
) !catalog_resources.TableTarget {
    if (std.mem.eql(u8, default_table_name, table_name)) {
        return try session.tableTargetFromObjectName(table_name);
    }
    return try session.tableTargetFromObjectName(table_name);
}

pub fn catalogRuntimeSchemaUnlessDefaultAlloc(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    default_table_name: []const u8,
    table_name: []const u8,
) !?storage_schema.TableSchema {
    if (std.mem.eql(u8, default_table_name, table_name)) return null;
    const schema_json = (try table_catalog.tableSchemaJsonAlloc(alloc, catalog, table_name)) orelse return error.TableNotFound;
    defer alloc.free(schema_json);
    var parsed_schema = schema_api.parseValidatedTableSchema(alloc, schema_json) catch return error.InvalidRowsRequest;
    defer parsed_schema.deinit(alloc);
    const runtime_schema = schema_api.deriveRuntimeTableSchema(alloc, parsed_schema) catch return error.InvalidRowsRequest;
    errdefer storage_schema.freeSchema(alloc, runtime_schema);
    if (runtime_schema.storage_mode != .relational or runtime_schema.primary_key == null) return error.InvalidRowsRequest;
    return runtime_schema;
}

pub fn loweredReadJoinCteTableName(
    default_table_name: []const u8,
    left_table_name: []const u8,
    right_table_name: []const u8,
    cte_count: usize,
    left_source_cte: []const u8,
    right_source_cte: []const u8,
) ![]const u8 {
    if (cte_count == 0) return default_table_name;
    if (left_source_cte.len > 0 and right_source_cte.len > 0 and
        !std.mem.eql(u8, left_table_name, right_table_name))
    {
        return error.UnsupportedSqlShape;
    }
    if (left_source_cte.len > 0) return left_table_name;
    if (right_source_cte.len > 0) return right_table_name;
    if (std.mem.eql(u8, left_table_name, right_table_name)) return left_table_name;
    return error.UnsupportedSqlShape;
}

pub fn loweredSetOperationToRowsOperation(operation: sql_adapter_runtime.SelectSetOperation) db_mod.types.RelationalRowsSetOperation {
    return switch (operation) {
        .union_distinct => .union_distinct,
        .union_all => .union_all,
        .intersect => .intersect,
        .except => .except,
    };
}

pub fn executeSetOperationOnQueryResultsAlloc(
    alloc: std.mem.Allocator,
    plan: db_mod.types.RelationalRowsSetOperationPlan,
    left_rows: []const []const u8,
    right_rows: []const []const u8,
) !db_mod.types.RelationalRowsQueryResult {
    const combined = try db_mod.DB.relationalRowsSetOperationRowsAlloc(alloc, plan.operation, left_rows, right_rows);
    defer freeOwnedRows(alloc, combined);
    try db_mod.DB.admitRelationalRowsSetOperationRowsAllowSpill(plan, combined);
    return try db_mod.DB.queryRelationalRowsFromSourceRowsStaticAlloc(alloc, "set_operation", combined, .{
        .select_all = true,
        .order_by = plan.order_by,
        .limit = plan.limit,
        .offset = plan.offset,
    });
}

fn freeOwnedRows(alloc: std.mem.Allocator, rows: []const []const u8) void {
    for (rows) |row| alloc.free(@constCast(row));
    if (rows.len > 0) alloc.free(rows);
}

pub fn rowsInsertSourceBatchFromRoutedScansWithSchemasAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    target_table_name: []const u8,
    source_table_name: []const u8,
    target_schema: storage_schema.TableSchema,
    source_schema: storage_schema.TableSchema,
    req: db_mod.types.RelationalRowsInsertSourceRequest,
    source_ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    consistency: raft_mod.ReadConsistency,
    conflict_resolver: ?relational_rows_api.UniqueSelectorResolver,
) !?relational_rows_api.OwnedRowsBatchRequest {
    if (req.source.source_cte.len != 0) return error.InvalidRowsRequest;
    if (req.source_table.len > 0 and !std.mem.eql(u8, req.source_table, source_table_name)) return error.InvalidRowsRequest;
    const plan: db_mod.types.RelationalRowsInsertSourcePlan = .{
        .ranges = source_ranges,
        .insert_source = req,
    };
    return try rowsInsertSourcePlanBatchFromRoutedScansWithSchemasAlloc(
        alloc,
        source,
        target_table_name,
        source_table_name,
        target_schema,
        source_schema,
        plan,
        consistency,
        conflict_resolver,
    );
}

pub fn rowsInsertSourceBatchFromRecursiveCtePlanAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    catalog: table_catalog.CatalogSource,
    default_table_name: []const u8,
    default_schema: storage_schema.TableSchema,
    target_table_name: []const u8,
    target_schema: storage_schema.TableSchema,
    recursive: sql_adapter_runtime.LoweredRecursiveCtePlan,
    req: db_mod.types.RelationalRowsInsertSourceRequest,
    consistency: raft_mod.ReadConsistency,
    conflict_resolver: ?relational_rows_api.UniqueSelectorResolver,
) !?relational_rows_api.OwnedRowsBatchRequest {
    return try rowsInsertSourceBatchFromRecursiveCtePlanWithSessionAlloc(
        alloc,
        source,
        catalog,
        catalog_resources.SqlCatalogSession.default(),
        default_table_name,
        default_schema,
        target_table_name,
        target_schema,
        recursive,
        req,
        consistency,
        conflict_resolver,
    );
}

pub fn rowsInsertSourceBatchFromRecursiveCtePlanWithSessionAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
    default_table_name: []const u8,
    default_schema: storage_schema.TableSchema,
    target_table_name: []const u8,
    target_schema: storage_schema.TableSchema,
    recursive: sql_adapter_runtime.LoweredRecursiveCtePlan,
    req: db_mod.types.RelationalRowsInsertSourceRequest,
    consistency: raft_mod.ReadConsistency,
    conflict_resolver: ?relational_rows_api.UniqueSelectorResolver,
) !?relational_rows_api.OwnedRowsBatchRequest {
    if (!std.mem.eql(u8, req.source.source_cte, recursive.cte_name)) return error.InvalidRowsRequest;
    var materialized = (try materializeLoweredRecursiveCteRowsWithSessionAlloc(
        alloc,
        source,
        catalog,
        session,
        default_table_name,
        default_schema,
        recursive,
        consistency,
    )) orelse return null;
    defer materialized.deinit(alloc);

    var source_schema = default_schema;
    source_schema.relational_columns = recursive.output_columns;
    source_schema.primary_key = null;

    var source_query = req.source;
    source_query.source_cte = "";
    source_query.select = &.{};
    source_query.json_extract = &.{};
    source_query.array_length = &.{};
    source_query.coalesce = &.{};
    source_query.field_aliases = &.{};
    source_query.expressions = &.{};
    source_query.select_all = true;
    var source_result = try relational_rows_api.executeRowsQueryOnJsonRowsAlloc(alloc, source_schema, source_query, materialized.rows);
    defer source_result.deinit(alloc);

    var effective_req = req;
    effective_req.source = source_query;
    effective_req.source.source_cte = recursive.cte_name;
    return try relational_rows_api.buildRowsInsertSourceBatchWithSchemasAlloc(
        alloc,
        target_table_name,
        target_schema,
        source_schema,
        effective_req,
        source_result.rows,
        conflict_resolver,
    );
}

pub fn rowsInsertSourcePlanBatchFromRoutedScansWithSchemasAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    target_table_name: []const u8,
    source_table_name: []const u8,
    target_schema: storage_schema.TableSchema,
    source_schema: storage_schema.TableSchema,
    plan: db_mod.types.RelationalRowsInsertSourcePlan,
    consistency: raft_mod.ReadConsistency,
    conflict_resolver: ?relational_rows_api.UniqueSelectorResolver,
) !?relational_rows_api.OwnedRowsBatchRequest {
    if (!scanPayloadCanStripSyntheticKey(source_schema)) return error.UnsupportedRowsQuery;
    if (plan.insert_source.source_table.len > 0 and !std.mem.eql(u8, plan.insert_source.source_table, source_table_name)) {
        return error.InvalidRowsRequest;
    }

    var base_rows = (try collectRowsFromRoutedScansAlloc(alloc, source, source_table_name, plan.ranges, consistency)) orelse return null;
    defer base_rows.deinit(alloc);

    return try relational_rows_api.buildRowsInsertSourcePlanBatchOnJsonRowsAlloc(
        alloc,
        target_table_name,
        target_schema,
        source_schema,
        plan,
        base_rows.rows,
        conflict_resolver,
    );
}

pub fn rowsMergeMutationBatchFromRoutedScansWithSchemasAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    target_table_name: []const u8,
    source_table_name: []const u8,
    target_schema: storage_schema.TableSchema,
    source_schema: storage_schema.TableSchema,
    plan: sql_adapter_runtime.LoweredMergeMutationPlan,
    target_query: db_mod.types.RelationalRowsQueryRequest,
    target_ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    source_query: db_mod.types.RelationalRowsQueryRequest,
    source_ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    consistency: raft_mod.ReadConsistency,
) !?relational_rows_api.OwnedRowsBatchRequest {
    const target_rows = (try collectMergeTargetRowsFromRoutedScansAlloc(
        alloc,
        source,
        target_table_name,
        target_schema,
        target_query,
        target_ranges,
        consistency,
    )) orelse return null;
    defer db_mod.types.freeRelationalRowsCollectedRows(alloc, target_rows);

    var source_rows = if (plan.ctes.len != 0 or source_query.source_cte.len != 0) blk: {
        if (plan.data_modifying_ctes.len != 0 or source_ranges.len != 0) return error.UnsupportedSqlShape;
        break :blk (try source.rowsQueryPlan(
            alloc,
            source_table_name,
            source_schema,
            .{ .ctes = plan.ctes, .query = source_query },
            consistency,
        )) orelse return null;
    } else (try collectMergeSourceRowsFromRoutedScansAlloc(
        alloc,
        source,
        source_table_name,
        source_schema,
        source_query,
        source_ranges,
        consistency,
    )) orelse return null;
    defer source_rows.deinit(alloc);

    const merge_targets = try alloc.alloc(sql_adapter_runtime.MergeExecutionTargetRow, target_rows.len);
    defer alloc.free(merge_targets);
    for (target_rows, 0..) |row, i| {
        merge_targets[i] = .{
            .key = row.key,
            .json = row.json,
            .version = row.version,
        };
    }

    return try sql_adapter_runtime.buildMergeMutationBatchAlloc(
        alloc,
        target_schema,
        source_schema,
        plan,
        merge_targets,
        source_rows.rows,
    );
}

pub fn rowsRecursiveMergeMutationBatchFromRoutedScansWithSchemasAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    catalog: table_catalog.CatalogSource,
    default_table_name: []const u8,
    target_table_name: []const u8,
    target_schema: storage_schema.TableSchema,
    recursive_source_schema: storage_schema.TableSchema,
    lowered: sql_adapter_runtime.LoweredRecursiveMergeMutation,
    target_query: db_mod.types.RelationalRowsQueryRequest,
    target_ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    consistency: raft_mod.ReadConsistency,
) !?relational_rows_api.OwnedRowsBatchRequest {
    return try rowsRecursiveMergeMutationBatchFromRoutedScansWithSchemasAndSessionAlloc(
        alloc,
        source,
        catalog,
        catalog_resources.SqlCatalogSession.default(),
        default_table_name,
        target_table_name,
        target_schema,
        recursive_source_schema,
        lowered,
        target_query,
        target_ranges,
        consistency,
    );
}

pub fn rowsRecursiveMergeMutationBatchFromRoutedScansWithSchemasAndSessionAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
    default_table_name: []const u8,
    target_table_name: []const u8,
    target_schema: storage_schema.TableSchema,
    recursive_source_schema: storage_schema.TableSchema,
    lowered: sql_adapter_runtime.LoweredRecursiveMergeMutation,
    target_query: db_mod.types.RelationalRowsQueryRequest,
    target_ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    consistency: raft_mod.ReadConsistency,
) !?relational_rows_api.OwnedRowsBatchRequest {
    if (!std.mem.eql(u8, lowered.merge.source.source_cte, lowered.recursive.cte_name)) return error.InvalidRowsRequest;

    const target_rows = (try collectMergeTargetRowsFromRoutedScansAlloc(
        alloc,
        source,
        target_table_name,
        target_schema,
        target_query,
        target_ranges,
        consistency,
    )) orelse return null;
    defer db_mod.types.freeRelationalRowsCollectedRows(alloc, target_rows);

    var materialized = (try materializeLoweredRecursiveCteRowsWithSessionAlloc(
        alloc,
        source,
        catalog,
        session,
        default_table_name,
        recursive_source_schema,
        lowered.recursive,
        consistency,
    )) orelse return null;
    defer materialized.deinit(alloc);

    var cte_schema = recursive_source_schema;
    cte_schema.relational_columns = lowered.recursive.output_columns;
    const synthetic_primary_key_columns = [_][]const u8{lowered.recursive.output_columns[0].name};
    cte_schema.primary_key = .{ .columns = synthetic_primary_key_columns[0..] };

    var source_query = lowered.merge.source;
    source_query.source_cte = "";
    var source_rows = try relational_rows_api.executeRowsQueryOnJsonRowsAlloc(alloc, cte_schema, source_query, materialized.rows);
    defer source_rows.deinit(alloc);

    const merge_targets = try alloc.alloc(sql_adapter_runtime.MergeExecutionTargetRow, target_rows.len);
    defer alloc.free(merge_targets);
    for (target_rows, 0..) |row, i| {
        merge_targets[i] = .{
            .key = row.key,
            .json = row.json,
            .version = row.version,
        };
    }

    return try sql_adapter_runtime.buildMergeMutationBatchAlloc(
        alloc,
        target_schema,
        cte_schema,
        lowered.merge,
        merge_targets,
        source_rows.rows,
    );
}

pub const RoutedRows = struct {
    rows: [][]const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.rows) |row| alloc.free(@constCast(row));
        if (self.rows.len > 0) alloc.free(self.rows);
        self.* = undefined;
    }
};

const RoutedMergeScanRows = struct {
    rows: []db_mod.types.RelationalRowsCollectedRow,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        db_mod.types.freeRelationalRowsCollectedRows(alloc, self.rows);
        self.* = undefined;
    }
};

pub fn collectMergeTargetRowsFromRoutedScansAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    schema: storage_schema.TableSchema,
    req: db_mod.types.RelationalRowsQueryRequest,
    ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    consistency: raft_mod.ReadConsistency,
) !?[]db_mod.types.RelationalRowsCollectedRow {
    var scanned = (try collectMergeScanRowsFromRoutedScansAlloc(alloc, source, table_name, schema, ranges, consistency)) orelse return null;
    defer scanned.deinit(alloc);

    const selected = try selectMergeScanRowsAlloc(alloc, schema, req, scanned.rows);
    errdefer db_mod.types.freeRelationalRowsCollectedRows(alloc, selected);

    for (selected) |*row| {
        var lookup = (try source.lookup(alloc, table_name, row.key, .{ .include_all_fields = true }, consistency)) orelse return error.TopologyChanged;
        defer lookup.deinit(alloc);
        if (!std.mem.eql(u8, lookup.json, row.json)) return error.TopologyChanged;
        row.version = lookup.version;
    }

    return selected;
}

pub fn collectMergeSourceRowsFromRoutedScansAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    schema: storage_schema.TableSchema,
    req: db_mod.types.RelationalRowsQueryRequest,
    ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsQueryResult {
    var scanned = (try collectMergeScanRowsFromRoutedScansAlloc(alloc, source, table_name, schema, ranges, consistency)) orelse return null;
    defer scanned.deinit(alloc);

    const selected = try selectMergeScanRowsAlloc(alloc, schema, req, scanned.rows);
    defer db_mod.types.freeRelationalRowsCollectedRows(alloc, selected);

    const rows = try alloc.alloc([]const u8, selected.len);
    errdefer alloc.free(rows);
    for (selected, 0..) |*row, i| {
        rows[i] = row.json;
        row.json = "";
    }
    return .{ .rows = rows, .total = std.math.cast(u32, rows.len) orelse return error.InvalidRowsRequest };
}

fn collectMergeScanRowsFromRoutedScansAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    schema: storage_schema.TableSchema,
    ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    consistency: raft_mod.ReadConsistency,
) !?RoutedMergeScanRows {
    if (!scanPayloadCanStripSyntheticKey(schema)) return error.UnsupportedRowsQuery;
    var rows = std.ArrayListUnmanaged(db_mod.types.RelationalRowsCollectedRow).empty;
    errdefer {
        for (rows.items) |row_value| {
            var row = row_value;
            row.deinit(alloc);
        }
        rows.deinit(alloc);
    }

    var budget = RoutedRowsMaterializationBudget.initDefault();
    var saw_source = false;
    if (ranges.len == 0) {
        saw_source = try appendMergeScanRowsFromRoutedScanAlloc(alloc, source, table_name, "", "", &rows, &budget, consistency);
    } else {
        for (ranges) |range| {
            saw_source = (try appendMergeScanRowsFromRoutedScanAlloc(alloc, source, table_name, range.start, range.end, &rows, &budget, consistency)) or saw_source;
        }
    }
    if (!saw_source) {
        for (rows.items) |row_value| {
            var row = row_value;
            row.deinit(alloc);
        }
        rows.deinit(alloc);
        return null;
    }

    return .{ .rows = try rows.toOwnedSlice(alloc) };
}

fn appendMergeScanRowsFromRoutedScanAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
    rows: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsCollectedRow),
    budget: *RoutedRowsMaterializationBudget,
    consistency: raft_mod.ReadConsistency,
) !bool {
    var scan_result = (try source.scan(alloc, table_name, from_key, to_key, .{
        .include_documents = true,
        .include_all_fields = true,
    }, consistency)) orelse return false;
    defer scan_result.deinit(alloc);

    var lines = std.mem.splitScalar(u8, scan_result.ndjson, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        const row = try mergeScanRowFromScanLineAlloc(alloc, line);
        errdefer {
            var owned = row;
            owned.deinit(alloc);
        }
        try budget.account(row.json);
        try rows.append(alloc, row);
    }
    return true;
}

fn mergeScanRowFromScanLineAlloc(alloc: std.mem.Allocator, line: []const u8) !db_mod.types.RelationalRowsCollectedRow {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, line, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRemoteResponse;
    const key_value = parsed.value.object.get("key") orelse return error.InvalidRemoteResponse;
    if (key_value != .string) return error.InvalidRemoteResponse;
    const key = try alloc.dupe(u8, key_value.string);
    errdefer alloc.free(key);
    if (parsed.value.object.fetchOrderedRemove("key") == null) return error.InvalidRemoteResponse;
    const json = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
    errdefer alloc.free(json);
    return .{ .key = key, .json = json, .version = 0 };
}

fn selectMergeScanRowsAlloc(
    alloc: std.mem.Allocator,
    schema: storage_schema.TableSchema,
    req: db_mod.types.RelationalRowsQueryRequest,
    scanned: []const db_mod.types.RelationalRowsCollectedRow,
) ![]db_mod.types.RelationalRowsCollectedRow {
    if (req.source_cte.len != 0 or req.row_claim != null) return error.UnsupportedRowsQuery;
    const row_jsons = try alloc.alloc([]const u8, scanned.len);
    defer alloc.free(row_jsons);
    for (scanned, 0..) |row, i| row_jsons[i] = row.json;

    var filter_req = req;
    filter_req.select = &.{};
    filter_req.json_extract = &.{};
    filter_req.array_length = &.{};
    filter_req.coalesce = &.{};
    filter_req.field_aliases = &.{};
    filter_req.expressions = &.{};
    filter_req.select_all = true;
    var filtered = try relational_rows_api.executeRowsQueryOnJsonRowsAlloc(alloc, schema, filter_req, row_jsons);
    defer filtered.deinit(alloc);

    var used = try alloc.alloc(bool, scanned.len);
    defer alloc.free(used);
    @memset(used, false);

    var out = std.ArrayListUnmanaged(db_mod.types.RelationalRowsCollectedRow).empty;
    errdefer {
        for (out.items) |row_value| {
            var row = row_value;
            row.deinit(alloc);
        }
        out.deinit(alloc);
    }
    try out.ensureUnusedCapacity(alloc, filtered.rows.len);
    for (filtered.rows) |selected_json| {
        var matched_index: ?usize = null;
        for (scanned, 0..) |row, i| {
            if (used[i]) continue;
            if (!std.mem.eql(u8, row.json, selected_json)) continue;
            matched_index = i;
            break;
        }
        const index = matched_index orelse return error.TopologyChanged;
        used[index] = true;
        const key = try alloc.dupe(u8, scanned[index].key);
        errdefer alloc.free(key);
        const json = try alloc.dupe(u8, scanned[index].json);
        errdefer alloc.free(json);
        out.appendAssumeCapacity(.{
            .key = key,
            .json = json,
            .version = scanned[index].version,
        });
    }
    return try out.toOwnedSlice(alloc);
}

pub fn collectRowsFromRoutedScansAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    consistency: raft_mod.ReadConsistency,
) !?RoutedRows {
    var rows = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (rows.items) |row| alloc.free(@constCast(row));
        rows.deinit(alloc);
    }

    var budget = RoutedRowsMaterializationBudget.initDefault();
    var saw_source = false;
    if (ranges.len == 0) {
        saw_source = try appendRowsFromRoutedScanAlloc(alloc, source, table_name, "", "", &rows, &budget, consistency);
    } else {
        for (ranges) |range| {
            saw_source = (try appendRowsFromRoutedScanAlloc(alloc, source, table_name, range.start, range.end, &rows, &budget, consistency)) or saw_source;
        }
    }
    if (!saw_source) {
        for (rows.items) |row| alloc.free(@constCast(row));
        rows.deinit(alloc);
        return null;
    }

    return .{ .rows = try rows.toOwnedSlice(alloc) };
}

pub fn rowsQueryPlanFromRoutedScansAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    runtime_schema: storage_schema.TableSchema,
    plan: db_mod.types.RelationalRowsQueryPlan,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsQueryResult {
    if (!scanPayloadCanStripSyntheticKey(runtime_schema)) return error.UnsupportedRowsQuery;

    var scanned_rows = (try collectRowsFromRoutedScansAlloc(alloc, source, table_name, plan.ranges, consistency)) orelse return null;
    defer scanned_rows.deinit(alloc);

    var local_plan = plan;
    local_plan.ranges = &.{};
    return try relational_rows_api.executeRowsQueryPlanOnJsonRowsAlloc(alloc, runtime_schema, local_plan, scanned_rows.rows);
}

pub fn rowsAggregatePlanFromRoutedScansAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    runtime_schema: storage_schema.TableSchema,
    plan: db_mod.types.RelationalRowsAggregatePlan,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsAggregateResult {
    if (!scanPayloadCanStripSyntheticKey(runtime_schema)) return error.UnsupportedRowsQuery;

    var scanned_rows = (try collectRowsFromRoutedScansAlloc(alloc, source, table_name, plan.ranges, consistency)) orelse return null;
    defer scanned_rows.deinit(alloc);

    var local_plan = plan;
    local_plan.ranges = &.{};
    return try relational_rows_api.executeRowsAggregatePlanOnJsonRowsAlloc(alloc, runtime_schema, local_plan, scanned_rows.rows);
}

pub fn rowsSetOperationPlanFromRoutedScansAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    runtime_schema: storage_schema.TableSchema,
    plan: db_mod.types.RelationalRowsSetOperationPlan,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsQueryResult {
    var left = (try rowsQueryPlanFromRoutedScansAlloc(alloc, source, table_name, runtime_schema, plan.left, consistency)) orelse return null;
    defer left.deinit(alloc);
    var right = (try rowsQueryPlanFromRoutedScansAlloc(alloc, source, table_name, runtime_schema, plan.right, consistency)) orelse return null;
    defer right.deinit(alloc);
    return try executeSetOperationOnQueryResultsAlloc(alloc, plan, left.rows, right.rows);
}

pub fn rowsWindowPlanFromRoutedScansAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    runtime_schema: storage_schema.TableSchema,
    plan: db_mod.types.RelationalRowsWindowPlan,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsWindowResult {
    if (!scanPayloadCanStripSyntheticKey(runtime_schema)) return error.UnsupportedRowsQuery;

    var scanned_rows = (try collectRowsFromRoutedScansAlloc(alloc, source, table_name, plan.ranges, consistency)) orelse return null;
    defer scanned_rows.deinit(alloc);

    var local_plan = plan;
    local_plan.ranges = &.{};
    return try relational_rows_api.executeRowsWindowPlanOnJsonRowsAlloc(alloc, runtime_schema, local_plan, scanned_rows.rows);
}

fn appendRowsFromRoutedScanAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
    rows: *std.ArrayListUnmanaged([]const u8),
    budget: *RoutedRowsMaterializationBudget,
    consistency: raft_mod.ReadConsistency,
) !bool {
    var scan_result = (try source.scan(alloc, table_name, from_key, to_key, .{
        .include_documents = true,
        .include_all_fields = true,
    }, consistency)) orelse return false;
    defer scan_result.deinit(alloc);

    var lines = std.mem.splitScalar(u8, scan_result.ndjson, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        const row = try rowJsonFromScanLineAlloc(alloc, line);
        errdefer alloc.free(row);
        try budget.account(row);
        try rows.append(alloc, row);
    }
    return true;
}

const RoutedRowsMaterializationBudget = struct {
    max_rows: u64,
    max_bytes: u64,
    rows: u64 = 0,
    bytes: u64 = 2,

    fn initDefault() RoutedRowsMaterializationBudget {
        return .init(db_mod.types.default_relational_rows_cte_max_rows, db_mod.types.default_relational_rows_cte_max_bytes);
    }

    fn init(max_rows: u64, max_bytes: u64) RoutedRowsMaterializationBudget {
        return .{
            .max_rows = max_rows,
            .max_bytes = max_bytes,
        };
    }

    fn account(self: *RoutedRowsMaterializationBudget, row_json: []const u8) !void {
        if (self.rows >= self.max_rows) return error.UnsupportedRowsQuery;
        var next_bytes = self.bytes;
        if (self.rows > 0) next_bytes = std.math.add(u64, next_bytes, 1) catch return error.UnsupportedRowsQuery;
        next_bytes = std.math.add(u64, next_bytes, @intCast(row_json.len)) catch return error.UnsupportedRowsQuery;
        if (next_bytes > self.max_bytes) return error.UnsupportedRowsQuery;
        self.rows += 1;
        self.bytes = next_bytes;
    }
};

pub fn routedRowsPlanRangesForJoinAlloc(
    alloc: std.mem.Allocator,
    left_ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    right_ranges: []const db_mod.types.RelationalRowsDocKeyRange,
) ![]const db_mod.types.RelationalRowsDocKeyRange {
    if ((left_ranges.len == 0) != (right_ranges.len == 0)) return error.InvalidQueryRequest;
    if (left_ranges.len == 0) return &.{};
    const sorted = try alloc.alloc(db_mod.types.RelationalRowsDocKeyRange, left_ranges.len + right_ranges.len);
    defer alloc.free(sorted);
    @memcpy(sorted[0..left_ranges.len], left_ranges);
    @memcpy(sorted[left_ranges.len..], right_ranges);
    std.sort.pdq(db_mod.types.RelationalRowsDocKeyRange, sorted, {}, routedRowsDocKeyRangeLessThan);

    var out = std.ArrayListUnmanaged(db_mod.types.RelationalRowsDocKeyRange).empty;
    errdefer out.deinit(alloc);
    for (sorted) |range| {
        if (out.items.len == 0) {
            try out.append(alloc, range);
            continue;
        }
        const last = &out.items[out.items.len - 1];
        if (routedRowsDocKeyRangesOverlapOrTouch(last.*, range)) {
            last.end = routedRowsDocKeyRangeMaxEnd(last.end, range.end);
        } else {
            try out.append(alloc, range);
        }
    }
    return try out.toOwnedSlice(alloc);
}

pub fn routedRowsPlanRangesForJoinCtesAlloc(
    alloc: std.mem.Allocator,
    cte_table_name: []const u8,
    left_table_name: []const u8,
    right_table_name: []const u8,
    left_query: db_mod.types.RelationalRowsQueryRequest,
    right_query: db_mod.types.RelationalRowsQueryRequest,
    left_ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    right_ranges: []const db_mod.types.RelationalRowsDocKeyRange,
) ![]const db_mod.types.RelationalRowsDocKeyRange {
    if ((left_ranges.len == 0) != (right_ranges.len == 0)) return error.InvalidQueryRequest;
    if (left_ranges.len == 0) return &.{};

    const include_left = left_query.source_cte.len != 0 or std.mem.eql(u8, left_table_name, cte_table_name);
    const include_right = right_query.source_cte.len != 0 or std.mem.eql(u8, right_table_name, cte_table_name);
    if (include_left and include_right) return try routedRowsPlanRangesForJoinAlloc(alloc, left_ranges, right_ranges);
    if (include_left) return try cloneRowsPlanRangesAlloc(alloc, left_ranges);
    if (include_right) return try cloneRowsPlanRangesAlloc(alloc, right_ranges);
    return &.{};
}

fn cloneRowsPlanRangesAlloc(
    alloc: std.mem.Allocator,
    ranges: []const db_mod.types.RelationalRowsDocKeyRange,
) ![]const db_mod.types.RelationalRowsDocKeyRange {
    if (ranges.len == 0) return &.{};
    const out = try alloc.alloc(db_mod.types.RelationalRowsDocKeyRange, ranges.len);
    @memcpy(out, ranges);
    return out;
}

fn routedRowsDocKeyRangesOverlapOrTouch(lhs: db_mod.types.RelationalRowsDocKeyRange, rhs: db_mod.types.RelationalRowsDocKeyRange) bool {
    if (lhs.end.len == 0) return true;
    if (rhs.start.len == 0) return true;
    return std.mem.order(u8, rhs.start, lhs.end) != .gt;
}

fn routedRowsDocKeyRangeMaxEnd(lhs: []const u8, rhs: []const u8) []const u8 {
    if (lhs.len == 0 or rhs.len == 0) return "";
    return if (std.mem.order(u8, lhs, rhs) == .lt) rhs else lhs;
}

fn routedRowsDocKeyRangeLessThan(_: void, lhs: db_mod.types.RelationalRowsDocKeyRange, rhs: db_mod.types.RelationalRowsDocKeyRange) bool {
    if (lhs.start.len == 0) return rhs.start.len != 0;
    if (rhs.start.len == 0) return false;
    const start_order = std.mem.order(u8, lhs.start, rhs.start);
    if (start_order != .eq) return start_order == .lt;
    if (lhs.end.len == 0) return false;
    if (rhs.end.len == 0) return true;
    return std.mem.order(u8, lhs.end, rhs.end) == .lt;
}

fn rowJsonFromScanLineAlloc(alloc: std.mem.Allocator, line: []const u8) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, line, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRemoteResponse;
    if (parsed.value.object.fetchOrderedRemove("key") == null) return error.InvalidRemoteResponse;
    return try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
}

pub fn scanPayloadCanStripSyntheticKey(schema: storage_schema.TableSchema) bool {
    for (schema.relational_columns) |column| {
        if (std.mem.eql(u8, column.name, "key")) return false;
        if (std.mem.eql(u8, column.path, "key")) return false;
    }
    return true;
}

pub fn effectiveSideTable(default_table_name: []const u8, maybe_table_name: []const u8) []const u8 {
    return if (maybe_table_name.len == 0) default_table_name else maybe_table_name;
}

pub fn rowsJoinPlanFromRoutedScansWithSchemasAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    cte_table_name: []const u8,
    left_table_name: []const u8,
    right_table_name: []const u8,
    cte_base_schema: storage_schema.TableSchema,
    left_schema: storage_schema.TableSchema,
    right_schema: storage_schema.TableSchema,
    plan: db_mod.types.RelationalRowsJoinPlan,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsJoinResult {
    if (!scanPayloadCanStripSyntheticKey(cte_base_schema) or
        !scanPayloadCanStripSyntheticKey(left_schema) or
        !scanPayloadCanStripSyntheticKey(right_schema))
    {
        return error.UnsupportedRowsQuery;
    }

    const empty_rows: []const []const u8 = &.{};
    var cte_rows_storage: ?RoutedRows = null;
    defer if (cte_rows_storage) |*rows| rows.deinit(alloc);
    const cte_rows = if (plan.ctes.len == 0) empty_rows else blk: {
        const cte_ranges = try routedRowsPlanRangesForJoinCtesAlloc(
            alloc,
            cte_table_name,
            left_table_name,
            right_table_name,
            plan.join.left,
            plan.join.right,
            plan.left_ranges,
            plan.right_ranges,
        );
        defer if (cte_ranges.len > 0) alloc.free(cte_ranges);
        cte_rows_storage = (try collectRowsFromRoutedScansAlloc(alloc, source, cte_table_name, cte_ranges, consistency)) orelse return null;
        break :blk cte_rows_storage.?.rows;
    };
    var left_rows = (try collectRowsFromRoutedScansAlloc(alloc, source, left_table_name, plan.left_ranges, consistency)) orelse return null;
    defer left_rows.deinit(alloc);
    var right_rows = (try collectRowsFromRoutedScansAlloc(alloc, source, right_table_name, plan.right_ranges, consistency)) orelse return null;
    defer right_rows.deinit(alloc);

    var local_plan = plan;
    local_plan.left_ranges = &.{};
    local_plan.right_ranges = &.{};
    return try relational_rows_api.executeRowsJoinPlanOnJsonRowsWithSchemasAlloc(alloc, cte_base_schema, left_schema, right_schema, local_plan, cte_rows, left_rows.rows, right_rows.rows);
}

pub fn rowsLateralPlanFromRoutedScansWithSchemasAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    cte_table_name: []const u8,
    left_table_name: []const u8,
    right_table_name: []const u8,
    cte_base_schema: storage_schema.TableSchema,
    left_schema: storage_schema.TableSchema,
    right_schema: storage_schema.TableSchema,
    plan: db_mod.types.RelationalRowsLateralPlan,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsJoinResult {
    if (!scanPayloadCanStripSyntheticKey(cte_base_schema) or
        !scanPayloadCanStripSyntheticKey(left_schema) or
        !scanPayloadCanStripSyntheticKey(right_schema))
    {
        return error.UnsupportedRowsQuery;
    }

    const empty_rows: []const []const u8 = &.{};
    var cte_rows_storage: ?RoutedRows = null;
    defer if (cte_rows_storage) |*rows| rows.deinit(alloc);
    const cte_rows = if (plan.ctes.len == 0) empty_rows else blk: {
        const cte_ranges = try routedRowsPlanRangesForJoinCtesAlloc(
            alloc,
            cte_table_name,
            left_table_name,
            right_table_name,
            plan.lateral.left,
            plan.lateral.right,
            plan.left_ranges,
            plan.right_ranges,
        );
        defer if (cte_ranges.len > 0) alloc.free(cte_ranges);
        cte_rows_storage = (try collectRowsFromRoutedScansAlloc(alloc, source, cte_table_name, cte_ranges, consistency)) orelse return null;
        break :blk cte_rows_storage.?.rows;
    };
    var left_rows = (try collectRowsFromRoutedScansAlloc(alloc, source, left_table_name, plan.left_ranges, consistency)) orelse return null;
    defer left_rows.deinit(alloc);
    var right_rows = (try collectRowsFromRoutedScansAlloc(alloc, source, right_table_name, plan.right_ranges, consistency)) orelse return null;
    defer right_rows.deinit(alloc);

    var local_plan = plan;
    local_plan.left_ranges = &.{};
    local_plan.right_ranges = &.{};
    return try relational_rows_api.executeRowsLateralPlanOnJsonRowsWithSchemasAlloc(alloc, cte_base_schema, left_schema, right_schema, local_plan, cte_rows, left_rows.rows, right_rows.rows);
}

test "lowered sql set operation materialization admission distinguishes spill from hard caps" {
    const rows = [_][]const u8{
        "{\"id\":\"a\"}",
        "{\"id\":\"b\"}",
    };
    const observed_bytes = db_mod.types.relationalRowsCteMaterializedJsonBytes(&rows) orelse return error.TestUnexpectedResult;

    try db_mod.DB.admitRelationalRowsSetOperationRows(.{
        .operation = .union_distinct,
        .left = .{},
        .right = .{},
        .max_rows = 2,
        .max_bytes = observed_bytes,
        .spill_after_bytes = observed_bytes,
    }, &rows);
    try std.testing.expectError(error.RelationalRowsCteMaterializationRejected, db_mod.DB.admitRelationalRowsSetOperationRows(.{
        .operation = .union_distinct,
        .left = .{},
        .right = .{},
        .max_rows = 1,
        .max_bytes = observed_bytes,
        .spill_after_bytes = observed_bytes,
    }, &rows));
    try std.testing.expectError(error.RelationalRowsCteMaterializationRejected, db_mod.DB.admitRelationalRowsSetOperationRows(.{
        .operation = .union_distinct,
        .left = .{},
        .right = .{},
        .max_rows = 2,
        .max_bytes = observed_bytes - 1,
        .spill_after_bytes = observed_bytes - 1,
    }, &rows));
    try std.testing.expectError(error.RelationalRowsCteSpillRequired, db_mod.DB.admitRelationalRowsSetOperationRows(.{
        .operation = .union_distinct,
        .left = .{},
        .right = .{},
        .max_rows = 2,
        .max_bytes = observed_bytes,
        .spill_after_bytes = observed_bytes - 1,
    }, &rows));
    try db_mod.DB.admitRelationalRowsSetOperationRowsAllowSpill(.{
        .operation = .union_distinct,
        .left = .{},
        .right = .{},
        .max_rows = 2,
        .max_bytes = observed_bytes,
        .spill_after_bytes = observed_bytes - 1,
    }, &rows);
    try std.testing.expectError(error.RelationalRowsCteMaterializationRejected, db_mod.DB.admitRelationalRowsSetOperationRowsAllowSpill(.{
        .operation = .union_distinct,
        .left = .{},
        .right = .{},
        .max_rows = 1,
        .max_bytes = observed_bytes,
        .spill_after_bytes = observed_bytes - 1,
    }, &rows));
}

test "lowered sql recursive cte materialization admission uses stream spill policy" {
    const rows = [_][]const u8{
        "{\"id\":\"a\"}",
        "{\"id\":\"b\"}",
    };
    const observed_bytes = db_mod.types.relationalRowsCteMaterializedJsonBytes(&rows) orelse return error.TestUnexpectedResult;
    const recursive = sql_adapter_runtime.LoweredRecursiveCtePlan{
        .cte_name = "walk",
        .operation = .union_all,
        .anchor = .{
            .table_name = "nodes",
            .plan = .{},
        },
        .recursive_member = .{ .join = .{
            .left_table_name = "nodes",
            .right_table_name = "walk",
            .join_type = .inner,
            .on = &.{},
            .projections = &.{},
        } },
        .max_rows = 2,
        .max_bytes = observed_bytes,
        .spill_after_bytes = observed_bytes - 1,
    };
    try admitRecursiveCteRows(recursive, &rows);

    const too_many_rows = sql_adapter_runtime.LoweredRecursiveCtePlan{
        .cte_name = "walk",
        .operation = .union_all,
        .anchor = .{
            .table_name = "nodes",
            .plan = .{},
        },
        .recursive_member = .{ .join = .{
            .left_table_name = "nodes",
            .right_table_name = "walk",
            .join_type = .inner,
            .on = &.{},
            .projections = &.{},
        } },
        .max_rows = 1,
        .max_bytes = observed_bytes,
        .spill_after_bytes = observed_bytes - 1,
    };
    try std.testing.expectError(error.RelationalRowsCteMaterializationRejected, admitRecursiveCteRows(too_many_rows, &rows));
}

test "routed rows materialization budget fails closed on row and byte caps" {
    var row_budget = RoutedRowsMaterializationBudget.init(2, 128);
    try row_budget.account("{\"id\":\"a\"}");
    try row_budget.account("{\"id\":\"b\"}");
    try std.testing.expectError(error.UnsupportedRowsQuery, row_budget.account("{\"id\":\"c\"}"));

    var byte_budget = RoutedRowsMaterializationBudget.init(8, 16);
    try byte_budget.account("{\"id\":\"a\"}");
    try std.testing.expectError(error.UnsupportedRowsQuery, byte_budget.account("{\"id\":\"b\"}"));
}

test "routed rows query plan executes over scanned owner rows with ctes" {
    const alloc = std.testing.allocator;

    var columns = [_]storage_schema.RelationalColumn{
        .{ .name = "id", .path = "id", .field_type = .keyword, .nullable = false },
        .{ .name = "status", .path = "status", .field_type = .keyword, .nullable = false },
        .{ .name = "amount", .path = "amount", .field_type = .numeric, .nullable = false },
    };
    const schema = storage_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = columns[0..],
    };

    const FakeRoutedSource = struct {
        scan_calls: usize = 0,

        fn source(self: *@This()) TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .rows_query_plan = rowsQueryPlan,
                    .rows_aggregate_plan = rowsAggregatePlan,
                    .rows_window_plan = rowsWindowPlan,
                    .rows_join_plan = rowsJoinPlan,
                    .rows_lateral_plan = rowsLateralPlan,
                },
            };
        }

        fn lookup(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            return null;
        }

        fn query(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.SearchRequest,
            _: raft_mod.ReadConsistency,
        ) !?query_api.QueryResponse {
            return null;
        }

        fn scan(
            ptr: *anyopaque,
            scan_alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: db_mod.types.ScanOptions,
            _: raft_mod.ReadConsistency,
        ) !?ScanResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(opts.include_documents);
            try std.testing.expect(opts.include_all_fields);
            self.scan_calls += 1;
            const ndjson = if (std.mem.eql(u8, table_name, "orders"))
                if (std.mem.eql(u8, from_key, "") and std.mem.eql(u8, to_key, "n"))
                    "{\"key\":\"a\",\"id\":\"a\",\"status\":\"open\",\"amount\":1}\n{\"key\":\"b\",\"id\":\"b\",\"status\":\"closed\",\"amount\":9}\n"
                else if (std.mem.eql(u8, from_key, "n") and std.mem.eql(u8, to_key, ""))
                    "{\"key\":\"z\",\"id\":\"z\",\"status\":\"open\",\"amount\":7}\n"
                else if (std.mem.eql(u8, from_key, "") and std.mem.eql(u8, to_key, ""))
                    "{\"key\":\"a\",\"id\":\"a\",\"status\":\"open\",\"amount\":1}\n{\"key\":\"b\",\"id\":\"b\",\"status\":\"closed\",\"amount\":9}\n{\"key\":\"z\",\"id\":\"z\",\"status\":\"open\",\"amount\":7}\n"
                else
                    return error.UnexpectedRange
            else if (std.mem.eql(u8, table_name, "customers"))
                if (std.mem.eql(u8, from_key, "") and std.mem.eql(u8, to_key, ""))
                    "{\"key\":\"c1\",\"id\":\"c1\",\"status\":\"open\",\"name\":\"Ada\"}\n{\"key\":\"c2\",\"id\":\"c2\",\"status\":\"closed\",\"name\":\"Grace\"}\n"
                else
                    return error.UnexpectedRange
            else if (std.mem.eql(u8, table_name, "oversized")) {
                try std.testing.expectEqualStrings("", from_key);
                try std.testing.expectEqualStrings("", to_key);
                var out = std.ArrayListUnmanaged(u8).empty;
                errdefer out.deinit(scan_alloc);
                const line = "{\"key\":\"x\",\"id\":\"x\",\"status\":\"open\",\"amount\":1}\n";
                var index: usize = 0;
                while (index <= db_mod.types.default_relational_rows_cte_max_rows) : (index += 1) {
                    try out.appendSlice(scan_alloc, line);
                }
                return .{ .ndjson = try out.toOwnedSlice(scan_alloc) };
            } else return error.TableNotFound;
            return .{ .ndjson = try scan_alloc.dupe(u8, ndjson) };
        }

        fn rowsQueryPlan(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsQueryPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsQueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try rowsQueryPlanFromRoutedScansAlloc(plan_alloc, self.source(), table_name, runtime_schema, plan, consistency);
        }

        fn rowsAggregatePlan(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsAggregatePlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsAggregateResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try rowsAggregatePlanFromRoutedScansAlloc(plan_alloc, self.source(), table_name, runtime_schema, plan, consistency);
        }

        fn rowsWindowPlan(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsWindowPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsWindowResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try rowsWindowPlanFromRoutedScansAlloc(plan_alloc, self.source(), table_name, runtime_schema, plan, consistency);
        }

        fn rowsJoinPlan(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsJoinPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsJoinResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try rowsJoinPlanFromRoutedScansWithSchemasAlloc(
                plan_alloc,
                self.source(),
                table_name,
                table_name,
                table_name,
                runtime_schema,
                runtime_schema,
                runtime_schema,
                plan,
                consistency,
            );
        }

        fn rowsLateralPlan(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsLateralPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsJoinResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try rowsLateralPlanFromRoutedScansWithSchemasAlloc(
                plan_alloc,
                self.source(),
                table_name,
                table_name,
                table_name,
                runtime_schema,
                runtime_schema,
                runtime_schema,
                plan,
                consistency,
            );
        }
    };

    var fake = FakeRoutedSource{};
    var source = fake.source();
    const cte_select = [_][]const u8{ "id", "amount", "status" };
    const cte_predicates = [_]storage_schema.RelationalCheck{.{
        .name = "status_open",
        .field = "status",
        .value_json = "\"open\"",
    }};
    const ctes = [_]db_mod.types.RelationalRowsCte{.{
        .name = "open_rows",
        .query = .{
            .predicates = cte_predicates[0..],
            .select = cte_select[0..],
            .select_all = false,
        },
    }};
    const ranges = [_]db_mod.types.RelationalRowsDocKeyRange{
        .{ .start = "", .end = "n" },
        .{ .start = "n", .end = "" },
    };
    const final_select = [_][]const u8{"id"};
    const order_by = [_]db_mod.types.RelationalRowsQueryOrder{.{
        .field = "amount",
        .direction = .desc,
    }};

    var result = (try source.rowsQueryPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .ranges = ranges[0..],
        .query = .{
            .source_cte = "open_rows",
            .select = final_select[0..],
            .select_all = false,
            .order_by = order_by[0..],
            .limit = 1,
        },
    }, .read_index)).?;
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), fake.scan_calls);
    try std.testing.expectEqual(@as(u32, 2), result.total);
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"z\"}", result.rows[0]);

    const aggregate_group_by = [_][]const u8{"status"};
    const aggregate_specs = [_]db_mod.types.RelationalRowsAggregateSpec{
        .{ .name = "order_count", .op = .count },
        .{ .name = "amount_sum", .op = .sum, .field = "amount" },
    };
    var aggregate_result = (try source.rowsAggregatePlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .ranges = ranges[0..],
        .aggregate = .{
            .source = .{ .source_cte = "open_rows" },
            .group_by = aggregate_group_by[0..],
            .aggregations = aggregate_specs[0..],
        },
    }, .read_index)).?;
    defer aggregate_result.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), aggregate_result.total_groups);
    try std.testing.expectEqualStrings("{\"status\":\"open\",\"order_count\":2,\"amount_sum\":8}", aggregate_result.rows[0]);

    const window_specs = [_]db_mod.types.RelationalRowsWindowSpec{.{
        .output = "rn",
        .function = .row_number,
        .order_by = &.{.{ .field = "amount", .direction = .desc }},
    }};
    const window_select = [_][]const u8{ "id", "amount" };
    var window_result = (try source.rowsWindowPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .ranges = ranges[0..],
        .window = .{
            .source = .{ .source_cte = "open_rows" },
            .windows = window_specs[0..],
            .select = window_select[0..],
            .select_all = false,
            .order_by = &.{.{ .field = "rn", .direction = .asc }},
        },
    }, .read_index)).?;
    defer window_result.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 2), window_result.total_rows);
    try std.testing.expectEqualStrings("{\"id\":\"z\",\"amount\":7,\"rn\":1}", window_result.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"a\",\"amount\":1,\"rn\":2}", window_result.rows[1]);

    const join_on = [_]db_mod.types.RelationalRowsJoinOn{.{
        .left_field = "status",
        .right_field = "status",
    }};
    const join_select = [_]db_mod.types.RelationalRowsJoinProjection{
        .{ .output = "left_id", .side = .left, .field = "id" },
        .{ .output = "right_id", .side = .right, .field = "id" },
    };
    var join_result = (try source.rowsJoinPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .join = .{
            .left = .{ .source_cte = "open_rows" },
            .right = .{},
            .on = join_on[0..],
            .select = join_select[0..],
            .order_by = &.{ .{ .field = "left_id", .direction = .asc }, .{ .field = "right_id", .direction = .asc } },
            .limit = 1,
        },
    }, .read_index)).?;
    defer join_result.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 4), join_result.total_rows);
    try std.testing.expectEqualStrings("{\"left_id\":\"a\",\"right_id\":\"a\"}", join_result.rows[0]);

    const lateral_correlations = [_]db_mod.types.RelationalRowsLateralCorrelation{.{
        .left_field = "status",
        .right_field = "status",
    }};
    const lateral_select = [_]db_mod.types.RelationalRowsJoinProjection{
        .{ .output = "left_id", .side = .left, .field = "id" },
        .{ .output = "latest_id", .side = .right, .field = "id" },
        .{ .output = "latest_amount", .side = .right, .field = "amount" },
    };
    var lateral_result = (try source.rowsLateralPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .lateral = .{
            .left = .{ .source_cte = "open_rows", .order_by = &.{.{ .field = "id", .direction = .asc }} },
            .right = .{ .order_by = &.{.{ .field = "amount", .direction = .desc }}, .limit = 1 },
            .correlations = lateral_correlations[0..],
            .select = lateral_select[0..],
            .order_by = &.{.{ .field = "left_id", .direction = .asc }},
        },
    }, .read_index)).?;
    defer lateral_result.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 2), lateral_result.total_rows);
    try std.testing.expectEqualStrings("{\"left_id\":\"a\",\"latest_id\":\"z\",\"latest_amount\":7}", lateral_result.rows[0]);
    try std.testing.expectEqualStrings("{\"left_id\":\"z\",\"latest_id\":\"z\",\"latest_amount\":7}", lateral_result.rows[1]);

    var customer_columns = [_]storage_schema.RelationalColumn{
        .{ .name = "id", .path = "id", .field_type = .keyword, .nullable = false },
        .{ .name = "status", .path = "status", .field_type = .keyword, .nullable = false },
        .{ .name = "name", .path = "name", .field_type = .keyword, .nullable = false },
    };
    const customer_schema = storage_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = customer_columns[0..],
    };
    const cross_join_select = [_]db_mod.types.RelationalRowsJoinProjection{
        .{ .output = "order_id", .side = .left, .field = "id" },
        .{ .output = "customer_name", .side = .right, .field = "name" },
    };
    const cross_scan_calls_before = fake.scan_calls;
    var cross_join_result = (try rowsJoinPlanFromRoutedScansWithSchemasAlloc(alloc, source, "orders", "orders", "customers", schema, schema, customer_schema, .{
        .right_table = "customers",
        .join = .{
            .left = .{},
            .right = .{},
            .on = join_on[0..],
            .select = cross_join_select[0..],
            .order_by = &.{.{ .field = "order_id", .direction = .asc }},
        },
    }, .read_index)).?;
    defer cross_join_result.deinit(alloc);
    try std.testing.expectEqual(cross_scan_calls_before + 2, fake.scan_calls);
    try std.testing.expectEqual(@as(u32, 3), cross_join_result.total_rows);
    try std.testing.expectEqualStrings("{\"order_id\":\"a\",\"customer_name\":\"Ada\"}", cross_join_result.rows[0]);
    try std.testing.expectEqualStrings("{\"order_id\":\"b\",\"customer_name\":\"Grace\"}", cross_join_result.rows[1]);
    try std.testing.expectEqualStrings("{\"order_id\":\"z\",\"customer_name\":\"Ada\"}", cross_join_result.rows[2]);

    const cross_lateral_select = [_]db_mod.types.RelationalRowsJoinProjection{
        .{ .output = "order_id", .side = .left, .field = "id" },
        .{ .output = "matched_customer", .side = .right, .field = "name" },
    };
    const cross_lateral_scan_calls_before = fake.scan_calls;
    var cross_lateral_result = (try rowsLateralPlanFromRoutedScansWithSchemasAlloc(alloc, source, "orders", "orders", "customers", schema, schema, customer_schema, .{
        .right_table = "customers",
        .lateral = .{
            .left = .{ .order_by = &.{.{ .field = "id", .direction = .asc }} },
            .right = .{ .order_by = &.{.{ .field = "name", .direction = .asc }}, .limit = 1 },
            .correlations = lateral_correlations[0..],
            .select = cross_lateral_select[0..],
            .order_by = &.{.{ .field = "order_id", .direction = .asc }},
        },
    }, .read_index)).?;
    defer cross_lateral_result.deinit(alloc);
    try std.testing.expectEqual(cross_lateral_scan_calls_before + 2, fake.scan_calls);
    try std.testing.expectEqual(@as(u32, 3), cross_lateral_result.total_rows);
    try std.testing.expectEqualStrings("{\"order_id\":\"a\",\"matched_customer\":\"Ada\"}", cross_lateral_result.rows[0]);
    try std.testing.expectEqualStrings("{\"order_id\":\"b\",\"matched_customer\":\"Grace\"}", cross_lateral_result.rows[1]);
    try std.testing.expectEqualStrings("{\"order_id\":\"z\",\"matched_customer\":\"Ada\"}", cross_lateral_result.rows[2]);

    var key_columns = [_]storage_schema.RelationalColumn{
        .{ .name = "key", .path = "key", .field_type = .keyword, .nullable = false },
    };
    const key_schema = storage_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = key_columns[0..],
    };
    try std.testing.expectError(error.UnsupportedRowsQuery, source.rowsQueryPlan(alloc, "orders", key_schema, .{}, .read_index));
    try std.testing.expectError(error.UnsupportedRowsQuery, source.rowsQueryPlan(alloc, "oversized", schema, .{}, .read_index));
}

test "lowered sql cross-table read plans execute through routed scans" {
    const alloc = std.testing.allocator;
    const orders_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"customer_id":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const customers_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"name":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;

    var parsed_orders = try schema_api.parseValidatedTableSchema(alloc, orders_schema_json);
    defer parsed_orders.deinit(alloc);
    const orders_schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_orders);
    defer storage_schema.freeSchema(alloc, orders_schema);

    var parsed_customers = try schema_api.parseValidatedTableSchema(alloc, customers_schema_json);
    defer parsed_customers.deinit(alloc);
    const customers_schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_customers);
    defer storage_schema.freeSchema(alloc, customers_schema);

    const FakeCatalog = struct {
        tables: [2]metadata_table_manager.TableRecord = .{
            .{ .table_id = 7, .name = "orders", .schema_json = orders_schema_json, .placement_role = "data" },
            .{ .table_id = 8, .name = "customers", .schema_json = customers_schema_json, .placement_role = "data" },
        },

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
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = self.tables[0..],
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeRoutedSource = struct {
        scan_calls: usize = 0,

        fn source(self: *@This()) TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .rows_query_plan = rowsQueryPlan,
                    .rows_set_operation_plan_catalog = rowsSetOperationPlanCatalog,
                },
            };
        }

        fn lookup(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            return null;
        }

        fn query(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.SearchRequest,
            _: raft_mod.ReadConsistency,
        ) !?query_api.QueryResponse {
            return null;
        }

        fn scan(
            ptr: *anyopaque,
            scan_alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: db_mod.types.ScanOptions,
            _: raft_mod.ReadConsistency,
        ) !?ScanResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(opts.include_documents);
            try std.testing.expect(opts.include_all_fields);
            try std.testing.expectEqualStrings("", from_key);
            try std.testing.expectEqualStrings("", to_key);
            self.scan_calls += 1;
            const ndjson = if (std.mem.eql(u8, table_name, "orders"))
                "{\"key\":\"o1\",\"id\":\"o1\",\"status\":\"open\",\"customer_id\":\"c1\",\"amount\":10}\n{\"key\":\"o2\",\"id\":\"o2\",\"status\":\"closed\",\"customer_id\":\"c2\",\"amount\":5}\n"
            else if (std.mem.eql(u8, table_name, "customers"))
                "{\"key\":\"c1\",\"id\":\"c1\",\"status\":\"open\",\"name\":\"Ada\"}\n{\"key\":\"c2\",\"id\":\"c2\",\"status\":\"closed\",\"name\":\"Grace\"}\n"
            else
                return error.TableNotFound;
            return .{ .ndjson = try scan_alloc.dupe(u8, ndjson) };
        }

        fn rowsQueryPlan(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsQueryPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsQueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try rowsQueryPlanFromRoutedScansAlloc(plan_alloc, self.source(), table_name, runtime_schema, plan, consistency);
        }

        fn rowsSetOperationPlanCatalog(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            target: catalog_resources.TableTarget,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsSetOperationPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsQueryResult {
            try std.testing.expectEqualStrings(catalog_resources.default_database_name, target.database_name);
            try std.testing.expectEqualStrings(catalog_resources.default_namespace_name, target.namespace_name);
            try std.testing.expectEqual(@as(?u32, db_mod.types.default_relational_rows_cte_max_rows), plan.max_rows);
            try std.testing.expectEqual(@as(?u64, db_mod.types.default_relational_rows_cte_max_bytes), plan.max_bytes);
            try std.testing.expectEqual(@as(?u64, db_mod.types.default_relational_rows_cte_spill_after_bytes), plan.spill_after_bytes);
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try rowsSetOperationPlanFromRoutedScansAlloc(plan_alloc, self.source(), target.table_name, runtime_schema, plan, consistency);
        }
    };

    var catalog = FakeCatalog{};
    var fake = FakeRoutedSource{};
    var lowered = try sql_adapter_runtime.lowerReadPlanWithCatalogAlloc(
        alloc,
        "SELECT o.id AS order_id, c.name AS customer_name FROM orders AS o LEFT JOIN customers AS c ON o.status = c.status ORDER BY order_id ASC",
        orders_schema,
        &.{},
        catalog.iface(),
    );
    defer lowered.deinit(alloc);

    var result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        fake.source(),
        catalog.iface(),
        "orders",
        orders_schema,
        lowered,
        .read_index,
    )).?;
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), fake.scan_calls);
    switch (result) {
        .join => |join_result| {
            try std.testing.expectEqual(@as(u32, 2), join_result.total_rows);
            try std.testing.expectEqual(@as(usize, 2), join_result.rows.len);
            try std.testing.expectEqualStrings("{\"order_id\":\"o1\",\"customer_name\":\"Ada\"}", join_result.rows[0]);
            try std.testing.expectEqualStrings("{\"order_id\":\"o2\",\"customer_name\":\"Grace\"}", join_result.rows[1]);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "lowered sql set operation plans preserve overlapping union all rows" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"enabled":{"type":"boolean"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;

    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer storage_schema.freeSchema(alloc, schema);

    const FakeCatalog = struct {
        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeSource = struct {
        scan_calls: usize = 0,

        fn source(self: *@This()) TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .rows_query_plan = rowsQueryPlan,
                    .rows_set_operation_plan_catalog = rowsSetOperationPlanCatalog,
                },
            };
        }

        fn lookup(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            return null;
        }

        fn query(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.SearchRequest,
            _: raft_mod.ReadConsistency,
        ) !?query_api.QueryResponse {
            return null;
        }

        fn scan(
            ptr: *anyopaque,
            scan_alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: db_mod.types.ScanOptions,
            _: raft_mod.ReadConsistency,
        ) !?ScanResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(opts.include_documents);
            try std.testing.expect(opts.include_all_fields);
            try std.testing.expectEqualStrings("usage_records", table_name);
            try std.testing.expectEqualStrings("", from_key);
            try std.testing.expectEqualStrings("", to_key);
            self.scan_calls += 1;
            const ndjson =
                "{\"key\":\"u1\",\"id\":\"u1\",\"status\":\"open\",\"enabled\":true}\n" ++
                "{\"key\":\"u2\",\"id\":\"u2\",\"status\":\"open\",\"enabled\":false}\n" ++
                "{\"key\":\"u3\",\"id\":\"u3\",\"status\":\"closed\",\"enabled\":true}\n";
            return .{ .ndjson = try scan_alloc.dupe(u8, ndjson) };
        }

        fn rowsQueryPlan(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsQueryPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsQueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try rowsQueryPlanFromRoutedScansAlloc(plan_alloc, self.source(), table_name, runtime_schema, plan, consistency);
        }

        fn rowsSetOperationPlanCatalog(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            target: catalog_resources.TableTarget,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsSetOperationPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsQueryResult {
            try std.testing.expectEqualStrings(catalog_resources.default_database_name, target.database_name);
            try std.testing.expectEqualStrings(catalog_resources.default_namespace_name, target.namespace_name);
            try std.testing.expectEqual(@as(?u32, db_mod.types.default_relational_rows_cte_max_rows), plan.max_rows);
            try std.testing.expectEqual(@as(?u64, db_mod.types.default_relational_rows_cte_max_bytes), plan.max_bytes);
            try std.testing.expectEqual(@as(?u64, db_mod.types.default_relational_rows_cte_spill_after_bytes), plan.spill_after_bytes);
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try rowsSetOperationPlanFromRoutedScansAlloc(plan_alloc, self.source(), target.table_name, runtime_schema, plan, consistency);
        }
    };

    var lowered = try sql_adapter_runtime.lowerReadPlanAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status = 'open' UNION ALL SELECT id FROM usage_records WHERE enabled IS TRUE",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);
    switch (lowered) {
        .set_operation => |set_operation| {
            try std.testing.expectEqual(@as(?u32, db_mod.types.default_relational_rows_cte_max_rows), set_operation.max_rows);
            try std.testing.expectEqual(@as(?u64, db_mod.types.default_relational_rows_cte_max_bytes), set_operation.max_bytes);
            try std.testing.expectEqual(@as(?u64, db_mod.types.default_relational_rows_cte_spill_after_bytes), set_operation.spill_after_bytes);
        },
        else => return error.TestUnexpectedResult,
    }

    var catalog = FakeCatalog{};
    var fake = FakeSource{};
    var result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        fake.source(),
        catalog.iface(),
        "usage_records",
        schema,
        lowered,
        .read_index,
    )).?;
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), fake.scan_calls);
    switch (result) {
        .set_operation => |query_result| {
            try std.testing.expectEqual(@as(u32, 4), query_result.total);
            try std.testing.expectEqual(@as(usize, 4), query_result.rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"u1\"}", query_result.rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"u2\"}", query_result.rows[1]);
            try std.testing.expectEqualStrings("{\"id\":\"u1\"}", query_result.rows[2]);
            try std.testing.expectEqualStrings("{\"id\":\"u3\"}", query_result.rows[3]);
        },
        else => return error.TestUnexpectedResult,
    }

    var lowered_distinct = try sql_adapter_runtime.lowerReadPlanAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status = 'open' UNION SELECT id FROM usage_records WHERE enabled IS TRUE",
        schema,
        &.{},
    );
    defer lowered_distinct.deinit(alloc);
    switch (lowered_distinct) {
        .set_operation => |set_operation| {
            try std.testing.expectEqual(@as(sql_adapter_runtime.SelectSetOperation, .union_distinct), set_operation.operation);
            try std.testing.expectEqualStrings("usage_records", set_operation.left.table_name);
            try std.testing.expectEqualStrings("usage_records", set_operation.right.table_name);
        },
        else => return error.TestUnexpectedResult,
    }

    var fake_distinct = FakeSource{};
    var distinct_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        fake_distinct.source(),
        catalog.iface(),
        "usage_records",
        schema,
        lowered_distinct,
        .read_index,
    )).?;
    defer distinct_result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), fake_distinct.scan_calls);
    switch (distinct_result) {
        .set_operation => |query_result| {
            try std.testing.expectEqual(@as(u32, 3), query_result.total);
            try std.testing.expectEqual(@as(usize, 3), query_result.rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"u1\"}", query_result.rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"u2\"}", query_result.rows[1]);
            try std.testing.expectEqualStrings("{\"id\":\"u3\"}", query_result.rows[2]);
        },
        else => return error.TestUnexpectedResult,
    }

    var lowered_intersect = try sql_adapter_runtime.lowerReadPlanAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status = 'open' INTERSECT SELECT id FROM usage_records WHERE enabled IS TRUE",
        schema,
        &.{},
    );
    defer lowered_intersect.deinit(alloc);
    switch (lowered_intersect) {
        .set_operation => |set_operation| {
            try std.testing.expectEqual(@as(sql_adapter_runtime.SelectSetOperation, .intersect), set_operation.operation);
            try std.testing.expectEqualStrings("usage_records", set_operation.left.table_name);
            try std.testing.expectEqualStrings("usage_records", set_operation.right.table_name);
        },
        else => return error.TestUnexpectedResult,
    }

    var fake_intersect = FakeSource{};
    var intersect_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        fake_intersect.source(),
        catalog.iface(),
        "usage_records",
        schema,
        lowered_intersect,
        .read_index,
    )).?;
    defer intersect_result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), fake_intersect.scan_calls);
    switch (intersect_result) {
        .set_operation => |query_result| {
            try std.testing.expectEqual(@as(u32, 1), query_result.total);
            try std.testing.expectEqual(@as(usize, 1), query_result.rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"u1\"}", query_result.rows[0]);
        },
        else => return error.TestUnexpectedResult,
    }

    var lowered_tail = try sql_adapter_runtime.lowerReadPlanAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status = 'open' UNION ALL SELECT id FROM usage_records WHERE enabled IS TRUE ORDER BY id DESC LIMIT 2 OFFSET 1",
        schema,
        &.{},
    );
    defer lowered_tail.deinit(alloc);
    switch (lowered_tail) {
        .set_operation => {},
        else => return error.TestUnexpectedResult,
    }

    var fake_tail = FakeSource{};
    var tail_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        fake_tail.source(),
        catalog.iface(),
        "usage_records",
        schema,
        lowered_tail,
        .read_index,
    )).?;
    defer tail_result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), fake_tail.scan_calls);
    switch (tail_result) {
        .set_operation => |query_result| {
            try std.testing.expectEqual(@as(u32, 4), query_result.total);
            try std.testing.expectEqual(@as(usize, 2), query_result.rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"u2\"}", query_result.rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"u1\"}", query_result.rows[1]);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "lowered sql set operation plans route cross table branches through catalog schemas" {
    const alloc = std.testing.allocator;
    const usage_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"enabled":{"type":"boolean"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const archived_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"enabled":{"type":"boolean"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;

    var parsed_usage = try schema_api.parseValidatedTableSchema(alloc, usage_schema_json);
    defer parsed_usage.deinit(alloc);
    const usage_schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_usage);
    defer storage_schema.freeSchema(alloc, usage_schema);

    var parsed_archived = try schema_api.parseValidatedTableSchema(alloc, archived_schema_json);
    defer parsed_archived.deinit(alloc);
    const archived_schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_archived);
    defer storage_schema.freeSchema(alloc, archived_schema);

    const FakeCatalog = struct {
        tables: [2]metadata_table_manager.TableRecord = .{
            .{ .table_id = 31, .name = "usage_records", .schema_json = usage_schema_json, .placement_role = "data" },
            .{ .table_id = 32, .name = "archived_records", .schema_json = archived_schema_json, .placement_role = "data" },
        },

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
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = self.tables[0..],
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeSource = struct {
        usage_scans: usize = 0,
        archived_scans: usize = 0,
        usage_catalog_queries: usize = 0,
        archived_catalog_queries: usize = 0,
        expected_database_name: []const u8 = catalog_resources.default_database_name,
        expected_namespace_name: []const u8 = catalog_resources.default_namespace_name,

        fn source(self: *@This()) TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .rows_query_plan = rowsQueryPlan,
                    .rows_query_plan_catalog = rowsQueryPlanCatalog,
                },
            };
        }

        fn lookup(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            return null;
        }

        fn query(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.SearchRequest,
            _: raft_mod.ReadConsistency,
        ) !?query_api.QueryResponse {
            return null;
        }

        fn scan(
            ptr: *anyopaque,
            scan_alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: db_mod.types.ScanOptions,
            _: raft_mod.ReadConsistency,
        ) !?ScanResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(opts.include_documents);
            try std.testing.expect(opts.include_all_fields);
            try std.testing.expectEqualStrings("", from_key);
            try std.testing.expectEqualStrings("", to_key);
            if (std.mem.eql(u8, table_name, "usage_records")) {
                self.usage_scans += 1;
                const ndjson =
                    "{\"key\":\"u1\",\"id\":\"u1\",\"status\":\"open\",\"enabled\":true}\n" ++
                    "{\"key\":\"u2\",\"id\":\"u2\",\"status\":\"open\",\"enabled\":false}\n" ++
                    "{\"key\":\"u3\",\"id\":\"u3\",\"status\":\"closed\",\"enabled\":true}\n";
                return .{ .ndjson = try scan_alloc.dupe(u8, ndjson) };
            }
            if (std.mem.eql(u8, table_name, "archived_records")) {
                self.archived_scans += 1;
                const ndjson =
                    "{\"key\":\"a1\",\"id\":\"a1\",\"status\":\"archived\",\"enabled\":true}\n" ++
                    "{\"key\":\"a2\",\"id\":\"a2\",\"status\":\"archived\",\"enabled\":false}\n" ++
                    "{\"key\":\"a3\",\"id\":\"a3\",\"status\":\"deleted\",\"enabled\":true}\n" ++
                    "{\"key\":\"u2\",\"id\":\"u2\",\"status\":\"deleted\",\"enabled\":false}\n";
                return .{ .ndjson = try scan_alloc.dupe(u8, ndjson) };
            }
            return error.TableNotFound;
        }

        fn rowsQueryPlan(
            _: *anyopaque,
            _: std.mem.Allocator,
            table_name: []const u8,
            _: storage_schema.TableSchema,
            _: db_mod.types.RelationalRowsQueryPlan,
            _: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsQueryResult {
            _ = table_name;
            return error.TestUnexpectedResult;
        }

        fn rowsQueryPlanCatalog(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            target: catalog_resources.TableTarget,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsQueryPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsQueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings(self.expected_database_name, target.database_name);
            try std.testing.expectEqualStrings(self.expected_namespace_name, target.namespace_name);
            if (std.mem.eql(u8, target.table_name, "usage_records")) {
                self.usage_catalog_queries += 1;
            } else if (std.mem.eql(u8, target.table_name, "archived_records")) {
                self.archived_catalog_queries += 1;
            } else {
                return error.TableNotFound;
            }
            return try rowsQueryPlanFromRoutedScansAlloc(plan_alloc, self.source(), target.table_name, runtime_schema, plan, consistency);
        }
    };

    var catalog = FakeCatalog{};
    var lowered = try sql_adapter_runtime.lowerReadPlanWithCatalogAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status = 'open' UNION ALL SELECT id FROM archived_records WHERE enabled IS TRUE",
        usage_schema,
        &.{},
        catalog.iface(),
    );
    defer lowered.deinit(alloc);
    switch (lowered) {
        .set_operation => {},
        else => return error.TestUnexpectedResult,
    }

    var fake = FakeSource{};
    var result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        fake.source(),
        catalog.iface(),
        "usage_records",
        usage_schema,
        lowered,
        .read_index,
    )).?;
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), fake.usage_scans);
    try std.testing.expectEqual(@as(usize, 1), fake.archived_scans);
    try std.testing.expectEqual(@as(usize, 1), fake.usage_catalog_queries);
    try std.testing.expectEqual(@as(usize, 1), fake.archived_catalog_queries);
    switch (result) {
        .set_operation => |query_result| {
            try std.testing.expectEqual(@as(u32, 4), query_result.total);
            try std.testing.expectEqual(@as(usize, 4), query_result.rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"u1\"}", query_result.rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"u2\"}", query_result.rows[1]);
            try std.testing.expectEqualStrings("{\"id\":\"a1\"}", query_result.rows[2]);
            try std.testing.expectEqualStrings("{\"id\":\"a3\"}", query_result.rows[3]);
        },
        else => return error.TestUnexpectedResult,
    }

    var tenant_fake = FakeSource{
        .expected_database_name = "tenant_ops",
        .expected_namespace_name = "analytics",
    };
    const tenant_session: catalog_resources.SqlCatalogSession = .{
        .current_database_name = "tenant_ops",
        .search_path = &.{ "analytics", "public" },
    };
    var tenant_result = (try executeLoweredSqlReadPlanWithSessionAlloc(
        alloc,
        tenant_fake.source(),
        catalog.iface(),
        tenant_session,
        "usage_records",
        usage_schema,
        lowered,
        .read_index,
    )).?;
    defer tenant_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), tenant_fake.usage_catalog_queries);
    try std.testing.expectEqual(@as(usize, 1), tenant_fake.archived_catalog_queries);
    switch (tenant_result) {
        .set_operation => |query_result| {
            try std.testing.expectEqual(@as(u32, 4), query_result.total);
            try std.testing.expectEqual(@as(usize, 4), query_result.rows.len);
        },
        else => return error.TestUnexpectedResult,
    }

    var lowered_except = try sql_adapter_runtime.lowerReadPlanWithCatalogAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status = 'open' EXCEPT SELECT id FROM archived_records WHERE status = 'deleted'",
        usage_schema,
        &.{},
        catalog.iface(),
    );
    defer lowered_except.deinit(alloc);
    switch (lowered_except) {
        .set_operation => |set_operation| try std.testing.expectEqualStrings("except", @tagName(set_operation.operation)),
        else => return error.TestUnexpectedResult,
    }

    var fake_except = FakeSource{};
    var except_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        fake_except.source(),
        catalog.iface(),
        "usage_records",
        usage_schema,
        lowered_except,
        .read_index,
    )).?;
    defer except_result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), fake_except.usage_scans);
    try std.testing.expectEqual(@as(usize, 1), fake_except.archived_scans);
    try std.testing.expectEqual(@as(usize, 1), fake_except.usage_catalog_queries);
    try std.testing.expectEqual(@as(usize, 1), fake_except.archived_catalog_queries);
    switch (except_result) {
        .set_operation => |query_result| {
            try std.testing.expectEqual(@as(u32, 1), query_result.total);
            try std.testing.expectEqual(@as(usize, 1), query_result.rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"u1\"}", query_result.rows[0]);
        },
        else => return error.TestUnexpectedResult,
    }

    var lowered_intersect = try sql_adapter_runtime.lowerReadPlanWithCatalogAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status = 'open' INTERSECT SELECT id FROM archived_records WHERE status = 'deleted'",
        usage_schema,
        &.{},
        catalog.iface(),
    );
    defer lowered_intersect.deinit(alloc);
    switch (lowered_intersect) {
        .set_operation => |set_operation| try std.testing.expectEqualStrings("intersect", @tagName(set_operation.operation)),
        else => return error.TestUnexpectedResult,
    }

    var fake_intersect = FakeSource{};
    var intersect_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        fake_intersect.source(),
        catalog.iface(),
        "usage_records",
        usage_schema,
        lowered_intersect,
        .read_index,
    )).?;
    defer intersect_result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), fake_intersect.usage_scans);
    try std.testing.expectEqual(@as(usize, 1), fake_intersect.archived_scans);
    try std.testing.expectEqual(@as(usize, 1), fake_intersect.usage_catalog_queries);
    try std.testing.expectEqual(@as(usize, 1), fake_intersect.archived_catalog_queries);
    switch (intersect_result) {
        .set_operation => |query_result| {
            try std.testing.expectEqual(@as(u32, 1), query_result.total);
            try std.testing.expectEqual(@as(usize, 1), query_result.rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"u2\"}", query_result.rows[0]);
        },
        else => return error.TestUnexpectedResult,
    }
}
