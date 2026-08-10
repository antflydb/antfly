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
const db_relational_rows = @import("../../storage/db/relational_rows.zig");
const storage_schema = @import("../../storage/schema.zig");
const document_sql_runtime = @import("../../sql/document_runtime.zig");
const sql_adapter = @import("../../sql/mod.zig");
const platform_time = @import("antfly_platform").time;
const raft_mod = @import("../../raft/domain.zig");
const raft_reconciler = @import("../../raft/reconciler.zig");
const schema_api = @import("../../schema/mod.zig");
const table_catalog = @import("../../metadata/catalog/routing.zig");
const catalog_resources = @import("../catalog_resources.zig");
const relational_rows_api = @import("../../sql/relational_rows.zig");
const row_spill = @import("../../sql/row_spill.zig");
const query_api = @import("../query.zig");
const core = @import("core.zig");
const document_sql = @import("document_sql.zig");

const TableReadSource = core.TableReadSource;
const LookupResponse = core.LookupResponse;
const ScanResponse = core.ScanResponse;
const appendScanLine = core.appendScanLine;
const nativeCatalogTableNameAlloc = table_catalog.nativeTableNameForCatalogTargetAlloc;

fn uniqueTestTmpPathAlloc(alloc: std.mem.Allocator, prefix: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "/tmp/{s}-{d}", .{ prefix, platform_time.monotonicNs() });
}

fn recursiveDmlFullRowSourceQuery(req: db_mod.types.RelationalRowsQueryRequest) db_mod.types.RelationalRowsQueryRequest {
    var query = req;
    query.source_cte = "";
    query.select = &.{};
    query.json_extract = &.{};
    query.array_length = &.{};
    query.coalesce = &.{};
    query.field_aliases = &.{};
    query.expressions = &.{};
    query.select_all = true;
    query.row_claim = null;
    return query;
}

fn dmlFullRowSourceQuery(req: db_mod.types.RelationalRowsQueryRequest) db_mod.types.RelationalRowsQueryRequest {
    var query = req;
    query.select = &.{};
    query.json_extract = &.{};
    query.array_length = &.{};
    query.coalesce = &.{};
    query.field_aliases = &.{};
    query.expressions = &.{};
    query.select_all = true;
    query.row_claim = null;
    return query;
}

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
    mode: sql_adapter.RelationPopulationMode,
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
    plan: sql_adapter.LoweredReadPlan,
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
    plan: sql_adapter.LoweredReadPlan,
    consistency: raft_mod.ReadConsistency,
) !?LoweredSqlReadPlanResult {
    return switch (plan) {
        .query => |lowered| blk: {
            const owned_schema = try catalogRuntimeSchemaUnlessDefaultAlloc(alloc, catalog, default_table_name, lowered.table_name);
            defer if (owned_schema) |schema| storage_schema.freeSchema(alloc, schema);
            const runtime_schema = owned_schema orelse default_schema;
            var result = if (lowered.system_time_as_of_sequence) |commit_sequence| result_blk: {
                if (lowered.system_time_as_of_timestamp_ns != null) return error.UnsupportedRowsQuery;
                const target = try catalogTargetForLoweredSqlTable(session, default_table_name, lowered.table_name);
                break :result_blk (try source.rowsQueryPlanCatalogSystemTimeAsOfSequence(alloc, target, runtime_schema, commit_sequence, lowered.plan, consistency)) orelse break :blk null;
            } else if (lowered.system_time_as_of_timestamp_ns) |timestamp_ns| result_blk: {
                const target = try catalogTargetForLoweredSqlTable(session, default_table_name, lowered.table_name);
                break :result_blk (try source.rowsQueryPlanCatalogSystemTimeAsOfTimestampNs(alloc, target, runtime_schema, timestamp_ns, lowered.plan, consistency)) orelse break :blk null;
            } else result_blk: {
                break :result_blk (try source.rowsQueryPlan(alloc, lowered.table_name, runtime_schema, lowered.plan, consistency)) orelse break :blk null;
            };
            errdefer result.deinit(alloc);
            break :blk .{ .query = result };
        },
        .document_query => |lowered| blk: {
            const catalog_table_name = if (lowered.view_mapping) |view_mapping|
                view_mapping.source_table
            else
                lowered.table_name;
            const target = try catalogTargetForLoweredSqlTable(session, default_table_name, catalog_table_name);
            const owned_schema = try catalogRuntimeSchemaUnlessDefaultAlloc(alloc, catalog, default_table_name, catalog_table_name);
            defer if (owned_schema) |schema| storage_schema.freeSchema(alloc, schema);
            const runtime_schema = owned_schema orelse default_schema;
            if (try executeRelationalFullTextDocumentReadAsRowsAlloc(alloc, source, lowered, runtime_schema, consistency)) |result| {
                var owned_result = result;
                errdefer owned_result.deinit(alloc);
                break :blk .{ .query = owned_result };
            }
            const native_table_name = try nativeCatalogTableNameAlloc(alloc, catalog, target);
            defer alloc.free(native_table_name);
            var adapter = document_sql.RuntimeSourceAdapter{
                .source = source,
                .target = target,
                .native_table_name = native_table_name,
                .public_table_name = lowered.table_name,
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

fn executeRelationalFullTextDocumentReadAsRowsAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    lowered: sql_adapter.DocumentReadPlan,
    runtime_schema: storage_schema.TableSchema,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsQueryResult {
    if (runtime_schema.storage_mode != .relational or runtime_schema.primary_key == null) return null;
    var plan = (relationalRowsPlanFromDocumentFullTextReadAlloc(alloc, lowered, runtime_schema) catch |err| switch (err) {
        error.UnsupportedRowsQuery => return null,
        else => return err,
    }) orelse return null;
    defer plan.deinit(alloc);
    return try source.rowsQueryPlan(alloc, lowered.table_name, runtime_schema, plan, consistency);
}

fn relationalRowsPlanFromDocumentFullTextReadAlloc(
    alloc: std.mem.Allocator,
    lowered: sql_adapter.DocumentReadPlan,
    runtime_schema: storage_schema.TableSchema,
) !?db_mod.types.RelationalRowsQueryPlan {
    if (lowered.view_mapping != null or lowered.unnest != null or lowered.lateral_subquery != null or lowered.order_by != null) return null;
    if (lowered.producer != .indexed_query) return null;
    const indexed = lowered.producer.indexed_query;
    if (indexed.native_query_json != null or indexed.full_text_query == null) return null;

    var request = db_mod.types.RelationalRowsQueryRequest{
        .select_all = false,
        .limit = lowered.limit,
    };
    errdefer request.deinit(alloc);

    if (indexed.index_name) |index_name| {
        request.primary_text_index_name = try alloc.dupe(u8, index_name);
    }
    request.full_text = try relationalRowsTextQueryFromDocumentFullTextStringAlloc(alloc, runtime_schema, indexed.full_text_query.?);

    try appendRelationalRowsProjectionFromDocumentReadAlloc(alloc, runtime_schema, lowered, &request);
    try appendRelationalRowsPredicatesFromDocumentFilterJsonAlloc(alloc, runtime_schema, indexed.filter_query_json, &request);
    try appendRelationalRowsPredicatesFromDocumentFilterJsonAlloc(alloc, runtime_schema, indexed.residual_filter_json, &request);

    return .{ .query = request };
}

fn appendRelationalRowsProjectionFromDocumentReadAlloc(
    alloc: std.mem.Allocator,
    runtime_schema: storage_schema.TableSchema,
    lowered: sql_adapter.DocumentReadPlan,
    request: *db_mod.types.RelationalRowsQueryRequest,
) !void {
    var select = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (select.items) |field| alloc.free(@constCast(field));
        select.deinit(alloc);
    }
    var aliases = std.ArrayListUnmanaged(db_mod.types.RelationalRowsFieldAliasProjection).empty;
    errdefer {
        for (aliases.items) |alias| {
            alloc.free(@constCast(alias.output));
            alloc.free(@constCast(alias.field));
        }
        aliases.deinit(alloc);
    }

    for (lowered.projection) |projection| {
        if (projection.kind != .field or projection.lateral) return error.UnsupportedRowsQuery;
        const field = relationalRowsFieldFromDocumentPath(runtime_schema, projection.field) orelse return error.UnsupportedRowsQuery;
        if (std.mem.eql(u8, projection.output, field)) {
            const owned_field = try alloc.dupe(u8, field);
            errdefer alloc.free(owned_field);
            try select.append(alloc, owned_field);
        } else {
            const output = try alloc.dupe(u8, projection.output);
            errdefer alloc.free(output);
            const owned_field = try alloc.dupe(u8, field);
            errdefer alloc.free(owned_field);
            try aliases.append(alloc, .{ .output = output, .field = owned_field });
        }
    }

    const owned_select = try select.toOwnedSlice(alloc);
    errdefer {
        for (owned_select) |field| alloc.free(@constCast(field));
        if (owned_select.len > 0) alloc.free(owned_select);
    }
    const owned_aliases = try aliases.toOwnedSlice(alloc);
    errdefer {
        for (owned_aliases) |alias| {
            alloc.free(@constCast(alias.output));
            alloc.free(@constCast(alias.field));
        }
        if (owned_aliases.len > 0) alloc.free(owned_aliases);
    }
    request.select = owned_select;
    request.field_aliases = owned_aliases;
}

fn appendRelationalRowsPredicatesFromDocumentFilterJsonAlloc(
    alloc: std.mem.Allocator,
    runtime_schema: storage_schema.TableSchema,
    maybe_filter_json: ?[]const u8,
    request: *db_mod.types.RelationalRowsQueryRequest,
) !void {
    const filter_json = maybe_filter_json orelse return;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, filter_json, .{}) catch return error.UnsupportedRowsQuery;
    defer parsed.deinit();
    if (parsed.value != .object) return error.UnsupportedRowsQuery;

    var predicates = std.ArrayListUnmanaged(storage_schema.RelationalCheck).empty;
    defer predicates.deinit(alloc);
    errdefer for (predicates.items) |predicate| freeRelationalRowsPredicateLocal(alloc, predicate);

    try appendRelationalRowsPredicatesFromDocumentFilterValueAlloc(alloc, runtime_schema, parsed.value, &predicates);
    if (predicates.items.len == 0) return;

    const old_len = request.predicates.len;
    const next = try alloc.alloc(storage_schema.RelationalCheck, old_len + predicates.items.len);
    errdefer alloc.free(next);
    if (old_len > 0) @memcpy(next[0..old_len], request.predicates);
    @memcpy(next[old_len..], predicates.items);
    if (old_len > 0) alloc.free(@constCast(request.predicates));
    request.predicates = next;
    predicates.items.len = 0;
}

fn appendRelationalRowsPredicatesFromDocumentFilterValueAlloc(
    alloc: std.mem.Allocator,
    runtime_schema: storage_schema.TableSchema,
    value: std.json.Value,
    predicates: *std.ArrayListUnmanaged(storage_schema.RelationalCheck),
) !void {
    if (value != .object) return error.UnsupportedRowsQuery;
    if (value.object.get("match_none") != null) return error.UnsupportedRowsQuery;
    if (value.object.get("conjuncts")) |conjuncts| {
        if (conjuncts != .array) return error.UnsupportedRowsQuery;
        for (conjuncts.array.items) |item| try appendRelationalRowsPredicatesFromDocumentFilterValueAlloc(alloc, runtime_schema, item, predicates);
        return;
    }
    if (value.object.get("bool")) |bool_value| {
        if (bool_value != .object) return error.UnsupportedRowsQuery;
        if (bool_value.object.get("filter")) |items| {
            if (items != .array) return error.UnsupportedRowsQuery;
            for (items.array.items) |item| try appendRelationalRowsPredicatesFromDocumentFilterValueAlloc(alloc, runtime_schema, item, predicates);
            return;
        }
        if (bool_value.object.get("must")) |items| {
            if (items != .array) return error.UnsupportedRowsQuery;
            for (items.array.items) |item| try appendRelationalRowsPredicatesFromDocumentFilterValueAlloc(alloc, runtime_schema, item, predicates);
            return;
        }
        return error.UnsupportedRowsQuery;
    }
    if (value.object.get("term")) |term| {
        try appendRelationalRowsTermPredicateFromDocumentFilterAlloc(alloc, runtime_schema, term, predicates);
        return;
    }
    return error.UnsupportedRowsQuery;
}

fn appendRelationalRowsTermPredicateFromDocumentFilterAlloc(
    alloc: std.mem.Allocator,
    runtime_schema: storage_schema.TableSchema,
    term: std.json.Value,
    predicates: *std.ArrayListUnmanaged(storage_schema.RelationalCheck),
) !void {
    if (term != .object) return error.UnsupportedRowsQuery;
    const path_value = term.object.get("path") orelse return error.UnsupportedRowsQuery;
    const value = term.object.get("value") orelse return error.UnsupportedRowsQuery;
    if (path_value != .string) return error.UnsupportedRowsQuery;
    const column = relationalRowsColumnFromDocumentPath(runtime_schema, path_value.string) orelse return error.UnsupportedRowsQuery;
    const field = try alloc.dupe(u8, column.name);
    errdefer alloc.free(field);
    const value_json = try jsonValueStringifyAllocLocal(alloc, value);
    errdefer alloc.free(value_json);
    const collation = if (column.collation) |name| try alloc.dupe(u8, name) else null;
    errdefer if (collation) |name| alloc.free(name);
    try predicates.append(alloc, .{
        .name = "",
        .field = field,
        .op = .eq,
        .value_json = value_json,
        .collation = collation,
    });
}

fn relationalRowsTextQueryFromDocumentFullTextStringAlloc(
    alloc: std.mem.Allocator,
    runtime_schema: storage_schema.TableSchema,
    query: []const u8,
) !db_mod.types.TextQuery {
    const colon = std.mem.indexOfScalar(u8, query, ':') orelse return error.UnsupportedRowsQuery;
    if (colon == 0) return error.UnsupportedRowsQuery;
    const raw_field = std.mem.trim(u8, query[0..colon], " \t\r\n");
    const text = std.mem.trim(u8, query[colon + 1 ..], " \t\r\n");
    if (raw_field.len == 0 or raw_field.len != colon or text.len == 0) return error.UnsupportedRowsQuery;
    for (raw_field) |ch| {
        if (!(std.ascii.isAlphanumeric(ch) or ch == '_' or ch == '-' or ch == '.' or ch == '/')) return error.UnsupportedRowsQuery;
    }
    const field = relationalRowsFieldFromDocumentPath(runtime_schema, raw_field) orelse return error.UnsupportedRowsQuery;
    const owned_field = try alloc.dupe(u8, field);
    errdefer alloc.free(owned_field);
    const owned_text = try alloc.dupe(u8, text);
    errdefer alloc.free(owned_text);
    return .{ .match = .{
        .field = owned_field,
        .text = owned_text,
    } };
}

fn relationalRowsFieldFromDocumentPath(
    runtime_schema: storage_schema.TableSchema,
    path: []const u8,
) ?[]const u8 {
    return if (relationalRowsColumnFromDocumentPath(runtime_schema, path)) |column| column.name else null;
}

fn relationalRowsColumnFromDocumentPath(
    runtime_schema: storage_schema.TableSchema,
    path: []const u8,
) ?storage_schema.RelationalColumn {
    const trimmed = if (std.mem.startsWith(u8, path, "/")) path[1..] else path;
    if (trimmed.len == 0 or std.mem.indexOfScalar(u8, trimmed, '/') != null) return null;
    for (runtime_schema.relational_columns) |column| {
        if (std.mem.eql(u8, column.name, trimmed) or std.mem.eql(u8, column.path, path) or std.mem.eql(u8, column.path, trimmed)) return column;
    }
    return null;
}

fn freeRelationalRowsPredicateLocal(alloc: std.mem.Allocator, predicate: storage_schema.RelationalCheck) void {
    if (predicate.field.len > 0) alloc.free(@constCast(predicate.field));
    if (predicate.value_json) |value_json| if (value_json.len > 0) alloc.free(@constCast(value_json));
    if (predicate.collation) |collation| if (collation.len > 0) alloc.free(@constCast(collation));
}

fn jsonValueStringifyAllocLocal(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) ![]const u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    try std.json.Stringify.value(value, .{}, &out.writer);
    return try out.toOwnedSlice();
}

test "relational full text document read lowers to rows query request" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"title":{"type":"text"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const runtime_schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer storage_schema.freeSchema(alloc, runtime_schema);

    var projections = [_]sql_adapter.DocumentProjection{
        .{ .kind = .field, .field = "id", .output = "id" },
        .{ .kind = .field, .field = "title", .output = "headline" },
    };
    const lowered = sql_adapter.DocumentReadPlan{
        .table_name = "articles",
        .projection = projections[0..],
        .producer = .{ .indexed_query = .{
            .index_name = "title_ft",
            .full_text_query = "title:alpha",
            .filter_query_json = "{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}",
        } },
        .limit = 10,
    };

    var plan = (try relationalRowsPlanFromDocumentFullTextReadAlloc(alloc, lowered, runtime_schema)) orelse return error.TestUnexpectedResult;
    defer plan.deinit(alloc);

    try std.testing.expectEqual(@as(?u32, 10), plan.query.limit);
    try std.testing.expectEqualStrings("title_ft", plan.query.primary_text_index_name.?);
    try std.testing.expectEqual(@as(usize, 1), plan.query.select.len);
    try std.testing.expectEqualStrings("id", plan.query.select[0]);
    try std.testing.expectEqual(@as(usize, 1), plan.query.field_aliases.len);
    try std.testing.expectEqualStrings("headline", plan.query.field_aliases[0].output);
    try std.testing.expectEqualStrings("title", plan.query.field_aliases[0].field);
    try std.testing.expectEqual(@as(usize, 1), plan.query.predicates.len);
    try std.testing.expectEqualStrings("status", plan.query.predicates[0].field);
    try std.testing.expectEqualStrings("\"active\"", plan.query.predicates[0].value_json.?);
    try std.testing.expect(plan.query.full_text != null);
    switch (plan.query.full_text.?) {
        .match => |match| {
            try std.testing.expectEqualStrings("title", match.field);
            try std.testing.expectEqualStrings("alpha", match.text);
        },
        else => return error.TestUnexpectedResult,
    }
}

pub fn executeLoweredRelationPopulationPlanAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    catalog: table_catalog.CatalogSource,
    default_table_name: []const u8,
    default_schema: storage_schema.TableSchema,
    plan: sql_adapter.LoweredRelationPopulationPlan,
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
    lowered: sql_adapter.LoweredSetOperationPlan,
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
        if (lowered.left.system_time_as_of_sequence) |commit_sequence| {
            const right_sequence = lowered.right.system_time_as_of_sequence orelse return error.UnsupportedRowsQuery;
            if (lowered.left.system_time_as_of_timestamp_ns != null or lowered.right.system_time_as_of_timestamp_ns != null) return error.UnsupportedRowsQuery;
            if (commit_sequence != right_sequence) return error.UnsupportedRowsQuery;
            var left_plan = lowered.left.plan;
            left_plan.ctes = lowered.ctes;
            var left = (try source.rowsQueryPlanCatalogSystemTimeAsOfSequence(alloc, target, left_schema, commit_sequence, left_plan, consistency)) orelse return null;
            defer left.deinit(alloc);
            var right_plan = lowered.right.plan;
            right_plan.ctes = lowered.ctes;
            var right = (try source.rowsQueryPlanCatalogSystemTimeAsOfSequence(alloc, target, left_schema, commit_sequence, right_plan, consistency)) orelse return null;
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
        } else if (lowered.left.system_time_as_of_timestamp_ns) |timestamp_ns| {
            const right_timestamp_ns = lowered.right.system_time_as_of_timestamp_ns orelse return error.UnsupportedRowsQuery;
            if (timestamp_ns != right_timestamp_ns) return error.UnsupportedRowsQuery;
            var left_plan = lowered.left.plan;
            left_plan.ctes = lowered.ctes;
            var left = (try source.rowsQueryPlanCatalogSystemTimeAsOfTimestampNs(alloc, target, left_schema, timestamp_ns, left_plan, consistency)) orelse return null;
            defer left.deinit(alloc);
            var right_plan = lowered.right.plan;
            right_plan.ctes = lowered.ctes;
            var right = (try source.rowsQueryPlanCatalogSystemTimeAsOfTimestampNs(alloc, target, left_schema, timestamp_ns, right_plan, consistency)) orelse return null;
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
        } else if (lowered.right.system_time_as_of_sequence != null) {
            return error.UnsupportedRowsQuery;
        } else if (lowered.right.system_time_as_of_timestamp_ns != null) {
            return error.UnsupportedRowsQuery;
        }
        if (lowered.ctes.len != 0) {
            return try executeLoweredSqlSetOperationPlanWithRoutedCtesAlloc(
                alloc,
                source,
                catalog,
                default_table_name,
                default_schema,
                left_schema,
                lowered,
                operation,
                consistency,
            );
        }
        return try source.rowsSetOperationPlanCatalog(alloc, target, left_schema, .{
            .operation = operation,
            .ctes = lowered.ctes,
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
    var left_plan = lowered.left.plan;
    var right_plan = lowered.right.plan;
    if (lowered.ctes.len != 0) {
        if (left_plan.ctes.len != 0 or right_plan.ctes.len != 0) return error.InvalidRowsRequest;
        if (left_plan.query.source_cte.len != 0) left_plan.ctes = lowered.ctes;
        if (right_plan.query.source_cte.len != 0) right_plan.ctes = lowered.ctes;
    }
    if (lowered.left.system_time_as_of_sequence) |commit_sequence| {
        const right_sequence = lowered.right.system_time_as_of_sequence orelse return error.UnsupportedRowsQuery;
        if (lowered.left.system_time_as_of_timestamp_ns != null or lowered.right.system_time_as_of_timestamp_ns != null) return error.UnsupportedRowsQuery;
        if (commit_sequence != right_sequence) return error.UnsupportedRowsQuery;
        var left = (try source.rowsQueryPlanCatalogSystemTimeAsOfSequence(alloc, left_target, left_schema, commit_sequence, left_plan, consistency)) orelse return null;
        defer left.deinit(alloc);
        var right = (try source.rowsQueryPlanCatalogSystemTimeAsOfSequence(alloc, right_target, right_schema, commit_sequence, right_plan, consistency)) orelse return null;
        defer right.deinit(alloc);
        return try executeSetOperationOnQueryResultsAlloc(alloc, .{
            .operation = operation,
            .left = left_plan,
            .right = right_plan,
            .order_by = lowered.order_by,
            .limit = lowered.limit,
            .offset = lowered.offset,
            .max_rows = lowered.max_rows,
            .max_bytes = lowered.max_bytes,
            .spill_after_bytes = lowered.spill_after_bytes,
        }, left.rows, right.rows);
    }
    if (lowered.left.system_time_as_of_timestamp_ns) |timestamp_ns| {
        const right_timestamp_ns = lowered.right.system_time_as_of_timestamp_ns orelse return error.UnsupportedRowsQuery;
        if (lowered.right.system_time_as_of_sequence != null) return error.UnsupportedRowsQuery;
        if (timestamp_ns != right_timestamp_ns) return error.UnsupportedRowsQuery;
        var left = (try source.rowsQueryPlanCatalogSystemTimeAsOfTimestampNs(alloc, left_target, left_schema, timestamp_ns, left_plan, consistency)) orelse return null;
        defer left.deinit(alloc);
        var right = (try source.rowsQueryPlanCatalogSystemTimeAsOfTimestampNs(alloc, right_target, right_schema, timestamp_ns, right_plan, consistency)) orelse return null;
        defer right.deinit(alloc);
        return try executeSetOperationOnQueryResultsAlloc(alloc, .{
            .operation = operation,
            .left = left_plan,
            .right = right_plan,
            .order_by = lowered.order_by,
            .limit = lowered.limit,
            .offset = lowered.offset,
            .max_rows = lowered.max_rows,
            .max_bytes = lowered.max_bytes,
            .spill_after_bytes = lowered.spill_after_bytes,
        }, left.rows, right.rows);
    }
    if (lowered.right.system_time_as_of_sequence != null or lowered.right.system_time_as_of_timestamp_ns != null) return error.UnsupportedRowsQuery;
    if (lowered.ctes.len != 0 and source.vtable.rows_query_plan_catalog == null) {
        return try executeLoweredSqlSetOperationPlanWithRoutedCtesAlloc(
            alloc,
            source,
            catalog,
            default_table_name,
            default_schema,
            left_schema,
            lowered,
            operation,
            consistency,
        );
    }
    var left = (try source.rowsQueryPlanCatalog(alloc, left_target, left_schema, left_plan, consistency)) orelse return null;
    defer left.deinit(alloc);
    var right = (try source.rowsQueryPlanCatalog(alloc, right_target, right_schema, right_plan, consistency)) orelse return null;
    defer right.deinit(alloc);
    return try executeSetOperationOnQueryResultsAlloc(alloc, .{
        .operation = operation,
        .left = left_plan,
        .right = right_plan,
        .order_by = lowered.order_by,
        .limit = lowered.limit,
        .offset = lowered.offset,
        .max_rows = lowered.max_rows,
        .max_bytes = lowered.max_bytes,
        .spill_after_bytes = lowered.spill_after_bytes,
    }, left.rows, right.rows);
}

fn executeLoweredSqlSetOperationPlanWithRoutedCtesAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    catalog: table_catalog.CatalogSource,
    default_table_name: []const u8,
    default_schema: storage_schema.TableSchema,
    left_schema: storage_schema.TableSchema,
    lowered: sql_adapter.LoweredSetOperationPlan,
    operation: db_mod.types.RelationalRowsSetOperation,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsQueryResult {
    if (lowered.left.system_time_as_of_sequence != null or
        lowered.left.system_time_as_of_timestamp_ns != null or
        lowered.right.system_time_as_of_sequence != null or
        lowered.right.system_time_as_of_timestamp_ns != null)
    {
        return error.UnsupportedRowsQuery;
    }
    if (lowered.left.plan.ctes.len != 0 or lowered.right.plan.ctes.len != 0) return error.InvalidRowsRequest;
    if (!scanPayloadCanStripSyntheticKey(left_schema)) return error.UnsupportedRowsQuery;

    var base_rows = (try collectStableRowsFromRoutedScansAlloc(alloc, source, lowered.left.table_name, left_schema, &.{}, consistency)) orelse return null;
    defer base_rows.deinit(alloc);

    var cte_sources = std.ArrayListUnmanaged(relational_rows_api.RowsCteJsonSource).empty;
    defer cte_sources.deinit(alloc);
    try cte_sources.append(alloc, .{
        .table_name = lowered.left.table_name,
        .schema = left_schema,
        .rows = base_rows.rows,
    });

    var owned_schemas = std.ArrayListUnmanaged(storage_schema.TableSchema).empty;
    defer {
        for (owned_schemas.items) |schema| storage_schema.freeSchema(alloc, schema);
        owned_schemas.deinit(alloc);
    }
    var source_rows = std.ArrayListUnmanaged(RoutedRows).empty;
    defer {
        for (source_rows.items) |*rows| rows.deinit(alloc);
        source_rows.deinit(alloc);
    }

    for (lowered.ctes) |cte| {
        try appendRoutedSetOperationCteSourceIfNeededAlloc(
            alloc,
            source,
            catalog,
            default_table_name,
            default_schema,
            cte.left_table,
            consistency,
            &owned_schemas,
            &source_rows,
            &cte_sources,
        );
        try appendRoutedSetOperationCteSourceIfNeededAlloc(
            alloc,
            source,
            catalog,
            default_table_name,
            default_schema,
            cte.right_table,
            consistency,
            &owned_schemas,
            &source_rows,
            &cte_sources,
        );
    }
    try appendRoutedSetOperationCteSourceIfNeededAlloc(
        alloc,
        source,
        catalog,
        default_table_name,
        default_schema,
        lowered.right.table_name,
        consistency,
        &owned_schemas,
        &source_rows,
        &cte_sources,
    );

    return try relational_rows_api.executeRowsSetOperationPlanOnJsonRowsWithSourcesAlloc(alloc, left_schema, .{
        .operation = operation,
        .ctes = lowered.ctes,
        .left = lowered.left.plan,
        .right = lowered.right.plan,
        .order_by = lowered.order_by,
        .limit = lowered.limit,
        .offset = lowered.offset,
        .max_rows = lowered.max_rows,
        .max_bytes = lowered.max_bytes,
        .spill_after_bytes = lowered.spill_after_bytes,
    }, base_rows.rows, cte_sources.items);
}

fn appendRoutedSetOperationCteSourceIfNeededAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    catalog: table_catalog.CatalogSource,
    default_table_name: []const u8,
    default_schema: storage_schema.TableSchema,
    table_name: []const u8,
    consistency: raft_mod.ReadConsistency,
    owned_schemas: *std.ArrayListUnmanaged(storage_schema.TableSchema),
    source_rows: *std.ArrayListUnmanaged(RoutedRows),
    cte_sources: *std.ArrayListUnmanaged(relational_rows_api.RowsCteJsonSource),
) !void {
    if (table_name.len == 0) return;
    for (cte_sources.items) |cte_source| {
        if (std.mem.eql(u8, cte_source.table_name, table_name)) return;
    }

    const owned_schema = try catalogRuntimeSchemaUnlessDefaultAlloc(alloc, catalog, default_table_name, table_name);
    var owned_schema_transferred = false;
    errdefer if (!owned_schema_transferred) {
        if (owned_schema) |schema| storage_schema.freeSchema(alloc, schema);
    };
    const runtime_schema = owned_schema orelse default_schema;
    if (!scanPayloadCanStripSyntheticKey(runtime_schema)) return error.UnsupportedRowsQuery;

    var rows = (try collectStableRowsFromRoutedScansAlloc(alloc, source, table_name, runtime_schema, &.{}, consistency)) orelse return error.TableNotFound;
    errdefer rows.deinit(alloc);

    if (owned_schema) |schema| {
        try owned_schemas.append(alloc, schema);
        owned_schema_transferred = true;
    }
    try source_rows.append(alloc, rows);
    rows = undefined;
    try cte_sources.append(alloc, .{
        .table_name = table_name,
        .schema = runtime_schema,
        .rows = source_rows.items[source_rows.items.len - 1].rows,
    });
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
        const LookupMode = enum { stable, missing, changed, first_range_changed };

        scan_calls: usize = 0,
        lookup_calls: usize = 0,
        lookup_mode: LookupMode = .stable,

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
            _: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!std.mem.eql(u8, table_name, "orders")) return error.TableNotFound;
            self.lookup_calls += 1;
            if (self.lookup_mode == .missing) return null;
            const json = if (std.mem.eql(u8, key, "o1"))
                if (self.lookup_mode == .first_range_changed)
                    "{\"id\":\"o1\",\"status\":\"closed\",\"amount\":10}"
                else
                    "{\"id\":\"o1\",\"status\":\"OPEN\",\"amount\":10}"
            else if (std.mem.eql(u8, key, "o2"))
                "{\"id\":\"o2\",\"status\":\"closed\",\"amount\":5}"
            else if (std.mem.eql(u8, key, "o3"))
                if (self.lookup_mode == .changed)
                    "{\"id\":\"o3\",\"status\":\"OPEN\",\"amount\":8}"
                else
                    "{\"id\":\"o3\",\"status\":\"OPEN\",\"amount\":7}"
            else
                return null;
            return .{ .json = try lookup_alloc.dupe(u8, json), .version = 1 };
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
            if (!std.mem.eql(u8, table_name, "orders")) return error.TableNotFound;
            self.scan_calls += 1;
            const ndjson = if (std.mem.eql(u8, from_key, "") and std.mem.eql(u8, to_key, "o2"))
                "{\"key\":\"o1\",\"version\":1,\"id\":\"o1\",\"status\":\"OPEN\",\"amount\":10}\n"
            else if (std.mem.eql(u8, from_key, "o2") and std.mem.eql(u8, to_key, ""))
                "{\"key\":\"o2\",\"version\":1,\"id\":\"o2\",\"status\":\"closed\",\"amount\":5}\n" ++
                    "{\"key\":\"o3\",\"version\":1,\"id\":\"o3\",\"status\":\"OPEN\",\"amount\":7}\n"
            else if (std.mem.eql(u8, from_key, "") and std.mem.eql(u8, to_key, ""))
                "{\"key\":\"o1\",\"version\":1,\"id\":\"o1\",\"status\":\"OPEN\",\"amount\":10}\n" ++
                    "{\"key\":\"o2\",\"version\":1,\"id\":\"o2\",\"status\":\"closed\",\"amount\":5}\n" ++
                    "{\"key\":\"o3\",\"version\":1,\"id\":\"o3\",\"status\":\"OPEN\",\"amount\":7}\n"
            else
                return error.TestUnexpectedResult;
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
    var lowered = try sql_adapter.lowerWritePlanWithCatalogAlloc(
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

            const doc_range = db_mod.types.RelationalRowsDocKeyRange{ .start = "row:a", .end = "row:z" };
            var cte_ranged_plan = plan;
            var cte_ranged_ctes = [_]db_mod.types.RelationalRowsCte{insert_source.ctes[0]};
            cte_ranged_ctes[0].query.doc_key_range = doc_range;
            cte_ranged_plan.ctes = cte_ranged_ctes[0..];
            const scans_before_cte_range = fake.scan_calls;
            try std.testing.expectError(error.InvalidRowsRequest, rowsInsertSourcePlanBatchFromRoutedScansWithSchemasAlloc(
                alloc,
                fake.source(),
                insert_source.table_name,
                insert_source.insert_source.req.source_table,
                target_schema,
                source_schema,
                cte_ranged_plan,
                .read_index,
                NoConflictResolver.resolver(),
            ));
            try std.testing.expectEqual(scans_before_cte_range, fake.scan_calls);

            var source_ranged_plan = plan;
            var source_ranged_req = insert_source.insert_source.req;
            source_ranged_req.source.doc_key_range = doc_range;
            source_ranged_plan.insert_source = source_ranged_req;
            const scans_before_source_range = fake.scan_calls;
            try std.testing.expectError(error.InvalidRowsRequest, rowsInsertSourcePlanBatchFromRoutedScansWithSchemasAlloc(
                alloc,
                fake.source(),
                insert_source.table_name,
                insert_source.insert_source.req.source_table,
                target_schema,
                source_schema,
                source_ranged_plan,
                .read_index,
                NoConflictResolver.resolver(),
            ));
            try std.testing.expectEqual(scans_before_source_range, fake.scan_calls);

            var missing = FakeRoutedSource{ .lookup_mode = .missing };
            try std.testing.expectError(error.TopologyChanged, rowsInsertSourcePlanBatchFromRoutedScansWithSchemasAlloc(
                alloc,
                missing.source(),
                insert_source.table_name,
                insert_source.insert_source.req.source_table,
                target_schema,
                source_schema,
                plan,
                .read_index,
                NoConflictResolver.resolver(),
            ));

            var changed = FakeRoutedSource{ .lookup_mode = .changed };
            try std.testing.expectError(error.TopologyChanged, rowsInsertSourcePlanBatchFromRoutedScansWithSchemasAlloc(
                alloc,
                changed.source(),
                insert_source.table_name,
                insert_source.insert_source.req.source_table,
                target_schema,
                source_schema,
                plan,
                .read_index,
                NoConflictResolver.resolver(),
            ));

            const routed_ranges = [_]db_mod.types.RelationalRowsDocKeyRange{
                .{ .start = "", .end = "o2" },
                .{ .start = "o2", .end = "" },
            };
            var per_range_plan = plan;
            per_range_plan.ranges = routed_ranges[0..];
            var first_range_changed = FakeRoutedSource{ .lookup_mode = .first_range_changed };
            try std.testing.expectError(error.TopologyChanged, rowsInsertSourcePlanBatchFromRoutedScansWithSchemasAlloc(
                alloc,
                first_range_changed.source(),
                insert_source.table_name,
                insert_source.insert_source.req.source_table,
                target_schema,
                source_schema,
                per_range_plan,
                .read_index,
                NoConflictResolver.resolver(),
            ));
            try std.testing.expectEqual(@as(usize, 1), first_range_changed.scan_calls);
            try std.testing.expectEqual(@as(usize, 1), first_range_changed.lookup_calls);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "lowered sql insert values scalar subqueries build batches from routed reads" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;

    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer storage_schema.freeSchema(alloc, schema);

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

    const FakeRoutedSource = struct {
        query_calls: usize = 0,
        cardinality_rows: bool = false,

        fn source(self: *@This()) TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .rows_query_plan = rowsQueryPlan,
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
            return error.UnsupportedOperation;
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
            return error.UnsupportedOperation;
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

        fn rowsQueryPlan(
            ptr: *anyopaque,
            query_alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsQueryPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsQueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(raft_mod.ReadConsistency.read_index, consistency);
            try std.testing.expectEqualStrings("usage_records", table_name);
            try std.testing.expectEqual(@as(usize, 3), runtime_schema.relational_columns.len);
            try std.testing.expectEqual(@as(usize, 1), plan.query.select.len);
            try std.testing.expectEqualStrings("status", plan.query.select[0]);
            self.query_calls += 1;

            const row_count: usize = if (self.cardinality_rows) 2 else 1;
            const rows = try query_alloc.alloc([]const u8, row_count);
            var initialized: usize = 0;
            errdefer {
                for (rows[0..initialized]) |row| query_alloc.free(row);
                query_alloc.free(rows);
            }
            rows[0] = try query_alloc.dupe(u8, "{\"status\":\"queued\"}");
            initialized += 1;
            if (row_count > 1) {
                rows[1] = try query_alloc.dupe(u8, "{\"status\":\"ready\"}");
                initialized += 1;
            }
            return .{ .rows = rows, .total = @intCast(row_count) };
        }
    };

    var lowered = try sql_adapter.lowerWritePlanAlloc(
        alloc,
        "INSERT INTO usage_records (id, status, amount) VALUES ('u2', (SELECT status FROM usage_records WHERE id = 'u1'), 7) RETURNING id, status, amount",
        schema,
        &.{},
        .{ .unique_resolver = NoConflictResolver.resolver(), .sync_level = .full_index },
    );
    defer lowered.deinit(alloc);

    switch (lowered) {
        .insert_source => |insert_source| {
            try std.testing.expectEqual(@as(usize, 1), insert_source.literal_source_rows.len);
            var fake = FakeRoutedSource{};
            var batch = (try rowsLoweredInsertSourceBatchFromRoutedScansWithSchemasAlloc(
                alloc,
                fake.source(),
                "usage_records",
                schema,
                schema,
                insert_source,
                .read_index,
                NoConflictResolver.resolver(),
            )).?;
            defer batch.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 1), fake.query_calls);
            try std.testing.expectEqual(db_mod.types.SyncLevel.full_index, batch.req.sync_level);
            try std.testing.expectEqual(@as(u32, 1), batch.inserted);
            try std.testing.expectEqual(@as(usize, 1), batch.returning_rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"u2\",\"status\":\"queued\",\"amount\":7}", batch.returning_rows[0]);

            var cardinality_fake = FakeRoutedSource{ .cardinality_rows = true };
            try std.testing.expectError(error.InvalidRowsRequest, rowsLoweredInsertSourceBatchFromRoutedScansWithSchemasAlloc(
                alloc,
                cardinality_fake.source(),
                "usage_records",
                schema,
                schema,
                insert_source,
                .read_index,
                NoConflictResolver.resolver(),
            ));
            try std.testing.expectEqual(@as(usize, 1), cardinality_fake.query_calls);
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
        const TargetLookupMode = enum { stable, missing, changed, version_changed };
        const SourceLookupMode = enum { stable, missing, changed, first_range_changed, version_changed };

        target_key: []const u8,
        target_row_json: []const u8,
        target_lookup_mode: TargetLookupMode = .stable,
        source_lookup_mode: SourceLookupMode = .stable,
        scan_calls: usize = 0,
        target_lookup_calls: usize = 0,
        source_lookup_calls: usize = 0,
        query_plan_calls: usize = 0,

        fn source(self: *@This()) TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .rows_query_plan = rowsQueryPlan,
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
            if (std.mem.eql(u8, table_name, "orders")) {
                if (!std.mem.eql(u8, key, self.target_key)) return null;
                self.target_lookup_calls += 1;
                return switch (self.target_lookup_mode) {
                    .stable => .{
                        .json = try lookup_alloc.dupe(u8, self.target_row_json),
                        .version = 17,
                    },
                    .missing => null,
                    .changed => .{
                        .json = try lookup_alloc.dupe(u8, "{\"id\":\"t1\",\"status\":\"closed\",\"organization_id\":\"org:1\"}"),
                        .version = 18,
                    },
                    .version_changed => .{
                        .json = try lookup_alloc.dupe(u8, self.target_row_json),
                        .version = 18,
                    },
                };
            }
            if (std.mem.eql(u8, table_name, "imports")) {
                self.source_lookup_calls += 1;
                if (std.mem.eql(u8, key, "import:s1")) {
                    return switch (self.source_lookup_mode) {
                        .missing => null,
                        .first_range_changed => .{
                            .json = try lookup_alloc.dupe(u8, "{\"id\":\"s1\",\"target_id\":\"t1\",\"status\":\"MOVED\",\"organization_id\":\"org:1\"}"),
                            .version = 23,
                        },
                        else => .{
                            .json = try lookup_alloc.dupe(u8, "{\"id\":\"s1\",\"target_id\":\"t1\",\"status\":\"UPDATED\",\"organization_id\":\"org:1\"}"),
                            .version = 21,
                        },
                    };
                }
                if (std.mem.eql(u8, key, "import:s2")) {
                    return switch (self.source_lookup_mode) {
                        .stable, .first_range_changed => .{
                            .json = try lookup_alloc.dupe(u8, "{\"id\":\"s2\",\"target_id\":\"new1\",\"status\":\"inserted\",\"organization_id\":\"org:2\"}"),
                            .version = 22,
                        },
                        .missing => null,
                        .changed => .{
                            .json = try lookup_alloc.dupe(u8, "{\"id\":\"s2\",\"target_id\":\"new1\",\"status\":\"moved\",\"organization_id\":\"org:2\"}"),
                            .version = 23,
                        },
                        .version_changed => .{
                            .json = try lookup_alloc.dupe(u8, "{\"id\":\"s2\",\"target_id\":\"new1\",\"status\":\"inserted\",\"organization_id\":\"org:2\"}"),
                            .version = 23,
                        },
                    };
                }
            }
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
                if (std.mem.eql(u8, from_key, "") and (std.mem.eql(u8, to_key, "") or std.mem.eql(u8, to_key, "n")))
                    try std.fmt.allocPrint(scan_alloc, "{{\"key\":{f},\"version\":17,\"id\":\"t1\",\"status\":\"open\",\"organization_id\":\"org:1\"}}\n", .{std.json.fmt(self.target_key, .{})})
                else if (std.mem.eql(u8, from_key, "n") and std.mem.eql(u8, to_key, ""))
                    try scan_alloc.dupe(u8, "")
                else
                    return error.TestUnexpectedResult
            else if (std.mem.eql(u8, table_name, "imports"))
                if (std.mem.eql(u8, from_key, "") and std.mem.eql(u8, to_key, "import:s2"))
                    try scan_alloc.dupe(u8, "{\"key\":\"import:s1\",\"version\":21,\"id\":\"s1\",\"target_id\":\"t1\",\"status\":\"UPDATED\",\"organization_id\":\"org:1\"}\n")
                else if (std.mem.eql(u8, from_key, "import:s2") and std.mem.eql(u8, to_key, ""))
                    try scan_alloc.dupe(u8, "{\"key\":\"import:s2\",\"version\":22,\"id\":\"s2\",\"target_id\":\"new1\",\"status\":\"inserted\",\"organization_id\":\"org:2\"}\n")
                else if (std.mem.eql(u8, from_key, "") and std.mem.eql(u8, to_key, ""))
                    try scan_alloc.dupe(u8, "{\"key\":\"import:s1\",\"version\":21,\"id\":\"s1\",\"target_id\":\"t1\",\"status\":\"UPDATED\",\"organization_id\":\"org:1\"}\n{\"key\":\"import:s2\",\"version\":22,\"id\":\"s2\",\"target_id\":\"new1\",\"status\":\"inserted\",\"organization_id\":\"org:2\"}\n")
                else
                    return error.TestUnexpectedResult
            else if (std.mem.eql(u8, table_name, "oversized_imports")) blk: {
                try std.testing.expectEqualStrings("", from_key);
                try std.testing.expectEqualStrings("", to_key);
                var out = std.ArrayListUnmanaged(u8).empty;
                errdefer out.deinit(scan_alloc);
                const line = "{\"key\":\"import:s1\",\"version\":21,\"id\":\"s1\",\"target_id\":\"t1\",\"status\":\"UPDATED\",\"organization_id\":\"org:1\"}\n";
                var index: usize = 0;
                while (index <= db_mod.types.default_relational_rows_cte_max_rows) : (index += 1) {
                    try out.appendSlice(scan_alloc, line);
                }
                break :blk try out.toOwnedSlice(scan_alloc);
            } else return error.TableNotFound;
            return .{ .ndjson = ndjson };
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
            if (!std.mem.eql(u8, table_name, "imports")) return null;
            self.query_plan_calls += 1;
            return try rowsQueryPlanFromRoutedScansAlloc(plan_alloc, self.source(), table_name, runtime_schema, plan, consistency);
        }
    };

    var catalog = FakeCatalog{};
    var lowered = try sql_adapter.lowerWritePlanWithCatalogAlloc(
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
            try std.testing.expectEqual(@as(usize, 1), fake.target_lookup_calls);
            try std.testing.expectEqual(@as(usize, 2), fake.source_lookup_calls);
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

            var cte_lowered = try sql_adapter.lowerWritePlanWithCatalogAlloc(
                alloc,
                "WITH ready_imports AS (SELECT id, target_id, status, organization_id FROM imports) MERGE INTO orders AS target USING ready_imports AS source ON target.id = source.target_id WHEN MATCHED THEN UPDATE SET status = lower(source.status) WHEN NOT MATCHED THEN INSERT (id, status, organization_id) VALUES (source.target_id, upper(source.status), source.organization_id) RETURNING id, status, lower(status) AS status_key",
                target_schema,
                &.{},
                .{},
                catalog.iface(),
            );
            defer cte_lowered.deinit(alloc);
            switch (cte_lowered) {
                .merge_mutation => |cte_merge| {
                    const source_projection_collision = [_]db_mod.types.RelationalRowsExpressionProjection{.{
                        .output = "target_id",
                        .expression = .{ .kind = .value, .value_json = "\"shadowed\"" },
                    }};
                    var cte_merge_plan = cte_merge;
                    cte_merge_plan.source.expressions = source_projection_collision[0..];

                    var cte_batch = (try rowsMergeMutationBatchFromRoutedScansWithSchemasAlloc(
                        alloc,
                        fake.source(),
                        "orders",
                        "imports",
                        target_schema,
                        source_schema,
                        cte_merge_plan,
                        .{ .select_all = true },
                        &.{},
                        cte_merge_plan.source,
                        &.{},
                        .read_index,
                    )).?;
                    defer cte_batch.deinit(alloc);

                    try std.testing.expectEqual(@as(usize, 1), fake.query_plan_calls);
                    try std.testing.expectEqual(@as(u32, 1), cte_batch.inserted);
                    try std.testing.expectEqual(@as(u32, 1), cte_batch.transformed);
                    try std.testing.expectEqual(@as(usize, 2), cte_batch.returning_rows.len);
                    try std.testing.expectEqualStrings("{\"id\":\"t1\",\"status\":\"updated\",\"status_key\":\"updated\"}", cte_batch.returning_rows[0]);
                    try std.testing.expectEqualStrings("{\"id\":\"new1\",\"status\":\"INSERTED\",\"status_key\":\"inserted\"}", cte_batch.returning_rows[1]);

                    const cte_source_ranges = [_]db_mod.types.RelationalRowsDocKeyRange{.{
                        .start = "",
                        .end = "import:s2",
                    }};
                    var ranged_cte_batch = (try rowsMergeMutationBatchFromRoutedScansWithSchemasAlloc(
                        alloc,
                        fake.source(),
                        "orders",
                        "imports",
                        target_schema,
                        source_schema,
                        cte_merge_plan,
                        .{ .select_all = true },
                        &.{},
                        cte_merge_plan.source,
                        cte_source_ranges[0..],
                        .read_index,
                    )).?;
                    defer ranged_cte_batch.deinit(alloc);

                    try std.testing.expectEqual(@as(usize, 2), fake.query_plan_calls);
                    try std.testing.expectEqual(@as(u32, 0), ranged_cte_batch.inserted);
                    try std.testing.expectEqual(@as(u32, 1), ranged_cte_batch.transformed);
                    try std.testing.expectEqual(@as(usize, 1), ranged_cte_batch.returning_rows.len);
                    try std.testing.expectEqualStrings("{\"id\":\"t1\",\"status\":\"updated\",\"status_key\":\"updated\"}", ranged_cte_batch.returning_rows[0]);
                },
                else => return error.TestUnexpectedResult,
            }

            const doc_range = db_mod.types.RelationalRowsDocKeyRange{ .start = "row:a", .end = "row:z" };
            const scans_before_ranged_merge = fake.scan_calls;
            try std.testing.expectError(error.InvalidRowsRequest, rowsMergeMutationBatchFromRoutedScansWithSchemasAlloc(
                alloc,
                fake.source(),
                "orders",
                "imports",
                target_schema,
                source_schema,
                merge,
                .{ .select_all = true, .doc_key_range = doc_range },
                &.{doc_range},
                .{ .select_all = true },
                &.{},
                .read_index,
            ));
            try std.testing.expectError(error.InvalidRowsRequest, rowsMergeMutationBatchFromRoutedScansWithSchemasAlloc(
                alloc,
                fake.source(),
                "orders",
                "imports",
                target_schema,
                source_schema,
                merge,
                .{ .select_all = true },
                &.{},
                .{ .select_all = true, .doc_key_range = doc_range },
                &.{doc_range},
                .read_index,
            ));
            try std.testing.expectEqual(scans_before_ranged_merge, fake.scan_calls);

            const target_split_ranges = [_]db_mod.types.RelationalRowsDocKeyRange{
                .{ .start = "", .end = "n" },
                .{ .start = "n", .end = "" },
            };
            var changed_first_target_range = FakeRoutedSource{ .target_key = target_key, .target_row_json = target_row_json, .target_lookup_mode = .changed };
            try std.testing.expectError(error.TopologyChanged, rowsMergeMutationBatchFromRoutedScansWithSchemasAlloc(
                alloc,
                changed_first_target_range.source(),
                "orders",
                "imports",
                target_schema,
                source_schema,
                merge,
                .{ .select_all = true },
                target_split_ranges[0..],
                .{ .select_all = true },
                &.{},
                .read_index,
            ));
            try std.testing.expectEqual(@as(usize, 1), changed_first_target_range.scan_calls);
            try std.testing.expectEqual(@as(usize, 1), changed_first_target_range.target_lookup_calls);
            try std.testing.expectEqual(@as(usize, 0), changed_first_target_range.source_lookup_calls);

            const target_closed_predicates = [_]storage_schema.RelationalCheck{.{
                .name = "status_closed",
                .field = "status",
                .value_json = "\"closed\"",
            }};
            var changed_unselected_target_range = FakeRoutedSource{ .target_key = target_key, .target_row_json = target_row_json, .target_lookup_mode = .changed };
            try std.testing.expectError(error.TopologyChanged, rowsMergeMutationBatchFromRoutedScansWithSchemasAlloc(
                alloc,
                changed_unselected_target_range.source(),
                "orders",
                "imports",
                target_schema,
                source_schema,
                merge,
                .{ .select_all = true, .predicates = target_closed_predicates[0..] },
                target_split_ranges[0..],
                .{ .select_all = true },
                &.{},
                .read_index,
            ));
            try std.testing.expectEqual(@as(usize, 1), changed_unselected_target_range.scan_calls);
            try std.testing.expectEqual(@as(usize, 1), changed_unselected_target_range.target_lookup_calls);
            try std.testing.expectEqual(@as(usize, 0), changed_unselected_target_range.source_lookup_calls);

            var changed_unselected_target_full_scan = FakeRoutedSource{ .target_key = target_key, .target_row_json = target_row_json, .target_lookup_mode = .changed };
            try std.testing.expectError(error.TopologyChanged, rowsMergeMutationBatchFromRoutedScansWithSchemasAlloc(
                alloc,
                changed_unselected_target_full_scan.source(),
                "orders",
                "imports",
                target_schema,
                source_schema,
                merge,
                .{ .select_all = true, .predicates = target_closed_predicates[0..] },
                &.{},
                .{ .select_all = true },
                &.{},
                .read_index,
            ));
            try std.testing.expectEqual(@as(usize, 1), changed_unselected_target_full_scan.scan_calls);
            try std.testing.expectEqual(@as(usize, 1), changed_unselected_target_full_scan.target_lookup_calls);
            try std.testing.expectEqual(@as(usize, 0), changed_unselected_target_full_scan.source_lookup_calls);

            const status_order = [_]db_mod.types.RelationalRowsQueryOrder{.{ .field = "status", .direction = .asc }};
            var changed_global_target_range = FakeRoutedSource{ .target_key = target_key, .target_row_json = target_row_json, .target_lookup_mode = .changed };
            try std.testing.expectError(error.TopologyChanged, rowsMergeMutationBatchFromRoutedScansWithSchemasAlloc(
                alloc,
                changed_global_target_range.source(),
                "orders",
                "imports",
                target_schema,
                source_schema,
                merge,
                .{ .select_all = true, .order_by = status_order[0..] },
                target_split_ranges[0..],
                .{ .select_all = true },
                &.{},
                .read_index,
            ));
            try std.testing.expectEqual(@as(usize, 1), changed_global_target_range.scan_calls);
            try std.testing.expectEqual(@as(usize, 1), changed_global_target_range.target_lookup_calls);
            try std.testing.expectEqual(@as(usize, 0), changed_global_target_range.source_lookup_calls);

            const source_split_ranges = [_]db_mod.types.RelationalRowsDocKeyRange{
                .{ .start = "", .end = "import:s2" },
                .{ .start = "import:s2", .end = "" },
            };
            var changed_first_source_range = FakeRoutedSource{ .target_key = target_key, .target_row_json = target_row_json, .source_lookup_mode = .first_range_changed };
            try std.testing.expectError(error.TopologyChanged, rowsMergeMutationBatchFromRoutedScansWithSchemasAlloc(
                alloc,
                changed_first_source_range.source(),
                "orders",
                "imports",
                target_schema,
                source_schema,
                merge,
                .{ .select_all = true },
                &.{},
                .{ .select_all = true },
                source_split_ranges[0..],
                .read_index,
            ));
            try std.testing.expectEqual(@as(usize, 2), changed_first_source_range.scan_calls);
            try std.testing.expectEqual(@as(usize, 1), changed_first_source_range.target_lookup_calls);
            try std.testing.expectEqual(@as(usize, 1), changed_first_source_range.source_lookup_calls);

            const source_moved_predicates = [_]storage_schema.RelationalCheck{.{
                .name = "status_moved",
                .field = "status",
                .value_json = "\"MOVED\"",
            }};
            var changed_unselected_source_range = FakeRoutedSource{ .target_key = target_key, .target_row_json = target_row_json, .source_lookup_mode = .first_range_changed };
            try std.testing.expectError(error.TopologyChanged, rowsMergeMutationBatchFromRoutedScansWithSchemasAlloc(
                alloc,
                changed_unselected_source_range.source(),
                "orders",
                "imports",
                target_schema,
                source_schema,
                merge,
                .{ .select_all = true },
                &.{},
                .{ .select_all = true, .predicates = source_moved_predicates[0..] },
                source_split_ranges[0..],
                .read_index,
            ));
            try std.testing.expectEqual(@as(usize, 2), changed_unselected_source_range.scan_calls);
            try std.testing.expectEqual(@as(usize, 1), changed_unselected_source_range.target_lookup_calls);
            try std.testing.expectEqual(@as(usize, 1), changed_unselected_source_range.source_lookup_calls);

            var changed_unselected_source_full_scan = FakeRoutedSource{ .target_key = target_key, .target_row_json = target_row_json, .source_lookup_mode = .first_range_changed };
            try std.testing.expectError(error.TopologyChanged, rowsMergeMutationBatchFromRoutedScansWithSchemasAlloc(
                alloc,
                changed_unselected_source_full_scan.source(),
                "orders",
                "imports",
                target_schema,
                source_schema,
                merge,
                .{ .select_all = true },
                &.{},
                .{ .select_all = true, .predicates = source_moved_predicates[0..] },
                &.{},
                .read_index,
            ));
            try std.testing.expectEqual(@as(usize, 2), changed_unselected_source_full_scan.scan_calls);
            try std.testing.expectEqual(@as(usize, 1), changed_unselected_source_full_scan.target_lookup_calls);
            try std.testing.expectEqual(@as(usize, 1), changed_unselected_source_full_scan.source_lookup_calls);

            var changed_global_source_range = FakeRoutedSource{ .target_key = target_key, .target_row_json = target_row_json, .source_lookup_mode = .first_range_changed };
            try std.testing.expectError(error.TopologyChanged, rowsMergeMutationBatchFromRoutedScansWithSchemasAlloc(
                alloc,
                changed_global_source_range.source(),
                "orders",
                "imports",
                target_schema,
                source_schema,
                merge,
                .{ .select_all = true },
                &.{},
                .{ .select_all = true, .order_by = status_order[0..] },
                source_split_ranges[0..],
                .read_index,
            ));
            try std.testing.expectEqual(@as(usize, 2), changed_global_source_range.scan_calls);
            try std.testing.expectEqual(@as(usize, 1), changed_global_source_range.target_lookup_calls);
            try std.testing.expectEqual(@as(usize, 1), changed_global_source_range.source_lookup_calls);

            var missing_target = FakeRoutedSource{ .target_key = target_key, .target_row_json = target_row_json, .target_lookup_mode = .missing };
            try std.testing.expectError(error.TopologyChanged, rowsMergeMutationBatchFromRoutedScansWithSchemasAlloc(
                alloc,
                missing_target.source(),
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
            ));

            var changed_target = FakeRoutedSource{ .target_key = target_key, .target_row_json = target_row_json, .target_lookup_mode = .changed };
            try std.testing.expectError(error.TopologyChanged, rowsMergeMutationBatchFromRoutedScansWithSchemasAlloc(
                alloc,
                changed_target.source(),
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
            ));

            var version_changed_target = FakeRoutedSource{ .target_key = target_key, .target_row_json = target_row_json, .target_lookup_mode = .version_changed };
            try std.testing.expectError(error.TopologyChanged, rowsMergeMutationBatchFromRoutedScansWithSchemasAlloc(
                alloc,
                version_changed_target.source(),
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
            ));

            var missing_source = FakeRoutedSource{ .target_key = target_key, .target_row_json = target_row_json, .source_lookup_mode = .missing };
            try std.testing.expectError(error.TopologyChanged, rowsMergeMutationBatchFromRoutedScansWithSchemasAlloc(
                alloc,
                missing_source.source(),
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
            ));

            var changed_source = FakeRoutedSource{ .target_key = target_key, .target_row_json = target_row_json, .source_lookup_mode = .changed };
            try std.testing.expectError(error.TopologyChanged, rowsMergeMutationBatchFromRoutedScansWithSchemasAlloc(
                alloc,
                changed_source.source(),
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
            ));

            var version_changed_source = FakeRoutedSource{ .target_key = target_key, .target_row_json = target_row_json, .source_lookup_mode = .version_changed };
            try std.testing.expectError(error.TopologyChanged, rowsMergeMutationBatchFromRoutedScansWithSchemasAlloc(
                alloc,
                version_changed_source.source(),
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
            ));

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

    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-recursive-merge-routed-scans");
    defer alloc.free(path);
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
                try appendScanLine(scan_alloc, &out, entry.id, json, try self.db.getTimestamp(scan_alloc, entry.id));
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

    var lowered = try sql_adapter.lowerWritePlanAlloc(
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
            var claimed_recursive_merge = recursive_merge;
            claimed_recursive_merge.merge.source.row_claim = .{
                .mode = .for_update,
                .owner_id = "session:illegal-recursive-merge-cte-claim",
                .txn_id = [_]u8{ 0x60, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x6b, 0x6c, 0x6d, 0x6e, 0x6f },
            };
            const scan_calls_before_claim = read_source.scan_calls;
            const query_calls_before_claim = read_source.query_plan_calls;
            try std.testing.expectError(error.UnsupportedRowsQuery, rowsRecursiveMergeMutationBatchFromRoutedScansWithSchemasAlloc(
                alloc,
                read_source.source(),
                catalog.iface(),
                "usage_records",
                "usage_records",
                schema,
                schema,
                claimed_recursive_merge,
                .{ .select_all = true },
                &.{},
                .read_index,
            ));
            try std.testing.expectEqual(scan_calls_before_claim, read_source.scan_calls);
            try std.testing.expectEqual(query_calls_before_claim, read_source.query_plan_calls);

            const merge_source_order = [_]db_mod.types.RelationalRowsQueryOrder{.{
                .field = "id",
                .direction = .desc,
            }};
            const merge_source_projection = [_]db_mod.types.RelationalRowsExpressionProjection{.{
                .output = "id",
                .expression = .{ .kind = .value, .value_json = "\"shadowed\"" },
            }};
            var projected_recursive_merge = recursive_merge;
            projected_recursive_merge.merge.source.order_by = merge_source_order[0..];
            projected_recursive_merge.merge.source.limit = 1;
            projected_recursive_merge.merge.source.expressions = merge_source_projection[0..];

            var batch = (try rowsRecursiveMergeMutationBatchFromRoutedScansWithSchemasAlloc(
                alloc,
                read_source.source(),
                catalog.iface(),
                "usage_records",
                "usage_records",
                schema,
                schema,
                projected_recursive_merge,
                .{ .select_all = true },
                &.{},
                .read_index,
            )).?;
            defer batch.deinit(alloc);

            try std.testing.expectEqual(@as(usize, 1), read_source.scan_calls);
            try std.testing.expectEqual(@as(usize, 2), read_source.query_plan_calls);
            try std.testing.expectEqual(@as(usize, 3), read_source.lookup_calls);
            try std.testing.expectEqual(@as(u32, 1), batch.transformed);
            try std.testing.expectEqual(@as(usize, 1), batch.returning_rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"b\",\"status\":\"child\"}", batch.returning_rows[0]);

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
            lookup_alloc: std.mem.Allocator,
            table_name: []const u8,
            key: []const u8,
            _: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            const json = if (std.mem.eql(u8, table_name, "orders"))
                if (std.mem.eql(u8, key, "a"))
                    "{\"id\":\"a\",\"status\":\"open\",\"amount\":1}"
                else if (std.mem.eql(u8, key, "b"))
                    "{\"id\":\"b\",\"status\":\"closed\",\"amount\":9}"
                else if (std.mem.eql(u8, key, "z"))
                    "{\"id\":\"z\",\"status\":\"open\",\"amount\":7}"
                else
                    return null
            else if (std.mem.eql(u8, table_name, "customers"))
                if (std.mem.eql(u8, key, "c1"))
                    "{\"id\":\"c1\",\"status\":\"open\",\"name\":\"Ada\"}"
                else if (std.mem.eql(u8, key, "c2"))
                    "{\"id\":\"c2\",\"status\":\"closed\",\"name\":\"Grace\"}"
                else
                    return null
            else
                return null;
            return .{ .json = try lookup_alloc.dupe(u8, json), .version = 1 };
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

    var lowered = try sql_adapter.lowerReadPlanAlloc(
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

    const recursive_plan = switch (lowered) {
        .recursive_cte => |recursive| recursive,
        else => return error.TestUnexpectedResult,
    };
    const materialized_bytes = db_mod.types.relationalRowsCteMaterializedJsonBytes(materialized.rows) orelse return error.TestUnexpectedResult;
    var spill_recursive_plan = recursive_plan;
    spill_recursive_plan.max_rows = @intCast(materialized.rows.len);
    spill_recursive_plan.max_bytes = materialized_bytes;
    spill_recursive_plan.spill_after_bytes = materialized_bytes - 1;
    var spill_materialized_fake = FakeSource{};
    var spill_materialized = (try materializeLoweredRecursiveCteRowsAlloc(
        alloc,
        spill_materialized_fake.source(),
        catalog.iface(),
        "nodes",
        schema,
        spill_recursive_plan,
        .read_index,
    )) orelse return error.TestUnexpectedResult;
    defer spill_materialized.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), spill_materialized_fake.calls);
    try std.testing.expectEqual(materialized.rows.len, spill_materialized.rows.len);
    for (materialized.rows, spill_materialized.rows) |expected, actual| {
        try std.testing.expectEqualStrings(expected, actual);
    }

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
    var claimed_insert_req = db_mod.types.RelationalRowsInsertSourceRequest{
        .source_table = "nodes",
        .source = recursive.final_query,
        .assignments = assignments[0..],
        .returning = returning[0..],
    };
    claimed_insert_req.source.row_claim = .{
        .mode = .for_update,
        .owner_id = "session:illegal-insert-cte-source-claim",
        .txn_id = [_]u8{ 0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5a, 0x5b, 0x5c, 0x5d, 0x5e, 0x5f },
    };
    var claimed_insert_fake = FakeSource{};
    try std.testing.expectError(error.UnsupportedRowsQuery, rowsInsertSourceBatchFromRecursiveCtePlanAlloc(
        alloc,
        claimed_insert_fake.source(),
        catalog.iface(),
        "nodes",
        schema,
        "walk_copies",
        schema,
        recursive,
        claimed_insert_req,
        .read_index,
        null,
    ));
    try std.testing.expectEqual(@as(usize, 0), claimed_insert_fake.calls);

    const insert_source_order = [_]db_mod.types.RelationalRowsQueryOrder{.{
        .field = "id",
        .direction = .desc,
    }};
    var selected_insert_source = recursive.final_query;
    selected_insert_source.order_by = insert_source_order[0..];
    selected_insert_source.offset = 1;
    selected_insert_source.limit = 1;

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
            .source = selected_insert_source,
            .assignments = assignments[0..],
            .returning = returning[0..],
        },
        .read_index,
        null,
    )) orelse return error.TestUnexpectedResult;
    defer batch.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), insert_fake.calls);
    try std.testing.expectEqual(@as(u32, 1), batch.inserted);
    try std.testing.expectEqual(@as(usize, 1), batch.returning_rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"c\",\"depth\":2}", batch.returning_rows[0]);

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
        const LookupMode = enum { stable, changed, version_changed };

        scan_calls: usize = 0,
        lookup_mode: LookupMode = .stable,

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
            _: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const json = if (std.mem.eql(u8, table_name, "orders"))
                if (std.mem.eql(u8, key, "o1"))
                    "{\"id\":\"o1\",\"status\":\"open\",\"customer_id\":\"c1\"}"
                else if (std.mem.eql(u8, key, "o2"))
                    if (self.lookup_mode == .changed)
                        "{\"id\":\"o2\",\"status\":\"open\",\"customer_id\":\"c2\"}"
                    else
                        "{\"id\":\"o2\",\"status\":\"closed\",\"customer_id\":\"c2\"}"
                else
                    return null
            else if (std.mem.eql(u8, table_name, "customers"))
                if (std.mem.eql(u8, key, "c1"))
                    "{\"id\":\"c1\",\"status\":\"open\",\"name\":\"Ada\"}"
                else if (std.mem.eql(u8, key, "c2"))
                    "{\"id\":\"c2\",\"status\":\"closed\",\"name\":\"Grace\"}"
                else
                    return null
            else
                return null;
            const version: u64 = if (self.lookup_mode == .version_changed and std.mem.eql(u8, table_name, "orders") and std.mem.eql(u8, key, "z")) 2 else 1;
            return .{ .json = try lookup_alloc.dupe(u8, json), .version = version };
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
                "{\"key\":\"o1\",\"version\":1,\"id\":\"o1\",\"status\":\"open\",\"customer_id\":\"c1\"}\n{\"key\":\"o2\",\"version\":1,\"id\":\"o2\",\"status\":\"closed\",\"customer_id\":\"c2\"}\n"
            else if (std.mem.eql(u8, table_name, "customers"))
                "{\"key\":\"c1\",\"version\":1,\"id\":\"c1\",\"status\":\"open\",\"name\":\"Ada\"}\n{\"key\":\"c2\",\"version\":1,\"id\":\"c2\",\"status\":\"closed\",\"name\":\"Grace\"}\n"
            else
                return error.TableNotFound;
            return .{ .ndjson = try scan_alloc.dupe(u8, ndjson) };
        }
    };

    var catalog = FakeCatalog{};
    var fake = FakeRoutedSource{};
    var lowered = try sql_adapter.lowerRelationPopulationPlanWithCatalogAlloc(
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

    try std.testing.expectEqual(sql_adapter.RelationPopulationMode.create_table_as, result.mode);
    try std.testing.expectEqualStrings("order_archive", result.target_table_name);
    try std.testing.expectEqual(@as(usize, 2), fake.scan_calls);
    try std.testing.expectEqual(@as(u32, 2), result.total);
    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expectEqualStrings("{\"order_id\":\"o1\",\"customer_name\":\"Ada\"}", result.rows[0]);
    try std.testing.expectEqualStrings("{\"order_id\":\"o2\",\"customer_name\":\"Grace\"}", result.rows[1]);

    var lowered_no_data = try sql_adapter.lowerRelationPopulationPlanWithCatalogAlloc(
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
    lowered: sql_adapter.LoweredRecursiveCtePlan,
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
    lowered: sql_adapter.LoweredRecursiveCtePlan,
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
    lowered: sql_adapter.LoweredRecursiveCtePlan,
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

    return try finalizeRecursiveCteMaterializedRowsAlloc(alloc, lowered, &materialized);
}

fn deinitRecursiveCteRows(alloc: std.mem.Allocator, rows: *std.ArrayListUnmanaged([]const u8)) void {
    for (rows.items) |row| alloc.free(@constCast(row));
    rows.deinit(alloc);
}

fn recursiveCteMaterializationDescriptor(lowered: sql_adapter.LoweredRecursiveCtePlan) db_mod.types.RelationalRowsCte {
    return .{
        .name = lowered.cte_name,
        .query = lowered.anchor.plan.query,
        .max_rows = lowered.max_rows,
        .max_bytes = lowered.max_bytes,
        .spill_after_bytes = lowered.spill_after_bytes,
    };
}

fn finalizeRecursiveCteMaterializedRowsAlloc(
    alloc: std.mem.Allocator,
    lowered: sql_adapter.LoweredRecursiveCtePlan,
    materialized: *std.ArrayListUnmanaged([]const u8),
) !RecursiveCteMaterializedRows {
    var rows = try materialized.toOwnedSlice(alloc);
    materialized.* = .empty;
    var rows_owned = true;
    errdefer if (rows_owned) row_spill.freeJsonRows(alloc, rows);

    const materialized_bytes = db_mod.types.relationalRowsCteMaterializedJsonBytes(rows) orelse return error.UnsupportedRowsQuery;
    switch (db_mod.types.relationalRowsCteMaterializationDecision(recursiveCteMaterializationDescriptor(lowered), rows.len, materialized_bytes)) {
        .memory => {
            rows_owned = false;
            return .{ .rows = rows };
        },
        .spill => {
            rows = try row_spill.spillAndReloadOwnedJsonRowsAlloc(alloc, rows, materialized_bytes, lowered.cte_name);
            rows_owned = false;
            return .{ .rows = rows };
        },
        .reject => return error.RelationalRowsCteMaterializationRejected,
    }
}

fn admitRecursiveCteRows(
    lowered: sql_adapter.LoweredRecursiveCtePlan,
    rows: []const []const u8,
) !void {
    const materialized_bytes = db_mod.types.relationalRowsCteMaterializedJsonBytes(rows) orelse return error.UnsupportedRowsQuery;
    try db_mod.DB.admitRelationalRowsCteMaterializationAllowSpill(recursiveCteMaterializationDescriptor(lowered), rows.len, materialized_bytes);
}

fn recursiveCteJoinRowsMatchAlloc(
    alloc: std.mem.Allocator,
    base_row_json: []const u8,
    cte_row_json: []const u8,
    join: sql_adapter.LoweredRecursiveCteJoinMemberPlan,
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

pub fn loweredSetOperationToRowsOperation(operation: sql_adapter.SelectSetOperation) db_mod.types.RelationalRowsSetOperation {
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
    var combined = try db_mod.DB.relationalRowsSetOperationRowsAlloc(alloc, plan.operation, left_rows, right_rows);
    var combined_owned = true;
    defer if (combined_owned) freeOwnedRows(alloc, combined);
    const materialized_bytes = db_mod.types.relationalRowsCteMaterializedJsonBytes(combined) orelse return error.UnsupportedRowsQuery;
    switch (db_mod.types.relationalRowsSetOperationMaterializationDecision(plan, combined.len, materialized_bytes)) {
        .memory => {},
        .reject => return error.RelationalRowsCteMaterializationRejected,
        .spill => {
            combined = try row_spill.spillAndReloadOwnedJsonRowsAlloc(alloc, combined, materialized_bytes, "set-operation");
            combined_owned = true;
        },
    }
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
    recursive: sql_adapter.LoweredRecursiveCtePlan,
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
    recursive: sql_adapter.LoweredRecursiveCtePlan,
    req: db_mod.types.RelationalRowsInsertSourceRequest,
    consistency: raft_mod.ReadConsistency,
    conflict_resolver: ?relational_rows_api.UniqueSelectorResolver,
) !?relational_rows_api.OwnedRowsBatchRequest {
    if (!std.mem.eql(u8, req.source.source_cte, recursive.cte_name)) return error.InvalidRowsRequest;
    try rejectMaterializedCteRowClaim(req.source);
    try rejectCoordinatorRoutedDocKeyRange(req.source, &.{});
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

    return try relational_rows_api.buildRowsInsertSourceBatchWithSchemasAlloc(
        alloc,
        target_table_name,
        target_schema,
        source_schema,
        req,
        materialized.rows,
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
    try rejectCteProducerDocKeyRanges(plan.ctes);
    try rejectCoordinatorRoutedDocKeyRange(plan.insert_source.source, plan.ranges);
    if (plan.insert_source.source_table.len > 0 and !std.mem.eql(u8, plan.insert_source.source_table, source_table_name)) {
        return error.InvalidRowsRequest;
    }

    var base_rows = (try collectStableRowsFromRoutedScansAlloc(alloc, source, source_table_name, source_schema, plan.ranges, consistency)) orelse return null;
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

pub fn rowsLoweredInsertSourceBatchFromRoutedScansWithSchemasAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    source_table_name: []const u8,
    target_schema: storage_schema.TableSchema,
    source_schema: storage_schema.TableSchema,
    lowered: sql_adapter.LoweredInsertSource,
    consistency: raft_mod.ReadConsistency,
    conflict_resolver: ?relational_rows_api.UniqueSelectorResolver,
) !?relational_rows_api.OwnedRowsBatchRequest {
    if (lowered.insert_source.req.source_table.len > 0 and !std.mem.eql(u8, lowered.insert_source.req.source_table, source_table_name)) {
        return error.InvalidRowsRequest;
    }
    if (lowered.literal_source_rows.len == 0) {
        const plan: db_mod.types.RelationalRowsInsertSourcePlan = .{
            .ctes = lowered.ctes,
            .insert_source = lowered.insert_source.req,
            .sync_level = lowered.sync_level,
        };
        return try rowsInsertSourcePlanBatchFromRoutedScansWithSchemasAlloc(
            alloc,
            source,
            lowered.table_name,
            source_table_name,
            target_schema,
            source_schema,
            plan,
            consistency,
            conflict_resolver,
        );
    }

    if (lowered.ctes.len != 0) return error.InvalidRowsRequest;
    if (lowered.insert_source.req.source.source_cte.len != 0) return error.InvalidRowsRequest;

    const materialized_rows = try literalInsertSourceRowsWithScalarSubqueriesFromRoutedReadsAlloc(
        alloc,
        source,
        source_table_name,
        source_schema,
        lowered.insert_source.req.source.scalar_subqueries,
        lowered.literal_source_row_scalar_subqueries,
        lowered.literal_source_rows,
        consistency,
    );
    defer freeOwnedRows(alloc, materialized_rows);

    var batch = try relational_rows_api.buildRowsInsertSourceBatchWithSchemasAlloc(
        alloc,
        lowered.table_name,
        target_schema,
        source_schema,
        lowered.insert_source.req,
        materialized_rows,
        conflict_resolver,
    );
    batch.req.sync_level = lowered.sync_level;
    return batch;
}

fn literalInsertSourceRowsWithScalarSubqueriesFromRoutedReadsAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    source_table_name: []const u8,
    source_schema: storage_schema.TableSchema,
    scalar_subqueries: []const db_mod.types.RelationalRowsScalarSubqueryProjection,
    row_scalar_subqueries: []const []const db_mod.types.RelationalRowsScalarSubqueryProjection,
    literal_source_rows: []const []const u8,
    consistency: raft_mod.ReadConsistency,
) ![]const []const u8 {
    if (literal_source_rows.len == 0) return error.InvalidRowsRequest;
    if (row_scalar_subqueries.len != 0 and row_scalar_subqueries.len != literal_source_rows.len) return error.InvalidRowsRequest;
    const rows = try alloc.alloc([]const u8, literal_source_rows.len);
    var initialized: usize = 0;
    errdefer {
        for (rows[0..initialized]) |row| alloc.free(row);
        alloc.free(rows);
    }

    for (literal_source_rows, 0..) |row_json, i| {
        const effective_scalar_subqueries = if (row_scalar_subqueries.len != 0 and row_scalar_subqueries[i].len != 0) row_scalar_subqueries[i] else scalar_subqueries;
        rows[i] = if (!hiddenScalarSubqueriesPresent(effective_scalar_subqueries))
            try alloc.dupe(u8, row_json)
        else
            try rowJsonWithHiddenScalarSubqueriesFromRoutedReadsAlloc(alloc, source, source_table_name, source_schema, row_json, effective_scalar_subqueries, consistency);
        initialized += 1;
    }
    return rows;
}

const LiteralScalarSubqueryValue = struct {
    output: []const u8,
    value_json: []const u8,
};

fn hiddenScalarSubqueriesPresent(scalar_subqueries: []const db_mod.types.RelationalRowsScalarSubqueryProjection) bool {
    for (scalar_subqueries) |projection| {
        if (projection.hidden) return true;
    }
    return false;
}

fn rowJsonWithHiddenScalarSubqueriesFromRoutedReadsAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    source_table_name: []const u8,
    source_schema: storage_schema.TableSchema,
    row_json: []const u8,
    scalar_subqueries: []const db_mod.types.RelationalRowsScalarSubqueryProjection,
    consistency: raft_mod.ReadConsistency,
) ![]const u8 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, row_json, .{}) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;

    var scalar_values = std.ArrayListUnmanaged(LiteralScalarSubqueryValue).empty;
    defer {
        for (scalar_values.items) |value| alloc.free(value.value_json);
        scalar_values.deinit(alloc);
    }

    for (scalar_subqueries) |projection| {
        if (!projection.hidden) continue;
        const value_json = try scalarSubqueryProjectionValueJsonFromRoutedReadsAlloc(alloc, source, source_table_name, source_schema, parsed.value, projection, consistency);
        errdefer alloc.free(value_json);
        try scalar_values.append(alloc, .{
            .output = projection.output,
            .value_json = value_json,
        });
    }
    return try rowJsonWithHiddenScalarValuesFromParsedAlloc(alloc, parsed.value, scalar_values.items);
}

fn scalarSubqueryProjectionValueJsonFromRoutedReadsAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    source_table_name: []const u8,
    source_schema: storage_schema.TableSchema,
    outer_row: std.json.Value,
    projection: db_mod.types.RelationalRowsScalarSubqueryProjection,
    consistency: raft_mod.ReadConsistency,
) ![]const u8 {
    var query = projection.query;
    var correlated_predicates: []storage_schema.RelationalCheck = &.{};
    defer freeOwnedRelationalChecks(alloc, correlated_predicates);
    var combined_predicates: []storage_schema.RelationalCheck = &.{};
    defer if (combined_predicates.len > 0) alloc.free(combined_predicates);
    if (projection.correlations.len != 0) {
        correlated_predicates = try lateralCorrelationPredicatesAlloc(alloc, outer_row, projection.correlations);
        if (correlated_predicates.len != projection.correlations.len) return try alloc.dupe(u8, "null");
        combined_predicates = try combinedBorrowedAndOwnedRelationalChecksAlloc(alloc, projection.query.predicates, correlated_predicates);
        query.predicates = combined_predicates;
    }
    var result = (try source.rowsQueryPlan(
        alloc,
        source_table_name,
        source_schema,
        .{ .query = query },
        consistency,
    )) orelse return error.TableNotFound;
    defer result.deinit(alloc);
    if (result.rows.len > 1) return error.InvalidRowsRequest;
    if (result.rows.len == 0) return try alloc.dupe(u8, "null");

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, result.rows[0], .{}) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    const value = jsonValueAtPath(parsed.value, projection.output_field) orelse return try alloc.dupe(u8, "null");
    return try std.json.Stringify.valueAlloc(alloc, value.*, .{});
}

fn lateralCorrelationPredicatesAlloc(
    alloc: std.mem.Allocator,
    left_row: std.json.Value,
    correlations: []const db_mod.types.RelationalRowsLateralCorrelation,
) ![]storage_schema.RelationalCheck {
    const predicates = try alloc.alloc(storage_schema.RelationalCheck, correlations.len);
    var initialized: usize = 0;
    errdefer {
        freeOwnedRelationalCheckContents(alloc, predicates[0..initialized]);
        alloc.free(predicates);
    }
    for (correlations) |correlation| {
        const selected = jsonValueAtPath(left_row, correlation.left_field) orelse break;
        if (selected.* == .null) break;
        const value_json = switch (selected.*) {
            .bool, .integer, .float, .string => try std.json.Stringify.valueAlloc(alloc, selected.*, .{}),
            else => return error.InvalidRowsRequest,
        };
        var value_transferred = false;
        errdefer if (!value_transferred) alloc.free(value_json);
        const name = try alloc.dupe(u8, "");
        var name_transferred = false;
        errdefer if (!name_transferred) alloc.free(name);
        const field = try alloc.dupe(u8, correlation.right_field);
        var field_transferred = false;
        errdefer if (!field_transferred) alloc.free(field);
        predicates[initialized] = .{
            .name = name,
            .field = field,
            .op = correlation.op,
            .value_json = value_json,
        };
        initialized += 1;
        name_transferred = true;
        field_transferred = true;
        value_transferred = true;
    }
    if (initialized != correlations.len) {
        freeOwnedRelationalCheckContents(alloc, predicates[0..initialized]);
        alloc.free(predicates);
        return &.{};
    }
    return predicates;
}

fn combinedBorrowedAndOwnedRelationalChecksAlloc(
    alloc: std.mem.Allocator,
    borrowed: []const storage_schema.RelationalCheck,
    owned: []const storage_schema.RelationalCheck,
) ![]storage_schema.RelationalCheck {
    const combined = try alloc.alloc(storage_schema.RelationalCheck, borrowed.len + owned.len);
    @memcpy(combined[0..borrowed.len], borrowed);
    @memcpy(combined[borrowed.len..], owned);
    return combined;
}

fn freeOwnedRelationalChecks(alloc: std.mem.Allocator, checks: []const storage_schema.RelationalCheck) void {
    freeOwnedRelationalCheckContents(alloc, checks);
    if (checks.len > 0) alloc.free(checks);
}

fn freeOwnedRelationalCheckContents(alloc: std.mem.Allocator, checks: []const storage_schema.RelationalCheck) void {
    for (checks) |check| {
        if (check.name.len > 0) alloc.free(@constCast(check.name));
        if (check.field.len > 0) alloc.free(@constCast(check.field));
        if (check.value_json) |value_json| if (value_json.len > 0) alloc.free(@constCast(value_json));
    }
}

fn rowJsonWithHiddenScalarValuesAlloc(
    alloc: std.mem.Allocator,
    row_json: []const u8,
    scalar_values: []const LiteralScalarSubqueryValue,
) ![]const u8 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, row_json, .{}) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    return try rowJsonWithHiddenScalarValuesFromParsedAlloc(alloc, parsed.value, scalar_values);
}

fn rowJsonWithHiddenScalarValuesFromParsedAlloc(
    alloc: std.mem.Allocator,
    row_value: std.json.Value,
    scalar_values: []const LiteralScalarSubqueryValue,
) ![]const u8 {
    if (row_value != .object) return error.InvalidRowsRequest;
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeByte('{');
    var first = true;
    for (row_value.object.keys(), row_value.object.values()) |field, value| {
        for (scalar_values) |scalar_value| {
            if (std.mem.eql(u8, field, scalar_value.output)) return error.InvalidRowsRequest;
        }
        if (!first) try writer.writeByte(',');
        first = false;
        try writer.print("{f}:", .{std.json.fmt(field, .{})});
        try std.json.Stringify.value(value, .{}, writer);
    }
    for (scalar_values) |scalar_value| {
        if (!first) try writer.writeByte(',');
        first = false;
        try writer.print("{f}:{s}", .{ std.json.fmt(scalar_value.output, .{}), scalar_value.value_json });
    }
    try writer.writeByte('}');
    return try out.toOwnedSlice();
}

fn jsonValueAtPath(root: std.json.Value, path: []const u8) ?*const std.json.Value {
    if (root != .object) return null;
    var current: *const std.json.Value = &root;
    var parts = std.mem.splitScalar(u8, path, '.');
    while (parts.next()) |part| {
        if (part.len == 0 or current.* != .object) return null;
        current = current.object.getPtr(part) orelse return null;
    }
    return current;
}

pub fn rowsMergeMutationBatchFromRoutedScansWithSchemasAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    target_table_name: []const u8,
    source_table_name: []const u8,
    target_schema: storage_schema.TableSchema,
    source_schema: storage_schema.TableSchema,
    plan: sql_adapter.LoweredMergeMutationPlan,
    target_query: db_mod.types.RelationalRowsQueryRequest,
    target_ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    source_query: db_mod.types.RelationalRowsQueryRequest,
    source_ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    consistency: raft_mod.ReadConsistency,
) !?relational_rows_api.OwnedRowsBatchRequest {
    return try rowsMergeMutationBatchFromRoutedScansWithSchemasAndDefaultContextAlloc(
        alloc,
        source,
        target_table_name,
        source_table_name,
        target_schema,
        source_schema,
        plan,
        target_query,
        target_ranges,
        source_query,
        source_ranges,
        consistency,
        .{},
    );
}

pub fn rowsMergeMutationBatchFromRoutedScansWithSchemasAndDefaultContextAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    target_table_name: []const u8,
    source_table_name: []const u8,
    target_schema: storage_schema.TableSchema,
    source_schema: storage_schema.TableSchema,
    plan: sql_adapter.LoweredMergeMutationPlan,
    target_query: db_mod.types.RelationalRowsQueryRequest,
    target_ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    source_query: db_mod.types.RelationalRowsQueryRequest,
    source_ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    consistency: raft_mod.ReadConsistency,
    default_context: relational_rows_api.DefaultValueContext,
) !?relational_rows_api.OwnedRowsBatchRequest {
    try rejectCoordinatorRoutedDocKeyRange(target_query, target_ranges);
    try rejectCoordinatorRoutedDocKeyRange(source_query, source_ranges);
    if (plan.ctes.len != 0 or source_query.source_cte.len != 0) {
        try rejectCteProducerDocKeyRanges(plan.ctes);
        try rejectMaterializedCteRowClaim(source_query);
    }

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
        if (plan.data_modifying_ctes.len != 0) return error.UnsupportedSqlShape;
        const full_source_query = dmlFullRowSourceQuery(source_query);
        break :blk (try source.rowsQueryPlan(
            alloc,
            source_table_name,
            source_schema,
            .{ .ctes = plan.ctes, .ranges = source_ranges, .query = full_source_query },
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

    const merge_targets = try alloc.alloc(sql_adapter.MergeExecutionTargetRow, target_rows.len);
    defer alloc.free(merge_targets);
    for (target_rows, 0..) |row, i| {
        merge_targets[i] = .{
            .key = row.key,
            .json = row.json,
            .version = row.version,
        };
    }

    return try sql_adapter.buildMergeMutationBatchAlloc(
        alloc,
        target_schema,
        source_schema,
        plan,
        merge_targets,
        source_rows.rows,
        default_context,
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
    lowered: sql_adapter.LoweredRecursiveMergeMutation,
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
    lowered: sql_adapter.LoweredRecursiveMergeMutation,
    target_query: db_mod.types.RelationalRowsQueryRequest,
    target_ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    consistency: raft_mod.ReadConsistency,
) !?relational_rows_api.OwnedRowsBatchRequest {
    return try rowsRecursiveMergeMutationBatchFromRoutedScansWithSchemasAndSessionAndDefaultContextAlloc(
        alloc,
        source,
        catalog,
        session,
        default_table_name,
        target_table_name,
        target_schema,
        recursive_source_schema,
        lowered,
        target_query,
        target_ranges,
        consistency,
        .{},
    );
}

pub fn rowsRecursiveMergeMutationBatchFromRoutedScansWithSchemasAndSessionAndDefaultContextAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
    default_table_name: []const u8,
    target_table_name: []const u8,
    target_schema: storage_schema.TableSchema,
    recursive_source_schema: storage_schema.TableSchema,
    lowered: sql_adapter.LoweredRecursiveMergeMutation,
    target_query: db_mod.types.RelationalRowsQueryRequest,
    target_ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    consistency: raft_mod.ReadConsistency,
    default_context: relational_rows_api.DefaultValueContext,
) !?relational_rows_api.OwnedRowsBatchRequest {
    if (!std.mem.eql(u8, lowered.merge.source.source_cte, lowered.recursive.cte_name)) return error.InvalidRowsRequest;
    try rejectMaterializedCteRowClaim(lowered.merge.source);
    try rejectCoordinatorRoutedDocKeyRange(lowered.merge.source, &.{});
    try rejectCoordinatorRoutedDocKeyRange(target_query, target_ranges);

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

    const source_query = recursiveDmlFullRowSourceQuery(lowered.merge.source);
    var source_rows = try relational_rows_api.executeRowsQueryOnJsonRowsAlloc(alloc, cte_schema, source_query, materialized.rows);
    defer source_rows.deinit(alloc);

    const merge_targets = try alloc.alloc(sql_adapter.MergeExecutionTargetRow, target_rows.len);
    defer alloc.free(merge_targets);
    for (target_rows, 0..) |row, i| {
        merge_targets[i] = .{
            .key = row.key,
            .json = row.json,
            .version = row.version,
        };
    }

    return try sql_adapter.buildMergeMutationBatchAlloc(
        alloc,
        target_schema,
        cte_schema,
        lowered.merge,
        merge_targets,
        source_rows.rows,
        default_context,
    );
}

fn rejectMaterializedCteRowClaim(query: db_mod.types.RelationalRowsQueryRequest) !void {
    if (query.source_cte.len != 0 and query.row_claim != null) return error.UnsupportedRowsQuery;
}

fn rejectCteProducerRowClaims(ctes: []const db_mod.types.RelationalRowsCte) !void {
    for (ctes) |cte| {
        if (cte.query.row_claim != null) return error.UnsupportedRowsQuery;
    }
}

fn rejectCteProducerDocKeyRanges(ctes: []const db_mod.types.RelationalRowsCte) !void {
    for (ctes) |cte| {
        if (cte.query.doc_key_range != null) return error.InvalidRowsRequest;
    }
}

fn rejectCoordinatorRoutedDocKeyRange(
    query: db_mod.types.RelationalRowsQueryRequest,
    ranges: []const db_mod.types.RelationalRowsDocKeyRange,
) !void {
    if (query.doc_key_range != null and (ranges.len != 0 or query.source_cte.len != 0)) return error.InvalidRowsRequest;
}

fn rejectEmbeddedRoutedDocKeyRange(query: db_mod.types.RelationalRowsQueryRequest) !void {
    if (query.doc_key_range != null) return error.InvalidRowsRequest;
}

fn rejectRoutedRowsQueryPlanRowClaims(plan: db_mod.types.RelationalRowsQueryPlan) !void {
    try rejectCteProducerRowClaims(plan.ctes);
    if (plan.query.row_claim != null) return error.UnsupportedRowsQuery;
}

fn rejectRoutedRowsQueryPlanDocKeyRanges(plan: db_mod.types.RelationalRowsQueryPlan) !void {
    try rejectCteProducerDocKeyRanges(plan.ctes);
    try rejectEmbeddedRoutedDocKeyRange(plan.query);
}

fn rejectRoutedRowsAggregatePlanRowClaims(plan: db_mod.types.RelationalRowsAggregatePlan) !void {
    try rejectCteProducerRowClaims(plan.ctes);
    if (plan.aggregate.source.row_claim != null) return error.UnsupportedRowsQuery;
}

fn rejectRoutedRowsAggregatePlanDocKeyRanges(plan: db_mod.types.RelationalRowsAggregatePlan) !void {
    try rejectCteProducerDocKeyRanges(plan.ctes);
    try rejectEmbeddedRoutedDocKeyRange(plan.aggregate.source);
}

fn rejectRoutedRowsWindowPlanRowClaims(plan: db_mod.types.RelationalRowsWindowPlan) !void {
    try rejectCteProducerRowClaims(plan.ctes);
    if (plan.window.source.row_claim != null) return error.UnsupportedRowsQuery;
}

fn rejectRoutedRowsWindowPlanDocKeyRanges(plan: db_mod.types.RelationalRowsWindowPlan) !void {
    try rejectCteProducerDocKeyRanges(plan.ctes);
    try rejectEmbeddedRoutedDocKeyRange(plan.window.source);
}

fn rejectRoutedRowsSetOperationPlanRowClaims(plan: db_mod.types.RelationalRowsSetOperationPlan) !void {
    try rejectCteProducerRowClaims(plan.ctes);
    if (plan.left.query.row_claim != null or plan.right.query.row_claim != null) return error.UnsupportedRowsQuery;
}

fn rejectRoutedRowsSetOperationPlanDocKeyRanges(plan: db_mod.types.RelationalRowsSetOperationPlan) !void {
    try rejectCteProducerDocKeyRanges(plan.ctes);
    try rejectEmbeddedRoutedDocKeyRange(plan.left.query);
    try rejectEmbeddedRoutedDocKeyRange(plan.right.query);
}

fn rejectRoutedRowsJoinPlanRowClaims(plan: db_mod.types.RelationalRowsJoinPlan) !void {
    try rejectCteProducerRowClaims(plan.ctes);
    if (plan.join.left.row_claim != null or plan.join.right.row_claim != null) return error.UnsupportedRowsQuery;
}

fn rejectRoutedRowsJoinPlanDocKeyRanges(plan: db_mod.types.RelationalRowsJoinPlan) !void {
    try rejectCteProducerDocKeyRanges(plan.ctes);
    try rejectEmbeddedRoutedDocKeyRange(plan.join.left);
    try rejectEmbeddedRoutedDocKeyRange(plan.join.right);
}

fn rejectRoutedRowsLateralPlanRowClaims(plan: db_mod.types.RelationalRowsLateralPlan) !void {
    try rejectCteProducerRowClaims(plan.ctes);
    if (plan.lateral.left.row_claim != null or plan.lateral.right.row_claim != null) return error.UnsupportedRowsQuery;
}

fn rejectRoutedRowsLateralPlanDocKeyRanges(plan: db_mod.types.RelationalRowsLateralPlan) !void {
    try rejectCteProducerDocKeyRanges(plan.ctes);
    try rejectEmbeddedRoutedDocKeyRange(plan.lateral.left);
    try rejectEmbeddedRoutedDocKeyRange(plan.lateral.right);
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
    materialized_rows: u64 = 0,
    materialized_bytes: u64 = 0,
    spilled: bool = false,
    spilled_rows: u64 = 0,
    spilled_bytes: u64 = 0,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        db_mod.types.freeRelationalRowsCollectedRows(alloc, self.rows);
        self.* = undefined;
    }
};

const RoutedScanRowFingerprint = struct {
    key: []u8,
    version: u64,
    json_hash: u64,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.key);
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
    try rejectCoordinatorRoutedDocKeyRange(req, ranges);
    if (mergeQueryCanSelectPerRange(req, ranges)) {
        return try collectSelectedMergeRowsFromRoutedScansAlloc(alloc, source, table_name, schema, req, ranges, consistency);
    }

    var scanned = (try collectStableMergeScanRowsFromRoutedScansAlloc(alloc, source, table_name, schema, ranges, consistency)) orelse return null;
    defer scanned.deinit(alloc);

    const selected = try selectMergeScanRowsAlloc(alloc, schema, req, scanned.rows);
    errdefer db_mod.types.freeRelationalRowsCollectedRows(alloc, selected);

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
    try rejectCoordinatorRoutedDocKeyRange(req, ranges);
    const selected = if (mergeQueryCanSelectPerRange(req, ranges))
        (try collectSelectedMergeRowsFromRoutedScansAlloc(alloc, source, table_name, schema, req, ranges, consistency)) orelse return null
    else selected_blk: {
        var scanned = (try collectStableMergeScanRowsFromRoutedScansAlloc(alloc, source, table_name, schema, ranges, consistency)) orelse return null;
        defer scanned.deinit(alloc);
        const rows = try selectMergeScanRowsAlloc(alloc, schema, req, scanned.rows);
        errdefer db_mod.types.freeRelationalRowsCollectedRows(alloc, rows);
        break :selected_blk rows;
    };
    defer db_mod.types.freeRelationalRowsCollectedRows(alloc, selected);

    const rows = try alloc.alloc([]const u8, selected.len);
    errdefer alloc.free(rows);
    for (selected, 0..) |*row, i| {
        rows[i] = row.json;
        row.json = "";
    }
    return .{ .rows = rows, .total = std.math.cast(u32, rows.len) orelse return error.InvalidRowsRequest };
}

fn mergeQueryCanSelectPerRange(req: db_mod.types.RelationalRowsQueryRequest, ranges: []const db_mod.types.RelationalRowsDocKeyRange) bool {
    return ranges.len > 0 and
        req.distinct_on.len == 0 and
        req.distinct_on_expressions.len == 0 and
        req.order_by.len == 0 and
        req.limit == null and
        req.offset == 0;
}

fn collectSelectedMergeRowsFromRoutedScansAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    schema: storage_schema.TableSchema,
    req: db_mod.types.RelationalRowsQueryRequest,
    ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    consistency: raft_mod.ReadConsistency,
) !?[]db_mod.types.RelationalRowsCollectedRow {
    if (!scanPayloadCanStripSyntheticKey(schema)) return error.UnsupportedRowsQuery;
    var selected_rows = std.ArrayListUnmanaged(db_mod.types.RelationalRowsCollectedRow).empty;
    errdefer {
        for (selected_rows.items) |row_value| {
            var row = row_value;
            row.deinit(alloc);
        }
        selected_rows.deinit(alloc);
    }

    var materialization = row_spill.JsonRowsMaterializationTracker.initDefault("routed-scan");
    var saw_source = false;
    for (ranges) |range| {
        var range_rows = std.ArrayListUnmanaged(db_mod.types.RelationalRowsCollectedRow).empty;
        defer {
            for (range_rows.items) |row_value| {
                var row = row_value;
                row.deinit(alloc);
            }
            range_rows.deinit(alloc);
        }

        saw_source = (try appendMergeScanRowsFromRoutedScanAlloc(alloc, source, table_name, range.start, range.end, &range_rows, &materialization, consistency)) or saw_source;
        try verifyRoutedScanRowsStillCurrentAlloc(alloc, source, table_name, range_rows.items, consistency);

        const selected = try selectMergeScanRowsAlloc(alloc, schema, req, range_rows.items);
        var selected_owned = true;
        errdefer if (selected_owned) db_mod.types.freeRelationalRowsCollectedRows(alloc, selected);

        try selected_rows.ensureUnusedCapacity(alloc, selected.len);
        for (selected) |row| {
            selected_rows.appendAssumeCapacity(row);
        }
        selected_owned = false;
        alloc.free(selected);
    }
    if (!saw_source) {
        for (selected_rows.items) |row_value| {
            var row = row_value;
            row.deinit(alloc);
        }
        selected_rows.deinit(alloc);
        return null;
    }

    var owned_rows = try selected_rows.toOwnedSlice(alloc);
    errdefer db_mod.types.freeRelationalRowsCollectedRows(alloc, owned_rows);
    if (materialization.spill_required) {
        owned_rows = try spillAndReloadCollectedRowsAlloc(alloc, owned_rows, "routed-scan");
    }
    return owned_rows;
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

    var materialization = row_spill.JsonRowsMaterializationTracker.initDefault("routed-scan");
    var saw_source = false;
    if (ranges.len == 0) {
        const start_len = rows.items.len;
        saw_source = try appendMergeScanRowsFromRoutedScanAlloc(alloc, source, table_name, "", "", &rows, &materialization, consistency);
        try verifyRoutedScanRowsStillCurrentAlloc(alloc, source, table_name, rows.items[start_len..], consistency);
    } else {
        for (ranges) |range| {
            const start_len = rows.items.len;
            saw_source = (try appendMergeScanRowsFromRoutedScanAlloc(alloc, source, table_name, range.start, range.end, &rows, &materialization, consistency)) or saw_source;
            try verifyRoutedScanRowsStillCurrentAlloc(alloc, source, table_name, rows.items[start_len..], consistency);
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

    var owned_rows = try rows.toOwnedSlice(alloc);
    errdefer db_mod.types.freeRelationalRowsCollectedRows(alloc, owned_rows);
    const spill_required = materialization.spill_required;
    if (materialization.spill_required) {
        owned_rows = try spillAndReloadCollectedRowsAlloc(alloc, owned_rows, "routed-scan");
    }
    return .{
        .rows = owned_rows,
        .materialized_rows = materialization.rows,
        .materialized_bytes = materialization.bytes,
        .spilled = spill_required,
        .spilled_rows = if (spill_required) materialization.rows else 0,
        .spilled_bytes = if (spill_required) materialization.bytes else 0,
    };
}

fn appendMergeScanRowsFromRoutedScanAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
    rows: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsCollectedRow),
    materialization: *row_spill.JsonRowsMaterializationTracker,
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
        try materialization.account(row.json);
        try rows.append(alloc, row);
    }
    return true;
}

fn mergeScanRowFromScanLineAlloc(alloc: std.mem.Allocator, line: []const u8) !db_mod.types.RelationalRowsCollectedRow {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, line, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRemoteResponse;
    const key_field = if (parsed.value.object.get("_id") != null) "_id" else "key";
    const key_value = parsed.value.object.get(key_field) orelse return error.InvalidRemoteResponse;
    if (key_value != .string) return error.InvalidRemoteResponse;
    const key = try alloc.dupe(u8, key_value.string);
    errdefer alloc.free(key);
    if (parsed.value.object.fetchOrderedRemove(key_field) == null) return error.InvalidRemoteResponse;
    const version_value = parsed.value.object.get("version") orelse return error.InvalidRemoteResponse;
    const version: u64 = switch (version_value) {
        .integer => |int| std.math.cast(u64, int) orelse return error.InvalidRemoteResponse,
        else => return error.InvalidRemoteResponse,
    };
    _ = parsed.value.object.fetchOrderedRemove("version");
    const json = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
    errdefer alloc.free(json);
    return .{ .key = key, .json = json, .version = version };
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

fn rowJsonsFromCollectedRowsAlloc(
    alloc: std.mem.Allocator,
    scanned: []const db_mod.types.RelationalRowsCollectedRow,
) ![]const []const u8 {
    const rows = try alloc.alloc([]const u8, scanned.len);
    for (scanned, 0..) |row, i| rows[i] = row.json;
    return rows;
}

fn collectStableRowsFromRoutedScansAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    schema: storage_schema.TableSchema,
    ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    consistency: raft_mod.ReadConsistency,
) !?RoutedRows {
    var scanned = (try collectStableMergeScanRowsFromRoutedScansAlloc(alloc, source, table_name, schema, ranges, consistency)) orelse return null;
    defer scanned.deinit(alloc);

    const rows = try alloc.alloc([]const u8, scanned.rows.len);
    errdefer alloc.free(rows);
    for (scanned.rows, 0..) |*row, i| {
        rows[i] = row.json;
        row.json = "";
    }
    return .{ .rows = rows };
}

fn collectStableMergeScanRowsFromRoutedScansAlloc(
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

    var materialization = row_spill.JsonRowsMaterializationTracker.initDefault("routed-scan");
    var saw_source = false;
    if (ranges.len == 0) {
        const start_len = rows.items.len;
        saw_source = try appendMergeScanRowsFromRoutedScanAlloc(alloc, source, table_name, "", "", &rows, &materialization, consistency);
        try verifyRoutedScanRowsStillCurrentAlloc(alloc, source, table_name, rows.items[start_len..], consistency);
    } else {
        for (ranges) |range| {
            const start_len = rows.items.len;
            saw_source = (try appendMergeScanRowsFromRoutedScanAlloc(alloc, source, table_name, range.start, range.end, &rows, &materialization, consistency)) or saw_source;
            try verifyRoutedScanRowsStillCurrentAlloc(alloc, source, table_name, rows.items[start_len..], consistency);
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

    var owned_rows = try rows.toOwnedSlice(alloc);
    errdefer db_mod.types.freeRelationalRowsCollectedRows(alloc, owned_rows);
    const spill_required = materialization.spill_required;
    if (materialization.spill_required) {
        owned_rows = try spillAndReloadCollectedRowsAlloc(alloc, owned_rows, "routed-scan");
    }
    return .{
        .rows = owned_rows,
        .materialized_rows = materialization.rows,
        .materialized_bytes = materialization.bytes,
        .spilled = spill_required,
        .spilled_rows = if (spill_required) materialization.rows else 0,
        .spilled_bytes = if (spill_required) materialization.bytes else 0,
    };
}

fn routedRowsQueryPlanCanUseStreamingCountOnly(plan: db_mod.types.RelationalRowsQueryPlan) bool {
    return plan.ctes.len == 0 and
        plan.query.source_cte.len == 0 and
        relational_rows_api.rowsQueryCanUseCountOnlyResultForRouting(plan.query);
}

fn routedRowsQueryPlanCanUseStreamingBoundedSorted(plan: db_mod.types.RelationalRowsQueryPlan) bool {
    return plan.ctes.len == 0 and
        plan.query.source_cte.len == 0 and
        relational_rows_api.rowsQueryCanUseBoundedSortedResultForRouting(plan.query);
}

fn routedRowsQueryPlanCanUseStreamingCteBoundedSorted(
    schema: storage_schema.TableSchema,
    plan: db_mod.types.RelationalRowsQueryPlan,
) bool {
    if (plan.ctes.len != 1) return false;
    const cte = plan.ctes[0];
    if (cte.name.len == 0 or !std.mem.eql(u8, plan.query.source_cte, cte.name)) return false;
    if (!relational_rows_api.rowsQueryCanUseBoundedSortedResultForRouting(plan.query)) return false;
    if (!routedRowsQueryRequestHasNoFilters(plan.query)) return false;
    if (!routedRowsQueryRequestHasSimpleProjection(plan.query)) return false;
    if (!routedRowsQueryOrdersUseSimpleFields(plan.query.order_by)) return false;
    if (!routedRowsQueryFieldsExistInCteOutput(schema, plan.query, cte.query)) return false;
    if (cte.join != null or cte.lateral != null or cte.table_function != null) return false;
    if (cte.max_rows != null or cte.max_bytes != null or cte.spill_after_bytes != null) return false;
    if (!routedRowsQueryRequestCanStreamAsRowPreservingFilter(cte.query)) return false;
    return routedRowsSelectFieldsExist(schema, cte.query.select);
}

fn routedRowsQueryPlanCanUseStreamingCteCountOnly(
    schema: storage_schema.TableSchema,
    plan: db_mod.types.RelationalRowsQueryPlan,
) bool {
    if (plan.ctes.len != 1) return false;
    const cte = plan.ctes[0];
    if (cte.name.len == 0 or !std.mem.eql(u8, plan.query.source_cte, cte.name)) return false;
    if (!relational_rows_api.rowsQueryCanUseCountOnlyResultForRouting(plan.query)) return false;
    if (!routedRowsQueryRequestHasNoFilters(plan.query)) return false;
    if (cte.join != null or cte.lateral != null or cte.table_function != null) return false;
    if (cte.max_rows != null or cte.max_bytes != null or cte.spill_after_bytes != null) return false;
    if (!routedRowsQueryRequestCanStreamAsRowPreservingFilter(cte.query)) return false;
    return routedRowsSelectFieldsExist(schema, cte.query.select);
}

fn routedRowsCteCanStreamAsCountFilter(
    schema: storage_schema.TableSchema,
    cte: db_mod.types.RelationalRowsCte,
) bool {
    if (cte.name.len == 0) return false;
    if (cte.join != null or cte.lateral != null or cte.table_function != null) return false;
    if (cte.max_rows != null or cte.max_bytes != null or cte.spill_after_bytes != null) return false;
    var local_query = cte.query;
    local_query.source_cte = "";
    if (!routedRowsQueryRequestCanStreamAsRowPreservingFilter(local_query)) return false;
    if (!routedRowsSelectFieldsExist(schema, cte.query.select)) return false;
    return routedRowsQueryFiltersUseSchemaFields(schema, cte.query);
}

fn routedRowsQueryRequestCanStreamAsRowPreservingFilter(req: db_mod.types.RelationalRowsQueryRequest) bool {
    return req.source_cte.len == 0 and
        req.row_claim == null and
        req.doc_key_range == null and
        req.distinct_on.len == 0 and
        req.distinct_on_expressions.len == 0 and
        req.order_by.len == 0 and
        req.limit == null and
        req.offset == 0 and
        req.json_extract.len == 0 and
        req.array_length.len == 0 and
        req.coalesce.len == 0 and
        req.field_aliases.len == 0 and
        req.expressions.len == 0 and
        req.scalar_subqueries.len == 0 and
        req.subquery_predicates.len == 0;
}

fn routedRowsQueryFiltersUseSchemaFields(schema: storage_schema.TableSchema, req: db_mod.types.RelationalRowsQueryRequest) bool {
    if (req.expression_predicates.len != 0 or
        req.expression_or_predicates.len != 0 or
        req.expression_not_predicates.len != 0 or
        req.expression_array_contains.len != 0) return false;
    for (req.predicates) |predicate| if (!routedRowsFieldExists(schema, predicate.field)) return false;
    for (req.array_any) |predicate| if (!routedRowsFieldExists(schema, predicate.field)) return false;
    for (req.array_contains) |predicate| if (!routedRowsFieldExists(schema, predicate.field)) return false;
    for (req.array_eq) |predicate| if (!routedRowsFieldExists(schema, predicate.field)) return false;
    for (req.in_predicates) |predicate| if (!routedRowsFieldExists(schema, predicate.field)) return false;
    for (req.json_contains) |predicate| if (!routedRowsFieldExists(schema, predicate.field)) return false;
    for (req.json_path_eq) |predicate| if (!routedRowsFieldExists(schema, predicate.field)) return false;
    for (req.json_path_exists) |predicate| if (!routedRowsFieldExists(schema, predicate.field)) return false;
    for (req.text_patterns) |predicate| if (!routedRowsFieldExists(schema, predicate.field)) return false;
    for (req.or_predicates) |group| if (!routedRowsPredicateGroupUsesSchemaFields(schema, group)) return false;
    for (req.not_predicates) |group| if (!routedRowsPredicateGroupUsesSchemaFields(schema, group)) return false;
    for (req.access_or_predicates) |group| if (!routedRowsAccessPredicateGroupUsesSchemaFields(schema, group)) return false;
    for (req.access_not_predicates) |group| if (!routedRowsAccessPredicateGroupUsesSchemaFields(schema, group)) return false;
    return true;
}

fn routedRowsPredicateGroupUsesSchemaFields(schema: storage_schema.TableSchema, group: db_mod.types.RelationalRowsPredicateGroup) bool {
    for (group.predicates) |predicate| if (!routedRowsFieldExists(schema, predicate.field)) return false;
    return true;
}

fn routedRowsAccessPredicateGroupUsesSchemaFields(schema: storage_schema.TableSchema, group: db_mod.types.RelationalRowsAccessPredicateGroup) bool {
    for (group.predicates) |predicate| if (!routedRowsFieldExists(schema, predicate.field)) return false;
    for (group.array_any) |predicate| if (!routedRowsFieldExists(schema, predicate.field)) return false;
    for (group.array_contains) |predicate| if (!routedRowsFieldExists(schema, predicate.field)) return false;
    for (group.array_eq) |predicate| if (!routedRowsFieldExists(schema, predicate.field)) return false;
    for (group.in_predicates) |predicate| if (!routedRowsFieldExists(schema, predicate.field)) return false;
    for (group.json_contains) |predicate| if (!routedRowsFieldExists(schema, predicate.field)) return false;
    for (group.json_path_eq) |predicate| if (!routedRowsFieldExists(schema, predicate.field)) return false;
    for (group.json_path_exists) |predicate| if (!routedRowsFieldExists(schema, predicate.field)) return false;
    for (group.text_patterns) |predicate| if (!routedRowsFieldExists(schema, predicate.field)) return false;
    return true;
}

fn routedRowsQueryRequestHasNoFilters(req: db_mod.types.RelationalRowsQueryRequest) bool {
    return req.predicates.len == 0 and
        req.array_any.len == 0 and
        req.array_contains.len == 0 and
        req.array_eq.len == 0 and
        req.in_predicates.len == 0 and
        req.json_contains.len == 0 and
        req.json_path_eq.len == 0 and
        req.json_path_exists.len == 0 and
        req.text_patterns.len == 0 and
        req.or_predicates.len == 0 and
        req.not_predicates.len == 0 and
        req.access_or_predicates.len == 0 and
        req.access_not_predicates.len == 0 and
        req.expression_predicates.len == 0 and
        req.expression_or_predicates.len == 0 and
        req.expression_not_predicates.len == 0 and
        req.expression_array_contains.len == 0 and
        req.subquery_predicates.len == 0;
}

fn routedRowsQueryRequestHasSimpleProjection(req: db_mod.types.RelationalRowsQueryRequest) bool {
    return req.json_extract.len == 0 and
        req.array_length.len == 0 and
        req.coalesce.len == 0 and
        req.field_aliases.len == 0 and
        req.expressions.len == 0;
}

fn routedRowsQueryOrdersUseSimpleFields(order_by: []const db_mod.types.RelationalRowsQueryOrder) bool {
    for (order_by) |order| {
        if (order.field.len == 0 or order.expression != null) return false;
    }
    return true;
}

fn routedRowsQueryFieldsExistInCteOutput(
    schema: storage_schema.TableSchema,
    req: db_mod.types.RelationalRowsQueryRequest,
    cte_query: db_mod.types.RelationalRowsQueryRequest,
) bool {
    if (cte_query.select_all) {
        if (!routedRowsSelectFieldsExist(schema, req.select)) return false;
        for (req.order_by) |order| {
            if (!routedRowsFieldExists(schema, order.field)) return false;
        }
        return true;
    }
    for (req.select) |field| {
        if (!routedRowsFieldInSelection(field, cte_query.select)) return false;
    }
    for (req.order_by) |order| {
        if (!routedRowsFieldInSelection(order.field, cte_query.select)) return false;
    }
    return true;
}

fn routedRowsFieldInSelection(field: []const u8, selected: []const []const u8) bool {
    for (selected) |candidate| {
        if (std.mem.eql(u8, field, candidate)) return true;
    }
    return false;
}

fn routedRowsSelectFieldsExist(schema: storage_schema.TableSchema, fields: []const []const u8) bool {
    if (fields.len == 0) return true;
    for (fields) |field| {
        if (!routedRowsFieldExists(schema, field)) return false;
    }
    return true;
}

fn routedRowsFieldExists(schema: storage_schema.TableSchema, field: []const u8) bool {
    for (schema.relational_columns) |column| {
        if (std.mem.eql(u8, field, column.name) or std.mem.eql(u8, field, column.path)) return true;
    }
    return false;
}

fn countRowsQueryFromRoutedScansAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    schema: storage_schema.TableSchema,
    req: db_mod.types.RelationalRowsQueryRequest,
    ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsQueryResult {
    var total: u32 = 0;
    var scanned_rows: u64 = 0;
    var saw_source = false;
    if (ranges.len == 0) {
        const counted = (try countRowsQueryFromRoutedScanAlloc(alloc, source, table_name, schema, req, "", "", consistency, &scanned_rows)) orelse return null;
        saw_source = true;
        total = try addRoutedRowsCount(total, counted);
    } else {
        for (ranges) |range| {
            const counted = (try countRowsQueryFromRoutedScanAlloc(alloc, source, table_name, schema, req, range.start, range.end, consistency, &scanned_rows)) orelse continue;
            saw_source = true;
            total = try addRoutedRowsCount(total, counted);
        }
    }
    if (!saw_source) return null;
    return .{
        .rows = &.{},
        .total = total,
        .total_exact = true,
        .include_profile = req.profile,
        .profile = .{
            .access_method = .base_scan,
            .total_mode = req.total_mode,
            .count_only = true,
            .base_scan_rows = scanned_rows,
            .candidate_rows = scanned_rows,
            .iterator_seeks = if (ranges.len == 0) 1 else @intCast(ranges.len),
        },
    };
}

fn countRowsQueryFromRoutedScansThroughCteChainAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    schema: storage_schema.TableSchema,
    plan: db_mod.types.RelationalRowsQueryPlan,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsQueryResult {
    if (!relational_rows_api.rowsQueryCanUseCountOnlyResultForRouting(plan.query)) return null;
    if (!routedRowsQueryFiltersUseSchemaFields(schema, plan.query)) return null;
    if (plan.query.source_cte.len == 0 or plan.ctes.len == 0) return null;

    var chain = std.ArrayListUnmanaged(db_mod.types.RelationalRowsCte).empty;
    defer chain.deinit(alloc);

    var source_cte = plan.query.source_cte;
    while (source_cte.len != 0) {
        if (chain.items.len >= plan.ctes.len) return null;
        const cte = routedRowsFindCte(plan.ctes, source_cte) orelse return null;
        if (!routedRowsCteCanStreamAsCountFilter(schema, cte)) return null;
        try chain.append(alloc, cte);
        source_cte = cte.query.source_cte;
    }
    if (chain.items.len == 0) return null;

    var predicates = std.ArrayListUnmanaged(storage_schema.RelationalCheck).empty;
    defer predicates.deinit(alloc);
    var array_any = std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayAnyPredicate).empty;
    defer array_any.deinit(alloc);
    var array_contains = std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayContainsPredicate).empty;
    defer array_contains.deinit(alloc);
    var array_eq = std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayEqPredicate).empty;
    defer array_eq.deinit(alloc);
    var in_predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsInPredicate).empty;
    defer in_predicates.deinit(alloc);
    var json_contains = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonContainsPredicate).empty;
    defer json_contains.deinit(alloc);
    var json_path_eq = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathEqPredicate).empty;
    defer json_path_eq.deinit(alloc);
    var json_path_exists = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathExistsPredicate).empty;
    defer json_path_exists.deinit(alloc);
    var text_patterns = std.ArrayListUnmanaged(db_mod.types.RelationalRowsTextPatternPredicate).empty;
    defer text_patterns.deinit(alloc);
    var or_predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup).empty;
    defer or_predicates.deinit(alloc);
    var not_predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup).empty;
    defer not_predicates.deinit(alloc);
    var access_or_predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsAccessPredicateGroup).empty;
    defer access_or_predicates.deinit(alloc);
    var access_not_predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsAccessPredicateGroup).empty;
    defer access_not_predicates.deinit(alloc);
    var expression_predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCondition).empty;
    defer expression_predicates.deinit(alloc);
    var expression_or_predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup).empty;
    defer expression_or_predicates.deinit(alloc);
    var expression_not_predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup).empty;
    defer expression_not_predicates.deinit(alloc);
    var expression_array_contains = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionArrayContainsPredicate).empty;
    defer expression_array_contains.deinit(alloc);

    var reverse_index = chain.items.len;
    while (reverse_index > 0) {
        reverse_index -= 1;
        const req = chain.items[reverse_index].query;
        try predicates.appendSlice(alloc, req.predicates);
        try array_any.appendSlice(alloc, req.array_any);
        try array_contains.appendSlice(alloc, req.array_contains);
        try array_eq.appendSlice(alloc, req.array_eq);
        try in_predicates.appendSlice(alloc, req.in_predicates);
        try json_contains.appendSlice(alloc, req.json_contains);
        try json_path_eq.appendSlice(alloc, req.json_path_eq);
        try json_path_exists.appendSlice(alloc, req.json_path_exists);
        try text_patterns.appendSlice(alloc, req.text_patterns);
        try or_predicates.appendSlice(alloc, req.or_predicates);
        try not_predicates.appendSlice(alloc, req.not_predicates);
        try access_or_predicates.appendSlice(alloc, req.access_or_predicates);
        try access_not_predicates.appendSlice(alloc, req.access_not_predicates);
        try expression_predicates.appendSlice(alloc, req.expression_predicates);
        try expression_or_predicates.appendSlice(alloc, req.expression_or_predicates);
        try expression_not_predicates.appendSlice(alloc, req.expression_not_predicates);
        try expression_array_contains.appendSlice(alloc, req.expression_array_contains);
    }
    try predicates.appendSlice(alloc, plan.query.predicates);
    try array_any.appendSlice(alloc, plan.query.array_any);
    try array_contains.appendSlice(alloc, plan.query.array_contains);
    try array_eq.appendSlice(alloc, plan.query.array_eq);
    try in_predicates.appendSlice(alloc, plan.query.in_predicates);
    try json_contains.appendSlice(alloc, plan.query.json_contains);
    try json_path_eq.appendSlice(alloc, plan.query.json_path_eq);
    try json_path_exists.appendSlice(alloc, plan.query.json_path_exists);
    try text_patterns.appendSlice(alloc, plan.query.text_patterns);
    try or_predicates.appendSlice(alloc, plan.query.or_predicates);
    try not_predicates.appendSlice(alloc, plan.query.not_predicates);
    try access_or_predicates.appendSlice(alloc, plan.query.access_or_predicates);
    try access_not_predicates.appendSlice(alloc, plan.query.access_not_predicates);
    try expression_predicates.appendSlice(alloc, plan.query.expression_predicates);
    try expression_or_predicates.appendSlice(alloc, plan.query.expression_or_predicates);
    try expression_not_predicates.appendSlice(alloc, plan.query.expression_not_predicates);
    try expression_array_contains.appendSlice(alloc, plan.query.expression_array_contains);

    const merged_req = db_mod.types.RelationalRowsQueryRequest{
        .predicates = predicates.items,
        .array_any = array_any.items,
        .array_contains = array_contains.items,
        .array_eq = array_eq.items,
        .in_predicates = in_predicates.items,
        .json_contains = json_contains.items,
        .json_path_eq = json_path_eq.items,
        .json_path_exists = json_path_exists.items,
        .text_patterns = text_patterns.items,
        .or_predicates = or_predicates.items,
        .not_predicates = not_predicates.items,
        .access_or_predicates = access_or_predicates.items,
        .access_not_predicates = access_not_predicates.items,
        .expression_predicates = expression_predicates.items,
        .expression_or_predicates = expression_or_predicates.items,
        .expression_not_predicates = expression_not_predicates.items,
        .expression_array_contains = expression_array_contains.items,
        .limit = 0,
        .total_mode = .exact,
        .profile = plan.query.profile,
    };
    return try countRowsQueryFromRoutedScansAlloc(alloc, source, table_name, schema, merged_req, plan.ranges, consistency);
}

fn routedRowsFindCte(ctes: []const db_mod.types.RelationalRowsCte, name: []const u8) ?db_mod.types.RelationalRowsCte {
    for (ctes) |cte| {
        if (std.mem.eql(u8, cte.name, name)) return cte;
    }
    return null;
}

fn addRoutedRowsCount(current: u32, delta: u32) !u32 {
    return std.math.add(u32, current, delta) catch error.UnsupportedRowsQuery;
}

fn routedRowsSetOperationPlanCanUseCountOnly(plan: db_mod.types.RelationalRowsSetOperationPlan) bool {
    if (plan.operation != .union_all and
        plan.operation != .union_distinct and
        plan.operation != .intersect and
        plan.operation != .except) return false;
    if (plan.order_by.len != 0 or plan.limit != null or plan.offset != 0) return false;
    if (!relational_rows_api.rowsQueryCanUseCountOnlyResultForRouting(plan.left.query)) return false;
    if (!relational_rows_api.rowsQueryCanUseCountOnlyResultForRouting(plan.right.query)) return false;
    if (plan.ctes.len != 0 and (plan.left.ctes.len != 0 or plan.right.ctes.len != 0)) return false;
    return true;
}

fn rowsUnionAllCountOnlyPlanFromRoutedScansAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    runtime_schema: storage_schema.TableSchema,
    plan: db_mod.types.RelationalRowsSetOperationPlan,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsQueryResult {
    if (plan.operation != .union_all or !routedRowsSetOperationPlanCanUseCountOnly(plan)) return null;

    var left_plan = plan.left;
    var right_plan = plan.right;
    if (plan.ctes.len != 0) {
        left_plan.ctes = plan.ctes;
        right_plan.ctes = plan.ctes;
    }

    var left = (try rowsQueryPlanFromRoutedScansAlloc(alloc, source, table_name, runtime_schema, left_plan, consistency)) orelse return null;
    defer left.deinit(alloc);
    if (left.rows.len != 0 or !left.total_exact) return null;

    var right = (try rowsQueryPlanFromRoutedScansAlloc(alloc, source, table_name, runtime_schema, right_plan, consistency)) orelse return null;
    defer right.deinit(alloc);
    if (right.rows.len != 0 or !right.total_exact) return null;

    var profile = db_mod.types.RelationalRowsQueryResult.Profile{
        .access_method = .base_scan,
        .total_mode = .exact,
        .count_only = true,
    };
    profile.base_scan_rows = try std.math.add(u64, left.profile.base_scan_rows, right.profile.base_scan_rows);
    profile.candidate_rows = try std.math.add(u64, left.profile.candidate_rows, right.profile.candidate_rows);
    profile.iterator_seeks = try std.math.add(u64, left.profile.iterator_seeks, right.profile.iterator_seeks);
    profile.covering_payload_rows = try std.math.add(u64, left.profile.covering_payload_rows, right.profile.covering_payload_rows);
    profile.covering_payload_rechecked_rows = try std.math.add(u64, left.profile.covering_payload_rechecked_rows, right.profile.covering_payload_rechecked_rows);
    profile.covering_payload_hydration_avoided_rows = try std.math.add(u64, left.profile.covering_payload_hydration_avoided_rows, right.profile.covering_payload_hydration_avoided_rows);
    profile.covering_payload_fallback_metadata_missing_rows = try std.math.add(u64, left.profile.covering_payload_fallback_metadata_missing_rows, right.profile.covering_payload_fallback_metadata_missing_rows);
    profile.covering_payload_fallback_row_generation_mismatch_rows = try std.math.add(u64, left.profile.covering_payload_fallback_row_generation_mismatch_rows, right.profile.covering_payload_fallback_row_generation_mismatch_rows);
    profile.covering_payload_fallback_index_generation_mismatch_rows = try std.math.add(u64, left.profile.covering_payload_fallback_index_generation_mismatch_rows, right.profile.covering_payload_fallback_index_generation_mismatch_rows);
    profile.covering_payload_fallback_schema_fingerprint_mismatch_rows = try std.math.add(u64, left.profile.covering_payload_fallback_schema_fingerprint_mismatch_rows, right.profile.covering_payload_fallback_schema_fingerprint_mismatch_rows);
    profile.covering_payload_fallback_residual_predicate_rows = try std.math.add(u64, left.profile.covering_payload_fallback_residual_predicate_rows, right.profile.covering_payload_fallback_residual_predicate_rows);
    profile.covering_payload_fallback_projection_shape_rows = try std.math.add(u64, left.profile.covering_payload_fallback_projection_shape_rows, right.profile.covering_payload_fallback_projection_shape_rows);
    profile.routed_materialization_fallbacks = try std.math.add(u64, left.profile.routed_materialization_fallbacks, right.profile.routed_materialization_fallbacks);
    profile.routed_materialized_rows = try std.math.add(u64, left.profile.routed_materialized_rows, right.profile.routed_materialized_rows);
    profile.routed_materialized_bytes = try std.math.add(u64, left.profile.routed_materialized_bytes, right.profile.routed_materialized_bytes);
    profile.routed_spill_count = try std.math.add(u64, left.profile.routed_spill_count, right.profile.routed_spill_count);
    profile.routed_spilled_rows = try std.math.add(u64, left.profile.routed_spilled_rows, right.profile.routed_spilled_rows);
    profile.routed_spilled_bytes = try std.math.add(u64, left.profile.routed_spilled_bytes, right.profile.routed_spilled_bytes);

    return .{
        .rows = &.{},
        .total = try addRoutedRowsCount(left.total, right.total),
        .total_exact = true,
        .include_profile = left.include_profile or right.include_profile,
        .profile = profile,
    };
}

fn routedRowsFreeStringSet(alloc: std.mem.Allocator, set: *std.StringHashMapUnmanaged(void)) void {
    var keys = set.keyIterator();
    while (keys.next()) |key| alloc.free(@constCast(key.*));
    set.deinit(alloc);
}

fn routedRowsPutStringSetKeyAlloc(
    alloc: std.mem.Allocator,
    set: *std.StringHashMapUnmanaged(void),
    key_value: []const u8,
) !bool {
    if (set.contains(key_value)) return false;
    const key = try alloc.dupe(u8, key_value);
    errdefer alloc.free(key);
    const gop = try set.getOrPut(alloc, key);
    if (gop.found_existing) {
        alloc.free(key);
        return false;
    }
    return true;
}

fn routedRowsStringSetCountAsU32(set: std.StringHashMapUnmanaged(void)) !u32 {
    return std.math.cast(u32, set.count()) orelse error.UnsupportedRowsQuery;
}

fn routedRowsSetMembershipCountAsU32(
    included: std.StringHashMapUnmanaged(void),
    probe: std.StringHashMapUnmanaged(void),
    want_present: bool,
) !u32 {
    var total: u32 = 0;
    var keys = included.keyIterator();
    while (keys.next()) |key| {
        if (probe.contains(key.*) == want_present) {
            total = try addRoutedRowsCount(total, 1);
        }
    }
    return total;
}

const RoutedRowsBorrowedFilterLists = struct {
    predicates: std.ArrayListUnmanaged(storage_schema.RelationalCheck) = .empty,
    array_any: std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayAnyPredicate) = .empty,
    array_contains: std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayContainsPredicate) = .empty,
    array_eq: std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayEqPredicate) = .empty,
    in_predicates: std.ArrayListUnmanaged(db_mod.types.RelationalRowsInPredicate) = .empty,
    json_contains: std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonContainsPredicate) = .empty,
    json_path_eq: std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathEqPredicate) = .empty,
    json_path_exists: std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathExistsPredicate) = .empty,
    text_patterns: std.ArrayListUnmanaged(db_mod.types.RelationalRowsTextPatternPredicate) = .empty,
    or_predicates: std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup) = .empty,
    not_predicates: std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup) = .empty,
    access_or_predicates: std.ArrayListUnmanaged(db_mod.types.RelationalRowsAccessPredicateGroup) = .empty,
    access_not_predicates: std.ArrayListUnmanaged(db_mod.types.RelationalRowsAccessPredicateGroup) = .empty,
    expression_predicates: std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCondition) = .empty,
    expression_or_predicates: std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup) = .empty,
    expression_not_predicates: std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionPredicateGroup) = .empty,
    expression_array_contains: std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionArrayContainsPredicate) = .empty,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.predicates.deinit(alloc);
        self.array_any.deinit(alloc);
        self.array_contains.deinit(alloc);
        self.array_eq.deinit(alloc);
        self.in_predicates.deinit(alloc);
        self.json_contains.deinit(alloc);
        self.json_path_eq.deinit(alloc);
        self.json_path_exists.deinit(alloc);
        self.text_patterns.deinit(alloc);
        self.or_predicates.deinit(alloc);
        self.not_predicates.deinit(alloc);
        self.access_or_predicates.deinit(alloc);
        self.access_not_predicates.deinit(alloc);
        self.expression_predicates.deinit(alloc);
        self.expression_or_predicates.deinit(alloc);
        self.expression_not_predicates.deinit(alloc);
        self.expression_array_contains.deinit(alloc);
    }

    fn append(self: *@This(), alloc: std.mem.Allocator, req: db_mod.types.RelationalRowsQueryRequest) !void {
        try self.predicates.appendSlice(alloc, req.predicates);
        try self.array_any.appendSlice(alloc, req.array_any);
        try self.array_contains.appendSlice(alloc, req.array_contains);
        try self.array_eq.appendSlice(alloc, req.array_eq);
        try self.in_predicates.appendSlice(alloc, req.in_predicates);
        try self.json_contains.appendSlice(alloc, req.json_contains);
        try self.json_path_eq.appendSlice(alloc, req.json_path_eq);
        try self.json_path_exists.appendSlice(alloc, req.json_path_exists);
        try self.text_patterns.appendSlice(alloc, req.text_patterns);
        try self.or_predicates.appendSlice(alloc, req.or_predicates);
        try self.not_predicates.appendSlice(alloc, req.not_predicates);
        try self.access_or_predicates.appendSlice(alloc, req.access_or_predicates);
        try self.access_not_predicates.appendSlice(alloc, req.access_not_predicates);
        try self.expression_predicates.appendSlice(alloc, req.expression_predicates);
        try self.expression_or_predicates.appendSlice(alloc, req.expression_or_predicates);
        try self.expression_not_predicates.appendSlice(alloc, req.expression_not_predicates);
        try self.expression_array_contains.appendSlice(alloc, req.expression_array_contains);
    }

    fn query(self: *@This(), base: db_mod.types.RelationalRowsQueryRequest) db_mod.types.RelationalRowsQueryRequest {
        var out = base;
        out.source_cte = "";
        out.predicates = self.predicates.items;
        out.array_any = self.array_any.items;
        out.array_contains = self.array_contains.items;
        out.array_eq = self.array_eq.items;
        out.in_predicates = self.in_predicates.items;
        out.json_contains = self.json_contains.items;
        out.json_path_eq = self.json_path_eq.items;
        out.json_path_exists = self.json_path_exists.items;
        out.text_patterns = self.text_patterns.items;
        out.or_predicates = self.or_predicates.items;
        out.not_predicates = self.not_predicates.items;
        out.access_or_predicates = self.access_or_predicates.items;
        out.access_not_predicates = self.access_not_predicates.items;
        out.expression_predicates = self.expression_predicates.items;
        out.expression_or_predicates = self.expression_or_predicates.items;
        out.expression_not_predicates = self.expression_not_predicates.items;
        out.expression_array_contains = self.expression_array_contains.items;
        out.limit = null;
        out.total_mode = .exact;
        out.profile = false;
        return out;
    }
};

fn routedRowsUnionDistinctMergedCteBranchQueryAlloc(
    alloc: std.mem.Allocator,
    schema: storage_schema.TableSchema,
    plan: db_mod.types.RelationalRowsQueryPlan,
    filters: *RoutedRowsBorrowedFilterLists,
) !?db_mod.types.RelationalRowsQueryRequest {
    if (plan.query.source_cte.len == 0) return null;
    if (!routedRowsQueryFiltersUseSchemaFields(schema, plan.query)) return null;

    var chain = std.ArrayListUnmanaged(db_mod.types.RelationalRowsCte).empty;
    defer chain.deinit(alloc);

    var source_cte = plan.query.source_cte;
    while (source_cte.len != 0) {
        if (chain.items.len >= plan.ctes.len) return null;
        const cte = routedRowsFindCte(plan.ctes, source_cte) orelse return null;
        if (!routedRowsCteCanStreamAsCountFilter(schema, cte)) return null;
        try chain.append(alloc, cte);
        source_cte = cte.query.source_cte;
    }
    if (chain.items.len == 0) return null;
    if (!routedRowsQueryFieldsExistInCteOutput(schema, plan.query, chain.items[0].query)) return null;

    var reverse_index = chain.items.len;
    while (reverse_index > 0) {
        reverse_index -= 1;
        try filters.append(alloc, chain.items[reverse_index].query);
    }
    try filters.append(alloc, plan.query);
    return filters.query(plan.query);
}

fn appendUnionDistinctProjectedRowsFromRoutedScanAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    schema: storage_schema.TableSchema,
    req: db_mod.types.RelationalRowsQueryRequest,
    from_key: []const u8,
    to_key: []const u8,
    consistency: raft_mod.ReadConsistency,
    seen: *std.StringHashMapUnmanaged(void),
    profile: *db_mod.types.RelationalRowsQueryResult.Profile,
) !bool {
    var scan_result = (try source.scan(alloc, table_name, from_key, to_key, .{
        .include_documents = true,
        .include_all_fields = true,
    }, consistency)) orelse return false;
    defer scan_result.deinit(alloc);

    var lines = std.mem.splitScalar(u8, scan_result.ndjson, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        var row = try mergeScanRowFromScanLineAlloc(alloc, line);
        defer row.deinit(alloc);
        profile.base_scan_rows += 1;
        profile.candidate_rows += 1;

        var lookup = (try source.lookup(alloc, table_name, row.key, .{ .include_all_fields = true }, consistency)) orelse return error.TopologyChanged;
        defer lookup.deinit(alloc);
        if (row.version != lookup.version) return error.TopologyChanged;
        if (!std.mem.eql(u8, row.json, lookup.json)) return error.TopologyChanged;

        var branch_req = req;
        branch_req.source_cte = "";
        branch_req.limit = null;
        branch_req.total_mode = .exact;
        branch_req.profile = false;
        const branch_rows = [_][]const u8{lookup.json};
        var projected = try relational_rows_api.executeRowsQueryOnJsonRowsAlloc(alloc, schema, branch_req, branch_rows[0..]);
        defer projected.deinit(alloc);
        profile.hydrated_rows += 1;
        profile.projected_rows += @intCast(projected.rows.len);
        for (projected.rows) |projected_row| {
            _ = try routedRowsPutStringSetKeyAlloc(alloc, seen, projected_row);
        }
    }
    return true;
}

fn appendUnionDistinctProjectedRowsFromRoutedScansAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    schema: storage_schema.TableSchema,
    plan: db_mod.types.RelationalRowsQueryPlan,
    consistency: raft_mod.ReadConsistency,
    seen: *std.StringHashMapUnmanaged(void),
    profile: *db_mod.types.RelationalRowsQueryResult.Profile,
) !bool {
    var filters = RoutedRowsBorrowedFilterLists{};
    defer filters.deinit(alloc);
    var query = plan.query;
    if (plan.query.source_cte.len != 0) {
        query = (try routedRowsUnionDistinctMergedCteBranchQueryAlloc(alloc, schema, plan, &filters)) orelse return false;
    } else if (plan.ctes.len != 0) {
        return false;
    }
    var saw_source = false;
    if (plan.ranges.len == 0) {
        saw_source = try appendUnionDistinctProjectedRowsFromRoutedScanAlloc(alloc, source, table_name, schema, query, "", "", consistency, seen, profile);
    } else {
        for (plan.ranges) |range| {
            saw_source = (try appendUnionDistinctProjectedRowsFromRoutedScanAlloc(alloc, source, table_name, schema, query, range.start, range.end, consistency, seen, profile)) or saw_source;
        }
    }
    profile.iterator_seeks += if (plan.ranges.len == 0) 1 else @intCast(plan.ranges.len);
    return saw_source;
}

fn rowsUnionDistinctCountOnlyPlanFromRoutedScansAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    runtime_schema: storage_schema.TableSchema,
    plan: db_mod.types.RelationalRowsSetOperationPlan,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsQueryResult {
    if (plan.operation != .union_distinct or !routedRowsSetOperationPlanCanUseCountOnly(plan)) return null;

    var seen = std.StringHashMapUnmanaged(void).empty;
    defer routedRowsFreeStringSet(alloc, &seen);

    var profile = db_mod.types.RelationalRowsQueryResult.Profile{
        .access_method = .base_scan,
        .total_mode = .exact,
        .count_only = true,
    };
    var left_plan = plan.left;
    var right_plan = plan.right;
    if (plan.ctes.len != 0) {
        left_plan.ctes = plan.ctes;
        right_plan.ctes = plan.ctes;
    }
    var saw_source = try appendUnionDistinctProjectedRowsFromRoutedScansAlloc(alloc, source, table_name, runtime_schema, left_plan, consistency, &seen, &profile);
    saw_source = (try appendUnionDistinctProjectedRowsFromRoutedScansAlloc(alloc, source, table_name, runtime_schema, right_plan, consistency, &seen, &profile)) or saw_source;
    if (!saw_source) return null;

    return .{
        .rows = &.{},
        .total = try routedRowsStringSetCountAsU32(seen),
        .total_exact = true,
        .include_profile = plan.left.query.profile or plan.right.query.profile,
        .profile = profile,
    };
}

fn rowsIntersectExceptCountOnlyPlanFromRoutedScansAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    runtime_schema: storage_schema.TableSchema,
    plan: db_mod.types.RelationalRowsSetOperationPlan,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsQueryResult {
    if ((plan.operation != .intersect and plan.operation != .except) or !routedRowsSetOperationPlanCanUseCountOnly(plan)) return null;

    var left_seen = std.StringHashMapUnmanaged(void).empty;
    defer routedRowsFreeStringSet(alloc, &left_seen);
    var right_seen = std.StringHashMapUnmanaged(void).empty;
    defer routedRowsFreeStringSet(alloc, &right_seen);

    var profile = db_mod.types.RelationalRowsQueryResult.Profile{
        .access_method = .base_scan,
        .total_mode = .exact,
        .count_only = true,
    };
    var left_plan = plan.left;
    var right_plan = plan.right;
    if (plan.ctes.len != 0) {
        left_plan.ctes = plan.ctes;
        right_plan.ctes = plan.ctes;
    }
    const saw_left = try appendUnionDistinctProjectedRowsFromRoutedScansAlloc(alloc, source, table_name, runtime_schema, left_plan, consistency, &left_seen, &profile);
    const saw_right = try appendUnionDistinctProjectedRowsFromRoutedScansAlloc(alloc, source, table_name, runtime_schema, right_plan, consistency, &right_seen, &profile);
    if (!saw_left and !saw_right) return null;

    const total = switch (plan.operation) {
        .intersect => try routedRowsSetMembershipCountAsU32(left_seen, right_seen, true),
        .except => try routedRowsSetMembershipCountAsU32(left_seen, right_seen, false),
        else => unreachable,
    };
    return .{
        .rows = &.{},
        .total = total,
        .total_exact = true,
        .include_profile = plan.left.query.profile or plan.right.query.profile,
        .profile = profile,
    };
}

fn boundedSortedRowsQueryFromRoutedScansAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    schema: storage_schema.TableSchema,
    req: db_mod.types.RelationalRowsQueryRequest,
    ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsQueryResult {
    var stream = try relational_rows_api.RowsQueryBoundedSortedStream.init(schema, req);
    defer stream.deinit(alloc);

    var fingerprints = std.ArrayListUnmanaged(RoutedScanRowFingerprint).empty;
    defer {
        for (fingerprints.items) |*fingerprint| fingerprint.deinit(alloc);
        fingerprints.deinit(alloc);
    }

    var saw_source = false;
    if (ranges.len == 0) {
        saw_source = try appendBoundedSortedRowsFromRoutedScanAlloc(alloc, source, table_name, "", "", &stream, &fingerprints, consistency);
    } else {
        for (ranges) |range| {
            saw_source = (try appendBoundedSortedRowsFromRoutedScanAlloc(alloc, source, table_name, range.start, range.end, &stream, &fingerprints, consistency)) or saw_source;
        }
    }
    if (!saw_source) return null;
    try verifyRoutedScanRangesUnchangedByFingerprintAlloc(alloc, source, table_name, ranges, fingerprints.items, consistency);
    return try stream.toResult(alloc);
}

fn boundedSortedRowsQueryFromRoutedScansThroughCteAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    schema: storage_schema.TableSchema,
    cte: db_mod.types.RelationalRowsCte,
    final_req: db_mod.types.RelationalRowsQueryRequest,
    ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsQueryResult {
    var local_req = final_req;
    local_req.source_cte = "";
    var stream = try relational_rows_api.RowsQueryBoundedSortedStream.init(schema, local_req);
    defer stream.deinit(alloc);

    var fingerprints = std.ArrayListUnmanaged(RoutedScanRowFingerprint).empty;
    defer {
        for (fingerprints.items) |*fingerprint| fingerprint.deinit(alloc);
        fingerprints.deinit(alloc);
    }

    var saw_source = false;
    if (ranges.len == 0) {
        saw_source = try appendBoundedSortedRowsFromRoutedScanThroughCteAlloc(alloc, source, table_name, schema, cte.query, "", "", &stream, &fingerprints, consistency);
    } else {
        for (ranges) |range| {
            saw_source = (try appendBoundedSortedRowsFromRoutedScanThroughCteAlloc(alloc, source, table_name, schema, cte.query, range.start, range.end, &stream, &fingerprints, consistency)) or saw_source;
        }
    }
    if (!saw_source) return null;
    try verifyRoutedScanRangesUnchangedByFingerprintAlloc(alloc, source, table_name, ranges, fingerprints.items, consistency);
    return try stream.toResult(alloc);
}

fn appendBoundedSortedRowsFromRoutedScanAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
    stream: *relational_rows_api.RowsQueryBoundedSortedStream,
    fingerprints: *std.ArrayListUnmanaged(RoutedScanRowFingerprint),
    consistency: raft_mod.ReadConsistency,
) !bool {
    var scan_result = (try source.scan(alloc, table_name, from_key, to_key, .{
        .include_documents = true,
        .include_all_fields = true,
    }, consistency)) orelse return false;
    defer scan_result.deinit(alloc);
    stream.recordIteratorSeek();

    var lines = std.mem.splitScalar(u8, scan_result.ndjson, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        var row = try mergeScanRowFromScanLineAlloc(alloc, line);
        defer row.deinit(alloc);

        var lookup = (try source.lookup(alloc, table_name, row.key, .{ .include_all_fields = true }, consistency)) orelse return error.TopologyChanged;
        defer lookup.deinit(alloc);
        if (row.version != lookup.version) return error.TopologyChanged;
        if (!std.mem.eql(u8, row.json, lookup.json)) return error.TopologyChanged;

        const key = try alloc.dupe(u8, row.key);
        errdefer alloc.free(key);
        try fingerprints.append(alloc, .{
            .key = key,
            .version = row.version,
            .json_hash = routedScanJsonHash(row.json),
        });
        try stream.appendScannedRowJsonAlloc(alloc, lookup.json);
    }
    return true;
}

fn appendBoundedSortedRowsFromRoutedScanThroughCteAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    schema: storage_schema.TableSchema,
    cte_query: db_mod.types.RelationalRowsQueryRequest,
    from_key: []const u8,
    to_key: []const u8,
    stream: *relational_rows_api.RowsQueryBoundedSortedStream,
    fingerprints: *std.ArrayListUnmanaged(RoutedScanRowFingerprint),
    consistency: raft_mod.ReadConsistency,
) !bool {
    var scan_result = (try source.scan(alloc, table_name, from_key, to_key, .{
        .include_documents = true,
        .include_all_fields = true,
    }, consistency)) orelse return false;
    defer scan_result.deinit(alloc);
    stream.recordIteratorSeek();

    var lines = std.mem.splitScalar(u8, scan_result.ndjson, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        var row = try mergeScanRowFromScanLineAlloc(alloc, line);
        defer row.deinit(alloc);

        var lookup = (try source.lookup(alloc, table_name, row.key, .{ .include_all_fields = true }, consistency)) orelse return error.TopologyChanged;
        defer lookup.deinit(alloc);
        if (row.version != lookup.version) return error.TopologyChanged;
        if (!std.mem.eql(u8, row.json, lookup.json)) return error.TopologyChanged;

        const key = try alloc.dupe(u8, row.key);
        errdefer alloc.free(key);
        try fingerprints.append(alloc, .{
            .key = key,
            .version = row.version,
            .json_hash = routedScanJsonHash(row.json),
        });

        const cte_rows = [_][]const u8{lookup.json};
        var cte_result = try relational_rows_api.executeRowsQueryOnJsonRowsAlloc(alloc, schema, cte_query, cte_rows[0..]);
        defer cte_result.deinit(alloc);
        if (cte_result.rows.len > 1) return error.UnsupportedRowsQuery;
        if (cte_result.rows.len == 1) try stream.appendScannedRowJsonAlloc(alloc, cte_result.rows[0]);
    }
    return true;
}

fn routedScanJsonHash(json: []const u8) u64 {
    return std.hash.Wyhash.hash(0, json);
}

fn countRowsQueryFromRoutedScanAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    schema: storage_schema.TableSchema,
    req: db_mod.types.RelationalRowsQueryRequest,
    from_key: []const u8,
    to_key: []const u8,
    consistency: raft_mod.ReadConsistency,
    scanned_rows: *u64,
) !?u32 {
    var scan_result = (try source.scan(alloc, table_name, from_key, to_key, .{
        .include_documents = true,
        .include_all_fields = true,
    }, consistency)) orelse return null;
    defer scan_result.deinit(alloc);

    var total: u32 = 0;
    var lines = std.mem.splitScalar(u8, scan_result.ndjson, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        var row = try mergeScanRowFromScanLineAlloc(alloc, line);
        defer row.deinit(alloc);
        scanned_rows.* += 1;

        var lookup = (try source.lookup(alloc, table_name, row.key, .{ .include_all_fields = true }, consistency)) orelse return error.TopologyChanged;
        defer lookup.deinit(alloc);
        if (row.version != lookup.version) return error.TopologyChanged;
        if (!std.mem.eql(u8, row.json, lookup.json)) return error.TopologyChanged;
        if (try relational_rows_api.rowsQueryJsonMatchesCountOnlyAlloc(alloc, schema, req, lookup.json)) {
            total = try addRoutedRowsCount(total, 1);
        }
    }
    return total;
}

fn verifyRoutedScanRowsStillCurrentAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    rows: []db_mod.types.RelationalRowsCollectedRow,
    consistency: raft_mod.ReadConsistency,
) !void {
    for (rows) |*row| {
        var lookup = (try source.lookup(alloc, table_name, row.key, .{ .include_all_fields = true }, consistency)) orelse return error.TopologyChanged;
        defer lookup.deinit(alloc);
        if (row.version != lookup.version) return error.TopologyChanged;
        if (!std.mem.eql(u8, lookup.json, row.json)) return error.TopologyChanged;
        row.version = lookup.version;
    }
}

fn routedRowsQueryRequestNeedsLivePaginationFence(req: db_mod.types.RelationalRowsQueryRequest) bool {
    return req.distinct_on.len != 0 or
        req.distinct_on_expressions.len != 0 or
        req.order_by.len != 0 or
        req.limit != null or
        req.offset != 0;
}

fn routedRowsQueryPlanNeedsLivePaginationFence(plan: db_mod.types.RelationalRowsQueryPlan) bool {
    if (routedRowsQueryRequestNeedsLivePaginationFence(plan.query)) return true;
    for (plan.ctes) |cte| {
        if (routedRowsQueryRequestNeedsLivePaginationFence(cte.query)) return true;
    }
    return false;
}

fn routedScanRowKeyInRange(row_key: []const u8, from_key: []const u8, to_key: []const u8) bool {
    if (from_key.len != 0 and std.mem.order(u8, row_key, from_key) == .lt) return false;
    if (to_key.len != 0 and std.mem.order(u8, row_key, to_key) != .lt) return false;
    return true;
}

fn verifyRoutedScanRangeUnchangedAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
    expected_rows: []const db_mod.types.RelationalRowsCollectedRow,
    consistency: raft_mod.ReadConsistency,
) !void {
    var current_rows = std.ArrayListUnmanaged(db_mod.types.RelationalRowsCollectedRow).empty;
    defer {
        for (current_rows.items) |row_value| {
            var row = row_value;
            row.deinit(alloc);
        }
        current_rows.deinit(alloc);
    }
    var materialization = row_spill.JsonRowsMaterializationTracker.initDefault("routed-scan");
    const saw_source = try appendMergeScanRowsFromRoutedScanAlloc(alloc, source, table_name, from_key, to_key, &current_rows, &materialization, consistency);
    if (!saw_source) return error.TopologyChanged;

    var expected_count: usize = 0;
    for (expected_rows) |row| {
        if (routedScanRowKeyInRange(row.key, from_key, to_key)) expected_count += 1;
    }
    if (current_rows.items.len != expected_count) return error.TopologyChanged;

    var expected_index: usize = 0;
    for (expected_rows) |expected| {
        if (!routedScanRowKeyInRange(expected.key, from_key, to_key)) continue;
        const current = current_rows.items[expected_index];
        if (!std.mem.eql(u8, current.key, expected.key)) return error.TopologyChanged;
        if (current.version != expected.version) return error.TopologyChanged;
        if (!std.mem.eql(u8, current.json, expected.json)) return error.TopologyChanged;
        expected_index += 1;
    }
}

fn verifyRoutedScanRangesUnchangedAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    expected_rows: []const db_mod.types.RelationalRowsCollectedRow,
    consistency: raft_mod.ReadConsistency,
) !void {
    if (ranges.len == 0) {
        try verifyRoutedScanRangeUnchangedAlloc(alloc, source, table_name, "", "", expected_rows, consistency);
        return;
    }
    for (ranges) |range| {
        try verifyRoutedScanRangeUnchangedAlloc(alloc, source, table_name, range.start, range.end, expected_rows, consistency);
    }
}

fn addRoutedMaterializationProfile(
    result: *db_mod.types.RelationalRowsQueryResult,
    scanned_rows: RoutedMergeScanRows,
) void {
    result.profile.routed_materialization_fallbacks += 1;
    result.profile.routed_materialized_rows += scanned_rows.materialized_rows;
    result.profile.routed_materialized_bytes += scanned_rows.materialized_bytes;
    if (scanned_rows.spilled) {
        result.profile.routed_spill_count += 1;
        result.profile.routed_spilled_rows += scanned_rows.spilled_rows;
        result.profile.routed_spilled_bytes += scanned_rows.spilled_bytes;
    }
}

fn verifyRoutedScanRangeUnchangedByFingerprintAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
    expected_rows: []const RoutedScanRowFingerprint,
    consistency: raft_mod.ReadConsistency,
) !void {
    var scan_result = (try source.scan(alloc, table_name, from_key, to_key, .{
        .include_documents = true,
        .include_all_fields = true,
    }, consistency)) orelse return error.TopologyChanged;
    defer scan_result.deinit(alloc);

    var expected_count: usize = 0;
    for (expected_rows) |row| {
        if (routedScanRowKeyInRange(row.key, from_key, to_key)) expected_count += 1;
    }

    var expected_index: usize = 0;
    var observed_count: usize = 0;
    var lines = std.mem.splitScalar(u8, scan_result.ndjson, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        if (observed_count >= expected_count) return error.TopologyChanged;
        var current = try mergeScanRowFromScanLineAlloc(alloc, line);
        defer current.deinit(alloc);

        while (expected_index < expected_rows.len and !routedScanRowKeyInRange(expected_rows[expected_index].key, from_key, to_key)) {
            expected_index += 1;
        }
        if (expected_index >= expected_rows.len) return error.TopologyChanged;
        const expected = expected_rows[expected_index];
        if (!std.mem.eql(u8, current.key, expected.key)) return error.TopologyChanged;
        if (current.version != expected.version) return error.TopologyChanged;
        if (routedScanJsonHash(current.json) != expected.json_hash) return error.TopologyChanged;
        expected_index += 1;
        observed_count += 1;
    }
    if (observed_count != expected_count) return error.TopologyChanged;
}

fn verifyRoutedScanRangesUnchangedByFingerprintAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    expected_rows: []const RoutedScanRowFingerprint,
    consistency: raft_mod.ReadConsistency,
) !void {
    if (ranges.len == 0) {
        try verifyRoutedScanRangeUnchangedByFingerprintAlloc(alloc, source, table_name, "", "", expected_rows, consistency);
        return;
    }
    for (ranges) |range| {
        try verifyRoutedScanRangeUnchangedByFingerprintAlloc(alloc, source, table_name, range.start, range.end, expected_rows, consistency);
    }
}

pub fn rowsQueryPlanFromRoutedScansAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    runtime_schema: storage_schema.TableSchema,
    plan: db_mod.types.RelationalRowsQueryPlan,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsQueryResult {
    try rejectRoutedRowsQueryPlanRowClaims(plan);
    try rejectRoutedRowsQueryPlanDocKeyRanges(plan);
    if (!scanPayloadCanStripSyntheticKey(runtime_schema)) return error.UnsupportedRowsQuery;
    if (routedRowsQueryPlanCanUseStreamingCountOnly(plan)) {
        return try countRowsQueryFromRoutedScansAlloc(alloc, source, table_name, runtime_schema, plan.query, plan.ranges, consistency);
    }
    if (routedRowsQueryPlanCanUseStreamingBoundedSorted(plan)) {
        return try boundedSortedRowsQueryFromRoutedScansAlloc(alloc, source, table_name, runtime_schema, plan.query, plan.ranges, consistency);
    }
    if (routedRowsQueryPlanCanUseStreamingCteBoundedSorted(runtime_schema, plan)) {
        return try boundedSortedRowsQueryFromRoutedScansThroughCteAlloc(alloc, source, table_name, runtime_schema, plan.ctes[0], plan.query, plan.ranges, consistency);
    }
    if (try countRowsQueryFromRoutedScansThroughCteChainAlloc(alloc, source, table_name, runtime_schema, plan, consistency)) |result| {
        return result;
    }
    if (routedRowsQueryPlanCanUseStreamingCteCountOnly(runtime_schema, plan)) {
        var cte_count_query = plan.ctes[0].query;
        cte_count_query.limit = 0;
        cte_count_query.total_mode = .exact;
        cte_count_query.profile = plan.query.profile;
        return try countRowsQueryFromRoutedScansAlloc(alloc, source, table_name, runtime_schema, cte_count_query, plan.ranges, consistency);
    }

    var scanned_rows = (try collectStableMergeScanRowsFromRoutedScansAlloc(alloc, source, table_name, runtime_schema, plan.ranges, consistency)) orelse return null;
    defer scanned_rows.deinit(alloc);
    if (routedRowsQueryPlanNeedsLivePaginationFence(plan)) {
        try verifyRoutedScanRangesUnchangedAlloc(alloc, source, table_name, plan.ranges, scanned_rows.rows, consistency);
    }
    const row_jsons = try rowJsonsFromCollectedRowsAlloc(alloc, scanned_rows.rows);
    defer alloc.free(row_jsons);

    var local_plan = plan;
    local_plan.ranges = &.{};
    var result = try relational_rows_api.executeRowsQueryPlanOnJsonRowsAlloc(alloc, runtime_schema, local_plan, row_jsons);
    addRoutedMaterializationProfile(&result, scanned_rows);
    return result;
}

pub fn rowsAggregatePlanFromRoutedScansAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    runtime_schema: storage_schema.TableSchema,
    plan: db_mod.types.RelationalRowsAggregatePlan,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsAggregateResult {
    try rejectRoutedRowsAggregatePlanRowClaims(plan);
    try rejectRoutedRowsAggregatePlanDocKeyRanges(plan);
    if (!scanPayloadCanStripSyntheticKey(runtime_schema)) return error.UnsupportedRowsQuery;

    var scanned_rows = (try collectStableRowsFromRoutedScansAlloc(alloc, source, table_name, runtime_schema, plan.ranges, consistency)) orelse return null;
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
    try rejectRoutedRowsSetOperationPlanRowClaims(plan);
    try rejectRoutedRowsSetOperationPlanDocKeyRanges(plan);
    if (!scanPayloadCanStripSyntheticKey(runtime_schema)) return error.UnsupportedRowsQuery;
    if (try rowsUnionAllCountOnlyPlanFromRoutedScansAlloc(alloc, source, table_name, runtime_schema, plan, consistency)) |result| {
        return result;
    }
    if (try rowsUnionDistinctCountOnlyPlanFromRoutedScansAlloc(alloc, source, table_name, runtime_schema, plan, consistency)) |result| {
        return result;
    }
    if (try rowsIntersectExceptCountOnlyPlanFromRoutedScansAlloc(alloc, source, table_name, runtime_schema, plan, consistency)) |result| {
        return result;
    }
    if (plan.ctes.len != 0) {
        if (plan.left.ctes.len != 0 or plan.right.ctes.len != 0) return error.InvalidRowsRequest;
        const cte_ranges = try routedRowsPlanRangesForSetOperationAlloc(alloc, plan.left.ranges, plan.right.ranges);
        defer if (cte_ranges.len > 0) alloc.free(cte_ranges);
        var scanned_rows = (try collectStableRowsFromRoutedScansAlloc(alloc, source, table_name, runtime_schema, cte_ranges, consistency)) orelse return null;
        defer scanned_rows.deinit(alloc);
        return try relational_rows_api.executeRowsSetOperationPlanOnJsonRowsAlloc(alloc, runtime_schema, plan, scanned_rows.rows);
    }
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
    try rejectRoutedRowsWindowPlanRowClaims(plan);
    try rejectRoutedRowsWindowPlanDocKeyRanges(plan);
    if (!scanPayloadCanStripSyntheticKey(runtime_schema)) return error.UnsupportedRowsQuery;

    var scanned_rows = (try collectStableRowsFromRoutedScansAlloc(alloc, source, table_name, runtime_schema, plan.ranges, consistency)) orelse return null;
    defer scanned_rows.deinit(alloc);

    var local_plan = plan;
    local_plan.ranges = &.{};
    return try relational_rows_api.executeRowsWindowPlanOnJsonRowsAlloc(alloc, runtime_schema, local_plan, scanned_rows.rows);
}

fn spillAndReloadCollectedRowsAlloc(
    alloc: std.mem.Allocator,
    rows: []db_mod.types.RelationalRowsCollectedRow,
    name: []const u8,
) ![]db_mod.types.RelationalRowsCollectedRow {
    const keyed = try alloc.alloc(row_spill.KeyedJsonRow, rows.len);
    defer alloc.free(keyed);
    for (rows, 0..) |row, i| keyed[i] = .{
        .key = row.key,
        .json = row.json,
        .version = row.version,
    };

    var spill = try row_spill.spillKeyedJsonRowsAlloc(alloc, keyed, name);
    defer spill.deinit(alloc);

    const loaded = try row_spill.loadKeyedJsonRowsAlloc(alloc, spill);
    errdefer row_spill.freeKeyedJsonRows(alloc, loaded);
    const out = try alloc.alloc(db_mod.types.RelationalRowsCollectedRow, loaded.len);
    errdefer alloc.free(out);
    for (loaded, 0..) |*row, i| {
        out[i] = .{
            .key = row.key,
            .json = row.json,
            .version = row.version,
        };
        row.key = "";
        row.json = "";
    }
    alloc.free(loaded);
    db_mod.types.freeRelationalRowsCollectedRows(alloc, rows);
    return out;
}

pub fn routedRowsPlanRangesForJoinAlloc(
    alloc: std.mem.Allocator,
    left_ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    right_ranges: []const db_mod.types.RelationalRowsDocKeyRange,
) ![]const db_mod.types.RelationalRowsDocKeyRange {
    if ((left_ranges.len == 0) != (right_ranges.len == 0)) return error.InvalidQueryRequest;
    if (left_ranges.len == 0) return &.{};
    return try routedRowsPlanRangesUnionAlloc(alloc, left_ranges, right_ranges);
}

fn routedRowsPlanRangesForSetOperationAlloc(
    alloc: std.mem.Allocator,
    left_ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    right_ranges: []const db_mod.types.RelationalRowsDocKeyRange,
) ![]const db_mod.types.RelationalRowsDocKeyRange {
    if (left_ranges.len == 0 or right_ranges.len == 0) return &.{};
    return try routedRowsPlanRangesUnionAlloc(alloc, left_ranges, right_ranges);
}

fn routedRowsPlanRangesUnionAlloc(
    alloc: std.mem.Allocator,
    left_ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    right_ranges: []const db_mod.types.RelationalRowsDocKeyRange,
) ![]const db_mod.types.RelationalRowsDocKeyRange {
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
    if (parsed.value.object.fetchOrderedRemove("_id") == null and
        parsed.value.object.fetchOrderedRemove("key") == null)
    {
        return error.InvalidRemoteResponse;
    }
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

fn routedRowsJoinPlanCanUseCountOnly(plan: db_mod.types.RelationalRowsJoinPlan) bool {
    const join = plan.join;
    if (join.join_type != .inner and join.join_type != .left and join.join_type != .full) return false;
    if (join.on.len == 0) return false;
    if (join.order_by.len != 0 or join.limit != null or join.offset != 0) return false;
    if (join.select.len != 0) return false;
    if (!relational_rows_api.rowsQueryCanUseCountOnlyResultForRouting(join.left)) return false;
    if (!relational_rows_api.rowsQueryCanUseCountOnlyResultForRouting(join.right)) return false;
    if (plan.ctes.len != 0 and (plan.join.left.source_cte.len == 0 and plan.join.right.source_cte.len == 0)) return false;
    return true;
}

fn routedRowsJoinHasOnExpressionPredicates(join: db_mod.types.RelationalRowsJoinRequest) bool {
    return join.on_expression_predicates.len != 0 or
        join.on_expression_or_predicates.len != 0 or
        join.on_expression_not_predicates.len != 0 or
        join.on_expression_array_contains.len != 0;
}

fn routedRowsJoinHasMatchPredicates(join: db_mod.types.RelationalRowsJoinRequest) bool {
    return join.match_expression_predicates.len != 0 or
        join.match_expression_or_predicates.len != 0 or
        join.match_expression_not_predicates.len != 0 or
        join.match_expression_array_contains.len != 0;
}

fn routedRowsJoinKeyJsonAlloc(
    alloc: std.mem.Allocator,
    row: std.json.Value,
    predicates: []const db_mod.types.RelationalRowsJoinOn,
    side: db_mod.types.RelationalRowsJoinProjectionSide,
) !?[]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeByte('[');
    for (predicates, 0..) |predicate, i| {
        const field = switch (side) {
            .left => predicate.left_field,
            .right => predicate.right_field,
        };
        const value = db_relational_rows.jsonValueAtPath(row, field) orelse {
            out.deinit();
            return null;
        };
        if (value.* == .null) {
            out.deinit();
            return null;
        }
        if (i != 0) try writer.writeByte(',');
        try std.json.Stringify.value(value.*, .{}, writer);
    }
    try writer.writeByte(']');
    return try out.toOwnedSlice();
}

fn routedRowsFreeJoinKeyCounts(alloc: std.mem.Allocator, counts: *std.StringHashMapUnmanaged(u32)) void {
    var keys = counts.keyIterator();
    while (keys.next()) |key| alloc.free(@constCast(key.*));
    counts.deinit(alloc);
}

const RoutedJoinRightRows = struct {
    rows: std.ArrayListUnmanaged([]u8) = .empty,
    matched: std.ArrayListUnmanaged(bool) = .empty,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.rows.items) |row| alloc.free(row);
        self.rows.deinit(alloc);
        self.matched.deinit(alloc);
        self.* = undefined;
    }
};

fn routedRowsFreeJoinRightRows(alloc: std.mem.Allocator, rows_by_key: *std.StringArrayHashMapUnmanaged(RoutedJoinRightRows)) void {
    for (rows_by_key.keys()) |key| alloc.free(key);
    for (rows_by_key.values()) |*rows| rows.deinit(alloc);
    rows_by_key.deinit(alloc);
}

fn routedRowsIncrementJoinKeyCountAlloc(
    alloc: std.mem.Allocator,
    counts: *std.StringHashMapUnmanaged(u32),
    key_value: []const u8,
) !void {
    const key = try alloc.dupe(u8, key_value);
    errdefer alloc.free(key);
    const gop = try counts.getOrPut(alloc, key);
    if (gop.found_existing) {
        alloc.free(key);
        gop.value_ptr.* = try addRoutedRowsCount(gop.value_ptr.*, 1);
    } else {
        gop.key_ptr.* = key;
        gop.value_ptr.* = 1;
    }
}

fn routedRowsAppendJoinRightRowAlloc(
    alloc: std.mem.Allocator,
    rows_by_key: *std.StringArrayHashMapUnmanaged(RoutedJoinRightRows),
    key_value: []const u8,
    row_json: []const u8,
) !void {
    const key = try alloc.dupe(u8, key_value);
    errdefer alloc.free(key);
    const gop = try rows_by_key.getOrPut(alloc, key);
    if (!gop.found_existing) {
        gop.key_ptr.* = key;
        gop.value_ptr.* = .{};
    } else {
        alloc.free(key);
    }
    const row_copy = try alloc.dupe(u8, row_json);
    errdefer alloc.free(row_copy);
    try gop.value_ptr.matched.append(alloc, false);
    errdefer gop.value_ptr.matched.items.len -= 1;
    try gop.value_ptr.rows.append(alloc, row_copy);
}

fn routedRowsJoinSideMergedQueryAlloc(
    alloc: std.mem.Allocator,
    schema: storage_schema.TableSchema,
    ctes: []const db_mod.types.RelationalRowsCte,
    side_query: db_mod.types.RelationalRowsQueryRequest,
    filters: *RoutedRowsBorrowedFilterLists,
) !?db_mod.types.RelationalRowsQueryRequest {
    const plan = db_mod.types.RelationalRowsQueryPlan{
        .ctes = ctes,
        .query = side_query,
    };
    var query = if (side_query.source_cte.len == 0) side_query else (try routedRowsUnionDistinctMergedCteBranchQueryAlloc(alloc, schema, plan, filters)) orelse return null;
    query.select = &.{};
    query.json_extract = &.{};
    query.array_length = &.{};
    query.coalesce = &.{};
    query.field_aliases = &.{};
    query.expressions = &.{};
    query.scalar_subqueries = &.{};
    query.select_all = true;
    query.limit = 0;
    query.total_mode = .exact;
    query.profile = false;
    return query;
}

fn routedRowsLateralSideMergedQueryAlloc(
    alloc: std.mem.Allocator,
    schema: storage_schema.TableSchema,
    ctes: []const db_mod.types.RelationalRowsCte,
    side_query: db_mod.types.RelationalRowsQueryRequest,
    filters: *RoutedRowsBorrowedFilterLists,
) !?db_mod.types.RelationalRowsQueryRequest {
    if (side_query.source_cte.len == 0) return side_query;
    var query = (try routedRowsUnionDistinctMergedCteBranchQueryAlloc(alloc, schema, .{
        .ctes = ctes,
        .query = side_query,
    }, filters)) orelse return null;
    query.limit = side_query.limit;
    query.total_mode = side_query.total_mode;
    query.profile = false;
    return query;
}

fn accumulateRoutedJoinSideCountsFromScanAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    schema: storage_schema.TableSchema,
    req: db_mod.types.RelationalRowsQueryRequest,
    join_on: []const db_mod.types.RelationalRowsJoinOn,
    side: db_mod.types.RelationalRowsJoinProjectionSide,
    from_key: []const u8,
    to_key: []const u8,
    consistency: raft_mod.ReadConsistency,
    counts: *std.StringHashMapUnmanaged(u32),
    scanned_rows: *u64,
    filtered_rows: ?*u64,
    matched_rows: *u64,
) !bool {
    var scan_result = (try source.scan(alloc, table_name, from_key, to_key, .{
        .include_documents = true,
        .include_all_fields = true,
    }, consistency)) orelse return false;
    defer scan_result.deinit(alloc);

    var lines = std.mem.splitScalar(u8, scan_result.ndjson, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        var row = try mergeScanRowFromScanLineAlloc(alloc, line);
        defer row.deinit(alloc);
        scanned_rows.* += 1;

        var lookup = (try source.lookup(alloc, table_name, row.key, .{ .include_all_fields = true }, consistency)) orelse return error.TopologyChanged;
        defer lookup.deinit(alloc);
        if (row.version != lookup.version) return error.TopologyChanged;
        if (!std.mem.eql(u8, row.json, lookup.json)) return error.TopologyChanged;
        if (!try relational_rows_api.rowsQueryJsonMatchesCountOnlyAlloc(alloc, schema, req, lookup.json)) continue;
        if (filtered_rows) |count| count.* += 1;

        var parsed = std.json.parseFromSlice(std.json.Value, alloc, lookup.json, .{}) catch return error.InvalidRowsRequest;
        defer parsed.deinit();
        if (parsed.value != .object) return error.InvalidRowsRequest;
        const key_json = (try routedRowsJoinKeyJsonAlloc(alloc, parsed.value, join_on, side)) orelse continue;
        defer alloc.free(key_json);
        try routedRowsIncrementJoinKeyCountAlloc(alloc, counts, key_json);
        matched_rows.* += 1;
    }
    return true;
}

fn accumulateRoutedJoinSideCountsAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    schema: storage_schema.TableSchema,
    req: db_mod.types.RelationalRowsQueryRequest,
    ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    join_on: []const db_mod.types.RelationalRowsJoinOn,
    side: db_mod.types.RelationalRowsJoinProjectionSide,
    consistency: raft_mod.ReadConsistency,
    counts: *std.StringHashMapUnmanaged(u32),
    scanned_rows: *u64,
    matched_rows: *u64,
    iterator_seeks: *u64,
) !bool {
    var saw_source = false;
    if (ranges.len == 0) {
        saw_source = try accumulateRoutedJoinSideCountsFromScanAlloc(alloc, source, table_name, schema, req, join_on, side, "", "", consistency, counts, scanned_rows, null, matched_rows);
        iterator_seeks.* += 1;
    } else {
        for (ranges) |range| {
            saw_source = (try accumulateRoutedJoinSideCountsFromScanAlloc(alloc, source, table_name, schema, req, join_on, side, range.start, range.end, consistency, counts, scanned_rows, null, matched_rows)) or saw_source;
        }
        iterator_seeks.* += @intCast(ranges.len);
    }
    return saw_source;
}

fn accumulateRoutedJoinFilteredSideCountsAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    schema: storage_schema.TableSchema,
    req: db_mod.types.RelationalRowsQueryRequest,
    ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    join_on: []const db_mod.types.RelationalRowsJoinOn,
    side: db_mod.types.RelationalRowsJoinProjectionSide,
    consistency: raft_mod.ReadConsistency,
    counts: *std.StringHashMapUnmanaged(u32),
    scanned_rows: *u64,
    filtered_rows: *u64,
    matched_rows: *u64,
    iterator_seeks: *u64,
) !bool {
    var saw_source = false;
    if (ranges.len == 0) {
        saw_source = try accumulateRoutedJoinSideCountsFromScanAlloc(alloc, source, table_name, schema, req, join_on, side, "", "", consistency, counts, scanned_rows, filtered_rows, matched_rows);
        iterator_seeks.* += 1;
    } else {
        for (ranges) |range| {
            saw_source = (try accumulateRoutedJoinSideCountsFromScanAlloc(alloc, source, table_name, schema, req, join_on, side, range.start, range.end, consistency, counts, scanned_rows, filtered_rows, matched_rows)) or saw_source;
        }
        iterator_seeks.* += @intCast(ranges.len);
    }
    return saw_source;
}

fn accumulateRoutedJoinRightRowsFromScanAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    schema: storage_schema.TableSchema,
    req: db_mod.types.RelationalRowsQueryRequest,
    join_on: []const db_mod.types.RelationalRowsJoinOn,
    from_key: []const u8,
    to_key: []const u8,
    consistency: raft_mod.ReadConsistency,
    rows_by_key: *std.StringArrayHashMapUnmanaged(RoutedJoinRightRows),
    scanned_rows: *u64,
    filtered_rows: ?*u64,
    matched_rows: *u64,
) !bool {
    var scan_result = (try source.scan(alloc, table_name, from_key, to_key, .{
        .include_documents = true,
        .include_all_fields = true,
    }, consistency)) orelse return false;
    defer scan_result.deinit(alloc);

    var lines = std.mem.splitScalar(u8, scan_result.ndjson, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        var row = try mergeScanRowFromScanLineAlloc(alloc, line);
        defer row.deinit(alloc);
        scanned_rows.* += 1;

        var lookup = (try source.lookup(alloc, table_name, row.key, .{ .include_all_fields = true }, consistency)) orelse return error.TopologyChanged;
        defer lookup.deinit(alloc);
        if (row.version != lookup.version) return error.TopologyChanged;
        if (!std.mem.eql(u8, row.json, lookup.json)) return error.TopologyChanged;
        if (!try relational_rows_api.rowsQueryJsonMatchesCountOnlyAlloc(alloc, schema, req, lookup.json)) continue;
        if (filtered_rows) |count| count.* += 1;

        var parsed = std.json.parseFromSlice(std.json.Value, alloc, lookup.json, .{}) catch return error.InvalidRowsRequest;
        defer parsed.deinit();
        if (parsed.value != .object) return error.InvalidRowsRequest;
        const key_json = (try routedRowsJoinKeyJsonAlloc(alloc, parsed.value, join_on, .right)) orelse continue;
        defer alloc.free(key_json);
        try routedRowsAppendJoinRightRowAlloc(alloc, rows_by_key, key_json, lookup.json);
        matched_rows.* += 1;
    }
    return true;
}

fn accumulateRoutedJoinRightRowsAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    schema: storage_schema.TableSchema,
    req: db_mod.types.RelationalRowsQueryRequest,
    ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    join_on: []const db_mod.types.RelationalRowsJoinOn,
    consistency: raft_mod.ReadConsistency,
    rows_by_key: *std.StringArrayHashMapUnmanaged(RoutedJoinRightRows),
    scanned_rows: *u64,
    filtered_rows: ?*u64,
    matched_rows: *u64,
    iterator_seeks: *u64,
) !bool {
    var saw_source = false;
    if (ranges.len == 0) {
        saw_source = try accumulateRoutedJoinRightRowsFromScanAlloc(alloc, source, table_name, schema, req, join_on, "", "", consistency, rows_by_key, scanned_rows, filtered_rows, matched_rows);
        iterator_seeks.* += 1;
    } else {
        for (ranges) |range| {
            saw_source = (try accumulateRoutedJoinRightRowsFromScanAlloc(alloc, source, table_name, schema, req, join_on, range.start, range.end, consistency, rows_by_key, scanned_rows, filtered_rows, matched_rows)) or saw_source;
        }
        iterator_seeks.* += @intCast(ranges.len);
    }
    return saw_source;
}

fn countRoutedJoinResidualMatchesFromLeftScanAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    schema: storage_schema.TableSchema,
    req: db_mod.types.RelationalRowsQueryRequest,
    join: db_mod.types.RelationalRowsJoinRequest,
    left_columns: []const storage_schema.RelationalColumn,
    right_columns: []const storage_schema.RelationalColumn,
    from_key: []const u8,
    to_key: []const u8,
    consistency: raft_mod.ReadConsistency,
    right_rows_by_key: *std.StringArrayHashMapUnmanaged(RoutedJoinRightRows),
    total: *u32,
    scanned_rows: *u64,
    matched_rows: *u64,
    null_extend_unmatched_left: bool,
    mark_right_on_match: bool,
) !bool {
    var scan_result = (try source.scan(alloc, table_name, from_key, to_key, .{
        .include_documents = true,
        .include_all_fields = true,
    }, consistency)) orelse return false;
    defer scan_result.deinit(alloc);

    var lines = std.mem.splitScalar(u8, scan_result.ndjson, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        var row = try mergeScanRowFromScanLineAlloc(alloc, line);
        defer row.deinit(alloc);
        scanned_rows.* += 1;

        var lookup = (try source.lookup(alloc, table_name, row.key, .{ .include_all_fields = true }, consistency)) orelse return error.TopologyChanged;
        defer lookup.deinit(alloc);
        if (row.version != lookup.version) return error.TopologyChanged;
        if (!std.mem.eql(u8, row.json, lookup.json)) return error.TopologyChanged;
        if (!try relational_rows_api.rowsQueryJsonMatchesCountOnlyAlloc(alloc, schema, req, lookup.json)) continue;
        matched_rows.* += 1;

        var parsed_left = std.json.parseFromSlice(std.json.Value, alloc, lookup.json, .{}) catch return error.InvalidRowsRequest;
        defer parsed_left.deinit();
        if (parsed_left.value != .object) return error.InvalidRowsRequest;
        const key_json = (try routedRowsJoinKeyJsonAlloc(alloc, parsed_left.value, join.on, .left)) orelse {
            if (null_extend_unmatched_left) total.* = try addRoutedRowsCount(total.*, 1);
            continue;
        };
        defer alloc.free(key_json);
        const right_rows = right_rows_by_key.getPtr(key_json) orelse {
            if (null_extend_unmatched_left) total.* = try addRoutedRowsCount(total.*, 1);
            continue;
        };
        var matched_any = false;
        for (right_rows.rows.items, 0..) |right_row, right_index| {
            const now_ns = platform_time.realtimeNs();
            if (!try db_relational_rows.joinOnExpressionPredicatesPass(alloc, parsed_left.value, left_columns, right_row, right_columns, join, now_ns)) continue;
            if (mark_right_on_match) right_rows.matched.items[right_index] = true;
            if (!try db_relational_rows.joinMatchPredicatesPass(alloc, parsed_left.value, left_columns, right_row, right_columns, join, now_ns)) continue;
            matched_any = true;
            total.* = try addRoutedRowsCount(total.*, 1);
        }
        if (!matched_any and null_extend_unmatched_left) total.* = try addRoutedRowsCount(total.*, 1);
    }
    return true;
}

fn countRoutedJoinResidualMatchesFromLeftAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    schema: storage_schema.TableSchema,
    req: db_mod.types.RelationalRowsQueryRequest,
    ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    join: db_mod.types.RelationalRowsJoinRequest,
    left_columns: []const storage_schema.RelationalColumn,
    right_columns: []const storage_schema.RelationalColumn,
    consistency: raft_mod.ReadConsistency,
    right_rows_by_key: *std.StringArrayHashMapUnmanaged(RoutedJoinRightRows),
    total: *u32,
    scanned_rows: *u64,
    matched_rows: *u64,
    iterator_seeks: *u64,
    null_extend_unmatched_left: bool,
    mark_right_on_match: bool,
) !bool {
    var saw_source = false;
    if (ranges.len == 0) {
        saw_source = try countRoutedJoinResidualMatchesFromLeftScanAlloc(alloc, source, table_name, schema, req, join, left_columns, right_columns, "", "", consistency, right_rows_by_key, total, scanned_rows, matched_rows, null_extend_unmatched_left, mark_right_on_match);
        iterator_seeks.* += 1;
    } else {
        for (ranges) |range| {
            saw_source = (try countRoutedJoinResidualMatchesFromLeftScanAlloc(alloc, source, table_name, schema, req, join, left_columns, right_columns, range.start, range.end, consistency, right_rows_by_key, total, scanned_rows, matched_rows, null_extend_unmatched_left, mark_right_on_match)) or saw_source;
        }
        iterator_seeks.* += @intCast(ranges.len);
    }
    return saw_source;
}

fn countUnmatchedRoutedJoinRightRows(rows_by_key: std.StringArrayHashMapUnmanaged(RoutedJoinRightRows), right_filtered_rows: u64, right_keyed_rows: u64) !u32 {
    if (right_filtered_rows < right_keyed_rows) return error.UnsupportedRowsQuery;
    var total = std.math.cast(u32, right_filtered_rows - right_keyed_rows) orelse return error.UnsupportedRowsQuery;
    for (rows_by_key.values()) |right_rows| {
        for (right_rows.matched.items) |matched| {
            if (!matched) total = try addRoutedRowsCount(total, 1);
        }
    }
    return total;
}

fn routedJoinInnerCountFromKeyCounts(left_counts: std.StringHashMapUnmanaged(u32), right_counts: std.StringHashMapUnmanaged(u32)) !u32 {
    var total: u32 = 0;
    var left_it = left_counts.iterator();
    while (left_it.next()) |entry| {
        const right_count = right_counts.get(entry.key_ptr.*) orelse continue;
        const matches = std.math.mul(u32, entry.value_ptr.*, right_count) catch return error.UnsupportedRowsQuery;
        total = try addRoutedRowsCount(total, matches);
    }
    return total;
}

fn routedJoinLeftCountFromKeyCounts(left_counts: std.StringHashMapUnmanaged(u32), right_counts: std.StringHashMapUnmanaged(u32), left_filtered_rows: u64) !u32 {
    var total: u32 = 0;
    var keyed_left_rows: u64 = 0;
    var left_it = left_counts.iterator();
    while (left_it.next()) |entry| {
        keyed_left_rows += entry.value_ptr.*;
        const right_count = right_counts.get(entry.key_ptr.*) orelse {
            total = try addRoutedRowsCount(total, entry.value_ptr.*);
            continue;
        };
        const matches = std.math.mul(u32, entry.value_ptr.*, right_count) catch return error.UnsupportedRowsQuery;
        total = try addRoutedRowsCount(total, matches);
    }
    if (left_filtered_rows < keyed_left_rows) return error.UnsupportedRowsQuery;
    const null_extended_rows = std.math.cast(u32, left_filtered_rows - keyed_left_rows) orelse return error.UnsupportedRowsQuery;
    return try addRoutedRowsCount(total, null_extended_rows);
}

fn routedJoinFullCountFromKeyCounts(left_counts: std.StringHashMapUnmanaged(u32), right_counts: std.StringHashMapUnmanaged(u32), left_filtered_rows: u64, right_filtered_rows: u64) !u32 {
    var total: u32 = 0;
    var keyed_left_rows: u64 = 0;
    var left_it = left_counts.iterator();
    while (left_it.next()) |entry| {
        keyed_left_rows += entry.value_ptr.*;
        const right_count = right_counts.get(entry.key_ptr.*) orelse {
            total = try addRoutedRowsCount(total, entry.value_ptr.*);
            continue;
        };
        const matches = std.math.mul(u32, entry.value_ptr.*, right_count) catch return error.UnsupportedRowsQuery;
        total = try addRoutedRowsCount(total, matches);
    }

    var keyed_right_rows: u64 = 0;
    var right_it = right_counts.iterator();
    while (right_it.next()) |entry| {
        keyed_right_rows += entry.value_ptr.*;
        if (left_counts.contains(entry.key_ptr.*)) continue;
        total = try addRoutedRowsCount(total, entry.value_ptr.*);
    }

    if (left_filtered_rows < keyed_left_rows or right_filtered_rows < keyed_right_rows) return error.UnsupportedRowsQuery;
    const null_extended_left_rows = std.math.cast(u32, left_filtered_rows - keyed_left_rows) orelse return error.UnsupportedRowsQuery;
    const null_extended_right_rows = std.math.cast(u32, right_filtered_rows - keyed_right_rows) orelse return error.UnsupportedRowsQuery;
    total = try addRoutedRowsCount(total, null_extended_left_rows);
    return try addRoutedRowsCount(total, null_extended_right_rows);
}

fn rowsJoinCountOnlyPlanFromRoutedScansWithSchemasAlloc(
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
    if (!routedRowsJoinPlanCanUseCountOnly(plan)) return null;

    const cte_ranges = if (plan.ctes.len == 0) &.{} else try routedRowsPlanRangesForJoinCtesAlloc(
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

    var left_filters = RoutedRowsBorrowedFilterLists{};
    defer left_filters.deinit(alloc);
    const left_uses_cte = plan.join.left.source_cte.len != 0;
    const left_scan_table = if (left_uses_cte) cte_table_name else left_table_name;
    const left_scan_schema = if (left_uses_cte) cte_base_schema else left_schema;
    const left_scan_ranges = if (left_uses_cte) cte_ranges else plan.left_ranges;
    const left_query = (try routedRowsJoinSideMergedQueryAlloc(alloc, left_scan_schema, plan.ctes, plan.join.left, &left_filters)) orelse return null;
    var right_filters = RoutedRowsBorrowedFilterLists{};
    defer right_filters.deinit(alloc);
    const right_uses_cte = plan.join.right.source_cte.len != 0;
    const right_scan_table = if (right_uses_cte) cte_table_name else right_table_name;
    const right_scan_schema = if (right_uses_cte) cte_base_schema else right_schema;
    const right_scan_ranges = if (right_uses_cte) cte_ranges else plan.right_ranges;
    const right_query = (try routedRowsJoinSideMergedQueryAlloc(alloc, right_scan_schema, plan.ctes, plan.join.right, &right_filters)) orelse return null;
    const has_residual_predicates = routedRowsJoinHasOnExpressionPredicates(plan.join) or routedRowsJoinHasMatchPredicates(plan.join);

    if (has_residual_predicates) {
        var right_rows_by_key = std.StringArrayHashMapUnmanaged(RoutedJoinRightRows).empty;
        defer routedRowsFreeJoinRightRows(alloc, &right_rows_by_key);

        var left_scanned_rows: u64 = 0;
        var left_matched_rows: u64 = 0;
        var right_scanned_rows: u64 = 0;
        var right_filtered_rows: u64 = 0;
        var right_matched_rows: u64 = 0;
        var iterator_seeks: u64 = 0;
        const saw_right = try accumulateRoutedJoinRightRowsAlloc(alloc, source, right_scan_table, right_scan_schema, right_query, right_scan_ranges, plan.join.on, consistency, &right_rows_by_key, &right_scanned_rows, if (plan.join.join_type == .full and !routedRowsJoinHasMatchPredicates(plan.join)) &right_filtered_rows else null, &right_matched_rows, &iterator_seeks);
        var total: u32 = 0;
        const null_extend_unmatched = (plan.join.join_type == .left or plan.join.join_type == .full) and !routedRowsJoinHasMatchPredicates(plan.join);
        const saw_left = try countRoutedJoinResidualMatchesFromLeftAlloc(
            alloc,
            source,
            left_scan_table,
            left_scan_schema,
            left_query,
            left_scan_ranges,
            plan.join,
            left_scan_schema.relational_columns,
            right_scan_schema.relational_columns,
            consistency,
            &right_rows_by_key,
            &total,
            &left_scanned_rows,
            &left_matched_rows,
            &iterator_seeks,
            null_extend_unmatched,
            plan.join.join_type == .full and !routedRowsJoinHasMatchPredicates(plan.join),
        );
        if (!saw_left or !saw_right) return null;
        if (plan.join.join_type == .full and !routedRowsJoinHasMatchPredicates(plan.join)) {
            total = try addRoutedRowsCount(total, try countUnmatchedRoutedJoinRightRows(right_rows_by_key, right_filtered_rows, right_matched_rows));
        }
        const left_count_for_strategy = std.math.cast(usize, left_matched_rows) orelse return error.UnsupportedRowsQuery;
        const right_count_for_strategy = std.math.cast(usize, right_matched_rows) orelse return error.UnsupportedRowsQuery;
        return .{
            .rows = &.{},
            .total_rows = total,
            .strategy_selection = db_mod.types.relationalRowsSelectJoinStrategy(plan.join, left_count_for_strategy, right_count_for_strategy, false),
        };
    }

    var left_counts = std.StringHashMapUnmanaged(u32).empty;
    defer routedRowsFreeJoinKeyCounts(alloc, &left_counts);
    var right_counts = std.StringHashMapUnmanaged(u32).empty;
    defer routedRowsFreeJoinKeyCounts(alloc, &right_counts);

    var left_scanned_rows: u64 = 0;
    var left_filtered_rows: u64 = 0;
    var left_matched_rows: u64 = 0;
    var right_scanned_rows: u64 = 0;
    var right_filtered_rows: u64 = 0;
    var right_matched_rows: u64 = 0;
    var iterator_seeks: u64 = 0;
    const saw_left = if (plan.join.join_type == .left or plan.join.join_type == .full)
        try accumulateRoutedJoinFilteredSideCountsAlloc(alloc, source, left_scan_table, left_scan_schema, left_query, left_scan_ranges, plan.join.on, .left, consistency, &left_counts, &left_scanned_rows, &left_filtered_rows, &left_matched_rows, &iterator_seeks)
    else
        try accumulateRoutedJoinSideCountsAlloc(alloc, source, left_scan_table, left_scan_schema, left_query, left_scan_ranges, plan.join.on, .left, consistency, &left_counts, &left_scanned_rows, &left_matched_rows, &iterator_seeks);
    const saw_right = if (plan.join.join_type == .full)
        try accumulateRoutedJoinFilteredSideCountsAlloc(alloc, source, right_scan_table, right_scan_schema, right_query, right_scan_ranges, plan.join.on, .right, consistency, &right_counts, &right_scanned_rows, &right_filtered_rows, &right_matched_rows, &iterator_seeks)
    else
        try accumulateRoutedJoinSideCountsAlloc(alloc, source, right_scan_table, right_scan_schema, right_query, right_scan_ranges, plan.join.on, .right, consistency, &right_counts, &right_scanned_rows, &right_matched_rows, &iterator_seeks);
    if (!saw_left or !saw_right) return null;

    const total = switch (plan.join.join_type) {
        .inner => try routedJoinInnerCountFromKeyCounts(left_counts, right_counts),
        .left => try routedJoinLeftCountFromKeyCounts(left_counts, right_counts, left_filtered_rows),
        .full => try routedJoinFullCountFromKeyCounts(left_counts, right_counts, left_filtered_rows, right_filtered_rows),
    };
    const left_count_for_strategy = std.math.cast(usize, left_matched_rows) orelse return error.UnsupportedRowsQuery;
    const right_count_for_strategy = std.math.cast(usize, right_matched_rows) orelse return error.UnsupportedRowsQuery;
    return .{
        .rows = &.{},
        .total_rows = total,
        .strategy_selection = db_mod.types.relationalRowsSelectJoinStrategy(plan.join, left_count_for_strategy, right_count_for_strategy, false),
    };
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
    try rejectRoutedRowsJoinPlanRowClaims(plan);
    try rejectRoutedRowsJoinPlanDocKeyRanges(plan);
    if (!scanPayloadCanStripSyntheticKey(cte_base_schema) or
        !scanPayloadCanStripSyntheticKey(left_schema) or
        !scanPayloadCanStripSyntheticKey(right_schema))
    {
        return error.UnsupportedRowsQuery;
    }
    try preflightRoutedRowsJoinStrategy(plan);
    if (try rowsJoinCountOnlyPlanFromRoutedScansWithSchemasAlloc(alloc, source, cte_table_name, left_table_name, right_table_name, cte_base_schema, left_schema, right_schema, plan, consistency)) |result| {
        return result;
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
        cte_rows_storage = (try collectStableRowsFromRoutedScansAlloc(alloc, source, cte_table_name, cte_base_schema, cte_ranges, consistency)) orelse return null;
        break :blk cte_rows_storage.?.rows;
    };
    var left_rows = (try collectStableRowsFromRoutedScansAlloc(alloc, source, left_table_name, left_schema, plan.left_ranges, consistency)) orelse return null;
    defer left_rows.deinit(alloc);
    var right_rows = (try collectStableRowsFromRoutedScansAlloc(alloc, source, right_table_name, right_schema, plan.right_ranges, consistency)) orelse return null;
    defer right_rows.deinit(alloc);

    var local_plan = plan;
    local_plan.left_ranges = &.{};
    local_plan.right_ranges = &.{};
    return try relational_rows_api.executeRowsJoinPlanOnJsonRowsWithSchemasAlloc(alloc, cte_base_schema, left_schema, right_schema, local_plan, cte_rows, left_rows.rows, right_rows.rows);
}

fn preflightRoutedRowsJoinStrategy(plan: db_mod.types.RelationalRowsJoinPlan) !void {
    const join = plan.join;
    if (join.strategy == .merge and (plan.left_ranges.len != 0 or plan.right_ranges.len != 0)) {
        return error.UnsupportedRowsQuery;
    }
    if (join.strategy == .merge and (routedRowsJoinInputHasFilterOrPagination(join.left) or routedRowsJoinInputHasFilterOrPagination(join.right))) {
        return error.UnsupportedRowsQuery;
    }
    if (join.strategy == .merge and !db_mod.types.relationalRowsJoinInputsSortedOnJoinKeys(join)) {
        return error.UnsupportedRowsQuery;
    }
}

fn routedRowsJoinInputHasFilterOrPagination(req: db_mod.types.RelationalRowsQueryRequest) bool {
    return req.predicates.len != 0 or
        req.array_any.len != 0 or
        req.array_contains.len != 0 or
        req.array_eq.len != 0 or
        req.in_predicates.len != 0 or
        req.json_contains.len != 0 or
        req.json_path_eq.len != 0 or
        req.json_path_exists.len != 0 or
        req.text_patterns.len != 0 or
        req.or_predicates.len != 0 or
        req.not_predicates.len != 0 or
        req.access_or_predicates.len != 0 or
        req.access_not_predicates.len != 0 or
        req.expression_predicates.len != 0 or
        req.expression_or_predicates.len != 0 or
        req.expression_not_predicates.len != 0 or
        req.expression_array_contains.len != 0 or
        req.limit != null or
        req.offset != 0;
}

fn routedRowsLateralPlanCanUseCountOnly(plan: db_mod.types.RelationalRowsLateralPlan) bool {
    const lateral = plan.lateral;
    if (lateral.limit == null or lateral.limit.? != 0) return false;
    if (lateral.offset != 0 or lateral.order_by.len != 0 or lateral.select.len != 0) return false;
    if (lateral.left.order_by.len != 0 and lateral.left.limit == null and lateral.left.offset != 0) return false;
    if (plan.ctes.len != 0 and lateral.left.source_cte.len == 0 and lateral.right.source_cte.len == 0) return false;
    return true;
}

fn routedRowsLateralRightCountForLeftAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    right_table_name: []const u8,
    right_schema: storage_schema.TableSchema,
    right_ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    ctes: []const db_mod.types.RelationalRowsCte,
    lateral: db_mod.types.RelationalRowsLateralRequest,
    left_row: std.json.Value,
    left_columns: []const storage_schema.RelationalColumn,
    right_columns: []const storage_schema.RelationalColumn,
    consistency: raft_mod.ReadConsistency,
) !?u32 {
    const correlated_predicates = try lateralCorrelationPredicatesAlloc(alloc, left_row, lateral.correlations);
    defer freeOwnedRelationalChecks(alloc, correlated_predicates);
    if (correlated_predicates.len != lateral.correlations.len) {
        return if (db_relational_rows.lateralHasMatchPredicates(lateral)) 0 else 1;
    }

    var right_source = db_relational_rows.joinSideSource(lateral.right);
    const combined_predicates = try combinedBorrowedAndOwnedRelationalChecksAlloc(alloc, lateral.right.predicates, correlated_predicates);
    defer if (combined_predicates.len > 0) alloc.free(combined_predicates);
    right_source.predicates = combined_predicates;
    var right_filters = RoutedRowsBorrowedFilterLists{};
    defer right_filters.deinit(alloc);
    right_source = (try routedRowsLateralSideMergedQueryAlloc(alloc, right_schema, ctes, right_source, &right_filters)) orelse return null;

    var right_result = (try source.rowsQueryPlan(alloc, right_table_name, right_schema, .{
        .ranges = right_ranges,
        .query = right_source,
    }, consistency)) orelse return null;
    defer right_result.deinit(alloc);

    var matched_rows: u32 = 0;
    for (right_result.rows) |right_row| {
        if (!try db_relational_rows.lateralMatchPredicatesPassWithColumns(alloc, left_row, left_columns, right_row, right_columns, lateral, platform_time.realtimeNs())) continue;
        matched_rows = try addRoutedRowsCount(matched_rows, 1);
    }
    if (matched_rows == 0 and !db_relational_rows.lateralHasMatchPredicates(lateral)) return 1;
    return matched_rows;
}

fn countRoutedLateralFromLeftScanAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    left_table_name: []const u8,
    right_table_name: []const u8,
    left_schema: storage_schema.TableSchema,
    right_schema: storage_schema.TableSchema,
    left_query: db_mod.types.RelationalRowsQueryRequest,
    right_ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    ctes: []const db_mod.types.RelationalRowsCte,
    lateral: db_mod.types.RelationalRowsLateralRequest,
    from_key: []const u8,
    to_key: []const u8,
    consistency: raft_mod.ReadConsistency,
    left_offset_remaining: *u32,
    left_limit_remaining: ?*u32,
    total: *u32,
) !bool {
    var scan_result = (try source.scan(alloc, left_table_name, from_key, to_key, .{
        .include_documents = true,
        .include_all_fields = true,
    }, consistency)) orelse return false;
    defer scan_result.deinit(alloc);

    var lines = std.mem.splitScalar(u8, scan_result.ndjson, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        var row = try mergeScanRowFromScanLineAlloc(alloc, line);
        defer row.deinit(alloc);

        var lookup = (try source.lookup(alloc, left_table_name, row.key, .{ .include_all_fields = true }, consistency)) orelse return error.TopologyChanged;
        defer lookup.deinit(alloc);
        if (row.version != lookup.version) return error.TopologyChanged;
        if (!std.mem.eql(u8, row.json, lookup.json)) return error.TopologyChanged;
        if (!try relational_rows_api.rowsQueryJsonMatchesCountOnlyAlloc(alloc, left_schema, left_query, lookup.json)) continue;
        if (left_offset_remaining.* > 0) {
            left_offset_remaining.* -= 1;
            continue;
        }
        if (left_limit_remaining) |remaining| {
            if (remaining.* == 0) return true;
            remaining.* -= 1;
        }

        var parsed_left = std.json.parseFromSlice(std.json.Value, alloc, lookup.json, .{}) catch return error.InvalidRowsRequest;
        defer parsed_left.deinit();
        if (parsed_left.value != .object) return error.InvalidRowsRequest;
        const right_count = (try routedRowsLateralRightCountForLeftAlloc(
            alloc,
            source,
            right_table_name,
            right_schema,
            right_ranges,
            ctes,
            lateral,
            parsed_left.value,
            left_schema.relational_columns,
            right_schema.relational_columns,
            consistency,
        )) orelse return false;
        total.* = try addRoutedRowsCount(total.*, right_count);
        if (left_limit_remaining) |remaining| {
            if (remaining.* == 0) return true;
        }
    }
    return true;
}

fn appendRoutedLateralOrderedLeftRowsFromScanAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
    consistency: raft_mod.ReadConsistency,
    stream: *relational_rows_api.RowsQueryBoundedSortedStream,
) !bool {
    var scan_result = (try source.scan(alloc, table_name, from_key, to_key, .{
        .include_documents = true,
        .include_all_fields = true,
    }, consistency)) orelse return false;
    defer scan_result.deinit(alloc);

    var lines = std.mem.splitScalar(u8, scan_result.ndjson, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        var row = try mergeScanRowFromScanLineAlloc(alloc, line);
        defer row.deinit(alloc);

        var lookup = (try source.lookup(alloc, table_name, row.key, .{ .include_all_fields = true }, consistency)) orelse return error.TopologyChanged;
        defer lookup.deinit(alloc);
        if (row.version != lookup.version) return error.TopologyChanged;
        if (!std.mem.eql(u8, row.json, lookup.json)) return error.TopologyChanged;
        try stream.appendScannedRowJsonAlloc(alloc, lookup.json);
    }
    return true;
}

fn collectRoutedLateralOrderedLeftRowsAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    table_name: []const u8,
    schema: storage_schema.TableSchema,
    req: db_mod.types.RelationalRowsQueryRequest,
    ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    consistency: raft_mod.ReadConsistency,
) !?db_mod.types.RelationalRowsQueryResult {
    var stream = try relational_rows_api.RowsQueryBoundedSortedStream.init(schema, req);
    defer stream.deinit(alloc);

    var saw_source = false;
    if (ranges.len == 0) {
        stream.recordIteratorSeek();
        saw_source = try appendRoutedLateralOrderedLeftRowsFromScanAlloc(alloc, source, table_name, "", "", consistency, &stream);
    } else {
        for (ranges) |range| {
            stream.recordIteratorSeek();
            saw_source = (try appendRoutedLateralOrderedLeftRowsFromScanAlloc(alloc, source, table_name, range.start, range.end, consistency, &stream)) or saw_source;
        }
    }
    if (!saw_source) return null;
    return try stream.toResult(alloc);
}

fn countRoutedLateralFromLeftRowsAlloc(
    alloc: std.mem.Allocator,
    source: core.TableReadSource,
    right_table_name: []const u8,
    right_schema: storage_schema.TableSchema,
    right_ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    ctes: []const db_mod.types.RelationalRowsCte,
    lateral: db_mod.types.RelationalRowsLateralRequest,
    left_rows: []const []const u8,
    left_columns: []const storage_schema.RelationalColumn,
    right_columns: []const storage_schema.RelationalColumn,
    consistency: raft_mod.ReadConsistency,
) !?u32 {
    var total: u32 = 0;
    for (left_rows) |left_row| {
        var parsed_left = std.json.parseFromSlice(std.json.Value, alloc, left_row, .{}) catch return error.InvalidRowsRequest;
        defer parsed_left.deinit();
        if (parsed_left.value != .object) return error.InvalidRowsRequest;
        const right_count = (try routedRowsLateralRightCountForLeftAlloc(
            alloc,
            source,
            right_table_name,
            right_schema,
            right_ranges,
            ctes,
            lateral,
            parsed_left.value,
            left_columns,
            right_columns,
            consistency,
        )) orelse return null;
        total = try addRoutedRowsCount(total, right_count);
    }
    return total;
}

fn rowsLateralCountOnlyPlanFromRoutedScansWithSchemasAlloc(
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
    if (!routedRowsLateralPlanCanUseCountOnly(plan)) return null;

    const cte_ranges = if (plan.ctes.len == 0) &.{} else try routedRowsPlanRangesForJoinCtesAlloc(
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

    var left_filters = RoutedRowsBorrowedFilterLists{};
    defer left_filters.deinit(alloc);
    const left_uses_cte = plan.lateral.left.source_cte.len != 0;
    const left_scan_table = if (left_uses_cte) cte_table_name else left_table_name;
    const left_scan_schema = if (left_uses_cte) cte_base_schema else left_schema;
    const left_scan_ranges = if (left_uses_cte) cte_ranges else plan.left_ranges;
    const left_source = db_relational_rows.lateralLeftSource(plan.lateral.left);
    var left_query = (try routedRowsJoinSideMergedQueryAlloc(alloc, left_scan_schema, plan.ctes, left_source, &left_filters)) orelse return null;
    const right_uses_cte = plan.lateral.right.source_cte.len != 0;
    const right_scan_table = if (right_uses_cte) cte_table_name else right_table_name;
    const right_scan_schema = if (right_uses_cte) cte_base_schema else right_schema;
    const right_scan_ranges = if (right_uses_cte) cte_ranges else plan.right_ranges;
    const left_needs_ordered_page = plan.lateral.left.order_by.len != 0 and plan.lateral.left.limit != null;
    if (left_needs_ordered_page) {
        if (plan.lateral.left.limit.? == 0) {
            return .{
                .rows = &.{},
                .total_rows = 0,
            };
        }
        left_query.limit = plan.lateral.left.limit;
        var ordered_left = (try collectRoutedLateralOrderedLeftRowsAlloc(alloc, source, left_scan_table, left_scan_schema, left_query, left_scan_ranges, consistency)) orelse return null;
        defer ordered_left.deinit(alloc);
        const total = (try countRoutedLateralFromLeftRowsAlloc(
            alloc,
            source,
            right_scan_table,
            right_scan_schema,
            right_scan_ranges,
            plan.ctes,
            plan.lateral,
            ordered_left.rows,
            left_scan_schema.relational_columns,
            right_scan_schema.relational_columns,
            consistency,
        )) orelse return null;
        return .{
            .rows = &.{},
            .total_rows = total,
        };
    }

    var total: u32 = 0;
    var saw_left = false;
    var left_offset_remaining = plan.lateral.left.offset;
    var left_limit_storage = plan.lateral.left.limit orelse std.math.maxInt(u32);
    const left_limit_remaining: ?*u32 = if (plan.lateral.left.limit != null) &left_limit_storage else null;
    if (left_scan_ranges.len == 0) {
        saw_left = try countRoutedLateralFromLeftScanAlloc(alloc, source, left_scan_table, right_scan_table, left_scan_schema, right_scan_schema, left_query, right_scan_ranges, plan.ctes, plan.lateral, "", "", consistency, &left_offset_remaining, left_limit_remaining, &total);
    } else {
        for (left_scan_ranges) |range| {
            saw_left = (try countRoutedLateralFromLeftScanAlloc(alloc, source, left_scan_table, right_scan_table, left_scan_schema, right_scan_schema, left_query, right_scan_ranges, plan.ctes, plan.lateral, range.start, range.end, consistency, &left_offset_remaining, left_limit_remaining, &total)) or saw_left;
            if (left_limit_remaining) |remaining| {
                if (remaining.* == 0) break;
            }
        }
    }
    if (!saw_left) return null;
    return .{
        .rows = &.{},
        .total_rows = total,
    };
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
    try rejectRoutedRowsLateralPlanRowClaims(plan);
    try rejectRoutedRowsLateralPlanDocKeyRanges(plan);
    if (!scanPayloadCanStripSyntheticKey(cte_base_schema) or
        !scanPayloadCanStripSyntheticKey(left_schema) or
        !scanPayloadCanStripSyntheticKey(right_schema))
    {
        return error.UnsupportedRowsQuery;
    }
    if (try rowsLateralCountOnlyPlanFromRoutedScansWithSchemasAlloc(alloc, source, cte_table_name, left_table_name, right_table_name, cte_base_schema, left_schema, right_schema, plan, consistency)) |result| {
        return result;
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
        cte_rows_storage = (try collectStableRowsFromRoutedScansAlloc(alloc, source, cte_table_name, cte_base_schema, cte_ranges, consistency)) orelse return null;
        break :blk cte_rows_storage.?.rows;
    };
    var left_rows = (try collectStableRowsFromRoutedScansAlloc(alloc, source, left_table_name, left_schema, plan.left_ranges, consistency)) orelse return null;
    defer left_rows.deinit(alloc);
    var right_rows = (try collectStableRowsFromRoutedScansAlloc(alloc, source, right_table_name, right_schema, plan.right_ranges, consistency)) orelse return null;
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
    var spilled_execution = try executeSetOperationOnQueryResultsAlloc(std.testing.allocator, .{
        .operation = .union_all,
        .left = .{},
        .right = .{},
        .max_rows = 2,
        .max_bytes = observed_bytes,
        .spill_after_bytes = observed_bytes - 1,
        .order_by = &.{.{ .field = "id", .direction = .desc }},
    }, rows[0..1], rows[1..2]);
    defer spilled_execution.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 2), spilled_execution.total);
    try std.testing.expectEqualStrings("{\"id\":\"b\"}", spilled_execution.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"a\"}", spilled_execution.rows[1]);
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
    const recursive = sql_adapter.LoweredRecursiveCtePlan{
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

    const too_many_rows = sql_adapter.LoweredRecursiveCtePlan{
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

test "routed rows materialization tracker uses shared spill and hard cap policy" {
    var row_budget = row_spill.JsonRowsMaterializationTracker.init("test", 2, 128, 128);
    try row_budget.account("{\"id\":\"a\"}");
    try row_budget.account("{\"id\":\"b\"}");
    try std.testing.expect(!row_budget.spill_required);
    try std.testing.expectError(error.UnsupportedRowsQuery, row_budget.account("{\"id\":\"c\"}"));

    var byte_budget = row_spill.JsonRowsMaterializationTracker.init("test", 8, 16, 16);
    try byte_budget.account("{\"id\":\"a\"}");
    try std.testing.expectError(error.UnsupportedRowsQuery, byte_budget.account("{\"id\":\"b\"}"));

    var spill_budget = row_spill.JsonRowsMaterializationTracker.init("test", 8, 64, 16);
    try spill_budget.account("{\"id\":\"a\"}");
    try std.testing.expect(!spill_budget.spill_required);
    try spill_budget.account("{\"id\":\"b\"}");
    try std.testing.expect(spill_budget.spill_required);

    const rows = [_][]const u8{
        "{\"id\":\"a\"}",
        "{\"id\":\"b\"}",
    };
    var owned_rows = try std.testing.allocator.alloc([]const u8, rows.len);
    var owned_rows_count: usize = 0;
    errdefer {
        for (owned_rows[0..owned_rows_count]) |row| std.testing.allocator.free(@constCast(row));
        std.testing.allocator.free(owned_rows);
    }
    for (rows, 0..) |row, i| {
        owned_rows[i] = try std.testing.allocator.dupe(u8, row);
        owned_rows_count += 1;
    }
    const reloaded = try row_spill.spillAndReloadOwnedJsonRowsAlloc(std.testing.allocator, owned_rows, spill_budget.bytes, "routed-test");
    owned_rows = &.{};
    owned_rows_count = 0;
    defer freeOwnedRows(std.testing.allocator, reloaded);
    try std.testing.expectEqual(@as(usize, 2), reloaded.len);
    try std.testing.expectEqualStrings(rows[0], reloaded[0]);
    try std.testing.expectEqualStrings(rows[1], reloaded[1]);

    var collected = try std.testing.allocator.alloc(db_mod.types.RelationalRowsCollectedRow, 2);
    collected[0] = .{ .key = try std.testing.allocator.dupe(u8, "a"), .json = try std.testing.allocator.dupe(u8, rows[0]), .version = 3 };
    collected[1] = .{ .key = try std.testing.allocator.dupe(u8, "b"), .json = try std.testing.allocator.dupe(u8, rows[1]), .version = 4 };
    errdefer db_mod.types.freeRelationalRowsCollectedRows(std.testing.allocator, collected);
    const reloaded_collected = try spillAndReloadCollectedRowsAlloc(std.testing.allocator, collected, "routed-keyed-test");
    collected = &.{};
    defer db_mod.types.freeRelationalRowsCollectedRows(std.testing.allocator, reloaded_collected);
    try std.testing.expectEqual(@as(usize, 2), reloaded_collected.len);
    try std.testing.expectEqualStrings("a", reloaded_collected[0].key);
    try std.testing.expectEqualStrings(rows[0], reloaded_collected[0].json);
    try std.testing.expectEqual(@as(u64, 3), reloaded_collected[0].version);
    try std.testing.expectEqualStrings("b", reloaded_collected[1].key);
    try std.testing.expectEqualStrings(rows[1], reloaded_collected[1].json);
    try std.testing.expectEqual(@as(u64, 4), reloaded_collected[1].version);

    var profile_result = db_mod.types.RelationalRowsQueryResult{ .include_profile = true };
    addRoutedMaterializationProfile(&profile_result, .{
        .rows = &.{},
        .materialized_rows = spill_budget.rows,
        .materialized_bytes = spill_budget.bytes,
        .spilled = true,
        .spilled_rows = spill_budget.rows,
        .spilled_bytes = spill_budget.bytes,
    });
    defer profile_result.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 1), profile_result.profile.routed_materialization_fallbacks);
    try std.testing.expectEqual(spill_budget.rows, profile_result.profile.routed_materialized_rows);
    try std.testing.expectEqual(spill_budget.bytes, profile_result.profile.routed_materialized_bytes);
    try std.testing.expectEqual(@as(u64, 1), profile_result.profile.routed_spill_count);
    try std.testing.expectEqual(spill_budget.rows, profile_result.profile.routed_spilled_rows);
    try std.testing.expectEqual(spill_budget.bytes, profile_result.profile.routed_spilled_bytes);
}

test "routed paginated rows fail closed when live writes change scanned range membership" {
    const alloc = std.testing.allocator;

    var columns = [_]storage_schema.RelationalColumn{
        .{ .name = "id", .path = "id", .field_type = .keyword, .nullable = false },
        .{ .name = "amount", .path = "amount", .field_type = .numeric, .nullable = false },
    };
    const schema = storage_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = columns[0..],
    };

    const FakeSource = struct {
        live_insert_after_collect: bool = false,
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
            self.lookup_calls += 1;
            try std.testing.expectEqualStrings("orders", table_name);
            try std.testing.expect(opts.include_all_fields);
            const json = if (std.mem.eql(u8, key, "a"))
                "{\"id\":\"a\",\"amount\":1}"
            else if (std.mem.eql(u8, key, "b"))
                "{\"id\":\"b\",\"amount\":4}"
            else if (std.mem.eql(u8, key, "q"))
                "{\"id\":\"q\",\"amount\":2}"
            else if (std.mem.eql(u8, key, "z"))
                "{\"id\":\"z\",\"amount\":7}"
            else
                return null;
            return .{ .json = try lookup_alloc.dupe(u8, json), .version = 1 };
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
            self.scan_calls += 1;
            try std.testing.expectEqualStrings("orders", table_name);
            try std.testing.expect(opts.include_documents);
            try std.testing.expect(opts.include_all_fields);

            var out = std.ArrayListUnmanaged(u8).empty;
            errdefer out.deinit(scan_alloc);
            if (std.mem.eql(u8, from_key, "") and std.mem.eql(u8, to_key, "n")) {
                try appendScanLine(scan_alloc, &out, "a", "{\"id\":\"a\",\"amount\":1}", 1);
                try appendScanLine(scan_alloc, &out, "b", "{\"id\":\"b\",\"amount\":4}", 1);
                if (self.live_insert_after_collect and self.scan_calls > 2) {
                    try appendScanLine(scan_alloc, &out, "c", "{\"id\":\"c\",\"amount\":6}", 1);
                }
            } else if (std.mem.eql(u8, from_key, "n") and std.mem.eql(u8, to_key, "")) {
                try appendScanLine(scan_alloc, &out, "q", "{\"id\":\"q\",\"amount\":2}", 1);
                try appendScanLine(scan_alloc, &out, "z", "{\"id\":\"z\",\"amount\":7}", 1);
            } else {
                return error.UnexpectedRange;
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
            return null;
        }
    };

    const ranges = [_]db_mod.types.RelationalRowsDocKeyRange{
        .{ .start = "", .end = "n" },
        .{ .start = "n", .end = "" },
    };
    const order_by = [_]db_mod.types.RelationalRowsQueryOrder{.{
        .field = "amount",
        .direction = .desc,
    }};

    var stable_fake = FakeSource{};
    const stable_source = stable_fake.source();
    var result = (try rowsQueryPlanFromRoutedScansAlloc(alloc, stable_source, "orders", schema, .{
        .ranges = ranges[0..],
        .query = .{
            .select = &.{"id"},
            .select_all = false,
            .order_by = order_by[0..],
            .limit = 2,
            .offset = 1,
            .total_mode = .bounded,
            .profile = true,
        },
    }, .read_index)).?;
    defer result.deinit(alloc);

    try std.testing.expect(result.include_profile);
    try std.testing.expect(!result.total_exact);
    try std.testing.expectEqual(@as(u32, 3), result.total);
    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"b\"}", result.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"q\"}", result.rows[1]);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryResult.AccessMethod.base_scan, result.profile.access_method);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryRequest.TotalMode.bounded, result.profile.total_mode);
    try std.testing.expectEqual(@as(u64, 4), result.profile.base_scan_rows);
    try std.testing.expectEqual(@as(u64, 4), result.profile.candidate_rows);
    try std.testing.expectEqual(@as(u64, 4), result.profile.candidate_stream_emitted);
    try std.testing.expectEqual(@as(u64, 3), result.profile.retained_candidate_rows);
    try std.testing.expectEqual(@as(u64, 2), result.profile.projected_rows);
    try std.testing.expectEqual(@as(u64, 2), result.profile.iterator_seeks);
    try std.testing.expectEqual(@as(usize, 4), stable_fake.scan_calls);
    try std.testing.expectEqual(@as(usize, 4), stable_fake.lookup_calls);

    var live_insert_fake = FakeSource{ .live_insert_after_collect = true };
    const live_insert_source = live_insert_fake.source();
    try std.testing.expectError(error.TopologyChanged, rowsQueryPlanFromRoutedScansAlloc(alloc, live_insert_source, "orders", schema, .{
        .ranges = ranges[0..],
        .query = .{
            .select = &.{"id"},
            .select_all = false,
            .order_by = order_by[0..],
            .limit = 2,
            .offset = 1,
        },
    }, .read_index));
    try std.testing.expectEqual(@as(usize, 3), live_insert_fake.scan_calls);
    try std.testing.expectEqual(@as(usize, 4), live_insert_fake.lookup_calls);
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
        const LookupMode = enum { stable, missing, changed, first_range_changed, version_changed, missing_version, zero_scan_version, rescan_inserted_first_range };

        scan_calls: usize = 0,
        lookup_calls: usize = 0,
        lookup_mode: LookupMode = .stable,

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
            ptr: *anyopaque,
            lookup_alloc: std.mem.Allocator,
            table_name: []const u8,
            key: []const u8,
            _: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.lookup_calls += 1;
            if (self.lookup_mode == .missing and std.mem.eql(u8, table_name, "orders") and std.mem.eql(u8, key, "z")) return null;
            const json = if (std.mem.eql(u8, table_name, "orders"))
                if (std.mem.eql(u8, key, "a"))
                    if (self.lookup_mode == .first_range_changed)
                        "{\"id\":\"a\",\"status\":\"open\",\"amount\":2}"
                    else
                        "{\"id\":\"a\",\"status\":\"open\",\"amount\":1}"
                else if (std.mem.eql(u8, key, "b"))
                    "{\"id\":\"b\",\"status\":\"closed\",\"amount\":9}"
                else if (std.mem.eql(u8, key, "z"))
                    if (self.lookup_mode == .changed)
                        "{\"id\":\"z\",\"status\":\"open\",\"amount\":8}"
                    else
                        "{\"id\":\"z\",\"status\":\"open\",\"amount\":7}"
                else
                    return null
            else if (std.mem.eql(u8, table_name, "customers"))
                if (std.mem.eql(u8, key, "c1"))
                    "{\"id\":\"c1\",\"status\":\"open\",\"name\":\"Ada\"}"
                else if (std.mem.eql(u8, key, "c2"))
                    "{\"id\":\"c2\",\"status\":\"closed\",\"name\":\"Grace\"}"
                else
                    return null
            else
                return null;
            const version: u64 = if (self.lookup_mode == .version_changed and std.mem.eql(u8, table_name, "orders") and std.mem.eql(u8, key, "z")) 2 else 1;
            return .{ .json = try lookup_alloc.dupe(u8, json), .version = version };
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
                    if (self.lookup_mode == .rescan_inserted_first_range and self.scan_calls > 2)
                        "{\"key\":\"a\",\"version\":1,\"id\":\"a\",\"status\":\"open\",\"amount\":1}\n{\"key\":\"b\",\"version\":1,\"id\":\"b\",\"status\":\"closed\",\"amount\":9}\n{\"key\":\"c\",\"version\":1,\"id\":\"c\",\"status\":\"open\",\"amount\":5}\n"
                    else
                        "{\"key\":\"a\",\"version\":1,\"id\":\"a\",\"status\":\"open\",\"amount\":1}\n{\"key\":\"b\",\"version\":1,\"id\":\"b\",\"status\":\"closed\",\"amount\":9}\n"
                else if (std.mem.eql(u8, from_key, "n") and std.mem.eql(u8, to_key, ""))
                    if (self.lookup_mode == .missing_version)
                        "{\"key\":\"z\",\"id\":\"z\",\"status\":\"open\",\"amount\":7}\n"
                    else if (self.lookup_mode == .zero_scan_version)
                        "{\"key\":\"z\",\"version\":0,\"id\":\"z\",\"status\":\"open\",\"amount\":7}\n"
                    else
                        "{\"key\":\"z\",\"version\":1,\"id\":\"z\",\"status\":\"open\",\"amount\":7}\n"
                else if (std.mem.eql(u8, from_key, "") and std.mem.eql(u8, to_key, ""))
                    "{\"key\":\"a\",\"version\":1,\"id\":\"a\",\"status\":\"open\",\"amount\":1}\n{\"key\":\"b\",\"version\":1,\"id\":\"b\",\"status\":\"closed\",\"amount\":9}\n{\"key\":\"z\",\"version\":1,\"id\":\"z\",\"status\":\"open\",\"amount\":7}\n"
                else
                    return error.UnexpectedRange
            else if (std.mem.eql(u8, table_name, "customers"))
                if (std.mem.eql(u8, from_key, "") and std.mem.eql(u8, to_key, ""))
                    "{\"key\":\"c1\",\"version\":1,\"id\":\"c1\",\"status\":\"open\",\"name\":\"Ada\"}\n{\"key\":\"c2\",\"version\":1,\"id\":\"c2\",\"status\":\"closed\",\"name\":\"Grace\"}\n"
                else
                    return error.UnexpectedRange
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
            .total_mode = .bounded,
            .profile = true,
        },
    }, .read_index)).?;
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 4), fake.scan_calls);
    try std.testing.expect(result.include_profile);
    try std.testing.expect(!result.total_exact);
    try std.testing.expectEqual(@as(u32, 1), result.total);
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"z\"}", result.rows[0]);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryResult.AccessMethod.base_scan, result.profile.access_method);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryRequest.TotalMode.bounded, result.profile.total_mode);
    try std.testing.expectEqual(@as(u64, 2), result.profile.base_scan_rows);
    try std.testing.expectEqual(@as(u64, 2), result.profile.candidate_rows);
    try std.testing.expectEqual(@as(u64, 2), result.profile.candidate_stream_emitted);
    try std.testing.expectEqual(@as(u64, 1), result.profile.retained_candidate_rows);
    try std.testing.expectEqual(@as(u64, 1), result.profile.projected_rows);
    try std.testing.expectEqual(@as(u64, 2), result.profile.iterator_seeks);
    try std.testing.expectEqual(@as(usize, 3), fake.lookup_calls);

    const count_only_scan_calls_before = fake.scan_calls;
    const count_only_lookup_calls_before = fake.lookup_calls;
    var count_only = (try source.rowsQueryPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .ranges = ranges[0..],
        .query = .{
            .source_cte = "open_rows",
            .limit = 0,
            .total_mode = .exact,
            .profile = true,
        },
    }, .read_index)).?;
    defer count_only.deinit(alloc);
    try std.testing.expect(count_only.include_profile);
    try std.testing.expect(count_only.total_exact);
    try std.testing.expectEqual(@as(u32, 2), count_only.total);
    try std.testing.expectEqual(@as(usize, 0), count_only.rows.len);
    try std.testing.expect(count_only.profile.count_only);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryRequest.TotalMode.exact, count_only.profile.total_mode);
    try std.testing.expectEqual(@as(u64, 0), count_only.profile.retained_candidate_rows);
    try std.testing.expectEqual(@as(u64, 0), count_only.profile.projected_rows);
    try std.testing.expectEqual(count_only_scan_calls_before + 2, fake.scan_calls);
    try std.testing.expectEqual(count_only_lookup_calls_before + 3, fake.lookup_calls);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryResult.AccessMethod.base_scan, count_only.profile.access_method);
    try std.testing.expectEqual(@as(u64, 3), count_only.profile.base_scan_rows);
    try std.testing.expectEqual(@as(u64, 3), count_only.profile.candidate_rows);

    const expensive_predicates = [_]storage_schema.RelationalCheck{.{
        .name = "amount_gt_five",
        .field = "amount",
        .op = .gt,
        .value_json = "5",
    }};
    const chained_ctes = [_]db_mod.types.RelationalRowsCte{
        .{
            .name = "open_rows",
            .query = .{
                .predicates = cte_predicates[0..],
                .select = cte_select[0..],
                .select_all = false,
            },
        },
        .{
            .name = "expensive_open_rows",
            .query = .{
                .source_cte = "open_rows",
                .predicates = expensive_predicates[0..],
                .select = cte_select[0..],
                .select_all = false,
            },
        },
    };
    const chained_count_scan_calls_before = fake.scan_calls;
    const chained_count_lookup_calls_before = fake.lookup_calls;
    var chained_count_only = (try source.rowsQueryPlan(alloc, "orders", schema, .{
        .ctes = chained_ctes[0..],
        .ranges = ranges[0..],
        .query = .{
            .source_cte = "expensive_open_rows",
            .limit = 0,
            .total_mode = .exact,
            .profile = true,
        },
    }, .read_index)).?;
    defer chained_count_only.deinit(alloc);
    try std.testing.expect(chained_count_only.include_profile);
    try std.testing.expect(chained_count_only.total_exact);
    try std.testing.expectEqual(@as(u32, 1), chained_count_only.total);
    try std.testing.expectEqual(@as(usize, 0), chained_count_only.rows.len);
    try std.testing.expect(chained_count_only.profile.count_only);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryRequest.TotalMode.exact, chained_count_only.profile.total_mode);
    try std.testing.expectEqual(@as(u64, 0), chained_count_only.profile.retained_candidate_rows);
    try std.testing.expectEqual(@as(u64, 0), chained_count_only.profile.projected_rows);
    try std.testing.expectEqual(@as(u64, 0), chained_count_only.profile.routed_materialization_fallbacks);
    try std.testing.expectEqual(chained_count_scan_calls_before + 2, fake.scan_calls);
    try std.testing.expectEqual(chained_count_lookup_calls_before + 3, fake.lookup_calls);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryResult.AccessMethod.base_scan, chained_count_only.profile.access_method);
    try std.testing.expectEqual(@as(u64, 3), chained_count_only.profile.base_scan_rows);
    try std.testing.expectEqual(@as(u64, 3), chained_count_only.profile.candidate_rows);

    const non_cte_count_scan_calls_before = fake.scan_calls;
    const non_cte_count_lookup_calls_before = fake.lookup_calls;
    var non_cte_count_only = (try source.rowsQueryPlan(alloc, "orders", schema, .{
        .ranges = ranges[0..],
        .query = .{
            .predicates = cte_predicates[0..],
            .limit = 0,
            .total_mode = .exact,
            .profile = true,
        },
    }, .read_index)).?;
    defer non_cte_count_only.deinit(alloc);
    try std.testing.expect(non_cte_count_only.include_profile);
    try std.testing.expect(non_cte_count_only.total_exact);
    try std.testing.expectEqual(@as(u32, 2), non_cte_count_only.total);
    try std.testing.expectEqual(@as(usize, 0), non_cte_count_only.rows.len);
    try std.testing.expect(non_cte_count_only.profile.count_only);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryRequest.TotalMode.exact, non_cte_count_only.profile.total_mode);
    try std.testing.expectEqual(@as(u64, 0), non_cte_count_only.profile.retained_candidate_rows);
    try std.testing.expectEqual(@as(u64, 0), non_cte_count_only.profile.projected_rows);
    try std.testing.expectEqual(non_cte_count_scan_calls_before + 2, fake.scan_calls);
    try std.testing.expectEqual(non_cte_count_lookup_calls_before + 3, fake.lookup_calls);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryResult.AccessMethod.base_scan, non_cte_count_only.profile.access_method);
    try std.testing.expectEqual(@as(u64, 3), non_cte_count_only.profile.base_scan_rows);
    try std.testing.expectEqual(@as(u64, 3), non_cte_count_only.profile.candidate_rows);

    const generic_fallback_scan_calls_before = fake.scan_calls;
    const generic_fallback_lookup_calls_before = fake.lookup_calls;
    const distinct_on = [_][]const u8{"status"};
    var generic_fallback = (try source.rowsQueryPlan(alloc, "orders", schema, .{
        .ranges = ranges[0..],
        .query = .{
            .distinct_on = distinct_on[0..],
            .select = &.{"status"},
            .select_all = false,
            .profile = true,
        },
    }, .read_index)).?;
    defer generic_fallback.deinit(alloc);
    try std.testing.expect(generic_fallback.include_profile);
    try std.testing.expect(generic_fallback.total_exact);
    try std.testing.expectEqual(@as(u32, 2), generic_fallback.total);
    try std.testing.expectEqual(@as(usize, 2), generic_fallback.rows.len);
    try std.testing.expectEqual(@as(u64, 1), generic_fallback.profile.routed_materialization_fallbacks);
    try std.testing.expectEqual(@as(u64, 3), generic_fallback.profile.routed_materialized_rows);
    try std.testing.expect(generic_fallback.profile.routed_materialized_bytes > 0);
    try std.testing.expectEqual(@as(u64, 0), generic_fallback.profile.routed_spill_count);
    try std.testing.expectEqual(generic_fallback_scan_calls_before + 4, fake.scan_calls);
    try std.testing.expectEqual(generic_fallback_lookup_calls_before + 3, fake.lookup_calls);

    var inserted_during_pagination_fake = FakeRoutedSource{ .lookup_mode = .rescan_inserted_first_range };
    var inserted_during_pagination_source = inserted_during_pagination_fake.source();
    try std.testing.expectError(error.TopologyChanged, inserted_during_pagination_source.rowsQueryPlan(alloc, "orders", schema, .{
        .ranges = ranges[0..],
        .query = .{
            .select = &.{"id"},
            .select_all = false,
            .order_by = &.{.{ .field = "amount", .direction = .desc }},
            .limit = 1,
        },
    }, .read_index));
    try std.testing.expectEqual(@as(usize, 3), inserted_during_pagination_fake.scan_calls);
    try std.testing.expectEqual(@as(usize, 3), inserted_during_pagination_fake.lookup_calls);

    const aggregate_group_by = [_][]const u8{"status"};
    const aggregate_expression_operands = [_]db_mod.types.RelationalRowsExpression{
        .{ .kind = .field, .field = "amount" },
        .{ .kind = .value, .value_json = "1" },
    };
    const aggregate_specs = [_]db_mod.types.RelationalRowsAggregateSpec{
        .{ .name = "order_count", .op = .count },
        .{ .name = "amount_sum", .op = .sum, .field = "amount" },
        .{
            .name = "amount_plus_one_sum",
            .op = .sum,
            .expression = .{
                .kind = .add,
                .operands = aggregate_expression_operands[0..],
            },
        },
    };
    try std.testing.expectEqual(
        db_mod.types.RelationalRowsAggregatePushdownCapability.local_expression_evaluation_required,
        db_mod.types.relationalRowsAggregatePushdownCapability(.{
            .source = .{ .source_cte = "open_rows" },
            .group_by = aggregate_group_by[0..],
            .aggregations = aggregate_specs[0..],
        }),
    );
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
    try std.testing.expectEqualStrings("{\"status\":\"open\",\"order_count\":2,\"amount_sum\":8,\"amount_plus_one_sum\":10}", aggregate_result.rows[0]);

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
    const lateral_correlations = [_]db_mod.types.RelationalRowsLateralCorrelation{.{
        .left_field = "status",
        .right_field = "status",
    }};

    const rejected_claim = db_mod.types.RowClaimRequest{
        .owner_id = "session:routed-read-claim",
        .txn_id = [_]u8{ 0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79, 0x7a, 0x7b, 0x7c, 0x7d, 0x7e, 0x7f },
    };
    const claimed_ctes = [_]db_mod.types.RelationalRowsCte{.{
        .name = "claimed_rows",
        .query = .{
            .row_claim = rejected_claim,
            .select = cte_select[0..],
            .select_all = false,
        },
    }};
    const scans_before_claimed_read = fake.scan_calls;
    try std.testing.expectError(error.UnsupportedRowsQuery, source.rowsQueryPlan(alloc, "orders", schema, .{
        .ctes = claimed_ctes[0..],
        .ranges = ranges[0..],
        .query = .{
            .source_cte = "claimed_rows",
            .select = final_select[0..],
            .select_all = false,
        },
    }, .read_index));
    try std.testing.expectError(error.UnsupportedRowsQuery, source.rowsAggregatePlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .ranges = ranges[0..],
        .aggregate = .{
            .source = .{ .source_cte = "open_rows", .row_claim = rejected_claim },
            .group_by = aggregate_group_by[0..],
            .aggregations = aggregate_specs[0..],
        },
    }, .read_index));
    try std.testing.expectError(error.UnsupportedRowsQuery, source.rowsWindowPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .ranges = ranges[0..],
        .window = .{
            .source = .{ .source_cte = "open_rows", .row_claim = rejected_claim },
            .windows = window_specs[0..],
            .select = window_select[0..],
            .select_all = false,
        },
    }, .read_index));
    try std.testing.expectError(error.UnsupportedRowsQuery, source.rowsJoinPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .join = .{
            .left = .{ .source_cte = "open_rows", .row_claim = rejected_claim },
            .right = .{},
            .on = join_on[0..],
            .select = join_select[0..],
        },
    }, .read_index));
    try std.testing.expectError(error.UnsupportedRowsQuery, source.rowsLateralPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .lateral = .{
            .left = .{ .source_cte = "open_rows" },
            .right = .{ .row_claim = rejected_claim, .order_by = &.{.{ .field = "amount", .direction = .desc }}, .limit = 1 },
            .correlations = lateral_correlations[0..],
            .select = join_select[0..],
        },
    }, .read_index));
    try std.testing.expectEqual(scans_before_claimed_read, fake.scan_calls);

    const rejected_doc_range = db_mod.types.RelationalRowsDocKeyRange{ .start = "row:a", .end = "row:z" };
    const scans_before_ranged_read = fake.scan_calls;
    try std.testing.expectError(error.InvalidRowsRequest, source.rowsQueryPlan(alloc, "orders", schema, .{
        .ranges = ranges[0..],
        .query = .{
            .doc_key_range = rejected_doc_range,
            .select = final_select[0..],
            .select_all = false,
        },
    }, .read_index));
    var doc_ranged_ctes = [_]db_mod.types.RelationalRowsCte{ctes[0]};
    doc_ranged_ctes[0].query.doc_key_range = rejected_doc_range;
    try std.testing.expectError(error.InvalidRowsRequest, source.rowsQueryPlan(alloc, "orders", schema, .{
        .ctes = doc_ranged_ctes[0..],
        .ranges = ranges[0..],
        .query = .{
            .source_cte = "open_rows",
            .select = final_select[0..],
            .select_all = false,
        },
    }, .read_index));
    try std.testing.expectError(error.InvalidRowsRequest, source.rowsAggregatePlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .ranges = ranges[0..],
        .aggregate = .{
            .source = .{ .source_cte = "open_rows", .doc_key_range = rejected_doc_range },
            .group_by = aggregate_group_by[0..],
            .aggregations = aggregate_specs[0..],
        },
    }, .read_index));
    try std.testing.expectError(error.InvalidRowsRequest, source.rowsWindowPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .ranges = ranges[0..],
        .window = .{
            .source = .{ .source_cte = "open_rows", .doc_key_range = rejected_doc_range },
            .windows = window_specs[0..],
            .select = window_select[0..],
            .select_all = false,
        },
    }, .read_index));
    try std.testing.expectError(error.InvalidRowsRequest, source.rowsJoinPlan(alloc, "orders", schema, .{
        .left_ranges = ranges[0..],
        .right_ranges = ranges[0..],
        .join = .{
            .left = .{ .doc_key_range = rejected_doc_range },
            .right = .{},
            .on = join_on[0..],
            .select = join_select[0..],
        },
    }, .read_index));
    try std.testing.expectError(error.InvalidRowsRequest, source.rowsLateralPlan(alloc, "orders", schema, .{
        .left_ranges = ranges[0..],
        .right_ranges = ranges[0..],
        .lateral = .{
            .left = .{},
            .right = .{ .doc_key_range = rejected_doc_range, .order_by = &.{.{ .field = "amount", .direction = .desc }}, .limit = 1 },
            .correlations = lateral_correlations[0..],
            .select = join_select[0..],
        },
    }, .read_index));
    try std.testing.expectEqual(scans_before_ranged_read, fake.scan_calls);

    const scans_before_embedded_range = fake.scan_calls;
    try std.testing.expectError(error.InvalidRowsRequest, source.rowsQueryPlan(alloc, "orders", schema, .{
        .query = .{
            .doc_key_range = rejected_doc_range,
            .select = final_select[0..],
            .select_all = false,
        },
    }, .read_index));
    try std.testing.expectError(error.InvalidRowsRequest, source.rowsAggregatePlan(alloc, "orders", schema, .{
        .aggregate = .{
            .source = .{ .doc_key_range = rejected_doc_range },
            .group_by = aggregate_group_by[0..],
            .aggregations = aggregate_specs[0..],
        },
    }, .read_index));
    try std.testing.expectError(error.InvalidRowsRequest, source.rowsWindowPlan(alloc, "orders", schema, .{
        .window = .{
            .source = .{ .doc_key_range = rejected_doc_range },
            .windows = window_specs[0..],
            .select = window_select[0..],
            .select_all = false,
        },
    }, .read_index));
    try std.testing.expectError(error.InvalidRowsRequest, source.rowsJoinPlan(alloc, "orders", schema, .{
        .join = .{
            .left = .{ .doc_key_range = rejected_doc_range },
            .right = .{},
            .on = join_on[0..],
            .select = join_select[0..],
        },
    }, .read_index));
    try std.testing.expectError(error.InvalidRowsRequest, source.rowsLateralPlan(alloc, "orders", schema, .{
        .lateral = .{
            .left = .{},
            .right = .{ .doc_key_range = rejected_doc_range, .order_by = &.{.{ .field = "amount", .direction = .desc }}, .limit = 1 },
            .correlations = lateral_correlations[0..],
            .select = join_select[0..],
        },
    }, .read_index));
    try std.testing.expectEqual(scans_before_embedded_range, fake.scan_calls);

    var first_range_changed_fake = FakeRoutedSource{ .lookup_mode = .first_range_changed };
    var first_range_changed_source = first_range_changed_fake.source();
    try std.testing.expectError(error.TopologyChanged, first_range_changed_source.rowsQueryPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .ranges = ranges[0..],
        .query = .{
            .source_cte = "open_rows",
            .select = &.{ "id", "amount" },
            .order_by = &.{.{ .field = "amount", .direction = .desc }},
        },
    }, .read_index));
    try std.testing.expectEqual(@as(usize, 1), first_range_changed_fake.scan_calls);
    try std.testing.expectEqual(@as(usize, 1), first_range_changed_fake.lookup_calls);

    var missing_fake = FakeRoutedSource{ .lookup_mode = .missing };
    var missing_source = missing_fake.source();
    try std.testing.expectError(error.TopologyChanged, missing_source.rowsQueryPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .ranges = ranges[0..],
        .query = .{
            .source_cte = "open_rows",
            .select = &.{ "id", "amount" },
            .order_by = &.{.{ .field = "amount", .direction = .desc }},
        },
    }, .read_index));
    try std.testing.expectEqual(@as(usize, 2), missing_fake.scan_calls);
    try std.testing.expectEqual(@as(usize, 3), missing_fake.lookup_calls);

    var missing_join_fake = FakeRoutedSource{ .lookup_mode = .missing };
    var missing_join_source = missing_join_fake.source();
    try std.testing.expectError(error.TopologyChanged, missing_join_source.rowsJoinPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .join = .{
            .left = .{ .source_cte = "open_rows" },
            .right = .{},
            .on = join_on[0..],
            .select = join_select[0..],
        },
    }, .read_index));
    try std.testing.expectEqual(@as(usize, 1), missing_join_fake.scan_calls);
    try std.testing.expectEqual(@as(usize, 3), missing_join_fake.lookup_calls);

    var missing_lateral_fake = FakeRoutedSource{ .lookup_mode = .missing };
    var missing_lateral_source = missing_lateral_fake.source();
    try std.testing.expectError(error.TopologyChanged, missing_lateral_source.rowsLateralPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .lateral = .{
            .left = .{ .source_cte = "open_rows" },
            .right = .{ .order_by = &.{.{ .field = "amount", .direction = .desc }}, .limit = 1 },
            .correlations = lateral_correlations[0..],
            .select = join_select[0..],
        },
    }, .read_index));
    try std.testing.expectEqual(@as(usize, 1), missing_lateral_fake.scan_calls);
    try std.testing.expectEqual(@as(usize, 3), missing_lateral_fake.lookup_calls);

    var missing_ranged_join_fake = FakeRoutedSource{ .lookup_mode = .missing };
    var missing_ranged_join_source = missing_ranged_join_fake.source();
    try std.testing.expectError(error.TopologyChanged, missing_ranged_join_source.rowsJoinPlan(alloc, "orders", schema, .{
        .left_ranges = ranges[0..],
        .right_ranges = ranges[0..],
        .join = .{
            .left = .{},
            .right = .{},
            .on = join_on[0..],
            .select = join_select[0..],
        },
    }, .read_index));
    try std.testing.expectEqual(@as(usize, 2), missing_ranged_join_fake.scan_calls);
    try std.testing.expectEqual(@as(usize, 3), missing_ranged_join_fake.lookup_calls);

    var changed_fake = FakeRoutedSource{ .lookup_mode = .changed };
    var changed_source = changed_fake.source();
    try std.testing.expectError(error.TopologyChanged, changed_source.rowsAggregatePlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .ranges = ranges[0..],
        .aggregate = .{
            .source = .{ .source_cte = "open_rows" },
            .group_by = aggregate_group_by[0..],
            .aggregations = aggregate_specs[0..],
        },
    }, .read_index));
    try std.testing.expectError(error.TopologyChanged, changed_source.rowsWindowPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .ranges = ranges[0..],
        .window = .{
            .source = .{ .source_cte = "open_rows" },
            .windows = window_specs[0..],
            .select = window_select[0..],
            .select_all = false,
            .order_by = &.{.{ .field = "rn", .direction = .asc }},
        },
    }, .read_index));

    try std.testing.expectError(error.TopologyChanged, changed_source.rowsJoinPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .join = .{
            .left = .{ .source_cte = "open_rows" },
            .right = .{},
            .on = join_on[0..],
            .select = join_select[0..],
        },
    }, .read_index));

    try std.testing.expectError(error.TopologyChanged, changed_source.rowsLateralPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .lateral = .{
            .left = .{ .source_cte = "open_rows" },
            .right = .{ .order_by = &.{.{ .field = "amount", .direction = .desc }}, .limit = 1 },
            .correlations = lateral_correlations[0..],
            .select = join_select[0..],
        },
    }, .read_index));
    try std.testing.expectError(error.TopologyChanged, collectMergeTargetRowsFromRoutedScansAlloc(
        alloc,
        changed_source,
        "orders",
        schema,
        .{
            .order_by = order_by[0..],
            .limit = 1,
            .select_all = true,
        },
        ranges[0..],
        .read_index,
    ));
    try std.testing.expectError(error.TopologyChanged, collectMergeSourceRowsFromRoutedScansAlloc(
        alloc,
        changed_source,
        "orders",
        schema,
        .{
            .order_by = order_by[0..],
            .limit = 1,
            .select_all = true,
        },
        ranges[0..],
        .read_index,
    ));

    var changed_ranged_join_fake = FakeRoutedSource{ .lookup_mode = .changed };
    var changed_ranged_join_source = changed_ranged_join_fake.source();
    try std.testing.expectError(error.TopologyChanged, changed_ranged_join_source.rowsJoinPlan(alloc, "orders", schema, .{
        .left_ranges = ranges[0..],
        .right_ranges = ranges[0..],
        .join = .{
            .left = .{},
            .right = .{},
            .on = join_on[0..],
            .select = join_select[0..],
        },
    }, .read_index));
    try std.testing.expectEqual(@as(usize, 2), changed_ranged_join_fake.scan_calls);
    try std.testing.expectEqual(@as(usize, 3), changed_ranged_join_fake.lookup_calls);

    var version_changed_fake = FakeRoutedSource{ .lookup_mode = .version_changed };
    var version_changed_source = version_changed_fake.source();
    try std.testing.expectError(error.TopologyChanged, version_changed_source.rowsQueryPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .ranges = ranges[0..],
        .query = .{
            .source_cte = "open_rows",
            .select = &.{ "id", "amount" },
            .order_by = &.{.{ .field = "amount", .direction = .desc }},
        },
    }, .read_index));
    try std.testing.expectError(error.TopologyChanged, version_changed_source.rowsAggregatePlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .ranges = ranges[0..],
        .aggregate = .{
            .source = .{ .source_cte = "open_rows" },
            .group_by = aggregate_group_by[0..],
            .aggregations = aggregate_specs[0..],
        },
    }, .read_index));
    try std.testing.expectError(error.TopologyChanged, version_changed_source.rowsWindowPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .ranges = ranges[0..],
        .window = .{
            .source = .{ .source_cte = "open_rows" },
            .windows = window_specs[0..],
            .select = window_select[0..],
            .select_all = false,
            .order_by = &.{.{ .field = "rn", .direction = .asc }},
        },
    }, .read_index));
    try std.testing.expectError(error.TopologyChanged, version_changed_source.rowsJoinPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .join = .{
            .left = .{ .source_cte = "open_rows" },
            .right = .{},
            .on = join_on[0..],
            .select = join_select[0..],
        },
    }, .read_index));
    try std.testing.expectError(error.TopologyChanged, version_changed_source.rowsLateralPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .lateral = .{
            .left = .{ .source_cte = "open_rows" },
            .right = .{ .order_by = &.{.{ .field = "amount", .direction = .desc }}, .limit = 1 },
            .correlations = lateral_correlations[0..],
            .select = join_select[0..],
        },
    }, .read_index));

    var version_changed_ranged_join_fake = FakeRoutedSource{ .lookup_mode = .version_changed };
    var version_changed_ranged_join_source = version_changed_ranged_join_fake.source();
    try std.testing.expectError(error.TopologyChanged, version_changed_ranged_join_source.rowsJoinPlan(alloc, "orders", schema, .{
        .left_ranges = ranges[0..],
        .right_ranges = ranges[0..],
        .join = .{
            .left = .{},
            .right = .{},
            .on = join_on[0..],
            .select = join_select[0..],
        },
    }, .read_index));
    try std.testing.expectEqual(@as(usize, 2), version_changed_ranged_join_fake.scan_calls);
    try std.testing.expectEqual(@as(usize, 3), version_changed_ranged_join_fake.lookup_calls);

    var missing_version_fake = FakeRoutedSource{ .lookup_mode = .missing_version };
    var missing_version_source = missing_version_fake.source();
    try std.testing.expectError(error.InvalidRemoteResponse, missing_version_source.rowsQueryPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .ranges = ranges[0..],
        .query = .{
            .source_cte = "open_rows",
            .select = &.{ "id", "amount" },
            .order_by = &.{.{ .field = "amount", .direction = .desc }},
        },
    }, .read_index));

    var zero_scan_version_fake = FakeRoutedSource{ .lookup_mode = .zero_scan_version };
    var zero_scan_version_source = zero_scan_version_fake.source();
    try std.testing.expectError(error.TopologyChanged, zero_scan_version_source.rowsQueryPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .ranges = ranges[0..],
        .query = .{
            .source_cte = "open_rows",
            .select = &.{ "id", "amount" },
            .order_by = &.{.{ .field = "amount", .direction = .desc }},
        },
    }, .read_index));

    const scans_before_unsupported_merge = fake.scan_calls;
    try std.testing.expectError(error.UnsupportedRowsQuery, source.rowsJoinPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .join = .{
            .left = .{ .source_cte = "open_rows" },
            .right = .{},
            .on = join_on[0..],
            .strategy = .merge,
            .select = join_select[0..],
        },
    }, .read_index));
    try std.testing.expectEqual(scans_before_unsupported_merge, fake.scan_calls);

    var merge_join_result = (try source.rowsJoinPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .join = .{
            .left = .{ .source_cte = "open_rows", .order_by = &.{.{ .field = "status", .direction = .asc }} },
            .right = .{ .order_by = &.{.{ .field = "status", .direction = .asc }} },
            .on = join_on[0..],
            .strategy = .merge,
            .select = join_select[0..],
            .order_by = &.{ .{ .field = "left_id", .direction = .asc }, .{ .field = "right_id", .direction = .asc } },
        },
    }, .read_index)).?;
    defer merge_join_result.deinit(alloc);
    try std.testing.expectEqual(db_mod.types.RelationalRowsJoinStrategy.merge, merge_join_result.strategy_selection.?.requested);
    try std.testing.expectEqual(db_mod.types.RelationalRowsJoinStrategy.merge, merge_join_result.strategy_selection.?.selected);
    try std.testing.expectEqual(@as(u32, 4), merge_join_result.total_rows);
    try std.testing.expectEqualStrings("{\"left_id\":\"a\",\"right_id\":\"a\"}", merge_join_result.rows[0]);

    const scans_before_ranged_merge = fake.scan_calls;
    try std.testing.expectError(error.UnsupportedRowsQuery, source.rowsJoinPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .left_ranges = ranges[0..],
        .right_ranges = ranges[0..],
        .join = .{
            .left = .{ .source_cte = "open_rows", .order_by = &.{.{ .field = "status", .direction = .asc }} },
            .right = .{ .order_by = &.{.{ .field = "status", .direction = .asc }} },
            .on = join_on[0..],
            .strategy = .merge,
            .select = join_select[0..],
        },
    }, .read_index));
    try std.testing.expectEqual(scans_before_ranged_merge, fake.scan_calls);

    const scans_before_filtered_merge = fake.scan_calls;
    try std.testing.expectError(error.UnsupportedRowsQuery, source.rowsJoinPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .join = .{
            .left = .{
                .source_cte = "open_rows",
                .predicates = cte_predicates[0..],
                .order_by = &.{.{ .field = "status", .direction = .asc }},
            },
            .right = .{ .order_by = &.{.{ .field = "status", .direction = .asc }} },
            .on = join_on[0..],
            .strategy = .merge,
            .select = join_select[0..],
        },
    }, .read_index));
    try std.testing.expectEqual(scans_before_filtered_merge, fake.scan_calls);

    const scans_before_paged_merge = fake.scan_calls;
    try std.testing.expectError(error.UnsupportedRowsQuery, source.rowsJoinPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .join = .{
            .left = .{ .source_cte = "open_rows", .order_by = &.{.{ .field = "status", .direction = .asc }} },
            .right = .{
                .order_by = &.{.{ .field = "status", .direction = .asc }},
                .limit = 1,
            },
            .on = join_on[0..],
            .strategy = .merge,
            .select = join_select[0..],
        },
    }, .read_index));
    try std.testing.expectEqual(scans_before_paged_merge, fake.scan_calls);

    const scans_before_offset_merge = fake.scan_calls;
    try std.testing.expectError(error.UnsupportedRowsQuery, source.rowsJoinPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .join = .{
            .left = .{
                .source_cte = "open_rows",
                .order_by = &.{.{ .field = "status", .direction = .asc }},
                .offset = 1,
            },
            .right = .{ .order_by = &.{.{ .field = "status", .direction = .asc }} },
            .on = join_on[0..],
            .strategy = .merge,
            .select = join_select[0..],
        },
    }, .read_index));
    try std.testing.expectEqual(scans_before_offset_merge, fake.scan_calls);

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

    const count_only_join_scan_calls_before = fake.scan_calls;
    const count_only_join_lookup_calls_before = fake.lookup_calls;
    var count_only_join = (try source.rowsJoinPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .join = .{
            .left = .{ .source_cte = "open_rows", .limit = 0, .total_mode = .exact },
            .right = .{ .limit = 0, .total_mode = .exact },
            .on = join_on[0..],
        },
    }, .read_index)).?;
    defer count_only_join.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 4), count_only_join.total_rows);
    try std.testing.expectEqual(@as(usize, 0), count_only_join.rows.len);
    try std.testing.expectEqual(count_only_join_scan_calls_before + 2, fake.scan_calls);
    try std.testing.expectEqual(count_only_join_lookup_calls_before + 6, fake.lookup_calls);

    const join_same_id_rhs = [_]db_mod.types.RelationalRowsExpression{.{
        .kind = .field,
        .field = "id",
        .field_source = .source,
    }};
    const join_amount_gt_three_rhs = [_]db_mod.types.RelationalRowsExpression{.{
        .kind = .value,
        .value_json = "3",
    }};
    const join_residual_predicates = [_]db_mod.types.RelationalRowsExpressionCondition{
        .{
            .lhs = .{ .kind = .field, .field = "id", .field_source = .row },
            .op = .eq,
            .rhs = join_same_id_rhs[0..],
        },
        .{
            .lhs = .{ .kind = .field, .field = "amount", .field_source = .row },
            .op = .gt,
            .rhs = join_amount_gt_three_rhs[0..],
        },
    };

    const count_only_join_on_residual_scan_calls_before = fake.scan_calls;
    const count_only_join_on_residual_lookup_calls_before = fake.lookup_calls;
    var count_only_join_on_residual = (try source.rowsJoinPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .join = .{
            .left = .{ .source_cte = "open_rows", .limit = 0, .total_mode = .exact },
            .right = .{ .limit = 0, .total_mode = .exact },
            .on = join_on[0..],
            .on_expression_predicates = join_residual_predicates[0..],
        },
    }, .read_index)).?;
    defer count_only_join_on_residual.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), count_only_join_on_residual.total_rows);
    try std.testing.expectEqual(@as(usize, 0), count_only_join_on_residual.rows.len);
    try std.testing.expectEqual(count_only_join_on_residual_scan_calls_before + 2, fake.scan_calls);
    try std.testing.expectEqual(count_only_join_on_residual_lookup_calls_before + 6, fake.lookup_calls);

    const count_only_join_match_residual_scan_calls_before = fake.scan_calls;
    const count_only_join_match_residual_lookup_calls_before = fake.lookup_calls;
    var count_only_join_match_residual = (try source.rowsJoinPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .join = .{
            .left = .{ .source_cte = "open_rows", .limit = 0, .total_mode = .exact },
            .right = .{ .limit = 0, .total_mode = .exact },
            .on = join_on[0..],
            .match_expression_predicates = join_residual_predicates[0..],
        },
    }, .read_index)).?;
    defer count_only_join_match_residual.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), count_only_join_match_residual.total_rows);
    try std.testing.expectEqual(@as(usize, 0), count_only_join_match_residual.rows.len);
    try std.testing.expectEqual(count_only_join_match_residual_scan_calls_before + 2, fake.scan_calls);
    try std.testing.expectEqual(count_only_join_match_residual_lookup_calls_before + 6, fake.lookup_calls);

    const closed_right_predicates = [_]storage_schema.RelationalCheck{.{
        .name = "status_closed",
        .field = "status",
        .value_json = "\"closed\"",
    }};
    const count_only_left_join_scan_calls_before = fake.scan_calls;
    const count_only_left_join_lookup_calls_before = fake.lookup_calls;
    var count_only_left_join = (try source.rowsJoinPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .join = .{
            .left = .{ .source_cte = "open_rows", .limit = 0, .total_mode = .exact },
            .right = .{ .predicates = closed_right_predicates[0..], .limit = 0, .total_mode = .exact },
            .on = join_on[0..],
            .join_type = .left,
        },
    }, .read_index)).?;
    defer count_only_left_join.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 2), count_only_left_join.total_rows);
    try std.testing.expectEqual(@as(usize, 0), count_only_left_join.rows.len);
    try std.testing.expectEqual(count_only_left_join_scan_calls_before + 2, fake.scan_calls);
    try std.testing.expectEqual(count_only_left_join_lookup_calls_before + 6, fake.lookup_calls);

    const count_only_full_join_scan_calls_before = fake.scan_calls;
    const count_only_full_join_lookup_calls_before = fake.lookup_calls;
    var count_only_full_join = (try source.rowsJoinPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .join = .{
            .left = .{ .source_cte = "open_rows", .limit = 0, .total_mode = .exact },
            .right = .{ .predicates = closed_right_predicates[0..], .limit = 0, .total_mode = .exact },
            .on = join_on[0..],
            .join_type = .full,
        },
    }, .read_index)).?;
    defer count_only_full_join.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 3), count_only_full_join.total_rows);
    try std.testing.expectEqual(@as(usize, 0), count_only_full_join.rows.len);
    try std.testing.expectEqual(count_only_full_join_scan_calls_before + 2, fake.scan_calls);
    try std.testing.expectEqual(count_only_full_join_lookup_calls_before + 6, fake.lookup_calls);

    const count_only_full_join_on_residual_scan_calls_before = fake.scan_calls;
    const count_only_full_join_on_residual_lookup_calls_before = fake.lookup_calls;
    var count_only_full_join_on_residual = (try source.rowsJoinPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .join = .{
            .left = .{ .source_cte = "open_rows", .limit = 0, .total_mode = .exact },
            .right = .{ .limit = 0, .total_mode = .exact },
            .on = join_on[0..],
            .join_type = .full,
            .on_expression_predicates = join_residual_predicates[0..],
        },
    }, .read_index)).?;
    defer count_only_full_join_on_residual.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 4), count_only_full_join_on_residual.total_rows);
    try std.testing.expectEqual(@as(usize, 0), count_only_full_join_on_residual.rows.len);
    try std.testing.expectEqual(count_only_full_join_on_residual_scan_calls_before + 2, fake.scan_calls);
    try std.testing.expectEqual(count_only_full_join_on_residual_lookup_calls_before + 6, fake.lookup_calls);

    const count_only_left_join_residual_scan_calls_before = fake.scan_calls;
    const count_only_left_join_residual_lookup_calls_before = fake.lookup_calls;
    var count_only_left_join_residual = (try source.rowsJoinPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .join = .{
            .left = .{ .source_cte = "open_rows", .limit = 0, .total_mode = .exact },
            .right = .{ .limit = 0, .total_mode = .exact },
            .on = join_on[0..],
            .join_type = .left,
            .match_expression_predicates = join_residual_predicates[0..],
        },
    }, .read_index)).?;
    defer count_only_left_join_residual.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), count_only_left_join_residual.total_rows);
    try std.testing.expectEqual(@as(usize, 0), count_only_left_join_residual.rows.len);
    try std.testing.expectEqual(count_only_left_join_residual_scan_calls_before + 2, fake.scan_calls);
    try std.testing.expectEqual(count_only_left_join_residual_lookup_calls_before + 6, fake.lookup_calls);

    const count_only_full_join_match_residual_scan_calls_before = fake.scan_calls;
    const count_only_full_join_match_residual_lookup_calls_before = fake.lookup_calls;
    var count_only_full_join_match_residual = (try source.rowsJoinPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .join = .{
            .left = .{ .source_cte = "open_rows", .limit = 0, .total_mode = .exact },
            .right = .{ .limit = 0, .total_mode = .exact },
            .on = join_on[0..],
            .join_type = .full,
            .match_expression_predicates = join_residual_predicates[0..],
        },
    }, .read_index)).?;
    defer count_only_full_join_match_residual.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), count_only_full_join_match_residual.total_rows);
    try std.testing.expectEqual(@as(usize, 0), count_only_full_join_match_residual.rows.len);
    try std.testing.expectEqual(count_only_full_join_match_residual_scan_calls_before + 2, fake.scan_calls);
    try std.testing.expectEqual(count_only_full_join_match_residual_lookup_calls_before + 6, fake.lookup_calls);

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

    var count_only_lateral = (try source.rowsLateralPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .lateral = .{
            .left = .{ .source_cte = "open_rows" },
            .right = .{ .order_by = &.{.{ .field = "amount", .direction = .desc }}, .limit = 1 },
            .correlations = lateral_correlations[0..],
            .limit = 0,
        },
    }, .read_index)).?;
    defer count_only_lateral.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 2), count_only_lateral.total_rows);
    try std.testing.expectEqual(@as(usize, 0), count_only_lateral.rows.len);

    var count_only_lateral_match_residual = (try source.rowsLateralPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .lateral = .{
            .left = .{ .source_cte = "open_rows" },
            .right = .{ .order_by = &.{.{ .field = "amount", .direction = .desc }}, .limit = 1 },
            .correlations = lateral_correlations[0..],
            .match_expression_predicates = join_residual_predicates[0..],
            .limit = 0,
        },
    }, .read_index)).?;
    defer count_only_lateral_match_residual.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), count_only_lateral_match_residual.total_rows);
    try std.testing.expectEqual(@as(usize, 0), count_only_lateral_match_residual.rows.len);

    var count_only_lateral_ordered_left = (try source.rowsLateralPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .lateral = .{
            .left = .{ .source_cte = "open_rows", .order_by = &.{.{ .field = "amount", .direction = .desc }} },
            .right = .{ .order_by = &.{.{ .field = "amount", .direction = .desc }}, .limit = 1 },
            .correlations = lateral_correlations[0..],
            .limit = 0,
        },
    }, .read_index)).?;
    defer count_only_lateral_ordered_left.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 2), count_only_lateral_ordered_left.total_rows);
    try std.testing.expectEqual(@as(usize, 0), count_only_lateral_ordered_left.rows.len);

    var count_only_lateral_ordered_left_limit_desc = (try source.rowsLateralPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .lateral = .{
            .left = .{ .source_cte = "open_rows", .order_by = &.{.{ .field = "amount", .direction = .desc }}, .limit = 1 },
            .right = .{ .order_by = &.{.{ .field = "amount", .direction = .desc }}, .limit = 1 },
            .correlations = lateral_correlations[0..],
            .match_expression_predicates = join_residual_predicates[0..],
            .limit = 0,
        },
    }, .read_index)).?;
    defer count_only_lateral_ordered_left_limit_desc.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), count_only_lateral_ordered_left_limit_desc.total_rows);
    try std.testing.expectEqual(@as(usize, 0), count_only_lateral_ordered_left_limit_desc.rows.len);

    var count_only_lateral_ordered_left_limit_asc = (try source.rowsLateralPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .lateral = .{
            .left = .{ .source_cte = "open_rows", .order_by = &.{.{ .field = "amount", .direction = .asc }}, .limit = 1 },
            .right = .{ .order_by = &.{.{ .field = "amount", .direction = .desc }}, .limit = 1 },
            .correlations = lateral_correlations[0..],
            .match_expression_predicates = join_residual_predicates[0..],
            .limit = 0,
        },
    }, .read_index)).?;
    defer count_only_lateral_ordered_left_limit_asc.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 0), count_only_lateral_ordered_left_limit_asc.total_rows);
    try std.testing.expectEqual(@as(usize, 0), count_only_lateral_ordered_left_limit_asc.rows.len);

    var count_only_lateral_left_limit_match_residual = (try source.rowsLateralPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .lateral = .{
            .left = .{ .source_cte = "open_rows", .limit = 1 },
            .right = .{ .order_by = &.{.{ .field = "amount", .direction = .desc }}, .limit = 1 },
            .correlations = lateral_correlations[0..],
            .match_expression_predicates = join_residual_predicates[0..],
            .limit = 0,
        },
    }, .read_index)).?;
    defer count_only_lateral_left_limit_match_residual.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 0), count_only_lateral_left_limit_match_residual.total_rows);
    try std.testing.expectEqual(@as(usize, 0), count_only_lateral_left_limit_match_residual.rows.len);

    var count_only_lateral_left_offset_match_residual = (try source.rowsLateralPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .lateral = .{
            .left = .{ .source_cte = "open_rows", .offset = 1, .limit = 1 },
            .right = .{ .order_by = &.{.{ .field = "amount", .direction = .desc }}, .limit = 1 },
            .correlations = lateral_correlations[0..],
            .match_expression_predicates = join_residual_predicates[0..],
            .limit = 0,
        },
    }, .read_index)).?;
    defer count_only_lateral_left_offset_match_residual.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), count_only_lateral_left_offset_match_residual.total_rows);
    try std.testing.expectEqual(@as(usize, 0), count_only_lateral_left_offset_match_residual.rows.len);

    var count_only_lateral_right_cte = (try source.rowsLateralPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .lateral = .{
            .left = .{},
            .right = .{ .source_cte = "open_rows", .order_by = &.{.{ .field = "amount", .direction = .desc }}, .limit = 1 },
            .correlations = lateral_correlations[0..],
            .limit = 0,
        },
    }, .read_index)).?;
    defer count_only_lateral_right_cte.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 3), count_only_lateral_right_cte.total_rows);
    try std.testing.expectEqual(@as(usize, 0), count_only_lateral_right_cte.rows.len);

    var count_only_lateral_right_cte_match_residual = (try source.rowsLateralPlan(alloc, "orders", schema, .{
        .ctes = ctes[0..],
        .lateral = .{
            .left = .{},
            .right = .{ .source_cte = "open_rows", .order_by = &.{.{ .field = "amount", .direction = .desc }}, .limit = 1 },
            .correlations = lateral_correlations[0..],
            .match_expression_predicates = join_residual_predicates[0..],
            .limit = 0,
        },
    }, .read_index)).?;
    defer count_only_lateral_right_cte_match_residual.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), count_only_lateral_right_cte_match_residual.total_rows);
    try std.testing.expectEqual(@as(usize, 0), count_only_lateral_right_cte_match_residual.rows.len);

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

    const oversized_rows = [_][]const u8{
        "{\"id\":\"a\",\"status\":\"open\",\"amount\":1}",
        "{\"id\":\"b\",\"status\":\"open\",\"amount\":2}",
        "{\"id\":\"c\",\"status\":\"open\",\"amount\":3}",
    };
    const oversized_bytes = db_mod.types.relationalRowsCteMaterializedJsonBytes(&oversized_rows) orelse return error.TestUnexpectedResult;
    try std.testing.expectError(error.RelationalRowsCteMaterializationRejected, db_mod.DB.admitRelationalRowsCteMaterialization(.{
        .name = "routed_rows_oversized",
        .max_rows = 2,
        .max_bytes = oversized_bytes,
        .spill_after_bytes = oversized_bytes,
    }, oversized_rows.len, oversized_bytes));
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
            lookup_alloc: std.mem.Allocator,
            table_name: []const u8,
            key: []const u8,
            _: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            const json = if (std.mem.eql(u8, table_name, "orders"))
                if (std.mem.eql(u8, key, "o1"))
                    "{\"id\":\"o1\",\"status\":\"open\",\"customer_id\":\"c1\",\"amount\":10}"
                else if (std.mem.eql(u8, key, "o2"))
                    "{\"id\":\"o2\",\"status\":\"closed\",\"customer_id\":\"c2\",\"amount\":5}"
                else
                    return null
            else if (std.mem.eql(u8, table_name, "customers"))
                if (std.mem.eql(u8, key, "c1"))
                    "{\"id\":\"c1\",\"status\":\"open\",\"name\":\"Ada\"}"
                else if (std.mem.eql(u8, key, "c2"))
                    "{\"id\":\"c2\",\"status\":\"closed\",\"name\":\"Grace\"}"
                else
                    return null
            else
                return null;
            return .{ .json = try lookup_alloc.dupe(u8, json), .version = 1 };
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
                "{\"key\":\"o1\",\"version\":1,\"id\":\"o1\",\"status\":\"open\",\"customer_id\":\"c1\",\"amount\":10}\n{\"key\":\"o2\",\"version\":1,\"id\":\"o2\",\"status\":\"closed\",\"customer_id\":\"c2\",\"amount\":5}\n"
            else if (std.mem.eql(u8, table_name, "customers"))
                "{\"key\":\"c1\",\"version\":1,\"id\":\"c1\",\"status\":\"open\",\"name\":\"Ada\"}\n{\"key\":\"c2\",\"version\":1,\"id\":\"c2\",\"status\":\"closed\",\"name\":\"Grace\"}\n"
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
    var lowered = try sql_adapter.lowerReadPlanWithCatalogAlloc(
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
        const LookupMode = enum { stable, changed, version_changed };

        scan_calls: usize = 0,
        lookup_calls: usize = 0,
        lookup_mode: LookupMode = .stable,

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
            ptr: *anyopaque,
            lookup_alloc: std.mem.Allocator,
            table_name: []const u8,
            key: []const u8,
            _: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!std.mem.eql(u8, table_name, "usage_records")) return error.TableNotFound;
            self.lookup_calls += 1;
            const json = if (std.mem.eql(u8, key, "u1"))
                "{\"id\":\"u1\",\"status\":\"open\",\"enabled\":true}"
            else if (std.mem.eql(u8, key, "u2"))
                if (self.lookup_mode == .changed)
                    "{\"id\":\"u2\",\"status\":\"closed\",\"enabled\":false}"
                else
                    "{\"id\":\"u2\",\"status\":\"open\",\"enabled\":false}"
            else if (std.mem.eql(u8, key, "u3"))
                "{\"id\":\"u3\",\"status\":\"closed\",\"enabled\":true}"
            else
                return null;
            const version: u64 = if (self.lookup_mode == .version_changed and std.mem.eql(u8, key, "u2")) 2 else 1;
            return .{ .json = try lookup_alloc.dupe(u8, json), .version = version };
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
            self.scan_calls += 1;
            const ndjson = if (std.mem.eql(u8, from_key, "") and std.mem.eql(u8, to_key, ""))
                "{\"key\":\"u1\",\"version\":1,\"id\":\"u1\",\"status\":\"open\",\"enabled\":true}\n" ++
                    "{\"key\":\"u2\",\"version\":1,\"id\":\"u2\",\"status\":\"open\",\"enabled\":false}\n" ++
                    "{\"key\":\"u3\",\"version\":1,\"id\":\"u3\",\"status\":\"closed\",\"enabled\":true}\n"
            else if (std.mem.eql(u8, from_key, "") and std.mem.eql(u8, to_key, "u2"))
                "{\"key\":\"u1\",\"version\":1,\"id\":\"u1\",\"status\":\"open\",\"enabled\":true}\n"
            else if (std.mem.eql(u8, from_key, "") and std.mem.eql(u8, to_key, "u3"))
                "{\"key\":\"u1\",\"version\":1,\"id\":\"u1\",\"status\":\"open\",\"enabled\":true}\n" ++
                    "{\"key\":\"u2\",\"version\":1,\"id\":\"u2\",\"status\":\"open\",\"enabled\":false}\n"
            else if (std.mem.eql(u8, from_key, "u2") and std.mem.eql(u8, to_key, "u3"))
                "{\"key\":\"u2\",\"version\":1,\"id\":\"u2\",\"status\":\"open\",\"enabled\":false}\n"
            else if (std.mem.eql(u8, from_key, "u2") and std.mem.eql(u8, to_key, ""))
                "{\"key\":\"u2\",\"version\":1,\"id\":\"u2\",\"status\":\"open\",\"enabled\":false}\n" ++
                    "{\"key\":\"u3\",\"version\":1,\"id\":\"u3\",\"status\":\"closed\",\"enabled\":true}\n"
            else if (std.mem.eql(u8, from_key, "u3") and std.mem.eql(u8, to_key, ""))
                "{\"key\":\"u3\",\"version\":1,\"id\":\"u3\",\"status\":\"closed\",\"enabled\":true}\n"
            else
                return error.UnexpectedRange;
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

    var lowered = try sql_adapter.lowerReadPlanAlloc(
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

    var changed_fake = FakeSource{ .lookup_mode = .changed };
    try std.testing.expectError(error.TopologyChanged, executeLoweredSqlReadPlanAlloc(
        alloc,
        changed_fake.source(),
        catalog.iface(),
        "usage_records",
        schema,
        lowered,
        .read_index,
    ));

    var version_changed_fake = FakeSource{ .lookup_mode = .version_changed };
    try std.testing.expectError(error.TopologyChanged, executeLoweredSqlReadPlanAlloc(
        alloc,
        version_changed_fake.source(),
        catalog.iface(),
        "usage_records",
        schema,
        lowered,
        .read_index,
    ));

    var lowered_distinct = try sql_adapter.lowerReadPlanAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status = 'open' UNION SELECT id FROM usage_records WHERE enabled IS TRUE",
        schema,
        &.{},
    );
    defer lowered_distinct.deinit(alloc);
    switch (lowered_distinct) {
        .set_operation => |set_operation| {
            try std.testing.expectEqual(@as(sql_adapter.SelectSetOperation, .union_distinct), set_operation.operation);
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

    const open_predicates = [_]storage_schema.RelationalCheck{.{
        .name = "status_open",
        .field = "status",
        .value_json = "\"open\"",
    }};
    const enabled_predicates = [_]storage_schema.RelationalCheck{.{
        .name = "enabled_true",
        .field = "enabled",
        .value_json = "true",
    }};
    const projected_id = [_][]const u8{"id"};
    var fake_distinct_count = FakeSource{};
    var distinct_count = (try rowsSetOperationPlanFromRoutedScansAlloc(alloc, fake_distinct_count.source(), "usage_records", schema, .{
        .operation = .union_distinct,
        .left = .{ .query = .{
            .predicates = open_predicates[0..],
            .select = projected_id[0..],
            .select_all = false,
            .limit = 0,
            .total_mode = .exact,
            .profile = true,
        } },
        .right = .{ .query = .{
            .predicates = enabled_predicates[0..],
            .select = projected_id[0..],
            .select_all = false,
            .limit = 0,
            .total_mode = .exact,
            .profile = true,
        } },
    }, .read_index)).?;
    defer distinct_count.deinit(alloc);
    try std.testing.expect(distinct_count.include_profile);
    try std.testing.expect(distinct_count.total_exact);
    try std.testing.expectEqual(@as(u32, 3), distinct_count.total);
    try std.testing.expectEqual(@as(usize, 0), distinct_count.rows.len);
    try std.testing.expect(distinct_count.profile.count_only);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryRequest.TotalMode.exact, distinct_count.profile.total_mode);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryResult.AccessMethod.base_scan, distinct_count.profile.access_method);
    try std.testing.expectEqual(@as(u64, 6), distinct_count.profile.base_scan_rows);
    try std.testing.expectEqual(@as(u64, 6), distinct_count.profile.candidate_rows);
    try std.testing.expectEqual(@as(u64, 6), distinct_count.profile.hydrated_rows);
    try std.testing.expectEqual(@as(u64, 4), distinct_count.profile.projected_rows);
    try std.testing.expectEqual(@as(u64, 2), distinct_count.profile.iterator_seeks);
    try std.testing.expectEqual(@as(u64, 0), distinct_count.profile.retained_candidate_rows);
    try std.testing.expectEqual(@as(u64, 0), distinct_count.profile.routed_materialization_fallbacks);
    try std.testing.expectEqual(@as(usize, 2), fake_distinct_count.scan_calls);
    try std.testing.expectEqual(@as(usize, 6), fake_distinct_count.lookup_calls);

    var lowered_intersect = try sql_adapter.lowerReadPlanAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status = 'open' INTERSECT SELECT id FROM usage_records WHERE enabled IS TRUE",
        schema,
        &.{},
    );
    defer lowered_intersect.deinit(alloc);
    switch (lowered_intersect) {
        .set_operation => |set_operation| {
            try std.testing.expectEqual(@as(sql_adapter.SelectSetOperation, .intersect), set_operation.operation);
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

    var fake_intersect_count = FakeSource{};
    var intersect_count = (try rowsSetOperationPlanFromRoutedScansAlloc(alloc, fake_intersect_count.source(), "usage_records", schema, .{
        .operation = .intersect,
        .left = .{ .query = .{
            .predicates = open_predicates[0..],
            .select = projected_id[0..],
            .select_all = false,
            .limit = 0,
            .total_mode = .exact,
            .profile = true,
        } },
        .right = .{ .query = .{
            .predicates = enabled_predicates[0..],
            .select = projected_id[0..],
            .select_all = false,
            .limit = 0,
            .total_mode = .exact,
            .profile = true,
        } },
    }, .read_index)).?;
    defer intersect_count.deinit(alloc);
    try std.testing.expect(intersect_count.include_profile);
    try std.testing.expect(intersect_count.total_exact);
    try std.testing.expectEqual(@as(u32, 1), intersect_count.total);
    try std.testing.expectEqual(@as(usize, 0), intersect_count.rows.len);
    try std.testing.expect(intersect_count.profile.count_only);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryRequest.TotalMode.exact, intersect_count.profile.total_mode);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryResult.AccessMethod.base_scan, intersect_count.profile.access_method);
    try std.testing.expectEqual(@as(u64, 6), intersect_count.profile.base_scan_rows);
    try std.testing.expectEqual(@as(u64, 6), intersect_count.profile.candidate_rows);
    try std.testing.expectEqual(@as(u64, 6), intersect_count.profile.hydrated_rows);
    try std.testing.expectEqual(@as(u64, 4), intersect_count.profile.projected_rows);
    try std.testing.expectEqual(@as(u64, 2), intersect_count.profile.iterator_seeks);
    try std.testing.expectEqual(@as(u64, 0), intersect_count.profile.retained_candidate_rows);
    try std.testing.expectEqual(@as(u64, 0), intersect_count.profile.routed_materialization_fallbacks);
    try std.testing.expectEqual(@as(usize, 2), fake_intersect_count.scan_calls);
    try std.testing.expectEqual(@as(usize, 6), fake_intersect_count.lookup_calls);

    var fake_except_count = FakeSource{};
    var except_count = (try rowsSetOperationPlanFromRoutedScansAlloc(alloc, fake_except_count.source(), "usage_records", schema, .{
        .operation = .except,
        .left = .{ .query = .{
            .predicates = open_predicates[0..],
            .select = projected_id[0..],
            .select_all = false,
            .limit = 0,
            .total_mode = .exact,
            .profile = true,
        } },
        .right = .{ .query = .{
            .predicates = enabled_predicates[0..],
            .select = projected_id[0..],
            .select_all = false,
            .limit = 0,
            .total_mode = .exact,
            .profile = true,
        } },
    }, .read_index)).?;
    defer except_count.deinit(alloc);
    try std.testing.expect(except_count.include_profile);
    try std.testing.expect(except_count.total_exact);
    try std.testing.expectEqual(@as(u32, 1), except_count.total);
    try std.testing.expectEqual(@as(usize, 0), except_count.rows.len);
    try std.testing.expect(except_count.profile.count_only);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryRequest.TotalMode.exact, except_count.profile.total_mode);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryResult.AccessMethod.base_scan, except_count.profile.access_method);
    try std.testing.expectEqual(@as(u64, 6), except_count.profile.base_scan_rows);
    try std.testing.expectEqual(@as(u64, 6), except_count.profile.candidate_rows);
    try std.testing.expectEqual(@as(u64, 6), except_count.profile.hydrated_rows);
    try std.testing.expectEqual(@as(u64, 4), except_count.profile.projected_rows);
    try std.testing.expectEqual(@as(u64, 2), except_count.profile.iterator_seeks);
    try std.testing.expectEqual(@as(u64, 0), except_count.profile.retained_candidate_rows);
    try std.testing.expectEqual(@as(u64, 0), except_count.profile.routed_materialization_fallbacks);
    try std.testing.expectEqual(@as(usize, 2), fake_except_count.scan_calls);
    try std.testing.expectEqual(@as(usize, 6), fake_except_count.lookup_calls);

    var lowered_tail = try sql_adapter.lowerReadPlanAlloc(
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

    var lowered_cte = try sql_adapter.lowerReadPlanAlloc(
        alloc,
        "WITH open_rows AS (SELECT id, status, enabled FROM usage_records WHERE status = 'open') SELECT id FROM open_rows UNION ALL SELECT id FROM open_rows ORDER BY id DESC LIMIT 3",
        schema,
        &.{},
    );
    defer lowered_cte.deinit(alloc);
    switch (lowered_cte) {
        .set_operation => |set_operation| {
            try std.testing.expectEqual(@as(usize, 1), set_operation.ctes.len);
            try std.testing.expectEqualStrings("open_rows", set_operation.ctes[0].name);
            try std.testing.expectEqualStrings("open_rows", set_operation.left.plan.query.source_cte);
            try std.testing.expectEqualStrings("open_rows", set_operation.right.plan.query.source_cte);
        },
        else => return error.TestUnexpectedResult,
    }

    var fake_cte = FakeSource{};
    var cte_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        fake_cte.source(),
        catalog.iface(),
        "usage_records",
        schema,
        lowered_cte,
        .read_index,
    )).?;
    defer cte_result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), fake_cte.scan_calls);
    switch (cte_result) {
        .set_operation => |query_result| {
            try std.testing.expectEqual(@as(u32, 4), query_result.total);
            try std.testing.expectEqual(@as(usize, 3), query_result.rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"u2\"}", query_result.rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"u2\"}", query_result.rows[1]);
            try std.testing.expectEqualStrings("{\"id\":\"u1\"}", query_result.rows[2]);
        },
        else => return error.TestUnexpectedResult,
    }

    const cte_select = [_][]const u8{ "id", "status", "enabled" };
    const ranged_ctes = [_]db_mod.types.RelationalRowsCte{.{
        .name = "enabled_rows",
        .query = .{
            .predicates = enabled_predicates[0..],
            .select = cte_select[0..],
            .select_all = false,
        },
    }};
    const left_cte_ranges = [_]db_mod.types.RelationalRowsDocKeyRange{.{
        .start = "",
        .end = "u2",
    }};
    const right_cte_ranges = [_]db_mod.types.RelationalRowsDocKeyRange{.{
        .start = "u3",
        .end = "",
    }};
    const id_select = [_][]const u8{"id"};
    const id_desc = [_]db_mod.types.RelationalRowsQueryOrder{.{
        .field = "id",
        .direction = .desc,
    }};
    var fake_ranged_cte = FakeSource{};
    var ranged_cte_result = (try rowsSetOperationPlanFromRoutedScansAlloc(alloc, fake_ranged_cte.source(), "usage_records", schema, .{
        .operation = .union_all,
        .ctes = ranged_ctes[0..],
        .left = .{
            .ranges = left_cte_ranges[0..],
            .query = .{
                .source_cte = "enabled_rows",
                .select = id_select[0..],
                .select_all = false,
            },
        },
        .right = .{
            .ranges = right_cte_ranges[0..],
            .query = .{
                .source_cte = "enabled_rows",
                .select = id_select[0..],
                .select_all = false,
            },
        },
        .order_by = id_desc[0..],
        .limit = 3,
    }, .read_index)).?;
    defer ranged_cte_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), fake_ranged_cte.scan_calls);
    try std.testing.expectEqual(@as(u32, 4), ranged_cte_result.total);
    try std.testing.expectEqual(@as(usize, 3), ranged_cte_result.rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"u3\"}", ranged_cte_result.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"u3\"}", ranged_cte_result.rows[1]);
    try std.testing.expectEqualStrings("{\"id\":\"u1\"}", ranged_cte_result.rows[2]);

    const count_only_scan_calls_before = fake_ranged_cte.scan_calls;
    const count_only_lookup_calls_before = fake_ranged_cte.lookup_calls;
    var ranged_cte_count_only = (try rowsSetOperationPlanFromRoutedScansAlloc(alloc, fake_ranged_cte.source(), "usage_records", schema, .{
        .operation = .union_all,
        .ctes = ranged_ctes[0..],
        .left = .{
            .ranges = left_cte_ranges[0..],
            .query = .{
                .source_cte = "enabled_rows",
                .limit = 0,
                .total_mode = .exact,
                .profile = true,
            },
        },
        .right = .{
            .ranges = right_cte_ranges[0..],
            .query = .{
                .source_cte = "enabled_rows",
                .limit = 0,
                .total_mode = .exact,
                .profile = true,
            },
        },
    }, .read_index)).?;
    defer ranged_cte_count_only.deinit(alloc);
    try std.testing.expect(ranged_cte_count_only.include_profile);
    try std.testing.expect(ranged_cte_count_only.total_exact);
    try std.testing.expectEqual(@as(u32, 2), ranged_cte_count_only.total);
    try std.testing.expectEqual(@as(usize, 0), ranged_cte_count_only.rows.len);
    try std.testing.expect(ranged_cte_count_only.profile.count_only);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryRequest.TotalMode.exact, ranged_cte_count_only.profile.total_mode);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryResult.AccessMethod.base_scan, ranged_cte_count_only.profile.access_method);
    try std.testing.expectEqual(@as(u64, 2), ranged_cte_count_only.profile.base_scan_rows);
    try std.testing.expectEqual(@as(u64, 2), ranged_cte_count_only.profile.candidate_rows);
    try std.testing.expectEqual(@as(u64, 2), ranged_cte_count_only.profile.iterator_seeks);
    try std.testing.expectEqual(@as(u64, 0), ranged_cte_count_only.profile.retained_candidate_rows);
    try std.testing.expectEqual(@as(u64, 0), ranged_cte_count_only.profile.projected_rows);
    try std.testing.expectEqual(@as(u64, 0), ranged_cte_count_only.profile.routed_materialization_fallbacks);
    try std.testing.expectEqual(count_only_scan_calls_before + 2, fake_ranged_cte.scan_calls);
    try std.testing.expectEqual(count_only_lookup_calls_before + 2, fake_ranged_cte.lookup_calls);

    const distinct_cte_scan_calls_before = fake_ranged_cte.scan_calls;
    const distinct_cte_lookup_calls_before = fake_ranged_cte.lookup_calls;
    var distinct_cte_count_only = (try rowsSetOperationPlanFromRoutedScansAlloc(alloc, fake_ranged_cte.source(), "usage_records", schema, .{
        .operation = .union_distinct,
        .ctes = ranged_ctes[0..],
        .left = .{ .query = .{
            .source_cte = "enabled_rows",
            .select = id_select[0..],
            .select_all = false,
            .limit = 0,
            .total_mode = .exact,
            .profile = true,
        } },
        .right = .{ .query = .{
            .source_cte = "enabled_rows",
            .select = id_select[0..],
            .select_all = false,
            .limit = 0,
            .total_mode = .exact,
            .profile = true,
        } },
    }, .read_index)).?;
    defer distinct_cte_count_only.deinit(alloc);
    try std.testing.expect(distinct_cte_count_only.include_profile);
    try std.testing.expect(distinct_cte_count_only.total_exact);
    try std.testing.expectEqual(@as(u32, 2), distinct_cte_count_only.total);
    try std.testing.expectEqual(@as(usize, 0), distinct_cte_count_only.rows.len);
    try std.testing.expect(distinct_cte_count_only.profile.count_only);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryRequest.TotalMode.exact, distinct_cte_count_only.profile.total_mode);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryResult.AccessMethod.base_scan, distinct_cte_count_only.profile.access_method);
    try std.testing.expectEqual(@as(u64, 6), distinct_cte_count_only.profile.base_scan_rows);
    try std.testing.expectEqual(@as(u64, 6), distinct_cte_count_only.profile.candidate_rows);
    try std.testing.expectEqual(@as(u64, 6), distinct_cte_count_only.profile.hydrated_rows);
    try std.testing.expectEqual(@as(u64, 4), distinct_cte_count_only.profile.projected_rows);
    try std.testing.expectEqual(@as(u64, 2), distinct_cte_count_only.profile.iterator_seeks);
    try std.testing.expectEqual(@as(u64, 0), distinct_cte_count_only.profile.retained_candidate_rows);
    try std.testing.expectEqual(@as(u64, 0), distinct_cte_count_only.profile.routed_materialization_fallbacks);
    try std.testing.expectEqual(distinct_cte_scan_calls_before + 2, fake_ranged_cte.scan_calls);
    try std.testing.expectEqual(distinct_cte_lookup_calls_before + 6, fake_ranged_cte.lookup_calls);

    const intersect_cte_scan_calls_before = fake_ranged_cte.scan_calls;
    const intersect_cte_lookup_calls_before = fake_ranged_cte.lookup_calls;
    var intersect_cte_count_only = (try rowsSetOperationPlanFromRoutedScansAlloc(alloc, fake_ranged_cte.source(), "usage_records", schema, .{
        .operation = .intersect,
        .ctes = ranged_ctes[0..],
        .left = .{ .query = .{
            .source_cte = "enabled_rows",
            .select = id_select[0..],
            .select_all = false,
            .limit = 0,
            .total_mode = .exact,
            .profile = true,
        } },
        .right = .{
            .ranges = right_cte_ranges[0..],
            .query = .{
                .source_cte = "enabled_rows",
                .select = id_select[0..],
                .select_all = false,
                .limit = 0,
                .total_mode = .exact,
                .profile = true,
            },
        },
    }, .read_index)).?;
    defer intersect_cte_count_only.deinit(alloc);
    try std.testing.expect(intersect_cte_count_only.include_profile);
    try std.testing.expect(intersect_cte_count_only.total_exact);
    try std.testing.expectEqual(@as(u32, 1), intersect_cte_count_only.total);
    try std.testing.expectEqual(@as(usize, 0), intersect_cte_count_only.rows.len);
    try std.testing.expect(intersect_cte_count_only.profile.count_only);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryRequest.TotalMode.exact, intersect_cte_count_only.profile.total_mode);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryResult.AccessMethod.base_scan, intersect_cte_count_only.profile.access_method);
    try std.testing.expectEqual(@as(u64, 4), intersect_cte_count_only.profile.base_scan_rows);
    try std.testing.expectEqual(@as(u64, 4), intersect_cte_count_only.profile.candidate_rows);
    try std.testing.expectEqual(@as(u64, 4), intersect_cte_count_only.profile.hydrated_rows);
    try std.testing.expectEqual(@as(u64, 3), intersect_cte_count_only.profile.projected_rows);
    try std.testing.expectEqual(@as(u64, 2), intersect_cte_count_only.profile.iterator_seeks);
    try std.testing.expectEqual(@as(u64, 0), intersect_cte_count_only.profile.retained_candidate_rows);
    try std.testing.expectEqual(@as(u64, 0), intersect_cte_count_only.profile.routed_materialization_fallbacks);
    try std.testing.expectEqual(intersect_cte_scan_calls_before + 2, fake_ranged_cte.scan_calls);
    try std.testing.expectEqual(intersect_cte_lookup_calls_before + 4, fake_ranged_cte.lookup_calls);

    const except_cte_scan_calls_before = fake_ranged_cte.scan_calls;
    const except_cte_lookup_calls_before = fake_ranged_cte.lookup_calls;
    var except_cte_count_only = (try rowsSetOperationPlanFromRoutedScansAlloc(alloc, fake_ranged_cte.source(), "usage_records", schema, .{
        .operation = .except,
        .ctes = ranged_ctes[0..],
        .left = .{ .query = .{
            .source_cte = "enabled_rows",
            .select = id_select[0..],
            .select_all = false,
            .limit = 0,
            .total_mode = .exact,
            .profile = true,
        } },
        .right = .{
            .ranges = right_cte_ranges[0..],
            .query = .{
                .source_cte = "enabled_rows",
                .select = id_select[0..],
                .select_all = false,
                .limit = 0,
                .total_mode = .exact,
                .profile = true,
            },
        },
    }, .read_index)).?;
    defer except_cte_count_only.deinit(alloc);
    try std.testing.expect(except_cte_count_only.include_profile);
    try std.testing.expect(except_cte_count_only.total_exact);
    try std.testing.expectEqual(@as(u32, 1), except_cte_count_only.total);
    try std.testing.expectEqual(@as(usize, 0), except_cte_count_only.rows.len);
    try std.testing.expect(except_cte_count_only.profile.count_only);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryRequest.TotalMode.exact, except_cte_count_only.profile.total_mode);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryResult.AccessMethod.base_scan, except_cte_count_only.profile.access_method);
    try std.testing.expectEqual(@as(u64, 4), except_cte_count_only.profile.base_scan_rows);
    try std.testing.expectEqual(@as(u64, 4), except_cte_count_only.profile.candidate_rows);
    try std.testing.expectEqual(@as(u64, 4), except_cte_count_only.profile.hydrated_rows);
    try std.testing.expectEqual(@as(u64, 3), except_cte_count_only.profile.projected_rows);
    try std.testing.expectEqual(@as(u64, 2), except_cte_count_only.profile.iterator_seeks);
    try std.testing.expectEqual(@as(u64, 0), except_cte_count_only.profile.retained_candidate_rows);
    try std.testing.expectEqual(@as(u64, 0), except_cte_count_only.profile.routed_materialization_fallbacks);
    try std.testing.expectEqual(except_cte_scan_calls_before + 2, fake_ranged_cte.scan_calls);
    try std.testing.expectEqual(except_cte_lookup_calls_before + 4, fake_ranged_cte.lookup_calls);
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
        usage_as_of_sequence_queries: usize = 0,
        archived_as_of_sequence_queries: usize = 0,
        usage_as_of_timestamp_queries: usize = 0,
        archived_as_of_timestamp_queries: usize = 0,
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
                    .rows_query_plan_catalog_system_time_as_of_sequence = rowsQueryPlanCatalogSystemTimeAsOfSequence,
                    .rows_query_plan_catalog_system_time_as_of_timestamp_ns = rowsQueryPlanCatalogSystemTimeAsOfTimestampNs,
                },
            };
        }

        fn lookup(
            _: *anyopaque,
            lookup_alloc: std.mem.Allocator,
            table_name: []const u8,
            key: []const u8,
            _: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            const json = if (std.mem.eql(u8, table_name, "usage_records"))
                if (std.mem.eql(u8, key, "u1"))
                    "{\"id\":\"u1\",\"status\":\"open\",\"enabled\":true}"
                else if (std.mem.eql(u8, key, "u2"))
                    "{\"id\":\"u2\",\"status\":\"open\",\"enabled\":false}"
                else if (std.mem.eql(u8, key, "u3"))
                    "{\"id\":\"u3\",\"status\":\"closed\",\"enabled\":true}"
                else
                    return null
            else if (std.mem.eql(u8, table_name, "archived_records"))
                if (std.mem.eql(u8, key, "a1"))
                    "{\"id\":\"a1\",\"status\":\"archived\",\"enabled\":true}"
                else if (std.mem.eql(u8, key, "a2"))
                    "{\"id\":\"a2\",\"status\":\"archived\",\"enabled\":false}"
                else if (std.mem.eql(u8, key, "a3"))
                    "{\"id\":\"a3\",\"status\":\"deleted\",\"enabled\":true}"
                else if (std.mem.eql(u8, key, "u2"))
                    "{\"id\":\"u2\",\"status\":\"deleted\",\"enabled\":false}"
                else
                    return null
            else
                return error.TableNotFound;
            return .{ .json = try lookup_alloc.dupe(u8, json), .version = 1 };
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
            if (std.mem.eql(u8, table_name, "usage_records")) {
                self.usage_scans += 1;
                const ndjson = if (std.mem.eql(u8, from_key, "") and std.mem.eql(u8, to_key, ""))
                    "{\"key\":\"u1\",\"version\":1,\"id\":\"u1\",\"status\":\"open\",\"enabled\":true}\n" ++
                        "{\"key\":\"u2\",\"version\":1,\"id\":\"u2\",\"status\":\"open\",\"enabled\":false}\n" ++
                        "{\"key\":\"u3\",\"version\":1,\"id\":\"u3\",\"status\":\"closed\",\"enabled\":true}\n"
                else if (std.mem.eql(u8, from_key, "") and std.mem.eql(u8, to_key, "u2"))
                    "{\"key\":\"u1\",\"version\":1,\"id\":\"u1\",\"status\":\"open\",\"enabled\":true}\n"
                else if (std.mem.eql(u8, from_key, "u3") and std.mem.eql(u8, to_key, ""))
                    "{\"key\":\"u3\",\"version\":1,\"id\":\"u3\",\"status\":\"closed\",\"enabled\":true}\n"
                else
                    return error.UnexpectedRange;
                return .{ .ndjson = try scan_alloc.dupe(u8, ndjson) };
            }
            try std.testing.expectEqualStrings("", from_key);
            try std.testing.expectEqualStrings("", to_key);
            if (std.mem.eql(u8, table_name, "archived_records")) {
                self.archived_scans += 1;
                const ndjson =
                    "{\"key\":\"a1\",\"version\":1,\"id\":\"a1\",\"status\":\"archived\",\"enabled\":true}\n" ++
                    "{\"key\":\"a2\",\"version\":1,\"id\":\"a2\",\"status\":\"archived\",\"enabled\":false}\n" ++
                    "{\"key\":\"a3\",\"version\":1,\"id\":\"a3\",\"status\":\"deleted\",\"enabled\":true}\n" ++
                    "{\"key\":\"u2\",\"version\":1,\"id\":\"u2\",\"status\":\"deleted\",\"enabled\":false}\n";
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

        fn rowsQueryPlanCatalogSystemTimeAsOfSequence(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            target: catalog_resources.TableTarget,
            runtime_schema: storage_schema.TableSchema,
            commit_sequence: u64,
            plan: db_mod.types.RelationalRowsQueryPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsQueryResult {
            try std.testing.expectEqual(@as(u64, 42), commit_sequence);
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings(self.expected_database_name, target.database_name);
            try std.testing.expectEqualStrings(self.expected_namespace_name, target.namespace_name);
            if (std.mem.eql(u8, target.table_name, "usage_records")) {
                self.usage_as_of_sequence_queries += 1;
            } else if (std.mem.eql(u8, target.table_name, "archived_records")) {
                self.archived_as_of_sequence_queries += 1;
            } else {
                return error.TableNotFound;
            }
            return try rowsQueryPlanFromRoutedScansAlloc(plan_alloc, self.source(), target.table_name, runtime_schema, plan, consistency);
        }

        fn rowsQueryPlanCatalogSystemTimeAsOfTimestampNs(
            ptr: *anyopaque,
            plan_alloc: std.mem.Allocator,
            target: catalog_resources.TableTarget,
            runtime_schema: storage_schema.TableSchema,
            timestamp_ns: u64,
            plan: db_mod.types.RelationalRowsQueryPlan,
            consistency: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsQueryResult {
            try std.testing.expectEqual(@as(u64, 55_000), timestamp_ns);
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings(self.expected_database_name, target.database_name);
            try std.testing.expectEqualStrings(self.expected_namespace_name, target.namespace_name);
            if (std.mem.eql(u8, target.table_name, "usage_records")) {
                self.usage_as_of_timestamp_queries += 1;
            } else if (std.mem.eql(u8, target.table_name, "archived_records")) {
                self.archived_as_of_timestamp_queries += 1;
            } else {
                return error.TableNotFound;
            }
            return try rowsQueryPlanFromRoutedScansAlloc(plan_alloc, self.source(), target.table_name, runtime_schema, plan, consistency);
        }
    };

    var catalog = FakeCatalog{};
    var lowered = try sql_adapter.lowerReadPlanWithCatalogAlloc(
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

    var lowered_cte = try sql_adapter.lowerReadPlanWithCatalogAlloc(
        alloc,
        "WITH open_usage AS (SELECT id, status, enabled FROM usage_records WHERE status = 'open') SELECT id FROM open_usage UNION ALL SELECT id FROM archived_records WHERE enabled IS TRUE",
        usage_schema,
        &.{},
        catalog.iface(),
    );
    defer lowered_cte.deinit(alloc);
    switch (lowered_cte) {
        .set_operation => |set_operation| {
            try std.testing.expectEqual(@as(usize, 1), set_operation.ctes.len);
            try std.testing.expectEqualStrings("open_usage", set_operation.ctes[0].name);
            try std.testing.expectEqualStrings("usage_records", set_operation.left.table_name);
            try std.testing.expectEqualStrings("open_usage", set_operation.left.plan.query.source_cte);
            try std.testing.expectEqualStrings("archived_records", set_operation.right.table_name);
            try std.testing.expectEqualStrings("", set_operation.right.plan.query.source_cte);
        },
        else => return error.TestUnexpectedResult,
    }

    var cte_fake = FakeSource{};
    var cte_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        cte_fake.source(),
        catalog.iface(),
        "usage_records",
        usage_schema,
        lowered_cte,
        .read_index,
    )).?;
    defer cte_result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), cte_fake.usage_scans);
    try std.testing.expectEqual(@as(usize, 1), cte_fake.archived_scans);
    try std.testing.expectEqual(@as(usize, 1), cte_fake.usage_catalog_queries);
    try std.testing.expectEqual(@as(usize, 1), cte_fake.archived_catalog_queries);
    switch (cte_result) {
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

    var lowered_as_of = try sql_adapter.lowerReadPlanWithCatalogAlloc(
        alloc,
        "SELECT id FROM usage_records FOR SYSTEM_TIME AS OF 42 WHERE status = 'open' UNION ALL SELECT id FROM archived_records FOR SYSTEM_TIME AS OF 42 WHERE enabled IS TRUE",
        usage_schema,
        &.{},
        catalog.iface(),
    );
    defer lowered_as_of.deinit(alloc);
    switch (lowered_as_of) {
        .set_operation => |set_operation| {
            try std.testing.expectEqual(@as(u64, 42), set_operation.left.system_time_as_of_sequence.?);
            try std.testing.expectEqual(@as(u64, 42), set_operation.right.system_time_as_of_sequence.?);
            try std.testing.expect(set_operation.left.system_time_as_of_timestamp_ns == null);
            try std.testing.expect(set_operation.right.system_time_as_of_timestamp_ns == null);
        },
        else => return error.TestUnexpectedResult,
    }

    var as_of_fake = FakeSource{};
    var as_of_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        as_of_fake.source(),
        catalog.iface(),
        "usage_records",
        usage_schema,
        lowered_as_of,
        .read_index,
    )).?;
    defer as_of_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), as_of_fake.usage_as_of_sequence_queries);
    try std.testing.expectEqual(@as(usize, 1), as_of_fake.archived_as_of_sequence_queries);
    try std.testing.expectEqual(@as(usize, 0), as_of_fake.usage_catalog_queries);
    try std.testing.expectEqual(@as(usize, 0), as_of_fake.archived_catalog_queries);
    switch (as_of_result) {
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

    var lowered_cte_as_of = try sql_adapter.lowerReadPlanWithCatalogAlloc(
        alloc,
        "WITH open_usage AS (SELECT id, status, enabled FROM usage_records WHERE status = 'open') SELECT id FROM open_usage FOR SYSTEM_TIME AS OF 42 UNION ALL SELECT id FROM archived_records FOR SYSTEM_TIME AS OF 42 WHERE enabled IS TRUE",
        usage_schema,
        &.{},
        catalog.iface(),
    );
    defer lowered_cte_as_of.deinit(alloc);
    switch (lowered_cte_as_of) {
        .set_operation => |set_operation| {
            try std.testing.expectEqual(@as(usize, 1), set_operation.ctes.len);
            try std.testing.expectEqualStrings("open_usage", set_operation.ctes[0].name);
            try std.testing.expectEqualStrings("usage_records", set_operation.left.table_name);
            try std.testing.expectEqualStrings("open_usage", set_operation.left.plan.query.source_cte);
            try std.testing.expectEqualStrings("archived_records", set_operation.right.table_name);
            try std.testing.expectEqualStrings("", set_operation.right.plan.query.source_cte);
            try std.testing.expectEqual(@as(u64, 42), set_operation.left.system_time_as_of_sequence.?);
            try std.testing.expectEqual(@as(u64, 42), set_operation.right.system_time_as_of_sequence.?);
        },
        else => return error.TestUnexpectedResult,
    }

    var cte_as_of_fake = FakeSource{};
    var cte_as_of_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        cte_as_of_fake.source(),
        catalog.iface(),
        "usage_records",
        usage_schema,
        lowered_cte_as_of,
        .read_index,
    )).?;
    defer cte_as_of_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), cte_as_of_fake.usage_as_of_sequence_queries);
    try std.testing.expectEqual(@as(usize, 1), cte_as_of_fake.archived_as_of_sequence_queries);
    try std.testing.expectEqual(@as(usize, 0), cte_as_of_fake.usage_catalog_queries);
    try std.testing.expectEqual(@as(usize, 0), cte_as_of_fake.archived_catalog_queries);
    switch (cte_as_of_result) {
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

    const TestRanges = struct {
        fn single(range_alloc: std.mem.Allocator, start: []const u8, end: []const u8) ![]const db_mod.types.RelationalRowsDocKeyRange {
            const ranges = try range_alloc.alloc(db_mod.types.RelationalRowsDocKeyRange, 1);
            errdefer range_alloc.free(ranges);
            ranges[0] = .{};
            if (start.len != 0) {
                ranges[0].start = try range_alloc.dupe(u8, start);
            }
            errdefer if (ranges[0].start.len != 0) range_alloc.free(@constCast(ranges[0].start));
            if (end.len != 0) {
                ranges[0].end = try range_alloc.dupe(u8, end);
            }
            return ranges;
        }

        fn free(range_alloc: std.mem.Allocator, ranges: []const db_mod.types.RelationalRowsDocKeyRange) void {
            for (ranges) |range| {
                if (range.start.len != 0) range_alloc.free(@constCast(range.start));
                if (range.end.len != 0) range_alloc.free(@constCast(range.end));
            }
            if (ranges.len != 0) range_alloc.free(ranges);
        }
    };

    var lowered_same_table_as_of = try sql_adapter.lowerReadPlanWithCatalogAlloc(
        alloc,
        "SELECT id FROM usage_records FOR SYSTEM_TIME AS OF 42 WHERE status = 'open' UNION ALL SELECT id FROM usage_records FOR SYSTEM_TIME AS OF 42 WHERE enabled IS TRUE",
        usage_schema,
        &.{},
        catalog.iface(),
    );
    defer lowered_same_table_as_of.deinit(alloc);
    var same_left_ranges = try TestRanges.single(alloc, "", "u2");
    errdefer TestRanges.free(alloc, same_left_ranges);
    var same_right_ranges = try TestRanges.single(alloc, "u3", "");
    errdefer TestRanges.free(alloc, same_right_ranges);
    switch (lowered_same_table_as_of) {
        .set_operation => {},
        else => return error.TestUnexpectedResult,
    }
    lowered_same_table_as_of.set_operation.left.plan.ranges = same_left_ranges;
    same_left_ranges = &.{};
    lowered_same_table_as_of.set_operation.right.plan.ranges = same_right_ranges;
    same_right_ranges = &.{};

    var same_as_of_fake = FakeSource{};
    var same_as_of_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        same_as_of_fake.source(),
        catalog.iface(),
        "usage_records",
        usage_schema,
        lowered_same_table_as_of,
        .read_index,
    )).?;
    defer same_as_of_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), same_as_of_fake.usage_as_of_sequence_queries);
    try std.testing.expectEqual(@as(usize, 2), same_as_of_fake.usage_scans);
    switch (same_as_of_result) {
        .set_operation => |query_result| {
            try std.testing.expectEqual(@as(u32, 2), query_result.total);
            try std.testing.expectEqual(@as(usize, 2), query_result.rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"u1\"}", query_result.rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"u3\"}", query_result.rows[1]);
        },
        else => return error.TestUnexpectedResult,
    }

    var lowered_as_of_timestamp = try sql_adapter.lowerReadPlanWithCatalogAlloc(
        alloc,
        "SELECT id FROM usage_records FOR SYSTEM_TIME AS OF TIMESTAMP '1970-01-01T00:00:00.000055Z' WHERE status = 'open' UNION ALL SELECT id FROM archived_records FOR SYSTEM_TIME AS OF TIMESTAMP '1970-01-01T00:00:00.000055Z' WHERE enabled IS TRUE",
        usage_schema,
        &.{},
        catalog.iface(),
    );
    defer lowered_as_of_timestamp.deinit(alloc);
    switch (lowered_as_of_timestamp) {
        .set_operation => |set_operation| {
            try std.testing.expect(set_operation.left.system_time_as_of_sequence == null);
            try std.testing.expect(set_operation.right.system_time_as_of_sequence == null);
            try std.testing.expectEqual(@as(u64, 55_000), set_operation.left.system_time_as_of_timestamp_ns.?);
            try std.testing.expectEqual(@as(u64, 55_000), set_operation.right.system_time_as_of_timestamp_ns.?);
        },
        else => return error.TestUnexpectedResult,
    }

    var as_of_timestamp_fake = FakeSource{};
    var as_of_timestamp_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        as_of_timestamp_fake.source(),
        catalog.iface(),
        "usage_records",
        usage_schema,
        lowered_as_of_timestamp,
        .read_index,
    )).?;
    defer as_of_timestamp_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), as_of_timestamp_fake.usage_as_of_timestamp_queries);
    try std.testing.expectEqual(@as(usize, 1), as_of_timestamp_fake.archived_as_of_timestamp_queries);
    try std.testing.expectEqual(@as(usize, 0), as_of_timestamp_fake.usage_catalog_queries);
    try std.testing.expectEqual(@as(usize, 0), as_of_timestamp_fake.archived_catalog_queries);
    switch (as_of_timestamp_result) {
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

    try std.testing.expectError(error.UnsupportedSqlShape, sql_adapter.lowerReadPlanWithCatalogAlloc(
        alloc,
        "SELECT id FROM usage_records FOR SYSTEM_TIME AS OF 42 WHERE status = 'open' UNION ALL SELECT id FROM archived_records FOR SYSTEM_TIME AS OF TIMESTAMP '1970-01-01T00:00:00.000055Z' WHERE enabled IS TRUE",
        usage_schema,
        &.{},
        catalog.iface(),
    ));
    try std.testing.expectError(error.UnsupportedSqlShape, sql_adapter.lowerReadPlanWithCatalogAlloc(
        alloc,
        "SELECT id FROM usage_records FOR SYSTEM_TIME AS OF TIMESTAMP '1970-01-01T00:00:00.000055Z' WHERE status = 'open' UNION ALL SELECT id FROM archived_records FOR SYSTEM_TIME AS OF TIMESTAMP '1970-01-01T00:00:00.000056Z' WHERE enabled IS TRUE",
        usage_schema,
        &.{},
        catalog.iface(),
    ));

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

    var lowered_except = try sql_adapter.lowerReadPlanWithCatalogAlloc(
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

    var lowered_intersect = try sql_adapter.lowerReadPlanWithCatalogAlloc(
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

test "lowered sql set operation RHS multi-join CTEs route incompatible side schemas" {
    const alloc = std.testing.allocator;
    const usage_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const archived_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"tenant_ref":{"type":"keyword"},"archived":{"type":"boolean"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const tenant_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant_ref":{"type":"keyword"},"active":{"type":"boolean"}},"required":["tenant_ref"],"additionalProperties":false}}},"primary_key":{"columns":["tenant_ref"]}}
    ;

    var parsed_usage = try schema_api.parseValidatedTableSchema(alloc, usage_schema_json);
    defer parsed_usage.deinit(alloc);
    const usage_schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_usage);
    defer storage_schema.freeSchema(alloc, usage_schema);

    const FakeCatalog = struct {
        tables: [3]metadata_table_manager.TableRecord = .{
            .{ .table_id = 41, .name = "usage_records", .schema_json = usage_schema_json, .placement_role = "data" },
            .{ .table_id = 42, .name = "archived_records", .schema_json = archived_schema_json, .placement_role = "data" },
            .{ .table_id = 43, .name = "tenant_records", .schema_json = tenant_schema_json, .placement_role = "data" },
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
        tenant_scans: usize = 0,

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
            lookup_alloc: std.mem.Allocator,
            table_name: []const u8,
            key: []const u8,
            _: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) !?LookupResponse {
            const json = if (std.mem.eql(u8, table_name, "usage_records"))
                if (std.mem.eql(u8, key, "u1"))
                    "{\"id\":\"u1\",\"status\":\"open\"}"
                else if (std.mem.eql(u8, key, "u2"))
                    "{\"id\":\"u2\",\"status\":\"closed\"}"
                else if (std.mem.eql(u8, key, "u3"))
                    "{\"id\":\"u3\",\"status\":\"open\"}"
                else
                    return null
            else if (std.mem.eql(u8, table_name, "archived_records"))
                if (std.mem.eql(u8, key, "u2"))
                    "{\"id\":\"u2\",\"tenant_ref\":\"t1\",\"archived\":true}"
                else if (std.mem.eql(u8, key, "u4"))
                    "{\"id\":\"u4\",\"tenant_ref\":\"t2\",\"archived\":true}"
                else
                    return null
            else if (std.mem.eql(u8, table_name, "tenant_records"))
                if (std.mem.eql(u8, key, "t1"))
                    "{\"tenant_ref\":\"t1\",\"active\":true}"
                else if (std.mem.eql(u8, key, "t2"))
                    "{\"tenant_ref\":\"t2\",\"active\":false}"
                else
                    return null
            else
                return error.TableNotFound;
            return .{ .json = try lookup_alloc.dupe(u8, json), .version = 1 };
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
                return .{ .ndjson = try scan_alloc.dupe(
                    u8,
                    "{\"key\":\"u1\",\"version\":1,\"id\":\"u1\",\"status\":\"open\"}\n" ++
                        "{\"key\":\"u2\",\"version\":1,\"id\":\"u2\",\"status\":\"closed\"}\n" ++
                        "{\"key\":\"u3\",\"version\":1,\"id\":\"u3\",\"status\":\"open\"}\n",
                ) };
            }
            if (std.mem.eql(u8, table_name, "archived_records")) {
                self.archived_scans += 1;
                return .{ .ndjson = try scan_alloc.dupe(
                    u8,
                    "{\"key\":\"u2\",\"version\":1,\"id\":\"u2\",\"tenant_ref\":\"t1\",\"archived\":true}\n" ++
                        "{\"key\":\"u4\",\"version\":1,\"id\":\"u4\",\"tenant_ref\":\"t2\",\"archived\":true}\n",
                ) };
            }
            if (std.mem.eql(u8, table_name, "tenant_records")) {
                self.tenant_scans += 1;
                return .{ .ndjson = try scan_alloc.dupe(
                    u8,
                    "{\"key\":\"t1\",\"version\":1,\"tenant_ref\":\"t1\",\"active\":true}\n" ++
                        "{\"key\":\"t2\",\"version\":1,\"tenant_ref\":\"t2\",\"active\":false}\n",
                ) };
            }
            return error.TableNotFound;
        }
    };

    const left_predicates = [_]storage_schema.RelationalCheck{.{
        .name = "usage_records_status_eq",
        .field = "status",
        .op = .eq,
        .value_json = "\"open\"",
    }};
    const left_select = [_][]const u8{"id"};
    const cte0_on = [_]db_mod.types.RelationalRowsJoinOn{.{ .left_field = "id", .right_field = "id" }};
    const cte0_select = [_]db_mod.types.RelationalRowsJoinProjection{.{
        .output = "a_tenant_ref",
        .side = .right,
        .field = "tenant_ref",
    }};
    const cte1_on = [_]db_mod.types.RelationalRowsJoinOn{.{ .left_field = "a_tenant_ref", .right_field = "tenant_ref" }};
    const cte1_select = [_]db_mod.types.RelationalRowsJoinProjection{.{
        .output = "id",
        .side = .right,
        .field = "tenant_ref",
    }};
    const ctes = [_]db_mod.types.RelationalRowsCte{
        .{
            .name = "__antfly_read_join_0",
            .left_table = "usage_records",
            .right_table = "archived_records",
            .join = .{
                .left = .{ .select_all = true },
                .right = .{ .select_all = true },
                .on = cte0_on[0..],
                .select = cte0_select[0..],
            },
        },
        .{
            .name = "__antfly_set_right_join_1",
            .left_table = "usage_records",
            .right_table = "tenant_records",
            .join = .{
                .left = .{ .source_cte = "__antfly_read_join_0", .select_all = true },
                .right = .{ .select_all = true },
                .on = cte1_on[0..],
                .select = cte1_select[0..],
            },
        },
    };
    const lowered = sql_adapter.LoweredReadPlan{ .set_operation = .{
        .operation = .union_all,
        .ctes = ctes[0..],
        .left = .{
            .table_name = "usage_records",
            .plan = .{ .query = .{
                .predicates = left_predicates[0..],
                .select = left_select[0..],
                .select_all = false,
            } },
        },
        .right = .{
            .table_name = "tenant_records",
            .plan = .{ .query = .{
                .source_cte = "__antfly_set_right_join_1",
                .select_all = true,
            } },
        },
    } };
    switch (lowered) {
        .set_operation => |set_operation| {
            try std.testing.expectEqual(@as(usize, 2), set_operation.ctes.len);
            try std.testing.expectEqualStrings("usage_records", set_operation.ctes[0].left_table);
            try std.testing.expectEqualStrings("archived_records", set_operation.ctes[0].right_table);
            try std.testing.expectEqualStrings("usage_records", set_operation.ctes[1].left_table);
            try std.testing.expectEqualStrings("tenant_records", set_operation.ctes[1].right_table);
            try std.testing.expectEqualStrings(set_operation.ctes[1].name, set_operation.right.plan.query.source_cte);
        },
        else => return error.TestUnexpectedResult,
    }

    var catalog = FakeCatalog{};
    var lowered_generated = try sql_adapter.lowerReadPlanWithCatalogAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status = 'open' UNION ALL SELECT tenant_records.tenant_ref AS id FROM usage_records JOIN archived_records ON usage_records.id = archived_records.id JOIN tenant_records ON archived_records.tenant_ref = tenant_records.tenant_ref",
        usage_schema,
        &.{},
        catalog.iface(),
    );
    defer lowered_generated.deinit(alloc);
    switch (lowered_generated) {
        .set_operation => |set_operation| {
            try std.testing.expectEqual(@as(usize, 2), set_operation.ctes.len);
            try std.testing.expectEqualStrings("usage_records", set_operation.ctes[0].left_table);
            try std.testing.expectEqualStrings("archived_records", set_operation.ctes[0].right_table);
            try std.testing.expectEqualStrings("usage_records", set_operation.ctes[1].left_table);
            try std.testing.expectEqualStrings("tenant_records", set_operation.ctes[1].right_table);
            try std.testing.expectEqualStrings(set_operation.ctes[1].name, set_operation.right.plan.query.source_cte);
        },
        else => return error.TestUnexpectedResult,
    }

    var generated_fake = FakeSource{};
    var generated_result = (try executeLoweredSqlReadPlanAlloc(
        alloc,
        generated_fake.source(),
        catalog.iface(),
        "usage_records",
        usage_schema,
        lowered_generated,
        .read_index,
    )).?;
    defer generated_result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), generated_fake.usage_scans);
    try std.testing.expectEqual(@as(usize, 1), generated_fake.archived_scans);
    try std.testing.expectEqual(@as(usize, 1), generated_fake.tenant_scans);
    switch (generated_result) {
        .set_operation => |query_result| {
            try std.testing.expectEqual(@as(u32, 3), query_result.total);
            try std.testing.expectEqual(@as(usize, 3), query_result.rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"u1\"}", query_result.rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"u3\"}", query_result.rows[1]);
            try std.testing.expectEqualStrings("{\"id\":\"t1\"}", query_result.rows[2]);
        },
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
    try std.testing.expectEqual(@as(usize, 1), fake.tenant_scans);
    switch (result) {
        .set_operation => |query_result| {
            try std.testing.expectEqual(@as(u32, 3), query_result.total);
            try std.testing.expectEqual(@as(usize, 3), query_result.rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"u1\"}", query_result.rows[0]);
            try std.testing.expectEqualStrings("{\"id\":\"u3\"}", query_result.rows[1]);
            try std.testing.expectEqualStrings("{\"id\":\"t1\"}", query_result.rows[2]);
        },
        else => return error.TestUnexpectedResult,
    }
}
