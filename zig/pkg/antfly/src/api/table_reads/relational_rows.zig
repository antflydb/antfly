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

const db_mod = @import("../../storage/db/mod.zig");
const storage_schema = @import("../../storage/schema.zig");
const sql_adapter_runtime = @import("../../sql/runtime.zig");
const raft_mod = @import("../../raft/mod.zig");
const schema_api = @import("../../schema/mod.zig");
const table_catalog = @import("../table_catalog.zig");
const catalog_resources = @import("../catalog_resources.zig");
const relational_rows_api = @import("../relational_rows.zig");
const core = @import("core.zig");

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

pub const RecursiveCteMaterializedRows = struct {
    rows: []const []const u8 = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.rows) |row| alloc.free(@constCast(row));
        if (self.rows.len > 0) alloc.free(self.rows);
        self.* = undefined;
    }
};

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
