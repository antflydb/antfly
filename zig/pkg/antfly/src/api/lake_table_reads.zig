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

//! Public typed rows share SQL's authorized external binding and cursor.
const std = @import("std");
const catalog = @import("antfly_local_sources").sql_catalog;
const Adapter = @import("sql_execution.zig").Adapter;
const helpers = @import("http_route_helpers.zig");

pub fn query(alloc: std.mem.Allocator, adapter: *Adapter, target: @import("antfly_local_sources").system_catalog_domain.Target, expected_id: u64, request: helpers.OwnedScanKeysRequest) !?[]u8 {
    if (!adapter.server.source.vtable.supports_query_definitions) return null;
    var budget: @import("antfly_local_sources").sql_memory_budget = .{ .backing = alloc, .limit = 64 * 1024 * 1024 };
    const scan_alloc = budget.allocator();
    var arena = std.heap.ArenaAllocator.init(scan_alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const definition_bytes = try adapter.server.source.systemCatalog(a, adapter.context, .{ .resolve_many = .{ .targets = &.{target}, .include_query_definitions = true } });
    const resolved = try std.json.parseFromSliceLeaky(@import("antfly_local_sources").system_catalog_domain.ResolvedMany, a, definition_bytes, .{ .allocate = .alloc_always });
    if (resolved.tables.len != 1) return error.InvalidSqlBackendResponse;
    const current = resolved.tables[0] orelse return error.TableNotFound;
    if (current.table_id != expected_id) return error.CatalogGenerationChanged;
    const definition = current.query_definition orelse return error.InvalidSqlBackendResponse;
    const external = try @import("antfly_local_sources").serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, definition.schema_json);
    if (external == null) return null;
    adapter.revision = resolved.revision;
    const backend = adapter.backend();
    const table = try backend.vtable.resolve(backend.ptr, a, .{ .database = target.database, .namespace = target.namespace, .table = target.table }, .read);
    if (table.id != expected_id) return error.CatalogGenerationChanged;
    if (table.external_base_source == null) return null;
    const native = try std.json.parseFromSliceLeaky(@import("antfly_metadata_openapi").types.RelationalRowQueryRequest, a, request.relational_query_json, .{ .parse_numbers = false });
    if (native.index != null or native.lower != null or native.upper != null or native.after != null) return error.UnsupportedRowsQuery;
    if (native.schema_version) |version| if (version != table.schema_version) return error.CatalogGenerationChanged;
    const input_conditions: []const @import("antfly_metadata_openapi").types.RelationalRowCondition = native.conditions orelse &.{};
    var fields: std.ArrayList([]const u8) = .empty;
    try fields.appendSlice(a, native.fields);
    for (input_conditions) |condition| {
        if (condition.collation != null) return error.UnsupportedRowsQuery;
        _ = try table.column(condition.column);
        const present = for (fields.items) |field| {
            if (std.mem.eql(u8, field, condition.column)) break true;
        } else false;
        if (!present) try fields.append(a, condition.column);
    }
    var pushed: std.ArrayList(catalog.Condition) = .empty;
    for (input_conditions) |condition| {
        const op: catalog.Condition.Op = switch (condition.op) {
            .eq => .eq,
            .ne => .neq,
            .lt => .lt,
            .lte => .lte,
            .gt => .gt,
            .gte => .gte,
            .is_null => .is_null,
            .is_not_null => .is_not_null,
            else => continue,
        };
        var value = condition.value orelse .null;
        if (value == .string and (try table.column(condition.column)).type == .integer) value = .{ .integer = std.fmt.parseInt(i64, value.string, 10) catch return error.InvalidQueryRequest };
        try pushed.append(a, .{ .column = condition.column, .op = op, .value = value });
    }
    const conditions = try normalizeConditions(a, table, input_conditions);
    const cursor = try adapter.openLakeScan(scan_alloc, table, .{ .fields = fields.items, .conditions = pushed.items, .after = if (request.from.len == 0) null else request.from, .before = if (request.to.len == 0) null else request.to, .primary_order = true, .limit = request.opts.limit });
    defer cursor.close(cursor.ptr);
    var output: std.Io.Writer.Allocating = .init(alloc);
    errdefer output.deinit();
    var remaining = request.opts.limit;
    while (remaining != 0) {
        const page = try cursor.next(cursor.ptr, scan_alloc, 256);
        var scratch = std.heap.ArenaAllocator.init(scan_alloc);
        defer scratch.deinit();
        defer page.deinit();
        for (page.rows) |row| {
            if (remaining == 0) break;
            _ = scratch.reset(.retain_capacity);
            const row_alloc = scratch.allocator();
            if (!try matchesNormalized(row_alloc, row, table, conditions)) continue;
            if (request.from.len != 0 and std.mem.order(u8, row.id, request.from) != .gt) continue;
            if (request.to.len != 0 and std.mem.order(u8, row.id, request.to) != .lt) continue;
            var projected = std.json.ObjectMap.empty;
            for (native.fields) |name| try projected.put(row_alloc, name, (try row.cell(name)).value);
            var encoded: std.Io.Writer.Allocating = .init(row_alloc);
            try std.json.Stringify.value(.{ ._id = row.id, .row = std.json.Value{ .object = projected }, .version = "0", .schema_version = table.schema_version }, .{}, &encoded.writer);
            if (encoded.written().len >= 32 * 1024 * 1024 -| output.written().len) return error.SqlResultTooLarge;
            try output.writer.writeAll(encoded.written());
            try output.writer.writeByte('\n');
            remaining -= 1;
        }
        if (page.after == null) break;
    }
    return try output.toOwnedSlice();
}

const Condition = @import("antfly_metadata_openapi").types.RelationalRowCondition;
fn normalizeConditions(a: std.mem.Allocator, table: catalog.Table, input: []const Condition) ![]const Condition {
    const result = try a.dupe(Condition, input);
    for (result) |*condition| {
        const kind = (try table.column(condition.column)).type;
        var value = condition.value orelse .null;
        if (value == .string and kind == .integer) value = .{ .integer = std.fmt.parseInt(i64, value.string, 10) catch return error.InvalidQueryRequest };
        condition.value = if (condition.op == .is_null or condition.op == .is_not_null) .null else try @import("antfly_local_sources").sql_lake_values.comparisonValue(a, value, kind);
    }
    return result;
}
fn matches(a: std.mem.Allocator, row: catalog.Row, table: catalog.Table, conditions: []const Condition) !bool {
    return matchesNormalized(a, row, table, try normalizeConditions(a, table, conditions));
}
fn matchesNormalized(a: std.mem.Allocator, row: catalog.Row, table: catalog.Table, conditions: []const Condition) !bool {
    for (conditions) |condition| {
        const stored = try row.cell(condition.column);
        const kind = (try table.column(condition.column)).type;
        const cell: @import("antfly_local_sources").sql_scalar.Datum = .{ .value = try @import("antfly_local_sources").sql_lake_values.comparisonValue(a, stored.value, kind), .sql_null = stored.sql_null };
        const operand = condition.value orelse .null;
        const match = switch (condition.op) {
            .is_null => cell.sql_null,
            .is_not_null => !cell.sql_null,
            .is_distinct, .is_not_distinct => blk: {
                const distinct = if (cell.sql_null or operand == .null) cell.sql_null != (operand == .null) else (try @import("antfly_local_sources").sql_scalar.compare(cell.value, operand)) != .eq;
                break :blk if (condition.op == .is_distinct) distinct else !distinct;
            },
            else => blk: {
                if (cell.sql_null or operand == .null) break :blk false;
                const order = try @import("antfly_local_sources").sql_scalar.compare(cell.value, operand);
                break :blk switch (condition.op) {
                    .eq => order == .eq,
                    .ne => order != .eq,
                    .lt => order == .lt,
                    .lte => order != .gt,
                    .gt => order == .gt,
                    .gte => order != .lt,
                    else => unreachable,
                };
            },
        };
        if (!match) return false;
    }
    return true;
}

test "lake SQL public rows datetime equality and distinct predicates share normalized operands" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var object = std.json.ObjectMap.empty;
    try object.put(a, "ts", .{ .integer = -1 });
    const table: catalog.Table = .{ .id = 1, .physical_name = "events", .schema_version = 1, .columns = &.{.{ .name = "ts", .path = "ts", .type = .datetime }} };
    const row: catalog.Row = .{ .id = "row", .version = 0, .value = .{ .object = object }, .sql_nulls = &.{false} };
    const conditions = [_]@import("antfly_metadata_openapi").types.RelationalRowCondition{
        .{ .column = "ts", .op = .eq, .value = .{ .string = "1970-01-01T00:59:59.999999999+01:00" } },
        .{ .column = "ts", .op = .is_not_distinct, .value = .{ .integer = -1 } },
    };
    try std.testing.expect(try matches(a, row, table, &conditions));
    const null_row: catalog.Row = .{ .id = "null", .version = 0, .value = .{ .object = std.json.ObjectMap.empty } };
    try std.testing.expect(try matches(a, null_row, table, &.{.{ .column = "ts", .op = .is_not_distinct }}));
    try std.testing.expect(!try matches(a, null_row, table, &conditions));
}
