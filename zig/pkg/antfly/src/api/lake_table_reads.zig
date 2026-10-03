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
const catalog = @import("../sql/catalog.zig");
const Adapter = @import("sql_execution.zig").Adapter;
const helpers = @import("http_route_helpers.zig");

pub fn query(alloc: std.mem.Allocator, adapter: *Adapter, target: @import("../system_catalog/domain.zig").Target, expected_id: u64, request: helpers.OwnedScanKeysRequest) !?[]u8 {
    if (!adapter.server.source.vtable.supports_query_definitions) return null;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const definition_bytes = try adapter.server.source.systemCatalog(a, adapter.context, .{ .resolve_many = .{ .targets = &.{target}, .include_query_definitions = true } });
    const resolved = try std.json.parseFromSliceLeaky(@import("../system_catalog/domain.zig").ResolvedMany, a, definition_bytes, .{ .allocate = .alloc_always });
    if (resolved.tables.len != 1) return error.InvalidSqlBackendResponse;
    const current = resolved.tables[0] orelse return error.TableNotFound;
    if (current.table_id != expected_id) return error.CatalogGenerationChanged;
    const definition = current.query_definition orelse return error.InvalidSqlBackendResponse;
    const external = try @import("../serverless/external_source/schema_binding.zig").externalBindingFromSchemaJsonAlloc(a, definition.schema_json);
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
    const cursor = try adapter.openLakeScan(alloc, table, .{ .fields = fields.items, .limit = request.opts.limit });
    defer cursor.close(cursor.ptr);
    var output: std.Io.Writer.Allocating = .init(alloc);
    errdefer output.deinit();
    var remaining = request.opts.limit;
    while (remaining != 0) {
        const page = try cursor.next(cursor.ptr, a, remaining);
        defer page.deinit();
        for (page.rows) |row| {
            if (!try matches(row, table, input_conditions)) continue;
            if (request.from.len != 0 and std.mem.order(u8, row.id, request.from) != .gt) continue;
            if (request.to.len != 0 and std.mem.order(u8, row.id, request.to) != .lt) continue;
            var projected = std.json.ObjectMap.empty;
            for (native.fields) |name| try projected.put(a, name, (try row.cell(name)).value);
            try std.json.Stringify.value(.{ ._id = row.id, .row = std.json.Value{ .object = projected }, .version = "0", .schema_version = table.schema_version }, .{}, &output.writer);
            try output.writer.writeByte('\n');
            remaining -= 1;
        }
        if (page.after == null) break;
    }
    return try output.toOwnedSlice();
}

fn matches(row: catalog.Row, table: catalog.Table, conditions: []const @import("antfly_metadata_openapi").types.RelationalRowCondition) !bool {
    for (conditions) |condition| {
        const cell = try row.cell(condition.column);
        var operand = condition.value orelse .null;
        if (operand == .string and (try table.column(condition.column)).type == .integer) operand = .{ .integer = std.fmt.parseInt(i64, operand.string, 10) catch return error.InvalidQueryRequest };
        const match = switch (condition.op) {
            .is_null => cell.sql_null,
            .is_not_null => !cell.sql_null,
            .is_distinct, .is_not_distinct => blk: {
                const distinct = if (cell.sql_null or operand == .null) cell.sql_null != (operand == .null) else (try @import("../sql/scalar.zig").compare(cell.value, operand)) != .eq;
                break :blk if (condition.op == .is_distinct) distinct else !distinct;
            },
            else => blk: {
                if (cell.sql_null or operand == .null) break :blk false;
                const order = try @import("../sql/scalar.zig").compare(cell.value, operand);
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
