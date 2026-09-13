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
const codec = @import("../algebraic/relational_row_codec.zig");
const schema = @import("../../schema.zig");
const document_query = @import("../document_query.zig");
const JsonView = @import("json_view.zig").View;

/// Positive field selections compiled against one immutable layout. Nested
/// paths reuse document projection semantics, but decode only selected columns.
pub const Plan = struct {
    pub const Source = union(enum) { logical: std.json.Value, json: *JsonView };
    pub const Sources = std.StringArrayHashMapUnmanaged(Source);
    base: codec.OrdinalProjectionPlan,
    arena: std.heap.ArenaAllocator,
    paths: []const []const []const u8 = &.{},

    /// Positive selections share one contract across row and column readers.
    /// A hyphen inside a name is literal; only a leading hyphen is exclusion.
    pub fn supports(fields: []const []const u8, include_all_fields: bool) bool {
        if (fields.len == 0) return !include_all_fields;
        for (fields) |field| {
            if (field.len == 0 or field[0] == '-' or field[0] == '_' or
                std.mem.indexOfScalar(u8, field, '*') != null) return false;
        }
        return true;
    }

    pub fn init(alloc: std.mem.Allocator, table: schema.TableSchema, layout: *const codec.PhysicalLayout, fields: []const []const u8) !Plan {
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        var nested = false;
        for (fields) |field| nested = nested or std.mem.indexOfScalar(u8, field, '.') != null;
        if (!nested) return .{ .base = try codec.OrdinalProjectionPlan.init(alloc, table, layout, fields), .arena = arena };

        const scratch = arena.allocator();
        const roots = try scratch.alloc([]const u8, fields.len);
        const paths = try scratch.alloc([]const []const u8, fields.len);
        for (fields, 0..) |field, i| {
            const owned = try scratch.dupe(u8, field);
            var tokens = std.mem.tokenizeScalar(u8, owned, '.');
            var parts = std.ArrayListUnmanaged([]const u8).empty;
            while (tokens.next()) |part| try parts.append(scratch, part);
            paths[i] = try parts.toOwnedSlice(scratch);
            roots[i] = if (paths[i].len > 0) paths[i][0] else "";
        }
        return .{
            .base = try codec.OrdinalProjectionPlan.init(alloc, table, layout, roots),
            .arena = arena,
            .paths = paths,
        };
    }

    pub fn deinit(self: *Plan) void {
        self.base.deinit();
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn project(self: Plan, alloc: std.mem.Allocator, row: codec.OrdinalRowView) ![]u8 {
        if (self.base.schema_version != row.table_schema.version) return error.RelationalRowSchemaMismatch;
        if (self.paths.len == 0) return try row.projectAlloc(alloc, self.base);
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        var source = Sources.empty;
        for (self.base.ordinals) |ordinal| {
            const cell = (try row.findCell(ordinal)) orelse continue;
            const value: Source = if (cell.is_json and !cell.is_null) blk: {
                const view = try scratch.create(JsonView);
                view.* = .init(scratch, cell.value.bytes_val);
                break :blk .{ .json = view };
            } else .{ .logical = try row.materializeCellAlloc(scratch, cell) };
            try source.put(scratch, row.table_schema.relational_columns[ordinal].name, value);
        }
        return self.projectSources(alloc, scratch, source);
    }

    pub fn projectSources(self: Plan, alloc: std.mem.Allocator, scratch: std.mem.Allocator, source: Sources) ![]u8 {
        var result = std.json.ObjectMap.empty;
        if (self.paths.len == 0) {
            var output = std.Io.Writer.Allocating.init(alloc);
            defer output.deinit();
            output.writer.writeByte('{') catch return error.OutOfMemory;
            // Sources are inserted in projection ordinal order.
            for (source.keys(), source.values(), 0..) |name, value, i| {
                if (i != 0) output.writer.writeByte(',') catch return error.OutOfMemory;
                std.json.Stringify.value(name, .{}, &output.writer) catch return error.OutOfMemory;
                output.writer.writeByte(':') catch return error.OutOfMemory;
                switch (value) {
                    .logical => |logical| std.json.Stringify.value(logical, .{}, &output.writer) catch return error.OutOfMemory,
                    // Whole JSON columns are already canonical API values.
                    // Emit their bytes without constructing a throwaway DOM.
                    .json => |view| output.writer.writeAll(view.root.raw) catch return error.OutOfMemory,
                }
            }
            output.writer.writeByte('}') catch return error.OutOfMemory;
            return output.toOwnedSlice();
        } else for (self.paths) |parts| {
            if (parts.len == 0) continue;
            const value = source.get(parts[0]) orelse continue;
            switch (value) {
                .json => |view| try view.include(scratch, &result, parts[0], parts[1..]),
                .logical => |logical| {
                    var object = std.json.ObjectMap.empty;
                    try object.put(scratch, parts[0], logical);
                    try document_query.applyIncludePath(scratch, object, &result, parts);
                },
            }
        }
        return std.json.Stringify.valueAlloc(alloc, std.json.Value{ .object = result }, .{});
    }
};

test "relational nested projection preserves document semantics with bounded scratch" {
    const alloc = std.testing.allocator;
    const columns = [_]schema.RelationalColumn{
        .{ .name = "payload", .path = "payload", .column_type = .json, .is_json = true, .json_kind = .any },
        .{ .name = "large", .path = "large", .column_type = .string },
    };
    const table = schema.TableSchema{ .version = 1, .storage_mode = .relational, .relational_columns = &columns };
    var layout = try codec.PhysicalLayout.init(alloc, table);
    defer layout.deinit();
    const large = try alloc.alloc(u8, 1024 * 1024);
    defer alloc.free(large);
    @memset(large, 'x');
    const cells = [_]codec.Cell{
        .{ .ordinal = 0, .path = "payload", .is_json = true, .value_type = .bytes_val, .value = .{ .bytes_val = "{\"customer\":{\"id\":9007199254740993,\"name\":\"Ada\"},\"items\":[{\"id\":1},{\"id\":2}],\"nil\":null}" } },
        .{ .ordinal = 1, .path = "large", .value_type = .bytes_val, .value = .{ .bytes_val = large } },
    };
    const encoded = try codec.serializeOrdinal(alloc, 1, &columns, &cells, @splat(0));
    defer alloc.free(encoded);
    const row = try codec.ordinalRowViewTrusted(encoded, table, &layout);
    const full = try row.reconstructValueAlloc(alloc);
    defer alloc.free(full);
    const selections = [_][]const []const u8{
        &.{"payload.customer.id"},
        &.{ "payload.items.id", "payload.nil", "missing.nested" },
        &.{"payload.items.1.id"},
        &.{ "payload.customer.name", "payload", "payload.customer.id" },
        &.{"..payload..customer.id."},
    };
    for (selections) |fields| {
        var plan = try Plan.init(alloc, table, &layout, fields);
        defer plan.deinit();
        const expected = try document_query.lookupJson(alloc, full, .{ .fields = fields, .include_all_fields = false });
        defer alloc.free(expected.json);
        // A 1 MiB unselected column must never be materialized. This is a
        // deterministic allocation bound, not a timing-sensitive benchmark.
        var buffer: [32 * 1024]u8 = undefined;
        var fixed = std.heap.FixedBufferAllocator.init(&buffer);
        const actual = try plan.project(fixed.allocator(), row);
        try std.testing.expectEqualStrings(expected.json, actual);
    }
    const Check = struct {
        fn run(failing: std.mem.Allocator, t: schema.TableSchema, l: *const codec.PhysicalLayout, r: codec.OrdinalRowView) !void {
            var plan = try Plan.init(failing, t, l, &.{ "payload.customer.id", "payload.items.0.id" });
            defer plan.deinit();
            const result = try plan.project(failing, r);
            defer failing.free(result);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Check.run, .{ table, &layout, row });
}
