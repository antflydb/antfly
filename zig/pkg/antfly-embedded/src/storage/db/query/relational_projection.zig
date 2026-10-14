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
    selectors: ?*const @import("json_projection_plan.zig").Selector = null,

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
        const compiled = try scratch.alloc(@import("json_projection_plan.zig").Path, paths.len);
        for (paths, compiled) |parts, *path| path.* = .{ .parts = parts };
        const selectors = try @import("json_projection_plan.zig").compile(scratch, compiled);
        return .{
            .selectors = selectors,
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
        var arena = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.reclaimingScratch(alloc));
        defer arena.deinit();
        return self.projectWithOutput(arena.allocator(), alloc, row);
    }

    pub fn projectWithOutput(self: Plan, scratch_alloc: std.mem.Allocator, output: std.mem.Allocator, row: codec.OrdinalRowView) ![]u8 {
        if (self.base.schema_version != row.table_schema.version) return error.RelationalRowSchemaMismatch;
        if (self.paths.len == 0) {
            const bytes = try row.projectAlloc(scratch_alloc, self.base);
            defer scratch_alloc.free(bytes);
            return output.dupe(u8, bytes);
        }
        var arena = std.heap.ArenaAllocator.init(scratch_alloc);
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
        return self.projectSources(output, scratch, source);
    }

    pub fn projectSources(self: Plan, alloc: std.mem.Allocator, scratch: std.mem.Allocator, source: Sources) ![]u8 {
        const Projection = @import("projection_output.zig").Output;
        var result: Projection.Object = .empty;
        if (self.paths.len == 0) {
            var output = std.Io.Writer.Allocating.init(scratch);
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
            return alloc.dupe(u8, output.written());
        } else {
            if (self.selectors) |selectors| for (source.keys(), source.values()) |name, value| {
                if (value == .json) if (selectors.fields.get(name)) |selected| {
                    if (selected.fields.count() != 0) try value.json.prepareProjectionWithOptions(selected, .visited, .any_container, .{ .defer_fanout = true });
                };
            };
            for (self.paths) |parts| {
                if (parts.len == 0) continue;
                const value = source.getPtr(parts[0]) orelse continue;
                switch (value.*) {
                    .json => |view| try Projection.include(scratch, &result, parts[0], .{ .span = .{ .view = view, .node = &view.root } }, parts[1..]),
                    .logical => |*logical| try Projection.include(scratch, &result, parts[0], .{ .logical = logical }, parts[1..]),
                }
            }
        }
        return (Projection{ .object = result }).serialize(scratch, alloc);
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

test "public graph metric borrowed wire native JSON sparse selectors reuse immutable plans" {
    const a = std.testing.allocator;
    const Counter = @import("../../lite/test_allocator.zig").BudgetAllocator;
    const columns = [_]schema.RelationalColumn{.{ .name = "payload", .path = "payload", .column_type = .json, .is_json = true, .json_kind = .any }};
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &columns };
    var layout = try codec.PhysicalLayout.init(a, table);
    defer layout.deinit();
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    try input.writer.writeAll("{\"arr\":[");
    for (0..20000) |i| {
        if (i != 0) try input.writer.writeByte(',');
        try input.writer.print("{d}", .{i});
    }
    try input.writer.writeAll("]}");
    const cells = [_]codec.Cell{.{ .ordinal = 0, .path = "payload", .is_json = true, .value_type = .bytes_val, .value = .{ .bytes_val = input.written() } }};
    const encoded = try codec.serializeOrdinal(a, 1, &columns, &cells, @splat(0));
    defer a.free(encoded);
    const row = try codec.ordinalRowViewTrusted(encoded, table, &layout);
    const fields = [_][]const u8{ "payload.arr.19999", "payload.arr.19998", "payload.arr.19997", "payload.arr.19996", "payload.arr.19995", "payload.arr.19994", "payload.arr.19993", "payload.arr.19992", "payload.arr.19991", "payload.arr.19990", "payload.arr.19989", "payload.arr.19988", "payload.arr.19987", "payload.arr.19986", "payload.arr.19985", "payload.arr.19984" };
    var plan = try Plan.init(a, table, &layout, &fields);
    defer plan.deinit();
    const selector = plan.selectors.?;
    const capacity = plan.arena.queryCapacity();
    for (0..16) |_| {
        var budget: Counter = .{ .backing = a, .limit = 64 * 1024 };
        const result = try plan.projectWithOutput(budget.allocator(), a, row);
        defer a.free(result);
        try std.testing.expectEqualStrings("{\"payload\":{\"arr\":[19984]}}", result);
        try std.testing.expectEqual(@as(usize, 0), budget.live);
        try std.testing.expectEqual(selector, plan.selectors.?);
        try std.testing.expectEqual(capacity, plan.arena.queryCapacity());
    }
    // Preserve native number lexemes and ignored duplicate-key behavior.
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var view = JsonView.init(arena.allocator(), "{\"arr\":[1e2],\"skip\":{\"x\":1,\"x\":2}}");
    var sources: Plan.Sources = .empty;
    try sources.put(arena.allocator(), "payload", .{ .json = &view });
    var lexical = try Plan.init(a, table, &layout, &.{"payload.arr.0"});
    defer lexical.deinit();
    const result = try lexical.projectSources(a, arena.allocator(), sources);
    defer a.free(result);
    try std.testing.expectEqualStrings("{\"payload\":{\"arr\":[1e2]}}", result);
}

test "public graph metric borrowed wire native JSON copy on write keeps output navigation in row scratch" {
    const a = std.testing.allocator;
    const columns = [_]schema.RelationalColumn{.{ .name = "payload", .path = "payload", .column_type = .json, .is_json = true, .json_kind = .any }};
    const table = schema.TableSchema{ .version = 1, .storage_mode = .relational, .relational_columns = &columns };
    var layout = try codec.PhysicalLayout.init(a, table);
    defer layout.deinit();
    var plan = try Plan.init(a, table, &layout, &.{ "payload", "payload.a.x" });
    defer plan.deinit();
    var source_arena = std.heap.ArenaAllocator.init(a);
    defer source_arena.deinit();
    var view = JsonView.init(source_arena.allocator(), "{\"a\":{\"x\":1,\"y\":2},\"other\":3}");
    var sources = Plan.Sources.empty;
    defer sources.deinit(a);
    try sources.put(a, "payload", .{ .json = &view });
    {
        var row_arena = std.heap.ArenaAllocator.init(a);
        defer row_arena.deinit();
        const bytes = try plan.projectSources(a, row_arena.allocator(), sources);
        defer a.free(bytes);
        try std.testing.expectEqualStrings("{\"payload\":{\"a\":{\"x\":1,\"y\":2},\"other\":3}}", bytes);
        // The caller's page-owned view still contains only the selected field.
        try std.testing.expectEqual(@as(usize, 1), view.root.children.?.len);
    }
    var values: std.ArrayListUnmanaged(std.json.Value) = .empty;
    try view.collect(&.{ "a", "y" }, false, &values);
    try std.testing.expectEqual(@as(i64, 2), values.items[0].integer);
}

test "public graph metric borrowed wire streaming native fanout leaves page navigation bounded" {
    const a = std.testing.allocator;
    const counters = @import("../../lite/test_allocator.zig");
    const columns = [_]schema.RelationalColumn{.{ .name = "payload", .path = "payload", .column_type = .json, .is_json = true, .json_kind = .any }};
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &columns };
    var layout = try codec.PhysicalLayout.init(a, table);
    defer layout.deinit();
    var plan = try Plan.init(a, table, &layout, &.{"payload.items.x"});
    defer plan.deinit();
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    try input.writer.writeAll("{\"items\":[");
    for (0..8000) |i| {
        if (i != 0) try input.writer.writeByte(',');
        try input.writer.writeAll("{\"x\":{\"m\":1e2,\"n\":2}}");
    }
    try input.writer.writeAll("]}");
    var no_resize: counters.NoResizeAllocator = .{ .backing = a };
    var budget: counters.BudgetAllocator = .{ .backing = no_resize.allocator(), .limit = 1024 * 1024 };
    {
        var owner = std.heap.ArenaAllocator.init(budget.allocator());
        defer owner.deinit();
        var view = JsonView.init(owner.allocator(), input.written());
        for (0..2) |_| {
            var row = std.heap.ArenaAllocator.init(budget.allocator());
            defer row.deinit();
            var source: Plan.Sources = .empty;
            try source.put(row.allocator(), "payload", .{ .json = &view });
            const bytes = try plan.projectSources(a, row.allocator(), source);
            defer a.free(bytes);
            try std.testing.expectEqualStrings("{\"payload\":", bytes[0..11]);
            try std.testing.expectEqualStrings(input.written(), bytes[11 .. bytes.len - 1]);
            const items = (try view.field(&view.root, "items")).?;
            try std.testing.expect(items.children == null);
            try std.testing.expectEqual(@as(usize, 0), items.array_index.count());
        }
    }
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    try std.testing.expect(budget.alloc_calls < 256);
    std.debug.print("STREAMING_NATIVE_FANOUT count=8000 iterations=2 allocations={d} peak={d}\n", .{ budget.alloc_calls, budget.peak });
    var owner = std.heap.ArenaAllocator.init(a);
    defer owner.deinit();
    var view = JsonView.init(owner.allocator(), "{\"items\":[{\"x\":1},{\"x\":2,\"x\":3}]}");
    var source: Plan.Sources = .empty;
    try source.put(owner.allocator(), "payload", .{ .json = &view });
    try std.testing.expectError(error.DuplicateField, plan.projectSources(a, owner.allocator(), source));
}
