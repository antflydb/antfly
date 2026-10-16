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
    pub const Source = union(enum) { logical: std.json.Value, json: *JsonView, native: *@import("projection_output.zig").NativeCell };
    pub const Sources = std.StringArrayHashMapUnmanaged(Source);
    base: codec.OrdinalProjectionPlan,
    arena: std.heap.ArenaAllocator,
    paths: []const []const []const u8 = &.{},
    selectors: ?*const @import("json_projection_plan.zig").Selector = null,
    path_selectors: []const ?*const @import("json_projection_plan.zig").Selector = &.{},

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
        const path_selectors = try scratch.alloc(?*const @import("json_projection_plan.zig").Selector, paths.len);
        for (compiled, path_selectors) |path, *selected| selected.* = if (path.parts.len > 2) (if (compiled.len == 1) selectors else try @import("json_projection_plan.zig").compile(scratch, &.{path})) else null;
        return .{
            .selectors = selectors,
            .base = try codec.OrdinalProjectionPlan.init(alloc, table, layout, roots),
            .arena = arena,
            .paths = paths,
            .path_selectors = path_selectors,
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
        return self.projectWithOutputPolicy(scratch_alloc, output, row, false);
    }
    pub fn projectWithOutputPolicy(self: Plan, scratch_alloc: std.mem.Allocator, output: std.mem.Allocator, row: codec.OrdinalRowView, exact: bool) ![]u8 {
        if (self.base.schema_version != row.table_schema.version) return error.RelationalRowSchemaMismatch;
        if (self.paths.len == 0) {
            var emitter: codec.OrdinalRowView.ProjectionWriter = .{ .row = row, .scratch = @import("antfly_platform").allocator.reclaimingScratch(scratch_alloc), .plan = self.base };
            defer emitter.deinit();
            return @import("serialized_output.zig").alloc(output, exact, &emitter);
        }
        var arena = std.heap.ArenaAllocator.init(scratch_alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        var source = Sources.empty;
        for (self.base.ordinals) |ordinal| {
            const cell = (try row.findCell(ordinal)) orelse continue;
            const value = try sourceForCell(scratch, &row.table_schema.relational_columns[ordinal], cell, true);
            try source.put(scratch, row.table_schema.relational_columns[ordinal].name, value);
        }
        return self.projectSourcesPolicy(output, scratch, source, exact);
    }

    /// Cell payload and schema stay pinned until synchronous serialization ends.
    /// Keep small scalar values inline; large strings/vectors use borrowed cells.
    /// `canonical_json` requires canonical storage admission, not just syntax
    /// validation. Callers without that provenance must leave it false.
    pub fn sourceForCell(alloc: std.mem.Allocator, column: *const schema.RelationalColumn, cell: codec.Cell, canonical_json: bool) !Source {
        if (cell.is_null) return .{ .logical = .null };
        if (cell.is_json) {
            const view = try alloc.create(JsonView);
            view.* = .init(alloc, cell.value.bytes_val);
            view.canonical_source = if (canonical_json) cell.value.bytes_val else null;
            return .{ .json = view };
        }
        return switch (column.column_type) {
            .integer, .number, .boolean, .datetime, .string, .blob, .geoshape => .{ .logical = try codec.borrowedJsonValueFromCellAlloc(alloc, column.*, cell) },
            else => blk: {
                const native = try alloc.create(@import("projection_output.zig").NativeCell);
                native.* = .{ .cell = cell, .column = column, .alloc = alloc };
                break :blk .{ .native = native };
            },
        };
    }

    pub fn projectSources(self: Plan, alloc: std.mem.Allocator, scratch: std.mem.Allocator, source: Sources) ![]u8 {
        return self.projectSourcesPolicy(alloc, scratch, source, false);
    }
    pub fn projectSourcesPolicy(self: Plan, alloc: std.mem.Allocator, scratch: std.mem.Allocator, source: Sources, exact: bool) ![]u8 {
        const Projection = @import("projection_output.zig").Output;
        if (self.paths.len == 0) return @import("serialized_output.zig").alloc(alloc, exact, SourceWriter{
            .source = source,
            .scratch = @import("antfly_platform").allocator.reclaimingScratch(scratch),
        });
        // Navigation belongs to this response, never to a retained page view.
        // Logical-only projections keep their existing allocator fast path.
        var navigation = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.reclaimingScratch(scratch));
        defer navigation.deinit();
        var ranges = JsonView.ValidatedRanges.init(scratch);
        defer ranges.deinit();
        // Choose one response-wide capacity before any column publishes proof.
        // The largest selected source may contain no cacheable ranges itself.
        if (self.selectors) |selectors| for (source.keys(), source.values()) |name, value| {
            if (value == .json) if (selectors.fields.get(name)) |selected| {
                if (selected.fields.count() != 0) ranges.reserveFor(value.json.root.raw.len);
            };
        };
        var row_alloc = scratch;
        var prepared: []?*JsonView = &.{};
        if (self.selectors) |selectors| for (source.keys(), source.values(), 0..) |name, value, i| {
            if (value == .json) if (selectors.fields.get(name)) |selected| {
                if (selected.fields.count() != 0) {
                    if (prepared.len == 0) {
                        row_alloc = navigation.allocator();
                        prepared = try row_alloc.alloc(?*JsonView, source.count());
                        @memset(prepared, null);
                    }
                    const view = try row_alloc.create(JsonView);
                    view.* = .init(row_alloc, value.json.root.raw);
                    view.parse_numbers = value.json.parse_numbers;
                    view.canonical_source = value.json.canonical_source;
                    view.root_sources = value.json.root_sources;
                    try view.prepareProjectionWithOptions(selected, .visited, .any_container, .{ .defer_fanout = true, .ranges = &ranges });
                    prepared[i] = view;
                }
            };
        };
        var projection_owner = @import("projection_output.zig").Owner.init(row_alloc);
        defer projection_owner.deinit();
        var result: Projection.Object = .empty;
        for (self.paths, self.path_selectors) |parts, selected| {
            if (parts.len == 0) continue;
            const i = source.getIndex(parts[0]) orelse continue;
            const value = &source.values()[i];
            switch (value.*) {
                .json => |original| {
                    const view = if (prepared.len != 0) prepared[i] orelse original else original;
                    try Projection.includePlanned(&projection_owner, &result, parts[0], .{ .span = .{ .view = view, .node = &view.root } }, parts[1..], if (selected) |s| s.fields.get(parts[0]) else null);
                },
                .native => |native| try Projection.includePlanned(&projection_owner, &result, parts[0], .{ .native = native }, parts[1..], null),
                .logical => |*logical| try Projection.includePlanned(&projection_owner, &result, parts[0], .{ .logical = logical }, parts[1..], null),
            }
        }
        return (Projection{ .object = result }).serializeWithPolicy(row_alloc, alloc, exact);
    }
    const SourceWriter = struct {
        source: Sources,
        scratch: std.mem.Allocator,
        pub fn write(self: @This(), writer: *std.Io.Writer) anyerror!void {
            try writer.writeByte('{');
            for (self.source.keys(), self.source.values(), 0..) |name, value, i| {
                if (i != 0) try writer.writeByte(',');
                try std.json.Stringify.value(name, .{}, writer);
                try writer.writeByte(':');
                switch (value) {
                    .logical => |logical| try std.json.Stringify.value(logical, .{}, writer),
                    .json => |view| try writer.writeAll(view.root.raw),
                    .native => |native| try native.write(self.scratch, writer),
                }
            }
            try writer.writeByte('}');
        }
    };
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
    for (0..16) |mode| {
        var budget: Counter = .{ .backing = a, .limit = 64 * 1024 };
        const result = try plan.projectWithOutputPolicy(budget.allocator(), a, row, mode % 2 == 0);
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
        // Temporary projection navigation never enters the page-owned view.
        try std.testing.expect(view.root.children == null);
        try std.testing.expect(view.validated_ranges == null);
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
            try std.testing.expect(view.root.children == null);
            try std.testing.expect(view.validated_ranges == null);
            try std.testing.expectEqual(@as(usize, 0), owner.queryCapacity());
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

test "public graph metric borrowed wire native columns share bounded cache without retaining response state" {
    const a = std.testing.allocator;
    const columns = [_]schema.RelationalColumn{
        .{ .name = "first", .path = "first", .column_type = .json, .is_json = true, .json_kind = .any },
        .{ .name = "second", .path = "second", .column_type = .json, .is_json = true, .json_kind = .any },
    };
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &columns };
    var layout = try codec.PhysicalLayout.init(a, table);
    defer layout.deinit();
    var plan = try Plan.init(a, table, &layout, &.{ "first.items.x.m", "second.items.x.m" });
    defer plan.deinit();
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    try input.writer.writeAll("{\"items\":[{\"x\":{\"m\":1e2,\"skip\":\"");
    try input.writer.splatByteAll('z', 16384);
    try input.writer.writeAll("\"}}],\"ignored\":{\"a\":1,\"a\":2}}");
    var owner = std.heap.ArenaAllocator.init(a);
    defer owner.deinit();
    var first = JsonView.init(owner.allocator(), input.written());
    var second = JsonView.init(owner.allocator(), "{\"items\":[{\"x\":{\"m\":2e2}}]}");
    var sources: Plan.Sources = .empty;
    defer sources.deinit(a);
    try sources.put(a, "first", .{ .json = &first });
    try sources.put(a, "second", .{ .json = &second });
    const Check = struct {
        fn run(scratch: std.mem.Allocator, output: std.mem.Allocator, p: Plan, src: Plan.Sources) !void {
            const result = try p.projectSourcesPolicy(output, scratch, src, true);
            defer output.free(result);
            try std.testing.expectEqualStrings("{\"first\":{\"items\":[{\"x\":{\"m\":1e2}}]},\"second\":{\"items\":[{\"x\":{\"m\":2e2}}]}}", result);
        }
    };
    for (0..100) |_| try Check.run(a, a, plan, sources);
    try std.testing.checkAllAllocationFailures(a, Check.run, .{ a, plan, sources });
    var quota: @import("../../lite/test_allocator.zig").BudgetAllocator = .{ .backing = a, .limit = 1024 };
    try std.testing.expectError(error.OutOfMemory, Check.run(quota.allocator(), a, plan, sources));
    try std.testing.expectEqual(@as(usize, 0), quota.live);
    try std.testing.expect(first.root.children == null);
    try std.testing.expect(second.root.children == null);
    try std.testing.expect(first.validated_ranges == null and second.validated_ranges == null);
    try std.testing.expectEqual(@as(usize, 0), owner.queryCapacity());
}

test "public graph metric borrowed wire native scoped fanout preserves ignored duplicates and failure ownership" {
    const a = std.testing.allocator;
    const columns = [_]schema.RelationalColumn{.{ .name = "payload", .path = "payload", .column_type = .json, .is_json = true, .json_kind = .any }};
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &columns };
    var layout = try codec.PhysicalLayout.init(a, table);
    defer layout.deinit();
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    var expected = std.Io.Writer.Allocating.init(a);
    defer expected.deinit();
    try input.writer.writeAll("{\"items\":[{\"x\":[");
    try expected.writer.writeAll("{\"payload\":{\"items\":[{\"x\":[");
    for (0..64) |i| {
        if (i != 0) {
            try input.writer.writeByte(',');
            try expected.writer.writeByte(',');
        }
        try input.writer.writeAll("{\"y\":1,\"ignored\":{\"d\":0,\"d\":1}}");
        try expected.writer.writeAll("{\"y\":1}");
    }
    try input.writer.writeAll("]}]}");
    try expected.writer.writeAll("]}]}}");
    var plan = try Plan.init(a, table, &layout, &.{"payload.items.x.y"});
    defer plan.deinit();
    var owner = std.heap.ArenaAllocator.init(a);
    defer owner.deinit();
    var view = JsonView.init(owner.allocator(), input.written());
    var source: Plan.Sources = .empty;
    try source.put(owner.allocator(), "payload", .{ .json = &view });
    const Check = struct {
        fn run(scratch: std.mem.Allocator, output: std.mem.Allocator, p: Plan, sources: Plan.Sources, want: []const u8) !void {
            const result = try p.projectSourcesPolicy(output, scratch, sources, true);
            defer output.free(result);
            try std.testing.expectEqualStrings(want, result);
        }
    };
    try Check.run(a, a, plan, source, expected.written());
    try std.testing.checkAllAllocationFailures(a, Check.run, .{ a, plan, source, expected.written() });
    try std.testing.expect(view.root.children == null and view.validated_ranges == null);
    var stronger = try Plan.init(a, table, &layout, &.{ "payload.items.x.y", "payload.items.x.ignored.d" });
    defer stronger.deinit();
    try std.testing.expectError(error.DuplicateField, stronger.projectSources(a, owner.allocator(), source));
}

test "public graph metric borrowed wire later large columns preserve deep native validation reuse" {
    const a = std.testing.allocator;
    const counters = @import("../../lite/test_allocator.zig");
    const columns = [_]schema.RelationalColumn{
        .{ .name = "payload", .path = "payload", .column_type = .json, .is_json = true, .json_kind = .any },
        .{ .name = "other", .path = "other", .column_type = .json, .is_json = true, .json_kind = .any },
    };
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &columns };
    var layout = try codec.PhysicalLayout.init(a, table);
    defer layout.deinit();
    var raw = std.Io.Writer.Allocating.init(a);
    defer raw.deinit();
    var field = std.Io.Writer.Allocating.init(a);
    defer field.deinit();
    try raw.writer.writeAll("{\"items\":[");
    try field.writer.writeAll("payload.items");
    for (0..32) |_| {
        try raw.writer.writeAll("{\"x\":[");
        try field.writer.writeAll(".x");
    }
    for (0..1000) |i| {
        if (i != 0) try raw.writer.writeByte(',');
        try raw.writer.writeAll("{\"absent\":0}");
    }
    for (0..32) |_| try raw.writer.writeAll("]}");
    try raw.writer.writeAll("]}");
    try field.writer.writeAll(".m");
    var large = std.Io.Writer.Allocating.init(a);
    defer large.deinit();
    try large.writer.writeAll("{\"padding\":\"");
    try large.writer.splatByteAll('a', 262144);
    try large.writer.writeAll("\"}");
    var payload = JsonView.init(a, raw.written());
    var other = JsonView.init(a, large.written());
    var plan = try Plan.init(a, table, &layout, &.{ field.written(), "other.missing" });
    defer plan.deinit();
    for ([_]bool{ false, true }) |large_first| {
        var sources: Plan.Sources = .empty;
        defer sources.deinit(a);
        if (large_first) try sources.put(a, "other", .{ .json = &other });
        try sources.put(a, "payload", .{ .json = &payload });
        if (!large_first) try sources.put(a, "other", .{ .json = &other });
        var no_resize: counters.NoResizeAllocator = .{ .backing = a };
        var budget: counters.BudgetAllocator = .{ .backing = no_resize.allocator() };
        const bytes = try plan.projectSources(a, budget.allocator(), sources);
        defer a.free(bytes);
        try std.testing.expectEqualStrings("{\"payload\":{},\"other\":{}}", bytes);
        // Clearing the first column's cache causes over 420 allocations here.
        // Leave headroom for hash collisions while catching repeated traversal.
        try std.testing.expect(budget.alloc_calls < 400);
        try std.testing.expectEqual(@as(usize, 0), budget.live);
        try std.testing.expect(payload.root.children == null and other.root.children == null);
    }
}

test "public graph metric borrowed wire native whole text and indexed vectors use bounded scratch" {
    const a = std.testing.allocator;
    const counters = @import("../../lite/test_allocator.zig");
    const columns = [_]schema.RelationalColumn{
        .{ .name = "payload", .path = "payload", .column_type = .json, .is_json = true, .json_kind = .any },
        .{ .name = "text", .path = "text", .column_type = .string },
        .{ .name = "embedding", .path = "embedding", .column_type = .dense_vector },
    };
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &columns };
    var layout = try codec.PhysicalLayout.init(a, table);
    defer layout.deinit();
    const text = try a.alloc(u8, 1024 * 1024);
    defer a.free(text);
    @memset(text, 'a');
    @memcpy(text[0..12], "\"\\\n\u{1}é中xyz");
    var raw = std.Io.Writer.Allocating.init(a);
    defer raw.deinit();
    try raw.writer.writeAll("{\"keep\":100,\"padding\":");
    try std.json.Stringify.value(text, .{}, &raw.writer);
    try raw.writer.writeByte('}');
    var vector: [1536 * 4]u8 = undefined;
    for (0..1536) |i| std.mem.writeInt(u32, vector[i * 4 ..][0..4], @bitCast(@as(f32, 0.1)), .little);
    const cells = [_]codec.Cell{
        .{ .ordinal = 0, .path = "payload", .is_json = true, .value_type = .bytes_val, .value = .{ .bytes_val = raw.written() } },
        .{ .ordinal = 1, .path = "text", .value_type = .bytes_val, .value = .{ .bytes_val = text } },
        .{ .ordinal = 2, .path = "embedding", .is_dense_vector = true, .value_type = .bytes_val, .value = .{ .bytes_val = &vector } },
    };
    const encoded = try codec.serializeOrdinal(a, 1, &columns, &cells, @splat(0));
    defer a.free(encoded);
    const row = try codec.ordinalRowViewTrusted(encoded, table, &layout);
    const selections = [_][]const []const u8{ &.{"payload"}, &.{ "payload.keep", "text" }, &.{ "payload.keep", "embedding" }, &.{ "payload.keep", "embedding.0" }, &.{"embedding.0"}, &.{"payload.padding"}, &.{"text"} };
    var legacy = std.heap.ArenaAllocator.init(a);
    defer legacy.deinit();
    var sources: Plan.Sources = .empty;
    for (&cells, &columns) |cell, column| {
        if (cell.is_json) {
            const view = try legacy.allocator().create(JsonView);
            view.* = .init(legacy.allocator(), cell.value.bytes_val);
            try sources.put(legacy.allocator(), column.name, .{ .json = view });
        } else try sources.put(legacy.allocator(), column.name, .{ .logical = try row.materializeCellAlloc(legacy.allocator(), cell) });
    }
    for (selections) |fields| {
        var plan = try Plan.init(a, table, &layout, fields);
        defer plan.deinit();
        const expected = if (plan.paths.len == 0) blk: {
            var whole: Plan.Sources = .empty;
            defer whole.deinit(a);
            for (plan.base.ordinals) |ordinal| {
                const name = columns[ordinal].name;
                try whole.put(a, name, sources.get(name).?);
            }
            break :blk try plan.projectSources(a, legacy.allocator(), whole);
        } else try plan.projectSources(a, legacy.allocator(), sources);
        defer a.free(expected);
        for ([_]bool{ false, true }) |exact| {
            var no_resize: counters.NoResizeAllocator = .{ .backing = a };
            var budget: counters.BudgetAllocator = .{ .backing = no_resize.allocator(), .limit = 16 * 1024 };
            const result = try plan.projectWithOutputPolicy(budget.allocator(), a, row, exact);
            defer a.free(result);
            try std.testing.expectEqualStrings(expected, result);
            if (plan.paths.len == 0) try std.testing.expectEqual(@as(usize, 0), budget.alloc_calls);
            try std.testing.expectEqual(@as(usize, 0), budget.live);
        }
    }
}

test "public graph metric borrowed wire borrowed native composites preserve ordered logical projections" {
    const a = std.testing.allocator;
    const arrays = @import("../../../sql/array_value.zig");
    const array_storage = @import("../../../sql/array_storage.zig");
    const columns = [_]schema.RelationalColumn{
        .{ .name = "point", .path = "point", .column_type = .geopoint },
        .{ .name = "array", .path = "array", .column_type = .sql_array, .sql_element_type = .int64 },
        .{ .name = "payload", .path = "payload", .column_type = .json, .is_json = true, .json_kind = .any },
        .{ .name = "text", .path = "text", .column_type = .string, .allows_null = true },
        .{ .name = "number", .path = "number", .column_type = .number },
    };
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &columns };
    var layout = try codec.PhysicalLayout.init(a, table);
    defer layout.deinit();
    const elements = [_]arrays.Element{ arrays.Element.json(.{ .integer = 9007199254740993 }), .{} };
    const array = try arrays.Value.init(.int64, &.{.{ .length = 2, .lower = -1 }}, &elements, .{});
    const array_bytes = try array_storage.encodeAlloc(a, array, .{});
    defer a.free(array_bytes);
    const cells = [_]codec.Cell{
        .{ .ordinal = 0, .path = "point", .value_type = .geo_point, .value = .{ .geo_point = .{ .lat = 0.1, .lon = -0.0 } } },
        .{ .ordinal = 1, .path = "array", .value_type = .bytes_val, .sql_array_element_type = .int64, .value = .{ .bytes_val = array_bytes } },
        .{ .ordinal = 2, .path = "payload", .is_json = true, .value_type = .bytes_val, .value = .{ .bytes_val = "{\"n\":100,\"s\":\"é\\n\"}" } },
        .{ .ordinal = 3, .path = "text", .value_type = .bytes_val, .is_null = true, .value = undefined },
        .{ .ordinal = 4, .path = "number", .value_type = .f64_val, .value = .{ .f64_val = 1e-20 } },
    };
    const encoded = try codec.serializeOrdinal(a, 1, &columns, &cells, @splat(0));
    defer a.free(encoded);
    const row = try codec.ordinalRowViewTrusted(encoded, table, &layout);
    var legacy = std.heap.ArenaAllocator.init(a);
    defer legacy.deinit();
    var sources: Plan.Sources = .empty;
    for (&columns, &cells) |column, cell| try sources.put(legacy.allocator(), column.name, .{ .logical = try row.materializeCellAlloc(legacy.allocator(), cell) });
    const selections = [_][]const []const u8{
        &.{ "array", "payload.n", "number", "text" },
        &.{ "array", "array.values.0", "point", "point.lat", "payload.s" },
        &.{ "array.values.0", "array.dimensions.0", "point.lon", "payload.n" },
        &.{ "point", "point.missing.x", "text.n", "payload.n" },
    };
    for (selections) |fields| {
        var plan = try Plan.init(a, table, &layout, fields);
        defer plan.deinit();
        const expected = try plan.projectSources(a, legacy.allocator(), sources);
        defer a.free(expected);
        const Check = struct {
            fn run(scratch: std.mem.Allocator, output: std.mem.Allocator, p: Plan, input: codec.OrdinalRowView, want: []const u8) !void {
                const result = try p.projectWithOutputPolicy(scratch, output, input, true);
                defer output.free(result);
                try std.testing.expectEqualStrings(want, result);
            }
        };
        try Check.run(a, a, plan, row, expected);
        try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Check.run, .{ a, plan, row, expected });
        try std.testing.checkAllAllocationFailures(a, struct {
            fn run(output: std.mem.Allocator, p: Plan, input: codec.OrdinalRowView, want: []const u8) !void {
                try Check.run(std.testing.allocator, output, p, input, want);
            }
        }.run, .{ plan, row, expected });
    }
    var typed_sources: Plan.Sources = .empty;
    var materialized: u64 = 0;
    for (&columns, &cells) |*column, cell| {
        const source = try Plan.sourceForCell(legacy.allocator(), column, cell, true);
        if (source == .native) source.native.materialized_count = &materialized;
        try typed_sources.put(legacy.allocator(), column.name, source);
    }
    var overlay = try Plan.init(a, table, &layout, &.{ "array.values.0", "array", "point.lat", "point", "payload.n" });
    defer overlay.deinit();
    const expected = try overlay.projectSources(a, legacy.allocator(), sources);
    defer a.free(expected);
    for (0..2) |_| {
        const bytes = try overlay.projectSources(a, legacy.allocator(), typed_sources);
        defer a.free(bytes);
        try std.testing.expectEqualStrings(expected, bytes);
        try std.testing.expectEqual(@as(u64, 2), materialized);
    }
}

test "public graph metric borrowed wire wide native writer snapshot releases metadata on every failure" {
    const a = std.testing.allocator;
    const names = [_][]const u8{ "a", "b", "c", "d", "e", "f", "g", "h", "i" };
    var columns: [names.len]schema.RelationalColumn = undefined;
    var cells: [names.len - 1]codec.Cell = undefined;
    for (&columns, names) |*column, name| column.* = .{ .name = name, .path = name, .column_type = .integer, .allows_null = true };
    for (&cells, 0..) |*cell, i| cell.* = .{ .ordinal = @intCast(i), .path = names[i], .value_type = .i64_val, .value = .{ .i64_val = @intCast(i) } };
    cells[cells.len - 1].is_null = true;
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &columns };
    var layout = try codec.PhysicalLayout.init(a, table);
    defer layout.deinit();
    const encoded = try codec.serializeOrdinal(a, 1, &columns, &cells, @splat(0));
    defer a.free(encoded);
    const row = try codec.ordinalRowViewSelective(encoded, table, &layout);
    var plan = try Plan.init(a, table, &layout, &names);
    defer plan.deinit();
    const Check = struct {
        fn run(alloc: std.mem.Allocator, p: Plan, input: codec.OrdinalRowView) !void {
            const expected = "{\"a\":0,\"b\":1,\"c\":2,\"d\":3,\"e\":4,\"f\":5,\"g\":6,\"h\":null}";
            for ([_]bool{ false, true }) |exact| {
                const bytes = try p.projectWithOutputPolicy(alloc, alloc, input, exact);
                defer alloc.free(bytes);
                try std.testing.expectEqualStrings(expected, bytes);
            }
            var output = std.heap.ArenaAllocator.init(alloc);
            defer output.deinit();
            const bytes = try p.projectWithOutputPolicy(alloc, output.allocator(), input, false);
            try std.testing.expectEqualStrings(expected, bytes);
        }
    };
    try Check.run(a, plan, row);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Check.run, .{ plan, row });
}
