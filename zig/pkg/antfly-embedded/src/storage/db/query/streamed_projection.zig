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

//! Producers borrow snapshot-owned rows/pages through a synchronous callback.
const std = @import("std");
const Plan = @import("relational_projection.zig").Plan;
const codec = @import("../algebraic/relational_row_codec.zig");
const document = @import("../document_query.zig");
const native_writer = @import("../../native_writer.zig");
pub const Context = struct {
    scratch: std.mem.Allocator,
    value: union(enum) {
        row: struct { plan: Plan, row: codec.OrdinalRowView },
        whole_row: codec.OrdinalRowView,
        sources: struct { plan: Plan, sources: Plan.Sources },
        document: struct { plan: *const document.SpanProjectionPlan, raw: []const u8 },
        json_fields: []const u8,
    },
    pub fn borrow(self: *@This()) @import("../types.zig").BorrowedScanDocument {
        return .{ .context = self, .write_fields_fn = writeFields, .count_projection_fn = countProjection };
    }
    fn isPresence(fields: []const []const u8, name: []const u8) bool {
        for (fields) |field| if (std.mem.eql(u8, field, name)) return true;
        return false;
    }
    fn rawValue(a: std.mem.Allocator, raw: []const u8, presence: bool) !std.json.Value {
        if (presence) {
            const trimmed = std.mem.trim(u8, raw, " \r\n\t");
            if (trimmed.len != 0 and (trimmed[0] == '{' or trimmed[0] == '[')) {
                const JsonView = @import("json_view.zig").View;
                var view = JsonView.init(a, raw);
                const selector: @import("json_projection_plan.zig").Selector = .{ .prepared = true };
                try view.prepareProjectionWithOptions(&selector, .all, .any_container, .{ .defer_fanout = true });
            }
            try @import("cypher").value.validateJsonSlice(a, raw, 1);
            return if (std.mem.eql(u8, std.mem.trim(u8, raw, " \r\n\t"), "null")) .null else .{ .bool = true };
        }
        return std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{ .parse_numbers = false });
    }
    fn countProjection(ptr: ?*anyopaque, a: std.mem.Allocator, presence: []const []const u8) !?std.json.Value {
        const self: *@This() = @ptrCast(@alignCast(ptr orelse return error.InvalidArgument));
        var result = std.json.ObjectMap.empty;
        switch (self.value) {
            .document => |v| {
                if (v.plan.include_all or v.plan.passthrough) return null;
                for (v.plan.paths) |path| if (path.parts.len != 1) return null;
                const JsonView = @import("json_view.zig").View;
                var view = JsonView.init(a, v.raw);
                // Keep the ordinary scan's syntax and duplicate-key checks;
                // count-only values are validated separately in Cypher's domain.
                try view.prepareProjectionWithOptions(v.plan.selector, .all, .object_only, .{ .defer_fanout = true });
                for (v.plan.paths) |path| {
                    const name = path.parts[0];
                    if (try view.field(&view.root, name)) |node| try result.put(a, name, try rawValue(a, node.raw, isPresence(presence, name)));
                }
            },
            .row => |v| {
                if (v.plan.paths.len != 0) return null;
                if (v.plan.base.schema_version != v.row.table_schema.version) return error.RelationalRowSchemaMismatch;
                for (v.plan.base.ordinals) |ordinal| {
                    const column = v.row.table_schema.relational_columns[ordinal];
                    const cell = (try v.row.findCell(ordinal)) orelse continue;
                    const value: std.json.Value = if (cell.is_null) .null else if (cell.is_json)
                        try rawValue(a, cell.value.bytes_val, isPresence(presence, column.name))
                    else if (isPresence(presence, column.name) and (column.column_type == .string or column.column_type == .blob or column.column_type == .geoshape))
                        .{ .bool = true }
                    else
                        try codec.borrowedJsonValueFromCellAlloc(a, column, cell);
                    try result.put(a, column.name, value);
                }
            },
            else => return null,
        }
        return .{ .object = result };
    }
    fn writeFields(ptr: ?*anyopaque, destination: native_writer.Borrow) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr orelse return error.InvalidArgument));
        var receiver = destination.receive();
        const writer = receiver.writer();
        switch (self.value) {
            .row => |v| try v.plan.writeRowFields(self.scratch, v.row, writer),
            .whole_row => |row| try row.writeScanFields(self.scratch, writer),
            .sources => |v| try v.plan.writeSourcesFields(self.scratch, v.sources, writer),
            .document => |v| try document.writeLookupFields(self.scratch, writer, v.raw, v.plan),
            .json_fields => |raw| try document.writeRawScanFields(self.scratch, writer, raw),
        }
        try receiver.finish();
    }
};

test "public graph metric borrowed wire synchronous scan projection streams bounded fields and retries after failure" {
    const a = std.testing.allocator;
    const text = try a.alloc(u8, 1024 * 1024);
    defer a.free(text);
    @memset(text, 'x');
    @memcpy(text[0..12], "\"\\\n\u{1}é中xyz");
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    try input.writer.writeAll("{\"_id\":\"stored\",\"title\":");
    try std.json.Stringify.value(text, .{}, &input.writer);
    try input.writer.writeAll(",\"nested\":{\"_id\":\"inner\",\"n\":1e2}}");
    const opts: @import("../types.zig").LookupOptions = .{ .fields = &.{ "_id", "title", "nested" }, .include_all_fields = false };
    var plan = (try document.SpanProjectionPlan.init(a, opts)).?;
    defer plan.deinit();
    var legacy = try document.lookupJson(a, input.written(), opts);
    defer legacy.deinit(a);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, legacy.json, .{});
    defer parsed.deinit();
    _ = parsed.value.object.swapRemove("_id");
    // Preserve projection order while dropping only the root _id.
    const want_title = parsed.value.object.get("title").?;
    const want_nested = parsed.value.object.get("nested").?;
    var expected = std.Io.Writer.Allocating.init(a);
    defer expected.deinit();
    try expected.writer.writeAll(",\"title\":");
    try std.json.Stringify.value(want_title, .{}, &expected.writer);
    try expected.writer.writeAll(",\"nested\":");
    try std.json.Stringify.value(want_nested, .{}, &expected.writer);
    var no_resize: @import("../../lite/test_allocator.zig").NoResizeAllocator = .{ .backing = a };
    var budget: @import("../../lite/test_allocator.zig").BudgetAllocator = .{ .backing = no_resize.allocator(), .limit = 32 * 1024 };
    var context: Context = .{ .scratch = budget.allocator(), .value = .{ .document = .{ .raw = input.written(), .plan = &plan } } };
    var failed = std.Io.Writer.fixed(&.{});
    try std.testing.expectError(error.WriteFailed, context.borrow().writeFields(&failed));
    for (0..2) |_| {
        var output = std.Io.Writer.Allocating.init(a);
        defer output.deinit();
        try context.borrow().writeFields(&output.writer);
        try std.testing.expectEqualStrings(expected.written(), output.written());
        try std.testing.expectEqual(@as(usize, 0), budget.live);
    }
}

test "public graph metric borrowed wire full row producers avoid reconstruction and preserve identity normalization" {
    const a = std.testing.allocator;
    const schema = @import("../../schema.zig");
    const columns = [_]schema.RelationalColumn{
        .{ .name = "text", .path = "text", .column_type = .string },
        .{ .name = "payload", .path = "payload", .column_type = .json, .is_json = true, .json_kind = .any },
        .{ .name = "ratio", .path = "ratio", .column_type = .number },
    };
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &columns };
    var layout = try codec.PhysicalLayout.init(a, table);
    defer layout.deinit();
    const large = try a.alloc(u8, 1024 * 1024);
    defer a.free(large);
    @memset(large, 'x');
    for ([_][]const u8{ "{\"n\":100}", "{\"_id\":\"inner\",\"n\":100}" }) |json| {
        const bytes = try codec.serializeOrdinal(a, 1, &columns, &.{
            .{ .ordinal = 0, .path = "text", .value_type = .bytes_val, .value = .{ .bytes_val = large } },
            .{ .ordinal = 1, .path = "payload", .is_json = true, .value_type = .bytes_val, .value = .{ .bytes_val = json } },
            .{ .ordinal = 2, .path = "ratio", .value_type = .f64_val, .value = .{ .f64_val = 1e-12 } },
        }, @splat(0));
        defer a.free(bytes);
        const row = try codec.ordinalRowViewTrusted(bytes, table, &layout);
        const legacy = try row.reconstructValueAlloc(a);
        defer a.free(legacy);
        var expected = std.Io.Writer.Allocating.init(a);
        defer expected.deinit();
        try document.writeRawScanFields(a, &expected.writer, legacy);
        var budget: @import("../../lite/test_allocator.zig").BudgetAllocator = .{ .backing = a, .limit = if (std.mem.indexOf(u8, json, "_id") == null) 0 else 8 * 1024 * 1024 };
        var context: Context = .{ .scratch = budget.allocator(), .value = .{ .whole_row = row } };
        var actual = std.Io.Writer.Allocating.init(a);
        defer actual.deinit();
        try context.borrow().writeFields(&actual.writer);
        try std.testing.expectEqualStrings(expected.written(), actual.written());
        try std.testing.expectEqual(@as(usize, 0), budget.live);
        var failed = std.Io.Writer.fixed(&.{});
        try std.testing.expectError(error.WriteFailed, context.borrow().writeFields(&failed));
        try std.testing.expectEqual(@as(usize, 0), budget.live);
    }
}

test "public graph metric borrowed wire owner COUNT validates wide values without materializing payloads" {
    const a = std.testing.allocator;
    var raw = std.Io.Writer.Allocating.init(a);
    defer raw.deinit();
    try raw.writer.writeAll("{\"tenant\":\"acme\",\"body\":\"\\u0041");
    try raw.writer.splatByteAll('x', 2 << 20);
    try raw.writer.writeAll("\",\"zero\":0,\"false\":false,\"array\":[],\"object\":{},\"nil\":null}");
    var plan = (try document.SpanProjectionPlan.init(a, .{ .fields = &.{ "tenant", "body", "zero", "false", "array", "object", "nil", "missing" }, .include_all_fields = false })).?;
    defer plan.deinit();
    var budget: @import("../../lite/test_allocator.zig").BudgetAllocator = .{ .backing = a, .limit = 32 << 10 };
    {
        var arena = std.heap.ArenaAllocator.init(budget.allocator());
        defer arena.deinit();
        var context: Context = .{ .scratch = arena.allocator(), .value = .{ .document = .{ .raw = raw.written(), .plan = &plan } } };
        const value = (try context.borrow().countProjection(arena.allocator(), &.{ "body", "zero", "false", "array", "object", "nil", "missing" })).?;
        try std.testing.expectEqualStrings("acme", value.object.get("tenant").?.string);
        for ([_][]const u8{ "body", "zero", "false", "array", "object" }) |field| try std.testing.expect(value.object.get(field).?.bool);
        try std.testing.expect(value.object.get("nil").? == .null);
        try std.testing.expect(value.object.get("missing") == null);
    }
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    for ([_][]const u8{ "{\"body\":1e999}", "{\"body\":9223372036854775808}" }) |invalid| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var context: Context = .{ .scratch = arena.allocator(), .value = .{ .document = .{ .raw = invalid, .plan = &plan } } };
        try std.testing.expectError(error.CypherNumericOverflow, context.borrow().countProjection(arena.allocator(), &.{"body"}));
    }
}

test "public graph metric borrowed wire owner COUNT validates relational JSON spans within bounded scratch" {
    const a = std.testing.allocator;
    const schema = @import("../../schema.zig");
    const columns = [_]schema.RelationalColumn{
        .{ .name = "body", .path = "body", .column_type = .json, .is_json = true, .json_kind = .any },
    };
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &columns };
    var layout = try codec.PhysicalLayout.init(a, table);
    defer layout.deinit();
    var plan = try Plan.init(a, table, &layout, &.{"body"});
    defer plan.deinit();
    var raw = std.Io.Writer.Allocating.init(a);
    defer raw.deinit();
    try raw.writer.writeAll("{\"nested\":\"\\u0041");
    try raw.writer.splatByteAll('x', 2 << 20);
    try raw.writer.writeAll("\"}");
    for ([_][]const u8{ raw.written(), "null", "false", "0", "[]", "{}", "{\"x\":1e999}", "{\"x\":0,\"x\":1}" }) |json| {
        // Construct checksum-valid fixtures directly, including noncanonical
        // escapes and invalid semantic domains, to exercise read validation.
        const bytes = try codec.serializePreparedOrdinal(a, 1, &columns, &.{.{ .ordinal = 0, .path = "body", .is_json = true, .value_type = .bytes_val, .value = .{ .bytes_val = json } }}, @splat(0));
        defer a.free(bytes);
        const row = try codec.ordinalRowViewTrusted(bytes, table, &layout);
        var budget: @import("../../lite/test_allocator.zig").BudgetAllocator = .{ .backing = a, .limit = 32 << 10 };
        {
            var arena = std.heap.ArenaAllocator.init(budget.allocator());
            defer arena.deinit();
            var context: Context = .{ .scratch = arena.allocator(), .value = .{ .row = .{ .plan = plan, .row = row } } };
            const result = context.borrow().countProjection(arena.allocator(), &.{"body"});
            if (std.mem.indexOf(u8, json, "1e999") != null) {
                try std.testing.expectError(error.CypherNumericOverflow, result);
            } else if (std.mem.eql(u8, json, "{\"x\":0,\"x\":1}")) {
                try std.testing.expectError(error.DuplicateField, result);
            } else {
                const value = (try result).?.object.get("body").?;
                if (std.mem.eql(u8, json, "null")) try std.testing.expect(value == .null) else try std.testing.expect(value.bool);
            }
        }
        try std.testing.expectEqual(@as(usize, 0), budget.live);
    }
}
