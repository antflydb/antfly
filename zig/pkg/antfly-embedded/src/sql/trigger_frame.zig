// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Typed OLD/NEW and PostgreSQL trigger variables, owned by one statement. All
//! row writes are copy-on-write local candidates; original MVCC images remain
//! untouched. No table, network or catalog effect port is present.
const std = @import("std");
const catalog = @import("catalog.zig");
const scalar = @import("scalar.zig");
const procedure = @import("procedure.zig");
const syntax = @import("trigger_definition.zig");
const entries = @import("../system_catalog/triggers.zig");
const A = std.mem.Allocator;
pub const variable_count = 9;
pub const variable_names = [_][]const u8{ "tg_name", "tg_when", "tg_level", "tg_op", "tg_table_name", "tg_table_schema", "tg_relid", "tg_nargs", "tg_argv" };

pub const Layout = struct {
    columns: []const scalar.Column,
    row: *const catalog.Row.TypedLayout,
    width: usize,
    table_id: u64,
    table_name: []const u8,
    table_schema: []const u8,
    pub fn init(a: A, columns: []const catalog.Column, table_id: u64, table_name: []const u8, table_schema: []const u8) !*const Layout {
        // A decoding/preparation layout also serves genuine trigger absence
        // and retains opaque physical identities without manufacturing OIDs.
        if (table_id == 0) return error.InvalidSqlTriggerManifest;
        const names = try a.alloc([]const u8, columns.len);
        const fields = try a.alloc(scalar.Column, columns.len * 2 + variable_count);
        for (columns, 0..) |column, i| {
            names[i] = try a.dupe(u8, column.name);
            for ([_][]const u8{ "new", "old" }, 0..) |prefix, image| fields[image * columns.len + i] = .{
                .name = try std.mem.concat(a, u8, &.{ prefix, "\x00", column.name }),
                .type = column.type,
                .element_type = column.element_type,
                .temporal_kind = column.temporal_kind,
                .numeric_modifier = column.numeric_modifier,
                .enum_view = column.enum_view,
                .domain_view = column.domain_view,
                .nullable = true,
            };
        }
        for (variable_names, 0..) |name, ordinal| fields[columns.len * 2 + ordinal] = .{
            .name = name,
            .type = if (ordinal == 8) .array else if (ordinal == 6 or ordinal == 7) .integer else .string,
            .element_type = if (ordinal == 8) .text else if (ordinal == 6) .int64 else if (ordinal == 7) .int32 else .text,
            .nullable = false,
        };
        const result = try a.create(Layout);
        result.* = .{ .columns = fields, .row = try catalog.Row.TypedLayout.init(a, names), .width = columns.len, .table_id = table_id, .table_name = try a.dupe(u8, table_name), .table_schema = try a.dupe(u8, table_schema) };
        return result;
    }
    pub fn frame(self: *const Layout, a: A, new: ?*const catalog.Row, old: ?*const catalog.Row) !*Owned {
        const result = try a.create(Owned);
        const values = try a.alloc(scalar.Datum, self.columns.len);
        @memset(values, .{});
        for ([_]?*const catalog.Row{ new, old }, 0..) |image, index| if (image) |row| {
            for (self.row.names, 0..) |name, ordinal| values[index * self.width + ordinal] = try row.cell(name);
        };
        result.* = .{
            .layout = self,
            .frame = .{ .columns = self.columns, .values = values, .width = self.width, .new_offset = 0, .old_offset = self.width, .new = new, .old = old, .ptr = result, .assign = Owned.assign },
        };
        return result;
    }
};
pub const Owned = struct {
    layout: *const Layout,
    frame: procedure.TriggerFrame,
    candidates: [2]?*catalog.Row = .{ null, null },
    /// A function owns local record variables. Only the dispatcher-selected
    /// BEFORE ROW result survives into the next invocation; modified OLD,
    /// ignored AFTER returns and statement-local records must not leak.
    pub fn reset(self: *Owned, new: ?*const catalog.Row, old: ?*const catalog.Row) !void {
        self.candidates = .{ null, null };
        self.frame.new = new;
        self.frame.old = old;
        @memset(self.frame.values[0 .. self.layout.width * 2], .{});
        for ([_]?*const catalog.Row{ new, old }, 0..) |image, index| if (image) |row| {
            for (self.layout.row.names, 0..) |name, ordinal| self.frame.values[index * self.layout.width + ordinal] = try row.cell(name);
        };
    }
    fn assign(raw: *anyopaque, a: A, image: procedure.TriggerImage, ordinal: usize, value: scalar.Datum) !*const catalog.Row {
        const self: *Owned = @ptrCast(@alignCast(raw));
        if (ordinal >= self.layout.width) return error.InvalidSqlBackendResponse;
        const index: usize = if (image == .new) 0 else 1;
        if (self.candidates[index] == null) {
            const original = if (image == .new) self.frame.new else self.frame.old;
            const row = try a.create(catalog.Row);
            row.* = try catalog.Row.fromDatums(a, if (original) |source| source.id else "", self.layout.row, self.frame.values[index * self.layout.width ..][0..self.layout.width]);
            if (original) |source| {
                row.version = source.version;
                row.expected_content_digest = source.expected_content_digest;
                row.document_version = source.document_version;
            }
            self.candidates[index] = row;
        }
        const row = self.candidates[index].?;
        @constCast(row.typed_cells.?.values)[ordinal] = try @import("operators.zig").cloneDatum(a, value);
        return row;
    }

    pub fn setVariables(self: *Owned, a: A, entry: entries.Entry, event: syntax.Event) !void {
        // Only an actual trigger invocation exposes TG_RELID as SQL data.
        // Reject unrepresentable identities before touching any frame value;
        // absent-state preparation never reaches this conversion.
        if (self.layout.table_id > std.math.maxInt(i64)) {
            if (comptime @import("builtin").mode == .debug) std.debug.print("trigger frame rejected logical relation identity id={} exceeds_signed=true\n", .{self.layout.table_id});
            return error.InvalidSqlTriggerManifest;
        }
        const array = @import("array_value.zig");
        const values = self.frame.values[self.layout.width * 2 ..];
        values[0] = .json(.{ .string = entry.name });
        values[1] = .json(.{ .string = if (entry.timing == .before) "BEFORE" else "AFTER" });
        values[2] = .json(.{ .string = if (entry.level == .row) "ROW" else "STATEMENT" });
        values[3] = .json(.{ .string = switch (event) {
            .insert => "INSERT",
            .update => "UPDATE",
            .delete => "DELETE",
            .truncate => "TRUNCATE",
        } });
        values[4] = .json(.{ .string = self.layout.table_name });
        values[5] = .json(.{ .string = self.layout.table_schema });
        values[6] = .json(.{ .integer = @intCast(self.layout.table_id) });
        values[7] = .json(.{ .integer = @intCast(entry.arguments.len) });
        const elements = try a.alloc(scalar.Datum, entry.arguments.len);
        for (entry.arguments, elements) |argument, *cell| cell.* = .json(.{ .string = argument });
        const dimensions = try a.alloc(array.Dimension, @intFromBool(elements.len != 0));
        if (elements.len != 0) dimensions[0] = .{ .lower = 0, .length = @intCast(elements.len) };
        const argv = try a.create(array.Value);
        argv.* = try array.Value.init(.text, dimensions, elements, .{ .elements = 32, .bytes = 65536 });
        values[8] = .typedArray(argv);
    }
};

test "trigger frames preserve MVCC inputs reset local records and expose zero-based TG_ARGV" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const layout = try Layout.init(a, &.{.{ .name = "n", .path = "n", .type = .integer, .element_type = .int64 }}, 7, "usage_records", "public");
    const original = try catalog.Row.fromDatums(a, "row", layout.row, &.{.json(.{ .integer = 10 })});
    const owned = try layout.frame(a, &original, null);
    const candidate = try owned.frame.assign(owned.frame.ptr, a, .new, 0, .json(.{ .integer = 42 }));
    try std.testing.expectEqual(@as(i64, 42), (try candidate.cell("n")).value.integer);
    try std.testing.expectEqual(@as(i64, 10), (try original.cell("n")).value.integer);
    // An ignored return or a local statement record does not become the next
    // trigger's image or leave its old candidate cache writable.
    try owned.reset(null, null);
    try std.testing.expect(owned.frame.values[0].sql_null);
    try std.testing.expectEqual(@as(?*const catalog.Row, null), owned.frame.new);
    const local = try owned.frame.assign(owned.frame.ptr, a, .new, 0, .json(.{ .integer = 99 }));
    try std.testing.expect(candidate != local);
    try std.testing.expectEqual(@as(i64, 42), (try candidate.cell("n")).value.integer);
    const entry: entries.Entry = .{ .id = 1, .generation = 1, .name = "update_timestamp", .timing = .before, .events = .{ .update = true }, .level = .statement, .function = .{ .id = 9, .namespace_id = 2, .database = "antfly", .namespace = "public", .name = "touch_updated_at" }, .arguments = &.{"updated_at_ns"} };
    try owned.setVariables(a, entry, .update);
    const variables = owned.frame.values[layout.width * 2 ..];
    try std.testing.expectEqualStrings("STATEMENT", variables[2].value.string);
    try std.testing.expectEqualStrings("UPDATE", variables[3].value.string);
    try std.testing.expectEqual(@as(i64, 1), variables[7].value.integer);
    const argv = variables[8].array.?;
    try std.testing.expectEqualStrings("updated_at_ns", argv.at(&.{0}).?.value.string);
    try std.testing.expect(argv.at(&.{1}) == null);
}

test "trigger absence layout accepts opaque unsigned identity but invocation never truncates TG_RELID" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const high = std.math.maxInt(u64);
    const layout = try Layout.init(a, &.{}, high, "ordinary_graph", "public");
    const frame = try layout.frame(a, null, null);
    try std.testing.expectEqual(@as(u64, high), layout.table_id);
    // An absence-only owner uses normalization without invoking a trigger.
    for (frame.frame.values) |value| try std.testing.expect(value.sql_null);
    const entry: entries.Entry = .{ .id = 1, .generation = 1, .name = "touch", .timing = .before, .events = .{ .update = true }, .level = .statement, .function = .{ .id = 9, .namespace_id = 2, .database = "antfly", .namespace = "public", .name = "touch" } };
    try std.testing.expectError(error.InvalidSqlTriggerManifest, frame.setVariables(a, entry, .update));
    for (frame.frame.values) |value| try std.testing.expect(value.sql_null);
    try std.testing.expectError(error.InvalidSqlTriggerManifest, Layout.init(a, &.{}, 0, "invalid", "public"));
}
