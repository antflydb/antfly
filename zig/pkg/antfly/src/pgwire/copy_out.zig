// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! PostgreSQL COPY payloads, independent of CopyData frame boundaries. A single
//! reusable, bounded field buffer permits exact CSV quoting and binary lengths
//! without buffering rows/results or traversing typed values twice.
const std = @import("std");
const backend = @import("backend.zig");
const values = @import("values.zig");
pub const Options = @import("antfly_local_sources").sql_ast.CopyOptions;

test "pgwire COPY PostgreSQL byte goldens preserve NULL empty and escaped fields" {
    const Reference = struct {
        rows: []const []const std.json.Value,
        profiles: []const struct { options: Options, sql: []const u8, hex: []const u8 },
    };
    const allocator = std.testing.allocator;
    const reference = try std.json.parseFromSlice(Reference, allocator, @embedFile("fixtures/copy_out_reference.json"), .{});
    defer reference.deinit();
    for (reference.value.profiles) |profile| {
        var arena: std.heap.ArenaAllocator = .init(allocator);
        defer arena.deinit();
        var encoder = try Encoder.init(allocator, arena.allocator(), &.{ .{ .name = "id", .type = .string }, .{ .name = "status", .type = .string } }, profile.options, 1024);
        defer encoder.deinit();
        var output: std.Io.Writer.Allocating = .init(allocator);
        defer output.deinit();
        try encoder.start(&output.writer);
        const result: backend.Result = .{ .columns = encoder.columns, .rows = reference.value.rows, .command_tag = "SELECT" };
        for (0..result.rowCount()) |index| try encoder.row(arena.allocator(), &output.writer, result, index);
        try encoder.finish(&output.writer);
        const expected = try allocator.alloc(u8, profile.hex.len / 2);
        defer allocator.free(expected);
        _ = try std.fmt.hexToBytes(expected, profile.hex);
        try std.testing.expectEqualSlices(u8, expected, output.written());
        // Options parsed from actual SQL must have the same immutable meaning
        // as the format profile independently checked by PostgreSQL.
        var compiled = try @import("antfly_local_sources").sql_compiler.compile(allocator, profile.sql, .{});
        defer compiled.deinit();
        try std.testing.expectEqualDeep(profile.options, compiled.statement.copy_to.options);
    }
}

test "pgwire COPY JSONB shares canonical typed wire output including binary payloads" {
    const a = std.testing.allocator;
    const Reference = struct { value: []const u8, profiles: []const struct { format: @FieldType(Options, "format"), sql: []const u8, hex: []const u8 } };
    const reference = try std.json.parseFromSlice(Reference, a, @embedFile("fixtures/copy_out_jsonb_reference.json"), .{});
    defer reference.deinit();
    const value = try std.json.parseFromSlice(std.json.Value, a, reference.value.value, .{ .parse_numbers = false });
    defer value.deinit();
    for (reference.value.profiles) |profile| {
        var arena: std.heap.ArenaAllocator = .init(a);
        defer arena.deinit();
        var encoder = try Encoder.init(a, arena.allocator(), &.{.{ .name = "j", .type = .json }}, .{ .format = profile.format }, 4096);
        defer encoder.deinit();
        var output: std.Io.Writer.Allocating = .init(a);
        defer output.deinit();
        const result: backend.Result = .{ .columns = encoder.columns, .rows = &.{&.{value.value}}, .command_tag = "SELECT" };
        try encoder.start(&output.writer);
        try encoder.row(arena.allocator(), &output.writer, result, 0);
        try encoder.finish(&output.writer);
        const expected = try arena.allocator().alloc(u8, profile.hex.len / 2);
        _ = try std.fmt.hexToBytes(expected, profile.hex);
        try std.testing.expectEqualSlices(u8, expected, output.written());
        const allocated = try values.encode(a, .json, if (profile.format == .binary) 1 else 0, value.value);
        defer a.free(allocated);
        try std.testing.expect(std.mem.indexOf(u8, allocated, "{\"a\": [true, null], \"long\": 1.2300}") != null);
    }
}

test "pgwire COPY validates force columns and enforces per-field allocation bounds" {
    const allocator = std.testing.allocator;
    var arena: std.heap.ArenaAllocator = .init(allocator);
    defer arena.deinit();
    const columns: []const backend.Column = &.{.{ .name = "s", .type = .string }};
    try std.testing.expectError(error.UnknownColumn, Encoder.init(allocator, arena.allocator(), columns, .{ .format = .csv, .force_quote_columns = &.{"missing"} }, 64));
    var encoder = try Encoder.init(allocator, arena.allocator(), columns, .{}, 3);
    defer encoder.deinit();
    var output: std.Io.Writer.Allocating = .init(allocator);
    defer output.deinit();
    const result: backend.Result = .{ .columns = columns, .rows = &.{&.{.{ .string = "four" }}}, .command_tag = "SELECT" };
    try std.testing.expectError(error.ProgramLimitExceeded, encoder.row(arena.allocator(), &output.writer, result, 0));
    try std.testing.expectEqual(@as(usize, 0), output.written().len);
}

pub const Encoder = struct {
    options: Options,
    columns: []const backend.Column,
    force: []const bool,
    field: std.Io.Writer.Allocating,
    field_limit: usize,

    /// force belongs to the caller's operation arena; field has explicit
    /// ownership and is charged to the connection's admitted allocator.
    pub fn init(allocator: std.mem.Allocator, arena: std.mem.Allocator, columns: []const backend.Column, options: Options, field_limit: usize) !Encoder {
        try options.validate();
        if (columns.len > std.math.maxInt(i16)) return error.ProgramLimitExceeded;
        const force = try arena.alloc(bool, columns.len);
        @memset(force, options.force_quote_all);
        for (options.force_quote_columns) |column_name| {
            var found = false;
            for (columns, 0..) |column, index| if (std.mem.eql(u8, column.name, column_name)) {
                force[index] = true;
                found = true;
            };
            if (!found) return error.UnknownColumn;
        }
        for (columns) |column| _ = try values.columnOid(column);
        return .{ .options = options, .columns = columns, .force = force, .field = .init(allocator), .field_limit = field_limit };
    }

    pub fn deinit(self: *Encoder) void {
        self.field.deinit();
    }

    pub fn start(self: *Encoder, writer: *std.Io.Writer) !void {
        if (self.options.format == .binary) {
            try writer.writeAll("PGCOPY\n\xff\r\n\x00");
            try writer.writeInt(i32, 0, .big);
            try writer.writeInt(i32, 0, .big);
        } else if (self.options.header) {
            for (self.columns, 0..) |column, index| {
                if (index != 0) try writer.writeByte(self.options.delimiterByte());
                try self.text(writer, column.name, false);
            }
            try writer.writeByte('\n');
        }
    }

    pub fn finish(self: *Encoder, writer: *std.Io.Writer) !void {
        if (self.options.format == .binary) try writer.writeInt(i16, -1, .big);
    }

    pub fn row(self: *Encoder, allocator: std.mem.Allocator, writer: *std.Io.Writer, result: backend.Result, row_index: usize) !void {
        if (self.options.format == .binary) try writer.writeInt(i16, @intCast(self.columns.len), .big);
        for (self.columns, 0..) |column, index| {
            if (index != 0 and self.options.format != .binary) try writer.writeByte(self.options.delimiterByte());
            const cell = try result.cell(allocator, row_index, index);
            if (cell.sql_null) {
                if (cell.value != .null or cell.array != null or cell.numeric != null) return error.InvalidResult;
                if (self.options.format == .binary) try writer.writeInt(i32, -1, .big) else try writer.writeAll(self.options.nullBytes());
                continue;
            }
            self.field.writer.end = 0;
            try values.encodeCellInto(allocator, &self.field.writer, column, if (self.options.format == .binary) 1 else 0, cell, self.field_limit);
            const bytes = self.field.written();
            if (self.options.format == .binary) {
                if (bytes.len > std.math.maxInt(i32)) return error.ProgramLimitExceeded;
                try writer.writeInt(i32, @intCast(bytes.len), .big);
                try writer.writeAll(bytes);
            } else try self.text(writer, bytes, self.force[index]);
        }
        if (self.options.format != .binary) try writer.writeByte('\n');
        // Large outliers must not become permanent per-connection residency.
        if (self.field.writer.buffer.len > 64 * 1024) {
            const allocator_ = self.field.allocator;
            self.field.deinit();
            self.field = .init(allocator_);
        }
    }

    fn text(self: *const Encoder, writer: *std.Io.Writer, bytes: []const u8, force: bool) !void {
        if (self.options.format == .csv) {
            const quote = self.options.quoteByte();
            const escape = self.options.escapeByte();
            const quoted = force or std.mem.eql(u8, bytes, self.options.nullBytes()) or
                (self.columns.len == 1 and std.mem.eql(u8, bytes, "\\.")) or std.mem.indexOfAny(u8, bytes, &.{ self.options.delimiterByte(), quote, '\n', '\r' }) != null;
            if (!quoted) return writer.writeAll(bytes);
            try writer.writeByte(quote);
            var begin: usize = 0;
            for (bytes, 0..) |byte, index| if (byte == quote or byte == escape) {
                try writer.writeAll(bytes[begin..index]);
                try writer.writeByte(escape);
                try writer.writeByte(byte);
                begin = index + 1;
            };
            try writer.writeAll(bytes[begin..]);
            try writer.writeByte(quote);
        } else {
            var begin: usize = 0;
            for (bytes, 0..) |byte, index| {
                const escaped: ?u8 = switch (byte) {
                    '\\' => '\\',
                    '\n' => 'n',
                    '\r' => 'r',
                    '\t' => 't',
                    8 => 'b',
                    12 => 'f',
                    11 => 'v',
                    else => if (byte == self.options.delimiterByte()) byte else null,
                };
                if (escaped) |code| {
                    try writer.writeAll(bytes[begin..index]);
                    try writer.writeByte('\\');
                    try writer.writeByte(code);
                    begin = index + 1;
                }
            }
            try writer.writeAll(bytes[begin..]);
        }
    }
};
