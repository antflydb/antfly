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

//! Bounded incremental COPY framing. Rows borrow one reusable record buffer;
//! wire frames may end anywhere, including inside quoted fields or length words.
const std = @import("std");
const Options = @import("copy_out.zig").Options;
pub const Field = ?[]const u8;
pub const Decoder = struct {
    alloc: std.mem.Allocator,
    options: Options,
    width: usize,
    limit: usize,
    record: std.ArrayList(u8) = .empty,
    fields: std.ArrayList(Field) = .empty,
    defaults: std.ArrayList(bool) = .empty,
    force: std.ArrayList(u8) = .empty,
    columns: []const []const u8 = &.{},
    ranges: std.ArrayList(?struct { start: usize, len: usize }) = .empty,
    quoted: bool = false,
    escaped: bool = false,
    quote_pending: bool = false,
    skip_lf: bool = false,
    first_cr: bool = false,
    ending: enum { unknown, lf, cr, crlf } = .unknown,
    header: bool,
    ended: bool = false,
    binary_header: bool = false,
    binary_offset: usize = 0,
    binary_fields: usize = 0,
    binary_length: ?usize = null,

    pub fn init(a: std.mem.Allocator, options: Options, width: usize, limit: usize) !Decoder {
        try options.validate();
        if (width == 0 or width > std.math.maxInt(i16)) return error.ProgramLimitExceeded;
        return .{ .alloc = a, .options = options, .width = width, .limit = limit, .header = options.header };
    }
    /// Names and masks are pinned once before CopyInResponse, never searched
    /// per field. Caller owns the immutable names for the decoder lifetime.
    pub fn setColumns(self: *Decoder, columns: []const []const u8) !void {
        if (columns.len != self.width) return error.InvalidCopyData;
        self.columns = columns;
        try self.force.resize(self.alloc, columns.len);
        @memset(self.force.items, @as(u8, if (self.options.force_null_all) 1 else 0) | @as(u8, if (self.options.force_not_null_all) 2 else 0));
        inline for (.{ "force_null_columns", "force_not_null_columns" }, 0..) |field, bit| {
            for (@field(self.options, field)) |name| {
                var found = false;
                for (columns, 0..) |column, i| if (std.mem.eql(u8, column, name)) {
                    self.force.items[i] |= @as(u8, 1) << bit;
                    found = true;
                    break;
                };
                if (!found) return error.UnknownColumn;
            }
        }
    }
    pub fn deinit(self: *Decoder) void {
        self.record.deinit(self.alloc);
        self.fields.deinit(self.alloc);
        self.defaults.deinit(self.alloc);
        self.force.deinit(self.alloc);
        self.ranges.deinit(self.alloc);
    }
    /// A returned row remains valid until the next call. Caller advances input
    /// by reference, consumes the row, then supplies the unconsumed suffix.
    pub fn next(self: *Decoder, input: *[]const u8) !?[]const Field {
        if (self.fields.items.len != 0 and self.options.format != .binary) {
            self.fields.clearRetainingCapacity();
            self.defaults.clearRetainingCapacity();
            self.record.clearRetainingCapacity();
        } else if (self.options.format == .binary and self.binary_fields == self.width) {
            self.fields.clearRetainingCapacity();
            self.defaults.clearRetainingCapacity();
            self.record.clearRetainingCapacity();
            self.binary_offset = 0;
            self.binary_fields = 0;
            self.ranges.clearRetainingCapacity();
        }
        if (self.ended and self.options.format == .text) {
            input.* = &.{};
            return null;
        }
        while (input.len != 0) {
            const ch = input.*[0];
            input.* = input.*[1..];
            if (self.ended) return error.InvalidCopyData;
            if (self.skip_lf) {
                self.skip_lf = false;
                if (ch == '\n') {
                    if (self.ending == .cr and !self.first_cr) return error.InvalidCopyData;
                    self.ending = .crlf;
                    continue;
                }
                if (self.ending == .crlf) return error.InvalidCopyData;
            }
            if (self.options.format == .binary) {
                try self.append(ch);
                if (try self.binary()) |row| return row;
                continue;
            }
            var delimiter = false;
            if (self.options.format == .text) {
                if (self.escaped) self.escaped = false else if (ch == '\\') self.escaped = true else delimiter = ch == '\n' or ch == '\r';
            } else {
                if (self.quote_pending) {
                    self.quote_pending = false;
                    if (ch == self.options.quoteByte()) {
                        try self.append(ch);
                        continue;
                    }
                    self.quoted = false;
                }
                if (self.escaped) self.escaped = false else if (self.quoted) {
                    if (ch == self.options.escapeByte() and ch != self.options.quoteByte()) self.escaped = true else if (ch == self.options.quoteByte()) self.quote_pending = true;
                } else if (ch == self.options.quoteByte()) self.quoted = true else delimiter = ch == '\n' or ch == '\r';
            }
            if (delimiter) {
                if (ch == '\r') {
                    if (self.ending == .lf) return error.InvalidCopyData;
                    self.first_cr = self.ending == .unknown;
                    if (self.first_cr) self.ending = .cr;
                    self.skip_lf = true;
                } else {
                    if (self.ending != .unknown and self.ending != .lf) return error.InvalidCopyData;
                    self.ending = .lf;
                }
                if (self.options.format == .text and std.mem.eql(u8, self.record.items, "\\.")) {
                    self.ended = true;
                    self.record.clearRetainingCapacity();
                    input.* = &.{};
                    return null;
                }
                if (self.header) {
                    try self.consumeHeader();
                    continue;
                }
                return try self.parseRecord();
            }
            try self.append(ch);
        }
        return null;
    }
    pub fn finish(self: *Decoder) !?[]const Field {
        if (self.options.format == .binary) {
            if (!self.ended) return error.InvalidCopyData;
            return null;
        }
        if (self.skip_lf and self.ending == .crlf) return error.InvalidCopyData;
        if (self.quoted and !self.quote_pending) return error.InvalidCopyData;
        // MATCH must see an actual header, including on an empty stream.
        if (self.header and self.options.header_match and self.record.items.len == 0) return error.InvalidCopyData;
        // A row returned by next has already been delivered.
        if (self.fields.items.len != 0 or self.record.items.len == 0) return null;
        if (self.options.format == .text and std.mem.eql(u8, self.record.items, "\\.")) return null;
        if (self.header) {
            try self.consumeHeader();
            return null;
        }
        return try self.parseRecord();
    }
    fn consumeHeader(self: *Decoder) !void {
        if (self.options.header_match) {
            if (self.columns.len != self.width) return error.InvalidCopyData;
            const fields = try self.parseRecord();
            for (fields, self.columns, 0..) |field, name, i| {
                if ((self.defaults.items.len != 0 and self.defaults.items[i]) or field == null or !std.mem.eql(u8, field.?, name)) return error.InvalidCopyData;
            }
        }
        self.header = false;
        self.fields.clearRetainingCapacity();
        self.defaults.clearRetainingCapacity();
        self.record.clearRetainingCapacity();
    }
    fn append(self: *Decoder, ch: u8) !void {
        if (self.record.items.len >= self.limit) return error.ProgramLimitExceeded;
        try self.record.append(self.alloc, ch);
    }
    fn parseRecord(self: *Decoder) ![]const Field {
        // Decode in place: output never overtakes the scanned source. Fields
        // retain independent NULL provenance, including quoted CSV empties.
        var read: usize = 0;
        var write: usize = 0;
        while (true) {
            const start = write;
            const raw_start = read;
            const nulls = self.options.nullBytes();
            const text_null = self.options.format == .text and std.mem.startsWith(u8, self.record.items[raw_start..], nulls) and (self.record.items.len - raw_start == nulls.len or self.record.items[raw_start + nulls.len] == self.options.delimiterByte());
            const text_default = if (self.options.default_string) |marker| self.options.format == .text and std.mem.startsWith(u8, self.record.items[raw_start..], marker) and (self.record.items.len - raw_start == marker.len or self.record.items[raw_start + marker.len] == self.options.delimiterByte()) else false;
            var in_quote = false;
            var was_quoted = false;
            while (read < self.record.items.len) {
                var ch = self.record.items[read];
                if (self.options.format == .csv) {
                    if (!in_quote and ch == self.options.delimiterByte()) break;
                    if (ch == self.options.quoteByte()) {
                        was_quoted = true;
                        if (in_quote and read + 1 < self.record.items.len and self.record.items[read + 1] == ch and self.options.escapeByte() == ch) {
                            read += 2;
                            self.record.items[write] = ch;
                            write += 1;
                            continue;
                        }
                        in_quote = !in_quote;
                        read += 1;
                        continue;
                    }
                    if (in_quote and ch == self.options.escapeByte() and read + 1 < self.record.items.len and (self.record.items[read + 1] == self.options.quoteByte() or self.record.items[read + 1] == ch)) {
                        read += 1;
                        ch = self.record.items[read];
                    }
                    read += 1;
                } else {
                    if (ch == self.options.delimiterByte()) break;
                    read += 1;
                    if (ch == '\\') {
                        if (read == self.record.items.len) break;
                        ch = self.record.items[read];
                        read += 1;
                        ch = switch (ch) {
                            '.' => return error.InvalidCopyData,
                            'b' => 8,
                            'f' => 12,
                            'n' => 10,
                            'r' => 13,
                            't' => 9,
                            'v' => 11,
                            '0'...'7' => blk: {
                                var n: u16 = ch - '0';
                                var count: usize = 1;
                                while (count < 3 and read < self.record.items.len and self.record.items[read] >= '0' and self.record.items[read] <= '7') : (count += 1) {
                                    n = n * 8 + self.record.items[read] - '0';
                                    read += 1;
                                }
                                break :blk @truncate(n);
                            },
                            'x' => blk: {
                                var n: u8 = 0;
                                var count: usize = 0;
                                while (count < 2 and read < self.record.items.len) : (count += 1) {
                                    const digit = std.fmt.charToDigit(self.record.items[read], 16) catch break;
                                    n = n * 16 + digit;
                                    read += 1;
                                }
                                break :blk if (count == 0) @as(u8, 'x') else n;
                            },
                            else => ch,
                        };
                    }
                }
                self.record.items[write] = ch;
                write += 1;
            }
            if (in_quote) return error.InvalidCopyData;
            const bytes = self.record.items[start..write];
            const index = self.fields.items.len;
            const mask = if (!self.header and index < self.force.items.len) self.force.items[index] else @as(u8, 0);
            const null_match = std.mem.eql(u8, bytes, self.options.nullBytes());
            const is_null = if (self.options.format == .csv) null_match and (if (was_quoted) mask & 1 != 0 else mask & 2 == 0) else text_null;
            // PostgreSQL compares unquoted raw input with DEFAULT, before
            // text backslash decoding, independently of the NULL marker.
            const is_default = if (self.options.default_string) |marker| !was_quoted and !is_null and (if (self.options.format == .csv) std.mem.eql(u8, bytes, marker) else text_default) else false;
            if (!is_null and (std.mem.indexOfScalar(u8, bytes, 0) != null or !std.unicode.utf8ValidateSlice(bytes))) return error.SqlInvalidTextEncoding;
            if (self.fields.items.len == self.width) return error.InvalidCopyData;
            try self.fields.append(self.alloc, if (is_null) null else bytes);
            try self.defaults.append(self.alloc, is_default);
            if (read == self.record.items.len) break;
            read += 1;
        }
        if (self.fields.items.len != self.width) return error.InvalidCopyData;
        return self.fields.items;
    }
    fn binary(self: *Decoder) !?[]const Field {
        const bytes = self.record.items;
        if (!self.binary_header) {
            if (bytes.len < 19) return null;
            if (!std.mem.eql(u8, bytes[0..11], "PGCOPY\n\xff\r\n\x00")) return error.InvalidCopyData;
            if (std.mem.readInt(u32, bytes[11..15], .big) & 0xffff0000 != 0) return error.InvalidCopyData;
            const extension = std.mem.readInt(i32, bytes[15..19], .big);
            if (extension < 0) return error.InvalidCopyData;
            const end = std.math.add(usize, 19, @intCast(extension)) catch return error.ProgramLimitExceeded;
            if (end > self.limit) return error.ProgramLimitExceeded;
            if (bytes.len < end) return null;
            self.binary_header = true;
            self.record.clearRetainingCapacity();
            return null;
        }
        if (self.binary_offset == 0) {
            if (bytes.len < 2) return null;
            const count = std.mem.readInt(i16, bytes[0..2], .big);
            if (count == -1) {
                self.ended = true;
                self.record.clearRetainingCapacity();
                return null;
            }
            if (count != self.width) return error.InvalidCopyData;
            self.binary_offset = 2;
        }
        if (self.binary_length == null) {
            if (bytes.len < self.binary_offset + 4) return null;
            const length = std.mem.readInt(i32, bytes[self.binary_offset..][0..4], .big);
            self.binary_offset += 4;
            if (length < -1) return error.InvalidCopyData;
            if (length == -1) {
                try self.ranges.append(self.alloc, null);
                self.binary_fields += 1;
            } else {
                const end = std.math.add(usize, self.binary_offset, @intCast(length)) catch return error.ProgramLimitExceeded;
                if (end > self.limit) return error.ProgramLimitExceeded;
                self.binary_length = @intCast(length);
            }
        }
        if (self.binary_length) |length| {
            if (bytes.len < self.binary_offset + length) return null;
            try self.ranges.append(self.alloc, .{ .start = self.binary_offset, .len = length });
            self.binary_offset += length;
            self.binary_length = null;
            self.binary_fields += 1;
        }
        if (self.binary_fields == self.width) {
            for (self.ranges.items) |range| try self.fields.append(self.alloc, if (range) |span| bytes[span.start..][0..span.len] else null);
            return self.fields.items;
        }
        return null;
    }
};

test "pgwire COPY input consumes every frame split with bounded owned record storage" {
    const a = std.testing.allocator;
    const columns = [_]@import("backend.zig").Column{ .{ .name = "id", .type = .string }, .{ .name = "value", .type = .string } };
    const rows: []const []const std.json.Value = &.{ &.{ .{ .string = "first" }, .{ .string = "a\tb\nc\\d\r\"é" } }, &.{ .null, .{ .string = "" } }, &.{ .{ .string = "\\N" }, .{ .string = "last" } } };
    for ([_]Options{ .{}, .{ .format = .csv }, .{ .format = .csv, .header = true, .escape = '!', .null_string = "nil" }, .{ .format = .binary } }) |options| {
        var arena: std.heap.ArenaAllocator = .init(a);
        defer arena.deinit();
        var encoder = try @import("copy_out.zig").Encoder.init(a, arena.allocator(), &columns, options, 4096);
        defer encoder.deinit();
        var output: std.Io.Writer.Allocating = .init(a);
        defer output.deinit();
        try encoder.start(&output.writer);
        for (rows, 0..) |_, i| try encoder.row(arena.allocator(), &output.writer, .{ .columns = &columns, .rows = rows, .command_tag = "SELECT" }, i);
        try encoder.finish(&output.writer);
        for (1..output.written().len + 1) |chunk_size| {
            var decoder = try Decoder.init(a, options, columns.len, 4096);
            defer decoder.deinit();
            var seen: usize = 0;
            var offset: usize = 0;
            while (offset < output.written().len) {
                const end = @min(offset + chunk_size, output.written().len);
                var chunk: []const u8 = output.written()[offset..end];
                while (try decoder.next(&chunk)) |actual| {
                    try std.testing.expect(seen < rows.len);
                    for (actual, rows[seen]) |field, expected| switch (expected) {
                        .null => try std.testing.expect(field == null),
                        .string => |bytes| try std.testing.expectEqualStrings(bytes, field orelse return error.UnexpectedNull),
                        else => unreachable,
                    };
                    seen += 1;
                }
                offset = end;
            }
            try std.testing.expect((try decoder.finish()) == null);
            try std.testing.expectEqual(rows.len, seen);
        }
    }
}

test "pgwire COPY text input distinguishes raw NULL escapes CRLF and unterminated final rows" {
    const a = std.testing.allocator;
    var decoder = try Decoder.init(a, .{}, 2, 1024);
    defer decoder.deinit();
    var bytes: []const u8 = "\\N\t\\116\r\n\\\\N\t\\x4e\\101\r\nlast\trow";
    var row = (try decoder.next(&bytes)).?;
    try std.testing.expect(row[0] == null);
    try std.testing.expectEqualStrings("N", row[1].?);
    row = (try decoder.next(&bytes)).?;
    try std.testing.expectEqualStrings("\\N", row[0].?);
    try std.testing.expectEqualStrings("NA", row[1].?);
    try std.testing.expect((try decoder.next(&bytes)) == null);
    row = (try decoder.finish()).?;
    try std.testing.expectEqualStrings("last", row[0].?);
    try std.testing.expectEqualStrings("row", row[1].?);
}

test "pgwire COPY input rejects malformed records binary framing and row amplification before admission" {
    const a = std.testing.allocator;
    for ([_]struct { options: Options, bytes: []const u8, finish: bool = false }{
        .{ .options = .{}, .bytes = "one\n" },
        .{ .options = .{}, .bytes = "one\ttwo\tthree\n" },
        .{ .options = .{ .format = .csv }, .bytes = "a,\"unterminated", .finish = true },
        .{ .options = .{ .format = .binary }, .bytes = "not a binary stream!" },
        .{ .options = .{ .format = .binary }, .bytes = "PGCOPY\n\xff\r\n\x00\x00\x01\x00\x00\x00\x00\x00\x00" },
        .{ .options = .{ .format = .binary }, .bytes = "PGCOPY\n\xff\r\n\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01" },
        .{ .options = .{ .format = .binary }, .bytes = "PGCOPY\n\xff\r\n\x00\x00\x00\x00\x00\x00\x00\x00\x00", .finish = true },
    }) |profile| {
        var decoder = try Decoder.init(a, profile.options, 2, 128);
        defer decoder.deinit();
        var input = profile.bytes;
        if (profile.finish) {
            try std.testing.expect((try decoder.next(&input)) == null);
            try std.testing.expectError(error.InvalidCopyData, decoder.finish());
        } else try std.testing.expectError(error.InvalidCopyData, decoder.next(&input));
    }
    var decoder = try Decoder.init(a, .{}, 1, 3);
    defer decoder.deinit();
    var too_large: []const u8 = "four\n";
    try std.testing.expectError(error.ProgramLimitExceeded, decoder.next(&too_large));
}

test "pgwire COPY input matches PostgreSQL loose headers trailing escapes mixed newlines and encoding errors" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "one\nx,y\n", "a,b,c\nx,y\n" }) |payload| {
        var decoder = try Decoder.init(a, .{ .format = .csv, .header = true }, 2, 1024);
        defer decoder.deinit();
        var input = payload;
        const row = (try decoder.next(&input)).?;
        try std.testing.expectEqualStrings("x", row[0].?);
        try std.testing.expectEqualStrings("y", row[1].?);
        try std.testing.expect((try decoder.next(&input)) == null);
        try std.testing.expect((try decoder.finish()) == null);
    }
    {
        var decoder = try Decoder.init(a, .{}, 1, 1024);
        defer decoder.deinit();
        var input: []const u8 = "backslash\\";
        try std.testing.expect((try decoder.next(&input)) == null);
        const row = (try decoder.finish()).?;
        try std.testing.expectEqualStrings("backslash", row[0].?);
    }
    for ([_][]const u8{ "x\nnext\r", "x\rnext\n", "x\r\nnext\n", "x\rnext\r\n" }) |payload| {
        var decoder = try Decoder.init(a, .{}, 1, 1024);
        defer decoder.deinit();
        var input = payload;
        try std.testing.expect((try decoder.next(&input)) != null);
        // A CR-terminated row is returned before checking a following LF.
        _ = decoder.next(&input) catch |err| {
            try std.testing.expectEqual(error.InvalidCopyData, err);
            continue;
        };
        try std.testing.expectError(error.InvalidCopyData, decoder.next(&input));
    }
    for ([_][]const u8{ "zero\\000\n", "\\xff\n" }) |payload| {
        var decoder = try Decoder.init(a, .{}, 1, 1024);
        defer decoder.deinit();
        var input = payload;
        try std.testing.expectError(error.SqlInvalidTextEncoding, decoder.next(&input));
    }
}

test "pgwire COPY input matches PostgreSQL 18 text terminators and literal CSV backslash dot" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "\\.\n", "\\.\r\n", "\\.\nignored\n" }) |payload| {
        var decoder = try Decoder.init(a, .{}, 1, 1024);
        defer decoder.deinit();
        var input = payload;
        try std.testing.expect((try decoder.next(&input)) == null);
        var queued: []const u8 = "ignored too\n";
        try std.testing.expect((try decoder.next(&queued)) == null);
        try std.testing.expect((try decoder.finish()) == null);
    }
    for ([_][]const u8{ "\\.\n", "\\.", "\"\\.\"\n" }) |payload| {
        var decoder = try Decoder.init(a, .{ .format = .csv }, 1, 1024);
        defer decoder.deinit();
        var input = payload;
        const row = (try decoder.next(&input)) orelse (try decoder.finish()).?;
        try std.testing.expectEqualStrings("\\.", row[0].?);
    }
    var decoder = try Decoder.init(a, .{}, 1, 1024);
    defer decoder.deinit();
    var bad: []const u8 = "\"\\.\"\n";
    try std.testing.expectError(error.InvalidCopyData, decoder.next(&bad));
}

test "pgwire COPY input reuses bounded row storage without warm allocations" {
    const a = std.testing.allocator;
    for ([_]Options{ .{}, .{ .format = .csv }, .{ .format = .binary } }) |options| {
        var arena: std.heap.ArenaAllocator = .init(a);
        defer arena.deinit();
        var encoder = try @import("copy_out.zig").Encoder.init(a, arena.allocator(), &.{ .{ .name = "a", .type = .string }, .{ .name = "b", .type = .string } }, options, 1024);
        defer encoder.deinit();
        var output: std.Io.Writer.Allocating = .init(a);
        defer output.deinit();
        try encoder.start(&output.writer);
        const header = output.written().len;
        try encoder.row(arena.allocator(), &output.writer, .{ .columns = encoder.columns, .rows = &.{&.{ .{ .string = "left" }, .{ .string = "comma, tab\t newline\n and quote\"" } }}, .command_tag = "SELECT" }, 0);
        var buffer: [8192]u8 = undefined;
        var backing: std.heap.FixedBufferAllocator = .init(&buffer);
        var decoder = try Decoder.init(backing.allocator(), options, 2, 1024);
        defer decoder.deinit();
        var initial: []const u8 = output.written();
        try std.testing.expect((try decoder.next(&initial)) != null);
        try std.testing.expect((try decoder.next(&initial)) == null);
        const baseline = backing.end_index;
        const start = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
        for (0..10000) |_| {
            var row: []const u8 = output.written()[header..];
            const fields = (try decoder.next(&row)).?;
            try std.testing.expectEqual(@as(usize, 2), fields.len);
            try std.testing.expectEqualStrings("left", fields[0].?);
            try std.testing.expect((try decoder.next(&row)) == null);
            try std.testing.expectEqual(baseline, backing.end_index);
        }
        const elapsed = std.Io.Clock.awake.now(std.testing.io).nanoseconds - start;
        std.debug.print("PGWIRE_COPY_INPUT codec={s} rows=10000 retained_bytes={d} warm_allocation_bytes=0 elapsed_ns={d}\n", .{ @tagName(options.format), baseline, elapsed });
    }
}

test "pgwire COPY input column masks default provenance and matching headers survive every split" {
    const a = std.testing.allocator;
    const data = "id,status,amount\n1,,n/a\n2,\"\",\"n/a\"\n";
    for (1..data.len + 1) |chunk| {
        var decoder = try Decoder.init(a, .{ .format = .csv, .header = true, .header_match = true, .force_null_columns = &.{"status"}, .force_not_null_columns = &.{"status"}, .default_string = "n/a" }, 3, 1024);
        defer decoder.deinit();
        try decoder.setColumns(&.{ "id", "status", "amount" });
        var offset: usize = 0;
        var rows: usize = 0;
        while (offset < data.len) {
            const end = @min(offset + chunk, data.len);
            var input = data[offset..end];
            while (try decoder.next(&input)) |row| {
                if (rows == 0) {
                    try std.testing.expectEqualStrings("", row[1].?);
                    try std.testing.expect(decoder.defaults.items[2]);
                } else {
                    try std.testing.expect(row[1] == null);
                    try std.testing.expectEqualStrings("n/a", row[2].?);
                    try std.testing.expect(!decoder.defaults.items[2]);
                }
                rows += 1;
            }
            offset = end;
        }
        try std.testing.expect((try decoder.finish()) == null);
        try std.testing.expectEqual(@as(usize, 2), rows);
    }
    var bad = try Decoder.init(a, .{ .header = true, .header_match = true }, 2, 1024);
    defer bad.deinit();
    try bad.setColumns(&.{ "id", "status" });
    try std.testing.expectError(error.InvalidCopyData, bad.finish());
    var header: []const u8 = "status\tid\n";
    try std.testing.expectError(error.InvalidCopyData, bad.next(&header));
    var marker_header = try Decoder.init(a, .{ .format = .csv, .header = true, .header_match = true, .default_string = "id" }, 1, 1024);
    defer marker_header.deinit();
    try marker_header.setColumns(&.{"id"});
    var default_header: []const u8 = "id\n";
    try std.testing.expectError(error.InvalidCopyData, marker_header.next(&default_header));
    var unknown = try Decoder.init(a, .{ .format = .csv, .force_null_columns = &.{"missing"} }, 1, 1024);
    defer unknown.deinit();
    try std.testing.expectError(error.UnknownColumn, unknown.setColumns(&.{"id"}));
}
