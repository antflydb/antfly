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

//! Negotiated native retained pages. Control operations keep their JSON wire
//! contract; page cells use the versioned, bounded SQL column block codec.
const std = @import("std");
const sources = @import("antfly_local_sources");
const catalog = sources.sql_catalog;
const View = sources.storage_relational_read_view.View;
const spill = sources.sql_spill;
const Datum = catalog.Row.Cell;
const A = std.mem.Allocator;
pub const media_type = "application/vnd.antfly.sql-page.v1";
pub const magic = "ARP\x01";
// ARP1 freezes NCB1, including its native value encodings. Incompatible spill
// or cell codec changes require a new media type, not silent wire evolution.
const block_magic = "NCB\x01";
pub const max_bytes = 32 << 20;

fn blob(w: *std.Io.Writer, bytes: []const u8) !void {
    try w.writeInt(u32, std.math.cast(u32, bytes.len) orelse return error.RetainedReadPageTooLarge, .little);
    try w.writeAll(bytes);
}

pub fn encode(alloc: A, sequence: u64, rows: []const View.Row, after: ?[]const u8) ![]u8 {
    var output_budget: sources.sql_memory_budget = .{ .backing = alloc, .limit = max_bytes };
    return encodeBounded(alloc, &output_budget, sequence, rows, after) catch |err| {
        if (output_budget.exhausted) return error.RetainedReadPageTooLarge;
        // Every writer in this memory-only codec is allocating. Preserve the
        // allocator failure identity rather than leaking Writer.WriteFailed.
        if (err == error.WriteFailed) return error.OutOfMemory;
        return err;
    };
}

fn encodeBounded(alloc: A, output_budget: *sources.sql_memory_budget, sequence: u64, rows: []const View.Row, after: ?[]const u8) ![]u8 {
    if (rows.len > 4096) return error.InvalidRetainedReadPage;
    var out: std.Io.Writer.Allocating = .init(output_budget.allocator());
    defer out.deinit();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var scratch_budget: sources.sql_memory_budget = .{ .backing = arena.allocator(), .limit = max_bytes * 4 };
    const a = scratch_budget.allocator();
    var names: std.StringArrayHashMapUnmanaged(void) = .empty;
    for (rows) |row| {
        const input = row.sqlRow();
        for (try input.fieldNames()) |name| try names.put(a, name, {});
    }
    // Width is bounded before allocating any per-row shape or value matrix.
    if (names.count() > 65536) return error.RetainedReadPageTooLarge;
    const w = &out.writer;
    try w.writeAll(magic);
    try w.writeInt(u64, sequence, .little);
    try w.writeInt(u32, @intCast(rows.len), .little);
    try w.writeInt(u32, @intCast(names.count()), .little);
    for (names.keys()) |name| try blob(w, name);
    if (after) |key| try blob(w, key) else try w.writeInt(u32, std.math.maxInt(u32), .little);
    for (rows) |row| {
        try blob(w, row.id);
        try w.writeInt(u64, row.version, .little);
        try w.writeInt(u32, row.schema_version, .little);
        try w.writeByte(@intFromBool(row.expected_content_digest != null));
        if (row.expected_content_digest) |digest| try w.writeAll(&digest);
        if (row.document) |document| {
            const json = try std.json.Stringify.valueAlloc(a, document, .{});
            try blob(w, json);
        } else try w.writeInt(u32, std.math.maxInt(u32), .little);
        const input = row.sqlRow();
        for (names.keys()) |name| try w.writeByte(@intFromBool(try input.hasField(name)));
    }
    // Both axes are tiled, so wide schemas retain the block codec's limits.
    var first: usize = 0;
    while (first < rows.len) {
        const count = @min(256, rows.len - first);
        var column: usize = 0;
        while (column < names.count()) {
            const width = @min(1024, names.count() - column);
            var block_arena = std.heap.ArenaAllocator.init(alloc);
            defer block_arena.deinit();
            var block_budget: sources.sql_memory_budget = .{ .backing = block_arena.allocator(), .limit = max_bytes * 4 };
            const b = block_budget.allocator();
            const block = try b.alloc(sources.sql_operators.Row, count);
            for (block, rows[first..][0..count], 0..) |*target, row, index| {
                const values = try b.alloc(Datum, width);
                const input = row.sqlRow();
                for (names.keys()[column..][0..width], values) |name, *value| value.* = try input.cell(name);
                target.* = .{ .values = values, .keys = &.{}, .ordinal = first + index };
            }
            const bytes = try spill.encodeColumnarBlockAlloc(b, block, max_bytes);
            if (!std.mem.startsWith(u8, bytes, block_magic)) return error.InvalidRetainedReadPage;
            try blob(w, bytes);
            column += width;
        }
        first += count;
    }
    return out.toOwnedSlice() catch |err| {
        if (output_budget.exhausted) return error.RetainedReadPageTooLarge;
        return err;
    };
}

const Reader = struct {
    bytes: []const u8,
    at: usize = 0,
    fn take(self: *Reader, n: usize) ![]const u8 {
        if (n > self.bytes.len - self.at) return error.InvalidRetainedReadPage;
        const bytes = self.bytes[self.at..][0..n];
        self.at += n;
        return bytes;
    }
    fn int(self: *Reader, comptime T: type) !T {
        return std.mem.readInt(T, (try self.take(@sizeOf(T)))[0..@sizeOf(T)], .little);
    }
    fn optional(self: *Reader) !?[]const u8 {
        const n = try self.int(u32);
        return if (n == std.math.maxInt(u32)) null else try self.take(n);
    }
    fn required(self: *Reader) ![]const u8 {
        return (try self.optional()) orelse error.InvalidRetainedReadPage;
    }
};

pub fn decode(alloc: A, bytes: []const u8, sequence: u64, schema_version: u32, limit: u32) !View.Page {
    if (limit == 0 or limit > 4096 or bytes.len > max_bytes) return error.InvalidRetainedReadPage;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    // The budget wraps the arena, so the page never retains a stack allocator.
    var budget: sources.sql_memory_budget = .{ .backing = arena.allocator(), .limit = max_bytes * 4 };
    const a = budget.allocator();
    const owned = try a.dupe(u8, bytes);
    var r: Reader = .{ .bytes = owned };
    if (!std.mem.eql(u8, try r.take(magic.len), magic) or try r.int(u64) != sequence) return error.InvalidRetainedReadPage;
    const count = try r.int(u32);
    const width = try r.int(u32);
    if (count > limit or width > 65536 or (count == 0 and width != 0)) return error.InvalidRetainedReadPage;
    const names = try a.alloc([]const u8, width);
    for (names) |*name| name.* = try r.required();
    const layout = try catalog.Row.TypedLayout.init(a, names);
    const after = try r.optional();
    const rows = try a.alloc(View.Row, count);
    for (rows) |*row| {
        const id = try r.required();
        const version = try r.int(u64);
        if (try r.int(u32) != schema_version) return error.InvalidRetainedReadPage;
        const has_digest = try r.int(u8);
        const digest: ?[32]u8 = switch (has_digest) {
            0 => null,
            1 => (try r.take(32))[0..32].*,
            else => return error.InvalidRetainedReadPage,
        };
        const document = if (try r.optional()) |json| try std.json.parseFromSliceLeaky(std.json.Value, a, json, .{ .allocate = .alloc_if_needed, .parse_numbers = false }) else null;
        const presence = try a.alloc(bool, width);
        for (presence) |*flag| flag.* = switch (try r.int(u8)) {
            0 => false,
            1 => true,
            else => return error.InvalidRetainedReadPage,
        };
        row.* = .{ .id = id, .version = version, .schema_version = schema_version, .value = .null, .typed_cells = .{ .layout = layout, .values = try a.alloc(Datum, width), .presence = presence }, .expected_content_digest = digest, .document = document };
    }
    var first: usize = 0;
    while (first < count) {
        const n = @min(256, count - first);
        var column: usize = 0;
        while (column < width) {
            const k = @min(1024, width - column);
            const encoded = try r.required();
            if (!std.mem.startsWith(u8, encoded, block_magic)) return error.InvalidRetainedReadPage;
            const block = try spill.decodeColumnarBlockInArena(a, encoded, max_bytes);
            if (block.count() != n or block.values.len != k or block.keys.len != 0) return error.InvalidRetainedReadPage;
            for (rows[first..][0..n], 0..) |*row, index| {
                if (block.ordinals[index] != first + index) return error.InvalidRetainedReadPage;
                const cells = row.typed_cells.?;
                for (@constCast(cells.values)[column..][0..k], column..) |*cell, ordinal| {
                    cell.* = try block.cell(index, ordinal - column);
                    if (!cells.presence.?[ordinal] and !cell.sql_null) return error.InvalidRetainedReadPage;
                }
            }
            column += k;
        }
        first += n;
    }
    if (r.at != bytes.len) return error.InvalidRetainedReadPage;
    return .{ .arena = arena, .rows = rows, .after = after };
}
