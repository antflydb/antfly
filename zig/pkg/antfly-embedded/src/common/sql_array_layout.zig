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

//! Schema-bound flat SQL array payloads. This module owns no memory and has no
//! SQL execution or native-store dependencies. Framing integers are little
//! endian; typed payloads retain their own codecs (NUMERIC is PostgreSQL binary).
//! The enclosing immutable column owns element identity and checksum coverage.
//!
//! v1: version:u8, rank:u8, flags:u16, count:u32; rank (length:u32, lower:i32)
//! axes; NULL bitmap; fixed-width slots OR count+1 u32 offsets and payload.
//! Fixed arrays choose the smaller of dense slots and compact non-NULL slots
//! with one u32 rank checkpoint per 64 cells. Both have O(1) cell addressing.
//! Rank-zero empty arrays consist of the eight-byte header only. SQL NULL
//! arrays are outer row nulls, never a second encoding inside this frame.
const std = @import("std");
pub const Kind = @import("sql_builtin_type.zig").Type;
pub const Dimension = struct { length: u32, lower: i32 = 1 };
pub const version: u8 = 1;
pub const header_size: usize = 8;
pub const max_rank: usize = 6;
const compact_flag: u16 = 1;
pub const Limits = struct {
    elements: usize = 65536,
    bytes: usize = 8 * 1024 * 1024,
    numeric_bytes: usize = 8 * 1024 * 1024,
    numeric_groups: usize = 65535,
};
const NoBudget = struct {
    pub fn charge(_: *@This(), _: u64) !void {}
    pub fn limit(_: *@This()) anyerror {
        return error.SqlProgramLimitExceeded;
    }
};

pub fn width(kind: Kind) usize {
    return switch (kind) {
        .text, .jsonb, .numeric => 0,
        .boolean => 1,
        .int16 => 2,
        .int32, .float32 => 4,
        .int64, .float64 => 8,
        .uuid => 16,
    };
}

pub fn bitmapSize(count: usize) usize {
    return (count + 7) / 8;
}

pub fn checkpointSize(count: usize) usize {
    return ((count + 63) / 64 + 1) * 4;
}

pub fn usesCompact(kind: Kind, count: usize, non_null: usize) bool {
    if (count == 0 or count > std.math.maxInt(i32) or non_null > count or width(kind) == 0) return false;
    const dense = std.math.mul(usize, count, width(kind)) catch return false;
    const values = std.math.mul(usize, non_null, width(kind)) catch return false;
    const compact = std.math.add(usize, checkpointSize(count), values) catch return false;
    return compact < dense;
}

pub fn encodedSectionSize(kind: Kind, rank: usize, count: usize, non_null: usize) !usize {
    const dense = try sectionSize(kind, rank, count);
    if (non_null > count) return error.InvalidSqlArrayShape;
    if (!usesCompact(kind, count, non_null)) return dense;
    return dense - count * width(kind) + checkpointSize(count) + non_null * width(kind);
}

pub fn sectionSize(kind: Kind, rank: usize, count: usize) !usize {
    if (rank > max_rank or count > std.math.maxInt(i32)) return error.SqlProgramLimitExceeded;
    if ((rank == 0) != (count == 0)) return error.InvalidSqlArrayShape;
    if (count == 0) return header_size;
    const slots = if (width(kind) != 0)
        std.math.mul(usize, width(kind), count) catch return error.SqlProgramLimitExceeded
    else
        std.math.mul(usize, count + 1, 4) catch return error.SqlProgramLimitExceeded;
    return std.math.add(usize, header_size + rank * 8 + bitmapSize(count), slots) catch error.SqlProgramLimitExceeded;
}

pub const Cell = struct { sql_null: bool, bytes: []const u8 };

pub const Shape = struct {
    rank: u8,
    count: u32,
    axes: [max_rank]Dimension,
    compact: bool,
};

/// Header-only projection for an already authenticated row. O(rank), no cell
/// scan or allocation. This proves addressing extents, not payload canonicality;
/// untrusted publication/restore must also use View.open and JSONB validation.
pub fn inspectShape(kind: Kind, bytes: []const u8, limits: Limits) !Shape {
    if (bytes.len > limits.bytes) return error.SqlProgramLimitExceeded;
    if (bytes.len < header_size or bytes[0] != version) return error.InvalidSqlArrayStorage;
    const flags = std.mem.readInt(u16, bytes[2..4], .little);
    if (flags & ~compact_flag != 0 or (flags != 0 and width(kind) == 0)) return error.InvalidSqlArrayStorage;
    const compact = flags != 0;
    const rank = bytes[1];
    const count = std.mem.readInt(u32, bytes[4..8], .little);
    if (rank > max_rank or count > limits.elements) return error.SqlProgramLimitExceeded;
    const dense = try sectionSize(kind, rank, count);
    const directory_end = if (count == 0) header_size else header_size + @as(usize, rank) * 8 + bitmapSize(count) + checkpointSize(count);
    const minimum = if (compact) directory_end else dense;
    if (bytes.len < minimum) return error.InvalidSqlArrayStorage;
    var result: Shape = .{ .rank = rank, .count = count, .axes = @splat(.{ .length = 0 }), .compact = compact };
    if (count == 0) {
        if (compact or bytes.len != header_size) return error.NonCanonicalSqlArrayStorage;
        return result;
    }
    var product: usize = 1;
    for (0..rank) |i| {
        const at = header_size + i * 8;
        const length = std.mem.readInt(u32, bytes[at..][0..4], .little);
        const lower = std.mem.readInt(i32, bytes[at + 4 ..][0..4], .little);
        if (length == 0) return error.NonCanonicalSqlArrayStorage;
        if (length > std.math.maxInt(i32) or @as(i64, lower) + length > std.math.maxInt(i32)) return error.SqlProgramLimitExceeded;
        product = std.math.mul(usize, product, length) catch return error.SqlProgramLimitExceeded;
        if (product > std.math.maxInt(i32)) return error.SqlProgramLimitExceeded;
        result.axes[i] = .{ .length = length, .lower = lower };
    }
    if (product != count) return error.InvalidSqlArrayShape;
    if (width(kind) != 0) {
        const non_null = if (compact) std.mem.readInt(u32, bytes[directory_end - 4 ..][0..4], .little) else count;
        if (non_null > count) return error.InvalidSqlArrayStorage;
        const expected = if (compact) directory_end + @as(usize, non_null) * width(kind) else dense;
        if (bytes.len != expected) return error.NonCanonicalSqlArrayStorage;
    } else {
        const slots_start = header_size + @as(usize, rank) * 8 + bitmapSize(count);
        if (std.mem.readInt(u32, bytes[slots_start..][0..4], .little) != 0 or
            std.mem.readInt(u32, bytes[slots_start + @as(usize, count) * 4 ..][0..4], .little) != bytes.len - minimum)
            return error.NonCanonicalSqlArrayStorage;
    }
    return result;
}

/// A checked directory into pinned bytes. Scalar physical canonicality is
/// checked on open; JSONB payload semantics/canonical JSON require the owning
/// JSON codec. A directory check is not a substitute for that publication gate.
pub const View = struct {
    kind: Kind,
    bytes: []const u8,
    rank: u8,
    count: u32,
    bitmap_start: usize,
    slots_start: usize,
    payload_start: usize,
    compact: bool,

    /// Only for canonical bytes admitted by the owning codec and protected by
    /// row/page authentication. Checks extents in O(rank), not payload values
    /// or checkpoint contents. Untrusted ingestion must use open instead.
    pub fn openAuthenticated(kind: Kind, bytes: []const u8, limits: Limits) !View {
        const shape = try inspectShape(kind, bytes, limits);
        const rank = shape.rank;
        const count = shape.count;
        const minimum = try sectionSize(kind, rank, count);
        if (count == 0) {
            return .{ .kind = kind, .bytes = bytes, .rank = 0, .count = 0, .bitmap_start = header_size, .slots_start = header_size, .payload_start = header_size, .compact = false };
        }
        const bitmap_start = header_size + @as(usize, rank) * 8;
        const slots_start = bitmap_start + bitmapSize(count);
        return .{ .kind = kind, .bytes = bytes, .rank = rank, .count = count, .bitmap_start = bitmap_start, .slots_start = slots_start, .payload_start = if (width(kind) == 0) minimum else slots_start + if (shape.compact) checkpointSize(count) else @as(usize, 0), .compact = shape.compact };
    }

    pub fn open(kind: Kind, bytes: []const u8, limits: Limits) !View {
        var budget: NoBudget = .{};
        return openWithBudget(kind, bytes, limits, &budget);
    }

    /// Strict admission scans share the caller's work and cancellation owner.
    /// Authentication-only projection remains O(rank) and is not validation.
    pub fn openWithBudget(kind: Kind, bytes: []const u8, limits: Limits, budget: anytype) !View {
        try budget.charge(1);
        const view = openAuthenticated(kind, bytes, limits) catch |err| return if (err == error.SqlProgramLimitExceeded) budget.limit() else err;
        const count = view.count;
        if (count == 0) return view;
        const slots_start = view.slots_start;
        const minimum = try sectionSize(kind, view.rank, count);
        if (count % 8 != 0) {
            const allowed: u8 = (@as(u8, 1) << @intCast(count % 8)) - 1;
            if (bytes[slots_start - 1] & ~allowed != 0) return error.NonCanonicalSqlArrayStorage;
        }
        if (width(kind) == 0) {
            var previous: u32 = 0;
            for (0..count) |i| {
                try budget.charge(1);
                const next = view.offset(i + 1);
                if (next < previous or next > bytes.len - minimum) return error.InvalidSqlArrayStorage;
                previous = next;
            }
        }
        if (width(kind) != 0) {
            var non_null: usize = 0;
            for (0..(count + 63) / 64) |block| {
                try budget.charge(1);
                if (view.compact and view.offset(block) != non_null) return error.InvalidSqlArrayStorage;
                const bits = @min(64, @as(usize, count) - block * 64);
                const mask = if (bits == 64) std.math.maxInt(u64) else (@as(u64, 1) << @intCast(bits)) - 1;
                non_null += @popCount(~view.nullWord(block) & mask);
            }
            if (view.compact and view.offset((count + 63) / 64) != non_null) return error.InvalidSqlArrayStorage;
            if (view.compact != usesCompact(kind, count, non_null)) return error.NonCanonicalSqlArrayStorage;
        }
        for (0..count) |i| {
            try budget.charge(1);
            const raw = view.cellUnchecked(i);
            if (raw.sql_null) {
                for (raw.bytes) |byte| if (byte != 0) return error.NonCanonicalSqlArrayStorage;
                if (width(kind) == 0 and raw.bytes.len != 0) return error.NonCanonicalSqlArrayStorage;
                continue;
            }
            switch (kind) {
                .boolean => if (raw.bytes[0] > 1) return error.NonCanonicalSqlArrayStorage,
                .float32 => {
                    const bits = std.mem.readInt(u32, raw.bytes[0..4], .little);
                    if (std.math.isNan(@as(f32, @bitCast(bits))) and bits != 0x7fc00000) return error.NonCanonicalSqlArrayStorage;
                },
                .float64 => {
                    const bits = std.mem.readInt(u64, raw.bytes[0..8], .little);
                    if (std.math.isNan(@as(f64, @bitCast(bits))) and bits != 0x7ff8000000000000) return error.NonCanonicalSqlArrayStorage;
                },
                .text => {
                    var position: usize = 0;
                    while (position < raw.bytes.len) {
                        var end = position + @min(raw.bytes.len - position, 256);
                        // Keep the standard vectorized validator, splitting
                        // only at codepoint boundaries (at most 3-byte backoff
                        // for valid UTF-8; malformed continuation runs reject).
                        if (end < raw.bytes.len) while (end > position and raw.bytes[end] & 0xc0 == 0x80) : (end -= 1) {};
                        if (end == position) return error.SqlInvalidTextEncoding;
                        try budget.charge(end - position);
                        const chunk = raw.bytes[position..end];
                        if (!std.unicode.utf8ValidateSlice(chunk) or std.mem.indexOfScalar(u8, chunk, 0) != null) return error.SqlInvalidTextEncoding;
                        position = end;
                    }
                },
                .jsonb => if (raw.bytes.len == 0) return error.InvalidSqlArrayStorage,
                .numeric => _ = @import("sql_numeric_layout.zig").View.openWithBudget(raw.bytes, .{
                    .bytes = @min(limits.bytes, limits.numeric_bytes),
                    .groups = limits.numeric_groups,
                }, budget) catch |err| return switch (err) {
                    error.InvalidSqlBinaryRepresentation => error.InvalidSqlArrayStorage,
                    else => err,
                },
                else => {},
            }
        }
        return view;
    }

    pub fn dimension(self: View, axis: usize) !Dimension {
        if (axis >= self.rank) return error.SqlArraySubscriptOutOfRange;
        const at = header_size + axis * 8;
        return .{ .length = std.mem.readInt(u32, self.bytes[at..][0..4], .little), .lower = std.mem.readInt(i32, self.bytes[at + 4 ..][0..4], .little) };
    }

    /// Logical identity, deliberately independent of frame version, dense vs
    /// compact slots, offsets and padding. JSONB and NUMERIC bytes must have
    /// crossed their canonical validation gates before hashing.
    pub fn updateLogicalHash(self: View, hasher: *std.crypto.hash.Blake3) void {
        hasher.update("sql-array-logical-v1");
        hasher.update(&.{ @backingInt(self.kind), self.rank });
        var number: [8]u8 = undefined;
        std.mem.writeInt(u64, &number, self.count, .little);
        hasher.update(&number);
        for (0..self.rank) |axis| {
            const at = header_size + axis * 8;
            hasher.update(self.bytes[at..][0..8]);
        }
        for (0..self.count) |i| {
            const raw = self.cellUnchecked(i);
            hasher.update(&.{@intFromBool(raw.sql_null)});
            if (raw.sql_null) continue;
            std.mem.writeInt(u64, &number, raw.bytes.len, .little);
            hasher.update(&number);
            // NUMERIC display scale and floating zero signs survive physical
            // encoding, but are not part of PostgreSQL logical equality.
            if (self.kind == .numeric) {
                const numeric = @import("sql_numeric_layout.zig").View.openAuthenticated(raw.bytes, .{}) catch unreachable;
                numeric.updateLogicalHash(hasher);
            } else if ((self.kind == .float32 and std.mem.readInt(u32, raw.bytes[0..4], .little) & 0x7fffffff == 0) or
                (self.kind == .float64 and std.mem.readInt(u64, raw.bytes[0..8], .little) & 0x7fffffffffffffff == 0))
            {
                @memset(&number, 0);
                hasher.update(number[0..raw.bytes.len]);
            } else hasher.update(raw.bytes);
        }
    }

    pub fn cell(self: View, index: usize) !Cell {
        if (index >= self.count) return error.SqlArraySubscriptOutOfRange;
        return self.cellUnchecked(index);
    }

    pub fn ordinal(self: View, subscripts: []const i32) !?usize {
        if (subscripts.len != self.rank or self.rank == 0) return null;
        var result: usize = 0;
        for (subscripts, 0..) |subscript, axis| {
            const d = try self.dimension(axis);
            const index = @as(i64, subscript) - d.lower;
            if (index < 0 or index >= d.length) return null;
            result = result * d.length + @as(usize, @intCast(index));
        }
        return result;
    }

    fn offset(self: View, i: usize) u32 {
        return std.mem.readInt(u32, self.bytes[self.slots_start + i * 4 ..][0..4], .little);
    }

    fn cellUnchecked(self: View, i: usize) Cell {
        const is_null = self.bytes[self.bitmap_start + i / 8] & (@as(u8, 1) << @intCast(i % 8)) != 0;
        const size = width(self.kind);
        if (self.compact and is_null) return .{ .sql_null = true, .bytes = self.bytes[self.payload_start..self.payload_start] };
        const index = if (self.compact) index: {
            const bit: u6 = @intCast(i % 64);
            const mask = (@as(u64, 1) << bit) - 1;
            break :index self.offset(i / 64) + @popCount(~self.nullWord(i / 64) & mask);
        } else i;
        const start = if (size == 0) self.payload_start + self.offset(i) else self.payload_start + index * size;
        const end = if (size == 0) self.payload_start + self.offset(i + 1) else start + size;
        return .{ .sql_null = is_null, .bytes = self.bytes[start..end] };
    }

    fn nullWord(self: View, block: usize) u64 {
        const start = self.bitmap_start + block * 8;
        const bytes = self.bytes[start..@min(start + 8, self.slots_start)];
        var result: u64 = 0;
        for (bytes, 0..) |byte, i| result |= @as(u64, byte) << @intCast(i * 8);
        return result;
    }
};

test "SQL flat array directory validates dimensions NULL padding and direct addressing without allocation" {
    // A 2x2 int16 array with nondefault bounds and one SQL NULL.
    const bytes = [_]u8{ version, 2, 0, 0, 4, 0, 0, 0, 2, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 2, 0, 0, 0, 3, 0, 0, 0, 2, 1, 0, 0, 0, 3, 0, 4, 0 };
    const view = try View.open(.int16, &bytes, .{});
    const shape = try inspectShape(.int16, &bytes, .{});
    try std.testing.expectEqual(@as(u32, 4), shape.count);
    try std.testing.expectEqual(Dimension{ .length = 2, .lower = -1 }, shape.axes[0]);
    try std.testing.expectEqual(@as(usize, 3), (try view.ordinal(&.{ 0, 4 })).?);
    try std.testing.expect((try view.ordinal(&.{ -2, 3 })) == null);
    try std.testing.expect((try view.cell(1)).sql_null);
    try std.testing.expectEqual(@as(i16, 4), std.mem.readInt(i16, (try view.cell(3)).bytes[0..2], .little));
    for (0..bytes.len) |length| try std.testing.expectError(error.InvalidSqlArrayStorage, View.open(.int16, bytes[0..length], .{}));
    var invalid = bytes;
    invalid[25 + 2] = 1;
    try std.testing.expectError(error.NonCanonicalSqlArrayStorage, View.open(.int16, &invalid, .{}));
    invalid = bytes;
    invalid[24] |= 0x80;
    try std.testing.expectError(error.NonCanonicalSqlArrayStorage, View.open(.int16, &invalid, .{}));
}
