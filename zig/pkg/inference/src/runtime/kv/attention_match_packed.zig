// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Canonical rotated4/int8 storage shared by checkpoints and CUDA adapters.
//! Separate spans preserve previously encoded bytes even when a later cycle
//! falls back to FP16. Exact rows remain in logical order outside these spans.
const std = @import("std");
const rotated = @import("turboquant_rotated.zig");
const Control = @import("attention_match.zig").Control;

pub const Span = extern struct {
    first: u32,
    rows: u32,
    /// Row offset into the packed K/V arrays, not a byte offset.
    offset: u32,
};
comptime {
    if (@sizeOf(Span) != 12) @compileError("packed span CUDA ABI mismatch");
}

pub const Index = union(enum) { exact: usize, compressed: usize };

pub const ExactRun = struct {
    first: usize,
    rows: usize,
    /// Row offset in the dense array containing only exact rows.
    offset: usize,
};

pub const ExactIterator = struct {
    spans: []const Span,
    total_rows: usize,
    span_index: usize = 0,
    row: usize = 0,
    exact_offset: usize = 0,

    pub fn next(self: *ExactIterator) ?ExactRun {
        while (self.row < self.total_rows) {
            const first = self.row;
            const end = if (self.span_index < self.spans.len) self.spans[self.span_index].first else self.total_rows;
            if (self.span_index < self.spans.len) {
                self.row = @as(usize, end) + self.spans[self.span_index].rows;
                self.span_index += 1;
            } else self.row = end;
            if (first == end) continue;
            const result: ExactRun = .{ .first = first, .rows = end - first, .offset = self.exact_offset };
            self.exact_offset += result.rows;
            return result;
        }
        return null;
    }
};

pub const Packed = struct {
    version: u32 = 0,
    spans: []const Span = &.{},
    keys: []const u8 = &.{},
    values: []const u8 = &.{},

    pub fn rows(self: Packed) usize {
        if (self.spans.len == 0) return 0;
        const last = self.spans[self.spans.len - 1];
        return @as(usize, last.offset) + last.rows;
    }

    pub fn bytes(self: Packed) usize {
        return self.spans.len * @sizeOf(Span) + self.keys.len + self.values.len;
    }

    /// Call after shape validation. Walk exact runs once without searching the
    /// compressed span index separately for every coordinate or row.
    pub fn exactRuns(self: Packed, total_rows: usize) ExactIterator {
        return .{ .spans = self.spans, .total_rows = total_rows };
    }

    /// Call after shape validation. Exact indices exclude all preceding packed
    /// rows; packed indices address the immutable compressed arrays directly.
    pub fn index(self: Packed, row: usize) Index {
        var low: usize = 0;
        var high = self.spans.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            if (self.spans[mid].first <= row) low = mid + 1 else high = mid;
        }
        if (low == 0) return .{ .exact = row };
        const span = self.spans[low - 1];
        if (row - span.first < span.rows) return .{ .compressed = @as(usize, span.offset) + row - span.first };
        return .{ .exact = row - (@as(usize, span.offset) + span.rows) };
    }

    pub fn deinit(self: *Packed, allocator: std.mem.Allocator) void {
        allocator.free(self.spans);
        allocator.free(self.keys);
        allocator.free(self.values);
        self.* = .{};
    }

    pub fn clone(self: Packed, allocator: std.mem.Allocator) !Packed {
        var result: Packed = .{ .version = self.version };
        errdefer result.deinit(allocator);
        result.spans = try allocator.dupe(Span, self.spans);
        result.keys = try allocator.dupe(u8, self.keys);
        result.values = try allocator.dupe(u8, self.values);
        return result;
    }

    /// Validate lengths before allocating or decoding values. Coverage checks
    /// belong to the checkpoint owner, which knows the represented positions.
    pub fn validate(self: Packed, total_rows: usize, dim: u32) !void {
        if (self.spans.len == 0) {
            if (self.version != 0 or self.keys.len != 0 or self.values.len != 0) return error.InvalidKvCheckpoint;
            return;
        }
        if (self.version != rotated.version or !rotated.supported(dim) or self.spans.len > total_rows)
            return error.InvalidKvCheckpoint;
        var end: usize = 0;
        var count: usize = 0;
        for (self.spans, 0..) |span, i| {
            if (span.rows == 0 or span.first > total_rows or span.rows > total_rows - span.first or
                (i > 0 and span.first <= end) or span.offset != count) return error.InvalidKvCheckpoint;
            end = @as(usize, span.first) + span.rows;
            count += span.rows;
        }
        if (self.keys.len != try mul(count, rotated.rowBytes(dim)) or self.values.len != try mul(count, valueBytes(dim)))
            return error.InvalidKvCheckpoint;
    }

    pub fn decodeRow(self: Packed, dim: u32, row: usize, keys: []f32, values: []f32) !void {
        if (keys.len != dim or values.len != dim or row >= self.rows()) return error.InvalidKvCheckpoint;
        const key_width = rotated.rowBytes(dim);
        if (key_width == 0 or self.keys.len < try mul(row + 1, key_width) or self.values.len < try mul(row + 1, valueBytes(dim))) return error.InvalidKvCheckpoint;
        rotated.decode(self.keys[row * key_width ..][0..key_width], keys) catch return error.InvalidKvCheckpoint;
        try decodeValues(self.values[row * valueBytes(dim) ..][0..valueBytes(dim)], values);
    }

    /// Append newly fitted rows without decoding/re-encoding prior spans. A
    /// contiguous new interval merges with its predecessor canonically; a gap
    /// remains exact. Publication is separate from numerical acceptance.
    pub fn append(self: Packed, allocator: std.mem.Allocator, first: usize, dim: u32, keys: []const f32, values: []const f32, control: Control) !Packed {
        try control.check();
        if (!rotated.supported(dim) or keys.len == 0 or keys.len % dim != 0 or values.len != keys.len) return error.InvalidKvCheckpoint;
        const count = keys.len / dim;
        if (first > std.math.maxInt(u32) or count > std.math.maxInt(u32) - first) return error.InvalidKvCheckpoint;
        try self.validate(first, dim);
        const previous = self.rows();
        const merge = self.spans.len > 0 and @as(usize, self.spans[self.spans.len - 1].first) + self.spans[self.spans.len - 1].rows == first;
        var result: Packed = .{ .version = rotated.version };
        errdefer result.deinit(allocator);
        const spans = try allocator.alloc(Span, self.spans.len + @intFromBool(!merge));
        result.spans = spans;
        @memcpy(spans[0..self.spans.len], self.spans);
        if (merge) spans[spans.len - 1].rows += @intCast(count) else spans[spans.len - 1] = .{ .first = @intCast(first), .rows = @intCast(count), .offset = @intCast(previous) };
        const encoded_keys = try allocator.alloc(u8, try mul(previous + count, rotated.rowBytes(dim)));
        result.keys = encoded_keys;
        const encoded_values = try allocator.alloc(u8, try mul(previous + count, valueBytes(dim)));
        result.values = encoded_values;
        @memcpy(encoded_keys[0..self.keys.len], self.keys);
        @memcpy(encoded_values[0..self.values.len], self.values);
        for (0..count) |row| {
            try control.check();
            const i = previous + row;
            try rotated.encode(keys[row * dim ..][0..dim], encoded_keys[i * rotated.rowBytes(dim) ..][0..rotated.rowBytes(dim)]);
            try encodeValues(values[row * dim ..][0..dim], encoded_values[i * valueBytes(dim) ..][0..valueBytes(dim)]);
        }
        try control.check();
        return result;
    }

    /// Filter only the newly appended interval. Earlier encoded bytes remain
    /// immutable, including when append merged the new interval into an old span.
    /// Two passes allocate the canonical spans and payload exactly once.
    pub fn selectNew(self: Packed, allocator: std.mem.Allocator, first: usize, selected: []const bool, dim: u32, control: Control) !Packed {
        try control.check();
        if (first > std.math.maxInt(u32) or selected.len > std.math.maxInt(u32) - first) return error.InvalidKvCheckpoint;
        try self.validate(first + selected.len, dim);
        for (selected, 0..) |_, i| {
            try control.check();
            if (self.index(first + i) != .compressed) return error.InvalidKvCheckpoint;
        }
        var count: usize = 0;
        var span_count: usize = 0;
        var previous: ?usize = null;
        for (self.spans) |span| for (0..span.rows) |i| {
            try control.check();
            const row = span.first + i;
            if (row >= first and !selected[row - first]) continue;
            if (previous == null or previous.? + 1 != row) span_count += 1;
            previous = row;
            count += 1;
        };
        if (count == 0) return .{};
        var result: Packed = .{ .version = rotated.version };
        errdefer result.deinit(allocator);
        const spans = try allocator.alloc(Span, span_count);
        result.spans = spans;
        const keys = try allocator.alloc(u8, try mul(count, rotated.rowBytes(dim)));
        result.keys = keys;
        const values = try allocator.alloc(u8, try mul(count, valueBytes(dim)));
        result.values = values;
        var output: usize = 0;
        var span_index: usize = 0;
        previous = null;
        for (self.spans) |span| for (0..span.rows) |i| {
            try control.check();
            const row = span.first + i;
            if (row >= first and !selected[row - first]) continue;
            if (previous == null or previous.? + 1 != row) {
                spans[span_index] = .{ .first = @intCast(row), .rows = 1, .offset = @intCast(output) };
                span_index += 1;
            } else spans[span_index - 1].rows += 1;
            previous = row;
            const source = span.offset + i;
            const kb = rotated.rowBytes(dim);
            const vb = valueBytes(dim);
            @memcpy(keys[output * kb ..][0..kb], self.keys[source * kb ..][0..kb]);
            @memcpy(values[output * vb ..][0..vb], self.values[source * vb ..][0..vb]);
            output += 1;
        };
        try control.check();
        return result;
    }
};

test "exact runs preserve dense offsets for every eight-row compressed layout" {
    for (0..256) |mask| {
        var spans: [8]Span = undefined;
        var span_count: usize = 0;
        var compressed_count: usize = 0;
        var row: usize = 0;
        while (row < 8) {
            if (mask & (@as(usize, 1) << @intCast(row)) == 0) {
                row += 1;
                continue;
            }
            const first = row;
            while (row < 8 and mask & (@as(usize, 1) << @intCast(row)) != 0) : (row += 1) {}
            spans[span_count] = .{ .first = @intCast(first), .rows = @intCast(row - first), .offset = @intCast(compressed_count) };
            span_count += 1;
            compressed_count += row - first;
        }
        const layout: Packed = .{ .spans = spans[0..span_count] };
        var runs = layout.exactRuns(8);
        var visited: [8]bool = @splat(false);
        var offset: usize = 0;
        while (runs.next()) |run| {
            try std.testing.expect(run.rows > 0);
            try std.testing.expectEqual(offset, run.offset);
            for (run.first..run.first + run.rows) |i| {
                try std.testing.expect(!visited[i]);
                try std.testing.expectEqualDeep(Index{ .exact = offset }, layout.index(i));
                visited[i] = true;
                offset += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 8) - compressed_count, offset);
        for (visited, 0..) |seen, i| try std.testing.expectEqual(mask & (@as(usize, 1) << @intCast(i)) == 0, seen);
        try std.testing.expectEqual(null, runs.next());
    }
    var empty = (Packed{}).exactRuns(0);
    try std.testing.expectEqual(null, empty.next());
}

fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.InvalidKvCheckpoint;
}

pub fn valueBytes(dim: u32) usize {
    return @as(usize, dim) + 4;
}

pub fn encodeValues(input: []const f32, output: []u8) !void {
    if (input.len == 0 or output.len != input.len + 4) return error.InvalidKvCheckpoint;
    var maximum: f32 = 0;
    for (input) |value| {
        if (!std.math.isFinite(value)) return error.InvalidKvCheckpoint;
        maximum = @max(maximum, @abs(value));
    }
    const scale: f32 = if (maximum == 0) 1 else maximum / 127;
    const inverse = 1 / scale;
    if (!std.math.isFinite(inverse) or !std.math.isFinite(scale) or scale <= 0 or scale > std.math.floatMax(f32) / 127) return error.InvalidKvCheckpoint;
    std.mem.writeInt(u32, output[0..4], @bitCast(scale), .little);
    for (input, output[4..]) |value, *byte|
        byte.* = @bitCast(@as(i8, @intFromFloat(std.math.clamp(@round(value * inverse), -127, 127))));
}

pub fn decodeValues(input: []const u8, output: []f32) !void {
    if (output.len == 0 or input.len != output.len + 4) return error.InvalidKvCheckpoint;
    const scale: f32 = @bitCast(std.mem.readInt(u32, input[0..4], .little));
    if (!std.math.isFinite(scale) or scale <= 0 or scale > std.math.floatMax(f32) / 127) return error.InvalidKvCheckpoint;
    for (input[4..], output) |byte, *value| {
        const code: i8 = @bitCast(byte);
        if (code == -128) return error.InvalidKvCheckpoint;
        value.* = @as(f32, @floatFromInt(code)) * scale;
    }
}

fn allocationTest(allocator: std.mem.Allocator) !void {
    const keys: [128]f32 = @splat(0.125);
    const values: [128]f32 = @splat(2);
    var first = try (Packed{}).append(allocator, 1, 64, &keys, &values, .{});
    defer first.deinit(allocator);
    var gap = try first.append(allocator, 5, 64, &keys, &values, .{});
    defer gap.deinit(allocator);
    var adjacent = try gap.append(allocator, 7, 64, &keys, &values, .{});
    defer adjacent.deinit(allocator);
    try adjacent.validate(10, 64);
    try std.testing.expectEqual(@as(usize, 2), adjacent.spans.len);
    try std.testing.expectEqual(@as(usize, 6), adjacent.rows());
    try std.testing.expectEqualSlices(u8, first.keys, adjacent.keys[0..first.keys.len]);
    try std.testing.expectEqualSlices(u8, first.values, adjacent.values[0..first.values.len]);
    for ([_]Index{ .{ .exact = 0 }, .{ .compressed = 0 }, .{ .compressed = 1 }, .{ .exact = 1 }, .{ .exact = 2 }, .{ .compressed = 2 }, .{ .compressed = 3 }, .{ .compressed = 4 }, .{ .compressed = 5 }, .{ .exact = 3 } }, 0..) |want, row|
        try std.testing.expectEqualDeep(want, adjacent.index(row));
    var cloned = try adjacent.clone(allocator);
    defer cloned.deinit(allocator);
    try std.testing.expectEqualSlices(u8, adjacent.keys, cloned.keys);
    var k: [64]f32 = undefined;
    var v: [64]f32 = undefined;
    try cloned.decodeRow(64, 5, &k, &v);
    for (v) |value| try std.testing.expectEqual(@as(f32, 2), value);
}

test "packed compaction spans preserve earlier bytes through gaps merges clones and allocation failures" {
    try allocationTest(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationTest, .{});
}

test "packed compaction validates canonical ranges row widths and value codes" {
    try std.testing.expectError(error.InvalidKvCheckpoint, (Packed{ .version = 1 }).validate(1, 64));
    const spans = [_]Span{ .{ .first = 1, .rows = 2, .offset = 0 }, .{ .first = 3, .rows = 1, .offset = 2 } };
    try std.testing.expectError(error.InvalidKvCheckpoint, (Packed{ .version = 1, .spans = &spans }).validate(4, 64));
    var bytes: [5]u8 = .{ 0, 0, 128, 63, 128 };
    var value: [1]f32 = undefined;
    try std.testing.expectError(error.InvalidKvCheckpoint, decodeValues(&bytes, &value));
    bytes[4] = 127;
    try decodeValues(&bytes, &value);
    try std.testing.expectEqual(@as(f32, 127), value[0]);
}

fn selectionAllocationTest(a: std.mem.Allocator) !void {
    const keys: [6 * 64]f32 = @splat(0.125);
    const values: [6 * 64]f32 = @splat(2);
    var old = try (Packed{}).append(a, 1, 64, keys[0..128], values[0..128], .{});
    defer old.deinit(a);
    for ([_]usize{ 3, 5 }) |first| {
        var all = try old.append(a, first, 64, &keys, &values, .{});
        defer all.deinit(a);
        for ([_][6]bool{ @splat(true), @splat(false), .{ true, false, true, false, true, false } }) |mask| {
            var filtered = try all.selectNew(a, first, &mask, 64, .{});
            defer filtered.deinit(a);
            try filtered.validate(first + mask.len, 64);
            try std.testing.expectEqualSlices(u8, old.keys, filtered.keys[0..old.keys.len]);
            try std.testing.expectEqualSlices(u8, old.values, filtered.values[0..old.values.len]);
            var compressed: usize = 0;
            for (0..first + mask.len) |row| {
                const is_compressed = (row >= 1 and row < 3) or (row >= first and mask[row - first]);
                const want: Index = if (is_compressed) .{ .compressed = compressed } else .{ .exact = row - compressed };
                try std.testing.expectEqualDeep(want, filtered.index(row));
                compressed += @intFromBool(is_compressed);
            }
        }
    }
    var empty = try old.selectNew(a, 1, &.{ false, false }, 64, .{});
    defer empty.deinit(a);
    try empty.validate(3, 64);
    try std.testing.expectEqual(@as(usize, 0), empty.rows());
}

test "packed training selection preserves old bytes canonical spans and exact indices atomically" {
    try selectionAllocationTest(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, selectionAllocationTest, .{});
}
