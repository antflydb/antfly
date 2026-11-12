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

//! Bounded, endian-stable immutable aggregate state. This is a codec, not an
//! authorization to evict native rows: the replicated freeze/publication receipt
//! must still fence writes, dependencies and authoritative extent ownership.
const std = @import("std");
const series = @import("retained_series.zig");
const types = @import("../retained_dataset_types.zig");
const A = std.mem.Allocator;
pub const encoding: u32 = 3;
pub const max_bytes = 16 * 1024 * 1024;
pub const max_rows = 65536;
const magic = "AFRS\x00\x00\x00\x03";
const v2_magic = "AFRS\x00\x00\x00\x02";
const legacy_magic = "AFRS\x00\x00\x00\x01";
pub const Receipt = struct {
    source_sequence: u64,
    /// Produced only from a native generation-closure receipt. The codec is
    /// never authority to close a source or evict its native state.
    source_closed: bool = false,
    admitted_ns: i128,
    complete_from_ns: i128,
    span: types.KeySpan,
};
pub const Decoded = struct { receipt: Receipt, rows: []const series.Bucket };

pub fn key(row: series.Bucket) types.RowKey {
    var dimensions: types.Id = undefined;
    std.crypto.hash.sha2.Sha256.hash(row.dimensions, &dimensions, .{});
    return types.rowKey(row.start_ns, dimensions);
}
fn fingerprint(a: A, spec: series.Spec) !types.Id {
    const bytes = try std.json.Stringify.valueAlloc(a, spec, .{});
    defer a.free(bytes);
    var result: types.Id = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &result, .{});
    return result;
}
fn validateReceipt(spec: series.Spec, receipt: Receipt) !void {
    try spec.validate();
    try receipt.span.validate();
    if (try spec.bucket(receipt.complete_from_ns) != receipt.complete_from_ns or
        std.mem.order(u8, &receipt.span.start, &types.rowKey(receipt.complete_from_ns, @splat(0))) == .lt) return error.RetainedSeriesCoverageMissing;
    // Every key below this boundary belongs to a closed bucket. A partial
    // bucket span is allowed only when the complete bucket is finalized.
    if (!receipt.source_closed) {
        const frontier = try spec.bucket(std.math.sub(i128, receipt.admitted_ns, spec.mutable_for_ns) catch return error.RetainedSeriesTimeOverflow);
        if (std.mem.order(u8, &receipt.span.end, &types.rowKey(frontier, @splat(0))) == .gt) return error.RetainedSeriesNotFinal;
    }
}
fn validateRows(spec: series.Spec, receipt: Receipt, rows: []const series.Bucket) !void {
    try validateReceipt(spec, receipt);
    if (rows.len > max_rows) return error.RetainedSeriesArchiveBudgetExceeded;
    var previous: ?types.RowKey = null;
    for (rows) |row| {
        try spec.validateBucket(row);
        const current = key(row);
        if (std.mem.order(u8, &current, &receipt.span.start) == .lt or std.mem.order(u8, &current, &receipt.span.end) != .lt or
            (previous != null and std.mem.order(u8, &previous.?, &current) != .lt)) return error.RetainedSeriesCorrupt;
        previous = current;
    }
}
const Encoder = struct {
    a: A,
    bytes: std.ArrayList(u8) = .empty,
    fn append(self: *Encoder, bytes: []const u8) !void {
        if (bytes.len > max_bytes - self.bytes.items.len) return error.RetainedSeriesArchiveBudgetExceeded;
        try self.bytes.appendSlice(self.a, bytes);
    }
    fn int(self: *Encoder, comptime T: type, value: T) !void {
        var bytes: [@sizeOf(T)]u8 = undefined;
        std.mem.writeInt(T, &bytes, value, .big);
        try self.append(&bytes);
    }
    fn optional(self: *Encoder, value: ?f64) !void {
        try self.int(u8, if (value == null) 0 else 1);
        if (value) |number| try self.int(u64, @bitCast(number));
    }
};
/// The caller obtains rows and progress from the same immutable native read cut.
/// Zero rows encode explicit complete empty coverage, never a missing file.
pub fn encode(a: A, spec: series.Spec, receipt: Receipt, rows: []const series.Bucket) ![]u8 {
    try validateRows(spec, receipt, rows);
    var out = Encoder{ .a = a };
    errdefer out.bytes.deinit(a);
    try out.append(magic);
    try out.append(&try fingerprint(a, spec));
    try out.int(u64, receipt.source_sequence);
    try out.int(u8, if (receipt.source_closed) 1 else 0);
    try out.int(i128, receipt.admitted_ns);
    try out.int(i128, receipt.complete_from_ns);
    try out.append(&receipt.span.start);
    try out.append(&receipt.span.end);
    try out.int(u32, @intCast(rows.len));
    for (rows) |row| {
        try out.int(i128, row.start_ns);
        try out.int(u16, @intCast(row.dimensions.len));
        try out.append(row.dimensions);
        try out.int(i64, row.rows);
        for (row.metrics) |metric| {
            try out.int(i64, metric.count);
            try out.int(u128, @bitCast(metric.sum));
            const exact = metric.exact_sum orelse "";
            try out.int(u16, @intCast(exact.len));
            try out.append(exact);
            try out.optional(metric.minimum);
            try out.optional(metric.maximum);
        }
    }
    return out.bytes.toOwnedSlice(a);
}
const Decoder = struct {
    bytes: []const u8,
    offset: usize = 0,
    fn take(self: *Decoder, count: usize) ![]const u8 {
        if (count > self.bytes.len - self.offset) return error.RetainedSeriesCorrupt;
        const result = self.bytes[self.offset..][0..count];
        self.offset += count;
        return result;
    }
    fn int(self: *Decoder, comptime T: type) !T {
        return std.mem.readInt(T, (try self.take(@sizeOf(T)))[0..@sizeOf(T)], .big);
    }
    fn optional(self: *Decoder) !?f64 {
        return switch (try self.int(u8)) {
            0 => null,
            1 => @bitCast(try self.int(u64)),
            else => error.RetainedSeriesCorrupt,
        };
    }
};
/// Returned dimensions borrow immutable bytes; metric arrays belong to a. Use
/// a request arena, and verify the segment checksum before calling this codec.
pub fn formatEncoding(bytes: []const u8) !u32 {
    if (bytes.len < magic.len) return error.RetainedSeriesCorrupt;
    if (std.mem.eql(u8, bytes[0..magic.len], magic)) return encoding;
    if (std.mem.eql(u8, bytes[0..v2_magic.len], v2_magic)) return 2;
    if (std.mem.eql(u8, bytes[0..legacy_magic.len], legacy_magic)) return 1;
    return error.RetainedSeriesCorrupt;
}

pub fn decode(a: A, spec: series.Spec, bytes: []const u8) !Decoded {
    if (bytes.len > max_bytes) return error.RetainedSeriesArchiveBudgetExceeded;
    var input = Decoder{ .bytes = bytes };
    const version = try formatEncoding(bytes);
    _ = try input.take(magic.len);
    if (!std.mem.eql(u8, try input.take(32), &try fingerprint(a, spec))) return error.RetainedSeriesDefinitionMismatch;
    var receipt: Receipt = undefined;
    receipt.source_sequence = try input.int(u64);
    receipt.source_closed = if (version == 1) false else switch (try input.int(u8)) {
        0 => false,
        1 => true,
        else => return error.RetainedSeriesCorrupt,
    };
    receipt.admitted_ns = try input.int(i128);
    receipt.complete_from_ns = try input.int(i128);
    receipt.span.start = (try input.take(96))[0..96].*;
    receipt.span.end = (try input.take(96))[0..96].*;
    try validateReceipt(spec, receipt);
    const count = try input.int(u32);
    const minimum_row_bytes = 26 + spec.laws.len * 26;
    if (count > max_rows or count > (bytes.len - input.offset) / minimum_row_bytes) return error.RetainedSeriesCorrupt;
    const rows = try a.alloc(series.Bucket, count);
    for (rows) |*row| {
        row.start_ns = try input.int(i128);
        const dimension_len = try input.int(u16);
        if (dimension_len > 4096) return error.RetainedSeriesCorrupt;
        row.dimensions = try input.take(dimension_len);
        row.rows = try input.int(i64);
        row.metrics = try a.alloc(series.Metric, spec.laws.len);
        for (row.metrics) |*metric| {
            metric.* = .{
                .count = try input.int(i64),
                .sum = @bitCast(try input.int(u128)),
                .exact_sum = if (version >= 3) exact: {
                    const len = try input.int(u16);
                    if (len == 0) break :exact null;
                    if (len > 550) return error.RetainedSeriesCorrupt;
                    const bytes_exact = try input.take(len);
                    _ = try series.ExactSum.parse(bytes_exact);
                    break :exact bytes_exact;
                } else null,
                .minimum = try input.optional(),
                .maximum = try input.optional(),
            };
        }
    }
    if (input.offset != bytes.len) return error.RetainedSeriesCorrupt;
    try validateRows(spec, receipt, rows);
    return .{ .receipt = receipt, .rows = rows };
}

test "retained series archive preserves exact count and wide average state and rejects changed definitions" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    const spec = series.Spec{ .id = @splat(1), .definition = @splat(2), .generation = 1, .source_partition = @splat(3), .source_incarnation = 1, .interval_ns = 60, .mutable_for_ns = 100, .laws = &.{ .count, .avg } };
    const count: i64 = 9_007_199_254_740_993;
    var metrics = [_]series.Metric{ .{ .count = count }, .{ .count = count, .sum = @as(f128, 1e308) * @as(f128, @floatFromInt(count)) } };
    const rows = [_]series.Bucket{.{ .start_ns = 0, .dimensions = "[]", .rows = count, .metrics = &metrics }};
    const receipt = Receipt{ .source_sequence = std.math.maxInt(u64), .admitted_ns = 1000, .complete_from_ns = 0, .span = try types.KeySpan.fromRange(.{ .start_ns = "0", .end_ns = "60" }) };
    const bytes = try encode(temp, spec, receipt, &rows);
    const decoded = try decode(temp, spec, bytes);
    try std.testing.expectEqual(count, decoded.rows[0].metrics[0].count);
    try std.testing.expectEqual(metrics[1].sum, decoded.rows[0].metrics[1].sum);
    try std.testing.expectEqual(@as(f64, 1e308), (try decoded.rows[0].metrics[1].finalize(.avg)).number);
    var changed = spec;
    changed.source_incarnation += 1;
    try std.testing.expectError(error.RetainedSeriesDefinitionMismatch, decode(temp, changed, bytes));
    try std.testing.expectError(error.RetainedSeriesCorrupt, decode(temp, spec, bytes[0 .. bytes.len - 1]));
    const trailing = try std.mem.concat(temp, u8, &.{ bytes, "x" });
    try std.testing.expectError(error.RetainedSeriesCorrupt, decode(temp, spec, trailing));
    for (0..@min(bytes.len, 280)) |n| try std.testing.expectError(error.RetainedSeriesCorrupt, decode(temp, spec, bytes[0..n]));
}

test "retained series archive requires closed complete coverage and ordered unique in-span rows" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const spec = series.Spec{ .id = @splat(1), .definition = @splat(2), .generation = 1, .source_partition = @splat(3), .source_incarnation = 1, .interval_ns = 60, .mutable_for_ns = 100, .laws = &.{.count} };
    var receipt = Receipt{ .source_sequence = 1, .admitted_ns = 159, .complete_from_ns = 0, .span = try types.KeySpan.fromRange(.{ .start_ns = "0", .end_ns = "60" }) };
    try std.testing.expectError(error.RetainedSeriesNotFinal, encode(a, spec, receipt, &.{}));
    receipt.admitted_ns = 160;
    const empty = try decode(a, spec, try encode(a, spec, receipt, &.{}));
    try std.testing.expectEqual(@as(usize, 0), empty.rows.len);
    var metrics = [_]series.Metric{.{ .count = 1 }};
    const row = series.Bucket{ .start_ns = 0, .dimensions = "[]", .rows = 1, .metrics = &metrics };
    try std.testing.expectError(error.RetainedSeriesCorrupt, encode(a, spec, receipt, &.{ row, row }));
    receipt.span.end = key(row);
    try std.testing.expectError(error.RetainedSeriesCorrupt, encode(a, spec, receipt, &.{row}));
    receipt.complete_from_ns = 60;
    try std.testing.expectError(error.RetainedSeriesCoverageMissing, encode(a, spec, receipt, &.{}));
}

test "retained series archive preserves source closure and admits legacy event-finalized states" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const spec = series.Spec{ .id = @splat(1), .definition = @splat(2), .generation = 1, .source_partition = @splat(3), .source_incarnation = 1, .interval_ns = 60, .mutable_for_ns = 100, .laws = &.{.count} };
    const receipt = Receipt{ .source_sequence = 1, .admitted_ns = 1, .complete_from_ns = 0, .source_closed = true, .span = try types.KeySpan.fromRange(.{ .start_ns = "600", .end_ns = "660" }) };
    const closed = try encode(a, spec, receipt, &.{});
    try std.testing.expect((try decode(a, spec, closed)).receipt.source_closed);
    try std.testing.expectEqual(encoding, try formatEncoding(closed));
    var invalid = try a.dupe(u8, closed);
    invalid[48] = 2;
    try std.testing.expectError(error.RetainedSeriesCorrupt, decode(a, spec, invalid));
    var normal = receipt;
    normal.source_closed = false;
    normal.admitted_ns = 1000;
    const modern = try encode(a, spec, normal, &.{});
    const legacy = try std.mem.concat(a, u8, &.{ legacy_magic, modern[8..48], modern[49..] });
    try std.testing.expectEqual(@as(u32, 1), try formatEncoding(legacy));
    const decoded = try decode(a, spec, legacy);
    try std.testing.expect(!decoded.receipt.source_closed);
    try std.testing.expectEqual(normal.source_sequence, decoded.receipt.source_sequence);
}
