// Copyright 2026 Antfly, Inc.
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

//! Dense HyperLogLog sketch for approximate distinct-value (cardinality)
//! estimation.
//!
//! A sketch is a register array of `2^precision` bytes. Each register holds
//! `rho` -- one plus the number of leading zero bits in the hash suffix routed
//! to that register -- so the union of two sketches is the register-wise max.
//! That makes the sketch a bounded join-semilattice (commutative, associative,
//! idempotent under merge with the all-zero sketch as identity), which is why
//! it slots into the algebraic index as a lattice "law": per-bucket sketches
//! can be maintained incrementally and merged across shards, and a cardinality
//! query reads the materialized sketch instead of rescanning and deduplicating
//! every value.
//!
//! The estimator is the classic Flajolet et al. bias-corrected harmonic mean
//! with linear counting in the small-cardinality regime. With 64-bit hashes the
//! large-range correction is unnecessary.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const default_precision: u6 = 14;
pub const min_precision: u6 = 4;
pub const max_precision: u6 = 18;

const magic = "hll1";
const seed: u64 = 0x9e3779b97f4a7c15;

pub const Error = error{
    InvalidHllPrecision,
    InvalidHllSketch,
    IncompatibleHllSketch,
};

fn validatePrecision(precision: u6) Error!void {
    if (precision < min_precision or precision > max_precision) return Error.InvalidHllPrecision;
}

fn registerCount(precision: u6) usize {
    return @as(usize, 1) << precision;
}

/// rho for a hash: register index is the top `precision` bits, the value is one
/// plus the number of leading zeros in the remaining suffix bits.
fn indexAndRho(precision: u6, hash: u64) struct { index: usize, rho: u8 } {
    const index: usize = @intCast(hash >> @intCast(64 - @as(u32, precision)));
    // Shift the index bits out; the suffix now occupies the high (64-precision)
    // bits with zero fill below. @clz over the full word therefore counts the
    // leading zeros of the suffix, capped so an all-zero suffix maps to the
    // maximum rho rather than overcounting the artificial low zero bits.
    const suffix = hash << @intCast(precision);
    const max_rho: u8 = @intCast(64 - @as(u32, precision) + 1);
    const rho: u8 = @min(@as(u8, @intCast(@clz(suffix))) + 1, max_rho);
    return .{ .index = index, .rho = rho };
}

pub fn hashBytes(bytes: []const u8) u64 {
    return std.hash.Wyhash.hash(seed, bytes);
}

pub const Sketch = struct {
    precision: u6,
    registers: []u8,

    pub fn init(alloc: Allocator, precision: u6) !Sketch {
        try validatePrecision(precision);
        const registers = try alloc.alloc(u8, registerCount(precision));
        @memset(registers, 0);
        return .{ .precision = precision, .registers = registers };
    }

    pub fn deinit(self: *Sketch, alloc: Allocator) void {
        alloc.free(self.registers);
        self.* = undefined;
    }

    pub fn clone(self: Sketch, alloc: Allocator) !Sketch {
        return .{ .precision = self.precision, .registers = try alloc.dupe(u8, self.registers) };
    }

    pub fn addHash(self: *Sketch, hash: u64) void {
        const located = indexAndRho(self.precision, hash);
        if (located.rho > self.registers[located.index]) self.registers[located.index] = located.rho;
    }

    pub fn add(self: *Sketch, bytes: []const u8) void {
        self.addHash(hashBytes(bytes));
    }

    pub fn merge(self: *Sketch, other: Sketch) !void {
        if (self.precision != other.precision) return Error.IncompatibleHllSketch;
        for (self.registers, other.registers) |*lhs, rhs| {
            if (rhs > lhs.*) lhs.* = rhs;
        }
    }

    pub fn isEmpty(self: Sketch) bool {
        for (self.registers) |register| {
            if (register != 0) return false;
        }
        return true;
    }

    fn alpha(m: f64) f64 {
        if (m <= 16) return 0.673;
        if (m <= 32) return 0.697;
        if (m <= 64) return 0.709;
        return 0.7213 / (1.0 + 1.079 / m);
    }

    pub fn estimate(self: Sketch) f64 {
        const m: f64 = @floatFromInt(self.registers.len);
        var sum: f64 = 0;
        var zeros: usize = 0;
        for (self.registers) |register| {
            if (register == 0) zeros += 1;
            // 2^-register, computed without pow.
            sum += 1.0 / @as(f64, @floatFromInt(@as(u64, 1) << @intCast(register)));
        }
        const raw = alpha(m) * m * m / sum;
        if (raw <= 2.5 * m and zeros > 0) {
            // Linear counting is more accurate for sparse sketches.
            return m * @log(m / @as(f64, @floatFromInt(zeros)));
        }
        return raw;
    }

    pub fn estimateRounded(self: Sketch) u64 {
        const value = self.estimate();
        if (value <= 0) return 0;
        return @intFromFloat(@round(value));
    }
};

/// Wire format: magic || precision (u8) || registers. Dense and fixed-size so
/// merges are a register-wise max over the byte tails.
pub fn encodeAlloc(alloc: Allocator, sketch: Sketch) ![]u8 {
    const out = try alloc.alloc(u8, magic.len + 1 + sketch.registers.len);
    @memcpy(out[0..magic.len], magic);
    out[magic.len] = @intCast(sketch.precision);
    @memcpy(out[magic.len + 1 ..], sketch.registers);
    return out;
}

fn registersView(bytes: []const u8) Error![]const u8 {
    if (bytes.len < magic.len + 1) return Error.InvalidHllSketch;
    if (!std.mem.eql(u8, bytes[0..magic.len], magic)) return Error.InvalidHllSketch;
    const precision: u6 = std.math.cast(u6, bytes[magic.len]) orelse return Error.InvalidHllPrecision;
    try validatePrecision(precision);
    const registers = bytes[magic.len + 1 ..];
    if (registers.len != registerCount(precision)) return Error.InvalidHllSketch;
    return registers;
}

pub fn decodeAlloc(alloc: Allocator, bytes: []const u8) !Sketch {
    const registers = try registersView(bytes);
    const precision: u6 = @intCast(bytes[magic.len]);
    return .{ .precision = precision, .registers = try alloc.dupe(u8, registers) };
}

/// A sketch holding a single value. This is the per-document "contribution"
/// folded into a bucket's materialized sketch.
pub fn singletonEncodedAlloc(alloc: Allocator, precision: u6, value: []const u8) ![]u8 {
    var sketch = try Sketch.init(alloc, precision);
    defer sketch.deinit(alloc);
    sketch.add(value);
    return try encodeAlloc(alloc, sketch);
}

/// Register-wise max of two encoded sketches (the lattice join). Either side may
/// be null/empty, in which case the other is returned (or an empty result).
pub fn mergeEncodedAlloc(alloc: Allocator, left: ?[]const u8, right: ?[]const u8) ![]u8 {
    if (left == null) {
        if (right) |bytes| return try alloc.dupe(u8, bytes);
        return Error.InvalidHllSketch;
    }
    if (right == null) return try alloc.dupe(u8, left.?);

    const left_registers = try registersView(left.?);
    const right_registers = try registersView(right.?);
    if (left_registers.len != right_registers.len) return Error.IncompatibleHllSketch;
    const out = try alloc.dupe(u8, left.?);
    const out_registers = out[magic.len + 1 ..];
    for (out_registers, right_registers) |*lhs, rhs| {
        if (rhs > lhs.*) lhs.* = rhs;
    }
    return out;
}

pub fn estimateEncoded(bytes: []const u8) !u64 {
    // An empty payload is the lattice identity (an absent sketch).
    if (bytes.len == 0) return 0;
    const registers = try registersView(bytes);
    const precision: u6 = @intCast(bytes[magic.len]);
    const sketch = Sketch{ .precision = precision, .registers = @constCast(registers) };
    return sketch.estimateRounded();
}

const testing = std.testing;

fn assertWithin(estimate: f64, truth: f64, relative_tolerance: f64) !void {
    const allowed = truth * relative_tolerance + 5.0;
    try testing.expect(@abs(estimate - truth) <= allowed);
}

test "empty sketch estimates zero" {
    var sketch = try Sketch.init(testing.allocator, default_precision);
    defer sketch.deinit(testing.allocator);
    try testing.expect(sketch.isEmpty());
    try testing.expectEqual(@as(u64, 0), sketch.estimateRounded());
}

test "estimates distinct counts within tolerance across scales" {
    const scales = [_]u64{ 10, 100, 1_000, 10_000, 100_000 };
    for (scales) |n| {
        var sketch = try Sketch.init(testing.allocator, default_precision);
        defer sketch.deinit(testing.allocator);
        var i: u64 = 0;
        while (i < n) : (i += 1) {
            var buf: [24]u8 = undefined;
            const value = std.fmt.bufPrint(&buf, "item-{d}", .{i}) catch unreachable;
            sketch.add(value);
        }
        // p=14 has ~0.81% standard error; allow a comfortable multiple.
        try assertWithin(sketch.estimate(), @floatFromInt(n), 0.05);
    }
}

test "duplicate insertions do not inflate the estimate" {
    var sketch = try Sketch.init(testing.allocator, default_precision);
    defer sketch.deinit(testing.allocator);
    for (0..5_000) |_| sketch.add("same-value");
    try testing.expect(sketch.estimateRounded() <= 2);
}

test "merge estimates the union and is idempotent" {
    const alloc = testing.allocator;
    var left = try Sketch.init(alloc, default_precision);
    defer left.deinit(alloc);
    var right = try Sketch.init(alloc, default_precision);
    defer right.deinit(alloc);
    for (0..5_000) |i| {
        var buf: [24]u8 = undefined;
        left.add(std.fmt.bufPrint(&buf, "a-{d}", .{i}) catch unreachable);
    }
    for (2_500..7_500) |i| {
        var buf: [24]u8 = undefined;
        right.add(std.fmt.bufPrint(&buf, "a-{d}", .{i}) catch unreachable);
    }
    try left.merge(right);
    try assertWithin(left.estimate(), 7_500, 0.05);

    const before = left.estimateRounded();
    try left.merge(right);
    try testing.expectEqual(before, left.estimateRounded());
}

test "encode/decode round trips" {
    const alloc = testing.allocator;
    var sketch = try Sketch.init(alloc, default_precision);
    defer sketch.deinit(alloc);
    for (0..1_000) |i| {
        var buf: [24]u8 = undefined;
        sketch.add(std.fmt.bufPrint(&buf, "v-{d}", .{i}) catch unreachable);
    }
    const encoded = try encodeAlloc(alloc, sketch);
    defer alloc.free(encoded);
    var decoded = try decodeAlloc(alloc, encoded);
    defer decoded.deinit(alloc);
    try testing.expectEqual(sketch.precision, decoded.precision);
    try testing.expectEqualSlices(u8, sketch.registers, decoded.registers);
    try testing.expectEqual(sketch.estimateRounded(), decoded.estimateRounded());
}

test "folding singletons equals direct insertion" {
    const alloc = testing.allocator;
    var direct = try Sketch.init(alloc, default_precision);
    defer direct.deinit(alloc);
    var folded: ?[]u8 = null;
    defer if (folded) |bytes| alloc.free(bytes);
    for (0..2_000) |i| {
        var buf: [24]u8 = undefined;
        const value = std.fmt.bufPrint(&buf, "k-{d}", .{i}) catch unreachable;
        direct.add(value);
        const singleton = try singletonEncodedAlloc(alloc, default_precision, value);
        defer alloc.free(singleton);
        const next = try mergeEncodedAlloc(alloc, folded, singleton);
        if (folded) |bytes| alloc.free(bytes);
        folded = next;
    }
    const direct_encoded = try encodeAlloc(alloc, direct);
    defer alloc.free(direct_encoded);
    // Register-wise max is order-independent, so the byte images match exactly.
    try testing.expectEqualSlices(u8, direct_encoded, folded.?);
    try testing.expectEqual(direct.estimateRounded(), try estimateEncoded(folded.?));
}

test "incompatible precisions cannot merge" {
    const alloc = testing.allocator;
    var small = try Sketch.init(alloc, min_precision);
    defer small.deinit(alloc);
    var large = try Sketch.init(alloc, default_precision);
    defer large.deinit(alloc);
    try testing.expectError(Error.IncompatibleHllSketch, small.merge(large));
}
