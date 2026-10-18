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

//! Bounded consumers of immutable, validated primitive/text array storage.
//! Physical cursors avoid per-cell rank replay and transient Datums. Owners,
//! SQL shape checks and fallback admission remain with the caller.
const std = @import("std");
const arrays = @import("array_value.zig");
const layout = @import("../common/sql_array_layout.zig");
const json = @import("json_order.zig");
const bitsAt = @import("array_compact.zig").ResultBuilder.nullBits;

pub fn supported(view: layout.View) bool {
    return view.validation != null and primitive(view.kind);
}
pub fn primitive(kind: arrays.ElementType) bool {
    return switch (kind) {
        .boolean, .int16, .int32, .int64, .float32, .float64, .text => true,
        else => false,
    };
}
fn Cell(comptime kind: arrays.ElementType) type {
    return switch (kind) {
        .boolean => bool,
        .int16, .int32, .int64 => i64,
        .float32, .float64 => f64,
        .text => []const u8,
        else => unreachable,
    };
}
fn read(comptime kind: arrays.ElementType, view: layout.View, logical: usize, physical: usize) Cell(kind) {
    if (kind == .text) {
        const first = std.mem.readInt(u32, view.bytes[view.slots_start + logical * 4 ..][0..4], .little);
        const last = std.mem.readInt(u32, view.bytes[view.slots_start + (logical + 1) * 4 ..][0..4], .little);
        return view.bytes[view.payload_start + first .. view.payload_start + last];
    }
    const width = layout.width(kind);
    const bytes = view.bytes[view.payload_start + (if (view.compact) physical else logical) * width ..][0..width];
    return switch (kind) {
        .boolean => bytes[0] != 0,
        .int16 => std.mem.readInt(i16, bytes[0..2], .little),
        .int32 => std.mem.readInt(i32, bytes[0..4], .little),
        .int64 => std.mem.readInt(i64, bytes[0..8], .little),
        .float32 => @as(f32, @bitCast(std.mem.readInt(u32, bytes[0..4], .little))),
        .float64 => @bitCast(std.mem.readInt(u64, bytes[0..8], .little)),
        else => unreachable,
    };
}
fn orderCell(comptime kind: arrays.ElementType, left: Cell(kind), right: Cell(kind), work: *arrays.Budget) !std.math.Order {
    return switch (kind) {
        .text => work.orderBytes(left, right),
        .boolean => std.math.order(@intFromBool(left), @intFromBool(right)),
        .float32, .float64 => if (std.math.isNan(left) or std.math.isNan(right)) (if (std.math.isNan(left) == std.math.isNan(right)) .eq else if (std.math.isNan(left)) .gt else .lt) else std.math.order(left, right),
        else => std.math.order(left, right),
    };
}
fn probeCell(comptime kind: arrays.ElementType, value: arrays.Element) Cell(kind) {
    return switch (kind) {
        .text => value.value.string,
        .boolean => value.value.bool,
        .float32, .float64 => value.value.float,
        else => value.value.integer,
    };
}

/// Probe has already passed the scalar kernel's widened type validation.
pub fn quantified(view: layout.View, probe: arrays.Element, comparison: arrays.Comparison, quantifier: arrays.Quantifier, work: *arrays.Budget) !?bool {
    if (probe.sql_null) {
        try work.consume(view.count);
        return if (view.count == 0) quantifier == .all else null;
    }
    return switch (view.kind) {
        inline .boolean, .int16, .int32, .int64, .float32, .float64, .text => |kind| quantifiedTyped(kind, view, probeCell(kind, probe), comparison, quantifier, work),
        else => error.InvalidSqlArrayShape,
    };
}
fn quantifiedTyped(comptime kind: arrays.ElementType, view: layout.View, probe: Cell(kind), comparison: arrays.Comparison, quantifier: arrays.Quantifier, work: *arrays.Budget) !?bool {
    var physical: usize = 0;
    var begin: usize = 0;
    var saw_null = false;
    while (begin < view.count) {
        const count: usize = @min(64, view.count - begin);
        const nulls = bitsAt(view, begin, count);
        saw_null = saw_null or nulls != 0;
        const mask = if (count == 64) std.math.maxInt(u64) else (@as(u64, 1) << @intCast(count)) - 1;
        var present = ~nulls & mask;
        var charged: usize = 0;
        while (present != 0) {
            const bit = @ctz(present);
            present &= present - 1;
            // Visit intervening NULLs, then this non-NULL comparison. Charge
            // only the prefix visited if ANY/ALL short-circuits here.
            try work.consume(bit + 1 - charged + 1);
            charged = bit + 1;
            const order = try orderCell(kind, probe, read(kind, view, begin + bit, physical), work);
            physical += 1;
            const matched = comparison.accepts(order);
            if (quantifier == .any and matched) return true;
            if (quantifier == .all and !matched) return false;
        }
        try work.consume(count - charged);
        begin += count;
    }
    return if (saw_null) null else quantifier == .all;
}

/// Compare a shared prefix; callers retain their own shape tie-break. The
/// canonical row comparator spends an extra comparison unit per present cell.
pub fn orderPrefix(left: layout.View, right: layout.View, work: *arrays.Budget, comptime extra_present_work: bool) !std.math.Order {
    if (left.validation != null and right.validation != null and left.bytes.ptr == right.bytes.ptr and left.bytes.len == right.bytes.len and left.count == right.count) {
        const proof = left.validation.?;
        try work.consume(proof.element_work + if (extra_present_work) proof.non_null else @as(usize, 0));
        return .eq;
    }
    return switch (left.kind) {
        inline .boolean, .int16, .int32, .int64, .float32, .float64, .text => |kind| orderTyped(kind, left, right, work, extra_present_work),
        else => error.InvalidSqlArrayShape,
    };
}
fn orderTyped(comptime kind: arrays.ElementType, left: layout.View, right: layout.View, work: *arrays.Budget, comptime extra: bool) !std.math.Order {
    var lp: usize = 0;
    var rp: usize = 0;
    var begin: usize = 0;
    const length = @min(left.count, right.count);
    // Most sort comparisons diverge immediately. Resolve the first cell
    // before loading full block masks or checking an equal payload span.
    if (length != 0) {
        try work.consume(1);
        const an = left.bytes[left.bitmap_start] & 1 != 0;
        const bn = right.bytes[right.bitmap_start] & 1 != 0;
        if (an != bn) return if (an) .gt else .lt;
        if (!an) {
            if (extra) try work.consume(1);
            const compared = try orderCell(kind, read(kind, left, 0, 0), read(kind, right, 0, 0), work);
            if (compared != .eq) return compared;
            lp = 1;
            rp = 1;
        }
        begin = 1;
    }
    while (begin < length) {
        const count: usize = @min(64, length - begin);
        const ln = bitsAt(left, begin, count);
        const rn = bitsAt(right, begin, count);
        const present = count - @popCount(ln);
        if (kind != .text and ln == rn and (left.compact == right.compact or ln == 0)) {
            const width = layout.width(kind);
            const size = (if (left.compact) present else count) * width;
            const lb = left.bytes[left.payload_start + (if (left.compact) lp else begin) * width ..][0..size];
            const rb = right.bytes[right.payload_start + (if (right.compact) rp else begin) * width ..][0..size];
            if (std.mem.eql(u8, lb, rb)) {
                try work.consume(count + if (extra) present else @as(usize, 0));
                lp += present;
                rp += present;
                begin += count;
                continue;
            }
        }
        for (0..count) |bit| {
            try work.consume(1);
            const an = ln & (@as(u64, 1) << @intCast(bit)) != 0;
            const bn = rn & (@as(u64, 1) << @intCast(bit)) != 0;
            const compared: std.math.Order = if (an or bn) (if (an == bn) .eq else if (an) .gt else .lt) else compared: {
                if (extra) try work.consume(1);
                break :compared try orderCell(kind, read(kind, left, begin + bit, lp), read(kind, right, begin + bit, rp), work);
            };
            if (compared != .eq) return compared;
            lp += @intFromBool(!an);
            rp += @intFromBool(!bn);
        }
        begin += count;
    }
    return .eq;
}

/// Append exactly the existing per-element semantic hashes to the outer hash;
/// physical encoding, NaN payloads and zero signs are never hashed directly.
pub fn hashElements(view: layout.View, hash: *std.hash.Wyhash, work: *arrays.Budget) !void {
    switch (view.kind) {
        inline .boolean, .int16, .int32, .int64, .float32, .float64, .text => |kind| try hashTyped(kind, view, hash, work),
        else => return error.InvalidSqlArrayShape,
    }
}
fn hashTyped(comptime kind: arrays.ElementType, view: layout.View, hash: *std.hash.Wyhash, work: *arrays.Budget) !void {
    var scratch: [64 * 8]u8 = undefined;
    var physical: usize = 0;
    var begin: usize = 0;
    while (begin < view.count) {
        const count: usize = @min(64, view.count - begin);
        const nulls = bitsAt(view, begin, count);
        try work.consume(count);
        for (0..count) |bit| {
            const is_null = nulls & (@as(u64, 1) << @intCast(bit)) != 0;
            const cell_hash: u64 = if (is_null) 0x53514c4e554c4c else cell: {
                const value = read(kind, view, begin + bit, physical);
                physical += 1;
                var bytes: [8]u8 = undefined;
                break :cell switch (kind) {
                    .boolean => try json.hashBoolean(value, work),
                    .text => try json.hashString(value, work),
                    .float32, .float64 => floating: {
                        const bits: u64 = if (std.math.isNan(value)) 0x7ff8000000000000 else if (value == 0) 0 else @bitCast(value);
                        std.mem.writeInt(u64, &bytes, bits, .little);
                        break :floating std.hash.Wyhash.hash(1, &bytes);
                    },
                    else => integer: {
                        std.mem.writeInt(i64, &bytes, value, .little);
                        break :integer std.hash.Wyhash.hash(2, &bytes);
                    },
                };
            };
            std.mem.writeInt(u64, scratch[bit * 8 ..][0..8], cell_hash, .little);
        }
        hash.update(scratch[0 .. count * 8]);
        begin += count;
    }
}

const Fixture = struct {
    native_value: arrays.Value,
    scalar_value: arrays.Value,
    fn init(a: std.mem.Allocator, kind: arrays.ElementType, count: usize, pattern: usize, variant: usize, lower_bound: i32) !Fixture {
        const cells = try a.alloc(arrays.Element, count);
        for (cells, 0..) |*cell, index| {
            const i = if (variant == 5 and index == 0) 16 else if (variant == 1 and index + 1 == count) index + 1 else index;
            cell.* = if (pattern == 2 or (pattern == 1 and index % 7 != 0) or (pattern == 3 and index == 0)) .{} else .json(switch (kind) {
                .boolean => .{ .bool = i % 2 == 0 },
                .int16, .int32, .int64 => .{ .integer = @as(i64, @intCast(i % 31)) - 15 },
                .float32, .float64 => .{ .float = if (i % 11 == 0) std.math.nan(f64) else if (i % 5 == 0) (if (variant == 2) 0.0 else -0.0) else if (i % 7 == 0) std.math.inf(f64) else @as(f64, @floatFromInt(i % 31)) - 15.5 },
                .text => .{ .string = if (i % 3 == 0) "é☺" else if (i % 3 == 1) "" else "long equal prefix differing tail" },
                else => unreachable,
            });
        }
        const dimensions = [_]arrays.Dimension{.{ .length = @intCast(count), .lower = lower_bound }};
        const prepared = try arrays.Value.init(kind, dimensions[0..@intFromBool(count != 0)], cells, .{});
        const encoded = try @import("array_storage.zig").encodeAlloc(a, prepared, .{});
        const value = try @import("array_storage.zig").viewLeaky(a, kind, encoded, .{});
        const unproven = try a.create(layout.View);
        unproven.* = value.stored.?.*;
        unproven.validation = null;
        var scalar_value = value;
        scalar_value.stored = unproven;
        return .{ .native_value = value, .scalar_value = scalar_value };
    }
};

test "SQL native consumers retain quantified comparisons hashes and exact work" {
    const a = std.testing.allocator;
    for ([_]arrays.ElementType{ .boolean, .int16, .int32, .int64, .float32, .float64, .text }) |kind| {
        for ([_]usize{ 0, 1, 17, 64, 65, 129 }) |count| for (0..4) |pattern| {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const region = arena.allocator();
            const left = try Fixture.init(region, kind, count, pattern, 0, -7);
            for (0..4) |probe| {
                const scalar: arrays.Element = if (probe == 0) .{} else .json(switch (kind) {
                    .boolean => .{ .bool = probe == 1 },
                    .int16, .int32, .int64 => .{ .integer = if (probe == 1) -15 else if (probe == 2) std.math.maxInt(i64) else std.math.minInt(i64) },
                    .float32, .float64 => .{ .float = if (probe == 1) std.math.nan(f64) else if (probe == 2) -0.0 else 1.0000000000001 },
                    .text => .{ .string = if (probe == 1) "é☺" else if (probe == 2) "long equal prefix differing tail" else "missing" },
                    else => unreachable,
                });
                for (std.enums.values(arrays.Comparison)) |comparison| for (std.enums.values(arrays.Quantifier)) |quantifier| {
                    var fw: arrays.Budget = .{};
                    var sw: arrays.Budget = .{};
                    try std.testing.expectEqual(try left.scalar_value.quantified(scalar, comparison, quantifier, &sw), try left.native_value.quantified(scalar, comparison, quantifier, &fw));
                    try std.testing.expectEqual(sw.remaining, fw.remaining);
                    var denied: arrays.Budget = .{ .remaining = (arrays.Budget{}).remaining - fw.remaining - 1 };
                    try std.testing.expectError(error.SqlProgramLimitExceeded, left.native_value.quantified(scalar, comparison, quantifier, &denied));
                };
            }
            var fh: arrays.Budget = .{};
            var sh: arrays.Budget = .{};
            const expected_hash = try left.scalar_value.semanticHash(&sh);
            try std.testing.expectEqual(expected_hash, try left.native_value.semanticHash(&fh));
            try std.testing.expectEqual(sh.remaining, fh.remaining);
            for (0..5) |variant| {
                const right = try Fixture.init(region, kind, count + @intFromBool(variant == 3), pattern, variant, if (variant == 3 or variant == 4) 1 else -7);
                var fw: arrays.Budget = .{};
                var sw: arrays.Budget = .{};
                const expected = try left.scalar_value.compare(right.scalar_value, &sw);
                try std.testing.expectEqual(expected, try left.native_value.compare(right.native_value, &fw));
                try std.testing.expectEqual(sw.remaining, fw.remaining);
                var denied: arrays.Budget = .{ .remaining = (arrays.Budget{}).remaining - fw.remaining - 1 };
                try std.testing.expectError(error.SqlProgramLimitExceeded, left.native_value.compare(right.native_value, &denied));
                if (variant == 0 or variant == 2) {
                    var rw: arrays.Budget = .{};
                    try std.testing.expectEqual(expected_hash, try right.native_value.semanticHash(&rw));
                }
            }
            var identity: arrays.Budget = .{};
            var ordinary: arrays.Budget = .{};
            try std.testing.expectEqual(try left.scalar_value.compare(left.scalar_value, &ordinary), try left.native_value.compare(left.native_value, &identity));
            try std.testing.expectEqual(ordinary.remaining, identity.remaining);
        };
    }
}

test "SQL native consumers measure scalar parity without allocation" {
    const a = std.testing.allocator;
    for ([_]arrays.ElementType{ .int64, .float64, .text }) |kind| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const region = arena.allocator();
        const source = try Fixture.init(region, kind, 16384, 1, 0, -7);
        const equal = try Fixture.init(region, kind, 16384, 1, 0, -7);
        const different = try Fixture.init(region, kind, 16384, 1, 5, -7);
        var comparison_work: arrays.Budget = .{};
        const first_order = try source.scalar_value.compare(different.scalar_value, &comparison_work);
        const first_probe = source.scalar_value.flatCell(0);
        for (0..5) |operation| {
            var oracle_hash: ?u64 = null;
            for (0..2) |mode| {
                var elapsed: i96 = 0;
                var total_work: usize = 0;
                for (0..20) |_| {
                    var work: arrays.Budget = .{};
                    const value = if (mode == 0) source.scalar_value else source.native_value;
                    const right = if (mode == 0) equal.scalar_value else equal.native_value;
                    const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
                    switch (operation) {
                        0 => try std.testing.expectEqual(@as(?bool, null), try value.quantified(.json(switch (kind) {
                            .int64 => .{ .integer = 999 },
                            .float64 => .{ .float = 999 },
                            .text => .{ .string = "missing" },
                            else => unreachable,
                        }), .eq, .any, &work)),
                        1 => try std.testing.expectEqual(std.math.Order.eq, try value.compare(right, &work)),
                        2 => {
                            const actual = try value.semanticHash(&work);
                            if (oracle_hash) |expected| try std.testing.expectEqual(expected, actual) else oracle_hash = actual;
                        },
                        3 => try std.testing.expectEqual(@as(?bool, true), try value.quantified(first_probe, .eq, .any, &work)),
                        4 => try std.testing.expectEqual(first_order, try value.compare(if (mode == 0) different.scalar_value else different.native_value, &work)),
                        else => unreachable,
                    }
                    elapsed += @intCast(std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start);
                    total_work += (arrays.Budget{}).remaining - work.remaining;
                }
                std.debug.print("SQL_NATIVE_CONSUMER kind={s} operation={s} native={} repetitions=20 elapsed_ns={} work={} allocations=0\n", .{ @tagName(kind), switch (operation) {
                    0 => "any",
                    1 => "compare",
                    2 => "hash",
                    3 => "any_first",
                    else => "compare_first",
                }, mode == 1, elapsed, total_work });
            }
        }
    }
}

test "SQL native consumers retain cancellation sticky failure and shape boundaries" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const region = arena.allocator();
    const value = try Fixture.init(region, .int64, 4096, 1, 0, -7);
    const other = try Fixture.init(region, .int64, 4096, 1, 0, -7);
    const Cancel = struct {
        calls: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls == 2) return error.Canceled;
        }
    };
    for (0..4) |operation| {
        var control: Cancel = .{};
        var context: @import("numeric_value.zig").Context = .{ .alloc = std.testing.failing_allocator, .checkpoint = Cancel.check, .ptr = &control };
        var work: arrays.Budget = .{ .shared = &context };
        switch (operation) {
            0 => try std.testing.expectError(error.Canceled, value.native_value.quantified(.json(.{ .integer = 999 }), .eq, .any, &work)),
            1 => try std.testing.expectError(error.Canceled, value.native_value.compare(other.native_value, &work)),
            2 => try std.testing.expectError(error.Canceled, value.native_value.semanticHash(&work)),
            3 => try std.testing.expectError(error.Canceled, value.native_value.compare(value.native_value, &work)),
            else => unreachable,
        }
        try std.testing.expectError(error.Canceled, work.consume(0));
    }
    var hash_work: arrays.Budget = .{ .remaining = 17 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, value.native_value.semanticHash(&hash_work));
    const small = try Fixture.init(region, .int64, 129, 0, 0, -7);
    const matrix_cells = try region.alloc(arrays.Element, 129);
    for (matrix_cells, 0..) |*cell, i| cell.* = small.native_value.flatCell(i);
    const matrix = try arrays.Value.init(.int64, &.{ .{ .length = 1 }, .{ .length = 129 } }, matrix_cells, .{});
    const encoded = try @import("array_storage.zig").encodeAlloc(region, matrix, .{});
    const native_matrix = try @import("array_storage.zig").viewLeaky(region, .int64, encoded, .{});
    var fw: arrays.Budget = .{};
    var sw: arrays.Budget = .{};
    try std.testing.expectEqual(try small.scalar_value.compare(matrix, &sw), try small.native_value.compare(native_matrix, &fw));
    try std.testing.expectEqual(sw.remaining, fw.remaining);
}

test "SQL native comparison retains mixed layouts and rank order" {
    const a = std.testing.allocator;
    for ([_]arrays.ElementType{ .boolean, .int16, .int32, .int64, .float32, .float64, .text }) |kind| for ([_]bool{ false, true }) |with_null| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const region = arena.allocator();
        const short = try Fixture.init(region, kind, 129, @as(usize, if (with_null) 3 else 0), 0, -7);
        const cells = try region.alloc(arrays.Element, 4096);
        for (cells, 0..) |*cell, i| cell.* = if (i < 129) short.native_value.flatCell(i) else .{};
        const prepared = try arrays.Value.init(kind, &.{.{ .length = 4096, .lower = -7 }}, cells, .{});
        const encoded = try @import("array_storage.zig").encodeAlloc(region, prepared, .{});
        const long = try @import("array_storage.zig").viewLeaky(region, kind, encoded, .{});
        if (kind != .text) try std.testing.expect(!short.native_value.stored.?.compact and long.stored.?.compact);
        for ([_]bool{ false, true }) |reverse| {
            const left = if (reverse) long else short.native_value;
            const right = if (reverse) short.native_value else long;
            const scalar_left = if (reverse) prepared else short.scalar_value;
            const scalar_right = if (reverse) short.scalar_value else prepared;
            var fw: arrays.Budget = .{};
            var sw: arrays.Budget = .{};
            try std.testing.expectEqual(try scalar_left.compare(scalar_right, &sw), try left.compare(right, &fw));
            try std.testing.expectEqual(sw.remaining, fw.remaining);
        }
    };
}
