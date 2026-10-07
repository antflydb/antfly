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

//! Allocation-free bounded structural JSON ordering and semantic hashing.
//! Object order is independent of insertion order; decimal number tokens are
//! compared exactly, without collapsing integers through floating point.
const std = @import("std");
const Json = std.json.Value;
const Order = std.math.Order;
/// Validate nesting before a dynamic JSON tree is constructed. The scanner
/// owns only a bounded nesting stack; strings and numeric tokens are borrowed.
pub fn admitText(a: std.mem.Allocator, text: []const u8, budget: *Budget) !void {
    try budget.consume(text.len);
    var scanner = std.json.Scanner.initCompleteInput(a, text);
    defer scanner.deinit();
    var depth: usize = 0;
    while (true) {
        const token = scanner.next() catch |err| return switch (err) {
            error.OutOfMemory => err,
            else => error.SqlInvalidTextRepresentation,
        };
        try budget.consume(1);
        switch (token) {
            .object_begin, .array_begin => {
                depth += 1;
                if (depth > 64) return error.SqlProgramLimitExceeded;
            },
            .object_end, .array_end => {
                if (depth == 0) return error.SqlInvalidTextRepresentation;
                depth -= 1;
            },
            .end_of_document => return,
            else => {},
        }
    }
}

pub const Budget = struct {
    remaining: usize = 1_048_576,
    pub fn consume(self: *Budget, amount: usize) !void {
        if (amount > self.remaining) return error.SqlProgramLimitExceeded;
        self.remaining -= amount;
    }
};

/// PostgreSQL JSONB strings and object keys obey the text domain recursively.
/// Share this allocation-free walk between arrays, native ingress and restore.
pub fn validateTextDomain(value: Json, budget: *Budget, depth: usize) !void {
    try budget.consume(1);
    if (depth > 64) return error.SqlProgramLimitExceeded;
    switch (value) {
        .string => |text| try validateText(text, budget),
        .array => |items| for (items.items) |item| try validateTextDomain(item, budget, depth + 1),
        .object => |items| {
            for (items.keys(), items.values()) |key, item| {
                try validateText(key, budget);
                try validateTextDomain(item, budget, depth + 1);
            }
        },
        else => {},
    }
}

fn validateText(text: []const u8, budget: *Budget) !void {
    try budget.consume(text.len);
    if (!std.unicode.utf8ValidateSlice(text) or std.mem.indexOfScalar(u8, text, 0) != null) return error.SqlTypeMismatch;
}

/// The caller owns the allocation region and its byte admission. All retained
/// tokens own their bytes, including exact decimal tokens and escaped strings.
pub fn parseTextLeaky(a: std.mem.Allocator, text: []const u8, budget: *Budget) !Json {
    try admitText(a, text, budget);
    return std.json.parseFromSliceLeaky(Json, a, text, .{ .allocate = .alloc_always, .parse_numbers = false, .max_value_len = text.len }) catch |err| return switch (err) {
        error.OutOfMemory => err,
        else => error.SqlInvalidTextRepresentation,
    };
}

/// A temporary quota allocator may own admission, but managed JSON arrays
/// must retain the enclosing region's stable allocator, not its stack wrapper.
pub fn rehomeArrayAllocators(value: *Json, owner: std.mem.Allocator, budget: *Budget, depth: usize) !void {
    try budget.consume(1);
    if (depth > 64) return error.SqlProgramLimitExceeded;
    switch (value.*) {
        .array => |*items| {
            items.allocator = owner;
            for (items.items) |*item| try rehomeArrayAllocators(item, owner, budget, depth + 1);
        },
        .object => |*items| for (items.values()) |*item| try rehomeArrayAllocators(item, owner, budget, depth + 1),
        else => {},
    }
}

test "JSON text admission bounds nesting before DOM allocation and unwinds faults" {
    const a = std.testing.allocator;
    var nested: [131]u8 = undefined;
    @memset(nested[0..65], '[');
    nested[65] = '0';
    @memset(nested[66..], ']');
    var budget: Budget = .{};
    try std.testing.expectError(error.SqlProgramLimitExceeded, parseTextLeaky(a, &nested, &budget));
    budget = .{};
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    _ = try parseTextLeaky(arena.allocator(), nested[1..130], &budget);
    budget = .{ .remaining = 1 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, admitText(a, "[0]", &budget));
    budget = .{};
    try std.testing.expectError(error.SqlInvalidTextRepresentation, admitText(a, "[0,]", &budget));
    const Faults = struct {
        fn run(backing: std.mem.Allocator) !void {
            var region = std.heap.ArenaAllocator.init(backing);
            defer region.deinit();
            var work: Budget = .{};
            _ = try parseTextLeaky(region.allocator(), "{\"n\":9007199254740993,\"s\":\"escaped\\ntext\",\"a\":[null,true]}", &work);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Faults.run, .{});
}

const Decimal = struct {
    negative: bool,
    magnitude: i64,
    digits: []const u8,

    fn parse(text: []const u8, budget: *Budget) !Decimal {
        try budget.consume(text.len);
        if (text.len == 0) return error.SqlTypeMismatch;
        var at: usize = @intFromBool(text[0] == '-');
        const start = at;
        var before: i64 = 0;
        var leading: i64 = 0;
        var seen_dot = false;
        var first: ?usize = null;
        var last: usize = 0;
        while (at < text.len and text[at] != 'e' and text[at] != 'E') : (at += 1) {
            if (text[at] == '.' and !seen_dot) {
                seen_dot = true;
                continue;
            }
            if (!std.ascii.isDigit(text[at])) return error.SqlTypeMismatch;
            if (!seen_dot) before += 1;
            if (text[at] != '0') {
                if (first == null) first = at;
                last = at;
            } else if (first == null) leading += 1;
        }
        if (at == start) return error.SqlTypeMismatch;
        const exponent = if (at < text.len) std.fmt.parseInt(i64, text[at + 1 ..], 10) catch return error.SqlNumericOutOfRange else 0;
        if (first == null) return .{ .negative = false, .magnitude = 0, .digits = "" };
        const magnitude = std.math.add(i64, exponent, before - leading - 1) catch return error.SqlNumericOutOfRange;
        return .{ .negative = text[0] == '-', .magnitude = magnitude, .digits = text[first.? .. last + 1] };
    }

    fn compare(a: Decimal, b: Decimal) Order {
        if (a.negative != b.negative) return if (a.negative) .lt else .gt;
        if (a.digits.len == 0 or b.digits.len == 0) return if (a.digits.len == b.digits.len) .eq else if (a.digits.len == 0) (if (b.negative) .gt else .lt) else if (a.negative) .lt else .gt;
        var order = std.math.order(a.magnitude, b.magnitude);
        if (order == .eq) {
            var i: usize = 0;
            var j: usize = 0;
            while (i < a.digits.len or j < b.digits.len) {
                if (i < a.digits.len and a.digits[i] == '.') i += 1;
                if (j < b.digits.len and b.digits[j] == '.') j += 1;
                const ac: u8 = if (i < a.digits.len) a.digits[i] else '0';
                const bc: u8 = if (j < b.digits.len) b.digits[j] else '0';
                order = std.math.order(ac, bc);
                if (order != .eq) break;
                i += @intFromBool(i < a.digits.len);
                j += @intFromBool(j < b.digits.len);
            }
        }
        return if (a.negative) order.invert() else order;
    }

    fn hash(self: Decimal) u64 {
        var state = std.hash.Wyhash.init(2);
        state.update(&.{@intFromBool(self.negative)});
        var magnitude: [8]u8 = undefined;
        std.mem.writeInt(i64, &magnitude, self.magnitude, .little);
        state.update(&magnitude);
        for (self.digits) |digit| if (digit != '.') state.update(&.{digit});
        return state.final();
    }
};

/// PostgreSQL NUMERIC-to-integer rounding (ties away from zero), without a
/// float intermediate. Work is bounded by input bytes plus at most 20 digits.
pub fn roundedInteger(text: []const u8, budget: *Budget) !i64 {
    const value = try Decimal.parse(text, budget);
    if (value.digits.len == 0 or value.magnitude < -1) return 0;
    if (value.magnitude > 18) return error.SqlNumericOutOfRange;
    var at: usize = 0;
    var magnitude: u64 = 0;
    const count: usize = @intCast(@max(0, value.magnitude + 1));
    for (0..count) |_| {
        if (at < value.digits.len and value.digits[at] == '.') at += 1;
        const digit = if (at < value.digits.len) value.digits[at] - '0' else 0;
        magnitude = magnitude * 10 + digit;
        at += @intFromBool(at < value.digits.len);
    }
    if (at < value.digits.len and value.digits[at] == '.') at += 1;
    if (at < value.digits.len and value.digits[at] >= '5') magnitude += 1;
    if (magnitude > @as(u64, std.math.maxInt(i64)) + @intFromBool(value.negative)) return error.SqlNumericOutOfRange;
    if (value.negative and magnitude == @as(u64, 1) << 63) return std.math.minInt(i64);
    return if (value.negative) -@as(i64, @intCast(magnitude)) else @intCast(magnitude);
}

fn rank(value: Json) u8 {
    return switch (value) {
        .null => 0,
        .string => 1,
        .integer, .float, .number_string => 2,
        .bool => 3,
        .array => 4,
        .object => 5,
    };
}
fn decimal(value: Json, buffer: []u8, budget: *Budget) !Decimal {
    const text = switch (value) {
        .integer => |v| try std.fmt.bufPrint(buffer, "{d}", .{v}),
        .float => |v| {
            if (!std.math.isFinite(v)) return error.SqlNumericOutOfRange;
            if (v == 0) return .{ .negative = false, .magnitude = 0, .digits = "" };
            const bits: u64 = @bitCast(v);
            const raw_exponent = (bits >> 52) & 0x7ff;
            var mantissa: u64 = bits & 0xfffffffffffff;
            if (raw_exponent != 0) mantissa |= 1 << 52;
            var exponent: i32 = if (raw_exponent == 0) -1074 else @as(i32, @intCast(raw_exponent)) - 1023 - 52;
            while (exponent < 0 and mantissa & 1 == 0) {
                mantissa >>= 1;
                exponent += 1;
            }
            var exact: u4096 = mantissa;
            if (exponent >= 0) exact <<= @intCast(exponent) else {
                try budget.consume(@intCast(-exponent));
                for (0..@intCast(-exponent)) |_| exact *= 5;
            }
            var result = try Decimal.parse(try std.fmt.bufPrint(buffer, "{d}", .{exact}), budget);
            result.negative = bits >> 63 != 0;
            if (exponent < 0) result.magnitude += exponent;
            return result;
        },
        .number_string => |v| v,
        else => return error.SqlTypeMismatch,
    };
    return Decimal.parse(text, budget);
}
fn keyOrder(a: []const u8, b: []const u8) Order {
    const lengths = std.math.order(a.len, b.len);
    return if (lengths == .eq) std.mem.order(u8, a, b) else lengths;
}

pub fn compare(a: Json, b: Json, budget: *Budget, depth: usize) anyerror!Order {
    if (depth > 64) return error.SqlProgramLimitExceeded;
    try budget.consume(1);
    const ranks = std.math.order(rank(a), rank(b));
    if (ranks != .eq) return ranks;
    if (rank(a) == 2) {
        var left: [768]u8 = undefined;
        var right: [768]u8 = undefined;
        return Decimal.compare(try decimal(a, &left, budget), try decimal(b, &right, budget));
    }
    return switch (a) {
        .null => .eq,
        .bool => std.math.order(@intFromBool(a.bool), @intFromBool(b.bool)),
        .string => blk: {
            try budget.consume(@min(a.string.len, b.string.len));
            break :blk std.mem.order(u8, a.string, b.string);
        },
        .array => blk: {
            const sizes = std.math.order(a.array.items.len, b.array.items.len);
            if (sizes != .eq) break :blk sizes;
            for (a.array.items, b.array.items) |left, right| {
                const result = try compare(left, right, budget, depth + 1);
                if (result != .eq) break :blk result;
            }
            break :blk .eq;
        },
        .object => blk: {
            const sizes = std.math.order(a.object.count(), b.object.count());
            if (sizes != .eq) break :blk sizes;
            var smallest: ?[]const u8 = null;
            var result: Order = .eq;
            for (a.object.keys(), a.object.values()) |key, value| {
                try budget.consume(key.len + 1);
                const other = b.object.get(key);
                const difference: Order = if (other) |item| try compare(value, item, budget, depth + 1) else .lt;
                if (difference != .eq and (smallest == null or keyOrder(key, smallest.?) == .lt)) {
                    smallest = key;
                    result = difference;
                }
            }
            for (b.object.keys()) |key| {
                try budget.consume(key.len + 1);
                if (!a.object.contains(key) and (smallest == null or keyOrder(key, smallest.?) == .lt)) {
                    smallest = key;
                    result = .gt;
                }
            }
            break :blk result;
        },
        else => unreachable,
    };
}

pub fn hash(value: Json, budget: *Budget, depth: usize) anyerror!u64 {
    if (depth > 64) return error.SqlProgramLimitExceeded;
    try budget.consume(1);
    if (rank(value) == 2) {
        var buffer: [768]u8 = undefined;
        return (try decimal(value, &buffer, budget)).hash();
    }
    var state = std.hash.Wyhash.init(rank(value));
    switch (value) {
        .null => {},
        .bool => state.update(&.{@intFromBool(value.bool)}),
        .string => {
            try budget.consume(value.string.len);
            state.update(value.string);
        },
        .array => for (value.array.items) |item| {
            var bytes: [8]u8 = undefined;
            std.mem.writeInt(u64, &bytes, try hash(item, budget, depth + 1), .little);
            state.update(&bytes);
        },
        .object => {
            var sum: u64 = 0;
            var mixed: u64 = 0;
            for (value.object.keys(), value.object.values()) |key, item| {
                try budget.consume(key.len + 1);
                const pair = std.hash.Wyhash.hash(try hash(item, budget, depth + 1), key);
                sum +%= pair;
                mixed ^= std.math.rotl(u64, pair, 23);
            }
            var bytes: [16]u8 = undefined;
            std.mem.writeInt(u64, bytes[0..8], sum, .little);
            std.mem.writeInt(u64, bytes[8..16], mixed, .little);
            state.update(&bytes);
        },
        else => unreachable,
    }
    return state.final();
}

test "structural JSON ordering and hashes ignore object order and exact numeric spelling" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const pairs = [_][2][]const u8{
        .{ "{\"b\":[1,2],\"a\":null}", "{\"a\":null,\"b\":[1.0,2e0]}" },
        .{ "-0.0", "0" },
        .{ "10000e-4", "1.0000" },
        .{ "9007199254740993", "9007199254740993.0" },
    };
    for (pairs) |pair| {
        const a = try std.json.parseFromSliceLeaky(Json, arena.allocator(), pair[0], .{ .parse_numbers = false });
        const b = try std.json.parseFromSliceLeaky(Json, arena.allocator(), pair[1], .{ .parse_numbers = false });
        var budget: Budget = .{};
        try std.testing.expectEqual(Order.eq, try compare(a, b, &budget, 0));
        try std.testing.expectEqual(try hash(a, &budget, 0), try hash(b, &budget, 0));
    }
    var budget: Budget = .{};
    try std.testing.expectEqual(Order.gt, try compare(.{ .number_string = "9007199254740993" }, .{ .float = 9007199254740992 }, &budget, 0));
    const rounded: Json = .{ .float = 1000000000000000128.0 };
    const exact: Json = .{ .integer = 1000000000000000128 };
    try std.testing.expectEqual(Order.eq, try compare(rounded, exact, &budget, 0));
    try std.testing.expectEqual(try hash(rounded, &budget, 0), try hash(exact, &budget, 0));
    var tiny: Budget = .{ .remaining = 1 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, compare(.{ .string = "abc" }, .{ .string = "abc" }, &tiny, 0));
}
