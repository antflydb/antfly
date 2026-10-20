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

//! JSON instance equality and canonical fingerprints. Numbers normalize as
//! decimals without rounding integer/string representations through f64.
const std = @import("std");
const A = std.mem.Allocator;
const V = std.json.Value;
const Big = std.math.big.int.Managed;

pub fn isNumber(value: V) bool {
    return value == .integer or value == .float or value == .number_string;
}
const Decimal = struct {
    negative: bool,
    storage: []u8,
    digits: []const u8,
    order: Big,
    fn init(a: A, value: V) !Decimal {
        var buffer: [128]u8 = undefined;
        const raw = if (value == .number_string) value.number_string else try std.fmt.bufPrint(&buffer, "{f}", .{std.json.fmt(value, .{})});
        if (value == .float and !std.math.isFinite(value.float)) return error.InvalidBatchRequest;
        // Validate number strings too: they can enter through native callers.
        var scanner = std.json.Scanner.initCompleteInput(a, raw);
        defer scanner.deinit();
        const token = try scanner.next();
        if (token != .number or try scanner.next() != .end_of_document) return error.InvalidBatchRequest;
        const exponent_start = std.mem.indexOfAny(u8, raw, "eE") orelse raw.len;
        const negative = raw[0] == '-';
        const mantissa = raw[@intFromBool(negative)..exponent_start];
        const storage = try a.alloc(u8, mantissa.len);
        errdefer a.free(storage);
        var size: usize = 0;
        var fraction: usize = 0;
        var after_dot = false;
        for (mantissa) |byte| {
            if (byte == '.') {
                after_dot = true;
                continue;
            }
            storage[size] = byte;
            size += 1;
            if (after_dot) fraction += 1;
        }
        var first: usize = 0;
        while (first < size and storage[first] == '0') first += 1;
        var last = size;
        while (last > first and storage[last - 1] == '0') last -= 1;
        var order = try Big.init(a);
        errdefer order.deinit();
        if (first == size) {
            try order.set(0);
            return .{ .negative = false, .storage = storage, .digits = storage[0..0], .order = order };
        }
        if (exponent_start < raw.len) {
            const exponent = raw[exponent_start + 1 ..];
            try order.setString(10, if (exponent[0] == '+') exponent[1..] else exponent);
        } else try order.set(0);
        try order.addScalar(&order, @as(i64, @intCast(size - first)) - @as(i64, @intCast(fraction)));
        return .{ .negative = negative, .storage = storage, .digits = storage[first..last], .order = order };
    }
    fn deinit(self: *Decimal, a: A) void {
        self.order.deinit();
        a.free(self.storage);
    }
};

pub fn numberOrder(a: A, left: V, right: V) !std.math.Order {
    if (left == .integer and right == .integer) return std.math.order(left.integer, right.integer);
    var l = try Decimal.init(a, left);
    defer l.deinit(a);
    var r = try Decimal.init(a, right);
    defer r.deinit(a);
    if (l.negative != r.negative) return if (l.negative) .lt else .gt;
    var order: std.math.Order = if (l.digits.len == 0 or r.digits.len == 0)
        std.math.order(l.digits.len, r.digits.len)
    else
        l.order.order(r.order);
    if (order == .eq) {
        var i: usize = 0;
        while (i < @max(l.digits.len, r.digits.len)) : (i += 1) {
            order = std.math.order(if (i < l.digits.len) l.digits[i] else '0', if (i < r.digits.len) r.digits[i] else '0');
            if (order != .eq) break;
        }
    }
    return if (l.negative) order.invert() else order;
}
pub fn integral(a: A, value: V) !bool {
    if (value == .integer) return true;
    var decimal = try Decimal.init(a, value);
    defer decimal.deinit(a);
    return decimal.digits.len == 0 or decimal.order.toConst().orderAgainstScalar(decimal.digits.len) != .lt;
}
pub fn equal(a: A, left: V, right: V) anyerror!bool {
    if (isNumber(left) and isNumber(right)) return try numberOrder(a, left, right) == .eq;
    if (std.meta.activeTag(left) != std.meta.activeTag(right)) return false;
    return switch (left) {
        .null => true,
        .bool => left.bool == right.bool,
        .string => std.mem.eql(u8, left.string, right.string),
        .array => blk: {
            if (left.array.items.len != right.array.items.len) break :blk false;
            for (left.array.items, right.array.items) |l, r| if (!try equal(a, l, r)) break :blk false;
            break :blk true;
        },
        .object => blk: {
            if (left.object.count() != right.object.count()) break :blk false;
            for (left.object.keys(), left.object.values()) |key, l| {
                if (!try equal(a, l, right.object.get(key) orelse break :blk false)) break :blk false;
            }
            break :blk true;
        },
        else => unreachable,
    };
}

/// Canonical semantic hash, followed by equality checks on collisions. This
/// keeps uniqueItems linear in input size rather than comparing every pair.
pub fn fingerprint(a: A, value: V) anyerror![32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    if (isNumber(value)) {
        hash.update("number");
        var decimal = try Decimal.init(a, value);
        defer decimal.deinit(a);
        hash.update(&.{@intFromBool(decimal.negative)});
        hash.update(decimal.digits);
        hash.update("e");
        const exponent = try decimal.order.toString(a, 10, .lower);
        defer a.free(exponent);
        hash.update(exponent);
    } else switch (value) {
        .null => hash.update("null"),
        .bool => hash.update(if (value.bool) "true" else "false"),
        .string => {
            hash.update("string");
            hash.update(value.string);
        },
        .array => {
            hash.update("array");
            for (value.array.items) |item| hash.update(&try fingerprint(a, item));
        },
        .object => {
            hash.update("object");
            // Map order has no meaning in JSON equality. Sort borrowed keys,
            // without modifying the caller's value or serializing its payload.
            const keys = try a.dupe([]const u8, value.object.keys());
            defer a.free(keys);
            std.mem.sort([]const u8, keys, {}, struct {
                fn less(_: void, l: []const u8, r: []const u8) bool {
                    return std.mem.order(u8, l, r) == .lt;
                }
            }.less);
            for (keys) |key| {
                var key_hash: [32]u8 = undefined;
                std.crypto.hash.Blake3.hash(key, &key_hash, .{});
                hash.update(&key_hash);
                hash.update(&try fingerprint(a, value.object.get(key).?));
            }
        },
        else => unreachable,
    }
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return digest;
}
