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
        if (token != .number or !std.mem.eql(u8, token.number, raw) or try scanner.next() != .end_of_document) return error.InvalidBatchRequest;
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
/// Convert a mathematical JSON integer without truncation, float rounding, or
/// expanding an arbitrary exponent. The bound belongs to the typed consumer.
pub fn unsignedInteger(a: A, value: V, maximum: u64) !u64 {
    if (value == .integer) {
        const result = std.math.cast(u64, value.integer) orelse return error.InvalidBatchRequest;
        return if (result <= maximum) result else error.InvalidBatchRequest;
    }
    if (!isNumber(value)) return error.InvalidBatchRequest;
    var decimal = Decimal.init(a, value) catch |err| return if (err == error.OutOfMemory) err else error.InvalidBatchRequest;
    defer decimal.deinit(a);
    if (decimal.digits.len == 0) return 0;
    if (decimal.negative or decimal.order.toConst().orderAgainstScalar(decimal.digits.len) == .lt) return error.InvalidBatchRequest;
    const places = decimal.order.toConst().toInt(u8) catch return error.InvalidBatchRequest;
    if (places > 20) return error.InvalidBatchRequest;
    var result: u64 = 0;
    for (0..places) |index| {
        const digit = if (index < decimal.digits.len) decimal.digits[index] - '0' else 0;
        result = std.math.mul(u64, result, 10) catch return error.InvalidBatchRequest;
        result = std.math.add(u64, result, digit) catch return error.InvalidBatchRequest;
    }
    return if (result <= maximum) result else error.InvalidBatchRequest;
}

pub fn equal(a: A, left: V, right: V) !bool {
    // Preserve allocation-free validation for the common scalar enum/const
    // case; containers alone need a work stack.
    if (isNumber(left) and isNumber(right)) return try numberOrder(a, left, right) == .eq;
    if (std.meta.activeTag(left) != std.meta.activeTag(right)) return false;
    switch (left) {
        .null => return true,
        .bool => return left.bool == right.bool,
        .string => return std.mem.eql(u8, left.string, right.string),
        .array, .object => {},
        else => unreachable,
    }
    const Pair = struct { left: V, right: V };
    var pending: std.ArrayList(Pair) = .empty;
    defer pending.deinit(a);
    try pending.append(a, .{ .left = left, .right = right });
    while (pending.pop()) |pair| {
        const l = pair.left;
        const r = pair.right;
        if (isNumber(l) and isNumber(r)) {
            if (try numberOrder(a, l, r) != .eq) return false;
            continue;
        }
        if (std.meta.activeTag(l) != std.meta.activeTag(r)) return false;
        switch (l) {
            .null => {},
            .bool => if (l.bool != r.bool) return false,
            .string => if (!std.mem.eql(u8, l.string, r.string)) return false,
            .array => {
                if (l.array.items.len != r.array.items.len) return false;
                for (l.array.items, r.array.items) |lv, rv| try pending.append(a, .{ .left = lv, .right = rv });
            },
            .object => {
                if (l.object.count() != r.object.count()) return false;
                for (l.object.keys(), l.object.values()) |key, lv| try pending.append(a, .{ .left = lv, .right = r.object.get(key) orelse return false });
            },
            else => unreachable,
        }
    }
    return true;
}

fn hashSize(hash: *std.crypto.hash.Blake3, size: usize) void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, @intCast(size), .little);
    hash.update(&bytes);
}

/// Canonical semantic hash, followed by equality checks on collisions. An
/// explicit work stack handles arbitrary instance depth without using the call
/// stack. Length framing distinguishes keys, scalar payloads and containers.
pub fn fingerprint(a: A, value: V) ![32]u8 {
    const Event = union(enum) { value: V, key: []const u8 };
    var pending: std.ArrayList(Event) = .empty;
    defer pending.deinit(a);
    try pending.append(a, .{ .value = value });
    var hash = std.crypto.hash.Blake3.init(.{});
    while (pending.pop()) |event| {
        if (event == .key) {
            hash.update("key");
            hashSize(&hash, event.key.len);
            hash.update(event.key);
            continue;
        }
        const next = event.value;
        if (isNumber(next)) {
            hash.update("number");
            var decimal = try Decimal.init(a, next);
            defer decimal.deinit(a);
            hash.update(&.{@intFromBool(decimal.negative)});
            hashSize(&hash, decimal.digits.len);
            hash.update(decimal.digits);
            const exponent = try decimal.order.toString(a, 10, .lower);
            defer a.free(exponent);
            hashSize(&hash, exponent.len);
            hash.update(exponent);
        } else switch (next) {
            .null => hash.update("null"),
            .bool => hash.update(if (next.bool) "true" else "false"),
            .string => {
                hash.update("string");
                hashSize(&hash, next.string.len);
                hash.update(next.string);
            },
            .array => {
                hash.update("array");
                hashSize(&hash, next.array.items.len);
                var index = next.array.items.len;
                while (index > 0) {
                    index -= 1;
                    try pending.append(a, .{ .value = next.array.items[index] });
                }
            },
            .object => {
                hash.update("object");
                hashSize(&hash, next.object.count());
                const keys = try a.dupe([]const u8, next.object.keys());
                defer a.free(keys);
                std.mem.sort([]const u8, keys, {}, struct {
                    fn less(_: void, l: []const u8, r: []const u8) bool {
                        return std.mem.order(u8, l, r) == .lt;
                    }
                }.less);
                var index = keys.len;
                while (index > 0) {
                    index -= 1;
                    const key = keys[index];
                    try pending.append(a, .{ .value = next.object.get(key).? });
                    try pending.append(a, .{ .key = key });
                }
            },
            else => unreachable,
        }
    }
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return digest;
}
