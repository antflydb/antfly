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

//! Owned JSON values for contracts that require exact decimal semantics.
//! Keep fractional/exponent literals and large integers as number_string;
//! canonical small integer tokens retain the normal integer representation.
const std = @import("std");
const A = std.mem.Allocator;
const V = std.json.Value;

pub fn parse(a: A, raw: []const u8) !std.json.Parsed(V) {
    var parsed = try std.json.parseFromSlice(V, a, raw, .{ .parse_numbers = false, .allocate = .alloc_always });
    errdefer parsed.deinit();
    try normalizeIntegers(parsed.arena.allocator(), &parsed.value);
    return parsed;
}

/// All allocations belong to the caller's arena, including error-path scratch.
pub fn parseLeaky(a: A, raw: []const u8) !V {
    var value = try std.json.parseFromSliceLeaky(V, a, raw, .{ .parse_numbers = false, .allocate = .alloc_always });
    try normalizeIntegers(a, &value);
    return value;
}

// Walk with heap-backed slices rather than recursion: JSON itself can nest
// much deeper than a constrained schema, including in ignored reply fields.
fn normalizeIntegers(a: A, value: *V) !void {
    var root = [_]V{value.*};
    var current: []V = &root;
    var parents: std.ArrayList([]V) = .empty;
    defer parents.deinit(a);
    while (true) {
        if (current.len == 0) {
            current = parents.pop() orelse break;
            continue;
        }
        const node = &current[0];
        current = current[1..];
        const children: []V = switch (node.*) {
            .object => node.object.values(),
            .array => node.array.items,
            .number_string => |raw| blk: {
                if (std.mem.indexOfAny(u8, raw, ".eE") == null) {
                    if (std.fmt.parseInt(i64, raw, 10)) |integer| node.* = .{ .integer = integer } else |_| {}
                }
                break :blk &.{};
            },
            else => &.{},
        };
        if (children.len != 0) {
            if (current.len != 0) try parents.append(a, current);
            current = children;
        }
    }
    value.* = root[0];
}

test "lossless JSON retains decimals exponents and large integers through serialization" {
    const a = std.testing.allocator;
    const raw = "{\"values\":[1.0000000000000001,1e-400,1e100000,18446744073709551615,1.0,-0e0,1]}";
    var parsed = try parse(a, raw);
    defer parsed.deinit();
    const values = parsed.value.object.get("values").?.array.items;
    for (values[0..6]) |value| try std.testing.expect(value == .number_string);
    try std.testing.expectEqual(@as(i64, 1), values[6].integer);
    const encoded = try std.json.Stringify.valueAlloc(a, parsed.value, .{});
    defer a.free(encoded);
    try std.testing.expectEqualStrings(raw, encoded);
}

fn allocationSweep(a: A) !void {
    var parsed = try parse(a, "{\"a\":[1.0000000000000001,1e-400,1],\"nested\":{\"value\":18446744073709551615}}");
    defer parsed.deinit();
}

test "lossless JSON releases every failed owned parse" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationSweep, .{});
}

test "lossless JSON normalization handles deep untrusted nesting without call stack growth" {
    const a = std.testing.allocator;
    var raw: std.ArrayList(u8) = .empty;
    defer raw.deinit(a);
    try raw.appendNTimes(a, '[', 6000);
    try raw.appendSlice(a, "1e-400");
    try raw.appendNTimes(a, ']', 6000);
    var parsed = try parse(a, raw.items);
    defer parsed.deinit();
    var value = parsed.value;
    for (0..6000) |_| value = value.array.items[0];
    try std.testing.expectEqualStrings("1e-400", value.number_string);
}
