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

//! Allocation-free bulk operations over storage identifiers.
const std = @import("std");
pub const Operation = enum(u32) { union_set = 0, intersection = 1, difference = 2 };
pub fn sort(comptime T: type, values: []T) void {
    std.mem.sortUnstable(T, values, {}, std.sort.asc(T));
}
pub fn unique(comptime T: type, values: []T) usize {
    if (values.len == 0) return 0;
    var count: usize = 1;
    for (values[1..]) |value| {
        if (values[count - 1] == value) continue;
        values[count] = value;
        count += 1;
    }
    return count;
}
// Inputs are sorted and unique; output is a separate caller-owned buffer.
pub fn merge(comptime operation: Operation, left: []const u32, right: []const u32, out: []u32) usize {
    const required = switch (operation) {
        .union_set => left.len + right.len,
        .intersection => @min(left.len, right.len),
        .difference => left.len,
    };
    std.debug.assert(out.len >= required);
    var i: usize = 0;
    var j: usize = 0;
    var count: usize = 0;
    while (i < left.len or j < right.len) {
        if (i == left.len) {
            if (operation == .union_set) {
                @memcpy(out[count..][0 .. right.len - j], right[j..]);
                count += right.len - j;
            }
            break;
        }
        if (j == right.len) {
            if (operation != .intersection) {
                @memcpy(out[count..][0 .. left.len - i], left[i..]);
                count += left.len - i;
            }
            break;
        }
        if (left[i] < right[j]) {
            if (operation != .intersection) {
                out[count] = left[i];
                count += 1;
            }
            i += 1;
        } else if (right[j] < left[i]) {
            if (operation == .union_set) {
                out[count] = right[j];
                count += 1;
            }
            j += 1;
        } else {
            if (operation != .difference) {
                out[count] = left[i];
                count += 1;
            }
            i += 1;
            j += 1;
        }
    }
    return count;
}

test "bulk ordinal merge matches set membership for all operations" {
    for (0..128) |seed| {
        var a: [64]u32 = undefined;
        var b: [64]u32 = undefined;
        var alen: usize = 0;
        var blen: usize = 0;
        for (0..64) |value| {
            if ((value * 17 + seed) % 5 < 2) {
                a[alen] = @intCast(value);
                alen += 1;
            }
            if ((value * 23 + seed) % 7 < 3) {
                b[blen] = @intCast(value);
                blen += 1;
            }
        }
        inline for (.{ Operation.union_set, Operation.intersection, Operation.difference }) |operation| {
            var output: [128]u32 = @splat(0xdeadbeef);
            const count = merge(operation, a[0..alen], b[0..blen], &output);
            var expected: usize = 0;
            for (0..64) |value| {
                const in_a = std.mem.indexOfScalar(u32, a[0..alen], @intCast(value)) != null;
                const in_b = std.mem.indexOfScalar(u32, b[0..blen], @intCast(value)) != null;
                const present = switch (operation) {
                    .union_set => in_a or in_b,
                    .intersection => in_a and in_b,
                    .difference => in_a and !in_b,
                };
                if (present) {
                    try std.testing.expectEqual(@as(u32, @intCast(value)), output[expected]);
                    expected += 1;
                }
            }
            try std.testing.expectEqual(expected, count);
            try std.testing.expectEqual(@as(u32, 0xdeadbeef), output[count]);
        }
    }
}
