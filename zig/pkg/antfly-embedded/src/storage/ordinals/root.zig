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

//! One native call per ordinal batch; buffers and allocation stay with callers.
pub const operations = @import("operations.zig");
const std = @import("std");
const native = @import("storage_ordinal_options").use_native_ordinal_core;
pub const Operation = operations.Operation;
extern fn antfly_storage_sort_u32_v1([*]u32, usize) callconv(.c) void;
extern fn antfly_storage_sort_u64_v1([*]u64, usize) callconv(.c) void;
extern fn antfly_storage_unique_u32_v1([*]u32, usize) callconv(.c) usize;
extern fn antfly_storage_union_u32_v1([*]const u32, usize, [*]const u32, usize, [*]u32, usize) callconv(.c) usize;
extern fn antfly_storage_intersection_u32_v1([*]const u32, usize, [*]const u32, usize, [*]u32, usize) callconv(.c) usize;
extern fn antfly_storage_difference_u32_v1([*]const u32, usize, [*]const u32, usize, [*]u32, usize) callconv(.c) usize;
pub inline fn sort(comptime T: type, values: []T) void {
    if (values.len < 2) return;
    if (native) {
        // pdqsort uses insertion sorting for these small inputs too. Keep
        // that fast path local instead of paying a native call per tiny set.
        if (values.len <= 24) return std.sort.insertion(T, values, {}, std.sort.asc(T));
        if (T == u32) return antfly_storage_sort_u32_v1(values.ptr, values.len);
        if (T == u64) return antfly_storage_sort_u64_v1(values.ptr, values.len);
        @compileError("unsupported storage ordinal width");
    }
    operations.sort(T, values);
}

test "configured ordinal worker preserves scalar extremes and empty merges" {
    var small = [_]u64{ 0xffffffffffffffff, 0, 0xffffffff, 0 };
    sort(u64, &small);
    try std.testing.expectEqualSlices(u64, &.{ 0, 0, 0xffffffff, 0xffffffffffffffff }, &small);
    var large: [128]u32 = undefined;
    for (&large, 0..) |*value, i| value.* = @intCast((127 - i) / 2);
    sort(u32, &large);
    const count = unique(&large);
    try std.testing.expectEqual(@as(usize, 64), count);
    for (large[0..count], 0..) |value, i| try std.testing.expectEqual(@as(u32, @intCast(i)), value);
    var output: [128]u32 = undefined;
    try std.testing.expectEqual(@as(usize, 0), merge(.intersection, &.{}, large[0..count], &output));
    try std.testing.expectEqual(count, merge(.union_set, &.{}, large[0..count], &output));
    try std.testing.expectEqualSlices(u32, large[0..count], output[0..count]);
    try std.testing.expectEqual(count, merge(.difference, large[0..count], &.{}, &output));
    try std.testing.expectEqualSlices(u32, large[0..count], output[0..count]);
    var other: [128]u32 = undefined;
    for (&other, 0..) |*value, i| value.* = @intCast(i * 2);
    var merged: [256]u32 = undefined;
    var expected: [256]u32 = undefined;
    inline for (.{ Operation.union_set, Operation.intersection, Operation.difference }) |operation| {
        const actual_count = merge(operation, large[0..count], &other, &merged);
        const expected_count = operations.merge(operation, large[0..count], &other, &expected);
        try std.testing.expectEqual(expected_count, actual_count);
        try std.testing.expectEqualSlices(u32, expected[0..expected_count], merged[0..actual_count]);
    }
    var wide: [64]u64 = undefined;
    for (&wide, 0..) |*value, i| value.* = 0xffffffffffffffff - @as(u64, @intCast(i));
    sort(u64, &wide);
    for (wide, 0..) |value, i| try std.testing.expectEqual(@as(u64, 0xffffffffffffffff - 63 + i), value);
}
pub inline fn unique(values: []u32) usize {
    if (values.len < 2) return values.len;
    if (native) return antfly_storage_unique_u32_v1(values.ptr, values.len);
    return operations.unique(u32, values);
}
pub inline fn merge(operation: Operation, left: []const u32, right: []const u32, out: []u32) usize {
    // Union remains local: the measured native union regressed throughput.
    if (native and operation != .union_set and left.len + right.len > 128) return switch (operation) {
        .union_set => antfly_storage_union_u32_v1(left.ptr, left.len, right.ptr, right.len, out.ptr, out.len),
        .intersection => antfly_storage_intersection_u32_v1(left.ptr, left.len, right.ptr, right.len, out.ptr, out.len),
        .difference => antfly_storage_difference_u32_v1(left.ptr, left.len, right.ptr, right.len, out.ptr, out.len),
    };
    return switch (operation) {
        inline else => |tag| operations.merge(tag, left, right, out),
    };
}
