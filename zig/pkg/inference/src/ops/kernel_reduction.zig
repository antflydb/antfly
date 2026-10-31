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

//! Target-owned reduction schedules. Sharing a tree never changes its lane
//! order or silently substitutes a backend's differently ordered intrinsic.
const std = @import("std");
pub const Operation = enum { sum, maximum };
pub const Result = enum { lane_zero, broadcast };
pub const Qualifier = enum { device, static_device };
pub const warp_steps = [_]u5{ 16, 8, 4, 2, 1 };

/// Keep full-warp participation and the existing shuffle-down tree explicit.
/// The caller must arrange that every lane participates under the full mask.
pub fn cudaWarpSource(comptime name: []const u8, comptime operation: Operation, comptime result: Result, comptime qualifier: Qualifier, comptime value: []const u8) []const u8 {
    @setEvalBranchQuota(100000);
    comptime var text: []const u8 = (if (qualifier == .static_device) "static " else "") ++ "__device__ __forceinline__ float " ++ name ++ "(float " ++ value ++ ") {\n";
    inline for (warp_steps) |step| {
        const shuffle = std.fmt.comptimePrint("__shfl_down_sync(0xffffffffu, {s}, {d})", .{ value, step });
        text = text ++ "    " ++ value ++ (if (operation == .sum) " += " ++ shuffle else " = fmaxf(" ++ value ++ ", " ++ shuffle ++ ")") ++ ";\n";
    }
    return text ++ "    return " ++ (if (result == .broadcast) "__shfl_sync(0xffffffffu, " ++ value ++ ", 0)" else value) ++ ";\n}";
}

/// Host oracle for the same shuffle schedule. Out-of-range shuffle-down lanes
/// read their own value, as CUDA does for an active full warp.
pub fn evaluate(operation: Operation, result: Result, input: [32]f32) [32]f32 {
    var values = input;
    for (warp_steps) |step| {
        const previous = values;
        for (&values, 0..) |*value, lane| {
            const other = previous[if (lane + step < 32) lane + step else lane];
            value.* = switch (operation) {
                .sum => previous[lane] + other,
                .maximum => @max(previous[lane], other),
            };
        }
    }
    return if (result == .broadcast) @splat(values[0]) else values;
}

test "shared reduction schedules preserve shuffle order and broadcast ownership" {
    var values: [32]f32 = undefined;
    for (&values, 0..) |*value, lane| value.* = @as(f32, @floatFromInt(lane)) - 15;
    // Known sum=16, maximum=16. A lane-zero result does not promise that
    // the other lanes contain the aggregate.
    try std.testing.expectEqual(@as(f32, 16), evaluate(.sum, .lane_zero, values)[0]);
    try std.testing.expect(evaluate(.sum, .lane_zero, values)[1] != 16);
    for (evaluate(.maximum, .broadcast, values)) |value| try std.testing.expectEqual(@as(f32, 16), value);
    const source = comptime cudaWarpSource("reduce", .sum, .lane_zero, .static_device, "value");
    try std.testing.expectEqual(@as(usize, 5), std.mem.count(u8, source, "__shfl_down_sync"));
    try std.testing.expectEqual(@as(usize, 0), std.mem.count(u8, source, "return __shfl_sync"));
}
