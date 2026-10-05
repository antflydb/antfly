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

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Op = enum {
    count,
    sum,
    sumsquares,
    avg,
    min,
    max,

    pub fn parse(text: []const u8) ?Op {
        const info = @typeInfo(Op).@"enum";
        inline for (info.field_names, info.field_values) |reflected_name, field_value| {
            if (std.mem.eql(u8, text, reflected_name)) return @fromBackingInt(@intCast(field_value));
        }
        return null;
    }
};

pub const AvgState = struct {
    sum: f64 = 0,
    count: i64 = 0,
};

pub fn encodeI64Alloc(alloc: Allocator, value: i64) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{}", .{value});
}

pub fn encodeF64Alloc(alloc: Allocator, value: f64) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{d}", .{value});
}

pub fn encodeAvgAlloc(alloc: Allocator, state: AvgState) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{d},{d}", .{ state.sum, state.count });
}

pub fn parseI64(bytes: []const u8) !i64 {
    return try std.fmt.parseInt(i64, bytes, 10);
}

pub fn parseF64(bytes: []const u8) !f64 {
    return try std.fmt.parseFloat(f64, bytes);
}

pub fn parseAvg(bytes: []const u8) !AvgState {
    const comma = std.mem.indexOfScalar(u8, bytes, ',') orelse return error.InvalidAggregateValue;
    return .{
        .sum = try parseF64(bytes[0..comma]),
        .count = try parseI64(bytes[comma + 1 ..]),
    };
}

pub fn addCount(old: ?[]const u8, delta: i64) !i64 {
    const prior = if (old) |bytes| try parseI64(bytes) else 0;
    return prior + delta;
}

pub fn addSum(old: ?[]const u8, delta: f64) !f64 {
    const prior = if (old) |bytes| try parseF64(bytes) else 0;
    return prior + delta;
}

pub fn addAvg(old: ?[]const u8, sum_delta: f64, count_delta: i64) !AvgState {
    var prior = if (old) |bytes| try parseAvg(bytes) else AvgState{};
    prior.sum += sum_delta;
    prior.count += count_delta;
    return prior;
}

test "avg state round trips" {
    const alloc = std.testing.allocator;
    const encoded = try encodeAvgAlloc(alloc, .{ .sum = 12.5, .count = 3 });
    defer alloc.free(encoded);
    const decoded = try parseAvg(encoded);
    try std.testing.expectEqual(@as(i64, 3), decoded.count);
    try std.testing.expectEqual(@as(f64, 12.5), decoded.sum);
}
