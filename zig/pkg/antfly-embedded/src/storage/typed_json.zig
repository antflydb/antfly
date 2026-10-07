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

//! Arena-owned typed JSON copies. Preserve exact numeric tokens and native
//! integer values without a serialize/parse round trip at page boundaries.
const std = @import("std");
const Json = std.json.Value;

pub fn clone(alloc: std.mem.Allocator, value: Json) error{ OutOfMemory, SqlProgramLimitExceeded }!Json {
    return cloneDepth(alloc, value, 0);
}

fn cloneDepth(alloc: std.mem.Allocator, value: Json, depth: usize) error{ OutOfMemory, SqlProgramLimitExceeded }!Json {
    if (depth > 64) return error.SqlProgramLimitExceeded;
    return switch (value) {
        .string => |text| .{ .string = try alloc.dupe(u8, text) },
        .number_string => |text| .{ .number_string = try alloc.dupe(u8, text) },
        .array => |items| blk: {
            var out = std.array_list.Managed(Json).init(alloc);
            try out.ensureTotalCapacity(items.items.len);
            for (items.items) |item| out.appendAssumeCapacity(try cloneDepth(alloc, item, depth + 1));
            break :blk .{ .array = out };
        },
        .object => |object| blk: {
            var out: std.json.ObjectMap = .empty;
            try out.ensureTotalCapacity(alloc, object.count());
            for (object.keys(), object.values()) |key, item| try out.put(alloc, try alloc.dupe(u8, key), try cloneDepth(alloc, item, depth + 1));
            break :blk .{ .object = out };
        },
        else => value,
    };
}

test "typed JSON clone preserves exact numeric representation" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const integer = try clone(arena.allocator(), .{ .integer = 9007199254740993 });
    try std.testing.expectEqual(@as(i64, 9007199254740993), integer.integer);
    const token = try clone(arena.allocator(), .{ .number_string = "18446744073709551615" });
    try std.testing.expectEqualStrings("18446744073709551615", token.number_string);
}
