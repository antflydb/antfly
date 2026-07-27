// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");

pub const max_fuzzy_edits: u8 = 2;

pub fn requireSingleRoot(value: std.json.Value) !std.json.ObjectMap {
    if (value != .object or value.object.count() != 1) {
        return error.InvalidArgument;
    }
    return value.object;
}

pub fn validateBool(object: std.json.ObjectMap) !void {
    if (object.get("minimum_should_match") != null and
        object.get("min_should") != null)
    {
        return error.InvalidArgument;
    }

    var recognized: usize = 0;
    var branches: usize = 0;
    inline for ([_][]const u8{
        "filter",
        "must",
        "should",
        "must_not",
        "minimum_should_match",
        "min_should",
    }) |key| {
        if (object.get(key) != null) {
            recognized += 1;
            if (comptime std.mem.eql(u8, key, "filter") or
                std.mem.eql(u8, key, "must") or
                std.mem.eql(u8, key, "should") or
                std.mem.eql(u8, key, "must_not"))
            {
                branches += 1;
            }
        }
    }
    if (recognized != object.count() or branches == 0) {
        return error.InvalidArgument;
    }
}

pub fn minimumShould(
    object: std.json.ObjectMap,
    should_len: usize,
    has_required: bool,
) !usize {
    const default_value: usize = if (!has_required and should_len > 0) 1 else 0;
    const value = object.get("minimum_should_match") orelse
        object.get("min_should") orelse return default_value;
    const parsed = try jsonU32(value);
    if (parsed > should_len) return error.InvalidArgument;
    return parsed;
}

fn jsonU32(value: std.json.Value) !u32 {
    return switch (value) {
        .integer => |number| std.math.cast(u32, number) orelse error.InvalidArgument,
        .float => |number| blk: {
            if (!std.math.isFinite(number) or @round(number) != number or number < 0 or
                number > @as(f64, @floatFromInt(std.math.maxInt(u32))))
            {
                return error.InvalidArgument;
            }
            break :blk @intFromFloat(number);
        },
        else => error.InvalidArgument,
    };
}

test "pattern filter contract enforces roots bool keys and thresholds" {
    const alloc = std.testing.allocator;
    var single = try std.json.parseFromSlice(
        std.json.Value,
        alloc,
        \\{"term":{"status":"active"}}
    ,
        .{},
    );
    defer single.deinit();
    _ = try requireSingleRoot(single.value);

    var ambiguous = try std.json.parseFromSlice(
        std.json.Value,
        alloc,
        \\{"term":{"status":"active"},"match_all":{}}
    ,
        .{},
    );
    defer ambiguous.deinit();
    try std.testing.expectError(error.InvalidArgument, requireSingleRoot(ambiguous.value));

    var bool_value = try std.json.parseFromSlice(
        std.json.Value,
        alloc,
        \\{"should":[],"minimum_should_match":2}
    ,
        .{},
    );
    defer bool_value.deinit();
    try validateBool(bool_value.value.object);
    try std.testing.expectEqual(
        @as(usize, 2),
        try minimumShould(bool_value.value.object, 3, false),
    );
    try std.testing.expectError(
        error.InvalidArgument,
        minimumShould(bool_value.value.object, 1, false),
    );
}
