// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Lossless JSON/physical-row boundary for exact SQL NUMERIC. API lexemes are
//! parsed once; physical rows own canonical binary, never formatted decimals.
const std = @import("std");
const numeric = @import("numeric_value.zig");
const binary = @import("numeric_binary.zig");

pub fn fromJson(ctx: *numeric.Context, value: std.json.Value) !numeric.Owned {
    return switch (value) {
        .number_string, .string => |text| numeric.parse(ctx, text),
        .integer => |integer| blk: {
            var text: [20]u8 = undefined;
            break :blk numeric.parse(ctx, try std.fmt.bufPrint(&text, "{d}", .{integer}));
        },
        // An already-rounded f64 is not an exact JSON input lexeme. SQL
        // explicit float casts perform their declared conversion upstream.
        else => error.InvalidBatchRequest,
    };
}

pub fn encodeJsonAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    var ctx: numeric.Context = .{ .alloc = alloc };
    var parsed = try fromJson(&ctx, value);
    defer parsed.deinit();
    return binary.encodeAlloc(&ctx, parsed.value);
}

pub fn jsonValueAlloc(alloc: std.mem.Allocator, bytes: []const u8) !std.json.Value {
    var ctx: numeric.Context = .{ .alloc = alloc };
    var parsed = try binary.decodeCanonical(&ctx, bytes);
    defer parsed.deinit();
    const text = try numeric.format(&ctx, parsed.value);
    return if (parsed.value.kind == .finite) .{ .number_string = text } else .{ .string = text };
}
