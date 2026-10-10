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
    return encodeJsonWithModifier(&ctx, value, null);
}

/// Shared exact JSON contract for schema annotations and durable casts. Both
/// members are required; unknown fields and lossy integer coercions are refused.
pub fn modifierFromJson(input: std.json.Value) !numeric.TypeModifier {
    if (input != .object or input.object.count() != 2) return error.SqlInvalidParameterValue;
    const Read = struct {
        fn integer(value: std.json.Value) !i64 {
            return switch (value) {
                .integer => value.integer,
                .number_string => |text| std.fmt.parseInt(i64, text, 10) catch error.SqlInvalidParameterValue,
                else => error.SqlInvalidParameterValue,
            };
        }
    };
    const precision = input.object.get("precision") orelse return error.SqlInvalidParameterValue;
    const scale = input.object.get("scale") orelse return error.SqlInvalidParameterValue;
    const modifier: numeric.TypeModifier = .{
        .precision = std.math.cast(u16, try Read.integer(precision)) orelse return error.SqlInvalidParameterValue,
        .scale = std.math.cast(i16, try Read.integer(scale)) orelse return error.SqlInvalidParameterValue,
    };
    try modifier.validate();
    return modifier;
}

/// One caller-owned budget covers parsing, assignment rounding and encoding.
/// Restore must verify the canonical value instead of calling this coercer.
pub fn encodeJsonWithModifier(ctx: *numeric.Context, value: std.json.Value, modifier: ?numeric.TypeModifier) ![]u8 {
    try ctx.charge(1);
    if (modifier) |constraint| try constraint.validate();
    var parsed = try fromJson(ctx, value);
    defer parsed.deinit();
    if (modifier) |constraint| {
        var constrained = try numeric.applyTypeModifier(ctx, parsed.value, constraint);
        defer constrained.deinit();
        return binary.encodeAlloc(ctx, constrained.value);
    }
    return binary.encodeAlloc(ctx, parsed.value);
}

/// Return a borrowed canonical view only after full schema-bound validation.
/// This performs no allocation and never repairs an invalid stored value.
pub fn verifyModifier(ctx: *numeric.Context, bytes: []const u8, modifier: numeric.TypeModifier) !binary.layout.View {
    const view = try binary.layout.View.openWithBudget(bytes, .{ .bytes = ctx.max_input_bytes, .groups = ctx.max_groups }, ctx);
    try view.verifyModifier(modifier, ctx);
    return view;
}

pub fn jsonValueAlloc(alloc: std.mem.Allocator, bytes: []const u8) !std.json.Value {
    var ctx: numeric.Context = .{ .alloc = alloc };
    var parsed = try binary.decodeCanonical(&ctx, bytes);
    defer parsed.deinit();
    const text = try numeric.format(&ctx, parsed.value);
    return if (parsed.value.kind == .finite) .{ .number_string = text } else .{ .string = text };
}

test "SQL NUMERIC modifier JSON requires exact bounded integer fields" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "{\"precision\":2,\"scale\":-3}", "{\"precision\":2,\"scale\":4}", "{\"precision\":1000,\"scale\":1000}" }) |input| {
        for ([_]bool{ true, false }) |parse_numbers| {
            var parsed = try std.json.parseFromSlice(std.json.Value, a, input, .{ .parse_numbers = parse_numbers });
            defer parsed.deinit();
            const modifier = try modifierFromJson(parsed.value);
            try modifier.validate();
        }
    }
    for ([_][]const u8{ "null", "[]", "{}", "{\"precision\":2}", "{\"precision\":0,\"scale\":0}", "{\"precision\":1001,\"scale\":0}", "{\"precision\":2,\"scale\":-1001}", "{\"precision\":2,\"scale\":1001}", "{\"precision\":2.5,\"scale\":1}", "{\"precision\":2,\"scale\":1.0}", "{\"precision\":\"2\",\"scale\":1}", "{\"precision\":2,\"scale\":null}", "{\"precision\":2,\"scale\":1,\"extra\":true}" }) |input| {
        for ([_]bool{ true, false }) |parse_numbers| {
            var parsed = try std.json.parseFromSlice(std.json.Value, a, input, .{ .parse_numbers = parse_numbers });
            defer parsed.deinit();
            try std.testing.expectError(error.SqlInvalidParameterValue, modifierFromJson(parsed.value));
        }
    }
}

test "SQL NUMERIC modifier storage separates write coercion from strict restore with PostgreSQL oracle" {
    const a = std.testing.allocator;
    const Entry = struct {
        op: []const u8,
        left: []const u8,
        precision: u16 = 0,
        scale: i16 = 0,
        expected: ?std.json.Value = null,
        @"error": ?[]const u8 = null,
    };
    const fixture = try std.json.parseFromSlice(struct { entries: []const Entry }, a, @embedFile("fixtures/sql_exact_numeric_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    var tested: usize = 0;
    for (fixture.value.entries) |entry| {
        if (!std.mem.eql(u8, entry.op, "typmod")) continue;
        tested += 1;
        const modifier: numeric.TypeModifier = .{ .precision = entry.precision, .scale = entry.scale };
        var ctx: numeric.Context = .{ .alloc = a };
        if (entry.@"error") |code| {
            try std.testing.expectError(if (std.mem.eql(u8, code, "22023"))
                error.SqlInvalidParameterValue
            else
                error.InvalidSqlNumber, encodeJsonWithModifier(&ctx, .{ .string = entry.left }, modifier));
            continue;
        }
        const encoded = try encodeJsonWithModifier(&ctx, .{ .string = entry.left }, modifier);
        defer a.free(encoded);
        const output = try jsonValueAlloc(a, encoded);
        defer a.free(if (output == .number_string) output.number_string else output.string);
        try std.testing.expectEqualStrings(entry.expected.?.string, if (output == .number_string) output.number_string else output.string);
        var none = std.heap.FixedBufferAllocator.init(&.{});
        var verification: numeric.Context = .{ .alloc = none.allocator() };
        _ = try verifyModifier(&verification, encoded, modifier);
    }
    try std.testing.expectEqual(@as(usize, 20), tested);
}

test "SQL NUMERIC modifier storage rejects canonical but unconstrained bytes without repair" {
    const a = std.testing.allocator;
    const cases = [_]struct { value: []const u8, precision: u16, scale: i16 }{
        .{ .value = "1.20", .precision = 4, .scale = 1 },
        .{ .value = "0", .precision = 4, .scale = 2 },
        .{ .value = "12001", .precision = 2, .scale = -3 },
        .{ .value = "100000", .precision = 2, .scale = -3 },
        .{ .value = ".0100", .precision = 2, .scale = 4 },
        .{ .value = "Infinity", .precision = 1000, .scale = 0 },
    };
    for (cases) |case| {
        const bytes = try encodeJsonAlloc(a, .{ .string = case.value });
        defer a.free(bytes);
        var none = std.heap.FixedBufferAllocator.init(&.{});
        var ctx: numeric.Context = .{ .alloc = none.allocator() };
        try std.testing.expectError(error.InvalidSqlBinaryRepresentation, verifyModifier(&ctx, bytes, .{ .precision = case.precision, .scale = case.scale }));
    }
}

test "SQL NUMERIC modifier storage shares sticky admission and unwinds every allocation fault" {
    const a = std.testing.allocator;
    const Run = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var ctx: numeric.Context = .{ .alloc = alloc };
            const bytes = try encodeJsonWithModifier(&ctx, .{ .string = "12.345" }, .{ .precision = 4, .scale = 2 });
            defer alloc.free(bytes);
            _ = try verifyModifier(&ctx, bytes, .{ .precision = 4, .scale = 2 });
            const output = try jsonValueAlloc(alloc, bytes);
            defer alloc.free(output.number_string);
            try std.testing.expectEqualStrings("12.35", output.number_string);
        }
        fn cancel(_: ?*anyopaque) anyerror!void {
            return error.Canceled;
        }
    };
    try std.testing.checkAllAllocationFailures(a, Run.run, .{});
    const bytes = try encodeJsonAlloc(a, .{ .string = "12.35" });
    defer a.free(bytes);
    var none = std.heap.FixedBufferAllocator.init(&.{});
    var verification: numeric.Context = .{ .alloc = none.allocator() };
    const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..10000) |_| _ = try verifyModifier(&verification, bytes, .{ .precision = 4, .scale = 2 });
    std.debug.print("NUMERIC strict modifier verification: rows=10000 allocated_bytes=0 elapsed_ns={}\n", .{std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start});
    verification = .{ .alloc = none.allocator(), .remaining = 0 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, verifyModifier(&verification, bytes, .{ .precision = 4, .scale = 2 }));
    try std.testing.expectError(error.SqlProgramLimitExceeded, encodeJsonWithModifier(&verification, .{ .string = "bad" }, .{ .precision = 0 }));
    verification = .{ .alloc = none.allocator(), .checkpoint = Run.cancel };
    try std.testing.expectError(error.Canceled, verifyModifier(&verification, bytes, .{ .precision = 4, .scale = 2 }));
    try std.testing.expectError(error.Canceled, encodeJsonWithModifier(&verification, .{ .string = "bad" }, .{ .precision = 0 }));
}
