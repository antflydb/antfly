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
    return encodeJsonWithModifier(&ctx, value, null);
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
