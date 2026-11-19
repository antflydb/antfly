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

//! Typed numeric functions shared by query and durable-expression consumers.
//! Callers own overload resolution, strict argument evaluation and retained
//! result admission. Exact results may borrow input limbs: pin that owner until
//! encoding/adoption finishes. Work and cancellation use the invocation context.
const std = @import("std");
const exact = @import("numeric_value.zig");

pub const Function = enum { abs, ceil, floor, round, trunc, sign, sqrt };

pub fn numeric(work: *exact.Context, function: Function, input: exact.Value, scale: i32) !exact.Owned {
    try work.charge(1);
    if (function == .abs) {
        var value = input;
        value.negative = false;
        if (value.kind == .negative_infinity) value.kind = .positive_infinity;
        return .{ .alloc = work.alloc, .value = value };
    }
    if (function == .sign) return .{ .alloc = work.alloc, .value = if (input.kind == .nan)
        .{ .kind = .nan }
    else if (input.isZero())
        .{}
    else
        .{ .digits = &.{1}, .negative = input.negative or input.kind == .negative_infinity } };
    if (function == .sqrt) return exact.squareRoot(work, input);
    var result = try exact.quantize(work, input, if (function == .round or function == .trunc) scale else 0, if (function == .round) .half_away else .truncate);
    errdefer result.deinit();
    if (function == .ceil or function == .floor) {
        const order = try exact.order(work, input, result.value);
        if ((function == .ceil and order == .gt) or (function == .floor and order == .lt)) {
            const one: exact.Value = .{ .digits = &.{1} };
            const adjusted = if (function == .ceil) try exact.add(work, result.value, one) else try exact.subtract(work, result.value, one);
            result.deinit();
            return adjusted;
        }
    }
    return result;
}

pub fn floating(work: *exact.Context, function: Function, input: f64) !f64 {
    try work.charge(1);
    return switch (function) {
        .abs => @abs(input),
        .ceil => @ceil(input),
        .floor => @floor(input),
        .round => roundTiesEven(input),
        .trunc => @trunc(input),
        .sign => if (input < 0) -1 else if (input > 0) 1 else 0,
        .sqrt => if (input < 0) error.SqlInvalidPowerArgument else @sqrt(input),
    };
}

fn roundTiesEven(value: f64) f64 {
    const magnitude = @abs(value);
    const integral = @floor(magnitude);
    const fraction = magnitude - integral;
    const rounded = if (fraction > 0.5 or (fraction == 0.5 and @mod(integral, 2) != 0)) integral + 1 else integral;
    return std.math.copysign(rounded, value);
}

test "SQL shared numeric functions borrow unary results and retain sticky admission" {
    const a = std.testing.allocator;
    var work: exact.Context = .{ .alloc = a };
    var input = try exact.parse(&work, "-9007199254740993.125");
    defer input.deinit();
    var none = std.heap.FixedBufferAllocator.init(&.{});
    work.alloc = none.allocator();
    var absolute = try numeric(&work, .abs, input.value, 0);
    defer absolute.deinit();
    try std.testing.expectEqual(@intFromPtr(input.value.digits.ptr), @intFromPtr(absolute.value.digits.ptr));
    try std.testing.expect(!absolute.value.negative);
    var sign = try numeric(&work, .sign, input.value, 0);
    defer sign.deinit();
    try std.testing.expect(sign.value.negative);
    try std.testing.expectEqual(@as(usize, 0), none.end_index);
    work.remaining = 0;
    try std.testing.expectError(error.SqlProgramLimitExceeded, numeric(&work, .abs, input.value, 0));
    try std.testing.expectError(error.SqlProgramLimitExceeded, floating(&work, .round, 2.5));
}

test "SQL shared numeric functions unwind every unpublished allocation failure" {
    const Harness = struct {
        fn run(a: std.mem.Allocator, function: Function) !void {
            var work: exact.Context = .{ .alloc = a };
            var input = try exact.parse(&work, if (function == .floor) "-9007199254740993.255" else "9007199254740993.255");
            defer input.deinit();
            var output = try numeric(&work, function, input.value, 2);
            defer output.deinit();
            const formatted = try exact.format(&work, output.value);
            defer a.free(formatted);
        }
    };
    for (std.enums.values(Function)) |function| try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{function});
}

test "SQL shared numeric functions match independent PostgreSQL values and floating bits" {
    const a = std.testing.allocator;
    const Entry = struct { domain: enum { numeric, float64 }, function: Function, input: ?[]const u8, scale: ?i32, expected: ?[]const u8, sqlstate: ?[]const u8 };
    const fixture = try std.json.parseFromSlice(struct { entries: []const Entry }, a, @embedFile("fixtures/sql_numeric_functions_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    try std.testing.expectEqual(@as(usize, 210), fixture.value.entries.len);
    for (fixture.value.entries) |entry| {
        const input = entry.input orelse continue; // SQL NULL belongs to the caller.
        errdefer std.debug.print("shared numeric oracle domain={s} function={s} input={s}\n", .{ @tagName(entry.domain), @tagName(entry.function), input });
        var work: exact.Context = .{ .alloc = a };
        if (entry.domain == .numeric) {
            var parsed = try exact.parse(&work, input);
            defer parsed.deinit();
            var output = numeric(&work, entry.function, parsed.value, entry.scale orelse 0) catch |err| {
                try std.testing.expectEqualStrings(entry.sqlstate orelse return err, @import("errors.zig").describe(err).code);
                continue;
            };
            defer output.deinit();
            try std.testing.expect(entry.sqlstate == null);
            const text = try exact.format(&work, output.value);
            defer a.free(text);
            try std.testing.expectEqualStrings(entry.expected.?, text);
        } else {
            const value = try @import("builtin_cast.zig").floatValue(f64, .{ .string = input });
            const output = floating(&work, entry.function, value) catch |err| {
                try std.testing.expectEqualStrings(entry.sqlstate orelse return err, @import("errors.zig").describe(err).code);
                continue;
            };
            try std.testing.expect(entry.sqlstate == null);
            var hex: [16]u8 = undefined;
            try std.testing.expectEqualStrings(entry.expected.?, try std.fmt.bufPrint(&hex, "{x:0>16}", .{@as(u64, @bitCast(output))}));
        }
    }
}

test "SQL shared numeric functions unwind every cancellation cut on the shared owner" {
    const a = std.testing.allocator;
    var parsing: exact.Context = .{ .alloc = a };
    var input = try exact.parse(&parsing, "123456789.125");
    defer input.deinit();
    const Control = struct {
        polls: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.polls += 1;
            if (self.polls == self.fail_at) return error.QueryCanceled;
        }
    };
    for (std.enums.values(Function)) |function| {
        var observed: Control = .{};
        var work: exact.Context = .{ .alloc = a, .checkpoint = Control.check, .ptr = &observed };
        var result = try numeric(&work, function, input.value, 0);
        defer result.deinit();
        for (1..observed.polls + 1) |cut| {
            var control: Control = .{ .fail_at = cut };
            work = .{ .alloc = a, .checkpoint = Control.check, .ptr = &control };
            try std.testing.expectError(error.QueryCanceled, numeric(&work, function, input.value, 0));
            try std.testing.expectError(error.QueryCanceled, work.charge(0));
        }
    }
}
