// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
//! PostgreSQL scalar regex contracts on the independently tested ARE backend.
//! Sessions belong to an execution owner, not an immutable prepared program.
const std = @import("std");
const native = @import("antfly_sql_regex");
const A = std.mem.Allocator;
const Json = std.json.Value;
pub const Function = enum { regexp_like, regexp_count, regexp_instr, regexp_substr, regexp_replace };
pub const Kind = enum { text, integer };
pub const Session = native.Session;
pub const Budget = native.Budget;

pub fn validArity(function: Function, count: usize) bool {
    return switch (function) {
        .regexp_like => count == 2 or count == 3,
        .regexp_count => count >= 2 and count <= 4,
        .regexp_instr => count >= 2 and count <= 7,
        .regexp_substr => count >= 2 and count <= 6,
        .regexp_replace => count >= 3 and count <= 6,
    };
}
pub fn argument(function: Function, count: usize, index: usize, replace_start: bool) Kind {
    if (index < 2) return .text;
    return switch (function) {
        .regexp_like => .text,
        .regexp_count => if (index == 2) .integer else .text,
        .regexp_instr => if (index == 5) .text else .integer,
        .regexp_substr => if (index == 4) .text else .integer,
        .regexp_replace => if (index == 2 or index == 5 or (count == 4 and !replace_start)) .text else .integer,
    };
}
fn integer(values: []const Json, index: usize, default: i32) !i32 {
    if (index >= values.len) return default;
    if (values[index] != .integer) return error.SqlTypeMismatch;
    return std.math.cast(i32, values[index].integer) orelse error.SqlNumericOutOfRange;
}
fn text(values: []const Json, index: usize) ![]const u8 {
    if (index >= values.len) return "";
    if (values[index] != .string) return error.SqlTypeMismatch;
    return values[index].string;
}

pub fn evaluate(alloc: A, session: *Session, function: Function, values: []const Json, maximum: usize, budget: *Budget) !Json {
    return evaluateNative(alloc, session, function, values, maximum, budget) catch |err| switch (err) {
        error.SqlExpressionTooLarge => error.SqlProgramLimitExceeded,
        error.SqlInvalidArgument => error.InvalidSqlParameters,
        error.SqlInvalidText => error.SqlInvalidTextEncoding,
        else => err,
    };
}
fn evaluateNative(alloc: A, session: *Session, function: Function, values: []const Json, maximum: usize, budget: *Budget) !Json {
    if (!validArity(function, values.len)) return error.SqlUndefinedFunction;
    for (values) |value| if (value == .null) return .null;
    const input = try text(values, 0);
    const pattern = try text(values, 1);
    const replace_start = function == .regexp_replace and (values.len > 4 or (values.len == 4 and values[3] == .integer));
    for (values, 0..) |value, i| switch (argument(function, values.len, i, replace_start)) {
        .text => {
            if (value != .string) return error.SqlTypeMismatch;
            if (std.mem.indexOfScalar(u8, value.string, 0) != null) return error.SqlInvalidTextEncoding;
        },
        .integer => if (value != .integer) return error.SqlTypeMismatch,
    };
    const start = try integer(values, switch (function) {
        .regexp_like => values.len,
        .regexp_replace => if (replace_start) 3 else values.len,
        else => 2,
    }, 1);
    const occurrence = try integer(values, switch (function) {
        .regexp_instr, .regexp_substr => 3,
        .regexp_replace => if (replace_start) 4 else values.len,
        else => values.len,
    }, 1);
    const end_option = if (function == .regexp_instr) try integer(values, 4, 0) else 0;
    const group = try integer(values, switch (function) {
        .regexp_instr => 6,
        .regexp_substr => 5,
        else => values.len,
    }, 0);
    if (start < 1 or occurrence < 0 or (occurrence == 0 and function != .regexp_replace) or group < 0 or end_option < 0 or end_option > 1) return error.InvalidSqlParameters;
    const options = try text(values, switch (function) {
        .regexp_like => 2,
        .regexp_count => 3,
        .regexp_instr => 5,
        .regexp_substr => 4,
        .regexp_replace => if (replace_start) 5 else 3,
    });
    try budget.charge(options.len + input.len);
    const flags = try native.Flags.parse(options, function == .regexp_replace);
    const program = try session.pattern(pattern, flags.native, budget);
    var subject = try native.Subject.init(alloc, input);
    defer subject.deinit();
    const position: usize = @intCast(start - 1);
    if (function == .regexp_replace) {
        var replacement = try session.replacement(alloc, try text(values, 2), budget);
        defer replacement.deinit();
        const selected: usize = if (values.len >= 5 or !flags.global) @intCast(occurrence) else 0;
        return .{ .string = try session.executor.replaceAlloc(alloc, program, subject, replacement.plan(), position, selected, maximum, budget) };
    }
    if (position > subject.len()) return switch (function) {
        .regexp_like => .{ .bool = false },
        .regexp_count, .regexp_instr => .{ .integer = 0 },
        else => .null,
    };
    if (group > program.captures) return if (function == .regexp_instr) .{ .integer = 0 } else .null;
    const spans = try alloc.alloc(native.Span, @as(usize, @intCast(group)) + 1);
    defer alloc.free(spans);
    var cursor = try native.MatchCursor.init(program, &session.executor, subject, position, spans);
    var count: i32 = 0;
    while (try cursor.next(budget)) {
        count = std.math.add(i32, count, 1) catch return error.SqlNumericOutOfRange;
        if (function == .regexp_count) continue;
        if (function == .regexp_like) return .{ .bool = true };
        if (count != occurrence) continue;
        const span = spans[@intCast(group)];
        if (function == .regexp_instr) return .{ .integer = if (span.start < 0) 0 else (if (end_option == 0) span.start else span.end) + 1 };
        const value = (try subject.slice(span)) orelse return .null;
        if (value.len > maximum) return error.SqlProgramLimitExceeded;
        try budget.charge(value.len);
        return .{ .string = try alloc.dupe(u8, value) };
    }
    return switch (function) {
        .regexp_like => .{ .bool = false },
        .regexp_count => .{ .integer = count },
        .regexp_instr => .{ .integer = 0 },
        else => .null,
    };
}
