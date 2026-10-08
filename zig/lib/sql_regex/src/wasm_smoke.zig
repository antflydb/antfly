// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const regex = @import("mod.zig");
pub const panic = std.debug.no_panic;
var buffer: [8 * 1024 * 1024]u8 = undefined;

const Golden = struct {
    format: u32,
    collation: []const u8,
    entries: []const struct {
        id: []const u8,
        pattern: []const u8,
        input: []const u8,
        flags: c_int,
        start: usize,
        captures: usize,
        matched: bool,
        spans: []const struct { start: c_long, end: c_long, text: ?[]const u8 },
    },
};
const GlobalGolden = struct {
    format: u32,
    collation: []const u8,
    entries: []const struct {
        id: []const u8,
        pattern: []const u8,
        input: []const u8,
        flags: c_int,
        start: usize,
        captures: usize,
        occurrences: []const []const struct { start: c_long, end: c_long, text: ?[]const u8 },
    },
};

export fn antfly_sql_regex_smoke() u32 {
    return run() catch 0;
}
fn run() !u32 {
    var memory = std.heap.FixedBufferAllocator.init(&buffer);
    const a = memory.allocator();
    const golden = try std.json.parseFromSlice(Golden, a, @embedFile("testdata/postgres.json"), .{});
    defer golden.deinit();
    if (golden.value.format != 1 or !std.mem.eql(u8, golden.value.collation, "C")) return error.InvalidReference;
    for (golden.value.entries) |case| {
        var budget: regex.Budget = .{};
        var program = try regex.Program.compile(a, case.pattern, case.flags, .{}, &budget);
        defer program.deinit();
        if (program.captures != case.captures) return error.WrongCaptures;
        var subject = try regex.Subject.init(a, case.input);
        defer subject.deinit();
        const spans = try a.alloc(regex.Span, case.spans.len);
        defer a.free(spans);
        if (case.matched != try program.find(subject, case.start, spans, &budget)) return error.WrongMatch;
        for (spans, case.spans) |actual, expected| {
            if (actual.start != expected.start or actual.end != expected.end) return error.WrongSpan;
            const text = try subject.slice(actual);
            if (expected.text) |value| {
                if (!std.mem.eql(u8, value, text orelse return error.MissingMatch)) return error.WrongText;
            } else if (text != null) return error.UnexpectedMatch;
        }
    }
    const global = try std.json.parseFromSlice(GlobalGolden, a, @embedFile("testdata/global-postgres.json"), .{});
    defer global.deinit();
    if (global.value.format != 1 or !std.mem.eql(u8, global.value.collation, "C")) return error.InvalidReference;
    var execution = regex.Executor.init(a, .{});
    defer execution.deinit();
    for (global.value.entries) |case| {
        var budget: regex.Budget = .{};
        var program = try regex.Program.compile(a, case.pattern, case.flags, .{}, &budget);
        defer program.deinit();
        if (program.captures != case.captures) return error.WrongCaptures;
        var subject = try regex.Subject.init(a, case.input);
        defer subject.deinit();
        const spans = try a.alloc(regex.Span, case.captures + 1);
        defer a.free(spans);
        var cursor = try regex.MatchCursor.init(&program, &execution, subject, case.start, spans);
        for (case.occurrences) |expected| {
            if (!try cursor.next(&budget)) return error.MissingMatch;
            for (spans, expected) |actual, capture| {
                if (actual.start != capture.start or actual.end != capture.end) return error.WrongSpan;
                const text = try subject.slice(actual);
                if (capture.text) |value| {
                    if (!std.mem.eql(u8, value, text orelse return error.MissingMatch)) return error.WrongText;
                } else if (text != null) return error.UnexpectedMatch;
            }
        }
        if (try cursor.next(&budget)) return error.UnexpectedMatch;
    }
    return @intCast(golden.value.entries.len + global.value.entries.len);
}
