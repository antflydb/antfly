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
        options: []const u8,
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
const ReplacementGolden = struct {
    format: u32,
    collation: []const u8,
    entries: []const struct { id: []const u8, pattern: []const u8, input: []const u8, replacement: []const u8, flags: c_int, start: usize, occurrence: usize, expected: []const u8 },
};

export fn antfly_sql_regex_smoke() u32 {
    return run() catch 0;
}
export fn antfly_sql_regex_replacement_smoke() u32 {
    return runReplacements() catch 0;
}
export fn antfly_sql_regex_capture_smoke() u32 {
    return runCaptures() catch 0;
}
fn runCaptures() !u32 {
    var memory = std.heap.FixedBufferAllocator.init(&buffer);
    const backing = memory.allocator();
    const golden = try std.json.parseFromSlice(Golden, backing, @embedFile("testdata/capture-postgres.json"), .{});
    defer golden.deinit();
    if (golden.value.format != 1 or !std.mem.eql(u8, golden.value.collation, "C")) return error.InvalidReference;
    for (golden.value.entries) |case| {
        // Reclaim the entire case even when the backing allocator is a fixed
        // buffer; individual tracked allocations need not be freed LIFO.
        var arena = std.heap.ArenaAllocator.init(backing);
        defer arena.deinit();
        const a = arena.allocator();
        var budget: regex.Budget = .{};
        const flags = try regex.Flags.parse(case.options, false);
        if (flags.native != case.flags) return error.WrongFlags;
        var program = try regex.Program.compile(a, case.pattern, flags.native, .{}, &budget);
        defer program.deinit();
        if (program.captures != case.captures) return error.WrongCaptures;
        var subject = try regex.Subject.init(a, case.input);
        defer subject.deinit();
        const spans = try a.alloc(regex.Span, case.spans.len);
        if (case.matched != try program.find(subject, case.start, spans, &budget)) return error.WrongMatch;
        for (spans, case.spans) |actual, expected| {
            if (actual.start != expected.start or actual.end != expected.end) return error.WrongSpan;
            const text = try subject.slice(actual);
            if (expected.text) |value| {
                if (!std.mem.eql(u8, value, text orelse return error.MissingMatch)) return error.WrongText;
            } else if (text != null) return error.UnexpectedMatch;
        }
    }
    return @intCast(golden.value.entries.len);
}
fn runReplacements() !u32 {
    var memory = std.heap.FixedBufferAllocator.init(&buffer);
    const a = memory.allocator();
    const golden = try std.json.parseFromSlice(ReplacementGolden, a, @embedFile("testdata/replacement-postgres.json"), .{});
    defer golden.deinit();
    if (golden.value.format != 1 or !std.mem.eql(u8, golden.value.collation, "C")) return error.InvalidReference;
    var session = regex.Session.init(a, .{}, 16 * 1024 * 1024);
    defer session.deinit();
    for (golden.value.entries) |case| {
        var budget: regex.Budget = .{};
        const program = try session.pattern(case.pattern, case.flags, &budget);
        var subject = try regex.Subject.init(a, case.input);
        defer subject.deinit();
        var replacement = try session.replacement(a, case.replacement, &budget);
        defer replacement.deinit();
        var repeat = try session.replacement(a, case.replacement, &budget);
        defer repeat.deinit();
        if (repeat.plan() != replacement.plan()) return error.WrongCache;
        const output = try session.executor.replaceAlloc(a, program, subject, replacement.plan(), case.start, case.occurrence, 1024, &budget);
        defer a.free(output);
        if (!std.mem.eql(u8, case.expected, output)) return error.WrongReplacement;
    }
    if (session.replacement_hits < golden.value.entries.len) return error.WrongCache;
    return @intCast(golden.value.entries.len);
}
fn run() !u32 {
    var memory = std.heap.FixedBufferAllocator.init(&buffer);
    const a = memory.allocator();
    const golden = try std.json.parseFromSlice(Golden, a, @embedFile("testdata/postgres.json"), .{});
    defer golden.deinit();
    if (golden.value.format != 1 or !std.mem.eql(u8, golden.value.collation, "C")) return error.InvalidReference;
    for (golden.value.entries) |case| {
        const flags = try regex.Flags.parse(case.options, false);
        if (flags.native != case.flags) return error.WrongFlags;
        var budget: regex.Budget = .{};
        var program = try regex.Program.compile(a, case.pattern, flags.native, .{}, &budget);
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
    var session = regex.Session.init(a, .{}, 16 * 1024 * 1024);
    defer session.deinit();
    for (global.value.entries) |case| {
        var budget: regex.Budget = .{};
        const program = try session.pattern(case.pattern, case.flags, &budget);
        if (program != try session.pattern(case.pattern, case.flags, &budget)) return error.WrongCache;
        if (program.captures != case.captures) return error.WrongCaptures;
        var subject = try regex.Subject.init(a, case.input);
        defer subject.deinit();
        const spans = try a.alloc(regex.Span, case.captures + 1);
        defer a.free(spans);
        var cursor = try regex.MatchCursor.init(program, &session.executor, subject, case.start, spans);
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
    if (session.hits < global.value.entries.len) return error.WrongCache;
    return @intCast(golden.value.entries.len + global.value.entries.len);
}
