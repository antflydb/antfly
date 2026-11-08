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

//! Java-compatible full-string predicates over the portable capture engine.
//! Dialect constructs without equivalent semantics are rejected explicitly.
const std = @import("std");
const capture = @import("cypher_regex_capture");
const A = std.mem.Allocator;
pub const Work = capture.Budget;
const Flags = struct { insensitive: bool = false, multiline: bool = false, dotall: bool = false, unix_lines: bool = false };
const Pattern = struct { text: []const u8, flags: Flags };
const final_terminator = "(?=(?:\\r\\n|(?<!\\r)\\n|[\\r\\u0085\\u2028\\u2029])?\\Z)";

fn nativeError(err: anyerror) anyerror {
    return switch (err) {
        error.InvalidPattern => error.CypherInvalidRegex,
        error.RegexLimitExceeded => error.CypherRegexUnavailable,
        else => err,
    };
}
fn points(a: A, text: []const u8, work: *Work) ![]const u32 {
    var result: std.ArrayList(u32) = .empty;
    var iterator = (std.unicode.Utf8View.init(text) catch return error.CypherInvalidRegex).iterator();
    while (iterator.nextCodepoint()) |point| {
        try work.charge(1);
        try result.append(a, point);
    }
    return result.toOwnedSlice(a);
}
fn translate(a: A, source: []const u8, work: *Work) !Pattern {
    if (source.len > 16 * 1024) return error.CypherLimitExceeded;
    var flags: Flags = .{};
    var index: usize = 0;
    while (std.mem.startsWith(u8, source[index..], "(?")) {
        const end = std.mem.indexOfScalarPos(u8, source, index + 2, ')') orelse break;
        const flag_text = source[index + 2 .. end];
        if (flag_text.len == 0 or std.mem.indexOfAny(u8, flag_text, ":=!<()") != null) break;
        var enabled = true;
        for (flag_text) |flag| switch (flag) {
            '-' => enabled = false,
            'i' => flags.insensitive = enabled,
            'm' => flags.multiline = enabled,
            's' => flags.dotall = enabled,
            'd' => flags.unix_lines = enabled,
            else => return error.CypherRegexUnavailable,
        };
        index = end + 1;
    }
    var result: std.ArrayList(u8) = .empty;
    try result.appendSlice(a, "\\A(?:");
    var in_class = false;
    var class_first = false;
    var class_negated = false;
    var quoted = false;
    var groups: [128]bool = undefined;
    var depth: usize = 0;
    var look_depth: usize = 0;
    while (index < source.len) : (index += 1) {
        try work.charge(1);
        const ch = source[index];
        if (quoted) {
            if (ch == '\\' and index + 1 < source.len and source[index + 1] == 'E') {
                quoted = false;
                index += 1;
                continue;
            }
            if (std.mem.indexOfScalar(u8, "\\.^$|?*+()[]{}-", ch) != null) try result.append(a, '\\');
            try result.append(a, ch);
            if (in_class) class_first = false;
            continue;
        }
        if (ch == '\\') {
            index += 1;
            if (index == source.len) return error.CypherInvalidRegex;
            const escaped = source[index];
            if (escaped == 'Q') {
                quoted = true;
                continue;
            }
            if (in_class) class_first = false;
            if (escaped == 'z' or escaped == 'Z' or escaped == 'A') {
                if (in_class) return error.CypherInvalidRegex;
                try result.appendSlice(a, "(?:");
                try result.appendSlice(a, if (escaped == 'Z') final_terminator else if (escaped == 'z') "\\Z" else "\\A");
                try result.append(a, ')');
                continue;
            }
            if (escaped == 'b' or escaped == 'B') {
                if (in_class and escaped == 'B') return error.CypherInvalidRegex;
                try result.append(a, '\\');
                try result.append(a, if (in_class) 'b' else if (escaped == 'b') 'y' else 'Y');
                continue;
            }
            if (escaped == 'x') {
                // Java consumes exactly two hex digits; ARE greedily consumes
                // up to eight. Normalize the byte escape before delegation.
                if (source.len - index < 3 or source[index + 1] == '{') return error.CypherRegexUnavailable;
                for (source[index + 1 ..][0..2]) |digit| if (!std.ascii.isHex(digit)) return error.CypherInvalidRegex;
                try result.appendSlice(a, "\\u00");
                try result.appendSlice(a, source[index + 1 ..][0..2]);
                index += 2;
                continue;
            }
            if ((escaped >= '0' and escaped <= '9') and (escaped == '0' or (index + 1 < source.len and std.ascii.isDigit(source[index + 1])))) return error.CypherRegexUnavailable;
            if (in_class and std.ascii.isDigit(escaped)) return error.CypherInvalidRegex;
            if (escaped >= '1' and escaped <= '9' and look_depth != 0) return error.CypherRegexUnavailable;
            if (std.ascii.isAlphabetic(escaped) and std.mem.indexOfScalar(u8, "dDsSwWuaefnrt", escaped) == null) return error.CypherRegexUnavailable;
            try result.append(a, '\\');
            try result.append(a, escaped);
            continue;
        }
        if (ch == '[') {
            // Java intersections/nested classes differ from POSIX classes.
            if (in_class) return error.CypherRegexUnavailable;
            in_class = true;
            class_first = true;
            class_negated = false;
        } else if (ch == ']') {
            if (in_class and class_first) {
                try result.appendSlice(a, "\\]");
                class_first = false;
                continue;
            }
            in_class = false;
        } else if (in_class and ch == '&' and index + 1 < source.len and source[index + 1] == '&') return error.CypherRegexUnavailable;
        if (in_class and ch != '[') {
            if (class_first and ch == '^' and !class_negated) class_negated = true else class_first = false;
        }
        if (!in_class and ch == '{' and (index + 1 == source.len or !std.ascii.isDigit(source[index + 1]))) return error.CypherInvalidRegex;
        if (!in_class and ch == '{') {
            var position = index + 1;
            while (position < source.len and (std.ascii.isDigit(source[position]) or source[position] == ',')) {
                if (source[position] == ',') {
                    position += 1;
                    continue;
                }
                var count: u32 = 0;
                while (position < source.len and std.ascii.isDigit(source[position])) : (position += 1) {
                    count = count *| 10 +| source[position] - '0';
                    if (count > 255) return error.CypherLimitExceeded;
                }
            }
        }
        if (!in_class and ch == '(') {
            if (depth == groups.len) return error.CypherRegexUnavailable;
            var look = false;
            if (index + 2 < source.len and source[index + 1] == '?') {
                const kind = source[index + 2];
                if (kind != ':' and kind != '=' and kind != '!' and !(kind == '<' and index + 3 < source.len and (source[index + 3] == '=' or source[index + 3] == '!'))) return error.CypherRegexUnavailable;
                look = kind != ':';
            } else if (look_depth != 0) return error.CypherRegexUnavailable;
            groups[depth] = look;
            depth += 1;
            if (look) look_depth += 1;
        } else if (!in_class and ch == ')') {
            if (depth == 0) return error.CypherInvalidRegex;
            depth -= 1;
            if (groups[depth]) look_depth -= 1;
        }
        if (!in_class and ch == '.') {
            try result.appendSlice(a, if (flags.dotall) "." else if (flags.unix_lines) "[^\\n]" else "[^\\n\\r\\u0085\\u2028\\u2029]");
        } else if (!in_class and ch == '^') {
            try result.appendSlice(a, if (!flags.multiline) "(?:\\A)" else if (flags.unix_lines) "(?:\\A|(?<=\\n))" else "(?:\\A|(?<=\\n)|(?<=\\r)(?!\\n)|(?<=\\u0085)|(?<=\\u2028)|(?<=\\u2029))");
        } else if (!in_class and ch == '$') {
            try result.appendSlice(a, "(?:");
            try result.appendSlice(a, if (!flags.multiline) (if (flags.unix_lines) "(?=\\n?\\Z)" else final_terminator) else if (flags.unix_lines) "(?:\\Z|(?=\\n))" else "(?:\\Z|(?=\\r)|(?<!\\r)(?=\\n)|(?=[\\u0085\\u2028\\u2029]))");
            try result.append(a, ')');
        } else try result.append(a, ch);
    }
    if (in_class or depth != 0) return error.CypherInvalidRegex;
    try result.appendSlice(a, ")\\Z");
    return .{ .text = result.items, .flags = flags };
}
fn compile(a: A, scratch: A, source: []const u8, work: *Work) !capture.Program {
    const pattern = try translate(scratch, source, work);
    return capture.Program.compile(a, try points(scratch, pattern.text, work), .{ .case_insensitive = pattern.flags.insensitive }, .{ .pattern_bytes = 1 << 20 }, work) catch |err| return nativeError(err);
}
pub fn validate(a: A, pattern: []const u8) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var work: Work = .{ .limit_error = error.CypherStateBudgetExceeded };
    var program = try compile(arena.allocator(), arena.allocator(), pattern, &work);
    defer program.deinit(arena.allocator());
}
pub const Runtime = struct {
    a: A,
    scratch: std.heap.ArenaAllocator,
    cache: std.StringHashMapUnmanaged(*capture.Program) = .empty,
    pub fn deinit(self: *Runtime) void {
        self.scratch.deinit();
        var entries = self.cache.iterator();
        while (entries.next()) |entry| {
            entry.value_ptr.*.deinit(self.a);
            self.a.destroy(entry.value_ptr.*);
            self.a.free(entry.key_ptr.*);
        }
        self.cache.deinit(self.a);
    }
    pub fn matches(self: *Runtime, text: []const u8, pattern: []const u8, work: *Work) !bool {
        _ = self.scratch.reset(.retain_capacity);
        const temporary = self.scratch.allocator();
        const program = self.cache.get(pattern) orelse blk: {
            var compiled = try compile(self.a, temporary, pattern, work);
            errdefer compiled.deinit(self.a);
            const entry = try self.a.create(capture.Program);
            errdefer self.a.destroy(entry);
            entry.* = compiled;
            const key = try self.a.dupe(u8, pattern);
            errdefer self.a.free(key);
            try self.cache.put(self.a, key, entry);
            break :blk entry;
        };
        const input = try points(temporary, text, work);
        const spans = try temporary.alloc(capture.Span, program.captures + 1);
        return program.find(temporary, input, 0, spans, work) catch |err| return nativeError(err);
    }
};

test "Cypher regular expressions match whole Unicode strings with Java flags and line terminators" {
    var runtime: Runtime = .{ .a = std.testing.allocator, .scratch = std.heap.ArenaAllocator.init(std.testing.allocator) };
    defer runtime.deinit();
    const Case = struct { text: []const u8, pattern: []const u8, matches: bool };
    const cases = [_]Case{
        .{ .text = "abc", .pattern = "a.*", .matches = true },
        .{ .text = "abc", .pattern = "b", .matches = false },
        .{ .text = "AbC", .pattern = "(?i)abc", .matches = true },
        .{ .text = "β", .pattern = ".", .matches = true },
        .{ .text = "Β", .pattern = "(?i)β", .matches = false },
        .{ .text = "ab", .pattern = "(?i-m)AB", .matches = true },
        .{ .text = "123", .pattern = "\\d+", .matches = true },
        .{ .text = "١٢٣", .pattern = "\\d+", .matches = false },
        .{ .text = "a_1", .pattern = "\\w+", .matches = true },
        .{ .text = "abc", .pattern = "\\babc\\b", .matches = true },
        .{ .text = "abc", .pattern = "\\Babc", .matches = false },
        .{ .text = "abab", .pattern = "(ab)\\1", .matches = true },
        .{ .text = "a.c[]", .pattern = "\\Qa.c[]\\E", .matches = true },
        .{ .text = "AB", .pattern = "\\x41B", .matches = true },
        .{ .text = "a\nb", .pattern = "(?m)^a$\\nb", .matches = true },
        .{ .text = "a\r\nb", .pattern = "(?m)^a$\\r\\nb", .matches = true },
        .{ .text = "a\r\nb", .pattern = "(?m)a\\r$\\nb", .matches = false },
        .{ .text = "a\nb", .pattern = "(?s)a.b", .matches = true },
        .{ .text = "a\n", .pattern = "a$", .matches = false },
        .{ .text = "a\n", .pattern = "a\\Z\\n", .matches = true },
        .{ .text = "a\n", .pattern = "a\\z\\n", .matches = false },
        .{ .text = "]", .pattern = "[]a.]", .matches = true },
        .{ .text = ".", .pattern = "[]a.]", .matches = true },
        .{ .text = "\n", .pattern = "[]a.]", .matches = false },
        .{ .text = "", .pattern = "^*", .matches = true },
    };
    for (cases) |case| {
        var work: Work = .{};
        try std.testing.expectEqual(case.matches, try runtime.matches(case.text, case.pattern, &work));
    }
    for ([_][]const u8{ "\n", "\r", "\u{85}", "\u{2028}", "\u{2029}" }) |terminator| {
        var work: Work = .{};
        try std.testing.expect(!try runtime.matches(terminator, ".", &work));
        try std.testing.expect(try runtime.matches(terminator, "(?s).", &work));
    }
}

test "Cypher regular expression failures preserve work quotas and allocator cleanup" {
    const Fixture = struct {
        fn run(a: A) !void {
            var runtime: Runtime = .{ .a = a, .scratch = std.heap.ArenaAllocator.init(a) };
            defer runtime.deinit();
            var work: Work = .{ .limit_error = error.CypherStateBudgetExceeded };
            try std.testing.expect(try runtime.matches("abab", "(ab)\\1", &work));
            try std.testing.expect(try runtime.matches("abab", "(ab)\\1", &work));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
    var runtime: Runtime = .{ .a = std.testing.allocator, .scratch = std.heap.ArenaAllocator.init(std.testing.allocator) };
    defer runtime.deinit();
    var denied: Work = .{ .remaining = 1, .limit_error = error.CypherStateBudgetExceeded };
    try std.testing.expectError(error.CypherStateBudgetExceeded, runtime.matches("abc", "a.*", &denied));
    try std.testing.expectError(error.CypherInvalidRegex, validate(std.testing.allocator, "("));
    try std.testing.expectError(error.CypherInvalidRegex, validate(std.testing.allocator, "a{foo}"));
    try std.testing.expectError(error.CypherLimitExceeded, validate(std.testing.allocator, "a{256}"));
    try std.testing.expectError(error.CypherRegexUnavailable, validate(std.testing.allocator, "(?=(a))(b)\\1"));
    try std.testing.expectError(error.CypherRegexUnavailable, validate(std.testing.allocator, "[a-z&&[^x]]"));
}
