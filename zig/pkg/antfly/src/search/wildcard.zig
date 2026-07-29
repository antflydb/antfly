// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");

const Token = union(enum) {
    literal: u8,
    any,
    star,
};

const ParsedToken = struct {
    token: Token,
    next: usize,
};

pub const SearchPlan = struct {
    literal_prefix: []const u8,
    exact: bool,
    owned_prefix: []u8 = &.{},

    pub fn deinit(self: *SearchPlan, alloc: std.mem.Allocator) void {
        if (self.owned_prefix.len > 0) alloc.free(self.owned_prefix);
        self.* = undefined;
    }
};

fn tokenAt(pattern: []const u8, index: usize) ?ParsedToken {
    if (index >= pattern.len) return null;
    if (pattern[index] == '\\') {
        if (index + 1 < pattern.len) {
            return .{
                .token = .{ .literal = pattern[index + 1] },
                .next = index + 2,
            };
        }
        return .{
            .token = .{ .literal = '\\' },
            .next = index + 1,
        };
    }
    return .{
        .token = switch (pattern[index]) {
            '*' => .star,
            '?' => .any,
            else => |byte| .{ .literal = byte },
        },
        .next = index + 1,
    };
}

/// Parse the portion of a wildcard pattern that can constrain a dictionary
/// scan. Plain patterns borrow their prefix; quoted prefixes are decoded once.
/// `exact` means the decoded prefix is the complete literal lookup key.
pub fn searchPlanAlloc(alloc: std.mem.Allocator, pattern: []const u8) !SearchPlan {
    var raw_prefix_end = pattern.len;
    var exact = true;
    var requires_decode = false;
    var index: usize = 0;
    while (index < pattern.len) {
        if (pattern[index] == '\\') {
            requires_decode = true;
            index += if (index + 1 < pattern.len) 2 else 1;
            continue;
        }
        if (pattern[index] == '*' or pattern[index] == '?') {
            raw_prefix_end = index;
            exact = false;
            break;
        }
        index += 1;
    }

    const raw_prefix = pattern[0..raw_prefix_end];
    if (!requires_decode) {
        return .{
            .literal_prefix = raw_prefix,
            .exact = exact,
        };
    }

    const storage = try alloc.alloc(u8, raw_prefix.len);
    errdefer alloc.free(storage);
    var output_index: usize = 0;
    index = 0;
    while (tokenAt(raw_prefix, index)) |parsed| {
        const literal = switch (parsed.token) {
            .literal => |byte| byte,
            // raw_prefix ends before the first unquoted operator.
            .any, .star => unreachable,
        };
        storage[output_index] = literal;
        output_index += 1;
        index = parsed.next;
    }
    return .{
        .literal_prefix = storage[0..output_index],
        .exact = exact,
        .owned_prefix = storage,
    };
}

/// Glob-style byte matcher. `*` and `?` are operators; `\` quotes the next
/// byte. Matching is allocation-free and uses the standard last-star
/// backtracking algorithm.
pub fn match(pattern: []const u8, text: []const u8) bool {
    var pattern_index: usize = 0;
    var text_index: usize = 0;
    var star_resume_pattern: ?usize = null;
    var star_resume_text: usize = 0;

    while (text_index < text.len) {
        if (tokenAt(pattern, pattern_index)) |parsed| {
            switch (parsed.token) {
                .literal => |byte| {
                    if (byte == text[text_index]) {
                        pattern_index = parsed.next;
                        text_index += 1;
                        continue;
                    }
                },
                .any => {
                    pattern_index = parsed.next;
                    text_index += 1;
                    continue;
                },
                .star => {
                    star_resume_pattern = parsed.next;
                    star_resume_text = text_index;
                    pattern_index = parsed.next;
                    continue;
                },
            }
        }

        const resume_pattern = star_resume_pattern orelse return false;
        star_resume_text += 1;
        text_index = star_resume_text;
        pattern_index = resume_pattern;
    }

    while (tokenAt(pattern, pattern_index)) |parsed| {
        if (parsed.token != .star) return false;
        pattern_index = parsed.next;
    }
    return true;
}

/// Escape user text so it is interpreted literally by `match`.
pub fn escapeLiteralAlloc(alloc: std.mem.Allocator, value: []const u8) ![]u8 {
    var escaped_len = value.len;
    for (value) |byte| {
        if (byte == '\\' or byte == '*' or byte == '?') escaped_len += 1;
    }
    const escaped = try alloc.alloc(u8, escaped_len);
    var output_index: usize = 0;
    for (value) |byte| {
        if (byte == '\\' or byte == '*' or byte == '?') {
            escaped[output_index] = '\\';
            output_index += 1;
        }
        escaped[output_index] = byte;
        output_index += 1;
    }
    return escaped;
}

test "wildcard matching distinguishes operators from escaped literals" {
    try std.testing.expect(match("foo*", "foobar"));
    try std.testing.expect(match("f?o", "foo"));
    try std.testing.expect(match("a\\*b", "a*b"));
    try std.testing.expect(!match("a\\*b", "axxb"));
    try std.testing.expect(match("\\?", "?"));
    try std.testing.expect(match("\\\\", "\\"));
    try std.testing.expect(match("\\", "\\"));
}

test "wildcard literal escaping round trips metacharacters" {
    const alloc = std.testing.allocator;
    const escaped = try escapeLiteralAlloc(alloc, "a*?\\b");
    defer alloc.free(escaped);
    try std.testing.expectEqualStrings("a\\*\\?\\\\b", escaped);
    try std.testing.expect(match(escaped, "a*?\\b"));
    try std.testing.expect(!match(escaped, "axxb"));
}

test "wildcard search plans preserve escaped exact literals and prefixes" {
    const alloc = std.testing.allocator;

    var plain = try searchPlanAlloc(alloc, "plain");
    defer plain.deinit(alloc);
    try std.testing.expect(plain.exact);
    try std.testing.expectEqualStrings("plain", plain.literal_prefix);
    try std.testing.expectEqual(@as(usize, 0), plain.owned_prefix.len);

    var escaped_exact = try searchPlanAlloc(alloc, "foo\\*bar\\?");
    defer escaped_exact.deinit(alloc);
    try std.testing.expect(escaped_exact.exact);
    try std.testing.expectEqualStrings("foo*bar?", escaped_exact.literal_prefix);

    var escaped_prefix = try searchPlanAlloc(alloc, "foo\\*bar*tail");
    defer escaped_prefix.deinit(alloc);
    try std.testing.expect(!escaped_prefix.exact);
    try std.testing.expectEqualStrings("foo*bar", escaped_prefix.literal_prefix);

    var trailing_escape = try searchPlanAlloc(alloc, "foo\\");
    defer trailing_escape.deinit(alloc);
    try std.testing.expect(trailing_escape.exact);
    try std.testing.expectEqualStrings("foo\\", trailing_escape.literal_prefix);
}
