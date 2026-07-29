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
