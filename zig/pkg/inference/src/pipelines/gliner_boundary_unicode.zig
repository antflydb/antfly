// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Python 3.12/Unicode15 literal re.IGNORECASE matching for record enums.
//! This is deliberately different from string.casefold: a literal "ss" does
//! not match "ß", while dotted/dotless I share one regex equivalence class.
const std = @import("std");
const data = @import("gliner_boundary_case_data.zig");
const unicode = @import("../finetune/gliner2_unicode_tables.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
pub const Mention = struct { choice: usize, start: usize, end: usize };
pub const Options = struct { max_steps: usize = 10000000, max_mentions: usize = 65536, control: ?Control = null };

fn representative(cp: u21) u21 {
    var low: usize = 0;
    var high = data.pairs.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        if (data.pairs[mid].cp < cp) low = mid + 1 else high = mid;
    }
    return if (low < data.pairs.len and data.pairs[low].cp == cp) data.pairs[low].representative else cp;
}
pub fn regexCaseEquivalent(left: u21, right: u21) bool {
    return representative(left) == representative(right);
}

/// Match each literal independently, without overlap within that literal,
/// then order by source position and declared choice order, as re.finditer.
pub fn findLiteralMentions(allocator: std.mem.Allocator, text: []const u8, choices: []const []const u8, options: Options) ![]Mention {
    const view = try std.unicode.Utf8View.init(text);
    var mentions = std.ArrayListUnmanaged(Mention).empty;
    errdefer mentions.deinit(allocator);
    var steps: usize = 0;
    for (choices, 0..) |choice, index| {
        if (choice.len == 0) return error.InvalidEnumChoice;
        const pattern = try std.unicode.Utf8View.init(choice);
        var outer = view.iterator();
        var previous: ?u21 = null;
        while (outer.i < text.len) {
            if (steps >= options.max_steps) return error.EnumMatchingLimitExceeded;
            steps += 1;
            if (steps % 128 == 1) if (options.control) |control| try control.check();
            const start = outer.i;
            const first = outer.nextCodepoint().?;
            const blocked = previous != null and unicode.isWord(previous.?);
            previous = first;
            if (blocked) continue;
            var candidate = view.iterator();
            candidate.i = start;
            var expected = pattern.iterator();
            var matched = true;
            var last = first;
            while (expected.nextCodepoint()) |wanted| {
                if (steps >= options.max_steps) return error.EnumMatchingLimitExceeded;
                steps += 1;
                const actual = candidate.nextCodepoint() orelse {
                    matched = false;
                    break;
                };
                if (!regexCaseEquivalent(wanted, actual)) {
                    matched = false;
                    break;
                }
                last = actual;
            }
            if (!matched) continue;
            var after = candidate;
            if (after.nextCodepoint()) |next| if (unicode.isWord(next)) continue;
            if (mentions.items.len == options.max_mentions) return error.EnumMatchingLimitExceeded;
            try mentions.append(allocator, .{ .choice = index, .start = start, .end = candidate.i });
            outer = candidate;
            previous = last;
        }
    }
    const Order = struct {
        fn less(_: void, a: Mention, b: Mention) bool {
            return if (a.start != b.start) a.start < b.start else a.choice < b.choice;
        }
    };
    std.mem.sort(Mention, mentions.items, {}, Order.less);
    return mentions.toOwnedSlice(allocator);
}

test "record enum literals match pinned Python Unicode regex fixture" {
    const Fixture = struct { format_version: u32, provenance: std.json.Value, pairs: usize, cases: []const struct { text: []const u8, choices: []const []const u8, matches: []const Mention } };
    const bytes = try @import("../architectures/gliner_boundary_parity_test.zig").fixtureBytes(std.testing.allocator, "case_equivalence.json");
    defer std.testing.allocator.free(bytes);
    const parsed = try std.json.parseFromSlice(Fixture, std.testing.allocator, bytes, .{});
    defer parsed.deinit();
    for (parsed.value.cases) |case| {
        const got = try findLiteralMentions(std.testing.allocator, case.text, case.choices, .{});
        defer std.testing.allocator.free(got);
        try std.testing.expectEqualDeep(case.matches, got);
    }
    try std.testing.expectError(error.EnumMatchingLimitExceeded, findLiteralMentions(std.testing.allocator, "text", &.{"text"}, .{ .max_steps = 0 }));
}
