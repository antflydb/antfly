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

//! Bounded normalization preserves a byte witness for every normalized byte.
//! Output offsets always refer to the immutable original caller text.
const std = @import("std");
const text = @import("antfly_extracting").text_processing;
const literal = text.alignment;
const A = std.mem.Allocator;

const Normalized = struct {
    bytes: []u8,
    starts: []u32,
    ends: []u32,
    fn deinit(self: Normalized, a: A) void {
        a.free(self.bytes);
        a.free(self.starts);
        a.free(self.ends);
    }
};
fn normalize(a: A, source: []const u8, control: ?text.Checkpoint) !Normalized {
    if (source.len > 4 * 1024 * 1024) return error.GeneratedExtractionLimitExceeded;
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(a);
    var starts: std.ArrayList(u32) = .empty;
    defer starts.deinit(a);
    var ends: std.ArrayList(u32) = .empty;
    defer ends.deinit(a);
    var it = (try std.unicode.Utf8View.init(source)).iterator();
    var checkpoints: usize = 0;
    var space = false;
    while (it.nextCodepointSlice()) |slice| {
        if (checkpoints % 1024 == 0) if (control) |value| try value.check();
        checkpoints += 1;
        const start = it.i - slice.len;
        const cp = try std.unicode.utf8Decode(slice);
        if (text.unicode_tables.isWhitespace(cp)) {
            if (space) {
                ends.items[ends.items.len - 1] = @intCast(it.i);
                continue;
            }
            try bytes.append(a, ' ');
            try starts.append(a, @intCast(start));
            try ends.append(a, @intCast(it.i));
            space = true;
        } else {
            for (slice, 0..) |byte, offset| {
                try bytes.append(a, byte);
                try starts.append(a, @intCast(start + offset));
                try ends.append(a, @intCast(start + offset + 1));
            }
            space = false;
        }
    }
    const owned = try bytes.toOwnedSlice(a);
    errdefer a.free(owned);
    const owned_starts = try starts.toOwnedSlice(a);
    errdefer a.free(owned_starts);
    return .{ .bytes = owned, .starts = owned_starts, .ends = try ends.toOwnedSlice(a) };
}
fn needsNormalization(source: []const u8) !bool {
    var it = (try std.unicode.Utf8View.init(source)).iterator();
    var space = false;
    while (it.nextCodepoint()) |cp| {
        if (text.unicode_tables.isWhitespace(cp)) {
            if (cp != ' ' or space) return true;
            space = true;
        } else space = false;
    }
    return false;
}

pub fn find(a: A, source: []const u8, forms: []const []const u8, options: literal.Options) ![]literal.Mention {
    if (forms.len == 0) return a.alloc(literal.Mention, 0);
    var needed = try needsNormalization(source);
    for (forms) |form| needed = needed or try needsNormalization(form);
    const exact = try literal.findLiteralMentions(a, source, forms, options);
    if (!needed) return exact;
    defer a.free(exact);
    const normalized = try normalize(a, source, options.control);
    defer normalized.deinit(a);
    const normalized_forms = try a.alloc(Normalized, forms.len);
    defer a.free(normalized_forms);
    var initialized: usize = 0;
    defer for (normalized_forms[0..initialized]) |form| form.deinit(a);
    const surfaces = try a.alloc([]const u8, forms.len);
    defer a.free(surfaces);
    for (forms, normalized_forms, surfaces) |form, *mapped, *surface| {
        mapped.* = try normalize(a, form, options.control);
        initialized += 1;
        surface.* = mapped.bytes;
    }
    const matches = try literal.findLiteralMentions(a, normalized.bytes, surfaces, options);
    defer a.free(matches);
    const Key = struct { choice: usize, start: usize, end: usize };
    var seen: std.AutoHashMapUnmanaged(Key, void) = .empty;
    defer seen.deinit(a);
    var result: std.ArrayList(literal.Mention) = .empty;
    errdefer result.deinit(a);
    for (exact) |mention| {
        try seen.put(a, .{ .choice = mention.choice, .start = mention.start, .end = mention.end }, {});
        try result.append(a, mention);
    }
    for (matches) |mention| {
        if (mention.end <= mention.start or mention.end > normalized.bytes.len) return error.InvalidConstrainedOutput;
        const start = normalized.starts[mention.start];
        const end = normalized.ends[mention.end - 1];
        const entry = try seen.getOrPut(a, .{ .choice = mention.choice, .start = start, .end = end });
        if (entry.found_existing) continue;
        if (result.items.len >= options.max_mentions) return error.GeneratedExtractionLimitExceeded;
        try result.append(a, .{ .choice = mention.choice, .start = start, .end = end });
    }
    std.mem.sort(literal.Mention, result.items, {}, struct {
        fn before(_: void, left: literal.Mention, right: literal.Mention) bool {
            return left.start < right.start or (left.start == right.start and (left.end < right.end or (left.end == right.end and left.choice < right.choice)));
        }
    }.before);
    return result.toOwnedSlice(a);
}

test "generated constrained alignment preserves original Unicode whitespace spans and every occurrence" {
    const a = std.testing.allocator;
    const source = "New\n  York meets NEW\u{a0}YORK and New York.";
    const mentions = try find(a, source, &.{"New York"}, .{});
    defer a.free(mentions);
    try std.testing.expectEqual(@as(usize, 3), mentions.len);
    try std.testing.expectEqualStrings("New\n  York", source[mentions[0].start..mentions[0].end]);
    try std.testing.expectEqualStrings("NEW\u{a0}YORK", source[mentions[1].start..mentions[1].end]);
    try std.testing.expectEqualStrings("New York", source[mentions[2].start..mentions[2].end]);
}

test "generated constrained normalized alignment unwinds every allocation failure" {
    const Exercise = struct {
        fn run(a: A) !void {
            const mentions = try find(a, "New\n York and NEW YORK", &.{ "New York", "absent" }, .{});
            defer a.free(mentions);
            try std.testing.expectEqual(@as(usize, 2), mentions.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Exercise.run, .{});
}
