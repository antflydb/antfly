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

//! Replace touched documents in an existing native text segment without
//! hydrating untouched document bodies or tokenizing their text again.
const std = @import("std");
const t = @import("types.zig");
const A = std.mem.Allocator;
const Cancellation = @import("antfly_cancellation").CancellationToken;
const absent = std.math.maxInt(u32);

pub fn encodeReplacementAlloc(a: A, base: t.Segment, delta: t.Segment, removed: []const []const u8, cancellation: Cancellation) ![]u8 {
    if (!std.mem.eql(u8, base.index_name, delta.index_name) or !std.mem.eql(u8, base.source_name, delta.source_name) or !std.mem.eql(u8, base.config_json, delta.config_json)) return error.TextIndexDefinitionChanged;
    var changed = std.StringHashMapUnmanaged(void).empty;
    defer changed.deinit(a);
    for (removed) |id| try changed.put(a, id, {});
    for ([_][]const t.DocumentEntry{ base.docs, delta.docs }) |docs| for (docs, 0..) |doc, i| {
        if (i != 0 and std.mem.order(u8, docs[i - 1].doc_id, doc.doc_id) != .lt) return error.InvalidTextSegment;
    };
    const old_map = try a.alloc(u32, base.docs.len);
    defer a.free(old_map);
    @memset(old_map, absent);
    const new_map = try a.alloc(u32, delta.docs.len);
    defer a.free(new_map);
    @memset(new_map, absent);
    var docs = std.ArrayListUnmanaged(t.DocumentEntry).empty;
    defer docs.deinit(a); // document strings are borrowed from the inputs
    var i: usize = 0;
    var j: usize = 0;
    while (i < base.docs.len or j < delta.docs.len) {
        if (docs.items.len % 64 == 0) try cancellation.check();
        if (i < base.docs.len and changed.contains(base.docs[i].doc_id)) {
            i += 1;
            continue;
        }
        if (docs.items.len >= absent) return error.TextSegmentTooLarge;
        if (j == delta.docs.len or (i < base.docs.len and std.mem.order(u8, base.docs[i].doc_id, delta.docs[j].doc_id) == .lt)) {
            old_map[i] = @intCast(docs.items.len);
            try docs.append(a, base.docs[i]);
            i += 1;
        } else {
            if (i < base.docs.len and std.mem.eql(u8, base.docs[i].doc_id, delta.docs[j].doc_id)) i += 1;
            new_map[j] = @intCast(docs.items.len);
            try docs.append(a, delta.docs[j]);
            j += 1;
        }
    }
    var terms = std.ArrayListUnmanaged(t.TermEntry).empty;
    defer {
        for (terms.items) |term| a.free(term.postings);
        terms.deinit(a);
    }
    i = 0;
    j = 0;
    while (i < base.terms.len or j < delta.terms.len) {
        try cancellation.check();
        const order: std.math.Order = if (i == base.terms.len) .gt else if (j == delta.terms.len) .lt else std.mem.order(u8, base.terms[i].term, delta.terms[j].term);
        const term = if (order == .gt) delta.terms[j].term else base.terms[i].term;
        const old_posts: []const t.Posting = if (order != .gt) base.terms[i].postings else &.{};
        const new_posts: []const t.Posting = if (order != .lt) delta.terms[j].postings else &.{};
        var posts = std.ArrayListUnmanaged(t.Posting).empty;
        defer posts.deinit(a);
        var old_pos: usize = 0;
        var new_pos: usize = 0;
        var left = try next(old_posts, old_map, &old_pos);
        var right = try next(new_posts, new_map, &new_pos);
        while (left != null or right != null) {
            if (posts.items.len % 256 == 0) try cancellation.check();
            const use_left = right == null or (left != null and left.?.doc_index < right.?.doc_index);
            const posting = if (use_left) left.? else right.?;
            if (posts.items.len != 0 and posts.items[posts.items.len - 1].doc_index >= posting.doc_index) return error.InvalidTextSegment;
            try posts.append(a, posting);
            if (use_left) left = try next(old_posts, old_map, &old_pos) else right = try next(new_posts, new_map, &new_pos);
        }
        if (posts.items.len != 0) {
            const owned = try posts.toOwnedSlice(a);
            errdefer a.free(owned);
            try terms.append(a, .{ .term = term, .postings = owned });
        }
        if (order != .gt) i += 1;
        if (order != .lt) j += 1;
    }
    return @import("codec.zig").encodeAlloc(a, .{ .index_name = delta.index_name, .source_name = delta.source_name, .config_json = delta.config_json, .docs = docs.items, .terms = terms.items });
}

fn next(postings: []const t.Posting, map: []const u32, position: *usize) !?t.Posting {
    while (position.* < postings.len) {
        const value = postings[position.*];
        position.* += 1;
        if (value.doc_index >= map.len or value.term_freq == 0) return error.InvalidTextSegment;
        const index = map[value.doc_index];
        if (index != absent) return .{ .doc_index = index, .term_freq = value.term_freq };
    }
    return null;
}

test "serverless incremental text replacement preserves ordinals and unwinds every allocation" {
    const Exercise = struct {
        fn run(a: A) !void {
            var old_docs = [_]t.DocumentEntry{
                .{ .doc_id = @constCast("a"), .normalized_text = @constCast("alpha alpha"), .token_count = 2 },
                .{ .doc_id = @constCast("c"), .normalized_text = @constCast("obsolete"), .token_count = 1 },
                .{ .doc_id = @constCast("z"), .normalized_text = @constCast("alpha"), .token_count = 1 },
            };
            var new_docs = [_]t.DocumentEntry{.{ .doc_id = @constCast("b"), .normalized_text = @constCast("alpha"), .token_count = 1 }};
            var old_alpha = [_]t.Posting{ .{ .doc_index = 0, .term_freq = 2 }, .{ .doc_index = 2, .term_freq = 1 } };
            var old_obsolete = [_]t.Posting{.{ .doc_index = 1, .term_freq = 1 }};
            var new_alpha = [_]t.Posting{.{ .doc_index = 0, .term_freq = 1 }};
            var old_terms = [_]t.TermEntry{ .{ .term = @constCast("alpha"), .postings = &old_alpha }, .{ .term = @constCast("obsolete"), .postings = &old_obsolete } };
            var new_terms = [_]t.TermEntry{.{ .term = @constCast("alpha"), .postings = &new_alpha }};
            const encoded = try encodeReplacementAlloc(a, .{ .docs = &old_docs, .terms = &old_terms }, .{ .docs = &new_docs, .terms = &new_terms }, &.{ "c", "c", "missing" }, .none);
            defer a.free(encoded);
            var decoded = try @import("codec.zig").decodeAlloc(a, encoded);
            defer decoded.deinit(a);
            try std.testing.expectEqual(@as(usize, 3), decoded.docs.len);
            try std.testing.expectEqualStrings("b", decoded.docs[1].doc_id);
            try std.testing.expectEqualStrings("z", decoded.docs[2].doc_id);
            try std.testing.expectEqual(@as(usize, 1), decoded.terms.len);
            try std.testing.expectEqual(@as(u32, 2), decoded.terms[0].postings[0].term_freq);
            try std.testing.expectEqual(@as(u32, 2), decoded.terms[0].postings[2].doc_index);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Exercise.run, .{});
}
