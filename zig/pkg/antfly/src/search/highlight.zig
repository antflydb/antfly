// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Highlighting: extract text fragments with query term positions.
//!
//! Re-analyzes stored text to find query terms, returns fragments with
//! byte-offset highlight spans for rendering (bold, underline, etc.).

const std = @import("std");
const Allocator = std.mem.Allocator;
const analysis_mod = @import("analysis.zig");

pub const Span = struct {
    start: u32,
    end: u32,
};

pub const Fragment = struct {
    text: []const u8,
    offset: u32,
    highlights: []const Span,
};

/// Highlight query terms in text, returning the best fragments.
///
/// Analyzes `text` with `analyzer` to find tokens matching `terms`.
/// Selects up to `max_fragments` windows of `fragment_size` bytes
/// ranked by term density, and returns fragments with highlight spans.
pub fn highlight(
    alloc: Allocator,
    text: []const u8,
    terms: []const []const u8,
    analyzer: *const analysis_mod.Analyzer,
    max_fragments: u32,
    fragment_size: u32,
) ![]Fragment {
    if (text.len == 0 or terms.len == 0) return &.{};

    // Analyze the text to get tokens with byte positions
    const tokens = try analyzer.analyze(alloc, text);
    defer analysis_mod.Analyzer.freeTokens(alloc, tokens);

    if (tokens.len == 0) return &.{};

    // Mark which tokens match query terms
    var match_flags = try alloc.alloc(bool, tokens.len);
    defer alloc.free(match_flags);
    for (tokens, 0..) |tok, i| {
        match_flags[i] = false;
        for (terms) |t| {
            if (std.mem.eql(u8, tok.term, t)) {
                match_flags[i] = true;
                break;
            }
        }
    }

    // Score windows by match density and select best non-overlapping ones
    const text_len: u32 = @intCast(text.len);
    const frag_size = @min(fragment_size, text_len);

    var windows = std.ArrayListUnmanaged(ScoredWindow).empty;
    defer windows.deinit(alloc);

    // Generate candidate windows centered on matching tokens
    for (tokens, 0..) |tok, i| {
        if (!match_flags[i]) continue;

        // Center window on this token
        const center = tok.start_byte + (tok.end_byte - tok.start_byte) / 2;
        const win_start = if (center >= frag_size / 2) center - frag_size / 2 else 0;
        const win_end = @min(win_start + frag_size, text_len);

        // Count matches in this window
        var match_count: u32 = 0;
        for (tokens, 0..) |t, j| {
            if (match_flags[j] and t.start_byte >= win_start and t.end_byte <= win_end) {
                match_count += 1;
            }
        }

        try windows.append(alloc, .{ .start = win_start, .end = win_end, .score = match_count });
    }

    if (windows.items.len == 0) return &.{};

    // Sort by score descending
    std.mem.sort(ScoredWindow, windows.items, {}, struct {
        fn cmp(_: void, a: ScoredWindow, b: ScoredWindow) bool {
            if (a.score != b.score) return a.score > b.score;
            return a.start < b.start;
        }
    }.cmp);

    // Select top non-overlapping windows
    var selected = std.ArrayListUnmanaged(ScoredWindow).empty;
    defer selected.deinit(alloc);

    for (windows.items) |w| {
        if (selected.items.len >= max_fragments) break;
        var overlaps = false;
        for (selected.items) |s| {
            if (w.start < s.end and w.end > s.start) {
                overlaps = true;
                break;
            }
        }
        if (!overlaps) try selected.append(alloc, w);
    }

    // Sort selected windows by position for natural reading order
    std.mem.sort(ScoredWindow, selected.items, {}, struct {
        fn cmp(_: void, a: ScoredWindow, b: ScoredWindow) bool {
            return a.start < b.start;
        }
    }.cmp);

    // Build fragments with highlight spans
    var fragments = try alloc.alloc(Fragment, selected.items.len);
    errdefer alloc.free(fragments);

    for (selected.items, 0..) |win, fi| {
        var spans = std.ArrayListUnmanaged(Span).empty;
        defer spans.deinit(alloc);

        for (tokens, 0..) |tok, ti| {
            if (match_flags[ti] and tok.start_byte >= win.start and tok.end_byte <= win.end) {
                try spans.append(alloc, .{
                    .start = tok.start_byte - win.start,
                    .end = tok.end_byte - win.start,
                });
            }
        }

        fragments[fi] = .{
            .text = text[win.start..win.end],
            .offset = win.start,
            .highlights = try alloc.dupe(Span, spans.items),
        };
    }

    return fragments;
}

/// Free fragments returned by highlight(). Does NOT free the source text.
pub fn freeFragments(alloc: Allocator, fragments: []Fragment) void {
    for (fragments) |f| {
        alloc.free(f.highlights);
    }
    alloc.free(fragments);
}

const ScoredWindow = struct {
    start: u32,
    end: u32,
    score: u32,
};

// ============================================================================
// Tests
// ============================================================================

test "highlight exact terms" {
    const alloc = std.testing.allocator;

    const text = "the quick brown fox jumps over the lazy dog";
    const terms = &[_][]const u8{"quick", "fox"};
    // Use simple analyzer (lowercase only, no stemming/stop words)
    const analyzer = &analysis_mod.simple_analyzer;

    const fragments = try highlight(alloc, text, terms, analyzer, 3, 100);
    defer freeFragments(alloc, fragments);

    try std.testing.expectEqual(@as(usize, 1), fragments.len);
    // Both terms should be highlighted
    try std.testing.expect(fragments[0].highlights.len >= 2);
}

test "highlight with stemming" {
    const alloc = std.testing.allocator;

    const text = "the runners are running quickly through fields";
    // After default analyzer (stem): "runner" → "runner", "running" → "run"
    // Query term "run" should match "running" (stemmed to "run")
    const terms = &[_][]const u8{"run"};
    const analyzer = &analysis_mod.default_analyzer;

    const fragments = try highlight(alloc, text, terms, analyzer, 3, 100);
    defer freeFragments(alloc, fragments);

    try std.testing.expectEqual(@as(usize, 1), fragments.len);
    // "running" should be highlighted (stems to "run")
    try std.testing.expect(fragments[0].highlights.len >= 1);
}

test "highlight empty text" {
    const alloc = std.testing.allocator;

    const fragments = try highlight(alloc, "", &[_][]const u8{"test"}, &analysis_mod.default_analyzer, 3, 50);
    try std.testing.expectEqual(@as(usize, 0), fragments.len);
}

test "highlight no matching terms" {
    const alloc = std.testing.allocator;

    const text = "hello world";
    const terms = &[_][]const u8{"xyz"};
    const analyzer = &analysis_mod.simple_analyzer;

    const fragments = try highlight(alloc, text, terms, analyzer, 3, 100);
    try std.testing.expectEqual(@as(usize, 0), fragments.len);
}

test "highlight span offsets" {
    const alloc = std.testing.allocator;

    const text = "hello world";
    const terms = &[_][]const u8{"world"};
    const analyzer = &analysis_mod.simple_analyzer;

    const fragments = try highlight(alloc, text, terms, analyzer, 1, 100);
    defer freeFragments(alloc, fragments);

    try std.testing.expectEqual(@as(usize, 1), fragments.len);
    try std.testing.expectEqual(@as(usize, 1), fragments[0].highlights.len);
    // "world" starts at byte 6 in text, fragment starts at 0 (text fits in one fragment)
    const span = fragments[0].highlights[0];
    const highlighted = fragments[0].text[span.start..span.end];
    try std.testing.expectEqualStrings("world", highlighted);
}
