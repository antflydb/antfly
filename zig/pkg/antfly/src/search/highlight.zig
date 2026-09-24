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

//! Highlighting: extract text fragments with query match positions.
//!
//! Re-analyzes stored text with the field's analyzer, locates the tokens (or
//! byte ranges inside tokens) that the query matched, and returns fragments
//! with byte-offset highlight spans for rendering (bold, underline, etc.).

const std = @import("std");
const Allocator = std.mem.Allocator;
const analysis_mod = @import("analysis.zig");
const wildcard_mod = @import("wildcard.zig");
const regex_mod = @import("regex.zig");

pub const Span = struct {
    start: u32,
    end: u32,
};

pub const Fragment = struct {
    text: []const u8,
    offset: u32,
    highlights: []const Span,
};

/// One way a query clause can match analyzed text. `term`, `prefix`,
/// `wildcard`, `fuzzy`, and `regexp` are evaluated against analyzed tokens
/// and mark the whole surface token. `contains` is evaluated against the raw
/// surface text (ASCII case-insensitively) and marks only the contained
/// bytes, including matches that span two adjacent tokens, which is how a
/// `substring` companion query is highlighted.
pub const Matcher = union(enum) {
    term: []const u8,
    prefix: []const u8,
    contains: []const u8,
    wildcard: []const u8,
    fuzzy: Fuzzy,
    regexp: Regexp,

    pub const Fuzzy = struct {
        term: []const u8,
        max_edits: u8,
    };

    pub const Regexp = struct {
        pattern: []const u8,
        compiled: *regex_mod.RegexAutomaton,
    };
};

/// Highlight query terms in text, returning the best fragments.
///
/// Analyzes `text` with `analyzer` to find tokens equal to `terms`. Selects
/// up to `max_fragments` windows of `fragment_size` bytes ranked by match
/// density, and returns fragments with highlight spans.
pub fn highlight(
    alloc: Allocator,
    text: []const u8,
    terms: []const []const u8,
    analyzer: *const analysis_mod.Analyzer,
    max_fragments: u32,
    fragment_size: u32,
) ![]Fragment {
    if (text.len == 0 or terms.len == 0) return &.{};
    const matchers = try alloc.alloc(Matcher, terms.len);
    defer alloc.free(matchers);
    for (terms, matchers) |term, *matcher| matcher.* = .{ .term = term };
    return highlightMatchers(alloc, text, matchers, analyzer, max_fragments, fragment_size);
}

/// Highlight every byte range matched by any of `matchers`.
pub fn highlightMatchers(
    alloc: Allocator,
    text: []const u8,
    matchers: []const Matcher,
    analyzer: *const analysis_mod.Analyzer,
    max_fragments: u32,
    fragment_size: u32,
) ![]Fragment {
    if (text.len == 0 or matchers.len == 0 or max_fragments == 0) return &.{};

    const tokens = try analyzer.analyze(alloc, text);
    defer analysis_mod.Analyzer.freeTokens(alloc, tokens);

    var spans = std.ArrayListUnmanaged(Span).empty;
    defer spans.deinit(alloc);
    try collectMatchSpans(alloc, tokens, matchers, &spans);

    // `contains` matchers come from substring companions, which index every
    // surface word and every adjacent word pair regardless of the root
    // field's stop words or stemming. Evaluate them over the plain surface
    // words so a span can never bridge a word the companion never joined.
    var has_contains = false;
    for (matchers) |matcher| has_contains = has_contains or matcher == .contains;
    if (has_contains) {
        const words = try analysis_mod.substring_query_analyzer.analyze(alloc, text);
        defer analysis_mod.Analyzer.freeTokens(alloc, words);
        try collectContainsSpans(alloc, text, words, matchers, &spans);
    }
    if (spans.items.len == 0) return &.{};
    normalizeSpans(&spans);

    // Score windows by match density and select the best non-overlapping ones.
    const text_len: u32 = @intCast(text.len);
    const frag_size = @max(@min(fragment_size, text_len), 1);

    var windows = std.ArrayListUnmanaged(ScoredWindow).empty;
    defer windows.deinit(alloc);

    for (spans.items) |span| {
        // Center the window on this match, snapping to UTF-8 boundaries.
        const center = span.start + (span.end - span.start) / 2;
        var win_start = if (center >= frag_size / 2) center - frag_size / 2 else 0;
        var win_end = @min(win_start + frag_size, text_len);
        if (win_end - win_start < frag_size and win_start > 0) {
            win_start = if (win_end >= frag_size) win_end - frag_size else 0;
        }
        win_start = utf8Floor(text, win_start);
        win_end = utf8Ceil(text, win_end);

        var match_count: u32 = 0;
        for (spans.items) |other| {
            if (other.start >= win_start and other.end <= win_end) match_count += 1;
        }
        try windows.append(alloc, .{ .start = win_start, .end = win_end, .score = match_count });
    }

    std.mem.sort(ScoredWindow, windows.items, {}, struct {
        fn cmp(_: void, a: ScoredWindow, b: ScoredWindow) bool {
            if (a.score != b.score) return a.score > b.score;
            return a.start < b.start;
        }
    }.cmp);

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

    std.mem.sort(ScoredWindow, selected.items, {}, struct {
        fn cmp(_: void, a: ScoredWindow, b: ScoredWindow) bool {
            return a.start < b.start;
        }
    }.cmp);

    const fragments = try alloc.alloc(Fragment, selected.items.len);
    var initialized: usize = 0;
    errdefer {
        for (fragments[0..initialized]) |fragment| alloc.free(fragment.highlights);
        alloc.free(fragments);
    }
    for (selected.items, 0..) |win, fi| {
        var fragment_spans = std.ArrayListUnmanaged(Span).empty;
        defer fragment_spans.deinit(alloc);
        for (spans.items) |span| {
            if (span.start >= win.start and span.end <= win.end) {
                try fragment_spans.append(alloc, .{
                    .start = span.start - win.start,
                    .end = span.end - win.start,
                });
            }
        }
        fragments[fi] = .{
            .text = text[win.start..win.end],
            .offset = win.start,
            .highlights = try alloc.dupe(Span, fragment_spans.items),
        };
        initialized += 1;
    }
    return fragments;
}

fn collectMatchSpans(
    alloc: Allocator,
    tokens: []const analysis_mod.Token,
    matchers: []const Matcher,
    spans: *std.ArrayListUnmanaged(Span),
) !void {
    for (tokens) |tok| {
        // Analyzers may emit several tokens for one surface span (shingles,
        // n-grams); the surface span is what gets highlighted.
        for (matchers) |matcher| {
            switch (matcher) {
                .term => |term| if (std.mem.eql(u8, tok.term, term)) try spans.append(alloc, .{ .start = tok.start_byte, .end = tok.end_byte }),
                .prefix => |prefix| if (std.mem.startsWith(u8, tok.term, prefix)) try spans.append(alloc, .{ .start = tok.start_byte, .end = tok.end_byte }),
                .wildcard => |pattern| if (wildcard_mod.match(pattern, tok.term)) try spans.append(alloc, .{ .start = tok.start_byte, .end = tok.end_byte }),
                .fuzzy => |fuzzy| if (boundedEditDistance(tok.term, fuzzy.term, fuzzy.max_edits) <= fuzzy.max_edits) try spans.append(alloc, .{ .start = tok.start_byte, .end = tok.end_byte }),
                .regexp => |regexp| if (regex_mod.matchesCompiled(regexp.pattern, regexp.compiled, tok.term)) try spans.append(alloc, .{ .start = tok.start_byte, .end = tok.end_byte }),
                .contains => {},
            }
        }
    }
}

/// `words` are the surface words of `text` as the substring companion sees
/// them (unicode words, lowercased, no stop words, no stemming). Matching is
/// ASCII case-insensitive, which is exactly the folding the companion's
/// `lowercase` filter applies at index time.
fn collectContainsSpans(
    alloc: Allocator,
    text: []const u8,
    words: []const analysis_mod.Token,
    matchers: []const Matcher,
    spans: *std.ArrayListUnmanaged(Span),
) !void {
    for (words, 0..) |word, i| {
        const surface = text[word.start_byte..word.end_byte];
        for (matchers) |matcher| {
            const needle = switch (matcher) {
                .contains => |needle| needle,
                else => continue,
            };
            if (needle.len == 0) continue;
            if (indexOfIgnoreCase(surface, needle)) |index| {
                try spans.append(alloc, .{
                    .start = word.start_byte + @as(u32, @intCast(index)),
                    .end = word.start_byte + @as(u32, @intCast(index + needle.len)),
                });
                continue;
            }
            // The companion joins adjacent words without a separator, so a
            // match may start inside this word and end inside the next one.
            if (i + 1 >= words.len) continue;
            const next = words[i + 1];
            const next_surface = text[next.start_byte..next.end_byte];
            if (try joinedContainsIgnoreCase(alloc, surface, next_surface, needle)) |joined_index| {
                if (joined_index >= surface.len or joined_index + needle.len <= surface.len) continue;
                try spans.append(alloc, .{
                    .start = word.start_byte + @as(u32, @intCast(joined_index)),
                    .end = next.start_byte + @as(u32, @intCast(joined_index + needle.len - surface.len)),
                });
            }
        }
    }
}

fn joinedContainsIgnoreCase(alloc: Allocator, a: []const u8, b: []const u8, needle: []const u8) !?usize {
    if (a.len + b.len < needle.len) return null;
    const joined = try alloc.alloc(u8, a.len + b.len);
    defer alloc.free(joined);
    @memcpy(joined[0..a.len], a);
    @memcpy(joined[a.len..], b);
    return indexOfIgnoreCase(joined, needle);
}

fn indexOfIgnoreCase(haystack: []const u8, needle: []const u8) ?usize {
    if (needle.len == 0 or needle.len > haystack.len) return null;
    var i: usize = 0;
    while (i + needle.len <= haystack.len) : (i += 1) {
        if (std.ascii.eqlIgnoreCase(haystack[i..][0..needle.len], needle)) return i;
    }
    return null;
}

/// Merge overlapping or touching spans and sort them by start offset.
fn normalizeSpans(spans: *std.ArrayListUnmanaged(Span)) void {
    std.mem.sort(Span, spans.items, {}, struct {
        fn cmp(_: void, a: Span, b: Span) bool {
            if (a.start != b.start) return a.start < b.start;
            return a.end > b.end;
        }
    }.cmp);
    var write: usize = 0;
    for (spans.items) |span| {
        if (write > 0 and span.start <= spans.items[write - 1].end) {
            spans.items[write - 1].end = @max(spans.items[write - 1].end, span.end);
            continue;
        }
        spans.items[write] = span;
        write += 1;
    }
    spans.items.len = write;
}

fn utf8Floor(text: []const u8, index: u32) u32 {
    var i = index;
    while (i > 0 and i < text.len and text[i] & 0xC0 == 0x80) i -= 1;
    return i;
}

fn utf8Ceil(text: []const u8, index: u32) u32 {
    var i = index;
    while (i < text.len and text[i] & 0xC0 == 0x80) i += 1;
    return i;
}

/// Levenshtein distance capped at `limit + 1` so long tokens bail out early.
fn boundedEditDistance(a: []const u8, b: []const u8, limit: u8) u32 {
    const cap: u32 = @as(u32, limit) + 1;
    if (a.len > b.len + cap or b.len > a.len + cap) return cap;
    if (a.len > 64 or b.len > 64) return if (std.mem.eql(u8, a, b)) 0 else cap;
    var prev: [65]u32 = undefined;
    var curr: [65]u32 = undefined;
    for (0..b.len + 1) |j| prev[j] = @intCast(j);
    for (a, 1..) |ca, i| {
        curr[0] = @intCast(i);
        var row_min: u32 = curr[0];
        for (b, 1..) |cb, j| {
            const cost: u32 = if (ca == cb) 0 else 1;
            curr[j] = @min(@min(prev[j] + 1, curr[j - 1] + 1), prev[j - 1] + cost);
            row_min = @min(row_min, curr[j]);
        }
        if (row_min > limit) return cap;
        @memcpy(prev[0 .. b.len + 1], curr[0 .. b.len + 1]);
    }
    return prev[b.len];
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
    const terms = &[_][]const u8{ "quick", "fox" };
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

test "highlight contains matcher marks bytes inside and across tokens" {
    const alloc = std.testing.allocator;
    const analyzer = &analysis_mod.simple_analyzer;

    const inside = try highlightMatchers(alloc, "install the Rag3-Weaver kit today", &.{.{ .contains = "g3we" }}, analyzer, 1, 200);
    defer freeFragments(alloc, inside);
    try std.testing.expectEqual(@as(usize, 1), inside.len);
    try std.testing.expectEqual(@as(usize, 1), inside[0].highlights.len);
    const span = inside[0].highlights[0];
    // The match starts inside "Rag3" and ends inside "Weaver", separator included.
    try std.testing.expectEqualStrings("g3-We", inside[0].text[span.start..span.end]);

    const whole = try highlightMatchers(alloc, "sku RAG3WEAVER", &.{.{ .contains = "rag3weaver" }}, analyzer, 1, 200);
    defer freeFragments(alloc, whole);
    try std.testing.expectEqual(@as(usize, 1), whole.len);
    const whole_span = whole[0].highlights[0];
    try std.testing.expectEqualStrings("RAG3WEAVER", whole[0].text[whole_span.start..whole_span.end]);

    const none = try highlightMatchers(alloc, "rag3 kit weaver", &.{.{ .contains = "g3we" }}, analyzer, 1, 200);
    try std.testing.expectEqual(@as(usize, 0), none.len);

    // The root analyzer drops "the", but the companion never joined
    // "rag3" with "weaver", so the highlight must not bridge them either.
    const stop_word = try highlightMatchers(alloc, "rag3 the weaver", &.{.{ .contains = "g3we" }}, &analysis_mod.default_analyzer, 1, 200);
    try std.testing.expectEqual(@as(usize, 0), stop_word.len);
    const stemmed = try highlightMatchers(alloc, "Rag3 Weavers", &.{.{ .contains = "g3weaver" }}, &analysis_mod.default_analyzer, 1, 200);
    defer freeFragments(alloc, stemmed);
    try std.testing.expectEqual(@as(usize, 1), stemmed.len);
    try std.testing.expectEqualStrings("g3 Weaver", stemmed[0].text[stemmed[0].highlights[0].start..stemmed[0].highlights[0].end]);
}

test "highlight prefix wildcard fuzzy and regexp matchers mark whole tokens" {
    const alloc = std.testing.allocator;
    const analyzer = &analysis_mod.simple_analyzer;
    const text = "scheduler schedules the schedule";

    var compiled = try regex_mod.compile(alloc, "sched[a-z]+s");
    defer compiled.deinit();
    const matchers = [_]Matcher{
        .{ .prefix = "schedul" },
        .{ .wildcard = "sched*e" },
        .{ .fuzzy = .{ .term = "schdule", .max_edits = 1 } },
        .{ .regexp = .{ .pattern = "sched[a-z]+s", .compiled = &compiled } },
    };
    const fragments = try highlightMatchers(alloc, text, &matchers, analyzer, 1, 200);
    defer freeFragments(alloc, fragments);
    try std.testing.expectEqual(@as(usize, 1), fragments.len);
    // Overlapping matches on one token merge into a single span per token.
    try std.testing.expectEqual(@as(usize, 3), fragments[0].highlights.len);
    try std.testing.expectEqualStrings("scheduler", fragments[0].text[fragments[0].highlights[0].start..fragments[0].highlights[0].end]);
    try std.testing.expectEqualStrings("schedules", fragments[0].text[fragments[0].highlights[1].start..fragments[0].highlights[1].end]);
    try std.testing.expectEqualStrings("schedule", fragments[0].text[fragments[0].highlights[2].start..fragments[0].highlights[2].end]);
}

test "highlight fragments respect size and count limits" {
    const alloc = std.testing.allocator;
    const analyzer = &analysis_mod.simple_analyzer;
    var text = std.ArrayListUnmanaged(u8).empty;
    defer text.deinit(alloc);
    for (0..40) |i| {
        const word = try std.fmt.allocPrint(alloc, "filler{d} ", .{i});
        defer alloc.free(word);
        try text.appendSlice(alloc, word);
        if (i % 10 == 9) try text.appendSlice(alloc, "needle ");
    }
    const fragments = try highlight(alloc, text.items, &.{"needle"}, analyzer, 2, 40);
    defer freeFragments(alloc, fragments);
    try std.testing.expectEqual(@as(usize, 2), fragments.len);
    for (fragments) |fragment| {
        try std.testing.expect(fragment.text.len <= 40);
        try std.testing.expect(fragment.highlights.len >= 1);
        for (fragment.highlights) |span| {
            try std.testing.expectEqualStrings("needle", fragment.text[span.start..span.end]);
        }
    }
    try std.testing.expect(fragments[0].offset < fragments[1].offset);
}
