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
pub const Control = @import("antfly_cancellation").CancellationToken;
const Poller = struct {
    control: Control,
    remaining: usize = 1,
    fn step(self: *Poller) !void {
        if (self.control.ptr == null) return;
        self.remaining -= 1;
        if (self.remaining == 0) {
            self.remaining = 256;
            try self.control.check();
        }
    }
};

pub const Span = struct {
    start: u32,
    end: u32,
};

pub const Fragment = struct {
    text: []const u8,
    offset: u32,
    highlights: []const Span,
    owned_text: bool = false,
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
    /// Exact keyword fields retain separators and case in one token.
    literal: []const u8,
    literal_prefix: []const u8,
    wildcard: []const u8,
    fuzzy: Fuzzy,
    regexp: Regexp,
    /// Pattern queries against a suffix-indexed substring companion.
    substring_term: []const u8,
    substring_wildcard: []const u8,
    substring_fuzzy: Fuzzy,
    substring_regexp: Regexp,

    pub const Fuzzy = struct {
        term: []const u8,
        max_edits: u8,
        prefix_len: u8 = 0,
    };

    pub const Regexp = struct {
        pattern: []const u8,
        compiled: *regex_mod.RegexAutomaton,
    };
};

/// Query-owned lookup tables. Exact terms and prefixes do not multiply token
/// traversal by the number of clauses; other patterns retain their semantics.
pub const Program = struct {
    terms: std.StringHashMapUnmanaged(void) = .empty,
    prefixes: std.StringHashMapUnmanaged(void) = .empty,
    lengths: std.ArrayListUnmanaged(usize) = .empty,
    patterns: std.ArrayListUnmanaged(Matcher) = .empty,
    pub fn init(a: Allocator, matchers: []const Matcher) !Program {
        var self: Program = .{};
        errdefer self.deinit(a);
        for (matchers) |m| switch (m) {
            .term => |v| try self.terms.put(a, v, {}),
            .prefix => |v| {
                try self.prefixes.put(a, v, {});
                if (std.mem.indexOfScalar(usize, self.lengths.items, v.len) == null) try self.lengths.append(a, v.len);
            },
            else => try self.patterns.append(a, m),
        };
        std.mem.sort(usize, self.lengths.items, {}, std.sort.asc(usize));
        return self;
    }
    pub fn deinit(self: *Program, a: Allocator) void {
        self.terms.deinit(a);
        self.prefixes.deinit(a);
        self.lengths.deinit(a);
        self.patterns.deinit(a);
    }
    fn matches(self: *const Program, text: []const u8, poller: *Poller) !bool {
        try poller.step();
        if (self.terms.contains(text)) return true;
        for (self.lengths.items) |n| {
            if (n > text.len) break;
            try poller.step();
            if (self.prefixes.contains(text[0..n])) return true;
        }
        return false;
    }
};
/// A value/field-scoped region, shared across query bindings but never retained
/// across source callbacks. Cache keys include byte identity and analyzer.
pub const AnalysisCache = struct {
    const Key = struct { pointer: usize, len: usize, analyzer: *const analysis_mod.Analyzer };
    a: Allocator,
    values: std.AutoHashMapUnmanaged(Key, []analysis_mod.Token) = .empty,
    analyses: usize = 0,
    fn get(self: *AnalysisCache, text: []const u8, analyzer: *const analysis_mod.Analyzer, poller: *Poller) ![]analysis_mod.Token {
        const key: Key = .{ .pointer = @intFromPtr(text.ptr), .len = text.len, .analyzer = analyzer };
        if (self.values.get(key)) |tokens| return tokens;
        var checked: CheckedAllocator = .{ .backing = self.a, .poller = poller };
        const controlled = analyzer.withControl(poller.control);
        const tokens = controlled.analyzeWithSourceOffsets(checked.allocator(), text) catch |err| return checked.failure orelse err;
        if (checked.failure) |err| return err;
        try self.values.put(self.a, key, tokens);
        self.analyses += 1;
        return tokens;
    }
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
    return highlightMatchersControlled(alloc, text, matchers, analyzer, max_fragments, fragment_size, .none);
}
/// The token is borrowed for this call. All matching, sorting, density and
/// selection loops poll it without changing ranking or source byte offsets.
pub fn highlightMatchersControlled(alloc: Allocator, text: []const u8, matchers: []const Matcher, analyzer: *const analysis_mod.Analyzer, max_fragments: u32, fragment_size: u32, control: Control) ![]Fragment {
    return highlightMatchersWithScratch(alloc, alloc, text, matchers, analyzer, max_fragments, fragment_size, control);
}
/// Scratch is a separate call-scoped capability. Only returned fragments and
/// their spans use the result allocator; token/window state never becomes
/// retained result memory when the result owner is monotonic.
pub fn highlightMatchersWithScratch(alloc: Allocator, scratch: Allocator, text: []const u8, matchers: []const Matcher, analyzer: *const analysis_mod.Analyzer, max_fragments: u32, fragment_size: u32, control: Control) ![]Fragment {
    return highlightPrepared(alloc, scratch, text, matchers, null, null, analyzer, max_fragments, fragment_size, control);
}
pub fn highlightPrepared(alloc: Allocator, scratch: Allocator, text: []const u8, matchers: []const Matcher, program: ?*const Program, cache: ?*AnalysisCache, analyzer: *const analysis_mod.Analyzer, max_fragments: u32, fragment_size: u32, control: Control) ![]Fragment {
    var workspace = std.heap.ArenaAllocator.init(scratch);
    defer workspace.deinit();
    const temporary = workspace.allocator();
    try control.check();
    var poller: Poller = .{ .control = control };
    if (text.len == 0 or matchers.len == 0 or max_fragments == 0) return &.{};

    var spans = std.ArrayListUnmanaged(Span).empty;
    defer spans.deinit(temporary);
    {
        var analysis_arena = std.heap.ArenaAllocator.init(temporary);
        defer analysis_arena.deinit();
        var checked: CheckedAllocator = .{ .backing = analysis_arena.allocator(), .poller = &poller };
        const token_allocator = checked.allocator();
        const controlled = analyzer.withControl(control);
        const tokens = if (cache) |shared| try shared.get(text, analyzer, &poller) else controlled.analyzeWithSourceOffsets(token_allocator, text) catch |err| return checked.failure orelse err;
        if (checked.failure) |err| return err;
        defer if (cache == null) analysis_mod.Analyzer.freeTokens(token_allocator, tokens);
        try control.check();

        try collectProgramSpans(temporary, tokens, matchers, program, &spans, &poller);

        for (matchers) |matcher| {
            switch (matcher) {
                .literal => |needle| {
                    if (needle.len > 0 and std.mem.eql(u8, text, needle)) {
                        try spans.append(temporary, .{ .start = 0, .end = @intCast(text.len) });
                    }
                },
                .literal_prefix => |prefix| {
                    if (prefix.len > 0 and std.mem.startsWith(u8, text, prefix)) {
                        try spans.append(temporary, .{ .start = 0, .end = @intCast(prefix.len) });
                    }
                },
                else => {},
            }
        }

        // `contains` matchers come from substring companions, which index every
        // surface word and every adjacent word pair regardless of the root
        // field's stop words or stemming. Evaluate them over the plain surface
        // words so a span can never bridge a word the companion never joined.
        var has_contains = false;
        var has_substring_pattern = false;
        for (matchers) |matcher| {
            has_contains = has_contains or matcher == .contains;
            has_substring_pattern = has_substring_pattern or matcher == .substring_term or matcher == .substring_wildcard or
                matcher == .substring_fuzzy or matcher == .substring_regexp;
        }
        if (has_contains or has_substring_pattern) {
            const words = if (cache) |shared| try shared.get(text, &analysis_mod.substring_query_analyzer, &poller) else analysis_mod.substring_query_analyzer.withControl(control).analyze(token_allocator, text) catch |err| return checked.failure orelse err;
            if (checked.failure) |err| return err;
            defer if (cache == null) analysis_mod.Analyzer.freeTokens(token_allocator, words);
            if (has_contains) try collectContainsSpans(temporary, text, words, matchers, &spans, &poller);
            if (has_substring_pattern) try collectSubstringPatternSpans(temporary, words, matchers, &spans, &poller);
        }
    }
    return selectFragments(alloc, temporary, &spans, .{ .text = text }, max_fragments, fragment_size, &poller);
}
const TextSource = union(enum) {
    text: []const u8,
    stored: *@import("../section/source_column.zig").Boundaries,
    fn length(self: TextSource) u32 {
        return switch (self) {
            .text => |s| @intCast(s.len),
            .stored => |s| s.text.length,
        };
    }
    fn floor(self: TextSource, at: u32) !u32 {
        return switch (self) {
            .text => |s| utf8Floor(s, at),
            .stored => |s| s.floor(at),
        };
    }
    fn ceil(self: TextSource, at: u32) !u32 {
        return switch (self) {
            .text => |s| utf8Ceil(s, at),
            .stored => |s| s.ceil(at),
        };
    }
    fn read(self: TextSource, a: Allocator, first: u32, end: u32, control: Control) ![]const u8 {
        return switch (self) {
            .text => |s| s[first..end],
            .stored => |s| try s.text.read(a, first, end, control),
        };
    }
};
pub fn highlightStored(a: Allocator, scratch: Allocator, source: @import("../section/source_column.zig").Text, tokens: []const analysis_mod.Token, matchers: []const Matcher, program: ?*const Program, count: u32, size: u32, control: Control) !?[]Fragment {
    for (matchers) |matcher| switch (matcher) {
        .term, .prefix, .wildcard, .regexp, .fuzzy => {},
        else => return null,
    };
    var arena = std.heap.ArenaAllocator.init(scratch);
    defer arena.deinit();
    var poller: Poller = .{ .control = control };
    try control.check();
    var spans: std.ArrayListUnmanaged(Span) = .empty;
    if (source.length == 0 or count == 0 or matchers.len == 0) return &.{};
    try collectProgramSpans(arena.allocator(), tokens, matchers, program, &spans, &poller);
    var boundaries = @import("../section/source_column.zig").Boundaries.init(scratch, source, control);
    defer boundaries.deinit();
    return try selectFragments(a, arena.allocator(), &spans, .{ .stored = &boundaries }, count, size, &poller);
}
fn selectFragments(alloc: Allocator, temporary: Allocator, spans: *std.ArrayListUnmanaged(Span), source: TextSource, max_fragments: u32, fragment_size: u32, poller: *Poller) ![]Fragment {
    if (spans.items.len == 0) return &.{};
    try normalizeSpans(temporary, spans, poller);

    // Score windows by match density and select the best non-overlapping ones.
    const text_len: u32 = source.length();
    const frag_size = @max(@min(fragment_size, text_len), 1);

    var windows = std.ArrayListUnmanaged(ScoredWindow).empty;
    defer windows.deinit(temporary);

    var sweep: DensitySweep = .{};
    for (spans.items) |span| {
        try poller.step();
        // Center the window on this match, snapping to UTF-8 boundaries.
        const center = span.start + (span.end - span.start) / 2;
        var win_start = if (center >= frag_size / 2) center - frag_size / 2 else 0;
        var win_end = @min(win_start + frag_size, text_len);
        if (win_end - win_start < frag_size and win_start > 0) {
            win_start = if (win_end >= frag_size) win_end - frag_size else 0;
        }
        win_start = try source.floor(win_start);
        win_end = try source.ceil(win_end);

        const match_count = try sweep.count(spans.items, win_start, win_end, poller);
        // Clipping at either text boundary can produce identical windows.
        if (windows.items.len != 0 and windows.items[windows.items.len - 1].start == win_start and windows.items[windows.items.len - 1].end == win_end) continue;
        try windows.append(temporary, .{ .start = win_start, .end = win_end, .score = match_count });
    }

    // Heapify in place; inspect only as many ranked candidates as the exact
    // greedy non-overlap selection needs. No full window sort or second copy.
    var heap_len = windows.items.len;
    var parent = heap_len / 2;
    while (parent > 0) {
        parent -= 1;
        try siftWindows(windows.items, heap_len, parent, poller);
    }
    var selected = std.ArrayListUnmanaged(ScoredWindow).empty;
    defer selected.deinit(temporary);
    while (heap_len != 0 and selected.items.len < max_fragments) {
        try poller.step();
        const w = windows.items[0];
        heap_len -= 1;
        if (heap_len != 0) {
            windows.items[0] = windows.items[heap_len];
            try siftWindows(windows.items, heap_len, 0, poller);
        }
        const overlaps = for (selected.items) |chosen| {
            try poller.step();
            if (w.start < chosen.end and w.end > chosen.start) break true;
        } else false;
        if (!overlaps) try selected.append(temporary, w);
    }

    try controlledSort(ScoredWindow, temporary, selected.items, poller, struct {
        fn cmp(_: void, a: ScoredWindow, b: ScoredWindow) bool {
            return a.start < b.start;
        }
    }.cmp);

    return renderSelectedFragments(alloc, temporary, spans.items, source, selected.items, poller);
}
fn renderSelectedFragments(alloc: Allocator, temporary: Allocator, spans: []const Span, source: TextSource, selected: []const ScoredWindow, poller: *Poller) ![]Fragment {
    const fragments = try alloc.alloc(Fragment, selected.len);
    var initialized: usize = 0;
    errdefer {
        for (fragments[0..initialized]) |fragment| {
            alloc.free(fragment.highlights);
            if (fragment.owned_text) alloc.free(fragment.text);
        }
        alloc.free(fragments);
    }
    var span_at: usize = 0;
    for (selected, 0..) |win, fi| {
        try poller.step();
        var fragment_spans = std.ArrayListUnmanaged(Span).empty;
        defer fragment_spans.deinit(temporary);
        while (span_at < spans.len and spans[span_at].end <= win.start) : (span_at += 1) try poller.step();
        for (spans[span_at..]) |span| {
            try poller.step();
            if (span.start >= win.end) break;
            if (span.start < win.end and span.end > win.start) {
                try fragment_spans.append(temporary, .{
                    .start = @max(span.start, win.start) - win.start,
                    .end = @min(span.end, win.end) - win.start,
                });
            }
        }
        const fragment_text = try source.read(alloc, win.start, win.end, poller.control);
        errdefer if (source == .stored) alloc.free(fragment_text);
        fragments[fi] = .{
            .text = fragment_text,
            .owned_text = source == .stored,
            .offset = win.start,
            .highlights = try alloc.dupe(Span, fragment_spans.items),
        };
        initialized += 1;
    }
    try poller.control.check();
    return fragments;
}

/// Tokenization is a call-scoped region. Allocation checkpoints make existing
/// analyzers cancellable without changing their mapping/filter contracts, and
/// region teardown also owns partial tokenizer/filter output on any failure.
const CheckedAllocator = struct {
    backing: Allocator,
    poller: *Poller,
    failure: ?anyerror = null,
    fn allocator(self: *CheckedAllocator) Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    fn active(self: *CheckedAllocator) bool {
        if (self.failure != null) return false;
        self.poller.step() catch |err| {
            self.failure = err;
            return false;
        };
        return true;
    }
    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *CheckedAllocator = @ptrCast(@alignCast(raw));
        if (!self.active()) return null;
        return self.backing.rawAlloc(len, alignment, ra);
    }
    fn resize(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
        const self: *CheckedAllocator = @ptrCast(@alignCast(raw));
        if (!self.active()) return false;
        return self.backing.rawResize(bytes, alignment, len, ra);
    }
    fn remap(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        const self: *CheckedAllocator = @ptrCast(@alignCast(raw));
        if (!self.active()) return null;
        return self.backing.rawRemap(bytes, alignment, len, ra);
    }
    fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *CheckedAllocator = @ptrCast(@alignCast(raw));
        self.backing.rawFree(bytes, alignment, ra);
    }
};

const DensitySweep = struct {
    first: usize = 0,
    end: usize = 0,
    fn count(self: *DensitySweep, spans: []const Span, start: u32, end: u32, poller: *Poller) !u32 {
        while (self.first < spans.len and spans[self.first].end <= start) : (self.first += 1) try poller.step();
        self.end = @max(self.end, self.first);
        while (self.end < spans.len and spans[self.end].start < end) : (self.end += 1) try poller.step();
        return @intCast(self.end - self.first);
    }
};
fn betterWindow(a: ScoredWindow, b: ScoredWindow) bool {
    if (a.score != b.score) return a.score > b.score;
    if (a.start != b.start) return a.start < b.start;
    return a.end < b.end;
}
fn siftWindows(windows: []ScoredWindow, count: usize, first: usize, poller: *Poller) !void {
    var at = first;
    while (at < count / 2) {
        try poller.step();
        var child = at * 2 + 1;
        if (child + 1 < count and betterWindow(windows[child + 1], windows[child])) child += 1;
        if (!betterWindow(windows[child], windows[at])) break;
        std.mem.swap(ScoredWindow, &windows[at], &windows[child]);
        at = child;
    }
}
/// Small in-place sorts and cooperative merge passes bound cancellation delay.
/// Scratch is released before windows are built, so peak span/window storage
/// stays linear. Callers with no control keep the ordinary in-place sort.
fn controlledSort(comptime T: type, a: Allocator, values: []T, poller: *Poller, comptime less: fn (void, T, T) bool) !void {
    try poller.control.check();
    if (poller.control.ptr == null) {
        std.mem.sort(T, values, {}, less);
        return;
    }
    const block = 256;
    var begin: usize = 0;
    while (begin < values.len) : (begin += block) {
        try poller.control.check();
        std.mem.sort(T, values[begin..@min(begin + block, values.len)], {}, less);
    }
    if (values.len <= block) return;
    const scratch = try a.alloc(T, values.len);
    defer a.free(scratch);
    var input = values;
    var output = scratch;
    var width: usize = block;
    while (width < values.len) : (width = @min(values.len, width *| 2)) {
        begin = 0;
        while (begin < values.len) : (begin += @min(values.len - begin, width *| 2)) {
            var left = begin;
            const middle = begin + @min(width, values.len - begin);
            var right = middle;
            const end = middle + @min(width, values.len - middle);
            for (output[begin..end]) |*value| {
                try poller.step();
                if (left < middle and (right == end or !less({}, input[right], input[left]))) {
                    value.* = input[left];
                    left += 1;
                } else {
                    value.* = input[right];
                    right += 1;
                }
            }
        }
        std.mem.swap([]T, &input, &output);
    }
    if (input.ptr != values.ptr) @memcpy(values, input);
    try poller.control.check();
}

fn collectMatchSpans(
    alloc: Allocator,
    tokens: []const analysis_mod.Token,
    matchers: []const Matcher,
    spans: *std.ArrayListUnmanaged(Span),
    poller: *Poller,
) !void {
    return collectProgramSpans(alloc, tokens, matchers, null, spans, poller);
}
fn collectProgramSpans(alloc: Allocator, tokens: []const analysis_mod.Token, matchers: []const Matcher, program: ?*const Program, spans: *std.ArrayListUnmanaged(Span), poller: *Poller) !void {
    const active_program: ?*const Program = if (matchers.len > 8) program else null;
    for (tokens) |tok| if (try matchesToken(alloc, tok.term, matchers, active_program, poller)) {
        try spans.append(alloc, .{ .start = tok.start_byte, .end = tok.end_byte });
    };
}
inline fn matchesToken(a: Allocator, term: []const u8, matchers: []const Matcher, program: ?*const Program, poller: *Poller) !bool {
    if (program) |compiled| if (try compiled.matches(term, poller)) return true;
    for (if (program) |compiled| compiled.patterns.items else matchers) |matcher| {
        try poller.step();
        switch (matcher) {
            .term => |value| if (std.mem.eql(u8, term, value)) return true,
            .prefix => |prefix| if (std.mem.startsWith(u8, term, prefix)) return true,
            .wildcard => |pattern| if (try wildcard_mod.matchControlled(pattern, term, poller.control)) return true,
            .fuzzy => |fuzzy| {
                const prefix: usize = fuzzy.prefix_len;
                if (term.len < prefix or fuzzy.term.len < prefix or !std.mem.eql(u8, term[0..prefix], fuzzy.term[0..prefix])) continue;
                if (try boundedEditDistance(a, term, fuzzy.term, fuzzy.max_edits, poller) <= fuzzy.max_edits) return true;
            },
            .regexp => |regexp| if (try regexpMatchesTerm(regexp, term, poller)) return true,
            else => {},
        }
    }
    return false;
}
/// Compile a borrowed dictionary predicate. Every distinct term is tested once;
/// occurrence blocks already selected by this predicate need no pattern replay.
fn prepareStoredPlan(a: Allocator, source: @import("../section/source_column.zig").Text, signature: [32]u8, matchers: []const Matcher, program: ?*const Program, control: Control) !?@import("../section/source_offsets.zig").Plan {
    const Selection = @import("../section/source_column.zig").Text.Selection;
    const Probe = struct {
        a: Allocator,
        matchers: []const Matcher,
        program: ?*const Program,
        poller: Poller,
        fn check(raw: *anyopaque, term: []const u8) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return matchesToken(self.a, term, self.matchers, self.program, &self.poller);
        }
    };
    var workspace = std.heap.ArenaAllocator.init(a);
    defer workspace.deinit();
    const scratch = workspace.allocator();
    var ranges: std.ArrayList(Selection.Range) = .empty;
    var bounded = true;
    for (matchers) |matcher| switch (matcher) {
        .term => |term| try ranges.append(scratch, .{ .lower = term }),
        .prefix => |prefix| try ranges.append(scratch, .{ .lower = prefix, .prefix = true }),
        else => bounded = false,
    };
    var probe: Probe = .{ .a = scratch, .matchers = matchers, .program = if (matchers.len > 8) program else null, .poller = .{ .control = control } };
    return source.planTokens(a, signature, .{ .ptr = &probe, .matches = Probe.check, .ranges = if (bounded) ranges.items else null }, control);
}
pub const StoredMatches = union(enum) {
    vector: @import("../section/source_column.zig").Text.Vector,
    stream: @import("../section/source_offsets.zig").Plan,
    pub fn deinit(self: *StoredMatches) void {
        switch (self.*) {
            .vector => |value| value.deinit(),
            .stream => |*value| value.deinit(),
        }
    }
};
pub fn prepareStoredMatches(a: Allocator, source: @import("../section/source_column.zig").Text, signature: [32]u8, matchers: []const Matcher, program: ?*const Program, control: Control) !?StoredMatches {
    const plan = (try prepareStoredPlan(a, source, signature, matchers, program, control)) orelse return null;
    // Tiny and highly diverse matches retain the simpler vector kernel. Dense
    // monotone postings use bounded source-order merging and fragment state.
    if (plan.ordered and plan.count >= 4096 and (plan.count > plan.selectedTermCount() *| 8 or plan.source_blocks.len != 0)) return .{ .stream = plan };
    const vector = try plan.materializeOwned(control);
    return .{ .vector = vector };
}
pub fn selectStoredTokens(a: Allocator, source: @import("../section/source_column.zig").Text, signature: [32]u8, matchers: []const Matcher, program: ?*const Program, control: Control) !?@import("../section/source_column.zig").Text.Vector {
    const plan = (try prepareStoredPlan(a, source, signature, matchers, program, control)) orelse return null;
    return try plan.materializeOwned(control);
}
pub fn highlightStoredMatches(a: Allocator, scratch: Allocator, source: @import("../section/source_column.zig").Text, matches: StoredMatches, count: u32, size: u32, control: Control) ![]Fragment {
    return switch (matches) {
        .vector => |value| highlightSelectedStored(a, scratch, source, value.items, count, size, control),
        .stream => |*value| streamFragments(a, scratch, source, value, count, size, control, null),
    };
}
pub fn highlightSelectedStored(a: Allocator, scratch: Allocator, source: @import("../section/source_column.zig").Text, selected: []const analysis_mod.Token, count: u32, size: u32, control: Control) ![]Fragment {
    var arena = std.heap.ArenaAllocator.init(scratch);
    defer arena.deinit();
    var poller: Poller = .{ .control = control };
    try control.check();
    var spans: std.ArrayListUnmanaged(Span) = .empty;
    for (selected) |token| {
        try poller.step();
        try spans.append(arena.allocator(), .{ .start = token.start_byte, .end = token.end_byte });
    }
    var boundaries = @import("../section/source_column.zig").Boundaries.init(scratch, source, control);
    defer boundaries.deinit();
    return selectFragments(a, arena.allocator(), &spans, .{ .stored = &boundaries }, count, size, &poller);
}

const StreamProfile = struct { queue_peak: usize = 0, windows_peak: usize = 0, spans: usize = 0, block_reads: usize = 0, pruned: bool = false };
const SpanStream = struct {
    iterator: *@import("../section/source_offsets.zig").Iterator,
    pending: ?Span = null,
    fn next(self: *SpanStream) !?Span {
        var result = self.pending orelse if (try self.iterator.nextSpan()) |token| Span{ .start = token.start_byte, .end = token.end_byte } else return null;
        self.pending = null;
        while (try self.iterator.nextSpan()) |token| {
            const span: Span = .{ .start = token.start_byte, .end = token.end_byte };
            if (span.start > result.end) {
                self.pending = span;
                break;
            }
            result.end = @max(result.end, span.end);
        }
        return result;
    }
};
// A min-heap by desirability retains only the globally best bounded candidates.
fn siftWorst(windows: []ScoredWindow, first: usize) void {
    var at = first;
    while (at < windows.len / 2) {
        var child = at * 2 + 1;
        if (child + 1 < windows.len and betterWindow(windows[child], windows[child + 1])) child += 1;
        if (!betterWindow(windows[at], windows[child])) break;
        std.mem.swap(ScoredWindow, &windows[at], &windows[child]);
        at = child;
    }
}
/// Exact greedy fragment selection with bounded state. Every candidate is
/// determined by one integer unsnapped start. UTF-8 moves either endpoint at
/// most three bytes, so one chosen window excludes fewer than 2*size+20 unique
/// candidates. Keeping (K-1)*(2*size+20)+1 best candidates therefore preserves
/// every greedy winner, including score/start/end ties; no approximation.
fn streamFragments(a: Allocator, scratch: Allocator, stored: @import("../section/source_column.zig").Text, plan: *const @import("../section/source_offsets.zig").Plan, count: u32, size: u32, control: Control, profile: ?*StreamProfile) ![]Fragment {
    if (count == 0 or plan.count == 0 or stored.length == 0) return &.{};
    const work = @import("../sql/memory_budget.zig").reclaimingAllocator(scratch);
    var boundaries = @import("../section/source_column.zig").Boundaries.init(work, stored, control);
    defer boundaries.deinit();
    const source: TextSource = .{ .stored = &boundaries };
    var iterator = try @import("../section/source_offsets.zig").Iterator.init(work, plan, control);
    defer iterator.deinit();
    var stream: SpanStream = .{ .iterator = &iterator };
    var poller: Poller = .{ .control = control };
    const width: u32 = @max(@min(size, stored.length), 1);
    const capacity = @min(@as(u64, plan.count), (@as(u64, count) - 1) *| (2 * @as(u64, width) + 20) +| 1);
    var queue: std.ArrayList(Span) = .empty;
    defer queue.deinit(work);
    var windows: std.ArrayList(ScoredWindow) = .empty;
    defer windows.deinit(work);
    var first: usize = 0;
    var candidate: usize = 0;
    var through: usize = 0;
    var done = false;
    var previous: ?ScoredWindow = null;
    var visited: usize = 0;
    const maximum_width = @as(u64, width) + stored.boundaryExpansion();
    while (true) {
        try poller.step();
        if (candidate == queue.items.len) {
            if (try stream.next()) |span| try queue.append(work, span) else break;
        }
        const span = queue.items[candidate];
        const center = span.start + (span.end - span.start) / 2;
        var start = if (center >= width / 2) center - width / 2 else 0;
        var end = @min(start + width, stored.length);
        if (end - start < width and start > 0) start = if (end >= width) end - width else 0;
        start = try source.floor(start);
        end = try source.ceil(end);
        while (first < queue.items.len and queue.items[first].end <= start) : (first += 1) try poller.step();
        through = @max(first, through);
        while (true) {
            while (through < queue.items.len and queue.items[through].start < end) : (through += 1) try poller.step();
            if (through != queue.items.len or done) break;
            if (try stream.next()) |next| try queue.append(work, next) else done = true;
        }
        const window: ScoredWindow = .{ .start = start, .end = end, .score = @intCast(through - first) };
        if (previous == null or previous.?.start != start or previous.?.end != end) {
            if (windows.items.len < capacity) {
                try windows.append(work, window);
                var at = windows.items.len - 1;
                while (at != 0) {
                    const parent = (at - 1) / 2;
                    if (!betterWindow(windows.items[parent], windows.items[at])) break;
                    std.mem.swap(ScoredWindow, &windows.items[parent], &windows.items[at]);
                    at = parent;
                }
            } else if (betterWindow(window, windows.items[0])) {
                windows.items[0] = window;
                siftWorst(windows.items, 0);
            }
            previous = window;
        }
        if (profile) |value| {
            value.queue_peak = @max(value.queue_peak, queue.items.len);
            value.windows_peak = @max(value.windows_peak, windows.items.len);
            value.spans += 1;
        }
        visited += 1;
        // Once every unseen candidate is bounded by the retained worst entry,
        // monotone start/end ties prove that no later window can improve it.
        // This skips the remaining occurrence blocks, without approximation.
        if (visited % 256 == 0 and windows.items.len == capacity) {
            const worst = windows.items[0];
            if (plan.densityUpper(maximum_width, start, worst.score, stored.boundaryExpansion() == 0)) |upper| {
                if (!betterWindow(.{ .start = start, .end = end, .score = upper }, worst)) {
                    if (profile) |value| value.pruned = true;
                    break;
                }
            }
        }
        candidate += 1;
        const discard = @min(first, candidate);
        if (discard >= 512) {
            std.mem.copyForwards(Span, queue.items, queue.items[discard..]);
            queue.items.len -= discard;
            first -= discard;
            candidate -= discard;
            through -= discard;
        }
    }
    // The bounded heap is now ranked with the identical greedy overlap rule.
    var parent = windows.items.len / 2;
    while (parent != 0) {
        parent -= 1;
        try siftWindows(windows.items, windows.items.len, parent, &poller);
    }
    var selected: std.ArrayList(ScoredWindow) = .empty;
    defer selected.deinit(work);
    var heap_len = windows.items.len;
    while (heap_len != 0 and selected.items.len < count) {
        try poller.step();
        const window = windows.items[0];
        heap_len -= 1;
        if (heap_len != 0) {
            windows.items[0] = windows.items[heap_len];
            try siftWindows(windows.items, heap_len, 0, &poller);
        }
        const overlaps = for (selected.items) |chosen| {
            if (window.start < chosen.end and window.end > chosen.start) break true;
        } else false;
        if (!overlaps) try selected.append(work, window);
    }
    try controlledSort(ScoredWindow, work, selected.items, &poller, struct {
        fn less(_: void, l: ScoredWindow, r: ScoredWindow) bool {
            return l.start < r.start;
        }
    }.less);
    // Seek independently to each output interval. Prefix-max end offsets
    // preserve long overlapping spans; clipped output never needs earlier
    // expired blocks or payloads in gaps between selected windows.
    const fragments = try a.alloc(Fragment, selected.items.len);
    var initialized: usize = 0;
    errdefer {
        for (fragments[0..initialized]) |fragment| {
            a.free(fragment.highlights);
            if (fragment.owned_text) a.free(fragment.text);
        }
        a.free(fragments);
    }
    var retained: std.ArrayList(Span) = .empty;
    defer retained.deinit(work);
    for (selected.items) |window| {
        try iterator.seek(window.start);
        stream = .{ .iterator = &iterator };
        retained.clearRetainingCapacity();
        while (try stream.next()) |span| {
            try poller.step();
            if (span.start >= window.end) break;
            if (span.end > window.start) try retained.append(work, span);
        }
        const one = try renderSelectedFragments(a, work, retained.items, source, &.{window}, &poller);
        fragments[initialized] = one[0];
        a.free(one);
        initialized += 1;
    }
    if (profile) |value| value.block_reads = iterator.blockReads();
    return fragments;
}

// Dictionary regexp queries feed the whole term to the automaton. The
// regex library's text-search helper intentionally permits partial matches.
fn regexpMatchesTerm(regexp: Matcher.Regexp, term: []const u8, poller: *Poller) !bool {
    const aut = regexp.compiled.automaton();
    var state = aut.start();
    for (term) |byte| {
        try poller.step();
        if (!aut.canMatch(state)) return false;
        state = aut.accept(state, byte);
    }
    return aut.isMatch(state);
}

/// Replay the substring companion's one-word and adjacent-word suffixes,
/// mapping positions through the source separator instead of treating a
/// joined shingle's byte offsets as contiguous source bytes.
fn collectSubstringPatternSpans(
    alloc: Allocator,
    words: []const analysis_mod.Token,
    matchers: []const Matcher,
    spans: *std.ArrayListUnmanaged(Span),
    poller: *Poller,
) !void {
    for (words, 0..) |first, index| {
        try poller.step();
        try collectSubstringPatternTermSpans(alloc, first, null, first.term, matchers, spans, poller);
        if (index + 1 < words.len) {
            const second = words[index + 1];
            const joined = try std.mem.concat(alloc, u8, &.{ first.term, second.term });
            defer alloc.free(joined);
            try collectSubstringPatternTermSpans(alloc, first, second, joined, matchers, spans, poller);
        }
    }
}

fn collectSubstringPatternTermSpans(
    alloc: Allocator,
    first: analysis_mod.Token,
    second: ?analysis_mod.Token,
    term: []const u8,
    matchers: []const Matcher,
    spans: *std.ArrayListUnmanaged(Span),
    poller: *Poller,
) !void {
    var start: usize = 0;
    while (start < term.len) : (start += 1) {
        try poller.step();
        if ((term[start] & 0xC0) == 0x80) continue;
        const suffix = term[start..];
        if (suffix.len < analysis_mod.substring_min_query_length) break;
        var length = @min(suffix.len, analysis_mod.substring_max_query_length);
        while (length > 0 and length < suffix.len and (suffix[length] & 0xC0) == 0x80) length -= 1;
        if (length < analysis_mod.substring_min_query_length) continue;
        const indexed_term = suffix[0..length];

        for (matchers) |matcher| {
            try poller.step();
            const matches = switch (matcher) {
                .substring_term => |needle| std.mem.eql(u8, needle, indexed_term),
                .substring_wildcard => |pattern| try wildcard_mod.matchControlled(pattern, indexed_term, poller.control),
                .substring_regexp => |regexp| try regexpMatchesTerm(regexp, indexed_term, poller),
                .substring_fuzzy => |fuzzy| blk: {
                    const prefix_len: usize = fuzzy.prefix_len;
                    if (indexed_term.len < prefix_len or fuzzy.term.len < prefix_len or
                        !std.mem.eql(u8, indexed_term[0..prefix_len], fuzzy.term[0..prefix_len])) break :blk false;
                    break :blk try boundedEditDistance(alloc, indexed_term, fuzzy.term, fuzzy.max_edits, poller) <= fuzzy.max_edits;
                },
                else => false,
            };
            if (!matches) continue;
            const span_start = if (second) |next|
                if (start >= first.term.len) next.start_byte + @as(u32, @intCast(start - first.term.len)) else first.start_byte + @as(u32, @intCast(start))
            else
                first.start_byte + @as(u32, @intCast(start));
            const term_end = start + length;
            const span_end = if (second) |next|
                if (term_end > first.term.len) next.start_byte + @as(u32, @intCast(term_end - first.term.len)) else first.start_byte + @as(u32, @intCast(term_end))
            else
                first.start_byte + @as(u32, @intCast(term_end));
            try spans.append(alloc, .{ .start = span_start, .end = span_end });
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
    poller: *Poller,
) !void {
    for (words, 0..) |word, i| {
        try poller.step();
        const surface = text[word.start_byte..word.end_byte];
        for (matchers) |matcher| {
            try poller.step();
            const needle = switch (matcher) {
                .contains => |needle| needle,
                else => continue,
            };
            if (needle.len == 0) continue;
            var search_at: usize = 0;
            while (search_at + needle.len <= surface.len) {
                try poller.step();
                const relative = try indexOfIgnoreCase(surface[search_at..], needle, poller) orelse break;
                const index = search_at + relative;
                try spans.append(alloc, .{
                    .start = word.start_byte + @as(u32, @intCast(index)),
                    .end = word.start_byte + @as(u32, @intCast(index + needle.len)),
                });
                search_at = index + 1;
            }
            // The companion joins adjacent words without a separator, so a
            // match may start inside this word and end inside the next one.
            if (i + 1 >= words.len) continue;
            const next = words[i + 1];
            const next_surface = text[next.start_byte..next.end_byte];
            if (surface.len + next_surface.len < needle.len) continue;
            const joined = try alloc.alloc(u8, surface.len + next_surface.len);
            defer alloc.free(joined);
            @memcpy(joined[0..surface.len], surface);
            @memcpy(joined[surface.len..], next_surface);
            search_at = 0;
            while (search_at + needle.len <= joined.len) {
                try poller.step();
                const relative = try indexOfIgnoreCase(joined[search_at..], needle, poller) orelse break;
                const joined_index = search_at + relative;
                if (joined_index < surface.len and joined_index + needle.len > surface.len) {
                    try spans.append(alloc, .{
                        .start = word.start_byte + @as(u32, @intCast(joined_index)),
                        .end = next.start_byte + @as(u32, @intCast(joined_index + needle.len - surface.len)),
                    });
                }
                search_at = joined_index + 1;
            }
        }
    }
}

fn indexOfIgnoreCase(haystack: []const u8, needle: []const u8, poller: *Poller) !?usize {
    if (needle.len == 0 or needle.len > haystack.len) return null;
    var i: usize = 0;
    while (i + needle.len <= haystack.len) : (i += 1) {
        try poller.step();
        if (std.ascii.eqlIgnoreCase(haystack[i..][0..needle.len], needle)) return i;
    }
    return null;
}

/// Merge overlapping or touching spans and sort them by start offset.
fn normalizeSpans(allocator: Allocator, spans: *std.ArrayListUnmanaged(Span), poller: *Poller) !void {
    try controlledSort(Span, allocator, spans.items, poller, struct {
        fn cmp(_: void, a: Span, b: Span) bool {
            if (a.start != b.start) return a.start < b.start;
            return a.end > b.end;
        }
    }.cmp);
    var write: usize = 0;
    for (spans.items) |span| {
        try poller.step();
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
fn boundedEditDistance(alloc: Allocator, a: []const u8, b: []const u8, limit: u8, poller: *Poller) !u32 {
    const cap: u32 = @as(u32, limit) + 1;
    if (std.mem.eql(u8, a, b)) return 0;
    if (a.len > b.len and a.len - b.len > cap) return cap;
    if (b.len > a.len and b.len - a.len > cap) return cap;
    var stack_prev: [65]u32 = undefined;
    var stack_curr: [65]u32 = undefined;
    const heap_prev = if (b.len > 64) try alloc.alloc(u32, b.len + 1) else null;
    defer if (heap_prev) |items| alloc.free(items);
    const heap_curr = if (b.len > 64) try alloc.alloc(u32, b.len + 1) else null;
    defer if (heap_curr) |items| alloc.free(items);
    var prev = if (heap_prev) |items| items else stack_prev[0..];
    var curr = if (heap_curr) |items| items else stack_curr[0..];
    for (0..b.len + 1) |j| prev[j] = @intCast(j);
    for (a, 1..) |ca, i| {
        try poller.step();
        curr[0] = @intCast(i);
        var row_min: u32 = curr[0];
        for (b, 1..) |cb, j| {
            try poller.step();
            const cost: u32 = if (ca == cb) 0 else 1;
            curr[j] = @min(@min(prev[j] + 1, curr[j - 1] + 1), prev[j - 1] + cost);
            row_min = @min(row_min, curr[j]);
        }
        if (row_min > limit) return cap;
        std.mem.swap([]u32, &prev, &curr);
    }
    return prev[b.len];
}

/// Free spans and owned stored-page snippets. Borrowed source text remains
/// owned by the caller.
pub fn freeFragments(alloc: Allocator, fragments: []Fragment) void {
    for (fragments) |f| {
        alloc.free(f.highlights);
        if (f.owned_text) alloc.free(f.text);
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

test "keyword matchers highlight a whole value across three words" {
    const alloc = std.testing.allocator;
    const text = "New York City";
    const exact = try highlightMatchers(alloc, text, &.{.{ .literal = text }}, &analysis_mod.simple_analyzer, 1, 100);
    defer freeFragments(alloc, exact);
    try std.testing.expectEqual(@as(usize, 1), exact.len);
    try std.testing.expectEqual(@as(usize, 1), exact[0].highlights.len);
    try std.testing.expectEqualStrings(text, exact[0].text[exact[0].highlights[0].start..exact[0].highlights[0].end]);

    const prefix = try highlightMatchers(alloc, text, &.{.{ .literal_prefix = "New York" }}, &analysis_mod.simple_analyzer, 1, 100);
    defer freeFragments(alloc, prefix);
    try std.testing.expectEqualStrings("New York", prefix[0].text[prefix[0].highlights[0].start..prefix[0].highlights[0].end]);

    const wrong_case = try highlightMatchers(alloc, text, &.{.{ .literal = "new york city" }}, &analysis_mod.simple_analyzer, 1, 100);
    try std.testing.expectEqual(@as(usize, 0), wrong_case.len);
    const substring = try highlightMatchers(alloc, text, &.{.{ .literal = "York" }}, &analysis_mod.simple_analyzer, 1, 100);
    try std.testing.expectEqual(@as(usize, 0), substring.len);
}

test "highlight clips a match wider than its fragment" {
    const alloc = std.testing.allocator;
    const text = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";
    const fragments = try highlightMatchers(alloc, text, &.{.{ .literal = text }}, &analysis_mod.keyword_analyzer, 1, 32);
    defer freeFragments(alloc, fragments);
    try std.testing.expectEqual(@as(usize, 1), fragments.len);
    try std.testing.expectEqual(@as(usize, 32), fragments[0].text.len);
    try std.testing.expectEqual(@as(usize, 1), fragments[0].highlights.len);
    try std.testing.expectEqual(@as(u32, 0), fragments[0].highlights[0].start);
    try std.testing.expectEqual(@as(u32, 32), fragments[0].highlights[0].end);
}

test "highlight offsets follow stored text through character filters" {
    const alloc = std.testing.allocator;
    const html = try highlightMatchers(alloc, "<p>hello</p>", &.{.{ .term = "hello" }}, &analysis_mod.html_analyzer, 1, 100);
    defer freeFragments(alloc, html);
    try std.testing.expectEqualStrings("hello", html[0].text[html[0].highlights[0].start..html[0].highlights[0].end]);

    const folded_analyzer = analysis_mod.Analyzer{
        .char_filters = &.{.ascii_fold},
        .tokenizer = .unicode_words,
        .filters = &.{.lowercase},
    };
    const folded = try highlightMatchers(alloc, "café", &.{.{ .term = "cafe" }}, &folded_analyzer, 1, 100);
    defer freeFragments(alloc, folded);
    try std.testing.expectEqualStrings("café", folded[0].text[folded[0].highlights[0].start..folded[0].highlights[0].end]);

    const joined_analyzer = analysis_mod.Analyzer{
        .char_filters = &.{.zero_width_non_joiner},
        .tokenizer = .unicode_words,
        .filters = &.{.lowercase},
    };
    const joined = try highlightMatchers(alloc, "he\u{200C}llo", &.{.{ .term = "hello" }}, &joined_analyzer, 1, 100);
    defer freeFragments(alloc, joined);
    try std.testing.expectEqualStrings("he\u{200C}llo", joined[0].text[joined[0].highlights[0].start..joined[0].highlights[0].end]);

    const combined_analyzer = analysis_mod.Analyzer{
        .char_filters = &.{ .html_strip, .ascii_fold },
        .tokenizer = .unicode_words,
        .filters = &.{.lowercase},
    };
    const combined = try highlightMatchers(alloc, "<p>caf&#233;</p>", &.{.{ .term = "cafe" }}, &combined_analyzer, 1, 100);
    defer freeFragments(alloc, combined);
    try std.testing.expectEqualStrings("caf&#233;", combined[0].text[combined[0].highlights[0].start..combined[0].highlights[0].end]);
}

test "substring highlights include repeated and crossing occurrences" {
    const alloc = std.testing.allocator;
    const repeated = try highlightMatchers(alloc, "bananana", &.{.{ .contains = "ana" }}, &analysis_mod.simple_analyzer, 1, 100);
    defer freeFragments(alloc, repeated);
    try std.testing.expectEqualStrings("ananana", repeated[0].text[repeated[0].highlights[0].start..repeated[0].highlights[0].end]);

    const crossing = try highlightMatchers(alloc, "foobarfoo bar", &.{.{ .contains = "foobar" }}, &analysis_mod.simple_analyzer, 1, 100);
    defer freeFragments(alloc, crossing);
    try std.testing.expectEqualStrings("foobarfoo bar", crossing[0].text[crossing[0].highlights[0].start..crossing[0].highlights[0].end]);
}

test "fuzzy highlights respect prefixes and long tokens" {
    const alloc = std.testing.allocator;
    const prefixed = try highlightMatchers(alloc, "cat bat", &.{.{ .fuzzy = .{ .term = "cat", .max_edits = 1, .prefix_len = 1 } }}, &analysis_mod.simple_analyzer, 1, 100);
    defer freeFragments(alloc, prefixed);
    try std.testing.expectEqual(@as(usize, 1), prefixed.len);
    try std.testing.expectEqual(@as(usize, 1), prefixed[0].highlights.len);
    try std.testing.expectEqualStrings("cat", prefixed[0].text[prefixed[0].highlights[0].start..prefixed[0].highlights[0].end]);

    const query = @as([65]u8, @splat('a'));
    var source = query;
    source[64] = 'b';
    const long = try highlightMatchers(alloc, &source, &.{.{ .fuzzy = .{ .term = &query, .max_edits = 1 } }}, &analysis_mod.simple_analyzer, 1, 100);
    defer freeFragments(alloc, long);
    try std.testing.expectEqual(@as(usize, 1), long.len);
    try std.testing.expectEqual(@as(usize, 1), long[0].highlights.len);
    try std.testing.expectEqual(@as(u32, 0), long[0].highlights[0].start);
    try std.testing.expectEqual(@as(u32, 65), long[0].highlights[0].end);
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

test "substring pattern matchers map joined suffixes to source bytes" {
    const alloc = std.testing.allocator;
    const text = "Rag3-Weaver";
    var compiled = try regex_mod.compile(alloc, "^g3we.*$");
    defer compiled.deinit();
    const matchers = [_]Matcher{
        .{ .substring_wildcard = "g3we*" },
        .{ .substring_regexp = .{ .pattern = "^g3we.*$", .compiled = &compiled } },
        .{ .substring_fuzzy = .{ .term = "g3weaver", .max_edits = 0 } },
    };
    for (matchers) |matcher| {
        const fragments = try highlightMatchers(alloc, text, &.{matcher}, &analysis_mod.default_analyzer, 1, 100);
        defer freeFragments(alloc, fragments);
        try std.testing.expectEqual(@as(usize, 1), fragments.len);
        try std.testing.expectEqualStrings("g3-Weaver", fragments[0].text[fragments[0].highlights[0].start..fragments[0].highlights[0].end]);
    }
    const second_word = try highlightMatchers(alloc, text, &.{.{ .substring_wildcard = "we*" }}, &analysis_mod.default_analyzer, 1, 100);
    defer freeFragments(alloc, second_word);
    try std.testing.expectEqualStrings("Weaver", second_word[0].text[second_word[0].highlights[0].start..second_word[0].highlights[0].end]);
}

test "regexp highlights use complete dictionary terms" {
    const alloc = std.testing.allocator;
    var compiled = try regex_mod.compile(alloc, "cat");
    defer compiled.deinit();
    const fragments = try highlightMatchers(alloc, "cat bobcat cats", &.{.{ .regexp = .{ .pattern = "cat", .compiled = &compiled } }}, &analysis_mod.simple_analyzer, 1, 100);
    defer freeFragments(alloc, fragments);
    try std.testing.expectEqual(@as(usize, 1), fragments[0].highlights.len);
    try std.testing.expectEqualStrings("cat", fragments[0].text[fragments[0].highlights[0].start..fragments[0].highlights[0].end]);

    var suffix_regex = try regex_mod.compile(alloc, "g3we.*");
    defer suffix_regex.deinit();
    const suffix_fragments = try highlightMatchers(alloc, "Rag3-Weaver", &.{.{ .substring_regexp = .{ .pattern = "g3we.*", .compiled = &suffix_regex } }}, &analysis_mod.simple_analyzer, 1, 100);
    defer freeFragments(alloc, suffix_fragments);
    try std.testing.expectEqualStrings("g3-Weaver", suffix_fragments[0].text[suffix_fragments[0].highlights[0].start..suffix_fragments[0].highlights[0].end]);
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

test "highlight custom shingle suffix offsets preserve source separators and character filters" {
    const alloc = std.testing.allocator;
    for ([_][]const u8{ "quiet   river", "quiet---river", "quiet\nriver", "quiet café" }) |text| {
        const analyzer: analysis_mod.Analyzer = .{ .tokenizer = .unicode_words, .filters = &.{
            .lowercase, .{ .shingle = .{ .min = 2, .max = 2, .separator = .none } }, .{ .suffix = .{} },
        } };
        const needle = if (std.mem.endsWith(u8, text, "café")) "café" else "river";
        const fragments = try highlightMatchers(alloc, text, &.{.{ .term = needle }}, &analyzer, 1, 64);
        defer freeFragments(alloc, fragments);
        try std.testing.expectEqual(@as(usize, 1), fragments.len);
        try std.testing.expectEqualStrings(needle, fragments[0].text[fragments[0].highlights[0].start..fragments[0].highlights[0].end]);
        // Terms and positions must stay identical to index analysis.
        const indexed = try analyzer.analyze(alloc, text);
        defer analysis_mod.Analyzer.freeTokens(alloc, indexed);
        const mapped = try analyzer.analyzeWithSourceOffsets(alloc, text);
        defer analysis_mod.Analyzer.freeTokens(alloc, mapped);
        try std.testing.expectEqual(indexed.len, mapped.len);
        for (indexed, mapped) |left, right| {
            try std.testing.expectEqualStrings(left.term, right.term);
            try std.testing.expectEqual(left.position, right.position);
            try std.testing.expect(right.source_offsets == null);
        }
    }
    const html: analysis_mod.Analyzer = .{ .char_filters = &.{.html_strip}, .tokenizer = .unicode_words, .filters = &.{
        .lowercase, .{ .shingle = .{ .min = 2, .max = 2, .separator = .none } }, .{ .suffix = .{} },
    } };
    const fragments = try highlightMatchers(alloc, "quiet <b>River</b>", &.{.{ .term = "river" }}, &html, 1, 64);
    defer freeFragments(alloc, fragments);
    try std.testing.expectEqualStrings("River", fragments[0].text[fragments[0].highlights[0].start..fragments[0].highlights[0].end]);
}

test "highlight custom suffix chains preserve capped ends and reversed byte mappings" {
    const alloc = std.testing.allocator;
    const capped: analysis_mod.Analyzer = .{ .tokenizer = .unicode_words, .filters = &.{
        .{ .shingle = .{ .min = 2, .max = 2, .separator = .none } }, .{ .suffix = .{ .min = 2, .max = 2 } }, .{ .suffix = .{ .min = 2, .max = 2 } },
    } };
    const fragments = try highlightMatchers(alloc, "quiet   river", &.{.{ .term = "ri" }}, &capped, 1, 64);
    defer freeFragments(alloc, fragments);
    try std.testing.expectEqualStrings("ri", fragments[0].text[fragments[0].highlights[0].start..fragments[0].highlights[0].end]);
    const reversed: analysis_mod.Analyzer = .{ .tokenizer = .unicode_words, .filters = &.{
        .{ .shingle = .{ .min = 2, .max = 2, .separator = .none } }, .reverse, .{ .suffix = .{} },
    } };
    const reverse_fragments = try highlightMatchers(alloc, "quiet   river", &.{.{ .term = "teiuq" }}, &reversed, 1, 64);
    defer freeFragments(alloc, reverse_fragments);
    try std.testing.expectEqualStrings("quiet", reverse_fragments[0].text[reverse_fragments[0].highlights[0].start..reverse_fragments[0].highlights[0].end]);
}

test "highlight suffixes map through camel case elision apostrophe grams truncation and stemming" {
    const alloc = std.testing.allocator;
    const Case = struct {
        text: []const u8,
        term: []const u8,
        expected: []const u8,
        tokenizer: analysis_mod.Tokenizer = .unicode_words,
        filters: []const analysis_mod.TokenFilter,
    };
    for ([_]Case{
        .{ .text = "quiet   River", .term = "river", .expected = "River", .filters = &.{ .{ .shingle = .{ .min = 2, .max = 2, .separator = .none } }, .camel_case, .{ .suffix = .{} } } },
        .{ .text = "l'river", .term = "iver", .expected = "iver", .tokenizer = .keyword, .filters = &.{ .elision, .{ .suffix = .{} } } },
        .{ .text = "river's", .term = "iver", .expected = "iver", .tokenizer = .keyword, .filters = &.{ .apostrophe, .{ .suffix = .{} } } },
        .{ .text = "quiet---river", .term = "ri", .expected = "ri", .filters = &.{ .{ .ngram = .{ .min = 3, .max = 3 } }, .{ .suffix = .{ .min = 2, .max = 2 } } } },
        .{ .text = "quiet---river", .term = "er", .expected = "er", .filters = &.{ .{ .edge_ngram = .{ .min = 3, .max = 3, .side = .back } }, .{ .suffix = .{ .min = 2, .max = 2 } } } },
        .{ .text = "river", .term = "ri", .expected = "ri", .filters = &.{ .{ .truncate = .{ .max_len = 4 } }, .{ .suffix = .{ .min = 2, .max = 2 } } } },
        // A replacement stem has no exact per-byte surface alignment; its
        // slices retain the original surface token instead of guessing.
        .{ .text = "running", .term = "un", .expected = "running", .filters = &.{ .stemmer, .{ .suffix = .{} } } },
    }) |case| {
        const analyzer: analysis_mod.Analyzer = .{ .tokenizer = case.tokenizer, .filters = case.filters };
        const fragments = try highlightMatchers(alloc, case.text, &.{.{ .term = case.term }}, &analyzer, 1, 64);
        defer freeFragments(alloc, fragments);
        try std.testing.expectEqual(@as(usize, 1), fragments.len);
        try std.testing.expectEqualStrings(case.expected, fragments[0].text[fragments[0].highlights[0].start..fragments[0].highlights[0].end]);
    }
}

// Independent quadratic selection oracle retained only for small tests.
fn referenceHighlightMatchers(
    alloc: Allocator,
    text: []const u8,
    matchers: []const Matcher,
    analyzer: *const analysis_mod.Analyzer,
    max_fragments: u32,
    fragment_size: u32,
) ![]Fragment {
    var poller: Poller = .{ .control = .none };
    if (text.len == 0 or matchers.len == 0 or max_fragments == 0) return &.{};

    const tokens = try analyzer.analyzeWithSourceOffsets(alloc, text);
    defer analysis_mod.Analyzer.freeTokens(alloc, tokens);

    var spans = std.ArrayListUnmanaged(Span).empty;
    defer spans.deinit(alloc);
    try collectMatchSpans(alloc, tokens, matchers, &spans, &poller);

    for (matchers) |matcher| {
        switch (matcher) {
            .literal => |needle| {
                if (needle.len > 0 and std.mem.eql(u8, text, needle)) {
                    try spans.append(alloc, .{ .start = 0, .end = @intCast(text.len) });
                }
            },
            .literal_prefix => |prefix| {
                if (prefix.len > 0 and std.mem.startsWith(u8, text, prefix)) {
                    try spans.append(alloc, .{ .start = 0, .end = @intCast(prefix.len) });
                }
            },
            else => {},
        }
    }

    // `contains` matchers come from substring companions, which index every
    // surface word and every adjacent word pair regardless of the root
    // field's stop words or stemming. Evaluate them over the plain surface
    // words so a span can never bridge a word the companion never joined.
    var has_contains = false;
    var has_substring_pattern = false;
    for (matchers) |matcher| {
        has_contains = has_contains or matcher == .contains;
        has_substring_pattern = has_substring_pattern or matcher == .substring_term or matcher == .substring_wildcard or
            matcher == .substring_fuzzy or matcher == .substring_regexp;
    }
    if (has_contains or has_substring_pattern) {
        const words = try analysis_mod.substring_query_analyzer.analyze(alloc, text);
        defer analysis_mod.Analyzer.freeTokens(alloc, words);
        if (has_contains) try collectContainsSpans(alloc, text, words, matchers, &spans, &poller);
        if (has_substring_pattern) try collectSubstringPatternSpans(alloc, words, matchers, &spans, &poller);
    }
    if (spans.items.len == 0) return &.{};
    try normalizeSpans(alloc, &spans, &poller);

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
            if (other.start < win_end and other.end > win_start) match_count += 1;
        }
        try windows.append(alloc, .{ .start = win_start, .end = win_end, .score = match_count });
    }

    std.mem.sort(ScoredWindow, windows.items, {}, struct {
        fn cmp(_: void, a: ScoredWindow, b: ScoredWindow) bool {
            if (a.score != b.score) return a.score > b.score;
            if (a.start != b.start) return a.start < b.start;
            return a.end < b.end;
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
            if (span.start < win.end and span.end > win.start) {
                try fragment_spans.append(alloc, .{
                    .start = @max(span.start, win.start) - win.start,
                    .end = @min(span.end, win.end) - win.start,
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

test "highlight density sweep and heap preserve reference density ranking with deterministic UTF8 ties and clipping" {
    const a = std.testing.allocator;
    var random = std.Random.DefaultPrng.init(731);
    const pieces = [_][]const u8{ "a ", "b ", "é ", "😀 ", "needle ", "none ", "éé", "x" };
    for (0..40) |_| {
        var text: std.ArrayList(u8) = .empty;
        defer text.deinit(a);
        for (0..80) |_| try text.appendSlice(a, pieces[random.random().uintLessThan(usize, pieces.len)]);
        for ([_]u32{ 1, 2, 7, 16, 33, 1000 }) |width| for ([_]u32{ 1, 3, 8 }) |limit| {
            const matchers = [_]Matcher{ .{ .term = "a" }, .{ .term = "b" }, .{ .term = "é" }, .{ .term = "needle" } };
            const expected = try referenceHighlightMatchers(a, text.items, &matchers, &analysis_mod.simple_analyzer, limit, width);
            defer freeFragments(a, expected);
            const actual = try highlightMatchers(a, text.items, &matchers, &analysis_mod.simple_analyzer, limit, width);
            defer freeFragments(a, actual);
            try std.testing.expectEqual(expected.len, actual.len);
            for (expected, actual) |left, right| {
                try std.testing.expectEqual(left.offset, right.offset);
                try std.testing.expectEqualStrings(left.text, right.text);
                try std.testing.expectEqualDeep(left.highlights, right.highlights);
            }
        };
    }
}

test "highlight dense windows advance density frontiers linearly and unwind cancellation" {
    const a = std.testing.allocator;
    var spans: [4096]Span = undefined;
    for (&spans, 0..) |*span, i| span.* = .{ .start = @intCast(i * 7), .end = @intCast(i * 7 + 6) };
    var poller: Poller = .{ .control = .none };
    var sweep: DensitySweep = .{};
    for (spans) |span| {
        const start = span.start -| 50;
        const end = start + 100;
        const expected = forCount: {
            var count: u32 = 0;
            for (spans) |other| if (other.start < end and other.end > start) {
                count += 1;
            };
            break :forCount count;
        };
        try std.testing.expectEqual(expected, try sweep.count(&spans, start, end, &poller));
    }
    try std.testing.expect(sweep.first + sweep.end <= spans.len * 2);
    const Check = struct {
        calls: usize = 0,
        limit: usize = std.math.maxInt(usize),
        fn checkpoint(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.calls += 1;
            if (self.calls == self.limit) return error.DeadlineExceeded;
        }
        fn token(self: *@This()) Control {
            return .{ .ptr = self, .check_fn = checkpoint };
        }
    };
    var text: [2100]u8 = undefined;
    for (0..300) |i| @memcpy(text[i * 7 ..][0..7], "needle ");
    var check: Check = .{};
    const result = try highlightMatchersControlled(a, &text, &.{.{ .term = "needle" }}, &analysis_mod.simple_analyzer, 3, 100, check.token());
    freeFragments(a, result);
    const calls = check.calls;
    for (1..calls + 1) |limit| {
        check = .{ .limit = limit };
        try std.testing.expectError(error.DeadlineExceeded, highlightMatchersControlled(a, &text, &.{.{ .term = "needle" }}, &analysis_mod.simple_analyzer, 3, 100, check.token()));
    }
    const Faults = struct {
        fn run(alloc: Allocator) !void {
            var control: Check = .{};
            const fragments = try highlightMatchersControlled(alloc, "a needle here, needle there, é needle", &.{.{ .term = "needle" }}, &analysis_mod.simple_analyzer, 3, 7, control.token());
            defer freeFragments(alloc, fragments);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Faults.run, .{});
    std.debug.print("HIGHLIGHT_DENSITY spans={d} frontier_advances={d} cancellation_points={d}\n", .{ spans.len, sweep.first + sweep.end, calls });
}

test "highlight prepared programs share analysis and preserve per-query fragment selection" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const text = "needle cats running needles cat runner needle";
    const first = [_]Matcher{ .{ .term = "needle" }, .{ .prefix = "run" }, .{ .prefix = "r" }, .{ .prefix = "runnz" }, .{ .term = "absent1" }, .{ .term = "absent2" }, .{ .term = "absent3" }, .{ .term = "absent4" }, .{ .term = "absent5" } };
    const second = [_]Matcher{ .{ .term = "cat" }, .{ .wildcard = "r*n" } };
    var cache: AnalysisCache = .{ .a = arena.allocator() };
    for ([_][]const Matcher{ &first, &second }) |matchers| {
        var program = try Program.init(a, matchers);
        defer program.deinit(a);
        const expected = try highlightMatchers(a, text, matchers, &analysis_mod.default_analyzer, 2, 17);
        defer freeFragments(a, expected);
        const actual = try highlightPrepared(a, arena.allocator(), text, matchers, &program, &cache, &analysis_mod.default_analyzer, 2, 17, .none);
        defer freeFragments(a, actual);
        try std.testing.expectEqualDeep(expected, actual);
    }
    try std.testing.expectEqual(@as(usize, 1), cache.analyses);
}

test "highlight byte scans cancel long punctuation tokens HTML and wildcard input with precise deadlines" {
    const a = std.testing.allocator;
    const State = struct {
        calls: usize = 0,
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.calls += 1;
            if (self.calls == 8) return error.DeadlineExceeded;
        }
    };
    const text = try a.alloc(u8, 256 * 1024);
    defer a.free(text);
    const html: analysis_mod.Analyzer = .{ .char_filters = &.{.html_strip}, .tokenizer = .unicode_words, .filters = &.{.lowercase} };
    for ([_]u8{ '.', 'x', 'X', '<' }) |byte| {
        @memset(text, byte);
        for ([_]*const analysis_mod.Analyzer{ &analysis_mod.default_analyzer, &html }) |analyzer| {
            var state: State = .{};
            try std.testing.expectError(error.DeadlineExceeded, highlightMatchersControlled(a, text, &.{.{ .term = "needle" }}, analyzer, 1, 32, .{ .ptr = &state, .check_fn = State.check }));
            try std.testing.expectEqual(@as(usize, 8), state.calls);
        }
    }
    @memset(text, 'x');
    var state: State = .{};
    try std.testing.expectError(error.DeadlineExceeded, wildcard_mod.matchControlled("*needle", text, .{ .ptr = &state, .check_fn = State.check }));
}

test "highlight stored original offsets read only winning string pages and preserve Unicode under allocation faults" {
    const a = std.testing.allocator;
    const column = @import("../section/source_column.zig");
    const segment = @import("../segment.zig");
    const source_mod = @import("../segment_source.zig");
    const text = try a.alloc(u8, column.page_size * 4);
    defer a.free(text);
    @memset(text, '.');
    const at = column.page_size - 4;
    @memcpy(text[at..][0.."é needle 😀".len], "é needle 😀");
    var memory = segment.MemorySegmentSink.init(a);
    defer memory.deinit();
    var sink = memory.sink();
    var writer = try column.Writer.init(a, &sink);
    defer writer.deinit();
    try writer.add(0, .{ .string = text }, &.{&analysis_mod.simple_analyzer});
    try std.testing.expect(try writer.finish());
    const Reads = struct {
        bytes: []const u8,
        read_bytes: usize = 0,
        fn close(_: *anyopaque) void {}
        fn read(raw: *anyopaque, offset: u64, into: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.read_bytes += into.len;
            @memcpy(into, self.bytes[@intCast(offset)..][0..into.len]);
        }
    };
    var reads: Reads = .{ .bytes = memory.out.items };
    const view = try source_mod.View.init(.{ .ranges = .{ .ptr = &reads, .length = reads.bytes.len, .read_into = Reads.read, .close = Reads.close } }, 0, reads.bytes.len);
    const Run = struct {
        fn run(alloc: Allocator, source: source_mod.View, original: []const u8) !void {
            var reader = try column.Reader.init(alloc, source);
            var cursor = column.Cursor.init(reader);
            // Reader ownership transfers to cursor.
            _ = &reader;
            defer cursor.deinit();
            const stored = (try cursor.text(0)).?;
            const signature = try column.fingerprint(alloc, &analysis_mod.simple_analyzer);
            const tokens = (try stored.tokens(alloc, signature, .none)).?;
            defer tokens.deinit();
            try std.testing.expect((try stored.tokens(alloc, @splat(0), .none)) == null);
            const matchers = [_]Matcher{ .{ .term = "needle" }, .{ .prefix = "nee" }, .{ .wildcard = "n*dle" }, .{ .fuzzy = .{ .term = "needlf", .max_edits = 1 } } };
            const expected = try highlightMatchers(alloc, original, &matchers, &analysis_mod.simple_analyzer, 1, 23);
            defer freeFragments(alloc, expected);
            const found = (try highlightStored(alloc, alloc, stored, tokens.items, &matchers, null, 1, 23, .none)).?;
            defer freeFragments(alloc, found);
            const selected = (try selectStoredTokens(alloc, stored, signature, &matchers, null, .none)).?;
            defer selected.deinit();
            const selective = try highlightSelectedStored(alloc, alloc, stored, selected.items, 1, 23, .none);
            defer freeFragments(alloc, selective);
            try std.testing.expectEqual(expected.len, selective.len);
            for (expected, selective) |left, right| {
                try std.testing.expectEqual(left.offset, right.offset);
                try std.testing.expectEqualStrings(left.text, right.text);
                try std.testing.expectEqualDeep(left.highlights, right.highlights);
            }
            try std.testing.expectEqual(expected.len, found.len);
            for (expected, found) |left, right| {
                try std.testing.expectEqual(left.offset, right.offset);
                try std.testing.expectEqualStrings(left.text, right.text);
                try std.testing.expectEqualDeep(left.highlights, right.highlights);
                try std.testing.expect(std.unicode.utf8ValidateSlice(right.text));
            }
        }
    };
    try Run.run(a, view, text);
    std.debug.print("STORED_HIGHLIGHT source_bytes={d} compressed_bytes={d} read_bytes={d}\n", .{ text.len, memory.out.items.len, reads.read_bytes });
    try std.testing.expect(reads.read_bytes < text.len / 4);
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{ view, text });
}

test "highlight streaming occurrences exactly preserve greedy windows with bounded memory faults and cancellation" {
    const a = std.testing.allocator;
    const column = @import("../section/source_column.zig");
    const segment = @import("../segment.zig");
    const view = @import("../segment_source.zig").View;
    var body: std.ArrayList(u8) = .empty;
    defer body.deinit(a);
    for (0..20000) |i| try body.appendSlice(a, if (i % 13 == 0) " other.... " else if (i % 3 == 0) " λ 🙂 " else "needle ");
    var memory = segment.MemorySegmentSink.init(a);
    defer memory.deinit();
    var sink = memory.sink();
    var writer = try column.Writer.init(a, &sink);
    defer writer.deinit();
    try writer.add(0, .{ .string = body.items }, &.{&analysis_mod.simple_analyzer});
    try std.testing.expect(try writer.finish());
    var cursor = column.Cursor.init(try column.Reader.init(a, try view.init(.{ .contiguous = memory.out.items }, 0, memory.out.items.len)));
    defer cursor.deinit();
    const stored = (try cursor.text(0)).?;
    const signature = try column.fingerprint(a, &analysis_mod.simple_analyzer);
    const matchers = [_]Matcher{ .{ .term = "needle" }, .{ .term = "λ" } };
    var matches = (try prepareStoredMatches(a, stored, signature, &matchers, null, .none)).?;
    defer matches.deinit();
    try std.testing.expect(matches == .stream);
    const vector = try matches.stream.materialize(a, .none);
    defer vector.deinit();
    for ([_]u32{ 1, 2, 3, 8 }) |count| for ([_]u32{ 1, 17, 100, 513 }) |width| {
        const expected = try highlightSelectedStored(a, a, stored, vector.items, count, width, .none);
        defer freeFragments(a, expected);
        var profile: StreamProfile = .{};
        var budget: @import("../sql/memory_budget.zig") = .{ .backing = a, .limit = 256 * 1024 };
        const actual = try streamFragments(budget.allocator(), budget.allocator(), stored, &matches.stream, count, width, .none, &profile);
        try std.testing.expectEqual(expected.len, actual.len);
        for (expected, actual) |old, new| {
            try std.testing.expectEqual(old.offset, new.offset);
            try std.testing.expectEqualStrings(old.text, new.text);
            try std.testing.expectEqualSlices(Span, old.highlights, new.highlights);
        }
        try std.testing.expect(profile.queue_peak < 2048 + width);
        try std.testing.expect(profile.windows_peak <= (@as(usize, count) - 1) * (2 * @as(usize, width) + 20) + 1);
        if (count == 3 and width == 100) std.debug.print("SOURCE_STREAM occurrences={d} queue_peak={d} windows_peak={d} peak_bytes={d}\n", .{ matches.stream.count, profile.queue_peak, profile.windows_peak, budget.peak });
        freeFragments(budget.allocator(), actual);
        try std.testing.expectEqual(@as(usize, 0), budget.live);
    };
    const Run = struct {
        fn run(alloc: Allocator, text: column.Text, plan: *const @import("../section/source_offsets.zig").Plan) !void {
            const result = try streamFragments(alloc, alloc, text, plan, 1, 17, .none, null);
            defer freeFragments(alloc, result);
        }
    };
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{ stored, &matches.stream });
    const Stop = struct {
        calls: usize = 0,
        limit: usize,
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.calls += 1;
            if (self.calls == self.limit) return error.DeadlineExceeded;
        }
    };
    for ([_]usize{ 1, 8, 32, 128, 256 }) |limit| {
        var stop: Stop = .{ .limit = limit };
        try std.testing.expectError(error.DeadlineExceeded, streamFragments(a, a, stored, &matches.stream, 3, 100, .{ .ptr = &stop, .check_fn = Stop.check }, null));
    }
}

test "highlight dense block certificates prune exact greedy winners and seek output intervals under faults" {
    const a = std.testing.allocator;
    const column = @import("../section/source_column.zig");
    const segment = @import("../segment.zig");
    const View = @import("../segment_source.zig").View;
    var body: std.ArrayList(u8) = .empty;
    defer body.deinit(a);
    try body.appendSlice(a, "densecase ");
    for (0..32768) |_| try body.appendSlice(a, "needle ");
    var memory = segment.MemorySegmentSink.init(a);
    defer memory.deinit();
    var sink = memory.sink();
    var writer = try column.Writer.init(a, &sink);
    defer writer.deinit();
    try writer.add(0, .{ .string = body.items }, &.{&analysis_mod.simple_analyzer});
    try std.testing.expect(try writer.finish());
    var cursor = column.Cursor.init(try column.Reader.init(a, try View.init(.{ .contiguous = memory.out.items }, 0, memory.out.items.len)));
    defer cursor.deinit();
    const stored = (try cursor.text(0)).?;
    var plan = (try stored.planTokens(a, try column.fingerprint(a, &analysis_mod.simple_analyzer), .{}, .none)).?;
    defer plan.deinit();
    const vector = try plan.materialize(a, .none);
    defer vector.deinit();
    for ([_]u32{ 1, 2, 3, 8 }) |count| for ([_]u32{ 1, 2, 7, 17, 100, 513 }) |width| {
        const expected = try highlightSelectedStored(a, a, stored, vector.items, count, width, .none);
        defer freeFragments(a, expected);
        var profile: StreamProfile = .{};
        const actual = try streamFragments(a, a, stored, &plan, count, width, .none, &profile);
        defer freeFragments(a, actual);
        try std.testing.expectEqual(expected.len, actual.len);
        for (expected, actual) |left, right| {
            try std.testing.expectEqual(left.offset, right.offset);
            try std.testing.expectEqualStrings(left.text, right.text);
            try std.testing.expectEqualSlices(Span, left.highlights, right.highlights);
        }
        if (count == 3 and width == 100) {
            try std.testing.expect(profile.pruned);
            try std.testing.expect(profile.spans < 1024);
            try std.testing.expect(profile.block_reads <= 12);
            std.debug.print("DENSE_BLOCK_PRUNING occurrences={d} visited={d} decoded_blocks={d} exact=true\n", .{ plan.count, profile.spans, profile.block_reads });
        }
    };
    const Faults = struct {
        fn run(alloc: Allocator, text: column.Text, offsets: *const @import("../section/source_offsets.zig").Plan) !void {
            const fragments = try streamFragments(alloc, alloc, text, offsets, 3, 100, .none, null);
            defer freeFragments(alloc, fragments);
        }
    };
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Faults.run, .{ stored, &plan });
}

test "highlight candidate aligned regular spans prune stride eight with exact greedy fragments" {
    const a = std.testing.allocator;
    const column = @import("../section/source_column.zig");
    const segment = @import("../segment.zig");
    const View = @import("../segment_source.zig").View;
    for ([_][]const u8{ "needle ", "running ", "abcdefgh ", "longerwordxx " }) |word| {
        var body: std.ArrayList(u8) = .empty;
        defer body.deinit(a);
        for (0..32768) |_| try body.appendSlice(a, word);
        var memory = segment.MemorySegmentSink.init(a);
        defer memory.deinit();
        var sink = memory.sink();
        var writer = try column.Writer.init(a, &sink);
        defer writer.deinit();
        try writer.add(0, .{ .string = body.items }, &.{&analysis_mod.simple_analyzer});
        try std.testing.expect(try writer.finish());
        var cursor = column.Cursor.init(try column.Reader.init(a, try View.init(.{ .contiguous = memory.out.items }, 0, memory.out.items.len)));
        defer cursor.deinit();
        const stored = (try cursor.text(0)).?;
        var plan = (try stored.planTokens(a, try column.fingerprint(a, &analysis_mod.simple_analyzer), .{}, .none)).?;
        defer plan.deinit();
        const vector = try plan.materialize(a, .none);
        defer vector.deinit();
        for ([_]u32{ 1, 17, 100, 513 }) |width| {
            const expected = try highlightSelectedStored(a, a, stored, vector.items, 3, width, .none);
            defer freeFragments(a, expected);
            var profile: StreamProfile = .{};
            const actual = try streamFragments(a, a, stored, &plan, 3, width, .none, &profile);
            defer freeFragments(a, actual);
            try std.testing.expectEqual(expected.len, actual.len);
            for (expected, actual) |old, new| {
                try std.testing.expectEqual(old.offset, new.offset);
                try std.testing.expectEqualStrings(old.text, new.text);
                try std.testing.expectEqualSlices(Span, old.highlights, new.highlights);
            }
            if (width == 100 and std.mem.eql(u8, word, "running ")) {
                try std.testing.expect(profile.pruned);
                try std.testing.expect(profile.spans <= 512);
                try std.testing.expect(profile.block_reads <= 6);
                std.debug.print("REGULAR_DENSITY occurrences={d} visited={d} blocks={d} exact=true\n", .{ plan.count, profile.spans, profile.block_reads });
            }
        }
    }
}

test "highlight diverse source lane bounds decoded heads preserves exact results and cancellation" {
    const a = std.testing.allocator;
    const column = @import("../section/source_column.zig");
    const segment = @import("../segment.zig");
    const View = @import("../segment_source.zig").View;
    var body: std.ArrayList(u8) = .empty;
    defer body.deinit(a);
    for (0..8192) |i| {
        var buffer: [24]u8 = undefined;
        try body.appendSlice(a, try std.fmt.bufPrint(&buffer, "term{d:0>5} ", .{i}));
    }
    var memory = segment.MemorySegmentSink.init(a);
    defer memory.deinit();
    var sink = memory.sink();
    var writer = try column.Writer.init(a, &sink);
    defer writer.deinit();
    try writer.add(0, .{ .string = body.items }, &.{&analysis_mod.simple_analyzer});
    try std.testing.expect(try writer.finish());
    var cursor = column.Cursor.init(try column.Reader.init(a, try View.init(.{ .contiguous = memory.out.items }, 0, memory.out.items.len)));
    defer cursor.deinit();
    const stored = (try cursor.text(0)).?;
    var matches = (try prepareStoredMatches(a, stored, try column.fingerprint(a, &analysis_mod.simple_analyzer), &.{.{ .prefix = "term" }}, null, .none)).?;
    defer matches.deinit();
    try std.testing.expect(matches == .stream);
    try std.testing.expectEqual(@as(usize, 8192), matches.stream.selectedTermCount());
    try std.testing.expectEqual(@as(usize, 0), matches.stream.terms.len);
    try std.testing.expectEqual(@as(usize, 1024), matches.stream.source_mask.?.len);
    try std.testing.expectEqual(@as(usize, 0), matches.stream.density_terms.len);
    {
        var spans = try @import("../section/source_offsets.zig").Iterator.init(a, &matches.stream, .none);
        defer spans.deinit();
        try std.testing.expect((try spans.nextSpan()) != null);
        try std.testing.expect(spans.resolved == null);
        try spans.reset();
        const full = (try spans.next()).?;
        try std.testing.expectEqualStrings("term00000", full.term);
    }
    const vector = try matches.stream.materialize(a, .none);
    defer vector.deinit();
    const expected = try highlightSelectedStored(a, a, stored, vector.items, 3, 100, .none);
    defer freeFragments(a, expected);
    var budget: @import("../sql/memory_budget.zig") = .{ .backing = a, .limit = 256 * 1024 };
    var profile: StreamProfile = .{};
    const actual = try streamFragments(budget.allocator(), budget.allocator(), stored, &matches.stream, 3, 100, .none, &profile);
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |old, new| {
        try std.testing.expectEqual(old.offset, new.offset);
        try std.testing.expectEqualStrings(old.text, new.text);
        try std.testing.expectEqualSlices(Span, old.highlights, new.highlights);
    }
    try std.testing.expect(profile.block_reads <= 16);
    std.debug.print("DIVERSE_SOURCE_LANE terms={d} occurrences={d} decoded_blocks={d} kernel_peak={d} exact=true\n", .{ matches.stream.selectedTermCount(), matches.stream.count, profile.block_reads, budget.peak });
    freeFragments(budget.allocator(), actual);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    const Faults = struct {
        fn run(alloc: Allocator, text: column.Text, plan: *const @import("../section/source_offsets.zig").Plan) !void {
            const fragments = try streamFragments(alloc, alloc, text, plan, 3, 100, .none, null);
            defer freeFragments(alloc, fragments);
        }
    };
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Faults.run, .{ stored, &matches.stream });
    const Stop = struct {
        calls: usize = 0,
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.calls += 1;
            if (self.calls == 32) return error.DeadlineExceeded;
        }
    };
    var stop: Stop = .{};
    try std.testing.expectError(error.DeadlineExceeded, streamFragments(a, a, stored, &matches.stream, 3, 100, .{ .ptr = &stop, .check_fn = Stop.check }, null));
}

test "highlight randomized stored fragment geometry preserves exact greedy output" {
    const a = std.testing.allocator;
    const column = @import("../section/source_column.zig");
    const offsets = @import("../section/source_offsets.zig");
    const segment = @import("../segment.zig");
    const View = @import("../segment_source.zig").View;
    var random = std.Random.DefaultPrng.init(0x9669791307);
    const rng = random.random();
    var checked: usize = 0;
    for (0..24) |seed| {
        const body = try a.alloc(u8, 4096 * 5 + 64);
        defer a.free(body);
        @memset(body, 'x');
        const tokens = try a.alloc(analysis_mod.Token, 4096);
        defer a.free(tokens);
        for (tokens, 0..) |*token, i| {
            const start: u32 = @intCast(i * 5);
            const length: u32 = if (seed % 2 == 0) 1 + rng.uintLessThan(u32, 4) else if (i % 37 == 0) 80 else 1 + rng.uintLessThan(u32, 12);
            token.* = .{ .term = if (i % 7 == 0) "other" else "needle", .start_byte = start, .end_byte = @min(@as(u32, @intCast(body.len)), start + length), .position = @intCast(i) };
        }
        var captured = segment.MemorySegmentSink.init(a);
        defer captured.deinit();
        var capture_sink = captured.sink();
        const encoded = try offsets.write(a, &capture_sink, tokens);
        var memory = segment.MemorySegmentSink.init(a);
        defer memory.deinit();
        var sink = memory.sink();
        var writer = try column.Writer.init(a, &sink);
        defer writer.deinit();
        const signature = try column.fingerprint(a, &analysis_mod.simple_analyzer);
        try writer.addPrepared(0, .{ .string = body }, .{ .view = try View.init(.{ .contiguous = captured.out.items }, 0, captured.out.items.len), .items = &.{.{ .signature = signature, .offset = 0, .bytes = encoded.bytes, .root_raw = encoded.root_raw, .count = encoded.count }} });
        try std.testing.expect(try writer.finish());
        var cursor = column.Cursor.init(try column.Reader.init(a, try View.init(.{ .contiguous = memory.out.items }, 0, memory.out.items.len)));
        defer cursor.deinit();
        const stored = (try cursor.text(0)).?;
        var plan = (try stored.planTokens(a, signature, .{}, .none)).?;
        defer plan.deinit();
        const vector = try plan.materialize(a, .none);
        defer vector.deinit();
        for ([_]u32{ 1, 2, 5 }) |count| for ([_]u32{ 1, 17, 100, 513 }) |width| {
            const expected = try highlightSelectedStored(a, a, stored, vector.items, count, width, .none);
            defer freeFragments(a, expected);
            const actual = try streamFragments(a, a, stored, &plan, count, width, .none, null);
            defer freeFragments(a, actual);
            try std.testing.expectEqual(expected.len, actual.len);
            for (expected, actual) |left, right| {
                try std.testing.expectEqual(left.offset, right.offset);
                try std.testing.expectEqualStrings(left.text, right.text);
                try std.testing.expectEqualSlices(Span, left.highlights, right.highlights);
            }
            checked += 1;
        };
    }
    std.debug.print("FRAGMENT_GEOMETRY cases={d} tokens=4096 disjoint_and_crossing=true\n", .{checked});
}

test "highlight suffix block density prunes a mixed field after an early dense cluster" {
    const a = std.testing.allocator;
    const column = @import("../section/source_column.zig");
    const segment = @import("../segment.zig");
    const View = @import("../segment_source.zig").View;
    var body: std.ArrayList(u8) = .empty;
    defer body.deinit(a);
    try body.appendSlice(a, "needle ");
    for (0..16384) |_| try body.appendSlice(a, "needle other ");
    var memory = segment.MemorySegmentSink.init(a);
    defer memory.deinit();
    var sink = memory.sink();
    var writer = try column.Writer.init(a, &sink);
    defer writer.deinit();
    try writer.add(0, .{ .string = body.items }, &.{&analysis_mod.simple_analyzer});
    try std.testing.expect(try writer.finish());
    var cursor = column.Cursor.init(try column.Reader.init(a, try View.init(.{ .contiguous = memory.out.items }, 0, memory.out.items.len)));
    defer cursor.deinit();
    const stored = (try cursor.text(0)).?;
    var plan = (try stored.planTokens(a, try column.fingerprint(a, &analysis_mod.simple_analyzer), .{}, .none)).?;
    defer plan.deinit();
    const vector = try plan.materialize(a, .none);
    defer vector.deinit();
    const expected = try highlightSelectedStored(a, a, stored, vector.items, 3, 100, .none);
    defer freeFragments(a, expected);
    var profile: StreamProfile = .{};
    const actual = try streamFragments(a, a, stored, &plan, 3, 100, .none, &profile);
    defer freeFragments(a, actual);
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |old, new| {
        try std.testing.expectEqual(old.offset, new.offset);
        try std.testing.expectEqualStrings(old.text, new.text);
        try std.testing.expectEqualSlices(Span, old.highlights, new.highlights);
    }
    try std.testing.expect(profile.pruned);
    try std.testing.expect(profile.spans <= 3072);
    try std.testing.expect(profile.block_reads <= 12);
    std.debug.print("CLUSTER_DENSITY terms={d} occurrences={d} visited={d} blocks={d} pruned={any} exact=true\n", .{ plan.terms.len, plan.count, profile.spans, profile.block_reads, profile.pruned });
}
