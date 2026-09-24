// Copyright 2026 Antfly, Inc.
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

//! Tree-packed Laya rows. See zig/pkg/inference/models/laya/LAYA.md.
//!
//! One row packs a shared state trunk and one branch per question into a
//! single sequence; candidate mode adds one branch per option under its
//! question. A token attends to a key exactly when the key's segment is an
//! ancestor of, or equal to, the token's own segment. The trunk therefore
//! never sees a question, so its encoding is identical for every question,
//! and sibling branches never see each other. Each branch's logical
//! positions continue from the end of its parent, so every root-to-leaf path
//! is laid out exactly as the unpacked sequence `[trunk; question; option]`.
const std = @import("std");
const model = @import("../models/laya.zig");
const laya = @import("laya.zig");
const Tokenizer = @import("inference_tokenizer").Tokenizer;

/// Token kind of the shared trunk. It receives no question-type embedding.
pub const trunk_kind: i64 = -1;
/// Additive attention bias for invisible keys. Finite, so a fully padded
/// score row can never produce NaN; every query always sees itself.
pub const blocked: f32 = -1e9;

/// Rows from `build` and `own` own every slice; `validate` also accepts views.
pub const Row = struct {
    ids: []const i64,
    /// Logical RoPE and sliding-window positions.
    positions: []const i64,
    /// Token -> segment. Segment 0 is the trunk.
    segments: []const i64,
    /// Segment -> parent segment; -1 for the trunk, otherwise a lower index.
    parents: []const i64,
    /// Token -> question type (`QuestionType` value), or `trunk_kind`.
    kinds: []const i64,
    /// Question -> token index of the branch's `[CLS]` anchor.
    anchors: []const i64,
    /// Question-major `[questions * width]` option markers, -1 padded.
    markers: []const i64,
    /// Question -> index into the caller's question list.
    question_index: []const usize,
    width: usize,

    pub fn questions(self: Row) usize {
        return self.anchors.len;
    }

    pub fn deinit(self: Row, a: std.mem.Allocator) void {
        for ([_][]const i64{ self.ids, self.positions, self.segments, self.parents, self.kinds, self.anchors, self.markers }) |slice| a.free(slice);
        a.free(self.question_index);
    }

    /// True when `query` may attend to `key`.
    pub fn visible(self: Row, query: usize, key: usize) bool {
        const target = self.segments[key];
        var segment = self.segments[query];
        while (segment >= 0) : (segment = self.parents[@intCast(segment)]) {
            if (segment == target) return true;
        }
        return false;
    }

    /// Longest logical length (trunk plus deepest branch path).
    pub fn logicalLength(self: Row) usize {
        var longest: i64 = 0;
        for (self.positions) |p| longest = @max(longest, p + 1);
        return @intCast(longest);
    }
};

/// Validate a row received over a tensor boundary before any model work.
pub fn validate(row: Row, max_len: usize, max_packed_len: usize, max_options: usize) !void {
    const n = row.ids.len;
    if (n == 0 or n > max_packed_len or row.positions.len != n or row.segments.len != n or row.kinds.len != n) return error.InvalidLayaPackedRow;
    if (row.parents.len == 0 or row.parents.len > n or row.parents[0] != -1) return error.InvalidLayaPackedRow;
    for (row.parents[1..], 1..) |parent, s| if (parent < 0 or parent >= s) return error.InvalidLayaPackedRow;
    const q = row.anchors.len;
    if (q == 0 or row.width < 2 or row.width > max_options or row.markers.len != q * row.width or row.question_index.len != q) return error.InvalidLayaPackedRow;
    for (row.segments, row.positions, row.kinds) |segment, position, kind| {
        if (segment < 0 or segment >= row.parents.len or position < 0 or position >= max_len) return error.InvalidLayaPackedRow;
        if ((segment == 0) != (kind == trunk_kind) or (kind != trunk_kind and (kind < 0 or kind > 2))) return error.InvalidLayaPackedRow;
    }
    for (row.anchors, 0..) |anchor, question| {
        if (anchor < 0 or anchor >= n or row.segments[@intCast(anchor)] == 0) return error.InvalidLayaPackedRow;
        const kind = row.kinds[@intCast(anchor)];
        var valid: usize = 0;
        for (row.markers[question * row.width ..][0..row.width]) |marker| {
            if (marker == -1) continue;
            if (marker < 0 or marker >= n) return error.InvalidLayaPackedRow;
            // Every option of a question must see that question's anchor.
            if (!row.visible(@intCast(marker), @intCast(anchor)) or row.kinds[@intCast(marker)] != kind) return error.InvalidLayaPackedRow;
            valid += 1;
        }
        if (valid < 2) return error.InvalidLayaPackedRow;
    }
}

/// Dense `[L, L]` additive bias, shared by every head. With `window_half`,
/// visible keys must also lie within that logical distance, matching
/// ModernBERT's local layers on the equivalent unpacked sequence.
pub fn bias(a: std.mem.Allocator, row: Row, window_half: ?usize) ![]f32 {
    const n = row.ids.len;
    const out = try a.alloc(f32, n * n);
    for (0..n) |q| for (0..n) |k| {
        var ok = row.visible(q, k);
        if (ok) if (window_half) |half| {
            ok = @abs(row.positions[q] - row.positions[k]) <= half;
        };
        out[q * n + k] = if (ok) 0 else blocked;
    };
    return out;
}

const Branch = struct {
    question: usize,
    kind: model.QuestionType,
    tokens: laya.QuestionTokens,
    /// Physical and logical cost of this question's subtree.
    physical: usize,
    logical: usize,
};

/// Pack all questions about one state into as few rows as `max_packed_len`
/// allows. The trunk is repeated only when a single row cannot hold every
/// branch. Caller owns the returned rows and slice.
pub fn build(a: std.mem.Allocator, tok: Tokenizer, cfg: model.Config, text: []const u8, questions: []const laya.Question) ![]Row {
    if (!cfg.packing.enabled() or questions.len == 0) return error.InvalidLayaPackedRow;
    const mask = cfg.mask_token[0..cfg.mask_token_len];
    const state = try laya.encodeClean(a, tok, text, mask);
    defer a.free(state);
    const trunk = state.len + 2;
    const branches = try a.alloc(Branch, questions.len);
    defer a.free(branches);
    var initialized: usize = 0;
    defer for (branches[0..initialized]) |branch| branch.tokens.deinit(a);
    for (questions, branches, 0..) |q, *branch, i| {
        const tokens = try laya.questionTokens(a, tok, cfg, q);
        branch.* = .{ .question = i, .kind = q.kind, .tokens = tokens, .physical = 0, .logical = 0 };
        initialized += 1;
        switch (cfg.packing.mode) {
            .question => {
                // [CLS] head [SEP] ([MASK] option)* [SEP]: upstream's head/options budget.
                branch.physical = tokens.head_len + tokens.options_len + 3;
                branch.logical = branch.physical;
            },
            .candidate => {
                // [CLS] head [SEP], then one [MASK] option branch per label.
                const head = @min(tokens.head.len, cfg.head_max_len);
                var longest: usize = 0;
                branch.physical = head + 2;
                for (tokens.options) |ids| {
                    const run = 1 + @min(ids.len, laya.max_option_tokens);
                    branch.physical += run;
                    longest = @max(longest, run);
                }
                branch.logical = head + 2 + longest;
            },
            .none => unreachable,
        }
        if (trunk + branch.logical > cfg.max_len or trunk + branch.physical > cfg.packing.max_packed_len) return error.ExtractionTextLimitExceeded;
    }
    var rows: std.ArrayListUnmanaged(Row) = .empty;
    errdefer {
        for (rows.items) |row| row.deinit(a);
        rows.deinit(a);
    }
    var first: usize = 0;
    while (first < branches.len) {
        var end = first;
        var used = trunk;
        while (end < branches.len and used + branches[end].physical <= cfg.packing.max_packed_len) : (end += 1) used += branches[end].physical;
        try rows.append(a, try emit(a, tok, cfg, state, branches[first..end], used));
        first = end;
    }
    return rows.toOwnedSlice(a);
}

fn emit(a: std.mem.Allocator, tok: Tokenizer, cfg: model.Config, state: []const i32, branches: []const Branch, total: usize) !Row {
    const special = tok.specialTokens();
    var width: usize = 2;
    var segment_count: usize = 1;
    for (branches) |branch| {
        width = @max(width, branch.tokens.options.len);
        segment_count += 1 + if (cfg.packing.mode == .candidate) branch.tokens.options.len else 0;
    }
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var w = Writer{
        .ids = try scratch.alloc(i64, total),
        .positions = try scratch.alloc(i64, total),
        .segments = try scratch.alloc(i64, total),
        .kinds = try scratch.alloc(i64, total),
    };
    const parents = try scratch.alloc(i64, segment_count);
    const anchors = try scratch.alloc(i64, branches.len);
    const markers = try scratch.alloc(i64, branches.len * width);
    const question_index = try scratch.alloc(usize, branches.len);
    @memset(markers, -1);
    parents[0] = -1;
    w.put(special.cls_id, 0, trunk_kind);
    for (state) |id| w.put(id, 0, trunk_kind);
    w.put(special.sep_id, 0, trunk_kind);
    const trunk_end: i64 = @intCast(w.at);
    var segment: usize = 1;
    for (branches, 0..) |branch, qi| {
        const t = branch.tokens;
        const kind: i64 = @intFromEnum(branch.kind);
        const question_segment = segment;
        parents[question_segment] = 0;
        segment += 1;
        question_index[qi] = branch.question;
        anchors[qi] = @intCast(w.at);
        w.position = trunk_end;
        w.put(special.cls_id, question_segment, kind);
        const head_len = if (cfg.packing.mode == .candidate) @min(t.head.len, cfg.head_max_len) else t.head_len;
        for (t.head[0..head_len]) |id| w.put(id, question_segment, kind);
        w.put(special.sep_id, question_segment, kind);
        const option_start = w.position;
        for (t.options, 0..) |option, i| {
            const run = if (cfg.packing.mode == .candidate) 1 + @min(option.len, laya.max_option_tokens) else t.optionRun(i);
            var owner = question_segment;
            if (cfg.packing.mode == .candidate) {
                owner = segment;
                parents[segment] = @intCast(question_segment);
                segment += 1;
                w.position = option_start;
            }
            markers[qi * width + i] = @intCast(w.at);
            w.put(special.mask_id, owner, kind);
            for (option[0 .. run - 1]) |id| w.put(id, owner, kind);
        }
        if (cfg.packing.mode == .question) w.put(special.sep_id, question_segment, kind);
    }
    std.debug.assert(w.at == total and segment == segment_count);
    return own(a, .{ .ids = w.ids, .positions = w.positions, .segments = w.segments, .parents = parents, .kinds = w.kinds, .anchors = anchors, .markers = markers, .question_index = question_index, .width = width });
}

/// Copy a row into `a`, so a partially built row never leaks on error.
pub fn own(a: std.mem.Allocator, row: Row) !Row {
    var out: Row = undefined;
    out.width = row.width;
    var done: usize = 0;
    const fields = .{ "ids", "positions", "segments", "parents", "kinds", "anchors", "markers" };
    errdefer inline for (fields, 0..) |name, i| {
        if (i < done) a.free(@field(out, name));
    };
    inline for (fields) |name| {
        @field(out, name) = try a.dupe(i64, @field(row, name));
        done += 1;
    }
    out.question_index = try a.dupe(usize, row.question_index);
    return out;
}

const Writer = struct {
    ids: []i64,
    positions: []i64,
    segments: []i64,
    kinds: []i64,
    at: usize = 0,
    position: i64 = 0,
    fn put(self: *Writer, id: anytype, segment: usize, kind: i64) void {
        self.ids[self.at] = @intCast(id);
        self.positions[self.at] = self.position;
        self.segments[self.at] = @intCast(segment);
        self.kinds[self.at] = kind;
        self.at += 1;
        self.position += 1;
    }
};
