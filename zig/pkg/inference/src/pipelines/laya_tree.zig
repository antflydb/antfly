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
    /// Trunk tokens attend to their whole tree (state plus every branch),
    /// not only to the trunk (`packing.trunk_sees: "questions"`). Branches
    /// still see only their ancestors. The trunk then depends on the
    /// question set, so it cannot be cached across rows.
    trunk_sees_tree: bool = false,
    /// Per-question upper layers (`packing.fuse_layers`): every tree holds
    /// exactly one question, and the fused layers attend as `upper()` does
    /// (each state copy also sees its question). The layers below attend as
    /// this row does, so every copy of a state encodes identically there.
    fused: bool = false,

    pub fn questions(self: Row) usize {
        return self.anchors.len;
    }

    pub fn deinit(self: Row, a: std.mem.Allocator) void {
        for ([_][]const i64{ self.ids, self.positions, self.segments, self.parents, self.kinds, self.anchors, self.markers }) |slice| a.free(slice);
        a.free(self.question_index);
    }

    /// The visibility of the fused upper layers: each trunk also sees its
    /// tree, which holds one question. Views the same slices.
    pub fn upper(self: Row) Row {
        var out = self;
        out.trunk_sees_tree = true;
        out.fused = false;
        return out;
    }

    /// True when `query` may attend to `key`.
    pub fn visible(self: Row, query: usize, key: usize) bool {
        const target = self.segments[key];
        if (self.trunk_sees_tree and self.parents[@intCast(self.segments[query])] == -1) {
            var root = target;
            while (self.parents[@intCast(root)] != -1) root = self.parents[@intCast(root)];
            return root == self.segments[query];
        }
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

/// Validate a row received over a tensor boundary before any model work. A
/// row may hold more than one trunk (multi-row batching, `coalesce` below):
/// each root segment (`parents[s] == -1`) starts an independent tree, and no
/// segment outside the trunk may itself be a root.
pub fn validate(row: Row, max_len: usize, max_packed_len: usize, max_options: usize) !void {
    const n = row.ids.len;
    if (n == 0 or n > max_packed_len or row.positions.len != n or row.segments.len != n or row.kinds.len != n) return error.InvalidLayaPackedRow;
    if (row.parents.len == 0 or row.parents.len > n or row.parents[0] != -1) return error.InvalidLayaPackedRow;
    for (row.parents[1..], 1..) |parent, s| if (parent != -1 and (parent < 0 or parent >= s)) return error.InvalidLayaPackedRow;
    const q = row.anchors.len;
    if (q == 0 or row.width < 2 or row.width > max_options or row.markers.len != q * row.width or row.question_index.len != q) return error.InvalidLayaPackedRow;
    for (row.segments, row.positions, row.kinds) |segment, position, kind| {
        if (segment < 0 or segment >= row.parents.len or position < 0 or position >= max_len) return error.InvalidLayaPackedRow;
        const is_root = row.parents[@intCast(segment)] == -1;
        if (is_root != (kind == trunk_kind) or (kind != trunk_kind and (kind < 0 or kind > 2))) return error.InvalidLayaPackedRow;
    }
    // Segments are contiguous and at most three deep from their own tree's
    // root (trunk, question, candidate), so each token's visible keys are at
    // most three ranges.
    for (row.segments[1..], 1..) |segment, i| {
        if (segment != row.segments[i - 1] and segment <= row.segments[i - 1]) return error.InvalidLayaPackedRow;
    }
    for (0..row.parents.len) |first| {
        // Count the segment itself: `ranges` has three slots per token.
        var depth: usize = 0;
        var segment: i64 = @intCast(first);
        while (segment >= 0) : (segment = row.parents[@intCast(segment)]) depth += 1;
        if (depth > 3) return error.InvalidLayaPackedRow;
    }
    if (row.fused and (row.trunk_sees_tree or treeCount(row) != q)) return error.InvalidLayaPackedRow;
    for (row.anchors, 0..) |anchor, question| {
        if (anchor < 0 or anchor >= n) return error.InvalidLayaPackedRow;
        const anchor_segment: usize = @intCast(row.segments[@intCast(anchor)]);
        if (row.parents[anchor_segment] == -1) return error.InvalidLayaPackedRow;
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

/// Number of independent trees (root segments) in a row.
pub fn treeCount(row: Row) usize {
    var count: usize = 0;
    for (row.parents) |parent| count += @intFromBool(parent == -1);
    return count;
}

/// Combine several independently built and valid rows into one physical row
/// for a single session call. Each input row keeps its own trunk as a
/// separate root segment; positions stay row-local (RoPE and the sliding
/// window never compare positions across trees), and every visible-key range
/// stays inside its owning row's token span, so trees are exactly as
/// isolated as separate calls. `owners[k]` names which `rows[i]` contributed
/// the merged row's `k`-th question, so a caller can map `question_index`
/// back to its original per-state member list. Caller owns the returned row
/// and `owners` slice (freed together, both from `a`).
pub const Coalesced = struct {
    row: Row,
    owners: []const usize,

    pub fn deinit(self: Coalesced, a: std.mem.Allocator) void {
        self.row.deinit(a);
        a.free(self.owners);
    }
};

pub fn coalesce(a: std.mem.Allocator, rows: []const Row) !Coalesced {
    if (rows.len == 0) return error.InvalidLayaPackedRow;
    var total_tokens: usize = 0;
    var total_segments: usize = 0;
    var total_questions: usize = 0;
    var width: usize = 0;
    for (rows) |r| {
        total_tokens += r.ids.len;
        total_segments += r.parents.len;
        total_questions += r.questions();
        width = @max(width, r.width);
    }
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const s = arena.allocator();
    const ids = try s.alloc(i64, total_tokens);
    const positions = try s.alloc(i64, total_tokens);
    const segments = try s.alloc(i64, total_tokens);
    const kinds = try s.alloc(i64, total_tokens);
    const parents = try s.alloc(i64, total_segments);
    const anchors = try s.alloc(i64, total_questions);
    const markers = try s.alloc(i64, total_questions * width);
    const question_index = try s.alloc(usize, total_questions);
    const owners = try a.alloc(usize, total_questions);
    errdefer a.free(owners);
    @memset(markers, -1);
    var token_offset: usize = 0;
    var segment_offset: usize = 0;
    var q_offset: usize = 0;
    for (rows, 0..) |r, ri| {
        const n = r.ids.len;
        @memcpy(ids[token_offset..][0..n], r.ids);
        @memcpy(positions[token_offset..][0..n], r.positions);
        @memcpy(kinds[token_offset..][0..n], r.kinds);
        for (r.segments, 0..) |seg, i| segments[token_offset + i] = seg + @as(i64, @intCast(segment_offset));
        for (r.parents, 0..) |parent, i| parents[segment_offset + i] = if (parent == -1) -1 else parent + @as(i64, @intCast(segment_offset));
        for (r.anchors, 0..) |anchor, i| anchors[q_offset + i] = anchor + @as(i64, @intCast(token_offset));
        for (0..r.questions()) |qi| {
            const dst = markers[(q_offset + qi) * width ..][0..width];
            const src = r.markers[qi * r.width ..][0..r.width];
            for (src, 0..) |marker, k| dst[k] = if (marker == -1) -1 else marker + @as(i64, @intCast(token_offset));
        }
        @memcpy(question_index[q_offset..][0..r.questions()], r.question_index);
        for (owners[q_offset..][0..r.questions()]) |*owner| owner.* = ri;
        token_offset += n;
        segment_offset += r.parents.len;
        q_offset += r.questions();
    }
    for (rows) |r| if (r.trunk_sees_tree != rows[0].trunk_sees_tree or r.fused != rows[0].fused) return error.InvalidLayaPackedRow;
    const row = try own(a, .{ .ids = ids, .positions = positions, .segments = segments, .parents = parents, .kinds = kinds, .anchors = anchors, .markers = markers, .question_index = question_index, .width = width, .trunk_sees_tree = rows[0].trunk_sees_tree, .fused = rows[0].fused });
    return .{ .row = row, .owners = owners };
}

/// Visible key ranges of tokens `first..` for `ops.SegmentAttention`: the
/// `[start, end)` extent of each token's segment and of its ancestors, three
/// pairs per token (unused pairs empty). Requires a validated row.
pub fn ranges(a: std.mem.Allocator, row: Row, first: usize) ![]u32 {
    const extents = try a.alloc([2]u32, row.parents.len);
    defer a.free(extents);
    for (extents) |*e| e.* = .{ 0, 0 };
    var i: usize = 0;
    while (i < row.segments.len) {
        const segment: usize = @intCast(row.segments[i]);
        var end = i;
        while (end < row.segments.len and row.segments[end] == row.segments[i]) end += 1;
        extents[segment] = .{ @intCast(i), @intCast(end) };
        i = end;
    }
    const out = try a.alloc(u32, (row.ids.len - first) * 6);
    @memset(out, 0);
    // Trees are contiguous: a tree runs from its root segment's first token
    // to the next root segment's first token.
    const tree_end = try a.alloc(u32, row.parents.len);
    defer a.free(tree_end);
    if (row.trunk_sees_tree) {
        var root: ?usize = null;
        for (row.segments, 0..) |seg, t| {
            const s_: usize = @intCast(seg);
            if (row.parents[s_] == -1 and (root == null or root.? != s_)) {
                if (root) |r| tree_end[r] = @intCast(t);
                root = s_;
            }
        }
        if (root) |r| tree_end[r] = @intCast(row.segments.len);
    }
    for (first..row.ids.len) |token| {
        const own_segment: usize = @intCast(row.segments[token]);
        if (row.trunk_sees_tree and row.parents[own_segment] == -1) {
            out[(token - first) * 6 ..][0..2].* = .{ extents[own_segment][0], tree_end[own_segment] };
            continue;
        }
        var slot: usize = 0;
        var segment = row.segments[token];
        while (segment >= 0 and slot < 3) : (segment = row.parents[@intCast(segment)]) {
            out[(token - first) * 6 + 2 * slot ..][0..2].* = extents[@intCast(segment)];
            slot += 1;
        }
    }
    return out;
}

/// Logical positions as i32 for `ops.SegmentAttention`.
pub fn positions32(a: std.mem.Allocator, positions: []const i64) ![]i32 {
    const out = try a.alloc(i32, positions.len);
    for (out, positions) |*dst, p| dst.* = @intCast(p);
    return out;
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

/// A branch's shape: `question` packs every option into one branch that sees
/// its siblings, like upstream's layout; `candidate` gives each option its
/// own branch, isolated from its siblings (LAYA.md, Layout).
pub const BranchStyle = enum(u8) { question, candidate };

fn styleOf(mode: model.PackingMode) BranchStyle {
    return if (mode == .candidate) .candidate else .question;
}

/// Pack all questions about one state into as few rows as `max_packed_len`
/// allows. The trunk is repeated only when a single row cannot hold every
/// branch. `style` defaults to the shape `cfg.packing.mode` implies; passing
/// `.question` explicitly builds a joint branch even when `cfg.packing.mode`
/// is `.candidate`, which is how two-stage choice (LAYA.md, roadmap 2b)
/// compares a shortlist of finalists off the same trunk. Caller owns the
/// returned rows and slice.
pub fn build(a: std.mem.Allocator, tok: Tokenizer, cfg: model.Config, text: []const u8, questions: []const laya.Question, style_override: ?BranchStyle) ![]Row {
    if (!cfg.packing.enabled() or questions.len == 0) return error.InvalidLayaPackedRow;
    const style = style_override orelse styleOf(cfg.packing.mode);
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
        switch (style) {
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
        }
        if (trunk + branch.logical > cfg.max_len or trunk + branch.physical > cfg.packing.max_packed_len) return error.ExtractionTextLimitExceeded;
    }
    var rows: std.ArrayListUnmanaged(Row) = .empty;
    errdefer {
        for (rows.items) |row| row.deinit(a);
        rows.deinit(a);
    }
    if (cfg.packing.fuse_layers > 0) {
        // Per-question upper layers: one tree per question, each with its own
        // copy of the state, coalesced into as few rows as fit.
        var first: usize = 0;
        while (first < branches.len) {
            var end = first;
            var used: usize = 0;
            while (end < branches.len and used + trunk + branches[end].physical <= cfg.packing.max_packed_len) : (end += 1) used += trunk + branches[end].physical;
            const trees = try a.alloc(Row, end - first);
            var made: usize = 0;
            defer {
                for (trees[0..made]) |t| t.deinit(a);
                a.free(trees);
            }
            for (first..end) |i| {
                trees[made] = try emit(a, tok, cfg, state, branches[i .. i + 1], trunk + branches[i].physical, style);
                made += 1;
            }
            const merged = try coalesce(a, trees);
            a.free(merged.owners);
            errdefer merged.row.deinit(a);
            try rows.append(a, merged.row);
            first = end;
        }
        return rows.toOwnedSlice(a);
    }
    var first: usize = 0;
    while (first < branches.len) {
        var end = first;
        var used = trunk;
        while (end < branches.len and used + branches[end].physical <= cfg.packing.max_packed_len) : (end += 1) used += branches[end].physical;
        try rows.append(a, try emit(a, tok, cfg, state, branches[first..end], used, style));
        first = end;
    }
    return rows.toOwnedSlice(a);
}

fn emit(a: std.mem.Allocator, tok: Tokenizer, cfg: model.Config, state: []const i32, branches: []const Branch, total: usize, style: BranchStyle) !Row {
    const special = tok.specialTokens();
    var width: usize = 2;
    var segment_count: usize = 1;
    for (branches) |branch| {
        width = @max(width, branch.tokens.options.len);
        segment_count += 1 + if (style == .candidate) branch.tokens.options.len else 0;
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
        const head_len = if (style == .candidate) @min(t.head.len, cfg.head_max_len) else t.head_len;
        for (t.head[0..head_len]) |id| w.put(id, question_segment, kind);
        w.put(special.sep_id, question_segment, kind);
        const option_start = w.position;
        for (t.options, 0..) |option, i| {
            const run = if (style == .candidate) 1 + @min(option.len, laya.max_option_tokens) else t.optionRun(i);
            var owner = question_segment;
            if (style == .candidate) {
                owner = segment;
                parents[segment] = @intCast(question_segment);
                segment += 1;
                w.position = option_start;
            }
            markers[qi * width + i] = @intCast(w.at);
            w.put(special.mask_id, owner, kind);
            for (option[0 .. run - 1]) |id| w.put(id, owner, kind);
        }
        if (style == .question) w.put(special.sep_id, question_segment, kind);
    }
    std.debug.assert(w.at == total and segment == segment_count);
    return own(a, .{ .ids = w.ids, .positions = w.positions, .segments = w.segments, .parents = parents, .kinds = w.kinds, .anchors = anchors, .markers = markers, .question_index = question_index, .width = width, .trunk_sees_tree = cfg.packing.trunk_sees_questions, .fused = cfg.packing.fuse_layers > 0 });
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
    out.trunk_sees_tree = row.trunk_sees_tree;
    out.fused = row.fused;
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

test "laya tree validation accepts three levels and rejects a fourth" {
    // trunk -> question -> candidate is the deepest layout `ranges` can cover.
    const three = Row{ .ids = &.{ 1, 2, 3, 4, 5 }, .positions = &.{ 0, 1, 2, 3, 3 }, .segments = &.{ 0, 1, 2, 3, 3 }, .parents = &.{ -1, 0, 1, 1 }, .kinds = &.{ trunk_kind, 0, 0, 0, 0 }, .anchors = &.{1}, .markers = &.{ 2, 3 }, .question_index = &.{0}, .width = 2 };
    try validate(three, 16, 16, 8);
    // A fourth level would silently lose sight of its root in `ranges`.
    const four = Row{ .ids = &.{ 1, 2, 3, 4, 5 }, .positions = &.{ 0, 1, 2, 3, 4 }, .segments = &.{ 0, 1, 2, 3, 4 }, .parents = &.{ -1, 0, 1, 2, 1 }, .kinds = &.{ trunk_kind, 0, 0, 0, 0 }, .anchors = &.{1}, .markers = &.{ 3, 4 }, .question_index = &.{0}, .width = 2 };
    try std.testing.expectError(error.InvalidLayaPackedRow, validate(four, 16, 16, 8));
}

test "laya question-aware trunk ranges match visibility, and trees stay isolated" {
    const a = std.testing.allocator;
    // Two trees: trunk 0 with branches 1, 2; trunk 3 with branch 4.
    const one = Row{ .ids = &.{ 1, 2, 3, 4, 5, 6, 7 }, .positions = &.{ 0, 1, 2, 3, 2, 3, 4 }, .segments = &.{ 0, 0, 1, 1, 2, 2, 2 }, .parents = &.{ -1, 0, 0 }, .kinds = &.{ trunk_kind, trunk_kind, 0, 0, 1, 1, 1 }, .anchors = &.{ 2, 4 }, .markers = &.{ 3, -1, 5, 6 }, .question_index = &.{ 0, 1 }, .width = 2, .trunk_sees_tree = true };
    const two = Row{ .ids = &.{ 8, 9, 10 }, .positions = &.{ 0, 1, 1 }, .segments = &.{ 0, 1, 1 }, .parents = &.{ -1, 0 }, .kinds = &.{ trunk_kind, 2, 2 }, .anchors = &.{1}, .markers = &.{ 1, 2 }, .question_index = &.{0}, .width = 2, .trunk_sees_tree = true };
    const merged = try coalesce(a, &.{ one, two });
    defer merged.deinit(a);
    const row = merged.row;
    try std.testing.expect(row.trunk_sees_tree);
    const r = try ranges(a, row, 0);
    defer a.free(r);
    for (0..row.ids.len) |q| for (0..row.ids.len) |k| {
        var in_range = false;
        for (0..3) |slot| {
            const lo = r[q * 6 + 2 * slot];
            const hi = r[q * 6 + 2 * slot + 1];
            if (k >= lo and k < hi) in_range = true;
        }
        try std.testing.expectEqual(row.visible(q, k), in_range);
    };
    // A trunk sees its own questions, never the other tree; a branch still
    // sees only its ancestors.
    try std.testing.expect(row.visible(0, 5) and !row.visible(0, 7) and !row.visible(7, 0));
    try std.testing.expect(!row.visible(2, 4) and row.visible(2, 0));
}
