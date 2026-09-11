// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Compact sparse relation proposals for the shared boundary pool. Endpoint
//! ranking and probability-product pair ranking match the pinned Fastino
//! TypedRelationPairGenerator, with explicit bounds and deterministic ties.
const std = @import("std");
const ops = @import("../architectures/gliner_boundary_ops.zig");
const decode = @import("gliner_boundary_decode.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
pub const Route = struct {
    batch_index: usize,
    relation_index: usize,
    head_queries: []const usize,
    tail_queries: []const usize,
    allow_self: bool = false,
};
pub const Input = struct {
    batch: usize,
    queries: usize,
    capacity: usize,
    spans: []const ops.Span, // [B,C], shared document pool
    valid: []const bool, // [B,C]
    query_mask: []const bool, // [B,Q]
    logits: []const f32, // [B,Q,C], uncalibrated pair logits
};
pub const Proposal = struct {
    batch_index: usize,
    relation_index: usize,
    head_query: usize,
    tail_query: usize,
    head_span: ops.Span,
    tail_span: ops.Span,
    head_probability: f32,
    tail_probability: f32,
};
pub const Options = struct {
    heads_per_relation: usize = 32,
    tails_per_relation: usize = 32,
    pair_cap: usize = 128,
    argument_threshold: f32 = 0,
    max_routes: usize = 512,
    max_endpoints: usize = 65536,
    max_pair_work: usize = 1024 * 1024,
    max_output_pairs: usize = 65536,
    control: ?Control = null,
};
const Endpoint = struct {
    query: usize,
    span: ops.Span,
    probability: f32,
    ordinal: usize,
    fn less(_: void, a: Endpoint, b: Endpoint) bool {
        if (a.probability != b.probability) return a.probability > b.probability;
        if (a.span.start != b.span.start) return a.span.start < b.span.start;
        if (a.span.end != b.span.end) return a.span.end < b.span.end;
        return a.ordinal < b.ordinal;
    }
};
const Pair = struct {
    head: usize,
    tail: usize,
    score: f32,
    fn less(_: void, a: Pair, b: Pair) bool {
        if (a.score != b.score) return a.score > b.score;
        return if (a.head != b.head) a.head < b.head else a.tail < b.tail;
    }
};

fn endpoints(input: Input, batch: usize, queries: []const usize, threshold: f32, output: []Endpoint) ![]Endpoint {
    var count: usize = 0;
    for (queries, 0..) |q, i| {
        if (q >= input.queries) return error.InvalidRelationRoute;
        for (queries[0..i]) |previous| if (previous == q) return error.InvalidRelationRoute;
        if (!input.query_mask[batch * input.queries + q]) continue;
        for (0..input.capacity) |c| {
            if (!input.valid[batch * input.capacity + c]) continue;
            const span = input.spans[batch * input.capacity + c];
            if (span.end <= span.start) return error.InvalidBoundarySpan;
            const ordinal = q * input.capacity + c;
            const logit = input.logits[(batch * input.queries + q) * input.capacity + c];
            if (!std.math.isFinite(logit)) return error.NonFiniteBoundaryScore;
            const probability = decode.sigmoid(logit);
            if (probability < threshold) continue;
            output[count] = .{ .query = q, .span = span, .probability = probability, .ordinal = ordinal };
            count += 1;
        }
    }
    std.mem.sort(Endpoint, output[0..count], {}, Endpoint.less);
    return output[0..count];
}

pub fn generate(allocator: std.mem.Allocator, input: Input, routes: []const Route, options: Options) ![]Proposal {
    if (options.control) |control| try control.check();
    if (routes.len > options.max_routes or options.heads_per_relation == 0 or options.tails_per_relation == 0 or options.pair_cap == 0)
        return error.RelationProposalLimitExceeded;
    if (!std.math.isFinite(options.argument_threshold) or options.argument_threshold < 0 or options.argument_threshold > 1) return error.InvalidDecodeThreshold;
    const bc = try std.math.mul(usize, input.batch, input.capacity);
    const bq = try std.math.mul(usize, input.batch, input.queries);
    if (input.spans.len != bc or input.valid.len != bc or input.query_mask.len != bq or input.logits.len != try std.math.mul(usize, bq, input.capacity)) return error.InvalidInputShape;
    const max_endpoints = try std.math.mul(usize, input.queries, input.capacity);
    if (max_endpoints > options.max_endpoints) return error.RelationProposalLimitExceeded;
    const max_pairs = try std.math.mul(usize, @min(max_endpoints, options.heads_per_relation), @min(max_endpoints, options.tails_per_relation));
    if (max_pairs > options.max_pair_work or try std.math.mul(usize, routes.len, max_pairs) > options.max_pair_work)
        return error.RelationProposalLimitExceeded;
    const head_scratch = try allocator.alloc(Endpoint, max_endpoints);
    defer allocator.free(head_scratch);
    const tail_scratch = try allocator.alloc(Endpoint, max_endpoints);
    defer allocator.free(tail_scratch);
    const pair_scratch = try allocator.alloc(Pair, max_pairs);
    defer allocator.free(pair_scratch);
    var result = std.ArrayListUnmanaged(Proposal).empty;
    errdefer result.deinit(allocator);
    for (routes) |route| {
        if (options.control) |control| try control.check();
        if (route.batch_index >= input.batch) return error.InvalidRelationRoute;
        const all_heads = try endpoints(input, route.batch_index, route.head_queries, options.argument_threshold, head_scratch);
        const all_tails = try endpoints(input, route.batch_index, route.tail_queries, options.argument_threshold, tail_scratch);
        const heads = all_heads[0..@min(all_heads.len, options.heads_per_relation)];
        const tails = all_tails[0..@min(all_tails.len, options.tails_per_relation)];
        var count: usize = 0;
        for (heads, 0..) |head, h| for (tails, 0..) |tail, t| {
            if (!route.allow_self and std.meta.eql(head.span, tail.span)) continue;
            pair_scratch[count] = .{ .head = h, .tail = t, .score = head.probability * tail.probability };
            count += 1;
        };
        std.mem.sort(Pair, pair_scratch[0..count], {}, Pair.less);
        for (pair_scratch[0..@min(count, options.pair_cap)]) |pair| {
            if (result.items.len >= options.max_output_pairs) return error.RelationProposalLimitExceeded;
            const head = heads[pair.head];
            const tail = tails[pair.tail];
            try result.append(allocator, .{ .batch_index = route.batch_index, .relation_index = route.relation_index, .head_query = head.query, .tail_query = tail.query, .head_span = head.span, .tail_span = tail.span, .head_probability = head.probability, .tail_probability = tail.probability });
        }
    }
    return result.toOwnedSlice(allocator);
}

test "gliner boundary relation proposals preserve typed ranking self exclusion and masks" {
    const Check = struct {
        fn run(a: std.mem.Allocator) !void {
            const input = Input{
                .batch = 1,
                .queries = 3,
                .capacity = 3,
                .spans = &.{ .{ .start = 4, .end = 5 }, .{ .start = 0, .end = 1 }, .{ .start = 8, .end = 9 } },
                .valid = &.{ true, true, false },
                .query_mask = &.{ true, true, false },
                .logits = &.{ 0, 0, 99, 1, 1, 99, 100, 100, 100 },
            };
            const pairs = try generate(a, input, &.{.{ .batch_index = 0, .relation_index = 0, .head_queries = &.{ 0, 2 }, .tail_queries = &.{1} }}, .{});
            defer a.free(pairs);
            try std.testing.expectEqual(@as(usize, 2), pairs.len);
            try std.testing.expectEqual(@as(usize, 0), pairs[0].head_span.start);
            try std.testing.expectEqual(@as(usize, 4), pairs[0].tail_span.start);
            try std.testing.expectEqual(@as(usize, 0), pairs[0].head_query);
            try std.testing.expectEqual(@as(usize, 1), pairs[0].tail_query);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

pub const Mention = struct { text: []const u8, start: usize, end: usize };
/// Call deduplicate separately for each ordinary relation type. JointIE keeps
/// occurrence-level graph identities and must use its own constrained decoder.
pub const Edge = struct { head: Mention, tail: Mention, probability: f32, source_index: usize = 0 };
pub const DedupOptions = struct {
    max_edges: usize = 512,
    max_text_bytes: usize = 1024 * 1024,
    max_comparisons: usize = 16 * 1024 * 1024,
    control: ?Control = null,
};
pub const Deduplicated = struct {
    allocator: std.mem.Allocator,
    arena: *std.heap.ArenaAllocator,
    edges: []Edge,
    comparisons: usize = 0,
    pub fn deinit(self: *Deduplicated) void {
        self.arena.deinit();
        self.allocator.destroy(self.arena);
        self.* = undefined;
    }
};
const Semantic = struct {
    edge: Edge,
    head: []const u8,
    tail: []const u8,
    head_tokens: []const []const u8,
    tail_tokens: []const []const u8,
    ordinal: usize,
    fn less(_: void, a: Semantic, b: Semantic) bool {
        if (a.edge.head.start != b.edge.head.start) return a.edge.head.start < b.edge.head.start;
        if (a.edge.tail.start != b.edge.tail.start) return a.edge.tail.start < b.edge.tail.start;
        if (a.edge.probability != b.edge.probability) return a.edge.probability > b.edge.probability;
        return a.ordinal < b.ordinal;
    }
};
const DedupWork = struct {
    options: DedupOptions,
    used: usize = 0,
    fn check(self: *DedupWork) !void {
        if (self.used >= self.options.max_comparisons) return error.RelationDedupLimitExceeded;
        self.used += 1;
        if (self.used % 256 == 0) if (self.options.control) |control| try control.check();
    }
};

fn sameCoordinates(a: Mention, b: Mention) bool {
    return a.start == b.start and a.end == b.end;
}
fn canonical(input: []const Edge, mention: Mention, comptime side: []const u8, work: *DedupWork) !Mention {
    var best = mention;
    for (input) |edge| {
        try work.check();
        const candidate = @field(edge, side);
        if (candidate.start > mention.start or candidate.end < mention.end) continue;
        const length = candidate.end - candidate.start;
        const best_length = best.end - best.start;
        // Equal coordinates use the last input surface, as the upstream dict.
        if (length > best_length or (length == best_length and candidate.start <= best.start)) best = candidate;
    }
    return best;
}

/// Python's full Unicode casefold followed by split/join whitespace collapse.
/// This is deliberately separate from tokenizer NFC and context-aware lower.
pub fn semanticText(a: std.mem.Allocator, text: []const u8) ![]u8 {
    return foldText(a, text, true);
}
pub fn fullCasefold(a: std.mem.Allocator, text: []const u8) ![]u8 {
    return foldText(a, text, false);
}
fn foldText(a: std.mem.Allocator, text: []const u8, collapse_whitespace: bool) ![]u8 {
    const data = @import("extraction_casefold_data.zig");
    const unicode = @import("../finetune/gliner2_unicode_tables.zig");
    var output = std.ArrayListUnmanaged(u8).empty;
    errdefer output.deinit(a);
    var view = (try std.unicode.Utf8View.init(text)).iterator();
    var pending_space = false;
    while (view.nextCodepoint()) |cp| {
        if (collapse_whitespace and unicode.isWhitespace(cp)) {
            pending_space = output.items.len > 0;
            continue;
        }
        if (pending_space) try output.append(a, ' ');
        pending_space = false;
        var lo: usize = 0;
        var hi: usize = data.entries.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (data.entries[mid].source < cp) lo = mid + 1 else hi = mid;
        }
        const unchanged = [_]u21{cp};
        const mapped: []const u21 = if (lo < data.entries.len and data.entries[lo].source == cp)
            data.entries[lo].target[0..data.entries[lo].len]
        else
            &unchanged;
        for (mapped) |value| {
            var bytes: [4]u8 = undefined;
            const len = try std.unicode.utf8Encode(value, &bytes);
            try output.appendSlice(a, bytes[0..len]);
        }
    }
    return output.toOwnedSlice(a);
}
fn tokens(a: std.mem.Allocator, text: []const u8) ![]const []const u8 {
    var list = std.ArrayListUnmanaged([]const u8).empty;
    errdefer list.deinit(a);
    var iter = std.mem.tokenizeScalar(u8, text, ' ');
    while (iter.next()) |value| try list.append(a, value);
    const Less = struct {
        fn less(_: void, l: []const u8, r: []const u8) bool {
            return std.mem.order(u8, l, r) == .lt;
        }
    };
    std.mem.sort([]const u8, list.items, {}, Less.less);
    var count: usize = 0;
    for (list.items) |value| {
        if (count > 0 and std.mem.eql(u8, list.items[count - 1], value)) continue;
        list.items[count] = value;
        count += 1;
    }
    list.shrinkRetainingCapacity(count);
    return list.toOwnedSlice(a);
}
fn subset(left: []const []const u8, right: []const []const u8, work: *DedupWork) !bool {
    if (left.len > right.len) return false;
    var j: usize = 0;
    for (left) |value| {
        while (j < right.len) : (j += 1) {
            try work.check();
            switch (std.mem.order(u8, value, right[j])) {
                .lt => return false,
                .eq => break,
                .gt => {},
            }
        }
        if (j == right.len) return false;
        j += 1;
    }
    return true;
}
fn prefer(a: Edge, b: Edge) bool {
    const ad = @max(a.head.start -| a.tail.end, a.tail.start -| a.head.end);
    const bd = @max(b.head.start -| b.tail.end, b.tail.start -| b.head.end);
    if (ad != bd) return ad < bd;
    if (a.probability != b.probability) return a.probability > b.probability;
    return if (a.head.start != b.head.start) a.head.start < b.head.start else a.tail.start < b.tail.start;
}

/// Returns fully owned surfaces and edges. Hard limits bound candidate count,
/// source text and comparison work; exhaustion never returns a partial set.
pub fn deduplicate(allocator: std.mem.Allocator, input: []const Edge, options: DedupOptions) !Deduplicated {
    if (options.control) |control| try control.check();
    if (input.len > options.max_edges) return error.RelationDedupLimitExceeded;
    var text_bytes: usize = 0;
    for (input) |edge| {
        if (!std.math.isFinite(edge.probability) or edge.probability < 0 or edge.probability > 1) return error.NonFiniteBoundaryScore;
        inline for (.{ edge.head, edge.tail }) |mention| {
            if (mention.end <= mention.start or !std.unicode.utf8ValidateSlice(mention.text)) return error.InvalidRelationMention;
            text_bytes = std.math.add(usize, text_bytes, mention.text.len) catch return error.RelationDedupLimitExceeded;
        }
    }
    if (text_bytes > options.max_text_bytes) return error.RelationDedupLimitExceeded;
    const arena = try allocator.create(std.heap.ArenaAllocator);
    arena.* = std.heap.ArenaAllocator.init(allocator);
    errdefer {
        arena.deinit();
        allocator.destroy(arena);
    }
    const a = arena.allocator();
    var work = DedupWork{ .options = options };
    var exact = std.ArrayListUnmanaged(Edge).empty;
    for (input) |edge| {
        var normalized = edge;
        normalized.head = try canonical(input, edge.head, "head", &work);
        normalized.tail = try canonical(input, edge.tail, "tail", &work);
        var duplicate = false;
        for (exact.items) |*previous| {
            try work.check();
            if (!sameCoordinates(previous.head, normalized.head) or !sameCoordinates(previous.tail, normalized.tail)) continue;
            if (edge.probability > previous.probability) previous.* = normalized;
            duplicate = true;
            break;
        }
        if (!duplicate) try exact.append(a, normalized);
    }
    var semantic = std.ArrayListUnmanaged(Semantic).empty;
    var normalized_source_bytes: usize = 0;
    for (exact.items) |edge| {
        normalized_source_bytes = std.math.add(usize, normalized_source_bytes, edge.head.text.len) catch return error.RelationDedupLimitExceeded;
        normalized_source_bytes = std.math.add(usize, normalized_source_bytes, edge.tail.text.len) catch return error.RelationDedupLimitExceeded;
        if (normalized_source_bytes > options.max_text_bytes) return error.RelationDedupLimitExceeded;
        const h = try semanticText(a, edge.head.text);
        const t = try semanticText(a, edge.tail.text);
        var duplicate = false;
        for (semantic.items) |*previous| {
            try work.check();
            if (!std.mem.eql(u8, previous.head, h) or !std.mem.eql(u8, previous.tail, t)) continue;
            if (prefer(edge, previous.edge)) previous.edge = edge;
            duplicate = true;
            break;
        }
        if (!duplicate) try semantic.append(a, .{ .edge = edge, .head = h, .tail = t, .head_tokens = try tokens(a, h), .tail_tokens = try tokens(a, t), .ordinal = semantic.items.len });
    }
    var kept = std.ArrayListUnmanaged(Semantic).empty;
    for (semantic.items, 0..) |item, i| {
        var dominated = false;
        for (semantic.items, 0..) |other, j| {
            if (i == j) continue;
            try work.check();
            const hh = item.head_tokens.len;
            const ht = item.tail_tokens.len;
            const oh = other.head_tokens.len;
            const ot = other.tail_tokens.len;
            if (((hh < oh and ht == ot) or (ht < ot and hh == oh)) and
                try subset(item.head_tokens, other.head_tokens, &work) and
                try subset(item.tail_tokens, other.tail_tokens, &work))
            {
                dominated = true;
                break;
            }
        }
        if (!dominated) try kept.append(a, item);
    }
    std.mem.sort(Semantic, kept.items, {}, Semantic.less);
    const result = try a.alloc(Edge, kept.items.len);
    for (kept.items, result) |item, *edge| {
        edge.* = item.edge;
        edge.head.text = try a.dupe(u8, item.edge.head.text);
        edge.tail.text = try a.dupe(u8, item.edge.tail.text);
    }
    if (options.control) |control| try control.check();
    return .{ .allocator = allocator, .arena = arena, .edges = result, .comparisons = work.used };
}

test "gliner boundary relation semantic Unicode folding and allocation cleanup" {
    const Check = struct {
        fn run(a: std.mem.Allocator) !void {
            const folded = try semanticText(a, "  STRAẞE\u{a0}\u{2003}Σςσ İ K ﬃ  ");
            defer a.free(folded);
            try std.testing.expectEqualStrings("strasse σσσ i\u{307} k ffi", folded);
            var result = try deduplicate(a, &.{
                .{ .head = .{ .text = "Straße", .start = 0, .end = 6 }, .tail = .{ .text = "Acme", .start = 100, .end = 104 }, .probability = 0.99 },
                .{ .head = .{ .text = "STRASSE", .start = 90, .end = 97 }, .tail = .{ .text = "ACME", .start = 100, .end = 104 }, .probability = 0.8 },
            }, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 1), result.edges.len);
            try std.testing.expectEqual(@as(usize, 90), result.edges[0].head.start);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}
