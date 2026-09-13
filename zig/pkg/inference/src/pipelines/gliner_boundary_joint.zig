// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! JointIE scoring adapter for Fastino 3c913c7. Relation proposals reuse the
//! shared pool, while every typed endpoint is force-scored by the separate
//! explicit-span head. Retained relations rescue their endpoints before entity
//! caps are applied. Neural calibration, proposal admission and graph utility
//! are separate contracts; no encoder is run by this module.
const std = @import("std");
const compute = @import("../ops/ops.zig");
const model = @import("../models/gliner_boundary.zig");
const head = @import("../architectures/gliner_boundary_head.zig");
const tasks = @import("../architectures/gliner_boundary_tasks.zig");
const Span = @import("../architectures/gliner_boundary_ops.zig").Span;
const processor = @import("gliner_boundary_processor.zig");
const schema_mod = @import("extraction_schema.zig");
const proposals = @import("gliner_boundary_relations.zig");
const joint = @import("extraction_joint_ie.zig");
const scoring = @import("gliner_boundary_scoring.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;

pub const Input = struct {
    prepared: *const processor.PreparedBatch,
    compiled: *const schema_mod.CompiledSchema,
    sample_index: usize,
    core: head.Input,
    scores: *const head.Result,
};
pub const ScoredInput = struct {
    prepared: *const processor.PreparedBatch,
    compiled: *const schema_mod.CompiledSchema,
    sample_index: usize,
    scores: scoring.CandidateScoreView,
};
pub const Options = struct {
    candidate_threshold: f64 = 0.05,
    /// Upstream's entity_threshold overrides candidate admission, not the
    /// decision threshold used to center a node's optimization utility.
    entity_threshold: ?f64 = null,
    relation_role_threshold: f64 = 0.05,
    top_k_entities: usize = 32,
    top_k_roles: usize = 12,
    relation_pair_cap: usize = 128,
    max_edges_per_type: usize = 256,
    rescue_relation_endpoints: bool = true,
    entity_weight: f64 = 1,
    relation_weight: f64 = 1,
    /// Add only after mapping model words to immutable original UTF-8 bytes.
    document_byte_offset: usize = 0,
    max_nodes: usize = 128,
    max_edges: usize = 256,
    max_raw_mentions: usize = 65536,
    max_raw_edges: usize = 65536,
    max_explicit_elements: usize = 65536,
    max_name_repr_bytes: usize = 1024 * 1024,
    max_work: usize = 10 * 1024 * 1024,
    proposal_limits: proposals.Options = .{},
    head_limits: head.Limits = .{},
    task_limits: tasks.Limits = .{},
    control: ?Control = null,
};
pub const MentionKey = struct { entity_type: usize, span: Span };
pub const MentionScore = struct {
    key: MentionKey,
    /// Already divided by pair_temperature. Explicit neural scores perform
    /// that division in FP32; shared-pool scores follow upstream's FP64 path.
    logit: f64,
    probability: ?f64 = null,
};
pub const ScoredEdge = struct {
    relation_type: usize,
    head: MentionKey,
    tail: MentionKey,
    /// Already divided by relation_temperature in FP32.
    logit: f64,
    probability: ?f64 = null,
};
pub const NodeMetadata = struct {
    token_span: Span,
    /// Low-probability retained edge endpoint; cap bypass alone is not rescue
    /// in the pinned upstream result contract.
    rescued: bool,
};
pub const Candidates = struct {
    allocator: Allocator,
    nodes: []joint.Node,
    edges: []joint.Edge,
    node_metadata: []NodeMetadata,
    source_spans: []joint.SourceSpan,
    pub fn sourceIdentity(self: *const Candidates) joint.SourceIdentity {
        return .{ .node_spans = self.source_spans };
    }
    pub fn deinit(self: *Candidates) void {
        self.allocator.free(self.nodes);
        self.allocator.free(self.edges);
        self.allocator.free(self.node_metadata);
        self.allocator.free(self.source_spans);
        self.* = undefined;
    }
};

fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b);
}
fn signed(value: usize) !i64 {
    return std.math.cast(i64, value) orelse error.InvalidJointScore;
}
fn probability(value: f64) bool {
    return std.math.isFinite(value) and value >= 0 and value <= 1;
}
fn sigmoid(value: f64) f64 {
    const exp = @exp(if (value >= 0) -value else value);
    return if (value >= 0) 1 / (1 + exp) else exp / (1 + exp);
}
fn centered(logit: f64, threshold: f64, weight: f64) !f64 {
    if (!probability(threshold) or threshold == 0 or threshold == 1) return error.InvalidJointDecisionThreshold;
    const result = weight * (logit - (@log(threshold) - std.math.log1p(-threshold)));
    if (!std.math.isFinite(result)) return error.NonFiniteJointUtility;
    return result;
}
const Work = struct {
    options: Options,
    steps: usize = 0,
    fn tick(self: *Work) !void {
        if (self.steps >= self.options.max_work) return error.JointScoringLimitExceeded;
        self.steps += 1;
        if (self.steps % 128 == 1) if (self.options.control) |control| try control.check();
    }
    fn check(self: *const Work) !void {
        if (self.options.control) |control| try control.check();
    }
};
fn validate(schema: schema_mod.JointSchema, options: Options) !void {
    if (!probability(options.candidate_threshold) or !probability(options.relation_role_threshold) or
        (options.entity_threshold != null and !probability(options.entity_threshold.?)) or
        !std.math.isFinite(options.entity_weight) or options.entity_weight < 0 or
        !std.math.isFinite(options.relation_weight) or options.relation_weight < 0)
        return error.InvalidJointScoringOptions;
    if (options.top_k_entities == 0 or options.top_k_roles == 0 or options.relation_pair_cap == 0 or
        options.max_edges_per_type == 0 or options.max_nodes > 256 or options.max_edges > 256)
        return error.JointScoringLimitExceeded;
    for (schema.entities) |entity| {
        _ = try centered(0, entity.threshold orelse 0.5, 1);
        if (entity.candidate_threshold) |value| if (!probability(value)) return error.InvalidJointScoringOptions;
        if (entity.max_candidates) |value| if (value == 0) return error.InvalidJointScoringOptions;
    }
    for (schema.relations) |relation| {
        _ = try centered(0, relation.threshold orelse 0.5, 1);
        if (relation.candidate_threshold) |value| if (!probability(value)) return error.InvalidJointScoringOptions;
        if (relation.head.len == 0 or relation.tail.len == 0) return error.InvalidJointScore;
        for (relation.head) |index| if (index >= schema.entities.len) return error.InvalidJointScore;
        for (relation.tail) |index| if (index >= schema.entities.len) return error.InvalidJointScore;
    }
    if (options.control) |control| try control.check();
}

fn source(sample: processor.Sample, span: Span) !?processor.ByteRange {
    if (span.end <= span.start or span.end > sample.words.len) return error.InvalidJointScore;
    if (span.start < sample.prefix_word_count) return null;
    const start = sample.words[span.start].source orelse return null;
    const end = sample.words[span.end - 1].source orelse return null;
    if (start.start >= end.end or end.end > sample.original_text.len) return error.InvalidJointScore;
    return .{ .start = start.start, .end = end.end };
}
const Mention = struct { score: MentionScore, probability: f64, bytes: processor.ByteRange };
const EdgeKey = struct { relation_type: usize, head: MentionKey, tail: MentionKey };
const Edge = struct { score: ScoredEdge, probability: f64, head: usize, tail: usize };
const Order = struct {
    schema: schema_mod.JointSchema,
    mentions: []const Mention,
    edges: []const Edge,
    name_reprs: []const []const u8,
    prefix: usize,

    fn mention(self: Order, left: usize, right: usize) bool {
        const a = self.mentions[left];
        const b = self.mentions[right];
        const names = std.mem.order(u8, self.schema.entities[a.score.key.entity_type].name, self.schema.entities[b.score.key.entity_type].name);
        if (names != .eq) return names == .lt;
        if (a.probability != b.probability) return a.probability > b.probability;
        if (a.score.key.span.start != b.score.key.span.start) return a.score.key.span.start < b.score.key.span.start;
        return a.score.key.span.end < b.score.key.span.end;
    }
    fn key(self: Order, a: MentionKey, b: MentionKey) std.math.Order {
        const names = std.mem.order(u8, self.name_reprs[a.entity_type], self.name_reprs[b.entity_type]);
        if (names != .eq) return names;
        // candidate_score_set_to_problem sorts str((type,start,end)), not the
        // numeric tuple. A decimal prefix ends with ',' or ')' before digits.
        var ab: [32]u8 = undefined;
        var bb: [32]u8 = undefined;
        const astart = std.fmt.bufPrint(&ab, "{d}", .{a.span.start - self.prefix}) catch unreachable;
        const bstart = std.fmt.bufPrint(&bb, "{d}", .{b.span.start - self.prefix}) catch unreachable;
        const starts = std.mem.order(u8, astart, bstart);
        if (starts != .eq) return starts;
        const aend = std.fmt.bufPrint(&ab, "{d}", .{a.span.end - self.prefix}) catch unreachable;
        const bend = std.fmt.bufPrint(&bb, "{d}", .{b.span.end - self.prefix}) catch unreachable;
        return std.mem.order(u8, aend, bend);
    }
    fn edge(self: Order, left: usize, right: usize) bool {
        const a = self.edges[left].score;
        const b = self.edges[right].score;
        const names = std.mem.order(u8, self.schema.relations[a.relation_type].name, self.schema.relations[b.relation_type].name);
        if (names != .eq) return names == .lt;
        if (a.logit != b.logit) return a.logit > b.logit;
        const heads = self.key(a.head, b.head);
        if (heads != .eq) return heads == .lt;
        return self.key(a.tail, b.tail) == .lt;
    }
};

/// Pure candidate finalization, also used by native reference fixtures. Every
/// sourceful edge endpoint must have an explicit or shared-pool MentionScore.
/// Contract caps can drop proposals; hard allocation/search bounds only fail.
pub fn fromScores(allocator: Allocator, schema: schema_mod.JointSchema, sample: processor.Sample, raw_mentions: []const MentionScore, raw_edges: []const ScoredEdge, options: Options) !Candidates {
    try validate(schema, options);
    if (raw_mentions.len > options.max_raw_mentions or raw_edges.len > options.max_raw_edges) return error.JointScoringLimitExceeded;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const scratch = arena.allocator();
    var work = Work{ .options = options };
    var mention_map = std.AutoHashMapUnmanaged(MentionKey, usize).empty;
    var mentions = std.ArrayListUnmanaged(Mention).empty;
    for (raw_mentions) |raw| {
        try work.tick();
        if (raw.key.entity_type >= schema.entities.len or !std.math.isFinite(raw.logit)) return error.InvalidJointScore;
        const p = raw.probability orelse sigmoid(raw.logit);
        if (!probability(p)) return error.InvalidJointScore;
        const bytes = (try source(sample, raw.key.span)) orelse continue;
        const entry = try mention_map.getOrPut(scratch, raw.key);
        if (entry.found_existing) {
            const old = &mentions.items[entry.value_ptr.*];
            if (raw.logit > old.score.logit) old.* = .{ .score = raw, .probability = p, .bytes = bytes };
        } else {
            entry.value_ptr.* = mentions.items.len;
            try mentions.append(scratch, .{ .score = raw, .probability = p, .bytes = bytes });
        }
    }
    var edge_map = std.AutoHashMapUnmanaged(EdgeKey, usize).empty;
    var edges = std.ArrayListUnmanaged(Edge).empty;
    for (raw_edges) |raw| {
        try work.tick();
        if (raw.relation_type >= schema.relations.len or raw.head.entity_type >= schema.entities.len or raw.tail.entity_type >= schema.entities.len or !std.math.isFinite(raw.logit)) return error.InvalidJointScore;
        const p = raw.probability orelse sigmoid(raw.logit);
        if (!probability(p)) return error.InvalidJointScore;
        const relation = schema.relations[raw.relation_type];
        if (std.mem.indexOfScalar(usize, relation.head, raw.head.entity_type) == null or std.mem.indexOfScalar(usize, relation.tail, raw.tail.entity_type) == null) return error.InvalidJointScore;
        if (try source(sample, raw.head.span) == null or try source(sample, raw.tail.span) == null) continue;
        if (p < (relation.candidate_threshold orelse options.relation_role_threshold)) continue;
        const h = mention_map.get(raw.head) orelse return error.MissingJointEndpointScore;
        const t = mention_map.get(raw.tail) orelse return error.MissingJointEndpointScore;
        const entry = try edge_map.getOrPut(scratch, .{ .relation_type = raw.relation_type, .head = raw.head, .tail = raw.tail });
        if (entry.found_existing) {
            const old = &edges.items[entry.value_ptr.*];
            if (raw.logit > old.score.logit) old.* = .{ .score = raw, .probability = p, .head = h, .tail = t };
        } else {
            entry.value_ptr.* = edges.items.len;
            try edges.append(scratch, .{ .score = raw, .probability = p, .head = h, .tail = t });
        }
    }
    const reprs = try scratch.alloc([]const u8, schema.entities.len);
    var repr_bytes: usize = 0;
    for (schema.entities, reprs) |entity, *repr| {
        repr.* = try pythonRepr(scratch, entity.name, options.max_name_repr_bytes -| repr_bytes);
        repr_bytes = try std.math.add(usize, repr_bytes, repr.len);
    }
    const order = Order{ .schema = schema, .mentions = mentions.items, .edges = edges.items, .name_reprs = reprs, .prefix = sample.prefix_word_count };
    const edge_order = try scratch.alloc(usize, edges.items.len);
    for (edge_order, 0..) |*index, i| index.* = i;
    std.mem.sort(usize, edge_order, order, Order.edge);
    try work.check();
    const rescued = try scratch.alloc(bool, mentions.items.len);
    @memset(rescued, false);
    const edge_counts = try scratch.alloc(usize, schema.relations.len);
    @memset(edge_counts, 0);
    const edge_cap = @min(options.relation_pair_cap, options.max_edges_per_type);
    var retained = std.ArrayListUnmanaged(usize).empty;
    for (edge_order) |index| {
        try work.tick();
        const edge = edges.items[index];
        if (edge_counts[edge.score.relation_type] >= edge_cap) continue;
        edge_counts[edge.score.relation_type] += 1;
        try retained.append(scratch, index);
        if (options.rescue_relation_endpoints) {
            rescued[edge.head] = true;
            rescued[edge.tail] = true;
        }
    }
    const mention_order = try scratch.alloc(usize, mentions.items.len);
    for (mention_order, 0..) |*index, i| index.* = i;
    std.mem.sort(usize, mention_order, order, Order.mention);
    try work.check();
    const node_counts = try scratch.alloc(usize, schema.entities.len);
    @memset(node_counts, 0);
    const remap = try scratch.alloc(?usize, mentions.items.len);
    @memset(remap, null);
    var nodes = std.ArrayListUnmanaged(joint.Node).empty;
    var metadata = std.ArrayListUnmanaged(NodeMetadata).empty;
    for (mention_order) |index| {
        try work.tick();
        const item = mentions.items[index];
        const kind = item.score.key.entity_type;
        const spec = schema.entities[kind];
        const floor = spec.candidate_threshold orelse options.entity_threshold orelse options.candidate_threshold;
        if (!rescued[index] and (item.probability < floor or node_counts[kind] >= (spec.max_candidates orelse options.top_k_entities))) continue;
        if (nodes.items.len >= options.max_nodes) return error.JointCandidateLimitExceeded;
        node_counts[kind] += 1;
        remap[index] = nodes.items.len;
        try nodes.append(scratch, .{ .entity_type = kind, .start = try std.math.add(usize, options.document_byte_offset, item.bytes.start), .end = try std.math.add(usize, options.document_byte_offset, item.bytes.end), .utility = try centered(item.score.logit, spec.threshold orelse 0.5, options.entity_weight), .probability = item.probability });
        try metadata.append(scratch, .{ .token_span = item.score.key.span, .rescued = rescued[index] and item.probability < floor });
    }
    var selected_edges = std.ArrayListUnmanaged(joint.Edge).empty;
    for (retained.items, 0..) |index, slot| {
        try work.tick();
        const edge = edges.items[index];
        const h = remap[edge.head] orelse continue;
        const t = remap[edge.tail] orelse continue;
        if (selected_edges.items.len >= options.max_edges) return error.JointCandidateLimitExceeded;
        const relation = schema.relations[edge.score.relation_type];
        try selected_edges.append(scratch, .{ .relation_type = edge.score.relation_type, .head = h, .tail = t, .utility = try centered(edge.score.logit, relation.threshold orelse 0.5, options.relation_weight), .probability = edge.probability, .slot = @intCast(slot), .hypothesis = @intCast(edge.score.relation_type) });
    }
    const owned_nodes = try allocator.dupe(joint.Node, nodes.items);
    errdefer allocator.free(owned_nodes);
    const owned_edges = try allocator.dupe(joint.Edge, selected_edges.items);
    errdefer allocator.free(owned_edges);
    const owned_metadata = try allocator.dupe(NodeMetadata, metadata.items);
    errdefer allocator.free(owned_metadata);
    const source_spans = try allocator.alloc(joint.SourceSpan, metadata.items.len);
    errdefer allocator.free(source_spans);
    for (metadata.items, source_spans) |item, *span| span.* = .{
        .start = item.token_span.start - sample.prefix_word_count,
        .end = item.token_span.end - sample.prefix_word_count,
    };
    try work.check();
    return .{ .allocator = allocator, .nodes = owned_nodes, .edges = owned_edges, .node_metadata = owned_metadata, .source_spans = source_spans };
}

fn findQuery(sample: processor.Sample, kind: processor.QueryKind, schema_index: usize) !usize {
    var result: ?usize = null;
    for (sample.queries, 0..) |query, q| if (query.kind == kind and query.schema_index == schema_index) {
        if (result != null) return error.InvalidJointRouting;
        result = q;
    };
    return result orelse error.InvalidJointRouting;
}
fn finite(values: []const f32, length: usize) !void {
    if (values.len != length) return error.InvalidJointRouting;
    for (values) |value| if (!std.math.isFinite(value)) return error.NonFiniteBoundaryScore;
}

/// Assemble one document's sparse score set from an already encoded batch.
/// The source batch width is retained for relation distance normalization.
pub fn buildNative(cb: *const compute.ComputeBackend, allocator: Allocator, config: *const model.Config, input: Input, options_: Options) !Candidates {
    var options = options_;
    options.control = options.control orelse input.core.control;
    if (cb.kind() != .native) return error.UnsupportedGlinerBoundaryBackend;
    try cb.checkExecutionControl();
    if (options.control) |control| try control.check();
    const core = input.core;
    const h: usize = config.encoder.hidden_size;
    if (input.sample_index >= input.prepared.samples.len or core.batch != input.prepared.samples.len or core.text_length != input.prepared.word_width or core.queries != input.prepared.query_width or
        core.text_lengths.len != core.batch or core.query_mask.len != try mul(core.batch, core.queries) or
        !std.mem.eql(bool, core.query_mask, input.prepared.query_marker_mask)) return error.InvalidJointRouting;
    if (input.prepared.samples[input.sample_index].words.len != core.text_lengths[input.sample_index]) return error.InvalidJointRouting;
    try finite(core.text_states, try mul(try mul(core.batch, core.text_length), h));
    try finite(core.query_states, try mul(try mul(core.batch, core.queries), h));
    var native = scoring.NativeContext{ .cb = cb, .config = config, .prepared = input.prepared, .core = .{ .text_states = core.text_states, .query_states = core.query_states, .classification_states = &.{}, .text_lengths = core.text_lengths }, .scores = input.scores };
    return buildScored(allocator, config, .{ .prepared = input.prepared, .compiled = input.compiled, .sample_index = input.sample_index, .scores = scoring.CandidateScoreView.fromNative(input.scores) }, native.scorer(), options);
}

/// Candidate admission/calibration is shared across CPU and resident devices.
/// Callbacks expose final scores; hidden encoder/candidate states stay private.
pub fn buildScored(allocator: Allocator, config: *const model.Config, input: ScoredInput, scorer: scoring.Scorer, options_: Options) !Candidates {
    var options = options_;
    const schema = input.compiled.schema.joint_ie orelse return error.InvalidJointRouting;
    try validate(schema, options);
    if (config.version != model.config_version or config.architecture_version != model.architecture_version or config.encoder.hidden_size == 0) return error.UnsupportedGlinerBoundaryVersion;
    try config.head.validate();
    try scorer.check();
    const prepared = input.prepared;
    const scores = input.scores;
    const b = input.sample_index;
    const batch = prepared.samples.len;
    const l = prepared.word_width;
    const q = prepared.query_width;
    const c = scores.pool.capacity;
    if (batch > options.head_limits.max_batch or l > options.head_limits.max_text_words or q > options.head_limits.max_queries) return error.JointScoringLimitExceeded;
    if (b >= batch or scores.batch != batch or scores.text_length != l or scores.queries != q or scores.pool.batch != batch or
        prepared.query_marker_mask.len != try mul(batch, q)) return error.InvalidJointRouting;
    const sample = prepared.samples[b];
    if (!sample.is_joint_ie or !std.mem.eql(u8, &sample.schema_fingerprint, &input.compiled.fingerprint) or sample.words.len > l) return error.InvalidJointRouting;
    const pair_count = try mul(try mul(batch, q), c);
    if (pair_count > options.head_limits.max_pool_pair_elements) return error.JointScoringLimitExceeded;
    try finite(scores.pair_logits, pair_count);
    if (scores.pool.indices.len != try mul(batch, c) or scores.pool.valid.len != try mul(batch, c)) return error.InvalidJointRouting;
    if (!std.math.isFinite(config.head.pair_temperature) or config.head.pair_temperature <= 0 or
        !std.math.isFinite(config.head.relation_temperature) or config.head.relation_temperature <= 0) return error.InvalidGlinerBoundaryConfig;
    if (schema.relations.len > 0 and !config.head.enable_relations) return error.UnsupportedGlinerBoundaryTask;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const scratch = arena.allocator();
    var work = Work{ .options = options };
    var mentions = std.ArrayListUnmanaged(MentionScore).empty;
    const entity_queries = try scratch.alloc(usize, schema.entities.len);
    for (schema.entities, entity_queries, 0..) |_, *query, entity| {
        query.* = try findQuery(sample, .entity, entity);
        if (query.* >= q or !prepared.query_marker_mask[b * q + query.*]) return error.InvalidJointRouting;
        for (0..c) |candidate| {
            try work.tick();
            if (!scores.pool.valid[b * c + candidate]) continue;
            const span = scores.pool.indices[b * c + candidate];
            if (try source(sample, span) == null) continue;
            if (mentions.items.len >= options.max_raw_mentions) return error.JointScoringLimitExceeded;
            try mentions.append(scratch, .{ .key = .{ .entity_type = entity, .span = span }, .logit = @as(f64, scores.pair_logits[(b * q + query.*) * c + candidate]) / @as(f64, config.head.pair_temperature) });
        }
    }
    var edges = std.ArrayListUnmanaged(ScoredEdge).empty;
    if (schema.relations.len > 0) {
        const routes = try scratch.alloc(proposals.Route, schema.relations.len);
        const role_queries = try scratch.alloc([2]usize, routes.len);
        for (schema.relations, routes, role_queries, 0..) |relation, *route, *roles, r| {
            try work.tick();
            roles.* = .{ try findQuery(sample, .relation_head, r), try findQuery(sample, .relation_tail, r) };
            for (roles) |query| if (query >= q or !prepared.query_marker_mask[b * q + query]) return error.InvalidJointRouting;
            route.* = .{ .batch_index = 0, .relation_index = r, .head_queries = roles[0..1], .tail_queries = roles[1..2], .allow_self = relation.allow_self };
        }
        var proposal_options = options.proposal_limits;
        proposal_options.heads_per_relation = @min(config.head.relation_heads_per_type, options.top_k_roles);
        proposal_options.tails_per_relation = @min(config.head.relation_tails_per_type, options.top_k_roles);
        proposal_options.pair_cap = @min(config.head.relation_pair_cap, options.relation_pair_cap);
        proposal_options.argument_threshold = config.head.relation_argument_proposal_threshold;
        proposal_options.control = options.control;
        const pairs = try proposals.generate(scratch, .{ .batch = 1, .queries = q, .capacity = c, .spans = scores.pool.indices[b * c ..][0..c], .valid = scores.pool.valid[b * c ..][0..c], .query_mask = prepared.query_marker_mask[b * q ..][0..q], .logits = scores.pair_logits[b * q * c ..][0 .. q * c] }, routes, proposal_options);
        if (pairs.len > 0) {
            var span_map = std.AutoHashMapUnmanaged(Span, usize).empty;
            var spans = std.ArrayListUnmanaged(Span).empty;
            var requirements = std.AutoHashMapUnmanaged(MentionKey, void).empty;
            const neural_pairs = try scratch.alloc(tasks.RelationPair, pairs.len);
            var typed_edge_count: usize = 0;
            for (pairs, neural_pairs) |pair, *neural| {
                try work.tick();
                neural.* = .{ .batch_index = 0, .relation_index = try signed(pair.relation_index), .head_span = .{ .start = try signed(pair.head_span.start), .end = try signed(pair.head_span.end) }, .tail_span = .{ .start = try signed(pair.tail_span.start), .end = try signed(pair.tail_span.end) } };
                for ([_]Span{ pair.head_span, pair.tail_span }) |span| {
                    const entry = try span_map.getOrPut(scratch, span);
                    if (!entry.found_existing) {
                        entry.value_ptr.* = spans.items.len;
                        try spans.append(scratch, span);
                        if (spans.items.len > options.head_limits.max_explicit_candidates or try mul(entity_queries.len, spans.items.len) > options.max_explicit_elements) return error.JointScoringLimitExceeded;
                    }
                }
                const relation = schema.relations[pair.relation_index];
                if (try source(sample, pair.head_span) != null and try source(sample, pair.tail_span) != null) {
                    typed_edge_count = try std.math.add(usize, typed_edge_count, try mul(relation.head.len, relation.tail.len));
                    if (typed_edge_count > options.max_raw_edges) return error.JointScoringLimitExceeded;
                }
                for (relation.head) |entity| {
                    try work.tick();
                    try requirements.put(scratch, .{ .entity_type = entity, .span = pair.head_span }, {});
                    if (try std.math.add(usize, mentions.items.len, requirements.count()) > options.max_raw_mentions) return error.JointScoringLimitExceeded;
                }
                for (relation.tail) |entity| {
                    try work.tick();
                    try requirements.put(scratch, .{ .entity_type = entity, .span = pair.tail_span }, {});
                    if (try std.math.add(usize, mentions.items.len, requirements.count()) > options.max_raw_mentions) return error.JointScoringLimitExceeded;
                }
            }
            const explicit_count = try mul(entity_queries.len, spans.items.len);
            if (explicit_count > options.max_explicit_elements or spans.items.len > options.head_limits.max_explicit_candidates or
                try std.math.add(usize, mentions.items.len, requirements.count()) > options.max_raw_mentions) return error.JointScoringLimitExceeded;
            const indices = try scratch.alloc(head.SignedSpan, explicit_count);
            for (0..entity_queries.len) |entity| {
                for (spans.items, 0..) |span, column| indices[entity * spans.items.len + column] = .{ .start = try signed(span.start), .end = try signed(span.end) };
            }
            var relation_scores = try scorer.relations(allocator, .{ .sample_index = b, .query_pairs = role_queries, .pairs = neural_pairs }, options.task_limits, options.control);
            defer relation_scores.deinit();
            try finite(relation_scores.logits, pairs.len);
            if (relation_scores.valid.len != pairs.len) return error.InvalidBoundaryScorerOutput;
            var endpoint_scores = try scorer.explicit(allocator, .{ .sample_index = b, .query_ids = entity_queries, .capacity = spans.items.len, .indices = indices }, options.head_limits, options.control);
            defer endpoint_scores.deinit();
            try finite(endpoint_scores.logits, explicit_count);
            if (endpoint_scores.valid.len != explicit_count) return error.InvalidBoundaryScorerOutput;
            // Stable entity/coordinate order is independent of hash iteration.
            const required_keys = try scratch.alloc(MentionKey, requirements.count());
            var iterator = requirements.keyIterator();
            var count: usize = 0;
            while (iterator.next()) |key| : (count += 1) required_keys[count] = key.*;
            std.mem.sort(MentionKey, required_keys, schema, struct {
                fn less(s: schema_mod.JointSchema, a: MentionKey, z: MentionKey) bool {
                    const names = std.mem.order(u8, s.entities[a.entity_type].name, s.entities[z.entity_type].name);
                    if (names != .eq) return names == .lt;
                    return if (a.span.start != z.span.start) a.span.start < z.span.start else a.span.end < z.span.end;
                }
            }.less);
            for (required_keys) |key| {
                try work.tick();
                if (try source(sample, key.span) == null) continue;
                const index = key.entity_type * spans.items.len + span_map.get(key.span).?;
                if (!endpoint_scores.valid[index]) return error.InvalidJointRouting;
                try mentions.append(scratch, .{ .key = key, .logit = @as(f64, endpoint_scores.logits[index] / config.head.pair_temperature) });
            }
            for (pairs, 0..) |pair, index| {
                try work.tick();
                if (try source(sample, pair.head_span) == null or try source(sample, pair.tail_span) == null) continue;
                if (!relation_scores.valid[index]) return error.InvalidJointRouting;
                const relation = schema.relations[pair.relation_index];
                for (relation.head) |head_type| for (relation.tail) |tail_type| {
                    if (edges.items.len >= options.max_raw_edges) return error.JointScoringLimitExceeded;
                    try edges.append(scratch, .{ .relation_type = pair.relation_index, .head = .{ .entity_type = head_type, .span = pair.head_span }, .tail = .{ .entity_type = tail_type, .span = pair.tail_span }, .logit = @as(f64, relation_scores.logits[index] / config.head.relation_temperature) });
                };
            }
        }
    }
    try scorer.check();
    try work.check();
    options.max_work -= work.steps;
    return fromScores(allocator, schema, sample, mentions.items, edges.items, options);
}

const test_text = "a a a a a a a a a a a a a a a a";
fn testSample(words: []processor.Word) processor.Sample {
    for (words, 0..) |*word, i| word.* = .{ .text = "a", .source = .{ .start = 2 * i, .end = 2 * i + 1 }, .input_start = i, .input_end = i + 1 };
    return .{ .original_text = test_text, .schema_fingerprint = .{0} ** 32, .input_ids = &.{}, .words = words, .groups = &.{}, .queries = &.{}, .classification_labels = &.{}, .enum_choices = &.{}, .prefix_word_count = 0, .body_word_count = words.len, .terminal_period_added = false, .is_joint_ie = true };
}
fn testMention(entity: usize, start: usize, logit: f64) MentionScore {
    return .{ .key = .{ .entity_type = entity, .span = .{ .start = start, .end = start + 1 } }, .logit = logit };
}
fn testEdge(h: MentionScore, t: MentionScore, logit: f64) ScoredEdge {
    return .{ .relation_type = 0, .head = h.key, .tail = t.key, .logit = logit };
}
const test_schema = "{\"joint_ie\":{\"entities\":{\"person\":{},\"company\":{}},\"relations\":{\"works_for\":{\"head\":[\"person\"],\"tail\":[\"company\"]}}}}";

test "gliner joint adapter retains edge endpoints beyond entity caps with centered utilities" {
    const a = std.testing.allocator;
    var compiled = try schema_mod.compile(a, "{\"joint_ie\":{\"entities\":{\"person\":{\"threshold\":0.75,\"candidate_threshold\":0.5,\"max_candidates\":1},\"company\":{\"candidate_threshold\":0.5}},\"relations\":{\"works_for\":{\"head\":[\"person\"],\"tail\":[\"company\"],\"threshold\":0.8}}}}", .{});
    defer compiled.deinit();
    var words: [16]processor.Word = undefined;
    const sample = testSample(&words);
    const mentions = [_]MentionScore{ testMention(0, 0, 2), testMention(0, 1, 1), testMention(0, 2, -3), testMention(1, 3, -0.5) };
    const edges = [_]ScoredEdge{ testEdge(mentions[2], mentions[3], 5), testEdge(mentions[1], mentions[3], 4), testEdge(mentions[2], mentions[3], 6) };
    var output = try fromScores(a, compiled.schema.joint_ie.?, sample, &mentions, &edges, .{ .relation_pair_cap = 1, .entity_weight = 2, .relation_weight = 3, .document_byte_offset = 100 });
    defer output.deinit();
    try std.testing.expectEqual(@as(usize, 3), output.nodes.len);
    try std.testing.expectEqual(@as(usize, 1), output.edges.len);
    try std.testing.expectEqual(@as(usize, 1), output.nodes[0].entity_type); // alphabetical company
    try std.testing.expectEqual(@as(usize, 106), output.nodes[0].start);
    try std.testing.expectEqual(@as(usize, 107), output.nodes[0].end);
    try std.testing.expectApproxEqAbs(@as(f64, -1), output.nodes[0].utility, 1e-12);
    try std.testing.expectApproxEqAbs(2 * (@as(f64, 2) - @log(@as(f64, 3))), output.nodes[1].utility, 1e-12);
    try std.testing.expectApproxEqAbs(2 * (@as(f64, -3) - @log(@as(f64, 3))), output.nodes[2].utility, 1e-12);
    try std.testing.expectApproxEqAbs(3 * (@as(f64, 6) - @log(@as(f64, 4))), output.edges[0].utility, 1e-12);
    try std.testing.expect(output.node_metadata[0].rescued and !output.node_metadata[1].rescued and output.node_metadata[2].rescued);
    try std.testing.expectEqual(@as(usize, 2), output.edges[0].head);
    try std.testing.expectEqual(@as(usize, 0), output.edges[0].tail);
    try std.testing.expectEqual(@as(?u64, 0), output.edges[0].slot);
    try std.testing.expectEqual(@as(?u64, 0), output.edges[0].hypothesis);
    var selected = try joint.decode(a, compiled.schema.joint_ie.?, output.nodes, output.edges, .{ .algorithm = .exact });
    defer selected.deinit();
    try std.testing.expectEqual(@as(usize, 1), selected.edges.len);
    try std.testing.expect(try joint.validateGlobal(a, compiled.schema.joint_ie.?, selected.nodes, selected.edges, .{}));
}

test "gliner joint adapter matches tuple repr ties and admission overrides" {
    const a = std.testing.allocator;
    var compiled = try schema_mod.compile(a, test_schema, .{});
    defer compiled.deinit();
    var words: [16]processor.Word = undefined;
    const sample = testSample(&words);
    const mentions = [_]MentionScore{ testMention(0, 2, -1), testMention(0, 10, -1), testMention(1, 15, 1) };
    const edges = [_]ScoredEdge{ testEdge(mentions[0], mentions[2], 1), testEdge(mentions[1], mentions[2], 1) };
    var output = try fromScores(a, compiled.schema.joint_ie.?, sample, &mentions, &edges, .{ .entity_threshold = 0.9, .relation_pair_cap = 1 });
    defer output.deinit();
    try std.testing.expectEqual(@as(usize, 2), output.nodes.len);
    try std.testing.expectEqual(@as(usize, 10), output.node_metadata[output.edges[0].head].token_span.start);
    try std.testing.expect(output.node_metadata[0].rescued and output.node_metadata[1].rescued);
    var no_rescue = try fromScores(a, compiled.schema.joint_ie.?, sample, &mentions, &edges, .{ .entity_threshold = 0.9, .rescue_relation_endpoints = false });
    defer no_rescue.deinit();
    try std.testing.expectEqual(@as(usize, 0), no_rescue.nodes.len);
    try std.testing.expectEqual(@as(usize, 0), no_rescue.edges.len);
}

test "gliner joint adapter source mapping excludes enum prefix and synthetic suffix" {
    const a = std.testing.allocator;
    var compiled = try schema_mod.compile(a, test_schema, .{});
    defer compiled.deinit();
    var words: [3]processor.Word = undefined;
    var sample = testSample(&words);
    sample.original_text = "ß";
    sample.prefix_word_count = 1;
    words[0].source = null;
    words[1].source = .{ .start = 0, .end = 2 };
    words[2].source = null;
    const mentions = [_]MentionScore{ testMention(0, 0, 100), testMention(0, 1, 1), testMention(0, 2, 100) };
    var output = try fromScores(a, compiled.schema.joint_ie.?, sample, &mentions, &.{}, .{ .document_byte_offset = 100 });
    defer output.deinit();
    try std.testing.expectEqual(@as(usize, 1), output.nodes.len);
    try std.testing.expectEqual(@as(usize, 100), output.nodes[0].start);
    try std.testing.expectEqual(@as(usize, 102), output.nodes[0].end);
    try std.testing.expectEqual(Span{ .start = 1, .end = 2 }, output.node_metadata[0].token_span);
    try std.testing.expectEqual(joint.SourceSpan{ .start = 0, .end = 1 }, output.sourceIdentity().node_spans[0]);
}

test "gliner joint adapter rejects malformed nonfinite and overbudget score sets" {
    const a = std.testing.allocator;
    var compiled = try schema_mod.compile(a, test_schema, .{});
    defer compiled.deinit();
    var words: [16]processor.Word = undefined;
    const sample = testSample(&words);
    const mentions = [_]MentionScore{ testMention(0, 0, 1), testMention(1, 1, 1) };
    const edges = [_]ScoredEdge{testEdge(mentions[0], mentions[1], 1)};
    const schema = compiled.schema.joint_ie.?;
    try std.testing.expectError(error.JointCandidateLimitExceeded, fromScores(a, schema, sample, &mentions, &edges, .{ .max_nodes = 1 }));
    try std.testing.expectError(error.JointCandidateLimitExceeded, fromScores(a, schema, sample, &mentions, &edges, .{ .max_edges = 0 }));
    try std.testing.expectError(error.JointScoringLimitExceeded, fromScores(a, schema, sample, &mentions, &edges, .{ .max_raw_mentions = 1 }));
    try std.testing.expectError(error.JointScoringLimitExceeded, fromScores(a, schema, sample, &mentions, &edges, .{ .max_work = 0 }));
    try std.testing.expectError(error.InvalidJointScore, fromScores(a, schema, sample, &.{testMention(0, 0, std.math.nan(f64))}, &.{}, .{}));
    try std.testing.expectError(error.MissingJointEndpointScore, fromScores(a, schema, sample, mentions[0..1], &edges, .{}));
    try std.testing.expectError(error.InvalidJointScore, fromScores(a, schema, sample, &mentions, &.{testEdge(mentions[1], mentions[0], 1)}, .{}));
    try std.testing.expectError(error.InvalidJointDecisionThreshold, schema_mod.compile(a, "{\"joint_ie\":{\"entities\":{\"person\":{\"threshold\":0}}}}", .{}));
    // Direct typed callers still receive the same failure if they bypass the
    // JSON compiler; the scoring boundary must validate its own threshold.
    const invalid_entities = [_]schema_mod.JointEntity{.{ .name = "person", .threshold = 0 }};
    const invalid_schema = schema_mod.JointSchema{ .entities = &invalid_entities, .relations = &.{}, .constraints = &.{} };
    try std.testing.expectError(error.InvalidJointDecisionThreshold, fromScores(a, invalid_schema, sample, &.{}, &.{}, .{}));
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, fromScores(a, schema, sample, &mentions, &edges, .{ .control = .{ .check_fn = Cancel.check } }));
}

test "gliner joint adapter releases every allocation on finalization failures" {
    var compiled = try schema_mod.compile(std.testing.allocator, test_schema, .{});
    defer compiled.deinit();
    const Check = struct {
        fn run(a: Allocator, schema: schema_mod.JointSchema) !void {
            var words: [16]processor.Word = undefined;
            const sample = testSample(&words);
            const mentions = [_]MentionScore{ testMention(0, 0, -1), testMention(1, 1, 2), testMention(0, 2, 1) };
            const edges = [_]ScoredEdge{testEdge(mentions[0], mentions[1], 3)};
            var output = try fromScores(a, schema, sample, &mentions, &edges, .{});
            defer output.deinit();
            try std.testing.expectEqual(@as(usize, 3), output.nodes.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{compiled.schema.joint_ie.?});
}

test "gliner joint adapter uses pinned Unicode 15 Python repr for stable edge ties" {
    const cases = [_][2][]const u8{
        .{ "O'Neil", "\"O'Neil\"" },
        .{ "a\"b", "'a\"b'" },
        .{ "a\t\n\r\\'\"", "'a\\t\\n\\r\\\\\\'\"'" },
        .{ "a\u{a0}é\u{200b}\u{f0000}", "'a\\xa0é\\u200b\\U000f0000'" },
        .{ "😀\u{378}", "'😀\\u0378'" },
    };
    for (cases) |case| {
        const repr = try pythonRepr(std.testing.allocator, case[0], 1024);
        defer std.testing.allocator.free(repr);
        try std.testing.expectEqualStrings(case[1], repr);
    }
    try std.testing.expectError(error.JointScoringLimitExceeded, pythonRepr(std.testing.allocator, "xx", 3));
}

test "gliner joint adapter matches pinned Python sparse candidate admission goldens" {
    const fixtures = @import("../architectures/gliner_boundary_parity_test.zig");
    const ExpectedNode = struct { entity_type: usize, start: usize, end: usize, utility: f64, probability: f64, token_span: Span, rescued: bool };
    const ExpectedEdge = struct { relation_type: usize, head: usize, tail: usize, utility: f64, probability: f64, slot: u64, hypothesis: u64 };
    const Case = struct {
        id: []const u8,
        text: []const u8,
        entity_names: []const []const u8,
        relation_names: []const []const u8,
        entity_thresholds: []const f64,
        relation_thresholds: []const f64,
        mentions: []const MentionScore,
        edges: []const ScoredEdge,
        options: struct {
            mention_threshold: f64,
            max_mentions_per_type: usize,
            max_mentions_by_type: struct { person: ?usize = null, organization: ?usize = null } = .{},
            rescue_relation_endpoints: bool,
            edge_candidate_threshold: f64,
            max_edges_per_type: usize,
            entity_weight: f64,
            relation_weight: f64,
        },
        expected: struct { nodes: []const ExpectedNode, edges: []const ExpectedEdge },
    };
    const Capture = struct { format_version: u32, provenance: struct { commit: []const u8 }, cases: []const Case };
    const a = std.testing.allocator;
    const bytes = try fixtures.fixtureBytes(a, "joint_candidates.json");
    defer a.free(bytes);
    const parsed = try std.json.parseFromSlice(Capture, a, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u32, 1), parsed.value.format_version);
    try std.testing.expectEqualStrings("3c913c7369301133d3b7699252074c4303ada50e", parsed.value.provenance.commit);
    try std.testing.expectEqual(@as(usize, 9), parsed.value.cases.len);
    for (parsed.value.cases) |case| {
        const entities = [_]schema_mod.JointEntity{
            .{ .name = case.entity_names[0], .threshold = case.entity_thresholds[0], .max_candidates = case.options.max_mentions_by_type.person },
            .{ .name = case.entity_names[1], .threshold = case.entity_thresholds[1], .max_candidates = case.options.max_mentions_by_type.organization },
        };
        const relations = [_]schema_mod.JointRelation{
            .{ .name = case.relation_names[0], .head = &.{0}, .tail = &.{1}, .threshold = case.relation_thresholds[0] },
            .{ .name = case.relation_names[1], .head = &.{0}, .tail = &.{0}, .threshold = case.relation_thresholds[1] },
        };
        const schema = schema_mod.JointSchema{ .entities = &entities, .relations = &relations, .constraints = &.{} };
        var words: [6]processor.Word = undefined;
        var sample = testSample(&words);
        sample.original_text = case.text;
        var output = try fromScores(a, schema, sample, case.mentions, case.edges, .{
            .candidate_threshold = case.options.mention_threshold,
            .top_k_entities = case.options.max_mentions_per_type,
            .max_edges_per_type = case.options.max_edges_per_type,
            .rescue_relation_endpoints = case.options.rescue_relation_endpoints,
            .relation_role_threshold = case.options.edge_candidate_threshold,
            .entity_weight = case.options.entity_weight,
            .relation_weight = case.options.relation_weight,
        });
        defer output.deinit();
        errdefer std.debug.print("JointIE sparse admission fixture: {s}\n", .{case.id});
        try std.testing.expectEqual(case.expected.nodes.len, output.nodes.len);
        try std.testing.expectEqual(case.expected.edges.len, output.edges.len);
        for (case.expected.nodes, output.nodes, output.node_metadata) |expected, node, metadata| {
            try std.testing.expectEqual(expected.entity_type, node.entity_type);
            try std.testing.expectEqual(expected.start, node.start);
            try std.testing.expectEqual(expected.end, node.end);
            try std.testing.expectApproxEqAbs(expected.utility, node.utility, 1e-12);
            try std.testing.expectApproxEqAbs(expected.probability, node.probability, 1e-12);
            try std.testing.expectEqual(expected.token_span, metadata.token_span);
            try std.testing.expectEqual(expected.rescued, metadata.rescued);
        }
        for (case.expected.edges, output.edges) |expected, edge| {
            try std.testing.expectEqual(expected.relation_type, edge.relation_type);
            try std.testing.expectEqual(expected.head, edge.head);
            try std.testing.expectEqual(expected.tail, edge.tail);
            try std.testing.expectEqual(@as(?u64, expected.slot), edge.slot);
            try std.testing.expectEqual(@as(?u64, expected.hypothesis), edge.hypothesis);
            try std.testing.expectApproxEqAbs(expected.utility, edge.utility, 1e-12);
            try std.testing.expectApproxEqAbs(expected.probability, edge.probability, 1e-12);
        }
    }
}

const pythonRepr = @import("extraction_python_repr.zig").format;
