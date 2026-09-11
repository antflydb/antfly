// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Internal GLiNER2.5 CPU task orchestration. Encoder states and every route
//! come from one prepared batch. Results own their strings and coordinates;
//! source-free enum values are explicit and never receive invented offsets.
//! This module does not register a serving capability or qualify a backend.
const std = @import("std");
const compute = @import("../ops/ops.zig");
const model = @import("../models/gliner_boundary.zig");
const head = @import("../architectures/gliner_boundary_head.zig");
const tasks = @import("../architectures/gliner_boundary_tasks.zig");
const ops = @import("../architectures/gliner_boundary_ops.zig");
const processor = @import("gliner_boundary_processor.zig");
const schema_mod = @import("extraction_schema.zig");
const boundary = @import("gliner_boundary_decode.zig");
const constraints = @import("extraction_constraints.zig");
const relation_decoder = @import("gliner_boundary_relations.zig");
const record_decoder = @import("gliner_boundary_records.zig");
const joint_decoder = @import("extraction_joint_ie.zig");
const joint_candidates = @import("gliner_boundary_joint.zig");
pub const scoring = @import("gliner_boundary_scoring.zig");
const unicode = @import("../finetune/gliner2_unicode_tables.zig");
const literals = @import("gliner_boundary_unicode.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;

pub const CoreView = scoring.CoreView;
pub const Label = struct { label: []const u8, confidence: f32 };
pub const Attribute = struct { name: []const u8, multi_label: bool, labels: []const Label };
pub const SourceSpan = struct {
    start: usize,
    end: usize,
    unit: boundary.OffsetUnit,
    /// Stable UTF-8 coordinates are also retained for validation and merging.
    byte_start: usize,
    byte_end: usize,
};
pub const Value = struct {
    text: []const u8,
    confidence: f32,
    source: ?SourceSpan,
    /// Exact model coordinates, including enum prefix. Internal routing only.
    token_span: ?ops.Span,
    derived: bool = false,
    attributes: []const Attribute = &.{},
};
pub const EntityGroup = struct { name: []const u8, dtype: schema_mod.DType, values: []Value };
pub const Classification = struct { name: []const u8, multi_label: bool, labels: []const Label };
pub const Relation = struct {
    name: []const u8,
    head: Value,
    tail: Value,
    confidence: f32,
    derived: bool = false,
    /// JointIE endpoint identity includes type as well as exact source span.
    head_entity_type: ?usize = null,
    tail_entity_type: ?usize = null,
    /// Internal ordinary-relation route. Equal names can have distinct typed
    /// source/target schema routes and must not merge across those routes.
    schema_index: ?usize = null,
};
pub const Field = struct { name: []const u8, dtype: schema_mod.DType, values: []const Value };
pub const RecordOccurrence = struct {
    /// Internal learned instance identity. A latent source seed is not a
    /// public natural anchor and must never be serialized as one.
    source_instance: usize,
    seed_field: ?usize = null,
    seed_source: ?SourceSpan = null,
};
pub const Record = struct { confidence: ?f32 = null, anchor: ?SourceSpan = null, fields: []const Field, occurrence: ?RecordOccurrence = null };
pub const Structure = struct { name: []const u8, instances: []const Record };
pub const SolverStatus = enum { optimal, feasible };
pub const Diagnostics = struct { status: SolverStatus, visited_nodes: usize, utility: f64, exhausted: bool = false };
pub const LongDocumentMetadata = struct {
    version: u32 = 1,
    window_count: usize,
    window_policy: enum { source_words_midpoint_ownership } = .source_words_midpoint_ownership,
    classification_aggregation: enum { owned_word_weighted_mean_raw_logits } = .owned_word_weighted_mean_raw_logits,
    duplicate_score: enum { maximum_calibrated_score } = .maximum_calibrated_score,
    natural_record_identity: enum { exact_source_anchor } = .exact_source_anchor,
    other_record_identity: enum { occurrence, semantic },
    solver_optimality_scope: enum { retained_candidate_graph } = .retained_candidate_graph,
};
pub const Sample = struct {
    entities: []const EntityGroup = &.{},
    classifications: []const Classification = &.{},
    relations: []const Relation = &.{},
    structures: []const Structure = &.{},
    classification_solver: ?Diagnostics = null,
    joint_solver: ?Diagnostics = null,
    record_solver: ?Diagnostics = null,
    long_document: ?LongDocumentMetadata = null,
};
pub const Result = struct {
    arena: std.heap.ArenaAllocator,
    samples: []const Sample,
    pub fn deinit(self: *Result) void {
        self.arena.deinit();
        self.* = undefined;
    }
};
pub const Options = struct {
    threshold: f32 = 0.5,
    overlap: boundary.OverlapPolicy = .flat,
    offset_unit: boundary.OffsetUnit = .utf8_bytes,
    /// Only a valid witness may survive a search-node budget. Cancellation,
    /// proven infeasibility, and exhaustion without a witness remain errors.
    best_effort: bool = false,
    max_output_values: usize = 65536,
    max_output_string_bytes: usize = 16 * 1024 * 1024,
    head_limits: head.Limits = .{},
    task_limits: tasks.Limits = .{},
    query_limits: boundary.Limits = .{},
    relation_limits: relation_decoder.Options = .{},
    relation_dedup_limits: relation_decoder.DedupOptions = .{},
    record_limits: record_decoder.Options = .{ .max_instances = 4096 },
    enum_limits: literals.Options = .{},
    classification_solver: constraints.SolveOptions = .{},
    joint_solver: joint_decoder.Options = .{ .profile = .fastino_v1, .algorithm = .beam },
    joint_candidates: joint_candidates.Options = .{},
    control: ?Control = null,
    regex_context: ?*anyopaque = null,
    /// Must use the bounded validator engine accepted by schema compilation.
    validate_value_fn: ?*const fn (?*anyopaque, schema_mod.RegexValidator, []const u8) anyerror!bool = null,
};

fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b);
}
fn finite(values: []const f32, expected: usize) !void {
    if (values.len != expected) return error.InvalidBoundaryPipelineInput;
    for (values) |value| if (!std.math.isFinite(value)) return error.NonFiniteBoundaryScore;
}
fn findQuery(sample: processor.Sample, kind: processor.QueryKind, schema_index: usize, label_index: ?usize) !usize {
    for (sample.queries, 0..) |query, q| {
        if (query.kind == kind and query.schema_index == schema_index and
            (label_index == null or query.label_index == label_index.?)) return q;
    }
    return error.InvalidBoundaryPipelineRouting;
}
fn softmax(logits: []const f32, temperature: f32, output: []f32) !void {
    if (logits.len != output.len or logits.len == 0 or temperature <= 0 or !std.math.isFinite(temperature))
        return error.InvalidBoundaryPipelineInput;
    var maximum = -std.math.inf(f32);
    for (logits) |value| maximum = @max(maximum, value / temperature);
    var total: f32 = 0;
    for (logits, output) |value, *probability| {
        probability.* = @exp(value / temperature - maximum);
        total += probability.*;
    }
    if (!std.math.isFinite(total) or total <= 0) return error.NonFiniteBoundaryScore;
    for (output) |*value| value.* /= total;
}
fn trim(text: []const u8) ![]const u8 {
    const view = try std.unicode.Utf8View.init(text);
    var iter = view.iterator();
    var first: ?usize = null;
    var end: usize = 0;
    while (iter.i < text.len) {
        const start = iter.i;
        if (!unicode.isWhitespace(iter.nextCodepoint().?)) {
            if (first == null) first = start;
            end = iter.i;
        }
    }
    return text[first orelse text.len .. @max(first orelse text.len, end)];
}

const Work = struct {
    scorer: scoring.Scorer,
    allocator: Allocator,
    scratch_allocator: Allocator,
    config: *const model.Config,
    prepared: *const processor.PreparedBatch,
    schemas: []const *const schema_mod.CompiledSchema,
    scores: ?scoring.CandidateScoreView,
    options: Options,
    window_candidates: bool = false,
    output_values: usize = 0,
    output_string_bytes: usize = 0,
    enum_steps: usize = 0,

    fn check(self: *const Work) !void {
        try self.scorer.check();
        if (self.options.control) |control| try control.check();
    }
    fn copy(self: *Work, text: []const u8) ![]const u8 {
        self.output_string_bytes = try std.math.add(usize, self.output_string_bytes, text.len);
        if (self.output_string_bytes > self.options.max_output_string_bytes) return error.ExtractionOutputLimitExceeded;
        return self.allocator.dupe(u8, text);
    }
    fn chargeValue(self: *Work) !void {
        self.output_values = try std.math.add(usize, self.output_values, 1);
        if (self.output_values > self.options.max_output_values) return error.ExtractionOutputLimitExceeded;
    }
    fn source(self: *Work, b: usize, span: ops.Span, offsets: boundary.OffsetMap) !?SourceSpan {
        const sample = self.prepared.samples[b];
        if (span.start < sample.prefix_word_count or span.end <= span.start or span.end > sample.words.len) return null;
        // Synthetic terminal punctuation has no caller source. Omit candidates
        // whose endpoints are synthetic; the pinned Python formatter can emit
        // an out-of-document span here, which violates the native source contract.
        const first = sample.words[span.start].source orelse return null;
        const last = sample.words[span.end - 1].source orelse return null;
        if (first.start > last.end or last.end > sample.original_text.len) return error.InvalidBoundaryPipelineRouting;
        const converted = try offsets.convert(.{ .start = first.start, .end = last.end }, self.options.offset_unit);
        return .{ .start = converted.start, .end = converted.end, .unit = self.options.offset_unit, .byte_start = first.start, .byte_end = last.end };
    }
    fn value(self: *Work, b: usize, span: ops.Span, probability: f32, validators: []const schema_mod.RegexValidator, offsets: boundary.OffsetMap) !?Value {
        const source_span = (try self.source(b, span, offsets)) orelse return null;
        const surface = try trim(self.prepared.samples[b].original_text[source_span.byte_start..source_span.byte_end]);
        if (surface.len == 0) return null;
        for (validators) |validator| {
            const validate = self.options.validate_value_fn orelse return error.MissingExtractionValidator;
            try self.check();
            if (!try validate(self.options.regex_context, validator, surface)) return null;
        }
        try self.chargeValue();
        return .{ .text = try self.copy(surface), .confidence = probability, .source = source_span, .token_span = span };
    }
    fn query(self: *Work, b: usize, q: usize, threshold: f32, abstain: bool) ![]boundary.Candidate {
        const scores = self.scores orelse return error.MissingBoundaryScores;
        const capacity = scores.pool.capacity;
        const spans = try self.allocator.alloc(boundary.ScoredSpan, capacity);
        defer self.allocator.free(spans);
        for (spans, 0..) |*out, c| {
            const span = scores.pool.indices[b * capacity + c];
            out.* = .{ .start = span.start, .end = span.end, .logit = scores.pair_logits[(b * self.prepared.query_width + q) * capacity + c], .valid = scores.pool.valid[b * capacity + c] and self.prepared.query_marker_mask[b * self.prepared.query_width + q] };
        }
        var limits = self.options.query_limits;
        limits.control = self.options.control;
        return boundary.decodeQuery(self.allocator, spans, .{
            .threshold = threshold,
            .pair_temperature = self.config.head.pair_temperature,
            .null_logit = if (abstain) if (scores.null_logits) |values| values[b * self.prepared.query_width + q] else null else null,
            .abstention_threshold = self.config.head.abstention_threshold,
            .count_log_rate = if (scores.count_log_rates) |values| values[b * self.prepared.query_width + q] else null,
            .adaptive_threshold = self.config.head.adaptive_threshold,
            .policy = self.options.overlap,
            .limits = limits,
        });
    }
    fn explicit(self: *Work, b: usize, queries: []const usize, spans: []const ops.Span) !scoring.ExplicitScores {
        if (queries.len == 0 or spans.len == 0) return error.InvalidBoundaryPipelineInput;
        const indices = try self.allocator.alloc(head.SignedSpan, try mul(queries.len, spans.len));
        defer self.allocator.free(indices);
        for (queries, 0..) |q, row| {
            if (q >= self.prepared.samples[b].queries.len) return error.InvalidBoundaryPipelineRouting;
            for (spans, 0..) |span, c| indices[row * spans.len + c] = .{ .start = @intCast(span.start), .end = @intCast(span.end) };
        }
        var result = try self.scorer.explicit(self.scratch_allocator, .{ .sample_index = b, .query_ids = queries, .capacity = spans.len, .indices = indices }, self.options.head_limits, self.options.control);
        errdefer result.deinit();
        try finite(result.logits, indices.len);
        if (result.valid.len != indices.len) return error.InvalidBoundaryScorerOutput;
        try self.check();
        return result;
    }
    fn entities(self: *Work, b: usize, offsets: boundary.OffsetMap) ![]EntityGroup {
        const schema = self.schemas[b].schema;
        const groups = try self.allocator.alloc(EntityGroup, schema.entities.len);
        for (schema.entities, groups, 0..) |entity, *group, e| {
            try self.check();
            const q = try findQuery(self.prepared.samples[b], .entity, e, null);
            const selected = try self.query(b, q, @floatCast(entity.threshold orelse self.options.threshold), true);
            defer self.allocator.free(selected);
            var values = std.ArrayListUnmanaged(Value).empty;
            for (selected) |candidate| {
                if (try self.value(b, .{ .start = candidate.start, .end = candidate.end }, candidate.probability, entity.validators, offsets)) |v| {
                    try values.append(self.allocator, v);
                    if (entity.dtype == .str and !self.window_candidates) break;
                }
            }
            group.* = .{ .name = try self.copy(entity.name), .dtype = entity.dtype, .values = try values.toOwnedSlice(self.allocator) };
        }
        try self.attributes(b, groups);
        return groups;
    }
    fn attributes(self: *Work, b: usize, entities_out: []EntityGroup) !void {
        const schema = self.schemas[b].schema;
        if (schema.entity_attributes.len == 0) return;
        var spans = std.ArrayListUnmanaged(ops.Span).empty;
        defer spans.deinit(self.allocator);
        for (entities_out) |group| for (group.values) |v| {
            const span = v.token_span.?;
            var found = false;
            for (spans.items) |old| if (old.start == span.start and old.end == span.end) {
                found = true;
                break;
            };
            if (!found) try spans.append(self.allocator, span);
        };
        if (spans.items.len == 0) return;
        var queries = std.ArrayListUnmanaged(usize).empty;
        defer queries.deinit(self.allocator);
        const group_rows = try self.allocator.alloc(usize, schema.entity_attributes.len);
        defer self.allocator.free(group_rows);
        for (schema.entity_attributes, 0..) |group, g| {
            group_rows[g] = queries.items.len;
            for (group.labels, 0..) |_, label| try queries.append(self.allocator, try findQuery(self.prepared.samples[b], .attribute, g, label));
        }
        var scored = try self.explicit(b, queries.items, spans.items);
        defer scored.deinit();
        for (entities_out, 0..) |*entity, e| for (entity.values) |*v| {
            var column: usize = 0;
            for (spans.items, 0..) |span, c| if (span.start == v.token_span.?.start and span.end == v.token_span.?.end) {
                column = c;
                break;
            };
            var attributes_out = std.ArrayListUnmanaged(Attribute).empty;
            for (schema.entity_attributes, 0..) |group, g| {
                if (group.applies_to) |indices| if (std.mem.indexOfScalar(usize, indices, e) == null) continue;
                const logits = try self.allocator.alloc(f32, group.labels.len);
                defer self.allocator.free(logits);
                const probabilities = try self.allocator.alloc(f32, group.labels.len);
                defer self.allocator.free(probabilities);
                for (logits, 0..) |*logit, label| logit.* = scored.logits[(group_rows[g] + label) * spans.items.len + column];
                if (group.multi_label) {
                    for (logits, probabilities) |logit, *probability| probability.* = boundary.sigmoid(logit / self.config.head.pair_temperature);
                } else try softmax(logits, self.config.head.pair_temperature, probabilities);
                var labels = std.ArrayListUnmanaged(Label).empty;
                var best: usize = 0;
                for (probabilities, 0..) |probability, label| {
                    if (probability > probabilities[best]) best = label;
                    if (group.multi_label and probability >= group.threshold) {
                        try self.chargeValue();
                        try labels.append(self.allocator, .{ .label = try self.copy(group.labels[label]), .confidence = probability });
                    }
                }
                if (!group.multi_label) {
                    try self.chargeValue();
                    try labels.append(self.allocator, .{ .label = try self.copy(group.labels[best]), .confidence = probabilities[best] });
                }
                try attributes_out.append(self.allocator, .{ .name = try self.copy(group.name), .multi_label = group.multi_label, .labels = try labels.toOwnedSlice(self.allocator) });
            }
            v.attributes = try attributes_out.toOwnedSlice(self.allocator);
        };
    }

    fn classificationCheck(context: ?*anyopaque) !void {
        const self: *Work = @ptrCast(@alignCast(context.?));
        try self.check();
        if (self.options.classification_solver.check_fn) |check_fn|
            try check_fn(self.options.classification_solver.check_context);
    }

    fn buildJoint(self: *Work, b: usize) !joint_candidates.Candidates {
        var candidate_options = self.options.joint_candidates;
        candidate_options.max_nodes = @min(candidate_options.max_nodes, self.options.joint_solver.max_nodes);
        candidate_options.max_edges = @min(candidate_options.max_edges, self.options.joint_solver.max_edges);
        candidate_options.control = self.options.control;
        candidate_options.head_limits = self.options.head_limits;
        candidate_options.task_limits = self.options.task_limits;
        return joint_candidates.buildScored(self.scratch_allocator, self.config, .{
            .prepared = self.prepared,
            .compiled = self.schemas[b],
            .sample_index = b,
            .scores = self.scores orelse return error.MissingBoundaryScores,
        }, self.scorer, candidate_options);
    }
    fn joint(self: *Work, b: usize, offsets: boundary.OffsetMap) !Sample {
        const schema = self.schemas[b].schema.joint_ie orelse return error.InvalidBoundaryPipelineRouting;
        var candidates = try self.buildJoint(b);
        defer candidates.deinit();
        var solver_options = self.options.joint_solver;
        solver_options.control = self.options.control;
        var decoded = try joint_decoder.decodeWithSourceIdentity(self.scratch_allocator, schema, candidates.nodes, candidates.edges, candidates.sourceIdentity(), solver_options);
        defer decoded.deinit();
        if (decoded.status == .search_exhausted or (decoded.exhausted and !self.options.best_effort)) return error.JointSearchExhausted;
        if (decoded.status == .infeasible) return error.JointConstraintsInfeasible;
        if (!decoded.valid()) return error.InvalidJointSelection;
        try joint_decoder.sortSourcePresentation(schema, decoded.nodes, decoded.edges, self.options.control);
        const values = try self.allocator.alloc(Value, decoded.nodes.len);
        for (decoded.nodes, decoded.node_source_indices, values) |node, source_index, *out| {
            if (source_index >= candidates.node_metadata.len) return error.InvalidJointSelection;
            out.* = (try self.value(b, candidates.node_metadata[source_index].token_span, @floatCast(node.probability), &.{}, offsets)) orelse return error.InvalidJointSelection;
        }
        const entities_out = try self.allocator.alloc(EntityGroup, schema.entities.len);
        for (schema.entities, entities_out, 0..) |entity, *out, e| {
            var members = std.ArrayListUnmanaged(Value).empty;
            for (decoded.nodes, values) |node, value_| if (node.entity_type == e) try members.append(self.allocator, value_);
            const SourceOrder = struct {
                fn less(_: void, left: Value, right: Value) bool {
                    return if (left.source.?.byte_start != right.source.?.byte_start) left.source.?.byte_start < right.source.?.byte_start else left.source.?.byte_end < right.source.?.byte_end;
                }
            };
            std.mem.sort(Value, members.items, {}, SourceOrder.less);
            out.* = .{ .name = try self.copy(entity.name), .dtype = .list, .values = try members.toOwnedSlice(self.allocator) };
        }
        const relations_out = try self.allocator.alloc(Relation, decoded.edges.len);
        for (decoded.edges, relations_out) |edge, *out| {
            if (edge.relation_type >= schema.relations.len or edge.head >= values.len or edge.tail >= values.len) return error.InvalidJointSelection;
            out.* = .{ .name = try self.copy(schema.relations[edge.relation_type].name), .head = values[edge.head], .tail = values[edge.tail], .confidence = @floatCast(edge.probability), .derived = edge.derived, .head_entity_type = decoded.nodes[edge.head].entity_type, .tail_entity_type = decoded.nodes[edge.tail].entity_type };
        }
        return .{ .entities = entities_out, .relations = relations_out, .joint_solver = .{ .status = if (decoded.status == .optimal and !decoded.exhausted) .optimal else .feasible, .visited_nodes = decoded.visited_nodes, .utility = decoded.utility, .exhausted = decoded.exhausted } };
    }

    fn relationValue(self: *Work, mention: relation_decoder.Mention, probability: f32, offsets: boundary.OffsetMap) !Value {
        try self.chargeValue();
        const bytes = try offsets.toBytes(.{ .start = mention.start, .end = mention.end }, .unicode_codepoints);
        const converted = try offsets.convert(bytes, self.options.offset_unit);
        return .{ .text = try self.copy(mention.text), .confidence = probability, .source = .{ .start = converted.start, .end = converted.end, .unit = self.options.offset_unit, .byte_start = bytes.start, .byte_end = bytes.end }, .token_span = null };
    }
    fn relations(self: *Work, b: usize, offsets: boundary.OffsetMap) ![]const Relation {
        const schema = self.schemas[b].schema;
        if (schema.relations.len == 0) return &.{};
        const scores = self.scores orelse return error.MissingBoundaryScores;
        const routes = try self.allocator.alloc(relation_decoder.Route, schema.relations.len);
        const qids = try self.allocator.alloc([2]usize, schema.relations.len);
        for (schema.relations, routes, qids, 0..) |_, *route, *ids, r| {
            ids.* = .{ try findQuery(self.prepared.samples[b], .relation_head, r, null), try findQuery(self.prepared.samples[b], .relation_tail, r, null) };
            route.* = .{ .batch_index = 0, .relation_index = r, .head_queries = ids[0..1], .tail_queries = ids[1..2] };
        }
        var options = self.options.relation_limits;
        options.heads_per_relation = @min(options.heads_per_relation, self.config.head.relation_heads_per_type);
        options.tails_per_relation = @min(options.tails_per_relation, self.config.head.relation_tails_per_type);
        options.pair_cap = @min(options.pair_cap, self.config.head.relation_pair_cap);
        options.argument_threshold = @max(options.argument_threshold, self.config.head.relation_argument_proposal_threshold);
        options.control = self.options.control;
        const capacity = scores.pool.capacity;
        const proposals = try relation_decoder.generate(self.allocator, .{ .batch = 1, .queries = self.prepared.query_width, .capacity = capacity, .spans = scores.pool.indices[b * capacity ..][0..capacity], .valid = scores.pool.valid[b * capacity ..][0..capacity], .query_mask = self.prepared.query_marker_mask[b * self.prepared.query_width ..][0..self.prepared.query_width], .logits = scores.pair_logits[b * self.prepared.query_width * capacity ..][0 .. self.prepared.query_width * capacity] }, routes, options);
        if (proposals.len == 0) return &.{};
        const pairs = try self.allocator.alloc(tasks.RelationPair, proposals.len);
        for (proposals, pairs) |proposal, *pair| pair.* = .{ .batch_index = 0, .relation_index = @intCast(proposal.relation_index), .head_span = .{ .start = @intCast(proposal.head_span.start), .end = @intCast(proposal.head_span.end) }, .tail_span = .{ .start = @intCast(proposal.tail_span.start), .end = @intCast(proposal.tail_span.end) } };
        var scored = try self.scorer.relations(self.scratch_allocator, .{ .sample_index = b, .query_pairs = qids, .pairs = pairs }, self.options.task_limits, self.options.control);
        defer scored.deinit();
        try finite(scored.logits, pairs.len);
        if (scored.valid.len != pairs.len) return error.InvalidBoundaryScorerOutput;
        var output = std.ArrayListUnmanaged(Relation).empty;
        for (schema.relations, 0..) |relation, r| {
            var edges = std.ArrayListUnmanaged(relation_decoder.Edge).empty;
            for (proposals, scored.logits, 0..) |proposal, logit, i| {
                if (proposal.relation_index != r or !scored.valid[i]) continue;
                const probability = boundary.sigmoid(logit / self.config.head.relation_temperature);
                if (probability < (relation.threshold orelse self.options.threshold)) continue;
                const hs = (try self.source(b, proposal.head_span, offsets)) orelse continue;
                const ts = (try self.source(b, proposal.tail_span, offsets)) orelse continue;
                const text = self.prepared.samples[b].original_text;
                const htext = try trim(text[hs.byte_start..hs.byte_end]);
                const ttext = try trim(text[ts.byte_start..ts.byte_end]);
                if (htext.len == 0 or ttext.len == 0) continue;
                // Upstream ranks repeated mention pairs by codepoint distance,
                // independently of the public response's requested offset unit.
                const hc = try offsets.convert(.{ .start = hs.byte_start, .end = hs.byte_end }, .unicode_codepoints);
                const tc = try offsets.convert(.{ .start = ts.byte_start, .end = ts.byte_end }, .unicode_codepoints);
                try edges.append(self.allocator, .{ .head = .{ .text = htext, .start = hc.start, .end = hc.end }, .tail = .{ .text = ttext, .start = tc.start, .end = tc.end }, .probability = probability, .source_index = i });
            }
            if (self.window_candidates) {
                for (edges.items) |edge| try output.append(self.allocator, .{ .name = try self.copy(relation.name), .head = try self.relationValue(edge.head, edge.probability, offsets), .tail = try self.relationValue(edge.tail, edge.probability, offsets), .confidence = edge.probability, .schema_index = r });
                continue;
            }
            var dedup_options = self.options.relation_dedup_limits;
            dedup_options.control = self.options.control;
            var deduped = try relation_decoder.deduplicate(self.scratch_allocator, edges.items, dedup_options);
            defer deduped.deinit();
            for (deduped.edges) |edge| try output.append(self.allocator, .{ .name = try self.copy(relation.name), .head = try self.relationValue(edge.head, edge.probability, offsets), .tail = try self.relationValue(edge.tail, edge.probability, offsets), .confidence = edge.probability, .schema_index = r });
        }
        return output.toOwnedSlice(self.allocator);
    }

    const EnumChoices = struct { values: []Value, allowed: []bool };
    fn validateValue(self: *Work, validators: []const schema_mod.RegexValidator, text: []const u8) !bool {
        for (validators) |validator| {
            try self.check();
            const validate = self.options.validate_value_fn orelse return error.MissingExtractionValidator;
            if (!try validate(self.options.regex_context, validator, text)) return false;
        }
        return true;
    }
    fn enumTick(self: *Work) !void {
        if (self.enum_steps >= self.options.enum_limits.max_steps) return error.ExtractionLiteralLimitExceeded;
        self.enum_steps += 1;
        if (self.enum_steps % 64 == 1) try self.check();
    }
    fn choices(self: *Work, b: usize, structure_index: usize, field_index: usize, q: usize) !EnumChoices {
        const field = self.schemas[b].schema.structures[structure_index].fields[field_index];
        const allowed = try self.allocator.alloc(bool, field.choices.len);
        for (field.choices, allowed) |choice, *valid| valid.* = try self.validateValue(field.validators, choice);
        const pairs = try self.allocator.alloc(ops.Span, field.choices.len);
        defer self.allocator.free(pairs);
        const found = try self.allocator.alloc(bool, field.choices.len);
        defer self.allocator.free(found);
        @memset(found, false);
        for (self.prepared.samples[b].enum_choices) |choice| {
            if (choice.structure_index != structure_index or choice.field_index != field_index) continue;
            if (choice.choice_index >= pairs.len) return error.InvalidBoundaryPipelineRouting;
            pairs[choice.choice_index] = .{ .start = choice.score_start, .end = choice.score_start + 1 };
            found[choice.choice_index] = true;
        }
        for (found) |present| if (!present) return error.InvalidBoundaryPipelineRouting;
        var scored = try self.explicit(b, &.{q}, pairs);
        defer scored.deinit();
        const values = try self.allocator.alloc(Value, field.choices.len);
        for (field.choices, values, scored.logits, pairs) |choice, *out, logit, span| {
            try self.chargeValue();
            out.* = .{ .text = try self.copy(choice), .confidence = boundary.sigmoid(logit / self.config.head.pair_temperature), .source = null, .token_span = span, .derived = true };
        }
        return .{ .values = values, .allowed = allowed };
    }
    fn selectChoices(self: *Work, field: schema_mod.Field, choices_: EnumChoices) ![]const Value {
        if (choices_.values.len == 0) return &.{};
        const threshold: f32 = @floatCast(field.threshold orelse self.options.threshold);
        var selected = std.ArrayListUnmanaged(Value).empty;
        var best: ?usize = null;
        for (choices_.values, choices_.allowed, 0..) |v, allowed, i| {
            if (!allowed) continue;
            if (best == null or v.confidence > choices_.values[best.?].confidence) best = i;
            if (field.dtype == .list and v.confidence >= threshold) try selected.append(self.allocator, v);
        }
        if (field.dtype == .str) if (best) |index| if (choices_.values[index].confidence >= threshold) try selected.append(self.allocator, choices_.values[index]);
        return selected.toOwnedSlice(self.allocator);
    }
    fn structures(self: *Work, b: usize, offsets: boundary.OffsetMap) ![]Structure {
        var output = std.ArrayListUnmanaged(Structure).empty;
        for (self.schemas[b].schema.structures, 0..) |structure, s| {
            try self.check();
            const instances = if (structure.mode != null) try self.records(b, s, offsets) else blk: {
                const fields = try self.allocator.alloc(Field, structure.fields.len);
                var present = false;
                for (structure.fields, fields, 0..) |field, *out, f| {
                    const q = try findQuery(self.prepared.samples[b], .field, s, f);
                    var values = std.ArrayListUnmanaged(Value).empty;
                    if (field.choices.len > 0) {
                        const all = try self.choices(b, s, f, q);
                        if (self.window_candidates) {
                            for (all.values, all.allowed) |choice, allowed| if (allowed) try values.append(self.allocator, choice);
                        } else {
                            const selected = try self.selectChoices(field, all);
                            try values.appendSlice(self.allocator, selected);
                        }
                    } else {
                        const selected = try self.query(b, q, @floatCast(field.threshold orelse self.options.threshold), false);
                        defer self.allocator.free(selected);
                        for (selected) |candidate| {
                            if (try self.value(b, .{ .start = candidate.start, .end = candidate.end }, candidate.probability, field.validators, offsets)) |v| {
                                try values.append(self.allocator, v);
                                if (field.dtype == .str and !self.window_candidates) break;
                            }
                        }
                    }
                    if (!self.window_candidates and (field.cardinality == .required_one or field.cardinality == .one_or_more) and values.items.len == 0) return error.RequiredRecordFieldMissing;
                    out.* = .{ .name = try self.copy(field.name), .dtype = field.dtype, .values = try values.toOwnedSlice(self.allocator) };
                    present = present or out.values.len > 0;
                }
                if (!present) break :blk &.{};
                const one = try self.allocator.alloc(Record, 1);
                one[0] = .{ .fields = fields };
                break :blk one;
            };
            if (instances.len > 0) try output.append(self.allocator, .{ .name = try self.copy(structure.name), .instances = instances });
        }
        return output.toOwnedSlice(self.allocator);
    }
    fn fieldCardinality(field: schema_mod.Field, anchor: bool) schema_mod.Cardinality {
        // The pinned public processor does not forward field_dtypes_list to
        // build_boundary_batch_metadata. Unspecified non-anchor fields are
        // therefore list-scored even when dtype="str" formats one value.
        // An explicit cardinality selects the corresponding learned decoder.
        return field.cardinality orelse if (anchor) .required_one else .zero_or_more;
    }
    fn candidateProbability(self: *Work, b: usize, q: usize, span: ops.Span) f32 {
        const scores = self.scores.?;
        var probability: f32 = 0;
        for (0..scores.pool.capacity) |c| {
            if (!scores.pool.valid[b * scores.pool.capacity + c]) continue;
            const candidate = scores.pool.indices[b * scores.pool.capacity + c];
            if (candidate.start == span.start and candidate.end == span.end)
                probability = @max(probability, boundary.sigmoid(scores.pair_logits[(b * self.prepared.query_width + q) * scores.pool.capacity + c] / self.config.head.pair_temperature));
        }
        return probability;
    }
    fn records(self: *Work, b: usize, structure_index: usize, offsets: boundary.OffsetMap) ![]const Record {
        const scores = self.scores orelse return error.MissingBoundaryScores;
        const structure = self.schemas[b].schema.structures[structure_index];
        const mode = structure.mode.?;
        const capacity = scores.pool.capacity;
        const inputs = try self.allocator.alloc(scoring.RecordFieldRoute, structure.fields.len);
        const query_ids = try self.allocator.alloc(usize, structure.fields.len);
        const enum_values = try self.allocator.alloc(EnumChoices, structure.fields.len);
        const enum_mentions = try self.allocator.alloc([]literals.Mention, structure.fields.len);
        for (inputs, structure.fields, 0..) |*input, field, f| {
            const q = try findQuery(self.prepared.samples[b], .field, structure_index, f);
            query_ids[f] = q;
            var all_mentions: []literals.Mention = &.{};
            if (field.choices.len > 0) {
                enum_values[f] = try self.choices(b, structure_index, f, q);
                var options = self.options.enum_limits;
                options.control = self.options.control;
                all_mentions = try literals.findLiteralMentions(self.allocator, self.prepared.samples[b].original_text, field.choices, options);
                var admitted = std.ArrayListUnmanaged(literals.Mention).empty;
                var any_allowed = false;
                for (enum_values[f].allowed) |allowed| any_allowed = any_allowed or allowed;
                const cardinality = fieldCardinality(field, structure.anchor == f);
                if (!any_allowed and (cardinality == .required_one or cardinality == .one_or_more)) return error.RequiredRecordFieldMissing;
                for (all_mentions) |mention| {
                    try self.enumTick();
                    if (enum_values[f].allowed[mention.choice]) try admitted.append(self.allocator, mention);
                }
                enum_mentions[f] = try admitted.toOwnedSlice(self.allocator);
            } else {
                enum_values[f] = .{ .values = &.{}, .allowed = &.{} };
                enum_mentions[f] = &.{};
            }
            var pool_indices = std.ArrayListUnmanaged(usize).empty;
            // Inference forward_group retains the full ordered pool for each
            // field. Latent seed identity includes the field, even when two
            // fields refer to an identical span/state.
            for (0..capacity) |c| {
                if (!scores.pool.valid[b * capacity + c]) continue;
                if (field.choices.len > 0 and field.validators.len > 0 and
                    !try self.enumPoolCandidateAllowed(b, structure_index, f, scores.pool.indices[b * capacity + c], enum_values[f], all_mentions, offsets)) continue;
                try pool_indices.append(self.allocator, c);
            }
            input.* = .{
                .query_id = q,
                .pool_indices = try pool_indices.toOwnedSlice(self.allocator),
            };
        }
        var scored = try self.scorer.record(self.scratch_allocator, .{ .sample_index = b, .mode = @enumFromInt(@intFromEnum(mode)), .fields = inputs, .anchor_field = structure.anchor }, self.options.task_limits, self.options.control);
        defer scored.deinit();
        if (scored.fields.len != structure.fields.len or scored.object_logits.len > self.options.task_limits.max_record_instances or
            scored.instance_seeds.len != scored.object_logits.len) return error.InvalidBoundaryScorerOutput;
        try finite(scored.object_logits, scored.instance_seeds.len);
        for (scored.fields, inputs) |field, route| {
            if (field.candidate_spans.len != route.pool_indices.len) return error.InvalidBoundaryScorerOutput;
            try finite(field.candidate_logits, route.pool_indices.len);
            try finite(field.assign_logits, try mul(scored.instance_seeds.len, route.pool_indices.len + 1));
            for (field.candidate_spans, route.pool_indices) |span, candidate| {
                const expected = scores.pool.indices[b * capacity + candidate];
                if (span.start != expected.start or span.end != expected.end) return error.InvalidBoundaryScorerOutput;
            }
        }
        for (scored.instance_seeds) |maybe_seed| if (maybe_seed) |seed| {
            if (seed.field >= scored.fields.len or seed.candidate >= scored.fields[seed.field].candidate_spans.len) return error.InvalidBoundaryScorerOutput;
        };
        const fields = try self.allocator.alloc(record_decoder.Field, structure.fields.len);
        for (fields, scored.fields, structure.fields, query_ids, 0..) |*out, field_scores, field, q, f| {
            const card = fieldCardinality(field, structure.anchor != null and structure.anchor.? == f);
            out.* = .{ .query_id = q, .spans = field_scores.candidate_spans, .assignment_logits = field_scores.assign_logits, .scalar = card == .required_one or card == .optional_one, .allows_absent = card == .optional_one or card == .zero_or_more, .exclusive = field.exclusive };
        }
        const instance_spans = try self.allocator.alloc(?ops.Span, scored.instance_seeds.len);
        for (scored.instance_seeds, instance_spans) |seed, *out| out.* = if (seed) |identity| scored.fields[identity.field].candidate_spans[identity.candidate] else null;
        var options = self.options.record_limits;
        options.anchor_threshold = self.options.threshold;
        options.object_threshold = self.options.threshold;
        options.field_threshold = self.options.threshold;
        options.temperature = self.config.head.record_temperature;
        options.control = self.options.control;
        options.assignment.control = self.options.control;
        var decoded = try record_decoder.decode(self.scratch_allocator, .{ .mode = @enumFromInt(@intFromEnum(mode)), .anchor_field = structure.anchor, .object_logits = scored.object_logits, .instance_spans = instance_spans, .fields = fields }, options);
        defer decoded.deinit();
        const anchors = try self.allocator.alloc(?SourceSpan, decoded.records.len);
        for (decoded.records, anchors) |record, *anchor| anchor.* = if (record.anchor) |span| try self.source(b, span, offsets) else null;
        var result = std.ArrayListUnmanaged(Record).empty;
        for (decoded.records, 0..) |record, r| {
            try self.check();
            const out_fields = try self.allocator.alloc(Field, structure.fields.len);
            var present = false;
            for (structure.fields, record.fields, out_fields, query_ids, 0..) |field, selected, *out, q, f| {
                const card = fieldCardinality(field, structure.anchor != null and structure.anchor.? == f);
                const scalar = card == .required_one or card == .optional_one or field.dtype == .str;
                const optional = card == .optional_one or card == .zero_or_more;
                var values = std.ArrayListUnmanaged(Value).empty;
                for (selected.values) |selection| {
                    const candidate_probability = self.candidateProbability(b, q, selection.span);
                    const probability = @min(candidate_probability, selection.probability);
                    if (optional and candidate_probability < self.options.threshold) continue;
                    if (field.threshold) |threshold| if (probability < threshold) continue;
                    if (try self.value(b, selection.span, probability, if (field.choices.len > 0) &.{} else field.validators, offsets)) |v| try values.append(self.allocator, v);
                }
                var final_values = try self.finalizeValues(values.items, scalar, offsets);
                if (field.choices.len > 0)
                    final_values = try self.recordChoices(b, field, scalar, enum_values[f], enum_mentions[f], final_values, anchors, r, offsets);
                if ((card == .required_one or card == .one_or_more) and final_values.len == 0) return error.RequiredRecordFieldMissing;
                out.* = .{ .name = try self.copy(field.name), .dtype = if (scalar) .str else .list, .values = final_values };
                present = present or final_values.len > 0;
            }
            if (present) {
                const seed = scored.instance_seeds[record.source_instance];
                const seed_source = if (seed) |identity| try self.source(b, scored.fields[identity.field].candidate_spans[identity.candidate], offsets) else null;
                try result.append(self.allocator, .{ .confidence = record.probability, .anchor = anchors[r], .fields = out_fields, .occurrence = .{ .source_instance = record.source_instance, .seed_field = if (seed) |identity| identity.field else null, .seed_source = seed_source } });
            }
        }
        return result.toOwnedSlice(self.allocator);
    }
    fn finalizeValues(self: *Work, values: []const Value, scalar: bool, offsets: boundary.OffsetMap) ![]const Value {
        const candidates = try self.allocator.alloc(boundary.Candidate, values.len);
        defer self.allocator.free(candidates);
        for (values, candidates, 0..) |value_, *out, i| {
            const coordinates = try offsets.convert(.{ .start = value_.source.?.byte_start, .end = value_.source.?.byte_end }, .unicode_codepoints);
            out.* = .{ .start = coordinates.start, .end = coordinates.end, .probability = value_.confidence, .source_index = i };
        }
        var limits = self.options.query_limits;
        limits.control = self.options.control;
        const selected = try boundary.resolveOverlaps(self.allocator, candidates, self.options.overlap, limits);
        defer self.allocator.free(selected);
        const result = try self.allocator.alloc(Value, if (scalar) @min(selected.len, 1) else selected.len);
        for (selected[0..result.len], result) |candidate, *out| out.* = values[candidate.source_index];
        return result;
    }
    fn enumPoolCandidateAllowed(self: *Work, b: usize, structure_index: usize, field_index: usize, span: ops.Span, choices_: EnumChoices, mentions: []const literals.Mention, offsets: boundary.OffsetMap) !bool {
        var matched = false;
        for (self.prepared.samples[b].enum_choices) |choice| {
            try self.enumTick();
            if (choice.structure_index != structure_index or choice.field_index != field_index or
                (span.start != choice.start and span.start != choice.score_start) or span.end != span.start + 1) continue;
            matched = true;
            if (choices_.allowed[choice.choice_index]) return true;
        }
        if (try self.source(b, span, offsets)) |source_| {
            for (mentions) |mention| {
                try self.enumTick();
                if (source_.byte_start != mention.start or source_.byte_end != mention.end) continue;
                matched = true;
                if (choices_.allowed[mention.choice]) return true;
            }
        }
        // Non-enum span proposals can lead to the calibrated enum fallback.
        // Its final declared value is subject to the same allowed mask.
        return !matched;
    }
    fn recordChoices(self: *Work, b: usize, field: schema_mod.Field, scalar: bool, choices_: EnumChoices, mentions: []const literals.Mention, selected: []const Value, anchors: []const ?SourceSpan, record_index: usize, offsets: boundary.OffsetMap) ![]const Value {
        if (anchors[record_index]) |anchor| {
            var preferred = std.ArrayListUnmanaged(literals.Mention).empty;
            for (mentions) |mention| {
                if (mention.choice >= choices_.allowed.len) return error.InvalidBoundaryPipelineRouting;
                if (!choices_.allowed[mention.choice]) continue;
                var first: ?usize = null;
                var preceding: ?usize = null;
                for (anchors, 0..) |maybe_anchor, a| if (maybe_anchor) |other| {
                    if (first == null or other.byte_start < anchors[first.?].?.byte_start) first = a;
                    if (other.byte_start <= mention.start and (preceding == null or other.byte_start >= anchors[preceding.?].?.byte_start)) preceding = a;
                };
                const owner = preceding orelse first orelse continue;
                if (owner != record_index) continue;
                var seen = false;
                for (preferred.items) |previous| if (previous.choice == mention.choice) {
                    seen = true;
                    break;
                };
                if (!seen) try preferred.append(self.allocator, mention);
            }
            if (preferred.items.len > 0) {
                var best: usize = 0;
                const anchor_cp = try offsets.convert(.{ .start = anchor.byte_start, .end = anchor.byte_end }, .unicode_codepoints);
                for (preferred.items, 0..) |mention, i| {
                    const mention_cp = try offsets.convert(.{ .start = mention.start, .end = mention.end }, .unicode_codepoints);
                    const distance = @max(anchor_cp.start -| mention_cp.end, mention_cp.start -| anchor_cp.end);
                    const old = preferred.items[best];
                    const old_cp = try offsets.convert(.{ .start = old.start, .end = old.end }, .unicode_codepoints);
                    const old_distance = @max(anchor_cp.start -| old_cp.end, old_cp.start -| anchor_cp.end);
                    if (distance < old_distance) best = i;
                }
                const out = try self.allocator.alloc(Value, if (scalar) 1 else preferred.items.len);
                for (out, 0..) |*value_, i| {
                    const mention = preferred.items[if (scalar) best else i];
                    value_.* = choices_.values[mention.choice];
                    const converted = try offsets.convert(.{ .start = mention.start, .end = mention.end }, self.options.offset_unit);
                    value_.source = .{ .start = converted.start, .end = converted.end, .unit = self.options.offset_unit, .byte_start = mention.start, .byte_end = mention.end };
                    value_.token_span = null;
                    value_.derived = false;
                }
                return out;
            }
            if (mentions.len > 0) return &.{};
        }
        var matched = std.ArrayListUnmanaged(Value).empty;
        for (selected) |value_| {
            const key = try relation_decoder.fullCasefold(self.allocator, value_.text);
            defer self.allocator.free(key);
            for (choices_.values, choices_.allowed) |choice, allowed| {
                if (!allowed) continue;
                const wanted = try relation_decoder.fullCasefold(self.allocator, choice.text);
                defer self.allocator.free(wanted);
                if (std.mem.eql(u8, key, wanted)) {
                    var canonical = value_;
                    canonical.text = choice.text;
                    try matched.append(self.allocator, canonical);
                    break;
                }
            }
        }
        _ = b;
        if (matched.items.len > 0) return matched.toOwnedSlice(self.allocator);
        var adjusted = field;
        adjusted.dtype = if (scalar) .str else .list;
        return self.selectChoices(adjusted, choices_);
    }

    fn classify(self: *Work, b: usize, all_logits: []const f32, output: *Sample) !void {
        const compiled = self.schemas[b];
        if (compiled.schema.classifications.len == 0) return;
        try self.check();
        var scratch = std.heap.ArenaAllocator.init(self.scratch_allocator);
        defer scratch.deinit();
        const a = scratch.allocator();
        const rows = try a.alloc([]const f64, compiled.schema.classifications.len);
        for (compiled.schema.classifications, 0..) |classification, task_index| {
            const count = classification.task.labels.len;
            const logits = try a.alloc(f64, count);
            const found = try a.alloc(bool, count);
            @memset(found, false);
            for (self.prepared.samples[b].classification_labels, 0..) |label, c| {
                if (label.schema_index != task_index) continue;
                if (label.label_index >= count or found[label.label_index]) return error.InvalidBoundaryPipelineRouting;
                found[label.label_index] = true;
                logits[label.label_index] = all_logits[b * self.prepared.classification_width + c];
            }
            for (found) |present| if (!present) return error.InvalidBoundaryPipelineRouting;
            rows[task_index] = logits;
        }
        var options = self.options;
        options.classification_solver.check_context = self;
        options.classification_solver.check_fn = classificationCheck;
        var presented = try presentClassifications(self.scratch_allocator, compiled, rows, self.config.head.classification_temperature, options);
        defer presented.deinit();
        const values = try self.allocator.alloc(Classification, presented.classifications.len);
        for (presented.classifications, values) |classification, *out| {
            const labels = try self.allocator.alloc(Label, classification.labels.len);
            for (classification.labels, labels) |label, *to| {
                try self.chargeValue();
                to.* = .{ .label = try self.copy(label.label), .confidence = label.confidence };
            }
            out.* = .{ .name = try self.copy(classification.name), .multi_label = classification.multi_label, .labels = labels };
        }
        output.classifications = values;
        output.classification_solver = presented.diagnostics;
        try self.check();
    }
};

/// Owned final classification presentation shared by single-document and
/// explicit window-aggregation callers. The logits are pre-activation rows in
/// schema task/label order. Aggregated f64 logits retain that precision for
/// constrained utility; model confidence uses the published f32 activation.
pub const ClassificationOutput = struct {
    arena: std.heap.ArenaAllocator,
    classifications: []const Classification,
    diagnostics: ?Diagnostics,
    output_values: usize,
    output_string_bytes: usize,

    pub fn deinit(self: *ClassificationOutput) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

const ClassificationPresentation = struct {
    allocator: Allocator,
    options: Options,
    output_values: usize = 0,
    output_string_bytes: usize = 0,

    fn check(context: ?*anyopaque) !void {
        const self: *const ClassificationPresentation = @ptrCast(@alignCast(context.?));
        if (self.options.control) |control| try control.check();
        if (self.options.classification_solver.check_fn) |callback|
            try callback(self.options.classification_solver.check_context);
    }
    fn copy(self: *ClassificationPresentation, text: []const u8) ![]const u8 {
        self.output_string_bytes = try std.math.add(usize, self.output_string_bytes, text.len);
        if (self.output_string_bytes > self.options.max_output_string_bytes) return error.ExtractionOutputLimitExceeded;
        return self.allocator.dupe(u8, text);
    }
    fn chargeValue(self: *ClassificationPresentation) !void {
        self.output_values = try std.math.add(usize, self.output_values, 1);
        if (self.output_values > self.options.max_output_values) return error.ExtractionOutputLimitExceeded;
    }
};

pub fn presentClassifications(allocator: Allocator, compiled: *const schema_mod.CompiledSchema, raw_rows: []const []const f64, model_classification_temperature: f32, options: Options) !ClassificationOutput {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    var presentation = ClassificationPresentation{ .allocator = arena.allocator(), .options = options };
    try ClassificationPresentation.check(&presentation);
    if (!std.math.isFinite(model_classification_temperature) or model_classification_temperature <= 0 or options.max_output_values == 0 or options.max_output_string_bytes == 0)
        return error.InvalidBoundaryPipelineOptions;
    const solver = options.classification_solver;
    if (solver.max_local_assignments == 0 or solver.max_subset_visits == 0 or solver.beam_width == 0 or solver.beam_width > 4096)
        return error.InvalidBoundaryPipelineOptions;
    const schema = compiled.schema;
    if (raw_rows.len != schema.classifications.len or raw_rows.len > constraints.max_tasks) return error.InvalidBoundaryPipelineInput;
    const structured = schema_mod.usesStructuredClassification(schema.classifications, schema.classification_constraints.roots.len > 0);
    // Compilation rejects all explicit top_k values on this route, including
    // 1. Also refuse a non-default value in an independently constructed IR.
    if (structured) for (schema.classifications) |classification| {
        if (classification.top_k != 1) return error.ConstrainedClassificationTopKUnsupported;
    };
    var scratch = std.heap.ArenaAllocator.init(allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    const probabilities = try a.alloc([]f32, schema.classifications.len);
    for (schema.classifications, raw_rows, probabilities) |classification, row, *probability_row| {
        try ClassificationPresentation.check(&presentation);
        const count = classification.task.labels.len;
        if (count == 0 or count > constraints.max_labels or row.len != count) return error.InvalidBoundaryPipelineInput;
        const logits = try a.alloc(f32, count);
        for (row, logits) |from, *to| {
            if (!std.math.isFinite(from) or @abs(from) > std.math.floatMax(f32)) return error.NonFiniteBoundaryScore;
            to.* = @floatCast(from);
        }
        const probs = try a.alloc(f32, count);
        probability_row.* = probs;
        const temperature: f32 = @as(f32, @floatCast(classification.task.temperature)) * (if (structured) @as(f32, 1) else model_classification_temperature);
        if (!std.math.isFinite(temperature) or temperature <= 0) return error.InvalidBoundaryPipelineOptions;
        const exclusive = classification.task.min_labels == 1 and classification.task.maximum() == 1;
        const sigmoid_activation = switch (classification.activation) {
            .sigmoid => true,
            .softmax => false,
            .auto => if (structured) !exclusive else classification.mode == .multi,
        };
        if (sigmoid_activation) {
            for (logits, probs) |logit, *probability| probability.* = boundary.sigmoid(logit / temperature);
        } else try softmax(logits, temperature, probs);
    }
    const selections = try a.alloc(constraints.Selection, schema.classifications.len);
    @memset(selections, 0);
    var diagnostics: ?Diagnostics = null;
    if (structured) {
        var solve_options = options.classification_solver;
        solve_options.check_context = &presentation;
        solve_options.check_fn = ClassificationPresentation.check;
        var solution = try constraints.solve(allocator, schema.classification_constraints, raw_rows, solve_options);
        defer solution.deinit();
        if (solution.status == .search_exhausted or (solution.exhausted and !options.best_effort)) return error.ClassificationSearchExhausted;
        if (solution.status == .infeasible) return error.ClassificationConstraintsInfeasible;
        if (!solution.valid() or solution.selections.len != selections.len) return error.InvalidClassificationSelection;
        if (try schema.classification_constraints.evaluate(.{ .selected = solution.selections, .possible = solution.selections }) != .yes) return error.InvalidClassificationSelection;
        @memcpy(selections, solution.selections);
        diagnostics = .{ .status = if (solution.status == .optimal and !solution.exhausted) .optimal else .feasible, .visited_nodes = solution.visited_nodes, .utility = solution.utility, .exhausted = solution.exhausted };
    } else {
        for (schema.classifications, probabilities, selections) |classification, probs, *selected| {
            var best: usize = 0;
            for (probs, 0..) |probability, label| {
                if (probability > probs[best]) best = label;
                if (classification.mode == .multi and probability >= classification.task.threshold)
                    selected.* |= @as(constraints.Selection, 1) << @intCast(label);
            }
            if (classification.mode == .multi) {
                if (selected.* == 0) selected.* = @as(constraints.Selection, 1) << @intCast(best);
            } else for (0..@min(classification.top_k, probs.len)) |_| {
                var next: ?usize = null;
                for (probs, 0..) |probability, label| {
                    if (selected.* & (@as(constraints.Selection, 1) << @intCast(label)) != 0) continue;
                    if (next == null or probability > probs[next.?]) next = label;
                }
                if (next) |label| selected.* |= @as(constraints.Selection, 1) << @intCast(label);
            }
        }
    }
    const values = try presentation.allocator.alloc(Classification, schema.classifications.len);
    for (schema.classifications, probabilities, selections, values) |classification, probs, selected, *out| {
        try ClassificationPresentation.check(&presentation);
        const labels = try presentation.allocator.alloc(Label, @popCount(selected));
        var destination: usize = 0;
        for (classification.task.labels, probs, 0..) |label, probability, index| {
            if (selected & (@as(constraints.Selection, 1) << @intCast(index)) != 0) {
                try presentation.chargeValue();
                labels[destination] = .{ .label = try presentation.copy(label), .confidence = probability };
                destination += 1;
            }
        }
        out.* = .{ .name = try presentation.copy(classification.task.name), .multi_label = classification.mode != .single or classification.top_k > 1, .labels = labels };
    }
    try ClassificationPresentation.check(&presentation);
    return .{ .arena = arena, .classifications = values, .diagnostics = diagnostics, .output_values = presentation.output_values, .output_string_bytes = presentation.output_string_bytes };
}

fn cloneOwned(comptime T: type, allocator: Allocator, value: T) Allocator.Error!T {
    return switch (@typeInfo(T)) {
        .pointer => |pointer| blk: {
            if (pointer.size != .slice) @compileError("result ownership accepts only slices");
            if (pointer.child == u8) break :blk try allocator.dupe(u8, value);
            const result = try allocator.alloc(pointer.child, value.len);
            for (value, result) |item, *out| out.* = try cloneOwned(pointer.child, allocator, item);
            break :blk result;
        },
        .optional => |optional| if (value) |item| try cloneOwned(optional.child, allocator, item) else null,
        .@"struct" => blk: {
            var result: T = undefined;
            inline for (std.meta.fields(T)) |field| @field(result, field.name) = try cloneOwned(field.type, allocator, @field(value, field.name));
            break :blk result;
        },
        else => value,
    };
}

/// Runs one admitted, already encoded batch. No partial batch is returned on
/// validation, cancellation, solver exhaustion, or allocation failure.
pub fn runNative(cb: *const compute.ComputeBackend, allocator: Allocator, config: *const model.Config, prepared: *const processor.PreparedBatch, schemas: []const *const schema_mod.CompiledSchema, core: CoreView, options: Options) !Result {
    return runNativePerSample(cb, allocator, config, prepared, schemas, core, options, &.{});
}

/// Validate semantic options before allocating or encoding a request. Backend
/// shape/resource plans and compiled route checks remain separate preflight.
pub fn validateOptions(options: Options) !void {
    if (!std.math.isFinite(options.threshold) or options.threshold < 0 or options.threshold > 1 or options.max_output_values == 0 or options.max_output_string_bytes == 0 or options.joint_candidates.document_byte_offset != 0)
        return error.InvalidBoundaryPipelineOptions;
    const classifier = options.classification_solver;
    if (classifier.max_local_assignments == 0 or classifier.max_subset_visits == 0 or classifier.beam_width == 0 or classifier.beam_width > 4096)
        return error.InvalidBoundaryPipelineOptions;
    const joint = options.joint_solver;
    if (joint.max_nodes == 0 or joint.max_nodes > 256 or joint.max_edges > 256 or joint.max_graph_edges == 0 or joint.max_graph_edges > 1024 or joint.beam_width == 0 or joint.beam_width > 1024 or joint.max_validation_steps == 0)
        return error.InvalidBoundaryPipelineOptions;
    const candidates = options.joint_candidates;
    for ([_]f64{ candidates.candidate_threshold, candidates.relation_role_threshold, candidates.entity_threshold orelse candidates.candidate_threshold, options.relation_limits.argument_threshold }) |probability|
        if (!std.math.isFinite(probability) or probability < 0 or probability > 1) return error.InvalidBoundaryPipelineOptions;
    if (!std.math.isFinite(candidates.entity_weight) or candidates.entity_weight < 0 or !std.math.isFinite(candidates.relation_weight) or candidates.relation_weight < 0 or
        candidates.top_k_entities == 0 or candidates.top_k_roles == 0 or candidates.relation_pair_cap == 0 or candidates.max_edges_per_type == 0 or candidates.max_nodes > 256 or candidates.max_edges > 256 or
        options.relation_limits.heads_per_relation == 0 or options.relation_limits.tails_per_relation == 0 or options.relation_limits.pair_cap == 0)
        return error.InvalidBoundaryPipelineOptions;
}

/// Per-item semantic options completely replace common semantic defaults.
/// Admission ceilings and the execution control remain request-wide; a larger
/// per-item search budget cannot bypass the common resource ceiling.
fn optionsForSample(common: Options, item: Options) Options {
    var result = common;
    result.threshold = item.threshold;
    result.overlap = item.overlap;
    result.offset_unit = item.offset_unit;
    result.best_effort = item.best_effort;
    result.relation_limits.heads_per_relation = @min(common.relation_limits.heads_per_relation, item.relation_limits.heads_per_relation);
    result.relation_limits.tails_per_relation = @min(common.relation_limits.tails_per_relation, item.relation_limits.tails_per_relation);
    result.relation_limits.pair_cap = @min(common.relation_limits.pair_cap, item.relation_limits.pair_cap);
    result.relation_limits.argument_threshold = item.relation_limits.argument_threshold;
    result.classification_solver.algorithm = item.classification_solver.algorithm;
    result.classification_solver.beam_width = @min(common.classification_solver.beam_width, item.classification_solver.beam_width);
    result.classification_solver.exact_node_budget = @min(common.classification_solver.exact_node_budget, item.classification_solver.exact_node_budget);
    result.classification_solver.beam_node_budget = @min(common.classification_solver.beam_node_budget, item.classification_solver.beam_node_budget);
    result.classification_solver.max_local_assignments = @min(common.classification_solver.max_local_assignments, item.classification_solver.max_local_assignments);
    result.classification_solver.max_subset_visits = @min(common.classification_solver.max_subset_visits, item.classification_solver.max_subset_visits);
    result.joint_solver.algorithm = item.joint_solver.algorithm;
    result.joint_solver.profile = item.joint_solver.profile;
    result.joint_solver.max_source_key_bytes = @min(common.joint_solver.max_source_key_bytes, item.joint_solver.max_source_key_bytes);
    result.joint_solver.beam_width = @min(common.joint_solver.beam_width, item.joint_solver.beam_width);
    result.joint_solver.exact_node_budget = @min(common.joint_solver.exact_node_budget, item.joint_solver.exact_node_budget);
    result.joint_solver.beam_node_budget = @min(common.joint_solver.beam_node_budget, item.joint_solver.beam_node_budget);
    result.joint_candidates.candidate_threshold = item.joint_candidates.candidate_threshold;
    result.joint_candidates.entity_threshold = item.joint_candidates.entity_threshold;
    result.joint_candidates.relation_role_threshold = item.joint_candidates.relation_role_threshold;
    result.joint_candidates.top_k_entities = item.joint_candidates.top_k_entities;
    result.joint_candidates.top_k_roles = item.joint_candidates.top_k_roles;
    result.joint_candidates.relation_pair_cap = @min(common.joint_candidates.relation_pair_cap, item.joint_candidates.relation_pair_cap);
    result.joint_candidates.max_edges_per_type = @min(common.joint_candidates.max_edges_per_type, item.joint_candidates.max_edges_per_type);
    result.joint_candidates.rescue_relation_endpoints = item.joint_candidates.rescue_relation_endpoints;
    result.joint_candidates.entity_weight = item.joint_candidates.entity_weight;
    result.joint_candidates.relation_weight = item.joint_candidates.relation_weight;
    return result;
}

/// Scores a heterogeneous batch once, then applies each complete item option
/// replacement. Empty per_sample selects the common options for every item.
pub fn runNativePerSample(cb: *const compute.ComputeBackend, allocator: Allocator, config: *const model.Config, prepared: *const processor.PreparedBatch, schemas: []const *const schema_mod.CompiledSchema, core: CoreView, options: Options, per_sample: []const Options) !Result {
    if (cb.kind() != .native) return error.UnsupportedGlinerBoundaryBackend;
    try cb.checkExecutionControl();
    if (options.control) |control| try control.check();
    try validateOptions(options);
    const batch = prepared.samples.len;
    if (per_sample.len != 0 and per_sample.len != batch) return error.InvalidBoundaryPipelineOptions;
    for (per_sample) |item| try validateOptions(item);
    const h: usize = config.encoder.hidden_size;
    if (batch == 0 or schemas.len != batch or core.text_lengths.len != batch or
        prepared.query_marker_mask.len != try mul(batch, prepared.query_width)) return error.InvalidBoundaryPipelineInput;
    try finite(core.text_states, try mul(try mul(batch, prepared.word_width), h));
    try finite(core.query_states, try mul(try mul(batch, prepared.query_width), h));
    try finite(core.classification_states, try mul(try mul(batch, prepared.classification_width), h));
    for (prepared.samples, schemas, core.text_lengths) |sample, schema, length| {
        if (!std.mem.eql(u8, &sample.schema_fingerprint, &schema.fingerprint) or length != sample.words.len or length > prepared.word_width or
            sample.queries.len > prepared.query_width or sample.classification_labels.len > prepared.classification_width)
            return error.InvalidBoundaryPipelineRouting;
        if (options.validate_value_fn == null) {
            for (schema.schema.entities) |entity| if (entity.validators.len > 0) return error.MissingExtractionValidator;
            for (schema.schema.structures) |structure| for (structure.fields) |field| if (field.validators.len > 0) return error.MissingExtractionValidator;
        }
    }
    var scores: ?head.Result = if (prepared.query_width > 0) try head.forwardNative(cb, allocator, config, .{
        .batch = batch,
        .text_length = prepared.word_width,
        .queries = prepared.query_width,
        .text_states = core.text_states,
        .query_states = core.query_states,
        .text_lengths = core.text_lengths,
        .query_mask = prepared.query_marker_mask,
        .control = options.control,
    }, options.head_limits) else null;
    defer if (scores) |*value| value.deinit();
    var native = scoring.NativeContext{ .cb = cb, .config = config, .prepared = prepared, .core = core, .scores = if (scores) |*value| value else null };
    return runScoredPerSample(allocator, config, prepared, schemas, if (scores) |*value| scoring.CandidateScoreView.fromNative(value) else null, native.scorer(), options, per_sample);
}

/// Shared decoder for CPU and resident-device scoring. Only final scalar
/// scores cross this interface. Context owners retain backend tensors until
/// this synchronous call returns; every output still owns its full payload.
pub fn runScored(allocator: Allocator, config: *const model.Config, prepared: *const processor.PreparedBatch, schemas: []const *const schema_mod.CompiledSchema, scores: ?scoring.CandidateScoreView, scorer: scoring.Scorer, options: Options) !Result {
    return runScoredPerSample(allocator, config, prepared, schemas, scores, scorer, options, &.{});
}

pub fn runScoredPerSample(allocator: Allocator, config: *const model.Config, prepared: *const processor.PreparedBatch, schemas: []const *const schema_mod.CompiledSchema, scores: ?scoring.CandidateScoreView, scorer: scoring.Scorer, options: Options, per_sample: []const Options) !Result {
    var result = try runScoredInternal(allocator, config, prepared, schemas, scores, scorer, options, per_sample, false);
    result.releaseEvidence();
    return result.outputs;
}

pub const WindowResult = struct {
    allocator: Allocator,
    outputs: Result,
    /// [B, classification_width], before activation or constrained decoding.
    classification_scores: ?scoring.ClassificationScores,
    /// One pre-solver retained graph per JointIE sample; null for other tasks.
    joint_candidates: []?joint_candidates.Candidates,
    fn releaseEvidence(self: *WindowResult) void {
        if (self.classification_scores) |*scores| scores.deinit();
        self.classification_scores = null;
        for (self.joint_candidates) |*candidates| if (candidates.*) |*value| value.deinit();
        self.allocator.free(self.joint_candidates);
        self.joint_candidates = &.{};
    }
    pub fn deinit(self: *WindowResult) void {
        self.releaseEvidence();
        self.outputs.deinit();
        self.* = undefined;
    }
};

/// Explicit internal window stage: one classifier pass, no local classification
/// or JointIE solve, no entity overlap/scalar truncation, and no ordinary
/// relation semantic dedup. The document stage must globally merge and solve
/// before these intermediate outputs can become a public response.
pub fn runScoredWindows(allocator: Allocator, config: *const model.Config, prepared: *const processor.PreparedBatch, schemas: []const *const schema_mod.CompiledSchema, scores: ?scoring.CandidateScoreView, scorer: scoring.Scorer, options: Options, per_sample: []const Options) !WindowResult {
    return runScoredInternal(allocator, config, prepared, schemas, scores, scorer, options, per_sample, true);
}

fn runScoredInternal(allocator: Allocator, config: *const model.Config, prepared: *const processor.PreparedBatch, schemas: []const *const schema_mod.CompiledSchema, scores: ?scoring.CandidateScoreView, scorer: scoring.Scorer, options: Options, per_sample: []const Options, window_candidates: bool) !WindowResult {
    try scorer.check();
    if (options.control) |control| try control.check();
    try validateOptions(options);
    const batch = prepared.samples.len;
    if (per_sample.len != 0 and per_sample.len != batch) return error.InvalidBoundaryPipelineOptions;
    for (per_sample) |item| try validateOptions(item);
    if (config.version != model.config_version or config.architecture_version != model.architecture_version) return error.UnsupportedGlinerBoundaryVersion;
    try config.head.validate();
    if (batch == 0 or batch > options.head_limits.max_batch or prepared.word_width > options.head_limits.max_text_words or prepared.query_width > options.head_limits.max_queries or
        schemas.len != batch or prepared.query_marker_mask.len != try mul(batch, prepared.query_width)) return error.InvalidBoundaryPipelineInput;
    for (prepared.samples, schemas) |sample, schema| {
        if (!std.mem.eql(u8, &sample.schema_fingerprint, &schema.fingerprint) or sample.words.len > prepared.word_width or
            sample.queries.len > prepared.query_width or sample.classification_labels.len > prepared.classification_width) return error.InvalidBoundaryPipelineRouting;
        if (options.validate_value_fn == null) {
            for (schema.schema.entities) |entity| if (entity.validators.len > 0) return error.MissingExtractionValidator;
            for (schema.schema.structures) |structure| for (structure.fields) |field| if (field.validators.len > 0) return error.MissingExtractionValidator;
        }
    }
    if (scores) |view| {
        const count = try mul(batch, view.pool.capacity);
        const query_count = try mul(batch, prepared.query_width);
        if (view.batch != batch or view.text_length != prepared.word_width or view.queries != prepared.query_width or view.pool.batch != batch or view.pool.capacity == 0 or
            count > options.head_limits.max_pool_pair_elements or view.pool.indices.len != count or view.pool.valid.len != count) return error.InvalidBoundaryScorerOutput;
        const pair_count = try mul(query_count, view.pool.capacity);
        if (pair_count > options.head_limits.max_pool_pair_elements) return error.ResourceLimitExceeded;
        try finite(view.pair_logits, pair_count);
        if (view.null_logits) |values| try finite(values, query_count);
        if (view.count_log_rates) |values| try finite(values, query_count);
        for (view.pool.indices, view.pool.valid, 0..) |span, valid, index| {
            if (valid and (span.end <= span.start or span.end > prepared.samples[index / view.pool.capacity].words.len)) return error.InvalidBoundaryScorerOutput;
        }
    } else if (prepared.query_width != 0) return error.MissingBoundaryScores;
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    var work = Work{ .scorer = scorer, .allocator = undefined, .scratch_allocator = allocator, .config = config, .prepared = prepared, .schemas = schemas, .scores = scores, .options = options, .window_candidates = window_candidates };
    try work.check();
    var classification_scores: ?scoring.ClassificationScores = if (prepared.classification_width > 0) try scorer.classify(allocator, options.task_limits, options.control) else null;
    errdefer if (classification_scores) |*value| value.deinit();
    if (classification_scores) |result| try finite(result.logits, try mul(batch, prepared.classification_width));
    const joint_sets = try allocator.alloc(?joint_candidates.Candidates, if (window_candidates) batch else 0);
    @memset(joint_sets, null);
    errdefer {
        for (joint_sets) |*candidates| if (candidates.*) |*value| value.deinit();
        allocator.free(joint_sets);
    }
    const samples = try arena.allocator().alloc(Sample, batch);
    for (samples, 0..) |*sample, b| {
        // Scratch is discarded after each sample. Learned scorers and solvers
        // use the caller allocator directly, so their deinit releases buffers
        // immediately instead of retaining them behind later result writes.
        var scratch = std.heap.ArenaAllocator.init(allocator);
        defer scratch.deinit();
        work.allocator = scratch.allocator();
        work.options = if (per_sample.len == 0) options else optionsForSample(options, per_sample[b]);
        if (window_candidates) work.options.overlap = .allow;
        var offsets = try boundary.OffsetMap.init(allocator, prepared.samples[b].original_text, prepared.samples[b].original_text.len);
        defer offsets.deinit();
        const schema = schemas[b].schema;
        var decoded: Sample = undefined;
        if (schema.joint_ie != null) {
            if (window_candidates) {
                joint_sets[b] = try work.buildJoint(b);
                decoded = .{};
            } else decoded = try work.joint(b, offsets);
        } else {
            decoded = .{ .entities = try work.entities(b, offsets) };
            if (!window_candidates) if (classification_scores) |scored| try work.classify(b, scored.logits, &decoded);
            decoded.structures = try work.structures(b, offsets);
            decoded.relations = try work.relations(b, offsets);
        }
        sample.* = try cloneOwned(Sample, arena.allocator(), decoded);
    }
    try work.check();
    return .{ .allocator = allocator, .outputs = .{ .arena = arena, .samples = samples }, .classification_scores = classification_scores, .joint_candidates = joint_sets };
}

pub const ExpectedValue = struct {
    text: []const u8,
    confidence: f32,
    source: ?boundary.Offsets,
    attributes: []const struct { name: []const u8, labels: []const Label },
};
test "gliner boundary record cardinality remains distinct from scalar presentation" {
    const scalar: schema_mod.Field = .{ .name = "organization", .dtype = .str };
    try std.testing.expectEqual(schema_mod.Cardinality.zero_or_more, Work.fieldCardinality(scalar, false));
    try std.testing.expectEqual(schema_mod.Cardinality.required_one, Work.fieldCardinality(scalar, true));
    var explicit = scalar;
    explicit.cardinality = .optional_one;
    try std.testing.expectEqual(schema_mod.Cardinality.optional_one, Work.fieldCardinality(explicit, false));
}
pub const ExpectedSample = struct {
    entities: []const struct { name: []const u8, values: []const ExpectedValue },
    classifications: []const struct { name: []const u8, labels: []const Label },
    structures: []const struct { name: []const u8, instances: []const struct { fields: []const struct { name: []const u8, values: []const ExpectedValue } } },
    relations: []const struct { name: []const u8, head: ExpectedValue, tail: ExpectedValue, confidence: f32, derived: bool = false, head_entity_type: ?usize = null, tail_entity_type: ?usize = null },
};
fn expectLabels(expected: []const Label, actual: []const Label) !void {
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |want, got| {
        try std.testing.expectEqualStrings(want.label, got.label);
        try std.testing.expectApproxEqAbs(want.confidence, got.confidence, 5e-4);
    }
}
fn expectValue(expected: ExpectedValue, actual: Value) !void {
    errdefer std.debug.print("value {s}: expected confidence {d}, actual {d}\n", .{ expected.text, expected.confidence, actual.confidence });
    try std.testing.expectEqualStrings(expected.text, actual.text);
    try std.testing.expectApproxEqAbs(expected.confidence, actual.confidence, 5e-4);
    if (expected.source) |source| {
        try std.testing.expect(actual.source != null);
        try std.testing.expectEqual(source.start, actual.source.?.start);
        try std.testing.expectEqual(source.end, actual.source.?.end);
    } else try std.testing.expect(actual.source == null);
    try std.testing.expectEqual(expected.attributes.len, actual.attributes.len);
    for (expected.attributes, 0..) |want, index| {
        for (expected.attributes[0..index]) |prior| try std.testing.expect(!std.mem.eql(u8, prior.name, want.name));
        var found: ?Attribute = null;
        for (actual.attributes) |got| if (std.mem.eql(u8, want.name, got.name)) {
            try std.testing.expect(found == null);
            found = got;
        };
        try std.testing.expect(found != null);
        try expectLabels(want.labels, found.?.labels);
    }
}
fn expectValues(expected: []const ExpectedValue, actual: []const Value) !void {
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |want, got| try expectValue(want, got);
}
pub fn expectSample(expected: ExpectedSample, actual: Sample) !void {
    try std.testing.expectEqual(expected.entities.len, actual.entities.len);
    for (expected.entities, actual.entities) |want, got| {
        try std.testing.expectEqualStrings(want.name, got.name);
        try expectValues(want.values, got.values);
    }
    try std.testing.expectEqual(expected.classifications.len, actual.classifications.len);
    for (expected.classifications, actual.classifications) |want, got| {
        try std.testing.expectEqualStrings(want.name, got.name);
        try expectLabels(want.labels, got.labels);
    }
    try std.testing.expectEqual(expected.structures.len, actual.structures.len);
    for (expected.structures, actual.structures) |want, got| {
        try std.testing.expectEqualStrings(want.name, got.name);
        try std.testing.expectEqual(want.instances.len, got.instances.len);
        for (want.instances, got.instances) |want_record, got_record| {
            try std.testing.expectEqual(want_record.fields.len, got_record.fields.len);
            for (want_record.fields, got_record.fields) |want_field, got_field| {
                try std.testing.expectEqualStrings(want_field.name, got_field.name);
                try expectValues(want_field.values, got_field.values);
            }
        }
    }
    try std.testing.expectEqual(expected.relations.len, actual.relations.len);
    for (expected.relations, actual.relations) |want, got| {
        try std.testing.expectEqualStrings(want.name, got.name);
        try expectValue(want.head, got.head);
        try expectValue(want.tail, got.tail);
        try std.testing.expectApproxEqAbs(want.confidence, got.confidence, 5e-4);
        try std.testing.expectEqual(want.derived, got.derived);
        try std.testing.expectEqual(want.head_entity_type, got.head_entity_type);
        try std.testing.expectEqual(want.tail_entity_type, got.tail_entity_type);
    }
}

pub const PublishedModelPin = struct { sha256: []const u8, size_bytes: usize };
pub const PublishedModelFiles = struct {
    @"config.json": PublishedModelPin,
    @"encoder_config/config.json": PublishedModelPin,
    @"model.safetensors": PublishedModelPin,
    @"tokenizer.json": PublishedModelPin,
    @"tokenizer_config.json": PublishedModelPin,
};
pub const ReferenceFixture = struct {
    format_version: u32,
    source_commit: []const u8,
    model: []const u8,
    model_id: []const u8,
    revision: []const u8,
    model_files: PublishedModelFiles,
    model_sha256: []const u8,
    tokenizer_sha256: []const u8,
    reference_sha256: []const u8,
    requests_sha256: []const u8,
    offset_unit: []const u8,
    cases: []const struct { id: []const u8, text: []const u8, schema: std.json.Value, expected: ExpectedSample },
};
fn expectPinnedBytes(expected: PublishedModelPin, bytes: []const u8) !void {
    try std.testing.expectEqual(expected.size_bytes, bytes.len);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    const actual_hash = std.fmt.bytesToHex(digest, .lower);
    try std.testing.expectEqualStrings(expected.sha256, &actual_hash);
}
fn readPinnedFile(a: Allocator, directory: []const u8, name: []const u8, expected: PublishedModelPin) ![]u8 {
    const path = try std.fs.path.join(a, &.{ directory, name });
    defer a.free(path);
    const bytes = try @import("../util/c_file.zig").readFile(a, path);
    errdefer a.free(bytes);
    try expectPinnedBytes(expected, bytes);
    return bytes;
}
fn publishedCheckpointParity(comptime variant: []const u8, comptime environment: [:0]const u8, comptime fixture_name: []const u8) !void {
    const directory = @import("antfly_platform").env.getenv(environment) orelse return error.SkipZigTest;
    const fixtures = @import("../architectures/gliner_boundary_parity_test.zig");
    const safetensors = @import("../models/safetensors.zig");
    const native = @import("../ops/native_compute.zig");
    const engine = @import("../architectures/gliner_boundary_engine.zig");
    const a = std.testing.allocator;
    const bytes = try fixtures.fixtureBytes(a, fixture_name);
    defer a.free(bytes);
    const fixture = try std.json.parseFromSlice(ReferenceFixture, a, bytes, .{});
    defer fixture.deinit();
    try std.testing.expectEqual(@as(u32, 2), fixture.value.format_version);
    try std.testing.expectEqualStrings("3c913c7369301133d3b7699252074c4303ada50e", fixture.value.source_commit);
    try std.testing.expectEqualStrings(variant, fixture.value.model);
    try std.testing.expectEqualStrings("fastino/gliner2.5-" ++ variant ++ "-v1", fixture.value.model_id);
    try std.testing.expectEqualStrings("unicode_codepoints", fixture.value.offset_unit);
    const reference_bytes = try fixtures.fixtureBytes(a, variant ++ "_reference/capture.json");
    defer a.free(reference_bytes);
    try expectPinnedBytes(.{ .sha256 = fixture.value.reference_sha256, .size_bytes = reference_bytes.len }, reference_bytes);
    const requests_bytes = try fixtures.fixtureBytes(a, "requests.json");
    defer a.free(requests_bytes);
    try expectPinnedBytes(.{ .sha256 = fixture.value.requests_sha256, .size_bytes = requests_bytes.len }, requests_bytes);
    const weight_path = try std.fs.path.join(a, &.{ directory, "model.safetensors" });
    defer a.free(weight_path);
    var weights = fixtures.TensorFixture{ .allocator = a, .reader = try safetensors.MMapReader.openFileAbsolute(a, weight_path) };
    defer weights.deinit();
    try expectPinnedBytes(fixture.value.model_files.@"model.safetensors", weights.reader.file_bytes);
    try std.testing.expectEqualStrings(fixture.value.model_sha256, fixture.value.model_files.@"model.safetensors".sha256);
    const tokenizer_bytes = try readPinnedFile(a, directory, "tokenizer.json", fixture.value.model_files.@"tokenizer.json");
    defer a.free(tokenizer_bytes);
    try std.testing.expectEqualStrings(fixture.value.tokenizer_sha256, fixture.value.model_files.@"tokenizer.json".sha256);
    const tokenizer = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    defer tokenizer.tokenizer().deinitTokenizer();
    const config_bytes = try readPinnedFile(a, directory, "config.json", fixture.value.model_files.@"config.json");
    defer a.free(config_bytes);
    const encoder_bytes = try readPinnedFile(a, directory, "encoder_config/config.json", fixture.value.model_files.@"encoder_config/config.json");
    defer a.free(encoder_bytes);
    const tokenizer_config = try readPinnedFile(a, directory, "tokenizer_config.json", fixture.value.model_files.@"tokenizer_config.json");
    defer a.free(tokenizer_config);
    const config = try model.parseConfig(a, config_bytes, encoder_bytes);
    var store = try weights.loadWeights();
    defer store.deinitOwned();
    var backend = native.NativeCompute.init(a, &store, null);
    defer backend.deinit();
    const cb = backend.computeBackend();
    for (fixture.value.cases) |case| {
        errdefer std.debug.print(variant ++ " checkpoint pipeline fixture: {s}\n", .{case.id});
        const schema_json = try std.json.Stringify.valueAlloc(a, case.schema, .{});
        defer a.free(schema_json);
        var schema = try schema_mod.compile(a, schema_json, .{});
        defer schema.deinit();
        var prepared = try processor.prepare(a, tokenizer.tokenizer(), &.{.{ .text = case.text, .schema = &schema }}, .{});
        defer prepared.deinit();
        var encoded = try engine.encodeNative(&cb, a, &config, &prepared, .{});
        defer encoded.deinit();
        var result = try runNative(&cb, a, &config, &prepared, &.{&schema}, .{ .text_states = encoded.text_states, .query_states = encoded.query_states, .classification_states = encoded.classification_states, .text_lengths = encoded.text_lengths }, .{ .offset_unit = .unicode_codepoints });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.samples.len);
        try expectSample(case.expected, result.samples[0]);
    }
}

test "gliner boundary pipeline Python parity pinned small checkpoint all inference tasks" {
    try publishedCheckpointParity("small", "ANTFLY_GLINER25_SMALL_MODEL_DIR", "pipeline_cases.json");
}
test "gliner boundary pipeline Python parity pinned base checkpoint all inference tasks" {
    try publishedCheckpointParity("base", "ANTFLY_GLINER25_BASE_MODEL_DIR", "pipeline_cases_base.json");
}
test "gliner boundary pipeline Python parity pinned multi checkpoint all inference tasks" {
    try publishedCheckpointParity("multi", "ANTFLY_GLINER25_MULTI_MODEL_DIR", "pipeline_cases_multi.json");
}

// These fixtures exercise the admitted encoded-state boundary directly. The
// classifier weights/states come from the independent tiny Python capture;
// no encoder or downloaded model is needed for lifecycle and solver tests.
fn classificationTestBatch(a: Allocator, schemas: []const *const schema_mod.CompiledSchema) !processor.PreparedBatch {
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const alloc = arena.allocator();
    const samples = try alloc.alloc(processor.Sample, schemas.len);
    for (samples, schemas) |*sample, schema| {
        var labels = std.ArrayListUnmanaged(processor.ClassificationLabel).empty;
        for (schema.schema.classifications, 0..) |task, t| for (task.task.labels, 0..) |label, l| {
            try labels.append(alloc, .{ .group_index = t, .schema_index = t, .label_index = l, .name = label, .marker_index = labels.items.len });
        };
        if (labels.items.len > 4) return error.InvalidFixtureTensor;
        sample.* = .{
            .original_text = "İ.",
            .schema_fingerprint = schema.fingerprint,
            .input_ids = &.{},
            .words = &.{.{ .text = "i̇", .source = .{ .start = 0, .end = 2 }, .input_start = 0, .input_end = 1 }},
            .groups = &.{},
            .queries = &.{},
            .classification_labels = try labels.toOwnedSlice(alloc),
            .enum_choices = &.{},
            .prefix_word_count = 0,
            .body_word_count = 1,
            .terminal_period_added = false,
            .is_joint_ie = false,
        };
    }
    return .{
        .arena = arena,
        .samples = samples,
        .sequence_length = 0,
        .word_width = 1,
        .query_width = 0,
        .classification_width = 4,
        .group_width = 0,
        .input_ids = &.{},
        .attention_mask = &.{},
        .text_word_indices = &.{},
        .text_word_mask = &.{},
        .query_marker_indices = &.{},
        .query_marker_mask = &.{},
        .query_group_index = &.{},
        .cls_marker_indices = &.{},
        .cls_marker_mask = &.{},
        .cls_group_index = &.{},
        .parent_marker_indices = &.{},
        .parent_marker_mask = &.{},
    };
}

test "gliner boundary pipeline heterogeneous batch ownership strict best effort cancellation and allocation cleanup" {
    const a = std.testing.allocator;
    const fixtures = @import("../architectures/gliner_boundary_parity_test.zig");
    const native = @import("../ops/native_compute.zig");
    const config_bytes = try fixtures.fixtureBytes(a, "models/base/config.json");
    defer a.free(config_bytes);
    const encoder_bytes = try fixtures.fixtureBytes(a, "models/base/encoder_config.json");
    defer a.free(encoder_bytes);
    var config = try model.parseConfig(a, config_bytes, encoder_bytes);
    config.encoder.hidden_size = 32;
    config.head.dropout = 0;
    var weights = try fixtures.TensorFixture.init(a, "tasks/weights.safetensors");
    defer weights.deinit();
    var reference = try fixtures.TensorFixture.init(a, "tasks/tensors.safetensors");
    defer reference.deinit();
    var store = try weights.loadWeights();
    defer store.deinitOwned();
    var backend = native.NativeCompute.init(a, &store, null);
    defer backend.deinit();
    const cb = backend.computeBackend();
    const cls_states = try reference.floats("classification.input.states");
    const core = CoreView{ .text_states = &([_]f32{0} ** 64), .query_states = &.{}, .classification_states = cls_states, .text_lengths = &.{ 1, 1 } };
    var retained = blk: {
        var first = try schema_mod.compile(a, "{\"classifications\":[{\"name\":\"choice\",\"labels\":[\"x\",\"y\"],\"min_labels\":1,\"max_labels\":1}]}", .{});
        defer first.deinit();
        var second = try schema_mod.compile(a, "{\"classifications\":[{\"name\":\"tone\",\"labels\":[\"negative\",\"positive\"]},{\"name\":\"tags\",\"labels\":[\"a\",\"b\"],\"multi_label\":true}]}", .{});
        defer second.deinit();
        const schemas = [_]*const schema_mod.CompiledSchema{ &first, &second };
        var prepared = try classificationTestBatch(a, &schemas);
        defer prepared.deinit();
        const common = Options{ .classification_solver = .{ .algorithm = .exact } };
        var items = [_]Options{ .{ .best_effort = true, .classification_solver = .{ .algorithm = .exact, .exact_node_budget = 1 } }, .{} };
        var strict = items;
        strict[0].best_effort = false;
        try std.testing.expectError(error.ClassificationSearchExhausted, runNativePerSample(&cb, a, &config, &prepared, &schemas, core, common, &strict));
        var no_witness = items;
        no_witness[0].classification_solver.exact_node_budget = 0;
        try std.testing.expectError(error.ClassificationSearchExhausted, runNativePerSample(&cb, a, &config, &prepared, &schemas, core, common, &no_witness));
        var window_scorer = scoring.NativeContext{ .cb = &cb, .config = &config, .prepared = &prepared, .core = core, .scores = null };
        var window_outputs = try runScoredWindows(a, &config, &prepared, &schemas, null, window_scorer.scorer(), common, &no_witness);
        defer window_outputs.deinit();
        try std.testing.expectEqual(@as(usize, 8), window_outputs.classification_scores.?.logits.len);
        for (window_outputs.outputs.samples) |sample| {
            try std.testing.expectEqual(@as(usize, 0), sample.classifications.len);
            try std.testing.expect(sample.classification_solver == null and sample.joint_solver == null);
        }
        for (window_outputs.joint_candidates) |candidates| try std.testing.expect(candidates == null);
        var small_common = common;
        small_common.max_output_values = 1;
        try std.testing.expectError(error.ExtractionOutputLimitExceeded, runNativePerSample(&cb, a, &config, &prepared, &schemas, core, small_common, &items));
        try std.testing.expectError(error.InvalidBoundaryPipelineOptions, runNativePerSample(&cb, a, &config, &prepared, &schemas, core, common, items[0..1]));
        var invalid = items;
        invalid[1].classification_solver.beam_width = 0;
        var malformed = core;
        malformed.classification_states = &.{std.math.nan(f32)};
        // Every item is validated before scanning states or running the head.
        try std.testing.expectError(error.InvalidBoundaryPipelineOptions, runNativePerSample(&cb, a, &config, &prepared, &schemas, malformed, common, &invalid));
        try std.testing.expectError(error.InvalidBoundaryPipelineRouting, runNativePerSample(&cb, a, &config, &prepared, &.{ &second, &first }, core, common, &items));
        const Cancellation = struct {
            visits: usize = 0,
            stop_after: usize,
            fn check(raw: ?*anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw.?));
                self.visits += 1;
                if (self.visits >= self.stop_after) return error.Cancelled;
            }
        };
        var cancelled = Cancellation{ .stop_after = 1 };
        var controlled = common;
        controlled.best_effort = true;
        controlled.control = .{ .ptr = &cancelled, .check_fn = Cancellation.check };
        try std.testing.expectError(error.Cancelled, runNativePerSample(&cb, a, &config, &prepared, &schemas, malformed, controlled, &items));
        cancelled = .{ .stop_after = 6 };
        try std.testing.expectError(error.Cancelled, runNativePerSample(&cb, a, &config, &prepared, &schemas, core, controlled, &items));
        try std.testing.expectEqual(@as(usize, 6), cancelled.visits);
        const Check = struct {
            fn run(allocator: Allocator, backend_: *const compute.ComputeBackend, config_: *const model.Config, prepared_: *const processor.PreparedBatch, schemas_: []const *const schema_mod.CompiledSchema, core_: CoreView, common_: Options, items_: []const Options) !void {
                var result = try runNativePerSample(backend_, allocator, config_, prepared_, schemas_, core_, common_, items_);
                defer result.deinit();
                try std.testing.expectEqual(@as(usize, 2), result.samples.len);
            }
            fn windows(allocator: Allocator, backend_: *const compute.ComputeBackend, config_: *const model.Config, prepared_: *const processor.PreparedBatch, schemas_: []const *const schema_mod.CompiledSchema, core_: CoreView, common_: Options, items_: []const Options) !void {
                var owner = scoring.NativeContext{ .cb = backend_, .config = config_, .prepared = prepared_, .core = core_, .scores = null };
                var result = try runScoredWindows(allocator, config_, prepared_, schemas_, null, owner.scorer(), common_, items_);
                defer result.deinit();
                try std.testing.expectEqual(@as(usize, 8), result.classification_scores.?.logits.len);
                try std.testing.expectEqual(@as(usize, 0), result.outputs.samples[0].classifications.len);
            }
        };
        try std.testing.checkAllAllocationFailures(a, Check.run, .{ &cb, &config, &prepared, &schemas, core, common, &items });
        try std.testing.checkAllAllocationFailures(a, Check.windows, .{ &cb, &config, &prepared, &schemas, core, common, &no_witness });
        // A later successful call demonstrates recovery. Results must remain
        // usable after both source schemas and prepared metadata are freed.
        break :blk try runNativePerSample(&cb, a, &config, &prepared, &schemas, core, common, &items);
    };
    defer retained.deinit();
    try std.testing.expectEqual(@as(usize, 2), retained.samples.len);
    const first = retained.samples[0];
    try std.testing.expect(first.classification_solver.?.exhausted);
    try std.testing.expectEqual(SolverStatus.feasible, first.classification_solver.?.status);
    try std.testing.expectEqualStrings("choice", first.classifications[0].name);
    try std.testing.expectEqualStrings("x", first.classifications[0].labels[0].label);
    const second = retained.samples[1];
    try std.testing.expect(second.classification_solver == null);
    try std.testing.expectEqualStrings("tone", second.classifications[0].name);
    try std.testing.expectEqualStrings("positive", second.classifications[0].labels[0].label);
    try std.testing.expectEqualStrings("tags", second.classifications[1].name);
    try std.testing.expect(second.classifications[1].multi_label);
    try std.testing.expectEqual(@as(usize, 2), second.classifications[1].labels.len);
}

test "gliner boundary pipeline enum validators cover source free fallback required fields and literal override" {
    const a = std.testing.allocator;
    const regex = @import("extraction_regex.zig");
    const fixtures = @import("../architectures/gliner_boundary_parity_test.zig");
    const config_bytes = try fixtures.fixtureBytes(a, "models/base/config.json");
    defer a.free(config_bytes);
    const encoder_bytes = try fixtures.fixtureBytes(a, "models/base/encoder_config.json");
    defer a.free(encoder_bytes);
    const config = try model.parseConfig(a, config_bytes, encoder_bytes);
    const Stub = struct {
        explicit_calls: usize = 0,
        record_calls: usize = 0,
        fn check(_: *anyopaque) !void {}
        fn classify(_: *anyopaque, _: Allocator, _: tasks.Limits, _: ?Control) !scoring.ClassificationScores {
            return error.UnexpectedScorerCall;
        }
        fn relations(_: *anyopaque, _: Allocator, _: scoring.RelationRequest, _: tasks.Limits, _: ?Control) !scoring.RelationScores {
            return error.UnexpectedScorerCall;
        }
        fn explicit(raw: *anyopaque, allocator: Allocator, request: scoring.ExplicitRequest, _: head.Limits, _: ?Control) !scoring.ExplicitScores {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.explicit_calls += 1;
            try std.testing.expectEqual(@as(usize, 2), request.indices.len);
            var storage = try scoring.Storage.init(allocator);
            errdefer storage.deinit();
            return .{ .storage = storage, .logits = try storage.alloc().dupe(f32, &.{ 10, 5 }), .valid = try storage.alloc().dupe(bool, &.{ true, true }) };
        }
        fn record(raw: *anyopaque, allocator: Allocator, request: scoring.RecordRequest, _: tasks.Limits, _: ?Control) !scoring.RecordScores {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.record_calls += 1;
            try std.testing.expectEqual(@as(usize, 1), request.fields.len);
            try std.testing.expectEqualSlices(usize, &.{0}, request.fields[0].pool_indices);
            var storage = try scoring.Storage.init(allocator);
            errdefer storage.deinit();
            const alloc = storage.alloc();
            const fields = try alloc.alloc(tasks.RecordFieldResult, 1);
            fields[0] = .{ .candidate_spans = try alloc.dupe(ops.Span, &.{.{ .start = 2, .end = 3 }}), .candidate_logits = try alloc.dupe(f32, &.{4}), .assign_logits = try alloc.dupe(f32, &.{ -4, 4 }) };
            return .{ .storage = storage, .object_logits = try alloc.dupe(f32, &.{5}), .instance_seeds = try alloc.dupe(?tasks.RecordSeed, &.{null}), .fields = fields };
        }
        fn scorer(self: *@This()) scoring.Scorer {
            return .{ .context = self, .check_fn = check, .classify_fn = classify, .explicit_fn = explicit, .relations_fn = relations, .record_fn = record };
        }
    };
    const cases = [_][]const u8{
        \\{"structures":{"event":{"fields":{"kind":{"type":"str","choices":["bad","good"],"validators":[{"type":"regex","pattern":"^good$","flags":0}]}}}}}
        ,
        \\{"structures":{"event":{"mode":"anchorless","fields":{"kind":{"type":"str","cardinality":"required_one","choices":["bad","good"],"validators":[{"type":"regex","pattern":"^good$","flags":0}]}}}}}
        ,
        \\{"structures":{"event":{"mode":"anchorless","fields":{"kind":{"type":"str","cardinality":"required_one","choices":["bad","good"],"validators":[{"type":"regex","pattern":"^never$","flags":0}]}}}}}
        ,
        \\{"structures":{"event":{"fields":{"kind":{"type":"str","cardinality":"required_one","choices":["bad","good"],"validators":[{"type":"regex","pattern":"^never$","flags":0}]}}}}}
        ,
    };
    for (cases, 0..) |json, case_index| {
        var validator = regex.Context.init(a, .{});
        defer validator.deinit();
        var compiled = try schema_mod.compile(a, json, validator.compilerOptions(.{}));
        defer compiled.deinit();
        const schemas = [_]*const schema_mod.CompiledSchema{&compiled};
        var prepared = try classificationTestBatch(a, &schemas);
        defer prepared.deinit();
        prepared.word_width = 3;
        prepared.query_width = 1;
        prepared.classification_width = 0;
        var query_mask = [_]bool{true};
        prepared.query_marker_mask = &query_mask;
        const sample = &@constCast(prepared.samples)[0];
        sample.original_text = "Z";
        sample.prefix_word_count = 2;
        sample.words = &.{ .{ .text = "bad", .source = null, .input_start = 0, .input_end = 1 }, .{ .text = "good", .source = null, .input_start = 1, .input_end = 2 }, .{ .text = "z", .source = .{ .start = 0, .end = 1 }, .input_start = 2, .input_end = 3 } };
        sample.queries = &.{.{ .kind = .field, .group_index = 0, .schema_index = 0, .role_index = 0, .label_index = 0, .name = "kind", .marker_index = 0 }};
        sample.enum_choices = &.{ .{ .structure_index = 0, .field_index = 0, .choice_index = 0, .start = 0, .end = 1, .score_start = 0 }, .{ .structure_index = 0, .field_index = 0, .choice_index = 1, .start = 1, .end = 2, .score_start = 1 } };
        var indices = [_]ops.Span{.{ .start = 2, .end = 3 }};
        var valid = [_]bool{true};
        const pool = ops.SharedPool{ .allocator = a, .batch = 1, .capacity = 1, .indices = &indices, .valid = &valid, .proposal_logits = &.{}, .compat_logits = &.{} };
        const scores = scoring.CandidateScoreView{ .batch = 1, .text_length = 3, .queries = 1, .pool = &pool, .pair_logits = &.{4}, .null_logits = null, .count_log_rates = null };
        var stub = Stub{};
        const options = Options{ .regex_context = &validator, .validate_value_fn = regex.Context.validateValue };
        if (case_index >= 2) {
            try std.testing.expectError(error.RequiredRecordFieldMissing, runScored(a, &config, &prepared, &schemas, scores, stub.scorer(), options));
            try std.testing.expectEqual(@as(usize, 0), stub.record_calls);
            if (case_index == 3) {
                var window = try runScoredWindows(a, &config, &prepared, &schemas, scores, stub.scorer(), options, &.{});
                defer window.deinit();
                try std.testing.expectEqual(@as(usize, 0), window.outputs.samples[0].structures.len);
            }
            continue;
        }
        var result = try runScored(a, &config, &prepared, &schemas, scores, stub.scorer(), options);
        defer result.deinit();
        const values = result.samples[0].structures[0].instances[0].fields[0].values;
        try std.testing.expectEqual(@as(usize, 1), values.len);
        try std.testing.expectEqualStrings("good", values[0].text);
        try std.testing.expect(values[0].source == null and values[0].derived);
        try std.testing.expectEqual(@as(usize, 1), stub.explicit_calls);
        try std.testing.expectEqual(@as(usize, if (case_index == 1) 1 else 0), stub.record_calls);
        if (case_index == 0) {
            var high_threshold = options;
            high_threshold.threshold = 0.99999;
            var window = try runScoredWindows(a, &config, &prepared, &schemas, scores, stub.scorer(), high_threshold, &.{});
            defer window.deinit();
            try std.testing.expectEqualStrings("good", window.outputs.samples[0].structures[0].instances[0].fields[0].values[0].text);
            try std.testing.expect(window.outputs.samples[0].structures[0].instances[0].fields[0].values[0].confidence < high_threshold.threshold);
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            var work = Work{ .scorer = stub.scorer(), .allocator = arena.allocator(), .scratch_allocator = a, .config = &config, .prepared = &prepared, .schemas = &schemas, .scores = scores, .options = options };
            const choices_ = try work.choices(0, 0, 0, 0);
            var offsets = try boundary.OffsetMap.init(a, "bad good", 8);
            defer offsets.deinit();
            const final = try work.recordChoices(0, compiled.schema.structures[0].fields[0], true, choices_, &.{ .{ .choice = 0, .start = 0, .end = 3 }, .{ .choice = 1, .start = 4, .end = 8 } }, &.{}, &.{.{ .start = 0, .end = 3, .unit = .utf8_bytes, .byte_start = 0, .byte_end = 3 }}, 0, offsets);
            try std.testing.expectEqualStrings("good", final[0].text);
            try std.testing.expectEqual(@as(usize, 4), final[0].source.?.byte_start);
            try std.testing.expect(!try work.enumPoolCandidateAllowed(0, 0, 0, .{ .start = 0, .end = 1 }, choices_, &.{}, offsets));
            try std.testing.expect(try work.enumPoolCandidateAllowed(0, 0, 0, .{ .start = 1, .end = 2 }, choices_, &.{}, offsets));
        }
    }
}

test "gliner boundary classification presentation shares temperature fallback precision and limits" {
    const a = std.testing.allocator;
    var ordinary = try schema_mod.compile(a,
        \\{"classifications":[{"name":"pick","labels":["first","second","third"],"top_k":2},{"name":"tags","labels":["a","b"],"multi_label":true,"threshold":0.99}]}
    , .{});
    defer ordinary.deinit();
    const raw = [_][]const f64{ &.{ 2, 0, 1 }, &.{ -10, -9 } };
    var ordinary_output = try presentClassifications(a, &ordinary, &raw, 2, .{});
    defer ordinary_output.deinit();
    try std.testing.expect(ordinary_output.diagnostics == null);
    try std.testing.expectEqual(@as(usize, 3), ordinary_output.output_values);
    try std.testing.expectEqualStrings("first", ordinary_output.classifications[0].labels[0].label);
    try std.testing.expectEqualStrings("third", ordinary_output.classifications[0].labels[1].label);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5064804), ordinary_output.classifications[0].labels[0].confidence, 0.000001);
    try std.testing.expectEqualStrings("b", ordinary_output.classifications[1].labels[0].label);
    try std.testing.expectApproxEqAbs(boundary.sigmoid(-4.5), ordinary_output.classifications[1].labels[0].confidence, 0.000001);
    try std.testing.expectError(error.InvalidBoundaryPipelineInput, presentClassifications(a, &ordinary, raw[0..1], 2, .{}));
    try std.testing.expectError(error.NonFiniteBoundaryScore, presentClassifications(a, &ordinary, &.{ &.{ 0, std.math.nan(f64), 0 }, raw[1] }, 2, .{}));
    try std.testing.expectError(error.ExtractionOutputLimitExceeded, presentClassifications(a, &ordinary, &raw, 2, .{ .max_output_values = 2 }));
    try std.testing.expectError(error.ExtractionOutputLimitExceeded, presentClassifications(a, &ordinary, &raw, 2, .{ .max_output_string_bytes = 2 }));
    var constrained = try schema_mod.compile(a,
        \\{"classifications":[{"name":"choice","labels":["a","b"],"min_labels":1,"max_labels":1,"temperature":2}]}
    , .{});
    defer constrained.deinit();
    var calibrated = try presentClassifications(a, &constrained, &.{&.{ 0, 2 }}, 99, .{ .classification_solver = .{ .algorithm = .exact } });
    defer calibrated.deinit();
    try std.testing.expectEqual(SolverStatus.optimal, calibrated.diagnostics.?.status);
    try std.testing.expectEqualStrings("b", calibrated.classifications[0].labels[0].label);
    try std.testing.expectApproxEqAbs(@as(f32, 0.7310586), calibrated.classifications[0].labels[0].confidence, 0.000001);
    // The final confidence rounds to f32, while global assignment retains the
    // meaningful f64 difference introduced by raw-window aggregation.
    var precise = try presentClassifications(a, &constrained, &.{&.{ 1, 1.000000001 }}, 99, .{ .classification_solver = .{ .algorithm = .exact } });
    defer precise.deinit();
    try std.testing.expectEqualStrings("b", precise.classifications[0].labels[0].label);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), precise.classifications[0].labels[0].confidence, 0.000001);
    const Check = struct {
        fn run(allocator: Allocator, compiled: *const schema_mod.CompiledSchema, rows: []const []const f64) !void {
            var output = try presentClassifications(allocator, compiled, rows, 2, .{});
            defer output.deinit();
            try std.testing.expectEqual(@as(usize, 3), output.output_values);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Check.run, .{ &ordinary, &raw });
}

test "gliner boundary classification top_k preserves ordinary and structured presentation" {
    const a = std.testing.allocator;
    var ordinary = try schema_mod.compile(a,
        \\{"classifications":[{"name":"pick","labels":["a","b","c"],"top_k":2},{"name":"one","labels":["x","y"],"top_k":1},{"name":"tags","labels":["left","right"],"multi_label":true,"top_k":1}]}
    , .{});
    defer ordinary.deinit();
    const ordinary_raw = [_][]const f64{ &.{ 2, 0, 1 }, &.{ 0, 4 }, &.{ 3, 2 } };
    const Ordinary = struct {
        fn run(allocator: Allocator, compiled: *const schema_mod.CompiledSchema, rows: []const []const f64) !void {
            var output = try presentClassifications(allocator, compiled, rows, 1, .{});
            defer output.deinit();
            try std.testing.expect(output.diagnostics == null);
            try std.testing.expectEqual(@as(usize, 5), output.output_values);
            try std.testing.expectEqual(@as(usize, 2), output.classifications[0].labels.len);
            try std.testing.expectEqualStrings("a", output.classifications[0].labels[0].label);
            try std.testing.expectEqualStrings("c", output.classifications[0].labels[1].label);
            try std.testing.expectEqual(@as(usize, 1), output.classifications[1].labels.len);
            try std.testing.expectEqualStrings("y", output.classifications[1].labels[0].label);
            // Ordinary multi-label selection still uses its threshold, even
            // when legacy top_k is explicitly present.
            try std.testing.expectEqual(@as(usize, 2), output.classifications[2].labels.len);
            try std.testing.expectEqualStrings("left", output.classifications[2].labels[0].label);
            try std.testing.expectEqualStrings("right", output.classifications[2].labels[1].label);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Ordinary.run, .{ &ordinary, &ordinary_raw });

    // Omitting top_k lets the whole collection use declared cardinalities.
    var mixed = try schema_mod.compile(a,
        \\{"classifications":[{"name":"ordinary","labels":["a","b","c"]},{"name":"structured","labels":["x","y"],"mode":"multi","min_labels":2,"max_labels":2}]}
    , .{});
    defer mixed.deinit();
    const mixed_raw = [_][]const f64{ &.{ 1, 5, -1 }, &.{ 5, 4 } };
    const Structured = struct {
        fn run(allocator: Allocator, compiled: *const schema_mod.CompiledSchema, rows: []const []const f64) !void {
            var output = try presentClassifications(allocator, compiled, rows, 1, .{ .classification_solver = .{ .algorithm = .exact } });
            defer output.deinit();
            try std.testing.expectEqual(SolverStatus.optimal, output.diagnostics.?.status);
            try std.testing.expectEqual(@as(usize, 3), output.output_values);
            try std.testing.expectEqual(@as(usize, 1), output.classifications[0].labels.len);
            try std.testing.expectEqualStrings("b", output.classifications[0].labels[0].label);
            try std.testing.expectEqual(@as(usize, 2), output.classifications[1].labels.len);
            try std.testing.expectEqualStrings("x", output.classifications[1].labels[0].label);
            try std.testing.expectEqualStrings("y", output.classifications[1].labels[1].label);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Structured.run, .{ &mixed, &mixed_raw });

    // Independent callers cannot bypass compilation and silently discard a
    // non-default top_k. Reject before the first presentation allocation.
    var classifications = [_]schema_mod.Classification{ mixed.schema.classifications[0], mixed.schema.classifications[1] };
    var malformed = mixed;
    malformed.schema.classifications = &classifications;
    for (0..classifications.len) |index| {
        classifications[index].top_k = 2;
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
        try std.testing.expectError(error.ConstrainedClassificationTopKUnsupported, presentClassifications(failing.allocator(), &malformed, &mixed_raw, 1, .{}));
        classifications[index].top_k = 1;
    }
    try Structured.run(a, &mixed, &mixed_raw);
}

test "gliner boundary pipeline attribute labels consume the shared value budget" {
    const a = std.testing.allocator;
    const fixtures = @import("../architectures/gliner_boundary_parity_test.zig");
    const config_bytes = try fixtures.fixtureBytes(a, "models/base/config.json");
    defer a.free(config_bytes);
    const encoder_bytes = try fixtures.fixtureBytes(a, "models/base/encoder_config.json");
    defer a.free(encoder_bytes);
    const config = try model.parseConfig(a, config_bytes, encoder_bytes);
    var compiled = try schema_mod.compile(a, "{\"entities\":[\"person\"],\"entity_attributes\":{\"mood\":{\"labels\":[\"happy\",\"calm\"],\"multi_label\":true}}}", .{});
    defer compiled.deinit();
    const schemas = [_]*const schema_mod.CompiledSchema{&compiled};
    var prepared = try classificationTestBatch(a, &schemas);
    defer prepared.deinit();
    prepared.query_width = 3;
    prepared.classification_width = 0;
    const sample = &@constCast(prepared.samples)[0];
    sample.queries = &.{
        .{ .kind = .entity, .group_index = 0, .schema_index = 0, .role_index = 0, .label_index = 0, .name = "person", .marker_index = 0 },
        .{ .kind = .attribute, .group_index = 0, .schema_index = 0, .role_index = 0, .label_index = 0, .name = "happy", .marker_index = 1 },
        .{ .kind = .attribute, .group_index = 0, .schema_index = 0, .role_index = 0, .label_index = 1, .name = "calm", .marker_index = 2 },
    };
    const Stub = struct {
        fn check(_: *anyopaque) !void {}
        fn classify(_: *anyopaque, _: Allocator, _: tasks.Limits, _: ?Control) !scoring.ClassificationScores {
            return error.UnexpectedScorerCall;
        }
        fn relations(_: *anyopaque, _: Allocator, _: scoring.RelationRequest, _: tasks.Limits, _: ?Control) !scoring.RelationScores {
            return error.UnexpectedScorerCall;
        }
        fn record(_: *anyopaque, _: Allocator, _: scoring.RecordRequest, _: tasks.Limits, _: ?Control) !scoring.RecordScores {
            return error.UnexpectedScorerCall;
        }
        fn explicit(_: *anyopaque, allocator: Allocator, request: scoring.ExplicitRequest, _: head.Limits, _: ?Control) !scoring.ExplicitScores {
            try std.testing.expectEqualSlices(usize, &.{ 1, 2 }, request.query_ids);
            try std.testing.expectEqual(@as(usize, 1), request.capacity);
            var storage = try scoring.Storage.init(allocator);
            errdefer storage.deinit();
            return .{ .storage = storage, .logits = try storage.alloc().dupe(f32, &.{ 4, 4 }), .valid = try storage.alloc().dupe(bool, &.{ true, true }) };
        }
        fn scorer(self: *@This()) scoring.Scorer {
            return .{ .context = self, .check_fn = check, .classify_fn = classify, .relations_fn = relations, .record_fn = record, .explicit_fn = explicit };
        }
    };
    for ([_]usize{ 2, 3 }) |limit| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var stub = Stub{};
        var work = Work{ .scorer = stub.scorer(), .allocator = arena.allocator(), .scratch_allocator = a, .config = &config, .prepared = &prepared, .schemas = &schemas, .scores = null, .options = .{ .max_output_values = limit }, .output_values = 1 };
        var values = [_]Value{.{ .text = "İ", .confidence = 0.9, .source = null, .token_span = .{ .start = 0, .end = 1 } }};
        var groups = [_]EntityGroup{.{ .name = "person", .dtype = .list, .values = &values }};
        if (limit == 2) {
            try std.testing.expectError(error.ExtractionOutputLimitExceeded, work.attributes(0, &groups));
        } else {
            try work.attributes(0, &groups);
            try std.testing.expectEqual(@as(usize, 3), work.output_values);
            try std.testing.expectEqual(@as(usize, 2), groups[0].values[0].attributes[0].labels.len);
        }
    }
}
