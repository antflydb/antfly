// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Admitted serial windows followed by document-global task decisions. Model
//! tensors die after each window; only bounded scalar evidence survives.
const std = @import("std");
const Allocator = std.mem.Allocator;
const wire = @import("extraction_v2.zig");
const model = @import("../models/gliner_boundary.zig");
const artifact = @import("../models/gliner_boundary_bundle.zig");
const processor = @import("../pipelines/gliner_boundary_processor.zig");
const pipeline = @import("../pipelines/gliner_boundary_pipeline.zig");
const document_mod = @import("../pipelines/gliner_boundary_long_document.zig");
const long_relations = @import("../pipelines/gliner_boundary_long_relations.zig");
const boundary = @import("../pipelines/gliner_boundary_decode.zig");
const engine = @import("../architectures/gliner_boundary_engine.zig");
const head = @import("../architectures/gliner_boundary_head.zig");
const device_request = @import("../architectures/gliner_boundary_request_device.zig");
const compute = @import("../ops/ops.zig");
const Tokenizer = @import("inference_tokenizer").Tokenizer;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const BoundedAllocator = @import("../runtime/bounded_allocator.zig").BoundedAllocator;
const observation = @import("extraction_observer.zig");
const qualification = @import("gliner_boundary_qualification.zig");

pub const Limits = struct {
    plan: document_mod.PlanOptions = .{},
    merge: document_mod.MergeOptions = .{},
    max_total_encoded_tokens: usize = 131072,
    max_total_attention_work: u64 = 8 * 1024 * 1024 * 1024,
    max_retained_evidence_bytes: usize = 128 * 1024 * 1024,
    max_request_windows: usize = 256,
};
pub const Options = struct {
    identity: artifact.Identity,
    processor: processor.Options = .{},
    engine: engine.Options = .{},
    device: device_request.Limits = .{},
    pipeline: pipeline.Options = .{},
    limits: Limits = .{},
    control: ?Control = null,
    observer: ?observation.Observer = null,
};
pub const Result = struct {
    arena: std.heap.ArenaAllocator,
    sample: pipeline.Sample,
    prompt_tokens: usize,
    attention_work_items: u64,
    window_count: usize,
    descriptor: document_mod.Descriptor,
    inference_fingerprint: [32]u8,
    peak_evidence_bytes: usize,
    pub fn deinit(self: *Result) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

fn check(options: Options) !void {
    if (options.control) |control| try control.check();
}

const EvidenceAllocationFailure = ?BoundedAllocator.AllocationFailure;
fn observeEvidenceAllocation(raw: ?*anyopaque, failure: BoundedAllocator.AllocationFailure) void {
    const terminal: *EvidenceAllocationFailure = @ptrCast(@alignCast(raw.?));
    terminal.* = failure;
}
fn evidenceAllocationError(terminal: EvidenceAllocationFailure, err: anyerror) anyerror {
    // A speculative resize/remap miss may recover. Only the terminal alloc
    // determines whether the configured ceiling or the backing owner failed.
    if (err != error.OutOfMemory) return err;
    const failure = terminal orelse return err;
    return if (failure.kind == .declared_limit) error.MemoryBudgetExceeded else err;
}

/// Hash semantic knobs explicitly: allocator addresses and cancellation
/// callbacks are not inference identity. Resource ceilings are included when
/// they affect retained candidates or search. Schema identity lives in Plan.
fn hashOptions(hash: *std.crypto.hash.sha2.Sha256, value: anytype) void {
    const T = @TypeOf(value);
    switch (@typeInfo(T)) {
        .@"struct" => |info| inline for (info.fields) |field| {
            // These carry ownership/cancellation/validator execution, not
            // semantics. Compiled validator expressions live in schema IDs.
            if (comptime !std.mem.eql(u8, field.name, "control") and !std.mem.eql(u8, field.name, "check_context") and !std.mem.eql(u8, field.name, "check_fn") and
                !std.mem.eql(u8, field.name, "regex_context") and !std.mem.eql(u8, field.name, "validate_value_fn"))
            {
                hash.update(field.name);
                hash.update("\x00");
                hashOptions(hash, @field(value, field.name));
            }
        },
        .optional => {
            hash.update(if (value == null) "0" else "1");
            if (value) |present| hashOptions(hash, present);
        },
        .@"enum" => {
            hash.update(@tagName(value));
            hash.update("\x00");
        },
        .bool => hash.update(if (value) "1" else "0"),
        .int => {
            var bytes: [8]u8 = undefined;
            std.mem.writeInt(u64, &bytes, @intCast(value), .little);
            hash.update(&bytes);
        },
        .float => |info| {
            const Bits = std.meta.Int(.unsigned, info.bits);
            var bytes: [info.bits / 8]u8 = undefined;
            std.mem.writeInt(Bits, &bytes, @bitCast(value), .little);
            hash.update(&bytes);
        },
        else => @compileError("Add an explicit semantic profile encoding for " ++ @typeName(T)),
    }
}

fn profile(a: Allocator, config: *const model.Config, item: *const wire.Item, options: Options, backend: []const u8) ![32]u8 {
    if (config.backbone != options.identity.backbone) return error.GlinerBoundaryArtifactMismatch;
    const bytes = try std.json.Stringify.valueAlloc(a, .{
        .version = document_mod.semantics_version,
        .request = item.options,
        .head = config.head,
        .word_splitter = options.processor.word_splitter,
        .head_limits = options.pipeline.head_limits,
        .task_math_limits = options.pipeline.task_limits.math,
        .max_output_values = options.pipeline.max_output_values,
        .classification = .{
            .algorithm = options.pipeline.classification_solver.algorithm,
            .beam_width = options.pipeline.classification_solver.beam_width,
            .exact_nodes = options.pipeline.classification_solver.exact_node_budget,
            .beam_nodes = options.pipeline.classification_solver.beam_node_budget,
            .max_local_assignments = options.pipeline.classification_solver.max_local_assignments,
        },
        .joint = .{
            .max_nodes = options.pipeline.joint_solver.max_nodes,
            .max_edges = options.pipeline.joint_solver.max_edges,
            .max_graph_edges = options.pipeline.joint_solver.max_graph_edges,
        },
    }, .{});
    defer a.free(bytes);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly-gliner-boundary-window-inference/v1\x00");
    hash.update(&options.identity.fingerprint());
    hash.update(backend);
    hash.update("\x00");
    hashOptions(&hash, options.pipeline);
    hashOptions(&hash, options.processor);
    hashOptions(&hash, options.engine);
    hashOptions(&hash, options.device);
    hash.update(bytes);
    return hash.finalResult();
}

const Window = struct {
    result: pipeline.WindowResult,
    logits: []const []const f64,
    mentions: []const document_mod.MentionCandidate,
    mention_values: []const pipeline.Value,
    records: []const document_mod.RecordCandidate,
};

fn collectWindow(a: Allocator, result: pipeline.WindowResult, prepared: *const processor.PreparedBatch, item: *const wire.Item) !Window {
    if (result.outputs.samples.len != 1 or result.joint_candidates.len != 1) return error.InvalidExtractionOutput;
    const sample = result.outputs.samples[0];
    const classes = item.compiled.schema.classifications;
    const rows = try a.alloc([]const f64, classes.len);
    for (classes, rows, 0..) |classification, *row, task_index| {
        const values = try a.alloc(f64, classification.task.labels.len);
        @memset(values, std.math.nan(f64));
        const scores = result.classification_scores orelse return error.InvalidBoundaryScorerOutput;
        for (prepared.samples[0].classification_labels, 0..) |label, index| {
            if (label.schema_index != task_index) continue;
            if (label.label_index >= values.len or index >= scores.logits.len or !std.math.isNan(values[label.label_index])) return error.InvalidBoundaryPipelineRouting;
            values[label.label_index] = scores.logits[index];
        }
        for (values) |value| if (!std.math.isFinite(value)) return error.InvalidBoundaryPipelineRouting;
        row.* = values;
    }
    var mentions = std.ArrayListUnmanaged(document_mod.MentionCandidate).empty;
    var values = std.ArrayListUnmanaged(pipeline.Value).empty;
    if (item.compiled.schema.joint_ie == null) {
        if (sample.entities.len != item.compiled.schema.entities.len) return error.InvalidExtractionOutput;
        for (sample.entities, item.compiled.schema.entities, 0..) |group, spec, entity_type| {
            if (!std.mem.eql(u8, group.name, spec.name)) return error.InvalidBoundaryPipelineRouting;
            for (group.values) |value| {
                try mentions.append(a, .{ .entity_type = entity_type, .source = value.source orelse return error.InvalidLongDocumentMention, .probability = value.confidence });
                try values.append(a, value);
            }
        }
    }
    var records = std.ArrayListUnmanaged(document_mod.RecordCandidate).empty;
    for (sample.structures) |group| {
        var structure_index: ?usize = null;
        for (item.compiled.schema.structures, 0..) |spec, index| if (std.mem.eql(u8, spec.name, group.name)) {
            if (structure_index != null) return error.InvalidBoundaryPipelineRouting;
            structure_index = index;
        };
        for (group.instances, 0..) |record, record_index| try records.append(a, .{ .structure_index = structure_index orelse return error.InvalidBoundaryPipelineRouting, .record_index = record_index, .record = record });
    }
    return .{ .result = result, .logits = rows, .mentions = try mentions.toOwnedSlice(a), .mention_values = try values.toOwnedSlice(a), .records = try records.toOwnedSlice(a) };
}

const Copier = struct {
    a: Allocator,
    document: document_mod.Plan,
    options: pipeline.Options,
    values: usize = 0,
    bytes: usize = 0,
    fn text(self: *Copier, value: []const u8) ![]const u8 {
        if (value.len > self.options.max_output_string_bytes -| self.bytes) return error.ExtractionOutputLimitExceeded;
        self.bytes += value.len;
        return self.a.dupe(u8, value);
    }
    fn charge(self: *Copier) !void {
        if (self.values >= self.options.max_output_values) return error.ExtractionOutputLimitExceeded;
        self.values += 1;
        if (self.options.control) |control| try control.check();
    }
    fn labels(self: *Copier, input: []const pipeline.Label) ![]const pipeline.Label {
        const out = try self.a.alloc(pipeline.Label, input.len);
        for (input, out) |label, *value| {
            try self.charge();
            value.* = .{ .label = try self.text(label.label), .confidence = label.confidence };
        }
        return out;
    }
    fn copyValue(self: *Copier, input: pipeline.Value, window: ?usize) !pipeline.Value {
        try self.charge();
        var out = input;
        out.text = try self.text(input.text);
        out.token_span = null;
        if (window) |index| if (input.source) |source| {
            out.source = try self.document.rebaseSource(index, source, self.options.offset_unit);
        };
        const attributes = try self.a.alloc(pipeline.Attribute, input.attributes.len);
        for (input.attributes, attributes) |attr, *copied| copied.* = .{ .name = try self.text(attr.name), .multi_label = attr.multi_label, .labels = try self.labels(attr.labels) };
        out.attributes = attributes;
        return out;
    }
    fn copyRecord(self: *Copier, original: pipeline.Record, window: ?usize) !pipeline.Record {
        const fields = try self.a.alloc(pipeline.Field, original.fields.len);
        for (original.fields, fields) |field, *out| {
            const values = try self.a.alloc(pipeline.Value, field.values.len);
            for (field.values, values) |value, *copied| copied.* = try self.copyValue(value, window);
            out.* = .{ .name = try self.text(field.name), .dtype = field.dtype, .values = values };
        }
        const anchor = if (original.anchor) |source| if (window) |index| try self.document.rebaseSource(index, source, self.options.offset_unit) else source else null;
        // Occurrence metadata belongs to pre-merge evidence only.
        return .{ .confidence = original.confidence, .anchor = anchor, .fields = fields };
    }
};

fn findRecord(window: Window, structure: usize, record: usize) !pipeline.Record {
    for (window.records) |candidate| if (candidate.structure_index == structure and candidate.record_index == record) return candidate.record;
    return error.InvalidBoundaryPipelineRouting;
}

fn mergeAll(allocator: Allocator, result_allocator: Allocator, document: document_mod.Plan, windows: []const Window, item: *const wire.Item, config: *const model.Config, options: Options) !pipeline.Sample {
    var scratch = std.heap.ArenaAllocator.init(allocator);
    defer scratch.deinit();
    const a = scratch.allocator();
    var copier = Copier{ .a = result_allocator, .document = document, .options = options.pipeline };
    var merge_options = options.limits.merge;
    merge_options.control = options.control;
    var output = pipeline.Sample{};
    if (item.compiled.schema.joint_ie) |joint_schema| {
        const inputs = try a.alloc(document_mod.WindowJointCandidates, windows.len);
        for (windows, inputs, 0..) |window, *input, index| {
            const candidates = window.result.joint_candidates[0] orelse return error.InvalidBoundaryPipelineRouting;
            input.* = .{ .identity = try document.identity(index), .nodes = candidates.nodes, .edges = candidates.edges };
        }
        var global = try document_mod.solveJoint(allocator, document, &item.compiled, inputs, .{ .merge = merge_options, .solver = options.pipeline.joint_solver, .best_effort = options.pipeline.best_effort });
        defer global.deinit();
        const nodes = global.result.nodes;
        const values = try a.alloc(pipeline.Value, nodes.len);
        for (nodes, values) |node, *value| {
            // mergeJointCandidates uses global UTF-8 coordinates.
            const source = boundary.Offsets{ .start = node.start, .end = node.end };
            const converted = try document.offsets.convert(source, options.pipeline.offset_unit);
            value.* = .{ .text = document.text[source.start..source.end], .confidence = @floatCast(node.probability), .source = .{ .start = converted.start, .end = converted.end, .unit = options.pipeline.offset_unit, .byte_start = source.start, .byte_end = source.end }, .token_span = null };
        }
        const groups = try result_allocator.alloc(pipeline.EntityGroup, joint_schema.entities.len);
        for (joint_schema.entities, groups, 0..) |spec, *group, kind| {
            var selected = std.ArrayListUnmanaged(pipeline.Value).empty;
            for (nodes, values) |node, value| if (node.entity_type == kind) try selected.append(result_allocator, try copier.copyValue(value, null));
            const SourceOrder = struct {
                fn less(_: void, left: pipeline.Value, right: pipeline.Value) bool {
                    return if (left.source.?.byte_start != right.source.?.byte_start) left.source.?.byte_start < right.source.?.byte_start else left.source.?.byte_end < right.source.?.byte_end;
                }
            };
            std.mem.sort(pipeline.Value, selected.items, {}, SourceOrder.less);
            group.* = .{ .name = try copier.text(spec.name), .dtype = .list, .values = try selected.toOwnedSlice(result_allocator) };
        }
        output.entities = groups;
        const edges = try result_allocator.alloc(pipeline.Relation, global.result.edges.len);
        for (global.result.edges, edges) |edge, *out| out.* = .{ .name = try copier.text(joint_schema.relations[edge.relation_type].name), .head = try copier.copyValue(values[edge.head], null), .tail = try copier.copyValue(values[edge.tail], null), .confidence = @floatCast(edge.probability), .derived = edge.derived, .head_entity_type = nodes[edge.head].entity_type, .tail_entity_type = nodes[edge.tail].entity_type };
        output.relations = edges;
        output.joint_solver = .{ .status = if (global.result.status == .optimal and !global.result.exhausted) .optimal else .feasible, .visited_nodes = global.result.visited_nodes, .utility = global.result.utility, .exhausted = global.result.exhausted };
        return output;
    }
    const mention_windows = try a.alloc(document_mod.WindowMentions, windows.len);
    const class_windows = try a.alloc(document_mod.WindowLogits, windows.len);
    const relation_windows = try a.alloc(long_relations.Window, windows.len);
    const record_windows = try a.alloc(document_mod.WindowRecords, windows.len);
    for (windows, 0..) |window, index| {
        const identity = try document.identity(index);
        mention_windows[index] = .{ .identity = identity, .pre_overlap_candidates = window.mentions };
        class_windows[index] = .{ .identity = identity, .logits = window.logits };
        relation_windows[index] = .{ .identity = identity, .relations = window.result.outputs.samples[0].relations };
        record_windows[index] = .{ .identity = identity, .records = window.records };
    }
    var mentions = try document_mod.mergeMentions(allocator, document, &item.compiled, mention_windows, options.pipeline.overlap, options.pipeline.offset_unit, merge_options);
    defer mentions.deinit();
    const groups = try result_allocator.alloc(pipeline.EntityGroup, item.compiled.schema.entities.len);
    for (item.compiled.schema.entities, groups, 0..) |spec, *group, kind| {
        var selected = std.ArrayListUnmanaged(pipeline.Value).empty;
        var best: ?document_mod.SelectedMention = null;
        for (mentions.selected) |mention| if (mention.entity_type == kind) {
            if (spec.dtype == .str) {
                if (best == null or mention.probability > best.?.probability) best = mention;
                continue;
            }
            const original = windows[mention.origin.window_index].mention_values[mention.origin.candidate_index];
            try selected.append(result_allocator, try copier.copyValue(original, mention.origin.window_index));
        };
        if (best) |mention| {
            const original = windows[mention.origin.window_index].mention_values[mention.origin.candidate_index];
            try selected.append(result_allocator, try copier.copyValue(original, mention.origin.window_index));
        }
        const Order = struct {
            fn less(_: void, left: pipeline.Value, right: pipeline.Value) bool {
                if (left.confidence != right.confidence) return left.confidence > right.confidence;
                if (left.source.?.byte_start != right.source.?.byte_start) return left.source.?.byte_start < right.source.?.byte_start;
                return left.source.?.byte_end < right.source.?.byte_end;
            }
        };
        std.mem.sort(pipeline.Value, selected.items, {}, Order.less);
        group.* = .{ .name = try copier.text(spec.name), .dtype = spec.dtype, .values = try selected.toOwnedSlice(result_allocator) };
    }
    output.entities = groups;
    if (item.compiled.schema.classifications.len > 0) {
        var raw = try document_mod.aggregateClassificationLogits(allocator, document, &item.compiled, class_windows, merge_options);
        defer raw.deinit();
        var classes = try pipeline.presentClassifications(allocator, &item.compiled, raw.logits, config.head.classification_temperature, options.pipeline);
        defer classes.deinit();
        const presented = try result_allocator.alloc(pipeline.Classification, classes.classifications.len);
        for (classes.classifications, presented) |classification, *out| out.* = .{ .name = try copier.text(classification.name), .multi_label = classification.multi_label, .labels = try copier.labels(classification.labels) };
        output.classifications = presented;
        output.classification_solver = classes.diagnostics;
    }
    if (item.compiled.schema.relations.len > 0) {
        var relation_merge_options = merge_options;
        relation_merge_options.max_work = @min(relation_merge_options.max_work, options.pipeline.relation_dedup_limits.max_comparisons);
        relation_merge_options.max_text_bytes = @min(relation_merge_options.max_text_bytes, options.pipeline.relation_dedup_limits.max_text_bytes);
        var relations = try long_relations.merge(allocator, document, &item.compiled, relation_windows, .{ .merge = relation_merge_options, .output_unit = options.pipeline.offset_unit, .max_edges_per_type = options.pipeline.relation_dedup_limits.max_edges });
        defer relations.deinit();
        const edges = try result_allocator.alloc(pipeline.Relation, relations.relations.len);
        for (relations.relations, edges) |edge, *out| {
            out.* = edge;
            out.name = try copier.text(edge.name);
            out.head = try copier.copyValue(edge.head, null);
            out.tail = try copier.copyValue(edge.tail, null);
        }
        output.relations = edges;
    }
    if (item.compiled.schema.structures.len > 0) {
        var legacy = try document_mod.mergeLegacyStructures(allocator, document, &item.compiled, record_windows, merge_options, options.pipeline);
        defer legacy.deinit();
        var records = try document_mod.mergeRecords(allocator, document, &item.compiled, record_windows, .{
            .merge = merge_options,
            .solver = .{
                .algorithm = switch (options.pipeline.classification_solver.algorithm) {
                    .auto => .auto,
                    .exact => .exact,
                    .beam => .beam,
                },
                .beam_width = options.pipeline.classification_solver.beam_width,
                .exact_node_budget = options.pipeline.classification_solver.exact_node_budget,
                .beam_node_budget = options.pipeline.classification_solver.beam_node_budget,
                .max_candidates = @min(4096, merge_options.max_input_candidates),
                .max_work = merge_options.max_work,
                .control = options.control,
            },
            .best_effort = options.pipeline.best_effort,
            .output_unit = options.pipeline.offset_unit,
            .regex_context = options.pipeline.regex_context,
            .validate_value_fn = options.pipeline.validate_value_fn,
        });
        defer records.deinit();
        var structures = std.ArrayListUnmanaged(pipeline.Structure).empty;
        var has_explicit = false;
        for (item.compiled.schema.structures, 0..) |spec, kind| {
            var selected = std.ArrayListUnmanaged(pipeline.Record).empty;
            if (spec.mode == null) {
                for (legacy.structures) |group| if (std.mem.eql(u8, group.name, spec.name)) {
                    for (group.instances) |record| try selected.append(result_allocator, try copier.copyRecord(record, null));
                };
                if (selected.items.len > 0) try structures.append(result_allocator, .{ .name = try copier.text(spec.name), .instances = try selected.toOwnedSlice(result_allocator) });
                continue;
            }
            has_explicit = true;
            for (records.selected) |record| if (record.structure_index == kind) {
                const original = try findRecord(windows[record.origin.window_index], kind, record.origin.record_index);
                try selected.append(result_allocator, try copier.copyRecord(original, record.origin.window_index));
            };
            if (selected.items.len > 0) try structures.append(result_allocator, .{ .name = try copier.text(spec.name), .instances = try selected.toOwnedSlice(result_allocator) });
        }
        output.structures = try structures.toOwnedSlice(result_allocator);
        output.record_solver = if (has_explicit) records.diagnostics else null;
    }
    return output;
}

pub fn executeNative(cb: *const compute.ComputeBackend, allocator: Allocator, config: *const model.Config, tokenizer: Tokenizer, item: *const wire.Item, supplied: Options) !Result {
    if (cb.kind() != .native) return error.UnsupportedGlinerBoundaryBackend;
    return execute(cb, allocator, config, tokenizer, item, supplied);
}

pub fn executeDevice(cb: *const compute.ComputeBackend, allocator: Allocator, config: *const model.Config, tokenizer: Tokenizer, item: *const wire.Item, supplied: Options) !Result {
    if (cb.kind() != .metal) return error.UnsupportedGlinerBoundaryBackend;
    if (cb.decoderRuntimeHasActiveFrame()) return error.GlinerBoundaryExternalFrame;
    return execute(cb, allocator, config, tokenizer, item, supplied);
}

pub const GeometryUsage = struct { prompt_tokens: usize, attention_work_items: u64, window_count: usize };
const PreparedDocument = struct {
    options: Options,
    document: document_mod.Plan,
    process_options: processor.Options,
    engine_options: engine.Options,
    device_options: device_request.Options,
};

fn prepareDocument(cb: *const compute.ComputeBackend, allocator: Allocator, config: *const model.Config, item: *const wire.Item, supplied: Options) !PreparedDocument {
    var options = supplied;
    try check(options);
    options.processor = item.options.preprocessing(options.processor);
    try item.options.validateNativeLimits(supplied.pipeline);
    options.pipeline = item.options.native(supplied.pipeline);
    options.pipeline.control = options.control;
    try pipeline.validateOptions(options.pipeline);
    try config.head.validate();
    if (options.pipeline.validate_value_fn == null) {
        for (item.compiled.schema.entities) |entity| if (entity.validators.len != 0) return error.MissingExtractionValidator;
        for (item.compiled.schema.structures) |structure| for (structure.fields) |field| if (field.validators.len != 0) return error.MissingExtractionValidator;
    }
    if (item.options.long_document.mode != .window or options.limits.max_total_encoded_tokens == 0 or options.limits.max_total_attention_work == 0 or
        options.limits.max_retained_evidence_bytes == 0 or options.limits.max_retained_evidence_bytes > 1024 * 1024 * 1024) return error.InvalidLongDocumentLimits;
    var planning = try qualification.longPlanning(config, item, options.limits.plan, options.control);
    planning.inference_fingerprint = try profile(allocator, config, item, options, @tagName(cb.kind()));
    observation.emit(options.observer, .{ .phase = .windowing });
    var document = try document_mod.plan(allocator, item.text, item.compiled.fingerprint, planning);
    errdefer document.deinit();
    observation.emit(options.observer, .{ .document_planned = document.windows.len });
    const process_options = qualification.windowProcessor(item, options.processor, planning, options.engine.limits.max_sequence_tokens, options.control);
    var engine_options = options.engine;
    engine_options.control = options.control;
    var device_options = device_request.Options{ .precision = options.identity.precision, .limits = options.device, .pipeline = options.pipeline };
    // The service encoder policy is shared by both execution backends.
    device_options.limits.encoder = options.engine.limits;
    device_options.pipeline.max_output_values = options.limits.merge.max_input_candidates;
    return .{ .options = options, .document = document, .process_options = process_options, .engine_options = engine_options, .device_options = device_options };
}

fn admitWindows(cb: *const compute.ComputeBackend, allocator: Allocator, config: *const model.Config, tokenizer: Tokenizer, item: *const wire.Item, planned: *const PreparedDocument, receiver: anytype) !GeometryUsage {
    const options = planned.options;
    const document = planned.document;
    const process_options = planned.process_options;
    const engine_options = planned.engine_options;
    const device_options = planned.device_options;
    // Exact tokenizer/schema and cumulative attention admission precede any
    // model execution. Recompute each small prepared window when it is used.
    var tokens: usize = 0;
    var attention: u64 = 0;
    observation.emit(options.observer, .{ .phase = .tokenizing });
    for (document.windows) |window| {
        try check(options);
        var prepared = try processor.prepare(allocator, tokenizer, &.{.{ .text = try document.windowText(window.index), .schema = &item.compiled }}, process_options);
        defer prepared.deinit();
        try receiver.observe(item.text.len, document.words.len, document.windows.len, &prepared);
        const attention_per_layer = switch (cb.kind()) {
            .native => (try engine.plan(config, &prepared, engine_options)).attention_work_items,
            .metal => (try device_request.plan(config, &prepared, &.{&item.compiled}, device_options)).encoder.native.attention_work_items,
            else => return error.UnsupportedGlinerBoundaryBackend,
        };
        // Count attention score elements across every encoder layer. This is
        // an admission work unit, not a FLOP or latency estimate.
        const window_attention = std.math.mul(u64, attention_per_layer, config.encoder.num_hidden_layers) catch return error.LongDocumentWorkLimitExceeded;
        tokens = std.math.add(usize, tokens, prepared.input_ids.len) catch return error.LongDocumentWorkLimitExceeded;
        attention = std.math.add(u64, attention, window_attention) catch return error.LongDocumentWorkLimitExceeded;
        if (tokens > options.limits.max_total_encoded_tokens or attention > options.limits.max_total_attention_work) return error.LongDocumentWorkLimitExceeded;
    }
    return .{ .prompt_tokens = tokens, .attention_work_items = attention, .window_count = document.windows.len };
}

/// Quiet bounded tokenization/planning only. The caller retains one policy
/// candidate set across the complete request; this function cannot run a model.
/// A compile-time scalar receiver lets model-free tests inspect this same path;
/// learned executeQualified still accepts only the concrete closed Gate.
pub fn qualifyGeometry(cb: *const compute.ComputeBackend, allocator: Allocator, config: *const model.Config, tokenizer: Tokenizer, item: *const wire.Item, supplied: Options, receiver: anytype) !GeometryUsage {
    var quiet = supplied;
    quiet.observer = null;
    var planned = try prepareDocument(cb, allocator, config, item, quiet);
    defer planned.document.deinit();
    const used = try admitWindows(cb, allocator, config, tokenizer, item, &planned, receiver);
    try check(quiet);
    return used;
}

pub fn executeQualified(cb: *const compute.ComputeBackend, allocator: Allocator, config: *const model.Config, tokenizer: Tokenizer, item: *const wire.Item, supplied: Options, gate: *qualification.Gate) !Result {
    if (cb.kind() != .native and cb.kind() != .metal) return error.UnsupportedGlinerBoundaryBackend;
    if (cb.kind() == .metal and cb.decoderRuntimeHasActiveFrame()) return error.GlinerBoundaryExternalFrame;
    return executeChecked(cb, allocator, config, tokenizer, item, supplied, gate);
}

fn execute(cb: *const compute.ComputeBackend, allocator: Allocator, config: *const model.Config, tokenizer: Tokenizer, item: *const wire.Item, supplied: Options) !Result {
    return executeChecked(cb, allocator, config, tokenizer, item, supplied, null);
}

const WindowGate = struct {
    value: ?*qualification.Gate,
    fn observe(self: *const WindowGate, bytes: usize, words: usize, count: usize, prepared: *const processor.PreparedBatch) !void {
        if (self.value) |gate| try gate.observe(bytes, words, count, prepared);
    }
};

fn executeChecked(cb: *const compute.ComputeBackend, allocator: Allocator, config: *const model.Config, tokenizer: Tokenizer, item: *const wire.Item, supplied: Options, gate: ?*qualification.Gate) !Result {
    var planned = try prepareDocument(cb, allocator, config, item, supplied);
    defer planned.document.deinit();
    const options = planned.options;
    const document = planned.document;
    const process_options = planned.process_options;
    const engine_options = planned.engine_options;
    const device_options = planned.device_options;
    const used = try admitWindows(cb, allocator, config, tokenizer, item, &planned, &WindowGate{ .value = gate });
    const tokens = used.prompt_tokens;
    const attention = used.attention_work_items;
    // Both addresses remain valid until all retained window/metadata owners
    // have deinitialized. No callback or failure pointer escapes this call.
    var terminal: EvidenceAllocationFailure = null;
    var budget = BoundedAllocator{ .backing = allocator, .limit = options.limits.max_retained_evidence_bytes, .failure_context = &terminal, .allocation_failed = observeEvidenceAllocation };
    defer std.debug.assert(budget.live == 0);
    var metadata = std.heap.ArenaAllocator.init(budget.allocator());
    defer metadata.deinit();
    const windows = metadata.allocator().alloc(Window, document.windows.len) catch |err| return evidenceAllocationError(terminal, err);
    var completed: usize = 0;
    defer for (windows[0..completed]) |*window| window.result.deinit();
    for (document.windows, windows) |window, *out| {
        try check(options);
        observation.emit(options.observer, .{ .phase = .tokenizing });
        var prepared = try processor.prepare(allocator, tokenizer, &.{.{ .text = try document.windowText(window.index), .schema = &item.compiled }}, process_options);
        defer prepared.deinit();
        if (gate) |active| try active.observe(item.text.len, document.words.len, document.windows.len, &prepared);
        var pipeline_options = options.pipeline;
        pipeline_options.control = options.control;
        // Retained candidates and final presented outputs have different
        // budgets: a scalar output may need many alternatives for merging.
        pipeline_options.max_output_values = options.limits.merge.max_input_candidates;
        observation.emit(options.observer, .{ .phase = .execution });
        var result = switch (cb.kind()) {
            .native => native: {
                var encoded = try engine.encodeNative(cb, allocator, config, &prepared, engine_options);
                defer encoded.deinit();
                var headed: ?head.Result = if (prepared.query_width > 0) try head.forwardNative(cb, allocator, config, encoded.asHeadInput(options.control), options.pipeline.head_limits) else null;
                defer if (headed) |*value| value.deinit();
                var scorer = pipeline.scoring.NativeContext{ .cb = cb, .config = config, .prepared = &prepared, .core = .{ .text_states = encoded.text_states, .query_states = encoded.query_states, .classification_states = encoded.classification_states, .text_lengths = encoded.text_lengths }, .scores = if (headed) |*value| value else null };
                break :native pipeline.runScoredWindows(budget.allocator(), config, &prepared, &.{&item.compiled}, if (headed) |*value| pipeline.scoring.CandidateScoreView.fromNative(value) else null, scorer.scorer(), pipeline_options, &.{}) catch |err| return evidenceAllocationError(terminal, err);
            },
            .metal => (device_request.runWindowsWithOutputAllocator(cb, allocator, budget.allocator(), config, &prepared, &.{&item.compiled}, device_options) catch |err| return evidenceAllocationError(terminal, err)).outputs,
            else => return error.UnsupportedGlinerBoundaryBackend,
        };
        errdefer result.deinit();
        out.* = collectWindow(metadata.allocator(), result, &prepared, item) catch |err| return evidenceAllocationError(terminal, err);
        completed += 1;
        observation.emit(options.observer, .window_completed);
    }
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    observation.emit(options.observer, .{ .phase = .merging });
    var sample = try mergeAll(allocator, arena.allocator(), document, windows, item, config, options);
    sample.long_document = .{ .window_count = windows.len, .other_record_identity = if (document.descriptor.other_record_identity == .occurrence) .occurrence else .semantic };
    try check(options);
    return .{ .arena = arena, .sample = sample, .prompt_tokens = tokens, .attention_work_items = attention, .window_count = windows.len, .descriptor = document.descriptor, .inference_fingerprint = document.inference_fingerprint.?, .peak_evidence_bytes = budget.peak };
}

test "gliner boundary long executor profile includes effective caps and excludes callback addresses" {
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    var left = pipeline.Options{};
    var first = std.crypto.hash.sha2.Sha256.init(.{});
    hashOptions(&first, left);
    left.control = .{ .check_fn = Cancel.check };
    var second = std.crypto.hash.sha2.Sha256.init(.{});
    hashOptions(&second, left);
    try std.testing.expectEqualSlices(u8, &first.finalResult(), &second.finalResult());
    left.record_limits.max_instances -= 1;
    var third = std.crypto.hash.sha2.Sha256.init(.{});
    hashOptions(&third, left);
    try std.testing.expect(!std.mem.eql(u8, &second.finalResult(), &third.finalResult()));
}

test "gliner boundary long executor declared evidence cap preserves backing OOM and releases partial allocations" {
    var terminal: EvidenceAllocationFailure = null;
    var budget = BoundedAllocator{ .backing = std.testing.allocator, .limit = 64, .failure_context = &terminal, .allocation_failed = observeEvidenceAllocation };
    const a = budget.allocator();
    const first = try a.alloc(u8, 32);
    const denied: anyerror = failed: {
        const unexpected = a.alloc(u8, 33) catch |err| break :failed evidenceAllocationError(terminal, err);
        a.free(unexpected);
        return error.TestUnexpectedResult;
    };
    a.free(first);
    try std.testing.expectEqual(error.MemoryBudgetExceeded, denied);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    const recovered = try a.alloc(u8, 64);
    a.free(recovered);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    try std.testing.expectEqual(error.Cancelled, evidenceAllocationError(terminal, error.Cancelled));

    var fail = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var backing_terminal: EvidenceAllocationFailure = null;
    var backing = BoundedAllocator{ .backing = fail.allocator(), .limit = 64, .failure_context = &backing_terminal, .allocation_failed = observeEvidenceAllocation };
    const failure: anyerror = failed: {
        const unexpected = backing.allocator().alloc(u8, 32) catch |err| break :failed evidenceAllocationError(backing_terminal, err);
        backing.allocator().free(unexpected);
        return error.TestUnexpectedResult;
    };
    try std.testing.expectEqual(error.OutOfMemory, failure);
    try std.testing.expect(!backing.denied);
    try std.testing.expectEqual(@as(usize, 0), backing.live);
}

test "gliner boundary long executor word splitter binds inference and window identities" {
    const a = std.testing.allocator;
    var request = try wire.parseJson(a,
        \\{"schema_version":2,"model":"m","schema":{"entities":["person"]},"inputs":[{"content":"東京 京都"},{"content":"東京 京都","options":{"word_splitter":"whitespace"}},{"content":"東京 京都","options":{"word_splitter":"char"}}]}
    , .{});
    defer request.deinit();
    const config = model.Config{ .version = 3, .architecture_version = 1, .max_len = 4096, .backbone = .small, .head = .{}, .encoder = std.mem.zeroes(model.EncoderConfig) };
    const digest = artifact.Digest.of("identity fixture");
    const identity = artifact.Identity{ .backbone = .small, .precision = .fp32, .weight = digest, .sidecars = [_]artifact.Digest{digest} ** 4 };
    var fingerprints: [3][32]u8 = undefined;
    var plan_fingerprints: [3][32]u8 = undefined;
    for (request.items, 0..) |*item, index| {
        const process = item.options.preprocessing(.{});
        fingerprints[index] = try profile(a, &config, item, .{ .identity = identity, .processor = process }, "native");
        var plan = try document_mod.plan(a, item.text, item.compiled.fingerprint, .{
            .mode = .windowed,
            .word_splitter = process.word_splitter,
            .max_window_body_words = 3,
            .overlap_words = 0,
            .inference_fingerprint = fingerprints[index],
        });
        defer plan.deinit();
        plan_fingerprints[index] = plan.fingerprint;
    }
    try std.testing.expectEqualSlices(u8, &fingerprints[0], &fingerprints[1]);
    try std.testing.expectEqualSlices(u8, &plan_fingerprints[0], &plan_fingerprints[1]);
    try std.testing.expect(!std.mem.eql(u8, &fingerprints[0], &fingerprints[2]));
    try std.testing.expect(!std.mem.eql(u8, &plan_fingerprints[0], &plan_fingerprints[2]));
}

test "gliner boundary long executor pinned small one-window task parity and global admission" {
    const directory = @import("antfly_platform").env.getenv("ANTFLY_GLINER25_SMALL_MODEL_DIR") orelse return error.SkipZigTest;
    try testPinnedSmallWindows(directory, false);
}

test "gliner boundary long executor pinned small Metal one-window task parity and global admission" {
    if (!@import("build_options").enable_metal) return error.SkipZigTest;
    const directory = @import("antfly_platform").env.getenv("ANTFLY_GLINER25_SMALL_MODEL_DIR") orelse return error.SkipZigTest;
    try testPinnedSmallWindows(directory, true);
}

fn testPinnedSmallWindows(directory: []const u8, metal: bool) !void {
    const a = std.testing.allocator;
    const fixtures = @import("../architectures/gliner_boundary_parity_test.zig");
    const bytes = try fixtures.fixtureBytes(a, "pipeline_cases.json");
    defer a.free(bytes);
    var reference = try std.json.parseFromSlice(pipeline.ReferenceFixture, a, bytes, .{});
    defer reference.deinit();
    const factory = @import("../architectures/session_factory.zig");
    const session = if (metal) try factory.createMetalSession(a, directory) else try factory.createNativeSession(a, directory);
    defer session.close();
    const identity = try factory.getGlinerBoundaryIdentity(session);
    try std.testing.expectEqualStrings(reference.value.model_files.@"model.safetensors".sha256, &identity.weight.sha256);
    const config = try factory.getGlinerBoundaryConfig(session);
    const path = try std.fs.path.join(a, &.{ directory, "tokenizer.json" });
    defer a.free(path);
    const tokenizer_bytes = try @import("../util/c_file.zig").readFileMax(a, path, 32 * 1024 * 1024);
    defer a.free(tokenizer_bytes);
    try std.testing.expectEqualStrings(reference.value.model_files.@"tokenizer.json".sha256, &artifact.Digest.of(tokenizer_bytes).sha256);
    const tokenizer = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    defer tokenizer.tokenizer().deinitTokenizer();
    const watchdog = if (metal) try @import("../hard_cancellation_watchdog.zig").HardCancellationWatchdog.create(a) else null;
    defer if (watchdog) |owner| owner.destroy();
    if (watchdog) |owner| try owner.start(std.testing.io);
    const control = Control{ .hard_cancellation = if (watchdog) |owner| owner.boundary() else null, .deadline_ns = @import("antfly_platform").time.monotonicNs() + 180 * std.time.ns_per_s };
    var managed = try factory.getManagedComputeBackend(session, a, null, control);
    defer managed.deinit();
    for (reference.value.cases) |case| {
        errdefer std.debug.print("long executor case: {s}\n", .{case.id});
        const raw = try std.json.Stringify.valueAlloc(a, .{ .schema_version = @as(u32, 2), .model = "boundary", .schema = case.schema, .inputs = .{.{ .content = case.text }}, .options = .{ .long_document = .{ .mode = "window" }, .offset_unit = "unicode_codepoints" } }, .{});
        defer a.free(raw);
        var request = try wire.parseJson(a, raw, .{});
        defer request.deinit();
        const item = &request.items[0];
        var options = Options{ .identity = identity, .pipeline = item.options.native(.{}), .control = control };
        var result = try execute(&managed.backend, a, &config, tokenizer.tokenizer(), item, options);
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.window_count);
        try pipeline.expectSample(case.expected, result.sample);
        options.limits.max_total_encoded_tokens = 1;
        try std.testing.expectError(error.LongDocumentWorkLimitExceeded, execute(&managed.backend, a, &config, tokenizer.tokenizer(), item, options));
    }
}

const FakeMergeMode = enum { success, output_limit, cancelled, exhausted };
fn fakeSourceValue(text: []const u8, start: usize, end: usize, confidence: f32) pipeline.Value {
    return .{ .text = text[start..end], .confidence = confidence, .source = .{ .start = start, .end = end, .unit = .utf8_bytes, .byte_start = start, .byte_end = end }, .token_span = null };
}
fn fakeWindow(a: Allocator, sample: pipeline.Sample, rows: []const []const f64, mentions: []const document_mod.MentionCandidate, values: []const pipeline.Value, records: []const document_mod.RecordCandidate) !Window {
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const samples = try arena.allocator().alloc(pipeline.Sample, 1);
    samples[0] = sample;
    const joints = try a.alloc(?@import("../pipelines/gliner_boundary_joint.zig").Candidates, 1);
    joints[0] = null;
    return .{ .result = .{ .allocator = a, .outputs = .{ .arena = arena, .samples = samples }, .classification_scores = null, .joint_candidates = joints }, .logits = rows, .mentions = mentions, .mention_values = values, .records = records };
}

/// All packets below are synthetic post-score evidence. No tokenizer, encoder,
/// boundary head, or checkpoint is used by these global integration tests.
fn exerciseFakeMerge(a: Allocator, mode: FakeMergeMode) !void {
    const fixtures = @import("../architectures/gliner_boundary_parity_test.zig");
    const config_bytes = try fixtures.fixtureBytes(a, "models/small/config.json");
    defer a.free(config_bytes);
    const encoder_bytes = try fixtures.fixtureBytes(a, "models/small/encoder_config.json");
    defer a.free(encoder_bytes);
    const config = try model.parseConfig(a, config_bytes, encoder_bytes);
    var request = try wire.parseJson(a,
        \\{"schema_version":2,"model":"synthetic","schema":{"entities":["symbol"],"entity_attributes":{"mood":{"labels":["calm","happy"]}},"classifications":[{"name":"choice","labels":["accept","reject"],"min_labels":1,"max_labels":1,"candidate_threshold":0.8}],"classification_constraints":[{"type":"Cardinality","task":"choice","minimum":1,"maximum":1}],"relations":[{"type":"connects"}],"structures":{"summary":{"fields":{"first":{"type":"str","cardinality":"required_one"},"last":{"type":"str","cardinality":"required_one"}}},"events":{"mode":"natural","anchor":"name","fields":{"name":{"type":"str","cardinality":"required_one"},"value":{"type":"str","cardinality":"required_one","exclusive":true}}}}},"inputs":[{"content":"Α X 😀 Y Z"}],"options":{"long_document":{"mode":"window","window_words":5,"overlap_words":3},"offset_unit":"utf16_codeunits"}}
    , .{});
    var request_live = true;
    defer if (request_live) request.deinit();
    const item = &request.items[0];
    var document = try document_mod.plan(a, item.text, item.compiled.fingerprint, .{ .mode = .windowed, .max_window_body_words = 5, .overlap_words = 3, .inference_fingerprint = [_]u8{7} ** 32 });
    var document_live = true;
    defer if (document_live) document.deinit();
    const left_text = try document.windowText(0);
    const right_text = try document.windowText(1);
    var left_entity = fakeSourceValue(left_text, 5, 9, 0.6);
    var right_entity = fakeSourceValue(right_text, 2, 6, 0.8);
    const mood = item.compiled.schema.entity_attributes[0];
    left_entity.attributes = &.{.{ .name = mood.name, .multi_label = false, .labels = &.{.{ .label = mood.labels[0], .confidence = 0.6 }} }};
    right_entity.attributes = &.{.{ .name = mood.name, .multi_label = false, .labels = &.{.{ .label = mood.labels[1], .confidence = 0.9 }} }};
    const left_records = [_]document_mod.RecordCandidate{
        .{ .structure_index = 0, .record_index = 0, .record = .{ .fields = &.{ .{ .name = "first", .dtype = .str, .values = &.{fakeSourceValue(left_text, 0, 2, 0.7)} }, .{ .name = "last", .dtype = .str, .values = &.{} } } } },
        .{ .structure_index = 1, .record_index = 0, .record = .{ .confidence = 0.8, .anchor = fakeSourceValue(left_text, 5, 9, 0.8).source, .fields = &.{ .{ .name = "name", .dtype = .str, .values = &.{fakeSourceValue(left_text, 5, 9, 0.8)} }, .{ .name = "value", .dtype = .str, .values = &.{fakeSourceValue(left_text, 3, 4, 0.8)} } } } },
    };
    const right_records = [_]document_mod.RecordCandidate{
        .{ .structure_index = 0, .record_index = 0, .record = .{ .fields = &.{ .{ .name = "first", .dtype = .str, .values = &.{} }, .{ .name = "last", .dtype = .str, .values = &.{fakeSourceValue(right_text, 9, 10, 0.8)} } } } },
        .{ .structure_index = 1, .record_index = 0, .record = .{ .confidence = 0.9, .anchor = fakeSourceValue(right_text, 2, 6, 0.9).source, .fields = &.{ .{ .name = "name", .dtype = .str, .values = &.{fakeSourceValue(right_text, 2, 6, 0.9)} }, .{ .name = "value", .dtype = .str, .values = &.{fakeSourceValue(right_text, 7, 8, 0.9)} } } } },
        .{ .structure_index = 1, .record_index = 1, .record = .{ .confidence = 0.85, .anchor = fakeSourceValue(right_text, 9, 10, 0.85).source, .fields = &.{ .{ .name = "name", .dtype = .str, .values = &.{fakeSourceValue(right_text, 9, 10, 0.85)} }, .{ .name = "value", .dtype = .str, .values = &.{fakeSourceValue(right_text, 7, 8, 0.85)} } } } },
    };
    var windows: [2]Window = undefined;
    var completed: usize = 0;
    defer for (windows[0..completed]) |*window| window.result.deinit();
    windows[0] = try fakeWindow(a, .{ .relations = &.{.{ .name = "connects", .schema_index = 0, .head = fakeSourceValue(left_text, 3, 4, 0.7), .tail = fakeSourceValue(left_text, 10, 11, 0.7), .confidence = 0.7 }} }, &.{&.{ -2, 2 }}, &.{.{ .entity_type = 0, .source = left_entity.source.?, .probability = left_entity.confidence }}, &.{left_entity}, &left_records);
    completed = 1;
    windows[1] = try fakeWindow(a, .{ .relations = &.{.{ .name = "connects", .schema_index = 0, .head = fakeSourceValue(right_text, 0, 1, 0.9), .tail = fakeSourceValue(right_text, 7, 8, 0.9), .confidence = 0.9 }} }, &.{&.{ 6, -6 }}, &.{.{ .entity_type = 0, .source = right_entity.source.?, .probability = right_entity.confidence }}, &.{right_entity}, &right_records);
    completed = 2;
    const constraints = @import("../pipelines/extraction_constraints.zig");
    var local = try constraints.solve(a, item.compiled.schema.classification_constraints, windows[0].logits, .{ .algorithm = .exact });
    defer local.deinit();
    // Local choices conflict. The final response must solve once from raw
    // aggregated logits, rather than unioning already selected labels.
    try std.testing.expectEqual(constraints.Status.optimal, local.status);
    try std.testing.expectEqual(@as(constraints.Selection, 2), local.selections[0]);
    const Cancellation = struct {
        calls: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls >= 4) return error.Cancelled;
        }
    };
    var cancelled = Cancellation{};
    var options = Options{ .identity = std.mem.zeroes(artifact.Identity), .pipeline = item.options.native(.{}) };
    options.pipeline.max_output_values = if (mode == .output_limit) 10 else 11;
    options.pipeline.classification_solver.algorithm = .exact;
    if (mode == .cancelled) {
        options.control = .{ .ptr = &cancelled, .check_fn = Cancellation.check };
        options.pipeline.control = options.control;
    }
    if (mode == .exhausted) options.pipeline.classification_solver.exact_node_budget = 0;
    var output_arena = std.heap.ArenaAllocator.init(a);
    defer output_arena.deinit();
    switch (mode) {
        .output_limit => return std.testing.expectError(error.ExtractionOutputLimitExceeded, mergeAll(a, output_arena.allocator(), document, &windows, item, &config, options)),
        .cancelled => {
            try std.testing.expectError(error.Cancelled, mergeAll(a, output_arena.allocator(), document, &windows, item, &config, options));
            try std.testing.expectEqual(@as(usize, 4), cancelled.calls);
            return;
        },
        .exhausted => return std.testing.expectError(error.ClassificationSearchExhausted, mergeAll(a, output_arena.allocator(), document, &windows, item, &config, options)),
        .success => {},
    }
    const output = try mergeAll(a, output_arena.allocator(), document, &windows, item, &config, options);
    // Destroy every source owner before inspecting the assembled response.
    for (&windows) |*window| window.result.deinit();
    completed = 0;
    document.deinit();
    document_live = false;
    request.deinit();
    request_live = false;
    try std.testing.expectEqualStrings("symbol", output.entities[0].name);
    const entity = output.entities[0].values[0];
    try std.testing.expectEqual(@as(usize, 1), output.entities[0].values.len);
    try std.testing.expectEqualStrings("😀", entity.text);
    try std.testing.expectEqual(@as(usize, 4), entity.source.?.start);
    try std.testing.expectEqual(@as(usize, 6), entity.source.?.end);
    try std.testing.expectEqualStrings("mood", entity.attributes[0].name);
    try std.testing.expectEqualStrings("happy", entity.attributes[0].labels[0].label);
    try std.testing.expectEqualStrings("accept", output.classifications[0].labels[0].label);
    try std.testing.expectEqual(pipeline.SolverStatus.optimal, output.classification_solver.?.status);
    try std.testing.expectEqual(@as(usize, 1), output.relations.len);
    try std.testing.expectEqualStrings("X", output.relations[0].head.text);
    try std.testing.expectEqualStrings("Y", output.relations[0].tail.text);
    try std.testing.expectEqual(@as(f32, 0.9), output.relations[0].confidence);
    try std.testing.expectEqual(@as(usize, 2), output.structures.len);
    try std.testing.expectEqualStrings("summary", output.structures[0].name);
    try std.testing.expectEqual(@as(usize, 1), output.structures[0].instances.len);
    try std.testing.expectEqualStrings("Α", output.structures[0].instances[0].fields[0].values[0].text);
    try std.testing.expectEqualStrings("Z", output.structures[0].instances[0].fields[1].values[0].text);
    const records = output.structures[1].instances;
    try std.testing.expectEqual(@as(usize, 2), records.len);
    try std.testing.expectEqualStrings("😀", records[0].fields[0].values[0].text);
    try std.testing.expectEqualStrings("X", records[0].fields[1].values[0].text);
    try std.testing.expectEqual(@as(?f32, 0.8), records[0].confidence);
    try std.testing.expectEqualStrings("Z", records[1].fields[0].values[0].text);
    try std.testing.expectEqualStrings("Y", records[1].fields[1].values[0].text);
    try std.testing.expectEqual(pipeline.SolverStatus.optimal, output.record_solver.?.status);
}

test "gliner boundary long executor fake windows preserve global decisions ownership and atomic limits" {
    for ([_]FakeMergeMode{ .success, .output_limit, .cancelled, .exhausted }) |mode| try exerciseFakeMerge(std.testing.allocator, mode);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, exerciseFakeMerge, .{FakeMergeMode.success});
}

// The current Zig arena can reject a larger speculative resize then try a
// smaller fresh node. This is a real allocator path, not a manually set denial.
test "gliner boundary long executor evidence terminal allocation preserves backing failure after speculative resize" {
    var fail = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
    var terminal: EvidenceAllocationFailure = null;
    var budget = BoundedAllocator{ .backing = fail.allocator(), .limit = 2048, .failure_context = &terminal, .allocation_failed = observeEvidenceAllocation };
    defer std.debug.assert(budget.live == 0);
    var arena = std.heap.ArenaAllocator.init(budget.allocator());
    defer arena.deinit();
    _ = try arena.allocator().alloc(u8, 8);
    const actual: anyerror = if (arena.allocator().alloc(u8, 1024)) |_| return error.ExpectedBackingAllocationFailure else |err| err;
    try std.testing.expect(budget.denied);
    const observed = terminal orelse return error.MissingTerminalAllocationFailure;
    try std.testing.expectEqual(.backing_allocator, observed.kind);
    try std.testing.expect(observed.requested_bytes <= observed.limit_bytes - observed.live_bytes);
    try std.testing.expectEqual(error.OutOfMemory, evidenceAllocationError(terminal, actual));
    try std.testing.expectEqual(error.Cancelled, evidenceAllocationError(terminal, error.Cancelled));
}

test "gliner boundary long executor evidence terminal allocation retains declared memory limit" {
    var terminal: EvidenceAllocationFailure = null;
    var budget = BoundedAllocator{ .backing = std.testing.allocator, .limit = 64, .failure_context = &terminal, .allocation_failed = observeEvidenceAllocation };
    defer std.debug.assert(budget.live == 0);
    var arena = std.heap.ArenaAllocator.init(budget.allocator());
    defer arena.deinit();
    const actual: anyerror = if (arena.allocator().alloc(u8, 1024)) |_| return error.ExpectedDeclaredAllocationFailure else |err| err;
    const observed = terminal orelse return error.MissingTerminalAllocationFailure;
    try std.testing.expectEqual(.declared_limit, observed.kind);
    try std.testing.expectEqual(error.MemoryBudgetExceeded, evidenceAllocationError(terminal, actual));
    try std.testing.expectEqual(error.OutOfMemory, evidenceAllocationError(null, error.OutOfMemory));
}
