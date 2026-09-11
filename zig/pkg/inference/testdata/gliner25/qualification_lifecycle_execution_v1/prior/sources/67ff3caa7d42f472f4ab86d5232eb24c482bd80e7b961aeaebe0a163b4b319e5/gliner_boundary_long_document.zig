// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Explicit, versioned long-document extension. Planning never truncates text.
//! Merge inputs must contain every planned window, including empty outputs.
//! Classification accepts all raw label logits; JointIE accepts candidates
//! before local decoding. Local winners cannot establish global feasibility.
//! This module is deliberately independent of serving and model execution.
const std = @import("std");
const Allocator = std.mem.Allocator;
const processor = @import("gliner_boundary_processor.zig");
const boundary = @import("gliner_boundary_decode.zig");
const schema_mod = @import("extraction_schema.zig");
const constraints = @import("extraction_constraints.zig");
const joint = @import("extraction_joint_ie.zig");
const pipeline = @import("gliner_boundary_pipeline.zig");
const relations = @import("gliner_boundary_relations.zig");
pub const record_selection = @import("gliner_boundary_record_selection.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const BoundedAllocator = @import("../runtime/bounded_allocator.zig").BoundedAllocator;

pub const semantics_version = 1;
pub const Mode = enum { reject, windowed };
pub const RecordIdentity = enum { occurrence, semantic };
pub const Descriptor = struct {
    version: u32 = semantics_version,
    mode: Mode,
    window_policy: enum { source_words_midpoint_ownership } = .source_words_midpoint_ownership,
    classification_aggregation: enum { owned_word_weighted_mean_raw_logits } = .owned_word_weighted_mean_raw_logits,
    duplicate_score: enum { maximum_calibrated_score } = .maximum_calibrated_score,
    natural_record_identity: enum { exact_source_anchor } = .exact_source_anchor,
    record_selection: enum { maximum_sum_whole_record_ranking_then_count_then_source } = .maximum_sum_whole_record_ranking_then_count_then_source,
    other_record_identity: RecordIdentity,
    solver_optimality_scope: enum { retained_candidate_graph } = .retained_candidate_graph,
    /// Word planning is followed by exact tokenizer/schema admission. The
    /// caller must reject an encoded window that exceeds its token budget.
    requires_encoded_token_admission: bool = true,
};

/// The bounded allocator is below the arena, so freed arena scratch still
/// counts toward the physical live ceiling. Its address remains heap-stable.
const Owner = struct {
    backing: Allocator,
    budget: BoundedAllocator,
    arena: std.heap.ArenaAllocator,
    terminal: ?BoundedAllocator.AllocationFailure = null,
    fn init(backing: Allocator, limit: usize) !*Owner {
        if (limit == 0 or limit > 1024 * 1024 * 1024) return error.InvalidLongDocumentLimits;
        const self = try backing.create(Owner);
        self.* = .{ .backing = backing, .budget = .{ .backing = backing, .limit = limit, .failure_context = self, .allocation_failed = observeAllocation }, .arena = undefined };
        self.arena = std.heap.ArenaAllocator.init(self.budget.allocator());
        return self;
    }
    fn allocator(self: *Owner) Allocator {
        return self.arena.allocator();
    }
    fn observeAllocation(raw: ?*anyopaque, failure: BoundedAllocator.AllocationFailure) void {
        const self: *Owner = @ptrCast(@alignCast(raw.?));
        self.terminal = failure;
    }
    fn allocationError(self: *const Owner, err: anyerror) anyerror {
        if (err != error.OutOfMemory) return err;
        const failure = self.terminal orelse return err;
        return if (failure.kind == .declared_limit) error.MemoryBudgetExceeded else err;
    }
    fn deinit(self: *Owner) void {
        const backing = self.backing;
        self.arena.deinit();
        std.debug.assert(self.budget.live == 0);
        backing.destroy(self);
    }
};

pub const PlanOptions = struct {
    mode: Mode = .reject,
    word_splitter: processor.WordSplitter = .whitespace,
    /// Includes the synthetic final word, excludes the synthetic enum prefix.
    max_window_body_words: usize = 4096,
    overlap_words: usize = 128,
    max_document_bytes: usize = 1024 * 1024,
    max_document_words: usize = 131072,
    max_windows: usize = 64,
    max_window_scan_bytes: usize = 16 * 1024 * 1024,
    max_memory_bytes: usize = 64 * 1024 * 1024,
    /// Caller hashes the immutable model/artifact, quantization, calibration
    /// and inference options. Planning may precede binding, but score merges
    /// cannot proceed until this profile is present and identical everywhere.
    inference_fingerprint: ?[32]u8 = null,
    other_record_identity: RecordIdentity = .occurrence,
    control: ?Control = null,
};
pub const Window = struct {
    index: usize,
    bytes: boundary.Offsets,
    words: boundary.Offsets,
    owned_words: boundary.Offsets,
    synthetic_terminal_word: bool,
    pub fn weight(self: Window) usize {
        return self.owned_words.end - self.owned_words.start;
    }
};
pub const Identity = struct {
    plan_fingerprint: [32]u8,
    schema_fingerprint: [32]u8,
    inference_fingerprint: [32]u8,
    window_index: usize,
};
pub const Plan = struct {
    owner: *Owner,
    text: []const u8,
    words: []const processor.ByteRange,
    windows: []const Window,
    offsets: boundary.OffsetMap,
    schema_fingerprint: [32]u8,
    inference_fingerprint: ?[32]u8,
    fingerprint: [32]u8,
    descriptor: Descriptor,

    pub fn deinit(self: *Plan) void {
        self.owner.deinit();
        self.* = undefined;
    }
    pub fn identity(self: Plan, index: usize) !Identity {
        if (index >= self.windows.len) return error.InvalidLongDocumentWindow;
        return .{ .plan_fingerprint = self.fingerprint, .schema_fingerprint = self.schema_fingerprint, .inference_fingerprint = self.inference_fingerprint orelse return error.UnboundLongDocumentInferenceProfile, .window_index = index };
    }
    pub fn windowText(self: Plan, index: usize) ![]const u8 {
        if (index >= self.windows.len) return error.InvalidLongDocumentWindow;
        const window = self.windows[index];
        return self.text[window.bytes.start..window.bytes.end];
    }
    pub fn validateIdentity(self: Plan, value: Identity) !void {
        const profile = self.inference_fingerprint orelse return error.UnboundLongDocumentInferenceProfile;
        if (value.window_index >= self.windows.len or
            !std.mem.eql(u8, &self.fingerprint, &value.plan_fingerprint) or
            !std.mem.eql(u8, &self.schema_fingerprint, &value.schema_fingerprint) or
            !std.mem.eql(u8, &profile, &value.inference_fingerprint)) return error.LongDocumentIdentityMismatch;
    }

    /// Rebase using one immutable document index. No second Unicode map or
    /// lowercase/normalization round trip can change source coordinates.
    pub fn rebase(self: Plan, index: usize, local: boundary.Offsets, from: boundary.OffsetUnit, to: boundary.OffsetUnit) !pipeline.SourceSpan {
        if (index >= self.windows.len or local.end <= local.start) return error.InvalidLongDocumentWindowSpan;
        const window = self.windows[index];
        const base = try self.offsets.convert(window.bytes, from);
        if (local.end > base.end - base.start) return error.InvalidLongDocumentWindowSpan;
        const source = try self.offsets.toBytes(.{
            .start = try std.math.add(usize, base.start, local.start),
            .end = try std.math.add(usize, base.start, local.end),
        }, from);
        if (source.start < window.bytes.start or source.end > window.bytes.end) return error.InvalidLongDocumentWindowSpan;
        const requested = try self.offsets.convert(source, to);
        return .{ .start = requested.start, .end = requested.end, .unit = to, .byte_start = source.start, .byte_end = source.end };
    }
    pub fn rebaseSource(self: Plan, index: usize, source: pipeline.SourceSpan, to: boundary.OffsetUnit) !pipeline.SourceSpan {
        const result = try self.rebase(index, .{ .start = source.start, .end = source.end }, source.unit, to);
        const base = self.windows[index].bytes.start;
        if (source.byte_start != result.byte_start - base or source.byte_end != result.byte_end - base)
            return error.InconsistentLongDocumentSourceSpan;
        return result;
    }

    /// Ownership is a deterministic tie break, not a proposal filter: a span
    /// visible only in a neighboring context window is retained.
    pub fn ownerOfSpan(self: Plan, bytes: boundary.Offsets) !usize {
        if (bytes.end <= bytes.start or bytes.end > self.text.len) return error.InvalidLongDocumentWindowSpan;
        _ = try self.offsets.convert(bytes, .utf8_bytes);
        if (self.words.len == 0) return 0;
        const midpoint = bytes.start + (bytes.end - bytes.start) / 2;
        var lo: usize = 0;
        var hi = self.words.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.words[mid].end <= midpoint) lo = mid + 1 else hi = mid;
        }
        const word = @min(lo, self.words.len - 1);
        for (self.windows) |window| if (word >= window.owned_words.start and word < window.owned_words.end) return window.index;
        return error.InvalidLongDocumentOwnership;
    }
};

fn check(control: ?Control) !void {
    if (control) |value| try value.check();
}
fn hashNumber(hash: *std.crypto.hash.sha2.Sha256, value: usize) void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, @intCast(value), .little);
    hash.update(&bytes);
}

pub fn plan(allocator: Allocator, text: []const u8, schema_fingerprint: [32]u8, options: PlanOptions) !Plan {
    try check(options.control);
    if (options.max_window_body_words == 0 or options.max_window_body_words > 4096 or
        options.overlap_words >= options.max_window_body_words or options.max_document_bytes == 0 or
        options.max_document_bytes > 16 * 1024 * 1024 or options.max_document_words == 0 or options.max_document_words > 1048576 or
        options.max_windows == 0 or options.max_windows > 1024 or options.max_window_scan_bytes == 0 or
        options.max_window_scan_bytes > 1024 * 1024 * 1024) return error.InvalidLongDocumentLimits;
    const owner = try Owner.init(allocator, options.max_memory_bytes);
    errdefer owner.deinit();
    return planOwned(owner, text, schema_fingerprint, options) catch |err| return owner.allocationError(err);
}

fn planOwned(owner: *Owner, text: []const u8, schema_fingerprint: [32]u8, options: PlanOptions) !Plan {
    const a = owner.allocator();
    const ranges = try processor.sourceWordRanges(a, text, .{
        .word_splitter = options.word_splitter,
        .max_text_bytes = options.max_document_bytes,
        .max_words = options.max_document_words,
        .control = options.control,
    });
    if (text.len > options.max_window_scan_bytes) return error.LongDocumentWorkLimitExceeded;
    var scanned = text.len;
    const terminal = try processor.terminalWordAdded(a, text, options.word_splitter);
    try check(options.control);
    const full_count = try std.math.add(usize, ranges.len, @intFromBool(terminal));
    var windows = std.ArrayListUnmanaged(Window).empty;
    if (full_count <= options.max_window_body_words) {
        try windows.append(a, .{ .index = 0, .bytes = .{ .start = 0, .end = text.len }, .words = .{ .start = 0, .end = ranges.len }, .owned_words = .{ .start = 0, .end = ranges.len }, .synthetic_terminal_word = terminal });
    } else {
        if (options.mode == .reject) return error.LongDocumentWindowingRequired;
        var start: usize = 0;
        while (start < ranges.len) {
            try check(options.control);
            if (windows.items.len >= options.max_windows) return error.LongDocumentWindowLimitExceeded;
            var end = @min(ranges.len, start + options.max_window_body_words);
            var bytes = boundary.Offsets{ .start = if (start == 0) 0 else ranges[start].start, .end = if (end == ranges.len) text.len else ranges[end - 1].end };
            if (bytes.end - bytes.start > options.max_window_scan_bytes - scanned) return error.LongDocumentWorkLimitExceeded;
            scanned += bytes.end - bytes.start;
            var extra = try processor.terminalWordAdded(a, text[bytes.start..bytes.end], options.word_splitter);
            if (end - start + @intFromBool(extra) > options.max_window_body_words) {
                end -= 1;
                if (end == start) return error.InvalidLongDocumentLimits;
                bytes.end = ranges[end - 1].end;
                if (bytes.end - bytes.start > options.max_window_scan_bytes - scanned) return error.LongDocumentWorkLimitExceeded;
                scanned += bytes.end - bytes.start;
                extra = try processor.terminalWordAdded(a, text[bytes.start..bytes.end], options.word_splitter);
            }
            if (end - start + @intFromBool(extra) > options.max_window_body_words) return error.InvalidLongDocumentLimits;
            try windows.append(a, .{ .index = windows.items.len, .bytes = bytes, .words = .{ .start = start, .end = end }, .owned_words = undefined, .synthetic_terminal_word = extra });
            if (end == ranges.len) break;
            if (end - start <= options.overlap_words) return error.InvalidLongDocumentLimits;
            start = end - options.overlap_words;
        }
        var owned_start: usize = 0;
        for (windows.items, 0..) |*window, i| {
            const owned_end = if (i + 1 == windows.items.len) ranges.len else (window.words.end + windows.items[i + 1].words.start) / 2;
            window.owned_words = .{ .start = owned_start, .end = owned_end };
            owned_start = owned_end;
        }
    }
    const copied = try a.dupe(u8, text);
    const offsets = try boundary.OffsetMap.init(a, copied, options.max_document_bytes);
    try check(options.control);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly-gliner25-window-plan/v1\x00");
    hash.update(&schema_fingerprint);
    if (options.inference_fingerprint) |profile| hash.update(&profile);
    hashNumber(&hash, text.len);
    hash.update(text);
    hashNumber(&hash, @intFromEnum(options.word_splitter));
    hashNumber(&hash, @intFromEnum(options.mode));
    hashNumber(&hash, @intFromEnum(options.other_record_identity));
    for (windows.items) |window| {
        hashNumber(&hash, window.bytes.start);
        hashNumber(&hash, window.bytes.end);
        hashNumber(&hash, window.owned_words.start);
        hashNumber(&hash, window.owned_words.end);
    }
    var fingerprint: [32]u8 = undefined;
    hash.final(&fingerprint);
    return .{ .owner = owner, .text = copied, .words = ranges, .windows = try windows.toOwnedSlice(a), .offsets = offsets, .schema_fingerprint = schema_fingerprint, .inference_fingerprint = options.inference_fingerprint, .fingerprint = fingerprint, .descriptor = .{ .mode = options.mode, .other_record_identity = options.other_record_identity } };
}

pub const MergeOptions = struct {
    max_input_candidates: usize = 65536,
    max_output_candidates: usize = 4096,
    max_text_bytes: usize = 1024 * 1024,
    max_work: usize = 10000000,
    max_memory_bytes: usize = 64 * 1024 * 1024,
    control: ?Control = null,
};
const Work = struct {
    options: MergeOptions,
    steps: usize = 0,
    inputs: usize = 0,
    text_bytes: usize = 0,
    fn init(options: MergeOptions) !Work {
        try check(options.control);
        if (options.max_input_candidates == 0 or options.max_input_candidates > 1048576 or
            options.max_output_candidates == 0 or options.max_output_candidates > 65536 or
            options.max_work == 0 or options.max_work > 1000000000 or options.max_text_bytes == 0 or
            options.max_text_bytes > 16 * 1024 * 1024) return error.InvalidLongDocumentLimits;
        return .{ .options = options };
    }
    fn tick(self: *Work) !void {
        if (self.steps >= self.options.max_work) return error.LongDocumentWorkLimitExceeded;
        self.steps += 1;
        if (self.steps % 64 == 1) try check(self.options.control);
    }
    fn admit(self: *Work, count: usize) !void {
        if (count > self.options.max_input_candidates - self.inputs) return error.LongDocumentCandidateLimitExceeded;
        self.inputs += count;
        try self.tick();
    }
    fn admitText(self: *Work, count: usize) !void {
        if (count > self.options.max_text_bytes - self.text_bytes) return error.LongDocumentTextLimitExceeded;
        self.text_bytes += count;
        try self.tick();
    }
};

/// Processing order depends on the plan, never worker completion order.
fn orderedWindows(a: Allocator, document: Plan, windows: anytype, work: *Work) ![]usize {
    if (windows.len != document.windows.len) return error.IncompleteLongDocumentWindows;
    const order = try a.alloc(usize, windows.len);
    @memset(order, std.math.maxInt(usize));
    for (windows, 0..) |window, index| {
        try work.tick();
        try document.validateIdentity(window.identity);
        const target = window.identity.window_index;
        if (order[target] != std.math.maxInt(usize)) return error.DuplicateLongDocumentWindow;
        order[target] = index;
    }
    return order;
}
fn validateSchema(document: Plan, compiled: *const schema_mod.CompiledSchema) !void {
    if (!std.mem.eql(u8, &document.schema_fingerprint, &compiled.fingerprint)) return error.LongDocumentIdentityMismatch;
}
fn validProbability(value: f64) bool {
    return std.math.isFinite(value) and value >= 0 and value <= 1;
}

pub const WindowLogits = struct { identity: Identity, logits: []const []const f64 };
pub const AggregatedLogits = struct {
    owner: *Owner,
    logits: []const []const f64,
    descriptor: Descriptor,
    pub fn deinit(self: *AggregatedLogits) void {
        self.owner.deinit();
        self.* = undefined;
    }
};

pub fn aggregateClassificationLogits(allocator: Allocator, document: Plan, compiled: *const schema_mod.CompiledSchema, windows: []const WindowLogits, options: MergeOptions) !AggregatedLogits {
    const work = try Work.init(options);
    try validateSchema(document, compiled);
    const owner = try Owner.init(allocator, options.max_memory_bytes);
    errdefer owner.deinit();
    return aggregateClassificationLogitsOwned(owner, document, compiled, windows, options, work) catch |err| return owner.allocationError(err);
}

fn aggregateClassificationLogitsOwned(owner: *Owner, document: Plan, compiled: *const schema_mod.CompiledSchema, windows: []const WindowLogits, options: MergeOptions, initial_work: Work) !AggregatedLogits {
    var work = initial_work;
    const a = owner.allocator();
    const order = try orderedWindows(a, document, windows, &work);
    const tasks = compiled.schema.classification_constraints.tasks;
    const sums = try a.alloc([]f64, tasks.len);
    const rows = try a.alloc([]const f64, tasks.len);
    for (tasks, sums, rows) |task, *sum, *row| {
        sum.* = try a.alloc(f64, task.labels.len);
        @memset(sum.*, 0);
        row.* = sum.*;
    }
    const total_weight: f64 = @floatFromInt(@max(document.words.len, 1));
    for (order, document.windows) |index, window| {
        const input = windows[index];
        if (input.logits.len != tasks.len) return error.InvalidLongDocumentClassificationLogits;
        const weight = @as(f64, @floatFromInt(if (document.words.len == 0) @as(usize, 1) else window.weight())) / total_weight;
        for (input.logits, tasks, sums) |scores, task, output| {
            if (scores.len != task.labels.len) return error.InvalidLongDocumentClassificationLogits;
            try work.admit(scores.len);
            for (scores, output) |score, *value| {
                try work.tick();
                if (!std.math.isFinite(score)) return error.NonFiniteBoundaryScore;
                value.* += score * weight;
                if (!std.math.isFinite(value.*)) return error.NonFiniteBoundaryScore;
            }
        }
    }
    try check(options.control);
    return .{ .owner = owner, .logits = rows, .descriptor = document.descriptor };
}

const ClassificationControl = struct {
    control: ?Control,
    original_context: ?*anyopaque,
    original_fn: ?*const fn (?*anyopaque) anyerror!void,
    fn checkBoth(raw: ?*anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        try check(self.control);
        if (self.original_fn) |f| try f(self.original_context);
    }
};
pub const ClassificationOptions = struct { merge: MergeOptions = .{}, solver: constraints.SolveOptions = .{}, best_effort: bool = false };
pub const GlobalClassification = struct {
    aggregated: AggregatedLogits,
    result: constraints.Result,
    pub fn deinit(self: *GlobalClassification) void {
        self.result.deinit();
        self.aggregated.deinit();
        self.* = undefined;
    }
};

/// Structured selection only. Ordinary classification presentation should
/// consume aggregateClassificationLogits with its activation/fallback policy.
pub fn solveClassifications(allocator: Allocator, document: Plan, compiled: *const schema_mod.CompiledSchema, windows: []const WindowLogits, options: ClassificationOptions) !GlobalClassification {
    var aggregated = try aggregateClassificationLogits(allocator, document, compiled, windows, options.merge);
    errdefer aggregated.deinit();
    var bridge = ClassificationControl{ .control = options.merge.control, .original_context = options.solver.check_context, .original_fn = options.solver.check_fn };
    var solver = options.solver;
    solver.check_context = &bridge;
    solver.check_fn = ClassificationControl.checkBoth;
    var result = constraints.solve(aggregated.owner.allocator(), compiled.schema.classification_constraints, aggregated.logits, solver) catch |err| return aggregated.owner.allocationError(err);
    errdefer result.deinit();
    if (!result.valid()) return if (result.status == .infeasible) error.LongDocumentClassificationInfeasible else error.LongDocumentClassificationSearchExhausted;
    if (result.exhausted and !options.best_effort) return error.LongDocumentClassificationSearchExhausted;
    const program = compiled.schema.classification_constraints;
    if (try program.evaluate(.{ .selected = result.selections, .possible = result.selections }) != .yes) return error.InvalidLongDocumentClassificationAssignment;
    try check(options.merge.control);
    return .{ .aggregated = aggregated, .result = result };
}

pub const WindowJointCandidates = struct { identity: Identity, nodes: []const joint.Node, edges: []const joint.Edge };
pub const CandidateOrigin = struct { window_index: usize, candidate_index: usize };
pub const JointCandidates = struct {
    owner: *Owner,
    nodes: []const joint.Node,
    edges: []const joint.Edge,
    node_origins: []const CandidateOrigin,
    edge_origins: []const CandidateOrigin,
    descriptor: Descriptor,
    pub fn deinit(self: *JointCandidates) void {
        self.owner.deinit();
        self.* = undefined;
    }
};

fn scopedId(window: usize, value: ?u64) !?u64 {
    const id = value orelse return null;
    if (id > std.math.maxInt(u32) or window > std.math.maxInt(u32)) return error.LongDocumentJointIdentityLimitExceeded;
    return (@as(u64, @intCast(window)) << 32) | id;
}
fn sameEdge(a: joint.Edge, b: joint.Edge) bool {
    return a.relation_type == b.relation_type and a.head == b.head and a.tail == b.tail and a.slot == b.slot and
        a.hypothesis == b.hypothesis and a.count_alternative == b.count_alternative;
}

pub fn mergeJointCandidates(allocator: Allocator, document: Plan, compiled: *const schema_mod.CompiledSchema, windows: []const WindowJointCandidates, options: MergeOptions, max_nodes: usize, max_edges: usize) !JointCandidates {
    const work = try Work.init(options);
    try validateSchema(document, compiled);
    _ = compiled.schema.joint_ie orelse return error.InvalidLongDocumentJointSchema;
    if (max_nodes == 0 or max_nodes > 256 or max_edges == 0 or max_edges > 256) return error.InvalidLongDocumentLimits;
    const owner = try Owner.init(allocator, options.max_memory_bytes);
    errdefer owner.deinit();
    return mergeJointCandidatesOwned(owner, document, compiled, windows, options, max_nodes, max_edges, work) catch |err| return owner.allocationError(err);
}

fn mergeJointCandidatesOwned(owner: *Owner, document: Plan, compiled: *const schema_mod.CompiledSchema, windows: []const WindowJointCandidates, options: MergeOptions, max_nodes: usize, max_edges: usize, initial_work: Work) !JointCandidates {
    var work = initial_work;
    const schema = compiled.schema.joint_ie.?;
    const a = owner.allocator();
    const order = try orderedWindows(a, document, windows, &work);
    var nodes = std.ArrayListUnmanaged(joint.Node).empty;
    var edges = std.ArrayListUnmanaged(joint.Edge).empty;
    var node_origins = std.ArrayListUnmanaged(CandidateOrigin).empty;
    var edge_origins = std.ArrayListUnmanaged(CandidateOrigin).empty;
    for (order, 0..) |index, window_index| {
        const input = windows[index];
        try work.admit(input.nodes.len);
        try work.admit(input.edges.len);
        const remap = try a.alloc(usize, input.nodes.len);
        for (input.nodes, remap, 0..) |candidate, *mapped, candidate_index| {
            try work.tick();
            if (candidate.entity_type >= schema.entities.len or !validProbability(candidate.probability) or !std.math.isFinite(candidate.utility))
                return error.InvalidJointCandidate;
            const source = try document.rebase(window_index, .{ .start = candidate.start, .end = candidate.end }, .utf8_bytes, .utf8_bytes);
            var normalized = candidate;
            normalized.start = source.byte_start;
            normalized.end = source.byte_end;
            var duplicate: ?usize = null;
            for (nodes.items, 0..) |previous, j| {
                try work.tick();
                if (previous.entity_type == normalized.entity_type and previous.start == normalized.start and previous.end == normalized.end) {
                    duplicate = j;
                    break;
                }
            }
            if (duplicate) |j| {
                mapped.* = j;
                normalized.required = normalized.required or nodes.items[j].required;
                if (normalized.utility > nodes.items[j].utility or (normalized.utility == nodes.items[j].utility and normalized.probability > nodes.items[j].probability)) {
                    nodes.items[j] = normalized;
                    node_origins.items[j] = .{ .window_index = window_index, .candidate_index = candidate_index };
                } else nodes.items[j].required = normalized.required;
            } else {
                if (nodes.items.len >= @min(max_nodes, options.max_output_candidates)) return error.LongDocumentCandidateLimitExceeded;
                mapped.* = nodes.items.len;
                try nodes.append(a, normalized);
                try node_origins.append(a, .{ .window_index = window_index, .candidate_index = candidate_index });
            }
        }
        for (input.edges, 0..) |candidate, candidate_index| {
            try work.tick();
            if (candidate.head >= remap.len or candidate.tail >= remap.len or candidate.relation_type >= schema.relations.len or
                !validProbability(candidate.probability) or !std.math.isFinite(candidate.utility) or
                (candidate.count_alternative != null and candidate.hypothesis == null)) return error.InvalidJointCandidate;
            var normalized = candidate;
            normalized.head = remap[candidate.head];
            normalized.tail = remap[candidate.tail];
            normalized.slot = try scopedId(window_index, candidate.slot);
            normalized.hypothesis = try scopedId(window_index, candidate.hypothesis);
            normalized.count_alternative = try scopedId(window_index, candidate.count_alternative);
            var duplicate: ?usize = null;
            for (edges.items, 0..) |previous, j| {
                try work.tick();
                if (sameEdge(previous, normalized)) {
                    duplicate = j;
                    break;
                }
            }
            if (duplicate) |j| {
                normalized.required = normalized.required or edges.items[j].required;
                if (normalized.utility > edges.items[j].utility or (normalized.utility == edges.items[j].utility and normalized.probability > edges.items[j].probability)) {
                    edges.items[j] = normalized;
                    edge_origins.items[j] = .{ .window_index = window_index, .candidate_index = candidate_index };
                } else edges.items[j].required = normalized.required;
            } else {
                if (edges.items.len >= @min(max_edges, options.max_output_candidates)) return error.LongDocumentCandidateLimitExceeded;
                try edges.append(a, normalized);
                try edge_origins.append(a, .{ .window_index = window_index, .candidate_index = candidate_index });
            }
        }
    }
    try check(options.control);
    return .{ .owner = owner, .nodes = try nodes.toOwnedSlice(a), .edges = try edges.toOwnedSlice(a), .node_origins = try node_origins.toOwnedSlice(a), .edge_origins = try edge_origins.toOwnedSlice(a), .descriptor = document.descriptor };
}

const JointControl = struct {
    merge: ?Control,
    solver: ?Control,
    fn checkBoth(raw: ?*anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        try check(self.merge);
        try check(self.solver);
    }
};
pub const JointOptions = struct { merge: MergeOptions = .{}, solver: joint.Options = .{}, best_effort: bool = false };
pub const GlobalJoint = struct {
    candidates: JointCandidates,
    result: joint.Result,
    pub fn deinit(self: *GlobalJoint) void {
        self.result.deinit();
        self.candidates.deinit();
        self.* = undefined;
    }
};

pub fn solveJoint(allocator: Allocator, document: Plan, compiled: *const schema_mod.CompiledSchema, windows: []const WindowJointCandidates, options: JointOptions) !GlobalJoint {
    var candidates = try mergeJointCandidates(allocator, document, compiled, windows, options.merge, options.solver.max_nodes, options.solver.max_edges);
    errdefer candidates.deinit();
    var bridge = JointControl{ .merge = options.merge.control, .solver = options.solver.control };
    var solver = options.solver;
    solver.control = .{ .ptr = &bridge, .check_fn = JointControl.checkBoth };
    const a = candidates.owner.allocator();
    var result = joint.decode(a, compiled.schema.joint_ie.?, candidates.nodes, candidates.edges, solver) catch |err| return candidates.owner.allocationError(err);
    errdefer result.deinit();
    if (!result.valid()) return if (result.status == .infeasible) error.LongDocumentJointInfeasible else error.LongDocumentJointSearchExhausted;
    if (result.exhausted and !options.best_effort) return error.LongDocumentJointSearchExhausted;
    if (!(joint.validateGlobal(a, compiled.schema.joint_ie.?, result.nodes, result.edges, solver) catch |err| return candidates.owner.allocationError(err))) return error.InvalidLongDocumentJointAssignment;
    try check(options.merge.control);
    return .{ .candidates = candidates, .result = result };
}

pub const MentionCandidate = struct { entity_type: usize, source: pipeline.SourceSpan, probability: f32 };
pub const WindowMentions = struct {
    identity: Identity,
    /// Threshold/validator-admitted candidates before per-window overlap.
    /// The winning origin retains that candidate's complete attribute payload.
    pre_overlap_candidates: []const MentionCandidate,
};
pub const SelectedMention = struct { entity_type: usize, source: pipeline.SourceSpan, probability: f32, origin: CandidateOrigin };
pub const Mentions = struct {
    owner: *Owner,
    selected: []const SelectedMention,
    descriptor: Descriptor,
    pub fn deinit(self: *Mentions) void {
        self.owner.deinit();
        self.* = undefined;
    }
};

pub fn mergeMentions(allocator: Allocator, document: Plan, compiled: *const schema_mod.CompiledSchema, windows: []const WindowMentions, policy: boundary.OverlapPolicy, output_unit: boundary.OffsetUnit, options: MergeOptions) !Mentions {
    const work = try Work.init(options);
    try validateSchema(document, compiled);
    const owner = try Owner.init(allocator, options.max_memory_bytes);
    errdefer owner.deinit();
    return mergeMentionsOwned(owner, document, compiled, windows, policy, output_unit, options, work) catch |err| return owner.allocationError(err);
}

fn mergeMentionsOwned(owner: *Owner, document: Plan, compiled: *const schema_mod.CompiledSchema, windows: []const WindowMentions, policy: boundary.OverlapPolicy, output_unit: boundary.OffsetUnit, options: MergeOptions, initial_work: Work) !Mentions {
    var work = initial_work;
    const a = owner.allocator();
    const order = try orderedWindows(a, document, windows, &work);
    var candidates = std.ArrayListUnmanaged(SelectedMention).empty;
    for (order, 0..) |index, window_index| {
        const input = windows[index].pre_overlap_candidates;
        try work.admit(input.len);
        for (input, 0..) |candidate, candidate_index| {
            try work.tick();
            if (candidate.entity_type >= compiled.schema.entities.len or !validProbability(candidate.probability)) return error.InvalidLongDocumentMention;
            const source = try document.rebaseSource(window_index, candidate.source, output_unit);
            const normalized = SelectedMention{ .entity_type = candidate.entity_type, .source = source, .probability = candidate.probability, .origin = .{ .window_index = window_index, .candidate_index = candidate_index } };
            var duplicate: ?usize = null;
            for (candidates.items, 0..) |previous, j| {
                try work.tick();
                if (previous.entity_type == normalized.entity_type and previous.source.byte_start == source.byte_start and previous.source.byte_end == source.byte_end) {
                    duplicate = j;
                    break;
                }
            }
            if (duplicate) |j| {
                const previous = candidates.items[j];
                const owns = try document.ownerOfSpan(.{ .start = source.byte_start, .end = source.byte_end });
                if (normalized.probability > previous.probability or
                    (normalized.probability == previous.probability and window_index == owns and previous.origin.window_index != owns)) candidates.items[j] = normalized;
            } else {
                if (candidates.items.len >= options.max_output_candidates) return error.LongDocumentCandidateLimitExceeded;
                try candidates.append(a, normalized);
            }
        }
    }
    var selected = std.ArrayListUnmanaged(SelectedMention).empty;
    const query_candidates = try a.alloc(boundary.Candidate, candidates.items.len);
    for (compiled.schema.entities, 0..) |_, entity_type| {
        var count: usize = 0;
        for (candidates.items, 0..) |candidate, index| {
            try work.tick();
            if (candidate.entity_type != entity_type) continue;
            query_candidates[count] = .{ .start = candidate.source.byte_start, .end = candidate.source.byte_end, .probability = candidate.probability, .source_index = index };
            count += 1;
        }
        // Account for the decoder's bounded quadratic dedup/tie history before
        // entering it. Cancellation continues through the shared decoder hook.
        const decoder_work = try std.math.mul(usize, count, count);
        if (decoder_work > options.max_work - work.steps) return error.LongDocumentWorkLimitExceeded;
        work.steps += decoder_work;
        const resolved = try boundary.resolveOverlaps(a, query_candidates[0..count], policy, .{ .max_candidates = options.max_output_candidates, .control = options.control });
        for (resolved) |candidate| try selected.append(a, candidates.items[candidate.source_index]);
    }
    const Less = struct {
        fn less(_: void, left: SelectedMention, right: SelectedMention) bool {
            if (left.entity_type != right.entity_type) return left.entity_type < right.entity_type;
            if (left.source.byte_start != right.source.byte_start) return left.source.byte_start < right.source.byte_start;
            return left.source.byte_end < right.source.byte_end;
        }
    };
    std.mem.sort(SelectedMention, selected.items, {}, Less.less);
    try check(options.control);
    return .{ .owner = owner, .selected = try selected.toOwnedSlice(a), .descriptor = document.descriptor };
}

pub const RecordCandidate = struct { structure_index: usize, record_index: usize, record: pipeline.Record };
pub const WindowRecords = struct { identity: Identity, records: []const RecordCandidate };
pub const RecordOrigin = struct { window_index: usize, record_index: usize };
pub const SelectedRecord = struct {
    structure_index: usize,
    origin: RecordOrigin,
    anchor: ?pipeline.SourceSpan,
    /// Whole-record ranking; a missing record probability uses the mean of
    /// its field probabilities. This is selection metadata, not a new model
    /// probability and must not overwrite the selected record's confidence.
    ranking_score: f64,
};
pub const Records = struct {
    owner: *Owner,
    /// Selection references keep whole source records intact. The caller must
    /// retain window results until it copies/rebases each selected payload.
    selected: []const SelectedRecord,
    descriptor: Descriptor,
    diagnostics: pipeline.Diagnostics,
    pub fn deinit(self: *Records) void {
        self.owner.deinit();
        self.* = undefined;
    }
};
pub const RecordOptions = struct {
    merge: MergeOptions = .{},
    solver: record_selection.Options = .{},
    best_effort: bool = false,
    output_unit: boundary.OffsetUnit = .utf8_bytes,
    regex_context: ?*anyopaque = null,
    validate_value_fn: ?*const fn (?*anyopaque, schema_mod.RegexValidator, []const u8) anyerror!bool = null,
};
const RecordEntry = struct {
    selected: SelectedRecord,
    key: []const u8,
    ordinal_identity: bool,
    field_keys: []const []const []const u8,
    owner_window: ?usize,
};
fn appendNumber(a: Allocator, bytes: *std.ArrayListUnmanaged(u8), number: usize) !void {
    var encoded: [8]u8 = undefined;
    std.mem.writeInt(u64, &encoded, @intCast(number), .little);
    try bytes.appendSlice(a, &encoded);
}
fn appendText(a: Allocator, bytes: *std.ArrayListUnmanaged(u8), text: []const u8) !void {
    try appendNumber(a, bytes, text.len);
    try bytes.appendSlice(a, text);
}
fn fieldKey(a: Allocator, document: Plan, window: usize, value: pipeline.Value, semantic: bool, work: *Work) ![]const u8 {
    try work.admitText(value.text.len);
    if (!std.unicode.utf8ValidateSlice(value.text) or !validProbability(value.confidence)) return error.InvalidLongDocumentRecord;
    var bytes = std.ArrayListUnmanaged(u8).empty;
    if (value.source) |source| {
        const global = try document.rebaseSource(window, source, .utf8_bytes);
        if (!semantic) {
            try bytes.append(a, 1);
            try appendNumber(a, &bytes, global.byte_start);
            try appendNumber(a, &bytes, global.byte_end);
            return bytes.toOwnedSlice(a);
        }
    }
    const text = try relations.semanticText(a, value.text);
    try bytes.append(a, 0);
    try appendText(a, &bytes, text);
    return bytes.toOwnedSlice(a);
}
fn keyLess(_: void, left: []const u8, right: []const u8) bool {
    return std.mem.order(u8, left, right) == .lt;
}
fn makeRecordEntry(a: Allocator, document: Plan, schema: schema_mod.Structure, candidate: RecordCandidate, window: usize, options: RecordOptions, work: *Work) !RecordEntry {
    const record = candidate.record;
    if (record.fields.len != schema.fields.len or (record.confidence != null and !validProbability(record.confidence.?))) return error.InvalidLongDocumentRecord;
    const anchor = if (record.anchor) |source| try document.rebaseSource(window, source, options.output_unit) else null;
    if (schema.mode == .natural and anchor == null) return error.MissingLongDocumentNaturalAnchor;
    if (schema.mode != .natural and anchor != null) return error.InvalidLongDocumentRecord;
    var seed_source: ?pipeline.SourceSpan = null;
    var seed_field: ?usize = null;
    if (record.occurrence) |occurrence| {
        if (occurrence.seed_field) |field| if (field >= schema.fields.len) return error.InvalidLongDocumentRecord;
        if (occurrence.seed_source) |source| {
            if (occurrence.seed_field == null or schema.mode == .anchorless) return error.InvalidLongDocumentRecord;
            seed_source = try document.rebaseSource(window, source, .utf8_bytes);
            seed_field = occurrence.seed_field;
        }
    }
    const semantic = anchor == null and document.descriptor.other_record_identity == .semantic;
    const stable_seed = schema.mode == .latent and !semantic and seed_source != null;
    var key = std.ArrayListUnmanaged(u8).empty;
    try appendNumber(a, &key, candidate.structure_index);
    if (anchor) |source| {
        try key.append(a, 1);
        try appendNumber(a, &key, source.byte_start);
        try appendNumber(a, &key, source.byte_end);
    } else if (stable_seed) {
        try key.append(a, 2);
        try appendNumber(a, &key, seed_field.?);
        try appendNumber(a, &key, seed_source.?.byte_start);
        try appendNumber(a, &key, seed_source.?.byte_end);
    } else try key.append(a, if (semantic) 3 else 4);
    const field_keys = try a.alloc([]const []const u8, schema.fields.len);
    var score: f64 = 0;
    var value_count: usize = 0;
    var any_source = seed_source != null;
    var owner_window: ?usize = if (stable_seed) try document.ownerOfSpan(.{ .start = seed_source.?.byte_start, .end = seed_source.?.byte_end }) else null;
    if (anchor) |source| owner_window = try document.ownerOfSpan(.{ .start = source.byte_start, .end = source.byte_end });
    for (record.fields, schema.fields, field_keys, 0..) |field, spec, *keys, field_index| {
        try work.admit(field.values.len);
        const cardinality = spec.cardinality orelse if (schema.anchor == field_index) schema_mod.Cardinality.required_one else schema_mod.Cardinality.zero_or_more;
        const dtype: schema_mod.DType = if (schema.mode != null and (cardinality == .required_one or cardinality == .optional_one or spec.dtype == .str)) .str else spec.dtype;
        if (!std.mem.eql(u8, field.name, spec.name) or field.dtype != dtype or (dtype == .str and field.values.len > 1)) return error.InvalidLongDocumentRecord;
        switch (cardinality) {
            .required_one => if (field.values.len != 1) return error.LongDocumentRequiredRecordFieldMissing,
            .one_or_more => if (field.values.len == 0) return error.LongDocumentRequiredRecordFieldMissing,
            .optional_one => if (field.values.len > 1) return error.InvalidLongDocumentRecord,
            .zero_or_more => {},
        }
        const occurrences = try a.alloc([]const u8, field.values.len);
        const identities = try a.alloc([]const u8, field.values.len);
        for (field.values, occurrences, identities) |value, *occurrence, *identity_key| {
            try work.tick();
            occurrence.* = try fieldKey(a, document, window, value, false, work);
            identity_key.* = if (document.descriptor.other_record_identity == .semantic and anchor == null)
                try fieldKey(a, document, window, value, true, work)
            else
                occurrence.*;
            score += value.confidence;
            value_count += 1;
            if (value.source) |source| {
                any_source = true;
                if (owner_window == null) {
                    const global = try document.rebaseSource(window, source, .utf8_bytes);
                    owner_window = try document.ownerOfSpan(.{ .start = global.byte_start, .end = global.byte_end });
                }
            }
            for (spec.validators) |validator| {
                try work.tick();
                const validate = options.validate_value_fn orelse return error.MissingLongDocumentValidator;
                if (!try validate(options.regex_context, validator, value.text)) return error.InvalidLongDocumentRecord;
            }
        }
        std.mem.sort([]const u8, occurrences, {}, keyLess);
        keys.* = occurrences;
        std.mem.sort([]const u8, identities, {}, keyLess);
        if (anchor == null and !stable_seed) {
            try appendNumber(a, &key, field_index);
            try appendNumber(a, &key, identities.len);
            for (identities) |value| try appendText(a, &key, value);
        }
    }
    if (value_count == 0) return error.InvalidLongDocumentRecord;
    // A wholly derived record has no occurrence identity. Do not invent one
    // or silently collapse separate events; semantic dedup is explicit.
    if (document.windows.len > 1 and anchor == null and !any_source and document.descriptor.other_record_identity == .occurrence)
        return error.AmbiguousLongDocumentRecordOccurrence;
    return .{
        .selected = .{ .structure_index = candidate.structure_index, .origin = .{ .window_index = window, .record_index = candidate.record_index }, .anchor = anchor, .ranking_score = record.confidence orelse score / @as(f64, @floatFromInt(value_count)) },
        .key = try key.toOwnedSlice(a),
        .ordinal_identity = anchor == null and !semantic,
        .field_keys = field_keys,
        .owner_window = owner_window,
    };
}

fn sameExclusiveResources(schema: schema_mod.Structure, left: RecordEntry, right: RecordEntry, work: *Work) !bool {
    for (schema.fields, 0..) |field, f| if (field.exclusive) {
        const l = left.field_keys[f];
        const r = right.field_keys[f];
        var i: usize = 0;
        var j: usize = 0;
        while (i < l.len and j < r.len) {
            try work.tick();
            if (!std.mem.eql(u8, l[i], r[j])) return false;
            const li = i;
            const rj = j;
            while (i < l.len and std.mem.eql(u8, l[li], l[i])) : (i += 1) try work.tick();
            while (j < r.len and std.mem.eql(u8, r[rj], r[j])) : (j += 1) try work.tick();
        }
        if (i != l.len or j != r.len) return false;
    };
    return true;
}
fn preferredRecord(left: RecordEntry, right: RecordEntry) bool {
    if (left.selected.ranking_score != right.selected.ranking_score) return left.selected.ranking_score > right.selected.ranking_score;
    const left_owned = left.owner_window == left.selected.origin.window_index;
    const right_owned = right.owner_window == right.selected.origin.window_index;
    if (left_owned != right_owned) return left_owned;
    if (left.selected.origin.window_index != right.selected.origin.window_index) return left.selected.origin.window_index < right.selected.origin.window_index;
    return left.selected.origin.record_index < right.selected.origin.record_index;
}

pub fn mergeRecords(allocator: Allocator, document: Plan, compiled: *const schema_mod.CompiledSchema, windows: []const WindowRecords, options: RecordOptions) !Records {
    const work = try Work.init(options.merge);
    try validateSchema(document, compiled);
    if (options.solver.max_candidates == 0 or options.solver.max_candidates > 4096) return error.InvalidLongDocumentRecordSelectionLimits;
    const owner = try Owner.init(allocator, options.merge.max_memory_bytes);
    errdefer owner.deinit();
    return mergeRecordsOwned(owner, document, compiled, windows, options, work) catch |err| return owner.allocationError(err);
}

fn mergeRecordsOwned(owner: *Owner, document: Plan, compiled: *const schema_mod.CompiledSchema, windows: []const WindowRecords, options: RecordOptions, initial_work: Work) !Records {
    var work = initial_work;
    const a = owner.allocator();
    const order = try orderedWindows(a, document, windows, &work);
    var entries = std.ArrayListUnmanaged(RecordEntry).empty;
    for (order, 0..) |index, window_index| {
        const records = windows[index].records;
        var occurrence_counts = std.StringHashMapUnmanaged(usize).empty;
        try work.admit(records.len);
        for (records, 0..) |candidate, record_position| {
            try work.tick();
            if (candidate.structure_index >= compiled.schema.structures.len) return error.InvalidLongDocumentRecord;
            for (records[0..record_position]) |previous| {
                try work.tick();
                if (previous.structure_index == candidate.structure_index and previous.record_index == candidate.record_index) return error.DuplicateLongDocumentRecordIdentity;
            }
            const schema = compiled.schema.structures[candidate.structure_index];
            if (schema.mode == null) continue;
            var entry = try makeRecordEntry(a, document, schema, candidate, window_index, options, &work);
            if (entry.ordinal_identity) {
                const count = try occurrence_counts.getOrPut(a, entry.key);
                if (!count.found_existing) count.value_ptr.* = 0;
                var identified = std.ArrayListUnmanaged(u8).empty;
                try identified.appendSlice(a, entry.key);
                try appendNumber(a, &identified, count.value_ptr.*);
                count.value_ptr.* += 1;
                entry.key = try identified.toOwnedSlice(a);
            }
            var dominated: ?usize = null;
            for (entries.items, 0..) |previous, j| {
                try work.tick();
                // A lower-confidence alternative can still be necessary to
                // preserve another anchor. It is safely dominated only when
                // identity AND every exclusive resource footprint agree.
                if (std.mem.eql(u8, entry.key, previous.key) and try sameExclusiveResources(schema, entry, previous, &work)) {
                    dominated = j;
                    break;
                }
            }
            if (dominated) |j| {
                if (preferredRecord(entry, entries.items[j])) entries.items[j] = entry;
            } else {
                if (entries.items.len >= options.solver.max_candidates) return error.LongDocumentCandidateLimitExceeded;
                try entries.append(a, entry);
            }
        }
    }
    const EntryOrder = struct {
        fn less(_: void, left: RecordEntry, right: RecordEntry) bool {
            if (left.selected.structure_index != right.selected.structure_index) return left.selected.structure_index < right.selected.structure_index;
            if (left.selected.anchor != null and right.selected.anchor != null) {
                if (left.selected.anchor.?.byte_start != right.selected.anchor.?.byte_start) return left.selected.anchor.?.byte_start < right.selected.anchor.?.byte_start;
                if (left.selected.anchor.?.byte_end != right.selected.anchor.?.byte_end) return left.selected.anchor.?.byte_end < right.selected.anchor.?.byte_end;
            }
            const identity_order = std.mem.order(u8, left.key, right.key);
            if (identity_order != .eq) return identity_order == .lt;
            return preferredRecord(left, right);
        }
    };
    std.mem.sort(RecordEntry, entries.items, {}, EntryOrder.less);
    const candidates = try a.alloc(record_selection.Candidate, entries.items.len);
    var resource_ids = std.StringHashMapUnmanaged(usize).empty;
    var identity: usize = 0;
    for (entries.items, candidates, 0..) |entry, *candidate, i| {
        try work.tick();
        if (i > 0 and !std.mem.eql(u8, entry.key, entries.items[i - 1].key)) identity += 1;
        var resources = std.ArrayListUnmanaged(usize).empty;
        const schema = compiled.schema.structures[entry.selected.structure_index];
        for (schema.fields, 0..) |field, f| if (field.exclusive) {
            for (entry.field_keys[f], 0..) |key, k| {
                try work.tick();
                if (k > 0 and std.mem.eql(u8, key, entry.field_keys[f][k - 1])) continue;
                var resource = std.ArrayListUnmanaged(u8).empty;
                try appendNumber(a, &resource, entry.selected.structure_index);
                try appendNumber(a, &resource, f);
                try appendText(a, &resource, key);
                const found = try resource_ids.getOrPut(a, try resource.toOwnedSlice(a));
                if (!found.found_existing) found.value_ptr.* = resource_ids.count() - 1;
                try resources.append(a, found.value_ptr.*);
            }
        };
        candidate.* = .{ .identity = identity, .score = entry.selected.ranking_score, .resources = try resources.toOwnedSlice(a) };
    }
    var solver_options = options.solver;
    solver_options.control = options.merge.control;
    solver_options.max_work = @min(solver_options.max_work, options.merge.max_work - work.steps);
    if (solver_options.max_work == 0) return error.LongDocumentWorkLimitExceeded;
    // The solver's temporary arena uses the live budget directly and is
    // released before the selected source references are materialized.
    var solution = try record_selection.solve(owner.budget.allocator(), candidates, solver_options);
    defer solution.deinit();
    work.steps += solution.work_steps;
    if (solution.exhausted and !options.best_effort) return error.LongDocumentRecordSearchExhausted;
    if (solution.selected.len > options.merge.max_output_candidates) return error.LongDocumentCandidateLimitExceeded;
    const selected = try a.alloc(SelectedRecord, solution.selected.len);
    for (solution.selected, selected) |index, *out| out.* = entries.items[index].selected;
    const Less = struct {
        fn less(_: void, left: SelectedRecord, right: SelectedRecord) bool {
            if (left.structure_index != right.structure_index) return left.structure_index < right.structure_index;
            if (left.anchor != null and right.anchor != null) {
                if (left.anchor.?.byte_start != right.anchor.?.byte_start) return left.anchor.?.byte_start < right.anchor.?.byte_start;
                if (left.anchor.?.byte_end != right.anchor.?.byte_end) return left.anchor.?.byte_end < right.anchor.?.byte_end;
            }
            if (left.origin.window_index != right.origin.window_index) return left.origin.window_index < right.origin.window_index;
            return left.origin.record_index < right.origin.record_index;
        }
    };
    std.mem.sort(SelectedRecord, selected, {}, Less.less);
    try check(options.merge.control);
    return .{ .owner = owner, .selected = selected, .descriptor = document.descriptor, .diagnostics = .{ .status = if (solution.status == .optimal) .optimal else .feasible, .utility = solution.utility, .visited_nodes = solution.visited_nodes, .exhausted = solution.exhausted } };
}

pub const LegacyStructures = struct {
    owner: *Owner,
    structures: []const pipeline.Structure,
    descriptor: Descriptor,
    output_values: usize,
    output_string_bytes: usize,
    pub fn deinit(self: *LegacyStructures) void {
        self.owner.deinit();
        self.* = undefined;
    }
};
const LegacyCandidate = struct {
    value: pipeline.Value,
    window: usize,
    owner_window: ?usize,
    choice: ?usize,
};
const LegacyOutput = struct {
    allocator: Allocator,
    options: pipeline.Options,
    max_values: usize,
    values: usize = 0,
    bytes: usize = 0,
    fn text(self: *LegacyOutput, value: []const u8) ![]const u8 {
        if (value.len > self.options.max_output_string_bytes - self.bytes) return error.ExtractionOutputLimitExceeded;
        self.bytes += value.len;
        return self.allocator.dupe(u8, value);
    }
    fn copy(self: *LegacyOutput, value: pipeline.Value) !pipeline.Value {
        if (self.values >= self.max_values) return error.ExtractionOutputLimitExceeded;
        self.values += 1;
        var owned = value;
        owned.text = try self.text(value.text);
        owned.token_span = null;
        return owned;
    }
};
fn legacyPreferred(left: LegacyCandidate, right: LegacyCandidate) bool {
    if (left.value.confidence != right.value.confidence) return left.value.confidence > right.value.confidence;
    if (left.choice != null and right.choice != null and left.choice.? != right.choice.?) return left.choice.? < right.choice.?;
    const l = left.owner_window == left.window;
    const r = right.owner_window == right.window;
    if (l != r) return l;
    return left.window < right.window;
}
fn legacyControl(raw: ?*anyopaque) !void {
    const work: *Work = @ptrCast(@alignCast(raw.?));
    try work.tick();
}

/// Global assembly for the legacy (mode=null) single structure. Fields are
/// independent model queries, so their union may use different windows. This
/// differs deliberately from explicit record modes, which retain whole learned
/// instances. Inputs may also contain explicit modes; those belong to the
/// separate mergeRecords stage and are ignored here.
pub fn mergeLegacyStructures(allocator: Allocator, document: Plan, compiled: *const schema_mod.CompiledSchema, windows: []const WindowRecords, merge_options: MergeOptions, options: pipeline.Options) !LegacyStructures {
    const work = try Work.init(merge_options);
    try validateSchema(document, compiled);
    if (options.control) |control| try control.check();
    if (!validProbability(options.threshold) or options.max_output_values == 0 or options.max_output_string_bytes == 0) return error.InvalidBoundaryPipelineOptions;
    const owner = try Owner.init(allocator, merge_options.max_memory_bytes);
    errdefer owner.deinit();
    return mergeLegacyStructuresOwned(owner, document, compiled, windows, merge_options, options, work) catch |err| return owner.allocationError(err);
}

fn mergeLegacyStructuresOwned(owner: *Owner, document: Plan, compiled: *const schema_mod.CompiledSchema, windows: []const WindowRecords, merge_options: MergeOptions, options: pipeline.Options, initial_work: Work) !LegacyStructures {
    var work = initial_work;
    const a = owner.allocator();
    const order = try orderedWindows(a, document, windows, &work);
    const structures = compiled.schema.structures;
    const groups = try a.alloc([]std.ArrayListUnmanaged(LegacyCandidate), structures.len);
    for (groups, structures) |*group, structure| {
        group.* = try a.alloc(std.ArrayListUnmanaged(LegacyCandidate), if (structure.mode == null) structure.fields.len else 0);
        @memset(group.*, .empty);
    }
    const seen = try a.alloc(bool, structures.len);
    for (order, 0..) |index, window| {
        @memset(seen, false);
        try work.admit(windows[index].records.len);
        for (windows[index].records) |candidate| {
            try work.tick();
            if (candidate.structure_index >= structures.len) return error.InvalidLongDocumentRecord;
            const structure = structures[candidate.structure_index];
            if (structure.mode != null) continue;
            if (seen[candidate.structure_index] or candidate.record_index != 0) return error.DuplicateLongDocumentRecordIdentity;
            seen[candidate.structure_index] = true;
            if (candidate.record.anchor != null or candidate.record.confidence != null or candidate.record.fields.len != structure.fields.len) return error.InvalidLongDocumentRecord;
            for (candidate.record.fields, structure.fields, groups[candidate.structure_index]) |field, spec, *group| {
                try work.admit(field.values.len);
                if (!std.mem.eql(u8, field.name, spec.name) or field.dtype != spec.dtype) return error.InvalidLongDocumentRecord;
                for (field.values) |value| {
                    try work.admitText(value.text.len);
                    if (!validProbability(value.confidence) or !std.unicode.utf8ValidateSlice(value.text) or value.text.len == 0 or value.attributes.len != 0) return error.InvalidLongDocumentRecord;
                    var admitted = LegacyCandidate{ .value = value, .window = window, .owner_window = null, .choice = null };
                    admitted.value.token_span = null;
                    if (spec.choices.len > 0) {
                        for (spec.choices, 0..) |choice, c| if (std.mem.eql(u8, choice, value.text)) {
                            admitted.choice = c;
                            break;
                        };
                        if (admitted.choice == null or value.source != null or !value.derived) return error.InvalidLongDocumentRecord;
                    } else {
                        if (value.derived) return error.InvalidLongDocumentRecord;
                        const source = value.source orelse return error.InvalidLongDocumentRecord;
                        admitted.value.source = try document.rebaseSource(window, source, options.offset_unit);
                        const global = admitted.value.source.?;
                        if (!std.mem.eql(u8, document.text[global.byte_start..global.byte_end], value.text)) return error.InconsistentLongDocumentSourceSpan;
                        admitted.owner_window = try document.ownerOfSpan(.{ .start = global.byte_start, .end = global.byte_end });
                    }
                    var allowed = true;
                    for (spec.validators) |validator| {
                        try work.tick();
                        if (options.control) |control| try control.check();
                        const validate = options.validate_value_fn orelse return error.MissingLongDocumentValidator;
                        if (!try validate(options.regex_context, validator, value.text)) {
                            allowed = false;
                            break;
                        }
                    }
                    if (!allowed) continue;
                    var duplicate: ?usize = null;
                    for (group.items, 0..) |previous, i| {
                        try work.tick();
                        const same = if (admitted.choice) |choice| previous.choice == choice else previous.value.source.?.byte_start == admitted.value.source.?.byte_start and previous.value.source.?.byte_end == admitted.value.source.?.byte_end;
                        if (same) {
                            duplicate = i;
                            break;
                        }
                    }
                    if (duplicate) |i| {
                        if (legacyPreferred(admitted, group.items[i])) group.items[i] = admitted;
                    } else try group.append(a, admitted);
                }
            }
        }
    }
    var output = LegacyOutput{ .allocator = a, .options = options, .max_values = @min(options.max_output_values, merge_options.max_output_candidates) };
    var selected_structures = std.ArrayListUnmanaged(pipeline.Structure).empty;
    for (structures, groups) |structure, fields| {
        if (structure.mode != null) continue;
        const selected_fields = try a.alloc(pipeline.Field, structure.fields.len);
        var present = false;
        for (structure.fields, fields, selected_fields) |spec, candidates, *field| {
            try work.tick();
            const scalar = spec.dtype == .str or spec.cardinality == .required_one or spec.cardinality == .optional_one;
            var values = std.ArrayListUnmanaged(pipeline.Value).empty;
            if (spec.choices.len > 0) {
                const threshold: f32 = @floatCast(spec.threshold orelse options.threshold);
                if (scalar) {
                    var best: ?LegacyCandidate = null;
                    for (candidates.items) |candidate| if (candidate.value.confidence >= threshold and (best == null or legacyPreferred(candidate, best.?))) {
                        best = candidate;
                    };
                    if (best) |candidate| try values.append(a, try output.copy(candidate.value));
                } else {
                    // Enum lists retain declaration order, including distinct
                    // declared choices that happen to normalize alike.
                    for (spec.choices, 0..) |_, choice| for (candidates.items) |candidate| if (candidate.choice == choice and candidate.value.confidence >= threshold) {
                        try values.append(a, try output.copy(candidate.value));
                        break;
                    };
                }
            } else {
                const scored = try a.alloc(boundary.Candidate, candidates.items.len);
                for (candidates.items, scored, 0..) |candidate, *to, index| {
                    const source = candidate.value.source.?;
                    const coordinates = try document.offsets.convert(.{ .start = source.byte_start, .end = source.byte_end }, .unicode_codepoints);
                    to.* = .{ .start = coordinates.start, .end = coordinates.end, .probability = candidate.value.confidence, .source_index = index };
                }
                if (scored.len > options.query_limits.max_candidates) return error.ExtractionCandidateLimitExceeded;
                // Charge a conservative bound for the resolver's duplicate
                // and overlap comparisons before entering its bounded work.
                const comparisons = try std.math.mul(usize, try std.math.mul(usize, scored.len, scored.len), 2);
                if (comparisons > merge_options.max_work - work.steps) return error.LongDocumentWorkLimitExceeded;
                work.steps += comparisons;
                const admitted = try boundary.resolveOverlaps(owner.budget.allocator(), scored, options.overlap, .{ .max_candidates = options.query_limits.max_candidates, .control = .{ .ptr = &work, .check_fn = legacyControl } });
                defer owner.budget.allocator().free(admitted);
                for (admitted[0..if (scalar) @min(admitted.len, 1) else admitted.len]) |candidate|
                    try values.append(a, try output.copy(candidates.items[candidate.source_index].value));
            }
            if ((spec.cardinality == .required_one or spec.cardinality == .one_or_more) and values.items.len == 0) return error.LongDocumentRequiredRecordFieldMissing;
            field.* = .{ .name = spec.name, .dtype = spec.dtype, .values = try values.toOwnedSlice(a) };
            present = present or field.values.len > 0;
        }
        if (present) {
            for (selected_fields) |*field| field.name = try output.text(field.name);
            const record = try a.alloc(pipeline.Record, 1);
            record[0] = .{ .fields = selected_fields };
            try selected_structures.append(a, .{ .name = try output.text(structure.name), .instances = record });
        }
    }
    try check(merge_options.control);
    if (options.control) |control| try control.check();
    return .{ .owner = owner, .structures = try selected_structures.toOwnedSlice(a), .descriptor = document.descriptor, .output_values = output.values, .output_string_bytes = output.bytes };
}

fn testOptions() PlanOptions {
    return .{ .mode = .windowed, .max_window_body_words = 4, .overlap_words = 1, .inference_fingerprint = [_]u8{9} ** 32 };
}
fn testSource(start: usize, end: usize) pipeline.SourceSpan {
    return .{ .start = start, .end = end, .unit = .utf8_bytes, .byte_start = start, .byte_end = end };
}
fn testValue(text: []const u8, start: usize, end: usize, probability: f32) pipeline.Value {
    return .{ .text = text, .confidence = probability, .source = testSource(start, end), .token_span = null };
}

test "gliner boundary long document planner is explicit source exact and accounts for synthetic words" {
    const a = std.testing.allocator;
    const fingerprint = [_]u8{1} ** 32;
    try std.testing.expectError(error.LongDocumentWindowingRequired, plan(a, "A B C D E", fingerprint, .{ .max_window_body_words = 4, .overlap_words = 1 }));
    var document = try plan(a, "İ 😀 中 x y", fingerprint, testOptions());
    defer document.deinit();
    try std.testing.expectEqual(@as(usize, 2), document.windows.len);
    try std.testing.expectEqualStrings("İ 😀 中", try document.windowText(0));
    try std.testing.expectEqualStrings("中 x y", try document.windowText(1));
    try std.testing.expectEqual(@as(usize, 2), document.windows[0].weight());
    try std.testing.expectEqual(@as(usize, 3), document.windows[1].weight());
    const cp = try document.rebase(1, .{ .start = 0, .end = 1 }, .unicode_codepoints, .unicode_codepoints);
    try std.testing.expectEqual(@as(usize, 4), cp.start);
    try std.testing.expectEqual(@as(usize, 5), cp.end);
    try std.testing.expectEqual(@as(usize, 8), cp.byte_start);
    const utf16 = try document.rebase(0, .{ .start = 2, .end = 3 }, .unicode_codepoints, .utf16_codeunits);
    try std.testing.expectEqual(@as(usize, 2), utf16.start);
    try std.testing.expectEqual(@as(usize, 4), utf16.end);
    try std.testing.expectError(error.InvalidUtf16Boundary, document.rebase(0, .{ .start = 3, .end = 4 }, .utf16_codeunits, .utf8_bytes));
    try std.testing.expectError(error.InvalidLongDocumentWindowSpan, document.rebase(1, .{ .start = 0, .end = 8 }, .utf8_bytes, .utf8_bytes));
    var inconsistent = testSource(0, 3);
    inconsistent.byte_end = 2;
    try std.testing.expectError(error.InconsistentLongDocumentSourceSpan, document.rebaseSource(1, inconsistent, .utf8_bytes));
    try std.testing.expectEqual(@as(usize, 1), try document.ownerOfSpan(.{ .start = 8, .end = 11 }));
    var unbound = try plan(a, "https://x.test", fingerprint, .{ .max_window_body_words = 1, .overlap_words = 0 });
    defer unbound.deinit();
    try std.testing.expect(!unbound.windows[0].synthetic_terminal_word);
    try std.testing.expectError(error.UnboundLongDocumentInferenceProfile, unbound.identity(0));
    var wrong = try document.identity(0);
    wrong.inference_fingerprint[0] ^= 1;
    try std.testing.expectError(error.LongDocumentIdentityMismatch, document.validateIdentity(wrong));
    var whitespace = try plan(a, " \t ", fingerprint, .{ .max_window_body_words = 1, .overlap_words = 0 });
    defer whitespace.deinit();
    try std.testing.expectEqualStrings(" \t ", try whitespace.windowText(0));
    try std.testing.expect(whitespace.windows[0].synthetic_terminal_word);
    var limits = testOptions();
    limits.max_windows = 1;
    try std.testing.expectError(error.LongDocumentWindowLimitExceeded, plan(a, "A B C D E", fingerprint, limits));
    limits = testOptions();
    limits.max_window_scan_bytes = 9;
    try std.testing.expectError(error.LongDocumentWorkLimitExceeded, plan(a, "A B C D E", fingerprint, limits));
    limits = testOptions();
    limits.max_memory_bytes = 1;
    try std.testing.expectError(error.MemoryBudgetExceeded, plan(a, "A B C D E", fingerprint, limits));
}

test "gliner boundary long document all declared owner caps preserve backing OOM and recovery" {
    const a = std.testing.allocator;
    const fingerprint = [_]u8{7} ** 32;
    var capped = testOptions();
    capped.max_memory_bytes = 1;
    try std.testing.expectError(error.MemoryBudgetExceeded, plan(a, "A", fingerprint, capped));
    var failing_plan = std.testing.FailingAllocator.init(a, .{ .fail_index = 1 });
    try std.testing.expectError(error.OutOfMemory, plan(failing_plan.allocator(), "A", fingerprint, testOptions()));

    var compiled = try schema_mod.compile(a,
        \\{"entities":["person"],"classifications":[{"name":"a","labels":["x","y"]}],"structures":{"event":{"fields":{"value":"list"}}}}
    , .{});
    defer compiled.deinit();
    var compiled_joint = try schema_mod.compile(a,
        \\{"joint_ie":{"entities":{"person":{}},"relations":{"knows":{"head":["person"],"tail":["person"]}}}}
    , .{});
    defer compiled_joint.deinit();
    var document = try plan(a, "A", compiled.fingerprint, testOptions());
    defer document.deinit();
    var joint_document = try plan(a, "A", compiled_joint.fingerprint, testOptions());
    defer joint_document.deinit();
    const Case = struct {
        const Kind = enum { classification, joint_candidates, mentions, records, legacy_records };
        fn run(allocator: Allocator, kind: Kind, planned: Plan, schema: *const schema_mod.CompiledSchema, limit: usize) !void {
            const identity = try planned.identity(0);
            const options = MergeOptions{ .max_memory_bytes = limit };
            switch (kind) {
                .classification => {
                    const windows = [_]WindowLogits{.{ .identity = identity, .logits = &.{&.{ 0.4, 0.6 }} }};
                    var result = try aggregateClassificationLogits(allocator, planned, schema, &windows, options);
                    defer result.deinit();
                    try std.testing.expectEqual(@as(usize, 1), result.logits.len);
                    try std.testing.expectApproxEqAbs(@as(f64, 0.6), result.logits[0][1], 1e-12);
                },
                .joint_candidates => {
                    const windows = [_]WindowJointCandidates{.{ .identity = identity, .nodes = &.{.{ .entity_type = 0, .start = 0, .end = 1, .utility = 1, .probability = 0.8 }}, .edges = &.{} }};
                    var result = try mergeJointCandidates(allocator, planned, schema, &windows, options, 256, 256);
                    defer result.deinit();
                    try std.testing.expectEqual(@as(usize, 1), result.nodes.len);
                },
                .mentions => {
                    const windows = [_]WindowMentions{.{ .identity = identity, .pre_overlap_candidates = &.{.{ .entity_type = 0, .source = testSource(0, 1), .probability = 0.8 }} }};
                    var result = try mergeMentions(allocator, planned, schema, &windows, .flat, .utf8_bytes, options);
                    defer result.deinit();
                    try std.testing.expectEqual(@as(usize, 1), result.selected.len);
                },
                .records => {
                    const windows = [_]WindowRecords{.{ .identity = identity, .records = &.{} }};
                    var result = try mergeRecords(allocator, planned, schema, &windows, .{ .merge = options });
                    defer result.deinit();
                    try std.testing.expectEqual(@as(usize, 0), result.selected.len);
                },
                .legacy_records => {
                    const windows = [_]WindowRecords{.{ .identity = identity, .records = &.{} }};
                    var result = try mergeLegacyStructures(allocator, planned, schema, &windows, options, .{});
                    defer result.deinit();
                },
            }
        }
    };
    inline for (std.meta.tags(Case.Kind)) |kind| {
        const planned = if (kind == .joint_candidates) joint_document else document;
        const schema = if (kind == .joint_candidates) &compiled_joint else &compiled;
        try std.testing.expectError(error.MemoryBudgetExceeded, Case.run(a, kind, planned, schema, 1));
        // Allocate the Owner successfully, then fail its first arena backing
        // allocation. This must remain OOM rather than a declared-cap error.
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 1 });
        try std.testing.expectError(error.OutOfMemory, Case.run(failing.allocator(), kind, planned, schema, 64 * 1024 * 1024));
        try Case.run(a, kind, planned, schema, 64 * 1024 * 1024);
    }
}

test "gliner boundary long document classifications aggregate all logits and solve globally" {
    const a = std.testing.allocator;
    var compiled = try schema_mod.compile(a,
        \\{"classifications":[{"name":"a","labels":["x","y"],"min_labels":1,"max_labels":1},{"name":"b","labels":["n","p"],"min_labels":1,"max_labels":1}],"classification_constraints":[{"type":"Iff","left":{"type":"LabelRef","task":"a","label":"x"},"right":{"type":"LabelRef","task":"b","label":"n"}}]}
    , .{});
    defer compiled.deinit();
    var document = try plan(a, "A B C D E", compiled.fingerprint, testOptions());
    defer document.deinit();
    const windows = [_]WindowLogits{
        .{ .identity = try document.identity(1), .logits = &.{ &.{ 0, 6 }, &.{ 0, 2 } } },
        .{ .identity = try document.identity(0), .logits = &.{ &.{ 10, 0 }, &.{ 0, 8 } } },
    };
    var result = try solveClassifications(a, document, &compiled, &windows, .{ .solver = .{ .algorithm = .exact } });
    defer result.deinit();
    try std.testing.expectApproxEqAbs(@as(f64, 4), result.aggregated.logits[0][0], 1e-12);
    try std.testing.expectApproxEqAbs(@as(f64, 3.6), result.aggregated.logits[0][1], 1e-12);
    try std.testing.expectApproxEqAbs(@as(f64, 4.4), result.aggregated.logits[1][1], 1e-12);
    try std.testing.expectEqualSlices(constraints.Selection, &.{ 2, 2 }, result.result.selections);
    try std.testing.expectEqual(constraints.Status.optimal, result.result.status);
    try std.testing.expectError(error.IncompleteLongDocumentWindows, aggregateClassificationLogits(a, document, &compiled, windows[0..1], .{}));
    try std.testing.expectError(error.DuplicateLongDocumentWindow, aggregateClassificationLogits(a, document, &compiled, &.{ windows[0], windows[0] }, .{}));
    var wrong = windows;
    wrong[0].logits = &.{&.{0}};
    try std.testing.expectError(error.InvalidLongDocumentClassificationLogits, aggregateClassificationLogits(a, document, &compiled, &wrong, .{}));
    wrong = windows;
    wrong[0].logits = &.{ &.{ std.math.nan(f64), 6 }, &.{ 0, 2 } };
    try std.testing.expectError(error.NonFiniteBoundaryScore, aggregateClassificationLogits(a, document, &compiled, &wrong, .{}));
    try std.testing.expectError(error.LongDocumentCandidateLimitExceeded, aggregateClassificationLogits(a, document, &compiled, &windows, .{ .max_input_candidates = 3 }));
    try std.testing.expectError(error.LongDocumentWorkLimitExceeded, aggregateClassificationLogits(a, document, &compiled, &windows, .{ .max_work = 1 }));
}

test "gliner boundary long document best effort requires a valid global witness" {
    const a = std.testing.allocator;
    var compiled = try schema_mod.compile(a, "{\"classifications\":[{\"name\":\"a\",\"labels\":[\"x\",\"y\"],\"min_labels\":1,\"max_labels\":1}]}", .{});
    defer compiled.deinit();
    var document = try plan(a, "A B C D E", compiled.fingerprint, testOptions());
    defer document.deinit();
    const windows = [_]WindowLogits{ .{ .identity = try document.identity(0), .logits = &.{&.{ 1, 2 }} }, .{ .identity = try document.identity(1), .logits = &.{&.{ 1, 2 }} } };
    const solver = constraints.SolveOptions{ .algorithm = .exact, .exact_node_budget = 1 };
    try std.testing.expectError(error.LongDocumentClassificationSearchExhausted, solveClassifications(a, document, &compiled, &windows, .{ .solver = solver }));
    var best = try solveClassifications(a, document, &compiled, &windows, .{ .solver = solver, .best_effort = true });
    defer best.deinit();
    try std.testing.expect(best.result.valid() and best.result.exhausted);
    try std.testing.expectError(error.LongDocumentClassificationSearchExhausted, solveClassifications(a, document, &compiled, &windows, .{ .solver = .{ .algorithm = .exact, .exact_node_budget = 0 }, .best_effort = true }));
}

test "gliner boundary long document JointIE deduplicates endpoints then rejects cross window cycles globally" {
    const a = std.testing.allocator;
    var compiled = try schema_mod.compile(a,
        \\{"joint_ie":{"entities":{"person":{}},"relations":{"knows":{"head":["person"],"tail":["person"]}},"constraints":[{"type":"AcyclicRelation","relation":"knows"}]}}
    , .{});
    defer compiled.deinit();
    var options = testOptions();
    options.max_window_body_words = 5;
    options.overlap_words = 3;
    var document = try plan(a, "A B C D E", compiled.fingerprint, options);
    defer document.deinit();
    const left = [_]joint.Node{ .{ .entity_type = 0, .start = 2, .end = 3, .utility = -1, .probability = 0.2 }, .{ .entity_type = 0, .start = 4, .end = 5, .utility = -1, .probability = 0.2 } };
    const right = [_]joint.Node{ .{ .entity_type = 0, .start = 0, .end = 1, .utility = -1, .probability = 0.2 }, .{ .entity_type = 0, .start = 2, .end = 3, .utility = -1, .probability = 0.2 } };
    var edge0 = [_]joint.Edge{.{ .relation_type = 0, .head = 0, .tail = 1, .utility = 3, .probability = 0.9, .slot = 7, .hypothesis = 2, .count_alternative = 3 }};
    var edge1 = [_]joint.Edge{.{ .relation_type = 0, .head = 1, .tail = 0, .utility = 2.5, .probability = 0.8, .slot = 7, .hypothesis = 2, .count_alternative = 3 }};
    const windows = [_]WindowJointCandidates{ .{ .identity = try document.identity(1), .nodes = &right, .edges = &edge1 }, .{ .identity = try document.identity(0), .nodes = &left, .edges = &edge0 } };
    var result = try solveJoint(a, document, &compiled, &windows, .{ .solver = .{ .algorithm = .exact } });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.candidates.nodes.len);
    try std.testing.expectEqual(@as(usize, 2), result.candidates.edges.len);
    try std.testing.expect(result.candidates.edges[0].slot != result.candidates.edges[1].slot);
    try std.testing.expectEqual(@as(usize, 1), result.result.edges.len);
    try std.testing.expectEqual(@as(f64, 1), result.result.utility);
    try std.testing.expect(try joint.validateGlobal(a, compiled.schema.joint_ie.?, result.result.nodes, result.result.edges, .{}));
    edge0[0].required = true;
    edge1[0].required = true;
    try std.testing.expectError(error.LongDocumentJointInfeasible, solveJoint(a, document, &compiled, &windows, .{}));
    edge0[0].required = false;
    edge1[0].required = false;
    try std.testing.expectError(error.LongDocumentCandidateLimitExceeded, mergeJointCandidates(a, document, &compiled, &windows, .{}, 1, 256));
    try std.testing.expectError(error.LongDocumentJointSearchExhausted, solveJoint(a, document, &compiled, &windows, .{ .solver = .{ .algorithm = .exact, .exact_node_budget = 0 } }));
    var best = try solveJoint(a, document, &compiled, &windows, .{ .solver = .{ .algorithm = .exact, .exact_node_budget = 0 }, .best_effort = true });
    defer best.deinit();
    try std.testing.expect(best.result.valid() and best.result.exhausted);
}

test "gliner boundary long document entity overlap is global per type with whole attribute origins" {
    const a = std.testing.allocator;
    var compiled = try schema_mod.compile(a, "{\"entities\":[\"person\",\"name\"]}", .{});
    defer compiled.deinit();
    var options = testOptions();
    options.max_window_body_words = 5;
    options.overlap_words = 3;
    var document = try plan(a, "A B C D E", compiled.fingerprint, options);
    defer document.deinit();
    const windows = [_]WindowMentions{
        .{ .identity = try document.identity(0), .pre_overlap_candidates = &.{ .{ .entity_type = 0, .source = testSource(0, 3), .probability = 0.7 }, .{ .entity_type = 0, .source = testSource(4, 5), .probability = 0.4 } } },
        .{ .identity = try document.identity(1), .pre_overlap_candidates = &.{ .{ .entity_type = 0, .source = testSource(0, 3), .probability = 0.9 }, .{ .entity_type = 0, .source = testSource(2, 3), .probability = 0.4 }, .{ .entity_type = 0, .source = testSource(4, 5), .probability = 0.6 }, .{ .entity_type = 1, .source = testSource(0, 3), .probability = 0.9 } } },
    };
    var result = try mergeMentions(a, document, &compiled, &windows, .flat, .utf8_bytes, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 4), result.selected.len);
    try std.testing.expectEqual(@as(usize, 0), result.selected[0].source.byte_start);
    try std.testing.expectEqual(@as(usize, 4), result.selected[1].source.byte_start);
    try std.testing.expectEqual(@as(usize, 1), result.selected[1].origin.window_index);
    try std.testing.expectEqual(@as(usize, 6), result.selected[2].source.byte_start);
    try std.testing.expectEqual(@as(usize, 1), result.selected[3].entity_type);
}

test "gliner boundary long document records preserve whole alternatives and solve global exclusivity" {
    const a = std.testing.allocator;
    var compiled = try schema_mod.compile(a,
        \\{"structures":{"event":{"mode":"natural","anchor":"name","fields":{"name":{"type":"str","cardinality":"required_one"},"value":{"type":"str","cardinality":"required_one"}}}}}
    , .{});
    defer compiled.deinit();
    var options = testOptions();
    options.max_window_body_words = 5;
    options.overlap_words = 3;
    var document = try plan(a, "A X B X C", compiled.fingerprint, options);
    defer document.deinit();
    const left = [_]RecordCandidate{.{ .structure_index = 0, .record_index = 0, .record = .{ .confidence = 0.8, .anchor = testSource(4, 5), .fields = &.{ .{ .name = "name", .dtype = .str, .values = &.{testValue("B", 4, 5, 0.8)} }, .{ .name = "value", .dtype = .str, .values = &.{testValue("X", 2, 3, 0.9)} } } } }};
    const right = [_]RecordCandidate{
        .{ .structure_index = 0, .record_index = 0, .record = .{ .confidence = 0.9, .anchor = testSource(2, 3), .fields = &.{ .{ .name = "name", .dtype = .str, .values = &.{testValue("B", 2, 3, 0.9)} }, .{ .name = "value", .dtype = .str, .values = &.{testValue("X", 4, 5, 0.1)} } } } },
        .{ .structure_index = 0, .record_index = 1, .record = .{ .confidence = 0.85, .anchor = testSource(6, 7), .fields = &.{ .{ .name = "name", .dtype = .str, .values = &.{testValue("C", 6, 7, 0.9)} }, .{ .name = "value", .dtype = .str, .values = &.{testValue("X", 4, 5, 0.9)} } } } },
    };
    const windows = [_]WindowRecords{ .{ .identity = try document.identity(0), .records = &left }, .{ .identity = try document.identity(1), .records = &right } };
    var result = try mergeRecords(a, document, &compiled, &windows, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.selected.len);
    try std.testing.expectEqual(@as(usize, 4), result.selected[0].anchor.?.byte_start);
    try std.testing.expectEqual(@as(usize, 1), result.selected[0].origin.window_index);
    try std.testing.expectEqual(@as(usize, 0), result.selected[0].origin.record_index);
    try std.testing.expectEqual(@as(usize, 8), result.selected[1].anchor.?.byte_start);
    var exclusive = try schema_mod.compile(a,
        \\{"structures":{"event":{"mode":"natural","anchor":"name","fields":{"name":{"type":"str","cardinality":"required_one"},"value":{"type":"str","cardinality":"required_one","exclusive":true}}}}}
    , .{});
    defer exclusive.deinit();
    var exclusive_plan = try plan(a, "A X B X C", exclusive.fingerprint, options);
    defer exclusive_plan.deinit();
    const exclusive_windows = [_]WindowRecords{ .{ .identity = try exclusive_plan.identity(0), .records = &left }, .{ .identity = try exclusive_plan.identity(1), .records = &right } };
    var reconciled = try mergeRecords(a, exclusive_plan, &exclusive, &exclusive_windows, .{});
    defer reconciled.deinit();
    try std.testing.expectEqual(pipeline.SolverStatus.optimal, reconciled.diagnostics.status);
    try std.testing.expectEqual(@as(usize, 2), reconciled.selected.len);
    try std.testing.expectEqual(@as(usize, 0), reconciled.selected[0].origin.window_index);
    try std.testing.expectEqual(@as(usize, 1), reconciled.selected[1].origin.window_index);
    try std.testing.expectEqual(@as(usize, 1), reconciled.selected[1].origin.record_index);
    try std.testing.expectApproxEqAbs(@as(f64, 1.65), reconciled.diagnostics.utility, 0.000001);
    try std.testing.expectError(error.LongDocumentRecordSearchExhausted, mergeRecords(a, exclusive_plan, &exclusive, &exclusive_windows, .{ .solver = .{ .algorithm = .exact, .exact_node_budget = 0 } }));
    var partial = try mergeRecords(a, exclusive_plan, &exclusive, &exclusive_windows, .{ .solver = .{ .algorithm = .exact, .exact_node_budget = 0 }, .best_effort = true });
    defer partial.deinit();
    try std.testing.expect(partial.diagnostics.exhausted);
    try std.testing.expectEqual(pipeline.SolverStatus.feasible, partial.diagnostics.status);
    try std.testing.expectEqual(@as(usize, 1), partial.selected.len);
    const Check = struct {
        fn run(allocator: Allocator, planned: Plan, schema: *const schema_mod.CompiledSchema, packets: []const WindowRecords) !void {
            var selected = try mergeRecords(allocator, planned, schema, packets, .{});
            defer selected.deinit();
            try std.testing.expectEqual(@as(usize, 2), selected.selected.len);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Check.run, .{ exclusive_plan, &exclusive, &exclusive_windows });
    var missing = right;
    missing[0].record.fields = &.{ .{ .name = "name", .dtype = .str, .values = &.{testValue("B", 2, 3, 0.9)} }, .{ .name = "value", .dtype = .str, .values = &.{} } };
    try std.testing.expectError(error.LongDocumentRequiredRecordFieldMissing, mergeRecords(a, document, &compiled, &.{ windows[0], .{ .identity = try document.identity(1), .records = &missing } }, .{}));
}

test "gliner boundary long document latent semantic identity is explicit and retains value multiplicity" {
    const a = std.testing.allocator;
    var compiled = try schema_mod.compile(a, "{\"structures\":{\"event\":{\"mode\":\"latent\",\"fields\":{\"value\":\"list\"}}}}", .{});
    defer compiled.deinit();
    var options = testOptions();
    options.max_window_body_words = 5;
    options.overlap_words = 3;
    var occurrence = try plan(a, "A X B X C", compiled.fingerprint, options);
    defer occurrence.deinit();
    options.other_record_identity = .semantic;
    var semantic = try plan(a, "A X B X C", compiled.fingerprint, options);
    defer semantic.deinit();
    const left = [_]RecordCandidate{.{ .structure_index = 0, .record_index = 0, .record = .{ .fields = &.{.{ .name = "value", .dtype = .list, .values = &.{testValue("X", 2, 3, 0.7)} }} } }};
    const right = [_]RecordCandidate{.{ .structure_index = 0, .record_index = 0, .record = .{ .fields = &.{.{ .name = "value", .dtype = .list, .values = &.{testValue("X", 4, 5, 0.8)} }} } }};
    var windows = [_]WindowRecords{ .{ .identity = try occurrence.identity(0), .records = &left }, .{ .identity = try occurrence.identity(1), .records = &right } };
    var exact = try mergeRecords(a, occurrence, &compiled, &windows, .{});
    defer exact.deinit();
    try std.testing.expectEqual(@as(usize, 2), exact.selected.len);
    windows[0].identity = try semantic.identity(0);
    windows[1].identity = try semantic.identity(1);
    var collapsed = try mergeRecords(a, semantic, &compiled, &windows, .{});
    defer collapsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), collapsed.selected.len);
    try std.testing.expectEqual(@as(usize, 1), collapsed.selected[0].origin.window_index);
    var repeated = right;
    repeated[0].record.fields = &.{.{ .name = "value", .dtype = .list, .values = &.{ testValue("X", 4, 5, 0.8), testValue("X", 4, 5, 0.8) } }};
    windows[1].records = &repeated;
    var multiplicity = try mergeRecords(a, semantic, &compiled, &windows, .{});
    defer multiplicity.deinit();
    try std.testing.expectEqual(@as(usize, 2), multiplicity.selected.len);
    var derived_value = testValue("X", 0, 1, 0.8);
    derived_value.source = null;
    derived_value.derived = true;
    repeated[0].record.fields = &.{.{ .name = "value", .dtype = .list, .values = &.{derived_value} }};
    windows[0].identity = try occurrence.identity(0);
    windows[1].identity = try occurrence.identity(1);
    try std.testing.expectError(error.AmbiguousLongDocumentRecordOccurrence, mergeRecords(a, occurrence, &compiled, &windows, .{}));
}

test "gliner boundary long document allocation failures cancellation and recovery are atomic" {
    const Check = struct {
        fn run(a: Allocator) !void {
            var compiled = try schema_mod.compile(a, "{\"classifications\":[{\"name\":\"a\",\"labels\":[\"x\",\"y\"],\"min_labels\":1,\"max_labels\":1}]}", .{});
            defer compiled.deinit();
            var document = try plan(a, "A B C D E", compiled.fingerprint, testOptions());
            defer document.deinit();
            const windows = [_]WindowLogits{ .{ .identity = try document.identity(0), .logits = &.{&.{ 1, 2 }} }, .{ .identity = try document.identity(1), .logits = &.{&.{ 1, 2 }} } };
            var result = try solveClassifications(a, document, &compiled, &windows, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(constraints.Selection, 2), result.result.selections[0]);
        }
        fn cancel(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
    var options = testOptions();
    options.control = .{ .check_fn = Check.cancel };
    try std.testing.expectError(error.Cancelled, plan(std.testing.allocator, "A B C D E", [_]u8{0} ** 32, options));
    try Check.run(std.testing.allocator);
}

test "gliner boundary long document legacy fields merge globally with Unicode and required enums" {
    const a = std.testing.allocator;
    const regex = @import("extraction_regex.zig");
    var validator = regex.Context.init(a, .{});
    defer validator.deinit();
    var compiled = try schema_mod.compile(a,
        \\{"structures":{"summary":{"fields":{"left":{"type":"str","cardinality":"required_one"},"right":{"type":"str","cardinality":"required_one"},"tags":"list","status":{"type":"str","cardinality":"required_one","choices":["bad","good"],"validators":[{"type":"regex","pattern":"^good$","flags":0}]}}}}}
    , validator.compilerOptions(.{}));
    defer compiled.deinit();
    var planning = testOptions();
    planning.max_window_body_words = 5;
    planning.overlap_words = 3;
    var document = try plan(a, "Α X 😀 Y Z", compiled.fingerprint, planning);
    defer document.deinit();
    try std.testing.expectEqual(@as(usize, 3), document.windows[1].bytes.start);
    const left = [_]RecordCandidate{.{ .structure_index = 0, .record_index = 0, .record = .{ .fields = &.{
        .{ .name = "left", .dtype = .str, .values = &.{testValue("Α", 0, 2, 0.7)} },
        .{ .name = "right", .dtype = .str, .values = &.{} },
        .{ .name = "tags", .dtype = .list, .values = &.{testValue("😀", 5, 9, 0.6)} },
        .{ .name = "status", .dtype = .str, .values = &.{ .{ .text = "bad", .confidence = 0.99, .source = null, .token_span = null, .derived = true }, .{ .text = "good", .confidence = 0.4, .source = null, .token_span = null, .derived = true } } },
    } } }};
    const right = [_]RecordCandidate{.{ .structure_index = 0, .record_index = 0, .record = .{ .fields = &.{
        .{ .name = "left", .dtype = .str, .values = &.{} },
        .{ .name = "right", .dtype = .str, .values = &.{testValue("Z", 9, 10, 0.8)} },
        .{ .name = "tags", .dtype = .list, .values = &.{ testValue("😀", 2, 6, 0.8), testValue("Y", 7, 8, 0.7) } },
        .{ .name = "status", .dtype = .str, .values = &.{ .{ .text = "bad", .confidence = 0.9, .source = null, .token_span = null, .derived = true }, .{ .text = "good", .confidence = 0.8, .source = null, .token_span = null, .derived = true } } },
    } } }};
    const windows = [_]WindowRecords{ .{ .identity = try document.identity(0), .records = &left }, .{ .identity = try document.identity(1), .records = &right } };
    const options = pipeline.Options{ .offset_unit = .utf16_codeunits, .regex_context = &validator, .validate_value_fn = regex.Context.validateValue };
    var merged = try mergeLegacyStructures(a, document, &compiled, &windows, .{}, options);
    defer merged.deinit();
    try std.testing.expectEqual(@as(usize, 1), merged.structures.len);
    const fields = merged.structures[0].instances[0].fields;
    try std.testing.expectEqual(@as(usize, 1), merged.structures[0].instances.len);
    try std.testing.expectEqualStrings("Α", fields[0].values[0].text);
    try std.testing.expectEqualStrings("Z", fields[1].values[0].text);
    try std.testing.expectEqual(@as(usize, 9), fields[1].values[0].source.?.start);
    try std.testing.expectEqual(@as(usize, 2), fields[2].values.len);
    try std.testing.expectEqual(@as(usize, 4), fields[2].values[0].source.?.start);
    try std.testing.expectEqual(@as(usize, 6), fields[2].values[0].source.?.end);
    try std.testing.expectEqual(@as(f32, 0.8), fields[2].values[0].confidence);
    try std.testing.expectEqualStrings("good", fields[3].values[0].text);
    try std.testing.expect(fields[3].values[0].derived and fields[3].values[0].source == null);
    try std.testing.expectEqual(@as(usize, 5), merged.output_values);
    var whole = try mergeRecords(a, document, &compiled, &windows, .{});
    defer whole.deinit();
    try std.testing.expectEqual(@as(usize, 0), whole.selected.len);
    var only_bad = right;
    only_bad[0].record.fields = &.{ right[0].record.fields[0], right[0].record.fields[1], right[0].record.fields[2], .{ .name = "status", .dtype = .str, .values = right[0].record.fields[3].values[0..1] } };
    try std.testing.expectError(error.LongDocumentRequiredRecordFieldMissing, mergeLegacyStructures(a, document, &compiled, &.{ windows[0], .{ .identity = try document.identity(1), .records = &only_bad } }, .{}, options));
    const Check = struct {
        fn run(allocator: Allocator, planned: Plan, schema: *const schema_mod.CompiledSchema, packets: []const WindowRecords, settings: pipeline.Options) !void {
            var result = try mergeLegacyStructures(allocator, planned, schema, packets, .{}, settings);
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 5), result.output_values);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Check.run, .{ document, &compiled, &windows, options });
}

test "gliner boundary long document occurrence identity preserves local multiplicity and latent source seeds" {
    const a = std.testing.allocator;
    var anchorless = try schema_mod.compile(a, "{\"structures\":{\"event\":{\"mode\":\"anchorless\",\"fields\":{\"value\":\"list\"}}}}", .{});
    defer anchorless.deinit();
    var options = testOptions();
    options.max_window_body_words = 5;
    options.overlap_words = 3;
    var document = try plan(a, "A X B X C", anchorless.fingerprint, options);
    defer document.deinit();
    var left = [_]RecordCandidate{
        .{ .structure_index = 0, .record_index = 0, .record = .{ .confidence = 0.6, .fields = &.{.{ .name = "value", .dtype = .list, .values = &.{testValue("X", 2, 3, 0.7)} }} } },
        .{ .structure_index = 0, .record_index = 1, .record = .{ .confidence = 0.8, .fields = &.{.{ .name = "value", .dtype = .list, .values = &.{testValue("X", 2, 3, 0.7)} }} } },
    };
    var right = [_]RecordCandidate{.{ .structure_index = 0, .record_index = 0, .record = .{ .confidence = 0.9, .fields = &.{.{ .name = "value", .dtype = .list, .values = &.{testValue("X", 0, 1, 0.7)} }} } }};
    var windows = [_]WindowRecords{ .{ .identity = try document.identity(0), .records = &left }, .{ .identity = try document.identity(1), .records = &right } };
    var result = try mergeRecords(a, document, &anchorless, &windows, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.selected.len);
    try std.testing.expectEqual(@as(usize, 1), result.selected[0].origin.record_index);
    try std.testing.expectEqual(@as(usize, 1), result.selected[1].origin.window_index);
    var latent = try schema_mod.compile(a, "{\"structures\":{\"event\":{\"mode\":\"latent\",\"fields\":{\"value\":\"list\"}}}}", .{});
    defer latent.deinit();
    var latent_plan = try plan(a, "A X B X C", latent.fingerprint, options);
    defer latent_plan.deinit();
    left[0].record.occurrence = .{ .source_instance = 0, .seed_field = 0, .seed_source = testSource(0, 1) };
    left[1].record.occurrence = .{ .source_instance = 1, .seed_field = 0, .seed_source = testSource(2, 3) };
    right[0].record.occurrence = .{ .source_instance = 5, .seed_field = 0, .seed_source = testSource(0, 1) };
    windows[0].identity = try latent_plan.identity(0);
    windows[1].identity = try latent_plan.identity(1);
    var seeded = try mergeRecords(a, latent_plan, &latent, &windows, .{});
    defer seeded.deinit();
    try std.testing.expectEqual(@as(usize, 2), seeded.selected.len);
    try std.testing.expectEqual(@as(usize, 0), seeded.selected[0].origin.record_index);
    try std.testing.expectEqual(@as(usize, 1), seeded.selected[1].origin.window_index);
    for (seeded.selected) |record| try std.testing.expect(record.anchor == null);
}

// Uses the same heap-stable owner and arena as planning and task merges. The
// allocator, rather than this test, produces the earlier resize denial.
test "gliner boundary long document terminal allocation preserves backing failure after speculative resize" {
    const owner = try Owner.init(std.testing.allocator, 2048);
    var fail = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
    owner.budget.backing = fail.allocator();
    defer owner.deinit();
    _ = try owner.allocator().alloc(u8, 8);
    const actual: anyerror = if (owner.allocator().alloc(u8, 1024)) |_| return error.ExpectedBackingAllocationFailure else |err| err;
    try std.testing.expect(owner.budget.denied);
    const observed = owner.terminal orelse return error.MissingTerminalAllocationFailure;
    try std.testing.expectEqual(.backing_allocator, observed.kind);
    try std.testing.expect(observed.requested_bytes <= observed.limit_bytes - observed.live_bytes);
    try std.testing.expectEqual(error.OutOfMemory, owner.allocationError(actual));
    try std.testing.expectEqual(error.Cancelled, owner.allocationError(error.Cancelled));
}

test "gliner boundary long document terminal allocation retains declared memory limit" {
    const owner = try Owner.init(std.testing.allocator, 64);
    defer owner.deinit();
    try std.testing.expectEqual(error.OutOfMemory, owner.allocationError(error.OutOfMemory));
    const actual: anyerror = if (owner.allocator().alloc(u8, 1024)) |_| return error.ExpectedDeclaredAllocationFailure else |err| err;
    const observed = owner.terminal orelse return error.MissingTerminalAllocationFailure;
    try std.testing.expectEqual(.declared_limit, observed.kind);
    try std.testing.expectEqual(error.MemoryBudgetExceeded, owner.allocationError(actual));
}
