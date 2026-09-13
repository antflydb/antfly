// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Detached record identities, candidate bindings, and Hungarian assignments.
//! Only assignment membership is detached. Matched field/object losses must be
//! recomputed from live logits, using these exact immutable target bindings.
const std = @import("std");
const Allocator = std.mem.Allocator;
const schema = @import("../pipelines/extraction_schema.zig");
const assignment = @import("../pipelines/extraction_assignment.zig");
const targets = @import("gliner_boundary_targets.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
pub const Span = targets.Span;
pub const Field = struct { query: usize, cardinality: schema.Cardinality };
pub const Layout = struct {
    structure: usize,
    group: usize,
    mode: schema.RecordMode,
    word_count: usize,
    fields: []const Field,
    candidate_spans: []const Span,
    candidate_valid: []const bool,
    /// [F,C], in the same order as assignment columns 1..C.
    field_membership: []const bool,
    instance_mask: []const bool,
    anchor_query: ?usize = null,
    /// For natural records, each active instance explicitly identifies its
    /// anchor candidate column. Other modes must leave this empty.
    anchor_candidates: []const ?usize = &.{},
};
pub const Options = struct {
    max_fields: usize = 1024,
    max_candidates: usize = 65536,
    max_instances: usize = 1024,
    max_records: usize = 256,
    max_words: usize = 16384,
    max_record_id_bytes: usize = 256,
    max_elements: usize = 16 * 1024 * 1024,
    max_work: usize = 100 * 1024 * 1024,
    assignment_limits: assignment.Limits = .{},
    control: ?Control = null,
};
pub const TargetMap = struct {
    arena: std.heap.ArenaAllocator,
    fingerprint: [32]u8,
    mode: schema.RecordMode,
    fields: []const Field,
    candidate_spans: []const Span,
    candidate_valid: []const bool,
    field_membership: []const bool,
    instance_mask: []const bool,
    anchor_field: ?usize,
    anchor_candidates: []const ?usize,
    record_ids: []const []const u8,
    /// Maps local gold columns back to the canonical sample.records ordering.
    record_indices: []const usize,
    /// [G,F,C]. Every distinct annotated value has at least one represented
    /// occurrence; alternative occurrences remain alternative positives.
    gold_indicator: []const bool,
    work: usize,
    pub fn deinit(self: *TargetMap) void {
        self.arena.deinit();
        self.* = undefined;
    }
};
pub const Logits = struct {
    objects: []const f32,
    /// [I,F,1+C], column 0 ABSENT. Invalid/member-excluded candidate columns
    /// are replaced by the pinned -1e4 constant before forming costs.
    assignments: []const f32,
};
pub const Costs = struct {
    allocator: Allocator,
    target_fingerprint: [32]u8,
    instances: usize,
    records: usize,
    /// [I,G] f64 Hungarian input. Runtime graph costs are widened exactly by
    /// fromScoredCosts; buildCosts is an independent high-precision reference.
    values: []const f64,
    work: usize,
    pub fn deinit(self: *Costs) void {
        self.allocator.free(self.values);
        self.* = undefined;
    }
};
pub const Pair = struct { instance: usize, record: usize, annotation: usize };
pub const Matches = struct {
    arena: std.heap.ArenaAllocator,
    target_fingerprint: [32]u8,
    pairs: []const Pair,
    object_targets: []const f32,
    /// Natural records have no learned object/no-object objective. Inactive
    /// hypotheses never receive object supervision or a gold assignment.
    object_mask: []const bool,
    total_cost: f64,
    work: usize,
    pub fn deinit(self: *Matches) void {
        self.arena.deinit();
        self.* = undefined;
    }
};
const Work = struct {
    options: Options,
    count: usize = 0,
    fn check(self: *const Work) !void {
        if (self.options.control) |control| try control.check();
    }
    fn tick(self: *Work) !void {
        if (self.count >= self.options.max_work) return error.BoundaryTrainingMatchingLimitExceeded;
        self.count += 1;
        if (self.count % 128 == 1) try self.check();
    }
};
fn elements(a: usize, b: usize, limit: usize) !usize {
    const n = std.math.mul(usize, a, b) catch return error.BoundaryTrainingMatchingLimitExceeded;
    if (n > limit) return error.BoundaryTrainingMatchingLimitExceeded;
    return n;
}
fn scalar(cardinality: schema.Cardinality) bool {
    return cardinality == .required_one or cardinality == .optional_one;
}
fn required(cardinality: schema.Cardinality) bool {
    return cardinality == .required_one or cardinality == .one_or_more;
}
fn hashInt(hash: *std.crypto.hash.sha2.Sha256, value: usize) void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, @intCast(value), .little);
    hash.update(&bytes);
}
fn hashFlag(hash: *std.crypto.hash.sha2.Sha256, value: bool) void {
    hash.update(&.{@intFromBool(value)});
}

/// Compiles one declared record group from a canonical sample's record list.
/// All inputs can be freed after return; every target binding is owned.
pub fn compile(a: Allocator, layout: Layout, records: []const targets.RecordTarget, options: Options) !TargetMap {
    var work = Work{ .options = options };
    try work.check();
    const nf = layout.fields.len;
    const nc = layout.candidate_spans.len;
    const ni = layout.instance_mask.len;
    if (nf == 0 or layout.candidate_valid.len != nc or layout.field_membership.len != try elements(nf, nc, options.max_elements)) return error.InvalidBoundaryTrainingRecordShape;
    if (nf > options.max_fields or nc > options.max_candidates or ni > options.max_instances or layout.word_count > options.max_words or
        records.len > options.max_elements or options.max_work == 0) return error.BoundaryTrainingMatchingLimitExceeded;
    _ = try elements(try elements(ni, nf, options.max_elements), std.math.add(usize, nc, 1) catch return error.BoundaryTrainingMatchingLimitExceeded, options.max_elements);
    var anchor_field: ?usize = null;
    for (layout.fields, 0..) |field, f| {
        try work.tick();
        for (layout.fields[0..f]) |previous| {
            try work.tick();
            if (previous.query == field.query) return error.InvalidBoundaryTrainingRecordLayout;
        }
        if (layout.anchor_query == field.query) anchor_field = f;
    }
    if (layout.mode == .natural) {
        if (anchor_field == null or layout.anchor_candidates.len != ni or !scalar(layout.fields[anchor_field.?].cardinality)) return error.InvalidBoundaryTrainingRecordLayout;
    } else if (layout.anchor_query != null or layout.anchor_candidates.len > 0) return error.InvalidBoundaryTrainingRecordLayout;
    var candidate_index = std.AutoHashMapUnmanaged(Span, usize).empty;
    defer candidate_index.deinit(a);
    for (layout.candidate_spans, layout.candidate_valid, 0..) |span, valid, c| {
        try work.tick();
        if (!valid) continue;
        if (span.start < 0 or span.end <= span.start or span.end > layout.word_count) return error.InvalidBoundaryTrainingRecordCandidate;
        const entry = try candidate_index.getOrPut(a, span);
        if (entry.found_existing) return error.DuplicateBoundaryTrainingRecordCandidate;
        entry.value_ptr.* = c;
    }
    for (layout.field_membership, 0..) |member, index| {
        try work.tick();
        if (member and !layout.candidate_valid[index % nc]) return error.InvalidBoundaryTrainingRecordMembership;
    }
    if (layout.mode == .natural) for (layout.instance_mask, layout.anchor_candidates, 0..) |active, seed, i| {
        try work.tick();
        if (!active) continue;
        const c = seed orelse return error.InvalidBoundaryTrainingRecordAnchor;
        if (c >= nc or !layout.candidate_valid[c] or !layout.field_membership[anchor_field.? * nc + c]) return error.InvalidBoundaryTrainingRecordAnchor;
        for (layout.instance_mask[0..i], layout.anchor_candidates[0..i]) |previous_active, previous_seed| {
            try work.tick();
            if (previous_active and previous_seed == seed) return error.AmbiguousBoundaryTrainingRecordAnchor;
        }
    };
    var ng: usize = 0;
    for (records) |record| {
        try work.tick();
        if (record.structure != layout.structure) continue;
        if (record.group != layout.group or record.fields.len != nf or record.anchor_query != layout.anchor_query) return error.InvalidBoundaryTrainingRecordLayout;
        ng += 1;
        if (ng > options.max_records) return error.BoundaryTrainingMatchingLimitExceeded;
    }
    const indicator_count = try elements(try elements(ng, nf, options.max_elements), nc, options.max_elements);
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const ids = try owned.alloc([]const u8, ng);
    const indices = try owned.alloc(usize, ng);
    const indicator = try owned.alloc(bool, indicator_count);
    @memset(indicator, false);
    var g: usize = 0;
    for (records, 0..) |record, source_index| {
        if (record.structure != layout.structure) continue;
        if (record.id.len == 0 or !std.unicode.utf8ValidateSlice(record.id)) return error.InvalidBoundaryTrainingRecordIdentity;
        if (record.id.len > options.max_record_id_bytes) return error.BoundaryTrainingMatchingLimitExceeded;
        for (ids[0..g]) |previous| {
            try work.tick();
            if (std.mem.eql(u8, previous, record.id)) return error.DuplicateBoundaryTrainingRecordIdentity;
        }
        ids[g] = try owned.dupe(u8, record.id);
        indices[g] = source_index;
        for (layout.fields, 0..) |field, f| {
            var target: ?targets.RecordField = null;
            for (record.fields) |candidate| {
                try work.tick();
                if (candidate.query == field.query) {
                    if (target != null) return error.InvalidBoundaryTrainingRecordLayout;
                    target = candidate;
                }
            }
            const values = (target orelse return error.InvalidBoundaryTrainingRecordLayout).values;
            if ((scalar(field.cardinality) and values.len > 1) or (required(field.cardinality) and values.len == 0)) return error.InvalidBoundaryTrainingRecordTargets;
            for (values) |value| {
                if (value.alternatives.len == 0) return error.InvalidBoundaryTrainingRecordTargets;
                var represented = false;
                for (value.alternatives) |span| {
                    try work.tick();
                    if (span.start < 0 or span.end <= span.start or span.end > layout.word_count) return error.InvalidBoundaryTrainingRecordTargets;
                    const c = candidate_index.get(span) orelse continue;
                    if (!layout.field_membership[f * nc + c]) continue;
                    indicator[(g * nf + f) * nc + c] = true;
                    represented = true;
                }
                if (!represented) return error.MissingBoundaryTrainingRecordCandidate;
            }
        }
        g += 1;
    }
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly.gliner2.5.record-targets.v1");
    for ([_]usize{ layout.structure, layout.group, @intFromEnum(layout.mode), layout.word_count, nf, nc, ni, ng }) |n| hashInt(&hash, n);
    for (layout.fields) |field| {
        hashInt(&hash, field.query);
        hashInt(&hash, @intFromEnum(field.cardinality));
    }
    for (layout.candidate_spans, layout.candidate_valid) |span, valid| {
        try work.tick();
        hashFlag(&hash, valid);
        hashInt(&hash, if (valid) @intCast(span.start) else 0);
        hashInt(&hash, if (valid) @intCast(span.end) else 0);
    }
    for (layout.field_membership) |member| {
        try work.tick();
        hashFlag(&hash, member);
    }
    for (layout.instance_mask) |active| {
        try work.tick();
        hashFlag(&hash, active);
    }
    for (layout.anchor_candidates) |seed| {
        try work.tick();
        hashFlag(&hash, seed != null);
        hashInt(&hash, seed orelse 0);
    }
    for (ids, indices) |id, index| {
        try work.tick();
        hashInt(&hash, id.len);
        hash.update(id);
        hashInt(&hash, index);
    }
    for (indicator) |positive| {
        try work.tick();
        hashFlag(&hash, positive);
    }
    var fingerprint: [32]u8 = undefined;
    hash.final(&fingerprint);
    const owned_fields = try owned.dupe(Field, layout.fields);
    const owned_spans = try owned.dupe(Span, layout.candidate_spans);
    const owned_valid = try owned.dupe(bool, layout.candidate_valid);
    const owned_membership = try owned.dupe(bool, layout.field_membership);
    const owned_mask = try owned.dupe(bool, layout.instance_mask);
    const owned_anchors = try owned.dupe(?usize, layout.anchor_candidates);
    try work.check();
    return .{ .arena = arena, .fingerprint = fingerprint, .mode = layout.mode, .fields = owned_fields, .candidate_spans = owned_spans, .candidate_valid = owned_valid, .field_membership = owned_membership, .instance_mask = owned_mask, .anchor_field = anchor_field, .anchor_candidates = owned_anchors, .record_ids = ids, .record_indices = indices, .gold_indicator = indicator, .work = work.count };
}

fn softplus(x: f64) f64 {
    return @max(x, 0) + std.math.log1p(@exp(-@abs(x)));
}
fn targetElements(target: *const TargetMap, work: *Work) !usize {
    try work.check();
    if (target.fields.len > work.options.max_fields or target.candidate_spans.len > work.options.max_candidates or
        target.instance_mask.len > work.options.max_instances or target.record_ids.len > work.options.max_records or work.options.max_work == 0)
        return error.BoundaryTrainingMatchingLimitExceeded;
    return elements(target.instance_mask.len, target.record_ids.len, work.options.max_elements);
}
fn assignmentLogit(target: *const TargetMap, values: []const f32, row: usize, field: usize, column: usize) !f64 {
    const nc = target.candidate_spans.len;
    if (column > 0 and !target.field_membership[field * nc + column - 1]) return -10000;
    const value = values[(row * target.fields.len + field) * (nc + 1) + column];
    if (!std.math.isFinite(value)) return error.InvalidBoundaryTrainingRecordLogit;
    return value;
}

/// Independent reference costs from exact f32 logits, calculated in f64.
/// Scalar costs use log probability mass of all alternative candidates (or
/// ABSENT), preserving the finite -1e4 target-mask sentinel. List matching costs
/// SUM candidate BCE terms; the subsequent matched field loss uses a mean.
pub fn buildCosts(a: Allocator, target: *const TargetMap, logits: Logits, options: Options) !Costs {
    var work = Work{ .options = options };
    const count = try targetElements(target, &work);
    const ni = target.instance_mask.len;
    const ng = target.record_ids.len;
    const nf = target.fields.len;
    const nc = target.candidate_spans.len;
    const width = std.math.add(usize, nc, 1) catch return error.BoundaryTrainingMatchingLimitExceeded;
    const logit_count = try elements(try elements(ni, nf, options.max_elements), width, options.max_elements);
    if (logits.objects.len != ni or logits.assignments.len != logit_count) return error.InvalidBoundaryTrainingRecordShape;
    const costs = try a.alloc(f64, count);
    errdefer a.free(costs);
    if (count == 0) {
        try work.check();
        return .{ .allocator = a, .target_fingerprint = target.fingerprint, .instances = ni, .records = ng, .values = costs, .work = work.count };
    }
    const logp = try a.alloc(f64, width);
    defer a.free(logp);
    for (0..ni) |i| {
        try work.tick();
        if (!target.instance_mask[i]) {
            @memset(costs[i * ng ..][0..ng], 10000);
            continue;
        }
        if (!std.math.isFinite(logits.objects[i])) return error.InvalidBoundaryTrainingRecordLogit;
        @memset(costs[i * ng ..][0..ng], softplus(-@as(f64, logits.objects[i])));
        for (target.fields, 0..) |field, f| {
            var maximum: f64 = -std.math.inf(f64);
            for (logp, 0..) |*value, c| {
                try work.tick();
                value.* = try assignmentLogit(target, logits.assignments, i, f, c);
                maximum = @max(maximum, value.*);
            }
            var normalizer: f64 = 0;
            for (logp) |value| {
                try work.tick();
                normalizer += @exp(value - maximum);
            }
            normalizer = maximum + @log(normalizer);
            if (!std.math.isFinite(normalizer)) return error.InvalidBoundaryTrainingRecordLogit;
            for (logp) |*value| {
                try work.tick();
                value.* -= normalizer;
            }
            for (0..ng) |g| {
                const positives = target.gold_indicator[(g * nf + f) * nc ..][0..nc];
                var term: f64 = 0;
                if (scalar(field.cardinality)) {
                    const absent = std.mem.indexOfScalar(bool, positives, true) == null;
                    var top: f64 = -std.math.inf(f64);
                    for (logp, 0..) |value, c| {
                        try work.tick();
                        top = @max(top, if (if (c == 0) absent else positives[c - 1]) value else -10000);
                    }
                    var mass: f64 = 0;
                    for (logp, 0..) |value, c| {
                        try work.tick();
                        mass += @exp((if (if (c == 0) absent else positives[c - 1]) value else -10000) - top);
                    }
                    term = -(top + @log(mass));
                } else {
                    for (positives, 0..) |positive, c| {
                        try work.tick();
                        const value = try assignmentLogit(target, logits.assignments, i, f, c + 1);
                        term += if (positive) softplus(-value) else softplus(value);
                    }
                }
                costs[i * ng + g] += term;
                if (!std.math.isFinite(costs[i * ng + g])) return error.InvalidBoundaryTrainingRecordLogit;
            }
        }
    }
    try work.check();
    return .{ .allocator = a, .target_fingerprint = target.fingerprint, .instances = ni, .records = ng, .values = costs, .work = work.count };
}

/// Bind actual backend-computed f32 costs without recomputing their reductions
/// on the host. This is the production graph integration entrypoint; the graph
/// must compute costs from the same TargetMap fingerprint before this call.
pub fn fromScoredCosts(a: Allocator, target: *const TargetMap, values: []const f32, options: Options) !Costs {
    var work = Work{ .options = options };
    const count = try targetElements(target, &work);
    if (values.len != count) return error.InvalidBoundaryTrainingRecordShape;
    const costs = try a.alloc(f64, count);
    errdefer a.free(costs);
    for (values, costs, 0..) |value, *cost, index| {
        try work.tick();
        if (!target.instance_mask[index / target.record_ids.len]) {
            cost.* = 10000;
            continue;
        }
        if (!std.math.isFinite(value)) return error.InvalidBoundaryTrainingRecordLogit;
        cost.* = value;
    }
    try work.check();
    return .{ .allocator = a, .target_fingerprint = target.fingerprint, .instances = target.instance_mask.len, .records = target.record_ids.len, .values = costs, .work = work.count };
}

/// Detached native FP32 implementation of build_dense_record_matching_cost.
/// Preserve log_softmax's (x-max)-log(sum(exp(x-max))) rounding and sum field
/// log probabilities before adding logsigmoid(object). Widen only the final
/// FP32 matrix for Hungarian; the f64 diagnostic reference is not equivalent
/// at exact and near ties. Invalid hypotheses are still removed by match().
pub fn buildFloat32Costs(a: Allocator, target: *const TargetMap, logits: Logits, options: Options) !Costs {
    var work = Work{ .options = options };
    const count = try targetElements(target, &work);
    const ni = target.instance_mask.len;
    const ng = target.record_ids.len;
    const nf = target.fields.len;
    const nc = target.candidate_spans.len;
    const width = std.math.add(usize, nc, 1) catch return error.BoundaryTrainingMatchingLimitExceeded;
    if (logits.objects.len != ni or logits.assignments.len != try elements(try elements(ni, nf, options.max_elements), width, options.max_elements)) return error.InvalidBoundaryTrainingRecordShape;
    const costs = try a.alloc(f64, count);
    errdefer a.free(costs);
    const logp = try a.alloc(f32, width);
    defer a.free(logp);
    const field_sum = try a.alloc(f32, ng);
    defer a.free(field_sum);
    for (0..ni) |i| {
        try work.tick();
        if (!std.math.isFinite(logits.objects[i])) return error.InvalidBoundaryTrainingRecordLogit;
        if (!target.instance_mask[i]) {
            @memset(costs[i * ng ..][0..ng], 10000);
            continue;
        }
        @memset(field_sum, 0);
        for (target.fields, 0..) |definition, f| {
            var top: f32 = -std.math.inf(f32);
            for (logp, 0..) |*value, c| {
                try work.tick();
                value.* = @floatCast(try assignmentLogit(target, logits.assignments, i, f, c));
                top = @max(top, value.*);
            }
            var mass: f32 = 0;
            for (logp) |value| mass += @exp(value - top);
            const log_mass = @log(mass);
            for (logp) |*value| value.* = (value.* - top) - log_mass;
            for (0..ng) |g| {
                const positives = target.gold_indicator[(g * nf + f) * nc ..][0..nc];
                var term: f32 = 0;
                if (scalar(definition.cardinality)) {
                    const absent = std.mem.indexOfScalar(bool, positives, true) == null;
                    var selected_top: f32 = -std.math.inf(f32);
                    for (logp, 0..) |value, c| {
                        try work.tick();
                        selected_top = @max(selected_top, if (if (c == 0) absent else positives[c - 1]) value else -10000);
                    }
                    var selected_mass: f32 = 0;
                    for (logp, 0..) |value, c| {
                        try work.tick();
                        selected_mass += @exp((if (if (c == 0) absent else positives[c - 1]) value else -10000) - selected_top);
                    }
                    term = @log(selected_mass) + selected_top;
                } else {
                    var sum: f32 = 0;
                    for (positives, 0..) |positive, c| {
                        try work.tick();
                        const value: f32 = @floatCast(try assignmentLogit(target, logits.assignments, i, f, c + 1));
                        const log_sigmoid = @min(value, 0) - std.math.log1p(@exp(-@abs(value)));
                        sum += (if (positive) @as(f32, 0) else value) - log_sigmoid;
                    }
                    term = -sum;
                }
                field_sum[g] += term;
            }
        }
        const object = logits.objects[i];
        const object_logp = @min(object, 0) - std.math.log1p(@exp(-@abs(object)));
        for (field_sum, 0..) |value, g| {
            const cost = -(object_logp + value);
            if (!std.math.isFinite(cost)) return error.InvalidBoundaryTrainingRecordLogit;
            costs[i * ng + g] = cost;
        }
    }
    try work.check();
    return .{ .allocator = a, .target_fingerprint = target.fingerprint, .instances = ni, .records = ng, .values = costs, .work = work.count };
}

/// Match every gold identity. Natural records bind their explicit anchor; the
/// remaining modes use the shared deterministic Hungarian implementation.
/// Invalid hypotheses are removed BEFORE matching. Unlike the pinned finite
/// sentinel/filter path, they cannot consume and then silently discard gold.
pub fn match(a: Allocator, target: *const TargetMap, costs: *const Costs, options: Options) !Matches {
    var work = Work{ .options = options };
    const count = try targetElements(target, &work);
    const ni = target.instance_mask.len;
    const ng = target.record_ids.len;
    const nf = target.fields.len;
    const nc = target.candidate_spans.len;
    if (costs.instances != ni or costs.records != ng or costs.values.len != count or !std.mem.eql(u8, &costs.target_fingerprint, &target.fingerprint)) return error.BoundaryTrainingRecordBindingMismatch;
    var available: usize = 0;
    for (target.instance_mask) |active| if (active) {
        available += 1;
    };
    if (available < ng) return error.BoundaryTrainingRecordCapacityExceeded;
    for (costs.values, 0..) |value, index| {
        try work.tick();
        if (target.instance_mask[index / ng] and !std.math.isFinite(value)) return error.InvalidBoundaryTrainingRecordLogit;
    }
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const pairs = try owned.alloc(Pair, ng);
    const object_targets = try owned.alloc(f32, ni);
    const object_mask = try owned.alloc(bool, ni);
    @memset(object_targets, 0);
    for (target.instance_mask, object_mask) |active, *mask| mask.* = active and target.mode != .natural;
    if (target.mode == .natural) {
        const used = try a.alloc(bool, ni);
        defer a.free(used);
        @memset(used, false);
        for (0..ng) |g| {
            const positives = target.gold_indicator[(g * nf + target.anchor_field.?) * nc ..][0..nc];
            var selected: ?usize = null;
            var candidate: ?usize = null;
            for (target.instance_mask, target.anchor_candidates, 0..) |active, seed, i| {
                try work.tick();
                if (!active or seed == null or !positives[seed.?]) continue;
                if (candidate == null or seed.? < candidate.?) {
                    selected = i;
                    candidate = seed.?;
                }
            }
            const i = selected orelse return error.MissingBoundaryTrainingRecordAnchor;
            if (used[i]) return error.AmbiguousBoundaryTrainingRecordAnchor;
            used[i] = true;
            pairs[g] = .{ .instance = i, .record = g, .annotation = target.record_indices[g] };
        }
    } else if (ng > 0) {
        const active_rows = try a.alloc(usize, available);
        defer a.free(active_rows);
        var row: usize = 0;
        for (target.instance_mask, 0..) |active, i| if (active) {
            active_rows[row] = i;
            row += 1;
        };
        const active_costs = try a.alloc(f64, try elements(available, ng, options.max_elements));
        defer a.free(active_costs);
        for (active_rows, 0..) |i, r| @memcpy(active_costs[r * ng ..][0..ng], costs.values[i * ng ..][0..ng]);
        var limits = options.assignment_limits;
        limits.max_dimension = @min(limits.max_dimension, @max(options.max_instances, options.max_records));
        limits.max_work = @min(limits.max_work, options.max_work -| work.count);
        limits.control = options.control;
        const matching_work = try elements(try elements(ng, ng, limits.max_work), available, limits.max_work);
        const matched = try assignment.solve(a, active_costs, available, ng, limits);
        defer a.free(matched);
        work.count += matching_work;
        if (matched.len != ng) return error.BoundaryTrainingRecordCapacityExceeded;
        for (matched, pairs) |pair, *out| {
            const instance = active_rows[pair.row];
            out.* = .{ .instance = instance, .record = pair.column, .annotation = target.record_indices[pair.column] };
            object_targets[instance] = 1;
        }
    }
    var total_cost: f64 = 0;
    for (pairs) |pair| {
        try work.tick();
        total_cost += costs.values[pair.instance * ng + pair.record];
    }
    if (!std.math.isFinite(total_cost)) return error.InvalidBoundaryTrainingRecordLogit;
    try work.check();
    return .{ .arena = arena, .target_fingerprint = target.fingerprint, .pairs = pairs, .object_targets = object_targets, .object_mask = object_mask, .total_cost = total_cost, .work = work.count };
}

const test_layout = Layout{
    .structure = 0,
    .group = 2,
    .mode = .latent,
    .word_count = 5,
    .fields = &.{ .{ .query = 3, .cardinality = .required_one }, .{ .query = 7, .cardinality = .zero_or_more } },
    .candidate_spans = &.{ .{ .start = 0, .end = 1 }, .{ .start = 1, .end = 2 }, .{ .start = 2, .end = 3 }, .{ .start = 3, .end = 4 } },
    .candidate_valid = &.{ true, true, true, true },
    .field_membership = &.{ true, true, false, false, false, false, true, true },
    .instance_mask = &.{ true, true, true },
};
const test_records = [_]targets.RecordTarget{
    .{ .structure = 0, .group = 2, .id = "first", .anchor_query = null, .fields = &.{
        .{ .field = 0, .query = 3, .values = &.{.{ .alternatives = &.{.{ .start = 0, .end = 1 }} }} },
        .{ .field = 1, .query = 7, .values = &.{.{ .alternatives = &.{.{ .start = 2, .end = 3 }} }} },
    } },
    .{ .structure = 0, .group = 2, .id = "second", .anchor_query = null, .fields = &.{
        .{ .field = 0, .query = 3, .values = &.{.{ .alternatives = &.{.{ .start = 1, .end = 2 }} }} },
        .{ .field = 1, .query = 7, .values = &.{.{ .alternatives = &.{.{ .start = 3, .end = 4 }} }} },
    } },
};
const test_logits = Logits{ .objects = &.{ 0, 0, 0 }, .assignments = &(@as([30]f32, @splat(0))) };

test "boundary training matching agrees with pinned targets costs and assignments" {
    const Fixture = struct {
        format_version: u32,
        provenance: struct { upstream_commit: []const u8 },
        cases: []const struct {
            id: []const u8,
            layout: Layout,
            records: []const targets.RecordTarget,
            logits: Logits,
            native_error: ?enum { ambiguous_anchor, missing_candidate, capacity } = null,
            expected: struct {
                indicator: []const bool,
                record_indices: []const usize,
                costs: []const f64,
                graph_costs: []const f32,
                pairs: []const Pair = &.{},
                graph_pairs: []const Pair = &.{},
                object_targets: []const f32 = &.{},
                object_mask: []const bool = &.{},
            },
        },
    };
    const a = std.testing.allocator;
    const bytes = try @import("../architectures/gliner_boundary_parity_test.zig").fixtureBytes(a, "training_matching.json");
    defer a.free(bytes);
    var fixture = try std.json.parseFromSlice(Fixture, a, bytes, .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    try std.testing.expectEqual(@as(u32, 1), fixture.value.format_version);
    try std.testing.expectEqualStrings("3c913c7369301133d3b7699252074c4303ada50e", fixture.value.provenance.upstream_commit);
    for (fixture.value.cases) |case| {
        errdefer std.debug.print("training matching fixture: {s}\n", .{case.id});
        const result = compile(a, case.layout, case.records, .{});
        if (case.native_error == .missing_candidate) {
            try std.testing.expectError(error.MissingBoundaryTrainingRecordCandidate, result);
            continue;
        }
        var target = try result;
        defer target.deinit();
        try std.testing.expectEqualSlices(bool, case.expected.indicator, target.gold_indicator);
        try std.testing.expectEqualSlices(usize, case.expected.record_indices, target.record_indices);
        var costs = try buildCosts(a, &target, case.logits, .{});
        defer costs.deinit();
        try std.testing.expectEqual(case.expected.costs.len, costs.values.len);
        for (case.expected.costs, costs.values) |expected, actual| try std.testing.expectApproxEqAbs(expected, actual, @max(@as(f64, 1), @abs(expected)) * 1e-12);
        var graph_costs = try fromScoredCosts(a, &target, case.expected.graph_costs, .{});
        defer graph_costs.deinit();
        for (case.expected.graph_costs, graph_costs.values) |expected, actual| try std.testing.expectEqual(@as(f64, expected), actual);
        var native_costs = try buildFloat32Costs(a, &target, case.logits, .{});
        defer native_costs.deinit();
        for (case.expected.graph_costs, native_costs.values) |expected, actual| try std.testing.expectApproxEqAbs(@as(f64, expected), actual, 3e-5 + @abs(@as(f64, expected)) * 3e-7);
        // The independent f64 reference can differ from Torch by an ulp.
        // SciPy's epsilon perturbation is sensitive even to that uniform
        // shift in exact ties, so compare arithmetic numerically above and
        // compare assignment using the EXACT captured costs below. Runtime
        // assignments always use the backend's f32 costs through the scored
        // bridge, and those are independently checked afterward.
        const reference_costs = Costs{ .allocator = a, .target_fingerprint = target.fingerprint, .instances = costs.instances, .records = costs.records, .values = case.expected.costs, .work = 0 };
        const decoded = match(a, &target, &reference_costs, .{});
        if (case.native_error) |failure| {
            try std.testing.expectError(switch (failure) {
                .ambiguous_anchor => error.AmbiguousBoundaryTrainingRecordAnchor,
                .capacity => error.BoundaryTrainingRecordCapacityExceeded,
                .missing_candidate => unreachable,
            }, decoded);
            continue;
        }
        var matched = try decoded;
        defer matched.deinit();
        try std.testing.expectEqualDeep(case.expected.pairs, matched.pairs);
        try std.testing.expectEqualSlices(f32, case.expected.object_targets, matched.object_targets);
        try std.testing.expectEqualSlices(bool, case.expected.object_mask, matched.object_mask);
        var graph_matched = try match(a, &target, &graph_costs, .{});
        defer graph_matched.deinit();
        try std.testing.expectEqualDeep(case.expected.graph_pairs, graph_matched.pairs);
        var native_matched = try match(a, &target, &native_costs, .{});
        defer native_matched.deinit();
        try std.testing.expectEqualDeep(case.expected.graph_pairs, native_matched.pairs);
    }
}

test "boundary training matching owns target identity and rejects stale scored bindings" {
    const a = std.testing.allocator;
    var spans = test_layout.candidate_spans[0..4].*;
    var identity = "first".*;
    var records = test_records;
    records[0].id = &identity;
    var layout = test_layout;
    layout.candidate_spans = &spans;
    var target = try compile(a, layout, &records, .{});
    defer target.deinit();
    spans[0].end = 5;
    identity[0] = 'x';
    try std.testing.expectEqual(@as(i64, 1), target.candidate_spans[0].end);
    try std.testing.expectEqualStrings("first", target.record_ids[0]);
    var original = try compile(a, test_layout, &test_records, .{});
    defer original.deinit();
    try std.testing.expectEqualSlices(u8, &original.fingerprint, &target.fingerprint);
    var costs = try fromScoredCosts(a, &target, &.{ 1, 2, 2, 1, 3, 3 }, .{});
    defer costs.deinit();
    costs.target_fingerprint[0] ^= 1;
    try std.testing.expectError(error.BoundaryTrainingRecordBindingMismatch, match(a, &target, &costs, .{}));
}

test "boundary training matching excludes masked hypotheses before high cost assignment" {
    const a = std.testing.allocator;
    var layout = test_layout;
    layout.instance_mask = &.{ true, false, true };
    var target = try compile(a, layout, &test_records, .{});
    defer target.deinit();
    var costs = try fromScoredCosts(a, &target, &.{ 30001, 40002, std.math.nan(f32), std.math.inf(f32), 50002, 60001 }, .{});
    defer costs.deinit();
    var matched = try match(a, &target, &costs, .{});
    defer matched.deinit();
    try std.testing.expectEqualDeep(@as([]const Pair, &.{ .{ .instance = 0, .record = 0, .annotation = 0 }, .{ .instance = 2, .record = 1, .annotation = 1 } }), matched.pairs);
    try std.testing.expectEqualSlices(f32, &.{ 1, 0, 1 }, matched.object_targets);
    try std.testing.expectEqualSlices(bool, &.{ true, false, true }, matched.object_mask);
    try std.testing.expectEqual(@as(f64, 90002), matched.total_cost);
    try std.testing.expectError(error.InvalidBoundaryTrainingRecordLogit, fromScoredCosts(a, &target, &.{ std.math.nan(f32), 0, 0, 0, 0, 0 }, .{}));
    layout.instance_mask = &.{ true, false, false };
    var insufficient = try compile(a, layout, &test_records, .{});
    defer insufficient.deinit();
    var insufficient_costs = try fromScoredCosts(a, &insufficient, &.{ 0, 0, 0, 0, 0, 0 }, .{});
    defer insufficient_costs.deinit();
    try std.testing.expectError(error.BoundaryTrainingRecordCapacityExceeded, match(a, &insufficient, &insufficient_costs, .{}));
}

test "boundary training matching validates masked logits and complete retained gold" {
    const a = std.testing.allocator;
    var target = try compile(a, test_layout, &test_records, .{});
    defer target.deinit();
    var logits = @as([30]f32, @splat(0));
    logits[3] = std.math.nan(f32); // field 0 cannot use candidate column 2.
    var masked = try buildCosts(a, &target, .{ .objects = test_logits.objects, .assignments = &logits }, .{});
    defer masked.deinit();
    logits[1] = std.math.nan(f32);
    try std.testing.expectError(error.InvalidBoundaryTrainingRecordLogit, buildCosts(a, &target, .{ .objects = test_logits.objects, .assignments = &logits }, .{}));
    var layout = test_layout;
    layout.field_membership = &.{ false, true, false, false, false, false, true, true };
    try std.testing.expectError(error.MissingBoundaryTrainingRecordCandidate, compile(a, layout, &test_records, .{}));
    layout = test_layout;
    layout.candidate_valid = &.{ false, true, true, true };
    try std.testing.expectError(error.InvalidBoundaryTrainingRecordMembership, compile(a, layout, &test_records, .{}));
    var records = test_records;
    records[1].id = records[0].id;
    try std.testing.expectError(error.DuplicateBoundaryTrainingRecordIdentity, compile(a, test_layout, &records, .{}));
}

fn allocationLifecycle(a: Allocator) !void {
    var target = try compile(a, test_layout, &test_records, .{});
    defer target.deinit();
    var costs = try buildCosts(a, &target, test_logits, .{});
    defer costs.deinit();
    var native_costs = try buildFloat32Costs(a, &target, test_logits, .{});
    defer native_costs.deinit();
    var scored = try fromScoredCosts(a, &target, &.{ 1, 2, 2, 1, 3, 3 }, .{});
    defer scored.deinit();
    var matched = try match(a, &target, &costs, .{});
    defer matched.deinit();
    var layout = test_layout;
    layout.mode = .natural;
    layout.anchor_query = 3;
    layout.anchor_candidates = &.{ 0, 1, null };
    layout.instance_mask = &.{ true, true, false };
    var records = test_records;
    for (&records) |*record| record.anchor_query = 3;
    var natural = try compile(a, layout, &records, .{});
    defer natural.deinit();
    var natural_costs = try buildCosts(a, &natural, test_logits, .{});
    defer natural_costs.deinit();
    var natural_matched = try match(a, &natural, &natural_costs, .{});
    defer natural_matched.deinit();
}
test "boundary training matching releases allocations on all failure points" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationLifecycle, .{});
}

test "boundary training matching bounds cancellation and empty supervision" {
    const a = std.testing.allocator;
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    const cancelled = Options{ .control = .{ .check_fn = Cancel.check } };
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.Cancelled, compile(failing.allocator(), test_layout, &test_records, cancelled));
    try std.testing.expectError(error.BoundaryTrainingMatchingLimitExceeded, compile(failing.allocator(), test_layout, &test_records, .{ .max_elements = 2 }));
    try std.testing.expect(!failing.has_induced_failure);
    try std.testing.expectError(error.BoundaryTrainingMatchingLimitExceeded, compile(a, test_layout, &test_records, .{ .max_work = 1 }));
    var target = try compile(a, test_layout, &test_records, .{});
    defer target.deinit();
    try std.testing.expectError(error.Cancelled, buildCosts(failing.allocator(), &target, test_logits, cancelled));
    try std.testing.expectError(error.BoundaryTrainingMatchingLimitExceeded, buildCosts(failing.allocator(), &target, test_logits, .{ .max_instances = 1 }));
    try std.testing.expect(!failing.has_induced_failure);
    var costs = try buildCosts(a, &target, test_logits, .{});
    defer costs.deinit();
    try std.testing.expectError(error.Cancelled, match(failing.allocator(), &target, &costs, cancelled));
    try std.testing.expectError(error.BoundaryTrainingMatchingLimitExceeded, match(a, &target, &costs, .{ .max_work = 7 }));
    var empty = try compile(a, test_layout, &.{}, .{});
    defer empty.deinit();
    var empty_costs = try buildCosts(a, &empty, test_logits, .{});
    defer empty_costs.deinit();
    var empty_match = try match(a, &empty, &empty_costs, .{});
    defer empty_match.deinit();
    try std.testing.expectEqual(@as(usize, 0), empty_match.pairs.len);
    try std.testing.expectEqual(@as(f64, 0), empty_match.total_cost);
    try std.testing.expectEqualSlices(f32, &.{ 0, 0, 0 }, empty_match.object_targets);
    try std.testing.expectEqualSlices(bool, &.{ true, true, true }, empty_match.object_mask);
}
