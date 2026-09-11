// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Final dense record loss and live-logit cotangents after detached matching.
//! The reduction follows compute_dense_batch_loss, not matching costs or the
//! differently normalized per-group compatibility helper. No learned value is
//! detached here: returned cotangents seed the retained differentiable graph.
const std = @import("std");
const matching = @import("gliner_boundary_matching.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;

pub const Group = struct {
    target: *const matching.TargetMap,
    matches: *const matching.Matches,
    logits: matching.Logits,
    /// Padded groups may be disabled only when they contain no gold records.
    enabled: bool = true,
};
pub const Options = struct {
    object_weight: f32 = 1,
    field_weight: f32 = 1,
    max_groups: usize = 4096,
    max_fields: usize = 1024,
    max_candidates: usize = 65536,
    max_instances: usize = 1024,
    max_records: usize = 256,
    max_elements: usize = 16 * 1024 * 1024,
    max_work: usize = 100 * 1024 * 1024,
    control: ?Control = null,
};
pub const Gradients = struct {
    objects: []f32,
    /// Same [I,F,1+C] layout as the input logits. Excluded candidate columns
    /// are constants in the source forward mask and receive zero derivative.
    assignments: []f32,
};
pub const Result = struct {
    allocator: Allocator,
    object_loss: f32,
    field_loss: f32,
    value: f32,
    gradients: []Gradients,
    storage: []f32,
    object_count: usize,
    matched_record_count: usize,
    decisions_fingerprint: [32]u8,
    work: usize,
    pub fn deinit(self: *Result) void {
        self.allocator.free(self.storage);
        self.allocator.free(self.gradients);
        self.* = undefined;
    }
};
const Work = struct {
    options: Options,
    steps: usize = 0,
    fn check(self: *const Work) !void {
        if (self.options.control) |control| try control.check();
    }
    fn tick(self: *Work) !void {
        if (self.steps >= self.options.max_work) return error.BoundaryTrainingRecordLossLimitExceeded;
        self.steps += 1;
        if (self.steps % 128 == 1) try self.check();
    }
};
fn count(a: usize, b: usize, limit: usize) !usize {
    const n = std.math.mul(usize, a, b) catch return error.BoundaryTrainingRecordLossLimitExceeded;
    if (n > limit) return error.BoundaryTrainingRecordLossLimitExceeded;
    return n;
}
fn addCount(a: usize, b: usize, limit: usize) !usize {
    const n = std.math.add(usize, a, b) catch return error.BoundaryTrainingRecordLossLimitExceeded;
    if (n > limit) return error.BoundaryTrainingRecordLossLimitExceeded;
    return n;
}
fn finite(value: f32) !void {
    if (!std.math.isFinite(value)) return error.NonFiniteBoundaryTrainingRecordLoss;
}
fn sigmoid(value: f32) f32 {
    if (value >= 0) return 1 / (1 + @exp(-value));
    const e = @exp(value);
    return e / (1 + e);
}
fn bce(value: f32, target: bool) f32 {
    return (if (target) @max(-value, 0) else @max(value, 0)) + std.math.log1p(@exp(-@abs(value)));
}
fn hashInt(hash: *std.crypto.hash.sha2.Sha256, value: usize) void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, @intCast(value), .little);
    hash.update(&bytes);
}
fn validate(group: Group, work: *Work) !usize {
    try work.check();
    const t = group.target;
    const m = group.matches;
    const o = work.options;
    const nf = t.fields.len;
    const nc = t.candidate_spans.len;
    const ni = t.instance_mask.len;
    const ng = t.record_ids.len;
    if (nf == 0 or t.candidate_valid.len != nc or t.record_indices.len != ng or
        t.field_membership.len != try count(nf, nc, o.max_elements) or
        t.gold_indicator.len != try count(try count(ng, nf, o.max_elements), nc, o.max_elements) or
        group.logits.objects.len != ni or group.logits.assignments.len != try count(try count(ni, nf, o.max_elements), try addCount(nc, 1, o.max_elements), o.max_elements) or
        m.object_targets.len != ni or m.object_mask.len != ni) return error.InvalidBoundaryTrainingRecordLossShape;
    if (nf > o.max_fields or nc > o.max_candidates or ni > o.max_instances or ng > o.max_records) return error.BoundaryTrainingRecordLossLimitExceeded;
    if (!std.mem.eql(u8, &t.fingerprint, &m.target_fingerprint)) return error.BoundaryTrainingRecordBindingMismatch;
    if (m.pairs.len != ng or (!group.enabled and ng != 0)) return error.InvalidBoundaryTrainingRecordLossMatches;
    if (t.mode == .natural and (t.anchor_field == null or t.anchor_field.? >= nf or t.anchor_candidates.len != ni)) return error.InvalidBoundaryTrainingRecordLossMatches;
    for (t.field_membership, 0..) |member, index| {
        try work.tick();
        if (member and !t.candidate_valid[index % nc]) return error.InvalidBoundaryTrainingRecordMembership;
    }
    for (t.gold_indicator, 0..) |gold, index| {
        try work.tick();
        if (gold and !t.field_membership[index % (nf * nc)]) return error.InvalidBoundaryTrainingRecordMembership;
    }
    for (m.pairs, 0..) |pair, p| {
        try work.tick();
        if (pair.instance >= ni or pair.record >= ng or pair.annotation != t.record_indices[pair.record] or !t.instance_mask[pair.instance]) return error.InvalidBoundaryTrainingRecordLossMatches;
        if (t.mode == .natural) {
            const anchor = t.anchor_candidates[pair.instance] orelse return error.InvalidBoundaryTrainingRecordLossMatches;
            if (anchor >= nc or !t.gold_indicator[(pair.record * nf + t.anchor_field.?) * nc + anchor]) return error.InvalidBoundaryTrainingRecordLossMatches;
        }
        for (m.pairs[0..p]) |previous| {
            try work.tick();
            if (pair.instance == previous.instance or pair.record == previous.record) return error.InvalidBoundaryTrainingRecordLossMatches;
        }
    }
    for (t.instance_mask, m.object_mask, m.object_targets, 0..) |active, mask, target, instance| {
        try work.tick();
        if (mask != (active and t.mode != .natural)) return error.InvalidBoundaryTrainingRecordLossMatches;
        var positive = false;
        if (t.mode != .natural) for (m.pairs) |pair| {
            try work.tick();
            if (pair.instance == instance) positive = true;
        };
        if (target != @as(f32, if (positive) 1 else 0)) return error.InvalidBoundaryTrainingRecordLossMatches;
    }
    for (group.logits.objects) |value| {
        try work.tick();
        try finite(value);
    }
    for (group.logits.assignments) |value| {
        try work.tick();
        try finite(value);
    }
    return addCount(group.logits.objects.len, group.logits.assignments.len, o.max_elements);
}

fn assignmentValue(row: []const f32, membership: []const bool, column: usize) f32 {
    return if (column == 0 or membership[column - 1]) row[column] else -10000;
}

/// Scalar targets select the probability MASS of all alternative occurrences.
/// The non-target -1e4 entries in upstream's second logsumexp are constants,
/// so when they dominate, the derivative is reduced by their share of mass.
fn scalarField(work: *Work, row: []const f32, membership: []const bool, gold: []const bool, output: []f32, scale: f32) !f32 {
    var present = false;
    for (gold) |positive| present = present or positive;
    var maximum: f32 = -std.math.inf(f32);
    for (row, 0..) |_, column| {
        try work.tick();
        maximum = @max(maximum, assignmentValue(row, membership, column));
    }
    var denominator: f32 = 0;
    for (row, 0..) |_, column| {
        try work.tick();
        const shifted = assignmentValue(row, membership, column) - maximum;
        try finite(shifted);
        denominator += @exp(shifted);
    }
    const log_sum = @log(denominator);
    try finite(log_sum);
    var target_maximum: f32 = -std.math.inf(f32);
    for (row, 0..) |_, column| {
        try work.tick();
        const target = if (column == 0) !present else gold[column - 1];
        const value = if (target) (assignmentValue(row, membership, column) - maximum) - log_sum else -10000;
        target_maximum = @max(target_maximum, value);
    }
    var target_sum: f32 = 0;
    for (row, 0..) |_, column| {
        try work.tick();
        const target = if (column == 0) !present else gold[column - 1];
        const value = if (target) (assignmentValue(row, membership, column) - maximum) - log_sum else -10000;
        target_sum += @exp(value - target_maximum);
    }
    const log_mass = target_maximum + @log(target_sum);
    try finite(log_mass);
    var live_target_mass: f32 = 0;
    for (row, 0..) |_, column| {
        try work.tick();
        const target = if (column == 0) !present else gold[column - 1];
        if (target) live_target_mass += @exp((assignmentValue(row, membership, column) - maximum) - log_sum - log_mass);
    }
    for (row, output, 0..) |_, *gradient, column| {
        try work.tick();
        if (column > 0 and !membership[column - 1]) continue;
        const logp = (assignmentValue(row, membership, column) - maximum) - log_sum;
        const target = if (column == 0) !present else gold[column - 1];
        gradient.* += (@exp(logp) * live_target_mass - (if (target) @exp(logp - log_mass) else 0)) * scale;
        try finite(gradient.*);
    }
    return -log_mass;
}

/// Groups carry only actual declared fields; a caller omits padded fields.
/// Global object/record denominators therefore equal the source field/group
/// masks without introducing a padded tensor or averaging per-group losses.
pub fn compute(a: Allocator, groups: []const Group, options: Options) !Result {
    var work = Work{ .options = options };
    try work.check();
    if (!std.math.isFinite(options.object_weight) or !std.math.isFinite(options.field_weight) or options.object_weight < 0 or options.field_weight < 0) return error.InvalidBoundaryTrainingRecordLossOptions;
    if (groups.len > options.max_groups or options.max_work == 0) return error.BoundaryTrainingRecordLossLimitExceeded;
    var elements: usize = 0;
    var objects: usize = 0;
    var matched: usize = 0;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly.boundary-record-loss.decisions.v1");
    hashInt(&hash, groups.len);
    hashInt(&hash, @as(u32, @bitCast(options.object_weight)));
    hashInt(&hash, @as(u32, @bitCast(options.field_weight)));
    for (groups) |group| {
        elements = try addCount(elements, try validate(group, &work), options.max_elements);
        hash.update(&group.target.fingerprint);
        hash.update(&.{@intFromBool(group.enabled)});
        for (group.matches.pairs) |pair| {
            hashInt(&hash, pair.instance);
            hashInt(&hash, pair.record);
            hashInt(&hash, pair.annotation);
        }
        if (!group.enabled) continue;
        matched = try addCount(matched, group.matches.pairs.len, options.max_elements);
        for (group.matches.object_mask) |keep| if (keep) {
            objects = try addCount(objects, 1, options.max_elements);
        };
    }
    const gradients = try a.alloc(Gradients, groups.len);
    errdefer a.free(gradients);
    const storage = try a.alloc(f32, elements);
    errdefer a.free(storage);
    @memset(storage, 0);
    const object_denom: f32 = @floatFromInt(@max(objects, 1));
    const record_denom: f32 = @floatFromInt(@max(matched, 1));
    var offset: usize = 0;
    var object_sum: f32 = 0;
    var field_sum: f32 = 0;
    for (groups, gradients) |group, *gradient| {
        try work.check();
        gradient.objects = storage[offset..][0..group.logits.objects.len];
        offset += group.logits.objects.len;
        gradient.assignments = storage[offset..][0..group.logits.assignments.len];
        offset += group.logits.assignments.len;
        if (!group.enabled) continue;
        const t = group.target;
        const nc = t.candidate_spans.len;
        const nf = t.fields.len;
        const field_denom: f32 = @floatFromInt(nf);
        const field_scale = (options.field_weight / record_denom) / field_denom;
        for (group.logits.objects, group.matches.object_mask, group.matches.object_targets, gradient.objects) |value, keep, target, *dy| {
            try work.tick();
            if (!keep) continue;
            object_sum += bce(value, target == 1);
            dy.* = ((sigmoid(value) - target) / object_denom) * options.object_weight;
            try finite(dy.*);
        }
        for (group.matches.pairs) |pair| {
            var record_sum: f32 = 0;
            for (t.fields, 0..) |spec, f| {
                try work.tick();
                const base = (pair.instance * nf + f) * (nc + 1);
                const row = group.logits.assignments[base..][0 .. nc + 1];
                const dy = gradient.assignments[base..][0 .. nc + 1];
                const membership = t.field_membership[f * nc ..][0..nc];
                const gold = t.gold_indicator[(pair.record * nf + f) * nc ..][0..nc];
                if (spec.cardinality == .required_one or spec.cardinality == .optional_one) {
                    record_sum += try scalarField(&work, row, membership, gold, dy, field_scale);
                } else {
                    if (nc == 0) return error.InvalidBoundaryTrainingRecordLossShape;
                    const denom: f32 = @floatFromInt(nc);
                    var list_sum: f32 = 0;
                    for (gold, membership, 0..) |positive, member, c| {
                        try work.tick();
                        const value = assignmentValue(row, membership, c + 1);
                        list_sum += bce(value, positive);
                        if (member) dy[c + 1] += ((sigmoid(value) - @as(f32, if (positive) 1 else 0)) / denom) * field_scale;
                        try finite(dy[c + 1]);
                    }
                    record_sum += list_sum / denom;
                }
            }
            field_sum += record_sum / field_denom;
        }
    }
    const object_loss = object_sum / object_denom;
    const field_loss = field_sum / record_denom;
    const value = options.object_weight * object_loss + options.field_weight * field_loss;
    try finite(object_loss);
    try finite(field_loss);
    try finite(value);
    try work.check();
    return .{ .allocator = a, .object_loss = object_loss, .field_loss = field_loss, .value = value, .gradients = gradients, .storage = storage, .object_count = objects, .matched_record_count = matched, .decisions_fingerprint = hash.finalResult(), .work = work.steps };
}

const TestOwner = struct {
    target: matching.TargetMap,
    matches: matching.Matches,
    fn deinit(self: *TestOwner) void {
        self.matches.deinit();
        self.target.deinit();
    }
    fn group(self: *const TestOwner) Group {
        return .{ .target = &self.target, .matches = &self.matches, .logits = .{ .objects = &.{ 0.2, -0.4 }, .assignments = &.{ 0.1, 0.3, -1, 0.2, -0.8, 0.4 } } };
    }
};
fn testOwner(a: Allocator) !TestOwner {
    var target = try matching.compile(a, .{ .structure = 0, .group = 0, .mode = .latent, .word_count = 2, .fields = &.{.{ .query = 0, .cardinality = .optional_one }}, .candidate_spans = &.{ .{ .start = 0, .end = 1 }, .{ .start = 1, .end = 2 } }, .candidate_valid = &.{ true, true }, .field_membership = &.{ true, true }, .instance_mask = &.{ true, true } }, &.{.{ .structure = 0, .group = 0, .id = "one", .anchor_query = null, .fields = &.{.{ .field = 0, .query = 0, .values = &.{.{ .alternatives = &.{.{ .start = 0, .end = 1 }} }} }} }}, .{});
    errdefer target.deinit();
    var costs = try matching.fromScoredCosts(a, &target, &.{ 0, 1 }, .{});
    defer costs.deinit();
    return .{ .target = target, .matches = try matching.match(a, &target, &costs, .{}) };
}
fn allocationLifecycle(a: Allocator, group: Group) !void {
    var result = try compute(a, &.{ group, group }, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.gradients.len);
}

test "boundary training record loss owns gradients and releases allocation failures" {
    var owner = try testOwner(std.testing.allocator);
    defer owner.deinit();
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationLifecycle, .{owner.group()});
}

test "boundary training record loss requires complete immutable matching and finite inputs" {
    const a = std.testing.allocator;
    var owner = try testOwner(a);
    defer owner.deinit();
    var bad_matches = owner.matches;
    bad_matches.target_fingerprint[0] ^= 1;
    var group = owner.group();
    group.matches = &bad_matches;
    try std.testing.expectError(error.BoundaryTrainingRecordBindingMismatch, compute(a, &.{group}, .{}));
    bad_matches = owner.matches;
    bad_matches.pairs = &.{};
    try std.testing.expectError(error.InvalidBoundaryTrainingRecordLossMatches, compute(a, &.{group}, .{}));
    bad_matches.pairs = &.{.{ .instance = 0, .record = 0, .annotation = 10 }};
    try std.testing.expectError(error.InvalidBoundaryTrainingRecordLossMatches, compute(a, &.{group}, .{}));
    bad_matches = owner.matches;
    bad_matches.object_targets = &.{ 0, 0 };
    try std.testing.expectError(error.InvalidBoundaryTrainingRecordLossMatches, compute(a, &.{group}, .{}));
    group = owner.group();
    group.enabled = false;
    try std.testing.expectError(error.InvalidBoundaryTrainingRecordLossMatches, compute(a, &.{group}, .{}));
    group = owner.group();
    group.logits.objects = &.{ 0, std.math.nan(f32) };
    try std.testing.expectError(error.NonFiniteBoundaryTrainingRecordLoss, compute(a, &.{group}, .{}));
    group = owner.group();
    group.logits.assignments = &.{ 0, 1 };
    try std.testing.expectError(error.InvalidBoundaryTrainingRecordLossShape, compute(a, &.{group}, .{}));
}

test "boundary training record loss bounds cancellation empty batches and weight options" {
    const a = std.testing.allocator;
    var owner = try testOwner(a);
    defer owner.deinit();
    try std.testing.expectError(error.InvalidBoundaryTrainingRecordLossOptions, compute(a, &.{owner.group()}, .{ .object_weight = -1 }));
    try std.testing.expectError(error.BoundaryTrainingRecordLossLimitExceeded, compute(a, &.{owner.group()}, .{ .max_elements = 1 }));
    try std.testing.expectError(error.BoundaryTrainingRecordLossLimitExceeded, compute(a, &.{owner.group()}, .{ .max_work = 30 }));
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, compute(a, &.{owner.group()}, .{ .control = .{ .check_fn = Cancel.check } }));
    var empty = try compute(a, &.{}, .{});
    defer empty.deinit();
    try std.testing.expectEqual(@as(f32, 0), empty.value);
    try std.testing.expectEqual(@as(usize, 0), empty.storage.len);
    var first = try compute(a, &.{owner.group()}, .{});
    defer first.deinit();
    var weighted = try compute(a, &.{owner.group()}, .{ .field_weight = 0 });
    defer weighted.deinit();
    try std.testing.expect(!std.mem.eql(u8, &first.decisions_fingerprint, &weighted.decisions_fingerprint));
    for (weighted.gradients[0].assignments) |value| try std.testing.expectEqual(@as(f32, 0), value);
}

const OracleGradient = struct { objects: []const f32, assignments: []const f32 };
const OracleGroup = struct {
    layout: matching.Layout,
    records: []const @import("gliner_boundary_targets.zig").RecordTarget,
    logits: matching.Logits,
    enabled: bool,
    expected: struct { graph_costs: []const f32, pairs: []const matching.Pair },
};
const OracleCase = struct {
    id: []const u8,
    object_weight: f32,
    field_weight: f32,
    groups: []const OracleGroup,
    expected: struct { object_loss: f32, field_loss: f32, value: f32, object_count: usize, matched_record_count: usize, gradients: []const OracleGradient },
};

test "boundary training record loss matches pinned batch values and every live logit gradient" {
    const a = std.testing.allocator;
    const bytes = try @import("../architectures/gliner_boundary_parity_test.zig").fixtureBytes(a, "training_record_loss.json");
    defer a.free(bytes);
    var parsed = try std.json.parseFromSlice(struct { source_commit: []const u8, cases: []const OracleCase }, a, bytes, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqualStrings("3c913c7369301133d3b7699252074c4303ada50e", parsed.value.source_commit);
    try std.testing.expectEqual(@as(usize, 10), parsed.value.cases.len);
    for (parsed.value.cases) |case| {
        errdefer std.debug.print("record loss oracle: {s}\n", .{case.id});
        const owned = try a.alloc(TestOwner, case.groups.len);
        defer a.free(owned);
        var initialized: usize = 0;
        defer for (owned[0..initialized]) |*item| item.deinit();
        const groups = try a.alloc(Group, case.groups.len);
        defer a.free(groups);
        for (case.groups, owned, groups) |raw, *item, *group| {
            var target = try matching.compile(a, raw.layout, raw.records, .{});
            errdefer target.deinit();
            var costs = try matching.fromScoredCosts(a, &target, raw.expected.graph_costs, .{});
            defer costs.deinit();
            var matches = try matching.match(a, &target, &costs, .{});
            errdefer matches.deinit();
            try std.testing.expectEqualSlices(matching.Pair, raw.expected.pairs, matches.pairs);
            item.* = .{ .target = target, .matches = matches };
            initialized += 1;
            group.* = .{ .target = &item.target, .matches = &item.matches, .logits = raw.logits, .enabled = raw.enabled };
        }
        var result = try compute(a, groups, .{ .object_weight = case.object_weight, .field_weight = case.field_weight });
        defer result.deinit();
        try std.testing.expectApproxEqAbs(case.expected.object_loss, result.object_loss, 2e-5 + @abs(case.expected.object_loss) * 4e-7);
        try std.testing.expectApproxEqAbs(case.expected.field_loss, result.field_loss, 2e-5 + @abs(case.expected.field_loss) * 4e-7);
        try std.testing.expectApproxEqAbs(case.expected.value, result.value, 2e-5 + @abs(case.expected.value) * 4e-7);
        try std.testing.expectEqual(case.expected.object_count, result.object_count);
        try std.testing.expectEqual(case.expected.matched_record_count, result.matched_record_count);
        try std.testing.expectEqual(case.expected.gradients.len, result.gradients.len);
        for (case.expected.gradients, result.gradients) |wanted, actual| {
            try std.testing.expectEqual(wanted.objects.len, actual.objects.len);
            try std.testing.expectEqual(wanted.assignments.len, actual.assignments.len);
            for (wanted.objects, actual.objects) |left, right| try std.testing.expectApproxEqAbs(left, right, 5e-5);
            for (wanted.assignments, actual.assignments) |left, right| try std.testing.expectApproxEqAbs(left, right, 5e-5);
        }
    }
}
