// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Schema-aware decoding of ragged GLiNER2.5 record-head scores. Exclusive
//! scalar fields use global assignment; list candidates have one best owner.
const std = @import("std");
const boundary = @import("gliner_boundary_decode.zig");
const matching = @import("extraction_assignment.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
pub const Span = @import("../architectures/gliner_boundary_ops.zig").Span;
pub const Mode = enum { natural, latent, anchorless };
pub const Field = struct {
    query_id: usize,
    spans: []const Span,
    /// [instances, 1+candidates], explicit ABSENT in column zero.
    assignment_logits: []const f32,
    scalar: bool,
    allows_absent: bool,
    exclusive: bool = false,
};
pub const Group = struct {
    mode: Mode,
    anchor_field: ?usize = null,
    object_logits: []const f32,
    instance_spans: []const ?Span,
    fields: []const Field,
};
pub const Value = struct { span: Span, probability: f32 };
pub const Selection = struct { query_id: usize, values: []const Value };
pub const Record = struct {
    source_instance: usize,
    probability: f32,
    anchor: ?Span,
    fields: []const Selection,
};
pub const Result = struct {
    arena: std.heap.ArenaAllocator,
    records: []const Record,
    pub fn deinit(self: *Result) void {
        self.arena.deinit();
        self.* = undefined;
    }
};
pub const Options = struct {
    anchor_threshold: f32 = 0.5,
    object_threshold: f32 = 0.5,
    field_threshold: f32 = 0.5,
    temperature: f32 = 1,
    max_instances: usize = 512,
    max_fields: usize = 256,
    max_score_elements: usize = 1024 * 1024,
    /// Values retained across complete, deduplicated records. A reusable
    /// single-record scratch buffer has this same ceiling; rejected duplicate
    /// records consume score work, but no retained-value capacity.
    max_output_values: usize = 65536,
    assignment: matching.Limits = .{},
    control: ?Control = null,

    fn check(self: Options) !void {
        if (self.control) |control| try control.check();
    }
};
const RankedInstance = struct {
    index: usize,
    probability: f32,
    fn less(_: void, a: RankedInstance, b: RankedInstance) bool {
        return if (a.probability != b.probability) a.probability > b.probability else a.index < b.index;
    }
};

fn softmax(logits: []const f32, temperature: f32, output: []f32) !void {
    var maximum = -std.math.inf(f32);
    for (logits) |logit| maximum = @max(maximum, logit / temperature);
    if (!std.math.isFinite(maximum)) return error.NonFiniteBoundaryScore;
    var sum: f32 = 0;
    for (logits, output) |logit, *value| {
        value.* = @exp(logit / temperature - maximum);
        sum += value.*;
    }
    if (!std.math.isFinite(sum) or sum <= 0) return error.NonFiniteBoundaryScore;
    for (output) |*value| value.* /= sum;
}

fn recordKey(scratch: []Span, record: Record, options: Options) ![32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    var bytes: [8]u8 = undefined;
    for (record.fields) |field| {
        try options.check();
        if (field.values.len == 0) continue;
        std.mem.writeInt(u64, &bytes, @intCast(field.query_id), .little);
        hash.update(&bytes);
        std.mem.writeInt(u64, &bytes, @intCast(field.values.len), .little);
        hash.update(&bytes);
        std.debug.assert(field.values.len <= scratch.len);
        for (field.values, scratch[0..field.values.len], 0..) |value, *span, index| {
            if (index % 256 == 0) try options.check();
            span.* = value.span;
        }
        std.mem.sort(Span, scratch[0..field.values.len], {}, struct {
            fn less(_: void, a: Span, b: Span) bool {
                return if (a.start != b.start) a.start < b.start else a.end < b.end;
            }
        }.less);
        for (scratch[0..field.values.len], 0..) |span, index| {
            if (index % 256 == 0) try options.check();
            std.mem.writeInt(u64, &bytes, @intCast(span.start), .little);
            hash.update(&bytes);
            std.mem.writeInt(u64, &bytes, @intCast(span.end), .little);
            hash.update(&bytes);
        }
    }
    return hash.finalResult();
}

fn probabilitiesForRow(logits: []const f32, scalar: bool, probabilities: []f32, options: Options) !void {
    try options.check();
    if (scalar) return softmax(logits, options.temperature, probabilities);
    for (logits, probabilities, 0..) |logit, *value, index| {
        if (index % 256 == 0) try options.check();
        value.* = boundary.sigmoid(logit / options.temperature);
    }
}

const ExclusiveDecisions = struct {
    const FieldDecision = struct {
        chosen: []?usize = &.{},
        owners: []?usize = &.{},
    };

    allocator: std.mem.Allocator,
    fields: []FieldDecision,

    fn deinit(self: *@This()) void {
        for (self.fields) |field| {
            self.allocator.free(field.chosen);
            self.allocator.free(field.owners);
        }
        self.allocator.free(self.fields);
        self.* = undefined;
    }

    fn init(allocator: std.mem.Allocator, group: Group, ranked: []const RankedInstance, options: Options) !@This() {
        var result = @This(){ .allocator = allocator, .fields = try allocator.alloc(FieldDecision, group.fields.len) };
        @memset(result.fields, .{});
        errdefer result.deinit();
        const selected = ranked.len;
        for (group.fields, result.fields) |field, *decision| {
            try options.check();
            const nc = field.spans.len;
            if (!field.exclusive or selected == 0 or nc == 0) continue;
            const stride = try std.math.add(usize, nc, 1);
            // The score validation already bounds this matrix. Only one
            // field's probabilities/costs survive at a time.
            const probabilities = try allocator.alloc(f32, try std.math.mul(usize, selected, stride));
            defer allocator.free(probabilities);
            for (ranked, 0..) |instance, r| {
                try probabilitiesForRow(field.assignment_logits[instance.index * stride ..][0..stride], field.scalar, probabilities[r * stride ..][0..stride], options);
            }
            if (field.scalar) {
                decision.chosen = try allocator.alloc(?usize, selected);
                @memset(decision.chosen, null);
                const columns = try std.math.add(usize, nc, selected);
                // Use the solver's existing dimensions/work contract before
                // allocating the additional emergency-ABSENT cost columns.
                if (selected > options.assignment.max_dimension or columns > options.assignment.max_dimension)
                    return error.ExtractionAssignmentLimitExceeded;
                const n = @min(selected, columns);
                const m = @max(selected, columns);
                if (try std.math.mul(usize, try std.math.mul(usize, n, n), m) > options.assignment.max_work)
                    return error.ExtractionAssignmentLimitExceeded;
                const costs = try allocator.alloc(f64, try std.math.mul(usize, selected, columns));
                defer allocator.free(costs);
                const eps = std.math.floatEps(f32);
                var maximum: f32 = 0;
                for (0..selected) |r| {
                    try options.check();
                    for (0..nc) |c| {
                        const cost = -@log(@max(eps, probabilities[r * stride + c + 1]));
                        costs[r * columns + c] = cost;
                        maximum = @max(maximum, cost);
                    }
                }
                var absent_max = maximum + 50;
                if (field.allows_absent) {
                    absent_max = 0;
                    for (0..selected) |r| absent_max = @max(absent_max, -@log(@max(eps, probabilities[r * stride])));
                }
                const invalid: f32 = @max(maximum, absent_max) + 1000;
                for (0..selected) |r| {
                    try options.check();
                    for (0..selected) |c| costs[r * columns + nc + c] = if (r != c) invalid else if (field.allows_absent) -@log(@max(eps, probabilities[r * stride])) else maximum + 50;
                }
                var assignment_limits = options.assignment;
                assignment_limits.control = options.control;
                const pairs = try matching.solve(allocator, costs, selected, columns, assignment_limits);
                defer allocator.free(pairs);
                for (pairs) |pair| if (pair.column < nc) {
                    const probability = probabilities[pair.row * stride + pair.column + 1];
                    if (!field.allows_absent or probability >= options.field_threshold) decision.chosen[pair.row] = pair.column;
                };
            } else {
                decision.owners = try allocator.alloc(?usize, nc);
                @memset(decision.owners, null);
                for (0..nc) |c| {
                    try options.check();
                    var best: usize = 0;
                    for (1..selected) |r| if (probabilities[r * stride + c + 1] > probabilities[best * stride + c + 1]) {
                        best = r;
                    };
                    if (probabilities[best * stride + c + 1] >= options.field_threshold) decision.owners[c] = best;
                }
            }
        }
        return result;
    }
};

/// Scratch never contains more than one complete record and never borrows
/// Result.arena. Its slices remain stable while that record is hashed/copied.
const RecordScratch = struct {
    allocator: std.mem.Allocator,
    values: []Value,
    fields: []Selection,
    probabilities: []f32,
    key_spans: []Span,
    used: usize = 0,

    fn init(allocator: std.mem.Allocator, group: Group, options: Options) !@This() {
        var row_values: usize = 0;
        var maximum_field_values: usize = 0;
        var maximum_stride: usize = 0;
        for (group.fields, 0..) |field, f| {
            const maximum = if (group.anchor_field == f) 1 else if (field.scalar) @min(field.spans.len, 1) else field.spans.len;
            row_values = try std.math.add(usize, row_values, maximum);
            maximum_field_values = @max(maximum_field_values, maximum);
            maximum_stride = @max(maximum_stride, try std.math.add(usize, field.spans.len, 1));
        }
        const value_capacity = @min(row_values, options.max_output_values);
        const values = try allocator.alloc(Value, value_capacity);
        errdefer allocator.free(values);
        const fields = try allocator.alloc(Selection, group.fields.len);
        errdefer allocator.free(fields);
        const probabilities = try allocator.alloc(f32, maximum_stride);
        errdefer allocator.free(probabilities);
        const key_spans = try allocator.alloc(Span, @min(maximum_field_values, value_capacity));
        return .{ .allocator = allocator, .values = values, .fields = fields, .probabilities = probabilities, .key_spans = key_spans };
    }

    fn deinit(self: *@This()) void {
        self.allocator.free(self.key_spans);
        self.allocator.free(self.probabilities);
        self.allocator.free(self.fields);
        self.allocator.free(self.values);
        self.* = undefined;
    }

    fn append(self: *@This(), value: Value) !void {
        // A record exceeding the full cap cannot duplicate an admissible
        // retained record. Do not substitute the remaining retained budget.
        if (self.used >= self.values.len) return error.ExtractionRecordLimitExceeded;
        self.values[self.used] = value;
        self.used += 1;
    }

    fn fill(self: *@This(), group: Group, instance: RankedInstance, rank: usize, decisions: ExclusiveDecisions, options: Options) !Record {
        self.used = 0;
        const anchor = if (group.mode == .natural) group.instance_spans[instance.index] else null;
        for (group.fields, self.fields, decisions.fields, 0..) |field, *selection, decision, f| {
            try options.check();
            const first = self.used;
            const nc = field.spans.len;
            const stride = nc + 1;
            const row = self.probabilities[0..stride];
            try probabilitiesForRow(field.assignment_logits[instance.index * stride ..][0..stride], field.scalar, row, options);
            if (group.anchor_field == f) {
                if (anchor) |span| try self.append(.{ .span = span, .probability = instance.probability });
            } else if (field.scalar) {
                var candidate: ?usize = if (decision.chosen.len > 0) decision.chosen[rank] else null;
                if (!field.exclusive and nc > 0) {
                    var best: usize = if (field.allows_absent) 0 else 1;
                    for (1..stride) |column| if (row[column] > row[best]) {
                        best = column;
                    };
                    if (best != 0 and (!field.allows_absent or row[best] >= options.field_threshold)) candidate = best - 1;
                }
                if (candidate) |c| try self.append(.{ .span = field.spans[c], .probability = row[c + 1] });
            } else {
                for (field.spans, 0..) |span, c| {
                    if (c % 256 == 0) try options.check();
                    const probability = row[c + 1];
                    if (field.exclusive) {
                        if (decision.owners[c] != rank) continue;
                    } else if (probability < options.field_threshold) continue;
                    try self.append(.{ .span = span, .probability = probability });
                }
            }
            selection.* = .{ .query_id = field.query_id, .values = self.values[first..self.used] };
        }
        return .{ .source_instance = instance.index, .probability = instance.probability, .anchor = anchor, .fields = self.fields };
    }

    fn retain(self: *const @This(), allocator: std.mem.Allocator, record: Record, options: Options) !Record {
        try options.check();
        const values = try allocator.dupe(Value, self.values[0..self.used]);
        errdefer allocator.free(values);
        const fields = try allocator.alloc(Selection, self.fields.len);
        errdefer allocator.free(fields);
        var cursor: usize = 0;
        for (self.fields, fields) |field, *out| {
            try options.check();
            out.* = .{ .query_id = field.query_id, .values = values[cursor..][0..field.values.len] };
            cursor += field.values.len;
        }
        std.debug.assert(cursor == self.used);
        return .{ .source_instance = record.source_instance, .probability = record.probability, .anchor = record.anchor, .fields = fields };
    }
};

/// Internal score decoder follows upstream's emergency-ABSENT behavior for
/// under-capacity required fields. Serving validates the final record contract
/// separately, after validators and enum resolution, without inventing values.
pub fn decode(allocator: std.mem.Allocator, group: Group, options: Options) !Result {
    try options.check();
    const ni = group.object_logits.len;
    if (ni > options.max_instances or group.fields.len > options.max_fields) return error.ExtractionRecordLimitExceeded;
    if (group.instance_spans.len != ni or group.fields.len == 0) return error.InvalidRecordShape;
    if ((group.mode == .natural and (group.anchor_field == null or group.anchor_field.? >= group.fields.len)) or
        (group.mode != .natural and group.anchor_field != null)) return error.InvalidRecordAnchor;
    if (!std.math.isFinite(options.temperature) or options.temperature <= 0) return error.InvalidDecodeThreshold;
    for ([_]f32{ options.anchor_threshold, options.object_threshold, options.field_threshold }) |threshold| {
        if (!std.math.isFinite(threshold) or threshold < 0 or threshold > 1) return error.InvalidDecodeThreshold;
    }
    var total_scores = ni;
    for (group.fields, 0..) |field, i| {
        try options.check();
        const count = try std.math.mul(usize, ni, try std.math.add(usize, field.spans.len, 1));
        if (field.assignment_logits.len != count) return error.InvalidRecordShape;
        total_scores = try std.math.add(usize, total_scores, try std.math.add(usize, count, field.spans.len));
        if (total_scores > options.max_score_elements) return error.ExtractionRecordLimitExceeded;
        for (group.fields[0..i]) |other| if (field.query_id == other.query_id) return error.DuplicateRecordField;
        for (field.spans) |span| if (span.end <= span.start) return error.InvalidBoundarySpan;
        for (field.assignment_logits, 0..) |logit, index| {
            if (index % 256 == 0) try options.check();
            if (!std.math.isFinite(logit)) return error.NonFiniteBoundaryScore;
        }
    }
    const ranked = try allocator.alloc(RankedInstance, ni);
    defer allocator.free(ranked);
    const threshold = if (group.mode == .anchorless) options.object_threshold else options.anchor_threshold;
    var selected: usize = 0;
    for (group.object_logits, 0..) |logit, i| {
        if (!std.math.isFinite(logit)) return error.NonFiniteBoundaryScore;
        const probability = boundary.sigmoid(logit / options.temperature);
        if (probability >= threshold) {
            ranked[selected] = .{ .index = i, .probability = probability };
            selected += 1;
        }
    }
    std.mem.sort(RankedInstance, ranked[0..selected], {}, RankedInstance.less);
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const a = arena.allocator();
    const records = try a.alloc(Record, selected);
    if (selected == 0) {
        try options.check();
        return .{ .arena = arena, .records = records };
    }
    var decisions = try ExclusiveDecisions.init(allocator, group, ranked[0..selected], options);
    defer decisions.deinit();
    var scratch = try RecordScratch.init(allocator, group, options);
    defer scratch.deinit();
    const keys = try allocator.alloc([32]u8, if (group.mode == .natural) 0 else selected);
    defer allocator.free(keys);
    var kept: usize = 0;
    var retained_values: usize = 0;
    for (ranked[0..selected], 0..) |instance, rank| {
        try options.check();
        const record = try scratch.fill(group, instance, rank, decisions, options);
        if (scratch.used == 0) continue;
        var key: [32]u8 = undefined;
        if (group.mode != .natural) {
            key = try recordKey(scratch.key_spans, record, options);
            var duplicate = false;
            for (keys[0..kept], 0..) |other, index| {
                if (index % 64 == 0) try options.check();
                if (std.mem.eql(u8, &key, &other)) {
                    duplicate = true;
                    break;
                }
            }
            if (duplicate) continue;
        }
        if (scratch.used > options.max_output_values - retained_values) return error.ExtractionRecordLimitExceeded;
        const owned = try scratch.retain(a, record, options);
        try options.check();
        records[kept] = owned;
        if (group.mode != .natural) keys[kept] = key;
        retained_values += scratch.used;
        kept += 1;
    }
    if (group.mode == .natural) std.mem.sort(Record, records[0..kept], {}, struct {
        fn less(_: void, left: Record, right: Record) bool {
            const l = left.anchor orelse return false;
            const r = right.anchor orelse return true;
            if (l.start != r.start) return l.start < r.start;
            return l.end < r.end;
        }
    }.less);
    try options.check();
    return .{ .arena = arena, .records = records[0..kept] };
}

test "gliner boundary records solve exclusive fields globally and preserve natural anchors" {
    const Check = struct {
        fn run(a: std.mem.Allocator) !void {
            const group = Group{
                .mode = .natural,
                .anchor_field = 0,
                .object_logits = &.{ 4, 3 },
                .instance_spans = &.{ Span{ .start = 10, .end = 11 }, Span{ .start = 0, .end = 1 } },
                .fields = &.{
                    .{ .query_id = 0, .spans = &.{}, .assignment_logits = &.{ 0, 0 }, .scalar = true, .allows_absent = false },
                    .{ .query_id = 1, .spans = &.{ .{ .start = 2, .end = 3 }, .{ .start = 12, .end = 13 } }, .assignment_logits = &.{ -10, 5, 4.8, -10, 6, -3 }, .scalar = true, .allows_absent = false, .exclusive = true },
                    .{ .query_id = 2, .spans = &.{ .{ .start = 4, .end = 5 }, .{ .start = 14, .end = 15 } }, .assignment_logits = &.{ 0, 2, 1, 0, 3, 0.5 }, .scalar = false, .allows_absent = true, .exclusive = true },
                },
            };
            var result = try decode(a, group, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 2), result.records.len);
            const first = result.records[0];
            const second = result.records[1];
            try std.testing.expectEqual(@as(usize, 1), first.source_instance);
            try std.testing.expectEqual(@as(usize, 0), first.anchor.?.start);
            try std.testing.expectEqual(@as(usize, 2), first.fields[1].values[0].span.start);
            try std.testing.expectEqual(@as(usize, 12), second.fields[1].values[0].span.start);
            try std.testing.expectEqual(@as(usize, 4), first.fields[2].values[0].span.start);
            try std.testing.expectEqual(@as(usize, 14), second.fields[2].values[0].span.start);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "gliner boundary records deduplicate latent seeds after assignment and enforce output limits" {
    const a = std.testing.allocator;
    var group = Group{
        .mode = .latent,
        .object_logits = &.{ 4, 3 },
        .instance_spans = &.{ Span{ .start = 1, .end = 2 }, Span{ .start = 3, .end = 4 } },
        .fields = &.{.{ .query_id = 4, .spans = &.{.{ .start = 0, .end = 2 }}, .assignment_logits = &.{ -2, 4, -2, 4 }, .scalar = true, .allows_absent = true }},
    };
    var result = try decode(a, group, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.records.len);
    try std.testing.expectEqual(@as(usize, 0), result.records[0].source_instance);
    try std.testing.expect(result.records[0].anchor == null);
    try std.testing.expectError(error.ExtractionRecordLimitExceeded, decode(a, group, .{ .max_output_values = 0 }));
    group.mode = .anchorless;
    var anchorless = try decode(a, group, .{ .object_threshold = 1 });
    defer anchorless.deinit();
    try std.testing.expectEqual(@as(usize, 0), anchorless.records.len);
    group.mode = .natural;
    try std.testing.expectError(error.InvalidRecordAnchor, decode(a, group, .{}));
}

test "gliner boundary records stream more than 65k duplicate values under unchanged retained and scratch limits" {
    const a = std.testing.allocator;
    const Bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator;
    const instances = 384;
    const candidates = 192;
    const stride = candidates + 1;
    const objects = try a.alloc(f32, instances);
    defer a.free(objects);
    @memset(objects, 4);
    objects[7] = 6;
    objects[9] = 6;
    const seeds = try a.alloc(?Span, instances);
    defer a.free(seeds);
    @memset(seeds, null);
    const spans = try a.alloc(Span, candidates);
    defer a.free(spans);
    for (spans, 0..) |*span, index| span.* = .{ .start = index * 2, .end = index * 2 + 1 };
    const absent = try a.alloc(f32, instances * stride);
    defer a.free(absent);
    @memset(absent, -4);
    const present = try a.alloc(f32, instances * stride);
    defer a.free(present);
    @memset(present, 4);
    @memset(present[7 * stride ..][0..stride], 1.25);
    @memset(present[9 * stride ..][0..stride], 3.5);
    const group = Group{
        .mode = .latent,
        .object_logits = objects,
        .instance_spans = seeds,
        .fields = &.{
            .{ .query_id = 9, .spans = spans, .assignment_logits = absent, .scalar = false, .allows_absent = true },
            .{ .query_id = 2, .spans = spans, .assignment_logits = present, .scalar = false, .allows_absent = true },
        },
    };
    // Same expansion shape as the pinned source diagnostic: 73728 selected
    // values, one distinct complete record key, 192 retained values. Inputs
    // belong to the caller and are excluded from the decoder's scratch cap.
    try std.testing.expect(instances * candidates > (Options{}).max_output_values);
    var budget = Bounded{ .backing = a, .limit = 256 * 1024 };
    {
        var result = try decode(budget.allocator(), group, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.records.len);
        const record = result.records[0];
        try std.testing.expectEqual(@as(usize, 7), record.source_instance);
        try std.testing.expectEqual(boundary.sigmoid(@as(f32, 6)), record.probability);
        try std.testing.expect(record.anchor == null);
        try std.testing.expectEqual(@as(usize, 2), record.fields.len);
        try std.testing.expectEqual(@as(usize, 9), record.fields[0].query_id);
        try std.testing.expectEqual(@as(usize, 0), record.fields[0].values.len);
        try std.testing.expectEqual(@as(usize, 2), record.fields[1].query_id);
        try std.testing.expectEqual(@as(usize, candidates), record.fields[1].values.len);
        for (record.fields[1].values, spans) |value, span| {
            try std.testing.expectEqual(span, value.span);
            // The first ranked duplicate owns its field probabilities. Do
            // not replace them with a later duplicate's stronger assignment.
            try std.testing.expectEqual(boundary.sigmoid(@as(f32, 1.25)), value.probability);
        }
    }
    try std.testing.expect(budget.peak > 0 and budget.peak <= budget.limit);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    try std.testing.expect(!budget.denied);
}

const streaming_test_group = Group{
    .mode = .latent,
    .object_logits = &.{ 4, 3, 2 },
    .instance_spans = &.{ null, null, null },
    .fields = &.{.{
        .query_id = 4,
        .spans = &.{ .{ .start = 1, .end = 2 }, .{ .start = 3, .end = 4 } },
        .assignment_logits = &.{ 0, 4, -4, 0, 3, -3, 0, 4, 4 },
        .scalar = false,
        .allows_absent = true,
    }},
};

test "gliner boundary records streaming charges unique values at the exact cap and preserves natural multiplicity" {
    const a = std.testing.allocator;
    for ([_]Mode{ .latent, .anchorless }) |mode| {
        var group = streaming_test_group;
        group.mode = mode;
        var result = try decode(a, group, .{ .max_output_values = 3 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 2), result.records.len);
        try std.testing.expectEqual(@as(usize, 0), result.records[0].source_instance);
        try std.testing.expectEqual(@as(usize, 2), result.records[1].source_instance);
        try std.testing.expectEqual(@as(usize, 1), result.records[0].fields[0].values.len);
        try std.testing.expectEqual(@as(usize, 2), result.records[1].fields[0].values.len);
        try std.testing.expectError(error.ExtractionRecordLimitExceeded, decode(a, group, .{ .max_output_values = 2 }));
        try std.testing.expectError(error.ExtractionRecordLimitExceeded, decode(a, group, .{ .max_output_values = 0 }));

        var fields = [_]Field{group.fields[0]};
        fields[0].assignment_logits = fields[0].assignment_logits[0..6];
        group.fields = &fields;
        group.object_logits = group.object_logits[0..2];
        group.instance_spans = group.instance_spans[0..2];
        // The first unique record fills the cap. Its later duplicate must
        // still fit the independent reusable row buffer and be discarded.
        var full = try decode(a, group, .{ .max_output_values = 1 });
        defer full.deinit();
        try std.testing.expectEqual(@as(usize, 1), full.records.len);
        try std.testing.expectEqual(@as(usize, 0), full.records[0].source_instance);

        fields[0].assignment_logits = streaming_test_group.fields[0].assignment_logits[6..9];
        group.object_logits = group.object_logits[0..1];
        group.instance_spans = group.instance_spans[0..1];
        try std.testing.expectError(error.ExtractionRecordLimitExceeded, decode(a, group, .{ .max_output_values = 1 }));
    }
    const natural = Group{
        .mode = .natural,
        .anchor_field = 0,
        .object_logits = &.{ 4, 3 },
        .instance_spans = &.{ Span{ .start = 10, .end = 11 }, Span{ .start = 10, .end = 11 } },
        .fields = &.{
            .{ .query_id = 0, .spans = &.{}, .assignment_logits = &.{ 0, 0 }, .scalar = true, .allows_absent = false },
            .{ .query_id = 4, .spans = &.{.{ .start = 1, .end = 2 }}, .assignment_logits = &.{ 0, 4, 0, 3 }, .scalar = false, .allows_absent = true },
        },
    };
    var result = try decode(a, natural, .{ .max_output_values = 4 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.records.len);
    for (result.records) |record| {
        try std.testing.expectEqual(natural.instance_spans[0], record.anchor);
        try std.testing.expectEqual(@as(usize, 1), record.fields[0].values.len);
        try std.testing.expectEqual(@as(usize, 1), record.fields[1].values.len);
    }
    try std.testing.expectError(error.ExtractionRecordLimitExceeded, decode(a, natural, .{ .max_output_values = 3 }));
}

const streaming_exclusive_group = Group{
    .mode = .latent,
    .object_logits = &.{ 4, 4, 3 },
    .instance_spans = &.{ null, null, null },
    .fields = &.{
        .{ .query_id = 4, .spans = &.{.{ .start = 0, .end = 1 }}, .assignment_logits = &.{ 0, 4, 0, 3, 0, 2 }, .scalar = false, .allows_absent = true },
        .{ .query_id = 9, .spans = &.{ .{ .start = 2, .end = 3 }, .{ .start = 4, .end = 5 } }, .assignment_logits = &.{ -10, 5, 4.8, -10, 6, -3, -10, -20, -20 }, .scalar = true, .allows_absent = false, .exclusive = true },
        .{ .query_id = 2, .spans = &.{ .{ .start = 6, .end = 7 }, .{ .start = 8, .end = 9 } }, .assignment_logits = &.{ 0, 2, 1, 0, 3, 0.5, 0, -3, 2 }, .scalar = false, .allows_absent = true, .exclusive = true },
    },
};

fn expectStreamingExclusive(allocator: std.mem.Allocator, control: ?Control) !void {
    var result = try decode(allocator, streaming_exclusive_group, .{ .max_output_values = 7, .control = control });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), result.records.len);
    for (result.records, 0..) |record, index| try std.testing.expectEqual(index, record.source_instance);
    // Identical nonexclusive fields do not authorize early deduplication.
    // Global scalar ownership moves the first row to its second candidate;
    // the final row uses the same finite emergency ABSENT as before.
    try std.testing.expectEqual(@as(usize, 4), result.records[0].fields[1].values[0].span.start);
    try std.testing.expectEqual(@as(usize, 2), result.records[1].fields[1].values[0].span.start);
    try std.testing.expectEqual(@as(usize, 0), result.records[2].fields[1].values.len);
    try std.testing.expectEqual(@as(usize, 0), result.records[0].fields[2].values.len);
    try std.testing.expectEqual(@as(usize, 6), result.records[1].fields[2].values[0].span.start);
    try std.testing.expectEqual(@as(usize, 8), result.records[2].fields[2].values[0].span.start);
}

test "gliner boundary records streaming preserves complete global assignments and allocation failure cleanup" {
    const Check = struct {
        fn run(allocator: std.mem.Allocator) !void {
            var result = try decode(allocator, streaming_test_group, .{ .max_output_values = 3 });
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 2), result.records.len);
            try expectStreamingExclusive(allocator, null);
            // A separately owned successful result remains valid across a
            // later decoder's allocation/error cleanup.
            try std.testing.expectEqual(@as(usize, 1), result.records[0].fields[0].values[0].span.start);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
    try std.testing.expectError(error.ExtractionRecordLimitExceeded, decode(std.testing.allocator, streaming_exclusive_group, .{ .max_output_values = 6 }));
}

test "gliner boundary records streaming preserves finite score instance and assignment rejections" {
    const a = std.testing.allocator;
    try std.testing.expectError(error.ExtractionRecordLimitExceeded, decode(a, streaming_test_group, .{ .max_instances = 2 }));
    try std.testing.expectError(error.ExtractionRecordLimitExceeded, decode(a, streaming_test_group, .{ .max_fields = 0 }));
    try std.testing.expectError(error.ExtractionRecordLimitExceeded, decode(a, streaming_test_group, .{ .max_score_elements = 13 }));
    var exact = try decode(a, streaming_test_group, .{ .max_score_elements = 14, .max_output_values = 3 });
    defer exact.deinit();
    try std.testing.expectEqual(@as(usize, 2), exact.records.len);
    try std.testing.expectError(error.ExtractionAssignmentLimitExceeded, decode(a, streaming_exclusive_group, .{ .assignment = .{ .max_dimension = 2 } }));
    try std.testing.expectError(error.ExtractionAssignmentLimitExceeded, decode(a, streaming_exclusive_group, .{ .assignment = .{ .max_work = 0 } }));
    var group = streaming_test_group;
    group.object_logits = &.{ 4, std.math.nan(f32), 2 };
    try std.testing.expectError(error.NonFiniteBoundaryScore, decode(a, group, .{}));
    group.object_logits = &.{ -4, -3, -2 };
    var fields = [_]Field{group.fields[0]};
    fields[0].assignment_logits = &.{ 0, 4, -4, 0, std.math.inf(f32), -3, 0, 4, 4 };
    group.fields = &fields;
    // Non-finite scores are invalid even when no instance would be emitted.
    try std.testing.expectError(error.NonFiniteBoundaryScore, decode(a, group, .{ .max_output_values = 0 }));
}

test "gliner boundary records streaming keeps record key bytes and span multiplicity stable" {
    const original = Record{
        .source_instance = 0,
        .probability = 0.9,
        .anchor = null,
        .fields = &.{
            .{ .query_id = 9, .values = &.{
                .{ .span = .{ .start = 5, .end = 7 }, .probability = 0.6 },
                .{ .span = .{ .start = 1, .end = 3 }, .probability = 0.7 },
                .{ .span = .{ .start = 1, .end = 3 }, .probability = 0.8 },
            } },
            .{ .query_id = 4, .values = &.{} },
            .{ .query_id = 2, .values = &.{.{ .span = .{ .start = 8, .end = 9 }, .probability = 0.2 }} },
        },
    };
    var scratch: [3]Span = undefined;
    const key = try recordKey(&scratch, original, .{});
    // SHA256 of the previous key's little-endian u64 stream:
    // 9,3,1,3,1,3,5,7,2,1,8,9. Field order and duplicate spans matter.
    const hex = std.fmt.bytesToHex(key, .lower);
    try std.testing.expectEqualStrings("3fdce8c7364d5d0fbc42b41634542b60d86d840cbce542e9ed53b200e78e6956", &hex);
    var changed_fields = [_]Selection{
        .{ .query_id = 9, .values = &.{
            .{ .span = .{ .start = 1, .end = 3 }, .probability = 0.1 },
            .{ .span = .{ .start = 5, .end = 7 }, .probability = 0.2 },
            .{ .span = .{ .start = 1, .end = 3 }, .probability = 0.3 },
        } },
        .{ .query_id = 99, .values = &.{} },
        original.fields[2],
    };
    var changed = original;
    changed.source_instance = 12;
    changed.probability = 0.1;
    changed.anchor = .{ .start = 12, .end = 13 };
    changed.fields = &changed_fields;
    try std.testing.expectEqual(key, try recordKey(&scratch, changed, .{}));
    changed_fields[0].values = changed_fields[0].values[0..2];
    try std.testing.expect(!std.mem.eql(u8, &key, &(try recordKey(&scratch, changed, .{}))));
    changed_fields[0] = original.fields[0];
    changed_fields[2].query_id = 3;
    try std.testing.expect(!std.mem.eql(u8, &key, &(try recordKey(&scratch, changed, .{}))));
}

test "gliner boundary records streaming cancellation discards partial retained owners and permits retry" {
    const Bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator;
    const Gate = struct {
        checks: usize = 0,
        stop_at: ?usize = null,
        failure: error{ Cancelled, Timeout } = error.Cancelled,

        fn check(raw: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.checks += 1;
            if (self.stop_at == self.checks) return self.failure;
        }

        fn control(self: *@This()) Control {
            return .{ .ptr = self, .check_fn = check };
        }
    };
    for ([_]Group{ streaming_test_group, streaming_exclusive_group }) |group| {
        var budget = Bounded{ .backing = std.testing.allocator, .limit = 64 * 1024 };
        var baseline = Gate{};
        {
            var result = try decode(budget.allocator(), group, .{ .control = baseline.control() });
            defer result.deinit();
            try std.testing.expect(result.records.len > 0);
        }
        try std.testing.expectEqual(@as(usize, 0), budget.live);
        try std.testing.expect(baseline.checks > 0 and baseline.checks < 2048);
        // Cancel at every observed check, including exclusive ownership,
        // duplicate key lookup, copies after a retained prefix, and return.
        // No callback-count constant or production test hook is required.
        for (1..baseline.checks + 1) |stop_at| {
            var cancelled = Gate{ .stop_at = stop_at };
            try std.testing.expectError(error.Cancelled, decode(budget.allocator(), group, .{ .control = cancelled.control() }));
            try std.testing.expectEqual(@as(usize, 0), budget.live);
            try std.testing.expect(!budget.denied);
        }
        var deadline = Gate{ .stop_at = baseline.checks, .failure = error.Timeout };
        try std.testing.expectError(error.Timeout, decode(budget.allocator(), group, .{ .control = deadline.control() }));
        try std.testing.expectEqual(@as(usize, 0), budget.live);
        {
            var retry = try decode(budget.allocator(), group, .{});
            defer retry.deinit();
            try std.testing.expect(retry.records.len > 0);
        }
        try std.testing.expectEqual(@as(usize, 0), budget.live);
    }
}
