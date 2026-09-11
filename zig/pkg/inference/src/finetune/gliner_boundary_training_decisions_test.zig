// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Capped, test-only copies of actual detached decision views. Replay seals
//! remain untouched: their backend-local continuous inputs are not compared
//! as if floating-point evaluation were bit-identical across devices.
const std = @import("std");
const events = @import("gliner_boundary_training_decisions.zig");
const fixture = @import("../architectures/gliner_boundary_parity_test.zig");
const BoundedAllocator = @import("../runtime/bounded_allocator.zig").BoundedAllocator;
const Allocator = std.mem.Allocator;

pub const TensorName = struct { tensor: []const u8 };
pub const Source = struct {
    pool: struct { indices: TensorName, mask: TensorName, gold_mask: TensorName },
    record_matches: ?[]const []const u8 = null,
    relation_pairs: ?std.json.ArrayHashMap([]const u8) = null,
};
pub const Expected = struct {
    reference: *const fixture.TensorFixture,
    outputs: *const std.json.ArrayHashMap([]const u8),
    source: Source,
    words: usize,
    batch: usize,
    relations: usize,
    pair_cap: usize,
    active_routes: []const usize,
};

fn integer(reference: *const fixture.TensorFixture, key: []const u8, index: usize) !i64 {
    const value = try reference.tensor(key);
    if (value.dtype != .i64 or index >= value.data.len / 8) return error.InvalidFixtureTensor;
    return std.mem.readInt(i64, value.data[index * 8 ..][0..8], .little);
}
fn booleans(a: Allocator, reference: *const fixture.TensorFixture, key: []const u8, actual: []const bool) !void {
    const expected = try reference.booleans(a, key);
    defer a.free(expected);
    try std.testing.expectEqualSlices(bool, expected, actual);
}
fn hardNegatives(a: Allocator, reference: *const fixture.TensorFixture, key: []const u8, actual: []const bool) !void {
    const shape = (try reference.tensor(key)).shape;
    if (shape.len != 3) return error.InvalidFixtureTensor;
    const expected = try reference.booleans(a, key);
    defer a.free(expected);
    try std.testing.expectEqual(expected.len, actual.len);
    const batch: usize = @intCast(shape[0]);
    const candidates: usize = @intCast(shape[1]);
    const queries: usize = @intCast(shape[2]);
    // The source shared-pool mask is [B,C,Q]; native loss inputs are [B,Q,C].
    for (0..batch) |b| for (0..queries) |q| for (0..candidates) |c|
        try std.testing.expectEqual(expected[(b * candidates + c) * queries + q], actual[(b * queries + q) * candidates + c]);
}

const RelationRow = struct {
    query: i32,
    endpoints: [4]i32,
    head_prefix: [2]i32,
    tail_prefix: [2]i32,
    valid: bool,
    label: f32,
    fn less(_: void, lhs: RelationRow, rhs: RelationRow) bool {
        if (lhs.valid != rhs.valid) return lhs.valid;
        if (lhs.query != rhs.query) return lhs.query < rhs.query;
        for (lhs.endpoints, rhs.endpoints) |a, b| if (a != b) return a < b;
        return false;
    }
};

pub const Capture = struct {
    budget: BoundedAllocator,
    records: std.ArrayListUnmanaged([]const u8) = .empty,
    inside_mean: ?[]const f32 = null,
    relation_source_rows: ?[]const usize = null,
    raw_relations: ?[]const u8 = null,
    expected: ?Expected = null,
    matched: usize = 0,

    pub fn init(a: Allocator) Capture {
        return .{ .budget = .{ .backing = a, .limit = 1024 * 1024 } };
    }
    pub fn observer(self: *Capture) events.Observer {
        return .{ .context = self, .observe = observe };
    }
    pub fn deinit(self: *Capture) void {
        const a = self.budget.allocator();
        for (self.records.items) |bytes| a.free(bytes);
        self.records.deinit(a);
        if (self.inside_mean) |values| a.free(values);
        if (self.relation_source_rows) |values| a.free(values);
        if (self.raw_relations) |values| a.free(values);
        std.debug.assert(self.budget.live == 0);
    }
    fn append(self: *Capture, value: anytype) !void {
        if (self.records.items.len >= 256) return error.DecisionTraceLimitExceeded;
        const a = self.budget.allocator();
        const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
        errdefer a.free(bytes);
        try self.records.append(a, bytes);
    }
    fn observe(raw: *anyopaque, event: events.Event) !void {
        const self: *Capture = @ptrCast(@alignCast(raw));
        switch (event) {
            .pool => |pool| {
                if (self.inside_mean != null) return error.UnexpectedDecisionTrace;
                self.inside_mean = try self.budget.allocator().dupe(f32, pool.inside_mean);
                const selection = pool.selection;
                try self.append(.{ .kind = "pool", .batch = selection.batch, .queries = selection.queries, .capacity = selection.capacity, .spans = selection.spans, .valid = selection.valid, .injected = selection.injected, .gold_labels = selection.gold_labels, .starts = pool.starts, .ends = pool.ends });
                if (self.expected) |expected| {
                    const source = expected.source.pool;
                    const shape = (try expected.reference.tensor(source.indices.tensor)).shape;
                    try std.testing.expectEqualSlices(i64, &.{ @intCast(selection.batch), @intCast(selection.capacity), 2 }, shape);
                    for (selection.spans, 0..) |span, i| {
                        try std.testing.expectEqual(try integer(expected.reference, source.indices.tensor, 2 * i), @as(i64, span.start));
                        try std.testing.expectEqual(try integer(expected.reference, source.indices.tensor, 2 * i + 1), @as(i64, span.end));
                    }
                    try booleans(self.budget.allocator(), expected.reference, source.mask.tensor, selection.valid);
                    try booleans(self.budget.allocator(), expected.reference, source.gold_mask.tensor, selection.gold_labels);
                    try fixture.expectFloats(try expected.reference.floats(expected.outputs.map.get("inside_prefix_mean").?), pool.inside_mean, 2e-5, 2e-5);
                }
            },
            .relations => |relation| {
                if (self.raw_relations != null) return error.UnexpectedDecisionTrace;
                const a = self.budget.allocator();
                self.raw_relations = try std.json.Stringify.valueAlloc(a, relation, .{});
                const rows = try a.alloc(RelationRow, relation.mask.len);
                defer a.free(rows);
                for (rows, 0..) |*row, i| row.* = .{ .query = relation.query_indices[i], .endpoints = .{ relation.text_indices[0][i], relation.text_indices[1][i], relation.text_indices[2][i], relation.text_indices[3][i] }, .head_prefix = .{ relation.head_prefix_start[i], relation.head_prefix_end[i] }, .tail_prefix = .{ relation.tail_prefix_start[i], relation.tail_prefix_end[i] }, .valid = relation.mask[i], .label = relation.labels[i] };
                if (self.expected) |expected| {
                    const source = expected.source.relation_pairs orelse return error.MissingFixtureTensor;
                    const source_mask = try expected.reference.booleans(self.budget.allocator(), source.map.get("pair_mask").?);
                    defer self.budget.allocator().free(source_mask);
                    const labels = try expected.reference.booleans(self.budget.allocator(), expected.outputs.map.get("relation_labels").?);
                    defer self.budget.allocator().free(labels);
                    try std.testing.expectEqual(expected.batch * expected.relations * expected.pair_cap, source_mask.len);
                    try std.testing.expectEqual(source_mask.len, labels.len);
                    try std.testing.expectEqual(expected.active_routes.len * expected.pair_cap, relation.mask.len);
                    try std.testing.expectEqual(relation.mask.len, relation.labels.len);
                    const source_rows = try a.alloc(usize, relation.mask.len);
                    self.relation_source_rows = source_rows;
                    @memset(source_rows, std.math.maxInt(usize));
                    var selected: usize = 0;
                    for (source_mask, labels, 0..) |valid, label, i| {
                        const batch = try integer(expected.reference, source.map.get("batch_index").?, i);
                        const relation_index = try integer(expected.reference, source.map.get("relation_index").?, i);
                        const route = i / expected.pair_cap;
                        try std.testing.expectEqual(@as(i64, @intCast(route / expected.relations)), batch);
                        try std.testing.expectEqual(@as(i64, @intCast(route % expected.relations)), relation_index);
                        if (std.mem.indexOfScalar(usize, expected.active_routes, route) == null) try std.testing.expect(!valid);
                        if (!valid) {
                            try std.testing.expect(!label);
                            continue;
                        }
                        const start = batch * @as(i64, @intCast(expected.words));
                        var endpoints: [4]i32 = undefined;
                        inline for (.{ "head_start", "head_end", "tail_start", "tail_end" }, 0..) |field, endpoint| {
                            const position = try integer(expected.reference, source.map.get(field).?, i);
                            endpoints[endpoint] = std.math.cast(i32, start + position - @as(i64, if (endpoint == 1 or endpoint == 3) 1 else 0)) orelse return error.InvalidFixtureTensor;
                        }
                        // Rank order depends on nearly tied FP32 products.
                        // Match every selected semantic pair bijectively;
                        // never accept a missing, duplicated or extra pair.
                        const native = for (rows, 0..) |row, index| {
                            if (row.valid and row.query == route and std.mem.eql(i32, &row.endpoints, &endpoints)) break index;
                        } else {
                            std.debug.print("missing relation source row{d} route{d} endpoints={any}\n", .{ i, route, endpoints });
                            return error.TestExpectedEqual;
                        };
                        try std.testing.expectEqual(std.math.maxInt(usize), source_rows[native]);
                        source_rows[native] = selected;
                        try std.testing.expectEqual(@as(f32, if (label) 1 else 0), relation.labels[native]);
                        try std.testing.expectEqual(@as(i64, @intCast(route)), @as(i64, relation.query_indices[native]));
                        const prefix_base = batch * @as(i64, @intCast(expected.words + 1));
                        inline for (.{ "head_start", "head_end", "tail_start", "tail_end" }, 0..) |field, endpoint| {
                            const position = try integer(expected.reference, source.map.get(field).?, i);
                            const actual = switch (endpoint) {
                                0 => relation.head_prefix_start[native],
                                1 => relation.head_prefix_end[native],
                                2 => relation.tail_prefix_start[native],
                                3 => relation.tail_prefix_end[native],
                                else => unreachable,
                            };
                            try std.testing.expectEqual(prefix_base + position, @as(i64, actual));
                        }
                        selected += 1;
                    }
                    for (rows, source_rows, 0..) |row, source_row, index| {
                        try std.testing.expectEqual(index < selected, row.valid);
                        try std.testing.expectEqual(row.valid, source_row != std.math.maxInt(usize));
                        if (!row.valid) {
                            try std.testing.expectEqual(@as(f32, 0), row.label);
                            try std.testing.expectEqual(@as(i32, 0), row.query);
                            try std.testing.expect(std.mem.allEqual(i32, &row.endpoints, 0));
                            try std.testing.expect(std.mem.allEqual(i32, &row.head_prefix, 0));
                            try std.testing.expect(std.mem.allEqual(i32, &row.tail_prefix, 0));
                        }
                    }
                }
                std.mem.sort(RelationRow, rows, {}, RelationRow.less);
                try self.append(.{ .kind = "relations", .rows = rows });
            },
            .boundary_loss => |loss| {
                try self.append(.{ .kind = "boundary_loss", .pair_query_mask = loss.pair_query_mask, .hard_negative_mask = loss.hard_negative_mask });
                if (self.expected) |expected| {
                    try booleans(self.budget.allocator(), expected.reference, expected.outputs.map.get("pair_query_mask").?, loss.pair_query_mask);
                    try hardNegatives(self.budget.allocator(), expected.reference, expected.outputs.map.get("hard_negative_mask").?, loss.hard_negative_mask);
                }
            },
            .record => |record| {
                const target = record.target;
                const matches = record.matches;
                try self.append(.{ .kind = "record", .group = record.group, .sample = record.sample, .schema_group = record.schema_group, .mode = target.mode, .fields = target.fields, .candidate_spans = target.candidate_spans, .candidate_valid = target.candidate_valid, .field_membership = target.field_membership, .instance_mask = target.instance_mask, .anchor_field = target.anchor_field, .anchor_candidates = target.anchor_candidates, .record_ids = target.record_ids, .record_indices = target.record_indices, .gold_indicator = target.gold_indicator, .matched = matches.pairs, .object_targets = matches.object_targets, .object_mask = matches.object_mask });
                if (self.expected) |expected| {
                    const keys = expected.source.record_matches orelse return error.MissingFixtureTensor;
                    try std.testing.expectEqual(@as(usize, 4), keys.len);
                    for (matches.pairs) |pair| {
                        for ([_]usize{ record.sample, record.schema_group, pair.instance, pair.record }, keys) |index, key|
                            try std.testing.expectEqual(try integer(expected.reference, key, self.matched), @as(i64, @intCast(index)));
                        self.matched += 1;
                    }
                }
            },
        }
    }
    pub fn finish(self: *const Capture) !void {
        if (self.expected) |expected| if (expected.source.record_matches) |keys| {
            try std.testing.expectEqual((try expected.reference.tensor(keys[0])).data.len / 8, self.matched);
        };
    }
};

pub fn expectEqual(expected: *const Capture, actual: *const Capture) !void {
    try std.testing.expect(expected.records.items.len != 0);
    try std.testing.expectEqual(expected.records.items.len, actual.records.items.len);
    for (expected.records.items, actual.records.items, 0..) |want, got, i| {
        errdefer std.debug.print("detached decision event{d}\n", .{i});
        // Full named fields and ordered arrays, not a second digest.
        try std.testing.expectEqualStrings(want, got);
    }
    try std.testing.expectEqual(expected.inside_mean != null, actual.inside_mean != null);
    if (expected.inside_mean) |values| try fixture.expectFloats(values, actual.inside_mean.?, 2e-5, 2e-5);
}

/// Transported dropout changes the retained input identity. Every detached
/// decision must nevertheless keep its exact backend-local ordered bytes.
pub fn expectReplay(expected: *const Capture, actual: *const Capture) !void {
    try expectEqual(expected, actual);
    try std.testing.expectEqual(expected.raw_relations != null, actual.raw_relations != null);
    if (expected.raw_relations) |raw| try std.testing.expectEqualStrings(raw, actual.raw_relations.?);
    if (expected.inside_mean) |values| try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(values), std.mem.sliceAsBytes(actual.inside_mean.?));
    try std.testing.expectEqual(expected.relation_source_rows != null, actual.relation_source_rows != null);
    if (expected.relation_source_rows) |values| try std.testing.expectEqualSlices(usize, values, actual.relation_source_rows.?);
}

pub const EventKind = std.meta.Tag(events.Event);
pub const Failure = struct {
    event: EventKind,
    reached: bool = false,
    pub fn observer(self: *Failure) events.Observer {
        return .{ .context = self, .observe = observe };
    }
    fn observe(raw: *anyopaque, event: events.Event) !void {
        const self: *Failure = @ptrCast(@alignCast(raw));
        if (std.meta.activeTag(event) == self.event) {
            self.reached = true;
            return error.DecisionObserverStopped;
        }
    }
};
