// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const Allocator = std.mem.Allocator;
const algebra = @import("algebra.zig");
const ir = @import("ir.zig");
const law = @import("law.zig");
const symbol = @import("symbol.zig");
const tensor = @import("tensor.zig");
const token = @import("token.zig");

pub const Partial = struct {
    canonical_axis: []const u8,
    metric: []const u8 = "",
    law_id: law.Id,
    value: []const u8,
};

pub fn freePartials(alloc: Allocator, partials: []Partial) void {
    for (partials) |partial| {
        alloc.free(@constCast(partial.canonical_axis));
        if (partial.metric.len > 0) alloc.free(@constCast(partial.metric));
        alloc.free(@constCast(partial.value));
    }
    if (partials.len > 0) alloc.free(partials);
}

pub const Merged = struct {
    canonical_axis: []u8,
    metric: []u8 = &.{},
    law_id: law.Id,
    value: []u8,

    pub fn deinit(self: *Merged, alloc: Allocator) void {
        alloc.free(self.canonical_axis);
        if (self.metric.len > 0) alloc.free(self.metric);
        alloc.free(self.value);
        self.* = undefined;
    }
};

pub const MergeSet = struct {
    rows: []Merged = &.{},

    pub fn deinit(self: *MergeSet, alloc: Allocator) void {
        for (self.rows) |*row| row.deinit(alloc);
        if (self.rows.len > 0) alloc.free(self.rows);
        self.* = .{};
    }
};

pub const BucketPartial = struct {
    canonical_axis: []const u8,
    key_json: []const u8,
    count: i64,
};

pub const MergedBucket = struct {
    canonical_axis: []u8,
    key_json: []u8,
    count: i64,

    pub fn deinit(self: *MergedBucket, alloc: Allocator) void {
        alloc.free(self.canonical_axis);
        alloc.free(self.key_json);
        self.* = undefined;
    }
};

pub const BucketMergeSet = struct {
    buckets: []MergedBucket = &.{},

    pub fn deinit(self: *BucketMergeSet, alloc: Allocator) void {
        for (self.buckets) |*bucket| bucket.deinit(alloc);
        if (self.buckets.len > 0) alloc.free(self.buckets);
        self.* = .{};
    }
};

pub const PartialProtocol = struct {
    owner: []const u8,
    layout: ir.PhysicalLayout,
    fragment: ir.TensorFragment,
    output_dims: []const ir.Dimension = &.{},
    law_id: law.Id,
    metric: []const u8 = "",
    program_id: []const u8 = "",
    expression_id: []const u8 = "",
    dictionary_identity: ?[]const u8 = null,
};

pub const PartialEnvelope = struct {
    protocol: PartialProtocol,
    partial: Partial,
};

pub fn freePartialEnvelopes(alloc: Allocator, envelopes: []PartialEnvelope) void {
    for (envelopes) |envelope| {
        alloc.free(@constCast(envelope.protocol.owner));
        if (envelope.protocol.output_dims.len > 0) alloc.free(@constCast(envelope.protocol.output_dims));
        if (envelope.protocol.metric.len > 0) alloc.free(@constCast(envelope.protocol.metric));
        if (envelope.protocol.program_id.len > 0) alloc.free(@constCast(envelope.protocol.program_id));
        if (envelope.protocol.expression_id.len > 0) alloc.free(@constCast(envelope.protocol.expression_id));
        if (envelope.protocol.dictionary_identity) |identity| alloc.free(@constCast(identity));
        alloc.free(@constCast(envelope.partial.canonical_axis));
        if (envelope.partial.metric.len > 0) alloc.free(@constCast(envelope.partial.metric));
        alloc.free(@constCast(envelope.partial.value));
    }
    if (envelopes.len > 0) alloc.free(envelopes);
}

pub const PartialProtocolRejectReason = enum {
    owner_mismatch,
    layout_mismatch,
    fragment_mismatch,
    missing_output_dimension,
    law_mismatch,
    metric_mismatch,
    program_mismatch,
    expression_mismatch,
    dictionary_mismatch,
};

pub const PartialProtocolProof = union(enum) {
    proven,
    rejected: PartialProtocolRejectReason,

    pub fn safe(self: PartialProtocolProof) bool {
        return self == .proven;
    }
};

pub fn validatePartialProtocol(expected: PartialProtocol, actual: PartialProtocol) PartialProtocolProof {
    if (!std.mem.eql(u8, expected.owner, actual.owner)) return .{ .rejected = .owner_mismatch };
    if (expected.layout != actual.layout) return .{ .rejected = .layout_mismatch };
    if (expected.fragment != actual.fragment) return .{ .rejected = .fragment_mismatch };
    for (expected.output_dims) |dim| {
        if (!containsDimension(actual.output_dims, dim)) return .{ .rejected = .missing_output_dimension };
    }
    if (expected.law_id != actual.law_id) return .{ .rejected = .law_mismatch };
    if (!std.mem.eql(u8, expected.metric, actual.metric)) return .{ .rejected = .metric_mismatch };
    if (!std.mem.eql(u8, expected.program_id, actual.program_id)) return .{ .rejected = .program_mismatch };
    if (!std.mem.eql(u8, expected.expression_id, actual.expression_id)) return .{ .rejected = .expression_mismatch };
    if (expected.dictionary_identity) |expected_dictionary| {
        const actual_dictionary = actual.dictionary_identity orelse return .{ .rejected = .dictionary_mismatch };
        if (!std.mem.eql(u8, expected_dictionary, actual_dictionary)) return .{ .rejected = .dictionary_mismatch };
    }
    return .proven;
}

pub fn mergeValidatedPartialsAlloc(alloc: Allocator, expected: PartialProtocol, envelopes: []const PartialEnvelope) !MergeSet {
    var partials = try alloc.alloc(Partial, envelopes.len);
    defer alloc.free(partials);
    for (envelopes, 0..) |envelope, i| {
        switch (validatePartialProtocol(expected, envelope.protocol)) {
            .proven => {},
            .rejected => return error.InvalidDistributedAlgebraicMerge,
        }
        if (envelope.partial.law_id != envelope.protocol.law_id) return error.InvalidDistributedAlgebraicMerge;
        if (!std.mem.eql(u8, envelope.partial.metric, envelope.protocol.metric)) return error.InvalidDistributedAlgebraicMerge;
        partials[i] = envelope.partial;
    }
    return try mergePartialsAlloc(alloc, partials);
}

const Pending = struct {
    canonical_axis: []u8,
    metric: []u8 = &.{},
    law_id: law.Id,
    value: []u8,

    fn deinit(self: *Pending, alloc: Allocator) void {
        alloc.free(self.canonical_axis);
        if (self.metric.len > 0) alloc.free(self.metric);
        alloc.free(self.value);
        self.* = undefined;
    }
};

const PendingBucket = struct {
    canonical_axis: []u8,
    key_json: []u8,
    count: i64,

    fn deinit(self: *PendingBucket, alloc: Allocator) void {
        alloc.free(self.canonical_axis);
        alloc.free(self.key_json);
        self.* = undefined;
    }
};

pub fn mergePartialsAlloc(alloc: Allocator, partials: []const Partial) !MergeSet {
    var map = std.StringHashMapUnmanaged(Pending).empty;
    defer {
        var it = map.iterator();
        while (it.next()) |entry| {
            alloc.free(entry.key_ptr.*);
            entry.value_ptr.deinit(alloc);
        }
        map.deinit(alloc);
    }
    for (partials) |partial| {
        const merge_key = try partialMergeKeyAlloc(alloc, partial.canonical_axis, partial.metric, partial.law_id);
        var merge_key_owned = true;
        errdefer if (merge_key_owned) alloc.free(merge_key);
        const gop = try map.getOrPut(alloc, merge_key);
        if (gop.found_existing) {
            merge_key_owned = false;
            alloc.free(merge_key);
            if (gop.value_ptr.law_id != partial.law_id) return error.InvalidDistributedAlgebraicMerge;
            const next = try tensor.mergeOneSlotValuesAlloc(alloc, partial.law_id, gop.value_ptr.value, partial.value);
            alloc.free(gop.value_ptr.value);
            gop.value_ptr.value = next;
        } else {
            gop.key_ptr.* = merge_key;
            var inserted = false;
            errdefer if (!inserted) {
                _ = map.remove(merge_key);
            };
            const canonical_axis = try alloc.dupe(u8, partial.canonical_axis);
            errdefer if (!inserted) alloc.free(canonical_axis);
            const metric = try alloc.dupe(u8, partial.metric);
            errdefer if (!inserted) alloc.free(metric);
            const value = try alloc.dupe(u8, partial.value);
            gop.value_ptr.* = .{
                .canonical_axis = canonical_axis,
                .metric = metric,
                .law_id = partial.law_id,
                .value = value,
            };
            merge_key_owned = false;
            inserted = true;
        }
    }
    var rows = std.ArrayListUnmanaged(Merged).empty;
    errdefer {
        for (rows.items) |*row| row.deinit(alloc);
        rows.deinit(alloc);
    }
    var it = map.iterator();
    while (it.next()) |entry| {
        try rows.append(alloc, .{
            .canonical_axis = try alloc.dupe(u8, entry.value_ptr.canonical_axis),
            .metric = try alloc.dupe(u8, entry.value_ptr.metric),
            .law_id = entry.value_ptr.law_id,
            .value = try alloc.dupe(u8, entry.value_ptr.value),
        });
    }
    std.mem.sort(Merged, rows.items, {}, lessMerged);
    return .{ .rows = try rows.toOwnedSlice(alloc) };
}

pub fn mergeBucketCountsAlloc(alloc: Allocator, partials: []const BucketPartial) !BucketMergeSet {
    var map = std.StringHashMapUnmanaged(PendingBucket).empty;
    defer {
        var it = map.iterator();
        while (it.next()) |entry| {
            alloc.free(entry.key_ptr.*);
            entry.value_ptr.deinit(alloc);
        }
        map.deinit(alloc);
    }
    for (partials) |partial| {
        const merge_key = try symbol.shardMergeKeyAlloc(alloc, partial.canonical_axis);
        var merge_key_owned = true;
        errdefer if (merge_key_owned) alloc.free(merge_key);
        const gop = try map.getOrPut(alloc, merge_key);
        if (gop.found_existing) {
            merge_key_owned = false;
            alloc.free(merge_key);
            if (!std.mem.eql(u8, gop.value_ptr.key_json, partial.key_json)) return error.InvalidDistributedAlgebraicMerge;
            gop.value_ptr.count = try mergeBucketCountValues(alloc, gop.value_ptr.count, partial.count);
        } else {
            gop.key_ptr.* = merge_key;
            var inserted = false;
            errdefer if (!inserted) {
                _ = map.remove(merge_key);
            };
            const canonical_axis = try alloc.dupe(u8, partial.canonical_axis);
            errdefer if (!inserted) alloc.free(canonical_axis);
            const key_json = try alloc.dupe(u8, partial.key_json);
            errdefer if (!inserted) alloc.free(key_json);
            gop.value_ptr.* = .{
                .canonical_axis = canonical_axis,
                .key_json = key_json,
                .count = partial.count,
            };
            merge_key_owned = false;
            inserted = true;
        }
    }

    var buckets = std.ArrayListUnmanaged(MergedBucket).empty;
    errdefer {
        for (buckets.items) |*bucket| bucket.deinit(alloc);
        buckets.deinit(alloc);
    }
    var it = map.iterator();
    while (it.next()) |entry| {
        try buckets.append(alloc, .{
            .canonical_axis = try alloc.dupe(u8, entry.value_ptr.canonical_axis),
            .key_json = try alloc.dupe(u8, entry.value_ptr.key_json),
            .count = entry.value_ptr.count,
        });
    }
    std.mem.sort(MergedBucket, buckets.items, {}, lessBucket);
    return .{ .buckets = try buckets.toOwnedSlice(alloc) };
}

fn mergeBucketCountValues(alloc: Allocator, left: i64, right: i64) !i64 {
    const left_text = try algebra.encodeI64Alloc(alloc, left);
    defer alloc.free(left_text);
    const right_text = try algebra.encodeI64Alloc(alloc, right);
    defer alloc.free(right_text);
    const merged_text = try tensor.mergeOneSlotValuesAlloc(alloc, .count, left_text, right_text);
    defer alloc.free(merged_text);
    return try algebra.parseI64(merged_text);
}

fn lessMerged(_: void, lhs: Merged, rhs: Merged) bool {
    const axis_order = std.mem.order(u8, lhs.canonical_axis, rhs.canonical_axis);
    if (axis_order != .eq) return axis_order == .lt;
    const metric_order = std.mem.order(u8, lhs.metric, rhs.metric);
    if (metric_order != .eq) return metric_order == .lt;
    return std.mem.order(u8, @tagName(lhs.law_id), @tagName(rhs.law_id)) == .lt;
}

fn lessBucket(_: void, lhs: MergedBucket, rhs: MergedBucket) bool {
    if (lhs.count == rhs.count) return std.mem.order(u8, lhs.key_json, rhs.key_json) == .lt;
    return lhs.count > rhs.count;
}

pub fn responseAxisKeyAlloc(alloc: Allocator, canonical_axis: []const u8) ![]u8 {
    return try token.canonicalTupleAlloc(alloc, &.{ "axis", canonical_axis });
}

fn partialMergeKeyAlloc(alloc: Allocator, canonical_axis: []const u8, metric: []const u8, law_id: law.Id) ![]u8 {
    const axis_key = try symbol.shardMergeKeyAlloc(alloc, canonical_axis);
    defer alloc.free(axis_key);
    return try token.canonicalTupleAlloc(alloc, &.{ "partial", axis_key, metric, @tagName(law_id) });
}

fn containsDimension(dims: []const ir.Dimension, needle: ir.Dimension) bool {
    for (dims) |dim| {
        if (dim == needle) return true;
    }
    return false;
}

test "distributed partial protocol validation rejects stale shard metadata" {
    const dims = [_]ir.Dimension{ .bucket, .scalar };
    const expected = PartialProtocol{
        .owner = "orders",
        .layout = .materialized_expr,
        .fragment = .merge,
        .output_dims = dims[0..],
        .law_id = .sum,
        .metric = "sum_by_customer",
        .program_id = "program:new",
        .expression_id = "expr:sum-by-customer",
        .dictionary_identity = "dict:customer:v1",
    };

    try std.testing.expect(validatePartialProtocol(expected, expected).safe());

    var wrong_owner = expected;
    wrong_owner.owner = "stale-orders";
    try expectPartialProtocolReject(.owner_mismatch, validatePartialProtocol(expected, wrong_owner));

    var wrong_program = expected;
    wrong_program.program_id = "program:old";
    try expectPartialProtocolReject(.program_mismatch, validatePartialProtocol(expected, wrong_program));

    var wrong_dictionary = expected;
    wrong_dictionary.dictionary_identity = "dict:customer:v0";
    try expectPartialProtocolReject(.dictionary_mismatch, validatePartialProtocol(expected, wrong_dictionary));

    const incomplete_dims = [_]ir.Dimension{.bucket};
    var missing_dim = expected;
    missing_dim.output_dims = incomplete_dims[0..];
    try expectPartialProtocolReject(.missing_output_dimension, validatePartialProtocol(expected, missing_dim));
}

test "distributed validated partial merge rejects protocol and payload disagreement" {
    const alloc = std.testing.allocator;
    const axis = try token.canonicalTupleAlloc(alloc, &.{"customer:alice"});
    defer alloc.free(axis);
    const dims = [_]ir.Dimension{ .bucket, .scalar };
    const protocol = PartialProtocol{
        .owner = "orders",
        .layout = .materialized_expr,
        .fragment = .merge,
        .output_dims = dims[0..],
        .law_id = .sum,
        .metric = "sum_by_customer",
        .program_id = "program:sum-by-customer",
        .expression_id = "expr:sum-by-customer",
    };
    const envelopes = [_]PartialEnvelope{
        .{
            .protocol = protocol,
            .partial = .{ .canonical_axis = axis, .metric = "sum_by_customer", .law_id = .sum, .value = "10" },
        },
        .{
            .protocol = protocol,
            .partial = .{ .canonical_axis = axis, .metric = "sum_by_customer", .law_id = .sum, .value = "7" },
        },
    };
    var merged = try mergeValidatedPartialsAlloc(alloc, protocol, envelopes[0..]);
    defer merged.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), merged.rows.len);
    try std.testing.expectEqualStrings("17", merged.rows[0].value);

    const bad_law_envelopes = [_]PartialEnvelope{.{
        .protocol = protocol,
        .partial = .{ .canonical_axis = axis, .metric = "sum_by_customer", .law_id = .count, .value = "1" },
    }};
    try std.testing.expectError(error.InvalidDistributedAlgebraicMerge, mergeValidatedPartialsAlloc(alloc, protocol, bad_law_envelopes[0..]));

    var stale_protocol = protocol;
    stale_protocol.expression_id = "expr:old";
    const stale_envelopes = [_]PartialEnvelope{.{
        .protocol = stale_protocol,
        .partial = .{ .canonical_axis = axis, .metric = "sum_by_customer", .law_id = .sum, .value = "1" },
    }};
    try std.testing.expectError(error.InvalidDistributedAlgebraicMerge, mergeValidatedPartialsAlloc(alloc, protocol, stale_envelopes[0..]));
}

test "distributed validated envelope merge ignores shard local symbol id assignment" {
    const alloc = std.testing.allocator;
    const axis = try token.canonicalTupleAlloc(alloc, &.{ "tenant:t1", "customer:alice" });
    defer alloc.free(axis);
    const local_a = [_]u8{0x11} ** symbol.id_len;
    const local_b = [_]u8{0x42} ** symbol.id_len;
    const merge_a = try symbol.shardMergeKeyFromResolvedSymbolAlloc(alloc, local_a[0..], axis);
    defer alloc.free(merge_a);
    const merge_b = try symbol.shardMergeKeyFromResolvedSymbolAlloc(alloc, local_b[0..], axis);
    defer alloc.free(merge_b);
    try std.testing.expectEqualStrings(merge_a, merge_b);

    const dims = [_]ir.Dimension{ .bucket, .scalar };
    const protocol = PartialProtocol{
        .owner = "orders",
        .layout = .materialized_expr,
        .fragment = .merge,
        .output_dims = dims[0..],
        .law_id = .sum,
        .metric = "sum_by_customer",
        .program_id = "program:sum-by-customer",
        .expression_id = "expr:sum-by-customer",
        .dictionary_identity = "dict:customer:v1",
    };
    const envelopes = [_]PartialEnvelope{
        .{
            .protocol = protocol,
            .partial = .{ .canonical_axis = axis, .metric = "sum_by_customer", .law_id = .sum, .value = "10" },
        },
        .{
            .protocol = protocol,
            .partial = .{ .canonical_axis = axis, .metric = "sum_by_customer", .law_id = .sum, .value = "7" },
        },
    };
    var merged = try mergeValidatedPartialsAlloc(alloc, protocol, envelopes[0..]);
    defer merged.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), merged.rows.len);
    try std.testing.expectEqualStrings(axis, merged.rows[0].canonical_axis);
    try std.testing.expectEqualStrings("sum_by_customer", merged.rows[0].metric);
    try std.testing.expectEqual(law.Id.sum, merged.rows[0].law_id);
    try std.testing.expectEqualStrings("17", merged.rows[0].value);
}

fn expectPartialProtocolReject(expected: PartialProtocolRejectReason, proof: PartialProtocolProof) !void {
    switch (proof) {
        .rejected => |actual| try std.testing.expectEqual(expected, actual),
        .proven => return error.TestUnexpectedResult,
    }
}

test "distributed merge ignores shard local symbol id assignment" {
    const alloc = std.testing.allocator;
    const axis = try token.canonicalTupleAlloc(alloc, &.{"tenant:t1"});
    defer alloc.free(axis);
    const partials = [_]Partial{
        .{ .canonical_axis = axis, .law_id = .sum, .value = "2.5" },
        .{ .canonical_axis = axis, .law_id = .sum, .value = "7.5" },
    };
    var merged = try mergePartialsAlloc(alloc, partials[0..]);
    defer merged.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), merged.rows.len);
    try std.testing.expectEqualStrings(axis, merged.rows[0].canonical_axis);
    try std.testing.expectEqualStrings("10", merged.rows[0].value);
}

test "distributed merge keeps different laws on same canonical axis separate" {
    const alloc = std.testing.allocator;
    const axis = try token.canonicalTupleAlloc(alloc, &.{"tenant:t1"});
    defer alloc.free(axis);
    const partials = [_]Partial{
        .{ .canonical_axis = axis, .law_id = .sum, .value = "2" },
        .{ .canonical_axis = axis, .law_id = .count, .value = "1" },
    };
    var merged = try mergePartialsAlloc(alloc, partials[0..]);
    defer merged.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), merged.rows.len);
    try std.testing.expectEqualStrings(axis, merged.rows[0].canonical_axis);
    try std.testing.expectEqualStrings("1", merged.rows[0].value);
    try std.testing.expectEqual(law.Id.count, merged.rows[0].law_id);
    try std.testing.expectEqualStrings(axis, merged.rows[1].canonical_axis);
    try std.testing.expectEqualStrings("2", merged.rows[1].value);
    try std.testing.expectEqual(law.Id.sum, merged.rows[1].law_id);
}

test "distributed merge keeps different metrics on same canonical axis separate" {
    const alloc = std.testing.allocator;
    const axis = try token.canonicalTupleAlloc(alloc, &.{"tenant:t1"});
    defer alloc.free(axis);
    const partials = [_]Partial{
        .{ .canonical_axis = axis, .metric = "count_by_tenant", .law_id = .count, .value = "2" },
        .{ .canonical_axis = axis, .metric = "sum_by_tenant", .law_id = .sum, .value = "7.5" },
        .{ .canonical_axis = axis, .metric = "count_by_tenant", .law_id = .count, .value = "3" },
    };
    var merged = try mergePartialsAlloc(alloc, partials[0..]);
    defer merged.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), merged.rows.len);
    try std.testing.expectEqualStrings(axis, merged.rows[0].canonical_axis);
    try std.testing.expectEqualStrings("count_by_tenant", merged.rows[0].metric);
    try std.testing.expectEqualStrings("5", merged.rows[0].value);
    try std.testing.expectEqualStrings(axis, merged.rows[1].canonical_axis);
    try std.testing.expectEqualStrings("sum_by_tenant", merged.rows[1].metric);
    try std.testing.expectEqualStrings("7.5", merged.rows[1].value);
}

test "distributed merge combines aggregate law families by canonical bucket axis" {
    const alloc = std.testing.allocator;
    const day_axis = try token.canonicalTupleAlloc(alloc, &.{"day:2026-05-18"});
    defer alloc.free(day_axis);
    const tenant_axis = try token.canonicalTupleAlloc(alloc, &.{ "tenant:t1", "day:2026-05-18" });
    defer alloc.free(tenant_axis);
    const left_avg = try algebra.encodeAvgAlloc(alloc, .{ .sum = 7, .count = 2 });
    defer alloc.free(left_avg);
    const right_avg = try algebra.encodeAvgAlloc(alloc, .{ .sum = 5, .count = 1 });
    defer alloc.free(right_avg);

    const partials = [_]Partial{
        .{ .canonical_axis = day_axis, .metric = "count_by_day", .law_id = .count, .value = "2" },
        .{ .canonical_axis = day_axis, .metric = "count_by_day", .law_id = .count, .value = "3" },
        .{ .canonical_axis = day_axis, .metric = "sum_by_day", .law_id = .sum, .value = "7.5" },
        .{ .canonical_axis = day_axis, .metric = "sum_by_day", .law_id = .sum, .value = "2.5" },
        .{ .canonical_axis = day_axis, .metric = "sumsquares_by_day", .law_id = .sumsquares, .value = "25" },
        .{ .canonical_axis = day_axis, .metric = "sumsquares_by_day", .law_id = .sumsquares, .value = "9" },
        .{ .canonical_axis = day_axis, .metric = "avg_by_day", .law_id = .avg, .value = left_avg },
        .{ .canonical_axis = day_axis, .metric = "avg_by_day", .law_id = .avg, .value = right_avg },
        .{ .canonical_axis = day_axis, .metric = "min_by_day", .law_id = .min, .value = "9" },
        .{ .canonical_axis = day_axis, .metric = "min_by_day", .law_id = .min, .value = "4" },
        .{ .canonical_axis = day_axis, .metric = "max_by_day", .law_id = .max, .value = "9" },
        .{ .canonical_axis = day_axis, .metric = "max_by_day", .law_id = .max, .value = "14" },
        .{ .canonical_axis = tenant_axis, .metric = "count_by_tenant_day", .law_id = .count, .value = "11" },
    };
    var merged = try mergePartialsAlloc(alloc, partials[0..]);
    defer merged.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 7), merged.rows.len);
    const avg_row = mergedRowByAxisMetricLaw(merged.rows, day_axis, "avg_by_day", .avg) orelse return error.TestUnexpectedResult;
    const avg_state = try algebra.parseAvg(avg_row.value);
    try std.testing.expectEqual(@as(f64, 12), avg_state.sum);
    try std.testing.expectEqual(@as(i64, 3), avg_state.count);
    try std.testing.expectEqualStrings("5", (mergedRowByAxisMetricLaw(merged.rows, day_axis, "count_by_day", .count) orelse return error.TestUnexpectedResult).value);
    try std.testing.expectEqualStrings("4", (mergedRowByAxisMetricLaw(merged.rows, day_axis, "min_by_day", .min) orelse return error.TestUnexpectedResult).value);
    try std.testing.expectEqualStrings("14", (mergedRowByAxisMetricLaw(merged.rows, day_axis, "max_by_day", .max) orelse return error.TestUnexpectedResult).value);
    try std.testing.expectEqualStrings("10", (mergedRowByAxisMetricLaw(merged.rows, day_axis, "sum_by_day", .sum) orelse return error.TestUnexpectedResult).value);
    try std.testing.expectEqualStrings("34", (mergedRowByAxisMetricLaw(merged.rows, day_axis, "sumsquares_by_day", .sumsquares) orelse return error.TestUnexpectedResult).value);
    try std.testing.expectEqualStrings("11", (mergedRowByAxisMetricLaw(merged.rows, tenant_axis, "count_by_tenant_day", .count) orelse return error.TestUnexpectedResult).value);
}

test "distributed merge combines set union lattice partials by canonical axis" {
    const alloc = std.testing.allocator;
    const axis = try token.canonicalTupleAlloc(alloc, &.{ "tenant:t1", "path:tags" });
    defer alloc.free(axis);
    const left = try token.canonicalTupleAlloc(alloc, &.{ "tag:b", "tag:a" });
    defer alloc.free(left);
    const right = try token.canonicalTupleAlloc(alloc, &.{ "tag:c", "tag:a" });
    defer alloc.free(right);

    const partials = [_]Partial{
        .{ .canonical_axis = axis, .metric = "tags_seen", .law_id = .set_union, .value = left },
        .{ .canonical_axis = axis, .metric = "tags_seen", .law_id = .set_union, .value = right },
    };
    var merged = try mergePartialsAlloc(alloc, partials[0..]);
    defer merged.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), merged.rows.len);
    try std.testing.expectEqualStrings(axis, merged.rows[0].canonical_axis);
    try std.testing.expectEqualStrings("tags_seen", merged.rows[0].metric);
    try std.testing.expectEqual(law.Id.set_union, merged.rows[0].law_id);
    const parts = try token.decodeTupleAlloc(alloc, merged.rows[0].value);
    defer {
        for (parts) |part| alloc.free(part);
        alloc.free(parts);
    }
    try std.testing.expectEqual(@as(usize, 3), parts.len);
    try std.testing.expectEqualStrings("tag:a", parts[0]);
    try std.testing.expectEqualStrings("tag:b", parts[1]);
    try std.testing.expectEqualStrings("tag:c", parts[2]);
}

fn mergedRowByAxisMetricLaw(rows: []const Merged, axis: []const u8, metric: []const u8, law_id: law.Id) ?Merged {
    for (rows) |row| {
        if (row.law_id == law_id and
            std.mem.eql(u8, row.canonical_axis, axis) and
            std.mem.eql(u8, row.metric, metric))
        {
            return row;
        }
    }
    return null;
}

test "distributed merge rejects malformed law values" {
    const alloc = std.testing.allocator;
    const axis = try token.canonicalTupleAlloc(alloc, &.{"tenant:t1"});
    defer alloc.free(axis);
    const partials = [_]Partial{
        .{ .canonical_axis = axis, .metric = "sum_by_tenant", .law_id = .sum, .value = "2" },
        .{ .canonical_axis = axis, .metric = "sum_by_tenant", .law_id = .sum, .value = "not-a-number" },
    };
    try std.testing.expectError(error.InvalidCharacter, mergePartialsAlloc(alloc, partials[0..]));
}

test "distributed bucket response merge uses canonical axes rather than shard ids" {
    const alloc = std.testing.allocator;
    const alice_axis = try token.canonicalTupleAlloc(alloc, &.{"customer:alice"});
    defer alloc.free(alice_axis);
    const bob_axis = try token.canonicalTupleAlloc(alloc, &.{"customer:bob"});
    defer alloc.free(bob_axis);

    const partials = [_]BucketPartial{
        .{ .canonical_axis = alice_axis, .key_json = "\"alice\"", .count = 2 },
        .{ .canonical_axis = bob_axis, .key_json = "\"bob\"", .count = 5 },
        .{ .canonical_axis = alice_axis, .key_json = "\"alice\"", .count = 4 },
    };
    var merged = try mergeBucketCountsAlloc(alloc, partials[0..]);
    defer merged.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), merged.buckets.len);
    try std.testing.expectEqualStrings("\"alice\"", merged.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 6), merged.buckets[0].count);
    try std.testing.expectEqualStrings("\"bob\"", merged.buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 5), merged.buckets[1].count);
}

test "distributed bucket response merge uses tensor count law" {
    const alloc = std.testing.allocator;
    const axis = try token.canonicalTupleAlloc(alloc, &.{"customer:alice"});
    defer alloc.free(axis);

    const partials = [_]BucketPartial{
        .{ .canonical_axis = axis, .key_json = "\"alice\"", .count = 2 },
        .{ .canonical_axis = axis, .key_json = "\"alice\"", .count = 3 },
    };
    var merged = try mergeBucketCountsAlloc(alloc, partials[0..]);
    defer merged.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), merged.buckets.len);
    try std.testing.expectEqual(@as(i64, 5), merged.buckets[0].count);
}

test "distributed bucket response merge rejects conflicting key json" {
    const alloc = std.testing.allocator;
    const axis = try token.canonicalTupleAlloc(alloc, &.{"customer:alice"});
    defer alloc.free(axis);
    const partials = [_]BucketPartial{
        .{ .canonical_axis = axis, .key_json = "\"alice\"", .count = 2 },
        .{ .canonical_axis = axis, .key_json = "\"ALICE\"", .count = 4 },
    };
    try std.testing.expectError(error.InvalidDistributedAlgebraicMerge, mergeBucketCountsAlloc(alloc, partials[0..]));
}
