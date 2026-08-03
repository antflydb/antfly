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
const introducer_mod = @import("../../introducer.zig");
const db_mod = @import("../../storage/db/mod.zig");
const db_query_search = @import("../../storage/db/query/search_exec.zig");
const distributed_stats_mod = @import("../../search/distributed_stats.zig");
const json_helpers = @import("../json_helpers.zig");
const regex_mod = @import("../../search/regex.zig");
const search_analysis = @import("../../search/analysis.zig");
const ip_range = @import("ip_range.zig");
const table_read_remote_wire = @import("remote_wire.zig");

const OwnedTextStatsFieldRequest = table_read_remote_wire.OwnedTextStatsFieldRequest;
const OwnedBackgroundTextStatsFieldRequest = table_read_remote_wire.OwnedBackgroundTextStatsFieldRequest;
const algebraic_ir = db_mod.algebraic.ir;
const algebraic_planner = db_mod.algebraic.planner;

const RangeCardinalityPlan = struct {
    kind: db_mod.algebraic.index.CardinalityRangeKind,
    ranges: []db_mod.algebraic.index.CardinalityRangeRequest,
    children: []db_mod.algebraic.index.CardinalityChildRequest,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.ranges) |range| {
            alloc.free(@constCast(range.name));
            if (range.start) |value| alloc.free(@constCast(value));
            if (range.end) |value| alloc.free(@constCast(value));
        }
        if (self.ranges.len > 0) alloc.free(self.ranges);
        if (self.children.len > 0) alloc.free(self.children);
        self.* = undefined;
    }
};

const HistogramCardinalityPlan = struct {
    kind: db_mod.algebraic.index.CardinalityHistogramKind,
    interval: f64 = 0,
    date_bucket: []const u8 = "",
    children: []db_mod.algebraic.index.CardinalityChildRequest,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.date_bucket.len > 0) alloc.free(@constCast(self.date_bucket));
        if (self.children.len > 0) alloc.free(self.children);
        self.* = undefined;
    }
};

fn algebraicTermsCardinalityChildRequestsAlloc(
    alloc: std.mem.Allocator,
    request: db_mod.aggregations.SearchAggregationRequest,
) !?[]db_mod.algebraic.index.CardinalityChildRequest {
    if (!std.mem.eql(u8, request.type, "terms") or request.field.len == 0 or request.background_query != null) return null;
    var count: usize = 0;
    for (request.aggregations) |child| {
        if (db_mod.aggregations.isPipelineAggregation(child.type)) continue;
        if (!std.mem.eql(u8, child.type, "cardinality") or child.field.len == 0) return null;
        count += 1;
    }
    if (count == 0) return null;
    const children = try alloc.alloc(db_mod.algebraic.index.CardinalityChildRequest, count);
    var idx: usize = 0;
    for (request.aggregations) |child| {
        if (db_mod.aggregations.isPipelineAggregation(child.type)) continue;
        children[idx] = .{ .name = child.name, .field = child.field };
        idx += 1;
    }
    return children;
}

pub fn algebraicDistributedTensorProgramForAggregationRequestAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    constraints: []const db_mod.aggregations.FixedConstraint,
    identity_read_generation: ?u64,
) !?algebraic_planner.TensorProgramQueryPlan {
    _ = identity_read_generation;

    const ir_constraints = try algebraicIrConstraintsFromFixedAlloc(alloc, constraints);
    defer if (ir_constraints.len > 0) alloc.free(ir_constraints);

    if (std.mem.eql(u8, request.type, "cardinality")) {
        return try algebraic_planner.planCardinalityPartialsTensorProgramAlloc(alloc, index, request.name, request.field, ir_constraints);
    }
    if (try algebraicTermsCardinalityChildRequestsAlloc(alloc, request)) |children| {
        defer alloc.free(children);
        return try algebraic_planner.planTermsCardinalityPartialsTensorProgramAlloc(alloc, index, request.name, request.field, children, ir_constraints);
    }
    if (try algebraicRangeCardinalityPlanAlloc(alloc, request)) |range_cardinality_value| {
        var range_cardinality = range_cardinality_value;
        defer range_cardinality.deinit(alloc);
        return try algebraic_planner.planRangeCardinalityPartialsTensorProgramAlloc(
            alloc,
            index,
            request.name,
            request.field,
            range_cardinality.kind,
            range_cardinality.ranges,
            range_cardinality.children,
            ir_constraints,
        );
    }
    if (try algebraicHistogramCardinalityPlanAlloc(alloc, request)) |histogram_cardinality_value| {
        var histogram_cardinality = histogram_cardinality_value;
        defer histogram_cardinality.deinit(alloc);
        return try algebraic_planner.planHistogramCardinalityPartialsTensorProgramAlloc(
            alloc,
            index,
            request.name,
            request.field,
            histogram_cardinality.kind,
            histogram_cardinality.interval,
            histogram_cardinality.date_bucket,
            histogram_cardinality.children,
            ir_constraints,
        );
    }
    if (try algebraicDistributedTensorProgramForRequestAlloc(alloc, index, request, constraints)) |plan| {
        return plan;
    }
    const materializations = (try algebraicDistributedMaterializationsForRequestAlloc(alloc, index, request, constraints)) orelse return null;
    defer db_mod.aggregations.freeAlgebraicDistributedMaterializations(alloc, materializations);
    return try algebraic_planner.planMaterializationPartialsTensorProgramAlloc(alloc, index, materializations);
}

fn algebraicHistogramCardinalityPlanAlloc(
    alloc: std.mem.Allocator,
    request: db_mod.aggregations.SearchAggregationRequest,
) !?HistogramCardinalityPlan {
    if (request.field.len == 0 or request.background_query != null) return null;
    const kind: db_mod.algebraic.index.CardinalityHistogramKind = if (std.mem.eql(u8, request.type, "histogram")) blk: {
        if (request.interval <= 0) return null;
        break :blk .numeric;
    } else if (std.mem.eql(u8, request.type, "date_histogram")) blk: {
        if (db_mod.aggregations.algebraicBucketName(request) == null) return null;
        break :blk .date;
    } else return null;

    var child_count: usize = 0;
    for (request.aggregations) |child| {
        if (db_mod.aggregations.isPipelineAggregation(child.type)) continue;
        if (!std.mem.eql(u8, child.type, "cardinality") or child.field.len == 0) return null;
        child_count += 1;
    }
    if (child_count == 0) return null;

    const children = try alloc.alloc(db_mod.algebraic.index.CardinalityChildRequest, child_count);
    var child_idx: usize = 0;
    for (request.aggregations) |child| {
        if (db_mod.aggregations.isPipelineAggregation(child.type)) continue;
        children[child_idx] = .{ .name = child.name, .field = child.field };
        child_idx += 1;
    }
    const date_bucket = if (kind == .date) try alloc.dupe(u8, db_mod.aggregations.algebraicBucketName(request).?) else "";
    errdefer if (date_bucket.len > 0) alloc.free(date_bucket);
    return .{
        .kind = kind,
        .interval = if (kind == .numeric) request.interval else 0,
        .date_bucket = date_bucket,
        .children = children,
    };
}

fn algebraicRangeCardinalityPlanAlloc(
    alloc: std.mem.Allocator,
    request: db_mod.aggregations.SearchAggregationRequest,
) !?RangeCardinalityPlan {
    if (request.field.len == 0 or request.background_query != null or request.distance_ranges.len > 0) return null;
    const kind: db_mod.algebraic.index.CardinalityRangeKind = if (std.mem.eql(u8, request.type, "range")) blk: {
        if (request.ranges.len == 0 or request.date_ranges.len > 0) return null;
        break :blk .numeric;
    } else if (std.mem.eql(u8, request.type, "date_range")) blk: {
        if (request.date_ranges.len == 0 or request.ranges.len > 0) return null;
        break :blk .date;
    } else return null;

    var child_count: usize = 0;
    for (request.aggregations) |child| {
        if (db_mod.aggregations.isPipelineAggregation(child.type)) continue;
        if (!std.mem.eql(u8, child.type, "cardinality") or child.field.len == 0) return null;
        child_count += 1;
    }
    if (child_count == 0) return null;

    const children = try alloc.alloc(db_mod.algebraic.index.CardinalityChildRequest, child_count);
    errdefer if (children.len > 0) alloc.free(children);
    var child_idx: usize = 0;
    for (request.aggregations) |child| {
        if (db_mod.aggregations.isPipelineAggregation(child.type)) continue;
        children[child_idx] = .{ .name = child.name, .field = child.field };
        child_idx += 1;
    }

    const range_count = if (kind == .numeric) request.ranges.len else request.date_ranges.len;
    const ranges = try alloc.alloc(db_mod.algebraic.index.CardinalityRangeRequest, range_count);
    var ranges_initialized: usize = 0;
    errdefer {
        for (ranges[0..ranges_initialized]) |range| {
            alloc.free(@constCast(range.name));
            if (range.start) |value| alloc.free(@constCast(value));
            if (range.end) |value| alloc.free(@constCast(value));
        }
        if (ranges.len > 0) alloc.free(ranges);
    }
    if (kind == .numeric) {
        for (request.ranges, 0..) |range, i| {
            const start_text = if (range.start) |value| try std.fmt.allocPrint(alloc, "{d}", .{value}) else null;
            errdefer if (start_text) |text| alloc.free(text);
            const end_text = if (range.end) |value| try std.fmt.allocPrint(alloc, "{d}", .{value}) else null;
            errdefer if (end_text) |text| alloc.free(text);
            ranges[i] = .{
                .name = try alloc.dupe(u8, range.name),
                .start = start_text,
                .end = end_text,
            };
            ranges_initialized += 1;
        }
    } else {
        for (request.date_ranges, 0..) |range, i| {
            ranges[i] = .{
                .name = try alloc.dupe(u8, range.name),
                .start = if (range.start) |value| try alloc.dupe(u8, value) else null,
                .end = if (range.end) |value| try alloc.dupe(u8, value) else null,
            };
            ranges_initialized += 1;
        }
    }
    return .{
        .kind = kind,
        .ranges = ranges,
        .children = children,
    };
}

fn algebraicDistributedMaterializationsForRequestAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    constraints: []const db_mod.aggregations.FixedConstraint,
) !?[][]const u8 {
    if (std.mem.eql(u8, request.type, "stats")) {
        return try db_mod.aggregations.algebraicDistributedStatsMaterializationsAlloc(alloc, index, request, constraints);
    }
    if (db_mod.algebraic.algebra.Op.parse(request.type) != null) {
        return try db_mod.aggregations.algebraicDistributedMetricMaterializationsAlloc(alloc, index, request, constraints);
    }
    if (std.mem.eql(u8, request.type, "date_histogram")) {
        return try db_mod.aggregations.algebraicDistributedDateHistogramMaterializationsAlloc(alloc, index, request, constraints);
    }
    if (std.mem.eql(u8, request.type, "histogram")) {
        return try db_mod.aggregations.algebraicDistributedHistogramMaterializationsAlloc(alloc, index, request, constraints);
    }
    if (std.mem.eql(u8, request.type, "range")) {
        return try db_mod.aggregations.algebraicDistributedRangeMaterializationsAlloc(alloc, index, request, constraints);
    }
    if (std.mem.eql(u8, request.type, "date_range")) {
        return try db_mod.aggregations.algebraicDistributedDateRangeMaterializationsAlloc(alloc, index, request, constraints);
    }
    if (std.mem.eql(u8, request.type, "terms")) {
        return try db_mod.aggregations.algebraicDistributedTermsMaterializationsAlloc(alloc, index, request, constraints);
    }
    return null;
}

fn algebraicDistributedTensorProgramForRequestAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    constraints: []const db_mod.aggregations.FixedConstraint,
) !?algebraic_planner.TensorProgramQueryPlan {
    if (request.algebraic_join) |join_ref| {
        return try algebraicDistributedJoinTensorProgramForRequestAlloc(alloc, index, request, constraints, join_ref);
    }
    if (db_mod.algebraic.algebra.Op.parse(request.type)) |op| {
        return try algebraic_planner.planMetricTensorProgramAlloc(alloc, index, .{
            .kind = .metric,
            .aggregation_name = request.name,
            .constraints = constraints,
            .metric = .{ .name = request.name, .op = op, .field = request.field },
        });
    }

    if (std.mem.eql(u8, request.type, "terms")) {
        if (request.field.len == 0 or request.background_query != null) return null;
        const bucket_field = index.fieldConfig(request.field, .group) orelse {
            const child_metrics = try algebraicDistributedChildMetricsAlloc(alloc, request.aggregations);
            defer if (child_metrics.len > 0) alloc.free(child_metrics);
            const ir_constraints = try algebraicIrConstraintsFromFixedAlloc(alloc, constraints);
            defer if (ir_constraints.len > 0) alloc.free(ir_constraints);
            if (jsonPointerField(request.field) != null and pathFactChildMetricsSupported(child_metrics)) {
                return try planPathFactTermsTensorProgramAlloc(alloc, index, request, request.field, ir_constraints, child_metrics);
            }
            return null;
        };
        if (algebraicConstraintValueForField(index, constraints, bucket_field.name) != null) return null;
        const child_metrics = try algebraicDistributedChildMetricsAlloc(alloc, request.aggregations);
        defer if (child_metrics.len > 0) alloc.free(child_metrics);
        return try algebraic_planner.planBucketQueryMultiOutputTensorProgramAlloc(alloc, index, .{
            .kind = .terms,
            .aggregation_name = request.name,
            .bucket_field = bucket_field.name,
            .constraints = constraints,
            .child_metrics = child_metrics,
        });
    }

    if (std.mem.eql(u8, request.type, "date_histogram")) {
        if (request.field.len == 0) return null;
        const time_field = index.fieldConfig(request.field, .time) orelse return null;
        const bucket_name = db_mod.aggregations.algebraicBucketName(request) orelse return null;
        const child_metrics = try algebraicDistributedChildMetricsAlloc(alloc, request.aggregations);
        defer if (child_metrics.len > 0) alloc.free(child_metrics);
        return try algebraic_planner.planBucketQueryMultiOutputTensorProgramAlloc(alloc, index, .{
            .kind = .date_histogram,
            .aggregation_name = request.name,
            .time_field = time_field.name,
            .time_bucket = bucket_name,
            .constraints = constraints,
            .child_metrics = child_metrics,
        });
    }

    if (std.mem.eql(u8, request.type, "date_range")) {
        if (request.field.len == 0 or request.date_ranges.len == 0 or request.ranges.len > 0 or request.distance_ranges.len > 0) return null;
        const child_metrics = try algebraicDistributedChildMetricsAlloc(alloc, request.aggregations);
        defer if (child_metrics.len > 0) alloc.free(child_metrics);
        const ir_constraints = try algebraicIrConstraintsFromFixedAlloc(alloc, constraints);
        defer if (ir_constraints.len > 0) alloc.free(ir_constraints);
        if (index.fieldConfig(request.field, .time)) |time_field| {
            if (!docFactChildMetricsSupported(index, child_metrics)) return null;
            return try planDocFactDateRangeTensorProgramAlloc(alloc, index, request, time_field.name, ir_constraints, child_metrics);
        }
        if (jsonPointerField(request.field) != null and pathFactChildMetricsSupported(child_metrics)) {
            return try planPathFactDateRangeTensorProgramAlloc(alloc, index, request, request.field, ir_constraints, child_metrics);
        }
        return null;
    }

    if (std.mem.eql(u8, request.type, "histogram") or std.mem.eql(u8, request.type, "range")) {
        if (request.field.len == 0) return null;
        if (std.mem.eql(u8, request.type, "histogram") and request.interval <= 0) return null;
        if (std.mem.eql(u8, request.type, "range") and (request.ranges.len == 0 or request.date_ranges.len > 0 or request.distance_ranges.len > 0)) return null;
        const bucket_field = index.fieldConfig(request.field, .group) orelse {
            const child_metrics = try algebraicDistributedChildMetricsAlloc(alloc, request.aggregations);
            defer if (child_metrics.len > 0) alloc.free(child_metrics);
            const ir_constraints = try algebraicIrConstraintsFromFixedAlloc(alloc, constraints);
            defer if (ir_constraints.len > 0) alloc.free(ir_constraints);
            if (index.fieldConfig(request.field, .measure)) |measure_field| {
                if (!docFactChildMetricsSupported(index, child_metrics)) return null;
                if (std.mem.eql(u8, request.type, "histogram")) {
                    return try planDocFactHistogramTensorProgramAlloc(alloc, index, request, measure_field.name, ir_constraints, child_metrics);
                }
                if (std.mem.eql(u8, request.type, "range")) {
                    return try planDocFactRangeTensorProgramAlloc(alloc, index, request, measure_field.name, ir_constraints, child_metrics);
                }
            }
            if (jsonPointerField(request.field) != null and pathFactChildMetricsSupported(child_metrics)) {
                if (std.mem.eql(u8, request.type, "histogram")) {
                    return try planPathFactHistogramTensorProgramAlloc(alloc, index, request, request.field, ir_constraints, child_metrics);
                }
                if (std.mem.eql(u8, request.type, "range")) {
                    return try planPathFactRangeTensorProgramAlloc(alloc, index, request, request.field, ir_constraints, child_metrics);
                }
            }
            return null;
        };
        const bucket_kind = db_mod.algebraic.value.kindFromFieldType(bucket_field.type);
        if (bucket_kind != .number and bucket_kind != .integer) return null;
        if (algebraicConstraintValueForField(index, constraints, bucket_field.name) != null) return null;
        const child_metrics = try algebraicDistributedChildMetricsAlloc(alloc, request.aggregations);
        defer if (child_metrics.len > 0) alloc.free(child_metrics);
        return try algebraic_planner.planBucketQueryMultiOutputTensorProgramAlloc(alloc, index, .{
            .kind = .terms,
            .aggregation_name = request.name,
            .bucket_field = bucket_field.name,
            .constraints = constraints,
            .child_metrics = child_metrics,
        });
    }

    return null;
}

fn algebraicDistributedJoinTensorProgramForRequestAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    constraints: []const db_mod.aggregations.FixedConstraint,
    join_ref: algebraic_ir.JoinRef,
) !?algebraic_planner.TensorProgramQueryPlan {
    if (db_mod.algebraic.algebra.Op.parse(request.type)) |op| {
        return try algebraic_planner.planMetricTensorProgramAlloc(alloc, index, .{
            .kind = .metric,
            .aggregation_name = request.name,
            .constraints = constraints,
            .metric = .{ .name = request.name, .op = op, .field = request.field },
            .join = join_ref,
        });
    }

    if (std.mem.eql(u8, request.type, "terms")) {
        if (request.field.len == 0 or request.background_query != null) return null;
        const child_metrics = try algebraicDistributedChildMetricsAlloc(alloc, request.aggregations);
        defer if (child_metrics.len > 0) alloc.free(child_metrics);
        return try algebraic_planner.planBucketQueryMultiOutputTensorProgramAlloc(alloc, index, .{
            .kind = .terms,
            .aggregation_name = request.name,
            .bucket_field = request.field,
            .constraints = constraints,
            .child_metrics = child_metrics,
            .join = join_ref,
        });
    }

    if (std.mem.eql(u8, request.type, "date_histogram")) {
        if (request.field.len == 0) return null;
        const bucket_name = db_mod.aggregations.algebraicBucketName(request) orelse return null;
        const child_metrics = try algebraicDistributedChildMetricsAlloc(alloc, request.aggregations);
        defer if (child_metrics.len > 0) alloc.free(child_metrics);
        return try algebraic_planner.planBucketQueryMultiOutputTensorProgramAlloc(alloc, index, .{
            .kind = .date_histogram,
            .aggregation_name = request.name,
            .time_field = request.field,
            .time_bucket = bucket_name,
            .constraints = constraints,
            .child_metrics = child_metrics,
            .join = join_ref,
        });
    }

    if (std.mem.eql(u8, request.type, "histogram")) {
        if (request.field.len == 0 or request.interval <= 0) return null;
        const child_metrics = try algebraicDistributedChildMetricsAlloc(alloc, request.aggregations);
        defer if (child_metrics.len > 0) alloc.free(child_metrics);
        return try algebraic_planner.planBucketQueryMultiOutputTensorProgramAlloc(alloc, index, .{
            .kind = .histogram,
            .aggregation_name = request.name,
            .bucket_field = request.field,
            .bucket_interval = request.interval,
            .constraints = constraints,
            .child_metrics = child_metrics,
            .join = join_ref,
        });
    }

    if (std.mem.eql(u8, request.type, "range")) {
        if (request.field.len == 0 or request.ranges.len == 0 or request.date_ranges.len > 0 or request.distance_ranges.len > 0) return null;
        return try planDerivedJoinRangeTensorProgramAlloc(alloc, index, request, constraints, join_ref, .numeric);
    }

    if (std.mem.eql(u8, request.type, "date_range")) {
        if (request.field.len == 0 or request.date_ranges.len == 0 or request.ranges.len > 0 or request.distance_ranges.len > 0) return null;
        return try planDerivedJoinRangeTensorProgramAlloc(alloc, index, request, constraints, join_ref, .date);
    }

    return null;
}

fn planDerivedJoinRangeTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    constraints: []const db_mod.aggregations.FixedConstraint,
    join_ref: algebraic_ir.JoinRef,
    kind: db_mod.algebraic.index.DerivedJoinRangeKind,
) !?algebraic_planner.TensorProgramQueryPlan {
    const range_field = algebraicDistributedDerivedJoinRangeField(index, request.field, kind) orelse return null;
    const child_metrics = try algebraicDistributedChildMetricsAlloc(alloc, request.aggregations);
    defer if (child_metrics.len > 0) alloc.free(child_metrics);
    if (kind == .numeric) {
        const ranges = try algebraicNumericRangeBoundsAlloc(alloc, request.ranges);
        defer freeAlgebraicOwnedRangeBounds(alloc, ranges);
        return try algebraic_planner.planDerivedJoinRangeTensorProgramAlloc(alloc, index, request.name, join_ref, range_field, ranges, child_metrics, constraints);
    }
    const ranges = try algebraicDateRangeBoundsAlloc(alloc, request.date_ranges);
    defer if (ranges.len > 0) alloc.free(ranges);
    return try algebraic_planner.planDerivedJoinRangeTensorProgramAlloc(alloc, index, request.name, join_ref, range_field, ranges, child_metrics, constraints);
}

fn algebraicDistributedDerivedJoinRangeField(
    index: *const db_mod.algebraic.index.Index,
    field_name: []const u8,
    kind: db_mod.algebraic.index.DerivedJoinRangeKind,
) ?algebraic_planner.DerivedJoinRangeField {
    var found: ?algebraic_planner.DerivedJoinRangeField = null;
    switch (kind) {
        .numeric => {
            if (index.fieldConfig(field_name, .measure)) |field| {
                const field_kind = db_mod.algebraic.value.kindFromFieldType(field.type);
                if (field_kind == .number or field_kind == .integer) found = .{ .name = field.name, .role = .measure, .kind = kind };
            }
            if (index.fieldConfig(field_name, .group)) |field| {
                const field_kind = db_mod.algebraic.value.kindFromFieldType(field.type);
                if (field_kind == .number or field_kind == .integer) {
                    if (found != null) return null;
                    found = .{ .name = field.name, .role = .group, .kind = kind };
                }
            }
        },
        .date => {
            if (index.fieldConfig(field_name, .time)) |field| {
                const field_kind = db_mod.algebraic.value.kindFromFieldType(field.type);
                if (field_kind == .datetime) found = .{ .name = field.name, .role = .time, .kind = kind };
            }
            if (index.fieldConfig(field_name, .group)) |field| {
                const field_kind = db_mod.algebraic.value.kindFromFieldType(field.type);
                if (field_kind == .datetime) {
                    if (found != null) return null;
                    found = .{ .name = field.name, .role = .group, .kind = kind };
                }
            }
        },
    }
    return found;
}

fn planPathFactTermsTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    bucket_path: []const u8,
    constraints: []const algebraic_ir.Constraint,
    child_metrics: []const algebraic_ir.Metric,
) !?algebraic_planner.TensorProgramQueryPlan {
    return try algebraic_planner.planPathFactTermsTensorProgramAlloc(alloc, index, request.name, bucket_path, constraints, child_metrics);
}

fn algebraicDistributedChildMetricsAlloc(
    alloc: std.mem.Allocator,
    requests: []const db_mod.aggregations.SearchAggregationRequest,
) ![]algebraic_ir.Metric {
    var count: usize = 0;
    for (requests) |request| {
        if (db_mod.aggregations.isPipelineAggregation(request.type)) continue;
        if (std.mem.eql(u8, request.type, "stats")) {
            count += 4;
            continue;
        }
        if (db_mod.algebraic.algebra.Op.parse(request.type) == null) return error.UnsupportedQueryRequest;
        count += 1;
    }
    const out = try alloc.alloc(algebraic_ir.Metric, count);
    errdefer if (out.len > 0) alloc.free(out);
    var filled: usize = 0;
    for (requests) |request| {
        if (db_mod.aggregations.isPipelineAggregation(request.type)) continue;
        if (std.mem.eql(u8, request.type, "stats")) {
            out[filled] = .{ .name = request.name, .op = .avg, .field = request.field };
            out[filled + 1] = .{ .name = request.name, .op = .min, .field = request.field };
            out[filled + 2] = .{ .name = request.name, .op = .max, .field = request.field };
            out[filled + 3] = .{ .name = request.name, .op = .sumsquares, .field = request.field };
            filled += 4;
            continue;
        }
        const op = db_mod.algebraic.algebra.Op.parse(request.type) orelse return error.UnsupportedQueryRequest;
        out[filled] = .{ .name = request.name, .op = op, .field = request.field };
        filled += 1;
    }
    return out;
}

fn docFactChildMetricsSupported(index: *const db_mod.algebraic.index.Index, metrics: []const algebraic_ir.Metric) bool {
    for (metrics) |metric| {
        if (metric.op == .count) continue;
        if (metric.field.len == 0) return false;
        if (index.fieldConfig(metric.field, .measure) == null) return false;
    }
    return true;
}

fn pathFactChildMetricsSupported(metrics: []const algebraic_ir.Metric) bool {
    for (metrics) |metric| {
        if (metric.op == .count) continue;
        if (jsonPointerField(metric.field) == null) return false;
    }
    return true;
}

fn jsonPointerField(field: []const u8) ?[]const u8 {
    if (std.mem.startsWith(u8, field, "/")) return field;
    return null;
}

fn planDocFactHistogramTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    measure_field: []const u8,
    constraints: []const algebraic_ir.Constraint,
    child_metrics: []const algebraic_ir.Metric,
) !?algebraic_planner.TensorProgramQueryPlan {
    return try algebraic_planner.planDocFactHistogramTensorProgramAlloc(alloc, index, request.name, measure_field, request.interval, constraints, child_metrics);
}

fn planDocFactRangeTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    measure_field: []const u8,
    constraints: []const algebraic_ir.Constraint,
    child_metrics: []const algebraic_ir.Metric,
) !?algebraic_planner.TensorProgramQueryPlan {
    const ranges = try algebraicNumericRangeBoundsAlloc(alloc, request.ranges);
    defer freeAlgebraicOwnedRangeBounds(alloc, ranges);
    return try algebraic_planner.planDocFactRangeTensorProgramAlloc(alloc, index, request.name, measure_field, ranges, constraints, child_metrics);
}

fn planDocFactDateRangeTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    time_field: []const u8,
    constraints: []const algebraic_ir.Constraint,
    child_metrics: []const algebraic_ir.Metric,
) !?algebraic_planner.TensorProgramQueryPlan {
    const ranges = try algebraicDateRangeBoundsAlloc(alloc, request.date_ranges);
    defer if (ranges.len > 0) alloc.free(ranges);
    return try algebraic_planner.planDocFactDateRangeTensorProgramAlloc(alloc, index, request.name, time_field, ranges, constraints, child_metrics);
}

fn planPathFactHistogramTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    bucket_path: []const u8,
    constraints: []const algebraic_ir.Constraint,
    child_metrics: []const algebraic_ir.Metric,
) !?algebraic_planner.TensorProgramQueryPlan {
    return try algebraic_planner.planPathFactHistogramTensorProgramAlloc(alloc, index, request.name, bucket_path, request.interval, constraints, child_metrics);
}

fn planPathFactRangeTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    bucket_path: []const u8,
    constraints: []const algebraic_ir.Constraint,
    child_metrics: []const algebraic_ir.Metric,
) !?algebraic_planner.TensorProgramQueryPlan {
    const ranges = try algebraicNumericRangeBoundsAlloc(alloc, request.ranges);
    defer freeAlgebraicOwnedRangeBounds(alloc, ranges);
    return try algebraic_planner.planPathFactRangeTensorProgramAlloc(alloc, index, request.name, bucket_path, ranges, constraints, child_metrics);
}

fn planPathFactDateRangeTensorProgramAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    bucket_path: []const u8,
    constraints: []const algebraic_ir.Constraint,
    child_metrics: []const algebraic_ir.Metric,
) !?algebraic_planner.TensorProgramQueryPlan {
    const ranges = try algebraicDateRangeBoundsAlloc(alloc, request.date_ranges);
    defer if (ranges.len > 0) alloc.free(ranges);
    return try algebraic_planner.planPathFactDateRangeTensorProgramAlloc(alloc, index, request.name, bucket_path, ranges, constraints, child_metrics);
}

fn algebraicNumericRangeBoundsAlloc(
    alloc: std.mem.Allocator,
    ranges: []const db_mod.aggregations.NumericRangeRequest,
) ![]algebraic_planner.RangeBound {
    const out = try alloc.alloc(algebraic_planner.RangeBound, ranges.len);
    @memset(out, .{});
    errdefer freeAlgebraicOwnedRangeBounds(alloc, out);
    for (ranges, 0..) |range, i| {
        out[i] = .{
            .start = if (range.start) |value| try std.fmt.allocPrint(alloc, "{d}", .{value}) else null,
            .end = if (range.end) |value| try std.fmt.allocPrint(alloc, "{d}", .{value}) else null,
        };
    }
    return out;
}

fn algebraicDateRangeBoundsAlloc(
    alloc: std.mem.Allocator,
    ranges: []const db_mod.aggregations.DateRangeRequest,
) ![]algebraic_planner.RangeBound {
    const out = try alloc.alloc(algebraic_planner.RangeBound, ranges.len);
    errdefer alloc.free(out);
    for (ranges, 0..) |range, i| {
        out[i] = .{ .start = range.start, .end = range.end };
    }
    return out;
}

fn freeAlgebraicOwnedRangeBounds(alloc: std.mem.Allocator, ranges: []algebraic_planner.RangeBound) void {
    for (ranges) |range| {
        if (range.start) |value| alloc.free(@constCast(value));
        if (range.end) |value| alloc.free(@constCast(value));
    }
    if (ranges.len > 0) alloc.free(ranges);
}

fn algebraicIrConstraintsFromFixedAlloc(
    alloc: std.mem.Allocator,
    constraints: []const db_mod.aggregations.FixedConstraint,
) ![]algebraic_ir.Constraint {
    const out = try alloc.alloc(algebraic_ir.Constraint, constraints.len);
    errdefer if (out.len > 0) alloc.free(out);
    for (constraints, 0..) |constraint, i| {
        out[i] = .{ .field = constraint.field, .value = constraint.value };
    }
    return out;
}

fn algebraicConstraintValueForField(
    index: *const db_mod.algebraic.index.Index,
    constraints: []const db_mod.aggregations.FixedConstraint,
    field_name: []const u8,
) ?[]const u8 {
    _ = index;
    for (constraints) |constraint| {
        if (std.mem.eql(u8, constraint.field, field_name)) return constraint.value;
    }
    return null;
}

pub fn algebraicAggregationFromDistributedPartialsAlloc(
    alloc: std.mem.Allocator,
    index: *db_mod.algebraic.index.Index,
    request: db_mod.aggregations.SearchAggregationRequest,
    constraints: []const db_mod.aggregations.FixedConstraint,
    merged: db_mod.algebraic.distributed.MergeSet,
) !?db_mod.aggregations.SearchAggregationResult {
    if (std.mem.eql(u8, request.type, "stats")) {
        return try db_mod.aggregations.algebraicStatsAggregationFromDistributedPartialsAlloc(alloc, index, request, constraints, merged);
    }
    if (db_mod.algebraic.algebra.Op.parse(request.type) != null) {
        return try db_mod.aggregations.algebraicMetricAggregationFromDistributedPartialsAlloc(alloc, index, request, constraints, merged);
    }
    if (std.mem.eql(u8, request.type, "cardinality")) {
        return try db_mod.aggregations.algebraicCardinalityAggregationFromDistributedPartialsAlloc(alloc, request, merged);
    }
    if (std.mem.eql(u8, request.type, "date_histogram")) {
        return try db_mod.aggregations.algebraicDateHistogramAggregationFromDistributedPartialsAlloc(alloc, index, request, constraints, merged);
    }
    if (std.mem.eql(u8, request.type, "histogram")) {
        return try db_mod.aggregations.algebraicHistogramAggregationFromDistributedPartialsAlloc(alloc, index, request, constraints, merged);
    }
    if (std.mem.eql(u8, request.type, "range")) {
        return try db_mod.aggregations.algebraicRangeAggregationFromDistributedPartialsAlloc(alloc, index, request, constraints, merged);
    }
    if (std.mem.eql(u8, request.type, "date_range")) {
        return try db_mod.aggregations.algebraicDateRangeAggregationFromDistributedPartialsAlloc(alloc, index, request, constraints, merged);
    }
    if (std.mem.eql(u8, request.type, "terms")) {
        return try db_mod.aggregations.algebraicTermsAggregationFromDistributedPartialsAlloc(alloc, index, request, constraints, merged);
    }
    return null;
}

pub const ParallelFanoutKind = enum {
    text_stats,
    query,
    preflight,
};

pub const FanoutPlanReason = enum {
    no_io,
    single_group,
    small_request,
    parallel,
};

pub const FanoutPlan = struct {
    parallel: bool,
    width: usize,
    reason: FanoutPlanReason,
};

pub const ParallelFanoutMetricsSnapshot = struct {
    text_stats_parallel_total: u64 = 0,
    text_stats_parallel_ns_total: u64 = 0,
    text_stats_fallback_total: u64 = 0,
    text_stats_planned_parallel_total: u64 = 0,
    text_stats_planned_sequential_total: u64 = 0,
    text_stats_planned_width_total: u64 = 0,
    text_stats_plan_no_io_total: u64 = 0,
    text_stats_plan_single_group_total: u64 = 0,
    text_stats_plan_small_request_total: u64 = 0,
    query_parallel_total: u64 = 0,
    query_parallel_ns_total: u64 = 0,
    query_fallback_total: u64 = 0,
    query_planned_parallel_total: u64 = 0,
    query_planned_sequential_total: u64 = 0,
    query_planned_width_total: u64 = 0,
    query_plan_no_io_total: u64 = 0,
    query_plan_single_group_total: u64 = 0,
    query_plan_small_request_total: u64 = 0,
    preflight_parallel_total: u64 = 0,
    preflight_parallel_ns_total: u64 = 0,
    preflight_fallback_total: u64 = 0,
    preflight_planned_parallel_total: u64 = 0,
    preflight_planned_sequential_total: u64 = 0,
    preflight_planned_width_total: u64 = 0,
    preflight_plan_no_io_total: u64 = 0,
    preflight_plan_single_group_total: u64 = 0,
    preflight_plan_small_request_total: u64 = 0,
};

var parallel_text_stats_total: std.atomic.Value(u64) = .init(0);
var parallel_text_stats_ns_total: std.atomic.Value(u64) = .init(0);
var fallback_text_stats_total: std.atomic.Value(u64) = .init(0);
var planned_parallel_text_stats_total: std.atomic.Value(u64) = .init(0);
var planned_sequential_text_stats_total: std.atomic.Value(u64) = .init(0);
var planned_width_text_stats_total: std.atomic.Value(u64) = .init(0);
var planned_no_io_text_stats_total: std.atomic.Value(u64) = .init(0);
var planned_single_group_text_stats_total: std.atomic.Value(u64) = .init(0);
var planned_small_request_text_stats_total: std.atomic.Value(u64) = .init(0);
var parallel_query_total: std.atomic.Value(u64) = .init(0);
var parallel_query_ns_total: std.atomic.Value(u64) = .init(0);
var fallback_query_total: std.atomic.Value(u64) = .init(0);
var planned_parallel_query_total: std.atomic.Value(u64) = .init(0);
var planned_sequential_query_total: std.atomic.Value(u64) = .init(0);
var planned_width_query_total: std.atomic.Value(u64) = .init(0);
var planned_no_io_query_total: std.atomic.Value(u64) = .init(0);
var planned_single_group_query_total: std.atomic.Value(u64) = .init(0);
var planned_small_request_query_total: std.atomic.Value(u64) = .init(0);
var parallel_preflight_total: std.atomic.Value(u64) = .init(0);
var parallel_preflight_ns_total: std.atomic.Value(u64) = .init(0);
var fallback_preflight_total: std.atomic.Value(u64) = .init(0);
var planned_parallel_preflight_total: std.atomic.Value(u64) = .init(0);
var planned_sequential_preflight_total: std.atomic.Value(u64) = .init(0);
var planned_width_preflight_total: std.atomic.Value(u64) = .init(0);
var planned_no_io_preflight_total: std.atomic.Value(u64) = .init(0);
var planned_single_group_preflight_total: std.atomic.Value(u64) = .init(0);
var planned_small_request_preflight_total: std.atomic.Value(u64) = .init(0);

pub fn recordFanoutPlan(kind: ParallelFanoutKind, plan: FanoutPlan) void {
    switch (kind) {
        .text_stats => {
            if (plan.parallel) {
                _ = planned_parallel_text_stats_total.fetchAdd(1, .monotonic);
            } else {
                _ = planned_sequential_text_stats_total.fetchAdd(1, .monotonic);
            }
            _ = planned_width_text_stats_total.fetchAdd(plan.width, .monotonic);
            switch (plan.reason) {
                .no_io => _ = planned_no_io_text_stats_total.fetchAdd(1, .monotonic),
                .single_group => _ = planned_single_group_text_stats_total.fetchAdd(1, .monotonic),
                .small_request => _ = planned_small_request_text_stats_total.fetchAdd(1, .monotonic),
                .parallel => {},
            }
        },
        .query => {
            if (plan.parallel) {
                _ = planned_parallel_query_total.fetchAdd(1, .monotonic);
            } else {
                _ = planned_sequential_query_total.fetchAdd(1, .monotonic);
            }
            _ = planned_width_query_total.fetchAdd(plan.width, .monotonic);
            switch (plan.reason) {
                .no_io => _ = planned_no_io_query_total.fetchAdd(1, .monotonic),
                .single_group => _ = planned_single_group_query_total.fetchAdd(1, .monotonic),
                .small_request => _ = planned_small_request_query_total.fetchAdd(1, .monotonic),
                .parallel => {},
            }
        },
        .preflight => {
            if (plan.parallel) {
                _ = planned_parallel_preflight_total.fetchAdd(1, .monotonic);
            } else {
                _ = planned_sequential_preflight_total.fetchAdd(1, .monotonic);
            }
            _ = planned_width_preflight_total.fetchAdd(plan.width, .monotonic);
            switch (plan.reason) {
                .no_io => _ = planned_no_io_preflight_total.fetchAdd(1, .monotonic),
                .single_group => _ = planned_single_group_preflight_total.fetchAdd(1, .monotonic),
                .small_request => _ = planned_small_request_preflight_total.fetchAdd(1, .monotonic),
                .parallel => {},
            }
        },
    }
}

pub fn recordParallelFanout(kind: ParallelFanoutKind, elapsed_ns: u64) void {
    switch (kind) {
        .text_stats => {
            _ = parallel_text_stats_total.fetchAdd(1, .monotonic);
            _ = parallel_text_stats_ns_total.fetchAdd(elapsed_ns, .monotonic);
        },
        .query => {
            _ = parallel_query_total.fetchAdd(1, .monotonic);
            _ = parallel_query_ns_total.fetchAdd(elapsed_ns, .monotonic);
        },
        .preflight => {
            _ = parallel_preflight_total.fetchAdd(1, .monotonic);
            _ = parallel_preflight_ns_total.fetchAdd(elapsed_ns, .monotonic);
        },
    }
}

pub fn recordParallelFanoutFallback(kind: ParallelFanoutKind) void {
    switch (kind) {
        .text_stats => _ = fallback_text_stats_total.fetchAdd(1, .monotonic),
        .query => _ = fallback_query_total.fetchAdd(1, .monotonic),
        .preflight => _ = fallback_preflight_total.fetchAdd(1, .monotonic),
    }
}

pub fn parallelFanoutMetricsSnapshot() ParallelFanoutMetricsSnapshot {
    return .{
        .text_stats_parallel_total = parallel_text_stats_total.load(.monotonic),
        .text_stats_parallel_ns_total = parallel_text_stats_ns_total.load(.monotonic),
        .text_stats_fallback_total = fallback_text_stats_total.load(.monotonic),
        .text_stats_planned_parallel_total = planned_parallel_text_stats_total.load(.monotonic),
        .text_stats_planned_sequential_total = planned_sequential_text_stats_total.load(.monotonic),
        .text_stats_planned_width_total = planned_width_text_stats_total.load(.monotonic),
        .text_stats_plan_no_io_total = planned_no_io_text_stats_total.load(.monotonic),
        .text_stats_plan_single_group_total = planned_single_group_text_stats_total.load(.monotonic),
        .text_stats_plan_small_request_total = planned_small_request_text_stats_total.load(.monotonic),
        .query_parallel_total = parallel_query_total.load(.monotonic),
        .query_parallel_ns_total = parallel_query_ns_total.load(.monotonic),
        .query_fallback_total = fallback_query_total.load(.monotonic),
        .query_planned_parallel_total = planned_parallel_query_total.load(.monotonic),
        .query_planned_sequential_total = planned_sequential_query_total.load(.monotonic),
        .query_planned_width_total = planned_width_query_total.load(.monotonic),
        .query_plan_no_io_total = planned_no_io_query_total.load(.monotonic),
        .query_plan_single_group_total = planned_single_group_query_total.load(.monotonic),
        .query_plan_small_request_total = planned_small_request_query_total.load(.monotonic),
        .preflight_parallel_total = parallel_preflight_total.load(.monotonic),
        .preflight_parallel_ns_total = parallel_preflight_ns_total.load(.monotonic),
        .preflight_fallback_total = fallback_preflight_total.load(.monotonic),
        .preflight_planned_parallel_total = planned_parallel_preflight_total.load(.monotonic),
        .preflight_planned_sequential_total = planned_sequential_preflight_total.load(.monotonic),
        .preflight_planned_width_total = planned_width_preflight_total.load(.monotonic),
        .preflight_plan_no_io_total = planned_no_io_preflight_total.load(.monotonic),
        .preflight_plan_single_group_total = planned_single_group_preflight_total.load(.monotonic),
        .preflight_plan_small_request_total = planned_small_request_preflight_total.load(.monotonic),
    };
}

fn ioAsyncLimitWidth(io_impl: *std.Io.Threaded, group_count: usize) usize {
    const raw = @intFromEnum(io_impl.async_limit);
    if (raw == 0) return 1;
    if (raw == std.math.maxInt(usize)) return @max(@as(usize, 1), group_count);
    return @max(@as(usize, 1), @min(group_count, raw));
}

pub fn ioAsyncLimitCap(io_impl: *std.Io.Threaded) usize {
    const raw = @intFromEnum(io_impl.async_limit);
    if (raw == 0) return 1;
    if (raw == std.math.maxInt(usize)) return std.math.maxInt(usize);
    return @max(@as(usize, 1), raw);
}

pub fn planFanout(kind: ParallelFanoutKind, io_impl: ?*std.Io.Threaded, group_count: usize) FanoutPlan {
    const attached_io = io_impl orelse return .{
        .parallel = false,
        .width = 1,
        .reason = .no_io,
    };
    if (group_count <= 1) return .{
        .parallel = false,
        .width = 1,
        .reason = .single_group,
    };

    const width_cap = ioAsyncLimitWidth(attached_io, group_count);
    const target_width = switch (kind) {
        .text_stats, .preflight => @min(width_cap, @min(group_count, @as(usize, 4))),
        .query => @min(width_cap, @min(group_count, @as(usize, 4))),
    };
    return .{
        .parallel = target_width > 1,
        .width = if (target_width > 0) target_width else 1,
        .reason = if (target_width > 1) .parallel else .small_request,
    };
}

pub fn planQueryFanout(
    io_impl: ?*std.Io.Threaded,
    group_count: usize,
    req: db_mod.types.SearchRequest,
) FanoutPlan {
    const attached_io = io_impl orelse return .{
        .parallel = false,
        .width = 1,
        .reason = .no_io,
    };
    if (group_count <= 1) return .{
        .parallel = false,
        .width = 1,
        .reason = .single_group,
    };
    if (group_count <= 2 and req.limit > 0 and req.limit <= 32) return .{
        .parallel = false,
        .width = 1,
        .reason = .small_request,
    };

    const width_cap = ioAsyncLimitWidth(attached_io, group_count);
    const result_window = req.limit + req.offset;
    const target_width: usize = if (result_window > 0 and result_window <= 32)
        @min(width_cap, @min(group_count, @as(usize, 4)))
    else
        @min(width_cap, @min(group_count, @as(usize, 8)));
    return .{
        .parallel = target_width > 1,
        .width = if (target_width > 0) target_width else 1,
        .reason = if (target_width > 1) .parallel else .small_request,
    };
}

pub fn queryNeedsDistributedTextStats(req: db_mod.types.SearchRequest) bool {
    if (req.distributed_text_stats.len > 0) return false;
    if (req.full_text != null) return true;
    if (db_query_search.isTextQuery(req.query) and !db_query_search.isDefaultMatchAll(req.query)) return true;
    return req.full_text_queries.len > 0;
}

pub fn collectSignificantTermsFieldRequests(
    alloc: std.mem.Allocator,
    requests: []const db_mod.aggregations.SearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
    text_analysis: *const introducer_mod.TextAnalysisConfig,
) ![]OwnedTextStatsFieldRequest {
    var grouped = std.StringHashMapUnmanaged(std.StringHashMapUnmanaged(void)){};
    defer {
        var it = grouped.iterator();
        while (it.next()) |entry| {
            var term_it = entry.value_ptr.keyIterator();
            while (term_it.next()) |term| alloc.free(term.*);
            entry.value_ptr.deinit(alloc);
            alloc.free(entry.key_ptr.*);
        }
        grouped.deinit(alloc);
    }

    try collectSignificantTermsFieldRequestsRecursive(alloc, &grouped, requests, hits, text_analysis);
    if (grouped.count() == 0) return &.{};

    const out = try alloc.alloc(OwnedTextStatsFieldRequest, grouped.count());
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*item| item.deinit(alloc);
        if (out.len > 0) alloc.free(out);
    }

    var it = grouped.iterator();
    while (it.next()) |entry| {
        const terms = try alloc.alloc([]const u8, entry.value_ptr.count());
        var term_index: usize = 0;
        var term_it = entry.value_ptr.keyIterator();
        while (term_it.next()) |term| {
            terms[term_index] = try alloc.dupe(u8, term.*);
            term_index += 1;
        }
        out[initialized] = .{
            .field = try alloc.dupe(u8, entry.key_ptr.*),
            .terms = terms,
        };
        initialized += 1;
    }
    return out;
}

pub fn collectSignificantTermsBackgroundFieldRequests(
    alloc: std.mem.Allocator,
    requests: []const db_mod.aggregations.SearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
    text_analysis: *const introducer_mod.TextAnalysisConfig,
) ![]OwnedBackgroundTextStatsFieldRequest {
    var out = std.ArrayListUnmanaged(OwnedBackgroundTextStatsFieldRequest).empty;
    errdefer {
        for (out.items) |*item| item.deinit(alloc);
        out.deinit(alloc);
    }
    try collectSignificantTermsBackgroundFieldRequestsRecursive(alloc, &out, requests, hits, text_analysis);
    return try out.toOwnedSlice(alloc);
}

fn collectSignificantTermsBackgroundFieldRequestsRecursive(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(OwnedBackgroundTextStatsFieldRequest),
    requests: []const db_mod.aggregations.SearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
    text_analysis: *const introducer_mod.TextAnalysisConfig,
) !void {
    for (requests) |request| {
        if (std.mem.eql(u8, request.type, "significant_terms") and request.background_query != null) {
            const candidate_limit = try db_mod.aggregations.significantTermsCandidateLimit(@intCast(if (request.size > 0) request.size else 10));
            var seen_terms = std.StringHashMapUnmanaged(void){};
            defer {
                var term_it = seen_terms.keyIterator();
                while (term_it.next()) |term| alloc.free(term.*);
                seen_terms.deinit(alloc);
            }
            const analyzer = try tableAggregationAnalyzerForField(text_analysis, request.field);
            try collectSignificantTermsFromHits(alloc, hits, request.field, analyzer, candidate_limit, &seen_terms);
            if (seen_terms.count() > 0) {
                const terms = try alloc.alloc([]const u8, seen_terms.count());
                var term_index: usize = 0;
                var term_it = seen_terms.keyIterator();
                while (term_it.next()) |term| {
                    terms[term_index] = try alloc.dupe(u8, term.*);
                    term_index += 1;
                }
                try out.append(alloc, .{
                    .aggregation_name = try alloc.dupe(u8, request.name),
                    .field = try alloc.dupe(u8, request.field),
                    .terms = terms,
                    .background_query = try cloneBackgroundQuery(alloc, request.background_query.?),
                });
            }
        }
        try collectSignificantTermsBackgroundFieldRequestsRecursive(alloc, out, request.aggregations, hits, text_analysis);
    }
}

fn cloneBackgroundQuery(
    alloc: std.mem.Allocator,
    query: db_mod.aggregations.BackgroundQuery,
) !db_mod.aggregations.BackgroundQuery {
    return switch (query) {
        .match_all => .{ .match_all = {} },
        .match => |match| .{ .match = .{
            .field = try alloc.dupe(u8, match.field),
            .text = try alloc.dupe(u8, match.text),
        } },
        .term => |term| .{ .term = .{
            .field = try alloc.dupe(u8, term.field),
            .term = try alloc.dupe(u8, term.term),
        } },
    };
}

fn collectSignificantTermsFieldRequestsRecursive(
    alloc: std.mem.Allocator,
    grouped: *std.StringHashMapUnmanaged(std.StringHashMapUnmanaged(void)),
    requests: []const db_mod.aggregations.SearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
    text_analysis: *const introducer_mod.TextAnalysisConfig,
) !void {
    for (requests) |request| {
        if (std.mem.eql(u8, request.type, "significant_terms") and request.background_query == null) {
            const candidate_limit = try db_mod.aggregations.significantTermsCandidateLimit(@intCast(if (request.size > 0) request.size else 10));
            const gop = try grouped.getOrPut(alloc, request.field);
            if (!gop.found_existing) {
                gop.key_ptr.* = try alloc.dupe(u8, request.field);
                gop.value_ptr.* = .{};
            }
            const analyzer = try tableAggregationAnalyzerForField(text_analysis, request.field);
            try collectSignificantTermsFromHits(alloc, hits, request.field, analyzer, candidate_limit, gop.value_ptr);
        }
        try collectSignificantTermsFieldRequestsRecursive(alloc, grouped, request.aggregations, hits, text_analysis);
    }
}

fn collectSignificantTermsFromHits(
    alloc: std.mem.Allocator,
    hits: []const db_mod.types.SearchHit,
    field: []const u8,
    analyzer: *const search_analysis.Analyzer,
    candidate_limit: usize,
    seen_terms: *std.StringHashMapUnmanaged(void),
) !void {
    for (hits) |hit| try collectSignificantTermsFromStoredAlloc(alloc, hit.stored_data orelse continue, field, analyzer, candidate_limit, seen_terms);
}

fn collectSignificantTermsFromStoredAlloc(
    alloc: std.mem.Allocator,
    stored: []const u8,
    field: []const u8,
    analyzer: *const search_analysis.Analyzer,
    candidate_limit: usize,
    seen_terms: *std.StringHashMapUnmanaged(void),
) !void {
    var parsed = (try json_helpers.parseJsonPathValueAlloc(alloc, stored, field)) orelse return;
    defer parsed.deinit();
    try collectSignificantTermsFromValue(alloc, parsed.value, analyzer, candidate_limit, seen_terms);
}

fn collectSignificantTermsFromValue(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    analyzer: *const search_analysis.Analyzer,
    candidate_limit: usize,
    seen_terms: *std.StringHashMapUnmanaged(void),
) !void {
    switch (value) {
        .array => |arr| for (arr.items) |item| try collectSignificantTermsFromValue(alloc, item, analyzer, candidate_limit, seen_terms),
        .string => {
            const tokens = try analyzer.analyze(alloc, value.string);
            defer search_analysis.Analyzer.freeTokens(alloc, tokens);
            for (tokens) |tok| {
                if (!seen_terms.contains(tok.term) and seen_terms.count() >= candidate_limit)
                    return error.QueryCandidateBudgetExceeded;
                const entry = try seen_terms.getOrPut(alloc, tok.term);
                if (entry.found_existing) continue;
                entry.key_ptr.* = try alloc.dupe(u8, tok.term);
            }
        },
        else => {},
    }
}

fn tableAggregationAnalyzerForField(
    cfg: *const introducer_mod.TextAnalysisConfig,
    field: []const u8,
) !*const search_analysis.Analyzer {
    var analyzer_name: ?[]const u8 = null;
    for (cfg.field_analyzers) |item| {
        if (!std.mem.eql(u8, item.field_name, field)) continue;
        if (analyzer_name) |existing| {
            if (!std.mem.eql(u8, existing, item.analyzer_name)) return error.InvalidTableIndexMetadata;
        } else {
            analyzer_name = item.analyzer_name;
        }
    }
    return introducer_mod.resolveAnalyzerName(analyzer_name orelse "standard", cfg.*) orelse
        error.InvalidTableIndexMetadata;
}

pub fn extractJsonValueAtPath(value: std.json.Value, path: []const u8) ?std.json.Value {
    return json_helpers.extractJsonPathValue(value, path);
}

pub const TextStatsFanoutSlot = struct {
    arena: std.heap.ArenaAllocator,
    fields: []const distributed_stats_mod.TextFieldStats = &.{},
    err: ?anyerror = null,

    fn init() TextStatsFanoutSlot {
        return .{
            .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
        };
    }

    fn deinit(self: *TextStatsFanoutSlot) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub const SearchFanoutSlot = struct {
    arena: std.heap.ArenaAllocator,
    result: ?db_mod.types.SearchResult = null,
    err: ?anyerror = null,

    fn init() SearchFanoutSlot {
        return .{
            .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
        };
    }

    fn deinit(self: *SearchFanoutSlot) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub const PreflightFanoutSlot = struct {
    arena: std.heap.ArenaAllocator,
    summary: ?db_mod.RuntimePreflightSummary = null,
    err: ?anyerror = null,

    fn init() PreflightFanoutSlot {
        return .{
            .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
        };
    }

    fn deinit(self: *PreflightFanoutSlot) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub fn initTextStatsFanoutSlots(alloc: std.mem.Allocator, count: usize) ![]TextStatsFanoutSlot {
    const slots = try alloc.alloc(TextStatsFanoutSlot, count);
    errdefer alloc.free(slots);
    for (slots) |*slot| slot.* = .init();
    return slots;
}

pub fn deinitTextStatsFanoutSlots(alloc: std.mem.Allocator, slots: []TextStatsFanoutSlot) void {
    for (slots) |*slot| slot.deinit();
    alloc.free(slots);
}

pub fn initSearchFanoutSlots(alloc: std.mem.Allocator, count: usize) ![]SearchFanoutSlot {
    const slots = try alloc.alloc(SearchFanoutSlot, count);
    errdefer alloc.free(slots);
    for (slots) |*slot| slot.* = .init();
    return slots;
}

pub fn deinitSearchFanoutSlots(alloc: std.mem.Allocator, slots: []SearchFanoutSlot) void {
    for (slots) |*slot| slot.deinit();
    alloc.free(slots);
}

pub fn initPreflightFanoutSlots(alloc: std.mem.Allocator, count: usize) ![]PreflightFanoutSlot {
    const slots = try alloc.alloc(PreflightFanoutSlot, count);
    errdefer alloc.free(slots);
    for (slots) |*slot| slot.* = .init();
    return slots;
}

pub fn deinitPreflightFanoutSlots(alloc: std.mem.Allocator, slots: []PreflightFanoutSlot) void {
    for (slots) |*slot| slot.deinit();
    alloc.free(slots);
}

pub fn cloneRuntimePreflightSummary(
    alloc: std.mem.Allocator,
    summary: db_mod.RuntimePreflightSummary,
) !db_mod.RuntimePreflightSummary {
    var cloned: db_mod.RuntimePreflightSummary = .{};
    errdefer cloned.deinit(alloc);
    try mergeRuntimePreflightSummaryNoFree(alloc, &cloned, summary);
    return cloned;
}

pub fn mergeRuntimePreflightSummary(
    alloc: std.mem.Allocator,
    target: *db_mod.RuntimePreflightSummary,
    extra: db_mod.RuntimePreflightSummary,
) !void {
    defer {
        var owned = extra;
        owned.deinit(alloc);
    }

    try mergeRuntimePreflightSummaryNoFree(alloc, target, extra);
}

pub fn mergeRuntimePreflightSummaryNoFree(
    alloc: std.mem.Allocator,
    target: *db_mod.RuntimePreflightSummary,
    extra: db_mod.RuntimePreflightSummary,
) !void {
    try mergeRuntimePreflightStrings(alloc, &target.result_refs, extra.result_refs);
    try mergeRuntimePreflightStrings(alloc, &target.graph_query_order, extra.graph_query_order);
    try mergeRuntimePreflightTextEstimates(alloc, &target.text_indexes, extra.text_indexes);
    try mergeRuntimePreflightEmbeddingEstimates(alloc, &target.embedding_indexes, extra.embedding_indexes);
    try mergeRuntimePreflightGraphEstimates(alloc, &target.graph_indexes, extra.graph_indexes);
    try mergeRuntimePreflightTextQueryStats(alloc, &target.text_query_stats, extra.text_query_stats);
    target.doc_id_value_count = @max(target.doc_id_value_count, extra.doc_id_value_count);
    target.filter_id_count = @max(target.filter_id_count, extra.filter_id_count);
    target.exclude_id_count = @max(target.exclude_id_count, extra.exclude_id_count);
    target.numeric_range_clause_count = @max(target.numeric_range_clause_count, extra.numeric_range_clause_count);
    target.term_range_clause_count = @max(target.term_range_clause_count, extra.term_range_clause_count);
    target.ip_range_clause_count = @max(target.ip_range_clause_count, extra.ip_range_clause_count);
    target.bool_field_clause_count = @max(target.bool_field_clause_count, extra.bool_field_clause_count);
    target.geo_filter_clause_count = @max(target.geo_filter_clause_count, extra.geo_filter_clause_count);
    target.positive_id_result_upper_bound = if (target.positive_id_result_upper_bound) |existing|
        if (extra.positive_id_result_upper_bound) |incoming|
            @min(existing, incoming)
        else
            existing
    else
        extra.positive_id_result_upper_bound;
    const target_pre_merge_lower_bound = if (target.structured_filter_doc_count_lower_bound) |value|
        value
    else if (target.structured_filter_count_exact)
        target.structured_filter_doc_count_estimate
    else
        target.structured_filter_doc_count_estimate;
    const extra_pre_merge_lower_bound = if (extra.structured_filter_doc_count_lower_bound) |value|
        value
    else if (extra.structured_filter_count_exact)
        extra.structured_filter_doc_count_estimate
    else
        extra.structured_filter_doc_count_estimate;
    if (target.structured_filter_count_exact and extra.structured_filter_count_exact) {
        if (target.structured_filter_doc_count_estimate) |existing| {
            if (extra.structured_filter_doc_count_estimate) |incoming| {
                target.structured_filter_doc_count_estimate = existing + incoming;
                target.structured_filter_count_exact = true;
            } else {
                target.structured_filter_doc_count_estimate = null;
                target.structured_filter_count_exact = false;
            }
        } else if (extra.structured_filter_doc_count_estimate) |incoming| {
            target.structured_filter_doc_count_estimate = incoming;
            target.structured_filter_count_exact = true;
        } else {
            target.structured_filter_doc_count_estimate = null;
            target.structured_filter_count_exact = false;
        }
    } else {
        target.structured_filter_doc_count_estimate = null;
        target.structured_filter_count_exact = false;
    }
    target.structured_filter_doc_count_sample_estimate = if (target.structured_filter_doc_count_sample_estimate) |existing|
        if (extra.structured_filter_doc_count_sample_estimate) |incoming|
            existing + incoming
        else
            existing
    else
        extra.structured_filter_doc_count_sample_estimate;
    target.structured_filter_count_sample_size += extra.structured_filter_count_sample_size;
    if (target.structured_filter_count_exact) {
        target.structured_filter_doc_count_sample_estimate = null;
        target.structured_filter_count_sample_size = 0;
    }
    if (target.structured_filter_count_exact) {
        target.structured_filter_doc_count_lower_bound = null;
    } else {
        target.structured_filter_doc_count_lower_bound = if (target_pre_merge_lower_bound != null or extra_pre_merge_lower_bound != null)
            (target_pre_merge_lower_bound orelse 0) + (extra_pre_merge_lower_bound orelse 0)
        else
            null;
    }
    target.structured_filter_count_budget_limit = if (target.structured_filter_count_budget_limit) |existing|
        if (extra.structured_filter_count_budget_limit) |incoming|
            @max(existing, incoming)
        else
            existing
    else
        extra.structured_filter_count_budget_limit;
    target.shard_result_window = @max(target.shard_result_window, extra.shard_result_window);
    target.shard_result_window_total += extra.shard_result_window_total;
    target.stored_projection_doc_upper_bound_total += extra.stored_projection_doc_upper_bound_total;
    target.rerank_doc_upper_bound = @max(target.rerank_doc_upper_bound, extra.rerank_doc_upper_bound);
    target.aggregation_may_scan_full_results = target.aggregation_may_scan_full_results or extra.aggregation_may_scan_full_results;
    target.shard_count += extra.shard_count;
    target.remote_shard_count += extra.remote_shard_count;
    target.dense_query_count += extra.dense_query_count;
    target.vector_worker_candidate_count += extra.vector_worker_candidate_count;
    target.vector_worker_fallback_count += extra.vector_worker_fallback_count;
    target.vector_worker_filter_constraint_count += extra.vector_worker_filter_constraint_count;
    target.vector_worker_requires_algebraic_filter_resolution = target.vector_worker_requires_algebraic_filter_resolution or
        extra.vector_worker_requires_algebraic_filter_resolution;
    target.dense_effective_k_total += extra.dense_effective_k_total;
    target.dense_search_width_total += extra.dense_search_width_total;
    target.dense_search_width_max = @max(target.dense_search_width_max, extra.dense_search_width_max);
    target.dense_epsilon_max = @max(target.dense_epsilon_max, extra.dense_epsilon_max);
    db_mod.deriveRuntimePreflightEstimates(target);
}

pub fn algebraicConstraintsForRequestAlloc(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
) !?[]db_mod.aggregations.FixedConstraint {
    var out = std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint).empty;
    errdefer freeAlgebraicConstraints(alloc, out.items);

    switch (req.query) {
        .match_all => {},
        .term => |term| {
            if (!std.mem.startsWith(u8, term.field, "/")) return null;
            const value_text = db_mod.algebraic.token.canonicalTupleAlloc(alloc, &.{ "string", term.term }) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, term.field, value_text) catch return null;
        },
        .match => |match| {
            if (!std.mem.startsWith(u8, match.field, "/") or match.analyzer != null or match.text.len == 0) return null;
            const value_text = db_mod.algebraic.index.pathFactStringMatchConstraintValueAlloc(alloc, match.text) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, match.field, value_text) catch return null;
        },
        .fuzzy => |fuzzy| {
            if (!std.mem.startsWith(u8, fuzzy.field, "/") or fuzzy.auto_fuzzy or fuzzy.prefix_len == 0) return null;
            const prefix_len: usize = @intCast(fuzzy.prefix_len);
            if (fuzzy.term.len < prefix_len) return null;
            const value_text = db_mod.algebraic.index.pathFactStringFuzzyConstraintValueAlloc(
                alloc,
                fuzzy.term,
                fuzzy.max_edits,
                fuzzy.prefix_len,
            ) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, fuzzy.field, value_text) catch return null;
        },
        .prefix => |prefix| {
            if (!std.mem.startsWith(u8, prefix.field, "/")) return null;
            const value_text = db_mod.algebraic.index.pathFactStringPrefixConstraintValueAlloc(alloc, prefix.prefix) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, prefix.field, value_text) catch return null;
        },
        .wildcard => |wildcard| {
            if (!std.mem.startsWith(u8, wildcard.field, "/")) return null;
            const literal_prefix = algebraicWildcardLiteralPrefix(wildcard.pattern);
            if (literal_prefix.len == 0 and algebraicWildcardPatternHasMeta(wildcard.pattern)) return null;
            const value_text = db_mod.algebraic.index.pathFactStringWildcardConstraintValueAlloc(alloc, wildcard.pattern) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, wildcard.field, value_text) catch return null;
        },
        .regexp => |regexp| {
            if (!std.mem.startsWith(u8, regexp.field, "/")) return null;
            if (algebraicRegexpLiteralPrefix(regexp.pattern).len == 0) return null;
            var compiled = regex_mod.compile(alloc, regexp.pattern) catch return null;
            defer compiled.deinit();
            const value_text = db_mod.algebraic.index.pathFactStringRegexpConstraintValueAlloc(alloc, regexp.pattern) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, regexp.field, value_text) catch return null;
        },
        .bool_field => |field| {
            const value_text = algebraicConstraintBoolValueAlloc(alloc, field.field, field.value) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, field.field, value_text) catch return null;
        },
        .numeric_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/")) return null;
            const value_text = db_mod.algebraic.index.pathFactNumericRangeConstraintValueAlloc(
                alloc,
                range.min,
                range.max,
                range.inclusive_min,
                range.inclusive_max,
            ) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, range.field, value_text) catch return null;
        },
        .term_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/") or (range.min == null and range.max == null)) return null;
            const value_text = db_mod.algebraic.index.pathFactTermRangeConstraintValueAlloc(
                alloc,
                range.min,
                range.max,
                range.inclusive_min,
                range.inclusive_max,
            ) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, range.field, value_text) catch return null;
        },
        .ip_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/") or !algebraicValidIpRange(range.cidr)) return null;
            const value_text = db_mod.algebraic.index.pathFactIpRangeConstraintValueAlloc(alloc, range.cidr) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, range.field, value_text) catch return null;
        },
        .geo_bbox => |bbox| {
            if (!std.mem.startsWith(u8, bbox.field, "/") or !algebraicValidGeoBBox(bbox.min_lat, bbox.min_lon, bbox.max_lat, bbox.max_lon)) return null;
            const value_text = db_mod.algebraic.index.pathFactGeoBBoxConstraintValueAlloc(
                alloc,
                bbox.min_lat,
                bbox.min_lon,
                bbox.max_lat,
                bbox.max_lon,
            ) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, bbox.field, value_text) catch return null;
        },
        .geo_distance => |distance| {
            if (!std.mem.startsWith(u8, distance.field, "/") or !algebraicValidGeoDistance(distance.lat, distance.lon, distance.radius_meters)) return null;
            const value_text = db_mod.algebraic.index.pathFactGeoDistanceConstraintValueAlloc(
                alloc,
                distance.lat,
                distance.lon,
                distance.radius_meters,
            ) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, distance.field, value_text) catch return null;
        },
        .geo_shape => |shape| {
            if (!std.mem.startsWith(u8, shape.field, "/") or !algebraicGeoShapeRelationSupported(shape.relation)) return null;
            const value_text = db_mod.algebraic.index.pathFactGeoShapeConstraintValueAlloc(
                alloc,
                @tagName(shape.relation),
                shape.polygons,
            ) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, shape.field, value_text) catch return null;
        },
        .date_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/")) return null;
            const start_text = if (range.start_ns) |ns| std.fmt.allocPrint(alloc, "{d}", .{ns}) catch return null else null;
            defer if (start_text) |value| alloc.free(value);
            const end_text = if (range.end_ns) |ns| std.fmt.allocPrint(alloc, "{d}", .{ns}) catch return null else null;
            defer if (end_text) |value| alloc.free(value);
            const value_text = db_mod.algebraic.index.pathFactDateRangeConstraintValueAlloc(
                alloc,
                start_text,
                end_text,
                range.inclusive_start,
                range.inclusive_end,
            ) catch return null;
            defer alloc.free(value_text);
            appendAlgebraicConstraint(&out, alloc, range.field, value_text) catch return null;
        },
        else => return null,
    }

    if (req.full_text) |text_query| {
        if (!(collectAlgebraicTextQueryConstraints(alloc, text_query, &out) catch return null)) return null;
    }

    if (req.filter_query_json.len > 0) {
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, req.filter_query_json, .{}) catch return null;
        defer parsed.deinit();
        if (!(collectAlgebraicFilterConstraints(alloc, parsed.value, &out) catch return null)) return null;
    }

    return try out.toOwnedSlice(alloc);
}

pub fn freeAlgebraicConstraints(
    alloc: std.mem.Allocator,
    constraints: []db_mod.aggregations.FixedConstraint,
) void {
    for (constraints) |constraint| {
        alloc.free(@constCast(constraint.field));
        alloc.free(@constCast(constraint.value));
    }
    if (constraints.len > 0) alloc.free(constraints);
}

const AlgebraicConstraintCollectError = std.mem.Allocator.Error || error{UnsupportedQueryRequest};

fn collectAlgebraicTextBoolQueryConstraints(
    alloc: std.mem.Allocator,
    bool_query: db_mod.types.TextBoolQuery,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    if (bool_query.must_not.len > 0) return false;
    if (bool_query.should.len > 0) {
        if (bool_query.must.len > 0) {
            if (bool_query.min_should != 0) return false;
        } else {
            if (bool_query.min_should != 0 and bool_query.min_should != 1) return false;
            return try collectAlgebraicTextShouldTermConstraint(alloc, bool_query.should, out);
        }
    }
    if (bool_query.min_should > 0) return false;
    for (bool_query.must) |query| {
        if (!(try collectAlgebraicTextQueryConstraints(alloc, query, out))) return false;
    }
    return true;
}

fn collectAlgebraicTextShouldTermConstraint(
    alloc: std.mem.Allocator,
    queries: []const db_mod.types.TextQuery,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    if (queries.len == 0) return false;
    var field: ?[]const u8 = null;
    const typed_values = try alloc.alloc([]const u8, queries.len);
    defer alloc.free(typed_values);
    var initialized: usize = 0;
    defer {
        for (typed_values[0..initialized]) |value| alloc.free(@constCast(value));
    }

    for (queries, 0..) |query, i| {
        const term = switch (query) {
            .term => |term| term,
            else => return false,
        };
        if (!std.mem.startsWith(u8, term.field, "/")) return false;
        if (field) |existing| {
            if (!std.mem.eql(u8, existing, term.field)) return false;
        } else {
            field = term.field;
        }
        typed_values[i] = try db_mod.algebraic.token.canonicalTupleAlloc(alloc, &.{ "string", term.term });
        initialized += 1;
    }

    try appendAlgebraicTypedAnyConstraint(out, alloc, field orelse return false, typed_values);
    return true;
}

fn collectAlgebraicTextQueryConstraints(
    alloc: std.mem.Allocator,
    query: db_mod.types.TextQuery,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    switch (query) {
        .match_all => return true,
        .term => |term| {
            if (!std.mem.startsWith(u8, term.field, "/")) return false;
            const value_text = try db_mod.algebraic.token.canonicalTupleAlloc(alloc, &.{ "string", term.term });
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, term.field, value_text);
            return true;
        },
        .match => |match| {
            if (!std.mem.startsWith(u8, match.field, "/") or match.analyzer != null or match.text.len == 0) return false;
            const value_text = try db_mod.algebraic.index.pathFactStringMatchConstraintValueAlloc(alloc, match.text);
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, match.field, value_text);
            return true;
        },
        .fuzzy => |fuzzy| {
            if (!std.mem.startsWith(u8, fuzzy.field, "/") or fuzzy.auto_fuzzy or fuzzy.prefix_len == 0) return false;
            const prefix_len: usize = @intCast(fuzzy.prefix_len);
            if (fuzzy.term.len < prefix_len) return false;
            const value_text = try db_mod.algebraic.index.pathFactStringFuzzyConstraintValueAlloc(
                alloc,
                fuzzy.term,
                fuzzy.max_edits,
                fuzzy.prefix_len,
            );
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, fuzzy.field, value_text);
            return true;
        },
        .prefix => |prefix| {
            if (!std.mem.startsWith(u8, prefix.field, "/")) return false;
            const value_text = try db_mod.algebraic.index.pathFactStringPrefixConstraintValueAlloc(alloc, prefix.prefix);
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, prefix.field, value_text);
            return true;
        },
        .wildcard => |wildcard| {
            if (!std.mem.startsWith(u8, wildcard.field, "/")) return false;
            const literal_prefix = algebraicWildcardLiteralPrefix(wildcard.pattern);
            if (literal_prefix.len == 0 and algebraicWildcardPatternHasMeta(wildcard.pattern)) return false;
            const value_text = try db_mod.algebraic.index.pathFactStringWildcardConstraintValueAlloc(alloc, wildcard.pattern);
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, wildcard.field, value_text);
            return true;
        },
        .regexp => |regexp| {
            if (!std.mem.startsWith(u8, regexp.field, "/")) return false;
            if (algebraicRegexpLiteralPrefix(regexp.pattern).len == 0) return false;
            var compiled = regex_mod.compile(alloc, regexp.pattern) catch return false;
            defer compiled.deinit();
            const value_text = try db_mod.algebraic.index.pathFactStringRegexpConstraintValueAlloc(alloc, regexp.pattern);
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, regexp.field, value_text);
            return true;
        },
        .bool_field => |field| {
            const value_text = try algebraicConstraintBoolValueAlloc(alloc, field.field, field.value);
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, field.field, value_text);
            return true;
        },
        .numeric_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/")) return false;
            const value_text = try db_mod.algebraic.index.pathFactNumericRangeConstraintValueAlloc(
                alloc,
                range.min,
                range.max,
                range.inclusive_min,
                range.inclusive_max,
            );
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, range.field, value_text);
            return true;
        },
        .date_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/")) return false;
            const start_text = if (range.start_ns) |ns| try std.fmt.allocPrint(alloc, "{d}", .{ns}) else null;
            defer if (start_text) |value| alloc.free(value);
            const end_text = if (range.end_ns) |ns| try std.fmt.allocPrint(alloc, "{d}", .{ns}) else null;
            defer if (end_text) |value| alloc.free(value);
            const value_text = try db_mod.algebraic.index.pathFactDateRangeConstraintValueAlloc(
                alloc,
                start_text,
                end_text,
                range.inclusive_start,
                range.inclusive_end,
            );
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, range.field, value_text);
            return true;
        },
        .term_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/") or (range.min == null and range.max == null)) return false;
            const value_text = try db_mod.algebraic.index.pathFactTermRangeConstraintValueAlloc(
                alloc,
                range.min,
                range.max,
                range.inclusive_min,
                range.inclusive_max,
            );
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, range.field, value_text);
            return true;
        },
        .ip_range => |range| {
            if (!std.mem.startsWith(u8, range.field, "/") or !algebraicValidIpRange(range.cidr)) return false;
            const value_text = try db_mod.algebraic.index.pathFactIpRangeConstraintValueAlloc(alloc, range.cidr);
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, range.field, value_text);
            return true;
        },
        .geo_bbox => |bbox| {
            if (!std.mem.startsWith(u8, bbox.field, "/") or !algebraicValidGeoBBox(bbox.min_lat, bbox.min_lon, bbox.max_lat, bbox.max_lon)) return false;
            const value_text = db_mod.algebraic.index.pathFactGeoBBoxConstraintValueAlloc(
                alloc,
                bbox.min_lat,
                bbox.min_lon,
                bbox.max_lat,
                bbox.max_lon,
            ) catch return false;
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, bbox.field, value_text);
            return true;
        },
        .geo_distance => |distance| {
            if (!std.mem.startsWith(u8, distance.field, "/") or !algebraicValidGeoDistance(distance.lat, distance.lon, distance.radius_meters)) return false;
            const value_text = db_mod.algebraic.index.pathFactGeoDistanceConstraintValueAlloc(
                alloc,
                distance.lat,
                distance.lon,
                distance.radius_meters,
            ) catch return false;
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, distance.field, value_text);
            return true;
        },
        .geo_shape => |shape| {
            if (!std.mem.startsWith(u8, shape.field, "/") or !algebraicGeoShapeRelationSupported(shape.relation)) return false;
            const value_text = db_mod.algebraic.index.pathFactGeoShapeConstraintValueAlloc(
                alloc,
                @tagName(shape.relation),
                shape.polygons,
            ) catch return false;
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, shape.field, value_text);
            return true;
        },
        .bool_query => |nested| return try collectAlgebraicTextBoolQueryConstraints(alloc, nested, out),
        else => return false,
    }
}

fn appendAlgebraicConstraint(
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
    alloc: std.mem.Allocator,
    field: []const u8,
    value: []const u8,
) AlgebraicConstraintCollectError!void {
    for (out.items) |existing| {
        if (!std.mem.eql(u8, existing.field, field)) continue;
        if (std.mem.eql(u8, existing.value, value)) return;
        return error.UnsupportedQueryRequest;
    }
    try out.append(alloc, .{
        .field = try alloc.dupe(u8, field),
        .value = try alloc.dupe(u8, value),
    });
}

fn collectAlgebraicFilterConstraints(
    alloc: std.mem.Allocator,
    filter: std.json.Value,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    if (filter != .object) return false;
    if (filter.object.get("match_all") != null) return true;
    if (filter.object.get("term")) |term| {
        const predicate = algebraicFilterTermPredicate(term, filter.object.get("field") orelse filter.object.get("path")) orelse return false;
        const value_text = try algebraicConstraintValueTextAlloc(alloc, predicate.field, predicate.value);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.field, value_text);
        return true;
    }
    if (filter.object.get("terms")) |terms| {
        if (!(try collectSingleValueTermsConstraint(alloc, terms, out))) return false;
        return true;
    }
    if (filter.object.get("match")) |match| {
        const predicate = algebraicPathMatchPredicate(match, filter.object.get("field")) orelse return false;
        if (predicate.text.len == 0) return false;
        const value_text = try db_mod.algebraic.index.pathFactStringMatchConstraintValueAlloc(alloc, predicate.text);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("bool_field")) |bool_field| {
        if (bool_field != .object) return false;
        const field = bool_field.object.get("field") orelse return false;
        const value = bool_field.object.get("value") orelse return false;
        if (field != .string or value != .bool) return false;
        const value_text = try algebraicConstraintBoolValueAlloc(alloc, field.string, value.bool);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, field.string, value_text);
        return true;
    }
    if (filter.object.get("exists")) |exists| {
        const path = algebraicExistsPath(exists) orelse return false;
        if (!std.mem.startsWith(u8, path, "/")) return false;
        try appendAlgebraicConstraint(out, alloc, path, db_mod.algebraic.index.path_fact_exists_constraint_value);
        return true;
    }
    if (filter.object.get("prefix")) |prefix| {
        const predicate = algebraicPathPrefixPredicate(prefix, filter.object.get("field")) orelse return false;
        const value_text = try db_mod.algebraic.index.pathFactStringPrefixConstraintValueAlloc(alloc, predicate.prefix);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("wildcard")) |wildcard| {
        const predicate = algebraicPathPatternPredicate(wildcard, "pattern") orelse return false;
        const literal_prefix = algebraicWildcardLiteralPrefix(predicate.text);
        if (literal_prefix.len == 0 and algebraicWildcardPatternHasMeta(predicate.text)) return false;
        const value_text = try db_mod.algebraic.index.pathFactStringWildcardConstraintValueAlloc(alloc, predicate.text);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("regexp")) |regexp| {
        const predicate = algebraicPathPatternPredicate(regexp, "pattern") orelse return false;
        if (algebraicRegexpLiteralPrefix(predicate.text).len == 0) return false;
        var compiled = regex_mod.compile(alloc, predicate.text) catch return false;
        defer compiled.deinit();
        const value_text = try db_mod.algebraic.index.pathFactStringRegexpConstraintValueAlloc(alloc, predicate.text);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("fuzzy")) |fuzzy| {
        const predicate = algebraicPathFuzzyPredicate(fuzzy) orelse return false;
        if (predicate.query.prefix_len == 0) return false;
        const prefix_len: usize = @intCast(predicate.query.prefix_len);
        if (predicate.query.term.len < prefix_len) return false;
        const value_text = try db_mod.algebraic.index.pathFactStringFuzzyConstraintValueAlloc(
            alloc,
            predicate.query.term,
            predicate.query.max_edits,
            predicate.query.prefix_len,
        );
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("numeric_range")) |range| {
        const predicate = algebraicPathNumericRangePredicate(range) orelse return false;
        const value_text = try db_mod.algebraic.index.pathFactNumericRangeConstraintValueAlloc(
            alloc,
            predicate.min,
            predicate.max,
            predicate.inclusive_min,
            predicate.inclusive_max,
        );
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("date_range")) |range| {
        const predicate = algebraicPathDateRangePredicate(range) orelse return false;
        const start_text = try algebraicDateBoundTextAlloc(alloc, predicate.start);
        defer if (start_text) |value| alloc.free(value);
        const end_text = try algebraicDateBoundTextAlloc(alloc, predicate.end);
        defer if (end_text) |value| alloc.free(value);
        const value_text = try db_mod.algebraic.index.pathFactDateRangeConstraintValueAlloc(
            alloc,
            start_text,
            end_text,
            predicate.inclusive_start,
            predicate.inclusive_end,
        );
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("ip_range")) |range| {
        const predicate = algebraicPathIpRangePredicate(range) orelse return false;
        const value_text = try db_mod.algebraic.index.pathFactIpRangeConstraintValueAlloc(alloc, predicate.cidr);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("geo_bbox")) |bbox| {
        const predicate = algebraicPathGeoBBoxPredicate(bbox) orelse return false;
        const value_text = db_mod.algebraic.index.pathFactGeoBBoxConstraintValueAlloc(
            alloc,
            predicate.min_lat,
            predicate.min_lon,
            predicate.max_lat,
            predicate.max_lon,
        ) catch return false;
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("geo_distance")) |distance| {
        const predicate = algebraicPathGeoDistancePredicate(distance) orelse return false;
        const value_text = db_mod.algebraic.index.pathFactGeoDistanceConstraintValueAlloc(
            alloc,
            predicate.lat,
            predicate.lon,
            predicate.radius_meters,
        ) catch return false;
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("geo_shape")) |shape| {
        var predicate = (try algebraicPathGeoShapePredicateAlloc(alloc, shape)) orelse return false;
        defer predicate.deinit(alloc);
        const value_text = db_mod.algebraic.index.pathFactGeoShapeConstraintValueAlloc(
            alloc,
            @tagName(predicate.relation),
            predicate.polygons,
        ) catch return false;
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("term_range")) |range| {
        const predicate = algebraicPathTermRangePredicate(range) orelse return false;
        const value_text = try db_mod.algebraic.index.pathFactTermRangeConstraintValueAlloc(
            alloc,
            predicate.min,
            predicate.max,
            predicate.inclusive_min,
            predicate.inclusive_max,
        );
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
        return true;
    }
    if (filter.object.get("range")) |range| {
        if (algebraicPathStandardNumericRangePredicate(range)) |predicate| {
            const value_text = try db_mod.algebraic.index.pathFactNumericRangeConstraintValueAlloc(
                alloc,
                predicate.min,
                predicate.max,
                predicate.inclusive_min,
                predicate.inclusive_max,
            );
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
            return true;
        }
        if (algebraicPathStandardDateRangePredicate(range)) |predicate| {
            const start_text = try algebraicDateBoundTextAlloc(alloc, predicate.start);
            defer if (start_text) |value| alloc.free(value);
            const end_text = try algebraicDateBoundTextAlloc(alloc, predicate.end);
            defer if (end_text) |value| alloc.free(value);
            const value_text = try db_mod.algebraic.index.pathFactDateRangeConstraintValueAlloc(
                alloc,
                start_text,
                end_text,
                predicate.inclusive_start,
                predicate.inclusive_end,
            );
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
            return true;
        }
        if (algebraicPathStandardTermRangePredicate(range)) |predicate| {
            const value_text = try db_mod.algebraic.index.pathFactTermRangeConstraintValueAlloc(
                alloc,
                predicate.min,
                predicate.max,
                predicate.inclusive_min,
                predicate.inclusive_max,
            );
            defer alloc.free(value_text);
            try appendAlgebraicConstraint(out, alloc, predicate.path, value_text);
            return true;
        }
        return false;
    }
    if (filter.object.get("conjuncts")) |conjuncts| {
        if (conjuncts != .array) return false;
        for (conjuncts.array.items) |item| {
            if (!(try collectAlgebraicFilterConstraints(alloc, item, out))) return false;
        }
        return true;
    }
    if (filter.object.get("disjuncts")) |disjuncts| {
        return try collectAlgebraicFilterShouldTermConstraint(alloc, disjuncts, out);
    }
    if (filter.object.get("bool")) |bool_query| {
        if (bool_query != .object) return false;
        if (bool_query.object.get("must_not") != null) return false;
        const must = bool_query.object.get("must");
        const filter_clause = bool_query.object.get("filter");
        const should = bool_query.object.get("should");
        if (should) |clause| {
            const min_should_value = bool_query.object.get("minimum_should_match") orelse bool_query.object.get("min_should");
            if (must != null or filter_clause != null) {
                if (!algebraicBoolShouldMinIsOptional(min_should_value)) return false;
            } else {
                if (!algebraicBoolShouldMinIsOne(min_should_value)) return false;
                return try collectAlgebraicFilterShouldTermConstraint(alloc, clause, out);
            }
        }
        if (must == null and filter_clause == null) return false;
        if (must) |clause| {
            if (!(try collectAlgebraicFilterConstraintClause(alloc, clause, out))) return false;
        }
        if (filter_clause) |clause| {
            if (!(try collectAlgebraicFilterConstraintClause(alloc, clause, out))) return false;
        }
        return true;
    }
    return false;
}

fn collectAlgebraicFilterConstraintClause(
    alloc: std.mem.Allocator,
    clause: std.json.Value,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    if (clause == .array) {
        if (clause.array.items.len == 0) return false;
        for (clause.array.items) |item| {
            if (!(try collectAlgebraicFilterConstraints(alloc, item, out))) return false;
        }
        return true;
    }
    return try collectAlgebraicFilterConstraints(alloc, clause, out);
}

fn algebraicBoolShouldMinIsOptional(value: ?std.json.Value) bool {
    const actual = value orelse return true;
    return switch (actual) {
        .integer => |number| number == 0,
        .float => |number| number == 0.0,
        .string => |text| std.mem.eql(u8, text, "0"),
        else => false,
    };
}

fn algebraicBoolShouldMinIsOne(value: ?std.json.Value) bool {
    const actual = value orelse return true;
    return switch (actual) {
        .integer => |number| number == 1,
        .float => |number| number == 1.0,
        .string => |text| std.mem.eql(u8, text, "1"),
        else => false,
    };
}

fn collectAlgebraicFilterShouldTermConstraint(
    alloc: std.mem.Allocator,
    clause: std.json.Value,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    const items = switch (clause) {
        .array => |array| array.items,
        else => return try collectAlgebraicFilterShouldTermItemsConstraint(alloc, &.{clause}, out),
    };
    return try collectAlgebraicFilterShouldTermItemsConstraint(alloc, items, out);
}

fn collectAlgebraicFilterShouldTermItemsConstraint(
    alloc: std.mem.Allocator,
    items: []const std.json.Value,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    if (items.len == 0) return false;
    var field: ?[]const u8 = null;
    const typed_values = try alloc.alloc([]const u8, items.len);
    defer alloc.free(typed_values);
    var initialized: usize = 0;
    defer {
        for (typed_values[0..initialized]) |value| alloc.free(@constCast(value));
    }

    for (items, 0..) |item, i| {
        const object = switch (item) {
            .object => |object| object,
            else => return false,
        };
        const predicate = algebraicFilterTermPredicate(object.get("term") orelse return false, object.get("field") orelse object.get("path")) orelse return false;
        if (!std.mem.startsWith(u8, predicate.field, "/")) return false;
        if (field) |existing| {
            if (!std.mem.eql(u8, existing, predicate.field)) return false;
        } else {
            field = predicate.field;
        }
        typed_values[i] = try algebraicConstraintValueTextAlloc(alloc, predicate.field, predicate.value);
        initialized += 1;
    }

    try appendAlgebraicTypedAnyConstraint(out, alloc, field orelse return false, typed_values);
    return true;
}

const AlgebraicFilterTermPredicate = struct {
    field: []const u8,
    value: std.json.Value,
};

fn algebraicFilterTermPredicate(term: std.json.Value, sibling_field_value: ?std.json.Value) ?AlgebraicFilterTermPredicate {
    if (term == .object) {
        if (term.object.get("field") orelse term.object.get("path")) |field_value| {
            const field = algebraicJsonString(field_value) orelse return null;
            const value = term.object.get("term") orelse term.object.get("value") orelse return null;
            return .{ .field = field, .value = value };
        }
        if (term.object.count() == 1) {
            var it = term.object.iterator();
            const entry = it.next() orelse return null;
            return .{ .field = entry.key_ptr.*, .value = entry.value_ptr.* };
        }
    }
    const field = algebraicJsonString(sibling_field_value orelse return null) orelse return null;
    return .{ .field = field, .value = term };
}

fn algebraicExistsPath(value: std.json.Value) ?[]const u8 {
    return switch (value) {
        .string => |field| field,
        .object => |object| blk: {
            const path = object.get("path") orelse object.get("field") orelse break :blk null;
            if (path != .string) break :blk null;
            break :blk path.string;
        },
        else => null,
    };
}

const AlgebraicPathPrefixPredicate = struct {
    path: []const u8,
    prefix: []const u8,
};

const AlgebraicPathTextPredicate = struct {
    path: []const u8,
    text: []const u8,
};

const AlgebraicFuzzyQuery = struct {
    term: []const u8,
    max_edits: u8,
    prefix_len: u8,
};

const AlgebraicPathFuzzyPredicate = struct {
    path: []const u8,
    query: AlgebraicFuzzyQuery,
};

const AlgebraicPathNumericRangePredicate = struct {
    path: []const u8,
    min: ?f64 = null,
    max: ?f64 = null,
    inclusive_min: bool = true,
    inclusive_max: bool = false,
};

const AlgebraicPathTermRangePredicate = struct {
    path: []const u8,
    min: ?[]const u8 = null,
    max: ?[]const u8 = null,
    inclusive_min: bool = true,
    inclusive_max: bool = false,
};

const AlgebraicPathDateRangePredicate = struct {
    path: []const u8,
    start: ?std.json.Value = null,
    end: ?std.json.Value = null,
    inclusive_start: bool = true,
    inclusive_end: bool = false,
};

const AlgebraicPathIpRangePredicate = struct {
    path: []const u8,
    cidr: []const u8,
};

const AlgebraicPathGeoBBoxPredicate = struct {
    path: []const u8,
    min_lat: f64,
    min_lon: f64,
    max_lat: f64,
    max_lon: f64,
};

const AlgebraicPathGeoDistancePredicate = struct {
    path: []const u8,
    lat: f64,
    lon: f64,
    radius_meters: f64,
};

const AlgebraicPathGeoShapePredicate = struct {
    path: []const u8,
    relation: db_mod.types.GeoShapeRelation,
    polygons: []const []const db_mod.types.GeoPoint,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.polygons) |polygon| {
            if (polygon.len > 0) alloc.free(@constCast(polygon));
        }
        if (self.polygons.len > 0) alloc.free(@constCast(self.polygons));
        self.* = undefined;
    }
};

fn algebraicJsonString(value: std.json.Value) ?[]const u8 {
    return switch (value) {
        .string => |text| text,
        else => null,
    };
}

fn algebraicPathPrefixPredicate(prefix_value: std.json.Value, sibling_field_value: ?std.json.Value) ?AlgebraicPathPrefixPredicate {
    return switch (prefix_value) {
        .object => |object| blk: {
            if (object.get("path")) |path_value| {
                const path = algebraicJsonString(path_value) orelse break :blk null;
                if (!std.mem.startsWith(u8, path, "/")) break :blk null;
                const prefix = algebraicJsonString(object.get("value") orelse object.get("prefix") orelse break :blk null) orelse break :blk null;
                break :blk .{ .path = path, .prefix = prefix };
            }
            if (object.get("role") == null) {
                if (object.get("field")) |field_value| {
                    const field = algebraicJsonString(field_value) orelse break :blk null;
                    if (std.mem.startsWith(u8, field, "/")) {
                        const prefix = algebraicJsonString(object.get("value") orelse object.get("prefix") orelse break :blk null) orelse break :blk null;
                        break :blk .{ .path = field, .prefix = prefix };
                    }
                }
            }
            if (object.count() == 1) {
                var it = object.iterator();
                const entry = it.next() orelse break :blk null;
                if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) break :blk null;
                const prefix = algebraicJsonString(entry.value_ptr.*) orelse break :blk null;
                break :blk .{ .path = entry.key_ptr.*, .prefix = prefix };
            }
            break :blk null;
        },
        .string => |prefix| blk: {
            const field = algebraicJsonString(sibling_field_value orelse break :blk null) orelse break :blk null;
            break :blk if (std.mem.startsWith(u8, field, "/")) .{ .path = field, .prefix = prefix } else null;
        },
        else => null,
    };
}

fn algebraicPathFromPredicateObject(object: anytype) ?[]const u8 {
    if (object.get("path")) |path_value| {
        const path = algebraicJsonString(path_value) orelse return null;
        if (!std.mem.startsWith(u8, path, "/")) return null;
        return path;
    }
    return null;
}

fn algebraicPathPatternPredicate(value: std.json.Value, pattern_field: []const u8) ?AlgebraicPathTextPredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (algebraicPathFromPredicateObject(object)) |path| {
        const text = algebraicJsonString(object.get(pattern_field) orelse object.get("value") orelse return null) orelse return null;
        return .{ .path = path, .text = text };
    }
    if (object.get("role") == null) {
        if (object.get("field")) |field_value| {
            const field = algebraicJsonString(field_value) orelse return null;
            if (std.mem.startsWith(u8, field, "/")) {
                const text = algebraicJsonString(object.get(pattern_field) orelse object.get("value") orelse return null) orelse return null;
                return .{ .path = field, .text = text };
            }
        }
    }
    if (object.count() == 1) {
        var it = object.iterator();
        const entry = it.next() orelse return null;
        if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) return null;
        const text = algebraicJsonString(entry.value_ptr.*) orelse return null;
        return .{ .path = entry.key_ptr.*, .text = text };
    }
    return null;
}

fn algebraicPathMatchPredicate(value: std.json.Value, sibling_field_value: ?std.json.Value) ?AlgebraicPathTextPredicate {
    return switch (value) {
        .object => algebraicPathPatternPredicate(value, "query"),
        .string => |text| blk: {
            const field = algebraicJsonString(sibling_field_value orelse break :blk null) orelse break :blk null;
            break :blk if (std.mem.startsWith(u8, field, "/")) .{ .path = field, .text = text } else null;
        },
        else => null,
    };
}

fn algebraicWildcardLiteralPrefix(pattern: []const u8) []const u8 {
    for (pattern, 0..) |ch, i| {
        if (ch == '*' or ch == '?') return pattern[0..i];
    }
    return pattern;
}

fn algebraicWildcardPatternHasMeta(pattern: []const u8) bool {
    return std.mem.indexOfAny(u8, pattern, "*?") != null;
}

fn algebraicRegexpLiteralPrefix(pattern: []const u8) []const u8 {
    for (pattern, 0..) |ch, i| {
        switch (ch) {
            '.', '^', '$', '*', '+', '?', '(', ')', '[', ']', '{', '}', '|', '\\' => return pattern[0..i],
            else => {},
        }
    }
    return pattern;
}

fn algebraicJsonU8(value: std.json.Value) ?u8 {
    return switch (value) {
        .integer => |number| std.math.cast(u8, number),
        .float => |number| blk: {
            if (!std.math.isFinite(number) or @round(number) != number) break :blk null;
            const parsed: i64 = @intFromFloat(number);
            break :blk std.math.cast(u8, parsed);
        },
        else => null,
    };
}

fn algebraicParseFuzzyOptions(object: anytype, out: *AlgebraicFuzzyQuery) bool {
    if (object.get("max_edits")) |edits| {
        out.max_edits = algebraicJsonU8(edits) orelse return false;
    }
    if (object.get("prefix_length")) |prefix| {
        out.prefix_len = algebraicJsonU8(prefix) orelse return false;
    }
    if (object.get("auto_fuzzy")) |auto| {
        if (auto != .bool) return false;
        if (auto.bool) out.max_edits = if (out.term.len > 5) 2 else if (out.term.len > 2) 1 else 0;
    }
    return true;
}

fn algebraicParseFuzzyQuery(value: std.json.Value) ?AlgebraicFuzzyQuery {
    return switch (value) {
        .string => |text| .{ .term = text, .max_edits = 1, .prefix_len = 0 },
        .object => |object| blk: {
            var out = AlgebraicFuzzyQuery{
                .term = algebraicJsonString(object.get("query") orelse object.get("value") orelse break :blk null) orelse break :blk null,
                .max_edits = 1,
                .prefix_len = 0,
            };
            if (!algebraicParseFuzzyOptions(object, &out)) break :blk null;
            break :blk out;
        },
        else => null,
    };
}

fn algebraicPathFuzzyPredicate(value: std.json.Value) ?AlgebraicPathFuzzyPredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (algebraicPathFromPredicateObject(object)) |path| {
        var query = AlgebraicFuzzyQuery{
            .term = algebraicJsonString(object.get("query") orelse object.get("value") orelse return null) orelse return null,
            .max_edits = 1,
            .prefix_len = 0,
        };
        if (!algebraicParseFuzzyOptions(object, &query)) return null;
        return .{ .path = path, .query = query };
    }
    if (object.get("role") == null) {
        if (object.get("field")) |field_value| {
            const field = algebraicJsonString(field_value) orelse return null;
            if (std.mem.startsWith(u8, field, "/")) {
                var query = AlgebraicFuzzyQuery{
                    .term = algebraicJsonString(object.get("query") orelse object.get("value") orelse return null) orelse return null,
                    .max_edits = 1,
                    .prefix_len = 0,
                };
                if (!algebraicParseFuzzyOptions(object, &query)) return null;
                return .{ .path = field, .query = query };
            }
        }
    }
    if (object.count() == 1) {
        var it = object.iterator();
        const entry = it.next() orelse return null;
        if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) return null;
        const query = algebraicParseFuzzyQuery(entry.value_ptr.*) orelse return null;
        return .{ .path = entry.key_ptr.*, .query = query };
    }
    return null;
}

fn algebraicOptionalBool(value: ?std.json.Value) ?bool {
    const actual = value orelse return null;
    return switch (actual) {
        .bool => |flag| flag,
        .null => null,
        else => null,
    };
}

fn algebraicOptionalF64(value: ?std.json.Value) ?f64 {
    const actual = value orelse return null;
    return switch (actual) {
        .integer => |number| @floatFromInt(number),
        .float => |number| number,
        .null => null,
        else => null,
    };
}

fn algebraicOptionalString(value: ?std.json.Value) ?[]const u8 {
    const actual = value orelse return null;
    return switch (actual) {
        .string => |text| text,
        .null => null,
        else => null,
    };
}

fn algebraicNumericJsonValue(value: std.json.Value) bool {
    return switch (value) {
        .integer, .float => true,
        else => false,
    };
}

fn algebraicStringJsonValue(value: std.json.Value) bool {
    return switch (value) {
        .string => true,
        else => false,
    };
}

fn algebraicDateJsonValue(value: std.json.Value) bool {
    return switch (value) {
        .integer => |number| number >= 0,
        .string => |text| (algebraicParseDateTimeOptionalToNs(text) catch null) != null,
        else => false,
    };
}

fn algebraicDateBoundTextAlloc(alloc: std.mem.Allocator, value: ?std.json.Value) !?[]u8 {
    const actual = value orelse return null;
    return switch (actual) {
        .integer => |number| if (number >= 0) try std.fmt.allocPrint(alloc, "{d}", .{number}) else error.UnsupportedQueryRequest,
        .string => |text| if ((try algebraicParseDateTimeOptionalToNs(text)) != null) try alloc.dupe(u8, text) else error.UnsupportedQueryRequest,
        .null => null,
        else => error.UnsupportedQueryRequest,
    };
}

fn algebraicValidIpRange(text: []const u8) bool {
    return ip_range.isValid(text);
}

fn algebraicValidLatitude(lat: f64) bool {
    return std.math.isFinite(lat) and lat >= -90.0 and lat <= 90.0;
}

fn algebraicValidLongitude(lon: f64) bool {
    return std.math.isFinite(lon) and lon >= -180.0 and lon <= 180.0;
}

fn algebraicValidGeoBBox(min_lat: f64, min_lon: f64, max_lat: f64, max_lon: f64) bool {
    return algebraicValidLatitude(min_lat) and
        algebraicValidLatitude(max_lat) and
        algebraicValidLongitude(min_lon) and
        algebraicValidLongitude(max_lon) and
        min_lat <= max_lat;
}

fn algebraicValidGeoDistance(lat: f64, lon: f64, radius_meters: f64) bool {
    return algebraicValidLatitude(lat) and
        algebraicValidLongitude(lon) and
        std.math.isFinite(radius_meters) and
        radius_meters >= 0;
}

fn algebraicGeoShapeRelationSupported(relation: db_mod.types.GeoShapeRelation) bool {
    return switch (relation) {
        .intersects, .within => true,
        .contains => false,
    };
}

fn algebraicParseDateTimeOptionalToNs(text: []const u8) !?u64 {
    if (try algebraicParseRfc3339ToNs(text)) |ts| return ts;
    if (text.len != 10 or text[4] != '-' or text[7] != '-') return null;
    const year = std.fmt.parseInt(i64, text[0..4], 10) catch return null;
    const month = std.fmt.parseInt(i64, text[5..7], 10) catch return null;
    const day = std.fmt.parseInt(i64, text[8..10], 10) catch return null;
    return algebraicCivilDateTimeToNs(year, month, day, 0, 0, 0, 0);
}

fn algebraicParseRfc3339ToNs(text: []const u8) !?u64 {
    if (text.len < 20) return null;
    if (text[4] != '-' or text[7] != '-' or text[10] != 'T' or text[13] != ':' or text[16] != ':') return null;
    const year = std.fmt.parseInt(i64, text[0..4], 10) catch return null;
    const month = std.fmt.parseInt(i64, text[5..7], 10) catch return null;
    const day = std.fmt.parseInt(i64, text[8..10], 10) catch return null;
    const hour = std.fmt.parseInt(i64, text[11..13], 10) catch return null;
    const minute = std.fmt.parseInt(i64, text[14..16], 10) catch return null;
    const second = std.fmt.parseInt(i64, text[17..19], 10) catch return null;
    var idx: usize = 19;
    var nanos: u64 = 0;
    if (idx < text.len and text[idx] == '.') {
        idx += 1;
        const frac_start = idx;
        while (idx < text.len and text[idx] >= '0' and text[idx] <= '9') : (idx += 1) {}
        const frac = text[frac_start..idx];
        if (frac.len == 0 or frac.len > 9) return null;
        var frac_ns = std.fmt.parseInt(u64, frac, 10) catch return null;
        var scale: usize = frac.len;
        while (scale < 9) : (scale += 1) frac_ns *= 10;
        nanos = frac_ns;
    }
    if (idx >= text.len or text[idx] != 'Z' or idx + 1 != text.len) return null;
    return algebraicCivilDateTimeToNs(year, month, day, hour, minute, second, nanos);
}

fn algebraicCivilDateTimeToNs(year: i64, month: i64, day: i64, hour: i64, minute: i64, second: i64, nanos: u64) ?u64 {
    if (month < 1 or month > 12 or day < 1 or day > 31 or hour < 0 or hour > 23 or minute < 0 or minute > 59 or second < 0 or second > 60) return null;
    const days = algebraicDaysFromCivil(year, month, day);
    if (days < 0) return null;
    const secs = days * 86_400 + hour * 3_600 + minute * 60 + second;
    if (secs < 0) return null;
    return @as(u64, @intCast(secs)) * std.time.ns_per_s + nanos;
}

fn algebraicDaysFromCivil(year: i64, month: i64, day: i64) i64 {
    var y = year;
    y -= if (month <= 2) 1 else 0;
    const era = @divFloor(y, 400);
    const yoe = y - era * 400;
    const mp = month + (if (month > 2) @as(i64, -3) else @as(i64, 9));
    const doy = @divFloor(153 * mp + 2, 5) + day - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146_097 + doe - 719_468;
}

fn algebraicPathIpRangePredicate(value: std.json.Value) ?AlgebraicPathIpRangePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    const cidr = algebraicJsonString(object.get("cidr") orelse return null) orelse return null;
    if (!algebraicValidIpRange(cidr)) return null;
    if (algebraicPathFromPredicateObject(object)) |path| {
        return .{ .path = path, .cidr = cidr };
    }
    if (object.get("role") == null) {
        if (object.get("field")) |field_value| {
            const field = algebraicJsonString(field_value) orelse return null;
            if (std.mem.startsWith(u8, field, "/")) return .{ .path = field, .cidr = cidr };
        }
    }
    return null;
}

fn algebraicPathGeoBBoxPredicate(value: std.json.Value) ?AlgebraicPathGeoBBoxPredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("role") != null) return null;
    const path = if (object.get("path")) |path_value|
        algebraicJsonString(path_value) orelse return null
    else if (object.get("field")) |field_value| blk: {
        const field = algebraicJsonString(field_value) orelse return null;
        if (!std.mem.startsWith(u8, field, "/")) return null;
        break :blk field;
    } else return null;
    if (!std.mem.startsWith(u8, path, "/")) return null;
    const min_lat = algebraicOptionalF64(object.get("min_lat")) orelse return null;
    const min_lon = algebraicOptionalF64(object.get("min_lon")) orelse return null;
    const max_lat = algebraicOptionalF64(object.get("max_lat")) orelse return null;
    const max_lon = algebraicOptionalF64(object.get("max_lon")) orelse return null;
    if (!algebraicValidGeoBBox(min_lat, min_lon, max_lat, max_lon)) return null;
    return .{
        .path = path,
        .min_lat = min_lat,
        .min_lon = min_lon,
        .max_lat = max_lat,
        .max_lon = max_lon,
    };
}

fn algebraicPathGeoDistancePredicate(value: std.json.Value) ?AlgebraicPathGeoDistancePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("role") != null) return null;
    const path = if (object.get("path")) |path_value|
        algebraicJsonString(path_value) orelse return null
    else if (object.get("field")) |field_value| blk: {
        const field = algebraicJsonString(field_value) orelse return null;
        if (!std.mem.startsWith(u8, field, "/")) return null;
        break :blk field;
    } else return null;
    if (!std.mem.startsWith(u8, path, "/")) return null;
    const lat = algebraicOptionalF64(object.get("lat")) orelse return null;
    const lon = algebraicOptionalF64(object.get("lon")) orelse return null;
    const radius_meters = algebraicOptionalF64(object.get("radius_meters")) orelse return null;
    if (!algebraicValidGeoDistance(lat, lon, radius_meters)) return null;
    return .{
        .path = path,
        .lat = lat,
        .lon = lon,
        .radius_meters = radius_meters,
    };
}

fn algebraicPathGeoShapePredicateAlloc(alloc: std.mem.Allocator, value: std.json.Value) !?AlgebraicPathGeoShapePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("role") != null) return null;
    const path = if (object.get("path")) |path_value|
        algebraicJsonString(path_value) orelse return null
    else if (object.get("field")) |field_value| blk: {
        const field = algebraicJsonString(field_value) orelse return null;
        if (!std.mem.startsWith(u8, field, "/")) return null;
        break :blk field;
    } else return null;
    if (!std.mem.startsWith(u8, path, "/")) return null;
    const relation_text = if (object.get("relation")) |relation_value|
        algebraicJsonString(relation_value) orelse return null
    else
        "intersects";
    const relation = std.meta.stringToEnum(db_mod.types.GeoShapeRelation, relation_text) orelse return null;
    if (!algebraicGeoShapeRelationSupported(relation)) return null;
    const polygons_value = object.get("polygons") orelse object.get("polygon") orelse return null;
    const polygons = try algebraicGeoShapePolygonsAlloc(alloc, polygons_value);
    errdefer {
        for (polygons) |polygon| {
            if (polygon.len > 0) alloc.free(@constCast(polygon));
        }
        if (polygons.len > 0) alloc.free(polygons);
    }
    return .{
        .path = path,
        .relation = relation,
        .polygons = polygons,
    };
}

fn algebraicGeoShapePolygonsAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]const []const db_mod.types.GeoPoint {
    const array = switch (value) {
        .array => |array| array,
        else => return error.UnsupportedQueryRequest,
    };
    if (array.items.len == 0) return error.UnsupportedQueryRequest;
    const first_is_point = array.items[0] == .object;
    const polygon_count: usize = if (first_is_point) 1 else array.items.len;
    var out = try alloc.alloc([]const db_mod.types.GeoPoint, polygon_count);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |polygon| {
            if (polygon.len > 0) alloc.free(@constCast(polygon));
        }
        if (out.len > 0) alloc.free(out);
    }
    if (first_is_point) {
        out[0] = try algebraicGeoShapePolygonAlloc(alloc, value);
        initialized = 1;
    } else {
        for (array.items, 0..) |item, i| {
            out[i] = try algebraicGeoShapePolygonAlloc(alloc, item);
            initialized += 1;
        }
    }
    return out;
}

fn algebraicGeoShapePolygonAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]const db_mod.types.GeoPoint {
    const array = switch (value) {
        .array => |array| array,
        else => return error.UnsupportedQueryRequest,
    };
    if (array.items.len < 3) return error.UnsupportedQueryRequest;
    var out = try alloc.alloc(db_mod.types.GeoPoint, array.items.len);
    errdefer if (out.len > 0) alloc.free(out);
    for (array.items, 0..) |item, i| {
        const object = switch (item) {
            .object => |object| object,
            else => return error.UnsupportedQueryRequest,
        };
        const lat = algebraicOptionalF64(object.get("lat")) orelse return error.UnsupportedQueryRequest;
        const lon = algebraicOptionalF64(object.get("lon")) orelse return error.UnsupportedQueryRequest;
        if (!algebraicValidLatitude(lat) or !algebraicValidLongitude(lon)) return error.UnsupportedQueryRequest;
        out[i] = .{ .lat = lat, .lon = lon };
    }
    return out;
}

fn algebraicPathNumericRangePredicate(value: std.json.Value) ?AlgebraicPathNumericRangePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    const path = if (object.get("path")) |path_value|
        algebraicJsonString(path_value) orelse return null
    else if (object.get("field")) |field_value| blk: {
        const field = algebraicJsonString(field_value) orelse return null;
        if (!std.mem.startsWith(u8, field, "/")) return null;
        break :blk field;
    } else return null;
    if (!std.mem.startsWith(u8, path, "/")) return null;
    const min = algebraicOptionalF64(object.get("min"));
    const max = algebraicOptionalF64(object.get("max"));
    if (min == null and max == null) return null;
    return .{
        .path = path,
        .min = min,
        .max = max,
        .inclusive_min = algebraicOptionalBool(object.get("inclusive_min")) orelse true,
        .inclusive_max = algebraicOptionalBool(object.get("inclusive_max")) orelse false,
    };
}

fn algebraicPathTermRangePredicate(value: std.json.Value) ?AlgebraicPathTermRangePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("path") != null or object.get("field") != null) {
        const path = if (object.get("path")) |path_value|
            algebraicJsonString(path_value) orelse return null
        else blk: {
            const field = algebraicJsonString(object.get("field").?) orelse return null;
            if (!std.mem.startsWith(u8, field, "/")) return null;
            break :blk field;
        };
        if (!std.mem.startsWith(u8, path, "/")) return null;
        const min = algebraicOptionalString(object.get("min"));
        const max = algebraicOptionalString(object.get("max"));
        if (min == null and max == null) return null;
        return .{
            .path = path,
            .min = min,
            .max = max,
            .inclusive_min = algebraicOptionalBool(object.get("inclusive_min")) orelse true,
            .inclusive_max = algebraicOptionalBool(object.get("inclusive_max")) orelse false,
        };
    }
    if (object.count() != 1) return null;
    var it = object.iterator();
    const entry = it.next() orelse return null;
    if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) return null;
    return algebraicTermRangeFromBounds(entry.key_ptr.*, switch (entry.value_ptr.*) {
        .object => |inner| inner,
        else => return null,
    });
}

fn algebraicPathDateRangePredicate(value: std.json.Value) ?AlgebraicPathDateRangePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("path") != null or object.get("field") != null) {
        const path = if (object.get("path")) |path_value|
            algebraicJsonString(path_value) orelse return null
        else blk: {
            const field = algebraicJsonString(object.get("field").?) orelse return null;
            if (!std.mem.startsWith(u8, field, "/")) return null;
            break :blk field;
        };
        if (!std.mem.startsWith(u8, path, "/")) return null;
        const start = object.get("start_ns") orelse object.get("start");
        const end = object.get("end_ns") orelse object.get("end");
        if (start == null and end == null) return null;
        if (start) |bound| if (!algebraicDateJsonValue(bound)) return null;
        if (end) |bound| if (!algebraicDateJsonValue(bound)) return null;
        return .{
            .path = path,
            .start = start,
            .end = end,
            .inclusive_start = algebraicOptionalBool(object.get("inclusive_start")) orelse true,
            .inclusive_end = algebraicOptionalBool(object.get("inclusive_end")) orelse false,
        };
    }
    if (object.count() != 1) return null;
    var it = object.iterator();
    const entry = it.next() orelse return null;
    if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) return null;
    const range_object = switch (entry.value_ptr.*) {
        .object => |inner| inner,
        else => return null,
    };
    return algebraicDateRangeFromBounds(entry.key_ptr.*, range_object);
}

const AlgebraicRangeBound = struct {
    value: std.json.Value,
    inclusive: bool,
};

fn algebraicSetRangeBound(found: *?AlgebraicRangeBound, value: std.json.Value, inclusive: bool) ?void {
    if (found.* != null) return null;
    found.* = .{ .value = value, .inclusive = inclusive };
}

fn algebraicStandardRangeLowerBound(object: std.json.ObjectMap) ?AlgebraicRangeBound {
    var found: ?AlgebraicRangeBound = null;
    if (object.get("gt")) |value| algebraicSetRangeBound(&found, value, false) orelse return null;
    if (object.get("gte")) |value| algebraicSetRangeBound(&found, value, true) orelse return null;
    return found;
}

fn algebraicStandardRangeUpperBound(object: std.json.ObjectMap) ?AlgebraicRangeBound {
    var found: ?AlgebraicRangeBound = null;
    if (object.get("lt")) |value| algebraicSetRangeBound(&found, value, false) orelse return null;
    if (object.get("lte")) |value| algebraicSetRangeBound(&found, value, true) orelse return null;
    return found;
}

fn algebraicTermRangeFromBounds(path: []const u8, object: std.json.ObjectMap) ?AlgebraicPathTermRangePredicate {
    const lower = algebraicStandardRangeLowerBound(object);
    const upper = algebraicStandardRangeUpperBound(object);
    if (lower == null and upper == null) return null;
    if (lower) |bound| if (!algebraicStringJsonValue(bound.value)) return null;
    if (upper) |bound| if (!algebraicStringJsonValue(bound.value)) return null;
    return .{
        .path = path,
        .min = if (lower) |bound| algebraicOptionalString(bound.value) else null,
        .max = if (upper) |bound| algebraicOptionalString(bound.value) else null,
        .inclusive_min = if (lower) |bound| bound.inclusive else true,
        .inclusive_max = if (upper) |bound| bound.inclusive else false,
    };
}

fn algebraicDateRangeFromBounds(path: []const u8, object: std.json.ObjectMap) ?AlgebraicPathDateRangePredicate {
    const lower = algebraicStandardRangeLowerBound(object);
    const upper = algebraicStandardRangeUpperBound(object);
    if (lower == null and upper == null) return null;
    if (lower) |bound| if (!algebraicDateJsonValue(bound.value)) return null;
    if (upper) |bound| if (!algebraicDateJsonValue(bound.value)) return null;
    return .{
        .path = path,
        .start = if (lower) |bound| bound.value else null,
        .end = if (upper) |bound| bound.value else null,
        .inclusive_start = if (lower) |bound| bound.inclusive else true,
        .inclusive_end = if (upper) |bound| bound.inclusive else false,
    };
}

fn algebraicPathStandardNumericRangePredicate(value: std.json.Value) ?AlgebraicPathNumericRangePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("field") != null or object.get("path") != null) {
        const path = if (object.get("path")) |path_value|
            algebraicJsonString(path_value) orelse return null
        else blk: {
            const field = algebraicJsonString(object.get("field").?) orelse return null;
            if (!std.mem.startsWith(u8, field, "/")) return null;
            break :blk field;
        };
        if (!std.mem.startsWith(u8, path, "/")) return null;
        const lower = algebraicStandardRangeLowerBound(object);
        const upper = algebraicStandardRangeUpperBound(object);
        if (lower == null and upper == null) return null;
        if (lower) |bound| if (!algebraicNumericJsonValue(bound.value)) return null;
        if (upper) |bound| if (!algebraicNumericJsonValue(bound.value)) return null;
        return .{
            .path = path,
            .min = if (lower) |bound| algebraicOptionalF64(bound.value) else null,
            .max = if (upper) |bound| algebraicOptionalF64(bound.value) else null,
            .inclusive_min = if (lower) |bound| bound.inclusive else true,
            .inclusive_max = if (upper) |bound| bound.inclusive else false,
        };
    }
    if (object.count() != 1) return null;
    var it = object.iterator();
    const entry = it.next() orelse return null;
    if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) return null;
    const range_object = switch (entry.value_ptr.*) {
        .object => |inner| inner,
        else => return null,
    };
    const lower = algebraicStandardRangeLowerBound(range_object);
    const upper = algebraicStandardRangeUpperBound(range_object);
    if (lower == null and upper == null) return null;
    if (lower) |bound| if (!algebraicNumericJsonValue(bound.value)) return null;
    if (upper) |bound| if (!algebraicNumericJsonValue(bound.value)) return null;
    return .{
        .path = entry.key_ptr.*,
        .min = if (lower) |bound| algebraicOptionalF64(bound.value) else null,
        .max = if (upper) |bound| algebraicOptionalF64(bound.value) else null,
        .inclusive_min = if (lower) |bound| bound.inclusive else true,
        .inclusive_max = if (upper) |bound| bound.inclusive else false,
    };
}

fn algebraicPathStandardDateRangePredicate(value: std.json.Value) ?AlgebraicPathDateRangePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("field") != null or object.get("path") != null) {
        const path = if (object.get("path")) |path_value|
            algebraicJsonString(path_value) orelse return null
        else blk: {
            const field = algebraicJsonString(object.get("field").?) orelse return null;
            if (!std.mem.startsWith(u8, field, "/")) return null;
            break :blk field;
        };
        if (!std.mem.startsWith(u8, path, "/")) return null;
        return algebraicDateRangeFromBounds(path, object);
    }
    if (object.count() != 1) return null;
    var it = object.iterator();
    const entry = it.next() orelse return null;
    if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) return null;
    const range_object = switch (entry.value_ptr.*) {
        .object => |inner| inner,
        else => return null,
    };
    return algebraicDateRangeFromBounds(entry.key_ptr.*, range_object);
}

fn algebraicPathStandardTermRangePredicate(value: std.json.Value) ?AlgebraicPathTermRangePredicate {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("field") != null or object.get("path") != null) {
        const path = if (object.get("path")) |path_value|
            algebraicJsonString(path_value) orelse return null
        else blk: {
            const field = algebraicJsonString(object.get("field").?) orelse return null;
            if (!std.mem.startsWith(u8, field, "/")) return null;
            break :blk field;
        };
        if (!std.mem.startsWith(u8, path, "/")) return null;
        return algebraicTermRangeFromBounds(path, object);
    }
    if (object.count() != 1) return null;
    var it = object.iterator();
    const entry = it.next() orelse return null;
    if (!std.mem.startsWith(u8, entry.key_ptr.*, "/")) return null;
    const range_object = switch (entry.value_ptr.*) {
        .object => |inner| inner,
        else => return null,
    };
    return algebraicTermRangeFromBounds(entry.key_ptr.*, range_object);
}

fn collectSingleValueTermsConstraint(
    alloc: std.mem.Allocator,
    terms: std.json.Value,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    if (terms != .object) return false;
    if (terms.object.count() == 1) {
        var it = terms.object.iterator();
        const entry = it.next() orelse return false;
        return try collectTermsValuesConstraint(alloc, entry.key_ptr.*, entry.value_ptr.*, out);
    }
    const field = terms.object.get("path") orelse terms.object.get("field") orelse return false;
    const values = terms.object.get("values") orelse terms.object.get("terms") orelse return false;
    if (field != .string) return false;
    return try collectTermsValuesConstraint(alloc, field.string, values, out);
}

fn collectTermsValuesConstraint(
    alloc: std.mem.Allocator,
    field: []const u8,
    values: std.json.Value,
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
) AlgebraicConstraintCollectError!bool {
    if (values != .array or values.array.items.len == 0) return false;
    if (!std.mem.startsWith(u8, field, "/")) {
        if (values.array.items.len != 1) return false;
        const value_text = try algebraicConstraintValueTextAlloc(alloc, field, values.array.items[0]);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, field, value_text);
        return true;
    }
    if (values.array.items.len == 1) {
        const value_text = try algebraicConstraintValueTextAlloc(alloc, field, values.array.items[0]);
        defer alloc.free(value_text);
        try appendAlgebraicConstraint(out, alloc, field, value_text);
        return true;
    }
    const typed_values = try alloc.alloc([]const u8, values.array.items.len);
    defer alloc.free(typed_values);
    var initialized: usize = 0;
    defer {
        for (typed_values[0..initialized]) |value| alloc.free(@constCast(value));
    }
    for (values.array.items, 0..) |item, i| {
        typed_values[i] = try algebraicConstraintValueTextAlloc(alloc, field, item);
        initialized += 1;
    }
    try appendAlgebraicTypedAnyConstraint(out, alloc, field, typed_values);
    return true;
}

fn appendAlgebraicTypedAnyConstraint(
    out: *std.ArrayListUnmanaged(db_mod.aggregations.FixedConstraint),
    alloc: std.mem.Allocator,
    field: []const u8,
    typed_values: []const []const u8,
) AlgebraicConstraintCollectError!void {
    if (typed_values.len == 0) return error.UnsupportedQueryRequest;
    if (typed_values.len == 1) {
        try appendAlgebraicConstraint(out, alloc, field, typed_values[0]);
        return;
    }
    const any_value = db_mod.algebraic.index.pathFactAnyConstraintValueAlloc(alloc, typed_values) catch return error.UnsupportedQueryRequest;
    defer alloc.free(any_value);
    try appendAlgebraicConstraint(out, alloc, field, any_value);
}

fn algebraicConstraintValueTextAlloc(alloc: std.mem.Allocator, field: []const u8, value: std.json.Value) AlgebraicConstraintCollectError![]u8 {
    const raw = switch (value) {
        .string => |text| try alloc.dupe(u8, text),
        .integer => |number| try std.fmt.allocPrint(alloc, "{d}", .{number}),
        .float => |number| try std.fmt.allocPrint(alloc, "{d}", .{number}),
        .bool => |flag| try alloc.dupe(u8, if (flag) "true" else "false"),
        .null => if (std.mem.startsWith(u8, field, "/")) try alloc.dupe(u8, "") else return error.UnsupportedQueryRequest,
        else => return error.UnsupportedQueryRequest,
    };
    errdefer alloc.free(raw);
    if (!std.mem.startsWith(u8, field, "/")) return raw;
    const kind = switch (value) {
        .string => "string",
        .integer, .float => "number",
        .bool => "bool",
        .null => "null",
        else => unreachable,
    };
    const typed = try db_mod.algebraic.token.canonicalTupleAlloc(alloc, &.{ kind, raw });
    alloc.free(raw);
    return typed;
}

fn algebraicConstraintBoolValueAlloc(alloc: std.mem.Allocator, field: []const u8, value: bool) AlgebraicConstraintCollectError![]u8 {
    const raw = if (value) "true" else "false";
    if (!std.mem.startsWith(u8, field, "/")) return try alloc.dupe(u8, raw);
    return try db_mod.algebraic.token.canonicalTupleAlloc(alloc, &.{ "bool", raw });
}

test "algebraic constraints accept scalar bool query and structured filters" {
    const alloc = std.testing.allocator;

    const from_bool = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .bool_field = .{ .field = "published", .value = true } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_bool);
    try std.testing.expectEqual(@as(usize, 1), from_bool.len);
    try std.testing.expectEqualStrings("published", from_bool[0].field);
    try std.testing.expectEqualStrings("true", from_bool[0].value);

    const bool_must_queries = [_]db_mod.types.TextQuery{
        .{ .term = .{ .field = "/tier", .term = "gold" } },
        .{ .prefix = .{ .field = "/tenant", .prefix = "ac" } },
    };
    const from_path_bool_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .full_text = .{ .bool_query = .{ .must = bool_must_queries[0..] } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_bool_query);
    try std.testing.expectEqual(@as(usize, 2), from_path_bool_query.len);
    try std.testing.expectEqualStrings("/tier", from_path_bool_query[0].field);
    const top_level_bool_term_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_bool_query[0].value);
    defer {
        for (top_level_bool_term_parts) |part| alloc.free(part);
        alloc.free(top_level_bool_term_parts);
    }
    try std.testing.expectEqual(@as(usize, 2), top_level_bool_term_parts.len);
    try std.testing.expectEqualStrings("string", top_level_bool_term_parts[0]);
    try std.testing.expectEqualStrings("gold", top_level_bool_term_parts[1]);
    try std.testing.expectEqualStrings("/tenant", from_path_bool_query[1].field);
    const top_level_bool_prefix_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_bool_query[1].value);
    defer {
        for (top_level_bool_prefix_parts) |part| alloc.free(part);
        alloc.free(top_level_bool_prefix_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), top_level_bool_prefix_parts.len);
    try std.testing.expectEqualStrings("pathfact-prefix:v1", top_level_bool_prefix_parts[0]);
    try std.testing.expectEqualStrings("string", top_level_bool_prefix_parts[1]);
    try std.testing.expectEqualStrings("ac", top_level_bool_prefix_parts[2]);

    const bool_optional_should_queries = [_]db_mod.types.TextQuery{
        .{ .term = .{ .field = "/region", .term = "west" } },
    };
    const from_path_bool_optional_should_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .full_text = .{ .bool_query = .{
            .must = bool_must_queries[0..],
            .should = bool_optional_should_queries[0..],
            .min_should = 0,
        } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_bool_optional_should_query);
    try std.testing.expectEqual(@as(usize, 2), from_path_bool_optional_should_query.len);
    try std.testing.expectEqualStrings("/tier", from_path_bool_optional_should_query[0].field);
    try std.testing.expectEqualStrings("/tenant", from_path_bool_optional_should_query[1].field);

    const required_optional_should_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .full_text = .{ .bool_query = .{
            .must = bool_must_queries[0..],
            .should = bool_optional_should_queries[0..],
            .min_should = 1,
        } },
    });
    try std.testing.expect(required_optional_should_query == null);

    const bool_should_queries = [_]db_mod.types.TextQuery{
        .{ .term = .{ .field = "/tier", .term = "gold" } },
        .{ .term = .{ .field = "/tier", .term = "silver" } },
    };
    const top_level_should_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .full_text = .{ .bool_query = .{ .should = bool_should_queries[0..], .min_should = 1 } },
    })).?;
    defer freeAlgebraicConstraints(alloc, top_level_should_query);
    try std.testing.expectEqual(@as(usize, 1), top_level_should_query.len);
    try std.testing.expectEqualStrings("/tier", top_level_should_query[0].field);
    const top_level_should_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, top_level_should_query[0].value);
    defer {
        for (top_level_should_parts) |part| alloc.free(part);
        alloc.free(top_level_should_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), top_level_should_parts.len);
    try std.testing.expectEqualStrings("pathfact-any:v1", top_level_should_parts[0]);

    const implicit_top_level_should_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .full_text = .{ .bool_query = .{ .should = bool_should_queries[0..] } },
    })).?;
    defer freeAlgebraicConstraints(alloc, implicit_top_level_should_query);
    try std.testing.expectEqual(@as(usize, 1), implicit_top_level_should_query.len);
    try std.testing.expectEqualStrings("/tier", implicit_top_level_should_query[0].field);
    const implicit_top_level_should_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, implicit_top_level_should_query[0].value);
    defer {
        for (implicit_top_level_should_parts) |part| alloc.free(part);
        alloc.free(implicit_top_level_should_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), implicit_top_level_should_parts.len);
    try std.testing.expectEqualStrings("pathfact-any:v1", implicit_top_level_should_parts[0]);

    const two_required_should_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .full_text = .{ .bool_query = .{ .should = bool_should_queries[0..], .min_should = 2 } },
    });
    try std.testing.expect(two_required_should_query == null);

    const mixed_field_should_queries = [_]db_mod.types.TextQuery{
        .{ .term = .{ .field = "/tier", .term = "gold" } },
        .{ .term = .{ .field = "/region", .term = "west" } },
    };
    const mixed_field_should_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .full_text = .{ .bool_query = .{ .should = mixed_field_should_queries[0..], .min_should = 1 } },
    });
    try std.testing.expect(mixed_field_should_query == null);

    const bool_must_not_queries = [_]db_mod.types.TextQuery{.{ .term = .{ .field = "/tier", .term = "gold" } }};
    const top_level_must_not_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .full_text = .{ .bool_query = .{ .must_not = bool_must_not_queries[0..] } },
    });
    try std.testing.expect(top_level_must_not_query == null);

    const from_path_term_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .term = .{ .field = "/tier", .term = "gold" } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_term_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_term_query.len);
    try std.testing.expectEqualStrings("/tier", from_path_term_query[0].field);
    const top_level_term_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_term_query[0].value);
    defer {
        for (top_level_term_parts) |part| alloc.free(part);
        alloc.free(top_level_term_parts);
    }
    try std.testing.expectEqual(@as(usize, 2), top_level_term_parts.len);
    try std.testing.expectEqualStrings("string", top_level_term_parts[0]);
    try std.testing.expectEqualStrings("gold", top_level_term_parts[1]);

    const from_path_match_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .match = .{ .field = "/tier", .text = "OLD" } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_match_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_match_query.len);
    try std.testing.expectEqualStrings("/tier", from_path_match_query[0].field);
    const top_level_match_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_match_query[0].value);
    defer {
        for (top_level_match_parts) |part| alloc.free(part);
        alloc.free(top_level_match_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), top_level_match_parts.len);
    try std.testing.expectEqualStrings("pathfact-match:v1", top_level_match_parts[0]);
    try std.testing.expectEqualStrings("string", top_level_match_parts[1]);
    try std.testing.expectEqualStrings("OLD", top_level_match_parts[2]);

    const analyzed_path_match_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .match = .{ .field = "/tier", .text = "gold", .analyzer = "default" } },
    });
    try std.testing.expect(analyzed_path_match_query == null);

    const from_path_fuzzy_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .fuzzy = .{ .field = "/tier", .term = "gild", .max_edits = 1, .prefix_len = 1 } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_fuzzy_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_fuzzy_query.len);
    try std.testing.expectEqualStrings("/tier", from_path_fuzzy_query[0].field);
    const top_level_fuzzy_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_fuzzy_query[0].value);
    defer {
        for (top_level_fuzzy_parts) |part| alloc.free(part);
        alloc.free(top_level_fuzzy_parts);
    }
    try std.testing.expectEqual(@as(usize, 5), top_level_fuzzy_parts.len);
    try std.testing.expectEqualStrings("pathfact-fuzzy:v1", top_level_fuzzy_parts[0]);
    try std.testing.expectEqualStrings("string", top_level_fuzzy_parts[1]);
    try std.testing.expectEqualStrings("gild", top_level_fuzzy_parts[2]);
    try std.testing.expectEqualStrings("1", top_level_fuzzy_parts[3]);
    try std.testing.expectEqualStrings("1", top_level_fuzzy_parts[4]);

    const unbounded_path_fuzzy_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .fuzzy = .{ .field = "/tier", .term = "gold", .max_edits = 1 } },
    });
    try std.testing.expect(unbounded_path_fuzzy_query == null);

    const auto_path_fuzzy_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .fuzzy = .{ .field = "/tier", .term = "gold", .auto_fuzzy = true, .prefix_len = 1 } },
    });
    try std.testing.expect(auto_path_fuzzy_query == null);

    const non_path_fuzzy_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .fuzzy = .{ .field = "tier", .term = "gold", .max_edits = 1, .prefix_len = 1 } },
    });
    try std.testing.expect(non_path_fuzzy_query == null);

    const from_path_prefix_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .prefix = .{ .field = "/tier", .prefix = "go" } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_prefix_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_prefix_query.len);
    try std.testing.expectEqualStrings("/tier", from_path_prefix_query[0].field);
    const top_level_prefix_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_prefix_query[0].value);
    defer {
        for (top_level_prefix_parts) |part| alloc.free(part);
        alloc.free(top_level_prefix_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), top_level_prefix_parts.len);
    try std.testing.expectEqualStrings("pathfact-prefix:v1", top_level_prefix_parts[0]);
    try std.testing.expectEqualStrings("string", top_level_prefix_parts[1]);
    try std.testing.expectEqualStrings("go", top_level_prefix_parts[2]);

    const non_path_prefix_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .prefix = .{ .field = "tier", .prefix = "go" } },
    });
    try std.testing.expect(non_path_prefix_query == null);

    const from_path_wildcard_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .wildcard = .{ .field = "/tier", .pattern = "go*" } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_wildcard_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_wildcard_query.len);
    try std.testing.expectEqualStrings("/tier", from_path_wildcard_query[0].field);
    const top_level_wildcard_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_wildcard_query[0].value);
    defer {
        for (top_level_wildcard_parts) |part| alloc.free(part);
        alloc.free(top_level_wildcard_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), top_level_wildcard_parts.len);
    try std.testing.expectEqualStrings("pathfact-wildcard:v1", top_level_wildcard_parts[0]);
    try std.testing.expectEqualStrings("string", top_level_wildcard_parts[1]);
    try std.testing.expectEqualStrings("go*", top_level_wildcard_parts[2]);

    const leading_wildcard_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .wildcard = .{ .field = "/tier", .pattern = "*old" } },
    });
    try std.testing.expect(leading_wildcard_query == null);

    const non_path_wildcard_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .wildcard = .{ .field = "tier", .pattern = "go*" } },
    });
    try std.testing.expect(non_path_wildcard_query == null);

    const from_path_regexp_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .regexp = .{ .field = "/tier", .pattern = "go.*" } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_regexp_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_regexp_query.len);
    try std.testing.expectEqualStrings("/tier", from_path_regexp_query[0].field);
    const top_level_regexp_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_regexp_query[0].value);
    defer {
        for (top_level_regexp_parts) |part| alloc.free(part);
        alloc.free(top_level_regexp_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), top_level_regexp_parts.len);
    try std.testing.expectEqualStrings("pathfact-regexp:v1", top_level_regexp_parts[0]);
    try std.testing.expectEqualStrings("string", top_level_regexp_parts[1]);
    try std.testing.expectEqualStrings("go.*", top_level_regexp_parts[2]);

    const leading_regexp_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .regexp = .{ .field = "/tier", .pattern = ".*old" } },
    });
    try std.testing.expect(leading_regexp_query == null);

    const invalid_regexp_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .regexp = .{ .field = "/tier", .pattern = "go(" } },
    });
    try std.testing.expect(invalid_regexp_query == null);

    const non_path_regexp_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .regexp = .{ .field = "tier", .pattern = "go.*" } },
    });
    try std.testing.expect(non_path_regexp_query == null);

    const from_path_numeric_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .numeric_range = .{ .field = "/amount", .min = 10, .max = 30, .inclusive_min = true, .inclusive_max = false } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_numeric_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_numeric_query.len);
    try std.testing.expectEqualStrings("/amount", from_path_numeric_query[0].field);
    const top_level_amount_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_numeric_query[0].value);
    defer {
        for (top_level_amount_range_parts) |part| alloc.free(part);
        alloc.free(top_level_amount_range_parts);
    }
    try std.testing.expectEqual(@as(usize, 6), top_level_amount_range_parts.len);
    try std.testing.expectEqualStrings("pathfact-numeric-range:v1", top_level_amount_range_parts[0]);
    try std.testing.expectEqualStrings("number", top_level_amount_range_parts[1]);
    try std.testing.expectEqualStrings("10", top_level_amount_range_parts[2]);
    try std.testing.expectEqualStrings("30", top_level_amount_range_parts[3]);
    try std.testing.expectEqualStrings("1", top_level_amount_range_parts[4]);
    try std.testing.expectEqualStrings("0", top_level_amount_range_parts[5]);

    const non_path_numeric_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .numeric_range = .{ .field = "amount", .min = 10 } },
    });
    try std.testing.expect(non_path_numeric_query == null);

    const from_path_term_range_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .term_range = .{ .field = "/status", .min = "active", .max = "archived", .inclusive_min = true, .inclusive_max = false } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_term_range_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_term_range_query.len);
    try std.testing.expectEqualStrings("/status", from_path_term_range_query[0].field);
    const top_level_term_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_term_range_query[0].value);
    defer {
        for (top_level_term_range_parts) |part| alloc.free(part);
        alloc.free(top_level_term_range_parts);
    }
    try std.testing.expectEqual(@as(usize, 6), top_level_term_range_parts.len);
    try std.testing.expectEqualStrings("pathfact-term-range:v1", top_level_term_range_parts[0]);
    try std.testing.expectEqualStrings("string", top_level_term_range_parts[1]);
    try std.testing.expectEqualStrings("active", top_level_term_range_parts[2]);
    try std.testing.expectEqualStrings("archived", top_level_term_range_parts[3]);
    try std.testing.expectEqualStrings("1", top_level_term_range_parts[4]);
    try std.testing.expectEqualStrings("0", top_level_term_range_parts[5]);

    const unbounded_term_range_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .term_range = .{ .field = "/status" } },
    });
    try std.testing.expect(unbounded_term_range_query == null);

    const non_path_term_range_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .term_range = .{ .field = "status", .min = "active" } },
    });
    try std.testing.expect(non_path_term_range_query == null);

    const from_path_ip_range_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .ip_range = .{ .field = "/client_ip", .cidr = "10.1.0.0/16" } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_ip_range_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_ip_range_query.len);
    try std.testing.expectEqualStrings("/client_ip", from_path_ip_range_query[0].field);
    const top_level_ip_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_ip_range_query[0].value);
    defer {
        for (top_level_ip_range_parts) |part| alloc.free(part);
        alloc.free(top_level_ip_range_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), top_level_ip_range_parts.len);
    try std.testing.expectEqualStrings("pathfact-ip-range:v1", top_level_ip_range_parts[0]);
    try std.testing.expectEqualStrings("ipv4", top_level_ip_range_parts[1]);
    try std.testing.expectEqualStrings("10.1.0.0/16", top_level_ip_range_parts[2]);

    const invalid_ip_range_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .ip_range = .{ .field = "/client_ip", .cidr = "10.999.0.0/16" } },
    });
    try std.testing.expect(invalid_ip_range_query == null);

    const non_path_ip_range_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .ip_range = .{ .field = "client_ip", .cidr = "10.1.0.0/16" } },
    });
    try std.testing.expect(non_path_ip_range_query == null);

    const from_path_geo_bbox_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .geo_bbox = .{ .field = "/location", .min_lat = 37.70, .min_lon = -122.50, .max_lat = 37.80, .max_lon = -122.30 } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_geo_bbox_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_geo_bbox_query.len);
    try std.testing.expectEqualStrings("/location", from_path_geo_bbox_query[0].field);
    const top_level_geo_bbox_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_geo_bbox_query[0].value);
    defer {
        for (top_level_geo_bbox_parts) |part| alloc.free(part);
        alloc.free(top_level_geo_bbox_parts);
    }
    try std.testing.expectEqual(@as(usize, 6), top_level_geo_bbox_parts.len);
    try std.testing.expectEqualStrings("pathfact-geo-bbox:v1", top_level_geo_bbox_parts[0]);
    try std.testing.expectEqualStrings("geo_point", top_level_geo_bbox_parts[1]);
    try std.testing.expectEqualStrings("37.7", top_level_geo_bbox_parts[2]);
    try std.testing.expectEqualStrings("-122.5", top_level_geo_bbox_parts[3]);
    try std.testing.expectEqualStrings("37.8", top_level_geo_bbox_parts[4]);
    try std.testing.expectEqualStrings("-122.3", top_level_geo_bbox_parts[5]);

    const from_path_geo_distance_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .geo_distance = .{ .field = "/location", .lat = 37.7749, .lon = -122.4194, .radius_meters = 2000 } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_geo_distance_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_geo_distance_query.len);
    try std.testing.expectEqualStrings("/location", from_path_geo_distance_query[0].field);
    const top_level_geo_distance_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_geo_distance_query[0].value);
    defer {
        for (top_level_geo_distance_parts) |part| alloc.free(part);
        alloc.free(top_level_geo_distance_parts);
    }
    try std.testing.expectEqual(@as(usize, 5), top_level_geo_distance_parts.len);
    try std.testing.expectEqualStrings("pathfact-geo-distance:v1", top_level_geo_distance_parts[0]);
    try std.testing.expectEqualStrings("geo_point", top_level_geo_distance_parts[1]);
    try std.testing.expectEqualStrings("37.7749", top_level_geo_distance_parts[2]);
    try std.testing.expectEqualStrings("-122.4194", top_level_geo_distance_parts[3]);
    try std.testing.expectEqualStrings("2000", top_level_geo_distance_parts[4]);

    const direct_shape_polygon = [_]db_mod.types.GeoPoint{
        .{ .lat = 37.0, .lon = -123.0 },
        .{ .lat = 38.0, .lon = -123.0 },
        .{ .lat = 38.0, .lon = -122.0 },
        .{ .lat = 37.0, .lon = -122.0 },
    };
    const direct_shape_polygons = [_][]const db_mod.types.GeoPoint{direct_shape_polygon[0..]};
    const from_path_geo_shape_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .geo_shape = .{
            .field = "/location",
            .relation = .intersects,
            .polygons = direct_shape_polygons[0..],
        } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_geo_shape_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_geo_shape_query.len);
    try std.testing.expectEqualStrings("/location", from_path_geo_shape_query[0].field);
    const top_level_geo_shape_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_geo_shape_query[0].value);
    defer {
        for (top_level_geo_shape_parts) |part| alloc.free(part);
        alloc.free(top_level_geo_shape_parts);
    }
    try std.testing.expectEqual(@as(usize, 13), top_level_geo_shape_parts.len);
    try std.testing.expectEqualStrings("pathfact-geo-shape:v1", top_level_geo_shape_parts[0]);
    try std.testing.expectEqualStrings("geo_point", top_level_geo_shape_parts[1]);
    try std.testing.expectEqualStrings("intersects", top_level_geo_shape_parts[2]);
    try std.testing.expectEqualStrings("1", top_level_geo_shape_parts[3]);
    try std.testing.expectEqualStrings("4", top_level_geo_shape_parts[4]);

    const contains_geo_shape_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .geo_shape = .{
            .field = "/location",
            .relation = .contains,
            .polygons = direct_shape_polygons[0..],
        } },
    });
    try std.testing.expect(contains_geo_shape_query == null);

    const non_path_geo_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .geo_bbox = .{ .field = "location", .min_lat = 37.70, .min_lon = -122.50, .max_lat = 37.80, .max_lon = -122.30 } },
    });
    try std.testing.expect(non_path_geo_query == null);

    const from_path_date_query = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .date_range = .{ .field = "/published_at", .start_ns = 1767225600000000000, .end_ns = 1767312000000000000, .inclusive_start = true, .inclusive_end = false } },
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_date_query);
    try std.testing.expectEqual(@as(usize, 1), from_path_date_query.len);
    try std.testing.expectEqualStrings("/published_at", from_path_date_query[0].field);
    const top_level_date_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_date_query[0].value);
    defer {
        for (top_level_date_range_parts) |part| alloc.free(part);
        alloc.free(top_level_date_range_parts);
    }
    try std.testing.expectEqual(@as(usize, 6), top_level_date_range_parts.len);
    try std.testing.expectEqualStrings("pathfact-date-range:v1", top_level_date_range_parts[0]);
    try std.testing.expectEqualStrings("datetime", top_level_date_range_parts[1]);
    try std.testing.expectEqualStrings("1767225600000000000", top_level_date_range_parts[2]);
    try std.testing.expectEqualStrings("1767312000000000000", top_level_date_range_parts[3]);
    try std.testing.expectEqualStrings("1", top_level_date_range_parts[4]);
    try std.testing.expectEqualStrings("0", top_level_date_range_parts[5]);

    const non_path_date_query = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .date_range = .{ .field = "published_at", .start_ns = 1767225600000000000 } },
    });
    try std.testing.expect(non_path_date_query == null);

    const from_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"must\":[{\"term\":{\"tenant\":\"t1\"}},{\"bool_field\":{\"field\":\"paid\",\"value\":false}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_filter);
    try std.testing.expectEqual(@as(usize, 2), from_filter.len);
    try std.testing.expectEqualStrings("tenant", from_filter[0].field);
    try std.testing.expectEqualStrings("t1", from_filter[0].value);
    try std.testing.expectEqualStrings("paid", from_filter[1].field);
    try std.testing.expectEqualStrings("false", from_filter[1].value);

    const from_numeric_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"term\":{\"amount\":42}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_numeric_filter);
    try std.testing.expectEqual(@as(usize, 1), from_numeric_filter.len);
    try std.testing.expectEqualStrings("amount", from_numeric_filter[0].field);
    try std.testing.expectEqualStrings("42", from_numeric_filter[0].value);

    const from_path_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"term\":{\"/active\":true}},{\"term\":{\"/tier\":\"gold\"}},{\"term\":{\"/deleted_at\":null}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_filter);
    try std.testing.expectEqual(@as(usize, 3), from_path_filter.len);
    try std.testing.expectEqualStrings("/active", from_path_filter[0].field);
    const active_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_filter[0].value);
    defer {
        for (active_parts) |part| alloc.free(part);
        alloc.free(active_parts);
    }
    try std.testing.expectEqual(@as(usize, 2), active_parts.len);
    try std.testing.expectEqualStrings("bool", active_parts[0]);
    try std.testing.expectEqualStrings("true", active_parts[1]);
    try std.testing.expectEqualStrings("/tier", from_path_filter[1].field);
    const tier_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_filter[1].value);
    defer {
        for (tier_parts) |part| alloc.free(part);
        alloc.free(tier_parts);
    }
    try std.testing.expectEqual(@as(usize, 2), tier_parts.len);
    try std.testing.expectEqualStrings("string", tier_parts[0]);
    try std.testing.expectEqualStrings("gold", tier_parts[1]);
    try std.testing.expectEqualStrings("/deleted_at", from_path_filter[2].field);
    const null_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_filter[2].value);
    defer {
        for (null_parts) |part| alloc.free(part);
        alloc.free(null_parts);
    }
    try std.testing.expectEqual(@as(usize, 2), null_parts.len);
    try std.testing.expectEqualStrings("null", null_parts[0]);
    try std.testing.expectEqualStrings("", null_parts[1]);

    const from_direct_path_term_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"term\":\"gold\",\"field\":\"/tier\"}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_direct_path_term_filter);
    try std.testing.expectEqual(@as(usize, 1), from_direct_path_term_filter.len);
    try std.testing.expectEqualStrings("/tier", from_direct_path_term_filter[0].field);
    const direct_path_term_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_direct_path_term_filter[0].value);
    defer {
        for (direct_path_term_parts) |part| alloc.free(part);
        alloc.free(direct_path_term_parts);
    }
    try std.testing.expectEqual(@as(usize, 2), direct_path_term_parts.len);
    try std.testing.expectEqualStrings("string", direct_path_term_parts[0]);
    try std.testing.expectEqualStrings("gold", direct_path_term_parts[1]);

    const from_direct_path_alias_term_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"term\":\"gold\",\"path\":\"/tier\"}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_direct_path_alias_term_filter);
    try std.testing.expectEqual(@as(usize, 1), from_direct_path_alias_term_filter.len);
    try std.testing.expectEqualStrings("/tier", from_direct_path_alias_term_filter[0].field);
    const direct_path_alias_term_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_direct_path_alias_term_filter[0].value);
    defer {
        for (direct_path_alias_term_parts) |part| alloc.free(part);
        alloc.free(direct_path_alias_term_parts);
    }
    try std.testing.expectEqual(@as(usize, 2), direct_path_alias_term_parts.len);
    try std.testing.expectEqualStrings("string", direct_path_alias_term_parts[0]);
    try std.testing.expectEqualStrings("gold", direct_path_alias_term_parts[1]);

    const from_field_value_path_term_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"term\":{\"field\":\"/tier\",\"value\":\"gold\"}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_field_value_path_term_filter);
    try std.testing.expectEqual(@as(usize, 1), from_field_value_path_term_filter.len);
    try std.testing.expectEqualStrings("/tier", from_field_value_path_term_filter[0].field);
    const field_value_path_term_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_field_value_path_term_filter[0].value);
    defer {
        for (field_value_path_term_parts) |part| alloc.free(part);
        alloc.free(field_value_path_term_parts);
    }
    try std.testing.expectEqual(@as(usize, 2), field_value_path_term_parts.len);
    try std.testing.expectEqualStrings("string", field_value_path_term_parts[0]);
    try std.testing.expectEqualStrings("gold", field_value_path_term_parts[1]);

    const from_wrapped_path_alias_term_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"term\":{\"path\":\"/tier\",\"value\":\"gold\"}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_wrapped_path_alias_term_filter);
    try std.testing.expectEqual(@as(usize, 1), from_wrapped_path_alias_term_filter.len);
    try std.testing.expectEqualStrings("/tier", from_wrapped_path_alias_term_filter[0].field);

    const from_terms_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":{\"terms\":{\"tenant\":[\"t1\"]}},\"must\":{\"terms\":{\"field\":\"region\",\"values\":[\"west\"]}}}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_terms_filter);
    try std.testing.expectEqual(@as(usize, 2), from_terms_filter.len);
    var saw_tenant_terms = false;
    var saw_region_terms = false;
    for (from_terms_filter) |constraint| {
        if (std.mem.eql(u8, constraint.field, "tenant") and std.mem.eql(u8, constraint.value, "t1")) saw_tenant_terms = true;
        if (std.mem.eql(u8, constraint.field, "region") and std.mem.eql(u8, constraint.value, "west")) saw_region_terms = true;
    }
    try std.testing.expect(saw_tenant_terms);
    try std.testing.expect(saw_region_terms);

    const from_optional_should_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":{\"terms\":{\"tenant\":[\"t1\"]}},\"should\":[{\"term\":{\"region\":\"west\"}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_optional_should_filter);
    try std.testing.expectEqual(@as(usize, 1), from_optional_should_filter.len);
    try std.testing.expectEqualStrings("tenant", from_optional_should_filter[0].field);
    try std.testing.expectEqualStrings("t1", from_optional_should_filter[0].value);

    const from_explicit_optional_should_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"must\":{\"terms\":{\"field\":\"region\",\"values\":[\"west\"]}},\"should\":[{\"term\":{\"tenant\":\"t1\"}}],\"minimum_should_match\":0}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_explicit_optional_should_filter);
    try std.testing.expectEqual(@as(usize, 1), from_explicit_optional_should_filter.len);
    try std.testing.expectEqualStrings("region", from_explicit_optional_should_filter[0].field);
    try std.testing.expectEqualStrings("west", from_explicit_optional_should_filter[0].value);

    const unsupported_required_optional_should_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"must\":{\"terms\":{\"field\":\"region\",\"values\":[\"west\"]}},\"should\":[{\"term\":{\"tenant\":\"t1\"}}],\"minimum_should_match\":1}}",
    });
    try std.testing.expect(unsupported_required_optional_should_filter == null);

    const from_path_should_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"should\":[{\"term\":{\"/tier\":\"gold\"}},{\"term\":{\"/tier\":\"silver\"}}],\"minimum_should_match\":1}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_should_filter);
    try std.testing.expectEqual(@as(usize, 1), from_path_should_filter.len);
    try std.testing.expectEqualStrings("/tier", from_path_should_filter[0].field);
    const filter_should_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_should_filter[0].value);
    defer {
        for (filter_should_parts) |part| alloc.free(part);
        alloc.free(filter_should_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), filter_should_parts.len);
    try std.testing.expectEqualStrings("pathfact-any:v1", filter_should_parts[0]);

    const from_path_disjuncts_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"disjuncts\":[{\"term\":{\"/tier\":\"gold\"}},{\"term\":{\"/tier\":\"bronze\"}}]}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_disjuncts_filter);
    try std.testing.expectEqual(@as(usize, 1), from_path_disjuncts_filter.len);
    try std.testing.expectEqualStrings("/tier", from_path_disjuncts_filter[0].field);
    const disjuncts_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_disjuncts_filter[0].value);
    defer {
        for (disjuncts_parts) |part| alloc.free(part);
        alloc.free(disjuncts_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), disjuncts_parts.len);
    try std.testing.expectEqualStrings("pathfact-any:v1", disjuncts_parts[0]);

    const from_direct_path_disjuncts_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"disjuncts\":[{\"term\":\"gold\",\"field\":\"/tier\"},{\"term\":\"bronze\",\"field\":\"/tier\"}]}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_direct_path_disjuncts_filter);
    try std.testing.expectEqual(@as(usize, 1), from_direct_path_disjuncts_filter.len);
    try std.testing.expectEqualStrings("/tier", from_direct_path_disjuncts_filter[0].field);
    const direct_disjuncts_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_direct_path_disjuncts_filter[0].value);
    defer {
        for (direct_disjuncts_parts) |part| alloc.free(part);
        alloc.free(direct_disjuncts_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), direct_disjuncts_parts.len);
    try std.testing.expectEqualStrings("pathfact-any:v1", direct_disjuncts_parts[0]);

    const from_field_value_path_disjuncts_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"disjuncts\":[{\"term\":{\"field\":\"/tier\",\"term\":\"gold\"}},{\"term\":{\"field\":\"/tier\",\"value\":\"bronze\"}}]}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_field_value_path_disjuncts_filter);
    try std.testing.expectEqual(@as(usize, 1), from_field_value_path_disjuncts_filter.len);
    try std.testing.expectEqualStrings("/tier", from_field_value_path_disjuncts_filter[0].field);
    const field_value_disjuncts_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_field_value_path_disjuncts_filter[0].value);
    defer {
        for (field_value_disjuncts_parts) |part| alloc.free(part);
        alloc.free(field_value_disjuncts_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), field_value_disjuncts_parts.len);
    try std.testing.expectEqualStrings("pathfact-any:v1", field_value_disjuncts_parts[0]);

    const from_path_alias_disjuncts_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"disjuncts\":[{\"term\":\"gold\",\"path\":\"/tier\"},{\"term\":{\"path\":\"/tier\",\"value\":\"bronze\"}}]}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_alias_disjuncts_filter);
    try std.testing.expectEqual(@as(usize, 1), from_path_alias_disjuncts_filter.len);
    try std.testing.expectEqualStrings("/tier", from_path_alias_disjuncts_filter[0].field);
    const path_alias_disjuncts_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_alias_disjuncts_filter[0].value);
    defer {
        for (path_alias_disjuncts_parts) |part| alloc.free(part);
        alloc.free(path_alias_disjuncts_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), path_alias_disjuncts_parts.len);
    try std.testing.expectEqualStrings("pathfact-any:v1", path_alias_disjuncts_parts[0]);

    const mixed_path_should_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"should\":[{\"term\":{\"/tier\":\"gold\"}},{\"term\":{\"/region\":\"west\"}}],\"minimum_should_match\":1}}",
    });
    try std.testing.expect(mixed_path_should_filter == null);

    const from_path_terms_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"terms\":{\"path\":\"/tier\",\"values\":[\"gold\",\"silver\",null]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_terms_filter);
    try std.testing.expectEqual(@as(usize, 1), from_path_terms_filter.len);
    try std.testing.expectEqualStrings("/tier", from_path_terms_filter[0].field);
    const any_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_terms_filter[0].value);
    defer {
        for (any_parts) |part| alloc.free(part);
        alloc.free(any_parts);
    }
    try std.testing.expectEqual(@as(usize, 4), any_parts.len);
    try std.testing.expectEqualStrings("pathfact-any:v1", any_parts[0]);
    const any_first = try db_mod.algebraic.token.decodeTupleAlloc(alloc, any_parts[1]);
    defer {
        for (any_first) |part| alloc.free(part);
        alloc.free(any_first);
    }
    try std.testing.expectEqualStrings("string", any_first[0]);
    try std.testing.expectEqualStrings("gold", any_first[1]);
    const any_null = try db_mod.algebraic.token.decodeTupleAlloc(alloc, any_parts[3]);
    defer {
        for (any_null) |part| alloc.free(part);
        alloc.free(any_null);
    }
    try std.testing.expectEqualStrings("null", any_null[0]);
    try std.testing.expectEqualStrings("", any_null[1]);

    const non_path_multi_terms_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"terms\":{\"tenant\":[\"t1\",\"t2\"]}}",
    });
    try std.testing.expect(non_path_multi_terms_filter == null);

    const from_path_exists_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"exists\":{\"path\":\"/metadata/tier\"}},{\"exists\":\"/tenant\"}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_exists_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_exists_filter.len);
    try std.testing.expectEqualStrings("/metadata/tier", from_path_exists_filter[0].field);
    try std.testing.expectEqualStrings(db_mod.algebraic.index.path_fact_exists_constraint_value, from_path_exists_filter[0].value);
    try std.testing.expectEqualStrings("/tenant", from_path_exists_filter[1].field);
    try std.testing.expectEqualStrings(db_mod.algebraic.index.path_fact_exists_constraint_value, from_path_exists_filter[1].value);

    const from_path_prefix_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"prefix\":{\"path\":\"/metadata/tier\",\"prefix\":\"go\"}},{\"prefix\":{\"/tenant\":\"ac\"}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_prefix_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_prefix_filter.len);
    try std.testing.expectEqualStrings("/metadata/tier", from_path_prefix_filter[0].field);
    const tier_prefix_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_prefix_filter[0].value);
    defer {
        for (tier_prefix_parts) |part| alloc.free(part);
        alloc.free(tier_prefix_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), tier_prefix_parts.len);
    try std.testing.expectEqualStrings("pathfact-prefix:v1", tier_prefix_parts[0]);
    try std.testing.expectEqualStrings("string", tier_prefix_parts[1]);
    try std.testing.expectEqualStrings("go", tier_prefix_parts[2]);
    try std.testing.expectEqualStrings("/tenant", from_path_prefix_filter[1].field);
    const tenant_prefix_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_prefix_filter[1].value);
    defer {
        for (tenant_prefix_parts) |part| alloc.free(part);
        alloc.free(tenant_prefix_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), tenant_prefix_parts.len);
    try std.testing.expectEqualStrings("pathfact-prefix:v1", tenant_prefix_parts[0]);
    try std.testing.expectEqualStrings("string", tenant_prefix_parts[1]);
    try std.testing.expectEqualStrings("ac", tenant_prefix_parts[2]);

    const non_path_prefix_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"prefix\":{\"tenant\":\"ac\"}}",
    });
    try std.testing.expect(non_path_prefix_filter == null);

    const from_path_match_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"match\":{\"path\":\"/metadata/tier\",\"value\":\"OLD\"}},{\"match\":{\"/tenant\":\"ICE\"}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_match_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_match_filter.len);
    try std.testing.expectEqualStrings("/metadata/tier", from_path_match_filter[0].field);
    const tier_match_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_match_filter[0].value);
    defer {
        for (tier_match_parts) |part| alloc.free(part);
        alloc.free(tier_match_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), tier_match_parts.len);
    try std.testing.expectEqualStrings("pathfact-match:v1", tier_match_parts[0]);
    try std.testing.expectEqualStrings("string", tier_match_parts[1]);
    try std.testing.expectEqualStrings("OLD", tier_match_parts[2]);
    try std.testing.expectEqualStrings("/tenant", from_path_match_filter[1].field);
    const tenant_match_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_match_filter[1].value);
    defer {
        for (tenant_match_parts) |part| alloc.free(part);
        alloc.free(tenant_match_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), tenant_match_parts.len);
    try std.testing.expectEqualStrings("pathfact-match:v1", tenant_match_parts[0]);
    try std.testing.expectEqualStrings("string", tenant_match_parts[1]);
    try std.testing.expectEqualStrings("ICE", tenant_match_parts[2]);

    const sibling_path_match_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"match\":\"old\",\"field\":\"/metadata/tier\"}",
    })).?;
    defer freeAlgebraicConstraints(alloc, sibling_path_match_filter);
    try std.testing.expectEqual(@as(usize, 1), sibling_path_match_filter.len);
    try std.testing.expectEqualStrings("/metadata/tier", sibling_path_match_filter[0].field);

    const non_path_match_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"match\":{\"tenant\":\"ICE\"}}",
    });
    try std.testing.expect(non_path_match_filter == null);

    const from_path_wildcard_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"wildcard\":{\"path\":\"/metadata/tier\",\"pattern\":\"go*\"}},{\"wildcard\":{\"/tenant\":\"ac?e\"}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_wildcard_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_wildcard_filter.len);
    try std.testing.expectEqualStrings("/metadata/tier", from_path_wildcard_filter[0].field);
    const tier_wildcard_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_wildcard_filter[0].value);
    defer {
        for (tier_wildcard_parts) |part| alloc.free(part);
        alloc.free(tier_wildcard_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), tier_wildcard_parts.len);
    try std.testing.expectEqualStrings("pathfact-wildcard:v1", tier_wildcard_parts[0]);
    try std.testing.expectEqualStrings("string", tier_wildcard_parts[1]);
    try std.testing.expectEqualStrings("go*", tier_wildcard_parts[2]);
    try std.testing.expectEqualStrings("/tenant", from_path_wildcard_filter[1].field);
    const tenant_wildcard_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_wildcard_filter[1].value);
    defer {
        for (tenant_wildcard_parts) |part| alloc.free(part);
        alloc.free(tenant_wildcard_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), tenant_wildcard_parts.len);
    try std.testing.expectEqualStrings("pathfact-wildcard:v1", tenant_wildcard_parts[0]);
    try std.testing.expectEqualStrings("string", tenant_wildcard_parts[1]);
    try std.testing.expectEqualStrings("ac?e", tenant_wildcard_parts[2]);

    const leading_wildcard_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"wildcard\":{\"/tenant\":\"*ice\"}}",
    });
    try std.testing.expect(leading_wildcard_filter == null);

    const non_path_wildcard_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"wildcard\":{\"tenant\":\"ac*\"}}",
    });
    try std.testing.expect(non_path_wildcard_filter == null);

    const from_path_regexp_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"regexp\":{\"path\":\"/metadata/tier\",\"pattern\":\"go.*\"}},{\"regexp\":{\"/tenant\":\"ac.e\"}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_regexp_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_regexp_filter.len);
    try std.testing.expectEqualStrings("/metadata/tier", from_path_regexp_filter[0].field);
    const tier_regexp_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_regexp_filter[0].value);
    defer {
        for (tier_regexp_parts) |part| alloc.free(part);
        alloc.free(tier_regexp_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), tier_regexp_parts.len);
    try std.testing.expectEqualStrings("pathfact-regexp:v1", tier_regexp_parts[0]);
    try std.testing.expectEqualStrings("string", tier_regexp_parts[1]);
    try std.testing.expectEqualStrings("go.*", tier_regexp_parts[2]);
    try std.testing.expectEqualStrings("/tenant", from_path_regexp_filter[1].field);
    const tenant_regexp_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_regexp_filter[1].value);
    defer {
        for (tenant_regexp_parts) |part| alloc.free(part);
        alloc.free(tenant_regexp_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), tenant_regexp_parts.len);
    try std.testing.expectEqualStrings("pathfact-regexp:v1", tenant_regexp_parts[0]);
    try std.testing.expectEqualStrings("string", tenant_regexp_parts[1]);
    try std.testing.expectEqualStrings("ac.e", tenant_regexp_parts[2]);

    const leading_regexp_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"regexp\":{\"/tenant\":\".*ice\"}}",
    });
    try std.testing.expect(leading_regexp_filter == null);

    const invalid_regexp_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"regexp\":{\"/tenant\":\"ac(\"}}",
    });
    try std.testing.expect(invalid_regexp_filter == null);

    const non_path_regexp_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"regexp\":{\"tenant\":\"ac.*\"}}",
    });
    try std.testing.expect(non_path_regexp_filter == null);

    const from_path_fuzzy_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"fuzzy\":{\"path\":\"/metadata/tier\",\"query\":\"gild\",\"prefix_length\":1,\"max_edits\":1}},{\"fuzzy\":{\"/tenant\":{\"query\":\"alpine\",\"prefix_length\":2,\"max_edits\":1}}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_fuzzy_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_fuzzy_filter.len);
    try std.testing.expectEqualStrings("/metadata/tier", from_path_fuzzy_filter[0].field);
    const tier_fuzzy_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_fuzzy_filter[0].value);
    defer {
        for (tier_fuzzy_parts) |part| alloc.free(part);
        alloc.free(tier_fuzzy_parts);
    }
    try std.testing.expectEqual(@as(usize, 5), tier_fuzzy_parts.len);
    try std.testing.expectEqualStrings("pathfact-fuzzy:v1", tier_fuzzy_parts[0]);
    try std.testing.expectEqualStrings("string", tier_fuzzy_parts[1]);
    try std.testing.expectEqualStrings("gild", tier_fuzzy_parts[2]);
    try std.testing.expectEqualStrings("1", tier_fuzzy_parts[3]);
    try std.testing.expectEqualStrings("1", tier_fuzzy_parts[4]);
    try std.testing.expectEqualStrings("/tenant", from_path_fuzzy_filter[1].field);
    const tenant_fuzzy_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_fuzzy_filter[1].value);
    defer {
        for (tenant_fuzzy_parts) |part| alloc.free(part);
        alloc.free(tenant_fuzzy_parts);
    }
    try std.testing.expectEqual(@as(usize, 5), tenant_fuzzy_parts.len);
    try std.testing.expectEqualStrings("pathfact-fuzzy:v1", tenant_fuzzy_parts[0]);
    try std.testing.expectEqualStrings("string", tenant_fuzzy_parts[1]);
    try std.testing.expectEqualStrings("alpine", tenant_fuzzy_parts[2]);
    try std.testing.expectEqualStrings("1", tenant_fuzzy_parts[3]);
    try std.testing.expectEqualStrings("2", tenant_fuzzy_parts[4]);

    const unbounded_fuzzy_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"fuzzy\":{\"/tenant\":{\"query\":\"alice\",\"max_edits\":1}}}",
    });
    try std.testing.expect(unbounded_fuzzy_filter == null);

    const non_path_fuzzy_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"fuzzy\":{\"tenant\":{\"query\":\"alice\",\"prefix_length\":1}}}",
    });
    try std.testing.expect(non_path_fuzzy_filter == null);

    const from_path_numeric_range_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"numeric_range\":{\"path\":\"/amount\",\"min\":10,\"max\":30,\"inclusive_min\":true,\"inclusive_max\":false}},{\"range\":{\"/score\":{\"gte\":7,\"lt\":9}}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_numeric_range_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_numeric_range_filter.len);
    try std.testing.expectEqualStrings("/amount", from_path_numeric_range_filter[0].field);
    const amount_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_numeric_range_filter[0].value);
    defer {
        for (amount_range_parts) |part| alloc.free(part);
        alloc.free(amount_range_parts);
    }
    try std.testing.expectEqual(@as(usize, 6), amount_range_parts.len);
    try std.testing.expectEqualStrings("pathfact-numeric-range:v1", amount_range_parts[0]);
    try std.testing.expectEqualStrings("number", amount_range_parts[1]);
    try std.testing.expectEqualStrings("10", amount_range_parts[2]);
    try std.testing.expectEqualStrings("30", amount_range_parts[3]);
    try std.testing.expectEqualStrings("1", amount_range_parts[4]);
    try std.testing.expectEqualStrings("0", amount_range_parts[5]);
    try std.testing.expectEqualStrings("/score", from_path_numeric_range_filter[1].field);
    const score_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_numeric_range_filter[1].value);
    defer {
        for (score_range_parts) |part| alloc.free(part);
        alloc.free(score_range_parts);
    }
    try std.testing.expectEqual(@as(usize, 6), score_range_parts.len);
    try std.testing.expectEqualStrings("pathfact-numeric-range:v1", score_range_parts[0]);
    try std.testing.expectEqualStrings("number", score_range_parts[1]);
    try std.testing.expectEqualStrings("7", score_range_parts[2]);
    try std.testing.expectEqualStrings("9", score_range_parts[3]);
    try std.testing.expectEqualStrings("1", score_range_parts[4]);
    try std.testing.expectEqualStrings("0", score_range_parts[5]);

    const upper_only_path_range_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"range\":{\"field\":\"/score\",\"lte\":9}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, upper_only_path_range_filter);
    try std.testing.expectEqual(@as(usize, 1), upper_only_path_range_filter.len);
    const upper_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, upper_only_path_range_filter[0].value);
    defer {
        for (upper_range_parts) |part| alloc.free(part);
        alloc.free(upper_range_parts);
    }
    try std.testing.expectEqualStrings("", upper_range_parts[2]);
    try std.testing.expectEqualStrings("9", upper_range_parts[3]);
    try std.testing.expectEqualStrings("1", upper_range_parts[4]);
    try std.testing.expectEqualStrings("1", upper_range_parts[5]);

    const non_path_numeric_range_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"numeric_range\":{\"field\":\"amount\",\"min\":10}}",
    });
    try std.testing.expect(non_path_numeric_range_filter == null);

    const from_path_date_range_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"date_range\":{\"path\":\"/published_at\",\"start\":\"2026-01-02T00:00:00Z\",\"end\":\"2026-01-03T00:00:00Z\",\"inclusive_start\":true,\"inclusive_end\":false}},{\"range\":{\"/created_at\":{\"gte\":\"2026-01-04\",\"lt\":\"2026-01-05\"}}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_date_range_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_date_range_filter.len);
    try std.testing.expectEqualStrings("/published_at", from_path_date_range_filter[0].field);
    const published_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_date_range_filter[0].value);
    defer {
        for (published_range_parts) |part| alloc.free(part);
        alloc.free(published_range_parts);
    }
    try std.testing.expectEqual(@as(usize, 6), published_range_parts.len);
    try std.testing.expectEqualStrings("pathfact-date-range:v1", published_range_parts[0]);
    try std.testing.expectEqualStrings("datetime", published_range_parts[1]);
    try std.testing.expectEqualStrings("2026-01-02T00:00:00Z", published_range_parts[2]);
    try std.testing.expectEqualStrings("2026-01-03T00:00:00Z", published_range_parts[3]);
    try std.testing.expectEqualStrings("1", published_range_parts[4]);
    try std.testing.expectEqualStrings("0", published_range_parts[5]);
    try std.testing.expectEqualStrings("/created_at", from_path_date_range_filter[1].field);
    const created_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_date_range_filter[1].value);
    defer {
        for (created_range_parts) |part| alloc.free(part);
        alloc.free(created_range_parts);
    }
    try std.testing.expectEqual(@as(usize, 6), created_range_parts.len);
    try std.testing.expectEqualStrings("pathfact-date-range:v1", created_range_parts[0]);
    try std.testing.expectEqualStrings("datetime", created_range_parts[1]);
    try std.testing.expectEqualStrings("2026-01-04", created_range_parts[2]);
    try std.testing.expectEqualStrings("2026-01-05", created_range_parts[3]);
    try std.testing.expectEqualStrings("1", created_range_parts[4]);
    try std.testing.expectEqualStrings("0", created_range_parts[5]);

    const non_path_date_range_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"date_range\":{\"field\":\"published_at\",\"start\":\"2026-01-02T00:00:00Z\"}}",
    });
    try std.testing.expect(non_path_date_range_filter == null);

    const from_path_ip_range_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"ip_range\":{\"path\":\"/client_ip\",\"cidr\":\"10.1.0.0/16\"}},{\"ip_range\":{\"field\":\"/gateway_ip\",\"cidr\":\"192.168.1.10\"}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_ip_range_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_ip_range_filter.len);
    try std.testing.expectEqualStrings("/client_ip", from_path_ip_range_filter[0].field);
    const client_ip_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_ip_range_filter[0].value);
    defer {
        for (client_ip_parts) |part| alloc.free(part);
        alloc.free(client_ip_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), client_ip_parts.len);
    try std.testing.expectEqualStrings("pathfact-ip-range:v1", client_ip_parts[0]);
    try std.testing.expectEqualStrings("ipv4", client_ip_parts[1]);
    try std.testing.expectEqualStrings("10.1.0.0/16", client_ip_parts[2]);
    try std.testing.expectEqualStrings("/gateway_ip", from_path_ip_range_filter[1].field);
    const gateway_ip_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_ip_range_filter[1].value);
    defer {
        for (gateway_ip_parts) |part| alloc.free(part);
        alloc.free(gateway_ip_parts);
    }
    try std.testing.expectEqual(@as(usize, 3), gateway_ip_parts.len);
    try std.testing.expectEqualStrings("pathfact-ip-range:v1", gateway_ip_parts[0]);
    try std.testing.expectEqualStrings("ipv4", gateway_ip_parts[1]);
    try std.testing.expectEqualStrings("192.168.1.10", gateway_ip_parts[2]);

    const non_path_ip_range_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"ip_range\":{\"field\":\"client_ip\",\"cidr\":\"10.1.0.0/16\"}}",
    });
    try std.testing.expect(non_path_ip_range_filter == null);

    const invalid_ip_range_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"ip_range\":{\"path\":\"/client_ip\",\"cidr\":\"10.999.0.0/16\"}}",
    });
    try std.testing.expect(invalid_ip_range_filter == null);

    const from_path_geo_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"geo_bbox\":{\"path\":\"/location\",\"min_lat\":37.70,\"min_lon\":-122.50,\"max_lat\":37.80,\"max_lon\":-122.30}},{\"geo_distance\":{\"field\":\"/warehouse\",\"lat\":40.7128,\"lon\":-74.006,\"radius_meters\":5000}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_geo_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_geo_filter.len);
    try std.testing.expectEqualStrings("/location", from_path_geo_filter[0].field);
    const location_geo_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_geo_filter[0].value);
    defer {
        for (location_geo_parts) |part| alloc.free(part);
        alloc.free(location_geo_parts);
    }
    try std.testing.expectEqualStrings("pathfact-geo-bbox:v1", location_geo_parts[0]);
    try std.testing.expectEqualStrings("/warehouse", from_path_geo_filter[1].field);
    const warehouse_geo_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_geo_filter[1].value);
    defer {
        for (warehouse_geo_parts) |part| alloc.free(part);
        alloc.free(warehouse_geo_parts);
    }
    try std.testing.expectEqualStrings("pathfact-geo-distance:v1", warehouse_geo_parts[0]);

    const from_path_geo_shape_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"geo_shape\":{\"path\":\"/location\",\"relation\":\"within\",\"polygons\":[[{\"lat\":37.0,\"lon\":-123.0},{\"lat\":38.0,\"lon\":-123.0},{\"lat\":38.0,\"lon\":-122.0},{\"lat\":37.0,\"lon\":-122.0}]]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_geo_shape_filter);
    try std.testing.expectEqual(@as(usize, 1), from_path_geo_shape_filter.len);
    try std.testing.expectEqualStrings("/location", from_path_geo_shape_filter[0].field);
    const location_geo_shape_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_geo_shape_filter[0].value);
    defer {
        for (location_geo_shape_parts) |part| alloc.free(part);
        alloc.free(location_geo_shape_parts);
    }
    try std.testing.expectEqual(@as(usize, 13), location_geo_shape_parts.len);
    try std.testing.expectEqualStrings("pathfact-geo-shape:v1", location_geo_shape_parts[0]);
    try std.testing.expectEqualStrings("geo_point", location_geo_shape_parts[1]);
    try std.testing.expectEqualStrings("within", location_geo_shape_parts[2]);

    const unsupported_geo_shape_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"geo_shape\":{\"path\":\"/location\",\"relation\":\"contains\",\"polygons\":[[{\"lat\":37.0,\"lon\":-123.0},{\"lat\":38.0,\"lon\":-123.0},{\"lat\":38.0,\"lon\":-122.0}]]}}",
    });
    try std.testing.expect(unsupported_geo_shape_filter == null);

    const non_path_geo_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"geo_bbox\":{\"field\":\"location\",\"min_lat\":37.70,\"min_lon\":-122.50,\"max_lat\":37.80,\"max_lon\":-122.30}}",
    });
    try std.testing.expect(non_path_geo_filter == null);

    const invalid_geo_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"geo_distance\":{\"path\":\"/location\",\"lat\":95,\"lon\":-122.4194,\"radius_meters\":2000}}",
    });
    try std.testing.expect(invalid_geo_filter == null);

    const from_path_term_range_filter = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"bool\":{\"filter\":[{\"term_range\":{\"path\":\"/tenant\",\"min\":\"alpi\",\"max\":\"alpj\",\"inclusive_min\":true,\"inclusive_max\":false}},{\"range\":{\"/status\":{\"gte\":\"active\",\"lt\":\"archived\"}}}]}}",
    })).?;
    defer freeAlgebraicConstraints(alloc, from_path_term_range_filter);
    try std.testing.expectEqual(@as(usize, 2), from_path_term_range_filter.len);
    try std.testing.expectEqualStrings("/tenant", from_path_term_range_filter[0].field);
    const tenant_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_term_range_filter[0].value);
    defer {
        for (tenant_range_parts) |part| alloc.free(part);
        alloc.free(tenant_range_parts);
    }
    try std.testing.expectEqual(@as(usize, 6), tenant_range_parts.len);
    try std.testing.expectEqualStrings("pathfact-term-range:v1", tenant_range_parts[0]);
    try std.testing.expectEqualStrings("string", tenant_range_parts[1]);
    try std.testing.expectEqualStrings("alpi", tenant_range_parts[2]);
    try std.testing.expectEqualStrings("alpj", tenant_range_parts[3]);
    try std.testing.expectEqualStrings("1", tenant_range_parts[4]);
    try std.testing.expectEqualStrings("0", tenant_range_parts[5]);
    try std.testing.expectEqualStrings("/status", from_path_term_range_filter[1].field);
    const status_range_parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, from_path_term_range_filter[1].value);
    defer {
        for (status_range_parts) |part| alloc.free(part);
        alloc.free(status_range_parts);
    }
    try std.testing.expectEqual(@as(usize, 6), status_range_parts.len);
    try std.testing.expectEqualStrings("pathfact-term-range:v1", status_range_parts[0]);
    try std.testing.expectEqualStrings("string", status_range_parts[1]);
    try std.testing.expectEqualStrings("active", status_range_parts[2]);
    try std.testing.expectEqualStrings("archived", status_range_parts[3]);
    try std.testing.expectEqualStrings("1", status_range_parts[4]);
    try std.testing.expectEqualStrings("0", status_range_parts[5]);

    const non_path_term_range_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"term_range\":{\"field\":\"tenant\",\"min\":\"a\"}}",
    });
    try std.testing.expect(non_path_term_range_filter == null);

    const non_path_exists_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"exists\":\"tenant\"}",
    });
    try std.testing.expect(non_path_exists_filter == null);

    const multi_terms_filter = try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"terms\":{\"tenant\":[\"t1\",\"t2\"]}}",
    });
    try std.testing.expect(multi_terms_filter == null);
}

test "algebraic constraints reject top-level text term query" {
    const alloc = std.testing.allocator;
    const constraints = try algebraicConstraintsForRequestAlloc(alloc, .{
        .query = .{ .term = .{ .field = "body", .term = "published" } },
    });
    try std.testing.expect(constraints == null);
}

test "merge runtime preflight summary preserves structured filter exact counts only when every shard is exact" {
    const alloc = std.testing.allocator;

    var exact_left: db_mod.RuntimePreflightSummary = .{
        .structured_filter_doc_count_estimate = 2,
        .structured_filter_count_exact = true,
    };
    defer exact_left.deinit(alloc);
    try mergeRuntimePreflightSummary(alloc, &exact_left, .{
        .structured_filter_doc_count_estimate = 3,
        .structured_filter_count_exact = true,
    });
    try std.testing.expectEqual(@as(?u64, 5), exact_left.structured_filter_doc_count_estimate);
    try std.testing.expect(exact_left.structured_filter_count_exact);
    try std.testing.expectEqual(@as(?u32, 5), exact_left.result_doc_estimate);

    var mixed: db_mod.RuntimePreflightSummary = .{
        .structured_filter_doc_count_estimate = 2,
        .structured_filter_count_exact = true,
        .vector_worker_candidate_count = 1,
        .vector_worker_filter_constraint_count = 2,
        .vector_worker_requires_algebraic_filter_resolution = true,
    };
    defer mixed.deinit(alloc);
    try mergeRuntimePreflightSummary(alloc, &mixed, .{
        .structured_filter_doc_count_estimate = 7,
        .structured_filter_count_exact = false,
        .vector_worker_fallback_count = 1,
        .vector_worker_filter_constraint_count = 1,
    });
    try std.testing.expectEqual(@as(?u64, null), mixed.structured_filter_doc_count_estimate);
    try std.testing.expect(!mixed.structured_filter_count_exact);
    try std.testing.expectEqual(@as(?u64, 9), mixed.structured_filter_doc_count_lower_bound);
    try std.testing.expectEqual(@as(?u32, null), mixed.result_doc_estimate);
    try std.testing.expectEqual(@as(u32, 1), mixed.vector_worker_candidate_count);
    try std.testing.expectEqual(@as(u32, 1), mixed.vector_worker_fallback_count);
    try std.testing.expectEqual(@as(u32, 3), mixed.vector_worker_filter_constraint_count);
    try std.testing.expect(mixed.vector_worker_requires_algebraic_filter_resolution);

    var lower_bounds: db_mod.RuntimePreflightSummary = .{
        .structured_filter_doc_count_lower_bound = 4,
        .structured_filter_count_exact = false,
    };
    defer lower_bounds.deinit(alloc);
    try mergeRuntimePreflightSummary(alloc, &lower_bounds, .{
        .structured_filter_doc_count_lower_bound = 6,
        .structured_filter_count_exact = false,
    });
    try std.testing.expectEqual(@as(?u64, 10), lower_bounds.structured_filter_doc_count_lower_bound);
    try std.testing.expect(!lower_bounds.structured_filter_count_exact);
    try std.testing.expectEqual(@as(?u32, null), lower_bounds.result_doc_estimate);

    var sampled: db_mod.RuntimePreflightSummary = .{
        .structured_filter_doc_count_sample_estimate = 4,
        .structured_filter_count_sample_size = 8,
        .structured_filter_count_exact = false,
    };
    defer sampled.deinit(alloc);
    try mergeRuntimePreflightSummary(alloc, &sampled, .{
        .structured_filter_doc_count_sample_estimate = 6,
        .structured_filter_count_sample_size = 12,
        .structured_filter_count_exact = false,
    });
    try std.testing.expectEqual(@as(?u64, 10), sampled.structured_filter_doc_count_sample_estimate);
    try std.testing.expectEqual(@as(u32, 20), sampled.structured_filter_count_sample_size);
    try std.testing.expect(!sampled.structured_filter_count_exact);
    try std.testing.expectEqual(@as(?u32, 10), sampled.result_doc_estimate);
}

fn mergeRuntimePreflightTextQueryStats(
    alloc: std.mem.Allocator,
    target: *[]const distributed_stats_mod.TextFieldStats,
    extra: []const distributed_stats_mod.TextFieldStats,
) !void {
    const merged = try mergeDistributedTextStats(alloc, &[_][]const distributed_stats_mod.TextFieldStats{
        target.*,
        extra,
    });
    distributed_stats_mod.deinitTextFieldStats(alloc, target.*);
    target.* = merged;
}

fn mergeRuntimePreflightStrings(
    alloc: std.mem.Allocator,
    target: *[]const []const u8,
    extra: []const []const u8,
) !void {
    var items = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (items.items) |item| alloc.free(item);
        items.deinit(alloc);
    }
    for (target.*) |item| try appendUniqueRuntimePreflightString(alloc, &items, item);
    for (extra) |item| try appendUniqueRuntimePreflightString(alloc, &items, item);
    freeRuntimePreflightStringSlice(alloc, target.*);
    target.* = if (items.items.len == 0) &.{} else try items.toOwnedSlice(alloc);
}

fn appendUniqueRuntimePreflightString(
    alloc: std.mem.Allocator,
    items: *std.ArrayListUnmanaged([]const u8),
    value: []const u8,
) !void {
    for (items.items) |existing| {
        if (std.mem.eql(u8, existing, value)) return;
    }
    try items.append(alloc, try alloc.dupe(u8, value));
}

fn freeRuntimePreflightStringSlice(alloc: std.mem.Allocator, items: []const []const u8) void {
    for (items) |item| alloc.free(@constCast(item));
    if (items.len > 0) alloc.free(@constCast(items));
}

fn mergeRuntimePreflightTextEstimates(
    alloc: std.mem.Allocator,
    target: *[]const db_mod.TextIndexEstimate,
    extra: []const db_mod.TextIndexEstimate,
) !void {
    var items = std.ArrayListUnmanaged(db_mod.TextIndexEstimate).empty;
    errdefer {
        for (items.items) |*item| item.deinit(alloc);
        items.deinit(alloc);
    }

    for (target.*) |item| try items.append(alloc, .{
        .name = try alloc.dupe(u8, item.name),
        .doc_count = item.doc_count,
        .chunk_backed = item.chunk_backed,
        .group_chunk_parents = item.group_chunk_parents,
    });
    for (extra) |item| {
        for (items.items) |*existing| {
            if (!std.mem.eql(u8, existing.name, item.name)) continue;
            existing.doc_count += item.doc_count;
            existing.chunk_backed = existing.chunk_backed or item.chunk_backed;
            existing.group_chunk_parents = existing.group_chunk_parents or item.group_chunk_parents;
            break;
        } else {
            try items.append(alloc, .{
                .name = try alloc.dupe(u8, item.name),
                .doc_count = item.doc_count,
                .chunk_backed = item.chunk_backed,
                .group_chunk_parents = item.group_chunk_parents,
            });
        }
    }

    for (target.*) |*item| item.deinit(alloc);
    if (target.*.len > 0) alloc.free(@constCast(target.*));
    target.* = if (items.items.len == 0) &.{} else try items.toOwnedSlice(alloc);
}

fn mergeRuntimePreflightEmbeddingEstimates(
    alloc: std.mem.Allocator,
    target: *[]const db_mod.EmbeddingIndexEstimate,
    extra: []const db_mod.EmbeddingIndexEstimate,
) !void {
    var items = std.ArrayListUnmanaged(db_mod.EmbeddingIndexEstimate).empty;
    errdefer {
        for (items.items) |*item| item.deinit(alloc);
        items.deinit(alloc);
    }

    for (target.*) |item| try items.append(alloc, .{
        .name = try alloc.dupe(u8, item.name),
        .sparse = item.sparse,
        .doc_count = item.doc_count,
        .dims = item.dims,
        .chunk_backed = item.chunk_backed,
    });
    for (extra) |item| {
        for (items.items) |*existing| {
            if (!std.mem.eql(u8, existing.name, item.name) or existing.sparse != item.sparse) continue;
            existing.doc_count += item.doc_count;
            existing.chunk_backed = existing.chunk_backed or item.chunk_backed;
            if (existing.dims == 0) existing.dims = item.dims;
            break;
        } else {
            try items.append(alloc, .{
                .name = try alloc.dupe(u8, item.name),
                .sparse = item.sparse,
                .doc_count = item.doc_count,
                .dims = item.dims,
                .chunk_backed = item.chunk_backed,
            });
        }
    }

    for (target.*) |*item| item.deinit(alloc);
    if (target.*.len > 0) alloc.free(@constCast(target.*));
    target.* = if (items.items.len == 0) &.{} else try items.toOwnedSlice(alloc);
}

fn mergeRuntimePreflightGraphEstimates(
    alloc: std.mem.Allocator,
    target: *[]const db_mod.GraphIndexEstimate,
    extra: []const db_mod.GraphIndexEstimate,
) !void {
    var items = std.ArrayListUnmanaged(db_mod.GraphIndexEstimate).empty;
    errdefer {
        for (items.items) |*item| item.deinit(alloc);
        items.deinit(alloc);
    }

    for (target.*) |item| try items.append(alloc, .{
        .name = try alloc.dupe(u8, item.name),
        .edge_count = item.edge_count,
        .node_count = item.node_count,
    });
    for (extra) |item| {
        for (items.items) |*existing| {
            if (!std.mem.eql(u8, existing.name, item.name)) continue;
            existing.edge_count += item.edge_count;
            existing.node_count += item.node_count;
            break;
        } else {
            try items.append(alloc, .{
                .name = try alloc.dupe(u8, item.name),
                .edge_count = item.edge_count,
                .node_count = item.node_count,
            });
        }
    }

    for (target.*) |*item| item.deinit(alloc);
    if (target.*.len > 0) alloc.free(@constCast(target.*));
    target.* = if (items.items.len == 0) &.{} else try items.toOwnedSlice(alloc);
}

pub fn mergeDistributedTextStats(
    alloc: std.mem.Allocator,
    groups: []const []const distributed_stats_mod.TextFieldStats,
) ![]const distributed_stats_mod.TextFieldStats {
    var fields = std.StringHashMapUnmanaged(struct {
        doc_count: u32 = 0,
        total_field_len: u64 = 0,
        terms: std.StringHashMapUnmanaged(u32) = .{},
    }){};
    defer {
        var it = fields.iterator();
        while (it.next()) |entry| {
            var term_it = entry.value_ptr.terms.keyIterator();
            while (term_it.next()) |term| alloc.free(term.*);
            entry.value_ptr.terms.deinit(alloc);
            alloc.free(entry.key_ptr.*);
        }
        fields.deinit(alloc);
    }

    for (groups) |items| {
        for (items) |item| {
            const gop = try fields.getOrPut(alloc, item.field);
            if (!gop.found_existing) {
                gop.key_ptr.* = try alloc.dupe(u8, item.field);
                gop.value_ptr.* = .{};
            }
            gop.value_ptr.doc_count +|= item.global_doc_count;
            gop.value_ptr.total_field_len +|= item.global_total_field_len;
            for (item.term_doc_freqs) |term| {
                const term_gop = try gop.value_ptr.terms.getOrPut(alloc, term.term);
                if (!term_gop.found_existing) {
                    term_gop.key_ptr.* = try alloc.dupe(u8, term.term);
                    term_gop.value_ptr.* = 0;
                }
                term_gop.value_ptr.* +|= term.doc_freq;
            }
        }
    }

    const out = try alloc.alloc(distributed_stats_mod.TextFieldStats, fields.count());
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*item| item.deinit(alloc);
        if (out.len > 0) alloc.free(out);
    }
    var it = fields.iterator();
    while (it.next()) |entry| {
        const term_doc_freqs = try alloc.alloc(distributed_stats_mod.TermDocFreq, entry.value_ptr.terms.count());
        var initialized_terms: usize = 0;
        errdefer {
            for (term_doc_freqs[0..initialized_terms]) |*item| item.deinit(alloc);
            if (term_doc_freqs.len > 0) alloc.free(term_doc_freqs);
        }
        var term_it = entry.value_ptr.terms.iterator();
        while (term_it.next()) |term_entry| {
            term_doc_freqs[initialized_terms] = .{
                .term = try alloc.dupe(u8, term_entry.key_ptr.*),
                .doc_freq = term_entry.value_ptr.*,
            };
            initialized_terms += 1;
        }
        out[initialized] = .{
            .field = try alloc.dupe(u8, entry.key_ptr.*),
            .global_doc_count = entry.value_ptr.doc_count,
            .global_total_field_len = entry.value_ptr.total_field_len,
            .term_doc_freqs = term_doc_freqs,
        };
        std.mem.sort(distributed_stats_mod.TermDocFreq, term_doc_freqs, {}, termDocFreqLessThan);
        initialized += 1;
    }
    std.mem.sort(distributed_stats_mod.TextFieldStats, out, {}, textFieldStatsLessThan);
    return out;
}

pub fn mergeDistributedBackgroundTextStats(
    alloc: std.mem.Allocator,
    groups: []const []const db_mod.aggregations.DistributedBackgroundTextStats,
) ![]const db_mod.aggregations.DistributedBackgroundTextStats {
    var fields = std.StringHashMapUnmanaged(struct {
        aggregation_name: []const u8,
        field: []const u8,
        background_doc_count: u32 = 0,
        terms: std.StringHashMapUnmanaged(u32) = .{},
    }){};
    defer {
        var it = fields.iterator();
        while (it.next()) |entry| {
            var term_it = entry.value_ptr.terms.keyIterator();
            while (term_it.next()) |term| alloc.free(term.*);
            entry.value_ptr.terms.deinit(alloc);
            alloc.free(entry.value_ptr.aggregation_name);
            alloc.free(entry.value_ptr.field);
            alloc.free(entry.key_ptr.*);
        }
        fields.deinit(alloc);
    }

    for (groups) |items| {
        for (items) |item| {
            const map_key = try textStatsTupleKeyAlloc(alloc, &.{ item.aggregation_name, item.field });
            defer alloc.free(map_key);
            const gop = try fields.getOrPut(alloc, map_key);
            if (!gop.found_existing) {
                gop.key_ptr.* = try alloc.dupe(u8, map_key);
                gop.value_ptr.* = .{
                    .aggregation_name = try alloc.dupe(u8, item.aggregation_name),
                    .field = try alloc.dupe(u8, item.field),
                };
            }
            gop.value_ptr.background_doc_count +|= item.background_doc_count;
            for (item.term_doc_freqs) |term| {
                const term_gop = try gop.value_ptr.terms.getOrPut(alloc, term.term);
                if (!term_gop.found_existing) {
                    term_gop.key_ptr.* = try alloc.dupe(u8, term.term);
                    term_gop.value_ptr.* = 0;
                }
                term_gop.value_ptr.* +|= term.doc_freq;
            }
        }
    }

    const out = try alloc.alloc(db_mod.aggregations.DistributedBackgroundTextStats, fields.count());
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*item| item.deinit(alloc);
        if (out.len > 0) alloc.free(out);
    }
    var it = fields.iterator();
    while (it.next()) |entry| {
        const term_doc_freqs = try alloc.alloc(distributed_stats_mod.TermDocFreq, entry.value_ptr.terms.count());
        var initialized_terms: usize = 0;
        errdefer {
            for (term_doc_freqs[0..initialized_terms]) |*item| item.deinit(alloc);
            if (term_doc_freqs.len > 0) alloc.free(term_doc_freqs);
        }
        var term_it = entry.value_ptr.terms.iterator();
        while (term_it.next()) |term_entry| {
            term_doc_freqs[initialized_terms] = .{
                .term = try alloc.dupe(u8, term_entry.key_ptr.*),
                .doc_freq = term_entry.value_ptr.*,
            };
            initialized_terms += 1;
        }
        out[initialized] = .{
            .aggregation_name = try alloc.dupe(u8, entry.value_ptr.aggregation_name),
            .field = try alloc.dupe(u8, entry.value_ptr.field),
            .background_doc_count = entry.value_ptr.background_doc_count,
            .term_doc_freqs = term_doc_freqs,
        };
        std.mem.sort(distributed_stats_mod.TermDocFreq, term_doc_freqs, {}, termDocFreqLessThan);
        initialized += 1;
    }
    std.mem.sort(db_mod.aggregations.DistributedBackgroundTextStats, out, {}, backgroundTextStatsLessThan);
    return out;
}

fn termDocFreqLessThan(_: void, lhs: distributed_stats_mod.TermDocFreq, rhs: distributed_stats_mod.TermDocFreq) bool {
    return std.mem.lessThan(u8, lhs.term, rhs.term);
}

fn textFieldStatsLessThan(_: void, lhs: distributed_stats_mod.TextFieldStats, rhs: distributed_stats_mod.TextFieldStats) bool {
    return std.mem.lessThan(u8, lhs.field, rhs.field);
}

fn backgroundTextStatsLessThan(_: void, lhs: db_mod.aggregations.DistributedBackgroundTextStats, rhs: db_mod.aggregations.DistributedBackgroundTextStats) bool {
    const aggregation_order = std.mem.order(u8, lhs.aggregation_name, rhs.aggregation_name);
    return switch (aggregation_order) {
        .lt => true,
        .gt => false,
        .eq => std.mem.lessThan(u8, lhs.field, rhs.field),
    };
}

fn textStatsTupleKeyAlloc(alloc: std.mem.Allocator, components: []const []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    for (components) |component| {
        if (component.len > std.math.maxInt(u32)) return error.KeyComponentTooLarge;
        var len_buf: [@sizeOf(u32)]u8 = undefined;
        std.mem.writeInt(u32, &len_buf, @intCast(component.len), .big);
        try out.appendSlice(alloc, &len_buf);
        try out.appendSlice(alloc, component);
    }

    return try out.toOwnedSlice(alloc);
}

fn backgroundTermDocFreq(items: []const distributed_stats_mod.TermDocFreq, term: []const u8) ?u32 {
    for (items) |item| {
        if (std.mem.eql(u8, item.term, term)) return item.doc_freq;
    }
    return null;
}

test "fanout planner uses io cap and request shape" {
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{
        .async_limit = .limited(8),
    });
    defer io_impl.deinit();

    const no_io_plan = planFanout(.text_stats, null, 4);
    try std.testing.expect(!no_io_plan.parallel);
    try std.testing.expectEqual(@as(usize, 1), no_io_plan.width);
    try std.testing.expectEqual(FanoutPlanReason.no_io, no_io_plan.reason);

    const text_stats_plan = planFanout(.text_stats, &io_impl, 6);
    try std.testing.expect(text_stats_plan.parallel);
    try std.testing.expectEqual(@as(usize, 4), text_stats_plan.width);
    try std.testing.expectEqual(FanoutPlanReason.parallel, text_stats_plan.reason);

    const small_query_plan = planQueryFanout(&io_impl, 2, .{
        .query = .{ .match = .{ .field = "body", .text = "hello" } },
        .limit = 10,
    });
    try std.testing.expect(!small_query_plan.parallel);
    try std.testing.expectEqual(@as(usize, 1), small_query_plan.width);
    try std.testing.expectEqual(FanoutPlanReason.small_request, small_query_plan.reason);

    const larger_query_plan = planQueryFanout(&io_impl, 6, .{
        .query = .{ .match = .{ .field = "body", .text = "hello" } },
        .limit = 100,
    });
    try std.testing.expect(larger_query_plan.parallel);
    try std.testing.expectEqual(@as(usize, 6), larger_query_plan.width);
    try std.testing.expectEqual(FanoutPlanReason.parallel, larger_query_plan.reason);
}

test "merge distributed text stats sums shard corpus stats by field and term" {
    const alloc = std.testing.allocator;

    const merged = try mergeDistributedTextStats(alloc, &.{
        &.{.{
            .field = "body",
            .global_doc_count = 2,
            .global_total_field_len = 9,
            .term_doc_freqs = &.{
                .{ .term = "alpha", .doc_freq = 2 },
                .{ .term = "beta", .doc_freq = 1 },
            },
        }},
        &.{
            .{
                .field = "body",
                .global_doc_count = 3,
                .global_total_field_len = 15,
                .term_doc_freqs = &.{
                    .{ .term = "alpha", .doc_freq = 1 },
                    .{ .term = "gamma", .doc_freq = 2 },
                },
            },
            .{
                .field = "title",
                .global_doc_count = 3,
                .global_total_field_len = 12,
                .term_doc_freqs = &.{
                    .{ .term = "hello", .doc_freq = 3 },
                },
            },
        },
    });
    defer distributed_stats_mod.deinitTextFieldStats(alloc, merged);

    try std.testing.expectEqual(@as(usize, 2), merged.len);
    try std.testing.expectEqualStrings("body", merged[0].field);
    try std.testing.expectEqualStrings("title", merged[1].field);

    const body = for (merged) |item| {
        if (std.mem.eql(u8, item.field, "body")) break item;
    } else unreachable;
    try std.testing.expectEqual(@as(u32, 5), body.global_doc_count);
    try std.testing.expectEqual(@as(u64, 24), body.global_total_field_len);
    try std.testing.expectEqualStrings("alpha", body.term_doc_freqs[0].term);
    try std.testing.expectEqualStrings("beta", body.term_doc_freqs[1].term);
    try std.testing.expectEqualStrings("gamma", body.term_doc_freqs[2].term);
    try std.testing.expectEqual(@as(?u32, 3), body.termDocFreq("alpha"));
    try std.testing.expectEqual(@as(?u32, 1), body.termDocFreq("beta"));
    try std.testing.expectEqual(@as(?u32, 2), body.termDocFreq("gamma"));

    const title = for (merged) |item| {
        if (std.mem.eql(u8, item.field, "title")) break item;
    } else unreachable;
    try std.testing.expectEqual(@as(u32, 3), title.global_doc_count);
    try std.testing.expectEqual(@as(?u32, 3), title.termDocFreq("hello"));
}

test "merge distributed background text stats keys preserve embedded separators" {
    const alloc = std.testing.allocator;

    const merged = try mergeDistributedBackgroundTextStats(alloc, &.{
        &.{.{
            .aggregation_name = "agg\x1ffield",
            .field = "name",
            .background_doc_count = 2,
            .term_doc_freqs = &.{.{ .term = "alpha", .doc_freq = 2 }},
        }},
        &.{.{
            .aggregation_name = "agg",
            .field = "field\x1fname",
            .background_doc_count = 3,
            .term_doc_freqs = &.{.{ .term = "beta", .doc_freq = 3 }},
        }},
    });
    defer db_mod.aggregations.deinitDistributedBackgroundTextStats(alloc, merged);

    try std.testing.expectEqual(@as(usize, 2), merged.len);
    try std.testing.expectEqualStrings("agg", merged[0].aggregation_name);
    try std.testing.expectEqualStrings("field\x1fname", merged[0].field);
    try std.testing.expectEqualStrings("agg\x1ffield", merged[1].aggregation_name);
    try std.testing.expectEqualStrings("name", merged[1].field);

    const left = for (merged) |item| {
        if (std.mem.eql(u8, item.aggregation_name, "agg\x1ffield")) break item;
    } else unreachable;
    try std.testing.expectEqualStrings("name", left.field);
    try std.testing.expectEqual(@as(u32, 2), left.background_doc_count);
    try std.testing.expectEqual(@as(?u32, 2), backgroundTermDocFreq(left.term_doc_freqs, "alpha"));

    const right = for (merged) |item| {
        if (std.mem.eql(u8, item.aggregation_name, "agg")) break item;
    } else unreachable;
    try std.testing.expectEqualStrings("field\x1fname", right.field);
    try std.testing.expectEqual(@as(u32, 3), right.background_doc_count);
    try std.testing.expectEqual(@as(?u32, 3), backgroundTermDocFreq(right.term_doc_freqs, "beta"));
}

test "collect significant terms field requests gathers unique field terms from hits" {
    const alloc = std.testing.allocator;

    const hits = try alloc.alloc(db_mod.types.SearchHit, 2);
    defer {
        for (hits) |*hit| hit.deinit(alloc);
        alloc.free(hits);
    }
    hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .stored_data = try alloc.dupe(u8, "{\"body\":\"alpha beta\",\"nested\":{\"body\":\"beta gamma\"}}"),
    };
    hits[1] = .{
        .id = try alloc.dupe(u8, "doc:b"),
        .stored_data = try alloc.dupe(u8, "{\"body\":\"alpha\",\"nested\":{\"body\":\"gamma\"}}"),
    };

    const requests = [_]db_mod.aggregations.SearchAggregationRequest{
        .{
            .name = "sig_body",
            .type = "significant_terms",
            .field = "body",
        },
        .{
            .name = "outer_terms",
            .type = "terms",
            .field = "status",
            .aggregations = &.{
                .{
                    .name = "nested_sig_body",
                    .type = "significant_terms",
                    .field = "nested.body",
                },
            },
        },
    };

    const text_analysis = introducer_mod.TextAnalysisConfig{};
    const field_requests = try collectSignificantTermsFieldRequests(alloc, &requests, hits, &text_analysis);
    defer {
        for (field_requests) |*item| item.deinit(alloc);
        if (field_requests.len > 0) alloc.free(field_requests);
    }

    try std.testing.expectEqual(@as(usize, 2), field_requests.len);

    const body = for (field_requests) |item| {
        if (std.mem.eql(u8, item.field, "body")) break item;
    } else unreachable;
    try std.testing.expectEqual(@as(usize, 2), body.terms.len);
    try std.testing.expect(std.mem.eql(u8, body.terms[0], "alpha") or std.mem.eql(u8, body.terms[1], "alpha"));
    try std.testing.expect(std.mem.eql(u8, body.terms[0], "beta") or std.mem.eql(u8, body.terms[1], "beta"));

    const nested = for (field_requests) |item| {
        if (std.mem.eql(u8, item.field, "nested.body")) break item;
    } else unreachable;
    try std.testing.expectEqual(@as(usize, 2), nested.terms.len);
    try std.testing.expect(std.mem.eql(u8, nested.terms[0], "beta") or std.mem.eql(u8, nested.terms[1], "beta"));
    try std.testing.expect(std.mem.eql(u8, nested.terms[0], "gamma") or std.mem.eql(u8, nested.terms[1], "gamma"));
}

test "distributed significant terms candidates use configured analyzers and bounded memory" {
    const alloc = std.testing.allocator;
    var text_analysis = try introducer_mod.parseTextAnalysisConfig(
        alloc,
        "{\"analysis_config\":{\"field_analyzers\":{\"body\":\"keyword\"}}}",
    );
    defer introducer_mod.freeTextAnalysisConfig(alloc, text_analysis);

    const hits = try alloc.alloc(db_mod.types.SearchHit, 1);
    defer {
        hits[0].deinit(alloc);
        alloc.free(hits);
    }
    hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .stored_data = try alloc.dupe(u8, "{\"body\":\"New York\"}"),
    };
    const requests = [_]db_mod.aggregations.SearchAggregationRequest{.{
        .name = "sig_body",
        .type = "significant_terms",
        .field = "body",
        .size = 1,
    }};
    const field_requests = try collectSignificantTermsFieldRequests(alloc, &requests, hits, &text_analysis);
    defer {
        for (field_requests) |*item| item.deinit(alloc);
        if (field_requests.len != 0) alloc.free(field_requests);
    }
    try std.testing.expectEqual(@as(usize, 1), field_requests.len);
    try std.testing.expectEqual(@as(usize, 1), field_requests[0].terms.len);
    try std.testing.expectEqualStrings("New York", field_requests[0].terms[0]);

    var seen = std.StringHashMapUnmanaged(void).empty;
    defer {
        var it = seen.keyIterator();
        while (it.next()) |term| alloc.free(term.*);
        seen.deinit(alloc);
    }
    try std.testing.expectError(
        error.QueryCandidateBudgetExceeded,
        collectSignificantTermsFromValue(alloc, .{ .string = "alpha beta" }, &search_analysis.default_analyzer, 1, &seen),
    );
}

test "algebraic distributed planner selects derived join tensor program for metric" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[
        \\   {"name":"kind","path":"kind","type":"keyword"},
        \\   {"name":"customer","path":"customer","type":"keyword"},
        \\   {"name":"region","path":"region","type":"keyword"}
        \\ ],
        \\ "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
        \\ "joins":[
        \\   {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\ ],
        \\ "materializations":[]}
    );
    defer index.close();

    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "joined_amount",
        .type = "sum",
        .field = "amount",
        .algebraic_join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    };
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, &.{})) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.join_fact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 2), program_plan.steps.len);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.join, program_plan.steps[1].expr.fragment);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.sum, program_plan.steps[1].expr.law_id.?);
    try std.testing.expectEqualStrings("joined_amount", program_plan.steps[1].expr.semantic_id.?);
}

test "algebraic distributed planner selects identity-stamped derived join tensor program" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[
        \\   {"name":"kind","path":"kind","type":"keyword"},
        \\   {"name":"customer","path":"customer","type":"keyword"},
        \\   {"name":"region","path":"region","type":"keyword"}
        \\ ],
        \\ "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
        \\ "joins":[
        \\   {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\ ],
        \\ "materializations":[]}
    );
    defer index.close();

    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "joined_amount",
        .type = "sum",
        .field = "amount",
        .algebraic_join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    };
    var unstamped_plan = (try algebraicDistributedTensorProgramForAggregationRequestAlloc(alloc, &index, request, &.{}, null)) orelse return error.TestUnexpectedResult;
    defer unstamped_plan.deinit(alloc);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.join, unstamped_plan.steps[1].expr.fragment);
    var stamped_plan = (try algebraicDistributedTensorProgramForAggregationRequestAlloc(alloc, &index, request, &.{}, 42)) orelse return error.TestUnexpectedResult;
    defer stamped_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), stamped_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.join_fact_rows, stamped_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 2), stamped_plan.steps.len);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.join, stamped_plan.steps[1].expr.fragment);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.sum, stamped_plan.steps[1].expr.law_id.?);
}

test "algebraic distributed planner selects derived join tensor program for terms child metric" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[
        \\   {"name":"kind","path":"kind","type":"keyword"},
        \\   {"name":"customer","path":"customer","type":"keyword"},
        \\   {"name":"region","path":"region","type":"keyword"}
        \\ ],
        \\ "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
        \\ "joins":[
        \\   {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\ ],
        \\ "materializations":[]}
    );
    defer index.close();

    const children = [_]db_mod.aggregations.SearchAggregationRequest{.{
        .name = "sum_amount",
        .type = "sum",
        .field = "amount",
    }};
    const constraints = [_]db_mod.aggregations.FixedConstraint{.{ .field = "customer", .value = "c1" }};
    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "regions",
        .type = "terms",
        .field = "region",
        .aggregations = children[0..],
        .algebraic_join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    };
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, constraints[0..])) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.join_fact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 3), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 2), program_plan.outputs.len);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.join, program_plan.steps[1].expr.fragment);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.count, program_plan.steps[1].expr.law_id.?);
    try std.testing.expectEqualStrings("regions", program_plan.steps[1].expr.semantic_id.?);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.join, program_plan.steps[2].expr.fragment);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.sum, program_plan.steps[2].expr.law_id.?);
    try std.testing.expectEqualStrings("sum_amount", program_plan.steps[2].expr.semantic_id.?);
}

test "algebraic distributed planner selects derived join tensor program for histogram" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[
        \\   {"name":"kind","path":"kind","type":"keyword"},
        \\   {"name":"customer","path":"customer","type":"keyword"}
        \\ ],
        \\ "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
        \\ "joins":[
        \\   {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\ ],
        \\ "materializations":[]}
    );
    defer index.close();

    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "amount_histogram",
        .type = "histogram",
        .field = "amount",
        .interval = 20,
        .algebraic_join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    };
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, &.{})) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.join_fact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 2), program_plan.steps.len);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.join, program_plan.steps[1].expr.fragment);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.count, program_plan.steps[1].expr.law_id.?);
    try std.testing.expectEqualStrings("amount_histogram", program_plan.steps[1].expr.semantic_id.?);
}

test "algebraic distributed planner selects derived join tensor program for range" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[
        \\   {"name":"kind","path":"kind","type":"keyword"},
        \\   {"name":"customer","path":"customer","type":"keyword"}
        \\ ],
        \\ "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
        \\ "joins":[
        \\   {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\ ],
        \\ "materializations":[]}
    );
    defer index.close();

    const children = [_]db_mod.aggregations.SearchAggregationRequest{.{
        .name = "amount_sum",
        .type = "sum",
        .field = "amount",
    }};
    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "amount_ranges",
        .type = "range",
        .field = "amount",
        .ranges = &.{
            .{ .name = "low", .start = 0, .end = 20 },
            .{ .name = "high", .start = 20, .end = 40 },
        },
        .aggregations = children[0..],
        .algebraic_join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    };
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, &.{})) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.join_fact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 5), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 4), program_plan.outputs.len);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.join, program_plan.steps[1].expr.fragment);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.join, program_plan.steps[2].expr.fragment);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.count, program_plan.steps[1].expr.law_id.?);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.sum, program_plan.steps[2].expr.law_id.?);

    var low_count = try db_mod.algebraic.index.decodeDerivedJoinFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer low_count.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.count, low_count.request.op);
    try std.testing.expectEqual(db_mod.algebraic.index.DerivedJoinRangeKind.numeric, low_count.request.range_kind.?);
    try std.testing.expectEqual(db_mod.algebraic.fact.Role.measure, low_count.request.range_role.?);
    try std.testing.expectEqualStrings("amount", low_count.request.range_field.?);
    try std.testing.expectEqualStrings("0", low_count.request.range_start.?);
    try std.testing.expectEqualStrings("20", low_count.request.range_end.?);

    var low_sum = try db_mod.algebraic.index.decodeDerivedJoinFoldMetadataAlloc(alloc, program_plan.steps[2].expr.metadata.?);
    defer low_sum.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.sum, low_sum.request.op);
    try std.testing.expectEqualStrings("amount", low_sum.request.measure.?);
}

test "algebraic distributed planner selects derived join tensor program for date range" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[
        \\   {"name":"kind","path":"kind","type":"keyword"},
        \\   {"name":"customer","path":"customer","type":"keyword"}
        \\ ],
        \\ "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
        \\ "time_fields":[{"name":"created_at","path":"created_at","type":"datetime"}],
        \\ "joins":[
        \\   {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\ ],
        \\ "materializations":[]}
    );
    defer index.close();

    const children = [_]db_mod.aggregations.SearchAggregationRequest{.{
        .name = "amount_sum",
        .type = "sum",
        .field = "amount",
    }};
    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "created_ranges",
        .type = "date_range",
        .field = "created_at",
        .date_ranges = &.{
            .{ .name = "may_1", .start = "2026-05-01T00:00:00Z", .end = "2026-05-02T00:00:00Z" },
            .{ .name = "may_2", .start = "2026-05-02T00:00:00Z", .end = "2026-05-03T00:00:00Z" },
        },
        .aggregations = children[0..],
        .algebraic_join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    };
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, &.{})) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.join_fact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 5), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 4), program_plan.outputs.len);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.count, program_plan.steps[1].expr.law_id.?);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.sum, program_plan.steps[2].expr.law_id.?);

    var first_count = try db_mod.algebraic.index.decodeDerivedJoinFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer first_count.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.count, first_count.request.op);
    try std.testing.expectEqual(db_mod.algebraic.index.DerivedJoinRangeKind.date, first_count.request.range_kind.?);
    try std.testing.expectEqual(db_mod.algebraic.fact.Role.time, first_count.request.range_role.?);
    try std.testing.expectEqualStrings("created_at", first_count.request.range_field.?);
    try std.testing.expectEqualStrings("2026-05-01T00:00:00Z", first_count.request.range_start.?);
    try std.testing.expectEqualStrings("2026-05-02T00:00:00Z", first_count.request.range_end.?);
}

test "algebraic distributed planner selects docfact tensor program for measure histogram" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[{"name":"tenant","path":"tenant","type":"keyword"}],
        \\ "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
        \\ "materializations":[]}
    );
    defer index.close();

    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "amount_histogram",
        .type = "histogram",
        .field = "amount",
        .interval = 10,
    };
    const constraints = [_]db_mod.aggregations.FixedConstraint{.{ .field = "tenant", .value = "t1" }};
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, constraints[0..])) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.docfact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 2), program_plan.steps.len);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.reduce, program_plan.steps[1].expr.fragment);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.count, program_plan.steps[1].expr.law_id.?);
    try std.testing.expectEqualStrings("amount_histogram", program_plan.steps[1].expr.semantic_id.?);
    try std.testing.expect(program_plan.steps[1].expr.metadata != null);

    var fold = try db_mod.algebraic.index.decodeDocFactBucketFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.histogram, fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.count, fold.request.op);
    try std.testing.expectEqualStrings("amount", fold.request.bucket_field);
    try std.testing.expectEqual(@as(f64, 10), fold.request.histogram_interval);
    try std.testing.expectEqual(@as(usize, 1), fold.request.constraints.len);
    try std.testing.expectEqualStrings("tenant", fold.request.constraints[0].field);
    try std.testing.expectEqualStrings("t1", fold.request.constraints[0].value);
}

test "algebraic distributed planner selects docfact tensor program for measure histogram child metric" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[{"name":"tenant","path":"tenant","type":"keyword"}],
        \\ "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
        \\ "materializations":[]}
    );
    defer index.close();

    const children = [_]db_mod.aggregations.SearchAggregationRequest{.{
        .name = "amount_sum",
        .type = "sum",
        .field = "amount",
    }};
    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "amount_histogram",
        .type = "histogram",
        .field = "amount",
        .interval = 10,
        .aggregations = children[0..],
    };
    const constraints = [_]db_mod.aggregations.FixedConstraint{.{ .field = "tenant", .value = "t1" }};
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, constraints[0..])) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.docfact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 3), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 2), program_plan.outputs.len);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.count, program_plan.steps[1].expr.law_id.?);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.sum, program_plan.steps[2].expr.law_id.?);
    try std.testing.expectEqualStrings("amount_histogram", program_plan.steps[1].expr.semantic_id.?);
    try std.testing.expectEqualStrings("amount_sum", program_plan.steps[2].expr.semantic_id.?);

    var count_fold = try db_mod.algebraic.index.decodeDocFactBucketFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer count_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.histogram, count_fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.count, count_fold.request.op);
    try std.testing.expect(count_fold.request.measure == null);
    var sum_fold = try db_mod.algebraic.index.decodeDocFactBucketFoldMetadataAlloc(alloc, program_plan.steps[2].expr.metadata.?);
    defer sum_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.histogram, sum_fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.sum, sum_fold.request.op);
    try std.testing.expectEqualStrings("amount", sum_fold.request.measure.?);
}

test "algebraic distributed planner selects pathfact tensor program for schemaless histogram" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,"materializations":[]}
    );
    defer index.close();

    const children = [_]db_mod.aggregations.SearchAggregationRequest{.{
        .name = "amount_sum",
        .type = "sum",
        .field = "/amount",
    }};
    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "amount_histogram",
        .type = "histogram",
        .field = "/amount",
        .interval = 10,
        .aggregations = children[0..],
    };
    const constraints = [_]db_mod.aggregations.FixedConstraint{.{ .field = "/tenant", .value = "t1" }};
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, constraints[0..])) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.pathfact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 7), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 6), program_plan.outputs.len);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.count, program_plan.steps[1].expr.law_id.?);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.sum, program_plan.steps[2].expr.law_id.?);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.sum, program_plan.steps[3].expr.law_id.?);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.count, program_plan.steps[4].expr.law_id.?);

    var count_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer count_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.histogram, count_fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.count, count_fold.request.op);
    try std.testing.expectEqualStrings("/amount", count_fold.request.bucket_path);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, count_fold.request.bucket_kind);
    try std.testing.expectEqual(@as(usize, 1), count_fold.request.constraints.len);
    try std.testing.expectEqualStrings("/tenant", count_fold.request.constraints[0].field);
    try std.testing.expectEqualStrings("t1", count_fold.request.constraints[0].value);
    var sum_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[2].expr.metadata.?);
    defer sum_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.sum, sum_fold.request.op);
    try std.testing.expectEqualStrings("/amount", sum_fold.request.measure_path.?);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, sum_fold.request.measure_kind);
    var number_string_sum_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[3].expr.metadata.?);
    defer number_string_sum_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, number_string_sum_fold.request.bucket_kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, number_string_sum_fold.request.measure_kind);
    var string_count_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[4].expr.metadata.?);
    defer string_count_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, string_count_fold.request.bucket_kind);
    var string_sum_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[6].expr.metadata.?);
    defer string_sum_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, string_sum_fold.request.bucket_kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, string_sum_fold.request.measure_kind);
}

test "algebraic distributed planner honors pathfact numeric-string policy" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,"pathfact_policy":{"allow_numeric_string_coercion":false},"materializations":[]}
    );
    defer index.close();

    const children = [_]db_mod.aggregations.SearchAggregationRequest{.{
        .name = "amount_sum",
        .type = "sum",
        .field = "/amount",
    }};
    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "amount_histogram",
        .type = "histogram",
        .field = "/amount",
        .interval = 10,
        .aggregations = children[0..],
    };
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, &.{})) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 2), program_plan.outputs.len);

    var count_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer count_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.histogram, count_fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, count_fold.request.bucket_kind);

    var sum_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[2].expr.metadata.?);
    defer sum_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.sum, sum_fold.request.op);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, sum_fold.request.bucket_kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, sum_fold.request.measure_kind);
}

test "algebraic distributed planner selects pathfact tensor program for schemaless terms" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,"materializations":[]}
    );
    defer index.close();

    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "tier_terms",
        .type = "terms",
        .field = "/tier",
    };
    const constraints = [_]db_mod.aggregations.FixedConstraint{.{ .field = "/tenant", .value = "t1" }};
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, constraints[0..])) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.pathfact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 7), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 6), program_plan.outputs.len);

    var string_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer string_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.terms, string_fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.count, string_fold.request.op);
    try std.testing.expectEqualStrings("/tier", string_fold.request.bucket_path);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, string_fold.request.bucket_kind);
    try std.testing.expectEqual(@as(usize, 1), string_fold.request.constraints.len);
    try std.testing.expectEqualStrings("/tenant", string_fold.request.constraints[0].field);
    try std.testing.expectEqualStrings("t1", string_fold.request.constraints[0].value);

    var number_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[2].expr.metadata.?);
    defer number_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.terms, number_fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, number_fold.request.bucket_kind);

    var bool_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[3].expr.metadata.?);
    defer bool_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.terms, bool_fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.bool, bool_fold.request.bucket_kind);

    var null_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[4].expr.metadata.?);
    defer null_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.terms, null_fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.null, null_fold.request.bucket_kind);

    var object_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[5].expr.metadata.?);
    defer object_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.terms, object_fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.object, object_fold.request.bucket_kind);

    var array_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[6].expr.metadata.?);
    defer array_fold.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.terms, array_fold.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.array, array_fold.request.bucket_kind);
}

test "algebraic distributed planner carries same-path disjunction as pathfact any constraint" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,"materializations":[]}
    );
    defer index.close();

    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "tier_terms",
        .type = "terms",
        .field = "/tier",
    };
    const constraints = (try algebraicConstraintsForRequestAlloc(alloc, .{
        .filter_query_json = "{\"disjuncts\":[{\"term\":{\"/tenant\":\"t1\"}},{\"term\":{\"/tenant\":\"t2\"}}]}",
    })).?;
    defer freeAlgebraicConstraints(alloc, constraints);
    try std.testing.expectEqual(@as(usize, 1), constraints.len);

    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, constraints)) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);

    var string_fold = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer string_fold.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), string_fold.request.constraints.len);
    try std.testing.expectEqualStrings("/tenant", string_fold.request.constraints[0].field);
    const parts = try db_mod.algebraic.token.decodeTupleAlloc(alloc, string_fold.request.constraints[0].value);
    defer {
        for (parts) |part| alloc.free(part);
        alloc.free(parts);
    }
    try std.testing.expectEqual(@as(usize, 3), parts.len);
    try std.testing.expectEqualStrings("pathfact-any:v1", parts[0]);
}

test "algebraic distributed planner selects pathfact tensor program for schemaless range" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,"materializations":[]}
    );
    defer index.close();

    const children = [_]db_mod.aggregations.SearchAggregationRequest{.{
        .name = "amount_sum",
        .type = "sum",
        .field = "/amount",
    }};
    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "amount_ranges",
        .type = "range",
        .field = "/amount",
        .ranges = &.{
            .{ .name = "low", .start = 0, .end = 20 },
            .{ .name = "high", .start = 20, .end = 30 },
        },
        .aggregations = children[0..],
    };
    const constraints = [_]db_mod.aggregations.FixedConstraint{.{ .field = "/tenant", .value = "t1" }};
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, constraints[0..])) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.pathfact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 13), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 12), program_plan.outputs.len);

    var low_number_count = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer low_number_count.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.range, low_number_count.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.count, low_number_count.request.op);
    try std.testing.expectEqualStrings("/amount", low_number_count.request.bucket_path);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, low_number_count.request.bucket_kind);
    try std.testing.expectEqualStrings("0", low_number_count.request.range_start.?);
    try std.testing.expectEqualStrings("20", low_number_count.request.range_end.?);
    try std.testing.expectEqual(@as(usize, 1), low_number_count.request.constraints.len);
    try std.testing.expectEqualStrings("/tenant", low_number_count.request.constraints[0].field);
    try std.testing.expectEqualStrings("t1", low_number_count.request.constraints[0].value);

    var low_number_sum = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[2].expr.metadata.?);
    defer low_number_sum.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.sum, low_number_sum.request.op);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, low_number_sum.request.bucket_kind);
    try std.testing.expectEqualStrings("/amount", low_number_sum.request.measure_path.?);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, low_number_sum.request.measure_kind);

    var low_number_string_sum = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[3].expr.metadata.?);
    defer low_number_string_sum.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, low_number_string_sum.request.bucket_kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, low_number_string_sum.request.measure_kind);

    var low_string_count = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[4].expr.metadata.?);
    defer low_string_count.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, low_string_count.request.bucket_kind);
    try std.testing.expectEqualStrings("0", low_string_count.request.range_start.?);
    try std.testing.expectEqualStrings("20", low_string_count.request.range_end.?);

    var low_string_sum = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[6].expr.metadata.?);
    defer low_string_sum.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, low_string_sum.request.bucket_kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, low_string_sum.request.measure_kind);

    var high_number_count = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[7].expr.metadata.?);
    defer high_number_count.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, high_number_count.request.bucket_kind);
    try std.testing.expectEqualStrings("20", high_number_count.request.range_start.?);
    try std.testing.expectEqualStrings("30", high_number_count.request.range_end.?);

    var high_string_sum = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[12].expr.metadata.?);
    defer high_string_sum.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.sum, high_string_sum.request.op);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, high_string_sum.request.bucket_kind);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, high_string_sum.request.measure_kind);
    try std.testing.expectEqualStrings("20", high_string_sum.request.range_start.?);
    try std.testing.expectEqualStrings("30", high_string_sum.request.range_end.?);
}

test "algebraic distributed planner selects multi-output docfact tensor program for measure range" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[{"name":"tenant","path":"tenant","type":"keyword"}],
        \\ "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
        \\ "materializations":[]}
    );
    defer index.close();

    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "amount_ranges",
        .type = "range",
        .field = "amount",
        .ranges = &.{
            .{ .name = "low", .start = 0, .end = 20 },
            .{ .name = "high", .start = 20, .end = 30 },
        },
    };
    const constraints = [_]db_mod.aggregations.FixedConstraint{.{ .field = "tenant", .value = "t1" }};
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, constraints[0..])) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.docfact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 3), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 2), program_plan.outputs.len);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.reduce, program_plan.steps[1].expr.fragment);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.reduce, program_plan.steps[2].expr.fragment);

    var low = try db_mod.algebraic.index.decodeDocFactBucketFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer low.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.range, low.request.kind);
    try std.testing.expectEqualStrings("0", low.request.range_start.?);
    try std.testing.expectEqualStrings("20", low.request.range_end.?);
    var high = try db_mod.algebraic.index.decodeDocFactBucketFoldMetadataAlloc(alloc, program_plan.steps[2].expr.metadata.?);
    defer high.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.range, high.request.kind);
    try std.testing.expectEqualStrings("20", high.request.range_start.?);
    try std.testing.expectEqualStrings("30", high.request.range_end.?);
}

test "algebraic distributed planner selects multi-output docfact tensor program for date range" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[{"name":"tenant","path":"tenant","type":"keyword"}],
        \\ "time_fields":[{"name":"created_at","path":"created_at","type":"datetime"}],
        \\ "materializations":[]}
    );
    defer index.close();

    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "created_ranges",
        .type = "date_range",
        .field = "created_at",
        .date_ranges = &.{
            .{ .name = "first", .start = "2026-05-01T12:00:00Z", .end = "2026-05-02T00:00:00Z" },
            .{ .name = "second", .start = "2026-05-02T00:00:00Z", .end = "2026-05-03T00:00:00Z" },
        },
    };
    const constraints = [_]db_mod.aggregations.FixedConstraint{.{ .field = "tenant", .value = "t1" }};
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, constraints[0..])) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.docfact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 3), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 2), program_plan.outputs.len);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.reduce, program_plan.steps[1].expr.fragment);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.reduce, program_plan.steps[2].expr.fragment);

    var first = try db_mod.algebraic.index.decodeDocFactBucketFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer first.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.date_range, first.request.kind);
    try std.testing.expectEqualStrings("2026-05-01T12:00:00Z", first.request.range_start.?);
    try std.testing.expectEqualStrings("2026-05-02T00:00:00Z", first.request.range_end.?);
    try std.testing.expectEqual(@as(usize, 1), first.request.constraints.len);
    try std.testing.expectEqualStrings("tenant", first.request.constraints[0].field);
    try std.testing.expectEqualStrings("t1", first.request.constraints[0].value);
    var second = try db_mod.algebraic.index.decodeDocFactBucketFoldMetadataAlloc(alloc, program_plan.steps[2].expr.metadata.?);
    defer second.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.date_range, second.request.kind);
    try std.testing.expectEqualStrings("2026-05-02T00:00:00Z", second.request.range_start.?);
    try std.testing.expectEqualStrings("2026-05-03T00:00:00Z", second.request.range_end.?);
}

test "algebraic distributed planner selects pathfact tensor program for schemaless date range" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,"materializations":[]}
    );
    defer index.close();

    const children = [_]db_mod.aggregations.SearchAggregationRequest{.{
        .name = "amount_sum",
        .type = "sum",
        .field = "/amount",
    }};
    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "created_ranges",
        .type = "date_range",
        .field = "/created_at",
        .date_ranges = &.{
            .{ .name = "first", .start = "2026-05-01T00:00:00Z", .end = "2026-05-02T00:00:00Z" },
            .{ .name = "second", .start = "2026-05-02T00:00:00Z", .end = "2026-05-03T00:00:00Z" },
        },
        .aggregations = children[0..],
    };
    const constraints = [_]db_mod.aggregations.FixedConstraint{.{ .field = "/tenant", .value = "t1" }};
    var program_plan = (try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, constraints[0..])) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), program_plan.access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.pathfact_rows, program_plan.access_paths[0].layout);
    try std.testing.expectEqual(@as(usize, 7), program_plan.steps.len);
    try std.testing.expectEqual(@as(usize, 6), program_plan.outputs.len);

    var first_count = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[1].expr.metadata.?);
    defer first_count.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.date_range, first_count.request.kind);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.count, first_count.request.op);
    try std.testing.expectEqualStrings("/created_at", first_count.request.bucket_path);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, first_count.request.bucket_kind);
    try std.testing.expectEqualStrings("2026-05-01T00:00:00Z", first_count.request.range_start.?);
    try std.testing.expectEqualStrings("2026-05-02T00:00:00Z", first_count.request.range_end.?);
    try std.testing.expectEqual(@as(usize, 1), first_count.request.constraints.len);
    try std.testing.expectEqualStrings("/tenant", first_count.request.constraints[0].field);
    try std.testing.expectEqualStrings("t1", first_count.request.constraints[0].value);

    var first_sum = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[2].expr.metadata.?);
    defer first_sum.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.sum, first_sum.request.op);
    try std.testing.expectEqualStrings("/amount", first_sum.request.measure_path.?);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.number, first_sum.request.measure_kind);

    var first_string_sum = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[3].expr.metadata.?);
    defer first_string_sum.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.sum, first_string_sum.request.op);
    try std.testing.expectEqualStrings("/amount", first_string_sum.request.measure_path.?);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, first_string_sum.request.measure_kind);

    var second_count = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[4].expr.metadata.?);
    defer second_count.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.index.DocFactBucketFoldKind.date_range, second_count.request.kind);
    try std.testing.expectEqualStrings("2026-05-02T00:00:00Z", second_count.request.range_start.?);
    try std.testing.expectEqualStrings("2026-05-03T00:00:00Z", second_count.request.range_end.?);

    var second_string_sum = try db_mod.algebraic.index.decodePathFactBucketFoldMetadataAlloc(alloc, program_plan.steps[6].expr.metadata.?);
    defer second_string_sum.deinit(alloc);
    try std.testing.expectEqual(db_mod.algebraic.algebra.Op.sum, second_string_sum.request.op);
    try std.testing.expectEqualStrings("/amount", second_string_sum.request.measure_path.?);
    try std.testing.expectEqual(db_mod.algebraic.pathfact.Kind.string, second_string_sum.request.measure_kind);
}

test "algebraic distributed planner honors pathfact datetime-string policy" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,"pathfact_policy":{"allow_datetime_string_coercion":false},"materializations":[]}
    );
    defer index.close();

    const request = db_mod.aggregations.SearchAggregationRequest{
        .name = "created_ranges",
        .type = "date_range",
        .field = "/created_at",
        .date_ranges = &.{
            .{ .name = "first", .start = "2026-05-01T00:00:00Z", .end = "2026-05-02T00:00:00Z" },
        },
    };
    const plan = try algebraicDistributedTensorProgramForRequestAlloc(alloc, &index, request, &.{});
    try std.testing.expect(plan == null);
}
