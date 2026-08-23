// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License at https://www.antfly.io/licensing/ELv2-license.

//! Serverless graph-metric admission and materializer compatibility policy.
//! Keep this independent of persistence so configuration validation, builders,
//! and reuse checks all enforce exactly the same bounded contract.

const std = @import("std");
const graph_mod = @import("../../graph/graph.zig");
const bounded_decode = @import("../bounded_decode.zig");

/// Increment whenever an implementation change can alter admission or output
/// without changing the user-visible metric configuration.
pub const materializer_epoch: u32 = 1;

pub const Limits = struct {
    // Keep graph admission aligned with the decoder and query-runtime
    // artifact contract. Larger topology artifacts cannot be served safely by
    // this runtime and are represented by durable rejected metric sidecars.
    max_graph_payload_bytes: usize = (bounded_decode.Limits{}).max_artifact_bytes,
    max_metric_payload_bytes: usize = 256 * 1024 * 1024,
    max_total_metric_payload_bytes: usize = 512 * 1024 * 1024,
    max_nodes: usize = 1_000_000,
    max_edges: usize = 10_000_000,
    max_work_items: u64 = 500_000_000,
    max_total_work_items: u64 = 1_000_000_000,
    max_graph_indexes: usize = 16,
    max_metrics: usize = 16,
    max_total_metrics: usize = 64,
    max_graph_index_name_bytes: usize = 256,
    max_edge_filter_types: usize = 64,
    max_metric_name_bytes: usize = 128,
    max_edge_type_bytes: usize = 256,
};

pub const Budget = struct {
    limits: Limits,
    work_items: u64 = 0,
    metric_payload_bytes: usize = 0,

    pub fn chargeWork(self: *Budget, amount: u64) !void {
        const next = std.math.add(u64, self.work_items, amount) catch
            return error.GraphMetricBuildBudgetExceeded;
        if (next > self.limits.max_total_work_items) return error.GraphMetricBuildBudgetExceeded;
        self.work_items = next;
    }

    pub fn chargePayload(self: *Budget, amount: usize) !void {
        const next = std.math.add(usize, self.metric_payload_bytes, amount) catch
            return error.GraphMetricBuildBudgetExceeded;
        if (next > self.limits.max_total_metric_payload_bytes) return error.GraphMetricBuildBudgetExceeded;
        self.metric_payload_bytes = next;
    }
};

pub fn validateLimits(limits: Limits) !void {
    if (limits.max_graph_payload_bytes == 0 or
        limits.max_graph_payload_bytes > (bounded_decode.Limits{}).max_artifact_bytes or
        limits.max_metric_payload_bytes == 0 or
        limits.max_total_metric_payload_bytes == 0 or
        limits.max_nodes == 0 or
        limits.max_edges == 0 or
        limits.max_work_items == 0 or
        limits.max_total_work_items == 0 or
        limits.max_graph_indexes == 0 or
        limits.max_metrics == 0 or
        limits.max_total_metrics == 0 or
        limits.max_graph_index_name_bytes == 0 or
        limits.max_edge_filter_types == 0 or
        limits.max_metric_name_bytes == 0 or
        limits.max_edge_type_bytes == 0)
    {
        return error.InvalidGraphMetricBuildOptions;
    }
}

pub fn validateCatalogFanout(graph_index_count: usize, metric_count: usize, limits: Limits) !void {
    try validateLimits(limits);
    if (graph_index_count > limits.max_graph_indexes or metric_count > limits.max_total_metrics) {
        return error.GraphMetricConfigurationLimitExceeded;
    }
}

pub fn validateConfigs(configs: []const graph_mod.GraphMetricConfig, limits: Limits) !void {
    try validateLimits(limits);
    if (configs.len > limits.max_metrics) return error.GraphMetricConfigurationLimitExceeded;
    for (configs) |config| {
        if (config.name.len == 0 or config.name.len > limits.max_metric_name_bytes or
            config.edge_filter.types.len > limits.max_edge_filter_types)
        {
            return error.GraphMetricConfigurationLimitExceeded;
        }
        for (config.edge_filter.types) |edge_type| {
            if (edge_type.len == 0 or edge_type.len > limits.max_edge_type_bytes) {
                return error.GraphMetricConfigurationLimitExceeded;
            }
        }
    }
}

pub fn workItems(node_count: usize, edge_count: usize, iterations: u32, passes: u64) !u64 {
    const per_iteration = std.math.add(u64, @intCast(node_count), @intCast(edge_count)) catch
        return error.GraphMetricBuildBudgetExceeded;
    const iteration_work = std.math.mul(u64, per_iteration, passes) catch
        return error.GraphMetricBuildBudgetExceeded;
    return std.math.mul(u64, iteration_work, iterations) catch
        return error.GraphMetricBuildBudgetExceeded;
}

pub fn metricWorkItems(kind: graph_mod.GraphMetricKind, node_count: usize, edge_count: usize, max_iterations: u32) !u64 {
    return switch (kind) {
        .degree => try workItems(node_count, edge_count, 1, 1),
        .pagerank, .eigenvector, .hits_authority, .hits_hub => try workItems(node_count, edge_count, max_iterations, 2),
    };
}

/// Total work charged for one materialization. Projection scans every source
/// node and outbound edge once before the metric kernel sees the filtered
/// graph, so those passes must be included in admission as well.
pub fn materializationWorkItems(
    kind: graph_mod.GraphMetricKind,
    source_node_count: usize,
    source_edge_count: usize,
    projected_node_count: usize,
    projected_edge_count: usize,
    max_iterations: u32,
) !u64 {
    const projection = try workItems(source_node_count, source_edge_count, 1, 1);
    const kernel = try metricWorkItems(kind, projected_node_count, projected_edge_count, max_iterations);
    return std.math.add(u64, projection, kernel) catch error.GraphMetricBuildBudgetExceeded;
}

pub fn materializerFingerprint(limits: Limits) u64 {
    var hasher = std.hash.Wyhash.init(0);
    hash(&hasher, materializer_epoch);
    inline for (std.meta.fields(Limits)) |field| hash(&hasher, @field(limits, field.name));
    const value = hasher.final() & std.math.maxInt(i64);
    return if (value == 0) 1 else value;
}

fn hash(hasher: *std.hash.Wyhash, value: anytype) void {
    const normalized: u64 = @intCast(value);
    var encoded: [@sizeOf(u64)]u8 = undefined;
    std.mem.writeInt(u64, &encoded, normalized, .little);
    hasher.update(&encoded);
}

test "serverless graph metric policy bounds aggregate work and configuration fanout" {
    var budget = Budget{ .limits = .{ .max_work_items = 10, .max_total_work_items = 12 } };
    try budget.chargeWork(10);
    try std.testing.expectError(error.GraphMetricBuildBudgetExceeded, budget.chargeWork(3));

    const configs = [_]graph_mod.GraphMetricConfig{
        .{ .name = "a" },
        .{ .name = "b" },
    };
    try std.testing.expectError(
        error.GraphMetricConfigurationLimitExceeded,
        validateConfigs(&configs, .{ .max_metrics = 1 }),
    );
    try std.testing.expectError(
        error.GraphMetricConfigurationLimitExceeded,
        validateCatalogFanout(2, 2, .{ .max_graph_indexes = 1 }),
    );
    try std.testing.expectError(
        error.GraphMetricConfigurationLimitExceeded,
        validateCatalogFanout(1, 2, .{ .max_total_metrics = 1 }),
    );
}

test "serverless graph metric policy fingerprint changes with materialization limits" {
    const baseline = materializerFingerprint(.{});
    try std.testing.expect(baseline != materializerFingerprint(.{ .max_nodes = 999_999 }));
}

test "serverless graph metric graph admission cannot exceed decoder capacity" {
    const decoder_limit = (bounded_decode.Limits{}).max_artifact_bytes;
    try std.testing.expectEqual(decoder_limit, (Limits{}).max_graph_payload_bytes);
    try std.testing.expectError(
        error.InvalidGraphMetricBuildOptions,
        validateLimits(.{ .max_graph_payload_bytes = decoder_limit + 1 }),
    );
}
