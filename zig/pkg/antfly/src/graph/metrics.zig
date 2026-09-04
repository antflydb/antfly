// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Storage-independent, bounded graph metric kernels. Persistence and work
//! scheduling deliberately live outside this module so the same algorithms can
//! be used by embedded and immutable lake-native graph implementations.

const std = @import("std");
const Allocator = std.mem.Allocator;
const CancellationToken = @import("../common/cancellation.zig").CancellationToken;
const metric_cost = @import("metric_cost.zig");

pub const Edge = struct {
    // Materializers cap graphs well below u32 addressability. Keeping the
    // immutable compute edge at eight bytes halves the hottest topology array
    // on 64-bit hosts and gives serverless builds a stable cross-platform wire
    // width instead of leaking usize into retained state.
    source: u32,
    target: u32,
};

pub const AdjacencyLane = enum(u2) {
    none,
    degrees,
    neighbors,
};

/// Describes the smallest topology a kernel family needs. Keeping this in the
/// storage-independent layer lets materializers plan one union topology for a
/// compatible metric group without paying for adjacency lanes no consumer
/// reads.
pub const TopologyRequirements = struct {
    incoming: AdjacencyLane = .none,
    outgoing: AdjacencyLane = .none,

    pub const degree = TopologyRequirements{ .incoming = .degrees, .outgoing = .degrees };
    pub const pagerank = TopologyRequirements{ .incoming = .neighbors, .outgoing = .degrees };
    pub const eigenvector = TopologyRequirements{ .incoming = .neighbors };
    pub const hits = TopologyRequirements{ .incoming = .neighbors, .outgoing = .neighbors };
    pub const full = hits;

    pub fn merge(self: TopologyRequirements, other: TopologyRequirements) TopologyRequirements {
        return .{
            .incoming = @enumFromInt(@max(@intFromEnum(self.incoming), @intFromEnum(other.incoming))),
            .outgoing = @enumFromInt(@max(@intFromEnum(self.outgoing), @intFromEnum(other.outgoing))),
        };
    }

    pub fn satisfies(self: TopologyRequirements, required: TopologyRequirements) bool {
        return @intFromEnum(self.incoming) >= @intFromEnum(required.incoming) and
            @intFromEnum(self.outgoing) >= @intFromEnum(required.outgoing);
    }
};

/// Compact target-owned and source-owned adjacency. Iterative kernels write
/// one output ordinal at a time, avoiding random scatter writes and providing
/// a race-free partition boundary for runtime-backed parallel execution.
pub const Topology = struct {
    node_count: usize,
    edge_count: usize,
    requirements: TopologyRequirements,
    incoming_offsets: []u32,
    incoming_sources: []u32,
    outgoing_offsets: []u32,
    outgoing_targets: []u32,

    pub fn initAlloc(alloc: Allocator, node_count: usize, edges: []const Edge, cancellation: CancellationToken) !Topology {
        return initAllocFor(alloc, node_count, edges, .full, cancellation);
    }

    pub fn initAllocFor(
        alloc: Allocator,
        node_count: usize,
        edges: []const Edge,
        requirements: TopologyRequirements,
        cancellation: CancellationToken,
    ) !Topology {
        if (node_count > std.math.maxInt(u32) or edges.len > std.math.maxInt(u32))
            return error.GraphMetricBuildBudgetExceeded;
        const offset_count = std.math.add(usize, node_count, 1) catch
            return error.GraphMetricBuildBudgetExceeded;
        const has_incoming = requirements.incoming != .none;
        const has_outgoing = requirements.outgoing != .none;
        const incoming_offsets = if (has_incoming) try alloc.alloc(u32, offset_count) else @constCast(&[_]u32{});
        errdefer if (has_incoming) alloc.free(incoming_offsets);
        const outgoing_offsets = if (has_outgoing) try alloc.alloc(u32, offset_count) else @constCast(&[_]u32{});
        errdefer if (has_outgoing) alloc.free(outgoing_offsets);
        if (has_incoming) @memset(incoming_offsets, 0);
        if (has_outgoing) @memset(outgoing_offsets, 0);
        for (edges, 0..) |edge, i| {
            if (i % 4096 == 0) try cancellation.check();
            if (@as(usize, edge.source) >= node_count or @as(usize, edge.target) >= node_count)
                return error.InvalidGraphMetricEdge;
            if (has_incoming) incoming_offsets[@as(usize, edge.target) + 1] += 1;
            if (has_outgoing) outgoing_offsets[@as(usize, edge.source) + 1] += 1;
        }
        for (1..offset_count) |i| {
            if (has_incoming) incoming_offsets[i] = std.math.add(u32, incoming_offsets[i], incoming_offsets[i - 1]) catch
                return error.GraphMetricBuildBudgetExceeded;
            if (has_outgoing) outgoing_offsets[i] = std.math.add(u32, outgoing_offsets[i], outgoing_offsets[i - 1]) catch
                return error.GraphMetricBuildBudgetExceeded;
        }
        const owns_incoming_sources = requirements.incoming == .neighbors;
        const owns_outgoing_targets = requirements.outgoing == .neighbors;
        const incoming_sources = if (owns_incoming_sources) try alloc.alloc(u32, edges.len) else @constCast(&[_]u32{});
        errdefer if (owns_incoming_sources) alloc.free(incoming_sources);
        const outgoing_targets = if (owns_outgoing_targets) try alloc.alloc(u32, edges.len) else @constCast(&[_]u32{});
        errdefer if (owns_outgoing_targets) alloc.free(outgoing_targets);
        const incoming_cursors = if (owns_incoming_sources) try alloc.dupe(u32, incoming_offsets[0..node_count]) else @constCast(&[_]u32{});
        defer if (owns_incoming_sources) alloc.free(incoming_cursors);
        const outgoing_cursors = if (owns_outgoing_targets) try alloc.dupe(u32, outgoing_offsets[0..node_count]) else @constCast(&[_]u32{});
        defer if (owns_outgoing_targets) alloc.free(outgoing_cursors);
        if (owns_incoming_sources or owns_outgoing_targets) {
            for (edges, 0..) |edge, i| {
                if (i % 4096 == 0) try cancellation.check();
                if (owns_incoming_sources) {
                    const incoming_position = incoming_cursors[edge.target];
                    incoming_sources[incoming_position] = edge.source;
                    incoming_cursors[edge.target] += 1;
                }
                if (owns_outgoing_targets) {
                    const outgoing_position = outgoing_cursors[edge.source];
                    outgoing_targets[outgoing_position] = edge.target;
                    outgoing_cursors[edge.source] += 1;
                }
            }
        }
        return .{
            .node_count = node_count,
            .edge_count = edges.len,
            .requirements = requirements,
            .incoming_offsets = incoming_offsets,
            .incoming_sources = incoming_sources,
            .outgoing_offsets = outgoing_offsets,
            .outgoing_targets = outgoing_targets,
        };
    }

    pub fn deinit(self: *Topology, alloc: Allocator) void {
        if (self.requirements.incoming != .none) alloc.free(self.incoming_offsets);
        if (self.requirements.incoming == .neighbors) alloc.free(self.incoming_sources);
        if (self.requirements.outgoing != .none) alloc.free(self.outgoing_offsets);
        if (self.requirements.outgoing == .neighbors) alloc.free(self.outgoing_targets);
        self.* = undefined;
    }

    pub fn nodeCount(self: Topology) usize {
        return self.node_count;
    }

    pub fn edgeCount(self: Topology) usize {
        return self.edge_count;
    }
};

pub const Options = struct {
    damping: f64 = 0.85,
    tolerance: f64 = 0.000001,
    max_iterations: u32 = 50,
    max_nodes: usize = 1_000_000,
    max_edges: usize = 10_000_000,
    max_work_items: u64 = 500_000_000,
    cancellation: CancellationToken = .none,
    io: ?std.Io = null,
    max_parallelism: usize = 1,
};

const parallel_edge_threshold: usize = 128 * 1024;
const parallel_vector_threshold: usize = 32 * 1024;
const max_kernel_parallelism: usize = 16;
const reduction_partitions: usize = 16;

fn parallelWidth(topology: Topology, options: Options) usize {
    if (options.io == null or options.max_parallelism < 2 or
        topology.edgeCount() < parallel_edge_threshold or topology.nodeCount() < 2)
    {
        return 1;
    }
    return @min(max_kernel_parallelism, @min(options.max_parallelism, topology.nodeCount()));
}

fn vectorParallelWidth(len: usize, options: Options) usize {
    if (options.io == null or options.max_parallelism < 2 or len < parallel_vector_threshold)
        return 1;
    return @min(max_kernel_parallelism, @min(options.max_parallelism, len));
}

fn vectorBoundary(len: usize, part: usize, parts: usize) usize {
    const whole = len / parts;
    const remainder = len % parts;
    return whole * part + @min(part, remainder);
}

fn logicalReductionParts(len: usize) usize {
    if (len < parallel_vector_threshold) return 1;
    return @min(reduction_partitions, len);
}

/// Partition by vertices plus incident edges, not merely vertex count. This
/// keeps power-law graphs balanced while retaining exclusive ownership of each
/// output ordinal.
fn weightedBoundary(offsets: []const u32, part: usize, width: usize) usize {
    if (part == 0) return 0;
    if (part >= width) return offsets.len - 1;
    const node_count = offsets.len - 1;
    const total_work = @as(u64, @intCast(node_count)) + offsets[node_count];
    const target = (total_work * part + width - 1) / width;
    var lower: usize = 0;
    var upper: usize = node_count;
    while (lower < upper) {
        const middle = lower + (upper - lower) / 2;
        const work = @as(u64, @intCast(middle)) + offsets[middle];
        if (work < target) lower = middle + 1 else upper = middle;
    }
    return lower;
}

pub const Result = struct {
    scores: []f64,
    iterations_completed: u32,
    converged: bool,
    delta: f64,

    pub fn deinit(self: *Result, alloc: Allocator) void {
        alloc.free(self.scores);
        self.* = undefined;
    }
};

pub const HitsResult = struct {
    authorities: []f64,
    hubs: []f64,
    iterations_completed: u32,
    converged: bool,
    delta: f64,

    pub fn deinit(self: *HitsResult, alloc: Allocator) void {
        alloc.free(self.authorities);
        alloc.free(self.hubs);
        self.* = undefined;
    }
};

fn validateInputBoundsAndOptions(node_count: usize, edge_count: usize, options: Options) !void {
    if (node_count > options.max_nodes or edge_count > options.max_edges) return error.GraphMetricBuildBudgetExceeded;
    if (!std.math.isFinite(options.damping) or options.damping < 0 or options.damping >= 1 or
        !std.math.isFinite(options.tolerance) or options.tolerance < 0 or
        options.max_iterations == 0 or options.max_iterations > 1_000 or
        options.max_nodes == 0 or options.max_edges == 0 or options.max_work_items == 0 or
        options.max_parallelism == 0 or options.max_parallelism > max_kernel_parallelism)
    {
        return error.InvalidGraphMetricOptions;
    }
}

fn admitWork(kind: metric_cost.Kind, node_count: usize, edge_count: usize, iterations: u32, max_work_items: u64) !void {
    const work = try metric_cost.kernelWorkItems(kind, node_count, edge_count, iterations);
    if (work > max_work_items) return error.GraphMetricBuildBudgetExceeded;
}

pub fn degreeAlloc(alloc: Allocator, node_count: usize, edges: []const Edge, options: Options) !Result {
    try validateInputBoundsAndOptions(node_count, edges.len, options);
    try admitWork(.degree, node_count, edges.len, 1, options.max_work_items);
    var topology = try Topology.initAllocFor(alloc, node_count, edges, .degree, options.cancellation);
    defer topology.deinit(alloc);
    return try degreeTopologyAlloc(alloc, topology, options);
}

fn validateTopology(topology: Topology, required: TopologyRequirements, options: Options) !void {
    const node_count = topology.nodeCount();
    const edge_count = topology.edgeCount();
    if (node_count > options.max_nodes or edge_count > options.max_edges) return error.GraphMetricBuildBudgetExceeded;
    if (!topology.requirements.satisfies(required)) return error.InvalidGraphMetricEdge;
    if (!std.math.isFinite(options.damping) or options.damping < 0 or options.damping >= 1 or
        !std.math.isFinite(options.tolerance) or options.tolerance < 0 or
        options.max_iterations == 0 or options.max_iterations > 1_000 or
        options.max_nodes == 0 or options.max_edges == 0 or options.max_work_items == 0 or
        options.max_parallelism == 0 or options.max_parallelism > max_kernel_parallelism)
    {
        return error.InvalidGraphMetricOptions;
    }
    const expected_offsets = node_count + 1;
    if (topology.requirements.incoming != .none and
        (topology.incoming_offsets.len != expected_offsets or topology.incoming_offsets[0] != 0 or
            @as(usize, topology.incoming_offsets[node_count]) != edge_count)) return error.InvalidGraphMetricEdge;
    if (topology.requirements.outgoing != .none and
        (topology.outgoing_offsets.len != expected_offsets or topology.outgoing_offsets[0] != 0 or
            @as(usize, topology.outgoing_offsets[node_count]) != edge_count)) return error.InvalidGraphMetricEdge;
    if (topology.requirements.incoming == .neighbors and topology.incoming_sources.len != edge_count)
        return error.InvalidGraphMetricEdge;
    if (topology.requirements.outgoing == .neighbors and topology.outgoing_targets.len != edge_count)
        return error.InvalidGraphMetricEdge;
    for (0..node_count) |i| {
        if ((topology.requirements.incoming != .none and topology.incoming_offsets[i] > topology.incoming_offsets[i + 1]) or
            (topology.requirements.outgoing != .none and topology.outgoing_offsets[i] > topology.outgoing_offsets[i + 1]))
        {
            return error.InvalidGraphMetricEdge;
        }
    }
    // Endpoint ordinals were validated while the immutable topology was
    // constructed. Revalidating O(E) neighbors for every metric sharing the
    // same projection defeats topology reuse and is not a useful trust boundary.
}

pub fn degreeTopologyAlloc(alloc: Allocator, topology: Topology, options: Options) !Result {
    try validateTopology(topology, .degree, options);
    try admitWork(.degree, topology.nodeCount(), topology.edgeCount(), 1, options.max_work_items);
    const node_count = topology.nodeCount();
    const scores = try alloc.alloc(f64, node_count);
    errdefer alloc.free(scores);
    for (scores, 0..) |*score, i| {
        if (i % 4096 == 0) try options.cancellation.check();
        const incoming = topology.incoming_offsets[i + 1] - topology.incoming_offsets[i];
        const outgoing = topology.outgoing_offsets[i + 1] - topology.outgoing_offsets[i];
        score.* = @floatFromInt(@as(u64, incoming) + @as(u64, outgoing));
    }
    return .{ .scores = scores, .iterations_completed = 1, .converged = true, .delta = 0 };
}

fn fillPageRankNext(
    topology: Topology,
    scores: []const f64,
    source_scale: []const f64,
    next: []f64,
    base: f64,
    options: Options,
) !f64 {
    const Worker = struct {
        fn run(
            graph: Topology,
            current: []const f64,
            scale: []const f64,
            output: []f64,
            base_score: f64,
            parts: usize,
            worker: usize,
            width: usize,
            partials: *[reduction_partitions]f64,
            cancellation: CancellationToken,
            failure: *?anyerror,
        ) void {
            var part = worker;
            while (part < parts) : (part += width) {
                var delta: f64 = 0;
                const start = weightedBoundary(graph.incoming_offsets, part, parts);
                const end = weightedBoundary(graph.incoming_offsets, part + 1, parts);
                for (start..end) |target| {
                    if ((target - start) % 4096 == 0) cancellation.check() catch |err| {
                        failure.* = err;
                        return;
                    };
                    var value = base_score;
                    const edge_start: usize = graph.incoming_offsets[target];
                    const edge_end: usize = graph.incoming_offsets[target + 1];
                    for (graph.incoming_sources[edge_start..edge_end], 0..) |source, edge_index| {
                        if (edge_index % 4096 == 0) cancellation.check() catch |err| {
                            failure.* = err;
                            return;
                        };
                        value += current[source] * scale[source];
                    }
                    output[target] = value;
                    delta += @abs(value - current[target]);
                }
                partials[part] = delta;
            }
        }
    };
    const parts = logicalReductionParts(topology.nodeCount());
    var partials: [reduction_partitions]f64 = @splat(0);
    const width = @min(parallelWidth(topology, options), parts);
    if (width == 1) {
        var failure: ?anyerror = null;
        Worker.run(topology, scores, source_scale, next, base, parts, 0, 1, &partials, options.cancellation, &failure);
        if (failure) |err| return err;
    } else {
        const io = options.io.?;
        var failures: [max_kernel_parallelism]?anyerror = @splat(null);
        var group: std.Io.Group = .init;
        for (0..width) |worker| group.async(io, Worker.run, .{
            topology, scores, source_scale, next, base, parts, worker, width, &partials, options.cancellation, &failures[worker],
        });
        try group.await(io);
        for (failures[0..width]) |failure| if (failure) |err| return err;
    }
    var delta: f64 = 0;
    for (partials[0..parts]) |partial| delta += partial;
    return delta;
}

fn fillAdjacencySums(
    topology: Topology,
    input: []const f64,
    output: []f64,
    incoming: bool,
    input_divisor: f64,
    options: Options,
) !void {
    const Worker = struct {
        fn run(
            graph: Topology,
            values: []const f64,
            result: []f64,
            use_incoming: bool,
            input_scale: f64,
            start: usize,
            end: usize,
            cancellation: CancellationToken,
            failure: *?anyerror,
        ) void {
            for (start..end) |ordinal| {
                if ((ordinal - start) % 4096 == 0) cancellation.check() catch |err| {
                    failure.* = err;
                    return;
                };
                var sum: f64 = 0;
                if (use_incoming) {
                    const edge_start: usize = graph.incoming_offsets[ordinal];
                    const edge_end: usize = graph.incoming_offsets[ordinal + 1];
                    for (graph.incoming_sources[edge_start..edge_end], 0..) |source, edge_index| {
                        if (edge_index % 4096 == 0) cancellation.check() catch |err| {
                            failure.* = err;
                            return;
                        };
                        sum += values[source] * input_scale;
                    }
                } else {
                    const edge_start: usize = graph.outgoing_offsets[ordinal];
                    const edge_end: usize = graph.outgoing_offsets[ordinal + 1];
                    for (graph.outgoing_targets[edge_start..edge_end], 0..) |target, edge_index| {
                        if (edge_index % 4096 == 0) cancellation.check() catch |err| {
                            failure.* = err;
                            return;
                        };
                        sum += values[target] * input_scale;
                    }
                }
                result[ordinal] = sum;
            }
        }
    };
    const offsets = if (incoming) topology.incoming_offsets else topology.outgoing_offsets;
    const input_scale = if (input_divisor > 0) 1.0 / input_divisor else 1.0;
    const width = parallelWidth(topology, options);
    if (width == 1) {
        var failure: ?anyerror = null;
        Worker.run(topology, input, output, incoming, input_scale, 0, topology.nodeCount(), options.cancellation, &failure);
        if (failure) |err| return err;
        return;
    }
    const io = options.io.?;
    var failures: [max_kernel_parallelism]?anyerror = @splat(null);
    var group: std.Io.Group = .init;
    for (0..width) |part| {
        const start = weightedBoundary(offsets, part, width);
        const end = weightedBoundary(offsets, part + 1, width);
        if (start == end) continue;
        group.async(io, Worker.run, .{ topology, input, output, incoming, input_scale, start, end, options.cancellation, &failures[part] });
    }
    try group.await(io);
    for (failures[0..width]) |failure| if (failure) |err| return err;
}

pub fn pageRankAlloc(alloc: Allocator, node_count: usize, edges: []const Edge, options: Options) !Result {
    try validateInputBoundsAndOptions(node_count, edges.len, options);
    try admitWork(.pagerank, node_count, edges.len, options.max_iterations, options.max_work_items);
    var topology = try Topology.initAllocFor(alloc, node_count, edges, .pagerank, options.cancellation);
    defer topology.deinit(alloc);
    return try pageRankTopologyAlloc(alloc, topology, options);
}

pub fn pageRankTopologyAlloc(alloc: Allocator, topology: Topology, options: Options) !Result {
    try validateTopology(topology, .pagerank, options);
    try admitWork(.pagerank, topology.nodeCount(), topology.edgeCount(), options.max_iterations, options.max_work_items);
    const node_count = topology.nodeCount();
    var scores = try alloc.alloc(f64, node_count);
    errdefer alloc.free(scores);
    if (node_count == 0) return .{ .scores = scores, .iterations_completed = 0, .converged = true, .delta = 0 };
    var next = try alloc.alloc(f64, node_count);
    defer alloc.free(next);
    // Reuse one dense vector for the damped reciprocal out-degree. This moves
    // division out of the O(E * iterations) edge loop without increasing peak
    // memory over the previous degree vector.
    const source_scale = try alloc.alloc(f64, node_count);
    defer alloc.free(source_scale);
    for (source_scale, 0..) |*scale, i| {
        if (i % 4096 == 0) try options.cancellation.check();
        const out_degree = topology.outgoing_offsets[i + 1] - topology.outgoing_offsets[i];
        scale.* = if (out_degree == 0) 0 else options.damping / @as(f64, @floatFromInt(out_degree));
    }
    const count: f64 = @floatFromInt(node_count);
    @memset(scores, 1.0 / count);

    var iteration: u32 = 0;
    var delta: f64 = 0;
    while (iteration < options.max_iterations) {
        try options.cancellation.check();
        iteration += 1;
        const sink_mass = try pageRankSinkMass(scores, source_scale, options);
        const base = (1.0 - options.damping + options.damping * sink_mass) / count;
        delta = try fillPageRankNext(topology, scores, source_scale, next, base, options);
        const previous = scores;
        scores = next;
        next = previous;
        if (!std.math.isFinite(delta)) return error.InvalidGraphMetricScore;
        if (delta <= options.tolerance) return .{ .scores = scores, .iterations_completed = iteration, .converged = true, .delta = delta };
    }
    return .{ .scores = scores, .iterations_completed = iteration, .converged = false, .delta = delta };
}

pub fn eigenvectorAlloc(alloc: Allocator, node_count: usize, edges: []const Edge, options: Options) !Result {
    try validateInputBoundsAndOptions(node_count, edges.len, options);
    try admitWork(.eigenvector, node_count, edges.len, options.max_iterations, options.max_work_items);
    var topology = try Topology.initAllocFor(alloc, node_count, edges, .eigenvector, options.cancellation);
    defer topology.deinit(alloc);
    return try eigenvectorTopologyAlloc(alloc, topology, options);
}

pub fn eigenvectorTopologyAlloc(alloc: Allocator, topology: Topology, options: Options) !Result {
    try validateTopology(topology, .eigenvector, options);
    try admitWork(.eigenvector, topology.nodeCount(), topology.edgeCount(), options.max_iterations, options.max_work_items);
    const node_count = topology.nodeCount();
    var scores = try alloc.alloc(f64, node_count);
    errdefer alloc.free(scores);
    if (node_count == 0) return .{ .scores = scores, .iterations_completed = 0, .converged = true, .delta = 0 };
    var next = try alloc.alloc(f64, node_count);
    defer alloc.free(next);
    @memset(scores, 1.0 / @sqrt(@as(f64, @floatFromInt(node_count))));
    var iteration: u32 = 0;
    var delta: f64 = 0;
    while (iteration < options.max_iterations) {
        try options.cancellation.check();
        iteration += 1;
        try fillAdjacencySums(topology, scores, next, true, 1, options);
        delta = try normalizeSwapAndDelta(&scores, &next, options);
        if (!std.math.isFinite(delta)) return error.InvalidGraphMetricScore;
        if (delta <= options.tolerance) return .{ .scores = scores, .iterations_completed = iteration, .converged = true, .delta = delta };
    }
    return .{ .scores = scores, .iterations_completed = iteration, .converged = false, .delta = delta };
}

pub fn hitsAlloc(alloc: Allocator, node_count: usize, edges: []const Edge, options: Options) !HitsResult {
    try validateInputBoundsAndOptions(node_count, edges.len, options);
    try admitWork(.hits, node_count, edges.len, options.max_iterations, options.max_work_items);
    var topology = try Topology.initAllocFor(alloc, node_count, edges, .hits, options.cancellation);
    defer topology.deinit(alloc);
    return try hitsTopologyAlloc(alloc, topology, options);
}

pub fn hitsTopologyAlloc(alloc: Allocator, topology: Topology, options: Options) !HitsResult {
    try validateTopology(topology, .hits, options);
    try admitWork(.hits, topology.nodeCount(), topology.edgeCount(), options.max_iterations, options.max_work_items);
    const node_count = topology.nodeCount();
    const authorities = try alloc.alloc(f64, node_count);
    errdefer alloc.free(authorities);
    const hubs = try alloc.alloc(f64, node_count);
    errdefer alloc.free(hubs);
    if (node_count == 0) return .{ .authorities = authorities, .hubs = hubs, .iterations_completed = 0, .converged = true, .delta = 0 };
    const next_authorities = try alloc.alloc(f64, node_count);
    defer alloc.free(next_authorities);
    const next_hubs = try alloc.alloc(f64, node_count);
    defer alloc.free(next_hubs);
    const initial = 1.0 / @sqrt(@as(f64, @floatFromInt(node_count)));
    @memset(authorities, initial);
    @memset(hubs, initial);
    var iteration: u32 = 0;
    var delta: f64 = 0;
    while (iteration < options.max_iterations) {
        try options.cancellation.check();
        iteration += 1;
        try fillAdjacencySums(topology, hubs, next_authorities, true, 1, options);
        const authority_norm = @sqrt(try normSquared(next_authorities, options));
        try fillAdjacencySums(topology, next_authorities, next_hubs, false, authority_norm, options);
        const hub_norm = @sqrt(try normSquared(next_hubs, options));
        delta = try replaceHitsAndDelta(authorities, hubs, next_authorities, next_hubs, authority_norm, hub_norm, options);
        if (!std.math.isFinite(delta)) return error.InvalidGraphMetricScore;
        if (delta <= options.tolerance) return .{ .authorities = authorities, .hubs = hubs, .iterations_completed = iteration, .converged = true, .delta = delta };
    }
    return .{ .authorities = authorities, .hubs = hubs, .iterations_completed = iteration, .converged = false, .delta = delta };
}

fn pageRankSinkMass(scores: []const f64, source_scale: []const f64, options: Options) !f64 {
    const Worker = struct {
        fn run(
            values: []const f64,
            scales: []const f64,
            parts: usize,
            worker: usize,
            width: usize,
            partials: *[reduction_partitions]f64,
            cancellation: CancellationToken,
            failure: *?anyerror,
        ) void {
            var part = worker;
            while (part < parts) : (part += width) {
                var sum: f64 = 0;
                const start = vectorBoundary(values.len, part, parts);
                const end = vectorBoundary(values.len, part + 1, parts);
                for (values[start..end], scales[start..end], 0..) |value, scale, i| {
                    if (i % 4096 == 0) cancellation.check() catch |err| {
                        failure.* = err;
                        return;
                    };
                    if (scale == 0) sum += value;
                }
                partials[part] = sum;
            }
        }
    };
    if (scores.len != source_scale.len) return error.InvalidGraphMetricScore;
    const parts = logicalReductionParts(scores.len);
    var partials: [reduction_partitions]f64 = @splat(0);
    const width = @min(vectorParallelWidth(scores.len, options), parts);
    if (width == 1) {
        var failure: ?anyerror = null;
        Worker.run(scores, source_scale, parts, 0, 1, &partials, options.cancellation, &failure);
        if (failure) |err| return err;
    } else {
        const io = options.io.?;
        var failures: [max_kernel_parallelism]?anyerror = @splat(null);
        var group: std.Io.Group = .init;
        for (0..width) |worker| group.async(io, Worker.run, .{
            scores, source_scale, parts, worker, width, &partials, options.cancellation, &failures[worker],
        });
        try group.await(io);
        for (failures[0..width]) |failure| if (failure) |err| return err;
    }
    var total: f64 = 0;
    for (partials[0..parts]) |partial| total += partial;
    return total;
}

fn normSquared(values: []const f64, options: Options) !f64 {
    const Worker = struct {
        fn run(
            input: []const f64,
            parts: usize,
            worker: usize,
            width: usize,
            partials: *[reduction_partitions]f64,
            cancellation: CancellationToken,
            failure: *?anyerror,
        ) void {
            var part = worker;
            while (part < parts) : (part += width) {
                var sum: f64 = 0;
                const start = vectorBoundary(input.len, part, parts);
                const end = vectorBoundary(input.len, part + 1, parts);
                for (input[start..end], 0..) |value, i| {
                    if (i % 4096 == 0) cancellation.check() catch |err| {
                        failure.* = err;
                        return;
                    };
                    sum += value * value;
                }
                partials[part] = sum;
            }
        }
    };
    const parts = logicalReductionParts(values.len);
    var partials: [reduction_partitions]f64 = @splat(0);
    const width = @min(vectorParallelWidth(values.len, options), parts);
    if (width == 1) {
        var failure: ?anyerror = null;
        Worker.run(values, parts, 0, 1, &partials, options.cancellation, &failure);
        if (failure) |err| return err;
    } else {
        const io = options.io.?;
        var failures: [max_kernel_parallelism]?anyerror = @splat(null);
        var group: std.Io.Group = .init;
        for (0..width) |worker| group.async(io, Worker.run, .{
            values, parts, worker, width, &partials, options.cancellation, &failures[worker],
        });
        try group.await(io);
        for (failures[0..width]) |failure| if (failure) |err| return err;
    }
    var total: f64 = 0;
    for (partials[0..parts]) |partial| total += partial;
    return total;
}

fn scaleValues(values: []f64, denominator: f64, options: Options) !void {
    const Worker = struct {
        fn run(
            output: []f64,
            divisor: f64,
            start: usize,
            end: usize,
            cancellation: CancellationToken,
            failure: *?anyerror,
        ) void {
            for (output[start..end], 0..) |*value, i| {
                if (i % 4096 == 0) cancellation.check() catch |err| {
                    failure.* = err;
                    return;
                };
                value.* /= divisor;
            }
        }
    };
    const width = vectorParallelWidth(values.len, options);
    if (width == 1) {
        var failure: ?anyerror = null;
        Worker.run(values, denominator, 0, values.len, options.cancellation, &failure);
        if (failure) |err| return err;
        return;
    }
    const io = options.io.?;
    var failures: [max_kernel_parallelism]?anyerror = @splat(null);
    var group: std.Io.Group = .init;
    for (0..width) |part| group.async(io, Worker.run, .{
        values,
        denominator,
        vectorBoundary(values.len, part, width),
        vectorBoundary(values.len, part + 1, width),
        options.cancellation,
        &failures[part],
    });
    try group.await(io);
    for (failures[0..width]) |failure| if (failure) |err| return err;
}

fn normalize(values: []f64, options: Options) !void {
    const norm_sq = try normSquared(values, options);
    const norm = @sqrt(norm_sq);
    if (norm > 0) try scaleValues(values, norm, options);
}

/// Normalize the newly computed vector and calculate convergence in the same
/// cache pass. Iterative eigenvector builds previously streamed the full
/// vector once to normalize and again to compare it with the prior vector.
fn normalizeSwapAndDelta(current: *[]f64, next: *[]f64, options: Options) !f64 {
    if (current.*.len != next.*.len) return error.InvalidGraphMetricScore;
    const norm = @sqrt(try normSquared(next.*, options));
    const Worker = struct {
        fn run(
            old_values: []const f64,
            new_values: []f64,
            divisor: f64,
            parts: usize,
            worker: usize,
            width: usize,
            partials: *[reduction_partitions]f64,
            cancellation: CancellationToken,
            failure: *?anyerror,
        ) void {
            var part = worker;
            while (part < parts) : (part += width) {
                var sum: f64 = 0;
                const start = vectorBoundary(old_values.len, part, parts);
                const end = vectorBoundary(old_values.len, part + 1, parts);
                for (start..end) |i| {
                    if ((i - start) % 4096 == 0) cancellation.check() catch |err| {
                        failure.* = err;
                        return;
                    };
                    if (divisor > 0) new_values[i] /= divisor;
                    sum += @abs(new_values[i] - old_values[i]);
                }
                partials[part] = sum;
            }
        }
    };
    const parts = logicalReductionParts(current.*.len);
    var partials: [reduction_partitions]f64 = @splat(0);
    const width = @min(vectorParallelWidth(current.*.len, options), parts);
    if (width == 1) {
        var failure: ?anyerror = null;
        Worker.run(current.*, next.*, norm, parts, 0, 1, &partials, options.cancellation, &failure);
        if (failure) |err| return err;
    } else {
        const io = options.io.?;
        var failures: [max_kernel_parallelism]?anyerror = @splat(null);
        var group: std.Io.Group = .init;
        for (0..width) |worker| group.async(io, Worker.run, .{
            current.*, next.*, norm, parts, worker, width, &partials, options.cancellation, &failures[worker],
        });
        try group.await(io);
        for (failures[0..width]) |failure| if (failure) |err| return err;
    }
    var delta: f64 = 0;
    for (partials[0..parts]) |partial| delta += partial;
    const previous = current.*;
    current.* = next.*;
    next.* = previous;
    return delta;
}

fn swapAndDelta(current: *[]f64, next: *[]f64, options: Options) !f64 {
    const Worker = struct {
        fn run(
            old_values: []const f64,
            new_values: []const f64,
            parts: usize,
            worker: usize,
            width: usize,
            partials: *[reduction_partitions]f64,
            cancellation: CancellationToken,
            failure: *?anyerror,
        ) void {
            var part = worker;
            while (part < parts) : (part += width) {
                var sum: f64 = 0;
                const start = vectorBoundary(old_values.len, part, parts);
                const end = vectorBoundary(old_values.len, part + 1, parts);
                for (old_values[start..end], new_values[start..end], 0..) |old, new, i| {
                    if (i % 4096 == 0) cancellation.check() catch |err| {
                        failure.* = err;
                        return;
                    };
                    sum += @abs(new - old);
                }
                partials[part] = sum;
            }
        }
    };
    if (current.*.len != next.*.len) return error.InvalidGraphMetricScore;
    const parts = logicalReductionParts(current.*.len);
    var partials: [reduction_partitions]f64 = @splat(0);
    const width = @min(vectorParallelWidth(current.*.len, options), parts);
    if (width == 1) {
        var failure: ?anyerror = null;
        Worker.run(current.*, next.*, parts, 0, 1, &partials, options.cancellation, &failure);
        if (failure) |err| return err;
    } else {
        const io = options.io.?;
        var failures: [max_kernel_parallelism]?anyerror = @splat(null);
        var group: std.Io.Group = .init;
        for (0..width) |worker| group.async(io, Worker.run, .{
            current.*, next.*, parts, worker, width, &partials, options.cancellation, &failures[worker],
        });
        try group.await(io);
        for (failures[0..width]) |failure| if (failure) |err| return err;
    }
    var delta: f64 = 0;
    for (partials[0..parts]) |partial| delta += partial;
    const previous = current.*;
    current.* = next.*;
    next.* = previous;
    return delta;
}

fn replaceHitsAndDelta(
    authorities: []f64,
    hubs: []f64,
    next_authorities: []const f64,
    next_hubs: []const f64,
    authority_norm: f64,
    hub_norm: f64,
    options: Options,
) !f64 {
    const Worker = struct {
        fn run(
            authority_values: []f64,
            hub_values: []f64,
            new_authorities: []const f64,
            new_hubs: []const f64,
            authority_divisor: f64,
            hub_divisor: f64,
            parts: usize,
            worker: usize,
            width: usize,
            partials: *[reduction_partitions]f64,
            cancellation: CancellationToken,
            failure: *?anyerror,
        ) void {
            var part = worker;
            while (part < parts) : (part += width) {
                var sum: f64 = 0;
                const start = vectorBoundary(authority_values.len, part, parts);
                const end = vectorBoundary(authority_values.len, part + 1, parts);
                for (start..end) |i| {
                    if ((i - start) % 4096 == 0) cancellation.check() catch |err| {
                        failure.* = err;
                        return;
                    };
                    const new_authority = if (authority_divisor > 0) new_authorities[i] / authority_divisor else new_authorities[i];
                    const new_hub = if (hub_divisor > 0) new_hubs[i] / hub_divisor else new_hubs[i];
                    sum += @abs(new_authority - authority_values[i]);
                    sum += @abs(new_hub - hub_values[i]);
                    authority_values[i] = new_authority;
                    hub_values[i] = new_hub;
                }
                partials[part] = sum;
            }
        }
    };
    if (authorities.len != hubs.len or authorities.len != next_authorities.len or authorities.len != next_hubs.len)
        return error.InvalidGraphMetricScore;
    const parts = logicalReductionParts(authorities.len);
    var partials: [reduction_partitions]f64 = @splat(0);
    const width = @min(vectorParallelWidth(authorities.len, options), parts);
    if (width == 1) {
        var failure: ?anyerror = null;
        Worker.run(authorities, hubs, next_authorities, next_hubs, authority_norm, hub_norm, parts, 0, 1, &partials, options.cancellation, &failure);
        if (failure) |err| return err;
    } else {
        const io = options.io.?;
        var failures: [max_kernel_parallelism]?anyerror = @splat(null);
        var group: std.Io.Group = .init;
        for (0..width) |worker| group.async(io, Worker.run, .{
            authorities, hubs, next_authorities, next_hubs, authority_norm, hub_norm, parts, worker, width, &partials, options.cancellation, &failures[worker],
        });
        try group.await(io);
        for (failures[0..width]) |failure| if (failure) |err| return err;
    }
    var delta: f64 = 0;
    for (partials[0..parts]) |partial| delta += partial;
    return delta;
}

test "serverless bounded graph metric kernels compute all supported metrics" {
    const edges = [_]Edge{ .{ .source = 0, .target = 1 }, .{ .source = 2, .target = 1 }, .{ .source = 1, .target = 0 } };
    var degree = try degreeAlloc(std.testing.allocator, 3, &edges, .{});
    defer degree.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(f64, 3), degree.scores[1]);
    var pagerank = try pageRankAlloc(std.testing.allocator, 3, &edges, .{});
    defer pagerank.deinit(std.testing.allocator);
    try std.testing.expect(pagerank.scores[1] > pagerank.scores[2]);
    var eigenvector = try eigenvectorAlloc(std.testing.allocator, 3, &edges, .{});
    defer eigenvector.deinit(std.testing.allocator);
    try std.testing.expect(eigenvector.iterations_completed > 0);
    var hits = try hitsAlloc(std.testing.allocator, 3, &edges, .{});
    defer hits.deinit(std.testing.allocator);
    try std.testing.expect(hits.authorities[1] > hits.authorities[2]);
}

test "serverless graph metric kernels reject unbounded work before allocating" {
    try std.testing.expectError(error.GraphMetricBuildBudgetExceeded, pageRankAlloc(std.testing.allocator, 2, &.{}, .{ .max_nodes = 1 }));
    try std.testing.expectError(error.InvalidGraphMetricEdge, degreeAlloc(std.testing.allocator, 1, &.{.{ .source = 0, .target = 1 }}, .{}));
}

test "graph metric topology materializes only requested adjacency lanes" {
    const edges = [_]Edge{ .{ .source = 0, .target = 1 }, .{ .source = 1, .target = 0 } };
    var degree_topology = try Topology.initAllocFor(std.testing.allocator, 2, &edges, .degree, .none);
    defer degree_topology.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 3), degree_topology.incoming_offsets.len);
    try std.testing.expectEqual(@as(usize, 0), degree_topology.incoming_sources.len);
    try std.testing.expectEqual(@as(usize, 3), degree_topology.outgoing_offsets.len);
    try std.testing.expectEqual(@as(usize, 0), degree_topology.outgoing_targets.len);
    try std.testing.expectError(error.InvalidGraphMetricEdge, pageRankTopologyAlloc(std.testing.allocator, degree_topology, .{}));

    var eigenvector_topology = try Topology.initAllocFor(std.testing.allocator, 2, &edges, .eigenvector, .none);
    defer eigenvector_topology.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, edges.len), eigenvector_topology.incoming_sources.len);
    try std.testing.expectEqual(@as(usize, 0), eigenvector_topology.outgoing_offsets.len);
    var eigenvector = try eigenvectorTopologyAlloc(std.testing.allocator, eigenvector_topology, .{});
    defer eigenvector.deinit(std.testing.allocator);
}

test "serverless graph metric runtime fanout preserves deterministic target-owned results" {
    const alloc = std.testing.allocator;
    const node_count: usize = parallel_vector_threshold;
    const edge_count: usize = parallel_edge_threshold;
    const edges = try alloc.alloc(Edge, edge_count);
    defer alloc.free(edges);
    for (edges, 0..) |*edge, i| {
        edge.* = .{
            .source = @intCast(i % node_count),
            .target = @intCast((i * 17 + 3) % node_count),
        };
    }
    var topology = try Topology.initAlloc(alloc, node_count, edges, .none);
    defer topology.deinit(alloc);
    const options = Options{ .max_iterations = 3, .max_work_items = 10_000_000 };
    var serial = try pageRankTopologyAlloc(alloc, topology, options);
    defer serial.deinit(alloc);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    var parallel_options = options;
    parallel_options.io = io_impl.io();
    parallel_options.max_parallelism = 4;
    var parallel = try pageRankTopologyAlloc(alloc, topology, parallel_options);
    defer parallel.deinit(alloc);
    try std.testing.expectEqualSlices(f64, serial.scores, parallel.scores);

    const values = try alloc.alloc(f64, node_count);
    defer alloc.free(values);
    const serial_incoming = try alloc.alloc(f64, node_count);
    defer alloc.free(serial_incoming);
    const parallel_incoming = try alloc.alloc(f64, node_count);
    defer alloc.free(parallel_incoming);
    const serial_outgoing = try alloc.alloc(f64, node_count);
    defer alloc.free(serial_outgoing);
    const parallel_outgoing = try alloc.alloc(f64, node_count);
    defer alloc.free(parallel_outgoing);
    for (values, 0..) |*value, i| value.* = @floatFromInt(i % 31);
    try fillAdjacencySums(topology, values, serial_incoming, true, 1, options);
    try fillAdjacencySums(topology, values, parallel_incoming, true, 1, parallel_options);
    try std.testing.expectEqualSlices(f64, serial_incoming, parallel_incoming);
    try fillAdjacencySums(topology, values, serial_outgoing, false, 1, options);
    try fillAdjacencySums(topology, values, parallel_outgoing, false, 1, parallel_options);
    try std.testing.expectEqualSlices(f64, serial_outgoing, parallel_outgoing);

    try std.testing.expectEqual(
        try normSquared(values, options),
        try normSquared(values, parallel_options),
    );
    const serial_normalized = try alloc.dupe(f64, values);
    defer alloc.free(serial_normalized);
    const parallel_normalized = try alloc.dupe(f64, values);
    defer alloc.free(parallel_normalized);
    try normalize(serial_normalized, options);
    try normalize(parallel_normalized, parallel_options);
    try std.testing.expectEqualSlices(f64, serial_normalized, parallel_normalized);
}
