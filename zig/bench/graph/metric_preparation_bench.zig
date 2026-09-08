// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const antfly = @import("antfly_zig");
const graph = antfly.serverless.graph_segment;
const metric = antfly.serverless.build.lake_graph_metric;

const PhaseAllocStats = struct {
    current_bytes: usize = 0,
    peak_bytes: usize = 0,
    total_alloc_bytes: usize = 0,
    total_free_bytes: usize = 0,
    alloc_count: usize = 0,
    free_count: usize = 0,

    fn noteAlloc(self: *PhaseAllocStats, len: usize) void {
        self.current_bytes +|= len;
        self.total_alloc_bytes +|= len;
        self.alloc_count +|= 1;
        self.peak_bytes = @max(self.peak_bytes, self.current_bytes);
    }

    fn noteFree(self: *PhaseAllocStats, len: usize) void {
        self.current_bytes -|= len;
        self.total_free_bytes +|= len;
        self.free_count +|= 1;
    }

    fn noteResize(self: *PhaseAllocStats, old_len: usize, new_len: usize) void {
        if (new_len > old_len) {
            self.noteAlloc(new_len - old_len);
        } else if (old_len > new_len) {
            self.noteFree(old_len - new_len);
        }
    }
};

const PhaseTrackingAllocator = struct {
    backing: std.mem.Allocator,
    stats: *PhaseAllocStats,

    fn allocator(self: *PhaseTrackingAllocator) std.mem.Allocator {
        return .{
            .ptr = self,
            .vtable = &.{
                .alloc = alloc,
                .resize = resize,
                .remap = remap,
                .free = free,
            },
        };
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *PhaseTrackingAllocator = @ptrCast(@alignCast(ctx));
        const ptr = self.backing.rawAlloc(len, alignment, ret_addr) orelse return null;
        self.stats.noteAlloc(len);
        return ptr;
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *PhaseTrackingAllocator = @ptrCast(@alignCast(ctx));
        if (!self.backing.rawResize(memory, alignment, new_len, ret_addr)) return false;
        self.stats.noteResize(memory.len, new_len);
        return true;
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *PhaseTrackingAllocator = @ptrCast(@alignCast(ctx));
        const ptr = self.backing.rawRemap(memory, alignment, new_len, ret_addr) orelse return null;
        self.stats.noteResize(memory.len, new_len);
        return ptr;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *PhaseTrackingAllocator = @ptrCast(@alignCast(ctx));
        self.backing.rawFree(memory, alignment, ret_addr);
        self.stats.noteFree(memory.len);
    }
};

fn benchmarkSparseProjections(output: anytype) !void {
    const alloc = std.heap.smp_allocator;
    const ids = try alloc.alloc([]const u8, 1_000_000);
    defer alloc.free(ids);
    @memset(ids, "unused");
    ids[0] = "a";
    ids[ids.len - 1] = "z";
    inline for (.{ .degree, .pagerank }) |kind| {
        var expected: ?u64 = null;
        for ([_]bool{ true, false }) |reference| {
            var times: [5]u64 = undefined;
            var last = PhaseAllocStats{};
            for (0..6) |sample| {
                var stats = PhaseAllocStats{};
                var tracking = PhaseTrackingAllocator{ .backing = alloc, .stats = &stats };
                const start = antfly.platform_time.monotonicNs();
                for (0..256) |_| {
                    const digest = try metric.benchmarkSparseProjection(tracking.allocator(), ids, kind, reference);
                    if (expected) |value| {
                        if (value != digest) return error.InvalidBenchmarkResult;
                    } else expected = digest;
                }
                const elapsed = (antfly.platform_time.monotonicNs() - start) / 256;
                if (stats.current_bytes != 0) return error.InvalidBenchmarkResult;
                if (sample != 0) times[sample - 1] = elapsed;
                last = stats;
            }
            std.mem.sort(u64, &times, {}, std.sort.asc(u64));
            const json = try std.json.Stringify.valueAlloc(alloc, .{
                .mode = if (reference) "sparse_source_wide_reference" else "sparse_active_endpoints",
                .kind = @tagName(kind),
                .source_nodes = ids.len,
                .active_nodes = 2,
                .edges = 2,
                .median_ns = times[2],
                .peak_bytes = last.peak_bytes,
                .allocation_count = last.alloc_count / 256,
                .note = "prepared dictionary excluded; exact node/CSR checksum parity; 256 repetitions per sample; six samples, first discarded",
            }, .{});
            defer alloc.free(json);
            try output.interface.writeAll(json);
            try output.interface.writeByte('\n');
            try output.flush();
        }
    }
}

pub fn main(init: std.process.Init) !void {
    var output_buf: [4096]u8 = undefined;
    var output = std.Io.File.stdout().writer(init.io, &output_buf);
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, std.heap.smp_allocator);
    defer args.deinit();
    _ = args.next();
    var staged_only = false;
    while (args.next()) |arg| {
        if (!std.mem.eql(u8, arg, "--staged-only")) return error.InvalidArgument;
        staged_only = true;
    }
    try benchmarkStagedQueries(init.io, &output);
    if (staged_only) return;
    try benchmarkStateful(&output);
    try benchmarkVectorWrites(&output);
    try benchmarkQuerySnapshots(init.io, &output);
    try benchmarkMembership(init.io, &output);
    try benchmarkOrdinalFold(&output);
    try benchmarkSealedVectors(init.io, &output);
    try benchmarkPublication(init.io, &output);
    try benchmarkRoutingWorkingSet(&output);
    try benchmarkSparseProjections(&output);
    try benchmarkCandidatePlanning(&output);
    try benchmarkAuthenticatedCache(init.io, &output);
    try benchmarkTopOwnership(&output);
    for ([_]usize{ 2_000, 20_000, 50_000 }) |nodes| {
        var fixture = std.heap.ArenaAllocator.init(std.heap.smp_allocator);
        defer fixture.deinit();
        const alloc = fixture.allocator();
        const degree = 8;
        const ids = try alloc.alloc([]u8, nodes);
        for (ids, 0..) |*id, i| id.* = try std.fmt.allocPrint(alloc, "source/snapshot/file-0001/customer-record-{d:0>8}", .{i});
        const adjacencies = try alloc.alloc(graph.Adjacency, nodes);
        var old_wire_bytes: usize = 14;
        for (adjacencies, 0..) |*adjacency, i| {
            const out = try alloc.alloc(graph.Edge, degree);
            const in = try alloc.alloc(graph.Edge, degree);
            for (out, in, 0..) |*forward, *reverse, j| {
                forward.* = .{ .neighbor_id = ids[(i + j + 1) % nodes], .edge_type = @constCast("follows"), .weight = 1 };
                reverse.* = .{ .neighbor_id = ids[(i + nodes - j - 1) % nodes], .edge_type = @constCast("follows"), .weight = 1 };
                old_wire_bytes += 2 * (16 + ids[i].len + "follows".len);
            }
            const less = struct {
                fn less(_: void, a: graph.Edge, b: graph.Edge) bool {
                    return graph.edgeLookupOrder(a.edge_type, a.neighbor_id, b.edge_type, b.neighbor_id) == .lt;
                }
            }.less;
            std.mem.sort(graph.Edge, out, {}, less);
            std.mem.sort(graph.Edge, in, {}, less);
            adjacency.* = .{ .node_id = ids[i], .out_edges = out, .in_edges = in };
            old_wire_bytes += 12 + ids[i].len;
        }
        const segment = graph.Segment{ .adjacencies = adjacencies };
        const payload = try graph.encodeAlloc(alloc, segment);
        if (nodes == 50_000) {
            var expected: ?usize = null;
            for ([_]bool{ true, false }) |reference| {
                var times: [5]u64 = undefined;
                var last = PhaseAllocStats{};
                for (0..6) |sample| {
                    var stats = PhaseAllocStats{};
                    var tracking = PhaseTrackingAllocator{ .backing = std.heap.smp_allocator, .stats = &stats };
                    const start = antfly.platform_time.monotonicNs();
                    const checksum = try metric.benchmarkProjection(tracking.allocator(), payload, reference);
                    const elapsed = antfly.platform_time.monotonicNs() - start;
                    if (expected) |value| {
                        if (value != checksum) return error.InvalidBenchmarkResult;
                    } else expected = checksum;
                    if (stats.current_bytes != 0) return error.InvalidBenchmarkResult;
                    if (sample != 0) times[sample - 1] = elapsed;
                    last = stats;
                }
                std.mem.sort(u64, &times, {}, std.sort.asc(u64));
                const json = try std.json.Stringify.valueAlloc(alloc, .{
                    .mode = if (reference) "projection_edge_copy_reference" else "projection_direct_csr",
                    .nodes = nodes,
                    .edges = nodes * degree,
                    .median_ns = times[2],
                    .min_ns = times[0],
                    .max_ns = times[4],
                    .allocation_count = last.alloc_count,
                    .allocated_bytes = last.total_alloc_bytes,
                    .peak_bytes = last.peak_bytes,
                    .note = "source preparation and PageRank projection; exact CSR checksum equality; excludes fetch, kernels and upload",
                }, .{});
                try output.interface.writeAll(json);
                try output.interface.writeByte('\n');
                try output.flush();
            }
        }
        if (nodes == 50_000) for ([_]bool{ true, false }) |reference| {
            var times: [5]u64 = undefined;
            var last = PhaseAllocStats{};
            for (0..6) |sample| {
                var stats = PhaseAllocStats{};
                var tracking = PhaseTrackingAllocator{ .backing = std.heap.smp_allocator, .stats = &stats };
                const start = antfly.platform_time.monotonicNs();
                const count = try metric.benchmarkRejectedOutput(tracking.allocator(), payload, reference);
                const elapsed = antfly.platform_time.monotonicNs() - start;
                if (count != nodes * degree or stats.current_bytes != 0) return error.InvalidBenchmarkResult;
                if (sample != 0) times[sample - 1] = elapsed;
                last = stats;
            }
            std.mem.sort(u64, &times, {}, std.sort.asc(u64));
            const json = try std.json.Stringify.valueAlloc(alloc, .{
                .mode = if (reference) "output_reject_after_kernel_reference" else "output_reject_before_kernel",
                .nodes = nodes,
                .edges = nodes * degree,
                .median_ns = times[2],
                .min_ns = times[0],
                .max_ns = times[4],
                .allocation_count = last.alloc_count,
                .allocated_bytes = last.total_alloc_bytes,
                .peak_bytes = last.peak_bytes,
                .note = "includes source and projection preparation; reference computes and encodes PageRank before quota rejection; excludes fetch and upload",
            }, .{});
            try output.interface.writeAll(json);
            try output.interface.writeByte('\n');
            try output.flush();
        };
        if (nodes == 50_000) for ([_]bool{ true, false }) |reference| {
            var times: [5]u64 = undefined;
            var last = PhaseAllocStats{};
            for (0..6) |sample| {
                var stats = PhaseAllocStats{};
                var tracking = PhaseTrackingAllocator{ .backing = std.heap.smp_allocator, .stats = &stats };
                const start = antfly.platform_time.monotonicNs();
                const edge_count = try metric.benchmarkRejectedPreparation(tracking.allocator(), payload, reference);
                const elapsed = antfly.platform_time.monotonicNs() - start;
                if (edge_count != nodes * degree or stats.current_bytes != 0) return error.InvalidBenchmarkResult;
                if (sample != 0) times[sample - 1] = elapsed;
                last = stats;
            }
            std.mem.sort(u64, &times, {}, std.sort.asc(u64));
            const json = try std.json.Stringify.valueAlloc(alloc, .{
                .mode = if (reference) "rejection_after_projection_reference" else "rejection_before_projection",
                .nodes = nodes,
                .edges = nodes * degree,
                .projection_groups = 16,
                .median_ns = times[2],
                .min_ns = times[0],
                .max_ns = times[4],
                .allocation_count = last.alloc_count,
                .allocated_bytes = last.total_alloc_bytes,
                .peak_bytes = last.peak_bytes,
                .note = "includes one source preparation and sixteen exhausted projection attempts; excludes fetch and rejection encoding",
            }, .{});
            try output.interface.writeAll(json);
            try output.interface.writeByte('\n');
            try output.flush();
        };
        for ([_]bool{ true, false }) |reference| {
            _ = try metric.benchmarkPreparation(std.heap.smp_allocator, payload, reference);
            var times: [5]u64 = undefined;
            var last = PhaseAllocStats{};
            for (&times) |*elapsed| {
                var stats = PhaseAllocStats{};
                var allocator = PhaseTrackingAllocator{ .backing = std.heap.smp_allocator, .stats = &stats };
                const start = antfly.platform_time.monotonicNs();
                const edge_count = try metric.benchmarkPreparation(allocator.allocator(), payload, reference);
                elapsed.* = antfly.platform_time.monotonicNs() - start;
                if (edge_count != nodes * degree or stats.current_bytes != 0) return error.InvalidBenchmarkResult;
                last = stats;
            }
            std.mem.sort(u64, &times, {}, std.sort.asc(u64));
            const json = try std.json.Stringify.valueAlloc(alloc, .{
                .mode = if (reference) "unpack_hash_reference" else "packed_ordinals",
                .nodes = nodes,
                .edges = nodes * degree,
                .v2_wire_bytes = old_wire_bytes,
                .v3_wire_bytes = payload.len,
                .median_ns = times[2],
                .min_ns = times[0],
                .max_ns = times[4],
                .allocation_count = last.alloc_count,
                .allocated_bytes = last.total_alloc_bytes,
                .peak_bytes = last.peak_bytes,
                .samples = times.len,
                .note = "same v3 input; excludes fetch, encoding and numeric kernel; one warmup",
            }, .{});
            try output.interface.writeAll(json);
            try output.interface.writeByte('\n');
            try output.flush();
        }
    }
}

const ScoreTxn = struct {
    raw: [8]u8 = .{ 0, 0, 0, 0, 0, 0, 0xf0, 0x3f },
    key_count: usize = 0,
    key_hash: u64 = 0,
    pub fn getManySorted(self: *@This(), keys: []const []const u8, values: []?[]const u8) !void {
        for (keys, values, 0..) |key, *value, i| {
            if (i > 0 and std.mem.order(u8, keys[i - 1], key) == .gt) return error.UnsortedBenchmarkKeys;
            self.key_hash +%= std.hash.Wyhash.hash(0, key);
            value.* = &self.raw;
        }
        self.key_count += keys.len;
    }
};

// Reference to the former sorted bounded reader: a key arena per batch and a
// freshly formatted complete metric/generation/node key per logical score.
fn referenceScores(alloc: std.mem.Allocator, txn: *ScoreTxn, names: []const []const u8, nodes: []const []const u8, columns: []const []?f64) !void {
    const rows = try alloc.alloc(usize, nodes.len);
    defer alloc.free(rows);
    for (rows, 0..) |*row, i| row.* = i;
    const Order = struct {
        nodes: []const []const u8,
        fn less(self: @This(), a: usize, b: usize) bool {
            const order = std.mem.order(u8, self.nodes[a], self.nodes[b]);
            return order == .lt or (order == .eq and a < b);
        }
    };
    std.mem.sort(usize, rows, Order{ .nodes = nodes }, Order.less);
    var offset: usize = 0;
    const total = names.len * nodes.len;
    const Pending = struct { column: usize, row: usize, key: []const u8 };
    while (offset < total) {
        const len = @min(4096, total - offset);
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const ka = arena.allocator();
        const pending = try alloc.alloc(Pending, len);
        defer alloc.free(pending);
        for (pending, 0..) |*item, i| {
            const flat = offset + i;
            const column = flat / nodes.len;
            const row = rows[flat % nodes.len];
            var generation_buf: [20]u8 = undefined;
            const generation = try std.fmt.bufPrint(&generation_buf, "{d}", .{@as(u64, 12345)});
            var key = std.ArrayListUnmanaged(u8).empty;
            defer key.deinit(ka);
            try key.appendSlice(ka, "meta:metric:");
            for ([_][]const u8{ names[column], "score", generation, nodes[row] }) |part| try antfly.internal_keys.appendEncodedComponent(&key, ka, part);
            item.* = .{ .column = column, .row = row, .key = try key.toOwnedSlice(ka) };
        }
        const keys = try alloc.alloc([]const u8, len);
        defer alloc.free(keys);
        const values = try alloc.alloc(?[]const u8, len);
        defer alloc.free(values);
        for (pending, keys) |item, *key| key.* = item.key;
        @memset(values, null);
        try txn.getManySorted(keys, values);
        for (pending, values) |item, value| columns[item.column][item.row] = if (value) |raw| @bitCast(std.mem.readInt(u64, raw[0..8], .little)) else null;
        offset += len;
    }
}

const VectorWriteTxn = struct {
    slots: []const [8]u8,
    reads: usize = 0,
    writes: usize = 0,
    sum: f64 = 0,
    pub fn get(self: *@This(), key: []const u8) ![]const u8 {
        self.reads += 1;
        const pos = std.mem.indexOf(u8, key, "node-") orelse return error.NotFound;
        const i = try std.fmt.parseInt(usize, key[pos + 5 ..][0..8], 10);
        return &self.slots[i];
    }
    pub fn getManySorted(self: *@This(), keys: []const []const u8, values: []?[]const u8) !void {
        for (keys, values) |key, *value| value.* = try self.get(key);
    }
    pub fn put(self: *@This(), _: []const u8, bytes: []const u8) !void {
        const chunk = antfly.graph.vector_chunk;
        self.writes += 1;
        for (0..chunk.entries) |i| self.sum += try chunk.get(bytes, i, false);
    }
    pub fn delete(_: *@This(), _: []const u8) anyerror!void {
        return error.NotFound;
    }
};

fn benchmarkVectorWrites(out: anytype) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.smp_allocator);
    defer arena.deinit();
    const fixture = arena.allocator();
    const nodes = try fixture.alloc([]const u8, 20_000);
    const slots = try fixture.alloc([8]u8, nodes.len);
    for (nodes, slots, 0..) |*node, *slot, i| {
        node.* = try std.fmt.allocPrint(fixture, "node-{d:0>8}", .{i});
        std.mem.writeInt(u64, slot, i + 1, .little);
    }
    for ([_]bool{ true, false }) |reference| {
        var times: [5]u64 = undefined;
        var last = PhaseAllocStats{};
        var last_txn = VectorWriteTxn{ .slots = slots };
        for (0..6) |sample| {
            var stats = PhaseAllocStats{};
            var tracking = PhaseTrackingAllocator{ .backing = std.heap.smp_allocator, .stats = &stats };
            var index: antfly.graph.GraphIndex = undefined;
            index.alloc = tracking.allocator();
            var txn = VectorWriteTxn{ .slots = slots };
            const start = antfly.platform_time.monotonicNs();
            try index.benchmarkVectorRowsAlloc(&txn, nodes, reference);
            const elapsed = antfly.platform_time.monotonicNs() - start;
            if (stats.current_bytes != 0 or txn.sum != @as(f64, @floatFromInt(nodes.len)) * 0.5) return error.InvalidBenchmarkResult;
            if (sample != 0) times[sample - 1] = elapsed;
            last = stats;
            last_txn = txn;
        }
        std.mem.sort(u64, &times, {}, std.sort.asc(u64));
        const json = try std.json.Stringify.valueAlloc(fixture, .{
            .mode = if (reference) "vector_write_node_ids_reference" else "vector_write_ordinal_rows",
            .rows = nodes.len,
            .storage_reads = last_txn.reads,
            .storage_writes = last_txn.writes,
            .median_ns = times[2],
            .min_ns = times[0],
            .max_ns = times[4],
            .allocation_count = last.alloc_count,
            .allocated_bytes = last.total_alloc_bytes,
            .peak_bytes = last.peak_bytes,
            .note = "one production vector write; mock storage; all output scores checked; excludes caller fixture and numerical iteration",
        }, .{});
        try out.interface.writeAll(json);
        try out.interface.writeByte('\n');
        try out.flush();
    }
}

fn benchmarkStagedQueries(io: std.Io, out: anytype) !void {
    const alloc = std.heap.smp_allocator;
    const query_mod = antfly.graph_query;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const fixture = arena.allocator();
    const root = try std.fmt.allocPrint(fixture, "/tmp/antfly-metric-staged-bench-{d}", .{antfly.platform_time.monotonicNs()});
    try std.Io.Dir.cwd().createDirPath(io, root);
    defer std.Io.Dir.cwd().deleteTree(io, root) catch {};
    const store_path = try std.fmt.allocPrint(fixture, "{s}/store\x00", .{root});
    const reverse_path = try std.fmt.allocPrint(fixture, "{s}/reverse\x00", .{root});
    var store = try antfly.docstore.DocStore.open(alloc, @ptrCast(store_path.ptr), .{});
    defer store.close();
    var configs: [16]antfly.graph.GraphMetricConfig = undefined;
    var reads: [16]query_mod.GraphMetricRead = undefined;
    var names: [16][]const u8 = undefined;
    for (&configs, &reads, &names, 0..) |*config, *read, *name, i| {
        name.* = try std.fmt.allocPrint(fixture, "metric-{d:0>2}", .{i});
        config.* = .{ .name = name.*, .kind = .degree, .refresh = .manual };
        read.* = .{ .name = name.* };
    }
    var index = try antfly.graph.GraphIndex.open(alloc, &store, @ptrCast(reverse_path.ptr), "graph", .{ .metric_configs = &configs });
    defer index.close();
    const ids = try fixture.alloc([]const u8, 100_000);
    const nodes = try fixture.alloc(query_mod.GraphResultNode, ids.len);
    for (ids, nodes, 0..) |*id, *node, i| {
        id.* = try std.fmt.allocPrint(fixture, "node-{d:0>8}", .{i});
        node.* = .{ .key = id.*, .depth = 0, .distance = 0 };
    }
    try index.benchmarkSeedScoreColumns(&names, ids);
    const query = query_mod.GraphQuery{
        .query_type = .neighbors,
        .index_name = "graph",
        .start_nodes = .{ .keys = &.{} },
        .metrics = &reads,
        .order_by = &.{.{ .name = names[0] }},
        .params = .{ .max_results = 10 },
    };
    const plan = try query_mod.MetricReadPlan.init(query);
    const policies: [16]antfly.graph.GraphIndex.GraphMetricColumnReadPolicy = @splat(.{ .require_published = true });
    for ([_]bool{ true, false }) |reference| {
        var times: [5]u64 = undefined;
        var last = PhaseAllocStats{};
        var keys: usize = 0;
        for (0..6) |sample| {
            var stats = PhaseAllocStats{};
            var tracking = PhaseTrackingAllocator{ .backing = alloc, .stats = &stats };
            const tracked = tracking.allocator();
            const start = antfly.platform_time.monotonicNs();
            {
                var session = try index.openGraphMetricReadSessionAlloc(tracked, &names, &policies);
                defer session.deinit();
                var work = try query_mod.GraphQueryEngine.MetricStageWorkspace.init(tracked, plan, nodes.len);
                defer work.deinit();
                try work.ensure(&session, if (reference) plan.dependencies.slice() else plan.orders.slice(), nodes);
                try work.select(query, true, plan.orders.slice(), plan.projections.slice(), &.{});
                try work.ensure(&session, plan.projections.slice(), nodes);
                keys = session.reads.keys;
                for (work.rows, 0..) |row, i| if (row != nodes.len - i - 1) return error.InvalidBenchmarkResult;
                for (work.columns) |column| for (column.?, 0..) |value, i| {
                    if (value != @as(f64, @floatFromInt(nodes.len - i - 1))) return error.InvalidBenchmarkResult;
                };
            }
            const elapsed = antfly.platform_time.monotonicNs() - start;
            if (stats.current_bytes != 0 or keys != (if (reference) @as(usize, 1_600_000) else 100_150)) return error.InvalidBenchmarkResult;
            if (sample != 0) times[sample - 1] = elapsed;
            last = stats;
        }
        std.mem.sort(u64, &times, {}, std.sort.asc(u64));
        const json = try std.json.Stringify.valueAlloc(fixture, .{
            .mode = if (reference) "stateful_eager_metric_columns" else "stateful_staged_metric_columns",
            .candidates = nodes.len,
            .metrics = names.len,
            .selected = 10,
            .score_keys = keys,
            .median_ns = times[2],
            .min_ns = times[0],
            .max_ns = times[4],
            .peak_bytes = last.peak_bytes,
            .allocation_count = last.alloc_count,
            .note = "real default storage; six warm-cache samples, first discarded; exact selected row and score parity; includes snapshot, score reads, selection and scratch frees; excludes fixture writes, traversal, backend-owned allocations and response encoding",
        }, .{});
        try out.interface.writeAll(json);
        try out.interface.writeByte('\n');
        try out.flush();
    }
}

fn benchmarkQuerySnapshots(io: std.Io, out: anytype) !void {
    const alloc = std.heap.smp_allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const fixture = arena.allocator();
    const root = try std.fmt.allocPrint(fixture, "/tmp/antfly-metric-query-bench-{d}", .{antfly.platform_time.monotonicNs()});
    try std.Io.Dir.cwd().createDirPath(io, root);
    defer std.Io.Dir.cwd().deleteTree(io, root) catch {};
    const store_path = try std.fmt.allocPrint(fixture, "{s}/store\x00", .{root});
    const reverse_path = try std.fmt.allocPrint(fixture, "{s}/reverse\x00", .{root});
    var store = try antfly.docstore.DocStore.open(alloc, @ptrCast(store_path.ptr), .{});
    defer store.close();
    const configs = [_]antfly.graph.GraphMetricConfig{.{ .name = "degree", .kind = .degree, .refresh = .manual }};
    var index = try antfly.graph.GraphIndex.open(alloc, &store, @ptrCast(reverse_path.ptr), "graph", .{ .metric_configs = &configs });
    defer index.close();
    const ids = try fixture.alloc([]const u8, 4096);
    for (ids, 0..) |*id, i| id.* = try std.fmt.allocPrint(fixture, "node-{d:0>8}", .{i});
    const writes = try fixture.alloc(antfly.graph.BatchWrite, ids.len * 4);
    for (writes, 0..) |*write, i| write.* = .{ .source = ids[i / 4], .target = ids[(i / 4 + i % 4 + 1) % ids.len], .edge_type = "follows" };
    try index.batchApply(writes, &.{});
    var published = try index.runGraphMetric("degree");
    defer published.deinit(alloc);
    var started = try index.ensureGraphMetricPlannedBuild("degree", index.edge_generation);
    defer started.deinit(alloc);
    for (0..8) |_| {
        var status = try index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        if (status.phase == .scan_edges_and_out_degree) break;
        _ = try index.runGraphMetricPlannedCoordinatorStepForMetric("degree");
        _ = try index.runGraphMetricPlannedWorkerPageStepForMetric("degree", "benchmark");
    }
    var active = try index.graphMetricStatus("degree");
    defer active.deinit(alloc);
    if (active.phase != .scan_edges_and_out_degree) return error.InvalidBenchmarkResult;
    for ([_]usize{ 1, 64 }) |rows| for ([_]bool{ true, false }) |reference| {
        var times: [21]u64 = undefined;
        var last = PhaseAllocStats{};
        for (0..22) |sample| {
            var stats = PhaseAllocStats{};
            var tracking = PhaseTrackingAllocator{ .backing = alloc, .stats = &stats };
            index.alloc = tracking.allocator();
            defer index.alloc = alloc;
            const start = antfly.platform_time.monotonicNs();
            var result = try index.benchmarkScoreSnapshotAlloc("degree", ids[0..rows], reference);
            const elapsed = antfly.platform_time.monotonicNs() - start;
            for (result.scores) |score| if (score != 8.0) return error.InvalidBenchmarkResult;
            result.deinit(index.alloc);
            if (stats.current_bytes != 0) return error.InvalidBenchmarkResult;
            if (sample != 0) times[sample - 1] = elapsed;
            last = stats;
        }
        std.mem.sort(u64, &times, {}, std.sort.asc(u64));
        const json = try std.json.Stringify.valueAlloc(fixture, .{
            .mode = if (reference) "query_operator_status_reference" else "query_compact_snapshot",
            .rows = rows,
            .nodes = ids.len,
            .edges = writes.len,
            .active_scan_pages = 256,
            .median_ns = times[10],
            .p95_ns = times[19],
            .min_ns = times[0],
            .max_ns = times[20],
            .allocation_count = last.alloc_count,
            .allocated_bytes = last.total_alloc_bytes,
            .peak_bytes = last.peak_bytes,
            .note = "real default storage; active rebuild; includes transaction, metadata and scores; validation and result free outside timer",
        }, .{});
        try out.interface.writeAll(json);
        try out.interface.writeByte('\n');
        try out.flush();
    };
}

fn benchmarkAuthenticatedCache(io: std.Io, out: anytype) !void {
    const alloc = std.heap.smp_allocator;
    const cache_mod = antfly.serverless.query.cache;
    const root = try std.fmt.allocPrint(alloc, "/tmp/antfly-cache-promotion-bench-{d}", .{antfly.platform_time.monotonicNs()});
    defer alloc.free(root);
    defer std.Io.Dir.cwd().deleteTree(io, root) catch {};
    var cache = try cache_mod.QueryCache.init(alloc, root);
    defer cache.deinit();
    const checksum = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const artifact_id = "sha256:" ++ checksum;
    const payload = try alloc.alloc(u8, 64 * 1024);
    defer alloc.free(payload);
    for (payload, 0..) |*byte, i| byte.* = @truncate(i);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(payload, &digest, .{});
    const block_id = "graph-metric-score-0-exact";
    try cache.publishAuthenticatedBlocks(artifact_id, payload.len, checksum, &.{.{ .block_id = block_id, .offset = 0, .contents = payload, .checksum = digest }}, .none);
    for ([_]bool{ false, true }) |warm| {
        var times: [5]u64 = undefined;
        var last = PhaseAllocStats{};
        for (0..6) |sample| {
            var stats = PhaseAllocStats{};
            var tracking = PhaseTrackingAllocator{ .backing = alloc, .stats = &stats };
            var elapsed: u64 = 0;
            for (0..64) |_| {
                if (!warm) {
                    cache.graph_metric_blocks.deinit();
                    cache.graph_metric_blocks = .{};
                }
                const start = antfly.platform_time.monotonicNs();
                var hit = (try cache.readAuthenticatedBlockIfPresentLease(tracking.allocator(), artifact_id, block_id, payload.len, checksum, &digest, 0, payload.len, .none)).?;
                elapsed += antfly.platform_time.monotonicNs() - start;
                if (!std.mem.eql(u8, hit.bytes(), payload)) return error.InvalidBenchmarkResult;
                hit.deinit();
            }
            if (stats.current_bytes != 0) return error.InvalidBenchmarkResult;
            if (sample != 0) times[sample - 1] = elapsed;
            last = stats;
        }
        std.mem.sort(u64, &times, {}, std.sort.asc(u64));
        const json = try std.json.Stringify.valueAlloc(alloc, .{
            .mode = if (warm) "authenticated_warm_memory_lease" else "authenticated_cold_memory_disk_hit",
            .lookups = 64,
            .block_bytes = payload.len,
            .median_ns = times[2],
            .min_ns = times[0],
            .max_ns = times[4],
            .allocation_count = last.alloc_count,
            .allocated_bytes = last.total_alloc_bytes,
            .peak_bytes = last.peak_bytes,
            .note = "warm disk in both cases; cold memory includes authentication and promotion; reset excluded; request payload allocations only, cache-owned allocations excluded; no network",
        }, .{});
        defer alloc.free(json);
        try out.interface.writeAll(json);
        try out.interface.writeByte('\n');
        try out.flush();
    }
}

fn benchmarkTopOwnership(out: anytype) !void {
    const alloc = std.heap.smp_allocator;
    const reader = antfly.serverless.query.graph_metric_reader;
    for ([_]bool{ true, false }) |reference| {
        var times: [5]u64 = undefined;
        var last = PhaseAllocStats{};
        for (0..6) |sample| {
            var stats = PhaseAllocStats{};
            var tracking = PhaseTrackingAllocator{ .backing = alloc, .stats = &stats };
            const a = tracking.allocator();
            const scores = try a.alloc(reader.Score, 10_000);
            for (scores) |*score| {
                const node = try a.alloc(u8, 4096);
                @memset(node, 'x');
                score.* = .{ .node_id = node, .value = 1 };
            }
            var result = reader.Result{ .scores = scores, .config_fingerprint = 1, .converged = true, .iterations_completed = 1, .delta = 0, .edge_filter = .{}, .metadata_version = 9, .published_generation = 1, .edge_generation = 1, .computed_at_ms = 1 };
            const resident = stats.current_bytes;
            stats = .{ .current_bytes = resident, .peak_bytes = resident };
            var session = antfly.serverless.query.QuerySession{ .alloc = a, .artifacts = undefined, .manifest = undefined };
            const start = antfly.platform_time.monotonicNs();
            const output: []reader.PublicScore = if (reference) blk: {
                const cloned = try a.alloc(reader.PublicScore, scores.len);
                for (scores, cloned) |score, *copy| copy.* = .{ .node = try a.dupe(u8, score.node_id), .score = score.value };
                break :blk cloned;
            } else try result.takePublicScoresAlloc(a, &session);
            const elapsed = antfly.platform_time.monotonicNs() - start;
            if (output.len != 10_000 or output[0].node.len != 4096 or output[0].score != 1) return error.InvalidBenchmarkResult;
            last = stats;
            for (output) |*score| score.deinit(a);
            a.free(output);
            result.deinit(a);
            if (stats.current_bytes != 0) return error.InvalidBenchmarkResult;
            if (sample != 0) times[sample - 1] = elapsed;
        }
        std.mem.sort(u64, &times, {}, std.sort.asc(u64));
        const json = try std.json.Stringify.valueAlloc(alloc, .{
            .mode = if (reference) "top_public_response_copy_reference" else "top_public_response_ownership_transfer",
            .nodes = 10_000,
            .node_id_bytes = 4096,
            .median_ns = times[2],
            .min_ns = times[0],
            .max_ns = times[4],
            .allocation_count = last.alloc_count,
            .allocated_bytes = last.total_alloc_bytes,
            .peak_bytes = last.peak_bytes,
            .note = "conversion only; peak includes retained input; input construction, result destruction, fetch and JSON encoding excluded",
        }, .{});
        defer alloc.free(json);
        try out.interface.writeAll(json);
        try out.interface.writeByte('\n');
        try out.flush();
    }
}

fn benchmarkMembership(io: std.Io, out: anytype) !void {
    const alloc = std.heap.smp_allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const fixture = arena.allocator();
    const root = try std.fmt.allocPrint(fixture, "/tmp/antfly-membership-bench-{d}", .{antfly.platform_time.monotonicNs()});
    try std.Io.Dir.cwd().createDirPath(io, root);
    defer std.Io.Dir.cwd().deleteTree(io, root) catch {};
    const store_path = try std.fmt.allocPrint(fixture, "{s}/store\x00", .{root});
    const reverse_path = try std.fmt.allocPrint(fixture, "{s}/reverse\x00", .{root});
    var store = try antfly.docstore.DocStore.open(alloc, @ptrCast(store_path.ptr), .{});
    defer store.close();
    var index = try antfly.graph.GraphIndex.open(alloc, &store, @ptrCast(reverse_path.ptr), "graph", .{});
    defer index.close();
    const ids = try fixture.alloc([]const u8, 64);
    for (ids, 0..) |*id, i| id.* = try std.fmt.allocPrint(fixture, "node-{d:0>8}", .{i});
    try index.benchmarkMembershipFixture(ids);
    for ([_]bool{ true, false }) |reference| {
        var times: [5]u64 = undefined;
        var last = PhaseAllocStats{};
        for (0..6) |sample| {
            var stats = PhaseAllocStats{};
            var tracking = PhaseTrackingAllocator{ .backing = alloc, .stats = &stats };
            index.alloc = tracking.allocator();
            defer index.alloc = alloc;
            const start = antfly.platform_time.monotonicNs();
            const count = try index.benchmarkMembershipRead(reference);
            const elapsed = antfly.platform_time.monotonicNs() - start;
            if (count != ids.len or stats.current_bytes != 0) return error.InvalidBenchmarkResult;
            if (sample != 0) times[sample - 1] = elapsed;
            last = stats;
        }
        std.mem.sort(u64, &times, {}, std.sort.asc(u64));
        const json = try std.json.Stringify.valueAlloc(fixture, .{
            .mode = if (reference) "membership_partials_reference" else "membership_sealed_blocks",
            .nodes = ids.len,
            .producer_partials = ids.len * 256,
            .median_ns = times[2],
            .min_ns = times[0],
            .max_ns = times[4],
            .allocation_count = last.alloc_count,
            .allocated_bytes = last.total_alloc_bytes,
            .peak_bytes = last.peak_bytes,
            .note = "real default storage; includes transaction, canonical membership and dictionary validation; excludes fixture writes and numeric fold",
        }, .{});
        try out.interface.writeAll(json);
        try out.interface.writeByte('\n');
        try out.flush();
    }
}

fn benchmarkPublication(io: std.Io, out: anytype) !void {
    const alloc = std.heap.smp_allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const fixture = arena.allocator();
    const scores = try fixture.alloc(antfly.graph.GraphIndex.GraphMetricScore, 8192);
    for (scores, 0..) |*score, i| score.* = .{ .node = try std.fmt.allocPrint(fixture, "node-{d:0>8}", .{i}), .score = @as(f64, @floatFromInt(i)) / 8192 };
    for ([_]usize{ 64, 4096 }) |limit| {
        var times: [5]u64 = undefined;
        var commits: usize = 0;
        for (0..6) |sample| {
            const root = try std.fmt.allocPrint(fixture, "/tmp/antfly-publication-bench-{d}", .{antfly.platform_time.monotonicNs()});
            try std.Io.Dir.cwd().createDirPath(io, root);
            defer std.Io.Dir.cwd().deleteTree(io, root) catch {};
            const path = try std.fmt.allocPrintSentinel(fixture, "{s}/reverse", .{root}, 0);
            var index = try antfly.graph.GraphIndex.open(alloc, {}, path, "graph", .{});
            defer index.close();
            const start = antfly.platform_time.monotonicNs();
            commits = try index.benchmarkScorePublication(scores, limit, 1);
            const elapsed = antfly.platform_time.monotonicNs() - start;
            if (commits != scores.len / limit) return error.InvalidBenchmarkResult;
            if (sample != 0) times[sample - 1] = elapsed;
        }
        std.mem.sort(u64, &times, {}, std.sort.asc(u64));
        const json = try std.json.Stringify.valueAlloc(fixture, .{
            .mode = if (limit == 64) "publication_64_node_reference" else "publication_bounded_4096_nodes",
            .nodes = scores.len,
            .checkpoint_commits = commits,
            .median_ns = times[2],
            .min_ns = times[0],
            .max_ns = times[4],
            .note = "real default storage; atomic score/staging/cursor commits and full primary-score validation; excludes graph computation, page fencing, and final top-k merge",
        }, .{});
        try out.interface.writeAll(json);
        try out.interface.writeByte('\n');
        try out.flush();
    }
}

fn benchmarkRoutingWorkingSet(out: anytype) !void {
    const routing = antfly.serverless.query.graph_metric_routing_cache;
    const alloc = std.heap.smp_allocator;
    for ([_]bool{ true, false }) |reference| {
        var times: [5]u64 = undefined;
        var last = PhaseAllocStats{};
        var fills: usize = 0;
        for (0..6) |sample| {
            var stats = PhaseAllocStats{};
            var tracking = PhaseTrackingAllocator{ .backing = alloc, .stats = &stats };
            const tracked = tracking.allocator();
            var cache = routing.Cache{};
            const entry_bytes = @sizeOf(routing.Entry) + 4096;
            const budget: usize = if (reference) entry_bytes * 64 else 1024 * 1024;
            fills = 0;
            const start = antfly.platform_time.monotonicNs();
            for (0..100) |_| {
                for (0..80) |i| {
                    var key: [32]u8 = undefined;
                    std.crypto.hash.sha2.Sha256.hash(std.mem.asBytes(&i), &key, .{});
                    var lease = cache.acquire(key) orelse blk: {
                        const entry = try tracked.create(routing.Entry);
                        entry.* = .{ .key = key, .alloc = tracked, .footer = try tracked.alloc(u8, 4096), .routing = .{ .entries = &.{}, .ranked_entries = &.{}, .footer_offset = 1, .top_score_count = 0 } };
                        @memset(entry.footer, @intCast(i));
                        fills += 1;
                        break :blk cache.adopt(entry, budget);
                    };
                    if (lease.entry.footer[0] != i) return error.InvalidBenchmarkResult;
                    lease.deinit();
                }
            }
            cache.deinit();
            const elapsed = antfly.platform_time.monotonicNs() - start;
            if (fills != (if (reference) @as(usize, 8000) else 80) or stats.current_bytes != 0) return error.InvalidBenchmarkResult;
            if (sample != 0) times[sample - 1] = elapsed;
            last = stats;
        }
        std.mem.sort(u64, &times, {}, std.sort.asc(u64));
        const json = try std.json.Stringify.valueAlloc(alloc, .{
            .mode = if (reference) "routing_64_entry_capacity_model" else "routing_byte_admission",
            .queries = 100,
            .entries_per_query = 80,
            .fills = fills,
            .median_ns = times[2],
            .min_ns = times[0],
            .max_ns = times[4],
            .allocation_count = last.alloc_count,
            .allocated_bytes = last.total_alloc_bytes,
            .peak_bytes = last.peak_bytes,
            .note = "production cache; equal 4 KiB entries model former 64-slot capacity with a byte limit; sequential released leases; excludes codec decoding, object I/O, and query execution",
        }, .{});
        defer alloc.free(json);
        try out.interface.writeAll(json);
        try out.interface.writeByte('\n');
        try out.flush();
    }
}

fn benchmarkOrdinalFold(out: anytype) !void {
    const alloc = std.heap.smp_allocator;
    const tiles = 4096;
    var expected: ?f64 = null;
    for ([_]bool{ true, false }) |reference| {
        var times: [5]u64 = undefined;
        var last = PhaseAllocStats{};
        for (0..6) |sample| {
            var stats = PhaseAllocStats{};
            var tracking = PhaseTrackingAllocator{ .backing = alloc, .stats = &stats };
            const start = antfly.platform_time.monotonicNs();
            const sum = try antfly.graph.GraphIndex.benchmarkOrdinalFold(tracking.allocator(), reference, tiles);
            const elapsed = antfly.platform_time.monotonicNs() - start;
            if (expected) |value| {
                if (sum != value) return error.InvalidBenchmarkResult;
            } else expected = sum;
            if (stats.current_bytes != 0) return error.InvalidBenchmarkResult;
            if (sample != 0) times[sample - 1] = elapsed;
            last = stats;
        }
        std.mem.sort(u64, &times, {}, std.sort.asc(u64));
        const json = try std.json.Stringify.valueAlloc(alloc, .{
            .mode = if (reference) "ordinal_fold_owned_reference" else "ordinal_fold_borrowed_scratch",
            .tiles = tiles,
            .edge_visits = tiles * 256,
            .median_ns = times[2],
            .min_ns = times[0],
            .max_ns = times[4],
            .allocation_count = last.alloc_count,
            .allocated_bytes = last.total_alloc_bytes,
            .peak_bytes = last.peak_bytes,
            .sum = expected.?,
            .note = "warm vector cache; validates and folds the same tile repeatedly; includes constant fixture setup in time but excludes fixture allocations; no storage I/O or checkpoint commit",
        }, .{});
        defer alloc.free(json);
        try out.interface.writeAll(json);
        try out.interface.writeByte('\n');
        try out.flush();
    }
}

fn benchmarkSealedVectors(io: std.Io, out: anytype) !void {
    const alloc = std.heap.smp_allocator;
    const root = try std.fmt.allocPrint(alloc, "/tmp/antfly-sealed-vector-bench-{d}", .{antfly.platform_time.monotonicNs()});
    defer alloc.free(root);
    defer std.Io.Dir.cwd().deleteTree(io, root) catch {};
    const primary = try std.fmt.allocPrintSentinel(alloc, "{s}/primary", .{root}, 0);
    defer alloc.free(primary);
    const reverse = try std.fmt.allocPrintSentinel(alloc, "{s}/reverse", .{root}, 0);
    defer alloc.free(reverse);
    var store = try antfly.docstore.DocStore.open(alloc, primary, .{});
    defer store.close();
    var index = try antfly.graph.GraphIndex.open(alloc, &store, reverse, "links", .{});
    defer index.close();
    try index.prepareSealedVectorBenchmark();
    for ([_]bool{ true, false }) |reference| {
        var times: [5]u64 = undefined;
        var last = PhaseAllocStats{};
        var reads: usize = 0;
        for (0..6) |sample| {
            var stats = PhaseAllocStats{};
            var tracking = PhaseTrackingAllocator{ .backing = alloc, .stats = &stats };
            index.alloc = tracking.allocator();
            defer index.alloc = alloc;
            const start = antfly.platform_time.monotonicNs();
            reads = try index.benchmarkSealedVectorGather(256, reference);
            const elapsed = antfly.platform_time.monotonicNs() - start;
            if (stats.current_bytes != 0) return error.InvalidBenchmarkResult;
            if (sample != 0) times[sample - 1] = elapsed;
            last = stats;
        }
        std.mem.sort(u64, &times, {}, std.sort.asc(u64));
        const json = try std.json.Stringify.valueAlloc(alloc, .{
            .mode = if (reference) "vector_checkpoint_local_reference" else "vector_sealed_cross_checkpoint",
            .nodes = 32768,
            .gathers = 256 * 2048,
            .checkpoints = 256,
            .storage_chunks = reads,
            .median_ns = times[2],
            .min_ns = times[0],
            .max_ns = times[4],
            .allocation_count = last.alloc_count,
            .allocated_bytes = last.total_alloc_bytes,
            .peak_bytes = last.peak_bytes,
            .note = "real default storage; uniform-source gathers and new read transactions; sealed cache starts empty; excludes fixture writes and fold/checkpoint commits; tracking excludes backend-owned allocations",
        }, .{});
        defer alloc.free(json);
        try out.interface.writeAll(json);
        try out.interface.writeByte('\n');
        try out.flush();
    }
}

fn benchmarkCandidatePlanning(out: anytype) !void {
    for ([_]bool{ true, false }) |common_prefix| try benchmarkCandidatePlanningIds(out, common_prefix);
}

fn benchmarkCandidatePlanningIds(out: anytype, common_prefix: bool) !void {
    const alloc = std.heap.smp_allocator;
    const reader = antfly.serverless.query.graph_metric_reader;
    const codec = antfly.serverless.graph_metric_segment.codec;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const fixture = arena.allocator();
    const count = 100_000;
    const canonical = try fixture.alloc([]const u8, count);
    for (canonical, 0..) |*id, i| id.* = if (common_prefix)
        try std.fmt.allocPrint(fixture, "graph/customer-record-{d:0>8}", .{i})
    else
        try std.fmt.allocPrint(fixture, "{x:0>16}", .{std.hash.Wyhash.hash(0, std.mem.asBytes(&i))});
    std.mem.sort([]const u8, canonical, {}, struct {
        fn less(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.order(u8, a, b) == .lt;
        }
    }.less);
    const ids = try fixture.alloc([]const u8, count);
    for (ids, 0..) |*id, i| id.* = canonical[(i * 7919) % count];
    const entries = try fixture.alloc(codec.RoutingEntry, (count + 255) / 256);
    for (entries, 0..) |*entry, i| entry.* = .{
        .block_index = i,
        .first_node_id = canonical[i * 256],
        .offset = i * 4096,
        .len = 4096,
    };
    const routing = codec.RoutingIndex{ .entries = entries, .top_score_count = 0, .ranked_entries = &.{}, .footer_offset = entries.len * 4096 };
    for ([_]usize{ 1, 16 }) |columns| {
        var expected: ?u64 = null;
        for ([_]bool{ true, false }) |reference| {
            var times: [5]u64 = undefined;
            var last = PhaseAllocStats{};
            for (0..6) |sample| {
                var stats = PhaseAllocStats{};
                var tracking = PhaseTrackingAllocator{ .backing = alloc, .stats = &stats };
                var session = antfly.serverless.query.QuerySession{ .alloc = tracking.allocator(), .artifacts = undefined, .manifest = undefined };
                const start = antfly.platform_time.monotonicNs();
                const sum = try reader.benchmarkCandidatePlanningAlloc(tracking.allocator(), &session, ids, routing, columns, reference);
                const elapsed = antfly.platform_time.monotonicNs() - start;
                if (expected) |value| {
                    if (sum != value) return error.InvalidBenchmarkResult;
                } else expected = sum;
                if (stats.current_bytes != 0) return error.InvalidBenchmarkResult;
                if (sample != 0) times[sample - 1] = elapsed;
                last = stats;
            }
            std.mem.sort(u64, &times, {}, std.sort.asc(u64));
            const json = try std.json.Stringify.valueAlloc(fixture, .{
                .mode = if (reference) "point_row_maps_reference" else "point_shared_candidate_order",
                .rows = count,
                .columns = columns,
                .blocks = entries.len,
                .id_shape = if (common_prefix) "common_prefix" else "hashed_hex",
                .node_id_bytes = ids[0].len,
                .median_ns = times[2],
                .min_ns = times[0],
                .max_ns = times[4],
                .allocation_count = last.alloc_count,
                .allocated_bytes = last.total_alloc_bytes,
                .peak_bytes = last.peak_bytes,
                .checksum = expected.?,
                .note = "row mapping only; permuted IDs; all legacy column maps retained; excludes output, routing/control ownership, span materialization, fetch and score decoding",
            }, .{});
            try out.interface.writeAll(json);
            try out.interface.writeByte('\n');
            try out.flush();
        }
    }
}

fn benchmarkStateful(out: anytype) !void {
    for ([_]bool{ false, true }) |duplicates| {
        var arena = std.heap.ArenaAllocator.init(std.heap.smp_allocator);
        defer arena.deinit();
        const fixture = arena.allocator();
        const count = 20_000;
        const nodes = try fixture.alloc([]const u8, count);
        for (nodes, 0..) |*node, i| node.* = try std.fmt.allocPrint(fixture, "snapshot/customer-record-{d:0>8}", .{(count - i - 1) / @as(usize, if (duplicates) 2 else 1)});
        const names: []const []const u8 = &.{ "a-rank", "b-rank", "c-rank", "d-rank" };
        var prefixes: [4]?[]const u8 = undefined;
        for (names, &prefixes) |name, *prefix| {
            var key = std.ArrayListUnmanaged(u8).empty;
            try key.appendSlice(fixture, "meta:metric:");
            for ([_][]const u8{ name, "score", "12345" }) |part| try antfly.internal_keys.appendEncodedComponent(&key, fixture, part);
            prefix.* = try key.toOwnedSlice(fixture);
        }
        var columns: [4][]?f64 = undefined;
        for (&columns) |*column| column.* = try fixture.alloc(?f64, count);
        for ([_]bool{ true, false }) |reference| {
            var times: [5]u64 = undefined;
            var last = PhaseAllocStats{};
            var last_txn = ScoreTxn{};
            // One warmup plus five measured runs. Storage is a synchronous
            // in-memory sink; all output cells are verified outside timing.
            for (0..6) |sample| {
                var stats = PhaseAllocStats{};
                var tracking = PhaseTrackingAllocator{ .backing = std.heap.smp_allocator, .stats = &stats };
                var txn = ScoreTxn{};
                const start = antfly.platform_time.monotonicNs();
                if (reference) try referenceScores(tracking.allocator(), &txn, names, nodes, &columns) else _ = try antfly.graph.score_read.populate(tracking.allocator(), &txn, &prefixes, nodes, &columns);
                const elapsed = antfly.platform_time.monotonicNs() - start;
                if (stats.current_bytes != 0 or txn.key_hash == 0) return error.InvalidBenchmarkResult;
                for (columns) |column| for (column) |score| if (score != 1.0) return error.InvalidBenchmarkResult;
                if (sample != 0) times[sample - 1] = elapsed;
                last = stats;
                last_txn = txn;
            }
            std.mem.sort(u64, &times, {}, std.sort.asc(u64));
            const json = try std.json.Stringify.valueAlloc(fixture, .{
                .mode = if (reference) "stateful_arena_reference" else "stateful_physical_slab",
                .rows = count,
                .columns = names.len,
                .duplicates = duplicates,
                .storage_keys = last_txn.key_count,
                .median_ns = times[2],
                .min_ns = times[0],
                .max_ns = times[4],
                .allocation_count = last.alloc_count,
                .allocated_bytes = last.total_alloc_bytes,
                .peak_bytes = last.peak_bytes,
                .samples = times.len,
                .note = "reader only; mock storage; excludes output arrays and input fixture; one warmup",
            }, .{});
            try out.interface.writeAll(json);
            try out.interface.writeByte('\n');
            try out.flush();
        }
    }
}
