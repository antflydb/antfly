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

pub fn main(init: std.process.Init) !void {
    var output_buf: [4096]u8 = undefined;
    var output = std.Io.File.stdout().writer(init.io, &output_buf);
    try benchmarkStateful(&output);
    try benchmarkVectorWrites(&output);
    try benchmarkQuerySnapshots(init.io, &output);
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
