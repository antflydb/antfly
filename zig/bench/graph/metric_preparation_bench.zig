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

fn benchmarkScoreJoin(output: anytype) !void {
    const codec = antfly.serverless.graph_metric_segment.codec;
    var block = codec.DecodedScoreBlock{ .len = 1024, .node_prefix = "collection/" };
    var suffixes: [1024][8]u8 = undefined;
    var names: [1024][19]u8 = undefined;
    var ids: [1024][]const u8 = undefined;
    var rows: [1024]u32 = undefined;
    var values: [1024]?f64 = @splat(null);
    for (&suffixes, &names, &ids, &rows, 0..) |*suffix, *name, *id, *row, i| {
        const text = try std.fmt.bufPrint(suffix, "{d:0>8}", .{i});
        id.* = try std.fmt.bufPrint(name, "collection/{s}", .{text});
        row.* = @intCast(i);
        block.scores[i] = .{ .node_suffix = text, .value = @floatFromInt(i) };
    }
    for ([_]usize{ 1, 16, 256, 1024 }) |count| {
        for (0..count) |i| rows[i] = @intCast(i * 1024 / count);
        for ([_]bool{ true, false }) |reference| {
            var times: [5]u64 = undefined;
            for (0..6) |sample| {
                const start = antfly.platform_time.monotonicNs();
                for (0..4096) |_| {
                    if (reference) {
                        for (rows[0..count]) |row| values[row] = block.score(ids[row]);
                    } else try block.populateSorted(&ids, rows[0..count], &values, .none);
                    std.mem.doNotOptimizeAway(&values);
                }
                const elapsed = (antfly.platform_time.monotonicNs() - start) / 4096;
                for (rows[0..count]) |row| if (values[row] != @as(f64, @floatFromInt(row))) return error.InvalidBenchmarkResult;
                if (sample != 0) times[sample - 1] = elapsed;
            }
            std.mem.sort(u64, &times, {}, std.sort.asc(u64));
            const json = try std.json.Stringify.valueAlloc(std.heap.smp_allocator, .{
                .mode = if (reference) "score_join_binary_reference" else "score_join_adaptive",
                .rows = count,
                .block_rows = 1024,
                .median_ns = times[2],
                .min_ns = times[0],
                .max_ns = times[4],
                .note = "borrowed decoded block; excludes decode and I/O; six samples, first discarded; 4096 repetitions",
            }, .{});
            defer std.heap.smp_allocator.free(json);
            try output.interface.writeAll(json);
            try output.interface.writeByte('\n');
            try output.flush();
        }
    }
}

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
    var topology_only = false;
    var score_join_only = false;
    var ordinal_cursors_only = false;
    var indexing_only = false;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--paged-only")) return @import("paged_read_bench.zig").run(init.io, &output);
        if (std.mem.eql(u8, arg, "--prune-only")) return benchmarkRangePrune(init.io, &output);
        if (std.mem.eql(u8, arg, "--ownership-reads-only")) return @import("ownership_read_bench.zig").run(init.io, &output);
        if (std.mem.eql(u8, arg, "--filtered-prefix-only")) return @import("paged_read_bench.zig").runFilteredPrefix(init.io, &output);
        if (std.mem.eql(u8, arg, "--native-scans-only")) return benchmarkNativeScans(init.io, &output);
        if (std.mem.eql(u8, arg, "--presence-only")) return benchmarkPresence(&output);
        if (std.mem.eql(u8, arg, "--tree-only")) return benchmarkTreeValidation(init.io, &output);
        if (std.mem.eql(u8, arg, "--indexing-only")) {
            indexing_only = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ordinal-cursors-only")) ordinal_cursors_only = true else if (std.mem.eql(u8, arg, "--staged-only")) staged_only = true else if (std.mem.eql(u8, arg, "--topology-only")) topology_only = true else if (std.mem.eql(u8, arg, "--score-join-only")) score_join_only = true else return error.InvalidArgument;
    }
    if (indexing_only) {
        try benchmarkGraphIndexConstruction(&output);
        try benchmarkTypedEdgeScans(init.io, &output);
        try benchmarkCommittedCounters(init.io, &output);
        try benchmarkSelectedTopologyReads(init.io, &output);
        return benchmarkSemanticMetricReuse(init.io, &output);
    }
    if (score_join_only) return benchmarkScoreJoin(&output);
    if (ordinal_cursors_only) return benchmarkOrdinalCursors(init.io, &output);
    if (topology_only) return benchmarkSharedTopology(init.io, &output);
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
    try benchmarkScoreJoin(&output);
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
                .note = "same current-wire input; excludes fetch, encoding and numeric kernel; one warmup",
            }, .{});
            try output.interface.writeAll(json);
            try output.interface.writeByte('\n');
            try output.flush();
        }
    }
}

fn benchmarkGraphIndexConstruction(out: anytype) !void {
    const alloc = std.heap.smp_allocator;
    const builder = antfly.serverless.build.builder;
    for ([_]usize{ 16, 256 }) |id_len| {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const fixture = arena.allocator();
        const ids = try fixture.alloc([]u8, 1024);
        for (ids, 0..) |*id, i| {
            id.* = try fixture.alloc(u8, id_len);
            @memset(id.*, 'x');
            _ = try std.fmt.bufPrint(id.*[id_len - 8 ..], "{d:0>8}", .{i});
        }
        const docs = try fixture.alloc(antfly.serverless.query.QueryMaterializedDocument, ids.len);
        const InputEdge = struct { target: []const u8, edge_type: []const u8 = "link" };
        var edges: [64]InputEdge = undefined;
        for (docs, 0..) |*doc, i| {
            for (&edges, 0..) |*edge, j| edge.* = .{ .target = ids[(i + j + 1) % ids.len] };
            doc.* = .{ .doc_id = ids[i], .body = try std.json.Stringify.valueAlloc(fixture, .{ .graph_edges = &edges }, .{}), .last_lsn = 0, .last_timestamp_ns = 0 };
        }
        var expected: ?[32]u8 = null;
        for ([_]bool{ true, false }) |reference| {
            var samples: [5]u64 = undefined;
            var measured: PhaseAllocStats = undefined;
            var payload_len: usize = 0;
            for (0..6) |sample| {
                var stats = PhaseAllocStats{};
                var tracking = PhaseTrackingAllocator{ .backing = alloc, .stats = &stats };
                const tracked = tracking.allocator();
                const started = antfly.platform_time.monotonicNs();
                const payload = if (reference)
                    (try builder.benchmarkReferenceGraphSegmentAlloc(tracked, "docs", docs, true)).payload orelse return error.InvalidBenchmarkResult
                else
                    (try builder.buildGraphSegmentAlloc(tracked, "docs", docs, true)).payload orelse return error.InvalidBenchmarkResult;
                const elapsed = antfly.platform_time.monotonicNs() - started;
                payload_len = payload.len;
                var checksum: [32]u8 = undefined;
                std.crypto.hash.sha2.Sha256.hash(payload, &checksum, .{});
                if (expected) |prior| {
                    if (!std.mem.eql(u8, &prior, &checksum)) return error.InvalidBenchmarkResult;
                } else expected = checksum;
                tracked.free(payload);
                if (stats.current_bytes != 0) return error.InvalidBenchmarkResult;
                if (sample != 0) samples[sample - 1] = elapsed;
                measured = stats;
            }
            std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
            const json = try std.json.Stringify.valueAlloc(fixture, .{
                .mode = if (reference) "graph_index_string_reference" else "graph_index_ordinal_builder",
                .nodes = ids.len,
                .edges = ids.len * edges.len,
                .id_bytes = id_len,
                .median_ns = samples[2],
                .peak_bytes = measured.peak_bytes,
                .allocation_count = measured.alloc_count,
                .payload_bytes = payload_len,
                .note = "same JSON input; exact encoded SHA256 parity; input residency excluded; six samples, first discarded; parse, construction and encoding included",
            }, .{});
            try out.interface.writeAll(json);
            try out.interface.writeByte('\n');
            try out.flush();
        }
    }
}

fn benchmarkTreeValidation(io: std.Io, out: anytype) !void {
    const alloc = std.heap.smp_allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const fixture = arena.allocator();
    const root = try std.fmt.allocPrint(fixture, "/tmp/antfly-tree-validation-bench-{d}", .{antfly.platform_time.monotonicNs()});
    try std.Io.Dir.cwd().createDirPath(io, root);
    defer std.Io.Dir.cwd().deleteTree(io, root) catch {};
    const store_path = try std.fmt.allocPrint(fixture, "{s}/store\x00", .{root});
    const reverse_path = try std.fmt.allocPrint(fixture, "{s}/reverse\x00", .{root});
    var store = try antfly.docstore.DocStore.open(alloc, @ptrCast(store_path.ptr), .{});
    defer store.close();
    var index = try antfly.graph.GraphIndex.open(alloc, &store, @ptrCast(reverse_path.ptr), "links", .{ .edge_type_configs = &.{.{ .name = "parent", .topology = .tree }} });
    defer index.close();
    for ([_]usize{ 2048, 8192, 16384 }) |count| {
        const writes = try fixture.alloc(antfly.graph.BatchWrite, count);
        for (writes, 0..) |*write, i| write.* = .{ .source = try std.fmt.allocPrint(fixture, "child-{d:0>8}", .{i}), .target = "parent", .edge_type = "parent" };
        for ([_]bool{ true, false }) |reference| {
            var times: [5]u64 = undefined;
            for (0..6) |sample| {
                const start = antfly.platform_time.monotonicNs();
                try index.benchmarkTreeBatchValidation(writes, reference);
                if (sample != 0) times[sample - 1] = antfly.platform_time.monotonicNs() - start;
            }
            std.mem.sort(u64, &times, {}, std.sort.asc(u64));
            const json = try std.json.Stringify.valueAlloc(fixture, .{ .mode = if (reference) "tree_prior_write_walk" else "tree_grouped_validation", .writes = count, .median_ns = times[2], .note = "read-only production validation against an empty native graph; identical valid distinct-source writes; excludes commit and fixture setup" }, .{});
            try out.interface.writeAll(json);
            try out.interface.writeByte('\n');
            try out.flush();
        }
    }
}

fn benchmarkNativeScans(io: std.Io, out: anytype) !void {
    const alloc = std.heap.smp_allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const root = try std.fmt.allocPrint(a, "/tmp/antfly-native-scan-bench-{d}", .{antfly.platform_time.monotonicNs()});
    try std.Io.Dir.cwd().createDirPath(io, root);
    defer std.Io.Dir.cwd().deleteTree(io, root) catch {};
    const forward = try std.fmt.allocPrintSentinel(a, "{s}/forward", .{root}, 0);
    const reverse = try std.fmt.allocPrintSentinel(a, "{s}/reverse", .{root}, 0);
    var index = try antfly.graph.GraphIndex.openWithPrivateStores(alloc, forward, reverse, "g", .{});
    defer index.close();
    const count = 65536;
    const writes = try a.alloc(antfly.graph.BatchWrite, count);
    for (writes, 0..) |*write, i| write.* = .{ .source = "hub", .target = try std.fmt.allocPrint(a, "node-{d:0>8}", .{i}), .edge_type = "link" };
    for (0..count / 4096) |i| try index.batchApply(writes[i * 4096 ..][0..4096], &.{});
    // Both paths use the same decoder, batch size and durable LSM fixture.
    // Also check that retaining the cursor doesn't sacrifice prefix stopping.
    for ([_]usize{ count, 1 }) |demand| {
        for ([_]bool{ false, true, false, true }) |retained| {
            var samples: [5]u64 = undefined;
            var last = PhaseAllocStats{};
            for (0..6) |sample| {
                var stats = PhaseAllocStats{};
                var tracking = PhaseTrackingAllocator{ .backing = alloc, .stats = &stats };
                const query_alloc = tracking.allocator();
                const start = std.Io.Clock.awake.now(io);
                var seen: usize = 0;
                if (retained) {
                    var scan = index.nativeEdgeScan("hub", &.{"link"}, .out);
                    defer scan.deinit(query_alloc);
                    while (seen < demand) {
                        const edges = (try scan.nextPage(query_alloc, @min(64, demand - seen), 256 * 1024)) orelse break;
                        seen += edges.len;
                        antfly.graph.GraphIndex.freeEdges(query_alloc, edges);
                    }
                } else {
                    var cursor: ?antfly.graph.EdgeScanCursor = null;
                    defer if (cursor) |*value| value.deinit(query_alloc);
                    while (seen < demand) {
                        var page = try index.getEdgesByTypesPage(query_alloc, "hub", &.{"link"}, .out, cursor, .{ .max_edges = @min(64, demand - seen), .max_owned_bytes = 256 * 1024 });
                        if (cursor) |*value| value.deinit(query_alloc);
                        cursor = page.next_cursor;
                        page.next_cursor = null;
                        seen += page.edges.len;
                        page.deinit(query_alloc);
                        if (cursor == null) break;
                    }
                }
                if (seen != demand or stats.current_bytes != 0) return error.InvalidBenchmarkResult;
                if (sample != 0) samples[sample - 1] = @intCast(start.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds());
                last = stats;
            }
            std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
            const json = try std.json.Stringify.valueAlloc(a, .{ .mode = if (retained) "native_retained_cursor" else "native_logical_pages", .edges = count, .demand = demand, .batch_records = 64, .median_ns = samples[2], .query_peak_bytes = last.peak_bytes, .query_alloc_count = last.alloc_count, .note = "warm default durable LSM, includes scan/result cleanup; excludes setup; query allocation stats exclude storage-owned snapshot/cursor allocations" }, .{});
            try out.interface.writeAll(json);
            try out.interface.writeByte('\n');
            try out.flush();
        }
    }
}

fn benchmarkRangePrune(io: std.Io, out: anytype) !void {
    const alloc = std.heap.smp_allocator;
    for ([_]usize{ 4096, 16384, 65536 }) |count| {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const root = try std.fmt.allocPrint(a, "/tmp/antfly-range-prune-bench-{d}", .{antfly.platform_time.monotonicNs()});
        try std.Io.Dir.cwd().createDirPath(io, root);
        defer std.Io.Dir.cwd().deleteTree(io, root) catch {};
        const forward = try std.fmt.allocPrintSentinel(a, "{s}/forward", .{root}, 0);
        const reverse = try std.fmt.allocPrintSentinel(a, "{s}/reverse", .{root}, 0);
        var index = try antfly.graph.GraphIndex.openWithPrivateStores(alloc, forward, reverse, "g", .{});
        defer index.close();
        const writes = try a.alloc(antfly.graph.BatchWrite, count);
        for (writes, 0..) |*write, i| write.* = .{ .source = try std.fmt.allocPrint(a, "source-{d:0>8}", .{i}), .target = "hub", .edge_type = "link" };
        var samples: [5]u64 = undefined;
        var fence_samples: [5]u64 = undefined;
        var lookup_samples: [5]u64 = undefined;
        var maximum_page_ns: u64 = 0;
        for (0..6) |sample| {
            try index.batchApply(writes, &.{});
            const start = std.Io.Clock.awake.now(io);
            try index.fenceOwnedRange(alloc, "source-", "");
            const fence_ns: u64 = @intCast(start.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds());
            if (index.edge_count != count) return error.UnexpectedSynchronousRetirement;
            const lookup_start = std.Io.Clock.awake.now(io);
            const hidden = try index.getEdges(alloc, "hub", "link", .in);
            defer antfly.graph.GraphIndex.freeEdges(alloc, hidden);
            if (hidden.len != 0) return error.InvalidBenchmarkResult;
            const lookup_ns: u64 = @intCast(lookup_start.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds());
            const retire_start = std.Io.Clock.awake.now(io);
            var removed: usize = 0;
            while (true) {
                const page_start = std.Io.Clock.awake.now(io);
                const page = try index.pruneOwnedRangePage();
                const page_ns: u64 = @intCast(page_start.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds());
                if (sample != 0) maximum_page_ns = @max(maximum_page_ns, page_ns);
                removed += page orelse break;
            }
            const elapsed: u64 = @intCast(retire_start.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds());
            if (removed != count or index.edge_count != 0 or index.node_count != 0) return error.InvalidBenchmarkResult;
            if (sample != 0) {
                samples[sample - 1] = elapsed;
                fence_samples[sample - 1] = fence_ns;
                lookup_samples[sample - 1] = lookup_ns;
            }
        }
        std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
        std.mem.sort(u64, &fence_samples, {}, std.sort.asc(u64));
        std.mem.sort(u64, &lookup_samples, {}, std.sort.asc(u64));
        const json = try std.json.Stringify.valueAlloc(a, .{ .mode = "durable_stateful_range_retirement", .edges = count, .page_record_limit = 1024, .page_identity_byte_limit = 4 * 1024 * 1024, .fence_median_ns = fence_samples[2], .retirement_median_ns = samples[2], .maximum_page_ns = maximum_page_ns, .scoped_incoming_median_ns = lookup_samples[2], .note = "default LSM; graph ownership fence and durability only, not end-to-end Raft apply; retirement includes all durable barriers; excludes insertion; five warm samples" }, .{});
        try out.interface.writeAll(json);
        try out.interface.writeByte('\n');
        try out.flush();
    }
}

fn benchmarkCommittedCounters(io: std.Io, out: anytype) !void {
    const alloc = std.heap.smp_allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const fixture = arena.allocator();
    const ids = try fixture.alloc([]const u8, 1024);
    for (ids, 0..) |*id, i| id.* = try std.fmt.allocPrint(fixture, "node-{d:0>8}", .{i});
    const writes = try fixture.alloc(antfly.graph.BatchWrite, ids.len * 64);
    const deletes = try fixture.alloc(antfly.graph.BatchDelete, writes.len);
    for (writes, deletes, 0..) |*write, *delete, i| {
        write.* = .{ .source = ids[i / 64], .target = ids[(i / 64 + i % 64 + 1) % ids.len], .edge_type = "link" };
        delete.* = .{ .source = write.source, .target = write.target, .edge_type = write.edge_type };
    }
    for ([_]bool{ true, false }) |reference| {
        const root = try std.fmt.allocPrint(fixture, "/tmp/antfly-global-counter-bench-{d}", .{antfly.platform_time.monotonicNs()});
        try std.Io.Dir.cwd().createDirPath(io, root);
        defer std.Io.Dir.cwd().deleteTree(io, root) catch {};
        const store_path = try std.fmt.allocPrint(fixture, "{s}/store\x00", .{root});
        const reverse_path = try std.fmt.allocPrint(fixture, "{s}/reverse\x00", .{root});
        var store = try antfly.docstore.DocStore.open(alloc, @ptrCast(store_path.ptr), .{});
        defer store.close();
        var index = try antfly.graph.GraphIndex.open(alloc, &store, @ptrCast(reverse_path.ptr), "links", .{});
        defer index.close();
        var samples: [5]u64 = undefined;
        for (0..6) |sample| {
            const started = antfly.platform_time.monotonicNs();
            try index.benchmarkBatchApply(writes, &.{}, reference);
            if (index.edge_count != writes.len or index.node_count != ids.len) return error.InvalidBenchmarkResult;
            try index.benchmarkBatchApply(&.{}, deletes, reference);
            const elapsed = antfly.platform_time.monotonicNs() - started;
            if (index.edge_count != 0 or index.node_count != 0) return error.InvalidBenchmarkResult;
            if (sample != 0) samples[sample - 1] = elapsed;
        }
        std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
        const json = try std.json.Stringify.valueAlloc(fixture, .{
            .mode = if (reference) "stateful_global_counters_per_edge_committed" else "stateful_global_counters_coalesced_committed",
            .edges = writes.len,
            .nodes = ids.len,
            .median_ns = samples[2],
            .endpoint_counter_reads = if (reference) writes.len * 4 else ids.len * 2,
            .note = "default durable LSM; scalar presence/per-edge counters versus sorted presence/coalesced counters; six insert+delete cycles, first discarded; identical original/final topology; includes both directional commits and WAL, excludes fixture; no forced compaction or reopen",
        }, .{});
        try out.interface.writeAll(json);
        try out.interface.writeByte('\n');
        try out.flush();
    }
}

fn benchmarkPresence(out: anytype) !void {
    const alloc = std.heap.smp_allocator;
    const lsm = antfly.lsm_backend;
    for ([_]usize{ 256, 16384 }) |value_bytes| {
        var fixture = std.heap.ArenaAllocator.init(alloc);
        defer fixture.deinit();
        const a = fixture.allocator();
        const keys = try a.alloc([]const u8, 1024);
        for (keys, 0..) |*key, i| key.* = try std.fmt.allocPrint(a, "reverse/link/source-{d:0>8}", .{i});
        const value = try a.alloc(u8, value_bytes);
        @memset(value, 'm');
        var stats = PhaseAllocStats{};
        var tracking = PhaseTrackingAllocator{ .backing = alloc, .stats = &stats };
        const measured_alloc = tracking.allocator();
        var storage = lsm.MemoryStorage.init(alloc);
        defer storage.deinit();
        var cache = lsm.Cache.init(measured_alloc, lsm.DefaultCacheSizeBytes);
        defer cache.deinit();
        var backend = try lsm.Backend.open(measured_alloc, "/graph-presence-bench", .{ .flush_threshold = 1, .storage = storage.storage(), .cache = &cache });
        defer backend.close();
        var runtime = try backend.runtimeStore(measured_alloc, .{ .name = "graph" });
        defer runtime.deinit();
        {
            var write = try runtime.beginWrite();
            errdefer write.abort();
            for (keys) |key| try write.put(key, value);
            try write.commit();
        }
        while (try backend.runMaintenanceStep()) {}
        for ([_]bool{ true, false }) |reference| {
            var samples: [5]u64 = undefined;
            var peaks: [5]usize = undefined;
            var copies: u64 = 0;
            for (0..6) |sample| {
                const baseline = stats.current_bytes;
                stats.peak_bytes = baseline;
                const before = backend.snapshotReadStats();
                const started = antfly.platform_time.monotonicNs();
                var batch = try runtime.beginBatch();
                errdefer batch.abort();
                if (reference) {
                    for (keys) |key| if ((try batch.get(key)).len != value.len) return error.InvalidBenchmarkResult;
                } else {
                    var present: [1024]bool = undefined;
                    try batch.containsManySorted(keys, &present);
                    for (present) |exists| if (!exists) return error.InvalidBenchmarkResult;
                }
                batch.abort();
                const elapsed = antfly.platform_time.monotonicNs() - started;
                copies = backend.snapshotReadStats().point_value_copies - before.point_value_copies;
                if (copies != (if (reference) @as(u64, keys.len) else 0)) return error.InvalidBenchmarkResult;
                if (sample != 0) {
                    samples[sample - 1] = elapsed;
                    peaks[sample - 1] = stats.peak_bytes -| baseline;
                }
            }
            std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
            std.mem.sort(usize, &peaks, {}, std.sort.asc(usize));
            const json = try std.json.Stringify.valueAlloc(a, .{ .mode = if (reference) "scalar_presence" else "sorted_presence", .keys = keys.len, .value_bytes = value_bytes, .median_ns = samples[2], .extra_peak_bytes = peaks[2], .value_copies = copies, .note = "warm immutable LSM runs and block cache on modeled storage; identical existing-key results; six samples, first discarded; includes batch lifetime; excludes fixture and disk latency" }, .{});
            try out.interface.writeAll(json);
            try out.interface.writeByte('\n');
            try out.flush();
        }
    }
}

fn benchmarkTypedEdgeScans(io: std.Io, out: anytype) !void {
    const alloc = std.heap.smp_allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const fixture = arena.allocator();
    const root = try std.fmt.allocPrint(fixture, "/tmp/antfly-typed-edge-bench-{d}", .{antfly.platform_time.monotonicNs()});
    try std.Io.Dir.cwd().createDirPath(io, root);
    defer std.Io.Dir.cwd().deleteTree(io, root) catch {};
    const store_path = try std.fmt.allocPrint(fixture, "{s}/store\x00", .{root});
    const reverse_path = try std.fmt.allocPrint(fixture, "{s}/reverse\x00", .{root});
    var store = try antfly.docstore.DocStore.open(alloc, @ptrCast(store_path.ptr), .{});
    defer store.close();
    var index = try antfly.graph.GraphIndex.open(alloc, &store, @ptrCast(reverse_path.ptr), "links", .{ .metric_configs = &.{.{ .name = "selected", .kind = .degree, .edge_filter = .{ .mode = .types, .types = &.{"type-00"} } }} });
    defer index.close();
    const ids = try fixture.alloc([]const u8, 1024);
    for (ids, 0..) |*id, i| id.* = try std.fmt.allocPrint(fixture, "node-{d:0>8}", .{i});
    const types = try fixture.alloc([]const u8, 16);
    for (types, 0..) |*kind, i| kind.* = try std.fmt.allocPrint(fixture, "type-{d:0>2}", .{i});
    const writes = try fixture.alloc(antfly.graph.BatchWrite, ids.len * 64);
    for (writes, 0..) |*write, i| write.* = .{ .source = ids[i / 64], .target = ids[(i / 64 + i % 64 + 1) % ids.len], .edge_type = types[i % types.len] };
    try index.batchApply(writes, &.{});
    for ([_]bool{ true, false }) |reference| {
        var samples: [5]u64 = undefined;
        var measured: PhaseAllocStats = undefined;
        var endpoint_reads: usize = 0;
        for (0..6) |sample| {
            var stats = PhaseAllocStats{};
            var tracking = PhaseTrackingAllocator{ .backing = alloc, .stats = &stats };
            const started = antfly.platform_time.monotonicNs();
            endpoint_reads = try index.benchmarkTypedMembershipUpdates(tracking.allocator(), writes, reference);
            const elapsed = antfly.platform_time.monotonicNs() - started;
            if (stats.current_bytes != 0) return error.InvalidBenchmarkResult;
            if (sample != 0) samples[sample - 1] = elapsed;
            measured = stats;
        }
        if (endpoint_reads != (if (reference) writes.len * 2 else ids.len * types.len)) return error.InvalidBenchmarkResult;
        std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
        const json = try std.json.Stringify.valueAlloc(fixture, .{
            .mode = if (reference) "stateful_membership_per_edge" else "stateful_membership_coalesced",
            .edges = writes.len,
            .endpoint_reads = endpoint_reads,
            .median_ns = samples[2],
            .scratch_peak_bytes = measured.peak_bytes,
            .scratch_allocations = measured.alloc_count,
            .note = "default durable LSM; identical all-edge removal followed by abort; six samples, first discarded; includes posting maintenance, excludes fixture and WAL commit; allocator measures update scratch only",
        }, .{});
        try out.interface.writeAll(json);
        try out.interface.writeByte('\n');
        try out.flush();
    }
    const filter = antfly.graph.GraphMetricEdgeFilter{ .mode = .types, .types = types[0..1] };
    const expected = try index.benchmarkMetricEdgeScan(filter, true);
    for ([_]bool{ true, false }) |reference| {
        var samples: [5]u64 = undefined;
        var measured: antfly.graph.GraphIndex.MetricEdgeScanBenchmark = undefined;
        for (0..6) |sample| {
            const started = antfly.platform_time.monotonicNs();
            measured = try index.benchmarkMetricEdgeScan(filter, reference);
            const elapsed = antfly.platform_time.monotonicNs() - started;
            if (measured.matched != expected.matched or measured.checksum != expected.checksum) return error.InvalidBenchmarkResult;
            if (sample != 0) samples[sample - 1] = elapsed;
        }
        if (measured.visited != (if (reference) writes.len else writes.len / types.len)) return error.InvalidBenchmarkResult;
        std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
        const json = try std.json.Stringify.valueAlloc(fixture, .{
            .mode = if (reference) "stateful_filtered_full_scan" else "stateful_filtered_type_postings",
            .edges = writes.len,
            .visited = measured.visited,
            .matched = measured.matched,
            .median_ns = samples[2],
            .note = "default durable LSM; same selected edge identity checksum; six samples, first discarded; discovery only, excludes fixture writes and numerical execution",
        }, .{});
        try out.interface.writeAll(json);
        try out.interface.writeByte('\n');
        try out.flush();
    }
    for ([_]bool{ true, false }) |reference| {
        var samples: [5]u64 = undefined;
        var steps: usize = 0;
        for (0..6) |sample| {
            const started = antfly.platform_time.monotonicNs();
            const result = try index.benchmarkPartitionCensus(if (reference) .{} else filter);
            const elapsed = antfly.platform_time.monotonicNs() - started;
            if (result.edges != (if (reference) writes.len else writes.len / types.len) or result.nodes != ids.len) return error.InvalidBenchmarkResult;
            steps = result.steps;
            if (sample != 0) samples[sample - 1] = elapsed;
        }
        std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
        const json = try std.json.Stringify.valueAlloc(fixture, .{
            .mode = if (reference) "stateful_global_census" else "stateful_selected_census",
            .source_edges = writes.len,
            .selected_edges = writes.len / types.len,
            .checkpoint_steps = steps,
            .median_ns = samples[2],
            .note = "cold plans on default durable LSM; includes clearing prior plan, durable checkpoints, counts and boundaries; excludes fixture writes and numerical execution",
        }, .{});
        try out.interface.writeAll(json);
        try out.interface.writeByte('\n');
        try out.flush();
    }
    const control_oracle = try index.benchmarkPartitionPlanControl(true);
    for ([_]bool{ true, false }) |reference| {
        var samples: [5]u64 = undefined;
        for (0..6) |sample| {
            const started = antfly.platform_time.monotonicNs();
            for (0..128) |_| if (try index.benchmarkPartitionPlanControl(reference) != control_oracle) return error.InvalidBenchmarkResult;
            if (sample != 0) samples[sample - 1] = (antfly.platform_time.monotonicNs() - started) / 128;
        }
        std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
        const json = try std.json.Stringify.valueAlloc(fixture, .{
            .mode = if (reference) "stateful_plan_with_boundaries" else "stateful_plan_control_only",
            .median_ns = samples[2],
            .control_bytes = 76,
            .note = "default durable LSM; same validated plan identity; 128 reads per sample; reference includes addressed boundary loading and validation; no writes",
        }, .{});
        try out.interface.writeAll(json);
        try out.interface.writeByte('\n');
        try out.flush();
    }
}

fn benchmarkSelectedTopologyReads(io: std.Io, out: anytype) !void {
    const alloc = std.heap.smp_allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const fixture = arena.allocator();
    const root = try std.fmt.allocPrint(fixture, "/tmp/antfly-selected-topology-{d}", .{antfly.platform_time.monotonicNs()});
    try std.Io.Dir.cwd().createDirPath(io, root);
    defer std.Io.Dir.cwd().deleteTree(io, root) catch {};
    var fs = try antfly.serverless.artifacts.FsStore.init(alloc, root);
    var artifacts = fs.artifactStore();
    defer artifacts.deinit();
    var builder = graph.Builder{ .alloc = alloc };
    defer builder.deinit();
    const ids = try fixture.alloc([]const u8, 16384);
    for (ids, 0..) |*id, i| id.* = try std.fmt.allocPrint(fixture, "node-{d:0>8}-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", .{i});
    for (ids, 0..) |id, i| {
        for (0..16) |j| try builder.addEdge(id, ids[(i + j + 1) % ids.len], "noise", 1, null);
        if (i < 256) try builder.addEdge(id, ids[(i + 1) % 256], "selected", 1, null);
    }
    const payload = try builder.encodeAlloc(256 * 1024 * 1024, .none);
    defer alloc.free(payload);
    var metadata = try artifacts.put(payload);
    defer metadata.deinit(alloc);
    var source = antfly.serverless.manifest.ArtifactRef{ .kind = .graph_segment, .name = "graph", .artifact_id = metadata.artifact_id, .checksum = metadata.checksum, .byte_len = metadata.byte_len };
    try graph.codec.compact.bindTopologyControl(&source, payload);
    try artifacts.verifyContentWithCancellationUsingAllocator(alloc, source.artifact_id, source.byte_len, source.checksum, .none);
    for ([_]bool{ true, false }) |sparse| {
        const config = antfly.graph.GraphMetricConfig{ .name = "degree", .kind = .degree, .edge_filter = if (sparse) .{ .mode = .types, .types = &.{"selected"} } else .{} };
        const oracle = try metric.benchmarkSelectedArtifactPreparation(alloc, &artifacts, source, config, true);
        for ([_][2]bool{ .{ true, true }, .{ false, true }, .{ true, false }, .{ false, false } }) |mode| {
            const reference = mode[0];
            const warm = mode[1];
            var samples: [5]u64 = undefined;
            var measured: PhaseAllocStats = undefined;
            var result: metric.SelectedPreparationBenchmark = undefined;
            for (0..6) |sample| {
                var cold_fs = try antfly.serverless.artifacts.FsStore.init(alloc, root);
                var cold_store = cold_fs.artifactStore();
                defer cold_store.deinit();
                var stats = PhaseAllocStats{};
                var tracker = PhaseTrackingAllocator{ .backing = alloc, .stats = &stats };
                const started = antfly.platform_time.monotonicNs();
                result = try metric.benchmarkSelectedArtifactPreparation(tracker.allocator(), if (warm) &artifacts else &cold_store, source, config, reference);
                const elapsed = antfly.platform_time.monotonicNs() - started;
                if (stats.current_bytes != 0 or result.edges != oracle.edges or !std.mem.eql(u8, &result.digest, &oracle.digest)) return error.InvalidBenchmarkResult;
                if (sample != 0) samples[sample - 1] = elapsed;
                measured = stats;
            }
            std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
            const json = try std.json.Stringify.valueAlloc(fixture, .{
                .mode = if (reference) "serverless_sourcewide_preparation" else "serverless_addressed_preparation",
                .sparse = sparse,
                .warm_identity = warm,
                .source_bytes = source.byte_len,
                .selected_edges = result.edges,
                .retained_nodes = result.retained_nodes,
                .read_bytes = result.read_bytes,
                .median_ns = samples[2],
                .peak_bytes = measured.peak_bytes,
                .allocations = measured.alloc_count,
                .note = "local source; cold identity uses a new verifier per sample, not a cold OS page cache; includes preparation and semantic identity, excludes fixture and numerical kernel; six samples, first discarded; digest parity",
            }, .{});
            try out.interface.writeAll(json);
            try out.interface.writeByte('\n');
            try out.flush();
        }
    }
}

fn benchmarkSemanticMetricReuse(io: std.Io, out: anytype) !void {
    const alloc = std.heap.smp_allocator;
    const manifest = antfly.serverless.manifest;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const fixture = arena.allocator();
    const root = try std.fmt.allocPrint(fixture, "/tmp/antfly-semantic-reuse-{d}", .{antfly.platform_time.monotonicNs()});
    try std.Io.Dir.cwd().createDirPath(io, root);
    defer std.Io.Dir.cwd().deleteTree(io, root) catch {};
    var fs = try antfly.serverless.artifacts.FsStore.init(alloc, root);
    var artifacts = fs.artifactStore();
    defer artifacts.deinit();
    const ids = try fixture.alloc([]const u8, 1024);
    for (ids, 0..) |*id, i| id.* = try std.fmt.allocPrint(fixture, "node-{d:0>8}", .{i});
    var sources: [2]manifest.ArtifactRef = undefined;
    for (&sources, 0..) |*source, variant| {
        var builder = graph.Builder{ .alloc = alloc };
        defer builder.deinit();
        for (ids, 0..) |id, i| for (0..64) |j| {
            const target = if (j % 4 == 0) 0 else (i + j * j + 1) % ids.len;
            try builder.addEdge(id, ids[target], "link", @floatFromInt(variant + 1), null);
        };
        const payload = try builder.encodeAlloc(16 * 1024 * 1024, .none);
        defer alloc.free(payload);
        var metadata = try artifacts.put(payload);
        defer metadata.deinit(alloc);
        source.* = .{ .kind = .graph_segment, .name = "graph", .artifact_id = try fixture.dupe(u8, metadata.artifact_id), .checksum = try fixture.dupe(u8, metadata.checksum), .byte_len = metadata.byte_len };
        try graph.codec.compact.bindTopologyControl(source, payload);
    }
    const config = antfly.graph.GraphMetricConfig{ .name = "rank", .kind = .pagerank, .max_iterations = 30, .tolerance = 1e-15 };
    const first_request = metric.PublicationRequest{ .graph_index_name = "graph", .source_graph = sources[0], .config = config, .provenance = .{ .published_generation = 1, .edge_generation = 1, .computed_at_ms = 1 } };
    var first_budget = antfly.serverless.build.graph_metric_policy.Budget{ .limits = .{} };
    const first = try metric.publishRequestsAlloc(alloc, &artifacts, &.{first_request}, .none, .{}, &first_budget, .{});
    defer {
        manifest.types.freeArtifactRefs(alloc, first);
        alloc.free(first);
    }
    const first_bytes = try artifacts.getVerifiedAllocWithCancellationUsingAllocator(alloc, first[0].artifact_id, first[0].byte_len, first[0].checksum, .none);
    defer alloc.free(first_bytes);
    var expected = try antfly.serverless.graph_metric_segment.decodeAlloc(alloc, first_bytes);
    defer expected.deinit(alloc);
    for ([_]bool{ true, false }) |reference| {
        var samples: [5]u64 = undefined;
        var measured = PhaseAllocStats{};
        var work: u64 = 0;
        for (0..6) |sample| {
            var stats = PhaseAllocStats{};
            var tracker = PhaseTrackingAllocator{ .backing = alloc, .stats = &stats };
            const tracked = tracker.allocator();
            const request = metric.PublicationRequest{ .graph_index_name = "graph", .source_graph = sources[1], .config = config, .prior_artifact = if (reference) null else first[0], .provenance = .{ .published_generation = 2, .edge_generation = 2, .computed_at_ms = 2 } };
            var budget = antfly.serverless.build.graph_metric_policy.Budget{ .limits = .{} };
            const started = antfly.platform_time.monotonicNs();
            const refs = try metric.publishRequestsWithPriorAlloc(tracked, &artifacts, &.{request}, if (reference) &.{} else first, .none, .{}, &budget, .{});
            const elapsed = antfly.platform_time.monotonicNs() - started;
            const bytes = try artifacts.getVerifiedAllocWithCancellationUsingAllocator(alloc, refs[0].artifact_id, refs[0].byte_len, refs[0].checksum, .none);
            defer alloc.free(bytes);
            var decoded = try antfly.serverless.graph_metric_segment.decodeAlloc(alloc, bytes);
            defer decoded.deinit(alloc);
            if (decoded.scores.len != expected.scores.len) return error.InvalidBenchmarkResult;
            for (decoded.scores, expected.scores) |actual, wanted| {
                if (!std.mem.eql(u8, actual.node_id, wanted.node_id) or actual.value != wanted.value) return error.InvalidBenchmarkResult;
            }
            if (!reference and (!std.mem.eql(u8, refs[0].artifact_id, first[0].artifact_id) or budget.work_items != 0)) return error.InvalidBenchmarkResult;
            work = budget.work_items;
            manifest.types.freeArtifactRefs(tracked, refs);
            tracked.free(refs);
            if (stats.current_bytes != 0) return error.InvalidBenchmarkResult;
            if (sample != 0) samples[sample - 1] = elapsed;
            measured = stats;
        }
        std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
        const json = try std.json.Stringify.valueAlloc(fixture, .{
            .mode = if (reference) "serverless_weight_change_recompute" else "serverless_weight_change_semantic_reuse",
            .nodes = ids.len,
            .edges = ids.len * 64,
            .median_ns = samples[2],
            .peak_bytes = measured.peak_bytes,
            .projection_kernel_work = work,
            .note = "same weighted source change; exact score parity; includes authenticated cached-source reads, preparation, identity hashing and publication; excludes graph construction and post-run score validation; six samples, first discarded",
        }, .{});
        try out.interface.writeAll(json);
        try out.interface.writeByte('\n');
        try out.flush();
    }
}

fn benchmarkOrdinalCursors(io: std.Io, out: anytype) !void {
    const alloc = std.heap.smp_allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const fixture = arena.allocator();
    for ([_]usize{ 16, 4096 }) |id_len| {
        const root = try std.fmt.allocPrint(fixture, "/tmp/antfly-ordinal-cursor-bench-{d}", .{antfly.platform_time.monotonicNs()});
        try std.Io.Dir.cwd().createDirPath(io, root);
        defer std.Io.Dir.cwd().deleteTree(io, root) catch {};
        const store_path = try std.fmt.allocPrint(fixture, "{s}/store\x00", .{root});
        const reverse_path = try std.fmt.allocPrint(fixture, "{s}/reverse\x00", .{root});
        var store = try antfly.docstore.DocStore.open(alloc, @ptrCast(store_path.ptr), .{});
        defer store.close();
        const configs = [_]antfly.graph.GraphMetricConfig{.{ .name = "rank", .kind = .pagerank, .refresh = .manual, .max_iterations = 1 }};
        var index = try antfly.graph.GraphIndex.open(alloc, &store, @ptrCast(reverse_path.ptr), "links", .{ .metric_configs = &configs });
        defer index.close();
        var ids: [256][]const u8 = undefined;
        for (&ids, 0..) |*id, i| {
            const bytes = try fixture.alloc(u8, id_len);
            @memset(bytes, 'x');
            _ = try std.fmt.bufPrint(bytes[0..8], "{d:0>8}", .{i});
            id.* = bytes;
        }
        for (ids, 0..) |id, i| try index.addEdge(id, ids[(i + 1) % ids.len], "cites", 1, 0, 0, "");
        try index.benchmarkPrepareOrdinalCursor("rank");
        const expected = try index.benchmarkOrdinalCursorRead("rank", true);
        for ([_]bool{ true, false }) |reference| {
            var samples: [5]u64 = undefined;
            for (0..6) |sample| {
                const started = antfly.platform_time.monotonicNs();
                for (0..64) |_| if (try index.benchmarkOrdinalCursorRead("rank", reference) != expected) return error.InvalidBenchmarkResult;
                const elapsed = (antfly.platform_time.monotonicNs() - started) / 64;
                if (sample != 0) samples[sample - 1] = elapsed;
            }
            std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
            const encoded = try std.json.Stringify.valueAlloc(fixture, .{
                .mode = if (reference) "stateful_string_cursor" else "stateful_ordinal_cursor",
                .nodes = ids.len,
                .node_id_bytes = id_len,
                .median_ns = samples[2],
                .note = "same sealed topology; exact ordinal checksum parity; cursor traversal only, excludes numeric kernel and writes; 64 repetitions; six samples, first discarded",
            }, .{});
            try out.interface.writeAll(encoded);
            try out.interface.writeByte('\n');
            try out.flush();
        }
    }
}

fn benchmarkSharedTopology(io: std.Io, out: anytype) !void {
    const alloc = std.heap.smp_allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const fixture = arena.allocator();
    const root = try std.fmt.allocPrint(fixture, "/tmp/antfly-shared-topology-bench-{d}", .{antfly.platform_time.monotonicNs()});
    try std.Io.Dir.cwd().createDirPath(io, root);
    defer std.Io.Dir.cwd().deleteTree(io, root) catch {};
    const store_path = try std.fmt.allocPrint(fixture, "{s}/store\x00", .{root});
    const reverse_path = try std.fmt.allocPrint(fixture, "{s}/reverse\x00", .{root});
    var store = try antfly.docstore.DocStore.open(alloc, @ptrCast(store_path.ptr), .{});
    defer store.close();
    const configs = [_]antfly.graph.GraphMetricConfig{
        .{ .name = "seed", .kind = .pagerank, .refresh = .manual, .max_iterations = 1 },
        .{ .name = "candidate", .kind = .pagerank, .refresh = .manual, .max_iterations = 1, .damping = 0.5 },
    };
    var index = try antfly.graph.GraphIndex.open(alloc, &store, @ptrCast(reverse_path.ptr), "links", .{ .metric_configs = &configs });
    defer index.close();
    const ids = try fixture.alloc([]const u8, 1024);
    for (ids, 0..) |*id, i| id.* = try std.fmt.allocPrint(fixture, "node-{d:0>8}", .{i});
    for (ids, 0..) |source, i| for (1..17) |offset| try index.addEdge(source, ids[(i + offset) % ids.len], "cites", 1, 0, 0, "");
    var seed = try index.runPageRankMetricPlanned("seed");
    seed.deinit(alloc);
    for ([_]bool{ false, true }) |reuse| {
        var samples: [5]u64 = undefined;
        var measured: antfly.graph.GraphIndex.TopologyBuildBenchmark = undefined;
        for (0..6) |sample| {
            const start = antfly.platform_time.monotonicNs();
            measured = try index.benchmarkTopologyBuild("candidate", reuse);
            const elapsed = antfly.platform_time.monotonicNs() - start;
            if (measured.adopted != reuse or measured.physical_edge_units != (if (reuse) @as(u64, 0) else 2 * 16 * ids.len)) return error.InvalidBenchmarkResult;
            if (sample != 0) samples[sample - 1] = elapsed;
            for (ids) |id| if (@abs((try index.graphMetricScore("candidate", id)).? - 1.0 / @as(f64, @floatFromInt(ids.len))) > 1e-12) return error.InvalidBenchmarkResult;
            while (try index.cleanupRetiredGraphMetricScoreGenerationPage("candidate")) {}
            for (0..16) |_| _ = try index.cleanupGraphMetricTopologyPage();
        }
        std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
        const encoded = try std.json.Stringify.valueAlloc(fixture, .{
            .mode = if (reuse) "stateful_shared_topology" else "stateful_independent_topology",
            .nodes = ids.len,
            .edges = ids.len * 16,
            .physical_edge_units = measured.physical_edge_units,
            .checkpoints = measured.checkpoints,
            .median_ns = samples[2],
            .min_ns = samples[0],
            .max_ns = samples[4],
            .note = "default storage; complete one-iteration numerical job including publication and job cleanup; six samples, first discarded; excludes fixture writes, verification and topology GC; verifies every score and physical edge work",
        }, .{});
        try out.interface.writeAll(encoded);
        try out.interface.writeByte('\n');
        try out.flush();
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
