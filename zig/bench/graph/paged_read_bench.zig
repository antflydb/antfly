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
const artifacts = antfly.serverless.artifacts;
const Allocator = std.mem.Allocator;

const Memory = struct {
    payload: []const u8,
    calls: usize = 0,
    bytes: usize = 0,
    fn deinit(_: Allocator, _: *anyopaque) void {}
    fn put(_: *anyopaque, _: Allocator, _: []const u8) !artifacts.ArtifactMetadata {
        return error.Unsupported;
    }
    fn get(ptr: *anyopaque, alloc: Allocator, _: []const u8) ![]u8 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.calls += 1;
        self.bytes += self.payload.len;
        return alloc.dupe(u8, self.payload);
    }
    fn range(ptr: *anyopaque, alloc: Allocator, _: []const u8, offset: u64, len: usize) ![]u8 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.calls += 1;
        self.bytes += len;
        return alloc.dupe(u8, self.payload[@intCast(offset)..][0..len]);
    }
    fn stat(_: *anyopaque, _: Allocator, _: []const u8) !artifacts.ArtifactMetadata {
        return error.Unsupported;
    }
    fn delete(_: *anyopaque, _: []const u8) !void {
        return error.Unsupported;
    }
    const vtable = artifacts.ArtifactStore.VTable{ .deinit = deinit, .put = put, .get_alloc = get, .get_range_alloc = range, .stat = stat, .delete = delete };
};

pub fn main(init: std.process.Init) !void {
    var buffer: [4096]u8 = undefined;
    var output = std.Io.File.stdout().writer(init.io, &buffer);
    try run(init.io, &output);
}

pub fn run(io: std.Io, out: anytype) !void {
    const alloc = std.heap.smp_allocator;
    for ([_]usize{ 64, 1024, 10000, 20000 }) |count| {
        var fixture = std.heap.ArenaAllocator.init(alloc);
        defer fixture.deinit();
        const a = fixture.allocator();
        var builder = graph.Builder{ .alloc = a };
        defer builder.deinit();
        for (0..count) |i| try builder.addEdge("a", "b", try std.fmt.allocPrint(a, "kind{d:0>5}", .{i}), 1, null);
        const payload = try builder.encodeAlloc(256 * 1024 * 1024, .none);
        const checksum = try digestAlloc(a, payload);
        var source = antfly.serverless.ArtifactRef{ .kind = .graph_segment, .name = "g", .artifact_id = try std.fmt.allocPrint(a, "sha256:{s}", .{checksum}), .checksum = checksum, .byte_len = payload.len };
        try graph.codec.compact.bindTopologyControl(&source, payload);
        var memory = Memory{ .payload = payload };
        var store = artifacts.ArtifactStore{ .allocator = alloc, .ptr = &memory, .vtable = &Memory.vtable };
        var samples: [5]u64 = undefined;
        for (0..6) |sample| {
            memory.calls = 0;
            memory.bytes = 0;
            const start = std.Io.Clock.awake.now(io);
            const result = try antfly.serverless.build.lake_graph_metric.benchmarkSelectedArtifactPreparation(alloc, &store, source, .{ .name = "degree", .kind = .degree }, false);
            if (result.edges != count) return error.InvalidBenchmarkResult;
            const elapsed: u64 = @intCast(start.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds());
            if (sample != 0) samples[sample - 1] = elapsed;
        }
        if (memory.bytes > payload.len or memory.calls > 8) return error.RangeAmplificationRegression;
        std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
        const json = try std.json.Stringify.valueAlloc(a, .{ .mode = "many_type_topology", .types = count, .artifact_bytes = payload.len, .range_calls = memory.calls, .read_bytes = memory.bytes, .median_ns = samples[2] }, .{});
        try out.interface.writeAll(json);
        try out.interface.writeByte('\n');
        try out.flush();
    }

    for ([_]usize{ 16384, 100000 }) |count| {
        var fixture = std.heap.ArenaAllocator.init(alloc);
        defer fixture.deinit();
        const a = fixture.allocator();
        const ids = try a.alloc([]const u8, count);
        for (ids, 0..) |*id, i| id.* = try std.fmt.allocPrint(a, "collection/customer-record-{d:0>8}", .{i});
        var builder = graph.Builder{ .alloc = a };
        defer builder.deinit();
        for (ids, 0..) |id, i| try builder.addEdge(id, ids[(i + 1) % count], "link", 1, null);
        const payload = try builder.encodeAlloc(256 * 1024 * 1024, .none);
        const checksum = try digestAlloc(a, payload);
        var source = antfly.serverless.ArtifactRef{ .kind = .graph_segment, .name = "g", .artifact_id = try std.fmt.allocPrint(a, "sha256:{s}", .{checksum}), .checksum = checksum, .byte_len = payload.len };
        try graph.codec.compact.bindTopologyControl(&source, payload);
        for ([_]bool{ true, false }) |reference| {
            var memory = Memory{ .payload = payload };
            var store = artifacts.ArtifactStore{ .allocator = alloc, .ptr = &memory, .vtable = &Memory.vtable };
            var samples: [5]u64 = undefined;
            for (0..6) |sample| {
                memory.calls = 0;
                memory.bytes = 0;
                const start = std.Io.Clock.awake.now(io);
                if (reference) {
                    const bytes = try store.getAlloc(source.artifact_id);
                    defer alloc.free(bytes);
                    var segment = try graph.decodeAlloc(alloc, bytes);
                    defer segment.deinit(alloc);
                    var index = try graph.AdjacencyIndex.init(alloc, segment);
                    defer index.deinit(alloc);
                    const row = index.find(segment, ids[count / 2]).?;
                    if (row.out_edges.len != 1 or !std.mem.eql(u8, row.out_edges[0].neighbor_id, ids[count / 2 + 1])) return error.InvalidBenchmarkResult;
                } else {
                    var remaining: u64 = 512 * 1024 * 1024;
                    var reader = (try graph.AdjacencyReader.init(alloc, &store, source, .none, &remaining)).?;
                    defer reader.deinit();
                    var work: usize = 100;
                    var row = (try reader.adjacency(ids[count / 2], &.{}, enum { out, in, both }.out, 1, &work)).?;
                    defer row.deinit(alloc);
                    if (row.out_edges.len != 1 or !std.mem.eql(u8, row.out_edges[0].neighbor_id, ids[count / 2 + 1])) return error.InvalidBenchmarkResult;
                }
                const elapsed: u64 = @intCast(start.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds());
                if (sample != 0) samples[sample - 1] = elapsed;
            }
            if (!reference and memory.bytes >= payload.len) return error.RangeAmplificationRegression;
            std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
            const json = try std.json.Stringify.valueAlloc(a, .{ .mode = if (reference) "whole_graph_adjacency" else "paged_graph_adjacency", .nodes = count, .artifact_bytes = payload.len, .range_calls = memory.calls, .read_bytes = memory.bytes, .median_ns = samples[2], .note = "fresh reader each sample; in-memory transport counts exact bytes; includes decoding/authentication and cleanup; no network latency model; full reference omits transport SHA verification" }, .{});
            try out.interface.writeAll(json);
            try out.interface.writeByte('\n');
            try out.flush();
        }
    }

    // Same authenticated reader and transport on both sides. Only the
    // consumer changes: eager hub materialization versus stopping at edge 1.
    for ([_]usize{ 16384, 100000 }) |count| {
        var fixture = std.heap.ArenaAllocator.init(alloc);
        defer fixture.deinit();
        const a = fixture.allocator();
        var builder = graph.Builder{ .alloc = a };
        defer builder.deinit();
        for (0..count) |i| try builder.addEdge("hub", try std.fmt.allocPrint(a, "customer-{d:0>8}", .{i}), "link", 1, null);
        const payload = try builder.encodeAlloc(256 * 1024 * 1024, .none);
        const checksum = try digestAlloc(a, payload);
        var source = antfly.serverless.ArtifactRef{ .kind = .graph_segment, .name = "g", .artifact_id = try std.fmt.allocPrint(a, "sha256:{s}", .{checksum}), .checksum = checksum, .byte_len = payload.len };
        try graph.codec.compact.bindTopologyControl(&source, payload);
        for ([_]bool{ true, false }) |reference| {
            var memory = Memory{ .payload = payload };
            var store = artifacts.ArtifactStore{ .allocator = alloc, .ptr = &memory, .vtable = &Memory.vtable };
            var samples: [5]u64 = undefined;
            var inspected: usize = 0;
            for (0..6) |sample| {
                memory.calls = 0;
                memory.bytes = 0;
                const start = std.Io.Clock.awake.now(io);
                var remaining: u64 = 512 * 1024 * 1024;
                var reader = (try graph.AdjacencyReader.init(alloc, &store, source, .none, &remaining)).?;
                var work: usize = count;
                if (reference) {
                    var row = (try reader.adjacency("hub", &.{}, enum { out, in, both }.out, count, &work)).?;
                    defer row.deinit(alloc);
                    if (row.out_edges.len != count) return error.InvalidBenchmarkResult;
                } else {
                    var cursor = try reader.cursor("hub", &.{}, false, &work);
                    defer cursor.deinit();
                    var edge = (try cursor.next()).?;
                    defer edge.deinit(alloc);
                    if (!std.mem.eql(u8, edge.neighbor_id, "customer-00000000")) return error.InvalidBenchmarkResult;
                }
                inspected = count - work;
                reader.deinit();
                if (sample != 0) samples[sample - 1] = @intCast(start.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds());
            }
            std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
            const json = try std.json.Stringify.valueAlloc(a, .{ .mode = if (reference) "eager_hub" else "cursor_hub_first_edge", .edges = count, .artifact_bytes = payload.len, .range_calls = memory.calls, .read_bytes = memory.bytes, .inspected_edges = inspected, .median_ns = samples[2] }, .{});
            try out.interface.writeAll(json);
            try out.interface.writeByte('\n');
            try out.flush();
        }
    }
    try demandQueries(io, out);
    try largeNodeDirectory(io, out);
}

fn demandQueries(io: std.Io, out: anytype) !void {
    const alloc = std.heap.smp_allocator;
    const query = antfly.serverless.query;
    for ([_]usize{ 16384, 100000 }) |count| {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const a = arena.allocator();
        var builder = graph.Builder{ .alloc = a };
        defer builder.deinit();
        for (0..count) |i| try builder.addEdge("hub", try std.fmt.allocPrint(a, "customer-{d:0>8}", .{i}), "link", 1, null);
        const payload = try builder.encodeAlloc(256 * 1024 * 1024, .none);
        const checksum = try digestAlloc(a, payload);
        var refs = [_]antfly.serverless.ArtifactRef{.{ .kind = .graph_segment, .name = "g", .artifact_id = try std.fmt.allocPrint(a, "sha256:{s}", .{checksum}), .checksum = checksum, .byte_len = payload.len }};
        try graph.codec.compact.bindTopologyControl(&refs[0], payload);
        for ([_]bool{ false, true }) |traversal| {
            var memory = Memory{ .payload = payload };
            var store = artifacts.ArtifactStore{ .allocator = alloc, .ptr = &memory, .vtable = &Memory.vtable };
            var samples: [5]u64 = undefined;
            for (0..6) |sample| {
                memory.calls = 0;
                memory.bytes = 0;
                const start = std.Io.Clock.awake.now(io);
                {
                    var session = query.QuerySession{ .alloc = alloc, .artifacts = &store, .owns_manifest = false, .manifest = .{ .namespace = "n", .version = 1, .built_at_ns = 0, .wal_start_lsn = 0, .wal_end_lsn = 0, .stats = .{}, .artifacts = &refs } };
                    defer session.deinit();
                    if (traversal) {
                        const nodes = try query.graphTraverseWithLimitsAlloc(alloc, &session, .{ .index_name = @constCast("g"), .start_doc_id = @constCast("hub"), .limit = 1, .max_depth = 2, .include_start = false }, .{ .max_edges_scanned = 1 });
                        defer alloc.free(nodes);
                        defer for (nodes) |*node| node.deinit(alloc);
                        if (nodes.len != 1 or !std.mem.eql(u8, nodes[0].doc_id, "customer-00000000")) return error.InvalidBenchmarkResult;
                    } else {
                        const nodes = try query.graphNeighborsWithLimitsAlloc(alloc, &session, .{ .index_name = @constCast("g"), .doc_id = @constCast("hub"), .limit = 1 }, .{ .max_edges_scanned = 1 });
                        defer alloc.free(nodes);
                        defer for (nodes) |*node| node.deinit(alloc);
                        if (nodes.len != 1 or !std.mem.eql(u8, nodes[0].doc_id, "customer-00000000")) return error.InvalidBenchmarkResult;
                    }
                }
                if (sample != 0) samples[sample - 1] = @intCast(start.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds());
            }
            std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
            const json = try std.json.Stringify.valueAlloc(a, .{ .mode = if (traversal) "traverse_hub_limit_1" else "neighbors_hub_limit_1", .edges = count, .artifact_bytes = payload.len, .range_calls = memory.calls, .read_bytes = memory.bytes, .edge_budget = 1, .median_ns = samples[2], .note = "cold query session, no cache or network latency; includes result materialization and cleanup" }, .{});
            try out.interface.writeAll(json);
            try out.interface.writeByte('\n');
            try out.flush();
        }
    }
}

fn largeNodeDirectory(io: std.Io, out: anytype) !void {
    const alloc = std.heap.smp_allocator;
    const wire = graph.codec.compact;
    const count: usize = 4_000_000;
    // Write canonical isolated rows directly to avoid charging a giant
    // dictionary hash-map fixture to this routing-only benchmark. Production
    // finishEncoding builds and authenticates every routing/control structure.
    const body_len = wire.header_len + count * (12 + 12);
    const size = body_len + try wire.topologyExtensionSize(&.{}, count, 0, body_len);
    const payload = try alloc.alloc(u8, size);
    defer alloc.free(payload);
    @memset(payload[0..wire.header_len], 0);
    @memcpy(payload[0..4], wire.wire_magic);
    std.mem.writeInt(u16, payload[4..6], wire.wire_version, .little);
    std.mem.writeInt(u32, payload[10..14], count, .little);
    std.mem.writeInt(u32, payload[18..22], count, .little);
    for (0..count) |i| {
        const begin = wire.header_len + i * 12;
        std.mem.writeInt(u32, payload[begin..][0..4], 8, .little);
        _ = try std.fmt.bufPrint(payload[begin + 4 ..][0..8], "{d:0>8}", .{i});
        const row = payload[wire.header_len + count * 12 + i * 12 ..][0..12];
        @memset(row, 0);
        std.mem.writeInt(u32, row[0..4], @intCast(i), .little);
    }
    const directory_len = wire.topologyDirectorySize(&.{}, count, body_len, 0);
    try wire.finishEncoding(alloc, payload, body_len, directory_len, .none);
    const checksum = try digestAlloc(alloc, payload);
    defer alloc.free(checksum);
    const id = try std.fmt.allocPrint(alloc, "sha256:{s}", .{checksum});
    defer alloc.free(id);
    var source = antfly.serverless.ArtifactRef{ .kind = .graph_segment, .name = "g", .artifact_id = id, .checksum = checksum, .byte_len = payload.len };
    try wire.bindTopologyControl(&source, payload);
    var memory = Memory{ .payload = payload };
    var store = artifacts.ArtifactStore{ .allocator = alloc, .ptr = &memory, .vtable = &Memory.vtable };
    var samples: [5]u64 = undefined;
    var retained: usize = 0;
    for (0..6) |sample| {
        memory.calls = 0;
        memory.bytes = 0;
        var remaining: u64 = 2 * 1024 * 1024;
        const start = std.Io.Clock.awake.now(io);
        var reader = (try graph.AdjacencyReader.init(alloc, &store, source, .none, &remaining)).?;
        if (!try reader.containsNode("03777777")) return error.InvalidBenchmarkResult;
        retained = reader.context.retainedBytes();
        reader.deinit();
        if (sample != 0) samples[sample - 1] = @intCast(start.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds());
    }
    if (directory_len <= 1024 * 1024 or retained >= 1024 * 1024) return error.InvalidBenchmarkResult;
    std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
    const json = try std.json.Stringify.valueAlloc(alloc, .{ .mode = "large_node_directory", .nodes = count, .artifact_bytes = payload.len, .directory_bytes = directory_len, .retained_control_data_bytes = retained, .range_calls = memory.calls, .read_bytes = memory.bytes, .median_ns = samples[2] }, .{});
    defer alloc.free(json);
    try out.interface.writeAll(json);
    try out.interface.writeByte('\n');
    try out.flush();
}

fn digestAlloc(alloc: Allocator, payload: []const u8) ![]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(payload, &digest, .{});
    return alloc.dupe(u8, &std.fmt.bytesToHex(digest, .lower));
}
