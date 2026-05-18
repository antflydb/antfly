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
const antfly = @import("antfly-zig");
const platform_time = antfly.platform_time;

const hbc_mod = antfly.hbc;

const DatasetEntry = struct {
    id: u64,
    vector: []const f32,
};

const QueryDef = struct {
    name: []const u8,
    vector: []const f32,
    k: usize,
};

const DumpNode = struct {
    id: u64,
    is_leaf: bool,
    parent: u64,
    level: u16,
    centroid: []f32,
    children: []u64,
    members: []u64,
};

const DumpHit = struct {
    id: u64,
    distance: f32,
};

const DumpQuery = struct {
    name: []const u8,
    vector: []const f32,
    k: usize,
    hits: []DumpHit,
};

const DumpOutput = struct {
    dimension: u32,
    branching_factor: u32,
    leaf_size: u32,
    search_width: u32,
    quantizer_seed: u64,
    use_quantization: bool,
    root_node: u64,
    active_count: u64,
    node_count: u64,
    nodes: []DumpNode,
    queries: []DumpQuery,
};

pub fn main(init: std.process.Init) !void {
    const alloc = init.gpa;

    var tp = TestPath{};
    const path = tp.init();
    defer tp.cleanup();

    var idx = try hbc_mod.HBCIndex.open(alloc, path, .{
        .dims = 2,
        .branching_factor = 2,
        .leaf_size = 2,
        .search_width = 4,
        .use_quantization = false,
        .quantizer_seed = 42,
    });
    defer idx.close();

    const dataset = [_]DatasetEntry{
        .{ .id = 1, .vector = &.{ 1.0, 0.0 } },
        .{ .id = 2, .vector = &.{ 0.9, 0.1 } },
        .{ .id = 3, .vector = &.{ 0.0, 1.0 } },
        .{ .id = 4, .vector = &.{ 0.1, 0.9 } },
        .{ .id = 5, .vector = &.{ 5.0, 5.0 } },
        .{ .id = 6, .vector = &.{ 5.2, 5.1 } },
    };
    for (dataset) |entry| {
        try idx.insert(entry.id, entry.vector);
    }

    const queries = [_]QueryDef{
        .{ .name = "q_axis_x", .vector = &.{ 1.0, 0.0 }, .k = 3 },
        .{ .name = "q_axis_y", .vector = &.{ 0.0, 1.0 }, .k = 3 },
        .{ .name = "q_cluster_far", .vector = &.{ 5.1, 5.1 }, .k = 3 },
    };

    var dump = try buildDump(alloc, &idx, &queries);
    defer deinitDump(alloc, &dump);

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    try std.json.Stringify.value(dump, .{ .whitespace = .indent_2 }, &stdout_writer.interface);
    try stdout_writer.interface.writeByte('\n');
    try stdout_writer.flush();
}

fn buildDump(alloc: std.mem.Allocator, idx: *hbc_mod.HBCIndex, queries: []const QueryDef) !DumpOutput {
    var txn = try idx.beginReadTxn();
    defer txn.abort();

    var queue = std.ArrayListUnmanaged(u64).empty;
    defer queue.deinit(alloc);
    try queue.append(alloc, idx.metadata.root_node);

    var seen = std.AutoHashMapUnmanaged(u64, void).empty;
    defer seen.deinit(alloc);

    var nodes = std.ArrayListUnmanaged(DumpNode).empty;
    errdefer {
        for (nodes.items) |*node| deinitNode(alloc, node);
        nodes.deinit(alloc);
    }

    var cursor: usize = 0;
    while (cursor < queue.items.len) {
        const node_id = queue.items[cursor];
        cursor += 1;

        const gop = try seen.getOrPut(alloc, node_id);
        if (gop.found_existing) continue;

        var node = try idx.loadNode(&txn, node_id);
        defer node.deinit(alloc);

        try nodes.append(alloc, .{
            .id = node.id,
            .is_leaf = node.is_leaf,
            .parent = node.parent,
            .level = node.level,
            .centroid = try alloc.dupe(f32, node.centroid),
            .children = try alloc.dupe(u64, node.children),
            .members = try alloc.dupe(u64, node.members),
        });

        for (node.children) |child_id| {
            try queue.append(alloc, child_id);
        }
    }

    std.mem.sort(DumpNode, nodes.items, {}, struct {
        fn lessThan(_: void, a: DumpNode, b: DumpNode) bool {
            return a.id < b.id;
        }
    }.lessThan);

    var query_results = try alloc.alloc(DumpQuery, queries.len);
    errdefer {
        for (query_results) |*query| deinitQuery(alloc, query);
        alloc.free(query_results);
    }

    for (queries, 0..) |query, i| {
        var results = try idx.search(query.vector, query.k);
        defer results.deinit();

        const hits = results.getHits();
        var dump_hits = try alloc.alloc(DumpHit, hits.len);
        for (hits, 0..) |hit, hit_idx| {
            dump_hits[hit_idx] = .{
                .id = hit.vector_id,
                .distance = hit.distance,
            };
        }

        query_results[i] = .{
            .name = try alloc.dupe(u8, query.name),
            .vector = try alloc.dupe(f32, query.vector),
            .k = query.k,
            .hits = dump_hits,
        };
    }

    return .{
        .dimension = idx.config.dims,
        .branching_factor = idx.config.branching_factor,
        .leaf_size = idx.config.leaf_size,
        .search_width = idx.config.search_width,
        .quantizer_seed = idx.config.quantizer_seed,
        .use_quantization = idx.config.use_quantization,
        .root_node = idx.metadata.root_node,
        .active_count = idx.metadata.active_count,
        .node_count = idx.metadata.node_count,
        .nodes = try nodes.toOwnedSlice(alloc),
        .queries = query_results,
    };
}

fn deinitDump(alloc: std.mem.Allocator, dump: *DumpOutput) void {
    for (dump.nodes) |*node| deinitNode(alloc, node);
    alloc.free(dump.nodes);
    for (dump.queries) |*query| deinitQuery(alloc, query);
    alloc.free(dump.queries);
    dump.* = undefined;
}

fn deinitNode(alloc: std.mem.Allocator, node: *DumpNode) void {
    alloc.free(node.centroid);
    alloc.free(node.children);
    alloc.free(node.members);
    node.* = undefined;
}

fn deinitQuery(alloc: std.mem.Allocator, query: *DumpQuery) void {
    alloc.free(query.name);
    alloc.free(query.vector);
    alloc.free(query.hits);
    query.* = undefined;
}

const TestPath = struct {
    buf: [256]u8 = undefined,

    fn init(self: *TestPath) [*:0]const u8 {
        const ts = tempPathId();
        const slice = std.fmt.bufPrint(&self.buf, "/tmp/antfly-hbc-parity-{d}\x00", .{ts}) catch unreachable;
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().createDirPath(io_impl.io(), std.mem.span(@as([*:0]const u8, @ptrCast(slice.ptr)))) catch unreachable;
        return @ptrCast(slice.ptr);
    }

    fn cleanup(self: *TestPath) void {
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(@as([*:0]const u8, @ptrCast(&self.buf)))) catch {};
    }
};

fn tempPathId() u64 {
    return platform_time.monotonicNs();
}
