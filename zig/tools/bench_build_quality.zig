//! Bounded-batch construction and held-out recall/candidate-work probe.
//! The read-only float32 fixture is external vector ownership; its bytes are
//! reported separately from build workspace. This excludes HTTP and WAL costs.
const std = @import("std");
const hbc = @import("antfly_hbc_isolate_root").hbc;
const Allocator = std.mem.Allocator;
const Source = struct {
    values: []const f32,
    dims: usize,
    fn load(ctx: *anyopaque, alloc: Allocator, id: u64, _: []const u8) ![]f32 {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        if (id == 0 or id > self.values.len / self.dims) return error.NotFound;
        return alloc.dupe(f32, self.values[(id - 1) * self.dims ..][0..self.dims]);
    }
    fn loadMatrix(ctx: *anyopaque, ids: []const u64, metadata: []const ?[]const u8, positions: []const usize, matrix: []f32, _: []f32, dims: usize, index: *hbc.HBCIndex, transform: hbc.HBCIndex.ExternalVectorTransformFn) !void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        if (ids.len != metadata.len or ids.len != positions.len or dims != self.dims) return error.InvalidFixture;
        for (ids, positions) |id, position| {
            if (id == 0 or id > self.values.len / dims) return error.NotFound;
            _ = transform(index, self.values[(id - 1) * dims ..][0..dims], matrix[position * dims ..][0..dims]);
        }
    }
};
fn now(io: std.Io) i96 {
    return std.Io.Clock.awake.now(io).nanoseconds;
}
fn read(comptime T: type, init: std.process.Init, path: []const u8, limit: usize) ![]T {
    const bytes = try std.Io.Dir.cwd().readFileAllocOptions(init.io, path, std.heap.smp_allocator, .limited(limit), .@"8", null);
    if (bytes.len % @sizeOf(T) != 0) return error.InvalidFixture;
    return std.mem.bytesAsSlice(T, bytes);
}
pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    if (args.len != 8) return error.ExpectedTrainQueriesTruthDimsAlgorithmBatchOutputPath;
    const dims = try std.fmt.parseInt(usize, args[4], 10);
    const rebuild_algorithm: ?hbc.TopologyRebuildAlgorithm = if (std.mem.startsWith(u8, args[5], "recluster_"))
        std.meta.stringToEnum(hbc.TopologyRebuildAlgorithm, args[5]["recluster_".len..]) orelse return error.InvalidAlgorithm
    else
        null;
    const algorithm: hbc.BulkBuildAlgo = if (rebuild_algorithm != null) .recursive else std.meta.stringToEnum(hbc.BulkBuildAlgo, args[5]) orelse return error.InvalidAlgorithm;
    const batch_size = try std.fmt.parseInt(usize, args[6], 10);
    if (dims == 0 or batch_size == 0 or batch_size > 8192 or dims * batch_size * 4 > 64 * 1024 * 1024) return error.BuildWorkspaceLimit;
    const alloc = std.heap.smp_allocator;
    const train = try read(f32, init, args[1], 4 * 1024 * 1024 * 1024);
    defer alloc.free(std.mem.sliceAsBytes(train));
    const queries = try read(f32, init, args[2], 64 * 1024 * 1024);
    defer alloc.free(std.mem.sliceAsBytes(queries));
    const truth = try read(u64, init, args[3], 8 * 1024 * 1024);
    defer alloc.free(std.mem.sliceAsBytes(truth));
    const count = train.len / dims;
    const nq = queries.len / dims;
    if (train.len % dims != 0 or queries.len % dims != 0 or truth.len != nq * 100) return error.InvalidFixture;
    var source: Source = .{ .values = train, .dims = dims };
    const path = try init.arena.allocator().dupeZ(u8, args[7]);
    var idx = try hbc.HBCIndex.open(alloc, path, .{
        .dims = @intCast(dims),
        .metric = .cosine,
        .storage_backend = .lsm,
        .leaf_size = 128,
        .branching_factor = 16,
        .bulk_build_algo = algorithm,
        .centroid_directory_mode = .flat_rabitq,
        .rerank_policy = .boundary,
        .max_cached_vectors = 0,
        .kmeans_backend = .cpu,
    });
    defer idx.close();
    idx.setIo(init.io);
    idx.setExternalVectorLoader(&source, Source.load);
    idx.setExternalVectorBatchTransformedMatrixLoader(&source, Source.loadMatrix);
    const items = try alloc.alloc(hbc.BatchInsertItem, batch_size);
    defer alloc.free(items);
    const keys = try alloc.alloc([24]u8, batch_size);
    defer alloc.free(keys);
    const started = now(init.io);
    idx.setExperimentalPostingAuthorityTransitionPermitted(true);
    try idx.finalizeExperimentalPostingGenerationAtAppliedSequence(0, .{ .flatten = false, .make_authoritative = true });
    try idx.beginExperimentalPostingMutationCapture();
    try idx.beginBulkIngestSession();
    var offset: usize = 0;
    while (offset < count) {
        const n = @min(batch_size, count - offset);
        for (items[0..n], 0..) |*item, i| item.* = .{
            .vector_id = offset + i + 1,
            .vector = train[(offset + i) * dims ..][0..dims],
            .metadata = try std.fmt.bufPrint(&keys[i], "doc:{d:0>10}", .{offset + i}),
        };
        if (offset == 0) {
            try idx.bulkBuildWithMetadataOptions(items[0..n], .{ .skip_vector_store = true, .algo = algorithm });
        } else {
            try idx.batchInsertWithMetadataOptions(items[0..n], .{
                .skip_vector_store = true,
                .assume_absent_ids = true,
                .centroid_only_routing = true,
                .allow_quantized_routing = true,
                .coalesce_leaf_writes = true,
                .defer_quantized_rebuild = true,
                .defer_quantized_rebuild_to_bulk_finish = true,
                .bulk_ingest = true,
                .defer_leaf_splits_to_batch_finish = true,
            });
        }
        offset += n;
        std.debug.print("build_progress rows={d} count={d}\n", .{ offset, count });
    }
    try idx.finishBulkIngestSessionWithOptions(.{ .compact = false });
    try idx.persistExperimentalPostingSidecarAtAppliedSequence(count, .{});
    const build_ns = now(init.io) - started;
    if (rebuild_algorithm) |rebuild| {
        const required = idx.topologyRebuildWorkspaceBytes(rebuild) orelse return error.BuildWorkspaceLimit;
        const rebuild_started = now(init.io);
        const rebuilt = (try idx.rebuildTopologyFromCurrentVectors(512 * 1024 * 1024, rebuild, 1, count)) orelse return error.BuildWorkspaceLimit;
        std.debug.print("topology_rebuild {{\"algorithm\":\"{s}\",\"workspace_bytes\":{d},\"vectors\":{d},\"elapsed_ns\":{d}}}\n", .{ args[5], required, rebuilt.vectors, now(init.io) - rebuild_started });
    }
    if (!(try idx.verifyTreeLinks()).consistent() or idx.stats().active_count != count) return error.InvalidBuiltTree;
    std.debug.print("build_quality {{\"algorithm\":\"{s}\",\"count\":{d},\"dims\":{d},\"batch\":{d},\"build_ns\":{d},\"fixture_bytes\":{d}}}\n", .{ args[5], count, dims, batch_size, build_ns, train.len * 4 });
    for ([_]usize{ 16, 32, 64, 128, 256, 512, 1024, 2048 }) |probes| {
        idx.config.flat_centroid_probe_count = probes;
        var found: usize = 0;
        var scored: u64 = 0;
        var leaves: u64 = 0;
        const began = now(init.io);
        for (0..nq) |qi| {
            var result = try idx.searchProfiledRequest(.{ .query = queries[qi * dims ..][0..dims], .k = 100, .search_width = @intCast(probes), .epsilon = 7, .rerank_factor = 4, .load_metadata = false });
            defer result.results.deinit();
            for (result.results.getHits()) |hit| if (std.mem.indexOfScalar(u64, truth[qi * 100 ..][0..100], hit.vector_id) != null) {
                found += 1;
            };
            scored += result.profile.approx_vectors_scored;
            leaves += result.profile.leaves_explored;
        }
        std.debug.print("routing_quality {{\"algorithm\":\"{s}\",\"count\":{d},\"probes\":{d},\"queries\":{d},\"hits\":{d},\"scored\":{d},\"leaves\":{d},\"elapsed_ns\":{d}}}\n", .{ args[5], count, probes, nq, found, scored, leaves, now(init.io) - began });
    }
}
