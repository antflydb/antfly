// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Bounded-batch or full-corpus construction and held-out recall probe.
//! Pass `full` in place of the batch size for normalized squared-L2 inputs.
//! An optional trailing argument selects the leaf capacity (default 128).
//! The read-only float32 fixture is external vector ownership; its bytes are
//! reported separately from build workspace. This excludes HTTP and WAL costs.
const std = @import("std");
const isolate = @import("antfly_hbc_isolate_root");
const hbc = isolate.hbc;
const Allocator = std.mem.Allocator;
const Source = struct {
    values: []const f32,
    dims: usize,
    count: usize,
    file: ?std.Io.File,
    io: std.Io,

    fn loadInto(ctx: *anyopaque, id: u64, _: []const u8, scratch: []f32) ![]const f32 {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        if (id == 0 or id > self.count or scratch.len < self.dims) return error.NotFound;
        const offset = (id - 1) * self.dims;
        if (self.file) |file| {
            const bytes = std.mem.sliceAsBytes(scratch[0..self.dims]);
            if (try file.readPositionalAll(self.io, bytes, offset * 4) != bytes.len) return error.InvalidFixture;
            return scratch[0..self.dims];
        }
        return self.values[offset..][0..self.dims];
    }

    fn load(ctx: *anyopaque, alloc: Allocator, id: u64, _: []const u8) ![]f32 {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        const values = try alloc.alloc(f32, self.dims);
        errdefer alloc.free(values);
        const loaded = try loadInto(ctx, id, "", values);
        if (loaded.ptr != values.ptr) @memcpy(values, loaded);
        return values;
    }
    fn loadMatrix(ctx: *anyopaque, ids: []const u64, metadata: []const ?[]const u8, positions: []const usize, matrix: []f32, scratch: []f32, dims: usize, index: *hbc.HBCIndex, transform: hbc.HBCIndex.ExternalVectorTransformFn) !void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        if (ids.len != metadata.len or ids.len != positions.len or dims != self.dims) return error.InvalidFixture;
        for (ids, positions) |id, position| {
            const input = try loadInto(ctx, id, "", scratch);
            _ = transform(index, input, matrix[position * dims ..][0..dims]);
        }
    }
};
fn now(io: std.Io) i96 {
    return std.Io.Clock.awake.now(io).nanoseconds;
}
pub fn read(comptime T: type, init: std.process.Init, path: []const u8, limit: usize) ![]T {
    const file = try std.Io.Dir.cwd().openFile(init.io, path, .{});
    defer file.close(init.io);
    const size = try file.length(init.io);
    if (size > limit or size % @sizeOf(T) != 0) return error.InvalidFixture;
    const alloc = std.heap.smp_allocator;
    const values = try alloc.alloc(T, @intCast(size / @sizeOf(T)));
    errdefer alloc.free(values);
    const bytes = std.mem.sliceAsBytes(values);
    // Darwin rejects a single read whose requested length exceeds INT_MAX.
    var offset: usize = 0;
    while (offset < bytes.len) {
        const chunk = bytes[offset..][0..@min(8 * 1024 * 1024, bytes.len - offset)];
        if (try file.readPositionalAll(init.io, chunk, offset) != chunk.len) return error.InvalidFixture;
        offset += chunk.len;
    }
    return values;
}
fn elapsed(start: u64) u64 {
    return isolate.platform_time.monotonicNs() -| start;
}

fn fullBuild(idx: *hbc.HBCIndex, train: []const f32, dims: usize) !void {
    const alloc = idx.alloc;
    const count = train.len / dims;
    const inputs = try alloc.alloc(hbc.PreparedBulkBuildInput, count);
    defer alloc.free(inputs);
    const keys = try alloc.alloc([24]u8, count);
    defer alloc.free(keys);
    for (inputs, keys, 0..) |*input, *key, i| {
        const vector = train[i * dims ..][0..dims];
        input.* = .{
            .vector_id = i + 1,
            .vector = vector,
            .transformed = vector,
            .metadata = try std.fmt.bufPrint(key, "doc:{d:0>10}", .{i}),
        };
    }
    try isolate.vectorindex.hbc_index.bulkBuildPreparedInputsBounded(idx, inputs, .{
        .skip_vector_store = true,
    }, 64 * 1024 * 1024, isolate.platform_time.monotonicNs, elapsed);
}

fn spooledBuild(idx: *hbc.HBCIndex, source: Source, path: []const u8, options: hbc.HBCIndex.SpoolBuildOptions) !void {
    const io = source.io;
    const a = idx.alloc;
    const spool_path = try std.fmt.allocPrint(a, "{s}/bulk-spool", .{path});
    defer a.free(spool_path);
    try std.Io.Dir.cwd().createDir(io, spool_path, .fromMode(0o700));
    defer std.Io.Dir.cwd().deleteTree(io, spool_path) catch {};
    const dir = try std.Io.Dir.cwd().openDir(io, spool_path, .{});
    defer dir.close(io);
    var spool: hbc.HBCIndex.BulkSpool = .{ .dir = dir, .io = io, .dims = source.dims };
    var buffer: [64 * 1024]u8 = undefined;
    var output = try spool.create(&buffer);
    var output_open = true;
    defer if (output_open) output.abort(spool);
    try output.enableSample(a, 16 * 32, options.sample_bytes, idx.rng.intN(std.math.maxInt(usize)));
    var read_buffer: [64 * 1024]u8 = undefined;
    var reader = source.file.?.reader(io, &read_buffer);
    const vector = try a.alloc(f32, source.dims);
    defer a.free(vector);
    for (0..source.count) |i| {
        try reader.interface.readSliceAll(std.mem.sliceAsBytes(vector));
        var key: [24]u8 = undefined;
        try output.append(i + 1, try std.fmt.bufPrint(&key, "doc:{d:0>10}", .{i}), vector);
    }
    const root = try output.finish(io);
    output_open = false;
    try idx.bulkBuildSpool(&spool, root, options);
}

pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    if (args.len < 8 or args.len > 11) return error.ExpectedTrainQueriesTruthDimsAlgorithmBatchOutputPath;
    const leaf_size = if (args.len >= 9) try std.fmt.parseInt(u32, args[8], 10) else 128;
    if (leaf_size == 0) return error.InvalidLeafSize;
    const dims = try std.fmt.parseInt(usize, args[4], 10);
    const rebuild_algorithm: ?hbc.TopologyRebuildAlgorithm = if (std.mem.startsWith(u8, args[5], "recluster_"))
        std.meta.stringToEnum(hbc.TopologyRebuildAlgorithm, args[5]["recluster_".len..]) orelse return error.InvalidAlgorithm
    else
        null;
    const spool_options: ?hbc.HBCIndex.SpoolBuildOptions = if (std.mem.eql(u8, args[5], "spool_lloyd"))
        .{ .algorithm = .lloyd }
    else if (std.mem.eql(u8, args[5], "spool_emeans"))
        .{ .algorithm = .emeans }
    else
        null;
    const algorithm: hbc.BulkBuildAlgo = if (rebuild_algorithm != null or spool_options != null) .recursive else std.meta.stringToEnum(hbc.BulkBuildAlgo, args[5]) orelse return error.InvalidAlgorithm;
    const reopen = std.mem.eql(u8, args[6], "reopen");
    const full = std.mem.eql(u8, args[6], "full") or reopen;
    const streaming = spool_options != null or reopen;
    if (spool_options != null and !full) return error.InvalidAlgorithm;
    const sample_size = if (args.len >= 10) try std.fmt.parseInt(usize, args[9], 10) else 0;
    if (sample_size != 0 and (!full or (algorithm != .emeans and algorithm != .kmeans))) return error.InvalidBulkTrainingSample;
    const batch_size = if (full) 1 else try std.fmt.parseInt(usize, args[6], 10);
    if (full and spool_options == null and (rebuild_algorithm != null or (algorithm != .kmeans and algorithm != .emeans and algorithm != .hilbert_seeded))) return error.InvalidAlgorithm;
    if (dims == 0 or batch_size == 0 or batch_size > 8192 or dims * batch_size * 4 > 64 * 1024 * 1024) return error.BuildWorkspaceLimit;
    const alloc = std.heap.smp_allocator;
    const load_started = now(init.io);
    const train_file = if (streaming) try std.Io.Dir.cwd().openFile(init.io, args[1], .{}) else null;
    defer if (train_file) |file| file.close(init.io);
    const train = if (train_file == null) try read(f32, init, args[1], 4 * 1024 * 1024 * 1024) else &.{};
    defer if (train_file == null) alloc.free(train);
    const fixture_bytes = if (train_file) |file| try file.length(init.io) else train.len * 4;
    const row_bytes = try std.math.mul(usize, dims, 4);
    const queries = try read(f32, init, args[2], 64 * 1024 * 1024);
    defer alloc.free(queries);
    const truth = try read(u64, init, args[3], 8 * 1024 * 1024);
    defer alloc.free(truth);
    const count = std.math.cast(usize, fixture_bytes / row_bytes) orelse return error.InvalidFixture;
    const nq = queries.len / dims;
    if (fixture_bytes % row_bytes != 0 or queries.len % dims != 0 or truth.len != nq * 100) return error.InvalidFixture;
    var source: Source = .{ .values = train, .dims = dims, .count = count, .file = train_file, .io = init.io };
    const path = try init.arena.allocator().dupeSentinel(u8, args[7], 0);
    const config: hbc.HBCConfig = .{
        .dims = @intCast(dims),
        .metric = if (full) .l2_squared else .cosine,
        .storage_backend = .lsm,
        .leaf_size = leaf_size,
        .branching_factor = 16,
        .bulk_build_algo = algorithm,
        .quantizer_seed = if (args.len == 11) try std.fmt.parseInt(u64, args[10], 10) else (hbc.HBCConfig{ .dims = 1 }).quantizer_seed,
        .bulk_build_training_sample_size = sample_size,
        .centroid_directory_mode = .flat_rabitq,
        .rerank_policy = .boundary,
        .max_cached_vectors = 0,
        .max_cached_metadata = if (streaming) 0 else (hbc.HBCConfig{ .dims = 1 }).max_cached_metadata,
        .max_cached_nodes = if (streaming) std.math.clamp((16 * 1024 * 1024) / (dims * (@as(usize, leaf_size) + 8)), 1, 256) else (hbc.HBCConfig{ .dims = 1 }).max_cached_nodes,
        .pinned_tree_depth = if (streaming) 0 else (hbc.HBCConfig{ .dims = 1 }).pinned_tree_depth,
        .kmeans_backend = .cpu,
    };
    var build_budget: isolate.memory_budget = .{ .backing = alloc, .limit = 256 * 1024 * 1024 };
    const index_alloc = if (streaming) build_budget.allocator() else alloc;
    var idx = try hbc.HBCIndex.open(index_alloc, path, config);
    var index_open = true;
    defer if (index_open) idx.close();
    idx.setIo(init.io);
    idx.setExternalVectorLoader(&source, Source.load);
    idx.setExternalVectorScratchLoader(&source, Source.loadInto);
    idx.setExternalVectorBatchTransformedMatrixLoader(&source, Source.loadMatrix);
    const started = now(init.io);
    if (reopen) {
        try idx.activateExperimentalPostingReads(count);
    } else if (full) {
        if (count == 0) return error.InvalidFixture;
        std.debug.print("full_build_start algorithm={s} vectors={d} dims={d} leaf_size={d} fixture_load_ns={d}\n", .{ args[5], count, dims, leaf_size, started - load_started });
        if (spool_options) |options| try spooledBuild(&idx, source, path, options) else try fullBuild(&idx, train, dims);
        const constructed = now(init.io);
        if (spool_options != null) std.debug.print("build_workspace {{\"limit_bytes\":{d},\"peak_bytes\":{d}}}\n", .{ build_budget.limit, build_budget.peak });
        const profile = idx.getWriteProfile();
        std.debug.print("leaf_training {{\"sample_limit\":{d},\"training_rows\":{d},\"training_ns\":{d},\"corpus_route_ns\":{d},\"leaf_count\":{d},\"min_leaf_members\":{d},\"max_leaf_members\":{d}}}\n", .{
            sample_size,                         profile.bulk_build_leaf_training_rows, profile.bulk_build_leaf_training_ns,
            profile.bulk_build_leaf_route_ns,    profile.bulk_build_leaf_count,         profile.bulk_build_leaf_min_members,
            profile.bulk_build_leaf_max_members,
        });
        std.debug.print("full_build_constructed algorithm={s} elapsed_ns={d} cluster_ns={d}\n", .{ args[5], constructed - started, profile.bulk_build_leaf_cluster_ns });
        idx.setExperimentalPostingAuthorityTransitionPermitted(true);
        try idx.finalizeExperimentalPostingGenerationAtAppliedSequence(count, .{ .flatten = true, .make_authoritative = true });
        var generation = (try idx.nativeBackupGeneration(alloc, count)) orelse return error.MissingNativePostingGeneration;
        defer generation.deinit();
        const published = now(init.io);
        if (spool_options != null) std.debug.print("publication_workspace {{\"limit_bytes\":{d},\"peak_bytes\":{d},\"live_bytes\":{d}}}\n", .{ build_budget.limit, build_budget.peak, build_budget.live });
        std.debug.print("full_build_phase {{\"algorithm\":\"{s}\",\"count\":{d},\"leaf_size\":{d},\"fixture_load_ns\":{d},\"construction_ns\":{d},\"leaf_cluster_ns\":{d},\"posting_publish_ns\":{d},\"segments\":{d}}}\n", .{
            args[5], count, leaf_size, started - load_started, constructed - started, profile.bulk_build_leaf_cluster_ns, published - constructed, generation.segment_generations.len,
        });
        idx.close();
        index_open = false;
        idx = try hbc.HBCIndex.open(index_alloc, path, config);
        index_open = true;
        idx.setIo(init.io);
        idx.setExternalVectorLoader(&source, Source.load);
        idx.setExternalVectorScratchLoader(&source, Source.loadInto);
        idx.setExternalVectorBatchTransformedMatrixLoader(&source, Source.loadMatrix);
        try idx.activateExperimentalPostingReads(count);
    } else {
        const items = try alloc.alloc(hbc.BatchInsertItem, batch_size);
        defer alloc.free(items);
        const keys = try alloc.alloc([24]u8, batch_size);
        defer alloc.free(keys);
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
    }
    const build_ns = now(init.io) - started;
    if (rebuild_algorithm) |rebuild| {
        const required = idx.topologyRebuildWorkspaceBytes(rebuild) orelse return error.BuildWorkspaceLimit;
        const rebuild_started = now(init.io);
        const rebuilt = (try idx.rebuildTopologyFromCurrentVectors(512 * 1024 * 1024, rebuild, 1, count)) orelse return error.BuildWorkspaceLimit;
        std.debug.print("topology_rebuild {{\"algorithm\":\"{s}\",\"workspace_bytes\":{d},\"vectors\":{d},\"elapsed_ns\":{d}}}\n", .{ args[5], required, rebuilt.vectors, now(init.io) - rebuild_started });
    }
    if (!(try idx.verifyTreeLinks()).consistent() or idx.stats().active_count != count) return error.InvalidBuiltTree;
    std.debug.print("build_quality {{\"algorithm\":\"{s}\",\"count\":{d},\"dims\":{d},\"batch\":{d},\"build_ns\":{d},\"fixture_bytes\":{d}}}\n", .{ args[5], count, dims, if (full) count else batch_size, build_ns, fixture_bytes });
    for ([_]usize{ 16, 32, 48, 56, 64, 72, 80, 96, 128, 256, 512, 1024, 2048, 4096, 8192, 16384 }) |probes| {
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
