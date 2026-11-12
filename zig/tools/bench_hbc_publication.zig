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

//! Qualify native publication from a sealed private HBC projection.
//! Input path must be a disposable clone, not a serving index. Modes share the
//! same serializers; only projection access differs. No exact source is read.
const std = @import("std");
const isolate = @import("antfly_hbc_isolate_root");
const hbc = isolate.hbc;
var point_reads = false;
var consolidate_projection = false;
var adaptive_projection = false;
var projection_runs_before: usize = 0;
var projection_runs_after: usize = 0;
pub fn hbcBootstrapPointReads() bool {
    return point_reads;
}
pub fn hbcBootstrapConsolidateProjection() ?bool {
    return if (adaptive_projection) null else consolidate_projection;
}
pub fn hbcBootstrapProjectionConsolidated(before: usize, after: usize) void {
    projection_runs_before = before;
    projection_runs_after = after;
}
const Source = struct {
    loads: usize = 0,
    fn load(raw: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8) ![]f32 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.loads += 1;
        return error.UnexpectedExactSourceRead;
    }
    fn active(_: *anyopaque) !void {}
};
fn now(io: std.Io) i96 {
    return std.Io.Clock.awake.now(io).nanoseconds;
}
fn beginPeakWindow(budget: *isolate.memory_budget) usize {
    while (!budget.mutex.tryLock()) std.atomic.spinLoopHint();
    defer budget.mutex.unlock();
    const peak = budget.peak;
    budget.peak = budget.live;
    return peak;
}
fn restorePeakWindow(budget: *isolate.memory_budget, initial: usize) void {
    while (!budget.mutex.tryLock()) std.atomic.spinLoopHint();
    defer budget.mutex.unlock();
    budget.peak = @max(initial, budget.peak);
}
pub fn main(init: std.process.Init) !void {
    const a = std.heap.smp_allocator;
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    if (args.len < 5 or args.len > 6) return error.ExpectedPathDimensionsCountModeAndOptionalCacheBytes;
    const bounded_entries = std.mem.eql(u8, args[4], "ordered_compacted_bounded");
    consolidate_projection = std.mem.eql(u8, args[4], "ordered_compacted") or bounded_entries;
    adaptive_projection = std.mem.eql(u8, args[4], "adaptive");
    point_reads = if (std.mem.eql(u8, args[4], "point")) true else if (std.mem.eql(u8, args[4], "ordered") or consolidate_projection or adaptive_projection) false else return error.InvalidMode;
    const path = try init.arena.allocator().dupeSentinel(u8, args[1], 0);
    const dims = try std.fmt.parseInt(u32, args[2], 10);
    const count = try std.fmt.parseInt(u64, args[3], 10);
    const cache_bytes = if (args.len == 6) try std.fmt.parseInt(usize, args[5], 10) else 16 * 1024 * 1024;
    var budget: isolate.memory_budget = .{ .backing = a, .limit = 256 * 1024 * 1024 };
    var cache = try isolate.lsm_backend.Cache.initFallible(budget.allocator(), cache_bytes);
    defer cache.deinit();
    const config: hbc.HBCConfig = .{ .dims = dims, .leaf_size = 100, .branching_factor = 16, .use_quantization = true, .max_cached_nodes = 50, .max_cached_vectors = 0, .max_cached_metadata = 0, .pinned_tree_depth = 0, .use_random_ortho_trans = false };
    const backend_options: isolate.lsm_backend.Options = .{ .max_run_file_entries = if (bounded_entries or adaptive_projection) 65536 else 0, .retain_bloom_filters = !consolidate_projection and !adaptive_projection, .bulk_ingest_tiered_l0_fan_in = 0, .compact_threshold_runs = std.math.maxInt(usize), .l0_soft_limit_runs = std.math.maxInt(usize), .l0_hard_limit_runs = std.math.maxInt(usize) };
    var index = try hbc.HBCIndex.openWithLsmOptions(budget.allocator(), path, config, .{ .cache = &cache, .backend_options = backend_options });
    var opened = true;
    defer if (opened) index.close();
    index.setIo(init.io);
    projection_runs_before = switch (index.env_owner) {
        .lsm => |handle| handle.backend.runs.count(),
        .native => return error.ExpectedPrivateProjection,
    };
    projection_runs_after = projection_runs_before;
    if (index.stats().active_count != count) return error.InvalidProjectionCount;
    var source: Source = .{};
    index.setExternalVectorLoader(&source, Source.load);
    index.setExperimentalPostingAuthorityTransitionPermitted(true);
    const Sample = struct { id: u64, value: []u8 };
    var samples: std.ArrayList(Sample) = .empty;
    defer {
        for (samples.items) |s| a.free(s.value);
        samples.deinit(a);
    }
    {
        var txn = try index.store.beginRead();
        defer txn.abort();
        var cursor = try txn.openCursor(.vecs);
        defer cursor.close();
        var key: [10]u8 = undefined;
        var entry = try cursor.seekAtOrAfter(isolate.vectorindex.hbc.encodeVecMetaKey(&key, 0));
        while (entry) |row| {
            if (row.key.len != 10 or !std.mem.eql(u8, row.key[0..2], "m:")) break;
            try samples.append(a, .{ .id = std.mem.readInt(u64, row.key[2..10], .big), .value = try a.dupe(u8, row.value) });
            if (samples.items.len == 100) break;
            entry = try cursor.next();
        }
    }
    const initial_peak_bytes = beginPeakWindow(&budget);
    std.debug.print("HBC_PUBLICATION_STARTED\n", .{});
    const started = now(init.io);
    try index.finalizeExperimentalPostingGenerationAtAppliedSequence(0, .{ .flatten = true, .make_authoritative = true, .check = Source.active, .context = &source });
    const elapsed = now(init.io) - started;
    const publication_peak_bytes = budget.footprint();
    restorePeakWindow(&budget, initial_peak_bytes);
    var current_hash: [32]u8 = undefined;
    var segment_count: usize = 0;
    {
        var generation = (try index.nativeBackupGeneration(a, 0)) orelse return error.MissingGeneration;
        defer generation.deinit();
        std.crypto.hash.sha2.Sha256.hash(generation.current_bytes, &current_hash, .{});
        segment_count = generation.segment_generations.len;
    }
    index.close();
    opened = false;
    index = try hbc.HBCIndex.openWithLsmOptions(budget.allocator(), path, config, .{ .cache = &cache, .backend_options = backend_options });
    opened = true;
    index.setIo(init.io);
    index.setExternalVectorLoader(&source, Source.load);
    _ = try index.activateExperimentalPostingReadsAtOrAfter(0);
    if (index.stats().active_count != count or source.loads != 0 or samples.items.len != @min(count, 100)) return error.InvalidReopenedGeneration;
    for (samples.items) |sample| {
        const value = (try index.getMetadata(sample.id)) orelse return error.MissingMetadata;
        defer budget.allocator().free(value);
        if (!std.mem.eql(u8, sample.value, value)) return error.MetadataMismatch;
    }
    const report = try std.json.Stringify.valueAlloc(a, .{ .mode = args[4], .dims = dims, .vectors = count, .publication_ns = elapsed, .run_entry_limit = backend_options.max_run_file_entries, .initial_peak_bytes = initial_peak_bytes, .publication_peak_bytes = publication_peak_bytes, .projection_runs_before = projection_runs_before, .projection_runs_after = projection_runs_after, .allocator_peak_bytes = budget.peak, .allocator_limit_bytes = budget.limit, .cache_bytes = cache_bytes, .exact_source_loads = source.loads, .metadata_samples = samples.items.len, .segments = segment_count, .current_sha256 = std.fmt.bytesToHex(current_hash, .lower), .builtin_is_test = @import("builtin").is_test, .optimize = @tagName(@import("builtin").mode) }, .{});
    defer a.free(report);
    std.debug.print("HBC_PUBLICATION {s}\n", .{report});
}
