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

const db_mod = antfly.db;
const doc_identity = db_mod.doc_identity;
const doc_set = db_mod.doc_set;
const platform_time = antfly.platform_time;
const resource_manager_mod = antfly.resource_manager;

const full_text_index_name = "ft_docid";
const sparse_index_name = "sp_docid";

const Config = struct {
    docs: usize = 4096,
    queries: usize = 16,
    repeats: usize = 8,
    filter_size: usize = 256,
    batch_size: usize = 256,
    sparse_dims: usize = 64,
    limit: u32 = 32,
    body_repeat: usize = 1,
    require_correctness: bool = true,
    max_ordinal_ratio: ?f64 = null,
    require_public_resolution_delta: bool = false,
    progress_every: usize = 0,
    defer_full_index_load: bool = false,
};

const QueryShape = enum {
    match_all_filter,
    full_text_filter,
    sparse_filter,

    fn label(self: QueryShape) []const u8 {
        return switch (self) {
            .match_all_filter => "match_all_filter",
            .full_text_filter => "full_text_filter",
            .sparse_filter => "sparse_filter",
        };
    }
};

const Mode = enum {
    public_ids,
    ordinal_docset,
    sparse_ids,

    fn label(self: Mode) []const u8 {
        return switch (self) {
            .public_ids => "public_ids",
            .ordinal_docset => "ordinal_docset",
            .sparse_ids => "sparse_ids",
        };
    }
};

const FilterPlan = struct {
    doc_ids: []const []const u8,
    native_ids: []const u64,
    ordinal_filter: doc_set.ResolvedDocFilter,

    fn deinit(self: *FilterPlan, alloc: std.mem.Allocator) void {
        if (self.doc_ids.len > 0) alloc.free(self.doc_ids);
        if (self.native_ids.len > 0) alloc.free(self.native_ids);
        self.ordinal_filter.deinit(alloc);
        self.* = undefined;
    }
};

const BenchResult = struct {
    shape: QueryShape,
    mode: Mode,
    repeats: usize,
    queries: usize,
    filter_size: usize,
    elapsed_ns: u64,
    checksum: u64,
    total_hits: u64,
    resolved_set_delta: u64,
    ordinal_list_delta: u64,
    ordinal_bitmap_delta: u64,
    doc_key_list_delta: u64,
    missing_coverage_delta: u64,
    matches_public: bool,
};

pub fn main(init: std.process.Init) !void {
    const alloc = std.heap.page_allocator;
    const cfg = try parseArgs(init.minimal.args);

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const out = &stdout_writer.interface;
    try out.print(
        "docid query bench docs={d} queries={d} repeats={d} filter_size={d} batch_size={d} sparse_dims={d} limit={d}\n",
        .{ cfg.docs, cfg.queries, cfg.repeats, cfg.filter_size, cfg.batch_size, cfg.sparse_dims, cfg.limit },
    );

    var path_buf: [256]u8 = undefined;
    const path = std.fmt.bufPrint(&path_buf, "/tmp/antfly-docid-query-bench-{d}", .{platform_time.monotonicNs()}) catch unreachable;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var resource_manager = resource_manager_mod.ResourceManager.init(.{});
    var db = try db_mod.DB.open(alloc, path, .{
        .primary_backend = .{ .lsm_memory = .{} },
        .start_index_workers = false,
        .resource_manager = &resource_manager,
    });
    defer db.close();

    try db.addIndex(.{
        .name = full_text_index_name,
        .kind = .full_text,
        .config_json = "{\"field\":\"title\"}",
    });
    try db.addIndex(.{
        .name = sparse_index_name,
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse\"}",
    });

    const doc_ids = try loadDocs(alloc, &db, cfg);
    defer freeDocIds(alloc, doc_ids);
    if (cfg.defer_full_index_load) {
        const index_wait_start = nanotime();
        try db.waitForCurrentSyncLevel(.full_index);
        try out.print(
            "{{\"event\":\"docid_query_bench_index_wait\",\"docs\":{d},\"elapsed_ns\":{d}}}\n",
            .{ cfg.docs, nanotime() - index_wait_start },
        );
        try stdout_writer.flush();
    }

    const generation = try db.currentIdentityReadGenerationForRequest(null);
    const prepare_start = nanotime();
    const filters = try buildFilterPlans(alloc, &db, doc_ids, cfg, generation);
    defer {
        for (filters) |*filter| filter.deinit(alloc);
        alloc.free(filters);
    }
    const prepare_ns = nanotime() - prepare_start;
    try out.print(
        "{{\"event\":\"docid_query_bench_prepare\",\"docs\":{d},\"queries\":{d},\"filter_size\":{d},\"identity_read_generation\":{d},\"prepare_ns\":{d}}}\n",
        .{ cfg.docs, cfg.queries, cfg.filter_size, generation, prepare_ns },
    );

    const shapes = [_]QueryShape{ .match_all_filter, .full_text_filter, .sparse_filter };
    for (shapes) |shape| {
        const public_result = try runBench(alloc, &db, cfg, filters, shape, .public_ids, generation, null);
        try printResult(out, cfg, public_result);

        const ordinal_result = try runBench(alloc, &db, cfg, filters, shape, .ordinal_docset, generation, public_result.checksum);
        try printResult(out, cfg, ordinal_result);
        try printSummary(out, cfg, public_result, ordinal_result);
        try stdout_writer.flush();
        try validateSummary(cfg, public_result, ordinal_result);
    }
    try printSparseProjectionResult(out, cfg, try runSparseIdProjectionBench(alloc, &db, doc_ids, cfg, generation, filters));
    try stdout_writer.flush();
}

fn loadDocs(alloc: std.mem.Allocator, db: *db_mod.DB, cfg: Config) ![][]u8 {
    const load_start = nanotime();
    const doc_ids = try alloc.alloc([]u8, cfg.docs);
    errdefer {
        for (doc_ids[0..]) |doc_id| if (doc_id.len > 0) alloc.free(doc_id);
        alloc.free(doc_ids);
    }
    @memset(doc_ids, &.{});

    var writes = std.ArrayListUnmanaged(db_mod.types.BatchWrite).empty;
    defer writes.deinit(alloc);
    var values = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (values.items) |value| alloc.free(value);
        values.deinit(alloc);
    }
    var total_profile = db_mod.BatchProfile{};
    var batches: usize = 0;

    for (0..cfg.docs) |i| {
        const doc_id = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{i});
        doc_ids[i] = doc_id;
        const value = try makeDocValue(alloc, i, cfg);
        try values.append(alloc, value);
        try writes.append(alloc, .{ .key = doc_id, .value = value });
        if (writes.items.len >= cfg.batch_size) {
            var profile = db_mod.BatchProfile{};
            try db.batchProfiled(.{ .writes = writes.items, .sync_level = loadSyncLevel(cfg) }, &profile);
            addBatchProfile(&total_profile, profile);
            batches += 1;
            writes.clearRetainingCapacity();
            printLoadProgress(cfg, i + 1, load_start);
        }
    }
    if (writes.items.len > 0) {
        var profile = db_mod.BatchProfile{};
        try db.batchProfiled(.{ .writes = writes.items, .sync_level = loadSyncLevel(cfg) }, &profile);
        addBatchProfile(&total_profile, profile);
        batches += 1;
        printLoadProgress(cfg, cfg.docs, load_start);
    }
    printLoadSummary(cfg, batches, nanotime() - load_start, total_profile);
    return doc_ids;
}

fn loadSyncLevel(cfg: Config) db_mod.types.SyncLevel {
    return if (cfg.defer_full_index_load) .write else .full_index;
}

fn printLoadProgress(cfg: Config, loaded: usize, load_start: u64) void {
    if (cfg.progress_every == 0) return;
    if (loaded != cfg.docs and loaded % cfg.progress_every != 0) return;
    std.debug.print(
        "{{\"event\":\"docid_query_bench_load_progress\",\"docs\":{d},\"loaded\":{d},\"elapsed_ns\":{d}}}\n",
        .{ cfg.docs, loaded, nanotime() - load_start },
    );
}

fn printLoadSummary(cfg: Config, batches: usize, elapsed_ns: u64, profile: db_mod.BatchProfile) void {
    std.debug.print(
        "{{\"event\":\"docid_query_bench_load_summary\",\"docs\":{d},\"batches\":{d},\"sync_level\":\"{s}\",\"elapsed_ns\":{d},\"profile_total_ns\":{d},\"extract_writes_ns\":{d},\"delete_artifacts_ns\":{d},\"precompute_generated_ns\":{d},\"identity_capacity_ns\":{d},\"identity_metadata_ns\":{d},\"identity_metadata_writes\":{d},\"build_derived_ns\":{d},\"apply_shadow_ns\":{d},\"collect_sync_targets_ns\":{d},\"store_write_ns\":{d},\"append_replay_journal_ns\":{d},\"backlog_pressure_ns\":{d},\"executor_notify_ns\":{d},\"sync_wait_ns\":{d},\"wait_sync_ns\":{d},\"derived_apply_ns\":{d},\"full_text_apply_ns\":{d},\"sparse_apply_ns\":{d},\"index_sync_ns\":{d}}}\n",
        .{
            cfg.docs,
            batches,
            db_mod.types.publicSyncLevelText(loadSyncLevel(cfg)),
            elapsed_ns,
            profile.total_ns,
            profile.extract_writes_ns,
            profile.delete_artifacts_ns,
            profile.precompute_generated_ns,
            profile.identity_capacity_check_ns,
            profile.identity_metadata_ns,
            profile.identity_metadata_writes,
            profile.build_derived_ns,
            profile.apply_shadow_ns,
            profile.collect_sync_targets_ns,
            profile.store_write_ns,
            profile.append_replay_journal_ns,
            profile.backlog_pressure_ns,
            profile.executor_notify_ns,
            profile.sync_wait_ns,
            profile.wait_sync_ns,
            profile.derived_apply_ns,
            profile.full_text_apply_ns,
            profile.sparse_apply_ns,
            profile.index_sync_ns,
        },
    );
}

fn addBatchProfile(total: *db_mod.BatchProfile, item: db_mod.BatchProfile) void {
    total.total_ns += item.total_ns;
    total.resolve_transforms_ns += item.resolve_transforms_ns;
    total.merge_effective_req_ns += item.merge_effective_req_ns;
    total.predicates_ns += item.predicates_ns;
    total.validate_range_ns += item.validate_range_ns;
    total.extract_writes_ns += item.extract_writes_ns;
    total.delete_artifacts_ns += item.delete_artifacts_ns;
    total.precompute_generated_ns += item.precompute_generated_ns;
    total.identity_capacity_check_ns += item.identity_capacity_check_ns;
    total.identity_metadata_ns += item.identity_metadata_ns;
    total.identity_metadata_writes += item.identity_metadata_writes;
    total.store_write_ns += item.store_write_ns;
    total.split_delta_ns += item.split_delta_ns;
    total.build_derived_ns += item.build_derived_ns;
    total.apply_shadow_ns += item.apply_shadow_ns;
    total.collect_sync_targets_ns += item.collect_sync_targets_ns;
    total.append_replay_journal_ns += item.append_replay_journal_ns;
    total.wait_sync_ns += item.wait_sync_ns;
    total.backlog_pressure_ns += item.backlog_pressure_ns;
    total.executor_notify_ns += item.executor_notify_ns;
    total.derived_apply_ns += item.derived_apply_ns;
    total.sync_wait_ns += item.sync_wait_ns;
    total.full_text_apply_ns += item.full_text_apply_ns;
    total.dense_apply_ns += item.dense_apply_ns;
    total.dense_delete_ns += item.dense_delete_ns;
    total.dense_doc_index_ns += item.dense_doc_index_ns;
    total.dense_embedding_apply_ns += item.dense_embedding_apply_ns;
    total.sparse_apply_ns += item.sparse_apply_ns;
    total.graph_apply_ns += item.graph_apply_ns;
    total.index_sync_ns += item.index_sync_ns;
    total.applied_sequence_save_ns += item.applied_sequence_save_ns;
    total.replay_journal_truncate_ns += item.replay_journal_truncate_ns;
    total.notify_enrichment_ns += item.notify_enrichment_ns;
}

fn makeDocValue(alloc: std.mem.Allocator, i: usize, cfg: Config) ![]u8 {
    var body = std.ArrayListUnmanaged(u8).empty;
    defer body.deinit(alloc);
    const token_text = fullTextToken(i);
    const sparse_dim = i % cfg.sparse_dims;
    const prefix = try std.fmt.allocPrint(
        alloc,
        "{{\"title\":\"{s} title {d}\",\"tenant\":\"tenant{d}\",\"score\":{d},\"sparse\":{{\"indices\":[{d}],\"values\":[1.0]}}",
        .{ token_text, i, i % 16, i, sparse_dim },
    );
    defer alloc.free(prefix);
    try body.appendSlice(alloc, prefix);
    if (cfg.body_repeat > 0) {
        try body.appendSlice(alloc, ",\"body\":\"");
        for (0..cfg.body_repeat) |_| {
            try body.appendSlice(alloc, token_text);
            try body.appendSlice(alloc, " body ");
        }
        try body.append(alloc, '"');
    }
    try body.append(alloc, '}');
    return try body.toOwnedSlice(alloc);
}

fn buildFilterPlans(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    doc_ids: []const []const u8,
    cfg: Config,
    generation: u64,
) ![]FilterPlan {
    const filters = try alloc.alloc(FilterPlan, cfg.queries);
    errdefer alloc.free(filters);
    for (filters) |*filter| filter.* = .{ .doc_ids = &.{}, .native_ids = &.{}, .ordinal_filter = .{} };

    for (filters, 0..) |*filter, q| {
        const count = @min(cfg.filter_size, doc_ids.len);
        const selected = try alloc.alloc([]const u8, count);
        errdefer alloc.free(selected);
        const start = (q * 131) % doc_ids.len;
        const step = 17;
        for (0..count) |i| {
            const doc_idx = (start + i * step) % doc_ids.len;
            selected[i] = doc_ids[doc_idx];
        }

        var txn = try db.core.store.beginProbeTxn();
        defer txn.abort();
        var include = try doc_identity.resolvedDocSetForIdsAtGenerationTxn(alloc, &txn, selected, generation);
        errdefer include.deinit(alloc);
        const native_ids = try sparseNativeIdsForResolvedSetAlloc(alloc, db, &include);
        errdefer alloc.free(native_ids);
        filter.* = .{
            .doc_ids = selected,
            .native_ids = native_ids,
            .ordinal_filter = .{
                .include = include,
            },
        };
    }
    return filters;
}

fn sparseNativeIdsForResolvedSetAlloc(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    set: *const doc_set.ResolvedDocSet,
) ![]const u64 {
    const ordinals = try ordinalsFromResolvedSetAlloc(alloc, set);
    defer alloc.free(ordinals);
    const doc_nums = try db.core.index_manager.lookupSparseDocNumsForOrdinalsAlloc(alloc, db.core.store, sparse_index_name, ordinals);
    defer alloc.free(doc_nums);
    const out = try alloc.alloc(u64, doc_nums.len);
    for (doc_nums, 0..) |doc_num, i| out[i] = doc_num;
    std.mem.sort(u64, out, {}, std.sort.asc(u64));
    return out;
}

fn ordinalsFromResolvedSetAlloc(alloc: std.mem.Allocator, set: *const doc_set.ResolvedDocSet) ![]const doc_set.DocOrdinal {
    return switch (set.*) {
        .ordinals => |ordinals| try alloc.dupe(doc_set.DocOrdinal, ordinals),
        .ordinal_bitmap => |*bitmap| blk: {
            var ordinals = std.ArrayListUnmanaged(doc_set.DocOrdinal).empty;
            errdefer ordinals.deinit(alloc);
            var iter = bitmap.iterator();
            while (iter.next()) |ordinal| try ordinals.append(alloc, ordinal);
            break :blk try ordinals.toOwnedSlice(alloc);
        },
        .none => try alloc.alloc(doc_set.DocOrdinal, 0),
        .all, .doc_keys => error.UnsupportedQueryRequest,
    };
}

fn runBench(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    cfg: Config,
    filters: []const FilterPlan,
    shape: QueryShape,
    mode: Mode,
    generation: u64,
    public_checksum: ?u64,
) !BenchResult {
    const before = try db.stats(alloc);
    defer db_mod.freeDBStats(alloc, before);
    const start = nanotime();
    var checksum: u64 = 0;
    var total_hits: u64 = 0;
    for (0..cfg.repeats) |repeat| {
        for (filters, 0..) |*filter, q| {
            var req = searchRequestForShape(cfg, shape, q + repeat);
            req.identity_read_generation = generation;
            switch (mode) {
                .public_ids => {
                    req.filter_doc_ids = filter.doc_ids;
                    req.filter_doc_ids_positive = true;
                },
                .ordinal_docset => {
                    req.resolved_doc_filter = &filter.ordinal_filter;
                    req.resolved_doc_filter_wire_context = .{
                        .namespace = doc_identity.default_namespace,
                        .identity_read_generation = generation,
                    };
                },
                .sparse_ids => {
                    if (shape != .sparse_filter) return error.InvalidArgument;
                    req.filter_ids = filter.native_ids;
                },
            }
            var result = try db.search(alloc, req);
            defer result.deinit();
            checksum = checksumResult(checksum, &result);
            total_hits += result.total_hits;
        }
    }
    const elapsed_ns = nanotime() - start;
    const after = try db.stats(alloc);
    defer db_mod.freeDBStats(alloc, after);
    return .{
        .shape = shape,
        .mode = mode,
        .repeats = cfg.repeats,
        .queries = filters.len,
        .filter_size = cfg.filter_size,
        .elapsed_ns = elapsed_ns,
        .checksum = checksum,
        .total_hits = total_hits,
        .resolved_set_delta = delta(after.doc_set_planning.resolved_set_count, before.doc_set_planning.resolved_set_count),
        .ordinal_list_delta = delta(after.doc_set_planning.ordinal_list_count, before.doc_set_planning.ordinal_list_count),
        .ordinal_bitmap_delta = delta(after.doc_set_planning.ordinal_bitmap_count, before.doc_set_planning.ordinal_bitmap_count),
        .doc_key_list_delta = delta(after.doc_set_planning.doc_key_list_count, before.doc_set_planning.doc_key_list_count),
        .missing_coverage_delta = delta(after.doc_set_planning.missing_ordinal_coverage_count, before.doc_set_planning.missing_ordinal_coverage_count),
        .matches_public = if (public_checksum) |expected| expected == checksum else true,
    };
}

fn searchRequestForShape(cfg: Config, shape: QueryShape, query_idx: usize) db_mod.types.SearchRequest {
    return switch (shape) {
        .match_all_filter => .{
            .query = .{ .match_all = {} },
            .limit = cfg.limit,
            .include_stored = false,
        },
        .full_text_filter => .{
            .index_name = full_text_index_name,
            .full_text = .{ .match = .{ .field = "title", .text = fullTextToken(query_idx) } },
            .limit = cfg.limit,
            .include_stored = false,
        },
        .sparse_filter => .{
            .index_name = sparse_index_name,
            .query = .{ .sparse_knn = .{
                .indices = sparseQueryIndices(query_idx, cfg.sparse_dims)[0..],
                .values = &.{1.0},
                .k = cfg.limit,
            } },
            .limit = cfg.limit,
            .include_stored = false,
        },
    };
}

fn fullTextToken(query_idx: usize) []const u8 {
    return switch (query_idx % 8) {
        0 => "token0",
        1 => "token1",
        2 => "token2",
        3 => "token3",
        4 => "token4",
        5 => "token5",
        6 => "token6",
        else => "token7",
    };
}

fn sparseQueryIndices(query_idx: usize, dims: usize) [1]u32 {
    return .{@intCast(query_idx % dims)};
}

fn checksumResult(seed: u64, result: *const db_mod.types.SearchResult) u64 {
    var hasher = std.hash.Wyhash.init(seed);
    var buf: [16]u8 = undefined;
    std.mem.writeInt(u64, buf[0..8], result.total_hits, .little);
    hasher.update(buf[0..8]);
    for (result.hits) |hit| {
        hasher.update(hit.id);
        const ordinal = hit.doc_ordinal orelse 0;
        std.mem.writeInt(u32, buf[0..4], ordinal, .little);
        hasher.update(buf[0..4]);
    }
    return hasher.final();
}

fn printResult(out: anytype, cfg: Config, result: BenchResult) !void {
    try out.print(
        "{{\"event\":\"docid_query_bench_result\",\"shape\":\"{s}\",\"mode\":\"{s}\",\"docs\":{d},\"queries\":{d},\"repeats\":{d},\"filter_size\":{d},\"limit\":{d},\"elapsed_ns\":{d},\"avg_ns\":{d},\"total_hits\":{d},\"checksum\":{d},\"matches_public\":{},\"resolved_set_delta\":{d},\"ordinal_list_delta\":{d},\"ordinal_bitmap_delta\":{d},\"doc_key_list_delta\":{d},\"missing_coverage_delta\":{d}}}\n",
        .{
            result.shape.label(),
            result.mode.label(),
            cfg.docs,
            result.queries,
            result.repeats,
            result.filter_size,
            cfg.limit,
            result.elapsed_ns,
            avgNs(result),
            result.total_hits,
            result.checksum,
            result.matches_public,
            result.resolved_set_delta,
            result.ordinal_list_delta,
            result.ordinal_bitmap_delta,
            result.doc_key_list_delta,
            result.missing_coverage_delta,
        },
    );
}

fn printSummary(out: anytype, cfg: Config, public_result: BenchResult, ordinal_result: BenchResult) !void {
    const public_avg_ns = avgNs(public_result);
    const ordinal_avg_ns = avgNs(ordinal_result);
    const checksums_match = public_result.checksum == ordinal_result.checksum;
    const total_hits_match = public_result.total_hits == ordinal_result.total_hits;
    const correctness_match = checksums_match and total_hits_match;
    try out.print(
        "{{\"event\":\"docid_query_bench_summary\",\"shape\":\"{s}\",\"docs\":{d},\"queries\":{d},\"repeats\":{d},\"filter_size\":{d},\"limit\":{d},\"public_avg_ns\":{d},\"ordinal_avg_ns\":{d},\"ordinal_vs_public_ratio\":{d:.6},\"public_total_hits\":{d},\"ordinal_total_hits\":{d},\"checksums_match\":{},\"total_hits_match\":{},\"correctness_match\":{},\"public_resolved_set_delta\":{d},\"ordinal_resolved_set_delta\":{d},\"public_doc_key_list_delta\":{d},\"ordinal_doc_key_list_delta\":{d}}}\n",
        .{
            public_result.shape.label(),
            cfg.docs,
            public_result.queries,
            public_result.repeats,
            public_result.filter_size,
            cfg.limit,
            public_avg_ns,
            ordinal_avg_ns,
            ordinalRatio(public_avg_ns, ordinal_avg_ns),
            public_result.total_hits,
            ordinal_result.total_hits,
            checksums_match,
            total_hits_match,
            correctness_match,
            public_result.resolved_set_delta,
            ordinal_result.resolved_set_delta,
            public_result.doc_key_list_delta,
            ordinal_result.doc_key_list_delta,
        },
    );
}

fn validateSummary(cfg: Config, public_result: BenchResult, ordinal_result: BenchResult) !void {
    if (cfg.require_correctness and (public_result.checksum != ordinal_result.checksum or public_result.total_hits != ordinal_result.total_hits)) {
        std.debug.print(
            "docid-query-bench correctness mismatch for {s}: public checksum={d} hits={d}, ordinal checksum={d} hits={d}\n",
            .{
                public_result.shape.label(),
                public_result.checksum,
                public_result.total_hits,
                ordinal_result.checksum,
                ordinal_result.total_hits,
            },
        );
        return error.BenchmarkCorrectnessMismatch;
    }
    if (cfg.require_public_resolution_delta and (public_result.resolved_set_delta == 0 or ordinal_result.resolved_set_delta != 0)) {
        std.debug.print(
            "docid-query-bench projection evidence missing for {s}: public resolved_set_delta={d}, ordinal resolved_set_delta={d}\n",
            .{ public_result.shape.label(), public_result.resolved_set_delta, ordinal_result.resolved_set_delta },
        );
        return error.BenchmarkProjectionEvidenceMissing;
    }
    if (cfg.max_ordinal_ratio) |max_ratio| {
        const public_avg_ns = avgNs(public_result);
        const ordinal_avg_ns = avgNs(ordinal_result);
        const actual_ratio = ordinalRatio(public_avg_ns, ordinal_avg_ns);
        if (actual_ratio > max_ratio) {
            std.debug.print(
                "docid-query-bench ordinal ratio exceeded for {s}: ratio={d:.6}, max={d:.6}, public_avg_ns={d}, ordinal_avg_ns={d}\n",
                .{ public_result.shape.label(), actual_ratio, max_ratio, public_avg_ns, ordinal_avg_ns },
            );
            return error.BenchmarkPerformanceRegression;
        }
    }
}

fn avgNs(result: BenchResult) u64 {
    const ops = result.repeats * result.queries;
    return if (ops == 0) 0 else result.elapsed_ns / ops;
}

fn ordinalRatio(public_avg_ns: u64, ordinal_avg_ns: u64) f64 {
    if (public_avg_ns == 0) return 0.0;
    return @as(f64, @floatFromInt(ordinal_avg_ns)) / @as(f64, @floatFromInt(public_avg_ns));
}

fn runSparseIdProjectionBench(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    doc_ids: []const []const u8,
    cfg: Config,
    generation: u64,
    filters: []const FilterPlan,
) !BenchResult {
    const candidates_by_dim = try buildSparseNativeCandidatesByDimAlloc(alloc, db, doc_ids, cfg, generation);
    defer {
        for (candidates_by_dim) |candidates| alloc.free(candidates);
        alloc.free(candidates_by_dim);
    }
    var elapsed_ns: u64 = 0;
    var checksum: u64 = 0;
    var total_hits: u64 = 0;
    for (0..cfg.repeats) |repeat| {
        for (filters, 0..) |filter, q| {
            const wanted_dim = (q + repeat) % cfg.sparse_dims;
            const candidates = candidates_by_dim[wanted_dim];
            const start = nanotime();
            const matched = intersectSortedU64Count(filter.native_ids, candidates);
            elapsed_ns += nanotime() - start;
            total_hits += matched;
            checksum = checksumSparseProjection(checksum, matched, filter.native_ids.len, candidates.len);
        }
    }
    return .{
        .shape = .sparse_filter,
        .mode = .sparse_ids,
        .repeats = cfg.repeats,
        .queries = filters.len,
        .filter_size = cfg.filter_size,
        .elapsed_ns = elapsed_ns,
        .checksum = checksum,
        .total_hits = total_hits,
        .resolved_set_delta = 0,
        .ordinal_list_delta = 0,
        .ordinal_bitmap_delta = 0,
        .doc_key_list_delta = 0,
        .missing_coverage_delta = 0,
        .matches_public = true,
    };
}

fn buildSparseNativeCandidatesByDimAlloc(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    doc_ids: []const []const u8,
    cfg: Config,
    generation: u64,
) ![]const []const u64 {
    const out = try alloc.alloc([]const u64, cfg.sparse_dims);
    errdefer alloc.free(out);
    @memset(out, &.{});
    for (out, 0..) |*slot, dim| {
        var selected = std.ArrayListUnmanaged([]const u8).empty;
        defer selected.deinit(alloc);
        for (doc_ids, 0..) |doc_id, doc_idx| {
            if (doc_idx % cfg.sparse_dims == dim) try selected.append(alloc, doc_id);
        }
        var txn = try db.core.store.beginProbeTxn();
        defer txn.abort();
        var set = try doc_identity.resolvedDocSetForIdsAtGenerationTxn(alloc, &txn, selected.items, generation);
        defer set.deinit(alloc);
        slot.* = try sparseNativeIdsForResolvedSetAlloc(alloc, db, &set);
    }
    return out;
}

fn printSparseProjectionResult(out: anytype, cfg: Config, result: BenchResult) !void {
    const ops = result.repeats * result.queries;
    const avg_ns = if (ops == 0) 0 else result.elapsed_ns / ops;
    try out.print(
        "{{\"event\":\"docid_query_bench_sparse_id_projection\",\"shape\":\"sparse_id_projection\",\"mode\":\"{s}\",\"docs\":{d},\"queries\":{d},\"repeats\":{d},\"filter_size\":{d},\"sparse_dims\":{d},\"elapsed_ns\":{d},\"avg_ns\":{d},\"total_matches\":{d},\"checksum\":{d}}}\n",
        .{
            result.mode.label(),
            cfg.docs,
            result.queries,
            result.repeats,
            result.filter_size,
            cfg.sparse_dims,
            result.elapsed_ns,
            avg_ns,
            result.total_hits,
            result.checksum,
        },
    );
}

fn intersectSortedU64Count(left: []const u64, right: []const u64) usize {
    var i: usize = 0;
    var j: usize = 0;
    var count: usize = 0;
    while (i < left.len and j < right.len) {
        if (left[i] == right[j]) {
            count += 1;
            i += 1;
            j += 1;
        } else if (left[i] < right[j]) {
            i += 1;
        } else {
            j += 1;
        }
    }
    return count;
}

fn checksumSparseProjection(seed: u64, matched: usize, filter_len: usize, candidate_len: usize) u64 {
    var hasher = std.hash.Wyhash.init(seed);
    var buf: [24]u8 = undefined;
    std.mem.writeInt(u64, buf[0..8], matched, .little);
    std.mem.writeInt(u64, buf[8..16], filter_len, .little);
    std.mem.writeInt(u64, buf[16..24], candidate_len, .little);
    hasher.update(&buf);
    return hasher.final();
}

fn freeDocIds(alloc: std.mem.Allocator, doc_ids: [][]u8) void {
    for (doc_ids) |doc_id| alloc.free(doc_id);
    alloc.free(doc_ids);
}

fn parseArgs(args_in: std.process.Args) !Config {
    var cfg = Config{};
    var args = std.process.Args.Iterator.init(args_in);
    _ = args.skip();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--docs")) {
            cfg.docs = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--queries")) {
            cfg.queries = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--repeats")) {
            cfg.repeats = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--filter-size")) {
            cfg.filter_size = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--batch-size")) {
            cfg.batch_size = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--sparse-dims")) {
            cfg.sparse_dims = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--limit")) {
            cfg.limit = @intCast(try parseNextUsize(&args, arg));
        } else if (std.mem.eql(u8, arg, "--body-repeat")) {
            cfg.body_repeat = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--allow-mismatch")) {
            cfg.require_correctness = false;
        } else if (std.mem.eql(u8, arg, "--max-ordinal-ratio")) {
            cfg.max_ordinal_ratio = try parseNextF64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--require-public-resolution-delta")) {
            cfg.require_public_resolution_delta = true;
        } else if (std.mem.eql(u8, arg, "--progress-every")) {
            cfg.progress_every = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--defer-full-index-load")) {
            cfg.defer_full_index_load = true;
        } else {
            std.debug.print("invalid argument: {s}\n", .{arg});
            return error.InvalidArgument;
        }
    }
    if (cfg.docs == 0 or cfg.queries == 0 or cfg.repeats == 0 or cfg.filter_size == 0 or cfg.sparse_dims == 0) return error.InvalidArgument;
    return cfg;
}

fn parseNextUsize(args: *std.process.Args.Iterator, flag: []const u8) !usize {
    const raw = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return try std.fmt.parseInt(usize, raw, 10);
}

fn parseNextF64(args: *std.process.Args.Iterator, flag: []const u8) !f64 {
    const raw = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return try std.fmt.parseFloat(f64, raw);
}

fn delta(after: u64, before: u64) u64 {
    return after -| before;
}

fn nanotime() u64 {
    return platform_time.monotonicNs();
}
