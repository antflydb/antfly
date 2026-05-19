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
const antfly = @import("antfly-zig");

const db_mod = antfly.db;
const db_types = db_mod.types;
const metadata_api = antfly.metadata_api;
const metadata_mod = antfly.metadata;
const metadata_table_manager = antfly.metadata.table_manager;
const metadata_transition_state = antfly.metadata.transition_state;
const public_api = antfly.public_api;
const raft_mod = antfly.raft;
const raft_reconciler = antfly.raft.reconciler;

const group_id: u64 = 7001;
const table_id: u64 = 7;
const node_id: u64 = 9;
const store_id: u64 = 19;
const table_name = "docs";

const BenchCatalogState = struct {
    dims: usize,
    table: metadata_table_manager.TableRecord,
    range: metadata_table_manager.RangeRecord,
    store: metadata_table_manager.StoreRecord,
    peer_node_ids: [1]u64,
    placement_intent: raft_reconciler.PlacementIntent,

    fn init(dims: usize) BenchCatalogState {
        var state = BenchCatalogState{
            .dims = dims,
            .table = .{
                .table_id = table_id,
                .name = table_name,
                .description = "provisioned dense ingest bench",
                .schema_json = "",
                .read_schema_json = "",
                .indexes_json = undefined,
                .replication_sources_json = "[]",
                .placement_role = "data",
            },
            .range = .{
                .group_id = group_id,
                .table_id = table_id,
                .start_key = "",
                .end_key = null,
            },
            .store = .{
                .store_id = store_id,
                .node_id = node_id,
                .role = "data",
                .live = true,
                .health_class = "healthy",
            },
            .peer_node_ids = .{node_id},
            .placement_intent = undefined,
        };
        state.table.indexes_json = switch (dims) {
            1536 => "{\"dense_idx\":{\"type\":\"embeddings\",\"dimension\":1536,\"distance_metric\":\"l2_squared\",\"external\":true}}",
            768 => "{\"dense_idx\":{\"type\":\"embeddings\",\"dimension\":768,\"distance_metric\":\"l2_squared\",\"external\":true}}",
            else => "",
        };
        state.placement_intent = .{
            .record = .{ .group_id = group_id, .replica_id = 1, .local_node_id = node_id },
            .store_id = store_id,
            .peer_node_ids = state.peer_node_ids[0..],
        };
        return state;
    }
};

const InputDoc = struct {
    key: []const u8,
    value: []const u8,
};

const ManagedDbHandle = union(enum) {
    leased: public_api.ProvisionedTableWriteCache.CachedDb,
    direct: db_mod.DB,

    fn dbPtr(self: *ManagedDbHandle) *db_mod.DB {
        return switch (self.*) {
            .leased => |*cached| cached.db,
            .direct => |*db| db,
        };
    }

    fn deinit(self: *ManagedDbHandle, alloc: std.mem.Allocator) void {
        switch (self.*) {
            .leased => |*cached| cached.deinit(alloc),
            .direct => |*db| db.close(),
        }
    }
};

const Config = struct {
    docs: usize = 50000,
    dims: usize = 1536,
    batch_size: usize = 100,
    seed: u64 = 42,
    hold_before_final_drain_ms: u64 = 0,
    sync_level: db_types.SyncLevel = .write,
};

const Summary = struct {
    docs: usize = 0,
    dims: usize = 0,
    batch_size: usize = 0,
    batches: usize = 0,
    write_ns: u64 = 0,
    max_batch_ns: u64 = 0,
    bulk_finish_ns: u64 = 0,
    final_drain_ns: u64 = 0,
    cached_write_dbs: usize = 0,
    write_cache_hits: u64 = 0,
    write_cache_misses: u64 = 0,
    dense_lsm_total_runs: u64 = 0,
    dense_lsm_total_run_bytes: u64 = 0,
    dense_lsm_l0_runs: u64 = 0,
    dense_lsm_l0_bytes: u64 = 0,
    hbc_quant_value_bytes: u64 = 0,
    hbc_insert_find_leaf_ns: u64 = 0,
    hbc_insert_mutate_leaf_ns: u64 = 0,
    hbc_insert_commit_ns: u64 = 0,
    hbc_refresh_quantized_ns: u64 = 0,
    async_begin_calls: u64 = 0,
    async_finish_calls: u64 = 0,
    async_flush_calls: u64 = 0,
    async_flush_ns: u64 = 0,
    lsm_total_runs: u64 = 0,
    lsm_l0_runs: u64 = 0,
    lsm_total_run_bytes: u64 = 0,

    fn writeNsPerDoc(self: Summary) u64 {
        if (self.docs == 0) return 0;
        return self.write_ns / self.docs;
    }
};

pub fn main(init: std.process.Init) !void {
    const alloc = std.heap.c_allocator;
    const cfg = try parseArgs(init.minimal.args);

    var path_buf: [256]u8 = undefined;
    const replica_root = tempPath(&path_buf);
    defer cleanupTempDir(replica_root);

    const summary = try runProvisionedDenseIngest(alloc, std.mem.span(replica_root), cfg);
    printSummary(cfg, summary);
}

fn parseArgs(args_in: std.process.Args) !Config {
    var cfg = Config{};
    var args = std.process.Args.Iterator.init(args_in);
    _ = args.skip();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--docs")) {
            cfg.docs = try parseNextUsize(&args, "--docs");
        } else if (std.mem.eql(u8, arg, "--dims")) {
            cfg.dims = try parseNextUsize(&args, "--dims");
        } else if (std.mem.eql(u8, arg, "--batch-size")) {
            cfg.batch_size = try parseNextUsize(&args, "--batch-size");
        } else if (std.mem.eql(u8, arg, "--seed")) {
            cfg.seed = try parseNextU64(&args, "--seed");
        } else if (std.mem.eql(u8, arg, "--hold-before-final-drain-ms")) {
            cfg.hold_before_final_drain_ms = try parseNextU64(&args, "--hold-before-final-drain-ms");
        } else if (std.mem.eql(u8, arg, "--sync-level")) {
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.sync_level = db_types.parsePublicSyncLevelText(raw) orelse return error.InvalidArgument;
        } else {
            return error.InvalidArgument;
        }
    }
    if (cfg.docs == 0 or cfg.dims == 0 or cfg.batch_size == 0) return error.InvalidArgument;
    return cfg;
}

fn runProvisionedDenseIngest(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    cfg: Config,
) !Summary {
    var storage = public_api.ProvisionedGroupStorage.init(alloc);
    defer storage.deinit();

    var catalog_state = BenchCatalogState.init(cfg.dims);
    const catalog = BenchCatalog.iface(&catalog_state);

    var write_source = public_api.ProvisionedTableWriteSource.init(replica_root_dir, catalog);
    defer write_source.deinit();
    var read_source = public_api.ProvisionedTableReadSource.init(
        replica_root_dir,
        catalog,
        raft_mod.read_gate.noopReadableLeaseRequester(),
    );
    storage.attachSources(&read_source, &write_source);

    const resolved_group = try public_api.table_catalog.resolveSingleRangeGroup(alloc, catalog, table_name);
    if (resolved_group == null) {
        std.debug.print("provisioned_dense_ingest_guardrail catalog_single_range_missing\n", .{});
        return error.MissingCatalogRange;
    }
    const first_doc_key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{0});
    defer alloc.free(first_doc_key);
    const resolved_key_group = try public_api.table_catalog.resolveGroupForKey(alloc, catalog, table_name, first_doc_key);
    if (resolved_key_group == null) {
        std.debug.print("provisioned_dense_ingest_guardrail catalog_key_route_missing key={s}\n", .{first_doc_key});
        return error.MissingCatalogKeyRoute;
    }

    const table_source = write_source.source();

    var summary: Summary = .{
        .docs = cfg.docs,
        .dims = cfg.dims,
        .batch_size = cfg.batch_size,
    };

    const writes_buf = try alloc.alloc(db_types.BatchWrite, cfg.batch_size);
    defer alloc.free(writes_buf);
    const batch_docs = try alloc.alloc(InputDoc, cfg.batch_size);
    defer {
        freeInputDocs(alloc, batch_docs);
    }
    for (batch_docs) |*doc| doc.* = .{ .key = &.{}, .value = &.{} };
    const vector_scratch = try alloc.alloc(f32, cfg.dims);
    defer alloc.free(vector_scratch);

    var start: usize = 0;
    while (start < cfg.docs) : (start += cfg.batch_size) {
        const end = @min(start + cfg.batch_size, cfg.docs);
        const writes = writes_buf[0 .. end - start];
        const docs = batch_docs[0 .. end - start];
        defer {
            for (docs) |*doc| {
                if (doc.key.len > 0) alloc.free(doc.key);
                if (doc.value.len > 0) alloc.free(doc.value);
                doc.* = .{ .key = &.{}, .value = &.{} };
            }
        }
        for (start..end, 0..) |doc_idx, i| {
            fillVector(vector_scratch, cfg.seed, doc_idx);
            docs[i] = .{
                .key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{doc_idx}),
                .value = try encodeVectorDocJson(alloc, vector_scratch, doc_idx),
            };
            writes[i] = .{
                .key = docs[i].key,
                .value = docs[i].value,
            };
        }

        const started = nowNs();
        _ = (table_source.batch(alloc, table_name, .{
            .writes = writes,
            .sync_level = cfg.sync_level,
        }) catch |err| {
            std.debug.print("provisioned_dense_ingest_guardrail batch_failed batch={d} start={d} end={d} err={s}\n", .{
                summary.batches,
                start,
                end,
                @errorName(err),
            });
            return err;
        }) orelse {
            std.debug.print("provisioned_dense_ingest_guardrail batch_unhandled batch={d} start={d} end={d}\n", .{
                summary.batches,
                start,
                end,
            });
            return error.UnhandledProvisionedBatch;
        };
        const wall_ns = elapsedSince(started);
        summary.write_ns += wall_ns;
        summary.max_batch_ns = @max(summary.max_batch_ns, wall_ns);
        summary.batches += 1;
    }

    if (cfg.hold_before_final_drain_ms > 0) sleepMs(cfg.hold_before_final_drain_ms);

    const finish_started = nowNs();
    _ = (table_source.finishBulkIngest(alloc, table_name, .{
        .compact = false,
        .max_deferred_l0_runs = 64,
    }) catch |err| {
        std.debug.print("provisioned_dense_ingest_guardrail finish_bulk_failed err={s}\n", .{@errorName(err)});
        return err;
    }) orelse {
        std.debug.print("provisioned_dense_ingest_guardrail finish_bulk_unhandled\n", .{});
        return error.UnhandledProvisionedFinish;
    };
    summary.bulk_finish_ns = elapsedSince(finish_started);

    var managed = leaseOrReopenManagedWriter(alloc, &storage, &write_source, cfg.dims) catch |err| {
        std.debug.print("provisioned_dense_ingest_guardrail lease_reopen_failed err={s}\n", .{@errorName(err)});
        return err;
    };
    defer managed.deinit(alloc);

    const drain_started = nowNs();
    managed.dbPtr().runUntilIdle() catch |err| {
        std.debug.print("provisioned_dense_ingest_guardrail drain_failed err={s}\n", .{@errorName(err)});
        return err;
    };
    summary.final_drain_ns = elapsedSince(drain_started);

    const write_cache_stats = storage.write_cache.cacheStats();
    summary.write_cache_hits = write_cache_stats.hit_count;
    summary.write_cache_misses = write_cache_stats.miss_count;
    summary.cached_write_dbs = write_source.cachedWriteDbCount();

    if (managed.dbPtr().core.index_manager.denseIndex("dense_idx")) |entry| {
        const profile = entry.index.getWriteProfile();
        summary.hbc_quant_value_bytes = profile.ns_quant_value_bytes;
        summary.hbc_insert_find_leaf_ns = profile.insert_find_leaf_ns;
        summary.hbc_insert_mutate_leaf_ns = profile.insert_mutate_leaf_ns;
        summary.hbc_insert_commit_ns = profile.insert_commit_ns;
        summary.hbc_refresh_quantized_ns = profile.refresh_quantized_ns;
        if (entry.index.snapshotLsmMaintenanceStats()) |stats| {
            summary.dense_lsm_total_runs = stats.total_runs;
            summary.dense_lsm_total_run_bytes = stats.total_run_bytes;
            summary.dense_lsm_l0_runs = stats.l0_runs;
            summary.dense_lsm_l0_bytes = stats.l0_bytes;
        }
    }

    const async_stats = write_source.asyncIndexingStats();
    summary.async_begin_calls = async_stats.dense_catch_up.begin_calls;
    summary.async_finish_calls = async_stats.dense_catch_up.finish_calls;
    summary.async_flush_calls = async_stats.applied_sequence.flush_calls;
    summary.async_flush_ns = async_stats.applied_sequence.flush_ns;

    const lsm_stats = write_source.lsmMaintenanceStats();
    summary.lsm_total_runs = lsm_stats.total_runs;
    summary.lsm_l0_runs = lsm_stats.l0_runs;
    summary.lsm_total_run_bytes = lsm_stats.total_run_bytes;

    return summary;
}

fn waitForManagedWriterLease(source: *public_api.ProvisionedTableWriteSource) !public_api.ProvisionedTableWriteSource.ManagedWriterGroupProbe {
    const deadline = nowNs() + 5 * std.time.ns_per_s;
    while (true) {
        const probe = source.probeManagedWriterGroupBestEffort(table_name, group_id);
        switch (probe) {
            .leased, .absent => return probe,
            .unknown => {},
        }
        if (nowNs() >= deadline) return error.Timeout;
        sleepMs(1);
    }
}

fn leaseOrReopenManagedWriter(
    alloc: std.mem.Allocator,
    storage: *public_api.ProvisionedGroupStorage,
    source: *public_api.ProvisionedTableWriteSource,
    dims: usize,
) !ManagedDbHandle {
    const probe = try waitForManagedWriterLease(source);
    switch (probe) {
        .leased => |owned| return .{ .leased = owned },
        .unknown => return error.Timeout,
        .absent => {},
    }

    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, source.replica_root_dir, group_id);
    defer alloc.free(path);
    _ = storage;
    _ = dims;
    return .{ .direct = try db_mod.DB.open(alloc, path, .{}) };
}

const BenchCatalog = struct {
    fn iface(state: *BenchCatalogState) public_api.table_catalog.CatalogSource {
        return .{
            .ptr = state,
            .vtable = &.{
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
            },
        };
    }

    fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
        const state: *BenchCatalogState = @ptrCast(@alignCast(ptr));
        if (state.table.indexes_json.len == 0) return error.InvalidArgument;
        return .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = @constCast((@as(*[1]metadata_table_manager.TableRecord, @ptrCast(&state.table)))[0..]),
            .ranges = @constCast((@as(*[1]metadata_table_manager.RangeRecord, @ptrCast(&state.range)))[0..]),
            .stores = @constCast((@as(*[1]metadata_table_manager.StoreRecord, @ptrCast(&state.store)))[0..]),
            .placement_intents = @constCast((@as(*[1]raft_reconciler.PlacementIntent, @ptrCast(&state.placement_intent)))[0..]),
            .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
            .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
        };
    }

    fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
};

fn parseNextUsize(args: anytype, flag: []const u8) !usize {
    const raw = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return try std.fmt.parseInt(usize, raw, 10);
}

fn parseNextU64(args: anytype, flag: []const u8) !u64 {
    const raw = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return try std.fmt.parseInt(u64, raw, 10);
}

fn freeInputDocs(alloc: std.mem.Allocator, docs: []InputDoc) void {
    for (docs) |doc| {
        if (doc.key.len > 0) alloc.free(doc.key);
        if (doc.value.len > 0) alloc.free(doc.value);
    }
    alloc.free(docs);
}

fn fillVector(vector: []f32, seed: u64, doc_idx: usize) void {
    const cluster = @as(f32, @floatFromInt(doc_idx % 8)) * 0.25;
    for (vector, 0..) |*value, dim_idx| {
        value.* = cluster + deterministicNoise(seed, doc_idx, dim_idx);
    }
    normalizeInPlace(vector);
}

fn encodeVectorDocJson(alloc: std.mem.Allocator, vector: []const f32, doc_idx: usize) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"title\":\"doc ");
    var title_buf: [32]u8 = undefined;
    const title = try std.fmt.bufPrint(&title_buf, "{d}", .{doc_idx});
    try out.appendSlice(alloc, title);
    try out.appendSlice(alloc, "\",\"_embeddings\":{\"dense_idx\":[");
    for (vector, 0..) |value, i| {
        if (i != 0) try out.append(alloc, ',');
        var num_buf: [32]u8 = undefined;
        const rendered = try std.fmt.bufPrint(&num_buf, "{d}", .{value});
        try out.appendSlice(alloc, rendered);
    }
    try out.appendSlice(alloc, "]}}");
    return out.toOwnedSlice(alloc);
}

fn printSummary(cfg: Config, summary: Summary) void {
    std.debug.print(
        "provisioned_dense_ingest_guardrail docs={d} dims={d} batch_size={d} sync={s} write_ns_per_doc={d} total_write_ms={d} bulk_finish_ms={d} final_drain_ms={d} cached_write_dbs={d} write_cache_hits={d} write_cache_misses={d} l0_runs={d} total_run_mb={d} dense_l0_runs={d} dense_total_run_mb={d}\n",
        .{
            summary.docs,
            summary.dims,
            summary.batch_size,
            db_types.publicSyncLevelText(cfg.sync_level),
            summary.writeNsPerDoc(),
            @divTrunc(summary.write_ns, std.time.ns_per_ms),
            @divTrunc(summary.bulk_finish_ns, std.time.ns_per_ms),
            @divTrunc(summary.final_drain_ns, std.time.ns_per_ms),
            summary.cached_write_dbs,
            summary.write_cache_hits,
            summary.write_cache_misses,
            summary.lsm_l0_runs,
            @divTrunc(summary.lsm_total_run_bytes, 1024 * 1024),
            summary.dense_lsm_l0_runs,
            @divTrunc(summary.dense_lsm_total_run_bytes, 1024 * 1024),
        },
    );
    std.debug.print(
        "  async begin={d} finish={d} flush_calls={d} flush_ms={d}\n",
        .{
            summary.async_begin_calls,
            summary.async_finish_calls,
            summary.async_flush_calls,
            @divTrunc(summary.async_flush_ns, std.time.ns_per_ms),
        },
    );
    std.debug.print(
        "  hbc_ms find_leaf={d} mutate_leaf={d} commit={d} refresh_quantized={d} hbc_quant_mb={d}\n",
        .{
            @divTrunc(summary.hbc_insert_find_leaf_ns, std.time.ns_per_ms),
            @divTrunc(summary.hbc_insert_mutate_leaf_ns, std.time.ns_per_ms),
            @divTrunc(summary.hbc_insert_commit_ns, std.time.ns_per_ms),
            @divTrunc(summary.hbc_refresh_quantized_ns, std.time.ns_per_ms),
            @divTrunc(summary.hbc_quant_value_bytes, 1024 * 1024),
        },
    );
}

fn deterministicNoise(seed: u64, doc_idx: usize, dim_idx: usize) f32 {
    var x = seed ^
        (@as(u64, @intCast(doc_idx + 1)) *% 0x9E3779B97F4A7C15) ^
        (@as(u64, @intCast(dim_idx + 1)) *% 0xC2B2AE3D27D4EB4F);
    x ^= x >> 33;
    x *%= 0xFF51AFD7ED558CCD;
    x ^= x >> 33;
    x *%= 0xC4CEB9FE1A85EC53;
    x ^= x >> 33;
    const scaled = @as(f32, @floatFromInt(x & 1023)) / 1024.0;
    return scaled * 0.01;
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

fn normalizeInPlace(vec: []f32) void {
    var sum_sq: f64 = 0;
    for (vec) |value| sum_sq += @as(f64, value) * @as(f64, value);
    if (sum_sq == 0) return;
    const inv = @as(f32, @floatCast(1.0 / std.math.sqrt(sum_sq)));
    for (vec) |*value| value.* *= inv;
}

fn sleepMs(duration_ms: u64) void {
    if (duration_ms == 0) return;
    const deadline = nowNs() +| (duration_ms * std.time.ns_per_ms);
    while (nowNs() < deadline) {
        std.Thread.yield() catch {};
    }
}

fn tempPath(buf: []u8) [*:0]const u8 {
    const path_bytes = std.fmt.bufPrint(buf, "/tmp/antfly-provisioned-dense-ingest-{d}\x00", .{nowNs()}) catch unreachable;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().createDirPath(io_impl.io(), std.mem.span(@as([*:0]const u8, @ptrCast(path_bytes.ptr)))) catch unreachable;
    return @ptrCast(path_bytes.ptr);
}

fn cleanupTempDir(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

fn nowNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => {},
        else => unreachable,
    }
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}

fn elapsedSince(start_ns: u64) u64 {
    return nowNs() - start_ns;
}
