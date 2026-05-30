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
const capi_mod = @import("antfly_capi");
const capi_db = capi_mod.db;
const capi = capi_mod.types;
const platform_time = antfly.platform_time;
const resource_manager_mod = antfly.resource_manager;

const db_mod = antfly.db;

const Workload = enum {
    explicit_dense,
    generated_dense,
    generated_chunked_dense,
};

const InputDoc = struct {
    key: []const u8,
    value: []const u8,
    body: []const u8,
};

const Config = struct {
    docs: usize = 2048,
    dims: usize = 128,
    queries: usize = 25,
    k: usize = 10,
    repeats: usize = 10,
    batch_size: usize = 128,
    seed: u64 = 42,
    profile_path: ?[]const u8 = null,
    inline_derived: bool = false,
    bulk_session: bool = false,
    ingest_only: bool = false,
    final_drain: bool = true,
    status_probe_every: usize = 0,
    maintenance_every: usize = 0,
    maintenance_steps: usize = 1,
    max_write_ns_per_doc: ?u64 = null,
    max_status_probe_ns: ?u64 = null,
    max_dense_lsm_run_bytes: ?u64 = null,
    max_dense_l0_runs: ?u64 = null,
    max_hbc_quant_value_bytes: ?u64 = null,
    sync_level: db_mod.types.SyncLevel = .full_index,
    workload: Workload = .explicit_dense,
    search_threads: usize = 1,
};

const SliceSummary = struct {
    used_bytes: u64 = 0,
    peak_bytes: u64 = 0,
    soft_limit_events: u64 = 0,
    hard_limit_rejections: u64 = 0,
};

const ResourceSummary = struct {
    lsm_block_table_cache: SliceSummary = .{},
    lsm_in_memory_state: SliceSummary = .{},
    hbc_node_metadata_cache: SliceSummary = .{},
    dense_search_working_set: SliceSummary = .{},
    dense_apply_working_set: SliceSummary = .{},
    dense_routing_working_set: SliceSummary = .{},
};

const Result = struct {
    db_search_ns: u64,
    db_search_concurrent_ns: u64,
    capi_packed_ns: u64,
    capi_wire_ns: u64,
    resources: ResourceSummary,
};

const IngestSummary = struct {
    docs: usize = 0,
    dims: usize = 0,
    batch_size: usize = 0,
    batches: usize = 0,
    write_ns: u64 = 0,
    max_batch_ns: u64 = 0,
    final_drain_ns: u64 = 0,
    maintenance_ns: u64 = 0,
    maintenance_steps: usize = 0,
    status_probe_count: usize = 0,
    status_probe_ns: u64 = 0,
    status_probe_max_ns: u64 = 0,
    dense_lsm_total_runs: u64 = 0,
    dense_lsm_total_run_bytes: u64 = 0,
    dense_lsm_l0_runs: u64 = 0,
    dense_lsm_l0_bytes: u64 = 0,
    dense_lsm_obsolete_paths: u64 = 0,
    hbc_insert_calls: u64 = 0,
    hbc_grouped_items: u64 = 0,
    hbc_grouped_fallback_items: u64 = 0,
    hbc_grouped_leaf_groups: u64 = 0,
    hbc_grouped_recursive_splits: u64 = 0,
    hbc_quant_value_bytes: u64 = 0,
    hbc_vecs_value_bytes: u64 = 0,
    hbc_nodes_value_bytes: u64 = 0,
    hbc_meta_value_bytes: u64 = 0,
    hbc_insert_find_leaf_ns: u64 = 0,
    hbc_insert_mutate_leaf_ns: u64 = 0,
    hbc_insert_commit_ns: u64 = 0,
    hbc_refresh_quantized_ns: u64 = 0,
    resolve_transforms_ns: u64 = 0,
    merge_effective_req_ns: u64 = 0,
    predicates_ns: u64 = 0,
    validate_range_ns: u64 = 0,
    extract_writes_ns: u64 = 0,
    delete_artifacts_ns: u64 = 0,
    precompute_generated_ns: u64 = 0,
    store_write_ns: u64 = 0,
    split_delta_ns: u64 = 0,
    build_derived_ns: u64 = 0,
    apply_shadow_ns: u64 = 0,
    collect_sync_targets_ns: u64 = 0,
    append_derived_log_ns: u64 = 0,
    wait_sync_ns: u64 = 0,
    backlog_pressure_ns: u64 = 0,
    executor_notify_ns: u64 = 0,
    derived_apply_ns: u64 = 0,
    sync_wait_ns: u64 = 0,
    full_text_apply_ns: u64 = 0,
    dense_apply_ns: u64 = 0,
    dense_delete_ns: u64 = 0,
    dense_doc_index_ns: u64 = 0,
    dense_embedding_apply_ns: u64 = 0,
    sparse_apply_ns: u64 = 0,
    graph_apply_ns: u64 = 0,
    index_sync_ns: u64 = 0,
    applied_sequence_save_ns: u64 = 0,
    derived_log_truncate_ns: u64 = 0,
    notify_enrichment_ns: u64 = 0,
    derived_log_append_calls: u64 = 0,
    derived_log_logical_entries: u64 = 0,
    derived_log_physical_commits: u64 = 0,
    derived_log_grouped_commits: u64 = 0,
    derived_log_grouped_requests: u64 = 0,
    enrichment_target_sequence: u64 = 0,
    enrichment_applied_sequence: u64 = 0,
    enrichment_processed_requests: u64 = 0,
    enrichment_skip_by_hash_count: u64 = 0,
    enrichment_codec_decode_failures: u64 = 0,
    enrichment_artifact_bytes_written: u64 = 0,
    enrichment_dense_artifact_bytes_written: u64 = 0,
    enrichment_sparse_artifact_bytes_written: u64 = 0,
    enrichment_chunk_artifact_bytes_written: u64 = 0,

    fn writeNsPerDoc(self: IngestSummary) u64 {
        if (self.docs == 0) return 0;
        return self.write_ns / self.docs;
    }
};

pub fn main(init: std.process.Init) !void {
    const alloc = std.heap.c_allocator;
    const cfg = try parseArgs(init.minimal.args);
    const dataset = if (cfg.workload == .explicit_dense) try makeDataset(alloc, cfg) else try alloc.alloc(f32, 0);
    const queries = try makeQueries(alloc, dataset, cfg);
    defer alloc.free(queries);

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    {
        var input_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer input_arena.deinit();
        const input_docs = try makeInputDocs(input_arena.allocator(), dataset, cfg);

        if (cfg.profile_path) |profile_path| {
            var file = if (std.fs.path.isAbsolute(profile_path))
                try std.Io.Dir.createFileAbsolute(init.io, profile_path, .{ .truncate = true })
            else
                try std.Io.Dir.cwd().createFile(init.io, profile_path, .{ .truncate = true });
            defer file.close(init.io);

            var profile_buffer: [8192]u8 = undefined;
            var profile_writer = file.writer(init.io, &profile_buffer);
            const summary = try seedDenseDB(alloc, std.mem.span(path), cfg, input_docs, &profile_writer.interface);
            try writeIngestSummaryJson(&profile_writer.interface, cfg, summary);
            try enforceGuardrails(cfg, summary);
            try profile_writer.end();
        } else {
            const summary = try seedDenseDB(alloc, std.mem.span(path), cfg, input_docs, null);
            try printIngestSummary(init.io, cfg, summary);
            try enforceGuardrails(cfg, summary);
        }
    }
    alloc.free(dataset);
    if (cfg.ingest_only) return;
    const result = try runBench(alloc, std.mem.span(path), cfg, queries);

    std.debug.print(
        "dense_stack docs={d} dims={d} queries={d} k={d} repeats={d} search_threads={d} db={d:.3}us db_concurrent={d:.3}us capi_packed={d:.3}us capi_wire={d:.3}us rm_dense_search_peak_mb={d:.2} rm_hbc_cache_peak_mb={d:.2} rm_lsm_state_peak_mb={d:.2}\n",
        .{
            cfg.docs,
            cfg.dims,
            cfg.queries,
            cfg.k,
            cfg.repeats,
            cfg.search_threads,
            @as(f64, @floatFromInt(result.db_search_ns)) / 1e3,
            @as(f64, @floatFromInt(result.db_search_concurrent_ns)) / 1e3,
            @as(f64, @floatFromInt(result.capi_packed_ns)) / 1e3,
            @as(f64, @floatFromInt(result.capi_wire_ns)) / 1e3,
            bytesToMiB(result.resources.dense_search_working_set.peak_bytes),
            bytesToMiB(result.resources.hbc_node_metadata_cache.peak_bytes),
            bytesToMiB(result.resources.lsm_in_memory_state.peak_bytes),
        },
    );
}

fn parseArgs(args_in: std.process.Args) !Config {
    var cfg = Config{};
    var saw_bulk_session = false;
    var args = std.process.Args.Iterator.init(args_in);
    _ = args.skip();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--docs")) {
            cfg.docs = try parseNextUsize(&args, "--docs");
        } else if (std.mem.eql(u8, arg, "--dims")) {
            cfg.dims = try parseNextUsize(&args, "--dims");
        } else if (std.mem.eql(u8, arg, "--queries")) {
            cfg.queries = try parseNextUsize(&args, "--queries");
        } else if (std.mem.eql(u8, arg, "--k")) {
            cfg.k = try parseNextUsize(&args, "--k");
        } else if (std.mem.eql(u8, arg, "--repeats")) {
            cfg.repeats = try parseNextUsize(&args, "--repeats");
        } else if (std.mem.eql(u8, arg, "--batch-size")) {
            cfg.batch_size = try parseNextUsize(&args, "--batch-size");
        } else if (std.mem.eql(u8, arg, "--seed")) {
            cfg.seed = try parseNextU64(&args, "--seed");
        } else if (std.mem.eql(u8, arg, "--profile")) {
            cfg.profile_path = args.next() orelse {
                std.debug.print("missing value for --profile\n", .{});
                return error.InvalidArgument;
            };
        } else if (std.mem.eql(u8, arg, "--inline-derived")) {
            cfg.inline_derived = true;
        } else if (std.mem.eql(u8, arg, "--bulk-session")) {
            cfg.bulk_session = true;
            saw_bulk_session = true;
        } else if (std.mem.eql(u8, arg, "--workload")) {
            const raw = args.next() orelse {
                std.debug.print("missing value for --workload\n", .{});
                return error.InvalidArgument;
            };
            cfg.workload = parseWorkload(raw) orelse {
                std.debug.print("invalid --workload value: {s}\n", .{raw});
                return error.InvalidArgument;
            };
        } else if (std.mem.eql(u8, arg, "--ingest-only")) {
            cfg.ingest_only = true;
        } else if (std.mem.eql(u8, arg, "--no-final-drain")) {
            cfg.final_drain = false;
        } else if (std.mem.eql(u8, arg, "--status-probe-every")) {
            cfg.status_probe_every = try parseNextUsize(&args, "--status-probe-every");
        } else if (std.mem.eql(u8, arg, "--maintenance-every")) {
            cfg.maintenance_every = try parseNextUsize(&args, "--maintenance-every");
        } else if (std.mem.eql(u8, arg, "--maintenance-steps")) {
            cfg.maintenance_steps = try parseNextUsize(&args, "--maintenance-steps");
        } else if (std.mem.eql(u8, arg, "--max-write-ns-per-doc")) {
            cfg.max_write_ns_per_doc = try parseNextU64(&args, "--max-write-ns-per-doc");
        } else if (std.mem.eql(u8, arg, "--max-status-probe-ns")) {
            cfg.max_status_probe_ns = try parseNextU64(&args, "--max-status-probe-ns");
        } else if (std.mem.eql(u8, arg, "--max-dense-lsm-run-bytes")) {
            cfg.max_dense_lsm_run_bytes = try parseNextU64(&args, "--max-dense-lsm-run-bytes");
        } else if (std.mem.eql(u8, arg, "--max-dense-l0-runs")) {
            cfg.max_dense_l0_runs = try parseNextU64(&args, "--max-dense-l0-runs");
        } else if (std.mem.eql(u8, arg, "--max-hbc-quant-value-bytes")) {
            cfg.max_hbc_quant_value_bytes = try parseNextU64(&args, "--max-hbc-quant-value-bytes");
        } else if (std.mem.eql(u8, arg, "--sync-level")) {
            const raw = args.next() orelse {
                std.debug.print("missing value for --sync-level\n", .{});
                return error.InvalidArgument;
            };
            cfg.sync_level = db_mod.types.parsePublicSyncLevelText(raw) orelse {
                std.debug.print("invalid --sync-level value: {s}\n", .{raw});
                return error.InvalidArgument;
            };
        } else if (std.mem.eql(u8, arg, "--search-threads")) {
            cfg.search_threads = try parseNextUsize(&args, "--search-threads");
        } else {
            return error.InvalidArgument;
        }
    }
    if (cfg.docs == 0 or cfg.dims == 0 or cfg.queries == 0 or cfg.k == 0 or cfg.repeats == 0 or cfg.batch_size == 0 or cfg.search_threads == 0) {
        return error.InvalidArgument;
    }
    if (!saw_bulk_session and cfg.workload != .explicit_dense) {
        cfg.bulk_session = true;
    }
    return cfg;
}

fn parseWorkload(raw: []const u8) ?Workload {
    if (std.mem.eql(u8, raw, "explicit_dense")) return .explicit_dense;
    if (std.mem.eql(u8, raw, "generated_dense")) return .generated_dense;
    if (std.mem.eql(u8, raw, "generated_chunked_dense")) return .generated_chunked_dense;
    return null;
}

fn parseNextUsize(args: *std.process.Args.Iterator, flag: []const u8) !usize {
    const raw = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return try std.fmt.parseInt(usize, raw, 10);
}

fn parseNextU64(args: *std.process.Args.Iterator, flag: []const u8) !u64 {
    const raw = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return try std.fmt.parseInt(u64, raw, 10);
}

fn makeDataset(alloc: std.mem.Allocator, cfg: Config) ![]f32 {
    const data = try alloc.alloc(f32, cfg.docs * cfg.dims);
    for (0..cfg.docs) |doc_idx| {
        const cluster = @as(f32, @floatFromInt(doc_idx % 8)) * 0.25;
        for (0..cfg.dims) |dim_idx| {
            data[doc_idx * cfg.dims + dim_idx] = cluster + deterministicNoise(cfg.seed, doc_idx, dim_idx);
        }
        _ = antfly.vector.normalize(data[doc_idx * cfg.dims ..][0..cfg.dims]);
    }
    return data;
}

fn makeQueries(alloc: std.mem.Allocator, dataset: []const f32, cfg: Config) ![]f32 {
    if (cfg.workload != .explicit_dense) return try makeGeneratedQueries(alloc, cfg);
    const queries = try alloc.alloc(f32, cfg.queries * cfg.dims);
    for (0..cfg.queries) |i| {
        const src_idx = (i * 997) % cfg.docs;
        @memcpy(
            queries[i * cfg.dims ..][0..cfg.dims],
            dataset[src_idx * cfg.dims ..][0..cfg.dims],
        );
    }
    return queries;
}

fn makeGeneratedQueries(alloc: std.mem.Allocator, cfg: Config) ![]f32 {
    var embedder = db_mod.embedder.DeterministicDenseEmbedder{};
    const queries = try alloc.alloc(f32, cfg.queries * cfg.dims);
    for (0..cfg.queries) |i| {
        const doc_idx = (i * 997) % cfg.docs;
        const body = try generatedBodyTextAlloc(alloc, doc_idx, cfg);
        defer alloc.free(body);
        const vector = try embedder.interface().embedDense(alloc, "dense_idx", body, @intCast(cfg.dims));
        defer alloc.free(vector);
        @memcpy(queries[i * cfg.dims ..][0..cfg.dims], vector);
    }
    return queries;
}

fn makeInputDocs(alloc: std.mem.Allocator, dataset: []const f32, cfg: Config) ![]InputDoc {
    const docs = try alloc.alloc(InputDoc, cfg.docs);
    for (0..cfg.docs) |doc_idx| {
        const key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{doc_idx});
        const body = try generatedBodyTextAlloc(alloc, doc_idx, cfg);
        const value = switch (cfg.workload) {
            .explicit_dense => try encodeVectorDocJson(alloc, dataset[doc_idx * cfg.dims ..][0..cfg.dims]),
            .generated_dense => try encodeGeneratedDenseDocJson(alloc, body),
            .generated_chunked_dense => try encodeGeneratedChunkedDenseDocJson(alloc, body),
        };
        docs[doc_idx] = .{
            .key = key,
            .value = value,
            .body = body,
        };
    }
    return docs;
}

fn generatedBodyTextAlloc(alloc: std.mem.Allocator, doc_idx: usize, cfg: Config) ![]u8 {
    const topic = switch (doc_idx % 8) {
        0 => "alpha",
        1 => "beta",
        2 => "gamma",
        3 => "delta",
        4 => "epsilon",
        5 => "zeta",
        6 => "eta",
        else => "theta",
    };
    return std.fmt.allocPrint(
        alloc,
        "document {d} topic {s} dims {d} repeated context repeated context repeated context repeated context tail {d}",
        .{ doc_idx, topic, cfg.dims, doc_idx % 97 },
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

fn seedDenseDB(
    alloc: std.mem.Allocator,
    path: []const u8,
    cfg: Config,
    input_docs: []const InputDoc,
    profile_writer: ?*std.Io.Writer,
) !IngestSummary {
    var deterministic_dense = db_mod.embedder.DeterministicDenseEmbedder{};
    var db = try db_mod.DB.open(alloc, path, .{
        .start_index_workers = !cfg.inline_derived,
        .enrichment = switch (cfg.workload) {
            .explicit_dense => .{},
            .generated_dense, .generated_chunked_dense => .{
                .owner_id = "dense-stack-bench",
                .dense_embedder = deterministic_dense.interface(),
            },
        },
    });
    defer db.close();

    const index_cfg = switch (cfg.workload) {
        .explicit_dense => try std.fmt.allocPrint(
            alloc,
            "{{\"field\":\"embedding\",\"dims\":{d},\"metric\":\"l2_squared\"}}",
            .{cfg.dims},
        ),
        .generated_dense => try std.fmt.allocPrint(
            alloc,
            "{{\"field\":\"embedding\",\"dims\":{d},\"metric\":\"l2_squared\",\"generator\":{{\"kind\":\"dense_embedding\",\"source_field\":\"body\"}}}}",
            .{cfg.dims},
        ),
        .generated_chunked_dense => try std.fmt.allocPrint(
            alloc,
            "{{\"field\":\"embedding\",\"dims\":{d},\"metric\":\"l2_squared\",\"generator\":{{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"artifact_name\":\"body_chunks_v1\",\"chunk_size\":256,\"chunk_overlap\":32,\"embedding_name\":\"dense_idx\"}}}}",
            .{cfg.dims},
        ),
    };
    defer alloc.free(index_cfg);

    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = index_cfg,
    });
    if (db.core.index_manager.denseIndex("dense_idx")) |entry| entry.index.resetWriteProfile();

    var bulk_session_open = false;
    if (cfg.bulk_session) {
        try db.beginBulkIngestSession();
        bulk_session_open = true;
        errdefer if (bulk_session_open) db.abortBulkIngestSession();
    }

    var batch_index: usize = 0;
    var summary = IngestSummary{
        .docs = cfg.docs,
        .dims = cfg.dims,
        .batch_size = cfg.batch_size,
    };
    const writes_buf = try alloc.alloc(db_mod.types.BatchWrite, cfg.batch_size);
    defer alloc.free(writes_buf);
    var start: usize = 0;
    while (start < cfg.docs) : ({
        start += cfg.batch_size;
        batch_index += 1;
    }) {
        const end = @min(start + cfg.batch_size, cfg.docs);
        const writes = writes_buf[0 .. end - start];
        for (start..end, 0..) |doc_idx, i| {
            writes[i] = .{
                .key = input_docs[doc_idx].key,
                .value = input_docs[doc_idx].value,
            };
        }

        const started = nowNs();
        var profile: db_mod.BatchProfile = .{};
        try db.batchProfiled(.{
            .writes = writes,
            .sync_level = cfg.sync_level,
        }, &profile);
        const wall_ns = elapsedSince(started);
        summary.write_ns += wall_ns;
        summary.max_batch_ns = @max(summary.max_batch_ns, wall_ns);
        accumulateBatchProfile(&summary, profile);
        if (profile_writer) |writer| {
            try writeBatchProfileJson(writer, cfg, batch_index, start, end, wall_ns, profile);
        }
        summary.batches += 1;

        if (cfg.status_probe_every > 0 and summary.batches % cfg.status_probe_every == 0) {
            const probe_start = nowNs();
            const stats = db.snapshotLsmMaintenanceStats();
            std.mem.doNotOptimizeAway(stats.total_run_bytes);
            const probe_ns = elapsedSince(probe_start);
            summary.status_probe_count += 1;
            summary.status_probe_ns += probe_ns;
            summary.status_probe_max_ns = @max(summary.status_probe_max_ns, probe_ns);
        }

        if (cfg.maintenance_every > 0 and summary.batches % cfg.maintenance_every == 0) {
            const maintenance_start = nowNs();
            var steps: usize = 0;
            while (steps < cfg.maintenance_steps) : (steps += 1) {
                if (!try db.runLsmMaintenanceStep()) break;
                summary.maintenance_steps += 1;
            }
            summary.maintenance_ns += elapsedSince(maintenance_start);
        }
    }

    if (cfg.bulk_session) {
        try db.finishBulkIngestSessionWithOptions(.{
            .compact = false,
            .max_deferred_l0_runs = 64,
        });
        bulk_session_open = false;
    }

    if (cfg.final_drain) {
        const drain_start = nowNs();
        try db.runUntilIdle();
        summary.final_drain_ns = elapsedSince(drain_start);
    }

    if (db.core.index_manager.denseIndex("dense_idx")) |entry| {
        const profile = entry.index.getWriteProfile();
        summary.hbc_insert_calls = profile.insert_calls;
        summary.hbc_grouped_items = profile.grouped_items;
        summary.hbc_grouped_fallback_items = profile.grouped_fallback_items;
        summary.hbc_grouped_leaf_groups = profile.grouped_leaf_groups;
        summary.hbc_grouped_recursive_splits = profile.grouped_recursive_splits;
        summary.hbc_quant_value_bytes = profile.ns_quant_value_bytes;
        summary.hbc_vecs_value_bytes = profile.ns_vecs_value_bytes;
        summary.hbc_nodes_value_bytes = profile.ns_nodes_value_bytes;
        summary.hbc_meta_value_bytes = profile.ns_meta_value_bytes;
        summary.hbc_insert_find_leaf_ns = profile.insert_find_leaf_ns;
        summary.hbc_insert_mutate_leaf_ns = profile.insert_mutate_leaf_ns;
        summary.hbc_insert_commit_ns = profile.insert_commit_ns;
        summary.hbc_refresh_quantized_ns = profile.refresh_quantized_ns;
        if (entry.index.snapshotLsmMaintenanceStats()) |stats| {
            summary.dense_lsm_total_runs = stats.total_runs;
            summary.dense_lsm_total_run_bytes = stats.total_run_bytes;
            summary.dense_lsm_l0_runs = stats.l0_runs;
            summary.dense_lsm_l0_bytes = stats.l0_bytes;
            summary.dense_lsm_obsolete_paths = stats.obsolete_paths;
        }
    }
    const derived_log_stats = db.core.change_journal.statsSnapshot();
    summary.derived_log_append_calls = derived_log_stats.append_calls;
    summary.derived_log_logical_entries = derived_log_stats.logical_entries;
    summary.derived_log_physical_commits = derived_log_stats.physical_commits;
    summary.derived_log_grouped_commits = derived_log_stats.grouped_commits;
    summary.derived_log_grouped_requests = derived_log_stats.grouped_requests;
    const pending_work = db.pendingWorkStats();
    summary.enrichment_target_sequence = pending_work.enrichment.target_sequence;
    summary.enrichment_applied_sequence = pending_work.enrichment.applied_sequence;
    summary.enrichment_processed_requests = pending_work.enrichment.processed_requests;
    summary.enrichment_skip_by_hash_count = pending_work.enrichment.skip_by_hash_count;
    summary.enrichment_codec_decode_failures = pending_work.enrichment.codec_decode_failures;
    summary.enrichment_artifact_bytes_written = pending_work.enrichment.artifact_bytes_written;
    summary.enrichment_dense_artifact_bytes_written = pending_work.enrichment.dense_artifact_bytes_written;
    summary.enrichment_sparse_artifact_bytes_written = pending_work.enrichment.sparse_artifact_bytes_written;
    summary.enrichment_chunk_artifact_bytes_written = pending_work.enrichment.chunk_artifact_bytes_written;

    return summary;
}

fn accumulateBatchProfile(summary: *IngestSummary, profile: db_mod.BatchProfile) void {
    summary.resolve_transforms_ns += profile.resolve_transforms_ns;
    summary.merge_effective_req_ns += profile.merge_effective_req_ns;
    summary.predicates_ns += profile.predicates_ns;
    summary.validate_range_ns += profile.validate_range_ns;
    summary.extract_writes_ns += profile.extract_writes_ns;
    summary.delete_artifacts_ns += profile.delete_artifacts_ns;
    summary.precompute_generated_ns += profile.precompute_generated_ns;
    summary.store_write_ns += profile.store_write_ns;
    summary.split_delta_ns += profile.split_delta_ns;
    summary.build_derived_ns += profile.build_derived_ns;
    summary.apply_shadow_ns += profile.apply_shadow_ns;
    summary.collect_sync_targets_ns += profile.collect_sync_targets_ns;
    summary.append_derived_log_ns += profile.append_replay_journal_ns;
    summary.wait_sync_ns += profile.wait_sync_ns;
    summary.backlog_pressure_ns += profile.backlog_pressure_ns;
    summary.executor_notify_ns += profile.executor_notify_ns;
    summary.derived_apply_ns += profile.derived_apply_ns;
    summary.sync_wait_ns += profile.sync_wait_ns;
    summary.full_text_apply_ns += profile.full_text_apply_ns;
    summary.dense_apply_ns += profile.dense_apply_ns;
    summary.dense_delete_ns += profile.dense_delete_ns;
    summary.dense_doc_index_ns += profile.dense_doc_index_ns;
    summary.dense_embedding_apply_ns += profile.dense_embedding_apply_ns;
    summary.sparse_apply_ns += profile.sparse_apply_ns;
    summary.graph_apply_ns += profile.graph_apply_ns;
    summary.index_sync_ns += profile.index_sync_ns;
    summary.applied_sequence_save_ns += profile.applied_sequence_save_ns;
    summary.derived_log_truncate_ns += profile.replay_journal_truncate_ns;
    summary.notify_enrichment_ns += profile.notify_enrichment_ns;
}

fn writeBatchProfileJson(
    writer: *std.Io.Writer,
    cfg: Config,
    batch_index: usize,
    start: usize,
    end: usize,
    wall_ns: u64,
    profile: db_mod.BatchProfile,
) !void {
    try writer.print(
        "{{\"phase\":\"write_batch\",\"batch_index\":{d},\"doc_start\":{d},\"doc_end\":{d},\"docs\":{d},\"total_docs\":{d},\"dims\":{d},\"batch_size\":{d},\"workload\":\"{s}\",\"inline_derived\":{any},\"bulk_session\":{any},\"sync_level\":\"{s}\",\"wall_ns\":{d},\"total_ns\":{d}",
        .{
            batch_index,
            start,
            end,
            end - start,
            cfg.docs,
            cfg.dims,
            cfg.batch_size,
            @tagName(cfg.workload),
            cfg.inline_derived,
            cfg.bulk_session,
            db_mod.types.publicSyncLevelText(cfg.sync_level),
            wall_ns,
            profile.total_ns,
        },
    );
    try writer.print(
        ",\"resolve_transforms_ns\":{d},\"merge_effective_req_ns\":{d},\"predicates_ns\":{d},\"validate_range_ns\":{d},\"extract_writes_ns\":{d},\"delete_artifacts_ns\":{d},\"precompute_generated_ns\":{d},\"store_write_ns\":{d},\"split_delta_ns\":{d},\"build_derived_ns\":{d},\"apply_shadow_ns\":{d},\"collect_sync_targets_ns\":{d},\"append_derived_log_ns\":{d}",
        .{
            profile.resolve_transforms_ns,
            profile.merge_effective_req_ns,
            profile.predicates_ns,
            profile.validate_range_ns,
            profile.extract_writes_ns,
            profile.delete_artifacts_ns,
            profile.precompute_generated_ns,
            profile.store_write_ns,
            profile.split_delta_ns,
            profile.build_derived_ns,
            profile.apply_shadow_ns,
            profile.collect_sync_targets_ns,
            profile.append_replay_journal_ns,
        },
    );
    try writer.print(
        ",\"wait_sync_ns\":{d},\"backlog_pressure_ns\":{d},\"executor_notify_ns\":{d},\"derived_apply_ns\":{d},\"sync_wait_ns\":{d},\"full_text_apply_ns\":{d},\"dense_apply_ns\":{d},\"dense_delete_ns\":{d},\"dense_doc_index_ns\":{d},\"dense_embedding_apply_ns\":{d},\"sparse_apply_ns\":{d},\"graph_apply_ns\":{d},\"index_sync_ns\":{d},\"applied_sequence_save_ns\":{d},\"derived_log_truncate_ns\":{d},\"notify_enrichment_ns\":{d}",
        .{
            profile.wait_sync_ns,
            profile.backlog_pressure_ns,
            profile.executor_notify_ns,
            profile.derived_apply_ns,
            profile.sync_wait_ns,
            profile.full_text_apply_ns,
            profile.dense_apply_ns,
            profile.dense_delete_ns,
            profile.dense_doc_index_ns,
            profile.dense_embedding_apply_ns,
            profile.sparse_apply_ns,
            profile.graph_apply_ns,
            profile.index_sync_ns,
            profile.applied_sequence_save_ns,
            profile.replay_journal_truncate_ns,
            profile.notify_enrichment_ns,
        },
    );
    try writer.print(
        ",\"hbc_insert_calls\":{d},\"hbc_grouped_items\":{d},\"hbc_grouped_fallback_items\":{d},\"hbc_grouped_leaf_groups\":{d},\"hbc_grouped_split_candidates\":{d},\"hbc_grouped_recursive_splits\":{d},\"hbc_grouped_leaf_range_writes\":{d},\"hbc_grouped_ancestor_range_refreshes\":{d},\"hbc_grouped_ancestor_range_nodes\":{d},\"hbc_grouped_node_body_writes\":{d},\"hbc_grouped_vec_leaf_writes\":{d}",
        .{
            profile.hbc_insert_calls,
            profile.hbc_grouped_items,
            profile.hbc_grouped_fallback_items,
            profile.hbc_grouped_leaf_groups,
            profile.hbc_grouped_split_candidates,
            profile.hbc_grouped_recursive_splits,
            profile.hbc_grouped_leaf_range_writes,
            profile.hbc_grouped_ancestor_range_refreshes,
            profile.hbc_grouped_ancestor_range_nodes,
            profile.hbc_grouped_node_body_writes,
            profile.hbc_grouped_vec_leaf_writes,
        },
    );
    try writer.print(
        ",\"hbc_save_node_calls\":{d},\"hbc_split_leaf_calls\":{d},\"hbc_split_internal_calls\":{d},\"hbc_range_put_calls\":{d},\"hbc_range_delete_calls\":{d},\"hbc_nodes_put_calls\":{d},\"hbc_nodes_append_calls\":{d},\"hbc_nodes_delete_calls\":{d},\"hbc_meta_put_calls\":{d},\"hbc_meta_append_calls\":{d},\"hbc_meta_delete_calls\":{d},\"hbc_quant_put_calls\":{d},\"hbc_quant_append_calls\":{d},\"hbc_quant_delete_calls\":{d},\"hbc_vecs_put_calls\":{d},\"hbc_vecs_append_calls\":{d},\"hbc_vecs_delete_calls\":{d}",
        .{
            profile.hbc_save_node_calls,
            profile.hbc_split_leaf_calls,
            profile.hbc_split_internal_calls,
            profile.hbc_range_put_calls,
            profile.hbc_range_delete_calls,
            profile.hbc_nodes_put_calls,
            profile.hbc_nodes_append_calls,
            profile.hbc_nodes_delete_calls,
            profile.hbc_meta_put_calls,
            profile.hbc_meta_append_calls,
            profile.hbc_meta_delete_calls,
            profile.hbc_quant_put_calls,
            profile.hbc_quant_append_calls,
            profile.hbc_quant_delete_calls,
            profile.hbc_vecs_put_calls,
            profile.hbc_vecs_append_calls,
            profile.hbc_vecs_delete_calls,
        },
    );
    try writer.print(
        ",\"hbc_insert_transform_ns\":{d},\"hbc_insert_store_vector_ns\":{d},\"hbc_insert_find_leaf_ns\":{d},\"hbc_insert_mutate_leaf_ns\":{d},\"hbc_insert_flush_metadata_ns\":{d},\"hbc_insert_commit_ns\":{d},\"hbc_save_node_ns\":{d},\"hbc_save_split_range_ns\":{d},\"hbc_update_parent_ns\":{d},\"hbc_split_leaf_ns\":{d},\"hbc_split_internal_ns\":{d},\"hbc_refresh_quantized_ns\":{d},\"hbc_quantized_vector_load_ns\":{d},\"hbc_quantized_compute_ns\":{d},\"hbc_quantized_store_ns\":{d},\"hbc_quantized_encode_ns\":{d},\"hbc_quantized_put_ns\":{d},\"hbc_bulk_build_store_ns\":{d},\"hbc_bulk_build_tree_ns\":{d}}}\n",
        .{
            profile.hbc_insert_transform_ns,
            profile.hbc_insert_store_vector_ns,
            profile.hbc_insert_find_leaf_ns,
            profile.hbc_insert_mutate_leaf_ns,
            profile.hbc_insert_flush_metadata_ns,
            profile.hbc_insert_commit_ns,
            profile.hbc_save_node_ns,
            profile.hbc_save_split_range_ns,
            profile.hbc_update_parent_ns,
            profile.hbc_split_leaf_ns,
            profile.hbc_split_internal_ns,
            profile.hbc_refresh_quantized_ns,
            profile.hbc_quantized_vector_load_ns,
            profile.hbc_quantized_compute_ns,
            profile.hbc_quantized_store_ns,
            profile.hbc_quantized_encode_ns,
            profile.hbc_quantized_put_ns,
            profile.hbc_bulk_build_store_ns,
            profile.hbc_bulk_build_tree_ns,
        },
    );
}

fn printIngestSummary(io: std.Io, cfg: Config, summary: IngestSummary) !void {
    var buffer: [4096]u8 = undefined;
    var writer = std.Io.File.stdout().writer(io, &buffer);
    try writeIngestSummaryJson(&writer.interface, cfg, summary);
    try writer.interface.flush();
}

fn writeIngestSummaryJson(writer: *std.Io.Writer, cfg: Config, summary: IngestSummary) !void {
    try writer.print(
        "{{\"phase\":\"ingest_summary\",\"docs\":{d},\"dims\":{d},\"batch_size\":{d},\"batches\":{d},\"workload\":\"{s}\",\"inline_derived\":{any},\"bulk_session\":{any},\"final_drain\":{any},\"sync_level\":\"{s}\",\"write_ns\":{d},\"write_ns_per_doc\":{d},\"max_batch_ns\":{d},\"final_drain_ns\":{d},\"maintenance_ns\":{d},\"maintenance_steps\":{d},\"status_probe_count\":{d},\"status_probe_ns\":{d},\"status_probe_max_ns\":{d}",
        .{
            summary.docs,
            summary.dims,
            summary.batch_size,
            summary.batches,
            @tagName(cfg.workload),
            cfg.inline_derived,
            cfg.bulk_session,
            cfg.final_drain,
            db_mod.types.publicSyncLevelText(cfg.sync_level),
            summary.write_ns,
            summary.writeNsPerDoc(),
            summary.max_batch_ns,
            summary.final_drain_ns,
            summary.maintenance_ns,
            summary.maintenance_steps,
            summary.status_probe_count,
            summary.status_probe_ns,
            summary.status_probe_max_ns,
        },
    );
    try writer.print(
        ",\"dense_lsm_total_runs\":{d},\"dense_lsm_total_run_bytes\":{d},\"dense_lsm_l0_runs\":{d},\"dense_lsm_l0_bytes\":{d},\"dense_lsm_obsolete_paths\":{d}",
        .{
            summary.dense_lsm_total_runs,
            summary.dense_lsm_total_run_bytes,
            summary.dense_lsm_l0_runs,
            summary.dense_lsm_l0_bytes,
            summary.dense_lsm_obsolete_paths,
        },
    );
    try writer.print(
        ",\"resolve_transforms_ns\":{d},\"merge_effective_req_ns\":{d},\"predicates_ns\":{d},\"validate_range_ns\":{d},\"extract_writes_ns\":{d},\"delete_artifacts_ns\":{d},\"precompute_generated_ns\":{d},\"store_write_ns\":{d},\"split_delta_ns\":{d},\"build_derived_ns\":{d},\"apply_shadow_ns\":{d},\"collect_sync_targets_ns\":{d},\"append_derived_log_ns\":{d},\"wait_sync_ns\":{d},\"backlog_pressure_ns\":{d},\"executor_notify_ns\":{d},\"derived_apply_ns\":{d},\"sync_wait_ns\":{d},\"full_text_apply_ns\":{d},\"dense_apply_ns\":{d},\"dense_delete_ns\":{d},\"dense_doc_index_ns\":{d},\"dense_embedding_apply_ns\":{d},\"sparse_apply_ns\":{d},\"graph_apply_ns\":{d},\"index_sync_ns\":{d},\"applied_sequence_save_ns\":{d},\"derived_log_truncate_ns\":{d},\"notify_enrichment_ns\":{d}",
        .{
            summary.resolve_transforms_ns,
            summary.merge_effective_req_ns,
            summary.predicates_ns,
            summary.validate_range_ns,
            summary.extract_writes_ns,
            summary.delete_artifacts_ns,
            summary.precompute_generated_ns,
            summary.store_write_ns,
            summary.split_delta_ns,
            summary.build_derived_ns,
            summary.apply_shadow_ns,
            summary.collect_sync_targets_ns,
            summary.append_derived_log_ns,
            summary.wait_sync_ns,
            summary.backlog_pressure_ns,
            summary.executor_notify_ns,
            summary.derived_apply_ns,
            summary.sync_wait_ns,
            summary.full_text_apply_ns,
            summary.dense_apply_ns,
            summary.dense_delete_ns,
            summary.dense_doc_index_ns,
            summary.dense_embedding_apply_ns,
            summary.sparse_apply_ns,
            summary.graph_apply_ns,
            summary.index_sync_ns,
            summary.applied_sequence_save_ns,
            summary.derived_log_truncate_ns,
            summary.notify_enrichment_ns,
        },
    );
    try writer.print(
        ",\"derived_log_append_calls\":{d},\"derived_log_logical_entries\":{d},\"derived_log_physical_commits\":{d},\"derived_log_grouped_commits\":{d},\"derived_log_grouped_requests\":{d},\"enrichment_target_sequence\":{d},\"enrichment_applied_sequence\":{d},\"enrichment_processed_requests\":{d},\"enrichment_skip_by_hash_count\":{d},\"enrichment_codec_decode_failures\":{d},\"enrichment_artifact_bytes_written\":{d},\"enrichment_dense_artifact_bytes_written\":{d},\"enrichment_sparse_artifact_bytes_written\":{d},\"enrichment_chunk_artifact_bytes_written\":{d}",
        .{
            summary.derived_log_append_calls,
            summary.derived_log_logical_entries,
            summary.derived_log_physical_commits,
            summary.derived_log_grouped_commits,
            summary.derived_log_grouped_requests,
            summary.enrichment_target_sequence,
            summary.enrichment_applied_sequence,
            summary.enrichment_processed_requests,
            summary.enrichment_skip_by_hash_count,
            summary.enrichment_codec_decode_failures,
            summary.enrichment_artifact_bytes_written,
            summary.enrichment_dense_artifact_bytes_written,
            summary.enrichment_sparse_artifact_bytes_written,
            summary.enrichment_chunk_artifact_bytes_written,
        },
    );
    try writer.print(
        ",\"hbc_insert_calls\":{d},\"hbc_grouped_items\":{d},\"hbc_grouped_fallback_items\":{d},\"hbc_grouped_leaf_groups\":{d},\"hbc_grouped_recursive_splits\":{d},\"hbc_quant_value_bytes\":{d},\"hbc_vecs_value_bytes\":{d},\"hbc_nodes_value_bytes\":{d},\"hbc_meta_value_bytes\":{d},\"hbc_insert_find_leaf_ns\":{d},\"hbc_insert_mutate_leaf_ns\":{d},\"hbc_insert_commit_ns\":{d},\"hbc_refresh_quantized_ns\":{d}}}\n",
        .{
            summary.hbc_insert_calls,
            summary.hbc_grouped_items,
            summary.hbc_grouped_fallback_items,
            summary.hbc_grouped_leaf_groups,
            summary.hbc_grouped_recursive_splits,
            summary.hbc_quant_value_bytes,
            summary.hbc_vecs_value_bytes,
            summary.hbc_nodes_value_bytes,
            summary.hbc_meta_value_bytes,
            summary.hbc_insert_find_leaf_ns,
            summary.hbc_insert_mutate_leaf_ns,
            summary.hbc_insert_commit_ns,
            summary.hbc_refresh_quantized_ns,
        },
    );
}

fn enforceGuardrails(cfg: Config, summary: IngestSummary) !void {
    if (cfg.max_write_ns_per_doc) |limit| {
        if (summary.writeNsPerDoc() > limit) {
            std.debug.print(
                "dense-stack guardrail failed: write_ns_per_doc={d} > limit={d}\n",
                .{ summary.writeNsPerDoc(), limit },
            );
            return error.GuardrailFailed;
        }
    }
    if (cfg.max_status_probe_ns) |limit| {
        if (summary.status_probe_max_ns > limit) {
            std.debug.print(
                "dense-stack guardrail failed: status_probe_max_ns={d} > limit={d}\n",
                .{ summary.status_probe_max_ns, limit },
            );
            return error.GuardrailFailed;
        }
    }
    if (cfg.max_dense_lsm_run_bytes) |limit| {
        if (summary.dense_lsm_total_run_bytes > limit) {
            std.debug.print(
                "dense-stack guardrail failed: dense_lsm_total_run_bytes={d} > limit={d}\n",
                .{ summary.dense_lsm_total_run_bytes, limit },
            );
            return error.GuardrailFailed;
        }
    }
    if (cfg.max_dense_l0_runs) |limit| {
        if (summary.dense_lsm_l0_runs > limit) {
            std.debug.print(
                "dense-stack guardrail failed: dense_lsm_l0_runs={d} > limit={d}\n",
                .{ summary.dense_lsm_l0_runs, limit },
            );
            return error.GuardrailFailed;
        }
    }
    if (cfg.max_hbc_quant_value_bytes) |limit| {
        if (summary.hbc_quant_value_bytes > limit) {
            std.debug.print(
                "dense-stack guardrail failed: hbc_quant_value_bytes={d} > limit={d}\n",
                .{ summary.hbc_quant_value_bytes, limit },
            );
            return error.GuardrailFailed;
        }
    }
}

fn encodeVectorDocJson(alloc: std.mem.Allocator, vector: []const f32) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"embedding\":[");
    for (vector, 0..) |value, i| {
        if (i != 0) try out.append(alloc, ',');
        try out.print(alloc, "{d}", .{value});
    }
    try out.appendSlice(alloc, "]}");
    return out.toOwnedSlice(alloc);
}

fn encodeGeneratedDenseDocJson(alloc: std.mem.Allocator, body: []const u8) ![]u8 {
    return std.fmt.allocPrint(alloc, "{{\"title\":\"dense doc\",\"body\":\"{s}\"}}", .{body});
}

fn encodeGeneratedChunkedDenseDocJson(alloc: std.mem.Allocator, body: []const u8) ![]u8 {
    return std.fmt.allocPrint(alloc, "{{\"title\":\"chunked dense doc\",\"body\":\"{s}\"}}", .{body});
}

fn runBench(alloc: std.mem.Allocator, path: []const u8, cfg: Config, queries: []const f32) !Result {
    var resource_manager = resource_manager_mod.ResourceManager.init(.{});
    var db = try db_mod.DB.open(alloc, path, .{ .resource_manager = &resource_manager });
    defer db.close();

    const query_total = cfg.queries * cfg.repeats;

    const db_search_ns = blk: {
        for (0..cfg.queries) |i| {
            var warm = try db.search(alloc, .{
                .index_name = "dense_idx",
                .query = .{ .dense_knn = .{
                    .vector = queries[i * cfg.dims ..][0..cfg.dims],
                    .k = @intCast(cfg.k),
                } },
                .limit = @intCast(cfg.k),
                .include_stored = false,
            });
            warm.deinit();
        }

        var total_ns: u64 = 0;
        for (0..cfg.repeats) |_| {
            for (0..cfg.queries) |i| {
                const start_ns = nowNs();
                var result = try db.search(alloc, .{
                    .index_name = "dense_idx",
                    .query = .{ .dense_knn = .{
                        .vector = queries[i * cfg.dims ..][0..cfg.dims],
                        .k = @intCast(cfg.k),
                    } },
                    .limit = @intCast(cfg.k),
                    .include_stored = false,
                });
                total_ns += elapsedSince(start_ns);
                std.mem.doNotOptimizeAway(result.hits.len);
                result.deinit();
            }
        }
        break :blk @divTrunc(total_ns, query_total);
    };

    const db_search_concurrent_ns = blk: {
        if (cfg.search_threads == 1) break :blk db_search_ns;

        const SearchWorker = struct {
            db: *db_mod.DB,
            queries: []const f32,
            dims: usize,
            query_count: usize,
            query_total: usize,
            k: usize,
            next_query: *std.atomic.Value(usize),
            start_flag: *std.atomic.Value(bool),
            err: ?anyerror = null,

            fn run(self: *@This()) void {
                while (!self.start_flag.load(.acquire)) std.atomic.spinLoopHint();
                while (true) {
                    const query_idx = self.next_query.fetchAdd(1, .acq_rel);
                    if (query_idx >= self.query_total) break;
                    const offset = (query_idx % self.query_count) * self.dims;
                    var result = self.db.search(std.heap.c_allocator, .{
                        .index_name = "dense_idx",
                        .query = .{ .dense_knn = .{
                            .vector = self.queries[offset..][0..self.dims],
                            .k = @intCast(self.k),
                        } },
                        .limit = @intCast(self.k),
                        .include_stored = false,
                    }) catch |err| {
                        self.err = err;
                        return;
                    };
                    std.mem.doNotOptimizeAway(result.hits.len);
                    result.deinit();
                }
            }
        };

        var next_query = std.atomic.Value(usize).init(0);
        var start_flag = std.atomic.Value(bool).init(false);
        const workers = try alloc.alloc(SearchWorker, cfg.search_threads);
        defer alloc.free(workers);
        const threads = try alloc.alloc(std.Thread, cfg.search_threads);
        defer alloc.free(threads);

        for (workers, 0..) |*worker, i| {
            worker.* = .{
                .db = &db,
                .queries = queries,
                .dims = cfg.dims,
                .query_count = cfg.queries,
                .query_total = query_total,
                .k = cfg.k,
                .next_query = &next_query,
                .start_flag = &start_flag,
            };
            threads[i] = try std.Thread.spawn(.{}, SearchWorker.run, .{worker});
        }

        const start_ns = nowNs();
        start_flag.store(true, .release);
        for (threads, workers) |thread, worker| {
            thread.join();
            if (worker.err) |err| return err;
        }
        break :blk @divTrunc(elapsedSince(start_ns), query_total);
    };

    const capi_packed_ns = blk: {
        const zpath = try alloc.dupeSentinel(u8, path, 0);
        defer alloc.free(zpath);

        var handle_ptr: ?*anyopaque = null;
        try expectOk(capi_db.antfly_db_open(zpath.ptr, &handle_ptr));
        defer capi_db.antfly_db_close(handle_ptr);

        for (0..cfg.queries) |i| {
            const query = queries[i * cfg.dims ..][0..cfg.dims];
            var packed_result: capi.PackedDenseSearchResult = .{};
            try expectOk(capi_db.antfly_db_search_dense(
                handle_ptr,
                slice("dense_idx"),
                query.ptr,
                query.len,
                @intCast(cfg.k),
                @intCast(cfg.k),
                0,
                &packed_result,
            ));
            capi_db.antfly_db_packed_dense_search_result_free(&packed_result);
        }

        var total_ns: u64 = 0;
        for (0..cfg.repeats) |_| {
            for (0..cfg.queries) |i| {
                const query = queries[i * cfg.dims ..][0..cfg.dims];
                var packed_result: capi.PackedDenseSearchResult = .{};
                const start_ns = nowNs();
                try expectOk(capi_db.antfly_db_search_dense(
                    handle_ptr,
                    slice("dense_idx"),
                    query.ptr,
                    query.len,
                    @intCast(cfg.k),
                    @intCast(cfg.k),
                    0,
                    &packed_result,
                ));
                total_ns += elapsedSince(start_ns);
                std.mem.doNotOptimizeAway(packed_result.hit_count);
                capi_db.antfly_db_packed_dense_search_result_free(&packed_result);
            }
        }
        break :blk @divTrunc(total_ns, query_total);
    };

    const capi_wire_ns = blk: {
        const zpath = try alloc.dupeSentinel(u8, path, 0);
        defer alloc.free(zpath);

        var handle_ptr: ?*anyopaque = null;
        try expectOk(capi_db.antfly_db_open(zpath.ptr, &handle_ptr));
        defer capi_db.antfly_db_close(handle_ptr);

        const requests = try alloc.alloc([]u8, cfg.queries);
        defer {
            for (requests) |req| alloc.free(req);
            alloc.free(requests);
        }
        for (0..cfg.queries) |i| {
            requests[i] = try encodeDenseWireRequest(alloc, "dense_idx", queries[i * cfg.dims ..][0..cfg.dims], @intCast(cfg.k), @intCast(cfg.k), 0);
        }

        for (requests) |req| {
            var buf: capi.Buffer = .{};
            try expectOk(capi_db.antfly_db_search_dense_wire(handle_ptr, sliceBytes(req), &buf));
            capi_db.antfly_db_buffer_free(buf.ptr, buf.len);
        }

        var total_ns: u64 = 0;
        for (0..cfg.repeats) |_| {
            for (requests) |req| {
                var buf: capi.Buffer = .{};
                const start_ns = nowNs();
                try expectOk(capi_db.antfly_db_search_dense_wire(handle_ptr, sliceBytes(req), &buf));
                total_ns += elapsedSince(start_ns);
                std.mem.doNotOptimizeAway(buf.len);
                capi_db.antfly_db_buffer_free(buf.ptr, buf.len);
            }
        }
        break :blk @divTrunc(total_ns, query_total);
    };

    return .{
        .db_search_ns = db_search_ns,
        .db_search_concurrent_ns = db_search_concurrent_ns,
        .capi_packed_ns = capi_packed_ns,
        .capi_wire_ns = capi_wire_ns,
        .resources = captureResourceSummary(&resource_manager),
    };
}

fn captureResourceSummary(manager: *resource_manager_mod.ResourceManager) ResourceSummary {
    const stats = manager.snapshot();
    return .{
        .lsm_block_table_cache = captureSliceSummary(stats.slices[@intFromEnum(resource_manager_mod.Slice.lsm_block_table_cache)]),
        .lsm_in_memory_state = captureSliceSummary(stats.slices[@intFromEnum(resource_manager_mod.Slice.lsm_in_memory_state)]),
        .hbc_node_metadata_cache = captureSliceSummary(stats.slices[@intFromEnum(resource_manager_mod.Slice.hbc_node_metadata_cache)]),
        .dense_search_working_set = captureSliceSummary(stats.slices[@intFromEnum(resource_manager_mod.Slice.dense_search_working_set)]),
        .dense_apply_working_set = captureSliceSummary(stats.slices[@intFromEnum(resource_manager_mod.Slice.dense_apply_working_set)]),
        .dense_routing_working_set = captureSliceSummary(stats.slices[@intFromEnum(resource_manager_mod.Slice.dense_routing_working_set)]),
    };
}

fn captureSliceSummary(stats: resource_manager_mod.SliceStats) SliceSummary {
    return .{
        .used_bytes = stats.used_bytes,
        .peak_bytes = stats.peak_bytes,
        .soft_limit_events = stats.soft_limit_events,
        .hard_limit_rejections = stats.hard_limit_rejections,
    };
}

fn slice(bytes: []const u8) capi.Slice {
    return .{
        .ptr = if (bytes.len == 0) null else bytes.ptr,
        .len = bytes.len,
    };
}

fn sliceBytes(bytes: []const u8) capi.Slice {
    return slice(bytes);
}

fn expectOk(code: capi.ErrorCode) !void {
    if (code != .ok) return error.Internal;
}

fn bytesToMiB(bytes: u64) f64 {
    return @as(f64, @floatFromInt(bytes)) / (1024.0 * 1024.0);
}

fn encodeDenseWireRequest(alloc: std.mem.Allocator, index_name: []const u8, vector: []const f32, k: u32, limit: u32, offset: u32) ![]u8 {
    const header_len: usize = 4 + 2 + 2 + 4 + 4 + 4 + 4 + 2 + 2;
    const total_len = header_len + index_name.len + vector.len * 4;
    const out = try alloc.alloc(u8, total_len);
    var cursor: usize = 0;
    std.mem.writeInt(u32, out[cursor..][0..4], 0x41464442, .little);
    cursor += 4;
    std.mem.writeInt(u16, out[cursor..][0..2], 1, .little);
    cursor += 2;
    std.mem.writeInt(u16, out[cursor..][0..2], 1, .little);
    cursor += 2;
    std.mem.writeInt(u32, out[cursor..][0..4], 0, .little);
    cursor += 4;
    std.mem.writeInt(u32, out[cursor..][0..4], k, .little);
    cursor += 4;
    std.mem.writeInt(u32, out[cursor..][0..4], limit, .little);
    cursor += 4;
    std.mem.writeInt(u32, out[cursor..][0..4], offset, .little);
    cursor += 4;
    std.mem.writeInt(u16, out[cursor..][0..2], @intCast(index_name.len), .little);
    cursor += 2;
    std.mem.writeInt(u16, out[cursor..][0..2], @intCast(vector.len), .little);
    cursor += 2;
    @memcpy(out[cursor..][0..index_name.len], index_name);
    cursor += index_name.len;
    for (vector) |value| {
        std.mem.writeInt(u32, out[cursor..][0..4], @bitCast(value), .little);
        cursor += 4;
    }
    return out;
}

fn tempPath(buf: []u8) [*:0]const u8 {
    const ts = nowNs();
    const path_bytes = std.fmt.bufPrint(buf, "/tmp/antfly-dense-stack-{d}\x00", .{ts}) catch unreachable;
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
    return platform_time.monotonicNs();
}

fn elapsedSince(start_ns: u64) u64 {
    return nowNs() - start_ns;
}
