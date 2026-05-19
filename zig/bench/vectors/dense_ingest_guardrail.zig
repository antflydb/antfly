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

const KmeansBackend = enum {
    auto,
    cpu,
    metal,
};

const KmeansUpdateStrategy = enum {
    auto,
    scatter,
    segmented,
    metal,
};

const InputDoc = struct {
    key: []const u8,
    value: []const u8,
};

const Config = struct {
    docs: usize = 5000,
    dims: usize = 1536,
    batch_size: usize = 500,
    leaf_size: usize = 7 * 24,
    branching_factor: usize = 7 * 24,
    kmeans_backend: KmeansBackend = .auto,
    kmeans_update_strategy: KmeansUpdateStrategy = .auto,
    bulk_rebuild_hbc_leaf_min_members: ?usize = null,
    seed: u64 = 42,
    inline_derived: bool = false,
    bulk_session: bool = false,
    final_drain: bool = true,
    hold_before_final_drain_ms: u64 = 0,
    status_probe_every: usize = 0,
    maintenance_every: usize = 0,
    maintenance_steps: usize = 1,
    max_write_ns_per_doc: ?u64 = null,
    max_status_probe_ns: ?u64 = null,
    max_dense_lsm_run_bytes: ?u64 = null,
    max_dense_l0_runs: ?u64 = null,
    max_hbc_quant_value_bytes: ?u64 = null,
    sync_level: db_types.SyncLevel = .write,
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
    hbc_split_leaf_calls: u64 = 0,
    hbc_split_internal_calls: u64 = 0,
    hbc_deferred_leaf_split_steps: u64 = 0,
    hbc_bulk_leaf_rebuild_calls: u64 = 0,
    hbc_bulk_leaf_rebuild_members_max: u64 = 0,
    hbc_kmeans_assignment_cpu_calls: u64 = 0,
    hbc_kmeans_assignment_metal_calls: u64 = 0,
    hbc_kmeans_assignment_cpu_ns: u64 = 0,
    hbc_kmeans_assignment_metal_ns: u64 = 0,
    hbc_kmeans_update_cpu_calls: u64 = 0,
    hbc_kmeans_update_metal_calls: u64 = 0,
    hbc_kmeans_update_cpu_ns: u64 = 0,
    hbc_kmeans_update_metal_ns: u64 = 0,
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
    append_replay_journal_ns: u64 = 0,
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
    replay_journal_truncate_ns: u64 = 0,
    notify_enrichment_ns: u64 = 0,

    fn writeNsPerDoc(self: IngestSummary) u64 {
        if (self.docs == 0) return 0;
        return self.write_ns / self.docs;
    }
};

pub fn main(init: std.process.Init) !void {
    const alloc = std.heap.c_allocator;
    const cfg = try parseArgs(init.minimal.args);
    const dataset = try makeDataset(alloc, cfg);
    defer alloc.free(dataset);
    const input_docs = try makeInputDocs(alloc, dataset, cfg);
    defer freeInputDocs(alloc, input_docs);

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    const summary = try seedDenseDB(alloc, std.mem.span(path), cfg, input_docs);
    try printSummary(cfg, summary);
    try enforceGuardrails(cfg, summary);
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
        } else if (std.mem.eql(u8, arg, "--leaf-size")) {
            cfg.leaf_size = try parseNextUsize(&args, "--leaf-size");
        } else if (std.mem.eql(u8, arg, "--branching-factor")) {
            cfg.branching_factor = try parseNextUsize(&args, "--branching-factor");
        } else if (std.mem.eql(u8, arg, "--kmeans-backend")) {
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.kmeans_backend = std.meta.stringToEnum(KmeansBackend, raw) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--kmeans-update-strategy")) {
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.kmeans_update_strategy = std.meta.stringToEnum(KmeansUpdateStrategy, raw) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--bulk-rebuild-hbc-leaf-min-members")) {
            cfg.bulk_rebuild_hbc_leaf_min_members = try parseNextUsize(&args, "--bulk-rebuild-hbc-leaf-min-members");
        } else if (std.mem.eql(u8, arg, "--seed")) {
            cfg.seed = try parseNextU64(&args, "--seed");
        } else if (std.mem.eql(u8, arg, "--inline-derived")) {
            cfg.inline_derived = true;
        } else if (std.mem.eql(u8, arg, "--bulk-session")) {
            cfg.bulk_session = true;
        } else if (std.mem.eql(u8, arg, "--no-final-drain")) {
            cfg.final_drain = false;
        } else if (std.mem.eql(u8, arg, "--hold-before-final-drain-ms")) {
            cfg.hold_before_final_drain_ms = try parseNextU64(&args, "--hold-before-final-drain-ms");
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
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.sync_level = db_types.parsePublicSyncLevelText(raw) orelse return error.InvalidArgument;
        } else {
            return error.InvalidArgument;
        }
    }
    if (cfg.docs == 0 or cfg.dims == 0 or cfg.batch_size == 0 or cfg.leaf_size == 0 or cfg.branching_factor == 0) return error.InvalidArgument;
    return cfg;
}

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

fn makeDataset(alloc: std.mem.Allocator, cfg: Config) ![]f32 {
    const data = try alloc.alloc(f32, cfg.docs * cfg.dims);
    for (0..cfg.docs) |doc_idx| {
        const cluster = @as(f32, @floatFromInt(doc_idx % 8)) * 0.25;
        const vec = data[doc_idx * cfg.dims ..][0..cfg.dims];
        for (0..cfg.dims) |dim_idx| {
            vec[dim_idx] = cluster + deterministicNoise(cfg.seed, doc_idx, dim_idx);
        }
        normalizeInPlace(vec);
    }
    return data;
}

fn makeInputDocs(alloc: std.mem.Allocator, dataset: []const f32, cfg: Config) ![]InputDoc {
    const docs = try alloc.alloc(InputDoc, cfg.docs);
    errdefer freeInputDocs(alloc, docs);
    for (0..cfg.docs) |doc_idx| {
        docs[doc_idx] = .{
            .key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{doc_idx}),
            .value = try encodeVectorDocJson(alloc, dataset[doc_idx * cfg.dims ..][0..cfg.dims]),
        };
    }
    return docs;
}

fn freeInputDocs(alloc: std.mem.Allocator, docs: []InputDoc) void {
    for (docs) |doc| {
        alloc.free(doc.key);
        alloc.free(doc.value);
    }
    alloc.free(docs);
}

fn seedDenseDB(
    alloc: std.mem.Allocator,
    path: []const u8,
    cfg: Config,
    input_docs: []const InputDoc,
) !IngestSummary {
    var db = try db_mod.DB.open(alloc, path, .{
        .start_index_workers = !cfg.inline_derived,
    });
    defer db.close();

    const index_cfg = try std.fmt.allocPrint(
        alloc,
        "{{\"field\":\"embedding\",\"dims\":{d},\"metric\":\"l2_squared\",\"leaf_size\":{d},\"branching_factor\":{d},\"kmeans_backend\":\"{s}\",\"kmeans_update_strategy\":\"{s}\"}}",
        .{ cfg.dims, cfg.leaf_size, cfg.branching_factor, @tagName(cfg.kmeans_backend), @tagName(cfg.kmeans_update_strategy) },
    );
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

    var summary = IngestSummary{
        .docs = cfg.docs,
        .dims = cfg.dims,
        .batch_size = cfg.batch_size,
    };

    const writes_buf = try alloc.alloc(db_types.BatchWrite, cfg.batch_size);
    defer alloc.free(writes_buf);

    var batch_index: usize = 0;
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
            .bulk_rebuild_hbc_leaf_min_members = cfg.bulk_rebuild_hbc_leaf_min_members,
        });
        bulk_session_open = false;
    }

    if (cfg.final_drain) {
        if (cfg.hold_before_final_drain_ms > 0) {
            sleepMs(cfg.hold_before_final_drain_ms);
        }
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
        summary.hbc_split_leaf_calls = profile.split_leaf_calls;
        summary.hbc_split_internal_calls = profile.split_internal_calls;
        summary.hbc_deferred_leaf_split_steps = profile.deferred_leaf_split_steps;
        summary.hbc_bulk_leaf_rebuild_calls = profile.bulk_leaf_rebuild_calls;
        summary.hbc_bulk_leaf_rebuild_members_max = profile.bulk_leaf_rebuild_members_max;
        summary.hbc_kmeans_assignment_cpu_calls = profile.kmeans_assignment_cpu_calls;
        summary.hbc_kmeans_assignment_metal_calls = profile.kmeans_assignment_metal_calls;
        summary.hbc_kmeans_assignment_cpu_ns = profile.kmeans_assignment_cpu_ns;
        summary.hbc_kmeans_assignment_metal_ns = profile.kmeans_assignment_metal_ns;
        summary.hbc_kmeans_update_cpu_calls = profile.kmeans_update_cpu_calls;
        summary.hbc_kmeans_update_metal_calls = profile.kmeans_update_metal_calls;
        summary.hbc_kmeans_update_cpu_ns = profile.kmeans_update_cpu_ns;
        summary.hbc_kmeans_update_metal_ns = profile.kmeans_update_metal_ns;
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
    summary.append_replay_journal_ns += profile.append_replay_journal_ns;
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
    summary.replay_journal_truncate_ns += profile.replay_journal_truncate_ns;
    summary.notify_enrichment_ns += profile.notify_enrichment_ns;
}

fn printSummary(cfg: Config, summary: IngestSummary) !void {
    std.debug.print(
        "dense_ingest_guardrail docs={d} dims={d} batch_size={d} leaf_size={d} branching_factor={d} kmeans_backend={s} kmeans_update_strategy={s} sync={s} write_ns_per_doc={d} total_write_ms={d} final_drain_ms={d} status_probe_max_ms={d} l0_runs={d} total_run_mb={d} hbc_quant_mb={d}\n",
        .{
            summary.docs,
            summary.dims,
            summary.batch_size,
            cfg.leaf_size,
            cfg.branching_factor,
            @tagName(cfg.kmeans_backend),
            @tagName(cfg.kmeans_update_strategy),
            db_types.publicSyncLevelText(cfg.sync_level),
            summary.writeNsPerDoc(),
            @divTrunc(summary.write_ns, std.time.ns_per_ms),
            @divTrunc(summary.final_drain_ns, std.time.ns_per_ms),
            @divTrunc(summary.status_probe_max_ns, std.time.ns_per_ms),
            summary.dense_lsm_l0_runs,
            @divTrunc(summary.dense_lsm_total_run_bytes, 1024 * 1024),
            @divTrunc(summary.hbc_quant_value_bytes, 1024 * 1024),
        },
    );
    std.debug.print(
        "  profile_ms extract={d} store_write={d} append_replay={d} derived_apply={d} dense_apply={d} dense_embedding_apply={d} index_sync={d} applied_sequence_save={d}\n",
        .{
            @divTrunc(summary.extract_writes_ns, std.time.ns_per_ms),
            @divTrunc(summary.store_write_ns, std.time.ns_per_ms),
            @divTrunc(summary.append_replay_journal_ns, std.time.ns_per_ms),
            @divTrunc(summary.derived_apply_ns, std.time.ns_per_ms),
            @divTrunc(summary.dense_apply_ns, std.time.ns_per_ms),
            @divTrunc(summary.dense_embedding_apply_ns, std.time.ns_per_ms),
            @divTrunc(summary.index_sync_ns, std.time.ns_per_ms),
            @divTrunc(summary.applied_sequence_save_ns, std.time.ns_per_ms),
        },
    );
    std.debug.print(
        "  hbc_ms find_leaf={d} mutate_leaf={d} commit={d} refresh_quantized={d} grouped_items={d} grouped_fallback={d} grouped_leaf_groups={d} recursive_splits={d} split_leaf_calls={d} split_internal_calls={d} deferred_split_steps={d} bulk_leaf_rebuilds={d} bulk_leaf_rebuild_members_max={d}\n",
        .{
            @divTrunc(summary.hbc_insert_find_leaf_ns, std.time.ns_per_ms),
            @divTrunc(summary.hbc_insert_mutate_leaf_ns, std.time.ns_per_ms),
            @divTrunc(summary.hbc_insert_commit_ns, std.time.ns_per_ms),
            @divTrunc(summary.hbc_refresh_quantized_ns, std.time.ns_per_ms),
            summary.hbc_grouped_items,
            summary.hbc_grouped_fallback_items,
            summary.hbc_grouped_leaf_groups,
            summary.hbc_grouped_recursive_splits,
            summary.hbc_split_leaf_calls,
            summary.hbc_split_internal_calls,
            summary.hbc_deferred_leaf_split_steps,
            summary.hbc_bulk_leaf_rebuild_calls,
            summary.hbc_bulk_leaf_rebuild_members_max,
        },
    );
    std.debug.print(
        "  kmeans assignment_cpu_calls={d} assignment_metal_calls={d} assignment_cpu_ms={d} assignment_metal_ms={d} update_cpu_calls={d} update_metal_calls={d} update_cpu_ms={d} update_metal_ms={d}\n",
        .{
            summary.hbc_kmeans_assignment_cpu_calls,
            summary.hbc_kmeans_assignment_metal_calls,
            @divTrunc(summary.hbc_kmeans_assignment_cpu_ns, std.time.ns_per_ms),
            @divTrunc(summary.hbc_kmeans_assignment_metal_ns, std.time.ns_per_ms),
            summary.hbc_kmeans_update_cpu_calls,
            summary.hbc_kmeans_update_metal_calls,
            @divTrunc(summary.hbc_kmeans_update_cpu_ns, std.time.ns_per_ms),
            @divTrunc(summary.hbc_kmeans_update_metal_ns, std.time.ns_per_ms),
        },
    );
}

fn enforceGuardrails(cfg: Config, summary: IngestSummary) !void {
    if (cfg.max_write_ns_per_doc) |limit| {
        if (summary.writeNsPerDoc() > limit) return error.GuardrailFailed;
    }
    if (cfg.max_status_probe_ns) |limit| {
        if (summary.status_probe_max_ns > limit) return error.GuardrailFailed;
    }
    if (cfg.max_dense_lsm_run_bytes) |limit| {
        if (summary.dense_lsm_total_run_bytes > limit) return error.GuardrailFailed;
    }
    if (cfg.max_dense_l0_runs) |limit| {
        if (summary.dense_lsm_l0_runs > limit) return error.GuardrailFailed;
    }
    if (cfg.max_hbc_quant_value_bytes) |limit| {
        if (summary.hbc_quant_value_bytes > limit) return error.GuardrailFailed;
    }
}

fn encodeVectorDocJson(alloc: std.mem.Allocator, vector: []const f32) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"embedding\":[");
    for (vector, 0..) |value, i| {
        if (i != 0) try out.append(alloc, ',');
        var num_buf: [32]u8 = undefined;
        const rendered = try std.fmt.bufPrint(&num_buf, "{d}", .{value});
        try out.appendSlice(alloc, rendered);
    }
    try out.appendSlice(alloc, "]}");
    return out.toOwnedSlice(alloc);
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

fn sleepMs(duration_ms: u64) void {
    if (duration_ms == 0) return;
    const deadline = nowNs() +| (duration_ms * std.time.ns_per_ms);
    while (nowNs() < deadline) {
        std.Thread.yield() catch {};
    }
}

fn normalizeInPlace(vec: []f32) void {
    var sum_sq: f64 = 0;
    for (vec) |value| sum_sq += @as(f64, value) * @as(f64, value);
    if (sum_sq == 0) return;
    const inv = @as(f32, @floatCast(1.0 / std.math.sqrt(sum_sq)));
    for (vec) |*value| value.* *= inv;
}

fn tempPath(buf: []u8) [*:0]const u8 {
    const path_bytes = std.fmt.bufPrint(buf, "/tmp/antfly-dense-ingest-{d}\x00", .{nowNs()}) catch unreachable;
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
