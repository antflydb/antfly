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
const platform_time = antfly.platform_time;
const resource_manager_mod = antfly.resource_manager;

const db_mod = antfly.db;
const replay_stream_mod = db_mod.replay_stream;

const Mode = enum {
    write,
    catchup,
    full,
};

const Workload = enum {
    documents,
    explicit_full_text,
    generated_chunked_full_text,
    explicit_dense,
    generated_dense,
    generated_chunked_dense,
};

const PrimaryKind = enum {
    lsm,
    lsm_memory,
    mem,
    lmdb,
};

const ResourceProfile = enum {
    normal,
    full_text_stress,
};

const TextCompactMode = enum {
    strict,
    best_effort,
};

const Config = struct {
    mode: Mode = .full,
    workload: Workload = .generated_chunked_dense,
    primary: PrimaryKind = .lsm,
    docs: usize = 10_000,
    overwrite_passes: usize = 1,
    body_repeat: usize = 1,
    dims: usize = 1536,
    batch_size: usize = 500,
    seed: u64 = 42,
    bulk_session: bool = false,
    sync_level: db_mod.types.SyncLevel = .write,
    stage_backlog: bool = false,
    force_text_compact: bool = false,
    text_compact_mode: TextCompactMode = .strict,
    resource_profile: ResourceProfile = .normal,
};

const ReplayStats = struct {
    last_sequence: u64 = 0,
    entries: usize = 0,
    payload_bytes: usize = 0,
};

const TextIndexSnapshotStats = struct {
    segments: usize = 0,
    bytes: usize = 0,
};

const BenchSliceStats = struct {
    used_bytes: u64 = 0,
    peak_bytes: u64 = 0,
    soft_limit_events: u64 = 0,
    hard_limit_rejections: u64 = 0,
    pressure: resource_manager_mod.Pressure = .normal,
};

const BenchResourceStats = struct {
    replay_window: BenchSliceStats = .{},
    full_text_pending_segments: BenchSliceStats = .{},
    derived_backlog: BenchSliceStats = .{},
    text_merge_buffers: BenchSliceStats = .{},
};

const WriteSummary = struct {
    docs: usize = 0,
    batches: usize = 0,
    write_sync_level: db_mod.types.SyncLevel = .write,
    write_ns: u64 = 0,
    max_batch_ns: u64 = 0,
    profile: db_mod.BatchProfile = .{},
    replay: ReplayStats = .{},
    pending_target_sequence: u64 = 0,
    enrichment_target_sequence: u64 = 0,
    enrichment_applied_sequence: u64 = 0,
    enrichment_processed_requests: u64 = 0,
    text_merge: db_mod.types.TextMergeStats = .{},
    text_index: TextIndexSnapshotStats = .{},
    resources: BenchResourceStats = .{},
    async_indexing: db_mod.types.AsyncIndexingStats = .{},
};

const CatchupSummary = struct {
    open_ns: u64 = 0,
    catchup_ns: u64 = 0,
    enrichment_ns: u64 = 0,
    derived_ns: u64 = 0,
    idle_ns: u64 = 0,
    force_text_compact_ns: u64 = 0,
    idle_deferred_for_pressure: bool = false,
    force_text_compact_deferred_for_pressure: bool = false,
    replay_before: ReplayStats = .{},
    replay_after_enrichment: ReplayStats = .{},
    replay_after: ReplayStats = .{},
    driven_derived_target: u64 = 0,
    driven_enrichment_target: u64 = 0,
    pending_target_before: u64 = 0,
    pending_target_after: u64 = 0,
    enrichment_target_before: u64 = 0,
    enrichment_target_after: u64 = 0,
    enrichment_applied_before: u64 = 0,
    enrichment_applied_after: u64 = 0,
    enrichment_processed_requests_after: u64 = 0,
    text_merge_before: db_mod.types.TextMergeStats = .{},
    text_merge_after: db_mod.types.TextMergeStats = .{},
    text_index_before: TextIndexSnapshotStats = .{},
    text_index_after_idle: TextIndexSnapshotStats = .{},
    text_index_after_force_compact: TextIndexSnapshotStats = .{},
    resources_before: BenchResourceStats = .{},
    resources_after: BenchResourceStats = .{},
    hbc_insert_calls: u64 = 0,
    hbc_grouped_items: u64 = 0,
    hbc_grouped_fallback_items: u64 = 0,
    hbc_insert_find_leaf_ns: u64 = 0,
    hbc_insert_mutate_leaf_ns: u64 = 0,
    hbc_insert_commit_ns: u64 = 0,
    hbc_refresh_quantized_ns: u64 = 0,
    dense_lsm_total_runs: u64 = 0,
    dense_lsm_total_run_bytes: u64 = 0,
    dense_lsm_l0_runs: u64 = 0,
};

pub fn main(init: std.process.Init) !void {
    const alloc = std.heap.c_allocator;
    const cfg = try parseArgs(init.minimal.args);

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    const write_summary = try seedReplayDB(alloc, std.mem.span(path), cfg);
    printWriteSummary(cfg, write_summary);
    if (cfg.mode == .write) return;

    const catchup_summary = try runCatchupBench(alloc, std.mem.span(path), cfg);
    printCatchupSummary(cfg, catchup_summary);
}

fn parseArgs(args_in: std.process.Args) !Config {
    var cfg = Config{};
    var saw_bulk_session = false;
    var args = std.process.Args.Iterator.init(args_in);
    _ = args.skip();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--mode")) {
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.mode = parseMode(raw) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--workload")) {
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.workload = parseWorkload(raw) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--primary")) {
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.primary = parsePrimary(raw) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--docs")) {
            cfg.docs = try parseNextUsize(&args, "--docs");
        } else if (std.mem.eql(u8, arg, "--overwrite-passes")) {
            cfg.overwrite_passes = try parseNextUsize(&args, "--overwrite-passes");
        } else if (std.mem.eql(u8, arg, "--body-repeat")) {
            cfg.body_repeat = try parseNextUsize(&args, "--body-repeat");
        } else if (std.mem.eql(u8, arg, "--dims")) {
            cfg.dims = try parseNextUsize(&args, "--dims");
        } else if (std.mem.eql(u8, arg, "--batch-size")) {
            cfg.batch_size = try parseNextUsize(&args, "--batch-size");
        } else if (std.mem.eql(u8, arg, "--seed")) {
            cfg.seed = try parseNextU64(&args, "--seed");
        } else if (std.mem.eql(u8, arg, "--bulk-session")) {
            cfg.bulk_session = true;
            saw_bulk_session = true;
        } else if (std.mem.eql(u8, arg, "--sync-level")) {
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.sync_level = db_mod.types.parsePublicSyncLevelText(raw) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--stage-backlog")) {
            cfg.stage_backlog = true;
        } else if (std.mem.eql(u8, arg, "--force-text-compact")) {
            cfg.force_text_compact = true;
        } else if (std.mem.eql(u8, arg, "--text-compact-mode")) {
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.text_compact_mode = parseTextCompactMode(raw) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--resource-profile")) {
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.resource_profile = parseResourceProfile(raw) orelse return error.InvalidArgument;
        } else {
            std.debug.print("invalid argument: {s}\n", .{arg});
            return error.InvalidArgument;
        }
    }

    if (!saw_bulk_session and switch (cfg.workload) {
        .generated_dense, .generated_chunked_dense => true,
        else => false,
    }) {
        cfg.bulk_session = true;
    }
    if (cfg.docs == 0 or cfg.overwrite_passes == 0 or cfg.body_repeat == 0 or cfg.dims == 0 or cfg.batch_size == 0) return error.InvalidArgument;
    return cfg;
}

fn parseMode(raw: []const u8) ?Mode {
    if (std.mem.eql(u8, raw, "write")) return .write;
    if (std.mem.eql(u8, raw, "catchup")) return .catchup;
    if (std.mem.eql(u8, raw, "full")) return .full;
    return null;
}

fn parseWorkload(raw: []const u8) ?Workload {
    if (std.mem.eql(u8, raw, "documents")) return .documents;
    if (std.mem.eql(u8, raw, "explicit_full_text")) return .explicit_full_text;
    if (std.mem.eql(u8, raw, "generated_chunked_full_text")) return .generated_chunked_full_text;
    if (std.mem.eql(u8, raw, "explicit_dense")) return .explicit_dense;
    if (std.mem.eql(u8, raw, "generated_dense")) return .generated_dense;
    if (std.mem.eql(u8, raw, "generated_chunked_dense")) return .generated_chunked_dense;
    return null;
}

fn parsePrimary(raw: []const u8) ?PrimaryKind {
    if (std.mem.eql(u8, raw, "lsm")) return .lsm;
    if (std.mem.eql(u8, raw, "lsm_memory")) return .lsm_memory;
    if (std.mem.eql(u8, raw, "mem")) return .mem;
    if (std.mem.eql(u8, raw, "lmdb")) return .lmdb;
    return null;
}

fn parseResourceProfile(raw: []const u8) ?ResourceProfile {
    if (std.mem.eql(u8, raw, "normal")) return .normal;
    if (std.mem.eql(u8, raw, "full_text_stress")) return .full_text_stress;
    return null;
}

fn parseTextCompactMode(raw: []const u8) ?TextCompactMode {
    if (std.mem.eql(u8, raw, "strict")) return .strict;
    if (std.mem.eql(u8, raw, "best_effort")) return .best_effort;
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

fn seedReplayDB(alloc: std.mem.Allocator, path: []const u8, cfg: Config) !WriteSummary {
    var deterministic_dense = db_mod.embedder.DeterministicDenseEmbedder{};
    var opts = openOptions(cfg, true);
    var resource_manager = resource_manager_mod.ResourceManager.init(resourceManagerOptions(cfg));
    opts.resource_manager = &resource_manager;
    if (workloadNeedsEnrichment(cfg.workload)) {
        opts.enrichment = .{
            .owner_id = "replay-bench",
            .dense_embedder = deterministic_dense.interface(),
        };
    }
    var db = try db_mod.DB.open(alloc, path, opts);
    defer db.close();

    try configureWorkload(alloc, &db, cfg);

    var bulk_session_open = false;
    if (cfg.bulk_session) {
        try db.beginBulkIngestSession();
        bulk_session_open = true;
        errdefer if (bulk_session_open) db.abortBulkIngestSession();
    }

    const writes_buf = try alloc.alloc(db_mod.types.BatchWrite, cfg.batch_size);
    defer alloc.free(writes_buf);

    var empty_vector: [0]f32 = .{};
    const vector_buf = if (cfg.workload == .explicit_dense) try alloc.alloc(f32, cfg.dims) else empty_vector[0..];
    defer if (cfg.workload == .explicit_dense) alloc.free(vector_buf);

    var summary = WriteSummary{ .docs = cfg.docs };
    const write_sync_level: db_mod.types.SyncLevel = if (cfg.stage_backlog) .write else cfg.sync_level;
    summary.write_sync_level = write_sync_level;
    for (0..cfg.overwrite_passes) |pass_idx| {
        var start: usize = 0;
        while (start < cfg.docs) : (start += cfg.batch_size) {
            const end = @min(start + cfg.batch_size, cfg.docs);
            {
                const writes = writes_buf[0 .. end - start];
                for (start..end, 0..) |doc_idx, i| {
                    writes[i] = try makeBatchWrite(alloc, cfg, doc_idx, pass_idx, vector_buf);
                }
                defer {
                    for (writes) |write| {
                        alloc.free(write.key);
                        alloc.free(write.value);
                    }
                }

                var profile: db_mod.BatchProfile = .{};
                const started = nowNs();
                try db.batchProfiled(.{
                    .writes = writes,
                    .sync_level = write_sync_level,
                }, &profile);
                const wall_ns = elapsedSince(started);
                summary.write_ns += wall_ns;
                summary.max_batch_ns = @max(summary.max_batch_ns, wall_ns);
                addBatchProfile(&summary.profile, profile);
                summary.batches += 1;
            }
        }
    }

    if (cfg.bulk_session) {
        try db.finishBulkIngestSessionWithOptions(.{
            .compact = false,
            .max_deferred_l0_runs = 64,
        });
        bulk_session_open = false;
    }

    summary.replay = try snapshotReplayStats(alloc, &db);
    const pending = db.pendingWorkStats();
    summary.pending_target_sequence = pending.derived_target_sequence;
    summary.enrichment_target_sequence = pending.enrichment.target_sequence;
    summary.enrichment_applied_sequence = pending.enrichment.applied_sequence;
    summary.enrichment_processed_requests = pending.enrichment.processed_requests;
    summary.text_merge = pending.text_merge;
    summary.text_index = snapshotTextIndexStats(&db, "ft_idx");
    summary.resources = captureBenchResourceStats(&resource_manager);
    summary.async_indexing = db.snapshotAsyncIndexingStats();
    return summary;
}

fn runCatchupBench(alloc: std.mem.Allocator, path: []const u8, cfg: Config) !CatchupSummary {
    var deterministic_dense = db_mod.embedder.DeterministicDenseEmbedder{};
    var opts = openOptions(cfg, false);
    var resource_manager = resource_manager_mod.ResourceManager.init(resourceManagerOptions(cfg));
    opts.resource_manager = &resource_manager;
    if (workloadNeedsEnrichment(cfg.workload)) {
        opts.enrichment = .{
            .owner_id = "replay-bench",
            .dense_embedder = deterministic_dense.interface(),
        };
    }
    const open_started = nowNs();
    var db = try db_mod.DB.open(alloc, path, opts);
    defer db.close();

    var summary = CatchupSummary{
        .open_ns = elapsedSince(open_started),
    };
    const replay_before = try snapshotReplayStats(alloc, &db);
    summary.replay_before = replay_before;
    const pending_before = db.pendingWorkStats();
    summary.pending_target_before = pending_before.derived_target_sequence;
    summary.enrichment_target_before = pending_before.enrichment.target_sequence;
    summary.enrichment_applied_before = pending_before.enrichment.applied_sequence;
    summary.text_merge_before = pending_before.text_merge;
    summary.text_index_before = snapshotTextIndexStats(&db, "ft_idx");
    summary.resources_before = captureBenchResourceStats(&resource_manager);

    const catchup_started = nowNs();
    if (pending_before.enrichment.target_sequence > pending_before.enrichment.applied_sequence) {
        summary.driven_enrichment_target = pending_before.enrichment.target_sequence;
        const enrichment_started = nowNs();
        try db.runEnrichmentUntil(pending_before.enrichment.target_sequence);
        summary.enrichment_ns = elapsedSince(enrichment_started);
    }
    const replay_after_enrichment = try snapshotReplayStats(alloc, &db);
    summary.replay_after_enrichment = replay_after_enrichment;
    const derived_target = @max(
        replay_before.last_sequence,
        @max(replay_after_enrichment.last_sequence, pending_before.derived_target_sequence),
    );
    if (derived_target != 0) {
        summary.driven_derived_target = derived_target;
        const derived_started = nowNs();
        try db.runDerivedUntil(derived_target);
        summary.derived_ns = elapsedSince(derived_started);
    }
    const idle_started = nowNs();
    db.runUntilIdle() catch |err| switch (err) {
        error.ResourceBudgetExceeded => summary.idle_deferred_for_pressure = true,
        else => return err,
    };
    summary.idle_ns = elapsedSince(idle_started);
    summary.text_index_after_idle = snapshotTextIndexStats(&db, "ft_idx");
    if (cfg.force_text_compact) {
        const compact_started = nowNs();
        switch (cfg.text_compact_mode) {
            .strict => try db.forceCompactTextIndexes(),
            .best_effort => db.bestEffortForceCompactTextIndexes() catch |err| switch (err) {
                error.ResourceBudgetExceeded => summary.force_text_compact_deferred_for_pressure = true,
                else => return err,
            },
        }
        summary.force_text_compact_ns = elapsedSince(compact_started);
    }
    summary.catchup_ns = elapsedSince(catchup_started);

    const pending_after = db.pendingWorkStats();
    summary.pending_target_after = pending_after.derived_target_sequence;
    summary.enrichment_target_after = pending_after.enrichment.target_sequence;
    summary.enrichment_applied_after = pending_after.enrichment.applied_sequence;
    summary.enrichment_processed_requests_after = pending_after.enrichment.processed_requests;
    summary.text_merge_after = pending_after.text_merge;
    summary.text_index_after_force_compact = snapshotTextIndexStats(&db, "ft_idx");
    summary.resources_after = captureBenchResourceStats(&resource_manager);
    summary.replay_after = try snapshotReplayStats(alloc, &db);

    if (db.core.index_manager.denseIndex("dense_idx")) |entry| {
        const profile = entry.index.getWriteProfile();
        summary.hbc_insert_calls = profile.insert_calls;
        summary.hbc_grouped_items = profile.grouped_items;
        summary.hbc_grouped_fallback_items = profile.grouped_fallback_items;
        summary.hbc_insert_find_leaf_ns = profile.insert_find_leaf_ns;
        summary.hbc_insert_mutate_leaf_ns = profile.insert_mutate_leaf_ns;
        summary.hbc_insert_commit_ns = profile.insert_commit_ns;
        summary.hbc_refresh_quantized_ns = profile.refresh_quantized_ns;
        if (entry.index.snapshotLsmMaintenanceStats()) |stats| {
            summary.dense_lsm_total_runs = stats.total_runs;
            summary.dense_lsm_total_run_bytes = stats.total_run_bytes;
            summary.dense_lsm_l0_runs = stats.l0_runs;
        }
    }

    return summary;
}

fn openOptions(cfg: Config, start_index_workers: bool) db_mod.OpenOptions {
    var opts: db_mod.OpenOptions = .{
        .start_index_workers = start_index_workers,
    };
    switch (cfg.primary) {
        .lsm => {},
        .lsm_memory => opts.primary_backend = .{ .lsm_memory = .{} },
        .mem => opts.primary_backend = .{ .mem = .{} },
        .lmdb => opts.primary_backend = .lmdb,
    }
    return opts;
}

fn resourceManagerOptions(cfg: Config) resource_manager_mod.Options {
    var opts: resource_manager_mod.Options = .{};
    switch (cfg.resource_profile) {
        .normal => {},
        .full_text_stress => {
            opts.budgets[@intFromEnum(resource_manager_mod.Slice.full_text_pending_segments)] = .{
                .soft_limit_bytes = 256 * 1024,
                .hard_limit_bytes = 512 * 1024,
            };
            opts.budgets[@intFromEnum(resource_manager_mod.Slice.text_merge_buffers)] = .{
                .soft_limit_bytes = 512 * 1024,
                .hard_limit_bytes = 1024 * 1024,
            };
            opts.budgets[@intFromEnum(resource_manager_mod.Slice.derived_backlog)] = .{
                .soft_limit_bytes = 512 * 1024,
                .hard_limit_bytes = 1024 * 1024,
            };
            opts.budgets[@intFromEnum(resource_manager_mod.Slice.derived_replay_window)] = .{
                .soft_limit_bytes = 256 * 1024,
                .hard_limit_bytes = 512 * 1024,
            };
        },
    }
    return opts;
}

fn workloadNeedsEnrichment(workload: Workload) bool {
    return switch (workload) {
        .generated_dense, .generated_chunked_dense, .generated_chunked_full_text => true,
        else => false,
    };
}

fn configureWorkload(alloc: std.mem.Allocator, db: *db_mod.DB, cfg: Config) !void {
    const index_cfg = switch (cfg.workload) {
        .documents => return,
        .explicit_full_text => "{\"field\":\"body\"}",
        .generated_chunked_full_text => "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"artifact_name\":\"body_chunks_v1\",\"chunker\":{\"provider\":\"antfly\",\"store_chunks\":false,\"full_text_index\":{},\"text\":{\"target_tokens\":8,\"overlap_tokens\":2}}}}",
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
    if (cfg.workload == .explicit_full_text) {
        try db.addIndex(.{
            .name = "ft_idx",
            .kind = .full_text,
            .config_json = index_cfg,
        });
        return;
    }
    if (cfg.workload == .generated_chunked_full_text) {
        try db.addIndex(.{
            .name = "ft_idx",
            .kind = .full_text,
            .config_json = "{}",
        });
        try db.addIndex(.{
            .name = "chunk_driver_idx",
            .kind = .dense_vector,
            .config_json = index_cfg,
        });
        return;
    }

    defer alloc.free(index_cfg);
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = index_cfg,
    });
    if (db.core.index_manager.denseIndex("dense_idx")) |entry| entry.index.resetWriteProfile();
}

fn makeBatchWrite(
    alloc: std.mem.Allocator,
    cfg: Config,
    doc_idx: usize,
    pass_idx: usize,
    vector_buf: []f32,
) !db_mod.types.BatchWrite {
    const key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{doc_idx});
    const value = switch (cfg.workload) {
        .documents => try encodeDocumentJsonAlloc(alloc, doc_idx, pass_idx, cfg),
        .explicit_full_text => try encodeDocumentJsonAlloc(alloc, doc_idx, pass_idx, cfg),
        .generated_dense, .generated_chunked_dense, .generated_chunked_full_text => try encodeDocumentJsonAlloc(alloc, doc_idx, pass_idx, cfg),
        .explicit_dense => try encodeVectorDocJsonAlloc(alloc, doc_idx, cfg, vector_buf),
    };
    return .{
        .key = key,
        .value = value,
    };
}

fn encodeDocumentJsonAlloc(alloc: std.mem.Allocator, doc_idx: usize, pass_idx: usize, cfg: Config) ![]u8 {
    const body = try generatedBodyTextAlloc(alloc, doc_idx, pass_idx, cfg);
    defer alloc.free(body);
    return try std.fmt.allocPrint(
        alloc,
        "{{\"title\":\"doc-{d}\",\"body\":\"{s}\"}}",
        .{ doc_idx, body },
    );
}

fn encodeVectorDocJsonAlloc(alloc: std.mem.Allocator, doc_idx: usize, cfg: Config, vector: []f32) ![]u8 {
    var norm_sq: f32 = 0;
    for (vector, 0..) |*slot, dim_idx| {
        const noise = deterministicNoise(cfg.seed, doc_idx, dim_idx);
        const cluster = @as(f32, @floatFromInt(doc_idx % 8)) * 0.25;
        slot.* = cluster + noise;
        norm_sq += slot.* * slot.*;
    }
    const inv_norm: f32 = 1.0 / @sqrt(norm_sq);
    for (vector) |*slot| slot.* *= inv_norm;
    return try std.fmt.allocPrint(
        alloc,
        "{{\"title\":\"doc-{d}\",\"embedding\":{f}}}",
        .{ doc_idx, std.json.fmt(vector, .{}) },
    );
}

fn generatedBodyTextAlloc(alloc: std.mem.Allocator, doc_idx: usize, pass_idx: usize, cfg: Config) ![]u8 {
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
    var buf = std.ArrayListUnmanaged(u8).empty;
    errdefer buf.deinit(alloc);
    const prefix = try std.fmt.allocPrint(alloc, "document {d} pass {d} topic {s} dims {d}", .{ doc_idx, pass_idx, topic, cfg.dims });
    defer alloc.free(prefix);
    try buf.appendSlice(alloc, prefix);
    for (0..cfg.body_repeat) |repeat_idx| {
        const segment = try std.fmt.allocPrint(alloc, " repeated context {s} token {d}", .{ topic, repeat_idx });
        defer alloc.free(segment);
        try buf.appendSlice(alloc, segment);
    }
    const suffix = try std.fmt.allocPrint(alloc, " tail {d}", .{(doc_idx + pass_idx) % 97});
    defer alloc.free(suffix);
    try buf.appendSlice(alloc, suffix);
    return try buf.toOwnedSlice(alloc);
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

fn snapshotReplayStats(alloc: std.mem.Allocator, db: *db_mod.DB) !ReplayStats {
    const entries = try replay_stream_mod.iterateFrom(alloc, db.core.store, 1);
    defer {
        for (entries) |*entry| entry.deinit(alloc);
        alloc.free(entries);
    }

    var payload_bytes: usize = 0;
    for (entries) |entry| payload_bytes += entry.payload.len;
    return .{
        .last_sequence = if (entries.len == 0) 0 else entries[entries.len - 1].sequence,
        .entries = entries.len,
        .payload_bytes = payload_bytes,
    };
}

fn addBatchProfile(total: *db_mod.BatchProfile, delta: db_mod.BatchProfile) void {
    total.total_ns += delta.total_ns;
    total.resolve_transforms_ns += delta.resolve_transforms_ns;
    total.merge_effective_req_ns += delta.merge_effective_req_ns;
    total.predicates_ns += delta.predicates_ns;
    total.validate_range_ns += delta.validate_range_ns;
    total.extract_writes_ns += delta.extract_writes_ns;
    total.delete_artifacts_ns += delta.delete_artifacts_ns;
    total.precompute_generated_ns += delta.precompute_generated_ns;
    total.store_write_ns += delta.store_write_ns;
    total.split_delta_ns += delta.split_delta_ns;
    total.build_derived_ns += delta.build_derived_ns;
    total.apply_shadow_ns += delta.apply_shadow_ns;
    total.collect_sync_targets_ns += delta.collect_sync_targets_ns;
    total.append_replay_journal_ns += delta.append_replay_journal_ns;
    total.wait_sync_ns += delta.wait_sync_ns;
    total.backlog_pressure_ns += delta.backlog_pressure_ns;
    total.executor_notify_ns += delta.executor_notify_ns;
    total.derived_apply_ns += delta.derived_apply_ns;
    total.sync_wait_ns += delta.sync_wait_ns;
    total.full_text_apply_ns += delta.full_text_apply_ns;
    total.dense_apply_ns += delta.dense_apply_ns;
    total.dense_delete_ns += delta.dense_delete_ns;
    total.dense_doc_index_ns += delta.dense_doc_index_ns;
    total.dense_embedding_apply_ns += delta.dense_embedding_apply_ns;
    total.sparse_apply_ns += delta.sparse_apply_ns;
    total.graph_apply_ns += delta.graph_apply_ns;
    total.index_sync_ns += delta.index_sync_ns;
    total.applied_sequence_save_ns += delta.applied_sequence_save_ns;
    total.replay_journal_truncate_ns += delta.replay_journal_truncate_ns;
    total.notify_enrichment_ns += delta.notify_enrichment_ns;
    total.hbc_insert_calls += delta.hbc_insert_calls;
    total.hbc_grouped_items += delta.hbc_grouped_items;
    total.hbc_grouped_fallback_items += delta.hbc_grouped_fallback_items;
    total.hbc_insert_find_leaf_ns += delta.hbc_insert_find_leaf_ns;
    total.hbc_insert_mutate_leaf_ns += delta.hbc_insert_mutate_leaf_ns;
    total.hbc_insert_commit_ns += delta.hbc_insert_commit_ns;
    total.hbc_refresh_quantized_ns += delta.hbc_refresh_quantized_ns;
}

fn printWriteSummary(cfg: Config, summary: WriteSummary) void {
    std.debug.print(
        "replay_bench_write mode={s} workload={s} primary={s} docs={d} dims={d} batch_size={d} batches={d} sync={s} write_ms={d:.3} max_batch_ms={d:.3} replay_seq={d} replay_entries={d} replay_payload_bytes={d} pending_target={d} enrichment_target={d} enrichment_applied={d} enrichment_processed={d}\n",
        .{
            @tagName(cfg.mode),
            @tagName(cfg.workload),
            @tagName(cfg.primary),
            cfg.docs,
            cfg.dims,
            cfg.batch_size,
            summary.batches,
            db_mod.types.publicSyncLevelText(summary.write_sync_level),
            nsToMsFloat(summary.write_ns),
            nsToMsFloat(summary.max_batch_ns),
            summary.replay.last_sequence,
            summary.replay.entries,
            summary.replay.payload_bytes,
            summary.pending_target_sequence,
            summary.enrichment_target_sequence,
            summary.enrichment_applied_sequence,
            summary.enrichment_processed_requests,
        },
    );
    std.debug.print(
        "replay_bench_write_shape overwrite_passes={d} body_repeat={d}\n",
        .{ cfg.overwrite_passes, cfg.body_repeat },
    );
    std.debug.print(
        "replay_bench_write_profile append_replay_journal_ms={d:.3} build_derived_ms={d:.3} precompute_generated_ms={d:.3} store_write_ms={d:.3} derived_apply_ms={d:.3} full_text_apply_ms={d:.3} dense_apply_ms={d:.3} dense_doc_index_ms={d:.3} dense_embedding_apply_ms={d:.3} sparse_apply_ms={d:.3} graph_apply_ms={d:.3} replay_journal_truncate_ms={d:.3} hbc_insert_calls={d} hbc_grouped_items={d} hbc_grouped_fallback_items={d} hbc_insert_commit_ms={d:.3} text_merge_pending_segments={d} text_merge_pending_bytes={d} text_merge_completed={d} text_merge_backpressure_events={d}\n",
        .{
            nsToMsFloat(summary.profile.append_replay_journal_ns),
            nsToMsFloat(summary.profile.build_derived_ns),
            nsToMsFloat(summary.profile.precompute_generated_ns),
            nsToMsFloat(summary.profile.store_write_ns),
            nsToMsFloat(summary.profile.derived_apply_ns),
            nsToMsFloat(summary.profile.full_text_apply_ns),
            nsToMsFloat(summary.profile.dense_apply_ns),
            nsToMsFloat(summary.profile.dense_doc_index_ns),
            nsToMsFloat(summary.profile.dense_embedding_apply_ns),
            nsToMsFloat(summary.profile.sparse_apply_ns),
            nsToMsFloat(summary.profile.graph_apply_ns),
            nsToMsFloat(summary.profile.replay_journal_truncate_ns),
            summary.profile.hbc_insert_calls,
            summary.profile.hbc_grouped_items,
            summary.profile.hbc_grouped_fallback_items,
            nsToMsFloat(summary.profile.hbc_insert_commit_ns),
            summary.text_merge.pending_segments,
            summary.text_merge.pending_bytes,
            summary.text_merge.completed_merges,
            summary.text_merge.backpressure_events,
        },
    );
    std.debug.print(
        "replay_bench_write_text_index segments={d} bytes={d}\n",
        .{ summary.text_index.segments, summary.text_index.bytes },
    );
    std.debug.print(
        "replay_bench_write_bulk_coalescing active={any} staged_keys={d} stage_batches={d} stage_writes={d} stage_deletes={d} stage_transforms={d} flush_calls={d} flushed_keys={d}\n",
        .{
            summary.async_indexing.bulk_coalescing.active_session,
            summary.async_indexing.bulk_coalescing.staged_keys,
            summary.async_indexing.bulk_coalescing.stage_batches,
            summary.async_indexing.bulk_coalescing.stage_writes,
            summary.async_indexing.bulk_coalescing.stage_deletes,
            summary.async_indexing.bulk_coalescing.stage_transforms,
            summary.async_indexing.bulk_coalescing.flush_calls,
            summary.async_indexing.bulk_coalescing.flushed_keys,
        },
    );
    printResourceSummary("replay_bench_write_resources", summary.resources);
}

fn printCatchupSummary(cfg: Config, summary: CatchupSummary) void {
    std.debug.print(
        "replay_bench_catchup workload={s} primary={s} docs={d} dims={d} batch_size={d} text_compact_mode={s} open_ms={d:.3} catchup_ms={d:.3} enrichment_ms={d:.3} derived_ms={d:.3} idle_ms={d:.3} force_text_compact_ms={d:.3} idle_deferred={any} force_text_compact_deferred={any} replay_seq={d} replay_entries={d} replay_payload_bytes={d} pending_before={d} pending_after={d} enrichment_target_before={d} enrichment_target_after={d} enrichment_applied_before={d} enrichment_applied_after={d} enrichment_processed_after={d}\n",
        .{
            @tagName(cfg.workload),
            @tagName(cfg.primary),
            cfg.docs,
            cfg.dims,
            cfg.batch_size,
            @tagName(cfg.text_compact_mode),
            nsToMsFloat(summary.open_ns),
            nsToMsFloat(summary.catchup_ns),
            nsToMsFloat(summary.enrichment_ns),
            nsToMsFloat(summary.derived_ns),
            nsToMsFloat(summary.idle_ns),
            nsToMsFloat(summary.force_text_compact_ns),
            summary.idle_deferred_for_pressure,
            summary.force_text_compact_deferred_for_pressure,
            summary.replay_after.last_sequence,
            summary.replay_after.entries,
            summary.replay_after.payload_bytes,
            summary.pending_target_before,
            summary.pending_target_after,
            summary.enrichment_target_before,
            summary.enrichment_target_after,
            summary.enrichment_applied_before,
            summary.enrichment_applied_after,
            summary.enrichment_processed_requests_after,
        },
    );
    std.debug.print(
        "replay_bench_catchup_shape overwrite_passes={d} body_repeat={d}\n",
        .{ cfg.overwrite_passes, cfg.body_repeat },
    );
    std.debug.print(
        "replay_bench_catchup_replay before_seq={d} before_entries={d} before_payload_bytes={d} after_enrichment_seq={d} after_enrichment_entries={d} after_enrichment_payload_bytes={d} driven_enrichment_target={d} driven_derived_target={d}\n",
        .{
            summary.replay_before.last_sequence,
            summary.replay_before.entries,
            summary.replay_before.payload_bytes,
            summary.replay_after_enrichment.last_sequence,
            summary.replay_after_enrichment.entries,
            summary.replay_after_enrichment.payload_bytes,
            summary.driven_enrichment_target,
            summary.driven_derived_target,
        },
    );
    std.debug.print(
        "replay_bench_catchup_text_merge pending_segments_before={d} pending_segments_after={d} pending_bytes_before={d} pending_bytes_after={d} completed_before={d} completed_after={d} backpressure_events_before={d} backpressure_events_after={d}\n",
        .{
            summary.text_merge_before.pending_segments,
            summary.text_merge_after.pending_segments,
            summary.text_merge_before.pending_bytes,
            summary.text_merge_after.pending_bytes,
            summary.text_merge_before.completed_merges,
            summary.text_merge_after.completed_merges,
            summary.text_merge_before.backpressure_events,
            summary.text_merge_after.backpressure_events,
        },
    );
    std.debug.print(
        "replay_bench_catchup_text_index segments_before={d} segments_after_idle={d} segments_after_force_compact={d} bytes_before={d} bytes_after_idle={d} bytes_after_force_compact={d}\n",
        .{
            summary.text_index_before.segments,
            summary.text_index_after_idle.segments,
            summary.text_index_after_force_compact.segments,
            summary.text_index_before.bytes,
            summary.text_index_after_idle.bytes,
            summary.text_index_after_force_compact.bytes,
        },
    );
    printCatchupResourceSummary(summary.resources_before, summary.resources_after);
    std.debug.print(
        "replay_bench_catchup_hbc insert_calls={d} grouped_items={d} grouped_fallback_items={d} insert_find_leaf_ms={d:.3} insert_mutate_leaf_ms={d:.3} insert_commit_ms={d:.3} refresh_quantized_ms={d:.3} dense_lsm_total_runs={d} dense_lsm_total_run_bytes={d} dense_lsm_l0_runs={d}\n",
        .{
            summary.hbc_insert_calls,
            summary.hbc_grouped_items,
            summary.hbc_grouped_fallback_items,
            nsToMsFloat(summary.hbc_insert_find_leaf_ns),
            nsToMsFloat(summary.hbc_insert_mutate_leaf_ns),
            nsToMsFloat(summary.hbc_insert_commit_ns),
            nsToMsFloat(summary.hbc_refresh_quantized_ns),
            summary.dense_lsm_total_runs,
            summary.dense_lsm_total_run_bytes,
            summary.dense_lsm_l0_runs,
        },
    );
}

fn captureBenchResourceStats(manager: *resource_manager_mod.ResourceManager) BenchResourceStats {
    return .{
        .replay_window = captureSliceStats(manager.sliceStats(.derived_replay_window)),
        .full_text_pending_segments = captureSliceStats(manager.sliceStats(.full_text_pending_segments)),
        .derived_backlog = captureSliceStats(manager.sliceStats(.derived_backlog)),
        .text_merge_buffers = captureSliceStats(manager.sliceStats(.text_merge_buffers)),
    };
}

fn captureSliceStats(stats: resource_manager_mod.SliceStats) BenchSliceStats {
    return .{
        .used_bytes = stats.used_bytes,
        .peak_bytes = stats.peak_bytes,
        .soft_limit_events = stats.soft_limit_events,
        .hard_limit_rejections = stats.hard_limit_rejections,
        .pressure = stats.pressure,
    };
}

fn printResourceSummary(prefix: []const u8, stats: BenchResourceStats) void {
    std.debug.print(
        "{s} replay_window_used={d} replay_window_peak={d} replay_window_soft={d} replay_window_hard={d} replay_window_pressure={s} full_text_pending_used={d} full_text_pending_peak={d} full_text_pending_soft={d} full_text_pending_hard={d} full_text_pending_pressure={s} derived_backlog_used={d} derived_backlog_peak={d} derived_backlog_soft={d} derived_backlog_hard={d} derived_backlog_pressure={s} text_merge_buffers_used={d} text_merge_buffers_peak={d} text_merge_buffers_soft={d} text_merge_buffers_hard={d} text_merge_buffers_pressure={s}\n",
        .{
            prefix,
            stats.replay_window.used_bytes,
            stats.replay_window.peak_bytes,
            stats.replay_window.soft_limit_events,
            stats.replay_window.hard_limit_rejections,
            @tagName(stats.replay_window.pressure),
            stats.full_text_pending_segments.used_bytes,
            stats.full_text_pending_segments.peak_bytes,
            stats.full_text_pending_segments.soft_limit_events,
            stats.full_text_pending_segments.hard_limit_rejections,
            @tagName(stats.full_text_pending_segments.pressure),
            stats.derived_backlog.used_bytes,
            stats.derived_backlog.peak_bytes,
            stats.derived_backlog.soft_limit_events,
            stats.derived_backlog.hard_limit_rejections,
            @tagName(stats.derived_backlog.pressure),
            stats.text_merge_buffers.used_bytes,
            stats.text_merge_buffers.peak_bytes,
            stats.text_merge_buffers.soft_limit_events,
            stats.text_merge_buffers.hard_limit_rejections,
            @tagName(stats.text_merge_buffers.pressure),
        },
    );
}

fn printCatchupResourceSummary(before: BenchResourceStats, after: BenchResourceStats) void {
    std.debug.print(
        "replay_bench_catchup_resources replay_window_used_before={d} replay_window_used_after={d} replay_window_peak_after={d} replay_window_soft_after={d} full_text_pending_used_before={d} full_text_pending_used_after={d} full_text_pending_peak_after={d} full_text_pending_soft_after={d} derived_backlog_used_before={d} derived_backlog_used_after={d} derived_backlog_peak_after={d} derived_backlog_soft_after={d} text_merge_buffers_used_before={d} text_merge_buffers_used_after={d} text_merge_buffers_peak_after={d} text_merge_buffers_soft_after={d}\n",
        .{
            before.replay_window.used_bytes,
            after.replay_window.used_bytes,
            after.replay_window.peak_bytes,
            after.replay_window.soft_limit_events,
            before.full_text_pending_segments.used_bytes,
            after.full_text_pending_segments.used_bytes,
            after.full_text_pending_segments.peak_bytes,
            after.full_text_pending_segments.soft_limit_events,
            before.derived_backlog.used_bytes,
            after.derived_backlog.used_bytes,
            after.derived_backlog.peak_bytes,
            after.derived_backlog.soft_limit_events,
            before.text_merge_buffers.used_bytes,
            after.text_merge_buffers.used_bytes,
            after.text_merge_buffers.peak_bytes,
            after.text_merge_buffers.soft_limit_events,
        },
    );
}

fn nsToMsFloat(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / @as(f64, std.time.ns_per_ms);
}

fn tempPath(buf: []u8) [*:0]const u8 {
    const ts = nowNs();
    const path_bytes = std.fmt.bufPrint(buf, "/tmp/antfly-replay-bench-{d}\x00", .{ts}) catch unreachable;
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

fn snapshotTextIndexStats(db: *db_mod.DB, index_name: []const u8) TextIndexSnapshotStats {
    const entry = db.core.index_manager.textIndex(index_name) orelse return .{};
    const snap = entry.snapshot();
    var total_bytes: usize = 0;
    for (snap.segments) |seg| total_bytes += seg.data.bytes().len;
    return .{
        .segments = snap.segments.len,
        .bytes = total_bytes,
    };
}
