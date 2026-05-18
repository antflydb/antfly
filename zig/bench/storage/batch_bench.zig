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
const replay_stream_mod = db_mod.replay_stream;
const platform_time = antfly.platform_time;
const resource_manager_mod = antfly.resource_manager;

const Workload = enum {
    documents,
    explicit_full_text,
    explicit_dense,
};

const PrimaryKind = enum {
    lsm,
    lsm_memory,
    mem,
    lmdb,
};

const MutationMode = enum {
    overwrite,
    transform,
};

const Config = struct {
    workload: Workload = .explicit_full_text,
    primary: PrimaryKind = .lsm,
    docs: usize = 10_000,
    overwrite_passes: usize = 1,
    body_repeat: usize = 1,
    dims: usize = 384,
    batch_size: usize = 500,
    seed: u64 = 42,
    bulk_session: bool = false,
    sync_level: db_mod.types.SyncLevel = .write,
    mutation_mode: MutationMode = .overwrite,
};

const ReplayStats = struct {
    last_sequence: u64 = 0,
    entries: usize = 0,
    payload_bytes: usize = 0,
};

const Summary = struct {
    requested_writes: usize = 0,
    requested_transforms: usize = 0,
    batches: usize = 0,
    stage_ns: u64 = 0,
    finish_ns: u64 = 0,
    total_ns: u64 = 0,
    max_batch_ns: u64 = 0,
    profile: db_mod.BatchProfile = .{},
    replay: ReplayStats = .{},
    async_indexing: db_mod.types.AsyncIndexingStats = .{},
};

pub fn main(init: std.process.Init) !void {
    const alloc = std.heap.c_allocator;
    const cfg = try parseArgs(init.minimal.args);

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    const summary = try runBench(alloc, path, cfg);
    printSummary(cfg, summary);
}

fn parseArgs(args_in: std.process.Args) !Config {
    var cfg = Config{};
    var args = std.process.Args.Iterator.init(args_in);
    _ = args.skip();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--workload")) {
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
        } else if (std.mem.eql(u8, arg, "--sync-level")) {
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.sync_level = db_mod.types.parsePublicSyncLevelText(raw) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--mutation-mode")) {
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.mutation_mode = parseMutationMode(raw) orelse return error.InvalidArgument;
        } else {
            std.debug.print("invalid argument: {s}\n", .{arg});
            return error.InvalidArgument;
        }
    }
    if (cfg.docs == 0 or cfg.overwrite_passes == 0 or cfg.body_repeat == 0 or cfg.batch_size == 0 or cfg.dims == 0) {
        return error.InvalidArgument;
    }
    if (cfg.mutation_mode == .transform and cfg.workload == .explicit_dense) {
        std.debug.print("--mutation-mode transform is not supported for explicit_dense\n", .{});
        return error.InvalidArgument;
    }
    return cfg;
}

fn parseWorkload(raw: []const u8) ?Workload {
    if (std.mem.eql(u8, raw, "documents")) return .documents;
    if (std.mem.eql(u8, raw, "explicit_full_text")) return .explicit_full_text;
    if (std.mem.eql(u8, raw, "explicit_dense")) return .explicit_dense;
    return null;
}

fn parsePrimary(raw: []const u8) ?PrimaryKind {
    if (std.mem.eql(u8, raw, "lsm")) return .lsm;
    if (std.mem.eql(u8, raw, "lsm_memory")) return .lsm_memory;
    if (std.mem.eql(u8, raw, "mem")) return .mem;
    if (std.mem.eql(u8, raw, "lmdb")) return .lmdb;
    return null;
}

fn parseMutationMode(raw: []const u8) ?MutationMode {
    if (std.mem.eql(u8, raw, "overwrite")) return .overwrite;
    if (std.mem.eql(u8, raw, "transform")) return .transform;
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

fn runBench(alloc: std.mem.Allocator, path: []const u8, cfg: Config) !Summary {
    var resource_manager = resource_manager_mod.ResourceManager.init(.{});
    var opts = openOptions(cfg);
    opts.resource_manager = &resource_manager;

    var db = try db_mod.DB.open(alloc, path, opts);
    defer db.close();

    try configureIndexes(alloc, &db, cfg);

    var total_profile = db_mod.BatchProfile{};
    var max_batch_ns: u64 = 0;
    var batches: usize = 0;
    const requested_writes = switch (cfg.mutation_mode) {
        .overwrite => cfg.docs * cfg.overwrite_passes,
        .transform => cfg.docs,
    };
    const requested_transforms = switch (cfg.mutation_mode) {
        .overwrite => 0,
        .transform => cfg.docs * (cfg.overwrite_passes - 1),
    };

    var empty_vector: [0]f32 = .{};
    const vector_buf = if (cfg.workload == .explicit_dense) try alloc.alloc(f32, cfg.dims) else empty_vector[0..];
    defer if (cfg.workload == .explicit_dense) alloc.free(vector_buf);

    if (cfg.bulk_session) try db.beginBulkIngestSession();
    errdefer if (cfg.bulk_session) db.abortBulkIngestSession();

    const stage_start_ns = nowNs();
    var start_doc: usize = 0;
    for (0..cfg.overwrite_passes) |pass_idx| {
        start_doc = 0;
        while (start_doc < cfg.docs) : (start_doc += cfg.batch_size) {
            const end_doc = @min(start_doc + cfg.batch_size, cfg.docs);
            var profile = db_mod.BatchProfile{};
            switch (cfg.mutation_mode) {
                .overwrite => {
                    const writes = try buildWrites(alloc, cfg, pass_idx, start_doc, end_doc, vector_buf);
                    defer freeWrites(alloc, writes);
                    try db.batchProfiled(.{
                        .writes = writes,
                        .sync_level = cfg.sync_level,
                    }, &profile);
                },
                .transform => {
                    if (pass_idx == 0) {
                        const writes = try buildWrites(alloc, cfg, pass_idx, start_doc, end_doc, vector_buf);
                        defer freeWrites(alloc, writes);
                        try db.batchProfiled(.{
                            .writes = writes,
                            .sync_level = cfg.sync_level,
                        }, &profile);
                    } else {
                        const transforms = try buildTransforms(alloc, cfg, pass_idx, start_doc, end_doc);
                        defer freeTransforms(alloc, transforms);
                        try db.batchProfiled(.{
                            .transforms = transforms,
                            .sync_level = cfg.sync_level,
                        }, &profile);
                    }
                },
            }
            addBatchProfile(&total_profile, profile);
            max_batch_ns = @max(max_batch_ns, profile.total_ns);
            batches += 1;
        }
    }
    const stage_ns = elapsedSince(stage_start_ns);

    var finish_ns: u64 = 0;
    if (cfg.bulk_session) {
        const finish_start_ns = nowNs();
        try db.finishBulkIngestSessionWithOptions(.{ .compact = false });
        finish_ns = elapsedSince(finish_start_ns);
    }

    return .{
        .requested_writes = requested_writes,
        .requested_transforms = requested_transforms,
        .batches = batches,
        .stage_ns = stage_ns,
        .finish_ns = finish_ns,
        .total_ns = stage_ns + finish_ns,
        .max_batch_ns = max_batch_ns,
        .profile = total_profile,
        .replay = try snapshotReplayStats(alloc, &db),
        .async_indexing = db.snapshotAsyncIndexingStats(),
    };
}

fn openOptions(cfg: Config) db_mod.OpenOptions {
    var opts: db_mod.OpenOptions = .{
        .start_index_workers = false,
    };
    switch (cfg.primary) {
        .lsm => {},
        .lsm_memory => opts.primary_backend = .{ .lsm_memory = .{} },
        .mem => opts.primary_backend = .{ .mem = .{} },
        .lmdb => opts.primary_backend = .lmdb,
    }
    return opts;
}

fn configureIndexes(alloc: std.mem.Allocator, db: *db_mod.DB, cfg: Config) !void {
    switch (cfg.workload) {
        .documents => return,
        .explicit_full_text => try db.addIndex(.{
            .name = "ft_idx",
            .kind = .full_text,
            .config_json = "{\"field\":\"body\"}",
        }),
        .explicit_dense => {
            const cfg_json = try std.fmt.allocPrint(
                alloc,
                "{{\"field\":\"embedding\",\"dims\":{d},\"metric\":\"l2_squared\"}}",
                .{cfg.dims},
            );
            defer alloc.free(cfg_json);
            try db.addIndex(.{
                .name = "dense_idx",
                .kind = .dense_vector,
                .config_json = cfg_json,
            });
        },
    }
}

fn buildWrites(
    alloc: std.mem.Allocator,
    cfg: Config,
    pass_idx: usize,
    start_doc: usize,
    end_doc: usize,
    vector_buf: []f32,
) ![]db_mod.types.BatchWrite {
    const writes = try alloc.alloc(db_mod.types.BatchWrite, end_doc - start_doc);
    errdefer {
        for (writes) |write| {
            alloc.free(write.key);
            alloc.free(write.value);
        }
        alloc.free(writes);
    }
    for (writes, start_doc..) |*write, doc_idx| {
        write.* = try makeBatchWrite(alloc, cfg, doc_idx, pass_idx, vector_buf);
    }
    return writes;
}

fn buildTransforms(
    alloc: std.mem.Allocator,
    cfg: Config,
    pass_idx: usize,
    start_doc: usize,
    end_doc: usize,
) ![]db_mod.types.DocumentTransform {
    const transforms = try alloc.alloc(db_mod.types.DocumentTransform, end_doc - start_doc);
    errdefer {
        for (transforms) |transform| freeTransform(alloc, transform);
        alloc.free(transforms);
    }
    for (transforms, start_doc..) |*transform, doc_idx| {
        transform.* = try makeDocumentTransform(alloc, cfg, doc_idx, pass_idx);
    }
    return transforms;
}

fn freeWrites(alloc: std.mem.Allocator, writes: []db_mod.types.BatchWrite) void {
    for (writes) |write| {
        alloc.free(write.key);
        alloc.free(write.value);
    }
    alloc.free(writes);
}

fn freeTransforms(alloc: std.mem.Allocator, transforms: []db_mod.types.DocumentTransform) void {
    for (transforms) |transform| freeTransform(alloc, transform);
    alloc.free(transforms);
}

fn freeTransform(alloc: std.mem.Allocator, transform: db_mod.types.DocumentTransform) void {
    alloc.free(transform.key);
    for (transform.operations) |op| {
        if (op.value_json) |value_json| alloc.free(value_json);
    }
    alloc.free(transform.operations);
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
        .documents, .explicit_full_text => try encodeDocumentJsonAlloc(alloc, doc_idx, pass_idx, cfg),
        .explicit_dense => try encodeVectorDocJsonAlloc(alloc, doc_idx, cfg, vector_buf),
    };
    return .{
        .key = key,
        .value = value,
    };
}

fn makeDocumentTransform(
    alloc: std.mem.Allocator,
    cfg: Config,
    doc_idx: usize,
    pass_idx: usize,
) !db_mod.types.DocumentTransform {
    const key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{doc_idx});
    errdefer alloc.free(key);

    const title_value = try std.fmt.allocPrint(alloc, "\"doc-{d}-pass-{d}\"", .{ doc_idx, pass_idx });
    errdefer alloc.free(title_value);

    const body = try generatedBodyTextAlloc(alloc, doc_idx, pass_idx, cfg);
    defer alloc.free(body);
    const body_value = try std.fmt.allocPrint(alloc, "\"{s}\"", .{body});
    errdefer alloc.free(body_value);

    const operations = try alloc.alloc(db_mod.types.TransformOp, 2);
    operations[0] = .{
        .op = .set,
        .path = "title",
        .value_json = title_value,
    };
    operations[1] = .{
        .op = .set,
        .path = "body",
        .value_json = body_value,
    };

    return .{
        .key = key,
        .operations = operations,
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

fn printSummary(cfg: Config, summary: Summary) void {
    std.debug.print(
        "batch_bench workload={s} mutation_mode={s} primary={s} docs={d} overwrite_passes={d} body_repeat={d} dims={d} batch_size={d} bulk_session={any} sync={s} requested_writes={d} requested_transforms={d} batches={d} stage_ms={d:.3} finish_ms={d:.3} total_ms={d:.3} max_batch_ms={d:.3}\n",
        .{
            @tagName(cfg.workload),
            @tagName(cfg.mutation_mode),
            @tagName(cfg.primary),
            cfg.docs,
            cfg.overwrite_passes,
            cfg.body_repeat,
            cfg.dims,
            cfg.batch_size,
            cfg.bulk_session,
            db_mod.types.publicSyncLevelText(cfg.sync_level),
            summary.requested_writes,
            summary.requested_transforms,
            summary.batches,
            nsToMsFloat(summary.stage_ns),
            nsToMsFloat(summary.finish_ns),
            nsToMsFloat(summary.total_ns),
            nsToMsFloat(summary.max_batch_ns),
        },
    );
    std.debug.print(
        "batch_bench_profile resolve_ms={d:.3} merge_req_ms={d:.3} store_write_ms={d:.3} build_derived_ms={d:.3} append_replay_journal_ms={d:.3} full_text_apply_ms={d:.3} dense_apply_ms={d:.3}\n",
        .{
            nsToMsFloat(summary.profile.resolve_transforms_ns),
            nsToMsFloat(summary.profile.merge_effective_req_ns),
            nsToMsFloat(summary.profile.store_write_ns),
            nsToMsFloat(summary.profile.build_derived_ns),
            nsToMsFloat(summary.profile.append_replay_journal_ns),
            nsToMsFloat(summary.profile.full_text_apply_ns),
            nsToMsFloat(summary.profile.dense_apply_ns),
        },
    );
    std.debug.print(
        "batch_bench_replay seq={d} entries={d} payload_bytes={d}\n",
        .{ summary.replay.last_sequence, summary.replay.entries, summary.replay.payload_bytes },
    );
    std.debug.print(
        "batch_bench_bulk_coalescing active={any} staged_keys={d} stage_batches={d} stage_writes={d} stage_deletes={d} stage_transforms={d} flush_calls={d} flushed_keys={d}\n",
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
}

fn nowNs() u64 {
    return platform_time.monotonicNs();
}

fn elapsedSince(start_ns: u64) u64 {
    return nowNs() - start_ns;
}

fn nsToMsFloat(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / @as(f64, @floatFromInt(std.time.ns_per_ms));
}

fn tempPath(buf: []u8) []u8 {
    return std.fmt.bufPrint(buf, "/tmp/antfly-batch-bench-{d}", .{platform_time.monotonicNs()}) catch unreachable;
}

fn cleanupTempDir(path: []const u8) void {
    _ = path;
}
