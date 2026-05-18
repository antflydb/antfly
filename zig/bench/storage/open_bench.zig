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
const platform_time = antfly.platform_time;
const resource_manager_mod = antfly.resource_manager;
const replay_stream_mod = db_mod.replay_stream;

const PrimaryKind = enum {
    lsm,
    lsm_memory,
    mem,
    lmdb,
};

const Config = struct {
    primary: PrimaryKind = .lsm,
    open_mode: db_mod.OpenMode = .writer,
    docs: usize = 10_000,
    batch_size: usize = 500,
    dims: usize = 384,
    indexes_text: usize = 1,
    indexes_dense: usize = 1,
    indexes_sparse: usize = 0,
    indexes_graph: usize = 0,
    index_open_parallelism: ?usize = null,
    stage_backlog: bool = false,
};

const ReplayStats = struct {
    last_sequence: u64 = 0,
    entries: usize = 0,
    payload_bytes: usize = 0,
};

const Summary = struct {
    seed_sync_level: db_mod.types.SyncLevel = .full_index,
    open_ns: u64 = 0,
    replay: ReplayStats = .{},
    pending_target_sequence: u64 = 0,
    enrichment_target_sequence: u64 = 0,
    enrichment_applied_sequence: u64 = 0,
    has_async_indexes: bool = false,
    text_merge_pending_segments: u64 = 0,
    text_merge_pending_bytes: u64 = 0,
    total_text_segments: usize = 0,
};

pub fn main(init: std.process.Init) !void {
    const alloc = std.heap.c_allocator;
    const cfg = try parseArgs(init.minimal.args);

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    try seedDb(alloc, std.mem.span(path), cfg);
    const summary = try measureOpen(alloc, std.mem.span(path), cfg);
    printSummary(cfg, summary);
}

fn parseArgs(args_in: std.process.Args) !Config {
    var cfg = Config{};
    var args = std.process.Args.Iterator.init(args_in);
    _ = args.skip();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--primary")) {
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.primary = parsePrimary(raw) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--open-mode")) {
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.open_mode = parseOpenMode(raw) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--docs")) {
            cfg.docs = try parseNextUsize(&args, "--docs");
        } else if (std.mem.eql(u8, arg, "--batch-size")) {
            cfg.batch_size = try parseNextUsize(&args, "--batch-size");
        } else if (std.mem.eql(u8, arg, "--dims")) {
            cfg.dims = try parseNextUsize(&args, "--dims");
        } else if (std.mem.eql(u8, arg, "--indexes-text")) {
            cfg.indexes_text = try parseNextUsize(&args, "--indexes-text");
        } else if (std.mem.eql(u8, arg, "--indexes-dense")) {
            cfg.indexes_dense = try parseNextUsize(&args, "--indexes-dense");
        } else if (std.mem.eql(u8, arg, "--indexes-sparse")) {
            cfg.indexes_sparse = try parseNextUsize(&args, "--indexes-sparse");
        } else if (std.mem.eql(u8, arg, "--indexes-graph")) {
            cfg.indexes_graph = try parseNextUsize(&args, "--indexes-graph");
        } else if (std.mem.eql(u8, arg, "--index-open-parallelism")) {
            cfg.index_open_parallelism = try parseNextUsize(&args, "--index-open-parallelism");
        } else if (std.mem.eql(u8, arg, "--stage-backlog")) {
            cfg.stage_backlog = true;
        } else {
            std.debug.print("invalid argument: {s}\n", .{arg});
            return error.InvalidArgument;
        }
    }

    if (cfg.batch_size == 0 or cfg.dims == 0) return error.InvalidArgument;
    return cfg;
}

fn parsePrimary(raw: []const u8) ?PrimaryKind {
    if (std.mem.eql(u8, raw, "lsm")) return .lsm;
    if (std.mem.eql(u8, raw, "lsm_memory")) return .lsm_memory;
    if (std.mem.eql(u8, raw, "mem")) return .mem;
    if (std.mem.eql(u8, raw, "lmdb")) return .lmdb;
    return null;
}

fn parseOpenMode(raw: []const u8) ?db_mod.OpenMode {
    if (std.mem.eql(u8, raw, "writer")) return .writer;
    if (std.mem.eql(u8, raw, "query_readonly")) return .query_readonly;
    if (std.mem.eql(u8, raw, "status_only")) return .status_only;
    return null;
}

fn parseNextUsize(args: *std.process.Args.Iterator, flag: []const u8) !usize {
    const raw = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return try std.fmt.parseInt(usize, raw, 10);
}

fn seedDb(alloc: std.mem.Allocator, path: []const u8, cfg: Config) !void {
    var deterministic_dense = db_mod.embedder.DeterministicDenseEmbedder{};
    var deterministic_sparse = db_mod.embedder.DeterministicSparseEmbedder{};
    var resource_manager = resource_manager_mod.ResourceManager.init(.{});
    var opts = openOptions(cfg, false);
    opts.resource_manager = &resource_manager;
    if (needsEnrichment(cfg)) {
        opts.enrichment = .{
            .owner_id = "open-bench-seed",
            .dense_embedder = deterministic_dense.interface(),
            .sparse_embedder = deterministic_sparse.interface(),
        };
    }

    var db = try db_mod.DB.open(alloc, path, opts);
    defer db.close();

    try configureIndexes(alloc, &db, cfg);

    const sync_level: db_mod.types.SyncLevel = if (cfg.stage_backlog) .write else .full_index;
    if (cfg.docs > 0) {
        try seedDocuments(alloc, &db, cfg, sync_level);
    }
    if (cfg.indexes_graph > 0 and cfg.docs > 1) {
        try seedGraphEdges(alloc, &db, cfg, sync_level);
    }
    if (!cfg.stage_backlog) try db.runUntilIdle();
}

fn measureOpen(alloc: std.mem.Allocator, path: []const u8, cfg: Config) !Summary {
    var deterministic_dense = db_mod.embedder.DeterministicDenseEmbedder{};
    var deterministic_sparse = db_mod.embedder.DeterministicSparseEmbedder{};
    var resource_manager = resource_manager_mod.ResourceManager.init(.{});
    var opts = openOptions(cfg, true);
    opts.resource_manager = &resource_manager;
    if (needsEnrichment(cfg)) {
        opts.enrichment = .{
            .owner_id = "open-bench-open",
            .dense_embedder = deterministic_dense.interface(),
            .sparse_embedder = deterministic_sparse.interface(),
        };
    }

    const started_ns = nowNs();
    var db = try db_mod.DB.open(alloc, path, opts);
    defer db.close();

    const pending = db.pendingWorkStats();
    return .{
        .seed_sync_level = if (cfg.stage_backlog) .write else .full_index,
        .open_ns = elapsedSince(started_ns),
        .replay = try snapshotReplayStats(alloc, &db),
        .pending_target_sequence = pending.derived_target_sequence,
        .enrichment_target_sequence = pending.enrichment.target_sequence,
        .enrichment_applied_sequence = pending.enrichment.applied_sequence,
        .has_async_indexes = pending.has_async_indexes,
        .text_merge_pending_segments = pending.text_merge.pending_segments,
        .text_merge_pending_bytes = pending.text_merge.pending_bytes,
        .total_text_segments = totalTextSegments(&db),
    };
}

fn openOptions(cfg: Config, start_index_workers: bool) db_mod.OpenOptions {
    var opts: db_mod.OpenOptions = .{
        .open_mode = cfg.open_mode,
        .index_open_parallelism = cfg.index_open_parallelism,
        .start_index_workers = cfg.open_mode == .writer and start_index_workers,
    };
    switch (cfg.primary) {
        .lsm => {},
        .lsm_memory => opts.primary_backend = .{ .lsm_memory = .{} },
        .mem => opts.primary_backend = .{ .mem = .{} },
        .lmdb => opts.primary_backend = .lmdb,
    }
    return opts;
}

fn needsEnrichment(cfg: Config) bool {
    return cfg.indexes_dense > 0 or cfg.indexes_sparse > 0;
}

fn configureIndexes(alloc: std.mem.Allocator, db: *db_mod.DB, cfg: Config) !void {
    for (0..cfg.indexes_text) |i| {
        const name = try std.fmt.allocPrint(alloc, "ft_{d:0>2}", .{i});
        defer alloc.free(name);
        try db.addIndex(.{
            .name = name,
            .kind = .full_text,
            .config_json = "{\"field\":\"body\"}",
        });
    }
    for (0..cfg.indexes_dense) |i| {
        const name = try std.fmt.allocPrint(alloc, "dv_{d:0>2}", .{i});
        defer alloc.free(name);
        const config_json = try std.fmt.allocPrint(
            alloc,
            "{{\"field\":\"embedding\",\"dims\":{d},\"metric\":\"l2_squared\",\"generator\":{{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"{s}\"}}}}",
            .{ cfg.dims, name },
        );
        defer alloc.free(config_json);
        try db.addIndex(.{
            .name = name,
            .kind = .dense_vector,
            .config_json = config_json,
        });
    }
    for (0..cfg.indexes_sparse) |i| {
        const name = try std.fmt.allocPrint(alloc, "sp_{d:0>2}", .{i});
        defer alloc.free(name);
        try db.addIndex(.{
            .name = name,
            .kind = .sparse_vector,
            .config_json = "{\"field\":\"sparse_embedding\",\"generator\":{\"kind\":\"sparse_embedding\",\"source_field\":\"body\"}}",
        });
    }
    for (0..cfg.indexes_graph) |i| {
        const name = try std.fmt.allocPrint(alloc, "gr_{d:0>2}", .{i});
        defer alloc.free(name);
        try db.addIndex(.{
            .name = name,
            .kind = .graph,
            .config_json = "{}",
        });
    }
}

fn seedDocuments(alloc: std.mem.Allocator, db: *db_mod.DB, cfg: Config, sync_level: db_mod.types.SyncLevel) !void {
    const writes_buf = try alloc.alloc(db_mod.types.BatchWrite, cfg.batch_size);
    defer alloc.free(writes_buf);

    var start: usize = 0;
    while (start < cfg.docs) : (start += cfg.batch_size) {
        const end = @min(start + cfg.batch_size, cfg.docs);
        const writes = writes_buf[0 .. end - start];
        for (start..end, 0..) |doc_idx, i| {
            writes[i] = .{
                .key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{doc_idx}),
                .value = try encodeDocumentJsonAlloc(alloc, doc_idx, cfg),
            };
        }
        defer {
            for (writes) |write| {
                alloc.free(write.key);
                alloc.free(write.value);
            }
        }
        try db.batch(.{
            .writes = writes,
            .sync_level = sync_level,
        });
    }
}

fn seedGraphEdges(alloc: std.mem.Allocator, db: *db_mod.DB, cfg: Config, sync_level: db_mod.types.SyncLevel) !void {
    const graph_writes = try alloc.alloc(db_mod.types.GraphEdgeWrite, cfg.indexes_graph);
    defer alloc.free(graph_writes);

    for (0..cfg.indexes_graph) |i| {
        const name = try std.fmt.allocPrint(alloc, "gr_{d:0>2}", .{i});
        defer alloc.free(name);
        graph_writes[i] = .{
            .index_name = name,
            .source = "doc:00000000",
            .target = "doc:00000001",
            .edge_type = "links",
            .weight = 1.0,
        };
    }

    try db.batch(.{
        .graph_writes = graph_writes,
        .sync_level = sync_level,
    });
}

fn encodeDocumentJsonAlloc(alloc: std.mem.Allocator, doc_idx: usize, cfg: Config) ![]u8 {
    const body = try generatedBodyTextAlloc(alloc, doc_idx, cfg);
    defer alloc.free(body);
    return try std.fmt.allocPrint(
        alloc,
        "{{\"title\":\"doc-{d}\",\"body\":\"{s}\"}}",
        .{ doc_idx, body },
    );
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
        "document {d} topic {s} dims {d} repeated context repeated context repeated context tail {d}",
        .{ doc_idx, topic, cfg.dims, doc_idx % 97 },
    );
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

fn totalTextSegments(db: *db_mod.DB) usize {
    var total: usize = 0;
    for (db.core.index_manager.text_indexes.items) |*entry| {
        total += entry.persistent.snapshot().segments.len;
    }
    return total;
}

fn printSummary(cfg: Config, summary: Summary) void {
    std.debug.print(
        "open_bench primary={s} open_mode={s} docs={d} batch_size={d} dims={d} indexes_text={d} indexes_dense={d} indexes_sparse={d} indexes_graph={d} index_open_parallelism={any} seed_sync={s} stage_backlog={} open_ms={d:.3} replay_seq={d} replay_entries={d} replay_payload_bytes={d} pending_target={d} enrichment_target={d} enrichment_applied={d} has_async_indexes={} text_merge_pending_segments={d} text_merge_pending_bytes={d} total_text_segments={d}\n",
        .{
            @tagName(cfg.primary),
            @tagName(cfg.open_mode),
            cfg.docs,
            cfg.batch_size,
            cfg.dims,
            cfg.indexes_text,
            cfg.indexes_dense,
            cfg.indexes_sparse,
            cfg.indexes_graph,
            cfg.index_open_parallelism,
            db_mod.types.publicSyncLevelText(summary.seed_sync_level),
            cfg.stage_backlog,
            nsToMsFloat(summary.open_ns),
            summary.replay.last_sequence,
            summary.replay.entries,
            summary.replay.payload_bytes,
            summary.pending_target_sequence,
            summary.enrichment_target_sequence,
            summary.enrichment_applied_sequence,
            summary.has_async_indexes,
            summary.text_merge_pending_segments,
            summary.text_merge_pending_bytes,
            summary.total_text_segments,
        },
    );
    std.debug.print(
        "open_bench_csv primary,open_mode,docs,batch_size,dims,indexes_text,indexes_dense,indexes_sparse,indexes_graph,index_open_parallelism,seed_sync,stage_backlog,open_ms,replay_seq,replay_entries,replay_payload_bytes,pending_target,enrichment_target,enrichment_applied,has_async_indexes,text_merge_pending_segments,text_merge_pending_bytes,total_text_segments\n",
        .{},
    );
    std.debug.print(
        "open_bench_csv {s},{s},{d},{d},{d},{d},{d},{d},{d},{any},{s},{any},{d:.3},{d},{d},{d},{d},{d},{d},{any},{d},{d},{d}\n",
        .{
            @tagName(cfg.primary),
            @tagName(cfg.open_mode),
            cfg.docs,
            cfg.batch_size,
            cfg.dims,
            cfg.indexes_text,
            cfg.indexes_dense,
            cfg.indexes_sparse,
            cfg.indexes_graph,
            cfg.index_open_parallelism,
            db_mod.types.publicSyncLevelText(summary.seed_sync_level),
            cfg.stage_backlog,
            nsToMsFloat(summary.open_ns),
            summary.replay.last_sequence,
            summary.replay.entries,
            summary.replay.payload_bytes,
            summary.pending_target_sequence,
            summary.enrichment_target_sequence,
            summary.enrichment_applied_sequence,
            summary.has_async_indexes,
            summary.text_merge_pending_segments,
            summary.text_merge_pending_bytes,
            summary.total_text_segments,
        },
    );
}

fn nsToMsFloat(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / @as(f64, std.time.ns_per_ms);
}

var temp_path_nonce: u64 = 0;

fn tempPath(buf: []u8) [*:0]const u8 {
    const ts = nowNs();
    const nonce = @atomicRmw(u64, &temp_path_nonce, .Add, 1, .monotonic);
    const path_bytes = std.fmt.bufPrint(buf, "/tmp/antfly-open-bench-{d}-{d}\x00", .{ ts, nonce }) catch unreachable;
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
