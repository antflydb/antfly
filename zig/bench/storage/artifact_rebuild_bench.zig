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
const backfill_state_mod = db_mod.backfill_state;
const platform_time = antfly.platform_time;
const resource_manager_mod = antfly.resource_manager;

const Config = struct {
    db_path: []const u8 = "",
    fixture_db_path: ?[]const u8 = null,
    poll_ms: u64 = 1_000,
    timeout_s: u64 = 600,
    stop_after_applied: ?u64 = null,
    force_dense_rebuild_from_start: bool = false,
    enable_text_merge: bool = false,
};

const Summary = struct {
    open_ns: u64 = 0,
    first_apply_ns: ?u64 = null,
    total_ns: u64 = 0,
    final_phase: db_mod.types.StartupCatchUpPhase = .idle,
    opened_indexes: u32 = 0,
    wal_replay_bytes: u64 = 0,
    wal_replay_ns: u64 = 0,
    current_sequence: u64 = 0,
    current_target_sequence: u64 = 0,
    current_applied_entries: u64 = 0,
    progress_updates: u64 = 0,
    derived_target_sequence: u64 = 0,
    lsm_wal_retained_bytes: u64 = 0,
    lsm_wal_retained_segments: u64 = 0,
};

pub fn main(init: std.process.Init) !void {
    const alloc = std.heap.c_allocator;
    const cfg = try parseArgs(alloc, init.minimal.args);
    defer {
        if (cfg.db_path.len > 0) alloc.free(cfg.db_path);
        if (cfg.fixture_db_path) |path| alloc.free(path);
    }

    const open_path = if (cfg.fixture_db_path) |fixture_path| blk: {
        try cloneDirAbsolute(alloc, cfg.db_path, fixture_path);
        break :blk fixture_path;
    } else cfg.db_path;

    if (cfg.force_dense_rebuild_from_start) {
        std.debug.print("artifact_rebuild_bench prepare open_path={s} mode=prepare\n", .{open_path});
        var prep_resource_manager = resource_manager_mod.ResourceManager.init(.{});
        var prep_db = openDb(alloc, open_path, &prep_resource_manager, cfg, false) catch |err| {
            std.debug.print("artifact_rebuild_bench prepare_open failed path={s} err={}\n", .{ open_path, err });
            return err;
        };
        defer prep_db.close();
        forceDenseArtifactRebuildFromStart(&prep_db) catch |err| {
            std.debug.print("artifact_rebuild_bench force_rebuild failed path={s} err={}\n", .{ open_path, err });
            return err;
        };
    }

    var resource_manager = resource_manager_mod.ResourceManager.init(.{});
    std.debug.print("artifact_rebuild_bench open open_path={s} mode=run\n", .{open_path});
    var db = openDb(alloc, open_path, &resource_manager, cfg, !cfg.force_dense_rebuild_from_start) catch |err| {
        std.debug.print("artifact_rebuild_bench run_open failed path={s} err={}\n", .{ open_path, err });
        return err;
    };
    defer db.close();

    const summary = if (cfg.force_dense_rebuild_from_start)
        try runExplicitDenseRebuildBench(alloc, &db, cfg)
    else
        try runBench(&db, cfg);
    printSummary(open_path, summary);
    std.process.exit(0);
}

fn parseArgs(alloc: std.mem.Allocator, args_in: std.process.Args) !Config {
    var cfg = Config{};
    var args = std.process.Args.Iterator.init(args_in);
    _ = args.skip();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--db-path")) {
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.db_path = try alloc.dupe(u8, raw);
        } else if (std.mem.eql(u8, arg, "--fixture-db-path")) {
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.fixture_db_path = try alloc.dupe(u8, raw);
        } else if (std.mem.eql(u8, arg, "--poll-ms")) {
            cfg.poll_ms = try parseNextU64(&args, "--poll-ms");
        } else if (std.mem.eql(u8, arg, "--timeout-s")) {
            cfg.timeout_s = try parseNextU64(&args, "--timeout-s");
        } else if (std.mem.eql(u8, arg, "--stop-after-applied")) {
            cfg.stop_after_applied = try parseNextU64(&args, "--stop-after-applied");
        } else if (std.mem.eql(u8, arg, "--force-dense-rebuild-from-start")) {
            cfg.force_dense_rebuild_from_start = true;
        } else if (std.mem.eql(u8, arg, "--enable-text-merge")) {
            cfg.enable_text_merge = true;
        } else {
            std.debug.print("invalid argument: {s}\n", .{arg});
            return error.InvalidArgument;
        }
    }
    if (cfg.db_path.len == 0 or cfg.poll_ms == 0 or cfg.timeout_s == 0) return error.InvalidArgument;
    return cfg;
}

fn parseNextU64(args: *std.process.Args.Iterator, flag: []const u8) !u64 {
    const raw = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return try std.fmt.parseInt(u64, raw, 10);
}

fn openDb(
    alloc: std.mem.Allocator,
    path: []const u8,
    resource_manager: *resource_manager_mod.ResourceManager,
    cfg: Config,
    start_index_workers: bool,
) !db_mod.DB {
    return try db_mod.DB.open(alloc, path, .{
        .open_mode = .writer,
        .start_index_workers = start_index_workers,
        .resource_manager = resource_manager,
        .text_merge = .{ .enabled = cfg.enable_text_merge },
    });
}

fn forceDenseArtifactRebuildFromStart(db: *db_mod.DB) !void {
    for (db.core.index_manager.dense_indexes.items) |entry| {
        const artifact_backed = entry.external or entry.chunk_name != null or entry.embedding_name != null;
        if (!artifact_backed) continue;
        const rebuild_root_path = try std.fmt.allocPrint(std.heap.page_allocator, "{s}/indexes/{s}", .{
            db.core.path,
            entry.config.name,
        });
        defer std.heap.page_allocator.free(rebuild_root_path);
        const rebuild_state = backfill_state_mod.RebuildState.init(rebuild_root_path);
        try rebuild_state.update("");
        std.debug.print("forced_dense_rebuild index={s} rebuild_root={s}\n", .{ entry.config.name, rebuild_root_path });
    }
}

fn runBench(db: *db_mod.DB, cfg: Config) !Summary {
    const open_ns = db.snapshotAsyncIndexingStats().startup.db_open_ns;
    const bench_started_ns = nowNs();
    const deadline_ns = bench_started_ns + cfg.timeout_s * std.time.ns_per_s;
    var summary = Summary{ .open_ns = open_ns };
    var last_applied_entries: u64 = 0;
    var last_progress_ns = bench_started_ns;

    while (true) {
        const async_stats = db.snapshotAsyncIndexingStats();
        const pending = db.pendingWorkStats();
        const maintenance = db.snapshotLsmMaintenanceStats();
        summary.final_phase = async_stats.startup.phase;
        summary.opened_indexes = async_stats.startup.opened_indexes;
        summary.wal_replay_bytes = async_stats.startup.wal_replay_bytes;
        summary.wal_replay_ns = async_stats.startup.wal_replay_ns;
        summary.current_sequence = async_stats.dense_catch_up.current_sequence;
        summary.current_target_sequence = async_stats.dense_catch_up.current_target_sequence;
        summary.current_applied_entries = async_stats.dense_catch_up.current_applied_entries;
        summary.progress_updates = async_stats.dense_catch_up.progress_updates;
        summary.derived_target_sequence = pending.derived_target_sequence;
        summary.lsm_wal_retained_bytes = maintenance.wal_retained_bytes;
        summary.lsm_wal_retained_segments = maintenance.wal_retained_segments;

        if (summary.first_apply_ns == null and summary.current_applied_entries > 0) {
            summary.first_apply_ns = elapsedSince(bench_started_ns);
        }
        if (summary.current_applied_entries != last_applied_entries) {
            const now = nowNs();
            const elapsed_ns = now - last_progress_ns;
            const delta = summary.current_applied_entries - last_applied_entries;
            const rate = if (elapsed_ns == 0) 0.0 else (@as(f64, @floatFromInt(delta)) * @as(f64, std.time.ns_per_s)) / @as(f64, @floatFromInt(elapsed_ns));
            std.debug.print(
                "progress phase={s} applied={d}/{d} delta={d} rate={d:.1}/s startup_active={} dense_active={} retained_wal_bytes={d}\n",
                .{
                    @tagName(summary.final_phase),
                    summary.current_applied_entries,
                    summary.current_target_sequence,
                    delta,
                    rate,
                    async_stats.startup.active,
                    async_stats.dense_catch_up.active,
                    summary.lsm_wal_retained_bytes,
                },
            );
            last_applied_entries = summary.current_applied_entries;
            last_progress_ns = now;
        }

        if (cfg.stop_after_applied) |target| {
            if (summary.current_applied_entries >= target) break;
        }

        const startup_idle = !async_stats.startup.active or async_stats.startup.phase == .idle;
        const dense_idle = !async_stats.dense_catch_up.active;
        if (startup_idle and dense_idle and pending.derived_target_sequence == 0) break;
        if (nowNs() >= deadline_ns) break;
        sleepNs(cfg.poll_ms * std.time.ns_per_ms);
    }

    summary.total_ns = elapsedSince(bench_started_ns);
    return summary;
}

fn runExplicitDenseRebuildBench(alloc: std.mem.Allocator, db: *db_mod.DB, cfg: Config) !Summary {
    const bench_started_ns = nowNs();
    var summary = Summary{
        .open_ns = db.snapshotAsyncIndexingStats().startup.db_open_ns,
        .opened_indexes = @intCast(db.core.index_manager.dense_indexes.items.len + db.core.index_manager.text_indexes.items.len + db.core.index_manager.sparse_indexes.items.len + db.core.index_manager.graph_indexes.items.len),
        .final_phase = .artifact_rebuild,
    };

    const ProgressCtx = struct {
        bench_started_ns: u64,
        summary: *Summary,
        stop_after_applied: ?u64,

        fn onProgress(ctx: *anyopaque, _: []const u8, progress: db_mod.ReplayProgress) !void {
            const state: *@This() = @ptrCast(@alignCast(ctx));
            state.summary.current_sequence = progress.sequence;
            state.summary.current_target_sequence = progress.target_sequence;
            state.summary.current_applied_entries = progress.applied_entries;
            state.summary.progress_updates += 1;
            if (state.summary.first_apply_ns == null and progress.applied_entries > 0) {
                state.summary.first_apply_ns = nowNs() - state.bench_started_ns;
            }
            if (state.stop_after_applied) |target| {
                if (progress.applied_entries >= target) return error.BenchStop;
            }
        }
    };

    var progress_ctx = ProgressCtx{
        .bench_started_ns = bench_started_ns,
        .summary = &summary,
        .stop_after_applied = cfg.stop_after_applied,
    };

    _ = db.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeededWithProgress(
        alloc,
        &progress_ctx,
        ProgressCtx.onProgress,
    ) catch |err| switch (err) {
        error.BenchStop => {},
        else => return err,
    };

    const pending = db.pendingWorkStats();
    const maintenance = db.snapshotLsmMaintenanceStats();
    summary.derived_target_sequence = pending.derived_target_sequence;
    summary.lsm_wal_retained_bytes = maintenance.wal_retained_bytes;
    summary.lsm_wal_retained_segments = maintenance.wal_retained_segments;
    summary.total_ns = elapsedSince(bench_started_ns);
    return summary;
}

fn printSummary(db_path: []const u8, summary: Summary) void {
    if (summary.first_apply_ns) |first_apply_ns| {
        std.debug.print(
            "artifact_rebuild_bench db_path={s} open_ms={d:.2} first_apply_ms={d:.2} total_s={d:.2} phase={s} opened_indexes={d} wal_replay_mb={d:.2} wal_replay_ms={d:.2} applied={d}/{d} progress_updates={d} derived_target_sequence={d} retained_wal_mb={d:.2} retained_wal_segments={d}\n",
            .{
                db_path,
                nsToMs(summary.open_ns),
                nsToMs(first_apply_ns),
                nsToS(summary.total_ns),
                @tagName(summary.final_phase),
                summary.opened_indexes,
                bytesToMiB(summary.wal_replay_bytes),
                nsToMs(summary.wal_replay_ns),
                summary.current_applied_entries,
                summary.current_target_sequence,
                summary.progress_updates,
                summary.derived_target_sequence,
                bytesToMiB(summary.lsm_wal_retained_bytes),
                summary.lsm_wal_retained_segments,
            },
        );
    } else {
        std.debug.print(
            "artifact_rebuild_bench db_path={s} open_ms={d:.2} first_apply_ms=n/a total_s={d:.2} phase={s} opened_indexes={d} wal_replay_mb={d:.2} wal_replay_ms={d:.2} applied={d}/{d} progress_updates={d} derived_target_sequence={d} retained_wal_mb={d:.2} retained_wal_segments={d}\n",
            .{
                db_path,
                nsToMs(summary.open_ns),
                nsToS(summary.total_ns),
                @tagName(summary.final_phase),
                summary.opened_indexes,
                bytesToMiB(summary.wal_replay_bytes),
                nsToMs(summary.wal_replay_ns),
                summary.current_applied_entries,
                summary.current_target_sequence,
                summary.progress_updates,
                summary.derived_target_sequence,
                bytesToMiB(summary.lsm_wal_retained_bytes),
                summary.lsm_wal_retained_segments,
            },
        );
    }
}

fn cloneDirAbsolute(alloc: std.mem.Allocator, src_path: []const u8, dst_path: []const u8) !void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const src_dir = std.Io.Dir.openDirAbsolute(io, src_path, .{ .iterate = true }) catch |err| {
        std.debug.print("clone open source failed path={s} err={}\n", .{ src_path, err });
        return err;
    };
    defer src_dir.close(io);

    std.Io.Dir.createDirAbsolute(io, dst_path, .default_dir) catch |err| switch (err) {
        error.PathAlreadyExists => return error.PathAlreadyExists,
        else => {
            std.debug.print("clone create dest failed path={s} err={}\n", .{ dst_path, err });
            return err;
        },
    };
    const dst_dir = std.Io.Dir.openDirAbsolute(io, dst_path, .{}) catch |err| {
        std.debug.print("clone open dest failed path={s} err={}\n", .{ dst_path, err });
        return err;
    };
    defer dst_dir.close(io);

    var walker = try src_dir.walk(alloc);
    defer walker.deinit();
    while (try walker.next(io)) |entry| {
        switch (entry.kind) {
            .directory => try dst_dir.createDirPath(io, entry.path),
            .file => {
                const parent = std.fs.path.dirname(entry.path);
                if (parent) |dir_name| try dst_dir.createDirPath(io, dir_name);
                src_dir.copyFile(entry.path, dst_dir, entry.path, io, .{}) catch |err| {
                    std.debug.print("clone copy failed entry={s} err={}\n", .{ entry.path, err });
                    return err;
                };
            },
            else => {},
        }
    }
}

fn nowNs() u64 {
    return platform_time.monotonicNs();
}

fn elapsedSince(start_ns: u64) u64 {
    return nowNs() - start_ns;
}

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / @as(f64, std.time.ns_per_ms);
}

fn nsToS(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / @as(f64, std.time.ns_per_s);
}

fn bytesToMiB(bytes: u64) f64 {
    return @as(f64, @floatFromInt(bytes)) / (1024.0 * 1024.0);
}

fn sleepNs(duration_ns: u64) void {
    var req = std.posix.timespec{
        .sec = @intCast(duration_ns / std.time.ns_per_s),
        .nsec = @intCast(duration_ns % std.time.ns_per_s),
    };
    while (true) switch (std.posix.errno(std.posix.system.nanosleep(&req, &req))) {
        .SUCCESS => return,
        .INTR => continue,
        else => return,
    };
}
