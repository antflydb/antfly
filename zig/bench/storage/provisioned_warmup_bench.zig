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
const metadata_api = antfly.metadata_api;
const metadata_mod = antfly.metadata;
const metadata_table_manager = antfly.metadata.table_manager;
const metadata_transition_state = antfly.metadata.transition_state;
const platform_time = antfly.platform_time;
const raft_mod = antfly.raft;
const raft_reconciler = antfly.raft.reconciler;

const group_id: u64 = 77;
const table_id: u64 = 7;
const node_id: u64 = 9;
const store_id: u64 = 19;
const table_name = "docs";

const Operation = enum {
    lookup,
    batch,
};

const Config = struct {
    docs: usize = 200,
    batch_size: usize = 25,
    body_repeat: usize = 8,
    warmup_timeout_ms: u64 = 10_000,
};

const Summary = struct {
    operation: Operation,
    warmup: bool,
    warmup_ns: u64 = 0,
    operation_ns: u64 = 0,
    read_cache_hits: u64 = 0,
    read_cache_misses: u64 = 0,
    write_cache_hits: u64 = 0,
    write_cache_misses: u64 = 0,
    warmup_started: u64 = 0,
    warmup_completed: u64 = 0,
    warmup_failed: u64 = 0,
    runtime_refresh_started: u64 = 0,
    runtime_refresh_completed: u64 = 0,
    runtime_refresh_failed: u64 = 0,
    runtime_tables: usize = 0,
    runtime_groups: usize = 0,
    lookup_found: bool = false,
    cached_write_dbs: usize = 0,
};

pub fn main(init: std.process.Init) !void {
    const alloc = std.heap.c_allocator;
    const cfg = try parseArgs(init.minimal.args);

    const cold_lookup = try runScenario(alloc, cfg, .lookup, false);
    const warm_lookup = try runScenario(alloc, cfg, .lookup, true);
    const cold_batch = try runScenario(alloc, cfg, .batch, false);
    const warm_batch = try runScenario(alloc, cfg, .batch, true);

    printSummary(cfg, cold_lookup);
    printSummary(cfg, warm_lookup);
    printSummary(cfg, cold_batch);
    printSummary(cfg, warm_batch);
    printComparison(.lookup, cold_lookup, warm_lookup);
    printComparison(.batch, cold_batch, warm_batch);
}

fn parseArgs(args_in: std.process.Args) !Config {
    var cfg = Config{};
    var args = std.process.Args.Iterator.init(args_in);
    _ = args.skip();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--docs")) {
            cfg.docs = try parseNextUsize(&args, "--docs");
        } else if (std.mem.eql(u8, arg, "--batch-size")) {
            cfg.batch_size = try parseNextUsize(&args, "--batch-size");
        } else if (std.mem.eql(u8, arg, "--body-repeat")) {
            cfg.body_repeat = try parseNextUsize(&args, "--body-repeat");
        } else if (std.mem.eql(u8, arg, "--warmup-timeout-ms")) {
            cfg.warmup_timeout_ms = try parseNextU64(&args, "--warmup-timeout-ms");
        } else {
            std.debug.print("invalid argument: {s}\n", .{arg});
            return error.InvalidArgument;
        }
    }
    if (cfg.docs == 0 or cfg.batch_size == 0 or cfg.body_repeat == 0 or cfg.warmup_timeout_ms == 0) return error.InvalidArgument;
    return cfg;
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

fn runScenario(alloc: std.mem.Allocator, cfg: Config, operation: Operation, warmup: bool) !Summary {
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);
    try seedReplicaRoot(alloc, std.mem.span(path), cfg);

    var server = initBenchServer(alloc, std.mem.span(path));
    defer server.deinit();
    server.provisioned_storage.attachSources(&server.read_source, &server.write_source);

    var summary: Summary = .{
        .operation = operation,
        .warmup = warmup,
    };

    if (warmup) {
        const started_ns = nowNs();
        try server.requestProvisionedCacheWarmup();
        try waitForStartupWarmup(&server, cfg.warmup_timeout_ms * std.time.ns_per_ms);
        summary.warmup_ns = elapsedSince(started_ns);
    }

    const operation_started_ns = nowNs();
    switch (operation) {
        .lookup => {
            var lookup = (try server.read_source.source().lookup(alloc, table_name, "doc:00000000", .{}, .read_index)).?;
            defer lookup.deinit(alloc);
            summary.lookup_found = true;
        },
        .batch => {
            _ = try server.write_source.source().batch(alloc, table_name, .{
                .writes = &.{.{ .key = "doc:warmup-bench", .value = "{\"title\":\"warmup bench\",\"body\":\"cache hit write\"}" }},
                .timestamp_ns = 2,
                .sync_level = .write,
            });
        },
    }
    summary.operation_ns = elapsedSince(operation_started_ns);

    const read_cache_stats = server.provisioned_storage.read_cache.cacheStats();
    const write_cache_stats = server.provisioned_storage.write_cache.cacheStats();
    const runtime_summary = server.provisioned_storage.runtime_status_cache.summary();
    summary.read_cache_hits = read_cache_stats.hit_count;
    summary.read_cache_misses = read_cache_stats.miss_count;
    summary.write_cache_hits = write_cache_stats.hit_count;
    summary.write_cache_misses = write_cache_stats.miss_count;
    summary.warmup_started = server.provisioned_warmup_started.load(.monotonic);
    summary.warmup_completed = server.provisioned_warmup_completed.load(.monotonic);
    summary.warmup_failed = server.provisioned_warmup_failed.load(.monotonic);
    summary.runtime_refresh_started = server.runtime_status_refresh_started.load(.monotonic);
    summary.runtime_refresh_completed = server.runtime_status_refresh_completed.load(.monotonic);
    summary.runtime_refresh_failed = server.runtime_status_refresh_failed.load(.monotonic);
    summary.runtime_tables = runtime_summary.table_count;
    summary.runtime_groups = runtime_summary.group_count;
    summary.cached_write_dbs = server.write_source.cachedWriteDbCount();
    return summary;
}

fn waitForStartupWarmup(server: *antfly.data.runtime.DataServer, timeout_ns: u64) !void {
    const deadline_ns = nowNs() + timeout_ns;
    while (true) {
        const warmup_done = !server.provisioned_warmup_active.load(.acquire);
        const refresh_done = !server.runtime_status_refresh_active.load(.acquire);
        const warmup_observed = server.provisioned_warmup_started.load(.monotonic) > 0;
        if (warmup_observed and warmup_done and refresh_done) return;
        if (nowNs() >= deadline_ns) return error.Timeout;
        sleepNs(std.time.ns_per_ms);
    }
}

fn seedReplicaRoot(alloc: std.mem.Allocator, replica_root_dir: []const u8, cfg: Config) !void {
    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(db_path);

    var db = try db_mod.DB.open(alloc, db_path, .{});
    defer db.close();

    const writes_buf = try alloc.alloc(db_mod.types.BatchWrite, cfg.batch_size);
    defer alloc.free(writes_buf);

    var start: usize = 0;
    while (start < cfg.docs) : (start += cfg.batch_size) {
        const end = @min(start + cfg.batch_size, cfg.docs);
        const writes = writes_buf[0 .. end - start];
        defer {
            for (writes) |write| {
                alloc.free(@constCast(write.key));
                alloc.free(@constCast(write.value));
            }
        }

        for (start..end, 0..) |doc_idx, i| {
            writes[i] = .{
                .key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{doc_idx}),
                .value = try documentJson(alloc, doc_idx, cfg.body_repeat),
            };
        }
        try db.batch(.{
            .writes = writes,
            .timestamp_ns = @intCast(end),
            .sync_level = .write,
        });
    }
}

fn documentJson(alloc: std.mem.Allocator, doc_idx: usize, body_repeat: usize) ![]u8 {
    var body = std.ArrayList(u8).empty;
    defer body.deinit(alloc);
    for (0..body_repeat) |i| {
        _ = i;
        try body.appendSlice(alloc, "warmup bench alpha beta gamma delta epsilon ");
    }
    return try std.fmt.allocPrint(
        alloc,
        "{{\"title\":\"doc {d}\",\"body\":\"{s}\"}}",
        .{ doc_idx, body.items },
    );
}

fn initBenchServer(alloc: std.mem.Allocator, replica_root_dir: []const u8) antfly.data.runtime.DataServer {
    return .{
        .alloc = alloc,
        .store_registration = .{
            .node_id = node_id,
            .store_id = store_id,
            .role = "data",
            .failure_domain = "bench",
        },
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            replica_root_dir,
            BenchCatalog.iface(),
            raft_mod.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
            replica_root_dir,
            BenchCatalog.iface(),
        ),
        .status_source = BenchStatus.iface(),
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
}

const BenchStatus = struct {
    fn iface() antfly.public_api.http_server.StatusSource {
        return .{
            .ptr = undefined,
            .vtable = &.{
                .status = status,
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
            },
        };
    }

    fn status(_: *anyopaque) !metadata_api.MetadataStatus {
        return .{ .metadata_group_id = 1, .metrics = .{} };
    }

    fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
        return .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                .table_id = table_id,
                .name = table_name,
                .description = "warmup bench table",
                .schema_json = "",
                .read_schema_json = "",
                .indexes_json = antfly.public_api.tables.default_indexes_json,
                .replication_sources_json = "[]",
                .placement_role = "data",
            }})[0..]),
            .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                .group_id = group_id,
                .table_id = table_id,
                .start_key = "",
                .end_key = null,
            }})[0..]),
            .stores = @constCast((&[_]metadata_table_manager.StoreRecord{.{
                .store_id = store_id,
                .node_id = node_id,
                .role = "data",
                .live = true,
                .health_class = "healthy",
            }})[0..]),
            .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{.{
                .record = .{ .group_id = group_id, .replica_id = 1, .local_node_id = node_id },
                .store_id = store_id,
                .peer_node_ids = &.{node_id},
            }})[0..]),
            .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
            .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
        };
    }

    fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
};

const BenchCatalog = struct {
    fn iface() antfly.public_api.table_catalog.CatalogSource {
        return .{
            .ptr = undefined,
            .vtable = &.{
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
            },
        };
    }

    fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
        return try BenchStatus.adminSnapshot(undefined);
    }

    fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
};

fn printSummary(cfg: Config, summary: Summary) void {
    std.debug.print(
        "provisioned_warmup_bench operation={s} warmup={} docs={d} batch_size={d} body_repeat={d} warmup_ms={d:.3} operation_ms={d:.3} read_cache_hits={d} read_cache_misses={d} write_cache_hits={d} write_cache_misses={d} warmup_started={d} warmup_completed={d} warmup_failed={d} runtime_refresh_started={d} runtime_refresh_completed={d} runtime_refresh_failed={d} runtime_tables={d} runtime_groups={d} lookup_found={} cached_write_dbs={d}\n",
        .{
            @tagName(summary.operation),
            summary.warmup,
            cfg.docs,
            cfg.batch_size,
            cfg.body_repeat,
            nsToMsFloat(summary.warmup_ns),
            nsToMsFloat(summary.operation_ns),
            summary.read_cache_hits,
            summary.read_cache_misses,
            summary.write_cache_hits,
            summary.write_cache_misses,
            summary.warmup_started,
            summary.warmup_completed,
            summary.warmup_failed,
            summary.runtime_refresh_started,
            summary.runtime_refresh_completed,
            summary.runtime_refresh_failed,
            summary.runtime_tables,
            summary.runtime_groups,
            summary.lookup_found,
            summary.cached_write_dbs,
        },
    );
    std.debug.print(
        "provisioned_warmup_bench_csv operation,warmup,docs,batch_size,body_repeat,warmup_ms,operation_ms,read_cache_hits,read_cache_misses,write_cache_hits,write_cache_misses,warmup_started,warmup_completed,warmup_failed,runtime_refresh_started,runtime_refresh_completed,runtime_refresh_failed,runtime_tables,runtime_groups,lookup_found,cached_write_dbs\n",
        .{},
    );
    std.debug.print(
        "provisioned_warmup_bench_csv {s},{any},{d},{d},{d},{d:.3},{d:.3},{d},{d},{d},{d},{d},{d},{d},{d},{d},{d},{d},{d},{any},{d}\n",
        .{
            @tagName(summary.operation),
            summary.warmup,
            cfg.docs,
            cfg.batch_size,
            cfg.body_repeat,
            nsToMsFloat(summary.warmup_ns),
            nsToMsFloat(summary.operation_ns),
            summary.read_cache_hits,
            summary.read_cache_misses,
            summary.write_cache_hits,
            summary.write_cache_misses,
            summary.warmup_started,
            summary.warmup_completed,
            summary.warmup_failed,
            summary.runtime_refresh_started,
            summary.runtime_refresh_completed,
            summary.runtime_refresh_failed,
            summary.runtime_tables,
            summary.runtime_groups,
            summary.lookup_found,
            summary.cached_write_dbs,
        },
    );
}

fn printComparison(operation: Operation, cold: Summary, warm: Summary) void {
    const op_delta_ns = cold.operation_ns -| warm.operation_ns;
    const ratio = if (warm.operation_ns == 0) 0.0 else @as(f64, @floatFromInt(cold.operation_ns)) / @as(f64, @floatFromInt(warm.operation_ns));
    std.debug.print(
        "provisioned_warmup_bench_compare operation={s} cold_operation_ms={d:.3} warm_operation_ms={d:.3} saved_ms={d:.3} speedup={d:.3}x\n",
        .{
            @tagName(operation),
            nsToMsFloat(cold.operation_ns),
            nsToMsFloat(warm.operation_ns),
            nsToMsFloat(op_delta_ns),
            ratio,
        },
    );
}

fn nsToMsFloat(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / @as(f64, @floatFromInt(std.time.ns_per_ms));
}

fn nowNs() u64 {
    return platform_time.monotonicNs();
}

fn elapsedSince(started_ns: u64) u64 {
    return nowNs() - started_ns;
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

var temp_path_nonce: u64 = 0;

fn tempPath(buf: []u8) [*:0]const u8 {
    const ts = nowNs();
    const nonce = @atomicRmw(u64, &temp_path_nonce, .Add, 1, .monotonic);
    const path_bytes = std.fmt.bufPrint(buf, "/tmp/antfly-provisioned-warmup-bench-{d}-{d}\x00", .{ ts, nonce }) catch unreachable;
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
