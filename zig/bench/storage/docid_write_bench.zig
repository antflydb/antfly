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

const Config = struct {
    docs: usize = 2000,
    batch_size: usize = 200,
    body_repeat: usize = 1,
    sync_level: ?db_mod.types.SyncLevel = null,
};

const Phase = enum {
    insert,
    update,
    delete,
};

const PhaseResult = struct {
    phase: Phase,
    sync_level: db_mod.types.SyncLevel,
    docs: usize,
    batches: usize,
    ns: u64,
    profile: db_mod.BatchProfile,
    doc_identity: db_mod.types.DocIdentityStats,
};

pub fn main(init: std.process.Init) !void {
    const alloc = std.heap.c_allocator;
    const cfg = try parseArgs(init.minimal.args);
    const sync_levels = [_]db_mod.types.SyncLevel{ .propose, .write, .full_index };

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const out = &stdout_writer.interface;
    try out.print(
        "docid write bench docs={d} batch_size={d} body_repeat={d} sync={s}\n",
        .{
            cfg.docs,
            cfg.batch_size,
            cfg.body_repeat,
            if (cfg.sync_level) |level| db_mod.types.publicSyncLevelText(level) else "all",
        },
    );

    if (cfg.sync_level) |level| {
        try runSyncLevel(alloc, out, cfg, level);
    } else {
        for (sync_levels) |level| try runSyncLevel(alloc, out, cfg, level);
    }
    try stdout_writer.flush();
}

fn runSyncLevel(
    alloc: std.mem.Allocator,
    out: anytype,
    cfg: Config,
    sync_level: db_mod.types.SyncLevel,
) !void {
    var path_buf: [256]u8 = undefined;
    const path = std.fmt.bufPrint(&path_buf, "/tmp/antfly-docid-write-bench-{d}-{s}", .{
        platform_time.monotonicNs(),
        db_mod.types.publicSyncLevelText(sync_level),
    }) catch unreachable;

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

    try printPhase(out, try runWritePhase(alloc, &db, cfg, sync_level, .insert));
    try printPhase(out, try runWritePhase(alloc, &db, cfg, sync_level, .update));
    try printPhase(out, try runDeletePhase(alloc, &db, cfg, sync_level));
}

fn runWritePhase(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    cfg: Config,
    sync_level: db_mod.types.SyncLevel,
    phase: Phase,
) !PhaseResult {
    var total_profile = db_mod.BatchProfile{};
    var batches: usize = 0;
    const pass_idx: usize = if (phase == .insert) 0 else 1;
    const start_ns = nowNs();
    var start_doc: usize = 0;
    while (start_doc < cfg.docs) : (start_doc += cfg.batch_size) {
        const end_doc = @min(start_doc + cfg.batch_size, cfg.docs);
        const writes = try buildWrites(alloc, cfg, pass_idx, start_doc, end_doc);
        defer freeWrites(alloc, writes);
        var profile = db_mod.BatchProfile{};
        try db.batchProfiled(.{
            .writes = writes,
            .sync_level = sync_level,
        }, &profile);
        addBatchProfile(&total_profile, profile);
        batches += 1;
    }
    return .{
        .phase = phase,
        .sync_level = sync_level,
        .docs = cfg.docs,
        .batches = batches,
        .ns = nowNs() - start_ns,
        .profile = total_profile,
        .doc_identity = try snapshotDocIdentityStats(alloc, db),
    };
}

fn runDeletePhase(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    cfg: Config,
    sync_level: db_mod.types.SyncLevel,
) !PhaseResult {
    var total_profile = db_mod.BatchProfile{};
    var batches: usize = 0;
    const start_ns = nowNs();
    var start_doc: usize = 0;
    while (start_doc < cfg.docs) : (start_doc += cfg.batch_size) {
        const end_doc = @min(start_doc + cfg.batch_size, cfg.docs);
        const deletes = try buildDeleteKeys(alloc, start_doc, end_doc);
        defer freeDeleteKeys(alloc, deletes);
        var profile = db_mod.BatchProfile{};
        try db.batchProfiled(.{
            .deletes = deletes,
            .sync_level = sync_level,
        }, &profile);
        addBatchProfile(&total_profile, profile);
        batches += 1;
    }
    return .{
        .phase = .delete,
        .sync_level = sync_level,
        .docs = cfg.docs,
        .batches = batches,
        .ns = nowNs() - start_ns,
        .profile = total_profile,
        .doc_identity = try snapshotDocIdentityStats(alloc, db),
    };
}

fn buildWrites(
    alloc: std.mem.Allocator,
    cfg: Config,
    pass_idx: usize,
    start_doc: usize,
    end_doc: usize,
) ![]db_mod.types.BatchWrite {
    const writes = try alloc.alloc(db_mod.types.BatchWrite, end_doc - start_doc);
    var initialized: usize = 0;
    errdefer {
        for (writes[0..initialized]) |write| {
            alloc.free(write.key);
            alloc.free(write.value);
        }
        alloc.free(writes);
    }
    for (writes, start_doc..) |*write, doc_idx| {
        write.* = .{
            .key = try docKeyAlloc(alloc, doc_idx),
            .value = try documentJsonAlloc(alloc, doc_idx, pass_idx, cfg.body_repeat),
        };
        initialized += 1;
    }
    return writes;
}

fn freeWrites(alloc: std.mem.Allocator, writes: []db_mod.types.BatchWrite) void {
    for (writes) |write| {
        alloc.free(write.key);
        alloc.free(write.value);
    }
    alloc.free(writes);
}

fn buildDeleteKeys(alloc: std.mem.Allocator, start_doc: usize, end_doc: usize) ![]const []const u8 {
    const keys = try alloc.alloc([]const u8, end_doc - start_doc);
    var initialized: usize = 0;
    errdefer {
        for (keys[0..initialized]) |key| alloc.free(@constCast(key));
        alloc.free(keys);
    }
    for (keys, start_doc..) |*key, doc_idx| {
        key.* = try docKeyAlloc(alloc, doc_idx);
        initialized += 1;
    }
    return keys;
}

fn freeDeleteKeys(alloc: std.mem.Allocator, keys: []const []const u8) void {
    for (keys) |key| alloc.free(@constCast(key));
    alloc.free(keys);
}

fn docKeyAlloc(alloc: std.mem.Allocator, doc_idx: usize) ![]u8 {
    return try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{doc_idx});
}

fn documentJsonAlloc(alloc: std.mem.Allocator, doc_idx: usize, pass_idx: usize, body_repeat: usize) ![]u8 {
    var body = std.ArrayListUnmanaged(u8).empty;
    defer body.deinit(alloc);
    const prefix = try std.fmt.allocPrint(alloc, "document {d} pass {d}", .{ doc_idx, pass_idx });
    defer alloc.free(prefix);
    try body.appendSlice(alloc, prefix);
    for (0..body_repeat) |i| {
        const token = try std.fmt.allocPrint(alloc, " token-{d}", .{(doc_idx + i) % 97});
        defer alloc.free(token);
        try body.appendSlice(alloc, token);
    }
    return try std.fmt.allocPrint(alloc, "{{\"title\":\"doc-{d}\",\"body\":\"{s}\"}}", .{ doc_idx, body.items });
}

fn snapshotDocIdentityStats(alloc: std.mem.Allocator, db: *db_mod.DB) !db_mod.types.DocIdentityStats {
    const stats = try db.diagnosticStats(alloc);
    defer db_mod.freeDBStats(alloc, stats);
    return stats.doc_identity;
}

fn addBatchProfile(total: *db_mod.BatchProfile, delta: db_mod.BatchProfile) void {
    total.total_ns += delta.total_ns;
    total.extract_writes_ns += delta.extract_writes_ns;
    total.delete_artifacts_ns += delta.delete_artifacts_ns;
    total.precompute_generated_ns += delta.precompute_generated_ns;
    total.identity_capacity_check_ns += delta.identity_capacity_check_ns;
    total.identity_metadata_ns += delta.identity_metadata_ns;
    total.identity_metadata_writes += delta.identity_metadata_writes;
    total.store_write_ns += delta.store_write_ns;
    total.append_replay_journal_ns += delta.append_replay_journal_ns;
    total.wait_sync_ns += delta.wait_sync_ns;
    total.sync_wait_ns += delta.sync_wait_ns;
    total.build_derived_ns += delta.build_derived_ns;
}

fn printPhase(writer: anytype, result: PhaseResult) !void {
    const ops_per_sec = if (result.ns == 0)
        0.0
    else
        @as(f64, @floatFromInt(result.docs)) / (@as(f64, @floatFromInt(result.ns)) / 1e9);
    try writer.print(
        "{{\"sync_level\":\"{s}\",\"phase\":\"{s}\",\"docs\":{d},\"batches\":{d},\"ns\":{d},\"ops_per_sec\":{d:.2},\"extract_writes_ns\":{d},\"delete_artifacts_ns\":{d},\"precompute_generated_ns\":{d},\"identity_capacity_ns\":{d},\"identity_metadata_ns\":{d},\"identity_metadata_writes\":{d},\"build_derived_ns\":{d},\"store_write_ns\":{d},\"append_replay_journal_ns\":{d},\"wait_sync_ns\":{d},\"sync_wait_ns\":{d},\"allocated_ordinals\":{d},\"live_ordinals\":{d},\"tombstone_ordinals\":{d},\"next_ordinal\":{d}}}\n",
        .{
            db_mod.types.publicSyncLevelText(result.sync_level),
            @tagName(result.phase),
            result.docs,
            result.batches,
            result.ns,
            ops_per_sec,
            result.profile.extract_writes_ns,
            result.profile.delete_artifacts_ns,
            result.profile.precompute_generated_ns,
            result.profile.identity_capacity_check_ns,
            result.profile.identity_metadata_ns,
            result.profile.identity_metadata_writes,
            result.profile.build_derived_ns,
            result.profile.store_write_ns,
            result.profile.append_replay_journal_ns,
            result.profile.wait_sync_ns,
            result.profile.sync_wait_ns,
            result.doc_identity.allocated_ordinals,
            result.doc_identity.live_ordinals,
            result.doc_identity.tombstone_ordinals,
            result.doc_identity.next_ordinal,
        },
    );
}

fn parseArgs(args_in: std.process.Args) !Config {
    var cfg = Config{};
    var args = std.process.Args.Iterator.init(args_in);
    _ = args.skip();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--docs")) {
            cfg.docs = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--batch-size")) {
            cfg.batch_size = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--body-repeat")) {
            cfg.body_repeat = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--sync-level")) {
            const raw = args.next() orelse return error.InvalidArgument;
            cfg.sync_level = db_mod.types.parsePublicSyncLevelText(raw) orelse return error.InvalidArgument;
        } else {
            std.debug.print("invalid argument: {s}\n", .{arg});
            return error.InvalidArgument;
        }
    }
    if (cfg.docs == 0 or cfg.batch_size == 0 or cfg.body_repeat == 0) return error.InvalidArgument;
    return cfg;
}

fn parseNextUsize(args: *std.process.Args.Iterator, flag: []const u8) !usize {
    const raw = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return try std.fmt.parseInt(usize, raw, 10);
}

fn nowNs() u64 {
    return platform_time.monotonicNs();
}
