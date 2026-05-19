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

const DB = antfly.db.DB;
const DocStore = antfly.docstore.DocStore;
const IndexManager = antfly.db.IndexManager;
const DenseSplitHandoff = antfly.db.DenseSplitHandoff;
const TextSplitHandoff = antfly.db.TextSplitHandoff;
const SparseSplitHandoff = antfly.db.SparseSplitHandoff;
const ShardManager = antfly.shard.ShardManager;
const KVPair = antfly.docstore.KVPair;
const types = antfly.db.types;

const Config = struct {
    samples: usize = 5,
    docs: usize = 20_000,
    value_size: usize = 256,
};

const DurStats = struct {
    values: std.ArrayListUnmanaged(u64) = .empty,
    total: u128 = 0,

    fn add(self: *DurStats, alloc: std.mem.Allocator, value: u64) !void {
        try self.values.append(alloc, value);
        self.total += value;
    }

    fn avg(self: DurStats) u64 {
        if (self.values.items.len == 0) return 0;
        return @intCast(self.total / self.values.items.len);
    }

    fn deinit(self: *DurStats, alloc: std.mem.Allocator) void {
        self.values.deinit(alloc);
        self.* = undefined;
    }
};

const Results = struct {
    old_prepare_ns: DurStats = .{},
    current_prepare_ns: DurStats = .{},
    old_prune_ns: DurStats = .{},
    current_prune_ns: DurStats = .{},
    old_store_finalize_ns: DurStats = .{},
    current_store_finalize_ns: DurStats = .{},

    fn deinit(self: *Results, alloc: std.mem.Allocator) void {
        self.old_prepare_ns.deinit(alloc);
        self.current_prepare_ns.deinit(alloc);
        self.old_prune_ns.deinit(alloc);
        self.current_prune_ns.deinit(alloc);
        self.old_store_finalize_ns.deinit(alloc);
        self.current_store_finalize_ns.deinit(alloc);
    }
};

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const cfg = try parseArgs(gpa, init.minimal.args);

    var results = Results{};
    defer results.deinit(gpa);

    var sample: usize = 0;
    while (sample < cfg.samples) : (sample += 1) {
        const run = try runSample(gpa, cfg, sample);
        try results.old_prepare_ns.add(gpa, run.old_prepare_ns);
        try results.current_prepare_ns.add(gpa, run.current_prepare_ns);
        try results.old_prune_ns.add(gpa, run.old_prune_ns);
        try results.current_prune_ns.add(gpa, run.current_prune_ns);
        try results.old_store_finalize_ns.add(gpa, run.old_store_finalize_ns);
        try results.current_store_finalize_ns.add(gpa, run.current_store_finalize_ns);
    }

    printStats("split_prepare_old", results.old_prepare_ns);
    printStats("split_prepare_current", results.current_prepare_ns);
    printStats("split_prune_old", results.old_prune_ns);
    printStats("split_prune_current", results.current_prune_ns);
    printStats("split_store_finalize_old", results.old_store_finalize_ns);
    printStats("split_store_finalize_current", results.current_store_finalize_ns);
}

fn parseArgs(alloc: std.mem.Allocator, proc_args: std.process.Args) !Config {
    var cfg = Config{};
    var args = try std.process.Args.Iterator.initAllocator(proc_args, alloc);
    defer args.deinit();
    _ = args.next();

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--samples")) {
            cfg.samples = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--docs")) {
            cfg.docs = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--value-size")) {
            cfg.value_size = try parseNextUsize(&args, arg);
        } else {
            return error.InvalidArgument;
        }
    }

    return cfg;
}

fn parseNextUsize(args: anytype, flag: []const u8) !usize {
    const value = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return std.fmt.parseUnsigned(usize, value, 10);
}

const SampleResult = struct {
    old_prepare_ns: u64,
    current_prepare_ns: u64,
    old_prune_ns: u64,
    current_prune_ns: u64,
    old_store_finalize_ns: u64,
    current_store_finalize_ns: u64,
};

fn runSample(alloc: std.mem.Allocator, cfg: Config, sample_idx: usize) !SampleResult {
    var src_buf: [256]u8 = undefined;
    const src_path = benchPath(&src_buf, "db-src", sample_idx);
    cleanupDbDirAt(src_path);
    defer cleanupDbDirAt(src_path);

    var src_store = try DocStore.open(alloc, src_path, .{});
    defer src_store.close();
    var src_indexes = try IndexManager.init(alloc, std.mem.span(src_path));
    defer src_indexes.deinit();
    src_indexes.updateRange(.{ .start = "", .end = "" });
    try src_indexes.addAllNoBackfill(&src_store, splitIndexConfigs());
    try populateStoreAndIndexes(alloc, &src_store, &src_indexes, cfg.docs, cfg.value_size);

    var src_mgr = try ShardManager.init(alloc, &src_store, .{ .start = "", .end = "" });
    defer src_mgr.deinit();

    var split_key_buf: [64]u8 = undefined;
    const split_key = formatDocKey(&split_key_buf, cfg.docs / 2);

    var old_dest_buf: [256]u8 = undefined;
    const old_dest_path = benchPath(&old_dest_buf, "db-old-dst", sample_idx);
    cleanupDbDirAt(old_dest_path);
    defer cleanupDbDirAt(old_dest_path);

    const old_started = nowNs();
    try emulateOldPrepareSplit(alloc, &src_mgr, split_key, old_dest_path);
    const old_prepare_ns = elapsedSince(old_started);

    var current_dest_buf: [256]u8 = undefined;
    const current_dest_path = benchPath(&current_dest_buf, "db-current-dst", sample_idx);
    cleanupDbDirAt(current_dest_path);
    defer cleanupDbDirAt(current_dest_path);

    const current_started = nowNs();
    try runCurrentPrepareSplit(alloc, &src_store, &src_mgr, &src_indexes, split_key, current_dest_path);
    const current_prepare_ns = elapsedSince(current_started);

    try expectPreparedSplitEquivalent(
        alloc,
        split_key,
        std.mem.span(old_dest_path),
        std.mem.span(current_dest_path),
        cfg.docs - (cfg.docs / 2),
    );

    const old_prune_ns = try benchmarkPruneSample(alloc, cfg, sample_idx, split_key, .old);
    const current_prune_ns = try benchmarkPruneSample(alloc, cfg, sample_idx, split_key, .current);
    const old_store_finalize_ns = try benchmarkStoreFinalizeSample(alloc, cfg, sample_idx, split_key, .old);
    const current_store_finalize_ns = try benchmarkStoreFinalizeSample(alloc, cfg, sample_idx, split_key, .current);

    return .{
        .old_prepare_ns = old_prepare_ns,
        .current_prepare_ns = current_prepare_ns,
        .old_prune_ns = old_prune_ns,
        .current_prune_ns = current_prune_ns,
        .old_store_finalize_ns = old_store_finalize_ns,
        .current_store_finalize_ns = current_store_finalize_ns,
    };
}

const PruneMode = enum {
    old,
    current,
};

fn benchmarkPruneSample(alloc: std.mem.Allocator, cfg: Config, sample_idx: usize, split_key: []const u8, mode: PruneMode) !u64 {
    var src_buf: [256]u8 = undefined;
    const tag = switch (mode) {
        .old => "db-prune-old-src",
        .current => "db-prune-current-src",
    };
    const src_path = benchPath(&src_buf, tag, sample_idx);
    cleanupDbDirAt(src_path);
    defer cleanupDbDirAt(src_path);

    var store = try DocStore.open(alloc, src_path, .{});
    defer store.close();
    var indexes = try IndexManager.init(alloc, std.mem.span(src_path));
    defer indexes.deinit();
    indexes.updateRange(.{ .start = "", .end = "" });
    try indexes.addAllNoBackfill(&store, splitIndexConfigs());
    try populateStoreAndIndexes(alloc, &store, &indexes, cfg.docs, cfg.value_size);

    const started = nowNs();
    switch (mode) {
        .old => try emulateOldPruneSplit(alloc, &store, &indexes, split_key, ""),
        .current => try emulateCurrentPruneSplit(alloc, &store, &indexes, split_key, ""),
    }
    return elapsedSince(started);
}

fn benchmarkStoreFinalizeSample(alloc: std.mem.Allocator, cfg: Config, sample_idx: usize, split_key: []const u8, mode: PruneMode) !u64 {
    var src_buf: [256]u8 = undefined;
    const tag = switch (mode) {
        .old => "db-store-finalize-old-src",
        .current => "db-store-finalize-current-src",
    };
    const src_path = benchPath(&src_buf, tag, sample_idx);
    cleanupDbDirAt(src_path);
    defer cleanupDbDirAt(src_path);

    var store = try DocStore.open(alloc, src_path, .{});
    defer store.close();
    try populateStoreOnly(alloc, &store, cfg.docs, cfg.value_size);

    const started = nowNs();
    switch (mode) {
        .old => try emulateOldStoreFinalize(alloc, &store, split_key, ""),
        .current => _ = try store.rewriteLeftInPlace(split_key),
    }
    return elapsedSince(started);
}

fn populateStoreAndIndexes(alloc: std.mem.Allocator, store: *DocStore, indexes: *IndexManager, docs: usize, value_size: usize) !void {
    const batch_size = 512;
    const payload = try alloc.alloc(u8, value_size);
    defer alloc.free(payload);
    @memset(payload, 'x');

    var writes = std.ArrayListUnmanaged(KVPair).empty;
    defer {
        for (writes.items) |write| alloc.free(@constCast(write.value));
        writes.deinit(alloc);
    }

    var key_buf: [64]u8 = undefined;
    var i: usize = 0;
    while (i < docs) : (i += 1) {
        const sparse_primary: u32 = @intCast(i % 64);
        const sparse_secondary: u32 = @intCast(128 + (i % 17));
        const value = try std.fmt.allocPrint(
            alloc,
            "{{\"title\":\"title {d:0>8}\",\"body\":\"{s}\",\"sparse\":{{\"indices\":[{d},{d}],\"values\":[1.0,0.25]}}}}",
            .{ i, payload, sparse_primary, sparse_secondary },
        );
        errdefer alloc.free(value);
        try writes.append(alloc, .{
            .key = try alloc.dupe(u8, formatDocKey(&key_buf, i)),
            .value = value,
        });

        if (writes.items.len == batch_size) {
            try store.putBatch(writes.items, &.{});
            try indexStoredWrites(indexes, store, writes.items);
            clearWrites(alloc, &writes);
        }
    }

    if (writes.items.len > 0) {
        try store.putBatch(writes.items, &.{});
        try indexStoredWrites(indexes, store, writes.items);
        clearWrites(alloc, &writes);
    }
}

fn populateStoreOnly(alloc: std.mem.Allocator, store: *DocStore, docs: usize, value_size: usize) !void {
    const batch_size = 512;
    const payload = try alloc.alloc(u8, value_size);
    defer alloc.free(payload);
    @memset(payload, 'x');

    var writes = std.ArrayListUnmanaged(KVPair).empty;
    defer {
        for (writes.items) |write| alloc.free(@constCast(write.value));
        writes.deinit(alloc);
    }

    var key_buf: [64]u8 = undefined;
    var i: usize = 0;
    while (i < docs) : (i += 1) {
        const value = try std.fmt.allocPrint(alloc, "{{\"title\":\"title {d:0>8}\",\"body\":\"{s}\"}}", .{ i, payload });
        errdefer alloc.free(value);
        try writes.append(alloc, .{
            .key = try alloc.dupe(u8, formatDocKey(&key_buf, i)),
            .value = value,
        });

        if (writes.items.len == batch_size) {
            try store.putBatch(writes.items, &.{});
            clearWrites(alloc, &writes);
        }
    }
    if (writes.items.len > 0) try store.putBatch(writes.items, &.{});
}

fn indexStoredWrites(indexes: *IndexManager, store: *DocStore, writes: []const KVPair) !void {
    var batch_writes = try std.heap.page_allocator.alloc(types.BatchWrite, writes.len);
    defer std.heap.page_allocator.free(batch_writes);
    for (writes, 0..) |write, i| {
        batch_writes[i] = .{
            .key = write.key,
            .value = write.value,
        };
    }
    try indexes.indexBatchWithOptions(store, batch_writes, .{ .compact_text = false });
}

fn clearWrites(alloc: std.mem.Allocator, writes: *std.ArrayListUnmanaged(KVPair)) void {
    for (writes.items) |write| {
        alloc.free(@constCast(write.key));
        alloc.free(@constCast(write.value));
    }
    writes.clearRetainingCapacity();
}

fn emulateOldPrepareSplit(alloc: std.mem.Allocator, src_mgr: *ShardManager, split_key: []const u8, dest_dir: [*:0]const u8) !void {
    var dest_store = try DocStore.open(alloc, dest_dir, .{});
    defer dest_store.close();

    var dest_indexes = try IndexManager.init(alloc, std.mem.span(dest_dir));
    defer dest_indexes.deinit();
    dest_indexes.updateRange(.{
        .start = split_key,
        .end = src_mgr.getByteRange().end,
    });

    try dest_indexes.addAllNoBackfill(&dest_store, splitIndexConfigs());
    try src_mgr.streamRange(split_key, src_mgr.getByteRange().end, &dest_store);
    try backfillPreparedDestination(alloc, &dest_store, &dest_indexes, split_key, src_mgr.getByteRange().end);
}

fn runCurrentPrepareSplit(
    alloc: std.mem.Allocator,
    src_store: *DocStore,
    src_mgr: *ShardManager,
    src_indexes: *IndexManager,
    split_key: []const u8,
    dest_dir: [*:0]const u8,
) !void {
    const dest_dir_buf = try alloc.dupe(u8, std.mem.span(dest_dir));
    defer alloc.free(dest_dir_buf);
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    try std.Io.Dir.cwd().createDirPath(io_impl.io(), std.mem.span(dest_dir));
    const page_split_built = try src_store.splitRightToDir(split_key, dest_dir_buf);

    var dest_store = try DocStore.open(alloc, dest_dir, .{});
    defer dest_store.close();

    var dest_indexes = try IndexManager.init(alloc, std.mem.span(dest_dir));
    defer dest_indexes.deinit();
    dest_indexes.updateRange(.{
        .start = split_key,
        .end = src_mgr.getByteRange().end,
    });

    try dest_indexes.addAllNoBackfill(&dest_store, splitIndexConfigs());
    const collect_skip_doc_keys = false;
    const dense_handoffs = try dest_indexes.handoffDenseFrom(src_indexes, &dest_store, split_key, collect_skip_doc_keys);
    defer {
        for (dense_handoffs) |*handoff| handoff.deinit(alloc);
        alloc.free(dense_handoffs);
    }
    const text_handoffs = try dest_indexes.handoffRightOnlyTextSegmentsFrom(src_indexes, split_key, collect_skip_doc_keys);
    defer {
        for (text_handoffs) |*handoff| handoff.deinit(alloc);
        alloc.free(text_handoffs);
    }
    const sparse_handoffs = try dest_indexes.handoffSparseFrom(src_indexes, split_key, src_mgr.getByteRange().end, collect_skip_doc_keys);
    defer {
        for (sparse_handoffs) |*handoff| handoff.deinit(alloc);
        alloc.free(sparse_handoffs);
    }
    _ = try dest_indexes.rebuildGraphSplitDestination(split_key, src_mgr.getByteRange().end);
    if (page_split_built) {
        try deleteSplitMetadataFromStore(alloc, &dest_store);
        if (dest_indexes.splitDestinationNeedsDocumentIndexing(dense_handoffs, text_handoffs, sparse_handoffs)) {
            if (!collect_skip_doc_keys) return error.UnexpectedSplitResidualIndexing;
            try indexPreparedDestinationRange(alloc, &dest_store, &dest_indexes, split_key, src_mgr.getByteRange().end, dense_handoffs, text_handoffs, sparse_handoffs);
        }
    } else {
        try streamRangeIntoPreparedDestination(alloc, src_store, split_key, src_mgr.getByteRange().end, &dest_store, &dest_indexes, dense_handoffs, text_handoffs, sparse_handoffs);
    }
}

fn backfillPreparedDestination(
    alloc: std.mem.Allocator,
    dest_store: *DocStore,
    dest_indexes: *IndexManager,
    lower: []const u8,
    upper: []const u8,
) !void {
    const batch_size = 8192;

    var txn = try dest_store.env.begin(.{ .read_only = true });
    defer txn.abort();

    var iter = try txn.rangeViewScanner(dest_store.dbi, lower);
    defer iter.close();

    var writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
    defer writes.deinit(alloc);

    outer: while (true) {
        const batch = iter.nextViewBatch() catch |err| switch (err) {
            antfly.lmdb.Error.NotFound => break,
            else => return err,
        };

        var index: usize = 0;
        while (index < batch.len()) : (index += 1) {
            const entry = try batch.entryAt(index);
            if (upper.len > 0 and std.mem.order(u8, entry.key, upper) != .lt) break :outer;

            try writes.append(alloc, .{
                .key = entry.key,
                .value = entry.value,
            });
            if (writes.items.len == batch_size) {
                try dest_indexes.indexBatchWithOptions(dest_store, writes.items, .{ .compact_text = false });
                writes.clearRetainingCapacity();
            }
        }
    }

    if (writes.items.len > 0) try dest_indexes.indexBatchWithOptions(dest_store, writes.items, .{ .compact_text = false });
}

fn emulateOldPruneSplit(
    alloc: std.mem.Allocator,
    store: *DocStore,
    indexes: *IndexManager,
    lower: []const u8,
    upper: []const u8,
) !void {
    const batch_size = 8192;

    var txn = try store.env.begin(.{ .read_only = true });
    defer txn.abort();

    var iter = try txn.rangeViewScanner(store.dbi, lower);
    defer iter.close();

    var delete_keys = std.ArrayListUnmanaged([]const u8).empty;
    defer delete_keys.deinit(alloc);

    outer: while (true) {
        const batch = iter.nextViewBatch() catch |err| switch (err) {
            antfly.lmdb.Error.NotFound => break,
            else => return err,
        };

        var index: usize = 0;
        while (index < batch.len()) : (index += 1) {
            const entry = try batch.entryAt(index);
            if (upper.len > 0 and std.mem.order(u8, entry.key, upper) != .lt) break :outer;

            try delete_keys.append(alloc, entry.key);
            if (delete_keys.items.len == batch_size) {
                try indexes.deleteBatchWithoutText(store, delete_keys.items);
                try tombstoneTextDeletesWithoutCompaction(indexes, delete_keys.items);
                delete_keys.clearRetainingCapacity();
            }
        }
    }

    if (delete_keys.items.len > 0) {
        try indexes.deleteBatchWithoutText(store, delete_keys.items);
        try tombstoneTextDeletesWithoutCompaction(indexes, delete_keys.items);
    }
}

fn tombstoneTextDeletesWithoutCompaction(indexes: *IndexManager, keys: []const []const u8) !void {
    for (indexes.text_indexes.items) |*entry| {
        for (keys) |key| {
            _ = try entry.persistent.deleteById(key);
        }
    }
}

fn emulateCurrentPruneSplit(
    _: std.mem.Allocator,
    store: *DocStore,
    indexes: *IndexManager,
    lower: []const u8,
    upper: []const u8,
) !void {
    try indexes.pruneTextSplitRange(lower);
    try indexes.pruneDenseSplitRange(store, lower);
    try indexes.pruneSparseSplitRange(lower, upper);
    try indexes.pruneGraphSplitRange(lower, upper);
}

fn emulateOldStoreFinalize(
    alloc: std.mem.Allocator,
    store: *DocStore,
    lower: []const u8,
    upper: []const u8,
) !void {
    const to_delete = try store.scanRange(alloc, lower, upper);
    defer DocStore.freeResults(alloc, to_delete);
    if (to_delete.len == 0) return;

    var delete_keys = std.ArrayListUnmanaged([]const u8).empty;
    defer delete_keys.deinit(alloc);

    for (to_delete) |kv| {
        try delete_keys.append(alloc, kv.key);
    }
    try store.putBatch(&.{}, delete_keys.items);
}

fn streamRangeIntoPreparedDestination(
    alloc: std.mem.Allocator,
    src_store: *DocStore,
    lower: []const u8,
    upper: []const u8,
    dest_store: *DocStore,
    dest_indexes: *IndexManager,
    dense_handoffs: []const DenseSplitHandoff,
    text_handoffs: []const TextSplitHandoff,
    sparse_handoffs: []const SparseSplitHandoff,
) !void {
    const batch_size = 8192;

    var txn = try src_store.env.begin(.{ .read_only = true });
    defer txn.abort();

    var iter = try txn.rangeViewScanner(src_store.dbi, lower);
    defer iter.close();

    var writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
    defer writes.deinit(alloc);

    outer: while (true) {
        const batch = iter.nextViewBatch() catch |err| switch (err) {
            antfly.lmdb.Error.NotFound => break,
            else => return err,
        };

        var index: usize = 0;
        while (index < batch.len()) : (index += 1) {
            const entry = try batch.entryAt(index);
            if (upper.len > 0 and std.mem.order(u8, entry.key, upper) != .lt) break :outer;

            try writes.append(alloc, .{
                .key = entry.key,
                .value = entry.value,
            });
            if (writes.items.len == batch_size) {
                try writeAndIndexBatch(dest_store, dest_indexes, writes.items, dense_handoffs, text_handoffs, sparse_handoffs);
                writes.clearRetainingCapacity();
            }
        }
    }

    if (writes.items.len > 0) try writeAndIndexBatch(dest_store, dest_indexes, writes.items, dense_handoffs, text_handoffs, sparse_handoffs);
}

fn indexPreparedDestinationRange(
    alloc: std.mem.Allocator,
    dest_store: *DocStore,
    dest_indexes: *IndexManager,
    lower: []const u8,
    upper: []const u8,
    dense_handoffs: []const DenseSplitHandoff,
    text_handoffs: []const TextSplitHandoff,
    sparse_handoffs: []const SparseSplitHandoff,
) !void {
    const batch_size = 8192;

    var txn = try dest_store.env.begin(.{ .read_only = true });
    defer txn.abort();

    var iter = try txn.rangeViewScanner(dest_store.dbi, lower);
    defer iter.close();

    var writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
    defer writes.deinit(alloc);

    outer: while (true) {
        const batch = iter.nextViewBatch() catch |err| switch (err) {
            antfly.lmdb.Error.NotFound => break,
            else => return err,
        };

        var index: usize = 0;
        while (index < batch.len()) : (index += 1) {
            const entry = try batch.entryAt(index);
            if (upper.len > 0 and std.mem.order(u8, entry.key, upper) != .lt) break :outer;
            if (isSplitMetadataKey(entry.key)) continue;

            try writes.append(alloc, .{
                .key = entry.key,
                .value = entry.value,
            });
            if (writes.items.len == batch_size) {
                try dest_indexes.indexSplitBatch(dest_store, writes.items, dense_handoffs, text_handoffs, sparse_handoffs);
                writes.clearRetainingCapacity();
            }
        }
    }

    if (writes.items.len > 0) {
        try dest_indexes.indexSplitBatch(dest_store, writes.items, dense_handoffs, text_handoffs, sparse_handoffs);
    }
}

fn writeAndIndexBatch(
    dest_store: *DocStore,
    dest_indexes: *IndexManager,
    writes: []const types.BatchWrite,
    dense_handoffs: []const DenseSplitHandoff,
    text_handoffs: []const TextSplitHandoff,
    sparse_handoffs: []const SparseSplitHandoff,
) !void {
    var txn = try dest_store.env.begin(.{});
    errdefer txn.abort();

    for (writes) |write| {
        try txn.put(dest_store.dbi, write.key, write.value, .{});
    }
    try txn.commit();

    try dest_indexes.indexSplitBatch(dest_store, writes, dense_handoffs, text_handoffs, sparse_handoffs);
}

fn deleteSplitMetadataFromStore(alloc: std.mem.Allocator, store: *DocStore) !void {
    try deletePrefix(alloc, store, "splitstate:");
    try deletePrefix(alloc, store, "splitdelta:");
}

fn deletePrefix(alloc: std.mem.Allocator, store: *DocStore, prefix: []const u8) !void {
    const keys = try store.scanPrefix(alloc, prefix);
    defer DocStore.freeResults(alloc, keys);
    if (keys.len == 0) return;

    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    for (keys) |item| try deletes.append(alloc, item.key);
    try store.putBatch(&.{}, deletes.items);
}

fn isSplitMetadataKey(key: []const u8) bool {
    return std.mem.startsWith(u8, key, "splitstate:") or std.mem.startsWith(u8, key, "splitdelta:");
}

fn splitIndexConfigs() []const types.IndexConfig {
    return &.{
        .{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{\"field\":\"title\"}",
        },
        .{
            .name = "sp_v1",
            .kind = .sparse_vector,
            .config_json = "{\"field\":\"sparse\"}",
        },
    };
}

fn expectPreparedSplitEquivalent(
    alloc: std.mem.Allocator,
    split_key: []const u8,
    old_dest_dir: []const u8,
    current_dest_dir: []const u8,
    expected_docs: usize,
) !void {
    var old_db = try DB.open(alloc, old_dest_dir, .{});
    defer old_db.close();
    var current_db = try DB.open(alloc, current_dest_dir, .{});
    defer current_db.close();

    const old_docs = try old_db.core.store.scanRange(alloc, split_key, "");
    defer antfly.docstore.DocStore.freeResults(alloc, old_docs);
    const current_docs = try current_db.core.store.scanRange(alloc, split_key, "");
    defer antfly.docstore.DocStore.freeResults(alloc, current_docs);

    try std.testing.expectEqual(expected_docs, old_docs.len);
    try std.testing.expectEqual(expected_docs, current_docs.len);

    const sample_doc_index = splitKeyIndex(split_key) + expected_docs - 1;
    const sample_title = try std.fmt.allocPrint(alloc, "title {d:0>8}", .{sample_doc_index});
    defer alloc.free(sample_title);

    var old_result = try old_db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = sample_title } },
    });
    defer old_result.deinit();
    var current_result = try current_db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = sample_title } },
    });
    defer current_result.deinit();

    try std.testing.expect(old_result.total_hits > 0);
    try std.testing.expectEqual(old_result.total_hits, current_result.total_hits);

    const sample_sparse_dim: u32 = @intCast(sample_doc_index % 64);
    var old_sparse = try old_db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{sample_sparse_dim},
            .values = &.{1.0},
            .k = 8,
        } },
        .limit = 8,
    });
    defer old_sparse.deinit();
    var current_sparse = try current_db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{sample_sparse_dim},
            .values = &.{1.0},
            .k = 8,
        } },
        .limit = 8,
    });
    defer current_sparse.deinit();

    try std.testing.expect(old_sparse.total_hits > 0);
    try std.testing.expectEqual(old_sparse.total_hits, current_sparse.total_hits);
}

fn splitKeyIndex(split_key: []const u8) usize {
    const suffix = split_key["doc:".len..];
    return std.fmt.parseUnsigned(usize, suffix, 10) catch unreachable;
}

fn printStats(label: []const u8, stats: DurStats) void {
    var sorted = std.ArrayListUnmanaged(u64).empty;
    defer sorted.deinit(std.heap.page_allocator);
    sorted.appendSlice(std.heap.page_allocator, stats.values.items) catch return;
    std.mem.sort(u64, sorted.items, {}, comptime std.sort.asc(u64));

    const median = percentileSorted(sorted.items, 50);
    const p95 = percentileSorted(sorted.items, 95);
    std.debug.print(
        "{s}: samples={d} avg={d:.3}ms median={d:.3}ms p95={d:.3}ms min={d:.3}ms max={d:.3}ms\n",
        .{
            label,
            stats.values.items.len,
            nsToMs(stats.avg()),
            nsToMs(median),
            nsToMs(p95),
            nsToMs(sorted.items[0]),
            nsToMs(sorted.items[sorted.items.len - 1]),
        },
    );
}

fn percentileSorted(sorted: []const u64, pct: usize) u64 {
    if (sorted.len == 0) return 0;
    if (sorted.len == 1) return sorted[0];
    const max_index = sorted.len - 1;
    const scaled = max_index * pct;
    const index = (scaled + 99) / 100;
    return sorted[@min(index, max_index)];
}

fn formatDocKey(buf: []u8, index: usize) []const u8 {
    return std.fmt.bufPrint(buf, "doc:{d:0>8}", .{index}) catch unreachable;
}

fn benchPath(buf: []u8, label: []const u8, sample_idx: usize) [*:0]const u8 {
    const base = "/tmp/antfly-db-split-bench-";
    const ts = @as(u64, @intCast(nowNs()));
    const path = std.fmt.bufPrint(buf, "{s}{s}-{d}-{d}\x00", .{ base, label, sample_idx, ts }) catch unreachable;
    return @ptrCast(path.ptr);
}

fn cleanupDbDirAt(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

fn elapsedSince(started_ns: i128) u64 {
    const now = nowNs();
    return @intCast(@max(now - started_ns, 0));
}

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / @as(f64, std.time.ns_per_ms);
}

fn nowNs() i128 {
    return @intCast(platform_time.monotonicNs());
}
