// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const backend_types = @import("backend_types.zig");
const mem_backend_mod = @import("mem_backend.zig");
const lsm_backend_mod = @import("lsm_backend/mod.zig");
const storage_sim = @import("sim_runtime.zig");
const segment_mod = @import("../segment.zig");
const inverted = @import("../section/inverted.zig");
const roaring = @import("../encoding/roaring.zig");

const namespaces = [_]backend_types.Namespace{
    .{},
    .{ .name = "docs" },
    .{ .name = "meta" },
};

const keys = [_][]const u8{
    "a",
    "b",
    "c",
    "doc:a",
    "doc:b",
    "doc:c",
    "meta:lsn",
    "meta:epoch",
};

var sim_nonce: std.atomic.Value(u64) = .init(0);

fn nowNs() u64 {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .awake);
    return @intCast(now.toNanoseconds());
}

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const nonce = sim_nonce.fetchAdd(1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-prefix-lsm-sim-{s}-{d}-{d}\x00", .{
        label,
        nowNs(),
        nonce,
    }) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

fn expectNamespaceEqual(
    mem_backend: *mem_backend_mod.Backend,
    lsm_backend: *lsm_backend_mod.Backend,
    namespace: backend_types.Namespace,
) !void {
    var mem_store = try mem_backend.runtimeStore(std.testing.allocator, namespace);
    defer mem_store.deinit();
    var lsm_store = try lsm_backend.runtimeStore(std.testing.allocator, namespace);
    defer lsm_store.deinit();

    var mem_txn = try mem_store.beginRead();
    defer mem_txn.abort();
    var lsm_txn = try lsm_store.beginRead();
    defer lsm_txn.abort();

    for (keys) |key| {
        const mem_value = mem_txn.get(key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        const lsm_value = lsm_txn.get(key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };

        if (mem_value == null or lsm_value == null) {
            try std.testing.expectEqual(mem_value == null, lsm_value == null);
        } else {
            try std.testing.expectEqualStrings(mem_value.?, lsm_value.?);
        }
    }

    var mem_cur = try mem_txn.openCursor();
    defer mem_cur.close();
    var lsm_cur = try lsm_txn.openCursor();
    defer lsm_cur.close();

    var mem_entry = mem_cur.first() catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    var lsm_entry = lsm_cur.first() catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    while (mem_entry != null or lsm_entry != null) {
        try std.testing.expectEqual(mem_entry == null, lsm_entry == null);
        if (mem_entry == null) break;
        try std.testing.expectEqualStrings(mem_entry.?.key, lsm_entry.?.key);
        try std.testing.expectEqualStrings(mem_entry.?.value, lsm_entry.?.value);
        mem_entry = mem_cur.next() catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        lsm_entry = lsm_cur.next() catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
    }
}

const SegmentDoc = struct {
    id: []const u8,
    stored: []const u8,
    hits: []const inverted.InvertedIndexBuilder.TermHit,
};

fn buildTextSegment(alloc: std.mem.Allocator, docs: []const SegmentDoc) ![]u8 {
    var builder = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer builder.deinit();
    for (docs, 0..) |doc, index| {
        try builder.addDocument(@intCast(index), doc.hits);
    }
    const inv_data = try builder.build();
    defer alloc.free(inv_data);

    var writer = segment_mod.SegmentWriter.init(alloc);
    defer writer.deinit();
    const field = try writer.addField("body");
    try writer.addSection(field, .inverted_text, inv_data);
    for (docs) |doc| try writer.addStoredDoc(doc.id, doc.stored);
    return try writer.build();
}

fn expectTermDocIds(
    reader: *segment_mod.SegmentReader,
    term: []const u8,
    expected_doc_ids: []const u32,
) !void {
    var inv_reader = (try reader.invertedIndex("body")) orelse return error.TestExpectedEqual;
    const result = inv_reader.lookup(term) orelse {
        try std.testing.expectEqual(@as(usize, 0), expected_doc_ids.len);
        return;
    };
    try std.testing.expectEqual(@as(u32, @intCast(expected_doc_ids.len)), result.docFreq());
    var iter = try result.iterator(std.testing.allocator);
    defer iter.deinit();
    for (expected_doc_ids) |expected_doc_id| {
        const hit = (try iter.next()) orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(expected_doc_id, hit.doc_id);
    }
    try std.testing.expect(try iter.next() == null);
}

fn putBoth(
    mem_backend: *mem_backend_mod.Backend,
    lsm_backend: *lsm_backend_mod.Backend,
    namespace: backend_types.Namespace,
    key: []const u8,
    value: []const u8,
) !void {
    var mem_txn = try mem_backend.beginWrite();
    defer mem_txn.abort();
    var lsm_txn = try lsm_backend.beginWrite();
    defer lsm_txn.abort();
    try mem_txn.put(namespace, key, value);
    try lsm_txn.put(namespace, key, value);
    try mem_txn.commit();
    try lsm_txn.commit();
}

fn deleteBoth(
    mem_backend: *mem_backend_mod.Backend,
    lsm_backend: *lsm_backend_mod.Backend,
    namespace: backend_types.Namespace,
    key: []const u8,
) !void {
    var mem_txn = try mem_backend.beginWrite();
    defer mem_txn.abort();
    var lsm_txn = try lsm_backend.beginWrite();
    defer lsm_txn.abort();
    try mem_txn.delete(namespace, key);
    try lsm_txn.delete(namespace, key);
    try mem_txn.commit();
    try lsm_txn.commit();
}

fn crashReopenLsm(
    lsm_backend: *lsm_backend_mod.Backend,
    modeled_device: *storage_sim.ModeledDevice,
    root_dir: []const u8,
    options: lsm_backend_mod.Options,
) !void {
    try modeled_device.device().crash();
    lsm_backend.abandonAfterCrash();
    lsm_backend.* = try lsm_backend_mod.Backend.open(std.testing.allocator, root_dir, options);
}

test "lsm backend simulation full durability publishes a newly created root before acknowledged writes" {
    var modeled_device = storage_sim.ModeledDevice.init(std.testing.allocator);
    defer modeled_device.deinit();

    const root_dir = "/lsm-modeled-root-owner/index";
    const open_options = lsm_backend_mod.Options{
        .flush_threshold = 1,
        .storage = modeled_device.storage(),
    };
    var lsm_backend = try lsm_backend_mod.Backend.open(std.testing.allocator, root_dir, open_options);
    defer lsm_backend.close();

    var txn = try lsm_backend.beginWrite();
    defer txn.abort();
    try txn.put(.{}, "durable-key", "durable-value");
    try txn.commit();

    try crashReopenLsm(&lsm_backend, &modeled_device, root_dir, open_options);

    var store = try lsm_backend.runtimeStore(std.testing.allocator, .{});
    defer store.deinit();
    var read_txn = try store.beginRead();
    defer read_txn.abort();
    try std.testing.expectEqualStrings("durable-value", try read_txn.get("durable-key"));
}

fn runCompactionChaosCampaign(seed: u64, steps: usize) !void {
    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();
    const root_dir = "/lsm-modeled-compaction-chaos";
    const options = lsm_backend_mod.Options{
        .flush_threshold = 6,
        .compact_threshold_runs = 2,
        .level_target_runs_base = 2,
        .obsolete_delete_retry_ns = 0,
    };

    var mem_backend = mem_backend_mod.Backend.init(std.testing.allocator, .{});
    defer mem_backend.close();
    var modeled_device = storage_sim.ModeledDevice.init(std.testing.allocator);
    defer modeled_device.deinit();
    var open_options = options;
    open_options.storage = modeled_device.storage();
    var lsm_backend = try lsm_backend_mod.Backend.open(std.testing.allocator, root_dir, open_options);
    defer lsm_backend.close();

    var value_buf: [96]u8 = undefined;
    var compactions: usize = 0;
    var step: usize = 0;
    while (step < steps) : (step += 1) {
        const namespace = namespaces[random.uintLessThan(usize, namespaces.len)];
        const key = keys[random.uintLessThan(usize, keys.len)];
        const op = random.uintLessThan(u8, 100);

        if (op < 72) {
            const value = try std.fmt.bufPrint(&value_buf, "chaos-{x}-{d}-{d}", .{ seed, step, random.int(u16) });
            try putBoth(&mem_backend, &lsm_backend, namespace, key, value);
        } else if (op < 88) {
            try deleteBoth(&mem_backend, &lsm_backend, namespace, key);
        } else if (op < 95) {
            try lsm_backend.sync(true);
        } else {
            try crashReopenLsm(&lsm_backend, &modeled_device, root_dir, open_options);
        }

        compactions += lsm_backend.compaction_stats.compactions;
        for (namespaces) |ns| try expectNamespaceEqual(&mem_backend, &lsm_backend, ns);
    }

    try lsm_backend.sync(true);
    try crashReopenLsm(&lsm_backend, &modeled_device, root_dir, open_options);
    for (namespaces) |ns| try expectNamespaceEqual(&mem_backend, &lsm_backend, ns);
    try std.testing.expect(compactions > 0);
}

test "lsm backend simulation matches memory backend under random operations" {
    var prng = std.Random.DefaultPrng.init(0x5EEDBEEF);
    const random = prng.random();

    var mem_backend = mem_backend_mod.Backend.init(std.testing.allocator, .{});
    defer mem_backend.close();
    var modeled_device = storage_sim.ModeledDevice.init(std.testing.allocator);
    defer modeled_device.deinit();
    var lsm_backend = try lsm_backend_mod.Backend.open(std.testing.allocator, "/lsm-modeled-random-ops", .{
        .flush_threshold = 3,
        .storage = modeled_device.storage(),
    });
    defer lsm_backend.close();

    var value_buf: [64]u8 = undefined;

    var step: usize = 0;
    while (step < 200) : (step += 1) {
        const namespace = namespaces[random.uintLessThan(usize, namespaces.len)];
        const key = keys[random.uintLessThan(usize, keys.len)];
        const op = random.uintLessThan(u8, 3);

        var mem_txn = try mem_backend.beginWrite();
        defer mem_txn.abort();
        var lsm_txn = try lsm_backend.beginWrite();
        defer lsm_txn.abort();

        switch (op) {
            0 => {
                const value = try std.fmt.bufPrint(&value_buf, "v-{d}-{d}", .{ step, op });
                try mem_txn.put(namespace, key, value);
                try lsm_txn.put(namespace, key, value);
            },
            1 => {
                try mem_txn.delete(namespace, key);
                try lsm_txn.delete(namespace, key);
            },
            2 => {
                const mem_value = mem_txn.get(namespace, key) catch |err| switch (err) {
                    error.NotFound => null,
                };
                const lsm_value = lsm_txn.get(namespace, key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                };
                if (mem_value == null or lsm_value == null) {
                    try std.testing.expectEqual(mem_value == null, lsm_value == null);
                } else {
                    try std.testing.expectEqualStrings(mem_value.?, lsm_value.?);
                }
            },
            else => unreachable,
        }

        try mem_txn.commit();
        try lsm_txn.commit();

        for (namespaces) |ns| {
            try expectNamespaceEqual(&mem_backend, &lsm_backend, ns);
        }
    }
}

test "lsm backend simulation matches memory backend across random reopen cycles" {
    var prng = std.Random.DefaultPrng.init(0xC0FFEE42);
    const random = prng.random();

    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "reopen");
    defer cleanupTmp(path);

    var mem_backend = mem_backend_mod.Backend.init(std.testing.allocator, .{});
    defer mem_backend.close();
    var modeled_device = storage_sim.ModeledDevice.init(std.testing.allocator);
    defer modeled_device.deinit();
    var lsm_backend = try lsm_backend_mod.Backend.open(std.testing.allocator, std.mem.span(path), .{
        .flush_threshold = 3,
        .compact_threshold_runs = 3,
        .storage = modeled_device.storage(),
    });
    defer lsm_backend.close();

    var value_buf: [64]u8 = undefined;

    var step: usize = 0;
    while (step < 150) : (step += 1) {
        const namespace = namespaces[random.uintLessThan(usize, namespaces.len)];
        const key = keys[random.uintLessThan(usize, keys.len)];
        const op = random.uintLessThan(u8, 3);

        var mem_txn = try mem_backend.beginWrite();
        defer mem_txn.abort();
        var lsm_txn = try lsm_backend.beginWrite();
        defer lsm_txn.abort();

        switch (op) {
            0 => {
                const value = try std.fmt.bufPrint(&value_buf, "rv-{d}-{d}", .{ step, op });
                try mem_txn.put(namespace, key, value);
                try lsm_txn.put(namespace, key, value);
            },
            1 => {
                try mem_txn.delete(namespace, key);
                try lsm_txn.delete(namespace, key);
            },
            2 => {
                const mem_value = mem_txn.get(namespace, key) catch |err| switch (err) {
                    error.NotFound => null,
                };
                const lsm_value = lsm_txn.get(namespace, key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                };
                if (mem_value == null or lsm_value == null) {
                    try std.testing.expectEqual(mem_value == null, lsm_value == null);
                } else {
                    try std.testing.expectEqualStrings(mem_value.?, lsm_value.?);
                }
            },
            else => unreachable,
        }

        try mem_txn.commit();
        try lsm_txn.commit();

        if (random.uintLessThan(u8, 8) == 0) {
            lsm_backend.close();
            try modeled_device.device().crash();
            lsm_backend = try lsm_backend_mod.Backend.open(std.testing.allocator, std.mem.span(path), .{
                .flush_threshold = 3,
                .compact_threshold_runs = 3,
                .storage = modeled_device.storage(),
            });
        }

        for (namespaces) |ns| {
            try expectNamespaceEqual(&mem_backend, &lsm_backend, ns);
        }
    }
}

test "lsm backend simulation compaction preserves modeled oracle across crash reopen" {
    var prng = std.Random.DefaultPrng.init(0xC0A15CED);
    const random = prng.random();

    var mem_backend = mem_backend_mod.Backend.init(std.testing.allocator, .{});
    defer mem_backend.close();
    var modeled_device = storage_sim.ModeledDevice.init(std.testing.allocator);
    defer modeled_device.deinit();
    var lsm_backend = try lsm_backend_mod.Backend.open(std.testing.allocator, "/lsm-modeled-compaction-vopr", .{
        .flush_threshold = 6,
        .compact_threshold_runs = 4,
        .level_target_runs_base = 2,
        .storage = modeled_device.storage(),
    });
    defer lsm_backend.close();

    var value_buf: [64]u8 = undefined;
    var observed_compactions: usize = 0;

    var step: usize = 0;
    while (step < 120) : (step += 1) {
        const namespace = namespaces[random.uintLessThan(usize, namespaces.len)];
        const key = keys[random.uintLessThan(usize, keys.len)];

        var mem_txn = try mem_backend.beginWrite();
        defer mem_txn.abort();
        var lsm_txn = try lsm_backend.beginWrite();
        defer lsm_txn.abort();

        if (step % 5 == 0) {
            try mem_txn.delete(namespace, key);
            try lsm_txn.delete(namespace, key);
        } else {
            const value = try std.fmt.bufPrint(&value_buf, "compact-v-{d}-{d}", .{ step, random.int(u16) });
            try mem_txn.put(namespace, key, value);
            try lsm_txn.put(namespace, key, value);
        }

        try mem_txn.commit();
        try lsm_txn.commit();

        for (namespaces) |ns| try expectNamespaceEqual(&mem_backend, &lsm_backend, ns);

        if (step != 0 and step % 17 == 0) {
            observed_compactions += lsm_backend.compaction_stats.compactions;
            lsm_backend.close();
            try modeled_device.device().crash();
            lsm_backend = try lsm_backend_mod.Backend.open(std.testing.allocator, "/lsm-modeled-compaction-vopr", .{
                .flush_threshold = 6,
                .compact_threshold_runs = 4,
                .level_target_runs_base = 2,
                .storage = modeled_device.storage(),
            });
            for (namespaces) |ns| try expectNamespaceEqual(&mem_backend, &lsm_backend, ns);
        }
    }

    observed_compactions += lsm_backend.compaction_stats.compactions;
    try std.testing.expect(observed_compactions > 0);
}

test "lsm backend simulation compaction run write fault is retryable" {
    const root_dir = "/lsm-modeled-compaction-write-fault";
    const options = lsm_backend_mod.Options{
        .flush_threshold = 1,
        .bulk_ingest_flush_threshold_multiplier = 1,
        .compact_threshold_runs = 1,
        .foreground_soft_compaction = true,
    };

    var mem_backend = mem_backend_mod.Backend.init(std.testing.allocator, .{});
    defer mem_backend.close();
    var modeled_device = storage_sim.ModeledDevice.init(std.testing.allocator);
    defer modeled_device.deinit();
    var open_options = options;
    open_options.storage = modeled_device.storage();
    var lsm_backend = try lsm_backend_mod.Backend.open(std.testing.allocator, root_dir, open_options);
    defer lsm_backend.close();

    try lsm_backend.beginBulkIngestSession();
    try putBoth(&mem_backend, &lsm_backend, .{ .name = "docs" }, "doc:a", "alpha-v1");
    try putBoth(&mem_backend, &lsm_backend, .{ .name = "docs" }, "doc:b", "beta-v1");

    const compacted_run_needle = try std.fmt.allocPrint(std.testing.allocator, "runs/{d}.tbl", .{lsm_backend.next_run_id});
    defer std.testing.allocator.free(compacted_run_needle);
    try modeled_device.injectWriteFailureForPathContains(compacted_run_needle);
    try std.testing.expectError(error.InjectedWriteFault, lsm_backend.finishBulkIngestSession());
    try std.testing.expect(lsm_backend.bulkIngestActive());
    try expectNamespaceEqual(&mem_backend, &lsm_backend, .{ .name = "docs" });

    // A failed finalization keeps ownership of the bulk-ingest session so the
    // caller can retry the same lifecycle transition (or abort it) without a
    // concurrent maintenance job publishing the intermediate run set.
    try lsm_backend.finishBulkIngestSession();
    try std.testing.expect(!lsm_backend.bulkIngestActive());
    try std.testing.expect(lsm_backend.compaction_stats.compactions > 0);
    try expectNamespaceEqual(&mem_backend, &lsm_backend, .{ .name = "docs" });

    try crashReopenLsm(&lsm_backend, &modeled_device, root_dir, open_options);
    try expectNamespaceEqual(&mem_backend, &lsm_backend, .{ .name = "docs" });
}

test "lsm backend simulation immutable flush write fault keeps wal-backed state visible" {
    const root_dir = "/lsm-modeled-immutable-flush-write-fault";
    const options = lsm_backend_mod.Options{
        .flush_threshold = 1000,
        .flush_threshold_bytes = 128,
        .compact_threshold_runs = 4,
        .wal_sync_on_commit = true,
    };

    var mem_backend = mem_backend_mod.Backend.init(std.testing.allocator, .{});
    defer mem_backend.close();
    var modeled_device = storage_sim.ModeledDevice.init(std.testing.allocator);
    defer modeled_device.deinit();
    var open_options = options;
    open_options.storage = modeled_device.storage();
    var lsm_backend = try lsm_backend_mod.Backend.open(std.testing.allocator, root_dir, open_options);
    defer lsm_backend.close();

    const large_value = [_]u8{'v'} ** 256;
    try putBoth(&mem_backend, &lsm_backend, .{ .name = "docs" }, "doc:a", large_value[0..]);
    try std.testing.expectEqual(@as(usize, 1), lsm_backend.immutable_memtables.items.len);
    try std.testing.expectEqual(@as(usize, 0), lsm_backend.runs.items.len);
    try expectNamespaceEqual(&mem_backend, &lsm_backend, .{ .name = "docs" });

    const run_needle = try std.fmt.allocPrint(std.testing.allocator, "runs/{d}.tbl", .{lsm_backend.next_run_id});
    defer std.testing.allocator.free(run_needle);
    try modeled_device.injectWriteFailureForPathContains(run_needle);
    try std.testing.expectError(error.InjectedWriteFault, lsm_backend.sync(true));
    try std.testing.expectEqual(@as(usize, 1), lsm_backend.immutable_memtables.items.len);
    try expectNamespaceEqual(&mem_backend, &lsm_backend, .{ .name = "docs" });

    try crashReopenLsm(&lsm_backend, &modeled_device, root_dir, open_options);
    try expectNamespaceEqual(&mem_backend, &lsm_backend, .{ .name = "docs" });

    try lsm_backend.sync(true);
    try std.testing.expectEqual(@as(usize, 0), lsm_backend.immutable_memtables.items.len);
    try expectNamespaceEqual(&mem_backend, &lsm_backend, .{ .name = "docs" });
}

test "lsm backend simulation manifest sync fault recovers previous compaction view" {
    const root_dir = "/lsm-modeled-compaction-manifest-fault";
    const options = lsm_backend_mod.Options{
        .flush_threshold = 1,
        .bulk_ingest_flush_threshold_multiplier = 1,
        .compact_threshold_runs = 1,
        .foreground_soft_compaction = true,
    };

    var mem_backend = mem_backend_mod.Backend.init(std.testing.allocator, .{});
    defer mem_backend.close();
    var modeled_device = storage_sim.ModeledDevice.init(std.testing.allocator);
    defer modeled_device.deinit();
    var open_options = options;
    open_options.storage = modeled_device.storage();
    var lsm_backend = try lsm_backend_mod.Backend.open(std.testing.allocator, root_dir, open_options);
    defer lsm_backend.close();

    try lsm_backend.beginBulkIngestSession();
    try putBoth(&mem_backend, &lsm_backend, .{ .name = "docs" }, "doc:a", "alpha-v1");
    try putBoth(&mem_backend, &lsm_backend, .{ .name = "docs" }, "doc:b", "beta-v1");
    try lsm_backend.finishBulkIngestSessionWithOptions(.{ .compact = false });
    try crashReopenLsm(&lsm_backend, &modeled_device, root_dir, open_options);
    try expectNamespaceEqual(&mem_backend, &lsm_backend, .{ .name = "docs" });

    try modeled_device.injectSyncFailureForPathContains("manifest.bin");
    try std.testing.expectError(error.InjectedSyncFault, lsm_backend.sync(true));
    try expectNamespaceEqual(&mem_backend, &lsm_backend, .{ .name = "docs" });

    try crashReopenLsm(&lsm_backend, &modeled_device, root_dir, open_options);
    try expectNamespaceEqual(&mem_backend, &lsm_backend, .{ .name = "docs" });
}

test "lsm backend simulation post-commit wal checkpoint fault is acknowledged and replayable" {
    const root_dir = "/lsm-modeled-committed-wal-checkpoint-fault";
    const options = lsm_backend_mod.Options{
        .flush_threshold = 1,
        .bulk_ingest_flush_threshold_multiplier = 1,
        .compact_threshold_runs = 100,
        .wal_sync_on_commit = true,
        .wal_checkpoint_dirty_bytes_multiplier = 2,
        .wal_checkpoint_dirty_bytes_floor = 128,
        .wal_soft_limit_bytes = 128,
        .foreground_soft_wal_checkpoint = true,
    };

    var modeled_device = storage_sim.ModeledDevice.init(std.testing.allocator);
    defer modeled_device.deinit();
    var open_options = options;
    open_options.storage = modeled_device.storage();
    var lsm_backend = try lsm_backend_mod.Backend.open(std.testing.allocator, root_dir, open_options);
    defer lsm_backend.close();

    try lsm_backend.beginBulkIngestSession();
    try modeled_device.injectSyncFailureForPathContains("manifest.bin");
    const value = [_]u8{'v'} ** 512;
    var txn = try lsm_backend.beginBatchWithOptions(.{
        .mode = .bulk_ingest,
        .defer_commit_flush = true,
    });
    try txn.appendPut(.{ .name = "docs" }, "doc:a", &value);
    // The WAL append and live-state install have committed. A failed manifest
    // sync is retryable maintenance and must not become a false write error.
    try txn.commit();

    const writes = lsm_backend.snapshotWriteStats();
    try std.testing.expectEqual(@as(u64, 1), writes.wal_pressure_failures);
    const maintenance = lsm_backend.snapshotMaintenanceStats();
    try std.testing.expect(maintenance.wal_checkpoint_pending);
    try std.testing.expectEqualSlices(u8, &value, try lsm_backend.getMergedWithMutable(&lsm_backend.mutable, .{ .name = "docs" }, "doc:a"));

    try crashReopenLsm(&lsm_backend, &modeled_device, root_dir, open_options);
    try std.testing.expectEqualSlices(u8, &value, try lsm_backend.getMergedWithMutable(&lsm_backend.mutable, .{ .name = "docs" }, "doc:a"));
}

test "lsm backend simulation hard wal admission fault rejects before append" {
    const root_dir = "/lsm-modeled-hard-wal-admission-fault";
    const options = lsm_backend_mod.Options{
        .flush_threshold_bytes = 1024 * 1024,
        .compact_threshold_runs = 100,
        .wal_sync_on_commit = true,
        .wal_hard_limit_bytes = 900,
    };

    var modeled_device = storage_sim.ModeledDevice.init(std.testing.allocator);
    defer modeled_device.deinit();
    var open_options = options;
    open_options.storage = modeled_device.storage();
    var lsm_backend = try lsm_backend_mod.Backend.open(std.testing.allocator, root_dir, open_options);
    defer lsm_backend.close();

    try lsm_backend.beginBulkIngestSession();
    var value_a: [512]u8 = undefined;
    @memset(&value_a, 'a');
    {
        var txn = try lsm_backend.beginBatchWithOptions(.{
            .mode = .bulk_ingest,
            .defer_commit_flush = true,
        });
        try txn.put(.{ .name = "docs" }, "doc:a", &value_a);
        try txn.commit();
    }

    try modeled_device.injectSyncFailureForPathContains("manifest.bin");
    var value_b: [512]u8 = undefined;
    @memset(&value_b, 'b');
    {
        var txn = try lsm_backend.beginBatchWithOptions(.{
            .mode = .bulk_ingest,
            .defer_commit_flush = true,
        });
        try txn.put(.{ .name = "docs" }, "doc:a", &value_b);
        try std.testing.expectError(error.InjectedSyncFault, txn.commit());
    }

    const writes = lsm_backend.snapshotWriteStats();
    try std.testing.expectEqual(@as(u64, 1), writes.wal_append_records);
    try std.testing.expectEqual(@as(u64, 1), writes.wal_pressure_failures);
    const maintenance = lsm_backend.snapshotMaintenanceStats();
    try std.testing.expect(maintenance.wal_checkpoint_pending);
    try std.testing.expect(maintenance.wal_pressure_blocked);
    try std.testing.expectEqualSlices(u8, &value_a, try lsm_backend.getMergedWithMutable(&lsm_backend.mutable, .{ .name = "docs" }, "doc:a"));

    // The bulk-only maintenance lane retries the failed admission checkpoint
    // without enabling compaction or requiring another client write.
    lsm_backend.makeWalCheckpointRetryDueForTest();
    try std.testing.expect(try lsm_backend.runMaintenanceStep());
    const repaired = lsm_backend.snapshotMaintenanceStats();
    try std.testing.expect(!repaired.wal_checkpoint_pending);
    try std.testing.expect(!repaired.wal_pressure_blocked);
    try std.testing.expectEqual(@as(u64, 0), repaired.wal_retained_bytes);

    try crashReopenLsm(&lsm_backend, &modeled_device, root_dir, open_options);
    try std.testing.expectEqualSlices(u8, &value_a, try lsm_backend.getMergedWithMutable(&lsm_backend.mutable, .{ .name = "docs" }, "doc:a"));
}

test "lsm backend simulation split manifest fault preserves the complete parent view" {
    const root_dir = "/lsm-modeled-split-manifest-fault";
    const options = lsm_backend_mod.Options{
        .flush_threshold = 1,
        .bulk_ingest_flush_threshold_multiplier = 1,
        .compact_threshold_runs = 100,
    };

    var mem_backend = mem_backend_mod.Backend.init(std.testing.allocator, .{});
    defer mem_backend.close();
    var modeled_device = storage_sim.ModeledDevice.init(std.testing.allocator);
    defer modeled_device.deinit();
    var open_options = options;
    open_options.storage = modeled_device.storage();
    var lsm_backend = try lsm_backend_mod.Backend.open(std.testing.allocator, root_dir, open_options);
    defer lsm_backend.close();

    try putBoth(&mem_backend, &lsm_backend, .{ .name = "docs" }, "doc:a", "alpha");
    try putBoth(&mem_backend, &lsm_backend, .{ .name = "docs" }, "doc:z", "omega");
    try lsm_backend.sync(true);
    try expectNamespaceEqual(&mem_backend, &lsm_backend, .{ .name = "docs" });

    try modeled_device.injectSyncFailureForPathContains("manifest.bin");
    try std.testing.expectError(error.InjectedSyncFault, lsm_backend.rewriteLeftInPlace("doc:m"));
    try expectNamespaceEqual(&mem_backend, &lsm_backend, .{ .name = "docs" });

    try crashReopenLsm(&lsm_backend, &modeled_device, root_dir, open_options);
    try expectNamespaceEqual(&mem_backend, &lsm_backend, .{ .name = "docs" });

    // Once the replacement manifest is durable, WAL retirement is cleanup:
    // its failure must preserve the committed split and enter the autonomous
    // checkpoint retry state rather than returning a false split failure.
    try modeled_device.injectSyncFailureForPathContains("/wal/index");
    try std.testing.expect(try lsm_backend.rewriteLeftInPlace("doc:m"));
    const deferred = lsm_backend.snapshotMaintenanceStats();
    try std.testing.expect(deferred.wal_checkpoint_pending);
    try std.testing.expect(!deferred.wal_pressure_blocked);
    try std.testing.expectEqual(@as(u64, 0), deferred.unpublished_wal_logical_bytes);
    try std.testing.expectEqual(@as(u64, 0), deferred.unpublished_wal_max_batch_logical_bytes);
    try std.testing.expectEqual(
        lsm_backend_mod.WalCheckpointRetryReason.checkpoint_failure,
        deferred.wal_checkpoint_retry_reason,
    );
    try modeled_device.injectSyncFailureForPathContains("/wal/index");
    lsm_backend.makeWalCheckpointRetryDueForTest();
    try std.testing.expectError(error.InjectedSyncFault, lsm_backend.runMaintenanceStep());
    const retried_failure = lsm_backend.snapshotMaintenanceStats();
    try std.testing.expect(retried_failure.wal_checkpoint_pending);
    try std.testing.expect(!retried_failure.wal_pressure_blocked);
    try std.testing.expectEqual(@as(u32, 2), retried_failure.wal_checkpoint_retry_attempts);
    lsm_backend.makeWalCheckpointRetryDueForTest();
    try std.testing.expect(try lsm_backend.runMaintenanceStep());
    const repaired = lsm_backend.snapshotMaintenanceStats();
    try std.testing.expect(!repaired.wal_checkpoint_pending);
    try std.testing.expect(!repaired.wal_pressure_blocked);
    try std.testing.expectEqual(@as(u64, 0), repaired.unpublished_wal_logical_bytes);
    try std.testing.expectEqual(@as(u64, 0), repaired.unpublished_wal_max_batch_logical_bytes);
    var read = try lsm_backend.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("alpha", try read.get(.{ .name = "docs" }, "doc:a"));
    try std.testing.expectError(error.NotFound, read.get(.{ .name = "docs" }, "doc:z"));
}

test "lsm backend simulation obsolete run cleanup fault recovers previous manifest" {
    const root_dir = "/lsm-modeled-compaction-cleanup-fault";
    const options = lsm_backend_mod.Options{
        .flush_threshold = 1,
        .bulk_ingest_flush_threshold_multiplier = 1,
        .compact_threshold_runs = 1,
        .foreground_soft_compaction = true,
        .obsolete_retention_ns = 0,
        .obsolete_delete_retry_ns = 0,
    };

    var mem_backend = mem_backend_mod.Backend.init(std.testing.allocator, .{});
    defer mem_backend.close();
    var modeled_device = storage_sim.ModeledDevice.init(std.testing.allocator);
    defer modeled_device.deinit();
    var open_options = options;
    open_options.storage = modeled_device.storage();
    var lsm_backend = try lsm_backend_mod.Backend.open(std.testing.allocator, root_dir, open_options);
    defer lsm_backend.close();

    try lsm_backend.beginBulkIngestSession();
    try putBoth(&mem_backend, &lsm_backend, .{ .name = "docs" }, "doc:a", "alpha-v1");
    try putBoth(&mem_backend, &lsm_backend, .{ .name = "docs" }, "doc:b", "beta-v1");
    try lsm_backend.finishBulkIngestSessionWithOptions(.{ .compact = false });
    try crashReopenLsm(&lsm_backend, &modeled_device, root_dir, open_options);
    try expectNamespaceEqual(&mem_backend, &lsm_backend, .{ .name = "docs" });

    try modeled_device.injectDeleteFailureForPathContains("runs/1.tbl");
    try lsm_backend.sync(true);
    const cleanup_stats = lsm_backend.snapshotMaintenanceStats();
    try std.testing.expect(cleanup_stats.obsolete_delete_failures > 0);
    try std.testing.expect(cleanup_stats.obsolete_delete_retries > 0);
    try expectNamespaceEqual(&mem_backend, &lsm_backend, .{ .name = "docs" });

    try crashReopenLsm(&lsm_backend, &modeled_device, root_dir, open_options);
    try expectNamespaceEqual(&mem_backend, &lsm_backend, .{ .name = "docs" });

    try lsm_backend.sync(true);
    try expectNamespaceEqual(&mem_backend, &lsm_backend, .{ .name = "docs" });
}

test "lsm backend simulation full-text segment compaction drops stale and deleted docs" {
    const alloc = std.testing.allocator;

    const seg1_docs = [_]SegmentDoc{
        .{
            .id = "doc:a",
            .stored = "{\"body\":\"old alpha stale\"}",
            .hits = &.{
                .{ .term = "alpha", .freq = 1, .norm = 3 },
                .{ .term = "stale", .freq = 1, .norm = 3 },
            },
        },
        .{
            .id = "doc:b",
            .stored = "{\"body\":\"beta shared\"}",
            .hits = &.{
                .{ .term = "beta", .freq = 1, .norm = 2 },
                .{ .term = "shared", .freq = 1, .norm = 2 },
            },
        },
    };
    const seg1 = try buildTextSegment(alloc, &seg1_docs);
    defer alloc.free(seg1);

    const seg2_docs = [_]SegmentDoc{
        .{
            .id = "doc:a",
            .stored = "{\"body\":\"fresh alpha shared\"}",
            .hits = &.{
                .{ .term = "fresh", .freq = 1, .norm = 3 },
                .{ .term = "alpha", .freq = 1, .norm = 3 },
                .{ .term = "shared", .freq = 1, .norm = 3 },
            },
        },
        .{
            .id = "doc:c",
            .stored = "{\"body\":\"gamma\"}",
            .hits = &.{.{ .term = "gamma", .freq = 1, .norm = 1 }},
        },
    };
    const seg2 = try buildTextSegment(alloc, &seg2_docs);
    defer alloc.free(seg2);

    const seg3_docs = [_]SegmentDoc{.{
        .id = "doc:d",
        .stored = "{\"body\":\"deleted tombstone\"}",
        .hits = &.{.{ .term = "tombstone", .freq = 1, .norm = 2 }},
    }};
    const seg3 = try buildTextSegment(alloc, &seg3_docs);
    defer alloc.free(seg3);

    var reader1 = try segment_mod.SegmentReader.init(alloc, seg1);
    defer reader1.deinit();
    var reader2 = try segment_mod.SegmentReader.init(alloc, seg2);
    defer reader2.deinit();
    var reader3 = try segment_mod.SegmentReader.init(alloc, seg3);
    defer reader3.deinit();

    var deleted1 = roaring.RoaringBitmap.init(alloc);
    defer deleted1.deinit();
    try deleted1.add(0);
    var deleted3 = roaring.RoaringBitmap.init(alloc);
    defer deleted3.deinit();
    try deleted3.add(0);

    const inputs = [_]segment_mod.MergeInput{
        .{ .reader = &reader1, .deleted = deleted1 },
        .{ .reader = &reader2 },
        .{ .reader = &reader3, .deleted = deleted3 },
    };
    const merged = try segment_mod.mergeSegmentInputs(alloc, &inputs);
    defer alloc.free(merged);

    var merged_reader = try segment_mod.SegmentReader.init(alloc, merged);
    defer merged_reader.deinit();

    try std.testing.expectEqual(@as(u32, 3), merged_reader.doc_count);
    try std.testing.expectEqualStrings("doc:b", ((try merged_reader.storedDoc(0)) orelse return error.TestExpectedEqual).id);
    try std.testing.expectEqualStrings("doc:a", ((try merged_reader.storedDoc(1)) orelse return error.TestExpectedEqual).id);
    try std.testing.expectEqualStrings("doc:c", ((try merged_reader.storedDoc(2)) orelse return error.TestExpectedEqual).id);

    try expectTermDocIds(&merged_reader, "stale", &.{});
    try expectTermDocIds(&merged_reader, "tombstone", &.{});
    try expectTermDocIds(&merged_reader, "beta", &.{0});
    try expectTermDocIds(&merged_reader, "fresh", &.{1});
    try expectTermDocIds(&merged_reader, "alpha", &.{1});
    try expectTermDocIds(&merged_reader, "shared", &.{ 0, 1 });
    try expectTermDocIds(&merged_reader, "gamma", &.{2});
}

test "lsm backend compaction chaos campaign preserves modeled oracle" {
    try runCompactionChaosCampaign(0xC04FAC10, 180);
    try runCompactionChaosCampaign(0xC04FAC11, 180);
    try runCompactionChaosCampaign(0xC04FAC12, 180);
}
