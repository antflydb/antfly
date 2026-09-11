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
const Backend = @import("../lsm_backend.zig").Backend;
const runtime = @import("runtime.zig");
const storage_io = @import("storage_io.zig");
const Cache = @import("cache.zig").Cache;
const clock = @import("antfly_platform").time;

fn write(backend: *Backend, value: []const u8) !void {
    var txn = try backend.beginWrite();
    errdefer txn.abort();
    try txn.put(.{ .name = "docs" }, "a", value);
    try txn.put(.{ .name = "docs" }, "b", value);
    try txn.commit();
}

test "current writer directory reads pin one tip and own values across publication" {
    const Hook = struct {
        var calls: usize = 0;
        fn publish(raw: *anyopaque) !void {
            runtime.test_current_point_unlocked_hook = null;
            const backend: *Backend = @ptrCast(@alignCast(raw));
            try std.testing.expect(backend.mu.tryLock());
            backend.mu.unlock();
            calls += 1;
            try write(backend, "new");
        }
    };
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |cached| {
        var storage = storage_io.MemoryStorage.init(alloc);
        defer storage.deinit();
        var cache = Cache.init(alloc, 1024 * 1024);
        defer cache.deinit();
        var backend = try Backend.open(alloc, "/current-writer-directory", .{ .storage = storage.storage(), .cache = if (cached) &cache else null, .flush_threshold = 1, .compact_threshold_runs = 10000 });
        defer backend.close();
        try write(&backend, "old");
        var txn = try backend.beginWrite();
        defer txn.abort();
        Hook.calls = 0;
        runtime.test_current_point_unlocked_hook = Hook.publish;
        defer runtime.test_current_point_unlocked_hook = null;
        const old = try txn.get(.{ .name = "docs" }, "a");
        try std.testing.expectEqualStrings("old", old);
        try std.testing.expectEqual(@as(usize, 1), Hook.calls);
        try std.testing.expectEqualStrings("new", try txn.get(.{ .name = "docs" }, "a"));
        try std.testing.expectEqualStrings("old", old);
        try txn.put(.{ .name = "docs" }, "a", "overlay");
        try std.testing.expectEqualStrings("overlay", try txn.get(.{ .name = "docs" }, "a"));
        try txn.delete(.{ .name = "docs" }, "a");
        try std.testing.expectError(error.NotFound, txn.get(.{ .name = "docs" }, "a"));
        try std.testing.expectError(error.NotFound, txn.get(.{ .name = "other" }, "b"));
        var deletion = try backend.beginWrite();
        errdefer deletion.abort();
        try deletion.delete(.{ .name = "docs" }, "b");
        try deletion.commit();
        try std.testing.expectError(error.NotFound, txn.get(.{ .name = "docs" }, "b"));
        const wide: [8192]u8 = @splat('x');
        try write(&backend, &wide);
        const owned_before = txn.held_values.items.len;
        try std.testing.expectEqualStrings(&wide, try txn.get(.{ .name = "docs" }, "b"));
        // Reuse an already owned decoded block instead of copying a wide row
        // again; cache borrows need exactly one transaction-owned allocation.
        try std.testing.expectEqual(owned_before + 1, txn.held_values.items.len);
        try std.testing.expectEqual(@as(u64, 0), backend.snapshotReadStats().run_group_builds);
    }
}

test "current writer directory batches keep a coherent tip during concurrent flush" {
    const Hook = struct {
        fn publish(raw: *anyopaque) !void {
            runtime.test_current_point_unlocked_hook = null;
            const backend: *Backend = @ptrCast(@alignCast(raw));
            try std.testing.expect(backend.mu.tryLock());
            const pinned = backend.hasVersionReaderPins();
            backend.mu.unlock();
            try std.testing.expect(pinned);
            try write(backend, "new");
        }
    };
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |cached| {
        var storage = storage_io.MemoryStorage.init(alloc);
        defer storage.deinit();
        var cache = Cache.init(alloc, 1024 * 1024);
        defer cache.deinit();
        var backend = try Backend.open(alloc, "/current-writer-batch", .{ .storage = storage.storage(), .cache = if (cached) &cache else null, .flush_threshold = 1, .compact_threshold_runs = 10000 });
        defer backend.close();
        try write(&backend, "old");
        backend.options.flush_threshold = 1000;
        {
            var mutable = try backend.beginWrite();
            errdefer mutable.abort();
            try mutable.put(.{ .name = "docs" }, "a", "old");
            try mutable.commit();
        }
        try std.testing.expect(backend.mutable.entryCount() != 0);
        backend.options.flush_threshold = 1;
        var txn = try runtime.BoundWriteTxn(Backend).open(&backend, .{ .name = "docs" });
        defer txn.abort();
        const keys = [_][]const u8{ "a", "b", "missing" };
        var values: [3]?[]const u8 = undefined;
        runtime.test_current_point_unlocked_hook = Hook.publish;
        defer runtime.test_current_point_unlocked_hook = null;
        try txn.getManySorted(&keys, &values);
        try std.testing.expectEqualStrings("old", values[0].?);
        try std.testing.expectEqualStrings("old", values[1].?);
        try std.testing.expect(values[2] == null);
        try std.testing.expectEqualStrings("new", try txn.get("a"));
        try std.testing.expectEqualStrings("old", values[0].?);
        try txn.put("a", "overlay");
        try txn.getManySorted(&keys, &values);
        try std.testing.expectEqualStrings("overlay", values[0].?);
        try std.testing.expectEqualStrings("new", values[1].?);
    }
}

test "current writer directory reads restore lock and ownership on cancellation" {
    const Hook = struct {
        fn cancel(_: *anyopaque) !void {
            return error.Canceled;
        }
    };
    const alloc = std.testing.allocator;
    var storage = storage_io.MemoryStorage.init(alloc);
    defer storage.deinit();
    var backend = try Backend.open(alloc, "/current-writer-cancel", .{ .storage = storage.storage(), .flush_threshold = 1 });
    defer backend.close();
    try write(&backend, "old");
    var txn = try backend.beginWrite();
    defer txn.abort();
    runtime.test_current_point_unlocked_hook = Hook.cancel;
    defer runtime.test_current_point_unlocked_hook = null;
    try std.testing.expectError(error.Canceled, txn.get(.{ .name = "docs" }, "a"));
    runtime.test_current_point_unlocked_hook = null;
    try std.testing.expectEqualStrings("old", try txn.get(.{ .name = "docs" }, "a"));
    var bound = try runtime.BoundWriteTxn(Backend).open(&backend, .{ .name = "docs" });
    defer bound.abort();
    const keys = [_][]const u8{ "a", "b" };
    var values: [2]?[]const u8 = undefined;
    runtime.test_current_point_unlocked_hook = Hook.cancel;
    try std.testing.expectError(error.Canceled, bound.getManySorted(&keys, &values));
    runtime.test_current_point_unlocked_hook = null;
    try bound.getManySorted(&keys, &values);
    try std.testing.expectEqualStrings("old", values[0].?);
    try std.testing.expectEqualStrings("old", values[1].?);
}

test "current writer directory wide batches keep only one owned allocation per result" {
    const alloc = std.testing.allocator;
    for ([_]usize{ 0, 1, 16 }) |concurrency| {
        var storage = storage_io.MemoryStorage.init(alloc);
        defer storage.deinit();
        var cache = Cache.init(alloc, 1024 * 1024);
        defer cache.deinit();
        var backend = try Backend.open(alloc, "/current-writer-wide-batch", .{
            .storage = storage.storage(),
            .cache = if (concurrency == 0) null else &cache,
            .max_concurrent_point_block_reads = concurrency,
            .flush_threshold = 1,
        });
        defer backend.close();
        const wide: [8192]u8 = @splat('x');
        try write(&backend, &wide);
        var txn = try runtime.BoundWriteTxn(Backend).open(&backend, .{ .name = "docs" });
        defer txn.abort();
        const keys = [_][]const u8{ "a", "b" };
        var values: [2]?[]const u8 = undefined;
        // Check warm-cache and fresh-cache reads; no decoded block should be
        // retained alongside a redundant second allocation of its row value.
        for (0..2) |_| {
            const before = txn.held_values.items.len;
            try txn.getManySorted(&keys, &values);
            try std.testing.expectEqual(before + 2, txn.held_values.items.len);
            try std.testing.expectEqualStrings(&wide, values[0].?);
            try std.testing.expectEqualStrings(&wide, values[1].?);
        }
    }
}

test "current writer directory owned batch retention benchmark" {
    if (@import("builtin").mode != .ReleaseFast) return error.SkipZigTest;
    const alloc = std.heap.smp_allocator;
    var storage = storage_io.MemoryStorage.init(alloc);
    defer storage.deinit();
    var backend = try Backend.open(alloc, "/owned-batch-benchmark", .{ .storage = storage.storage(), .flush_threshold = 1 });
    defer backend.close();
    const count = 32;
    var raw_keys: [count][8]u8 = undefined;
    var keys: [count][]const u8 = undefined;
    var write_txn = try backend.beginWrite();
    errdefer write_txn.abort();
    const wide: [8192]u8 = @splat('x');
    for (&raw_keys, &keys, 0..) |*raw, *key, i| {
        std.mem.writeInt(u64, raw, i, .big);
        key.* = raw;
        try write_txn.put(.{}, key.*, &wide);
    }
    try write_txn.commit();
    defer runtime.test_duplicate_owned_point_results = false;
    for ([_]bool{ true, false }) |duplicate| {
        runtime.test_duplicate_owned_point_results = duplicate;
        var samples: [7]u64 = undefined;
        var retained_bytes: usize = 0;
        var retained_allocations: usize = 0;
        for (0..8) |sample| {
            const start = clock.monotonicNs();
            for (0..32) |_| {
                var txn = try runtime.BoundWriteTxn(Backend).open(&backend, .{});
                defer txn.abort();
                var values: [count]?[]const u8 = undefined;
                try txn.getManySorted(&keys, &values);
                retained_bytes = 0;
                retained_allocations = txn.held_values.items.len;
                for (txn.held_values.items) |value| retained_bytes += value.len;
                for (values) |value| try std.testing.expectEqualStrings(&wide, value.?);
            }
            if (sample != 0) samples[sample - 1] = (clock.monotonicNs() - start) / 32;
        }
        std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
        std.debug.print("owned-batch duplicate={} rows={d} value_bytes={d} median_batch_ns={d} retained_allocations={d} retained_bytes={d}\n", .{ duplicate, count, wide.len, samples[3], retained_allocations, retained_bytes });
    }
}

test "current writer directory point scaling benchmark" {
    if (@import("builtin").mode != .ReleaseFast) return error.SkipZigTest;
    const alloc = std.heap.smp_allocator;
    for ([_]usize{ 1000, 10000, 100000 }) |count| {
        const keys = try alloc.alloc(u8, count * 8);
        defer alloc.free(keys);
        var backend = Backend.init(alloc, .{ .wal_enabled = false });
        defer backend.close();
        for (0..count) |i| {
            const key = keys[i * 8 ..][0..8];
            std.mem.writeInt(u64, key, i * 2, .big);
            try backend.runs.append(alloc, .{ .id = i + 1, .level = 1, .size_bytes = 1, .path = @constCast("unused-benchmark.sst"), .smallest_namespace_name = null, .smallest_key = key, .largest_namespace_name = null, .largest_key = key, .entry_count = 1, .tombstone_count = 0, .bloom_filter = null, .owns_metadata = false, .owns_path = false, .state = null });
        }
        _ = try backend.planningDirectory();
        var txn = try backend.beginWrite();
        defer txn.abort();
        var query: [8]u8 = undefined;
        // A gap inside the level (not outside its overall bounds) forces an
        // indexed seek while doing no SST I/O in either implementation.
        std.mem.writeInt(u64, &query, count - 1, .big);
        var ns: [2]u64 = undefined;
        for (0..2) |variant| {
            runtime.test_current_point_rank_walk = variant == 0;
            defer runtime.test_current_point_rank_walk = false;
            try std.testing.expectError(error.NotFound, txn.get(.{}, &query));
            var samples: [7]u64 = undefined;
            for (&samples) |*sample| {
                const start = clock.monotonicNs();
                for (0..100) |_| try std.testing.expectError(error.NotFound, txn.get(.{}, &query));
                sample.* = (clock.monotonicNs() - start) / 100;
            }
            std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
            ns[variant] = samples[samples.len / 2];
        }
        try std.testing.expectEqual(@as(u64, 0), backend.snapshotReadStats().run_group_builds);
        std.debug.print("current-writer-point runs={d} rank_walk_ns={d} indexed_ns={d}\n", .{ count, ns[0], ns[1] });
    }
}

test "current writer directory overflow candidates unwind every allocation failure" {
    const Fixture = struct {
        fn check(alloc: std.mem.Allocator) !void {
            var backend = Backend.init(alloc, .{});
            defer backend.close();
            for (0..17) |i| {
                var state: @import("state.zig").State = .{};
                errdefer state.deinit(alloc);
                try state.appendUpsert(alloc, .{}, "a", "value", false);
                try backend.runs.append(alloc, .{ .id = i + 1, .level = 0, .size_bytes = 1, .path = null, .smallest_namespace_name = null, .smallest_key = @constCast("a"), .largest_namespace_name = null, .largest_key = @constCast("a"), .entry_count = 1, .tombstone_count = 0, .bloom_filter = null, .owns_metadata = false, .owns_path = false, .state = state });
            }
            var txn = try backend.beginWrite();
            defer txn.abort();
            try std.testing.expectEqualStrings("value", try txn.get(.{}, "a"));
            try std.testing.expectEqual(@as(usize, 1), txn.held_values.items.len);
            try std.testing.expectEqual(@as(u64, 0), backend.snapshotReadStats().run_group_builds);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.check, .{});
}
