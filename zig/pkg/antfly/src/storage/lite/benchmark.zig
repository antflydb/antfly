// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Reproducible storage workloads; wall-clock values are observations, not tests.
const std = @import("std");
const docstore = @import("docstore.zig");
const native = @import("native.zig");
const resource = @import("../resource_manager.zig");
const time = @import("antfly_platform").time;

test "lite throughput benchmark" {
    if (std.c.getenv("ANTFLY_LITE_BENCH") == null) return error.SkipZigTest;
    // Avoid measuring the test allocator's leak-tracking overhead.
    const alloc = std.heap.c_allocator;
    for ([_]usize{ 1000, 4000, 16000 }) |count| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/throughput.aflite", .{tmp.sub_path});
        defer alloc.free(path);
        var store = try docstore.Store.createWithOptions(alloc, path, .{ .no_sync = true, .io = std.testing.io });
        defer store.close();
        const buffers = try alloc.alloc([24]u8, count);
        defer alloc.free(buffers);
        const keys = try alloc.alloc([]const u8, count);
        defer alloc.free(keys);
        for (keys, 0..) |*key, i| key.* = try std.fmt.bufPrint(&buffers[i], "bench-{d:0>8}", .{i});
        var txn = try store.beginWrite();
        var txn_active = true;
        defer if (txn_active) txn.abort();
        const start = time.monotonicNs();
        for (keys) |key| {
            try std.testing.expectError(error.NotFound, txn.get(key));
            try txn.put(key, "small document payload");
        }
        const assembled = time.monotonicNs();
        try txn.commit();
        txn_active = false;
        const committed = time.monotonicNs();
        const values = try alloc.alloc(?[]const u8, count);
        defer alloc.free(values);
        @memset(values, null);
        defer for (values) |value| if (value) |bytes| alloc.free(bytes);
        const before_reads = store.file.test_page_reads.load(.monotonic);
        const reading = time.monotonicNs();
        if (@hasDecl(native.NativeFile, "getDocumentsAtCheckpointAlloc")) {
            try store.file.getDocumentsAtCheckpointAlloc(alloc, store.file.activeCheckpoint(), keys, values);
        } else {
            for (keys, values) |key, *value| value.* = try store.file.getDocumentAlloc(alloc, key);
        }
        const read = time.monotonicNs();
        for (values) |value| try std.testing.expectEqualStrings("small document payload", value.?);
        std.debug.print("LITE_BENCH n={d} assemble_ns={d} commit_ns={d} read_ns={d} logical_reads={d} bytes={d}\n", .{
            count,                                                      assembled - start,                                                                committed - assembled, read - reading,
            store.file.test_page_reads.load(.monotonic) - before_reads, store.file.activeCheckpoint().page_count * @as(u64, store.file.header.page_size),
        });
    }
}

test "lite throughput benchmark vacuum catchup" {
    if (std.c.getenv("ANTFLY_LITE_BENCH") == null) return error.SkipZigTest;
    const a = std.heap.c_allocator;
    for ([_]usize{ 1024, 4096 }) |n| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/catchup.aflite", .{tmp.sub_path});
        defer a.free(path);
        var budgets = resource.Options.defaultBudgets();
        budgets[@intFromEnum(resource.Slice.lite_native_page_cache)] = .{ .soft_limit_bytes = 32768, .hard_limit_bytes = 65536 };
        var manager = resource.ResourceManager.init(.{ .budgets = budgets });
        var file = try native.NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true, .resource_manager = &manager });
        defer file.close();
        try file.putDocument("seed", "value");
        var image = try file.prepareVacuum(null);
        defer image.deinit();
        var capture = native.ChangeCapture{};
        defer capture.deinit(a);
        file.change_capture = &capture;
        defer file.change_capture = null;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const muts = try arena.allocator().alloc(native.DocumentMutation, n);
        for (muts, 0..) |*m, i| m.* = .{ .key = try std.fmt.allocPrint(arena.allocator(), "key-{d:0>8}", .{i}), .value = "small payload" };
        const start = time.monotonicNs();
        try file.putDocumentBatch(muts);
        const written = time.monotonicNs();
        try file.applyCapturedChanges(&image.prepared, &capture, &image.report, null);
        const caught = time.monotonicNs();
        std.debug.print("LITE_BENCH_CATCHUP n={d} source_bytes={d} image_bytes={d} batch_ns={d} catchup_ns={d}\n", .{ n, file.activeCheckpoint().page_count * 4096, image.prepared.activeCheckpoint().page_count * 4096, written - start, caught - written });
        std.debug.print("LITE_BENCH_BUDGET n={d} configured_hard=65536 accounted_bytes={d} prepared_cache_bytes={d} prepared_budget_attached={}\n", .{ n, manager.sliceStats(.lite_native_page_cache).used_bytes, image.prepared.page_cache.total_bytes, image.prepared.page_cache.resource_manager != null });
        try std.testing.expect((try image.prepared.check()).valid);
    }
}

test "lite throughput benchmark packed cursor" {
    if (std.c.getenv("ANTFLY_LITE_BENCH") == null) return error.SkipZigTest;
    const a = std.heap.c_allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/cursor.aflite", .{tmp.sub_path});
    defer a.free(path);
    var store = try docstore.Store.createWithOptions(a, path, .{ .no_sync = true, .io = std.testing.io });
    defer store.close();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const n = 16000;
    const muts = try arena.allocator().alloc(native.DocumentMutation, n);
    const keys = try arena.allocator().alloc([]const u8, n);
    for (muts, keys, 0..) |*m, *key, i| {
        key.* = try std.fmt.allocPrint(arena.allocator(), "key-{d:0>8}", .{i});
        m.* = .{ .key = key.*, .value = "small payload" };
    }
    try store.file.putDocumentBatch(muts);
    for (0..3) |_| {
        var txn = try store.beginRead();
        defer txn.abort();
        var cursor = try txn.openCursor();
        defer cursor.close();
        const start = time.monotonicNs();
        var entry = try cursor.first();
        var count: usize = 0;
        while (true) {
            try std.testing.expectEqualStrings("small payload", entry.value);
            count += 1;
            entry = cursor.next() catch |err| switch (err) {
                error.NotFound => break,
                else => return err,
            };
        }
        const scanned = time.monotonicNs();
        const values = try a.alloc(?[]const u8, n);
        defer a.free(values);
        try txn.getManySorted(keys, values);
        const batched = time.monotonicNs();
        try std.testing.expectEqual(@as(usize, n), count);
        std.debug.print("LITE_BENCH_CURSOR n={d} scan_ns={d} batch_ns={d}\n", .{ n, scanned - start, batched - scanned });
    }
}

test "lite throughput benchmark warm point views" {
    if (std.c.getenv("ANTFLY_LITE_BENCH") == null) return error.SkipZigTest;
    const a = std.heap.c_allocator;
    for ([_]usize{ 16384, 65536 }) |count| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/point.aflite", .{tmp.sub_path});
        defer a.free(path);
        var file = try native.NativeFile.createWithIo(a, std.testing.io, path, .{ .no_sync = true });
        defer file.close();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const batch = try arena.allocator().alloc(native.DocumentMutation, count);
        for (batch, 0..) |*m, i| m.* = .{ .key = try std.fmt.allocPrint(arena.allocator(), "ns\x00key-{d:0>8}", .{i}), .value = "value" };
        try file.putDocumentBatch(batch);
        for (batch) |m| {
            const value = (try file.getDocumentAlloc(a, m.key)).?;
            a.free(value);
        }
        const before = file.test_index_comparisons.load(.monotonic);
        const hits = file.test_index_view_hits.load(.monotonic);
        const started = time.monotonicNs();
        for (0..count) |i| {
            const value = (try file.getDocumentAlloc(a, batch[(i * 4051) % count].key)).?;
            defer a.free(value);
            try std.testing.expectEqualStrings("value", value);
        }
        std.debug.print("LITE_BENCH_POINT n={d} elapsed_ns={d} comparisons={d} view_hits={d} cache_bytes={d}\n", .{ count, time.monotonicNs() - started, file.test_index_comparisons.load(.monotonic) - before, file.test_index_view_hits.load(.monotonic) - hits, file.page_cache.total_bytes });
    }
}
