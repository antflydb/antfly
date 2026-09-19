// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Reproducible storage workloads; wall-clock values are observations, not tests.
const std = @import("std");
const docstore = @import("docstore.zig");
const native = @import("native.zig");
const time = @import("antfly_platform").time;

test "lite throughput benchmark" {
    if (std.c.getenv("ANTFLY_LITE_BENCH") == null) return error.SkipZigTest;
    const alloc = std.testing.allocator;
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
