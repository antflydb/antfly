// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Opt-in cold LSM benchmark. Compare identical rows and selected logical
//! keys in independently reopened stores; startup navigation is excluded from
//! the counters. Timing is reported, never a correctness threshold.
const std = @import("std");
const DocStore = @import("docstore.zig").DocStore;
const lsm = @import("lsm_backend.zig");
const internal = @import("internal_keys.zig");
const platform = @import("antfly_platform");
const A = std.mem.Allocator;
const row_count = 512;
const selected_count = 32;
const body_bytes = 8192;

fn key(a: A, ordinal: usize) ![]u8 {
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @intCast(ordinal), .little);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(&raw, &digest, .{});
    return internal.documentKeyAlloc(a, &std.fmt.bytesToHex(digest, .lower));
}
fn body(ordinal: usize) [body_bytes]u8 {
    var value: [body_bytes]u8 = undefined;
    const prefix = "{\"body\":\"";
    @memcpy(value[0..prefix.len], prefix);
    var random: u64 = @as(u64, @intCast(ordinal)) + 1;
    for (value[prefix.len .. value.len - 2]) |*byte| {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        byte.* = 'a' + @as(u8, @intCast(random % 26));
    }
    @memcpy(value[value.len - 2 ..], "\"}");
    return value;
}
const Measurement = struct { blocks: u64, bytes: u64, elapsed_ns: u64, point_gets: u64, batches: u64 };
fn measure(a: A, path: []const u8, clustered: bool) !Measurement {
    {
        var backend = try lsm.Backend.open(a, path, .{ .flush_threshold_bytes = 16 * 1024 * 1024 });
        defer backend.close();
        var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
        defer store.close();
        {
            var writer = try store.beginWriteBatch();
            errdefer writer.abort();
            for (0..row_count) |ordinal| {
                const physical = try key(a, ordinal);
                defer a.free(physical);
                const value = body(ordinal);
                try writer.put(physical, &value);
            }
            try writer.commit();
        }
        if (clustered) for (0..row_count / 64) |page| {
            var writer = try store.beginWriteTxn();
            errdefer writer.abort();
            for (page * 64..(page + 1) * 64) |ordinal| {
                const physical = try key(a, ordinal);
                defer a.free(physical);
                _ = try writer.relocatePhysicalRow(physical, @splat(7), @intCast(ordinal));
            }
            try writer.commit();
        };
        try backend.flushBufferedWritesWithOptions(.{ .flush = true, .compact = true });
        try store.sync(true);
    }
    var backend = try lsm.Backend.open(a, path, .{});
    defer backend.close();
    var store = try DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    var selected: [selected_count][]const u8 = undefined;
    var initialized: usize = 0;
    defer for (selected[0..initialized]) |physical| a.free(physical);
    for (&selected, 0..) |*physical, ordinal| {
        physical.* = try key(a, ordinal);
        initialized += 1;
    }
    std.mem.sort([]const u8, &selected, {}, struct {
        fn less(_: void, lhs: []const u8, rhs: []const u8) bool {
            return std.mem.lessThan(u8, lhs, rhs);
        }
    }.less);
    var read = try store.beginReadTxn();
    defer read.abort();
    const before = backend.snapshotReadStats();
    const started = platform.time.monotonicNs();
    var values: [selected_count]?[]u8 = undefined;
    try read.getManySortedOwnedScoped(a, &selected, &values);
    const elapsed = platform.time.monotonicNs() -| started;
    defer for (values) |value| if (value) |bytes| a.free(bytes);
    for (values) |value| try std.testing.expectEqual(@as(usize, body_bytes), (value orelse return error.MissingBenchmarkRow).len);
    const after = backend.snapshotReadStats();
    return .{ .blocks = after.table_block_loads - before.table_block_loads, .bytes = after.table_block_bytes - before.table_block_bytes, .elapsed_ns = @intCast(elapsed), .point_gets = after.point_gets - before.point_gets, .batches = after.get_many_sorted_calls - before.get_many_sorted_calls };
}

test "row heap benchmark cold clustered batches bound body read amplification" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(base);
    const inline_path = try std.fmt.allocPrint(a, "{s}/inline", .{base});
    defer a.free(inline_path);
    const clustered_path = try std.fmt.allocPrint(a, "{s}/clustered", .{base});
    defer a.free(clustered_path);
    const baseline = try measure(a, inline_path, false);
    const clustered = try measure(a, clustered_path, true);
    std.debug.print("cold row heap: rows={d} row_bytes={d} selected={d} inline={any} clustered={any}\n", .{ row_count, body_bytes, selected_count, baseline, clustered });
    try std.testing.expect(baseline.bytes != 0);
    try std.testing.expect(clustered.bytes < baseline.bytes);
}
