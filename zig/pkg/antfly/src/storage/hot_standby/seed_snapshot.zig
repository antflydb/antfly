// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Physical HA seed capture shared by inline and compiled storage owners.
const std = @import("std");
const Io = std.Io;
const backups_api = @import("../../api/backups.zig");

pub fn capture(alloc: std.mem.Allocator, db: anytype, db_path: []const u8, snapshot_token: []const u8, destination_root: []const u8) !void {
    switch (db.primary_backend) {
        .lmdb, .lsm => {},
        .mem, .lsm_memory => return error.HASeedSnapshotUnsupportedBackend,
    }
    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/{s}", .{ db_path, snapshot_token });
    defer alloc.free(snapshot_root);
    var io_impl = Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    Io.Dir.cwd().deleteTree(io_impl.io(), snapshot_root) catch {};
    defer Io.Dir.cwd().deleteTree(io_impl.io(), snapshot_root) catch {};
    const maintenance_clock = db.backend_runtime.monotonicClock();
    const maintenance_deadline_ns = maintenance_clock.nowRealtimeNs() +| std.time.ns_per_s;
    _ = db.snapshotHASeed(snapshot_token, maintenance_deadline_ns) catch |err| switch (err) {
        error.EnrichmentWaitCanceled,
        error.EnrichmentWaitTimeout,
        error.EnrichmentRetryInProgress,
        => return error.HASeedSnapshotRuntimeBusy,
        else => return err,
    };
    try backups_api.copyDirectoryRecursive(alloc, snapshot_root, destination_root);
}
