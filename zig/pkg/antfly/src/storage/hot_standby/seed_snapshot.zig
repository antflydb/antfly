// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Physical HA seed capture shared by inline and compiled storage owners.
const std = @import("std");
const Io = std.Io;
const backups_api = @import("../../api/local_backups.zig");

pub fn capture(alloc: std.mem.Allocator, db: anytype, db_path: []const u8, snapshot_token: []const u8, destination_root: []const u8) !void {
    switch (db.primary_backend) {
        .lsm => {},
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
