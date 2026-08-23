// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Harness adapter for production replication lifecycle boundaries. The
//! replication kernel exposes a production-neutral hook; VOPR assigns stable
//! semantic identities and owns suspension policy here.

const std = @import("std");
const vopr = @import("vopr");
const replication = @import("../metadata/replication_backfill.zig");

pub const Hook = struct {
    vopr_io: *vopr.vopr_io.VoprIo,

    pub fn lifecycle(self: *Hook) replication.ReplicationLifecycleHook {
        return .{ .ptr = self, .reach_fn = reach };
    }

    pub fn stableId(event: replication.ReplicationLifecycleEvent) vopr.id.StableId {
        var result = vopr.id.stable("replication-lifecycle.phase", @tagName(event.phase));
        result = vopr.id.derive("replication-lifecycle.table", result, event.table_id);
        result = vopr.id.derive("replication-lifecycle.source", result, event.source_ordinal);
        result = vopr.id.derive("replication-lifecycle.offset", result, event.snapshot_offset);
        result = vopr.id.derive("replication-lifecycle.authority", result, event.authority_id);
        return vopr.id.derive(
            "replication-lifecycle.checkpoint",
            result,
            vopr.id.digest(event.checkpoint),
        );
    }

    fn reach(ptr: *anyopaque, event: replication.ReplicationLifecycleEvent) !void {
        const self: *Hook = @ptrCast(@alignCast(ptr));
        try self.vopr_io.safepoint(stableId(event));
    }
};

test "replication lifecycle adapter records stable VoprIo safepoints" {
    var vopr_io = try vopr.vopr_io.VoprIo.init(.{
        .instrumentation = .{ .enabled = false, .map_digest = 0x52504c },
    });
    defer vopr_io.deinit();
    var adapter = Hook{ .vopr_io = &vopr_io };
    const hook = adapter.lifecycle();

    const applied = replication.ReplicationLifecycleEvent{
        .phase = .snapshot_batch_applied,
        .table_id = 11,
        .source_ordinal = 2,
        .snapshot_offset = 64,
        .authority_id = 9,
        .checkpoint = "snapshot:64",
    };
    const persisted = replication.ReplicationLifecycleEvent{
        .phase = .snapshot_checkpoint_persisted,
        .table_id = 11,
        .source_ordinal = 2,
        .snapshot_offset = 64,
        .authority_id = 9,
        .checkpoint = "snapshot:64",
    };
    try hook.reach(applied);
    try hook.reach(persisted);

    try std.testing.expectEqual(@as(u64, 1), vopr_io.instrumentation.count(Hook.stableId(applied)));
    try std.testing.expectEqual(@as(u64, 1), vopr_io.instrumentation.count(Hook.stableId(persisted)));
    try std.testing.expect(Hook.stableId(applied) != Hook.stableId(persisted));
    try std.testing.expect(Hook.stableId(applied) != Hook.stableId(.{
        .phase = .snapshot_batch_applied,
        .table_id = 11,
        .source_ordinal = 2,
        .snapshot_offset = 65,
        .authority_id = 9,
        .checkpoint = "snapshot:65",
    }));
    try vopr_io.ensureNoCapabilityViolation();
}
