// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! HA crash-hardening scenarios.
//!
//! These tests intentionally compose the storage-facing HA primitives across
//! close/reopen boundaries. They are the local stand-in for later process-kill
//! and network-partition chaos tests in the operator/e2e layer.

const std = @import("std");
const Allocator = std.mem.Allocator;
const backup_manifest = @import("backup_manifest.zig");
const bootstrap = @import("bootstrap.zig");
const fencing = @import("fencing.zig");
const primary_mod = @import("primary.zig");
const rejoin = @import("rejoin.zig");
const replication_record = @import("replication_record.zig");
const session = @import("session.zig");
const standby_mod = @import("standby.zig");

var test_path_counter: u64 = 0;

const TestPaths = struct {
    primary_log: [:0]u8,
    primary_slots: [:0]u8,
    standby_log: [:0]u8,
    standby_progress: [:0]u8,
    fence_wal: [:0]u8,

    fn deinit(self: TestPaths, alloc: Allocator) void {
        alloc.free(self.primary_log);
        alloc.free(self.primary_slots);
        alloc.free(self.standby_log);
        alloc.free(self.standby_progress);
        alloc.free(self.fence_wal);
    }
};

fn testPaths(alloc: Allocator, comptime name: []const u8) !TestPaths {
    const nonce = @atomicRmw(u64, &test_path_counter, .Add, 1, .seq_cst);
    const primary_log = try allocPrintPath(alloc, name, "primary-log", nonce);
    defer alloc.free(primary_log);
    const primary_slots = try allocPrintPath(alloc, name, "primary-slots", nonce);
    defer alloc.free(primary_slots);
    const standby_log = try allocPrintPath(alloc, name, "standby-log", nonce);
    defer alloc.free(standby_log);
    const standby_progress = try allocPrintPath(alloc, name, "standby-progress", nonce);
    defer alloc.free(standby_progress);
    const fence_wal = try allocPrintPath(alloc, name, "fence-wal", nonce);
    defer alloc.free(fence_wal);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_slots) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_progress) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), fence_wal) catch {};

    return .{
        .primary_log = try alloc.dupeSentinel(u8, primary_log, 0),
        .primary_slots = try alloc.dupeSentinel(u8, primary_slots, 0),
        .standby_log = try alloc.dupeSentinel(u8, standby_log, 0),
        .standby_progress = try alloc.dupeSentinel(u8, standby_progress, 0),
        .fence_wal = try alloc.dupeSentinel(u8, fence_wal, 0),
    };
}

fn allocPrintPath(alloc: Allocator, comptime name: []const u8, comptime part: []const u8, nonce: u64) ![]u8 {
    return try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/ha-chaos-" ++ name ++ "-" ++ part ++ "-{d}-{d}",
        .{ std.testing.random_seed, nonce },
    );
}

fn testIdentity() standby_mod.Identity {
    return .{
        .cluster_id = 100,
        .shard_id = 10,
        .table_id = 20,
        .timeline_id = 1,
        .epoch = 1,
    };
}

fn baseRecord(identity: standby_mod.Identity, lsn: u64, payload: []const u8) replication_record.Record {
    return .{
        .kind = .batch_mutation,
        .payload_codec = .raw,
        .cluster_id = identity.cluster_id,
        .shard_id = identity.shard_id,
        .table_id = identity.table_id,
        .timeline_id = identity.timeline_id,
        .epoch = identity.epoch,
        .lsn = lsn,
        .previous_lsn = lsn - 1,
        .payload = payload,
    };
}

fn timelineSwitchRecord(identity: standby_mod.Identity, lsn: u64, previous_lsn: u64) replication_record.Record {
    return .{
        .kind = .timeline_switch,
        .payload_codec = .raw,
        .cluster_id = identity.cluster_id,
        .shard_id = identity.shard_id,
        .table_id = identity.table_id,
        .timeline_id = identity.timeline_id,
        .epoch = identity.epoch,
        .lsn = lsn,
        .previous_lsn = previous_lsn,
        .payload = "timeline-switch",
    };
}

fn testFiles() [2]backup_manifest.FileEntry {
    return .{
        .{ .path = "manifest", .kind = .manifest, .size_bytes = 8, .crc32 = backup_manifest.crc32("manifest") },
        .{ .path = "sst/0001", .kind = .sstable, .size_bytes = 7, .crc32 = backup_manifest.crc32("sstable") },
    };
}

fn testContents() [2]backup_manifest.FileContent {
    return .{
        .{ .path = "manifest", .bytes = "manifest" },
        .{ .path = "sst/0001", .bytes = "sstable" },
    };
}

const ApplyCapture = struct {
    alloc: Allocator,
    payloads: std.ArrayListUnmanaged([]u8) = .empty,
    fail_at_lsn: u64 = 0,

    fn deinit(self: *ApplyCapture) void {
        for (self.payloads.items) |payload| self.alloc.free(payload);
        self.payloads.deinit(self.alloc);
        self.* = undefined;
    }

    fn apply(ctx: *anyopaque, record: replication_record.RecordView) !void {
        const self: *ApplyCapture = @ptrCast(@alignCast(ctx));
        if (record.lsn == self.fail_at_lsn) return error.IntentionalApplyFailure;
        const owned = try self.alloc.dupe(u8, record.payload);
        errdefer self.alloc.free(owned);
        try self.payloads.append(self.alloc, owned);
    }
};

test "storage.ha chaos crash during base backup preserves slot pin and catch-up boundary" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "base-backup");
    defer paths.deinit(alloc);
    const identity = testIdentity();
    const files = testFiles();
    const contents = testContents();

    {
        var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
        defer primary.close();
        const started = try primary.beginBaseBackup(.{
            .slot_name = "standby-a",
            .manifest_id = "base-0001",
        });
        try std.testing.expectEqual(@as(u64, 1), started.backup_lsn);
        _ = try primary.append(.{ .payload = "during-copy" });
    }

    {
        var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
        defer primary.close();
        const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(@as(u64, 1), slot.restart_lsn);
        try std.testing.expectEqual(@as(u64, 0), slot.received_lsn);

        const ended = try primary.endBaseBackup(.{
            .identity = identity,
            .manifest_id = "base-0001",
            .backup_lsn = 1,
            .checkpoint_lsn = 2,
            .files = &files,
        });
        try std.testing.expectEqual(@as(u64, 3), ended.end_record_lsn);
    }

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();

    const manifest = backup_manifest.ManifestView{
        .identity = identity,
        .manifest_id = "base-0001",
        .backup_lsn = 1,
        .checkpoint_lsn = 2,
        .files = &files,
        .flags = 0,
    };
    _ = try bootstrap.bootstrapFromManifest(alloc, &standby, manifest, &contents);
    try std.testing.expectEqual(@as(u64, 3), standby.nextReceiveLsn());
    try primary.activateSeededSlot("standby-a", identity.timeline_id, 2, 2, 2);

    var capture = ApplyCapture{ .alloc = alloc };
    defer capture.deinit();
    const result = try session.replicateAvailable(alloc, &primary, "standby-a", &standby, &capture, ApplyCapture.apply);
    try std.testing.expectEqual(@as(usize, 1), result.received_count);
    try std.testing.expectEqual(@as(usize, 1), result.applied_count);
    try std.testing.expectEqual(@as(u64, 3), result.progress.applied_lsn);
}

test "storage.ha chaos crash after receive replays durable WAL before streaming resumes" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "receive-crash");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    {
        var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
        defer standby.close();
        try std.testing.expectEqual(@as(u64, 1), try standby.receive(baseRecord(identity, 1, "one")));
    }

    {
        var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
        defer standby.close();
        try std.testing.expectEqual(@as(u64, 1), standby.currentProgress().received_lsn);
        try std.testing.expectEqual(@as(u64, 0), standby.currentProgress().applied_lsn);

        var capture = ApplyCapture{ .alloc = alloc };
        defer capture.deinit();
        try std.testing.expectEqual(@as(usize, 1), try standby.applyAvailable(&capture, ApplyCapture.apply));
        try std.testing.expectEqualStrings("one", capture.payloads.items[0]);
        try std.testing.expectEqual(@as(u64, 2), standby.nextReceiveLsn());
    }
}

test "storage.ha chaos rejects noncontiguous records and follows timeline switch across restart" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "ordering-timeline");
    defer paths.deinit(alloc);
    const identity = testIdentity();
    const promoted_identity = standby_mod.Identity{
        .cluster_id = identity.cluster_id,
        .shard_id = identity.shard_id,
        .table_id = identity.table_id,
        .timeline_id = 2,
        .epoch = 2,
    };

    {
        var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
        defer standby.close();
        try std.testing.expectEqual(@as(u64, 1), try standby.receive(baseRecord(identity, 1, "one")));
        try std.testing.expectError(error.RecordAlreadyReceived, standby.receive(baseRecord(identity, 1, "duplicate")));
        try std.testing.expectError(error.UnexpectedRecordLsn, standby.receive(baseRecord(identity, 3, "gap")));

        var wrong_previous = baseRecord(identity, 2, "wrong-previous");
        wrong_previous.previous_lsn = 0;
        try std.testing.expectError(error.UnexpectedPreviousLsn, standby.receive(wrong_previous));

        var future_timeline = baseRecord(promoted_identity, 2, "future-timeline-data");
        future_timeline.previous_lsn = 1;
        try std.testing.expectError(error.WrongTimeline, standby.receive(future_timeline));

        var capture = ApplyCapture{ .alloc = alloc };
        defer capture.deinit();
        try std.testing.expectEqual(@as(usize, 1), try standby.applyAvailable(&capture, ApplyCapture.apply));
        try std.testing.expectEqualStrings("one", capture.payloads.items[0]);

        try std.testing.expectEqual(@as(u64, 2), try standby.receive(timelineSwitchRecord(promoted_identity, 2, 1)));
        try std.testing.expectEqual(@as(u64, 2), standby.identitySnapshot().timeline_id);
    }

    {
        var reopened = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
        defer reopened.close();
        try std.testing.expectEqual(@as(u64, 2), reopened.identitySnapshot().timeline_id);
        try std.testing.expectEqual(@as(u64, 2), reopened.currentProgress().applied_lsn);
        try std.testing.expectError(error.WrongTimeline, reopened.receive(baseRecord(identity, 3, "old-timeline")));

        try std.testing.expectEqual(@as(u64, 3), try reopened.receive(baseRecord(promoted_identity, 3, "new-timeline")));
        var capture = ApplyCapture{ .alloc = alloc };
        defer capture.deinit();
        try std.testing.expectEqual(@as(usize, 1), try reopened.applyAvailable(&capture, ApplyCapture.apply));
        try std.testing.expectEqualStrings("new-timeline", capture.payloads.items[0]);
    }
}

test "storage.ha chaos rejects out-of-order WAL without poisoning receive cursor" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "out-of-order");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();

    try std.testing.expectError(error.UnexpectedRecordLsn, standby.receive(baseRecord(identity, 2, "two-before-one")));
    try std.testing.expectEqual(@as(u64, 0), standby.currentProgress().received_lsn);
    try std.testing.expectEqual(@as(u64, 1), standby.nextReceiveLsn());

    try std.testing.expectEqual(@as(u64, 1), try standby.receive(baseRecord(identity, 1, "one")));
    try std.testing.expectError(error.UnexpectedRecordLsn, standby.receive(baseRecord(identity, 3, "three-before-two")));
    try std.testing.expectEqual(@as(u64, 1), standby.currentProgress().received_lsn);
    try std.testing.expectEqual(@as(u64, 2), standby.nextReceiveLsn());

    try std.testing.expectEqual(@as(u64, 2), try standby.receive(baseRecord(identity, 2, "two")));
    try std.testing.expectEqual(@as(u64, 3), try standby.receive(baseRecord(identity, 3, "three")));

    var capture = ApplyCapture{ .alloc = alloc };
    defer capture.deinit();
    try std.testing.expectEqual(@as(usize, 3), try standby.applyAvailable(&capture, ApplyCapture.apply));
    try std.testing.expectEqualStrings("one", capture.payloads.items[0]);
    try std.testing.expectEqualStrings("two", capture.payloads.items[1]);
    try std.testing.expectEqualStrings("three", capture.payloads.items[2]);
}

test "storage.ha chaos crash during apply preserves remote write and blocks remote apply" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "apply-crash");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);
    _ = try primary.append(.{ .payload = "one" });
    _ = try primary.append(.{ .payload = "two" });

    {
        var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
        defer standby.close();
        var capture = ApplyCapture{ .alloc = alloc, .fail_at_lsn = 2 };
        defer capture.deinit();
        try std.testing.expectError(
            error.IntentionalApplyFailure,
            session.replicateAvailable(alloc, &primary, "standby-a", &standby, &capture, ApplyCapture.apply),
        );
    }

    const names = [_][]const u8{"standby-a"};
    var decision = try primary.evaluateDurability(2, .{
        .mode = .remote_write,
        .standby_names = &names,
    });
    try std.testing.expectEqual(primary_mod.DurabilityStatus.satisfied, decision.status);
    decision = try primary.evaluateDurability(2, .{
        .mode = .remote_apply,
        .standby_names = &names,
    });
    try std.testing.expectEqual(primary_mod.DurabilityStatus.would_block, decision.status);

    {
        var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
        defer standby.close();
        var capture = ApplyCapture{ .alloc = alloc };
        defer capture.deinit();
        const result = try session.replicateAvailable(alloc, &primary, "standby-a", &standby, &capture, ApplyCapture.apply);
        try std.testing.expectEqual(@as(usize, 0), result.received_count);
        try std.testing.expectEqual(@as(usize, 1), result.applied_count);
        try std.testing.expectEqual(@as(u64, 2), result.progress.applied_lsn);
    }

    decision = try primary.evaluateDurability(2, .{
        .mode = .remote_apply,
        .standby_names = &names,
    });
    try std.testing.expectEqual(primary_mod.DurabilityStatus.satisfied, decision.status);
}

test "storage.ha chaos crash after apply before ack reports durable progress on resume" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "apply-before-ack-crash");
    defer paths.deinit(alloc);
    const identity = testIdentity();
    const names = [_][]const u8{"standby-a"};

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);
    _ = try primary.append(.{ .payload = "one" });

    {
        var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
        defer standby.close();
        try std.testing.expectEqual(@as(u64, 1), try standby.receive(baseRecord(identity, 1, "one")));

        var capture = ApplyCapture{ .alloc = alloc };
        defer capture.deinit();
        try std.testing.expectEqual(@as(usize, 1), try standby.applyAvailable(&capture, ApplyCapture.apply));
        try std.testing.expectEqualStrings("one", capture.payloads.items[0]);
        try std.testing.expectEqual(@as(u64, 1), standby.currentProgress().applied_lsn);
        // Simulate a process crash before the standby status update reaches
        // the primary. The primary must not infer remote_apply durability yet.
    }

    var decision = try primary.evaluateDurability(1, .{
        .mode = .remote_apply,
        .standby_names = &names,
    });
    try std.testing.expectEqual(primary_mod.DurabilityStatus.would_block, decision.status);

    {
        var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
        defer standby.close();
        try std.testing.expectEqual(@as(u64, 1), standby.currentProgress().received_lsn);
        try std.testing.expectEqual(@as(u64, 1), standby.currentProgress().applied_lsn);

        var capture = ApplyCapture{ .alloc = alloc };
        defer capture.deinit();
        const resumed = try session.replicateAvailable(alloc, &primary, "standby-a", &standby, &capture, ApplyCapture.apply);
        try std.testing.expectEqual(@as(usize, 0), resumed.received_count);
        try std.testing.expectEqual(@as(usize, 0), resumed.applied_count);
        try std.testing.expectEqual(@as(u64, 1), resumed.progress.applied_lsn);
    }

    decision = try primary.evaluateDurability(1, .{
        .mode = .remote_apply,
        .standby_names = &names,
    });
    try std.testing.expectEqual(primary_mod.DurabilityStatus.satisfied, decision.status);
}

test "storage.ha chaos primary restart preserves synchronous acknowledgement boundaries" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "primary-sync-ack-crash");
    defer paths.deinit(alloc);
    const identity = testIdentity();
    const names = [_][]const u8{"standby-a"};

    {
        var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
        defer primary.close();
        try primary.createSlot("standby-a", 0);
        _ = try primary.append(.{ .payload = "one" });

        const decision = try primary.evaluateDurability(1, .{
            .mode = .remote_write,
            .standby_names = &names,
        });
        try std.testing.expectEqual(primary_mod.DurabilityStatus.would_block, decision.status);
    }

    {
        var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
        defer primary.close();
        var decision = try primary.evaluateDurability(1, .{
            .mode = .remote_write,
            .standby_names = &names,
        });
        try std.testing.expectEqual(primary_mod.DurabilityStatus.would_block, decision.status);

        try primary.standbyStatusUpdate("standby-a", identity.timeline_id, 1, 0);
        decision = try primary.evaluateDurability(1, .{
            .mode = .remote_write,
            .standby_names = &names,
        });
        try std.testing.expectEqual(primary_mod.DurabilityStatus.satisfied, decision.status);
        decision = try primary.evaluateDurability(1, .{
            .mode = .remote_apply,
            .standby_names = &names,
        });
        try std.testing.expectEqual(primary_mod.DurabilityStatus.would_block, decision.status);
    }

    {
        var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
        defer primary.close();
        var decision = try primary.evaluateDurability(1, .{
            .mode = .remote_write,
            .standby_names = &names,
        });
        try std.testing.expectEqual(primary_mod.DurabilityStatus.satisfied, decision.status);
        decision = try primary.evaluateDurability(1, .{
            .mode = .remote_apply,
            .standby_names = &names,
        });
        try std.testing.expectEqual(primary_mod.DurabilityStatus.would_block, decision.status);

        try primary.standbyStatusUpdate("standby-a", identity.timeline_id, 1, 1);
        decision = try primary.evaluateDurability(1, .{
            .mode = .remote_apply,
            .standby_names = &names,
        });
        try std.testing.expectEqual(primary_mod.DurabilityStatus.satisfied, decision.status);
    }

    {
        var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
        defer primary.close();
        const decision = try primary.evaluateDurability(1, .{
            .mode = .remote_apply,
            .standby_names = &names,
        });
        try std.testing.expectEqual(primary_mod.DurabilityStatus.satisfied, decision.status);
    }
}

test "storage.ha chaos lag retention forces reseed and former primary cannot rewind expired WAL" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "retention-rejoin");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);
    var i: usize = 0;
    while (i < 8) : (i += 1) {
        _ = try primary.append(.{ .payload = "entry" });
    }
    try primary.standbyStatusUpdate("standby-a", identity.timeline_id, 2, 1);

    const snapshot = try primary.retentionSnapshot(.{ .max_lag_lsn = 3 });
    try std.testing.expectEqual(@as(usize, 1), snapshot.reseed_recommended);
    try std.testing.expectEqual(@as(usize, 0), snapshot.active_slots);
    try std.testing.expectEqual(@as(u64, 0), snapshot.retained_lsn_count);
    const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
    try std.testing.expect(slot.reseed_required);

    const receipt = fencing.Receipt{
        .identity = .{
            .cluster_id = identity.cluster_id,
            .shard_id = identity.shard_id,
            .table_id = identity.table_id,
            .timeline_id = 2,
            .epoch = 2,
        },
        .old_primary_id = "primary-a",
        .promoted_node_id = "standby-a",
        .parent_timeline_id = identity.timeline_id,
        .parent_epoch = identity.epoch,
        .new_timeline_id = 2,
        .new_epoch = 2,
        .required_lsn = 4,
        .observed_lsn = 4,
        .generation = 1,
        .forced = false,
        .token = "token",
        .reason = "chaos",
    };
    const assessment = rejoin.assessFormerPrimary(.{
        .node_id = "primary-a",
        .identity = identity,
        .last_lsn = 8,
    }, receipt, .{ .retained_from_lsn = 5 });
    try std.testing.expectEqual(rejoin.Action.reseed, assessment.action);
    try std.testing.expectEqual(rejoin.Reason.parent_timeline_wal_expired, assessment.reason);
}

test "storage.ha chaos network partition requires fence before standby promotion" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "network-partition");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();

    try primary.createSlot("standby-a", 0);
    _ = try primary.append(.{ .payload = "before-partition" });

    var capture = ApplyCapture{ .alloc = alloc };
    defer capture.deinit();
    const replicated = try session.replicateAvailable(alloc, &primary, "standby-a", &standby, &capture, ApplyCapture.apply);
    try std.testing.expectEqual(@as(u64, 1), replicated.progress.applied_lsn);

    const sync_names = [_][]const u8{"standby-a"};
    _ = try primary.append(.{ .payload = "partitioned-primary-write" });
    var decision = try primary.evaluateDurability(2, .{
        .mode = .remote_apply,
        .standby_names = &sync_names,
        .failure_policy = .block,
    });
    try std.testing.expectEqual(primary_mod.DurabilityStatus.would_block, decision.status);

    decision = try primary.evaluateDurability(2, .{
        .mode = .remote_apply,
        .standby_names = &sync_names,
        .failure_policy = .degrade_to_async,
    });
    try std.testing.expectEqual(primary_mod.DurabilityStatus.degraded_to_async, decision.status);

    try std.testing.expectError(error.FencingRequired, standby.promote(.{
        .new_timeline_id = 2,
        .new_epoch = 2,
        .required_lsn = 2,
    }));
    try std.testing.expectError(error.PromotionRequiresForce, standby.promote(.{
        .new_timeline_id = 2,
        .new_epoch = 2,
        .required_lsn = 2,
        .fencing_confirmed = true,
    }));

    var fence_store = try fencing.Store.open(alloc, paths.fence_wal.ptr, .{});
    defer fence_store.close();
    const receipt = try fence_store.acquirePromotionFence(.{
        .identity = identity,
        .old_primary_id = "primary-a",
        .promoted_node_id = "standby-a",
        .new_timeline_id = 2,
        .new_epoch = 2,
        .generation = 1,
        .required_lsn = 2,
        .observed_lsn = standby.currentProgress().applied_lsn,
        .force = true,
        .reason = "network-partition",
    });
    defer fencing.freeReceipt(alloc, receipt);

    const promoted = try standby.promote(receipt.promotionRequest());
    try std.testing.expect(promoted.forced);
    try std.testing.expect(promoted.data_loss_possible);
    try std.testing.expectEqual(@as(u64, 2), standby.identitySnapshot().timeline_id);
    try std.testing.expectEqual(@as(u64, 2), standby.currentProgress().applied_lsn);

    const rejoin_blocked = rejoin.assessFormerPrimary(.{
        .node_id = "primary-a",
        .identity = identity,
        .last_lsn = primary.lastLsn(),
    }, receipt, .{ .retained_from_lsn = 1 });
    try std.testing.expectEqual(rejoin.Action.reseed, rejoin_blocked.action);
    try std.testing.expectEqual(rejoin.Reason.parent_timeline_retained, rejoin_blocked.reason);

    const rejoin_allowed = rejoin.assessFormerPrimary(.{
        .node_id = "primary-a",
        .identity = identity,
        .last_lsn = primary.lastLsn(),
    }, receipt, .{
        .retained_from_lsn = 1,
        .allow_rewind_after_forced_promotion = true,
    });
    try std.testing.expectEqual(rejoin.Action.reseed, rejoin_allowed.action);
    try std.testing.expect(!rejoin_allowed.data_loss_discarded);
    try std.testing.expectError(
        error.RejoinRewindNotAllowed,
        rejoin.rewindReplicationLog(alloc, &primary.log, rejoin_allowed),
    );
    var retained_divergent = (try primary.log.entryAt(alloc, 2)) orelse return error.TestExpectedEqual;
    defer retained_divergent.deinit(alloc);

    try std.testing.expectError(error.WrongTimeline, standby.receive(baseRecord(identity, 3, "old-timeline-after-promotion")));
}
