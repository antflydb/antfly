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

//! Former-primary rejoin assessment.
//!
//! After promotion, a returned former primary must not resume writes on the old
//! timeline. This module gives operators, CLI commands, and future automation a
//! deterministic answer: no fence means reject; a compatible fenced parent
//! timeline can rewind if required WAL is retained; otherwise the node must be
//! reseeded.

const std = @import("std");
const fencing = @import("fencing.zig");
const replication_log = @import("replication_log.zig");
const replication_record = @import("replication_record.zig");
const standby_mod = @import("standby.zig");

var test_path_counter: u64 = 0;

pub const Action = enum {
    reject_unfenced,
    already_current,
    rewind,
    reseed,
};

pub const Reason = enum {
    no_fence,
    current_timeline,
    parent_timeline_retained,
    parent_timeline_wal_expired,
    incompatible_timeline,
    wrong_old_primary,
    wrong_cluster,
    wrong_shard,
    wrong_table,
    local_lsn_before_fork,
};

pub const FormerPrimaryState = struct {
    node_id: []const u8,
    identity: standby_mod.Identity,
    last_lsn: u64,
};

pub const RejoinPolicy = struct {
    retained_from_lsn: u64,
    allow_rewind_after_forced_promotion: bool = false,
};

pub const Assessment = struct {
    action: Action,
    reason: Reason,
    former_node_id: []const u8,
    target_timeline_id: u64,
    target_epoch: u64,
    parent_cluster_id: u64 = 0,
    parent_shard_id: u64 = 0,
    parent_table_id: u64 = 0,
    parent_timeline_id: u64 = 0,
    parent_epoch: u64 = 0,
    required_lsn: u64 = 0,
    fork_lsn: u64,
    former_last_lsn: u64,
    retained_from_lsn: u64,
    forced: bool = false,
    data_loss_discarded: bool,
};

pub const RewindResult = struct {
    node_id: []const u8,
    fork_lsn: u64,
    previous_last_lsn: u64,
    current_last_lsn: u64,
    next_lsn: u64,
    discarded_lsn_count: u64,
    target_timeline_id: u64,
    target_epoch: u64,
    data_loss_discarded: bool,
};

pub const ReseedResult = struct {
    node_id: []const u8,
    slot_name: []const u8,
    target_timeline_id: u64,
    target_epoch: u64,
    fork_lsn: u64,
    former_last_lsn: u64,
    reseed_required: bool,
    base_backup_required: bool,
};

pub fn assessFormerPrimary(
    former: FormerPrimaryState,
    receipt: ?fencing.Receipt,
    policy: RejoinPolicy,
) Assessment {
    const fence = receipt orelse return .{
        .action = .reject_unfenced,
        .reason = .no_fence,
        .former_node_id = former.node_id,
        .target_timeline_id = former.identity.timeline_id,
        .target_epoch = former.identity.epoch,
        .parent_cluster_id = former.identity.cluster_id,
        .parent_shard_id = former.identity.shard_id,
        .parent_table_id = former.identity.table_id,
        .parent_timeline_id = former.identity.timeline_id,
        .parent_epoch = former.identity.epoch,
        .fork_lsn = former.last_lsn,
        .former_last_lsn = former.last_lsn,
        .retained_from_lsn = policy.retained_from_lsn,
        .data_loss_discarded = false,
    };

    if (former.identity.cluster_id != fence.identity.cluster_id) return reseed(.wrong_cluster, former, fence, policy);
    if (former.identity.shard_id != fence.identity.shard_id) return reseed(.wrong_shard, former, fence, policy);
    if (former.identity.table_id != fence.identity.table_id) return reseed(.wrong_table, former, fence, policy);
    if (!std.mem.eql(u8, former.node_id, fence.old_primary_id)) return reseed(.wrong_old_primary, former, fence, policy);

    if (former.identity.timeline_id == fence.new_timeline_id and former.identity.epoch == fence.new_epoch) {
        return .{
            .action = .already_current,
            .reason = .current_timeline,
            .former_node_id = former.node_id,
            .target_timeline_id = fence.new_timeline_id,
            .target_epoch = fence.new_epoch,
            .parent_cluster_id = fence.identity.cluster_id,
            .parent_shard_id = fence.identity.shard_id,
            .parent_table_id = fence.identity.table_id,
            .parent_timeline_id = fence.parent_timeline_id,
            .parent_epoch = fence.parent_epoch,
            .required_lsn = fence.required_lsn,
            .fork_lsn = fence.observed_lsn,
            .former_last_lsn = former.last_lsn,
            .retained_from_lsn = policy.retained_from_lsn,
            .forced = fence.forced,
            .data_loss_discarded = false,
        };
    }

    if (former.identity.timeline_id != fence.parent_timeline_id or former.identity.epoch != fence.parent_epoch) {
        return reseed(.incompatible_timeline, former, fence, policy);
    }

    const fork_lsn = fence.observed_lsn;
    if (former.last_lsn < fork_lsn) {
        return reseed(.local_lsn_before_fork, former, fence, policy);
    }

    if (fork_lsn < policy.retained_from_lsn) {
        return reseed(.parent_timeline_wal_expired, former, fence, policy);
    }

    // The replication log is an effects stream, not an undo log for the LSM.
    // Once this primary has applied a write beyond the promotion fork, removing
    // the corresponding HA envelope would hide the divergent state without
    // rolling it back. Only an exact-fork former primary is safe to reuse in
    // place; every divergent branch needs a fresh base backup.
    if (former.last_lsn > fork_lsn) {
        return reseed(.parent_timeline_retained, former, fence, policy);
    }

    if (fence.forced and !policy.allow_rewind_after_forced_promotion) {
        return reseed(.parent_timeline_retained, former, fence, policy);
    }

    return .{
        .action = .rewind,
        .reason = .parent_timeline_retained,
        .former_node_id = former.node_id,
        .target_timeline_id = fence.new_timeline_id,
        .target_epoch = fence.new_epoch,
        .parent_cluster_id = fence.identity.cluster_id,
        .parent_shard_id = fence.identity.shard_id,
        .parent_table_id = fence.identity.table_id,
        .parent_timeline_id = fence.parent_timeline_id,
        .parent_epoch = fence.parent_epoch,
        .required_lsn = fence.required_lsn,
        .fork_lsn = fork_lsn,
        .former_last_lsn = former.last_lsn,
        .retained_from_lsn = policy.retained_from_lsn,
        .forced = fence.forced,
        .data_loss_discarded = false,
    };
}

pub fn rewindReplicationLog(
    alloc: std.mem.Allocator,
    log: *replication_log.ReplicationLog,
    assessment: Assessment,
) !RewindResult {
    if (assessment.action != .rewind) return error.RejoinRewindNotAllowed;

    const previous_last_lsn = log.lastLsn();
    if (previous_last_lsn != assessment.former_last_lsn) return error.RejoinAssessmentStale;
    if (assessment.fork_lsn < assessment.retained_from_lsn) return error.WalNoLongerRetained;
    if (previous_last_lsn < assessment.fork_lsn) return error.FormerPrimaryBeforeFork;
    if (previous_last_lsn > assessment.fork_lsn) return error.RejoinDivergentDataRequiresReseed;

    if (assessment.fork_lsn > 0) {
        var fork_entry = (try log.entryAt(alloc, assessment.fork_lsn)) orelse return error.WalNoLongerRetained;
        defer fork_entry.deinit(alloc);
        try validateForkRecord(assessment, fork_entry.record);
    }

    try log.truncateAfter(assessment.fork_lsn);
    const switch_lsn = std.math.add(u64, assessment.fork_lsn, 1) catch return error.LsnOverflow;
    const payload = try rejoinTimelineSwitchPayload(alloc, assessment);
    defer alloc.free(payload);
    _ = try log.append(alloc, .{
        .kind = .timeline_switch,
        .payload_codec = .json,
        .cluster_id = assessment.parent_cluster_id,
        .shard_id = assessment.parent_shard_id,
        .table_id = assessment.parent_table_id,
        .timeline_id = assessment.target_timeline_id,
        .epoch = assessment.target_epoch,
        .lsn = switch_lsn,
        .previous_lsn = assessment.fork_lsn,
        .payload = payload,
    });
    const current_last_lsn = log.lastLsn();
    return .{
        .node_id = assessment.former_node_id,
        .fork_lsn = assessment.fork_lsn,
        .previous_last_lsn = previous_last_lsn,
        .current_last_lsn = current_last_lsn,
        .next_lsn = log.nextLsn(),
        .discarded_lsn_count = previous_last_lsn - assessment.fork_lsn,
        .target_timeline_id = assessment.target_timeline_id,
        .target_epoch = assessment.target_epoch,
        .data_loss_discarded = assessment.data_loss_discarded,
    };
}

fn reseed(reason: Reason, former: FormerPrimaryState, receipt: fencing.Receipt, policy: RejoinPolicy) Assessment {
    return .{
        .action = .reseed,
        .reason = reason,
        .former_node_id = former.node_id,
        .target_timeline_id = receipt.new_timeline_id,
        .target_epoch = receipt.new_epoch,
        .parent_cluster_id = receipt.identity.cluster_id,
        .parent_shard_id = receipt.identity.shard_id,
        .parent_table_id = receipt.identity.table_id,
        .parent_timeline_id = receipt.parent_timeline_id,
        .parent_epoch = receipt.parent_epoch,
        .required_lsn = receipt.required_lsn,
        .fork_lsn = receipt.observed_lsn,
        .former_last_lsn = former.last_lsn,
        .retained_from_lsn = policy.retained_from_lsn,
        .forced = receipt.forced,
        .data_loss_discarded = false,
    };
}

fn rejoinTimelineSwitchPayload(alloc: std.mem.Allocator, assessment: Assessment) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, .{
        .parent_timeline_id = assessment.parent_timeline_id,
        .new_timeline_id = assessment.target_timeline_id,
        .parent_epoch = assessment.parent_epoch,
        .new_epoch = assessment.target_epoch,
        .required_lsn = assessment.required_lsn,
        .forced = assessment.forced,
        .data_loss_possible = false,
    }, .{});
}

fn validateForkRecord(assessment: Assessment, record: replication_record.RecordView) !void {
    if (record.lsn != assessment.fork_lsn) return error.RejoinForkIdentityMismatch;
    if (assessment.parent_cluster_id != 0 and record.cluster_id != assessment.parent_cluster_id) {
        return error.RejoinForkIdentityMismatch;
    }
    if (assessment.parent_shard_id != 0 and record.shard_id != assessment.parent_shard_id) {
        return error.RejoinForkIdentityMismatch;
    }
    if (assessment.parent_table_id != 0 and record.table_id != assessment.parent_table_id) {
        return error.RejoinForkIdentityMismatch;
    }
    if (assessment.parent_timeline_id != 0 and record.timeline_id != assessment.parent_timeline_id) {
        return error.RejoinForkIdentityMismatch;
    }
    if (assessment.parent_epoch != 0 and record.epoch != assessment.parent_epoch) {
        return error.RejoinForkIdentityMismatch;
    }
}

fn testPath(alloc: std.mem.Allocator, comptime name: []const u8) ![:0]u8 {
    const nonce = @atomicRmw(u64, &test_path_counter, .Add, 1, .seq_cst);
    const raw = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/ha-rejoin-" ++ name ++ "-{d}-{d}",
        .{ std.testing.random_seed, nonce },
    );
    defer alloc.free(raw);
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), raw) catch {};
    return try alloc.dupeZ(u8, raw);
}

fn baseRecord(lsn: u64, payload: []const u8) replication_record.Record {
    const identity = parentIdentity();
    return .{
        .kind = .batch_mutation,
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

fn parentIdentity() standby_mod.Identity {
    return .{
        .cluster_id = 100,
        .shard_id = 10,
        .table_id = 20,
        .timeline_id = 1,
        .epoch = 1,
    };
}

fn promotedReceipt() fencing.Receipt {
    const parent = parentIdentity();
    return .{
        .identity = .{
            .cluster_id = parent.cluster_id,
            .shard_id = parent.shard_id,
            .table_id = parent.table_id,
            .timeline_id = 2,
            .epoch = 2,
        },
        .old_primary_id = "primary-a",
        .promoted_node_id = "standby-b",
        .parent_timeline_id = parent.timeline_id,
        .parent_epoch = parent.epoch,
        .new_timeline_id = 2,
        .new_epoch = 2,
        .required_lsn = 10,
        .observed_lsn = 10,
        .generation = 1,
        .forced = false,
        .token = "token",
        .reason = "manual",
    };
}

test "storage.ha rejoin rejects former primary without a fence" {
    const former = FormerPrimaryState{
        .node_id = "primary-a",
        .identity = parentIdentity(),
        .last_lsn = 12,
    };
    const assessment = assessFormerPrimary(former, null, .{ .retained_from_lsn = 1 });
    try std.testing.expectEqual(Action.reject_unfenced, assessment.action);
    try std.testing.expectEqual(Reason.no_fence, assessment.reason);
    try std.testing.expectEqual(@as(u64, 12), assessment.fork_lsn);
}

test "storage.ha rejoin rewinds compatible fenced former primary when WAL is retained" {
    const former = FormerPrimaryState{
        .node_id = "primary-a",
        .identity = parentIdentity(),
        .last_lsn = 10,
    };
    const receipt = promotedReceipt();
    const assessment = assessFormerPrimary(former, receipt, .{ .retained_from_lsn = 8 });
    try std.testing.expectEqual(Action.rewind, assessment.action);
    try std.testing.expectEqual(Reason.parent_timeline_retained, assessment.reason);
    try std.testing.expectEqual(@as(u64, 2), assessment.target_timeline_id);
    try std.testing.expectEqual(@as(u64, 10), assessment.fork_lsn);
    try std.testing.expect(!assessment.data_loss_discarded);
}

test "storage.ha rejoin reseeds when timeline is incompatible or WAL expired" {
    const receipt = promotedReceipt();
    var former = FormerPrimaryState{
        .node_id = "primary-a",
        .identity = parentIdentity(),
        .last_lsn = 12,
    };

    var assessment = assessFormerPrimary(former, receipt, .{ .retained_from_lsn = 11 });
    try std.testing.expectEqual(Action.reseed, assessment.action);
    try std.testing.expectEqual(Reason.parent_timeline_wal_expired, assessment.reason);

    former.identity.timeline_id = 99;
    former.identity.epoch = 99;
    assessment = assessFormerPrimary(former, receipt, .{ .retained_from_lsn = 1 });
    try std.testing.expectEqual(Action.reseed, assessment.action);
    try std.testing.expectEqual(Reason.incompatible_timeline, assessment.reason);
}

test "storage.ha rejoin treats current timeline as already joined" {
    const receipt = promotedReceipt();
    var identity = parentIdentity();
    identity.timeline_id = 2;
    identity.epoch = 2;
    const former = FormerPrimaryState{
        .node_id = "primary-a",
        .identity = identity,
        .last_lsn = 13,
    };

    const assessment = assessFormerPrimary(former, receipt, .{ .retained_from_lsn = 1 });
    try std.testing.expectEqual(Action.already_current, assessment.action);
    try std.testing.expectEqual(Reason.current_timeline, assessment.reason);
}

test "storage.ha rejoin requires explicit policy for forced-promotion rewind" {
    var receipt = promotedReceipt();
    receipt.forced = true;
    receipt.observed_lsn = 8;
    const former = FormerPrimaryState{
        .node_id = "primary-a",
        .identity = parentIdentity(),
        .last_lsn = 8,
    };

    var assessment = assessFormerPrimary(former, receipt, .{ .retained_from_lsn = 1 });
    try std.testing.expectEqual(Action.reseed, assessment.action);
    try std.testing.expectEqual(Reason.parent_timeline_retained, assessment.reason);

    assessment = assessFormerPrimary(former, receipt, .{
        .retained_from_lsn = 1,
        .allow_rewind_after_forced_promotion = true,
    });
    try std.testing.expectEqual(Action.rewind, assessment.action);
    try std.testing.expect(!assessment.data_loss_discarded);
}

test "storage.ha rejoin reseeds a divergent former primary instead of hiding applied writes" {
    var receipt = promotedReceipt();
    receipt.required_lsn = 2;
    receipt.observed_lsn = 2;
    const assessment = assessFormerPrimary(.{
        .node_id = "primary-a",
        .identity = parentIdentity(),
        .last_lsn = 3,
    }, receipt, .{ .retained_from_lsn = 1 });

    // Truncating the logical HA stream cannot roll back a batch that was
    // already applied to the former primary's LSM. A divergent data branch
    // therefore requires a fresh base backup even when the parent WAL is
    // retained.
    try std.testing.expectEqual(Action.reseed, assessment.action);
    try std.testing.expectEqual(Reason.parent_timeline_retained, assessment.reason);
    try std.testing.expect(!assessment.data_loss_discarded);
}

test "storage.ha rejoin rewind installs the promoted timeline boundary" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "rewind-timeline-switch");
    defer alloc.free(path);

    var log = try replication_log.ReplicationLog.open(path.ptr, .{});
    defer log.close();
    _ = try log.append(alloc, baseRecord(1, "one"));
    _ = try log.append(alloc, baseRecord(2, "two"));

    var receipt = promotedReceipt();
    receipt.required_lsn = 2;
    receipt.observed_lsn = 2;
    const assessment = assessFormerPrimary(.{
        .node_id = "primary-a",
        .identity = parentIdentity(),
        .last_lsn = log.lastLsn(),
    }, receipt, .{ .retained_from_lsn = 1 });
    try std.testing.expectEqual(Action.rewind, assessment.action);

    const result = try rewindReplicationLog(alloc, &log, assessment);
    try std.testing.expectEqual(@as(u64, 3), result.current_last_lsn);
    try std.testing.expectEqual(@as(u64, 4), result.next_lsn);
    try std.testing.expectEqual(@as(u64, 0), result.discarded_lsn_count);

    var boundary = (try log.entryAt(alloc, 3)) orelse return error.TestExpectedEqual;
    defer boundary.deinit(alloc);
    try std.testing.expectEqual(replication_record.RecordKind.timeline_switch, boundary.record.kind);
    try std.testing.expectEqual(replication_record.PayloadCodec.json, boundary.record.payload_codec);
    try std.testing.expectEqual(@as(u64, 2), boundary.record.previous_lsn);
    try std.testing.expectEqual(receipt.new_timeline_id, boundary.record.timeline_id);
    try std.testing.expectEqual(receipt.new_epoch, boundary.record.epoch);
}

test "storage.ha rejoin rewind defensively rejects a divergent log suffix" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "rewind-log");
    defer alloc.free(path);

    var log = try replication_log.ReplicationLog.open(path.ptr, .{});
    defer log.close();
    _ = try log.append(alloc, baseRecord(1, "one"));
    _ = try log.append(alloc, baseRecord(2, "two"));
    _ = try log.append(alloc, baseRecord(3, "divergent-three"));
    _ = try log.append(alloc, baseRecord(4, "divergent-four"));

    var receipt = promotedReceipt();
    receipt.required_lsn = 2;
    receipt.observed_lsn = 2;
    var assessment = assessFormerPrimary(.{
        .node_id = "primary-a",
        .identity = parentIdentity(),
        .last_lsn = log.lastLsn(),
    }, receipt, .{ .retained_from_lsn = 1 });
    try std.testing.expectEqual(Action.reseed, assessment.action);

    // Even a stale or malicious caller cannot bypass the assessment outcome by
    // relabeling it as a rewind request.
    assessment.action = .rewind;
    try std.testing.expectError(
        error.RejoinDivergentDataRequiresReseed,
        rewindReplicationLog(alloc, &log, assessment),
    );

    const entries = try log.iterateFrom(alloc, 1);
    defer replication_log.freeEntries(alloc, entries);
    try std.testing.expectEqual(@as(usize, 4), entries.len);
    try std.testing.expectEqualStrings("one", entries[0].record.payload);
    try std.testing.expectEqualStrings("two", entries[1].record.payload);
    try std.testing.expectEqualStrings("divergent-three", entries[2].record.payload);
    try std.testing.expectEqualStrings("divergent-four", entries[3].record.payload);
    try std.testing.expectEqual(@as(u64, 4), log.lastLsn());
}

test "storage.ha rejoin rewind rejects stale or non-rewind assessments" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "rewind-reject");
    defer alloc.free(path);

    var log = try replication_log.ReplicationLog.open(path.ptr, .{});
    defer log.close();
    _ = try log.append(alloc, baseRecord(1, "one"));
    _ = try log.append(alloc, baseRecord(2, "two"));

    var receipt = promotedReceipt();
    receipt.required_lsn = 2;
    receipt.observed_lsn = 2;
    const assessment = assessFormerPrimary(.{
        .node_id = "primary-a",
        .identity = parentIdentity(),
        .last_lsn = log.lastLsn(),
    }, receipt, .{ .retained_from_lsn = 1 });
    try std.testing.expectEqual(Action.rewind, assessment.action);

    _ = try log.append(alloc, baseRecord(3, "late-write"));
    try std.testing.expectError(
        error.RejoinAssessmentStale,
        rewindReplicationLog(alloc, &log, assessment),
    );

    const no_fence = assessFormerPrimary(.{
        .node_id = "primary-a",
        .identity = parentIdentity(),
        .last_lsn = log.lastLsn(),
    }, null, .{ .retained_from_lsn = 1 });
    try std.testing.expectError(
        error.RejoinRewindNotAllowed,
        rewindReplicationLog(alloc, &log, no_fence),
    );
}

test "storage.ha rejoin rewind requires retained fork record" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "rewind-retained");
    defer alloc.free(path);

    var log = try replication_log.ReplicationLog.open(path.ptr, .{});
    defer log.close();
    _ = try log.append(alloc, baseRecord(1, "one"));
    _ = try log.append(alloc, baseRecord(2, "two"));
    _ = try log.append(alloc, baseRecord(3, "three"));

    var receipt = promotedReceipt();
    receipt.required_lsn = 3;
    receipt.observed_lsn = 3;
    const assessment = assessFormerPrimary(.{
        .node_id = "primary-a",
        .identity = parentIdentity(),
        .last_lsn = log.lastLsn(),
    }, receipt, .{ .retained_from_lsn = 1 });
    try std.testing.expectEqual(Action.rewind, assessment.action);

    try log.truncate(3);
    try std.testing.expectError(
        error.WalNoLongerRetained,
        rewindReplicationLog(alloc, &log, assessment),
    );
}

test "storage.ha rejoin rewind rejects fork record identity mismatch" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "rewind-identity");
    defer alloc.free(path);

    var log = try replication_log.ReplicationLog.open(path.ptr, .{});
    defer log.close();
    _ = try log.append(alloc, baseRecord(1, "one"));
    _ = try log.append(alloc, baseRecord(2, "two"));
    var wrong_timeline = baseRecord(3, "wrong-parent-fork");
    wrong_timeline.timeline_id = 99;
    _ = try log.append(alloc, wrong_timeline);

    var receipt = promotedReceipt();
    receipt.required_lsn = 3;
    receipt.observed_lsn = 3;
    const assessment = assessFormerPrimary(.{
        .node_id = "primary-a",
        .identity = parentIdentity(),
        .last_lsn = log.lastLsn(),
    }, receipt, .{ .retained_from_lsn = 1 });
    try std.testing.expectEqual(Action.rewind, assessment.action);

    try std.testing.expectError(
        error.RejoinForkIdentityMismatch,
        rewindReplicationLog(alloc, &log, assessment),
    );
    try std.testing.expectEqual(@as(u64, 3), log.lastLsn());
}
