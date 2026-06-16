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
const standby_mod = @import("standby.zig");

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
    fork_lsn: u64,
    former_last_lsn: u64,
    retained_from_lsn: u64,
    data_loss_discarded: bool,
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
            .fork_lsn = fence.observed_lsn,
            .former_last_lsn = former.last_lsn,
            .retained_from_lsn = policy.retained_from_lsn,
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

    if (fence.forced and !policy.allow_rewind_after_forced_promotion) {
        return reseed(.parent_timeline_retained, former, fence, policy);
    }

    return .{
        .action = .rewind,
        .reason = .parent_timeline_retained,
        .former_node_id = former.node_id,
        .target_timeline_id = fence.new_timeline_id,
        .target_epoch = fence.new_epoch,
        .fork_lsn = fork_lsn,
        .former_last_lsn = former.last_lsn,
        .retained_from_lsn = policy.retained_from_lsn,
        .data_loss_discarded = former.last_lsn > fork_lsn or fence.forced,
    };
}

fn reseed(reason: Reason, former: FormerPrimaryState, receipt: fencing.Receipt, policy: RejoinPolicy) Assessment {
    return .{
        .action = .reseed,
        .reason = reason,
        .former_node_id = former.node_id,
        .target_timeline_id = receipt.new_timeline_id,
        .target_epoch = receipt.new_epoch,
        .fork_lsn = receipt.observed_lsn,
        .former_last_lsn = former.last_lsn,
        .retained_from_lsn = policy.retained_from_lsn,
        .data_loss_discarded = false,
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
        .last_lsn = 12,
    };
    const receipt = promotedReceipt();
    const assessment = assessFormerPrimary(former, receipt, .{ .retained_from_lsn = 8 });
    try std.testing.expectEqual(Action.rewind, assessment.action);
    try std.testing.expectEqual(Reason.parent_timeline_retained, assessment.reason);
    try std.testing.expectEqual(@as(u64, 2), assessment.target_timeline_id);
    try std.testing.expectEqual(@as(u64, 10), assessment.fork_lsn);
    try std.testing.expect(assessment.data_loss_discarded);
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
        .last_lsn = 10,
    };

    var assessment = assessFormerPrimary(former, receipt, .{ .retained_from_lsn = 1 });
    try std.testing.expectEqual(Action.reseed, assessment.action);
    try std.testing.expectEqual(Reason.parent_timeline_retained, assessment.reason);

    assessment = assessFormerPrimary(former, receipt, .{
        .retained_from_lsn = 1,
        .allow_rewind_after_forced_promotion = true,
    });
    try std.testing.expectEqual(Action.rewind, assessment.action);
    try std.testing.expect(assessment.data_loss_discarded);
}
