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

//! HA operator reconciliation planner.
//!
//! This module is intentionally independent of Kubernetes APIs. It models the
//! CRD fields and observed HA state that an operator needs, then returns stable
//! desired actions and conditions. The Go/Kubernetes controller can translate
//! these structs into CRD status, Services, Leases, pod bootstrap jobs, and admin
//! API calls without reimplementing the safety policy.

const std = @import("std");
const Allocator = std.mem.Allocator;
const fencing = @import("fencing.zig");
const primary_mod = @import("primary.zig");
const rejoin = @import("rejoin.zig");
const slot_store = @import("slot_store.zig");
const status = @import("status.zig");

pub const Mode = enum {
    disabled,
    hot_standby,
};

pub const FencingAuthority = enum {
    none,
    kubernetes_lease,
    storage_fence,
    metadata_raft,
    external,
};

pub const AutoFailoverPolicy = struct {
    enabled: bool = false,
    fencing_authority: FencingAuthority = .none,
    require_remote_apply: bool = true,
    maximum_lag_lsn: u64 = 0,
};

pub const StandbySpec = struct {
    name: []const u8,
    desired: bool = true,
    initial_lsn: ?u64 = null,
};

pub const Spec = struct {
    mode: Mode = .disabled,
    standbys: []const StandbySpec = &.{},
    sync_policy: ?primary_mod.SyncPolicy = null,
    retention_policy: slot_store.RetentionPolicy = .{},
    auto_failover: AutoFailoverPolicy = .{},
};

pub const ConditionType = enum {
    available,
    degraded,
    retention_pressure,
    reseed_required,
    automatic_failover_ready,
};

pub const Condition = struct {
    type: ConditionType,
    status: bool,
    reason: []const u8,
    message: []const u8,
};

pub const ActionKind = enum {
    create_slot,
    resume_slot,
    pause_slot,
    drop_slot,
    seed_standby,
    mark_reseed,
    acquire_fence,
    promote_standby,
    update_primary_endpoint,
    demote_former_primary,
    rewind_former_primary,
    reseed_former_primary,
};

pub const Action = struct {
    kind: ActionKind,
    standby_name: ?[]const u8 = null,
    slot_name: ?[]const u8 = null,
    target_lsn: ?u64 = null,
    reason: []const u8,
};

pub const Observed = struct {
    primary: status.PrimarySnapshot,
    former_primary: ?rejoin.FormerPrimaryState = null,
    promotion_receipt: ?fencing.Receipt = null,
    rejoin_policy: rejoin.RejoinPolicy = .{ .retained_from_lsn = 0 },
};

pub const Plan = struct {
    actions: []Action,
    conditions: []Condition,
    former_primary_assessment: ?rejoin.Assessment = null,
    automatic_promotion_allowed: bool,
    desired_standby_count: usize,
    healthy_standby_count: usize,
    reseed_required_count: usize,

    pub fn deinit(self: *Plan, alloc: Allocator) void {
        alloc.free(self.actions);
        alloc.free(self.conditions);
        self.* = undefined;
    }
};

pub fn reconcile(alloc: Allocator, spec: Spec, observed: Observed) !Plan {
    var actions = std.ArrayListUnmanaged(Action).empty;
    errdefer actions.deinit(alloc);
    var conditions = std.ArrayListUnmanaged(Condition).empty;
    errdefer conditions.deinit(alloc);

    if (spec.mode == .disabled) {
        try conditions.append(alloc, .{
            .type = .available,
            .status = false,
            .reason = "Disabled",
            .message = "HA hot standby mode is disabled",
        });
        return .{
            .actions = try actions.toOwnedSlice(alloc),
            .conditions = try conditions.toOwnedSlice(alloc),
            .automatic_promotion_allowed = false,
            .desired_standby_count = 0,
            .healthy_standby_count = 0,
            .reseed_required_count = 0,
        };
    }

    var desired_count: usize = 0;
    var healthy_count: usize = 0;
    var reseed_count: usize = 0;

    for (spec.standbys) |standby| {
        if (!standby.desired) {
            if (findSlot(observed.primary.slots, standby.name) != null) {
                try actions.append(alloc, .{
                    .kind = .pause_slot,
                    .standby_name = standby.name,
                    .slot_name = standby.name,
                    .reason = "StandbyRemovedFromSpec",
                });
            }
            continue;
        }

        desired_count += 1;
        const slot = findSlot(observed.primary.slots, standby.name) orelse {
            try actions.append(alloc, .{
                .kind = .create_slot,
                .standby_name = standby.name,
                .slot_name = standby.name,
                .target_lsn = standby.initial_lsn orelse observed.primary.current_lsn,
                .reason = "SlotMissing",
            });
            try actions.append(alloc, .{
                .kind = .seed_standby,
                .standby_name = standby.name,
                .slot_name = standby.name,
                .target_lsn = standby.initial_lsn orelse observed.primary.current_lsn,
                .reason = "StandbyNeedsBaseBackup",
            });
            continue;
        };

        if (!slot.active and !slot.reseed_required) {
            try actions.append(alloc, .{
                .kind = .resume_slot,
                .standby_name = standby.name,
                .slot_name = standby.name,
                .reason = "SlotInactive",
            });
        }

        if (slot.reseed_required or slot.status == .reseed_required) {
            reseed_count += 1;
            try actions.append(alloc, .{
                .kind = .mark_reseed,
                .standby_name = standby.name,
                .slot_name = standby.name,
                .target_lsn = observed.primary.current_lsn,
                .reason = "SlotRequiresReseed",
            });
            continue;
        }

        if (slot.active and slot.apply_lag_lsn == 0) healthy_count += 1;
    }

    const retention_pressure = observed.primary.retention.reseed_recommended > 0;
    const degraded = isDegraded(observed.primary.durability);
    const automatic_allowed = automaticPromotionAllowed(spec, observed.primary);

    try conditions.append(alloc, .{
        .type = .available,
        .status = healthy_count > 0 or desired_count == 0,
        .reason = if (healthy_count > 0) "HealthyStandbyAvailable" else "NoHealthyStandby",
        .message = if (healthy_count > 0) "At least one desired standby is caught up to apply" else "No desired standby is caught up to apply",
    });
    try conditions.append(alloc, .{
        .type = .degraded,
        .status = degraded,
        .reason = if (degraded) "SyncPolicyUnsatisfied" else "SyncPolicySatisfied",
        .message = if (degraded) "Synchronous HA policy is not currently satisfied" else "Synchronous HA policy is satisfied or not configured",
    });
    try conditions.append(alloc, .{
        .type = .retention_pressure,
        .status = retention_pressure,
        .reason = if (retention_pressure) "RetentionCapExceeded" else "RetentionWithinPolicy",
        .message = if (retention_pressure) "One or more slots are forcing WAL retention beyond policy" else "WAL retention is within configured policy",
    });
    try conditions.append(alloc, .{
        .type = .reseed_required,
        .status = reseed_count > 0,
        .reason = if (reseed_count > 0) "StandbyRequiresReseed" else "NoReseedRequired",
        .message = if (reseed_count > 0) "One or more desired standbys must be reseeded" else "No desired standby requires reseed",
    });
    try conditions.append(alloc, .{
        .type = .automatic_failover_ready,
        .status = automatic_allowed,
        .reason = automaticFailoverReason(spec, observed.primary, automatic_allowed),
        .message = if (automatic_allowed) "Automatic failover may acquire a fence and promote a caught-up standby" else "Automatic failover is disabled or missing a safe fencing/readiness prerequisite",
    });

    if (automatic_allowed) {
        try actions.append(alloc, .{
            .kind = .acquire_fence,
            .target_lsn = observed.primary.current_lsn,
            .reason = "AutomaticFailoverReady",
        });
        try actions.append(alloc, .{
            .kind = .promote_standby,
            .target_lsn = observed.primary.current_lsn,
            .reason = "AutomaticFailoverReady",
        });
        try actions.append(alloc, .{
            .kind = .update_primary_endpoint,
            .target_lsn = observed.primary.current_lsn,
            .reason = "PromotionPlanned",
        });
        try actions.append(alloc, .{
            .kind = .demote_former_primary,
            .target_lsn = observed.primary.current_lsn,
            .reason = "PromotionPlanned",
        });
    }

    const former_primary_assessment = if (observed.former_primary) |former| blk: {
        const assessment = rejoin.assessFormerPrimary(former, observed.promotion_receipt, observed.rejoin_policy);
        try appendFormerPrimaryAction(alloc, &actions, assessment);
        break :blk assessment;
    } else null;

    return .{
        .actions = try actions.toOwnedSlice(alloc),
        .conditions = try conditions.toOwnedSlice(alloc),
        .former_primary_assessment = former_primary_assessment,
        .automatic_promotion_allowed = automatic_allowed,
        .desired_standby_count = desired_count,
        .healthy_standby_count = healthy_count,
        .reseed_required_count = reseed_count,
    };
}

fn appendFormerPrimaryAction(
    alloc: Allocator,
    actions: *std.ArrayListUnmanaged(Action),
    assessment: rejoin.Assessment,
) !void {
    switch (assessment.action) {
        .reject_unfenced => try actions.append(alloc, .{
            .kind = .demote_former_primary,
            .standby_name = assessment.former_node_id,
            .target_lsn = assessment.former_last_lsn,
            .reason = "FormerPrimaryRejectedUnfenced",
        }),
        .already_current => {},
        .rewind => try actions.append(alloc, .{
            .kind = .rewind_former_primary,
            .standby_name = assessment.former_node_id,
            .target_lsn = assessment.fork_lsn,
            .reason = "FormerPrimaryCanRewind",
        }),
        .reseed => try actions.append(alloc, .{
            .kind = .reseed_former_primary,
            .standby_name = assessment.former_node_id,
            .target_lsn = assessment.fork_lsn,
            .reason = "FormerPrimaryRequiresReseed",
        }),
    }
}

fn findSlot(slots: []const status.SlotSnapshot, name: []const u8) ?status.SlotSnapshot {
    for (slots) |slot| {
        if (std.mem.eql(u8, slot.name, name)) return slot;
    }
    return null;
}

fn isDegraded(durability: ?primary_mod.DurabilityDecision) bool {
    const decision = durability orelse return false;
    return decision.status != .satisfied;
}

fn automaticPromotionAllowed(spec: Spec, primary: status.PrimarySnapshot) bool {
    if (!spec.auto_failover.enabled) return false;
    if (spec.auto_failover.fencing_authority == .none) return false;
    if (isDegraded(primary.durability)) return false;

    const max_lag = spec.auto_failover.maximum_lag_lsn;
    for (spec.standbys) |standby| {
        if (!standby.desired) continue;
        const slot = findSlot(primary.slots, standby.name) orelse continue;
        if (!slot.active or slot.reseed_required) continue;
        if (slot.status == .reseed_required) continue;
        if (slot.received_lsn + max_lag < primary.current_lsn) continue;
        if (spec.auto_failover.require_remote_apply) {
            if (slot.applied_lsn + max_lag < primary.current_lsn) continue;
        }
        return true;
    }
    return false;
}

fn automaticFailoverReason(spec: Spec, primary: status.PrimarySnapshot, allowed: bool) []const u8 {
    if (allowed) return "FencedPromotionReady";
    if (!spec.auto_failover.enabled) return "AutomaticFailoverDisabled";
    if (spec.auto_failover.fencing_authority == .none) return "FencingAuthorityMissing";
    if (isDegraded(primary.durability)) return "SyncPolicyUnsatisfied";
    return "NoEligibleStandby";
}

fn condition(plan: Plan, condition_type: ConditionType) ?Condition {
    for (plan.conditions) |item| {
        if (item.type == condition_type) return item;
    }
    return null;
}

test "storage.ha operator plans slots and standby bootstrap" {
    const alloc = std.testing.allocator;
    const slots = [_]status.SlotSnapshot{};
    const primary = status.PrimarySnapshot{
        .identity = .{ .cluster_id = 100, .shard_id = 10, .table_id = 20, .timeline_id = 1, .epoch = 1 },
        .current_lsn = 7,
        .slots = @constCast(slots[0..]),
        .retention = .{
            .primary_lsn = 7,
            .oldest_restart_lsn = 8,
            .retained_lsn_count = 0,
            .active_slots = 0,
            .reseed_recommended = 0,
        },
    };
    const standbys = [_]StandbySpec{
        .{ .name = "standby-a", .initial_lsn = 3 },
    };

    var plan = try reconcile(alloc, .{
        .mode = .hot_standby,
        .standbys = &standbys,
    }, .{ .primary = primary });
    defer plan.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), plan.actions.len);
    try std.testing.expectEqual(ActionKind.create_slot, plan.actions[0].kind);
    try std.testing.expectEqual(@as(?u64, 3), plan.actions[0].target_lsn);
    try std.testing.expectEqual(ActionKind.seed_standby, plan.actions[1].kind);
    try std.testing.expectEqual(@as(usize, 1), plan.desired_standby_count);
    try std.testing.expectEqual(@as(usize, 0), plan.healthy_standby_count);
    try std.testing.expect(!plan.automatic_promotion_allowed);
}

test "storage.ha operator reports retention pressure degraded sync and reseed" {
    const alloc = std.testing.allocator;
    const slots = [_]status.SlotSnapshot{
        .{
            .name = "standby-a",
            .timeline_id = 1,
            .active = true,
            .reseed_required = true,
            .restart_lsn = 1,
            .received_lsn = 2,
            .applied_lsn = 1,
            .write_lag_lsn = 8,
            .apply_lag_lsn = 9,
            .retention_lag_lsn = 9,
            .status = .reseed_required,
        },
    };
    const primary = status.PrimarySnapshot{
        .identity = .{ .cluster_id = 100, .shard_id = 10, .table_id = 20, .timeline_id = 1, .epoch = 1 },
        .current_lsn = 10,
        .slots = @constCast(slots[0..]),
        .retention = .{
            .primary_lsn = 10,
            .oldest_restart_lsn = 1,
            .retained_lsn_count = 9,
            .active_slots = 1,
            .reseed_recommended = 1,
        },
        .durability = .{
            .status = .would_block,
            .mode = .remote_apply,
            .selection = .any,
            .target_lsn = 10,
            .satisfied_count = 0,
            .required_count = 1,
            .candidate_count = 1,
        },
    };
    const standbys = [_]StandbySpec{
        .{ .name = "standby-a" },
    };

    var plan = try reconcile(alloc, .{
        .mode = .hot_standby,
        .standbys = &standbys,
        .auto_failover = .{
            .enabled = true,
            .fencing_authority = .kubernetes_lease,
        },
    }, .{ .primary = primary });
    defer plan.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), plan.reseed_required_count);
    try std.testing.expectEqual(ActionKind.mark_reseed, plan.actions[0].kind);
    try std.testing.expect((condition(plan, .retention_pressure) orelse return error.TestExpectedEqual).status);
    try std.testing.expect((condition(plan, .degraded) orelse return error.TestExpectedEqual).status);
    try std.testing.expect((condition(plan, .reseed_required) orelse return error.TestExpectedEqual).status);
    const failover = condition(plan, .automatic_failover_ready) orelse return error.TestExpectedEqual;
    try std.testing.expect(!failover.status);
    try std.testing.expectEqualStrings("SyncPolicyUnsatisfied", failover.reason);
}

test "storage.ha operator gates automatic promotion on fencing and caught up standby" {
    const alloc = std.testing.allocator;
    const slots = [_]status.SlotSnapshot{
        .{
            .name = "standby-a",
            .timeline_id = 1,
            .active = true,
            .reseed_required = false,
            .restart_lsn = 10,
            .received_lsn = 12,
            .applied_lsn = 12,
            .write_lag_lsn = 0,
            .apply_lag_lsn = 0,
            .retention_lag_lsn = 2,
            .status = .healthy,
        },
    };
    const primary = status.PrimarySnapshot{
        .identity = .{ .cluster_id = 100, .shard_id = 10, .table_id = 20, .timeline_id = 1, .epoch = 1 },
        .current_lsn = 12,
        .slots = @constCast(slots[0..]),
        .retention = .{
            .primary_lsn = 12,
            .oldest_restart_lsn = 10,
            .retained_lsn_count = 2,
            .active_slots = 1,
            .reseed_recommended = 0,
        },
        .durability = .{
            .status = .satisfied,
            .mode = .remote_apply,
            .selection = .any,
            .target_lsn = 12,
            .satisfied_count = 1,
            .required_count = 1,
            .candidate_count = 1,
        },
    };
    const standbys = [_]StandbySpec{
        .{ .name = "standby-a" },
    };

    var unsafe = try reconcile(alloc, .{
        .mode = .hot_standby,
        .standbys = &standbys,
        .auto_failover = .{
            .enabled = true,
            .fencing_authority = .none,
        },
    }, .{ .primary = primary });
    defer unsafe.deinit(alloc);
    try std.testing.expect(!unsafe.automatic_promotion_allowed);
    try std.testing.expectEqualStrings(
        "FencingAuthorityMissing",
        (condition(unsafe, .automatic_failover_ready) orelse return error.TestExpectedEqual).reason,
    );

    var safe = try reconcile(alloc, .{
        .mode = .hot_standby,
        .standbys = &standbys,
        .auto_failover = .{
            .enabled = true,
            .fencing_authority = .kubernetes_lease,
            .maximum_lag_lsn = 0,
        },
    }, .{ .primary = primary });
    defer safe.deinit(alloc);
    try std.testing.expect(safe.automatic_promotion_allowed);
    try std.testing.expectEqual(@as(usize, 4), safe.actions.len);
    try std.testing.expectEqual(ActionKind.acquire_fence, safe.actions[0].kind);
    try std.testing.expectEqual(ActionKind.promote_standby, safe.actions[1].kind);
    try std.testing.expectEqual(ActionKind.update_primary_endpoint, safe.actions[2].kind);
    try std.testing.expectEqual(ActionKind.demote_former_primary, safe.actions[3].kind);
    try std.testing.expect((condition(safe, .automatic_failover_ready) orelse return error.TestExpectedEqual).status);
}

test "storage.ha operator plans former primary demotion without fence" {
    const alloc = std.testing.allocator;
    const slots = [_]status.SlotSnapshot{};
    const primary = status.PrimarySnapshot{
        .identity = .{ .cluster_id = 100, .shard_id = 10, .table_id = 20, .timeline_id = 2, .epoch = 2 },
        .current_lsn = 12,
        .slots = @constCast(slots[0..]),
        .retention = .{
            .primary_lsn = 12,
            .oldest_restart_lsn = 10,
            .retained_lsn_count = 2,
            .active_slots = 0,
            .reseed_recommended = 0,
        },
    };
    const former_identity = primary.identity;

    var plan = try reconcile(alloc, .{
        .mode = .hot_standby,
    }, .{
        .primary = primary,
        .former_primary = .{
            .node_id = "primary-a",
            .identity = former_identity,
            .last_lsn = 12,
        },
        .promotion_receipt = null,
        .rejoin_policy = .{ .retained_from_lsn = 10 },
    });
    defer plan.deinit(alloc);

    const assessment = plan.former_primary_assessment orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(rejoin.Action.reject_unfenced, assessment.action);
    try std.testing.expectEqual(@as(usize, 1), plan.actions.len);
    try std.testing.expectEqual(ActionKind.demote_former_primary, plan.actions[0].kind);
    try std.testing.expectEqualStrings("FormerPrimaryRejectedUnfenced", plan.actions[0].reason);
}

test "storage.ha operator plans former primary rewind or reseed from fence receipt" {
    const alloc = std.testing.allocator;
    const slots = [_]status.SlotSnapshot{};
    const parent_identity = status.PrimarySnapshot{
        .identity = .{ .cluster_id = 100, .shard_id = 10, .table_id = 20, .timeline_id = 1, .epoch = 1 },
        .current_lsn = 12,
        .slots = @constCast(slots[0..]),
        .retention = .{
            .primary_lsn = 12,
            .oldest_restart_lsn = 10,
            .retained_lsn_count = 2,
            .active_slots = 0,
            .reseed_recommended = 0,
        },
    };
    var promoted_primary = parent_identity;
    promoted_primary.identity.timeline_id = 2;
    promoted_primary.identity.epoch = 2;
    const receipt = fencing.Receipt{
        .identity = promoted_primary.identity,
        .old_primary_id = "primary-a",
        .promoted_node_id = "standby-b",
        .parent_timeline_id = parent_identity.identity.timeline_id,
        .parent_epoch = parent_identity.identity.epoch,
        .new_timeline_id = promoted_primary.identity.timeline_id,
        .new_epoch = promoted_primary.identity.epoch,
        .required_lsn = 10,
        .observed_lsn = 10,
        .generation = 1,
        .forced = false,
        .token = "token",
        .reason = "manual",
    };

    var rewind = try reconcile(alloc, .{
        .mode = .hot_standby,
    }, .{
        .primary = promoted_primary,
        .former_primary = .{
            .node_id = "primary-a",
            .identity = parent_identity.identity,
            .last_lsn = 12,
        },
        .promotion_receipt = receipt,
        .rejoin_policy = .{ .retained_from_lsn = 8 },
    });
    defer rewind.deinit(alloc);
    try std.testing.expectEqual(rejoin.Action.rewind, rewind.former_primary_assessment.?.action);
    try std.testing.expectEqual(ActionKind.rewind_former_primary, rewind.actions[0].kind);
    try std.testing.expectEqual(@as(?u64, 10), rewind.actions[0].target_lsn);

    var reseed = try reconcile(alloc, .{
        .mode = .hot_standby,
    }, .{
        .primary = promoted_primary,
        .former_primary = .{
            .node_id = "primary-a",
            .identity = parent_identity.identity,
            .last_lsn = 12,
        },
        .promotion_receipt = receipt,
        .rejoin_policy = .{ .retained_from_lsn = 11 },
    });
    defer reseed.deinit(alloc);
    try std.testing.expectEqual(rejoin.Action.reseed, reseed.former_primary_assessment.?.action);
    try std.testing.expectEqual(ActionKind.reseed_former_primary, reseed.actions[0].kind);
    try std.testing.expectEqualStrings("FormerPrimaryRequiresReseed", reseed.actions[0].reason);
}
