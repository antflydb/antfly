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
const admin_cli = @import("admin_cli.zig");
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

pub const FencingObservation = struct {
    authority: FencingAuthority = .none,
    ready: bool = false,
    holder: ?[]const u8 = null,
    generation: ?u64 = null,
    reason: []const u8 = "NotObserved",
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
    unhealthy,
    lagging,
    retention_pressure,
    reseed_required,
    automatic_failover_ready,
};

pub const ConditionSeverity = enum {
    info,
    warning,
    critical,
};

pub const Condition = struct {
    type: ConditionType,
    status: bool,
    severity: ConditionSeverity = .info,
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

pub const ActionPhase = enum {
    reconcile,
    fence,
    promote,
    route,
    rejoin,
};

pub const ActionExecutor = enum {
    admin_command,
    controller_action,
};

pub const FencingPrecondition = struct {
    authority: FencingAuthority,
    holder: []const u8,
    generation: u64,
    reason: []const u8,
};

pub const Action = struct {
    kind: ActionKind,
    phase: ActionPhase = .reconcile,
    executor: ActionExecutor = .admin_command,
    depends_on: ?ActionKind = null,
    fencing_precondition: ?FencingPrecondition = null,
    standby_name: ?[]const u8 = null,
    slot_name: ?[]const u8 = null,
    target_lsn: ?u64 = null,
    route_from: ?[]const u8 = null,
    route_to: ?[]const u8 = null,
    reason: []const u8,
};

pub const AdminCommandOptions = struct {
    manifest_id_prefix: []const u8 = "base",
    promote_from_current_fence: bool = true,
    old_primary_id: ?[]const u8 = null,
    promoted_node_id: ?[]const u8 = null,
    new_timeline_id: ?u64 = null,
    new_epoch: ?u64 = null,
    former_last_lsn: ?u64 = null,
    retained_from_lsn: u64 = 0,
    allow_forced_rewind: bool = false,
    fence_generation: ?u64 = null,
    fence_token: ?[]const u8 = null,
    force: bool = false,
    reason: []const u8 = "operator",
};

pub const AdminCommand = struct {
    argv: []const []const u8,
    owned_args: []const []u8,

    pub fn deinit(self: *AdminCommand, alloc: Allocator) void {
        for (self.owned_args) |arg| alloc.free(arg);
        alloc.free(self.owned_args);
        alloc.free(self.argv);
        self.* = undefined;
    }

    pub fn parsePlan(self: AdminCommand, alloc: Allocator) !admin_cli.Plan {
        return try admin_cli.parse(alloc, self.argv);
    }
};

pub const Observed = struct {
    primary: status.PrimarySnapshot,
    current_primary_id: ?[]const u8 = null,
    primary_admin_unavailable: bool = false,
    fencing: FencingObservation = .{},
    former_primary: ?rejoin.FormerPrimaryState = null,
    promotion_receipt: ?fencing.Receipt = null,
    rejoin_policy: rejoin.RejoinPolicy = .{ .retained_from_lsn = 0 },
};

pub const Plan = struct {
    actions: []Action,
    conditions: []Condition,
    fencing: FencingObservation = .{},
    former_primary_assessment: ?rejoin.Assessment = null,
    automatic_promotion_allowed: bool,
    desired_standby_count: usize,
    healthy_standby_count: usize,
    unhealthy_standby_count: usize,
    lagging_standby_count: usize,
    reseed_required_count: usize,

    pub fn deinit(self: *Plan, alloc: Allocator) void {
        alloc.free(self.actions);
        alloc.free(self.conditions);
        self.* = undefined;
    }
};

pub const PlanDocument = struct {
    schema_version: u32 = 1,
    automatic_promotion_allowed: bool,
    fencing: FencingObservation,
    desired_standby_count: usize,
    healthy_standby_count: usize,
    unhealthy_standby_count: usize,
    lagging_standby_count: usize,
    reseed_required_count: usize,
    actions: []const Action,
    conditions: []const Condition,
    former_primary_assessment: ?rejoin.Assessment,
};

pub fn planDocument(plan: Plan) PlanDocument {
    return .{
        .automatic_promotion_allowed = plan.automatic_promotion_allowed,
        .fencing = plan.fencing,
        .desired_standby_count = plan.desired_standby_count,
        .healthy_standby_count = plan.healthy_standby_count,
        .unhealthy_standby_count = plan.unhealthy_standby_count,
        .lagging_standby_count = plan.lagging_standby_count,
        .reseed_required_count = plan.reseed_required_count,
        .actions = plan.actions,
        .conditions = plan.conditions,
        .former_primary_assessment = plan.former_primary_assessment,
    };
}

pub fn renderJsonAlloc(alloc: Allocator, plan: Plan) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, planDocument(plan), .{});
}

pub fn reconcile(alloc: Allocator, spec: Spec, observed: Observed) !Plan {
    var actions = std.ArrayListUnmanaged(Action).empty;
    errdefer actions.deinit(alloc);
    var conditions = std.ArrayListUnmanaged(Condition).empty;
    errdefer conditions.deinit(alloc);

    if (spec.mode == .disabled) {
        try conditions.append(alloc, .{
            .type = .available,
            .status = false,
            .severity = .info,
            .reason = "Disabled",
            .message = "HA hot standby mode is disabled",
        });
        return .{
            .actions = try actions.toOwnedSlice(alloc),
            .conditions = try conditions.toOwnedSlice(alloc),
            .fencing = observed.fencing,
            .automatic_promotion_allowed = false,
            .desired_standby_count = 0,
            .healthy_standby_count = 0,
            .unhealthy_standby_count = 0,
            .lagging_standby_count = 0,
            .reseed_required_count = 0,
        };
    }

    var desired_count: usize = 0;
    var healthy_count: usize = 0;
    var unhealthy_count: usize = 0;
    var inactive_count: usize = 0;
    var replication_error_count: usize = 0;
    var lagging_count: usize = 0;
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
                .kind = .seed_standby,
                .standby_name = standby.name,
                .slot_name = standby.name,
                .target_lsn = standby.initial_lsn orelse observed.primary.current_lsn,
                .reason = "StandbyNeedsBaseBackup",
            });
            continue;
        };

        var slot_unhealthy = false;
        if (slot.last_error != null) {
            replication_error_count += 1;
            slot_unhealthy = true;
        }
        if (!slot.active and !slot.reseed_required) {
            inactive_count += 1;
            slot_unhealthy = true;
            try actions.append(alloc, .{
                .kind = .resume_slot,
                .standby_name = standby.name,
                .slot_name = standby.name,
                .reason = "SlotInactive",
            });
        }
        if (slot_unhealthy) unhealthy_count += 1;

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

        if (slot.status == .lagging) lagging_count += 1;
        if (slot.active and slot.apply_lag_lsn == 0) healthy_count += 1;
    }

    const retention_pressure = observed.primary.retention.reseed_recommended > 0;
    const degraded = isDegraded(observed.primary.durability);
    const automatic_standby = automaticPromotionStandby(spec, observed);
    const automatic_allowed = automatic_standby != null;

    try conditions.append(alloc, .{
        .type = .available,
        .status = healthy_count > 0 or desired_count == 0,
        .severity = if (healthy_count > 0 or desired_count == 0) .info else .warning,
        .reason = if (healthy_count > 0) "HealthyStandbyAvailable" else "NoHealthyStandby",
        .message = if (healthy_count > 0) "At least one desired standby is caught up to apply" else "No desired standby is caught up to apply",
    });
    try conditions.append(alloc, .{
        .type = .degraded,
        .status = degraded,
        .severity = if (degraded) .critical else .info,
        .reason = if (degraded) "SyncPolicyUnsatisfied" else "SyncPolicySatisfied",
        .message = if (degraded) "Synchronous HA policy is not currently satisfied" else "Synchronous HA policy is satisfied or not configured",
    });
    try conditions.append(alloc, .{
        .type = .unhealthy,
        .status = unhealthy_count > 0,
        .severity = if (replication_error_count > 0) .critical else if (unhealthy_count > 0) .warning else .info,
        .reason = unhealthyConditionReason(replication_error_count, inactive_count),
        .message = if (replication_error_count > 0)
            "One or more desired standbys have a recorded replication or apply error"
        else if (inactive_count > 0)
            "One or more desired standby slots are inactive"
        else
            "No desired standby has a recorded unhealthy replication state",
    });
    try conditions.append(alloc, .{
        .type = .retention_pressure,
        .status = retention_pressure,
        .severity = if (retention_pressure) .warning else .info,
        .reason = if (retention_pressure) "RetentionCapExceeded" else "RetentionWithinPolicy",
        .message = if (retention_pressure) "One or more slots are forcing WAL retention beyond policy" else "WAL retention is within configured policy",
    });
    try conditions.append(alloc, .{
        .type = .lagging,
        .status = lagging_count > 0,
        .severity = if (lagging_count > 0) .warning else .info,
        .reason = if (lagging_count > 0) "StandbyLagExceedsPolicy" else "NoLaggingStandby",
        .message = if (lagging_count > 0) "One or more desired standbys exceed configured lag policy" else "No desired standby exceeds configured lag policy",
    });
    try conditions.append(alloc, .{
        .type = .reseed_required,
        .status = reseed_count > 0,
        .severity = if (reseed_count > 0) .warning else .info,
        .reason = if (reseed_count > 0) "StandbyRequiresReseed" else "NoReseedRequired",
        .message = if (reseed_count > 0) "One or more desired standbys must be reseeded" else "No desired standby requires reseed",
    });
    try conditions.append(alloc, .{
        .type = .automatic_failover_ready,
        .status = automatic_allowed,
        .severity = if (automatic_allowed or !spec.auto_failover.enabled) .info else .warning,
        .reason = automaticFailoverReason(spec, observed, automatic_allowed),
        .message = if (automatic_allowed) "Automatic failover may acquire a fence and promote a caught-up standby" else "Automatic failover is disabled or missing a safe fencing/readiness prerequisite",
    });

    if (automatic_allowed) {
        const promoted_node_id = automatic_standby orelse unreachable;
        const fence_precondition = automaticFencingPrecondition(observed.fencing) orelse unreachable;
        try actions.append(alloc, .{
            .kind = .acquire_fence,
            .phase = .fence,
            .fencing_precondition = fence_precondition,
            .standby_name = promoted_node_id,
            .target_lsn = observed.primary.current_lsn,
            .reason = "AutomaticFailoverReady",
        });
        try actions.append(alloc, .{
            .kind = .promote_standby,
            .phase = .promote,
            .depends_on = .acquire_fence,
            .fencing_precondition = fence_precondition,
            .standby_name = promoted_node_id,
            .target_lsn = observed.primary.current_lsn,
            .reason = "AutomaticFailoverReady",
        });
        try actions.append(alloc, .{
            .kind = .update_primary_endpoint,
            .phase = .route,
            .executor = .controller_action,
            .depends_on = .promote_standby,
            .fencing_precondition = fence_precondition,
            .standby_name = promoted_node_id,
            .route_from = observed.current_primary_id,
            .route_to = promoted_node_id,
            .target_lsn = observed.primary.current_lsn,
            .reason = "PromotionPlanned",
        });
        if (observed.current_primary_id) |current_primary_id| {
            try actions.append(alloc, .{
                .kind = .demote_former_primary,
                .phase = .rejoin,
                .depends_on = .promote_standby,
                .fencing_precondition = fence_precondition,
                .standby_name = current_primary_id,
                .target_lsn = observed.primary.current_lsn,
                .reason = "PromotionPlanned",
            });
        }
    }

    const former_primary_assessment = if (observed.former_primary) |former| blk: {
        const assessment = rejoin.assessFormerPrimary(former, observed.promotion_receipt, observed.rejoin_policy);
        try appendFormerPrimaryAction(alloc, &actions, assessment);
        break :blk assessment;
    } else null;

    return .{
        .actions = try actions.toOwnedSlice(alloc),
        .conditions = try conditions.toOwnedSlice(alloc),
        .fencing = observed.fencing,
        .former_primary_assessment = former_primary_assessment,
        .automatic_promotion_allowed = automatic_allowed,
        .desired_standby_count = desired_count,
        .healthy_standby_count = healthy_count,
        .unhealthy_standby_count = unhealthy_count,
        .lagging_standby_count = lagging_count,
        .reseed_required_count = reseed_count,
    };
}

pub fn adminCommandForAction(
    alloc: Allocator,
    action: Action,
    identity: primary_mod.Identity,
    options: AdminCommandOptions,
) !?AdminCommand {
    if (action.executor == .controller_action) return null;

    var argv = std.ArrayListUnmanaged([]const u8).empty;
    errdefer argv.deinit(alloc);
    var owned_args = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (owned_args.items) |arg| alloc.free(arg);
        owned_args.deinit(alloc);
    }

    switch (action.kind) {
        .create_slot => {
            const slot_name = action.slot_name orelse action.standby_name orelse return error.SlotNameMissing;
            try appendArg(alloc, &argv, "slot");
            try appendArg(alloc, &argv, "create");
            try appendArg(alloc, &argv, "--slot");
            try appendArg(alloc, &argv, slot_name);
            if (action.target_lsn) |target_lsn| {
                try appendArg(alloc, &argv, "--initial-lsn");
                try appendOwnedFmt(alloc, &argv, &owned_args, "{d}", .{target_lsn});
            }
        },
        .resume_slot => try appendSlotLifecycleCommand(alloc, &argv, "resume", action),
        .pause_slot => try appendSlotLifecycleCommand(alloc, &argv, "pause", action),
        .drop_slot => try appendSlotLifecycleCommand(alloc, &argv, "drop", action),
        .seed_standby, .mark_reseed => {
            const standby_name = action.standby_name orelse action.slot_name orelse return error.StandbyNameMissing;
            try appendArg(alloc, &argv, "seed");
            try appendArg(alloc, &argv, "begin");
            try appendArg(alloc, &argv, "--slot");
            try appendArg(alloc, &argv, standby_name);
            try appendArg(alloc, &argv, "--manifest-id");
            try appendOwnedFmt(
                alloc,
                &argv,
                &owned_args,
                "{s}-{s}-{d}",
                .{ options.manifest_id_prefix, standby_name, action.target_lsn orelse 0 },
            );
        },
        .acquire_fence => {
            const promoted_node_id = options.promoted_node_id orelse action.standby_name orelse return error.PromotedNodeIdMissing;
            const old_primary_id = options.old_primary_id orelse return error.OldPrimaryIdMissing;
            try appendArg(alloc, &argv, "fence");
            try appendArg(alloc, &argv, "acquire");
            try appendIdentityArgs(alloc, &argv, &owned_args, identity);
            try appendArg(alloc, &argv, "--old-primary-id");
            try appendArg(alloc, &argv, old_primary_id);
            try appendArg(alloc, &argv, "--promoted-node-id");
            try appendArg(alloc, &argv, promoted_node_id);
            try appendArg(alloc, &argv, "--new-timeline-id");
            try appendOwnedFmt(alloc, &argv, &owned_args, "{d}", .{options.new_timeline_id orelse return error.NewTimelineIdMissing});
            try appendArg(alloc, &argv, "--new-epoch");
            try appendOwnedFmt(alloc, &argv, &owned_args, "{d}", .{options.new_epoch orelse return error.NewEpochMissing});
            try appendArg(alloc, &argv, "--required-lsn");
            try appendOwnedFmt(alloc, &argv, &owned_args, "{d}", .{action.target_lsn orelse return error.RequiredLsnMissing});
            try appendArg(alloc, &argv, "--observed-lsn");
            try appendOwnedFmt(alloc, &argv, &owned_args, "{d}", .{action.target_lsn orelse return error.ObservedLsnMissing});
            if (options.force) try appendArg(alloc, &argv, "--force");
            try appendArg(alloc, &argv, "--reason");
            try appendArg(alloc, &argv, options.reason);
        },
        .promote_standby => {
            try appendArg(alloc, &argv, "promote");
            if (options.promote_from_current_fence) {
                try appendArg(alloc, &argv, "--current-fence");
            } else {
                const promoted_node_id = options.promoted_node_id orelse action.standby_name orelse return error.PromotedNodeIdMissing;
                const old_primary_id = options.old_primary_id orelse return error.OldPrimaryIdMissing;
                try appendIdentityArgs(alloc, &argv, &owned_args, identity);
                try appendArg(alloc, &argv, "--old-primary-id");
                try appendArg(alloc, &argv, old_primary_id);
                try appendArg(alloc, &argv, "--promoted-node-id");
                try appendArg(alloc, &argv, promoted_node_id);
                try appendArg(alloc, &argv, "--new-timeline-id");
                try appendOwnedFmt(alloc, &argv, &owned_args, "{d}", .{options.new_timeline_id orelse return error.NewTimelineIdMissing});
                try appendArg(alloc, &argv, "--new-epoch");
                try appendOwnedFmt(alloc, &argv, &owned_args, "{d}", .{options.new_epoch orelse return error.NewEpochMissing});
                try appendArg(alloc, &argv, "--required-lsn");
                try appendOwnedFmt(alloc, &argv, &owned_args, "{d}", .{action.target_lsn orelse return error.RequiredLsnMissing});
                try appendArg(alloc, &argv, "--observed-lsn");
                try appendOwnedFmt(alloc, &argv, &owned_args, "{d}", .{action.target_lsn orelse return error.ObservedLsnMissing});
                if (options.force) try appendArg(alloc, &argv, "--force");
                try appendArg(alloc, &argv, "--reason");
                try appendArg(alloc, &argv, options.reason);
            }
        },
        .update_primary_endpoint,
        => return null,
        .demote_former_primary,
        .rewind_former_primary,
        .reseed_former_primary,
        => try appendFormerPrimaryRejoinCommand(alloc, &argv, &owned_args, action, identity, options),
    }

    const argv_slice = try argv.toOwnedSlice(alloc);
    errdefer alloc.free(argv_slice);
    const owned_slice = try owned_args.toOwnedSlice(alloc);
    return .{
        .argv = argv_slice,
        .owned_args = owned_slice,
    };
}

fn appendFormerPrimaryRejoinCommand(
    alloc: Allocator,
    argv: *std.ArrayListUnmanaged([]const u8),
    owned_args: *std.ArrayListUnmanaged([]u8),
    action: Action,
    identity: primary_mod.Identity,
    options: AdminCommandOptions,
) !void {
    const node_id = action.standby_name orelse return error.FormerPrimaryNodeIdMissing;
    const target_lsn = action.target_lsn orelse return error.RejoinTargetLsnMissing;
    const last_lsn = options.former_last_lsn orelse target_lsn;

    try appendArg(alloc, argv, "rejoin");
    try appendArg(alloc, argv, "assess");
    try appendArg(alloc, argv, "--node-id");
    try appendArg(alloc, argv, node_id);
    try appendIdentityArgs(alloc, argv, owned_args, identity);
    try appendArg(alloc, argv, "--last-lsn");
    try appendOwnedFmt(alloc, argv, owned_args, "{d}", .{last_lsn});
    try appendArg(alloc, argv, "--retained-from-lsn");
    try appendOwnedFmt(alloc, argv, owned_args, "{d}", .{options.retained_from_lsn});
    if (options.allow_forced_rewind) try appendArg(alloc, argv, "--allow-forced-rewind");

    switch (action.kind) {
        .demote_former_primary => return,
        .rewind_former_primary,
        .reseed_former_primary,
        => {},
        else => return error.InvalidFormerPrimaryAction,
    }

    const old_primary_id = options.old_primary_id orelse node_id;
    const promoted_node_id = options.promoted_node_id orelse return error.PromotedNodeIdMissing;
    const new_timeline_id = options.new_timeline_id orelse return error.NewTimelineIdMissing;
    const new_epoch = options.new_epoch orelse return error.NewEpochMissing;
    const fence_generation = options.fence_generation orelse return error.FenceGenerationMissing;
    const fence_token = options.fence_token orelse return error.FenceTokenMissing;

    try appendArg(alloc, argv, "--fence-old-primary-id");
    try appendArg(alloc, argv, old_primary_id);
    try appendArg(alloc, argv, "--fence-promoted-node-id");
    try appendArg(alloc, argv, promoted_node_id);
    try appendArg(alloc, argv, "--fence-parent-timeline-id");
    try appendOwnedFmt(alloc, argv, owned_args, "{d}", .{identity.timeline_id});
    try appendArg(alloc, argv, "--fence-parent-epoch");
    try appendOwnedFmt(alloc, argv, owned_args, "{d}", .{identity.epoch});
    try appendArg(alloc, argv, "--fence-new-timeline-id");
    try appendOwnedFmt(alloc, argv, owned_args, "{d}", .{new_timeline_id});
    try appendArg(alloc, argv, "--fence-new-epoch");
    try appendOwnedFmt(alloc, argv, owned_args, "{d}", .{new_epoch});
    try appendArg(alloc, argv, "--fence-required-lsn");
    try appendOwnedFmt(alloc, argv, owned_args, "{d}", .{target_lsn});
    try appendArg(alloc, argv, "--fence-observed-lsn");
    try appendOwnedFmt(alloc, argv, owned_args, "{d}", .{target_lsn});
    try appendArg(alloc, argv, "--fence-generation");
    try appendOwnedFmt(alloc, argv, owned_args, "{d}", .{fence_generation});
    try appendArg(alloc, argv, "--fence-token");
    try appendArg(alloc, argv, fence_token);
    try appendArg(alloc, argv, "--fence-reason");
    try appendArg(alloc, argv, options.reason);
    if (options.force) try appendArg(alloc, argv, "--fence-forced");
}

fn appendFormerPrimaryAction(
    alloc: Allocator,
    actions: *std.ArrayListUnmanaged(Action),
    assessment: rejoin.Assessment,
) !void {
    switch (assessment.action) {
        .reject_unfenced => try actions.append(alloc, .{
            .kind = .demote_former_primary,
            .phase = .rejoin,
            .standby_name = assessment.former_node_id,
            .target_lsn = assessment.former_last_lsn,
            .reason = "FormerPrimaryRejectedUnfenced",
        }),
        .already_current => {},
        .rewind => try actions.append(alloc, .{
            .kind = .rewind_former_primary,
            .phase = .rejoin,
            .standby_name = assessment.former_node_id,
            .target_lsn = assessment.fork_lsn,
            .reason = "FormerPrimaryCanRewind",
        }),
        .reseed => try actions.append(alloc, .{
            .kind = .reseed_former_primary,
            .phase = .rejoin,
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

fn unhealthyConditionReason(replication_error_count: usize, inactive_count: usize) []const u8 {
    if (replication_error_count > 0) return "StandbyReplicationError";
    if (inactive_count > 0) return "StandbyInactive";
    return "NoUnhealthyStandby";
}

fn automaticPromotionAllowed(spec: Spec, observed: Observed) bool {
    return automaticPromotionStandby(spec, observed) != null;
}

fn automaticPromotionStandby(spec: Spec, observed: Observed) ?[]const u8 {
    const primary = observed.primary;
    const fencing_observed = observed.fencing;
    if (!spec.auto_failover.enabled) return null;
    if (spec.auto_failover.fencing_authority == .none) return null;
    if (!fencingAuthorityReady(spec.auto_failover, fencing_observed)) return null;
    if (!observed.primary_admin_unavailable) return null;
    if (primary.current_lsn == 0) return null;
    if (promotionAlreadyRecorded(observed)) return null;
    if (isDegraded(primary.durability)) return null;
    const fence_holder = fencing_observed.holder orelse return null;
    _ = fencing_observed.generation orelse return null;

    const max_lag = spec.auto_failover.maximum_lag_lsn;
    for (spec.standbys) |standby| {
        if (!standby.desired) continue;
        if (!std.mem.eql(u8, standby.name, fence_holder)) continue;
        const slot = findSlot(primary.slots, standby.name) orelse continue;
        if (!slot.active or slot.reseed_required) continue;
        if (slot.status == .reseed_required) continue;
        if (slot.last_error != null) continue;
        if (exceedsLagPolicy(slot.received_lsn, primary.current_lsn, max_lag)) continue;
        if (spec.auto_failover.require_remote_apply) {
            if (exceedsLagPolicy(slot.applied_lsn, primary.current_lsn, max_lag)) continue;
        }
        if (exceedsLagPolicy(slot.safe_read_lsn, primary.current_lsn, max_lag)) continue;
        return standby.name;
    }
    return null;
}

fn promotionAlreadyRecorded(observed: Observed) bool {
    const receipt = observed.promotion_receipt orelse return false;
    const identity = observed.primary.identity;
    return receipt.identity.cluster_id == identity.cluster_id and
        receipt.identity.shard_id == identity.shard_id and
        receipt.identity.table_id == identity.table_id and
        receipt.parent_timeline_id == identity.timeline_id and
        receipt.parent_epoch == identity.epoch and
        receipt.old_primary_id.len > 0 and
        receipt.promoted_node_id.len > 0 and
        receipt.new_timeline_id != 0 and
        receipt.new_epoch != 0 and
        receipt.required_lsn != 0;
}

fn fencingAuthorityReady(policy: AutoFailoverPolicy, observed: FencingObservation) bool {
    if (policy.fencing_authority == .none) return false;
    if (observed.authority != policy.fencing_authority) return false;
    return observed.ready;
}

fn automaticFencingPrecondition(observed: FencingObservation) ?FencingPrecondition {
    if (!observed.ready) return null;
    const holder = observed.holder orelse return null;
    const generation = observed.generation orelse return null;
    return .{
        .authority = observed.authority,
        .holder = holder,
        .generation = generation,
        .reason = observed.reason,
    };
}

fn appendSlotLifecycleCommand(
    alloc: Allocator,
    argv: *std.ArrayListUnmanaged([]const u8),
    action_name: []const u8,
    action: Action,
) !void {
    const slot_name = action.slot_name orelse action.standby_name orelse return error.SlotNameMissing;
    try appendArg(alloc, argv, "slot");
    try appendArg(alloc, argv, action_name);
    try appendArg(alloc, argv, "--slot");
    try appendArg(alloc, argv, slot_name);
}

fn appendIdentityArgs(
    alloc: Allocator,
    argv: *std.ArrayListUnmanaged([]const u8),
    owned_args: *std.ArrayListUnmanaged([]u8),
    identity: primary_mod.Identity,
) !void {
    try appendArg(alloc, argv, "--cluster-id");
    try appendOwnedFmt(alloc, argv, owned_args, "{d}", .{identity.cluster_id});
    try appendArg(alloc, argv, "--shard-id");
    try appendOwnedFmt(alloc, argv, owned_args, "{d}", .{identity.shard_id});
    try appendArg(alloc, argv, "--table-id");
    try appendOwnedFmt(alloc, argv, owned_args, "{d}", .{identity.table_id});
    try appendArg(alloc, argv, "--timeline-id");
    try appendOwnedFmt(alloc, argv, owned_args, "{d}", .{identity.timeline_id});
    try appendArg(alloc, argv, "--epoch");
    try appendOwnedFmt(alloc, argv, owned_args, "{d}", .{identity.epoch});
}

fn appendArg(
    alloc: Allocator,
    argv: *std.ArrayListUnmanaged([]const u8),
    arg: []const u8,
) !void {
    try argv.append(alloc, arg);
}

fn appendOwnedFmt(
    alloc: Allocator,
    argv: *std.ArrayListUnmanaged([]const u8),
    owned_args: *std.ArrayListUnmanaged([]u8),
    comptime fmt: []const u8,
    args: anytype,
) !void {
    const owned = try std.fmt.allocPrint(alloc, fmt, args);
    errdefer alloc.free(owned);
    try owned_args.append(alloc, owned);
    try argv.append(alloc, owned);
}

fn automaticFailoverReason(spec: Spec, observed: Observed, allowed: bool) []const u8 {
    const primary = observed.primary;
    const fencing_observed = observed.fencing;
    if (allowed) return "FencedPromotionReady";
    if (!spec.auto_failover.enabled) return "AutomaticFailoverDisabled";
    if (spec.auto_failover.fencing_authority == .none) return "FencingAuthorityMissing";
    if (fencing_observed.authority != .none and fencing_observed.authority != spec.auto_failover.fencing_authority) return "FencingAuthorityMismatch";
    if (!fencing_observed.ready) return "FencingAuthorityNotReady";
    if (!observed.primary_admin_unavailable) return "PrimaryStillReachable";
    if (primary.current_lsn == 0) return "PromotionBoundaryMissing";
    if (promotionAlreadyRecorded(observed)) return "PromotionAlreadyRecorded";
    if (isDegraded(primary.durability)) return "SyncPolicyUnsatisfied";
    const fence_holder = fencing_observed.holder orelse return "FencingHolderMissing";
    _ = fencing_observed.generation orelse return "FencingGenerationMissing";
    if (!desiredStandbyNamed(spec, fence_holder)) return "FencingHolderNotDesired";
    if (automaticFailoverSlotReason(spec, primary, fence_holder)) |reason| return reason;
    return "NoEligibleStandby";
}

fn automaticFailoverSlotReason(spec: Spec, primary: status.PrimarySnapshot, standby_name: []const u8) ?[]const u8 {
    const slot = findSlot(primary.slots, standby_name) orelse return "StandbySlotMissing";
    if (!slot.active) return "StandbySlotInactive";
    if (slot.reseed_required or slot.status == .reseed_required) return "StandbyRequiresReseed";
    if (slot.last_error != null) return "StandbyReplicationError";
    const max_lag = spec.auto_failover.maximum_lag_lsn;
    if (exceedsLagPolicy(slot.received_lsn, primary.current_lsn, max_lag)) return "StandbyReceiveLagExceedsPolicy";
    if (spec.auto_failover.require_remote_apply and
        exceedsLagPolicy(slot.applied_lsn, primary.current_lsn, max_lag))
    {
        return "StandbyApplyLagExceedsPolicy";
    }
    if (exceedsLagPolicy(slot.safe_read_lsn, primary.current_lsn, max_lag)) return "StandbySafeReadLagExceedsPolicy";
    return null;
}

fn exceedsLagPolicy(progress_lsn: u64, target_lsn: u64, max_lag_lsn: u64) bool {
    return progress_lsn +| max_lag_lsn < target_lsn;
}

fn desiredStandbyNamed(spec: Spec, name: []const u8) bool {
    for (spec.standbys) |standby| {
        if (standby.desired and std.mem.eql(u8, standby.name, name)) return true;
    }
    return false;
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

    try std.testing.expectEqual(@as(usize, 1), plan.actions.len);
    try std.testing.expectEqual(ActionKind.seed_standby, plan.actions[0].kind);
    try std.testing.expectEqual(@as(?u64, 3), plan.actions[0].target_lsn);
    try std.testing.expectEqual(@as(usize, 1), plan.desired_standby_count);
    try std.testing.expectEqual(@as(usize, 0), plan.healthy_standby_count);
    const available = condition(plan, .available) orelse return error.TestExpectedEqual;
    try std.testing.expect(!available.status);
    try std.testing.expectEqual(ConditionSeverity.warning, available.severity);
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
            .safe_read_lsn = 1,
            .write_lag_lsn = 8,
            .apply_lag_lsn = 9,
            .safe_read_lag_lsn = 9,
            .retention_lag_lsn = 9,
            .status = .reseed_required,
            .last_error = "IntentionalApplyFailure",
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
            .progress_lsn = 1,
            .missing_lsn_count = 9,
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
    }, .{
        .primary = primary,
        .primary_admin_unavailable = true,
        .fencing = .{
            .authority = .kubernetes_lease,
            .ready = true,
            .holder = "standby-a",
            .generation = 6,
            .reason = "LeaseAcquired",
        },
    });
    defer plan.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), plan.reseed_required_count);
    try std.testing.expectEqual(@as(usize, 1), plan.unhealthy_standby_count);
    try std.testing.expectEqual(ActionKind.mark_reseed, plan.actions[0].kind);
    const retention_pressure = condition(plan, .retention_pressure) orelse return error.TestExpectedEqual;
    try std.testing.expect(retention_pressure.status);
    try std.testing.expectEqual(ConditionSeverity.warning, retention_pressure.severity);
    const degraded_condition = condition(plan, .degraded) orelse return error.TestExpectedEqual;
    try std.testing.expect(degraded_condition.status);
    try std.testing.expectEqual(ConditionSeverity.critical, degraded_condition.severity);
    const unhealthy_condition = condition(plan, .unhealthy) orelse return error.TestExpectedEqual;
    try std.testing.expect(unhealthy_condition.status);
    try std.testing.expectEqual(ConditionSeverity.critical, unhealthy_condition.severity);
    try std.testing.expectEqualStrings("StandbyReplicationError", unhealthy_condition.reason);
    const reseed_condition = condition(plan, .reseed_required) orelse return error.TestExpectedEqual;
    try std.testing.expect(reseed_condition.status);
    try std.testing.expectEqual(ConditionSeverity.warning, reseed_condition.severity);
    const failover = condition(plan, .automatic_failover_ready) orelse return error.TestExpectedEqual;
    try std.testing.expect(!failover.status);
    try std.testing.expectEqual(ConditionSeverity.warning, failover.severity);
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
            .safe_read_lsn = 12,
            .write_lag_lsn = 0,
            .apply_lag_lsn = 0,
            .safe_read_lag_lsn = 0,
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
            .progress_lsn = 12,
            .missing_lsn_count = 0,
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

    var unobserved = try reconcile(alloc, .{
        .mode = .hot_standby,
        .standbys = &standbys,
        .auto_failover = .{
            .enabled = true,
            .fencing_authority = .kubernetes_lease,
            .maximum_lag_lsn = 0,
        },
    }, .{ .primary = primary });
    defer unobserved.deinit(alloc);
    try std.testing.expect(!unobserved.automatic_promotion_allowed);
    try std.testing.expectEqualStrings(
        "FencingAuthorityNotReady",
        (condition(unobserved, .automatic_failover_ready) orelse return error.TestExpectedEqual).reason,
    );

    var holder_missing = try reconcile(alloc, .{
        .mode = .hot_standby,
        .standbys = &standbys,
        .auto_failover = .{
            .enabled = true,
            .fencing_authority = .kubernetes_lease,
            .maximum_lag_lsn = 0,
        },
    }, .{
        .primary = primary,
        .primary_admin_unavailable = true,
        .fencing = .{
            .authority = .kubernetes_lease,
            .ready = true,
            .generation = 6,
            .reason = "LeaseAcquired",
        },
    });
    defer holder_missing.deinit(alloc);
    try std.testing.expect(!holder_missing.automatic_promotion_allowed);
    try std.testing.expectEqualStrings(
        "FencingHolderMissing",
        (condition(holder_missing, .automatic_failover_ready) orelse return error.TestExpectedEqual).reason,
    );

    var wrong_holder = try reconcile(alloc, .{
        .mode = .hot_standby,
        .standbys = &standbys,
        .auto_failover = .{
            .enabled = true,
            .fencing_authority = .kubernetes_lease,
            .maximum_lag_lsn = 0,
        },
    }, .{
        .primary = primary,
        .primary_admin_unavailable = true,
        .fencing = .{
            .authority = .kubernetes_lease,
            .ready = true,
            .holder = "standby-b",
            .generation = 6,
            .reason = "LeaseAcquired",
        },
    });
    defer wrong_holder.deinit(alloc);
    try std.testing.expect(!wrong_holder.automatic_promotion_allowed);
    try std.testing.expectEqualStrings(
        "FencingHolderNotDesired",
        (condition(wrong_holder, .automatic_failover_ready) orelse return error.TestExpectedEqual).reason,
    );

    var generation_missing = try reconcile(alloc, .{
        .mode = .hot_standby,
        .standbys = &standbys,
        .auto_failover = .{
            .enabled = true,
            .fencing_authority = .kubernetes_lease,
            .maximum_lag_lsn = 0,
        },
    }, .{
        .primary = primary,
        .primary_admin_unavailable = true,
        .fencing = .{
            .authority = .kubernetes_lease,
            .ready = true,
            .holder = "standby-a",
            .reason = "LeaseAcquired",
        },
    });
    defer generation_missing.deinit(alloc);
    try std.testing.expect(!generation_missing.automatic_promotion_allowed);
    try std.testing.expectEqualStrings(
        "FencingGenerationMissing",
        (condition(generation_missing, .automatic_failover_ready) orelse return error.TestExpectedEqual).reason,
    );

    var primary_reachable = try reconcile(alloc, .{
        .mode = .hot_standby,
        .standbys = &standbys,
        .auto_failover = .{
            .enabled = true,
            .fencing_authority = .kubernetes_lease,
            .maximum_lag_lsn = 0,
        },
    }, .{
        .primary = primary,
        .fencing = .{
            .authority = .kubernetes_lease,
            .ready = true,
            .holder = "standby-a",
            .generation = 6,
            .reason = "LeaseAcquired",
        },
    });
    defer primary_reachable.deinit(alloc);
    try std.testing.expect(!primary_reachable.automatic_promotion_allowed);
    try std.testing.expectEqualStrings(
        "PrimaryStillReachable",
        (condition(primary_reachable, .automatic_failover_ready) orelse return error.TestExpectedEqual).reason,
    );

    var missing_boundary_primary = primary;
    missing_boundary_primary.current_lsn = 0;
    missing_boundary_primary.retention.primary_lsn = 0;
    var missing_boundary = try reconcile(alloc, .{
        .mode = .hot_standby,
        .standbys = &standbys,
        .auto_failover = .{
            .enabled = true,
            .fencing_authority = .kubernetes_lease,
            .maximum_lag_lsn = 0,
        },
    }, .{
        .primary = missing_boundary_primary,
        .primary_admin_unavailable = true,
        .fencing = .{
            .authority = .kubernetes_lease,
            .ready = true,
            .holder = "standby-a",
            .generation = 6,
            .reason = "LeaseAcquired",
        },
    });
    defer missing_boundary.deinit(alloc);
    try std.testing.expect(!missing_boundary.automatic_promotion_allowed);
    try std.testing.expectEqualStrings(
        "PromotionBoundaryMissing",
        (condition(missing_boundary, .automatic_failover_ready) orelse return error.TestExpectedEqual).reason,
    );

    const recorded_receipt = fencing.Receipt{
        .identity = primary.identity,
        .old_primary_id = "primary-a",
        .promoted_node_id = "standby-a",
        .parent_timeline_id = primary.identity.timeline_id,
        .parent_epoch = primary.identity.epoch,
        .new_timeline_id = primary.identity.timeline_id + 1,
        .new_epoch = primary.identity.epoch + 1,
        .required_lsn = primary.current_lsn,
        .observed_lsn = primary.current_lsn,
        .generation = 6,
        .forced = false,
        .token = "token",
        .reason = "LeaseAcquired",
    };
    var already_recorded = try reconcile(alloc, .{
        .mode = .hot_standby,
        .standbys = &standbys,
        .auto_failover = .{
            .enabled = true,
            .fencing_authority = .kubernetes_lease,
            .maximum_lag_lsn = 0,
        },
    }, .{
        .primary = primary,
        .primary_admin_unavailable = true,
        .fencing = .{
            .authority = .kubernetes_lease,
            .ready = true,
            .holder = "standby-a",
            .generation = 6,
            .reason = "LeaseAcquired",
        },
        .promotion_receipt = recorded_receipt,
    });
    defer already_recorded.deinit(alloc);
    try std.testing.expect(!already_recorded.automatic_promotion_allowed);
    try std.testing.expectEqualStrings(
        "PromotionAlreadyRecorded",
        (condition(already_recorded, .automatic_failover_ready) orelse return error.TestExpectedEqual).reason,
    );

    var inactive_slots = slots;
    inactive_slots[0].active = false;
    var inactive_primary = primary;
    inactive_primary.slots = inactive_slots[0..];
    var inactive = try reconcile(alloc, .{
        .mode = .hot_standby,
        .standbys = &standbys,
        .auto_failover = .{
            .enabled = true,
            .fencing_authority = .kubernetes_lease,
            .maximum_lag_lsn = 0,
        },
    }, .{
        .primary = inactive_primary,
        .primary_admin_unavailable = true,
        .fencing = .{
            .authority = .kubernetes_lease,
            .ready = true,
            .holder = "standby-a",
            .generation = 6,
            .reason = "LeaseAcquired",
        },
    });
    defer inactive.deinit(alloc);
    try std.testing.expect(!inactive.automatic_promotion_allowed);
    try std.testing.expectEqualStrings(
        "StandbySlotInactive",
        (condition(inactive, .automatic_failover_ready) orelse return error.TestExpectedEqual).reason,
    );
    const inactive_unhealthy = condition(inactive, .unhealthy) orelse return error.TestExpectedEqual;
    try std.testing.expect(inactive_unhealthy.status);
    try std.testing.expectEqual(ConditionSeverity.warning, inactive_unhealthy.severity);
    try std.testing.expectEqualStrings("StandbyInactive", inactive_unhealthy.reason);

    var error_slots = slots;
    error_slots[0].last_error = "IntentionalApplyFailure";
    var error_primary = primary;
    error_primary.slots = error_slots[0..];
    var replication_error = try reconcile(alloc, .{
        .mode = .hot_standby,
        .standbys = &standbys,
        .auto_failover = .{
            .enabled = true,
            .fencing_authority = .kubernetes_lease,
            .maximum_lag_lsn = 0,
        },
    }, .{
        .primary = error_primary,
        .primary_admin_unavailable = true,
        .fencing = .{
            .authority = .kubernetes_lease,
            .ready = true,
            .holder = "standby-a",
            .generation = 6,
            .reason = "LeaseAcquired",
        },
    });
    defer replication_error.deinit(alloc);
    try std.testing.expect(!replication_error.automatic_promotion_allowed);
    try std.testing.expectEqualStrings(
        "StandbyReplicationError",
        (condition(replication_error, .automatic_failover_ready) orelse return error.TestExpectedEqual).reason,
    );
    const replication_unhealthy = condition(replication_error, .unhealthy) orelse return error.TestExpectedEqual;
    try std.testing.expect(replication_unhealthy.status);
    try std.testing.expectEqual(ConditionSeverity.critical, replication_unhealthy.severity);

    var receive_lag_slots = slots;
    receive_lag_slots[0].received_lsn = 11;
    receive_lag_slots[0].applied_lsn = 11;
    receive_lag_slots[0].write_lag_lsn = 1;
    receive_lag_slots[0].apply_lag_lsn = 1;
    receive_lag_slots[0].status = .lagging;
    var receive_lag_primary = primary;
    receive_lag_primary.slots = receive_lag_slots[0..];
    var receive_lag = try reconcile(alloc, .{
        .mode = .hot_standby,
        .standbys = &standbys,
        .auto_failover = .{
            .enabled = true,
            .fencing_authority = .kubernetes_lease,
            .maximum_lag_lsn = 0,
        },
    }, .{
        .primary = receive_lag_primary,
        .primary_admin_unavailable = true,
        .fencing = .{
            .authority = .kubernetes_lease,
            .ready = true,
            .holder = "standby-a",
            .generation = 6,
            .reason = "LeaseAcquired",
        },
    });
    defer receive_lag.deinit(alloc);
    try std.testing.expect(!receive_lag.automatic_promotion_allowed);
    try std.testing.expectEqualStrings(
        "StandbyReceiveLagExceedsPolicy",
        (condition(receive_lag, .automatic_failover_ready) orelse return error.TestExpectedEqual).reason,
    );
    try std.testing.expectEqual(@as(usize, 1), receive_lag.lagging_standby_count);
    const lagging_condition = condition(receive_lag, .lagging) orelse return error.TestExpectedEqual;
    try std.testing.expect(lagging_condition.status);
    try std.testing.expectEqual(ConditionSeverity.warning, lagging_condition.severity);
    try std.testing.expectEqualStrings("StandbyLagExceedsPolicy", lagging_condition.reason);

    var apply_lag_slots = slots;
    apply_lag_slots[0].applied_lsn = 11;
    apply_lag_slots[0].apply_lag_lsn = 1;
    var apply_lag_primary = primary;
    apply_lag_primary.slots = apply_lag_slots[0..];
    var apply_lag = try reconcile(alloc, .{
        .mode = .hot_standby,
        .standbys = &standbys,
        .auto_failover = .{
            .enabled = true,
            .fencing_authority = .kubernetes_lease,
            .maximum_lag_lsn = 0,
        },
    }, .{
        .primary = apply_lag_primary,
        .primary_admin_unavailable = true,
        .fencing = .{
            .authority = .kubernetes_lease,
            .ready = true,
            .holder = "standby-a",
            .generation = 6,
            .reason = "LeaseAcquired",
        },
    });
    defer apply_lag.deinit(alloc);
    try std.testing.expect(!apply_lag.automatic_promotion_allowed);
    try std.testing.expectEqualStrings(
        "StandbyApplyLagExceedsPolicy",
        (condition(apply_lag, .automatic_failover_ready) orelse return error.TestExpectedEqual).reason,
    );

    var safe_read_lag_slots = slots;
    safe_read_lag_slots[0].safe_read_lsn = 11;
    safe_read_lag_slots[0].safe_read_lag_lsn = 1;
    var safe_read_lag_primary = primary;
    safe_read_lag_primary.slots = safe_read_lag_slots[0..];
    var safe_read_lag = try reconcile(alloc, .{
        .mode = .hot_standby,
        .standbys = &standbys,
        .auto_failover = .{
            .enabled = true,
            .fencing_authority = .kubernetes_lease,
            .maximum_lag_lsn = 0,
        },
    }, .{
        .primary = safe_read_lag_primary,
        .primary_admin_unavailable = true,
        .fencing = .{
            .authority = .kubernetes_lease,
            .ready = true,
            .holder = "standby-a",
            .generation = 6,
            .reason = "LeaseAcquired",
        },
    });
    defer safe_read_lag.deinit(alloc);
    try std.testing.expect(!safe_read_lag.automatic_promotion_allowed);
    try std.testing.expectEqualStrings(
        "StandbySafeReadLagExceedsPolicy",
        (condition(safe_read_lag, .automatic_failover_ready) orelse return error.TestExpectedEqual).reason,
    );

    var safe = try reconcile(alloc, .{
        .mode = .hot_standby,
        .standbys = &standbys,
        .auto_failover = .{
            .enabled = true,
            .fencing_authority = .kubernetes_lease,
            .maximum_lag_lsn = 0,
        },
    }, .{
        .primary = primary,
        .current_primary_id = "primary-a",
        .primary_admin_unavailable = true,
        .fencing = .{
            .authority = .kubernetes_lease,
            .ready = true,
            .holder = "standby-a",
            .generation = 7,
            .reason = "LeaseAcquired",
        },
    });
    defer safe.deinit(alloc);
    try std.testing.expect(safe.automatic_promotion_allowed);
    try std.testing.expectEqual(@as(usize, 4), safe.actions.len);
    try std.testing.expectEqual(ActionKind.acquire_fence, safe.actions[0].kind);
    try std.testing.expectEqual(ActionKind.promote_standby, safe.actions[1].kind);
    try std.testing.expectEqual(ActionKind.update_primary_endpoint, safe.actions[2].kind);
    try std.testing.expectEqual(ActionKind.demote_former_primary, safe.actions[3].kind);
    try std.testing.expectEqual(ActionPhase.fence, safe.actions[0].phase);
    try std.testing.expectEqual(ActionPhase.promote, safe.actions[1].phase);
    try std.testing.expectEqual(ActionPhase.route, safe.actions[2].phase);
    try std.testing.expectEqual(ActionPhase.rejoin, safe.actions[3].phase);
    try std.testing.expectEqual(ActionExecutor.admin_command, safe.actions[0].executor);
    try std.testing.expectEqual(ActionExecutor.admin_command, safe.actions[1].executor);
    try std.testing.expectEqual(ActionExecutor.controller_action, safe.actions[2].executor);
    try std.testing.expectEqual(ActionExecutor.admin_command, safe.actions[3].executor);
    try std.testing.expectEqual(@as(?ActionKind, null), safe.actions[0].depends_on);
    try std.testing.expectEqual(@as(?ActionKind, .acquire_fence), safe.actions[1].depends_on);
    try std.testing.expectEqual(@as(?ActionKind, .promote_standby), safe.actions[2].depends_on);
    try std.testing.expectEqual(@as(?ActionKind, .promote_standby), safe.actions[3].depends_on);
    for (safe.actions) |action| {
        const precondition = action.fencing_precondition orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(FencingAuthority.kubernetes_lease, precondition.authority);
        try std.testing.expectEqualStrings("standby-a", precondition.holder);
        try std.testing.expectEqual(@as(u64, 7), precondition.generation);
        try std.testing.expectEqualStrings("LeaseAcquired", precondition.reason);
    }
    try std.testing.expectEqualStrings("standby-a", safe.actions[2].standby_name.?);
    try std.testing.expectEqualStrings("primary-a", safe.actions[2].route_from.?);
    try std.testing.expectEqualStrings("standby-a", safe.actions[2].route_to.?);
    try std.testing.expectEqualStrings("primary-a", safe.actions[3].standby_name.?);
    try std.testing.expect((try adminCommandForAction(alloc, safe.actions[2], primary.identity, .{})) == null);
    try std.testing.expect((condition(safe, .automatic_failover_ready) orelse return error.TestExpectedEqual).status);
}

test "storage.ha operator renders versioned json plan for controllers" {
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
            .safe_read_lsn = 12,
            .write_lag_lsn = 0,
            .apply_lag_lsn = 0,
            .safe_read_lag_lsn = 0,
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
            .progress_lsn = 12,
            .missing_lsn_count = 0,
            .satisfied_count = 1,
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
    }, .{
        .primary = primary,
        .current_primary_id = "primary-a",
        .primary_admin_unavailable = true,
        .fencing = .{
            .authority = .kubernetes_lease,
            .ready = true,
            .holder = "standby-a",
            .generation = 11,
            .reason = "LeaseAcquired",
        },
    });
    defer plan.deinit(alloc);

    const rendered = try renderJsonAlloc(alloc, plan);
    defer alloc.free(rendered);

    try expectContains(rendered, "\"schema_version\":1");
    try expectContains(rendered, "\"automatic_promotion_allowed\":true");
    try expectContains(rendered, "\"fencing\"");
    try expectContains(rendered, "\"authority\":\"kubernetes_lease\"");
    try expectContains(rendered, "\"ready\":true");
    try expectContains(rendered, "\"desired_standby_count\":1");
    try expectContains(rendered, "\"healthy_standby_count\":1");
    try expectContains(rendered, "\"unhealthy_standby_count\":0");
    try expectContains(rendered, "\"lagging_standby_count\":0");
    try expectContains(rendered, "\"kind\":\"acquire_fence\"");
    try expectContains(rendered, "\"phase\":\"fence\"");
    try expectContains(rendered, "\"executor\":\"admin_command\"");
    try expectContains(rendered, "\"executor\":\"controller_action\"");
    try expectContains(rendered, "\"route_from\":\"primary-a\"");
    try expectContains(rendered, "\"route_to\":\"standby-a\"");
    try expectContains(rendered, "\"fencing_precondition\"");
    try expectContains(rendered, "\"generation\":11");
    try expectContains(rendered, "\"holder\":\"standby-a\"");
    try expectContains(rendered, "\"depends_on\":\"acquire_fence\"");
    try expectContains(rendered, "\"severity\":\"info\"");
    try expectContains(rendered, "\"type\":\"automatic_failover_ready\"");
    try expectContains(rendered, "\"reason\":\"FencedPromotionReady\"");
}

test "storage.ha operator renders executable admin commands for slot and seed actions" {
    const alloc = std.testing.allocator;
    const identity = primary_mod.Identity{ .cluster_id = 100, .shard_id = 10, .table_id = 20, .timeline_id = 1, .epoch = 1 };

    var create = (try adminCommandForAction(alloc, .{
        .kind = .create_slot,
        .standby_name = "standby-a",
        .slot_name = "standby-a",
        .target_lsn = 7,
        .reason = "SlotMissing",
    }, identity, .{})).?;
    defer create.deinit(alloc);
    var create_plan = try create.parsePlan(alloc);
    defer create_plan.deinit(alloc);
    try std.testing.expectEqual(admin_cli.OutputFormat.json, create_plan.output);
    try std.testing.expectEqualStrings("standby-a", create_plan.command.slot.request.slot_name);
    try std.testing.expectEqual(@as(?u64, 7), create_plan.command.slot.request.initial_lsn);

    var seed = (try adminCommandForAction(alloc, .{
        .kind = .seed_standby,
        .standby_name = "standby-a",
        .slot_name = "standby-a",
        .target_lsn = 7,
        .reason = "StandbyNeedsBaseBackup",
    }, identity, .{ .manifest_id_prefix = "operator-base" })).?;
    defer seed.deinit(alloc);
    var seed_plan = try seed.parsePlan(alloc);
    defer seed_plan.deinit(alloc);
    try std.testing.expectEqualStrings("standby-a", seed_plan.command.seed.begin.slot_name);
    try std.testing.expectEqualStrings("operator-base-standby-a-7", seed_plan.command.seed.begin.manifest_id);
}

test "storage.ha operator renders executable admin command for fenced promotion" {
    const alloc = std.testing.allocator;
    const identity = primary_mod.Identity{ .cluster_id = 100, .shard_id = 10, .table_id = 20, .timeline_id = 1, .epoch = 1 };

    var acquire = (try adminCommandForAction(alloc, .{
        .kind = .acquire_fence,
        .standby_name = "standby-a",
        .target_lsn = 12,
        .reason = "AutomaticFailoverReady",
    }, identity, .{
        .old_primary_id = "primary-a",
        .new_timeline_id = 2,
        .new_epoch = 2,
        .reason = "operator-approved",
    })).?;
    defer acquire.deinit(alloc);

    var acquire_plan = try acquire.parsePlan(alloc);
    defer acquire_plan.deinit(alloc);
    const acquire_fence = acquire_plan.command.fence_acquire;
    try std.testing.expectEqual(@as(u64, 100), acquire_fence.identity.cluster_id);
    try std.testing.expectEqualStrings("primary-a", acquire_fence.old_primary_id);
    try std.testing.expectEqualStrings("standby-a", acquire_fence.promoted_node_id);
    try std.testing.expectEqual(@as(u64, 2), acquire_fence.new_timeline_id);
    try std.testing.expectEqual(@as(u64, 12), acquire_fence.required_lsn);
    try std.testing.expectEqual(@as(u64, 12), acquire_fence.observed_lsn);
    try std.testing.expectEqualStrings("operator-approved", acquire_fence.reason);

    var command = (try adminCommandForAction(alloc, .{
        .kind = .promote_standby,
        .standby_name = "standby-a",
        .target_lsn = 12,
        .reason = "AutomaticFailoverReady",
    }, identity, .{
        .old_primary_id = "primary-a",
        .new_timeline_id = 2,
        .new_epoch = 2,
        .reason = "operator-approved",
    })).?;
    defer command.deinit(alloc);

    var plan = try command.parsePlan(alloc);
    defer plan.deinit(alloc);
    switch (plan.command) {
        .promote_current_fence => {},
        else => return error.TestExpectedEqual,
    }

    var explicit_command = (try adminCommandForAction(alloc, .{
        .kind = .promote_standby,
        .standby_name = "standby-a",
        .target_lsn = 12,
        .reason = "AutomaticFailoverReady",
    }, identity, .{
        .promote_from_current_fence = false,
        .old_primary_id = "primary-a",
        .new_timeline_id = 2,
        .new_epoch = 2,
        .reason = "operator-approved",
    })).?;
    defer explicit_command.deinit(alloc);

    var explicit_plan = try explicit_command.parsePlan(alloc);
    defer explicit_plan.deinit(alloc);
    const fence = explicit_plan.command.promote.fence;
    try std.testing.expectEqual(@as(u64, 100), fence.identity.cluster_id);
    try std.testing.expectEqualStrings("primary-a", fence.old_primary_id);
    try std.testing.expectEqualStrings("standby-a", fence.promoted_node_id);
    try std.testing.expectEqual(@as(u64, 2), fence.new_timeline_id);
    try std.testing.expectEqual(@as(u64, 12), fence.required_lsn);
    try std.testing.expectEqual(@as(u64, 12), fence.observed_lsn);
    try std.testing.expectEqualStrings("operator-approved", fence.reason);
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
    try std.testing.expectEqual(ActionPhase.rejoin, plan.actions[0].phase);
    try std.testing.expectEqualStrings("FormerPrimaryRejectedUnfenced", plan.actions[0].reason);

    var command = (try adminCommandForAction(alloc, plan.actions[0], former_identity, .{
        .retained_from_lsn = 10,
    })).?;
    defer command.deinit(alloc);
    var command_plan = try command.parsePlan(alloc);
    defer command_plan.deinit(alloc);
    try std.testing.expectEqualStrings("primary-a", command_plan.command.rejoin_assess.former.node_id);
    try std.testing.expectEqual(@as(u64, 2), command_plan.command.rejoin_assess.former.identity.timeline_id);
    try std.testing.expectEqual(@as(u64, 12), command_plan.command.rejoin_assess.former.last_lsn);
    try std.testing.expectEqual(@as(u64, 10), command_plan.command.rejoin_assess.policy.retained_from_lsn);
    try std.testing.expect(command_plan.command.rejoin_assess.receipt == null);
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
    try std.testing.expectEqual(ActionPhase.rejoin, rewind.actions[0].phase);
    try std.testing.expectEqual(@as(?u64, 10), rewind.actions[0].target_lsn);

    var rewind_command = (try adminCommandForAction(alloc, rewind.actions[0], parent_identity.identity, .{
        .old_primary_id = receipt.old_primary_id,
        .promoted_node_id = receipt.promoted_node_id,
        .new_timeline_id = receipt.new_timeline_id,
        .new_epoch = receipt.new_epoch,
        .former_last_lsn = 12,
        .retained_from_lsn = 8,
        .fence_generation = receipt.generation,
        .fence_token = receipt.token,
        .reason = receipt.reason,
    })).?;
    defer rewind_command.deinit(alloc);
    var rewind_command_plan = try rewind_command.parsePlan(alloc);
    defer rewind_command_plan.deinit(alloc);
    const rewind_rejoin = rewind_command_plan.command.rejoin_assess;
    try std.testing.expectEqualStrings("primary-a", rewind_rejoin.former.node_id);
    try std.testing.expectEqual(@as(u64, 1), rewind_rejoin.former.identity.timeline_id);
    try std.testing.expectEqual(@as(u64, 12), rewind_rejoin.former.last_lsn);
    try std.testing.expectEqual(@as(u64, 8), rewind_rejoin.policy.retained_from_lsn);
    try std.testing.expectEqualStrings("standby-b", rewind_rejoin.receipt.?.promoted_node_id);
    try std.testing.expectEqual(@as(u64, 2), rewind_rejoin.receipt.?.new_timeline_id);
    try std.testing.expectEqual(@as(u64, 10), rewind_rejoin.receipt.?.observed_lsn);

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

    var reseed_command = (try adminCommandForAction(alloc, reseed.actions[0], parent_identity.identity, .{
        .old_primary_id = receipt.old_primary_id,
        .promoted_node_id = receipt.promoted_node_id,
        .new_timeline_id = receipt.new_timeline_id,
        .new_epoch = receipt.new_epoch,
        .former_last_lsn = 12,
        .retained_from_lsn = 11,
        .fence_generation = receipt.generation,
        .fence_token = receipt.token,
        .reason = receipt.reason,
    })).?;
    defer reseed_command.deinit(alloc);
    var reseed_command_plan = try reseed_command.parsePlan(alloc);
    defer reseed_command_plan.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 11), reseed_command_plan.command.rejoin_assess.policy.retained_from_lsn);
    try std.testing.expectEqualStrings("standby-b", reseed_command_plan.command.rejoin_assess.receipt.?.promoted_node_id);
}

fn expectContains(haystack: []const u8, needle: []const u8) !void {
    try std.testing.expect(std.mem.indexOf(u8, haystack, needle) != null);
}
