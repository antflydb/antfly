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

//! Shared HA admin command executor.
//!
//! `admin_cli.zig` owns parsing and stable command vocabulary. `admin.zig`
//! owns individual storage operations. This module binds the two so CLI, HTTP,
//! and operator integration layers can execute the same command contract without
//! recreating dispatch, handle checks, status/metrics selection, or result
//! cleanup.

const std = @import("std");
const Allocator = std.mem.Allocator;
const admin_api = @import("../../admin/mod.zig");
const admin = @import("admin.zig");
const admin_cli = @import("admin_cli.zig");
const backup_manifest = @import("backup_manifest.zig");
const commit_gate = @import("commit_gate.zig");
const fencing = @import("fencing.zig");
const metrics = @import("metrics.zig");
const owner_job_gate = @import("owner_job_gate.zig");
const operator = @import("operator.zig");
const primary_mod = @import("primary.zig");
const read_gate = @import("read_gate.zig");
const rejoin = @import("rejoin.zig");
const replication_api = @import("replication_api.zig");
const replication_log = @import("replication_log.zig");
const replication_record = @import("replication_record.zig");
const session = @import("session.zig");
const slot_store = @import("slot_store.zig");
const standby_mod = @import("standby.zig");
const status = @import("status.zig");
const validation = @import("validation.zig");
const write_gate = @import("write_gate.zig");

var test_path_counter: u64 = 0;

const max_manifest_bytes = 64 * 1024 * 1024;
const max_manifest_file_bytes = 256 * 1024 * 1024;

pub const MetadataAppliedLsnFn = *const fn (ctx: *anyopaque) anyerror!u64;

pub const Context = struct {
    primary: ?*primary_mod.Primary = null,
    primary_node_id: ?[]const u8 = null,
    standby: ?*standby_mod.Standby = null,
    standby_node_id: ?[]const u8 = null,
    promoted_standby_handoff: ?standby_mod.PromotionHandoff = null,
    fence_store: ?*fencing.Store = null,
    former_primary_log: ?*replication_log.ReplicationLog = null,
    metadata_applied_lsn_ctx: ?*anyopaque = null,
    metadata_applied_lsn_fn: ?MetadataAppliedLsnFn = null,
};

pub const SeedFinishResult = struct {
    manifest_id: []u8,
    backup_lsn: u64,
    end_record_lsn: u64,

    pub fn deinit(self: *SeedFinishResult, alloc: Allocator) void {
        alloc.free(self.manifest_id);
        self.* = undefined;
    }
};

pub const SeedBootstrapResult = struct {
    manifest_id: []u8,
    backup_lsn: u64,
    checkpoint_lsn: u64,

    pub fn deinit(self: *SeedBootstrapResult, alloc: Allocator) void {
        alloc.free(self.manifest_id);
        self.* = undefined;
    }
};

pub const RejoinRewindResult = struct {
    assessment: rejoin.Assessment,
    rewind: rejoin.RewindResult,
};

pub const RejoinReseedResult = struct {
    assessment: rejoin.Assessment,
    reseed: rejoin.ReseedResult,
};

pub const Result = union(enum) {
    identify_system: replication_api.IdentifySystemResponse,
    slot: admin.SlotResult,
    slot_list: status.PrimarySnapshot,
    seed_begin: primary_mod.BaseBackupStartResult,
    seed_finish: SeedFinishResult,
    seed_bootstrap: SeedBootstrapResult,
    start_replication: replication_api.StartReplicationResponse,
    stream_once: session.Result,
    standby_status_update: replication_api.StandbyStatusUpdateResponse,
    primary_status: status.PrimarySnapshot,
    standby_status: status.StandbySnapshot,
    primary_metrics: metrics.PrimaryMetrics,
    standby_metrics: metrics.StandbyMetrics,
    commit_check: commit_gate.GateResult,
    commit_append: commit_gate.AppendResult,
    read_check: read_gate.Decision,
    write_check: write_gate.Decision,
    owner_job_check: owner_job_gate.Decision,
    fence_acquire: admin.FenceReceiptResult,
    fence_current: ?admin.FenceReceiptResult,
    promote_assess: status.PromotionAssessment,
    promote_current_fence: admin.FencedPromotionResult,
    promote: admin.FencedPromotionResult,
    rejoin_assess: rejoin.Assessment,
    rejoin_rewind: RejoinRewindResult,
    rejoin_reseed: RejoinReseedResult,
    operator_plan: operator.Plan,

    pub fn deinit(self: *Result, alloc: Allocator) void {
        switch (self.*) {
            .start_replication => |*result| result.deinit(alloc),
            .slot_list => |*snapshot| snapshot.deinit(alloc),
            .seed_finish => |*result| result.deinit(alloc),
            .seed_bootstrap => |*result| result.deinit(alloc),
            .primary_status => |*snapshot| snapshot.deinit(alloc),
            .primary_metrics => |*snapshot| snapshot.deinit(alloc),
            .fence_acquire => |*result| result.deinit(alloc),
            .fence_current => |*maybe_result| if (maybe_result.*) |*result| result.deinit(alloc),
            .promote_current_fence => |*result| result.deinit(alloc),
            .promote => |*result| result.deinit(alloc),
            .operator_plan => |*result| result.deinit(alloc),
            else => {},
        }
        self.* = undefined;
    }
};

pub const ResultDocument = struct {
    schema_version: u32 = 1,
    result: Result,
};

pub const RenderedOutput = struct {
    content_type: []const u8,
    body: []u8,

    pub fn deinit(self: *RenderedOutput, alloc: Allocator) void {
        alloc.free(self.body);
        self.* = undefined;
    }
};

pub fn resultDocument(result: Result) ResultDocument {
    return .{ .result = result };
}

pub fn renderJsonAlloc(alloc: Allocator, result: Result) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, resultDocument(result), .{});
}

fn renderJsonWithContextAlloc(alloc: Allocator, maybe_ctx: ?Context, result: Result) ![]u8 {
    return switch (result) {
        .slot => |slot_result| {
            if (primaryActionNodeID(maybe_ctx)) |node_id| return try renderSlotActionJsonAlloc(alloc, node_id, slot_result);
            return try renderJsonAlloc(alloc, result);
        },
        .slot_list => |snapshot| try renderSlotListJsonAlloc(alloc, snapshot),
        .seed_begin => |response| {
            if (primaryActionNodeID(maybe_ctx)) |node_id| return try renderSeedBeginJsonAlloc(alloc, node_id, response);
            return try renderJsonAlloc(alloc, result);
        },
        .seed_finish => |response| {
            if (primaryActionNodeID(maybe_ctx)) |node_id| return try renderSeedFinishJsonAlloc(alloc, node_id, response);
            return try renderJsonAlloc(alloc, result);
        },
        .seed_bootstrap => |response| {
            if (standbyActionNodeID(maybe_ctx)) |node_id| return try renderSeedBootstrapJsonAlloc(alloc, node_id, response);
            return try renderJsonAlloc(alloc, result);
        },
        .primary_status => |snapshot| blk: {
            const node_id = primaryActionNodeID(maybe_ctx) orelse return error.PrimaryNodeIDUnavailable;
            break :blk try renderPrimaryStatusJsonAlloc(alloc, node_id, snapshot);
        },
        .standby_status => |snapshot| blk: {
            const node_id = standbyActionNodeID(maybe_ctx) orelse return error.StandbyNodeIDUnavailable;
            break :blk try renderStandbyStatusJsonAlloc(alloc, node_id, snapshot);
        },
        .commit_check => |gate| try renderCommitCheckJsonAlloc(alloc, gate),
        .commit_append => |append_result| try renderCommitAppendJsonAlloc(alloc, append_result),
        .read_check => |decision| try renderReadCheckJsonAlloc(alloc, decision),
        .write_check => |decision| try renderWriteCheckJsonAlloc(alloc, decision),
        .owner_job_check => |decision| try renderOwnerJobCheckJsonAlloc(alloc, decision),
        .fence_acquire => |fence_result| try renderFenceAcquireJsonAlloc(alloc, fence_result),
        .fence_current => |maybe_fence_result| try renderFenceCurrentJsonAlloc(alloc, maybe_fence_result),
        .promote_assess => |assessment| {
            if (standbyActionNodeID(maybe_ctx)) |node_id| {
                return try renderPromotionAssessJsonAlloc(alloc, node_id, assessment);
            }
            return try renderJsonAlloc(alloc, result);
        },
        .promote_current_fence => |promotion_result| try renderPromotionJsonAlloc(alloc, promotion_result),
        .promote => |promotion_result| try renderPromotionJsonAlloc(alloc, promotion_result),
        .rejoin_assess => |assessment| try renderRejoinAssessJsonAlloc(alloc, assessment),
        .rejoin_rewind => |rewind_result| try renderRejoinRewindJsonAlloc(alloc, rewind_result),
        .rejoin_reseed => |reseed_result| {
            if (primaryActionNodeID(maybe_ctx)) |node_id| return try renderRejoinReseedJsonAlloc(alloc, node_id, reseed_result);
            return try renderJsonAlloc(alloc, result);
        },
        else => try renderJsonAlloc(alloc, result),
    };
}

fn renderSlotActionJsonAlloc(alloc: Allocator, node_id: []const u8, result: admin.SlotResult) ![]u8 {
    const action_kind = switch (result) {
        .create => "replication_slot_create",
        .pause => "replication_slot_pause",
        .@"resume" => "replication_slot_resume",
        .drop => "replication_slot_drop",
    };
    const slot_name = switch (result) {
        inline else => |slot| slot.slot_name,
    };
    const slot_action = switch (result) {
        .create => "create",
        .pause => "pause",
        .@"resume" => "resume",
        .drop => "drop",
    };
    const action_id = try std.fmt.allocPrint(alloc, "{s}:{s}", .{ action_kind, slot_name });
    defer alloc.free(action_id);
    return try std.json.Stringify.valueAlloc(alloc, admin_api.HAReplicationSlotActionResponse{
        .schema_version = 1,
        .action = .{
            .action_id = action_id,
            .action_kind = action_kind,
            .target = slot_name,
            .state = "applied",
            .node_id = node_id,
        },
        .slot_action = slot_action,
        .slot = switch (result) {
            inline else => |slot| try adminReplicationSlot(slot, if (@hasField(@TypeOf(slot), "dropped")) slot.dropped else null),
        },
    }, .{});
}

fn renderSlotListJsonAlloc(alloc: Allocator, snapshot: status.PrimarySnapshot) ![]u8 {
    const slots = try adminReplicationSlots(alloc, snapshot);
    defer alloc.free(slots);
    return try std.json.Stringify.valueAlloc(alloc, admin_api.HAReplicationSlotListResponse{
        .schema_version = 1,
        .slots = slots,
    }, .{});
}

fn renderSeedBeginJsonAlloc(alloc: Allocator, node_id: []const u8, response: primary_mod.BaseBackupStartResult) ![]u8 {
    const action_id = try std.fmt.allocPrint(alloc, "base_backup_begin:{s}", .{response.manifest_id});
    defer alloc.free(action_id);
    return try std.json.Stringify.valueAlloc(alloc, admin_api.HABaseBackupBeginResponse{
        .schema_version = 1,
        .action = adminActionReceipt(action_id, "base_backup_begin", response.manifest_id, "applied", node_id),
        .slot_name = response.slot_name,
        .manifest_id = response.manifest_id,
        .backup_lsn = try adminI64(response.backup_lsn),
        .start_record_lsn = try adminI64(response.start_record_lsn),
    }, .{});
}

fn renderSeedFinishJsonAlloc(alloc: Allocator, node_id: []const u8, response: SeedFinishResult) ![]u8 {
    const action_id = try std.fmt.allocPrint(alloc, "base_backup_finish:{s}", .{response.manifest_id});
    defer alloc.free(action_id);
    return try std.json.Stringify.valueAlloc(alloc, admin_api.HABaseBackupFinishResponse{
        .schema_version = 1,
        .action = adminActionReceipt(action_id, "base_backup_finish", response.manifest_id, "applied", node_id),
        .manifest_id = response.manifest_id,
        .backup_lsn = try adminI64(response.backup_lsn),
        .end_record_lsn = try adminI64(response.end_record_lsn),
    }, .{});
}

fn renderSeedBootstrapJsonAlloc(alloc: Allocator, node_id: []const u8, response: SeedBootstrapResult) ![]u8 {
    const action_id = try std.fmt.allocPrint(alloc, "standby_bootstrap:{s}", .{response.manifest_id});
    defer alloc.free(action_id);
    return try std.json.Stringify.valueAlloc(alloc, admin_api.HAStandbyBootstrapResponse{
        .schema_version = 1,
        .action = adminActionReceipt(action_id, "standby_bootstrap", response.manifest_id, "applied", node_id),
        .manifest_id = response.manifest_id,
        .backup_lsn = try adminI64(response.backup_lsn),
        .checkpoint_lsn = try adminI64(response.checkpoint_lsn),
    }, .{});
}

fn renderPrimaryStatusJsonAlloc(alloc: Allocator, node_id: []const u8, snapshot: status.PrimarySnapshot) ![]u8 {
    const response = admin_api.HAPrimaryStatusResponse{
        .schema_version = 1,
        .snapshot = try adminPrimarySnapshot(alloc, snapshot, node_id),
    };
    defer alloc.free(response.snapshot.slots);
    return try std.json.Stringify.valueAlloc(alloc, response, .{});
}

fn renderStandbyStatusJsonAlloc(alloc: Allocator, node_id: []const u8, snapshot: status.StandbySnapshot) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, admin_api.HAStandbyStatusResponse{
        .schema_version = 1,
        .snapshot = try adminStandbySnapshot(snapshot, node_id),
    }, .{});
}

fn renderCommitCheckJsonAlloc(alloc: Allocator, gate: commit_gate.GateResult) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, admin_api.HACommitCheckResponse{
        .schema_version = 1,
        .gate = try adminCommitGate(gate),
    }, .{});
}

fn renderCommitAppendJsonAlloc(alloc: Allocator, append_result: commit_gate.AppendResult) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, admin_api.HACommitAppendResponse{
        .schema_version = 1,
        .lsn = try adminI64(append_result.lsn),
        .gate = try adminCommitGate(append_result.gate),
    }, .{});
}

fn renderReadCheckJsonAlloc(alloc: Allocator, decision: read_gate.Decision) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, admin_api.HAReadCheckResponse{
        .schema_version = 1,
        .decision = try adminReadDecision(decision),
    }, .{});
}

fn renderWriteCheckJsonAlloc(alloc: Allocator, decision: write_gate.Decision) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, admin_api.HAWriteCheckResponse{
        .schema_version = 1,
        .decision = try adminWriteDecision(decision),
    }, .{});
}

fn renderOwnerJobCheckJsonAlloc(alloc: Allocator, decision: owner_job_gate.Decision) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, admin_api.HAOwnerJobCheckResponse{
        .schema_version = 1,
        .decision = try adminOwnerJobDecision(decision),
    }, .{});
}

fn renderFenceAcquireJsonAlloc(alloc: Allocator, result: admin.FenceReceiptResult) ![]u8 {
    const action_id = try std.fmt.allocPrint(alloc, "fence_acquire:{s}", .{result.receipt.promoted_node_id});
    defer alloc.free(action_id);
    return try std.json.Stringify.valueAlloc(alloc, admin_api.HAFenceResponse{
        .schema_version = 1,
        .action = adminActionReceipt(action_id, "fence_acquire", result.receipt.promoted_node_id, "applied", result.receipt.promoted_node_id),
        .receipt = try adminFenceReceipt(result.receipt),
    }, .{});
}

fn renderFenceCurrentJsonAlloc(alloc: Allocator, maybe_result: ?admin.FenceReceiptResult) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, admin_api.HACurrentFenceResponse{
        .schema_version = 1,
        .held = maybe_result != null,
        .receipt = if (maybe_result) |result| try adminFenceReceipt(result.receipt) else null,
    }, .{});
}

fn renderPromotionJsonAlloc(alloc: Allocator, result: admin.FencedPromotionResult) ![]u8 {
    const action_id = try std.fmt.allocPrint(alloc, "promotion:{s}", .{result.promoted_node_id});
    defer alloc.free(action_id);
    return try std.json.Stringify.valueAlloc(alloc, admin_api.HAPromotionResponse{
        .schema_version = 1,
        .action = adminActionReceipt(action_id, "promotion", result.promoted_node_id, "applied", result.promoted_node_id),
        .assessment = try adminPromotionAssessment(result.assessment),
        .promotion = .{
            .node_id = result.promoted_node_id,
            .switch_lsn = try adminI64(result.promotion.switch_lsn),
            .old_identity = try adminIdentity(result.promotion.old_identity),
            .new_identity = try adminIdentity(result.promotion.new_identity),
            .forced = result.promotion.forced,
            .data_loss_possible = result.promotion.data_loss_possible,
        },
        .fence_generation = try adminI64(result.fence_generation),
        .fence_token = result.fence_token,
        .forced = result.forced,
    }, .{});
}

fn renderRejoinAssessJsonAlloc(alloc: Allocator, assessment: rejoin.Assessment) ![]u8 {
    const action_id = try std.fmt.allocPrint(alloc, "rejoin_assess:{s}", .{assessment.former_node_id});
    defer alloc.free(action_id);
    return try std.json.Stringify.valueAlloc(alloc, admin_api.HARejoinAssessResponse{
        .schema_version = 1,
        .action = adminActionReceipt(action_id, "rejoin_assess", assessment.former_node_id, "assessed", assessment.former_node_id),
        .assessment = try adminRejoinAssessment(assessment),
    }, .{});
}

fn renderRejoinRewindJsonAlloc(alloc: Allocator, result: RejoinRewindResult) ![]u8 {
    const action_id = try std.fmt.allocPrint(alloc, "rejoin_rewind:{s}", .{result.assessment.former_node_id});
    defer alloc.free(action_id);
    return try std.json.Stringify.valueAlloc(alloc, admin_api.HARejoinAssessResponse{
        .schema_version = 1,
        .action = adminActionReceipt(action_id, "rejoin_rewind", result.assessment.former_node_id, "applied", result.assessment.former_node_id),
        .assessment = try adminRejoinAssessment(result.assessment),
        .rewind = try adminRejoinRewindResult(result.rewind),
    }, .{});
}

fn renderRejoinReseedJsonAlloc(alloc: Allocator, node_id: []const u8, result: RejoinReseedResult) ![]u8 {
    const action_id = try std.fmt.allocPrint(alloc, "rejoin_reseed:{s}", .{result.assessment.former_node_id});
    defer alloc.free(action_id);
    return try std.json.Stringify.valueAlloc(alloc, admin_api.HARejoinAssessResponse{
        .schema_version = 1,
        .action = adminActionReceipt(action_id, "rejoin_reseed", result.assessment.former_node_id, "applied", node_id),
        .assessment = try adminRejoinAssessment(result.assessment),
        .reseed = try adminRejoinReseedResult(result.reseed),
    }, .{});
}

fn renderPromotionAssessJsonAlloc(
    alloc: Allocator,
    node_id: []const u8,
    assessment: status.PromotionAssessment,
) ![]u8 {
    const action_id = try std.fmt.allocPrint(alloc, "promotion_assess:{s}", .{node_id});
    defer alloc.free(action_id);
    return try std.json.Stringify.valueAlloc(alloc, admin_api.HAPromotionAssessResponse{
        .schema_version = 1,
        .action = .{
            .action_id = action_id,
            .action_kind = "promotion_assess",
            .target = node_id,
            .state = "assessed",
            .node_id = node_id,
        },
        .assessment = try adminPromotionAssessment(assessment),
    }, .{});
}

fn adminPromotionAssessment(assessment: status.PromotionAssessment) !admin_api.HAPromotionAssessment {
    return .{
        .required_lsn = try adminI64(assessment.required_lsn),
        .received_lsn = try adminI64(assessment.received_lsn),
        .applied_lsn = try adminI64(assessment.applied_lsn),
        .has_required_lsn = assessment.has_required_lsn,
        .caught_up_to_received = assessment.caught_up_to_received,
        .fencing_confirmed = assessment.fencing_confirmed,
        .force = assessment.force,
        .mode = @tagName(assessment.mode),
        .data_loss_possible = assessment.data_loss_possible,
        .safe = assessment.safe,
        .requires_fencing = assessment.requires_fencing,
        .requires_force = assessment.requires_force,
        .can_promote = assessment.can_promote,
    };
}

fn adminI64(value: u64) !i64 {
    if (value > @as(u64, @intCast(std.math.maxInt(i64)))) return error.AdminOpenAPIIntegerOverflow;
    return @intCast(value);
}

fn adminActionReceipt(
    action_id: []const u8,
    action_kind: []const u8,
    target: []const u8,
    state: []const u8,
    node_id: []const u8,
) admin_api.HAActionReceipt {
    return .{
        .action_id = action_id,
        .action_kind = action_kind,
        .target = target,
        .state = state,
        .node_id = node_id,
    };
}

fn adminReplicationSlot(slot: anytype, dropped: ?bool) !admin_api.HAReplicationSlot {
    return .{
        .slot_name = slot.slot_name,
        .timeline_id = try adminI64(slot.timeline_id),
        .restart_lsn = try adminI64(slot.restart_lsn),
        .received_lsn = try adminI64(slot.received_lsn),
        .applied_lsn = try adminI64(slot.applied_lsn),
        .safe_read_lsn = try adminI64(slot.safe_read_lsn),
        .active = slot.active,
        .reseed_required = slot.reseed_required,
        .last_error = if (slot.last_error) |last_error| .{ .value = last_error } else .null_value,
        .current_lsn = try adminI64(slot.current_lsn),
        .dropped = if (dropped) |value| .{ .value = value } else .null_value,
    };
}

fn adminReplicationSlots(alloc: Allocator, snapshot: status.PrimarySnapshot) ![]admin_api.HAReplicationSlot {
    const slots = try alloc.alloc(admin_api.HAReplicationSlot, snapshot.slots.len);
    errdefer alloc.free(slots);
    for (snapshot.slots, 0..) |slot, idx| {
        slots[idx] = .{
            .slot_name = slot.name,
            .timeline_id = try adminI64(slot.timeline_id),
            .restart_lsn = try adminI64(slot.restart_lsn),
            .received_lsn = try adminI64(slot.received_lsn),
            .applied_lsn = try adminI64(slot.applied_lsn),
            .safe_read_lsn = try adminI64(slot.safe_read_lsn),
            .active = slot.active,
            .reseed_required = slot.reseed_required,
            .last_error = if (slot.last_error) |last_error| .{ .value = last_error } else .null_value,
            .current_lsn = try adminI64(snapshot.current_lsn),
        };
    }
    return slots;
}

fn adminIdentity(identity: standby_mod.Identity) !admin_api.HAIdentity {
    return .{
        .cluster_id = try adminI64(identity.cluster_id),
        .shard_id = try adminI64(identity.shard_id),
        .table_id = try adminI64(identity.table_id),
        .timeline_id = try adminI64(identity.timeline_id),
        .epoch = try adminI64(identity.epoch),
    };
}

fn adminPrimarySnapshot(alloc: Allocator, snapshot: status.PrimarySnapshot, node_id: []const u8) !admin_api.HAPrimarySnapshot {
    return .{
        .role = @tagName(snapshot.role),
        .node_id = node_id,
        .identity = try adminIdentity(snapshot.identity),
        .current_lsn = try adminI64(snapshot.current_lsn),
        .slots = try adminSlotSnapshots(alloc, snapshot.slots),
        .retention = try adminRetentionSnapshot(snapshot.retention),
        .durability = if (snapshot.durability) |decision| try adminDurabilityDecision(decision) else null,
    };
}

fn adminStandbySnapshot(snapshot: status.StandbySnapshot, node_id: []const u8) !admin_api.HAStandbySnapshot {
    return .{
        .role = @tagName(snapshot.role),
        .node_id = node_id,
        .identity = try adminIdentity(snapshot.identity),
        .received_lsn = try adminI64(snapshot.received_lsn),
        .applied_lsn = try adminI64(snapshot.applied_lsn),
        .safe_read_lsn = try adminI64(snapshot.safe_read_lsn),
        .upstream_lsn = if (snapshot.upstream_lsn) |value| .{ .value = try adminI64(value) } else .null_value,
        .write_lag_lsn = if (snapshot.write_lag_lsn) |value| .{ .value = try adminI64(value) } else .null_value,
        .receive_lag_lsn = if (snapshot.receive_lag_lsn) |value| .{ .value = try adminI64(value) } else .null_value,
        .apply_lag_lsn = if (snapshot.apply_lag_lsn) |value| .{ .value = try adminI64(value) } else .null_value,
        .last_error = if (snapshot.last_error) |last_error| .{ .value = last_error } else .null_value,
        .last_attempt_ns = if (snapshot.last_attempt_ns) |value| .{ .value = try adminI64(value) } else .null_value,
        .last_success_ns = if (snapshot.last_success_ns) |value| .{ .value = try adminI64(value) } else .null_value,
        .replication_failures_total = if (snapshot.replication_failures_total) |value| .{ .value = try adminI64(value) } else .null_value,
        .unapplied_lsn_count = try adminI64(snapshot.unapplied_lsn_count),
        .caught_up_to_received = snapshot.caught_up_to_received,
        .can_serve_safe_reads = snapshot.can_serve_safe_reads,
    };
}

fn adminSlotSnapshots(alloc: Allocator, slots: []const status.SlotSnapshot) ![]admin_api.HASlotSnapshot {
    const admin_slots = try alloc.alloc(admin_api.HASlotSnapshot, slots.len);
    errdefer alloc.free(admin_slots);
    for (slots, 0..) |slot, idx| {
        admin_slots[idx] = .{
            .name = slot.name,
            .timeline_id = try adminI64(slot.timeline_id),
            .active = slot.active,
            .reseed_required = slot.reseed_required,
            .restart_lsn = try adminI64(slot.restart_lsn),
            .received_lsn = try adminI64(slot.received_lsn),
            .applied_lsn = try adminI64(slot.applied_lsn),
            .safe_read_lsn = try adminI64(slot.safe_read_lsn),
            .write_lag_lsn = try adminI64(slot.write_lag_lsn),
            .apply_lag_lsn = try adminI64(slot.apply_lag_lsn),
            .safe_read_lag_lsn = try adminI64(slot.safe_read_lag_lsn),
            .retention_lag_lsn = try adminI64(slot.retention_lag_lsn),
            .status = @tagName(slot.status),
            .last_error = if (slot.last_error) |last_error| .{ .value = last_error } else .null_value,
        };
    }
    return admin_slots;
}

fn adminRetentionSnapshot(snapshot: slot_store.RetentionSnapshot) !admin_api.HARetentionSnapshot {
    return .{
        .primary_lsn = try adminI64(snapshot.primary_lsn),
        .oldest_restart_lsn = try adminI64(snapshot.oldest_restart_lsn),
        .retained_lsn_count = try adminI64(snapshot.retained_lsn_count),
        .retained_byte_count = try adminI64(snapshot.retained_byte_count),
        .retained_age_ns = try adminI64(snapshot.retained_age_ns),
        .active_slots = try adminI64(snapshot.active_slots),
        .reseed_recommended = try adminI64(snapshot.reseed_recommended),
    };
}

fn adminCommitGate(gate: commit_gate.GateResult) !admin_api.HACommitGate {
    return .{
        .target_lsn = try adminI64(gate.target_lsn),
        .action = @tagName(gate.action),
        .durability = try adminDurabilityDecision(gate.decision),
    };
}

fn adminDurabilityDecision(decision: primary_mod.DurabilityDecision) !admin_api.HADurabilityDecision {
    return .{
        .status = @tagName(decision.status),
        .mode = @tagName(decision.mode),
        .selection = @tagName(decision.selection),
        .target_lsn = try adminI64(decision.target_lsn),
        .progress_lsn = try adminI64(decision.progress_lsn),
        .missing_lsn_count = try adminI64(decision.missing_lsn_count),
        .satisfied_count = try adminI64(decision.satisfied_count),
        .required_count = try adminI64(decision.required_count),
        .candidate_count = try adminI64(decision.candidate_count),
    };
}

fn adminReadDecision(decision: read_gate.Decision) !admin_api.HAReadDecision {
    return .{
        .action = @tagName(decision.action),
        .consistency = @tagName(decision.consistency),
        .required_lsn = if (decision.required_lsn) |value| .{ .value = try adminI64(value) } else .null_value,
        .required_metadata_lsn = if (decision.required_metadata_lsn) |value| .{ .value = try adminI64(value) } else .null_value,
        .received_lsn = try adminI64(decision.received_lsn),
        .applied_lsn = try adminI64(decision.applied_lsn),
        .safe_read_lsn = try adminI64(decision.safe_read_lsn),
        .metadata_applied_lsn = if (decision.metadata_applied_lsn) |value| .{ .value = try adminI64(value) } else .null_value,
        .serve_lsn = if (decision.serve_lsn) |value| .{ .value = try adminI64(value) } else .null_value,
        .missing_lsn_count = try adminI64(decision.missing_lsn_count),
        .metadata_missing_lsn_count = try adminI64(decision.metadata_missing_lsn_count),
    };
}

fn adminWriteDecision(decision: write_gate.Decision) !admin_api.HAWriteDecision {
    return .{
        .role = @tagName(decision.role),
        .action = @tagName(decision.action),
        .identity = try adminIdentity(decision.identity),
        .durable_lsn = try adminI64(decision.durable_lsn),
        .next_lsn = try adminI64(decision.next_lsn),
        .promotion_handoff = if (decision.promotion_handoff) |handoff| try adminPromotionHandoff(handoff) else null,
    };
}

fn adminOwnerJobDecision(decision: owner_job_gate.Decision) !admin_api.HAOwnerJobDecision {
    return .{
        .kind = @tagName(decision.kind),
        .role = @tagName(decision.role),
        .action = @tagName(decision.action),
        .identity = try adminIdentity(decision.identity),
        .durable_lsn = try adminI64(decision.durable_lsn),
        .next_lsn = try adminI64(decision.next_lsn),
        .promotion_handoff = if (decision.promotion_handoff) |handoff| try adminPromotionHandoff(handoff) else null,
    };
}

fn adminPromotionHandoff(handoff: standby_mod.PromotionHandoff) !admin_api.HAPromotionHandoff {
    return .{
        .identity = try adminIdentity(handoff.identity),
        .switch_lsn = try adminI64(handoff.switch_lsn),
        .next_lsn = try adminI64(handoff.next_lsn),
    };
}

fn adminFenceReceipt(receipt: fencing.Receipt) !admin_api.HAFenceReceipt {
    return .{
        .identity = try adminIdentity(receipt.identity),
        .old_primary_id = receipt.old_primary_id,
        .promoted_node_id = receipt.promoted_node_id,
        .parent_timeline_id = try adminI64(receipt.parent_timeline_id),
        .parent_epoch = try adminI64(receipt.parent_epoch),
        .new_timeline_id = try adminI64(receipt.new_timeline_id),
        .new_epoch = try adminI64(receipt.new_epoch),
        .required_lsn = try adminI64(receipt.required_lsn),
        .observed_lsn = try adminI64(receipt.observed_lsn),
        .generation = try adminI64(receipt.generation),
        .forced = receipt.forced,
        .token = receipt.token,
        .reason = receipt.reason,
    };
}

fn adminRejoinAssessment(assessment: rejoin.Assessment) !admin_api.HARejoinAssessment {
    return .{
        .action = @tagName(assessment.action),
        .reason = @tagName(assessment.reason),
        .former_node_id = assessment.former_node_id,
        .target_timeline_id = try adminI64(assessment.target_timeline_id),
        .target_epoch = try adminI64(assessment.target_epoch),
        .parent_cluster_id = try adminI64(assessment.parent_cluster_id),
        .parent_shard_id = try adminI64(assessment.parent_shard_id),
        .parent_table_id = try adminI64(assessment.parent_table_id),
        .parent_timeline_id = try adminI64(assessment.parent_timeline_id),
        .parent_epoch = try adminI64(assessment.parent_epoch),
        .fork_lsn = try adminI64(assessment.fork_lsn),
        .former_last_lsn = try adminI64(assessment.former_last_lsn),
        .retained_from_lsn = try adminI64(assessment.retained_from_lsn),
        .data_loss_discarded = assessment.data_loss_discarded,
    };
}

fn adminRejoinRewindResult(result: rejoin.RewindResult) !admin_api.HARejoinRewindResult {
    return .{
        .node_id = result.node_id,
        .fork_lsn = try adminI64(result.fork_lsn),
        .previous_last_lsn = try adminI64(result.previous_last_lsn),
        .current_last_lsn = try adminI64(result.current_last_lsn),
        .next_lsn = try adminI64(result.next_lsn),
        .discarded_lsn_count = try adminI64(result.discarded_lsn_count),
        .target_timeline_id = try adminI64(result.target_timeline_id),
        .target_epoch = try adminI64(result.target_epoch),
        .data_loss_discarded = result.data_loss_discarded,
    };
}

fn adminRejoinReseedResult(result: rejoin.ReseedResult) !admin_api.HARejoinReseedResult {
    return .{
        .node_id = result.node_id,
        .slot_name = result.slot_name,
        .target_timeline_id = try adminI64(result.target_timeline_id),
        .target_epoch = try adminI64(result.target_epoch),
        .fork_lsn = try adminI64(result.fork_lsn),
        .former_last_lsn = try adminI64(result.former_last_lsn),
        .reseed_required = result.reseed_required,
        .base_backup_required = result.base_backup_required,
    };
}

pub fn renderPrometheusAlloc(alloc: Allocator, result: Result) ![]u8 {
    return switch (result) {
        .slot_list => |snapshot| blk: {
            var metric_snapshot = try metrics.fromPrimarySnapshot(alloc, snapshot);
            defer metric_snapshot.deinit(alloc);
            break :blk try metrics.renderPrimaryPrometheusAlloc(alloc, metric_snapshot);
        },
        .primary_status => |snapshot| blk: {
            var metric_snapshot = try metrics.fromPrimarySnapshot(alloc, snapshot);
            defer metric_snapshot.deinit(alloc);
            break :blk try metrics.renderPrimaryPrometheusAlloc(alloc, metric_snapshot);
        },
        .standby_status => |snapshot| try metrics.renderStandbyPrometheusAlloc(
            alloc,
            metrics.fromStandbySnapshot(snapshot),
        ),
        .promote_assess => |assessment| try metrics.renderPromotionPrometheusAlloc(
            alloc,
            metrics.fromPromotionAssessment(assessment),
        ),
        .rejoin_assess => |assessment| try metrics.renderRejoinPrometheusAlloc(
            alloc,
            metrics.fromRejoinAssessment(assessment),
        ),
        .rejoin_rewind => |rejoin_result| try metrics.renderRejoinPrometheusAlloc(
            alloc,
            metrics.fromRejoinAssessment(rejoin_result.assessment),
        ),
        .rejoin_reseed => |rejoin_result| try metrics.renderRejoinPrometheusAlloc(
            alloc,
            metrics.fromRejoinAssessment(rejoin_result.assessment),
        ),
        .primary_metrics => |metric_snapshot| try metrics.renderPrimaryPrometheusAlloc(alloc, metric_snapshot),
        .standby_metrics => |metric_snapshot| try metrics.renderStandbyPrometheusAlloc(alloc, metric_snapshot),
        else => error.PrometheusUnsupportedForResult,
    };
}

pub fn renderTableAlloc(alloc: Allocator, result: Result) ![]u8 {
    return try renderTableWithContextAlloc(alloc, null, result);
}

pub fn renderTableForContextAlloc(alloc: Allocator, ctx: Context, result: Result) ![]u8 {
    return try renderTableWithContextAlloc(alloc, ctx, result);
}

fn renderTableWithContextAlloc(alloc: Allocator, maybe_ctx: ?Context, result: Result) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try appendLine(alloc, &out, "result", resultName(result));
    switch (result) {
        .identify_system => |response| {
            try appendIdentityLines(alloc, &out, "identity", response.identity);
            try appendU64Line(alloc, &out, "current_lsn", response.current_lsn);
            try appendU64Line(alloc, &out, "next_lsn", response.next_lsn);
            try appendU64Line(alloc, &out, "record_format_version", response.record_format_version);
        },
        .slot => |slot_result| try appendSlotResultLines(alloc, &out, slot_result, primaryActionNodeID(maybe_ctx)),
        .slot_list => |snapshot| try appendPrimarySnapshotLines(alloc, &out, snapshot),
        .seed_begin => |response| {
            try appendActionReceiptLines(alloc, &out, "base_backup_begin", response.manifest_id, "applied", primaryActionNodeID(maybe_ctx));
            try appendLine(alloc, &out, "slot_name", response.slot_name);
            try appendLine(alloc, &out, "manifest_id", response.manifest_id);
            try appendU64Line(alloc, &out, "backup_lsn", response.backup_lsn);
            try appendU64Line(alloc, &out, "start_record_lsn", response.start_record_lsn);
        },
        .seed_finish => |response| {
            try appendActionReceiptLines(alloc, &out, "base_backup_finish", response.manifest_id, "applied", primaryActionNodeID(maybe_ctx));
            try appendLine(alloc, &out, "manifest_id", response.manifest_id);
            try appendU64Line(alloc, &out, "backup_lsn", response.backup_lsn);
            try appendU64Line(alloc, &out, "end_record_lsn", response.end_record_lsn);
        },
        .seed_bootstrap => |response| {
            try appendActionReceiptLines(alloc, &out, "standby_bootstrap", response.manifest_id, "applied", standbyActionNodeID(maybe_ctx));
            try appendLine(alloc, &out, "manifest_id", response.manifest_id);
            try appendU64Line(alloc, &out, "backup_lsn", response.backup_lsn);
            try appendU64Line(alloc, &out, "checkpoint_lsn", response.checkpoint_lsn);
        },
        .start_replication => |response| {
            try appendLine(alloc, &out, "slot_name", response.slot_name);
            try appendU64Line(alloc, &out, "timeline_id", response.timeline_id);
            try appendU64Line(alloc, &out, "from_lsn", response.from_lsn);
            try appendU64Line(alloc, &out, "current_lsn", response.current_lsn);
            try appendU64Line(alloc, &out, "last_sent_lsn", response.last_sent_lsn);
            try appendU64Line(alloc, &out, "next_lsn", response.next_lsn);
            try appendBoolLine(alloc, &out, "end_of_wal", response.end_of_wal);
            try appendUsizeLine(alloc, &out, "encoded_bytes", response.encoded_bytes);
            try appendUsizeLine(alloc, &out, "record_count", response.records.len);
        },
        .stream_once => |response| {
            try appendUsizeLine(alloc, &out, "received_count", response.received_count);
            try appendUsizeLine(alloc, &out, "applied_count", response.applied_count);
            try appendU64Line(alloc, &out, "received_lsn", response.progress.received_lsn);
            try appendU64Line(alloc, &out, "applied_lsn", response.progress.applied_lsn);
            try appendU64Line(alloc, &out, "safe_read_lsn", response.progress.safe_read_lsn);
        },
        .standby_status_update => |response| {
            try appendLine(alloc, &out, "slot_name", response.slot_name);
            try appendU64Line(alloc, &out, "timeline_id", response.timeline_id);
            try appendU64Line(alloc, &out, "received_lsn", response.received_lsn);
            try appendU64Line(alloc, &out, "applied_lsn", response.applied_lsn);
            try appendU64Line(alloc, &out, "safe_read_lsn", response.safe_read_lsn);
            try appendU64Line(alloc, &out, "restart_lsn", response.restart_lsn);
            try appendBoolLine(alloc, &out, "active", response.active);
            try appendBoolLine(alloc, &out, "reseed_required", response.reseed_required);
            try appendOptionalLine(alloc, &out, "last_error", response.last_error);
            try appendU64Line(alloc, &out, "current_lsn", response.current_lsn);
        },
        .primary_status => |snapshot| try appendPrimarySnapshotLines(alloc, &out, snapshot),
        .standby_status => |snapshot| try appendStandbySnapshotLines(alloc, &out, snapshot),
        .primary_metrics => |snapshot| try appendPrimaryMetricsLines(alloc, &out, snapshot),
        .standby_metrics => |snapshot| try appendStandbyMetricsLines(alloc, &out, snapshot),
        .commit_check => |gate| {
            try appendCommitGateLines(alloc, &out, gate);
        },
        .commit_append => |append_result| {
            try appendU64Line(alloc, &out, "lsn", append_result.lsn);
            try appendCommitGateLines(alloc, &out, append_result.gate);
        },
        .read_check => |decision| {
            try appendLine(alloc, &out, "action", @tagName(decision.action));
            try appendLine(alloc, &out, "consistency", @tagName(decision.consistency));
            try appendOptionalU64Line(alloc, &out, "required_lsn", decision.required_lsn);
            try appendOptionalU64Line(alloc, &out, "required_metadata_lsn", decision.required_metadata_lsn);
            try appendU64Line(alloc, &out, "received_lsn", decision.received_lsn);
            try appendU64Line(alloc, &out, "applied_lsn", decision.applied_lsn);
            try appendU64Line(alloc, &out, "safe_read_lsn", decision.safe_read_lsn);
            try appendOptionalU64Line(alloc, &out, "metadata_applied_lsn", decision.metadata_applied_lsn);
            try appendOptionalU64Line(alloc, &out, "serve_lsn", decision.serve_lsn);
            try appendU64Line(alloc, &out, "missing_lsn_count", decision.missing_lsn_count);
            try appendU64Line(alloc, &out, "metadata_missing_lsn_count", decision.metadata_missing_lsn_count);
        },
        .write_check => |decision| try appendWriteGateLines(alloc, &out, decision),
        .owner_job_check => |decision| try appendOwnerJobGateLines(alloc, &out, decision),
        .fence_acquire => |fence_result| {
            try appendActionReceiptLines(alloc, &out, "fence_acquire", fence_result.receipt.promoted_node_id, "applied", fence_result.receipt.promoted_node_id);
            try appendFenceReceiptLines(alloc, &out, fence_result.receipt);
        },
        .fence_current => |maybe_fence_result| {
            if (maybe_fence_result) |fence_result| {
                try appendBoolLine(alloc, &out, "held", true);
                try appendFenceReceiptLines(alloc, &out, fence_result.receipt);
            } else {
                try appendBoolLine(alloc, &out, "held", false);
            }
        },
        .promote_assess => |assessment| {
            if (standbyActionNodeID(maybe_ctx)) |node_id| {
                try appendActionReceiptLines(alloc, &out, "promotion_assess", node_id, "assessed", node_id);
            }
            try appendPromotionAssessmentLines(alloc, &out, "assessment", assessment);
        },
        .promote_current_fence => |promotion_result| {
            try appendActionReceiptLines(alloc, &out, "promotion", promotion_result.promoted_node_id, "applied", promotion_result.promoted_node_id);
            try appendPromotionResultLines(alloc, &out, promotion_result);
        },
        .promote => |promotion_result| {
            try appendActionReceiptLines(alloc, &out, "promotion", promotion_result.promoted_node_id, "applied", promotion_result.promoted_node_id);
            try appendPromotionResultLines(alloc, &out, promotion_result);
        },
        .rejoin_assess => |assessment| {
            try appendActionReceiptLines(alloc, &out, "rejoin_assess", assessment.former_node_id, "assessed", assessment.former_node_id);
            try appendRejoinAssessmentLines(alloc, &out, "", assessment);
        },
        .rejoin_rewind => |rewind_result| {
            try appendActionReceiptLines(alloc, &out, "rejoin_rewind", rewind_result.assessment.former_node_id, "applied", rewind_result.assessment.former_node_id);
            try appendRejoinAssessmentLines(alloc, &out, "assessment", rewind_result.assessment);
            try appendLine(alloc, &out, "rewind.node_id", rewind_result.rewind.node_id);
            try appendU64Line(alloc, &out, "rewind.fork_lsn", rewind_result.rewind.fork_lsn);
            try appendU64Line(alloc, &out, "rewind.previous_last_lsn", rewind_result.rewind.previous_last_lsn);
            try appendU64Line(alloc, &out, "rewind.current_last_lsn", rewind_result.rewind.current_last_lsn);
            try appendU64Line(alloc, &out, "rewind.next_lsn", rewind_result.rewind.next_lsn);
            try appendU64Line(alloc, &out, "rewind.discarded_lsn_count", rewind_result.rewind.discarded_lsn_count);
            try appendU64Line(alloc, &out, "rewind.target_timeline_id", rewind_result.rewind.target_timeline_id);
            try appendU64Line(alloc, &out, "rewind.target_epoch", rewind_result.rewind.target_epoch);
            try appendBoolLine(alloc, &out, "rewind.data_loss_discarded", rewind_result.rewind.data_loss_discarded);
        },
        .rejoin_reseed => |reseed_result| {
            try appendActionReceiptLines(alloc, &out, "rejoin_reseed", reseed_result.assessment.former_node_id, "applied", primaryActionNodeID(maybe_ctx));
            try appendRejoinAssessmentLines(alloc, &out, "assessment", reseed_result.assessment);
            try appendLine(alloc, &out, "reseed.node_id", reseed_result.reseed.node_id);
            try appendLine(alloc, &out, "reseed.slot_name", reseed_result.reseed.slot_name);
            try appendU64Line(alloc, &out, "reseed.target_timeline_id", reseed_result.reseed.target_timeline_id);
            try appendU64Line(alloc, &out, "reseed.target_epoch", reseed_result.reseed.target_epoch);
            try appendU64Line(alloc, &out, "reseed.fork_lsn", reseed_result.reseed.fork_lsn);
            try appendU64Line(alloc, &out, "reseed.former_last_lsn", reseed_result.reseed.former_last_lsn);
            try appendBoolLine(alloc, &out, "reseed.reseed_required", reseed_result.reseed.reseed_required);
            try appendBoolLine(alloc, &out, "reseed.base_backup_required", reseed_result.reseed.base_backup_required);
        },
        .operator_plan => |operator_plan| {
            try appendBoolLine(alloc, &out, "automatic_promotion_allowed", operator_plan.automatic_promotion_allowed);
            try appendUsizeLine(alloc, &out, "desired_standby_count", operator_plan.desired_standby_count);
            try appendUsizeLine(alloc, &out, "healthy_standby_count", operator_plan.healthy_standby_count);
            try appendUsizeLine(alloc, &out, "unhealthy_standby_count", operator_plan.unhealthy_standby_count);
            try appendUsizeLine(alloc, &out, "lagging_standby_count", operator_plan.lagging_standby_count);
            try appendUsizeLine(alloc, &out, "reseed_required_count", operator_plan.reseed_required_count);
            try appendUsizeLine(alloc, &out, "action_count", operator_plan.actions.len);
            try appendUsizeLine(alloc, &out, "condition_count", operator_plan.conditions.len);
            for (operator_plan.actions, 0..) |action, idx| {
                try appendOperatorActionLines(alloc, &out, idx, action);
            }
            if (operator_plan.former_primary_assessment) |assessment| {
                try appendLine(alloc, &out, "former_primary.action", @tagName(assessment.action));
                try appendLine(alloc, &out, "former_primary.reason", @tagName(assessment.reason));
                try appendLine(alloc, &out, "former_primary.node_id", assessment.former_node_id);
                try appendU64Line(alloc, &out, "former_primary.fork_lsn", assessment.fork_lsn);
            }
        },
    }

    return try out.toOwnedSlice(alloc);
}

pub fn renderOutputAlloc(alloc: Allocator, result: Result, output: admin_cli.OutputFormat) !RenderedOutput {
    return switch (output) {
        .json => .{
            .content_type = "application/json",
            .body = try renderJsonAlloc(alloc, result),
        },
        .prometheus => .{
            .content_type = "text/plain; version=0.0.4",
            .body = try renderPrometheusAlloc(alloc, result),
        },
        .table => .{
            .content_type = "text/plain; charset=utf-8",
            .body = try renderTableAlloc(alloc, result),
        },
    };
}

pub fn executeAndRenderAlloc(alloc: Allocator, ctx: Context, plan: admin_cli.Plan) !RenderedOutput {
    var result = try execute(alloc, ctx, plan);
    defer result.deinit(alloc);
    return switch (plan.output) {
        .json => .{
            .content_type = "application/json",
            .body = try renderJsonWithContextAlloc(alloc, ctx, result),
        },
        .prometheus => try renderOutputAlloc(alloc, result, plan.output),
        .table => .{
            .content_type = "text/plain; charset=utf-8",
            .body = try renderTableForContextAlloc(alloc, ctx, result),
        },
    };
}

pub fn execute(alloc: Allocator, ctx: Context, plan: admin_cli.Plan) !Result {
    return switch (plan.command) {
        .identify_system => .{ .identify_system = admin.identifyPrimary(try requirePrimary(ctx)) },
        .slot => |command| .{
            .slot = try admin.applySlotAction(try requirePrimary(ctx), command.action, command.request),
        },
        .slot_list => |command| .{
            .slot_list = try admin.primaryStatus(alloc, try requirePrimary(ctx), command.retention_policy, null),
        },
        .seed => |command| try executeSeed(alloc, ctx, command),
        .start_replication => |request| .{
            .start_replication = try replication_api.startReplication(alloc, try requirePrimary(ctx), request),
        },
        .stream_once => |command| .{
            .stream_once = try executeStreamOnce(
                alloc,
                try requirePrimary(ctx),
                try requireStandby(ctx),
                command,
            ),
        },
        .standby_status_update => |request| .{
            .standby_status_update = try admin.updateStandbyProgress(try requirePrimary(ctx), request),
        },
        .primary_status => |command| try executePrimaryStatus(alloc, try requirePrimary(ctx), command),
        .standby_status => |command| executeStandbyStatus(try requireStandby(ctx), command),
        .commit_check => |command| .{
            .commit_check = try admin.evaluateCommit(try requirePrimary(ctx), command.target_lsn, command.policy),
        },
        .commit_append => |command| .{
            .commit_append = try commit_gate.appendAndEvaluate(try requirePrimary(ctx), command.append, command.policy),
        },
        .read_check => |request| .{
            .read_check = try admin.evaluateStandbyRead(try requireStandby(ctx), try readRequestWithContext(ctx, request)),
        },
        .write_check => |command| .{
            .write_check = try executeWriteCheck(ctx, command),
        },
        .owner_job_check => |command| .{
            .owner_job_check = try executeOwnerJobCheck(ctx, command),
        },
        .fence_acquire => |request| .{
            .fence_acquire = try admin.acquirePromotionFence(alloc, try requireFenceStore(ctx), request),
        },
        .fence_current => .{
            .fence_current = try admin.currentPromotionFence(alloc, try requireFenceStore(ctx)),
        },
        .promote_assess => |command| .{
            .promote_assess = try executePromoteAssess(alloc, ctx, command),
        },
        .promote_current_fence => .{
            .promote_current_fence = try admin.promoteWithCurrentFence(alloc, try requireFenceStore(ctx), try requireStandby(ctx)),
        },
        .promote => |request| .{
            .promote = try admin.promoteWithFence(alloc, try requireFenceStore(ctx), try requireStandby(ctx), request),
        },
        .rejoin_assess => |command| .{
            .rejoin_assess = admin.assessFormerPrimaryRejoin(command.former, command.receipt, command.policy),
        },
        .rejoin_rewind => |command| try executeRejoinRewind(alloc, ctx, command),
        .rejoin_reseed => |command| try executeRejoinReseed(ctx, command),
        .operator_plan => |command| .{
            .operator_plan = try executeOperatorPlan(alloc, try requirePrimary(ctx), command),
        },
    };
}

pub fn readRequestWithContext(ctx: Context, request: read_gate.Request) !read_gate.Request {
    if (request.metadata_applied_lsn != null) return request;
    const provider = ctx.metadata_applied_lsn_fn orelse return request;
    const provider_ctx = ctx.metadata_applied_lsn_ctx orelse return error.MetadataAppliedLsnProviderMissingContext;

    var enriched = request;
    enriched.metadata_applied_lsn = try provider(provider_ctx);
    return enriched;
}

fn executeRejoinRewind(alloc: Allocator, ctx: Context, command: admin_cli.RejoinAssessCommand) !Result {
    const assessment = admin.assessFormerPrimaryRejoin(command.former, command.receipt, command.policy);
    return .{ .rejoin_rewind = .{
        .assessment = assessment,
        .rewind = try admin.rewindFormerPrimaryReplicationLog(alloc, try requireFormerPrimaryLog(ctx), assessment),
    } };
}

fn executeRejoinReseed(ctx: Context, command: admin_cli.RejoinAssessCommand) !Result {
    const assessment = admin.assessFormerPrimaryRejoin(command.former, command.receipt, command.policy);
    return .{ .rejoin_reseed = .{
        .assessment = assessment,
        .reseed = try admin.markFormerPrimaryForReseed(try requirePrimary(ctx), assessment),
    } };
}

fn executeOperatorPlan(
    alloc: Allocator,
    primary: *primary_mod.Primary,
    command: admin_cli.OperatorPlanCommand,
) !operator.Plan {
    var snapshot = try admin.primaryStatus(alloc, primary, command.spec.retention_policy, command.spec.sync_policy);
    defer snapshot.deinit(alloc);
    return try operator.reconcile(alloc, command.spec, .{
        .primary = snapshot,
        .current_primary_id = command.current_primary_id,
        .primary_admin_unavailable = command.primary_admin_unavailable,
        .fencing = command.fencing,
        .former_primary = command.former_primary,
        .promotion_receipt = command.promotion_receipt,
        .rejoin_policy = command.rejoin_policy,
    });
}

fn executeWriteCheck(ctx: Context, command: admin_cli.WriteCheckCommand) !write_gate.Decision {
    return switch (command.role) {
        .primary => try evaluatePrimaryWriteWithContext(ctx, command.request),
        .standby => if (ctx.standby) |standby|
            try admin.evaluateStandbyWrite(standby, command.request)
        else
            try admin.evaluatePromotedPrimaryWrite(
                try requirePrimary(ctx),
                ctx.promoted_standby_handoff orelse return error.StandbyUnavailable,
                command.request,
            ),
    };
}

fn evaluatePrimaryWriteWithContext(ctx: Context, request: write_gate.Request) !write_gate.Decision {
    const primary = try requirePrimary(ctx);
    if (ctx.fence_store) |fence_store| {
        if (primaryActionNodeID(ctx)) |node_id| {
            return try write_gate.evaluateFencedPrimary(.{
                .primary = primary,
                .fence_store = fence_store,
                .node_id = node_id,
            }, request);
        }
        return error.PrimaryNodeIDUnavailable;
    }
    return try admin.evaluatePrimaryWrite(primary, request);
}

fn executeOwnerJobCheck(ctx: Context, command: admin_cli.OwnerJobCheckCommand) !owner_job_gate.Decision {
    return switch (command.role) {
        .primary => try admin.evaluatePrimaryOwnerJob(try requirePrimary(ctx), command.request),
        .standby => if (ctx.standby) |standby|
            try admin.evaluateStandbyOwnerJob(standby, command.request)
        else
            try admin.evaluatePromotedPrimaryOwnerJob(
                try requirePrimary(ctx),
                ctx.promoted_standby_handoff orelse return error.StandbyUnavailable,
                command.request,
            ),
    };
}

fn executePromoteAssess(
    alloc: Allocator,
    ctx: Context,
    command: admin_cli.PromoteAssessCommand,
) !status.PromotionAssessment {
    const standby = try requireStandby(ctx);
    if (command.use_current_fence) {
        var current = (try admin.currentPromotionFence(alloc, try requireFenceStore(ctx))) orelse return error.FenceReceiptMissing;
        defer current.deinit(alloc);
        return admin.assessPromotionWithFence(standby, current.receipt);
    }
    return admin.assessPromotion(standby, command.check);
}

fn executeStreamOnce(
    alloc: Allocator,
    primary: *primary_mod.Primary,
    standby: *standby_mod.Standby,
    command: admin_cli.StreamOnceCommand,
) !session.Result {
    var apply_ctx = NoopApply{};
    return try session.replicateAvailable(
        alloc,
        primary,
        command.slot_name,
        standby,
        &apply_ctx,
        NoopApply.apply,
    );
}

const NoopApply = struct {
    fn apply(_: *anyopaque, _: replication_record.RecordView) !void {}
};

fn executeSeed(alloc: Allocator, ctx: Context, command: admin_cli.SeedCommand) !Result {
    return switch (command) {
        .begin => |request| .{ .seed_begin = try admin.beginBaseBackup(try requirePrimary(ctx), request) },
        .finish => |request| try executeSeedFinish(alloc, try requirePrimary(ctx), request),
        .bootstrap => |request| try executeSeedBootstrap(alloc, try requireStandby(ctx), request),
    };
}

fn executeSeedFinish(
    alloc: Allocator,
    primary: *primary_mod.Primary,
    command: admin_cli.SeedManifestPathCommand,
) !Result {
    var decoded = try readManifestAlloc(alloc, command.manifest_path);
    defer decoded.deinit(alloc);

    const ended = try admin.endBaseBackup(primary, .{
        .identity = decoded.view.identity,
        .manifest_id = decoded.view.manifest_id,
        .backup_lsn = decoded.view.backup_lsn,
        .checkpoint_lsn = decoded.view.checkpoint_lsn,
        .files = decoded.view.files,
        .flags = decoded.view.flags,
    });

    return .{ .seed_finish = .{
        .manifest_id = try alloc.dupe(u8, ended.manifest_id),
        .backup_lsn = ended.backup_lsn,
        .end_record_lsn = ended.end_record_lsn,
    } };
}

fn executeSeedBootstrap(
    alloc: Allocator,
    standby: *standby_mod.Standby,
    command: admin_cli.SeedBootstrapCommand,
) !Result {
    var decoded = try readManifestAlloc(alloc, command.manifest_path);
    defer decoded.deinit(alloc);
    var contents = try readManifestContentsAlloc(alloc, decoded.view, command.content_root orelse manifestParent(command.manifest_path));
    defer contents.deinit(alloc);

    const bootstrapped = try admin.bootstrapStandby(alloc, standby, decoded.view, contents.items);
    return .{ .seed_bootstrap = .{
        .manifest_id = try alloc.dupe(u8, bootstrapped.manifest_id),
        .backup_lsn = bootstrapped.backup_lsn,
        .checkpoint_lsn = bootstrapped.checkpoint_lsn,
    } };
}

fn executePrimaryStatus(
    alloc: Allocator,
    primary: *primary_mod.Primary,
    command: admin_cli.PrimaryStatusCommand,
) !Result {
    var snapshot = try admin.primaryStatus(alloc, primary, command.retention_policy, command.sync_policy);
    errdefer snapshot.deinit(alloc);

    return switch (command.view) {
        .status => .{ .primary_status = snapshot },
        .metrics => blk: {
            var metric_snapshot = try metrics.fromPrimarySnapshot(alloc, snapshot);
            errdefer metric_snapshot.deinit(alloc);
            snapshot.deinit(alloc);
            break :blk .{ .primary_metrics = metric_snapshot };
        },
    };
}

fn executeStandbyStatus(standby: *const standby_mod.Standby, command: admin_cli.StandbyStatusCommand) Result {
    const snapshot = admin.standbyStatus(standby, command.upstream_lsn);
    return switch (command.view) {
        .status => .{ .standby_status = snapshot },
        .metrics => .{ .standby_metrics = metrics.fromStandbySnapshot(snapshot) },
    };
}

const DecodedManifest = struct {
    raw: []u8,
    view: backup_manifest.ManifestView,

    fn deinit(self: *DecodedManifest, alloc: Allocator) void {
        backup_manifest.freeDecoded(alloc, self.view);
        alloc.free(self.raw);
        self.* = undefined;
    }
};

const ManifestContents = struct {
    items: []backup_manifest.FileContent,

    fn deinit(self: *ManifestContents, alloc: Allocator) void {
        for (self.items) |content| alloc.free(content.bytes);
        alloc.free(self.items);
        self.* = undefined;
    }
};

fn readManifestAlloc(alloc: Allocator, path: []const u8) !DecodedManifest {
    const raw = try readFileAlloc(alloc, path, max_manifest_bytes);
    errdefer alloc.free(raw);
    const view = try backup_manifest.decodeAlloc(alloc, raw);
    return .{
        .raw = raw,
        .view = view,
    };
}

fn readManifestContentsAlloc(
    alloc: Allocator,
    manifest: backup_manifest.ManifestView,
    content_root: []const u8,
) !ManifestContents {
    const items = try alloc.alloc(backup_manifest.FileContent, manifest.files.len);
    errdefer alloc.free(items);

    var filled: usize = 0;
    errdefer for (items[0..filled]) |content| alloc.free(content.bytes);
    for (manifest.files, 0..) |file, idx| {
        const path = try std.fs.path.join(alloc, &.{ content_root, file.path });
        defer alloc.free(path);
        const max_bytes = try checkedManifestFileReadLimit(file.size_bytes);
        items[idx] = .{
            .path = file.path,
            .bytes = try readFileAlloc(alloc, path, max_bytes),
        };
        filled += 1;
    }

    return .{ .items = items };
}

fn checkedManifestFileReadLimit(size_bytes: u64) !usize {
    if (size_bytes > max_manifest_file_bytes) return error.ManifestFileTooLarge;
    return @intCast(size_bytes + 1);
}

fn readFileAlloc(alloc: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    return try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(max_bytes));
}

fn manifestParent(path: []const u8) []const u8 {
    return std.fs.path.dirname(path) orelse ".";
}

fn requirePrimary(ctx: Context) !*primary_mod.Primary {
    return ctx.primary orelse error.PrimaryUnavailable;
}

fn requireStandby(ctx: Context) !*standby_mod.Standby {
    return ctx.standby orelse error.StandbyUnavailable;
}

fn requireFenceStore(ctx: Context) !*fencing.Store {
    return ctx.fence_store orelse error.FenceStoreUnavailable;
}

fn requireFormerPrimaryLog(ctx: Context) !*replication_log.ReplicationLog {
    return ctx.former_primary_log orelse error.FormerPrimaryLogUnavailable;
}

fn resultName(result: Result) []const u8 {
    return switch (result) {
        .identify_system => "identify_system",
        .slot => "slot",
        .slot_list => "slot_list",
        .seed_begin => "seed_begin",
        .seed_finish => "seed_finish",
        .seed_bootstrap => "seed_bootstrap",
        .start_replication => "start_replication",
        .stream_once => "stream_once",
        .standby_status_update => "standby_status_update",
        .primary_status => "primary_status",
        .standby_status => "standby_status",
        .primary_metrics => "primary_metrics",
        .standby_metrics => "standby_metrics",
        .commit_check => "commit_check",
        .commit_append => "commit_append",
        .read_check => "read_check",
        .write_check => "write_check",
        .owner_job_check => "owner_job_check",
        .fence_acquire => "fence_acquire",
        .fence_current => "fence_current",
        .promote_assess => "promote_assess",
        .promote_current_fence => "promote_current_fence",
        .promote => "promote",
        .rejoin_assess => "rejoin_assess",
        .rejoin_rewind => "rejoin_rewind",
        .rejoin_reseed => "rejoin_reseed",
        .operator_plan => "operator_plan",
    };
}

fn appendSlotResultLines(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    result: admin.SlotResult,
    node_id: ?[]const u8,
) !void {
    switch (result) {
        .create => |slot| {
            try appendActionReceiptLines(alloc, out, "replication_slot_create", slot.slot_name, "applied", node_id);
            try appendLine(alloc, out, "slot_action", "create");
            try appendCreateSlotResponseLines(alloc, out, slot);
        },
        .pause => |slot| {
            try appendActionReceiptLines(alloc, out, "replication_slot_pause", slot.slot_name, "applied", node_id);
            try appendLine(alloc, out, "slot_action", "pause");
            try appendSlotLifecycleResponseLines(alloc, out, slot);
        },
        .@"resume" => |slot| {
            try appendActionReceiptLines(alloc, out, "replication_slot_resume", slot.slot_name, "applied", node_id);
            try appendLine(alloc, out, "slot_action", "resume");
            try appendSlotLifecycleResponseLines(alloc, out, slot);
        },
        .drop => |slot| {
            try appendActionReceiptLines(alloc, out, "replication_slot_drop", slot.slot_name, "applied", node_id);
            try appendLine(alloc, out, "slot_action", "drop");
            try appendSlotLifecycleResponseLines(alloc, out, slot);
        },
    }
}

fn appendActionReceiptLines(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    action_kind: []const u8,
    target: []const u8,
    state: []const u8,
    node_id: ?[]const u8,
) !void {
    const action_id = try std.fmt.allocPrint(alloc, "{s}:{s}", .{ action_kind, target });
    defer alloc.free(action_id);
    try appendLine(alloc, out, "action.action_id", action_id);
    try appendLine(alloc, out, "action.action_kind", action_kind);
    try appendLine(alloc, out, "action.target", target);
    try appendLine(alloc, out, "action.state", state);
    if (node_id) |raw_node_id| {
        if (validation.isIdentifier(raw_node_id)) {
            try appendLine(alloc, out, "action.node_id", raw_node_id);
        }
    }
}

fn primaryActionNodeID(maybe_ctx: ?Context) ?[]const u8 {
    const ctx = maybe_ctx orelse return null;
    const node_id = ctx.primary_node_id orelse return null;
    if (validation.isIdentifier(node_id)) return node_id;
    return null;
}

fn standbyActionNodeID(maybe_ctx: ?Context) ?[]const u8 {
    const ctx = maybe_ctx orelse return null;
    const node_id = ctx.standby_node_id orelse return null;
    if (validation.isIdentifier(node_id)) return node_id;
    return null;
}

fn appendCreateSlotResponseLines(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    slot: replication_api.CreateReplicationSlotResponse,
) !void {
    try appendLine(alloc, out, "slot_name", slot.slot_name);
    try appendU64Line(alloc, out, "timeline_id", slot.timeline_id);
    try appendU64Line(alloc, out, "restart_lsn", slot.restart_lsn);
    try appendU64Line(alloc, out, "received_lsn", slot.received_lsn);
    try appendU64Line(alloc, out, "applied_lsn", slot.applied_lsn);
    try appendU64Line(alloc, out, "safe_read_lsn", slot.safe_read_lsn);
    try appendBoolLine(alloc, out, "active", slot.active);
    try appendBoolLine(alloc, out, "reseed_required", slot.reseed_required);
    try appendOptionalLine(alloc, out, "last_error", slot.last_error);
    try appendU64Line(alloc, out, "current_lsn", slot.current_lsn);
}

fn appendSlotLifecycleResponseLines(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    slot: replication_api.SlotLifecycleResponse,
) !void {
    try appendLine(alloc, out, "slot_name", slot.slot_name);
    try appendU64Line(alloc, out, "timeline_id", slot.timeline_id);
    try appendU64Line(alloc, out, "restart_lsn", slot.restart_lsn);
    try appendU64Line(alloc, out, "received_lsn", slot.received_lsn);
    try appendU64Line(alloc, out, "applied_lsn", slot.applied_lsn);
    try appendU64Line(alloc, out, "safe_read_lsn", slot.safe_read_lsn);
    try appendBoolLine(alloc, out, "active", slot.active);
    try appendBoolLine(alloc, out, "reseed_required", slot.reseed_required);
    try appendOptionalLine(alloc, out, "last_error", slot.last_error);
    try appendU64Line(alloc, out, "current_lsn", slot.current_lsn);
    try appendBoolLine(alloc, out, "dropped", slot.dropped);
}

fn appendPrimarySnapshotLines(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    snapshot: status.PrimarySnapshot,
) !void {
    try appendLine(alloc, out, "role", @tagName(snapshot.role));
    try appendIdentityLines(alloc, out, "identity", snapshot.identity);
    try appendU64Line(alloc, out, "current_lsn", snapshot.current_lsn);
    try appendUsizeLine(alloc, out, "slot_count", snapshot.slots.len);
    try appendU64Line(alloc, out, "retention.primary_lsn", snapshot.retention.primary_lsn);
    try appendU64Line(alloc, out, "retention.oldest_restart_lsn", snapshot.retention.oldest_restart_lsn);
    try appendU64Line(alloc, out, "retention.retained_lsn_count", snapshot.retention.retained_lsn_count);
    try appendU64Line(alloc, out, "retention.retained_byte_count", snapshot.retention.retained_byte_count);
    try appendUsizeLine(alloc, out, "retention.active_slots", snapshot.retention.active_slots);
    try appendUsizeLine(alloc, out, "retention.reseed_recommended", snapshot.retention.reseed_recommended);

    if (snapshot.durability) |decision| {
        try appendBoolLine(alloc, out, "durability.configured", true);
        try appendLine(alloc, out, "durability.status", @tagName(decision.status));
        try appendLine(alloc, out, "durability.mode", @tagName(decision.mode));
        try appendLine(alloc, out, "durability.selection", @tagName(decision.selection));
        try appendU64Line(alloc, out, "durability.target_lsn", decision.target_lsn);
        try appendU64Line(alloc, out, "durability.progress_lsn", decision.progress_lsn);
        try appendU64Line(alloc, out, "durability.missing_lsn_count", decision.missing_lsn_count);
        try appendUsizeLine(alloc, out, "durability.satisfied_count", decision.satisfied_count);
        try appendUsizeLine(alloc, out, "durability.required_count", decision.required_count);
        try appendUsizeLine(alloc, out, "durability.candidate_count", decision.candidate_count);
    } else {
        try appendBoolLine(alloc, out, "durability.configured", false);
    }

    for (snapshot.slots, 0..) |slot, idx| try appendSlotSnapshotLines(alloc, out, idx, slot);
}

fn appendSlotSnapshotLines(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    idx: usize,
    slot: status.SlotSnapshot,
) !void {
    try appendIndexedLine(alloc, out, "slots", idx, "name", slot.name);
    try appendIndexedU64Line(alloc, out, "slots", idx, "timeline_id", slot.timeline_id);
    try appendIndexedBoolLine(alloc, out, "slots", idx, "active", slot.active);
    try appendIndexedBoolLine(alloc, out, "slots", idx, "reseed_required", slot.reseed_required);
    try appendIndexedU64Line(alloc, out, "slots", idx, "restart_lsn", slot.restart_lsn);
    try appendIndexedU64Line(alloc, out, "slots", idx, "received_lsn", slot.received_lsn);
    try appendIndexedU64Line(alloc, out, "slots", idx, "applied_lsn", slot.applied_lsn);
    try appendIndexedU64Line(alloc, out, "slots", idx, "safe_read_lsn", slot.safe_read_lsn);
    try appendIndexedU64Line(alloc, out, "slots", idx, "write_lag_lsn", slot.write_lag_lsn);
    try appendIndexedU64Line(alloc, out, "slots", idx, "apply_lag_lsn", slot.apply_lag_lsn);
    try appendIndexedU64Line(alloc, out, "slots", idx, "safe_read_lag_lsn", slot.safe_read_lag_lsn);
    try appendIndexedU64Line(alloc, out, "slots", idx, "retention_lag_lsn", slot.retention_lag_lsn);
    try appendIndexedLine(alloc, out, "slots", idx, "status", @tagName(slot.status));
    try appendIndexedOptionalLine(alloc, out, "slots", idx, "last_error", slot.last_error);
}

fn appendStandbySnapshotLines(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    snapshot: status.StandbySnapshot,
) !void {
    try appendLine(alloc, out, "role", @tagName(snapshot.role));
    try appendIdentityLines(alloc, out, "identity", snapshot.identity);
    try appendU64Line(alloc, out, "received_lsn", snapshot.received_lsn);
    try appendU64Line(alloc, out, "applied_lsn", snapshot.applied_lsn);
    try appendU64Line(alloc, out, "safe_read_lsn", snapshot.safe_read_lsn);
    try appendOptionalU64Line(alloc, out, "upstream_lsn", snapshot.upstream_lsn);
    try appendOptionalU64Line(alloc, out, "write_lag_lsn", snapshot.write_lag_lsn);
    try appendOptionalU64Line(alloc, out, "receive_lag_lsn", snapshot.receive_lag_lsn);
    try appendOptionalU64Line(alloc, out, "apply_lag_lsn", snapshot.apply_lag_lsn);
    try appendOptionalLine(alloc, out, "last_error", snapshot.last_error);
    try appendOptionalU64Line(alloc, out, "last_attempt_ns", snapshot.last_attempt_ns);
    try appendOptionalU64Line(alloc, out, "last_success_ns", snapshot.last_success_ns);
    try appendOptionalU64Line(alloc, out, "replication_failures_total", snapshot.replication_failures_total);
    try appendU64Line(alloc, out, "unapplied_lsn_count", snapshot.unapplied_lsn_count);
    try appendBoolLine(alloc, out, "caught_up_to_received", snapshot.caught_up_to_received);
    try appendBoolLine(alloc, out, "can_serve_safe_reads", snapshot.can_serve_safe_reads);
}

fn appendPrimaryMetricsLines(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    snapshot: metrics.PrimaryMetrics,
) !void {
    try appendU64Line(alloc, out, "current_lsn", snapshot.current_lsn);
    try appendU64Line(alloc, out, "slot_count", snapshot.slot_count);
    try appendU64Line(alloc, out, "active_slots", snapshot.active_slots);
    try appendU64Line(alloc, out, "reseed_required_slots", snapshot.reseed_required_slots);
    try appendU64Line(alloc, out, "max_write_lag_lsn", snapshot.max_write_lag_lsn);
    try appendU64Line(alloc, out, "max_apply_lag_lsn", snapshot.max_apply_lag_lsn);
    try appendU64Line(alloc, out, "max_safe_read_lag_lsn", snapshot.max_safe_read_lag_lsn);
    try appendU64Line(alloc, out, "max_retention_lag_lsn", snapshot.max_retention_lag_lsn);
    try appendU64Line(alloc, out, "retention_oldest_restart_lsn", snapshot.retention_oldest_restart_lsn);
    try appendU64Line(alloc, out, "retention_retained_lsn_count", snapshot.retention_retained_lsn_count);
    try appendU64Line(alloc, out, "retention_retained_byte_count", snapshot.retention_retained_byte_count);
    try appendU64Line(alloc, out, "retention_active_slots", snapshot.retention_active_slots);
    try appendU64Line(alloc, out, "retention_reseed_recommended", snapshot.retention_reseed_recommended);
    try appendU64Line(alloc, out, "durability_configured", snapshot.durability_configured);
    try appendU64Line(alloc, out, "durability_satisfied", snapshot.durability_satisfied);
    try appendU64Line(alloc, out, "durability_degraded", snapshot.durability_degraded);
    try appendU64Line(alloc, out, "durability_status_code", snapshot.durability_status_code);
    try appendU64Line(alloc, out, "durability_target_lsn", snapshot.durability_target_lsn);
    try appendU64Line(alloc, out, "durability_progress_lsn", snapshot.durability_progress_lsn);
    try appendU64Line(alloc, out, "durability_missing_lsn_count", snapshot.durability_missing_lsn_count);
    try appendU64Line(alloc, out, "durability_required_count", snapshot.durability_required_count);
    try appendU64Line(alloc, out, "durability_satisfied_count", snapshot.durability_satisfied_count);
    try appendU64Line(alloc, out, "durability_candidate_count", snapshot.durability_candidate_count);

    for (snapshot.slots, 0..) |slot, idx| try appendSlotMetricsLines(alloc, out, idx, slot);
}

fn appendSlotMetricsLines(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    idx: usize,
    slot: metrics.SlotMetrics,
) !void {
    try appendIndexedLine(alloc, out, "slots", idx, "name", slot.name);
    try appendIndexedU64Line(alloc, out, "slots", idx, "active", slot.active);
    try appendIndexedU64Line(alloc, out, "slots", idx, "reseed_required", slot.reseed_required);
    try appendIndexedU64Line(alloc, out, "slots", idx, "received_lsn", slot.received_lsn);
    try appendIndexedU64Line(alloc, out, "slots", idx, "applied_lsn", slot.applied_lsn);
    try appendIndexedU64Line(alloc, out, "slots", idx, "safe_read_lsn", slot.safe_read_lsn);
    try appendIndexedU64Line(alloc, out, "slots", idx, "restart_lsn", slot.restart_lsn);
    try appendIndexedU64Line(alloc, out, "slots", idx, "write_lag_lsn", slot.write_lag_lsn);
    try appendIndexedU64Line(alloc, out, "slots", idx, "apply_lag_lsn", slot.apply_lag_lsn);
    try appendIndexedU64Line(alloc, out, "slots", idx, "safe_read_lag_lsn", slot.safe_read_lag_lsn);
    try appendIndexedU64Line(alloc, out, "slots", idx, "retention_lag_lsn", slot.retention_lag_lsn);
    try appendIndexedU64Line(alloc, out, "slots", idx, "status_code", slot.status_code);
    try appendIndexedU64Line(alloc, out, "slots", idx, "last_error", slot.last_error);
}

fn appendStandbyMetricsLines(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    snapshot: metrics.StandbyMetrics,
) !void {
    try appendU64Line(alloc, out, "received_lsn", snapshot.received_lsn);
    try appendU64Line(alloc, out, "applied_lsn", snapshot.applied_lsn);
    try appendU64Line(alloc, out, "safe_read_lsn", snapshot.safe_read_lsn);
    try appendU64Line(alloc, out, "upstream_configured", snapshot.upstream_configured);
    try appendU64Line(alloc, out, "receive_lag_lsn", snapshot.receive_lag_lsn);
    try appendU64Line(alloc, out, "apply_lag_lsn", snapshot.apply_lag_lsn);
    try appendU64Line(alloc, out, "unapplied_lsn_count", snapshot.unapplied_lsn_count);
    try appendU64Line(alloc, out, "caught_up_to_received", snapshot.caught_up_to_received);
    try appendU64Line(alloc, out, "can_serve_safe_reads", snapshot.can_serve_safe_reads);
}

fn appendCommitGateLines(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    gate: commit_gate.GateResult,
) !void {
    try appendU64Line(alloc, out, "target_lsn", gate.target_lsn);
    try appendLine(alloc, out, "action", @tagName(gate.action));
    try appendLine(alloc, out, "durability.status", @tagName(gate.decision.status));
    try appendLine(alloc, out, "durability.mode", @tagName(gate.decision.mode));
    try appendLine(alloc, out, "durability.selection", @tagName(gate.decision.selection));
    try appendU64Line(alloc, out, "durability.target_lsn", gate.decision.target_lsn);
    try appendU64Line(alloc, out, "durability.progress_lsn", gate.decision.progress_lsn);
    try appendU64Line(alloc, out, "durability.missing_lsn_count", gate.decision.missing_lsn_count);
    try appendUsizeLine(alloc, out, "durability.satisfied_count", gate.decision.satisfied_count);
    try appendUsizeLine(alloc, out, "durability.required_count", gate.decision.required_count);
    try appendUsizeLine(alloc, out, "durability.candidate_count", gate.decision.candidate_count);
}

fn appendWriteGateLines(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    decision: write_gate.Decision,
) !void {
    try appendLine(alloc, out, "role", @tagName(decision.role));
    try appendLine(alloc, out, "action", @tagName(decision.action));
    try appendBoolLine(alloc, out, "can_write", decision.canWrite());
    try appendIdentityLines(alloc, out, "identity", decision.identity);
    try appendU64Line(alloc, out, "durable_lsn", decision.durable_lsn);
    try appendU64Line(alloc, out, "next_lsn", decision.next_lsn);
    if (decision.promotion_handoff) |handoff| {
        try appendIdentityLines(alloc, out, "handoff.identity", handoff.identity);
        try appendU64Line(alloc, out, "handoff.switch_lsn", handoff.switch_lsn);
        try appendU64Line(alloc, out, "handoff.next_lsn", handoff.next_lsn);
    }
}

fn appendOwnerJobGateLines(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    decision: owner_job_gate.Decision,
) !void {
    try appendLine(alloc, out, "kind", @tagName(decision.kind));
    try appendLine(alloc, out, "role", @tagName(decision.role));
    try appendLine(alloc, out, "action", @tagName(decision.action));
    try appendBoolLine(alloc, out, "can_run", decision.canRun());
    try appendIdentityLines(alloc, out, "identity", decision.identity);
    try appendU64Line(alloc, out, "durable_lsn", decision.durable_lsn);
    try appendU64Line(alloc, out, "next_lsn", decision.next_lsn);
    if (decision.promotion_handoff) |handoff| {
        try appendIdentityLines(alloc, out, "handoff.identity", handoff.identity);
        try appendU64Line(alloc, out, "handoff.switch_lsn", handoff.switch_lsn);
        try appendU64Line(alloc, out, "handoff.next_lsn", handoff.next_lsn);
    }
}

fn appendPromotionAssessmentLines(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    prefix: []const u8,
    assessment: status.PromotionAssessment,
) !void {
    try appendPrefixedU64Line(alloc, out, prefix, "required_lsn", assessment.required_lsn);
    try appendPrefixedU64Line(alloc, out, prefix, "received_lsn", assessment.received_lsn);
    try appendPrefixedU64Line(alloc, out, prefix, "applied_lsn", assessment.applied_lsn);
    try appendPrefixedBoolLine(alloc, out, prefix, "has_required_lsn", assessment.has_required_lsn);
    try appendPrefixedBoolLine(alloc, out, prefix, "caught_up_to_received", assessment.caught_up_to_received);
    try appendPrefixedBoolLine(alloc, out, prefix, "fencing_confirmed", assessment.fencing_confirmed);
    try appendPrefixedBoolLine(alloc, out, prefix, "force", assessment.force);
    try appendPrefixedLine(alloc, out, prefix, "mode", @tagName(assessment.mode));
    try appendPrefixedBoolLine(alloc, out, prefix, "data_loss_possible", assessment.data_loss_possible);
    try appendPrefixedBoolLine(alloc, out, prefix, "safe", assessment.safe);
    try appendPrefixedBoolLine(alloc, out, prefix, "requires_fencing", assessment.requires_fencing);
    try appendPrefixedBoolLine(alloc, out, prefix, "requires_force", assessment.requires_force);
    try appendPrefixedBoolLine(alloc, out, prefix, "can_promote", assessment.can_promote);
}

fn appendRejoinAssessmentLines(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    prefix: []const u8,
    assessment: rejoin.Assessment,
) !void {
    try appendPrefixedLine(alloc, out, prefix, "action", @tagName(assessment.action));
    try appendPrefixedLine(alloc, out, prefix, "reason", @tagName(assessment.reason));
    try appendPrefixedLine(alloc, out, prefix, "former_node_id", assessment.former_node_id);
    try appendPrefixedU64Line(alloc, out, prefix, "target_timeline_id", assessment.target_timeline_id);
    try appendPrefixedU64Line(alloc, out, prefix, "target_epoch", assessment.target_epoch);
    try appendPrefixedU64Line(alloc, out, prefix, "parent_cluster_id", assessment.parent_cluster_id);
    try appendPrefixedU64Line(alloc, out, prefix, "parent_shard_id", assessment.parent_shard_id);
    try appendPrefixedU64Line(alloc, out, prefix, "parent_table_id", assessment.parent_table_id);
    try appendPrefixedU64Line(alloc, out, prefix, "parent_timeline_id", assessment.parent_timeline_id);
    try appendPrefixedU64Line(alloc, out, prefix, "parent_epoch", assessment.parent_epoch);
    try appendPrefixedU64Line(alloc, out, prefix, "fork_lsn", assessment.fork_lsn);
    try appendPrefixedU64Line(alloc, out, prefix, "former_last_lsn", assessment.former_last_lsn);
    try appendPrefixedU64Line(alloc, out, prefix, "retained_from_lsn", assessment.retained_from_lsn);
    try appendPrefixedBoolLine(alloc, out, prefix, "data_loss_discarded", assessment.data_loss_discarded);
}

fn appendPromotionResultLines(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    promotion_result: admin.FencedPromotionResult,
) !void {
    try appendPromotionAssessmentLines(alloc, out, "assessment", promotion_result.assessment);
    try appendLine(alloc, out, "promotion.node_id", promotion_result.promoted_node_id);
    try appendU64Line(alloc, out, "promotion.switch_lsn", promotion_result.promotion.switch_lsn);
    try appendIdentityLines(alloc, out, "promotion.old_identity", promotion_result.promotion.old_identity);
    try appendIdentityLines(alloc, out, "promotion.new_identity", promotion_result.promotion.new_identity);
    try appendBoolLine(alloc, out, "promotion.forced", promotion_result.promotion.forced);
    try appendBoolLine(alloc, out, "promotion.data_loss_possible", promotion_result.promotion.data_loss_possible);
    try appendU64Line(alloc, out, "fence_generation", promotion_result.fence_generation);
    try appendLine(alloc, out, "fence_token", promotion_result.fence_token);
    try appendBoolLine(alloc, out, "forced", promotion_result.forced);
}

fn appendOperatorActionLines(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    idx: usize,
    action: operator.Action,
) !void {
    try appendIndexedLine(alloc, out, "actions", idx, "kind", @tagName(action.kind));
    try appendIndexedLine(alloc, out, "actions", idx, "phase", @tagName(action.phase));
    try appendIndexedLine(alloc, out, "actions", idx, "executor", @tagName(action.executor));
    try appendIndexedLine(alloc, out, "actions", idx, "reason", action.reason);
    if (action.depends_on) |depends_on| try appendIndexedLine(alloc, out, "actions", idx, "depends_on", @tagName(depends_on));
    if (action.fencing_precondition) |fence| {
        try appendIndexedLine(alloc, out, "actions", idx, "fence_authority", @tagName(fence.authority));
        try appendIndexedLine(alloc, out, "actions", idx, "fence_holder", fence.holder);
        try appendIndexedU64Line(alloc, out, "actions", idx, "fence_generation", fence.generation);
        try appendIndexedLine(alloc, out, "actions", idx, "fence_reason", fence.reason);
    }
    if (action.standby_name) |standby_name| try appendIndexedLine(alloc, out, "actions", idx, "standby_name", standby_name);
    if (action.slot_name) |slot_name| try appendIndexedLine(alloc, out, "actions", idx, "slot_name", slot_name);
    if (action.target_lsn) |target_lsn| try appendIndexedU64Line(alloc, out, "actions", idx, "target_lsn", target_lsn);
    if (action.seed_manifest_path) |seed_manifest_path| try appendIndexedLine(alloc, out, "actions", idx, "seed_manifest_path", seed_manifest_path);
    if (action.seed_content_root) |seed_content_root| try appendIndexedLine(alloc, out, "actions", idx, "seed_content_root", seed_content_root);
    if (action.route_from) |route_from| try appendIndexedLine(alloc, out, "actions", idx, "route_from", route_from);
    if (action.route_to) |route_to| try appendIndexedLine(alloc, out, "actions", idx, "route_to", route_to);
    if (action.admin_url) |admin_url| try appendIndexedLine(alloc, out, "actions", idx, "admin_url", admin_url);
    if (action.admin_method) |admin_method| try appendIndexedLine(alloc, out, "actions", idx, "admin_method", admin_method);
    if (action.admin_path) |admin_path| try appendIndexedLine(alloc, out, "actions", idx, "admin_path", admin_path);
}

fn appendFenceReceiptLines(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    receipt: fencing.Receipt,
) !void {
    try appendIdentityLines(alloc, out, "identity", receipt.identity);
    try appendLine(alloc, out, "old_primary_id", receipt.old_primary_id);
    try appendLine(alloc, out, "promoted_node_id", receipt.promoted_node_id);
    try appendU64Line(alloc, out, "parent_timeline_id", receipt.parent_timeline_id);
    try appendU64Line(alloc, out, "parent_epoch", receipt.parent_epoch);
    try appendU64Line(alloc, out, "new_timeline_id", receipt.new_timeline_id);
    try appendU64Line(alloc, out, "new_epoch", receipt.new_epoch);
    try appendU64Line(alloc, out, "required_lsn", receipt.required_lsn);
    try appendU64Line(alloc, out, "observed_lsn", receipt.observed_lsn);
    try appendU64Line(alloc, out, "generation", receipt.generation);
    try appendBoolLine(alloc, out, "forced", receipt.forced);
    try appendLine(alloc, out, "token", receipt.token);
    try appendLine(alloc, out, "reason", receipt.reason);
}

fn appendIdentityLines(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    prefix: []const u8,
    identity: standby_mod.Identity,
) !void {
    try appendPrefixedU64Line(alloc, out, prefix, "cluster_id", identity.cluster_id);
    try appendPrefixedU64Line(alloc, out, prefix, "shard_id", identity.shard_id);
    try appendPrefixedU64Line(alloc, out, prefix, "table_id", identity.table_id);
    try appendPrefixedU64Line(alloc, out, prefix, "timeline_id", identity.timeline_id);
    try appendPrefixedU64Line(alloc, out, prefix, "epoch", identity.epoch);
}

fn appendLine(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), key: []const u8, value: []const u8) !void {
    try out.appendSlice(alloc, key);
    try out.append(alloc, '=');
    try out.appendSlice(alloc, value);
    try out.append(alloc, '\n');
}

fn appendU64Line(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), key: []const u8, value: u64) !void {
    var buf: [32]u8 = undefined;
    const text = try std.fmt.bufPrint(&buf, "{d}", .{value});
    try appendLine(alloc, out, key, text);
}

fn appendUsizeLine(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), key: []const u8, value: usize) !void {
    var buf: [32]u8 = undefined;
    const text = try std.fmt.bufPrint(&buf, "{d}", .{value});
    try appendLine(alloc, out, key, text);
}

fn appendBoolLine(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), key: []const u8, value: bool) !void {
    try appendLine(alloc, out, key, if (value) "true" else "false");
}

fn appendOptionalU64Line(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), key: []const u8, value: ?u64) !void {
    if (value) |present| {
        try appendU64Line(alloc, out, key, present);
    } else {
        try appendLine(alloc, out, key, "-");
    }
}

fn appendOptionalLine(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), key: []const u8, value: ?[]const u8) !void {
    try appendLine(alloc, out, key, value orelse "-");
}

fn prefixedKeyAlloc(alloc: Allocator, prefix: []const u8, suffix: []const u8) ![]u8 {
    if (prefix.len == 0) return try alloc.dupe(u8, suffix);
    return try std.fmt.allocPrint(alloc, "{s}.{s}", .{ prefix, suffix });
}

fn appendPrefixedLine(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    prefix: []const u8,
    suffix: []const u8,
    value: []const u8,
) !void {
    const key = try prefixedKeyAlloc(alloc, prefix, suffix);
    defer alloc.free(key);
    try appendLine(alloc, out, key, value);
}

fn appendPrefixedU64Line(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    prefix: []const u8,
    suffix: []const u8,
    value: u64,
) !void {
    const key = try prefixedKeyAlloc(alloc, prefix, suffix);
    defer alloc.free(key);
    try appendU64Line(alloc, out, key, value);
}

fn appendPrefixedBoolLine(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    prefix: []const u8,
    suffix: []const u8,
    value: bool,
) !void {
    const key = try prefixedKeyAlloc(alloc, prefix, suffix);
    defer alloc.free(key);
    try appendBoolLine(alloc, out, key, value);
}

fn appendIndexedLine(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    prefix: []const u8,
    idx: usize,
    suffix: []const u8,
    value: []const u8,
) !void {
    const key = try std.fmt.allocPrint(alloc, "{s}.{d}.{s}", .{ prefix, idx, suffix });
    defer alloc.free(key);
    try appendLine(alloc, out, key, value);
}

fn appendIndexedOptionalLine(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    prefix: []const u8,
    idx: usize,
    suffix: []const u8,
    value: ?[]const u8,
) !void {
    const key = try std.fmt.allocPrint(alloc, "{s}.{d}.{s}", .{ prefix, idx, suffix });
    defer alloc.free(key);
    try appendLine(alloc, out, key, value orelse "-");
}

fn appendIndexedU64Line(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    prefix: []const u8,
    idx: usize,
    suffix: []const u8,
    value: u64,
) !void {
    const key = try std.fmt.allocPrint(alloc, "{s}.{d}.{s}", .{ prefix, idx, suffix });
    defer alloc.free(key);
    try appendU64Line(alloc, out, key, value);
}

fn appendIndexedBoolLine(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    prefix: []const u8,
    idx: usize,
    suffix: []const u8,
    value: bool,
) !void {
    const key = try std.fmt.allocPrint(alloc, "{s}.{d}.{s}", .{ prefix, idx, suffix });
    defer alloc.free(key);
    try appendBoolLine(alloc, out, key, value);
}

const TestPaths = struct {
    primary_log: [:0]u8,
    primary_slots: [:0]u8,
    standby_log: [:0]u8,
    standby_progress: [:0]u8,
    fence_wal: [:0]u8,
    backup_root: [:0]u8,

    fn deinit(self: TestPaths, alloc: Allocator) void {
        alloc.free(self.primary_log);
        alloc.free(self.primary_slots);
        alloc.free(self.standby_log);
        alloc.free(self.standby_progress);
        alloc.free(self.fence_wal);
        alloc.free(self.backup_root);
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
    const backup_root = try allocPrintPath(alloc, name, "backup-root", nonce);
    defer alloc.free(backup_root);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_slots) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_progress) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), fence_wal) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};

    return .{
        .primary_log = try alloc.dupeSentinel(u8, primary_log, 0),
        .primary_slots = try alloc.dupeSentinel(u8, primary_slots, 0),
        .standby_log = try alloc.dupeSentinel(u8, standby_log, 0),
        .standby_progress = try alloc.dupeSentinel(u8, standby_progress, 0),
        .fence_wal = try alloc.dupeSentinel(u8, fence_wal, 0),
        .backup_root = try alloc.dupeSentinel(u8, backup_root, 0),
    };
}

fn allocPrintPath(alloc: Allocator, comptime name: []const u8, comptime part: []const u8, nonce: u64) ![]u8 {
    return try std.fmt.allocPrint(
        alloc,
        "/tmp/antfly-ha-admin-exec-" ++ name ++ "-" ++ part ++ "-{d}-{d}",
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

fn seedFiles() [2]backup_manifest.FileEntry {
    return .{
        .{ .path = "manifest", .kind = .manifest, .size_bytes = 8, .crc32 = backup_manifest.crc32("manifest") },
        .{ .path = "sst/0001", .kind = .sstable, .size_bytes = 7, .crc32 = backup_manifest.crc32("sstable") },
    };
}

fn writeTestFile(path: []const u8, bytes: []const u8) !void {
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    if (std.fs.path.dirname(path)) |parent| try std.Io.Dir.cwd().createDirPath(io_impl.io(), parent);
    try std.Io.Dir.cwd().writeFile(io_impl.io(), .{
        .sub_path = path,
        .data = bytes,
    });
}

const ApplyCounter = struct {
    count: usize = 0,

    fn apply(ctx: *anyopaque, _: replication_record.RecordView) !void {
        const self: *ApplyCounter = @ptrCast(@alignCast(ctx));
        self.count += 1;
    }
};

test "storage.ha admin exec runs slot lifecycle and status commands" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "status");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    _ = try primary.append(.{ .payload = "one" });
    _ = try primary.append(.{ .payload = "two" });

    var create_plan = try admin_cli.parse(alloc, &.{ "slot", "create", "standby-a", "--initial-lsn", "1" });
    defer create_plan.deinit(alloc);
    var created = try execute(alloc, .{ .primary = &primary }, create_plan);
    defer created.deinit(alloc);
    try std.testing.expectEqualStrings("standby-a", created.slot.create.slot_name);
    try std.testing.expectEqual(@as(u64, 1), created.slot.create.restart_lsn);
    const created_table = try renderTableAlloc(alloc, created);
    defer alloc.free(created_table);
    try expectContains(created_table, "action.action_id=replication_slot_create:standby-a\n");
    try expectContains(created_table, "action.action_kind=replication_slot_create\n");
    try expectContains(created_table, "action.target=standby-a\n");
    try expectContains(created_table, "action.state=applied\n");
    try expectContains(created_table, "last_error=-\n");
    const created_context_table = try renderTableForContextAlloc(alloc, .{ .primary_node_id = "primary-a" }, created);
    defer alloc.free(created_context_table);
    try expectContains(created_context_table, "action.node_id=primary-a\n");
    const invalid_created_context_table = try renderTableForContextAlloc(alloc, .{ .primary_node_id = "primary/a" }, created);
    defer alloc.free(invalid_created_context_table);
    try std.testing.expect(std.mem.indexOf(u8, invalid_created_context_table, "action.node_id=") == null);

    var ack_plan = try admin_cli.parse(alloc, &.{ "standby", "ack", "--slot", "standby-a", "--timeline-id", "1", "--received-lsn", "2", "--applied-lsn", "1", "--safe-read-lsn", "1" });
    defer ack_plan.deinit(alloc);
    var acked = try execute(alloc, .{ .primary = &primary }, ack_plan);
    defer acked.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), acked.standby_status_update.received_lsn);
    try std.testing.expectEqual(@as(u64, 1), acked.standby_status_update.safe_read_lsn);
    try std.testing.expect(acked.standby_status_update.active);
    try std.testing.expect(!acked.standby_status_update.reseed_required);
    const acked_table = try renderTableAlloc(alloc, acked);
    defer alloc.free(acked_table);
    try expectContains(acked_table, "active=true\n");
    try expectContains(acked_table, "safe_read_lsn=1\n");
    try expectContains(acked_table, "reseed_required=false\n");
    try expectContains(acked_table, "last_error=-\n");
    try primary.reportReplicationError("standby-a", "IntentionalApplyFailure");

    var pause_plan = try admin_cli.parse(alloc, &.{ "slot", "pause", "standby-a" });
    defer pause_plan.deinit(alloc);
    var paused = try execute(alloc, .{ .primary = &primary }, pause_plan);
    defer paused.deinit(alloc);
    const paused_table = try renderTableAlloc(alloc, paused);
    defer alloc.free(paused_table);
    try expectContains(paused_table, "action.action_id=replication_slot_pause:standby-a\n");
    try expectContains(paused_table, "slot_action=pause\n");
    try expectContains(paused_table, "last_error=IntentionalApplyFailure\n");

    var resume_plan = try admin_cli.parse(alloc, &.{ "slot", "resume", "standby-a" });
    defer resume_plan.deinit(alloc);
    var resumed = try execute(alloc, .{ .primary = &primary }, resume_plan);
    defer resumed.deinit(alloc);
    const resumed_table = try renderTableAlloc(alloc, resumed);
    defer alloc.free(resumed_table);
    try expectContains(resumed_table, "action.action_id=replication_slot_resume:standby-a\n");
    try expectContains(resumed_table, "slot_action=resume\n");
    try expectContains(resumed_table, "last_error=IntentionalApplyFailure\n");
    const resumed_json = try renderJsonWithContextAlloc(alloc, .{ .primary_node_id = "primary-a" }, resumed);
    defer alloc.free(resumed_json);
    try expectContains(resumed_json, "\"schema_version\":1");
    try expectContains(resumed_json, "\"action_kind\":\"replication_slot_resume\"");
    try expectContains(resumed_json, "\"node_id\":\"primary-a\"");
    try expectContains(resumed_json, "\"slot_action\":\"resume\"");

    var primary_status_plan = try admin_cli.parse(alloc, &.{
        "status",
        "primary",
        "--max-lag-lsn",
        "10",
        "--sync-mode",
        "remote-apply",
        "--sync-standby",
        "standby-a",
    });
    defer primary_status_plan.deinit(alloc);
    var primary_status = try execute(alloc, .{ .primary = &primary }, primary_status_plan);
    defer primary_status.deinit(alloc);
    try std.testing.expectEqualStrings("IntentionalApplyFailure", primary_status.primary_status.slots[0].last_error.?);

    const status_table = try renderTableAlloc(alloc, primary_status);
    defer alloc.free(status_table);
    try expectContains(status_table, "durability.target_lsn=2\n");
    try expectContains(status_table, "durability.progress_lsn=1\n");
    try expectContains(status_table, "durability.missing_lsn_count=1\n");
    try expectContains(status_table, "slots.0.last_error=IntentionalApplyFailure\n");

    var status_json = try executeAndRenderAlloc(alloc, .{ .primary = &primary, .primary_node_id = "primary-a" }, primary_status_plan);
    defer status_json.deinit(alloc);
    try std.testing.expectEqualStrings("application/json", status_json.content_type);
    try expectContains(status_json.body, "\"schema_version\":1");
    try expectContains(status_json.body, "\"snapshot\"");
    try expectContains(status_json.body, "\"node_id\":\"primary-a\"");
    try expectContains(status_json.body, "\"durability\"");
    try expectContains(status_json.body, "\"last_error\":\"IntentionalApplyFailure\"");
    try std.testing.expectError(
        error.PrimaryNodeIDUnavailable,
        renderJsonWithContextAlloc(alloc, .{ .primary_node_id = "primary/a" }, primary_status),
    );

    var status_plan = try admin_cli.parse(alloc, &.{
        "status",
        "primary",
        "--view",
        "metrics",
        "--max-lag-lsn",
        "10",
        "--sync-mode",
        "remote-apply",
        "--sync-standby",
        "standby-a",
    });
    defer status_plan.deinit(alloc);
    var primary_metrics = try execute(alloc, .{ .primary = &primary }, status_plan);
    defer primary_metrics.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), primary_metrics.primary_metrics.current_lsn);
    try std.testing.expectEqual(@as(u64, 1), primary_metrics.primary_metrics.slot_count);
    try std.testing.expectEqual(@as(u64, 1), primary_metrics.primary_metrics.max_apply_lag_lsn);
    try std.testing.expectEqual(@as(u64, 2), primary_metrics.primary_metrics.durability_target_lsn);
    try std.testing.expectEqual(@as(u64, 1), primary_metrics.primary_metrics.durability_progress_lsn);
    try std.testing.expectEqual(@as(u64, 1), primary_metrics.primary_metrics.durability_missing_lsn_count);

    const json_body = try renderJsonAlloc(alloc, primary_metrics);
    defer alloc.free(json_body);
    try expectContains(json_body, "\"schema_version\":1");
    try expectContains(json_body, "\"primary_metrics\"");
    try expectContains(json_body, "\"current_lsn\":2");

    const prometheus_body = try renderPrometheusAlloc(alloc, primary_metrics);
    defer alloc.free(prometheus_body);
    try expectContains(prometheus_body, "antfly_ha_primary_current_lsn 2\n");
    try expectContains(prometheus_body, "antfly_ha_slot_apply_lag_lsn{slot=\"standby-a\"} 1\n");

    const table_body = try renderTableAlloc(alloc, primary_metrics);
    defer alloc.free(table_body);
    try expectContains(table_body, "result=primary_metrics\n");
    try expectContains(table_body, "current_lsn=2\n");
    try expectContains(table_body, "slot_count=1\n");
    try expectContains(table_body, "durability_target_lsn=2\n");
    try expectContains(table_body, "durability_progress_lsn=1\n");
    try expectContains(table_body, "durability_missing_lsn_count=1\n");
    try expectContains(table_body, "slots.0.name=standby-a\n");
    try expectContains(table_body, "slots.0.last_error=1\n");

    var rendered_plan = try admin_cli.parse(alloc, &.{ "--prometheus", "status", "primary", "--view", "metrics", "--max-lag-lsn", "10" });
    defer rendered_plan.deinit(alloc);
    var rendered = try executeAndRenderAlloc(alloc, .{ .primary = &primary }, rendered_plan);
    defer rendered.deinit(alloc);
    try std.testing.expectEqualStrings("text/plain; version=0.0.4", rendered.content_type);
    try expectContains(rendered.body, "antfly_ha_primary_current_lsn 2\n");
}

test "storage.ha admin exec renders operator plan command" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "operator-plan");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);
    _ = try primary.append(.{ .payload = "one" });
    try primary.standbyStatusUpdate("standby-a", identity.timeline_id, 1, 1);

    var plan = try admin_cli.parse(alloc, &.{
        "operator",                    "plan",
        "--standby",                   "standby-a",
        "--standby-route-selector",    "--sync-mode",
        "remote-apply",                "--sync-standby",
        "standby-a",                   "--auto-failover",
        "--fencing-authority",         "kubernetes-lease",
        "--current-primary-id",        "primary-a",
        "--primary-admin-unavailable", "--fence-authority",
        "kubernetes-lease",            "--fence-ready",
        "--fence-holder",              "standby-a",
        "--fence-generation",          "9",
        "--fence-reason",              "LeaseAcquired",
    });
    defer plan.deinit(alloc);

    var result = try execute(alloc, .{ .primary = &primary }, plan);
    defer result.deinit(alloc);
    try std.testing.expect(result.operator_plan.automatic_promotion_allowed);
    try std.testing.expectEqual(@as(usize, 5), result.operator_plan.actions.len);
    try std.testing.expectEqual(operator.ActionKind.update_primary_endpoint, result.operator_plan.actions[3].kind);
    try std.testing.expectEqual(@as(u64, 9), result.operator_plan.actions[3].fencing_precondition.?.generation);

    const json_body = try renderJsonAlloc(alloc, result);
    defer alloc.free(json_body);
    try expectContains(json_body, "\"operator_plan\"");
    try expectContains(json_body, "\"automatic_promotion_allowed\":true");
    try expectContains(json_body, "\"fencing_precondition\"");
    try expectContains(json_body, "\"generation\":9");

    const table_body = try renderTableAlloc(alloc, result);
    defer alloc.free(table_body);
    try expectContains(table_body, "result=operator_plan\n");
    try expectContains(table_body, "automatic_promotion_allowed=true\n");
    try expectContains(table_body, "action_count=5\n");
    try expectContains(table_body, "actions.0.kind=acquire_fence\n");
    try expectContains(table_body, "actions.0.phase=fence\n");
    try expectContains(table_body, "actions.0.executor=admin_api\n");
    try expectContains(table_body, "actions.0.fence_authority=kubernetes_lease\n");
    try expectContains(table_body, "actions.0.admin_method=POST\n");
    try expectContains(table_body, "actions.0.admin_path=" ++ admin_api.routes.ha_fence ++ "\n");
    try expectContains(table_body, "actions.1.kind=assess_promotion\n");
    try expectContains(table_body, "actions.1.depends_on=acquire_fence\n");
    try expectContains(table_body, "actions.1.admin_path=" ++ admin_api.routes.ha_promotion_assess ++ "\n");
    try expectContains(table_body, "actions.2.kind=promote_standby\n");
    try expectContains(table_body, "actions.2.depends_on=assess_promotion\n");
    try expectContains(table_body, "actions.2.admin_path=" ++ admin_api.routes.ha_promotion_current_fence ++ "\n");
    try expectContains(table_body, "actions.3.kind=update_primary_endpoint\n");
    try expectContains(table_body, "actions.3.executor=controller_action\n");
    try expectContains(table_body, "actions.3.route_to=standby-a\n");
    try expectContains(table_body, "actions.4.kind=demote_former_primary\n");
    try expectContains(table_body, "actions.4.admin_path=" ++ admin_api.routes.ha_rejoin_assess ++ "\n");
}

test "storage.ha admin exec operator plan assesses former primary rejoin" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "operator-rejoin");
    defer paths.deinit(alloc);
    var promoted_identity = testIdentity();
    promoted_identity.timeline_id = 2;
    promoted_identity.epoch = 2;

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, promoted_identity, .{});
    defer primary.close();

    var plan = try admin_cli.parse(alloc, &.{
        "operator",                     "plan",
        "--former-primary-id",          "primary-a",
        "--former-cluster-id",          "100",
        "--former-shard-id",            "10",
        "--former-table-id",            "20",
        "--former-timeline-id",         "1",
        "--former-epoch",               "1",
        "--former-last-lsn",            "10",
        "--retained-from-lsn",          "8",
        "--receipt-old-primary-id",     "primary-a",
        "--receipt-promoted-node-id",   "standby-a",
        "--receipt-parent-timeline-id", "1",
        "--receipt-parent-epoch",       "1",
        "--receipt-new-timeline-id",    "2",
        "--receipt-new-epoch",          "2",
        "--receipt-required-lsn",       "10",
        "--receipt-observed-lsn",       "10",
        "--receipt-generation",         "3",
        "--receipt-token",              "token",
        "--receipt-reason",             "operator-approved",
    });
    defer plan.deinit(alloc);

    var result = try execute(alloc, .{ .primary = &primary }, plan);
    defer result.deinit(alloc);
    const assessment = result.operator_plan.former_primary_assessment orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(rejoin.Action.rewind, assessment.action);
    try std.testing.expectEqual(@as(usize, 1), result.operator_plan.actions.len);
    try std.testing.expectEqual(operator.ActionKind.rewind_former_primary, result.operator_plan.actions[0].kind);
    try std.testing.expectEqual(@as(?u64, 10), result.operator_plan.actions[0].target_lsn);

    const table_body = try renderTableAlloc(alloc, result);
    defer alloc.free(table_body);
    try expectContains(table_body, "former_primary.action=rewind\n");
    try expectContains(table_body, "former_primary.reason=parent_timeline_retained\n");
    try expectContains(table_body, "former_primary.fork_lsn=10\n");
    try expectContains(table_body, "actions.0.kind=rewind_former_primary\n");
    try expectContains(table_body, "actions.0.phase=rejoin\n");
    try expectContains(table_body, "actions.0.target_lsn=10\n");
}

test "storage.ha admin exec executes former primary rejoin rewind and reseed commands" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "rejoin-execute");
    defer paths.deinit(alloc);

    var former_identity = testIdentity();
    former_identity.timeline_id = 1;
    former_identity.epoch = 1;
    {
        var former_primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, former_identity, .{});
        defer former_primary.close();
        for (0..10) |_| {
            _ = try former_primary.append(.{ .payload = "parent" });
        }
    }

    var former_log = try replication_log.ReplicationLog.open(paths.primary_log.ptr, .{});
    defer former_log.close();

    var promoted_identity = former_identity;
    promoted_identity.timeline_id = 2;
    promoted_identity.epoch = 2;
    var promoted_primary = try primary_mod.Primary.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, promoted_identity, .{});
    defer promoted_primary.close();
    try promoted_primary.createSlot("primary-a", 0);

    var rewind_plan = try admin_cli.parse(alloc, &.{
        "--table",
        "rejoin",
        "rewind",
        "--node-id",
        "primary-a",
        "--cluster-id",
        "100",
        "--shard-id",
        "10",
        "--table-id",
        "20",
        "--timeline-id",
        "1",
        "--epoch",
        "1",
        "--last-lsn",
        "10",
        "--retained-from-lsn",
        "8",
        "--fence-old-primary-id",
        "primary-a",
        "--fence-promoted-node-id",
        "standby-a",
        "--fence-parent-timeline-id",
        "1",
        "--fence-parent-epoch",
        "1",
        "--fence-new-timeline-id",
        "2",
        "--fence-new-epoch",
        "2",
        "--fence-required-lsn",
        "10",
        "--fence-observed-lsn",
        "10",
        "--fence-generation",
        "3",
        "--fence-token",
        "token",
    });
    defer rewind_plan.deinit(alloc);

    var rewind_result = try execute(alloc, .{ .former_primary_log = &former_log }, rewind_plan);
    defer rewind_result.deinit(alloc);
    try std.testing.expectEqual(rejoin.Action.rewind, rewind_result.rejoin_rewind.assessment.action);
    try std.testing.expectEqual(@as(u64, 10), rewind_result.rejoin_rewind.rewind.previous_last_lsn);
    try std.testing.expectEqual(@as(u64, 11), rewind_result.rejoin_rewind.rewind.current_last_lsn);
    try std.testing.expectEqual(@as(u64, 0), rewind_result.rejoin_rewind.rewind.discarded_lsn_count);

    const rewind_table = try renderTableAlloc(alloc, rewind_result);
    defer alloc.free(rewind_table);
    try expectContains(rewind_table, "result=rejoin_rewind\n");
    try expectContains(rewind_table, "action.action_id=rejoin_rewind:primary-a\n");
    try expectContains(rewind_table, "action.action_kind=rejoin_rewind\n");
    try expectContains(rewind_table, "action.target=primary-a\n");
    try expectContains(rewind_table, "action.state=applied\n");
    try expectContains(rewind_table, "assessment.action=rewind\n");
    try expectContains(rewind_table, "rewind.node_id=primary-a\n");
    try expectContains(rewind_table, "rewind.discarded_lsn_count=0\n");
    const rewind_json = try renderJsonWithContextAlloc(alloc, null, rewind_result);
    defer alloc.free(rewind_json);
    try expectContains(rewind_json, "\"schema_version\":1");
    try expectContains(rewind_json, "\"action_kind\":\"rejoin_rewind\"");
    try expectContains(rewind_json, "\"assessment\"");
    try expectContains(rewind_json, "\"rewind\"");
    try expectContains(rewind_json, "\"discarded_lsn_count\":0");

    var reseed_plan = try admin_cli.parse(alloc, &.{
        "--table",
        "rejoin",
        "reseed",
        "--node-id",
        "primary-a",
        "--cluster-id",
        "100",
        "--shard-id",
        "10",
        "--table-id",
        "20",
        "--timeline-id",
        "1",
        "--epoch",
        "1",
        "--last-lsn",
        "12",
        "--retained-from-lsn",
        "11",
        "--fence-old-primary-id",
        "primary-a",
        "--fence-promoted-node-id",
        "standby-a",
        "--fence-parent-timeline-id",
        "1",
        "--fence-parent-epoch",
        "1",
        "--fence-new-timeline-id",
        "2",
        "--fence-new-epoch",
        "2",
        "--fence-required-lsn",
        "10",
        "--fence-observed-lsn",
        "10",
        "--fence-generation",
        "3",
        "--fence-token",
        "token",
    });
    defer reseed_plan.deinit(alloc);

    var reseed_result = try execute(alloc, .{ .primary = &promoted_primary }, reseed_plan);
    defer reseed_result.deinit(alloc);
    try std.testing.expectEqual(rejoin.Action.reseed, reseed_result.rejoin_reseed.assessment.action);
    try std.testing.expectEqualStrings("primary-a", reseed_result.rejoin_reseed.reseed.slot_name);
    try std.testing.expect(reseed_result.rejoin_reseed.reseed.reseed_required);
    try std.testing.expect(reseed_result.rejoin_reseed.reseed.base_backup_required);

    const reseed_table = try renderTableAlloc(alloc, reseed_result);
    defer alloc.free(reseed_table);
    try expectContains(reseed_table, "result=rejoin_reseed\n");
    try expectContains(reseed_table, "action.action_id=rejoin_reseed:primary-a\n");
    try expectContains(reseed_table, "assessment.action=reseed\n");
    try expectContains(reseed_table, "reseed.node_id=primary-a\n");
    try expectContains(reseed_table, "reseed.slot_name=primary-a\n");
    const reseed_json = try renderJsonWithContextAlloc(alloc, .{ .primary_node_id = "standby-a" }, reseed_result);
    defer alloc.free(reseed_json);
    try expectContains(reseed_json, "\"schema_version\":1");
    try expectContains(reseed_json, "\"action_kind\":\"rejoin_reseed\"");
    try expectContains(reseed_json, "\"node_id\":\"standby-a\"");
    try expectContains(reseed_json, "\"assessment\"");
    try expectContains(reseed_json, "\"reseed\"");
    try expectContains(reseed_json, "\"slot_name\":\"primary-a\"");
}

test "storage.ha admin exec finishes and bootstraps seed manifests from files" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "seed-files");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();

    var begin_plan = try admin_cli.parse(alloc, &.{ "seed", "begin", "--slot", "standby-a", "--manifest-id", "base-0001" });
    defer begin_plan.deinit(alloc);
    var begun = try execute(alloc, .{ .primary = &primary }, begin_plan);
    defer begun.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), begun.seed_begin.backup_lsn);
    const begin_table = try renderTableAlloc(alloc, begun);
    defer alloc.free(begin_table);
    try expectContains(begin_table, "action.action_id=base_backup_begin:base-0001\n");
    try expectContains(begin_table, "action.action_kind=base_backup_begin\n");
    try expectContains(begin_table, "action.target=base-0001\n");
    try expectContains(begin_table, "action.state=applied\n");
    try expectContains(begin_table, "slot_name=standby-a\n");
    try expectContains(begin_table, "manifest_id=base-0001\n");
    const begin_json = try renderJsonWithContextAlloc(alloc, .{ .primary_node_id = "primary-a" }, begun);
    defer alloc.free(begin_json);
    try expectContains(begin_json, "\"schema_version\":1");
    try expectContains(begin_json, "\"action_kind\":\"base_backup_begin\"");
    try expectContains(begin_json, "\"node_id\":\"primary-a\"");
    try expectContains(begin_json, "\"slot_name\":\"standby-a\"");
    try std.testing.expectEqual(@as(u64, 2), try primary.append(.{ .payload = "during-copy" }));

    const files = seedFiles();
    const encoded_manifest = try backup_manifest.encodeAlloc(alloc, .{
        .identity = identity,
        .manifest_id = "base-0001",
        .backup_lsn = begun.seed_begin.backup_lsn,
        .checkpoint_lsn = 2,
        .files = &files,
    });
    defer alloc.free(encoded_manifest);

    const manifest_path = try std.fs.path.join(alloc, &.{ paths.backup_root, "backup.afha" });
    defer alloc.free(manifest_path);
    const manifest_file_path = try std.fs.path.join(alloc, &.{ paths.backup_root, "manifest" });
    defer alloc.free(manifest_file_path);
    const sstable_path = try std.fs.path.join(alloc, &.{ paths.backup_root, "sst/0001" });
    defer alloc.free(sstable_path);
    try writeTestFile(manifest_path, encoded_manifest);
    try writeTestFile(manifest_file_path, "manifest");
    try writeTestFile(sstable_path, "sstable");

    var finish_plan = try admin_cli.parse(alloc, &.{ "seed", "finish", "--manifest", manifest_path });
    defer finish_plan.deinit(alloc);
    var finished = try execute(alloc, .{ .primary = &primary }, finish_plan);
    defer finished.deinit(alloc);
    try std.testing.expectEqualStrings("base-0001", finished.seed_finish.manifest_id);
    try std.testing.expectEqual(@as(u64, 1), finished.seed_finish.backup_lsn);
    try std.testing.expectEqual(@as(u64, 3), finished.seed_finish.end_record_lsn);
    const finish_table = try renderTableAlloc(alloc, finished);
    defer alloc.free(finish_table);
    try expectContains(finish_table, "action.action_id=base_backup_finish:base-0001\n");
    try expectContains(finish_table, "end_record_lsn=3\n");
    const finish_json = try renderJsonWithContextAlloc(alloc, .{ .primary_node_id = "primary-a" }, finished);
    defer alloc.free(finish_json);
    try expectContains(finish_json, "\"schema_version\":1");
    try expectContains(finish_json, "\"action_kind\":\"base_backup_finish\"");
    try expectContains(finish_json, "\"node_id\":\"primary-a\"");
    try expectContains(finish_json, "\"end_record_lsn\":3");

    var bootstrap_plan = try admin_cli.parse(alloc, &.{ "seed", "bootstrap", "--manifest", manifest_path, "--content-root", paths.backup_root });
    defer bootstrap_plan.deinit(alloc);
    var bootstrapped = try execute(alloc, .{ .standby = &standby }, bootstrap_plan);
    defer bootstrapped.deinit(alloc);
    try std.testing.expectEqualStrings("base-0001", bootstrapped.seed_bootstrap.manifest_id);
    try std.testing.expectEqual(@as(u64, 1), bootstrapped.seed_bootstrap.backup_lsn);
    try std.testing.expectEqual(@as(u64, 2), bootstrapped.seed_bootstrap.checkpoint_lsn);
    try std.testing.expectEqual(@as(u64, 3), standby.nextReceiveLsn());

    const table_body = try renderTableAlloc(alloc, bootstrapped);
    defer alloc.free(table_body);
    try expectContains(table_body, "result=seed_bootstrap\n");
    try expectContains(table_body, "action.action_id=standby_bootstrap:base-0001\n");
    try expectContains(table_body, "manifest_id=base-0001\n");
    try expectContains(table_body, "checkpoint_lsn=2\n");
    const context_table_body = try renderTableForContextAlloc(alloc, .{ .standby_node_id = "standby-a" }, bootstrapped);
    defer alloc.free(context_table_body);
    try expectContains(context_table_body, "action.node_id=standby-a\n");
    const bootstrap_json = try renderJsonWithContextAlloc(alloc, .{ .standby_node_id = "standby-a" }, bootstrapped);
    defer alloc.free(bootstrap_json);
    try expectContains(bootstrap_json, "\"schema_version\":1");
    try expectContains(bootstrap_json, "\"action_kind\":\"standby_bootstrap\"");
    try expectContains(bootstrap_json, "\"node_id\":\"standby-a\"");
    try expectContains(bootstrap_json, "\"checkpoint_lsn\":2");
}

test "storage.ha admin exec streams one local replication session" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "stream-once");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();

    try primary.createSlot("standby-a", 0);
    try std.testing.expectEqual(@as(u64, 1), try primary.append(.{ .payload = "one" }));
    try std.testing.expectEqual(@as(u64, 2), try primary.append(.{ .payload = "two" }));

    var stream_plan = try admin_cli.parse(alloc, &.{ "stream", "once", "--slot", "standby-a" });
    defer stream_plan.deinit(alloc);
    var streamed = try execute(alloc, .{ .primary = &primary, .standby = &standby }, stream_plan);
    defer streamed.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), streamed.stream_once.received_count);
    try std.testing.expectEqual(@as(usize, 2), streamed.stream_once.applied_count);
    try std.testing.expectEqual(@as(u64, 2), streamed.stream_once.progress.received_lsn);
    try std.testing.expectEqual(@as(u64, 2), streamed.stream_once.progress.applied_lsn);
    const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 2), slot.received_lsn);
    try std.testing.expectEqual(@as(u64, 2), slot.applied_lsn);

    const table_body = try renderTableAlloc(alloc, streamed);
    defer alloc.free(table_body);
    try expectContains(table_body, "result=stream_once\n");
    try expectContains(table_body, "received_count=2\n");
    try expectContains(table_body, "applied_lsn=2\n");

    var rendered_plan = try admin_cli.parse(alloc, &.{ "--table", "stream", "once", "standby-a" });
    defer rendered_plan.deinit(alloc);
    var rendered = try executeAndRenderAlloc(alloc, .{ .primary = &primary, .standby = &standby }, rendered_plan);
    defer rendered.deinit(alloc);
    try std.testing.expectEqualStrings("text/plain; charset=utf-8", rendered.content_type);
    try expectContains(rendered.body, "received_count=0\n");
    try expectContains(rendered.body, "applied_count=0\n");
}

test "storage.ha admin exec runs read commit promote and rejoin commands" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "promote");
    defer paths.deinit(alloc);
    const identity = testIdentity();
    const MetadataProgress = struct {
        lsn: u64,

        fn load(ctx: *anyopaque) !u64 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            return self.lsn;
        }
    };

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();
    var fence_store = try fencing.Store.open(alloc, paths.fence_wal.ptr, .{});
    defer fence_store.close();

    try primary.createSlot("standby-a", 0);
    _ = try primary.append(.{ .payload = "one" });
    try primary.standbyStatusUpdate("standby-a", identity.timeline_id, 1, 1);
    _ = try standby.receive(baseRecord(identity, 1, "one"));
    var apply_counter = ApplyCounter{};
    try std.testing.expectEqual(@as(usize, 1), try standby.applyAvailable(&apply_counter, ApplyCounter.apply));

    var commit_plan = try admin_cli.parse(alloc, &.{ "commit", "check", "--target-lsn", "1", "--sync-mode", "remote-apply", "--sync-standby", "standby-a" });
    defer commit_plan.deinit(alloc);
    var commit = try execute(alloc, .{ .primary = &primary }, commit_plan);
    defer commit.deinit(alloc);
    try std.testing.expectEqual(commit_gate.Action.acknowledge, commit.commit_check.action);

    var append_plan = try admin_cli.parse(alloc, &.{
        "--table",
        "commit",
        "append",
        "--payload",
        "two",
        "--sync-mode",
        "remote-apply",
        "--sync-standby",
        "standby-a",
        "--sync-failure",
        "degrade-to-async",
    });
    defer append_plan.deinit(alloc);
    var appended = try execute(alloc, .{ .primary = &primary }, append_plan);
    defer appended.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), appended.commit_append.lsn);
    try std.testing.expectEqual(commit_gate.Action.acknowledge_degraded, appended.commit_append.gate.action);
    try std.testing.expectEqual(primary_mod.DurabilityStatus.degraded_to_async, appended.commit_append.gate.decision.status);

    const append_table = try renderTableAlloc(alloc, appended);
    defer alloc.free(append_table);
    try expectContains(append_table, "result=commit_append\n");
    try expectContains(append_table, "lsn=2\n");
    try expectContains(append_table, "action=acknowledge_degraded\n");
    try expectContains(append_table, "durability.progress_lsn=1\n");
    try expectContains(append_table, "durability.missing_lsn_count=1\n");

    var read_plan = try admin_cli.parse(alloc, &.{ "read", "check", "--at-least-lsn", "1" });
    defer read_plan.deinit(alloc);
    var read = try execute(alloc, .{ .standby = &standby }, read_plan);
    defer read.deinit(alloc);
    try std.testing.expectEqual(read_gate.Action.serve_standby, read.read_check.action);

    var metadata_read_plan = try admin_cli.parse(alloc, &.{
        "--table",
        "read",
        "check",
        "--at-least-lsn",
        "1",
        "--metadata-applied-lsn",
        "0",
    });
    defer metadata_read_plan.deinit(alloc);
    var metadata_read = try execute(alloc, .{ .standby = &standby }, metadata_read_plan);
    defer metadata_read.deinit(alloc);
    try std.testing.expectEqual(read_gate.Action.wait_for_metadata, metadata_read.read_check.action);
    try std.testing.expectEqual(@as(u64, 1), metadata_read.read_check.metadata_missing_lsn_count);
    const metadata_read_table = try renderTableAlloc(alloc, metadata_read);
    defer alloc.free(metadata_read_table);
    try expectContains(metadata_read_table, "result=read_check\n");
    try expectContains(metadata_read_table, "action=wait_for_metadata\n");
    try expectContains(metadata_read_table, "metadata_applied_lsn=0\n");
    try expectContains(metadata_read_table, "metadata_missing_lsn_count=1\n");

    var provider_progress = MetadataProgress{ .lsn = 0 };
    var provider_read_plan = try admin_cli.parse(alloc, &.{ "read", "check", "--at-least-lsn", "1" });
    defer provider_read_plan.deinit(alloc);
    var provider_read = try execute(alloc, .{
        .standby = &standby,
        .metadata_applied_lsn_ctx = &provider_progress,
        .metadata_applied_lsn_fn = MetadataProgress.load,
    }, provider_read_plan);
    defer provider_read.deinit(alloc);
    try std.testing.expectEqual(read_gate.Action.wait_for_metadata, provider_read.read_check.action);
    try std.testing.expectEqual(@as(?u64, 0), provider_read.read_check.metadata_applied_lsn);
    try std.testing.expectEqual(@as(u64, 1), provider_read.read_check.metadata_missing_lsn_count);

    provider_progress.lsn = 1;
    var provider_ready = try execute(alloc, .{
        .standby = &standby,
        .metadata_applied_lsn_ctx = &provider_progress,
        .metadata_applied_lsn_fn = MetadataProgress.load,
    }, provider_read_plan);
    defer provider_ready.deinit(alloc);
    try std.testing.expectEqual(read_gate.Action.serve_standby, provider_ready.read_check.action);
    try std.testing.expectEqual(@as(?u64, 1), provider_ready.read_check.metadata_applied_lsn);

    var primary_write_plan = try admin_cli.parse(alloc, &.{ "--table", "write", "check", "--role", "primary" });
    defer primary_write_plan.deinit(alloc);
    var primary_write = try execute(alloc, .{ .primary = &primary }, primary_write_plan);
    defer primary_write.deinit(alloc);
    try std.testing.expect(primary_write.write_check.canWrite());
    try std.testing.expectEqual(write_gate.Action.allow_write, primary_write.write_check.action);
    const primary_write_table = try renderTableAlloc(alloc, primary_write);
    defer alloc.free(primary_write_table);
    try expectContains(primary_write_table, "result=write_check\n");
    try expectContains(primary_write_table, "action=allow_write\n");
    try expectContains(primary_write_table, "can_write=true\n");
    try std.testing.expectError(
        error.PrimaryNodeIDUnavailable,
        execute(alloc, .{ .primary = &primary, .primary_node_id = "primary/a", .fence_store = &fence_store }, primary_write_plan),
    );

    var standby_write_plan = try admin_cli.parse(alloc, &.{ "write", "check", "--role", "standby" });
    defer standby_write_plan.deinit(alloc);
    var standby_write = try execute(alloc, .{ .standby = &standby }, standby_write_plan);
    defer standby_write.deinit(alloc);
    try std.testing.expect(!standby_write.write_check.canWrite());
    try std.testing.expectEqual(write_gate.Action.reject_read_only_standby, standby_write.write_check.action);

    var owner_job_plan = try admin_cli.parse(alloc, &.{ "--table", "owner-job", "check", "--role", "standby", "--kind", "retention-advance" });
    defer owner_job_plan.deinit(alloc);
    var owner_job = try execute(alloc, .{ .standby = &standby }, owner_job_plan);
    defer owner_job.deinit(alloc);
    try std.testing.expect(!owner_job.owner_job_check.canRun());
    try std.testing.expectEqual(owner_job_gate.Action.disable_on_standby, owner_job.owner_job_check.action);
    const owner_job_table = try renderTableAlloc(alloc, owner_job);
    defer alloc.free(owner_job_table);
    try expectContains(owner_job_table, "result=owner_job_check\n");
    try expectContains(owner_job_table, "kind=retention_advance\n");
    try expectContains(owner_job_table, "action=disable_on_standby\n");

    var fence_plan = try admin_cli.parse(alloc, &.{
        "--table",
        "fence",
        "acquire",
        "--cluster-id",
        "100",
        "--shard-id",
        "10",
        "--table-id",
        "20",
        "--timeline-id",
        "1",
        "--epoch",
        "1",
        "--old-primary-id",
        "primary-a",
        "--promoted-node-id",
        "standby-a",
        "--new-timeline-id",
        "2",
        "--new-epoch",
        "2",
        "--required-lsn",
        "1",
        "--observed-lsn",
        "1",
        "--reason",
        "operator-approved",
    });
    defer fence_plan.deinit(alloc);
    var fenced = try execute(alloc, .{ .fence_store = &fence_store }, fence_plan);
    defer fenced.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), fenced.fence_acquire.receipt.generation);
    try std.testing.expectEqualStrings("standby-a", fenced.fence_acquire.receipt.promoted_node_id);

    const fence_table = try renderTableAlloc(alloc, fenced);
    defer alloc.free(fence_table);
    try expectContains(fence_table, "result=fence_acquire\n");
    try expectContains(fence_table, "action.action_id=fence_acquire:standby-a\n");
    try expectContains(fence_table, "action.action_kind=fence_acquire\n");
    try expectContains(fence_table, "action.target=standby-a\n");
    try expectContains(fence_table, "action.state=applied\n");
    try expectContains(fence_table, "promoted_node_id=standby-a\n");
    try expectContains(fence_table, "generation=1\n");
    const fence_json = try renderJsonWithContextAlloc(alloc, null, fenced);
    defer alloc.free(fence_json);
    try expectContains(fence_json, "\"schema_version\":1");
    try expectContains(fence_json, "\"action_kind\":\"fence_acquire\"");
    try expectContains(fence_json, "\"receipt\"");
    try expectContains(fence_json, "\"promoted_node_id\":\"standby-a\"");

    var current_fence_plan = try admin_cli.parse(alloc, &.{ "--table", "fence", "current" });
    defer current_fence_plan.deinit(alloc);
    var current_fence = try execute(alloc, .{ .fence_store = &fence_store }, current_fence_plan);
    defer current_fence.deinit(alloc);
    try std.testing.expect(current_fence.fence_current != null);
    const current_fence_table = try renderTableAlloc(alloc, current_fence);
    defer alloc.free(current_fence_table);
    try expectContains(current_fence_table, "result=fence_current\n");
    try expectContains(current_fence_table, "held=true\n");
    try expectContains(current_fence_table, "old_primary_id=primary-a\n");
    const current_fence_json = try renderJsonWithContextAlloc(alloc, null, current_fence);
    defer alloc.free(current_fence_json);
    try expectContains(current_fence_json, "\"schema_version\":1");
    try expectContains(current_fence_json, "\"held\":true");
    try expectContains(current_fence_json, "\"receipt\"");

    var direct_assess_plan = try admin_cli.parse(alloc, &.{ "--table", "promote", "assess", "--required-lsn", "1", "--fencing-confirmed" });
    defer direct_assess_plan.deinit(alloc);
    var direct_assess = try execute(alloc, .{ .standby = &standby }, direct_assess_plan);
    defer direct_assess.deinit(alloc);
    try std.testing.expect(direct_assess.promote_assess.safe);
    try std.testing.expect(direct_assess.promote_assess.can_promote);
    const direct_assess_table = try renderTableAlloc(alloc, direct_assess);
    defer alloc.free(direct_assess_table);
    try expectContains(direct_assess_table, "result=promote_assess\n");
    try expectContains(direct_assess_table, "assessment.can_promote=true\n");

    const direct_assess_context_table = try renderTableForContextAlloc(alloc, .{ .standby_node_id = "standby-a" }, direct_assess);
    defer alloc.free(direct_assess_context_table);
    try expectContains(direct_assess_context_table, "action.action_id=promotion_assess:standby-a\n");
    try expectContains(direct_assess_context_table, "action.action_kind=promotion_assess\n");
    try expectContains(direct_assess_context_table, "action.target=standby-a\n");
    try expectContains(direct_assess_context_table, "action.state=assessed\n");
    try expectContains(direct_assess_context_table, "action.node_id=standby-a\n");

    var direct_assess_json_plan = try admin_cli.parse(alloc, &.{ "--json", "promote", "assess", "--required-lsn", "1", "--fencing-confirmed" });
    defer direct_assess_json_plan.deinit(alloc);
    var direct_assess_json = try executeAndRenderAlloc(alloc, .{ .standby = &standby, .standby_node_id = "standby-a" }, direct_assess_json_plan);
    defer direct_assess_json.deinit(alloc);
    try std.testing.expectEqualStrings("application/json", direct_assess_json.content_type);
    try expectContains(direct_assess_json.body, "\"schema_version\":1");
    try expectContains(direct_assess_json.body, "\"action\":{\"action_id\":\"promotion_assess:standby-a\"");
    try expectContains(direct_assess_json.body, "\"action_kind\":\"promotion_assess\"");
    try expectContains(direct_assess_json.body, "\"target\":\"standby-a\"");
    try expectContains(direct_assess_json.body, "\"state\":\"assessed\"");
    try expectContains(direct_assess_json.body, "\"node_id\":\"standby-a\"");
    try expectContains(direct_assess_json.body, "\"assessment\":");
    try expectContains(direct_assess_json.body, "\"can_promote\":true");

    var fenced_assess_plan = try admin_cli.parse(alloc, &.{ "--prometheus", "promote", "assess", "--current-fence" });
    defer fenced_assess_plan.deinit(alloc);
    var fenced_assess = try execute(alloc, .{ .standby = &standby, .fence_store = &fence_store }, fenced_assess_plan);
    defer fenced_assess.deinit(alloc);
    try std.testing.expect(fenced_assess.promote_assess.fencing_confirmed);
    try std.testing.expect(fenced_assess.promote_assess.can_promote);
    var fenced_assess_prometheus = try renderOutputAlloc(alloc, fenced_assess, fenced_assess_plan.output);
    defer fenced_assess_prometheus.deinit(alloc);
    try std.testing.expectEqualStrings("text/plain; version=0.0.4", fenced_assess_prometheus.content_type);
    try expectContains(fenced_assess_prometheus.body, "antfly_ha_promotion_can_promote 1\n");

    var promote_plan = try admin_cli.parse(alloc, &.{ "--table", "promote", "--current-fence" });
    defer promote_plan.deinit(alloc);
    var promoted = try execute(alloc, .{ .standby = &standby, .fence_store = &fence_store }, promote_plan);
    defer promoted.deinit(alloc);
    try std.testing.expect(promoted.promote_current_fence.assessment.can_promote);
    try std.testing.expectEqual(@as(u64, 2), standby.identitySnapshot().timeline_id);
    const promoted_table = try renderTableAlloc(alloc, promoted);
    defer alloc.free(promoted_table);
    try expectContains(promoted_table, "result=promote_current_fence\n");
    try expectContains(promoted_table, "action.action_id=promotion:standby-a\n");
    try expectContains(promoted_table, "action.action_kind=promotion\n");
    try expectContains(promoted_table, "action.target=standby-a\n");
    try expectContains(promoted_table, "action.state=applied\n");
    try expectContains(promoted_table, "promotion.node_id=standby-a\n");
    try expectContains(promoted_table, "promotion.new_identity.timeline_id=2\n");
    const promoted_json = try renderJsonWithContextAlloc(alloc, null, promoted);
    defer alloc.free(promoted_json);
    try expectContains(promoted_json, "\"schema_version\":1");
    try expectContains(promoted_json, "\"action_kind\":\"promotion\"");
    try expectContains(promoted_json, "\"promotion\"");
    try expectContains(promoted_json, "\"fence_token\":\"");

    var rejoin_plan = try admin_cli.parse(alloc, &.{
        "rejoin",              "assess",
        "--node-id",           "primary-a",
        "--cluster-id",        "100",
        "--shard-id",          "10",
        "--table-id",          "20",
        "--timeline-id",       "1",
        "--epoch",             "1",
        "--last-lsn",          "1",
        "--retained-from-lsn", "1",
    });
    defer rejoin_plan.deinit(alloc);
    var rejoin_result = try execute(alloc, .{}, rejoin_plan);
    defer rejoin_result.deinit(alloc);
    try std.testing.expectEqual(rejoin.Action.reject_unfenced, rejoin_result.rejoin_assess.action);
    const rejoin_table = try renderTableAlloc(alloc, rejoin_result);
    defer alloc.free(rejoin_table);
    try expectContains(rejoin_table, "action.action_id=rejoin_assess:primary-a\n");
    try expectContains(rejoin_table, "action.action_kind=rejoin_assess\n");
    try expectContains(rejoin_table, "action.target=primary-a\n");
    try expectContains(rejoin_table, "action.state=assessed\n");
    const rejoin_json = try renderJsonWithContextAlloc(alloc, null, rejoin_result);
    defer alloc.free(rejoin_json);
    try expectContains(rejoin_json, "\"schema_version\":1");
    try expectContains(rejoin_json, "\"action_kind\":\"rejoin_assess\"");
    try expectContains(rejoin_json, "\"assessment\"");
    try expectContains(rejoin_json, "\"action\":\"reject_unfenced\"");
    const rejoin_prometheus = try renderPrometheusAlloc(alloc, rejoin_result);
    defer alloc.free(rejoin_prometheus);
    try expectContains(rejoin_prometheus, "antfly_ha_rejoin_action_code 0\n");
    try expectContains(rejoin_prometheus, "antfly_ha_rejoin_reason_code 0\n");
    try expectContains(rejoin_prometheus, "antfly_ha_rejoin_rejected_unfenced 1\n");

    const read_json = try renderJsonAlloc(alloc, read);
    defer alloc.free(read_json);
    try expectContains(read_json, "\"schema_version\":1");
    try expectContains(read_json, "\"read_check\"");
    try expectContains(read_json, "\"action\":\"serve_standby\"");

    try std.testing.expectError(error.PrometheusUnsupportedForResult, renderPrometheusAlloc(alloc, read));

    var json_plan = try admin_cli.parse(alloc, &.{ "read", "check", "--at-least-lsn", "1" });
    defer json_plan.deinit(alloc);
    var rendered_json = try executeAndRenderAlloc(alloc, .{ .standby = &standby }, json_plan);
    defer rendered_json.deinit(alloc);
    try std.testing.expectEqualStrings("application/json", rendered_json.content_type);
    try expectContains(rendered_json.body, "\"schema_version\":1");
    try expectContains(rendered_json.body, "\"decision\"");
    try expectContains(rendered_json.body, "\"action\":\"serve_standby\"");

    var table_plan = try admin_cli.parse(alloc, &.{ "--table", "read", "check", "--at-least-lsn", "1" });
    defer table_plan.deinit(alloc);
    var rendered_table = try executeAndRenderAlloc(alloc, .{ .standby = &standby }, table_plan);
    defer rendered_table.deinit(alloc);
    try std.testing.expectEqualStrings("text/plain; charset=utf-8", rendered_table.content_type);
    try expectContains(rendered_table.body, "result=read_check\n");
    try expectContains(rendered_table.body, "action=serve_standby\n");
    try expectContains(rendered_table.body, "applied_lsn=");
}

fn expectContains(haystack: []const u8, needle: []const u8) !void {
    try std.testing.expect(std.mem.indexOf(u8, haystack, needle) != null);
}
