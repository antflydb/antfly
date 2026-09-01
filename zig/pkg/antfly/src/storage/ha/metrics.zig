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

//! Stable HA metric snapshots derived from the admin/status surfaces.
//!
//! These structs intentionally contain only numeric gauges plus owned slot
//! labels. Prometheus exporters, CLI status commands, and operators can map
//! them to their native format without re-implementing lag, retention, sync, or
//! promotion calculations.

const std = @import("std");
const Allocator = std.mem.Allocator;
const primary_mod = @import("primary.zig");
const rejoin = @import("rejoin.zig");
const slot_store = @import("slot_store.zig");
const status_mod = @import("status.zig");

pub const SlotStatusCode = enum(u64) {
    healthy = 0,
    lagging = 1,
    reseed_required = 2,
};

pub const DurabilityStatusCode = enum(u64) {
    satisfied = 0,
    would_block = 1,
    fail_closed = 2,
    degraded_to_async = 3,
    not_configured = 4,
};

pub const RejoinActionCode = enum(u64) {
    reject_unfenced = 0,
    already_current = 1,
    rewind = 2,
    reseed = 3,
};

pub const RejoinReasonCode = enum(u64) {
    no_fence = 0,
    current_timeline = 1,
    parent_timeline_retained = 2,
    parent_timeline_wal_expired = 3,
    incompatible_timeline = 4,
    wrong_old_primary = 5,
    wrong_cluster = 6,
    wrong_shard = 7,
    wrong_table = 8,
    local_lsn_before_fork = 9,
};

pub const SlotMetrics = struct {
    name: []const u8,
    active: u64,
    reseed_required: u64,
    received_lsn: u64,
    applied_lsn: u64,
    safe_read_lsn: u64,
    restart_lsn: u64,
    write_lag_lsn: u64,
    apply_lag_lsn: u64,
    safe_read_lag_lsn: u64,
    retention_lag_lsn: u64,
    status_code: u64,
    last_error: u64,
};

pub const PrimaryMetrics = struct {
    current_lsn: u64,
    slot_count: u64,
    active_slots: u64,
    reseed_required_slots: u64,
    max_write_lag_lsn: u64,
    max_apply_lag_lsn: u64,
    max_safe_read_lag_lsn: u64,
    max_retention_lag_lsn: u64,
    retention_oldest_restart_lsn: u64,
    retention_retained_lsn_count: u64,
    retention_retained_byte_count: u64,
    retention_retained_age_ns: u64,
    retention_active_slots: u64,
    retention_reseed_recommended: u64,
    durability_configured: u64,
    durability_satisfied: u64,
    durability_degraded: u64,
    durability_status_code: u64,
    durability_target_lsn: u64,
    durability_progress_lsn: u64,
    durability_missing_lsn_count: u64,
    durability_required_count: u64,
    durability_satisfied_count: u64,
    durability_candidate_count: u64,
    slots: []SlotMetrics,

    pub fn deinit(self: *PrimaryMetrics, alloc: Allocator) void {
        for (self.slots) |slot| alloc.free(slot.name);
        alloc.free(self.slots);
        self.* = undefined;
    }
};

pub const StandbyMetrics = struct {
    received_lsn: u64,
    applied_lsn: u64,
    safe_read_lsn: u64,
    upstream_configured: u64,
    write_lag_lsn: u64,
    receive_lag_lsn: u64,
    apply_lag_lsn: u64,
    unapplied_lsn_count: u64,
    caught_up_to_received: u64,
    can_serve_safe_reads: u64,
};

pub const PromotionMetrics = struct {
    required_lsn: u64,
    received_lsn: u64,
    applied_lsn: u64,
    has_required_lsn: u64,
    caught_up_to_received: u64,
    fencing_confirmed: u64,
    force: u64,
    data_loss_possible: u64,
    safe: u64,
    requires_fencing: u64,
    requires_force: u64,
    can_promote: u64,
};

pub const RejoinMetrics = struct {
    action_code: u64,
    reason_code: u64,
    rejected_unfenced: u64,
    already_current: u64,
    can_rewind: u64,
    requires_reseed: u64,
    target_timeline_id: u64,
    target_epoch: u64,
    parent_cluster_id: u64,
    parent_shard_id: u64,
    parent_table_id: u64,
    parent_timeline_id: u64,
    parent_epoch: u64,
    fork_lsn: u64,
    former_last_lsn: u64,
    retained_from_lsn: u64,
    data_loss_discarded: u64,
};

pub fn fromPrimarySnapshot(alloc: Allocator, snapshot: status_mod.PrimarySnapshot) !PrimaryMetrics {
    const slots = try alloc.alloc(SlotMetrics, snapshot.slots.len);
    errdefer alloc.free(slots);

    var filled: usize = 0;
    errdefer for (slots[0..filled]) |slot| alloc.free(slot.name);

    var active_slots: u64 = 0;
    var reseed_required_slots: u64 = 0;
    var max_write_lag_lsn: u64 = 0;
    var max_apply_lag_lsn: u64 = 0;
    var max_safe_read_lag_lsn: u64 = 0;
    var max_retention_lag_lsn: u64 = 0;

    for (snapshot.slots, 0..) |slot, idx| {
        if (slot.active) active_slots += 1;
        if (slot.reseed_required) reseed_required_slots += 1;
        max_write_lag_lsn = @max(max_write_lag_lsn, slot.write_lag_lsn);
        max_apply_lag_lsn = @max(max_apply_lag_lsn, slot.apply_lag_lsn);
        max_safe_read_lag_lsn = @max(max_safe_read_lag_lsn, slot.safe_read_lag_lsn);
        max_retention_lag_lsn = @max(max_retention_lag_lsn, slot.retention_lag_lsn);

        slots[idx] = .{
            .name = try alloc.dupe(u8, slot.name),
            .active = boolGauge(slot.active),
            .reseed_required = boolGauge(slot.reseed_required),
            .received_lsn = slot.received_lsn,
            .applied_lsn = slot.applied_lsn,
            .safe_read_lsn = slot.safe_read_lsn,
            .restart_lsn = slot.restart_lsn,
            .write_lag_lsn = slot.write_lag_lsn,
            .apply_lag_lsn = slot.apply_lag_lsn,
            .safe_read_lag_lsn = slot.safe_read_lag_lsn,
            .retention_lag_lsn = slot.retention_lag_lsn,
            .status_code = @backingInt(slotStatusCode(slot.status)),
            .last_error = boolGauge(slot.last_error != null),
        };
        filled += 1;
    }

    const durability = snapshot.durability;
    const durability_status_code = if (durability) |decision|
        @backingInt(durabilityStatusCode(decision.status))
    else
        @backingInt(DurabilityStatusCode.not_configured);
    const durability_satisfied = if (durability) |decision|
        boolGauge(decision.status == .satisfied)
    else
        0;
    const durability_degraded = if (durability) |decision|
        boolGauge(decision.status != .satisfied)
    else
        0;

    return .{
        .current_lsn = snapshot.current_lsn,
        .slot_count = @intCast(snapshot.slots.len),
        .active_slots = active_slots,
        .reseed_required_slots = reseed_required_slots,
        .max_write_lag_lsn = max_write_lag_lsn,
        .max_apply_lag_lsn = max_apply_lag_lsn,
        .max_safe_read_lag_lsn = max_safe_read_lag_lsn,
        .max_retention_lag_lsn = max_retention_lag_lsn,
        .retention_oldest_restart_lsn = snapshot.retention.oldest_restart_lsn,
        .retention_retained_lsn_count = snapshot.retention.retained_lsn_count,
        .retention_retained_byte_count = snapshot.retention.retained_byte_count,
        .retention_retained_age_ns = snapshot.retention.retained_age_ns,
        .retention_active_slots = @intCast(snapshot.retention.active_slots),
        .retention_reseed_recommended = @intCast(snapshot.retention.reseed_recommended),
        .durability_configured = boolGauge(durability != null),
        .durability_satisfied = durability_satisfied,
        .durability_degraded = durability_degraded,
        .durability_status_code = durability_status_code,
        .durability_target_lsn = if (durability) |decision| decision.target_lsn else 0,
        .durability_progress_lsn = if (durability) |decision| decision.progress_lsn else 0,
        .durability_missing_lsn_count = if (durability) |decision| decision.missing_lsn_count else 0,
        .durability_required_count = if (durability) |decision| @intCast(decision.required_count) else 0,
        .durability_satisfied_count = if (durability) |decision| @intCast(decision.satisfied_count) else 0,
        .durability_candidate_count = if (durability) |decision| @intCast(decision.candidate_count) else 0,
        .slots = slots,
    };
}

pub fn fromStandbySnapshot(snapshot: status_mod.StandbySnapshot) StandbyMetrics {
    return .{
        .received_lsn = snapshot.received_lsn,
        .applied_lsn = snapshot.applied_lsn,
        .safe_read_lsn = snapshot.safe_read_lsn,
        .upstream_configured = boolGauge(snapshot.upstream_lsn != null),
        .write_lag_lsn = snapshot.write_lag_lsn orelse 0,
        .receive_lag_lsn = snapshot.receive_lag_lsn orelse 0,
        .apply_lag_lsn = snapshot.apply_lag_lsn orelse 0,
        .unapplied_lsn_count = snapshot.unapplied_lsn_count,
        .caught_up_to_received = boolGauge(snapshot.caught_up_to_received),
        .can_serve_safe_reads = boolGauge(snapshot.can_serve_safe_reads),
    };
}

pub fn fromPromotionAssessment(assessment: status_mod.PromotionAssessment) PromotionMetrics {
    return .{
        .required_lsn = assessment.required_lsn,
        .received_lsn = assessment.received_lsn,
        .applied_lsn = assessment.applied_lsn,
        .has_required_lsn = boolGauge(assessment.has_required_lsn),
        .caught_up_to_received = boolGauge(assessment.caught_up_to_received),
        .fencing_confirmed = boolGauge(assessment.fencing_confirmed),
        .force = boolGauge(assessment.force),
        .data_loss_possible = boolGauge(assessment.data_loss_possible),
        .safe = boolGauge(assessment.safe),
        .requires_fencing = boolGauge(assessment.requires_fencing),
        .requires_force = boolGauge(assessment.requires_force),
        .can_promote = boolGauge(assessment.can_promote),
    };
}

pub fn fromRejoinAssessment(assessment: rejoin.Assessment) RejoinMetrics {
    return .{
        .action_code = @backingInt(rejoinActionCode(assessment.action)),
        .reason_code = @backingInt(rejoinReasonCode(assessment.reason)),
        .rejected_unfenced = boolGauge(assessment.action == .reject_unfenced),
        .already_current = boolGauge(assessment.action == .already_current),
        .can_rewind = boolGauge(assessment.action == .rewind),
        .requires_reseed = boolGauge(assessment.action == .reseed),
        .target_timeline_id = assessment.target_timeline_id,
        .target_epoch = assessment.target_epoch,
        .parent_cluster_id = assessment.parent_cluster_id,
        .parent_shard_id = assessment.parent_shard_id,
        .parent_table_id = assessment.parent_table_id,
        .parent_timeline_id = assessment.parent_timeline_id,
        .parent_epoch = assessment.parent_epoch,
        .fork_lsn = assessment.fork_lsn,
        .former_last_lsn = assessment.former_last_lsn,
        .retained_from_lsn = assessment.retained_from_lsn,
        .data_loss_discarded = boolGauge(assessment.data_loss_discarded),
    };
}

pub fn slotStatusCode(slot_status: slot_store.SlotStatus) SlotStatusCode {
    return switch (slot_status) {
        .healthy => .healthy,
        .lagging => .lagging,
        .reseed_required => .reseed_required,
    };
}

pub fn durabilityStatusCode(durability_status: primary_mod.DurabilityStatus) DurabilityStatusCode {
    return switch (durability_status) {
        .satisfied => .satisfied,
        .would_block => .would_block,
        .fail_closed => .fail_closed,
        .degraded_to_async => .degraded_to_async,
    };
}

pub fn rejoinActionCode(action: rejoin.Action) RejoinActionCode {
    return switch (action) {
        .reject_unfenced => .reject_unfenced,
        .already_current => .already_current,
        .rewind => .rewind,
        .reseed => .reseed,
    };
}

pub fn rejoinReasonCode(reason: rejoin.Reason) RejoinReasonCode {
    return switch (reason) {
        .no_fence => .no_fence,
        .current_timeline => .current_timeline,
        .parent_timeline_retained => .parent_timeline_retained,
        .parent_timeline_wal_expired => .parent_timeline_wal_expired,
        .incompatible_timeline => .incompatible_timeline,
        .wrong_old_primary => .wrong_old_primary,
        .wrong_cluster => .wrong_cluster,
        .wrong_shard => .wrong_shard,
        .wrong_table => .wrong_table,
        .local_lsn_before_fork => .local_lsn_before_fork,
    };
}

pub fn renderPrimaryPrometheusAlloc(alloc: Allocator, metrics: PrimaryMetrics) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try appendGauge(alloc, &out, "antfly_ha_primary_current_lsn", metrics.current_lsn);
    try appendGauge(alloc, &out, "antfly_ha_primary_slot_count", metrics.slot_count);
    try appendGauge(alloc, &out, "antfly_ha_primary_active_slots", metrics.active_slots);
    try appendGauge(alloc, &out, "antfly_ha_primary_reseed_required_slots", metrics.reseed_required_slots);
    try appendGauge(alloc, &out, "antfly_ha_primary_max_write_lag_lsn", metrics.max_write_lag_lsn);
    try appendGauge(alloc, &out, "antfly_ha_primary_max_apply_lag_lsn", metrics.max_apply_lag_lsn);
    try appendGauge(alloc, &out, "antfly_ha_primary_max_safe_read_lag_lsn", metrics.max_safe_read_lag_lsn);
    try appendGauge(alloc, &out, "antfly_ha_primary_max_retention_lag_lsn", metrics.max_retention_lag_lsn);
    try appendGauge(alloc, &out, "antfly_ha_primary_retention_oldest_restart_lsn", metrics.retention_oldest_restart_lsn);
    try appendGauge(alloc, &out, "antfly_ha_primary_retention_retained_lsn_count", metrics.retention_retained_lsn_count);
    try appendGauge(alloc, &out, "antfly_ha_primary_retention_retained_byte_count", metrics.retention_retained_byte_count);
    try appendGauge(alloc, &out, "antfly_ha_primary_retention_retained_age_ns", metrics.retention_retained_age_ns);
    try appendGauge(alloc, &out, "antfly_ha_primary_retention_active_slots", metrics.retention_active_slots);
    try appendGauge(alloc, &out, "antfly_ha_primary_retention_reseed_recommended", metrics.retention_reseed_recommended);
    try appendGauge(alloc, &out, "antfly_ha_primary_durability_configured", metrics.durability_configured);
    try appendGauge(alloc, &out, "antfly_ha_primary_durability_satisfied", metrics.durability_satisfied);
    try appendGauge(alloc, &out, "antfly_ha_primary_durability_degraded", metrics.durability_degraded);
    try appendGauge(alloc, &out, "antfly_ha_primary_durability_status_code", metrics.durability_status_code);
    try appendGauge(alloc, &out, "antfly_ha_primary_durability_target_lsn", metrics.durability_target_lsn);
    try appendGauge(alloc, &out, "antfly_ha_primary_durability_progress_lsn", metrics.durability_progress_lsn);
    try appendGauge(alloc, &out, "antfly_ha_primary_durability_missing_lsn_count", metrics.durability_missing_lsn_count);
    try appendGauge(alloc, &out, "antfly_ha_primary_durability_required_count", metrics.durability_required_count);
    try appendGauge(alloc, &out, "antfly_ha_primary_durability_satisfied_count", metrics.durability_satisfied_count);
    try appendGauge(alloc, &out, "antfly_ha_primary_durability_candidate_count", metrics.durability_candidate_count);

    try appendSlotGauges(alloc, &out, "antfly_ha_slot_active", metrics.slots, .active);
    try appendSlotGauges(alloc, &out, "antfly_ha_slot_reseed_required", metrics.slots, .reseed_required);
    try appendSlotGauges(alloc, &out, "antfly_ha_slot_received_lsn", metrics.slots, .received_lsn);
    try appendSlotGauges(alloc, &out, "antfly_ha_slot_applied_lsn", metrics.slots, .applied_lsn);
    try appendSlotGauges(alloc, &out, "antfly_ha_slot_safe_read_lsn", metrics.slots, .safe_read_lsn);
    try appendSlotGauges(alloc, &out, "antfly_ha_slot_restart_lsn", metrics.slots, .restart_lsn);
    try appendSlotGauges(alloc, &out, "antfly_ha_slot_write_lag_lsn", metrics.slots, .write_lag_lsn);
    try appendSlotGauges(alloc, &out, "antfly_ha_slot_apply_lag_lsn", metrics.slots, .apply_lag_lsn);
    try appendSlotGauges(alloc, &out, "antfly_ha_slot_safe_read_lag_lsn", metrics.slots, .safe_read_lag_lsn);
    try appendSlotGauges(alloc, &out, "antfly_ha_slot_retention_lag_lsn", metrics.slots, .retention_lag_lsn);
    try appendSlotGauges(alloc, &out, "antfly_ha_slot_status_code", metrics.slots, .status_code);
    try appendSlotGauges(alloc, &out, "antfly_ha_slot_last_error", metrics.slots, .last_error);

    return try out.toOwnedSlice(alloc);
}

pub fn renderStandbyPrometheusAlloc(alloc: Allocator, metrics: StandbyMetrics) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try appendGauge(alloc, &out, "antfly_ha_standby_received_lsn", metrics.received_lsn);
    try appendGauge(alloc, &out, "antfly_ha_standby_applied_lsn", metrics.applied_lsn);
    try appendGauge(alloc, &out, "antfly_ha_standby_safe_read_lsn", metrics.safe_read_lsn);
    try appendGauge(alloc, &out, "antfly_ha_standby_upstream_configured", metrics.upstream_configured);
    try appendGauge(alloc, &out, "antfly_ha_standby_write_lag_lsn", metrics.write_lag_lsn);
    try appendGauge(alloc, &out, "antfly_ha_standby_receive_lag_lsn", metrics.receive_lag_lsn);
    try appendGauge(alloc, &out, "antfly_ha_standby_apply_lag_lsn", metrics.apply_lag_lsn);
    try appendGauge(alloc, &out, "antfly_ha_standby_unapplied_lsn_count", metrics.unapplied_lsn_count);
    try appendGauge(alloc, &out, "antfly_ha_standby_caught_up_to_received", metrics.caught_up_to_received);
    try appendGauge(alloc, &out, "antfly_ha_standby_can_serve_safe_reads", metrics.can_serve_safe_reads);

    return try out.toOwnedSlice(alloc);
}

pub fn renderPromotionPrometheusAlloc(alloc: Allocator, metrics: PromotionMetrics) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try appendGauge(alloc, &out, "antfly_ha_promotion_required_lsn", metrics.required_lsn);
    try appendGauge(alloc, &out, "antfly_ha_promotion_received_lsn", metrics.received_lsn);
    try appendGauge(alloc, &out, "antfly_ha_promotion_applied_lsn", metrics.applied_lsn);
    try appendGauge(alloc, &out, "antfly_ha_promotion_has_required_lsn", metrics.has_required_lsn);
    try appendGauge(alloc, &out, "antfly_ha_promotion_caught_up_to_received", metrics.caught_up_to_received);
    try appendGauge(alloc, &out, "antfly_ha_promotion_fencing_confirmed", metrics.fencing_confirmed);
    try appendGauge(alloc, &out, "antfly_ha_promotion_force", metrics.force);
    try appendGauge(alloc, &out, "antfly_ha_promotion_data_loss_possible", metrics.data_loss_possible);
    try appendGauge(alloc, &out, "antfly_ha_promotion_safe", metrics.safe);
    try appendGauge(alloc, &out, "antfly_ha_promotion_requires_fencing", metrics.requires_fencing);
    try appendGauge(alloc, &out, "antfly_ha_promotion_requires_force", metrics.requires_force);
    try appendGauge(alloc, &out, "antfly_ha_promotion_can_promote", metrics.can_promote);

    return try out.toOwnedSlice(alloc);
}

pub fn renderRejoinPrometheusAlloc(alloc: Allocator, metrics: RejoinMetrics) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try appendGauge(alloc, &out, "antfly_ha_rejoin_action_code", metrics.action_code);
    try appendGauge(alloc, &out, "antfly_ha_rejoin_reason_code", metrics.reason_code);
    try appendGauge(alloc, &out, "antfly_ha_rejoin_rejected_unfenced", metrics.rejected_unfenced);
    try appendGauge(alloc, &out, "antfly_ha_rejoin_already_current", metrics.already_current);
    try appendGauge(alloc, &out, "antfly_ha_rejoin_can_rewind", metrics.can_rewind);
    try appendGauge(alloc, &out, "antfly_ha_rejoin_requires_reseed", metrics.requires_reseed);
    try appendGauge(alloc, &out, "antfly_ha_rejoin_target_timeline_id", metrics.target_timeline_id);
    try appendGauge(alloc, &out, "antfly_ha_rejoin_target_epoch", metrics.target_epoch);
    try appendGauge(alloc, &out, "antfly_ha_rejoin_parent_cluster_id", metrics.parent_cluster_id);
    try appendGauge(alloc, &out, "antfly_ha_rejoin_parent_shard_id", metrics.parent_shard_id);
    try appendGauge(alloc, &out, "antfly_ha_rejoin_parent_table_id", metrics.parent_table_id);
    try appendGauge(alloc, &out, "antfly_ha_rejoin_parent_timeline_id", metrics.parent_timeline_id);
    try appendGauge(alloc, &out, "antfly_ha_rejoin_parent_epoch", metrics.parent_epoch);
    try appendGauge(alloc, &out, "antfly_ha_rejoin_fork_lsn", metrics.fork_lsn);
    try appendGauge(alloc, &out, "antfly_ha_rejoin_former_last_lsn", metrics.former_last_lsn);
    try appendGauge(alloc, &out, "antfly_ha_rejoin_retained_from_lsn", metrics.retained_from_lsn);
    try appendGauge(alloc, &out, "antfly_ha_rejoin_data_loss_discarded", metrics.data_loss_discarded);

    return try out.toOwnedSlice(alloc);
}

const SlotMetricField = enum {
    active,
    reseed_required,
    received_lsn,
    applied_lsn,
    safe_read_lsn,
    restart_lsn,
    write_lag_lsn,
    apply_lag_lsn,
    safe_read_lag_lsn,
    retention_lag_lsn,
    status_code,
    last_error,
};

fn appendGauge(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), name: []const u8, value: u64) !void {
    try appendMetricHeader(alloc, out, name);
    try appendMetricName(alloc, out, name);
    try out.append(alloc, ' ');
    try appendU64(alloc, out, value);
    try out.append(alloc, '\n');
}

fn appendSlotGauges(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    name: []const u8,
    slots: []const SlotMetrics,
    field: SlotMetricField,
) !void {
    try appendMetricHeader(alloc, out, name);
    for (slots) |slot| {
        try appendMetricName(alloc, out, name);
        try out.appendSlice(alloc, "{slot=\"");
        try appendEscapedLabelValue(alloc, out, slot.name);
        try out.appendSlice(alloc, "\"} ");
        try appendU64(alloc, out, slotMetricValue(slot, field));
        try out.append(alloc, '\n');
    }
}

fn appendMetricHeader(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), name: []const u8) !void {
    try out.appendSlice(alloc, "# TYPE ");
    try out.appendSlice(alloc, name);
    try out.appendSlice(alloc, " gauge\n");
}

fn appendMetricName(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), name: []const u8) !void {
    try out.appendSlice(alloc, name);
}

fn appendU64(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u64) !void {
    var buf: [32]u8 = undefined;
    const text = try std.fmt.bufPrint(&buf, "{d}", .{value});
    try out.appendSlice(alloc, text);
}

fn appendEscapedLabelValue(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    for (value) |byte| {
        switch (byte) {
            '\\' => try out.appendSlice(alloc, "\\\\"),
            '"' => try out.appendSlice(alloc, "\\\""),
            '\n' => try out.appendSlice(alloc, "\\n"),
            else => try out.append(alloc, byte),
        }
    }
}

fn slotMetricValue(slot: SlotMetrics, field: SlotMetricField) u64 {
    return switch (field) {
        .active => slot.active,
        .reseed_required => slot.reseed_required,
        .received_lsn => slot.received_lsn,
        .applied_lsn => slot.applied_lsn,
        .safe_read_lsn => slot.safe_read_lsn,
        .restart_lsn => slot.restart_lsn,
        .write_lag_lsn => slot.write_lag_lsn,
        .apply_lag_lsn => slot.apply_lag_lsn,
        .safe_read_lag_lsn => slot.safe_read_lag_lsn,
        .retention_lag_lsn => slot.retention_lag_lsn,
        .status_code => slot.status_code,
        .last_error => slot.last_error,
    };
}

fn boolGauge(value: bool) u64 {
    return if (value) 1 else 0;
}

test "storage.ha metrics derives primary gauges from status snapshot" {
    const alloc = std.testing.allocator;

    var slot_snapshots = [_]status_mod.SlotSnapshot{
        .{
            .name = "standby-a",
            .timeline_id = 7,
            .active = true,
            .reseed_required = false,
            .restart_lsn = 8,
            .received_lsn = 18,
            .applied_lsn = 17,
            .safe_read_lsn = 16,
            .write_lag_lsn = 2,
            .apply_lag_lsn = 3,
            .safe_read_lag_lsn = 4,
            .retention_lag_lsn = 12,
            .status = .healthy,
        },
        .{
            .name = "standby-b",
            .timeline_id = 7,
            .active = false,
            .reseed_required = true,
            .restart_lsn = 3,
            .received_lsn = 9,
            .applied_lsn = 6,
            .safe_read_lsn = 5,
            .write_lag_lsn = 11,
            .apply_lag_lsn = 14,
            .safe_read_lag_lsn = 15,
            .retention_lag_lsn = 17,
            .status = .reseed_required,
            .last_error = "IntentionalApplyFailure",
        },
    };
    const snapshot = status_mod.PrimarySnapshot{
        .identity = .{
            .cluster_id = 1,
            .shard_id = 2,
            .table_id = 3,
            .timeline_id = 7,
            .epoch = 4,
        },
        .current_lsn = 20,
        .slots = slot_snapshots[0..],
        .retention = .{
            .primary_lsn = 20,
            .oldest_restart_lsn = 3,
            .retained_lsn_count = 17,
            .retained_byte_count = 8192,
            .retained_age_ns = 5000,
            .active_slots = 1,
            .reseed_recommended = 1,
        },
        .durability = .{
            .status = .would_block,
            .mode = .remote_write,
            .selection = .any,
            .target_lsn = 20,
            .progress_lsn = 18,
            .missing_lsn_count = 2,
            .satisfied_count = 0,
            .required_count = 1,
            .candidate_count = 2,
        },
    };

    var metrics = try fromPrimarySnapshot(alloc, snapshot);
    defer metrics.deinit(alloc);

    try std.testing.expectEqual(@as(u64, 20), metrics.current_lsn);
    try std.testing.expectEqual(@as(u64, 2), metrics.slot_count);
    try std.testing.expectEqual(@as(u64, 1), metrics.active_slots);
    try std.testing.expectEqual(@as(u64, 1), metrics.reseed_required_slots);
    try std.testing.expectEqual(@as(u64, 11), metrics.max_write_lag_lsn);
    try std.testing.expectEqual(@as(u64, 14), metrics.max_apply_lag_lsn);
    try std.testing.expectEqual(@as(u64, 15), metrics.max_safe_read_lag_lsn);
    try std.testing.expectEqual(@as(u64, 17), metrics.max_retention_lag_lsn);
    try std.testing.expectEqual(@as(u64, 3), metrics.retention_oldest_restart_lsn);
    try std.testing.expectEqual(@as(u64, 17), metrics.retention_retained_lsn_count);
    try std.testing.expectEqual(@as(u64, 8192), metrics.retention_retained_byte_count);
    try std.testing.expectEqual(@as(u64, 5000), metrics.retention_retained_age_ns);
    try std.testing.expectEqual(@as(u64, 1), metrics.retention_active_slots);
    try std.testing.expectEqual(@as(u64, 1), metrics.retention_reseed_recommended);
    try std.testing.expectEqual(@as(u64, 1), metrics.durability_configured);
    try std.testing.expectEqual(@as(u64, 0), metrics.durability_satisfied);
    try std.testing.expectEqual(@as(u64, 1), metrics.durability_degraded);
    try std.testing.expectEqual(@as(u64, @backingInt(DurabilityStatusCode.would_block)), metrics.durability_status_code);
    try std.testing.expectEqual(@as(u64, 20), metrics.durability_target_lsn);
    try std.testing.expectEqual(@as(u64, 18), metrics.durability_progress_lsn);
    try std.testing.expectEqual(@as(u64, 2), metrics.durability_missing_lsn_count);
    try std.testing.expectEqual(@as(u64, 1), metrics.durability_required_count);
    try std.testing.expectEqual(@as(u64, 0), metrics.durability_satisfied_count);
    try std.testing.expectEqual(@as(u64, 2), metrics.durability_candidate_count);
    try std.testing.expectEqualStrings("standby-b", metrics.slots[1].name);
    try std.testing.expectEqual(@as(u64, 5), metrics.slots[1].safe_read_lsn);
    try std.testing.expectEqual(@as(u64, 15), metrics.slots[1].safe_read_lag_lsn);
    try std.testing.expectEqual(@as(u64, @backingInt(SlotStatusCode.reseed_required)), metrics.slots[1].status_code);
    try std.testing.expectEqual(@as(u64, 1), metrics.slots[1].last_error);
}

test "storage.ha metrics derives standby and promotion gauges" {
    const standby_snapshot = status_mod.StandbySnapshot{
        .identity = .{
            .cluster_id = 1,
            .shard_id = 2,
            .table_id = 3,
            .timeline_id = 4,
            .epoch = 5,
        },
        .received_lsn = 8,
        .applied_lsn = 6,
        .safe_read_lsn = 6,
        .upstream_lsn = 10,
        .write_lag_lsn = 2,
        .receive_lag_lsn = 2,
        .apply_lag_lsn = 4,
        .unapplied_lsn_count = 2,
        .caught_up_to_received = false,
        .can_serve_safe_reads = true,
    };
    const standby_metrics = fromStandbySnapshot(standby_snapshot);
    try std.testing.expectEqual(@as(u64, 8), standby_metrics.received_lsn);
    try std.testing.expectEqual(@as(u64, 1), standby_metrics.upstream_configured);
    try std.testing.expectEqual(@as(u64, 2), standby_metrics.write_lag_lsn);
    try std.testing.expectEqual(@as(u64, 2), standby_metrics.receive_lag_lsn);
    try std.testing.expectEqual(@as(u64, 4), standby_metrics.apply_lag_lsn);
    try std.testing.expectEqual(@as(u64, 0), standby_metrics.caught_up_to_received);
    try std.testing.expectEqual(@as(u64, 1), standby_metrics.can_serve_safe_reads);

    const promotion_metrics = fromPromotionAssessment(.{
        .required_lsn = 10,
        .received_lsn = 8,
        .applied_lsn = 6,
        .has_required_lsn = false,
        .caught_up_to_received = false,
        .fencing_confirmed = true,
        .force = false,
        .mode = .blocked,
        .data_loss_possible = true,
        .safe = false,
        .requires_fencing = false,
        .requires_force = true,
        .can_promote = false,
    });
    try std.testing.expectEqual(@as(u64, 10), promotion_metrics.required_lsn);
    try std.testing.expectEqual(@as(u64, 0), promotion_metrics.has_required_lsn);
    try std.testing.expectEqual(@as(u64, 1), promotion_metrics.fencing_confirmed);
    try std.testing.expectEqual(@as(u64, 1), promotion_metrics.data_loss_possible);
    try std.testing.expectEqual(@as(u64, 1), promotion_metrics.requires_force);
    try std.testing.expectEqual(@as(u64, 0), promotion_metrics.can_promote);
}

test "storage.ha metrics derives rejoin gauges" {
    const assessment = rejoin.Assessment{
        .action = .reseed,
        .reason = .parent_timeline_wal_expired,
        .former_node_id = "primary-a",
        .target_timeline_id = 5,
        .target_epoch = 7,
        .parent_cluster_id = 100,
        .parent_shard_id = 10,
        .parent_table_id = 20,
        .parent_timeline_id = 4,
        .parent_epoch = 6,
        .fork_lsn = 12,
        .former_last_lsn = 13,
        .retained_from_lsn = 14,
        .data_loss_discarded = true,
    };
    const rejoin_metrics = fromRejoinAssessment(assessment);
    try std.testing.expectEqual(@as(u64, @backingInt(RejoinActionCode.reseed)), rejoin_metrics.action_code);
    try std.testing.expectEqual(@as(u64, @backingInt(RejoinReasonCode.parent_timeline_wal_expired)), rejoin_metrics.reason_code);
    try std.testing.expectEqual(@as(u64, 0), rejoin_metrics.rejected_unfenced);
    try std.testing.expectEqual(@as(u64, 0), rejoin_metrics.already_current);
    try std.testing.expectEqual(@as(u64, 0), rejoin_metrics.can_rewind);
    try std.testing.expectEqual(@as(u64, 1), rejoin_metrics.requires_reseed);
    try std.testing.expectEqual(@as(u64, 5), rejoin_metrics.target_timeline_id);
    try std.testing.expectEqual(@as(u64, 7), rejoin_metrics.target_epoch);
    try std.testing.expectEqual(@as(u64, 100), rejoin_metrics.parent_cluster_id);
    try std.testing.expectEqual(@as(u64, 4), rejoin_metrics.parent_timeline_id);
    try std.testing.expectEqual(@as(u64, 12), rejoin_metrics.fork_lsn);
    try std.testing.expectEqual(@as(u64, 13), rejoin_metrics.former_last_lsn);
    try std.testing.expectEqual(@as(u64, 14), rejoin_metrics.retained_from_lsn);
    try std.testing.expectEqual(@as(u64, 1), rejoin_metrics.data_loss_discarded);
}

test "storage.ha metrics renders prometheus text" {
    const alloc = std.testing.allocator;

    var slots = [_]SlotMetrics{
        .{
            .name = "standby\"a\\b\nc",
            .active = 1,
            .reseed_required = 0,
            .received_lsn = 18,
            .applied_lsn = 17,
            .safe_read_lsn = 16,
            .restart_lsn = 8,
            .write_lag_lsn = 2,
            .apply_lag_lsn = 3,
            .safe_read_lag_lsn = 4,
            .retention_lag_lsn = 12,
            .status_code = @backingInt(SlotStatusCode.healthy),
            .last_error = 0,
        },
    };
    const primary = PrimaryMetrics{
        .current_lsn = 20,
        .slot_count = 1,
        .active_slots = 1,
        .reseed_required_slots = 0,
        .max_write_lag_lsn = 2,
        .max_apply_lag_lsn = 3,
        .max_safe_read_lag_lsn = 4,
        .max_retention_lag_lsn = 12,
        .retention_oldest_restart_lsn = 8,
        .retention_retained_lsn_count = 12,
        .retention_retained_byte_count = 4096,
        .retention_retained_age_ns = 3000,
        .retention_active_slots = 1,
        .retention_reseed_recommended = 0,
        .durability_configured = 1,
        .durability_satisfied = 1,
        .durability_degraded = 0,
        .durability_status_code = @backingInt(DurabilityStatusCode.satisfied),
        .durability_target_lsn = 20,
        .durability_progress_lsn = 20,
        .durability_missing_lsn_count = 0,
        .durability_required_count = 1,
        .durability_satisfied_count = 1,
        .durability_candidate_count = 1,
        .slots = slots[0..],
    };

    const primary_text = try renderPrimaryPrometheusAlloc(alloc, primary);
    defer alloc.free(primary_text);
    try expectContains(primary_text, "# TYPE antfly_ha_primary_current_lsn gauge\n");
    try expectContains(primary_text, "antfly_ha_primary_current_lsn 20\n");
    try expectContains(primary_text, "antfly_ha_primary_durability_satisfied 1\n");
    try expectContains(primary_text, "antfly_ha_primary_durability_progress_lsn 20\n");
    try expectContains(primary_text, "antfly_ha_primary_durability_missing_lsn_count 0\n");
    try expectContains(primary_text, "antfly_ha_primary_retention_retained_age_ns 3000\n");
    try expectContains(primary_text, "# TYPE antfly_ha_slot_apply_lag_lsn gauge\n");
    try expectContains(primary_text, "antfly_ha_slot_apply_lag_lsn{slot=\"standby\\\"a\\\\b\\nc\"} 3\n");
    try expectContains(primary_text, "antfly_ha_slot_safe_read_lag_lsn{slot=\"standby\\\"a\\\\b\\nc\"} 4\n");
    try expectContains(primary_text, "antfly_ha_slot_last_error{slot=\"standby\\\"a\\\\b\\nc\"} 0\n");

    const standby_text = try renderStandbyPrometheusAlloc(alloc, .{
        .received_lsn = 12,
        .applied_lsn = 10,
        .safe_read_lsn = 10,
        .upstream_configured = 1,
        .write_lag_lsn = 4,
        .receive_lag_lsn = 4,
        .apply_lag_lsn = 2,
        .unapplied_lsn_count = 2,
        .caught_up_to_received = 0,
        .can_serve_safe_reads = 1,
    });
    defer alloc.free(standby_text);
    try expectContains(standby_text, "antfly_ha_standby_write_lag_lsn 4\n");
    try expectContains(standby_text, "antfly_ha_standby_can_serve_safe_reads 1\n");

    const promotion_text = try renderPromotionPrometheusAlloc(alloc, .{
        .required_lsn = 14,
        .received_lsn = 12,
        .applied_lsn = 10,
        .has_required_lsn = 0,
        .caught_up_to_received = 0,
        .fencing_confirmed = 1,
        .force = 0,
        .data_loss_possible = 1,
        .safe = 0,
        .requires_fencing = 0,
        .requires_force = 1,
        .can_promote = 0,
    });
    defer alloc.free(promotion_text);
    try expectContains(promotion_text, "antfly_ha_promotion_requires_force 1\n");
    try expectContains(promotion_text, "antfly_ha_promotion_can_promote 0\n");

    const rejoin_text = try renderRejoinPrometheusAlloc(alloc, .{
        .action_code = @backingInt(RejoinActionCode.rewind),
        .reason_code = @backingInt(RejoinReasonCode.parent_timeline_retained),
        .rejected_unfenced = 0,
        .already_current = 0,
        .can_rewind = 1,
        .requires_reseed = 0,
        .target_timeline_id = 5,
        .target_epoch = 7,
        .parent_cluster_id = 100,
        .parent_shard_id = 10,
        .parent_table_id = 20,
        .parent_timeline_id = 4,
        .parent_epoch = 6,
        .fork_lsn = 12,
        .former_last_lsn = 13,
        .retained_from_lsn = 8,
        .data_loss_discarded = 1,
    });
    defer alloc.free(rejoin_text);
    try expectContains(rejoin_text, "antfly_ha_rejoin_action_code 2\n");
    try expectContains(rejoin_text, "antfly_ha_rejoin_reason_code 2\n");
    try expectContains(rejoin_text, "antfly_ha_rejoin_can_rewind 1\n");
    try expectContains(rejoin_text, "antfly_ha_rejoin_requires_reseed 0\n");
    try expectContains(rejoin_text, "antfly_ha_rejoin_data_loss_discarded 1\n");
}

fn expectContains(haystack: []const u8, needle: []const u8) !void {
    try std.testing.expect(std.mem.indexOf(u8, haystack, needle) != null);
}
