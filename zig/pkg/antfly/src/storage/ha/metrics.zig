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

pub const SlotMetrics = struct {
    name: []const u8,
    active: u64,
    reseed_required: u64,
    received_lsn: u64,
    applied_lsn: u64,
    restart_lsn: u64,
    write_lag_lsn: u64,
    apply_lag_lsn: u64,
    retention_lag_lsn: u64,
    status_code: u64,
};

pub const PrimaryMetrics = struct {
    current_lsn: u64,
    slot_count: u64,
    active_slots: u64,
    reseed_required_slots: u64,
    max_write_lag_lsn: u64,
    max_apply_lag_lsn: u64,
    max_retention_lag_lsn: u64,
    retention_oldest_restart_lsn: u64,
    retention_retained_lsn_count: u64,
    retention_active_slots: u64,
    retention_reseed_recommended: u64,
    durability_configured: u64,
    durability_satisfied: u64,
    durability_degraded: u64,
    durability_status_code: u64,
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

pub fn fromPrimarySnapshot(alloc: Allocator, snapshot: status_mod.PrimarySnapshot) !PrimaryMetrics {
    const slots = try alloc.alloc(SlotMetrics, snapshot.slots.len);
    errdefer alloc.free(slots);

    var filled: usize = 0;
    errdefer for (slots[0..filled]) |slot| alloc.free(slot.name);

    var active_slots: u64 = 0;
    var reseed_required_slots: u64 = 0;
    var max_write_lag_lsn: u64 = 0;
    var max_apply_lag_lsn: u64 = 0;
    var max_retention_lag_lsn: u64 = 0;

    for (snapshot.slots, 0..) |slot, idx| {
        if (slot.active) active_slots += 1;
        if (slot.reseed_required) reseed_required_slots += 1;
        max_write_lag_lsn = @max(max_write_lag_lsn, slot.write_lag_lsn);
        max_apply_lag_lsn = @max(max_apply_lag_lsn, slot.apply_lag_lsn);
        max_retention_lag_lsn = @max(max_retention_lag_lsn, slot.retention_lag_lsn);

        slots[idx] = .{
            .name = try alloc.dupe(u8, slot.name),
            .active = boolGauge(slot.active),
            .reseed_required = boolGauge(slot.reseed_required),
            .received_lsn = slot.received_lsn,
            .applied_lsn = slot.applied_lsn,
            .restart_lsn = slot.restart_lsn,
            .write_lag_lsn = slot.write_lag_lsn,
            .apply_lag_lsn = slot.apply_lag_lsn,
            .retention_lag_lsn = slot.retention_lag_lsn,
            .status_code = @intFromEnum(slotStatusCode(slot.status)),
        };
        filled += 1;
    }

    const durability = snapshot.durability;
    const durability_status_code = if (durability) |decision|
        @intFromEnum(durabilityStatusCode(decision.status))
    else
        @intFromEnum(DurabilityStatusCode.not_configured);
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
        .max_retention_lag_lsn = max_retention_lag_lsn,
        .retention_oldest_restart_lsn = snapshot.retention.oldest_restart_lsn,
        .retention_retained_lsn_count = snapshot.retention.retained_lsn_count,
        .retention_active_slots = @intCast(snapshot.retention.active_slots),
        .retention_reseed_recommended = @intCast(snapshot.retention.reseed_recommended),
        .durability_configured = boolGauge(durability != null),
        .durability_satisfied = durability_satisfied,
        .durability_degraded = durability_degraded,
        .durability_status_code = durability_status_code,
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
            .write_lag_lsn = 2,
            .apply_lag_lsn = 3,
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
            .write_lag_lsn = 11,
            .apply_lag_lsn = 14,
            .retention_lag_lsn = 17,
            .status = .reseed_required,
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
            .active_slots = 1,
            .reseed_recommended = 1,
        },
        .durability = .{
            .status = .would_block,
            .mode = .remote_write,
            .selection = .any,
            .target_lsn = 20,
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
    try std.testing.expectEqual(@as(u64, 17), metrics.max_retention_lag_lsn);
    try std.testing.expectEqual(@as(u64, 3), metrics.retention_oldest_restart_lsn);
    try std.testing.expectEqual(@as(u64, 17), metrics.retention_retained_lsn_count);
    try std.testing.expectEqual(@as(u64, 1), metrics.retention_active_slots);
    try std.testing.expectEqual(@as(u64, 1), metrics.retention_reseed_recommended);
    try std.testing.expectEqual(@as(u64, 1), metrics.durability_configured);
    try std.testing.expectEqual(@as(u64, 0), metrics.durability_satisfied);
    try std.testing.expectEqual(@as(u64, 1), metrics.durability_degraded);
    try std.testing.expectEqual(@as(u64, @intFromEnum(DurabilityStatusCode.would_block)), metrics.durability_status_code);
    try std.testing.expectEqual(@as(u64, 1), metrics.durability_required_count);
    try std.testing.expectEqual(@as(u64, 0), metrics.durability_satisfied_count);
    try std.testing.expectEqual(@as(u64, 2), metrics.durability_candidate_count);
    try std.testing.expectEqualStrings("standby-b", metrics.slots[1].name);
    try std.testing.expectEqual(@as(u64, @intFromEnum(SlotStatusCode.reseed_required)), metrics.slots[1].status_code);
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
        .receive_lag_lsn = 2,
        .apply_lag_lsn = 4,
        .unapplied_lsn_count = 2,
        .caught_up_to_received = false,
        .can_serve_safe_reads = true,
    };
    const standby_metrics = fromStandbySnapshot(standby_snapshot);
    try std.testing.expectEqual(@as(u64, 8), standby_metrics.received_lsn);
    try std.testing.expectEqual(@as(u64, 1), standby_metrics.upstream_configured);
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
