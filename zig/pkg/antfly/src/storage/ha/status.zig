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

//! HA status snapshots for admin APIs, CLI commands, and operators.
//!
//! These structs intentionally contain plain JSON-serializable values and owned
//! strings so callers can expose primary slot health, standby lag, sync policy
//! state, retention pressure, and promotion readiness without reaching into the
//! storage internals directly.

const std = @import("std");
const Allocator = std.mem.Allocator;
const fencing = @import("fencing.zig");
const primary_mod = @import("primary.zig");
const replication_record = @import("replication_record.zig");
const slot_store = @import("slot_store.zig");
const standby_mod = @import("standby.zig");

var test_path_counter: u64 = 0;

pub const Role = enum {
    primary,
    standby,
};

pub const SlotSnapshot = struct {
    name: []const u8,
    timeline_id: u64,
    active: bool,
    reseed_required: bool,
    restart_lsn: u64,
    received_lsn: u64,
    applied_lsn: u64,
    safe_read_lsn: u64,
    write_lag_lsn: u64,
    apply_lag_lsn: u64,
    safe_read_lag_lsn: u64,
    retention_lag_lsn: u64,
    status: slot_store.SlotStatus,
    last_error: ?[]const u8 = null,
};

pub const PrimarySnapshot = struct {
    role: Role = .primary,
    identity: primary_mod.Identity,
    current_lsn: u64,
    slots: []SlotSnapshot,
    retention: slot_store.RetentionSnapshot,
    durability: ?primary_mod.DurabilityDecision = null,

    pub fn deinit(self: *PrimarySnapshot, alloc: Allocator) void {
        for (self.slots) |slot| {
            alloc.free(slot.name);
            if (slot.last_error) |last_error| alloc.free(last_error);
        }
        alloc.free(self.slots);
        self.* = undefined;
    }
};

pub const StandbySnapshot = struct {
    role: Role = .standby,
    identity: standby_mod.Identity,
    received_lsn: u64,
    applied_lsn: u64,
    safe_read_lsn: u64,
    upstream_lsn: ?u64,
    write_lag_lsn: ?u64,
    receive_lag_lsn: ?u64,
    apply_lag_lsn: ?u64,
    last_error: ?[]const u8 = null,
    last_attempt_ns: ?u64 = null,
    last_success_ns: ?u64 = null,
    replication_failures_total: ?u64 = null,
    unapplied_lsn_count: u64,
    caught_up_to_received: bool,
    can_serve_safe_reads: bool,
};

pub const PromotionCheck = struct {
    required_lsn: ?u64 = null,
    fencing_confirmed: bool = false,
    force: bool = false,
};

pub const PromotionMode = enum {
    blocked,
    safe,
    forced,
    lossy,
};

pub const PromotionAssessment = struct {
    required_lsn: u64,
    received_lsn: u64,
    applied_lsn: u64,
    has_required_lsn: bool,
    caught_up_to_received: bool,
    fencing_confirmed: bool,
    force: bool,
    mode: PromotionMode,
    data_loss_possible: bool,
    safe: bool,
    requires_fencing: bool,
    requires_force: bool,
    can_promote: bool,
};

pub const PrimaryStatusDocument = struct {
    schema_version: u32 = 1,
    snapshot: PrimarySnapshot,
};

pub const StandbyStatusDocument = struct {
    schema_version: u32 = 1,
    snapshot: StandbySnapshot,
};

pub const PromotionStatusDocument = struct {
    schema_version: u32 = 1,
    assessment: PromotionAssessment,
};

pub fn primaryStatusDocument(snapshot: PrimarySnapshot) PrimaryStatusDocument {
    return .{ .snapshot = snapshot };
}

pub fn standbyStatusDocument(snapshot: StandbySnapshot) StandbyStatusDocument {
    return .{ .snapshot = snapshot };
}

pub fn promotionStatusDocument(assessment: PromotionAssessment) PromotionStatusDocument {
    return .{ .assessment = assessment };
}

pub fn renderPrimaryJsonAlloc(alloc: Allocator, snapshot: PrimarySnapshot) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, primaryStatusDocument(snapshot), .{});
}

pub fn renderStandbyJsonAlloc(alloc: Allocator, snapshot: StandbySnapshot) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, standbyStatusDocument(snapshot), .{});
}

pub fn renderPromotionJsonAlloc(alloc: Allocator, assessment: PromotionAssessment) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, promotionStatusDocument(assessment), .{});
}

pub fn primarySnapshot(
    alloc: Allocator,
    primary: *primary_mod.Primary,
    retention_policy: slot_store.RetentionPolicy,
    sync_policy: ?primary_mod.SyncPolicy,
) !PrimarySnapshot {
    const current_lsn = primary.lastLsn();
    const retention = try primary.retentionSnapshot(retention_policy);
    const slot_states = try primary.slots.listAlloc(alloc);
    defer slot_store.freeSlotList(alloc, slot_states);

    const slots = try alloc.alloc(SlotSnapshot, slot_states.len);
    errdefer alloc.free(slots);

    var filled: usize = 0;
    errdefer for (slots[0..filled]) |slot| {
        alloc.free(slot.name);
        if (slot.last_error) |last_error| alloc.free(last_error);
    };
    for (slot_states, 0..) |slot, idx| {
        const owned_name = try alloc.dupe(u8, slot.name);
        errdefer alloc.free(owned_name);
        const owned_last_error = if (slot.last_error) |last_error|
            try alloc.dupe(u8, last_error)
        else
            null;
        errdefer if (owned_last_error) |last_error| alloc.free(last_error);
        slots[idx] = .{
            .name = owned_name,
            .timeline_id = slot.timeline_id,
            .active = slot.active,
            .reseed_required = slot.reseed_required,
            .restart_lsn = slot.restart_lsn,
            .received_lsn = slot.received_lsn,
            .applied_lsn = slot.applied_lsn,
            .safe_read_lsn = slot.safe_read_lsn,
            .write_lag_lsn = current_lsn -| slot.received_lsn,
            .apply_lag_lsn = current_lsn -| slot.applied_lsn,
            .safe_read_lag_lsn = current_lsn -| slot.safe_read_lsn,
            .retention_lag_lsn = current_lsn -| slot.restart_lsn,
            .status = slot.status(current_lsn, retention_policy.max_lag_lsn),
            .last_error = owned_last_error,
        };
        filled += 1;
    }

    const durability = if (sync_policy) |policy|
        try primary.evaluateDurability(current_lsn, policy)
    else
        null;

    return .{
        .identity = primary.identity,
        .current_lsn = current_lsn,
        .slots = slots,
        .retention = retention,
        .durability = durability,
    };
}

pub fn standbySnapshot(standby: *const standby_mod.Standby, upstream_lsn: ?u64) StandbySnapshot {
    const snapshot = standby.snapshot();
    const progress = snapshot.progress;
    return .{
        .identity = snapshot.identity,
        .received_lsn = progress.received_lsn,
        .applied_lsn = progress.applied_lsn,
        .safe_read_lsn = progress.safe_read_lsn,
        .upstream_lsn = upstream_lsn,
        .write_lag_lsn = if (upstream_lsn) |lsn| lsn -| progress.received_lsn else null,
        .receive_lag_lsn = if (upstream_lsn) |lsn| lsn -| progress.received_lsn else null,
        .apply_lag_lsn = if (upstream_lsn) |lsn| lsn -| progress.applied_lsn else null,
        .unapplied_lsn_count = progress.received_lsn -| progress.applied_lsn,
        .caught_up_to_received = progress.applied_lsn >= progress.received_lsn,
        .can_serve_safe_reads = progress.safe_read_lsn <= progress.applied_lsn,
    };
}

pub fn assessPromotion(standby: *const standby_mod.Standby, check: PromotionCheck) PromotionAssessment {
    const progress = standby.currentProgress();
    const required_lsn = check.required_lsn orelse progress.received_lsn;
    const has_required_lsn = progress.received_lsn >= required_lsn;
    const caught_up_to_received = progress.applied_lsn >= progress.received_lsn;
    const data_loss_possible = !has_required_lsn or !caught_up_to_received or progress.applied_lsn < required_lsn;
    const requires_fencing = !check.fencing_confirmed and !check.force;
    const requires_force = data_loss_possible and !check.force;
    const safe = check.fencing_confirmed and !data_loss_possible;
    const can_promote = !requires_fencing and (!requires_force or check.force);
    return .{
        .required_lsn = required_lsn,
        .received_lsn = progress.received_lsn,
        .applied_lsn = progress.applied_lsn,
        .has_required_lsn = has_required_lsn,
        .caught_up_to_received = caught_up_to_received,
        .fencing_confirmed = check.fencing_confirmed,
        .force = check.force,
        .mode = promotionMode(.{
            .force = check.force,
            .data_loss_possible = data_loss_possible,
            .can_promote = can_promote,
        }),
        .data_loss_possible = data_loss_possible,
        .safe = safe,
        .requires_fencing = requires_fencing,
        .requires_force = requires_force,
        .can_promote = can_promote,
    };
}

pub fn promotionMode(input: struct {
    force: bool,
    data_loss_possible: bool,
    can_promote: bool,
}) PromotionMode {
    if (!input.can_promote) return .blocked;
    if (input.data_loss_possible) return .lossy;
    if (input.force) return .forced;
    return .safe;
}

pub fn assessPromotionWithFence(standby: *const standby_mod.Standby, receipt: fencing.Receipt) PromotionAssessment {
    return assessPromotion(standby, .{
        .required_lsn = receipt.required_lsn,
        .fencing_confirmed = true,
        .force = receipt.forced,
    });
}

const TestPaths = struct {
    primary_log: [:0]u8,
    primary_slots: [:0]u8,
    standby_log: [:0]u8,
    standby_progress: [:0]u8,

    fn deinit(self: TestPaths, alloc: Allocator) void {
        alloc.free(self.primary_log);
        alloc.free(self.primary_slots);
        alloc.free(self.standby_log);
        alloc.free(self.standby_progress);
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

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_slots) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_progress) catch {};

    return .{
        .primary_log = try alloc.dupeZ(u8, primary_log),
        .primary_slots = try alloc.dupeZ(u8, primary_slots),
        .standby_log = try alloc.dupeZ(u8, standby_log),
        .standby_progress = try alloc.dupeZ(u8, standby_progress),
    };
}

fn allocPrintPath(alloc: Allocator, comptime name: []const u8, comptime part: []const u8, nonce: u64) ![]u8 {
    return try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/ha-status-" ++ name ++ "-" ++ part ++ "-{d}-{d}",
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

const ApplyCapture = struct {
    fail_at_lsn: u64 = 0,

    fn apply(ctx: *anyopaque, record: replication_record.RecordView) !void {
        const self: *ApplyCapture = @ptrCast(@alignCast(ctx));
        if (record.lsn == self.fail_at_lsn) return error.IntentionalApplyFailure;
    }
};

test "storage.ha status snapshots primary slot lag retention and sync policy" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "primary");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    try primary.createSlot("a", 0);
    try primary.createSlot("b", 0);
    _ = try primary.append(.{ .payload = "one" });
    _ = try primary.append(.{ .payload = "two" });
    _ = try primary.append(.{ .payload = "three" });
    try primary.standbyStatusUpdateWithSafeRead("a", identity.timeline_id, 3, 2, 1);
    try primary.standbyStatusUpdate("b", identity.timeline_id, 1, 1);
    try primary.reportReplicationError("b", "IntentionalApplyFailure");

    const names = [_][]const u8{ "a", "b" };
    var snapshot = try primarySnapshot(alloc, &primary, .{ .max_lag_lsn = 1 }, .{
        .mode = .remote_apply,
        .selection = .any,
        .required = 1,
        .standby_names = &names,
    });
    defer snapshot.deinit(alloc);

    try std.testing.expectEqual(Role.primary, snapshot.role);
    try std.testing.expectEqual(@as(u64, 3), snapshot.current_lsn);
    try std.testing.expectEqual(@as(usize, 2), snapshot.slots.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.retention.active_slots);
    try std.testing.expectEqual(@as(usize, 1), snapshot.retention.reseed_recommended);
    try std.testing.expectEqual(primary_mod.DurabilityStatus.would_block, snapshot.durability.?.status);
    try std.testing.expectEqual(@as(usize, 1), snapshot.durability.?.candidate_count);

    const slot_a = if (std.mem.eql(u8, snapshot.slots[0].name, "a")) snapshot.slots[0] else snapshot.slots[1];
    try std.testing.expectEqual(@as(u64, 2), slot_a.applied_lsn);
    try std.testing.expectEqual(@as(u64, 1), slot_a.safe_read_lsn);
    try std.testing.expectEqual(@as(u64, 2), slot_a.safe_read_lag_lsn);

    const slot_b = if (std.mem.eql(u8, snapshot.slots[0].name, "b")) snapshot.slots[0] else snapshot.slots[1];
    try std.testing.expectEqual(slot_store.SlotStatus.reseed_required, slot_b.status);
    try std.testing.expect(slot_b.reseed_required);
    try std.testing.expectEqual(@as(u64, 2), slot_b.write_lag_lsn);
    try std.testing.expectEqual(@as(u64, 2), slot_b.apply_lag_lsn);
    try std.testing.expectEqualStrings("IntentionalApplyFailure", slot_b.last_error.?);

    const encoded = try renderPrimaryJsonAlloc(alloc, snapshot);
    defer alloc.free(encoded);
    try expectContains(encoded, "\"schema_version\":1");
    try expectContains(encoded, "\"snapshot\"");
    try expectContains(encoded, "\"role\":\"primary\"");
    try expectContains(encoded, "\"current_lsn\":3");
    try expectContains(encoded, "\"reseed_required\"");
    try expectContains(encoded, "\"last_error\":\"IntentionalApplyFailure\"");
}

test "storage.ha status snapshots standby lag and promotion readiness" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "standby");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();
    _ = try standby.receive(baseRecord(identity, 1, "one"));
    _ = try standby.receive(baseRecord(identity, 2, "two"));

    {
        var capture = ApplyCapture{ .fail_at_lsn = 2 };
        try std.testing.expectError(error.IntentionalApplyFailure, standby.applyAvailable(&capture, ApplyCapture.apply));
    }

    const lagging = standbySnapshot(&standby, 3);
    try std.testing.expectEqual(Role.standby, lagging.role);
    try std.testing.expectEqual(@as(u64, 2), lagging.received_lsn);
    try std.testing.expectEqual(@as(u64, 1), lagging.applied_lsn);
    try std.testing.expectEqual(@as(u64, 1), lagging.write_lag_lsn.?);
    try std.testing.expectEqual(@as(u64, 1), lagging.receive_lag_lsn.?);
    try std.testing.expectEqual(@as(u64, 2), lagging.apply_lag_lsn.?);
    try std.testing.expectEqual(@as(u64, 1), lagging.unapplied_lsn_count);
    try std.testing.expect(!lagging.caught_up_to_received);

    const unsafe = assessPromotion(&standby, .{ .required_lsn = 2, .fencing_confirmed = true });
    try std.testing.expect(unsafe.data_loss_possible);
    try std.testing.expect(unsafe.requires_force);
    try std.testing.expect(!unsafe.safe);
    try std.testing.expect(!unsafe.can_promote);
    try std.testing.expectEqual(PromotionMode.blocked, unsafe.mode);

    {
        var capture = ApplyCapture{};
        try std.testing.expectEqual(@as(usize, 1), try standby.applyAvailable(&capture, ApplyCapture.apply));
    }

    const ready = standbySnapshot(&standby, 2);
    try std.testing.expectEqual(@as(u64, 0), ready.write_lag_lsn.?);
    try std.testing.expectEqual(@as(u64, 0), ready.receive_lag_lsn.?);
    try std.testing.expectEqual(@as(u64, 0), ready.apply_lag_lsn.?);
    try std.testing.expect(ready.caught_up_to_received);

    const safe = assessPromotion(&standby, .{ .required_lsn = 2, .fencing_confirmed = true });
    try std.testing.expect(!safe.data_loss_possible);
    try std.testing.expect(safe.safe);
    try std.testing.expect(safe.can_promote);
    try std.testing.expectEqual(PromotionMode.safe, safe.mode);

    const forced = assessPromotion(&standby, .{ .required_lsn = 2, .fencing_confirmed = true, .force = true });
    try std.testing.expect(!forced.data_loss_possible);
    try std.testing.expect(forced.can_promote);
    try std.testing.expectEqual(PromotionMode.forced, forced.mode);

    const fenced = assessPromotionWithFence(&standby, .{
        .identity = .{
            .cluster_id = identity.cluster_id,
            .shard_id = identity.shard_id,
            .table_id = identity.table_id,
            .timeline_id = 2,
            .epoch = 2,
        },
        .old_primary_id = "primary-a",
        .promoted_node_id = "standby-b",
        .parent_timeline_id = identity.timeline_id,
        .parent_epoch = identity.epoch,
        .new_timeline_id = 2,
        .new_epoch = 2,
        .required_lsn = 2,
        .observed_lsn = 2,
        .generation = 1,
        .forced = false,
        .token = "token",
        .reason = "test",
    });
    try std.testing.expect(fenced.fencing_confirmed);
    try std.testing.expect(fenced.safe);
    try std.testing.expect(fenced.can_promote);
    try std.testing.expectEqual(PromotionMode.safe, fenced.mode);

    const standby_json = try renderStandbyJsonAlloc(alloc, ready);
    defer alloc.free(standby_json);
    try expectContains(standby_json, "\"schema_version\":1");
    try expectContains(standby_json, "\"snapshot\"");
    try expectContains(standby_json, "\"role\":\"standby\"");
    try expectContains(standby_json, "\"applied_lsn\":2");

    const promotion_json = try renderPromotionJsonAlloc(alloc, fenced);
    defer alloc.free(promotion_json);
    try expectContains(promotion_json, "\"schema_version\":1");
    try expectContains(promotion_json, "\"assessment\"");
    try expectContains(promotion_json, "\"mode\":\"safe\"");
    try expectContains(promotion_json, "\"safe\":true");
    try expectContains(promotion_json, "\"can_promote\":true");
}

fn expectContains(haystack: []const u8, needle: []const u8) !void {
    try std.testing.expect(std.mem.indexOf(u8, haystack, needle) != null);
}
