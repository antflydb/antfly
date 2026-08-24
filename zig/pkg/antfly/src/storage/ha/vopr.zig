// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Replayable HA lifecycle scenario over the real primary, standby, slot,
//! replication-log, progress-WAL, fencing, retention, promotion, and rejoin
//! implementations.

const std = @import("std");
const builtin = @import("builtin");
const vopr = @import("vopr");
const primary_mod = @import("primary.zig");
const standby_mod = @import("standby.zig");
const replication_log = @import("replication_log.zig");
const replication_record = @import("replication_record.zig");
const fencing = @import("fencing.zig");
const rejoin = @import("rejoin.zig");

const stream_slot = "standby-a";
const seed_slot = "seed-b";

var path_nonce: std.atomic.Value(u64) = .init(0);

const append_id = vopr.id.stable("transition", "storage.ha.primary.append");
const receive_id = vopr.id.stable("transition", "storage.ha.standby.receive_next");
const apply_id = vopr.id.stable("transition", "storage.ha.standby.apply_available");
const report_id = vopr.id.stable("transition", "storage.ha.primary.report_progress");
const restart_primary_id = vopr.id.stable("transition", "storage.ha.primary.restart");
const restart_standby_id = vopr.id.stable("transition", "storage.ha.standby.restart");
const durability_id = vopr.id.stable("transition", "storage.ha.check_remote_apply_durability");
const retention_id = vopr.id.stable("transition", "storage.ha.evaluate_retention");
const backup_id = vopr.id.stable("transition", "storage.ha.begin_base_backup");
const unfenced_promotion_id = vopr.id.stable("transition", "storage.ha.reject_unfenced_promotion");
const acquire_fence_id = vopr.id.stable("transition", "storage.ha.acquire_promotion_fence");
const promote_id = vopr.id.stable("transition", "storage.ha.promote_standby");
const rejoin_id = vopr.id.stable("transition", "storage.ha.assess_former_primary");
const finish_id = vopr.id.stable("transition", "storage.ha.finish");

const progress_ordered_id = vopr.id.stable("property", "storage.ha.progress_is_ordered");
const applied_prefix_id = vopr.id.stable("property", "storage.ha.applied_payloads_are_primary_prefix");
const durability_sound_id = vopr.id.stable("property", "storage.ha.remote_apply_ack_is_sound");
const backup_pinned_id = vopr.id.stable("property", "storage.ha.base_backup_pin_survives_restart");
const fenced_promotion_id = vopr.id.stable("property", "storage.ha.promotion_requires_durable_fence");
const timeline_id = vopr.id.stable("property", "storage.ha.timeline_and_epoch_are_monotonic");
const rejoin_safe_id = vopr.id.stable("property", "storage.ha.former_primary_never_rejoins_unsafely");
const complete_id = vopr.id.stable("property", "storage.ha.campaign_completed");

const Paths = struct {
    primary_log: [:0]u8,
    primary_slots: [:0]u8,
    standby_log: [:0]u8,
    standby_progress: [:0]u8,
    fence_wal: [:0]u8,

    fn init(allocator: std.mem.Allocator) !Paths {
        const nonce = path_nonce.fetchAdd(1, .monotonic);
        const primary_log = try path(allocator, nonce, "primary-log");
        errdefer allocator.free(primary_log);
        const primary_slots = try path(allocator, nonce, "primary-slots");
        errdefer allocator.free(primary_slots);
        const standby_log = try path(allocator, nonce, "standby-log");
        errdefer allocator.free(standby_log);
        const standby_progress = try path(allocator, nonce, "standby-progress");
        errdefer allocator.free(standby_progress);
        const fence_wal = try path(allocator, nonce, "fence-wal");
        return .{ .primary_log = primary_log, .primary_slots = primary_slots, .standby_log = standby_log, .standby_progress = standby_progress, .fence_wal = fence_wal };
    }

    fn deinit(self: Paths, allocator: std.mem.Allocator) void {
        var io_impl = std.Io.Threaded.init(allocator, .{}); // vopr-audit: allow(native_thread_or_io) real HA storage remains an explicit native differential backend
        defer io_impl.deinit();
        inline for (.{ self.primary_log, self.primary_slots, self.standby_log, self.standby_progress, self.fence_wal }) |item| {
            std.Io.Dir.cwd().deleteTree(io_impl.io(), item) catch {};
            allocator.free(item);
        }
    }
};

const ApplyModel = struct {
    allocator: std.mem.Allocator,
    values: std.ArrayListUnmanaged(u64) = .empty,

    fn deinit(self: *ApplyModel) void {
        self.values.deinit(self.allocator);
    }

    fn apply(ctx: *anyopaque, entry: replication_record.RecordView) !void {
        if (entry.kind != .batch_mutation or entry.payload.len != @sizeOf(u64)) return;
        const self: *ApplyModel = @ptrCast(@alignCast(ctx));
        try self.values.append(self.allocator, std.mem.readInt(u64, entry.payload[0..8], .little));
    }
};

pub fn Scenario(comptime action_budget: u64) type {
    return struct {
        pub const name: []const u8 = "ha-lifecycle";
        pub const version: u32 = 1;
        pub const properties = &[_]vopr.property.Declaration{
            .{ .id = progress_ordered_id, .name = "storage.ha.progress_is_ordered", .kind = .always },
            .{ .id = applied_prefix_id, .name = "storage.ha.applied_payloads_are_primary_prefix", .kind = .always },
            .{ .id = durability_sound_id, .name = "storage.ha.remote_apply_ack_is_sound", .kind = .always },
            .{ .id = backup_pinned_id, .name = "storage.ha.base_backup_pin_survives_restart", .kind = .always },
            .{ .id = fenced_promotion_id, .name = "storage.ha.promotion_requires_durable_fence", .kind = .always },
            .{ .id = timeline_id, .name = "storage.ha.timeline_and_epoch_are_monotonic", .kind = .always },
            .{ .id = rejoin_safe_id, .name = "storage.ha.former_primary_never_rejoins_unsafely", .kind = .always },
            .{ .id = complete_id, .name = "storage.ha.campaign_completed", .kind = .reachable },
        };

        const State = struct {
            allocator: std.mem.Allocator,
            paths: Paths,
            original_identity: standby_mod.Identity,
            primary: primary_mod.Primary,
            standby: standby_mod.Standby,
            fence_store: fencing.Store,
            faults: vopr.fault.Controller,
            applied: ApplyModel,
            appended: std.ArrayListUnmanaged(u64) = .empty,
            receipt: ?fencing.Receipt = null,
            actions: u64 = 0,
            primary_restarts: u64 = 0,
            standby_restarts: u64 = 0,
            backup_started: bool = false,
            backup_pin_valid: bool = true,
            retention_evaluations: u64 = 0,
            reseed_required: bool = false,
            unfenced_rejections: u64 = 0,
            promoted: bool = false,
            promotion_fenced: bool = true,
            timeline_monotonic: bool = true,
            rejoin_safe: bool = true,
            durability_sound: bool = true,
            finished: bool = false,

            fn deinit(self: *State) void {
                if (self.receipt) |receipt| fencing.freeReceipt(self.allocator, receipt);
                self.appended.deinit(self.allocator);
                self.applied.deinit();
                self.faults.deinit();
                self.fence_store.close();
                self.standby.close();
                self.primary.close();
                self.paths.deinit(self.allocator);
            }
        };

        pub const World = struct { state: *State };

        pub fn init(allocator: std.mem.Allocator) !World {
            const state = try allocator.create(State);
            errdefer allocator.destroy(state);
            state.* = undefined;
            state.allocator = allocator;
            state.paths = try Paths.init(allocator);
            errdefer state.paths.deinit(allocator);
            state.original_identity = .{
                .cluster_id = 100,
                .shard_id = 10,
                .table_id = 20,
                .timeline_id = 1,
                .epoch = 1,
            };
            state.primary = try primary_mod.Primary.open(allocator, state.paths.primary_log.ptr, state.paths.primary_slots.ptr, state.original_identity, .{});
            errdefer state.primary.close();
            try state.primary.createSlot(stream_slot, 0);
            state.standby = try standby_mod.Standby.open(allocator, state.paths.standby_log.ptr, state.paths.standby_progress.ptr, state.original_identity, .{});
            errdefer state.standby.close();
            state.fence_store = try fencing.Store.open(allocator, state.paths.fence_wal.ptr, .{});
            errdefer state.fence_store.close();
            state.faults = try vopr.fault.Controller.init(allocator, 2, .{
                .max_simultaneous_node_failures = 1,
                .max_partitioned_links = 1,
                .minimum_healthy_nodes = 1,
            });
            state.applied = .{ .allocator = allocator };
            state.appended = .empty;
            state.receipt = null;
            state.actions = 0;
            state.primary_restarts = 0;
            state.standby_restarts = 0;
            state.backup_started = false;
            state.backup_pin_valid = true;
            state.retention_evaluations = 0;
            state.reseed_required = false;
            state.unfenced_rejections = 0;
            state.promoted = false;
            state.promotion_fenced = true;
            state.timeline_monotonic = true;
            state.rejoin_safe = true;
            state.durability_sound = true;
            state.finished = false;
            return .{ .state = state };
        }

        pub fn deinit(world: *World, allocator: std.mem.Allocator) void {
            world.state.deinit();
            allocator.destroy(world.state);
            world.* = undefined;
        }

        pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: std.mem.Allocator) !void {
            const state = world.state;
            if (state.actions >= action_budget) {
                try list.append(allocator, .{ .id = finish_id, .name = "storage.ha.finish", .kind = .quiescence });
                return;
            }

            if (!state.promoted) {
                try list.append(allocator, .{ .id = append_id, .name = "storage.ha.primary.append", .kind = .workload });
                try list.append(allocator, .{ .id = durability_id, .name = "storage.ha.check_remote_apply_durability", .kind = .workload });
                try list.append(allocator, .{ .id = retention_id, .name = "storage.ha.evaluate_retention", .kind = .maintenance });
                if (!state.backup_started) try list.append(allocator, .{ .id = backup_id, .name = "storage.ha.begin_base_backup", .kind = .maintenance });
                if (!partitionActive(state) and !state.reseed_required and state.standby.nextReceiveLsn() <= state.primary.lastLsn())
                    try list.append(allocator, .{ .id = receive_id, .name = "storage.ha.standby.receive_next", .kind = .scheduler });
                const progress = state.standby.currentProgress();
                if (progress.applied_lsn < progress.received_lsn)
                    try list.append(allocator, .{ .id = apply_id, .name = "storage.ha.standby.apply_available", .kind = .scheduler });
                if (!partitionActive(state) and !state.reseed_required)
                    try list.append(allocator, .{ .id = report_id, .name = "storage.ha.primary.report_progress", .kind = .scheduler });
                if (partitionActive(state) and state.receipt == null) {
                    try list.append(allocator, .{ .id = unfenced_promotion_id, .name = "storage.ha.reject_unfenced_promotion", .kind = .workload });
                    if (progress.received_lsn > 0)
                        try list.append(allocator, .{ .id = acquire_fence_id, .name = "storage.ha.acquire_promotion_fence", .kind = .maintenance });
                }
                if (state.receipt != null)
                    try list.append(allocator, .{ .id = promote_id, .name = "storage.ha.promote_standby", .kind = .workload });
            } else {
                try list.append(allocator, .{ .id = rejoin_id, .name = "storage.ha.assess_former_primary", .kind = .maintenance });
            }

            try list.append(allocator, .{ .id = restart_primary_id, .name = "storage.ha.primary.restart", .kind = .fault });
            try list.append(allocator, .{ .id = restart_standby_id, .name = "storage.ha.standby.restart", .kind = .fault });
            const partition = partitionSpec();
            if (state.faults.isActive(partition.id))
                try list.append(allocator, partition.stopTransition())
            else if ((try state.faults.admission(partition)).isAllowed())
                try list.append(allocator, partition.startTransition());
        }

        pub fn execute(
            world: *World,
            selected: vopr.transition.Transition,
            events: *vopr.event.Sink,
            allocator: std.mem.Allocator,
        ) !vopr.outcome.TransitionOutcome {
            const state = world.state;
            if (selected.id == finish_id) {
                try finish(state, events, allocator);
                return vopr.outcome.TransitionOutcome.targetReached("storage.ha.campaign_complete", state.actions);
            }
            state.actions += 1;

            if (selected.id == append_id) {
                const value = state.appended.items.len + 1;
                var payload: [8]u8 = undefined;
                std.mem.writeInt(u64, &payload, value, .little);
                _ = try state.primary.append(.{ .payload = &payload });
                try state.appended.append(allocator, value);
                try events.emitNamed(allocator, .client_response, "storage.ha.primary_append_acknowledged", value);
            } else if (selected.id == receive_id) {
                try receiveNext(state);
                try events.emitNamed(allocator, .state_change, "storage.ha.record_received", state.standby.currentProgress().received_lsn);
            } else if (selected.id == apply_id) {
                const count = try state.standby.applyAvailable(&state.applied, ApplyModel.apply);
                try events.emitNamed(allocator, .state_change, "storage.ha.records_applied", count);
            } else if (selected.id == report_id) {
                try reportProgress(state);
                try events.emitNamed(allocator, .state_change, "storage.ha.progress_reported", state.standby.currentProgress().applied_lsn);
            } else if (selected.id == durability_id) {
                state.durability_sound = state.durability_sound and try remoteApplyDecisionSound(state);
            } else if (selected.id == restart_primary_id) {
                try restartPrimary(state, events, allocator);
            } else if (selected.id == restart_standby_id) {
                try restartStandby(state, events, allocator);
            } else if (selected.id == partitionSpec().startTransition().id) {
                try state.faults.start(partitionSpec(), events, allocator);
            } else if (selected.id == partitionSpec().stopTransition().id) {
                try state.faults.stop(partitionSpec().id, events, allocator);
            } else if (selected.id == retention_id) {
                _ = try state.primary.retentionSnapshot(.{ .max_lag_lsn = 3 });
                state.retention_evaluations += 1;
                state.reseed_required = if (state.primary.slot(stream_slot)) |slot| slot.reseed_required else true;
                try events.emitNamed(allocator, .state_change, "storage.ha.retention_evaluated", @intFromBool(state.reseed_required));
            } else if (selected.id == backup_id) {
                _ = try state.primary.beginBaseBackup(.{ .slot_name = seed_slot, .manifest_id = "vopr-seed" });
                state.backup_started = true;
                state.backup_pin_valid = state.backup_pin_valid and backupPinValid(state);
                try events.emitNamed(allocator, .state_change, "storage.ha.base_backup_started", state.primary.lastLsn());
            } else if (selected.id == unfenced_promotion_id) {
                const progress = state.standby.currentProgress();
                _ = state.standby.promote(.{
                    .new_timeline_id = 2,
                    .new_epoch = 2,
                    .required_lsn = @max(@as(u64, 1), state.primary.lastLsn()),
                }) catch |err| {
                    if (err != error.FencingRequired) return err;
                    state.unfenced_rejections += 1;
                    try events.emitNamed(allocator, .client_response, "storage.ha.unfenced_promotion_rejected", progress.applied_lsn);
                    return vopr.outcome.TransitionOutcome.rejected("storage.ha.fencing_required", progress.applied_lsn);
                };
                state.promotion_fenced = false;
            } else if (selected.id == acquire_fence_id) {
                const progress = state.standby.currentProgress();
                const required = @max(@as(u64, 1), state.primary.lastLsn());
                state.receipt = try state.fence_store.acquirePromotionFence(.{
                    .identity = state.original_identity,
                    .old_primary_id = "primary-a",
                    .promoted_node_id = "standby-a",
                    .new_timeline_id = 2,
                    .new_epoch = 2,
                    .required_lsn = required,
                    .observed_lsn = progress.applied_lsn,
                    .force = progress.applied_lsn < required,
                    .reason = "vopr-partition",
                });
                try events.emitNamed(allocator, .state_change, "storage.ha.promotion_fence_acquired", state.receipt.?.generation);
            } else if (selected.id == promote_id) {
                const receipt = state.receipt orelse return error.PromotionFenceMissing;
                _ = try state.standby.promote(receipt.promotionRequest());
                state.promoted = true;
                state.promotion_fenced = state.promotion_fenced and true;
                const identity = state.standby.identitySnapshot();
                state.timeline_monotonic = state.timeline_monotonic and identity.timeline_id > state.original_identity.timeline_id and identity.epoch > state.original_identity.epoch;
                try events.emitNamed(allocator, .state_change, "storage.ha.standby_promoted", identity.timeline_id);
            } else if (selected.id == rejoin_id) {
                const assessment = rejoin.assessFormerPrimary(.{
                    .node_id = "primary-a",
                    .identity = state.original_identity,
                    .last_lsn = state.primary.lastLsn(),
                }, state.receipt, .{ .retained_from_lsn = 1 });
                state.rejoin_safe = state.rejoin_safe and (assessment.action != .rewind or
                    (assessment.former_last_lsn == assessment.fork_lsn and
                        assessment.fork_lsn >= assessment.retained_from_lsn and
                        !assessment.forced));
                try events.emitNamed(allocator, .state_change, "storage.ha.former_primary_assessed", @intFromEnum(assessment.action));
            } else return error.UnknownHaVoprTransition;

            state.durability_sound = state.durability_sound and try remoteApplyDecisionSound(state);
            return vopr.outcome.TransitionOutcome.applied();
        }

        pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
            const state = world.state;
            const progress = state.standby.currentProgress();
            const identity = state.standby.identitySnapshot();
            const slot = state.primary.slot(stream_slot);
            try builder.addNamed(allocator, "storage.ha.actions", @intCast(state.actions));
            try builder.addNamed(allocator, "storage.ha.primary_lsn", @intCast(state.primary.lastLsn()));
            try builder.addNamed(allocator, "storage.ha.received_lsn", @intCast(progress.received_lsn));
            try builder.addNamed(allocator, "storage.ha.applied_lsn", @intCast(progress.applied_lsn));
            try builder.addNamed(allocator, "storage.ha.safe_read_lsn", @intCast(progress.safe_read_lsn));
            try builder.addNamed(allocator, "storage.ha.slot_received_lsn", @intCast(if (slot) |value| value.received_lsn else 0));
            try builder.addNamed(allocator, "storage.ha.slot_applied_lsn", @intCast(if (slot) |value| value.applied_lsn else 0));
            try builder.addNamed(allocator, "storage.ha.applied_payloads", @intCast(state.applied.values.items.len));
            try builder.addNamed(allocator, "storage.ha.primary_restarts", @intCast(state.primary_restarts));
            try builder.addNamed(allocator, "storage.ha.standby_restarts", @intCast(state.standby_restarts));
            try builder.addNamed(allocator, "storage.ha.partitioned", @intFromBool(partitionActive(state)));
            try builder.addNamed(allocator, "storage.ha.backup_started", @intFromBool(state.backup_started));
            try builder.addNamed(allocator, "storage.ha.reseed_required", @intFromBool(state.reseed_required));
            try builder.addNamed(allocator, "storage.ha.fence_generation", @intCast(if (state.receipt) |receipt| receipt.generation else 0));
            try builder.addNamed(allocator, "storage.ha.timeline", @intCast(identity.timeline_id));
            try builder.addNamed(allocator, "storage.ha.epoch", @intCast(identity.epoch));
            try builder.addNamed(allocator, "storage.ha.promoted", @intFromBool(state.promoted));
            try builder.addNamed(allocator, "storage.ha.finished", @intFromBool(state.finished));
        }

        pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
            const state = world.state;
            const progress = state.standby.currentProgress();
            const ordered = progress.safe_read_lsn <= progress.applied_lsn and progress.applied_lsn <= progress.received_lsn;
            try sink.check(allocator, progress_ordered_id, ordered);
            try sink.check(allocator, applied_prefix_id, appliedIsPrefix(state));
            try sink.check(allocator, durability_sound_id, state.durability_sound);
            try sink.check(allocator, backup_pinned_id, state.backup_pin_valid);
            try sink.check(allocator, fenced_promotion_id, !state.promoted or (state.receipt != null and state.promotion_fenced));
            try sink.check(allocator, timeline_id, state.timeline_monotonic);
            try sink.check(allocator, rejoin_safe_id, state.rejoin_safe);
            try sink.check(allocator, complete_id, state.finished);
        }

        pub fn done(world: *World) bool {
            return world.state.finished;
        }
    };
}

pub const CliScenario = Scenario(32);

pub fn record(allocator: std.mem.Allocator, seed: u64) !vopr.trace.Trace {
    var seeded = vopr.choice.Seeded.init(seed);
    return vopr.runner.run(CliScenario, allocator, seeded.source(), .{
        .system = "antfly",
        .seed = seed,
        .transition_budget = 33,
        .source_revision = "ha-vopr-cli",
        .target = "native",
        .optimize = @tagName(builtin.mode),
    });
}

pub fn replay(allocator: std.mem.Allocator, artifact: *const vopr.trace.Trace) !vopr.trace.Trace {
    return vopr.replay.exact(CliScenario, allocator, artifact);
}

fn path(allocator: std.mem.Allocator, nonce: u64, part: []const u8) ![:0]u8 {
    // The process-local address prevents a prior aborted test process from
    // colliding with a fresh replay whose nonce starts at zero again.
    return std.fmt.allocPrintSentinel(allocator, ".zig-cache/tmp/ha-vopr-{x}-{d}-{s}", .{ @intFromPtr(&path_nonce), nonce, part }, 0); // vopr-audit: allow(unstable_identity) pointer value isolates native differential scratch paths and never enters trace semantics
}

fn partitionSpec() vopr.fault.Spec {
    return .{
        .id = vopr.id.stable("fault", "storage.ha.primary_standby_partition"),
        .name = "storage.ha.primary_standby_partition",
        .kind = .partitioned_link,
        .lifecycle = .persistent,
        .resource_id = vopr.id.stable("resource", "storage.ha.replication_link"),
    };
}

fn nodeCrashSpec(name: []const u8) vopr.fault.Spec {
    return .{
        .id = vopr.id.stable("fault", name),
        .name = name,
        .kind = .node,
        .lifecycle = .one_shot,
        .resource_id = vopr.id.stable("resource", name),
    };
}

fn partitionActive(state: anytype) bool {
    return state.faults.isActive(partitionSpec().id);
}

fn receiveNext(state: anytype) !void {
    const entries = try state.primary.streamFrom(state.allocator, stream_slot, state.standby.nextReceiveLsn());
    defer replication_log.freeEntries(state.allocator, entries);
    if (entries.len == 0) return error.NoHaRecordAvailable;
    _ = try state.standby.receive(entries[0].record);
}

fn reportProgress(state: anytype) !void {
    const progress = state.standby.currentProgress();
    try state.primary.standbyStatusUpdateWithSafeRead(
        stream_slot,
        state.standby.identitySnapshot().timeline_id,
        progress.received_lsn,
        progress.applied_lsn,
        progress.safe_read_lsn,
    );
}

fn restartPrimary(state: anytype, events: *vopr.event.Sink, allocator: std.mem.Allocator) !void {
    const spec = nodeCrashSpec("storage.ha.primary.restart");
    try state.faults.start(spec, events, allocator);
    const backup_before = backupSlotSnapshot(state);
    state.primary.close();
    state.primary = try primary_mod.Primary.open(allocator, state.paths.primary_log.ptr, state.paths.primary_slots.ptr, state.original_identity, .{});
    state.backup_pin_valid = state.backup_pin_valid and std.meta.eql(backup_before, backupSlotSnapshot(state));
    state.primary_restarts += 1;
    _ = try state.faults.consumeOneShot(.node, spec.resource_id, events, allocator);
}

fn restartStandby(state: anytype, events: *vopr.event.Sink, allocator: std.mem.Allocator) !void {
    const spec = nodeCrashSpec("storage.ha.standby.restart");
    try state.faults.start(spec, events, allocator);
    state.standby.close();
    state.standby = try standby_mod.Standby.open(allocator, state.paths.standby_log.ptr, state.paths.standby_progress.ptr, state.original_identity, .{});
    state.standby_restarts += 1;
    _ = try state.faults.consumeOneShot(.node, spec.resource_id, events, allocator);
}

fn backupPinValid(state: anytype) bool {
    if (!state.backup_started) return true;
    const slot = state.primary.slot(seed_slot) orelse return false;
    return slot.lifecycle == .seeding and !slot.active and !slot.reseed_required and slot.restart_lsn <= state.primary.lastLsn();
}

const BackupSlotSnapshot = struct {
    present: bool = false,
    timeline_id: u64 = 0,
    restart_lsn: u64 = 0,
    received_lsn: u64 = 0,
    applied_lsn: u64 = 0,
    safe_read_lsn: u64 = 0,
    active: bool = false,
    lifecycle: u8 = 0,
    reseed_required: bool = false,
};

fn backupSlotSnapshot(state: anytype) BackupSlotSnapshot {
    const slot = state.primary.slot(seed_slot) orelse return .{};
    return .{
        .present = true,
        .timeline_id = slot.timeline_id,
        .restart_lsn = slot.restart_lsn,
        .received_lsn = slot.received_lsn,
        .applied_lsn = slot.applied_lsn,
        .safe_read_lsn = slot.safe_read_lsn,
        .active = slot.active,
        .lifecycle = @intFromEnum(slot.lifecycle),
        .reseed_required = slot.reseed_required,
    };
}

fn appliedIsPrefix(state: anytype) bool {
    if (state.applied.values.items.len > state.appended.items.len) return false;
    return std.mem.eql(u64, state.applied.values.items, state.appended.items[0..state.applied.values.items.len]);
}

fn remoteApplyDecisionSound(state: anytype) !bool {
    if (state.primary.lastLsn() == 0) return true;
    const names = [_][]const u8{stream_slot};
    const decision = try state.primary.evaluateDurability(state.primary.lastLsn(), .{
        .mode = .remote_apply,
        .standby_names = &names,
    });
    const slot = state.primary.slot(stream_slot) orelse return decision.status != .satisfied;
    return (decision.status == .satisfied) == (slot.active and !slot.reseed_required and slot.applied_lsn >= state.primary.lastLsn());
}

fn finish(state: anytype, events: *vopr.event.Sink, allocator: std.mem.Allocator) !void {
    if (partitionActive(state)) try state.faults.stop(partitionSpec().id, events, allocator);
    if (!state.promoted and !state.reseed_required) {
        while (state.standby.nextReceiveLsn() <= state.primary.lastLsn()) try receiveNext(state);
        _ = try state.standby.applyAvailable(&state.applied, ApplyModel.apply);
        try reportProgress(state);
    }
    try restartPrimary(state, events, allocator);
    try restartStandby(state, events, allocator);
    state.faults.beginQuietSuffix();
    state.durability_sound = state.durability_sound and try remoteApplyDecisionSound(state);
    state.finished = true;
    try events.emitNamed(allocator, .domain, "storage.ha.campaign_complete", state.actions);
}

fn runRecordReplay(comptime budget: u64, seed: u64) !void {
    const HaScenario = Scenario(budget);
    var seeded = vopr.choice.Seeded.init(seed);
    var artifact = try vopr.runner.run(HaScenario, std.testing.allocator, seeded.source(), .{
        .system = "antfly",
        .seed = seed,
        .transition_budget = budget + 1,
    });
    defer artifact.deinit();
    try std.testing.expectEqual(@as(u64, 0), artifact.summary.?.property_failures);
    var replayed = try vopr.replay.exact(HaScenario, std.testing.allocator, &artifact);
    replayed.deinit();
}

fn runScripted(comptime budget: u64, selections: []const vopr.id.StableId) !vopr.trace.Trace {
    const HaScenario = Scenario(budget);
    var scripted = vopr.choice.Scripted{ .selections = selections };
    var artifact = try vopr.runner.run(HaScenario, std.testing.allocator, scripted.source(), .{
        .system = "antfly",
        .transition_budget = selections.len,
    });
    errdefer artifact.deinit();
    try std.testing.expectEqual(@as(u64, 0), artifact.summary.?.property_failures);
    var replayed = try vopr.replay.exact(HaScenario, std.testing.allocator, &artifact);
    replayed.deinit();
    return artifact;
}

fn expectEvent(artifact: *const vopr.trace.Trace, name: []const u8) !void {
    for (artifact.events.items) |value| {
        if (std.mem.eql(u8, value.name, name)) return;
    }
    return error.ExpectedHaVoprEventMissing;
}

test "HA VOPR replays crash standby fencing retention backup and promotion lifecycles" {
    try runRecordReplay(32, 0xA17F_AA11);
    try runRecordReplay(32, 0xA17F_AA12);
}

test "HA VOPR preserves exact progress and property streams across fresh worlds" {
    const HaScenario = Scenario(20);
    var seeded = vopr.choice.Seeded.init(0xA17F_AA13);
    var artifact = try vopr.runner.run(HaScenario, std.testing.allocator, seeded.source(), .{
        .system = "antfly",
        .seed = 0xA17F_AA13,
        .transition_budget = 21,
    });
    defer artifact.deinit();
    for (0..5) |_| {
        var replayed = try vopr.replay.exact(HaScenario, std.testing.allocator, &artifact);
        replayed.deinit();
    }
}

test "HA VOPR scripted partition rejects unfenced promotion and safely fences promotes and rejoins" {
    const selections = [_]vopr.id.StableId{
        append_id,
        receive_id,
        apply_id,
        report_id,
        partitionSpec().startTransition().id,
        unfenced_promotion_id,
        acquire_fence_id,
        promote_id,
        rejoin_id,
        finish_id,
    };
    var artifact = try runScripted(9, &selections);
    defer artifact.deinit();
    try expectEvent(&artifact, "storage.ha.unfenced_promotion_rejected");
    try expectEvent(&artifact, "storage.ha.promotion_fence_acquired");
    try expectEvent(&artifact, "storage.ha.standby_promoted");
    try expectEvent(&artifact, "storage.ha.former_primary_assessed");
}

test "HA VOPR scripted receive apply report and backup crash windows recover durably" {
    const selections = [_]vopr.id.StableId{
        append_id,
        receive_id,
        restart_standby_id,
        apply_id,
        restart_standby_id,
        report_id,
        restart_primary_id,
        durability_id,
        backup_id,
        restart_primary_id,
        finish_id,
    };
    var artifact = try runScripted(10, &selections);
    defer artifact.deinit();
    try expectEvent(&artifact, "storage.ha.base_backup_started");
    try expectEvent(&artifact, "storage.ha.campaign_complete");
}

test "HA VOPR scripted retention expires lagging stream and persists reseed state" {
    const selections = [_]vopr.id.StableId{
        append_id,
        append_id,
        append_id,
        append_id,
        append_id,
        retention_id,
        restart_primary_id,
        finish_id,
    };
    var artifact = try runScripted(7, &selections);
    defer artifact.deinit();
    try expectEvent(&artifact, "storage.ha.retention_evaluated");
    const final = artifact.observations.items[artifact.observations.items.len - 1];
    var saw_reseed = false;
    for (final.features) |feature| {
        if (std.mem.eql(u8, feature.name, "storage.ha.reseed_required")) {
            try std.testing.expectEqual(@as(i64, 1), feature.value);
            saw_reseed = true;
        }
    }
    try std.testing.expect(saw_reseed);
}
