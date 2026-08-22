// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Adapter from the modeled WAL campaign to the standalone VOPR scenario
//! contract. The world owns an in-memory durability model, so a recorded
//! decision stream reconstructs a clean storage generation without depending
//! on a host path, process ID, wall clock, or hidden PRNG.

const std = @import("std");
const vopr = @import("vopr");
const wal_mod = @import("wal.zig");
const storage_sim = @import("sim_runtime.zig");

const Allocator = std.mem.Allocator;
const WAL = wal_mod.WAL;
const WalOptions = wal_mod.WalOptions;

const Entry = struct {
    const Durability = enum { required, uncertain };

    lsn: u64,
    data: []u8,
    durability: Durability = .required,
};

const append_id = vopr.id.stable("transition", "storage.wal.append");
const append_batch_base = vopr.id.stable("transition", "storage.wal.append_batch");
const reopen_base = vopr.id.stable("transition", "storage.wal.reopen_and_verify_from");
const truncate_base = vopr.id.stable("transition", "storage.wal.truncate_and_verify_from");
const verify_base = vopr.id.stable("transition", "storage.wal.verify_from");
const crash_id = vopr.id.stable("transition", "storage.wal.crash_and_recover");
const inject_write_failure_id = vopr.id.stable("transition", "storage.wal.inject_write_failure");
const inject_sync_failure_id = vopr.id.stable("transition", "storage.wal.inject_sync_failure");
const inject_partial_write_id = vopr.id.stable("transition", "storage.wal.inject_partial_write");
const inject_dropped_sync_id = vopr.id.stable("transition", "storage.wal.inject_dropped_sync");
const inject_device_full_id = vopr.id.stable("transition", "storage.wal.inject_device_full");

const model_property_id = vopr.id.stable("property", "storage.wal.visible_state_matches_acknowledged_model");
const durable_property_id = vopr.id.stable("property", "storage.wal.acknowledged_entries_survive_crash");
const recovered_property_id = vopr.id.stable("property", "storage.wal.modeled_crash_recovery_reached");

fn ModeledWalScenario(comptime action_budget: u64) type {
    return struct {
        const Self = @This();

        pub const name: []const u8 = "modeled-wal";
        pub const version: u32 = 4;
        pub const properties = &[_]vopr.property.Declaration{
            .{ .id = model_property_id, .name = "storage.wal.visible_state_matches_acknowledged_model", .kind = .always },
            .{ .id = durable_property_id, .name = "storage.wal.acknowledged_entries_survive_crash", .kind = .always },
            .{ .id = recovered_property_id, .name = "storage.wal.modeled_crash_recovery_reached", .kind = .reachable },
        };

        const State = struct {
            const PendingIoFault = enum { write, sync, partial_write, dropped_sync, device_full };

            allocator: Allocator,
            runtime: storage_sim.Runtime,
            device: storage_sim.ModeledDevice,
            opts: WalOptions,
            wal: WAL,
            wal_open: bool = true,
            model: std.ArrayListUnmanaged(Entry) = .empty,
            next_lsn: u64 = 1,
            decisions: u64 = 0,
            reopens: u64 = 0,
            recovered: bool = false,
            recovery_unavailable: bool = false,
            recovery_error_digest: u64 = 0,
            pending_io_fault: ?PendingIoFault = null,
            fault_consumptions_before: u64 = 0,
            rejected_operations: u64 = 0,
            uncertain_acknowledgements: u64 = 0,
            partial_write_outcomes: u64 = 0,
            dropped_sync_outcomes: u64 = 0,
            device_full_outcomes: u64 = 0,

            fn deinit(self: *State) void {
                if (self.wal_open) self.wal.close();
                for (self.model.items) |entry| self.allocator.free(entry.data);
                self.model.deinit(self.allocator);
                self.device.deinit();
                self.runtime.deinit();
            }
        };

        pub const World = struct {
            state: *State,
        };

        pub fn init(allocator: Allocator) !World {
            const state = try allocator.create(State);
            errdefer allocator.destroy(state);

            state.allocator = allocator;
            state.runtime = storage_sim.Runtime.init(allocator);
            errdefer state.runtime.deinit();
            state.device = storage_sim.ModeledDevice.init(allocator);
            errdefer state.device.deinit();
            state.opts = .{
                .backend = .lsm,
                .storage = state.device.storage(),
                .clock = state.runtime.clock(),
                .commit_scheduler = state.runtime.completionScheduler(),
                .group_commit_window_ns = std.time.ns_per_ms,
                .model_commit_backend_completions = true,
            };
            state.wal = try WAL.open("/vopr/modeled-wal", state.opts);
            state.wal_open = true;
            state.model = .empty;
            state.next_lsn = 1;
            state.decisions = 0;
            state.reopens = 0;
            state.recovered = false;
            state.recovery_unavailable = false;
            state.recovery_error_digest = 0;
            state.pending_io_fault = null;
            state.fault_consumptions_before = 0;
            state.rejected_operations = 0;
            state.uncertain_acknowledgements = 0;
            state.partial_write_outcomes = 0;
            state.dropped_sync_outcomes = 0;
            state.device_full_outcomes = 0;
            return .{ .state = state };
        }

        pub fn deinit(world: *World, allocator: Allocator) void {
            world.state.deinit();
            allocator.destroy(world.state);
            world.* = undefined;
        }

        pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: Allocator) !void {
            const state = world.state;
            if (state.decisions >= action_budget and state.pending_io_fault == null) {
                try list.append(allocator, .{
                    .id = crash_id,
                    .name = "storage.wal.crash_and_recover",
                    .kind = .fault,
                });
                return;
            }

            try list.append(allocator, .{
                .id = append_id,
                .name = "storage.wal.append",
                .kind = .workload,
            });
            if (state.pending_io_fault != null) return;
            try list.append(allocator, .{
                .id = inject_write_failure_id,
                .name = "storage.wal.inject_write_failure",
                .kind = .fault,
            });
            try list.append(allocator, .{
                .id = inject_sync_failure_id,
                .name = "storage.wal.inject_sync_failure",
                .kind = .fault,
            });
            try list.append(allocator, .{
                .id = inject_device_full_id,
                .name = "storage.wal.inject_device_full",
                .kind = .fault,
            });
            // Torn writes and silently dropped syncs require immediate crash
            // observation; a later successful sync could legitimately make
            // the uncertain bytes durable. Enable them only for the terminal
            // fault/workload pair so the next transition is recovery.
            if (action_budget - state.decisions == 2) {
                try list.append(allocator, .{
                    .id = inject_partial_write_id,
                    .name = "storage.wal.inject_partial_write",
                    .kind = .fault,
                    .parameter = 7,
                });
                try list.append(allocator, .{
                    .id = inject_dropped_sync_id,
                    .name = "storage.wal.inject_dropped_sync",
                    .kind = .fault,
                });
            }
            for (2..5) |batch_len| {
                try list.append(allocator, .{
                    .id = vopr.id.derive("storage.wal.append_batch", append_batch_base, batch_len),
                    .name = "storage.wal.append_batch",
                    .kind = .workload,
                    .parameter = @intCast(batch_len),
                });
            }

            const cursor_limit = state.next_lsn + 1;
            var from_lsn: u64 = 1;
            while (from_lsn <= cursor_limit) : (from_lsn += 1) {
                try list.append(allocator, .{
                    .id = vopr.id.derive("storage.wal.reopen", reopen_base, from_lsn),
                    .name = "storage.wal.reopen_and_verify_from",
                    .kind = .maintenance,
                    .parameter = @intCast(from_lsn),
                });
                try list.append(allocator, .{
                    .id = vopr.id.derive("storage.wal.verify", verify_base, from_lsn),
                    .name = "storage.wal.verify_from",
                    .kind = .workload,
                    .parameter = @intCast(from_lsn),
                });
            }

            for (state.model.items) |entry| {
                from_lsn = 1;
                while (from_lsn <= cursor_limit) : (from_lsn += 1) {
                    const encoded_pair = packPair(entry.lsn, from_lsn);
                    try list.append(allocator, .{
                        .id = vopr.id.derive("storage.wal.truncate", truncate_base, encoded_pair),
                        .name = "storage.wal.truncate_and_verify_from",
                        .kind = .maintenance,
                        .parameter = @bitCast(encoded_pair),
                    });
                }
            }
        }

        pub fn execute(
            world: *World,
            selected: vopr.transition.Transition,
            events: *vopr.event.Sink,
            allocator: Allocator,
        ) !vopr.outcome.TransitionOutcome {
            const state = world.state;
            if (selected.id == crash_id) {
                try state.device.device().crash();
                state.wal.abandonAfterCrash();
                state.wal_open = false;
                state.wal = WAL.open("/vopr/modeled-wal", state.opts) catch |err| {
                    if (!allowedUnavailableRecovery(state, err)) return err;
                    discardUncertainModel(state);
                    state.next_lsn = 1;
                    state.recovery_unavailable = true;
                    state.recovery_error_digest = vopr.id.digest(@errorName(err));
                    state.recovered = true;
                    try events.emitNamed(allocator, .client_response, "storage.wal.recovery_rejected_uncertain_state", state.recovery_error_digest);
                    return vopr.outcome.TransitionOutcome.rejected("storage.wal.recovery_rejected_uncertain_state", state.recovery_error_digest);
                };
                state.wal_open = true;
                try reconcileRecoveredModel(state);
                state.recovered = true;
                try events.emitNamed(allocator, .state_change, "storage.wal.recovered", state.next_lsn);
                return vopr.outcome.TransitionOutcome.targetReached("storage.wal.recovered", state.next_lsn);
            }

            state.decisions += 1;
            if (selected.id == inject_write_failure_id) {
                state.device.injectWriteFailure();
                state.pending_io_fault = .write;
                try events.emitNamed(allocator, .injected_error, "storage.wal.write_failure_armed", state.decisions);
                return vopr.outcome.TransitionOutcome.applied();
            }
            if (selected.id == inject_sync_failure_id) {
                state.device.injectSyncFailure();
                state.pending_io_fault = .sync;
                try events.emitNamed(allocator, .injected_error, "storage.wal.sync_failure_armed", state.decisions);
                return vopr.outcome.TransitionOutcome.applied();
            }
            if (selected.id == inject_partial_write_id) {
                state.fault_consumptions_before = state.device.partialWriteFaultsConsumed();
                state.device.injectPartialWrite(@intCast(selected.parameter));
                state.pending_io_fault = .partial_write;
                try events.emitNamed(allocator, .injected_error, "storage.wal.partial_write_armed", @intCast(selected.parameter));
                return vopr.outcome.TransitionOutcome.applied();
            }
            if (selected.id == inject_dropped_sync_id) {
                state.fault_consumptions_before = state.device.droppedSyncsConsumed();
                state.device.dropNextSync();
                state.pending_io_fault = .dropped_sync;
                try events.emitNamed(allocator, .injected_error, "storage.wal.dropped_sync_armed", state.decisions);
                return vopr.outcome.TransitionOutcome.applied();
            }
            if (selected.id == inject_device_full_id) {
                state.fault_consumptions_before = state.device.deviceFullFaultsConsumed();
                state.device.setCapacityBytes(state.device.usedBytes());
                state.pending_io_fault = .device_full;
                try events.emitNamed(allocator, .injected_error, "storage.wal.device_full_armed", state.decisions);
                return vopr.outcome.TransitionOutcome.applied();
            }
            if (selected.id == append_id) {
                const rejected_before = state.rejected_operations;
                const injected_before = state.dropped_sync_outcomes;
                try appendOne(state, events, allocator, 0);
                if (state.rejected_operations != rejected_before)
                    return vopr.outcome.TransitionOutcome.rejected("storage.wal.append_rejected", state.rejected_operations);
                if (state.dropped_sync_outcomes != injected_before)
                    return vopr.outcome.TransitionOutcome.injectedError("storage.wal.dropped_sync_consumed", state.dropped_sync_outcomes);
                return vopr.outcome.TransitionOutcome.applied();
            }
            if (selected.id == vopr.id.derive("storage.wal.append_batch", append_batch_base, @intCast(selected.parameter))) {
                const rejected_before = state.rejected_operations;
                const injected_before = state.dropped_sync_outcomes;
                try appendBatch(state, events, allocator, @intCast(selected.parameter));
                if (state.rejected_operations != rejected_before)
                    return vopr.outcome.TransitionOutcome.rejected("storage.wal.append_batch_rejected", state.rejected_operations);
                if (state.dropped_sync_outcomes != injected_before)
                    return vopr.outcome.TransitionOutcome.injectedError("storage.wal.dropped_sync_consumed", state.dropped_sync_outcomes);
                return vopr.outcome.TransitionOutcome.applied();
            }
            if (std.mem.eql(u8, selected.name, "storage.wal.reopen_and_verify_from")) {
                try reopen(state);
                state.reopens += 1;
                try events.emitNamed(allocator, .state_change, "storage.wal.reopened", @intCast(selected.parameter));
                return vopr.outcome.TransitionOutcome.applied();
            }
            if (std.mem.eql(u8, selected.name, "storage.wal.verify_from")) {
                try events.emitNamed(allocator, .domain, "storage.wal.verified", @intCast(selected.parameter));
                return vopr.outcome.TransitionOutcome.applied();
            }
            if (std.mem.eql(u8, selected.name, "storage.wal.truncate_and_verify_from")) {
                const pair = unpackPair(@bitCast(selected.parameter));
                try state.wal.truncate(pair.left);
                truncateModel(state, pair.left);
                try events.emitNamed(allocator, .state_change, "storage.wal.truncated", pair.left);
                return vopr.outcome.TransitionOutcome.applied();
            }
            return error.UnknownWalVoprTransition;
        }

        pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: Allocator) !void {
            const state = world.state;
            try builder.addNamed(allocator, "storage.wal.model_entries", @intCast(state.model.items.len));
            try builder.addNamed(allocator, "storage.wal.last_lsn", @intCast(if (state.wal_open) state.wal.lastLsn() else lastLsn(state.next_lsn)));
            try builder.addNamed(allocator, "storage.wal.next_lsn", @intCast(state.next_lsn));
            try builder.addNamed(allocator, "storage.wal.model_digest", @bitCast(modelDigest(state.model.items)));
            try builder.addNamed(allocator, "storage.wal.virtual_time_ns", @intCast(state.runtime.clock().nowNs()));
            try builder.addNamed(allocator, "storage.wal.reopen_count", @intCast(state.reopens));
            try builder.addNamed(allocator, "storage.wal.recovered", @intFromBool(state.recovered));
            try builder.addNamed(allocator, "storage.wal.recovery_unavailable", @intFromBool(state.recovery_unavailable));
            try builder.addNamed(allocator, "storage.wal.recovery_error_digest", @bitCast(state.recovery_error_digest));
            try builder.addNamed(allocator, "storage.wal.io_fault_pending", @intFromBool(state.pending_io_fault != null));
            try builder.addNamed(allocator, "storage.wal.rejected_operations", @intCast(state.rejected_operations));
            try builder.addNamed(allocator, "storage.wal.uncertain_acknowledgements", @intCast(state.uncertain_acknowledgements));
            try builder.addNamed(allocator, "storage.wal.partial_write_outcomes", @intCast(state.partial_write_outcomes));
            try builder.addNamed(allocator, "storage.wal.dropped_sync_outcomes", @intCast(state.dropped_sync_outcomes));
            try builder.addNamed(allocator, "storage.wal.device_full_outcomes", @intCast(state.device_full_outcomes));
        }

        pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: Allocator) !void {
            const state = world.state;
            const matches = if (state.recovery_unavailable) !hasRequiredEntries(state.model.items) else try stateMatches(state, 1);
            try sink.check(allocator, model_property_id, matches);
            try sink.check(allocator, durable_property_id, !state.recovered or matches);
            try sink.check(allocator, recovered_property_id, state.recovered);
        }

        pub fn done(world: *World) bool {
            return world.state.recovered;
        }
    };
}

fn appendOne(state: anytype, events: *vopr.event.Sink, allocator: Allocator, slot: usize) !void {
    const payload = try payloadAlloc(state.allocator, state.decisions, slot);
    errdefer state.allocator.free(payload);
    const lsn = state.wal.append(payload) catch |err| {
        const pending = state.pending_io_fault orelse return err;
        const expected = switch (pending) {
            .write => err == error.InjectedWriteFault,
            .sync => err == error.InjectedSyncFault,
            .partial_write => err == error.InjectedPartialWriteFault,
            .dropped_sync => false,
            .device_full => err == error.InjectedDeviceFull,
        };
        if (!expected) return err;
        if (pending == .partial_write) {
            if (state.device.partialWriteFaultsConsumed() != state.fault_consumptions_before + 1)
                return error.InjectedPartialWriteFaultWasNotConsumed;
            state.partial_write_outcomes += 1;
        }
        if (pending == .device_full) {
            if (state.device.deviceFullFaultsConsumed() != state.fault_consumptions_before + 1)
                return error.InjectedDeviceFullWasNotConsumed;
            state.device.setCapacityBytes(null);
            state.device_full_outcomes += 1;
        }
        state.pending_io_fault = null;
        state.rejected_operations += 1;
        try events.emitNamed(allocator, .client_response, "storage.wal.append_rejected", vopr.id.digest(@errorName(err)));
        state.allocator.free(payload);
        return;
    };
    const durability: Entry.Durability = if (state.pending_io_fault) |pending| switch (pending) {
        .dropped_sync => blk: {
            if (state.device.droppedSyncsConsumed() != state.fault_consumptions_before + 1)
                return error.InjectedDroppedSyncWasNotConsumed;
            state.pending_io_fault = null;
            state.dropped_sync_outcomes += 1;
            state.uncertain_acknowledgements += 1;
            break :blk .uncertain;
        },
        .device_full => return error.InjectedDeviceFullWasNotConsumed,
        else => return error.InjectedIoFaultWasNotConsumed,
    } else .required;
    if (lsn != state.next_lsn) return error.WalLsnDiverged;
    if (durability == .required) {
        // A successful later sync makes any earlier uncertain prefix durable.
        for (state.model.items) |*entry| entry.durability = .required;
    }
    try state.model.append(state.allocator, .{ .lsn = lsn, .data = payload, .durability = durability });
    state.next_lsn += 1;
    try events.emitNamed(
        allocator,
        .client_response,
        if (durability == .required) "storage.wal.append_acknowledged" else "storage.wal.append_acknowledged_with_uncertain_durability",
        lsn,
    );
}

fn appendBatch(state: anytype, events: *vopr.event.Sink, allocator: Allocator, count: usize) !void {
    var owned: [4][]u8 = undefined;
    var entries: [4][]const u8 = undefined;
    var made: usize = 0;
    errdefer for (owned[0..made]) |payload| state.allocator.free(payload);
    while (made < count) : (made += 1) {
        owned[made] = try payloadAlloc(state.allocator, state.decisions, made);
        entries[made] = owned[made];
    }
    const result = try state.wal.appendBatch(entries[0..count]);
    if (result.first_lsn != state.next_lsn or result.count != count) return error.WalBatchLsnDiverged;
    for (state.model.items) |*entry| entry.durability = .required;
    try state.model.ensureUnusedCapacity(state.allocator, count);
    for (owned[0..count], 0..) |payload, index| {
        state.model.appendAssumeCapacity(.{ .lsn = result.lsnAt(index), .data = payload });
    }
    state.next_lsn += count;
    try events.emitNamed(allocator, .client_response, "storage.wal.batch_acknowledged", result.first_lsn);
}

fn payloadAlloc(allocator: Allocator, decision: u64, slot: usize) ![]u8 {
    return std.fmt.allocPrint(allocator, "vopr-action-{d}-slot-{d}", .{ decision, slot });
}

fn reopen(state: anytype) !void {
    state.wal.close();
    state.wal_open = false;
    state.wal = try WAL.open("/vopr/modeled-wal", state.opts);
    state.wal_open = true;
}

fn truncateModel(state: anytype, up_to_lsn: u64) void {
    var kept: usize = 0;
    for (state.model.items) |entry| {
        if (entry.lsn <= up_to_lsn) {
            state.allocator.free(entry.data);
            continue;
        }
        state.model.items[kept] = entry;
        kept += 1;
    }
    state.model.items.len = kept;
}

fn reconcileRecoveredModel(state: anytype) !void {
    const actual = try state.wal.iterateFrom(state.allocator, 1);
    defer {
        for (actual) |entry| state.allocator.free(@constCast(entry.data));
        state.allocator.free(actual);
    }
    if (actual.len > state.model.items.len) return error.WalRecoveryProducedUnacknowledgedEntry;
    for (actual, state.model.items[0..actual.len]) |recovered, expected| {
        if (recovered.lsn != expected.lsn or !std.mem.eql(u8, recovered.data, expected.data))
            return error.WalRecoveryOutcomeOutsideModeledSet;
    }
    for (state.model.items[actual.len..]) |missing| {
        if (missing.durability == .required) return error.WalRecoveryLostRequiredEntry;
        state.allocator.free(missing.data);
    }
    state.model.items.len = actual.len;
    for (state.model.items) |*entry| entry.durability = .required;
    state.next_lsn = state.wal.lastLsn() + 1;
}

fn allowedUnavailableRecovery(state: anytype, err: anyerror) bool {
    if (hasRequiredEntries(state.model.items)) return false;
    return switch (err) {
        error.CorruptLsmWalIndex,
        error.UnsupportedLsmWalHeader,
        => true,
        else => false,
    };
}

fn hasRequiredEntries(entries: []const Entry) bool {
    for (entries) |entry| if (entry.durability == .required) return true;
    return false;
}

fn discardUncertainModel(state: anytype) void {
    std.debug.assert(!hasRequiredEntries(state.model.items));
    for (state.model.items) |entry| state.allocator.free(entry.data);
    state.model.clearRetainingCapacity();
}

fn stateMatches(state: anytype, from_lsn: u64) !bool {
    if (state.wal.lastLsn() != lastLsn(state.next_lsn)) return false;
    const actual = try state.wal.iterateFrom(state.allocator, from_lsn);
    defer {
        for (actual) |entry| state.allocator.free(@constCast(entry.data));
        state.allocator.free(actual);
    }

    var expected_count: usize = 0;
    for (state.model.items) |entry| if (entry.lsn >= from_lsn) {
        expected_count += 1;
    };
    if (actual.len != expected_count) return false;
    var actual_index: usize = 0;
    for (state.model.items) |entry| {
        if (entry.lsn < from_lsn) continue;
        if (entry.lsn != actual[actual_index].lsn) return false;
        if (!std.mem.eql(u8, entry.data, actual[actual_index].data)) return false;
        actual_index += 1;
    }
    return true;
}

fn lastLsn(next_lsn: u64) u64 {
    return if (next_lsn <= 1) 0 else next_lsn - 1;
}

fn modelDigest(entries: []const Entry) u64 {
    var digest: u64 = 0;
    for (entries) |entry| {
        digest = vopr.id.derive("storage.wal.model.lsn", digest, entry.lsn);
        digest = vopr.id.derive("storage.wal.model.data", digest, vopr.id.digest(entry.data));
        digest = vopr.id.derive("storage.wal.model.durability", digest, @intFromEnum(entry.durability));
    }
    return digest;
}

fn packPair(left: u64, right: u64) u64 {
    std.debug.assert(left <= std.math.maxInt(u32));
    std.debug.assert(right <= std.math.maxInt(u32));
    return (left << 32) | right;
}

const Pair = struct { left: u64, right: u64 };

fn unpackPair(encoded_pair: u64) Pair {
    return .{ .left = encoded_pair >> 32, .right = encoded_pair & std.math.maxInt(u32) };
}

fn runRecordReplay(comptime action_budget: u64, seed: u64) !void {
    const Scenario = ModeledWalScenario(action_budget);
    var seeded = vopr.choice.Seeded.init(seed);
    var recorded = try vopr.runner.run(Scenario, std.testing.allocator, seeded.source(), .{
        .system = "antfly",
        .seed = seed,
        .transition_budget = action_budget + 1,
        .source_revision = "wal-vopr-test",
        .target = "native",
        .optimize = @tagName(@import("builtin").mode),
    });
    defer recorded.deinit();
    try std.testing.expectEqual(@as(u64, 0), recorded.summary.?.property_failures);
    try std.testing.expectEqual(action_budget + 1, recorded.summary.?.transitions);
    try std.testing.expect(recorded.faults.items.len >= 1);
    try std.testing.expectEqual(crash_id, recorded.faults.items[recorded.faults.items.len - 1].id);

    const encoded = try recorded.renderAlloc(std.testing.allocator);
    defer std.testing.allocator.free(encoded);
    var parsed = try vopr.trace.parseAlloc(std.testing.allocator, encoded);
    defer parsed.deinit();
    var replayed = try vopr.replay.exact(Scenario, std.testing.allocator, &parsed);
    replayed.deinit();
}

test "modeled WAL campaign records and exactly replays VOPR traces" {
    try runRecordReplay(24, 0xA17F_5001);
    try runRecordReplay(24, 0xA17F_5002);
}

test "modeled WAL VOPR classifies injected write and sync outcomes" {
    const Scenario = ModeledWalScenario(6);
    const FaultSequence = struct {
        phase: usize = 0,

        fn source(self: *@This()) vopr.choice.Source {
            return .{ .ptr = self, .choose_fn = choose, .finish_fn = finish };
        }

        fn finish(_: *anyopaque) !void {}

        fn choose(ptr: *anyopaque, request: vopr.choice.Request) !u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const names = [_][]const u8{
                "storage.wal.inject_write_failure",
                "storage.wal.append",
                "storage.wal.inject_sync_failure",
                "storage.wal.append",
                "storage.wal.inject_device_full",
                "storage.wal.append",
                "storage.wal.crash_and_recover",
            };
            const wanted = names[self.phase];
            for (request.enabled) |candidate| {
                if (!std.mem.eql(u8, candidate.name, wanted)) continue;
                self.phase += 1;
                return candidate.id;
            }
            return error.ExpectedWalFaultTransitionNotEnabled;
        }
    };

    var sequence = FaultSequence{};
    var recorded = try vopr.runner.run(Scenario, std.testing.allocator, sequence.source(), .{
        .system = "antfly",
        .seed = 0xA17F_FA17,
        .transition_budget = 7,
    });
    defer recorded.deinit();
    try std.testing.expectEqual(@as(usize, 7), sequence.phase);
    try std.testing.expectEqual(@as(usize, 4), recorded.faults.items.len);
    try std.testing.expectEqual(@as(u64, 0), recorded.summary.?.property_failures);
    var rejected: usize = 0;
    for (recorded.events.items) |event| rejected += @intFromBool(std.mem.eql(u8, event.name, "storage.wal.append_rejected"));
    try std.testing.expectEqual(@as(usize, 3), rejected);

    var replayed = try vopr.replay.exact(Scenario, std.testing.allocator, &recorded);
    replayed.deinit();
}

test "modeled WAL VOPR constrains partial-write and dropped-sync recovery outcomes" {
    const Scenario = ModeledWalScenario(2);
    const Sequence = struct {
        fault_name: []const u8,
        phase: usize = 0,

        fn source(self: *@This()) vopr.choice.Source {
            return .{ .ptr = self, .choose_fn = choose, .finish_fn = finish };
        }

        fn finish(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.phase != 3) return error.WalOutcomeSequenceIncomplete;
        }

        fn choose(ptr: *anyopaque, request: vopr.choice.Request) !u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const wanted = switch (self.phase) {
                0 => self.fault_name,
                1 => "storage.wal.append",
                2 => "storage.wal.crash_and_recover",
                else => return error.WalOutcomeSequenceHasExtraChoice,
            };
            for (request.enabled) |candidate| {
                if (!std.mem.eql(u8, candidate.name, wanted)) continue;
                self.phase += 1;
                return candidate.id;
            }
            return error.ExpectedWalOutcomeTransitionNotEnabled;
        }
    };

    var partial_sequence = Sequence{ .fault_name = "storage.wal.inject_partial_write" };
    var partial = try vopr.runner.run(Scenario, std.testing.allocator, partial_sequence.source(), .{
        .system = "antfly",
        .seed = 0xA17F_7A11,
        .transition_budget = 3,
    });
    defer partial.deinit();
    try std.testing.expectEqual(@as(u64, 0), partial.summary.?.property_failures);
    try expectEventCount(&partial, "storage.wal.append_rejected", 1);
    var partial_replay = try vopr.replay.exact(Scenario, std.testing.allocator, &partial);
    partial_replay.deinit();

    var dropped_sequence = Sequence{ .fault_name = "storage.wal.inject_dropped_sync" };
    var dropped = try vopr.runner.run(Scenario, std.testing.allocator, dropped_sequence.source(), .{
        .system = "antfly",
        .seed = 0xA17F_DA07,
        .transition_budget = 3,
    });
    defer dropped.deinit();
    try std.testing.expectEqual(@as(u64, 0), dropped.summary.?.property_failures);
    try expectEventCount(&dropped, "storage.wal.append_acknowledged_with_uncertain_durability", 1);
    const recovered_outcomes = countEvents(&dropped, "storage.wal.recovered") +
        countEvents(&dropped, "storage.wal.recovery_rejected_uncertain_state");
    try std.testing.expectEqual(@as(usize, 1), recovered_outcomes);
    var dropped_replay = try vopr.replay.exact(Scenario, std.testing.allocator, &dropped);
    dropped_replay.deinit();
}

fn expectEventCount(artifact: *const vopr.trace.Trace, name: []const u8, expected: usize) !void {
    try std.testing.expectEqual(expected, countEvents(artifact, name));
}

fn countEvents(artifact: *const vopr.trace.Trace, name: []const u8) usize {
    var count: usize = 0;
    for (artifact.events.items) |event| count += @intFromBool(std.mem.eql(u8, event.name, name));
    return count;
}
