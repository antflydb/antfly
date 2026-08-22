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
    lsn: u64,
    data: []u8,
};

const append_id = vopr.id.stable("transition", "storage.wal.append");
const append_batch_base = vopr.id.stable("transition", "storage.wal.append_batch");
const reopen_base = vopr.id.stable("transition", "storage.wal.reopen_and_verify_from");
const truncate_base = vopr.id.stable("transition", "storage.wal.truncate_and_verify_from");
const verify_base = vopr.id.stable("transition", "storage.wal.verify_from");
const crash_id = vopr.id.stable("transition", "storage.wal.crash_and_recover");

const model_property_id = vopr.id.stable("property", "storage.wal.visible_state_matches_acknowledged_model");
const durable_property_id = vopr.id.stable("property", "storage.wal.acknowledged_entries_survive_crash");
const recovered_property_id = vopr.id.stable("property", "storage.wal.modeled_crash_recovery_reached");

fn ModeledWalScenario(comptime action_budget: u64) type {
    return struct {
        const Self = @This();

        pub const name: []const u8 = "modeled-wal";
        pub const version: u32 = 1;
        pub const properties = &[_]vopr.property.Declaration{
            .{ .id = model_property_id, .name = "storage.wal.visible_state_matches_acknowledged_model", .kind = .always },
            .{ .id = durable_property_id, .name = "storage.wal.acknowledged_entries_survive_crash", .kind = .always },
            .{ .id = recovered_property_id, .name = "storage.wal.modeled_crash_recovery_reached", .kind = .reachable },
        };

        const State = struct {
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
            return .{ .state = state };
        }

        pub fn deinit(world: *World, allocator: Allocator) void {
            world.state.deinit();
            allocator.destroy(world.state);
            world.* = undefined;
        }

        pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: Allocator) !void {
            const state = world.state;
            if (state.decisions >= action_budget) {
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
        ) !void {
            const state = world.state;
            if (selected.id == crash_id) {
                try state.device.device().crash();
                state.wal.abandonAfterCrash();
                state.wal_open = false;
                state.wal = try WAL.open("/vopr/modeled-wal", state.opts);
                state.wal_open = true;
                state.recovered = true;
                try events.emitNamed(allocator, .state_change, "storage.wal.recovered", state.next_lsn);
                return;
            }

            state.decisions += 1;
            if (selected.id == append_id) {
                try appendOne(state, events, allocator, 0);
                return;
            }
            if (selected.id == vopr.id.derive("storage.wal.append_batch", append_batch_base, @intCast(selected.parameter))) {
                try appendBatch(state, events, allocator, @intCast(selected.parameter));
                return;
            }
            if (std.mem.eql(u8, selected.name, "storage.wal.reopen_and_verify_from")) {
                try reopen(state);
                state.reopens += 1;
                try events.emitNamed(allocator, .state_change, "storage.wal.reopened", @intCast(selected.parameter));
                return;
            }
            if (std.mem.eql(u8, selected.name, "storage.wal.verify_from")) {
                try events.emitNamed(allocator, .domain, "storage.wal.verified", @intCast(selected.parameter));
                return;
            }
            if (std.mem.eql(u8, selected.name, "storage.wal.truncate_and_verify_from")) {
                const pair = unpackPair(@bitCast(selected.parameter));
                try state.wal.truncate(pair.left);
                truncateModel(state, pair.left);
                try events.emitNamed(allocator, .state_change, "storage.wal.truncated", pair.left);
                return;
            }
            return error.UnknownWalVoprTransition;
        }

        pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: Allocator) !void {
            const state = world.state;
            try builder.addNamed(allocator, "storage.wal.model_entries", @intCast(state.model.items.len));
            try builder.addNamed(allocator, "storage.wal.last_lsn", @intCast(state.wal.lastLsn()));
            try builder.addNamed(allocator, "storage.wal.next_lsn", @intCast(state.next_lsn));
            try builder.addNamed(allocator, "storage.wal.model_digest", @bitCast(modelDigest(state.model.items)));
            try builder.addNamed(allocator, "storage.wal.virtual_time_ns", @intCast(state.runtime.clock().nowNs()));
            try builder.addNamed(allocator, "storage.wal.reopen_count", @intCast(state.reopens));
            try builder.addNamed(allocator, "storage.wal.recovered", @intFromBool(state.recovered));
        }

        pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: Allocator) !void {
            const state = world.state;
            const matches = try stateMatches(state, 1);
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
    const lsn = try state.wal.append(payload);
    if (lsn != state.next_lsn) return error.WalLsnDiverged;
    try state.model.append(state.allocator, .{ .lsn = lsn, .data = payload });
    state.next_lsn += 1;
    try events.emitNamed(allocator, .client_response, "storage.wal.append_acknowledged", lsn);
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
    try std.testing.expectEqual(@as(usize, 1), recorded.faults.items.len);

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
