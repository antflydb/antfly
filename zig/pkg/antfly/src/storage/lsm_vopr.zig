// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Replayable LSM scenario over the real backend, WAL, manifest, compaction,
//! maintenance, and modeled storage implementation.

const std = @import("std");
const builtin = @import("builtin");
const vopr = @import("vopr");
const backend_types = @import("backend_types.zig");
const mem_backend_mod = @import("mem_backend.zig");
const lsm_backend_mod = @import("lsm_backend/mod.zig");
const storage_sim = @import("sim_runtime.zig");

const namespaces = [_]backend_types.Namespace{
    .{},
    .{ .name = "docs" },
    .{ .name = "meta" },
};

const keys = [_][]const u8{
    "a",
    "b",
    "c",
    "doc:a",
    "doc:b",
    "doc:c",
    "meta:lsn",
    "meta:epoch",
};

const root_dir = "/vopr/lsm-backend";

const finish_id = vopr.id.stable("transition", "storage.lsm.finish");
const put_base = vopr.id.stable("transition", "storage.lsm.put");
const delete_base = vopr.id.stable("transition", "storage.lsm.delete");
const read_verify_id = vopr.id.stable("transition", "storage.lsm.read_verify");
const sync_id = vopr.id.stable("transition", "storage.lsm.sync_compact");
const maintenance_id = vopr.id.stable("transition", "storage.lsm.maintenance_step");
const crash_id = vopr.id.stable("transition", "storage.lsm.crash_reopen");
const fault_probe_id = vopr.id.stable("transition", "storage.lsm.consume_storage_fault");

const visible_model_id = vopr.id.stable("property", "storage.lsm.visible_state_matches_memory_model");
const acknowledged_durable_id = vopr.id.stable("property", "storage.lsm.acknowledged_state_survives_crash");
const maintenance_safe_id = vopr.id.stable("property", "storage.lsm.maintenance_preserves_visible_state");
const complete_id = vopr.id.stable("property", "storage.lsm.campaign_completed");

const PendingFault = enum(u8) { write, sync, device_full };

pub fn Scenario(comptime action_budget: u64) type {
    return struct {
        pub const name: []const u8 = "lsm-backend";
        pub const version: u32 = 1;
        pub const properties = &[_]vopr.property.Declaration{
            .{ .id = visible_model_id, .name = "storage.lsm.visible_state_matches_memory_model", .kind = .always },
            .{ .id = acknowledged_durable_id, .name = "storage.lsm.acknowledged_state_survives_crash", .kind = .always },
            .{ .id = maintenance_safe_id, .name = "storage.lsm.maintenance_preserves_visible_state", .kind = .always },
            .{ .id = complete_id, .name = "storage.lsm.campaign_completed", .kind = .reachable },
        };

        const State = struct {
            allocator: std.mem.Allocator,
            mem: mem_backend_mod.Backend,
            device: storage_sim.ModeledDevice,
            options: lsm_backend_mod.Options,
            lsm: lsm_backend_mod.Backend,
            faults: vopr.fault.Controller,
            pending_fault: ?PendingFault = null,
            actions: u64 = 0,
            writes: u64 = 0,
            deletes: u64 = 0,
            crashes: u64 = 0,
            maintenance_steps: u64 = 0,
            rejected_fault_probes: u64 = 0,
            visible_matches: bool = true,
            crash_durable: bool = true,
            maintenance_safe: bool = true,
            finished: bool = false,

            fn deinit(self: *State) void {
                self.faults.deinit();
                self.lsm.close();
                self.device.deinit();
                self.mem.close();
            }
        };

        pub const World = struct { state: *State };

        pub fn init(allocator: std.mem.Allocator) !World {
            const state = try allocator.create(State);
            errdefer allocator.destroy(state);
            state.* = undefined;
            state.allocator = allocator;
            state.mem = mem_backend_mod.Backend.init(allocator, .{});
            errdefer state.mem.close();
            state.device = storage_sim.ModeledDevice.init(allocator);
            errdefer state.device.deinit();
            state.options = .{
                .flush_threshold = 3,
                .flush_threshold_bytes = 256,
                .compact_threshold_runs = 2,
                .level_target_runs_base = 2,
                .foreground_soft_compaction = true,
                .obsolete_retention_ns = 0,
                .obsolete_delete_retry_ns = 0,
                .wal_sync_on_commit = true,
                .storage = state.device.storage(),
            };
            state.lsm = try lsm_backend_mod.Backend.open(allocator, root_dir, state.options);
            errdefer state.lsm.close();
            state.faults = try vopr.fault.Controller.init(allocator, 1, .{
                .max_simultaneous_node_failures = 1,
                .max_storage_faults_per_durability_epoch = 1,
                .minimum_healthy_nodes = 0,
                .allow_quorum_loss = true,
            });
            state.pending_fault = null;
            state.actions = 0;
            state.writes = 0;
            state.deletes = 0;
            state.crashes = 0;
            state.maintenance_steps = 0;
            state.rejected_fault_probes = 0;
            state.visible_matches = true;
            state.crash_durable = true;
            state.maintenance_safe = true;
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
            if (state.pending_fault != null) {
                try list.append(allocator, .{
                    .id = fault_probe_id,
                    .name = "storage.lsm.consume_storage_fault",
                    .kind = .scheduler,
                    .resource_id = storageResource(state.pending_fault.?),
                    .parameter = @intFromEnum(state.pending_fault.?),
                });
                return;
            }
            if (state.actions >= action_budget) {
                try list.append(allocator, .{ .id = finish_id, .name = "storage.lsm.finish", .kind = .quiescence });
                return;
            }

            for (namespaces, 0..) |_, namespace_index| {
                for (keys, 0..) |_, key_index| {
                    const parameter = packLocation(namespace_index, key_index);
                    try list.append(allocator, .{
                        .id = vopr.id.derive("storage.lsm.put", put_base, parameter),
                        .name = "storage.lsm.put",
                        .kind = .workload,
                        .parameter = @bitCast(parameter),
                    });
                    try list.append(allocator, .{
                        .id = vopr.id.derive("storage.lsm.delete", delete_base, parameter),
                        .name = "storage.lsm.delete",
                        .kind = .workload,
                        .parameter = @bitCast(parameter),
                    });
                }
            }
            try list.append(allocator, .{ .id = read_verify_id, .name = "storage.lsm.read_verify", .kind = .workload });
            try list.append(allocator, .{ .id = sync_id, .name = "storage.lsm.sync_compact", .kind = .maintenance });
            try list.append(allocator, .{ .id = maintenance_id, .name = "storage.lsm.maintenance_step", .kind = .maintenance });
            try list.append(allocator, .{ .id = crash_id, .name = "storage.lsm.crash_reopen", .kind = .fault });
            for (std.enums.values(PendingFault)) |fault_kind| {
                const spec = storageFaultSpec(fault_kind);
                if ((try state.faults.admission(spec)).isAllowed()) try list.append(allocator, spec.startTransition());
            }
        }

        pub fn execute(
            world: *World,
            selected: vopr.transition.Transition,
            events: *vopr.event.Sink,
            allocator: std.mem.Allocator,
        ) !vopr.outcome.TransitionOutcome {
            const state = world.state;
            if (selected.id == finish_id) {
                state.faults.beginQuietSuffix();
                try state.lsm.sync(true);
                try crashReopen(state);
                state.crash_durable = state.crash_durable and try modelMatches(state);
                state.visible_matches = state.visible_matches and state.crash_durable;
                state.finished = true;
                try events.emitNamed(allocator, .domain, "storage.lsm.campaign_complete", state.writes + state.deletes);
                return vopr.outcome.TransitionOutcome.targetReached("storage.lsm.campaign_complete", state.writes + state.deletes);
            }

            if (selected.id == fault_probe_id) {
                const kind = state.pending_fault orelse return error.LsmFaultProbeWithoutFault;
                const spec = storageFaultSpec(kind);
                const rejected = switch (kind) {
                    .write => try consumeWriteFault(state),
                    .sync => try consumeSyncFault(state),
                    .device_full => try consumeDeviceFullFault(state),
                };
                if (!rejected) return error.LsmStorageFaultWasNotConsumed;
                state.rejected_fault_probes += 1;
                state.pending_fault = null;
                _ = try state.faults.consumeOneShot(.storage, spec.resource_id, events, allocator);
                state.visible_matches = state.visible_matches and try modelMatches(state);
                try events.emitNamed(allocator, .injected_error, "storage.lsm.fault_consumed", @intFromEnum(kind));
                return vopr.outcome.TransitionOutcome.injectedError("storage.lsm.expected_storage_error", @intFromEnum(kind));
            }

            for (std.enums.values(PendingFault)) |fault_kind| {
                const spec = storageFaultSpec(fault_kind);
                if (selected.id != spec.startTransition().id) continue;
                try state.faults.start(spec, events, allocator);
                switch (fault_kind) {
                    .write => state.device.injectWriteFailure(),
                    .sync => state.device.injectSyncFailure(),
                    .device_full => state.device.setCapacityBytes(state.device.usedBytes()),
                }
                state.pending_fault = fault_kind;
                state.actions += 1;
                return vopr.outcome.TransitionOutcome.applied();
            }

            state.actions += 1;
            if (std.mem.eql(u8, selected.name, "storage.lsm.put")) {
                const location = unpackLocation(@bitCast(selected.parameter));
                var value_buf: [64]u8 = undefined;
                const value = try std.fmt.bufPrint(&value_buf, "vopr-{d}-{d}-{d}", .{ state.actions, location.namespace, location.key });
                try putBoth(state, namespaces[location.namespace], keys[location.key], value);
                state.writes += 1;
                try events.emitNamed(allocator, .client_response, "storage.lsm.put_acknowledged", selected.id);
            } else if (std.mem.eql(u8, selected.name, "storage.lsm.delete")) {
                const location = unpackLocation(@bitCast(selected.parameter));
                try deleteBoth(state, namespaces[location.namespace], keys[location.key]);
                state.deletes += 1;
                try events.emitNamed(allocator, .client_response, "storage.lsm.delete_acknowledged", selected.id);
            } else if (selected.id == read_verify_id) {
                state.visible_matches = state.visible_matches and try modelMatches(state);
            } else if (selected.id == sync_id) {
                try state.lsm.sync(true);
                const matches = try modelMatches(state);
                state.maintenance_safe = state.maintenance_safe and matches;
                state.visible_matches = state.visible_matches and matches;
                try events.emitNamed(allocator, .state_change, "storage.lsm.sync_completed", state.lsm.compaction_stats.compactions);
            } else if (selected.id == maintenance_id) {
                const ran = try state.lsm.runMaintenanceStep();
                state.maintenance_steps += @intFromBool(ran);
                const matches = try modelMatches(state);
                state.maintenance_safe = state.maintenance_safe and matches;
                state.visible_matches = state.visible_matches and matches;
                try events.emitNamed(allocator, .state_change, "storage.lsm.maintenance_completed", @intFromBool(ran));
            } else if (selected.id == crash_id) {
                const spec = crashSpec();
                try state.faults.start(spec, events, allocator);
                try crashReopen(state);
                _ = try state.faults.consumeOneShot(.node, spec.resource_id, events, allocator);
                state.crashes += 1;
                const matches = try modelMatches(state);
                state.crash_durable = state.crash_durable and matches;
                state.visible_matches = state.visible_matches and matches;
                try events.emitNamed(allocator, .state_change, "storage.lsm.crash_recovered", state.crashes);
            } else {
                return error.UnknownLsmVoprTransition;
            }
            state.visible_matches = state.visible_matches and try modelMatches(state);
            return vopr.outcome.TransitionOutcome.applied();
        }

        pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
            const state = world.state;
            const maintenance = state.lsm.snapshotMaintenanceStats();
            const writes = state.lsm.snapshotWriteStats();
            try builder.addNamed(allocator, "storage.lsm.actions", @intCast(state.actions));
            try builder.addNamed(allocator, "storage.lsm.writes", @intCast(state.writes));
            try builder.addNamed(allocator, "storage.lsm.deletes", @intCast(state.deletes));
            try builder.addNamed(allocator, "storage.lsm.crashes", @intCast(state.crashes));
            try builder.addNamed(allocator, "storage.lsm.runs", @intCast(state.lsm.runs.items.len));
            try builder.addNamed(allocator, "storage.lsm.immutable_memtables", @intCast(state.lsm.immutable_memtables.items.len));
            try builder.addNamed(allocator, "storage.lsm.compactions", @intCast(state.lsm.compaction_stats.compactions));
            try builder.addNamed(allocator, "storage.lsm.wal_append_records", @intCast(writes.wal_append_records));
            try builder.addNamed(allocator, "storage.lsm.wal_checkpoint_pending", @intFromBool(maintenance.wal_checkpoint_pending));
            try builder.addNamed(allocator, "storage.lsm.pending_fault", if (state.pending_fault) |fault| @intFromEnum(fault) + 1 else 0);
            try builder.addNamed(allocator, "storage.lsm.rejected_fault_probes", @intCast(state.rejected_fault_probes));
            try builder.addNamed(allocator, "storage.lsm.finished", @intFromBool(state.finished));
        }

        pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
            const state = world.state;
            try sink.check(allocator, visible_model_id, state.visible_matches);
            try sink.check(allocator, acknowledged_durable_id, state.crash_durable);
            try sink.check(allocator, maintenance_safe_id, state.maintenance_safe);
            try sink.check(allocator, complete_id, state.finished);
        }

        pub fn done(world: *World) bool {
            return world.state.finished;
        }
    };
}

pub const CliScenario = Scenario(24);

pub fn record(allocator: std.mem.Allocator, seed: u64) !vopr.trace.Trace {
    var seeded = vopr.choice.Seeded.init(seed);
    return vopr.runner.run(CliScenario, allocator, seeded.source(), .{
        .system = "antfly",
        .seed = seed,
        .transition_budget = 49,
        .source_revision = "lsm-vopr-cli",
        .target = "native",
        .optimize = @tagName(builtin.mode),
    });
}

pub fn replay(allocator: std.mem.Allocator, artifact: *const vopr.trace.Trace) !vopr.trace.Trace {
    return vopr.replay.exact(CliScenario, allocator, artifact);
}

fn putBoth(state: anytype, namespace: backend_types.Namespace, key: []const u8, value: []const u8) !void {
    var lsm_txn = try state.lsm.beginWrite();
    defer lsm_txn.abort();
    try lsm_txn.put(namespace, key, value);
    try lsm_txn.commit();

    var mem_txn = try state.mem.beginWrite();
    defer mem_txn.abort();
    try mem_txn.put(namespace, key, value);
    try mem_txn.commit();
}

fn deleteBoth(state: anytype, namespace: backend_types.Namespace, key: []const u8) !void {
    var lsm_txn = try state.lsm.beginWrite();
    defer lsm_txn.abort();
    try lsm_txn.delete(namespace, key);
    try lsm_txn.commit();

    var mem_txn = try state.mem.beginWrite();
    defer mem_txn.abort();
    try mem_txn.delete(namespace, key);
    try mem_txn.commit();
}

fn modelMatches(state: anytype) !bool {
    for (namespaces) |namespace| {
        var mem_store = try state.mem.runtimeStore(state.allocator, namespace);
        defer mem_store.deinit();
        var lsm_store = try state.lsm.runtimeStore(state.allocator, namespace);
        defer lsm_store.deinit();
        var mem_read = try mem_store.beginRead();
        defer mem_read.abort();
        var lsm_read = try lsm_store.beginRead();
        defer lsm_read.abort();
        for (keys) |key| {
            const mem_value = mem_read.get(key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            const lsm_value = lsm_read.get(key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            if (mem_value == null or lsm_value == null) {
                if ((mem_value == null) != (lsm_value == null)) return false;
            } else if (!std.mem.eql(u8, mem_value.?, lsm_value.?)) return false;
        }
    }
    return true;
}

fn crashReopen(state: anytype) !void {
    try state.device.device().crash();
    state.lsm.abandonAfterCrash();
    state.lsm = try lsm_backend_mod.Backend.open(state.allocator, root_dir, state.options);
}

fn consumeWriteFault(state: anytype) !bool {
    var txn = try state.lsm.beginWrite();
    defer txn.abort();
    try txn.put(.{}, "fault:write", "must-not-commit");
    txn.commit() catch |err| return err == error.InjectedWriteFault;
    return false;
}

fn consumeSyncFault(state: anytype) !bool {
    state.lsm.sync(true) catch |err| return err == error.InjectedSyncFault;
    return false;
}

fn consumeDeviceFullFault(state: anytype) !bool {
    defer state.device.setCapacityBytes(null);
    var txn = try state.lsm.beginWrite();
    defer txn.abort();
    try txn.put(.{}, "fault:full", "must-not-commit");
    txn.commit() catch |err| return err == error.InjectedDeviceFull;
    return false;
}

fn storageFaultSpec(kind: PendingFault) vopr.fault.Spec {
    return .{
        .id = vopr.id.derive("storage.lsm.fault", vopr.id.stable("fault", "storage.lsm.storage"), @intFromEnum(kind)),
        .name = "storage.lsm.storage_fault",
        .kind = .storage,
        .lifecycle = .one_shot,
        .resource_id = storageResource(kind),
    };
}

fn storageResource(kind: PendingFault) u64 {
    return vopr.id.derive("storage.lsm.resource", vopr.id.stable("resource", "storage.lsm.device"), @intFromEnum(kind));
}

fn crashSpec() vopr.fault.Spec {
    return .{
        .id = vopr.id.stable("fault", "storage.lsm.crash"),
        .name = "storage.lsm.crash_reopen",
        .kind = .node,
        .lifecycle = .one_shot,
        .resource_id = vopr.id.stable("resource", "storage.lsm.process"),
    };
}

const Location = struct { namespace: usize, key: usize };

fn packLocation(namespace: usize, key: usize) u64 {
    return (@as(u64, @intCast(namespace)) << 32) | @as(u64, @intCast(key));
}

fn unpackLocation(value: u64) Location {
    return .{ .namespace = @intCast(value >> 32), .key = @intCast(value & std.math.maxInt(u32)) };
}

fn runRecordReplay(comptime budget: u64, seed: u64) !void {
    const LsmScenario = Scenario(budget);
    var seeded = vopr.choice.Seeded.init(seed);
    var artifact = try vopr.runner.run(LsmScenario, std.testing.allocator, seeded.source(), .{
        .system = "antfly",
        .seed = seed,
        .transition_budget = budget * 2 + 1,
    });
    defer artifact.deinit();
    try std.testing.expectEqual(@as(u64, 0), artifact.summary.?.property_failures);
    var replayed = try vopr.replay.exact(LsmScenario, std.testing.allocator, &artifact);
    replayed.deinit();
}

test "LSM VOPR replays workload compaction maintenance crash and modeled faults" {
    try runRecordReplay(32, 0xA17F_1501);
    try runRecordReplay(32, 0xA17F_1502);
}

test "LSM VOPR exact replay preserves acknowledged state across repeated crash recovery" {
    const LsmScenario = Scenario(20);
    var seeded = vopr.choice.Seeded.init(0xA17F_1503);
    var artifact = try vopr.runner.run(LsmScenario, std.testing.allocator, seeded.source(), .{
        .system = "antfly",
        .seed = 0xA17F_1503,
        .transition_budget = 41,
    });
    defer artifact.deinit();
    for (0..5) |_| {
        var replayed = try vopr.replay.exact(LsmScenario, std.testing.allocator, &artifact);
        replayed.deinit();
    }
}

test "LSM VOPR consumes write sync and device-full faults as replayable outcomes" {
    const LsmScenario = Scenario(2);
    const Sequence = struct {
        fault: PendingFault,
        step: usize = 0,

        fn source(self: *@This()) vopr.choice.Source {
            return .{ .ptr = self, .choose_fn = choose, .finish_fn = finish };
        }

        fn choose(ptr: *anyopaque, request: vopr.choice.Request) !u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const wanted = switch (self.step) {
                0 => vopr.id.derive("storage.lsm.put", put_base, packLocation(0, 0)),
                1 => storageFaultSpec(self.fault).startTransition().id,
                2 => fault_probe_id,
                3 => finish_id,
                else => return error.LsmFaultSequenceHasExtraChoice,
            };
            for (request.enabled) |candidate| if (candidate.id == wanted) {
                self.step += 1;
                return wanted;
            };
            return error.ExpectedLsmFaultTransitionNotEnabled;
        }

        fn finish(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.step != 4) return error.LsmFaultSequenceIncomplete;
        }
    };

    for (std.enums.values(PendingFault)) |fault| {
        var sequence = Sequence{ .fault = fault };
        var artifact = try vopr.runner.run(LsmScenario, std.testing.allocator, sequence.source(), .{
            .system = "antfly",
            .seed = 0,
            .transition_budget = 4,
        });
        defer artifact.deinit();
        try std.testing.expectEqual(@as(u64, 0), artifact.summary.?.property_failures);
        try std.testing.expectEqual(@as(usize, 2), artifact.faults.items.len);
        var replayed = try vopr.replay.exact(LsmScenario, std.testing.allocator, &artifact);
        replayed.deinit();
    }
}
