// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Interleave restore admission and publication for three shards on three
//! replicas using the production generation locks and import-proof validator.
const std = @import("std");
const vopr = @import("vopr");
const lifecycle = @import("../storage/db/generation_lifecycle.zig");
const admission = @import("../storage/restore_admission.zig");
const Identity = @import("../storage/restore_identity.zig").Identity;

pub const Scenario = struct {
    pub const name: []const u8 = "restore-admission";
    pub const version: u32 = 1;
    const sound_id = vopr.id.stable(name, "exact-import-before-owner");
    const complete_id = vopr.id.stable(name, "all-replicas-admitted");
    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = sound_id, .name = name ++ ".exact-import-before-owner", .kind = .always },
        .{ .id = complete_id, .name = name ++ ".all-replicas-admitted", .kind = .reachable },
    };
    const digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const identity: Identity = .{ .backup_id = "backup", .location = "file:///backup", .snapshot_path = "groups/1.afb", .artifact_sha256 = digest };
    const Phase = enum { absent, stale, imported, pinned, released, complete };
    const State = struct {
        io: vopr.vopr_io.VoprIo,
        phases: [9]Phase = @splat(.absent),
        leases: [9]?lifecycle.ReadLease = @splat(null),
        progress: u64 = 0,
        sound: bool = true,
    };
    pub const World = struct { state: *State };

    pub fn init(alloc: std.mem.Allocator) !World {
        const state = try alloc.create(State);
        errdefer alloc.destroy(state);
        state.* = .{ .io = try vopr.vopr_io.VoprIo.init(.{
            .seed = 0x3570_2e,
            .required = .of(&.{ .files, .task_scheduling, .synchronization, .deterministic_entropy, .clock_read }),
        }) };
        return .{ .state = state };
    }
    pub fn deinit(world: *World, alloc: std.mem.Allocator) void {
        for (&world.state.leases) |*lease| if (lease.*) |*value| value.deinit();
        world.state.io.deinit();
        alloc.destroy(world.state);
        world.* = undefined;
    }
    fn stepName(comptime replica: usize, comptime phase: Phase) []const u8 {
        return std.fmt.comptimePrint("restore-admission.replica-{d}.{s}", .{ replica, @tagName(phase) });
    }
    pub fn enumerate(world: *World, list: *vopr.transition.List, alloc: std.mem.Allocator) !void {
        inline for (0..9) |replica| {
            inline for (comptime std.meta.tags(Phase)) |phase| {
                if (phase != .complete and world.state.phases[replica] == phase) {
                    const step = comptime stepName(replica, phase);
                    try list.append(alloc, .{ .id = vopr.id.stable(name, step), .name = step, .kind = .workload });
                }
            }
        }
    }
    fn publish(alloc: std.mem.Allocator, io: std.Io, path: []const u8, group: u64, replica: usize, exact: bool) !void {
        var transition = try lifecycle.beginProcessExclusiveWithIo(path, io);
        defer transition.deinit();
        var staged = try transition.beginStaging();
        defer staged.deinit();
        // Every stale marker is well formed but proves a different import (or
        // an import whose primary bytes have not been published yet).
        const marker = try std.json.Stringify.valueAlloc(alloc, .{
            .format_version = @as(u32, 1),
            .backup_id = if (!exact and replica == 0) "old-backup" else identity.backup_id,
            .location = if (!exact and replica == 1) "file:///other" else identity.location,
            .snapshot_path = if (!exact and replica == 2) "groups/other.afb" else identity.snapshot_path,
            .artifact_sha256 = if (!exact and replica == 3) "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff" else digest,
            .native_manifest_size_bytes = @as(u64, if (!exact and (replica == 4 or replica == 5)) 123 else 0),
            .native_manifest_sha256 = if (!exact and (replica == 4 or replica == 5)) digest else "",
            .group_id = if (!exact and replica == 6) group + 10 else group,
            .primary_restored = exact or (replica != 7 and replica != 8),
            .runtime_repair_complete = false,
            .phase = "rebuild_graph",
            .last_error = "",
        }, .{});
        defer alloc.free(marker);
        const marker_path = try std.fs.path.join(alloc, &.{ staged.path(), ".restore-state" });
        defer alloc.free(marker_path);
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = marker_path, .data = marker });
        if (try staged.publish() != .durable) return error.TestUnexpectedResult;
    }
    fn run(world: *World, alloc: std.mem.Allocator, replica: usize) !void {
        const state = world.state;
        const io = state.io.io();
        const group: u64 = replica % 3 + 1;
        const path = try std.fmt.allocPrint(alloc, "/restore/node-{d}/group-{d}/table-db", .{ replica / 3, group });
        defer alloc.free(path);
        const stale_path = try std.fmt.allocPrint(alloc, "{s}-other-import", .{path});
        defer alloc.free(stale_path);
        switch (state.phases[replica]) {
            .absent, .stale => {
                const probe_path = if (state.phases[replica] == .stale) stale_path else path;
                if (admission.acquire(alloc, io, probe_path, group, identity)) |value| {
                    var lease = value;
                    lease.deinit();
                    state.sound = false;
                } else |err| if (err != error.StorageReadTemporarilyUnavailable) return err;
                // A rejected proof must release its lease. Use a separate
                // fixture root for each import identity; production atomic
                // replacement is covered by generation/native restore tests.
                var transition = try lifecycle.beginProcessExclusiveWithIo(probe_path, io);
                transition.deinit();
                const exact = state.phases[replica] == .stale;
                try publish(alloc, io, if (exact) path else stale_path, group, replica, exact);
            },
            .imported => state.leases[replica] = try admission.acquire(alloc, io, path, group, identity),
            .pinned => {
                if (lifecycle.beginProcessExclusiveWithIo(path, io)) |value| {
                    var transition = value;
                    transition.deinit();
                    state.sound = false;
                } else |err| if (err != error.GenerationTransitionActive) return err;
                state.leases[replica].?.deinit();
                state.leases[replica] = null;
            },
            .released => {
                var transition = try lifecycle.beginProcessExclusiveWithIo(path, io);
                transition.deinit();
            },
            .complete => return error.InvalidRestoreAdmissionTransition,
        }
        state.phases[replica] = @enumFromInt(@intFromEnum(state.phases[replica]) + 1);
        state.progress += 1;
    }
    pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, alloc: std.mem.Allocator) !vopr.outcome.TransitionOutcome {
        inline for (0..9) |replica| {
            inline for (comptime std.meta.tags(Phase)) |phase| {
                if (phase != .complete and selected.id == vopr.id.stable(name, stepName(replica, phase))) {
                    if (world.state.phases[replica] != phase) return error.InvalidRestoreAdmissionTransition;
                    try run(world, alloc, replica);
                    try events.emitNamed(alloc, .domain, selected.name, world.state.progress);
                    return .applied();
                }
            }
        }
        return error.InvalidRestoreAdmissionTransition;
    }
    pub fn observe(world: *World, builder: *vopr.observation.Builder, alloc: std.mem.Allocator) !void {
        try builder.addNamed(alloc, name ++ ".progress", @intCast(world.state.progress));
        inline for (0..9) |replica| try builder.addNamed(alloc, std.fmt.comptimePrint("replica-{d}", .{replica}), @intFromEnum(world.state.phases[replica]));
    }
    pub fn evaluate(world: *World, sink: *vopr.property.Sink, alloc: std.mem.Allocator) !void {
        try sink.check(alloc, sound_id, world.state.sound);
        try sink.check(alloc, complete_id, done(world));
    }
    pub fn healthSnapshot(world: *World) vopr.health.Snapshot {
        return .{ .progress_expected = true, .progress_units = world.state.progress, .active_tasks = world.state.io.tasks.activeTaskCount(), .cleanup_complete = done(world) };
    }
    pub fn done(world: *World) bool {
        for (world.state.phases) |phase| if (phase != .complete) return false;
        return true;
    }
};

test "restore admission VOPR exact replays three by three publication interleavings" {
    const backend_ids = vopr.vopr_io.artifactBackendIds();
    for (0..256) |ordinal| {
        var choices = vopr.choice.Seeded.init(0x3570_2e + ordinal);
        var artifact = try vopr.runner.run(Scenario, std.testing.allocator, choices.source(), .{
            .system = "antfly",
            .seed = 0x3570_2e + ordinal,
            .transition_budget = 45,
            .backend_ids = &backend_ids,
        });
        defer artifact.deinit();
        try std.testing.expectEqual(@as(u64, 0), artifact.summary.?.property_failures);
        var replayed = try vopr.replay.exact(Scenario, std.testing.allocator, &artifact);
        replayed.deinit();
    }
}
