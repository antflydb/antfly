// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Harness adapter for production replication lifecycle boundaries. The
//! replication kernel exposes a production-neutral hook; VOPR assigns stable
//! semantic identities and owns suspension policy here.

const std = @import("std");
const vopr = @import("vopr");
const replication = @import("../metadata/replication_backfill.zig");
const foreign = @import("../foreign/source.zig");
const table_writes = @import("../api/table_write_source.zig");
const db_types = @import("../storage/db/types.zig");
const table_manager = @import("../metadata/table_manager.zig");

pub const Hook = struct {
    vopr_io: *vopr.vopr_io.VoprIo,

    pub fn lifecycle(self: *Hook) replication.ReplicationLifecycleHook {
        return .{ .ptr = self, .reach_fn = reach };
    }

    pub fn stableId(event: replication.ReplicationLifecycleEvent) vopr.id.StableId {
        var result = vopr.id.stable("replication-lifecycle.phase", @tagName(event.phase));
        result = vopr.id.derive("replication-lifecycle.table", result, event.table_id);
        result = vopr.id.derive("replication-lifecycle.source", result, event.source_ordinal);
        result = vopr.id.derive("replication-lifecycle.offset", result, event.snapshot_offset);
        result = vopr.id.derive("replication-lifecycle.authority", result, event.authority_id);
        return vopr.id.derive(
            "replication-lifecycle.checkpoint",
            result,
            vopr.id.digest(event.checkpoint),
        );
    }

    fn reach(ptr: *anyopaque, event: replication.ReplicationLifecycleEvent) !void {
        const self: *Hook = @ptrCast(@alignCast(ptr));
        try self.vopr_io.safepoint(stableId(event));
    }
};

test "replication lifecycle adapter records stable VoprIo safepoints" {
    var vopr_io = try vopr.vopr_io.VoprIo.init(.{
        .instrumentation = .{ .enabled = false, .map_digest = 0x52504c },
    });
    defer vopr_io.deinit();
    var adapter = Hook{ .vopr_io = &vopr_io };
    const hook = adapter.lifecycle();

    const applied = replication.ReplicationLifecycleEvent{
        .phase = .snapshot_batch_applied,
        .table_id = 11,
        .source_ordinal = 2,
        .snapshot_offset = 64,
        .authority_id = 9,
        .checkpoint = "snapshot:64",
    };
    const persisted = replication.ReplicationLifecycleEvent{
        .phase = .snapshot_checkpoint_persisted,
        .table_id = 11,
        .source_ordinal = 2,
        .snapshot_offset = 64,
        .authority_id = 9,
        .checkpoint = "snapshot:64",
    };
    try hook.reach(applied);
    try hook.reach(persisted);

    try std.testing.expectEqual(@as(u64, 1), vopr_io.instrumentation.count(Hook.stableId(applied)));
    try std.testing.expectEqual(@as(u64, 1), vopr_io.instrumentation.count(Hook.stableId(persisted)));
    try std.testing.expect(Hook.stableId(applied) != Hook.stableId(persisted));
    try std.testing.expect(Hook.stableId(applied) != Hook.stableId(.{
        .phase = .snapshot_batch_applied,
        .table_id = 11,
        .source_ordinal = 2,
        .snapshot_offset = 65,
        .authority_id = 9,
        .checkpoint = "snapshot:65",
    }));
    try vopr_io.ensureNoCapabilityViolation();
}

/// A production-runner campaign rather than a second replication model. Each
/// history selects one immediate fault class, executes the real snapshot and
/// streaming runners, then restarts from the durable status record.
pub const Scenario = struct {
    pub const name: []const u8 = "replication-backfill-production-recovery";
    pub const version: u32 = 1;

    const safe_checkpoint_id = vopr.id.stable(name, "checkpoint-never-ahead-of-apply");
    const no_loss_id = vopr.id.stable(name, "snapshot-stream-no-loss");
    const stale_rejected_id = vopr.id.stable(name, "stale-owner-rejected");
    const duplicate_safe_id = vopr.id.stable(name, "duplicate-apply-is-idempotent");
    const recovery_id = vopr.id.stable(name, "faulted-attempt-recovers");
    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = safe_checkpoint_id, .name = name ++ ".checkpoint-never-ahead-of-apply", .kind = .always },
        .{ .id = no_loss_id, .name = name ++ ".snapshot-stream-no-loss", .kind = .always },
        .{ .id = stale_rejected_id, .name = name ++ ".stale-owner-rejected", .kind = .always },
        .{ .id = duplicate_safe_id, .name = name ++ ".duplicate-apply-is-idempotent", .kind = .always },
        .{ .id = recovery_id, .name = name ++ ".faulted-attempt-recovers", .kind = .reachable },
    };

    const Mode = enum {
        clean,
        source_crash,
        target_crash,
        cancellation,
        stale_owner,
        topology_change,
        schema_change,
        stream_crash_after_apply,
    };
    const mode_ids = [_]vopr.id.StableId{
        vopr.id.stable(name, "clean"),
        vopr.id.stable(name, "source-crash"),
        vopr.id.stable(name, "target-crash"),
        vopr.id.stable(name, "cancellation"),
        vopr.id.stable(name, "stale-owner"),
        vopr.id.stable(name, "topology-change"),
        vopr.id.stable(name, "schema-change"),
        vopr.id.stable(name, "stream-crash-after-apply"),
    };

    const State = struct {
        allocator: std.mem.Allocator,
        sim: vopr.vopr_io.VoprIo,
        registry: foreign.Registry = .{},
        records: std.ArrayListUnmanaged(table_manager.ReplicationSourceStatusRecord) = .empty,
        mode: Mode = .clean,
        complete: bool = false,
        lease_valid: bool = true,
        fault_armed: bool = true,
        first_attempt_failed: bool = false,
        stale_rejected: bool = false,
        target_rebalanced: bool = false,
        schema_changed: bool = false,
        applied_mask: u8 = 0,
        apply_calls: u32 = 0,
        lifecycle_calls: u32 = 0,
        max_snapshot_checkpoint: u64 = 0,

        const config_v1 = "[{\"type\":\"postgres\",\"dsn\":\"postgres://vopr\",\"postgres_table\":\"users_v1\",\"key_template\":\"id\"}]";
        const config_v2 = "[{\"type\":\"postgres\",\"dsn\":\"postgres://vopr\",\"postgres_table\":\"users_v2\",\"key_template\":\"id\"}]";

        fn deinit(self: *@This()) void {
            for (self.records.items) |record| table_manager.freeReplicationSourceStatus(self.allocator, record);
            self.records.deinit(self.allocator);
            self.registry.deinit(self.allocator);
            self.sim.deinit();
        }

        fn table(self: *@This()) table_manager.TableRecord {
            return .{
                .table_id = 41,
                .name = if (self.target_rebalanced) "docs_rebalanced" else "docs",
                .replication_sources_json = if (self.schema_changed) config_v2 else config_v1,
            };
        }

        fn latest(self: *@This()) ?table_manager.ReplicationSourceStatusRecord {
            if (self.records.items.len == 0) return null;
            return self.records.items[self.records.items.len - 1];
        }

        fn upsertReplicationSourceStatus(self: *@This(), record: table_manager.ReplicationSourceStatusRecord) !void {
            try self.records.append(self.allocator, try table_manager.cloneReplicationSourceStatus(self.allocator, record));
            self.max_snapshot_checkpoint = @max(self.max_snapshot_checkpoint, record.snapshot_offset);
        }

        fn checkpoint(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!self.lease_valid) return error.CdcWorkLeaseLost;
        }

        fn deadline(_: *anyopaque) !?u64 {
            return null;
        }

        fn permit(self: *@This()) replication.WorkPermit {
            return .{ .ptr = self, .checkpoint_fn = checkpoint, .deadline_fn = deadline };
        }

        fn lifecycle(self: *@This()) replication.ReplicationLifecycleHook {
            return .{ .ptr = self, .reach_fn = reach };
        }

        fn reach(ptr: *anyopaque, event: replication.ReplicationLifecycleEvent) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.lifecycle_calls += 1;
            try self.sim.safepoint(Hook.stableId(event));
            if (!self.fault_armed) return;
            switch (self.mode) {
                .source_crash => if (event.phase == .provider_prepared) {
                    self.fault_armed = false;
                    return error.ConnectionResetByPeer;
                },
                .cancellation => if (event.phase == .provider_prepared) {
                    self.fault_armed = false;
                    self.lease_valid = false;
                },
                .stale_owner => if (event.phase == .snapshot_batch_applied) {
                    self.fault_armed = false;
                    self.lease_valid = false;
                },
                .topology_change => if (event.phase == .snapshot_batch_applied) {
                    self.fault_armed = false;
                    self.target_rebalanced = true;
                    return error.ConnectionResetByPeer;
                },
                .schema_change => if (event.phase == .snapshot_batch_applied) {
                    self.fault_armed = false;
                    self.schema_changed = true;
                    return error.ConnectionResetByPeer;
                },
                .stream_crash_after_apply => if (event.phase == .stream_change_applied) {
                    self.fault_armed = false;
                    return error.ConnectionResetByPeer;
                },
                else => {},
            }
        }

        fn sourceFactory(ptr: *anyopaque, allocator: std.mem.Allocator, config: foreign.Config) !foreign.Source {
            allocator.free(config.dsn);
            return .{ .ptr = ptr, .vtable = &.{
                .deinit = sourceDeinit,
                .query = sourceQuery,
                .statistics = sourceStatistics,
                .prepare_replication = prepareReplication,
                .poll_changes = pollChanges,
            } };
        }

        fn sourceDeinit(_: *anyopaque, _: std.mem.Allocator) void {}

        fn sourceStatistics(_: *anyopaque, _: []const u8) !foreign.TableStatistics {
            return .{ .row_count = 2, .size_bytes = 64 };
        }

        fn sourceQuery(_: *anyopaque, allocator: std.mem.Allocator, params: foreign.QueryParams) !foreign.QueryResult {
            const rows_json = [_][]const u8{
                "{\"id\":\"doc:1\",\"name\":\"alpha\"}",
                "{\"id\":\"doc:2\",\"name\":\"beta\"}",
            };
            if (params.offset >= rows_json.len) return .{ .total = rows_json.len };
            const count = @min(params.limit orelse rows_json.len, rows_json.len - params.offset);
            const rows = try allocator.alloc(std.json.Value, count);
            errdefer allocator.free(rows);
            for (rows_json[params.offset..][0..count], 0..) |json, index| {
                rows[index] = try std.json.parseFromSliceLeaky(std.json.Value, allocator, json, .{});
            }
            return .{ .rows = rows, .total = rows_json.len };
        }

        fn prepareReplication(_: *anyopaque, allocator: std.mem.Allocator, _: foreign.ReplicationPollParams) !foreign.ReplicationPrepareResult {
            return .{ .checkpoint = try allocator.dupe(u8, "lsn:2") };
        }

        fn pollChanges(_: *anyopaque, allocator: std.mem.Allocator, params: foreign.ReplicationPollParams) !foreign.ReplicationPollResult {
            if (params.checkpoint) |checkpoint_value| {
                if (std.mem.eql(u8, checkpoint_value, "lsn:3"))
                    return .{ .checkpoint = try allocator.dupe(u8, "lsn:3") };
            }
            const changes = try allocator.alloc(foreign.ReplicationChange, 1);
            errdefer allocator.free(changes);
            changes[0] = .{
                .op = .insert,
                .checkpoint = try allocator.dupe(u8, "lsn:3"),
                .row = try std.json.parseFromSliceLeaky(std.json.Value, allocator, "{\"id\":\"doc:3\",\"name\":\"gamma\"}", .{}),
            };
            return .{
                .changes = changes,
                .checkpoint = try allocator.dupe(u8, "lsn:3"),
            };
        }

        fn batch(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8, req: db_types.BatchRequest) !?void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.apply_calls += 1;
            if (self.mode == .target_crash and self.fault_armed) {
                self.fault_armed = false;
                return error.ConnectionResetByPeer;
            }
            for (req.writes) |write| self.markApplied(write.key);
            for (req.transforms) |transform| self.markApplied(transform.key);
            for (req.deletes) |key| self.clearApplied(key);
            return {};
        }

        fn markApplied(self: *@This(), key: []const u8) void {
            if (std.mem.eql(u8, key, "doc:1")) self.applied_mask |= 1;
            if (std.mem.eql(u8, key, "doc:2")) self.applied_mask |= 2;
            if (std.mem.eql(u8, key, "doc:3")) self.applied_mask |= 4;
        }

        fn clearApplied(self: *@This(), key: []const u8) void {
            if (std.mem.eql(u8, key, "doc:1")) self.applied_mask &= ~@as(u8, 1);
            if (std.mem.eql(u8, key, "doc:2")) self.applied_mask &= ~@as(u8, 2);
            if (std.mem.eql(u8, key, "doc:3")) self.applied_mask &= ~@as(u8, 4);
        }

        fn writeSource(self: *@This()) table_writes.TableWriteSource {
            return .{ .ptr = self, .vtable = &.{ .batch = batch } };
        }

        fn run(self: *@This()) !void {
            var snapshot = replication.SnapshotBackfillRunner{
                .alloc = self.allocator,
                .io = self.sim.io(),
                .registry = &self.registry,
                .write_source = self.writeSource(),
                .batch_size = 1,
                .work_permit = self.permit(),
                .lifecycle_hook = self.lifecycle(),
            };
            const first_table = self.table();
            var snapshot_complete = false;
            const first = snapshot.runTableSource(self, first_table, 0);
            if (first) |summary| {
                snapshot_complete = summary.snapshot_complete;
            } else |err| {
                self.first_attempt_failed = true;
                self.stale_rejected = self.mode != .stale_owner or err == error.CdcWorkLeaseLost;
            }
            if (!snapshot_complete) {
                self.lease_valid = true;
                self.fault_armed = self.mode == .stream_crash_after_apply;
                const status = self.latest() orelse return error.MissingReplicationStatus;
                const resumed = try snapshot.runTableSourceFromStatus(
                    self,
                    self.table(),
                    0,
                    @intCast(status.snapshot_offset),
                    status,
                );
                if (!resumed.snapshot_complete) return error.SnapshotDidNotComplete;
            }

            var status = self.latest() orelse return error.MissingReplicationStatus;
            var stream = replication.StreamingReplicationRunner{
                .alloc = self.allocator,
                .io = self.sim.io(),
                .registry = &self.registry,
                .write_source = self.writeSource(),
                .batch_size = 1,
                .work_permit = self.permit(),
                .lifecycle_hook = self.lifecycle(),
            };
            const streamed = stream.runTableSourceFromCheckpoint(
                self,
                self.table(),
                0,
                @intCast(status.snapshot_offset),
                status.cutover_mode,
                status.prepared_checkpoint,
                status,
            );
            if (streamed) |_| {} else |_| {
                self.first_attempt_failed = true;
                self.lease_valid = true;
                self.fault_armed = false;
                status = self.latest() orelse return error.MissingReplicationStatus;
                _ = try stream.runTableSourceFromCheckpoint(
                    self,
                    self.table(),
                    0,
                    @intCast(status.snapshot_offset),
                    status.cutover_mode,
                    if (status.stream_checkpoint.len > 0) status.stream_checkpoint else status.prepared_checkpoint,
                    status,
                );
            }
            self.complete = true;
        }
    };

    pub const World = struct { state: *State };

    pub fn init(allocator: std.mem.Allocator) !World {
        const state = try allocator.create(State);
        errdefer allocator.destroy(state);
        state.* = .{
            .allocator = allocator,
            .sim = try vopr.vopr_io.VoprIo.init(.{
                .seed = 0x52504c,
                .instrumentation = .{ .enabled = false, .map_digest = 0x52504c },
            }),
        };
        errdefer state.sim.deinit();
        try state.registry.registerWithContext(allocator, .postgres, state, State.sourceFactory, null);
        return .{ .state = state };
    }

    pub fn deinit(world: *World, allocator: std.mem.Allocator) void {
        world.state.deinit();
        allocator.destroy(world.state);
        world.* = undefined;
    }

    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: std.mem.Allocator) !void {
        if (world.state.complete) return;
        inline for (std.meta.tags(Mode), mode_ids) |mode, id| {
            try list.append(allocator, .{ .id = id, .name = name ++ "." ++ @tagName(mode), .kind = if (mode == .clean) .workload else .fault });
        }
    }

    pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, allocator: std.mem.Allocator) !vopr.outcome.TransitionOutcome {
        var found = false;
        inline for (std.meta.tags(Mode), mode_ids) |mode, id| {
            if (selected.id == id) {
                world.state.mode = mode;
                found = true;
            }
        }
        if (!found) return error.InvalidReplicationVoprTransition;
        try world.state.run();
        try events.emitNamed(allocator, .domain, selected.name, world.state.applied_mask);
        return .applied();
    }

    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
        try builder.addNamed(allocator, name ++ ".complete", @intFromBool(world.state.complete));
        try builder.addNamed(allocator, name ++ ".applied-mask", world.state.applied_mask);
        try builder.addNamed(allocator, name ++ ".apply-calls", world.state.apply_calls);
        try builder.addNamed(allocator, name ++ ".lifecycle-calls", world.state.lifecycle_calls);
        try builder.addNamed(allocator, name ++ ".checkpoint", world.state.max_snapshot_checkpoint);
    }

    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
        const state = world.state;
        try sink.check(allocator, safe_checkpoint_id, state.max_snapshot_checkpoint <= @popCount(state.applied_mask & 3));
        try sink.check(allocator, no_loss_id, !state.complete or state.applied_mask == 7);
        try sink.check(allocator, stale_rejected_id, state.mode != .stale_owner or state.stale_rejected);
        const duplicate_mode = state.mode == .stale_owner or
            state.mode == .topology_change or
            state.mode == .schema_change or
            state.mode == .stream_crash_after_apply;
        try sink.check(
            allocator,
            duplicate_safe_id,
            !state.complete or !duplicate_mode or (state.apply_calls > 3 and state.applied_mask == 7),
        );
        try sink.check(allocator, recovery_id, state.complete and (state.mode == .clean or state.first_attempt_failed));
    }

    pub fn done(world: *World) bool {
        return world.state.complete;
    }
};

test "replication backfill VOPR exact replays every production recovery mode" {
    const backend_ids = vopr.vopr_io.artifactBackendIds();
    for (Scenario.mode_ids) |mode_id| {
        var scripted = vopr.choice.Scripted{ .selections = &.{mode_id} };
        var recorded = try vopr.runner.run(Scenario, std.testing.allocator, scripted.source(), .{
            .system = "antfly",
            .transition_budget = 1,
            .backend_ids = &backend_ids,
            .source_revision = "replication-backfill-vopr-v1",
            .target = "native",
            .optimize = @tagName(@import("builtin").mode),
        });
        defer recorded.deinit();
        try std.testing.expectEqual(@as(u64, 0), recorded.summary.property_failures);
        for (0..20) |_| {
            var replayed = try vopr.replay.exact(Scenario, std.testing.allocator, &recorded);
            replayed.deinit();
        }
    }
}
