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
const VoprTestAllocator = std.heap.DebugAllocator(.{ .stack_trace_frames = 0 });

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

    const service_node = vopr.service_rate.Node{
        .id = vopr.id.stable(name, "service-node.source-1"),
        .name = name ++ ".service-node.source-1",
    };
    const snapshot_operation = vopr.service_rate.Operation.named(name ++ ".snapshot-step", 40);
    const stream_operation = vopr.service_rate.Operation.named(name ++ ".stream-step", 25);
    const service_operations = [_]vopr.service_rate.Operation{ snapshot_operation, stream_operation };
    const node_slowdown_fault_id = vopr.id.stable(name, "service-rate.node-slowdown");
    const snapshot_slowdown_fault_id = vopr.id.stable(name, "service-rate.snapshot-slowdown");

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
    const mode_names = [_][]const u8{
        name ++ ".clean",
        name ++ ".source-crash",
        name ++ ".target-crash",
        name ++ ".cancellation",
        name ++ ".stale-owner",
        name ++ ".topology-change",
        name ++ ".schema-change",
        name ++ ".stream-crash-after-apply",
    };

    /// Reusable production replication owner. Focused histories own their
    /// VoprIo, while deployment-shaped histories borrow the cluster runtime
    /// and may supply the real target write path and shared work permit.
    pub const Fixture = struct {
        allocator: std.mem.Allocator,
        owned_sim: ?vopr.vopr_io.VoprIo,
        sim: *vopr.vopr_io.VoprIo,
        registry: foreign.Registry = .{},
        records: std.ArrayListUnmanaged(table_manager.ReplicationSourceStatusRecord) = .empty,
        external_work_permit: ?replication.WorkPermit = null,
        external_write_source: ?table_writes.TableWriteSource = null,
        mode: Mode = .clean,
        complete: bool = false,
        lease_valid: bool = true,
        fault_armed: bool = true,
        first_attempt_failed: bool = false,
        stale_rejected: bool = false,
        target_rebalanced: bool = false,
        schema_changed: bool = false,
        schema_v2_query_seen: bool = false,
        source_crash_pending: bool = false,
        source_crash_injected: bool = false,
        source_crash_error_code: u64 = 0,
        source_sessions_opened: u64 = 0,
        source_sessions_closed: u64 = 0,
        source_sessions_live: u64 = 0,
        source_sessions_peak: u64 = 0,
        source_crash_session: u64 = 0,
        source_recovery_session: u64 = 0,
        applied_mask: u8 = 0,
        apply_calls: u32 = 0,
        lifecycle_calls: u32 = 0,
        max_snapshot_checkpoint: u64 = 0,
        service_model: ?vopr.service_rate.Model = null,
        service_port: ?vopr.service_rate.Port = null,
        service_heal_stage: u8 = 0,
        service_charges: [2]u64 = .{ 0, 0 },

        const config_v1 = "[{\"type\":\"postgres\",\"dsn\":\"postgres://vopr\",\"postgres_table\":\"users_v1\",\"key_template\":\"id\"}]";
        const config_v2 = "[{\"type\":\"postgres\",\"dsn\":\"postgres://vopr\",\"postgres_table\":\"users_v2\",\"key_template\":\"id\"}]";

        const SourceSession = struct {
            fixture: *Fixture,
            generation: u64,
        };

        fn init(allocator: std.mem.Allocator) !*Fixture {
            const self = try allocator.create(Fixture);
            errdefer allocator.destroy(self);
            self.* = .{
                .allocator = allocator,
                .owned_sim = try vopr.vopr_io.VoprIo.init(.{
                    .seed = 0x52504c,
                    .instrumentation = .{ .enabled = false, .map_digest = 0x52504c },
                }),
                .sim = undefined,
            };
            errdefer self.owned_sim.?.deinit();
            self.sim = &self.owned_sim.?;
            return try self.initRegistry();
        }

        pub fn initWithVoprIo(
            allocator: std.mem.Allocator,
            sim: *vopr.vopr_io.VoprIo,
        ) !*Fixture {
            const self = try allocator.create(Fixture);
            errdefer allocator.destroy(self);
            self.* = .{
                .allocator = allocator,
                .owned_sim = null,
                .sim = sim,
            };
            return try self.initRegistry();
        }

        fn initRegistry(self: *Fixture) !*Fixture {
            try self.registry.registerWithContext(
                self.allocator,
                .postgres,
                self,
                sourceFactory,
                null,
            );
            return self;
        }

        pub fn deinit(self: *@This()) void {
            for (self.records.items) |record| table_manager.freeReplicationSourceStatus(self.allocator, record);
            self.records.deinit(self.allocator);
            self.registry.deinit(self.allocator);
            self.service_port = null;
            if (self.service_model) |*model| model.deinit();
            self.service_model = null;
            if (self.owned_sim) |*owned| owned.deinit();
            const allocator = self.allocator;
            self.* = undefined;
            allocator.destroy(self);
        }

        pub fn setWorkPermit(self: *@This(), permit_override: replication.WorkPermit) void {
            self.external_work_permit = permit_override;
        }

        pub fn setWriteSource(self: *@This(), write_source: table_writes.TableWriteSource) void {
            self.external_write_source = write_source;
        }

        fn enableServiceRates(self: *@This()) !void {
            self.service_model = try vopr.service_rate.Model.init(
                self.allocator,
                &.{service_node},
                &service_operations,
            );
            errdefer {
                self.service_model.?.deinit();
                self.service_model = null;
            }
            const model = &self.service_model.?;
            try model.activate(.{
                .fault_id = node_slowdown_fault_id,
                .node_id = service_node.id,
                .multiplier_ppm = 2 * vopr.service_rate.parts_per_million,
            });
            try model.activate(.{
                .fault_id = snapshot_slowdown_fault_id,
                .node_id = service_node.id,
                .operation_id = snapshot_operation.id,
                .multiplier_ppm = 2 * vopr.service_rate.parts_per_million,
            });
            self.service_port = try model.port(self.sim.io(), service_node.id);
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

        pub fn upsertReplicationSourceStatus(self: *@This(), record: table_manager.ReplicationSourceStatusRecord) !void {
            try self.records.append(self.allocator, try table_manager.cloneReplicationSourceStatus(self.allocator, record));
            self.max_snapshot_checkpoint = @max(self.max_snapshot_checkpoint, record.snapshot_offset);
        }

        fn checkpoint(ptr: *anyopaque, kind: replication.WorkKind) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!self.lease_valid) return error.CdcWorkLeaseLost;
            const port = self.service_port orelse return;
            const operation, const charge_index = switch (kind) {
                .snapshot_step => .{ snapshot_operation, @as(usize, 0) },
                .stream_step => .{ stream_operation, @as(usize, 1) },
            };
            _ = try port.charge(operation.id, 1);
            self.service_charges[charge_index] += 1;
            switch (self.service_heal_stage) {
                0 => {
                    try self.service_model.?.heal(snapshot_slowdown_fault_id);
                    self.service_heal_stage = 1;
                },
                1 => {
                    try self.service_model.?.heal(node_slowdown_fault_id);
                    self.service_heal_stage = 2;
                },
                else => {},
            }
        }

        fn deadline(_: *anyopaque) !u64 {
            return std.math.maxInt(u64);
        }

        fn permit(self: *@This()) replication.WorkPermit {
            return self.external_work_permit orelse
                .{ .ptr = self, .checkpoint_fn = checkpoint, .deadline_fn = deadline };
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
                    // Arm the actual provider query boundary. Throwing from
                    // this lifecycle hook would only prove runner recovery
                    // from a harness callback, not replacement of a failed
                    // source session.
                    self.source_crash_pending = true;
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
            const self: *@This() = @ptrCast(@alignCast(ptr));
            allocator.free(config.dsn);
            const session = try allocator.create(SourceSession);
            self.source_sessions_opened +|= 1;
            self.source_sessions_live +|= 1;
            self.source_sessions_peak = @max(self.source_sessions_peak, self.source_sessions_live);
            session.* = .{
                .fixture = self,
                .generation = self.source_sessions_opened,
            };
            return .{ .ptr = session, .vtable = &.{
                .deinit = sourceDeinit,
                .query = sourceQuery,
                .statistics = sourceStatistics,
                .prepare_replication = prepareReplication,
                .poll_changes = pollChanges,
            } };
        }

        fn sourceDeinit(ptr: *anyopaque, allocator: std.mem.Allocator) void {
            const session: *SourceSession = @ptrCast(@alignCast(ptr));
            std.debug.assert(session.fixture.source_sessions_live > 0);
            session.fixture.source_sessions_live -= 1;
            session.fixture.source_sessions_closed +|= 1;
            allocator.destroy(session);
        }

        fn sourceStatistics(_: *anyopaque, _: []const u8) !foreign.TableStatistics {
            return .{ .row_count = 2, .size_bytes = 64 };
        }

        fn sourceQuery(ptr: *anyopaque, allocator: std.mem.Allocator, params: foreign.QueryParams) !foreign.QueryResult {
            const session: *SourceSession = @ptrCast(@alignCast(ptr));
            const self = session.fixture;
            if (self.source_crash_pending) {
                self.source_crash_pending = false;
                self.source_crash_injected = true;
                self.source_crash_error_code = @intFromError(error.ConnectionResetByPeer);
                self.source_crash_session = session.generation;
                return error.ConnectionResetByPeer;
            }
            if (self.source_crash_injected and session.generation > self.source_crash_session)
                self.source_recovery_session = session.generation;
            if (std.mem.eql(u8, params.table, "users_v2"))
                self.schema_v2_query_seen = true;
            const rows_json = [_][]const u8{
                "{\"id\":\"doc:d\",\"name\":\"alpha\"}",
                "{\"id\":\"doc:e\",\"name\":\"beta\"}",
            };
            // The production snapshot runner converts its durable keyset
            // checkpoint into a filter and resets the numeric offset to zero.
            // Honor that contract so a resumed VOPR history cannot return the
            // same terminal row forever.
            const start = if (params.filter_query_json) |filter|
                if (std.mem.indexOf(u8, filter, "doc:e") != null) rows_json.len else 1
            else
                params.offset;
            if (start >= rows_json.len) return .{ .total = rows_json.len };
            const count = @min(params.limit orelse rows_json.len, rows_json.len - start);
            const rows = try allocator.alloc(std.json.Value, count);
            errdefer allocator.free(rows);
            for (rows_json[start..][0..count], 0..) |json, index| {
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
                .row = try std.json.parseFromSliceLeaky(std.json.Value, allocator, "{\"id\":\"doc:f\",\"name\":\"gamma\"}", .{}),
            };
            return .{
                .changes = changes,
                .checkpoint = try allocator.dupe(u8, "lsn:3"),
            };
        }

        fn batch(
            ptr: *anyopaque,
            allocator: std.mem.Allocator,
            table_name: []const u8,
            req: db_types.BatchRequest,
        ) !?void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.apply_calls += 1;
            if (self.mode == .target_crash and self.fault_armed) {
                self.fault_armed = false;
                return error.ConnectionResetByPeer;
            }
            if (self.external_write_source) |write_source| {
                _ = try write_source.batch(allocator, table_name, req) orelse
                    return error.ReplicationTargetBatchUnsupported;
            }
            for (req.writes) |write| self.markApplied(write.key);
            for (req.transforms) |transform| self.markApplied(transform.key);
            for (req.deletes) |key| self.clearApplied(key);
            return {};
        }

        fn markApplied(self: *@This(), key: []const u8) void {
            if (std.mem.eql(u8, key, "doc:d")) self.applied_mask |= 1;
            if (std.mem.eql(u8, key, "doc:e")) self.applied_mask |= 2;
            if (std.mem.eql(u8, key, "doc:f")) self.applied_mask |= 4;
        }

        fn clearApplied(self: *@This(), key: []const u8) void {
            if (std.mem.eql(u8, key, "doc:d")) self.applied_mask &= ~@as(u8, 1);
            if (std.mem.eql(u8, key, "doc:e")) self.applied_mask &= ~@as(u8, 2);
            if (std.mem.eql(u8, key, "doc:f")) self.applied_mask &= ~@as(u8, 4);
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

        pub fn runClean(self: *@This()) !void {
            self.mode = .clean;
            try self.run();
        }

        /// Interrupt after the first snapshot batch has reached the target,
        /// publish a changed source schema, and resume from durable status.
        /// Deployment-shaped histories use this entry point directly; there
        /// is intentionally no compatibility selector or stringly mode API.
        pub fn runSchemaChange(self: *@This()) !void {
            self.mode = .schema_change;
            self.fault_armed = true;
            try self.run();
        }

        /// Fail the first provider query after durable preparation, close that
        /// owned source session, and resume through a newly created session.
        /// This entry point is shared by focused and deployment histories.
        pub fn runSourceCrash(self: *@This()) !void {
            self.mode = .source_crash;
            self.fault_armed = true;
            try self.run();
        }

        pub fn completionSound(self: *const @This()) bool {
            return self.complete and self.applied_mask == 7 and
                self.max_snapshot_checkpoint <= @popCount(self.applied_mask & 3) and
                self.apply_calls >= 3 and self.lifecycle_calls > 0;
        }

        pub fn schemaChangeRecoverySound(self: *const @This()) bool {
            return self.completionSound() and self.first_attempt_failed and
                self.schema_changed and self.schema_v2_query_seen and
                !self.fault_armed;
        }

        pub fn sourceCrashRecoverySound(self: *const @This()) bool {
            return self.completionSound() and self.first_attempt_failed and
                self.source_crash_injected and !self.source_crash_pending and
                self.source_crash_error_code == @intFromError(error.ConnectionResetByPeer) and
                self.source_crash_session == 1 and
                self.source_recovery_session == 2 and
                self.source_sessions_opened == 3 and
                self.source_sessions_closed == self.source_sessions_opened and
                self.source_sessions_live == 0 and self.source_sessions_peak == 1 and
                !self.fault_armed;
        }

        pub fn firstAttemptFailed(self: *const @This()) bool {
            return self.first_attempt_failed;
        }

        pub fn schemaChanged(self: *const @This()) bool {
            return self.schema_changed;
        }

        pub fn schemaV2QuerySeen(self: *const @This()) bool {
            return self.schema_v2_query_seen;
        }
    };

    pub const World = struct { state: *Fixture };

    pub fn init(allocator: std.mem.Allocator) !World {
        return .{ .state = try Fixture.init(allocator) };
    }

    pub fn deinit(world: *World, _: std.mem.Allocator) void {
        world.state.deinit();
        world.* = undefined;
    }

    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: std.mem.Allocator) !void {
        if (world.state.complete) return;
        inline for (std.meta.tags(Mode), mode_ids, mode_names) |mode, id, mode_name| {
            try list.append(allocator, .{ .id = id, .name = mode_name, .kind = if (mode == .clean) .workload else .fault });
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
        try builder.addNamed(allocator, name ++ ".applied-mask", @intCast(world.state.applied_mask));
        try builder.addNamed(allocator, name ++ ".apply-calls", @intCast(world.state.apply_calls));
        try builder.addNamed(allocator, name ++ ".lifecycle-calls", @intCast(world.state.lifecycle_calls));
        try builder.addNamed(allocator, name ++ ".checkpoint", @intCast(world.state.max_snapshot_checkpoint));
        try builder.addNamed(allocator, name ++ ".source-crash-injected", @intFromBool(world.state.source_crash_injected));
        try builder.addNamed(allocator, name ++ ".source-crash-error", @intCast(world.state.source_crash_error_code));
        try builder.addNamed(allocator, name ++ ".source-sessions-opened", @intCast(world.state.source_sessions_opened));
        try builder.addNamed(allocator, name ++ ".source-sessions-closed", @intCast(world.state.source_sessions_closed));
        try builder.addNamed(allocator, name ++ ".source-sessions-live", @intCast(world.state.source_sessions_live));
        try builder.addNamed(allocator, name ++ ".source-sessions-peak", @intCast(world.state.source_sessions_peak));
        try builder.addNamed(allocator, name ++ ".source-crash-session", @intCast(world.state.source_crash_session));
        try builder.addNamed(allocator, name ++ ".source-recovery-session", @intCast(world.state.source_recovery_session));
    }

    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
        const state = world.state;
        try sink.check(allocator, safe_checkpoint_id, state.max_snapshot_checkpoint <= @popCount(state.applied_mask & 3));
        try sink.check(allocator, no_loss_id, !state.complete or state.applied_mask == 7);
        try sink.check(allocator, stale_rejected_id, state.mode != .stale_owner or state.stale_rejected);
        try sink.check(
            allocator,
            duplicate_safe_id,
            !state.complete or state.applied_mask == 7,
        );
        try sink.check(allocator, recovery_id, state.complete and
            (state.mode == .clean or state.first_attempt_failed) and
            (state.mode != .source_crash or state.sourceCrashRecoverySound()));
    }

    pub fn healthSnapshot(world: *World) vopr.health.Snapshot {
        const state = world.state;
        return state.sim.healthSnapshot(.{
            .progress_expected = true,
            .progress_units = @popCount(state.applied_mask) + state.lifecycle_calls,
            .recovery_expected = state.mode != .clean,
            .recovery_complete = state.complete and (state.mode == .clean or state.first_attempt_failed),
            .consistency_valid = state.max_snapshot_checkpoint <= @popCount(state.applied_mask & 3) and
                (!state.complete or state.applied_mask == 7),
            .cleanup_complete = state.complete,
        });
    }

    pub fn done(world: *World) bool {
        return world.state.complete;
    }
};

test "replication backfill service rates compose and heal across production snapshot and stream" {
    var alloc_state: VoprTestAllocator = .init;
    defer _ = alloc_state.deinit();
    const alloc = alloc_state.allocator();
    var world = try Scenario.init(alloc);
    defer Scenario.deinit(&world, alloc);
    const state = world.state;
    try state.enableServiceRates();

    var done = false;
    var failure: ?anyerror = null;
    const Task = struct {
        fn run(target: *Scenario.Fixture, completed: *bool, task_failure: *?anyerror) void {
            target.run() catch |err| {
                task_failure.* = err;
            };
            completed.* = true;
        }
    };
    const io = state.sim.io();
    _ = io.async(Task.run, .{ state, &done, &failure });

    var enabled: vopr.transition.List = .{};
    defer enabled.deinit(alloc);
    var events: vopr.event.Sink = .{};
    defer events.deinit(alloc);
    var transitions: usize = 0;
    while (!state.sim.scheduler().quiescent()) {
        enabled.items.clearRetainingCapacity();
        try state.sim.scheduler().enumerateReady(&enabled, alloc);
        try enabled.canonicalize();
        if (enabled.items.items.len == 0) return error.VoprReplicationServiceRateDeadlock;
        try state.sim.scheduler().executeReady(enabled.items.items[0].id, &events, alloc);
        transitions += 1;
        if (transitions > 10_000) return error.VoprReplicationServiceRateTransitionBudgetExceeded;
    }

    try std.testing.expect(done);
    if (failure) |err| return err;
    try std.testing.expect(state.complete);
    try std.testing.expectEqual(@as(u8, 7), state.applied_mask);
    try std.testing.expectEqual(@as(u8, 2), state.service_heal_stage);
    try std.testing.expect(state.service_charges[0] >= 2);
    try std.testing.expect(state.service_charges[1] >= 1);
    const usage = try state.service_model.?.nodeUsage(Scenario.service_node.id);
    try std.testing.expectEqual(state.service_charges[0] + state.service_charges[1], usage.charges);
    try std.testing.expectEqual(state.service_charges[0] + state.service_charges[1], usage.units);
    const expected_snapshot_ns = Scenario.snapshot_operation.base_cost_ns * (state.service_charges[0] + 4);
    const expected_stream_ns = Scenario.stream_operation.base_cost_ns * state.service_charges[1];
    try std.testing.expectEqual(expected_snapshot_ns + expected_stream_ns, usage.charged_ns);
    try std.testing.expectEqualDeep(vopr.service_rate.Usage{
        .charges = state.service_charges[0],
        .units = state.service_charges[0],
        .charged_ns = expected_snapshot_ns,
    }, try state.service_model.?.operationUsage(Scenario.service_node.id, Scenario.snapshot_operation.id));
    try std.testing.expectEqualDeep(vopr.service_rate.Usage{
        .charges = state.service_charges[1],
        .units = state.service_charges[1],
        .charged_ns = expected_stream_ns,
    }, try state.service_model.?.operationUsage(Scenario.service_node.id, Scenario.stream_operation.id));
    try std.testing.expectEqual(@as(usize, 0), state.service_model.?.active.items.len);
    try state.sim.ensureNoCapabilityViolation();
}

test "replication backfill VOPR exact replays every production recovery mode" {
    const backend_ids = vopr.vopr_io.artifactBackendIds();
    var duplicate_apply_observed = false;
    for (Scenario.mode_ids, Scenario.mode_names) |mode_id, mode_name| {
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
        if (recorded.summary.?.property_failures != 0) {
            std.debug.print("replication VOPR mode {s} failed properties\n", .{mode_name});
            for (recorded.failures.items) |failure|
                std.debug.print("  failure={s} class={s}\n", .{ failure.identity, @tagName(failure.class) });
            if (recorded.observations.items.len > 0)
                for (recorded.observations.items[recorded.observations.items.len - 1].features) |feature|
                    std.debug.print("  {s}={d}\n", .{ feature.name, feature.value });
        }
        try std.testing.expectEqual(@as(u64, 0), recorded.summary.?.property_failures);
        if (recorded.observations.items.len > 0) {
            for (recorded.observations.items[recorded.observations.items.len - 1].features) |feature| {
                if (std.mem.eql(u8, feature.name, Scenario.name ++ ".apply-calls") and feature.value > 3) {
                    duplicate_apply_observed = true;
                }
            }
        }
        for (0..20) |_| {
            var replayed = try vopr.replay.exact(Scenario, std.testing.allocator, &recorded);
            replayed.deinit();
        }
    }
    try std.testing.expect(duplicate_apply_observed);
}
