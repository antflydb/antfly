// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
const engine = @import("../db/db.zig");
const DB = engine.DB;
const OpenOptions = engine.OpenOptions;
const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
const internal_keys = @import("../internal_keys.zig");
const docstore_mod = @import("../docstore.zig");
const ha_primary_mod = @import("primary.zig");
const ha_public_gate_state_mod = @import("public_gate_state.zig");
const ha_effects_mod = @import("../db/replication_effects.zig");
const replication_effects_mod = ha_effects_mod;
const ha_replication_record_mod = @import("../db/replication_record.zig");
const replication_ingress = @import("../db/replication_ingress.zig");
const publisher_adapter = @import("db_commit.zig");
test "db graph endpoint cleanup pages HA mirrors exact effects across directory progress" {
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |sync_mirror| {
        var primary_dir = try TestDirectory.init("ha-cleanup-primary");
        defer primary_dir.cleanup();
        var replica_dir = try TestDirectory.init("ha-cleanup-replica");
        defer replica_dir.cleanup();
        var log_dir = try TestDirectory.init("ha-cleanup-log");
        defer log_dir.cleanup();
        var slots_dir = try TestDirectory.init("ha-cleanup-slots");
        defer slots_dir.cleanup();
        var stream = try ha_primary_mod.Primary.open(alloc, log_dir.path().ptr, slots_dir.path().ptr, .{ .cluster_id = 200, .shard_id = 3, .table_id = 9, .timeline_id = 1, .epoch = 1 }, .{});
        defer stream.close();
        var primary = try DB.open(alloc, primary_dir.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
        defer primary.close();
        var replica = try DB.open(alloc, replica_dir.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
        defer replica.close();
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        const rows = try scratch.alloc(docstore_mod.KVPair, 300);
        for (rows, 0..) |*row, i| row.* = .{ .key = try std.fmt.allocPrint(scratch, "doc:{d}", .{i}), .value = "{}" };
        const job = try internal_keys.graphEndpointCleanupKeyAlloc(scratch, "hub");
        for ([_]*DB{ &primary, &replica }) |db| {
            try db.core.store.putBatch(rows, &.{});
            try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
            try db.batch(.{ .graph_writes = &.{.{ .index_name = "g", .source = "a", .target = "hub", .edge_type = "R" }}, .sync_level = .full_index });
            try db.core.store.put(job, "hub");
        }
        // A base restore or physical rewrite can leave the standby's local
        // incoming directory incomplete. It must not spend this HA command
        // rebuilding a page instead of applying the primary's retirements.
        try replica.core.store.invalidateGraphDirectories();
        const Wait = struct {
            fn wait(_: *anyopaque, active_ptr: *anyopaque, target: u64, _: ha_primary_mod.SyncPolicy) !void {
                const active: *ha_primary_mod.Primary = @ptrCast(@alignCast(active_ptr));
                try active.standbyStatusUpdate("standby-a", active.identity.timeline_id, target, target);
            }
        };
        var wait_ctx: u8 = 0;
        const names = [_][]const u8{"standby-a"};
        if (sync_mirror) try stream.createSlot("standby-a", 0);
        var last_lsn = @import("antfly_platform").atomic.Value(u64).init(0);
        primary.local_execution.replication_async_batch_mirror = .{
            .publisher = publisher_adapter.bind(&stream),
            .last_lsn = &last_lsn,
            .sync_policy = if (sync_mirror) .{ .mode = .remote_apply, .standby_names = &names, .failure_policy = .block } else .{},
            .sync_wait_ctx = &wait_ctx,
            .sync_wait_fn = Wait.wait,
        };
        try std.testing.expect(try engine.test_support.runStandaloneGraphEndpointCleanupStep(&primary));
        var entry = (try stream.log.entryAt(alloc, last_lsn.load(.acquire))).?;
        defer entry.deinit(alloc);
        var decoded = try ha_effects_mod.decodeBatchMutationRequest(alloc, entry.record);
        defer decoded.deinit();
        try std.testing.expect(decoded.value.request.graph_endpoint_cleanup);
        try std.testing.expect(decoded.value.request.graph_endpoint_cleanup_planned);
        try std.testing.expectEqual(@as(usize, 1), decoded.value.request.graph_deletes.len);
        try std.testing.expectEqual(@as(usize, 1), decoded.value.request.deletes.len);
        try replication_ingress.applyRecord(&replica, entry.record);
        try std.testing.expect(!try primary.core.store.hasGraphEndpointCleanup());
        try std.testing.expect(!try replica.core.store.hasGraphEndpointCleanup());
        // Exact retirement is authoritative even before directory repair and
        // derived replay. Duplicate delivery cannot consume another page.
        const artifact = try internal_keys.graphRelationshipArtifactKeyAlloc(scratch, "a", "g", "R", "hub", "a", "");
        for ([_]*DB{ &primary, &replica }) |db| try std.testing.expect(try db.core.store.graphRelationshipRetired(artifact));
        try replication_ingress.applyRecord(&replica, entry.record);
        try std.testing.expectEqual(entry.record.lsn, try replica.replicationAppliedSequence());
    }
}

test "db graph endpoint cleanup pages HA denied owners reopen and replay without local planning" {
    const alloc = std.testing.allocator;
    for ([_]ha_public_gate_state_mod.Role{ .standby, .transitioning, .fenced_primary }) |role| {
        var directory = try TestDirectory.init("ha-cleanup-authority");
        defer directory.cleanup();
        const job = try internal_keys.graphEndpointCleanupKeyAlloc(alloc, "hub");
        defer alloc.free(job);
        {
            var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
            defer db.close();
            try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
            try db.batch(.{ .graph_writes = &.{.{ .index_name = "g", .source = "a", .target = "hub", .edge_type = "R" }}, .sync_level = .full_index });
            // An HA endpoint deletion may be durable at shutdown while its
            // exact cleanup page is still in flight from the primary.
            try db.core.store.put(job, "hub");
            try db.core.store.invalidateGraphDirectories();
        }
        var gate: ha_public_gate_state_mod.State = .{};
        gate.role.store(@intFromEnum(role), .release);
        var reopened = try DB.open(alloc, directory.path(), .{ .replication_write_gate = .{ .shared = .{ .state = gate.storageWriteState() } }, .start_optional_runtimes = false, .start_index_workers = false });
        defer reopened.close();
        try std.testing.expect(try reopened.core.store.hasGraphEndpointCleanup());
        try std.testing.expect(!try engine.test_support.drainStandaloneGraphEndpointCleanup(&reopened));
        try std.testing.expect(!try engine.test_support.runStandaloneGraphEndpointCleanupStep(&reopened));
        try std.testing.expect((try reopened.prepareGraphEndpointCleanupBatch(alloc)) == null);
        const reset = try reopened.core.store.get(alloc, internal_keys.graph_directory_reset_key);
        defer alloc.free(reset);
        try std.testing.expectEqualSlices(u8, &.{0}, reset);
        const payload = try replication_effects_mod.encodeBatchMutationRequestAlloc(alloc, .{ .graph_endpoint_cleanup = true, .graph_endpoint_cleanup_planned = true, .graph_endpoint_cleanup_guards = &.{.{ .endpoint = "hub", .generation = 0 }}, .graph_deletes = &.{.{ .index_name = "g", .source = "a", .target = "hub", .edge_type = "R" }}, .deletes = &.{job}, .sync_level = .write });
        defer alloc.free(payload);
        const record: ha_replication_record_mod.RecordView = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 1, .previous_lsn = 0, .payload = payload };
        try replication_ingress.applyRecord(&reopened, record);
        try std.testing.expect(!try reopened.core.store.hasGraphEndpointCleanup());
        try std.testing.expectEqual(@as(u64, 1), try reopened.replicationAppliedSequence());
        const artifact = try internal_keys.graphRelationshipArtifactKeyAlloc(alloc, "a", "g", "R", "hub", "a", "");
        defer alloc.free(artifact);
        try std.testing.expect(try reopened.core.store.graphRelationshipRetired(artifact));
        // Idle directory backfill is subject to the same authority gate.
        try std.testing.expect(!try engine.test_support.runStandaloneGraphEndpointCleanupStep(&reopened));
        try std.testing.expect((try reopened.prepareGraphEndpointCleanupBatch(alloc)) == null);
        try std.testing.expect(!try reopened.core.store.graphIncomingDirectoryReady());
    }
}

test "db graph endpoint cleanup pages HA promotion requires fresh owner generation" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("ha-cleanup-promotion");
    defer directory.cleanup();
    var log_dir = try TestDirectory.init("ha-cleanup-promotion-log");
    defer log_dir.cleanup();
    var slots_dir = try TestDirectory.init("ha-cleanup-promotion-slots");
    defer slots_dir.cleanup();
    var stream = try ha_primary_mod.Primary.open(alloc, log_dir.path().ptr, slots_dir.path().ptr, .{ .cluster_id = 200, .shard_id = 3, .table_id = 9, .timeline_id = 1, .epoch = 1 }, .{});
    defer stream.close();
    {
        var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
        defer db.close();
        const job = try internal_keys.graphEndpointCleanupKeyAlloc(alloc, "hub");
        defer alloc.free(job);
        try db.core.store.put(job, "hub");
    }
    var gate: ha_public_gate_state_mod.State = .{};
    gate.role.store(@intFromEnum(ha_public_gate_state_mod.Role.standby), .release);
    const opts: OpenOptions = .{ .replication_write_gate = .{ .shared = .{ .state = gate.storageWriteState() } }, .start_optional_runtimes = false, .start_index_workers = false };
    {
        var old_owner = try DB.open(alloc, directory.path(), opts);
        defer old_owner.close();
        gate.publishPrimary(&stream, false);
        try std.testing.expect(!try engine.test_support.drainStandaloneGraphEndpointCleanup(&old_owner));
        try std.testing.expect(!try engine.test_support.runStandaloneGraphEndpointCleanupStep(&old_owner));
        try std.testing.expect((try old_owner.prepareGraphEndpointCleanupBatch(alloc)) == null);
        try std.testing.expect(try old_owner.core.store.hasGraphEndpointCleanup());
    }
    var primary_opts = opts;
    primary_opts.replication_async_batch_mirror = .{ .publisher = publisher_adapter.bind(&stream) };
    var primary = try DB.open(alloc, directory.path(), primary_opts);
    defer primary.close();
    try std.testing.expect(!try primary.core.store.hasGraphEndpointCleanup());
    var entry = (try stream.log.entryAt(alloc, stream.lastLsn())).?;
    defer entry.deinit(alloc);
    var decoded = try ha_effects_mod.decodeBatchMutationRequest(alloc, entry.record);
    defer decoded.deinit();
    try std.testing.expect(decoded.value.request.graph_endpoint_cleanup_planned);
    try std.testing.expectEqual(@as(usize, 1), decoded.value.request.deletes.len);
}
