// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Hot standby integration for local restore_staging mutations.
const engine = @import("../db/restore_staging.zig");
const OwnerBootstrap = engine.OwnerBootstrap;
const Phase = engine.Phase;
const Scope = engine.Scope;
const digest = engine.digest;
const key = engine.key;
const replication_ingress = @import("../db/replication_ingress.zig");
const std = @import("std");

const hot_standby_publisher_adapter = @import("db_commit.zig");

test "relational integrity restore staging Raft controls retain HA append obligations and replay original import timestamps" {
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    const primary_mod = @import("primary.zig");
    const effects = @import("../db/replication_effects.zig");
    const types = @import("../db/types.zig");
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    const source_path = try std.fmt.allocPrint(owned, ".zig-cache/tmp/{s}/source-ha", .{tmp.sub_path});
    var source_options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    {
        var source = try db_mod.DB.open(alloc, source_path, source_options);
        defer source.close();
        try source.setSchemaJson(alloc, "{}");
        try source.batch(.{ .timestamp_ns = 1234, .writes = &.{.{ .key = "row", .value = "{\"id\":1}" }} });
    }
    source_options.open_mode = .query_readonly;
    var source = try db_mod.DB.open(alloc, source_path, source_options);
    defer source.close();
    for ([_]bool{ false, true }, 0..) |synchronous, trial| {
        const path = try std.fmt.allocPrint(owned, ".zig-cache/tmp/{s}/primary-{d}", .{ tmp.sub_path, trial });
        const replica_path = try std.fmt.allocPrint(owned, ".zig-cache/tmp/{s}/standby-{d}", .{ tmp.sub_path, trial });
        const log_path = try std.fmt.allocPrintSentinel(owned, ".zig-cache/tmp/{s}/log-{d}", .{ tmp.sub_path, trial }, 0);
        const slots_path = try std.fmt.allocPrintSentinel(owned, ".zig-cache/tmp/{s}/slots-{d}", .{ tmp.sub_path, trial }, 0);
        var primary = try primary_mod.Primary.open(alloc, log_path, slots_path, .{ .cluster_id = 1, .timeline_id = 1, .epoch = 1, .table_id = 10, .shard_id = 11 }, .{});
        defer primary.close();
        try primary.createSlot("standby", 0);
        const Ack = struct {
            calls: usize = 0,
            fn wait(ptr: *anyopaque, stream_ctx: *anyopaque, lsn: u64, _: primary_mod.SyncPolicy) !void {
                const stream: *primary_mod.Primary = @ptrCast(@alignCast(stream_ctx));
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.calls += 1;
                if (self.calls == 1) return error.InjectedRestoreMirrorWaitFailure;
                try stream.standbyStatusUpdate("standby", 1, lsn, lsn);
            }
        };
        var ack: Ack = .{};
        const target_options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 10, .shard_id = 11, .range_id = 11 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
        var target = try db_mod.DB.open(alloc, path, target_options);
        defer target.close();
        var replica = try db_mod.DB.open(alloc, replica_path, target_options);
        defer replica.close();
        try target.setSchemaJson(alloc, "{}");
        try replica.setSchemaJson(alloc, "{}");
        const schema = try @import("../schema.zig").serializeSchema(owned, target.core.schema orelse .{});
        const scope: Scope = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .source_artifact_digest = @splat(3), .source_namespace = source_options.identity_namespace.?, .target_namespace = target_options.identity_namespace.?, .target_schema_digest = digest(schema) };
        try target.reserveRestoreStagingScoped(alloc, scope);
        try replica.reserveRestoreStagingScoped(alloc, scope);
        const bootstrap: OwnerBootstrap = .{ .scope = scope, .table_name = "docs", .schema_json = "{}", .indexes_json = "{}", .byte_range = .{ .start = "", .end = "" } };
        try target.installRestoreStagingBootstrap(alloc, bootstrap);
        try target.installRestoreStagingBootstrap(alloc, bootstrap);
        var changed_bootstrap = bootstrap;
        changed_bootstrap.table_name = "another-table";
        try std.testing.expectError(error.RestoreStagingScopeChanged, target.installRestoreStagingBootstrap(alloc, changed_bootstrap));
        {
            var stored_bootstrap = (try target.readRestoreStagingBootstrap(alloc)) orelse return error.TestUnexpectedResult;
            defer stored_bootstrap.deinit();
            try std.testing.expectEqualStrings("docs", stored_bootstrap.value.table_name);
        }
        target.local_execution.replication_async_batch_mirror = hot_standby_publisher_adapter.bindMirror(&primary, .{
            .sync_policy = .{ .mode = if (synchronous) .remote_write else .async, .standby_names = &.{"standby"}, .failure_policy = .block },
            .sync_wait_ctx = &ack,
            .sync_wait_fn = Ack.wait,
        });
        const begin: types.BatchRequest = .{ .restore_staging = .{ .begin = scope } };
        if (synchronous) {
            try std.testing.expectError(error.InjectedRestoreMirrorWaitFailure, @import("../server_db_adapter.zig").applyOrdered(&target, begin, .{ .term = 1, .index = 1 }));
            try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
        }
        try @import("../server_db_adapter.zig").applyOrdered(&target, begin, .{ .term = 1, .index = 1 });
        try @import("../server_db_adapter.zig").applyOrdered(&target, begin, .{ .term = 1, .index = 1 });
        try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
        var raft_index: u64 = 2;
        while (true) : (raft_index += 1) {
            var page = try target.prepareRestoreStagingPage(alloc, scope, &source, 128, .none);
            defer page.deinit();
            if (page.batch) |batch| {
                try @import("../server_db_adapter.zig").applyOrdered(&target, batch, .{ .term = 1, .index = raft_index });
                try @import("../server_db_adapter.zig").applyOrdered(&target, batch, .{ .term = 1, .index = raft_index });
            }
            if (page.phase == .imported) break;
        }
        for ([_]Phase{ .validated, .published }) |phase| {
            raft_index += 1;
            try @import("../server_db_adapter.zig").applyOrdered(&target, .{ .restore_staging = .{ .finish = .{ .scope = scope.digest(), .phase = phase } } }, .{ .term = 1, .index = raft_index });
        }
        var saw_import = false;
        for (1..primary.lastLsn() + 1) |lsn| {
            var entry = (try primary.log.entryAt(alloc, lsn)) orelse return error.TestUnexpectedResult;
            defer entry.deinit(alloc);
            var decoded = try effects.decodeBatchMutationRequest(alloc, entry.record);
            defer decoded.deinit();
            if (decoded.value.request.restore_staging.? == .begin) {
                const restored_bootstrap = decoded.value.restore_staging_bootstrap orelse return error.TestUnexpectedResult;
                try std.testing.expectEqualStrings("docs", restored_bootstrap.table_name);
                try std.testing.expectEqualSlices(u8, &scope.digest(), &restored_bootstrap.scope.digest());
                try replica.installRestoreStagingBootstrap(alloc, restored_bootstrap);
            } else try std.testing.expect(decoded.value.restore_staging_bootstrap == null);
            if (decoded.value.request.restore_staging.? == .import_page and decoded.value.request.writes.len != 0) {
                saw_import = true;
                try std.testing.expectEqual(@as(u64, 1234), decoded.value.request.restore_staging.?.import_page.timestamps[0].timestamp);
            }
            try replication_ingress.applyRecord(&replica, entry.record);
        }
        try std.testing.expect(saw_import);
        var found = (try replica.lookup(alloc, "row", .{})) orelse return error.TestUnexpectedResult;
        defer found.deinit(alloc);
        try std.testing.expectEqualStrings("{\"id\":1}", found.json);
    }
}
