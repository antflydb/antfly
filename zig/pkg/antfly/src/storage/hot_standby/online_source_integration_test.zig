// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Hot standby integration for local online_source mutations.
const engine = @import("../db/online_source.zig");
const Scope = engine.Scope;
const std = @import("std");

const hot_standby_publisher_adapter = @import("db_commit.zig");

test "relational index system online source durable standby outbox resumes before already applied Raft receipt" {
    try sourceOutboxRecovery(false);
}

test "relational index system native rewrite source clock is forwarded by durable outbox across lost acknowledgement" {
    try sourceOutboxRecovery(true);
}

fn sourceOutboxRecovery(native_authority: bool) !void {
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    const primary_mod = @import("primary.zig");
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    const path = try std.fmt.allocPrint(owned, ".zig-cache/tmp/{s}/source-outbox", .{tmp.sub_path});
    const log_path = try std.fmt.allocPrintSentinel(owned, ".zig-cache/tmp/{s}/log", .{tmp.sub_path}, 0);
    const slots_path = try std.fmt.allocPrintSentinel(owned, ".zig-cache/tmp/{s}/slots", .{tmp.sub_path}, 0);
    var primary = try primary_mod.Primary.open(alloc, log_path, slots_path, .{ .cluster_id = 1, .timeline_id = 1, .epoch = 1, .table_id = 1, .shard_id = 2 }, .{});
    defer primary.close();
    try primary.createSlot("standby", 0);
    const Ack = struct {
        calls: usize = 0,
        fn wait(ptr: *anyopaque, stream_ctx: *anyopaque, lsn: u64, _: primary_mod.SyncPolicy) !void {
            const stream: *primary_mod.Primary = @ptrCast(@alignCast(stream_ctx));
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.calls == 1) return error.InjectedSourceMirrorWaitFailure;
            try stream.standbyStatusUpdate("standby", 1, lsn, lsn);
        }
    };
    var ack: Ack = .{};
    const options: db_mod.OpenOptions = .{ .online_source_authority = if (native_authority) .native else null, .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    var scope: Scope = undefined;
    {
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        try db.setSchemaJson(alloc, "{}");
        const owner = try db.relationalTopologyIdentity();
        scope = .{
            .authority = if (native_authority) .native else .raft,
            .fence = .{ .admission_epoch = owner.next_epoch, .transition_id = 44, .attempt = 1, .peer_group_id = 3, .owner_group_id = 2, .role = if (native_authority) .rewrite_source else .merge_source, .namespace = owner.namespace, .catalog_digest = owner.catalog_digest },
            .receiver_namespace = .{ .table_id = if (native_authority) 4 else 1, .shard_id = 3, .range_id = 3 },
            .consumer_epoch = 1,
            .copy_attempt = .{ .donor_term = if (native_authority) 0 else 1, .sequence = 1 },
        };
        db.replication_async_batch_mirror = .{ .publisher = hot_standby_publisher_adapter.bind(&primary), .sync_policy = .{ .mode = .remote_write, .standby_names = &.{"standby"}, .failure_policy = .block }, .sync_wait_ctx = &ack, .sync_wait_fn = Ack.wait };
        const request: @import("../db/types.zig").BatchRequest = .{ .online_source = .{ .admit = .{ .scope = scope } } };
        try std.testing.expectError(error.InjectedSourceMirrorWaitFailure, if (native_authority) db.batch(request) else @import("../server_db_adapter.zig").applyOrdered(&db, request, .{ .term = 1, .index = 11 }));
        try std.testing.expectEqual(@as(u64, if (native_authority) 1 else 11), (try db.onlineSourceStatus(scope)).admitted_applied_index);
        try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
        // Simulate loss of process-local mirror state before its outstanding
        // acknowledgement can complete. The owner/outbox remain durable.
        db.replication_async_batch_mirror = null;
    }
    {
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        db.replication_async_batch_mirror = .{ .publisher = hot_standby_publisher_adapter.bind(&primary), .sync_policy = .{ .mode = .remote_write, .standby_names = &.{"standby"}, .failure_policy = .block }, .sync_wait_ctx = &ack, .sync_wait_fn = Ack.wait };
        const request: @import("../db/types.zig").BatchRequest = .{ .online_source = .{ .admit = .{ .scope = scope } } };
        if (native_authority) try db.batch(request) else try @import("../server_db_adapter.zig").applyOrdered(&db, request, .{ .term = 1, .index = 11 });
        try std.testing.expect(ack.calls >= 2);
        try std.testing.expectEqual(@as(u64, if (native_authority) 2 else 1), primary.lastLsn());
        var entry = (try primary.log.entryAt(alloc, 1)) orelse return error.TestUnexpectedResult;
        defer entry.deinit(alloc);
        var decoded = try @import("../db/replication_effects.zig").decodeBatchMutationRequest(alloc, entry.record);
        defer decoded.deinit();
        try std.testing.expectEqual(@as(?u64, if (native_authority) 1 else 11), decoded.value.online_source_applied_index);
        try std.testing.expectEqualSlices(u8, &scope.pin(), &decoded.value.request.online_source.?.scope().pin());
        if (native_authority) {
            var retry = (try primary.log.entryAt(alloc, 2)).?;
            defer retry.deinit(alloc);
            var retry_decoded = try @import("../db/replication_effects.zig").decodeBatchMutationRequest(alloc, retry.record);
            defer retry_decoded.deinit();
            try std.testing.expectEqual(@as(?u64, 2), retry_decoded.value.online_source_applied_index);
            try std.testing.expectEqual(.native, retry_decoded.value.request.online_source.?.scope().authority);
            try std.testing.expectEqual(@as(u64, 1), (try db.onlineSourceStatus(scope)).admitted_applied_index);
            try std.testing.expect((try db.raftAppliedEntry()) == null);
        }
    }
}
