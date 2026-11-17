// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Explicit table transaction origins cross only reviewed callback contracts.
//! Real native roots through the production distributed coordinator/local
//! participant worker. Catalog placement is immutable fixture input; no Raft
//! entry, history admission or source grant is manufactured here.
const std = @import("std");
const txn = @import("distributed_txn.zig");
const local = @import("antfly_local_sources");
const db_mod = local.storage_db_selected_root.db;
const Bound = local.api_local_table_writes.BoundTableWriteSource;
const metadata = @import("../metadata/api.zig");
const Catalog = struct {
    fn source() @import("table_catalog.zig").CatalogSource {
        return .{ .ptr = undefined, .vtable = &.{ .admin_snapshot = snapshot, .free_admin_snapshot = free } };
    }
    fn snapshot(_: *anyopaque) !metadata.AdminSnapshot {
        const tables = @import("../metadata/table_manager.zig");
        const reconciler = @import("../raft/reconciler.zig");
        const transitions = @import("../metadata/transition_state.zig");
        return .{
            .status = .{ .metadata_group_id = 1, .metadata_incarnation = @splat('1'), .metrics = .{} },
            .tables = @constCast((&[_]tables.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data" }})[0..]),
            .ranges = @constCast((&[_]tables.RangeRecord{
                .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
            })[0..]),
            .stores = @constCast((&[_]tables.StoreRecord{})[0..]),
            .placement_intents = @constCast((&[_]reconciler.PlacementIntent{})[0..]),
            .split_transitions = @constCast((&[_]transitions.SplitTransitionRecord{})[0..]),
            .merge_transitions = @constCast((&[_]transitions.MergeTransitionRecord{})[0..]),
        };
    }
    fn free(_: *anyopaque, _: *metadata.AdminSnapshot) void {}
};
const Adapter = struct {
    left: *Bound,
    right: *Bound,
    fn participant(self: *@This(), group: u64) !txn.LocalTableWriteParticipantWorker {
        const bound = switch (group) {
            7001 => self.left,
            7002 => self.right,
            else => return error.UnknownGroup,
        };
        return txn.LocalTableWriteParticipantWorker.init(bound.source());
    }
    fn worker(self: *@This()) txn.ParticipantWorker {
        return .{ .supports_transaction_producers = true, .ptr = self, .vtable = &.{ .begin_group = begin, .prepare_group = prepare, .resolve_group = resolve, .status_group = status, .acknowledge_group = acknowledge, .resolve_group_until = resolveUntil, .status_group_until = statusUntil } };
    }
    fn begin(ptr: *anyopaque, a: std.mem.Allocator, group: u64, table: []const u8, input: txn.TxnBeginRequest) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        var native = try self.participant(group);
        try native.worker().beginGroup(a, group, table, input);
    }
    fn prepare(ptr: *anyopaque, a: std.mem.Allocator, group: u64, table: []const u8, input: txn.TxnPrepareRequest) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        var native = try self.participant(group);
        try native.worker().prepareGroup(a, group, table, input);
    }
    fn resolve(ptr: *anyopaque, a: std.mem.Allocator, group: u64, table: []const u8, input: txn.TxnResolveRequest) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        var native = try self.participant(group);
        try native.worker().resolveGroup(a, group, table, input);
    }
    fn resolveUntil(ptr: *anyopaque, a: std.mem.Allocator, group: u64, table: []const u8, input: txn.TxnResolveRequest, deadline: u64) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        var native = try self.participant(group);
        try native.worker().resolveGroupUntil(a, group, table, input, deadline);
    }
    fn status(ptr: *anyopaque, a: std.mem.Allocator, group: u64, table: []const u8, id: db_mod.types.TxnId) !db_mod.types.TxnStatus {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        var native = try self.participant(group);
        return native.worker().statusGroup(a, group, table, id);
    }
    fn statusUntil(ptr: *anyopaque, a: std.mem.Allocator, group: u64, table: []const u8, id: db_mod.types.TxnId, deadline: u64) !db_mod.types.TxnStatus {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        var native = try self.participant(group);
        return native.worker().statusGroupUntil(a, group, table, id, deadline);
    }
    fn acknowledge(ptr: *anyopaque, a: std.mem.Allocator, group: u64, table: []const u8, input: txn.TxnAcknowledgeRequest) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        var native = try self.participant(group);
        try native.worker().acknowledgeGroup(a, group, table, input);
    }
};
pub fn run(a: std.mem.Allocator) !void {
    var directory = try local.common_test_directory.TestDirectory.init("producer-native-participants");
    defer directory.cleanup();
    const left_path = try std.fmt.allocPrint(a, "{s}/left", .{directory.path()});
    defer a.free(left_path);
    const right_path = try std.fmt.allocPrint(a, "{s}/right", .{directory.path()});
    defer a.free(right_path);
    const left_options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 7, .shard_id = 7001, .range_id = 7001 }, .online_source_authority = .native, .start_index_workers = false, .start_optional_runtimes = false };
    var right_options = left_options;
    right_options.identity_namespace = .{ .table_id = 7, .shard_id = 7002, .range_id = 7002 };
    // Reopen both physical roots, then replay each original stable transaction.
    for (0..2) |pass| {
        var left_db = try db_mod.DB.open(a, left_path, left_options);
        defer left_db.close();
        var right_db = try db_mod.DB.open(a, right_path, right_options);
        defer right_db.close();
        var left = Bound.init("docs", &left_db);
        left.owner_group_id = 7001;
        var right = Bound.init("docs", &right_db);
        right.owner_group_id = 7002;
        var adapter: Adapter = .{ .left = &left, .right = &right };
        for ([_]@FieldType(db_mod.types.BatchRequest, "source_producer"){ .ordinary, .legacy_cdc }, 0..) |producer, index| {
            const id: db_mod.types.TxnId = @splat(@intCast(50 + index));
            const tables = [_]txn.TableCommitRequest{.{ .table_name = "docs", .source_producer = producer, .writes = &.{ .{ .key = "doc:a", .value = "{}" }, .{ .key = "doc:z", .value = "{}" } } }};
            const outcome = try txn.executeMultiTableCommitWithOptions(a, Catalog.source(), adapter.worker(), id, 100, 200 + index, &tables, .write, null, .{ .retain_terminal = true });
            try std.testing.expect(outcome == .committed);
            try std.testing.expectEqual(@as(usize, 2), outcome.committed.participant_count);
            try std.testing.expectEqual(db_mod.types.TxnStatus.committed, try left_db.getTransactionStatus(id));
            try std.testing.expectEqual(db_mod.types.TxnStatus.committed, try right_db.getTransactionStatus(id));
            try std.testing.expect(try left_db.transactionRetainsCoordinatorAcknowledgement(id));
            try std.testing.expect(!try right_db.transactionRetainsCoordinatorAcknowledgement(id));
            var changed = tables[0];
            changed.source_producer = if (producer == .ordinary) .legacy_cdc else .ordinary;
            try std.testing.expectError(error.SourceTransactionIdentityConflict, txn.executeMultiTableCommitWithOptions(a, Catalog.source(), adapter.worker(), id, 100, 200 + index, &.{changed}, .write, null, .{ .retain_terminal = true }));
        }
        const left_row = (try left_db.get(a, "doc:a")) orelse return error.TestExpectedProducerRow;
        defer a.free(left_row);
        const right_row = (try right_db.get(a, "doc:z")) orelse return error.TestExpectedProducerRow;
        defer a.free(right_row);
        try std.testing.expectEqualStrings("{}", left_row);
        try std.testing.expectEqualStrings("{}", right_row);
        try std.testing.expect(try left_db.get(a, "doc:z") == null);
        try std.testing.expect(try right_db.get(a, "doc:a") == null);
        _ = pass;
    }
}
