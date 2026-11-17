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

const std = @import("std");
const abi = @import("kernel_owner_abi");
const client = @import("kernel_owner_client.zig");
const local = @import("antfly_local_sources");
const db_mod = local.storage_db_db;
const policy = local.storage_source_producer_policy;
const ordered = local.storage_source_legacy_terminal_ordered;
const inventory = local.storage_source_legacy_inventory;
const completion = local.api_local_table_writes.table_write_source.history_resolution_wire.completion;

test "storage owner legacy terminal resolution crosses native ABI and survives reopen" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/legacy-terminal-owner", .{tmp.sub_path});
    defer a.free(path);
    const namespace: local.storage_db_doc_identity_namespace.Namespace = .{ .table_id = 7, .shard_id = 11, .range_id = 13 };
    const options: db_mod.OpenOptions = .{ .identity_namespace = namespace, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    const id: [16]u8 = @splat(231);
    const pending_id: [16]u8 = @splat(232);
    var command: policy.LegacyTerminalCommand = undefined;
    var pending: policy.LegacyTerminalCommand = undefined;
    {
        var db = try db_mod.DB.open(a, path, options);
        defer db.close();
        try db.applyOrderedCommittedMutation(.{ .source_producer = .ordinary, .writes = &.{.{ .key = "seed", .value = "{}" }}, .sync_level = .write }, .{ .term = 1, .index = 1 }, false);
        _ = try db.beginTransactionScoped(id, 100, 101, &.{}, true, true, null);
        try db.writeTransaction(id, .{ .writes = &.{.{ .key = "recovered", .value = "{\"value\":1}" }} });
        _ = try db.beginTransactionScoped(pending_id, 100, 101, &.{}, true, true, null);
        const owner: completion.RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = local.storage_db_online_source_contract.namespaceBytes(namespace), .table_binding = @splat(2), .root_generation = db.core.root_generation, .group_id = 7001, .topology_epoch = 1 };
        const key = completion.record_prefix.* ++ id;
        var read = try db.core.store.beginReadTxn();
        const raw = try a.dupe(u8, try read.get(&key));
        pending = .{ .version = 1, .owner = owner, .proof = .{ .root = try local.storage_source_scope.Basis.fromOwner(owner), .transaction_id = pending_id, .record_digest = completion.bytesDigest(try read.get(&(completion.record_prefix.* ++ pending_id))) } };
        read.abort();
        defer a.free(raw);
        raw[0] = 1; // canonical committed status in this historic fixture
        std.mem.writeInt(u64, raw[9..17], 200, .little);
        std.mem.writeInt(u64, raw[25..33], 201, .little);
        try db.core.store.putBatch(&.{.{ .key = &key, .value = raw }}, &.{});
        command = .{ .version = 1, .owner = owner, .proof = .{ .root = try local.storage_source_scope.Basis.fromOwner(owner), .transaction_id = id, .record_digest = completion.bytesDigest(raw) } };
        _ = try db.applyOrderedSourceProducerClosure(owner, .{ .term = 1, .index = 2 }, false);
    }
    const open: abi.OpenRequest = .{ .path = .fromSlice(path), .table_name = .fromSlice("docs"), .group_id = command.owner.group_id, .has_identity_namespace = 1, .identity_table_id = namespace.table_id, .identity_shard_id = namespace.shard_id, .identity_range_id = namespace.range_id, .lsm_root_generation = command.owner.root_generation, .schema_json = .fromSlice("{}"), .indexes_json = .fromSlice("{}") };
    {
        var native = try client.Owner.open(open);
        defer native.deinit();
        try checkInventory(&native, a, command.owner, 2, false);
        const rejected = try native.orderedLegacyTerminalResolution(a, "docs", pending, 1, 3);
        try std.testing.expect(rejected == .rejected and rejected.rejected == .unresolved);
        var wrong = command;
        wrong.owner.group_id += 1;
        wrong.proof.root.group_id += 1;
        try std.testing.expectError(error.HistoryResynchronizationRequired, native.orderedLegacyTerminalResolution(a, "docs", wrong, 1, 3));
        const committed = try native.orderedLegacyTerminalResolution(a, "docs", command, 1, 4);
        try std.testing.expect(committed == .applied and committed.applied.kind == .committed and committed.applied.timestamp == 200);
        try std.testing.expectEqualDeep(committed, try native.orderedLegacyTerminalResolution(a, "docs", command, 1, 4));
    }
    {
        var native = try client.Owner.open(open);
        defer native.deinit();
        const reopened = try native.orderedLegacyTerminalResolution(a, "docs", command, 1, 4);
        try std.testing.expect(reopened == .applied and reopened.applied.kind == .committed and reopened.applied.timestamp == 200);
        const query: ordered.Query = .{ .version = 1, .owner = command.owner, .command = command, .entry = .{ .term = 1, .index = 4 }, .minimum_applied_index = 4 };
        try checkInventory(&native, a, command.owner, 4, true);
        const observed = try native.legacyTerminalOutcome(a, "docs", query);
        try std.testing.expectEqualDeep(reopened, observed.outcome);
        try std.testing.expectEqual(@as(u64, 4), observed.cut.index);
        try std.testing.expectEqualDeep(observed, try native.legacyTerminalOutcome(a, "docs", query));
        var future = query;
        future.minimum_applied_index = 5;
        try std.testing.expectError(error.StorageReadTemporarilyUnavailable, native.legacyTerminalOutcome(a, "docs", future));
        var expired = query;
        expired.command = pending;
        expired.entry.index = 3;
        try std.testing.expect((try native.legacyTerminalOutcome(a, "docs", expired)).outcome == .superseded);
        var read_request: abi.JsonOperationRequest = .{ .version = abi.abi_version - 1 };
        var read_response: abi.OwnedBytes = .{ .len = 99 };
        try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_legacy_terminal_outcome(null, &read_request, &read_response));
        try std.testing.expectEqual(@as(usize, 0), read_response.len);
        read_request.version = abi.abi_version;
        read_request.request_json.len = ordered.max_query_bytes + 1;
        try std.testing.expectEqual(abi.Status.invalid_argument, abi.antfly_storage_owner_legacy_terminal_outcome(null, &read_request, &read_response));

        try std.testing.expect(try native.orderedLegacyTerminalResolution(a, "docs", pending, 1, 3) == .superseded);
        var forged = command;
        forged.proof.record_digest[0] ^= 1;
        try std.testing.expectError(error.ConflictingOrderedApplyReceipt, native.orderedLegacyTerminalResolution(a, "docs", forged, 1, 4));
        var response: abi.OwnedBytes = .{ .len = 99 };
        var request: abi.ReplicatedBatchAtRaftEntryRequest = .{ .version = abi.abi_version - 1 };
        try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_ordered_legacy_terminal_resolution(null, &request, &response));
        try std.testing.expectEqual(@as(usize, 0), response.len);
        request.version = abi.abi_version;
        request.raft_term = 1;
        request.raft_index = 5;
        request.request_json.len = policy.max_legacy_terminal_command_bytes + 1;
        try std.testing.expectEqual(abi.Status.invalid_argument, abi.antfly_storage_owner_ordered_legacy_terminal_resolution(null, &request, &response));
    }
    {
        var native = try client.Owner.open(open);
        defer native.deinit();
        try checkInventory(&native, a, command.owner, 4, true);
    }
    {
        var db = try db_mod.DB.open(a, path, options);
        defer db.close();
        const row = (try db.get(a, "recovered")) orelse return error.TestExpectedOrdinaryDocument;
        defer a.free(row);
        try std.testing.expectEqualStrings("{\"value\":1}", row);
        try std.testing.expectEqual(@as(u64, 4), (try db.orderedApplyReceipt()).?.index);
        try std.testing.expectEqual(local.storage_transactions.TxnStatus.pending, try db.getTransactionStatus(pending_id));
    }
}

fn checkInventory(native: *client.Owner, a: std.mem.Allocator, owner: completion.RoutedOwner, index: u64, resolved: bool) !void {
    const query: inventory.Query = .{ .version = 1, .owner = owner, .after = null, .max_records = 256, .minimum_applied_index = index };
    var observed = try native.legacyRecoveryInventory(a, "docs", query);
    defer observed.deinit(a);
    try std.testing.expectEqual(index, observed.cut.index);
    try std.testing.expectEqual(@as(usize, 2), observed.page.scanned_records);
    try std.testing.expectEqual(@as(usize, if (resolved) 1 else 2), observed.page.items.len);
    try std.testing.expectEqualDeep(@as(?[16]u8, @splat(232)), observed.page.next_after);
    try std.testing.expect(observed.page.reached_end);
    var wrong = query;
    wrong.owner.group_id += 1;
    try std.testing.expectError(error.HistoryResynchronizationRequired, native.legacyRecoveryInventory(a, "docs", wrong));
    wrong = query;
    wrong.minimum_applied_index += 1;
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, native.legacyRecoveryInventory(a, "docs", wrong));
    wrong = query;
    wrong.after = @splat(232);
    var empty = try native.legacyRecoveryInventory(a, "docs", wrong);
    defer empty.deinit(a);
    try std.testing.expect(empty.page.reached_end and empty.page.items.len == 0 and empty.page.scanned_records == 0);
    try std.testing.expectEqualDeep(wrong.after, empty.page.next_after);
    var response: abi.OwnedBytes = .{ .len = 99 };
    var request: abi.JsonOperationRequest = .{ .version = abi.abi_version - 1 };
    try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_legacy_recovery_inventory(null, &request, &response));
    try std.testing.expectEqual(@as(usize, 0), response.len);
    request.version = abi.abi_version;
    request.request_json.len = inventory.max_query_bytes + 1;
    try std.testing.expectEqual(abi.Status.invalid_argument, abi.antfly_storage_owner_legacy_recovery_inventory(null, &request, &response));
    try std.testing.expectEqual(@as(usize, 0), response.len);
}
