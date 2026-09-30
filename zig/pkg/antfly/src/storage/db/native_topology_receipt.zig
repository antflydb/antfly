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

//! Bounded native topology-control deduplication. One slot per role/action
//! retains the latest fence; the effect, slot and native clock share a txn.
const std = @import("std");
const topology = @import("relational_integrity_topology_contract.zig");
const position = @import("receipt_position.zig");
const authority = @import("../source_authority.zig");

pub const prefix = "\x00\x00__metadata__:native_topology_receipt:v1:";
const encoded_len = 4 + 136 + 32 + position.Position.encoded_len + 32;

pub fn isKey(candidate: []const u8) bool {
    return std.mem.startsWith(u8, candidate, prefix);
}

fn key(command: topology.Command) [prefix.len + 2]u8 {
    var result: [prefix.len + 2]u8 = undefined;
    @memcpy(result[0..prefix.len], prefix);
    result[prefix.len] = @intCast(@intFromEnum(command.fence.role));
    result[prefix.len + 1] = @intCast(@intFromEnum(command.action));
    return result;
}

pub const Prepared = struct { receipt: position.Native, duplicate: bool };

pub fn supports(command: topology.Command) bool {
    return switch (command.fence.role) {
        .rewrite_source => switch (command.action) {
            .begin, .cancel, .abort_transition, .seal_graph_retirement, .seal_generation_handoff => true,
            else => false,
        },
        .rewrite_destination => command.action == .install_generation_handoff,
        .truncate_parent => switch (command.action) {
            .begin, .cancel, .stage_parent_retirement, .activate_parent_retirement, .acknowledge_parent_retirement => true,
            else => false,
        },
        else => false,
    };
}

/// Native receipt envelopes cannot smuggle unrelated effects. Comparing the
/// canonical request with its control-only projection also covers new fields.
pub fn validateRequest(alloc: std.mem.Allocator, request: @import("types.zig").BatchRequest) !void {
    const command = request.relational_topology orelse return error.InvalidBatchRequest;
    if (!supports(command)) return error.InvalidBatchRequest;
    const expected: @import("types.zig").BatchRequest = .{
        .relational_topology = command,
        .restore_staging_scope = request.restore_staging_scope,
        .restore_staging_plan_id = request.restore_staging_plan_id,
        .sync_level = request.sync_level,
    };
    const actual_bytes = try std.json.Stringify.valueAlloc(alloc, request, .{});
    defer alloc.free(actual_bytes);
    const expected_bytes = try std.json.Stringify.valueAlloc(alloc, expected, .{});
    defer alloc.free(expected_bytes);
    if (!std.mem.eql(u8, actual_bytes, expected_bytes)) return error.InvalidBatchRequest;
}

pub fn stage(alloc: std.mem.Allocator, txn: anytype, command: topology.Command, replay: ?position.Native) !Prepared {
    if (!supports(command)) return error.InvalidBatchRequest;
    const namespace = @import("online_source_contract.zig").namespaceBytes(command.fence.namespace);
    const owner = try authority.require(txn, .native, namespace);
    if (replay) |value| {
        try value.validate();
        if (!value.namespace.eql(command.fence.namespace)) return error.IdentityNamespaceMismatch;
    }
    const serialized = try std.json.Stringify.valueAlloc(alloc, command, .{});
    defer alloc.free(serialized);
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(serialized, &digest, .{});
    const slot = key(command);
    const previous = txn.get(&slot) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    if (previous) |bytes| {
        if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..4], "NTR1")) return error.InvalidControlReceiptPosition;
        var checksum: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(bytes[0 .. encoded_len - 32], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, bytes[encoded_len - 32 ..])) return error.InvalidControlReceiptPosition;
        const fence = try topology.Fence.decode(bytes[4..140]);
        const stamp = try position.Position.decode(bytes[172..205]);
        if (stamp != .native) return error.InvalidControlReceiptPosition;
        try stamp.requireNamespace(fence.namespace);
        // Authenticated generation adoption binds a new durable namespace and
        // resets its native clock. Copied prior-incarnation slots are inert;
        // they cannot authorize retries or constrain the new owner's epochs.
        if (fence.namespace.eql(command.fence.namespace)) {
            if (owner.sequence < stamp.native.sequence) return error.InvalidControlReceiptPosition;
            if (fence.eql(command.fence)) {
                if (!std.mem.eql(u8, &digest, bytes[140..172])) return error.IntegrityTopologyChanged;
                if (replay) |value| if (!std.meta.eql(value, stamp.native)) return error.InvalidControlReceiptPosition;
                return .{ .receipt = stamp.native, .duplicate = true };
            }
            if (command.fence.admission_epoch <= fence.admission_epoch) return error.IntegrityTopologyChanged;
        }
    }
    const sequence = try authority.advance(txn, namespace, if (replay) |value| value.sequence else null);
    const receipt: position.Native = .{ .namespace = command.fence.namespace, .sequence = sequence };
    var bytes: [encoded_len]u8 = undefined;
    @memcpy(bytes[0..4], "NTR1");
    @memcpy(bytes[4..140], &try command.fence.encode());
    @memcpy(bytes[140..172], &digest);
    @memcpy(bytes[172..205], &try (position.Position{ .native = receipt }).encode());
    std.crypto.hash.Blake3.hash(bytes[0..205], bytes[205..237], .{});
    try txn.put(&slot, &bytes);
    return .{ .receipt = receipt, .duplicate = false };
}

test "native topology receipts survive restart and exact standby replay without Raft watermarks" {
    const alloc = std.testing.allocator;
    const db_mod = @import("db.zig");
    const effects = @import("../hot_standby/effects.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const primary_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/native-topology-primary", .{tmp.sub_path});
    defer alloc.free(primary_path);
    const replica_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/native-topology-replica", .{tmp.sub_path});
    defer alloc.free(replica_path);
    const log_path = try std.fmt.allocPrintSentinel(alloc, ".zig-cache/tmp/{s}/native-topology-log", .{tmp.sub_path}, 0);
    defer alloc.free(log_path);
    const slots_path = try std.fmt.allocPrintSentinel(alloc, ".zig-cache/tmp/{s}/native-topology-slots", .{tmp.sub_path}, 0);
    defer alloc.free(slots_path);
    var stream = try @import("../hot_standby/primary.zig").Primary.open(alloc, log_path, slots_path, .{ .cluster_id = 1, .timeline_id = 1, .epoch = 1, .table_id = 11, .shard_id = 12 }, .{});
    defer stream.close();
    const ns: @import("doc_identity_namespace.zig").Namespace = .{ .table_id = 11, .shard_id = 12, .range_id = 13 };
    const options: db_mod.OpenOptions = .{ .identity_namespace = ns, .online_source_authority = .native, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    var primary_options = options;
    primary_options.ha_async_batch_mirror = .{ .primary = &stream };
    var primary = try db_mod.DB.open(alloc, primary_path, primary_options);
    var primary_open = true;
    defer if (primary_open) primary.close();
    var replica = try db_mod.DB.open(alloc, replica_path, options);
    defer replica.close();
    try primary.setSchemaJson(alloc, "{}");
    try replica.setSchemaJson(alloc, "{}");
    const identity = try primary.relationalTopologyIdentity();
    const fence: topology.Fence = .{ .namespace = ns, .role = .rewrite_source, .owner_group_id = 12, .peer_group_id = 14, .transition_id = 15, .attempt = 1, .admission_epoch = identity.next_epoch, .catalog_digest = identity.catalog_digest };
    const begin: @import("types.zig").BatchRequest = .{ .relational_topology = .{ .fence = fence, .action = .begin } };
    try std.testing.expectError(error.OnlineSourceScopeChanged, primary.batchRaftReplicatedApply(begin, .{ .term = 1, .index = 1 }));
    try primary.batch(begin);
    try primary.batch(begin); // Lost acknowledgement, identical position.
    {
        var read = try primary.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(u64, 1), (try authority.load(&read)).?.sequence);
    }
    try std.testing.expectEqual(@as(u64, 2), stream.lastLsn());
    var first = (try stream.log.entryAt(alloc, 1)).?;
    defer first.deinit(alloc);
    var duplicate = (try stream.log.entryAt(alloc, 2)).?;
    defer duplicate.deinit(alloc);
    var decoded = try effects.decodeBatchMutationRequest(alloc, first.record);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u64, 1), decoded.value.native_topology_position.?.sequence);
    try std.testing.expect(decoded.value.graph_retirement_raft_entry == null);
    try replica.applyHAReplicationRecord(first.record);
    try replica.applyHAReplicationRecord(first.record);
    try replica.applyHAReplicationRecord(duplicate.record);
    try std.testing.expect((try replica.relationalTopologyStatus()).fence.?.eql(fence));
    // Invalid seal must roll back its clock and dedup record with the effect.
    var invalid = begin;
    invalid.relational_topology.?.action = .seal_generation_handoff;
    invalid.relational_topology.?.generation_handoff_seal = .{ .plan_digest = @splat(1), .admissions_digest = @splat(2), .retired_digest = @splat(3), .retired_count = 0 };
    try std.testing.expectError(error.GenerationHandoffIntentMissing, primary.batch(invalid));
    try std.testing.expectEqual(@as(u64, 2), stream.lastLsn());
    {
        var read = try primary.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(u64, 1), (try authority.load(&read)).?.sequence);
    }
    var cancel = begin;
    cancel.relational_topology.?.action = .cancel;
    try primary.batch(cancel);
    var canceled = (try stream.log.entryAt(alloc, 3)).?;
    defer canceled.deinit(alloc);
    try replica.applyHAReplicationRecord(canceled.record);
    primary.close();
    primary_open = false;
    primary = try db_mod.DB.open(alloc, primary_path, primary_options);
    primary_open = true;
    // Delayed begin retries cannot resurrect a canceled fence.
    try primary.batch(begin);
    try primary.batch(cancel);
    try std.testing.expect((try primary.relationalTopologyStatus()).fence == null);
    try std.testing.expect((try replica.relationalTopologyStatus()).fence == null);
    try std.testing.expect((try primary.raftAppliedEntry()) == null);
    try std.testing.expect((try replica.raftAppliedEntry()) == null);
    for ([_]*db_mod.DB{ &primary, &replica }) |owner| {
        var read = try owner.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(u64, 2), (try authority.load(&read)).?.sequence);
    }
    var smuggled = begin;
    smuggled.writes = &.{.{ .key = "bad", .value = "{}" }};
    try std.testing.expectError(error.InvalidBatchRequest, effects.encodeNativeTopologyMutationRequestAlloc(alloc, smuggled, .{ .namespace = ns, .sequence = 3 }));
    try std.testing.expectError(error.IdentityNamespaceMismatch, effects.encodeNativeTopologyMutationRequestAlloc(alloc, begin, .{ .namespace = .{ .table_id = 11, .shard_id = 99, .range_id = 13 }, .sequence = 3 }));
    // A new stream LSN does not authorize changing a previously committed
    // native receipt position. Reject it without advancing either cursor.
    const forged_bytes = try effects.encodeNativeTopologyMutationRequestAlloc(alloc, begin, .{ .namespace = ns, .sequence = 99 });
    defer alloc.free(forged_bytes);
    var forged = canceled.record;
    forged.lsn = 4;
    forged.previous_lsn = 3;
    forged.payload = forged_bytes;
    try std.testing.expectError(error.InvalidControlReceiptPosition, replica.applyHAReplicationRecord(forged));
    try std.testing.expectEqual(@as(u64, 3), try replica.haAppliedReplicationLsn());
    {
        var txn = try replica.core.store.beginWriteTxn();
        defer txn.abort();
        const adopted: @import("doc_identity_namespace.zig").Namespace = .{ .table_id = 21, .shard_id = 22, .range_id = 23 };
        const namespace_bytes = @import("online_source_contract.zig").namespaceBytes(adopted);
        try txn.put(&@import("../internal_keys.zig").identity_namespace_key, &namespace_bytes);
        try authority.bind(&txn, .native, namespace_bytes);
        var adopted_command = begin.relational_topology.?;
        adopted_command.fence.namespace = adopted;
        const prepared = try stage(alloc, &txn, adopted_command, null);
        try std.testing.expectEqual(@as(u64, 1), prepared.receipt.sequence);
        try std.testing.expect(!prepared.duplicate);
        try std.testing.expect(prepared.receipt.namespace.eql(adopted));
    }
}
