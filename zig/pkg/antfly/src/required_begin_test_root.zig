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

pub const antfly_sources = @import("source_owner_physical.zig");
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
const std = @import("std");
const sources = @import("antfly_local_sources");
const batch = sources.api_batch;
const db_mod = sources.storage_db_mod;
const raft_batch = @import("data/raft_batch.zig");
const dispatch = @import("storage/server_transaction_dispatch.zig");
const codec = sources.storage_prepared_transaction_effects;

fn rejected(a: std.mem.Allocator, result: anyerror!batch.OwnedBatchRequest) !void {
    if (result) |value| {
        var unexpected = value;
        unexpected.deinit(a);
        return error.TestExpectedError;
    } else |err| if (err != error.InvalidBatchRequest) return err;
}

fn request(mode: codec.Mode) db_mod.types.BatchRequest {
    return .{ .transaction = .{ .begin = .{
        .txn_id = @splat(0xe5),
        .begin_timestamp = 1,
        .created_at_ns = 2,
        .topology_epoch = 3,
        .retain_terminal = true,
        .participants = &.{"table2:00000004:docs:7"},
        .preparation_requirement = .{ .mode = mode },
    } } };
}

test "required replicated begin preserves mode through Raft encoding apply reopen and replay" {
    const a = std.testing.allocator;
    for (std.meta.tags(codec.Mode)) |mode| {
        var directory = try sources.common_test_directory.TestDirectory.init("required-raft-begin");
        defer directory.cleanup();
        const req = request(mode);
        try std.testing.expectError(error.PreparedTransactionMigrationRequired, @import("data/runtime.zig").DataServer.ensurePreparedTransactionProposalSupported(req));
        var legacy = req;
        legacy.transaction.?.begin.preparation_requirement = null;
        try @import("data/runtime.zig").DataServer.ensurePreparedTransactionProposalSupported(legacy);
        try std.testing.expectEqual(@as(u16, 23), @import("data/runtime.zig").DataServer.requiredRaftBatchProtocolVersion(req));
        const id = req.transaction.?.begin.txn_id;
        const encoded = try raft_batch.encode(a, "docs", req);
        defer a.free(encoded);
        var decoded = try raft_batch.decode(a, encoded);
        defer decoded.deinit(a);
        try std.testing.expectEqual(mode, decoded.batch.req.transaction.?.begin.preparation_requirement.?.mode);
        {
            var db = try db_mod.DB.open(a, directory.path(), .{ .start_optional_runtimes = false });
            defer db.close();
            try std.testing.expectError(error.PreparedTransactionProtocolUnavailable, dispatch.applyReplicatedTransactionMutation(a, &db, "docs", 7, decoded.batch.req));
            try std.testing.expectError(error.PreparedTransactionProtocolDisabled, dispatch.applyReplicatedTransactionMutationAtRaftEntry(a, &db, "docs", 7, decoded.batch.req, .{ .term = 1, .index = 1 }));
            try std.testing.expect((try sources.storage_db_apply_receipts.readOrderedApplyReceipt(a, db.core.store)) == null);
            try db.enablePreparedTransactionProtocol();
            try dispatch.applyReplicatedTransactionMutationAtRaftEntry(a, &db, "docs", 7, decoded.batch.req, .{ .term = 1, .index = 1 });
            var manager = try db.core.initTxnManager();
            defer manager.deinit();
            try std.testing.expectEqual(mode, (try manager.getPreparationDecision(id, null)).requirement.?.mode);
            // A replay must not need fresh catalog admission after its marker
            // is durable. A new entry still has to pass the witness gate.
            try db.core.store.put(sources.storage_db_artifact_inventory.resolver_key, "{\"version\":6,\"resolvers\":[]}");
            try dispatch.applyReplicatedTransactionMutationAtRaftEntry(a, &db, "docs", 7, decoded.batch.req, .{ .term = 1, .index = 1 });
            try std.testing.expectError(error.InvalidPreparedTransactionProtocol, dispatch.applyReplicatedTransactionMutationAtRaftEntry(a, &db, "docs", 7, decoded.batch.req, .{ .term = 1, .index = 2 }));
            try std.testing.expectEqual(@as(u64, 1), (try sources.storage_db_apply_receipts.readOrderedApplyReceipt(a, db.core.store)).?.index);
        }
        var reopened = try db_mod.DB.open(a, directory.path(), .{ .start_optional_runtimes = false });
        defer reopened.close();
        try dispatch.applyReplicatedTransactionMutationAtRaftEntry(a, &reopened, "docs", 7, decoded.batch.req, .{ .term = 1, .index = 1 });
        var manager = try reopened.core.initTxnManager();
        defer manager.deinit();
        try std.testing.expectEqual(mode, (try manager.getPreparationDecision(id, null)).requirement.?.mode);
        var changed = request(if (mode == .deferred) .synchronous else .deferred);
        try std.testing.expectError(error.PreparationContractConflict, dispatch.applyReplicatedTransactionMutationAtRaftEntry(a, &reopened, "docs", 7, changed, .{ .term = 1, .index = 2 }));
        try std.testing.expectEqual(@as(u64, 1), (try sources.storage_db_apply_receipts.readOrderedApplyReceipt(a, reopened.core.store)).?.index);
        changed.transaction.?.begin.preparation_requirement = .{ .mode = mode };
        try dispatch.applyReplicatedTransactionMutationAtRaftEntry(a, &reopened, "docs", 7, changed, .{ .term = 1, .index = 2 });
        try std.testing.expectEqual(@as(u64, 2), (try sources.storage_db_apply_receipts.readOrderedApplyReceipt(a, reopened.core.store)).?.index);
    }
}

test "required replicated begin codec rejects stripped phase missing mode and public ingress" {
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            for (std.meta.tags(codec.Mode)) |mode| {
                const encoded = try batch.encodeBatchRequest(a, request(mode));
                defer a.free(encoded);
                try std.testing.expect(std.mem.indexOf(u8, encoded, "begin_required_v1") != null);
                var parsed = try batch.parseInternalBatchRequest(a, encoded);
                defer parsed.deinit(a);
                try std.testing.expectEqual(mode, parsed.req.transaction.?.begin.preparation_requirement.?.mode);
                try rejected(a, batch.parseBatchRequest(a, encoded));
                const stripped = try std.mem.replaceOwned(u8, a, encoded, "begin_required_v1", "begin");
                defer a.free(stripped);
                try rejected(a, batch.parseInternalBatchRequest(a, stripped));
                const unknown = try std.mem.replaceOwned(u8, a, encoded, @tagName(mode), "unknown");
                defer a.free(unknown);
                try rejected(a, batch.parseInternalBatchRequest(a, unknown));
                const missing = try std.mem.replaceOwned(u8, a, encoded, "preparation_mode", "stripped_mode");
                defer a.free(missing);
                try rejected(a, batch.parseInternalBatchRequest(a, missing));
            }
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
    try std.testing.expectEqual(@as(u16, 23), @import("common/data_raft_protocol.zig").batch_required_preparation_protocol_version);
    try std.testing.expect(raft_batch.protocol_version >= 23);
    const barrier = try raft_batch.encodeProtocolBarrier(std.testing.allocator, "docs", 23);
    defer std.testing.allocator.free(barrier);
    var decoded = try raft_batch.decode(std.testing.allocator, barrier);
    defer decoded.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(?u16, 23), decoded.protocol_barrier_version);
    try std.testing.expectError(error.UnsupportedRaftBatchProtocolVersion, raft_batch.decode(std.testing.allocator, "{\"table\":\"docs\",\"protocol_barrier\":24,\"batch\":null}"));
}
