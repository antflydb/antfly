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
    try std.testing.expectError(error.UnsupportedRaftBatchProtocolVersion, raft_batch.decode(std.testing.allocator, "{\"table\":\"docs\",\"protocol_barrier\":25,\"batch\":null}"));
}

test "required replicated begin prepare and final seal persist accepted effects before decision" {
    const a = std.testing.allocator;
    for (std.meta.tags(codec.Mode)) |mode| {
        var directory = try sources.common_test_directory.TestDirectory.init("required-raft-seal");
        defer directory.cleanup();
        const id = request(mode).transaction.?.begin.txn_id;
        {
            var db = try db_mod.DB.open(a, directory.path(), .{ .start_optional_runtimes = false });
            defer db.close();
            try db.enablePreparedTransactionProtocol();
            try dispatch.applyReplicatedTransactionMutationAtRaftEntry(a, &db, "docs", 7, request(mode), .{ .term = 1, .index = 1 });
            const prepare: db_mod.types.BatchRequest = .{
                .writes = &.{.{ .key = "doc:sealed", .value = "{}" }},
                .transaction = .{ .prepare = .{ .txn_id = id, .topology_epoch = 3, .preparation_requirement = .{ .mode = mode } } },
            };
            var wrong = prepare;
            wrong.transaction.?.prepare.preparation_requirement = .{ .mode = if (mode == .deferred) .synchronous else .deferred };
            try std.testing.expectError(error.PreparationContractConflict, dispatch.applyReplicatedTransactionMutationAtRaftEntry(a, &db, "docs", 7, wrong, .{ .term = 1, .index = 2 }));
            try std.testing.expectEqual(@as(u64, 1), (try sources.storage_db_apply_receipts.readOrderedApplyReceipt(a, db.core.store)).?.index);
            const bytes = try raft_batch.encode(a, "docs", prepare);
            defer a.free(bytes);
            var parsed = try raft_batch.decode(a, bytes);
            defer parsed.deinit(a);
            try std.testing.expectEqual(@as(u16, 24), @import("data/runtime.zig").DataServer.requiredRaftBatchProtocolVersion(parsed.batch.req));
            try std.testing.expectError(error.PreparedTransactionMigrationRequired, @import("data/runtime.zig").DataServer.ensurePreparedTransactionProposalSupported(parsed.batch.req));
            try dispatch.applyReplicatedTransactionMutationAtRaftEntry(a, &db, "docs", 7, parsed.batch.req, .{ .term = 1, .index = 2 });
            var manager = try db.core.initTxnManager();
            defer manager.deinit();
            const contract = try manager.preparedEffectsContract(id, try db.core.preparedEffectsCatalogDigest(null), mode, null);
            var stale = contract;
            stale.intent_revision += 1;
            const bad = try codec.encode(a, .{ .contract = stale, .effects = .{} }, null);
            defer a.free(bad);
            try std.testing.expectError(error.PreparedEffectsMismatch, db.sealReplicatedTransactionEffectsAtOrderedReceipt(id, bad, mode, .{ .term = 1, .index = 3 }, null));
            try std.testing.expectEqual(@as(u64, 2), (try sources.storage_db_apply_receipts.readOrderedApplyReceipt(a, db.core.store)).?.index);
            const frame = try codec.encode(a, .{ .contract = contract, .effects = .{} }, null);
            defer a.free(frame);
            const seal: db_mod.types.BatchRequest = .{ .transaction = .{ .prepare = .{ .txn_id = id, .topology_epoch = 3, .preparation_requirement = .{ .mode = mode }, .sealed_effects = frame } } };
            const encoded = try raft_batch.encode(a, "docs", seal);
            defer a.free(encoded);
            var decoded = try raft_batch.decode(a, encoded);
            defer decoded.deinit(a);
            try std.testing.expectEqualSlices(u8, frame, decoded.batch.req.transaction.?.prepare.sealed_effects.?);
            try std.testing.expectError(error.PreparedTransactionProtocolUnavailable, dispatch.applyReplicatedTransactionMutation(a, &db, "docs", 7, decoded.batch.req));
            try dispatch.applyReplicatedTransactionMutationAtRaftEntry(a, &db, "docs", 7, decoded.batch.req, .{ .term = 1, .index = 3 });
            try std.testing.expect(try manager.hasPreparedEffects(id));
            try std.testing.expect((try db.get(a, "doc:sealed")) == null);
            // A different result can never replace the first accepted frame.
            const other = try codec.encode(a, .{ .contract = contract, .effects = .{ .artifact_writes = @constCast(&[_]db_mod.types.BatchWrite{.{ .key = "artifact:other", .value = "other" }}) } }, null);
            defer a.free(other);
            try std.testing.expectError(error.PreparedEffectsConflict, db.sealReplicatedTransactionEffectsAtOrderedReceipt(id, other, mode, .{ .term = 1, .index = 4 }, null));
            try std.testing.expectEqual(@as(u64, 3), (try sources.storage_db_apply_receipts.readOrderedApplyReceipt(a, db.core.store)).?.index);
            // Same accepted bytes may advance a later ordered retry without
            // reacquiring the frame's durable grant.
            try db.sealReplicatedTransactionEffectsAtOrderedReceipt(id, frame, mode, .{ .term = 1, .index = 4 }, null);
        }
        var reopened = try db_mod.DB.open(a, directory.path(), .{ .start_optional_runtimes = false });
        defer reopened.close();
        var manager = try reopened.core.initTxnManager();
        defer manager.deinit();
        var plan = (try manager.loadPreparedEffects(a, id, null)).?;
        defer plan.deinit();
        try std.testing.expectEqual(mode, plan.decoded.plan.contract.mode);
        // Replaying PREPARE after the seal must not try to rewrite frozen intents.
        try reopened.writeReplicatedTransactionWithRequirementAtOrderedReceipt(id, .{}, .{ .term = 1, .index = 2 }, .{ .mode = mode });
        try reopened.sealReplicatedTransactionEffectsAtOrderedReceipt(id, "replay ignores retired command bytes", mode, .{ .term = 1, .index = 3 }, null);
        try reopened.resolveReplicatedTransactionAtOrderedReceipt(id, .committed, 100, .write, .none, .{ .term = 1, .index = 5 }, null);
        try std.testing.expect(!try manager.hasPreparedEffects(id));
        try std.testing.expectEqual(mode, (try manager.getPreparationDecision(id, null)).requirement.?.mode);
        const value = (try reopened.get(a, "doc:sealed")).?;
        defer a.free(value);
        try std.testing.expectEqualStrings("{}", value);
        try reopened.sealReplicatedTransactionEffectsAtOrderedReceipt(id, "already resolved", mode, .{ .term = 1, .index = 4 }, null);
    }
}

test "required replicated begin seal codec rejects downgrade and damaged frames under allocation failure" {
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            const id: db_mod.types.TxnId = @splat(0xe5);
            for (std.meta.tags(codec.Mode)) |mode| {
                const frame = try codec.encode(a, .{ .contract = .{ .txn_id = id, .intent_revision = 1, .intent_digest = @splat(1), .catalog_digest = @splat(2), .mode = mode }, .effects = .{} }, null);
                defer a.free(frame);
                const req: db_mod.types.BatchRequest = .{ .transaction = .{ .prepare = .{ .txn_id = id, .topology_epoch = 3, .preparation_requirement = .{ .mode = mode }, .sealed_effects = frame } } };
                var mixed = req;
                mixed.relational_repair = true;
                try std.testing.expectError(error.InvalidBatchRequest, batch.encodeBatchRequest(a, mixed));
                try std.testing.expectError(error.InvalidBatchRequest, dispatch.applyReplicatedTransactionMutationAtRaftEntry(a, undefined, "docs", 7, mixed, .{ .term = 1, .index = 1 }));
                const encoded = try batch.encodeBatchRequest(a, req);
                defer a.free(encoded);
                var decoded = try batch.parseInternalBatchRequest(a, encoded);
                defer decoded.deinit(a);
                try std.testing.expectEqualSlices(u8, frame, decoded.req.transaction.?.prepare.sealed_effects.?);
                try rejected(a, batch.parseBatchRequest(a, encoded));
                const with_control = try std.fmt.allocPrint(a, "{{\"_relational_repair\":true,{s}", .{encoded[1..]});
                defer a.free(with_control);
                try rejected(a, batch.parseInternalBatchRequest(a, with_control));
                const stripped = try std.mem.replaceOwned(u8, a, encoded, "seal_required_v1", "prepare");
                defer a.free(stripped);
                try rejected(a, batch.parseInternalBatchRequest(a, stripped));
                const missing = try std.mem.replaceOwned(u8, a, encoded, "prepared_effects_base64", "stripped_effects");
                defer a.free(missing);
                try rejected(a, batch.parseInternalBatchRequest(a, missing));
                const corrupt = try a.dupe(u8, encoded);
                defer a.free(corrupt);
                const prefix = "\"prepared_effects_base64\":\"";
                corrupt[(std.mem.indexOf(u8, corrupt, prefix) orelse return error.TestUnexpectedResult) + prefix.len] = '!';
                try rejected(a, batch.parseInternalBatchRequest(a, corrupt));
            }
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}
