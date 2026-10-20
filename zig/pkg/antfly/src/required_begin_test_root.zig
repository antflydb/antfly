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

pub const consumer_tests_only = true;
pub const capi_dependencies = @import("capi_dependencies.zig");
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
        try std.testing.expectEqual(@as(u16, 26), @import("data/runtime.zig").DataServer.requiredRaftBatchProtocolVersion(req));
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
            const catalog_json = try std.fmt.allocPrint(a, "{{\"version\":{d},\"resolvers\":[]}}", .{sources.storage_db_catalog_resolver_catalog.prepared_transaction_reader_version});
            defer a.free(catalog_json);
            try db.core.store.put(sources.storage_db_artifact_inventory.resolver_key, catalog_json);
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

test "required replicated begin rejects stale catalog epochs after reopen without advancing its receipt" {
    const a = std.testing.allocator;
    for (std.meta.tags(codec.Mode)) |mode| {
        var directory = try sources.common_test_directory.TestDirectory.init("required-raft-stale-epoch");
        defer directory.cleanup();
        {
            var db = try db_mod.DB.open(a, directory.path(), .{ .start_optional_runtimes = false });
            defer db.close();
            try db.enablePreparedTransactionProtocol();
            const stale = try std.fmt.allocPrint(a, "{{\"version\":{d},\"resolvers\":[]}}", .{sources.storage_db_catalog_resolver_catalog.prepared_transaction_reader_version - 1});
            defer a.free(stale);
            try db.core.store.put(sources.storage_db_artifact_inventory.resolver_key, stale);
        }
        var reopened = try db_mod.DB.open(a, directory.path(), .{ .start_optional_runtimes = false });
        defer reopened.close();
        const req = request(mode);
        try std.testing.expectError(error.InvalidPreparedTransactionProtocol, dispatch.applyReplicatedTransactionMutationAtRaftEntry(a, &reopened, "docs", 7, req, .{ .term = 1, .index = 1 }));
        var manager = try reopened.core.initTxnManager();
        defer manager.deinit();
        try std.testing.expectError(error.TxnNotFound, manager.getTransactionStatus(req.transaction.?.begin.txn_id));
        try std.testing.expect((try sources.storage_db_apply_receipts.readOrderedApplyReceipt(a, reopened.core.store)) == null);
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
    try std.testing.expectEqual(@as(u16, 26), @import("common/data_raft_protocol.zig").batch_required_preparation_protocol_version);
    try std.testing.expect(raft_batch.protocol_version >= 26);
    const barrier = try raft_batch.encodeProtocolBarrier(std.testing.allocator, "docs", 26);
    defer std.testing.allocator.free(barrier);
    var decoded = try raft_batch.decode(std.testing.allocator, barrier);
    defer decoded.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(?u16, 26), decoded.protocol_barrier_version);
    const unsupported = try std.fmt.allocPrint(std.testing.allocator, "{{\"table\":\"docs\",\"protocol_barrier\":{d},\"batch\":null}}", .{raft_batch.protocol_version + 1});
    defer std.testing.allocator.free(unsupported);
    if (raft_batch.decode(std.testing.allocator, unsupported)) |value| {
        var unexpected = value;
        defer unexpected.deinit(std.testing.allocator);
        return error.TestExpectedError;
    } else |err| try std.testing.expectEqual(error.UnsupportedRaftBatchProtocolVersion, err);
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
            try std.testing.expectEqual(@as(u16, 27), @import("data/runtime.zig").DataServer.requiredRaftBatchProtocolVersion(parsed.batch.req));
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

test "required replicated begin bound seal receipts survive restart and reject route or participant drift" {
    const a = std.testing.allocator;
    const runtime = @import("antfly_runtime_abi");
    const Denied = struct {
        fn lifetime(_: *anyopaque) callconv(.c) runtime.error_abi.Status {
            return runtime.error_abi.statusFromError(error.Canceled);
        }
        fn cpu(_: *anyopaque) callconv(.c) runtime.error_abi.Status {
            return runtime.error_abi.statusFromError(error.Canceled);
        }
        fn phase(_: *anyopaque, _: c_int) callconv(.c) runtime.error_abi.Status {
            return runtime.error_abi.statusFromError(error.Canceled);
        }
    };
    var caller_state: u8 = 0;
    const expired: runtime.runnable_abi.Borrowed = .{ .context = &caller_state, .cpu_slots = 1, .parent_class = @backingInt(runtime.runnable_abi.Class.write), .check_lifetime = Denied.lifetime, .checkpoint = Denied.cpu, .park = Denied.phase, .enter = Denied.phase };
    for (std.meta.tags(codec.Mode)) |mode| {
        var directory = try sources.common_test_directory.TestDirectory.init("bound-seal-receipts");
        defer directory.cleanup();
        const id = request(mode).transaction.?.begin.txn_id;
        var participant: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash("table2:00000004:docs:7", &participant, .{});
        const binding: codec.Binding = .{ .group_id = 7, .topology_epoch = 3, .participant_digest = participant, .route_digest = @splat(9) };
        const expected: sources.api_prepared_transaction_protocol.SealRequest = .{ .txn_id = id, .group_id = 7, .topology_epoch = 3, .participant_digest = participant, .route_digest = binding.route_digest, .requirement = .{ .mode = mode } };
        var fingerprint: [32]u8 = undefined;
        {
            var db = try db_mod.DB.open(a, directory.path(), .{ .start_optional_runtimes = false });
            defer db.close();
            try db.enablePreparedTransactionProtocol();
            var begin = request(mode);
            begin.transaction.?.begin.preparation_binding = binding;
            var malformed = begin;
            malformed.transaction.?.begin.preparation_requirement = null;
            try std.testing.expectError(error.PreparedTransactionMigrationRequired, @import("data/runtime.zig").DataServer.ensurePreparedTransactionProposalSupported(malformed));
            try std.testing.expectError(error.InvalidBatchRequest, batch.encodeBatchRequest(a, malformed));
            const malformed_prepare: db_mod.types.BatchRequest = .{ .transaction = .{ .prepare = .{ .txn_id = id, .topology_epoch = 3, .preparation_binding = binding } } };
            try std.testing.expectError(error.PreparedTransactionMigrationRequired, @import("data/runtime.zig").DataServer.ensurePreparedTransactionProposalSupported(malformed_prepare));
            try std.testing.expectError(error.InvalidBatchRequest, batch.encodeBatchRequest(a, malformed_prepare));
            const bytes = try raft_batch.encode(a, "docs", begin);
            defer a.free(bytes);
            var parsed = try raft_batch.decode(a, bytes);
            defer parsed.deinit(a);
            try std.testing.expectEqual(@as(u16, 28), @import("data/runtime.zig").DataServer.requiredRaftBatchProtocolVersion(parsed.batch.req));
            try std.testing.expectError(error.Canceled, dispatch.applyReplicatedTransactionMutationInternalWithExecution(a, &db, "docs", 7, parsed.batch.req, .none, null, expired));
            try dispatch.applyReplicatedTransactionMutationInternalWithExecution(a, &db, "docs", 7, parsed.batch.req, .none, .{ .term = 1, .index = 1 }, expired);
            const prepare: db_mod.types.BatchRequest = .{ .transaction = .{ .prepare = .{ .txn_id = id, .topology_epoch = 3, .preparation_requirement = .{ .mode = mode }, .preparation_binding = binding } } };
            try dispatch.applyReplicatedTransactionMutationInternalWithExecution(a, &db, "docs", 7, prepare, .none, .{ .term = 1, .index = 2 }, expired);
            var manager = try db.core.initTxnManager();
            defer manager.deinit();
            try std.testing.expectEqualDeep(binding, (try manager.getPreparationDecision(id, null)).binding.?);
            try std.testing.expectError(error.PreparedEffectsRequired, manager.getBoundPreparedSealReceipt(id, binding, null));
            const contract = try manager.preparedEffectsContract(id, try db.core.preparedEffectsCatalogDigest(null), mode, null);
            const frame = try codec.encode(a, .{ .contract = contract, .effects = .{} }, null);
            defer a.free(frame);
            var seal: db_mod.types.BatchRequest = .{ .transaction = .{ .prepare = .{ .txn_id = id, .topology_epoch = 3, .preparation_requirement = .{ .mode = mode }, .preparation_binding = binding, .sealed_effects = frame } } };
            try std.testing.expectError(error.PreparationContractConflict, db.sealReplicatedTransactionEffectsAtOrderedReceipt(id, frame, mode, .{ .term = 1, .index = 3 }, null));
            for (0..4) |field| {
                var changed = binding;
                switch (field) {
                    0 => changed.group_id += 1,
                    1 => changed.topology_epoch += 1,
                    2 => changed.participant_digest[0] ^= 1,
                    else => changed.route_digest[0] ^= 1,
                }
                seal.transaction.?.prepare.preparation_binding = changed;
                try std.testing.expectError(error.PreparationContractConflict, dispatch.applyReplicatedTransactionMutationAtRaftEntry(a, &db, "docs", 7, seal, .{ .term = 1, .index = 3 }));
            }
            try std.testing.expectEqual(@as(u64, 2), (try sources.storage_db_apply_receipts.readOrderedApplyReceipt(a, db.core.store)).?.index);
            seal.transaction.?.prepare.preparation_binding = binding;
            const encoded = try raft_batch.encode(a, "docs", seal);
            defer a.free(encoded);
            var decoded = try raft_batch.decode(a, encoded);
            defer decoded.deinit(a);
            const stripped = try std.mem.replaceOwned(u8, a, encoded, "seal_bound_v1", "seal_required_v1");
            defer a.free(stripped);
            if (raft_batch.decode(a, stripped)) |value| {
                var unexpected = value;
                unexpected.deinit(a);
                return error.TestExpectedError;
            } else |err| try std.testing.expectEqual(error.InvalidBatchRequest, err);
            // Test the actual nested batch through its own decoder as well.
            const batch_bytes = try batch.encodeBatchRequest(a, seal);
            defer a.free(batch_bytes);
            const bare = try std.mem.replaceOwned(u8, a, batch_bytes, "seal_bound_v1", "seal_required_v1");
            defer a.free(bare);
            try rejected(a, batch.parseInternalBatchRequest(a, bare));
            try dispatch.applyReplicatedTransactionMutationInternalWithExecution(a, &db, "docs", 7, decoded.batch.req, .none, .{ .term = 1, .index = 3 }, expired);
            const durable = try manager.getBoundPreparedSealReceipt(id, binding, null);
            const receipt = sources.api_prepared_transaction_protocol.SealReceipt.fromDurable(durable);
            try receipt.validate(expected);
            const decision = sources.api_prepared_transaction_protocol.Decision.fromDurable(try db.getTransactionPreparationDecision(id, null));
            try std.testing.expectEqual(sources.storage_transactions.TxnStatus.pending, try decision.validate(expected.requirement, binding));
            try assertPreparationOwnerStatus(&db, id);
            fingerprint = receipt.frame_digest;
        }
        var reopened = try db_mod.DB.open(a, directory.path(), .{ .start_optional_runtimes = false });
        defer reopened.close();
        var manager = try reopened.core.initTxnManager();
        defer manager.deinit();
        const receipt = sources.api_prepared_transaction_protocol.SealReceipt.fromDurable(try manager.getBoundPreparedSealReceipt(id, binding, null));
        try receipt.validate(expected);
        try std.testing.expectEqualSlices(u8, &fingerprint, &receipt.frame_digest);
        try reopened.resolveReplicatedTransactionAtOrderedReceipt(id, .committed, 100, .write, .none, .{ .term = 1, .index = 4 }, null);
        try std.testing.expectEqualDeep(binding, (try manager.getPreparationDecision(id, null)).binding.?);
        const decision = sources.api_prepared_transaction_protocol.Decision.fromDurable(try reopened.getTransactionPreparationDecision(id, null));
        try std.testing.expectEqual(sources.storage_transactions.TxnStatus.committed, try decision.validate(expected.requirement, binding));
        try assertPreparationStatusAdmission(&reopened, id);
        try assertPreparationOwnerStatus(&reopened, id);
        try std.testing.expectError(error.PreparedEffectsRequired, manager.getBoundPreparedSealReceipt(id, binding, null));
        var read = try reopened.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectError(error.UnsupportedDatabaseReaderVersion, sources.storage_db_catalog_resolver_catalog.readMinimumReaderVersion(&read, 6));
    }
}

fn assertPreparationStatusAdmission(db: *db_mod.DB, id: [16]u8) !void {
    const admission = sources.common_read_admission;
    const Control = struct {
        calls: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        cause: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const ordinal = self.calls;
            self.calls += 1;
            if (ordinal == self.fail_at) return self.cause;
        }
        fn port(self: *@This()) admission.Payload {
            return .{ .ptr = self, .vtable = &.{ .checkpoint = check, .check_lifetime = check, .wait_for_progress = check } };
        }
    };
    var baseline: Control = .{};
    const initial = baseline.port();
    _ = try db.getTransactionPreparationDecision(id, admission.Port.init(&initial));
    for ([_]anyerror{ error.NotFound, error.TxnNotFound, error.Canceled, error.MemoryLimitExceeded }) |cause| {
        for (0..baseline.calls) |ordinal| {
            var original: Control = .{ .fail_at = ordinal, .cause = cause };
            const payload = original.port();
            try std.testing.expectError(cause, db.getTransactionPreparationDecision(id, admission.Port.init(&payload)));
            try std.testing.expect(db.core.tryLockApplyExclusive());
            db.core.unlockApplyExclusive();
        }
        // A control read yields while a writer holds publication authority;
        // the wait's original rejection cannot become missing transaction.
        db.core.lockApplyExclusive();
        var original: Control = .{ .fail_at = 1, .cause = cause };
        const payload = original.port();
        const observed = db.getTransactionPreparationDecision(id, admission.Port.init(&payload));
        db.core.unlockApplyExclusive();
        try std.testing.expectError(cause, observed);
    }
}

/// Exercise the production C entry on the actual native DB without requiring
/// the full storage archive qualification build.
fn assertPreparationOwnerStatus(db: *db_mod.DB, txn_id: [16]u8) !void {
    const owner = @import("capi/server_owner.zig");
    const abi = @import("kernel_owner_abi");
    const handles = sources.capi_handles;
    const protocol = sources.api_prepared_transaction_protocol;
    const admission = sources.common_read_admission;
    const Control = struct {
        calls: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        cause: anyerror = error.TxnNotFound,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const ordinal = self.calls;
            self.calls += 1;
            if (ordinal == self.fail_at) return self.cause;
        }
    };
    // The handle borrows this DB's core; only the caller closes it.
    var handle: handles.Handle = .{ .alloc = std.testing.allocator, .db = db.*, .storage_owner_table_name = @constCast("docs") };
    const registry = handles.handleRegistry();
    const registered = try registry.register(&handle);
    defer {
        _, const id = registry.beginClose(registered).?;
        registry.finishClose(id);
    }
    var control: Control = .{};
    const payload: admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = Control.check, .check_lifetime = Control.check, .wait_for_progress = Control.check } };
    const port = admission.Port.init(&payload);
    const status_request: abi.TransactionPreparationStatusRequest = .{ .table_name = .fromSlice("docs"), .txn_id = .{ .bytes = txn_id }, .port = &port };
    var result: abi.TransactionPreparationStatusResult = .{};
    try std.testing.expectEqual(abi.Status.ok, owner.storageOwnerTransactionPreparationStatus(registered, &status_request, &result));
    const actual = (try protocol.observationFromOwner(result)).known;
    const expected = protocol.Decision.fromDurable(try db.getTransactionPreparationDecision(txn_id, null));
    try std.testing.expectEqualDeep(expected, actual);
    const checkpoints = control.calls;
    for ([_]anyerror{ error.TxnNotFound, error.Canceled, error.Timeout, error.MemoryLimitExceeded }) |cause| for (0..checkpoints) |index| {
        control = .{ .fail_at = index, .cause = cause };
        // Mirror the caller-edge trace used by the typed owner client: unknown
        // original errors must survive the narrower C status enum.
        var caller: admission.RejectionTrace = .{};
        const traced = caller.bind(port).?;
        var traced_request = status_request;
        traced_request.port = &traced;
        const status = owner.storageOwnerTransactionPreparationStatus(registered, &traced_request, &result);
        try std.testing.expect(status != .ok);
        try std.testing.expectEqual(cause, caller.failure.?);
        try std.testing.expectEqual(@as(u8, 0), result.present);
    };
    control = .{};
    var missing = status_request;
    missing.txn_id.bytes = @splat(0x5d);
    try std.testing.expectEqual(abi.Status.ok, owner.storageOwnerTransactionPreparationStatus(registered, &missing, &result));
    try std.testing.expect((try protocol.observationFromOwner(result)) == .missing);
    // An original missing-shaped error wins even when no transaction exists.
    control = .{ .fail_at = 0 };
    const missing_failure = owner.storageOwnerTransactionPreparationStatus(registered, &missing, &result);
    try std.testing.expectError(error.TxnNotFound, @import("kernel_error_identity").statusToError(missing_failure));
    control = .{};
    var invalid = status_request;
    invalid.version = 0;
    result = .{ .present = 1, .has_binding = 1 };
    try std.testing.expectEqual(abi.Status.invalid_abi, owner.storageOwnerTransactionPreparationStatus(null, &invalid, &result));
    try std.testing.expectEqualDeep(abi.TransactionPreparationStatusResult{}, result);
}
