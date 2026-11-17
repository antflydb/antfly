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

//! Real NativeKV qualification of original-owner source attempt retirement.
const std = @import("std");
const transactions = @import("transactions.zig");
const completion = @import("transaction_completion.zig");
const scope = @import("source_scope.zig");
const retirement = @import("source_scope_retirement.zig");
const ordered = @import("source_scope_retirement_ordered.zig");
const DocStore = @import("docstore.zig").DocStore;
const keys = @import("internal_keys.zig");

pub fn run(a: std.mem.Allocator, path: [:0]const u8) !void {
    const coordinator: completion.RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = @splat(1), .table_binding = @splat(2), .root_generation = 0, .group_id = 2, .topology_epoch = 3 };
    var owner = coordinator;
    owner.namespace = @splat(3);
    owner.group_id = 4;
    const root = try scope.Basis.fromOwner(owner);
    const spec: scope.Spec = .{ .actor_table_id = 3, .source_id = @splat(7), .generation = 2, .binding = @splat(8), .checkpoint_owner = try scope.Basis.fromOwner(coordinator) };
    const members = [_]completion.Participant{
        .{ .namespace = coordinator.namespace, .owner_fence = coordinator.digest(), .roles = completion.Roles.checkpoint, .input_digest = @splat(5), .locator = "checkpoint", .routed_owner = coordinator },
        .{ .namespace = owner.namespace, .owner_fence = owner.digest(), .roles = completion.Roles.effects, .input_digest = @splat(6), .locator = "rows", .routed_owner = owner },
    };
    var admission: completion.Admission = .{ .version = 3, .transaction_id = @splat(9), .admission_digest = @splat(11), .coordinator_namespace = coordinator.namespace, .coordinator_fence = coordinator.digest(), .participants = &members };
    const query: @import("history_decision_wire.zig").Query = .{ .owner = coordinator, .transaction_id = admission.transaction_id, .minimum_applied_index = 5 };
    const reference: scope.Reference = .{ .domain = spec.domain(), .binding = spec.binding, .generation = 2, .token = 10, .sequence = 1 };
    var command: retirement.Command = .{ .version = 1, .intent = .{ .version = 1, .owner = owner, .coordinator = query, .reference = reference, .source_purpose = @splat(12), .admission_digest = admission.digest() }, .observation = .{ .request_digest = query.digest(), .cut = .{ .term = 2, .index = 6 }, .decision = .{ .record = .{ .status = .aborted, .begin_timestamp = 10, .created_at_ns = 11, .commit_version = 0, .finalized_at_ns = 20, .prepared = true, .intents_resolved = true, .digest = @splat(13), .source_purpose = @splat(12), .source_scope = reference }, .admission = .{ .admission = admission, .digest = admission.digest() } } } };
    const marker = @import("docstore.zig").KVPair{ .key = "\x00\x00__metadata__:test-source-retire", .value = "retired" };
    {
        var store = try DocStore.open(a, path.ptr, .{});
        defer store.close();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var bind = try store.beginWriteTxn();
        errdefer bind.abort();
        try bind.put(&keys.identity_namespace_key, &owner.namespace);
        try @import("source_authority.zig").bind(&bind, .native, owner.namespace);
        _ = try scope.Store.bind(arena.allocator(), &bind, root, spec, .{ .group_id = 1, .incarnation = root.metadata_incarnation, .index = 10 });
        try bind.commit();
        const history = @import("committed_history.zig");
        var enroll = try store.beginWriteTxn();
        errdefer enroll.abort();
        const consumer = try history.admit(&enroll, owner.namespace, @splat(1), 4 * 1024 * 1024, 1000, 1);
        try enroll.commit();
        var release = try store.beginWriteTxn();
        errdefer release.abort();
        try history.release(&release, owner.namespace, consumer.id, consumer.epoch);
        try release.commit();
        var manager = try transactions.TxnManager.init(a, &store);
        defer manager.deinit();
        // A receipt-validation failure after staging the token fence must roll
        // back that fence and every completion write with it.
        {
            var failed = try store.beginWriteTxn();
            defer failed.abort();
            var invalid = try retirement.applyUnenlisted(arena.allocator(), &failed, command, .{ .term = 3, .index = 19 });
            invalid.closed_through_token += 1;
            try failed.put(marker.key, marker.value);
            try std.testing.expectError(error.SourceAdmissionCorrupt, ordered.stage(arena.allocator(), &failed, command, invalid));
        }
        {
            var read = try store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqual(@as(u64, 0), (try scope.Store.readRecord(arena.allocator(), &read, spec.domain())).?.closed_through_token);
            try std.testing.expectError(error.NotFound, read.get(marker.key));
            try std.testing.expect(try ordered.replay(a, &read, command, .{ .term = 3, .index = 19 }) == .superseded);
        }
        const reply = try manager.retireScopedSourceAttempt(command, .{ .term = 3, .index = 20 }, &.{marker});
        try reply.validate(command.intent);
        try std.testing.expectEqual(@TypeOf(reply.disposition).fenced, reply.disposition);
        try std.testing.expectError(error.TxnNotFound, manager.getTransactionStatus(admission.transaction_id));
        const stored_marker = try store.get(a, marker.key);
        defer a.free(stored_marker);
        try std.testing.expectEqualStrings(marker.value, stored_marker);
    }
    {
        var store = try DocStore.open(a, path.ptr, .{});
        defer store.close();
        var manager = try transactions.TxnManager.init(a, &store);
        defer manager.deinit();
        {
            var read = try store.beginReadTxn();
            defer read.abort();
            const recovered = (try ordered.replay(a, &read, command, .{ .term = 3, .index = 20 })).applied;
            try std.testing.expectEqual(@TypeOf(recovered.disposition).fenced, recovered.disposition);
            try std.testing.expectEqual(@as(u64, 10), recovered.closed_through_token);
            try std.testing.expectError(error.ConflictingOrderedApplyReceipt, ordered.replay(a, &read, command, .{ .term = 4, .index = 20 }));
            var changed = command;
            changed.intent.coordinator.minimum_applied_index += 1;
            changed.observation.request_digest = changed.intent.coordinator.digest();
            try std.testing.expectError(error.ConflictingOrderedApplyReceipt, ordered.replay(a, &read, changed, .{ .term = 3, .index = 20 }));
        }
        const retry = try manager.retireScopedSourceAttempt(command, .{ .term = 3, .index = 21 }, &.{});
        try std.testing.expectEqual(@TypeOf(retry.disposition).already_fenced, retry.disposition);
        const extra: transactions.MutationExtraBatch = .{ .source_purpose = command.intent.source_purpose, .source_scope = reference, .completion_owner = owner, .completion_input_digest = members[1].input_digest };
        try std.testing.expectError(error.SourceCheckpointConflict, manager.initParticipantWithCompletionAdmission(admission, 10, 11, true, extra));
        try std.testing.expectError(error.TxnNotFound, manager.getTransactionStatus(admission.transaction_id));
        // The next legitimate attempt can still use this source sequence.
        admission.transaction_id = @splat(22);
        command.intent.coordinator.transaction_id = admission.transaction_id;
        command.intent.reference.token = 11;
        command.intent.admission_digest = admission.digest();
        command.observation.request_digest = command.intent.coordinator.digest();
        command.observation.decision.?.record.source_scope = command.intent.reference;
        command.observation.decision.?.admission = .{ .admission = admission, .digest = admission.digest() };
        var admitted_extra = extra;
        admitted_extra.source_scope = command.intent.reference;
        try manager.initParticipantWithCompletionAdmission(admission, 10, 11, true, admitted_extra);
        try manager.writeIntentsExtraBatch(admission.transaction_id, &.{.{ .key = "a", .value = "prepared" }}, &.{}, admitted_extra);
        try std.testing.expect(try manager.hasIntents(admission.transaction_id));
        const drained = try manager.retireScopedSourceAttempt(command, .{ .term = 3, .index = 22 }, &.{marker});
        try drained.validate(command.intent);
        try std.testing.expectEqual(transactions.TxnStatus.aborted, try manager.getTransactionStatus(admission.transaction_id));
        try std.testing.expect(!try manager.hasIntents(admission.transaction_id));
        const resolved = try manager.getResolvedParticipants(a, admission.transaction_id);
        defer transactions.freeParticipantList(a, resolved);
        try std.testing.expectEqual(@as(usize, 1), resolved.len);
        try std.testing.expectEqualStrings("checkpoint", resolved[0]);
        {
            var probe = try store.beginReadTxn();
            defer probe.abort();
            const scoped = (try scope.Store.readRecord(a, &probe, reference.domain)).?;
            try std.testing.expectEqual(@as(u64, 11), scoped.closed_through_token);
            try std.testing.expect(scoped.slot == null);
            try std.testing.expectEqualDeep(drained, (try ordered.replay(a, &probe, command, drained.cut)).applied);
        }
        // Already-resolved retries with no marker writes still publish their
        // receipt; they do not recertify or reapply any row effects.
        const terminal_retry = try manager.retireScopedSourceAttempt(command, .{ .term = 3, .index = 23 }, &.{});
        try std.testing.expectEqual(@TypeOf(terminal_retry.disposition).already_fenced, terminal_retry.disposition);
        {
            var last = try store.beginReadTxn();
            defer last.abort();
            try std.testing.expectEqualDeep(terminal_retry, (try ordered.replay(a, &last, command, terminal_retry.cut)).applied);
            try std.testing.expect(try ordered.replay(a, &last, command, drained.cut) == .superseded);
        }
    }
    {
        var store = try DocStore.open(a, path.ptr, .{});
        defer store.close();
        var manager = try transactions.TxnManager.init(a, &store);
        defer manager.deinit();
        {
            var read = try store.beginReadTxn();
            defer read.abort();
            const recovered = (try ordered.replay(a, &read, command, .{ .term = 3, .index = 23 })).applied;
            try std.testing.expectEqual(@TypeOf(recovered.disposition).already_fenced, recovered.disposition);
        }
        // Model a durable ABORT decision whose empty intent cleanup has not
        // yet been acknowledged. Recovery must finish the scope and receipt
        // together even though no primary row intents remain.
        admission.transaction_id = @splat(23);
        command.intent.coordinator.transaction_id = admission.transaction_id;
        command.intent.reference.token = 12;
        command.intent.admission_digest = admission.digest();
        command.observation.request_digest = command.intent.coordinator.digest();
        command.observation.decision.?.record.source_scope = command.intent.reference;
        command.observation.decision.?.admission = .{ .admission = admission, .digest = admission.digest() };
        try manager.initParticipantWithCompletionAdmission(admission, 10, 11, true, .{ .source_purpose = command.intent.source_purpose, .source_scope = command.intent.reference, .completion_owner = owner, .completion_input_digest = members[1].input_digest });
        var record_key: [completion.record_prefix.len + 16]u8 = undefined;
        @memcpy(record_key[0..completion.record_prefix.len], completion.record_prefix);
        @memcpy(record_key[completion.record_prefix.len..], &admission.transaction_id);
        const raw = try store.get(a, &record_key);
        defer a.free(raw);
        try std.testing.expectEqual(@as(usize, 175), raw.len);
        raw[0] = @backingInt(transactions.TxnStatus.aborted);
        std.mem.writeInt(u64, raw[25..33], 20, .little);
        try std.testing.expectEqual(@as(u8, 0), raw[52]);
        var decision_write = try store.beginWriteTxn();
        errdefer decision_write.abort();
        try decision_write.put(&record_key, raw);
        try decision_write.commit();
        const finished = try manager.retireScopedSourceAttempt(command, .{ .term = 3, .index = 24 }, &.{marker});
        try std.testing.expectEqual(@as(u64, 12), finished.closed_through_token);
        {
            var read = try store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqualDeep(finished, (try ordered.replay(a, &read, command, finished.cut)).applied);
        }
        // Corrupt and oversized receipts are errors, never success or expiry.
        const receipt_bytes = try store.get(a, ordered.receipt_key);
        defer a.free(receipt_bytes);
        {
            var corrupt = try store.beginWriteTxn();
            defer corrupt.abort();
            receipt_bytes[receipt_bytes.len - 2] ^= 1;
            try corrupt.put(ordered.receipt_key, receipt_bytes);
            try std.testing.expectError(error.SourceAdmissionCorrupt, ordered.replay(a, &corrupt, command, finished.cut));
        }
        {
            var oversized = try store.beginWriteTxn();
            defer oversized.abort();
            const bytes = try a.alloc(u8, ordered.max_record_bytes + 1);
            defer a.free(bytes);
            @memset(bytes, ' ');
            try oversized.put(ordered.receipt_key, bytes);
            try std.testing.expectError(error.SourceAdmissionCorrupt, ordered.replay(a, &oversized, command, finished.cut));
        }
    }
}
