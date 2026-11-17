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

//! Compiled-engine original terminal legacy recovery and historical effects.
const std = @import("std");
const db_mod = @import("db.zig");
const completion = @import("../transaction_completion.zig");
const transactions = @import("../transactions.zig");
const scope = @import("../source_scope.zig");
const ordered = @import("../source_legacy_terminal_ordered.zig");
const inventory = @import("../source_legacy_inventory.zig");
const wire = @import("../../api/batch_internal.zig");
pub fn run(a: std.mem.Allocator, parent: []const u8, options: db_mod.OpenOptions, owner: completion.RoutedOwner) !void {
    const path = try std.fmt.allocPrint(a, "{s}/legacy-terminal-recovery", .{parent});
    defer a.free(path);
    var reference: @import("../history_consumer.zig").Reference = undefined;
    var proofs: [2]transactions.LegacyTerminalResolution = undefined;
    var manifests: [2]completion.Digest = undefined;
    var pending: transactions.LegacyTerminalResolution = undefined;
    {
        var db = try db_mod.DB.open(a, path, options);
        defer db.close();
        const grant: @import("../history_consumer.zig").Grant = .{ .namespace = owner.namespace, .authority = @splat(1), .sequence = 1, .consumer = @splat(2), .definition = @splat(3), .expires_at_seconds = 1000, .policy_revision = 1, .limit = 16 * 1024 * 1024 };
        const enrolled = try db.applyOrderedHistoryConsumerControl(.{ .enroll = grant }, .{ .term = 1, .index = 1 }, false);
        reference = .{ .namespace = grant.namespace, .authority = grant.authority, .consumer = grant.consumer, .epoch = enrolled.applied.consumer.?.epoch, .grant = grant.sequence, .definition = grant.definition };
        for (0..2) |i| {
            const id: [16]u8 = @splat(@as(u8, @intCast(221 + i)));
            var request: @import("types.zig").TransactionIntentRequest = .{ .completion_legacy_producer = true, .document_version_scheme = .revision_v1, .completion_owner = owner, .writes = &.{.{ .key = if (i == 0) "legacy-commit" else "legacy-abort", .value = "{\"value\":1}" }} };
            const input = try wire.completionPrepareInput(a, try request.completionBatch(id, 100, owner.topology_epoch));
            defer a.free(input);
            request.completion_input_digest = completion.preparedInputDigest(owner.namespace, input);
            const members = [_]completion.Participant{.{ .namespace = owner.namespace, .owner_fence = owner.digest(), .roles = completion.Roles.effects, .input_digest = request.completion_input_digest, .locator = "table2:00000004:docs:2", .routed_owner = owner }};
            const admission: completion.Admission = .{ .version = 3, .transaction_id = id, .admission_digest = @splat(@as(u8, @intCast(225 + i))), .coordinator_namespace = owner.namespace, .coordinator_fence = owner.digest(), .participants = &members };
            manifests[i] = (try admission.manifest(200)).digest();
            const encoded = try completion.encodeAdmission(a, admission);
            defer a.free(encoded);
            _ = try db.beginReplicatedLegacyCompletionTransaction(encoded, owner, 100, 101, true, .{ .term = 1, .index = 2 + 2 * i });
            try db.writeReplicatedTransactionAtOrderedReceipt(id, request, .{ .term = 1, .index = 3 + 2 * i });
            // Model the legacy decision/effect crash window with its original
            // canonical record. Only the original decision fields are staged.
            const key = completion.record_prefix.* ++ id;
            var read = try db.core.store.beginReadTxn();
            const raw = try a.dupe(u8, try read.get(&key));
            read.abort();
            defer a.free(raw);
            raw[0] = @intCast(@backingInt(if (i == 0) transactions.TxnStatus.committed else transactions.TxnStatus.aborted));
            std.mem.writeInt(u64, raw[9..17], if (i == 0) 200 else 0, .little);
            std.mem.writeInt(u64, raw[25..33], 201, .little);
            try db.core.store.putBatch(&.{.{ .key = &key, .value = raw }}, &.{});
            proofs[i] = .{ .root = try scope.Basis.fromOwner(owner), .transaction_id = id, .record_digest = completion.bytesDigest(raw) };
        }
        const pending_id: [16]u8 = @splat(230);
        _ = try db.beginTransactionScoped(pending_id, 100, 101, &.{}, true, true, null);
        var pending_read = try db.core.store.beginReadTxn();
        pending = .{ .root = try scope.Basis.fromOwner(owner), .transaction_id = pending_id, .record_digest = completion.bytesDigest(try pending_read.get(&(completion.record_prefix.* ++ pending_id))) };
        pending_read.abort();
        try std.testing.expectError(error.SourcePendingWork, db.legacyRecoveryInventory(a, .{ .version = 1, .owner = owner, .after = null, .max_records = 256, .minimum_applied_index = 5 }));
        _ = try db.applyOrderedSourceProducerClosure(owner, .{ .term = 1, .index = 6 }, false);
        try checkInventory(&db, a, owner, 6, false);
    }
    {
        var db = try db_mod.DB.open(a, path, options);
        defer db.close();
        try checkInventory(&db, a, owner, 6, false);
        var stale = owner;
        stale.root_generation += 1;
        try std.testing.expectError(error.InvalidSourceAdmission, db.resolveOrderedClosedLegacyTerminal(stale, proofs[0], .{ .term = 1, .index = 7 }));
        var stale_proof = proofs[0];
        stale_proof.root = try scope.Basis.fromOwner(stale);
        try std.testing.expectError(error.HistoryResynchronizationRequired, db.resolveOrderedClosedLegacyTerminal(stale, stale_proof, .{ .term = 1, .index = 7 }));
        const pending_outcome = try db.resolveOrderedClosedLegacyTerminal(owner, pending, .{ .term = 1, .index = 7 });
        try std.testing.expect(pending_outcome == .rejected and pending_outcome.rejected == .unresolved);
        try std.testing.expectEqualDeep(pending_outcome, try db.resolveOrderedClosedLegacyTerminal(owner, pending, .{ .term = 1, .index = 7 }));
        const pending_query: ordered.Query = .{ .version = 1, .owner = owner, .command = .{ .version = 1, .owner = owner, .proof = pending }, .entry = .{ .term = 1, .index = 7 }, .minimum_applied_index = 7 };
        const pending_read = try db.legacyTerminalOutcome(pending_query);
        try std.testing.expectEqualDeep(pending_outcome, pending_read.outcome);
        try std.testing.expectEqual(@as(u64, 7), pending_read.cut.index);
        var future = pending_query;
        future.minimum_applied_index += 1;
        try std.testing.expectError(error.StorageReadTemporarilyUnavailable, db.legacyTerminalOutcome(future));
        var wrong_term = pending_query;
        wrong_term.entry.term += 1;
        try std.testing.expectError(error.ConflictingOrderedApplyReceipt, db.legacyTerminalOutcome(wrong_term));
        try std.testing.expectEqual(@as(u64, 7), (try db.orderedApplyReceipt()).?.index);

        var forged = proofs[0];
        forged.record_digest[0] ^= 1;
        var wrong_cohort = owner;
        wrong_cohort.metadata_incarnation = "22222222222222222222222222222222".*;
        var wrong_cohort_proof = pending;
        wrong_cohort_proof.root = try scope.Basis.fromOwner(wrong_cohort);
        try std.testing.expectError(error.HistoryResynchronizationRequired, db.resolveOrderedClosedLegacyTerminal(wrong_cohort, wrong_cohort_proof, .{ .term = 1, .index = 8 }));
        try std.testing.expectEqualDeep(pending_outcome, try db.resolveOrderedClosedLegacyTerminal(owner, pending, .{ .term = 1, .index = 7 }));
        const conflict = try db.resolveOrderedClosedLegacyTerminal(owner, forged, .{ .term = 1, .index = 8 });
        try std.testing.expect(conflict == .rejected and conflict.rejected == .conflict);
        try std.testing.expectError(error.SourcePendingWork, db.resolveReplicatedCompletionTransactionAtOrderedReceipt(proofs[0].transaction_id, .committed, 200, .{ .term = 1, .index = 9 }, null, owner));
        try std.testing.expectEqual(@as(u64, 8), (try db.orderedApplyReceipt()).?.index);
        try std.testing.expect(try db.get(a, "legacy-commit") == null);
        const committed = try db.resolveOrderedClosedLegacyTerminal(owner, proofs[0], .{ .term = 1, .index = 9 });
        try std.testing.expect(committed == .applied and committed.applied.kind == .committed and committed.applied.timestamp == 200);
        const committed_query: ordered.Query = .{ .version = 1, .owner = owner, .command = .{ .version = 1, .owner = owner, .proof = proofs[0] }, .entry = .{ .term = 1, .index = 9 }, .minimum_applied_index = 9 };
        const committed_read = try db.legacyTerminalOutcome(committed_query);
        try std.testing.expectEqualDeep(committed, committed_read.outcome);
        try std.testing.expectEqual(@as(u64, 9), committed_read.cut.index);

        try std.testing.expectError(error.ConflictingOrderedApplyReceipt, db.resolveOrderedClosedLegacyTerminal(owner, forged, .{ .term = 1, .index = 9 }));
        const row = (try db.get(a, "legacy-commit")) orelse return error.TestExpectedRecoveredLegacyRow;
        defer a.free(row);
        try std.testing.expectEqualStrings("{\"value\":1}", row);
        try std.testing.expectEqualDeep(committed, try db.resolveOrderedClosedLegacyTerminal(owner, proofs[0], .{ .term = 1, .index = 9 }));
        const resolution = try db.historyResolution(a, .{ .version = 1, .authority = .raft, .namespace = owner.namespace, .transaction_id = proofs[0].transaction_id, .owner_fence = owner.digest(), .manifest_digest = manifests[0], .commit_version = 200, .minimum_applied_index = 9 });
        try std.testing.expect(resolution.resolution != null and resolution.resolution.?.outcome == .effects);
        var page = try db.orderedHistoryPage(a, .{ .reference = reference, .after = .{}, .now_seconds = 1 }, 9);
        defer page.deinit();
        try std.testing.expectEqualDeep(proofs[0].transaction_id, page.header.?.transaction_id);
        try std.testing.expectEqual(@as(usize, 1), page.events.len);
        try std.testing.expect(page.events[0].before == null);
        try std.testing.expectEqualStrings("{\"value\":1}", page.events[0].after.?);
        const document_id = try @import("../history_page.zig").documentId(a, page.events[0].key);
        defer a.free(document_id);
        try std.testing.expectEqualStrings("legacy-commit", document_id);
        const aborted = try db.resolveOrderedClosedLegacyTerminal(owner, proofs[1], .{ .term = 1, .index = 10 });
        try std.testing.expect(aborted == .applied and aborted.applied.kind == .aborted and aborted.applied.timestamp == 201);
        try std.testing.expect(try db.get(a, "legacy-abort") == null);
        try std.testing.expectEqual(@as(u64, 10), (try db.orderedApplyReceipt()).?.index);
        try std.testing.expectEqual(transactions.TxnStatus.pending, try db.getTransactionStatus(pending.transaction_id));
        try checkInventory(&db, a, owner, 10, true);
        var after_abort = try db.orderedHistoryPage(a, .{ .reference = reference, .after = page.next, .now_seconds = 1 }, 10);
        defer after_abort.deinit();
        try std.testing.expect(after_abort.caught_up and after_abort.events.len == 0);
        try std.testing.expectEqualDeep(page.next, after_abort.next);
    }
    {
        var db = try db_mod.DB.open(a, path, options);
        defer db.close();
        _ = try db.resolveOrderedClosedLegacyTerminal(owner, proofs[1], .{ .term = 1, .index = 10 });
        try std.testing.expect(try db.resolveOrderedClosedLegacyTerminal(owner, pending, .{ .term = 1, .index = 7 }) == .superseded);
        const query: ordered.Query = .{ .version = 1, .owner = owner, .command = .{ .version = 1, .owner = owner, .proof = proofs[1] }, .entry = .{ .term = 1, .index = 10 }, .minimum_applied_index = 10 };
        try checkInventory(&db, a, owner, 10, true);
        const reopened = try db.legacyTerminalOutcome(query);
        try std.testing.expect(reopened.outcome == .applied and reopened.outcome.applied.kind == .aborted and reopened.outcome.applied.timestamp == 201);
        var expired = query;
        expired.command.proof = pending;
        expired.entry.index = 7;
        try std.testing.expect((try db.legacyTerminalOutcome(expired)).outcome == .superseded);
        try std.testing.expectEqual(@as(u64, 10), (try db.orderedApplyReceipt()).?.index);

        const row = (try db.get(a, "legacy-commit")) orelse return error.TestExpectedReopenedLegacyRow;
        defer a.free(row);
        try std.testing.expect(try db.get(a, "legacy-abort") == null);
    }
}

fn checkInventory(db: *db_mod.DB, a: std.mem.Allocator, owner: completion.RoutedOwner, index: u64, resolved: bool) !void {
    const query: inventory.Query = .{ .version = 1, .owner = owner, .after = null, .max_records = 256, .minimum_applied_index = index };
    const before = try db.orderedApplyReceipt();
    var observed = try db.legacyRecoveryInventory(a, query);
    defer observed.deinit(a);
    try observed.validate(query);
    try std.testing.expectEqual(index, observed.cut.index);
    try std.testing.expectEqual(@as(usize, 3), observed.page.scanned_records);
    try std.testing.expectEqual(@as(usize, if (resolved) 1 else 3), observed.page.items.len);
    try std.testing.expect(observed.page.reached_end);
    try std.testing.expectEqual(@as(u64, 6), observed.page.closure.first_closed_at.index);
    try std.testing.expectEqualDeep(@as(?[16]u8, @splat(230)), observed.page.next_after);
    try std.testing.expectEqualDeep(before, try db.orderedApplyReceipt());
    const encoded_query = try inventory.encodeQuery(a, query);
    defer a.free(encoded_query);
    try std.testing.expectEqualDeep(query, try inventory.decodeQuery(a, encoded_query));
    const encoded = try inventory.encodeObservation(a, query, observed);
    defer a.free(encoded);
    var decoded = try inventory.decodeObservation(a, encoded, query);
    defer decoded.deinit(a);
    try std.testing.expectEqualDeep(observed, decoded);
    var raw = try std.json.parseFromSlice(std.json.Value, a, encoded, .{});
    defer raw.deinit();
    _ = raw.value.object.getPtr("owner").?.object.swapRemove("root_generation");
    const stripped = try std.json.Stringify.valueAlloc(a, raw.value, .{});
    defer a.free(stripped);
    try std.testing.expectError(error.SourceAdmissionCorrupt, inventory.decodeObservation(a, stripped, query));
    var bounded = query;
    bounded.max_records = 1;
    var scanned: usize = 0;
    var returned: usize = 0;
    while (true) {
        var page = try db.legacyRecoveryInventory(a, bounded);
        defer page.deinit(a);
        try std.testing.expect(page.page.scanned_records <= 1);
        scanned += page.page.scanned_records;
        returned += page.page.items.len;
        if (page.page.reached_end) break;
        bounded.after = page.page.next_after;
    }
    try std.testing.expectEqual(observed.page.scanned_records, scanned);
    try std.testing.expectEqual(observed.page.items.len, returned);
    bounded = query;
    bounded.minimum_applied_index += 1;
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, db.legacyRecoveryInventory(a, bounded));
    bounded = query;
    bounded.owner.root_generation += 1;
    try std.testing.expectError(error.HistoryResynchronizationRequired, db.legacyRecoveryInventory(a, bounded));
    bounded = query;
    bounded.owner.metadata_incarnation[0] ^= 1;
    try std.testing.expectError(error.HistoryResynchronizationRequired, db.legacyRecoveryInventory(a, bounded));
    bounded = query;
    bounded.max_records = 0;
    try std.testing.expectError(error.InvalidSourceAdmission, db.legacyRecoveryInventory(a, bounded));
    bounded.max_records = 257;
    try std.testing.expectError(error.InvalidSourceAdmission, db.legacyRecoveryInventory(a, bounded));
    var forged = observed;
    forged.page.scanned_records = 0;
    try std.testing.expectError(error.SourceAdmissionCorrupt, forged.validate(query));
    forged = observed;
    forged.page.next_after = @splat(0);
    try std.testing.expectError(error.SourceAdmissionCorrupt, forged.validate(query));
    forged = observed;
    forged.cut.index -= 1;
    try std.testing.expectError(error.SourceAdmissionCorrupt, forged.validate(query));
}
