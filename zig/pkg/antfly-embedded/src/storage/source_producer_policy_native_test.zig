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

//! Real NativeKV closure/rollback/reopen checks. Cuts and classification are
//! modeled trusted inputs here; production ingress and drain proof are unwired.
const std = @import("std");
const policy = @import("source_producer_policy.zig");
const scope = @import("source_scope.zig");
const DocStore = @import("docstore.zig").DocStore;
const marker_key = "\x00\x00__metadata__:test-producer-entry";
const effect_key = "\x00\x00__metadata__:test-producer-effect";
fn apply(a: std.mem.Allocator, store: *DocStore, root: scope.Basis, producer: policy.Producer, value: []const u8) !void {
    var txn = try store.beginWriteTxn();
    errdefer txn.abort();
    try policy.Store.require(a, &txn, root, producer);
    try txn.put(effect_key, value);
    try txn.commit();
}
pub fn run(a: std.mem.Allocator, path: [:0]const u8, root: scope.Basis, original: @import("source_scope_retirement.zig").Command) !void {
    const entry: scope.Cut = .{ .term = 3, .index = 100 };
    {
        var store = try DocStore.open(a, path.ptr, .{});
        defer store.close();
        try apply(a, &store, root, .unlabelled, "before");
        {
            const transactions = @import("transactions.zig");
            const completion = @import("transaction_completion.zig");
            var manager = try transactions.TxnManager.init(a, &store);
            defer manager.deinit();
            var member = try original.observation.decision.?.admission.admission.member(root.namespace);
            member.roles = completion.Roles.effects;
            const admission: completion.Admission = .{ .version = 3, .transaction_id = @splat(238), .admission_digest = @splat(239), .coordinator_namespace = member.namespace, .coordinator_fence = member.owner_fence, .participants = &.{member} };
            try manager.initTransactionWithCompletionAdmission(admission, 5, 6, true, .{});
        }
        {
            var failed = try store.beginWriteTxn();
            defer failed.abort();
            _ = try policy.Store.close(a, &failed, root, entry);
            try failed.put(marker_key, "100");
        }
        {
            var read = try store.beginReadTxn();
            defer read.abort();
            try std.testing.expect(try policy.Store.read(a, &read, root) == null);
            try std.testing.expectError(error.NotFound, read.get(marker_key));
        }
        {
            var txn = try store.beginWriteTxn();
            errdefer txn.abort();
            const closed = try policy.Store.close(a, &txn, root, entry);
            try std.testing.expectEqualDeep(entry, closed.first_closed_at);
            try txn.put(marker_key, "100");
            try txn.commit();
        }
        try std.testing.expectError(error.SourcePendingWork, apply(a, &store, root, .unlabelled, "delayed-old-request"));
        try std.testing.expectError(error.SourcePendingWork, apply(a, &store, root, .legacy_cdc, "delayed-labelled-worker"));
        const previous = try store.get(a, effect_key);
        defer a.free(previous);
        try std.testing.expectEqualStrings("before", previous);
    }
    {
        var store = try DocStore.open(a, path.ptr, .{});
        defer store.close();
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        const closed = try policy.Store.read(a, &txn, root);
        try std.testing.expectEqualDeep(entry, closed.?.first_closed_at);
        try std.testing.expectEqualStrings("100", try txn.get(marker_key));
        const again = try policy.Store.close(a, &txn, root, .{ .term = 4, .index = 200 });
        try std.testing.expectEqualDeep(closed.?, again);
        try std.testing.expectError(error.ConflictingOrderedApplyReceipt, policy.Store.close(a, &txn, root, .{ .term = 4, .index = 100 }));
        try std.testing.expectError(error.ConflictingOrderedApplyReceipt, policy.Store.close(a, &txn, root, .{ .term = 2, .index = 200 }));
        var changed_root = root;
        changed_root.root_generation += 1;
        try std.testing.expectError(error.HistoryResynchronizationRequired, policy.Store.require(a, &txn, changed_root, .ordinary));
        try std.testing.expectError(error.SourcePendingWork, policy.Store.require(a, &txn, root, .unlabelled));
        try policy.Store.require(a, &txn, root, .ordinary);
        try policy.Store.require(a, &txn, root, .scoped_source);
    }
    {
        var store = try DocStore.open(a, path.ptr, .{});
        defer store.close();
        try apply(a, &store, root, .ordinary, "authenticated-ordinary");
        try std.testing.expectError(error.SourcePendingWork, apply(a, &store, root, .unlabelled, "still-old"));
        const retained = try store.get(a, effect_key);
        defer a.free(retained);
        try std.testing.expectEqualStrings("authenticated-ordinary", retained);
        {
            const transactions = @import("transactions.zig");
            var manager = try transactions.TxnManager.init(a, &store);
            defer manager.deinit();
            var admission = original.observation.decision.?.admission.admission;
            admission.transaction_id = @splat(233);
            admission.admission_digest = @splat(235);
            var reference = original.intent.reference;
            reference.token = 13;
            const purpose: scope.Digest = @splat(234);
            const member = try admission.member(root.namespace);
            const extra: transactions.MutationExtraBatch = .{ .source_scope = reference, .source_purpose = purpose, .completion_owner = original.intent.owner, .completion_input_digest = member.input_digest };
            try manager.initParticipantWithCompletionAdmission(admission, 30, 31, true, extra);
            try manager.writeIntentsExtraBatch(admission.transaction_id, &.{}, &.{}, extra);
            _ = try manager.resolveIntentsWithExtraBatch(admission.transaction_id, .aborted, 40, .{ .source_scope = reference, .source_purpose = purpose, .completion_owner = original.intent.owner, .source_scope_cut = .{ .term = 3, .index = 101 } });
            try std.testing.expectEqual(transactions.TxnStatus.aborted, try manager.getTransactionStatus(admission.transaction_id));
        }
        {
            const transactions = @import("transactions.zig");
            const completion = @import("transaction_completion.zig");
            var manager = try transactions.TxnManager.init(a, &store);
            defer manager.deinit();
            var member = try original.observation.decision.?.admission.admission.member(root.namespace);
            member.roles = completion.Roles.effects;
            const admission: completion.Admission = .{ .version = 3, .transaction_id = @splat(236), .admission_digest = @splat(237), .coordinator_namespace = member.namespace, .coordinator_fence = member.owner_fence, .participants = &.{member} };
            const extra: transactions.MutationExtraBatch = .{ .ordinary_producer = true, .completion_owner = member.routed_owner, .completion_input_digest = member.input_digest };
            var old_admission = admission;
            old_admission.transaction_id = @splat(238);
            old_admission.admission_digest = @splat(239);
            try std.testing.expectError(error.SourceTransactionIdentityConflict, manager.initTransactionWithCompletionAdmission(old_admission, 5, 6, true, extra));
            var wrong_member = member;
            wrong_member.routed_owner.?.root_generation += 1;
            wrong_member.owner_fence = wrong_member.routed_owner.?.digest();
            var wrong_admission = admission;
            wrong_admission.participants = &.{wrong_member};
            wrong_admission.coordinator_fence = wrong_member.owner_fence;
            try std.testing.expectError(error.HistoryResynchronizationRequired, manager.initTransactionWithCompletionAdmission(wrong_admission, 50, 51, true, extra));
            try std.testing.expectError(error.TxnNotFound, manager.getTransactionStatus(admission.transaction_id));
            try manager.initTransactionWithCompletionAdmission(admission, 50, 51, true, extra);
            try std.testing.expectError(error.SourceTransactionIdentityConflict, manager.initTransactionWithCompletionAdmission(admission, 50, 51, true, .{}));
            var no_label = extra;
            no_label.ordinary_producer = false;
            try std.testing.expectError(error.SourceTransactionIdentityConflict, manager.writeIntentsExtraBatch(admission.transaction_id, &.{}, &.{}, no_label));
            try manager.writeIntentsExtraBatch(admission.transaction_id, &.{}, &.{}, extra);
            var captured = try manager.collectIntentBatch(a, admission.transaction_id);
            defer captured.deinit(a);
            try std.testing.expect(captured.ordinary_producer);
        }
        {
            var corrupt = try store.beginWriteTxn();
            defer corrupt.abort();
            try corrupt.put(policy.key, "{}");
            try std.testing.expectError(error.SourceAdmissionCorrupt, policy.Store.require(a, &corrupt, root, .ordinary));
        }
        {
            const oversized = try a.alloc(u8, policy.max_bytes + 1);
            defer a.free(oversized);
            @memset(oversized, ' ');
            var corrupt = try store.beginWriteTxn();
            defer corrupt.abort();
            try corrupt.put(policy.key, oversized);
            try std.testing.expectError(error.SourceAdmissionCorrupt, policy.Store.require(a, &corrupt, root, .ordinary));
        }
    }
    {
        var store = try DocStore.open(a, path.ptr, .{});
        defer store.close();
        const transactions = @import("transactions.zig");
        var manager = try transactions.TxnManager.init(a, &store);
        defer manager.deinit();
        var captured = try manager.collectIntentBatch(a, @splat(236));
        defer captured.deinit(a);
        try std.testing.expect(captured.ordinary_producer);
        // Resolution inherits the original record; no caller label is needed.
        _ = try manager.resolveIntentsWithExtraBatch(@splat(236), .committed, 60, .{});
        try std.testing.expectEqual(transactions.TxnStatus.committed, try manager.getTransactionStatus(@splat(236)));
        const page = try manager.listTransactionsPage(a, null, 64);
        defer a.free(page.items);
        var found = false;
        for (page.items) |item| if (std.mem.eql(u8, &item.txn_id, &@as(transactions.TxnId, @splat(236)))) {
            try std.testing.expect(item.ordinary_producer);
            found = true;
        };
        try std.testing.expect(found);
    }
}
