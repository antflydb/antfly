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

//! Native DB/compiled-engine ordinary completion across producer closure.
//! Owner, policy and applying entry are fixture authority, not a production
//! closure activation or proof that legacy work has drained.
const std = @import("std");
const db_mod = @import("db.zig");
const completion = @import("../transaction_completion.zig");
const wire = @import("../../api/batch_internal.zig");
pub fn run(a: std.mem.Allocator, parent: []const u8, options: db_mod.OpenOptions, owner: completion.RoutedOwner) !void {
    const path = try std.fmt.allocPrint(a, "{s}/ordinary-producer", .{parent});
    defer a.free(path);
    const id: [16]u8 = @splat(243);
    var request: @import("types.zig").TransactionIntentRequest = .{ .completion_ordinary_producer = true, .document_version_scheme = .revision_v1, .completion_owner = owner, .writes = &.{.{ .key = "ordinary-txn-row", .value = "{\"value\":1}" }} };
    const prepare = try request.completionBatch(id, 100, owner.topology_epoch);
    const input = try wire.completionPrepareInput(a, prepare);
    defer a.free(input);
    request.completion_input_digest = completion.preparedInputDigest(owner.namespace, input);
    const locator = "table2:00000004:docs:2";
    const members = [_]completion.Participant{.{ .namespace = owner.namespace, .owner_fence = owner.digest(), .roles = completion.Roles.effects, .input_digest = request.completion_input_digest, .locator = locator, .routed_owner = owner }};
    const admission: completion.Admission = .{ .version = 3, .transaction_id = id, .admission_digest = @splat(244), .coordinator_namespace = owner.namespace, .coordinator_fence = owner.digest(), .participants = &members };
    const encoded = try completion.encodeAdmission(a, admission);
    defer a.free(encoded);
    {
        var db = try db_mod.DB.open(a, path, options);
        defer db.close();
        const grant: @import("../history_consumer.zig").Grant = .{ .namespace = owner.namespace, .authority = @splat(1), .sequence = 1, .consumer = @splat(2), .definition = @splat(3), .expires_at_seconds = 1000, .policy_revision = 1, .limit = 4 * 1024 * 1024 };
        _ = try db.applyOrderedHistoryConsumerControl(.{ .enroll = grant }, .{ .term = 1, .index = 1 }, false);
        _ = try db.applyOrderedSourceProducerClosure(owner, .{ .term = 1, .index = 2 }, false);
        try std.testing.expectError(error.SourcePendingWork, db.beginReplicatedCompletionTransaction(encoded, owner, 100, 101, true, .{ .term = 1, .index = 3 }));
        _ = try db.beginReplicatedOrdinaryCompletionTransaction(encoded, owner, 100, 101, true, .{ .term = 1, .index = 3 });
        try std.testing.expectError(error.SourceTransactionIdentityConflict, db.beginReplicatedCompletionTransaction(encoded, owner, 100, 101, true, .{ .term = 1, .index = 3 }));
        var old_prepare = request;
        old_prepare.completion_ordinary_producer = false;
        try std.testing.expectError(error.SourceTransactionIdentityConflict, db.writeReplicatedTransactionAtOrderedReceipt(id, old_prepare, .{ .term = 1, .index = 4 }));
        try std.testing.expectEqual(@as(u64, 3), (try db.orderedApplyReceipt()).?.index);
        try db.writeReplicatedTransactionAtOrderedReceipt(id, request, .{ .term = 1, .index = 4 });
        try std.testing.expect(try db.get(a, "ordinary-txn-row") == null);
    }
    {
        var db = try db_mod.DB.open(a, path, options);
        defer db.close();
        var old_prepare = request;
        old_prepare.completion_ordinary_producer = false;
        try std.testing.expectError(error.SourceTransactionIdentityConflict, db.writeReplicatedTransactionAtOrderedReceipt(id, old_prepare, .{ .term = 1, .index = 5 }));
        try std.testing.expectEqual(@as(u64, 4), (try db.orderedApplyReceipt()).?.index);
        // Fresh PREPARE retry retains original images and advances only its marker.
        try db.writeReplicatedTransactionAtOrderedReceipt(id, request, .{ .term = 1, .index = 5 });
        try db.resolveReplicatedCompletionTransactionAtOrderedReceipt(id, .committed, 200, .{ .term = 1, .index = 6 }, null, owner);
        const row = (try db.get(a, "ordinary-txn-row")) orelse return error.TestExpectedOrdinaryDocument;
        defer a.free(row);
        try std.testing.expectEqualStrings("{\"value\":1}", row);
        try db.resolveReplicatedCompletionTransactionAtOrderedReceipt(id, .committed, 200, .{ .term = 1, .index = 6 }, null, owner);
        try std.testing.expectEqual(@as(u64, 6), (try db.orderedApplyReceipt()).?.index);
        const resolution = try db.historyResolution(a, .{ .version = 1, .authority = .raft, .namespace = owner.namespace, .transaction_id = id, .owner_fence = owner.digest(), .manifest_digest = (try admission.manifest(200)).digest(), .commit_version = 200, .minimum_applied_index = 6 });
        try std.testing.expect(resolution.resolution != null);
        try std.testing.expect(resolution.resolution.?.outcome == .effects);
    }
}
