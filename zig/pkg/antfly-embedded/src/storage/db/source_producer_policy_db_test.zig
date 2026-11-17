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

//! Real compiled-engine producer closure and delayed ordinary mutation checks.
//! Explicit producer labels are trusted fixture input; production ingress is
//! still unwired and this closure never establishes transaction drainage.
const std = @import("std");
const db_mod = @import("db.zig");
const completion = @import("../transaction_completion.zig");
const scope = @import("../source_scope.zig");
const policy = @import("../source_producer_policy.zig");
pub fn run(a: std.mem.Allocator, path: []const u8, options: db_mod.OpenOptions, owner: completion.RoutedOwner, domain: scope.Digest) !void {
    const before: @import("types.zig").BatchRequest = .{ .writes = &.{.{ .key = "producer-before", .value = "{}" }}, .sync_level = .write };
    const late: @import("types.zig").BatchRequest = .{ .writes = &.{.{ .key = "producer-late", .value = "{}" }}, .sync_level = .write };
    const ordinary: @import("types.zig").BatchRequest = .{ .writes = &.{.{ .key = "producer-ordinary", .value = "{}" }}, .sync_level = .write };
    {
        var db = try db_mod.DB.open(a, path, options);
        defer db.close();
        try db.applyOrderedCommittedMutation(before, .{ .term = 1, .index = 11 }, false);
        const closed = try db.applyOrderedSourceProducerClosure(owner, .{ .term = 1, .index = 12 }, false);
        try std.testing.expectEqual(@as(u64, 12), closed.first_closed_at.index);
        try std.testing.expectEqualDeep(closed, try db.applyOrderedSourceProducerClosure(owner, .{ .term = 1, .index = 12 }, false));
        try std.testing.expectError(error.SourceOutcomeUnresolved, db.applyOrderedSourceProducerClosure(owner, .{ .term = 1, .index = 11 }, false));
        try std.testing.expectError(error.SourcePendingWork, db.applyOrderedCommittedMutation(late, .{ .term = 1, .index = 13 }, false));
        try std.testing.expect(try db.get(a, "producer-late") == null);
        try std.testing.expectError(error.StorageReadTemporarilyUnavailable, db.sourceScopeObservation(.{ .version = 1, .owner = owner, .domain = domain, .minimum_applied_index = 13 }));
        try db.applyOrderedCommittedMutationWithProducer(ordinary, .ordinary, .{ .term = 1, .index = 13 }, false);
        try std.testing.expectError(error.SourcePendingWork, db.applyOrderedCommittedMutationWithProducer(late, .legacy_cdc, .{ .term = 1, .index = 14 }, false));
        try std.testing.expectEqualDeep(closed, try db.applyOrderedSourceProducerClosure(owner, .{ .term = 1, .index = 14 }, false));
    }
    {
        var db = try db_mod.DB.open(a, path, options);
        defer db.close();
        try std.testing.expectError(error.SourcePendingWork, db.applyOrderedCommittedMutation(late, .{ .term = 1, .index = 15 }, false));
        // Ordered replay is recognized before checking a newly closed policy.
        try db.applyOrderedCommittedMutation(ordinary, .{ .term = 1, .index = 13 }, false);
        const stored = (try db.get(a, "producer-ordinary")) orelse return error.TestExpectedOrdinaryDocument;
        defer a.free(stored);
        try std.testing.expect(try db.get(a, "producer-late") == null);
        const observed = try db.sourceScopeObservation(.{ .version = 1, .owner = owner, .domain = domain, .minimum_applied_index = 14 });
        try std.testing.expectEqual(@as(u64, 14), observed.cut.index);
        {
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            try txn.put(policy.key, "{}");
            try txn.commit();
        }
        try std.testing.expectError(error.SourceAdmissionCorrupt, db.applyOrderedCommittedMutationWithProducer(late, .ordinary, .{ .term = 1, .index = 15 }, false));
        try std.testing.expect(try db.get(a, "producer-late") == null);
    }
}
