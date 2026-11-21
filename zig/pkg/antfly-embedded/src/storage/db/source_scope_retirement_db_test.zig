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

//! Real DB/compiled-engine retirement. Registry binding is fixture setup only;
//! no production worker handover or remote coordinator read is manufactured.
const std = @import("std");
const db_mod = @import("db.zig");
const completion = @import("../transaction_completion.zig");
const scope = @import("../source_scope.zig");
const retirement = @import("../source_scope_retirement.zig");
const ordered = @import("../source_scope_retirement_ordered.zig");
pub fn run(a: std.mem.Allocator, path: []const u8, options: db_mod.OpenOptions, owner: completion.RoutedOwner) !void {
    var coordinator = owner;
    std.mem.writeInt(u64, coordinator.namespace[8..16], 17, .big);
    std.mem.writeInt(u64, coordinator.namespace[16..24], 18, .big);
    coordinator.group_id = 3;
    const spec: scope.Spec = .{ .actor_table_id = 3, .source_id = @splat(40), .generation = 1, .binding = @splat(41), .checkpoint_owner = try scope.Basis.fromOwner(coordinator) };
    const members = [_]completion.Participant{
        .{ .namespace = owner.namespace, .owner_fence = owner.digest(), .roles = completion.Roles.effects, .input_digest = @splat(5), .locator = "rows", .routed_owner = owner },
        .{ .namespace = coordinator.namespace, .owner_fence = coordinator.digest(), .roles = completion.Roles.checkpoint, .input_digest = @splat(6), .locator = "checkpoint", .routed_owner = coordinator },
    };
    const admission: completion.Admission = .{ .version = 3, .transaction_id = @splat(9), .admission_digest = @splat(11), .coordinator_namespace = coordinator.namespace, .coordinator_fence = coordinator.digest(), .participants = &members };
    const query: @import("../history_decision_wire.zig").Query = .{ .owner = coordinator, .transaction_id = admission.transaction_id, .minimum_applied_index = 1 };
    const reference: scope.Reference = .{ .domain = spec.domain(), .binding = spec.binding, .generation = 1, .token = 10, .sequence = 1 };
    const command: retirement.Command = .{ .version = 1, .intent = .{ .version = 1, .owner = owner, .coordinator = query, .reference = reference, .source_purpose = @splat(12), .admission_digest = admission.digest() }, .observation = .{ .request_digest = query.digest(), .cut = .{ .term = 2, .index = 6 }, .decision = .{ .record = .{ .status = .aborted, .begin_timestamp = 10, .created_at_ns = 11, .commit_version = 0, .finalized_at_ns = 20, .prepared = true, .intents_resolved = true, .digest = @splat(13), .source_purpose = @splat(12), .source_scope = reference }, .admission = .{ .admission = admission, .digest = admission.digest() } } } };
    {
        var db = try db_mod.DB.open(a, path, options);
        defer db.close();
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        _ = try scope.Store.bind(a, &txn, try scope.Basis.fromOwner(owner), spec, .{ .group_id = 1, .incarnation = owner.metadata_incarnation, .index = 15 });
        try txn.commit();
        const applied = try db.applyOrderedSourceRetirement(command, .{ .term = 1, .index = 8 }, false);
        try std.testing.expectEqual(@as(u64, 10), applied.applied.closed_through_token);
        try std.testing.expectEqualDeep(applied, try db.applyOrderedSourceRetirement(command, .{ .term = 1, .index = 8 }, false));
        const observed = try db.sourceScopeObservation(.{ .version = 1, .owner = owner, .domain = spec.domain(), .minimum_applied_index = 8 });
        try std.testing.expectEqual(@as(u64, 8), observed.cut.index);
        try std.testing.expectEqual(@as(u64, 10), observed.record.?.closed_through_token);
    }
    {
        var db = try db_mod.DB.open(a, path, options);
        defer db.close();
        const recovered = try db.orderedSourceRetirementOutcome(.{ .command = command, .entry = .{ .term = 1, .index = 8 }, .minimum_applied_index = 8 });
        try std.testing.expectEqual(@as(u64, 8), recovered.applied.cut.index);
        var different_read = command;
        different_read.observation.cut.index += 1;
        try std.testing.expectError(error.ConflictingOrderedApplyReceipt, db.orderedSourceRetirementOutcome(.{ .command = different_read, .entry = .{ .term = 1, .index = 8 }, .minimum_applied_index = 8 }));
        var conflicting = command;
        conflicting.intent.reference.binding[0] ^= 1;
        conflicting.observation.decision.?.record.source_scope = conflicting.intent.reference;
        const rejected = try db.applyOrderedSourceRetirement(conflicting, .{ .term = 1, .index = 9 }, false);
        try std.testing.expectEqual(ordered.Rejection.conflict, rejected.rejected);
        try std.testing.expectEqualDeep(rejected, try db.applyOrderedSourceRetirement(conflicting, .{ .term = 1, .index = 9 }, false));
        const observed = try db.sourceScopeObservation(.{ .version = 1, .owner = owner, .domain = spec.domain(), .minimum_applied_index = 9 });
        try std.testing.expectEqual(@as(u64, 9), observed.cut.index);
        try std.testing.expectEqual(@as(u64, 10), observed.record.?.closed_through_token);
        try std.testing.expectError(error.StorageReadTemporarilyUnavailable, db.orderedSourceRetirementOutcome(.{ .command = conflicting, .entry = .{ .term = 1, .index = 9 }, .minimum_applied_index = 10 }));
        const retried = try db.applyOrderedSourceRetirement(command, .{ .term = 1, .index = 10 }, false);
        try std.testing.expectEqual(@as(u64, 10), retried.applied.cut.index);
        try std.testing.expect((try db.orderedSourceRetirementOutcome(.{ .command = conflicting, .entry = .{ .term = 1, .index = 9 }, .minimum_applied_index = 10 })) == .superseded);
    }
    {
        var db = try db_mod.DB.open(a, path, options);
        defer db.close();
        const recovered = try db.orderedSourceRetirementOutcome(.{ .command = command, .entry = .{ .term = 1, .index = 10 }, .minimum_applied_index = 10 });
        try std.testing.expectEqual(@as(u64, 10), recovered.applied.cut.index);
        try std.testing.expectEqual(@as(u64, 10), recovered.applied.closed_through_token);
    }
    try @import("source_producer_policy_db_test.zig").run(a, path, options, owner, spec.domain());
}
