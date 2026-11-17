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

//! Real DB/compiled-engine qualification. Direct binding below is test setup;
//! it supplies no evidence that production legacy source workers have drained.
const std = @import("std");
const db_mod = @import("db.zig");
const control = @import("../source_scope_control.zig");
const ordered = @import("../source_scope_ordered.zig");
const scope = control.scope;
const completion = @import("../transaction_completion.zig");
pub fn run(a: std.mem.Allocator, path: []const u8) !void {
    const options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 7, .shard_id = 11, .range_id = 14 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    var owner: completion.RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = undefined, .table_binding = @splat(2), .root_generation = 0, .group_id = 2, .topology_epoch = 3 };
    owner.namespace = @import("online_source_contract.zig").namespaceBytes(options.identity_namespace.?);
    var command: control.Command = undefined;
    {
        var db = try db_mod.DB.open(a, path, options);
        defer db.close();
        owner.root_generation = db.core.root_generation;
        command = .{ .version = 1, .intent = .{ .version = 1, .owner = owner, .spec = .{ .actor_table_id = 3, .source_id = @splat(4), .generation = 1, .binding = @splat(5), .checkpoint_owner = try scope.Basis.fromOwner(owner) }, .minimum_metadata_index = 10, .action = .bind }, .metadata_cut = .{ .group_id = 1, .incarnation = owner.metadata_incarnation, .index = 10 } };
        const blocked = try db.applyOrderedSourceScopeControl(command, .{ .term = 1, .index = 1 }, false);
        try std.testing.expectEqual(ordered.Rejection.pending, blocked.rejected);
        const observed = try db.sourceScopeObservation(.{ .version = 1, .owner = owner, .domain = command.intent.spec.domain(), .minimum_applied_index = 1 });
        try std.testing.expect(!observed.header.strict);
        try std.testing.expect(observed.record == null);
        try std.testing.expectEqual(@as(u64, 1), observed.cut.index);
    }
    {
        var db = try db_mod.DB.open(a, path, options);
        defer db.close();
        const recovered = try db.orderedSourceScopeOutcome(.{ .command = command, .entry = .{ .term = 1, .index = 1 }, .minimum_applied_index = 1 });
        try std.testing.expectEqual(ordered.Rejection.pending, recovered.rejected);
        try std.testing.expectEqualDeep(recovered, try db.applyOrderedSourceScopeControl(command, .{ .term = 1, .index = 1 }, false));
        // Establish a strict scope only as fixture setup. Native controls never
        // infer this migration authorization from root/transaction absence.
        {
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            _ = try scope.Store.bind(a, &txn, try scope.Basis.fromOwner(owner), command.intent.spec, command.metadata_cut);
            try txn.commit();
        }
        command.metadata_cut.index = 11;
        const bound = try db.applyOrderedSourceScopeControl(command, .{ .term = 1, .index = 2 }, false);
        try std.testing.expectEqual(@as(u64, 11), bound.applied.header.metadata_floor);
        try std.testing.expectEqual(@as(u64, 10), bound.applied.record.?.bound_metadata_index);
        command.intent.action = .close;
        command.metadata_cut.index = 12;
        const closed = try db.applyOrderedSourceScopeControl(command, .{ .term = 1, .index = 3 }, false);
        try std.testing.expectEqual(scope.Phase.closed, closed.applied.record.?.phase);
        try std.testing.expectEqualDeep(scope.Cut{ .term = 1, .index = 3 }, closed.applied.record.?.closed_at.?);
        try std.testing.expectEqualDeep(closed, try db.applyOrderedSourceScopeControl(command, .{ .term = 1, .index = 3 }, false));
        command.intent.action = .{ .drop = .{ .term = 1, .index = 2 } };
        command.metadata_cut.index = 13;
        try std.testing.expectEqual(ordered.Rejection.conflict, (try db.applyOrderedSourceScopeControl(command, .{ .term = 1, .index = 4 }, false)).rejected);
        command.intent.action = .{ .drop = closed.applied.record.?.closed_at.? };
        const dropped = try db.applyOrderedSourceScopeControl(command, .{ .term = 1, .index = 5 }, false);
        try std.testing.expect(dropped.applied.record == null);
        try std.testing.expect(dropped.applied.header.strict);
        try std.testing.expectEqual(@as(u16, 0), dropped.applied.header.domains);
        command.metadata_cut.index = 14;
        const refreshed_drop = try db.applyOrderedSourceScopeControl(command, .{ .term = 1, .index = 6 }, false);
        try std.testing.expect(refreshed_drop.applied.record == null);
        try std.testing.expectEqual(@as(u64, 14), refreshed_drop.applied.header.metadata_floor);
        command.intent.action = .bind;
        command.metadata_cut.index = 11;
        try std.testing.expectEqual(ordered.Rejection.conflict, (try db.applyOrderedSourceScopeControl(command, .{ .term = 1, .index = 7 }, false)).rejected);
    }
    {
        var db = try db_mod.DB.open(a, path, options);
        defer db.close();
        const recovery: ordered.Recovery = .{ .command = command, .entry = .{ .term = 1, .index = 7 }, .minimum_applied_index = 7 };
        try std.testing.expectEqual(ordered.Rejection.conflict, (try db.orderedSourceScopeOutcome(recovery)).rejected);
        try std.testing.expectError(error.ConflictingOrderedApplyReceipt, db.applyOrderedSourceScopeControl(command, .{ .term = 2, .index = 7 }, false));
        var future = recovery;
        future.minimum_applied_index = 8;
        try std.testing.expectError(error.StorageReadTemporarilyUnavailable, db.orderedSourceScopeOutcome(future));
        var changed = command;
        changed.intent.owner.root_generation += 1;
        try std.testing.expectError(error.HistoryResynchronizationRequired, db.applyOrderedSourceScopeControl(changed, .{ .term = 1, .index = 8 }, false));
        var old = recovery;
        old.entry.index = 5;
        try std.testing.expect(try db.orderedSourceScopeOutcome(old) == .superseded);
        const observed = try db.sourceScopeObservation(.{ .version = 1, .owner = owner, .domain = command.intent.spec.domain(), .minimum_applied_index = 7 });
        try std.testing.expect(observed.header.strict);
        try std.testing.expectEqual(@as(u64, 14), observed.header.metadata_floor);
        try std.testing.expect(observed.record == null);
        try std.testing.expectEqual(@as(u64, 7), observed.cut.index);
    }
}
