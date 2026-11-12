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

//! Durable journal tests use the actual metadata DocStore and restart path.
const std = @import("std");
const model = @import("retained_dataset_owners.zig");
const journal = @import("retained_dataset_owner_store.zig");
const Store = @import("storage/raft_apply_store.zig").RaftApplyStore;
fn planFor(resource: u64, source: u64) model.Plan {
    var bytes: [8]u8 = undefined;
    var dataset: model.Id = undefined;
    std.mem.writeInt(u64, &bytes, resource, .big);
    std.crypto.hash.sha2.Sha256.hash(&bytes, &dataset, .{});
    var source_id: model.Id = undefined;
    std.mem.writeInt(u64, &bytes, source, .big);
    std.crypto.hash.sha2.Sha256.hash(&bytes, &source_id, .{});
    var plan: model.Plan = .{ .id = @splat(0), .resource = resource, .dataset = dataset, .generation = 1, .definition = @splat(3), .source = source_id, .source_table = source, .topology_generation = 2, .membership = @splat(5) };
    var membership: @import("topology_protocol.zig").RangeMembershipAccumulator = .{};
    membership.add(71) catch unreachable;
    if (resource == 41) membership.add(72) catch unreachable;
    plan.membership = membership.finish(source).digest;
    plan.id = plan.identity();
    return plan;
}
fn ownerFor(plan: model.Plan, group: u64) model.Owner {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(&plan.source);
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, group, .big);
    hash.update(&bytes);
    return .{ .plan = plan.id, .group = group, .partition = hash.finalResult(), .incarnation = 1 };
}

test "system catalog retained owner journal preserves fair work and cancellation across rollback restart and name-index rebuild" {
    const a = std.testing.allocator;
    const group: u64 = 21;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/retained-owners", .{tmp.sub_path});
    defer a.free(path);
    var store = try Store.init(a, .{ .root_dir = path });
    var opened = true;
    defer if (opened) store.deinit();
    var first = planFor(41, 7);
    const owner_a = ownerFor(first, 71);
    const owner_b = ownerFor(first, 72);
    var second = planFor(42, 8);
    {
        var txn = try store.store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expect(try journal.create(a, &txn, group, first));
        try std.testing.expect(!try journal.create(a, &txn, group, first));
        try std.testing.expectEqual(@as(?journal.WorkKey, null), try journal.nextWork(&txn, group, .{}));
        first = try journal.register(a, &txn, group, .{ .resource = first.resource, .generation = 1 }, first.revision, owner_a);
        first = try journal.register(a, &txn, group, .{ .resource = first.resource, .generation = 1 }, first.revision, owner_b);
        first = try journal.transition(a, &txn, group, .{ .resource = first.resource, .generation = 1 }, first.revision, .complete_registration);
        try std.testing.expect(try journal.create(a, &txn, group, second));
        second = try journal.register(a, &txn, group, .{ .resource = second.resource, .generation = 1 }, second.revision, ownerFor(second, 71));
        for (0..1000) |index| {
            var unrelated = planFor(100 + index, 10000 + index);
            _ = try journal.create(a, &txn, group, unrelated);
            unrelated = try journal.register(a, &txn, group, .{ .resource = unrelated.resource, .generation = 1 }, unrelated.revision, ownerFor(unrelated, 71));
        }
        try txn.commit();
    }
    {
        var txn = try store.store.beginReadTxn();
        defer txn.abort();
        const one = (try journal.nextWork(&txn, group, .{})).?;
        const two = (try journal.nextWork(&txn, group, one.cursor)).?;
        const three = (try journal.nextWork(&txn, group, two.cursor)).?;
        try std.testing.expectEqual(@as(u64, 41), one.cursor.resource);
        try std.testing.expectEqual(@as(u64, 71), one.cursor.owner_group);
        try std.testing.expectEqual(@as(u64, 41), two.cursor.resource);
        try std.testing.expectEqual(@as(u64, 72), two.cursor.owner_group);
        try std.testing.expectEqual(@as(u64, 42), three.cursor.resource);
        try std.testing.expectEqualDeep(one, (try journal.nextWork(&txn, group, .{ .resource = std.math.maxInt(u64), .generation = std.math.maxInt(u64), .owner_group = std.math.maxInt(u64) })).?);
        var buffer: [64 * 1024]u8 = undefined;
        var bounded = std.heap.FixedBufferAllocator.init(&buffer);
        const root = (try journal.getPlan(bounded.allocator(), &txn, group, 41, 1)).?;
        var owner = (try journal.getOwner(bounded.allocator(), &txn, group, root.id, 71)).?;
        defer owner.deinit();
        try std.testing.expectEqualDeep(owner_a, owner.value);
    }
    {
        var txn = try store.store.beginWriteTxn();
        defer txn.abort();
        _ = try journal.register(a, &txn, group, .{ .resource = second.resource, .generation = 1 }, second.revision, ownerFor(second, 72));
    }
    {
        var txn = try store.store.beginWriteTxn();
        errdefer txn.abort();
        first = try journal.transition(a, &txn, group, .{ .resource = 41, .generation = 1 }, first.revision, .retire);
        try std.testing.expectError(error.RetainedOwnerPlanChanged, journal.releaseSource(a, &txn, group, .{ .resource = 41, .generation = 1 }, first.revision, @splat(8)));
        for ([_]model.Owner{ owner_a, owner_b }) |previous| {
            var revoked = previous;
            revoked.revision += 1;
            revoked.mode = .revoked;
            revoked.native_receipt = @splat(9);
            const cursor: journal.Cursor = .{ .resource = 41, .generation = 1, .owner_group = previous.group };
            const stale_revision = first.revision;
            first = try journal.acknowledge(a, &txn, group, cursor, first.revision, previous.revision, revoked);
            try std.testing.expectError(error.RetainedOwnerPlanChanged, journal.acknowledge(a, &txn, group, cursor, stale_revision, previous.revision, revoked));
        }
        first = try journal.releaseSource(a, &txn, group, .{ .resource = 41, .generation = 1 }, first.revision, @splat(8));
        first = try journal.finishCollection(a, &txn, group, .{ .resource = 41, .generation = 1 }, first.revision, @splat(10));
        try txn.commit();
    }
    try store.rebuildDerivedCatalogIndexes(group);
    // Raft replacement must carry all three journal projections, including
    // terminal cancellation receipts and work for unfinished resources.
    const snapshot = try store.snapshotBuilder().buildSnapshot(a, group);
    defer a.free(snapshot);
    const replica_path = try std.fmt.allocPrint(a, "{s}-replica", .{path});
    defer a.free(replica_path);
    var replica = try Store.init(a, .{ .root_dir = replica_path });
    defer replica.deinit();
    try std.testing.expect(try replica.snapshotBuilder().installSnapshot(a, group, 100, snapshot));
    {
        var txn = try replica.store.beginReadTxn();
        defer txn.abort();
        try std.testing.expectEqualDeep(first, (try journal.getPlan(a, &txn, group, 41, 1)).?);
        try std.testing.expectEqualDeep(second, (try journal.getPlan(a, &txn, group, 42, 1)).?);
        var revoked = (try journal.getOwner(a, &txn, group, first.id, 71)).?;
        defer revoked.deinit();
        try std.testing.expect(revoked.value.mode == .revoked);
        const next = (try journal.nextWork(&txn, group, .{})).?;
        try std.testing.expectEqual(@as(u64, 42), next.cursor.resource);
        try std.testing.expectEqual(@as(u64, 71), next.cursor.owner_group);
    }
    store.deinit();
    opened = false;
    store = try Store.init(a, .{ .root_dir = path });
    opened = true;
    {
        var txn = try store.store.beginReadTxn();
        defer txn.abort();
        const restored = (try journal.getPlan(a, &txn, group, 41, 1)).?;
        try std.testing.expectEqualDeep(first, restored);
        try std.testing.expect(restored.phase == .retired);
        const next = (try journal.nextWork(&txn, group, .{})).?;
        try std.testing.expectEqual(@as(u64, 42), next.cursor.resource);
        try std.testing.expectEqual(@as(u64, 71), next.cursor.owner_group);
        // An owner inserted into an aborted transaction never becomes work.
        var absent = try journal.getOwner(a, &txn, group, second.id, 72);
        defer if (absent) |*value| value.deinit();
        try std.testing.expect(absent == null);
        try std.testing.expectEqualDeep(second, (try journal.getPlan(a, &txn, group, 42, 1)).?);
    }
}
