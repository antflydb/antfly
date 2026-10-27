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

const std = @import("std");
const graph = @import("immutable_graph.zig");
const publication = @import("index_publication.zig");
const ds = @import("docstore.zig");
const lsm = @import("lsm_backend.zig");
const Writer = struct {
    inner: *ds.DocStore.Txn,
    remaining: ?usize = null,
    fn mutate(self: *@This()) !void {
        if (self.remaining) |*left| {
            if (left.* == 0) return error.OutOfMemory;
            left.* -= 1;
        }
    }
    pub fn get(self: *@This(), name: []const u8) ![]const u8 {
        return self.inner.getPhysical(name);
    }
    pub fn put(self: *@This(), name: []const u8, value: []const u8) !void {
        try self.mutate();
        try self.inner.put(name, value);
    }
    pub fn delete(self: *@This(), name: []const u8) !void {
        try self.mutate();
        try self.inner.delete(name);
    }
    pub fn poison(self: *@This()) void {
        self.inner.poison();
    }
    pub fn hasPrefix(self: *@This(), prefix: []const u8) !bool {
        return self.inner.hasPrefix(prefix);
    }
    pub fn openCursor(self: *@This()) !ds.DocStore.Txn.CursorAdapter {
        return self.inner.openPhysicalCursorAdapter();
    }
};
const Retirer = struct {
    block: bool = true,
    retired: usize = 0,
    pub fn retire(self: *@This(), _: graph.Digest, _: graph.Digest, payload: []const u8) !bool {
        if (self.block and std.mem.eql(u8, payload, "replaced")) return false;
        self.retired += 1;
        return true;
    }
};

test "index source recovery immutable family edges share children and wait for native reader drainage" {
    const a = std.testing.allocator;
    var backend = lsm.Backend.init(a, .{});
    defer backend.close();
    var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    const owner: graph.Digest = @splat(17);
    const slot: graph.Digest = @splat(18);
    const epoch: @import("index_source_epoch.zig").Owner = .{ .identity = owner, .current = 1, .floor = 0 };
    const old_binding = epoch.newBinding(@splat(19));
    const next_binding = epoch.newBinding(@splat(20));
    var old_pins = try publication.SharedReaders.open(a, old_binding);
    defer old_pins.deinit();
    var held = try old_pins.lease();
    defer held.deinit();
    var old_root: graph.Root = undefined;
    var next_root: graph.Root = undefined;
    var shared: graph.Digest = undefined;
    var shared_directory: graph.Digest = undefined;
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var writer: Writer = .{ .inner = &txn };
        try graph.initialize(&writer, owner);
        shared = try graph.stage(&writer, owner, .{ .kind = .leaf, .payload = "unchanged" }, .{});
        const replaced = try graph.stage(&writer, owner, .{ .kind = .leaf, .payload = "replaced" }, .{});
        shared_directory = try graph.stage(&writer, owner, .{ .kind = .branch, .level = 1, .children = &.{shared} }, .{});
        const changed_directory = try graph.stage(&writer, owner, .{ .kind = .branch, .level = 1, .children = &.{replaced} }, .{});
        const old = try graph.stage(&writer, owner, .{ .kind = .family, .children = &.{ shared_directory, changed_directory } }, .{});
        try std.testing.expect(try publication.stagePublishRoleInEpoch(a, &writer, old_binding, old, &.{1}, 1, .family));
        try std.testing.expect(try graph.compareAndSwap(&writer, owner, slot, null, old_binding.publication, old, .{}));
        old_root = (try graph.loadRoot(&writer, owner, slot)).?;
        try txn.commit();
    }
    // A parent can never retain a family root. This bounds structural depth
    // and prevents chains of whole historical families from pinning one another.
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        var writer: Writer = .{ .inner = &txn };
        try std.testing.expectError(error.InvalidImmutableNode, graph.stage(&writer, owner, .{ .kind = .family, .children = &.{old_root.node} }, .{}));
        try std.testing.expectError(error.RetainedEffectsTransactionFailed, txn.commit());
    }
    // Fail after one child's count has been increased. Poisoning must make
    // both the new-node charge and that increment impossible to commit.
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        var writer: Writer = .{ .inner = &txn, .remaining = 2 };
        try std.testing.expectError(error.OutOfMemory, graph.stage(&writer, owner, .{ .kind = .family, .children = &.{ shared, shared } }, .{}));
        try std.testing.expectError(error.RetainedEffectsTransactionFailed, txn.commit());
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var writer: Writer = .{ .inner = &txn };
        const before = try graph.Budget.load(&writer, owner);
        try std.testing.expectEqual(@as(u64, 5), before.nodes);
        try std.testing.expectEqual(@as(u64, 1), try graph.referenceCount(&writer, owner, shared));
        try std.testing.expectEqualDeep(shared, try graph.stage(&writer, owner, .{ .kind = .leaf, .payload = "unchanged" }, .{}));
        try std.testing.expectEqualDeep(before, try graph.Budget.load(&writer, owner));
        const current = try graph.openHeld(&writer, &held);
        const old_directory = try current.child(&writer, 1);
        const old_leaf = try old_directory.child(&writer, 0);
        try std.testing.expectEqualDeep(old_root.node, try graph.replacePath(&writer, &current, &.{ 1, 0 }, old_leaf.id, .{}));
        try std.testing.expectEqualDeep(before, try graph.Budget.load(&writer, owner));
        const changed = try graph.stage(&writer, owner, .{ .kind = .leaf, .payload = "new" }, .{});
        const next = try graph.replacePath(&writer, &current, &.{ 1, 0 }, changed, .{});
        try std.testing.expect(try publication.stagePublishRoleInEpoch(a, &writer, next_binding, next, &.{2}, 1, .family));
        try std.testing.expect(try graph.compareAndSwap(&writer, owner, slot, old_root, next_binding.publication, next, .{}));
        next_root = (try graph.loadRoot(&writer, owner, slot)).?;
        try std.testing.expect(!try graph.compareAndSwap(&writer, owner, slot, old_root, @splat(21), next, .{}));
        try publication.stageRetire(a, &writer, old_binding, &.{});
        try std.testing.expectEqual(@as(u64, 8), (try graph.Budget.load(&writer, owner)).nodes);
        try std.testing.expectEqual(@as(u64, 2), try graph.referenceCount(&writer, owner, shared_directory));
        // Sharing the directory adds one edge to it, not to each descendant.
        try std.testing.expectEqual(@as(u64, 1), try graph.referenceCount(&writer, owner, shared));
        try txn.commit();
    }
    old_pins.retire();
    try std.testing.expect(old_pins.drained() == null);
    var captured: graph.HeldNode = undefined;
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        var reader: Writer = .{ .inner = &txn };
        captured = try graph.openHeld(&reader, &held);
        const previous_directory = try captured.child(&reader, 1);
        const previous_leaf = try previous_directory.child(&reader, 0);
        try std.testing.expectEqualStrings("replaced", try previous_leaf.metadata());
        var json: std.Io.Writer.Allocating = .init(a);
        defer json.deinit();
        try std.testing.expectError(error.WriteFailed, std.json.Stringify.value(captured, .{}, &json.writer));
        try std.testing.expectError(error.UnexpectedToken, std.json.parseFromSlice(graph.HeldNode, a, "{}", .{}));
    }
    var unrelated: publication.ReaderPins = .{ .binding = next_binding };
    unrelated.retire();
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        var writer: Writer = .{ .inner = &txn };
        const decision = (try publication.decision(&writer, old_binding)).?;
        try std.testing.expectError(error.IndexSourceReadersActive, graph.releaseRetired(&writer, owner, decision, unrelated.drained().?));
        try std.testing.expectError(error.RetainedEffectsTransactionFailed, txn.commit());
    }
    held.deinit();
    try std.testing.expectError(error.IndexSourceLeaseClosed, captured.metadata());
    var retirer: Retirer = .{};
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var writer: Writer = .{ .inner = &txn };
        try std.testing.expect(try graph.releaseRetired(&writer, owner, (try publication.decision(&writer, old_binding)).?, old_pins.drained().?));
        try std.testing.expect(!try graph.releaseRetired(&writer, owner, (try publication.decision(&writer, old_binding)).?, old_pins.drained().?));
        try txn.commit();
    }
    for (0..8) |_| {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var writer: Writer = .{ .inner = &txn };
        const progress = try graph.collect(&writer, owner, 1, &retirer);
        try std.testing.expect(progress.scanned <= 1);
        try txn.commit();
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var writer: Writer = .{ .inner = &txn };
        try std.testing.expectEqual(@as(u64, 1), try graph.referenceCount(&writer, owner, shared));
        try std.testing.expectEqual(@as(u64, 6), (try graph.Budget.load(&writer, owner)).nodes);
        try std.testing.expect(try graph.removeRoot(&writer, owner, slot, next_root));
        try publication.stageRetire(a, &writer, next_binding, &.{});
        try std.testing.expect(try graph.releaseRetired(&writer, owner, (try publication.decision(&writer, next_binding)).?, unrelated.drained().?));
        try txn.commit();
    }
    retirer.block = false;
    for (0..16) |_| {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var writer: Writer = .{ .inner = &txn };
        _ = try graph.collect(&writer, owner, 2, &retirer);
        try txn.commit();
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        var writer: Writer = .{ .inner = &txn };
        try std.testing.expectEqualDeep(graph.Budget{}, try graph.Budget.load(&writer, owner));
        try std.testing.expectEqual(@as(usize, 3), retirer.retired);
    }
}

test "index source recovery released reader leases do not follow freed owner cells" {
    const a = std.testing.allocator;
    var owner = try publication.SharedReaders.open(a, .{ .owner = @splat(74), .publication = @splat(75) });
    var lease = try owner.lease();
    owner.deinit();
    lease.deinit();
    // No handle remains to keep the cell alive. Checking lifetime must precede
    // any attempt to load identity from that released cell.
    try std.testing.expectError(error.IndexSourceLeaseClosed, lease.binding());
    try std.testing.expectError(error.IndexSourceLeaseClosed, lease.clone());
}

test "index source recovery immutable graph collection and quotas survive restart" {
    const a = std.testing.allocator;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/family-graph", .{temporary.sub_path});
    defer a.free(path);
    const owner: graph.Digest = @splat(27);
    var leaf: graph.Digest = undefined;
    var retained_budget: graph.Budget = undefined;
    for (0..2) |pass| {
        var backend = try lsm.Backend.open(a, path, .{});
        defer backend.close();
        var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
        defer store.close();
        if (pass == 0) {
            {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                var writer: Writer = .{ .inner = &txn };
                try graph.initialize(&writer, owner);
                leaf = try graph.stage(&writer, owner, .{ .kind = .leaf, .payload = "replaced" }, .{});
                _ = try graph.stage(&writer, owner, .{ .kind = .branch, .level = 1, .children = &.{leaf} }, .{});
                try txn.commit();
            }
            {
                var txn = try store.beginWriteTxn();
                defer txn.abort();
                var writer: Writer = .{ .inner = &txn };
                try std.testing.expectError(error.ImmutableGraphFull, graph.stage(&writer, owner, .{ .kind = .leaf, .payload = "over quota" }, .{ .nodes = 2 }));
                try std.testing.expectError(error.RetainedEffectsTransactionFailed, txn.commit());
            }
            var retirer: Retirer = .{};
            for (0..4) |_| {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                var writer: Writer = .{ .inner = &txn };
                _ = try graph.collect(&writer, owner, 1, &retirer);
                try txn.commit();
            }
            {
                var txn = try store.beginReadTxn();
                defer txn.abort();
                var reader: Writer = .{ .inner = &txn };
                retained_budget = try graph.Budget.load(&reader, owner);
                try std.testing.expectEqual(@as(u64, 1), retained_budget.nodes);
                try std.testing.expectEqual(@as(u64, 0), try graph.referenceCount(&reader, owner, leaf));
            }
            try store.sync(true);
        } else {
            {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                var writer: Writer = .{ .inner = &txn };
                try graph.initialize(&writer, owner);
                try std.testing.expectEqualDeep(retained_budget, try graph.Budget.load(&writer, owner));
                const node = try graph.loadNode(&writer, owner, leaf);
                try std.testing.expectEqualStrings("replaced", node.payload());
                try txn.commit();
            }
            var retirer: Retirer = .{ .block = false };
            for (0..4) |_| {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                var writer: Writer = .{ .inner = &txn };
                _ = try graph.collect(&writer, owner, 1, &retirer);
                try txn.commit();
            }
            {
                var txn = try store.beginReadTxn();
                defer txn.abort();
                var reader: Writer = .{ .inner = &txn };
                try std.testing.expectEqualDeep(graph.Budget{}, try graph.Budget.load(&reader, owner));
            }
            // Losing the budget after complete reclamation still cannot reset
            // the ownership domain. The initialization witness is permanent.
            const prefix = "family_budget_v1:";
            var name: [prefix.len + 32]u8 = undefined;
            @memcpy(name[0..prefix.len], prefix);
            @memcpy(name[prefix.len..], &owner);
            try store.delete(&name);
            var txn = try store.beginWriteTxn();
            defer txn.abort();
            var writer: Writer = .{ .inner = &txn };
            try std.testing.expectError(error.ImmutableGraphCorrupt, graph.initialize(&writer, owner));
            try std.testing.expectError(error.RetainedEffectsTransactionFailed, txn.commit());
        }
    }
}

test "index source recovery family source cohorts select authenticated ordinals and copy bounded paths" {
    const family = @import("index_source_family.zig");
    const maps = @import("index_source_map.zig");
    const claims = @import("index_source_claim.zig");
    const a = std.testing.allocator;
    var backend = lsm.Backend.init(a, .{});
    defer backend.close();
    var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    const owner: graph.Digest = @splat(93);
    const slot: graph.Digest = @splat(94);
    const epoch: @import("index_source_epoch.zig").Owner = .{ .identity = owner, .current = 1, .families = true };
    const parent = epoch.newBinding(@splat(95));
    const layout: family.Layout = .{ .segment = 71, .documents = 4161, .physical = @splat(96), .configuration = @splat(97) };
    var root: graph.Digest = undefined;
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var writer: Writer = .{ .inner = &txn };
        try graph.initialize(&writer, owner);
        var builder = try family.Builder.init(owner, layout);
        var first: u32 = 0;
        while (first < layout.documents) {
            var entropy: [16]u8 = @splat(0);
            std.mem.writeInt(u32, entropy[0..4], first + 1, .little);
            const binding = epoch.newBinding(entropy);
            var refs: [64]claims.Reference = undefined;
            const count = try layout.count(first);
            for (refs[0..count]) |*ref| ref.* = .{ .binding = binding, .document = @splat(1), .revision = .{ .domain = .mutation, .value = 1 }, .root = @splat(2), .snapshot = @splat(3) };
            const plan = try maps.Plan.prepare(a, binding, try layout.identity(first), refs[0..count]);
            defer plan.deinit(a);
            try plan.stage(&writer);
            _ = try publication.stagePublishRoleInEpoch(a, &writer, binding, plan.root(), &.{}, 1, .source_leaf);
            try builder.append(&writer, (try publication.decision(&writer, binding)).?, .{});
            first += count;
        }
        root = try builder.finish(&writer, .{});
        _ = try publication.stagePublishRoleInEpoch(a, &writer, parent, root, &.{layout.segment}, 1, .family);
        try std.testing.expect(try graph.compareAndSwap(&writer, owner, slot, null, parent.publication, root, .{}));
        try txn.commit();
    }
    var pins = try publication.SharedReaders.open(a, parent);
    defer pins.deinit();
    var lease = try pins.lease();
    defer lease.deinit();
    var txn = try store.beginWriteTxn();
    errdefer txn.abort();
    var writer: Writer = .{ .inner = &txn };
    const held = try graph.openHeld(&writer, &lease);
    for ([_]u32{ 0, 63, 64, 4095, 4096, 4160 }) |document| {
        const selection = try family.select(&writer, &held, document);
        try std.testing.expectEqual(document / 64 * 64, selection.leaf.first);
        try std.testing.expectEqual(@as(u8, 3), selection.path_length);
        const binding: publication.Binding = .{ .owner = owner, .publication = selection.leaf.publication_id };
        const decision = (try publication.decision(&writer, binding)).?;
        try decision.requireSourceRole();
        try std.testing.expectError(error.IndexPublicationFenceMismatch, decision.requireRole(.segment));
        try std.testing.expectError(error.IndexPublicationFenceMismatch, decision.requireRole(.family));
        try std.testing.expectEqual(selection.leaf.count, (try maps.summary(&writer, binding, selection.leaf.root)).documents);
    }
    try std.testing.expectError(error.InvalidIndexSourceFamily, family.select(&writer, &held, layout.documents));
    const selection = try family.select(&writer, &held, 4160);
    var refs: [1]claims.Reference = .{.{ .binding = epoch.newBinding(@splat(98)), .document = @splat(4), .revision = .{ .domain = .mutation, .value = 2 }, .root = @splat(5), .snapshot = @splat(6) }};
    const plan = try maps.Plan.prepare(a, refs[0].binding, try layout.identity(selection.leaf.first), &refs);
    defer plan.deinit(a);
    try plan.stage(&writer);
    _ = try publication.stagePublishRoleInEpoch(a, &writer, refs[0].binding, plan.root(), &.{}, 1, .source_leaf);
    const replacement = try family.stageLeaf(&writer, layout, (try publication.decision(&writer, refs[0].binding)).?, selection.leaf.first, .{});
    const before = try graph.Budget.load(&writer, owner);
    const next = try graph.replacePath(&writer, &held, selection.path[0..selection.path_length], replacement, .{});
    const after = try graph.Budget.load(&writer, owner);
    try std.testing.expectEqual(@as(u64, 3), after.nodes - before.nodes);
    // Changed cohort adds only its two directories and the family root.
    const next_binding = epoch.newBinding(@splat(99));
    _ = try publication.stagePublishRoleInEpoch(a, &writer, next_binding, next, &.{layout.segment}, 1, .family);
    try std.testing.expect(try graph.compareAndSwap(&writer, owner, slot, (try graph.loadRoot(&writer, owner, slot)).?, next_binding.publication, next, .{}));
    try txn.commit();
    var next_pins = try publication.SharedReaders.open(a, next_binding);
    defer next_pins.deinit();
    var next_lease = try next_pins.lease();
    defer next_lease.deinit();
    var read = try store.beginReadTxn();
    defer read.abort();
    var reader: Writer = .{ .inner = &read };
    const current = try graph.openHeld(&reader, &next_lease);
    const changed = try family.select(&reader, &current, 4160);
    const previous = try family.select(&reader, &held, 4160);
    try std.testing.expect(!std.meta.eql(changed.leaf, previous.leaf));
    const unchanged_old = try family.select(&reader, &held, 1);
    const unchanged_new = try family.select(&reader, &current, 1);
    try std.testing.expectEqualDeep(unchanged_old.leaf, unchanged_new.leaf);
    lease.deinit();
    try std.testing.expectError(error.IndexSourceLeaseClosed, family.select(&reader, &held, 1));
}

test "index source recovery multi-path replacement copies shared ancestors once" {
    const a = std.testing.allocator;
    var backend = lsm.Backend.init(a, .{});
    defer backend.close();
    var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    const owner: graph.Digest = @splat(151);
    const epoch: @import("index_source_epoch.zig").Owner = .{ .identity = owner, .current = 1, .floor = 0 };
    const binding = epoch.newBinding(@splat(152));
    var pins = try publication.SharedReaders.open(a, binding);
    defer pins.deinit();
    var lease = try pins.lease();
    defer lease.deinit();
    var original: graph.Digest = undefined;
    var leaf: graph.Digest = undefined;
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var writer: Writer = .{ .inner = &txn };
        try graph.initialize(&writer, owner);
        leaf = try graph.stage(&writer, owner, .{ .kind = .leaf, .payload = "same" }, .{});
        const branch = try graph.stage(&writer, owner, .{ .kind = .branch, .level = 1, .children = &.{ leaf, leaf, leaf } }, .{});
        original = try graph.stage(&writer, owner, .{ .kind = .family, .children = &.{branch} }, .{});
        try std.testing.expect(try publication.stagePublishRoleInEpoch(a, &writer, binding, original, &.{1}, 1, .family));
        try txn.commit();
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        var writer: Writer = .{ .inner = &txn };
        const held = try graph.openHeld(&writer, &lease);
        const before = try graph.Budget.load(&writer, owner);
        try std.testing.expectEqualDeep(original, try graph.replacePaths(&writer, &held, &.{}, .{}));
        try std.testing.expectEqualDeep(original, try graph.replacePaths(&writer, &held, &.{
            .{ .path = &.{ 0, 0 }, .replacement = leaf },
            .{ .path = &.{ 0, 2 }, .replacement = leaf },
        }, .{}));
        try std.testing.expectEqualDeep(before, try graph.Budget.load(&writer, owner));
        const left = try graph.stage(&writer, owner, .{ .kind = .leaf, .payload = "left" }, .{});
        const right = try graph.stage(&writer, owner, .{ .kind = .leaf, .payload = "right" }, .{});
        const ready = try graph.Budget.load(&writer, owner);
        const next = try graph.replacePathsWithMetadata(&writer, &held, &.{
            .{ .path = &.{ 0, 0 }, .replacement = left },
            .{ .path = &.{ 0, 2 }, .replacement = right },
        }, "new generation metadata", .{});
        try std.testing.expectEqual(ready.nodes + 2, (try graph.Budget.load(&writer, owner)).nodes);
        const root = try graph.loadNode(&writer, owner, next);
        try std.testing.expectEqualStrings("new generation metadata", root.payload());
        const directory = try graph.loadNode(&writer, owner, root.child(0));
        try std.testing.expectEqualDeep(left, directory.child(0));
        try std.testing.expectEqualDeep(leaf, directory.child(1));
        try std.testing.expectEqualDeep(right, directory.child(2));
        try std.testing.expectEqual(@as(u64, 4), try graph.referenceCount(&writer, owner, leaf));
        // Metadata-only invalidation copies exactly the root and preserves the
        // old directory. Repeating identical metadata does not stage a node.
        const metadata_ready = try graph.Budget.load(&writer, owner);
        const metadata_only = try graph.replacePathsWithMetadata(&writer, &held, &.{}, "retry invalidated", .{});
        const metadata_root = try graph.loadNode(&writer, owner, metadata_only);
        try std.testing.expectEqual(metadata_ready.nodes + 1, (try graph.Budget.load(&writer, owner)).nodes);
        try std.testing.expectEqualStrings("retry invalidated", metadata_root.payload());
        try std.testing.expectEqualDeep(held.node.child(0), metadata_root.child(0));
        try std.testing.expectEqualDeep(original, try graph.replacePathsWithMetadata(&writer, &held, &.{}, "", .{}));
    }
    // Invalid, overlapping, duplicate and unsorted paths cannot commit any
    // staged graph changes. The original live receipt remains navigable.
    const bad = [_][2][]const u8{
        .{ &.{0}, &.{ 0, 1 } },      .{ &.{ 0, 0 }, &.{ 0, 0 } },
        .{ &.{ 0, 2 }, &.{ 0, 1 } }, .{ &.{ 0, 0 }, &.{ 0, 3 } },
    };
    for (bad) |paths| {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        var writer: Writer = .{ .inner = &txn };
        const held = try graph.openHeld(&writer, &lease);
        try std.testing.expectError(error.InvalidImmutableNode, graph.replacePaths(&writer, &held, &.{
            .{ .path = paths[0], .replacement = leaf },
            .{ .path = paths[1], .replacement = leaf },
        }, .{}));
        try std.testing.expectError(error.RetainedEffectsTransactionFailed, txn.commit());
    }
}

test "index source recovery field generations share packs and replace only authenticated field paths" {
    const fields = @import("index_field_generation.zig");
    const a = std.testing.allocator;
    var backend = lsm.Backend.init(a, .{});
    defer backend.close();
    var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    const owner: graph.Digest = @splat(181);
    const epoch: @import("index_source_epoch.zig").Owner = .{ .identity = owner, .current = 1, .floor = 0 };
    const original_binding = epoch.newBinding(@splat(182));
    const next_binding = epoch.newBinding(@splat(183));
    const slot: graph.Digest = @splat(184);
    var pins = try publication.SharedReaders.open(a, original_binding);
    defer pins.deinit();
    var lease = try pins.lease();
    defer lease.deinit();
    var next_pins = try publication.SharedReaders.open(a, next_binding);
    defer next_pins.deinit();
    var next_lease = try next_pins.lease();
    defer next_lease.deinit();
    const directory: fields.Directory = .{ .fields = 65, .documents = 2, .ordinal_inventory = @splat(185), .configuration = @splat(186) };
    var original_resource: graph.Digest = undefined;
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var writer: Writer = .{ .inner = &txn };
        try graph.initialize(&writer, owner);
        original_resource = try fields.stageResource(&writer, owner, .{ .file = 1, .payload_bytes = 1024, .physical_bytes = 1088, .identity = @splat(187) }, .{});
        var builder = try fields.DirectoryBuilder.init(owner, directory);
        for (0..65) |ordinal| {
            var name: [32]u8 = undefined;
            const node = try fields.stageField(&writer, owner, .{
                .name = try std.fmt.bufPrint(&name, "f{d:0>4}", .{ordinal}),
                .documents = directory.documents,
                .ordinal_inventory = directory.ordinal_inventory,
                .configuration = directory.configuration,
                .inputs = @splat(188),
                .sections = &.{.{ .kind = .typed_doc_values, .resource = 0, .offset = 0, .length = 1024, .checksum = 1 }},
            }, &.{original_resource}, .{});
            try builder.append(&writer, node, .{});
        }
        const tree = try builder.finish(&writer, .{});
        const root = try graph.stage(&writer, owner, .{ .kind = .family, .children = &.{tree}, .payload = "native fields fixture" }, .{});
        try std.testing.expect(try publication.stagePublishRoleInEpoch(a, &writer, original_binding, root, &.{1}, 1, .family));
        try std.testing.expect(try graph.compareAndSwap(&writer, owner, slot, null, original_binding.publication, root, .{}));
        try std.testing.expectEqual(@as(u64, 65), try graph.referenceCount(&writer, owner, original_resource));
        try txn.commit();
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var writer: Writer = .{ .inner = &txn };
        const held = try graph.openHeld(&writer, &lease);
        const first = try fields.select(&writer, &held, 0, 0);
        const last = try fields.select(&writer, &held, 0, 64);
        try std.testing.expectEqualStrings("f0000", (try first.field.descriptor()).name());
        try std.testing.expectEqualStrings("f0064", (try last.field.descriptor()).name());
        var resource = try last.field.resource(&writer, try (try last.field.descriptor()).section(0));
        try std.testing.expectEqual(@as(u64, 1), (try resource.descriptor()).file);
        const changed_pack = try fields.stageResource(&writer, owner, .{ .file = 2, .payload_bytes = 512, .physical_bytes = 576, .identity = @splat(189) }, .{});
        const changed = try fields.stageField(&writer, owner, .{
            .name = "f0064",
            .documents = directory.documents,
            .ordinal_inventory = directory.ordinal_inventory,
            .configuration = directory.configuration,
            .inputs = @splat(190),
            .sections = &.{.{ .kind = .typed_doc_values, .resource = 0, .offset = 0, .length = 512, .checksum = 2 }},
        }, &.{changed_pack}, .{});
        const before = try graph.Budget.load(&writer, owner);
        const next = try fields.replace(&writer, &held, &last, changed, .{});
        // Family, directory wrapper and two dense-tree ancestors only. Other
        // fields and their physical resources acquire no new descendant edges.
        try std.testing.expectEqual(before.nodes + 4, (try graph.Budget.load(&writer, owner)).nodes);
        try std.testing.expectEqual(@as(u64, 65), try graph.referenceCount(&writer, owner, original_resource));
        try std.testing.expect(try publication.stagePublishRoleInEpoch(a, &writer, next_binding, next, &.{2}, 1, .family));
        try std.testing.expect(try graph.compareAndSwap(&writer, owner, slot, (try graph.loadRoot(&writer, owner, slot)).?, next_binding.publication, next, .{}));
        try publication.stageRetire(a, &writer, original_binding, &.{});
        try txn.commit();
    }
    // A receiver rebases every field and acquires each distinct physical pack
    // once. The callback receives actual donor membership, never a bare hash.
    {
        const receiver: graph.Digest = @splat(191);
        const receiver_epoch: @import("index_source_epoch.zig").Owner = .{ .identity = receiver, .current = 1, .floor = 0 };
        const receiver_binding = receiver_epoch.newBinding(@splat(192));
        var receiver_pins = try publication.SharedReaders.open(a, receiver_binding);
        defer receiver_pins.deinit();
        var receiver_lease = try receiver_pins.lease();
        defer receiver_lease.deinit();
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        var writer: Writer = .{ .inner = &txn };
        try graph.initialize(&writer, receiver);
        const held = try graph.openHeld(&writer, &next_lease);
        const Importer = struct {
            writer: *Writer,
            owner: graph.Digest,
            copies: u64 = 0,
            pub fn importResource(self: *@This(), _: std.mem.Allocator, source: *const fields.HeldResource) !graph.Digest {
                var resource = try source.descriptor();
                resource.file = 100 + self.copies;
                self.copies += 1;
                return fields.stageResource(self.writer, self.owner, resource, .{});
            }
        };
        var importer: Importer = .{ .writer = &writer, .owner = receiver };
        const copied = try fields.rebase(a, &writer, &held, 0, &writer, receiver, &importer, .{}, .{});
        try std.testing.expectEqual(@as(u64, 2), importer.copies);
        const root = try graph.stage(&writer, receiver, .{ .kind = .family, .children = &.{copied}, .payload = "received native fields" }, .{});
        try std.testing.expect(try publication.stagePublishRoleInEpoch(a, &writer, receiver_binding, root, &.{3}, 1, .family));
        const received = try graph.openHeld(&writer, &receiver_lease);
        const first = try fields.select(&writer, &received, 0, 0);
        const last = try fields.select(&writer, &received, 0, 64);
        var first_resource = try first.field.resource(&writer, try (try first.field.descriptor()).section(0));
        var last_resource = try last.field.resource(&writer, try (try last.field.descriptor()).section(0));
        try std.testing.expectEqual(@as(u64, 100), (try first_resource.descriptor()).file);
        try std.testing.expectEqual(@as(u64, 101), (try last_resource.descriptor()).file);
        try std.testing.expectEqualStrings("f0064", (try last.field.descriptor()).name());
    }
    pins.retire();
    try std.testing.expect(pins.drained() == null);
    var old_resource: fields.HeldResource = undefined;
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        var reader: Writer = .{ .inner = &txn };
        const old = try graph.openHeld(&reader, &lease);
        const next = try graph.openHeld(&reader, &next_lease);
        const old_last = try fields.select(&reader, &old, 0, 64);
        const next_last = try fields.select(&reader, &next, 0, 64);
        old_resource = try old_last.field.resource(&reader, try (try old_last.field.descriptor()).section(0));
        var new_resource = try next_last.field.resource(&reader, try (try next_last.field.descriptor()).section(0));
        try std.testing.expectEqual(@as(u64, 1), (try old_resource.descriptor()).file);
        try std.testing.expectEqual(@as(u64, 2), (try new_resource.descriptor()).file);
        const old_first = try fields.select(&reader, &old, 0, 0);
        const new_first = try fields.select(&reader, &next, 0, 0);
        try std.testing.expectEqualDeep(old_first.field.node.id, new_first.field.node.id);
    }
    lease.deinit();
    try std.testing.expectError(error.IndexSourceLeaseClosed, old_resource.descriptor());
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var writer: Writer = .{ .inner = &txn };
        try std.testing.expect(try graph.releaseRetired(&writer, owner, (try publication.decision(&writer, original_binding)).?, pins.drained().?));
        try txn.commit();
    }
    var retirer: Retirer = .{ .block = false };
    for (0..16) |_| {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var writer: Writer = .{ .inner = &txn };
        _ = try graph.collect(&writer, owner, 128, &retirer);
        try txn.commit();
    }
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        var reader: Writer = .{ .inner = &txn };
        try std.testing.expectEqual(@as(u64, 64), try graph.referenceCount(&reader, owner, original_resource));
        try std.testing.expectEqual(@as(usize, 0), retirer.retired);
    }
}

test "index source recovery physical field pins release logical sources before unchanged packs" {
    const fields = @import("index_field_generation.zig");
    const a = std.testing.allocator;
    var backend = lsm.Backend.init(a, .{});
    defer backend.close();
    var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    const owner: graph.Digest = @splat(211);
    const epochs = @import("index_source_epoch.zig");
    const epoch: epochs.Owner = .{ .identity = owner, .current = 1, .floor = 0, .families = true, .fields = true };
    const field_binding = epoch.newBinding(@splat(212));
    const family_binding = epoch.newBinding(@splat(213));
    const slot: graph.Digest = @splat(214);
    var field_pins = try publication.SharedReaders.open(a, field_binding);
    defer field_pins.deinit();
    var field_lease = try field_pins.lease();
    defer field_lease.deinit();
    var family_pins = try publication.SharedReaders.open(a, family_binding);
    defer family_pins.deinit();
    var source_leaf: graph.Digest = undefined;
    var field_root: graph.Digest = undefined;
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var writer: Writer = .{ .inner = &txn };
        try writer.put(epochs.owner_key, &try epoch.encode());
        try graph.initialize(&writer, owner);
        const pack = try fields.stageResource(&writer, owner, .{ .file = 11, .payload_bytes = 5, .physical_bytes = 69, .identity = @splat(215) }, .{});
        const field = try fields.stageField(&writer, owner, .{
            .name = "body",
            .documents = 2,
            .ordinal_inventory = @splat(216),
            .configuration = @splat(217),
            .inputs = @splat(218),
            .sections = &.{.{ .kind = .typed_doc_values, .resource = 0, .offset = 0, .length = 5, .checksum = 1 }},
        }, &.{pack}, .{});
        var builder = try fields.DirectoryBuilder.init(owner, .{ .fields = 1, .documents = 2, .ordinal_inventory = @splat(216), .configuration = @splat(217), .publication_id = field_binding.publication });
        try builder.append(&writer, field, .{});
        field_root = try builder.finish(&writer, .{});
        try fields.stagePublication(a, &writer, field_binding, field_root, .{});
        source_leaf = try graph.stage(&writer, owner, .{ .kind = .leaf, .payload = "superseded source" }, .{});
        const family = try graph.stage(&writer, owner, .{ .kind = .family, .children = &.{ source_leaf, field_root }, .payload = "logical source and physical fields" }, .{});
        try std.testing.expect(try publication.stagePublishRoleInEpoch(a, &writer, family_binding, family, &.{1}, 1, .family));
        try std.testing.expect(try graph.compareAndSwap(&writer, owner, slot, null, family_binding.publication, family, .{}));
        try txn.commit();
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var writer: Writer = .{ .inner = &txn };
        const root = (try graph.loadRoot(&writer, owner, slot)).?;
        try std.testing.expect(try graph.removeRoot(&writer, owner, slot, root));
        try publication.stageRetire(a, &writer, family_binding, &.{});
        try txn.commit();
    }
    family_pins.retire();
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var writer: Writer = .{ .inner = &txn };
        try std.testing.expect(try graph.releaseRetired(&writer, owner, (try publication.decision(&writer, family_binding)).?, family_pins.drained().?));
        try txn.commit();
    }
    const RetireResources = struct {
        writer: *Writer = undefined,
        allocator: std.mem.Allocator,
        binding: publication.Binding,
        pins: *publication.SharedReaders.Handle,
        sources: usize = 0,
        packs: usize = 0,
        pub fn retire(self: *@This(), actual_owner: graph.Digest, node: graph.Digest, payload: []const u8) !bool {
            if (!std.mem.eql(u8, &actual_owner, &self.binding.owner)) return error.IndexSourceOwnerMismatch;
            if (std.mem.eql(u8, payload, "superseded source")) {
                self.sources += 1;
                return true;
            }
            if (std.mem.startsWith(u8, payload, "IFR1")) {
                _ = try fields.Resource.decode(payload);
                self.packs += 1;
                return true;
            }
            const directory = try fields.Directory.decode(payload);
            if (!std.mem.eql(u8, &directory.publication_id.?, &self.binding.publication)) return error.IndexPublicationFenceMismatch;
            try publication.stageRetire(self.allocator, self.writer, self.binding, &.{});
            self.pins.retire();
            const drained = self.pins.drained() orelse return false;
            try fields.completeRetirement(self.writer, node, (try publication.decision(self.writer, self.binding)).?, drained);
            return true;
        }
    };
    var retirer: RetireResources = .{ .allocator = a, .binding = field_binding, .pins = &field_pins };
    for (0..16) |_| {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var writer: Writer = .{ .inner = &txn };
        retirer.writer = &writer;
        _ = try graph.collect(&writer, owner, 128, &retirer);
        try txn.commit();
    }
    var held_fields: graph.HeldNode = undefined;
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        var reader: Writer = .{ .inner = &txn };
        try std.testing.expectEqual(@as(usize, 1), retirer.sources);
        try std.testing.expectEqual(@as(usize, 0), retirer.packs);
        try std.testing.expectError(error.ImmutableGraphCorrupt, graph.loadNode(&reader, owner, source_leaf));
        try std.testing.expectEqual(@as(u64, 0), try graph.referenceCount(&reader, owner, field_root));
        held_fields = try graph.openHeldFields(&reader, &field_lease);
        const selected = try fields.selectFields(&reader, &held_fields, 0);
        try std.testing.expectEqualStrings("body", (try selected.field.descriptor()).name());
        try std.testing.expectError(error.IndexPublicationFenceMismatch, (try publication.decision(&reader, field_binding)).?.requireSourceRole());
    }
    field_lease.deinit();
    try std.testing.expectError(error.IndexSourceLeaseClosed, held_fields.metadata());
    for (0..16) |_| {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var writer: Writer = .{ .inner = &txn };
        retirer.writer = &writer;
        _ = try graph.collect(&writer, owner, 128, &retirer);
        try txn.commit();
    }
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        var reader: Writer = .{ .inner = &txn };
        try std.testing.expectEqual(@as(usize, 1), retirer.packs);
        try std.testing.expectEqualDeep(graph.Budget{}, try graph.Budget.load(&reader, owner));
        try std.testing.expect(try graph.resourceNode(&reader, owner, field_binding.publication) == null);
    }
}

test "index source recovery immutable pack file reads pin inode and runtime after parent close" {
    const io = @import("lsm_backend/storage_io.zig");
    const a = std.testing.allocator;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/immutable-pack", .{temporary.sub_path});
    defer a.free(path);
    var native = try io.NativeStorage.init(a, .threaded);
    var native_live = true;
    defer if (native_live) native.deinit();
    const storage = native.storage();
    if (storage.vtable.open_leased_immutable_source == null) return error.SkipZigTest;
    try storage.writeFileAbsolute(path, "before!");
    var held = try storage.openLeasedImmutableSource(a, path);
    defer held.close();
    const Sweep = struct {
        fn run(allocator: std.mem.Allocator, view: io.Storage, name: []const u8) !void {
            var source = try view.openLeasedImmutableSource(allocator, name);
            defer source.close();
            var bytes: [7]u8 = undefined;
            try source.readInto(0, &bytes);
            try std.testing.expectEqualStrings("before!", &bytes);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Sweep.run, .{ storage, path });
    {
        var writer = try storage.beginAtomicWrite(a, path);
        var active = true;
        defer if (active) writer.abort();
        try writer.appendSlice("after!!");
        active = false;
        try writer.finish();
    }
    var runtime = try storage.acquireLease();
    defer runtime.deinit();
    native_live = false;
    native.deinit();
    var bytes: [7]u8 = undefined;
    try held.readInto(0, &bytes);
    try std.testing.expectEqualStrings("before!", &bytes);
    // A pre-existing storage lease can lazily open another immutable pack after
    // the parent closes. No fresh owner lease is admitted during shutdown.
    try std.testing.expectError(error.StorageClosed, runtime.view.acquireLease());
    var current = try runtime.view.openLeasedImmutableSource(a, path);
    defer current.close();
    try current.readInto(0, &bytes);
    try std.testing.expectEqualStrings("after!!", &bytes);
    try runtime.view.deleteFileAbsolute(path);
    try current.readInto(0, &bytes);
    try std.testing.expectEqualStrings("after!!", &bytes);
    try std.testing.expectError(error.EndOfStream, held.readInto(1, &bytes));
}

test "index source recovery field pack providers pin cold versions and outlive metadata and storage owners" {
    const fields = @import("index_field_generation.zig");
    const field_source = @import("index_field_source.zig");
    const pack = @import("../field_pack.zig");
    const segment = @import("../segment.zig");
    const io = @import("lsm_backend/storage_io.zig");
    const a = std.testing.allocator;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const root_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/field-packs", .{temporary.sub_path});
    defer a.free(root_path);
    const pack_path = try std.fmt.allocPrint(a, "{s}/77.seg", .{root_path});
    defer a.free(pack_path);
    var native = try io.NativeStorage.init(a, .threaded);
    var native_live = true;
    defer if (native_live) native.deinit();
    const storage = native.storage();
    if (storage.vtable.open_leased_immutable_source == null) return error.SkipZigTest;
    try storage.createDirPath(root_path);
    var bytes = segment.MemorySegmentSink.init(a);
    defer bytes.deinit();
    var sink = bytes.sink();
    var encoder = try pack.Encoder.init(a, &sink, .{});
    defer encoder.deinit();
    try encoder.append("alpha");
    const metadata = try encoder.finish();
    {
        var writer = try storage.beginAtomicWrite(a, pack_path);
        var active = true;
        defer if (active) writer.abort();
        try writer.appendSlice(bytes.out.items);
        active = false;
        try writer.finish();
    }
    var backend = lsm.Backend.init(a, .{});
    var backend_live = true;
    defer if (backend_live) backend.close();
    var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    var store_live = true;
    defer if (store_live) store.close();
    const epochs = @import("index_source_epoch.zig");
    const epoch: epochs.Owner = .{ .identity = @splat(221), .current = 1, .floor = 0, .families = true, .fields = true };
    const binding = epoch.newBinding(@splat(222));
    var pins = try publication.SharedReaders.open(a, binding);
    defer pins.deinit();
    var lease = try pins.lease();
    defer lease.deinit();
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var writer: Writer = .{ .inner = &txn };
        try writer.put(epochs.owner_key, &try epoch.encode());
        try graph.initialize(&writer, epoch.identity);
        const resource = try fields.stageResource(&writer, epoch.identity, .{ .file = 77, .payload_bytes = metadata.payload_bytes, .physical_bytes = bytes.out.items.len, .identity = metadata.identity }, .{});
        const field = try fields.stageField(&writer, epoch.identity, .{
            .name = "body",
            .documents = 1,
            .ordinal_inventory = @splat(223),
            .configuration = @splat(224),
            .inputs = @splat(225),
            .sections = &.{.{ .kind = .typed_doc_values, .resource = 0, .offset = 0, .length = 5, .checksum = 0 }},
        }, &.{resource}, .{});
        var builder = try fields.DirectoryBuilder.init(epoch.identity, .{ .fields = 1, .documents = 1, .ordinal_inventory = @splat(223), .configuration = @splat(224), .publication_id = binding.publication });
        try builder.append(&writer, field, .{});
        const root = try builder.finish(&writer, .{});
        try fields.stagePublication(a, &writer, binding, root, .{});
        try txn.commit();
    }
    const baseline = native.snapshotStats().fd_admitted_descriptors;
    var inventory: field_source.Inventory = undefined;
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        var reader: Writer = .{ .inner = &txn };
        const Sweep = struct {
            fn run(allocator: std.mem.Allocator, source_reader: *Writer, pin: *const publication.SharedReaders.Lease, view: io.Storage, directory: []const u8) !void {
                var input = try field_source.open(allocator, source_reader, pin, view, directory, .{});
                defer input.deinit();
                var source = (try input.section(allocator, "body", .typed_doc_values)).?;
                defer source.close();
                var output: [5]u8 = undefined;
                try source.readInto(0, &output);
                try std.testing.expectEqualStrings("alpha", &output);
            }
        };
        try std.testing.checkAllAllocationFailures(a, Sweep.run, .{ &reader, &lease, storage, root_path });
        try std.testing.expectEqual(baseline, native.snapshotStats().fd_admitted_descriptors);
        inventory = try field_source.open(a, &reader, &lease, storage, root_path, .{});
    }
    defer inventory.deinit();
    try std.testing.expectEqual(baseline, native.snapshotStats().fd_admitted_descriptors);
    var first = (try inventory.section(a, "body", .typed_doc_values)).?;
    defer first.close();
    var second = (try inventory.section(a, "body", .typed_doc_values)).?;
    defer second.close();
    try std.testing.expectEqual(baseline, native.snapshotStats().fd_admitted_descriptors);
    try std.testing.expect(try inventory.section(a, "missing", .typed_doc_values) == null);
    {
        var replacement = try storage.beginAtomicWrite(a, pack_path);
        var active = true;
        defer if (active) replacement.abort();
        try replacement.appendSlice("unrelated replacement, before the first pack read");
        active = false;
        try replacement.finish();
    }
    var output: [5]u8 = undefined;
    try first.readInto(0, &output);
    try std.testing.expectEqualStrings("alpha", &output);
    try second.readInto(0, &output);
    try std.testing.expectEqual(baseline, native.snapshotStats().fd_admitted_descriptors);
    inventory.deinit();
    lease.deinit();
    pins.retire();
    try std.testing.expect(pins.drained() == null);
    store_live = false;
    store.close();
    backend_live = false;
    backend.close();
    native_live = false;
    native.deinit();
    try second.readInto(0, &output);
    try std.testing.expectEqualStrings("alpha", &output);
    first.close();
    try std.testing.expect(pins.drained() == null);
    second.close();
    try std.testing.expect(pins.drained() != null);
}

test "index source recovery packed field generations use normal segment decoding and scoring" {
    const field_build = @import("index_field_build.zig");
    const field_source = @import("index_field_source.zig");
    const pack = @import("../field_pack.zig");
    const segment = @import("../segment.zig");
    const index = @import("../index.zig");
    const intro = @import("../introducer.zig");
    const io = @import("lsm_backend/storage_io.zig");
    const a = std.testing.allocator;
    const original = try intro.buildSegment(a, .{ .docs = &.{
        .{ .id = "one", .stored_data = "{}", .doc_ordinal = 11, .fields = &.{.{ .field_name = "body", .hits = &.{ .{ .term = "alpha", .freq = 1, .positions = &.{0} }, .{ .term = "beta", .freq = 1, .positions = &.{1} } } }} },
        .{ .id = "two", .stored_data = "{}", .doc_ordinal = 12, .fields = &.{.{ .field_name = "body", .hits = &.{.{ .term = "alpha", .freq = 1, .positions = &.{0} }} }} },
    } });
    defer a.free(original);
    var reference = try segment.SegmentReader.init(a, original);
    defer reference.deinit();
    const Inputs = struct {
        pub fn inputForField(_: *@This(), name: []const u8) ![32]u8 {
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(name, &digest, .{});
            return digest;
        }
    };
    var inputs: Inputs = .{};
    var memory = segment.MemorySegmentSink.init(a);
    defer memory.deinit();
    var sink = memory.sink();
    var encoder = try pack.Encoder.init(a, &sink, .{});
    defer encoder.deinit();
    var plan = try field_build.prepare(a, &reference, &encoder, 88, @splat(231), @splat(232), &inputs);
    defer plan.deinit();
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const root_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/packed-reader", .{temporary.sub_path});
    defer a.free(root_path);
    const pack_path = try std.fmt.allocPrint(a, "{s}/88.seg", .{root_path});
    defer a.free(pack_path);
    var native = try io.NativeStorage.init(a, .threaded);
    defer native.deinit();
    const storage = native.storage();
    if (storage.vtable.open_leased_immutable_source == null) return error.SkipZigTest;
    try storage.createDirPath(root_path);
    {
        var writer = try storage.beginAtomicWrite(a, pack_path);
        var active = true;
        defer if (active) writer.abort();
        try writer.appendSlice(memory.out.items);
        active = false;
        try writer.finish();
    }
    var backend = lsm.Backend.init(a, .{});
    defer backend.close();
    var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    const epochs = @import("index_source_epoch.zig");
    const epoch: epochs.Owner = .{ .identity = @splat(233), .current = 1, .floor = 0, .families = true, .fields = true };
    const binding = epoch.newBinding(@splat(234));
    var pins = try publication.SharedReaders.open(a, binding);
    defer pins.deinit();
    var lease = try pins.lease();
    defer lease.deinit();
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var writer: Writer = .{ .inner = &txn };
        try writer.put(epochs.owner_key, &try epoch.encode());
        try graph.initialize(&writer, epoch.identity);
        const field_root = try plan.stage(&writer, binding, .{});
        const family = @import("index_source_family.zig");
        const maps = @import("index_source_map.zig");
        const claims = @import("index_source_claim.zig");
        const parent = epoch.newBinding(@splat(235));
        const leaf_binding = epoch.newBinding(@splat(236));
        const layout: family.Layout = .{ .segment = 7, .documents = reference.doc_count, .physical = field_root, .configuration = plan.configuration, .field_inventory = .{ .ordinal = plan.ordinal_inventory, .count = @intCast(plan.records.len) } };
        var same_ordinals = layout;
        same_ordinals.segment = 999;
        same_ordinals.physical = @splat(237);
        same_ordinals.configuration = @splat(238);
        try std.testing.expectEqualDeep(try layout.identity(0), try same_ordinals.identity(0));
        const references: [2]claims.Reference = .{
            .{ .binding = leaf_binding, .document = @splat(1), .revision = .{ .domain = .mutation, .value = 1 }, .root = @splat(2), .snapshot = @splat(3) },
            .{ .binding = leaf_binding, .document = @splat(4), .revision = .{ .domain = .mutation, .value = 1 }, .root = @splat(5), .snapshot = @splat(6) },
        };
        const source_map = try maps.Plan.prepare(a, leaf_binding, try layout.identity(0), &references);
        defer source_map.deinit(a);
        try source_map.stage(&writer);
        _ = try publication.stagePublishRoleInEpoch(a, &writer, leaf_binding, source_map.root(), &.{}, 1, .source_leaf);
        var family_builder = try family.Builder.initWithFields(epoch.identity, layout, field_root);
        try family_builder.append(&writer, (try publication.decision(&writer, leaf_binding)).?, .{});
        const family_root = try family_builder.finish(&writer, .{});
        _ = try publication.stagePublishRoleInEpoch(a, &writer, parent, family_root, &.{layout.segment}, 1, .family);
        var parent_pins = try publication.SharedReaders.open(a, parent);
        defer parent_pins.deinit();
        var parent_lease = try parent_pins.lease();
        defer parent_lease.deinit();
        const held_family = try graph.openHeld(&writer, &parent_lease);
        const selected = try family.select(&writer, &held_family, 1);
        try std.testing.expectEqualDeep(layout, selected.layout);
        try std.testing.expectEqual(@as(usize, 2), held_family.node.childCount());
        try txn.commit();
    }
    // Reuse is proven under an actual field lease and survives the metadata
    // read transaction. The unchanged body contributes no bytes to the pack.
    var incremental_bytes = segment.MemorySegmentSink.init(a);
    defer incremental_bytes.deinit();
    var incremental_sink = incremental_bytes.sink();
    var incremental_encoder = try pack.Encoder.init(a, &incremental_sink, .{});
    defer incremental_encoder.deinit();
    var incremental: field_build.Plan = undefined;
    var original_field: graph.Digest = undefined;
    const body_ordinal: u32 = for (plan.admission.fields, 0..) |field, ordinal| {
        if (std.mem.eql(u8, field.name, "body")) break @intCast(ordinal);
    } else return error.MissingBodyField;
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        var reader: Writer = .{ .inner = &txn };
        incremental = try field_build.prepareIncremental(a, &reference, &incremental_encoder, 89, plan.ordinal_inventory, plan.configuration, &inputs, &reader, &lease);
        const held = try graph.openHeldFields(&reader, &lease);
        original_field = (try @import("index_field_generation.zig").selectFields(&reader, &held, body_ordinal)).field.node.id;
    }
    defer incremental.deinit();
    try std.testing.expectEqual(@as(usize, 0), incremental.records[body_ordinal].sections.len);
    try std.testing.expect(incremental.records[body_ordinal].reused != null);
    var expected_payload = plan.admission.stored_length;
    for (incremental.admission.fields, incremental.records) |field, record| {
        if (field.name.len != 0 and field.name[0] == 0) {
            try std.testing.expect(record.reused == null);
            for (record.sections) |part| expected_payload += part.length;
        }
    }
    try std.testing.expectEqual(expected_payload, incremental.resource.payload_bytes);
    try std.testing.expect(incremental.resource.payload_bytes < plan.resource.payload_bytes);
    const next_binding = epoch.newBinding(@splat(241));
    var next_pins = try publication.SharedReaders.open(a, next_binding);
    defer next_pins.deinit();
    var next_lease = try next_pins.lease();
    defer next_lease.deinit();
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var writer: Writer = .{ .inner = &txn };
        _ = try incremental.stage(&writer, next_binding, .{});
        const held = try graph.openHeldFields(&writer, &next_lease);
        const reused = try @import("index_field_generation.zig").selectFields(&writer, &held, body_ordinal);
        try std.testing.expectEqualDeep(original_field, reused.field.node.id);
        // Fresh ordinal metadata gives each directory its own branch;
        // both branches independently own the same unchanged body field.
        try std.testing.expectEqual(@as(u64, 2), try graph.referenceCount(&writer, epoch.identity, original_field));
        try txn.commit();
    }
    var inventory: field_source.Inventory = undefined;
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        var reader: Writer = .{ .inner = &txn };
        inventory = try field_source.open(a, &reader, &lease, storage, root_path, .{});
    }
    defer inventory.deinit();
    const baseline = native.snapshotStats().fd_admitted_descriptors;
    const Sweep = struct {
        fn run(allocator: std.mem.Allocator, input: field_source.Inventory) !void {
            var authenticated = try field_source.authenticated(input, allocator);
            defer authenticated.deinit();
            var reader = try authenticated.initReader(allocator);
            defer reader.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(a, Sweep.run, .{inventory});
    var authenticated = try field_source.authenticated(inventory, a);
    var authenticated_owned = true;
    defer if (authenticated_owned) authenticated.deinit();
    {
        var reader = try authenticated.initReader(a);
        defer reader.deinit();
        try std.testing.expectEqual(reference.doc_count, reader.doc_count);
    }
    try std.testing.expectEqual(baseline, native.snapshotStats().fd_admitted_descriptors);
    var packed_index = try index.IndexWriter.init(a);
    defer packed_index.deinit();
    const packed_data = try index.SegmentData.fromAuthenticated(authenticated);
    authenticated_owned = false;
    try packed_index.addSegmentWithIdData(1, packed_data);
    inventory.deinit();
    var original_index = try index.IndexWriter.init(a);
    defer original_index.deinit();
    try original_index.addSegment(original);
    for ([_][]const u8{ "alpha", "beta", "absent" }) |term| {
        const expected = try original_index.snapshot().search(a, "body", &.{term}, 10);
        defer a.free(expected.hits);
        const actual = try packed_index.snapshot().search(a, "body", &.{term}, 10);
        defer a.free(actual.hits);
        try std.testing.expectEqual(expected.total_count, actual.total_count);
        try std.testing.expectEqual(expected.hits.len, actual.hits.len);
        for (expected.hits, actual.hits) |old, new| {
            try std.testing.expectEqual(old.doc_id, new.doc_id);
            try std.testing.expectApproxEqAbs(old.score, new.score, 0.000001);
        }
    }
    const reader = &packed_index.snapshot().segments[0].reader;
    for (0..2) |ordinal| {
        const expected = (try reference.storedIdAlloc(a, @intCast(ordinal))).?;
        defer a.free(expected);
        const actual = (try reader.storedIdAlloc(a, @intCast(ordinal))).?;
        defer a.free(actual);
        try std.testing.expectEqualStrings(expected, actual);
        try std.testing.expectEqual(try reference.docOrdinal(@intCast(ordinal)), try reader.docOrdinal(@intCast(ordinal)));
    }
    try std.testing.expect(!packed_index.snapshot().segments[0].data.isMapped());
}
