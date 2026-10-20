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

//! Bounded private unit-source publication. A partition contains a fixed
//! ordinal cell, so append/update/delete never rewrites a whole group's unit
//! directory. Source rows and generated replay are committed by the caller in
//! the same owner transaction as stageSnapshot's lease/revision proof.
const std = @import("std");
const contract = @import("group_source_contract.zig");
const state = @import("group_source_state.zig");
const identity = @import("group_source_identity.zig");
const A = std.mem.Allocator;
pub const UnitSource = struct { document: []const u8, payload: []const u8 };
pub const Prepared = struct {
    arena: std.heap.ArenaAllocator,
    units: []const UnitSource,
    pub fn init(a: A, artifact: []const u8, command: contract.Publish, previous: ?state.Snapshot) !Prepared {
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const owned = arena.allocator();
        const root = try command.held.group.anchor(owned);
        const partition = try identity.partition(owned, root, command.held.partition.segment, command.held.partition.cell);
        const output = try owned.alloc(UnitSource, command.units.len);
        var seen: std.AutoHashMapUnmanaged([32]u8, void) = .empty;
        for (command.units, output) |unit, *value| {
            if ((try seen.getOrPut(owned, unit.id)).found_existing) return error.InvalidGroupedSourceCommand;
            var birth: [8]u8 = undefined;
            std.mem.writeInt(u64, &birth, command.held.lease.token, .big);
            const lifetime_id = @import("group_sources.zig").digest(try std.mem.concat(owned, u8, &.{ &unit.id, &birth }));
            var document: []const u8 = try std.fmt.allocPrint(owned, "{s}/u/{x}", .{ partition, &lifetime_id });
            if (previous) |old| {
                for (old.identities, 0..) |logical, i| if (std.mem.eql(u8, &logical, &unit.id)) {
                    document = old.units[i];
                    break;
                };
            }
            const id = document[document.len - 64 ..];
            const fingerprint = try std.fmt.allocPrint(owned, "{x}", .{&unit.fingerprint});
            const payload = try std.json.Stringify.valueAlloc(owned, .{
                ._group_key = try @import("group_sources.zig").scalarMetadata(unit.group),
                ._segment_key = try @import("group_sources.zig").scalarMetadata(unit.segment),
                ._group_identity = unit.group,
                ._segment_identity = unit.segment,
                ._group_root = root,
                ._group_artifact = artifact,
                ._member_keys = unit.member_keys,
                ._ordinal_start = unit.ordinal_start,
                ._ordinal_end = unit.ordinal_end,
                ._group_source_fingerprint = fingerprint,
                ._group_unit = .{
                    .object = "group.unit",
                    .schema_version = @as(u32, 1),
                    .unit_id = id,
                    .text = unit.text,
                    ._group_key = try @import("group_sources.zig").scalarMetadata(unit.group),
                    ._segment_key = try @import("group_sources.zig").scalarMetadata(unit.segment),
                    .member_keys = unit.member_keys,
                    .ordinal_start = unit.ordinal_start,
                    .ordinal_end = unit.ordinal_end,
                    .fingerprint = fingerprint,
                },
            }, .{});
            value.* = .{ .document = document, .payload = payload };
        }
        return .{ .arena = arena, .units = output };
    }
    pub fn deinit(self: *Prepared) void {
        self.arena.deinit();
        self.* = undefined;
    }
};
pub const Staged = struct {
    changed: []const []const u8,
    deleted: []const []const u8,
    sequence: u64,
    /// The enrichment scanner consumes changed source IDs. A retired private
    /// unit must be replayed as an absent source so every producer publishes
    /// its tombstone receipt; the derived delete list alone cannot do that.
    pub fn replayDocuments(self: Staged, a: A) ![]const []const u8 {
        return std.mem.concat(a, []const u8, &.{ self.changed, self.deleted });
    }
    pub fn hasReplay(self: Staged) bool {
        return self.changed.len > 0 or self.deleted.len > 0;
    }
};
fn optional(txn: anytype, selected: []const u8) !?[]const u8 {
    return txn.get(selected) catch |err| if (err == error.NotFound) null else return err;
}

/// `Keys` is the storage key encoder, supplied by the production owner (and
/// isolated transaction fixtures). The returned row identities feed the normal
/// generated-enrichment journal; no provider or index projection runs here.
pub fn stage(comptime Keys: type, a: A, txn: anytype, prepared: *const Prepared, command: contract.Publish, next_sequence: u64) !Staged {
    const pending = try state.verify(a, txn, command.held.group, command.held.partition, command.held.lease, command.held.now_ms);
    const previous = try state.readSnapshot(a, txn, command.held.group, command.held.partition);
    var changed: std.ArrayList([]const u8) = .empty;
    var deleted: std.ArrayList([]const u8) = .empty;
    var desired: std.StringHashMapUnmanaged(void) = .empty;
    var timestamp: [8]u8 = undefined;
    const timestamp_ns = std.math.mul(u64, command.held.now_ms, std.time.ns_per_ms) catch return error.InvalidGroupedSourceCommand;
    std.mem.writeInt(u64, &timestamp, timestamp_ns, .little);
    const documents = try a.alloc([]const u8, prepared.units.len);
    const identities = try a.alloc([32]u8, command.units.len);
    for (command.units, identities) |unit, *logical| logical.* = unit.id;
    for (prepared.units, documents) |unit, *document| {
        document.* = unit.document;
        try desired.put(a, unit.document, {});
        const source_key = try Keys.documentKeyAlloc(a, unit.document);
        // Virtual sources must never overwrite an older user primary row.
        const user_key = try a.dupe(u8, source_key);
        user_key[user_key.len - 1] = Keys.primary_kind;
        if (try optional(txn, user_key) != null) return error.GroupedSourceIdentityConflict;
        const old = try optional(txn, source_key);
        if (old == null or !std.mem.eql(u8, old.?, unit.payload)) {
            try txn.put(source_key, unit.payload);
            try txn.put(try Keys.ttlKeyAlloc(a, unit.document), &timestamp);
            try changed.append(a, try a.dupe(u8, unit.document));
        } else if (!pending.source_changed or try state.unitContextChanged(a, txn, command.held.group, command.held.partition, unit.document)) {
            // Context changes replay prior-aware producers without changing the
            // source revision, preserving independent unit producers' receipts.
            try changed.append(a, try a.dupe(u8, unit.document));
        }
    }
    if (previous) |old| for (old.units) |document| {
        if (desired.contains(document)) continue;
        try txn.delete(try Keys.documentKeyAlloc(a, document));
        try txn.delete(try Keys.ttlKeyAlloc(a, document));
        try state.clearUnitContext(a, txn, command.held.group, command.held.partition, document);
        try deleted.append(a, document);
    };
    var retired: std.ArrayList([]const u8) = .empty;
    var seen_retired: std.StringHashMapUnmanaged(void) = .empty;
    const Txn = @typeInfo(@TypeOf(txn)).pointer.child;
    if (previous) |old| for (old.retired) |document| {
        if (desired.contains(document)) continue;
        if (comptime @hasDecl(Txn, "sourceSettled")) if (try txn.sourceSettled(document)) {
            try txn.delete(try identity.unitMembershipKey(a, document));
            continue;
        };
        if (!(try seen_retired.getOrPut(a, document)).found_existing) try retired.append(a, document);
    };
    for (deleted.items) |document| if (!(try seen_retired.getOrPut(a, document)).found_existing) try retired.append(a, document);
    if (retired.items.len > 256) return error.GroupedSourceRetirementPending;
    const has_replay = changed.items.len > 0 or deleted.items.len > 0;
    const sequence = if (has_replay) next_sequence else if (previous) |old| old.sequence else 0;
    try state.stageSnapshot(a, txn, command.held.group, command.held.partition, command.held.lease, command.held.now_ms, .{ .revision = pending.revision, .input_digest = command.input_digest, .units = documents, .identities = identities, .retired = retired.items, .sequence = sequence, .ordinal = if (command.units.len > 0) command.units[0].ordinal_start else pending.ordinal });
    return .{ .changed = try changed.toOwnedSlice(a), .deleted = try deleted.toOwnedSlice(a), .sequence = sequence };
}

test "group unit publication changes only affected units replays context and retires bounded partitions" {
    try exercisePublication(.source_only);
}

test "group unit publication replays unchanged units when source and context invalidations coalesce" {
    try exercisePublication(.context_first);
    try exercisePublication(.source_first);
}

fn exercisePublication(coalescing: enum { source_only, context_first, source_first }) !void {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Txn = struct {
        const Self = @This();
        a: A,
        map: std.StringHashMapUnmanaged([]const u8) = .empty,
        const Entry = struct { key: []const u8, value: []const u8 };
        const Cursor = struct {
            txn: *Self,
            last: []const u8 = "",
            pub fn close(_: *@This()) void {}
            pub fn seekAtOrAfter(self: *@This(), start: []const u8) !?Entry {
                var selected: ?Entry = null;
                var it = self.txn.map.iterator();
                while (it.next()) |entry| {
                    if (std.mem.order(u8, entry.key_ptr.*, start) == .lt) continue;
                    if (selected == null or std.mem.order(u8, entry.key_ptr.*, selected.?.key) == .lt) selected = .{ .key = entry.key_ptr.*, .value = entry.value_ptr.* };
                }
                if (selected) |row| self.last = row.key;
                return selected;
            }
            pub fn next(self: *@This()) !?Entry {
                return self.seekAtOrAfter(try std.mem.concat(self.txn.a, u8, &.{ self.last, "\x00" }));
            }
        };
        pub fn openPhysicalCursorAdapter(self: *@This()) !Cursor {
            return .{ .txn = self };
        }
        pub fn get(self: *@This(), selected: []const u8) ![]const u8 {
            return self.map.get(selected) orelse error.NotFound;
        }
        pub fn put(self: *@This(), selected: []const u8, value: []const u8) !void {
            try self.map.put(self.a, try self.a.dupe(u8, selected), try self.a.dupe(u8, value));
        }
        pub fn delete(self: *@This(), selected: []const u8) !void {
            _ = self.map.remove(selected);
        }
    };
    const Keys = struct {
        pub const primary_kind: u8 = 0x10;
        pub fn documentKeyAlloc(alloc: A, document: []const u8) ![]u8 {
            return std.mem.concat(alloc, u8, &.{ document, &.{identity.primary_kind} });
        }
        pub fn ttlKeyAlloc(alloc: A, document: []const u8) ![]u8 {
            return std.mem.concat(alloc, u8, &.{ document, &.{0x11} });
        }
    };
    const groups = @import("group_sources.zig");
    const definition = @import("group_source_index.zig").Definition{ .table_incarnation = 7, .configuration = groups.digest("turns") };
    const group = state.Group{ .definition = definition, .key = "thread" };
    const partition = @import("group_source_index.zig").Partition{ .group = "thread", .segment = "turn", .cell = 0 };
    const source = state.Owner{ .namespace = @splat(1), .incarnation = 8 };
    var event = @import("group_source_index.zig").Invalidation{ .definition = definition, .partition = partition, .document = "one", .source_revision = 1, .ordinal = 1 };
    var txn = Txn{ .a = a };
    _ = try state.invalidate(a, &txn, source, event);
    const lease = try state.claim(a, &txn, group, (try state.nextPending(a, &txn, group)).?, groups.digest("worker"), 100, 1000);
    const cfg = groups.Config{ .group_by = "thread", .order_by = "ordinal", .segment_by = "turn", .field = "text", .max_bytes = 4 };
    var members = [_]groups.Member{ .{ .document = "one", .version = 1, .ordinal = 1, .segment = "turn", .text = "one" }, .{ .document = "two", .version = 1, .ordinal = 2, .segment = "turn", .text = "two" } };
    var command = contract.Publish{ .held = .{ .group = group, .partition = partition, .lease = lease, .now_ms = 101 }, .topology_epoch = 1, .sources = &.{.{ .owner = source, .revision = 1 }}, .units = try groups.units(a, cfg, group.key, &members), .input_digest = groups.digest("initial") };
    var prepared = try Prepared.init(a, "turns", command, try state.readSnapshot(a, &txn, group, partition));
    defer prepared.deinit();
    const first = try stage(Keys, a, &txn, &prepared, command, 10);
    try std.testing.expectEqual(@as(usize, 2), first.changed.len);
    const unchanged_key = try Keys.documentKeyAlloc(a, prepared.units[0].document);
    const unchanged_payload = try txn.get(unchanged_key);
    try state.finish(a, &txn, group, partition, lease, 102, groups.digest("published-first"));
    event.source_revision = 2;
    if (coalescing == .context_first) try state.invalidateContext(a, &txn, group, &.{.{ .partition = partition, .ordinal = 1 }}, 1);
    _ = try state.invalidate(a, &txn, source, event);
    if (coalescing == .source_first) try state.invalidateContext(a, &txn, group, &.{.{ .partition = partition, .ordinal = 1 }}, 1);
    command.held.lease = try state.claim(a, &txn, group, (try state.nextPending(a, &txn, group)).?, groups.digest("worker"), 200, 1000);
    command.held.now_ms = 201;
    members[1].text = "two!";
    command.units = try groups.units(a, cfg, group.key, &members);
    command.input_digest = groups.digest("changed");
    var updated = try Prepared.init(a, "turns", command, try state.readSnapshot(a, &txn, group, partition));
    defer updated.deinit();
    const second = try stage(Keys, a, &txn, &updated, command, 11);
    // Context debt survives either arrival order. The unchanged unit must
    // replay without rewriting its source payload or source revision.
    try std.testing.expectEqual(@as(usize, if (coalescing == .source_only) 1 else 2), second.changed.len);
    if (coalescing != .source_only) try std.testing.expect(!try state.unitContextChanged(a, &txn, group, partition, prepared.units[0].document));
    try std.testing.expectEqualStrings(unchanged_payload, try txn.get(unchanged_key));
    try state.finish(a, &txn, group, partition, command.held.lease, 202, groups.digest("published-second"));
    try state.invalidateContext(a, &txn, group, &.{.{ .partition = partition, .ordinal = 1 }}, 1);
    command.held.lease = try state.claim(a, &txn, group, (try state.nextPending(a, &txn, group)).?, groups.digest("worker"), 300, 1000);
    command.held.now_ms = 301;
    const context_only = try stage(Keys, a, &txn, &updated, command, 12);
    try std.testing.expectEqual(@as(usize, 2), context_only.changed.len);
    try std.testing.expectEqualStrings(unchanged_payload, try txn.get(unchanged_key));
    try state.finish(a, &txn, group, partition, command.held.lease, 302, groups.digest("published-context"));
    event.source_revision = 3;
    _ = try state.invalidate(a, &txn, source, event);
    command.held.lease = try state.claim(a, &txn, group, (try state.nextPending(a, &txn, group)).?, groups.digest("worker"), 400, 1000);
    command.held.now_ms = 401;
    command.units = &.{};
    command.input_digest = groups.digest("empty");
    var empty = try Prepared.init(a, "turns", command, try state.readSnapshot(a, &txn, group, partition));
    defer empty.deinit();
    const retired = try stage(Keys, a, &txn, &empty, command, 13);
    try std.testing.expectEqual(@as(usize, 2), retired.deleted.len);
    try std.testing.expectError(error.NotFound, txn.get(unchanged_key));
    try std.testing.expectError(error.NotFound, txn.get(try identity.unitHorizonEpochKey(a, prepared.units[0].document, 1)));
    try std.testing.expectEqual(@as(usize, 0), (try state.readSnapshot(a, &txn, group, partition)).?.units.len);
    try state.finish(a, &txn, group, partition, command.held.lease, 402, groups.digest("published-empty"));
    event.source_revision = 4;
    _ = try state.invalidate(a, &txn, source, event);
    command.held.lease = try state.claim(a, &txn, group, (try state.nextPending(a, &txn, group)).?, groups.digest("worker"), 500, 1000);
    command.held.now_ms = 501;
    command.units = try groups.units(a, cfg, group.key, &members);
    command.input_digest = groups.digest("reborn");
    var reborn = try Prepared.init(a, "turns", command, try state.readSnapshot(a, &txn, group, partition));
    defer reborn.deinit();
    _ = try stage(Keys, a, &txn, &reborn, command, 14);
    try std.testing.expect(!std.mem.eql(u8, prepared.units[0].document, reborn.units[0].document));
    try std.testing.expectError(error.NotFound, txn.get(unchanged_key));
    const epoch_key = try identity.unitContextEpochKey(a, reborn.units[0].document);
    const epoch_before = try txn.get(epoch_key);
    try std.testing.expect(try state.unitIsCurrent(a, &txn, reborn.units[0].document));
    try state.finish(a, &txn, group, partition, command.held.lease, 502, groups.digest("published-reborn"));
    event.source_revision = 5;
    event.repair = true;
    try @import("group_source_propagation.zig").advance(a, &txn, source, event, 1);
    command.held.lease = try state.claim(a, &txn, group, (try state.nextPending(a, &txn, group)).?, groups.digest("worker"), 600, 1000);
    command.held.now_ms = 601;
    var repair = try Prepared.init(a, "turns", command, try state.readSnapshot(a, &txn, group, partition));
    defer repair.deinit();
    const repaired = try stage(Keys, a, &txn, &repair, command, 15);
    try std.testing.expect(!repaired.hasReplay());
    try std.testing.expectEqualStrings(reborn.units[0].document, repair.units[0].document);
    try std.testing.expect(epoch_before.ptr == (try txn.get(epoch_key)).ptr);
}
