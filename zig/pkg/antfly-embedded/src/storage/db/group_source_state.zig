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

//! Group-owner transactional state. Source owners deliver invalidations, not
//! text afterimages. Their local clocks are deduplicated independently; an old
//! owner can at worst cause redundant work, never restore deleted membership.
//! Provider work runs outside these transactions, fenced by a group lease and
//! the selected partition's revision. Other partitions may continue to append.
const std = @import("std");
const index = @import("group_source_index.zig");
const groups = @import("group_sources.zig");
const A = std.mem.Allocator;
const Digest = groups.Digest;
const identity = @import("group_source_identity.zig");
pub const metadata_kind = identity.metadata_kind;
pub const Owner = struct { namespace: [24]u8, incarnation: u128 };
pub const Group = struct {
    definition: index.Definition,
    key: []const u8,
    pub fn anchor(self: Group, a: A) ![]u8 {
        try self.definition.validate();
        if (self.key.len == 0 or self.key.len > 4096) return error.InvalidGroupedSourceMutation;
        return identity.anchor(a, self.definition.table_incarnation, self.definition.configuration, groups.digest(self.key));
    }
};
pub fn isAnchor(document: []const u8) bool {
    return identity.isAnchor(document);
}
fn key(a: A, group: Group, kind: u8, suffix: []const u8) ![]u8 {
    const anchor = try group.anchor(a);
    defer a.free(anchor);
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(a);
    try out.append(a, 1);
    for (anchor) |byte| {
        try out.append(a, byte);
        if (byte == 0) try out.append(a, 255);
    }
    try out.appendSlice(a, &.{ 0, 0, metadata_kind, kind });
    try out.appendSlice(a, suffix);
    return out.toOwnedSlice(a);
}
fn hash(value: anytype, a: A) !Digest {
    const raw = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(raw);
    return groups.digest(raw);
}
fn encode(a: A, selected: []const u8, value: anytype) ![]u8 {
    const payload = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(payload);
    if (payload.len > 262144) return error.GroupedSourceCorrupt;
    var checksum = std.crypto.hash.sha2.Sha256.init(.{});
    checksum.update(selected);
    checksum.update(payload);
    return std.mem.concat(a, u8, &.{ "GSC1", payload, &checksum.finalResult() });
}
fn optional(txn: anytype, selected: []const u8) !?[]const u8 {
    return txn.get(selected) catch |err| if (err == error.NotFound) null else return err;
}
fn load(comptime T: type, a: A, txn: anytype, selected: []const u8) !?T {
    const raw = (try optional(txn, selected)) orelse return null;
    if (raw.len < 36 or raw.len > 262180 or !std.mem.eql(u8, raw[0..4], "GSC1")) return error.GroupedSourceCorrupt;
    var checksum = std.crypto.hash.sha2.Sha256.init(.{});
    checksum.update(selected);
    checksum.update(raw[4 .. raw.len - 32]);
    if (!std.mem.eql(u8, raw[raw.len - 32 ..], &checksum.finalResult())) return error.GroupedSourceCorrupt;
    return std.json.parseFromSliceLeaky(T, a, raw[4 .. raw.len - 32], .{ .allocate = .alloc_always }) catch |err| if (err == error.OutOfMemory) err else error.GroupedSourceCorrupt;
}
fn store(a: A, txn: anytype, selected: []const u8, value: anytype) !void {
    const raw = try encode(a, selected, value);
    defer a.free(raw);
    try txn.put(selected, raw);
}
pub const Head = struct { revision: u64 = 0, pending: u64 = 0, lease_counter: u64 = 0 };
pub const Pending = struct {
    partition: index.Partition,
    revision: u64,
    /// Lower bound, possibly including a since-deleted member. This only
    /// schedules work early; unit ordering uses the refreshed index snapshot.
    ordinal: i64,
    source_changed: bool,
    /// Earliest uncompleted cause, retained when newer work coalesces.
    required_since: u64 = 0,
};
const Receipt = struct { source_revision: u64, digest: Digest, scheduled_revision: u64 };
fn partitionIdentity(a: A, partition: index.Partition) !Digest {
    return hash(partition, a);
}
pub fn pendingKey(a: A, group: Group, partition: index.Partition) ![]u8 {
    return key(a, group, 2, &(try partitionIdentity(a, partition)));
}
pub fn queuePrefix(a: A, group: Group) ![]u8 {
    return key(a, group, 3, "");
}
fn queueKey(a: A, group: Group, pending: Pending) ![]u8 {
    var order: [8]u8 = undefined;
    std.mem.writeInt(u64, &order, @as(u64, @bitCast(pending.ordinal)) ^ (@as(u64, 1) << 63), .big);
    var segment: std.ArrayList(u8) = .empty;
    for (pending.partition.segment) |byte| {
        try segment.append(a, byte);
        if (byte == 0) try segment.append(a, 255);
    }
    try segment.appendSlice(a, &.{ 0, 0 });
    const suffix = try std.mem.concat(a, u8, &.{ &order, segment.items, &(try partitionIdentity(a, pending.partition)) });
    defer a.free(suffix);
    return key(a, group, 3, suffix);
}
fn causalQueueKey(a: A, group: Group, pending: Pending) ![]u8 {
    var order: [8]u8 = undefined;
    std.mem.writeInt(u64, &order, pending.required_since, .big);
    return key(a, group, 9, try std.mem.concat(a, u8, &.{ &order, &(try partitionIdentity(a, pending.partition)) }));
}
fn enqueue(a: A, txn: anytype, group: Group, head: *Head, partition: index.Partition, ordinal: i64, source_changed: bool) !u64 {
    const selected = try pendingKey(a, group, partition);
    const previous = try load(Pending, a, txn, selected);
    const revision = try std.math.add(u64, head.revision, 1);
    head.revision = revision;
    var pending = Pending{ .partition = partition, .revision = revision, .ordinal = ordinal, .source_changed = source_changed, .required_since = revision };
    if (previous) |old| {
        try txn.delete(try queueKey(a, group, old));
        try txn.delete(try causalQueueKey(a, group, old));
        pending.required_since = if (old.required_since == 0) old.revision else old.required_since;
        pending.ordinal = @min(old.ordinal, ordinal);
        pending.source_changed = old.source_changed or source_changed;
    } else head.pending = try std.math.add(u64, head.pending, 1);
    try store(a, txn, selected, pending);
    const queue = try queueKey(a, group, pending);
    try store(a, txn, queue, pending);
    try store(a, txn, try causalQueueKey(a, group, pending), pending);
    return revision;
}

/// The private owner port authenticates source ownership and the immutable
/// table/configuration before calling this function. It commits this receipt
/// before acknowledging delivery to the source's durable outbox.
pub fn invalidate(a: A, txn: anytype, owner: Owner, event: index.Invalidation) !u64 {
    if (owner.incarnation == 0 or std.mem.allEqual(u8, &owner.namespace, 0) or event.source_revision == 0) return error.InvalidGroupedSourceMutation;
    const group = Group{ .definition = event.definition, .key = event.partition.group };
    const receipt_key = try key(a, group, 4, &(try hash(.{ owner, event.document, event.partition }, a)));
    const fingerprint = try index.eventDigest(a, event);
    var previous_enrollment: u64 = 0;
    if (try load(Receipt, a, txn, receipt_key)) |old| {
        if (old.source_revision > event.source_revision) return old.scheduled_revision;
        if (old.source_revision == event.source_revision) {
            if (!std.mem.eql(u8, &old.digest, &fingerprint)) return error.GroupedSourceCorrupt;
            if (try readSnapshot(a, txn, group, event.partition) != null or try pendingFor(a, txn, group, event.partition) != null) return old.scheduled_revision;
            previous_enrollment = old.scheduled_revision;
        }
    }
    const head_key = try key(a, group, 1, "");
    var head = (try load(Head, a, txn, head_key)) orelse Head{};
    head.revision = @max(head.revision, previous_enrollment);
    var ordinal = event.ordinal;
    if (try readSnapshot(a, txn, group, event.partition)) |snapshot| if (snapshot.units.len != 0) {
        ordinal = @min(ordinal, snapshot.ordinal);
    };
    const revision = try enqueue(a, txn, group, &head, event.partition, ordinal, true);
    try store(a, txn, receipt_key, Receipt{ .source_revision = event.source_revision, .digest = fingerprint, .scheduled_revision = revision });
    try store(a, txn, head_key, head);
    try @import("group_source_queue.zig").stageGroup(a, txn, try group.anchor(a), group);
    return revision;
}

/// Call only for the bounded later partitions selected by an ORIGINAL source
/// invalidation. Completing/recomputing an output never calls this method, so
/// context invalidation cannot transitively spread through a whole group.
pub fn invalidateContext(a: A, txn: anytype, group: Group, later: []const struct { partition: index.Partition, ordinal: i64 }, horizon: u16) !void {
    if (horizon == 0 or horizon > 128 or later.len > horizon) return error.InvalidGroupedPriorContext;
    for (later) |value| _ = try invalidateContextHorizons(a, txn, group, value.partition, value.ordinal, &.{horizon});
}
const HorizonEpoch = struct { horizon: u16, revision: u64 };
const ContextEpoch = struct { revision: u64, horizons: []const HorizonEpoch = &.{} };
/// Persist only the configured policies affected by this original event. The
/// legacy epoch invalidates pre-upgrade receipts once; new receipts observe
/// their own horizon key and remain reusable outside that horizon.
pub fn invalidateContextHorizons(a: A, txn: anytype, group: Group, partition: index.Partition, ordinal: i64, horizons: []const u16) !u64 {
    if (!std.mem.eql(u8, group.key, partition.group) or horizons.len > 128) return error.InvalidGroupedPriorContext;
    const head_key = try key(a, group, 1, "");
    var head = (try load(Head, a, txn, head_key)) orelse Head{};
    const pending_revision = try enqueue(a, txn, group, &head, partition, ordinal, false);
    const clock_key = try leaseClockKey(a, group.definition.table_incarnation);
    const revision = try std.math.add(u64, (try load(u64, a, txn, clock_key)) orelse 0, 1);
    try store(a, txn, clock_key, revision);
    const selected = try contextEpochKey(a, group, partition);
    const old = try load(ContextEpoch, a, txn, selected);
    var policies: std.ArrayList(HorizonEpoch) = .empty;
    if (old) |value| try policies.appendSlice(a, value.horizons);
    for (horizons) |horizon| {
        if (horizon == 0 or horizon > 128) return error.InvalidGroupedPriorContext;
        var found = false;
        for (policies.items) |*policy| if (policy.horizon == horizon) {
            policy.revision = revision;
            found = true;
            break;
        };
        if (!found) try policies.append(a, .{ .horizon = horizon, .revision = revision });
    }
    try store(a, txn, selected, ContextEpoch{ .revision = revision, .horizons = policies.items });
    // Publish unit guards together with the replacement snapshot, in group
    // order. Eagerly waking a later unit here can put its producer before the
    // predecessor's replacement on the ordinary replay stream, deadlocking
    // replay on the very predecessor it still needs to materialize.
    // The durable pending queue already fences prior-context reads meanwhile.
    try store(a, txn, head_key, head);
    if (head.pending > 0) try @import("group_source_queue.zig").stageGroup(a, txn, try group.anchor(a), group);
    return pending_revision;
}
/// Retiring a unit removes both the legacy guard and every persisted policy.
pub fn clearUnitContext(a: A, txn: anytype, group: Group, partition: index.Partition, unit: []const u8) !void {
    try txn.delete(try identity.unitContextEpochKey(a, unit));
    if (try load(ContextEpoch, a, txn, try contextEpochKey(a, group, partition))) |epoch| {
        for (epoch.horizons) |policy| try txn.delete(try identity.unitHorizonEpochKey(a, unit, policy.horizon));
    }
}

pub const lease_clock_prefix = "\x00\x00G\x05";
fn leaseClockKey(a: A, table: u128) ![]u8 {
    var raw: [16]u8 = undefined;
    std.mem.writeInt(u128, &raw, table, .big);
    return std.mem.concat(a, u8, &.{ lease_clock_prefix, &raw });
}
pub fn ensureLeaseFloor(a: A, txn: anytype, table: u128, floor: u64) !void {
    const selected = try leaseClockKey(a, table);
    const previous = (try load(u64, a, txn, selected)) orelse 0;
    if (previous < floor) try store(a, txn, selected, floor);
}
fn ownerIncarnation(txn: anytype) u128 {
    const T = @typeInfo(@TypeOf(txn)).pointer.child;
    return if (comptime @hasDecl(T, "groupIncarnation")) txn.groupIncarnation() else 1;
}
pub const Lease = struct {
    incarnation: u128 = 1,
    owner: Digest,
    token: u64,
    revision: u64,
    partition: Digest,
    expires_ms: u64,
};

pub fn claim(a: A, txn: anytype, group: Group, pending: Pending, owner: Digest, now_ms: u64, ttl_ms: u32) !Lease {
    if (ttl_ms == 0 or ttl_ms > 300000 or std.mem.allEqual(u8, &owner, 0)) return error.InvalidGroupedSourceMutation;
    const head_key = try key(a, group, 1, "");
    var head = (try load(Head, a, txn, head_key)) orelse return error.EnrichmentSourceChanged;
    const current = (try load(Pending, a, txn, try pendingKey(a, group, pending.partition))) orelse return error.EnrichmentSourceChanged;
    if (pending.revision != current.revision) return error.EnrichmentSourceChanged;
    const lease_key = try key(a, group, 5, "");
    if (try load(Lease, a, txn, lease_key)) |old| if (old.incarnation == ownerIncarnation(txn) and old.expires_ms > now_ms and old.revision == pending.revision) return error.GroupedSourceBusy;
    // The worker obtains the first queue record using an ordered snapshot;
    // recheck it here so concurrently inserted earlier work cannot be skipped.
    var cursor = try txn.openPhysicalCursorAdapter();
    defer cursor.close();
    const prefix = try queuePrefix(a, group);
    const first = (try cursor.seekAtOrAfter(prefix)) orelse return error.EnrichmentSourceChanged;
    if (!std.mem.eql(u8, first.key, try queueKey(a, group, current))) return error.GroupedSourceEarlierWorkPending;
    const clock_key = try leaseClockKey(a, group.definition.table_incarnation);
    head.lease_counter = try std.math.add(u64, @max(head.lease_counter, (try load(u64, a, txn, clock_key)) orelse 0), 1);
    try store(a, txn, clock_key, head.lease_counter);
    const lease = Lease{ .incarnation = ownerIncarnation(txn), .owner = owner, .token = head.lease_counter, .revision = pending.revision, .partition = try partitionIdentity(a, pending.partition), .expires_ms = try std.math.add(u64, now_ms, ttl_ms) };
    try store(a, txn, lease_key, lease);
    try store(a, txn, head_key, head);
    return lease;
}

pub fn verify(a: A, txn: anytype, group: Group, partition: index.Partition, expected: Lease, now_ms: u64) !Pending {
    const lease = (try load(Lease, a, txn, try key(a, group, 5, ""))) orelse return error.EnrichmentSourceChanged;
    if (expected.incarnation != ownerIncarnation(txn) or expected.incarnation != lease.incarnation or expected.token != lease.token or expected.revision != lease.revision or !std.mem.eql(u8, &expected.owner, &lease.owner) or !std.mem.eql(u8, &expected.partition, &lease.partition) or now_ms >= lease.expires_ms or !std.mem.eql(u8, &expected.partition, &(try partitionIdentity(a, partition)))) return error.EnrichmentSourceChanged;
    const pending = (try load(Pending, a, txn, try pendingKey(a, group, partition))) orelse return error.EnrichmentSourceChanged;
    if (pending.revision != expected.revision) return error.EnrichmentSourceChanged;
    var cursor = try txn.openPhysicalCursorAdapter();
    defer cursor.close();
    const first = (try cursor.seekAtOrAfter(try queuePrefix(a, group))) orelse return error.EnrichmentSourceChanged;
    if (!std.mem.eql(u8, first.key, try queueKey(a, group, pending))) return error.GroupedSourceEarlierWorkPending;
    return pending;
}

/// Call in the SAME writer transaction as ordinary unit publication. A failed
/// publication or stale lease rolls everything back, retaining the dirty work.
pub fn finish(a: A, txn: anytype, group: Group, partition: index.Partition, expected: Lease, now_ms: u64, publication: Digest) !void {
    if (std.mem.allEqual(u8, &publication, 0)) return error.InvalidGroupedSourceMutation;
    const pending = try verify(a, txn, group, partition, expected, now_ms);
    const head_key = try key(a, group, 1, "");
    var head = (try load(Head, a, txn, head_key)) orelse return error.GroupedSourceCorrupt;
    head.pending = std.math.sub(u64, head.pending, 1) catch return error.GroupedSourceCorrupt;
    if (try readSnapshot(a, txn, group, partition)) |value| {
        var completed = value;
        completed.retired = &.{};
        try store(a, txn, try snapshotKey(a, group, partition), completed);
        if (completed.units.len > 0) try store(a, txn, try directoryKey(a, group, partition, completed.ordinal), DirectoryEntry{ .partition = partition, .snapshot = completed });
    }
    try txn.delete(try pendingKey(a, group, partition));
    try txn.delete(try queueKey(a, group, pending));
    try txn.delete(try causalQueueKey(a, group, pending));
    try txn.delete(try key(a, group, 5, ""));
    try store(a, txn, try key(a, group, 6, &(try partitionIdentity(a, partition))), .{ .revision = pending.revision, .publication = publication });
    try store(a, txn, head_key, head);
    if (head.pending == 0) try @import("group_source_queue.zig").removeGroup(a, txn, try group.anchor(a));
}
pub fn settled(a: A, txn: anytype, group: Group, partition: index.Partition, revision: u64) !bool {
    const Published = struct { revision: u64, publication: Digest };
    const value = (try load(Published, a, txn, try key(a, group, 6, &(try partitionIdentity(a, partition))))) orelse return false;
    if (std.mem.allEqual(u8, &value.publication, 0)) return error.GroupedSourceCorrupt;
    return value.revision >= revision;
}

pub fn heartbeat(a: A, txn: anytype, group: Group, partition: index.Partition, expected: Lease, now_ms: u64, ttl_ms: u32) !void {
    if (ttl_ms == 0 or ttl_ms > 300000) return error.InvalidGroupedSourceMutation;
    _ = try verify(a, txn, group, partition, expected, now_ms);
    var renewed = expected;
    renewed.expires_ms = try std.math.add(u64, now_ms, ttl_ms);
    try store(a, txn, try key(a, group, 5, ""), renewed);
}

test "group owner orders work fences stale publication and preserves independent future appends" {
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
            pub fn close(_: *@This()) void {}
            pub fn seekAtOrAfter(self: *@This(), start: []const u8) !?Entry {
                var selected: ?Entry = null;
                var it = self.txn.map.iterator();
                while (it.next()) |entry| {
                    if (std.mem.order(u8, entry.key_ptr.*, start) == .lt) continue;
                    if (selected == null or std.mem.order(u8, entry.key_ptr.*, selected.?.key) == .lt) selected = .{ .key = entry.key_ptr.*, .value = entry.value_ptr.* };
                }
                return selected;
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
    var txn = Txn{ .a = a };
    const definition = index.Definition{ .table_incarnation = 7, .configuration = groups.digest("config") };
    const group = Group{ .definition = definition, .key = "thread" };
    const source = Owner{ .namespace = @splat(1), .incarnation = 3 };
    var first = index.Invalidation{ .definition = definition, .partition = .{ .group = "thread", .segment = "a", .cell = 0 }, .document = "one", .source_revision = 1, .ordinal = 1 };
    var second = first;
    second.partition.segment = "b";
    second.document = "two";
    second.ordinal = 2;
    const r1 = try invalidate(a, &txn, source, first);
    try std.testing.expectEqual(r1, try invalidate(a, &txn, source, first));
    const r2 = try invalidate(a, &txn, source, second);
    try std.testing.expect(r2 > r1);
    const p1 = (try load(Pending, a, &txn, try pendingKey(a, group, first.partition))).?;
    const p2 = (try load(Pending, a, &txn, try pendingKey(a, group, second.partition))).?;
    const worker = groups.digest("worker");
    try std.testing.expectError(error.GroupedSourceEarlierWorkPending, claim(a, &txn, group, p2, worker, 10, 100));
    const lease = try claim(a, &txn, group, p1, worker, 10, 100);
    second.source_revision = 2;
    _ = try invalidate(a, &txn, source, second);
    // Future independent work does not starve a currently running segment.
    try heartbeat(a, &txn, group, first.partition, lease, 90, 100);
    try finish(a, &txn, group, first.partition, lease, 120, groups.digest("publication-one"));
    try std.testing.expect(try settled(a, &txn, group, first.partition, r1));
    const p2new = (try load(Pending, a, &txn, try pendingKey(a, group, second.partition))).?;
    const lease2 = try claim(a, &txn, group, p2new, worker, 121, 100);
    first.source_revision = 2;
    _ = try invalidate(a, &txn, source, first);
    try std.testing.expectError(error.GroupedSourceEarlierWorkPending, finish(a, &txn, group, second.partition, lease2, 122, groups.digest("stale")));
    try std.testing.expectError(error.EnrichmentSourceChanged, verify(a, &txn, group, second.partition, lease2, 222));
    const p1new = (try load(Pending, a, &txn, try pendingKey(a, group, first.partition))).?;
    const renewed = try claim(a, &txn, group, p1new, worker, 222, 100);
    try std.testing.expect(renewed.token > lease2.token);
    first.source_revision = 3;
    _ = try invalidate(a, &txn, source, first);
    try std.testing.expectError(error.EnrichmentSourceChanged, finish(a, &txn, group, first.partition, renewed, 223, groups.digest("stale-again")));
    // Restart uses durable records alone; old clocks are independently deduped.
    first.source_revision = 1;
    _ = try invalidate(a, &txn, source, first);
    const still_pending = (try load(Pending, a, &txn, try pendingKey(a, group, first.partition))).?;
    try std.testing.expect(still_pending.revision > renewed.revision);
}

pub const Snapshot = struct {
    revision: u64,
    input_digest: Digest,
    units: []const []const u8,
    identities: []const Digest = &.{},
    retired: []const []const u8 = &.{},
    sequence: u64,
    ordinal: i64 = 0,
};
pub fn snapshotKey(a: A, group: Group, partition: index.Partition) ![]u8 {
    return key(a, group, 7, &(try partitionIdentity(a, partition)));
}
pub fn readSnapshot(a: A, txn: anytype, group: Group, partition: index.Partition) !?Snapshot {
    const snapshot = (try load(Snapshot, a, txn, try snapshotKey(a, group, partition))) orelse return null;
    if (snapshot.revision == 0 or (snapshot.identities.len != 0 and snapshot.identities.len != snapshot.units.len) or snapshot.units.len > 128 or snapshot.retired.len > 256 or (snapshot.units.len > 0 and snapshot.sequence == 0) or std.mem.allEqual(u8, &snapshot.input_digest, 0)) return error.GroupedSourceCorrupt;
    const partition_anchor = try identity.partition(a, try group.anchor(a), partition.segment, partition.cell);
    const prefix = try std.mem.concat(a, u8, &.{ partition_anchor, "/u/" });
    const all_units = try std.mem.concat(a, []const u8, &.{ snapshot.units, snapshot.retired });
    for (all_units) |unit| {
        if (unit.len != prefix.len + 64 or !std.mem.startsWith(u8, unit, prefix)) return error.GroupedSourceCorrupt;
        for (unit[prefix.len..]) |byte| if (!std.ascii.isHex(byte)) return error.GroupedSourceCorrupt;
    }
    return snapshot;
}
pub fn stageSnapshot(a: A, txn: anytype, group: Group, partition: index.Partition, lease: Lease, now_ms: u64, snapshot: Snapshot) !void {
    var pending = try verify(a, txn, group, partition, lease, now_ms);
    if (snapshot.revision != lease.revision or (snapshot.identities.len != 0 and snapshot.identities.len != snapshot.units.len) or snapshot.units.len > 128 or snapshot.retired.len > 256 or (snapshot.units.len > 0 and snapshot.sequence == 0) or std.mem.allEqual(u8, &snapshot.input_digest, 0)) return error.InvalidGroupedSourceCommand;
    if (try readSnapshot(a, txn, group, partition)) |old| try txn.delete(try directoryKey(a, group, partition, old.ordinal));
    try store(a, txn, try snapshotKey(a, group, partition), snapshot);
    if (snapshot.units.len != 0 and pending.ordinal != snapshot.ordinal) {
        try txn.delete(try queueKey(a, group, pending));
        pending.ordinal = snapshot.ordinal;
        try store(a, txn, try pendingKey(a, group, partition), pending);
        try store(a, txn, try queueKey(a, group, pending), pending);
        try store(a, txn, try causalQueueKey(a, group, pending), pending);
    }
    const epoch_key = try contextEpochKey(a, group, partition);
    const context_epoch = try load(ContextEpoch, a, txn, epoch_key);
    for (snapshot.units) |unit| {
        const unit_key = try identity.unitContextEpochKey(a, unit);
        const desired = if (context_epoch) |epoch| try std.json.Stringify.valueAlloc(a, .{ .revision = epoch.revision }, .{}) else null;
        const existing = txn.get(unit_key) catch |err| if (err == error.NotFound) null else return err;
        if (desired) |raw| {
            if (existing == null or !std.mem.eql(u8, existing.?, raw)) try txn.put(unit_key, raw);
        } else if (existing != null) try txn.delete(unit_key);
        if (context_epoch) |epoch| for (epoch.horizons) |policy| {
            const policy_key = try identity.unitHorizonEpochKey(a, unit, policy.horizon);
            const policy_value = try std.json.Stringify.valueAlloc(a, .{ .revision = policy.revision }, .{});
            const previous = txn.get(policy_key) catch |err| if (err == error.NotFound) null else return err;
            if (previous == null or !std.mem.eql(u8, previous.?, policy_value)) try txn.put(policy_key, policy_value);
        };
        try store(a, txn, try identity.unitMembershipKey(a, unit), UnitMembership{ .snapshot_key_base64 = try @import("group_source_reads.zig").encode(a, try snapshotKey(a, group, partition)) });
    }
    if (snapshot.units.len > 0) try store(a, txn, try directoryKey(a, group, partition, snapshot.ordinal), DirectoryEntry{ .partition = partition, .snapshot = snapshot });
}

pub fn pendingFor(a: A, txn: anytype, group: Group, partition: index.Partition) !?Pending {
    return load(Pending, a, txn, try pendingKey(a, group, partition));
}
pub fn hasPending(a: A, txn: anytype, group: Group) !bool {
    const head = (try load(Head, a, txn, try key(a, group, 1, ""))) orelse return false;
    return head.pending != 0;
}
pub fn nextPending(a: A, txn: anytype, group: Group) !?Pending {
    var cursor = try txn.openPhysicalCursorAdapter();
    defer cursor.close();
    const prefix = try queuePrefix(a, group);
    const first = (try cursor.seekAtOrAfter(prefix)) orelse return null;
    if (!std.mem.startsWith(u8, first.key, prefix)) return null;
    const value = (try load(Pending, a, txn, first.key)) orelse return error.GroupedSourceCorrupt;
    if (!std.mem.eql(u8, first.key, try queueKey(a, group, value))) return error.GroupedSourceCorrupt;
    return value;
}

pub fn currentLease(a: A, txn: anytype, group: Group) !?Lease {
    return load(Lease, a, txn, try key(a, group, 5, ""));
}

pub const DirectoryEntry = struct { partition: index.Partition, snapshot: Snapshot };
pub fn directoryPrefix(a: A, group: Group) ![]u8 {
    return key(a, group, 8, "");
}
pub fn directoryOrdinalBound(a: A, group: Group, ordinal: i64, upper: bool) ![]u8 {
    var order: [8]u8 = undefined;
    std.mem.writeInt(u64, &order, @as(u64, @bitCast(ordinal)) ^ (@as(u64, 1) << 63), .big);
    const next = try key(a, group, 8, &order);
    if (!upper) return next;
    // Segment scalar identities are valid UTF-8 and cannot start with 0xff.
    return std.mem.concat(a, u8, &.{ next, &.{255} });
}
pub fn directoryKey(a: A, group: Group, partition: index.Partition, ordinal: i64) ![]u8 {
    var order: [8]u8 = undefined;
    std.mem.writeInt(u64, &order, @as(u64, @bitCast(ordinal)) ^ (@as(u64, 1) << 63), .big);
    var segment: std.ArrayList(u8) = .empty;
    for (partition.segment) |byte| {
        try segment.append(a, byte);
        if (byte == 0) try segment.append(a, 255);
    }
    try segment.appendSlice(a, &.{ 0, 0 });
    return key(a, group, 8, try std.mem.concat(a, u8, &.{ &order, segment.items, &(try partitionIdentity(a, partition)) }));
}
pub fn directoryEntry(a: A, txn: anytype, group: Group, selected: []const u8) !DirectoryEntry {
    const value = (try load(DirectoryEntry, a, txn, selected)) orelse return error.GroupedSourceCorrupt;
    if (!std.mem.eql(u8, value.partition.group, group.key) or !std.mem.eql(u8, selected, try directoryKey(a, group, value.partition, value.snapshot.ordinal))) return error.GroupedSourceCorrupt;
    return value;
}
/// Finite causes settle while unrelated later appends continue arriving.
pub fn frontierSettled(a: A, txn: anytype, group: Group, frontier: u64) !bool {
    var cursor = try txn.openPhysicalCursorAdapter();
    defer cursor.close();
    const prefix = try key(a, group, 9, "");
    const first = (try cursor.seekAtOrAfter(prefix)) orelse return true;
    if (!std.mem.startsWith(u8, first.key, prefix)) return true;
    const pending = (try load(Pending, a, txn, first.key)) orelse return error.GroupedSourceCorrupt;
    if (!std.mem.eql(u8, first.key, try causalQueueKey(a, group, pending))) return error.GroupedSourceCorrupt;
    return pending.required_since > frontier;
}
pub fn contextEpochKey(a: A, group: Group, partition: index.Partition) ![]u8 {
    return key(a, group, 11, &(try partitionIdentity(a, partition)));
}

pub const metadataKey = key;
pub const readRecord = load;
pub const writeRecord = store;
pub fn currentHead(a: A, txn: anytype, group: Group) !Head {
    return (try load(Head, a, txn, try key(a, group, 1, ""))) orelse Head{};
}

pub const UnitMembership = struct { snapshot_key_base64: []const u8 };
pub fn membershipSnapshotKey(a: A, document: []const u8, membership: UnitMembership) ![]const u8 {
    if (membership.snapshot_key_base64.len > 32768) return error.GroupedSourceCorrupt;
    const selected = try @import("group_source_reads.zig").decode(a, membership.snapshot_key_base64);
    const owner = try @import("group_source_transfer.zig").info(a, selected);
    if (owner.kind != metadata_kind or owner.suffix.len != 33 or owner.suffix[0] != 7 or !std.mem.eql(u8, owner.document, identity.root(document) orelse return error.GroupedSourceCorrupt)) return error.GroupedSourceCorrupt;
    return selected;
}
pub fn unitIsCurrent(a: A, txn: anytype, document: []const u8) !bool {
    const membership = (try load(UnitMembership, a, txn, try identity.unitMembershipKey(a, document))) orelse return false;
    const snapshot = (try load(Snapshot, a, txn, try membershipSnapshotKey(a, document, membership))) orelse return false;
    if (snapshot.units.len > 128 or snapshot.retired.len > 256) return error.GroupedSourceCorrupt;
    for (snapshot.units) |unit| if (std.mem.eql(u8, unit, document)) return true;
    for (snapshot.retired) |unit| if (std.mem.eql(u8, unit, document)) return true;
    return false;
}
