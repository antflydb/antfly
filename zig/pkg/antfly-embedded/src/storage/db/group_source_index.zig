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

//! Source-owned covering membership index and invalidation outbox. Every
//! update runs in the primary row transaction. Group owners receive only
//! invalidations and query these current ordered ranges; they never apply
//! out-of-order replicated copies of source text. Reverse/outbox records are
//! document-owned so range transfer can reconstruct their forward companions.
const std = @import("std");
const groups = @import("group_sources.zig");
const A = std.mem.Allocator;
const V = std.json.Value;
const Digest = groups.Digest;
pub const forward_namespace = "\x00\x00G\x01";
/// Reserved document-local families; centralized by internal_keys at wiring.
pub const reverse_kind = @import("group_source_identity.zig").reverse_kind;
pub const outbox_kind = @import("group_source_identity.zig").outbox_kind;
pub const max_record_bytes = 4 * 1024 * 1024 + 32768;
pub const Definition = struct {
    table_incarnation: u128,
    configuration: Digest,
    pub fn validate(self: Definition) !void {
        if (self.table_incarnation == 0 or std.mem.allEqual(u8, &self.configuration, 0)) return error.InvalidGroupedSourceConfig;
    }
};
pub const Partition = struct {
    group: []const u8,
    segment: []const u8,
    cell: i64,
};
pub const Projection = struct {
    partition: Partition,
    member: groups.Member,
};
pub const Invalidation = struct {
    definition: Definition,
    partition: Partition,
    document: []const u8,
    /// Source-owned monotone local commit number, never compared across owners.
    source_revision: u64,
    /// The oldest still-unacknowledged source replay obligation survives
    /// coalescing with newer writes. Zero denotes catalog backfill work.
    since_sequence: u64 = 0,
    through_sequence: u64 = 0,
    ordinal: i64,
    repair: bool = false,
    /// The group owner durably enrolled this event. Causal visibility debt can
    /// outlive enrollment; it must not block the later work that discharges it.
    delivered: bool = false,
    delivery_revision: u64 = 0,
};
pub const Filter = struct {
    ptr: *anyopaque,
    matches: *const fn (*anyopaque, V, V) anyerror!bool,
};
fn appendComponent(a: A, out: *std.ArrayList(u8), bytes: []const u8) !void {
    for (bytes) |byte| {
        try out.append(a, byte);
        if (byte == 0) try out.append(a, 255);
    }
    try out.appendSlice(a, &.{ 0, 0 });
}
fn documentKey(a: A, document: []const u8, kind: u8, definition: Definition) ![]u8 {
    try definition.validate();
    if (document.len == 0 or document.len > 4096) return error.InvalidGroupedSourceMember;
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(a);
    try out.append(a, 1);
    try appendComponent(a, &out, document);
    try out.append(a, kind);
    try out.appendSlice(a, &definition.configuration);
    return out.toOwnedSlice(a);
}
pub fn reverseKey(a: A, document: []const u8, definition: Definition) ![]u8 {
    return documentKey(a, document, reverse_kind, definition);
}
pub fn partitionPrefix(a: A, definition: Definition, partition: Partition) ![]u8 {
    try definition.validate();
    if (partition.group.len == 0 or partition.segment.len == 0 or partition.group.len > 4096 or partition.segment.len > 4096) return error.InvalidGroupedSourceMember;
    var table: [16]u8 = undefined;
    var cell: [8]u8 = undefined;
    std.mem.writeInt(u128, &table, definition.table_incarnation, .big);
    std.mem.writeInt(u64, &cell, @as(u64, @bitCast(partition.cell)) ^ (@as(u64, 1) << 63), .big);
    return std.mem.concat(a, u8, &.{ forward_namespace, &table, &definition.configuration, &groups.digest(partition.group), &groups.digest(partition.segment), &cell });
}
pub fn forwardKey(a: A, definition: Definition, projection: Projection) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(a);
    const prefix = try partitionPrefix(a, definition, projection.partition);
    defer a.free(prefix);
    try out.appendSlice(a, prefix);
    var ordinal: [8]u8 = undefined;
    std.mem.writeInt(u64, &ordinal, @as(u64, @bitCast(projection.member.ordinal)) ^ (@as(u64, 1) << 63), .big);
    try out.appendSlice(a, &ordinal);
    try appendComponent(a, &out, projection.member.document);
    return out.toOwnedSlice(a);
}
/// A clear page names a derived key, not an authoritative afterimage. Decode
/// its document without consulting replica-local covering bytes.
pub fn forwardDocument(a: A, definition: Definition, selected: []const u8) ![]const u8 {
    if (selected.len < 135 or !std.mem.startsWith(u8, selected, forward_namespace) or std.mem.readInt(u128, selected[4..20], .big) != definition.table_incarnation or !std.mem.eql(u8, selected[20..52], &definition.configuration)) return error.GroupedSourceCorrupt;
    var out: std.ArrayList(u8) = .empty;
    var offset: usize = 132;
    while (offset < selected.len) {
        const byte = selected[offset];
        offset += 1;
        if (byte != 0) {
            try out.append(a, byte);
        } else {
            if (offset == selected.len) return error.GroupedSourceCorrupt;
            const next = selected[offset];
            offset += 1;
            if (next == 0) {
                if (offset != selected.len or out.items.len == 0 or out.items.len > 4096) return error.GroupedSourceCorrupt;
                return out.toOwnedSlice(a);
            }
            if (next != 255) return error.GroupedSourceCorrupt;
            try out.append(a, 0);
        }
        if (out.items.len > 4096) return error.GroupedSourceCorrupt;
    }
    return error.GroupedSourceCorrupt;
}
fn encode(a: A, key: []const u8, value: anytype) ![]u8 {
    const payload = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(payload);
    if (payload.len > max_record_bytes) return error.GroupedSourceMemberTooLarge;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(key);
    hash.update(payload);
    return std.mem.concat(a, u8, &.{ "GSI1", payload, &hash.finalResult() });
}
fn recordPayload(key: []const u8, raw: []const u8) ![]const u8 {
    if (raw.len < 36 or raw.len > max_record_bytes + 36 or !std.mem.eql(u8, raw[0..4], "GSI1")) return error.GroupedSourceCorrupt;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(key);
    hash.update(raw[4 .. raw.len - 32]);
    if (!std.mem.eql(u8, raw[raw.len - 32 ..], &hash.finalResult())) return error.GroupedSourceCorrupt;
    return raw[4 .. raw.len - 32];
}
fn decode(comptime T: type, a: A, key: []const u8, raw: []const u8) !T {
    return std.json.parseFromSliceLeaky(T, a, try recordPayload(key, raw), .{ .allocate = .alloc_always }) catch |err| if (err == error.OutOfMemory) err else error.GroupedSourceCorrupt;
}
/// The covering row accelerates text reads; only the document-owned reverse
/// afterimage authorizes membership. Extra local hints cannot revive old text.
pub fn coveringIsCurrent(a: A, txn: anytype, definition: Definition, projection: Projection, forward: []const u8) !bool {
    const selected = try reverseKey(a, projection.member.document, definition);
    defer a.free(selected);
    const raw = (try optional(txn, selected)) orelse return false;
    return std.mem.eql(u8, try recordPayload(selected, raw), forward[4 .. forward.len - 32]);
}
fn optional(txn: anytype, key: []const u8) !?[]const u8 {
    return txn.get(key) catch |err| if (err == error.NotFound) null else return err;
}
fn partitionEqual(left: Partition, right: Partition) bool {
    return left.cell == right.cell and std.mem.eql(u8, left.group, right.group) and std.mem.eql(u8, left.segment, right.segment);
}
fn equivalent(left: ?Projection, right: ?Projection) bool {
    if (left == null or right == null) return left == null and right == null;
    return partitionEqual(left.?.partition, right.?.partition) and left.?.member.ordinal == right.?.member.ordinal and
        std.mem.eql(u8, left.?.member.document, right.?.member.document) and std.mem.eql(u8, left.?.member.text, right.?.member.text);
}
pub fn dirty(a: A, txn: anytype, definition: Definition, partition: Partition, document: []const u8, revision: u64, sequence: u64, ordinal: i64) !void {
    return dirtyMode(a, txn, definition, partition, document, revision, sequence, ordinal, false);
}
fn dirtyMode(a: A, txn: anytype, definition: Definition, partition: Partition, document: []const u8, revision: u64, sequence: u64, ordinal: i64, repair: bool) !void {
    const point = try documentKey(a, document, outbox_kind, definition);
    defer a.free(point);
    const prefix = try partitionPrefix(a, definition, partition);
    defer a.free(prefix);
    const key = try std.mem.concat(a, u8, &.{ point, &groups.digest(prefix) });
    defer a.free(key);
    var since_sequence = sequence;
    var first_ordinal = ordinal;
    var prior: ?Invalidation = null;
    if (try optional(txn, key)) |previous| {
        const pending = try readInvalidation(a, key, previous);
        if (repair) {
            _ = try setDelivery(a, txn, key, previous, false);
            return; // Existing delivery already repairs this partition.
        }
        prior = pending;
        since_sequence = @min(since_sequence, pending.since_sequence);
        first_ordinal = @min(first_ordinal, pending.ordinal);
    }
    const partition_revision = try partitionRevisionKey(a, definition, partition);
    var revision_bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &revision_bytes, revision, .big);
    try txn.put(partition_revision, try @import("group_source_queue.zig").encode(a, partition_revision, &revision_bytes));
    const event = Invalidation{ .definition = definition, .partition = partition, .document = document, .source_revision = revision, .since_sequence = since_sequence, .through_sequence = sequence, .ordinal = first_ordinal, .repair = repair };
    const raw = try encode(a, key, event);
    defer a.free(raw);
    try txn.put(key, raw);
    try @import("group_source_queue.zig").stageSource(a, txn, key);
    try @import("group_source_debt.zig").stage(a, txn, key, prior, event);
    try @import("group_source_order.zig").stage(a, txn, key, prior, event);
}

/// Reconstructing ownership is not a new history event. It may repair a lost
/// coordinator without replaying unchanged units or extending prior context.
pub fn repairPartition(a: A, txn: anytype, definition: Definition, projection: Projection, revision: u64) !void {
    try dirtyMode(a, txn, definition, projection.partition, projection.member.document, revision, 0, projection.member.ordinal, true);
}

/// Call on a single-writer transaction with the row's logical afterimage.
/// The reverse record is the previous grouping projection, so deletes/moves
/// need neither an old-row parse nor a table scan. No provider executes here.
/// Allocation belongs to a short transaction arena and is bounded per row.
pub fn update(a: A, txn: anytype, definition: Definition, config: groups.Config, document: []const u8, revision: u64, after: ?V, filter: ?Filter) !bool {
    try definition.validate();
    try config.validate();
    if (revision == 0) return error.InvalidGroupedSourceMutation;
    const next: ?Projection = if (after) |value| blk: {
        const matches = if (config.filter == .null) true else if (filter) |bound| try bound.matches(bound.ptr, config.filter, value) else return error.InvalidGroupedSourceConfig;
        const selection = (try groups.selectMember(a, config, document, revision, value, matches)) orelse break :blk null;
        break :blk .{ .partition = .{ .group = selection.group, .segment = selection.member.segment, .cell = @divFloor(selection.member.ordinal, @as(i64, config.window_ordinals)) }, .member = selection.member };
    } else null;
    return updateProjection(a, txn, definition, document, revision, revision, next);
}

pub fn updateProjection(a: A, txn: anytype, definition: Definition, document: []const u8, revision: u64, sequence: u64, selected: ?Projection) !bool {
    try definition.validate();
    if (revision == 0) return error.InvalidGroupedSourceMutation;
    var next = selected;
    if (next) |*value| {
        if (!std.mem.eql(u8, value.member.document, document)) return error.InvalidGroupedSourceMutation;
        value.member.version = revision;
    }
    const reverse = try reverseKey(a, document, definition);
    defer a.free(reverse);
    const previous: ?Projection = if (try optional(txn, reverse)) |raw| try decode(Projection, a, reverse, raw) else null;
    if (equivalent(previous, next)) return false;
    if (previous) |old| {
        try txn.delete(try forwardKey(a, definition, old));
        try dirty(a, txn, definition, old.partition, document, revision, sequence, old.member.ordinal);
    }
    if (next) |value| {
        const forward = try forwardKey(a, definition, value);
        try txn.put(forward, try encode(a, forward, value));
        try txn.put(reverse, try encode(a, reverse, value));
        if (previous == null or !partitionEqual(previous.?.partition, value.partition)) try dirty(a, txn, definition, value.partition, document, revision, sequence, value.member.ordinal);
    } else if (previous != null) try txn.delete(reverse);
    return true;
}

pub fn eventDigest(a: A, event: Invalidation) !Digest {
    var logical = event;
    logical.delivered = false;
    logical.delivery_revision = 0;
    return groups.digest(try std.json.Stringify.valueAlloc(a, logical, .{}));
}
pub fn setDelivery(a: A, txn: anytype, key: []const u8, observed: []const u8, delivered: bool) !bool {
    const current = (try optional(txn, key)) orelse return false;
    if (!std.mem.eql(u8, current, observed)) return false;
    var event = try readInvalidation(a, key, current);
    if (event.delivered == delivered) return true;
    const previous = event;
    event.delivered = delivered;
    const Txn = @typeInfo(@TypeOf(txn)).pointer.child;
    event.delivery_revision = if (comptime @hasDecl(Txn, "eventRevision")) try txn.eventRevision(event.definition) else try reserveRevision(a, txn, event.definition, null);
    try txn.put(key, try encode(a, key, event));
    try @import("group_source_order.zig").stage(a, txn, key, previous, event);
    return true;
}

/// A source owner removes an outbox entry only after the group owner's durable
/// acknowledgement. Comparing the complete key-bound bytes preserves a newer
/// coalesced invalidation if it arrived while the delivery was in flight.
pub fn acknowledge(a: A, txn: anytype, key: []const u8, observed: []const u8) !bool {
    const current = (try optional(txn, key)) orelse return true;
    if (!std.mem.eql(u8, current, observed)) return false;
    try txn.delete(key);
    try @import("group_source_queue.zig").removeSource(a, txn, key);
    try @import("group_source_debt.zig").remove(a, txn, key, try readInvalidation(a, key, observed));
    try @import("group_source_order.zig").remove(a, txn, key, try readInvalidation(a, key, observed));
    return true;
}
pub fn readInvalidation(a: A, key: []const u8, raw: []const u8) !Invalidation {
    const value = try decode(Invalidation, a, key, raw);
    try value.definition.validate();
    if (value.source_revision == 0 or value.since_sequence > value.through_sequence) return error.GroupedSourceCorrupt;
    const point = try documentKey(a, value.document, outbox_kind, value.definition);
    const prefix = try partitionPrefix(a, value.definition, value.partition);
    const expected = try std.mem.concat(a, u8, &.{ point, &groups.digest(prefix) });
    if (!std.mem.eql(u8, key, expected)) return error.GroupedSourceCorrupt;
    return value;
}
pub fn readProjection(a: A, definition: Definition, key: []const u8, raw: []const u8) !Projection {
    const value = try decode(Projection, a, key, raw);
    if (!std.mem.eql(u8, key, try forwardKey(a, definition, value))) return error.GroupedSourceCorrupt;
    return value;
}

/// Range transfer consumes the document-owned reverse record and recreates
/// the exact forward companion; copying arbitrary global index ranges could
/// import memberships outside the transferred primary-key interval.
pub fn companion(a: A, definition: Definition, reverse: []const u8, raw: []const u8) !struct { key: []u8, value: []u8 } {
    const projection = try decode(Projection, a, reverse, raw);
    if (!std.mem.eql(u8, reverse, try reverseKey(a, projection.member.document, definition))) return error.GroupedSourceCorrupt;
    const key = try forwardKey(a, definition, projection);
    return .{ .key = key, .value = try encode(a, key, projection) };
}

test "group source covering index stages moves deletes and revision-safe invalidation acknowledgement" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Txn = struct {
        a: A,
        map: std.StringHashMapUnmanaged([]const u8) = .empty,
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            return self.map.get(key) orelse error.NotFound;
        }
        pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            try self.map.put(self.a, try self.a.dupe(u8, key), try self.a.dupe(u8, value));
        }
        pub fn delete(self: *@This(), key: []const u8) !void {
            _ = self.map.remove(key);
        }
    };
    var txn = Txn{ .a = a };
    const definition = Definition{ .table_incarnation = 7, .configuration = groups.digest("group-source-config") };
    const cfg = groups.Config{ .group_by = "thread", .order_by = "ordinal", .segment_by = "turn", .field = "text" };
    const row = try std.json.parseFromSliceLeaky(V, a,
        \\{"thread":"thread-a","ordinal":1,"turn":"turn-a","text":"hello"}
    , .{});
    try std.testing.expect(try update(a, &txn, definition, cfg, "row", 1, row, null));
    const reverse = try reverseKey(a, "row", definition);
    const transferred = try companion(a, definition, reverse, try txn.get(reverse));
    try std.testing.expectEqualStrings(try txn.get(transferred.key), transferred.value);
    try std.testing.expectEqualStrings("row", try forwardDocument(a, definition, transferred.key));
    const covered = try readProjection(a, definition, transferred.key, transferred.value);
    try std.testing.expect(try coveringIsCurrent(a, &txn, definition, covered, transferred.value));
    const before_count = txn.map.count();
    // An unrelated primary rewrite creates no invalidation or index churn.
    try std.testing.expect(!try update(a, &txn, definition, cfg, "row", 2, row, null));
    try std.testing.expectEqual(before_count, txn.map.count());
    var old_key: []const u8 = undefined;
    var old_value: []const u8 = undefined;
    var it = txn.map.iterator();
    while (it.next()) |entry| {
        if (entry.key_ptr.*.len > reverse.len and entry.key_ptr.*[reverse.len - 33] == outbox_kind) {
            old_key = entry.key_ptr.*;
            old_value = entry.value_ptr.*;
            break;
        }
    }
    const moved = try std.json.parseFromSliceLeaky(V, a,
        \\{"thread":"thread-b","ordinal":129,"turn":"turn-b","text":"changed"}
    , .{});
    try std.testing.expect(try update(a, &txn, definition, cfg, "row", 3, moved, null));
    try std.testing.expectError(error.NotFound, txn.get(transferred.key));
    try std.testing.expect(!try coveringIsCurrent(a, &txn, definition, covered, transferred.value));
    try std.testing.expect(!try acknowledge(a, &txn, old_key, old_value));
    const newer = try txn.get(old_key);
    const invalidation = try readInvalidation(a, old_key, newer);
    try std.testing.expectEqual(@as(u64, 3), invalidation.source_revision);
    try std.testing.expectEqualStrings("thread-a", invalidation.partition.group);
    try std.testing.expect(try acknowledge(a, &txn, old_key, newer));
    try std.testing.expect(try update(a, &txn, definition, cfg, "row", 4, null, null));
    try std.testing.expectError(error.NotFound, txn.get(reverse));
    try std.testing.expect(!try update(a, &txn, definition, cfg, "row", 5, null, null));
    const damaged = try a.dupe(u8, transferred.value);
    damaged[damaged.len - 1] ^= 1;
    try std.testing.expectError(error.GroupedSourceCorrupt, readProjection(a, definition, transferred.key, damaged));
}

pub const revision_prefix = "\x00\x00G\x06";
pub fn revisionKey(a: A, definition: Definition) ![]u8 {
    try definition.validate();
    var table: [16]u8 = undefined;
    std.mem.writeInt(u128, &table, definition.table_incarnation, .big);
    return std.mem.concat(a, u8, &.{ revision_prefix, &table });
}
pub fn currentRevision(a: A, txn: anytype, definition: Definition) !u64 {
    const key = try revisionKey(a, definition);
    defer a.free(key);
    const raw = (try optional(txn, key)) orelse return 0;
    try validateRevisionClock(key, raw);
    return std.mem.readInt(u64, raw[0..8], .big);
}
pub fn nextRevision(a: A, txn: anytype, definition: Definition) !u64 {
    return reserveRevision(a, txn, definition, null);
}
pub fn reserveRevision(a: A, txn: anytype, definition: Definition, raft_index: ?u64) !u64 {
    const value = @max(try std.math.add(u64, try currentRevision(a, txn, definition), 1), raft_index orelse 0);
    const key = try revisionKey(a, definition);
    defer a.free(key);
    var raw: [40]u8 = undefined;
    std.mem.writeInt(u64, raw[0..8], value, .big);
    @memcpy(raw[8..], &@import("group_source_queue.zig").checksum(key, raw[0..8]));
    try txn.put(key, &raw);
    return value;
}

pub fn partitionRevisionKey(a: A, definition: Definition, partition: Partition) ![]u8 {
    const selected = try partitionPrefix(a, definition, partition);
    selected[3] = 8;
    return selected;
}
pub fn partitionRevision(a: A, txn: anytype, definition: Definition, partition: Partition) !u64 {
    const selected = try partitionRevisionKey(a, definition, partition);
    const raw = txn.get(selected) catch |err| return if (err == error.NotFound) 0 else err;
    const bytes = try @import("group_source_queue.zig").decode(selected, raw);
    if (bytes.len != 8) return error.GroupedSourceCorrupt;
    return std.mem.readInt(u64, bytes[0..8], .big);
}

pub fn readReverse(a: A, definition: Definition, selected: []const u8, raw: []const u8) !Projection {
    const value = try decode(Projection, a, selected, raw);
    if (value.member.version == 0 or !std.mem.eql(u8, selected, try reverseKey(a, value.member.document, definition))) return error.GroupedSourceCorrupt;
    _ = try forwardKey(a, definition, value);
    return value;
}
pub fn ensureRevisionFloor(a: A, txn: anytype, definition: Definition, floor: u64) !void {
    if (try currentRevision(a, txn, definition) >= floor) return;
    const selected = try revisionKey(a, definition);
    var raw: [40]u8 = undefined;
    std.mem.writeInt(u64, raw[0..8], floor, .big);
    @memcpy(raw[8..], &@import("group_source_queue.zig").checksum(selected, raw[0..8]));
    try txn.put(selected, &raw);
}

pub fn isRevisionClockKey(selected: []const u8) bool {
    return selected.len == revision_prefix.len + 16 and std.mem.startsWith(u8, selected, revision_prefix);
}
pub fn validateRevisionClock(selected: []const u8, raw: []const u8) !void {
    if (!isRevisionClockKey(selected) or raw.len != 40 or std.mem.readInt(u64, raw[0..8], .big) == 0 or !std.mem.eql(u8, raw[8..], &@import("group_source_queue.zig").checksum(selected, raw[0..8]))) return error.GroupedSourceCorrupt;
}

test "source revision clock is table scoped portable and independent of retired configurations" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Txn = struct {
        a: A,
        values: std.StringHashMapUnmanaged([]const u8) = .empty,
        pub fn get(self: *@This(), selected: []const u8) anyerror![]const u8 {
            return self.values.get(selected) orelse error.NotFound;
        }
        pub fn put(self: *@This(), selected: []const u8, raw: []const u8) !void {
            try self.values.put(self.a, try self.a.dupe(u8, selected), try self.a.dupe(u8, raw));
        }
    };
    var txn = Txn{ .a = a };
    const first = Definition{ .table_incarnation = 7, .configuration = groups.digest("first") };
    const replacement = Definition{ .table_incarnation = 7, .configuration = groups.digest("replacement") };
    try std.testing.expectEqual(@as(u64, 1), try nextRevision(a, &txn, first));
    try std.testing.expectEqual(@as(u64, 2), try nextRevision(a, &txn, replacement));
    try ensureRevisionFloor(a, &txn, first, 99);
    try std.testing.expectEqual(@as(u64, 100), try nextRevision(a, &txn, replacement));
    // A new owner can inherit a clock ahead of its own Raft log. Its first
    // event must still be newer than an imported outbox already delivered.
    try std.testing.expectEqual(@as(u64, 101), try reserveRevision(a, &txn, replacement, 3));
    try std.testing.expectEqual(@as(u64, 500), try reserveRevision(a, &txn, replacement, 500));
    const selected = try revisionKey(a, first);
    try std.testing.expect(isRevisionClockKey(selected));
    try std.testing.expectEqual(@as(usize, 20), selected.len);
    try validateRevisionClock(selected, try txn.get(selected));
    const damaged = try a.dupe(u8, try txn.get(selected));
    damaged[0] ^= 1;
    try std.testing.expectError(error.GroupedSourceCorrupt, validateRevisionClock(selected, damaged));
}
