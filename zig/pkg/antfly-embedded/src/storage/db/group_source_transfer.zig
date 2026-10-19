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

//! Typed document-owned group records. Source-owned reverse/outbox records
//! and coordinator records travel with their logical owner, while global
//! covering/work indexes are reconstructed under the receiver's writer fence.
const std = @import("std");
const groups = @import("group_sources.zig");
const identity = @import("group_source_identity.zig");
const index = @import("group_source_index.zig");
const state = @import("group_source_state.zig");
const queue = @import("group_source_queue.zig");
const A = std.mem.Allocator;
pub const Info = struct { document: []const u8, kind: u8, suffix: []const u8 };
fn terminator(selected: []const u8) ?usize {
    if (selected.len < 4 or selected[0] != 1) return null;
    var offset: usize = 1;
    while (offset < selected.len) {
        if (selected[offset] != 0) {
            offset += 1;
            continue;
        }
        if (offset + 1 >= selected.len) return null;
        switch (selected[offset + 1]) {
            255 => offset += 2,
            0 => return offset,
            else => return null,
        }
    }
    return null;
}
pub fn isKey(selected: []const u8) bool {
    const end = terminator(selected) orelse return false;
    if (end + 2 >= selected.len) return false;
    return switch (selected[end + 2]) {
        identity.primary_kind => end + 3 == selected.len,
        0x11 => end + 3 == selected.len and std.mem.indexOf(u8, selected[1..end], "/\x00\xffantfly/group/v1/") != null,
        identity.reverse_kind => selected.len == end + 3 + 32,
        identity.outbox_kind => selected.len == end + 3 + 64,
        identity.metadata_kind => selected.len > end + 3,
        else => false,
    };
}
pub fn info(a: A, selected: []const u8) !Info {
    if (!isKey(selected) or selected.len > 32768) return error.InvalidGroupedSourceTransfer;
    const end = terminator(selected).?;
    var document: std.ArrayList(u8) = .empty;
    var offset: usize = 1;
    while (offset < end) : (offset += 1) {
        try document.append(a, selected[offset]);
        if (selected[offset] == 0) offset += 1;
    }
    if (document.items.len == 0 or document.items.len > 4096) return error.InvalidGroupedSourceTransfer;
    return .{ .document = try document.toOwnedSlice(a), .kind = selected[end + 2], .suffix = selected[end + 3 ..] };
}
fn metadataPayload(selected: []const u8, raw: []const u8) ![]const u8 {
    if (identity.isUnitContextEpochKey(selected)) {
        if (raw.len > 128) return error.InvalidGroupedSourceTransfer;
        return raw;
    }
    if (raw.len < 36 or raw.len > 262180 or !std.mem.eql(u8, raw[0..4], "GSC1") or !std.mem.eql(u8, raw[raw.len - 32 ..], &queue.checksum(selected, raw[4 .. raw.len - 32]))) return error.InvalidGroupedSourceTransfer;
    return raw[4 .. raw.len - 32];
}
pub fn validate(a: A, selected: []const u8, value: ?[]const u8) !void {
    const owner = try info(a, selected);
    const raw = value orelse return;
    if (raw.len > index.max_record_bytes) return error.InvalidGroupedSourceTransfer;
    switch (owner.kind) {
        identity.primary_kind => {
            if (!identity.isAnchor(owner.document)) return error.InvalidGroupedSourceTransfer;
            const source = try std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{});
            if (source != .object) return error.InvalidGroupedSourceTransfer;
            const root = source.object.get("_group_root") orelse return error.InvalidGroupedSourceTransfer;
            const unit = source.object.get("_group_unit") orelse return error.InvalidGroupedSourceTransfer;
            if (root != .string or unit != .object or !std.mem.eql(u8, root.string, identity.root(owner.document).?)) return error.InvalidGroupedSourceTransfer;
            const text = unit.object.get("text") orelse return error.InvalidGroupedSourceTransfer;
            const id = unit.object.get("unit_id") orelse return error.InvalidGroupedSourceTransfer;
            if (text != .string or id != .string or id.string.len != 64 or !std.mem.endsWith(u8, owner.document, id.string)) return error.InvalidGroupedSourceTransfer;
        },
        0x11 => {
            if (!identity.isAnchor(owner.document) or raw.len != 8 or std.mem.readInt(u64, raw[0..8], .little) == 0) return error.InvalidGroupedSourceTransfer;
        },
        identity.reverse_kind => {
            // The table identity is supplied by the certified transfer scope
            // during companion reconstruction. Verify key-bound bytes here.
            if (raw.len < 36 or !std.mem.eql(u8, raw[0..4], "GSI1") or !std.mem.eql(u8, raw[raw.len - 32 ..], &queue.checksum(selected, raw[4 .. raw.len - 32]))) return error.InvalidGroupedSourceTransfer;
            const projection = try std.json.parseFromSliceLeaky(index.Projection, a, raw[4 .. raw.len - 32], .{});
            if (!std.mem.eql(u8, projection.member.document, owner.document)) return error.InvalidGroupedSourceTransfer;
        },
        identity.outbox_kind => {
            const event = try index.readInvalidation(a, selected, raw);
            if (!std.mem.eql(u8, event.document, owner.document)) return error.InvalidGroupedSourceTransfer;
        },
        identity.metadata_kind => {
            if (!identity.isAnchor(owner.document)) return error.InvalidGroupedSourceTransfer;
            const payload = try metadataPayload(selected, raw);
            _ = try std.json.parseFromSliceLeaky(std.json.Value, a, payload, .{});
            if (owner.suffix.len == 1 and owner.suffix[0] == 13) {
                const membership = try std.json.parseFromSliceLeaky(state.UnitMembership, a, payload, .{});
                _ = try state.membershipSnapshotKey(a, owner.document, membership);
            }
        },
        else => return error.InvalidGroupedSourceTransfer,
    }
}
/// Call after staging the exact transferred record, in the SAME transaction.
/// Advancing the local clock prevents an imported source revision from making
/// the receiver's next ordinary mutation look older than its imported outbox.
pub fn reconstruct(a: A, txn: anytype, table: u128, selected: []const u8, value: ?[]const u8) !void {
    try validate(a, selected, value);
    const owner = try info(a, selected);
    const raw = value orelse return;
    switch (owner.kind) {
        identity.reverse_kind => {
            const definition = index.Definition{ .table_incarnation = table, .configuration = owner.suffix[0..32].* };
            const projection = try index.readReverse(a, definition, selected, raw);
            try index.ensureRevisionFloor(a, txn, definition, std.math.cast(u64, projection.member.version) orelse return error.InvalidGroupedSourceTransfer);
            const companion = try index.companion(a, definition, selected, raw);
            try txn.put(companion.key, companion.value);
        },
        identity.outbox_kind => {
            const event = try index.readInvalidation(a, selected, raw);
            if (event.definition.table_incarnation != table) return error.InvalidGroupedSourceTransfer;
            try index.ensureRevisionFloor(a, txn, event.definition, event.source_revision);
            try queue.stageSource(a, txn, selected);
            try @import("group_source_debt.zig").stage(a, txn, selected, null, event);
            try @import("group_source_order.zig").stage(a, txn, selected, null, event);
        },
        identity.metadata_kind => {
            if (owner.suffix[0] == 11) {
                const payload = try metadataPayload(selected, value.?);
                const epoch = try std.json.parseFromSliceLeaky(struct { revision: u64 }, a, payload, .{ .ignore_unknown_fields = true });
                try state.ensureLeaseFloor(a, txn, table, epoch.revision);
            }
            if (owner.suffix[0] == 1) {
                const head = (try state.readRecord(state.Head, a, txn, selected)) orelse return error.InvalidGroupedSourceTransfer;
                if (identity.tableOf(owner.document) != table) return error.InvalidGroupedSourceTransfer;
                try state.ensureLeaseFloor(a, txn, table, head.lease_counter);
            }
            if (owner.suffix[0] == 2) {
                const pending = (try state.readRecord(state.Pending, a, txn, selected)) orelse return error.InvalidGroupedSourceTransfer;
                const group = state.Group{ .definition = .{ .table_incarnation = identity.tableOf(owner.document) orelse return error.InvalidGroupedSourceTransfer, .configuration = identity.configurationOf(owner.document) orelse return error.InvalidGroupedSourceTransfer }, .key = pending.partition.group };
                if (group.definition.table_incarnation != table or !std.mem.eql(u8, selected, try state.pendingKey(a, group, pending.partition))) return error.InvalidGroupedSourceTransfer;
                try queue.stageGroup(a, txn, try group.anchor(a), group);
            }
        },
        else => {},
    }
}

/// Exact afterimage staging and companion replacement share one transaction.
/// In particular a transferred reverse tombstone must remove its old forward
/// entry rather than leave a ghost membership behind after a source delete.
pub fn stage(a: A, txn: anytype, table: u128, selected: []const u8, value: ?[]const u8) !void {
    try validate(a, selected, value);
    const owner = try info(a, selected);
    if (owner.kind == identity.reverse_kind) {
        const definition = index.Definition{ .table_incarnation = table, .configuration = owner.suffix[0..32].* };
        const old = txn.get(selected) catch |err| if (err == error.NotFound) null else return err;
        if (old) |raw| {
            const previous = try index.readReverse(a, definition, selected, raw);
            try txn.delete(try index.forwardKey(a, definition, previous));
        }
    }
    if (owner.kind == identity.outbox_kind) {
        const old = txn.get(selected) catch |err| if (err == error.NotFound) null else return err;
        if (old) |raw| {
            const event = try index.readInvalidation(a, selected, raw);
            try @import("group_source_debt.zig").remove(a, txn, selected, event);
            try @import("group_source_order.zig").remove(a, txn, selected, event);
        }
    }
    if (value) |raw| try txn.put(selected, raw) else try txn.delete(selected);
    try reconstruct(a, txn, table, selected, value);
    if (value == null and owner.kind == identity.outbox_kind) try queue.removeSource(a, txn, selected);
    if (value == null and owner.kind == identity.metadata_kind and owner.suffix[0] == 2) {
        const head_key = try identity.documentMetadataKey(a, owner.document, &.{1});
        if (try state.readRecord(state.Head, a, txn, head_key)) |head| if (head.pending == 0) try queue.removeGroup(a, txn, owner.document);
    }
}

test "group range transfer rebuilds covering indexes and outboxes without clock regression or tombstone ghosts" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Txn = struct {
        a: A,
        values: std.StringHashMapUnmanaged([]const u8) = .empty,
        pub fn get(self: *@This(), selected: []const u8) ![]const u8 {
            return self.values.get(selected) orelse error.NotFound;
        }
        pub fn put(self: *@This(), selected: []const u8, raw: []const u8) !void {
            try self.values.put(self.a, try self.a.dupe(u8, selected), try self.a.dupe(u8, raw));
        }
        pub fn delete(self: *@This(), selected: []const u8) !void {
            _ = self.values.remove(selected);
        }
    };
    var donor = Txn{ .a = a };
    var receiver = Txn{ .a = a };
    const definition = index.Definition{ .table_incarnation = 7, .configuration = groups.digest("configuration") };
    const document = "row\x00/one";
    const selected = index.Projection{ .partition = .{ .group = "thread", .segment = "turn", .cell = 0 }, .member = .{ .document = document, .version = 100, .ordinal = 3, .segment = "turn", .text = "source text" } };
    _ = try index.updateProjection(a, &donor, definition, document, 100, 40, selected);
    const reverse = try index.reverseKey(a, document, definition);
    var outbox: []const u8 = "";
    var it = donor.values.iterator();
    while (it.next()) |entry| if (isKey(entry.key_ptr.*)) {
        const ownership = try info(a, entry.key_ptr.*);
        if (ownership.kind == identity.outbox_kind) outbox = entry.key_ptr.*;
    };
    try std.testing.expect(outbox.len > 0);
    try stage(a, &receiver, 7, reverse, try donor.get(reverse));
    try stage(a, &receiver, 7, outbox, try donor.get(outbox));
    const forward = try index.forwardKey(a, definition, selected);
    _ = try receiver.get(forward);
    _ = try receiver.get(try queue.sourceKey(a, outbox));
    const next = try index.nextRevision(a, &receiver, definition);
    try std.testing.expect(next > 100);
    var changed = selected;
    changed.member.text = "new receiver text";
    _ = try index.updateProjection(a, &receiver, definition, document, next, 90, changed);
    const event = try index.readInvalidation(a, outbox, try receiver.get(outbox));
    try std.testing.expectEqual(next, event.source_revision);
    try std.testing.expectEqual(@as(u64, 40), event.since_sequence);
    try std.testing.expectEqual(@as(u64, 90), event.through_sequence);
    try stage(a, &receiver, 7, reverse, null);
    try stage(a, &receiver, 7, outbox, null);
    try std.testing.expectError(error.NotFound, receiver.get(forward));
    try std.testing.expectError(error.NotFound, receiver.get(try queue.sourceKey(a, outbox)));
    const anchor = try identity.anchor(a, 7, definition.configuration, groups.digest("thread"));
    const ttl = try identity.documentMetadataKey(a, anchor, "");
    ttl[ttl.len - 1] = 0x11;
    var timestamp: [8]u8 = undefined;
    std.mem.writeInt(u64, &timestamp, 123456789, .little);
    try std.testing.expect(isKey(ttl));
    try stage(a, &receiver, 7, ttl, &timestamp);
    try std.testing.expectEqualSlices(u8, &timestamp, try receiver.get(ttl));
    try std.testing.expectError(error.InvalidGroupedSourceTransfer, validate(a, ttl, "bad"));
    try stage(a, &receiver, 7, ttl, null);
    try std.testing.expectError(error.NotFound, receiver.get(ttl));
    // A child may precede its coordinator in a certified transfer.
    const unit_prefix = try identity.partition(a, anchor, "turn", 0);
    const unit = try std.fmt.allocPrint(a, "{s}/u/{x}", .{ unit_prefix, &groups.digest("birth") });
    const context_key = try identity.unitContextEpochKey(a, unit);
    try stage(a, &receiver, 7, context_key, "{\"revision\":999}");
    var clock: [20]u8 = undefined;
    @memcpy(clock[0..4], state.lease_clock_prefix);
    std.mem.writeInt(u128, clock[4..20], 7, .big);
    try std.testing.expectEqual(@as(u64, 999), (try state.readRecord(u64, a, &receiver, &clock)).?);
    const group = state.Group{ .definition = definition, .key = "thread" };
    const partition = index.Partition{ .group = "thread", .segment = "turn", .cell = 0 };
    const snapshot_key = try state.snapshotKey(a, group, partition);
    const membership_key = try identity.unitMembershipKey(a, unit);
    try state.writeRecord(a, &donor, membership_key, state.UnitMembership{ .snapshot_key_base64 = try @import("group_source_reads.zig").encode(a, snapshot_key) });
    try stage(a, &receiver, 7, membership_key, try donor.get(membership_key));
    try std.testing.expect(!try state.unitIsCurrent(a, &receiver, unit));
    try state.writeRecord(a, &donor, snapshot_key, state.Snapshot{ .revision = 1, .input_digest = groups.digest("snapshot"), .units = &.{unit}, .sequence = 1 });
    try stage(a, &receiver, 7, snapshot_key, try donor.get(snapshot_key));
    try std.testing.expect(try state.unitIsCurrent(a, &receiver, unit));
    const corrupted = try a.dupe(u8, try donor.get(reverse));
    corrupted[corrupted.len - 1] ^= 1;
    try std.testing.expectError(error.InvalidGroupedSourceTransfer, validate(a, reverse, corrupted));
}
