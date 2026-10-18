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

//! Generation-addressed retirement, not repeated full-table vacuuming. The
//! catalog enqueues IDs in its publication transaction. IDs never resurrect;
//! each bounded page deletes forward/reverse companions and advances its
//! durable cursor atomically. Readers of older snapshots retain their records.
const std = @import("std");
const time = @import("antfly_platform").time;
const catalog = @import("relational_index_catalog.zig");
const records = @import("relational_index_records.zig");
const jobs = @import("relational_index_jobs.zig");
const maintenance = @import("relational_index_maintenance_contract.zig");
const Allocator = std.mem.Allocator;
pub const prefix = "\x00\x00__metadata__:relational_index_retired:";
const max_cursor = 1024 * 1024;
pub const max_pending_generations: usize = 2 * catalog.max_index_count;
// One extra generation may be queued by the sole canceled private owner job.
// Its exact intent, burned binding and durable admission marker guard retries;
// ordinary churn cannot consume this cancellation-only headroom.
pub const max_total_pending_generations: usize = max_pending_generations + 1;

/// Admission is bounded independently of table size. The caller's catalog
/// transaction owns the queue decision and its insertions atomically. Refuse
/// new churn at capacity; deletion workers never need admission headroom.
pub fn admitRetirements(txn: anytype, additional: usize) !void {
    if (additional == 0) return;
    if (additional > max_pending_generations) return error.ResourceBudgetExceeded;
    var cursor = try txn.openCursor();
    defer cursor.close();
    var entry = try cursor.seekAtOrAfter(prefix);
    var count = additional;
    while (entry) |kv| : (entry = try cursor.next()) {
        if (!std.mem.startsWith(u8, kv.key, prefix)) break;
        if (count == max_pending_generations) return error.ResourceBudgetExceeded;
        _ = try records.Id.decode(kv.key[prefix.len..]);
        count += 1;
    }
}

pub fn key(id: records.Id) [prefix.len + records.Id.encoded_len]u8 {
    var result: [prefix.len + records.Id.encoded_len]u8 = undefined;
    @memcpy(result[0..prefix.len], prefix);
    @memcpy(result[prefix.len..], &id.encode());
    return result;
}

fn checksum(id: records.Id, bytes: []const u8) [32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update(&key(id));
    hash.update(bytes);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

const Phase = enum(u8) { forward, ownership };
const Progress = struct { phase: Phase, after: []const u8 };

pub fn initial(id: records.Id) [40]u8 {
    var value: [40]u8 = undefined;
    @memcpy(value[0..8], "ARGC\x03\x00\x00\x00");
    @memcpy(value[8..], &checksum(id, value[0..8]));
    return value;
}

fn encode(alloc: Allocator, id: records.Id, phase: Phase, after: []const u8) ![]u8 {
    if (after.len > max_cursor) return error.InvalidRelationalIndexGcProgress;
    const value = try alloc.alloc(u8, 40 + after.len);
    @memcpy(value[0..8], "ARGC\x03\x00\x00\x00");
    value[5] = @backingInt(phase);
    @memcpy(value[8..][0..after.len], after);
    @memcpy(value[value.len - 32 ..], &checksum(id, value[0 .. value.len - 32]));
    return value;
}

pub fn decode(id: records.Id, value: []const u8) ![]const u8 {
    return (try decodeProgress(id, value)).after;
}

fn decodeProgress(id: records.Id, value: []const u8) !Progress {
    if (value.len < 40 or value.len > 40 + max_cursor or !std.mem.eql(u8, value[0..5], "ARGC\x03") or value[6] != 0 or value[7] != 0 or
        !std.mem.eql(u8, value[value.len - 32 ..], &checksum(id, value[0 .. value.len - 32])))
        return error.InvalidRelationalIndexGcProgress;
    const phase = std.enums.fromInt(Phase, value[5]) orelse return error.InvalidRelationalIndexGcProgress;
    const after = value[8 .. value.len - 32];
    if (after.len != 0) {
        if (phase == .forward) {
            if (!std.mem.startsWith(u8, after, &(try records.forwardPrefix(id)))) return error.InvalidRelationalIndexGcProgress;
        } else if (!std.mem.startsWith(u8, after, &(try records.ownershipPrefix(id)))) return error.InvalidRelationalIndexGcProgress;
    }
    return .{ .phase = phase, .after = after };
}

pub const Page = struct {
    /// Fixed leader selection. Replay owns the same interval/count rather
    /// than treating the follower's clock as part of the replicated command.
    pub const Replay = struct {
        pub const max_wire_bytes = 9 * 1024 * 1024;
        pub const max_progress_bytes = 40 + max_cursor;
        binding: @import("relational_index_owner_job.zig").Binding,
        expected: []const u8,
        next: ?[]const u8,
        records_scanned: u16,

        pub fn jsonStringify(self: @This(), stream: anytype) @TypeOf(stream.*).Error!void {
            try @import("relational_integrity_json.zig").write(self, stream);
        }

        pub fn validate(self: @This()) !void {
            _ = try self.binding.encode();
            if (self.binding.authority != .raft or self.binding.phase != .reserved or self.records_scanned > 256) return error.InvalidRelationalIndexGcProgress;
            const before = try decodeProgress(self.binding.index, self.expected);
            if (self.next) |bytes| {
                const after = try decodeProgress(self.binding.index, bytes);
                if (before.phase == after.phase) {
                    if (self.records_scanned == 0 or std.mem.order(u8, after.after, before.after) != .gt) return error.InvalidRelationalIndexGcProgress;
                } else if (before.phase != .forward or after.phase != .ownership or after.after.len != 0) return error.InvalidRelationalIndexGcProgress;
            } else if (before.phase != .ownership) return error.InvalidRelationalIndexGcProgress;
        }

        pub fn applied(self: @This(), txn: anytype) !bool {
            try self.validate();
            const owner_jobs = @import("relational_index_owner_job.zig");
            _ = try owner_jobs.requireCancellationCleanup(txn, self.binding);
            const raw = txn.get(&key(self.binding.index)) catch |err| {
                if (err != error.NotFound) return err;
                try owner_jobs.requireCancellationArtifactsRetired(txn, self.binding);
                return true;
            };
            if (std.mem.eql(u8, raw, self.expected)) return false;
            const current = try decodeProgress(self.binding.index, raw);
            const after = try decodeProgress(self.binding.index, self.next orelse return error.VersionConflict);
            if (current.phase == .ownership and after.phase == .forward) return true;
            if (current.phase == after.phase and std.mem.order(u8, current.after, after.after) != .lt) return true;
            return error.VersionConflict;
        }

        pub fn requireApplied(self: @This(), txn: anytype) !void {
            if (!try self.applied(txn)) return error.VersionConflict;
        }
    };

    arena: std.heap.ArenaAllocator,
    pinned: ?catalog.WriteSnapshot,
    namespace: u64,
    owner: [32]u8,
    id: records.Id,
    expected: []const u8,
    next: ?[]const u8,
    deletes: []const []const u8,
    records_scanned: usize,
    consumed: bool = false,

    pub fn deinit(self: *Page) void {
        if (self.pinned) |*value| value.deinit();
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn prepare(alloc: Allocator, io: ?std.Io, core: anytype) !?Page {
        return prepareSelected(alloc, io, core, null, null);
    }

    /// Cancellation must not wait behind the unrelated public retirement queue.
    /// Selection is a point lookup; scanning retains the same bounded engine.
    pub fn prepareGeneration(alloc: Allocator, io: ?std.Io, core: anytype, id: records.Id) !?Page {
        return prepareSelected(alloc, io, core, id, null);
    }

    pub fn command(self: *const Page, binding: @import("relational_index_owner_job.zig").Binding) !Replay {
        if (binding.index.mapKey() != self.id.mapKey()) return error.PrivateIndexOwnerJobChanged;
        const result: Replay = .{ .binding = binding, .expected = self.expected, .next = self.next, .records_scanned = @intCast(self.records_scanned) };
        try result.validate();
        return result;
    }

    pub fn prepareReplay(alloc: Allocator, io: ?std.Io, core: anytype, replay: Replay) !?Page {
        try replay.validate();
        return prepareSelected(alloc, io, core, replay.binding.index, replay);
    }

    fn prepareSelected(alloc: Allocator, io: ?std.Io, core: anytype, selected: ?records.Id, replay: ?Replay) !?Page {
        if (io) |runtime| try runtime.checkCancel();
        const namespace = core.schemaNamespaceGeneration();
        // No published indexes is a valid pinned state, not an empty queue.
        // Private cancellation and dropping the last public index both need
        // retirement while this controller is absent. stage() CASes absence
        // as well as the namespace generation and durable owner preimage.
        var pinned = core.relational_indexes.acquire();
        var owned = true;
        defer if (owned) {
            if (pinned) |*value| value.deinit();
        };
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer if (owned) arena.deinit();
        const page_alloc = arena.allocator();
        var read = try core.store.beginReadTxn();
        defer read.abort();
        if (replay) |fixed| if (try fixed.applied(&read)) return null;
        const owner = try jobs.ownership(&read);
        var cursor = try read.openCursor();
        defer cursor.close();
        const Selection = struct { id: records.Id, value: []const u8 };
        const pending: Selection = if (selected) |id| blk: {
            const value = read.get(&key(id)) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            };
            break :blk .{ .id = id, .value = value };
        } else blk: {
            const entry = (try cursor.seekAtOrAfter(prefix)) orelse return null;
            if (!std.mem.startsWith(u8, entry.key, prefix)) return null;
            break :blk .{ .id = try records.Id.decode(entry.key[prefix.len..]), .value = entry.value };
        };
        const id = pending.id;
        if (pinned) |value| for (value.plan.boundIndexes()) |index| if (index.id().mapKey() == id.mapKey())
            return error.ActiveRelationalIndexRetirement;
        const expected = try page_alloc.dupe(u8, pending.value);
        if (replay) |fixed| if (!std.mem.eql(u8, fixed.expected, expected)) return error.VersionConflict;
        const progress = try decodeProgress(id, expected);
        const after = progress.after;
        const forward_prefix = try records.forwardPrefix(id);
        const ownership_prefix = try records.ownershipPrefix(id);
        var deletes = std.ArrayList([]const u8).empty;
        var next = after;
        var scanned: usize = 0;
        var bytes: usize = 0;
        var exhausted = true;
        const started = if (replay == null) time.monotonicNs() else 0;
        const expect_eof = if (replay) |fixed| if (fixed.next) |value| (try decodeProgress(id, value)).phase != progress.phase else true else false;
        const lower: []const u8 = if (progress.phase == .forward) &forward_prefix else &ownership_prefix;
        var entry = try cursor.seekAtOrAfter(if (after.len == 0) lower else after);
        while (entry) |kv| : (entry = try cursor.next()) {
            if (!std.mem.startsWith(u8, kv.key, lower)) break;
            if (after.len != 0 and std.mem.order(u8, kv.key, after) != .gt) continue;
            if (replay) |fixed| if (scanned == fixed.records_scanned) return error.InvalidRelationalIndexGcProgress;
            if (io) |runtime| try runtime.checkCancel();
            if (kv.key.len > max_cursor) return error.ResourceBudgetExceeded;
            next = try page_alloc.dupe(u8, kv.key);
            try deletes.append(page_alloc, next);
            if (progress.phase == .ownership) {
                var reverse = std.ArrayList(u8).empty;
                // Ownership is independent of tuple/value integrity. A lost
                // forward entry cannot hide its reverse from this prefix scan.
                try records.appendReverseFromOwnership(page_alloc, &reverse, kv.key);
                try deletes.append(page_alloc, reverse.items);
                bytes +|= reverse.items.len;
            }
            scanned += 1;
            bytes +|= next.len;
            if (replay) |fixed| {
                if (scanned == fixed.records_scanned and !expect_eof) {
                    exhausted = false;
                    break;
                }
                if (scanned >= 256 or bytes >= 1024 * 1024) return error.InvalidRelationalIndexGcProgress;
            } else if (scanned >= 256 or bytes >= 1024 * 1024 or time.monotonicNs() -| started >= 5 * std.time.ns_per_ms) {
                exhausted = false;
                break;
            }
        }
        const continuation = if (!exhausted) try encode(page_alloc, id, progress.phase, next) else if (progress.phase == .forward) try encode(page_alloc, id, .ownership, "") else null;
        if (replay) |fixed| {
            if (scanned != fixed.records_scanned or (continuation == null) != (fixed.next == null)) return error.InvalidRelationalIndexGcProgress;
            if (continuation) |value| if (!std.mem.eql(u8, value, fixed.next.?)) return error.InvalidRelationalIndexGcProgress;
        }
        owned = false;
        return .{ .arena = arena, .pinned = pinned, .namespace = namespace, .owner = owner, .id = id, .expected = expected, .next = continuation, .deletes = deletes.items, .records_scanned = scanned };
    }

    /// Caller owns apply-exclusive and the snapshot/hot-standby mutation leases.
    pub fn commit(self: *Page, core: anytype) !void {
        var manager = try core.initTxnManager();
        defer manager.deinit();
        try manager.checkOrdinaryWriteConflict(&maintenance.controlKey(self.id));
        var txn = try core.store.beginWriteTxn();
        errdefer txn.abort();
        try self.stage(core, &txn);
        try txn.commit();
    }

    /// Compose deletion with the caller's authority checks in one transaction.
    /// Caller owns apply-exclusive, checks maintenance intent conflicts before
    /// opening the writer, and aborts the transaction on every staging error.
    pub fn stage(self: *Page, core: anytype, txn: anytype) !void {
        if (self.consumed) return error.RelationalIndexPageConsumed;
        self.consumed = true;
        if (core.schemaNamespaceGeneration() != self.namespace or !core.relational_indexes.isCurrent(self.pinned))
            return error.PreparedGenerationChanged;
        if (!std.mem.eql(u8, &self.owner, &(try jobs.ownership(txn)))) return error.PreparedGenerationChanged;
        const record_key = key(self.id);
        const actual = txn.get(&record_key) catch |err| switch (err) {
            error.NotFound => return error.PreparedGenerationChanged,
            else => return err,
        };
        if (!std.mem.eql(u8, self.expected, actual)) return error.PreparedGenerationChanged;
        for (self.deletes) |item| try remove(txn, item);
        if (self.next) |continuation| try txn.put(&record_key, continuation) else {
            try remove(txn, &jobs.progressKey(self.id));
            try remove(txn, &maintenance.controlKey(self.id));
            try txn.delete(&record_key);
        }
    }
};

fn remove(txn: anytype, item: []const u8) !void {
    txn.delete(item) catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
}

test "relational index retirement progress binds its generation" {
    const id = records.Id{ .generation = 2, .slot = 1 };
    try std.testing.expectEqualStrings("", try decode(id, &initial(id)));
    try std.testing.expectError(error.InvalidRelationalIndexGcProgress, decode(.{ .generation = 3, .slot = 1 }, &initial(id)));
}

test "relational index retirement admission bounds churn without restricting cleanup" {
    const Fixture = struct {
        const Self = @This();
        pending: usize,
        visits: usize = 0,
        const Cursor = struct {
            fixture: *Self,
            const Entry = struct { key: []const u8 };
            const retired_key = key(.{ .generation = 1, .slot = 0 });
            pub fn close(_: *Cursor) void {}
            pub fn seekAtOrAfter(self: *Cursor, _: []const u8) !?Entry {
                return self.next();
            }
            pub fn next(self: *Cursor) !?Entry {
                if (self.fixture.visits == self.fixture.pending) return null;
                self.fixture.visits += 1;
                return .{ .key = &retired_key };
            }
        };
        pub fn openCursor(self: *@This()) !Cursor {
            return .{ .fixture = self };
        }
    };
    var full = Fixture{ .pending = max_pending_generations };
    try admitRetirements(&full, 0);
    try std.testing.expectEqual(@as(usize, 0), full.visits);
    try std.testing.expectError(error.ResourceBudgetExceeded, admitRetirements(&full, 1));
    try std.testing.expectEqual(max_pending_generations, full.visits);
    var room = Fixture{ .pending = max_pending_generations - catalog.max_index_count };
    try admitRetirements(&room, catalog.max_index_count);
    try std.testing.expectEqual(room.pending, room.visits);
    var oversized = Fixture{ .pending = 0 };
    try std.testing.expectError(error.ResourceBudgetExceeded, admitRetirements(&oversized, max_pending_generations + 1));
    try std.testing.expectEqual(@as(usize, 0), oversized.visits);
}
