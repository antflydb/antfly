// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

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
const internal = @import("../internal_keys.zig");
const Allocator = std.mem.Allocator;
pub const prefix = "\x00\x00__metadata__:relational_index_retired:";
const max_cursor = 1024 * 1024;
pub const max_pending_generations: usize = 2 * catalog.max_index_count;

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

const Phase = enum(u8) { forward, reverse };
const Progress = struct { phase: Phase, after: []const u8 };

pub fn initial(id: records.Id) [40]u8 {
    var value: [40]u8 = undefined;
    @memcpy(value[0..8], "ARGC\x02\x00\x00\x00");
    @memcpy(value[8..], &checksum(id, value[0..8]));
    return value;
}

fn encode(alloc: Allocator, id: records.Id, phase: Phase, after: []const u8) ![]u8 {
    if (after.len > max_cursor) return error.InvalidRelationalIndexGcProgress;
    const value = try alloc.alloc(u8, 40 + after.len);
    @memcpy(value[0..8], "ARGC\x02\x00\x00\x00");
    value[5] = @intFromEnum(phase);
    @memcpy(value[8..][0..after.len], after);
    @memcpy(value[value.len - 32 ..], &checksum(id, value[0 .. value.len - 32]));
    return value;
}

pub fn decode(id: records.Id, value: []const u8) ![]const u8 {
    return (try decodeProgress(id, value)).after;
}

fn decodeProgress(id: records.Id, value: []const u8) !Progress {
    if (value.len < 40 or value.len > 40 + max_cursor or !std.mem.eql(u8, value[0..5], "ARGC\x02") or value[6] != 0 or value[7] != 0 or
        !std.mem.eql(u8, value[value.len - 32 ..], &checksum(id, value[0 .. value.len - 32])))
        return error.InvalidRelationalIndexGcProgress;
    const phase = std.enums.fromInt(Phase, value[5]) orelse return error.InvalidRelationalIndexGcProgress;
    const after = value[8 .. value.len - 32];
    if (after.len != 0) {
        if (phase == .forward) {
            if (!std.mem.startsWith(u8, after, &(try records.forwardPrefix(id)))) return error.InvalidRelationalIndexGcProgress;
        } else if (after[0] != internal.user_namespace) return error.InvalidRelationalIndexGcProgress;
    }
    return .{ .phase = phase, .after = after };
}

pub const Page = struct {
    arena: std.heap.ArenaAllocator,
    pinned: catalog.WriteSnapshot,
    namespace: u64,
    owner: [32]u8,
    id: records.Id,
    expected: []const u8,
    next: ?[]const u8,
    deletes: []const []const u8,
    consumed: bool = false,

    pub fn deinit(self: *Page) void {
        self.pinned.deinit();
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn prepare(alloc: Allocator, io: ?std.Io, core: anytype) !?Page {
        if (io) |runtime| try runtime.checkCancel();
        const namespace = core.schemaNamespaceGeneration();
        var pinned = core.relational_indexes.acquire() orelse return null;
        var owned = true;
        defer if (owned) pinned.deinit();
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer if (owned) arena.deinit();
        const page_alloc = arena.allocator();
        var read = try core.store.beginReadTxn();
        defer read.abort();
        const owner = try jobs.ownership(&read);
        var cursor = try read.openCursor();
        defer cursor.close();
        const pending = (try cursor.seekAtOrAfter(prefix)) orelse return null;
        if (!std.mem.startsWith(u8, pending.key, prefix)) return null;
        const id = try records.Id.decode(pending.key[prefix.len..]);
        for (pinned.plan.boundIndexes()) |index| if (index.id().mapKey() == id.mapKey())
            return error.ActiveRelationalIndexRetirement;
        const expected = try page_alloc.dupe(u8, pending.value);
        const progress = try decodeProgress(id, expected);
        const after = progress.after;
        const forward_prefix = try records.forwardPrefix(id);
        var deletes = std.ArrayList([]const u8).empty;
        var next = after;
        var scanned: usize = 0;
        var bytes: usize = 0;
        var exhausted = true;
        const started = time.monotonicNs();
        const lower: []const u8 = if (progress.phase == .forward) &forward_prefix else &.{internal.user_namespace};
        var entry = try cursor.seekAtOrAfter(if (after.len == 0) lower else after);
        while (entry) |kv| : (entry = try cursor.next()) {
            if (!std.mem.startsWith(u8, kv.key, lower)) break;
            if (after.len != 0 and std.mem.order(u8, kv.key, after) != .gt) continue;
            if (io) |runtime| try runtime.checkCancel();
            if (kv.key.len > max_cursor) return error.ResourceBudgetExceeded;
            next = try page_alloc.dupe(u8, kv.key);
            if (progress.phase == .forward) {
                try deletes.append(page_alloc, next);
                if (records.forwardOwnership(kv.key)) |forward| {
                    const reverse = try page_alloc.alloc(u8, 1 + forward.document_component.len + 1 + records.Id.encoded_len);
                    reverse[0] = internal.user_namespace;
                    @memcpy(reverse[1..][0..forward.document_component.len], forward.document_component);
                    reverse[reverse.len - records.Id.encoded_len - 1] = internal.relational_index_reverse_kind;
                    @memcpy(reverse[reverse.len - records.Id.encoded_len ..], &id.encode());
                    // Retirement authorizes deletion of the entire generation, not
                    // conditional replacement of a tuple. Its validated key ownership
                    // is sufficient: avoid a cold reverse point-read per entry and do
                    // not make a corrupt retired value prevent its own deletion.
                    try deletes.append(page_alloc, reverse);
                    bytes +|= reverse.len;
                } else |_| {} // The retired prefix authorizes malformed derived keys too.
            } else if (records.parseReverseKey(kv.key)) |reverse| {
                if (reverse.id.mapKey() == id.mapKey()) try deletes.append(page_alloc, next);
            } else |_| {}
            scanned += 1;
            bytes +|= next.len;
            if (scanned >= 256 or bytes >= 1024 * 1024 or time.monotonicNs() -| started >= 5 * std.time.ns_per_ms) {
                exhausted = false;
                break;
            }
        }
        const continuation = if (!exhausted) try encode(page_alloc, id, progress.phase, next) else if (progress.phase == .forward) try encode(page_alloc, id, .reverse, "") else null;
        owned = false;
        return .{ .arena = arena, .pinned = pinned, .namespace = namespace, .owner = owner, .id = id, .expected = expected, .next = continuation, .deletes = deletes.items };
    }

    /// Caller owns apply-exclusive and the snapshot/HA mutation leases.
    pub fn commit(self: *Page, core: anytype) !void {
        if (self.consumed) return error.RelationalIndexPageConsumed;
        self.consumed = true;
        if (core.schemaNamespaceGeneration() != self.namespace or !core.relational_indexes.isCurrent(self.pinned))
            return error.PreparedGenerationChanged;
        var manager = try core.initTxnManager();
        defer manager.deinit();
        try manager.checkOrdinaryWriteConflict(&maintenance.controlKey(self.id));
        var txn = try core.store.beginWriteTxn();
        errdefer txn.abort();
        if (!std.mem.eql(u8, &self.owner, &(try jobs.ownership(&txn)))) return error.PreparedGenerationChanged;
        const record_key = key(self.id);
        const actual = txn.get(&record_key) catch |err| switch (err) {
            error.NotFound => return error.PreparedGenerationChanged,
            else => return err,
        };
        if (!std.mem.eql(u8, self.expected, actual)) return error.PreparedGenerationChanged;
        for (self.deletes) |item| try remove(&txn, item);
        if (self.next) |continuation| try txn.put(&record_key, continuation) else {
            try remove(&txn, &jobs.progressKey(self.id));
            try remove(&txn, &maintenance.controlKey(self.id));
            try txn.delete(&record_key);
        }
        try txn.commit();
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
