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
const internal = @import("../internal_keys.zig");
const Allocator = std.mem.Allocator;
pub const prefix = "\x00\x00__metadata__:relational_index_retired:";
const max_cursor = 1024 * 1024;

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

pub fn initial(id: records.Id) [40]u8 {
    var value: [40]u8 = undefined;
    @memcpy(value[0..8], "ARGC\x01\x00\x00\x00");
    @memcpy(value[8..], &checksum(id, value[0..8]));
    return value;
}

fn encode(alloc: Allocator, id: records.Id, after: []const u8) ![]u8 {
    if (after.len > max_cursor) return error.InvalidRelationalIndexGcProgress;
    const value = try alloc.alloc(u8, 40 + after.len);
    @memcpy(value[0..8], "ARGC\x01\x00\x00\x00");
    @memcpy(value[8..][0..after.len], after);
    @memcpy(value[value.len - 32 ..], &checksum(id, value[0 .. value.len - 32]));
    return value;
}

pub fn decode(id: records.Id, value: []const u8) ![]const u8 {
    if (value.len < 40 or value.len > 40 + max_cursor or !std.mem.eql(u8, value[0..8], "ARGC\x01\x00\x00\x00") or
        !std.mem.eql(u8, value[value.len - 32 ..], &checksum(id, value[0 .. value.len - 32])))
        return error.InvalidRelationalIndexGcProgress;
    const after = value[8 .. value.len - 32];
    if (after.len != 0) {
        if (!std.mem.startsWith(u8, after, &(try records.forwardPrefix(id)))) return error.InvalidRelationalIndexGcProgress;
        _ = try records.forwardOwnership(after);
    }
    return after;
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
        const after = try decode(id, expected);
        const forward_prefix = try records.forwardPrefix(id);
        var deletes = std.ArrayList([]const u8).empty;
        var next = after;
        var scanned: usize = 0;
        var bytes: usize = 0;
        var exhausted = true;
        const started = time.monotonicNs();
        var entry = try cursor.seekAtOrAfter(if (after.len == 0) &forward_prefix else after);
        while (entry) |kv| : (entry = try cursor.next()) {
            if (!std.mem.startsWith(u8, kv.key, &forward_prefix)) break;
            if (after.len != 0 and std.mem.order(u8, kv.key, after) != .gt) continue;
            if (io) |runtime| try runtime.checkCancel();
            const forward = try records.forwardOwnership(kv.key);
            if (kv.value.len != 0) return error.InvalidRelationalIndexForwardValue;
            next = try page_alloc.dupe(u8, kv.key);
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
            try deletes.append(page_alloc, next);
            scanned += 1;
            bytes +|= next.len + reverse.len;
            if (scanned >= 256 or bytes >= 1024 * 1024 or time.monotonicNs() -| started >= 5 * std.time.ns_per_ms) {
                exhausted = false;
                break;
            }
        }
        const continuation = if (exhausted) null else try encode(page_alloc, id, next);
        owned = false;
        return .{ .arena = arena, .pinned = pinned, .namespace = namespace, .owner = owner, .id = id, .expected = expected, .next = continuation, .deletes = deletes.items };
    }

    /// Caller owns apply-exclusive and the snapshot/HA mutation leases.
    pub fn commit(self: *Page, core: anytype) !void {
        if (self.consumed) return error.RelationalIndexPageConsumed;
        self.consumed = true;
        if (core.schemaNamespaceGeneration() != self.namespace or !core.relational_indexes.isCurrent(self.pinned))
            return error.PreparedGenerationChanged;
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
