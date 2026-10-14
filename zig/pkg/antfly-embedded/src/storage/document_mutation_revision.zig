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

//! Optimistic read fences for a document and its derived physical state.
//! Revisions advance atomically with mutations, independently of TTL clocks.
//! Keep tombstones after deletion to reject delete/reinsert ABA.
const std = @import("std");
const keys = @import("internal_keys.zig");
const prefix = "\x00\x00__metadata__:document_mutation_revision_v1:";
pub const Key = [prefix.len + 32]u8;
const content_prefix = "\x00\x00__metadata__:document_content_authority_v1:";
pub const ContentKey = [content_prefix.len + 32]u8;
pub const Content = struct { revision: u64, bytes: u64, kind: u8, digest: [32]u8 };
pub fn contentKey(key: Key) ContentKey {
    var result: ContentKey = undefined;
    @memcpy(result[0..content_prefix.len], content_prefix);
    @memcpy(result[content_prefix.len..], key[prefix.len..]);
    return result;
}
fn decodeContent(raw: []const u8) !Content {
    if (raw.len != 49) return error.InvalidDocumentContentAuthority;
    return .{ .revision = std.mem.readInt(u64, raw[0..8], .little), .bytes = std.mem.readInt(u64, raw[8..16], .little), .kind = raw[16], .digest = raw[17..49].* };
}
/// The existing mutation revision is also the compatibility fence. Older
/// writers advance it without refreshing this cache, forcing a primary read.
pub fn loadContent(txn: anytype, physical: []const u8) !?Content {
    if (!keys.isStoredDocumentRowKey(physical)) return null;
    const key = keyForPhysical(physical).?;
    const raw = txn.get(&contentKey(key)) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    const value = try decodeContent(raw);
    if (value.kind != physical[physical.len - 1] or value.revision != try load(txn, key)) return null;
    return value;
}
/// Decode a bounded proof window from one sorted, deduplicated metadata read.
/// Values are reduced to owned scalars before any primary read can retire them.
pub fn loadWindow(txn: anytype, a: std.mem.Allocator, physical: []const []const u8, ids: []const []const u8, contents: []?Content, timestamps: []u64) !void {
    if (physical.len != ids.len or contents.len != ids.len or timestamps.len != ids.len or ids.len > 64) return error.InvalidArgument;
    const Entry = struct {
        key: []const u8,
        row: usize,
        kind: enum { content, revision, timestamp },
        value_index: usize = 0,
        fn less(_: void, left: @This(), right: @This()) bool {
            return std.mem.order(u8, left.key, right.key) == .lt;
        }
    };
    const entries = try a.alloc(Entry, ids.len * 3);
    defer a.free(entries);
    var initialized: usize = 0;
    defer for (entries[0..initialized]) |entry| a.free(entry.key);
    for (physical, ids, 0..) |row, id, i| {
        const revision = keyForPhysical(row) orelse return error.InvalidArgument;
        entries[initialized] = .{ .key = try a.dupe(u8, &contentKey(revision)), .row = i, .kind = .content };
        initialized += 1;
        entries[initialized] = .{ .key = try a.dupe(u8, &revision), .row = i, .kind = .revision };
        initialized += 1;
        entries[initialized] = .{ .key = try keys.ttlKeyAlloc(a, id), .row = i, .kind = .timestamp };
        initialized += 1;
    }
    std.mem.sort(Entry, entries, {}, Entry.less);
    var sorted: [192][]const u8 = undefined;
    var values: [192]?[]const u8 = undefined;
    var count: usize = 0;
    for (entries) |*entry| {
        if (count == 0 or !std.mem.eql(u8, sorted[count - 1], entry.key)) {
            sorted[count] = entry.key;
            count += 1;
        }
        entry.value_index = count - 1;
    }
    try txn.getManySorted(sorted[0..count], values[0..count]);
    @memset(contents, null);
    @memset(timestamps, 0);
    var revisions: [64]u64 = @splat(0);
    for (entries) |entry| if (values[entry.value_index]) |raw| switch (entry.kind) {
        .content => contents[entry.row] = try decodeContent(raw),
        .revision => {
            if (raw.len != 8) return error.InvalidDocumentMutationRevision;
            revisions[entry.row] = std.mem.readInt(u64, raw[0..8], .little);
        },
        .timestamp => {
            if (raw.len != 8) return error.InvalidTimestamp;
            timestamps[entry.row] = std.mem.readInt(u64, raw[0..8], .little);
        },
    };
    for (contents, physical, 0..) |*content, row, i| if (content.*) |cached| {
        if (!keys.isStoredDocumentRowKey(row) or cached.kind != row[row.len - 1] or cached.revision != revisions[i]) content.* = null;
    };
}

fn encodeContent(value: Content) [49]u8 {
    var raw: [49]u8 = undefined;
    std.mem.writeInt(u64, raw[0..8], value.revision, .little);
    std.mem.writeInt(u64, raw[8..16], value.bytes, .little);
    raw[16] = value.kind;
    @memcpy(raw[17..49], &value.digest);
    return raw;
}

fn keyForPrefix(document_prefix: []const u8) Key {
    var result: Key = undefined;
    @memcpy(result[0..prefix.len], prefix);
    std.crypto.hash.sha2.Sha256.hash(document_prefix, result[prefix.len..], .{});
    return result;
}

pub fn keyForPhysical(candidate: []const u8) ?Key {
    if (!keys.isInternalUserKey(candidate)) return null;
    const end = (keys.findComponentTerminator(candidate, 1) orelse return null) + 2;
    return keyForPrefix(candidate[0..end]);
}

pub fn keyForDocumentAlloc(alloc: std.mem.Allocator, document: []const u8) !Key {
    const physical_prefix = try keys.documentExactPrefixAlloc(alloc, document);
    defer alloc.free(physical_prefix);
    return keyForPrefix(physical_prefix);
}

pub fn load(txn: anytype, key: Key) !u64 {
    const raw = txn.get(&key) catch |err| switch (err) {
        error.NotFound => return 0,
        else => return err,
    };
    if (raw.len != 8) return error.InvalidDocumentMutationRevision;
    return std.mem.readInt(u64, raw[0..8], .little);
}

const backfill_key = "\x00\x00__metadata__:document_content_backfill_v1";

/// A disposable cache migration. Hash under a read snapshot; publish only
/// under the ordinary leader/apply gate with matching document revisions.
/// Cursor and cache records share one physical transaction; user data and
/// logical mutation revisions are never rewritten by this migration.
pub const BackfillPage = struct {
    const Entry = struct { physical: []const u8, content: Content };
    arena: std.heap.ArenaAllocator,
    before: ?[]const u8,
    after: []const u8,
    entries: []const Entry,
    complete: bool,
    pub fn deinit(self: *@This()) void {
        self.arena.deinit();
    }
    pub fn prepare(alloc: std.mem.Allocator, store: anytype, restart: bool) !?@This() {
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const a = arena.allocator();
        var read = try store.beginReadTxnWithBlockCacheAdmission(.transient);
        defer read.abort();
        const before: ?[]const u8 = before: {
            const raw = read.get(backfill_key) catch |err| switch (err) {
                error.NotFound => break :before null,
                else => return err,
            };
            if (raw.len == 0 or raw.len > 65537 or raw[0] > 1) return error.InvalidDocumentAuthorityCursor;
            break :before try a.dupe(u8, raw);
        };
        if (!restart) if (before) |state| if (state[0] == 1) {
            arena.deinit();
            return null;
        };
        var metadata = try read.openReadScope(a);
        defer metadata.close();
        var cursor = try read.openPhysicalCursorAdapter();
        defer cursor.close();
        const previous = if (!restart and before != null) before.?[1..] else &.{keys.user_namespace};
        var item = try cursor.seekAtOrAfter(previous);
        if (!restart and before != null) if (item) |row| if (std.mem.eql(u8, previous, row.key)) {
            item = try cursor.next();
        };
        var entries = std.ArrayListUnmanaged(Entry).empty;
        var last: []const u8 = previous;
        var rows: usize = 0;
        var bytes: usize = 0;
        const deadline = @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_ms;
        while (item) |row| {
            if (!keys.isInternalUserKey(row.key)) break;
            if (rows > 0 and (rows >= 128 or entries.items.len >= 32 or bytes >= 4 * 1024 * 1024 or row.value.len > 4 * 1024 * 1024 -| bytes or @import("antfly_platform").time.monotonicNs() >= deadline)) break;
            if (row.key.len > 65536) return error.InvalidDocumentAuthorityCursor;
            last = try a.dupe(u8, row.key);
            // Metadata has a separate scope; its point reads cannot retire
            // the physical cursor's current primary value.
            if (keys.isStoredDocumentRowKey(row.key)) {
                var digest: [32]u8 = undefined;
                // Physical primary rows have no embedding-payload indirection.
                // Check the cache first; cursor values retain their own lease.
                if (try loadContent(&metadata, last) == null) {
                    std.crypto.hash.sha2.Sha256.hash(row.value, &digest, .{});
                    const revision = try load(&metadata, keyForPhysical(last).?);
                    try entries.append(a, .{ .physical = last, .content = .{ .revision = revision, .bytes = row.value.len, .kind = last[last.len - 1], .digest = digest } });
                }
            }
            rows += 1;
            bytes +|= row.value.len;
            item = try cursor.next();
        }
        const complete = item == null or !keys.isInternalUserKey(item.?.key);
        const after = try a.alloc(u8, 1 + last.len);
        after[0] = if (complete) 1 else 0;
        @memcpy(after[1..], last);
        return .{ .arena = arena, .before = before, .after = after, .entries = entries.items, .complete = complete };
    }
    pub fn stage(self: *const @This(), txn: anytype) !usize {
        const current = txn.get(backfill_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if ((current == null) != (self.before == null) or (current != null and !std.mem.eql(u8, current.?, self.before.?))) return error.DocumentAuthorityCursorChanged;
        var installed: usize = 0;
        for (self.entries) |entry| {
            const key = keyForPhysical(entry.physical).?;
            if (try load(txn, key) != entry.content.revision or !try txn.contains(entry.physical)) continue;
            if (try loadContent(txn, entry.physical) != null) continue;
            try txn.put(&contentKey(key), &encodeContent(entry.content));
            installed += 1;
        }
        try txn.put(backfill_key, self.after);
        return installed;
    }
};

/// One revision per affected document per transaction, including artifact-only
/// commits. Capture fixed keys rather than retaining every artifact key.
pub const Capture = struct {
    const Change = union(enum) { unchanged, deleted, written: Content };
    documents: std.AutoHashMapUnmanaged(Key, Change) = .empty,
    staged: bool = false,
    poisoned: bool = false,

    pub fn deinit(self: *Capture, alloc: std.mem.Allocator) void {
        self.documents.deinit(alloc);
    }

    pub fn touch(self: *Capture, alloc: std.mem.Allocator, physical_key: []const u8) !void {
        const key = keyForPhysical(physical_key) orelse return;
        if (self.staged or self.poisoned) return error.RetainedEffectsTransactionFailed;
        errdefer self.poisoned = true;
        const entry = try self.documents.getOrPut(alloc, key);
        if (!entry.found_existing) entry.value_ptr.* = .unchanged;
    }

    /// Hash primary bytes once at the common physical writer boundary. The
    /// fixed map entry coalesces repeated writes and retains no document body.
    pub fn touchValue(self: *Capture, alloc: std.mem.Allocator, physical: []const u8, value: ?[]const u8) !void {
        try self.touch(alloc, physical);
        if (!keys.isStoredDocumentRowKey(physical)) return;
        const entry = self.documents.getPtr(keyForPhysical(physical).?).?;
        if (value) |bytes| {
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
            entry.* = .{ .written = .{ .revision = 0, .bytes = bytes.len, .kind = physical[physical.len - 1], .digest = digest } };
        } else entry.* = .deleted;
    }

    pub fn stage(self: *Capture, txn: anytype) !void {
        if (self.poisoned) return error.RetainedEffectsTransactionFailed;
        if (self.staged) return;
        errdefer self.poisoned = true;
        var iter = self.documents.iterator();
        while (iter.next()) |entry| {
            const key = entry.key_ptr.*;
            const old = try load(txn, key);
            const next = std.math.add(u64, old, 1) catch return error.DocumentMutationRevisionExhausted;
            const cached_key = contentKey(key);
            var cached: ?Content = switch (entry.value_ptr.*) {
                .written => |value| value,
                .deleted => null,
                .unchanged => blk: {
                    const raw = txn.get(&cached_key) catch |err| switch (err) {
                        error.NotFound => break :blk null,
                        else => return err,
                    };
                    const value = try decodeContent(raw);
                    break :blk if (value.revision == old) value else null;
                },
            };
            // Update before publishing the revision; both share the commit.
            // Artifact/TTL-only changes can retain a previously proven digest.
            if (cached) |*value| {
                value.revision = next;
                try txn.put(&cached_key, &encodeContent(value.*));
            } else if (entry.value_ptr.* == .deleted) {
                txn.delete(&cached_key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
            }
            var raw: [8]u8 = undefined;
            std.mem.writeInt(u64, &raw, next, .little);
            try txn.put(&key, &raw);
        }
        self.staged = true;
    }
};

test "document mutation revision capture owns allocation failures and coalesces owners" {
    const Case = struct {
        const Fake = struct {
            writes: usize = 0,
            revision: [8]u8 = .{ 7, 0, 0, 0, 0, 0, 0, 0 },
            pub fn get(self: *@This(), key: []const u8) ![]const u8 {
                if (std.mem.startsWith(u8, key, content_prefix)) return error.NotFound;
                return &self.revision;
            }
            pub fn delete(_: *@This(), _: []const u8) !void {
                return error.NotFound;
            }
            pub fn put(self: *@This(), _: []const u8, raw: []const u8) !void {
                try std.testing.expectEqual(@as(u64, 8), std.mem.readInt(u64, raw[0..8], .little));
                self.writes += 1;
            }
        };
        fn run(alloc: std.mem.Allocator) !void {
            var capture = Capture{};
            defer capture.deinit(alloc);
            var txn = Fake{};
            for ([_][]const u8{ "\x01doc:a\x00\x00\x10", "\x01doc:a\x00\x00\x20asset", "\x01doc:b\x00\x00\x10" }) |physical| {
                capture.touch(alloc, physical) catch |err| {
                    try std.testing.expectError(error.RetainedEffectsTransactionFailed, capture.stage(&txn));
                    try std.testing.expectEqual(@as(usize, 0), txn.writes);
                    return err;
                };
            }
            try capture.stage(&txn);
            try capture.stage(&txn);
            try std.testing.expectEqual(@as(usize, 2), txn.writes);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "public graph metric borrowed wire cached content authority fences legacy writes and coalesces primary bytes" {
    const Case = struct {
        const Fake = struct {
            revision: [8]u8 = @splat(0),
            content: ?[49]u8 = null,
            read_bytes: usize = 0,
            pub fn get(self: *@This(), key: []const u8) ![]const u8 {
                if (std.mem.startsWith(u8, key, content_prefix)) {
                    if (self.content == null) return error.NotFound;
                    self.read_bytes += 49;
                    return &self.content.?;
                }
                if (!std.mem.startsWith(u8, key, prefix)) return error.TestUnexpectedResult;
                self.read_bytes += 8;
                return &self.revision;
            }
            pub fn put(self: *@This(), key: []const u8, raw: []const u8) !void {
                if (std.mem.startsWith(u8, key, content_prefix)) self.content = raw[0..49].* else self.revision = raw[0..8].*;
            }
            pub fn delete(self: *@This(), _: []const u8) !void {
                self.content = null;
            }
        };
        fn run(a: std.mem.Allocator) !void {
            const physical = try keys.documentKeyAlloc(a, "doc\x00binary");
            defer a.free(physical);
            var fake = Fake{};
            {
                var capture = Capture{};
                defer capture.deinit(a);
                try capture.touchValue(a, physical, "old");
                try capture.touchValue(a, physical, "last bytes");
                try capture.stage(&fake);
            }
            fake.read_bytes = 0;
            const cached = (try loadContent(&fake, physical)).?;
            try std.testing.expectEqual(@as(u64, 10), cached.bytes);
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash("last bytes", &digest, .{});
            try std.testing.expectEqualSlices(u8, &digest, &cached.digest);
            try std.testing.expectEqual(@as(usize, 57), fake.read_bytes);
            // TTL/artifact-only revisions preserve content without hashing it.
            {
                var capture = Capture{};
                defer capture.deinit(a);
                try capture.touch(a, physical);
                try capture.stage(&fake);
            }
            try std.testing.expectEqual(@as(u64, 2), (try loadContent(&fake, physical)).?.revision);
            // An older writer advances the revision without our cache update.
            std.mem.writeInt(u64, &fake.revision, 3, .little);
            try std.testing.expect((try loadContent(&fake, physical)) == null);
            {
                var capture = Capture{};
                defer capture.deinit(a);
                try capture.touch(a, physical);
                try capture.stage(&fake);
            }
            try std.testing.expect((try loadContent(&fake, physical)) == null);
            {
                var capture = Capture{};
                defer capture.deinit(a);
                try capture.touchValue(a, physical, null);
                try capture.stage(&fake);
            }
            try std.testing.expect(fake.content == null);
            try std.testing.expectEqual(@as(u64, 5), try load(&fake, keyForPhysical(physical).?));
        }
    };
    try Case.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
    std.debug.print("CONTENT_AUTHORITY cached_read_bytes=57 primary_reads=0 query_hash_bytes=0\n", .{});
}

test "public graph metric borrowed wire metadata window batches deduplicates and fences stale content" {
    const Case = struct {
        const Fake = struct {
            calls: usize = 0,
            count: usize = 0,
            revision: [8]u8 = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
            timestamp: [8]u8 = .{ 7, 0, 0, 0, 0, 0, 0, 0 },
            content: [49]u8 = encodeContent(.{ .revision = 1, .bytes = 16384, .kind = 0, .digest = @splat(3) }),
            pub fn getManySorted(self: *@This(), sorted: []const []const u8, values: []?[]const u8) !void {
                self.calls += 1;
                self.count = sorted.len;
                for (sorted, values, 0..) |key, *value, i| {
                    if (i > 0) try std.testing.expect(std.mem.order(u8, sorted[i - 1], key) == .lt);
                    value.* = if (std.mem.startsWith(u8, key, content_prefix)) &self.content else if (std.mem.startsWith(u8, key, prefix)) &self.revision else &self.timestamp;
                }
            }
        };
        fn run(a: std.mem.Allocator) !void {
            const physical = try keys.documentKeyAlloc(a, "doc");
            defer a.free(physical);
            var fake = Fake{};
            // Use the real primary suffix rather than assuming its encoding.
            fake.content[16] = physical[physical.len - 1];
            const rows: [64][]const u8 = @splat(physical);
            const ids: [64][]const u8 = @splat("doc");
            var contents: [64]?Content = undefined;
            var timestamps: [64]u64 = undefined;
            try loadWindow(&fake, a, &rows, &ids, &contents, &timestamps);
            try std.testing.expectEqual(@as(usize, 1), fake.calls);
            try std.testing.expectEqual(@as(usize, 3), fake.count);
            for (contents, timestamps) |content, timestamp| {
                try std.testing.expectEqual(@as(u64, 16384), content.?.bytes);
                try std.testing.expectEqual(@as(u64, 7), timestamp);
            }
            fake.revision[0] = 2;
            try loadWindow(&fake, a, &rows, &ids, &contents, &timestamps);
            for (contents) |content| try std.testing.expect(content == null);
        }
    };
    try Case.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
    std.debug.print("AUTHORITY_WINDOW rows=64 point_calls_before=192 batch_calls_after=1 unique_keys=3\n", .{});
}
