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
const projection = @import("document_projection.zig");
const value_identity = @import("source_value_identity.zig");
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
    const raw = txn.get(&contentKey(key)) catch |err| {
        if (err == error.NotFound) return null;
        return err;
    };
    const value = try decodeContent(raw);
    if (value.kind != physical[physical.len - 1] or value.revision != try load(txn, key)) return null;
    return value;
}
/// Decode a bounded proof window from one sorted, deduplicated metadata read.
/// Values are reduced to owned scalars before any primary read can retire them.
pub fn loadWindow(txn: anytype, a: std.mem.Allocator, physical: []const []const u8, ids: []const []const u8, contents: []?Content, timestamps: []u64) !void {
    return loadWindowWithRevisions(txn, a, physical, ids, contents, timestamps, null);
}

pub fn loadWindowWithRevisions(txn: anytype, a: std.mem.Allocator, physical: []const []const u8, ids: []const []const u8, contents: []?Content, timestamps: []u64, mutation_revisions: ?[]u64) !void {
    if (mutation_revisions) |out| if (out.len != physical.len) return error.InvalidArgument;
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
    var key_bytes: usize = 0;
    for (ids) |id| key_bytes = try std.math.add(usize, key_bytes, @sizeOf(ContentKey) + @sizeOf(Key) + 2 + keys.encodedComponentLen(id));
    const slab = try a.alloc(u8, key_bytes);
    defer a.free(slab);
    var offset: usize = 0;
    for (physical, ids, 0..) |row, id, i| {
        const revision = keyForPhysical(row) orelse return error.InvalidArgument;
        const content = slab[offset..][0..@sizeOf(ContentKey)];
        @memcpy(content, &contentKey(revision));
        offset += content.len;
        const mutation = slab[offset..][0..@sizeOf(Key)];
        @memcpy(mutation, &revision);
        offset += mutation.len;
        const ttl = slab[offset..][0 .. 2 + keys.encodedComponentLen(id)];
        ttl[0] = keys.user_namespace;
        const end = 1 + keys.encodeComponent(ttl[1..], id);
        ttl[end] = keys.ttl_kind;
        offset += ttl.len;
        entries[i * 3] = .{ .key = content, .row = i, .kind = .content };
        entries[i * 3 + 1] = .{ .key = mutation, .row = i, .kind = .revision };
        entries[i * 3 + 2] = .{ .key = ttl, .row = i, .kind = .timestamp };
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
    if (mutation_revisions) |out| @memcpy(out, revisions[0..out.len]);
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
    const raw = txn.get(&key) catch |err| {
        if (err == error.NotFound) return 0;
        return err;
    };
    if (raw.len != 8) return error.InvalidDocumentMutationRevision;
    return std.mem.readInt(u64, raw[0..8], .little);
}

/// Foreground reads already hash a missing proof. Keep a bounded, disposable
/// repair queue rather than restarting a physical migration for each miss.
/// Drain ownership before publication under the ordinary leader/apply gates.
pub const RepairQueue = struct {
    const Entry = struct { physical: []u8, content: Content };
    pub const max_entries = 64;
    pub const max_key_bytes = 256 * 1024;
    mutex: std.atomic.Mutex = .unlocked,
    pending: std.atomic.Value(bool) = .init(false),
    entries: [max_entries]Entry = undefined,
    count: usize = 0,
    key_bytes: usize = 0,

    /// False requests the durable sweep on contention/overflow. Allocation
    /// failure is also a fallback, never a failure of the foreground query.
    pub fn enqueue(self: *@This(), a: std.mem.Allocator, physical: []const u8, content: Content) bool {
        if (!self.mutex.tryLock()) return false;
        defer self.mutex.unlock();
        for (self.entries[0..self.count]) |*entry| if (std.mem.eql(u8, entry.physical, physical)) {
            if (content.revision >= entry.content.revision) entry.content = content;
            return true;
        };
        if (self.count == max_entries or physical.len > max_key_bytes -| self.key_bytes) return false;
        const owned = a.dupe(u8, physical) catch return false;
        self.entries[self.count] = .{ .physical = owned, .content = content };
        self.count += 1;
        self.key_bytes += physical.len;
        self.pending.store(true, .release);
        return true;
    }
    pub const Page = struct {
        entries: [max_entries]Entry = undefined,
        count: usize = 0,
        pub fn stage(self: *const @This(), txn: anytype) !usize {
            var installed: usize = 0;
            for (self.entries[0..self.count]) |entry| installed += @intFromBool(try stageContent(txn, entry.physical, entry.content));
            return installed;
        }
        pub fn deinit(self: *@This(), a: std.mem.Allocator) void {
            for (self.entries[0..self.count]) |entry| a.free(entry.physical);
            self.count = 0;
        }
        pub fn retry(self: *@This(), queue: *RepairQueue, a: std.mem.Allocator) bool {
            var complete = true;
            for (self.entries[0..self.count]) |entry| complete = queue.enqueue(a, entry.physical, entry.content) and complete;
            return complete;
        }
    };
    /// Transfer owned keys under a short lock; no storage reads or gate waits
    /// hold the queue mutex. Foreground readers can enqueue the next page.
    pub fn take(self: *@This()) ?Page {
        if (!self.mutex.tryLock()) return null;
        defer self.mutex.unlock();
        var page: Page = .{ .count = self.count };
        @memcpy(page.entries[0..page.count], self.entries[0..self.count]);
        self.count = 0;
        self.key_bytes = 0;
        self.pending.store(false, .release);
        return page;
    }
    pub fn clear(self: *@This(), a: std.mem.Allocator) void {
        for (self.entries[0..self.count]) |entry| a.free(entry.physical);
        self.count = 0;
        self.key_bytes = 0;
        self.pending.store(false, .release);
    }
    pub fn deinit(self: *@This(), a: std.mem.Allocator) void {
        self.clear(a);
    }
};

fn stageContent(txn: anytype, physical: []const u8, content: Content) !bool {
    const key = keyForPhysical(physical) orelse return error.InvalidArgument;
    if (try load(txn, key) != content.revision or !try txn.contains(physical)) return false;
    if (try loadContent(txn, physical) != null) return false;
    try txn.put(&contentKey(key), &encodeContent(content));
    return true;
}

const backfill_key = "\x00\x00__metadata__:document_content_backfill_v2";

/// A disposable cache migration. Hash under a read snapshot; publish only
/// under the ordinary leader/apply gate with matching document revisions.
/// Cursor and cache records share one physical transaction; user data and
/// logical mutation revisions are never rewritten by this migration.
pub const BackfillPage = struct {
    const Entry = struct { physical: []const u8, content: Content, directory: ?[]const u8, identities: ?[]const u8 = null };
    arena: std.heap.ArenaAllocator,
    before: ?[]const u8,
    after: []const u8,
    entries: []const Entry,
    complete: bool,
    pub fn deinit(self: *@This()) void {
        self.arena.deinit();
    }
    pub fn prepare(alloc: std.mem.Allocator, store: anytype, restart: bool) !?@This() {
        return prepareWithClock(alloc, store, restart, MonotonicClock{});
    }
    const MonotonicClock = struct {
        pub fn now(_: @This()) u64 {
            return @import("antfly_platform").time.monotonicNs();
        }
    };
    /// The clock is a static dependency: production retains its bounded scan
    /// with no function-pointer overhead, while tests can control preemption.
    pub fn prepareWithClock(alloc: std.mem.Allocator, store: anytype, restart: bool, clock: anytype) !?@This() {
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
        var cursor = try read.openPhysicalKeyCursorAdapter();
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
        const deadline = clock.now() +| 2 * std.time.ns_per_ms;
        while (item) |row| {
            if (!keys.isInternalUserKey(row.key)) break;
            if (rows > 0 and (rows >= 128 or entries.items.len >= 32 or bytes >= 4 * 1024 * 1024 or clock.now() >= deadline)) break;
            if (row.key.len > 65536) return error.InvalidDocumentAuthorityCursor;
            last = try a.dupe(u8, row.key);
            // Metadata has a separate scope; its point reads cannot retire
            // the physical cursor's current primary value.
            if (keys.isStoredDocumentRowKey(row.key)) {
                var digest: [32]u8 = undefined;
                // Physical primary rows have no embedding-payload indirection.
                // Check the cache first; cursor values retain their own lease.
                const cached = try loadContent(&metadata, last);
                const needs_projection = if (cached) |proof| blk: {
                    if (proof.kind != keys.primary_kind or proof.bytes < projection.minimum_bytes) break :blk false;
                    const encoded = metadata.get(&projection.keyForRevision(keyForPhysical(last).?)) catch |err| {
                        if (err == error.NotFound) break :blk true;
                        return err;
                    };
                    if (!try projection.isCurrent(a, encoded, proof)) break :blk true;
                    const paths = (try projection.Directory.open(a, encoded, proof)) orelse break :blk false;
                    if (paths.version < 2) break :blk true;
                    const certificate = metadata.get(&value_identity.keyForRevision(keyForPhysical(last).?)) catch |err| {
                        if (err == error.NotFound) break :blk true;
                        return err;
                    };
                    break :blk value_identity.Certificate.open(certificate, encoded, proof) == null;
                } else false;
                if (cached == null or needs_projection) {
                    // Independently scoped primary bytes die after this row;
                    // only bounded directory/proof data survives to publication.
                    var primary = try read.openReadScope(alloc);
                    defer primary.close();
                    const raw = try primary.get(last);
                    const content = cached orelse blk: {
                        std.crypto.hash.sha2.Sha256.hash(raw, &digest, .{});
                        break :blk Content{ .revision = try load(&metadata, keyForPhysical(last).?), .bytes = raw.len, .kind = last[last.len - 1], .digest = digest };
                    };
                    const publication = try value_identity.buildAuthority(a, raw, content.kind, content.digest);
                    const directory = publication.directory;
                    const identities = publication.identities;
                    if (cached == null or directory != null) try entries.append(a, .{ .physical = last, .content = content, .directory = directory, .identities = identities });
                    bytes +|= raw.len;
                }
            }
            rows += 1;
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
            var changed = try stageContent(txn, entry.physical, entry.content);
            if (entry.directory) |directory| if (try loadContent(txn, entry.physical)) |current_content| {
                if (std.meta.eql(current_content, entry.content)) {
                    try txn.put(&projection.keyForRevision(keyForPhysical(entry.physical).?), directory);
                    if (entry.identities) |identities| try txn.put(&value_identity.keyForRevision(keyForPhysical(entry.physical).?), identities);
                    changed = true;
                }
            };
            installed += @intFromBool(changed);
        }
        try txn.put(backfill_key, self.after);
        return installed;
    }
};

/// One revision per affected document per transaction, including artifact-only
/// commits. Capture fixed keys rather than retaining every artifact key.
pub const Capture = struct {
    const Change = union(enum) { unchanged, deleted, written: struct { proof: Content, directory: ?[]u8, identities: ?[]u8 = null } };
    documents: std.AutoHashMapUnmanaged(Key, Change) = .empty,
    staged: bool = false,
    identity_reuse: value_identity.ReuseStats = .{},
    poisoned: bool = false,

    pub fn deinit(self: *Capture, alloc: std.mem.Allocator) void {
        var values = self.documents.valueIterator();
        while (values.next()) |value| if (value.* == .written) {
            if (value.written.directory) |directory| alloc.free(directory);
            if (value.written.identities) |identities| alloc.free(identities);
        };
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
        return self.touchValuePrepared(alloc, physical, value, null);
    }

    /// The writer's snapshot qualifies prior metadata before any primary write.
    /// Fresh/unsupported rows require no primary-body read. Repeated writes use
    /// this transaction's most recent publication rather than the older snapshot.
    pub fn touchValueWithReader(self: *Capture, alloc: std.mem.Allocator, reader: anytype, physical: []const u8, value: ?[]const u8) !void {
        try self.touch(alloc, physical);
        if (!keys.isStoredDocumentRowKey(physical) or value == null or value.?.len < projection.minimum_bytes)
            return self.touchValuePrepared(alloc, physical, value, null);
        const key = keyForPhysical(physical).?;
        const entry = self.documents.getPtr(key).?;
        if (entry.* != .unchanged) return self.touchValuePrepared(alloc, physical, value, null);
        errdefer self.poisoned = true;
        const previous_proof = loadContent(reader, physical) catch |err| switch (err) {
            error.InvalidDocumentContentAuthority, error.InvalidDocumentMutationRevision => null,
            else => return err,
        };
        const proof = previous_proof orelse return self.touchValuePrepared(alloc, physical, value, null);
        if (proof.bytes < projection.minimum_bytes) return self.touchValuePrepared(alloc, physical, value, null);
        var arena = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.reclaimingScratch(alloc));
        defer arena.deinit();
        const scratch = arena.allocator();
        const raw_directory = reader.get(&projection.keyForRevision(key)) catch |err| {
            if (err == error.NotFound) return self.touchValuePrepared(alloc, physical, value, null);
            return err;
        };
        if (raw_directory.len > projection.maximum_bytes) return self.touchValuePrepared(alloc, physical, value, null);
        if (!std.mem.startsWith(u8, raw_directory, "DPD2")) return self.touchValuePrepared(alloc, physical, value, null);
        const directory = try scratch.dupe(u8, raw_directory);
        const raw_certificate = reader.get(&value_identity.keyForRevision(key)) catch |err| {
            if (err == error.NotFound) return self.touchValuePrepared(alloc, physical, value, null);
            return err;
        };
        if (raw_certificate.len > value_identity.maximum_bytes) return self.touchValuePrepared(alloc, physical, value, null);
        return self.touchValuePrepared(alloc, physical, value, .{ .bytes = proof.bytes, .kind = proof.kind, .digest = proof.digest, .directory = directory, .identities = raw_certificate });
    }

    fn touchValuePrepared(self: *Capture, alloc: std.mem.Allocator, physical: []const u8, value: ?[]const u8, prior: ?value_identity.Prior) !void {
        try self.touch(alloc, physical);
        if (!keys.isStoredDocumentRowKey(physical)) return;
        const entry = self.documents.getPtr(keyForPhysical(physical).?).?;
        errdefer self.poisoned = true;
        const next: Change = if (value) |bytes| blk: {
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
            const kind = physical[physical.len - 1];
            const previous: ?value_identity.Prior = if (entry.* == .written and entry.written.directory != null and entry.written.identities != null)
                .{ .bytes = entry.written.proof.bytes, .kind = entry.written.proof.kind, .digest = entry.written.proof.digest, .directory = entry.written.directory.?, .identities = entry.written.identities.? }
            else
                prior;
            var reuse: value_identity.ReuseStats = .{};
            const publication = if (previous) |qualified| try value_identity.buildAuthorityReusing(alloc, bytes, kind, digest, qualified, &reuse) else try value_identity.buildAuthority(alloc, bytes, kind, digest);
            self.identity_reuse.reused_bytes +|= reuse.reused_bytes;
            self.identity_reuse.reused_leaves +|= reuse.reused_leaves;
            self.identity_reuse.canonical_bytes +|= reuse.canonical_bytes;
            break :blk .{ .written = .{ .proof = .{ .revision = 0, .bytes = bytes.len, .kind = kind, .digest = digest }, .directory = publication.directory, .identities = publication.identities } };
        } else .deleted;
        if (entry.* == .written) {
            if (entry.written.directory) |directory| alloc.free(directory);
            if (entry.written.identities) |identities| alloc.free(identities);
        }
        entry.* = next;
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
                .written => |value| value.proof,
                .deleted => null,
                .unchanged => blk: {
                    const raw = txn.get(&cached_key) catch |err| {
                        if (err == error.NotFound) break :blk null;
                        return err;
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
                txn.delete(&cached_key) catch |err| {
                    if (err != error.NotFound) return err;
                };
            }
            switch (entry.value_ptr.*) {
                .written => |value| if (value.directory) |directory| {
                    try txn.put(&projection.keyForRevision(key), directory);
                    if (value.identities) |identities| try txn.put(&value_identity.keyForRevision(key), identities);
                },
                // Small writes do not add a cache lookup/delete to the hot
                // path. A previous directory is harmless: digest/length checks
                // exclude it, and a rewrite or deletion replaces/removes it.
                .deleted => {
                    txn.delete(&projection.keyForRevision(key)) catch |err| {
                        if (err != error.NotFound) return err;
                    };
                    txn.delete(&value_identity.keyForRevision(key)) catch |err| {
                        if (err != error.NotFound) return err;
                    };
                },
                .unchanged => {},
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
            pub fn delete(self: *@This(), key: []const u8) !void {
                if (std.mem.startsWith(u8, key, content_prefix)) self.content = null;
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

test "public graph metric borrowed wire backfill skips cached bodies and publishes metadata only" {
    const a = std.testing.allocator;
    const Lite = @import("lite/docstore.zig");
    const Store = @import("docstore.zig").DocStore;
    const Counter = @import("lite/test_allocator.zig").BudgetAllocator;
    var counted: Counter = .{ .backing = a };
    const backing = counted.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/authority-key-only.aflite", .{tmp.sub_path});
    defer a.free(path);
    var lite = try Lite.Store.createWithOptions(backing, path, .{ .io = std.testing.io, .no_sync = true });
    defer lite.close();
    var store = try Store.openRuntime(backing, try lite.runtimeStore(backing));
    defer store.close();
    const key = try keys.documentKeyAlloc(a, "doc");
    defer a.free(key);
    const raw = try a.alloc(u8, 256 * 1024);
    defer a.free(raw);
    @memset(raw, 'x');
    try store.put(key, raw);
    var anchor = try lite.beginRead();
    var anchor_live = true;
    defer if (anchor_live) anchor.abort();
    const file = try anchor.readFile();
    file.test_backing_read_bytes.store(0, .monotonic);
    var warm = (try BackfillPage.prepare(backing, &store, false)).?;
    defer warm.deinit();
    try std.testing.expectEqual(@as(usize, 0), warm.entries.len);
    const warm_bytes = file.test_backing_read_bytes.load(.monotonic);
    try std.testing.expect(warm_bytes < raw.len / 4);
    anchor.abort();
    anchor_live = false;
    try store.delete(&contentKey(keyForPhysical(key).?));
    var missing = (try BackfillPage.prepare(backing, &store, false)).?;
    defer missing.deinit();
    try std.testing.expectEqual(@as(usize, 1), missing.entries.len);
    {
        var write = try store.beginWriteTxn();
        defer write.abort();
        const before = counted.live;
        try std.testing.expectEqual(@as(usize, 1), try missing.stage(&write));
        const publication_retained = counted.live - before;
        try std.testing.expect(publication_retained < raw.len / 4);
        std.debug.print("BACKFILL_KEY_ONLY primary_bytes={d} warm_read_bytes={d} publish_retained={d}\n", .{ raw.len, warm_bytes, publication_retained });
    }
    // The same physical presence operation observes pending deletes/reinserts.
    var write = try store.beginWriteTxn();
    defer write.abort();
    try std.testing.expect(try write.contains(key));
    try write.delete(key);
    try std.testing.expect(!try write.contains(key));
    try write.put(key, "new");
    try std.testing.expect(try write.contains(key));
    try std.testing.expect(!try write.contains("missing"));
}

test "public graph metric borrowed wire targeted proof repair bounds deduplicates and fences publication" {
    const a = std.testing.allocator;
    var queue: RepairQueue = .{};
    defer queue.deinit(a);
    const physical = "\x01doc:repair\x00\x00\x10";
    const content: Content = .{ .revision = 9, .bytes = 1024, .kind = 0x10, .digest = @splat(7) };
    try std.testing.expect(queue.enqueue(a, physical, content));
    var older = content;
    older.revision = 8;
    try std.testing.expect(queue.enqueue(a, physical, older));
    try std.testing.expectEqual(@as(usize, 1), queue.count);
    try std.testing.expectEqual(@as(u64, 9), queue.entries[0].content.revision);
    const Fake = struct {
        revision: [8]u8 = .{ 9, 0, 0, 0, 0, 0, 0, 0 },
        present: bool = true,
        fail: bool = false,
        writes: usize = 0,
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            if (self.fail) return error.TestPublicationFailure;
            if (std.mem.startsWith(u8, key, content_prefix)) return error.NotFound;
            return &self.revision;
        }
        pub fn contains(self: *@This(), _: []const u8) !bool {
            return self.present;
        }
        pub fn put(self: *@This(), _: []const u8, raw: []const u8) !void {
            try std.testing.expectEqual(@as(u64, 9), (try decodeContent(raw)).revision);
            self.writes += 1;
        }
    };
    var page = queue.take().?;
    defer page.deinit(a);
    try std.testing.expect(!queue.pending.load(.acquire));
    var txn: Fake = .{};
    txn.fail = true;
    try std.testing.expectError(error.TestPublicationFailure, page.stage(&txn));
    try std.testing.expect(page.retry(&queue, a));
    try std.testing.expect(queue.pending.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), queue.count);
    txn.fail = false;
    txn.revision[0] = 10;
    try std.testing.expectEqual(@as(usize, 0), try page.stage(&txn));
    txn.revision[0] = 9;
    txn.present = false;
    try std.testing.expectEqual(@as(usize, 0), try page.stage(&txn));
    txn.present = true;
    try std.testing.expectEqual(@as(usize, 1), try page.stage(&txn));
    try std.testing.expectEqual(@as(usize, 1), txn.writes);
    for (1..RepairQueue.max_entries) |i| {
        const key = try std.fmt.allocPrint(a, "key-{d}", .{i});
        defer a.free(key);
        try std.testing.expect(queue.enqueue(a, key, content));
    }
    try std.testing.expect(!queue.enqueue(a, "overflow", content));
    try std.testing.expect(queue.mutex.tryLock());
    try std.testing.expect(!queue.enqueue(a, "contended", content));
    queue.mutex.unlock();
    queue.clear(a);
    const huge = try a.alloc(u8, RepairQueue.max_key_bytes + 1);
    defer a.free(huge);
    try std.testing.expect(!queue.enqueue(a, huge, content));
    try std.testing.expect(!queue.pending.load(.acquire));
    const Fail = struct {
        fn run(allocator: std.mem.Allocator) !void {
            var q: RepairQueue = .{};
            defer q.deinit(allocator);
            if (!q.enqueue(allocator, physical, content)) return error.OutOfMemory;
            try std.testing.expect(q.pending.load(.acquire));
        }
    };
    try std.testing.checkAllAllocationFailures(a, Fail.run, .{});
}

test "public graph metric borrowed wire backfill deadline yields one row and resumes without skipping proofs" {
    const a = std.testing.allocator;
    const Lite = @import("lite/docstore.zig");
    const Store = @import("docstore.zig").DocStore;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/authority-deadline.aflite", .{tmp.sub_path});
    defer a.free(path);
    var lite = try Lite.Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true });
    defer lite.close();
    var store = try Store.openRuntime(a, try lite.runtimeStore(a));
    defer store.close();
    const ids = [_][]const u8{ "a", "b", "c" };
    for (ids) |id| {
        const key = try keys.documentKeyAlloc(a, id);
        defer a.free(key);
        try store.put(key, id);
        try store.delete(&contentKey(keyForPhysical(key).?));
    }
    const Clock = struct {
        value: u64 = 0,
        pub fn now(self: *@This()) u64 {
            const result = self.value;
            self.value += 2 * std.time.ns_per_ms;
            return result;
        }
    };
    var clock: Clock = .{};
    var pages: usize = 0;
    var installed: usize = 0;
    while (try BackfillPage.prepareWithClock(a, &store, false, &clock)) |prepared| {
        var page = prepared;
        defer page.deinit();
        try std.testing.expectEqual(@as(usize, 1), page.entries.len);
        pages += 1;
        try std.testing.expect(pages <= ids.len);
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        installed += try page.stage(&write);
        try write.commit();
    }
    try std.testing.expectEqual(ids.len, pages);
    try std.testing.expectEqual(ids.len, installed);
    var read = try store.beginReadTxn();
    defer read.abort();
    for (ids) |id| {
        const key = try keys.documentKeyAlloc(a, id);
        defer a.free(key);
        const content = (try loadContent(&read, key)).?;
        var expected: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(id, &expected, .{});
        try std.testing.expectEqualSlices(u8, &expected, &content.digest);
    }
}

test "public graph metric borrowed wire metadata window slab encodes binary identities with two allocations" {
    const Case = struct {
        const ids = [_][]const u8{ "a\x00b", "", "\xc3\xa9", "plain" };
        const Fake = struct {
            ttl: [ids.len][]const u8,
            calls: usize = 0,
            pub fn getManySorted(self: *@This(), sorted: []const []const u8, values: []?[]const u8) !void {
                self.calls += 1;
                try std.testing.expectEqual(@as(usize, ids.len * 3), sorted.len);
                var ttl_count: usize = 0;
                for (sorted, values, 0..) |key, *value, i| {
                    if (i > 0) try std.testing.expect(std.mem.order(u8, sorted[i - 1], key) == .lt);
                    if (key[0] == keys.user_namespace) {
                        const matched = for (self.ttl) |expected| {
                            if (std.mem.eql(u8, expected, key)) break true;
                        } else false;
                        try std.testing.expect(matched);
                        ttl_count += 1;
                    }
                    value.* = null;
                }
                try std.testing.expectEqual(ids.len, ttl_count);
            }
        };
        fn run(a: std.mem.Allocator) !void {
            var physical: [ids.len][]u8 = undefined;
            var ttl: [ids.len][]u8 = undefined;
            var initialized: usize = 0;
            defer for (0..initialized) |i| {
                a.free(physical[i]);
                a.free(ttl[i]);
            };
            for (ids, 0..) |id, i| {
                physical[i] = try keys.documentKeyAlloc(a, id);
                errdefer a.free(physical[i]);
                ttl[i] = try keys.ttlKeyAlloc(a, id);
                initialized += 1;
            }
            var rows: [64][]const u8 = undefined;
            var names: [64][]const u8 = undefined;
            for (&rows, &names, 0..) |*row, *name, i| {
                row.* = physical[i % ids.len];
                name.* = ids[i % ids.len];
            }
            var contents: [64]?Content = undefined;
            var timestamps: [64]u64 = undefined;
            var revisions: [64]u64 = undefined;
            var fake: Fake = .{ .ttl = ttl };
            var counted: std.testing.FailingAllocator = .init(a, .{});
            try loadWindowWithRevisions(&fake, counted.allocator(), &rows, &names, &contents, &timestamps, &revisions);
            try std.testing.expectEqual(@as(usize, 2), counted.allocations);
            try std.testing.expectEqual(counted.allocated_bytes, counted.freed_bytes);
            try std.testing.expectEqual(@as(usize, 1), fake.calls);
            for (contents, timestamps, revisions) |content, timestamp, revision| {
                try std.testing.expect(content == null);
                try std.testing.expectEqual(@as(u64, 0), timestamp);
                try std.testing.expectEqual(@as(u64, 0), revision);
            }
        }
    };
    try Case.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "projected authority backfill fences concurrent replacement and preserves logical revisions" {
    const a = std.testing.allocator;
    const Lite = @import("lite/docstore.zig");
    const Store = @import("docstore.zig").DocStore;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/projection-migration.aflite", .{tmp.sub_path});
    defer a.free(path);
    var lite = try Lite.Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true });
    defer lite.close();
    var store = try Store.openRuntime(a, try lite.runtimeStore(a));
    defer store.close();
    const physical = try keys.documentKeyAlloc(a, "doc");
    defer a.free(physical);
    const revision_key = keyForPhysical(physical).?;
    const directory_key = projection.keyForRevision(revision_key);
    const ignored = try a.alloc(u8, projection.minimum_bytes);
    defer a.free(ignored);
    @memset(ignored, 'x');
    const large = try std.fmt.allocPrint(a, "{{\"ignored\":\"{s}\",\"body\":\"old\"}}", .{ignored});
    defer a.free(large);
    try store.put(physical, large);
    try store.delete(&directory_key);
    var stale = (try BackfillPage.prepare(a, &store, true)).?;
    defer stale.deinit();
    try std.testing.expectEqual(@as(usize, 1), stale.entries.len);
    try std.testing.expect(stale.entries[0].directory != null);
    try store.put(physical, "{\"body\":\"new\"}");
    {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        try std.testing.expectEqual(@as(usize, 0), try stale.stage(&write));
        try std.testing.expectError(error.NotFound, write.get(&directory_key));
        try write.commit();
    }
    try store.put(physical, large);
    try store.delete(&directory_key);
    var current = (try BackfillPage.prepare(a, &store, true)).?;
    defer current.deinit();
    {
        var write = try store.beginWriteTxn();
        errdefer write.abort();
        const before = try load(&write, revision_key);
        try std.testing.expectEqual(@as(usize, 1), try current.stage(&write));
        try std.testing.expectEqual(before, try load(&write, revision_key));
        const proof = (try loadContent(&write, physical)).?;
        const directory = try write.get(&directory_key);
        try std.testing.expect(try projection.isCurrent(a, directory, proof));
        try write.commit();
    }
}

test "document mutation revision reuses qualified leaf identities across metadata updates and rejects stale proofs" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    // Escapes exercise the expensive typed canonicalization path as well as
    // borrowed ASCII. Added/reordered siblings must not change path identity.
    const escaped = try temp.alloc(u8, 96 * 1024);
    for (escaped, 0..) |*byte, i| byte.* = "\\u0061"[i % 6];
    const original = try std.fmt.allocPrint(temp, "{{\"payload\":{{\"image\":\"{s}\"}},\"revision\":1}}", .{escaped});
    const changed = try std.fmt.allocPrint(temp, "{{\"acl\":\"new\",\"payload\":{{\"extra\":true,\"image\":\"{s}\"}},\"revision\":2}}", .{escaped});
    const physical = try keys.documentKeyAlloc(temp, "reuse");
    var capture: Capture = .{};
    defer capture.deinit(a);
    try capture.touchValue(a, physical, original);
    const reference_digest = blk: {
        var result: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(changed, &result, .{});
        break :blk result;
    };
    const reference = try value_identity.buildAuthority(temp, changed, keys.primary_kind, reference_digest);
    try capture.touchValue(a, physical, changed);
    const updated = capture.documents.get(keyForPhysical(physical).?).?.written;
    try std.testing.expectEqualSlices(u8, reference.directory.?, updated.directory.?);
    try std.testing.expectEqualSlices(u8, reference.identities.?, updated.identities.?);
    try std.testing.expect(capture.identity_reuse.reused_bytes >= escaped.len);
    try std.testing.expect(capture.identity_reuse.canonical_bytes < 128);
    // Same payload at a different path cannot reuse the old path's proof.
    const renamed = try std.fmt.allocPrint(temp, "{{\"payload\":{{\"other\":\"{s}\"}},\"revision\":3}}", .{escaped});
    capture.identity_reuse = .{};
    try capture.touchValue(a, physical, renamed);
    try std.testing.expectEqual(@as(usize, 0), capture.identity_reuse.reused_bytes);
    // A corrupt/unknown canonical ABI must force reference canonicalization.
    const current = capture.documents.get(keyForPhysical(physical).?).?.written;
    const stale = try temp.dupe(u8, current.identities.?);
    stale[79] ^= 1;
    var stats: value_identity.ReuseStats = .{};
    const rejected = try value_identity.buildAuthorityReusing(temp, renamed, keys.primary_kind, current.proof.digest, .{ .bytes = current.proof.bytes, .kind = current.proof.kind, .digest = current.proof.digest, .directory = current.directory.?, .identities = stale }, &stats);
    try std.testing.expectEqualSlices(u8, current.identities.?, rejected.identities.?);
    try std.testing.expectEqual(@as(usize, 0), stats.reused_bytes);
    // Real writer integration: snapshot-qualified metadata, repeated writes,
    // committed revisions and ordinary conditional identity readers agree.
    var backend = @import("lsm_backend.zig").Backend.init(a, .{ .flush_threshold = 1024 * 1024 });
    defer backend.close();
    var store = try @import("docstore.zig").DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    try store.put(physical, original);
    var write = try store.beginWriteTxn();
    var committed = false;
    defer if (!committed) write.abort();
    try write.put(physical, changed);
    try std.testing.expect(write.document_revisions.identity_reuse.reused_bytes >= escaped.len);
    try write.put(physical, renamed);
    try write.commit();
    committed = true;
    var read = try store.beginReadTxn();
    defer read.abort();
    const proof = (try loadContent(&read, physical)).?;
    try std.testing.expectEqual(@as(u64, 2), proof.revision);
    const encoded = try read.get(&value_identity.keyForRevision(keyForPhysical(physical).?));
    const expected = capture.documents.get(keyForPhysical(physical).?).?.written;
    try std.testing.expectEqualSlices(u8, expected.identities.?, encoded);
}

test "document mutation revision reused publications release every allocation on failure" {
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            const physical = try keys.documentKeyAlloc(a, "reuse-failure");
            defer a.free(physical);
            const first = "{\"large\":\"" ++ @as([16 * 1024]u8, @splat('x')) ++ "\",\"revision\":1}";
            const second = "{\"large\":\"" ++ @as([16 * 1024]u8, @splat('x')) ++ "\",\"revision\":2}";
            var capture: Capture = .{};
            defer capture.deinit(a);
            try capture.touchValue(a, physical, first);
            try capture.touchValue(a, physical, second);
            try std.testing.expect(capture.identity_reuse.reused_bytes >= 16 * 1024);
        }
    };
    try Case.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}
