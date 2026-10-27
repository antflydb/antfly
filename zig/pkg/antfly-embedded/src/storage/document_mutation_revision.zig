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
const logical = @import("document_commitment.zig");
pub const Algorithm = logical.Algorithm;
const prefix = "\x00\x00__metadata__:document_mutation_revision_v1:";
pub const Key = [prefix.len + 32]u8;
const content_prefix = "\x00\x00__metadata__:document_content_authority_v1:";
pub const ContentKey = [content_prefix.len + 32]u8;
pub const Content = struct { revision: u64, bytes: u64, kind: u8, digest: [32]u8, manifest: bool = false, layout_cooldown: u2 = 0, algorithm: Algorithm = .sha256_json };
pub fn contentKey(key: Key) ContentKey {
    var result: ContentKey = undefined;
    @memcpy(result[0..content_prefix.len], content_prefix);
    @memcpy(result[content_prefix.len..], key[prefix.len..]);
    return result;
}
fn decodeContent(raw: []const u8) !Content {
    if (raw.len != 49) return error.InvalidDocumentContentAuthority;
    return .{ .revision = std.mem.readInt(u64, raw[0..8], .little), .bytes = std.mem.readInt(u64, raw[8..16], .little), .kind = raw[16] & 0x17, .layout_cooldown = @intCast(((raw[16] >> 3) & 1) | (((raw[16] >> 5) & 1) << 1)), .digest = raw[17..49].*, .manifest = raw[16] & 0x80 != 0, .algorithm = if (raw[16] & 0x40 != 0) .logical_json_v1 else .sha256_json };
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
    const T = switch (@typeInfo(@TypeOf(txn))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(txn),
    };
    if (comptime @hasDecl(T, "usesLogicalDocumentIdentity")) if (value.algorithm == .sha256_json and value.bytes >= projection.minimum_bytes and keys.isPrimaryDocumentKey(physical) and try txn.usesLogicalDocumentIdentity()) return null;
    return value;
}
/// Verify cached roots against current mutation revisions in one sorted read.
/// Fixed windows carry only metadata and decode every borrowed value before the
/// next read can retire it. Duplicate owners are supported; no body is fetched.
pub fn loadContentWindow(txn: anytype, revision_keys: []const Key, contents: []?Content, mutation_revisions: []u64) !void {
    if (revision_keys.len != contents.len or mutation_revisions.len != contents.len or revision_keys.len > 64) return error.InvalidArgument;
    @memset(contents, null);
    @memset(mutation_revisions, 0);
    if (revision_keys.len == 0) return;
    const Entry = struct {
        key: [@max(@sizeOf(Key), @sizeOf(ContentKey))]u8,
        length: u8,
        row: u8,
        content: bool,
        fn bytes(self: *const @This()) []const u8 {
            return self.key[0..self.length];
        }
        fn less(_: void, left: @This(), right: @This()) bool {
            return std.mem.order(u8, left.bytes(), right.bytes()) == .lt;
        }
    };
    var entries: [128]Entry = undefined;
    const count = revision_keys.len * 2;
    for (revision_keys, 0..) |key, row| {
        entries[row * 2] = .{ .key = undefined, .length = @sizeOf(Key), .row = @intCast(row), .content = false };
        @memcpy(entries[row * 2].key[0..@sizeOf(Key)], &key);
        entries[row * 2 + 1] = .{ .key = undefined, .length = @sizeOf(ContentKey), .row = @intCast(row), .content = true };
        @memcpy(entries[row * 2 + 1].key[0..@sizeOf(ContentKey)], &contentKey(key));
    }
    std.mem.sort(Entry, entries[0..count], {}, Entry.less);
    var names: [128][]const u8 = undefined;
    var values: [128]?[]const u8 = undefined;
    for (entries[0..count], names[0..count]) |*entry, *name| name.* = entry.bytes();
    @memset(values[0..count], null);
    try txn.getManySorted(names[0..count], values[0..count]);
    for (entries[0..count], values[0..count]) |entry, raw| if (raw) |bytes| {
        if (entry.content) {
            contents[entry.row] = decodeContent(bytes) catch null;
        } else {
            if (bytes.len != 8) return error.InvalidDocumentMutationRevision;
            mutation_revisions[entry.row] = std.mem.readInt(u64, bytes[0..8], .little);
        }
    };
    for (contents, 0..) |*content, i| if (content.*) |cached| {
        if (cached.revision != mutation_revisions[i]) content.* = null;
    };
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
        const T = switch (@typeInfo(@TypeOf(txn))) {
            .pointer => |ptr| ptr.child,
            else => @TypeOf(txn),
        };
        if (comptime @hasDecl(T, "usesLogicalDocumentIdentity")) {
            if (cached.algorithm == .sha256_json and cached.bytes >= projection.minimum_bytes and keys.isPrimaryDocumentKey(row) and try txn.usesLogicalDocumentIdentity()) content.* = null;
        }
    };
}

fn encodeContent(value: Content) [49]u8 {
    var raw: [49]u8 = undefined;
    std.mem.writeInt(u64, raw[0..8], value.revision, .little);
    std.mem.writeInt(u64, raw[8..16], value.bytes, .little);
    raw[16] = value.kind | @as(u8, if (value.layout_cooldown & 1 != 0) 0x08 else 0) | @as(u8, if (value.layout_cooldown & 2 != 0) 0x20 else 0) | @as(u8, if (value.manifest) 0x80 else 0) | @as(u8, if (value.algorithm == .logical_json_v1) 0x40 else 0);
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
                        var rebuilt: Content = .{ .revision = try load(&metadata, keyForPhysical(last).?), .bytes = raw.len, .kind = last[last.len - 1], .digest = digest };
                        const T = @TypeOf(primary);
                        if (comptime @hasDecl(T, "getPhysical")) {
                            const physical = try primary.getPhysical(last);
                            const payload = @import("document_payload.zig");
                            if (payload.isManifest(physical)) {
                                const manifest = try payload.Manifest.decode(a, physical);
                                rebuilt.manifest = true;
                                rebuilt.algorithm = .logical_json_v1;
                                rebuilt.bytes = manifest.logical_bytes;
                                rebuilt.digest = manifest.logical_digest;
                            }
                        }
                        if (!rebuilt.manifest) {
                            if (comptime @hasField(@TypeOf(primary), "logical_documents")) if (raw.len >= projection.minimum_bytes and keys.isPrimaryDocumentKey(last) and try primary.usesLogicalDocumentIdentity()) {
                                rebuilt.digest = try logical.compute(a, raw);
                                rebuilt.algorithm = .logical_json_v1;
                            };
                        }
                        break :blk rebuilt;
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
    const Change = union(enum) { unchanged, deleted, written: struct { proof: Content, directory: ?[]u8, prior_manifest: ?bool = null, reused_bytes: usize = 0, identities: ?[]u8 = null, logical_record: ?[]u8 = null } };
    documents: std.AutoHashMapUnmanaged(Key, Change) = .empty,
    validations: std.AutoHashMapUnmanaged(Key, struct { certificate: @import("document_validation.zig").Certificate, key: [32]u8, physical: []u8 }) = .empty,
    staged: bool = false,
    document_format: @import("document_payload.zig").Admission = .unchecked,
    logical_commitments: bool = false,
    logical_minimum_bytes: usize = projection.minimum_bytes,
    minimum_reader_format: u32 = 26,
    validation_allocator: ?std.mem.Allocator = null,
    document_payload_stats: @import("document_payload.zig").Stats = .{},
    identity_reuse: value_identity.ReuseStats = .{},
    poisoned: bool = false,

    pub fn deinit(self: *Capture, alloc: std.mem.Allocator) void {
        var values = self.documents.valueIterator();
        while (values.next()) |value| if (value.* == .written) {
            if (value.written.directory) |directory| alloc.free(directory);
            if (value.written.identities) |identities| alloc.free(identities);
            if (value.written.logical_record) |record| alloc.free(record);
        };
        self.documents.deinit(alloc);
        var validations = self.validations.valueIterator();
        while (validations.next()) |validated| alloc.free(validated.physical);
        self.validations.deinit(alloc);
    }

    /// The common writer binds a native validation result to the actual source
    /// commitment it just captured, never to a caller-supplied digest.
    pub fn recordValidation(self: *Capture, a: std.mem.Allocator, physical: []const u8, value: []const u8, participant: ?@import("commit_participant.zig").Participant) !void {
        const provider = (participant orelse return).document_validations orelse return;
        const stamp = provider.find(provider.ptr, physical, value) orelse return;
        const publication = self.sourcePublication(physical) orelse return;
        if (publication.proof.algorithm != .logical_json_v1) return;
        errdefer self.poisoned = true;
        const certificate: @import("document_validation.zig").Certificate = .{ .root = publication.proof.digest, .rules = stamp.rules, .schema_version = stamp.schema_version, .physical = @splat(0) };
        if (self.validations.getPtr(keyForPhysical(physical).?)) |prior| {
            prior.certificate = certificate;
            prior.key = stamp.key;
            return;
        }
        self.validation_allocator = a;
        const owned = try a.dupe(u8, physical);
        errdefer a.free(owned);
        try self.validations.put(a, keyForPhysical(physical).?, .{ .certificate = certificate, .key = stamp.key, .physical = owned });
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
    pub const SourcePublication = struct { proof: Content, directory: ?[]const u8, identities: ?[]const u8, logical_record: ?[]const u8, prior_manifest: ?bool, reused_bytes: usize };
    pub fn sourcePublication(self: *Capture, physical: []const u8) ?SourcePublication {
        const key = keyForPhysical(physical) orelse return null;
        const entry = self.documents.get(key) orelse return null;
        if (entry != .written) return null;
        return .{ .proof = entry.written.proof, .directory = entry.written.directory, .identities = entry.written.identities, .logical_record = entry.written.logical_record, .prior_manifest = entry.written.prior_manifest, .reused_bytes = entry.written.reused_bytes };
    }
    /// Adopt a writer-prepared publication without re-reading immutable values.
    pub fn touchMetadataPatch(self: *Capture, alloc: std.mem.Allocator, physical: []const u8, patch: @import("document_payload.zig").MetadataPatch) !void {
        try self.touch(alloc, physical);
        errdefer self.poisoned = true;
        const directory = try alloc.dupe(u8, patch.directory);
        errdefer alloc.free(directory);
        const identities = try alloc.dupe(u8, patch.identities);
        errdefer alloc.free(identities);
        const logical_record = try alloc.dupe(u8, patch.logical_record);
        const entry = self.documents.getPtr(keyForPhysical(physical).?).?;
        if (entry.* == .written) {
            if (entry.written.directory) |value| alloc.free(value);
            if (entry.written.identities) |value| alloc.free(value);
            if (entry.written.logical_record) |value| alloc.free(value);
        }
        entry.* = .{ .written = .{ .proof = patch.proof, .directory = directory, .identities = identities, .logical_record = logical_record, .prior_manifest = true } };
    }

    pub fn notePriorExternal(self: *Capture, physical: []const u8) void {
        const key = keyForPhysical(physical) orelse return;
        const entry = self.documents.getPtr(key) orelse return;
        if (entry.* == .written) entry.written.prior_manifest = true;
    }

    pub fn markManifest(self: *Capture, physical: []const u8, manifest: ?[]const u8) void {
        const key = keyForPhysical(physical) orelse return;
        const entry = self.documents.getPtr(key) orelse return;
        if (entry.* != .written) return;
        if (manifest != null) entry.written.proof.layout_cooldown = 0 else if (entry.written.prior_manifest == true) {
            entry.written.proof.layout_cooldown = 3;
        } else if (entry.written.reused_bytes >= @max(@as(u64, @import("document_payload.zig").minimum_value_bytes), entry.written.proof.bytes / 2)) {
            // Require consecutive stable reuses after churn. The two-bit hint
            // shares the existing authority record; it adds no admission key.
            entry.written.proof.layout_cooldown -|= 1;
        } else if (entry.written.proof.layout_cooldown != 0) {
            entry.written.proof.layout_cooldown = 3;
        }
        entry.written.proof.manifest = manifest != null;
        if (manifest) |raw| std.debug.assert(std.mem.eql(u8, &entry.written.proof.digest, &@import("document_payload.zig").publishedCommitment(raw)));
    }

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
        return self.touchValuePrepared(alloc, physical, value, .{ .bytes = proof.bytes, .kind = proof.kind, .manifest = proof.manifest, .layout_cooldown = proof.layout_cooldown, .digest = proof.digest, .directory = directory, .identities = raw_certificate });
    }

    fn touchValuePrepared(self: *Capture, alloc: std.mem.Allocator, physical: []const u8, value: ?[]const u8, prior: ?value_identity.Prior) !void {
        try self.touch(alloc, physical);
        if (!keys.isStoredDocumentRowKey(physical)) return;
        const entry = self.documents.getPtr(keyForPhysical(physical).?).?;
        errdefer self.poisoned = true;
        const next: Change = if (value) |bytes| blk: {
            var digest: [32]u8 = undefined;
            var logical_record: ?[]u8 = null;
            errdefer if (logical_record) |record| alloc.free(record);
            const logical_mode = self.logical_commitments and keys.isPrimaryDocumentKey(physical) and bytes.len >= self.logical_minimum_bytes;
            if (logical_mode) digest = @splat(0) else std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
            const kind = physical[physical.len - 1];
            const previous: ?value_identity.Prior = if (entry.* == .written and entry.written.directory != null and entry.written.identities != null)
                .{ .bytes = entry.written.proof.bytes, .kind = entry.written.proof.kind, .manifest = entry.written.proof.manifest, .layout_cooldown = entry.written.proof.layout_cooldown, .digest = entry.written.proof.digest, .directory = entry.written.directory.?, .identities = entry.written.identities.? }
            else
                prior;
            var reuse: value_identity.ReuseStats = .{};
            const publication = if (previous) |qualified| try value_identity.buildAuthorityReusing(alloc, bytes, kind, digest, qualified, &reuse) else try value_identity.buildAuthority(alloc, bytes, kind, digest);
            errdefer if (publication.directory) |directory| alloc.free(directory);
            errdefer if (publication.identities) |identities| alloc.free(identities);
            if (logical_mode) {
                const root = (try logical.fromPublication(alloc, bytes, kind, digest, publication)) orelse try logical.build(alloc, bytes);
                defer root.deinit(alloc);
                try value_identity.rebindPublication(publication, digest, root.digest);
                digest = root.digest;
                logical_record = logical.encode(alloc, root) catch |err| switch (err) {
                    error.InvalidDocumentCommitment => null,
                    else => return err,
                };
            }
            self.identity_reuse.reused_bytes +|= reuse.reused_bytes;
            self.identity_reuse.reused_leaves +|= reuse.reused_leaves;
            self.identity_reuse.canonical_bytes +|= reuse.canonical_bytes;
            break :blk .{ .written = .{ .proof = .{ .revision = 0, .bytes = bytes.len, .kind = kind, .digest = digest, .layout_cooldown = if (previous) |p| p.layout_cooldown else 0, .algorithm = if (logical_mode) .logical_json_v1 else .sha256_json }, .logical_record = logical_record, .directory = publication.directory, .prior_manifest = if (previous) |p| p.manifest else null, .reused_bytes = reuse.reused_bytes, .identities = publication.identities } };
        } else .deleted;
        if (entry.* == .written) {
            if (entry.written.directory) |directory| alloc.free(directory);
            if (entry.written.identities) |identities| alloc.free(identities);
            if (entry.written.logical_record) |record| alloc.free(record);
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
            const validation = @import("document_validation.zig");
            if (entry.value_ptr.* == .deleted and self.logical_commitments) {
                // Avoid an absent-key tombstone for rows never carrying a
                // validation certificate. Low-level/native deletes also prune
                // stale or malformed certificates without authenticating them.
                const present = @import("document_payload.zig").rawGet(txn, &validation.keyForRevision(key)) catch |err| blk: {
                    if (err == error.NotFound) break :blk null;
                    return err;
                };
                if (present != null) try txn.delete(&validation.keyForRevision(key));
            } else if (cached) |current| if (self.validations.get(key)) |validated| {
                // A later unvalidated PUT may have replaced this transaction's
                // source. A proof for earlier bytes must never follow it.
                if (validated.certificate.matches(current, validated.certificate.rules, validated.certificate.schema_version, validated.certificate.physical)) {
                    // Content.manifest is a disposable hint. Authenticate the
                    // final physical afterimage in this writer, including a
                    // possible later PUT/appendPUT of the same logical source.
                    const payload = @import("document_payload.zig");
                    const physical = try payload.rawGet(txn, validated.physical);
                    if (payload.isManifest(physical)) {
                        const a = self.validation_allocator orelse return error.InvalidBatch;
                        var arena = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.reclaimingScratch(a));
                        defer arena.deinit();
                        const manifest = try payload.Manifest.decode(arena.allocator(), physical);
                        if (std.mem.eql(u8, &manifest.logical_digest, &current.digest)) {
                            var certificate = validated.certificate;
                            certificate.physical = manifest.physical_commitment;
                            try txn.put(&validation.keyForRevision(key), &certificate.encode(validated.key, validation.keyForRevision(key)));
                        }
                    }
                }
            };
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

test "document payload validation publication owns failures and binds actual manifests without cache hints" {
    const validation = @import("document_validation.zig");
    const payload = @import("document_payload.zig");
    const participation = @import("commit_participant.zig");
    const Case = struct {
        const Provider = struct {
            row: []const u8,
            value: []const u8,
            stamp: validation.Stamp,
            fn find(raw: *const anyopaque, physical: []const u8, value: []const u8) ?validation.Stamp {
                const self: *const @This() = @ptrCast(@alignCast(raw));
                if (!std.mem.eql(u8, physical, self.row) or !std.mem.eql(u8, value, self.value)) return null;
                return self.stamp;
            }
        };
        const Writer = struct {
            row: []const u8,
            manifest: []const u8,
            stamp: validation.Stamp,
            root: [32]u8,
            certificates: usize = 0,
            fail_certificate: bool = false,
            pub fn get(self: *@This(), key: []const u8) ![]const u8 {
                return self.getPhysical(key);
            }
            pub fn getPhysical(self: *@This(), key: []const u8) ![]const u8 {
                return if (std.mem.eql(u8, key, self.row)) self.manifest else error.NotFound;
            }
            pub fn delete(_: *@This(), _: []const u8) !void {
                return error.NotFound;
            }
            pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
                const row = validation.keyForPhysical(self.row).?;
                if (!std.mem.eql(u8, key, &row)) return;
                if (self.fail_certificate) return error.SyntheticCertificateFailure;
                const certificate = validation.Certificate.decode(value, self.stamp.key, row) orelse return error.InvalidTestCertificate;
                try std.testing.expectEqualSlices(u8, &self.root, &certificate.root);
                try std.testing.expectEqualSlices(u8, self.manifest[self.manifest.len - 32 ..], &certificate.physical);
                try std.testing.expectEqualSlices(u8, &self.stamp.rules, &certificate.rules);
                self.certificates += 1;
            }
        };
        fn run(a: std.mem.Allocator, fail_certificate: bool) !void {
            return runInner(a, fail_certificate) catch |err| switch (err) {
                error.WriteFailed => error.OutOfMemory,
                else => err,
            };
        }
        fn runInner(a: std.mem.Allocator, fail_certificate: bool) !void {
            const row = try keys.documentKeyAlloc(a, "doc\x00id");
            defer a.free(row);
            const body = try a.alloc(u8, payload.minimum_value_bytes + 2);
            defer a.free(body);
            @memset(body, 'x');
            body[0] = '\"';
            body[body.len - 1] = '\"';
            const value = try std.fmt.allocPrint(a, "{{\"body\":{s},\"metadata\":1}}", .{body});
            defer a.free(value);
            var capture: Capture = .{ .logical_commitments = true };
            defer capture.deinit(a);
            try capture.touchValue(a, row, value);
            const source = capture.sourcePublication(row).?;
            try std.testing.expect(!source.proof.manifest);
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(body, &digest, .{});
            const manifest = try payload.encodeLogical(a, &.{ .{ .inline_json = "{\"body\":" }, .{ .blob = .{ .length = body.len, .digest = digest } }, .{ .inline_json = ",\"metadata\":1}" } }, source.proof.digest, source.logical_record.?);
            defer a.free(manifest);
            const provider: Provider = .{ .row = row, .value = value, .stamp = .{ .rules = @splat(0x23), .schema_version = 7, .key = @splat(0x45) } };
            const participant = participation.Participant.withDocumentValidations(null, .{ .ptr = &provider, .find = Provider.find });
            try capture.recordValidation(a, row, "unvalidated different bytes", participant);
            try std.testing.expectEqual(@as(usize, 0), capture.validations.count());
            try capture.recordValidation(a, row, value, participant);
            try capture.recordValidation(a, row, value, participant);
            try std.testing.expectEqual(@as(usize, 1), capture.validations.count());
            var writer: Writer = .{ .row = row, .manifest = manifest, .stamp = provider.stamp, .root = source.proof.digest, .fail_certificate = fail_certificate };
            if (fail_certificate) {
                if (capture.stage(&writer)) |_| return error.ExpectedCertificateFailure else |err| switch (err) {
                    error.SyntheticCertificateFailure => {},
                    else => return err,
                }
                try std.testing.expect(capture.poisoned);
                try std.testing.expectError(error.RetainedEffectsTransactionFailed, capture.stage(&writer));
            } else {
                try capture.stage(&writer);
                try std.testing.expectEqual(@as(usize, 1), writer.certificates);
            }
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{false});
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{true});
}

test "document payload validation root windows batch sorted metadata and fence every owner" {
    const Fake = struct {
        revision_keys: [64]Key,
        content_keys: [64]ContentKey,
        proofs: [64][49]u8,
        revisions: [64][8]u8,
        missing_content: ?usize = null,
        malformed_content: ?usize = null,
        malformed_revision: ?usize = null,
        calls: usize = 0,
        pub fn getManySorted(self: *@This(), names: []const []const u8, values: []?[]const u8) !void {
            self.calls += 1;
            for (names, values, 0..) |name, *value, i| {
                if (i != 0) try std.testing.expect(std.mem.order(u8, names[i - 1], name) != .gt);
                value.* = null;
                for (self.revision_keys, self.content_keys, 0..) |revision, content, row| {
                    if (std.mem.eql(u8, name, &revision)) {
                        value.* = if (self.malformed_revision == row) "bad" else &self.revisions[row];
                        break;
                    }
                    if (std.mem.eql(u8, name, &content)) {
                        value.* = if (self.missing_content == row) null else if (self.malformed_content == row) "bad" else &self.proofs[row];
                        break;
                    }
                } else return error.UnexpectedMetadataKey;
            }
        }
    };
    const a = std.testing.allocator;
    var fake: Fake = undefined;
    fake.missing_content = null;
    fake.malformed_content = null;
    fake.malformed_revision = null;
    fake.calls = 0;
    for (0..64) |i| {
        var id: [32]u8 = undefined;
        const physical = try keys.documentKeyAlloc(a, try std.fmt.bufPrint(&id, "doc-{d}", .{i}));
        defer a.free(physical);
        fake.revision_keys[i] = keyForPhysical(physical).?;
        fake.content_keys[i] = contentKey(fake.revision_keys[i]);
        fake.proofs[i] = encodeContent(.{ .revision = 100 + i, .bytes = 1000 + i, .kind = keys.primary_kind, .digest = @splat(@intCast(i)), .algorithm = .logical_json_v1 });
        std.mem.writeInt(u64, &fake.revisions[i], 100 + i, .little);
    }
    var contents: [64]?Content = undefined;
    var revisions: [64]u64 = undefined;
    try loadContentWindow(&fake, &fake.revision_keys, &contents, &revisions);
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    for (contents, revisions, 0..) |content, revision, i| {
        try std.testing.expectEqual(@as(u64, 100 + i), revision);
        try std.testing.expectEqual(revision, content.?.revision);
        try std.testing.expectEqual(@as(u64, 1000 + i), content.?.bytes);
    }
    const duplicates = [_]Key{ fake.revision_keys[3], fake.revision_keys[0], fake.revision_keys[3] };
    try loadContentWindow(&fake, &duplicates, contents[0..3], revisions[0..3]);
    try std.testing.expectEqual(@as(u64, 103), contents[0].?.revision);
    try std.testing.expectEqual(@as(u64, 100), contents[1].?.revision);
    try std.testing.expectEqual(contents[0].?, contents[2].?);
    fake.missing_content = 0;
    fake.malformed_content = 2;
    fake.proofs[1][0] ^= 1;
    try loadContentWindow(&fake, &fake.revision_keys, &contents, &revisions);
    try std.testing.expect(contents[0] == null and contents[1] == null and contents[2] == null);
    try std.testing.expectEqual(@as(u64, 102), revisions[2]);
    try std.testing.expect(contents[3] != null);
    fake.malformed_revision = 3;
    try std.testing.expectError(error.InvalidDocumentMutationRevision, loadContentWindow(&fake, &fake.revision_keys, &contents, &revisions));
    const calls = fake.calls;
    try loadContentWindow(&fake, &.{}, contents[0..0], revisions[0..0]);
    try std.testing.expectEqual(calls, fake.calls);
    var oversized: [65]Key = undefined;
    var invalid_contents: [65]?Content = undefined;
    var invalid_revisions: [65]u64 = undefined;
    try std.testing.expectError(error.InvalidArgument, loadContentWindow(&fake, &oversized, &invalid_contents, &invalid_revisions));
    try std.testing.expectEqual(calls, fake.calls);
}
