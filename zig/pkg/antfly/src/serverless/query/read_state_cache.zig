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

//! Bounded derived immutable overlays. No authority, leases, sessions, caller
//! allocators or resolver pointers survive a request. Fresh root identity (or
//! its native-published immediate parent) is required before reuse.
const std = @import("std");
const A = std.mem.Allocator;
const Overlay = @import("live_overlay.zig").Overlay;
const Snapshot = @import("../wal/types.zig").ReadSnapshot;
const Wal = @import("../wal/store.zig").WalStore;
pub const Cache = struct {
    pub const max_bytes = 256 * 1024;
    pub const max_entries = 4;
    const Entry = struct { scope: [32]u8, base: u64, snapshot: Snapshot, overlay: Overlay };
    const CostHint = struct { scope: [32]u8, base: u64, identity: [32]u8, limit: usize };
    oversized: [max_entries]?CostHint = @splat(null),
    next_hint: usize = 0,
    mu: std.atomic.Mutex = .unlocked,
    entries: [max_entries]?Entry = @splat(null),
    bytes: usize = 0,
    next: usize = 0,
    hits: u64 = 0,
    extensions: u64 = 0,
    fn scope(namespace: []const u8) [32]u8 {
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(namespace, &digest, .{});
        return digest;
    }
    fn charge(overlay: *const Overlay) usize {
        return @sizeOf(Entry) + overlay.retainedBytes();
    }
    fn remove(self: *Cache, index: usize) void {
        if (self.entries[index]) |*entry| {
            self.bytes -= charge(&entry.overlay);
            entry.overlay.deinit();
            self.entries[index] = null;
        }
    }
    pub fn deinit(self: *Cache) void {
        @import("antfly_platform").sync.lockYielding(&self.mu);
        defer self.mu.unlock();
        for (0..max_entries) |i| self.remove(i);
        for (&self.oversized) |*hint| if (hint.* != null) {
            hint.* = null;
            self.bytes -= @sizeOf(CostHint);
        };
    }
    pub fn canWarm(self: *Cache, namespace: []const u8, base: u64, snapshot: Snapshot, limit: usize) bool {
        @import("antfly_platform").sync.lockYielding(&self.mu);
        defer self.mu.unlock();
        for (self.oversized) |hint| if (hint) |value| {
            if (value.base == base and value.limit >= limit and std.mem.eql(u8, &value.scope, &scope(namespace)) and std.mem.eql(u8, &value.identity, &snapshot.identity)) return false;
        };
        return true;
    }
    /// A cost hint for one immutable prefix, not a missing-object or authority
    /// cache. A changed root/base/budget must be planned independently.
    pub fn noteOversized(self: *Cache, namespace: []const u8, base: u64, snapshot: Snapshot, limit: usize) void {
        @import("antfly_platform").sync.lockYielding(&self.mu);
        defer self.mu.unlock();
        if (self.oversized[self.next_hint] != null) self.bytes -= @sizeOf(CostHint);
        self.oversized[self.next_hint] = .{ .scope = scope(namespace), .base = base, .identity = snapshot.identity, .limit = limit };
        self.bytes += @sizeOf(CostHint);
        self.next_hint = (self.next_hint + 1) % max_entries;
        while (self.bytes > max_bytes) {
            self.remove(self.next);
            self.next = (self.next + 1) % max_entries;
        }
    }
    const Candidate = struct { overlay: Overlay, from: u64, extends: bool };
    fn cached(self: *Cache, a: A, namespace: []const u8, base: u64, snapshot: Snapshot, max_body_bytes: usize, resolver: ?Overlay.BodyResolver) !?Candidate {
        @import("antfly_platform").sync.lockYielding(&self.mu);
        defer self.mu.unlock();
        for (&self.entries) |*slot| if (slot.*) |*entry| {
            if (entry.base != base or !std.mem.eql(u8, &entry.scope, &scope(namespace)) or !std.mem.eql(u8, &entry.snapshot.lineage, &snapshot.lineage)) continue;
            const same = entry.snapshot.latest_lsn == snapshot.latest_lsn and std.mem.eql(u8, &entry.snapshot.identity, &snapshot.identity);
            const extends = snapshot.latest_lsn > entry.snapshot.latest_lsn and snapshot.parent_lsn == entry.snapshot.latest_lsn and snapshot.parent_identity != null and std.mem.eql(u8, &snapshot.parent_identity.?, &entry.snapshot.identity);
            if (same or extends) return .{ .overlay = try entry.overlay.clone(a, max_body_bytes, resolver), .from = entry.snapshot.latest_lsn, .extends = extends };
        };
        return null;
    }
    pub fn prepare(self: *Cache, a: A, namespace: []const u8, base: u64, snapshot: Snapshot, max_records: usize, max_body_bytes: usize, resolver: ?Overlay.BodyResolver, wal: *Wal, cancellation: ?@import("objectstore").CancellationToken) !?Overlay {
        if (cancellation) |token| try token.check();
        if (snapshot.latest_lsn < base or snapshot.latest_lsn - base > max_records or base + 1 < snapshot.first_lsn) return null;
        const cached_value = (try self.cached(a, namespace, base, snapshot, max_body_bytes, resolver)) orelse return null;
        var candidate = cached_value.overlay;
        errdefer candidate.deinit();
        // No cache lock, shared state or caller pointer crosses object I/O.
        if (cached_value.extends) {
            const tail = try wal.readRangeAlloc(namespace, .{ .start_lsn = cached_value.from + 1, .end_lsn = snapshot.latest_lsn, .max_records = max_records, .max_bytes = max_body_bytes - candidate.bytes, .cancellation = cancellation });
            defer @import("../wal/types.zig").freeRecords(wal.allocator, tail);
            try candidate.extend(tail, snapshot.latest_lsn);
        }
        @import("antfly_platform").sync.lockYielding(&self.mu);
        if (cached_value.extends) self.extensions += 1 else self.hits += 1;
        self.mu.unlock();
        return candidate;
    }
    fn alreadyCurrent(self: *Cache, namespace: []const u8, base: u64, snapshot: Snapshot) bool {
        for (self.entries) |entry| if (entry) |value| {
            if (value.base != base or !std.mem.eql(u8, &value.scope, &scope(namespace))) continue;
            if (std.mem.eql(u8, &value.snapshot.identity, &snapshot.identity) or (std.mem.eql(u8, &value.snapshot.lineage, &snapshot.lineage) and value.snapshot.latest_lsn > snapshot.latest_lsn)) return true;
        };
        return false;
    }
    pub fn remember(self: *Cache, persistent: A, namespace: []const u8, base: u64, snapshot: Snapshot, overlay: *const Overlay) !void {
        if (base > snapshot.latest_lsn or overlay.cutoff != snapshot.latest_lsn) return error.InvalidWal;
        if (charge(overlay) > max_bytes - @sizeOf(CostHint) * max_entries) return;
        @import("antfly_platform").sync.lockYielding(&self.mu);
        const current = self.alreadyCurrent(namespace, base, snapshot);
        self.mu.unlock();
        if (current) return;
        // Clone before replacing the published cache entry. Failed allocation
        // cannot partially advance a watermark or poison subsequent requests.
        var copy = try overlay.clone(persistent, overlay.max_bytes, null);
        errdefer copy.deinit();
        @import("antfly_platform").sync.lockYielding(&self.mu);
        defer self.mu.unlock();
        if (self.alreadyCurrent(namespace, base, snapshot)) {
            copy.deinit();
            return;
        }
        for (self.entries, 0..) |entry, i| if (entry) |value| {
            if (std.mem.eql(u8, &value.scope, &scope(namespace))) self.remove(i);
        };
        self.remove(self.next);
        while (self.bytes + charge(&copy) > max_bytes) {
            self.next = (self.next + 1) % max_entries;
            self.remove(self.next);
        }
        self.entries[self.next] = .{ .scope = scope(namespace), .base = base, .snapshot = snapshot, .overlay = copy };
        self.bytes += charge(&copy);
        self.next = (self.next + 1) % max_entries;
    }
};

test "serverless native read plan state cache extends authenticated parents and rejects gaps epochs and expired prefixes" {
    const a = std.testing.allocator;
    var memory = @import("objectstore").MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("db");
    var indexed = try @import("../wal/indexed_object_store.zig").IndexedObjectStore.init(a, client, "db", "state");
    defer indexed.deinit();
    indexed.read_state_enabled = true;
    indexed.inline_tail_enabled = true;
    var wal = indexed.walStore();
    var cache: Cache = .{};
    defer cache.deinit();
    const View = @import("live_view.zig").View;
    const payload = try @import("../api/codec.zig").encodeMutationAlloc(a, .{ .kind = .upsert, .doc_id = "one", .body = "value" });
    defer a.free(payload);
    _ = try indexed.append("docs", 0, payload);
    const options: View.Options = .{ .state_cache = &cache, .cache_allocator = a };
    var first = try View.open(a, &wal, "docs", 0, options);
    first.deinit();
    var hit = try View.open(a, &wal, "docs", 0, options);
    hit.deinit();
    try std.testing.expectEqual(@as(u64, 1), cache.hits);
    try std.testing.expect(cache.bytes <= Cache.max_bytes);
    const deleted = try @import("../api/codec.zig").encodeMutationAlloc(a, .{ .kind = .delete, .doc_id = "one" });
    defer a.free(deleted);
    _ = try indexed.append("docs", 0, deleted);
    var extended = try View.open(a, &wal, "docs", 0, options);
    defer extended.deinit();
    try std.testing.expectEqual(@as(u64, 1), cache.extensions);
    try std.testing.expectEqual(.deleted, try extended.overlay.lookup("one"));
    _ = try indexed.append("docs", 0, payload);
    _ = try indexed.append("docs", 0, payload);
    var missed = try View.open(a, &wal, "docs", 0, options);
    missed.deinit();
    try std.testing.expectEqual(@as(u64, 1), cache.extensions);
    var isolated = try View.open(a, &wal, "other", 0, options);
    defer isolated.deinit();
    try std.testing.expectEqual(@as(usize, 0), isolated.overlay.by_id.count());
    _ = try indexed.beginCollection("docs", @splat(1));
    var after_epoch = try View.open(a, &wal, "docs", 0, options);
    after_epoch.deinit();
    try std.testing.expectEqual(@as(u64, 1), cache.hits);
    _ = try indexed.truncatePrefix("docs", 2);
    try std.testing.expectError(error.WalSnapshotExpired, View.open(a, &wal, "docs", 0, options));
    var published = try View.open(a, &wal, "docs", 4, options);
    defer published.deinit();
    try std.testing.expectEqual(@as(usize, 0), published.overlay.by_id.count());
    try std.testing.expect(cache.bytes <= Cache.max_bytes);
}

test "serverless native read plan state cache clone failure preserves the committed cached watermark" {
    const a = std.testing.allocator;
    const payload = try @import("../api/codec.zig").encodeMutationAlloc(a, .{ .kind = .upsert, .doc_id = "one", .body = "original" });
    defer a.free(payload);
    var overlay = try Overlay.init(a, &.{.{ .lsn = 1, .timestamp_ns = 0, .payload = payload }}, 0, 1, 8192);
    defer overlay.deinit();
    var cache: Cache = .{};
    defer cache.deinit();
    const snapshot: Snapshot = .{ .latest_lsn = 1, .first_lsn = 1, .identity = @splat(1), .lineage = @splat(1) };
    try cache.remember(a, "docs", 0, snapshot, &overlay);
    const bytes = cache.bytes;
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    // An exact cache hit must not allocate or replace persistent state.
    try cache.remember(failing.allocator(), "docs", 0, snapshot, &overlay);
    var changed = snapshot;
    changed.identity[0] = 2;
    try std.testing.expectError(error.OutOfMemory, cache.remember(failing.allocator(), "docs", 0, changed, &overlay));
    try std.testing.expectEqual(bytes, cache.bytes);
    var count: usize = 0;
    for (cache.entries) |entry| if (entry != null) {
        count += 1;
    };
    try std.testing.expectEqual(@as(usize, 1), count);
}

fn cacheRememberAllocationCase(a: A) !void {
    const backing = std.testing.allocator;
    const payload = try @import("../api/codec.zig").encodeMutationAlloc(backing, .{ .kind = .upsert, .doc_id = "one", .body = "owned" });
    defer backing.free(payload);
    var overlay = try Overlay.init(backing, &.{.{ .lsn = 1, .timestamp_ns = 0, .payload = payload }}, 0, 1, 8192);
    defer overlay.deinit();
    var cache: Cache = .{};
    defer cache.deinit();
    const snapshot: Snapshot = .{ .latest_lsn = 1, .first_lsn = 1, .identity = @splat(1), .lineage = @splat(1) };
    try cache.remember(backing, "docs", 0, snapshot, &overlay);
    const before = cache.bytes;
    var changed = snapshot;
    changed.identity[0] = 2;
    cache.remember(a, "docs", 0, changed, &overlay) catch |err| {
        try std.testing.expectEqual(before, cache.bytes);
        return err;
    };
}
test "serverless native read plan state cache allocation failures release every cloned byte" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, cacheRememberAllocationCase, .{});
}

test "serverless native read plan warms tiny tails and remembers bounded oversized planning hints" {
    const a = std.testing.allocator;
    var memory = @import("objectstore").MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("db");
    var indexed = try @import("../wal/indexed_object_store.zig").IndexedObjectStore.init(a, client, "db", "warm");
    defer indexed.deinit();
    indexed.read_state_enabled = true;
    indexed.document_index_enabled = true;
    indexed.inline_tail_enabled = true;
    var wal = indexed.walStore();
    var cache: Cache = .{};
    defer cache.deinit();
    const View = @import("live_view.zig").View;
    const payload = try @import("../api/codec.zig").encodeMutationAlloc(a, .{ .kind = .upsert, .doc_id = "one", .body = "small" });
    defer a.free(payload);
    _ = try indexed.append("small", 0, payload);
    _ = try indexed.append("small", 0, payload);
    const options: View.Options = .{ .state_cache = &cache, .cache_allocator = a, .point_id = "missing" };
    var warmed = try View.open(a, &wal, "small", 0, options);
    warmed.deinit();
    var reused = try View.open(a, &wal, "small", 0, options);
    reused.deinit();
    try std.testing.expectEqual(@as(u64, 1), cache.hits);
    var limited = options;
    limited.max_records = 1;
    var point_only = try View.open(a, &wal, "small", 0, limited);
    defer point_only.deinit();
    try std.testing.expectEqualStrings("missing", point_only.point_id.?);
    const body = try a.alloc(u8, 96 * 1024);
    defer a.free(body);
    @memset(body, 'x');
    const large = try @import("../api/codec.zig").encodeMutationAlloc(a, .{ .kind = .upsert, .doc_id = "large", .body = body });
    defer a.free(large);
    _ = try indexed.append("large", 0, large);
    _ = try indexed.append("large", 0, payload);
    const snapshot = try indexed.readSnapshot("large");
    memory.resetOperationCount();
    var projected = try View.open(a, &wal, "large", 0, options);
    projected.deinit();
    const first = memory.operationCount();
    try std.testing.expect(!cache.canWarm("large", 0, snapshot, 64 * 1024));
    try std.testing.expect(cache.canWarm("large", 0, snapshot, 128 * 1024));
    memory.resetOperationCount();
    var hinted = try View.open(a, &wal, "large", 0, options);
    hinted.deinit();
    try std.testing.expect(memory.operationCount() < first);
    _ = try indexed.append("large", 0, payload);
    try std.testing.expect(cache.canWarm("large", 0, try indexed.readSnapshot("large"), 64 * 1024));
    cache.deinit();
    try std.testing.expectEqual(@as(usize, 0), cache.bytes);
}
