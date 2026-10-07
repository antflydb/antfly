// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Lazy authenticated contribution lookup and copy-on-write live-set updates.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.metadata_lake_index_catalog;
const tree = @import("../serverless/graph_segment/page_tree.zig");
const pages = @import("../serverless/graph_segment/page_store.zig");
const stores = @import("../serverless/artifacts/store.zig");
const A = std.mem.Allocator;
const Cancellation = @import("antfly_cancellation").CancellationToken;
pub const Index = struct {
    a: A,
    root: ?tree.Ref,
    store: stores.ArtifactStore,
    reads: u64 = 512 * 1024 * 1024,
    writes: u64 = 512 * 1024 * 1024,
    bridge: pages.PageStore = undefined,
    cache: PageCache = undefined,
    retained: std.AutoHashMapUnmanaged([32]u8, bool) = .empty,
    pub fn init(self: *Index, a: A, store: stores.ArtifactStore, root: ?tree.Ref, cancellation: Cancellation) !void {
        const scope = store.upload_scope orelse return error.InvalidArtifactUploadScope;
        self.* = .{ .a = a, .root = root, .store = store };
        self.bridge = .{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = &self.store, .cancellation = cancellation, .remaining_read_bytes = &self.reads, .remaining_write_bytes = &self.writes };
        self.cache = .{ .underlying = self.bridge.store(), .slots = try std.heap.page_allocator.alloc(?tree.Ref, 4096) };
        @memset(self.cache.slots, null);
    }
    pub fn deinit(self: *Index) void {
        self.retained.deinit(self.a);
        self.cache.deinit();
    }
    pub fn lookup(self: *Index, a: A, key: [32]u8) !?catalog.FileContribution {
        var cursor = try tree.Cursor.init(std.heap.page_allocator, self.cache.store(), self.root, &key, null);
        defer cursor.deinit();
        const entry = try cursor.next() orelse return null;
        if (!std.mem.eql(u8, entry.key, &key)) return null;
        const value = try std.json.parseFromSliceLeaky(catalog.FileContribution, a, entry.value, .{ .allocate = .alloc_always });
        try validate(value);
        if (!std.mem.eql(u8, &key, &identity(value))) return error.InvalidLakeIndexCatalog;
        return value;
    }
    /// Retain the authenticated ownership graph without opening aggregate
    /// roots, range directories or blocks. A gray entry detects cycles; the
    /// depth and total live-set bounds also cover malformed durable metadata.
    pub fn retain(self: *Index, key: [32]u8) !void {
        return self.retainAt(key, 0);
    }
    fn retainAt(self: *Index, key: [32]u8, depth: usize) anyerror!void {
        try self.bridge.cancellation.check();
        if (self.retained.get(key)) |complete| return if (complete) {} else error.InvalidLakeIndexCatalog;
        if (depth > 256 or self.retained.count() >= catalog.max_contributions) return error.InvalidLakeIndexCatalog;
        try self.retained.put(self.a, key, false);
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const value = try self.lookup(arena.allocator(), key) orelse return error.InvalidLakeIndexCatalog;
        for (value.owned orelse &.{}) |child| try self.retainAt(child, depth + 1);
        self.retained.getPtr(key).?.* = true;
    }
    pub fn update(self: *Index, values: []const catalog.FileContribution) !?tree.Ref {
        if (values.len > catalog.max_contributions) return error.InvalidLakeIndexCatalog;
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        var live: std.AutoHashMapUnmanaged([32]u8, usize) = .empty;
        for (values, 0..) |value, position| {
            if (position % 1024 == 0) try self.bridge.cancellation.check();
            try validate(value);
            const key = identity(value);
            const entry = try live.getOrPut(a, key);
            if (entry.found_existing) {
                const previous = values[entry.value_ptr.*].artifact;
                if (!std.mem.eql(u8, previous.artifact_id, value.artifact.artifact_id) or !std.mem.eql(u8, previous.checksum, value.artifact.checksum) or previous.byte_len != value.artifact.byte_len or previous.metadata_version != value.artifact.metadata_version) return error.InvalidLakeIndexCatalog;
                const owned = values[entry.value_ptr.*].owned;
                if ((owned == null) != (value.owned == null)) return error.InvalidLakeIndexCatalog;
                if (owned) |keys| {
                    if (keys.len != value.owned.?.len) return error.InvalidLakeIndexCatalog;
                    for (keys, value.owned.?) |left, right| if (!std.mem.eql(u8, &left, &right)) return error.InvalidLakeIndexCatalog;
                }
            }
            entry.value_ptr.* = position;
        }
        for (values) |value| for (value.owned orelse &.{}) |child| {
            if (!live.contains(child) and !self.retained.contains(child)) return error.InvalidLakeIndexCatalog;
        };
        var changes: std.ArrayList(tree.Mutation) = .empty;
        var it = live.iterator();
        while (it.next()) |entry| {
            const key = entry.key_ptr.*;
            const bytes = try std.json.Stringify.valueAlloc(a, values[entry.value_ptr.*], .{});
            var cursor = try tree.Cursor.init(std.heap.page_allocator, self.cache.store(), self.root, &key, null);
            defer cursor.deinit();
            const prior = try cursor.next();
            if (prior != null and std.mem.eql(u8, prior.?.key, &key) and std.mem.eql(u8, prior.?.value, bytes)) {
                try self.retained.put(self.a, key, true);
            } else try changes.append(a, .{ .key = try a.dupe(u8, &key), .value = bytes });
        }
        const keep = try a.alloc([]const u8, self.retained.count());
        var retained = self.retained.iterator();
        var next: usize = 0;
        while (retained.next()) |entry| {
            if (!entry.value_ptr.*) return error.InvalidLakeIndexCatalog;
            keep[next] = try a.dupe(u8, entry.key_ptr);
            next += 1;
        }
        std.mem.sort([]const u8, keep, {}, struct {
            fn less(_: void, l: []const u8, r: []const u8) bool {
                return std.mem.order(u8, l, r) == .lt;
            }
        }.less);
        std.mem.sort(tree.Mutation, changes.items, {}, struct {
            fn less(_: void, l: tree.Mutation, r: tree.Mutation) bool {
                return std.mem.order(u8, l.key, r.key) == .lt;
            }
        }.less);
        const base = try tree.retainKnown(std.heap.page_allocator, self.cache.store(), self.root, keep);
        const result = try tree.apply(std.heap.page_allocator, self.cache.store(), base, changes.items);
        if (result) |root| if (root.records > catalog.max_contributions) return error.InvalidLakeIndexCatalog;
        return result;
    }
};
pub fn identity(value: catalog.FileContribution) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("native-contribution-lookup-v1");
    hash.update(&value.file);
    hash.update(&value.recipe);
    hash.update(value.name);
    return hash.finalResult();
}
pub fn validate(value: catalog.FileContribution) !void {
    if (value.name.len == 0 or value.name.len > 128 or std.mem.allEqual(u8, &value.file, 0) or std.mem.allEqual(u8, &value.recipe, 0) or value.artifact.kind != .algebraic_segment) return error.InvalidLakeIndexCatalog;
    try stores.validateSha256ArtifactIdentity(value.artifact.artifact_id, value.artifact.checksum);
    if (value.owned) |keys| {
        if (keys.len > 66) return error.InvalidLakeIndexCatalog;
        for (keys, 0..) |key, i| {
            if (std.mem.allEqual(u8, &key, 0) or std.mem.eql(u8, &key, &identity(value))) return error.InvalidLakeIndexCatalog;
            for (keys[0..i]) |prior| if (std.mem.eql(u8, &key, &prior)) return error.InvalidLakeIndexCatalog;
        }
    }
}

/// Build-local FIFO metadata cache. Hash lookup avoids a linear cache search
/// for every file/recipe probe. Both bytes and entry count are hard bounds.
const PageCache = struct {
    underlying: tree.Store,
    entries: std.AutoHashMapUnmanaged(tree.Ref, []u8) = .empty,
    slots: []?tree.Ref,
    next: usize = 0,
    bytes: usize = 0,
    const a = std.heap.page_allocator;
    fn deinit(self: *@This()) void {
        var it = self.entries.valueIterator();
        while (it.next()) |value| a.free(value.*);
        self.entries.deinit(a);
        a.free(self.slots);
    }
    pub fn store(self: *@This()) tree.Store {
        return .{ .domain = self.underlying.domain, .attempt = self.underlying.attempt, .ptr = self, .get = get, .put = put, .check = check };
    }
    fn check(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.underlying.check(self.underlying.ptr);
    }
    fn get(raw: *anyopaque, alloc: A, ref: tree.Ref) ![]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try check(raw);
        if (self.entries.get(ref)) |bytes| return alloc.dupe(u8, bytes);
        const bytes = try self.underlying.get(self.underlying.ptr, alloc, ref);
        errdefer alloc.free(bytes);
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        if (bytes.len != ref.bytes or !std.mem.eql(u8, &digest, &ref.digest)) return error.ArtifactIntegrityMismatch;
        while (self.slots[self.next] != null or bytes.len > 128 * 1024 * 1024 -| self.bytes) {
            if (self.slots[self.next]) |old| {
                const value = self.entries.fetchRemove(old).?.value;
                self.bytes -= value.len;
                a.free(value);
                self.slots[self.next] = null;
            }
            self.next = (self.next + 1) % self.slots.len;
        }
        const owned = try a.dupe(u8, bytes);
        errdefer a.free(owned);
        try self.entries.put(a, ref, owned);
        self.slots[self.next] = ref;
        self.bytes += owned.len;
        self.next = (self.next + 1) % self.slots.len;
        return bytes;
    }
    fn put(raw: *anyopaque, ref: tree.Ref, bytes: []const u8) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.underlying.put(self.underlying.ptr, ref, bytes);
    }
};
