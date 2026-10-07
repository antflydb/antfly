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
    pub fn init(self: *Index, a: A, store: stores.ArtifactStore, root: ?tree.Ref, cancellation: Cancellation) !void {
        const scope = store.upload_scope orelse return error.InvalidArtifactUploadScope;
        self.* = .{ .a = a, .root = root, .store = store };
        self.bridge = .{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = &self.store, .cancellation = cancellation, .remaining_read_bytes = &self.reads, .remaining_write_bytes = &self.writes };
        self.cache = .{ .underlying = self.bridge.store(), .slots = try std.heap.page_allocator.alloc(?tree.Ref, 4096) };
        @memset(self.cache.slots, null);
    }
    pub fn deinit(self: *Index) void {
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
            }
            entry.value_ptr.* = position;
        }
        var changes: std.ArrayList(tree.Mutation) = .empty;
        var cursor = try tree.Cursor.init(std.heap.page_allocator, self.cache.store(), self.root, "", null);
        defer cursor.deinit();
        while (try cursor.next()) |entry| {
            if (entry.key.len != 32) return error.InvalidLakeIndexCatalog;
            const key = entry.key[0..32].*;
            if (live.fetchRemove(key)) |next| {
                const bytes = try std.json.Stringify.valueAlloc(std.heap.page_allocator, values[next.value], .{});
                defer std.heap.page_allocator.free(bytes);
                if (!std.mem.eql(u8, entry.value, bytes)) try changes.append(a, .{ .key = try a.dupe(u8, &key), .value = try a.dupe(u8, bytes) });
            } else try changes.append(a, .{ .key = try a.dupe(u8, &key), .value = null });
        }
        var it = live.iterator();
        while (it.next()) |entry| try changes.append(a, .{ .key = try a.dupe(u8, entry.key_ptr), .value = try std.json.Stringify.valueAlloc(a, values[entry.value_ptr.*], .{}) });
        std.mem.sort(tree.Mutation, changes.items, {}, struct {
            fn less(_: void, l: tree.Mutation, r: tree.Mutation) bool {
                return std.mem.order(u8, l.key, r.key) == .lt;
            }
        }.less);
        return tree.apply(std.heap.page_allocator, self.cache.store(), self.root, changes.items);
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
