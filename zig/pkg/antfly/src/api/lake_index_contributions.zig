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
    counted: bool = false,
    covered_files: std.AutoHashMapUnmanaged(catalog.Digest, void) = .empty,
    has_file_coverage: bool = false,
    migrating: bool = false,
    prior_roots: []const catalog.Digest = &.{},
    roots: std.AutoHashMapUnmanaged(catalog.Digest, void) = .empty,
    pub fn init(self: *Index, a: A, store: stores.ArtifactStore, root: ?tree.Ref, cancellation: Cancellation) !void {
        const scope = store.upload_scope orelse return error.InvalidArtifactUploadScope;
        self.* = .{ .a = a, .root = root, .store = store };
        self.bridge = .{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = &self.store, .cancellation = cancellation, .remaining_read_bytes = &self.reads, .remaining_write_bytes = &self.writes };
        self.cache = .{ .underlying = self.bridge.store(), .slots = try std.heap.page_allocator.alloc(?tree.Ref, 4096) };
        @memset(self.cache.slots, null);
    }
    pub fn deinit(self: *Index) void {
        self.retained.deinit(self.a);
        self.roots.deinit(self.a);
        self.covered_files.deinit(self.a);
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
        // A counted generation retains descendants through existing incoming
        // edges. Only publication roots and changed edges need adjustment.
        if (self.counted) return self.bridge.cancellation.check();
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
    pub fn includeRoot(self: *Index, key: catalog.Digest) !void {
        if (self.roots.count() >= catalog.max_directory_artifacts and !self.roots.contains(key)) return error.InvalidLakeIndexCatalog;
        try self.roots.put(self.a, key, {});
    }
    pub fn rootKeys(self: *Index, a: A) ![]catalog.Digest {
        if (!self.counted) return a.alloc(catalog.Digest, 0);
        const keys = try a.alloc(catalog.Digest, self.roots.count());
        var iterator = self.roots.keyIterator();
        for (keys) |*key| key.* = iterator.next().?.*;
        std.mem.sort(catalog.Digest, keys, {}, struct {
            fn less(_: void, left: catalog.Digest, right: catalog.Digest) bool {
                return std.mem.order(u8, &left, &right) == .lt;
            }
        }.less);
        return keys;
    }
    pub fn update(self: *Index, values: []const catalog.FileContribution) !?tree.Ref {
        if (self.counted) return self.updateCounted(values);
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
    /// Immutable reference counts let unchanged ownership subtrees survive
    /// without loading or parsing their descendants. New roots are admitted
    /// before old roots retire, so shared aliases never briefly become dead.
    fn updateCounted(self: *Index, values: []const catalog.FileContribution) !?tree.Ref {
        if (values.len > catalog.max_contributions) return error.InvalidLakeIndexCatalog;
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        var delta: CountedUpdate = .{ .index = self, .a = a };
        for (values) |value| {
            try validate(value);
            const key = identity(value);
            const entry = try delta.pending.getOrPut(a, key);
            if (entry.found_existing and !try sameContribution(a, entry.value_ptr.*, value)) return error.InvalidLakeIndexCatalog;
            entry.value_ptr.* = value;
        }
        var proposed = delta.pending.keyIterator();
        while (proposed.next()) |key| try delta.replace(key.*);
        var prior: std.AutoHashMapUnmanaged(catalog.Digest, void) = .empty;
        for (self.prior_roots) |key| {
            const entry = try prior.getOrPut(a, key);
            if (entry.found_existing) return error.InvalidLakeIndexCatalog;
        }
        var roots = self.roots.keyIterator();
        while (roots.next()) |key| if (!prior.contains(key.*)) try delta.adjust(key.*, true, 0);
        for (self.prior_roots) |key| if (!self.roots.contains(key)) try delta.adjust(key, false, 0);
        var changes: std.ArrayList(tree.Mutation) = .empty;
        if (self.migrating) {
            // One-time protocol-27 upgrade also removes obsolete records. No
            // counted refresh repeats this old-generation census.
            var cursor = try tree.Cursor.init(std.heap.page_allocator, self.cache.store(), self.root, "", null);
            defer cursor.deinit();
            while (try cursor.next()) |entry| {
                if (entry.key.len != 32) return error.InvalidLakeIndexCatalog;
                const key = entry.key[0..32].*;
                if (!delta.entries.contains(key)) try changes.append(a, .{ .key = try a.dupe(u8, entry.key), .value = null });
            }
        }
        var iterator = delta.entries.iterator();
        while (iterator.next()) |entry| {
            const value = entry.value_ptr.value;
            const bytes = if (value.references == 0) null else try std.json.Stringify.valueAlloc(a, value, .{});
            const original = entry.value_ptr.original;
            if ((bytes == null and original == null) or (bytes != null and original != null and std.mem.eql(u8, bytes.?, original.?))) continue;
            try changes.append(a, .{ .key = try a.dupe(u8, entry.key_ptr), .value = bytes });
        }
        std.mem.sort(tree.Mutation, changes.items, {}, struct {
            fn less(_: void, left: tree.Mutation, right: tree.Mutation) bool {
                return std.mem.order(u8, left.key, right.key) == .lt;
            }
        }.less);
        const root = try tree.apply(std.heap.page_allocator, self.cache.store(), self.root, changes.items);
        if (root) |ref| if (ref.records > catalog.max_contributions) return error.InvalidLakeIndexCatalog;
        return root;
    }
};
const CountedUpdate = struct {
    const Entry = struct { value: catalog.FileContribution, original: ?[]const u8, visiting: bool = false };
    index: *Index,
    a: A,
    pending: std.AutoHashMapUnmanaged(catalog.Digest, catalog.FileContribution) = .empty,
    entries: std.AutoHashMapUnmanaged(catalog.Digest, Entry) = .empty,
    fn touch(self: *@This(), key: catalog.Digest) !void {
        if (self.entries.contains(key)) return;
        if (self.entries.count() >= catalog.max_contributions) return error.InvalidLakeIndexCatalog;
        const old = try self.index.lookup(self.a, key);
        if (old) |value| if (!self.index.migrating and value.references == 0) return error.InvalidLakeIndexCatalog;
        const proposed = self.pending.get(key);
        var value = old orelse proposed orelse return error.InvalidLakeIndexCatalog;
        value.references = if (old) |previous| if (self.index.migrating) 0 else previous.references else 0;
        try self.entries.put(self.a, key, .{ .value = value, .original = if (old) |previous| try std.json.Stringify.valueAlloc(self.a, previous, .{}) else null });
    }
    fn replace(self: *@This(), key: catalog.Digest) !void {
        try self.touch(key);
        const old = self.entries.get(key).?.value;
        var value = self.pending.get(key).?;
        value.references = old.references;
        if (try sameContribution(self.a, old, value)) return;
        const before = old.owned orelse &.{};
        const after = value.owned orelse &.{};
        if (old.references != 0) {
            // A stable reduction identity owns the same two mathematical
            // children. Cohort layout changes may replace only range aliases;
            // these are terminal lookup records, so replacement cannot create
            // a cycle through an unchanged live subtree.
            if ((old.owned == null) != (value.owned == null)) return error.InvalidLakeIndexCatalog;
            const changed = !std.mem.eql(u8, std.mem.sliceAsBytes(before), std.mem.sliceAsBytes(after));
            if (changed and (before.len < 2 or after.len < 2 or !std.mem.eql(u8, std.mem.sliceAsBytes(before[0..2]), std.mem.sliceAsBytes(after[0..2])))) return error.InvalidLakeIndexCatalog;
            for (after) |child| if (!containsKey(before, child)) {
                try self.touch(child);
                if (self.entries.get(child).?.value.owned != null) return error.InvalidLakeIndexCatalog;
                try self.adjust(child, true, 0);
            };
            for (before) |child| if (!containsKey(after, child)) try self.adjust(child, false, 0);
        }
        self.entries.getPtr(key).?.value = value;
    }
    fn adjust(self: *@This(), key: catalog.Digest, increase: bool, depth: usize) anyerror!void {
        try self.index.bridge.cancellation.check();
        if (depth > 256) return error.InvalidLakeIndexCatalog;
        try self.touch(key);
        const entry = self.entries.getPtr(key).?;
        if (entry.visiting) return error.InvalidLakeIndexCatalog;
        const before = entry.value.references;
        entry.value.references = if (increase) std.math.add(u32, before, 1) catch return error.InvalidLakeIndexCatalog else std.math.sub(u32, before, 1) catch return error.InvalidLakeIndexCatalog;
        if ((increase and before != 0) or (!increase and entry.value.references != 0)) return;
        const children = entry.value.owned orelse &.{};
        entry.visiting = true;
        for (children) |child| try self.adjust(child, increase, depth + 1);
        self.entries.getPtr(key).?.visiting = false;
    }
};
fn containsKey(keys: []const catalog.Digest, wanted: catalog.Digest) bool {
    for (keys) |key| if (std.mem.eql(u8, &key, &wanted)) return true;
    return false;
}
fn sameContribution(a: A, left: catalog.FileContribution, right: catalog.FileContribution) !bool {
    var l = left;
    var r = right;
    l.references = 0;
    r.references = 0;
    return std.mem.eql(u8, try std.json.Stringify.valueAlloc(a, l, .{}), try std.json.Stringify.valueAlloc(a, r, .{}));
}

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

test "external lake counted ownership preserves shared aliases retires dead branches and upgrades legacy counts" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-counted-contributions");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(9), 1, std.testing.io);
    var upload = try store.put("aggregate placeholder");
    defer upload.deinit(a);
    const artifact: local.serverless_manifest_artifact_ref.ArtifactRef = .{ .kind = .algebraic_segment, .artifact_id = upload.artifact_id, .checksum = upload.checksum, .byte_len = upload.byte_len };
    var values: [8]catalog.FileContribution = undefined;
    for (&values, 0..) |*value, i| value.* = .{ .file = @splat(@intCast(i + 1)), .recipe = @splat(1), .name = "total", .artifact = artifact };
    const left_children = [_]catalog.Digest{ identity(values[0]), identity(values[2]) };
    const right_children = [_]catalog.Digest{ identity(values[1]), identity(values[2]) };
    values[3].owned = &left_children;
    values[4].owned = &right_children;
    const old_children = [_]catalog.Digest{ identity(values[3]), identity(values[4]) };
    values[5].owned = &old_children;
    const new_children = [_]catalog.Digest{ identity(values[3]), identity(values[6]) };
    values[7].owned = &new_children;
    var first: Index = undefined;
    try first.init(a, store, null, .none);
    defer first.deinit();
    first.counted = true;
    try first.includeRoot(identity(values[5]));
    const old_root = (try first.update(values[0..6])).?;
    const old_roots = try first.rootKeys(a);
    defer a.free(old_roots);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    first.root = old_root;
    try std.testing.expectEqual(@as(u32, 2), (try first.lookup(arena.allocator(), identity(values[2]))).?.references);
    var next: Index = undefined;
    try next.init(a, store, old_root, .none);
    defer next.deinit();
    next.counted = true;
    next.prior_roots = old_roots;
    try next.includeRoot(identity(values[7]));
    const next_root = (try next.update(values[6..8])).?;
    try std.testing.expectEqual(@as(u64, 5), next_root.records);
    next.root = next_root;
    try std.testing.expect((try next.lookup(arena.allocator(), identity(values[1]))) == null);
    try std.testing.expect((try next.lookup(arena.allocator(), identity(values[4]))) == null);
    try std.testing.expect((try next.lookup(arena.allocator(), identity(values[5]))) == null);
    try std.testing.expectEqual(@as(u32, 1), (try next.lookup(arena.allocator(), identity(values[2]))).?.references);
    const next_roots = try next.rootKeys(a);
    defer a.free(next_roots);
    var same: Index = undefined;
    try same.init(a, store, next_root, .none);
    defer same.deinit();
    same.counted = true;
    same.prior_roots = next_roots;
    try same.includeRoot(identity(values[7]));
    const reads = same.reads;
    try std.testing.expect(next_root.eql((try same.update(&.{})).?));
    try std.testing.expectEqual(reads, same.reads);
    // A new cohort may rewrite a live logical leaf without changing the
    // mathematical reduction identity. Old immutable generations stay valid.
    var rewritten_upload = try store.put("rewritten aggregate cohort");
    defer rewritten_upload.deinit(a);
    var rewritten = values[0];
    rewritten.artifact = .{ .kind = .algebraic_segment, .artifact_id = rewritten_upload.artifact_id, .checksum = rewritten_upload.checksum, .byte_len = rewritten_upload.byte_len };
    var replacement: Index = undefined;
    try replacement.init(a, store, next_root, .none);
    defer replacement.deinit();
    replacement.counted = true;
    replacement.prior_roots = next_roots;
    try replacement.includeRoot(identity(values[7]));
    replacement.root = (try replacement.update(&.{rewritten})).?;
    const replaced = (try replacement.lookup(arena.allocator(), identity(values[0]))).?;
    try std.testing.expectEqual(@as(u32, 1), replaced.references);
    try std.testing.expectEqualStrings(rewritten_upload.artifact_id, replaced.artifact.artifact_id);
    try std.testing.expectEqualStrings(upload.artifact_id, (try next.lookup(arena.allocator(), identity(values[0]))).?.artifact.artifact_id);
    // Range alias edges may change while the two reduction children stay
    // fixed. Retire only the old alias, preserving shared child references.
    var alias_parent = values[7];
    const alias_before = [_]catalog.Digest{ identity(values[3]), identity(values[6]), identity(values[2]) };
    alias_parent.owned = &alias_before;
    var alias_add: Index = undefined;
    try alias_add.init(a, store, replacement.root, .none);
    defer alias_add.deinit();
    alias_add.counted = true;
    alias_add.prior_roots = next_roots;
    try alias_add.includeRoot(identity(values[7]));
    alias_add.root = (try alias_add.update(&.{alias_parent})).?;
    try std.testing.expectEqual(@as(u32, 2), (try alias_add.lookup(arena.allocator(), identity(values[2]))).?.references);
    var fresh_alias = values[1];
    fresh_alias.file = @splat(22);
    const alias_after = [_]catalog.Digest{ identity(values[3]), identity(values[6]), identity(fresh_alias) };
    alias_parent.owned = &alias_after;
    var alias_replace: Index = undefined;
    try alias_replace.init(a, store, alias_add.root, .none);
    defer alias_replace.deinit();
    alias_replace.counted = true;
    alias_replace.prior_roots = next_roots;
    try alias_replace.includeRoot(identity(values[7]));
    alias_replace.root = (try alias_replace.update(&.{ alias_parent, fresh_alias })).?;
    try std.testing.expectEqual(@as(u32, 1), (try alias_replace.lookup(arena.allocator(), identity(values[2]))).?.references);
    try std.testing.expectEqual(@as(u32, 1), (try alias_replace.lookup(arena.allocator(), identity(fresh_alias))).?.references);
    var migration: Index = undefined;
    try migration.init(a, store, next_root, .none);
    defer migration.deinit();
    migration.counted = true;
    migration.migrating = true;
    try migration.includeRoot(identity(values[3]));
    const migrated_root = (try migration.update(&.{})).?;
    try std.testing.expectEqual(@as(u64, 3), migrated_root.records);
    migration.root = migrated_root;
    try std.testing.expectEqual(@as(u32, 1), (try migration.lookup(arena.allocator(), identity(values[2]))).?.references);
    var cycle: Index = undefined;
    try cycle.init(a, store, null, .none);
    defer cycle.deinit();
    cycle.counted = true;
    const x_children = [_]catalog.Digest{identity(values[1])};
    const y_children = [_]catalog.Digest{identity(values[0])};
    values[0].owned = &x_children;
    values[1].owned = &y_children;
    try cycle.includeRoot(identity(values[0]));
    try std.testing.expectError(error.InvalidLakeIndexCatalog, cycle.update(values[0..2]));
}
