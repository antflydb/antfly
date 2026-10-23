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

//! Persistent file and chunk maps for immutable native generations.
const std = @import("std");
const local = @import("antfly_local_sources");
const files = @import("lake_index_native_files.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const stores = @import("../serverless/artifacts/store.zig");
pub const tree = @import("../serverless/graph_segment/page_tree.zig");
const page_store = @import("../serverless/graph_segment/page_store.zig");
const A = std.mem.Allocator;
const Token = @import("antfly_cancellation").CancellationToken;

pub const Access = struct {
    store: stores.ArtifactStore,
    domain: [32]u8,
    token: Token,
    cached: ?artifacts.CachedRead = null,
    context: local.serverless_query_lake_read_context.Context = .{},
    navigation: ?*Navigation = null,
    reads: u64 = 512 * 1024 * 1024,
    writes: u64 = 512 * 1024 * 1024,
    work: ?*u64 = null,
    pages: page_store.PageStore = undefined,
    pub fn init(self: *Access, store: stores.ArtifactStore, domain: [32]u8, token: Token, cached: ?artifacts.CachedRead) void {
        self.* = .{ .store = store, .domain = domain, .token = token, .cached = cached };
    }
    pub fn backing(self: *Access) tree.Store {
        self.pages = .{ .domain = self.domain, .attempt = if (self.store.upload_scope) |scope| scope.attempt else @splat(0), .artifacts = &self.store, .cancellation = self.token, .remaining_read_bytes = &self.reads, .remaining_write_bytes = self.work orelse &self.writes, .read_cache = if (self.cached != null or self.navigation != null or self.context.execution != null) .{ .ptr = self, .read = readPage } else null };
        var result = self.pages.store();
        if (self.navigation != null) result.decoded_page = .{ .ptr = self, .read = acquirePage };
        return result;
    }
    fn acquirePage(raw: *anyopaque, a: A, ref: tree.Ref) !tree.Page {
        const self: *Access = @ptrCast(@alignCast(raw));
        try self.token.check();
        if (self.cached) |cached| try cached.context.ensureActive();
        // Hits consume the same logical read quota as misses. The independent
        // loader below must not charge this operation a second time.
        try stores.chargeReadBudget(&self.reads, ref.bytes);
        return self.navigation.?.acquirePage(a, self, ref);
    }
    fn readPage(raw: *anyopaque, a: A, store: *stores.ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, offset: u64, len: usize, _: [32]u8, token: Token, remaining: *u64) ![]u8 {
        const self: *Access = @ptrCast(@alignCast(raw));
        if (offset != 0 or len != ref.byte_len) return error.InvalidNativeLakeFiles;
        try stores.chargeReadBudget(remaining, ref.byte_len);
        return artifacts.readArtifactFor(a, store.*, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, token, self.cached, if (self.cached) |cache| cache.context else self.context);
    }
};
pub fn key(ordinal: usize) [8]u8 {
    var result: [8]u8 = undefined;
    std.mem.writeInt(u64, &result, ordinal, .big);
    return result;
}
/// Reader/build-owned immutable navigation. Page identities are content bound;
/// file descriptors are additionally bound to the exact generation directory.
/// No request authority is retained: callers check their live context first.
pub const Navigation = struct {
    const Cache = local.serverless_query_lake_decoded_cache.Cache;
    const DecodedPage = struct { page: tree.Page };
    allocator: A = std.heap.page_allocator,
    mutex: std.atomic.Mutex = .unlocked,
    pages: ?*Cache = null,
    descriptors: [32]?struct { root: tree.Ref, domain: [32]u8, file: files.File } = @splat(null),
    next_file: usize = 0,
    chunk_descriptors: [64]?struct { root: tree.Ref, domain: [32]u8, ordinal: usize, ref: artifacts.ChunkRef } = @splat(null),
    next_chunk: usize = 0,
    pub fn deinit(self: *Navigation) void {
        if (self.pages) |pages| {
            pages.deinit();
            self.allocator.destroy(pages);
        }
        for (&self.descriptors) |*entry| if (entry.*) |value| {
            self.allocator.free(value.file.path);
            entry.* = null;
        };
        for (&self.chunk_descriptors) |*entry| if (entry.*) |value| {
            self.allocator.free(value.ref.artifact_id);
            self.allocator.free(value.ref.checksum);
            entry.* = null;
        };
        self.pages = null;
    }
    /// Cached items contain only immutable decoded pages. Operation-owned access bridges
    /// carry all authority, so there is no request context to detach here.
    pub fn detach(_: *Navigation) void {}
    fn pageCache(self: *Navigation) !*Cache {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.pages == null) {
            const pages = try self.allocator.create(Cache);
            pages.* = .{ .a = self.allocator, .max_bytes = 4 * 1024 * 1024, .max_entries = 128, .max_loading_bytes = 8 * 1024 * 1024, .max_loaders = 4 };
            self.pages = pages;
        }
        return self.pages.?;
    }
    fn acquirePage(self: *Navigation, a: A, access: *Access, ref: tree.Ref) !tree.Page {
        const Check = struct {
            token: Token,
            parent: local.serverless_query_lake_read_context.Context,
            fn check(raw: *anyopaque) !void {
                const value: *@This() = @ptrCast(@alignCast(raw));
                try value.token.check();
                try value.parent.ensureActive();
            }
        };
        var check: Check = .{ .token = access.token, .parent = if (access.cached) |value| value.context else .{} };
        try Check.check(&check);
        var context = check.parent;
        context.checkpoint = .{ .ptr = &check, .check = Check.check };
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("native-navigation-decoded-page-v2");
        hash.update(&access.domain);
        hash.update(&ref.digest);
        hash.update(&ref.attempt);
        var shape: [13]u8 = undefined;
        std.mem.writeInt(u32, shape[0..4], ref.bytes, .little);
        shape[4] = ref.height;
        std.mem.writeInt(u64, shape[5..13], ref.records, .little);
        hash.update(&shape);
        const Loader = struct {
            access: *Access,
            ref: tree.Ref,
            fn load(raw: *anyopaque, item: *local.serverless_query_lake_decoded_cache.Item) !void {
                const loader: *@This() = @ptrCast(@alignCast(raw));
                const out = item.arena.allocator();
                const page = out.create(DecodedPage) catch |err| {
                    if (err == error.OutOfMemory and item.budget.isLocallyExhausted()) return error.SqlMemoryLimitExceeded;
                    return err;
                };
                page.* = .{ .page = tree.readPage(out, loader.access.backing(), loader.ref) catch |err| {
                    if (err == error.OutOfMemory and item.budget.isLocallyExhausted()) return error.SqlMemoryLimitExceeded;
                    return err;
                } };
                item.payload = .{ .extension = page };
            }
            fn release(owner: *anyopaque, item: *anyopaque) void {
                const lease: local.serverless_query_lake_decoded_cache.Lease = .{ .cache = @ptrCast(@alignCast(owner)), .item = @ptrCast(@alignCast(item)) };
                lease.release();
            }
        };
        // This loader is operation-scoped and deliberately has no navigation
        // hook, avoiding recursive flights. All verified buffers and entry views
        // belong to the bounded cache item, with no request allocator retained.
        var underlying: Access = undefined;
        underlying.init(access.store, access.domain, access.token, access.cached);
        var loader: Loader = .{ .access = &underlying, .ref = ref };
        const pages = try self.pageCache();
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        const lease = pages.acquire(digest, pages.max_loading_bytes, context, .{ .ptr = &loader, .load = Loader.load }) catch |err| {
            // Optional cache pressure must not prevent an admitted required read.
            if (err != error.SqlMemoryLimitExceeded) return err;
            try Check.check(&check);
            return tree.readPage(a, underlying.backing(), ref);
        };
        errdefer lease.release();
        try Check.check(&check);
        const decoded: *const DecodedPage = @ptrCast(@alignCast(lease.item.payload.extension));
        var page = decoded.page;
        page.lease = .{ .owner = lease.cache, .item = lease.item, .release = Loader.release };
        return page;
    }
    fn backing(self: *Navigation, access: *Access, store: stores.ArtifactStore, domain: [32]u8, token: Token, cached: ?artifacts.CachedRead) tree.Store {
        access.init(store, domain, token, cached);
        access.navigation = self;
        return access.backing();
    }
    pub fn lookup(self: *Navigation, root: files.Root, store: stores.ArtifactStore, path: []const u8, token: Token, cached: ?artifacts.CachedRead) !?files.File {
        try token.check();
        if (cached) |value| try value.context.ensureActive();
        if (root.directory == null) return root.find(path);
        {
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            defer self.mutex.unlock();
            for (self.descriptors) |entry| if (entry) |value| if (value.root.eql(root.directory.?) and std.mem.eql(u8, &value.domain, &root.domain) and std.mem.eql(u8, value.file.path, path)) {
                var result = value.file;
                result.path = path;
                return result;
            };
        }
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        var access: Access = undefined;
        var cursor = try tree.Cursor.init(arena.allocator(), self.backing(&access, store, root.domain, token, cached), root.directory, path, null);
        defer cursor.deinit();
        const row = (try cursor.next()) orelse return null;
        if (!std.mem.eql(u8, row.key, path)) return null;
        var file = try decodeFile(arena.allocator(), row.key, row.value, root.domain);
        file.path = try self.allocator.dupe(u8, path);
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const slot = self.next_file % self.descriptors.len;
        if (self.descriptors[slot]) |old| self.allocator.free(old.file.path);
        self.descriptors[slot] = .{ .root = root.directory.?, .domain = root.domain, .file = file };
        self.next_file +%= 1;
        file.path = path;
        return file;
    }
    /// A single seek walks adjacent chunk descriptors, including cross-page
    /// ranges. Returned identities are owned by the operation arena.
    pub fn chunks(self: *Navigation, a: A, root: files.Root, store: stores.ArtifactStore, file: files.File, first: usize, count: usize, token: Token, cached: ?artifacts.CachedRead) ![]const artifacts.ChunkRef {
        try token.check();
        if (cached) |value| try value.context.ensureActive();
        const total = file.bytes / files.chunk_bytes + @intFromBool(file.bytes % files.chunk_bytes != 0);
        if (first > total or count > total - first) return error.InvalidNativeLakeFiles;
        const result = try a.alloc(artifacts.ChunkRef, count);
        if (count == 0) return result;
        if (file.chunk_root == null) {
            @memcpy(result, file.chunks[first..][0..count]);
            return result;
        }
        {
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            defer self.mutex.unlock();
            if (count <= self.chunk_descriptors.len) {
                const hits = for (result, first..) |*ref, ordinal| {
                    const found = for (self.chunk_descriptors) |entry| {
                        if (entry) |value| if (value.root.eql(file.chunk_root.?) and value.ordinal == ordinal and std.mem.eql(u8, &value.domain, &root.domain)) break value.ref;
                    } else break false;
                    if (found.byte_len != @min(files.chunk_bytes, file.bytes - @as(u64, @intCast(ordinal)) * files.chunk_bytes)) return error.InvalidNativeLakeFiles;
                    ref.* = found;
                } else true;
                if (hits) {
                    // Cache entries are private; operation-owned IDs survive later
                    // cache eviction and concurrent payload prefetch.
                    for (result) |*ref| {
                        ref.artifact_id = try a.dupe(u8, ref.artifact_id);
                        ref.checksum = try a.dupe(u8, ref.checksum);
                    }
                    return result;
                }
            }
        }
        var access: Access = undefined;
        var cursor = try tree.Cursor.init(a, self.backing(&access, store, root.domain, token, cached), file.chunk_root, &key(first), null);
        defer cursor.deinit();
        for (result, first..) |*ref, ordinal| {
            const row = (try cursor.next()) orelse return error.InvalidNativeLakeFiles;
            if (!std.mem.eql(u8, row.key, &key(ordinal))) return error.InvalidNativeLakeFiles;
            ref.* = try decodeChunk(a, row.value, root.domain);
            if (ref.byte_len != @min(files.chunk_bytes, file.bytes - @as(u64, @intCast(ordinal)) * files.chunk_bytes)) return error.InvalidNativeLakeFiles;
            if (count <= self.chunk_descriptors.len) {
                const id = try self.allocator.dupe(u8, ref.artifact_id);
                errdefer self.allocator.free(id);
                const checksum = try self.allocator.dupe(u8, ref.checksum);
                @import("antfly_platform").sync.lockYielding(&self.mutex);
                defer self.mutex.unlock();
                const slot = self.next_chunk % self.chunk_descriptors.len;
                if (self.chunk_descriptors[slot]) |old| {
                    self.allocator.free(old.ref.artifact_id);
                    self.allocator.free(old.ref.checksum);
                }
                self.chunk_descriptors[slot] = .{ .root = file.chunk_root.?, .domain = root.domain, .ordinal = ordinal, .ref = .{ .artifact_id = id, .checksum = checksum, .byte_len = ref.byte_len } };
                self.next_chunk +%= 1;
            }
        }
        return result;
    }
};
pub fn chunk(a: A, access: *Access, file: files.File, ordinal: usize) !artifacts.ChunkRef {
    const expected = file.bytes / files.chunk_bytes + @intFromBool(file.bytes % files.chunk_bytes != 0);
    if (ordinal >= expected) return error.InvalidNativeLakeFiles;
    if (file.chunk_root == null) return file.chunks[ordinal];
    var cursor = try tree.Cursor.init(a, access.backing(), file.chunk_root, &key(ordinal), null);
    defer cursor.deinit();
    const row = (try cursor.next()) orelse return error.InvalidNativeLakeFiles;
    if (!std.mem.eql(u8, row.key, &key(ordinal))) return error.InvalidNativeLakeFiles;
    const ref = try decodeChunk(a, row.value, access.domain);
    if (ref.byte_len != @min(files.chunk_bytes, file.bytes - @as(u64, @intCast(ordinal)) * files.chunk_bytes)) return error.InvalidNativeLakeFiles;
    return ref;
}
pub fn decodeChunk(a: A, bytes: []const u8, domain: [32]u8) !artifacts.ChunkRef {
    const ref = try std.json.parseFromSliceLeaky(artifacts.ChunkRef, a, bytes, .{ .allocate = .alloc_always });
    try stores.validateSha256ArtifactIdentity(ref.artifact_id, ref.checksum);
    const scope = (try stores.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.InvalidNativeLakeFiles;
    if (ref.byte_len == 0 or ref.byte_len > files.chunk_bytes or !std.mem.eql(u8, &scope.domain, &domain)) return error.InvalidNativeLakeFiles;
    return ref;
}
pub fn decodeFile(a: A, path: []const u8, bytes: []const u8, domain: [32]u8) !files.File {
    const file = try std.json.parseFromSliceLeaky(files.File, a, bytes, .{ .allocate = .alloc_always });
    if (!std.mem.eql(u8, path, file.path) or file.chunks.len != 0 or file.origin != null) return error.InvalidNativeLakeFiles;
    try file.validate(domain);
    return file;
}
/// Return only value-owned references. The path borrows the lookup argument;
/// no parsed descriptors or page buffers escape this small operation arena.
pub fn lookup(root: files.Root, store: stores.ArtifactStore, path: []const u8, token: Token, cached: ?artifacts.CachedRead) !?files.File {
    if (root.directory == null) return root.find(path);
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var access: Access = undefined;
    access.init(store, root.domain, token, cached);
    var cursor = try tree.Cursor.init(a, access.backing(), root.directory, path, null);
    defer cursor.deinit();
    const row = (try cursor.next()) orelse return null;
    if (!std.mem.eql(u8, path, row.key)) return null;
    var file = try decodeFile(a, row.key, row.value, root.domain);
    file.path = path;
    file.chunks = &.{};
    return file;
}
pub fn entries(a: A, root: files.Root, store: stores.ArtifactStore, token: Token) ![]const files.File {
    if (root.directory == null) return root.files;
    var access: Access = undefined;
    access.init(store, root.domain, token, null);
    var cursor = try tree.Cursor.init(a, access.backing(), root.directory, "", null);
    defer cursor.deinit();
    const result = try a.alloc(files.File, root.file_count);
    var index: usize = 0;
    while (try cursor.next()) |row| {
        if (index == result.len) return error.InvalidNativeLakeFiles;
        result[index] = try decodeFile(a, row.key, row.value, root.domain);
        index += 1;
    }
    if (index != result.len) return error.InvalidNativeLakeFiles;
    return result;
}
/// Immediate virtual children. Seek only the requested directory prefix and
/// inspect keys; listing never decodes every native file/chunk descriptor.
pub fn names(a: A, root: files.Root, store: stores.ArtifactStore, relative: []const u8, directories: bool, token: Token, cached: ?artifacts.CachedRead) ![][]u8 {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const prefix = if (relative.len == 0) "" else try std.fmt.allocPrint(arena.allocator(), "{s}/", .{relative});
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    defer seen.deinit(a);
    var result: std.ArrayList([]u8) = .empty;
    errdefer {
        for (result.items) |name| a.free(name);
        result.deinit(a);
    }
    const Collect = struct {
        fn add(alloc: A, output: *std.ArrayList([]u8), found: *std.StringHashMapUnmanaged(void), stem: []const u8, path: []const u8, dirs: bool) !void {
            if (!std.mem.startsWith(u8, path, stem)) return;
            const rest = path[stem.len..];
            const slash = std.mem.indexOfScalar(u8, rest, '/');
            if (dirs != (slash != null) or rest.len == 0) return;
            const name = if (slash) |end| rest[0..end] else rest;
            if (found.contains(name)) return;
            const copy = try alloc.dupe(u8, name);
            output.append(alloc, copy) catch |err| {
                alloc.free(copy);
                return err;
            };
            try found.put(alloc, copy, {});
        }
    };
    if (root.directory) |directory| {
        var access: Access = undefined;
        access.init(store, root.domain, token, cached);
        const upper = if (prefix.len == 0) null else bound: {
            const bytes = try arena.allocator().dupe(u8, prefix);
            bytes[bytes.len - 1] += 1;
            break :bound bytes;
        };
        var cursor = try tree.Cursor.init(arena.allocator(), access.backing(), directory, prefix, upper);
        defer cursor.deinit();
        while (try cursor.next()) |row| try Collect.add(a, &result, &seen, prefix, row.key, directories);
    } else for (root.files) |file| {
        try token.check();
        try Collect.add(a, &result, &seen, prefix, file.path, directories);
    }
    std.mem.sort([]u8, result.items, {}, struct {
        fn less(_: void, left: []u8, right: []u8) bool {
            return std.mem.lessThan(u8, left, right);
        }
    }.less);
    return result.toOwnedSlice(a);
}

/// The chunk tree keeps immutable prefixes by reference. Only a partial tail
/// and appended blocks are read/uploaded; page edits are flushed in bounded waves.
pub fn publishPrefix(a: A, out: A, source: local.storage_lsm_backend.Storage, physical: []const u8, relative: []const u8, size: u64, store: *stores.ArtifactStore, token: Token, budget: files.OutputBudget, reuse: ?files.Reuse) !files.File {
    const count = std.math.cast(usize, size / files.chunk_bytes + @intFromBool(size % files.chunk_bytes != 0)) orelse return error.NativeLakeFilesTooLarge;
    if (count > files.max_chunks) return error.NativeLakeFilesTooLarge;
    try stores.chargeReadBudget(budget.generation, size);
    const scope = store.upload_scope orelse return error.InvalidArtifactUploadScope;
    var access: Access = undefined;
    access.init(store.*, scope.domain, token, null);
    access.work = budget.work;
    const old = if (reuse) |previous| try previous.lookup(store.*, relative, token) else null;
    var root: ?tree.Ref = null;
    var begin: usize = 0;
    if (old) |prior| if (prior.chunk_root != null and prior.bytes <= size) {
        root = prior.chunk_root;
        begin = @intCast(prior.bytes / files.chunk_bytes);
        if (prior.bytes == size) begin = count;
    };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var pending: std.ArrayList(tree.Mutation) = .empty;
    var ordinal: usize = if (root != null) begin else 0;
    while (ordinal < count) : (ordinal += 1) {
        try token.check();
        const offset = @as(u64, @intCast(ordinal)) * files.chunk_bytes;
        const length = @min(files.chunk_bytes, size - offset);
        const ref = if (old) |prior| retained: {
            if (offset >= prior.bytes) break :retained null;
            const previous = if (reuse.?.navigation) |navigation| (try navigation.chunks(arena.allocator(), reuse.?.root, store.*, prior, ordinal, 1, token, null))[0] else try chunk(arena.allocator(), &access, prior, ordinal);
            break :retained if (previous.byte_len == length) previous else null;
        } else null;
        const retained = ref orelse upload: {
            const bytes = try source.readFileRangeAlloc(a, physical, offset, @intCast(length));
            defer a.free(bytes);
            if (budget.work) |remaining| try stores.chargeReadBudget(remaining, bytes.len);
            var uploaded = try store.putWithCancellation(bytes, token);
            defer uploaded.deinit(store.allocator);
            break :upload artifacts.ChunkRef{ .artifact_id = try arena.allocator().dupe(u8, uploaded.artifact_id), .checksum = try arena.allocator().dupe(u8, uploaded.checksum), .byte_len = uploaded.byte_len };
        };
        const encoded = try std.json.Stringify.valueAlloc(arena.allocator(), retained, .{});
        try pending.append(arena.allocator(), .{ .key = try arena.allocator().dupe(u8, &key(ordinal)), .value = encoded });
        if (pending.items.len == 256 or ordinal + 1 == count) {
            root = try tree.apply(a, access.backing(), root, pending.items);
            _ = arena.reset(.free_all);
            pending = .empty;
        }
    }
    return .{ .path = try out.dupe(u8, relative), .bytes = size, .chunk_root = root, .origin = if (old) |prior| prior.origin else null };
}
pub const FileChange = struct { path: []const u8, file: ?files.File };
/// Apply explicit, sorted file mutations. Counters change with the touched
/// descriptors; no complete predecessor directory is read or materialized.
pub fn applyChanges(a: A, store: *stores.ArtifactStore, prior: files.Root, changes: []const FileChange, token: Token, work: ?*u64) !files.Root {
    try prior.validate();
    if (prior.version != 2) return error.InvalidNativeLakeFiles;
    const scope = store.upload_scope orelse return error.InvalidArtifactUploadScope;
    if (!std.mem.eql(u8, &scope.domain, &prior.domain)) return error.InvalidNativeLakeFiles;
    if (changes.len == 0) return prior;
    var navigation: Navigation = .{ .allocator = a };
    defer navigation.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const edits = try arena.allocator().alloc(tree.Mutation, changes.len);
    var result = prior;
    for (changes, edits, 0..) |change, *edit, index| {
        try token.check();
        if (index != 0 and std.mem.order(u8, changes[index - 1].path, change.path) != .lt) return error.InvalidNativeLakeFiles;
        const old = existing: {
            if (change.file) |file| if (file.origin) |origin| if (origin.matches(prior, change.path)) break :existing @as(?files.File, .{ .path = change.path, .bytes = origin.bytes, .chunk_root = origin.chunk_root });
            break :existing try navigation.lookup(prior, store.*, change.path, token, null);
        };
        if (old) |file| {
            result.file_count = std.math.sub(u32, result.file_count, 1) catch return error.InvalidNativeLakeFiles;
            result.chunk_count = std.math.sub(u64, result.chunk_count, file.bytes / files.chunk_bytes + @intFromBool(file.bytes % files.chunk_bytes != 0)) catch return error.InvalidNativeLakeFiles;
        }
        edit.* = .{ .key = change.path, .value = null };
        if (change.file) |file| {
            if (!std.mem.eql(u8, change.path, file.path) or file.chunks.len != 0) return error.InvalidNativeLakeFiles;
            try file.validate(prior.domain);
            result.file_count = std.math.add(u32, result.file_count, 1) catch return error.NativeLakeFilesTooLarge;
            result.chunk_count = std.math.add(u64, result.chunk_count, file.bytes / files.chunk_bytes + @intFromBool(file.bytes % files.chunk_bytes != 0)) catch return error.NativeLakeFilesTooLarge;
            edit.value = try std.json.Stringify.valueAlloc(arena.allocator(), file, .{});
        }
    }
    var access: Access = undefined;
    access.init(store.*, prior.domain, token, null);
    access.work = work;
    result.directory = try tree.apply(a, access.backing(), prior.directory, edits);
    try result.validate();
    return result;
}

/// File descriptors are small; unchanged descriptors produce no page writes.
/// Legacy inline chunks migrate once, while current generations path-copy only
/// changed files and retain unchanged chunk-map subtrees across checkpoints.
pub fn persist(a: A, out: A, store: *stores.ArtifactStore, staging: files.Root, previous: ?files.Root, token: Token, work: ?*u64) !files.Root {
    try staging.validate();
    const scope = store.upload_scope orelse return error.InvalidArtifactUploadScope;
    if (!std.mem.eql(u8, &scope.domain, &staging.domain)) return error.InvalidNativeLakeFiles;
    if (previous) |old| if (!std.mem.eql(u8, &old.domain, &staging.domain)) return error.InvalidNativeLakeFiles;
    var access: Access = undefined;
    access.init(store.*, staging.domain, token, null);
    access.work = work;
    var root = if (previous) |old| old.directory else null;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var navigation: Navigation = .{ .allocator = a };
    defer navigation.deinit();
    var known: std.ArrayList([]const u8) = .empty;
    var total: u64 = 0;
    var edits: std.ArrayList(FileChange) = .empty;
    var retained_chunks: u64 = 0;
    for (staging.files) |input| {
        try token.check();
        var file = input;
        total += file.bytes / files.chunk_bytes + @intFromBool(file.bytes % files.chunk_bytes != 0);
        if (total > files.max_chunks) return error.NativeLakeFilesTooLarge;
        const old = if (previous) |prior| authenticated: {
            if (file.origin) |origin| if (origin.matches(prior, file.path)) break :authenticated files.File{ .path = file.path, .bytes = origin.bytes, .chunk_root = origin.chunk_root };
            break :authenticated try navigation.lookup(prior, store.*, file.path, token, null);
        } else null;
        if (root != null) if (old) |value| {
            try known.append(arena.allocator(), file.path);
            retained_chunks += value.bytes / files.chunk_bytes + @intFromBool(value.bytes % files.chunk_bytes != 0);
        };
        if (file.chunk_root == null and file.chunks.len != 0) if (old) |prior| {
            if (prior.bytes == file.bytes and prior.chunk_root != null) {
                var scratch = std.heap.ArenaAllocator.init(a);
                defer scratch.deinit();
                const refs = try navigation.chunks(scratch.allocator(), previous.?, store.*, prior, 0, file.chunks.len, token, null);
                const same = for (file.chunks, refs) |ref, previous_ref| {
                    if (ref.byte_len != previous_ref.byte_len or !std.mem.eql(u8, ref.artifact_id, previous_ref.artifact_id) or !std.mem.eql(u8, ref.checksum, previous_ref.checksum)) break false;
                } else true;
                if (same) file.chunk_root = prior.chunk_root;
            }
        };
        if (file.chunk_root == null and file.chunks.len != 0) {
            const changes = try arena.allocator().alloc(tree.Mutation, file.chunks.len);
            for (file.chunks, changes, 0..) |ref, *change, ordinal| change.* = .{ .key = try arena.allocator().dupe(u8, &key(ordinal)), .value = try std.json.Stringify.valueAlloc(arena.allocator(), ref, .{}) };
            file.chunk_root = try tree.apply(a, access.backing(), null, changes);
        }
        file.chunks = &.{};
        if (root != null) if (old) |value| if (value.bytes == file.bytes and ((value.chunk_root == null and file.chunk_root == null) or (value.chunk_root != null and file.chunk_root != null and value.chunk_root.?.eql(file.chunk_root.?)))) continue;
        try edits.append(arena.allocator(), .{ .path = file.path, .file = file });
    }
    // Every retained name was authenticated by its point lookup above. Counts
    // preserve complete old subtrees without fetching or decoding descendants;
    // omitted files disappear through this structural retain operation.
    root = try tree.retainKnown(a, access.backing(), root, if (root != null) known.items else &.{});
    const retained: files.Root = .{ .version = 2, .domain = staging.domain, .directory = root, .file_count = @intCast(known.items.len), .chunk_count = retained_chunks };
    const result = try applyChanges(a, store, retained, edits.items, token, work);
    if (result.file_count != staging.files.len or result.chunk_count != total) return error.InvalidNativeLakeFiles;
    _ = out;
    return result;
}

test "external lake paged native generations retain chunk prefixes without repeated serialization or source reads" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    var temporary = try local.common_test_directory.TestDirectory.init("native-generation-map");
    defer temporary.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, temporary.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(7), .attempt = @splat(3) };
    const payload = try out.alloc(u8, files.chunk_bytes);
    @memset(payload, 42);
    var uploaded = try store.put(payload);
    defer uploaded.deinit(a);
    const refs = try out.alloc(artifacts.ChunkRef, 1024);
    @memset(refs, .{ .artifact_id = uploaded.artifact_id, .checksum = uploaded.checksum, .byte_len = uploaded.byte_len });
    const length: u64 = refs.len * files.chunk_bytes;
    const legacy: files.Root = .{ .domain = @splat(7), .files = &.{.{ .path = "run", .bytes = length, .chunks = refs }} };
    const paged = try persist(a, out, &store, legacy, null, .none, null);
    try std.testing.expectEqual(@as(usize, 0), paged.files.len);
    try std.testing.expectEqual(@as(u64, 1024), paged.chunk_count);
    try std.testing.expect((try std.json.Stringify.valueAlloc(out, paged, .{})).len < 1024);
    const Counter = struct {
        var original: stores.ArtifactStore = undefined;
        var calls: usize = 0;
        fn read(_: *anyopaque, alloc: A, id: []const u8, offset: u64, len: usize, token: Token) ![]u8 {
            calls += 1;
            return original.getRangeAllocWithCancellationUsingAllocator(alloc, id, offset, len, token);
        }
    };
    Counter.original = store;
    Counter.calls = 0;
    var counted_vtable = store.vtable.*;
    counted_vtable.get_range_alloc_with_cancellation = Counter.read;
    store.vtable = &counted_vtable;
    var work: u64 = 0;
    for (0..8) |_| {
        var generation = length;
        const same = try files.publishPrefix(a, out, undefined, "unread", "run", length, &store, .none, files.OutputBudget{ .generation = &generation, .work = &work }, .{ .root = paged });
        const before_finalize = Counter.calls;
        const next = try persist(a, out, &store, .{ .domain = legacy.domain, .files = &.{same} }, paged, .none, &work);
        try std.testing.expectEqual(before_finalize, Counter.calls);
        try std.testing.expect(std.mem.indexOf(u8, try std.json.Stringify.valueAlloc(out, same, .{}), "origin") == null);
        try std.testing.expect(next.directory.?.eql(paged.directory.?));
        try std.testing.expectEqual(@as(u64, 0), work);
        try std.testing.expectEqual(@as(u64, 0), generation);
    }
    var tail = try store.put("z");
    defer tail.deinit(a);
    const source_chunks = try out.alloc(artifacts.ChunkRef, refs.len + 1);
    @memcpy(source_chunks[0..refs.len], refs);
    source_chunks[refs.len] = .{ .artifact_id = tail.artifact_id, .checksum = tail.checksum, .byte_len = 1 };
    var source: files.Reader = .{ .root = .{ .domain = legacy.domain, .files = &.{.{ .path = "run", .bytes = length + 1, .chunks = source_chunks }} }, .prefix = "/candidate", .store = store, .context = .{}, .scratch = a };
    defer source.deinit();
    var generation = length + 1;
    work = 1024 * 1024;
    const appended = try files.publishPrefix(a, out, source.storage(), "/candidate/run", "run", length + 1, &store, .none, files.OutputBudget{ .generation = &generation, .work = &work }, .{ .root = paged });
    try std.testing.expectEqual(@as(u64, 1025), appended.chunk_root.?.records);
    const next = try persist(a, out, &store, .{ .domain = legacy.domain, .files = &.{appended} }, paged, .none, &work);
    var access: Access = undefined;
    access.init(store, legacy.domain, .none, null);
    const old_file = (try lookup(paged, store, "run", .none, null)).?;
    const new_file = (try lookup(next, store, "run", .none, null)).?;
    try std.testing.expectEqualStrings((try chunk(out, &access, old_file, 0)).artifact_id, (try chunk(out, &access, new_file, 0)).artifact_id);
    var reader: files.Reader = .{ .root = next, .prefix = "/persisted", .store = store, .context = .{}, .scratch = a, .max_block_bytes = 1024 * 1024 };
    defer reader.deinit();
    const bytes = try reader.storage().readFileRangeAlloc(a, "/persisted/run", length, 1);
    defer a.free(bytes);
    try std.testing.expectEqualStrings("z", bytes);
    var again: [1]u8 = undefined;
    const warm_calls = Counter.calls;
    try reader.storage().readFileRangeInto(a, "/persisted/run", length, &again);
    try std.testing.expectEqual(warm_calls, Counter.calls);
    // One navigation cache can be reused by a producer across checkpoints.
    // The same path in a changed generation must acquire a new descriptor.
    var navigation: Navigation = .{};
    defer navigation.deinit();
    try std.testing.expectEqual(length, (try navigation.lookup(paged, store, "run", .none, null)).?.bytes);
    try std.testing.expectEqual(length + 1, (try navigation.lookup(next, store, "run", .none, null)).?.bytes);
    try std.testing.expectEqualStrings("z", &again);
    const erased = try applyChanges(a, &store, next, &.{.{ .path = "run", .file = null }}, .none, null);
    try std.testing.expect(erased.directory == null and erased.file_count == 0 and erased.chunk_count == 0);
    const retired = try persist(a, out, &store, .{ .domain = legacy.domain }, next, .none, null);
    try std.testing.expect(retired.directory == null);
    try std.testing.expect((try lookup(next, store, "run", .none, null)) != null);
    try std.testing.checkAllAllocationFailures(a, navigationFaultScenario, .{ store, next, length });
    const nested = try persist(a, out, &store, .{ .domain = legacy.domain, .files = &.{
        .{ .path = "nested/manifest.bin", .bytes = 0 },
        .{ .path = "nested/runs/5.tbl", .bytes = new_file.bytes, .chunk_root = new_file.chunk_root },
        new_file,
    } }, next, .none, null);
    var virtual: files.Reader = .{ .root = nested, .prefix = "/virtual", .store = store, .context = .{}, .scratch = a, .navigation = .{ .allocator = a } };
    defer virtual.deinit();
    try std.testing.expectEqual(2 * (length + 1), try virtual.storage().treeBytes(a, "/virtual"));
    const candidate_path = try std.fs.path.join(a, &.{ temporary.path(), "candidate" });
    defer a.free(candidate_path);
    const overlay = try files.Overlay.create(a, nested, store, candidate_path, .{}, .none);
    defer overlay.release();
    try std.testing.expectEqual(2 * (length + 1), try overlay.storage().treeBytes(a, candidate_path));
    const subtree = try std.fs.path.join(a, &.{ candidate_path, "nested" });
    defer a.free(subtree);
    try overlay.storage().deleteTree(subtree);
    try std.testing.expectEqual(length + 1, try overlay.storage().treeBytes(a, candidate_path));
}

fn navigationFaultScenario(backing: A, source: stores.ArtifactStore, root: files.Root, offset: u64) !void {
    // Arena growth must have the same allocation count for every injected run;
    // the backing allocator may otherwise resize in place depending on address.
    var vtable = backing.vtable.*;
    vtable.resize = A.noResize;
    vtable.remap = A.noRemap;
    const a: A = .{ .ptr = backing.ptr, .vtable = &vtable };
    var reader: files.Reader = .{ .root = root, .prefix = "/fault", .store = source, .context = .{}, .scratch = a, .navigation = .{ .allocator = a }, .max_block_bytes = 1024 * 1024 };
    defer reader.deinit();
    const bytes = try reader.storage().readFileRangeAlloc(a, "/fault/run", offset, 1);
    defer a.free(bytes);
    try std.testing.expectEqualStrings("z", bytes);
    var store = source;
    const erased = try applyChanges(a, &store, root, &.{.{ .path = "run", .file = null }}, .none, null);
    try std.testing.expect(erased.directory == null);
}

test "external lake native navigation overlaps cold pages shares flights and checks warm authority" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var temporary = try local.common_test_directory.TestDirectory.init("native-navigation-flights");
    defer temporary.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, temporary.path());
    defer fs.deinit();
    var original = fs.artifactStore();
    original.upload_scope = .{ .domain = @splat(7), .attempt = @splat(3) };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const paths = [_][]const u8{ "first", "second" };
    var roots: [2]files.Root = undefined;
    for (&roots, paths) |*root, path| root.* = try persist(a, arena.allocator(), &original, .{ .domain = @splat(7), .files = &.{.{ .path = path, .bytes = 0 }} }, null, .none, null);
    const Worker = struct {
        var active: *@This() = undefined;
        original: stores.ArtifactStore,
        navigation: *Navigation,
        cache: *local.serverless_query_lake_serving_cache.Cache,
        gate: std.Io.Event = .unset,
        entered: std.Io.Event = .unset,
        calls: std.atomic.Value(usize) = .init(0),
        canceled: std.atomic.Value(bool) = .init(false),
        fn read(_: *anyopaque, alloc: A, id: []const u8, length: usize, token: Token) ![]u8 {
            const self = active;
            if (self.calls.fetchAdd(1, .acq_rel) + 1 == 2) self.entered.set(std.testing.io);
            try self.gate.wait(std.testing.io);
            return self.original.getRangeAllocWithCancellationUsingAllocator(alloc, id, 0, length, token);
        }
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.canceled.load(.acquire)) return error.DeadlineExceeded;
        }
        fn options(self: *@This(), cancelable: bool) artifacts.CachedRead {
            return .{ .cache = self.cache, .scope = @splat(9), .context = .{ .io = std.testing.io, .checkpoint = if (cancelable) .{ .ptr = self, .check = check } else null } };
        }
        fn run(self: *@This(), store: stores.ArtifactStore, root: files.Root, path: []const u8, cancelable: bool) anyerror!void {
            const file = (try self.navigation.lookup(root, store, path, .none, self.options(cancelable))).?;
            try std.testing.expectEqualStrings(path, file.path);
            try std.testing.expectEqual(@as(u64, 0), file.bytes);
        }
    };
    var navigation: Navigation = .{ .allocator = a };
    defer navigation.deinit();
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    var worker: Worker = .{ .original = original, .navigation = &navigation, .cache = &cache };
    Worker.active = &worker;
    var vtable = original.vtable.*;
    vtable.get_bounded_alloc_with_cancellation = Worker.read;
    var store = original;
    store.vtable = &vtable;
    var left = try io.concurrent(Worker.run, .{ &worker, store, roots[0], paths[0], false });
    defer {
        worker.gate.set(io);
        left.await(io) catch {};
    }
    var right = try io.concurrent(Worker.run, .{ &worker, store, roots[1], paths[1], false });
    defer {
        worker.gate.set(io);
        right.await(io) catch {};
    }
    try worker.entered.waitTimeout(io, .{ .duration = .{ .clock = .awake, .raw = .fromSeconds(5) } });
    var same = try io.concurrent(Worker.run, .{ &worker, store, roots[0], paths[0], false });
    defer {
        worker.gate.set(io);
        same.await(io) catch {};
    }
    var canceled = try io.concurrent(Worker.run, .{ &worker, store, roots[0], paths[0], true });
    defer {
        worker.gate.set(io);
        canceled.await(io) catch {};
    }
    const pages = try navigation.pageCache();
    var joined = false;
    for (0..5000) |_| {
        @import("antfly_platform").sync.lockYielding(&pages.mutex);
        var count: usize = 0;
        var flights = pages.flights.valueIterator();
        while (flights.next()) |flight| count += flight.*.refs;
        const ready = pages.flights.count() == 2 and count == 4;
        const admitted = pages.loading_bytes;
        pages.mutex.unlock();
        if (ready) {
            try std.testing.expect(admitted <= pages.max_loading_bytes);
            joined = true;
            break;
        }
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(joined);
    worker.canceled.store(true, .release);
    try std.testing.expectError(error.DeadlineExceeded, canceled.await(io));
    worker.gate.set(io);
    try left.await(io);
    try right.await(io);
    try same.await(io);
    try std.testing.expectEqual(@as(usize, 2), worker.calls.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), pages.loading_bytes);
    try std.testing.expect(pages.bytes <= pages.max_bytes);
    try std.testing.expectError(error.DeadlineExceeded, worker.run(store, roots[0], paths[0], true));
    try worker.run(store, roots[0], paths[0], false);
    try std.testing.expectEqual(@as(usize, 2), worker.calls.load(.acquire));
    std.debug.print("native navigation cold pages: overlapping=2 shared_readers=3 provider_reads={}\n", .{worker.calls.load(.acquire)});
}

test "external lake native decoded navigation leases avoid warm copies preserve quotas and authenticate shape" {
    const a = std.testing.allocator;
    var temporary = try local.common_test_directory.TestDirectory.init("native-navigation-leases");
    defer temporary.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, temporary.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(7), .attempt = @splat(3) };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const root = try persist(a, arena.allocator(), &store, .{ .domain = @splat(7), .files = &.{.{ .path = "file", .bytes = 0 }} }, null, .none, null);
    var navigation: Navigation = .{ .allocator = a };
    defer navigation.deinit();
    var access: Access = undefined;
    const backing = navigation.backing(&access, store, root.domain, .none, null);
    var cold = try tree.readPage(a, backing, root.directory.?);
    defer cold.deinit(a);
    try std.testing.expect(cold.lease != null);
    // Warm immutable views require no request allocation or repeated parse.
    var failed = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    const before = access.reads;
    var warm = try tree.readPage(failed.allocator(), backing, root.directory.?);
    defer warm.deinit(failed.allocator());
    try std.testing.expect(cold.bytes.ptr == warm.bytes.ptr);
    try std.testing.expect(cold.entries.ptr == warm.entries.ptr);
    try std.testing.expectEqual(before - root.directory.?.bytes, access.reads);
    access.reads = 0;
    try std.testing.expectError(error.ArtifactReadBudgetExceeded, tree.readPage(a, backing, root.directory.?));
    access.reads = 512 * 1024 * 1024;
    var wrong = root.directory.?;
    wrong.records += 1;
    try std.testing.expectError(error.InvalidGraphPage, tree.readPage(a, backing, wrong));
    access.domain = @splat(8);
    try std.testing.expectError(error.FileNotFound, tree.readPage(a, access.backing(), root.directory.?));
    // Optional residency pressure cannot invalidate already pinned views.
    const cache = try navigation.pageCache();
    cache.max_entries = 0;
    try std.testing.expectEqualStrings("file", warm.entries[0].key);
    // Declined cache workspace preserves the required, admitted uncached read.
    var pressure: Navigation = .{ .allocator = a };
    defer pressure.deinit();
    (try pressure.pageCache()).max_loading_bytes = 1;
    var uncached_access: Access = undefined;
    var uncached = try tree.readPage(a, pressure.backing(&uncached_access, store, root.domain, .none, null), root.directory.?);
    defer uncached.deinit(a);
    try std.testing.expect(uncached.lease == null);
    try std.testing.expectEqualStrings("file", uncached.entries[0].key);
    try std.testing.expectEqual(512 * 1024 * 1024 - @as(u64, root.directory.?.bytes), uncached_access.reads);
}
