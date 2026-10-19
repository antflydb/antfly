// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
    reads: u64 = 512 * 1024 * 1024,
    writes: u64 = 512 * 1024 * 1024,
    work: ?*u64 = null,
    pages: page_store.PageStore = undefined,
    pub fn init(self: *Access, store: stores.ArtifactStore, domain: [32]u8, token: Token, cached: ?artifacts.CachedRead) void {
        self.* = .{ .store = store, .domain = domain, .token = token, .cached = cached };
    }
    pub fn backing(self: *Access) tree.Store {
        self.pages = .{ .domain = self.domain, .attempt = if (self.store.upload_scope) |scope| scope.attempt else @splat(0), .artifacts = &self.store, .cancellation = self.token, .remaining_read_bytes = &self.reads, .remaining_write_bytes = self.work orelse &self.writes, .read_cache = if (self.cached != null) .{ .ptr = self, .read = readPage } else null };
        return self.pages.store();
    }
    fn readPage(raw: *anyopaque, a: A, store: *stores.ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, offset: u64, len: usize, _: [32]u8, token: Token, remaining: *u64) ![]u8 {
        const self: *Access = @ptrCast(@alignCast(raw));
        if (offset != 0 or len != ref.byte_len) return error.InvalidNativeLakeFiles;
        try stores.chargeReadBudget(remaining, ref.byte_len);
        return artifacts.readArtifact(a, store.*, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, token, self.cached);
    }
};
fn key(ordinal: usize) [8]u8 {
    var result: [8]u8 = undefined;
    std.mem.writeInt(u64, &result, ordinal, .big);
    return result;
}
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
    if (!std.mem.eql(u8, path, file.path) or file.chunks.len != 0) return error.InvalidNativeLakeFiles;
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
            const previous = try chunk(arena.allocator(), &access, prior, ordinal);
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
    return .{ .path = try out.dupe(u8, relative), .bytes = size, .chunk_root = root };
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
    const prior = if (previous) |old| try entries(arena.allocator(), old, store.*, token) else &.{};
    var total: u64 = 0;
    var edits: std.ArrayList(tree.Mutation) = .empty;
    for (staging.files) |input| {
        try token.check();
        var file = input;
        total += file.bytes / files.chunk_bytes + @intFromBool(file.bytes % files.chunk_bytes != 0);
        if (total > files.max_chunks) return error.NativeLakeFilesTooLarge;
        if (file.chunk_root == null and file.chunks.len != 0) {
            const prior_file = (files.Root{ .domain = staging.domain, .files = prior }).find(file.path);
            if (prior_file) |old| if (old.bytes == file.bytes and old.chunk_root != null) {
                var scratch = std.heap.ArenaAllocator.init(a);
                defer scratch.deinit();
                const same = for (file.chunks, 0..) |ref, ordinal| {
                    _ = scratch.reset(.retain_capacity);
                    const previous_ref = try chunk(scratch.allocator(), &access, old, ordinal);
                    if (ref.byte_len != previous_ref.byte_len or !std.mem.eql(u8, ref.artifact_id, previous_ref.artifact_id) or !std.mem.eql(u8, ref.checksum, previous_ref.checksum)) break false;
                } else true;
                if (same) file.chunk_root = old.chunk_root;
            };
        }
        if (file.chunk_root == null and file.chunks.len != 0) {
            const changes = try arena.allocator().alloc(tree.Mutation, file.chunks.len);
            for (file.chunks, changes, 0..) |ref, *change, ordinal| change.* = .{ .key = try arena.allocator().dupe(u8, &key(ordinal)), .value = try std.json.Stringify.valueAlloc(arena.allocator(), ref, .{}) };
            file.chunk_root = try tree.apply(a, access.backing(), null, changes);
        }
        file.chunks = &.{};
        const old = (files.Root{ .domain = staging.domain, .files = prior }).find(file.path);
        if (old) |value| if (value.bytes == file.bytes and value.chunk_root != null and file.chunk_root != null and value.chunk_root.?.eql(file.chunk_root.?)) continue;
        const encoded = try std.json.Stringify.valueAlloc(arena.allocator(), file, .{});
        try edits.append(arena.allocator(), .{ .key = file.path, .value = encoded });
    }
    for (prior) |old| if (staging.find(old.path) == null) try edits.append(arena.allocator(), .{ .key = old.path, .value = null });
    std.mem.sort(tree.Mutation, edits.items, {}, struct {
        fn less(_: void, l: tree.Mutation, r: tree.Mutation) bool {
            return std.mem.lessThan(u8, l.key, r.key);
        }
    }.less);
    if (edits.items.len != 0) root = try tree.apply(a, access.backing(), root, edits.items);
    const result: files.Root = .{ .version = 2, .domain = staging.domain, .directory = root, .file_count = @intCast(staging.files.len), .chunk_count = total };
    try result.validate();
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
    var work: u64 = 0;
    for (0..8) |_| {
        var generation = length;
        const same = try files.publishPrefix(a, out, undefined, "unread", "run", length, &store, .none, files.OutputBudget{ .generation = &generation, .work = &work }, .{ .root = paged });
        const next = try persist(a, out, &store, .{ .domain = legacy.domain, .files = &.{same} }, paged, .none, &work);
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
    try reader.storage().readFileRangeInto(a, "/persisted/run", length, &again);
    try std.testing.expectEqualStrings("z", &again);
    const retired = try persist(a, out, &store, .{ .domain = legacy.domain }, next, .none, null);
    try std.testing.expect(retired.directory == null);
    try std.testing.expect((try lookup(next, store, "run", .none, null)) != null);
}
