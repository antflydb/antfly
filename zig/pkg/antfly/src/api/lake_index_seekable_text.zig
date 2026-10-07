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

//! Authenticated metadata plus independently addressable posting blocks.
//! Native segment offsets, dictionaries, norms and global scoring stay intact.
const std = @import("std");
const local = @import("antfly_local_sources");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const stores = @import("../serverless/artifacts/store.zig");
const Cancellation = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
const Ref = artifacts.ChunkRef;
const block_bytes = 64 * 1024;
pub const Piece = struct { offset: usize, ref: Ref };
pub const Range = struct { offset: usize, len: usize };
pub const Directory = struct {
    version: u16 = 1,
    bytes: usize,
    metadata: []const Piece,
    blocks: []const Piece,
    terms: []const Range,
    pub fn validate(self: Directory) !void {
        if (self.version != 1 or self.bytes == 0 or self.bytes > 32 * 1024 * 1024 or self.metadata.len > 16384 or self.blocks.len > 16384 or self.terms.len > 200000) return error.InvalidNativeLakeTextCorpus;
        // The two sorted streams must cover the original segment exactly.
        var position: usize = 0;
        var m: usize = 0;
        var b: usize = 0;
        while (m < self.metadata.len or b < self.blocks.len) {
            const meta = b == self.blocks.len or (m < self.metadata.len and self.metadata[m].offset < self.blocks[b].offset);
            const piece = if (meta) self.metadata[m] else self.blocks[b];
            if (piece.offset != position or piece.ref.byte_len == 0 or piece.ref.byte_len > self.bytes - position or (!meta and piece.ref.byte_len > block_bytes)) return error.InvalidNativeLakeTextCorpus;
            try stores.validateSha256ArtifactIdentity(piece.ref.artifact_id, piece.ref.checksum);
            position += @intCast(piece.ref.byte_len);
            if (meta) m += 1 else b += 1;
        }
        if (position != self.bytes) return error.InvalidNativeLakeTextCorpus;
        position = 0;
        for (self.terms) |term| {
            if (term.offset < position or term.len == 0 or term.offset > self.bytes or term.len > self.bytes - term.offset) return error.InvalidNativeLakeTextCorpus;
            position = term.offset + term.len;
        }
    }
};
pub const Read = struct {
    store: stores.ArtifactStore,
    cache: ?artifacts.CachedRead,
    context: local.serverless_query_lake_read_context.Context,
    cancellation: Cancellation,
    fn check(self: Read) !void {
        try self.context.ensureActive();
        try self.cancellation.check();
    }
};
fn upload(a: A, store: *stores.ArtifactStore, bytes: []const u8, cancellation: Cancellation) !Ref {
    var writer = store.*;
    writer.allocator = a;
    const ref = try writer.putWithCancellation(bytes, cancellation);
    return .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len };
}
pub fn publish(a: A, out: A, store: *stores.ArtifactStore, bytes: []const u8, cancellation: Cancellation) !Ref {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var reader = try local.index.SegmentReader.init(ca, bytes);
    defer reader.deinit();
    var holes: std.ArrayList(Range) = .empty;
    var terms: std.ArrayList(Range) = .empty;
    for (reader.fields) |field| for (field.sections) |section| {
        if (section.section_type != .inverted_text) continue;
        var inv = (try reader.invertedIndex(field.name)).?;
        var iterator = try inv.termIterator();
        defer iterator.deinit();
        while (try iterator.next()) |term| if (term.result == .postings) {
            const data = term.result.postings.serialized_data;
            try terms.append(ca, .{ .offset = @intFromPtr(data.ptr) - @intFromPtr(bytes.ptr), .len = data.len });
        };
        const section_start: usize = @intCast(section.offset);
        const section_len: usize = @intCast(section.length);
        const header = bytes[section_start..][0..33];
        const dict_len = std.mem.readInt(u32, header[21..25], .little);
        const bloom_len = std.mem.readInt(u32, header[25..29], .little);
        const norms_len = std.mem.readInt(u32, header[29..33], .little);
        const end = section_start + section_len - dict_len - bloom_len - norms_len;
        if (end > section_start + 33) try holes.append(ca, .{ .offset = section_start + 33, .len = end - section_start - 33 });
    };
    const Less = struct {
        fn less(_: void, x: Range, y: Range) bool {
            return x.offset < y.offset;
        }
    };
    std.mem.sort(Range, holes.items, {}, Less.less);
    std.mem.sort(Range, terms.items, {}, Less.less);
    var metadata: std.ArrayList(Piece) = .empty;
    var blocks: std.ArrayList(Piece) = .empty;
    var position: usize = 0;
    for (holes.items) |hole| {
        if (position < hole.offset) try metadata.append(ca, .{ .offset = position, .ref = try upload(ca, store, bytes[position..hole.offset], cancellation) });
        position = hole.offset;
        while (position < hole.offset + hole.len) {
            const end = @min(position + block_bytes, hole.offset + hole.len);
            try blocks.append(ca, .{ .offset = position, .ref = try upload(ca, store, bytes[position..end], cancellation) });
            position = end;
        }
    }
    if (position < bytes.len) try metadata.append(ca, .{ .offset = position, .ref = try upload(ca, store, bytes[position..], cancellation) });
    const directory: Directory = .{ .bytes = bytes.len, .metadata = metadata.items, .blocks = blocks.items, .terms = terms.items };
    try directory.validate();
    const encoded = try std.json.Stringify.valueAlloc(ca, directory, .{});
    if (encoded.len > 4 * 1024 * 1024) return error.NativeLakeTextCorpusTooLarge;
    return upload(out, store, encoded, cancellation);
}
pub fn loadDirectory(a: A, read: Read, ref: Ref) !Directory {
    if (ref.byte_len > 4 * 1024 * 1024) return error.InvalidNativeLakeTextCorpus;
    const bytes = try artifacts.readArtifact(a, read.store, ref, read.cancellation, read.cache);
    defer a.free(bytes);
    const directory = try std.json.parseFromSliceLeaky(Directory, a, bytes, .{ .allocate = .alloc_always });
    try directory.validate();
    const scope = (try stores.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.InvalidNativeLakeTextCorpus;
    for ([_][]const Piece{ directory.metadata, directory.blocks }) |pieces| for (pieces) |piece| {
        const child = (try stores.uploadScopeFromArtifactId(piece.ref.artifact_id)) orelse return error.InvalidNativeLakeTextCorpus;
        if (!std.mem.eql(u8, &child.domain, &scope.domain)) return error.InvalidNativeLakeTextCorpus;
    };
    return directory;
}
const Owner = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    directory: Directory,
    bytes: []u8,
    loaded: []bool,
    loading: []bool,
    fallback: ?Read,
    mutex: std.atomic.Mutex = .unlocked,
    fn release(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const a = self.a;
        a.free(self.bytes);
        self.arena.deinit();
        a.destroy(self);
    }
    fn ensure(raw: *anyopaque, request: ?*anyopaque, offset: usize) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const read = if (request) |ptr| @as(*const Read, @ptrCast(@alignCast(ptr))).* else self.fallback orelse return error.NativeLakeTextReadContextRequired;
        try read.check();
        var low: usize = 0;
        var high = self.directory.terms.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            if (self.directory.terms[mid].offset < offset) low = mid + 1 else high = mid;
        }
        if (low == self.directory.terms.len or self.directory.terms[low].offset != offset) return error.InvalidNativeLakeTextCorpus;
        const term = self.directory.terms[low];
        var covered: usize = 0;
        var first: usize = 0;
        var last = self.directory.blocks.len;
        while (first < last) {
            const mid = first + (last - first) / 2;
            const piece = self.directory.blocks[mid];
            if (piece.offset + piece.ref.byte_len <= offset) first = mid + 1 else last = mid;
        }
        for (self.directory.blocks[first..], first..) |piece, i| {
            const end = piece.offset + @as(usize, @intCast(piece.ref.byte_len));
            if (piece.offset >= offset + term.len) break;
            while (true) {
                try read.check();
                @import("antfly_platform").sync.lockYielding(&self.mutex);
                if (self.loaded[i]) {
                    self.mutex.unlock();
                    break;
                }
                if (self.loading[i]) {
                    self.mutex.unlock();
                    if (read.context.io) |io| try io.sleep(.fromMilliseconds(1), .awake) else @import("antfly_platform").time.yieldNow();
                    continue;
                }
                self.loading[i] = true;
                self.mutex.unlock();
                // Distinct immutable blocks may load concurrently. Failure
                // clears only this flight so another request can retry it.
                errdefer {
                    @import("antfly_platform").sync.lockYielding(&self.mutex);
                    self.loading[i] = false;
                    self.mutex.unlock();
                }
                const data = try artifacts.readArtifact(self.a, read.store, piece.ref, read.cancellation, read.cache);
                defer self.a.free(data);
                try read.check();
                @memcpy(self.bytes[piece.offset..end], data);
                @import("antfly_platform").sync.lockYielding(&self.mutex);
                self.loaded[i] = true;
                self.loading[i] = false;
                self.mutex.unlock();
                break;
            }
            covered += @min(end, offset + term.len) - @max(piece.offset, offset);
        }
        if (covered != term.len) return error.InvalidNativeLakeTextCorpus;
        try read.check();
    }
};
pub fn load(a: A, read: Read, ref: Ref) !local.index.SegmentData {
    const owner = try a.create(Owner);
    owner.* = .{ .a = a, .arena = .init(a), .directory = undefined, .bytes = &.{}, .loaded = &.{}, .loading = &.{}, .fallback = read };
    errdefer Owner.release(owner);
    owner.directory = try loadDirectory(owner.arena.allocator(), read, ref);
    owner.bytes = try a.alloc(u8, owner.directory.bytes);
    @memset(owner.bytes, 0);
    owner.loaded = try owner.arena.allocator().alloc(bool, owner.directory.blocks.len);
    @memset(owner.loaded, false);
    owner.loading = try owner.arena.allocator().alloc(bool, owner.directory.blocks.len);
    @memset(owner.loading, false);
    for (owner.directory.metadata) |piece| {
        try read.check();
        const data = try artifacts.readArtifact(a, read.store, piece.ref, read.cancellation, read.cache);
        defer a.free(data);
        @memcpy(owner.bytes[piece.offset..][0..data.len], data);
    }
    return .{ .owned_view = .{ .bytes = owner.bytes, .owner = owner, .release = Owner.release, .file_backed = false, .postings_loader = .{ .ptr = owner, .ensure = Owner.ensure } } };
}

test "external lake seekable text loads touched postings and keeps exact scoring" {
    const a = std.testing.allocator;
    const mapper = local.storage_db_document_mapper;
    // Repeated terms force posting lists; distinct terms exercise dictionary
    // iteration without fetching payloads for an absent exact term.
    const encoded = (try mapper.buildTextSegmentFromDocuments(a, &.{
        .{ .key = "one", .value = "{\"body\":\"alpha beta alpha\"}" },
        .{ .key = "two", .value = "{\"body\":\"alpha gamma\"}" },
        .{ .key = "three", .value = "{\"body\":\"beta gamma\"}" },
    }, .{}, null)).?;
    defer a.free(encoded);
    var directory = try local.common_test_directory.TestDirectory.init("seekable-text");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(9), 1, std.testing.io);
    const ref = try publish(a, a, &store, encoded, .none);
    defer a.free(ref.artifact_id);
    defer a.free(ref.checksum);
    const data = try load(a, .{ .store = store, .cache = null, .context = .{ .io = std.testing.io }, .cancellation = .none }, ref);
    const owner: *Owner = @ptrCast(@alignCast(data.owned_view.owner));
    var lazy = try local.index.IndexWriter.init(a);
    defer lazy.deinit();
    try lazy.addSegmentWithIdData(1, data);
    var eager = try local.index.IndexWriter.init(a);
    defer eager.deinit();
    try eager.addSegmentWithId(1, encoded);
    for (owner.loaded) |loaded| try std.testing.expect(!loaded);
    const absent = try lazy.snapshot().search(a, "body", &.{"absent"}, 10);
    defer a.free(absent.hits);
    try std.testing.expectEqual(@as(u64, 0), absent.total_count);
    for (owner.loaded) |loaded| try std.testing.expect(!loaded);
    for ([_][]const u8{ "alpha", "beta", "gamma" }) |term| {
        const expected = try eager.snapshot().search(a, "body", &.{term}, 10);
        defer a.free(expected.hits);
        const actual = try lazy.snapshot().search(a, "body", &.{term}, 10);
        defer a.free(actual.hits);
        try std.testing.expectEqual(expected.total_count, actual.total_count);
        try std.testing.expectEqual(expected.hits.len, actual.hits.len);
        for (expected.hits, actual.hits) |left, right| {
            try std.testing.expectEqual(left.doc_id, right.doc_id);
            try std.testing.expectEqual(left.score, right.score);
        }
    }
    var loaded_count: usize = 0;
    for (owner.loaded) |loaded| loaded_count += @intFromBool(loaded);
    try std.testing.expect(loaded_count != 0);
    // A cache entry must use the current query's store capability, and a
    // failed flight must remain retryable by a later authorized reader.
    var retry_writer = try local.index.IndexWriter.init(a);
    defer retry_writer.deinit();
    try retry_writer.addSegmentWithIdData(1, try load(a, .{ .store = store, .cache = null, .context = .{}, .cancellation = .none }, ref));
    const Denied = struct {
        fn stat(_: *anyopaque, _: A, _: []const u8) !stores.ArtifactMetadata {
            return error.TestRemoteUnavailable;
        }
    };
    var vtable = store.vtable.*;
    vtable.stat = Denied.stat;
    vtable.stat_with_cancellation = null;
    var denied = store;
    denied.vtable = &vtable;
    var failed: Read = .{ .store = denied, .cache = null, .context = .{}, .cancellation = .none };
    const failed_snapshot = try retry_writer.acquireSnapshotWithReadContext(&failed);
    defer failed_snapshot.release();
    try std.testing.expectError(error.TestRemoteUnavailable, failed_snapshot.search(a, "body", &.{"alpha"}, 10));
    const retried = try retry_writer.snapshot().search(a, "body", &.{"alpha"}, 10);
    defer a.free(retried.hits);
    try std.testing.expectEqual(@as(u32, 2), retried.total_count);
    // A fresh query's context must supersede the builder's context, even if
    // all needed blocks are already warm in the shared physical owner.
    var expired: Read = .{ .store = store, .cache = null, .context = .{ .deadline_ns = 1 }, .cancellation = .none };
    const snapshot = try lazy.acquireSnapshotWithReadContext(&expired);
    defer snapshot.release();
    try std.testing.expectError(error.DeadlineExceeded, snapshot.search(a, "body", &.{"alpha"}, 10));
}

/// Process caches retain payload ownership, never the opening request's
/// credentials, cancellation callback or lease capability.
pub fn loadQueryScoped(a: A, read: Read, ref: Ref) !local.index.SegmentData {
    const data = try load(a, read, ref);
    const owner: *Owner = @ptrCast(@alignCast(data.owned_view.owner));
    owner.fallback = null;
    return data;
}
