// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
    version: u16 = 2,
    bytes: usize,
    metadata: []const Piece,
    blocks: []const Piece,
    terms: []const Range,
    pub fn validate(self: Directory) !void {
        if ((self.version != 1 and self.version != 2) or self.bytes == 0 or self.bytes > 32 * 1024 * 1024 or self.metadata.len > 16384 or self.blocks.len > 16384 or self.terms.len > 200000) return error.InvalidNativeLakeTextCorpus;
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
    // Native decoders address every section through bounded ranges. Fixed
    // blocks authenticate metadata and payload alike, without reconstructing
    // a virtual contiguous segment or special-casing postings wire versions.
    var blocks: std.ArrayList(Piece) = .empty;
    var position: usize = 0;
    while (position < bytes.len) {
        const end = @min(position + block_bytes, bytes.len);
        try blocks.append(ca, .{ .offset = position, .ref = try upload(ca, store, bytes[position..end], cancellation) });
        position = end;
    }
    const directory: Directory = .{ .bytes = bytes.len, .metadata = &.{}, .blocks = blocks.items, .terms = &.{} };
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
    fallback: ?Read,
    query_scoped: bool = false,
    // Diagnostics track touched payload blocks without retaining payloads.
    loaded: []bool,
    mutex: std.atomic.Mutex = .unlocked,
    fn release(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const a = self.a;
        self.arena.deinit();
        a.destroy(self);
    }
    fn seal(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.query_scoped) self.fallback = null;
    }
    fn source(self: *@This()) local.index.SegmentSource {
        return .{ .ranges = .{ .ptr = self, .length = self.directory.bytes, .read_into = readInitial, .close = release, .bind_read_context = bind, .seal_read_context = seal } };
    }
    fn readInitial(raw: *anyopaque, offset: u64, out: []u8) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.read(self.fallback orelse return error.NativeLakeTextReadContextRequired, offset, out);
    }
    const Query = struct {
        a: A,
        owner: *Owner,
        capability: *const Read,
        fn read(raw: *anyopaque, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.owner.read(self.capability.*, offset, out);
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.a.destroy(self);
        }
    };
    fn bind(raw: *anyopaque, a: A, context: *anyopaque) !local.index.SegmentSource {
        const owner: *@This() = @ptrCast(@alignCast(raw));
        const capability: *const Read = @ptrCast(@alignCast(context));
        try capability.check();
        const query = try a.create(Query);
        query.* = .{ .a = a, .owner = owner, .capability = capability };
        return .{ .ranges = .{ .ptr = query, .length = owner.directory.bytes, .read_into = Query.read, .close = Query.close } };
    }
    fn containing(pieces: []const Piece, offset: usize) ?usize {
        var low: usize = 0;
        var high = pieces.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            if (pieces[mid].offset <= offset) low = mid + 1 else high = mid;
        }
        if (low == 0) return null;
        const index = low - 1;
        return if (offset - pieces[index].offset < pieces[index].ref.byte_len) index else null;
    }
    fn read(self: *@This(), capability: Read, start: u64, out: []u8) !void {
        try capability.check();
        if (start > self.directory.bytes or out.len > self.directory.bytes - start) return error.InvalidNativeLakeTextCorpus;
        var offset: usize = @intCast(start);
        var done: usize = 0;
        while (done < out.len) {
            try capability.check();
            const metadata = containing(self.directory.metadata, offset);
            const block = if (metadata == null) containing(self.directory.blocks, offset) else null;
            const piece = if (metadata) |index| self.directory.metadata[index] else if (block) |index| self.directory.blocks[index] else return error.InvalidNativeLakeTextCorpus;
            var lease = try artifacts.readArtifactLease(self.a, capability.store, piece.ref, capability.cancellation, capability.cache);
            defer lease.deinit();
            const data = lease.bytes();
            try capability.check();
            const within = offset - piece.offset;
            const take = @min(out.len - done, data.len - within);
            @memcpy(out[done..][0..take], data[within..][0..take]);
            if (block) |index| {
                @import("antfly_platform").sync.lockYielding(&self.mutex);
                self.loaded[index] = true;
                self.mutex.unlock();
            }
            offset += take;
            done += take;
        }
        try capability.check();
    }
};
pub fn load(a: A, read: Read, ref: Ref) !local.index.SegmentData {
    const owner = try a.create(Owner);
    owner.* = .{ .a = a, .arena = .init(a), .directory = undefined, .loaded = &.{}, .fallback = read };
    errdefer Owner.release(owner);
    owner.directory = try loadDirectory(owner.arena.allocator(), read, ref);
    owner.loaded = try owner.arena.allocator().alloc(bool, owner.directory.blocks.len);
    @memset(owner.loaded, false);
    return .fromNative(owner.source());
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
    const owner: *Owner = @ptrCast(@alignCast(data.native.ranges.ptr));
    var lazy = try local.index.IndexWriter.init(a);
    defer lazy.deinit();
    try lazy.addSegmentWithIdData(1, data);
    var eager = try local.index.IndexWriter.init(a);
    defer eager.deinit();
    try eager.addSegmentWithId(1, encoded);
    const before_absent = try a.dupe(bool, owner.loaded);
    defer a.free(before_absent);
    const absent = try lazy.snapshot().search(a, "body", &.{"absent"}, 10);
    defer a.free(absent.hits);
    try std.testing.expectEqual(@as(u64, 0), absent.total_count);
    try std.testing.expectEqualSlices(bool, before_absent, owner.loaded);
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
    // Bound readers use the current query's store capability. A failed read
    // must leave shared navigation reusable by a later authorized query.
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
    const denied_snapshot = try retry_writer.acquireSnapshotWithReadContext(&failed);
    defer denied_snapshot.release();
    var denied_bytes: [4]u8 = undefined;
    try std.testing.expectError(error.TestRemoteUnavailable, denied_snapshot.segments[0].query_source.?.readInto(0, &denied_bytes));
    var authorized: Read = .{ .store = store, .cache = null, .context = .{}, .cancellation = .none };
    const authorized_snapshot = try retry_writer.acquireSnapshotWithReadContext(&authorized);
    defer authorized_snapshot.release();
    const retried = try authorized_snapshot.search(a, "body", &.{"alpha"}, 10);
    defer a.free(retried.hits);
    try std.testing.expectEqual(@as(u32, 2), retried.total_count);
    // A fresh query's context must supersede the builder's context, even if
    // all needed blocks are already warm in the shared physical owner.
    var expired: Read = .{ .store = store, .cache = null, .context = .{ .deadline_ns = 1 }, .cancellation = .none };
    try std.testing.expectError(error.DeadlineExceeded, lazy.acquireSnapshotWithReadContext(&expired));
}

/// Process caches retain payload ownership, never the opening request's
/// credentials, cancellation callback or lease capability.
pub fn loadQueryScoped(a: A, read: Read, ref: Ref) !local.index.SegmentData {
    const data = try load(a, read, ref);
    const owner: *Owner = @ptrCast(@alignCast(data.native.ranges.ptr));
    owner.query_scoped = true;
    return data;
}

test "external lake native text seeks a common term without reading its position corpus" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const text = try ca.alloc(u8, 6 * 128);
    for (0..128) |i| @memcpy(text[i * 6 ..][0..6], "alpha ");
    const docs = try ca.alloc(local.introducer.TextDocument, 12000);
    const fields: []const local.introducer.TextField = &.{.{ .field_name = "body", .text = text }};
    for (docs, 0..) |*doc, index| doc.* = .{ .id = try std.fmt.allocPrint(ca, "doc-{d:0>5}", .{index}), .stored_data = "{}", .text_fields = fields };
    const encoded = try local.storage_db_document_mapper.buildTextSegmentsFromProjectionBatch(ca, .{ .docs = docs }, .{}, .{ .target_segment_bytes = 32 * 1024 * 1024, .target_build_memory_bytes = 64 * 1024 * 1024, .store_document_source = false });
    try std.testing.expectEqual(@as(usize, 1), encoded.len);
    try std.testing.expect(encoded[0].len > block_bytes * 4);
    var directory = try local.common_test_directory.TestDirectory.init("seekable-text-large");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(8), 1, std.testing.io);
    const ref = try publish(ca, ca, &store, encoded[0], .none);
    const data = try load(a, .{ .store = store, .cache = null, .context = .{ .io = std.testing.io }, .cancellation = .none }, ref);
    const owner: *Owner = @ptrCast(@alignCast(data.native.ranges.ptr));
    var writer = try local.index.IndexWriter.init(a);
    defer writer.deinit();
    try writer.addSegmentWithIdData(1, data);
    const snapshot = writer.snapshot();
    try std.testing.expectEqual(@as(u32, 12000), try snapshot.termDocFreq(a, "body", "alpha"));
    var inv = (try snapshot.segments[0].reader.invertedIndexScoped(a, "body")).?;
    defer inv.deinit();
    const lookup = (try inv.lookup("alpha")).?;
    var iterator = try lookup.iterator(a);
    defer iterator.deinit();
    const hit = (try iterator.advanceTo(11999)).?;
    try std.testing.expectEqual(@as(u32, 11999), hit.doc_id);
    var touched: usize = 0;
    for (owner.loaded) |loaded| touched += @intFromBool(loaded);
    try std.testing.expect(touched < owner.loaded.len);
    // Borrowed read capability is never retained after cache admission.
    owner.query_scoped = true;
    Owner.seal(owner);
    try std.testing.expect(owner.fallback == null);
    var fork = try writer.forkImmutable();
    defer fork.deinit();
    try fork.replaceSegmentsManyData(&.{1}, &.{});
    try std.testing.expectEqual(@as(u32, 0), fork.snapshot().liveDocCount());
    var authorized: Read = .{ .store = store, .cache = null, .context = .{ .io = std.testing.io }, .cancellation = .none };
    const bound = try writer.acquireSnapshotWithReadContext(&authorized);
    defer bound.release();
    try std.testing.expectEqual(@as(u32, 12000), try bound.termDocFreq(a, "body", "alpha"));
}
