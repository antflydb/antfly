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
const block_bytes = 256 * 1024;
const pack_bytes = 1024 * 1024;
pub const Piece = struct {
    offset: usize,
    // For packed ranges this is the authenticated range identity, not a
    // separately uploaded object. GC retains pack, never this synthetic key.
    ref: Ref,
    pack: ?Ref = null,
    pack_offset: u64 = 0,
    pub fn retainedArtifact(self: Piece) Ref {
        return self.pack orelse self.ref;
    }
};
pub const Range = struct { offset: usize, len: usize };
pub const Directory = struct {
    version: u16 = 3,
    bytes: usize,
    metadata: []const Piece,
    blocks: []const Piece,
    terms: []const Range,
    pub fn validate(self: Directory) !void {
        if ((self.version != 1 and self.version != 2 and self.version != 3) or self.bytes == 0 or self.bytes > 32 * 1024 * 1024 or self.metadata.len > 16384 or self.blocks.len > 16384 or self.terms.len > 200000) return error.InvalidNativeLakeTextCorpus;
        // The two sorted streams must cover the original segment exactly.
        var position: usize = 0;
        var m: usize = 0;
        var b: usize = 0;
        while (m < self.metadata.len or b < self.blocks.len) {
            const meta = b == self.blocks.len or (m < self.metadata.len and self.metadata[m].offset < self.blocks[b].offset);
            const piece = if (meta) self.metadata[m] else self.blocks[b];
            if (piece.offset != position or piece.ref.byte_len == 0 or piece.ref.byte_len > self.bytes - position or (!meta and piece.ref.byte_len > block_bytes)) return error.InvalidNativeLakeTextCorpus;
            try stores.validateSha256ArtifactIdentity(piece.ref.artifact_id, piece.ref.checksum);
            if (piece.pack) |pack| {
                if (self.version < 3 or pack.byte_len == 0 or pack.byte_len > pack_bytes or piece.pack_offset > pack.byte_len or piece.ref.byte_len > pack.byte_len - piece.pack_offset) return error.InvalidNativeLakeTextCorpus;
                try stores.validateSha256ArtifactIdentity(pack.artifact_id, pack.checksum);
            } else if (piece.pack_offset != 0) return error.InvalidNativeLakeTextCorpus;
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
    resource_manager: ?*local.storage_resource_manager.ResourceManager = null,
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
    // Upload large immutable packs, with independently authenticated bounded
    // range units. Broad reads need four times fewer GETs than 64 KiB objects,
    // while sparse queries never download an entire pack to authenticate it.
    var blocks: std.ArrayList(Piece) = .empty;
    var position: usize = 0;
    while (position < bytes.len) {
        const pack_end = @min(position + pack_bytes, bytes.len);
        const pack = try upload(ca, store, bytes[position..pack_end], cancellation);
        var offset = position;
        while (offset < pack_end) {
            const end = @min(offset + block_bytes, pack_end);
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(bytes[offset..end], &digest, .{});
            const checksum = try ca.dupe(u8, &std.fmt.bytesToHex(digest, .lower));
            const scope = (try stores.uploadScopeFromArtifactId(pack.artifact_id)) orelse return error.InvalidArtifactUploadScope;
            const id = try scope.artifactId(checksum);
            try blocks.append(ca, .{ .offset = offset, .ref = .{ .artifact_id = try ca.dupe(u8, &id), .checksum = checksum, .byte_len = end - offset }, .pack = pack, .pack_offset = offset - position });
            offset = end;
        }
        position = pack_end;
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
        if (piece.pack) |pack| {
            const parent = (try stores.uploadScopeFromArtifactId(pack.artifact_id)) orelse return error.InvalidNativeLakeTextCorpus;
            if (!std.mem.eql(u8, &parent.domain, &scope.domain)) return error.InvalidNativeLakeTextCorpus;
        }
    };
    return directory;
}
fn readPieceLease(a: A, store: stores.ArtifactStore, piece: Piece, cancellation: Cancellation, cached: ?artifacts.CachedRead) !local.serverless_query_lake_serving_cache.Cache.ImmutableLease {
    if (piece.pack == null) return artifacts.readArtifactLease(a, store, piece.ref, cancellation, cached);
    const Loader = struct {
        store: stores.ArtifactStore,
        piece: Piece,
        cancellation: Cancellation,
        fn load(raw: *anyopaque, alloc: A) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var source = self.store;
            const bytes = try source.getRangeAllocWithCancellationUsingAllocator(alloc, self.piece.pack.?.artifact_id, self.piece.pack_offset, @intCast(self.piece.ref.byte_len), self.cancellation);
            errdefer alloc.free(bytes);
            if (bytes.len != self.piece.ref.byte_len) return error.ArtifactIntegrityMismatch;
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
            if (!std.mem.eql(u8, &digest, &(try stores.sha256DigestFromChecksum(self.piece.ref.checksum)))) return error.ArtifactIntegrityMismatch;
            try self.cancellation.check();
            return bytes;
        }
    };
    try cancellation.check();
    var loader: Loader = .{ .store = store, .piece = piece, .cancellation = cancellation };
    if (cached) |cache| return cache.cache.readImmutableBlockLease(a, cache.scope, piece.ref.artifact_id, @intCast(piece.ref.byte_len), try stores.sha256DigestFromChecksum(piece.ref.checksum), cache.context, .{ .ptr = &loader, .load = Loader.load });
    return .{ .heap = .{ .alloc = a, .bytes = try Loader.load(&loader, a) } };
}
const Owner = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    directory: Directory,
    fallback: ?Read,
    query_scoped: bool = false,
    resource_manager: ?*local.storage_resource_manager.ResourceManager = null,
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
        return .{ .ranges = .{ .ptr = self, .length = self.directory.bytes, .read_into = readInitial, .close = release, .bind_read_context = bind, .seal_read_context = seal, .resource_manager = self.resource_manager } };
    }
    fn readInitial(raw: *anyopaque, offset: u64, out: []u8) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.read(self.fallback orelse return error.NativeLakeTextReadContextRequired, offset, out);
    }
    const Query = struct {
        a: A,
        owner: *Owner,
        capability: *const Read,
        io: ?std.Io,
        pending: [4]?local.sql_parallel_scheduler.Task(anyerror!void) = @splat(null),
        offsets: [4]?u64 = @splat(null),
        recent_pages: [4]?u64 = @splat(null),
        recent_cursor: usize = 0,
        cancelled: std.atomic.Value(bool) = .init(false),
        mutex: std.atomic.Mutex = .unlocked,
        fn checkContext(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.cancelled.load(.acquire)) return error.Canceled;
            try self.capability.check();
        }
        fn canceled(raw: *const anyopaque) bool {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            if (self.cancelled.load(.acquire)) return true;
            self.capability.check() catch return true;
            if (self.capability.cache) |cache| cache.context.ensureActive() catch return true;
            return false;
        }
        fn warm(self: *@This(), piece: Piece) anyerror!void {
            try self.capability.check();
            const token: Cancellation = .{ .ptr = self, .is_cancelled_fn = canceled };
            var cached = self.capability.cache;
            if (cached) |*cache| cache.context.cancellation = .{ .ptr = self, .is_cancelled_fn = canceled };
            if (cached) |cache| if (piece.pack) |pack| {
                if (try cache.cache.pinImmutableBlock(std.heap.page_allocator, cache.scope, pack.artifact_id, @intCast(pack.byte_len), try stores.sha256DigestFromChecksum(pack.checksum), cache.context)) |value| {
                    var lease = value;
                    lease.deinit();
                    return;
                }
            };
            var lease = try readPieceLease(std.heap.page_allocator, self.capability.store, piece, token, cached);
            defer lease.deinit();
        }
        fn drain(self: *@This(), cancel: bool) void {
            if (cancel) self.cancelled.store(true, .release);
            const io = self.io orelse return;
            for (&self.pending) |*slot| if (slot.*) |*task| {
                if (cancel) task.cancel(io) catch {} else task.await(io) catch {};
                slot.* = null;
            };
            self.offsets = @splat(null);
        }
        fn rememberPage(self: *@This(), page: u64) void {
            for (self.recent_pages) |prior| if (prior == page) return;
            self.recent_pages[self.recent_cursor] = page;
            self.recent_cursor = (self.recent_cursor + 1) % self.recent_pages.len;
        }
        fn reap(self: *@This(), io: std.Io) void {
            for (&self.pending, &self.offsets) |*slot, *offset| {
                if (slot.*) |*task| {
                    if (!task.isComplete()) continue;
                    const succeeded = if (task.await(io)) |_| true else |_| false;
                    if (succeeded) self.rememberPage(offset.*.? / block_bytes);
                    slot.* = null;
                    offset.* = null;
                }
            }
        }
        fn prefetch(raw: *anyopaque, start: u64, length: u64) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            defer self.mutex.unlock();
            if (self.cancelled.load(.acquire)) return;
            // Without a shared cache speculative work cannot benefit a reader.
            if (self.capability.cache == null) return;
            const io = self.capability.context.io orelse return;
            self.capability.check() catch return;
            self.reap(io);
            if (start > self.owner.directory.bytes or length > self.owner.directory.bytes - start) return;
            var offset = start;
            var count: usize = 0;
            while (offset < start + length and count < self.pending.len) : (count += 1) {
                const metadata = containing(self.owner.directory.metadata, @intCast(offset));
                const block = if (metadata == null) containing(self.owner.directory.blocks, @intCast(offset)) else null;
                var piece = if (metadata) |i| self.owner.directory.metadata[i] else if (block) |i| self.owner.directory.blocks[i] else return;
                if (piece.pack) |pack| {
                    if (offset == piece.offset and piece.pack_offset == 0 and pack.byte_len <= start + length - offset and self.owner.completePack(piece.offset, pack))
                        piece = .{ .offset = piece.offset, .ref = pack, .pack = pack };
                }
                offset = piece.offset + piece.ref.byte_len;
                // Legacy v1 artifacts can be large; keep speculative bytes bounded.
                if (piece.ref.byte_len > pack_bytes) continue;
                // Most native chunks share a page: do not spawn a worker for
                // every small decoder read of an already consumed page.
                var duplicate = false;
                for (self.recent_pages) |page| if (page == piece.offset / block_bytes) {
                    duplicate = true;
                    break;
                };
                for (self.offsets) |prior| if (prior == piece.offset) {
                    duplicate = true;
                    break;
                };
                if (duplicate) continue;
                var slot: usize = 0;
                while (slot < self.pending.len and self.pending[slot] != null) : (slot += 1) {}
                // A full speculative queue yields instead of stalling WAND
                // on an unvisited range. Required reads recycle consumed slots.
                if (slot == self.pending.len) return;
                self.pending[slot] = local.sql_parallel_scheduler.global().submitTransient(io, @intCast(piece.ref.byte_len * 2), warm, .{ self, piece }) orelse return;
                self.offsets[slot] = piece.offset;
            }
        }
        fn read(raw: *anyopaque, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.cancelled.load(.acquire)) return error.Canceled;
            // Four independent authenticated units/packs, at most 4 MiB of
            // source bytes. Required reads share the same singleflight key.
            if (out.len > block_bytes) prefetch(raw, offset, out.len);
            try self.owner.read(self.capability.*, offset, out);
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            defer self.mutex.unlock();
            if (out.len != 0) {
                var page = offset / block_bytes;
                const last = (offset + out.len - 1) / block_bytes;
                var count: usize = 0;
                while (page <= last and count < self.recent_pages.len) : ({
                    page += 1;
                    count += 1;
                }) {
                    self.rememberPage(page);
                }
            }
            if (self.capability.context.io) |io| for (&self.pending, &self.offsets) |*slot, *start| {
                if (start.*) |position| if (position < offset + out.len and position + block_bytes > offset) {
                    if (slot.*) |*task| task.await(io) catch {};
                    slot.* = null;
                    start.* = null;
                };
            };
            try self.capability.check();
        }
        fn quiesce(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.cancelled.store(true, .release);
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            defer self.mutex.unlock();
            self.drain(true);
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            quiesce(raw);
            self.a.destroy(self);
        }
    };
    fn bind(raw: *anyopaque, a: A, context: *anyopaque) !local.index.SegmentSource {
        const owner: *@This() = @ptrCast(@alignCast(raw));
        const capability: *const Read = @ptrCast(@alignCast(context));
        try capability.check();
        const query = try a.create(Query);
        query.* = .{ .a = a, .owner = owner, .capability = capability, .io = capability.context.io };
        return .{ .ranges = .{ .ptr = query, .length = owner.directory.bytes, .read_into = Query.read, .close = Query.close, .prefetch = Query.prefetch, .quiesce_read_context = Query.quiesce, .resource_manager = capability.resource_manager orelse owner.resource_manager, .read_io = capability.context.io, .check_read_context = Query.checkContext } };
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
            if (piece.pack) |pack| {
                // Reuse a broad read for later sparse reads without another
                // allocation or GET. A cold sparse read never fetches a pack.
                var pack_lease: ?local.serverless_query_lake_serving_cache.Cache.ImmutableLease = if (capability.cache) |cached|
                    try cached.cache.pinImmutableBlock(self.a, cached.scope, pack.artifact_id, @intCast(pack.byte_len), try stores.sha256DigestFromChecksum(pack.checksum), cached.context)
                else
                    null;
                const complete = offset == piece.offset and piece.pack_offset == 0 and pack.byte_len <= out.len - done and self.completePack(piece.offset, pack);
                if (pack_lease == null and complete) pack_lease = try readPieceLease(self.a, capability.store, .{ .offset = piece.offset, .ref = pack, .pack = pack }, capability.cancellation, capability.cache);
                if (pack_lease) |*lease| {
                    defer lease.deinit();
                    const data = lease.bytes();
                    const take = if (complete) @as(usize, @intCast(pack.byte_len)) else @min(out.len - done, @as(usize, @intCast(piece.ref.byte_len)) - (offset - piece.offset));
                    // The root authenticates both pack and per-unit digests.
                    // Do not let a broad read bypass a malformed unit proof.
                    var position = piece.offset;
                    while (position < offset + take) {
                        const m = containing(self.directory.metadata, position);
                        const b = if (m == null) containing(self.directory.blocks, position) else null;
                        const unit = if (m) |i| self.directory.metadata[i] else if (b) |i| self.directory.blocks[i] else return error.InvalidNativeLakeTextCorpus;
                        const first: usize = @intCast(unit.pack_offset);
                        const length: usize = @intCast(unit.ref.byte_len);
                        if (first > data.len or length > data.len - first) return error.InvalidNativeLakeTextCorpus;
                        var digest: [32]u8 = undefined;
                        std.crypto.hash.sha2.Sha256.hash(data[first..][0..length], &digest, .{});
                        if (!std.mem.eql(u8, &digest, &(try stores.sha256DigestFromChecksum(unit.ref.checksum)))) return error.ArtifactIntegrityMismatch;
                        try capability.check();
                        if (b) |i| {
                            @import("antfly_platform").sync.lockYielding(&self.mutex);
                            self.loaded[i] = true;
                            self.mutex.unlock();
                        }
                        position += length;
                    }
                    const within: usize = @intCast(piece.pack_offset + offset - piece.offset);
                    if (within > data.len or take > data.len - within) return error.InvalidNativeLakeTextCorpus;
                    @memcpy(out[done..][0..take], data[within..][0..take]);
                    offset += take;
                    done += take;
                    continue;
                }
            }
            var lease = try readPieceLease(self.a, capability.store, piece, capability.cancellation, capability.cache);
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
    fn completePack(self: *const @This(), start: usize, pack: Ref) bool {
        if (pack.byte_len > pack_bytes or pack.byte_len > self.directory.bytes - start) return false;
        var position = start;
        while (position < start + pack.byte_len) {
            const metadata = containing(self.directory.metadata, position);
            const block = if (metadata == null) containing(self.directory.blocks, position) else null;
            const piece = if (metadata) |i| self.directory.metadata[i] else if (block) |i| self.directory.blocks[i] else return false;
            const owner = piece.pack orelse return false;
            if (piece.offset != position or piece.pack_offset != position - start or piece.ref.byte_len > start + pack.byte_len - position or
                owner.byte_len != pack.byte_len or !std.mem.eql(u8, owner.artifact_id, pack.artifact_id) or !std.mem.eql(u8, owner.checksum, pack.checksum)) return false;
            position += @intCast(piece.ref.byte_len);
        }
        return position == start + pack.byte_len;
    }
};
pub fn load(a: A, read: Read, ref: Ref) !local.index.SegmentData {
    const owner = try a.create(Owner);
    owner.* = .{ .a = a, .arena = .init(a), .directory = undefined, .loaded = &.{}, .fallback = read, .resource_manager = read.resource_manager };
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
        fn range(_: *anyopaque, _: A, _: []const u8, _: u64, _: usize) ![]u8 {
            return error.TestRemoteUnavailable;
        }
    };
    var vtable = store.vtable.*;
    vtable.stat = Denied.stat;
    vtable.stat_with_cancellation = null;
    vtable.get_range_alloc = Denied.range;
    vtable.get_range_alloc_with_cancellation = null;
    var denied = store;
    denied.vtable = &vtable;
    var failed: Read = .{ .store = denied, .cache = null, .context = .{}, .cancellation = .none };
    const denied_snapshot = try retry_writer.acquireSnapshotWithReadContext(&failed);
    defer denied_snapshot.release();
    try std.testing.expectError(error.TestRemoteUnavailable, denied_snapshot.termDocFreq(a, "body", "alpha"));
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

test "external lake cold ranked highlights survive restart with no provider bytes" {
    const a = std.testing.allocator;
    const Cache = local.serverless_query_lake_serving_cache.Cache;
    const types = local.storage_db_types;
    const search = local.storage_db_query_search_exec;
    const encoded = (try local.storage_db_document_mapper.buildTextSegmentFromDocuments(a, &.{
        .{ .key = "one", .value = "{\"body\":\"alpha beta alpha\",\"label\":\"first\"}" },
        .{ .key = "two", .value = "{\"body\":\"alpha gamma\",\"label\":\"second\"}" },
        .{ .key = "three", .value = "{\"body\":\"beta gamma\",\"label\":\"third\"}" },
    }, .{}, null)).?;
    defer a.free(encoded);
    var directory = try local.common_test_directory.TestDirectory.init("highlight-restart");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(5), 1, std.testing.io);
    const ref = try publish(a, a, &store, encoded, .none);
    defer a.free(ref.artifact_id);
    defer a.free(ref.checksum);
    var io_impl = std.Io.Threaded.init(a, .{});
    defer io_impl.deinit();
    const root = try std.fs.path.join(a, &.{ directory.path(), "cache" });
    defer a.free(root);
    // The second process has neither decoded navigation nor RAM payloads.
    // Deny the provider, not merely its counters: a cache miss must fail.
    const Denied = struct {
        fn get(_: *anyopaque, _: A, _: []const u8) ![]u8 {
            return error.TestRemoteUnavailable;
        }
        fn stat(_: *anyopaque, _: A, _: []const u8) !stores.ArtifactMetadata {
            return error.TestRemoteUnavailable;
        }
        fn range(_: *anyopaque, _: A, _: []const u8, _: u64, _: usize) ![]u8 {
            return error.TestRemoteUnavailable;
        }
    };
    var denied = store.vtable.*;
    denied.get_alloc = Denied.get;
    denied.get_alloc_with_cancellation = null;
    denied.stat = Denied.stat;
    denied.stat_with_cancellation = null;
    denied.get_range_alloc = Denied.range;
    denied.get_range_alloc_with_cancellation = null;
    var first_score: f32 = 0;
    for (0..2) |process| {
        var cache = Cache.init(a);
        defer cache.deinit();
        try cache.ensurePersistent(io_impl.io(), root, .{}, .{});
        if (process == 1) store.vtable = &denied;
        const cached: artifacts.CachedRead = .{ .cache = &cache, .scope = @splat(5), .context = .{ .io = io_impl.io() } };
        const data = try load(a, .{ .store = store, .cache = cached, .context = cached.context, .cancellation = .none }, ref);
        var writer = try local.index.IndexWriter.init(a);
        defer writer.deinit();
        try writer.addSegmentWithIdData(1, data);
        const snapshot = writer.snapshot();
        const result = try snapshot.search(a, "body", &.{"alpha"}, 1);
        defer a.free(result.hits);
        try std.testing.expectEqual(@as(u64, 2), result.total_count);
        try std.testing.expectEqual(@as(usize, 1), result.hits.len);
        if (process == 0) first_score = result.hits[0].score else try std.testing.expectEqual(first_score, result.hits[0].score);
        const stored = (try snapshot.storedDocDecompressed(a, result.hits[0].doc_id)).?;
        defer a.free(stored.data);
        var hits = [_]types.SearchHit{.{ .id = try a.dupe(u8, stored.id) }};
        defer hits[0].deinit(a);
        // Use the same typed source projection and highlighter as delivery;
        // no Parquet source is available to hide an accidental fallback.
        hits[0].source_value = .{ .object = .empty };
        try @import("lake_index_source_projection.zig").appendStored(a, snapshot, result.hits[0].doc_id, stored.id, &.{ "label", "body" }, &hits[0].source_value.?);
        try search.attachHighlightsWithIndexQueries(a, .{ .fields = &.{"body"} }, &.{.{
            .query = .{ .match = .{ .field = "body", .text = "alpha" } },
            .text_analysis = .{},
            .runtime_schema = null,
        }}, &hits, null);
        try std.testing.expectEqual(@as(usize, 1), hits[0].highlights.len);
        try std.testing.expect(hits[0].highlights[0].fragments.len != 0);
        const stats = cache.snapshot();
        if (process == 0) {
            try std.testing.expect(stats.provider_reads > 0);
            try std.testing.expect(stats.provider_bytes > 0);
        } else {
            try std.testing.expectEqual(@as(u64, 0), stats.provider_reads);
            try std.testing.expectEqual(@as(u64, 0), stats.provider_bytes);
            try std.testing.expect(stats.disk_hits > 0);
            try std.testing.expect(cache.persistentStats().?.entries > 0);
        }
        // Implicit shutdown drains accepted writes before the fresh process.
    }
}

test "external lake native text seeks a common term without reading its position corpus" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const text = try ca.alloc(u8, 6 * 128);
    for (0..128) |i| @memcpy(text[i * 6 ..][0..6], "alpha ");
    const docs = try ca.alloc(local.introducer.TextDocument, 48000);
    const fields: []const local.introducer.TextField = &.{.{ .field_name = "body", .text = text }};
    for (docs, 0..) |*doc, index| doc.* = .{ .id = try std.fmt.allocPrint(ca, "doc-{d:0>5}", .{index}), .stored_data = "{}", .text_fields = fields };
    const encoded = try local.storage_db_document_mapper.buildTextSegmentsFromProjectionBatch(ca, .{ .docs = docs }, .{}, .{ .target_segment_bytes = 128 * 1024 * 1024, .target_build_memory_bytes = 512 * 1024 * 1024, .store_document_source = false });
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
    try std.testing.expectEqual(@as(u32, 48000), try snapshot.termDocFreq(a, "body", "alpha"));
    var inv = (try snapshot.segments[0].reader.invertedIndexScoped(a, "body")).?;
    defer inv.deinit();
    const lookup = (try inv.lookup("alpha")).?;
    var iterator = try lookup.iterator(a);
    defer iterator.deinit();
    const hit = (try iterator.advanceTo(47999)).?;
    try std.testing.expectEqual(@as(u32, 47999), hit.doc_id);
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
    try std.testing.expectEqual(@as(u32, 48000), try bound.termDocFreq(a, "body", "alpha"));
}

test "external lake native text prefetch warms bounded authenticated blocks and joins speculative failures" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("seekable-text-prefetch");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(7), 1, std.testing.io);
    const bytes = try a.alloc(u8, block_bytes * 20);
    defer a.free(bytes);
    // Distinct block identities prevent cache deduplication hiding fan-out.
    for (0..20) |i| @memset(bytes[i * block_bytes ..][0..block_bytes], @intCast(i));
    const ref = try publish(a, a, &store, bytes, .none);
    defer a.free(ref.artifact_id);
    defer a.free(ref.checksum);
    const data = try load(a, .{ .store = store, .cache = null, .context = .{}, .cancellation = .none }, ref);
    var physical = data.native;
    defer physical.close();
    const owner: *Owner = @ptrCast(@alignCast(physical.ranges.ptr));
    var cache = local.serverless_query_lake_serving_cache.Cache.init(std.heap.page_allocator);
    defer cache.deinit();
    var capability: Read = .{ .store = store, .cache = .{ .cache = &cache, .scope = @splat(1), .context = .{ .io = std.testing.io } }, .context = .{ .io = std.testing.io }, .cancellation = .none };
    var source = try Owner.bind(owner, a, &capability);
    var open = true;
    defer if (open) source.close();
    source.prefetch(0, bytes.len);
    const query: *Owner.Query = @ptrCast(@alignCast(source.ranges.ptr));
    // WAND can skip every hinted page. Completed tasks must not permanently
    // occupy the four slots even when no required read overlaps them.
    for (&query.pending) |*slot| if (slot.*) |*task| {
        while (!task.isComplete()) std.atomic.spinLoopHint();
    };
    source.prefetch(block_bytes * 16, block_bytes);
    query.drain(false);
    try std.testing.expectEqual(@as(u64, 5), cache.stats.provider_reads);
    var sample: [1]u8 = undefined;
    try source.readInto(block_bytes * 3, &sample);
    try std.testing.expectEqual(@as(u8, 3), sample[0]);
    try std.testing.expectEqual(@as(u64, 5), cache.stats.provider_reads);
    // Speculative failures remain invisible until that block is required.
    const Denied = struct {
        fn stat(_: *anyopaque, _: A, _: []const u8) !stores.ArtifactMetadata {
            return error.TestRemoteUnavailable;
        }
        fn range(_: *anyopaque, _: A, _: []const u8, _: u64, _: usize) ![]u8 {
            return error.TestRemoteUnavailable;
        }
    };
    var vtable = store.vtable.*;
    vtable.stat = Denied.stat;
    vtable.stat_with_cancellation = null;
    vtable.get_range_alloc = Denied.range;
    vtable.get_range_alloc_with_cancellation = null;
    capability.store.vtable = &vtable;
    source.prefetch(block_bytes * 19, block_bytes);
    query.drain(false);
    try std.testing.expectError(error.TestRemoteUnavailable, source.readInto(block_bytes * 19, &sample));
    source.prefetch(block_bytes * 19, block_bytes);
    source.quiesceReadContext();
    try std.testing.expectError(error.Canceled, source.readInto(0, &sample));
    source.close();
    open = false;
    try std.testing.expectEqual(@as(usize, 0), local.sql_parallel_scheduler.global().workers);
    try std.testing.expectEqual(@as(usize, 0), local.sql_parallel_scheduler.global().bytes);
}

test "external lake packed text ranges reduce requests and authenticate sparse reads" {
    const a = std.testing.allocator;
    const Fs = @import("../serverless/artifacts/fs_store.zig").FsStore;
    var directory = try local.common_test_directory.TestDirectory.init("packed-text-ranges");
    defer directory.cleanup();
    var fs = try Fs.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(6), 1, std.testing.io);
    const bytes = try a.alloc(u8, pack_bytes);
    defer a.free(bytes);
    for (bytes, 0..) |*byte, i| byte.* = @truncate(i / 1024);
    const ref = try publish(a, a, &store, bytes, .none);
    defer a.free(ref.artifact_id);
    defer a.free(ref.checksum);
    const data = try load(a, .{ .store = store, .cache = null, .context = .{}, .cancellation = .none }, ref);
    var source = data.native;
    defer source.close();
    const owner: *Owner = @ptrCast(@alignCast(source.ranges.ptr));
    try std.testing.expectEqual(@as(u16, 3), owner.directory.version);
    try std.testing.expectEqual(@as(usize, 4), owner.directory.blocks.len);
    for (owner.directory.blocks) |piece| try std.testing.expectEqualStrings(owner.directory.blocks[0].pack.?.artifact_id, piece.retainedArtifact().artifact_id);
    const Observed = struct {
        var reads: usize = 0;
        var transferred: usize = 0;
        var corrupt: bool = false;
        fn range(raw: *anyopaque, alloc: A, id: []const u8, offset: u64, len: usize, cancellation: Cancellation) ![]u8 {
            const provider: *Fs = @ptrCast(@alignCast(raw));
            reads += 1;
            transferred += len;
            const result = try provider.getRangeAllocWithCancellation(alloc, id, offset, len, cancellation);
            if (corrupt and result.len != 0) result[0] ^= 1;
            return result;
        }
    };
    Observed.reads = 0;
    Observed.transferred = 0;
    Observed.corrupt = false;
    var vtable = store.vtable.*;
    vtable.get_range_alloc_with_cancellation = Observed.range;
    var observed = store;
    observed.vtable = &vtable;
    const output = try a.alloc(u8, bytes.len);
    defer a.free(output);
    try owner.read(.{ .store = observed, .cache = null, .context = .{}, .cancellation = .none }, 0, output);
    try std.testing.expectEqualSlices(u8, bytes, output);
    try std.testing.expectEqual(@as(usize, 1), Observed.reads);
    try std.testing.expectEqual(bytes.len, Observed.transferred);
    var sample: [1]u8 = undefined;
    Observed.transferred = 0;
    try owner.read(.{ .store = observed, .cache = null, .context = .{}, .cancellation = .none }, block_bytes + 17, &sample);
    try std.testing.expectEqual(@as(usize, block_bytes), Observed.transferred);
    Observed.corrupt = true;
    defer Observed.corrupt = false;
    try std.testing.expectError(error.ArtifactIntegrityMismatch, owner.read(.{ .store = observed, .cache = null, .context = .{}, .cancellation = .none }, 0, &sample));
    Observed.corrupt = false;
    var io_impl = std.Io.Threaded.init(a, .{});
    defer io_impl.deinit();
    const cache_root = try std.fs.path.join(a, &.{ directory.path(), "range-cache" });
    defer a.free(cache_root);
    {
        var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
        defer cache.deinit();
        try cache.ensurePersistent(io_impl.io(), cache_root, .{}, .{});
        const cached: artifacts.CachedRead = .{ .cache = &cache, .scope = @splat(4), .context = .{ .io = io_impl.io() } };
        Observed.reads = 0;
        try owner.read(.{ .store = observed, .cache = cached, .context = cached.context, .cancellation = .none }, 0, output);
        try std.testing.expectEqual(@as(usize, 1), Observed.reads);
        try owner.read(.{ .store = observed, .cache = cached, .context = cached.context, .cancellation = .none }, block_bytes * 2 + 17, &sample);
        try std.testing.expectEqual(@as(usize, 1), Observed.reads);
    }
    {
        var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
        defer cache.deinit();
        try cache.ensurePersistent(io_impl.io(), cache_root, .{}, .{});
        const cached: artifacts.CachedRead = .{ .cache = &cache, .scope = @splat(4), .context = .{ .io = io_impl.io() } };
        Observed.reads = 0;
        Observed.transferred = 0;
        try owner.read(.{ .store = observed, .cache = cached, .context = cached.context, .cancellation = .none }, block_bytes * 2 + 17, &sample);
        try std.testing.expectEqual(bytes[block_bytes * 2 + 17], sample[0]);
        try std.testing.expectEqual(@as(usize, 0), Observed.reads);
        try std.testing.expectEqual(@as(usize, 0), Observed.transferred);
        try std.testing.expect(cache.snapshot().disk_hits > 0);
        try std.testing.expectError(error.DeadlineExceeded, owner.read(.{ .store = observed, .cache = cached, .context = .{ .deadline_ns = 0 }, .cancellation = .none }, 0, &sample));
    }
    var invalid = owner.directory.blocks[0];
    invalid.pack_offset = pack_bytes;
    const malformed: Directory = .{ .bytes = block_bytes, .metadata = &.{}, .blocks = &.{invalid}, .terms = &.{} };
    try std.testing.expectError(error.InvalidNativeLakeTextCorpus, malformed.validate());
}

test "external lake packed readers retain legacy directory compatibility and resource pressure fallback" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("packed-text-legacy");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(5), 1, std.testing.io);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const leaf = try upload(ca, &store, "legacy-native-bytes", .none);
    const legacy: Directory = .{ .version = 2, .bytes = leaf.byte_len, .metadata = &.{}, .blocks = &.{.{ .offset = 0, .ref = leaf }}, .terms = &.{} };
    const ref = try upload(ca, &store, try std.json.Stringify.valueAlloc(ca, legacy, .{}), .none);
    const Resources = local.storage_resource_manager;
    var budgets = Resources.Options.defaultBudgets();
    budgets[@backingInt(Resources.Slice.lite_native_page_cache)] = .{ .soft_limit_bytes = 1, .hard_limit_bytes = 1 };
    var manager = Resources.ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(a);
    const data = try load(a, .{ .store = store, .cache = null, .context = .{}, .cancellation = .none, .resource_manager = &manager }, ref);
    var physical = data.native;
    defer physical.close();
    var capability: Read = .{ .store = store, .cache = null, .context = .{ .io = std.testing.io }, .cancellation = .none };
    var query = try physical.ranges.bind_read_context.?(physical.ranges.ptr, a, &capability);
    defer query.close();
    try std.testing.expect(query.resourceManager() == &manager);
    var cache = try local.segment_source.ConcurrentBlockCache.init(a, query, 256 * 1024);
    defer cache.deinit();
    var sample: [6]u8 = undefined;
    try cache.readInto(0, &sample);
    try std.testing.expectEqualStrings("legacy", &sample);
    try std.testing.expectEqual(@as(usize, 0), cache.retainedBytes());
}
