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

//! Authenticated metadata plus independently addressable posting blocks.
//! Native segment offsets, dictionaries, norms and global scoring stay intact.
const std = @import("std");
const local = @import("antfly_local_sources");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const decoded = @import("lake_index_decoded_metadata.zig");
const stores = @import("../serverless/artifacts/store.zig");
const Cancellation = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
const Ref = artifacts.ChunkRef;
const block_bytes: usize = 64 * 1024;
const legacy_block_bytes: usize = 256 * 1024;
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
    version: u16 = 4,
    /// Optional compressed opening pages. Metadata pieces still name original
    /// immutable pack ranges, so residency denial can use bounded range reads.
    metadata_bundle: ?Ref = null,
    metadata_decoded_bytes: usize = 0,
    bytes: usize,
    metadata: []const Piece,
    blocks: []const Piece,
    terms: []const Range,
    pub fn validate(self: Directory) !void {
        if ((self.version < 1 or self.version > 5) or self.bytes == 0 or self.bytes > 32 * 1024 * 1024 or self.metadata.len > 16384 or self.blocks.len > 16384 or self.terms.len > 200000) return error.InvalidNativeLakeTextCorpus;
        if (self.metadata_bundle) |bundle| {
            if (self.version != 5 or self.metadata.len == 0 or self.metadata_decoded_bytes == 0 or self.metadata_decoded_bytes > pack_bytes or bundle.byte_len == 0 or bundle.byte_len >= self.metadata_decoded_bytes) return error.InvalidNativeLakeTextCorpus;
            try stores.validateSha256ArtifactIdentity(bundle.artifact_id, bundle.checksum);
            var total: u64 = 0;
            for (self.metadata) |piece| total += piece.ref.byte_len;
            if (total != self.metadata_decoded_bytes) return error.InvalidNativeLakeTextCorpus;
        } else if (self.metadata_decoded_bytes != 0) return error.InvalidNativeLakeTextCorpus;
        // The two sorted streams must cover the original segment exactly.
        var position: usize = 0;
        var m: usize = 0;
        var b: usize = 0;
        while (m < self.metadata.len or b < self.blocks.len) {
            const meta = b == self.blocks.len or (m < self.metadata.len and self.metadata[m].offset < self.blocks[b].offset);
            const piece = if (meta) self.metadata[m] else self.blocks[b];
            if (piece.offset != position or piece.ref.byte_len == 0 or piece.ref.byte_len > self.bytes - position or (!meta and piece.ref.byte_len > (if (self.version < 4) legacy_block_bytes else block_bytes))) return error.InvalidNativeLakeTextCorpus;
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
pub const Descriptor = struct { manifest: local.serverless_manifest_artifact_ref.ArtifactRef, ordinal: usize };
pub const Descriptors = std.StringHashMapUnmanaged(Descriptor);
pub const Read = struct {
    /// Generation-owned descriptor routing. Shared physical sources never
    /// decide which generation's file manifest a reader must retain.
    descriptors: ?*const Descriptors = null,
    store: stores.ArtifactStore,
    cache: ?artifacts.CachedRead,
    context: local.serverless_query_lake_read_context.Context,
    cancellation: Cancellation,
    resource_manager: ?*local.storage_resource_manager.ResourceManager = null,
    fn check(self: Read) !void {
        try self.context.ensureActive();
        if (self.cache) |cached| try cached.context.ensureActive();
        try self.cancellation.check();
    }
};
fn upload(a: A, store: *stores.ArtifactStore, bytes: []const u8, cancellation: Cancellation) !Ref {
    var writer = store.*;
    writer.allocator = a;
    const ref = try writer.putWithCancellation(bytes, cancellation);
    defer if (ref.provider_version) |version| writer.allocator.free(version);
    return .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len };
}
fn initialMetadata(a: A, source: local.index.SegmentSource) ![]const Range {
    const length = std.math.cast(usize, source.len()) orelse return error.NativeLakeTextCorpusTooLarge;
    var footer: [24]u8 = undefined;
    // Opaque/legacy fixtures keep their existing all-block representation.
    if (length < 56) return &.{};
    try source.readInto(length - footer.len, &footer);
    if (!std.mem.eql(u8, footer[20..], "AFSM")) return &.{};
    var reader = try local.segment.RangeSegmentReader.init(a, source, .{});
    reader.owns_source = false;
    defer reader.deinit();
    var ranges: std.ArrayList(Range) = .empty;
    try ranges.append(a, .{ .offset = @intCast(reader.stored_offset), .len = @intCast(@min(21, reader.stored_metadata_length)) });
    if (reader.num_blocks != 0) try ranges.append(a, .{
        .offset = @intCast(reader.stored_offset + 21 + @as(u64, reader.doc_count) * 24),
        .len = @as(usize, reader.num_blocks) * 8,
    });
    const tail = if (reader.paged_source) |paged| paged.directory.offset else std.mem.readInt(u64, footer[0..8], .big);
    try ranges.append(a, .{ .offset = @intCast(tail), .len = length - @as(usize, @intCast(tail)) });
    for (reader.fields) |field| for (field.sections) |section| {
        if (section.section_type != .inverted_text or section.length == 0) continue;
        const view = (try reader.sectionView(field.name, .inverted_text)).?;
        const inverted = try local.section_inverted.RangeInvertedIndexReader.init(a, view, pack_bytes);
        for (try inverted.initialMetadataViews()) |metadata| if (metadata) |range| {
            try ranges.append(a, .{ .offset = @intCast(range.offset), .len = @intCast(range.length) });
        };
    };
    std.mem.sort(Range, ranges.items, {}, struct {
        fn less(_: void, lhs: Range, rhs: Range) bool {
            return lhs.offset < rhs.offset;
        }
    }.less);
    var merged: std.ArrayList(Range) = .empty;
    for (ranges.items) |requested| {
        // Native v5 integrity checks authenticate complete 64 KiB pages.
        // Bundle those same pages so tiny metadata reads do not fetch adjacent
        // original-pack ranges solely to establish the native CRC proof.
        const page_start = requested.offset / block_bytes * block_bytes;
        const page_end = @min(length, std.mem.alignForward(usize, requested.offset + requested.len, block_bytes));
        const range: Range = if (reader.paged_source != null) .{ .offset = page_start, .len = page_end - page_start } else requested;
        if (merged.items.len != 0) {
            const last = &merged.items[merged.items.len - 1];
            if (range.offset <= last.offset + last.len) {
                last.len = @max(last.offset + last.len, range.offset + range.len) - last.offset;
                continue;
            }
        }
        try merged.append(a, range);
    }
    var metadata_length: usize = 0;
    for (merged.items) |range| metadata_length = try std.math.add(usize, metadata_length, range.len);
    // Oversized optional navigation stays in authenticated native blocks.
    return if (metadata_length <= pack_bytes and metadata_length < length / 2) merged.items else &.{};
}
fn packedPiece(a: A, pack: Ref, offset: usize, pack_offset: usize, bytes: []const u8) !Piece {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    const checksum = try a.dupe(u8, &std.fmt.bytesToHex(digest, .lower));
    const scope = (try stores.uploadScopeFromArtifactId(pack.artifact_id)) orelse return error.InvalidArtifactUploadScope;
    const id = try scope.artifactId(checksum);
    return .{ .offset = offset, .ref = .{ .artifact_id = try a.dupe(u8, &id), .checksum = checksum, .byte_len = bytes.len }, .pack = pack, .pack_offset = pack_offset };
}
/// Content-defined boundaries resynchronize after posting insertions/deletions.
/// Logical ranges remain at most 64 KiB and retain ordinary SHA256 proofs.
const gear_table = blk: {
    @setEvalBranchQuota(10000);
    var table: [256]u64 = undefined;
    for (&table, 0..) |*entry, byte| {
        var gear = @as(u64, byte) +% 0x9e3779b97f4a7c15;
        gear = (gear ^ (gear >> 30)) *% 0xbf58476d1ce4e5b9;
        gear = (gear ^ (gear >> 27)) *% 0x94d049bb133111eb;
        entry.* = gear ^ (gear >> 31);
    }
    break :blk table;
};
fn chunkLength(bytes: []const u8) usize {
    const maximum = @min(block_bytes, bytes.len);
    if (maximum <= 8 * 1024) return maximum;
    var rolling: u64 = 0;
    for (bytes[0..maximum], 0..) |byte, i| {
        rolling = (rolling << 1) +% gear_table[byte];
        if (i >= 8 * 1024 and rolling & 0x7fff == 0) return i + 1;
    }
    return maximum;
}
/// Operation-owned reusable immutable blocks. References keep their original
/// attempt/pack locators, so existing cloud certificates and GC trace them.
pub const Reuse = struct {
    a: A,
    budget: local.sql_memory_budget,
    arena: std.heap.ArenaAllocator = undefined,
    pieces: std.AutoHashMapUnmanaged([32]u8, Piece) = .empty,
    reused_bytes: u64 = 0,
    uploaded_bytes: u64 = 0,
    pub fn create(a: A, store: stores.ArtifactStore, refs: []const Ref, token: Cancellation) !?*Reuse {
        const self = try a.create(Reuse);
        self.* = .{ .a = a, .budget = .{ .backing = local.sql_memory_budget.reclaimingAllocator(a), .limit = 16 * 1024 * 1024 } };
        self.arena = .init(self.budget.allocator());
        var owned = true;
        defer if (owned) self.deinit();
        self.prepare(store, refs, token) catch |err| {
            if (err == error.OutOfMemory and self.budget.isExhausted()) return null;
            return err;
        };
        owned = false;
        return self;
    }
    fn prepare(self: *Reuse, store: stores.ArtifactStore, refs: []const Ref, token: Cancellation) !void {
        const a = self.arena.allocator();
        for (refs) |ref| {
            try token.check();
            const directory = try loadDirectoryInner(a, .{ .store = store, .cache = null, .context = .{}, .cancellation = token }, ref, true);
            for (directory.blocks) |piece| try self.pieces.put(a, try stores.sha256DigestFromChecksum(piece.ref.checksum), piece);
        }
    }
    pub fn deinit(self: *Reuse) void {
        self.arena.deinit();
        self.a.destroy(self);
    }
};
/// Repack only changed content. A merge may change every native byte offset,
/// while unchanged posting blocks keep their authenticated old physical ranges.
pub fn publishSourceReusing(a: A, out: A, store: *stores.ArtifactStore, source: local.index.SegmentSource, cancellation: Cancellation, opening: ?*@import("lake_index_text_opening_pack.zig").Collector, reuse: ?*Reuse) !Ref {
    const previous = reuse orelse return publishSourceWithOpeningCollector(a, out, store, source, cancellation, opening);
    const length = std.math.cast(usize, source.len()) orelse return error.NativeLakeTextCorpusTooLarge;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const ranges = try initialMetadata(ca, source);
    var metadata: std.ArrayList(Piece) = .empty;
    var blocks: std.ArrayList(Piece) = .empty;
    var compressed_bundle: ?Ref = null;
    var decoded_bytes: usize = 0;
    if (ranges.len != 0) {
        var bundle: std.ArrayList(u8) = .empty;
        for (ranges) |range| try source.readInto(range.offset, try bundle.addManyAsSlice(ca, range.len));
        const compressed = try local.encoding_snappy.encode(ca, bundle.items);
        if (compressed.len <= bundle.items.len - bundle.items.len / 8) {
            compressed_bundle = try upload(ca, store, compressed, cancellation);
            if (opening) |collector| try collector.add(compressed_bundle.?, compressed);
            decoded_bytes = bundle.items.len;
        } else {
            const pack = try upload(ca, store, bundle.items, cancellation);
            if (opening) |collector| try collector.add(pack, bundle.items);
            var offset: usize = 0;
            for (ranges) |range| {
                try metadata.append(ca, try packedPiece(ca, pack, range.offset, offset, bundle.items[offset..][0..range.len]));
                offset += range.len;
            }
        }
    }
    const Pending = struct { offset: usize, bytes: []const u8, pack_offset: usize, pack_slot: usize, retained: ?Piece, metadata: bool };
    var position: usize = 0;
    var meta: usize = 0;
    var ahead: ?*PackReadAhead = null;
    defer if (ahead) |job| job.deinit(store.io_phase.?.io);
    while (position < length) {
        try cancellation.check();
        var wave = std.heap.ArenaAllocator.init(a);
        defer wave.deinit();
        const wa = wave.allocator();
        var contents: [stores.ArtifactStore.max_upload_batch][]const u8 = undefined;
        var count: usize = 0;
        var next = position;
        const reuse_width = @max(1, @min(uploadWidth(a, store), (@import("../serverless/build/lake_build_limits.zig").WorkingSetAllocator.headroom(a) orelse 0) / (4 * pack_bytes)));
        const width = if (store.io_phase != null and @import("lake_index_segment_spill.zig").LocalSegment.readAheadSource(source) != null)
            @min(reuse_width, @max(1, store.io_phase.?.max_parallel -| 1))
        else
            reuse_width;
        const ready = ahead;
        ahead = null;
        defer if (ready) |job| job.deinit(store.io_phase.?.io);
        if (ready) |job| {
            try job.join(store);
            count = job.count;
            for (contents[0..count], 0..) |*payload, i| payload.* = job.bytes[i * pack_bytes .. @min(job.bytes.len, (i + 1) * pack_bytes)];
            next += job.bytes.len;
        } else while (count < width and next < length) : (count += 1) {
            const end = @min(length, next + pack_bytes);
            contents[count] = if (source == .contiguous) source.contiguous[next..end] else blk: {
                const bytes = try wa.alloc(u8, end - next);
                try source.readInto(next, bytes);
                break :blk bytes;
            };
            next = end;
        }
        if (next < length) ahead = PackReadAhead.start(a, store, source, next, width, cancellation);
        var pending: std.ArrayList(Pending) = .empty;
        var uploads: [stores.ArtifactStore.max_upload_batch][]const u8 = undefined;
        var upload_count: usize = 0;
        var base = position;
        for (contents[0..count]) |payload| {
            const end = base + payload.len;
            var changed: std.ArrayList(u8) = .empty;
            var offset = base;
            while (offset < end) {
                while (meta < ranges.len and ranges[meta].offset + ranges[meta].len <= offset) meta += 1;
                const is_meta = meta < ranges.len and ranges[meta].offset <= offset;
                const limit = @min(end, if (meta < ranges.len) (if (is_meta) ranges[meta].offset + ranges[meta].len else ranges[meta].offset) else length);
                if (is_meta and compressed_bundle == null) {
                    offset = limit;
                    continue;
                }
                const following = if (is_meta) limit else offset + chunkLength(payload[offset - base .. limit - base]);
                const bytes = payload[offset - base .. following - base];
                var digest: [32]u8 = undefined;
                std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
                const retained = if (!is_meta) previous.pieces.get(digest) else null;
                const reused = if (retained) |piece| piece.ref.byte_len == bytes.len else false;
                try pending.append(wa, .{ .offset = offset, .bytes = bytes, .pack_offset = changed.items.len, .pack_slot = upload_count, .retained = if (reused) retained else null, .metadata = is_meta });
                if (!reused) try changed.appendSlice(wa, bytes);
                offset = following;
            }
            if (changed.items.len != 0) {
                uploads[upload_count] = changed.items;
                upload_count += 1;
            }
            previous.uploaded_bytes += changed.items.len;
            base = end;
        }
        // One bounded transport wave shares fresh publication's admission and
        // drain contract. Retained ranges keep their original immutable packs.
        var uploaded: [stores.ArtifactStore.max_upload_batch]stores.ArtifactMetadata = undefined;
        try store.putBatchWithCancellation(if (store.io_phase) |phase| phase.io else null, uploads[0..upload_count], uploaded[0..upload_count], cancellation);
        defer for (uploaded[0..upload_count]) |*item| item.deinit(store.allocator);
        var packs: [stores.ArtifactStore.max_upload_batch]Ref = undefined;
        for (uploaded[0..upload_count], packs[0..upload_count]) |item, *pack| pack.* = .{ .artifact_id = try ca.dupe(u8, item.artifact_id), .checksum = try ca.dupe(u8, item.checksum), .byte_len = item.byte_len };
        for (pending.items) |item| {
            const piece = if (item.retained) |retained| blk: {
                var result = retained;
                result.offset = item.offset;
                previous.reused_bytes += item.bytes.len;
                break :blk result;
            } else try packedPiece(ca, packs[item.pack_slot], item.offset, item.pack_offset, item.bytes);
            if (item.metadata) try metadata.append(ca, piece) else try blocks.append(ca, piece);
        }
        position = next;
    }
    const directory: Directory = .{ .version = if (compressed_bundle != null) 5 else 4, .metadata_bundle = compressed_bundle, .metadata_decoded_bytes = decoded_bytes, .bytes = length, .metadata = metadata.items, .blocks = blocks.items, .terms = &.{} };
    try directory.validate();
    const encoded = try std.json.Stringify.valueAlloc(ca, directory, .{});
    if (encoded.len > 4 * 1024 * 1024) return error.NativeLakeTextCorpusTooLarge;
    const result = try upload(out, store, encoded, cancellation);
    if (opening) |collector| try collector.add(result, encoded);
    return result;
}
fn uploadWidth(a: A, store: *const stores.ArtifactStore) usize {
    const phase = store.io_phase orelse return 1;
    const available = @import("../serverless/build/lake_build_limits.zig").WorkingSetAllocator.headroom(a) orelse return 1;
    // Borrowed payloads need no capture buffer. Leave conservative headroom
    // for bounded provider staging and returned identities during parallel I/O.
    return @max(1, @min(stores.ArtifactStore.max_upload_batch, phase.max_parallel, available / (2 * pack_bytes)));
}
/// One optional next wave. The caller owns the source until cancellation/join
/// drains; a child never borrows its parent's CPU/I/O phase or arena state.
const PackReadAhead = struct {
    const Scheduler = local.sql_parallel_scheduler;
    a: A,
    source: local.index.SegmentSource,
    token: Cancellation,
    offset: usize,
    bytes: []u8,
    count: usize,
    scheduler: *Scheduler.Scheduler,
    reservation: usize,
    task: ?Scheduler.Task(anyerror!void) = null,
    fn run(self: *PackReadAhead) anyerror!void {
        try self.token.check();
        try self.source.readInto(self.offset, self.bytes);
        try self.token.check();
    }
    fn start(a: A, store: *stores.ArtifactStore, source: local.index.SegmentSource, offset: usize, width: usize, token: Cancellation) ?*PackReadAhead {
        const phase = store.io_phase orelse return null;
        const scheduler = phase.scheduler orelse return null;
        if (phase.park == null or phase.resume_cpu == null or phase.max_parallel < 2) return null;
        const raw = @import("lake_index_segment_spill.zig").LocalSegment.readAheadSource(source) orelse return null;
        const length = @min(width * pack_bytes, source.len() - offset);
        if (length == 0) return null;
        const reservation = length * 2 + 64 * 1024;
        if ((@import("../serverless/build/lake_build_limits.zig").WorkingSetAllocator.headroom(a) orelse 0) < reservation) return null;
        if (!scheduler.reservePull(0, reservation)) return null;
        var transferred = false;
        defer if (!transferred) scheduler.releasePull(0, reservation);
        const job = a.create(PackReadAhead) catch return null;
        var job_live = true;
        defer if (job_live) a.destroy(job);
        const bytes = a.alloc(u8, length) catch return null;
        var bytes_live = true;
        defer if (bytes_live) a.free(bytes);
        job.* = .{ .a = a, .source = raw, .token = token, .offset = offset, .bytes = bytes, .count = std.math.divCeil(usize, length, pack_bytes) catch unreachable, .scheduler = scheduler, .reservation = reservation };
        job.task = scheduler.submitIoTransient(phase.io, 0, run, .{job}) orelse return null;
        transferred = true;
        job_live = false;
        bytes_live = false;
        return job;
    }
    fn join(self: *PackReadAhead, store: *stores.ArtifactStore) !void {
        const phase = store.io_phase.?;
        phase.park.?(phase.ptr);
        const result = self.task.?.await(phase.io);
        self.task = null;
        try phase.resume_cpu.?(phase.ptr);
        try result;
        try self.token.check();
    }
    fn deinit(self: *PackReadAhead, io: std.Io) void {
        if (self.task) |*task| _ = task.cancel(io) catch {};
        self.a.free(self.bytes);
        self.scheduler.releasePull(0, self.reservation);
        self.a.destroy(self);
    }
};
pub fn publish(a: A, out: A, store: *stores.ArtifactStore, bytes: []const u8, cancellation: Cancellation) !Ref {
    return publishWithOpeningCollector(a, out, store, bytes, cancellation, null);
}
pub fn publishWithOpeningCollector(a: A, out: A, store: *stores.ArtifactStore, bytes: []const u8, cancellation: Cancellation, opening: ?*@import("lake_index_text_opening_pack.zig").Collector) !Ref {
    return publishSourceWithOpeningCollector(a, out, store, .{ .contiguous = bytes }, cancellation, opening);
}
/// Borrow an immutable source and capture at most one admitted pack wave.
/// File-backed compaction never needs a complete contiguous output buffer.
pub fn publishSource(a: A, out: A, store: *stores.ArtifactStore, source: local.index.SegmentSource, cancellation: Cancellation) !Ref {
    return publishSourceWithOpeningCollector(a, out, store, source, cancellation, null);
}
pub fn publishSourceWithOpeningCollector(a: A, out: A, store: *stores.ArtifactStore, source: local.index.SegmentSource, cancellation: Cancellation, opening: ?*@import("lake_index_text_opening_pack.zig").Collector) !Ref {
    const length = std.math.cast(usize, source.len()) orelse return error.NativeLakeTextCorpusTooLarge;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    // Upload large immutable packs, with independently authenticated bounded
    // range units. Broad reads coalesce adjacent units into one pack range GET,
    // while sparse queries never download an entire pack to authenticate it.
    const ranges = try initialMetadata(ca, source);
    var metadata: std.ArrayList(Piece) = .empty;
    var compressed_bundle: ?Ref = null;
    var decoded_bytes: usize = 0;
    if (ranges.len != 0) {
        var bundle: std.ArrayList(u8) = .empty;
        for (ranges) |range| {
            const target = try bundle.addManyAsSlice(ca, range.len);
            try source.readInto(range.offset, target);
        }
        const compressed = try local.encoding_snappy.encode(ca, bundle.items);
        if (compressed.len <= bundle.items.len - bundle.items.len / 8) {
            compressed_bundle = try upload(ca, store, compressed, cancellation);
            if (opening) |collector| try collector.add(compressed_bundle.?, compressed);
            decoded_bytes = bundle.items.len;
        } else {
            const pack = try upload(ca, store, bundle.items, cancellation);
            if (opening) |collector| try collector.add(pack, bundle.items);
            var offset: usize = 0;
            for (ranges) |range| {
                try metadata.append(ca, try packedPiece(ca, pack, range.offset, offset, bundle.items[offset..][0..range.len]));
                offset += range.len;
            }
        }
    }
    var blocks: std.ArrayList(Piece) = .empty;
    var position: usize = 0;
    var metadata_position: usize = 0;
    var pending: ?*PackReadAhead = null;
    defer if (pending) |job| job.deinit(store.io_phase.?.io);
    while (position < length) {
        var wave = std.heap.ArenaAllocator.init(a);
        defer wave.deinit();
        var contents: [stores.ArtifactStore.max_upload_batch][]const u8 = undefined;
        var uploaded: [stores.ArtifactStore.max_upload_batch]stores.ArtifactMetadata = undefined;
        var count: usize = 0;
        var next = position;
        const width = if (store.io_phase != null and @import("lake_index_segment_spill.zig").LocalSegment.readAheadSource(source) != null)
            @min(uploadWidth(a, store), @max(1, store.io_phase.?.max_parallel -| 1))
        else
            uploadWidth(a, store);
        const ready = pending;
        pending = null;
        defer if (ready) |job| job.deinit(store.io_phase.?.io);
        if (ready) |job| {
            try job.join(store);
            count = job.count;
            for (contents[0..count], 0..) |*payload, i| payload.* = job.bytes[i * pack_bytes .. @min(job.bytes.len, (i + 1) * pack_bytes)];
            next += job.bytes.len;
        } else while (count < width and next < length) : (count += 1) {
            const end = @min(next + pack_bytes, length);
            if (source == .contiguous) {
                contents[count] = source.contiguous[next..end];
            } else {
                const captured = try wave.allocator().alloc(u8, end - next);
                try source.readInto(next, captured);
                contents[count] = captured;
            }
            next = end;
        }
        if (next < length) pending = PackReadAhead.start(a, store, source, next, width, cancellation);
        // Payloads borrow the already-admitted immutable segment. Metadata
        // stays on the synchronized store allocator until every child drains;
        // only this serial owner mutates descriptor arenas after CPU resumes.
        try store.putBatchWithCancellation(if (store.io_phase) |phase| phase.io else null, contents[0..count], uploaded[0..count], cancellation);
        defer for (uploaded[0..count]) |*item| item.deinit(store.allocator);
        for (contents[0..count], uploaded[0..count]) |payload, item| {
            const pack: Ref = .{ .artifact_id = try ca.dupe(u8, item.artifact_id), .checksum = try ca.dupe(u8, item.checksum), .byte_len = item.byte_len };
            const pack_end = position + payload.len;
            var offset = position;
            while (offset < pack_end) {
                while (metadata_position < ranges.len and ranges[metadata_position].offset + ranges[metadata_position].len <= offset) metadata_position += 1;
                if (metadata_position < ranges.len and ranges[metadata_position].offset <= offset) {
                    const end = @min(pack_end, ranges[metadata_position].offset + ranges[metadata_position].len);
                    if (compressed_bundle != null) try metadata.append(ca, try packedPiece(ca, pack, offset, offset - position, payload[offset - position .. end - position]));
                    offset = end;
                    continue;
                }
                const limit = @min(pack_end, if (metadata_position < ranges.len) ranges[metadata_position].offset else length);
                const end = offset + chunkLength(payload[offset - position .. limit - position]);
                try blocks.append(ca, try packedPiece(ca, pack, offset, offset - position, payload[offset - position .. end - position]));
                offset = end;
            }
            position = pack_end;
        }
    }
    const directory: Directory = .{ .version = if (compressed_bundle != null) 5 else 4, .metadata_bundle = compressed_bundle, .metadata_decoded_bytes = decoded_bytes, .bytes = length, .metadata = metadata.items, .blocks = blocks.items, .terms = &.{} };
    try directory.validate();
    const encoded = try std.json.Stringify.valueAlloc(ca, directory, .{});
    if (encoded.len > 4 * 1024 * 1024) return error.NativeLakeTextCorpusTooLarge;
    const ref = try upload(out, store, encoded, cancellation);
    if (opening) |collector| try collector.add(ref, encoded);
    return ref;
}
pub fn loadDirectory(a: A, read: Read, ref: Ref) !Directory {
    return loadDirectoryInner(a, read, ref, false);
}
fn loadDirectoryInner(a: A, read: Read, ref: Ref, publication: bool) !Directory {
    if (ref.byte_len > 4 * 1024 * 1024) return error.InvalidNativeLakeTextCorpus;
    const bytes = if (publication) try artifacts.readPublicationArtifact(a, read.store, ref, read.cancellation) else try artifacts.readArtifact(a, read.store, ref, read.cancellation, read.cache);
    defer a.free(bytes);
    const directory = try std.json.parseFromSliceLeaky(Directory, a, bytes, .{ .allocate = .alloc_always });
    try directory.validate();
    const scope = (try stores.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.InvalidNativeLakeTextCorpus;
    if (directory.metadata_bundle) |bundle| {
        const child = (try stores.uploadScopeFromArtifactId(bundle.artifact_id)) orelse return error.InvalidNativeLakeTextCorpus;
        if (!std.mem.eql(u8, &child.domain, &scope.domain)) return error.InvalidNativeLakeTextCorpus;
    }
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
const Decoded = local.serverless_query_lake_decoded_cache;
const ServingCache = local.serverless_query_lake_serving_cache.Cache;
const PhysicalResult = struct {
    const Unit = struct { offset: u64, length: u64, result: anyerror!ServingCache.VerifiedBytes };
    fn get(self: *const @This(), piece: Piece) !?ServingCache.VerifiedBytes {
        for (self.units) |unit| if (unit.offset == piece.pack_offset and unit.length == piece.ref.byte_len) {
            const value = try unit.result;
            const digest = try stores.sha256DigestFromChecksum(piece.ref.checksum);
            if (!std.mem.eql(u8, &value.digest, &digest)) return error.ArtifactIntegrityMismatch;
            return value;
        };
        return null;
    }
    units: []const Unit,
    start: u64,
    end: u64,
};
const PhysicalPin = struct {
    lease: Decoded.Lease,
    fn release(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.lease.release();
        std.heap.page_allocator.destroy(self);
    }
    fn retain(lease: Decoded.Lease, value: ServingCache.VerifiedBytes) !ServingCache.VerifiedLease {
        const self = try std.heap.page_allocator.create(@This());
        self.* = .{ .lease = lease.retain() };
        return .{ .value = value, .owner = .{ .bytes = value.bytes, .owner = .{ .shared = .{ .ptr = self, .release_fn = release } } } };
    }
};
const PhysicalWarm = struct {
    lease: ServingCache.ImmutableLease,
    fn release(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.lease.deinit();
        std.heap.page_allocator.destroy(self);
    }
    fn retain(self: @This(), digest: [32]u8) !ServingCache.VerifiedLease {
        var owned = self.lease;
        errdefer owned.deinit();
        const holder = try std.heap.page_allocator.create(@This());
        holder.* = self;
        const bytes = owned.bytes();
        return .{ .value = .{ .bytes = bytes, .digest = digest }, .owner = .{ .bytes = bytes, .owner = .{ .shared = .{ .ptr = holder, .release_fn = release } } } };
    }
};
fn requiredRange(a: A, store: stores.ArtifactStore, id: []const u8, offset: u64, length: usize, token: Cancellation, publication: bool) ![]u8 {
    var phase = if (publication) try store.beginIo(1) else stores.IoGuard{};
    defer phase.abort();
    var backend = store;
    backend.io_phase = null;
    const bytes = backend.getRangeAllocWithCancellationUsingAllocator(a, id, offset, length, token) catch |err| return phase.failed(err);
    errdefer a.free(bytes);
    try phase.finish();
    try token.check();
    return bytes;
}
const CoalescedRead = struct {
    publication: bool = false,
    store: stores.ArtifactStore,
    pack: Ref,
    start: u64,
    length: usize,
    logical_end: usize,
    cancellation: Cancellation,
    a: A,
    bytes: ?[]u8 = null,
    pieces: []const Piece = &.{},
    shared: ?Decoded.Lease = null,
    reused: std.ArrayList(Decoded.Lease) = .empty,
    fn deinit(self: *@This()) void {
        if (self.bytes) |bytes| self.a.free(bytes);
        if (self.shared) |lease| lease.release();
        for (self.reused.items) |lease| lease.release();
        self.reused.deinit(self.a);
    }
    fn loadPhysical(raw: *anyopaque, item: *Decoded.Item) !void {
        const loader: *PhysicalLoader = @ptrCast(@alignCast(raw));
        const self = loader.group;
        const cache = loader.cached;
        const a = item.arena.allocator();
        var units: std.ArrayList(PhysicalResult.Unit) = .empty;
        const missing = try a.alloc(bool, self.pieces.len);
        // All joined flights have completed before this loader is registered.
        // Reuse their immutable units without waiting while owning a flight.
        try cache.cache.physical.dependMany(item, self.reused.items);
        for (self.pieces, missing) |piece, *miss| {
            try self.cancellation.check();
            const reused = found: {
                for (self.reused.items) |lease| {
                    const result: *PhysicalResult = @ptrCast(@alignCast(lease.item.payload.extension));
                    for (result.units) |unit| if (unit.offset == piece.pack_offset and unit.length == piece.ref.byte_len) {
                        // Successful slices must match this directory's proof.
                        if (unit.result) |value| {
                            if (!std.mem.eql(u8, &value.digest, &(try stores.sha256DigestFromChecksum(piece.ref.checksum)))) return error.ArtifactIntegrityMismatch;
                        } else |_| {}
                        try units.append(a, unit);
                        break :found true;
                    };
                }
                break :found false;
            };
            if (reused) {
                miss.* = false;
                continue;
            }
            miss.* = !(cache.cache.probeImmutableBlock(a, cache.scope, piece.ref.artifact_id, @intCast(piece.ref.byte_len), try stores.sha256DigestFromChecksum(piece.ref.checksum), cache.context) catch |err| {
                cache.context.ensureActive() catch return error.Canceled;
                return err;
            });
        }
        var first: usize = 0;
        while (first < missing.len) {
            if (!missing[first]) {
                first += 1;
                continue;
            }
            var end = first + 1;
            while (end < missing.len and missing[end]) : (end += 1) {}
            const start = self.pieces[first].pack_offset;
            const last = self.pieces[end - 1];
            const length: usize = @intCast(last.pack_offset + last.ref.byte_len - start);
            const bytes = self.store.getRangeAllocWithCancellationUsingAllocator(a, self.pack.artifact_id, start, length, self.cancellation) catch |err| {
                try self.cancellation.check();
                cache.context.ensureActive() catch return error.Canceled;
                if (err == error.OutOfMemory) return err;
                for (self.pieces[first..end]) |piece| try units.append(a, .{ .offset = piece.pack_offset, .length = piece.ref.byte_len, .result = err });
                first = end;
                continue;
            };
            cache.cache.recordPhysicalRead(bytes.len);
            for (self.pieces[first..end]) |piece| {
                const offset: usize = @intCast(piece.pack_offset - start);
                const value: anyerror!ServingCache.VerifiedBytes = if (bytes.len != length) error.ArtifactIntegrityMismatch else ServingCache.VerifiedBytes.authenticate(bytes[offset..][0..@intCast(piece.ref.byte_len)], try stores.sha256DigestFromChecksum(piece.ref.checksum));
                try units.append(a, .{ .offset = piece.pack_offset, .length = piece.ref.byte_len, .result = value });
            }
            first = end;
        }
        const result = try a.create(PhysicalResult);
        result.* = .{ .start = self.start, .end = self.start + self.length, .units = units.items };
        item.payload = .{ .extension = result };
    }
    const PhysicalLoader = struct {
        group: *CoalescedRead,
        cached: artifacts.CachedRead,
        fn canceled(raw: *const anyopaque) bool {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            self.group.cancellation.check() catch return true;
            self.cached.context.ensureActive() catch return true;
            return false;
        }
    };
    fn remainingCoverage(self: *const @This(), domain: [32]u8) Decoded.Cache.Coverage {
        const end = self.start + self.length;
        var begin = self.start;
        while (begin < end) {
            var covered = begin;
            for (self.reused.items) |lease| {
                const result: *const PhysicalResult = @ptrCast(@alignCast(lease.item.payload.extension));
                if (result.start <= begin) covered = @max(covered, @min(end, result.end));
            }
            if (covered != begin) {
                begin = covered;
                continue;
            }
            var finish = end;
            for (self.reused.items) |lease| {
                const result: *const PhysicalResult = @ptrCast(@alignCast(lease.item.payload.extension));
                if (result.start > begin) finish = @min(finish, result.start);
            }
            return .{ .domain = domain, .start = begin, .end = finish, .overlap = true };
        }
        // All intervals have been composed. Build our final immutable result
        // without joining an already consumed partial interval again.
        return .{ .domain = domain, .start = self.start, .end = end };
    }
    fn verifiedLease(self: *@This(), a: A, piece: Piece, cache: artifacts.CachedRead) !ServingCache.VerifiedLease {
        // One pack-scoped flight, acquired without waiting on other unit
        // flights. Its owned result contains only verified cold runs.
        if (self.shared) |lease| {
            const result: *PhysicalResult = @ptrCast(@alignCast(lease.item.payload.extension));
            if (try result.get(piece)) |value| return PhysicalPin.retain(lease, value);
            try self.reused.append(self.a, lease);
            self.shared = null;
        }
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("native-lake-physical-pack-flight-v1");
        hash.update(&cache.scope);
        hash.update(self.pack.artifact_id);
        hash.update(self.pack.checksum);
        const domain = hash.finalResult();
        var interval: [16]u8 = undefined;
        std.mem.writeInt(u64, interval[0..8], self.start, .little);
        std.mem.writeInt(u64, interval[8..16], self.start + self.length, .little);
        var ranged = std.crypto.hash.sha2.Sha256.init(.{});
        ranged.update(&domain);
        ranged.update(&interval);
        var loader: PhysicalLoader = .{ .group = self, .cached = cache };
        var flight_context = cache.context;
        // Publish request-authority failures as cancellation so a live
        // waiter retries under its own capability instead of inheriting a
        // leader's expired reader lease. Restore our exact error below.
        flight_context.checkpoint = null;
        flight_context.cancellation = .fromCallback(&loader, PhysicalLoader.canceled);
        while (true) {
            try self.cancellation.check();
            const lease = if (cache.context.io != null)
                cache.cache.physical.acquireCovered(ranged.finalResult(), pack_bytes * 3, flight_context, .{ .ptr = &loader, .load = loadPhysical }, self.remainingCoverage(domain)) catch |err| {
                    try cache.context.ensureActive();
                    try self.cancellation.check();
                    return err;
                }
            else inline_load: {
                const value = try cache.cache.physical.create(pack_bytes * 3);
                errdefer value.release();
                try loadPhysical(&loader, value.item);
                break :inline_load value;
            };
            var owns_lease = true;
            defer if (owns_lease) lease.release();
            const result: *PhysicalResult = @ptrCast(@alignCast(lease.item.payload.extension));
            if (result.start > self.start or result.end < self.start + self.length) {
                try self.reused.append(self.a, lease);
                owns_lease = false;
                continue;
            }
            if (try result.get(piece)) |value| {
                self.shared = lease;
                owns_lease = false;
                return PhysicalPin.retain(lease, value);
            }
            // Residency can change while a flight is planned. Recheck before
            // falling back to a direct unit read.
            if (try cache.cache.lookupImmutableBlockLease(a, cache.scope, piece.ref.artifact_id, @intCast(piece.ref.byte_len), try stores.sha256DigestFromChecksum(piece.ref.checksum), cache.context)) |warm| {
                const owner: PhysicalWarm = .{ .lease = warm };
                return try owner.retain(try stores.sha256DigestFromChecksum(piece.ref.checksum));
            }
            // A concurrent flight may cover a different requested subset,
            // or this unit became warm while its leader was being planned.
            // A single-unit direct fallback cannot form a flight cycle.
            const bytes = try self.store.getRangeAllocWithCancellationUsingAllocator(a, self.pack.artifact_id, piece.pack_offset, @intCast(piece.ref.byte_len), self.cancellation);
            cache.cache.recordPhysicalRead(bytes.len);
            errdefer a.free(bytes);
            const value = try ServingCache.VerifiedBytes.authenticate(bytes, try stores.sha256DigestFromChecksum(piece.ref.checksum));
            return .{ .value = value, .owner = .{ .bytes = bytes, .owner = .{ .allocation = a } } };
        }
    }
    fn range(self: *@This(), a: A, piece: Piece) ![]u8 {
        if (self.bytes == null) {
            self.bytes = try requiredRange(self.a, self.store, self.pack.artifact_id, self.start, self.length, self.cancellation, self.publication);
            if (self.bytes.?.len != self.length) return error.ArtifactIntegrityMismatch;
        }
        const offset: usize = @intCast(piece.pack_offset - self.start);
        return a.dupe(u8, self.bytes.?[offset..][0..@intCast(piece.ref.byte_len)]);
    }
};
fn readPieceLease(a: A, store: stores.ArtifactStore, piece: Piece, cancellation: Cancellation, cached: ?artifacts.CachedRead) !local.serverless_query_lake_serving_cache.Cache.ImmutableLease {
    return readPieceLeaseCoalesced(a, store, piece, cancellation, cached, null, false);
}
fn readPieceLeaseCoalesced(a: A, store: stores.ArtifactStore, piece: Piece, cancellation: Cancellation, cached: ?artifacts.CachedRead, coalesced: ?*CoalescedRead, publication: bool) !local.serverless_query_lake_serving_cache.Cache.ImmutableLease {
    if (piece.pack == null) {
        if (publication) return .{ .heap = .{ .alloc = a, .bytes = try artifacts.readPublicationArtifact(a, store, piece.ref, cancellation) } };
        return artifacts.readArtifactLease(a, store, piece.ref, cancellation, cached);
    }
    const Loader = struct {
        store: stores.ArtifactStore,
        piece: Piece,
        cancellation: Cancellation,
        coalesced: ?*CoalescedRead,
        cached: ?artifacts.CachedRead,
        publication: bool,
        traffic: local.serverless_query_lake_serving_cache.Cache.ProviderRead = .{ .requests = 0, .bytes = 0 },
        fn providerRead(raw: *anyopaque) local.serverless_query_lake_serving_cache.Cache.ProviderRead {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.traffic;
        }
        fn load(raw: *anyopaque, alloc: A) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.traffic = if (self.coalesced) |group| if (group.bytes == null) .{ .requests = 1, .bytes = group.length } else .{ .requests = 0, .bytes = 0 } else .{ .requests = 1, .bytes = self.piece.ref.byte_len };
            if (self.coalesced != null and self.cached != null) self.traffic = .{ .requests = 0, .bytes = 0 };
            const bytes = if (self.coalesced) |group| try group.range(alloc, self.piece) else try requiredRange(alloc, self.store, self.piece.pack.?.artifact_id, self.piece.pack_offset, @intCast(self.piece.ref.byte_len), self.cancellation, self.publication);
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
    if (cached) |cache| {
        const length: usize = @intCast(piece.ref.byte_len);
        const digest = try stores.sha256DigestFromChecksum(piece.ref.checksum);
        if (try cache.cache.lookupImmutableBlockLease(a, cache.scope, piece.ref.artifact_id, length, digest, cache.context)) |lease| return lease;
        const verified = if (coalesced) |group| try group.verifiedLease(a, piece, cache) else direct: {
            var provider = store;
            const bytes = try provider.getRangeAllocWithCancellationUsingAllocator(a, piece.pack.?.artifact_id, piece.pack_offset, length, cancellation);
            errdefer a.free(bytes);
            cache.cache.recordPhysicalRead(bytes.len);
            break :direct ServingCache.VerifiedLease{ .value = try ServingCache.VerifiedBytes.authenticate(bytes, digest), .owner = .{ .bytes = bytes, .owner = .{ .allocation = a } } };
        };
        return cache.cache.admitVerifiedBlock(a, cache.scope, piece.ref.artifact_id, length, digest, cache.context, verified);
    }
    var loader: Loader = .{ .store = store, .piece = piece, .cancellation = cancellation, .coalesced = coalesced, .cached = cached, .publication = publication };
    return .{ .heap = .{ .alloc = a, .bytes = try Loader.load(&loader, a) } };
}
const Owner = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    directory: Directory,
    directory_bytes: usize = 0,
    directory_ref: ?Ref = null,
    manifest_ref: ?local.serverless_manifest_artifact_ref.ArtifactRef = null,
    manifest_ordinal: usize = 0,
    fallback: ?Read,
    query_scoped: bool = false,
    // Only the operation-owned loadPublication factory opts in. Keep the
    // shared Read capability and retained serving-owner footprint unchanged.
    publication: bool = false,
    metadata_bundle: []u8 = &.{},
    metadata_resident_bytes: std.atomic.Value(usize) = .init(0),
    resource_manager: ?*local.storage_resource_manager.ResourceManager = null,
    // Diagnostics track touched payload blocks without retaining payloads.
    loaded: []bool,
    mutex: std.atomic.Mutex = .unlocked,
    fn release(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const a = self.a;
        a.free(self.metadata_bundle);
        self.arena.deinit();
        a.destroy(self);
    }
    const DirectoryPin = struct {
        value: Directory,
        shared: ?decoded.Owned(Directory) = null,
        arena: ?std.heap.ArenaAllocator = null,
        fn release(self: *DirectoryPin) void {
            if (self.shared) |lease| lease.release();
            if (self.arena) |*arena| arena.deinit();
        }
    };
    fn directoryArtifact(self: *Owner) local.serverless_manifest_artifact_ref.ArtifactRef {
        const ref = self.directory_ref.?;
        return .{ .kind = .external_base_source, .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len };
    }
    fn decodeDirectory(a: A, store: stores.ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, token: Cancellation, cache: ?artifacts.CachedRead) !Directory {
        return loadDirectory(a, .{ .store = store, .cache = cache, .context = if (cache) |value| value.context else .{}, .cancellation = token }, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len });
    }
    fn pinDirectory(self: *Owner, capability: Read) !DirectoryPin {
        try capability.check();
        if (self.directory_ref == null) return .{ .value = self.directory };
        if (capability.cache) |cache| {
            const Guard = struct {
                read: *const Read,
                parent: local.serverless_query_lake_read_context.Context,
                fn check(raw: *anyopaque) !void {
                    const self_: *@This() = @ptrCast(@alignCast(raw));
                    try self_.read.check();
                    try self_.parent.ensureActive();
                }
            };
            // Decode admission and singleflight waits check both the current
            // capability and the cache context, even when their fences differ.
            var guard: Guard = .{ .read = &capability, .parent = cache.context };
            var scoped = cache;
            scoped.context.checkpoint = .{ .ptr = &guard, .check = Guard.check };
            const owned = try decoded.acquire(Directory, scoped, capability.store, self.directoryArtifact(), capability.cancellation, decodeDirectory);
            errdefer owned.release();
            if (owned.value.bytes != self.directory_bytes) return error.InvalidNativeLakeTextCorpus;
            try capability.check();
            return .{ .value = owned.value.*, .shared = owned };
        }
        // Uncached readers own only a call-local directory. Neither path
        // charges parsed directories to the long-lived native corpus heap.
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        errdefer arena.deinit();
        const value = try loadDirectory(arena.allocator(), capability, self.directory_ref.?);
        if (value.bytes != self.directory_bytes) return error.InvalidNativeLakeTextCorpus;
        return .{ .value = value, .arena = arena };
    }
    fn seal(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.query_scoped) self.fallback = null;
    }
    fn source(self: *@This()) local.index.SegmentSource {
        return .{ .ranges = .{ .ptr = self, .length = self.directory_bytes, .read_into = readInitial, .close = release, .bind_read_context = bind, .seal_read_context = seal, .retained_bytes = retainedBytes, .resource_manager = self.resource_manager } };
    }
    fn retainedBytes(raw: *anyopaque) usize {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return self.metadata_resident_bytes.load(.acquire);
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
        directory_pins: [4]?*WorkerPin = @splat(null),
        offsets: [4]?u64 = @splat(null),
        lengths: [4]u64 = @splat(0),
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
        const WorkerPin = struct {
            lease: local.serverless_query_lake_decoded_cache.Lease,
            released: std.atomic.Value(bool) = .init(false),
            fn release(self: *@This()) void {
                if (!self.released.swap(true, .acq_rel)) self.lease.release();
            }
            fn destroy(self: *@This()) void {
                // cancel-before-start does not invoke the worker's defer.
                self.release();
                std.heap.page_allocator.destroy(self);
            }
        };
        fn warm(self: *@This(), pieces: []const Piece, pin: ?*WorkerPin) anyerror!void {
            // Navigation belongs to the worker, so completion releases the pin
            // even when this segment is never visited by the query again.
            defer if (pin) |lease| lease.release();
            try self.capability.check();
            const token: Cancellation = .{ .ptr = self, .is_cancelled_fn = canceled };
            var cached = self.capability.cache;
            if (cached) |*cache| cache.context.cancellation = .{ .ptr = self, .is_cancelled_fn = canceled };
            const first = pieces[0];
            const last = pieces[pieces.len - 1];
            var group: ?CoalescedRead = if (first.pack != null) .{ .store = self.capability.store, .pack = first.pack.?, .start = first.pack_offset, .length = last.offset + @as(usize, @intCast(last.ref.byte_len)) - first.offset, .logical_end = last.offset + @as(usize, @intCast(last.ref.byte_len)), .cancellation = token, .a = std.heap.page_allocator, .pieces = pieces } else null;
            defer if (group) |*value| value.deinit();
            for (pieces) |piece| {
                try self.capability.check();
                var lease = try readPieceLeaseCoalesced(std.heap.page_allocator, self.capability.store, piece, token, cached, if (group) |*value| value else null, false);
                lease.deinit();
            }
        }
        fn drain(self: *@This(), cancel: bool) void {
            if (cancel) self.cancelled.store(true, .release);
            const io = self.io orelse return;
            for (&self.pending, &self.directory_pins) |*slot, *pin| if (slot.*) |*task| {
                if (cancel) task.cancel(io) catch {} else task.await(io) catch {};
                slot.* = null;
                if (pin.*) |owned| owned.destroy();
                pin.* = null;
            };
            self.offsets = @splat(null);
        }
        fn rememberPage(self: *@This(), page: u64) void {
            for (self.recent_pages) |prior| if (prior == page) return;
            self.recent_pages[self.recent_cursor] = page;
            self.recent_cursor = (self.recent_cursor + 1) % self.recent_pages.len;
        }
        fn reap(self: *@This(), io: std.Io) void {
            for (&self.pending, &self.offsets, &self.directory_pins) |*slot, *offset, *pin| {
                if (slot.*) |*task| {
                    if (!task.isComplete()) continue;
                    const succeeded = if (task.await(io)) |_| true else |_| false;
                    if (succeeded) self.rememberPage(offset.*.? / block_bytes);
                    slot.* = null;
                    offset.* = null;
                    if (pin.*) |owned| owned.destroy();
                    pin.* = null;
                }
            }
        }
        fn prefetch(raw: *anyopaque, start: u64, length: u64) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            // Required reads own cold directory discovery. Speculation never
            // loads it while holding the query's queue mutex.
            const cached = self.capability.cache orelse return;
            self.capability.check() catch return;
            const pin: ?decoded.Owned(Directory) = if (self.owner.directory_ref != null) (decoded.lookup(Directory, cached, self.owner.directoryArtifact()) catch return) orelse return else null;
            defer if (pin) |owned| owned.release();
            const directory = if (pin) |owned| owned.value.* else self.owner.directory;
            if (directory.bytes != self.owner.directory_bytes) return;
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            defer self.mutex.unlock();
            if (self.cancelled.load(.acquire)) return;
            // Without a shared cache speculative work cannot benefit a reader.
            if (self.capability.cache == null) return;
            const io = self.capability.context.io orelse return;
            self.capability.check() catch return;
            self.reap(io);
            if (start > directory.bytes or length > directory.bytes - start) return;
            var offset = start;
            var count: usize = 0;
            while (offset < start + length and count < self.pending.len) : (count += 1) {
                const metadata = containing(directory.metadata, @intCast(offset));
                const block = if (metadata == null) containing(directory.blocks, @intCast(offset)) else null;
                const piece = if (metadata) |i| directory.metadata[i] else if (block) |i| directory.blocks[i] else return;
                if (metadata != null and directory.metadata_bundle != null) {
                    offset = piece.offset + piece.ref.byte_len;
                    continue;
                }
                var pieces: []const Piece = if (metadata) |i| directory.metadata[i..][0..1] else directory.blocks[block.?..][0..1];
                if (block) |i| if (piece.pack) |pack| {
                    var end = i + 1;
                    var finish = piece.offset + piece.ref.byte_len;
                    while (end < directory.blocks.len) : (end += 1) {
                        const next = directory.blocks[end];
                        if (next.offset != finish or next.offset >= start + length or next.pack == null or !std.mem.eql(u8, next.pack.?.artifact_id, pack.artifact_id) or next.pack_offset != piece.pack_offset + finish - piece.offset) break;
                        finish += next.ref.byte_len;
                    }
                    pieces = directory.blocks[i..end];
                };
                const last = pieces[pieces.len - 1];
                offset = last.offset + last.ref.byte_len;
                // Legacy v1 artifacts can be large; keep speculative bytes bounded.
                if (piece.ref.byte_len > pack_bytes) continue;
                // Most native chunks share a page: do not spawn a worker for
                // every small decoder read of an already consumed page.
                var duplicate = false;
                for (self.recent_pages) |page| if (pieces.len == 1 and page == piece.offset / block_bytes) {
                    duplicate = true;
                    break;
                };
                for (self.offsets, self.lengths) |prior, span| if (prior) |position| {
                    if (position <= piece.offset and offset <= position + span) {
                        duplicate = true;
                        break;
                    }
                };
                if (duplicate) continue;
                var slot: usize = 0;
                while (slot < self.pending.len and self.pending[slot] != null) : (slot += 1) {}
                // A full speculative queue yields instead of stalling WAND
                // on an unvisited range. Required reads recycle consumed slots.
                if (slot == self.pending.len) return;
                const worker_pin: ?*WorkerPin = if (pin) |owned| blk: {
                    const worker = std.heap.page_allocator.create(WorkerPin) catch return;
                    worker.* = .{ .lease = owned.lease.retain() };
                    break :blk worker;
                } else null;
                self.pending[slot] = local.sql_parallel_scheduler.global().submitIoTransient(io, @as(usize, @intCast(offset - piece.offset)) * 2, warm, .{ self, pieces, worker_pin }) orelse {
                    if (worker_pin) |owned| owned.destroy();
                    return;
                };
                self.directory_pins[slot] = worker_pin;
                self.offsets[slot] = piece.offset;
                self.lengths[slot] = offset - piece.offset;
            }
        }
        fn read(raw: *anyopaque, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.cancelled.load(.acquire)) return error.Canceled;
            // Required broad reads coalesce ranges themselves; speculative
            // per-page GETs would duplicate that provider request.
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
            // A required cache hit must never join a larger speculative span.
            // Completed work can be recycled now; live work stays owned by
            // the query and is canceled/joined by quiesce before capability exit.
            if (self.capability.context.io) |io| self.reap(io);
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
        return .{ .ranges = .{ .ptr = query, .length = owner.directory_bytes, .read_into = Query.read, .close = Query.close, .prefetch = Query.prefetch, .quiesce_read_context = Query.quiesce, .resource_manager = capability.resource_manager orelse owner.resource_manager, .read_io = capability.context.io, .check_read_context = Query.checkContext } };
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
        var pin = try self.pinDirectory(capability);
        defer pin.release();
        const directory = pin.value;
        if (start > directory.bytes or out.len > directory.bytes - start) return error.InvalidNativeLakeTextCorpus;
        if (self.publication) return self.readPublication(directory, capability, start, out);
        const io = capability.context.io orelse return self.readSerial(directory, capability, start, out);
        if (out.len <= pack_bytes) return self.readSerial(directory, capability, start, out);
        var tasks: [3]?local.sql_parallel_scheduler.Task(anyerror!void) = @splat(null);
        defer for (&tasks) |*task| if (task.*) |*active| {
            active.await(io) catch {};
        };
        var done: usize = 0;
        while (done < out.len) {
            for (0..4) |lane| {
                if (done == out.len) break;
                const position: usize = @intCast(start + done);
                const index = containing(directory.blocks, position) orelse {
                    try self.readSerial(directory, capability, start + done, out[done..]);
                    done = out.len;
                    break;
                };
                const piece = directory.blocks[index];
                const finish: usize = if (piece.pack) |pack| piece.offset + @as(usize, @intCast(pack.byte_len - piece.pack_offset)) else piece.offset + @as(usize, @intCast(piece.ref.byte_len));
                const take = @min(out.len - done, finish - position);
                const target = out[done..][0..take];
                if (lane < tasks.len) {
                    tasks[lane] = local.sql_parallel_scheduler.global().submitIo(io, pack_bytes * 3, readSerial, .{ self, directory, capability, start + done, target });
                    if (tasks[lane] == null) try self.readSerial(directory, capability, start + done, target);
                } else try self.readSerial(directory, capability, start + done, target);
                done += take;
            }
            var failure: ?anyerror = null;
            for (&tasks) |*task| if (task.*) |*active| {
                active.await(io) catch |err| {
                    failure = failure orelse err;
                };
                task.* = null;
            };
            if (failure) |err| return err;
        }
        try capability.check();
    }
    fn readPublication(self: *@This(), directory: Directory, capability: Read, start: u64, out: []u8) !void {
        const phase_cap = capability.store.io_phase orelse return self.readSerialPhase(directory, capability, start, out, true);
        const scratch = self.a;
        const available = @import("../serverless/build/lake_build_limits.zig").WorkingSetAllocator.headroom(scratch);
        const memory_width = if (available) |bytes| @max(1, bytes / (pack_bytes * 3)) else 1;
        const width = @max(1, @min(4, phase_cap.max_parallel, memory_width));
        if (out.len <= pack_bytes) return self.readSerialPhase(directory, capability, start, out, true);
        // Reserve the whole bounded wave while parking its CPU owner. Children
        // use raw transport, so a one-slot scheduler cannot nest I/O admission.
        var worker = capability;
        worker.store.io_phase = null;
        const Worker = struct {
            fn run(owner: *Owner, dir: Directory, cap: Read, offset: u64, target: []u8, failure: *?anyerror) void {
                owner.readSerial(dir, cap, offset, target) catch |err| {
                    failure.* = err;
                };
            }
        };
        var done: usize = 0;
        while (done < out.len) {
            try capability.check();
            const count = @min(width, std.math.divCeil(usize, out.len - done, pack_bytes) catch unreachable);
            var phase = try capability.store.beginIo(count);
            defer phase.abort();
            var failures: [4]?anyerror = @splat(null);
            var group: std.Io.Group = .init;
            defer group.cancel(phase_cap.io);
            for (0..count) |lane| {
                const take = @min(pack_bytes, out.len - done);
                group.async(phase_cap.io, Worker.run, .{ self, directory, worker, start + done, out[done..][0..take], &failures[lane] });
                done += take;
            }
            group.await(phase_cap.io) catch |err| {
                group.cancel(phase_cap.io);
                return phase.failed(err);
            };
            for (failures[0..count]) |failure| if (failure) |err| return phase.failed(err);
            try phase.finish();
        }
        try capability.check();
    }
    fn metadataBytes(self: *@This(), directory: Directory, capability: Read, publication: bool) !?[]const u8 {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        try capability.check();
        if (self.metadata_bundle.len == 0) {
            const pack = directory.metadata_bundle orelse return null;
            const encoded = (if (publication) artifacts.readPublicationArtifact(self.a, capability.store, pack, capability.cancellation) else artifacts.readArtifact(self.a, capability.store, pack, capability.cancellation, capability.cache)) catch |err| switch (err) {
                // Navigation residency is optional; continue through bounded
                // verified range leases when this physical owner is full.
                error.OutOfMemory => return null,
                else => return err,
            };
            const bytes = if (directory.metadata_bundle != null) decode: {
                defer self.a.free(encoded);
                if (try local.encoding_snappy.decodedLen(encoded) != directory.metadata_decoded_bytes) return error.InvalidNativeLakeTextCorpus;
                const expanded = self.a.alloc(u8, directory.metadata_decoded_bytes) catch return null;
                errdefer self.a.free(expanded);
                try local.encoding_snappy.decodeInto(encoded, expanded);
                break :decode expanded;
            } else encoded;
            errdefer self.a.free(bytes);
            var start: usize = 0;
            for (directory.metadata) |piece| {
                _ = try ServingCache.VerifiedBytes.authenticate(bytes[start..][0..@intCast(piece.ref.byte_len)], try stores.sha256DigestFromChecksum(piece.ref.checksum));
                start += @intCast(piece.ref.byte_len);
            }
            try capability.check();
            // The publication allocator accounts this bounded immutable
            // navigation until the last physical owner releases it. Neither
            // the opening credentials nor cancellation capability survive.
            self.metadata_bundle = bytes;
            self.metadata_resident_bytes.store(bytes.len, .release);
        }
        return self.metadata_bundle;
    }
    fn readSerial(self: *@This(), directory: Directory, capability: Read, start: u64, out: []u8) anyerror!void {
        return self.readSerialPhase(directory, capability, start, out, false);
    }
    fn readSerialPhase(self: *@This(), directory: Directory, capability: Read, start: u64, out: []u8, publication: bool) anyerror!void {
        try capability.check();
        if (start > directory.bytes or out.len > directory.bytes - start) return error.InvalidNativeLakeTextCorpus;
        const transport = if (self.publication) self.a else std.heap.page_allocator;
        var offset: usize = @intCast(start);
        var done: usize = 0;
        var group: ?CoalescedRead = null;
        defer if (group) |*value| value.deinit();
        while (done < out.len) {
            try capability.check();
            const metadata = containing(directory.metadata, offset);
            const block = if (metadata == null) containing(directory.blocks, offset) else null;
            const piece = if (metadata) |index| directory.metadata[index] else if (block) |index| directory.blocks[index] else return error.InvalidNativeLakeTextCorpus;
            if (metadata != null and directory.metadata_bundle != null) if (try self.metadataBytes(directory, capability, publication)) |bytes| {
                const within = offset - piece.offset;
                const take = @min(out.len - done, @as(usize, @intCast(piece.ref.byte_len)) - within);
                try capability.check();
                var position: usize = 0;
                for (directory.metadata[0..metadata.?]) |prior| position += @intCast(prior.ref.byte_len);
                @memcpy(out[done..][0..take], bytes[position + within ..][0..take]);
                offset += take;
                done += take;
                continue;
            };
            if (group == null or offset >= group.?.logical_end) {
                if (group) |*value| value.deinit();
                group = null;
                if (block) |index| if (piece.pack) |pack| {
                    var length: usize = @intCast(piece.ref.byte_len);
                    var next = index + 1;
                    const requested_end = @as(usize, @intCast(start)) + out.len;
                    while (next < directory.blocks.len) : (next += 1) {
                        const following = directory.blocks[next];
                        if (following.offset != piece.offset + length or following.offset >= requested_end or following.pack == null or !std.mem.eql(u8, following.pack.?.artifact_id, pack.artifact_id) or following.pack_offset != piece.pack_offset + length) break;
                        length += @intCast(following.ref.byte_len);
                    }
                    group = .{ .publication = publication, .store = capability.store, .pack = pack, .start = piece.pack_offset, .length = length, .logical_end = piece.offset + length, .cancellation = capability.cancellation, .a = transport, .pieces = directory.blocks[index..next] };
                };
            }
            var lease = try readPieceLeaseCoalesced(transport, capability.store, piece, capability.cancellation, capability.cache, if (group) |*value| value else null, publication);
            defer lease.deinit();
            const data = lease.bytes();
            try capability.check();
            const within = offset - piece.offset;
            const take = @min(out.len - done, data.len - within);
            @memcpy(out[done..][0..take], data[within..][0..take]);
            if (block) |index| if (self.directory_ref == null) {
                @import("antfly_platform").sync.lockYielding(&self.mutex);
                self.loaded[index] = true;
                self.mutex.unlock();
            };
            offset += take;
            done += take;
        }
        try capability.check();
    }
};
/// Structural admission is manifest-owned; the full posting directory is
/// singleflight-loaded only when a query actually accesses this segment.
pub fn loadAdmitted(a: A, read: Read, ref: Ref, metadata: local.segment.SegmentReader.Admission) !local.index.SegmentData {
    try metadata.validate();
    if (metadata.bytes == 0 or metadata.bytes > 32 * 1024 * 1024) return error.InvalidNativeLakeTextCorpus;
    try read.check();
    const owner = try a.create(Owner);
    owner.* = .{ .a = a, .arena = .init(a), .directory = .{ .bytes = @intCast(metadata.bytes), .metadata = &.{}, .blocks = &.{}, .terms = &.{} }, .directory_bytes = @intCast(metadata.bytes), .loaded = &.{}, .fallback = read, .query_scoped = true, .resource_manager = read.resource_manager };
    errdefer Owner.release(owner);
    owner.directory_ref = .{ .artifact_id = try owner.arena.allocator().dupe(u8, ref.artifact_id), .checksum = try owner.arena.allocator().dupe(u8, ref.checksum), .byte_len = ref.byte_len };
    return .fromNative(owner.source());
}

/// Only the compact route is retained by a corpus. The selected query admits
/// its full file descriptor through the scope-fenced shared metadata cache.
pub fn loadRouted(a: A, read: Read, ref: Ref, bytes: u64, manifest: local.serverless_manifest_artifact_ref.ArtifactRef, ordinal: usize) !local.index.SegmentData {
    if (bytes == 0 or bytes > 32 * 1024 * 1024) return error.InvalidNativeLakeTextCorpus;
    try read.check();
    const owner = try a.create(Owner);
    owner.* = .{ .a = a, .arena = .init(a), .directory = .{ .bytes = @intCast(bytes), .metadata = &.{}, .blocks = &.{}, .terms = &.{} }, .directory_bytes = @intCast(bytes), .loaded = &.{}, .fallback = read, .query_scoped = true, .resource_manager = read.resource_manager, .manifest_ordinal = ordinal };
    errdefer Owner.release(owner);
    owner.directory_ref = .{ .artifact_id = try owner.arena.allocator().dupe(u8, ref.artifact_id), .checksum = try owner.arena.allocator().dupe(u8, ref.checksum), .byte_len = ref.byte_len };
    var owned = manifest;
    owned.artifact_id = try owner.arena.allocator().dupe(u8, manifest.artifact_id);
    owned.checksum = try owner.arena.allocator().dupe(u8, manifest.checksum);
    owner.manifest_ref = owned;
    return .fromNative(owner.source());
}
pub fn routedReader(a: A, source: local.index.SegmentSource) !local.segment.SegmentReader {
    if (source != .ranges) return error.InvalidNativeLakeTextCorpus;
    const query: *Owner.Query = @ptrCast(@alignCast(source.ranges.ptr));
    const owner = query.owner;
    const read = query.capability.*;
    try read.check();
    const descriptor = if (read.descriptors) |routes| routes.get(owner.directory_ref.?.artifact_id) orelse return error.InvalidNativeLakeTextCorpus else Descriptor{ .manifest = owner.manifest_ref orelse return error.InvalidNativeLakeTextCorpus, .ordinal = owner.manifest_ordinal };
    const manifest = descriptor.manifest;
    const corpus = @import("lake_index_native_text.zig");
    const lease: ?decoded.Owned(corpus.FileGroup) = if (read.cache) |cached| try decoded.acquire(corpus.FileGroup, cached, read.store, manifest, read.cancellation, corpus.loadCachedFile) else null;
    defer if (lease) |owned| owned.release();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const group = if (lease) |owned| owned.value.* else try corpus.loadCachedFile(arena.allocator(), read.store, manifest, read.cancellation, null);
    const index = descriptor.ordinal;
    if (index >= group.segments.len or group.admissions.len != group.segments.len) return error.InvalidNativeLakeTextCorpus;
    const ref = owner.directory_ref.?;
    if (!std.mem.eql(u8, group.segments[index].artifact_id, ref.artifact_id) or !std.mem.eql(u8, group.segments[index].checksum, ref.checksum) or group.segments[index].byte_len != ref.byte_len) return error.InvalidNativeLakeTextCorpus;
    return local.segment.SegmentReader.initAuthenticated(a, source, group.admissions[index]);
}
/// The caller owns this source and all of its readers until merge completion.
/// It must not publish it into a shared serving corpus.
pub fn loadPublication(a: A, read: Read, ref: Ref) !local.index.SegmentData {
    std.debug.assert(read.cache == null);
    return loadInner(a, read, ref, true);
}
pub fn load(a: A, read: Read, ref: Ref) !local.index.SegmentData {
    return loadInner(a, read, ref, false);
}
fn loadInner(a: A, read: Read, ref: Ref, publication: bool) !local.index.SegmentData {
    const owner = try a.create(Owner);
    owner.* = .{ .a = a, .arena = .init(a), .directory = undefined, .loaded = &.{}, .fallback = read, .resource_manager = read.resource_manager, .publication = publication };
    errdefer Owner.release(owner);
    owner.directory = try loadDirectoryInner(owner.arena.allocator(), read, ref, publication);
    owner.directory_bytes = owner.directory.bytes;
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
    var reference_reader = try local.segment.SegmentReader.init(a, encoded);
    defer reference_reader.deinit();
    const admission = try reference_reader.admission(a);
    defer admission.deinit(a);
    const Install = struct {
        fn run(allocator: A, writer: *local.index.IndexWriter, data_: local.index.SegmentData, metadata: local.segment.SegmentReader.Admission) !void {
            var owned = data_;
            errdefer owned.deinit(allocator);
            var replacements = [_]local.index.ReplacementSegmentData{.{ .id = 1, .data = owned, .prepared_reader = try local.segment.SegmentReader.initAuthenticated(allocator, owned.native, metadata) }};
            defer if (replacements[0].prepared_reader) |*reader| reader.deinit();
            try writer.replaceSegmentsManyData(&.{}, &replacements);
        }
    };
    const data = try loadAdmitted(a, .{ .store = store, .cache = null, .context = .{ .io = std.testing.io }, .cancellation = .none }, ref, admission);
    const owner: *Owner = @ptrCast(@alignCast(data.native.ranges.ptr));
    var lazy = try local.index.IndexWriter.init(a);
    defer lazy.deinit();
    try Install.run(a, &lazy, data, admission);
    try std.testing.expectEqual(@as(usize, 0), owner.directory.blocks.len);
    try std.testing.expectEqual(@as(usize, 0), owner.loaded.len);
    var eager = try local.index.IndexWriter.init(a);
    defer eager.deinit();
    try eager.addSegmentWithId(1, encoded);
    const absent = try lazy.snapshot().search(a, "body", &.{"absent"}, 10);
    defer a.free(absent.hits);
    try std.testing.expectEqual(@as(u64, 0), absent.total_count);
    try std.testing.expectEqual(@as(usize, 0), owner.directory.blocks.len);
    // The tiny fixture shares dictionary navigation and postings in one
    // authenticated block. Required dictionary discovery may touch that block;
    // the large-corpus tests below bound posting and position payload reads.
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
    // Admitted owners retain no touched-block array or parsed directory.
    // Exact result comparison above exercises actual required payload reads.
    try std.testing.expectEqual(@as(usize, 0), owner.loaded.len);
    // Bound readers use the current query's store capability. A failed read
    // must leave shared navigation reusable by a later authorized query.
    var retry_writer = try local.index.IndexWriter.init(a);
    defer retry_writer.deinit();
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
    // Even a completely unavailable provider permits authenticated admission.
    try Install.run(a, &retry_writer, try loadAdmitted(a, .{ .store = denied, .cache = null, .context = .{}, .cancellation = .none }, ref, admission), admission);
    var failed: Read = .{ .store = denied, .cache = null, .context = .{}, .cancellation = .none };
    const denied_snapshot = try retry_writer.acquireSnapshotWithReadContext(&failed);
    defer denied_snapshot.release();
    try std.testing.expectError(error.TestRemoteUnavailable, denied_snapshot.termDocFreq(a, "body", "alpha"));
    var denied_bytes: [4]u8 = undefined;
    try std.testing.expectError(error.TestRemoteUnavailable, (try denied_snapshot.readSegment(0)).query_source.?.readInto(0, &denied_bytes));
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
        var hydrator = @import("lake_index_source_projection.zig").Hydrator.init(a);
        defer hydrator.deinit();
        try hydrator.append(a, snapshot, result.hits[0].doc_id, stored.id, &.{ "label", "body" }, &hits[0].source_value.?);
        try std.testing.expectEqual(@as(usize, 1), hydrator.blocks.decode_count);
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
    var inv = (try (try snapshot.readSegment(0)).reader.invertedIndexScoped(a, "body")).?;
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
    source.prefetch(0, block_bytes * 4);
    const query: *Owner.Query = @ptrCast(@alignCast(source.ranges.ptr));
    // WAND can skip every hinted page. Completed tasks must not permanently
    // occupy the four slots even when no required read overlaps them.
    for (&query.pending) |*slot| if (slot.*) |*task| {
        while (!task.isComplete()) std.atomic.spinLoopHint();
    };
    source.prefetch(block_bytes * 16, block_bytes);
    query.drain(false);
    try std.testing.expectEqual(@as(u64, 2), cache.stats.provider_reads);
    var sample: [1]u8 = undefined;
    try source.readInto(block_bytes * 3, &sample);
    try std.testing.expectEqual(@as(u8, 3), sample[0]);
    try std.testing.expectEqual(@as(u64, 2), cache.stats.provider_reads);
    // A warm required unit returns while a larger overlapping speculative
    // task is gated. This catches accidental joining of the whole pack.
    const Gated = struct {
        started: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        read_done: std.Io.Event = .unset,
        fn speculate(self: *@This()) anyerror!void {
            self.started.set(std.testing.io);
            try self.release.wait(std.testing.io);
        }
        fn required(self: *@This(), input: *local.index.SegmentSource) !void {
            defer self.read_done.set(std.testing.io);
            var out: [1]u8 = undefined;
            try input.readInto(block_bytes * 3, &out);
            try std.testing.expectEqual(@as(u8, 3), out[0]);
        }
    };
    var gated: Gated = .{};
    defer gated.release.set(std.testing.io);
    query.pending[0] = local.sql_parallel_scheduler.global().submitTransient(std.testing.io, pack_bytes, Gated.speculate, .{&gated}) orelse return error.TestUnexpectedResult;
    query.offsets[0] = 0;
    query.lengths[0] = pack_bytes;
    try gated.started.wait(std.testing.io);
    var required = try std.testing.io.concurrent(Gated.required, .{ &gated, &source });
    var required_open = true;
    defer {
        gated.release.set(std.testing.io);
        if (required_open) _ = required.cancel(std.testing.io) catch {};
    }
    const ready = gated.read_done.waitTimeout(std.testing.io, .{ .duration = .{ .clock = .awake, .raw = .fromMilliseconds(250) } });
    const remained_speculative = query.pending[0] != null;
    gated.release.set(std.testing.io);
    const completed = required.await(std.testing.io);
    required_open = false;
    try completed;
    try ready;
    try std.testing.expect(remained_speculative);
    query.drain(false);
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
    for (bytes, 0..) |*byte, i| byte.* = @truncate(i / block_bytes);
    const ref = try publish(a, a, &store, bytes, .none);
    defer a.free(ref.artifact_id);
    defer a.free(ref.checksum);
    const data = try load(a, .{ .store = store, .cache = null, .context = .{}, .cancellation = .none }, ref);
    var source = data.native;
    defer source.close();
    const owner: *Owner = @ptrCast(@alignCast(source.ranges.ptr));
    try std.testing.expectEqual(@as(u16, 4), owner.directory.version);
    try std.testing.expectEqual(@as(usize, 16), owner.directory.blocks.len);
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
    // Warm grouped reads must not issue a pack GET, and denied admission
    // must still serve through the same bounded coalesced provider read.
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    const cached_read: Read = .{ .store = observed, .cache = .{ .cache = &cache, .scope = @splat(2), .context = .{ .io = std.testing.io } }, .context = .{}, .cancellation = .none };
    Observed.reads = 0;
    try owner.read(cached_read, 0, output);
    try std.testing.expectEqual(@as(usize, 1), Observed.reads);
    try std.testing.expectEqual(@as(u64, 1), cache.stats.provider_reads);
    try std.testing.expectEqual(@as(u64, pack_bytes), cache.stats.provider_bytes);
    try owner.read(cached_read, 0, output);
    try std.testing.expectEqual(@as(usize, 1), Observed.reads);
    // Fifteen resident units must not be downloaded to fill one missing unit.
    for (0..2) |mode| {
        var partial = cached_read;
        partial.cache.?.scope = @splat(@as(u8, @intCast(4 + mode)));
        if (mode == 1) partial.cache.?.context.io = null;
        for (0..15) |i| try owner.read(partial, i * block_bytes, &sample);
        Observed.reads = 0;
        Observed.transferred = 0;
        try owner.read(partial, 0, output);
        try std.testing.expectEqual(@as(usize, 1), Observed.reads);
        try std.testing.expectEqual(@as(usize, block_bytes), Observed.transferred);
        try std.testing.expectEqualSlices(u8, bytes, output);
    }
    cache.max_entries = 0;
    var denied = cached_read;
    denied.cache.?.scope = @splat(3);
    Observed.reads = 0;
    try owner.read(denied, 0, output);
    try std.testing.expectEqual(@as(usize, 1), Observed.reads);
    try std.testing.expectEqualSlices(u8, bytes, output);
    Observed.corrupt = true;
    defer Observed.corrupt = false;
    try std.testing.expectError(error.ArtifactIntegrityMismatch, owner.read(.{ .store = observed, .cache = null, .context = .{}, .cancellation = .none }, 0, &sample));
    Observed.corrupt = false;
    var io_impl = std.Io.Threaded.init(a, .{});
    defer io_impl.deinit();
    const cache_root = try std.fs.path.join(a, &.{ directory.path(), "range-cache" });
    defer a.free(cache_root);
    {
        var restart_cache = local.serverless_query_lake_serving_cache.Cache.init(a);
        defer restart_cache.deinit();
        try restart_cache.ensurePersistent(io_impl.io(), cache_root, .{}, .{});
        const cached: artifacts.CachedRead = .{ .cache = &restart_cache, .scope = @splat(4), .context = .{ .io = io_impl.io() } };
        Observed.reads = 0;
        try owner.read(.{ .store = observed, .cache = cached, .context = cached.context, .cancellation = .none }, 0, output);
        try std.testing.expectEqual(@as(usize, 1), Observed.reads);
        try owner.read(.{ .store = observed, .cache = cached, .context = cached.context, .cancellation = .none }, block_bytes * 2 + 17, &sample);
        try std.testing.expectEqual(@as(usize, 1), Observed.reads);
    }
    {
        var restart_cache = local.serverless_query_lake_serving_cache.Cache.init(a);
        defer restart_cache.deinit();
        try restart_cache.ensurePersistent(io_impl.io(), cache_root, .{}, .{});
        const cached: artifacts.CachedRead = .{ .cache = &restart_cache, .scope = @splat(4), .context = .{ .io = io_impl.io() } };
        Observed.reads = 0;
        Observed.transferred = 0;
        try owner.read(.{ .store = observed, .cache = cached, .context = cached.context, .cancellation = .none }, block_bytes * 2 + 17, &sample);
        try std.testing.expectEqual(bytes[block_bytes * 2 + 17], sample[0]);
        try std.testing.expectEqual(@as(usize, 0), Observed.reads);
        try std.testing.expectEqual(@as(usize, 0), Observed.transferred);
        try std.testing.expect(restart_cache.snapshot().disk_hits > 0);
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

test "external lake physical pack flights share different unit leaders without residency and isolate cancellation" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var directory = try local.common_test_directory.TestDirectory.init("packed-flight-sharing");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(9), 1, io);
    const bytes = try a.alloc(u8, pack_bytes);
    defer a.free(bytes);
    for (bytes, 0..) |*byte, i| byte.* = @truncate(i / block_bytes);
    const ref = try publish(a, a, &store, bytes, .none);
    defer a.free(ref.artifact_id);
    defer a.free(ref.checksum);
    const data = try load(a, .{ .store = store, .cache = null, .context = .{}, .cancellation = .none }, ref);
    var source = data.native;
    defer source.close();
    const owner: *Owner = @ptrCast(@alignCast(source.ranges.ptr));
    const Provider = struct {
        base: stores.ArtifactStore,
        started: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        calls: std.atomic.Value(usize) = .init(0),
        corrupt_first: bool = false,
        fn range(raw: *anyopaque, alloc: A, id: []const u8, offset: u64, len: usize, cancellation: Cancellation) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.calls.fetchAdd(1, .monotonic);
            self.started.set(std.testing.io);
            try self.release.wait(std.testing.io);
            const result = try self.base.getRangeAllocWithCancellationUsingAllocator(alloc, id, offset, len, cancellation);
            if (self.corrupt_first and offset == 0 and result.len != 0) result[0] ^= 1;
            return result;
        }
    };
    var provider: Provider = .{ .base = store };
    var vtable = store.vtable.*;
    vtable.get_range_alloc_with_cancellation = Provider.range;
    var observed = store;
    observed.ptr = &provider;
    observed.vtable = &vtable;
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    cache.max_entries = 0;
    var read: Read = .{ .store = observed, .cache = .{ .cache = &cache, .scope = @splat(7), .context = .{ .io = io } }, .context = .{ .io = io }, .cancellation = .none };
    const output = try a.alloc(u8, bytes.len);
    defer a.free(output);
    const other = try a.alloc(u8, bytes.len - block_bytes);
    defer a.free(other);
    var leader = try io.concurrent(Owner.read, .{ owner, read, @as(u64, 0), output });
    var leader_open = true;
    defer if (leader_open) {
        provider.release.set(io);
        leader.cancel(io) catch {};
    };
    try provider.started.waitTimeout(io, .{ .duration = .{ .clock = .awake, .raw = .fromSeconds(5) } });
    const Cancel = struct {
        flag: std.atomic.Value(bool) = .init(false),
        fn check(raw: *const anyopaque) bool {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            return self.flag.load(.acquire);
        }
    };
    var cancel: Cancel = .{};
    read.cache.?.context.cancellation = .fromCallback(&cancel, Cancel.check);
    var waiter = try io.concurrent(Owner.read, .{ owner, read, @as(u64, block_bytes), other });
    var waiter_open = true;
    defer if (waiter_open) {
        provider.release.set(io);
        waiter.cancel(io) catch {};
    };
    const Wait = struct {
        fn shared(cache_: *local.serverless_query_lake_serving_cache.Cache) !void {
            for (0..500) |_| {
                @import("antfly_platform").sync.lockYielding(&cache_.physical.mutex);
                var values = cache_.physical.flights.valueIterator();
                const joined = if (values.next()) |flight| flight.*.refs > 1 else false;
                cache_.physical.mutex.unlock();
                if (joined) return;
                try std.testing.io.sleep(.fromMilliseconds(2), .awake);
            }
            return error.TestPhysicalFlightDidNotJoin;
        }
    };
    try Wait.shared(&cache);
    cancel.flag.store(true, .release);
    const canceled_result = waiter.await(io);
    waiter_open = false;
    try std.testing.expectError(error.Canceled, canceled_result);
    read.cache.?.context.cancellation = null;
    var live = try io.concurrent(Owner.read, .{ owner, read, @as(u64, block_bytes), other });
    var live_open = true;
    defer if (live_open) {
        provider.release.set(io);
        live.cancel(io) catch {};
    };
    try Wait.shared(&cache);
    provider.release.set(io);
    const leader_result = leader.await(io);
    leader_open = false;
    try leader_result;
    const live_result = live.await(io);
    live_open = false;
    try live_result;
    try std.testing.expectEqualSlices(u8, bytes, output);
    try std.testing.expectEqualSlices(u8, bytes[block_bytes..], other);
    try std.testing.expectEqual(@as(usize, 1), provider.calls.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), cache.snapshot().provider_reads);
    try std.testing.expectEqual(@as(usize, 0), cache.physical.bytes);
    try std.testing.expectEqual(@as(usize, 0), cache.physical.flights.count());
    const Expired = struct {
        flag: std.atomic.Value(bool) = .init(false),
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.flag.load(.acquire)) return error.TestReaderLeaseExpired;
        }
    };
    var expired: Expired = .{};
    provider.started = .unset;
    provider.release = .unset;
    read.cache.?.context.checkpoint = .{ .ptr = &expired, .check = Expired.check };
    leader = try io.concurrent(Owner.read, .{ owner, read, @as(u64, 0), output });
    leader_open = true;
    try provider.started.waitTimeout(io, .{ .duration = .{ .clock = .awake, .raw = .fromSeconds(5) } });
    read.cache.?.context.checkpoint = null;
    live = try io.concurrent(Owner.read, .{ owner, read, @as(u64, block_bytes), other });
    live_open = true;
    try Wait.shared(&cache);
    expired.flag.store(true, .release);
    provider.release.set(io);
    const expired_result = leader.await(io);
    leader_open = false;
    try std.testing.expectError(error.TestReaderLeaseExpired, expired_result);
    const retried_result = live.await(io);
    live_open = false;
    try retried_result;
    try std.testing.expectEqualSlices(u8, bytes[block_bytes..], other);
    try std.testing.expectEqual(@as(usize, 3), provider.calls.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), cache.physical.flights.count());
    // A broad leader records corrupt unit zero without poisoning a waiter
    // that needs only the independently authenticated healthy unit one.
    provider.started = .unset;
    provider.release = .unset;
    provider.corrupt_first = true;
    leader = try io.concurrent(Owner.read, .{ owner, read, @as(u64, 0), output[0 .. block_bytes * 2] });
    leader_open = true;
    try provider.started.wait(io);
    live = try io.concurrent(Owner.read, .{ owner, read, @as(u64, block_bytes), other[0..1] });
    live_open = true;
    try Wait.shared(&cache);
    provider.release.set(io);
    const corrupt_result = leader.await(io);
    leader_open = false;
    try std.testing.expectError(error.ArtifactIntegrityMismatch, corrupt_result);
    const healthy_result = live.await(io);
    live_open = false;
    try healthy_result;
    try std.testing.expectEqual(@as(u8, 1), other[0]);
    try std.testing.expectEqual(@as(usize, 4), provider.calls.load(.acquire));
    // Disjoint requests in one pack must enter the provider concurrently.
    provider.started = .unset;
    provider.release = .unset;
    provider.corrupt_first = false;
    leader = try io.concurrent(Owner.read, .{ owner, read, @as(u64, 0), output[0..1] });
    leader_open = true;
    try provider.started.wait(io);
    live = try io.concurrent(Owner.read, .{ owner, read, @as(u64, block_bytes), other[0..1] });
    live_open = true;
    var overlapped = false;
    for (0..500) |_| {
        if (provider.calls.load(.acquire) == 6) {
            overlapped = true;
            break;
        }
        try io.sleep(.fromMilliseconds(2), .awake);
    }
    provider.release.set(io);
    const first_disjoint = leader.await(io);
    leader_open = false;
    try first_disjoint;
    const second_disjoint = live.await(io);
    live_open = false;
    try second_disjoint;
    try std.testing.expect(overlapped);
    try std.testing.expectEqual(@as(usize, 0), cache.physical.flights.count());
    // Partially overlapping requests compose completed verified slices. The
    // second reader downloads only its unclaimed tail, even with no residency.
    provider.started = .unset;
    provider.release = .unset;
    const before_bytes = cache.snapshot().provider_bytes;
    leader = try io.concurrent(Owner.read, .{ owner, read, @as(u64, 0), output[0 .. block_bytes * 2] });
    leader_open = true;
    try provider.started.wait(io);
    live = try io.concurrent(Owner.read, .{ owner, read, @as(u64, block_bytes), other[0 .. block_bytes * 2] });
    live_open = true;
    try Wait.shared(&cache);
    provider.release.set(io);
    const overlap_leader = leader.await(io);
    leader_open = false;
    try overlap_leader;
    const overlap_waiter = live.await(io);
    live_open = false;
    try overlap_waiter;
    try std.testing.expectEqualSlices(u8, bytes[0 .. block_bytes * 2], output[0 .. block_bytes * 2]);
    try std.testing.expectEqualSlices(u8, bytes[block_bytes .. block_bytes * 3], other[0 .. block_bytes * 2]);
    try std.testing.expectEqual(@as(u64, block_bytes * 3), cache.snapshot().provider_bytes - before_bytes);
    try std.testing.expectEqual(@as(usize, 8), provider.calls.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), cache.physical.flights.count());
}

test "external lake required pack reads overlap and return every scheduler reservation" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var directory = try local.common_test_directory.TestDirectory.init("packed-required-parallel");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(10), 1, io);
    const bytes = try a.alloc(u8, pack_bytes * 4);
    defer a.free(bytes);
    for (bytes, 0..) |*byte, i| byte.* = @truncate(i / block_bytes);
    const ref = try publish(a, a, &store, bytes, .none);
    defer a.free(ref.artifact_id);
    defer a.free(ref.checksum);
    const data = try load(a, .{ .store = store, .cache = null, .context = .{}, .cancellation = .none }, ref);
    var source = data.native;
    defer source.close();
    const owner: *Owner = @ptrCast(@alignCast(source.ranges.ptr));
    const Provider = struct {
        base: stores.ArtifactStore,
        active: std.atomic.Value(usize) = .init(0),
        peak: std.atomic.Value(usize) = .init(0),
        fail: bool = false,
        execution: ?*local.sql_parallel_scheduler.Execution = null,
        fn range(raw: *anyopaque, alloc: A, id: []const u8, offset: u64, len: usize, cancellation: Cancellation) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.execution) |execution| {
                try std.testing.expect(!execution.active);
                var other: local.sql_parallel_scheduler.Execution = .{ .scheduler = execution.scheduler, .bytes = 1 };
                try other.enter(std.testing.io);
                other.pause();
            }
            const active = self.active.fetchAdd(1, .acq_rel) + 1;
            defer _ = self.active.fetchSub(1, .acq_rel);
            _ = self.peak.fetchMax(active, .monotonic);
            try std.testing.io.sleep(.fromMilliseconds(15), .awake);
            if (self.fail) return error.TestRemoteUnavailable;
            return self.base.getRangeAllocWithCancellationUsingAllocator(alloc, id, offset, len, cancellation);
        }
    };
    var provider: Provider = .{ .base = store };
    var vtable = store.vtable.*;
    vtable.get_range_alloc_with_cancellation = Provider.range;
    var observed = store;
    observed.ptr = &provider;
    observed.vtable = &vtable;
    const output = try a.alloc(u8, bytes.len);
    defer a.free(output);
    const read: Read = .{ .store = observed, .cache = null, .context = .{ .io = io }, .cancellation = .none };
    try owner.read(read, 0, output);
    try std.testing.expectEqualSlices(u8, bytes, output);
    try std.testing.expect(provider.peak.load(.acquire) > 1);
    try std.testing.expect(provider.peak.load(.acquire) <= 4);
    provider.fail = true;
    try std.testing.expectError(error.TestRemoteUnavailable, owner.read(read, 0, output));
    try std.testing.expectEqual(@as(usize, 0), provider.active.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), local.sql_parallel_scheduler.global().workers);
    try std.testing.expectEqual(@as(usize, 0), local.sql_parallel_scheduler.global().bytes);
    const Phase = struct {
        execution: local.sql_parallel_scheduler.Execution,
        pending: ?local.sql_parallel_scheduler.Execution.Io = null,
        fn begin(raw: *anyopaque, count: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.pending = try self.execution.beginIo(std.testing.io, count);
        }
        fn finish(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var pending = self.pending.?;
            self.pending = null;
            try pending.finish();
        }
        fn abort(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.pending) |*pending| pending.abort();
            self.pending = null;
        }
    };
    // Both bounded parallel waves and the single-lane fallback park the CPU
    // owner for every provider read, then resume it before decoding continues.
    for ([_]usize{ 1, 3, 3 }, 0..) |width, case| {
        var build = try @import("../serverless/build/lake_build_limits.zig").WorkingSetAllocator.init(a, .{ .max_working_set_bytes = pack_bytes * 12 });
        var memory: local.sql_memory_budget = .{ .backing = build.allocator(), .limit = pack_bytes + pack_bytes * 3 * (if (case == 2) @as(usize, 1) else width) };
        const expected_width: usize = if (case == 2) 1 else width;
        var scheduler: local.sql_parallel_scheduler.Scheduler = .{ .max_workers = 1, .max_io_workers = width };
        var phase: Phase = .{ .execution = .{ .scheduler = &scheduler, .bytes = 1 } };
        try phase.execution.enter(io);
        defer phase.execution.pause();
        const capability: stores.IoPhase = .{ .io = io, .ptr = &phase, .max_parallel = width, .begin = Phase.begin, .finish = Phase.finish, .abort = Phase.abort };
        var publication = read;
        publication.store.io_phase = &capability;
        provider.execution = &phase.execution;
        defer provider.execution = null;
        provider.fail = false;
        provider.peak.store(0, .release);
        {
            var publication_data = try loadPublication(memory.allocator(), .{ .store = store, .cache = null, .context = .{ .io = io }, .cancellation = .none }, ref);
            defer publication_data.deinit(memory.allocator());
            const publication_owner: *Owner = @ptrCast(@alignCast(publication_data.native.ranges.ptr));
            const retained = memory.live;
            try publication_owner.read(publication, 0, output);
            try std.testing.expectEqualSlices(u8, bytes, output);
            try std.testing.expect(phase.execution.active);
            try std.testing.expectEqual(@as(usize, 0), scheduler.io_workers);
            if (expected_width > 1) try std.testing.expect(provider.peak.load(.acquire) > 1);
            try std.testing.expect(provider.peak.load(.acquire) <= expected_width);
            try std.testing.expect(memory.peak > retained and memory.peak <= memory.limit);
            try std.testing.expectEqual(retained, memory.live);

            // Small reads and broad backend failures restore the same admission.
            try publication_owner.read(publication, 17, output[0..97]);
            try std.testing.expectEqualSlices(u8, bytes[17..][0..97], output[0..97]);
            provider.fail = true;
            try std.testing.expectError(error.TestRemoteUnavailable, publication_owner.read(publication, 0, output));
            try std.testing.expect(phase.execution.active);
            try std.testing.expectEqual(@as(usize, 0), provider.active.load(.acquire));
            try std.testing.expectEqual(@as(usize, 0), scheduler.io_workers);
        }
        try std.testing.expectEqual(@as(usize, 0), memory.live);
        try std.testing.expectEqual(@as(usize, 0), build.live_bytes);
    }
}

test "external lake admitted directories evict independently of the corpus heap and pin live readers" {
    const a = std.testing.allocator;
    const encoded = (try local.storage_db_document_mapper.buildTextSegmentFromDocuments(a, &.{.{ .key = "one", .value = "{\"body\":\"alpha beta\"}" }}, .{}, null)).?;
    defer a.free(encoded);
    var reference = try local.segment.SegmentReader.init(a, encoded);
    defer reference.deinit();
    const admission = try reference.admission(a);
    defer admission.deinit(a);
    var directory = try local.common_test_directory.TestDirectory.init("evict-native-directories");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    cache.decoded.max_entries = 1;
    const descriptor_count = 12;
    // The corpus pays for fixed reader capabilities and small owned reference
    // strings. Decoded directories must remain outside this per-owner bound.
    var memory: local.sql_memory_budget = .{ .backing = a, .limit = descriptor_count * (@sizeOf(Owner) + 1024) };
    const capability: Read = .{ .store = store, .cache = .{ .cache = &cache, .scope = @splat(3), .context = .{ .io = std.testing.io } }, .context = .{ .io = std.testing.io }, .cancellation = .none };
    var first: ?Owner.DirectoryPin = null;
    defer if (first) |*pin| pin.release();
    var sources: [descriptor_count]?local.index.SegmentSource = @splat(null);
    defer for (&sources) |*source| if (source.*) |*owned| owned.close();
    for (&sources, 0..) |*source, i| {
        store.upload_scope = try stores.UploadScope.forPublication(@splat(@intCast(i + 1)), 1, std.testing.io);
        const ref = try publish(a, a, &store, encoded, .none);
        defer a.free(ref.artifact_id);
        defer a.free(ref.checksum);
        const data = try loadAdmitted(memory.allocator(), capability, ref, admission);
        source.* = data.native;
        const owner: *Owner = @ptrCast(@alignCast(source.*.?.ranges.ptr));
        var pin = try owner.pinDirectory(capability);
        defer pin.release();
        try std.testing.expectEqual(encoded.len, pin.value.bytes);
        try std.testing.expectEqual(@as(usize, 0), owner.directory.blocks.len);
        if (i == 0) {
            first = .{ .value = pin.value, .shared = .{ .lease = pin.shared.?.lease.retain(), .value = pin.shared.?.value } };
            var read = capability;
            var query_source = try Owner.bind(owner, a, &read);
            defer query_source.close();
            query_source.prefetch(0, encoded.len);
            const query: *Owner.Query = @ptrCast(@alignCast(query_source.ranges.ptr));
            var worker_pins: usize = 0;
            for (query.pending) |worker| worker_pins += @intFromBool(worker != null);
            try std.testing.expect(worker_pins != 0);
            // Completion must release navigation even before the query joins
            // or visits this segment again. The tiny descriptor remains until
            // drain so canceled-before-start workers have an owner too.
            for (&query.pending) |*slot| if (slot.*) |*task| {
                var attempts: usize = 0;
                while (!task.isComplete() and attempts < 5000) : (attempts += 1) try std.testing.io.sleep(.fromMilliseconds(1), .awake);
                try std.testing.expect(task.isComplete());
            };
            for (query.directory_pins) |worker| if (worker) |retained| try std.testing.expect(retained.released.load(.acquire));
            query.drain(false);
            for (query.pending) |worker| try std.testing.expect(worker == null);
        }
        if (i == 1) {
            // The first directory stays readable while a second gets an
            // uncached lease under pinned pressure. Then it becomes evictable.
            try std.testing.expectEqual(encoded.len, first.?.value.bytes);
            first.?.release();
            first = null;
        }
        try std.testing.expect(cache.decoded.entries.count() <= 1);
        var expired = capability;
        expired.context.deadline_ns = 0;
        try std.testing.expectError(error.DeadlineExceeded, owner.pinDirectory(expired));
    }
    for (&sources) |*source| {
        source.*.?.close();
        source.* = null;
    }
    try std.testing.expectEqual(@as(usize, 0), memory.live);
    try std.testing.expect(memory.peak < memory.limit);
}

test "external lake seekable metadata opens all field navigation with two authenticated reads" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const documents = try ca.alloc(local.storage_db_document_mapper.MapperDoc, 4000);
    for (documents, 0..) |*document, i| {
        document.* = .{ .key = try std.fmt.allocPrint(ca, "document-{d}", .{i}), .value = try std.fmt.allocPrint(ca, "{{\"body\":\"alpha term{d} shared needle {s}\"}}", .{ i, "payload payload payload payload payload payload payload payload payload payload payload payload payload payload payload payload payload payload payload payload payload payload payload payload payload payload payload payload payload payload payload payload payload payload payload payload payload payload payload payload " }) };
    }
    const bytes = (try local.storage_db_document_mapper.buildTextSegmentFromDocuments(a, documents, .{}, null)).?;
    defer a.free(bytes);
    var directory = try local.common_test_directory.TestDirectory.init("seekable-metadata-bundle");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(19), 1, std.testing.io);
    const ref = try publish(ca, ca, &store, bytes, .none);
    var cache = ServingCache.init(a);
    defer cache.deinit();
    const cached: artifacts.CachedRead = .{ .cache = &cache, .scope = @splat(2), .context = .{ .io = std.testing.io } };
    const data = try load(a, .{ .store = store, .cache = cached, .context = .{}, .cancellation = .none }, ref);
    const owner: *Owner = @ptrCast(@alignCast(data.native.ranges.ptr));
    try std.testing.expect(owner.directory.metadata.len > 1);
    var writer = try local.index.IndexWriter.init(a);
    defer writer.deinit();
    try writer.addSegmentWithIdData(1, data);
    const opened = cache.snapshot();
    try std.testing.expectEqual(@as(u64, 2), opened.provider_reads);
    try std.testing.expectEqual(ref.byte_len + owner.directory.metadata_bundle.?.byte_len, opened.provider_bytes);
    try std.testing.expectEqual(owner.metadata_bundle.len, data.native.retainedBytes());
    try std.testing.expectEqual(@as(u16, 5), owner.directory.version);
    try std.testing.expect(owner.directory.metadata_bundle.?.byte_len < owner.metadata_bundle.len);
    for (owner.loaded) |loaded| try std.testing.expect(!loaded);
    var eager = try local.index.IndexWriter.init(a);
    defer eager.deinit();
    try eager.addSegmentWithId(1, bytes);
    const expected = try eager.snapshot().search(a, "body", &.{"needle"}, 10);
    defer a.free(expected.hits);
    const actual = try writer.snapshot().search(a, "body", &.{"needle"}, 10);
    defer a.free(actual.hits);
    try std.testing.expectEqual(expected.total_count, actual.total_count);
    for (expected.hits, actual.hits) |lhs, rhs| {
        try std.testing.expectEqual(lhs.doc_id, rhs.doc_id);
        try std.testing.expectEqual(lhs.score, rhs.score);
    }
    // Native byte offsets, including metadata/payload boundaries, remain exact.
    const roundtrip = try a.alloc(u8, bytes.len);
    defer a.free(roundtrip);
    try data.native.readInto(0, roundtrip);
    try std.testing.expectEqualSlices(u8, bytes, roundtrip);
    var expired_cache = cached;
    expired_cache.context.deadline_ns = 1;
    var warm_byte: [1]u8 = undefined;
    try std.testing.expectError(error.DeadlineExceeded, owner.read(.{ .store = store, .cache = expired_cache, .context = .{}, .cancellation = .none }, 0, &warm_byte));
    // A valid bundle cannot authorize a substituted logical range identity.
    const wrong = try ca.dupe(Piece, owner.directory.metadata);
    const checksum = "1111111111111111111111111111111111111111111111111111111111111111";
    const scoped_id = try store.upload_scope.?.artifactId(checksum);
    wrong[0].ref = .{ .artifact_id = &scoped_id, .checksum = checksum, .byte_len = wrong[0].ref.byte_len };
    var altered = owner.directory;
    altered.metadata = wrong;
    const altered_ref = try upload(ca, &store, try std.json.Stringify.valueAlloc(ca, altered, .{}), .none);
    var bad = try load(a, .{ .store = store, .cache = null, .context = .{}, .cancellation = .none }, altered_ref);
    defer bad.deinit(a);
    try std.testing.expectError(error.ArtifactIntegrityMismatch, bad.initReader(a));

    var pressured = try load(a, .{ .store = store, .cache = cached, .context = .{}, .cancellation = .none }, ref);
    defer pressured.deinit(a);
    const pressured_owner: *Owner = @ptrCast(@alignCast(pressured.native.ranges.ptr));
    var denied_alloc = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    pressured_owner.a = denied_alloc.allocator();
    var header: [21]u8 = undefined;
    try pressured.native.readInto(0, &header);
    try std.testing.expectEqualSlices(u8, bytes[0..header.len], &header);
    try std.testing.expectEqual(@as(usize, 0), pressured.native.retainedBytes());

    // A single cohort fills both original authenticated identities. Opening
    // uses the normal loader and performs no additional provider requests.
    const opening = @import("lake_index_text_opening_pack.zig");
    var collector: opening.Collector = .{ .a = a, .out = ca, .store = &store, .cancellation = .none };
    defer collector.deinit();
    const packed_ref = try publishWithOpeningCollector(ca, ca, &store, bytes, .none, &collector);
    try collector.flush();
    try std.testing.expectEqual(@as(usize, 1), collector.refs.items.len);
    var packed_cache = ServingCache.init(a);
    defer packed_cache.deinit();
    const packed_read: artifacts.CachedRead = .{ .cache = &packed_cache, .scope = @splat(3), .context = .{ .io = std.testing.io } };
    try opening.warm(a, store, collector.refs.items[0], store.upload_scope.?.domain, packed_read, .none);
    var packed_data = try load(a, .{ .store = store, .cache = packed_read, .context = .{}, .cancellation = .none }, packed_ref);
    defer packed_data.deinit(a);
    var packed_reader = try packed_data.initReader(a);
    defer packed_reader.deinit();
    try std.testing.expectEqual(@as(u64, 1), packed_cache.snapshot().provider_reads);
    try std.testing.expect(packed_cache.snapshot().provider_bytes < opened.provider_bytes);
    var expired = packed_read;
    expired.context.deadline_ns = 1;
    try std.testing.expectError(error.DeadlineExceeded, opening.warm(a, store, collector.refs.items[0], store.upload_scope.?.domain, expired, .none));
}

test "external lake seekable source publication bounds capture and preserves byte backed identity" {
    const Source = struct {
        bytes: []const u8,
        largest: usize = 0,
        fn read(raw: *anyopaque, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.largest = @max(self.largest, out.len);
            @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
        }
        fn close(_: *anyopaque) void {
            @panic("publication closed its borrowed source");
        }
        fn begin(_: *anyopaque, _: usize) !void {}
        fn finish(_: *anyopaque) !void {}
        fn abort(_: *anyopaque) void {}
    };
    const a = std.testing.allocator;
    var temporary = try local.common_test_directory.TestDirectory.init("seekable-source-publication");
    defer temporary.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, temporary.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(7), .attempt = @splat(1) };
    const bytes = try a.alloc(u8, 4 * pack_bytes + 17);
    defer a.free(bytes);
    @memset(bytes, 7);
    var source: Source = .{ .bytes = bytes };
    const phase: stores.IoPhase = .{ .io = std.testing.io, .ptr = &source, .max_parallel = 4, .begin = Source.begin, .finish = Source.finish, .abort = Source.abort };
    store.io_phase = &phase;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const expected = try publish(a, arena.allocator(), &store, bytes, .none);
    var memory: local.sql_memory_budget = .{ .backing = a, .limit = 3 * pack_bytes };
    const actual = try publishSource(memory.allocator(), arena.allocator(), &store, .{ .ranges = .{ .ptr = &source, .length = bytes.len, .read_into = Source.read, .close = Source.close } }, .none);
    try std.testing.expectEqualStrings(expected.artifact_id, actual.artifact_id);
    try std.testing.expect(source.largest <= pack_bytes);
    try std.testing.expectEqual(@as(usize, 0), memory.live);
    try std.testing.expect(!memory.exhausted);
    var fixed_bytes: [4096]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&fixed_bytes);
    var unknown: local.sql_memory_budget = .{ .backing = fixed.allocator(), .limit = 64 * pack_bytes };
    try std.testing.expectEqual(@as(usize, 1), uploadWidth(unknown.allocator(), &store));
}

test "external lake seekable source publication pipelines local reads and drains admission" {
    const a = std.testing.allocator;
    const Check = struct {
        fn call(_: *anyopaque) !void {}
    };
    var check: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &check, .checkpoint = Check.call, .async_writes = false };
    defer manager.deinit();
    var directory = try local.common_test_directory.TestDirectory.init("seekable-pack-read-ahead");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(7), .attempt = @splat(1) };
    const bytes = try a.alloc(u8, 7 * pack_bytes + 17);
    defer a.free(bytes);
    for (bytes, 0..) |*byte, i| byte.* = @truncate(i);
    const staged = try @import("lake_index_segment_spill.zig").LocalSegment.create(a, &manager, &store, bytes);
    defer staged.release();
    var output = std.heap.ArenaAllocator.init(a);
    defer output.deinit();
    const expected = try publish(a, output.allocator(), &store, bytes, .none);
    const old = try publish(a, output.allocator(), &store, &.{0}, .none);
    for ([_]bool{ false, true }) |reusing| for ([_]usize{ 1, 3 }) |lanes| {
        var scheduler: local.sql_parallel_scheduler.Scheduler = .{ .max_workers = 1, .max_io_workers = lanes, .max_bytes = 128 * 1024 * 1024 };
        var execution: local.sql_parallel_scheduler.Execution = .{ .scheduler = &scheduler, .bytes = 1024 };
        try execution.enter(std.testing.io);
        defer execution.pause();
        var phase: stores.ExecutionIo = .{ .execution = &execution, .io = std.testing.io };
        store.io_phase = phase.capability();
        defer store.io_phase = null;
        staged.test_ahead_reads.store(0, .monotonic);
        const reuse = if (reusing) (try Reuse.create(a, store, &.{old}, .none)).? else null;
        defer if (reuse) |prior| prior.deinit();
        const actual = try publishSourceReusing(a, output.allocator(), &store, staged.source(), .none, null, reuse);
        try std.testing.expectEqualStrings(expected.artifact_id, actual.artifact_id);
        try std.testing.expectEqual(lanes > 1, staged.test_ahead_reads.load(.monotonic) > 0);
        try std.testing.expectEqual(@as(usize, 0), scheduler.pull_bytes);
        try std.testing.expectEqual(@as(usize, 0), scheduler.io_workers);
        try std.testing.expect(execution.active);
    };
}

test "external lake source pack upload failure and cancellation drain pending local read ahead" {
    const a = std.testing.allocator;
    const Check = struct {
        fn call(_: *anyopaque) !void {}
    };
    const Fail = struct {
        fn put(_: *anyopaque, _: A, _: stores.UploadScope, _: []const u8, _: Cancellation) !stores.ArtifactMetadata {
            return error.TestUploadFailed;
        }
    };
    var marker: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = Check.call, .async_writes = false };
    defer manager.deinit();
    var directory = try local.common_test_directory.TestDirectory.init("seekable-read-ahead-failure");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(7), .attempt = @splat(1) };
    const bytes = try a.alloc(u8, 7 * pack_bytes + 17);
    defer a.free(bytes);
    for (bytes, 0..) |*byte, i| byte.* = @truncate(i);
    const staged = try @import("lake_index_segment_spill.zig").LocalSegment.create(a, &manager, &store, bytes);
    defer staged.release();
    var scheduler: local.sql_parallel_scheduler.Scheduler = .{ .max_workers = 1, .max_io_workers = 3, .max_bytes = 128 * 1024 * 1024 };
    var execution: local.sql_parallel_scheduler.Execution = .{ .scheduler = &scheduler, .bytes = 1024 };
    try execution.enter(std.testing.io);
    defer execution.pause();
    var phase: stores.ExecutionIo = .{ .execution = &execution, .io = std.testing.io };
    store.io_phase = phase.capability();
    defer store.io_phase = null;
    const prior = try publish(a, a, &store, &.{0}, .none);
    defer a.free(prior.artifact_id);
    defer a.free(prior.checksum);
    const reuse = (try Reuse.create(a, store, &.{prior}, .none)).?;
    defer reuse.deinit();
    var vtable = store.vtable.*;
    vtable.put_scoped = Fail.put;
    store.vtable = &vtable;
    try std.testing.expectError(error.TestUploadFailed, publishSource(a, a, &store, staged.source(), .none));
    try std.testing.expectError(error.TestUploadFailed, publishSourceReusing(a, a, &store, staged.source(), .none, null, reuse));
    try std.testing.expectEqual(@as(usize, 0), scheduler.pull_bytes);
    try std.testing.expectEqual(@as(usize, 0), scheduler.io_workers);
    try std.testing.expect(execution.active);
    // Cancel a separately admitted next wave before adoption. Cancel/drain must
    // finish before any leased source file or retained buffer is released.
    var signal = std.atomic.Value(bool).init(false);
    const token = Cancellation.fromAtomic(&signal);
    const pending = PackReadAhead.start(a, &store, staged.source(), 0, 2, token) orelse return error.TestExpectedReadAhead;
    signal.store(true, .release);
    try std.testing.expectError(error.Canceled, pending.join(&store));
    pending.deinit(std.testing.io);
    try std.testing.expectEqual(@as(usize, 0), scheduler.pull_bytes);
    try std.testing.expectEqual(@as(usize, 0), scheduler.io_workers);
    try std.testing.expect(execution.active);
}

test "external lake content block reuse survives shifted offsets restart and integrity checks" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("posting-block-reuse");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(8), 1, std.testing.io);
    const input = try a.alloc(u8, 4 * pack_bytes);
    defer a.free(input);
    var random: u64 = 0x123456789abcdef;
    for (input) |*byte| {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        byte.* = @truncate(random);
    }
    const first = try publish(a, out, &store, input, .none);
    const changed = try a.alloc(u8, input.len + 17);
    defer a.free(changed);
    @memcpy(changed[0..100], input[0..100]);
    @memset(changed[100..117], 'x');
    @memcpy(changed[117..], input[100..]);
    store.upload_scope = try stores.UploadScope.forPublication(@splat(8), 2, std.testing.io);
    const reuse = (try Reuse.create(a, store, &.{first}, .none)).?;
    const second = try publishSourceReusing(a, out, &store, .{ .contiguous = changed }, .none, null, reuse);
    try std.testing.expect(reuse.reused_bytes > changed.len * 3 / 4);
    try std.testing.expect(reuse.uploaded_bytes < changed.len / 4);
    reuse.deinit();
    // Reopen without local verification caches or the reuse index. The
    // persisted directory alone owns old and new physical pack ranges.
    fs.deinit();
    fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.initExisting(a, directory.path());
    store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(8), 3, std.testing.io);
    var data = try load(a, .{ .store = fs.artifactStore(), .cache = null, .context = .{}, .cancellation = .none }, second);
    defer data.deinit(a);
    const restored = try a.alloc(u8, changed.len);
    defer a.free(restored);
    try data.source().readInto(0, restored);
    try std.testing.expectEqualSlices(u8, changed, restored);
    const committed = try loadDirectory(out, .{ .store = store, .cache = null, .context = .{}, .cancellation = .none }, second);
    var old_refs: usize = 0;
    for (committed.blocks) |piece| {
        if ((try stores.uploadScopeFromArtifactId(piece.retainedArtifact().artifact_id)).?.fencingToken() == 1) old_refs += 1;
    }
    try std.testing.expect(old_refs > 32);
    const identical = (try Reuse.create(a, store, &.{second}, .none)).?;
    defer identical.deinit();
    _ = try publishSourceReusing(a, out, &store, .{ .contiguous = changed }, .none, null, identical);
    try std.testing.expectEqual(@as(u64, 0), identical.uploaded_bytes);
}
