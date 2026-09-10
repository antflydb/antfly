// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Leaf-local mutation representation used by the experimental native HBC authority.
//! A manifest owns ordered references to immutable, mmap-friendly RaBitQ
//! chunks. Deletion replaces only references; appends encode only new rows.
//! Chunk identity plus ordinal identifies a vector revision (an ID does not).
//! The enclosing posting WAL transaction must bind this manifest to topology,
//! routing bounds, vector revisions and source coverage. It must fsync chunks
//! before publishing a manifest that references them. This module performs no
//! I/O and never publishes implicitly; preparation and rebasing are off-lane.
const std = @import("std");
const Allocator = std.mem.Allocator;
const Crc32 = @import("antfly_hash").Crc32;
const directory = @import("quantized_directory.zig");
const runtime = @import("hbc_runtime.zig");
const proto = @import("antfly_vector").proto;

const header_len = 80;
const manifest_header_len = 104;
const run_len = 40;
const max_encoded_bytes = 64 * 1024 * 1024;

pub fn isManifest(bytes: []const u8) bool {
    return bytes.len >= 4 and std.mem.eql(u8, bytes[0..4], "AFRM");
}

/// Recovery can schedule bounded deferred work without touching code pages.
/// The manifest explicitly names its first outstanding maintenance revision.
pub fn manifestHasDebt(bytes: []const u8) !bool {
    return (try ManifestStats.decode(bytes)).first_debt_revision != 0;
}

/// Checksummed maintenance metadata, readable without touching scoring pages.
/// It describes this exact leaf/origin/revision, never aggregate index debt.
pub const ManifestStats = struct {
    identity: Identity,
    revision: u64,
    first_debt_revision: u64,
    rows: u64,
    runs: usize,
    chunks: usize,
    physical_rows: u64,
    physical_bytes: u64,

    pub const Layout = struct { identity: Identity, serial: u64, revision: u64, rows: u64, bytes: u64 };

    /// The newest compact physical base named by this manifest. Reading this
    /// descriptor does not resolve or fault any scoring payload.
    pub fn compactLayout(bytes: []const u8) !?Layout {
        const stats = try decode(bytes);
        var result: ?Layout = null;
        for (0..stats.runs) |i| {
            const off = manifest_header_len + i * run_len;
            const serial = get(u64, bytes, off);
            if (serial & (@as(u64, 1) << 63) == 0) continue;
            if (result) |old| if (serial <= old.serial) continue;
            result = .{ .identity = stats.identity, .serial = serial, .revision = get(u64, bytes, off + 8), .rows = stats.physical_rows, .bytes = stats.physical_bytes };
        }
        return result;
    }

    /// A clean old manifest can become a subset of a compact chunk after
    /// handoff. Conservatively schedule that leaf for normalization without
    /// decoding candidate planes on the writer lane or during recovery.
    pub fn withLayoutDebt(stats: ManifestStats, bytes: []const u8, layout: ?Layout) ManifestStats {
        const compact = layout orelse return stats;
        if (!std.meta.eql(stats.identity, compact.identity) or stats.revision < compact.revision) return stats;
        for (0..stats.runs) |i| {
            const off = manifest_header_len + i * run_len;
            if (get(u64, bytes, off) == compact.serial or get(u64, bytes, off + 8) > compact.revision) continue;
            var result = stats;
            result.first_debt_revision = @max(stats.first_debt_revision, compact.revision);
            result.physical_rows = @max(stats.physical_rows, compact.rows);
            result.physical_bytes = @max(stats.physical_bytes, compact.bytes);
            return result;
        }
        return stats;
    }

    pub fn decode(bytes: []const u8) !ManifestStats {
        try validateFrame(bytes, "AFRM");
        if (bytes.len < manifest_header_len) return error.InvalidPostingRows;
        const result: ManifestStats = .{
            .identity = readIdentity(bytes),
            .revision = get(u64, bytes, 40),
            .first_debt_revision = get(u64, bytes, 72),
            .rows = get(u64, bytes, 56),
            .runs = std.math.cast(usize, get(u64, bytes, 64)) orelse return error.InvalidPostingRows,
            .chunks = get(u32, bytes, 96),
            .physical_rows = get(u64, bytes, 88),
            .physical_bytes = get(u64, bytes, 80),
        };
        if (!result.identity.valid() or result.revision == 0 or result.first_debt_revision > result.revision or
            result.runs > Policy.hard_runs or result.chunks > Policy.hard_chunks or result.chunks > result.runs or
            result.physical_bytes > Policy.hard_bytes or result.rows > result.physical_rows or
            bytes.len != manifest_header_len + result.runs * run_len or get(u32, bytes, 100) != 0 or
            (result.first_debt_revision != 0) != (result.chunks > 1 or result.physical_rows > result.rows)) return error.InvalidPostingRows;
        return result;
    }

    pub fn needsRepack(self: ManifestStats, policy: Policy, age_ns: u64) bool {
        const removed = self.physical_rows -| self.rows;
        return self.runs > policy.soft_runs or self.chunks > policy.soft_chunks or
            (self.first_debt_revision != 0 and self.physical_bytes > policy.soft_bytes) or
            (removed != 0 and removed *| 100 >= self.physical_rows *| policy.tombstone_percent) or
            (self.first_debt_revision != 0 and age_ns >= policy.max_age_ns);
    }
};

/// A committed allocator value lives at native row-chunk key zero. Serial
/// zero is never a chunk. Replaying this value with the rest of the capture
/// prevents identity reuse across WAL rotation, checkpointing and restart.
pub const Allocation = struct {
    incarnation: u64,
    serial: u64,

    pub fn encode(self: Allocation) [24]u8 {
        var bytes: [24]u8 = @splat(0);
        @memcpy(bytes[0..4], "AFRA");
        put(u64, &bytes, 8, self.incarnation);
        put(u64, &bytes, 16, self.serial);
        put(u32, &bytes, 4, Crc32.hash(bytes[8..]));
        return bytes;
    }

    pub fn decode(bytes: []const u8) !Allocation {
        if (bytes.len != 24 or !std.mem.eql(u8, bytes[0..4], "AFRA") or
            get(u32, bytes, 4) != Crc32.hash(bytes[8..]) or get(u64, bytes, 8) == 0 or get(u64, bytes, 16) > std.math.maxInt(u63))
            return error.InvalidPostingRows;
        return .{ .incarnation = get(u64, bytes, 8), .serial = get(u64, bytes, 16) };
    }
};

pub const Identity = struct {
    incarnation: u64,
    leaf: u64,
    origin: u64,

    fn valid(self: Identity) bool {
        return self.incarnation != 0 and self.leaf != 0 and self.origin != 0;
    }
};

pub const RowRef = struct {
    chunk: u64,
    row: u32,
};

/// An authenticated physical reference, independent of a vector ID. Redirects
/// preserve it across repacking; an update with the same ID is a different row.
pub const Reference = struct {
    serial: u64,
    revision: u64,
    bytes: u64,
    checksum: u32,
    start: u32,
    len: u32,

    pub fn fromRun(run: Run) Reference {
        return .{ .serial = run.chunk.serial, .revision = run.chunk.revision, .bytes = run.chunk.bytes.len, .checksum = run.chunk.checksum, .start = run.start, .len = run.len };
    }
};

/// Durable translation of captured row revisions, published in the same
/// segment as their replacement chunk. Newer WAL manifests can still name
/// the old rows without undoing the compact layout or retaining their payload.
/// A gap denotes an already deleted revision; resolving it fails closed.
pub const Redirect = struct {
    bytes: []const u8,
    pub const range_len = 40;
    pub const max_hops = 8;

    pub fn isRedirect(bytes: []const u8) bool {
        return bytes.len >= 4 and std.mem.eql(u8, bytes[0..4], "AFRR");
    }

    pub fn init(bytes: []const u8) !Redirect {
        try validateFrame(bytes, "AFRR");
        const range_count = get(u32, bytes, 72);
        if (!readIdentity(bytes).valid() or get(u64, bytes, 40) == 0 or get(u64, bytes, 48) == 0 or
            range_count == 0 or range_count > Policy.hard_runs or bytes.len != header_len + @as(usize, range_count) * range_len or get(u32, bytes, 76) != 0)
            return error.InvalidPostingRows;
        const self: Redirect = .{ .bytes = bytes };
        var previous_end: u64 = 0;
        for (0..range_count) |i| {
            const off = header_len + i * range_len;
            const range_start = get(u32, bytes, off);
            const ref = self.target(i);
            const end = @as(u64, range_start) + ref.len;
            if (range_start < previous_end or ref.len == 0 or end > get(u32, bytes, 68) or
                ref.serial == 0 or ref.serial == get(u64, bytes, 40) or ref.revision < get(u64, bytes, 48) or
                ref.bytes < header_len or @as(u64, ref.start) + ref.len > std.math.maxInt(u32)) return error.InvalidPostingRows;
            previous_end = end;
        }
        return self;
    }

    pub fn validateReference(self: Redirect, identity: Identity, ref: Reference) !void {
        if (!std.meta.eql(identity, readIdentity(self.bytes)) or ref.serial != get(u64, self.bytes, 40) or
            ref.revision != get(u64, self.bytes, 48) or ref.bytes != get(u64, self.bytes, 56) or ref.checksum != get(u32, self.bytes, 64) or
            ref.len == 0 or @as(u64, ref.start) + ref.len > get(u32, self.bytes, 68)) return error.PostingChunkIdentityConflict;
    }

    pub fn count(self: Redirect) usize {
        return get(u32, self.bytes, 72);
    }

    pub fn start(self: Redirect, i: usize) u32 {
        return get(u32, self.bytes, header_len + i * range_len);
    }

    pub fn target(self: Redirect, i: usize) Reference {
        const off = header_len + i * range_len;
        return .{ .len = get(u32, self.bytes, off + 4), .serial = get(u64, self.bytes, off + 8), .revision = get(u64, self.bytes, off + 16), .bytes = get(u64, self.bytes, off + 24), .checksum = get(u32, self.bytes, off + 32), .start = get(u32, self.bytes, off + 36) };
    }
};

/// Resolving metadata never loads authoritative vectors. The resolver owns
/// borrowed frames/chunks until Snapshot.init takes independent chunk leases.
pub fn resolveReference(alloc: Allocator, identity: Identity, ref: Reference, resolver: anytype, runs: *std.ArrayListUnmanaged(Run), depth: usize) anyerror!bool {
    if (depth >= Redirect.max_hops) return error.PostingRowRedirectDepthExceeded;
    const Resolver = switch (@typeInfo(@TypeOf(resolver))) {
        .pointer => |p| p.child,
        else => @TypeOf(resolver),
    };
    if (comptime @hasDecl(Resolver, "redirect")) {
        if (try resolver.redirect(ref.serial)) |redirect| {
            try redirect.validateReference(identity, ref);
            var next = ref.start;
            const end = @as(u64, next) + ref.len;
            for (0..redirect.count()) |i| {
                var target = redirect.target(i);
                const start = redirect.start(i);
                const range_end = @as(u64, start) + target.len;
                if (range_end <= next) continue;
                if (next == end) break;
                if (start > next) return error.StalePostingRow;
                target.start += next - start;
                target.len = @intCast(@min(end, range_end) - next);
                _ = try resolveReference(alloc, identity, target, resolver, runs, depth + 1);
                next += target.len;
            }
            if (next != end) return error.StalePostingRow;
            return true;
        }
    }
    const chunk = (try resolver.get(ref.serial)) orelse return error.MissingPostingChunk;
    if (chunk.revision != ref.revision or chunk.bytes.len != ref.bytes or chunk.checksum != ref.checksum) return error.PostingChunkIdentityConflict;
    if (!std.meta.eql(identity, chunk.identity) or ref.len == 0 or ref.start > chunk.view.count or ref.len > chunk.view.count - ref.start) return error.InvalidPostingRows;
    try appendRun(alloc, runs, .{ .chunk = chunk, .start = ref.start, .len = ref.len });
    return false;
}

/// One physical chunk; serials must never be reused within an incarnation.
/// The byte buffer can be owned heap memory or a retained mapping. Reader
/// verification state is private, while all scoring columns stay borrowed.
/// Scoring origins are separate immutable objects. Every row chunk in a leaf
/// generation retains this object; neither deletion nor repacking duplicates
/// its centroid. Its durable key is Identity.origin in the row-chunk namespace.
pub const Origin = struct {
    alloc: Allocator,
    refs: std.atomic.Value(u32) = .init(1),
    identity: Identity,
    bytes: []align(8) const u8,
    centroid: []const f32,
    centroid_norm: f32,
    metric: u8,
    checksum: u32,

    pub fn decode(alloc: Allocator, encoded: []const u8) !*Origin {
        try validateFrame(encoded, "AFRO");
        const identity = readIdentity(encoded);
        const dims = get(u64, encoded, 40);
        if (!identity.valid() or dims == 0 or dims != (encoded.len - header_len) / 4 or
            (encoded.len - header_len) % 4 != 0 or get(u64, encoded, 48) > 2 or
            get(u32, encoded, 60) != 0 or get(u64, encoded, 64) != 0 or get(u64, encoded, 72) != 0)
            return error.InvalidPostingRows;
        const norm: f32 = @bitCast(get(u32, encoded, 56));
        if (!std.math.isFinite(norm) or norm < 0) return error.InvalidPostingRows;
        const bytes = try alloc.alignedAlloc(u8, .@"8", encoded.len);
        errdefer alloc.free(bytes);
        @memcpy(bytes, encoded);
        const centroid = std.mem.bytesAsSlice(f32, bytes[header_len..]);
        for (centroid) |value| if (!std.math.isFinite(value)) return error.InvalidPostingRows;
        const self = try alloc.create(Origin);
        self.* = .{ .alloc = alloc, .identity = identity, .bytes = bytes, .centroid = centroid, .centroid_norm = norm, .metric = @intCast(get(u64, bytes, 48)), .checksum = Crc32.hash(bytes) };
        return self;
    }

    pub fn build(alloc: Allocator, identity: Identity, set: *const proto.RaBitQuantizedVectorSet) !*Origin {
        if (@intFromEnum(set.metric) < 0 or @intFromEnum(set.metric) > 2) return error.InvalidPostingRows;
        const size = try std.math.add(usize, header_len, try std.math.mul(usize, set.centroid.len, 4));
        if (size > max_encoded_bytes) return error.PostingRowBackpressure;
        const bytes = try alloc.alloc(u8, size);
        defer alloc.free(bytes);
        @memset(bytes, 0);
        writeHeader(bytes, "AFRO", identity);
        put(u64, bytes, 40, set.centroid.len);
        put(u64, bytes, 48, @intCast(@intFromEnum(set.metric)));
        put(u32, bytes, 56, @bitCast(set.centroid_norm));
        @memcpy(bytes[header_len..], std.mem.sliceAsBytes(set.centroid));
        seal(bytes);
        return decode(alloc, bytes);
    }

    pub fn retain(self: *Origin) void {
        const before = self.refs.fetchAdd(1, .monotonic);
        std.debug.assert(before != 0 and before != std.math.maxInt(u32));
    }

    pub fn release(self: *Origin) void {
        const before = self.refs.fetchSub(1, .acq_rel);
        std.debug.assert(before != 0);
        if (before != 1) return;
        self.alloc.free(self.bytes);
        self.alloc.destroy(self);
    }
};

pub const Chunk = struct {
    alloc: Allocator,
    refs: std.atomic.Value(u32) = .init(1),
    identity: Identity,
    serial: u64,
    revision: u64,
    bytes: []const u8,
    checksum: u32,
    origin: *Origin,
    view: directory.View,
    backing: Backing,

    pub const Backing = union(enum) {
        owned: std.mem.Alignment,
        leased: struct { ptr: *anyopaque, release: *const fn (*anyopaque) void },
    };

    /// Validates the frame before exposing a dependency key to the resolver.
    pub fn originIdentity(bytes: []const u8) !Identity {
        try validateFrame(bytes, "AFRC");
        const identity = readIdentity(bytes);
        if (!identity.valid()) return error.InvalidPostingRows;
        return identity;
    }

    /// Takes backing only on success, retaining origin independently. AFRC V2
    /// contains IDs and scoring columns only; AFRO must be durable in the same
    /// transaction/reference closure before this chunk can be published.
    pub fn open(alloc: Allocator, bytes: []const u8, backing: Backing, origin: *Origin) !*Chunk {
        const identity = try originIdentity(bytes);
        if (!std.meta.eql(identity, origin.identity) or get(u32, bytes, 72) != origin.checksum)
            return error.PostingScoringOriginMismatch;
        const count_u64 = get(u64, bytes, 56);
        const flags = get(u32, bytes, 76);
        if (@intFromPtr(bytes.ptr) % 8 != 0 or count_u64 == 0 or count_u64 > std.math.maxInt(u32) or
            get(u64, bytes, 40) == 0 or get(u64, bytes, 48) == 0 or
            get(u64, bytes, 64) != bytes.len - header_len or flags > 1 or
            (flags == 1 and origin.metric != 0)) return error.InvalidPostingRows;
        const count: usize = @intCast(count_u64);
        const width = @import("antfly_vector").rabitq.codeWidth(origin.centroid.len);
        const row_bytes = try std.math.add(usize, 20 + @as(usize, if (flags == 1) 0 else 4), try std.math.mul(usize, width, 8));
        if (try std.math.mul(usize, count, row_bytes) != bytes.len - header_len)
            return error.InvalidPostingRows;
        var cursor: usize = header_len;
        const view = directory.View{
            .metric = origin.metric,
            .centroid = origin.centroid,
            .centroid_norm = origin.centroid_norm,
            .member_ids = takeColumn(u64, bytes, &cursor, count),
            .codes = takeColumn(u64, bytes, &cursor, count * width),
            .code_counts = takeColumn(u32, bytes, &cursor, count),
            .centroid_distances = takeColumn(f32, bytes, &cursor, count),
            .quantized_dot_products = takeColumn(f32, bytes, &cursor, count),
            .centroid_dot_products = takeColumn(f32, bytes, &cursor, if (flags == 1) 0 else count),
            .omitted_l2_centroid_dots = flags == 1,
            .count = count,
            .width = width,
            .projections = null,
        };
        var ids = std.AutoHashMapUnmanaged(u64, void).empty;
        defer ids.deinit(alloc);
        for (view.member_ids) |id| if ((try ids.getOrPut(alloc, id)).found_existing) return error.DuplicatePostingVector;
        const self = try alloc.create(Chunk);
        origin.retain();
        self.* = .{
            .alloc = alloc,
            .identity = identity,
            .serial = get(u64, bytes, 40),
            .revision = get(u64, bytes, 48),
            .bytes = bytes,
            .checksum = Crc32.hash(bytes),
            .origin = origin,
            .view = view,
            .backing = backing,
        };
        return self;
    }

    fn takeColumn(comptime T: type, bytes: []const u8, cursor: *usize, count: usize) []const T {
        const start = cursor.*;
        cursor.* += count * @sizeOf(T);
        return std.mem.bytesAsSlice(T, @as([]align(@alignOf(T)) const u8, @alignCast(bytes[start..cursor.*])));
    }

    /// Convenience for constructing an initial leaf. Appends and repacks use
    /// buildWithOrigin so their query views also share the centroid allocation.
    pub fn build(alloc: Allocator, identity: Identity, serial: u64, revision: u64, ids: []const u64, set: *const proto.RaBitQuantizedVectorSet) !*Chunk {
        const origin = try Origin.build(alloc, identity, set);
        defer origin.release();
        return buildWithOrigin(alloc, origin, serial, revision, ids, set);
    }

    pub fn buildWithOrigin(alloc: Allocator, origin: *Origin, serial: u64, revision: u64, ids: []const u64, set: *const proto.RaBitQuantizedVectorSet) !*Chunk {
        if (!std.mem.eql(u8, std.mem.sliceAsBytes(origin.centroid), std.mem.sliceAsBytes(set.centroid)) or
            @as(u32, @bitCast(origin.centroid_norm)) != @as(u32, @bitCast(set.centroid_norm)) or
            origin.metric != @intFromEnum(set.metric)) return error.PostingScoringOriginMismatch;
        const count = ids.len;
        const width = @import("antfly_vector").rabitq.codeWidth(origin.centroid.len);
        const omitted = origin.metric == 0 and set.centroid_dot_products.len == 0;
        if (serial == 0 or revision == 0 or count == 0 or count > std.math.maxInt(u32) or
            count != set.getCount() or set.codes.width != width or set.codes.data.len != count * width or
            set.code_counts.len != count or set.centroid_distances.len != count or
            set.quantized_dot_products.len != count or (!omitted and set.centroid_dot_products.len != count))
            return error.InvalidPostingRows;
        const row_bytes = try std.math.add(usize, 20 + @as(usize, if (omitted) 0 else 4), try std.math.mul(usize, width, 8));
        const len = try std.math.add(usize, header_len, try std.math.mul(usize, count, row_bytes));
        if (len > max_encoded_bytes) return error.PostingRowBackpressure;
        const bytes = try alloc.alignedAlloc(u8, .@"8", len);
        errdefer alloc.free(bytes);
        @memset(bytes, 0);
        writeHeader(bytes, "AFRC", origin.identity);
        put(u16, bytes, 4, 2);
        put(u64, bytes, 40, serial);
        put(u64, bytes, 48, revision);
        put(u64, bytes, 56, count);
        put(u64, bytes, 64, len - header_len);
        put(u32, bytes, 72, origin.checksum);
        put(u32, bytes, 76, if (omitted) 1 else 0);
        var cursor: usize = header_len;
        inline for (.{ std.mem.sliceAsBytes(ids), std.mem.sliceAsBytes(set.codes.data), std.mem.sliceAsBytes(set.code_counts), std.mem.sliceAsBytes(set.centroid_distances), std.mem.sliceAsBytes(set.quantized_dot_products), std.mem.sliceAsBytes(set.centroid_dot_products) }) |column| {
            @memcpy(bytes[cursor..][0..column.len], column);
            cursor += column.len;
        }
        std.debug.assert(cursor == bytes.len);
        seal(bytes);
        return open(alloc, bytes, .{ .owned = .@"8" }, origin);
    }

    pub fn retain(self: *Chunk) void {
        const before = self.refs.fetchAdd(1, .monotonic);
        std.debug.assert(before != 0 and before != std.math.maxInt(u32));
    }

    pub fn release(self: *Chunk) void {
        const before = self.refs.fetchSub(1, .acq_rel);
        std.debug.assert(before != 0);
        if (before != 1) return;
        self.origin.release();
        switch (self.backing) {
            .owned => |alignment| self.alloc.rawFree(@constCast(self.bytes), alignment, @returnAddress()),
            .leased => |lease| lease.release(lease.ptr),
        }
        self.alloc.destroy(self);
    }
};

pub const Run = struct {
    chunk: *Chunk,
    start: u32,
    len: u32,

    /// No allocation or aggregate decoding. The caller holds a Snapshot lease.
    pub fn scan(self: Run) runtime.NativeLeafScanView {
        const first: usize = self.start;
        const end = first + self.len;
        const view = self.chunk.view;
        var set = view.asProto();
        set.codes.count = self.len;
        set.codes.data = set.codes.data[first * view.width .. end * view.width];
        set.code_counts = set.code_counts[first..end];
        set.centroid_distances = set.centroid_distances[first..end];
        set.quantized_dot_products = set.quantized_dot_products[first..end];
        if (set.centroid_dot_products.len != 0) set.centroid_dot_products = set.centroid_dot_products[first..end];
        return .{ .member_ids = view.member_ids[first..end], .quantized = .{ .rabit = set } };
    }
};

/// This is a bounded leaf view, not a chain of full predecessor snapshots.
/// Retaining old queries/durable manifests retains only referenced chunks.
pub const Snapshot = struct {
    alloc: Allocator,
    identity: Identity,
    revision: u64,
    coverage: u64,
    runs: []Run,
    row_count: usize,
    physical_bytes: u64,
    physical_rows: u64,
    chunk_count: usize,
    first_debt_revision: u64 = 0,

    pub fn init(alloc: Allocator, identity: Identity, revision: u64, coverage: u64, runs: []const Run) !Snapshot {
        return initValidated(alloc, identity, revision, coverage, runs, true, false);
    }

    fn initValidated(alloc: Allocator, identity: Identity, revision: u64, coverage: u64, runs: []const Run, comptime validate_ids: bool, redirected: bool) !Snapshot {
        if (!identity.valid() or revision == 0) return error.InvalidPostingRows;
        if (runs.len > Policy.hard_runs) return error.PostingRowBackpressure;
        var ids = std.AutoHashMapUnmanaged(u64, void).empty;
        defer ids.deinit(alloc);
        var chunks = std.AutoHashMapUnmanaged(u64, *Chunk).empty;
        defer chunks.deinit(alloc);
        var count: usize = 0;
        var bytes: u64 = if (runs.len == 0) 0 else runs[0].chunk.origin.bytes.len;
        var physical_rows: u64 = 0;
        // Handoff can retain one compact base plus a source-admitted tail.
        // Reads/recovery must accept that bounded overlap; fresh mutations
        // still use the strict limit and backpressure until repacking drains
        // it. No extra scoring payload is allocated here (only leases).
        const max_chunks: usize = Policy.hard_chunks + @as(usize, @intFromBool(redirected));
        const max_bytes: u64 = Policy.hard_bytes * @as(u64, if (redirected) 2 else 1);
        for (runs) |run| {
            const chunk = run.chunk;
            if (!std.meta.eql(identity, chunk.identity) or chunk.revision > revision or
                run.len == 0 or run.start > chunk.view.count or run.len > chunk.view.count - run.start)
                return error.InvalidPostingRows;
            if (runs.len != 0 and !sameOrigin(runs[0].chunk.view, chunk.view)) return error.PostingScoringOriginMismatch;
            const entry = try chunks.getOrPut(alloc, chunk.serial);
            if (entry.found_existing) {
                if (entry.value_ptr.* != chunk) return error.PostingChunkIdentityConflict;
            } else {
                entry.value_ptr.* = chunk;
                bytes = try std.math.add(u64, bytes, chunk.bytes.len);
                physical_rows += chunk.view.count;
                // Reject as soon as the retained-work limit is exceeded, before
                // validating/allocating IDs for another potentially large chunk.
                if (chunks.count() > max_chunks or bytes > max_bytes) return error.PostingRowBackpressure;
            }
            if (validate_ids) for (run.scan().member_ids) |id| {
                if ((try ids.getOrPut(alloc, id)).found_existing) return error.DuplicatePostingVector;
            };
            count = try std.math.add(usize, count, run.len);
        }
        if (chunks.count() > max_chunks or bytes > max_bytes) return error.PostingRowBackpressure;
        // The reference array is small; no candidate payload is copied.
        const owned = try alloc.dupe(Run, runs);
        for (owned) |run| run.chunk.retain();
        return .{ .alloc = alloc, .identity = identity, .revision = revision, .coverage = coverage, .runs = owned, .row_count = count, .physical_bytes = bytes, .physical_rows = physical_rows, .chunk_count = chunks.count(), .first_debt_revision = if (chunks.count() > 1 or physical_rows > count) revision else 0 };
    }

    pub fn clone(self: *const Snapshot) !Snapshot {
        const runs = try self.alloc.dupe(Run, self.runs);
        for (runs) |run| run.chunk.retain();
        var result = self.*;
        result.runs = runs;
        return result;
    }

    /// Compatibility/maintenance only. Serving uses scoreTo/Run.scan and
    /// mutation uses row references; neither needs this aggregate allocation.
    pub fn materialize(self: *const Snapshot, alloc: Allocator) !proto.RaBitQuantizedVectorSet {
        if (self.runs.len == 0) return error.InvalidPostingRows;
        const origin = self.runs[0].chunk.view;
        var set: proto.RaBitQuantizedVectorSet = .{ .metric = @enumFromInt(origin.metric), .centroid_norm = origin.centroid_norm };
        errdefer set.deinit(alloc);
        set.centroid = try alloc.dupe(f32, origin.centroid);
        set.codes = .{ .count = @intCast(self.row_count), .width = @intCast(origin.width), .data = try alloc.alloc(u64, self.row_count * origin.width) };
        set.code_counts = try alloc.alloc(u32, self.row_count);
        set.centroid_distances = try alloc.alloc(f32, self.row_count);
        set.quantized_dot_products = try alloc.alloc(f32, self.row_count);
        if (!origin.omitted_l2_centroid_dots) set.centroid_dot_products = try alloc.alloc(f32, self.row_count);
        var offset: usize = 0;
        for (self.runs) |run| {
            const source = run.scan().quantized.rabit;
            const end = offset + run.len;
            @memcpy(set.codes.data[offset * origin.width .. end * origin.width], source.codes.data);
            @memcpy(set.code_counts[offset..end], source.code_counts);
            @memcpy(set.centroid_distances[offset..end], source.centroid_distances);
            @memcpy(set.quantized_dot_products[offset..end], source.quantized_dot_products);
            if (set.centroid_dot_products.len != 0) @memcpy(set.centroid_dot_products[offset..end], source.centroid_dot_products);
            offset = end;
        }
        return set;
    }

    pub fn deinit(self: *Snapshot) void {
        for (self.runs) |run| run.chunk.release();
        self.alloc.free(self.runs);
        self.* = undefined;
    }

    /// Fused, allocation-free scoring in canonical live order. Consecutive
    /// chunks share one query preparation against the leaf's retained origin,
    /// rather than repeating normalization/quantization for each chunk or gap. A bounded
    /// stack wave avoids turning adversarial fragmentation into query scratch.
    /// output.write(vector_id, distance, error_bound) uses the existing selector;
    /// authoritative completion and filter/visibility semantics stay above it.
    pub fn scoreTo(self: *const Snapshot, quantizer: *const @import("antfly_vector").quantizer.RaBitQuantizer, query: []const f32, scratch: *@import("antfly_vector").quantizer.RaBitQuantizer.EstimateScratch, cancellation: ?@import("antfly_vector").quantizer.CancellationToken, output: anytype) !void {
        const q = @import("antfly_vector").quantizer;
        if (cancellation) |token| try token.check();
        if (query.len != quantizer.dims) return error.InvalidPostingRows;
        if (self.runs.len == 0) return;
        const origin = self.runs[0].chunk.view.asProto();
        const prepared = try quantizer.prepareEstimate(&origin, query, scratch, cancellation);
        var ranges: [128]q.ScoreRange = undefined;
        var next: usize = 0;
        while (next < self.runs.len) {
            if (cancellation) |token| try token.check();
            const chunk = self.runs[next].chunk;
            if (chunk.view.centroid.len != quantizer.dims or chunk.view.metric != @intFromEnum(quantizer.distance_metric))
                return error.InvalidPostingRows;
            var count: usize = 0;
            var previous_end: usize = 0;
            while (next < self.runs.len and count < ranges.len) {
                const run = self.runs[next];
                if (run.chunk != chunk or run.start < previous_end) break;
                ranges[count] = .{ .start = run.start, .end = @as(usize, run.start) + run.len };
                previous_end = ranges[count].end;
                count += 1;
                next += 1;
            }
            const Output = struct {
                ids: []const u64,
                sink: @TypeOf(output),
                pub fn write(self_: @This(), row: usize, distance: f32, bound: f32) void {
                    self_.sink.write(self_.ids[row], distance, bound);
                }
            };
            const set = chunk.view.asProto();
            try quantizer.estimatePreparedDistancesInRangesTo(&set, prepared, cancellation, ranges[0..count], Output{ .ids = chunk.view.member_ids, .sink = output });
        }
    }

    /// Deletes are exact old row references, ordered in canonical live order.
    /// A stale delete cannot remove a newly appended row with the same ID.
    /// All validation/preparation happens before any enclosing durable append.
    pub fn mutate(self: *const Snapshot, expected_revision: u64, revision: u64, coverage: u64, deletes: []const RowRef, append: ?*Chunk) !Snapshot {
        if (expected_revision != self.revision) return error.PostingRowsSuperseded;
        if (revision <= self.revision or coverage < self.coverage) return error.PostingRowSequenceRegression;
        if (deletes.len > self.row_count) return error.StalePostingRow;
        if (append) |chunk| {
            if (chunk.revision != revision) return error.PostingRowSequenceRegression;
            for (self.runs) |run| if (run.chunk.serial == chunk.serial) return error.PostingChunkIdentityConflict;
            if (self.runs.len != 0 and !sameOrigin(self.runs[0].chunk.view, chunk.view)) return error.PostingScoringOriginMismatch;
        }
        var runs = std.ArrayListUnmanaged(Run).empty;
        defer runs.deinit(self.alloc);
        try runs.ensureTotalCapacity(self.alloc, @min(Policy.hard_runs, self.runs.len + deletes.len + @intFromBool(append != null)));
        var deleted: usize = 0;
        for (self.runs) |run| {
            var start = run.start;
            const end = run.start + run.len;
            while (deleted < deletes.len and deletes[deleted].chunk == run.chunk.serial and
                deletes[deleted].row >= start and deletes[deleted].row < end)
            {
                const row = deletes[deleted].row;
                try appendRun(self.alloc, &runs, .{ .chunk = run.chunk, .start = start, .len = row - start });
                start = row + 1;
                deleted += 1;
            }
            try appendRun(self.alloc, &runs, .{ .chunk = run.chunk, .start = start, .len = end - start });
        }
        if (deleted != deletes.len) return error.StalePostingRow;
        if (append) |chunk| {
            // Parent and chunk were validated once at construction/recovery.
            // Only collisions between appended IDs and live survivors are new.
            // Do not rebuild a survivor-sized ID hash table on every deletion.
            var new_ids = std.AutoHashMapUnmanaged(u64, void).empty;
            defer new_ids.deinit(self.alloc);
            for (chunk.view.member_ids) |id| try new_ids.put(self.alloc, id, {});
            for (runs.items) |run| for (run.scan().member_ids) |id| {
                if (new_ids.contains(id)) return error.DuplicatePostingVector;
            };
            try appendRun(self.alloc, &runs, .{ .chunk = chunk, .start = 0, .len = @intCast(chunk.view.count) });
        }
        var result = try initValidated(self.alloc, self.identity, revision, coverage, runs.items, false, false);
        if (result.first_debt_revision != 0 and self.first_debt_revision != 0) result.first_debt_revision = self.first_debt_revision;
        return result;
    }

    /// Self-framed, checksummed manifest suitable for a committed posting-WAL
    /// value. No chunk payload is embedded or copied here, including at restart.
    pub fn encode(self: *const Snapshot) ![]u8 {
        const bytes = try self.alloc.alloc(u8, manifest_header_len + self.runs.len * run_len);
        @memset(bytes, 0);
        writeHeader(bytes, "AFRM", self.identity);
        put(u16, bytes, 4, 2);
        put(u64, bytes, 40, self.revision);
        put(u64, bytes, 48, self.coverage);
        put(u64, bytes, 56, self.row_count);
        put(u64, bytes, 64, self.runs.len);
        put(u64, bytes, 72, self.first_debt_revision);
        put(u64, bytes, 80, self.physical_bytes);
        put(u64, bytes, 88, self.physical_rows);
        put(u32, bytes, 96, @intCast(self.chunk_count));
        for (self.runs, 0..) |run, i| {
            const off = manifest_header_len + i * run_len;
            put(u64, bytes, off, run.chunk.serial);
            put(u64, bytes, off + 8, run.chunk.revision);
            put(u64, bytes, off + 16, run.chunk.bytes.len);
            put(u32, bytes, off + 24, run.chunk.checksum);
            put(u32, bytes, off + 28, run.start);
            put(u32, bytes, off + 32, run.len);
        }
        seal(bytes);
        return bytes;
    }

    /// resolver.get(serial) returns a borrowed Chunk pinned during this call.
    /// Repeated lookups of the same serial must return the same Chunk object.
    /// Result retains its own references; missing/corrupt dependencies fail closed.
    pub fn decode(alloc: Allocator, bytes: []const u8, resolver: anytype) !Snapshot {
        const stats = try ManifestStats.decode(bytes);
        const count = stats.runs;
        var runs = std.ArrayListUnmanaged(Run).empty;
        defer runs.deinit(alloc);
        var redirected = false;
        for (0..count) |i| {
            const off = manifest_header_len + i * run_len;
            if (get(u32, bytes, off + 36) != 0) return error.PostingChunkIdentityConflict;
            redirected = try resolveReference(alloc, readIdentity(bytes), .{ .serial = get(u64, bytes, off), .revision = get(u64, bytes, off + 8), .bytes = get(u64, bytes, off + 16), .checksum = get(u32, bytes, off + 24), .start = get(u32, bytes, off + 28), .len = get(u32, bytes, off + 32) }, resolver, &runs, 0) or redirected;
        }
        var result = try initValidated(alloc, readIdentity(bytes), get(u64, bytes, 40), get(u64, bytes, 48), runs.items, true, redirected);
        errdefer result.deinit();
        if (result.row_count != get(u64, bytes, 56)) return error.InvalidPostingRows;
        if (!redirected and (result.physical_rows != stats.physical_rows or result.physical_bytes != stats.physical_bytes or result.chunk_count != stats.chunks)) return error.InvalidPostingRows;
        // A tail may have removed an entire old chunk. Its old physical view
        // was clean, but a redirect can now select a subset of a compact chunk
        // containing other captured rows. That introduces real packing debt;
        // it must not make this otherwise valid durable manifest unreadable.
        if (result.first_debt_revision != 0 and stats.first_debt_revision != 0) result.first_debt_revision = stats.first_debt_revision;
        return result;
    }
};

fn appendRun(alloc: Allocator, runs: *std.ArrayListUnmanaged(Run), run: Run) !void {
    if (run.len == 0) return;
    if (runs.items.len != 0) {
        const last = &runs.items[runs.items.len - 1];
        if (last.chunk == run.chunk and last.start + last.len == run.start) {
            last.len += run.len;
            return;
        }
    }
    if (runs.items.len >= Policy.hard_runs) return error.PostingRowBackpressure;
    try runs.append(alloc, run);
}

fn sameOrigin(a: directory.View, b: directory.View) bool {
    return a.metric == b.metric and a.width == b.width and
        a.omitted_l2_centroid_dots == b.omitted_l2_centroid_dots and
        @as(u32, @bitCast(a.centroid_norm)) == @as(u32, @bitCast(b.centroid_norm)) and
        std.mem.eql(u8, std.mem.sliceAsBytes(a.centroid), std.mem.sliceAsBytes(b.centroid));
}

/// Scheduling is based on retained work, not a fixed number of mutations.
/// The governor admits preparation memory/I/O externally. Soft debt queues
/// maintenance; hard debt returns explicit retryable backpressure, never does
/// synchronous survivor-vector reads or a hidden recenter on the writer lane.
pub const Policy = struct {
    pub const hard_runs = 4096;
    pub const hard_chunks = 256;
    pub const hard_bytes = 64 * 1024 * 1024;
    soft_runs: usize = 128,
    soft_chunks: usize = 16,
    soft_bytes: u64 = 8 * 1024 * 1024,
    tombstone_percent: u8 = 25,
    max_age_ns: u64 = 30 * std.time.ns_per_s,

    pub fn needsRepack(self: Policy, view: *const Snapshot, debt_age_ns: u64) bool {
        const removed = view.physical_rows -| view.row_count;
        return view.runs.len > self.soft_runs or view.chunk_count > self.soft_chunks or
            ((view.chunk_count > 1 or removed != 0) and view.physical_bytes > self.soft_bytes) or
            (removed != 0 and removed *| 100 >= view.physical_rows *| self.tombstone_percent) or
            (view.chunk_count > 1 or removed != 0) and debt_age_ns >= self.max_age_ns;
    }
};

/// Prepared leaf repack. Owns its captured view and replacement chunk. Old
/// queries keep their chunks; newer rows remain a bounded tail in rebase().
/// Does not recenter: preserving the scoring origin keeps existing radius
/// bounds valid. A future recenter must publish a new origin and routing proof.
pub const Repack = struct {
    base: Snapshot,
    compact: ?*Chunk,

    /// One small redirect per physical input chunk. Sort source ranges for
    /// bounded lookup while retaining canonical destination order. Source
    /// payloads can be reclaimed once old generation leases drain.
    pub fn encodeRedirect(self: *const Repack, source: *Chunk) ![]u8 {
        const compact = self.compact orelse return error.InvalidPostingRows;
        const Range = struct { start: u32, len: u32, target: u32 };
        var ranges = std.ArrayListUnmanaged(Range).empty;
        defer ranges.deinit(self.base.alloc);
        var next: u32 = 0;
        for (self.base.runs) |run| {
            if (run.chunk == source) try ranges.append(self.base.alloc, .{ .start = run.start, .len = run.len, .target = next });
            next += run.len;
        }
        if (ranges.items.len == 0) return error.PostingRowsSuperseded;
        std.mem.sort(Range, ranges.items, {}, struct {
            fn less(_: void, a: Range, b: Range) bool {
                return a.start < b.start;
            }
        }.less);
        const bytes = try self.base.alloc.alloc(u8, header_len + ranges.items.len * Redirect.range_len);
        errdefer self.base.alloc.free(bytes);
        @memset(bytes, 0);
        writeHeader(bytes, "AFRR", source.identity);
        put(u64, bytes, 40, source.serial);
        put(u64, bytes, 48, source.revision);
        put(u64, bytes, 56, source.bytes.len);
        put(u32, bytes, 64, source.checksum);
        put(u32, bytes, 68, @intCast(source.view.count));
        put(u32, bytes, 72, @intCast(ranges.items.len));
        for (ranges.items, 0..) |range, i| {
            const off = header_len + i * Redirect.range_len;
            put(u32, bytes, off, range.start);
            put(u32, bytes, off + 4, range.len);
            put(u64, bytes, off + 8, compact.serial);
            put(u64, bytes, off + 16, compact.revision);
            put(u64, bytes, off + 24, compact.bytes.len);
            put(u32, bytes, off + 32, compact.checksum);
            put(u32, bytes, off + 36, range.target);
        }
        seal(bytes);
        _ = try Redirect.init(bytes);
        return bytes;
    }

    pub fn prepare(base: *const Snapshot, serial: u64) !Repack {
        var pinned = try base.clone();
        errdefer pinned.deinit();
        for (base.runs) |run| if (run.chunk.serial == serial) return error.PostingChunkIdentityConflict;
        if (base.row_count == 0) return .{ .base = pinned, .compact = null };
        const a = base.alloc;
        const origin = base.runs[0].chunk.view;
        var set: proto.RaBitQuantizedVectorSet = .{ .metric = @enumFromInt(origin.metric), .centroid_norm = origin.centroid_norm };
        defer set.deinit(a);
        set.centroid = try a.dupe(f32, origin.centroid);
        set.codes = .{ .count = @intCast(base.row_count), .width = @intCast(origin.width), .data = try a.alloc(u64, base.row_count * origin.width) };
        set.code_counts = try a.alloc(u32, base.row_count);
        set.centroid_distances = try a.alloc(f32, base.row_count);
        set.quantized_dot_products = try a.alloc(f32, base.row_count);
        if (!origin.omitted_l2_centroid_dots) set.centroid_dot_products = try a.alloc(f32, base.row_count);
        const ids = try a.alloc(u64, base.row_count);
        defer a.free(ids);
        var offset: usize = 0;
        for (base.runs) |run| {
            if (!sameOrigin(origin, run.chunk.view) or origin.omitted_l2_centroid_dots != run.chunk.view.omitted_l2_centroid_dots)
                return error.PostingScoringOriginMismatch;
            const scan = run.scan();
            const source = scan.quantized.rabit;
            const end = offset + run.len;
            @memcpy(ids[offset..end], scan.member_ids);
            @memcpy(set.codes.data[offset * origin.width .. end * origin.width], source.codes.data);
            @memcpy(set.code_counts[offset..end], source.code_counts);
            @memcpy(set.centroid_distances[offset..end], source.centroid_distances);
            @memcpy(set.quantized_dot_products[offset..end], source.quantized_dot_products);
            if (set.centroid_dot_products.len != 0) @memcpy(set.centroid_dot_products[offset..end], source.centroid_dot_products);
            offset = end;
        }
        return .{ .base = pinned, .compact = try Chunk.buildWithOrigin(a, base.runs[0].chunk.origin, serial, base.revision, ids, &set) };
    }

    pub fn deinit(self: *Repack) void {
        self.base.deinit();
        if (self.compact) |chunk| chunk.release();
        self.* = undefined;
    }

    /// Also off-lane. Publication must compare the exact `current` generation
    /// token once more, after staging/durability, before its O(1) pointer swap.
    /// A concurrently completed repack/recenter is not an append-only tail.
    pub fn rebase(self: *const Repack, current: *const Snapshot) !Snapshot {
        if (!std.meta.eql(self.base.identity, current.identity) or current.revision < self.base.revision or current.coverage < self.base.coverage)
            return error.PostingRowsSuperseded;
        var positions = std.AutoHashMapUnmanaged(RowRef, u32).empty;
        defer positions.deinit(current.alloc);
        var chunks = std.AutoHashMapUnmanaged(u64, *Chunk).empty;
        defer chunks.deinit(current.alloc);
        var next: u32 = 0;
        for (self.base.runs) |run| {
            try chunks.put(current.alloc, run.chunk.serial, run.chunk);
            for (run.start..run.start + run.len) |row| {
                try positions.put(current.alloc, .{ .chunk = run.chunk.serial, .row = @intCast(row) }, next);
                next += 1;
            }
        }
        var runs = std.ArrayListUnmanaged(Run).empty;
        defer runs.deinit(current.alloc);
        for (current.runs) |run| {
            if (chunks.get(run.chunk.serial)) |captured| {
                // A reader replacement/recovery is a new publication identity,
                // even when its logical revision and coverage happen to match.
                if (captured != run.chunk) return error.PostingRowsSuperseded;
            }
            for (run.start..run.start + run.len) |row| {
                if (positions.get(.{ .chunk = run.chunk.serial, .row = @intCast(row) })) |position| {
                    try appendRun(current.alloc, &runs, .{ .chunk = self.compact orelse return error.PostingRowsSuperseded, .start = position, .len = 1 });
                } else {
                    if (run.chunk.revision <= self.base.revision or (self.compact != null and run.chunk.serial == self.compact.?.serial))
                        return error.PostingRowsSuperseded;
                    try appendRun(current.alloc, &runs, .{ .chunk = run.chunk, .start = @intCast(row), .len = 1 });
                }
            }
        }
        return Snapshot.initValidated(current.alloc, current.identity, current.revision, current.coverage, runs.items, false, true);
    }
};

fn put(comptime T: type, bytes: []u8, offset: usize, value: T) void {
    std.mem.writeInt(T, bytes[offset..][0..@sizeOf(T)], value, .little);
}

fn get(comptime T: type, bytes: []const u8, offset: usize) T {
    return std.mem.readInt(T, bytes[offset..][0..@sizeOf(T)], .little);
}

fn readIdentity(bytes: []const u8) Identity {
    return .{ .incarnation = get(u64, bytes, 16), .leaf = get(u64, bytes, 24), .origin = get(u64, bytes, 32) };
}

fn writeHeader(bytes: []u8, magic: *const [4]u8, identity: Identity) void {
    @memcpy(bytes[0..4], magic);
    put(u16, bytes, 4, 1);
    put(u32, bytes, 8, @intCast(bytes.len));
    put(u64, bytes, 16, identity.incarnation);
    put(u64, bytes, 24, identity.leaf);
    put(u64, bytes, 32, identity.origin);
}

fn checksum(bytes: []const u8) u32 {
    var crc = Crc32.init();
    crc.update(bytes[0..12]);
    crc.update(bytes[16..]);
    return crc.final();
}

fn seal(bytes: []u8) void {
    put(u32, bytes, 12, checksum(bytes));
}

fn validateFrame(bytes: []const u8, magic: *const [4]u8) !void {
    if (bytes.len < header_len or bytes.len > max_encoded_bytes or !std.mem.eql(u8, bytes[0..4], magic) or
        get(u16, bytes, 4) != @as(u16, if (std.mem.eql(u8, magic, "AFRC") or std.mem.eql(u8, magic, "AFRM")) 2 else 1) or get(u16, bytes, 6) != 0 or get(u32, bytes, 8) != bytes.len)
        return error.InvalidPostingRows;
    if (get(u32, bytes, 12) != checksum(bytes)) return error.PostingRowChecksumMismatch;
}

const test_identity: Identity = .{ .incarnation = 71, .leaf = 2, .origin = 11 };

fn testChunk(alloc: Allocator, serial: u64, revision: u64, ids: []const u64, metric: @import("antfly_vector").vector.DistanceMetric) !*Chunk {
    const vectors = try alloc.alloc(f32, ids.len * 4);
    defer alloc.free(vectors);
    for (vectors, 0..) |*v, i| v.* = @as(f32, @floatFromInt((i * 17 + serial * 3) % 23)) / 23;
    var quantizer = try @import("antfly_vector").quantizer.RaBitQuantizer.init(alloc, 4, 42, metric);
    defer quantizer.deinit();
    var set = try quantizer.quantize(&.{ 0.5, 0.25, 0.1, 0.3 }, vectors, ids.len);
    defer set.deinit(alloc);
    return Chunk.build(alloc, test_identity, serial, revision, ids, &set);
}

fn expectIds(view: *const Snapshot, expected: []const u64) !void {
    try std.testing.expectEqual(expected.len, view.row_count);
    var offset: usize = 0;
    for (view.runs) |run| {
        const scan = run.scan();
        try std.testing.expectEqualSlices(u64, expected[offset..][0..run.len], scan.member_ids);
        try std.testing.expectEqual(@as(usize, run.len), scan.quantized.getCount());
        // A scan must actually borrow code storage, not materialize a copy.
        try std.testing.expectEqual(run.chunk.view.codes[run.start * run.chunk.view.width ..].ptr, scan.quantized.rabit.codes.data.ptr);
        offset += run.len;
    }
}

test "posting row allocator and recovery debt are authenticated without loading chunks" {
    var encoded = (Allocation{ .incarnation = 7, .serial = 42 }).encode();
    try std.testing.expectEqualDeep(Allocation{ .incarnation = 7, .serial = 42 }, try Allocation.decode(&encoded));
    encoded[16] ^= 1;
    try std.testing.expectError(error.InvalidPostingRows, Allocation.decode(&encoded));
    try std.testing.expectError(error.InvalidPostingRows, Allocation.decode(&(Allocation{ .incarnation = 0, .serial = 1 }).encode()));
    try std.testing.expectError(error.InvalidPostingRows, Allocation.decode(&(Allocation{ .incarnation = 1, .serial = @as(u64, 1) << 63 }).encode()));
    const a = std.testing.allocator;
    const chunk = try testChunk(a, 1, 1, &.{ 1, 2, 3, 4 }, .cosine);
    defer chunk.release();
    var base = try Snapshot.init(a, test_identity, 1, 10, &.{.{ .chunk = chunk, .start = 0, .len = 4 }});
    defer base.deinit();
    const clean = try base.encode();
    defer a.free(clean);
    try std.testing.expect(!try manifestHasDebt(clean));
    var changed = try base.mutate(1, 2, 11, &.{.{ .chunk = 1, .row = 3 }}, null);
    defer changed.deinit();
    const dirty = try changed.encode();
    defer a.free(dirty);
    try std.testing.expect(try manifestHasDebt(dirty));
    var repack = try Repack.prepare(&changed, 2);
    defer repack.deinit();
    var compact = try repack.rebase(&changed);
    defer compact.deinit();
    const compact_bytes = try compact.encode();
    defer a.free(compact_bytes);
    try std.testing.expect(!try manifestHasDebt(compact_bytes));
}

test "posting row deltas preserve revision identity and old query leases" {
    const a = std.testing.allocator;
    for ([_]@import("antfly_vector").vector.DistanceMetric{ .l2_squared, .cosine, .inner_product }) |metric| {
        const chunk = try testChunk(a, 1, 1, &.{ 10, 20, 30, 40 }, metric);
        defer chunk.release();
        var base = try Snapshot.init(a, test_identity, 1, 100, &.{.{ .chunk = chunk, .start = 0, .len = 4 }});
        defer base.deinit();
        const append = try testChunk(a, 2, 2, &.{ 20, 50 }, metric);
        defer append.release();
        var changed = try base.mutate(1, 2, 101, &.{.{ .chunk = 1, .row = 1 }}, append);
        defer changed.deinit();
        try expectIds(&base, &.{ 10, 20, 30, 40 });
        try expectIds(&changed, &.{ 10, 30, 40, 20, 50 });
        try std.testing.expectError(error.PostingRowsSuperseded, changed.mutate(1, 3, 102, &.{}, null));
        try std.testing.expectError(error.StalePostingRow, changed.mutate(2, 3, 102, &.{.{ .chunk = 1, .row = 1 }}, null));
        try std.testing.expectError(error.StalePostingRow, base.mutate(1, 2, 101, &.{ .{ .chunk = 1, .row = 1 }, .{ .chunk = 1, .row = 1 } }, null));
        try std.testing.expectError(error.StalePostingRow, base.mutate(1, 2, 101, &.{ .{ .chunk = 1, .row = 2 }, .{ .chunk = 1, .row = 1 } }, null));
        try std.testing.expectError(error.PostingRowSequenceRegression, changed.mutate(2, 3, 100, &.{}, null));
        try std.testing.expectError(error.DuplicatePostingVector, base.mutate(1, 2, 101, &.{}, append));
        var deleted_new = try changed.mutate(2, 3, 102, &.{.{ .chunk = 2, .row = 0 }}, null);
        defer deleted_new.deinit();
        try expectIds(&deleted_new, &.{ 10, 30, 40, 50 });
    }
}

test "posting row durable redirects preserve repacking across newer manifests" {
    const a = std.testing.allocator;
    const chunk = try testChunk(a, 1, 1, &.{ 10, 20, 30, 40, 50 }, .cosine);
    defer chunk.release();
    var original = try Snapshot.init(a, test_identity, 1, 100, &.{.{ .chunk = chunk, .start = 0, .len = 5 }});
    defer original.deinit();
    var captured = try original.mutate(1, 2, 101, &.{.{ .chunk = 1, .row = 1 }}, null);
    defer captured.deinit();
    var repack = try Repack.prepare(&captured, 1000);
    defer repack.deinit();
    const redirect_bytes = try repack.encodeRedirect(chunk);
    defer a.free(redirect_bytes);
    const extra = try testChunk(a, 2, 3, &.{20}, .cosine);
    defer extra.release();
    var tail = try captured.mutate(2, 3, 102, &.{.{ .chunk = 1, .row = 3 }}, extra);
    defer tail.deinit();
    const encoded = try tail.encode();
    defer a.free(encoded);
    const Resolver = struct {
        redirect_bytes: []const u8,
        redirect_serial: u64 = 1,
        compact: ?*Chunk,
        extra: *Chunk,
        pub fn redirect(self: @This(), serial: u64) !?Redirect {
            return if (serial == self.redirect_serial) try Redirect.init(self.redirect_bytes) else null;
        }
        pub fn get(self: @This(), serial: u64) !?*Chunk {
            if (serial == self.extra.serial) return self.extra;
            if (self.compact) |value| if (serial == value.serial) return value;
            // The original scoring payload is deliberately unavailable.
            return null;
        }
    };
    const resolver = Resolver{ .redirect_bytes = redirect_bytes, .compact = repack.compact, .extra = extra };
    var reopened = try Snapshot.decode(a, encoded, resolver);
    defer reopened.deinit();
    try expectIds(&reopened, &.{ 10, 30, 50, 20 });
    try std.testing.expectEqual(@as(u64, 102), reopened.coverage);
    for (reopened.runs) |run| try std.testing.expect(run.chunk.serial != 1);
    // No resurrection of an already deleted revision, even with a valid CRC.
    const old_manifest = try original.encode();
    defer a.free(old_manifest);
    try std.testing.expectError(error.StalePostingRow, Snapshot.decode(a, old_manifest, resolver));
    var missing = resolver;
    missing.compact = null;
    try std.testing.expectError(error.MissingPostingChunk, Snapshot.decode(a, encoded, missing));
    const damaged = try a.dupe(u8, redirect_bytes);
    defer a.free(damaged);
    damaged[64] ^= 1;
    try std.testing.expectError(error.PostingRowChecksumMismatch, Redirect.init(damaged));
    seal(damaged);
    var wrong = resolver;
    wrong.redirect_bytes = damaged;
    try std.testing.expectError(error.PostingChunkIdentityConflict, Snapshot.decode(a, encoded, wrong));

    // Removing whole old chunks may make the source tail look physically
    // clean. After translation it still retains a subset of a compact base.
    var second = try Repack.prepare(&tail, (@as(u64, 1) << 63) | 2000);
    defer second.deinit();
    const extra_redirect = try second.encodeRedirect(extra);
    defer a.free(extra_redirect);
    var only_extra = try tail.mutate(3, 4, 103, &.{ .{ .chunk = 1, .row = 0 }, .{ .chunk = 1, .row = 2 }, .{ .chunk = 1, .row = 4 } }, null);
    defer only_extra.deinit();
    try std.testing.expectEqual(@as(u64, 0), only_extra.first_debt_revision);
    const clean_bytes = try only_extra.encode();
    defer a.free(clean_bytes);
    var translated = try Snapshot.decode(a, clean_bytes, Resolver{ .redirect_serial = 2, .redirect_bytes = extra_redirect, .compact = second.compact, .extra = extra });
    defer translated.deinit();
    try expectIds(&translated, &.{20});
    try std.testing.expect(translated.first_debt_revision != 0);
    var compact = try second.rebase(&tail);
    defer compact.deinit();
    const compact_bytes = try compact.encode();
    defer a.free(compact_bytes);
    const pending = (try ManifestStats.decode(clean_bytes)).withLayoutDebt(clean_bytes, try ManifestStats.compactLayout(compact_bytes));
    try std.testing.expect(pending.first_debt_revision != 0);
    try std.testing.expect(pending.physical_rows >= translated.physical_rows);
}

test "posting row shared origins remove dimension-scaled chunk duplication" {
    const a = std.testing.allocator;
    var quantizer = try @import("antfly_vector").quantizer.RaBitQuantizer.init(a, 768, 42, .cosine);
    defer quantizer.deinit();
    const center = [_]f32{0.125} ** 768;
    const vector = [_]f32{0.25} ** 768;
    var set = try quantizer.quantize(&center, &vector, 1);
    defer set.deinit(a);
    const origin = try Origin.build(a, test_identity, &set);
    defer origin.release();
    var chunks: [64]*Chunk = undefined;
    var initialized: usize = 0;
    defer for (chunks[0..initialized]) |chunk| chunk.release();
    var new_bytes: usize = origin.bytes.len;
    for (&chunks, 0..) |*chunk, i| {
        chunk.* = try Chunk.buildWithOrigin(a, origin, i + 1, 1, &.{i + 1}, &set);
        initialized += 1;
        try std.testing.expect(chunk.*.origin == origin);
        try std.testing.expect(chunk.*.view.centroid.ptr == origin.centroid.ptr);
        new_bytes += chunk.*.bytes.len;
    }
    // Reproduce the prior full-AFQD payload only as a sizing oracle.
    var previous = try directory.Writer.init(a, 768, @intCast(@intFromEnum(set.metric)));
    defer previous.deinit();
    const ids = [_]u64{1};
    try previous.appendWithMemberBytes(test_identity.leaf, &set, std.mem.sliceAsBytes(&ids));
    const previous_bytes = try previous.build();
    defer a.free(previous_bytes);
    try std.testing.expect(new_bytes * 8 < (header_len + previous_bytes.len) * chunks.len);
    var runs: [64]Run = undefined;
    for (&runs, chunks) |*run, chunk| run.* = .{ .chunk = chunk, .start = 0, .len = 1 };
    var snapshot = try Snapshot.init(a, test_identity, 1, 100, &runs);
    defer snapshot.deinit();
    try std.testing.expectEqual(new_bytes, snapshot.physical_bytes);
    var repack = try Repack.prepare(&snapshot, 1000);
    defer repack.deinit();
    try std.testing.expect(repack.compact.?.origin == origin);

    var different_set = set;
    var different_center = center;
    different_center[0] += 0.125;
    different_set.centroid = &different_center;
    const wrong = try Origin.build(a, test_identity, &different_set);
    defer wrong.release();
    try std.testing.expectError(error.PostingScoringOriginMismatch, Chunk.open(a, chunks[0].bytes, .{ .owned = .@"8" }, wrong));
    const damaged = try a.dupe(u8, origin.bytes);
    defer a.free(damaged);
    damaged[damaged.len - 1] ^= 1;
    try std.testing.expectError(error.PostingRowChecksumMismatch, Origin.decode(a, damaged));
}

test "posting row manifests recover independently and reject broken dependencies" {
    const a = std.testing.allocator;
    const chunk = try testChunk(a, 1, 1, &.{ 10, 20, 30, 40 }, .cosine);
    defer chunk.release();
    var base = try Snapshot.init(a, test_identity, 1, 100, &.{.{ .chunk = chunk, .start = 0, .len = 4 }});
    defer base.deinit();
    var changed = try base.mutate(1, 2, 101, &.{.{ .chunk = 1, .row = 1 }}, null);
    defer changed.deinit();
    const encoded = try changed.encode();
    defer a.free(encoded);
    // Reopen separate bytes/readers; no decoded aggregate or predecessor view.
    const reopened_bytes = try a.dupe(u8, chunk.bytes);
    const reopened_origin = try Origin.decode(a, chunk.origin.bytes);
    defer reopened_origin.release();
    const reopened_chunk = Chunk.open(a, reopened_bytes, .{ .owned = .@"1" }, reopened_origin) catch |err| {
        a.free(reopened_bytes);
        return err;
    };
    defer reopened_chunk.release();
    const Resolver = struct {
        chunk: ?*Chunk,
        pub fn get(self: @This(), serial: u64) !?*Chunk {
            const found = self.chunk orelse return null;
            return if (found.serial == serial) found else null;
        }
    };
    var reopened = try Snapshot.decode(a, encoded, Resolver{ .chunk = reopened_chunk });
    defer reopened.deinit();
    try expectIds(&reopened, &.{ 10, 30, 40 });
    try std.testing.expectEqual(@as(u64, 101), reopened.coverage);
    const roundtrip = try reopened.encode();
    defer a.free(roundtrip);
    try std.testing.expectEqualSlices(u8, encoded, roundtrip);
    try std.testing.expectError(error.MissingPostingChunk, Snapshot.decode(a, encoded, Resolver{ .chunk = null }));
    for (0..encoded.len) |len| try std.testing.expectError(error.InvalidPostingRows, Snapshot.decode(a, encoded[0..len], Resolver{ .chunk = reopened_chunk }));
    const damaged = try a.dupe(u8, encoded);
    defer a.free(damaged);
    damaged[48] ^= 1;
    try std.testing.expectError(error.PostingRowChecksumMismatch, Snapshot.decode(a, damaged, Resolver{ .chunk = reopened_chunk }));
    @memcpy(damaged, encoded);
    put(u32, damaged, manifest_header_len + 24, chunk.checksum ^ 1);
    seal(damaged);
    try std.testing.expectError(error.PostingChunkIdentityConflict, Snapshot.decode(a, damaged, Resolver{ .chunk = reopened_chunk }));
    @memcpy(damaged, encoded);
    put(u32, damaged, manifest_header_len + 32, std.math.maxInt(u32));
    seal(damaged);
    try std.testing.expectError(error.InvalidPostingRows, Snapshot.decode(a, damaged, Resolver{ .chunk = reopened_chunk }));
    // Chunk corruption is checked before creating a lease or exposing slices.
    const bad_chunk = try a.dupe(u8, chunk.bytes);
    defer a.free(bad_chunk);
    bad_chunk[bad_chunk.len - 1] ^= 1;
    try std.testing.expectError(error.PostingRowChecksumMismatch, Chunk.open(a, bad_chunk, .{ .owned = .@"1" }, chunk.origin));
}

test "posting row repack preserves newer mutations without resurrecting rows" {
    const a = std.testing.allocator;
    const chunk = try testChunk(a, 1, 1, &.{ 10, 20, 30, 40 }, .cosine);
    defer chunk.release();
    var base = try Snapshot.init(a, test_identity, 1, 100, &.{.{ .chunk = chunk, .start = 0, .len = 4 }});
    defer base.deinit();
    var dirty = try base.mutate(1, 2, 101, &.{.{ .chunk = 1, .row = 1 }}, null);
    defer dirty.deinit();
    var repack = try Repack.prepare(&dirty, 3);
    defer repack.deinit();
    const append = try testChunk(a, 2, 3, &.{ 20, 50 }, .cosine);
    defer append.release();
    var newer = try dirty.mutate(2, 3, 102, &.{.{ .chunk = 1, .row = 2 }}, append);
    defer newer.deinit();
    var rebased = try repack.rebase(&newer);
    defer rebased.deinit();
    try expectIds(&rebased, &.{ 10, 40, 20, 50 });
    try expectIds(&dirty, &.{ 10, 30, 40 });
    try expectIds(&base, &.{ 10, 20, 30, 40 });
    try std.testing.expectEqual(@as(u64, 102), rebased.coverage);
    try std.testing.expectEqual(@as(u64, 3), rebased.revision);
    try std.testing.expectEqual(@as(u64, 3), rebased.runs[0].chunk.serial);
    try std.testing.expectEqual(@as(u64, 2), rebased.runs[rebased.runs.len - 1].chunk.serial);
    try std.testing.expectError(error.PostingRowsSuperseded, repack.rebase(&base));
    // Another compactor changed physical identity under the same logical
    // revision: matching coverage/revision alone must not validate it.
    var other = try Repack.prepare(&dirty, 4);
    defer other.deinit();
    var other_view = try other.rebase(&dirty);
    defer other_view.deinit();
    try std.testing.expectError(error.PostingRowsSuperseded, repack.rebase(&other_view));
}

test "posting row debt is bounded by density bytes fanout and age" {
    const a = std.testing.allocator;
    const chunk = try testChunk(a, 1, 1, &.{ 10, 20, 30, 40 }, .l2_squared);
    defer chunk.release();
    var base = try Snapshot.init(a, test_identity, 1, 100, &.{.{ .chunk = chunk, .start = 0, .len = 4 }});
    defer base.deinit();
    try std.testing.expect(!Policy.needsRepack(.{}, &base, std.math.maxInt(u64)));
    var dirty = try base.mutate(1, 2, 101, &.{.{ .chunk = 1, .row = 1 }}, null);
    defer dirty.deinit();
    try std.testing.expect(Policy.needsRepack(.{}, &dirty, 0));
    try std.testing.expect(!Policy.needsRepack(.{ .tombstone_percent = 50 }, &dirty, 0));
    try std.testing.expect(Policy.needsRepack(.{ .tombstone_percent = 50 }, &dirty, 30 * std.time.ns_per_s));
    try std.testing.expect(Policy.needsRepack(.{ .tombstone_percent = 50, .soft_runs = 1 }, &dirty, 0));
    // A compact chunk cannot become smaller through another identical repack.
    try std.testing.expect(!Policy.needsRepack(.{ .soft_bytes = 1 }, &base, 0));
    try std.testing.expect(Policy.needsRepack(.{ .soft_bytes = 1 }, &dirty, 0));
    const too_many = try a.alloc(Run, Policy.hard_runs + 1);
    defer a.free(too_many);
    try std.testing.expectError(error.PostingRowBackpressure, Snapshot.init(a, test_identity, 1, 100, too_many));
}

test "posting row preparation and recovery are allocation failure safe" {
    const Attempt = struct {
        fn run(a: Allocator) !void {
            const chunk = try testChunk(a, 1, 1, &.{ 10, 20, 30, 40 }, .cosine);
            defer chunk.release();
            var base = try Snapshot.init(a, test_identity, 1, 100, &.{.{ .chunk = chunk, .start = 0, .len = 4 }});
            defer base.deinit();
            var dirty = try base.mutate(1, 2, 101, &.{.{ .chunk = 1, .row = 1 }}, null);
            defer dirty.deinit();
            var repack = try Repack.prepare(&dirty, 2);
            defer repack.deinit();
            const redirect_bytes = try repack.encodeRedirect(chunk);
            defer a.free(redirect_bytes);
            var rebased = try repack.rebase(&dirty);
            defer rebased.deinit();
            const encoded = try rebased.encode();
            defer a.free(encoded);
            const Resolver = struct {
                chunk: *Chunk,
                redirect_bytes: []const u8,
                pub fn redirect(self: @This(), serial: u64) !?Redirect {
                    return if (serial == 1) try Redirect.init(self.redirect_bytes) else null;
                }
                pub fn get(self: @This(), _: u64) !?*Chunk {
                    return self.chunk;
                }
            };
            const resolver = Resolver{ .chunk = repack.compact.?, .redirect_bytes = redirect_bytes };
            var restored = try Snapshot.decode(a, encoded, resolver);
            defer restored.deinit();
            try expectIds(&restored, &.{ 10, 30, 40 });
            const old_encoded = try dirty.encode();
            defer a.free(old_encoded);
            var old_restored = try Snapshot.decode(a, old_encoded, resolver);
            defer old_restored.deinit();
            try expectIds(&old_restored, &.{ 10, 30, 40 });
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Attempt.run, .{});
}

test "posting row fused scoring matches repacked scores bounds order and cancellation" {
    const a = std.testing.allocator;
    const vector = @import("antfly_vector");
    const Output = struct {
        ids: [8]u64 = undefined,
        distances: [8]f32 = undefined,
        bounds: [8]f32 = undefined,
        count: usize = 0,
        cancelled: bool = false,
        cancel_after: usize = std.math.maxInt(usize),
        pub fn write(self: *@This(), id: u64, distance: f32, bound: f32) void {
            self.ids[self.count] = id;
            self.distances[self.count] = distance;
            self.bounds[self.count] = bound;
            self.count += 1;
            if (self.count >= self.cancel_after) self.cancelled = true;
        }
        fn isCancelled(ptr: *const anyopaque) bool {
            return @as(*const @This(), @ptrCast(@alignCast(ptr))).cancelled;
        }
    };
    for ([_]vector.vector.DistanceMetric{ .l2_squared, .cosine, .inner_product }) |metric| {
        const chunk = try testChunk(a, 1, 1, &.{ 10, 20, 30, 40, 50, 60 }, metric);
        defer chunk.release();
        var base = try Snapshot.init(a, test_identity, 1, 100, &.{.{ .chunk = chunk, .start = 0, .len = 6 }});
        defer base.deinit();
        const append = try testChunk(a, 2, 2, &.{ 20, 70 }, metric);
        defer append.release();
        var dirty = try base.mutate(1, 2, 101, &.{ .{ .chunk = 1, .row = 1 }, .{ .chunk = 1, .row = 3 } }, append);
        defer dirty.deinit();
        var repack = try Repack.prepare(&dirty, 3);
        defer repack.deinit();
        var compact = try repack.rebase(&dirty);
        defer compact.deinit();
        var quantizer = try vector.quantizer.RaBitQuantizer.init(a, 4, 42, metric);
        defer quantizer.deinit();
        var scratch = try vector.quantizer.RaBitQuantizer.EstimateScratch.init(a, 4);
        defer scratch.deinit(a);
        for ([_][4]f32{ .{ 0.1, 0.9, 0.3, 0.4 }, .{ 0.5, 0.25, 0.1, 0.3 }, .{ 0, 0, 0, 0 } }) |query| {
            var before = Output{};
            var after = Output{};
            const prepare_epoch = scratch.prepare_epoch;
            try dirty.scoreTo(&quantizer, &query, &scratch, null, &before);
            try std.testing.expectEqual(prepare_epoch + 1, scratch.prepare_epoch);
            try compact.scoreTo(&quantizer, &query, &scratch, null, &after);
            try std.testing.expectEqual(prepare_epoch + 2, scratch.prepare_epoch);
            try std.testing.expectEqual(@as(usize, 6), before.count);
            try std.testing.expectEqualSlices(u64, before.ids[0..6], after.ids[0..6]);
            try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(before.distances[0..6]), std.mem.sliceAsBytes(after.distances[0..6]));
            try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(before.bounds[0..6]), std.mem.sliceAsBytes(after.bounds[0..6]));
            var cancelled = Output{ .cancelled = true };
            const token = vector.quantizer.CancellationToken{ .ptr = &cancelled, .is_cancelled_fn = Output.isCancelled };
            try std.testing.expectError(error.Canceled, dirty.scoreTo(&quantizer, &query, &scratch, token, &cancelled));
            try std.testing.expectEqual(@as(usize, 0), cancelled.count);
            cancelled.cancelled = false;
            cancelled.cancel_after = 1;
            try std.testing.expectError(error.Canceled, dirty.scoreTo(&quantizer, &query, &scratch, token, &cancelled));
            try std.testing.expect(cancelled.count < dirty.row_count);
        }
    }
}

test "posting row chunk leases outlive publication and release exactly once" {
    const a = std.testing.allocator;
    const owned = try testChunk(a, 1, 1, &.{ 10, 20, 30, 40 }, .cosine);
    defer owned.release();
    const Lease = struct {
        released: usize = 0,
        fn release(ptr: *anyopaque) void {
            @as(*@This(), @ptrCast(@alignCast(ptr))).released += 1;
        }
    };
    var lease = Lease{};
    const borrowed = try Chunk.open(a, owned.bytes, .{ .leased = .{ .ptr = &lease, .release = Lease.release } }, owned.origin);
    var query = try Snapshot.init(a, test_identity, 1, 100, &.{.{ .chunk = borrowed, .start = 0, .len = 4 }});
    borrowed.release();
    {
        defer query.deinit();
        var changed = try query.mutate(1, 2, 101, &.{ .{ .chunk = 1, .row = 0 }, .{ .chunk = 1, .row = 1 }, .{ .chunk = 1, .row = 2 }, .{ .chunk = 1, .row = 3 } }, null);
        defer changed.deinit();
        try std.testing.expectEqual(@as(usize, 0), changed.runs.len);
        try std.testing.expectEqual(@as(u64, 0), changed.physical_bytes);
        try std.testing.expectEqual(@as(usize, 0), lease.released);
        try expectIds(&query, &.{ 10, 20, 30, 40 });
    }
    try std.testing.expectEqual(@as(usize, 1), lease.released);
}

test "posting row manifest visibility follows the enclosing WAL commit" {
    const a = std.testing.allocator;
    const wal = @import("posting_wal.zig");
    const chunk = try testChunk(a, 1, 1, &.{ 10, 20 }, .cosine);
    defer chunk.release();
    var base = try Snapshot.init(a, test_identity, 1, 100, &.{.{ .chunk = chunk, .start = 0, .len = 2 }});
    defer base.deinit();
    const before = try base.encode();
    defer a.free(before);
    var changed = try base.mutate(1, 2, 101, &.{.{ .chunk = 1, .row = 1 }}, null);
    defer changed.deinit();
    const after = try changed.encode();
    defer a.free(after);
    var writer = wal.Writer.init(a);
    defer writer.deinit();
    // Test the opaque value's transaction boundary, not HBC format activation.
    try writer.append(.quantized_checkpoint, 1, test_identity.leaf, 100, before);
    try writer.commit(1, 100);
    const committed = writer.bytes().len;
    try writer.append(.quantized_checkpoint, 2, test_identity.leaf, 101, after);
    try writer.commit(2, 101);
    for (committed..writer.bytes().len) |len| {
        var replay = try wal.Replay.parse(a, writer.bytes()[0..len]);
        defer replay.deinit();
        try std.testing.expectEqual(@as(u64, 100), replay.covered_source_sequence);
        try std.testing.expectEqualSlices(u8, before, replay.latest(test_identity.leaf, .quantized_checkpoint).?.payload);
    }
    var replay = try wal.Replay.parse(a, writer.bytes());
    defer replay.deinit();
    try std.testing.expectEqual(@as(u64, 101), replay.covered_source_sequence);
    try std.testing.expectEqualSlices(u8, after, replay.latest(test_identity.leaf, .quantized_checkpoint).?.payload);
}

test "posting row repack worker permits mutations while the old query stays leased" {
    const a = std.testing.allocator;
    // The I/O runtime outlives all chunk/query/worker cleanup.
    var runtime_io = std.Io.Threaded.init(a, .{});
    defer runtime_io.deinit();
    const io = runtime_io.io();
    const chunk = try testChunk(a, 1, 1, &.{ 10, 20, 30, 40 }, .cosine);
    defer chunk.release();
    var query = try Snapshot.init(a, test_identity, 1, 100, &.{.{ .chunk = chunk, .start = 0, .len = 4 }});
    defer query.deinit();
    const Worker = struct {
        captured: *const Snapshot,
        ready: std.atomic.Value(bool) = .init(false),
        proceed: std.atomic.Value(bool) = .init(false),
        current: ?*const Snapshot = null,
        result: ?Snapshot = null,
        failure: ?anyerror = null,

        fn run(self: *@This(), worker_io: std.Io) std.Io.Cancelable!void {
            self.work(worker_io) catch |err| {
                self.failure = err;
            };
        }

        fn work(self: *@This(), worker_io: std.Io) !void {
            var prepared = try Repack.prepare(self.captured, 3);
            defer prepared.deinit();
            self.ready.store(true, .release);
            const deadline = std.Io.Clock.awake.now(worker_io).nanoseconds + 5 * std.time.ns_per_s;
            while (!self.proceed.load(.acquire)) {
                if (std.Io.Clock.awake.now(worker_io).nanoseconds >= deadline) return error.TestUnexpectedResult;
                try worker_io.sleep(.fromMilliseconds(1), .awake);
            }
            self.result = try prepared.rebase(self.current.?);
        }
    };
    var worker = Worker{ .captured = &query };
    defer if (worker.result) |*result| result.deinit();
    // current is declared before group so worker cancellation/join always
    // precedes the destruction of the snapshot shared through resume.
    var current: ?Snapshot = null;
    defer if (current) |*view| view.deinit();
    var group = std.Io.Group.init;
    defer group.cancel(io);
    try group.concurrent(io, Worker.run, .{ &worker, io });
    const deadline = std.Io.Clock.awake.now(io).nanoseconds + 5 * std.time.ns_per_s;
    while (!worker.ready.load(.acquire)) {
        if (std.Io.Clock.awake.now(io).nanoseconds >= deadline) return error.TestUnexpectedResult;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    const appended = try testChunk(a, 2, 2, &.{20}, .cosine);
    defer appended.release();
    current = try query.mutate(1, 2, 101, &.{.{ .chunk = 1, .row = 1 }}, appended);
    worker.current = &current.?;
    worker.proceed.store(true, .release);
    // Concurrent readers can still access every original immutable row.
    try expectIds(&query, &.{ 10, 20, 30, 40 });
    try group.await(io);
    if (worker.failure) |err| return err;
    try expectIds(&worker.result.?, &.{ 10, 30, 40, 20 });
    try expectIds(&current.?, &.{ 10, 30, 40, 20 });
}

test "posting row chunks reject origin aliasing and ambiguous physical identities" {
    const a = std.testing.allocator;
    const first = try testChunk(a, 1, 1, &.{ 10, 20 }, .cosine);
    defer first.release();
    const duplicate = try testChunk(a, 1, 1, &.{ 30, 40 }, .cosine);
    defer duplicate.release();
    try std.testing.expectError(error.PostingChunkIdentityConflict, Snapshot.init(a, test_identity, 1, 100, &.{
        .{ .chunk = first, .start = 0, .len = 2 }, .{ .chunk = duplicate, .start = 0, .len = 2 },
    }));
    var base = try Snapshot.init(a, test_identity, 1, 100, &.{.{ .chunk = first, .start = 0, .len = 2 }});
    defer base.deinit();
    var changed_origin = first.view.asProto();
    changed_origin.centroid = @constCast(&[_]f32{ 1, 2, 3, 4 });
    const wrong_origin = try Chunk.build(a, test_identity, 2, 2, &.{ 30, 40 }, &changed_origin);
    defer wrong_origin.release();
    try std.testing.expectError(error.PostingScoringOriginMismatch, base.mutate(1, 2, 101, &.{}, wrong_origin));
    const replacement = try Chunk.build(a, .{ .incarnation = 72, .leaf = 2, .origin = 11 }, 2, 2, &.{ 30, 40 }, &first.view.asProto());
    defer replacement.release();
    try std.testing.expectError(error.InvalidPostingRows, base.mutate(1, 2, 101, &.{}, replacement));
}

test "posting row shared query preparation microbenchmark" {
    if (@import("builtin").mode != .ReleaseFast) return error.SkipZigTest;
    // A same-binary kernel comparison, not an end-to-end performance result.
    const a = std.testing.allocator;
    const vector = @import("antfly_vector");
    const dims = 768;
    const rows = 1024;
    const iterations = 1000;
    const data = try a.alloc(f32, rows * dims);
    defer a.free(data);
    for (data, 0..) |*value, i| value.* = @as(f32, @floatFromInt(i % 31)) / 31;
    var ids: [rows]u64 = undefined;
    for (&ids, 0..) |*id, i| id.* = i + 1;
    const origin = [_]f32{0.1} ** dims;
    const query = [_]f32{0.3} ** dims;
    var quantizer = try vector.quantizer.RaBitQuantizer.init(a, dims, 42, .cosine);
    defer quantizer.deinit();
    var scratch = try vector.quantizer.RaBitQuantizer.EstimateScratch.init(a, dims);
    defer scratch.deinit(a);
    const Sink = struct {
        sum: f64 = 0,
        pub fn write(self: *@This(), id: u64, distance: f32, bound: f32) void {
            self.sum += @as(f64, @floatFromInt(id)) + distance + bound;
        }
    };
    const Output = struct {
        sink: *Sink,
        ids: []const u64,
        pub fn write(out: @This(), i: usize, distance: f32, bound: f32) void {
            out.sink.write(out.ids[i], distance, bound);
        }
    };
    for ([_]usize{ 1, 16, 64 }) |chunk_count| {
        var runs: [64]Run = undefined;
        var built: usize = 0;
        defer for (runs[0..built]) |run| run.chunk.release();
        const count = rows / chunk_count;
        for (0..chunk_count) |i| {
            var set = try quantizer.quantize(&origin, data[i * count * dims ..][0 .. count * dims], count);
            defer set.deinit(a);
            const chunk = try Chunk.build(a, test_identity, i + 1, 1, ids[i * count ..][0..count], &set);
            runs[i] = .{ .chunk = chunk, .start = 0, .len = @intCast(count) };
            built += 1;
        }
        var snapshot = try Snapshot.init(a, test_identity, 1, 100, runs[0..built]);
        defer snapshot.deinit();
        var expected_sum: ?f64 = null;
        for (0..4) |round| for (0..2) |arm| {
            const shared = (round + arm) % 2 == 1;
            var sink = Sink{};
            const started = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
            for (0..iterations) |_| {
                if (shared) {
                    try snapshot.scoreTo(&quantizer, &query, &scratch, null, &sink);
                } else for (snapshot.runs) |run| {
                    const set = run.chunk.view.asProto();
                    try quantizer.estimateDistancesInRangesTo(&set, &query, &scratch, null, &.{.{ .start = run.start, .end = run.start + run.len }}, Output{ .sink = &sink, .ids = run.chunk.view.member_ids });
                }
            }
            const elapsed = std.Io.Clock.awake.now(std.testing.io).nanoseconds - started;
            if (expected_sum) |expected| try std.testing.expectEqual(expected, sink.sum) else expected_sum = sink.sum;
            std.mem.doNotOptimizeAway(sink.sum);
            std.debug.print("posting-rows query-preparation chunks={} round={} shared={} ns_per_query={d:.3}\n", .{ chunk_count, round, shared, @as(f64, @floatFromInt(elapsed)) / iterations });
        };
    }
}

test "posting row representation microbenchmark" {
    if (@import("builtin").mode != .ReleaseFast) return error.SkipZigTest;
    // Synthetic repeated-leaf work, NOT a 50K/1M corpus, HTTP benchmark or
    // recall qualification. Isolates representation costs using the same leaf,
    // mutation, allocator and query, with reversed arm order in each pair.
    const a = std.testing.allocator;
    const vector = @import("antfly_vector");
    const dims = 768;
    const rows = 1024;
    const vectors = try a.alloc(f32, rows * dims);
    defer a.free(vectors);
    for (vectors, 0..) |*value, i| value.* = @as(f32, @floatFromInt((i * 13 + i / dims * 7) % 31)) / 31;
    var ids: [rows]u64 = undefined;
    for (&ids, 0..) |*id, i| id.* = i + 1;
    const origin = [_]f32{0.1} ** dims;
    const query = [_]f32{0.3} ** dims;
    var quantizer = try vector.quantizer.RaBitQuantizer.init(a, dims, 42, .cosine);
    defer quantizer.deinit();
    var set = try quantizer.quantize(&origin, vectors, rows);
    defer set.deinit(a);
    const chunk = try Chunk.build(a, test_identity, 1, 1, &ids, &set);
    defer chunk.release();
    var source = try Snapshot.init(a, test_identity, 1, 100, &.{.{ .chunk = chunk, .start = 0, .len = rows }});
    defer source.deinit();
    var deletes: [32]RowRef = undefined;
    var selected: [rows - 32]usize = undefined;
    var kept: usize = 0;
    for (0..rows) |row| {
        if (row % 32 == 15) {
            deletes[row / 32] = .{ .chunk = 1, .row = @intCast(row) };
        } else {
            selected[kept] = row;
            kept += 1;
        }
    }
    const borrowed: runtime.QuantizedSet = .{ .rabit = chunk.view.asProto() };
    var replacement_ids: [32]u64 = undefined;
    for (&replacement_ids, deletes) |*id, deleted| id.* = ids[deleted.row];
    for ([_]bool{ false, true }) |replace| for ([_]usize{ 50_000, 1_000_000 }) |work_rows| {
        const visits = (work_rows + rows - 1) / rows;
        for (0..4) |round| for (0..2) |arm| {
            const delta = (arm + round) % 2 == 1;
            var counting = std.testing.FailingAllocator.init(std.heap.smp_allocator, .{});
            const measured = counting.allocator();
            var source_with_allocator = source;
            source_with_allocator.alloc = measured; // borrowed; never deinit this copy
            var encoded_bytes: u64 = 0;
            const start = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
            for (0..visits) |_| {
                if (delta) {
                    var append: ?*Chunk = null;
                    defer if (append) |added| added.release();
                    if (replace) {
                        // Quantizer uses the measured allocator in both arms.
                        // Include quantization, encoding and verification of
                        // new rows; do not prebuild candidate append chunks.
                        var measured_quantizer = quantizer;
                        measured_quantizer.alloc = measured;
                        var added = try measured_quantizer.quantize(&origin, vectors[0 .. 32 * dims], 32);
                        defer added.deinit(measured);
                        append = try Chunk.build(measured, test_identity, 2, 2, &replacement_ids, &added);
                        encoded_bytes += append.?.bytes.len;
                    }
                    var changed = try source_with_allocator.mutate(1, 2, 101, &deletes, append);
                    defer changed.deinit();
                    const encoded = try changed.encode();
                    defer measured.free(encoded);
                    encoded_bytes += encoded.len;
                    std.mem.doNotOptimizeAway(encoded.ptr);
                } else {
                    // Optimistic current path: already has a borrowed decoded
                    // source, excludes upstream cache/storage/WAL patch costs.
                    var copied = try borrowed.selectRows(measured, &selected);
                    defer copied.deinit(measured);
                    if (replace) {
                        var measured_quantizer = quantizer;
                        measured_quantizer.alloc = measured;
                        try measured_quantizer.quantizeWithSet(&copied.rabit, vectors[0 .. 32 * dims], 32);
                    }
                    const encoded = try copied.rabit.encode(measured);
                    defer measured.free(encoded);
                    encoded_bytes += encoded.len;
                    std.mem.doNotOptimizeAway(encoded.ptr);
                }
            }
            const elapsed = std.Io.Clock.awake.now(std.testing.io).nanoseconds - start;
            try std.testing.expectEqual(counting.allocated_bytes, counting.freed_bytes);
            std.debug.print("posting-rows mutation replace={} work_rows={} leaf_visits={} round={} delta={} ns_per_leaf={d:.1} requested_bytes={} encoded_bytes={} allocations={}\n", .{
                replace, work_rows, visits, round, delta, @as(f64, @floatFromInt(elapsed)) / @as(f64, @floatFromInt(visits)), counting.allocated_bytes, encoded_bytes, counting.allocations,
            });
        };
    };
    var scratch = try vector.quantizer.RaBitQuantizer.EstimateScratch.init(a, dims);
    defer scratch.deinit(a);
    const Sink = struct {
        sum: f64 = 0,
        pub fn write(self: *@This(), _: u64, distance: f32, bound: f32) void {
            self.sum += distance + bound;
        }
    };
    for ([_]bool{ false, true }) |replace| {
        var append: ?*Chunk = null;
        defer if (append) |chunk_| chunk_.release();
        if (replace) {
            var added = try quantizer.quantize(&origin, vectors[0 .. 32 * dims], 32);
            defer added.deinit(a);
            append = try Chunk.build(a, test_identity, 3, 2, &replacement_ids, &added);
        }
        var dirty = try source.mutate(1, 2, 101, &deletes, append);
        defer dirty.deinit();
        var repack = try Repack.prepare(&dirty, 2);
        defer repack.deinit();
        var compact = try repack.rebase(&dirty);
        defer compact.deinit();
        for (0..4) |round| for (0..2) |arm| {
            const fragmented = (round + arm) % 2 == 1;
            const view = if (fragmented) &dirty else &compact;
            var sink = Sink{};
            const start = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
            for (0..1000) |_| try view.scoreTo(&quantizer, &query, &scratch, null, &sink);
            const elapsed = std.Io.Clock.awake.now(std.testing.io).nanoseconds - start;
            std.mem.doNotOptimizeAway(sink.sum);
            std.debug.print("posting-rows query replace={} round={} fragmented={} ns_per_live_row={d:.3} runs={} chunks={} retained_bytes={}\n", .{
                replace, round, fragmented, @as(f64, @floatFromInt(elapsed)) / @as(f64, @floatFromInt(1000 * view.row_count)), view.runs.len, view.chunk_count, view.physical_bytes,
            });
        };
    }
}
