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
const run_len = 40;
const max_encoded_bytes = 64 * 1024 * 1024;

pub fn isManifest(bytes: []const u8) bool {
    return bytes.len >= 4 and std.mem.eql(u8, bytes[0..4], "AFRM");
}

/// Recovery can schedule bounded deferred work without touching code pages.
/// A compact chunk is stamped with its manifest revision; a later deletion
/// advances only the manifest. Multiple runs also imply deferred packing work.
pub fn manifestHasDebt(bytes: []const u8) !bool {
    try validateFrame(bytes, "AFRM");
    const count = get(u64, bytes, 64);
    if (count > Policy.hard_runs or bytes.len != header_len + count * run_len or get(u64, bytes, 72) != 0)
        return error.InvalidPostingRows;
    return count > 1 or (count == 1 and (get(u64, bytes, 40) > get(u64, bytes, header_len + 8) or get(u32, bytes, header_len + 28) != 0));
}

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

/// One physical chunk; serials must never be reused within an incarnation.
/// The byte buffer can be owned heap memory or a retained mapping. Reader
/// verification state is private, while all scoring columns stay borrowed.
pub const Chunk = struct {
    alloc: Allocator,
    refs: std.atomic.Value(u32) = .init(1),
    identity: Identity,
    serial: u64,
    revision: u64,
    bytes: []const u8,
    checksum: u32,
    reader: directory.VerifiedReader,
    view: directory.View,
    backing: Backing,

    pub const Backing = union(enum) {
        /// Original allocation alignment (not the incidental pointer alignment).
        owned: std.mem.Alignment,
        leased: struct { ptr: *anyopaque, release: *const fn (*anyopaque) void },
    };

    /// On success takes ownership of backing, not on failure. A lease must
    /// already cover bytes and remain alive until the final chunk reference.
    pub fn open(alloc: Allocator, bytes: []const u8, backing: Backing) !*Chunk {
        try validateFrame(bytes, "AFRC");
        if (get(u64, bytes, 64) != bytes.len - header_len or get(u64, bytes, 72) != 0)
            return error.InvalidPostingRows;
        const identity = readIdentity(bytes);
        if (!identity.valid() or get(u64, bytes, 40) == 0 or get(u64, bytes, 48) == 0 or get(u64, bytes, 56) != 0)
            return error.InvalidPostingRows;
        var reader = try directory.VerifiedReader.init(alloc, bytes[header_len..]);
        errdefer reader.deinit();
        if (reader.reader.posting_count != 1) return error.InvalidPostingRows;
        const view = (try reader.get(identity.leaf)) orelse return error.InvalidPostingRows;
        if (view.metric > 2 or view.width != @import("antfly_vector").rabitq.codeWidth(view.centroid.len) or
            view.count == 0 or view.count > std.math.maxInt(u32) or view.member_ids.len != view.count or
            view.subgroup_plan != null or view.projections != null) return error.InvalidPostingRows;
        // A malformed chunk must not introduce ambiguous live revisions.
        var ids = std.AutoHashMapUnmanaged(u64, void).empty;
        defer ids.deinit(alloc);
        for (view.member_ids) |id| if ((try ids.getOrPut(alloc, id)).found_existing) return error.DuplicatePostingVector;
        const self = try alloc.create(Chunk);
        self.* = .{
            .alloc = alloc,
            .identity = identity,
            .serial = get(u64, bytes, 40),
            .revision = get(u64, bytes, 48),
            .bytes = bytes,
            .checksum = Crc32.hash(bytes),
            .reader = reader,
            .view = view,
            .backing = backing,
        };
        return self;
    }

    pub fn build(alloc: Allocator, identity: Identity, serial: u64, revision: u64, ids: []const u64, set: *const proto.RaBitQuantizedVectorSet) !*Chunk {
        if (!identity.valid() or serial == 0 or revision == 0 or ids.len != set.getCount()) return error.InvalidPostingRows;
        var writer = try directory.Writer.init(alloc, set.centroid.len, @intCast(@intFromEnum(set.metric)));
        defer writer.deinit();
        const members = try alloc.alloc(u8, try std.math.mul(usize, ids.len, 8));
        defer alloc.free(members);
        for (ids, 0..) |id, i| put(u64, members, i * 8, id);
        try writer.appendWithMemberBytes(identity.leaf, set, members);
        const payload = try writer.build();
        defer alloc.free(payload);
        const len = try std.math.add(usize, header_len, payload.len);
        if (len > max_encoded_bytes) return error.PostingRowBackpressure;
        // AFQD arrays need eight-byte alignment. The outer header preserves it.
        const bytes = try alloc.alignedAlloc(u8, .@"8", len);
        errdefer alloc.free(bytes);
        @memset(bytes, 0);
        writeHeader(bytes, "AFRC", identity);
        put(u64, bytes, 40, serial);
        put(u64, bytes, 48, revision);
        put(u64, bytes, 64, payload.len);
        @memcpy(bytes[header_len..], payload);
        seal(bytes);
        return open(alloc, bytes, .{ .owned = .@"8" });
    }

    pub fn retain(self: *Chunk) void {
        const before = self.refs.fetchAdd(1, .monotonic);
        std.debug.assert(before != 0 and before != std.math.maxInt(u32));
    }

    pub fn release(self: *Chunk) void {
        const before = self.refs.fetchSub(1, .acq_rel);
        std.debug.assert(before != 0);
        if (before != 1) return;
        self.reader.deinit();
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

    pub fn init(alloc: Allocator, identity: Identity, revision: u64, coverage: u64, runs: []const Run) !Snapshot {
        return initValidated(alloc, identity, revision, coverage, runs, true);
    }

    fn initValidated(alloc: Allocator, identity: Identity, revision: u64, coverage: u64, runs: []const Run, comptime validate_ids: bool) !Snapshot {
        if (!identity.valid() or revision == 0) return error.InvalidPostingRows;
        if (runs.len > Policy.hard_runs) return error.PostingRowBackpressure;
        var ids = std.AutoHashMapUnmanaged(u64, void).empty;
        defer ids.deinit(alloc);
        var chunks = std.AutoHashMapUnmanaged(u64, *Chunk).empty;
        defer chunks.deinit(alloc);
        var count: usize = 0;
        var bytes: u64 = 0;
        var physical_rows: u64 = 0;
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
                if (chunks.count() > Policy.hard_chunks or bytes > Policy.hard_bytes) return error.PostingRowBackpressure;
            }
            if (validate_ids) for (run.scan().member_ids) |id| {
                if ((try ids.getOrPut(alloc, id)).found_existing) return error.DuplicatePostingVector;
            };
            count = try std.math.add(usize, count, run.len);
        }
        if (chunks.count() > Policy.hard_chunks or bytes > Policy.hard_bytes) return error.PostingRowBackpressure;
        // The reference array is small; no candidate payload is copied.
        const owned = try alloc.dupe(Run, runs);
        for (owned) |run| run.chunk.retain();
        return .{ .alloc = alloc, .identity = identity, .revision = revision, .coverage = coverage, .runs = owned, .row_count = count, .physical_bytes = bytes, .physical_rows = physical_rows, .chunk_count = chunks.count() };
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
        return initValidated(self.alloc, self.identity, revision, coverage, runs.items, false);
    }

    /// Self-framed, checksummed manifest suitable for a committed posting-WAL
    /// value. No chunk payload is embedded or copied here, including at restart.
    pub fn encode(self: *const Snapshot) ![]u8 {
        const bytes = try self.alloc.alloc(u8, header_len + self.runs.len * run_len);
        @memset(bytes, 0);
        writeHeader(bytes, "AFRM", self.identity);
        put(u64, bytes, 40, self.revision);
        put(u64, bytes, 48, self.coverage);
        put(u64, bytes, 56, self.row_count);
        put(u64, bytes, 64, self.runs.len);
        for (self.runs, 0..) |run, i| {
            const off = header_len + i * run_len;
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
        try validateFrame(bytes, "AFRM");
        const count = get(u64, bytes, 64);
        if (count > Policy.hard_runs or bytes.len != header_len + count * run_len or get(u64, bytes, 72) != 0)
            return error.InvalidPostingRows;
        const runs = try alloc.alloc(Run, @intCast(count));
        defer alloc.free(runs);
        for (runs, 0..) |*run, i| {
            const off = header_len + i * run_len;
            const chunk = (try resolver.get(get(u64, bytes, off))) orelse return error.MissingPostingChunk;
            if (chunk.revision != get(u64, bytes, off + 8) or chunk.bytes.len != get(u64, bytes, off + 16) or
                chunk.checksum != get(u32, bytes, off + 24) or get(u32, bytes, off + 36) != 0)
                return error.PostingChunkIdentityConflict;
            run.* = .{ .chunk = chunk, .start = get(u32, bytes, off + 28), .len = get(u32, bytes, off + 32) };
        }
        var result = try init(alloc, readIdentity(bytes), get(u64, bytes, 40), get(u64, bytes, 48), runs);
        errdefer result.deinit();
        if (result.row_count != get(u64, bytes, 56)) return error.InvalidPostingRows;
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
        return .{ .base = pinned, .compact = try Chunk.build(a, base.identity, serial, base.revision, ids, &set) };
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
        return Snapshot.initValidated(current.alloc, current.identity, current.revision, current.coverage, runs.items, false);
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
        get(u16, bytes, 4) != 1 or get(u16, bytes, 6) != 0 or get(u32, bytes, 8) != bytes.len)
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
    const reopened_chunk = Chunk.open(a, reopened_bytes, .{ .owned = .@"1" }) catch |err| {
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
    put(u32, damaged, header_len + 24, chunk.checksum ^ 1);
    seal(damaged);
    try std.testing.expectError(error.PostingChunkIdentityConflict, Snapshot.decode(a, damaged, Resolver{ .chunk = reopened_chunk }));
    @memcpy(damaged, encoded);
    put(u32, damaged, header_len + 32, std.math.maxInt(u32));
    seal(damaged);
    try std.testing.expectError(error.InvalidPostingRows, Snapshot.decode(a, damaged, Resolver{ .chunk = reopened_chunk }));
    // Chunk corruption is checked before creating a lease or exposing slices.
    const bad_chunk = try a.dupe(u8, chunk.bytes);
    defer a.free(bad_chunk);
    bad_chunk[bad_chunk.len - 1] ^= 1;
    try std.testing.expectError(error.PostingRowChecksumMismatch, Chunk.open(a, bad_chunk, .{ .owned = .@"1" }));
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
            var rebased = try repack.rebase(&dirty);
            defer rebased.deinit();
            const encoded = try rebased.encode();
            defer a.free(encoded);
            const Resolver = struct {
                chunk: *Chunk,
                pub fn get(self: @This(), _: u64) !?*Chunk {
                    return self.chunk;
                }
            };
            var restored = try Snapshot.decode(a, encoded, Resolver{ .chunk = repack.compact.? });
            defer restored.deinit();
            try expectIds(&restored, &.{ 10, 30, 40 });
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
    const borrowed = try Chunk.open(a, owned.bytes, .{ .leased = .{ .ptr = &lease, .release = Lease.release } });
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
