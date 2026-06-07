// Copyright 2026 Antfly, Inc.
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

//! SPFresh-style posting-list shape (experimental, standalone).
//!
//! This is a self-contained prototype of the target vector-index shape described
//! in SPFRESH.md, built so the storage shape can be benchmarked in isolation
//! against the current eager-HBC behavior. It deliberately holds the payload
//! format constant (raw f32 vectors) so the experiment measures the *shape*
//! mechanics, not quantization:
//!
//!   - posting contents:
//!       - an immutable base blob (sorted vector_id + raw vector)
//!       - an append-only delta/tail (insert/replace and tombstone records)
//!       - a background fold that rewrites base from base+deltas
//!   - centroid directory: posting_id -> centroid / count / generation / dirty,
//!     refreshed only by fold/bounded repair (lazy)
//!   - assignment map: vector_id -> posting_id (+ which posting currently owns it)
//!   - routing/query: route to candidate postings by centroid, then overlay
//!     base + deltas at query time so contents are always correct even while
//!     centroids/folds lag behind.
//!
//! IO is accounted as logical block bytes moved (writes/appends/reads), which is
//! the right granularity for a write-amplification comparison and is free of
//! filesystem timing noise. Memory is the caller's RSS plus `residentBytes()`;
//! CPU is wall-ns plus distance-op counts; QPS/recall come from the query path.

const std = @import("std");
const av = @import("antfly_vector");
const vec = av.vector;

const Allocator = std.mem.Allocator;
const ArrayU8 = std.ArrayListUnmanaged(u8);

pub const VectorId = u64;
pub const PostingId = u32;

pub const op_put: u8 = 0;
pub const op_del: u8 = 1;

pub const IoCounters = struct {
    base_writes: u64 = 0,
    base_write_bytes: u64 = 0,
    delta_appends: u64 = 0,
    delta_append_bytes: u64 = 0,
    reads: u64 = 0,
    read_bytes: u64 = 0,
    folds: u64 = 0,
    fold_input_bytes: u64 = 0,

    pub fn sub(a: IoCounters, b: IoCounters) IoCounters {
        return .{
            .base_writes = a.base_writes - b.base_writes,
            .base_write_bytes = a.base_write_bytes - b.base_write_bytes,
            .delta_appends = a.delta_appends - b.delta_appends,
            .delta_append_bytes = a.delta_append_bytes - b.delta_append_bytes,
            .reads = a.reads - b.reads,
            .read_bytes = a.read_bytes - b.read_bytes,
            .folds = a.folds - b.folds,
            .fold_input_bytes = a.fold_input_bytes - b.fold_input_bytes,
        };
    }
};

/// In-memory blob store with faithful logical-IO accounting. One base blob and
/// one append-only delta blob per posting. Bytes passed to write/append/read are
/// what a real block device would move, so the counters are the disk-IO metric.
pub const BlobStore = struct {
    alloc: Allocator,
    bases: std.AutoHashMapUnmanaged(PostingId, ArrayU8) = .empty,
    deltas: std.AutoHashMapUnmanaged(PostingId, ArrayU8) = .empty,
    counters: IoCounters = .{},

    pub fn init(alloc: Allocator) BlobStore {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *BlobStore) void {
        var bit = self.bases.valueIterator();
        while (bit.next()) |v| v.deinit(self.alloc);
        var dit = self.deltas.valueIterator();
        while (dit.next()) |v| v.deinit(self.alloc);
        self.bases.deinit(self.alloc);
        self.deltas.deinit(self.alloc);
    }

    pub fn writeBase(self: *BlobStore, posting: PostingId, bytes: []const u8) !void {
        const gop = try self.bases.getOrPut(self.alloc, posting);
        if (!gop.found_existing) gop.value_ptr.* = .empty;
        gop.value_ptr.clearRetainingCapacity();
        try gop.value_ptr.appendSlice(self.alloc, bytes);
        self.counters.base_writes += 1;
        self.counters.base_write_bytes += bytes.len;
    }

    pub fn appendDelta(self: *BlobStore, posting: PostingId, bytes: []const u8) !void {
        const gop = try self.deltas.getOrPut(self.alloc, posting);
        if (!gop.found_existing) gop.value_ptr.* = .empty;
        try gop.value_ptr.appendSlice(self.alloc, bytes);
        self.counters.delta_appends += 1;
        self.counters.delta_append_bytes += bytes.len;
    }

    pub fn readBase(self: *BlobStore, posting: PostingId) []const u8 {
        const b = self.bases.getPtr(posting) orelse return &.{};
        self.counters.reads += 1;
        self.counters.read_bytes += b.items.len;
        return b.items;
    }

    pub fn readDelta(self: *BlobStore, posting: PostingId) []const u8 {
        const d = self.deltas.getPtr(posting) orelse return &.{};
        self.counters.reads += 1;
        self.counters.read_bytes += d.items.len;
        return d.items;
    }

    pub fn clearDelta(self: *BlobStore, posting: PostingId) void {
        if (self.deltas.getPtr(posting)) |d| d.clearRetainingCapacity();
    }

    pub fn deltaLen(self: *BlobStore, posting: PostingId) usize {
        const d = self.deltas.getPtr(posting) orelse return 0;
        return d.items.len;
    }

    pub fn baseLen(self: *BlobStore, posting: PostingId) usize {
        const b = self.bases.getPtr(posting) orelse return 0;
        return b.items.len;
    }

    pub fn residentBytes(self: *BlobStore) u64 {
        var total: u64 = 0;
        var bit = self.bases.valueIterator();
        while (bit.next()) |v| total += v.items.len;
        var dit = self.deltas.valueIterator();
        while (dit.next()) |v| total += v.items.len;
        return total;
    }
};

const Centroid = struct {
    centroid: []f32,
    count: u32 = 0, // live member count (best-effort; exact after fold)
    deltas: u32 = 0, // unfolded delta records since last fold
    gen: u32 = 0,
    dirty: bool = false,
    mut_ver: u64 = 0, // bumped on every write touching this posting
    cent_ver: u64 = 0, // mut_ver as of the last centroid refresh (fold)
};

const Assignment = struct {
    posting: PostingId,
};

pub const RepairStats = struct {
    folded: u32 = 0,
    live_after: u64 = 0,
};

pub const QueryStats = struct {
    distances: u64 = 0, // member distance computations
    centroid_distances: u64 = 0, // routing distance computations
    postings_scanned: u64 = 0,
    members_scanned: u64 = 0,
};

pub const Config = struct {
    dim: u32,
    num_postings: PostingId,
};

pub const Index = struct {
    alloc: Allocator,
    dim: u32,
    store: BlobStore,
    cents: []Centroid,
    assign: std.AutoHashMapUnmanaged(VectorId, Assignment) = .empty,

    // reusable scratch
    rec_scratch: ArrayU8 = .empty,
    overlay_ids: std.ArrayListUnmanaged(VectorId) = .empty,
    overlay_vecs: std.ArrayListUnmanaged(f32) = .empty,
    overlay_pos: std.AutoHashMapUnmanaged(VectorId, usize) = .empty,
    route_dist: []f32 = &.{},

    pub fn init(alloc: Allocator, cfg: Config, seeds: []const []const f32) !Index {
        std.debug.assert(seeds.len == cfg.num_postings);
        const cents = try alloc.alloc(Centroid, cfg.num_postings);
        errdefer alloc.free(cents);
        for (cents, 0..) |*c, i| {
            const buf = try alloc.alloc(f32, cfg.dim);
            @memcpy(buf, seeds[i][0..cfg.dim]);
            c.* = .{ .centroid = buf };
        }
        const route_dist = try alloc.alloc(f32, cfg.num_postings);
        return .{
            .alloc = alloc,
            .dim = cfg.dim,
            .store = BlobStore.init(alloc),
            .cents = cents,
            .route_dist = route_dist,
        };
    }

    pub fn deinit(self: *Index) void {
        for (self.cents) |c| self.alloc.free(c.centroid);
        self.alloc.free(self.cents);
        self.alloc.free(self.route_dist);
        self.store.deinit();
        self.assign.deinit(self.alloc);
        self.rec_scratch.deinit(self.alloc);
        self.overlay_ids.deinit(self.alloc);
        self.overlay_vecs.deinit(self.alloc);
        self.overlay_pos.deinit(self.alloc);
    }

    fn route1(self: *Index, q: []const f32) PostingId {
        var best: PostingId = 0;
        var best_d: f32 = std.math.floatMax(f32);
        for (self.cents, 0..) |c, i| {
            const d = vec.l2SquaredDistance(q, c.centroid);
            if (d < best_d) {
                best_d = d;
                best = @intCast(i);
            }
        }
        return best;
    }

    fn encodePut(self: *Index, id: VectorId, v: []const f32) !void {
        self.rec_scratch.clearRetainingCapacity();
        try self.rec_scratch.append(self.alloc, op_put);
        var idb: [8]u8 = undefined;
        std.mem.writeInt(u64, &idb, id, .little);
        try self.rec_scratch.appendSlice(self.alloc, &idb);
        var fb: [4]u8 = undefined;
        for (v[0..self.dim]) |f| {
            std.mem.writeInt(u32, &fb, @bitCast(f), .little);
            try self.rec_scratch.appendSlice(self.alloc, &fb);
        }
    }

    fn encodeDel(self: *Index, id: VectorId) !void {
        self.rec_scratch.clearRetainingCapacity();
        try self.rec_scratch.append(self.alloc, op_del);
        var idb: [8]u8 = undefined;
        std.mem.writeInt(u64, &idb, id, .little);
        try self.rec_scratch.appendSlice(self.alloc, &idb);
    }

    fn touch(self: *Index, posting: PostingId) void {
        const c = &self.cents[posting];
        c.deltas += 1;
        c.mut_ver += 1;
        c.dirty = true;
    }

    /// Insert a brand new vector (assumed absent).
    pub fn insert(self: *Index, id: VectorId, v: []const f32) !void {
        const target = self.route1(v);
        try self.encodePut(id, v);
        try self.store.appendDelta(target, self.rec_scratch.items);
        try self.assign.put(self.alloc, id, .{ .posting = target });
        self.cents[target].count += 1;
        self.touch(target);
    }

    /// Overwrite an existing vector. Routes the new vector; if it now belongs to
    /// a different posting, tombstones the old one and inserts into the new
    /// (cross-posting replacement). Otherwise it is an in-place replacement delta.
    pub fn update(self: *Index, id: VectorId, v: []const f32) !void {
        const target = self.route1(v);
        if (self.assign.get(id)) |old| {
            if (old.posting != target) {
                try self.encodeDel(id);
                try self.store.appendDelta(old.posting, self.rec_scratch.items);
                if (self.cents[old.posting].count > 0) self.cents[old.posting].count -= 1;
                self.touch(old.posting);
                self.cents[target].count += 1;
            }
        } else {
            self.cents[target].count += 1;
        }
        try self.encodePut(id, v);
        try self.store.appendDelta(target, self.rec_scratch.items);
        try self.assign.put(self.alloc, id, .{ .posting = target });
        self.touch(target);
    }

    pub fn remove(self: *Index, id: VectorId) !void {
        const old = self.assign.get(id) orelse return;
        try self.encodeDel(id);
        try self.store.appendDelta(old.posting, self.rec_scratch.items);
        if (self.cents[old.posting].count > 0) self.cents[old.posting].count -= 1;
        self.touch(old.posting);
        _ = self.assign.remove(id);
    }

    /// Build the live member set for a posting by overlaying base + deltas.
    /// Results land in self.overlay_ids / self.overlay_vecs (flattened, dim each).
    fn overlay(self: *Index, posting: PostingId) !void {
        self.overlay_ids.clearRetainingCapacity();
        self.overlay_vecs.clearRetainingCapacity();
        self.overlay_pos.clearRetainingCapacity();

        const base = self.store.readBase(posting);
        try self.parseInto(base, false);
        const delta = self.store.readDelta(posting);
        try self.parseInto(delta, true);
    }

    // Parse a blob of records into the overlay buffers. `is_delta` blobs may
    // contain tombstones and replacements; later records win.
    fn parseInto(self: *Index, blob: []const u8, is_delta: bool) !void {
        var off: usize = 0;
        const stride = 1 + 8 + 4 * @as(usize, self.dim);
        const del_stride = 1 + 8;
        while (off < blob.len) {
            const op = blob[off];
            if (op == op_del) {
                const id = std.mem.readInt(u64, blob[off + 1 ..][0..8], .little);
                if (self.overlay_pos.get(id)) |pos| {
                    // mark dead by overwriting id with a tombstone sentinel slot:
                    // simplest is to remove from pos map and leave the vec slot,
                    // but we compact lazily — track liveness via pos map only.
                    _ = self.overlay_pos.remove(id);
                    self.overlay_ids.items[pos] = std.math.maxInt(VectorId); // dead marker
                }
                off += del_stride;
                continue;
            }
            // op_put
            const id = std.mem.readInt(u64, blob[off + 1 ..][0..8], .little);
            const vstart = off + 9;
            if (self.overlay_pos.get(id)) |pos| {
                // replacement: rewrite the existing flattened slot
                const dst = self.overlay_vecs.items[pos * self.dim ..][0..self.dim];
                decodeVec(blob[vstart..], dst);
                self.overlay_ids.items[pos] = id;
            } else {
                const pos = self.overlay_ids.items.len;
                try self.overlay_ids.append(self.alloc, id);
                const base_len = self.overlay_vecs.items.len;
                try self.overlay_vecs.resize(self.alloc, base_len + self.dim);
                const dst = self.overlay_vecs.items[base_len..][0..self.dim];
                decodeVec(blob[vstart..], dst);
                try self.overlay_pos.put(self.alloc, id, pos);
            }
            _ = is_delta;
            off += stride;
        }
    }

    /// Fold base+deltas into a fresh base, recompute the centroid from live
    /// members, and clear the delta tail. This is the background maintenance unit.
    fn fold(self: *Index, posting: PostingId) !void {
        const before = self.store.counters.read_bytes;
        try self.overlay(posting);
        self.store.counters.fold_input_bytes += self.store.counters.read_bytes - before;

        // Write new base as a sequence of op_put records (only live members), so
        // the same parser reads base and delta blobs uniformly.
        self.rec_scratch.clearRetainingCapacity();
        var live: u32 = 0;
        var sum: []f32 = try self.alloc.alloc(f32, self.dim);
        defer self.alloc.free(sum);
        @memset(sum, 0);
        var i: usize = 0;
        while (i < self.overlay_ids.items.len) : (i += 1) {
            const id = self.overlay_ids.items[i];
            if (id == std.math.maxInt(VectorId)) continue; // dead
            const v = self.overlay_vecs.items[i * self.dim ..][0..self.dim];
            try self.rec_scratch.append(self.alloc, op_put);
            var idb: [8]u8 = undefined;
            std.mem.writeInt(u64, &idb, id, .little);
            try self.rec_scratch.appendSlice(self.alloc, &idb);
            var fb: [4]u8 = undefined;
            for (v, 0..) |f, d| {
                std.mem.writeInt(u32, &fb, @bitCast(f), .little);
                try self.rec_scratch.appendSlice(self.alloc, &fb);
                sum[d] += f;
            }
            live += 1;
        }
        try self.store.writeBase(posting, self.rec_scratch.items);
        self.store.clearDelta(posting);

        const c = &self.cents[posting];
        if (live > 0) {
            const inv: f32 = 1.0 / @as(f32, @floatFromInt(live));
            for (c.centroid, 0..) |*ce, d| ce.* = sum[d] * inv;
        }
        c.count = live;
        c.deltas = 0;
        c.gen += 1;
        c.dirty = false;
        c.cent_ver = c.mut_ver;
        self.store.counters.folds += 1;
    }

    /// Bounded background repair: fold up to `budget` of the dirtiest postings
    /// (most unfolded deltas first, approximated by a single scan).
    pub fn repair(self: *Index, budget: u32) !RepairStats {
        var stats: RepairStats = .{};
        var done: u32 = 0;
        // Greedy: repeatedly pick the dirty posting with the most deltas.
        while (done < budget) {
            var best: ?PostingId = null;
            var best_deltas: u32 = 0;
            for (self.cents, 0..) |c, i| {
                if (c.dirty and c.deltas > best_deltas) {
                    best_deltas = c.deltas;
                    best = @intCast(i);
                }
            }
            const p = best orelse break;
            try self.fold(p);
            done += 1;
        }
        stats.folded = done;
        var live: u64 = 0;
        for (self.cents) |c| live += c.count;
        stats.live_after = live;
        return stats;
    }

    /// LSM-style background maintenance: fold a posting only when its delta tail
    /// has grown to at least `fold_ratio_pct`% of its base size (and past a small
    /// absolute floor). This bounds query read-amp to ~(1 + ratio) of base while
    /// avoiding the over-compaction trap of re-folding clean postings.
    pub fn repairTriggered(self: *Index, fold_ratio_pct: u32, min_delta_bytes: usize) !RepairStats {
        var stats: RepairStats = .{};
        for (self.cents, 0..) |c, i| {
            if (!c.dirty) continue;
            const p: PostingId = @intCast(i);
            const dlen = self.store.deltaLen(p);
            if (dlen < min_delta_bytes) continue;
            const blen = self.store.baseLen(p);
            if (blen == 0 or dlen * 100 >= blen * fold_ratio_pct) {
                try self.fold(p);
                stats.folded += 1;
            }
        }
        var live: u64 = 0;
        for (self.cents) |c| live += c.count;
        stats.live_after = live;
        return stats;
    }

    pub fn maxPostingCount(self: *Index) u32 {
        var m: u32 = 0;
        for (self.cents) |c| m = @max(m, c.count);
        return m;
    }

    pub fn dirtyPostings(self: *Index) u32 {
        var n: u32 = 0;
        for (self.cents) |c| {
            if (c.dirty) n += 1;
        }
        return n;
    }

    pub fn unfoldedDeltas(self: *Index) u64 {
        var n: u64 = 0;
        for (self.cents) |c| n += c.deltas;
        return n;
    }

    /// Average centroid staleness across dirty postings (mut_ver - cent_ver).
    pub fn avgStaleness(self: *Index) f64 {
        var sum: u64 = 0;
        var n: u64 = 0;
        for (self.cents) |c| {
            if (c.dirty) {
                sum += c.mut_ver - c.cent_ver;
                n += 1;
            }
        }
        if (n == 0) return 0;
        return @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(n));
    }

    /// Search: route to `nprobe` candidate postings by centroid, overlay each,
    /// and return the top-`k` nearest by exact distance.
    pub fn query(
        self: *Index,
        q: []const f32,
        k: usize,
        nprobe: usize,
        out_ids: []VectorId,
        out_dists: []f32,
        stats: *QueryStats,
    ) !usize {
        // route: compute centroid distances, pick nprobe smallest
        for (self.cents, 0..) |c, i| {
            self.route_dist[i] = vec.l2SquaredDistance(q, c.centroid);
        }
        stats.centroid_distances += self.cents.len;

        var found: usize = 0; // entries in out_ids/out_dists (kept sorted asc by dist)
        var probe: usize = 0;
        const np = @min(nprobe, self.cents.len);
        while (probe < np) : (probe += 1) {
            // argmin over route_dist
            var bp: usize = 0;
            var bd: f32 = std.math.floatMax(f32);
            for (self.route_dist, 0..) |d, i| {
                if (d < bd) {
                    bd = d;
                    bp = i;
                }
            }
            self.route_dist[bp] = std.math.floatMax(f32); // consume
            const posting: PostingId = @intCast(bp);

            try self.overlay(posting);
            stats.postings_scanned += 1;
            var i: usize = 0;
            while (i < self.overlay_ids.items.len) : (i += 1) {
                const id = self.overlay_ids.items[i];
                if (id == std.math.maxInt(VectorId)) continue;
                const v = self.overlay_vecs.items[i * self.dim ..][0..self.dim];
                const d = vec.l2SquaredDistance(q, v);
                stats.distances += 1;
                stats.members_scanned += 1;
                found = insertTopK(out_ids, out_dists, found, k, id, d);
            }
        }
        return found;
    }
};

fn insertTopK(ids: []VectorId, dists: []f32, found: usize, k: usize, id: VectorId, d: f32) usize {
    if (found >= k and d >= dists[found - 1]) return found;
    var n = found;
    if (n < k) {
        n += 1;
    }
    var i: usize = n - 1;
    // shift larger entries right
    while (i > 0 and dists[i - 1] > d) : (i -= 1) {
        dists[i] = dists[i - 1];
        ids[i] = ids[i - 1];
    }
    dists[i] = d;
    ids[i] = id;
    return n;
}

fn decodeVec(src: []const u8, dst: []f32) void {
    for (dst, 0..) |*o, i| {
        const bits = std.mem.readInt(u32, src[i * 4 ..][0..4], .little);
        o.* = @bitCast(bits);
    }
}

test "insert, query, fold roundtrip" {
    const alloc = std.testing.allocator;
    const dim: u32 = 4;
    const seed0 = [_]f32{ 0, 0, 0, 0 };
    const seed1 = [_]f32{ 10, 10, 10, 10 };
    var seeds = [_][]const f32{ &seed0, &seed1 };
    var idx = try Index.init(alloc, .{ .dim = dim, .num_postings = 2 }, &seeds);
    defer idx.deinit();

    const a = [_]f32{ 1, 1, 1, 1 };
    const b = [_]f32{ 9, 9, 9, 9 };
    try idx.insert(1, &a);
    try idx.insert(2, &b);

    var ids: [4]VectorId = undefined;
    var dists: [4]f32 = undefined;
    var qs: QueryStats = .{};
    const n = try idx.query(&a, 1, 2, &ids, &dists, &qs);
    try std.testing.expectEqual(@as(usize, 1), n);
    try std.testing.expectEqual(@as(VectorId, 1), ids[0]);

    // overwrite id 1 to be near b, then fold and re-query near b's seed
    try idx.update(1, &b);
    _ = try idx.repair(2);
    try std.testing.expectEqual(@as(u64, 0), idx.unfoldedDeltas());

    var qs2: QueryStats = .{};
    const n2 = try idx.query(&b, 2, 2, &ids, &dists, &qs2);
    try std.testing.expectEqual(@as(usize, 2), n2);
}
