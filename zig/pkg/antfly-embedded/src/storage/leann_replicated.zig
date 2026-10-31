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

//! Table-facing state of a leader-replicated LEANN index (one per replica).
//!
//! Two inputs, kept apart:
//!
//! - Local derived replay (`noteWrites`) tells this replica which documents
//!   changed. It updates replica-local vector-id bindings and a durable
//!   "dirty" set of keys awaiting replication, each with the digest of its
//!   latest vector (or a delete marker). Every replica keeps this set, so a
//!   new leader already holds whatever was not yet replicated.
//! - Committed records (`applyRecord`, in Raft order) change the graph. A
//!   key leaves the dirty set only when the record indexed exactly the
//!   vector this replica last saw (matching digest), so a newer local change
//!   that raced ahead of the record is kept for re-planning.
//!
//! Only the leader calls `plan`, which turns dirty keys into one record at a
//! time (or a rebuild, past the delete threshold). Records the log skips as
//! stale leave their keys dirty, so they are simply planned again.

const std = @import("std");
const Allocator = std.mem.Allocator;
const leann = @import("antfly_vectorindex").leann;
const search_results = @import("antfly_vectorindex").search_results;
const search_types = @import("antfly_vectorindex").search_types;
const replica_mod = @import("leann_replica.zig");

pub const file_name = "leann.replica";
const magic = "LEANNRST";
const format_version: u32 = 1;
const delete_digest: u64 = 0;

/// Loads a stored vector by local vector id and key (HBC's loader shape).
pub const VectorLoader = struct {
    ctx: *anyopaque,
    load: *const fn (ctx: *anyopaque, alloc: Allocator, vector_id: u64, metadata: []const u8) anyerror![]f32,
};

pub const Options = struct {
    replica: replica_mod.Options,
    /// Keys per planned record.
    max_record_keys: usize = 256,
    /// Plan a rebuild once this fraction of graph nodes is deleted.
    rebuild_deleted_fraction: f64 = 0.2,
    search_ef: u32 = 0,
    bound_scale: f32 = 1.0,
    batch_size: u32 = 0,
};

/// A planned record, plus generation bytes the leader must make fetchable
/// when the record does not inline them.
pub const Planned = struct {
    record: []u8,
    blob: ?Blob = null,

    pub const Blob = struct { generation: u64, digest: u64, bytes: []u8 };

    pub fn deinit(self: *Planned, alloc: Allocator) void {
        alloc.free(self.record);
        if (self.blob) |b| alloc.free(b.bytes);
        self.* = undefined;
    }
};

pub const Stats = struct {
    live: u64,
    graph_nodes: u64,
    deleted_nodes: u64,
    dirty: u64,
    generation: u64,
    applied_raft_index: u64,
};

pub const ReplicatedState = struct {
    alloc: Allocator,
    opts: Options,
    replica: replica_mod.Replica,
    /// key -> local vector id. Owns the keys.
    bindings: std.StringHashMapUnmanaged(u64) = .empty,
    /// local vector id -> key (borrowed from `bindings`).
    key_of: std.AutoHashMapUnmanaged(u64, []const u8) = .empty,
    /// key -> vector digest awaiting replication (0 = delete). Owns the keys.
    dirty: std.StringHashMapUnmanaged(u64) = .empty,
    /// Raft index of the last record applied (or skipped).
    applied_raft_index: u64 = 0,
    /// Set while a planned record is uncommitted; cleared when any record
    /// applies or is skipped, so a lost proposal is re-planned.
    inflight: bool = false,
    loader: ?VectorLoader = null,
    fetcher: ?replica_mod.GenerationFetcher = null,

    pub fn init(alloc: Allocator, opts: Options) !ReplicatedState {
        return .{ .alloc = alloc, .opts = opts, .replica = try replica_mod.Replica.init(alloc, opts.replica) };
    }

    pub fn deinit(self: *ReplicatedState) void {
        self.replica.deinit();
        var it = self.bindings.keyIterator();
        while (it.next()) |k| self.alloc.free(k.*);
        self.bindings.deinit(self.alloc);
        self.key_of.deinit(self.alloc);
        var dit = self.dirty.keyIterator();
        while (dit.next()) |k| self.alloc.free(k.*);
        self.dirty.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn stats(self: *const ReplicatedState) Stats {
        const index = self.replica.index;
        return .{
            .live = self.replica.liveCount(),
            .graph_nodes = if (index) |*i| i.node_count else 0,
            .deleted_nodes = if (index) |*i| i.deleted_count else 0,
            .dirty = self.dirty.count(),
            .generation = self.replica.generation,
            .applied_raft_index = self.applied_raft_index,
        };
    }

    // ---------------------------------------------------------------------
    // Local derived replay
    // ---------------------------------------------------------------------

    /// Records local writes (`vector_id`, `vector`, `metadata` = key) and
    /// deletes (by vector id). Idempotent.
    pub fn noteWrites(self: *ReplicatedState, items: anytype, deletes: []const u64) !void {
        for (deletes) |vector_id| {
            const key = self.key_of.get(vector_id) orelse continue;
            try self.markDirty(key, delete_digest);
            _ = self.key_of.remove(vector_id);
            const kv = self.bindings.fetchRemove(key).?;
            self.alloc.free(kv.key);
        }
        for (items) |item| {
            const key: []const u8 = item.metadata;
            if (key.len == 0) return error.MissingLeannKey;
            if (item.vector.len != self.opts.replica.config.dims) return error.DimensionMismatch;
            try self.bind(key, item.vector_id);
            try self.markDirty(key, replica_mod.vectorDigest(item.vector));
        }
    }

    fn bind(self: *ReplicatedState, key: []const u8, vector_id: u64) !void {
        if (self.bindings.getEntry(key)) |e| {
            if (e.value_ptr.* == vector_id) return;
            _ = self.key_of.remove(e.value_ptr.*);
            e.value_ptr.* = vector_id;
            try self.key_of.put(self.alloc, vector_id, e.key_ptr.*);
            return;
        }
        const owned = try self.alloc.dupe(u8, key);
        errdefer self.alloc.free(owned);
        try self.bindings.ensureUnusedCapacity(self.alloc, 1);
        try self.key_of.ensureUnusedCapacity(self.alloc, 1);
        if (self.key_of.get(vector_id)) |old_key| {
            // A vector id rebound to a new key: drop the stale binding.
            const kv = self.bindings.fetchRemove(old_key).?;
            self.alloc.free(kv.key);
        }
        self.bindings.putAssumeCapacity(owned, vector_id);
        self.key_of.putAssumeCapacity(vector_id, owned);
    }

    fn markDirty(self: *ReplicatedState, key: []const u8, digest: u64) !void {
        if (self.dirty.getPtr(key)) |d| {
            d.* = digest;
            return;
        }
        const owned = try self.alloc.dupe(u8, key);
        errdefer self.alloc.free(owned);
        try self.dirty.put(self.alloc, owned, digest);
    }

    fn clearDirty(self: *ReplicatedState, key: []const u8) void {
        if (self.dirty.fetchRemove(key)) |kv| self.alloc.free(kv.key);
    }

    // ---------------------------------------------------------------------
    // Leader planning
    // ---------------------------------------------------------------------

    /// Plans the next record, or returns null when nothing is pending or a
    /// record is already in flight. Leader only.
    pub fn plan(self: *ReplicatedState, alloc: Allocator) !?Planned {
        if (self.inflight) return null;
        if (self.dirty.count() == 0) return try self.planRebuildIfNeeded(alloc);
        const loader = self.loader orelse return error.MissingVectorLoader;

        var work = std.ArrayListUnmanaged(replica_mod.Work).empty;
        defer {
            for (work.items) |w| if (w.vector) |v| alloc.free(v);
            work.deinit(alloc);
        }
        var it = self.dirty.iterator();
        while (it.next()) |e| {
            if (work.items.len >= self.opts.max_record_keys) break;
            const key = e.key_ptr.*;
            if (e.value_ptr.* == delete_digest) {
                if (self.replica.contains(key)) try work.append(alloc, .{ .key = key, .vector = null });
                continue;
            }
            const vector_id = self.bindings.get(key) orelse continue;
            const v = loader.load(loader.ctx, alloc, vector_id, key) catch |err| switch (err) {
                error.NotFound => continue,
                else => return err,
            };
            try work.append(alloc, .{ .key = key, .vector = v });
        }
        if (work.items.len == 0) {
            // Everything dirty was a delete of an absent key or unloadable;
            // nothing to replicate for those.
            var stale = std.ArrayListUnmanaged([]const u8).empty;
            defer stale.deinit(alloc);
            var sit = self.dirty.iterator();
            while (sit.next()) |e| if (e.value_ptr.* == delete_digest and !self.replica.contains(e.key_ptr.*)) try stale.append(alloc, e.key_ptr.*);
            for (stale.items) |k| self.clearDirty(k);
            return null;
        }
        var ctx = KeyLoader{ .state = self, .loader = loader };
        const record = (try self.replica.plan(alloc, work.items, ctx.keyLoader())) orelse return null;
        self.inflight = true;
        return .{ .record = record };
    }

    fn planRebuildIfNeeded(self: *ReplicatedState, alloc: Allocator) !?Planned {
        const index = self.replica.index orelse return null;
        if (index.deletedFraction() < self.opts.rebuild_deleted_fraction) return null;
        const loader = self.loader orelse return error.MissingVectorLoader;
        var ctx = KeyLoader{ .state = self, .loader = loader };
        const planned = try self.replica.planRebuild(alloc, ctx.keyLoader());
        self.inflight = true;
        const inlined = planned.bytes.len <= self.opts.replica.inline_generation_limit;
        if (inlined) {
            alloc.free(planned.bytes);
            return .{ .record = planned.record };
        }
        return .{ .record = planned.record, .blob = .{ .generation = planned.generation, .digest = planned.digest, .bytes = planned.bytes } };
    }

    /// Key -> stored vector, through the local binding.
    const KeyLoader = struct {
        state: *ReplicatedState,
        loader: VectorLoader,

        fn keyLoader(self: *KeyLoader) replica_mod.VectorLoader {
            return .{ .ctx = self, .load = load };
        }

        fn load(raw: *anyopaque, alloc: Allocator, key: []const u8) anyerror![]f32 {
            const self: *KeyLoader = @ptrCast(@alignCast(raw));
            const vector_id = self.state.bindings.get(key) orelse return error.NotFound;
            return self.loader.load(self.loader.ctx, alloc, vector_id, key);
        }
    };

    // ---------------------------------------------------------------------
    // Committed records
    // ---------------------------------------------------------------------

    /// Applies the record committed at `raft_index`. Records at or below the
    /// last applied index are ignored, so replay after a crash is safe.
    /// Returns `error.GenerationUnavailable` (state unchanged) when a needed
    /// generation cannot be fetched yet.
    pub fn applyRecord(self: *ReplicatedState, record: []const u8, raft_index: u64) !replica_mod.Outcome {
        if (raft_index <= self.applied_raft_index) return .stale;
        const outcome = try self.replica.apply(record, self.fetcher);
        if (outcome == .applied) {
            const effects = try replica_mod.Replica.recordEffects(self.alloc, record);
            defer self.alloc.free(effects.effects);
            for (effects.effects) |e| {
                const local = self.dirty.get(e.key) orelse continue;
                const covered = if (e.digest) |d| local == d else local == delete_digest;
                if (covered) self.clearDirty(e.key);
            }
        }
        self.applied_raft_index = raft_index;
        self.inflight = false;
        return outcome;
    }

    // ---------------------------------------------------------------------
    // Search
    // ---------------------------------------------------------------------

    /// Answers an HBC search request. Graph results are keys; they map to
    /// this replica's vector ids through the bindings (keys without a local
    /// binding, e.g. deleted locally ahead of the log, are skipped).
    pub fn search(self: *const ReplicatedState, alloc: Allocator, req: search_types.SearchRequest) !search_results.SearchResults {
        var out = search_results.SearchResults.init(alloc, req.k);
        errdefer out.deinit();
        if (req.k == 0) return out;
        if (req.cancellation) |token| if (token.isCancelled()) return error.Cancelled;
        const loader = self.loader orelse return error.MissingVectorLoader;
        var ctx = KeyLoader{ .state = @constCast(self), .loader = loader };
        var want: u32 = @intCast(@max(req.k, 1));
        const filtered = req.filter_ids.len > 0 or req.exclude_ids.len > 0 or req.key_predicate != null or
            req.filter_prefix.len > 0 or req.distance_over != null or req.distance_under != null;
        if (filtered) want = @intCast(@max(req.k * 4, req.k + 64));
        const live: u32 = @intCast(@max(self.replica.liveCount(), 1));
        while (true) {
            want = @min(want, live);
            const hits = try self.replica.search(alloc, req.query, want, ctx.keyLoader());
            defer alloc.free(hits);
            var passed: usize = 0;
            for (hits) |h| {
                const vector_id = self.bindings.get(h.key) orelse continue;
                if (!try allows(req, vector_id, h.key, h.distance)) continue;
                passed += 1;
            }
            if (passed >= req.k or want >= live or !filtered) {
                for (hits) |h| {
                    const vector_id = self.bindings.get(h.key) orelse continue;
                    if (!try allows(req, vector_id, h.key, h.distance)) continue;
                    out.addResultWithOwnedMetadata(vector_id, h.distance, 0, if (req.load_metadata) try alloc.dupe(u8, h.key) else null);
                }
                return out;
            }
            want *|= 4;
        }
    }

    fn allows(req: search_types.SearchRequest, vector_id: u64, key: []const u8, dist: f32) !bool {
        if (req.filter_ids.len > 0 and std.mem.indexOfScalar(u64, req.filter_ids, vector_id) == null) return false;
        if (std.mem.indexOfScalar(u64, req.exclude_ids, vector_id) != null) return false;
        if (req.filter_prefix.len > 0 and !std.mem.startsWith(u8, key, req.filter_prefix)) return false;
        if (req.key_predicate) |p| if (!try p.allows(p.ptr, key)) return false;
        if (req.distance_over) |t| if (!(dist > t)) return false;
        if (req.distance_under) |t| if (!(dist < t)) return false;
        return true;
    }

    pub fn getMetadata(self: *const ReplicatedState, alloc: Allocator, vector_id: u64) !?[]u8 {
        const key = self.key_of.get(vector_id) orelse return null;
        return try alloc.dupe(u8, key);
    }

    // ---------------------------------------------------------------------
    // Persistence
    // ---------------------------------------------------------------------

    pub fn encode(self: *const ReplicatedState, alloc: Allocator) ![]u8 {
        var w: std.Io.Writer.Allocating = .init(alloc);
        defer w.deinit();
        const wr = &w.writer;
        try wr.writeAll(magic);
        try wr.writeInt(u32, format_version, .little);
        try wr.writeInt(u64, self.applied_raft_index, .little);
        const state = try self.replica.encodeState(alloc);
        defer alloc.free(state);
        try writeBytes(wr, state);
        try wr.writeInt(u32, self.bindings.count(), .little);
        var it = self.bindings.iterator();
        while (it.next()) |e| {
            try writeBytes(wr, e.key_ptr.*);
            try wr.writeInt(u64, e.value_ptr.*, .little);
        }
        try wr.writeInt(u32, self.dirty.count(), .little);
        var dit = self.dirty.iterator();
        while (dit.next()) |e| {
            try writeBytes(wr, e.key_ptr.*);
            try wr.writeInt(u64, e.value_ptr.*, .little);
        }
        const crc = std.hash.Crc32.hash(w.written());
        try wr.writeInt(u32, crc, .little);
        return w.toOwnedSlice();
    }

    pub fn decode(alloc: Allocator, opts: Options, bytes: []const u8) !ReplicatedState {
        if (bytes.len < magic.len + 16) return error.InvalidLeannState;
        const body = bytes[0 .. bytes.len - 4];
        if (std.hash.Crc32.hash(body) != std.mem.readInt(u32, bytes[bytes.len - 4 ..][0..4], .little)) return error.InvalidLeannState;
        var r: std.Io.Reader = .fixed(body);
        const head = r.take(magic.len) catch return error.InvalidLeannState;
        if (!std.mem.eql(u8, head, magic)) return error.InvalidLeannState;
        if ((r.takeInt(u32, .little) catch return error.InvalidLeannState) != format_version) return error.InvalidLeannState;
        const applied = r.takeInt(u64, .little) catch return error.InvalidLeannState;
        var self = ReplicatedState{
            .alloc = alloc,
            .opts = opts,
            .replica = try replica_mod.Replica.decodeState(alloc, opts.replica, try readBytes(&r)),
            .applied_raft_index = applied,
        };
        errdefer self.deinit();
        const nb = r.takeInt(u32, .little) catch return error.InvalidLeannState;
        for (0..nb) |_| {
            const key = try readBytes(&r);
            try self.bind(key, r.takeInt(u64, .little) catch return error.InvalidLeannState);
        }
        const nd = r.takeInt(u32, .little) catch return error.InvalidLeannState;
        for (0..nd) |_| {
            const key = try readBytes(&r);
            try self.markDirty(key, r.takeInt(u64, .little) catch return error.InvalidLeannState);
        }
        if (r.bufferedLen() != 0) return error.InvalidLeannState;
        return self;
    }
};

fn writeBytes(wr: *std.Io.Writer, b: []const u8) !void {
    try wr.writeInt(u32, @intCast(b.len), .little);
    try wr.writeAll(b);
}

fn readBytes(r: *std.Io.Reader) ![]const u8 {
    const len = r.takeInt(u32, .little) catch return error.InvalidLeannState;
    return r.take(len) catch error.InvalidLeannState;
}

// -------------------------------------------------------------------------
// Tests
// -------------------------------------------------------------------------

const testing = std.testing;

/// Primary store stand-in: stored vectors by local vector id, which differ
/// per replica.
const TestPrimary = struct {
    vectors: std.AutoHashMapUnmanaged(u64, []f32) = .empty,
    alloc: Allocator,

    fn deinit(self: *TestPrimary) void {
        var it = self.vectors.valueIterator();
        while (it.next()) |v| self.alloc.free(v.*);
        self.vectors.deinit(self.alloc);
    }

    fn put(self: *TestPrimary, id: u64, v: []const f32) !void {
        const gop = try self.vectors.getOrPut(self.alloc, id);
        if (gop.found_existing) self.alloc.free(gop.value_ptr.*);
        gop.value_ptr.* = try self.alloc.dupe(f32, v);
    }

    fn loader(self: *TestPrimary) VectorLoader {
        return .{ .ctx = self, .load = load };
    }

    fn load(raw: *anyopaque, alloc: Allocator, vector_id: u64, _: []const u8) anyerror![]f32 {
        const self: *TestPrimary = @ptrCast(@alignCast(raw));
        const v = self.vectors.get(vector_id) orelse return error.NotFound;
        return try alloc.dupe(f32, v);
    }
};

const TestItem = struct { vector_id: u64, vector: []const f32, metadata: []const u8 };

test "leann replicated state: derived replay plus committed records converge" {
    const alloc = testing.allocator;
    const dims = 12;
    const opts = Options{ .replica = .{ .config = .{ .dims = dims, .metric = .cosine, .max_degree = 10 }, .build_threshold = 40 }, .max_record_keys = 25 };
    var leader = try ReplicatedState.init(alloc, opts);
    defer leader.deinit();
    var follower = try ReplicatedState.init(alloc, opts);
    defer follower.deinit();
    var leader_primary = TestPrimary{ .alloc = alloc };
    defer leader_primary.deinit();
    var follower_primary = TestPrimary{ .alloc = alloc };
    defer follower_primary.deinit();
    leader.loader = leader_primary.loader();
    follower.loader = follower_primary.loader();

    var prng = std.Random.DefaultPrng.init(71);
    const rand = prng.random();
    var keys: [120][16]u8 = undefined;
    var vecs: [120][dims]f32 = undefined;
    for (&vecs, 0..) |*v, i| for (v, 0..) |*x, j| {
        x.* = @as(f32, @floatFromInt((i % 5 + j) % 4)) + rand.floatNorm(f32) * 0.3;
    };

    var raft_index: u64 = 0;
    var log = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (log.items) |e| alloc.free(e);
        log.deinit(alloc);
    }
    var follower_applied: usize = 0;

    for (0..120) |i| {
        const key = try std.fmt.bufPrint(&keys[i], "doc:{d:0>4}", .{i});
        // Vector ids differ per replica.
        try leader_primary.put(i + 1, &vecs[i]);
        try follower_primary.put(1000 + i, &vecs[i]);
        try leader.noteWrites(&[_]TestItem{.{ .vector_id = i + 1, .vector = &vecs[i], .metadata = key }}, &.{});
        // The follower's derived replay runs ahead of the log here.
        try follower.noteWrites(&[_]TestItem{.{ .vector_id = 1000 + i, .vector = &vecs[i], .metadata = key }}, &.{});

        if (i % 10 == 9) {
            while (try leader.plan(alloc)) |planned_value| {
                var planned = planned_value;
                defer planned.deinit(alloc);
                raft_index += 1;
                try log.append(alloc, try alloc.dupe(u8, planned.record));
                _ = try leader.applyRecord(planned.record, raft_index);
            }
        }
    }
    // Overwrite one doc on both replicas after the leader planned but
    // before the follower applies: the follower must keep it dirty.
    var newer = vecs[5];
    newer[0] += 5;
    try follower_primary.put(1005, &newer);
    try follower.noteWrites(&[_]TestItem{.{ .vector_id = 1005, .vector = &newer, .metadata = keys[5][0..8] }}, &.{});

    while (follower_applied < log.items.len) : (follower_applied += 1) {
        _ = try follower.applyRecord(log.items[follower_applied], follower_applied + 1);
    }
    // Replaying the same records is a no-op.
    try testing.expectEqual(replica_mod.Outcome.stale, try follower.applyRecord(log.items[0], 1));

    const a = try leader.replica.encodeState(alloc);
    defer alloc.free(a);
    const b = try follower.replica.encodeState(alloc);
    defer alloc.free(b);
    try testing.expectEqualSlices(u8, a, b);
    try testing.expectEqual(@as(u64, 0), leader.stats().dirty);
    try testing.expectEqual(@as(u64, 1), follower.stats().dirty); // the newer doc:0005

    // Search maps keys to each replica's own vector ids.
    var res = try follower.search(alloc, .{ .query = &vecs[77], .k = 1 });
    defer res.deinit();
    try testing.expectEqual(@as(u64, 1077), res.items.items[0].vector_id);
    try testing.expectEqualStrings(keys[77][0..8], res.items.items[0].metadata.?);
    var res2 = try leader.search(alloc, .{ .query = &vecs[77], .k = 3, .exclude_ids = &.{78} });
    defer res2.deinit();
    for (res2.items.items) |item| try testing.expect(item.vector_id != 78);

    // Deletes replicate too; persistence round-trips.
    try leader.noteWrites(&[_]TestItem{}, &.{ 10, 11 });
    var planned = (try leader.plan(alloc)).?;
    defer planned.deinit(alloc);
    raft_index += 1;
    _ = try leader.applyRecord(planned.record, raft_index);
    try testing.expectEqual(@as(u64, 118), leader.stats().live);
    const saved = try leader.encode(alloc);
    defer alloc.free(saved);
    var restored = try ReplicatedState.decode(alloc, opts, saved);
    defer restored.deinit();
    const saved2 = try restored.encode(alloc);
    defer alloc.free(saved2);
    try testing.expectEqual(leader.stats().live, restored.stats().live);
    try testing.expectEqual(raft_index, restored.applied_raft_index);
}

test "leann replicated state: a lost proposal is re-planned by the next leader" {
    const alloc = testing.allocator;
    const dims = 8;
    const opts = Options{ .replica = .{ .config = .{ .dims = dims, .max_degree = 8 }, .build_threshold = 10 } };
    var a = try ReplicatedState.init(alloc, opts);
    defer a.deinit();
    var b = try ReplicatedState.init(alloc, opts);
    defer b.deinit();
    var primary = TestPrimary{ .alloc = alloc };
    defer primary.deinit();
    a.loader = primary.loader();
    b.loader = primary.loader();
    var keys: [6][8]u8 = undefined;
    for (0..6) |i| {
        var v: [dims]f32 = @splat(@floatFromInt(i));
        v[0] += 1;
        try primary.put(i, &v);
        const key = try std.fmt.bufPrint(&keys[i], "k{d}", .{i});
        try a.noteWrites(&[_]TestItem{.{ .vector_id = i, .vector = &v, .metadata = key }}, &.{});
        try b.noteWrites(&[_]TestItem{.{ .vector_id = i, .vector = &v, .metadata = key }}, &.{});
    }
    // Leader a plans, but its proposal is never committed (it lost leadership).
    var lost = (try a.plan(alloc)).?;
    lost.deinit(alloc);
    try testing.expect(a.inflight);
    // New leader b plans the same keys and commits; both apply.
    var p = (try b.plan(alloc)).?;
    defer p.deinit(alloc);
    try testing.expectEqual(replica_mod.Outcome.applied, try b.applyRecord(p.record, 1));
    try testing.expectEqual(replica_mod.Outcome.applied, try a.applyRecord(p.record, 1));
    try testing.expect(!a.inflight);
    try testing.expectEqual(@as(u64, 0), a.stats().dirty);
    try testing.expectEqual(@as(u64, 6), a.stats().live);
}
