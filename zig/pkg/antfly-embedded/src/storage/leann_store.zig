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

//! Table-facing state for an `algorithm: leann` dense index.
//!
//! The HBC adapter routes writes, deletes, searches and stats here when a
//! LeannStore is attached, so the index manager keeps its usual vector-id
//! mapping, post-filters, hydration and paging. This module adds what the
//! standalone `vectorindex.leann` graph lacks for a table:
//!
//! - a map from the manager's u64 vector ids (and their metadata keys) to
//!   LEANN's dense node ids,
//! - an exact buffer for the first `build_threshold` vectors, since a graph
//!   and its codebooks need a build set,
//! - idempotent upserts and deletes, so replay after a crash is safe,
//! - HBC request filters applied to an over-fetched candidate list,
//! - a CRC-checked file written atomically on sync,
//! - a rebuild from stored vectors once deletes exceed a threshold.
//!
//! Exact distances come from the index's vector loader (the persisted
//! embedding artifacts), so no full-precision vectors are kept here once the
//! graph is built.

const std = @import("std");
const Allocator = std.mem.Allocator;
const leann = @import("antfly_vectorindex").leann;
const search_results = @import("antfly_vectorindex").search_results;
const search_types = @import("antfly_vectorindex").search_types;
const lsm_backend = @import("lsm_backend/mod.zig");
const vec = @import("antfly_vector").vector;
const replicated_mod = @import("leann_replicated.zig");
const replica_mod = @import("leann_replica.zig");

pub const file_name = "leann.store";
const magic = "LEANNSTR";
/// Version 2 adds the transient-storage elide list.
const format_version: u32 = 2;

pub const Options = struct {
    dims: u32,
    metric: vec.DistanceMetric,
    max_degree: u32 = 30,
    quantizer: leann.Quantizer = .rabitq,
    /// RaBitQ bits per dimension; 0 picks `leann.Config.default_rabitq_bits`.
    /// Existing graphs keep the width they were built with until rebuilt.
    rabitq_bits: u8 = 0,
    search_ef: u32 = 0,
    bound_scale: f32 = 1.0,
    batch_size: u32 = 0,
    /// Re-embedding time per query before reporting incomplete coverage.
    /// Zero disables the budget.
    reembed_budget_ms: u32 = 0,
    /// Persist through this storage provider (Lite, remote dense storage)
    /// instead of host files. The path is the same logical path.
    storage: ?lsm_backend.Storage = null,
    /// Vectors served by exact search before the first graph build.
    build_threshold: u32 = 256,
    /// Rebuild on sync once this fraction of graph nodes is deleted.
    rebuild_deleted_fraction: f64 = 0.2,
    seed: u64 = 42,
    /// Leader replication: graph changes arrive only as committed records
    /// (see leann_replicated.zig); local writes just mark keys dirty.
    replicated: bool = false,
    /// `vector_storage: transient`: rebuild from RaBitQ codes, and list keys
    /// the graph covers so the owner can replace their stored vectors by
    /// stubs (`pendingElide` / `clearElided`).
    transient: bool = false,
    media_int8: bool = false,
};

pub const ElideEntry = replicated_mod.ElideEntry;
pub const vectorDigest = replica_mod.vectorDigest;
pub const GenerationFetcher = replica_mod.GenerationFetcher;

pub const Filter = @import("leann_filter.zig").Filter;
const RecoverySession = @import("leann_recovery.zig").Session;

/// Exact-vector loading, with an optional per-search recovery context.
pub const VectorLoader = struct {
    /// Preferred during searches; session state is released when the query ends.
    load_batch_session: ?*const fn (*anyopaque, Allocator, []const u64, []const []const u8, []f32, []bool, *u64, *RecoverySession) anyerror!void = null,
    ctx: *anyopaque,
    load: *const fn (ctx: *anyopaque, alloc: Allocator, vector_id: u64, metadata: []const u8) anyerror![]f32,
    /// Optional batch form: fills `out[i * dims ..]` and `found[i]` for each
    /// id. Entries with empty metadata are skipped (left not found).
    load_batch: ?*const fn (ctx: *anyopaque, alloc: Allocator, vector_ids: []const u64, metadata: []const []const u8, out: []f32, found: []bool) anyerror!void = null,
    load_batch_timed: ?*const fn (ctx: *anyopaque, alloc: Allocator, vector_ids: []const u64, metadata: []const []const u8, out: []f32, found: []bool, reembed_ns: *u64) anyerror!void = null,
    /// Validate an indexed digest without re-embedding the source.
    validate_digest: ?*const fn (ctx: *anyopaque, alloc: Allocator, metadata: []const u8, digest: u64) anyerror!bool = null,
};

const ElideNode = struct {
    entry: ElideEntry,
    next: ?*ElideNode = null,
};

const Node = struct {
    vector_id: u64,
    /// Owned. Kept for deleted nodes until the next rebuild is not needed:
    /// deleted nodes are never recomputed, so it is freed on delete.
    metadata: []u8,
};

const Pending = struct {
    vector_id: u64,
    metadata: []u8,
    vector: []f32,
};

pub const Stats = struct {
    live: u64,
    graph_nodes: u64,
    deleted_nodes: u64,
    pending: u64,
};

pub const LeannStore = struct {
    alloc: Allocator,
    io: std.Io,
    opts: Options,
    path: []u8,
    lock: std.Io.RwLock = .init,
    index: ?leann.Index = null,
    nodes: std.ArrayListUnmanaged(Node) = .empty,
    node_of: std.AutoHashMapUnmanaged(u64, u32) = .empty,
    pending: std.ArrayListUnmanaged(Pending) = .empty,
    pending_of: std.AutoHashMapUnmanaged(u64, u32) = .empty,
    loader: ?VectorLoader = null,
    dirty: bool = false,
    /// Transient storage, local mode: keys (owned) and vector digests the
    /// graph covers whose stored vectors may still be full. Persisted with
    /// the store so a crash before the owner elides them loses nothing.
    elide_first: ?*ElideNode = null,
    elide_last: ?*ElideNode = null,
    elide_count: usize = 0,
    /// Entries covered by the last persisted state.
    elide_durable: usize = 0,
    /// Set in replicated mode; the local-mode fields above stay empty.
    replicated: ?*replicated_mod.ReplicatedState = null,
    durable_raft_index: u64 = 0,

    /// Opens the store kept in `dir_path`, loading its file when present.
    pub fn open(alloc: Allocator, io: std.Io, dir_path: []const u8, opts: Options) !*LeannStore {
        if (comptime @import("builtin").os.tag == .freestanding) return error.UnsupportedPlatform;
        if (opts.dims == 0) return error.InvalidIndexConfig;
        // Reject a graph configuration the first build would refuse (for
        // example RaBitQ below two dimensions) before any write is accepted;
        // otherwise derived indexing fails forever on committed data.
        _ = configFromOptions(opts).resolved() catch return error.InvalidIndexConfig;
        const self = try alloc.create(LeannStore);
        errdefer alloc.destroy(self);
        self.* = .{
            .alloc = alloc,
            .io = io,
            .opts = opts,
            .path = try std.fs.path.join(alloc, &.{ dir_path, file_name }),
        };
        errdefer alloc.free(self.path);
        if (opts.replicated) {
            alloc.free(self.path);
            self.path = try std.fs.path.join(alloc, &.{ dir_path, replicated_mod.file_name });
            const state = try alloc.create(replicated_mod.ReplicatedState);
            errdefer alloc.destroy(state);
            const ropts = replicatedOptions(opts);
            const existing = try self.readPersisted();
            defer if (existing) |b| alloc.free(b);
            state.* = if (existing) |b| try replicated_mod.ReplicatedState.decode(alloc, ropts, b) else try replicated_mod.ReplicatedState.init(alloc, ropts);
            self.replicated = state;
            self.durable_raft_index = state.applied_raft_index;
            return self;
        }
        const bytes = (try self.readPersisted()) orelse return self;
        defer alloc.free(bytes);
        self.decode(bytes) catch |err| {
            self.clearState();
            return err;
        };
        return self;
    }

    fn replicatedOptions(opts: Options) replicated_mod.Options {
        return .{
            .replica = .{
                .config = .{
                    .dims = opts.dims,
                    .metric = @fromBackingInt(@intCast(@as(u8, @intCast(@backingInt(opts.metric))))),
                    .quantizer = opts.quantizer,
                    .rabitq_bits = opts.rabitq_bits,
                    .max_degree = opts.max_degree,
                    .seed = opts.seed,
                },
                .build_threshold = opts.build_threshold,
            },
            .rebuild_deleted_fraction = opts.rebuild_deleted_fraction,
            .search_ef = opts.search_ef,
            .bound_scale = opts.bound_scale,
            .batch_size = opts.batch_size,
            .reembed_budget_ms = opts.reembed_budget_ms,
            .transient = opts.transient,
            .media_int8 = opts.media_int8,
        };
    }

    pub fn deinit(self: *LeannStore) void {
        if (self.replicated) |state| {
            state.deinit();
            self.alloc.destroy(state);
        }
        self.clearState();
        self.nodes.deinit(self.alloc);
        self.node_of.deinit(self.alloc);
        self.pending.deinit(self.alloc);
        self.pending_of.deinit(self.alloc);
        self.alloc.free(self.path);
        self.alloc.destroy(self);
    }

    fn clearState(self: *LeannStore) void {
        if (self.index) |*index| index.deinit();
        self.index = null;
        for (self.nodes.items) |node| self.alloc.free(node.metadata);
        self.nodes.clearRetainingCapacity();
        self.node_of.clearRetainingCapacity();
        for (self.pending.items) |p| self.freePending(p);
        self.pending.clearRetainingCapacity();
        self.pending_of.clearRetainingCapacity();
        while (self.elide_first) |node| {
            self.elide_first = node.next;
            self.alloc.free(node.entry.key);
            self.alloc.destroy(node);
        }
        self.elide_last = null;
        self.elide_count = 0;
        self.elide_durable = 0;
    }

    fn noteCovered(self: *LeannStore, key: []const u8, vector: []const f32) !void {
        if (!self.opts.transient or key.len == 0) return;
        const owned = try self.alloc.dupe(u8, key);
        errdefer self.alloc.free(owned);
        try self.appendElideOwned(.{ .key = owned, .digest = replica_mod.vectorDigest(vector) });
    }

    fn appendElideOwned(self: *LeannStore, entry: ElideEntry) !void {
        const node = try self.alloc.create(ElideNode);
        node.* = .{ .entry = entry };
        if (self.elide_last) |last| last.next = node else self.elide_first = node;
        self.elide_last = node;
        self.elide_count += 1;
    }

    /// Replay of a document whose stored vector is already an elided stub:
    /// the graph holds the vector with `digest`. Replicated mode rebinds the
    /// key to `vector_id` and queues a refresh only when the graph lacks that
    /// vector. Local mode elides only nodes its persisted graph holds, so it
    /// has nothing to do.
    pub fn noteElided(self: *LeannStore, vector_id: u64, key: []const u8, digest: u64) !void {
        self.lock.lockUncancelable(self.io);
        defer self.lock.unlock(self.io);
        if (self.replicated) |state| {
            try state.noteElided(key, vector_id, digest);
            self.dirty = true;
        }
    }

    /// Whether replay may rely on the graph for an elided vector. Replicated
    /// mode always can (`noteElided` queues a refresh when needed); local
    /// mode only when the vector id is already a graph or buffer node.
    pub fn holdsElided(self: *LeannStore, vector_id: u64) bool {
        if (self.replicated != null) return true;
        self.lock.lockSharedUncancelable(self.io);
        defer self.lock.unlockShared(self.io);
        return self.node_of.contains(vector_id) or self.pending_of.contains(vector_id);
    }

    /// Transient storage, local mode: copies of the keys whose stored vector
    /// the persisted graph covers (free with `freeElide`). Replicated mode
    /// uses elide records instead.
    pub fn pendingElideAlloc(self: *LeannStore, alloc: Allocator, limit: usize) ![]ElideEntry {
        self.lock.lockSharedUncancelable(self.io);
        defer self.lock.unlockShared(self.io);
        const count = @min(limit, self.elide_durable);
        const out = try alloc.alloc(ElideEntry, count);
        var made: usize = 0;
        errdefer {
            for (out[0..made]) |entry| alloc.free(entry.key);
            alloc.free(out);
        }
        var node = self.elide_first;
        for (out) |*item| {
            const current = node.?;
            item.* = .{ .key = try alloc.dupe(u8, current.entry.key), .digest = current.entry.digest };
            made += 1;
            node = current.next;
        }
        return out;
    }

    /// Covered keys whose stored vectors are waiting to become stubs.
    pub fn pendingElideCount(self: *LeannStore) usize {
        self.lock.lockSharedUncancelable(self.io);
        defer self.lock.unlockShared(self.io);
        return self.elide_durable;
    }

    pub fn freeElide(alloc: Allocator, entries: []ElideEntry) void {
        for (entries) |e| alloc.free(e.key);
        alloc.free(entries);
    }

    /// Drops the first `count` pending entries once the owner has replaced
    /// (or skipped) their stored vectors.
    pub fn clearElided(self: *LeannStore, count: usize) void {
        self.lock.lockUncancelable(self.io);
        defer self.lock.unlock(self.io);
        const n = @min(count, self.elide_durable);
        for (0..n) |_| {
            const node = self.elide_first.?;
            self.elide_first = node.next;
            self.alloc.free(node.entry.key);
            self.alloc.destroy(node);
        }
        self.elide_count -= n;
        self.elide_durable -= n;
        if (self.elide_first == null) self.elide_last = null;
        if (n > 0) self.dirty = true;
    }

    fn freePending(self: *LeannStore, p: Pending) void {
        self.alloc.free(p.metadata);
        self.alloc.free(p.vector);
    }

    pub fn setLoader(self: *LeannStore, loader: VectorLoader) void {
        self.loader = loader;
        if (self.replicated) |state| state.loader = .{ .ctx = loader.ctx, .load = loader.load, .load_batch = loader.load_batch, .load_batch_timed = loader.load_batch_timed, .load_batch_session = loader.load_batch_session, .validate_digest = loader.validate_digest };
    }

    pub fn isReplicated(self: *const LeannStore) bool {
        return self.replicated != null;
    }

    /// Replicated mode, leader only: plans the next record (see
    /// `ReplicatedState.plan`).
    pub fn planRecord(self: *LeannStore, alloc: Allocator) !?replicated_mod.Planned {
        const state = self.replicated orelse return error.NotReplicated;
        self.lock.lockUncancelable(self.io);
        defer self.lock.unlock(self.io);
        return try state.plan(alloc);
    }

    /// Replicated mode: applies the record committed at `raft_index`.
    /// `fetcher` supplies chunked generations; it is used only during this
    /// call, under the store lock.
    pub fn applyRecord(self: *LeannStore, record: []const u8, raft_index: u64, fetcher: ?GenerationFetcher) !replica_mod.Outcome {
        const state = self.replicated orelse return error.NotReplicated;
        self.lock.lockUncancelable(self.io);
        defer self.lock.unlock(self.io);
        state.fetcher = fetcher;
        defer state.fetcher = null;
        const outcome = try state.applyRecord(record, raft_index);
        self.dirty = true;
        return outcome;
    }

    /// Replicated mode: write sequence a `full_index` caller must see
    /// covered by committed records (see `graphCovers`). Null in local mode,
    /// where writes reach the graph during derived replay.
    pub fn coverageTarget(self: *LeannStore) ?u64 {
        const state = self.replicated orelse return null;
        self.lock.lockSharedUncancelable(self.io);
        defer self.lock.unlockShared(self.io);
        return state.coverageTarget();
    }

    /// Replicated mode: whether committed records cover every write up to
    /// `target`, so those vectors are searchable on this replica.
    pub fn graphCovers(self: *LeannStore, target: u64) bool {
        const state = self.replicated orelse return true;
        self.lock.lockSharedUncancelable(self.io);
        defer self.lock.unlockShared(self.io);
        return state.covers(target);
    }

    /// Replicated mode: advances past a record of another incarnation.
    pub fn skipRecord(self: *LeannStore, raft_index: u64) void {
        const state = self.replicated orelse return;
        self.lock.lockUncancelable(self.io);
        defer self.lock.unlock(self.io);
        state.skipRecord(raft_index);
        self.dirty = true;
    }

    /// Replicated mode: Raft index of the last applied record (0 if none).
    pub fn appliedRaftIndex(self: *LeannStore) u64 {
        const state = self.replicated orelse return 0;
        self.lock.lockSharedUncancelable(self.io);
        defer self.lock.unlockShared(self.io);
        return state.applied_raft_index;
    }

    /// Replicated mode: reports the lowest chunk Raft index read while
    /// fetching a generation (see `ReplicatedState.generation_floor`).
    pub fn noteFetchFloor(self: *LeannStore, raft_index: u64) void {
        const state = self.replicated orelse return;
        state.last_fetch_floor = if (state.last_fetch_floor) |f| @min(f, raft_index) else raft_index;
    }

    /// Replicated mode: earliest Raft index still needed for catch-up, and
    /// the current generation.
    pub fn generationFloor(self: *LeannStore) struct { floor: u64, generation: u64 } {
        const state = self.replicated orelse return .{ .floor = 0, .generation = 0 };
        self.lock.lockSharedUncancelable(self.io);
        defer self.lock.unlockShared(self.io);
        return .{ .floor = state.generation_floor, .generation = state.replica.generation };
    }

    /// Replicated mode: Raft index durably persisted by the last `sync`.
    pub fn durableRaftIndex(self: *LeannStore) u64 {
        self.lock.lockSharedUncancelable(self.io);
        defer self.lock.unlockShared(self.io);
        return self.durable_raft_index;
    }

    pub fn stats(self: *LeannStore) Stats {
        self.lock.lockSharedUncancelable(self.io);
        defer self.lock.unlockShared(self.io);
        return self.statsLocked();
    }

    fn statsLocked(self: *const LeannStore) Stats {
        if (self.replicated) |state| {
            const st = state.stats();
            return .{ .live = st.live, .graph_nodes = st.graph_nodes, .deleted_nodes = st.deleted_nodes, .pending = st.dirty };
        }
        const graph_nodes: u64 = self.nodes.items.len;
        const deleted: u64 = if (self.index) |*index| index.deleted_count else 0;
        return .{
            .live = self.node_of.count() + self.pending.items.len,
            .graph_nodes = graph_nodes,
            .deleted_nodes = deleted,
            .pending = self.pending.items.len,
        };
    }

    pub fn liveCount(self: *LeannStore) u64 {
        return self.stats().live;
    }

    // ---------------------------------------------------------------------
    // Writes
    // ---------------------------------------------------------------------

    /// Applies upserts and deletes. `items` are HBC BatchInsertItem-shaped
    /// (`vector_id`, `vector`, `metadata`). Re-applying the same batch is a
    /// no-op in effect, which makes replay after a crash safe.
    pub fn apply(self: *LeannStore, items: anytype, deletes: []const u64) !void {
        self.lock.lockUncancelable(self.io);
        defer self.lock.unlock(self.io);
        if (self.replicated) |state| {
            try state.noteWrites(items, deletes);
            if (deletes.len > 0 or items.len > 0) self.dirty = true;
            return;
        }
        for (deletes) |vector_id| try self.deleteLocked(vector_id);
        for (items) |item| try self.upsertLocked(item.vector_id, item.vector, item.metadata);
        if (deletes.len > 0 or items.len > 0) self.dirty = true;
    }

    fn deleteLocked(self: *LeannStore, vector_id: u64) !void {
        if (self.node_of.fetchRemove(vector_id)) |kv| {
            const node = &self.nodes.items[kv.value];
            self.alloc.free(node.metadata);
            node.metadata = &.{};
            try self.index.?.markDeleted(kv.value);
            return;
        }
        if (self.pending_of.fetchRemove(vector_id)) |kv| {
            const slot = kv.value;
            self.freePending(self.pending.items[slot]);
            _ = self.pending.swapRemove(slot);
            if (slot < self.pending.items.len) {
                try self.pending_of.put(self.alloc, self.pending.items[slot].vector_id, slot);
            }
        }
    }

    fn upsertLocked(self: *LeannStore, vector_id: u64, vector: []const f32, metadata: []const u8) !void {
        if (vector.len != self.opts.dims) return error.DimensionMismatch;
        try self.deleteLocked(vector_id);
        if (self.index) |*index| {
            const owned_meta = try self.alloc.dupe(u8, metadata);
            const id = blk: {
                // Owned here only until the node takes it below.
                errdefer self.alloc.free(owned_meta);
                try self.nodes.ensureUnusedCapacity(self.alloc, 1);
                try self.node_of.ensureUnusedCapacity(self.alloc, 1);
                var ctx = SourceCtx{ .store = self, .far_from = vector };
                break :blk try index.insert(ctx.source(), vector);
            };
            std.debug.assert(id == self.nodes.items.len);
            self.nodes.appendAssumeCapacity(.{ .vector_id = vector_id, .metadata = owned_meta });
            self.node_of.putAssumeCapacity(vector_id, id);
            // The node owns the metadata now; a failure here leaves it indexed.
            try self.noteCovered(metadata, vector);
            return;
        }
        {
            // Owned here only until the pending entry takes them; after that a
            // failed graph build leaves them buffered, not freed.
            const owned_meta = try self.alloc.dupe(u8, metadata);
            errdefer self.alloc.free(owned_meta);
            const owned_vec = try self.alloc.dupe(f32, vector);
            errdefer self.alloc.free(owned_vec);
            try self.pending_of.put(self.alloc, vector_id, @intCast(self.pending.items.len));
            errdefer _ = self.pending_of.remove(vector_id);
            try self.pending.append(self.alloc, .{ .vector_id = vector_id, .metadata = owned_meta, .vector = owned_vec });
        }
        if (self.pending.items.len >= self.opts.build_threshold) try self.buildFromPendingLocked();
    }

    fn leannConfig(self: *const LeannStore) leann.Config {
        return configFromOptions(self.opts);
    }

    fn configFromOptions(opts: Options) leann.Config {
        return .{
            .dims = opts.dims,
            .metric = @fromBackingInt(@intCast(@as(u8, @intCast(@backingInt(opts.metric))))),
            .quantizer = opts.quantizer,
            .rabitq_bits = opts.rabitq_bits,
            .max_degree = opts.max_degree,
            .seed = opts.seed,
        };
    }

    /// Builds the graph from the exact buffer, then drops the buffered
    /// vectors.
    fn buildFromPendingLocked(self: *LeannStore) !void {
        std.debug.assert(self.index == null);
        const n = self.pending.items.len;
        if (n == 0) return;
        const d: usize = self.opts.dims;
        const flat = try self.alloc.alloc(f32, n * d);
        defer self.alloc.free(flat);
        for (self.pending.items, 0..) |p, i| @memcpy(flat[i * d ..][0..d], p.vector);
        var index = try leann.Index.build(self.alloc, self.leannConfig(), flat);
        errdefer index.deinit();

        try self.nodes.ensureTotalCapacity(self.alloc, n);
        try self.node_of.ensureTotalCapacity(self.alloc, @intCast(n));
        for (self.pending.items) |p| try self.noteCovered(p.metadata, p.vector);
        for (self.pending.items, 0..) |p, i| {
            self.nodes.appendAssumeCapacity(.{ .vector_id = p.vector_id, .metadata = p.metadata });
            self.node_of.putAssumeCapacity(p.vector_id, @intCast(i));
            self.alloc.free(p.vector);
        }
        self.pending.clearRetainingCapacity();
        self.pending_of.clearRetainingCapacity();
        self.index = index;
    }

    /// Rebuilds the graph from the live nodes' stored vectors. Falls back to
    /// the exact buffer when too few remain for a graph.
    fn rebuildLocked(self: *LeannStore) !void {
        if (self.opts.transient) return self.rebuildFromCodesLocked();
        const loader = self.loader orelse return;
        const d: usize = self.opts.dims;
        var live = std.ArrayListUnmanaged(Pending).empty;
        defer {
            for (live.items) |p| self.freePending(p);
            live.deinit(self.alloc);
        }
        for (self.nodes.items, 0..) |node, i| {
            if (self.index.?.isDeleted(@intCast(i))) continue;
            const v = loader.load(loader.ctx, self.alloc, node.vector_id, node.metadata) catch |err| switch (err) {
                error.NotFound => continue,
                else => return err,
            };
            errdefer self.alloc.free(v);
            if (v.len != d) return error.DimensionMismatch;
            const meta = try self.alloc.dupe(u8, node.metadata);
            errdefer self.alloc.free(meta);
            try live.append(self.alloc, .{ .vector_id = node.vector_id, .metadata = meta, .vector = v });
        }

        // Reserve before touching the old graph, so the hand-over below cannot
        // fail halfway: a buffer is owned either by `live` or by `pending`,
        // never both.
        try self.pending.ensureUnusedCapacity(self.alloc, live.items.len);
        try self.pending_of.ensureUnusedCapacity(self.alloc, @intCast(live.items.len));
        // Swap in the rebuilt state only after every vector loaded.
        if (self.index) |*index| index.deinit();
        self.index = null;
        for (self.nodes.items) |node| self.alloc.free(node.metadata);
        self.nodes.clearRetainingCapacity();
        self.node_of.clearRetainingCapacity();
        for (live.items) |p| {
            self.pending_of.putAssumeCapacity(p.vector_id, @intCast(self.pending.items.len));
            self.pending.appendAssumeCapacity(p);
        }
        live.clearRetainingCapacity();
        // A failed build leaves the vectors buffered (and searchable).
        if (self.pending.items.len >= self.opts.build_threshold) try self.buildFromPendingLocked();
    }

    /// Transient storage: rebuilds the graph from the live nodes' RaBitQ
    /// codes, so no stored vector is needed.
    fn rebuildFromCodesLocked(self: *LeannStore) !void {
        const old = &(self.index orelse return);
        var live = std.ArrayListUnmanaged(u32).empty;
        defer live.deinit(self.alloc);
        for (self.nodes.items, 0..) |_, i| if (!old.isDeleted(@intCast(i))) try live.append(self.alloc, @intCast(i));
        if (live.items.len == 0) return;
        var rebuilt = try old.rebuildFromCodes(self.alloc, live.items, null);
        errdefer rebuilt.deinit();
        var nodes = try std.ArrayListUnmanaged(Node).initCapacity(self.alloc, live.items.len);
        errdefer nodes.deinit(self.alloc);
        var node_of = std.AutoHashMapUnmanaged(u64, u32).empty;
        errdefer node_of.deinit(self.alloc);
        try node_of.ensureTotalCapacity(self.alloc, @intCast(live.items.len));
        for (live.items, 0..) |old_id, new_id| {
            const node = self.nodes.items[old_id];
            nodes.appendAssumeCapacity(node);
            node_of.putAssumeCapacity(node.vector_id, @intCast(new_id));
        }
        // Commit: free deleted nodes' (empty) metadata, swap everything in.
        for (self.nodes.items, 0..) |node, i| if (old.isDeleted(@intCast(i))) self.alloc.free(node.metadata);
        old.deinit();
        self.index = rebuilt;
        self.nodes.deinit(self.alloc);
        self.nodes = nodes;
        self.node_of.deinit(self.alloc);
        self.node_of = node_of;
    }

    // ---------------------------------------------------------------------
    // Search
    // ---------------------------------------------------------------------

    /// Answers an HBC search request with HBC's result type, so callers map
    /// vector ids and metadata exactly as they do for HBC. Results are sorted
    /// nearest first, as HBC returns them: `SearchResults` is a heap that
    /// keeps the worst result first until sorted.
    pub fn search(self: *LeannStore, alloc: Allocator, req: search_types.SearchRequest) !search_results.SearchResults {
        return self.searchWithStats(alloc, req, null);
    }

    pub fn searchWithStats(self: *LeannStore, alloc: Allocator, req: search_types.SearchRequest, profile_out: ?*leann.SearchStats) !search_results.SearchResults {
        return self.searchWithWorkingAllocatorAndStats(alloc, alloc, req, profile_out);
    }

    pub fn searchWithWorkingAllocator(self: *LeannStore, result_alloc: Allocator, alloc: Allocator, req: search_types.SearchRequest) !search_results.SearchResults {
        return self.searchWithWorkingAllocatorAndStats(result_alloc, alloc, req, null);
    }

    pub fn searchWithWorkingAllocatorAndStats(self: *LeannStore, result_alloc: Allocator, alloc: Allocator, req: search_types.SearchRequest, search_stats: ?*leann.SearchStats) !search_results.SearchResults {
        if (search_stats) |profile| profile.* = .{};
        if (req.query.len != self.opts.dims) return error.DimensionMismatch;
        if (req.cancellation) |token| if (token.isCancelled()) return error.Cancelled;
        if (req.deadline_ns) |deadline| if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.Timeout;
        if (!self.opts.transient) {
            try self.acquireSearchRead(req);
            defer self.lock.unlockShared(self.io);
            var result = if (self.replicated) |state| try state.searchWithStats(result_alloc, req, search_stats) else try self.searchLocalLocked(result_alloc, req, search_stats);
            result.sort();
            return result;
        }
        if (req.cancellation) |token| if (token.isCancelled()) return error.Cancelled;
        if (req.deadline_ns) |deadline| if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.Timeout;
        var filter = try Filter.init(alloc, req);
        defer filter.deinit(alloc);
        var recovery: RecoverySession = .{
            .cancellation = req.cancellation,
            .source_read = if (req.external_work) |work| work.read else null,
            .execution = req.external_work,
            .deadline_ns = req.deadline_ns,
            .budget_ns = if (self.opts.transient) @as(u64, self.opts.reembed_budget_ms) * std.time.ns_per_ms else 0,
        };
        defer recovery.deinit();
        const accounting = if (req.external_work) |work| work.reembed_elapsed_ns else null;
        if (accounting) |elapsed| recovery.elapsed_ns = elapsed.*;
        defer if (accounting) |elapsed| {
            elapsed.* = recovery.elapsed_ns;
        };
        const session_loader = if (self.loader) |loader| loader.load_batch_session != null else false;
        var gate = SearchGate.init(req, if (session_loader or !self.opts.transient) 0 else self.opts.reembed_budget_ms);
        var want: usize = if (filter.distanceActive() and !search_types.requiresExhaustiveCoverage(req)) @max(req.k *| 4, req.k +| 64) else req.k;
        while (true) {
            try recovery.check();
            var captured = try self.captureSearch(result_alloc, alloc, req, &filter, want, &gate);
            defer captured.deinit(alloc);
            var source = CapturedSource{
                .alloc = alloc,
                .dims = self.opts.dims,
                .loader = captured.loader,
                .rows = captured.rows.items,
                .strict = search_types.requiresExhaustiveCoverage(req),
                .session = &recovery,
                .reembed_ns = &gate.reembed_ns,
            };
            if (captured.plan) |*plan| {
                const response = score: {
                    if (req.external_work) |work| try work.enter(work.ptr);
                    defer if (req.external_work) |work| work.leave(work.ptr);
                    break :score try plan.score(alloc, .{ .ctx = &source, .recompute_fn = CapturedSource.recompute, .result_order = CapturedSource.order }, req.query, captured.options);
                };
                var scored = response;
                defer scored.deinit(alloc);
                if (req.external_work) |work| try work.validate(work.ptr);
                var profile = scored.stats;
                profile.matches.verified += captured.pending_matches;
                profile.matches.complete = profile.matches.complete and captured.direct;
                if (search_stats) |output| output.* = profile;
                if (scored.stats.budget_exhausted) return error.ReembedBudgetExceeded;
                for (scored.results) |result| {
                    const row = source.row(result.id) orelse return error.InvalidLeannStore;
                    if (!filter.distanceAllowed(result.distance)) continue;
                    captured.results.addResultWithOwnedMetadata(row.vector_id, result.distance, 0, if (req.load_metadata) try result_alloc.dupe(u8, row.metadata) else null);
                }
                captured.results.candidate_coverage = profile.matches.candidateCoverage(captured.results.getHits().len);
            }
            if (search_types.requiresExhaustiveCoverage(req) or captured.results.items.items.len >= req.k or
                want >= captured.live or !filter.distanceActive() or captured.truncated)
            {
                var results = captured.results;
                captured.results = search_results.SearchResults.init(result_alloc, 0);
                results.sort();
                return results;
            }
            want = @min(want *| 4, captured.live);
        }
    }

    const CapturedRow = struct {
        node_id: u32,
        vector_id: u64,
        metadata: []u8,

        fn less(_: void, a: @This(), b: @This()) bool {
            return a.node_id < b.node_id;
        }
    };
    const CapturedSearch = struct {
        plan: ?leann.PreparedSearch = null,
        options: leann.SearchOptions = .{},
        rows: std.ArrayListUnmanaged(CapturedRow) = .empty,
        results: search_results.SearchResults,
        live: usize = 0,
        truncated: bool = false,
        direct: bool = false,
        pending_matches: usize = 0,
        loader: ?VectorLoader = null,

        fn deinit(self: *@This(), alloc: Allocator) void {
            if (self.plan) |*plan| plan.deinit(alloc);
            for (self.rows.items) |row| alloc.free(row.metadata);
            self.rows.deinit(alloc);
            self.results.deinit();
        }
    };

    fn acquireSearchRead(self: *LeannStore, req: search_types.SearchRequest) !void {
        while (!self.lock.tryLockShared(self.io)) {
            if (req.cancellation) |token| if (token.isCancelled()) return error.Cancelled;
            if (req.deadline_ns) |deadline| if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.Timeout;
            try self.io.sleep(.fromMilliseconds(1), .awake);
        }
    }

    fn captureSearch(self: *LeannStore, result_alloc: Allocator, alloc: Allocator, req: search_types.SearchRequest, filter: *Filter, want: usize, gate: *SearchGate) !CapturedSearch {
        try self.acquireSearchRead(req);
        defer self.lock.unlockShared(self.io);
        var captured = CapturedSearch{ .results = search_results.SearchResults.init(result_alloc, req.k), .loader = self.loader };
        errdefer captured.deinit(alloc);
        if (req.k == 0) return captured;
        const metric: leann.Metric = @fromBackingInt(@intCast(@backingInt(self.opts.metric)));
        const query = try alloc.dupe(f32, req.query);
        defer alloc.free(query);
        if (metric == .cosine) normalize(query);
        const scratch = try alloc.alloc(f32, self.opts.dims);
        defer alloc.free(scratch);
        var buffered: std.ArrayListUnmanaged(CapturedRow) = .empty;
        defer {
            for (buffered.items) |row| alloc.free(row.metadata);
            buffered.deinit(alloc);
        }
        const graph: ?*const leann.Index = if (self.replicated) |state|
            if (state.replica.index) |*index| index else null
        else if (self.index) |*index| index else null;
        if (self.replicated) |state| {
            for (state.replica.buffer_keys.items, state.replica.buffer_vectors.items) |key, vector| {
                const id = state.bindings.get(key) orelse continue;
                if (!try filter.allows(id, key)) continue;
                if (state.dirty.contains(key)) {
                    const owned = try alloc.dupe(u8, key);
                    errdefer alloc.free(owned);
                    try buffered.append(alloc, .{ .node_id = 0, .vector_id = id, .metadata = owned });
                    continue;
                }
                @memcpy(scratch, vector);
                if (metric == .cosine) normalize(scratch);
                const dist = leann.distance(metric, query, scratch);
                if (filter.distanceAllowed(dist)) {
                    captured.pending_matches += 1;
                    captured.results.addResultWithOwnedMetadata(id, dist, 0, if (req.load_metadata) try result_alloc.dupe(u8, key) else null);
                }
            }
        } else {
            for (self.pending.items) |pending| {
                if (!try filter.allows(pending.vector_id, pending.metadata)) continue;
                @memcpy(scratch, pending.vector);
                if (metric == .cosine) normalize(scratch);
                const dist = leann.distance(metric, query, scratch);
                if (filter.distanceAllowed(dist)) {
                    captured.pending_matches += 1;
                    captured.results.addResultWithOwnedMetadata(pending.vector_id, dist, 0, if (req.load_metadata) try result_alloc.dupe(u8, pending.metadata) else null);
                }
            }
        }
        if (graph == null and buffered.items.len == 0) return captured;
        const index = graph;

        const NodePredicate = struct {
            store: *LeannStore,
            filter: *Filter,

            fn allows(raw: *const anyopaque, id: u32) !bool {
                const self_: *const @This() = @ptrCast(@alignCast(raw));
                if (self_.store.replicated) |state| {
                    const key = state.replica.keys.items[id];
                    const vector_id = state.bindings.get(key) orelse return false;
                    return self_.filter.allows(vector_id, key);
                }
                const node = self_.store.nodes.items[id];
                return self_.filter.allows(node.vector_id, node.metadata);
            }
        };
        var predicate = NodePredicate{ .store = self, .filter = filter };
        captured.live = if (self.replicated) |state| state.replica.liveCount() else self.node_of.count();
        var candidates: std.ArrayListUnmanaged(u32) = .empty;
        defer candidates.deinit(alloc);
        const direct = (filter.included_count != null) and (search_types.requiresExhaustiveCoverage(req) or filter.directMode(self.opts.dims, captured.live, true, if (index) |value| value.rabitq != null else false) != .graph);
        captured.direct = direct or search_types.requiresExhaustiveCoverage(req);
        if (direct) {
            var ids = filter.ids.include.keyIterator();
            while (ids.next()) |id| {
                if (self.replicated) |state| {
                    const key = state.key_of.get(id.*) orelse continue;
                    if (state.replica.node_of.get(key)) |node| try candidates.append(alloc, node);
                } else if (self.node_of.get(id.*)) |node| try candidates.append(alloc, node);
            }
        }
        captured.options = .{
            .k = @intCast(@min(want, captured.live)),
            .ef = if (search_types.requiresExhaustiveCoverage(req)) @intCast(captured.live) else self.opts.search_ef,
            .bound_scale = self.opts.bound_scale,
            .batch_size = self.opts.batch_size,
            .filter = if (self.replicated != null or filter.nodeActive() or search_types.requiresExhaustiveCoverage(req)) .{ .ctx = &predicate, .allows_fn = NodePredicate.allows } else null,
            .candidate_ids = if (direct) candidates.items else null,
            .filter_visit_limit = if (!search_types.requiresExhaustiveCoverage(req) and (self.replicated != null or filter.nodeActive())) @max(65536, @as(u32, @intCast(@min(req.k *| 16, std.math.maxInt(u32))))) else 0,
            .exhaustive = search_types.requiresExhaustiveCoverage(req),
            .distance_over = req.distance_over,
            .distance_under = req.distance_under,
            .score_quantized_source = self.opts.media_int8,
            .verify_source = true,
            .gate = gate.recomputeGate(),
        };
        captured.plan = if (index) |value| try value.prepareSearch(alloc, req.query, captured.options) else .{
            .candidates = try alloc.alloc(leann.PreparedCandidate, 0),
            .metric = metric,
            .dims = self.opts.dims,
            .has_bounds = false,
        };
        captured.truncated = captured.plan.?.stats.filter_truncated;
        for (captured.plan.?.candidates) |*candidate| {
            const vector_id, const metadata = if (self.replicated) |state| row: {
                const key = state.replica.keys.items[candidate.id];
                if (state.dirty.contains(key)) {
                    candidate.lower = -std.math.inf(f32);
                    candidate.retained = false;
                }
                break :row .{ state.bindings.get(key) orelse return error.InvalidLeannStore, key };
            } else row: {
                const node = self.nodes.items[candidate.id];
                break :row .{ node.vector_id, node.metadata };
            };
            const owned = try alloc.dupe(u8, metadata);
            errdefer alloc.free(owned);
            try captured.rows.append(alloc, .{ .node_id = candidate.id, .vector_id = vector_id, .metadata = owned });
        }
        // Dirty exact-buffer rows also use the pinned current source. Their
        // synthetic IDs live only inside this plan, beyond every graph node.
        const graph_count = if (index) |value| value.node_count else 0;
        const original_count = captured.plan.?.candidates.len;
        captured.plan.?.candidates = try alloc.realloc(captured.plan.?.candidates, original_count + buffered.items.len);
        try captured.rows.ensureUnusedCapacity(alloc, buffered.items.len);
        if (@as(usize, graph_count) + buffered.items.len > std.math.maxInt(u32)) return error.InvalidLeannStore;
        for (buffered.items, 0..) |*row, offset| {
            row.node_id = @intCast(@as(usize, graph_count) + offset);
            captured.plan.?.candidates[original_count + offset] = .{ .id = row.node_id, .lower = -std.math.inf(f32), .est = 0 };
            captured.rows.appendAssumeCapacity(row.*);
        }
        buffered.clearRetainingCapacity();
        captured.direct = captured.direct or captured.plan.?.candidates.len == captured.live;
        std.mem.sortUnstable(CapturedRow, captured.rows.items, {}, CapturedRow.less);
        // All pointers into the graph and stack predicates end at preparation.
        captured.options.filter = null;
        captured.options.candidate_ids = null;
        return captured;
    }

    const CapturedSource = struct {
        alloc: Allocator,
        dims: u32,
        loader: ?VectorLoader,
        rows: []const CapturedRow,
        strict: bool,
        session: *RecoverySession,
        reembed_ns: *u64,

        fn row(self: *const @This(), id: u32) ?CapturedRow {
            var lo: usize = 0;
            var hi = self.rows.len;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                if (self.rows[mid].node_id < id) lo = mid + 1 else hi = mid;
            }
            return if (lo < self.rows.len and self.rows[lo].node_id == id) self.rows[lo] else null;
        }

        fn order(raw: *anyopaque, id: u32) u64 {
            const self_: *@This() = @ptrCast(@alignCast(raw));
            return self_.row(id).?.vector_id;
        }

        fn recompute(raw: *anyopaque, ids: []const u32, out: []f32) !void {
            const self_: *@This() = @ptrCast(@alignCast(raw));
            try self_.session.check();
            const loader = self_.loader orelse return error.MissingVectorLoader;
            const vector_ids = try self_.alloc.alloc(u64, ids.len);
            defer self_.alloc.free(vector_ids);
            const metadata = try self_.alloc.alloc([]const u8, ids.len);
            defer self_.alloc.free(metadata);
            const found = try self_.alloc.alloc(bool, ids.len);
            defer self_.alloc.free(found);
            @memset(found, false);
            for (ids, vector_ids, metadata) |id, *vector_id, *key| {
                const item = self_.row(id) orelse return error.InvalidLeannStore;
                vector_id.* = item.vector_id;
                key.* = item.metadata;
            }
            if (loader.load_batch_session) |load| {
                try load(loader.ctx, self_.alloc, vector_ids, metadata, out, found, self_.reembed_ns, self_.session);
            } else if (loader.load_batch_timed) |load| {
                try load(loader.ctx, self_.alloc, vector_ids, metadata, out, found, self_.reembed_ns);
            } else if (loader.load_batch) |load| {
                try load(loader.ctx, self_.alloc, vector_ids, metadata, out, found);
            } else for (vector_ids, metadata, 0..) |id, key, offset| {
                const vector = loader.load(loader.ctx, self_.alloc, id, key) catch |err| switch (err) {
                    error.NotFound => continue,
                    else => return err,
                };
                defer self_.alloc.free(vector);
                if (vector.len != self_.dims) return error.DimensionMismatch;
                @memcpy(out[offset * self_.dims ..][0..self_.dims], vector);
                found[offset] = true;
            }
            try self_.session.check();
            // The loader has already cached this completed batch. Stop a
            // superseded attempt before dispatching any remaining candidates.
            if (self_.session.execution) |work| try work.validate(work.ptr);
            for (found, 0..) |exists, offset| if (!exists) {
                if (self_.strict) return error.IncompletePublishedSnapshot;
                @memset(out[offset * self_.dims ..][0..self_.dims], std.math.nan(f32));
            };
        }
    };

    fn searchLocalLocked(self: *LeannStore, alloc: Allocator, req: search_types.SearchRequest, search_stats: ?*leann.SearchStats) !search_results.SearchResults {
        var out = search_results.SearchResults.init(alloc, req.k);
        errdefer out.deinit();
        if (req.k == 0) return out;
        if (req.cancellation) |token| if (token.isCancelled()) return error.Cancelled;

        var filter = try Filter.init(alloc, req);
        defer filter.deinit(alloc);

        const q = try alloc.dupe(f32, req.query);
        defer alloc.free(q);
        if (self.opts.metric == .cosine) normalize(q);

        // Exact buffer.
        const scratch = try alloc.alloc(f32, self.opts.dims);
        defer alloc.free(scratch);
        const direct_mode = filter.directMode(self.opts.dims, self.node_of.count() + self.pending.items.len, self.opts.transient, self.opts.quantizer == .rabitq);
        const direct = direct_mode != .graph;
        const graph_ids: []u32 = if (direct) try alloc.alloc(u32, filter.included_count.?) else &.{};
        defer if (direct) alloc.free(graph_ids);
        var graph_count: usize = 0;
        var pending_matches: usize = 0;
        var matches: leann.MatchCoverage = .{ .complete = true };
        if (direct) {
            var ids = filter.ids.include.keyIterator();
            while (ids.next()) |id| {
                if (filter.ids.exclude.contains(id.*)) continue;
                if (self.node_of.get(id.*)) |node_id| {
                    graph_ids[graph_count] = node_id;
                    graph_count += 1;
                }
                if (self.pending_of.get(id.*)) |slot| {
                    pending_matches += @intFromBool(try self.scorePending(alloc, &filter, q, scratch, slot, &out));
                }
            }
            std.mem.sortUnstable(u32, graph_ids[0..graph_count], {}, std.sort.asc(u32));
        } else {
            for (0..self.pending.items.len) |slot| {
                pending_matches += @intFromBool(try self.scorePending(alloc, &filter, q, scratch, slot, &out));
            }
        }

        if (self.index) |*index| {
            var ctx = SourceCtx{ .store = self, .far_from = if (filter.active()) null else req.query };
            var gate = SearchGate.init(req, 0);
            const opts: leann.SearchOptions = .{
                .k = std.math.cast(u32, req.k) orelse std.math.maxInt(u32),
                .ef = self.opts.search_ef,
                .bound_scale = self.opts.bound_scale,
                .batch_size = self.opts.batch_size,
                .gate = gate.recomputeGate(),
                .exhaustive = search_types.requiresExhaustiveCoverage(req),
                .distance_over = req.distance_over,
                .distance_under = req.distance_under,
                .verify_source = true,
            };
            const FilterContext = struct {
                store: *const LeannStore,
                filter: *const Filter,

                fn allows(raw: *const anyopaque, id: u32) !bool {
                    const context: *const @This() = @ptrCast(@alignCast(raw));
                    const node = context.store.nodes.items[id];
                    return context.filter.allows(node.vector_id, node.metadata);
                }

                fn order(raw: *const anyopaque, id: u32) u64 {
                    const context: *const @This() = @ptrCast(@alignCast(raw));
                    return context.store.nodes.items[id].vector_id;
                }

                fn cancelled(raw: *const anyopaque) bool {
                    const context: *const @This() = @ptrCast(@alignCast(raw));
                    return context.filter.cancelled();
                }
            };
            const filter_ctx = FilterContext{ .store = self, .filter = &filter };
            var node_filter = NodeFilter{ .store = self, .filter = &filter };
            var resp = if (search_types.requiresExhaustiveCoverage(req)) full: {
                var full_opts = opts;
                full_opts.filter = .{ .ctx = &node_filter, .allows_fn = NodeFilter.allows };
                ctx.strict = true;
                break :full try index.search(alloc, ctx.source(), req.query, full_opts);
            } else if (filter.active())
                try index.searchFiltered(alloc, ctx.source(), req.query, opts, .{
                    .ctx = &filter_ctx,
                    .allows = FilterContext.allows,
                    .result_order = FilterContext.order,
                    .cancelled = FilterContext.cancelled,
                    .eligible_count = filter.eligibleCount(),
                    .direct_verify_bounds = direct_mode == .bounds,
                    .distance_over = req.distance_over,
                    .distance_under = req.distance_under,
                }, if (direct) graph_ids[0..graph_count] else null)
            else
                try index.search(alloc, ctx.source(), req.query, opts);
            defer resp.deinit(alloc);
            matches = resp.stats.matches;
            if (search_stats) |profile| profile.* = resp.stats;
            for (resp.results) |result| {
                const node = self.nodes.items[result.id];
                out.addResultWithOwnedMetadata(node.vector_id, result.distance, 0, if (req.load_metadata) try alloc.dupe(u8, node.metadata) else null);
            }
        }
        matches.verified += pending_matches;
        if (filter.active()) out.candidate_coverage = matches.candidateCoverage(out.getHits().len);
        out.sort();
        return out;
    }

    fn scorePending(self: *const LeannStore, alloc: Allocator, filter: *const Filter, q: []const f32, scratch: []f32, slot: usize, out: *search_results.SearchResults) !bool {
        if (filter.cancelled()) return error.Cancelled;
        const item = self.pending.items[slot];
        if (!try filter.allows(item.vector_id, item.metadata)) return false;
        @memcpy(scratch, item.vector);
        if (self.opts.metric == .cosine) normalize(scratch);
        const dist = leann.distance(@fromBackingInt(@intCast(@as(u8, @intCast(@backingInt(self.opts.metric))))), q, scratch);
        if (!std.math.isFinite(dist) or !filter.distanceAllowed(dist)) return false;
        out.addResultWithOwnedMetadata(item.vector_id, dist, 0, if (filter.request.load_metadata) try alloc.dupe(u8, item.metadata) else null);
        return true;
    }

    /// Metadata for a live vector id, owned by `alloc`.
    pub fn dropKeysWhere(self: *LeannStore, ctx: anytype, comptime keep: fn (@TypeOf(ctx), []const u8) bool) !usize {
        if (self.replicated) |state| {
            self.lock.lockUncancelable(self.io);
            defer self.lock.unlock(self.io);
            const count = state.dropKeysWhere(ctx, keep) catch |err| {
                self.dirty = true; // A partial mutation must still be persisted.
                return err;
            };
            if (count != 0) self.dirty = true;
            return count;
        }
        var ids = std.ArrayListUnmanaged(u64).empty;
        defer ids.deinit(self.alloc);
        {
            self.lock.lockSharedUncancelable(self.io);
            defer self.lock.unlockShared(self.io);
            var nodes = self.node_of.iterator();
            while (nodes.next()) |e| {
                if (!keep(ctx, self.nodes.items[e.value_ptr.*].metadata)) try ids.append(self.alloc, e.key_ptr.*);
            }
            for (self.pending.items) |p| if (!keep(ctx, p.metadata)) try ids.append(self.alloc, p.vector_id);
        }
        if (ids.items.len == 0) return 0;
        const no_writes: []const struct { vector_id: u64, vector: []const f32, metadata: []const u8 } = &.{};
        try self.apply(no_writes, ids.items);
        return ids.items.len;
    }

    /// Metadata for a live vector id, owned by `alloc`.
    pub fn getMetadata(self: *LeannStore, alloc: Allocator, vector_id: u64) !?[]u8 {
        self.lock.lockSharedUncancelable(self.io);
        defer self.lock.unlockShared(self.io);
        if (self.replicated) |state| return try state.getMetadata(alloc, vector_id);
        if (self.node_of.get(vector_id)) |id| return try alloc.dupe(u8, self.nodes.items[id].metadata);
        if (self.pending_of.get(vector_id)) |slot| return try alloc.dupe(u8, self.pending.items[slot].metadata);
        return null;
    }

    /// EmbeddingSource over the vector loader. Deleted or missing nodes get a
    /// vector pointing away from `far_from`, which puts them at the far end
    /// of every metric instead of failing the search.
    const SourceCtx = struct {
        store: *LeannStore,
        far_from: ?[]const f32,
        reembed_ns: ?*u64 = null,
        strict: bool = false,
        recovery: ?*RecoverySession = null,

        fn source(self: *SourceCtx) leann.EmbeddingSource {
            return .{ .ctx = self, .recompute_fn = recompute };
        }

        fn recompute(raw: *anyopaque, ids: []const u32, out: []f32) anyerror!void {
            const self: *SourceCtx = @ptrCast(@alignCast(raw));
            const store = self.store;
            const d: usize = store.opts.dims;
            const loader = store.loader orelse return error.MissingVectorLoader;
            if (loader.load_batch != null or loader.load_batch_timed != null or loader.load_batch_session != null) {
                const vector_ids = try store.alloc.alloc(u64, ids.len);
                defer store.alloc.free(vector_ids);
                const metas = try store.alloc.alloc([]const u8, ids.len);
                defer store.alloc.free(metas);
                const found = try store.alloc.alloc(bool, ids.len);
                defer store.alloc.free(found);
                for (ids, vector_ids, metas) |id, *vid, *m| {
                    const node = store.nodes.items[id];
                    vid.* = node.vector_id;
                    m.* = if (store.index.?.isDeleted(id)) "" else node.metadata;
                }
                @memset(found, false);
                var ignored_ns: u64 = 0;
                if (loader.load_batch_session) |load_session| {
                    var fallback: RecoverySession = .{};
                    defer fallback.deinit();
                    try load_session(loader.ctx, store.alloc, vector_ids, metas, out[0 .. ids.len * d], found, self.reembed_ns orelse &ignored_ns, self.recovery orelse &fallback);
                } else if (loader.load_batch_timed) |timed| {
                    try timed(loader.ctx, store.alloc, vector_ids, metas, out[0 .. ids.len * d], found, self.reembed_ns orelse &ignored_ns);
                } else try loader.load_batch.?(loader.ctx, store.alloc, vector_ids, metas, out[0 .. ids.len * d], found);
                for (found, 0..) |ok, i| if (!ok) {
                    if (self.strict and metas[i].len != 0) return error.IncompletePublishedSnapshot;
                    @memset(out[i * d ..][0..d], std.math.nan(f32));
                };
                return;
            }
            for (ids, 0..) |id, i| {
                const row = out[i * d ..][0..d];
                const node = store.nodes.items[id];
                const loaded: ?[]f32 = blk: {
                    if (store.index.?.isDeleted(id) or node.metadata.len == 0) break :blk null;
                    break :blk loader.load(loader.ctx, store.alloc, node.vector_id, node.metadata) catch |err| switch (err) {
                        error.NotFound => null,
                        else => return err,
                    };
                };
                if (loaded) |v| {
                    defer store.alloc.free(v);
                    if (v.len != d) return error.DimensionMismatch;
                    @memcpy(row, v);
                } else {
                    if (self.strict and node.metadata.len != 0 and !store.index.?.isDeleted(id)) return error.IncompletePublishedSnapshot;
                    @memset(row, std.math.nan(f32));
                }
            }
        }
    };

    // ---------------------------------------------------------------------
    // Persistence
    // ---------------------------------------------------------------------

    /// Rebuilds if deletes passed the threshold, then persists when anything
    /// changed. Call before the owner records an applied sequence.
    pub fn sync(self: *LeannStore) !void {
        self.lock.lockUncancelable(self.io);
        defer self.lock.unlock(self.io);
        if (self.replicated) |state| {
            // Rebuilds are planned by the leader, never done locally.
            if (!self.dirty) return;
            const bytes = try state.encode(self.alloc);
            defer self.alloc.free(bytes);
            try self.persist(bytes);
            self.durable_raft_index = state.applied_raft_index;
            self.dirty = false;
            return;
        }
        if (self.index) |*index| {
            if (index.deletedFraction() >= self.opts.rebuild_deleted_fraction and (self.loader != null or self.opts.transient)) {
                try self.rebuildLocked();
                self.dirty = true;
            }
        }
        if (!self.dirty) return;
        const bytes = try self.encode();
        defer self.alloc.free(bytes);
        try self.persist(bytes);
        self.dirty = false;
        self.elide_durable = self.elide_count;
    }

    /// The persisted file, or null when it was never written.
    fn readPersisted(self: *LeannStore) !?[]u8 {
        if (self.opts.storage) |storage| {
            return storage.readFileAlloc(self.alloc, self.path, std.math.maxInt(usize)) catch |err| switch (err) {
                error.FileNotFound, error.NotDir => null,
                else => err,
            };
        }
        return std.Io.Dir.cwd().readFileAlloc(self.io, self.path, self.alloc, .unlimited) catch |err| switch (err) {
            error.FileNotFound => null,
            else => err,
        };
    }

    /// Atomically replaces the persisted file. Every write lands in a new
    /// file (temp + rename), which native backups rely on to pin it.
    fn persist(self: *LeannStore, bytes: []const u8) !void {
        if (self.opts.storage) |storage| {
            var sink = try storage.beginAtomicWrite(self.alloc, self.path);
            var sink_active = true;
            defer if (sink_active) sink.abort();
            try sink.appendSlice(bytes);
            sink_active = false;
            try sink.finish();
            return;
        }
        try writeFileAtomicallyDurable(self.alloc, self.io, self.path, bytes);
    }

    /// Native backups: the persisted file to pin, or null while the store
    /// has never been persisted (it is empty). Fails while state newer than
    /// the file exists, since the capture must match the applied sequence.
    pub fn nativeBackupPath(self: *LeannStore) !?[]const u8 {
        self.lock.lockSharedUncancelable(self.io);
        defer self.lock.unlockShared(self.io);
        if (self.dirty) return error.NativeBackupProjectionNotQuiescent;
        if (self.replicated) |state| if (self.durable_raft_index != state.applied_raft_index)
            return error.NativeBackupProjectionNotQuiescent;
        if (self.opts.storage) |storage| if (!storage.supportsHostPathGenerationPublication()) return error.NativeBackupProjectionBackendUnsupported;
        std.Io.Dir.cwd().access(self.io, self.path, .{}) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        return self.path;
    }

    fn encode(self: *const LeannStore) ![]u8 {
        var w: std.Io.Writer.Allocating = .init(self.alloc);
        defer w.deinit();
        const wr = &w.writer;
        try wr.writeAll(magic);
        try wr.writeInt(u32, format_version, .little);
        try wr.writeInt(u32, self.opts.dims, .little);
        try wr.writeInt(u32, @intCast(self.nodes.items.len), .little);
        for (self.nodes.items) |node| {
            try wr.writeInt(u64, node.vector_id, .little);
            try wr.writeInt(u32, @intCast(node.metadata.len), .little);
            try wr.writeAll(node.metadata);
        }
        try wr.writeInt(u32, @intCast(self.pending.items.len), .little);
        for (self.pending.items) |p| {
            try wr.writeInt(u64, p.vector_id, .little);
            try wr.writeInt(u32, @intCast(p.metadata.len), .little);
            try wr.writeAll(p.metadata);
            for (p.vector) |f| try wr.writeInt(u32, @bitCast(f), .little);
        }
        if (self.index) |*index| {
            const blob = try index.serialize(self.alloc);
            defer self.alloc.free(blob);
            try wr.writeByte(1);
            try wr.writeInt(u64, blob.len, .little);
            try wr.writeAll(blob);
        } else try wr.writeByte(0);
        try wr.writeInt(u32, @intCast(self.elide_count), .little);
        var elide_node = self.elide_first;
        while (elide_node) |node| : (elide_node = node.next) {
            const e = node.entry;
            try wr.writeInt(u32, @intCast(e.key.len), .little);
            try wr.writeAll(e.key);
            try wr.writeInt(u64, e.digest, .little);
        }
        const crc = @import("antfly_hash").Crc32.hash(w.written());
        try wr.writeInt(u32, crc, .little);
        return w.toOwnedSlice();
    }

    fn decode(self: *LeannStore, bytes: []const u8) !void {
        if (bytes.len < magic.len + 8) return error.InvalidLeannStore;
        const body = bytes[0 .. bytes.len - 4];
        if (@import("antfly_hash").Crc32.hash(body) != std.mem.readInt(u32, bytes[bytes.len - 4 ..][0..4], .little)) return error.InvalidLeannStore;
        var r: std.Io.Reader = .fixed(body);
        const head = r.take(magic.len) catch return error.InvalidLeannStore;
        if (!std.mem.eql(u8, head, magic)) return error.InvalidLeannStore;
        const version = r.takeInt(u32, .little) catch return error.InvalidLeannStore;
        if (version != format_version and version != 1) return error.InvalidLeannStore;
        if ((r.takeInt(u32, .little) catch return error.InvalidLeannStore) != self.opts.dims) return error.DimensionMismatch;

        const node_count = r.takeInt(u32, .little) catch return error.InvalidLeannStore;
        try self.nodes.ensureTotalCapacity(self.alloc, node_count);
        for (0..node_count) |_| {
            const vector_id = r.takeInt(u64, .little) catch return error.InvalidLeannStore;
            const len = r.takeInt(u32, .little) catch return error.InvalidLeannStore;
            const meta = try self.alloc.dupe(u8, r.take(len) catch return error.InvalidLeannStore);
            self.nodes.appendAssumeCapacity(.{ .vector_id = vector_id, .metadata = meta });
        }
        const pending_count = r.takeInt(u32, .little) catch return error.InvalidLeannStore;
        for (0..pending_count) |_| {
            const vector_id = r.takeInt(u64, .little) catch return error.InvalidLeannStore;
            const len = r.takeInt(u32, .little) catch return error.InvalidLeannStore;
            const meta = try self.alloc.dupe(u8, r.take(len) catch return error.InvalidLeannStore);
            errdefer self.alloc.free(meta);
            const v = try self.alloc.alloc(f32, self.opts.dims);
            errdefer self.alloc.free(v);
            for (v) |*f| f.* = @bitCast(r.takeInt(u32, .little) catch return error.InvalidLeannStore);
            try self.pending_of.put(self.alloc, vector_id, @intCast(self.pending.items.len));
            try self.pending.append(self.alloc, .{ .vector_id = vector_id, .metadata = meta, .vector = v });
        }
        const has_index = r.takeByte() catch return error.InvalidLeannStore;
        if (has_index == 1) {
            const len = r.takeInt(u64, .little) catch return error.InvalidLeannStore;
            const blob = r.take(@intCast(len)) catch return error.InvalidLeannStore;
            self.index = try leann.Index.deserialize(self.alloc, blob);
            if (self.index.?.node_count != node_count) return error.InvalidLeannStore;
            for (self.nodes.items, 0..) |node, i| {
                if (self.index.?.isDeleted(@intCast(i))) continue;
                try self.node_of.put(self.alloc, node.vector_id, @intCast(i));
            }
        } else if (has_index != 0 or node_count != 0) return error.InvalidLeannStore;
        if (version >= 2) {
            const elide_count = r.takeInt(u32, .little) catch return error.InvalidLeannStore;
            for (0..elide_count) |_| {
                const len = r.takeInt(u32, .little) catch return error.InvalidLeannStore;
                const key = try self.alloc.dupe(u8, r.take(len) catch return error.InvalidLeannStore);
                errdefer self.alloc.free(key);
                const digest = r.takeInt(u64, .little) catch return error.InvalidLeannStore;
                try self.appendElideOwned(.{ .key = key, .digest = digest });
            }
            self.elide_durable = self.elide_count;
        }
        if (r.bufferedLen() != 0) return error.InvalidLeannStore;
    }
};

/// HBC request filters, applied to graph and buffer candidates.
/// Adapts a request filter to graph node ids.
const NodeFilter = struct {
    store: *const LeannStore,
    filter: *const Filter,

    fn allows(raw: *const anyopaque, id: u32) anyerror!bool {
        const self: *const NodeFilter = @ptrCast(@alignCast(raw));
        const node = self.store.nodes.items[id];
        return self.filter.allows(node.vector_id, node.metadata);
    }
};

/// Bounded cache of vectors re-embedded for transient LEANN indexes, so hot
/// candidates are not re-embedded on every query. Keyed by the index
/// incarnation, the document key and the digest of the dropped vector: an
/// edited document (new digest) or a recreated index never sees a stale
/// vector. Evicts the least recently used entries. Thread-safe.
pub const ReembedCache = struct {
    const Entry = struct { key: []u8, vector: []f32, prev: ?*Entry = null, next: ?*Entry = null };

    limit_bytes: usize,
    mutex: std.atomic.Mutex = .unlocked,
    map: std.StringHashMapUnmanaged(*Entry) = .empty,
    oldest: ?*Entry = null,
    newest: ?*Entry = null,
    bytes: usize = 0,
    hits: u64 = 0,
    misses: u64 = 0,

    pub fn deinit(self: *ReembedCache, alloc: Allocator) void {
        var it = self.map.valueIterator();
        while (it.next()) |value| {
            const entry = value.*;
            alloc.free(entry.key);
            alloc.free(entry.vector);
            alloc.destroy(entry);
        }
        self.map.deinit(alloc);
        self.* = .{ .limit_bytes = self.limit_bytes };
    }

    fn lock(self: *ReembedCache) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    fn keyInto(buf: []u8, incarnation: u64, doc_key: []const u8, digest: u64) ?[]const u8 {
        if (doc_key.len + 16 > buf.len) return null;
        std.mem.writeInt(u64, buf[0..8], incarnation, .little);
        std.mem.writeInt(u64, buf[8..16], digest, .little);
        @memcpy(buf[16..][0..doc_key.len], doc_key);
        return buf[0 .. 16 + doc_key.len];
    }

    /// Copies the cached vector into `out` and returns true on a hit.
    pub fn get(self: *ReembedCache, incarnation: u64, doc_key: []const u8, digest: u64, out: []f32) bool {
        if (self.limit_bytes == 0) return false;
        var prefix: [16]u8 = undefined;
        std.mem.writeInt(u64, prefix[0..8], incarnation, .little);
        std.mem.writeInt(u64, prefix[8..16], digest, .little);
        self.lock();
        defer self.mutex.unlock();
        const Lookup = struct {
            pub fn hash(_: @This(), key: anytype) u64 {
                var h = std.hash.Wyhash.init(0);
                h.update(key.prefix);
                h.update(key.doc);
                return h.final();
            }
            pub fn eql(_: @This(), key: anytype, stored: []const u8) bool {
                return stored.len == key.prefix.len + key.doc.len and std.mem.eql(u8, stored[0..16], key.prefix) and std.mem.eql(u8, stored[16..], key.doc);
            }
        };
        const entry = self.map.getAdapted(.{ .prefix = @as([]const u8, &prefix), .doc = doc_key }, Lookup{}) orelse {
            self.misses += 1;
            return false;
        };
        if (entry.vector.len != out.len) return false;
        @memcpy(out, entry.vector);
        self.touch(entry);
        self.hits += 1;
        return true;
    }

    pub fn put(self: *ReembedCache, alloc: Allocator, incarnation: u64, doc_key: []const u8, digest: u64, vector: []const f32) !void {
        const size = doc_key.len + 16 + vector.len * @sizeOf(f32);
        if (size > self.limit_bytes) return;
        const owned_key = try alloc.alloc(u8, doc_key.len + 16);
        errdefer alloc.free(owned_key);
        _ = keyInto(owned_key, incarnation, doc_key, digest);
        self.lock();
        defer self.mutex.unlock();
        if (self.map.contains(owned_key)) {
            alloc.free(owned_key);
            return;
        }
        while (self.bytes + size > self.limit_bytes) self.evictOldest(alloc);
        const owned_vector = try alloc.dupe(f32, vector);
        errdefer alloc.free(owned_vector);
        const entry = try alloc.create(Entry);
        errdefer alloc.destroy(entry);
        entry.* = .{ .key = owned_key, .vector = owned_vector };
        try self.map.put(alloc, owned_key, entry);
        self.linkNewest(entry);
        self.bytes += size;
    }

    fn unlink(self: *ReembedCache, entry: *Entry) void {
        if (entry.prev) |prev| prev.next = entry.next else self.oldest = entry.next;
        if (entry.next) |next| next.prev = entry.prev else self.newest = entry.prev;
        entry.prev = null;
        entry.next = null;
    }

    fn linkNewest(self: *ReembedCache, entry: *Entry) void {
        entry.prev = self.newest;
        if (self.newest) |last| last.next = entry else self.oldest = entry;
        self.newest = entry;
    }

    fn touch(self: *ReembedCache, entry: *Entry) void {
        if (self.newest == entry) return;
        self.unlink(entry);
        self.linkNewest(entry);
    }

    fn evictOldest(self: *ReembedCache, alloc: Allocator) void {
        const victim = self.oldest orelse return;
        self.unlink(victim);
        _ = self.map.remove(victim.key);
        self.bytes -= victim.key.len + victim.vector.len * @sizeOf(f32);
        alloc.free(victim.key);
        alloc.free(victim.vector);
        alloc.destroy(victim);
    }
};

/// A small allowlist is cheaper to score directly. Keep large allowlists
/// as graph predicates, with the same visit budget as other bounded filters.
pub fn useDirectCandidates(req: search_types.SearchRequest, allowed: usize, live: usize, search_ef: u32) bool {
    if (search_types.requiresExhaustiveCoverage(req)) return true;
    const effort = @max(req.k, if (search_ef == 0) @as(usize, 512) else search_ef);
    if (allowed > 65536) return false;
    if (allowed <= @max(1024, effort *| 4)) return true;
    // Filling the filtered beam costs roughly effort / selectivity graph
    // probes, each less cache-friendly than a direct code scan. Favor direct
    // scoring until its estimated cost exceeds that walk, within the cap.
    const walk_cost = live *| effort *| 32;
    return allowed *| allowed <= walk_cost;
}

test "LEANN allowlist routing bounds direct scoring on large indexes" {
    const req: search_types.SearchRequest = .{ .query = &.{0}, .k = 10 };
    try std.testing.expect(useDirectCandidates(req, 32, 10_000, 0));
    try std.testing.expect(useDirectCandidates(req, 6500, 100_000, 0));
    try std.testing.expect(useDirectCandidates(req, 40_000, 100_000, 0));
    try std.testing.expect(!useDirectCandidates(req, 41_000, 100_000, 0));
    try std.testing.expect(!useDirectCandidates(req, 50_000, 100_000, 0));
    try std.testing.expect(useDirectCandidates(req, 65_536, 10_000_000, 0));
    try std.testing.expect(!useDirectCandidates(req, 65_537, 10_000_000, 0));
    var exhaustive = req;
    exhaustive.search_effort = 1;
    try std.testing.expect(useDirectCandidates(exhaustive, 100_000, 1_000_000, 4096));
}

/// Per-query gate for re-embedding: the request's cancellation aborts the
/// search; once the time budget is spent, the store reports incomplete
/// scoring rather than returning unverified candidates.
pub const SearchGate = struct {
    cancellation: ?search_types.CancellationToken,
    deadline_ns: ?u64,
    budget_ns: u64,
    reembed_ns: u64 = 0,

    pub fn init(req: search_types.SearchRequest, budget_ms: u32) SearchGate {
        return .{
            .cancellation = req.cancellation,
            .deadline_ns = req.deadline_ns,
            .budget_ns = @as(u64, budget_ms) * std.time.ns_per_ms,
        };
    }

    pub fn recomputeGate(self: *const SearchGate) ?leann.RecomputeGate {
        if (self.cancellation == null and self.deadline_ns == null and self.budget_ns == 0) return null;
        return .{ .ctx = self, .check_fn = check, .cancel_fn = checkCancellation };
    }

    fn checkCancellation(raw: *const anyopaque) anyerror!void {
        const self: *const SearchGate = @ptrCast(@alignCast(raw));
        if (self.cancellation) |token| if (token.isCancelled()) return error.Cancelled;
        if (self.deadline_ns) |deadline| if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.Timeout;
    }

    fn check(raw: *const anyopaque) anyerror!bool {
        const self: *const SearchGate = @ptrCast(@alignCast(raw));
        try checkCancellation(raw);
        if (self.budget_ns != 0 and self.reembed_ns >= self.budget_ns) return false;
        return true;
    }
};

fn normalize(v: []f32) void {
    var s: f32 = 0;
    for (v) |x| s += x * x;
    const n = @sqrt(s);
    if (n == 0) return;
    for (v) |*x| x.* /= n;
}

fn writeFileAtomicallyDurable(alloc: Allocator, io: std.Io, path: []const u8, contents: []const u8) !void {
    // In-memory browser indexes never publish host files. Keep their close and
    // sync paths independent of native directory handles and POSIX constants.
    if (comptime @import("builtin").os.tag == .freestanding) return error.UnsupportedPlatform;
    const fs_paths = @import("antfly_runtime_fs").fs_paths;
    const tmp_path = try std.fmt.allocPrint(alloc, "{s}.tmp", .{path});
    defer alloc.free(tmp_path);
    if (std.fs.path.dirname(path)) |parent| try fs_paths.createDirPathPortable(io, parent);
    {
        var file = try fs_paths.createFilePortable(io, tmp_path, .{ .truncate = true });
        defer file.close(io);
        var buf: [16 * 1024]u8 = undefined;
        var writer = file.writer(io, &buf);
        try writer.interface.writeAll(contents);
        try writer.end();
        try file.sync(io);
    }
    std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io) catch |err| {
        std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};
        return err;
    };
    if (std.fs.path.dirname(path)) |parent| try fs_paths.syncDirPortable(io, parent);
}

// -------------------------------------------------------------------------
// Tests
// -------------------------------------------------------------------------

const testing = std.testing;

const TestVectors = struct {
    data: []f32,
    dims: usize,

    fn loader(self: *TestVectors) VectorLoader {
        return .{ .ctx = self, .load = load };
    }

    fn load(ctx: *anyopaque, alloc: Allocator, vector_id: u64, _: []const u8) anyerror![]f32 {
        const self: *TestVectors = @ptrCast(@alignCast(ctx));
        const start = @as(usize, @intCast(vector_id)) * self.dims;
        if (start + self.dims > self.data.len) return error.NotFound;
        return try alloc.dupe(f32, self.data[start..][0..self.dims]);
    }
};

const TestItem = struct { vector_id: u64, vector: []const f32, metadata: []const u8 };

fn testData(alloc: Allocator, n: usize, dims: usize) ![]f32 {
    var prng = std.Random.DefaultPrng.init(9);
    const r = prng.random();
    const centers = try alloc.alloc(f32, 8 * dims);
    defer alloc.free(centers);
    for (centers) |*c| c.* = r.floatNorm(f32) * 3;
    const data = try alloc.alloc(f32, n * dims);
    for (0..n) |i| {
        const c = r.uintLessThan(usize, 8);
        for (0..dims) |j| data[i * dims + j] = centers[c * dims + j] + r.floatNorm(f32);
    }
    return data;
}

fn addAll(alloc: Allocator, store: *LeannStore, data: []const f32, dims: usize, from: usize, to: usize) !void {
    var items = std.ArrayListUnmanaged(TestItem).empty;
    defer {
        for (items.items) |it| alloc.free(it.metadata);
        items.deinit(alloc);
    }
    for (from..to) |i| {
        try items.append(alloc, .{
            .vector_id = i,
            .vector = data[i * dims ..][0..dims],
            .metadata = try std.fmt.allocPrint(alloc, "doc-{d}", .{i}),
        });
    }
    try store.apply(items.items, &.{});
}

test "leann elision queue bounds batches and persists remaining work" {
    const alloc = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realPathFileAlloc(testing.io, ".", alloc);
    defer alloc.free(dir);
    const dims = 16;
    const data = try testData(alloc, 300, dims);
    defer alloc.free(data);
    var vectors = TestVectors{ .data = data, .dims = dims };
    const opts = Options{ .dims = dims, .metric = .l2_squared, .transient = true };
    {
        var store = try LeannStore.open(alloc, testing.io, dir, opts);
        defer store.deinit();
        store.setLoader(vectors.loader());
        try addAll(alloc, store, data, dims, 0, 300);
        try store.sync();
        try testing.expectEqual(@as(usize, 300), store.pendingElideCount());
        const first = try store.pendingElideAlloc(alloc, 37);
        defer LeannStore.freeElide(alloc, first);
        try testing.expectEqual(@as(usize, 37), first.len);
        store.clearElided(first.len);
        try testing.expectEqual(@as(usize, 263), store.pendingElideCount());
        try store.sync();
    }
    var reopened = try LeannStore.open(alloc, testing.io, dir, opts);
    defer reopened.deinit();
    try testing.expectEqual(@as(usize, 263), reopened.pendingElideCount());
    while (reopened.pendingElideCount() != 0) {
        const batch = try reopened.pendingElideAlloc(alloc, 37);
        defer LeannStore.freeElide(alloc, batch);
        try testing.expect(batch.len <= 37);
        reopened.clearElided(batch.len);
    }
}

test "leann search budget counts reembedding time only" {
    var gate = SearchGate.init(.{ .query = &.{}, .k = 1 }, 10);
    try testing.expect(try leann.RecomputeGate.open(gate.recomputeGate()));
    gate.reembed_ns = 9 * std.time.ns_per_ms;
    try testing.expect(try leann.RecomputeGate.open(gate.recomputeGate()));
    gate.reembed_ns = 10 * std.time.ns_per_ms;
    try testing.expect(!try leann.RecomputeGate.open(gate.recomputeGate()));
}

test "leann search forwards reembedding time from batch loader" {
    const alloc = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realPathFileAlloc(testing.io, ".", alloc);
    defer alloc.free(dir);
    const dims = 16;
    const data = try testData(alloc, 300, dims);
    defer alloc.free(data);
    const Timed = struct {
        vectors: TestVectors,
        calls: usize = 0,
        cancel: ?*std.atomic.Value(bool) = null,
        released: bool = false,
        fn load(ctx: *anyopaque, a: Allocator, id: u64, meta: []const u8) anyerror![]f32 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            return TestVectors.load(&self.vectors, a, id, meta);
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.released = true;
        }
        fn sessionBatch(ctx: *anyopaque, a: Allocator, ids: []const u64, metas: []const []const u8, out: []f32, found: []bool, elapsed: *u64, session: *RecoverySession) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (self.cancel) |signal| {
                try testing.expect(session.cancellation != null);
                session.state = ctx;
                session.release = release;
                signal.store(true, .release);
                return session.check();
            }
            // Only the first batch needs inference. Subsequent batches model
            // full stored vectors or hits in the re-embedding cache.
            if (self.calls == 0) {
                try session.checkDispatch(elapsed.*);
                try batch(ctx, a, ids, metas, out, found, elapsed);
            } else {
                for (ids, 0..) |id, i| {
                    const start = @as(usize, @intCast(id)) * dims;
                    @memcpy(out[i * dims ..][0..dims], self.vectors.data[start..][0..dims]);
                    found[i] = true;
                }
            }
        }
        fn batch(ctx: *anyopaque, _: Allocator, ids: []const u64, _: []const []const u8, out: []f32, found: []bool, elapsed: *u64) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.calls += 1;
            elapsed.* += 10 * std.time.ns_per_ms;
            for (ids, 0..) |id, i| {
                const start = @as(usize, @intCast(id)) * dims;
                @memcpy(out[i * dims ..][0..dims], self.vectors.data[start..][0..dims]);
                found[i] = true;
            }
        }
    };
    var source = Timed{ .vectors = .{ .data = data, .dims = dims } };
    var store = try LeannStore.open(alloc, testing.io, dir, .{ .dims = dims, .metric = .l2_squared, .transient = true, .reembed_budget_ms = 10, .batch_size = 1 });
    defer store.deinit();
    store.setLoader(.{ .ctx = &source, .load = Timed.load, .load_batch_timed = Timed.batch });
    try addAll(alloc, store, data, dims, 0, 300);
    source.calls = 0;
    try testing.expectError(error.ReembedBudgetExceeded, store.search(alloc, .{ .query = data[0..dims], .k = 50 }));
    try testing.expectEqual(@as(usize, 1), source.calls);
    source.calls = 0;
    try testing.expectError(error.ReembedBudgetExceeded, store.search(alloc, .{ .query = data[0..dims], .k = 50, .search_effort = 1 }));
    try testing.expectEqual(@as(usize, 1), source.calls);
    store.setLoader(.{ .ctx = &source, .load = Timed.load, .load_batch_session = Timed.sessionBatch });
    source.calls = 0;
    var mixed = try store.search(alloc, .{ .query = data[0..dims], .k = 50, .search_effort = 1 });
    defer mixed.deinit();
    try testing.expectEqual(@as(usize, 50), mixed.items.items.len);
    try testing.expectEqual(@as(usize, 1), source.calls);
    var signal = std.atomic.Value(bool).init(false);
    source.cancel = &signal;
    try testing.expectError(error.Cancelled, store.search(alloc, .{ .query = data[0..dims], .k = 50, .cancellation = search_types.CancellationToken.fromAtomic(&signal) }));
    try testing.expect(source.released);
}

test "leann search releases graph ownership before invoking its vector loader" {
    const alloc = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    var store = try LeannStore.open(alloc, testing.io, path, .{ .dims = 16, .metric = .l2_squared, .max_degree = 8, .build_threshold = 32, .transient = true });
    defer store.deinit();
    const data = try testData(alloc, 100, 16);
    defer alloc.free(data);
    const Source = struct {
        store: *LeannStore,
        vectors: []const f32,
        mutated: bool = false,

        fn load(raw: *anyopaque, a: Allocator, id: u64, metadata: []const u8) ![]f32 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!self.mutated) {
                self.mutated = true;
                // This needs the exclusive graph lock and frees the original
                // metadata. The active query owns its candidate bytes.
                try self.store.apply(@as([]const TestItem, &.{}), &.{id});
            }
            try testing.expect(metadata.len > 0);
            return a.dupe(f32, self.vectors[id * 16 ..][0..16]);
        }
    };
    var source = Source{ .store = store, .vectors = data };
    store.setLoader(.{ .ctx = &source, .load = Source.load });
    try addAll(alloc, store, data, 16, 0, 100);
    var results = try store.search(alloc, .{ .query = data[0..16], .k = 3, .search_effort = 1 });
    defer results.deinit();
    try testing.expect(source.mutated);
    try testing.expectEqual(@as(usize, 3), results.items.items.len);
    try testing.expectEqual(@as(u64, 0), results.items.items[0].vector_id);
}

test "leann full effort distance bounds score each surviving vector once" {
    const alloc = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realPathFileAlloc(testing.io, ".", alloc);
    defer alloc.free(dir);
    const dims = 8;
    const data = try alloc.alloc(f32, 300 * dims);
    defer alloc.free(data);
    for (0..300) |i| @memset(data[i * dims ..][0..dims], @floatFromInt(i));
    const Counting = struct {
        vectors: TestVectors,
        calls: usize = 0,
        fn load(raw: *anyopaque, a: Allocator, id: u64, metadata: []const u8) anyerror![]f32 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return TestVectors.load(&self.vectors, a, id, metadata);
        }
    };
    var source = Counting{ .vectors = .{ .data = data, .dims = dims } };
    var store = try LeannStore.open(alloc, testing.io, dir, .{ .dims = dims, .metric = .l2_squared, .build_threshold = 32, .max_degree = 8 });
    defer store.deinit();
    store.setLoader(.{ .ctx = &source, .load = Counting.load });
    try addAll(alloc, store, data, dims, 0, 300);
    try store.apply(@as([]const TestItem, &.{}), &.{290});
    source.calls = 0;
    const query: [dims]f32 = @splat(0);
    var results = try store.search(alloc, .{ .query = &query, .k = 3, .search_effort = 1, .distance_over = 8 * 289 * 289, .distance_under = 8 * 295 * 295 });
    defer results.deinit();
    try testing.expectEqual(@as(usize, 3), results.items.items.len);
    for (results.items.items, 291..) |hit, expected| try testing.expectEqual(@as(u64, @intCast(expected)), hit.vector_id);
    try testing.expectEqual(@as(usize, 299), source.calls);
}

test "leann reembed cache: hits by digest, misses on a new digest, evicts least recent" {
    const alloc = testing.allocator;
    const v1 = [_]f32{ 1, 2, 3, 4 };
    const v2 = [_]f32{ 5, 6, 7, 8 };
    // Room for two entries of a 7-byte key plus 16 vector bytes.
    var cache = ReembedCache{ .limit_bytes = 2 * (16 + 7 + 16) };
    defer cache.deinit(alloc);
    var out: [4]f32 = undefined;
    try testing.expect(!cache.get(1, "doc:aa", 10, &out));
    try cache.put(alloc, 1, "doc:aa", 10, &v1);
    try testing.expect(cache.get(1, "doc:aa", 10, &out));
    try testing.expectEqualSlices(f32, &v1, &out);
    // An edited document (new digest) or another incarnation misses.
    try testing.expect(!cache.get(1, "doc:aa", 11, &out));
    try testing.expect(!cache.get(2, "doc:aa", 10, &out));
    try cache.put(alloc, 1, "doc:bb", 10, &v2);
    _ = cache.get(1, "doc:aa", 10, &out); // doc:aa is now most recent
    try cache.put(alloc, 1, "doc:cc", 10, &v2); // evicts doc:bb
    try testing.expect(cache.get(1, "doc:aa", 10, &out));
    try testing.expect(!cache.get(1, "doc:bb", 10, &out));
    try testing.expect(cache.get(1, "doc:cc", 10, &out));
}

test "leann reembed cache accepts long binary keys without allocating on lookup" {
    const a = testing.allocator;
    var cache = ReembedCache{ .limit_bytes = 1 << 20 };
    defer cache.deinit(a);
    var key: [4096]u8 = @splat('x');
    key[13] = 0;
    const vector = [_]f32{ 1, 2, 3, 4 };
    var out: [4]f32 = undefined;
    try cache.put(a, 7, &key, 9, &vector);
    try testing.expect(cache.get(7, &key, 9, &out));
    try testing.expectEqualSlices(f32, &vector, &out);
    key[4000] = 'z';
    try testing.expect(!cache.get(7, &key, 9, &out));
}

test "leann store: persists through a storage provider and reopens" {
    const alloc = testing.allocator;
    const io = testing.io;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();
    const dims = 16;
    const n = 300;
    const data = try testData(alloc, n, dims);
    defer alloc.free(data);
    var vectors = TestVectors{ .data = data, .dims = dims };
    const opts = Options{ .dims = dims, .metric = .l2_squared, .max_degree = 8, .build_threshold = 100, .storage = memory.storage() };
    {
        var store = try LeannStore.open(alloc, io, "__test/leann_idx", opts);
        defer store.deinit();
        store.setLoader(vectors.loader());
        try addAll(alloc, store, data, dims, 0, n);
        try store.sync();
        // Native backups pin host files only.
        try testing.expectError(error.NativeBackupProjectionBackendUnsupported, store.nativeBackupPath());
    }
    var reopened = try LeannStore.open(alloc, io, "__test/leann_idx", opts);
    defer reopened.deinit();
    reopened.setLoader(vectors.loader());
    try testing.expectEqual(@as(u64, n), reopened.liveCount());
    var res = try reopened.search(alloc, .{ .query = data[42 * dims ..][0..dims], .k = 3 });
    defer res.deinit();
    try testing.expectEqual(@as(u64, 42), res.items.items[0].vector_id);
}

fn expectAscending(items: []const search_results.SearchResult) !void {
    for (items[1..], items[0 .. items.len - 1]) |item, prev| try testing.expect(prev.distance <= item.distance);
}

test "leann store: buffer, build, search, filters, delete, persist" {
    const alloc = testing.allocator;
    const io = testing.io;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(dir);

    const dims = 64;
    const n = 600;
    const data = try testData(alloc, n, dims);
    defer alloc.free(data);
    var vectors = TestVectors{ .data = data, .dims = dims };
    const opts = Options{ .dims = dims, .metric = .l2_squared, .max_degree = 16, .build_threshold = 200 };

    var store = try LeannStore.open(alloc, io, dir, opts);
    store.setLoader(vectors.loader());

    // Before the build threshold everything is exact.
    try addAll(alloc, store, data, dims, 0, 100);
    try testing.expect(store.index == null);
    {
        var res = try store.search(alloc, .{ .query = data[7 * dims ..][0..dims], .k = 1 });
        defer res.deinit();
        try testing.expectEqual(@as(u64, 7), res.items.items[0].vector_id);
        try testing.expectEqualStrings("doc-7", res.items.items[0].metadata.?);
    }

    try addAll(alloc, store, data, dims, 100, n);
    try testing.expect(store.index != null);
    try testing.expectEqual(@as(u64, n), store.liveCount());
    for ([_]u64{ 5, 250, 599 }) |id| {
        var res = try store.search(alloc, .{ .query = data[id * dims ..][0..dims], .k = 5 });
        defer res.deinit();
        try testing.expectEqual(id, res.items.items[0].vector_id);
        try expectAscending(res.items.items);
    }

    // A selective id filter is applied inside the walk and still returns
    // every allowed node, nearest first.
    {
        const allowed = [_]u64{ 3, 97, 211, 404, 577 };
        var res = try store.search(alloc, .{ .query = data[211 * dims ..][0..dims], .k = 5, .filter_ids = &allowed });
        defer res.deinit();
        try testing.expectEqual(@as(usize, 5), res.items.items.len);
        try testing.expectEqual(@as(u64, 211), res.items.items[0].vector_id);
        for (res.items.items) |item| try testing.expect(std.mem.indexOfScalar(u64, &allowed, item.vector_id) != null);
        try expectAscending(res.items.items);
    }

    // Ranked like HBC: nearest first across the whole result list. The
    // underlying heap keeps the worst result first, so an unsorted k=10
    // answer put the nearest at rank 8.
    {
        const q = data[123 * dims ..][0..dims];
        var res = try store.search(alloc, .{ .query = q, .k = 10 });
        defer res.deinit();
        try testing.expectEqual(@as(usize, 10), res.items.items.len);
        try testing.expectEqual(@as(u64, 123), res.items.items[0].vector_id);
        try expectAscending(res.items.items);
    }

    // Filters: exclude the exact match, then allow only a small set.
    {
        var res = try store.search(alloc, .{ .query = data[250 * dims ..][0..dims], .k = 3, .exclude_ids = &.{250} });
        defer res.deinit();
        for (res.items.items) |item| try testing.expect(item.vector_id != 250);
        var res2 = try store.search(alloc, .{ .query = data[250 * dims ..][0..dims], .k = 3, .filter_ids = &.{ 3, 4, 5 } });
        defer res2.deinit();
        try testing.expectEqual(@as(usize, 3), res2.items.items.len);
        for (res2.items.items) |item| try testing.expect(item.vector_id >= 3 and item.vector_id <= 5);
    }

    // Upsert and delete are idempotent.
    try store.apply(&[_]TestItem{.{ .vector_id = 250, .vector = data[250 * dims ..][0..dims], .metadata = "doc-250" }}, &.{});
    try store.apply(&[_]TestItem{}, &.{ 251, 251, 9999 });
    try testing.expectEqual(@as(u64, n - 1), store.liveCount());
    try testing.expect((try store.getMetadata(alloc, 251)) == null);

    try store.sync();
    const before = store.stats();
    store.deinit();

    var reopened = try LeannStore.open(alloc, io, dir, opts);
    defer reopened.deinit();
    reopened.setLoader(vectors.loader());
    const after = reopened.stats();
    try testing.expectEqual(before.live, after.live);
    try testing.expectEqual(before.graph_nodes, after.graph_nodes);
    var res = try reopened.search(alloc, .{ .query = data[250 * dims ..][0..dims], .k = 1 });
    defer res.deinit();
    try testing.expectEqual(@as(u64, 250), res.items.items[0].vector_id);
}

test "leann store: rebuild after deletes keeps live vectors searchable" {
    const alloc = testing.allocator;
    const io = testing.io;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(dir);

    const dims = 32;
    const n = 400;
    const data = try testData(alloc, n, dims);
    defer alloc.free(data);
    var vectors = TestVectors{ .data = data, .dims = dims };
    var store = try LeannStore.open(alloc, io, dir, .{ .dims = dims, .metric = .cosine, .max_degree = 16, .build_threshold = 100 });
    defer store.deinit();
    store.setLoader(vectors.loader());
    try addAll(alloc, store, data, dims, 0, n);

    var deletes = std.ArrayListUnmanaged(u64).empty;
    defer deletes.deinit(alloc);
    for (0..n) |i| if (i % 3 == 0) try deletes.append(alloc, i);
    try store.apply(&[_]TestItem{}, deletes.items);
    try store.sync(); // past the 20% threshold: rebuild
    const s = store.stats();
    try testing.expectEqual(@as(u64, 0), s.deleted_nodes);
    try testing.expectEqual(@as(u64, n - deletes.items.len), s.live);
    var res = try store.search(alloc, .{ .query = data[10 * dims ..][0..dims], .k = 1 });
    defer res.deinit();
    try testing.expectEqual(@as(u64, 10), res.items.items[0].vector_id);
}

test "leann store: rejects a corrupt file" {
    const alloc = testing.allocator;
    const io = testing.io;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(dir);
    try tmp.dir.writeFile(io, .{ .sub_path = file_name, .data = "LEANNSTRgarbage-garbage" });
    try testing.expectError(error.InvalidLeannStore, LeannStore.open(alloc, io, dir, .{ .dims = 4, .metric = .l2_squared }));
}

test "leann store: allocation failures during upserts never free buffered vectors twice" {
    const alloc = testing.allocator;
    const io = testing.io;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(dir);

    const dims = 16;
    const n = 40;
    const data = try testData(alloc, n, dims);
    defer alloc.free(data);
    var vectors = TestVectors{ .data = data, .dims = dims };
    // Transient storage also exercises noteCovered after graph inserts.
    const opts = Options{ .dims = dims, .metric = .l2_squared, .max_degree = 8, .build_threshold = 24, .quantizer = .rabitq, .transient = true };

    var fail_index: usize = 0;
    while (true) : (fail_index += 1) {
        var store = try LeannStore.open(alloc, io, dir, opts);
        defer store.deinit();
        store.setLoader(vectors.loader());
        var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = fail_index });
        store.alloc = failing.allocator();
        // One upsert at a time: the 24th builds the graph, later ones insert.
        var failed = false;
        for (0..n) |i| {
            addAll(alloc, store, data, dims, i, i + 1) catch |err| {
                try testing.expectEqual(error.OutOfMemory, err);
                failed = true;
                break;
            };
        }
        store.alloc = alloc;
        // Whatever was accepted stays usable and is freed exactly once.
        var res = try store.search(alloc, .{ .query = data[0..dims], .k = 1 });
        res.deinit();
        if (!failed) break;
    }
}

test "leann store: rejects graph settings the first build would refuse" {
    const alloc = testing.allocator;
    const io = testing.io;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(dir);
    // RaBitQ needs at least two dimensions.
    try testing.expectError(error.InvalidIndexConfig, LeannStore.open(alloc, io, dir, .{ .dims = 1, .metric = .l2_squared, .quantizer = .rabitq }));
    try testing.expectError(error.InvalidIndexConfig, LeannStore.open(alloc, io, dir, .{ .dims = 1, .metric = .l2_squared, .quantizer = .rabitq, .replicated = true }));
    var ok = try LeannStore.open(alloc, io, dir, .{ .dims = 1, .metric = .l2_squared, .quantizer = .pq });
    ok.deinit();
}

test "leann store: a failed persistent rebuild keeps every vector owned once" {
    const alloc = testing.allocator;
    const io = testing.io;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(dir);

    const dims = 16;
    const n = 60;
    const data = try testData(alloc, n, dims);
    defer alloc.free(data);
    var vectors = TestVectors{ .data = data, .dims = dims };
    const opts = Options{ .dims = dims, .metric = .l2_squared, .max_degree = 8, .build_threshold = 24 };

    var fail_index: usize = 0;
    while (true) : (fail_index += 1) {
        var store = try LeannStore.open(alloc, io, dir, opts);
        defer store.deinit();
        store.setLoader(vectors.loader());
        try addAll(alloc, store, data, dims, 0, n);
        try testing.expect(store.index != null);
        var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = fail_index });
        store.alloc = failing.allocator();
        store.lock.lockUncancelable(store.io);
        const result = store.rebuildLocked();
        store.lock.unlock(store.io);
        store.alloc = alloc;
        if (result) |_| {} else |err| try testing.expectEqual(error.OutOfMemory, err);
        var res = try store.search(alloc, .{ .query = data[0..dims], .k = 1 });
        res.deinit();
        if (!failing.has_induced_failure) break;
    }
}
