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
const vec = @import("antfly_vector").vector;
const replicated_mod = @import("leann_replicated.zig");
const replica_mod = @import("leann_replica.zig");

pub const file_name = "leann.store";
const magic = "LEANNSTR";
const format_version: u32 = 1;

pub const Options = struct {
    dims: u32,
    metric: vec.DistanceMetric,
    max_degree: u32 = 30,
    quantizer: leann.Quantizer = .rabitq,
    search_ef: u32 = 0,
    bound_scale: f32 = 1.0,
    batch_size: u32 = 0,
    /// Vectors served by exact search before the first graph build.
    build_threshold: u32 = 256,
    /// Rebuild on sync once this fraction of graph nodes is deleted.
    rebuild_deleted_fraction: f64 = 0.2,
    seed: u64 = 42,
    /// Leader replication: graph changes arrive only as committed records
    /// (see leann_replicated.zig); local writes just mark keys dirty.
    replicated: bool = false,
};

/// Loads the exact stored vector for a vector id. Same shape as the HBC
/// external vector loader, which the adapter passes through.
pub const VectorLoader = struct {
    ctx: *anyopaque,
    load: *const fn (ctx: *anyopaque, alloc: Allocator, vector_id: u64, metadata: []const u8) anyerror![]f32,
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
    /// Set in replicated mode; the local-mode fields above stay empty.
    replicated: ?*replicated_mod.ReplicatedState = null,
    durable_raft_index: u64 = 0,

    /// Opens the store kept in `dir_path`, loading its file when present.
    pub fn open(alloc: Allocator, io: std.Io, dir_path: []const u8, opts: Options) !*LeannStore {
        if (opts.dims == 0) return error.InvalidIndexConfig;
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
            const existing = std.Io.Dir.cwd().readFileAlloc(io, self.path, alloc, .unlimited) catch |err| switch (err) {
                error.FileNotFound => null,
                else => return err,
            };
            defer if (existing) |b| alloc.free(b);
            state.* = if (existing) |b| try replicated_mod.ReplicatedState.decode(alloc, ropts, b) else try replicated_mod.ReplicatedState.init(alloc, ropts);
            self.replicated = state;
            self.durable_raft_index = state.applied_raft_index;
            return self;
        }
        const bytes = std.Io.Dir.cwd().readFileAlloc(io, self.path, alloc, .unlimited) catch |err| switch (err) {
            error.FileNotFound => return self,
            else => return err,
        };
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
                    .max_degree = opts.max_degree,
                    .seed = opts.seed,
                },
                .build_threshold = opts.build_threshold,
            },
            .rebuild_deleted_fraction = opts.rebuild_deleted_fraction,
            .search_ef = opts.search_ef,
            .bound_scale = opts.bound_scale,
            .batch_size = opts.batch_size,
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
    }

    fn freePending(self: *LeannStore, p: Pending) void {
        self.alloc.free(p.metadata);
        self.alloc.free(p.vector);
    }

    pub fn setLoader(self: *LeannStore, loader: VectorLoader) void {
        self.loader = loader;
        if (self.replicated) |state| state.loader = .{ .ctx = loader.ctx, .load = loader.load };
    }

    pub fn isReplicated(self: *const LeannStore) bool {
        return self.replicated != null;
    }

    /// Replicated mode: where install records fetch chunked generations.
    pub fn setGenerationFetcher(self: *LeannStore, fetcher: replica_mod.GenerationFetcher) void {
        if (self.replicated) |state| state.fetcher = fetcher;
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
    pub fn applyRecord(self: *LeannStore, record: []const u8, raft_index: u64) !replica_mod.Outcome {
        const state = self.replicated orelse return error.NotReplicated;
        self.lock.lockUncancelable(self.io);
        defer self.lock.unlock(self.io);
        const outcome = try state.applyRecord(record, raft_index);
        self.dirty = true;
        return outcome;
    }

    /// Replicated mode: Raft index of the last applied record (0 if none).
    pub fn appliedRaftIndex(self: *LeannStore) u64 {
        const state = self.replicated orelse return 0;
        self.lock.lockSharedUncancelable(self.io);
        defer self.lock.unlockShared(self.io);
        return state.applied_raft_index;
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
            errdefer self.alloc.free(owned_meta);
            try self.nodes.ensureUnusedCapacity(self.alloc, 1);
            try self.node_of.ensureUnusedCapacity(self.alloc, 1);
            var ctx = SourceCtx{ .store = self, .far_from = vector };
            const id = try index.insert(ctx.source(), vector);
            std.debug.assert(id == self.nodes.items.len);
            self.nodes.appendAssumeCapacity(.{ .vector_id = vector_id, .metadata = owned_meta });
            self.node_of.putAssumeCapacity(vector_id, id);
            return;
        }
        const owned_meta = try self.alloc.dupe(u8, metadata);
        errdefer self.alloc.free(owned_meta);
        const owned_vec = try self.alloc.dupe(f32, vector);
        errdefer self.alloc.free(owned_vec);
        try self.pending_of.put(self.alloc, vector_id, @intCast(self.pending.items.len));
        errdefer _ = self.pending_of.remove(vector_id);
        try self.pending.append(self.alloc, .{ .vector_id = vector_id, .metadata = owned_meta, .vector = owned_vec });
        if (self.pending.items.len >= self.opts.build_threshold) try self.buildFromPendingLocked();
    }

    fn leannConfig(self: *const LeannStore) leann.Config {
        return .{
            .dims = self.opts.dims,
            .metric = @fromBackingInt(@intCast(@as(u8, @intCast(@backingInt(self.opts.metric))))),
            .quantizer = self.opts.quantizer,
            .max_degree = self.opts.max_degree,
            .seed = self.opts.seed,
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

        // Swap in the rebuilt state only after every vector loaded.
        if (self.index) |*index| index.deinit();
        self.index = null;
        for (self.nodes.items) |node| self.alloc.free(node.metadata);
        self.nodes.clearRetainingCapacity();
        self.node_of.clearRetainingCapacity();
        for (live.items) |p| {
            try self.pending_of.put(self.alloc, p.vector_id, @intCast(self.pending.items.len));
            try self.pending.append(self.alloc, p);
        }
        live.clearRetainingCapacity();
        if (self.pending.items.len >= self.opts.build_threshold) try self.buildFromPendingLocked();
    }

    // ---------------------------------------------------------------------
    // Search
    // ---------------------------------------------------------------------

    /// Answers an HBC search request with HBC's result type, so callers map
    /// vector ids and metadata exactly as they do for HBC.
    pub fn search(self: *LeannStore, alloc: Allocator, req: search_types.SearchRequest) !search_results.SearchResults {
        if (req.query.len != self.opts.dims) return error.DimensionMismatch;
        self.lock.lockSharedUncancelable(self.io);
        defer self.lock.unlockShared(self.io);
        if (self.replicated) |state| return try state.search(alloc, req);

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
        for (self.pending.items) |p| {
            if (!try filter.allows(p.vector_id, p.metadata)) continue;
            @memcpy(scratch, p.vector);
            if (self.opts.metric == .cosine) normalize(scratch);
            const dist = leann.distance(@fromBackingInt(@intCast(@as(u8, @intCast(@backingInt(self.opts.metric))))), q, scratch);
            if (!filter.distanceAllowed(dist)) continue;
            out.addResultWithOwnedMetadata(p.vector_id, dist, 0, if (req.load_metadata) try alloc.dupe(u8, p.metadata) else null);
        }

        if (self.index == null) return out;
        const index = &self.index.?;
        const live: usize = self.node_of.count();
        if (live == 0) return out;

        // Over-fetch when filters can reject graph results, widening until
        // k results pass or the whole graph has been considered.
        var want: usize = if (filter.active()) @max(req.k * 4, req.k + 64) else req.k;
        var ctx = SourceCtx{ .store = self, .far_from = req.query };
        while (true) {
            want = @min(want, live);
            var resp = try index.search(alloc, ctx.source(), req.query, .{
                .k = @intCast(want),
                .ef = if (self.opts.search_ef > 0) @max(self.opts.search_ef, @as(u32, @intCast(want))) else 0,
                .bound_scale = self.opts.bound_scale,
                .batch_size = self.opts.batch_size,
            });
            defer resp.deinit(alloc);
            var passed: usize = 0;
            for (resp.results) |r| {
                const node = self.nodes.items[r.id];
                if (!try filter.allows(node.vector_id, node.metadata)) continue;
                if (!filter.distanceAllowed(r.distance)) continue;
                passed += 1;
            }
            if (passed >= req.k or want >= live or !filter.active()) {
                for (resp.results) |r| {
                    const node = self.nodes.items[r.id];
                    if (!try filter.allows(node.vector_id, node.metadata)) continue;
                    if (!filter.distanceAllowed(r.distance)) continue;
                    out.addResultWithOwnedMetadata(node.vector_id, r.distance, 0, if (req.load_metadata) try alloc.dupe(u8, node.metadata) else null);
                }
                return out;
            }
            want *= 4;
        }
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
        far_from: []const f32,

        fn source(self: *SourceCtx) leann.EmbeddingSource {
            return .{ .ctx = self, .recompute_fn = recompute };
        }

        fn recompute(raw: *anyopaque, ids: []const u32, out: []f32) anyerror!void {
            const self: *SourceCtx = @ptrCast(@alignCast(raw));
            const store = self.store;
            const d: usize = store.opts.dims;
            for (ids, 0..) |id, i| {
                const row = out[i * d ..][0..d];
                const node = store.nodes.items[id];
                const loaded: ?[]f32 = blk: {
                    if (store.index.?.isDeleted(id) or node.metadata.len == 0) break :blk null;
                    const loader = store.loader orelse return error.MissingVectorLoader;
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
                    for (row, self.far_from) |*o, x| o.* = -1000 * x;
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
            try writeFileAtomicallyDurable(self.alloc, self.io, self.path, bytes);
            self.durable_raft_index = state.applied_raft_index;
            self.dirty = false;
            return;
        }
        if (self.index) |*index| {
            if (index.deletedFraction() >= self.opts.rebuild_deleted_fraction and self.loader != null) {
                try self.rebuildLocked();
                self.dirty = true;
            }
        }
        if (!self.dirty) return;
        const bytes = try self.encode();
        defer self.alloc.free(bytes);
        try writeFileAtomicallyDurable(self.alloc, self.io, self.path, bytes);
        self.dirty = false;
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
        const crc = std.hash.Crc32.hash(w.written());
        try wr.writeInt(u32, crc, .little);
        return w.toOwnedSlice();
    }

    fn decode(self: *LeannStore, bytes: []const u8) !void {
        if (bytes.len < magic.len + 8) return error.InvalidLeannStore;
        const body = bytes[0 .. bytes.len - 4];
        if (std.hash.Crc32.hash(body) != std.mem.readInt(u32, bytes[bytes.len - 4 ..][0..4], .little)) return error.InvalidLeannStore;
        var r: std.Io.Reader = .fixed(body);
        const head = r.take(magic.len) catch return error.InvalidLeannStore;
        if (!std.mem.eql(u8, head, magic)) return error.InvalidLeannStore;
        if ((r.takeInt(u32, .little) catch return error.InvalidLeannStore) != format_version) return error.InvalidLeannStore;
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
        if (r.bufferedLen() != 0) return error.InvalidLeannStore;
    }
};

/// HBC request filters, applied to graph and buffer candidates.
const Filter = struct {
    allowed: std.AutoHashMapUnmanaged(u64, void) = .empty,
    excluded: std.AutoHashMapUnmanaged(u64, void) = .empty,
    has_allowed: bool,
    key_predicate: ?search_types.KeyPredicate,
    prefix: []const u8,
    over: ?f32,
    under: ?f32,

    fn init(alloc: Allocator, req: search_types.SearchRequest) !Filter {
        var self = Filter{
            .has_allowed = req.filter_ids.len > 0,
            .key_predicate = req.key_predicate,
            .prefix = req.filter_prefix,
            .over = req.distance_over,
            .under = req.distance_under,
        };
        errdefer self.deinit(alloc);
        for (req.filter_ids) |id| try self.allowed.put(alloc, id, {});
        for (req.exclude_ids) |id| try self.excluded.put(alloc, id, {});
        return self;
    }

    fn deinit(self: *Filter, alloc: Allocator) void {
        self.allowed.deinit(alloc);
        self.excluded.deinit(alloc);
    }

    fn active(self: *const Filter) bool {
        return self.has_allowed or self.excluded.count() > 0 or self.key_predicate != null or
            self.prefix.len > 0 or self.over != null or self.under != null;
    }

    fn allows(self: *const Filter, vector_id: u64, metadata: []const u8) !bool {
        if (self.has_allowed and !self.allowed.contains(vector_id)) return false;
        if (self.excluded.contains(vector_id)) return false;
        if (self.prefix.len > 0 and !std.mem.startsWith(u8, metadata, self.prefix)) return false;
        if (self.key_predicate) |p| if (!try p.allows(p.ptr, metadata)) return false;
        return true;
    }

    fn distanceAllowed(self: *const Filter, dist: f32) bool {
        if (self.over) |t| if (!(dist > t)) return false;
        if (self.under) |t| if (!(dist < t)) return false;
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
        var best = res.items.items[0];
        for (res.items.items) |item| if (item.distance < best.distance) {
            best = item;
        };
        try testing.expectEqual(id, best.vector_id);
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
