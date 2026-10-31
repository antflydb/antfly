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

//! Leader-replicated state machine for a LEANN index.
//!
//! Only the shard leader computes graph changes. It plans them into a
//! `Record`, which is committed through the replicated log; every replica,
//! the leader included, applies committed records in log order. Replicas
//! therefore hold byte-identical graphs, even on hardware whose
//! floating-point results differ, because no replica other than the planner
//! computes a distance.
//!
//! Nodes are keyed by document or artifact key, which is the same on every
//! replica (vector ids are assigned locally and are not).
//!
//! Records carry the state they were planned against (`generation`,
//! `version`). A record planned against a different state is skipped, and
//! because every replica sees the same records in the same order, every
//! replica skips it. The leader then re-plans the affected keys. A record
//! that fails validation is rolled back and rejected the same way. Neither
//! case needs a lock between planning and commit, and a leader change
//! mid-flight only causes a re-plan.
//!
//! Two kinds of content:
//!
//! - Writes. Before the first graph exists, records carry raw vectors for an
//!   exact buffer. Afterwards they carry `leann.Delta`s.
//! - Generations. Graph builds (the first one, and rebuilds after deletes)
//!   are computed on the leader and installed by a record holding either
//!   the generation bytes inline (small) or only their digest; replicas then
//!   fetch large generations out of band through a `GenerationFetcher`.

const std = @import("std");
const Allocator = std.mem.Allocator;
const leann = @import("antfly_vectorindex").leann;

pub const Options = struct {
    config: leann.Config,
    /// Buffered vectors that trigger the first graph build.
    build_threshold: u32 = 256,
    /// Generations up to this size are carried inside the install record.
    inline_generation_limit: usize = 1 << 20,
};

/// Loads the exact vector for a key (the stored embedding). Used only by
/// the planner and by generation builds.
pub const VectorLoader = struct {
    ctx: *anyopaque,
    load: *const fn (ctx: *anyopaque, alloc: Allocator, key: []const u8) anyerror![]f32,
};

/// Fetches the bytes of a generation whose install record did not inline
/// them. Implementations verify nothing; `Replica` checks the digest.
pub const GenerationFetcher = struct {
    ctx: *anyopaque,
    fetch: *const fn (ctx: *anyopaque, alloc: Allocator, generation: u64, digest: u64) anyerror![]u8,
};

/// One unit of planner input: an upsert (`vector` set) or a delete.
pub const Work = struct {
    key: []const u8,
    vector: ?[]const f32,
};

pub const Outcome = enum {
    applied,
    /// Planned against another (generation, version); nothing changed.
    stale,
    /// Failed validation; rolled back, nothing changed.
    rejected,
};

const OpKind = enum(u8) {
    buffer_put = 1,
    buffer_delete = 2,
    graph_insert = 3,
    graph_delete = 4,
    install_generation = 5,
};

const record_magic: u8 = 0x4c; // 'L'
const generation_magic = "LEANNGEN";
const generation_format: u32 = 1;

pub const Replica = struct {
    alloc: Allocator,
    opts: Options,
    /// 0 until the first graph build is installed.
    generation: u64 = 0,
    /// Records applied since the current generation was installed (or since
    /// creation, before the first build).
    version: u64 = 0,
    index: ?leann.Index = null,
    /// Graph node id -> key. Deleted nodes keep their key until a rebuild.
    keys: std.ArrayListUnmanaged([]u8) = .empty,
    /// Key -> live graph node.
    node_of: std.StringHashMapUnmanaged(u32) = .empty,
    /// Exact buffer before the first build, in commit order.
    buffer_keys: std.ArrayListUnmanaged([]u8) = .empty,
    buffer_vectors: std.ArrayListUnmanaged([]f32) = .empty,

    pub fn init(alloc: Allocator, opts: Options) !Replica {
        _ = try opts.config.resolved();
        return .{ .alloc = alloc, .opts = opts };
    }

    pub fn deinit(self: *Replica) void {
        self.clear();
        self.keys.deinit(self.alloc);
        self.node_of.deinit(self.alloc);
        self.buffer_keys.deinit(self.alloc);
        self.buffer_vectors.deinit(self.alloc);
        self.* = undefined;
    }

    fn clear(self: *Replica) void {
        if (self.index) |*index| index.deinit();
        self.index = null;
        for (self.keys.items) |k| self.alloc.free(k);
        self.keys.clearRetainingCapacity();
        self.node_of.clearRetainingCapacity();
        for (self.buffer_keys.items) |k| self.alloc.free(k);
        for (self.buffer_vectors.items) |v| self.alloc.free(v);
        self.buffer_keys.clearRetainingCapacity();
        self.buffer_vectors.clearRetainingCapacity();
    }

    pub fn liveCount(self: *const Replica) usize {
        return self.node_of.count() + self.buffer_keys.items.len;
    }

    fn bufferIndex(self: *const Replica, key: []const u8) ?usize {
        for (self.buffer_keys.items, 0..) |k, i| if (std.mem.eql(u8, k, key)) return i;
        return null;
    }

    pub fn contains(self: *const Replica, key: []const u8) bool {
        return self.node_of.contains(key) or self.bufferIndex(key) != null;
    }

    // ---------------------------------------------------------------------
    // Planning (leader only)
    // ---------------------------------------------------------------------

    /// Plans `work` into an encoded record, or returns null when there is
    /// nothing to change. Leaves this replica's state unchanged: ops are
    /// applied to plan the next one, then reverted. When the buffer reaches
    /// the build threshold, the record ends with the first generation.
    pub fn plan(self: *Replica, alloc: Allocator, work: []const Work, loader: VectorLoader) !?[]u8 {
        var ops = OpList{};
        defer ops.deinit(alloc);
        var undo = UndoLog{};
        defer undo.deinit(self);
        errdefer self.revert(&undo);

        for (work) |w| {
            if (w.vector) |v| if (v.len != self.opts.config.dims) return error.DimensionMismatch;
            if (self.index != null) {
                if (self.node_of.get(w.key)) |node| {
                    var delta = try self.index.?.planDelete(node);
                    try self.applyGraphDelete(w.key, delta, &undo);
                    try ops.append(alloc, .{ .kind = .graph_delete, .key = w.key, .delta = try delta.encode(alloc) });
                    delta.deinit(alloc);
                }
                if (w.vector) |v| {
                    var ctx = PlanSource{ .replica = self, .loader = loader, .far_from = v };
                    var delta = try self.index.?.planInsert(alloc, ctx.source(), v);
                    defer delta.deinit(alloc);
                    try self.applyGraphInsert(w.key, delta, &undo);
                    try ops.append(alloc, .{ .kind = .graph_insert, .key = w.key, .delta = try delta.encode(alloc) });
                }
            } else {
                if (w.vector) |v| {
                    try self.applyBufferPut(w.key, v, &undo);
                    try ops.append(alloc, .{ .kind = .buffer_put, .key = w.key, .vector = v });
                } else if (self.bufferIndex(w.key) != null) {
                    try self.applyBufferDelete(w.key, &undo);
                    try ops.append(alloc, .{ .kind = .buffer_delete, .key = w.key });
                }
            }
        }

        var generation_bytes: ?[]u8 = null;
        defer if (generation_bytes) |b| alloc.free(b);
        if (self.index == null and self.buffer_keys.items.len >= self.opts.build_threshold) {
            generation_bytes = try self.buildFromBuffer(alloc);
        }
        self.revert(&undo);
        if (ops.items.items.len == 0 and generation_bytes == null) return null;
        if (generation_bytes) |bytes| {
            try ops.append(alloc, .{
                .kind = .install_generation,
                .generation = self.generation + 1,
                .digest = std.hash.Wyhash.hash(0, bytes),
                .payload = bytes,
            });
        }
        return try encodeRecord(alloc, self.generation, self.version, ops.items.items);
    }

    /// Plans a rebuild of the graph from the live keys' stored vectors.
    /// Returns the install record and the generation bytes; when the bytes
    /// exceed the inline limit the record holds only their digest, and the
    /// caller must make the bytes fetchable by `generation` and digest.
    pub fn planRebuild(self: *Replica, alloc: Allocator, loader: VectorLoader) !struct { record: []u8, generation: u64, digest: u64, bytes: []u8 } {
        if (self.index == null) return error.NoGraph;
        var live_keys = std.ArrayListUnmanaged([]const u8).empty;
        defer live_keys.deinit(alloc);
        for (self.keys.items, 0..) |k, i| if (!self.index.?.isDeleted(@intCast(i))) try live_keys.append(alloc, k);
        const bytes = try self.buildGenerationBytes(alloc, live_keys.items, loader);
        errdefer alloc.free(bytes);
        const generation = self.generation + 1;
        const digest = std.hash.Wyhash.hash(0, bytes);
        const record = try encodeRecord(alloc, self.generation, self.version, &.{.{
            .kind = .install_generation,
            .generation = generation,
            .digest = digest,
            .payload = if (bytes.len <= self.opts.inline_generation_limit) bytes else null,
        }});
        return .{ .record = record, .generation = generation, .digest = digest, .bytes = bytes };
    }

    fn buildFromBuffer(self: *Replica, alloc: Allocator) ![]u8 {
        const Ctx = struct {
            replica: *Replica,
            fn load(raw: *anyopaque, a: Allocator, key: []const u8) anyerror![]f32 {
                const c: *@This() = @ptrCast(@alignCast(raw));
                const i = c.replica.bufferIndex(key) orelse return error.NotFound;
                return try a.dupe(f32, c.replica.buffer_vectors.items[i]);
            }
        };
        var ctx = Ctx{ .replica = self };
        const keys: []const []const u8 = self.buffer_keys.items;
        return try self.buildGenerationBytes(alloc, keys, .{ .ctx = &ctx, .load = Ctx.load });
    }

    /// Builds a graph over `keys` (in that order) and encodes it as
    /// generation bytes.
    fn buildGenerationBytes(self: *Replica, alloc: Allocator, keys: []const []const u8, loader: VectorLoader) ![]u8 {
        const d: usize = self.opts.config.dims;
        const flat = try alloc.alloc(f32, keys.len * d);
        defer alloc.free(flat);
        for (keys, 0..) |k, i| {
            const v = try loader.load(loader.ctx, alloc, k);
            defer alloc.free(v);
            if (v.len != d) return error.DimensionMismatch;
            @memcpy(flat[i * d ..][0..d], v);
        }
        var index = try leann.Index.build(alloc, self.opts.config, flat);
        defer index.deinit();
        const graph = try index.serialize(alloc);
        defer alloc.free(graph);
        var w: std.Io.Writer.Allocating = .init(alloc);
        defer w.deinit();
        const wr = &w.writer;
        try wr.writeAll(generation_magic);
        try wr.writeInt(u32, generation_format, .little);
        try wr.writeInt(u32, @intCast(keys.len), .little);
        for (keys) |k| try writeBytes(wr, k);
        try writeBytes(wr, graph);
        return w.toOwnedSlice();
    }

    // ---------------------------------------------------------------------
    // Applying committed records (every replica)
    // ---------------------------------------------------------------------

    /// Applies one committed record. `fetcher` supplies generations that
    /// were not inlined; if it fails, the error is returned and the record
    /// must be retried (state is unchanged).
    pub fn apply(self: *Replica, record: []const u8, fetcher: ?GenerationFetcher) !Outcome {
        var decoded = try decodeRecord(self.alloc, record);
        defer decoded.deinit(self.alloc);
        if (decoded.generation != self.generation or decoded.version != self.version) return .stale;

        // Fetch any generation first so a fetch failure changes nothing.
        var fetched: ?[]u8 = null;
        defer if (fetched) |b| self.alloc.free(b);
        for (decoded.ops) |op| if (op.kind == .install_generation and op.payload == null) {
            const f = fetcher orelse return error.GenerationUnavailable;
            fetched = try f.fetch(f.ctx, self.alloc, op.generation, op.digest);
        };

        var undo = UndoLog{};
        defer undo.deinit(self);
        for (decoded.ops) |op| {
            self.applyOp(op, fetched, &undo) catch |err| switch (err) {
                error.OutOfMemory => {
                    self.revert(&undo);
                    return err;
                },
                else => {
                    self.revert(&undo);
                    return .rejected;
                },
            };
        }
        undo.commit(self);
        self.version += 1;
        if (undo.installed) self.version = 0;
        return .applied;
    }

    fn applyOp(self: *Replica, op: Op, fetched: ?[]const u8, undo: *UndoLog) !void {
        switch (op.kind) {
            .buffer_put => {
                if (self.index != null) return error.InvalidRecord;
                const v = op.vector orelse return error.InvalidRecord;
                if (v.len != self.opts.config.dims) return error.InvalidRecord;
                try self.applyBufferPut(op.key, v, undo);
            },
            .buffer_delete => {
                if (self.index != null or self.bufferIndex(op.key) == null) return error.InvalidRecord;
                try self.applyBufferDelete(op.key, undo);
            },
            .graph_insert, .graph_delete => {
                if (self.index == null) return error.InvalidRecord;
                var delta = try leann.Delta.decode(self.alloc, op.delta orelse return error.InvalidRecord);
                defer delta.deinit(self.alloc);
                if (op.kind == .graph_insert) {
                    if (delta.kind != .insert or self.node_of.contains(op.key)) return error.InvalidRecord;
                    try self.applyGraphInsert(op.key, delta, undo);
                } else {
                    const node = self.node_of.get(op.key) orelse return error.InvalidRecord;
                    if (delta.kind != .delete or delta.id != node) return error.InvalidRecord;
                    try self.applyGraphDelete(op.key, delta, undo);
                }
            },
            .install_generation => {
                if (op.generation != self.generation + 1) return error.InvalidRecord;
                const bytes = op.payload orelse fetched orelse return error.InvalidRecord;
                if (std.hash.Wyhash.hash(0, bytes) != op.digest) return error.InvalidRecord;
                try self.installGenerationBytes(bytes, op.generation, undo);
            },
        }
    }

    fn applyBufferPut(self: *Replica, key: []const u8, v: []const f32, undo: *UndoLog) !void {
        const owned_v = try self.alloc.dupe(f32, v);
        errdefer self.alloc.free(owned_v);
        if (self.bufferIndex(key)) |i| {
            try undo.entries.append(self.alloc, .{ .buffer_replaced = .{ .index = i, .old = self.buffer_vectors.items[i] } });
            self.buffer_vectors.items[i] = owned_v;
            return;
        }
        const owned_k = try self.alloc.dupe(u8, key);
        errdefer self.alloc.free(owned_k);
        try self.buffer_keys.ensureUnusedCapacity(self.alloc, 1);
        try self.buffer_vectors.ensureUnusedCapacity(self.alloc, 1);
        try undo.entries.append(self.alloc, .buffer_appended);
        self.buffer_keys.appendAssumeCapacity(owned_k);
        self.buffer_vectors.appendAssumeCapacity(owned_v);
    }

    fn applyBufferDelete(self: *Replica, key: []const u8, undo: *UndoLog) !void {
        const i = self.bufferIndex(key).?;
        try undo.entries.ensureUnusedCapacity(self.alloc, 1);
        // Ordered remove keeps commit order, which the build depends on.
        const k = self.buffer_keys.orderedRemove(i);
        const v = self.buffer_vectors.orderedRemove(i);
        undo.entries.appendAssumeCapacity(.{ .buffer_removed = .{ .index = i, .key = k, .vector = v } });
    }

    fn applyGraphInsert(self: *Replica, key: []const u8, delta: leann.Delta, undo: *UndoLog) !void {
        const owned_k = try self.alloc.dupe(u8, key);
        errdefer self.alloc.free(owned_k);
        try self.keys.ensureUnusedCapacity(self.alloc, 1);
        try self.node_of.ensureUnusedCapacity(self.alloc, 1);
        try undo.entries.ensureUnusedCapacity(self.alloc, 1);
        const delta_copy = try cloneDelta(self.alloc, delta);
        errdefer {
            var dc = delta_copy;
            dc.deinit(self.alloc);
        }
        const graph_undo = try self.index.?.applyDeltaUndoable(delta);
        self.keys.appendAssumeCapacity(owned_k);
        self.node_of.putAssumeCapacity(owned_k, delta.id);
        undo.entries.appendAssumeCapacity(.{ .graph = .{ .delta = delta_copy, .undo = graph_undo } });
    }

    fn applyGraphDelete(self: *Replica, key: []const u8, delta: leann.Delta, undo: *UndoLog) !void {
        try undo.entries.ensureUnusedCapacity(self.alloc, 1);
        const delta_copy = try cloneDelta(self.alloc, delta);
        errdefer {
            var dc = delta_copy;
            dc.deinit(self.alloc);
        }
        const graph_undo = try self.index.?.applyDeltaUndoable(delta);
        _ = self.node_of.remove(key);
        undo.entries.appendAssumeCapacity(.{ .graph = .{ .delta = delta_copy, .undo = graph_undo } });
    }

    fn installGenerationBytes(self: *Replica, bytes: []const u8, generation: u64, undo: *UndoLog) !void {
        var decoded = try decodeGeneration(self.alloc, bytes, self.opts.config.dims);
        errdefer decoded.deinit(self.alloc);
        // Every buffered key must be in the generation or deleted since;
        // generations replace the whole state.
        try undo.entries.ensureUnusedCapacity(self.alloc, 1);
        const saved = Saved{
            .generation = self.generation,
            .version = self.version,
            .index = self.index,
            .keys = self.keys,
            .node_of = self.node_of,
            .buffer_keys = self.buffer_keys,
            .buffer_vectors = self.buffer_vectors,
        };
        self.index = decoded.index;
        self.keys = decoded.keys;
        self.node_of = decoded.node_of;
        self.buffer_keys = .empty;
        self.buffer_vectors = .empty;
        self.generation = generation;
        undo.entries.appendAssumeCapacity(.{ .installed = saved });
        undo.installed = true;
    }

    fn revert(self: *Replica, undo: *UndoLog) void {
        var i = undo.entries.items.len;
        while (i > 0) {
            i -= 1;
            switch (undo.entries.items[i]) {
                .buffer_appended => {
                    self.alloc.free(self.buffer_keys.pop().?);
                    self.alloc.free(self.buffer_vectors.pop().?);
                },
                .buffer_replaced => |r| {
                    self.alloc.free(self.buffer_vectors.items[r.index]);
                    self.buffer_vectors.items[r.index] = r.old;
                },
                .buffer_removed => |r| {
                    self.buffer_keys.insertAssumeCapacity(r.index, r.key);
                    self.buffer_vectors.insertAssumeCapacity(r.index, r.vector);
                },
                .graph => |*g| {
                    if (g.delta.kind == .insert) {
                        const key = self.keys.pop().?;
                        _ = self.node_of.remove(key);
                        self.alloc.free(key);
                    } else {
                        self.node_of.putAssumeCapacity(self.keys.items[g.delta.id], g.delta.id);
                    }
                    self.index.?.revertDelta(g.delta, g.undo);
                    g.delta.deinit(self.alloc);
                },
                .installed => |s| {
                    var current = State{ .index = self.index, .keys = self.keys, .node_of = self.node_of };
                    current.deinit(self.alloc);
                    self.generation = s.generation;
                    self.version = s.version;
                    self.index = s.index;
                    self.keys = s.keys;
                    self.node_of = s.node_of;
                    self.buffer_keys = s.buffer_keys;
                    self.buffer_vectors = s.buffer_vectors;
                },
            }
        }
        undo.entries.clearRetainingCapacity();
        undo.installed = false;
    }

    // ---------------------------------------------------------------------
    // Search
    // ---------------------------------------------------------------------

    pub const Hit = struct { key: []const u8, distance: f32 };

    /// k nearest live keys. Keys borrow from the replica until the next
    /// state change. `source` recomputes exact vectors by graph node id;
    /// see `nodeKey`.
    pub fn search(self: *const Replica, alloc: Allocator, query: []const f32, k: u32, loader: VectorLoader) ![]Hit {
        var hits = std.ArrayListUnmanaged(Hit).empty;
        errdefer hits.deinit(alloc);
        const metric = self.opts.config.metric;
        const q = try alloc.dupe(f32, query);
        defer alloc.free(q);
        if (metric == .cosine) normalize(q);
        const tmp = try alloc.alloc(f32, query.len);
        defer alloc.free(tmp);
        for (self.buffer_keys.items, self.buffer_vectors.items) |key, v| {
            @memcpy(tmp, v);
            if (metric == .cosine) normalize(tmp);
            try hits.append(alloc, .{ .key = key, .distance = leann.distance(metric, q, tmp) });
        }
        if (self.index) |*index| {
            var ctx = SearchSource{ .replica = self, .loader = loader, .far_from = query };
            var resp = try index.search(alloc, ctx.source(), query, .{ .k = k });
            defer resp.deinit(alloc);
            for (resp.results) |r| try hits.append(alloc, .{ .key = self.keys.items[r.id], .distance = r.distance });
        }
        std.mem.sortUnstable(Hit, hits.items, {}, struct {
            fn lt(_: void, a: Hit, b: Hit) bool {
                return a.distance < b.distance;
            }
        }.lt);
        if (hits.items.len > k) hits.shrinkRetainingCapacity(k);
        return hits.toOwnedSlice(alloc);
    }

    // ---------------------------------------------------------------------
    // State persistence (local; replicas also persist the log position)
    // ---------------------------------------------------------------------

    /// Encodes the full replica state. Two replicas that applied the same
    /// records produce identical bytes.
    pub fn encodeState(self: *const Replica, alloc: Allocator) ![]u8 {
        var w: std.Io.Writer.Allocating = .init(alloc);
        defer w.deinit();
        const wr = &w.writer;
        try wr.writeAll("LEANNRPL");
        try wr.writeInt(u64, self.generation, .little);
        try wr.writeInt(u64, self.version, .little);
        try wr.writeInt(u32, @intCast(self.keys.items.len), .little);
        for (self.keys.items) |k| try writeBytes(wr, k);
        if (self.index) |*index| {
            const graph = try index.serialize(alloc);
            defer alloc.free(graph);
            try wr.writeByte(1);
            try writeBytes(wr, graph);
        } else try wr.writeByte(0);
        try wr.writeInt(u32, @intCast(self.buffer_keys.items.len), .little);
        for (self.buffer_keys.items, self.buffer_vectors.items) |k, v| {
            try writeBytes(wr, k);
            for (v) |f| try wr.writeInt(u32, @bitCast(f), .little);
        }
        return w.toOwnedSlice();
    }

    pub fn decodeState(alloc: Allocator, opts: Options, bytes: []const u8) !Replica {
        var self = try Replica.init(alloc, opts);
        errdefer self.deinit();
        var r: std.Io.Reader = .fixed(bytes);
        const head = r.take(8) catch return error.InvalidState;
        if (!std.mem.eql(u8, head, "LEANNRPL")) return error.InvalidState;
        self.generation = r.takeInt(u64, .little) catch return error.InvalidState;
        self.version = r.takeInt(u64, .little) catch return error.InvalidState;
        const n = r.takeInt(u32, .little) catch return error.InvalidState;
        for (0..n) |_| {
            const k = try alloc.dupe(u8, try readBytes(&r));
            errdefer alloc.free(k);
            try self.keys.append(alloc, k);
        }
        const has_graph = r.takeByte() catch return error.InvalidState;
        if (has_graph == 1) {
            self.index = try leann.Index.deserialize(alloc, try readBytes(&r));
            if (self.index.?.node_count != n) return error.InvalidState;
            for (self.keys.items, 0..) |k, i| {
                if (!self.index.?.isDeleted(@intCast(i))) try self.node_of.put(alloc, k, @intCast(i));
            }
        } else if (has_graph != 0 or n != 0) return error.InvalidState;
        const nb = r.takeInt(u32, .little) catch return error.InvalidState;
        for (0..nb) |_| {
            const k = try alloc.dupe(u8, try readBytes(&r));
            errdefer alloc.free(k);
            const v = try alloc.alloc(f32, opts.config.dims);
            errdefer alloc.free(v);
            for (v) |*f| f.* = @bitCast(r.takeInt(u32, .little) catch return error.InvalidState);
            try self.buffer_keys.append(alloc, k);
            try self.buffer_vectors.append(alloc, v);
        }
        if (r.bufferedLen() != 0) return error.InvalidState;
        return self;
    }

    // ---------------------------------------------------------------------
    // Embedding sources
    // ---------------------------------------------------------------------

    /// Planner source: exact vectors by node id through the key loader.
    const PlanSource = struct {
        replica: *const Replica,
        loader: VectorLoader,
        far_from: []const f32,

        fn source(self: *PlanSource) leann.EmbeddingSource {
            return .{ .ctx = self, .recompute_fn = recompute };
        }

        fn recompute(raw: *anyopaque, ids: []const u32, out: []f32) anyerror!void {
            const self: *PlanSource = @ptrCast(@alignCast(raw));
            return recomputeByKey(self.replica, self.loader, self.far_from, ids, out);
        }
    };

    const SearchSource = PlanSource;
};

fn recomputeByKey(replica: *const Replica, loader: VectorLoader, far_from: []const f32, ids: []const u32, out: []f32) !void {
    const d: usize = replica.opts.config.dims;
    for (ids, 0..) |id, i| {
        const row = out[i * d ..][0..d];
        const loaded: ?[]f32 = if (replica.index.?.isDeleted(id))
            null
        else
            loader.load(loader.ctx, replica.alloc, replica.keys.items[id]) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
        if (loaded) |v| {
            defer replica.alloc.free(v);
            if (v.len != d) return error.DimensionMismatch;
            @memcpy(row, v);
        } else {
            for (row, far_from) |*o, x| o.* = -1000 * x;
        }
    }
}

const State = struct {
    index: ?leann.Index,
    keys: std.ArrayListUnmanaged([]u8),
    node_of: std.StringHashMapUnmanaged(u32),

    fn deinit(self: *State, alloc: Allocator) void {
        if (self.index) |*index| index.deinit();
        for (self.keys.items) |k| alloc.free(k);
        self.keys.deinit(alloc);
        self.node_of.deinit(alloc);
    }
};

const Saved = struct {
    generation: u64,
    version: u64,
    index: ?leann.Index,
    keys: std.ArrayListUnmanaged([]u8),
    node_of: std.StringHashMapUnmanaged(u32),
    buffer_keys: std.ArrayListUnmanaged([]u8),
    buffer_vectors: std.ArrayListUnmanaged([]f32),
};

const UndoEntry = union(enum) {
    buffer_appended,
    buffer_replaced: struct { index: usize, old: []f32 },
    buffer_removed: struct { index: usize, key: []u8, vector: []f32 },
    graph: struct { delta: leann.Delta, undo: leann.Index.Undo },
    installed: Saved,
};

const UndoLog = struct {
    entries: std.ArrayListUnmanaged(UndoEntry) = .empty,
    installed: bool = false,

    /// Drops what reverting would have restored.
    fn commit(self: *UndoLog, replica: *Replica) void {
        const alloc = replica.alloc;
        for (self.entries.items) |*e| switch (e.*) {
            .buffer_appended => {},
            .buffer_replaced => |r| alloc.free(r.old),
            .buffer_removed => |r| {
                alloc.free(r.key);
                alloc.free(r.vector);
            },
            .graph => |*g| g.delta.deinit(alloc),
            .installed => |s| {
                var old = State{ .index = s.index, .keys = s.keys, .node_of = s.node_of };
                old.deinit(alloc);
                var bk = s.buffer_keys;
                var bv = s.buffer_vectors;
                for (bk.items) |k| alloc.free(k);
                for (bv.items) |v| alloc.free(v);
                bk.deinit(alloc);
                bv.deinit(alloc);
            },
        };
        self.entries.clearRetainingCapacity();
    }

    fn deinit(self: *UndoLog, replica: *Replica) void {
        // Callers commit or revert before this; anything left is reverted
        // by the error path, so only the list itself is freed here.
        self.entries.deinit(replica.alloc);
    }
};

fn cloneDelta(alloc: Allocator, d: leann.Delta) !leann.Delta {
    const bytes = try d.encode(alloc);
    defer alloc.free(bytes);
    return try leann.Delta.decode(alloc, bytes);
}

// -------------------------------------------------------------------------
// Record and generation encoding
// -------------------------------------------------------------------------

const Op = struct {
    kind: OpKind,
    key: []const u8 = "",
    vector: ?[]const f32 = null,
    delta: ?[]const u8 = null,
    generation: u64 = 0,
    digest: u64 = 0,
    payload: ?[]const u8 = null,
};

const OpList = struct {
    items: std.ArrayListUnmanaged(Op) = .empty,
    owned: std.ArrayListUnmanaged([]u8) = .empty,

    fn append(self: *OpList, alloc: Allocator, op: Op) !void {
        if (op.delta) |d| try self.owned.append(alloc, @constCast(d));
        try self.items.append(alloc, op);
    }

    fn deinit(self: *OpList, alloc: Allocator) void {
        for (self.owned.items) |b| alloc.free(b);
        self.owned.deinit(alloc);
        self.items.deinit(alloc);
    }
};

fn encodeRecord(alloc: Allocator, generation: u64, version: u64, ops: []const Op) ![]u8 {
    var w: std.Io.Writer.Allocating = .init(alloc);
    defer w.deinit();
    const wr = &w.writer;
    try wr.writeByte(record_magic);
    try wr.writeInt(u64, generation, .little);
    try wr.writeInt(u64, version, .little);
    try wr.writeInt(u32, @intCast(ops.len), .little);
    for (ops) |op| {
        try wr.writeByte(@backingInt(op.kind));
        switch (op.kind) {
            .buffer_put => {
                try writeBytes(wr, op.key);
                const v = op.vector.?;
                try wr.writeInt(u32, @intCast(v.len), .little);
                for (v) |f| try wr.writeInt(u32, @bitCast(f), .little);
            },
            .buffer_delete => try writeBytes(wr, op.key),
            .graph_insert, .graph_delete => {
                try writeBytes(wr, op.key);
                try writeBytes(wr, op.delta.?);
            },
            .install_generation => {
                try wr.writeInt(u64, op.generation, .little);
                try wr.writeInt(u64, op.digest, .little);
                if (op.payload) |p| {
                    try wr.writeByte(1);
                    try writeBytes(wr, p);
                } else try wr.writeByte(0);
            },
        }
    }
    const crc = std.hash.Crc32.hash(w.written());
    try wr.writeInt(u32, crc, .little);
    return w.toOwnedSlice();
}

const DecodedRecord = struct {
    generation: u64,
    version: u64,
    ops: []Op,
    vectors: std.ArrayListUnmanaged([]f32) = .empty,

    fn deinit(self: *DecodedRecord, alloc: Allocator) void {
        for (self.vectors.items) |v| alloc.free(v);
        self.vectors.deinit(alloc);
        alloc.free(self.ops);
    }
};

/// Decodes a record. Keys, deltas and payloads borrow from `bytes`.
fn decodeRecord(alloc: Allocator, bytes: []const u8) !DecodedRecord {
    if (bytes.len < 25) return error.InvalidRecord;
    const body = bytes[0 .. bytes.len - 4];
    if (std.hash.Crc32.hash(body) != std.mem.readInt(u32, bytes[bytes.len - 4 ..][0..4], .little)) return error.InvalidRecord;
    var r: std.Io.Reader = .fixed(body);
    if ((r.takeByte() catch return error.InvalidRecord) != record_magic) return error.InvalidRecord;
    var out = DecodedRecord{
        .generation = r.takeInt(u64, .little) catch return error.InvalidRecord,
        .version = r.takeInt(u64, .little) catch return error.InvalidRecord,
        .ops = &.{},
    };
    const n = r.takeInt(u32, .little) catch return error.InvalidRecord;
    if (n > 1 << 20) return error.InvalidRecord;
    out.ops = try alloc.alloc(Op, n);
    errdefer out.deinit(alloc);
    for (out.ops) |*op| {
        const kind = std.enums.fromInt(OpKind, r.takeByte() catch return error.InvalidRecord) orelse return error.InvalidRecord;
        op.* = .{ .kind = kind };
        switch (kind) {
            .buffer_put => {
                op.key = try readBytes(&r);
                const len = r.takeInt(u32, .little) catch return error.InvalidRecord;
                if (len > 1 << 16) return error.InvalidRecord;
                const v = try alloc.alloc(f32, len);
                errdefer alloc.free(v);
                for (v) |*f| f.* = @bitCast(r.takeInt(u32, .little) catch return error.InvalidRecord);
                try out.vectors.append(alloc, v);
                op.vector = v;
            },
            .buffer_delete => op.key = try readBytes(&r),
            .graph_insert, .graph_delete => {
                op.key = try readBytes(&r);
                op.delta = try readBytes(&r);
            },
            .install_generation => {
                op.generation = r.takeInt(u64, .little) catch return error.InvalidRecord;
                op.digest = r.takeInt(u64, .little) catch return error.InvalidRecord;
                const inline_flag = r.takeByte() catch return error.InvalidRecord;
                if (inline_flag == 1) op.payload = try readBytes(&r) else if (inline_flag != 0) return error.InvalidRecord;
            },
        }
    }
    if (r.bufferedLen() != 0) return error.InvalidRecord;
    return out;
}

const DecodedGeneration = struct {
    index: ?leann.Index,
    keys: std.ArrayListUnmanaged([]u8),
    node_of: std.StringHashMapUnmanaged(u32),

    fn deinit(self: *DecodedGeneration, alloc: Allocator) void {
        var s = State{ .index = self.index, .keys = self.keys, .node_of = self.node_of };
        s.deinit(alloc);
    }
};

fn decodeGeneration(alloc: Allocator, bytes: []const u8, dims: u32) !DecodedGeneration {
    var out = DecodedGeneration{ .index = null, .keys = .empty, .node_of = .empty };
    errdefer out.deinit(alloc);
    var r: std.Io.Reader = .fixed(bytes);
    const head = r.take(generation_magic.len) catch return error.InvalidGeneration;
    if (!std.mem.eql(u8, head, generation_magic)) return error.InvalidGeneration;
    if ((r.takeInt(u32, .little) catch return error.InvalidGeneration) != generation_format) return error.InvalidGeneration;
    const n = r.takeInt(u32, .little) catch return error.InvalidGeneration;
    for (0..n) |_| {
        const k = try alloc.dupe(u8, try readBytes(&r));
        errdefer alloc.free(k);
        try out.keys.append(alloc, k);
    }
    out.index = try leann.Index.deserialize(alloc, try readBytes(&r));
    if (out.index.?.node_count != n or out.index.?.config.dims != dims) return error.InvalidGeneration;
    for (out.keys.items, 0..) |k, i| {
        const gop = try out.node_of.getOrPut(alloc, k);
        if (gop.found_existing) return error.InvalidGeneration;
        gop.value_ptr.* = @intCast(i);
    }
    if (r.bufferedLen() != 0) return error.InvalidGeneration;
    return out;
}

fn writeBytes(wr: *std.Io.Writer, b: []const u8) !void {
    try wr.writeInt(u32, @intCast(b.len), .little);
    try wr.writeAll(b);
}

fn readBytes(r: *std.Io.Reader) ![]const u8 {
    const len = r.takeInt(u32, .little) catch return error.InvalidRecord;
    return r.take(len) catch error.InvalidRecord;
}

fn normalize(v: []f32) void {
    var s: f32 = 0;
    for (v) |x| s += x * x;
    const n = @sqrt(s);
    if (n == 0) return;
    for (v) |*x| x.* /= n;
}

// -------------------------------------------------------------------------
// Tests: a simulated replicated log
// -------------------------------------------------------------------------

const testing = std.testing;

/// Stored embeddings by key, standing in for the primary store's artifacts.
const TestStore = struct {
    map: std.StringHashMapUnmanaged([]f32) = .empty,
    alloc: Allocator,

    fn deinit(self: *TestStore) void {
        var it = self.map.iterator();
        while (it.next()) |e| {
            self.alloc.free(e.key_ptr.*);
            self.alloc.free(e.value_ptr.*);
        }
        self.map.deinit(self.alloc);
    }

    fn put(self: *TestStore, key: []const u8, v: []const f32) !void {
        const gop = try self.map.getOrPut(self.alloc, key);
        if (gop.found_existing) {
            self.alloc.free(gop.value_ptr.*);
        } else gop.key_ptr.* = try self.alloc.dupe(u8, key);
        gop.value_ptr.* = try self.alloc.dupe(f32, v);
    }

    fn remove(self: *TestStore, key: []const u8) void {
        if (self.map.fetchRemove(key)) |kv| {
            self.alloc.free(kv.key);
            self.alloc.free(kv.value);
        }
    }

    fn loader(self: *TestStore) VectorLoader {
        return .{ .ctx = self, .load = load };
    }

    fn load(raw: *anyopaque, alloc: Allocator, key: []const u8) anyerror![]f32 {
        const self: *TestStore = @ptrCast(@alignCast(raw));
        const v = self.map.get(key) orelse return error.NotFound;
        return try alloc.dupe(f32, v);
    }
};

/// Out-of-band generation storage.
const TestBlobs = struct {
    map: std.AutoHashMapUnmanaged(u64, []u8) = .empty,
    alloc: Allocator,

    fn deinit(self: *TestBlobs) void {
        var it = self.map.valueIterator();
        while (it.next()) |v| self.alloc.free(v.*);
        self.map.deinit(self.alloc);
    }

    fn fetcher(self: *TestBlobs) GenerationFetcher {
        return .{ .ctx = self, .fetch = fetch };
    }

    fn fetch(raw: *anyopaque, alloc: Allocator, generation: u64, _: u64) anyerror![]u8 {
        const self: *TestBlobs = @ptrCast(@alignCast(raw));
        const b = self.map.get(generation) orelse return error.GenerationUnavailable;
        return try alloc.dupe(u8, b);
    }
};

fn testVector(rand: std.Random, out: []f32, cluster: usize) void {
    for (out, 0..) |*x, j| x.* = @as(f32, @floatFromInt((cluster * 7 + j) % 5)) + rand.floatNorm(f32) * 0.4;
}

test "leann replica: leader-planned records keep replicas identical across leader changes" {
    const alloc = testing.allocator;
    const dims = 16;
    const opts = Options{ .config = .{ .dims = dims, .metric = .cosine, .quantizer = .rabitq, .max_degree = 12 }, .build_threshold = 64, .inline_generation_limit = 1 << 20 };
    var replicas: [3]Replica = undefined;
    for (&replicas) |*r| r.* = try Replica.init(alloc, opts);
    defer for (&replicas) |*r| r.deinit();
    var store = TestStore{ .alloc = alloc };
    defer store.deinit();
    var blobs = TestBlobs{ .alloc = alloc };
    defer blobs.deinit();

    var prng = std.Random.DefaultPrng.init(61);
    const rand = prng.random();
    var log = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (log.items) |e| alloc.free(e);
        log.deinit(alloc);
    }
    var applied: [3]usize = .{ 0, 0, 0 };
    var stale_seen: usize = 0;
    var keybuf: [32]u8 = undefined;
    var v: [dims]f32 = undefined;

    var round: usize = 0;
    var next_key: usize = 0;
    while (round < 60) : (round += 1) {
        const leader = round / 20; // two leader changes
        // Work: 8 upserts (some overwrite) and occasional deletes.
        var work = std.ArrayListUnmanaged(Work).empty;
        defer {
            for (work.items) |w| {
                alloc.free(w.key);
                if (w.vector) |wv| alloc.free(wv);
            }
            work.deinit(alloc);
        }
        for (0..8) |_| {
            const id = if (next_key > 10 and rand.uintLessThan(u8, 5) == 0) rand.uintLessThan(usize, next_key) else blk: {
                next_key += 1;
                break :blk next_key - 1;
            };
            const key = try std.fmt.bufPrint(&keybuf, "doc:{d:0>5}", .{id});
            testVector(rand, &v, id % 6);
            try store.put(key, &v);
            const owned_key = try alloc.dupe(u8, key);
            errdefer alloc.free(owned_key);
            try work.append(alloc, .{ .key = owned_key, .vector = try alloc.dupe(f32, &v) });
        }
        if (round % 5 == 4 and next_key > 20) {
            const key = try std.fmt.bufPrint(&keybuf, "doc:{d:0>5}", .{rand.uintLessThan(usize, next_key)});
            store.remove(key);
            try work.append(alloc, .{ .key = try alloc.dupe(u8, key), .vector = null });
        }

        // The leader may lag: catch up on committed records first.
        while (applied[leader] < log.items.len) : (applied[leader] += 1) {
            if (try replicas[leader].apply(log.items[applied[leader]], blobs.fetcher()) == .stale) stale_seen += 1;
        }
        if (try replicas[leader].plan(alloc, work.items, store.loader())) |record| try log.append(alloc, record);

        // Every few rounds, a deposed leader's late record arrives after the
        // new leader's: it was planned against old state and must be skipped.
        if (round == 21) {
            const old = &replicas[0];
            if (try old.plan(alloc, work.items, store.loader())) |late| try log.append(alloc, late);
        }

        // Followers apply at different paces.
        for (&replicas, 0..) |*r, i| {
            if (i == leader) continue;
            const until = if (i == 2) log.items.len -| 1 else log.items.len;
            while (applied[i] < until) : (applied[i] += 1) {
                if (try r.apply(log.items[applied[i]], blobs.fetcher()) == .stale) stale_seen += 1;
            }
        }
        // Rebuild once, shipped out of band.
        if (round == 45) {
            const r = &replicas[leader];
            while (applied[leader] < log.items.len) : (applied[leader] += 1) {
                _ = try r.apply(log.items[applied[leader]], blobs.fetcher());
            }
            const planned = try r.planRebuild(alloc, store.loader());
            alloc.free(planned.record);
            // Force the out-of-band path regardless of size.
            const record = try encodeRecord(alloc, r.generation, r.version, &.{.{ .kind = .install_generation, .generation = planned.generation, .digest = planned.digest, .payload = null }});
            try blobs.map.put(alloc, planned.generation, planned.bytes);
            try log.append(alloc, record);
        }
    }
    for (&replicas, 0..) |*r, i| {
        while (applied[i] < log.items.len) : (applied[i] += 1) _ = try r.apply(log.items[applied[i]], blobs.fetcher());
    }

    try testing.expect(stale_seen > 0);
    try testing.expect(replicas[0].generation == 2); // first build + rebuild
    const s0 = try replicas[0].encodeState(alloc);
    defer alloc.free(s0);
    for (replicas[1..]) |*r| {
        const s = try r.encodeState(alloc);
        defer alloc.free(s);
        try testing.expectEqualSlices(u8, s0, s);
    }

    // State survives a local restart.
    var restored = try Replica.decodeState(alloc, opts, s0);
    defer restored.deinit();
    const s1 = try restored.encodeState(alloc);
    defer alloc.free(s1);
    try testing.expectEqualSlices(u8, s0, s1);

    // Every stored key is live and searchable; deleted keys are gone.
    var it = store.map.iterator();
    var checked: usize = 0;
    while (it.next()) |e| : (checked += 1) {
        if (checked >= 20) break;
        try testing.expect(replicas[1].contains(e.key_ptr.*));
        const hits = try replicas[1].search(alloc, e.value_ptr.*, 3, store.loader());
        defer alloc.free(hits);
        try testing.expectEqualStrings(e.key_ptr.*, hits[0].key);
    }
    try testing.expectEqual(store.map.count(), replicas[1].liveCount());
}

test "leann replica: missing generation leaves state unchanged until fetchable" {
    const alloc = testing.allocator;
    const dims = 8;
    const opts = Options{ .config = .{ .dims = dims, .max_degree = 8 }, .build_threshold = 32, .inline_generation_limit = 0 };
    var leader = try Replica.init(alloc, opts);
    defer leader.deinit();
    var follower = try Replica.init(alloc, opts);
    defer follower.deinit();
    var store = TestStore{ .alloc = alloc };
    defer store.deinit();
    var prng = std.Random.DefaultPrng.init(3);
    var keys: [40][16]u8 = undefined;
    var work: [40]Work = undefined;
    var v: [dims]f32 = undefined;
    for (&work, 0..) |*w, i| {
        const key = try std.fmt.bufPrint(&keys[i], "k{d}", .{i});
        testVector(prng.random(), &v, i % 3);
        try store.put(key, &v);
        w.* = .{ .key = key, .vector = store.map.get(key).? };
    }
    // Inline limit 0: the first build's generation is still inlined by plan
    // (it is part of the write record), so this exercises the rebuild path.
    const first = (try leader.plan(alloc, &work, store.loader())).?;
    defer alloc.free(first);
    try testing.expectEqual(Outcome.applied, try leader.apply(first, null));
    try testing.expectEqual(Outcome.applied, try follower.apply(first, null));

    const planned = try leader.planRebuild(alloc, store.loader());
    defer alloc.free(planned.record);
    defer alloc.free(planned.bytes);
    const before = try follower.encodeState(alloc);
    defer alloc.free(before);
    try testing.expectError(error.GenerationUnavailable, follower.apply(planned.record, null));
    const after = try follower.encodeState(alloc);
    defer alloc.free(after);
    try testing.expectEqualSlices(u8, before, after);

    var blobs = TestBlobs{ .alloc = alloc };
    defer blobs.deinit();
    try blobs.map.put(alloc, planned.generation, try alloc.dupe(u8, planned.bytes));
    try testing.expectEqual(Outcome.applied, try follower.apply(planned.record, blobs.fetcher()));
    try testing.expectEqual(@as(u64, 2), follower.generation);

    // A corrupted generation is rejected and rolled back.
    var bad_blobs = TestBlobs{ .alloc = alloc };
    defer bad_blobs.deinit();
    const corrupt = try alloc.dupe(u8, planned.bytes);
    corrupt[corrupt.len / 2] ^= 0xff;
    try bad_blobs.map.put(alloc, planned.generation, corrupt);
    var other = try Replica.decodeState(alloc, opts, before);
    defer other.deinit();
    try testing.expectEqual(Outcome.rejected, try other.apply(planned.record, bad_blobs.fetcher()));
    const unchanged = try other.encodeState(alloc);
    defer alloc.free(unchanged);
    try testing.expectEqualSlices(u8, before, unchanged);
}
