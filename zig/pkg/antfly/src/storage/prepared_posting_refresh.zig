// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Owned input/output for optimistic, topology-preserving posting refresh.
//! Capture requires index mutation ownership. build() accesses no live index.
const std = @import("std");
const vi = @import("antfly_vectorindex");
const clock = @import("antfly_platform").time;
const vector = @import("antfly_vector").vector;
const quantizer_mod = @import("antfly_vector").quantizer;
const resources = @import("resource_manager.zig");
const Node = vi.types.Node;

pub const Prepared = struct {
    backing_alloc: std.mem.Allocator,
    arena: std.heap.ArenaAllocator,
    alloc: std.mem.Allocator,
    config: vi.types.HBCConfig,
    quantizer: quantizer_mod.RaBitQuantizer,
    rot: vector.RandomOrthogonalTransformer,
    input: ?Input = null,
    input_ns: u64 = 0,
    metadata_ns: u64 = 0,
    read_ns: u64 = 0,
    binding_hits: usize = 0,
    input_rows: usize = 0,
    admission_ns: u64 = 0,
    published: bool = false,
    detached: bool = false,
    input_loaded: bool = false,

    identity: u64,
    epoch: u64,
    leaf_id: u64,
    index_name: []const u8 = "",
    nodes: std.AutoHashMapUnmanaged(u64, Node) = .empty,
    changed: std.ArrayListUnmanaged(u64) = .empty,
    outputs: std.ArrayListUnmanaged(Output) = .empty,
    reservations: std.ArrayListUnmanaged(resources.Reservation) = .empty,
    manager: ?*resources.ResourceManager,
    vectors: []f32 = &.{},
    write_profile: vi.hbc_runtime.WriteProfile = .{},
    built: bool = false,
    capture_ns: u64 = 0,
    build_ns: u64 = 0,

    pub const Input = struct {
        ctx: *anyopaque,
        load: *const fn (*anyopaque, *Prepared) anyerror!void,
        release: *const fn (*anyopaque) void,
    };
    pub const Output = struct {
        id: u64,
        packed_bytes: []const u8,
        state: ?vi.types.PostingState,
        quantized: ?[]const u8,
        replace_quantized: bool,
    };

    fn reserve(self: *Prepared, bytes: usize) !void {
        const manager = self.manager orelse return;
        var reservation = try manager.reserve(.dense_repair_working_set, @intCast(bytes));
        errdefer reservation.release();
        try self.reservations.append(self.backing_alloc, reservation);
    }

    pub fn deinit(self: *Prepared) void {
        const backing = self.backing_alloc;
        if (self.input) |input| input.release(input.ctx);
        self.arena.deinit();
        for (self.reservations.items) |*reservation| reservation.release();
        self.reservations.deinit(backing);
        backing.destroy(self);
    }

    /// Copies vectors while the index's mutation owner guarantees coherent
    /// membership/source bindings. No borrowed source/cache pointer escapes.
    pub fn capture(index: anytype, node_id: u64, identity: u64) !?*Prepared {
        return captureWithOptions(index, node_id, identity, false);
    }

    pub fn captureWithOptions(index: anytype, node_id: u64, identity: u64, detached: bool) !?*Prepared {
        var txn = try index.beginReadTxn();
        defer txn.abort();
        var leaf = try index.loadNode(&txn, node_id);
        defer leaf.deinit(index.alloc);
        if (!leaf.is_leaf or !leaf.posting_state.dirty) return null;
        const self = try index.alloc.create(Prepared);
        self.* = .{
            .backing_alloc = index.alloc,
            .arena = .init(index.alloc),
            .alloc = undefined,
            .config = index.config,
            .quantizer = index.quantizer,
            .rot = index.rot,
            .detached = detached,
            .identity = identity,
            .epoch = index.published_mutation_epoch.load(.acquire),
            .leaf_id = node_id,
            .manager = index.resource_manager,
        };
        errdefer self.deinit();
        self.alloc = self.arena.allocator();
        try self.reserve(65536);
        self.quantizer.alloc = self.alloc;
        self.rot.alloc = self.alloc;
        try self.reserve(try std.math.mul(usize, index.rot.rotations.len, @sizeOf(@TypeOf(index.rot.rotations[0]))));
        self.rot.rotations = try self.alloc.dupe(@TypeOf(index.rot.rotations[0]), index.rot.rotations);
        try self.captureNode(&leaf, true);
        self.quantizer.unbias = try self.alloc.dupe(f32, index.quantizer.unbias);
        if (leaf.posting_state.centroid_dirty) {
            var parent_id = leaf.parent;
            while (parent_id != 0) {
                // Corrupt parent cycles must not grow unbounded scratch.
                if (std.mem.indexOfScalar(u64, self.changed.items, parent_id) != null) return error.Corrupted;
                var parent = try index.loadNode(&txn, parent_id);
                defer parent.deinit(index.alloc);
                if (parent.is_leaf) return error.Corrupted;
                try self.captureNode(&parent, true);
                for (parent.children) |id| {
                    if (self.nodes.contains(id)) continue;
                    var child = try index.loadNode(&txn, id);
                    defer child.deinit(index.alloc);
                    try self.captureNode(&child, false);
                }
                parent_id = parent.parent;
            }
        }
        const matrix_len = try std.math.mul(usize, leaf.members.len, index.config.dims);
        // Matrix, quantizer scratch, output, and transaction lookup workspace
        // were admitted by captureNode before these allocations.
        if (leaf.posting_state.centroid_dirty or (leaf.posting_state.payload_dirty and index.config.use_quantization)) {
            self.vectors = try self.alloc.alloc(f32, matrix_len);
            if (matrix_len != 0 and !detached) try index.loadPostingVectorsTransformed(&txn, leaf.members, self.vectors);
        }
        return self;
    }

    fn captureNode(self: *Prepared, node: *const Node, changed: bool) !void {
        const count = if (node.is_leaf) node.members.len else node.children.len;
        const floats = try std.math.mul(usize, if (changed) @max(count, 1) else 1, self.config.dims);
        const budget = try std.math.add(usize, try std.math.mul(usize, floats, 64), try std.math.add(usize, 4096, try std.math.mul(usize, count, if (changed) 256 else 0)));
        try self.reserve(budget);
        var owned = node.*;
        owned.backing = &.{};
        owned.centroid = try self.alloc.dupe(f32, node.centroid);
        owned.members = if (changed) try self.alloc.dupe(u64, node.members) else &.{};
        owned.children = if (changed) try self.alloc.dupe(u64, node.children) else &.{};
        try self.nodes.put(self.alloc, node.id, owned);
        if (changed) try self.changed.append(self.alloc, node.id);
    }

    // Read facade for the same internal-centroid/radius implementation used
    // by synchronous repair; all nodes here belong to the preparation arena.
    pub fn getCachedNodeClone(self: *Prepared, id: u64) !?Node {
        return try (self.nodes.get(id) orelse return error.NotFound).clone(self.alloc);
    }
    pub fn loadNodeFromStorage(_: *Prepared, _: anytype, _: u64) !Node {
        return error.NotFound;
    }
    pub fn cacheNode(_: *Prepared, _: *const Node) !void {}

    pub fn build(self: *Prepared) !void {
        if (self.built) return error.InvalidArgument;
        if (self.input) |input| {
            const started = clock.monotonicNs();
            try input.load(input.ctx, self);
            self.input_ns = clock.monotonicNs() -| started;
            input.release(input.ctx);
            self.input = null;
            self.input_loaded = true;
        } else if (self.detached and !self.input_loaded and self.vectors.len != 0) return error.PreparedInputUnavailable;
        for (self.changed.items, 0..) |id, position| {
            const node = self.nodes.getPtr(id) orelse return error.NotFound;
            const refresh_payload = if (position == 0) node.posting_state.payload_dirty else true;
            if (position == 0) {
                if (node.posting_state.centroid_dirty)
                    try vi.posting.PostingStore.recomputeCentroidFromTransformedVectors(self, node, self.vectors);
                if (refresh_payload) vi.posting.PostingStore.notePayloadRefreshed(node);
                node.posting_state.dirty = node.posting_state.centroid_dirty or node.posting_state.payload_dirty;
            } else {
                try vi.hbc_index.recomputeInternalCentroid(self, {}, node);
            }
            const count = if (node.is_leaf) node.members.len else node.children.len;
            var encoded_quantized: ?[]const u8 = null;
            if (refresh_payload and self.config.use_quantization and count != 0) {
                const matrix = if (position == 0) self.vectors else blk: {
                    const data = try self.alloc.alloc(f32, try std.math.mul(usize, count, self.config.dims));
                    for (node.children, 0..) |child_id, row| {
                        const child = self.nodes.get(child_id) orelse return error.NotFound;
                        @memcpy(data[row * self.config.dims ..][0..self.config.dims], child.centroid);
                    }
                    break :blk data;
                };
                var qs: vi.hbc_runtime.QuantizedSet = if (node.parent == 0)
                    .{ .nonquant = .{ .vectors = .{ .dims = @intCast(self.config.dims), .count = @intCast(count), .data = try self.alloc.dupe(f32, matrix) } } }
                else
                    .{ .rabit = try self.quantizer.quantize(node.centroid, matrix, count) };
                defer qs.deinit(self.alloc);
                encoded_quantized = switch (qs) {
                    .rabit => |*set| try set.encode(self.alloc),
                    .nonquant => |*set| try set.encode(self.alloc),
                };
            }
            const centers = std.mem.sliceAsBytes(node.centroid);
            const ids = std.mem.sliceAsBytes(if (node.is_leaf) node.members else node.children);
            const bytes = try self.alloc.alloc(u8, vi.hbc.packedNodeValueSize(centers.len, ids.len));
            const packed_bytes = try vi.hbc.encodePackedNodeValue(bytes, .{ .is_leaf = node.is_leaf, .level = node.level, .parent = node.parent }, node.covering_radius, centers, ids);
            try self.outputs.append(self.alloc, .{
                .id = id,
                .packed_bytes = packed_bytes,
                .state = if (node.is_leaf) node.posting_state else null,
                .quantized = encoded_quantized,
                .replace_quantized = refresh_payload and self.config.use_quantization,
            });
        }
        self.built = true;
    }
};
