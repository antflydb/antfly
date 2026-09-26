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

//! Session contract for one tree-packed Laya row (models/laya/LAYA.md).
//!
//! Inputs, all i64: `input_ids`, `position_ids`, `token_segment`,
//! `token_qtype` as `[1, L]`; `segment_parent` as `[1, S]`; `marker_pos` as
//! `[Q, W]`; `anchor_pos` as `[Q, 1]`. Outputs: `logits` `[Q, W]` and
//! `action_logits` `[Q, n_act]`.
const std = @import("std");
const ops = @import("../ops/ops.zig");
const modern = @import("modern_bert.zig");
const head = @import("laya_head.zig");
const tree = @import("../pipelines/laya_tree.zig");
const trunk_cache = @import("laya_trunk_cache.zig");
const build_options = @import("build_options");
const metal_compute = if (build_options.enable_metal) @import("../ops/metal_compute.zig") else struct {};
const MetalTensor = if (build_options.enable_metal) @import("../backends/metal_tensor.zig").MetalTensor else void;
const Tensor = @import("../backends/tensor.zig").Tensor;
const TensorInfo = @import("../backends/tensor.zig").TensorInfo;

pub const input_count = 7;
pub const inputs_info = [_]TensorInfo{
    .{ .name = "input_ids", .dtype = .i64, .shape = &.{ 1, -1 } },
    .{ .name = "position_ids", .dtype = .i64, .shape = &.{ 1, -1 } },
    .{ .name = "token_segment", .dtype = .i64, .shape = &.{ 1, -1 } },
    .{ .name = "segment_parent", .dtype = .i64, .shape = &.{ 1, -1 } },
    .{ .name = "token_qtype", .dtype = .i64, .shape = &.{ 1, -1 } },
    .{ .name = "marker_pos", .dtype = .i64, .shape = &.{ -1, -1 } },
    .{ .name = "anchor_pos", .dtype = .i64, .shape = &.{ -1, 1 } },
};

/// Copy an i64 matrix; request tensors carry no alignment guarantee.
fn matrix(a: std.mem.Allocator, tensor: Tensor, rows: ?usize, columns: ?usize) ![]const i64 {
    if (tensor.dtype != .i64 or tensor.shape.len != 2 or tensor.shape[0] <= 0 or tensor.shape[1] <= 0) return error.InvalidLayaInputs;
    const r: usize = @intCast(tensor.shape[0]);
    const c: usize = @intCast(tensor.shape[1]);
    if ((rows != null and rows.? != r) or (columns != null and columns.? != c)) return error.InvalidLayaInputs;
    const count = std.math.mul(usize, r, c) catch return error.InvalidLayaInputs;
    if (tensor.data.len != count * @sizeOf(i64)) return error.InvalidLayaInputs;
    const values = try a.alloc(i64, count);
    @memcpy(std.mem.sliceAsBytes(values), tensor.data);
    return values;
}

/// A validated row owned by `a`; free it with `Row.deinit`.
pub fn view(a: std.mem.Allocator, cfg: modern.Config, inputs: []const Tensor) !tree.Row {
    const laya = cfg.laya orelse return error.InvalidLayaConfig;
    if (!laya.packing.enabled() or inputs.len != input_count) return error.InvalidLayaInputs;
    const markers_tensor = inputs[5];
    if (markers_tensor.shape.len != 2 or markers_tensor.shape[0] <= 0 or markers_tensor.shape[1] <= 0) return error.InvalidLayaInputs;
    const questions: usize = @intCast(markers_tensor.shape[0]);
    const width: usize = @intCast(markers_tensor.shape[1]);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const s = arena.allocator();
    const ids = try matrix(s, inputs[0], 1, null);
    const index = try s.alloc(usize, questions);
    for (index, 0..) |*value, i| value.* = i;
    const row = tree.Row{
        .ids = ids,
        .positions = try matrix(s, inputs[1], 1, ids.len),
        .segments = try matrix(s, inputs[2], 1, ids.len),
        .parents = try matrix(s, inputs[3], 1, null),
        .kinds = try matrix(s, inputs[4], 1, ids.len),
        .markers = try matrix(s, markers_tensor, questions, width),
        .anchors = try matrix(s, inputs[6], questions, 1),
        .question_index = index,
        .width = width,
    };
    try tree.validate(row, laya.max_len, laya.packing.max_packed_len, laya.maxOptions());
    for (ids) |id| if (id < 0 or id >= cfg.vocab_size) return error.InvalidLayaInputs;
    return tree.own(a, row);
}

pub fn run(cb: *const ops.ComputeBackend, a: std.mem.Allocator, cfg: modern.Config, inputs: []const Tensor, cache: ?*trunk_cache.Cache) ![]Tensor {
    const laya = cfg.laya orelse return error.InvalidLayaConfig;
    const row = try view(a, cfg, inputs);
    defer row.deinit(a);
    return forwardRow(cb, a, cfg, laya, row, cache);
}

const Laya = @import("../models/laya.zig").Config;

/// Encoder and decision head for one validated row. With a cache on the CPU
/// backend, the trunk's per-layer keys and values are reused across rows and
/// requests, and only the branch tokens are encoded (laya_trunk_cache.zig).
pub fn forwardRow(cb: *const ops.ComputeBackend, a: std.mem.Allocator, cfg: modern.Config, laya: Laya, row: tree.Row, cache: ?*trunk_cache.Cache) ![]Tensor {
    const store = cache orelse return forwardFull(cb, a, cfg, laya, row);
    // A multi-row batch (pipelines/laya.zig, laya_tree.coalesce) packs
    // several states' trees into one row. Segment attention already keeps
    // their cost proportional to visible keys, so batching pays off without
    // the trunk cache; caching a forest of trunks is not implemented yet.
    if (tree.treeCount(row) != 1) return forwardFull(cb, a, cfg, laya, row);
    const trunk = trunkRows(row) orelse return forwardFull(cb, a, cfg, laya, row);
    if (store.limit_bytes == 0 or trunk < store.min_tokens or trunk == row.ids.len) return forwardFull(cb, a, cfg, laya, row);
    const layers = cfg.num_hidden_layers + laya.head_layers;
    const key = trunk_cache.Cache.key(row.ids[0..trunk], layers, cfg.hidden_size);
    const entry = store.acquire(key) orelse blk: {
        const created = try store.create(key, trunk, layers, cfg.hidden_size, !deviceCache(cb));
        errdefer store.release(created);
        try fillTrunk(cb, a, cfg, laya, row.ids[0..trunk], created, store.allocator);
        store.publish(created);
        break :blk created;
    };
    defer store.release(entry);
    return forwardCached(cb, a, cfg, laya, row, entry);
}

/// Trunk length when the trunk is exactly rows `0..T` at positions `0..T-1`.
fn trunkRows(row: tree.Row) ?usize {
    var trunk: usize = 0;
    while (trunk < row.ids.len and row.segments[trunk] == 0) : (trunk += 1) {
        if (row.positions[trunk] != trunk) return null;
    }
    for (row.segments[trunk..]) |segment| if (segment == 0) return null;
    return if (trunk == 0) null else trunk;
}

/// Trunk tensors stay on Metal devices across requests; elsewhere they are
/// host memory and re-uploaded per request (cheap on CPU).
fn deviceCache(cb: *const ops.ComputeBackend) bool {
    return build_options.enable_metal and cb.kind() == .metal;
}

const DeviceTrunk = if (build_options.enable_metal) struct {
    /// `[layer][keys, values]`; half tensors when `half`.
    tensors: []MetalTensor,
    half: bool,

    fn destroy(raw: ?*anyopaque, allocator: std.mem.Allocator) void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        for (self.tensors) |*tensor| tensor.deinit();
        allocator.free(self.tensors);
        allocator.destroy(self);
    }
} else struct {};

/// Encode the trunk alone (it never attends to a branch) and keep every
/// encoder and head layer's keys and values in `entry`, at its precision.
fn fillTrunk(cb: *const ops.ComputeBackend, a: std.mem.Allocator, cfg: modern.Config, laya: Laya, ids: []const i64, entry: *trunk_cache.Entry, cache_allocator: std.mem.Allocator) !void {
    const n = cfg.num_hidden_layers;
    if (comptime build_options.enable_metal) if (deviceCache(cb)) {
        const tensors = try a.alloc(?ops.CT, 2 * entry.layers);
        defer a.free(tensors);
        @memset(tensors, null);
        defer for (tensors) |tensor| if (tensor) |t| cb.free(t);
        const keys = tensors[0..entry.layers];
        const values = tensors[entry.layers..];
        const encoded = try modern.forwardCapturingCT(cb, a, cfg, ids, .{ .key_tensors = keys[0..n], .value_tensors = values[0..n] });
        defer cb.free(encoded);
        try head.captureTrunk(cb, a, laya, encoded, ids.len, cfg.hidden_size, .{ .key_tensors = keys[n..], .value_tensors = values[n..] });
        const compute: *metal_compute.MetalCompute = @ptrCast(@alignCast(cb.ptr));
        const half = entry.precision == .f16;
        const trunk = try cache_allocator.create(DeviceTrunk);
        errdefer cache_allocator.destroy(trunk);
        trunk.* = .{ .tensors = try cache_allocator.alloc(MetalTensor, 2 * entry.layers), .half = half };
        var retained: usize = 0;
        errdefer {
            for (trunk.tensors[0..retained]) |*tensor| tensor.deinit();
            cache_allocator.free(trunk.tensors);
        }
        for (0..entry.layers) |layer| for ([_]?ops.CT{ keys[layer], values[layer] }) |tensor| {
            trunk.tensors[retained] = (if (half)
                try compute.deviceHalfCopy(tensor.?)
            else
                try metal_compute.MetalCompute.retainDenseDeviceTensor(tensor.?)) orelse return error.UnsupportedLayaDeviceCache;
            retained += 1;
        };
        entry.device = trunk;
        entry.device_deinit = DeviceTrunk.destroy;
        return;
    };
    const width = ids.len * cfg.hidden_size;
    const scratch = try a.alloc(f32, 2 * entry.layers * width);
    defer a.free(scratch);
    const keys = try a.alloc([]f32, entry.layers);
    defer a.free(keys);
    const values = try a.alloc([]f32, entry.layers);
    defer a.free(values);
    for (keys, values, 0..) |*k, *v, layer| {
        k.* = scratch[(2 * layer) * width ..][0..width];
        v.* = scratch[(2 * layer + 1) * width ..][0..width];
    }
    const encoded = try modern.forwardCapturingCT(cb, a, cfg, ids, .{ .keys = keys[0..n], .values = values[0..n] });
    defer cb.free(encoded);
    try head.captureTrunk(cb, a, laya, encoded, ids.len, cfg.hidden_size, .{ .keys = keys[n..], .values = values[n..] });
    for (0..2 * entry.layers) |slot| entry.store(slot, scratch[slot * width ..][0..width]);
}

/// A request-local f32 tensor for one cached trunk layer (`which` 0 = keys).
fn trunkTensor(cb: *const ops.ComputeBackend, a: std.mem.Allocator, entry: *const trunk_cache.Entry, layer: usize, which: usize) !ops.CT {
    if (comptime build_options.enable_metal) if (entry.device) |raw| {
        const trunk: *const DeviceTrunk = @ptrCast(@alignCast(raw));
        const compute: *metal_compute.MetalCompute = @ptrCast(@alignCast(cb.ptr));
        const tensor = &trunk.tensors[2 * layer + which];
        return if (trunk.half) compute.ctFromHalfDeviceTensor(tensor) else compute.ctFromRetainedDeviceTensor(tensor);
    };
    const values = try a.alloc(f32, entry.tokens * entry.hidden);
    defer a.free(values);
    entry.load(2 * layer + which, values);
    return cb.fromFloat32Shape(values, &.{ @intCast(entry.tokens), @intCast(entry.hidden) });
}

fn forwardCached(cb: *const ops.ComputeBackend, a: std.mem.Allocator, cfg: modern.Config, laya: Laya, row: tree.Row, entry: *const trunk_cache.Entry) ![]Tensor {
    const trunk = entry.tokens;
    const hidden = cfg.hidden_size;
    const segments = try tree.ranges(a, row, trunk);
    defer a.free(segments);
    const key_positions = try tree.positions32(a, row.positions);
    defer a.free(key_positions);
    const keys = try a.alloc(ops.CT, entry.layers);
    defer a.free(keys);
    const values = try a.alloc(ops.CT, entry.layers);
    defer a.free(values);
    var uploaded: usize = 0;
    defer for (keys[0..uploaded], values[0..uploaded]) |k, v| {
        cb.free(k);
        cb.free(v);
    };
    for (keys, values, 0..) |*k, *v, layer| {
        k.* = try trunkTensor(cb, a, entry, layer, 0);
        v.* = trunkTensor(cb, a, entry, layer, 1) catch |err| {
            cb.free(k.*);
            return err;
        };
        uploaded += 1;
    }
    const layers = cfg.num_hidden_layers;
    const branch: modern.Packed = .{ .positions = row.positions[trunk..], .ranges = segments, .key_positions = key_positions };
    const encoded = try modern.forwardBranchesCT(cb, a, cfg, row.ids[trunk..], branch, .{ .prefix_rows = trunk, .keys = keys[0..layers], .values = values[0..layers] });
    defer cb.free(encoded);
    return head.forwardPackedBranches(cb, a, laya, encoded, branch, row.kinds[trunk..], row.markers, row.anchors, row.width, hidden, .{ .rows = trunk, .keys = keys[layers..], .values = values[layers..] });
}

/// Encoder and decision head over every token of one validated row.
pub fn forwardFull(cb: *const ops.ComputeBackend, a: std.mem.Allocator, cfg: modern.Config, laya: Laya, row: tree.Row) ![]Tensor {
    const segments = try tree.ranges(a, row, 0);
    defer a.free(segments);
    const key_positions = try tree.positions32(a, row.positions);
    defer a.free(key_positions);
    const packed_row: modern.Packed = .{ .positions = row.positions, .ranges = segments, .key_positions = key_positions };
    const encoded = try modern.forwardPackedCT(cb, a, cfg, row.ids, packed_row);
    defer cb.free(encoded);
    return head.forwardPacked(cb, a, laya, encoded, packed_row, row.kinds, row.markers, row.anchors, row.width, cfg.hidden_size);
}

/// Peak transient bytes for one row: FFN and projection activations plus
/// one head's segment-attention staging. Packed attention keeps no
/// `[L, L]` state. Packed rows are always batch 1.
pub fn workspaceBytes(cfg: modern.Config, sequence: usize) !usize {
    const mul = std.math.mul;
    const add = std.math.add;
    const activations = try mul(usize, sequence, try add(usize, try mul(usize, cfg.hidden_size, 8), try mul(usize, cfg.intermediate_size, 4)));
    const staging = try add(usize, try mul(usize, try mul(usize, sequence, 4), cfg.hidden_size / @max(cfg.num_attention_heads, 1)), 64 * 256);
    return mul(usize, try add(usize, activations, staging), @sizeOf(f32));
}
