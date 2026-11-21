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

//! Device-resident Memorilla span head (docs/plans/memorilla-copy17-native.md
//! 8.21): the `span-head.safetensors` of a hybrid span-window bundle, verified
//! against `manifest.json` on load (`memorilla_span.Head.loadBundle`), uploaded
//! once as F32 resident weights of its own CUDA compute and run per request
//! on the device. `Owner.forward` computes what the host
//! `memorilla_span.forward` computes (projection + LayerNorm, segment and
//! position rows, the no-answer row, two pre-norm encoder layers with exact
//! GELU, final LayerNorm, start/end logits) with cuBLASLt F32 GEMMs, the
//! backend's LayerNorm/GELU/add and full self-attention on the tiled F32
//! kernel; only the `[T + 1, 2]` logits come back to the host. Results agree
//! with the host head to F32 rounding, not bit for bit.
//!
//! Cached per decoder by `memorilla_reader_cache.SpanHeadCache` (stat-fenced
//! identity of the bundle directory, `manifest.json`, `protocol.json` and the
//! weights; full sha256 verification on every miss).
const std = @import("std");
const build_options = @import("build_options");
const span = @import("../pipelines/memorilla_span.zig");
const bundle = @import("memorilla.zig");
const ops = @import("../ops/ops.zig");
const tensor_mod = @import("../backends/tensor.zig");
const c_file = @import("../util/c_file.zig");
const memory = @import("../runtime/root.zig").tier.memory;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const cuda = if (build_options.enable_cuda) @import("../ops/cuda/cuda_compute.zig") else struct {};
const A = std.mem.Allocator;
const CB = ops.ComputeBackend;
const CT = ops.CT;
const this_module = @This();

/// Upper bound of one head's F32 weights (span1: 130.3 MB).
pub const max_weight_bytes: usize = 256 * 1024 * 1024;
/// Upper bound of one request's device activations (`scratchBytes`).
pub const max_scratch_bytes: usize = 512 * 1024 * 1024;
/// The tiled attention kernel's head width.
pub const head_dim: usize = 128;

pub fn weightPlan() memory.AdmissionAmounts {
    return .{
        // The parsed host head while uploading plus one tensor copy in flight;
        // only the small row tables stay after the load.
        .host_weight_bytes = max_weight_bytes + 64 * 1024 * 1024,
        .backend_weight_bytes = max_weight_bytes,
        .backend_scratch_bytes = max_scratch_bytes,
    };
}

pub fn requireLimits(limits: memory.Limits) !void {
    const plan = weightPlan();
    if (limits.host_limit_bytes == 0 or limits.backend_limit_bytes == 0 or limits.combined_limit_bytes == 0)
        return error.MemorillaFiniteBudgetRequired;
    if (plan.hostTotalBytes() > limits.host_limit_bytes or plan.backendTotalBytes() > limits.backend_limit_bytes or
        (try std.math.add(usize, plan.hostTotalBytes(), plan.backendTotalBytes())) > limits.combined_limit_bytes) return error.ResourceLimitExceeded;
}

pub fn requireAdmission(admission: *const memory.AdmissionLease, limits: memory.Limits) !void {
    try requireLimits(limits);
    const plan = weightPlan();
    if (admission.controller == null or admission.retain_backend_class != .gpu or
        admission.amounts.host_weight_bytes < plan.host_weight_bytes or
        admission.amounts.backend_weight_bytes < plan.backend_weight_bytes or
        admission.amounts.backend_scratch_bytes < plan.backend_scratch_bytes)
        return error.MemorillaReaderAdmissionRequired;
}

/// Head geometry (a protocol with another geometry is a different head).
pub const Geometry = struct {
    width: usize,
    hidden: usize,
    layers: usize,
    heads: usize,
    max_positions: usize,
    max_documents: usize,

    pub fn of(protocol: span.Protocol) Geometry {
        return .{ .width = protocol.width, .hidden = protocol.hidden, .layers = protocol.layers, .heads = protocol.heads, .max_positions = protocol.max_positions, .max_documents = protocol.max_documents };
    }

    /// F32 bytes of the device-resident tensors (everything but the segment,
    /// position and no-answer rows, which stay on the host).
    pub fn deviceBytes(self: Geometry) usize {
        const h = self.hidden;
        const per_layer = 4 * h * h + 4 * h + 8 * h * h + 5 * h + 4 * h;
        return @sizeOf(f32) * (h * self.width + 3 * h + self.layers * per_layer + 2 * h + 2 * h + 2);
    }
};

/// Device activations of one forward over `rows` document tokens: the
/// features, the residual stream and its temporaries, q/k/v, the attention
/// output and the 4x MLP hidden state (twice, before and after GELU).
pub fn scratchBytes(geometry: Geometry, rows: usize) !usize {
    const n = try std.math.add(usize, rows, 1);
    const h = geometry.hidden;
    const features = try std.math.mul(usize, rows, geometry.width);
    const per_row = 12 * h + 8 * h;
    return @sizeOf(f32) * (try std.math.add(usize, features, try std.math.mul(usize, n, per_row)));
}

/// The span fields of the bundle's pinned `protocol.json` (sha256 and size
/// checked against `manifest.json`).
fn readProtocol(a: A, directory: []const u8) !span.Protocol {
    const manifest_path = try std.fs.path.join(a, &.{ directory, "manifest.json" });
    defer a.free(manifest_path);
    const manifest_bytes = try c_file.readFileMax(a, manifest_path, 1024 * 1024);
    defer a.free(manifest_bytes);
    var manifest = std.json.parseFromSlice(std.json.Value, a, manifest_bytes, .{}) catch return error.InvalidMemorillaManifest;
    defer manifest.deinit();
    const protocol_path = try std.fs.path.join(a, &.{ directory, "protocol.json" });
    defer a.free(protocol_path);
    const protocol_bytes = try c_file.readFileMax(a, protocol_path, 256 * 1024);
    defer a.free(protocol_bytes);
    try bundle.verifyFile(manifest.value, "protocol.json", protocol_bytes);
    return (try span.parseProtocol(a, protocol_bytes)) orelse error.UnsupportedMemorillaProfile;
}

pub const Owner = struct {
    pub const runtime = this_module;
    /// Every pinned file the head depends on; a drift in any retires it.
    pub const identity_files = [_][]const u8{ "manifest.json", "protocol.json", span.weights_file_name };

    allocator: A,
    compute: if (build_options.enable_cuda) *cuda.CudaCompute else void,
    geometry: Geometry,
    /// sha256 of `span-head.safetensors`.
    sha256: [64]u8,
    /// Host rows for the input addend: the no-answer row `[h]`, the position
    /// table `[max_positions + 1, h]` and the segment table `[max_documents, h]`.
    null_row: []f32,
    position: []f32,
    segment: []f32,

    /// Caller owns the GPU admission lease and must release it AFTER deinit.
    pub fn open(a: A, io: std.Io, directory: []const u8, admission: *const memory.AdmissionLease, limits: memory.Limits, control: Control) !Owner {
        _ = io;
        try control.check();
        try requireAdmission(admission, limits);
        if (comptime !build_options.enable_cuda) return error.CudaNotEnabled;
        try @import("memorilla_qualification.zig").requireCudaEnvironment();
        const backing = @import("antfly_platform").allocator.reclaimingScratch(a);
        var guard = try control.enterUninterruptible(.uninterruptible);
        defer guard.deinit();
        const protocol = try readProtocol(backing, directory);
        const geometry = Geometry.of(protocol);
        if (geometry.heads == 0 or geometry.hidden != geometry.heads * head_dim or geometry.max_documents == 0 or
            span.Head.weightBytes(protocol) > max_weight_bytes) return error.UnsupportedMemorillaProfile;
        var head = try span.Head.loadBundle(backing, directory, protocol);
        defer head.deinit();
        try control.check();
        const null_row = try backing.dupe(f32, head.null_row);
        errdefer backing.free(null_row);
        const position = try backing.dupe(f32, head.position);
        errdefer backing.free(position);
        const segment = try backing.dupe(f32, head.segment);
        errdefer backing.free(segment);
        const compute = try backing.create(cuda.CudaCompute);
        errdefer backing.destroy(compute);
        compute.* = try cuda.CudaCompute.init(backing);
        errdefer compute.deinit();
        if (compute.ctx.info.compute_major != 12 or compute.ctx.info.compute_minor != 1) return error.UnsupportedMemorillaDevice;
        compute.strict_f32_weights = true;
        compute.capture_gates = .{};
        try upload(backing, compute, &head, control);
        _ = compute.releaseMemorillaRequestResources() catch @panic("Memorilla span head load cleanup failed; worker restart required");
        if (compute.snapshotStats().resident_weight_bytes != geometry.deviceBytes()) return error.UnsupportedMemorillaPrecision;
        try control.check();
        return .{ .allocator = backing, .compute = compute, .geometry = geometry, .sha256 = head.sha256, .null_row = null_row, .position = position, .segment = segment };
    }

    pub fn deinit(self: *Owner) void {
        if (comptime build_options.enable_cuda) {
            self.compute.deinit();
            self.allocator.destroy(self.compute);
        }
        self.allocator.free(self.null_row);
        self.allocator.free(self.position);
        self.allocator.free(self.segment);
        self.* = undefined;
    }

    /// Start/end logits `[T + 1]` (index 0 = no answer) of `features
    /// [rows, width]` whose documents hold `counts` tokens each. Every request
    /// tensor is freed and the compute's request resources are released before
    /// this returns; on an error the caller retires the entry.
    pub fn forward(self: *Owner, a: A, protocol: span.Protocol, features: []const f32, counts: []const usize) !span.Logits {
        if (comptime !build_options.enable_cuda) return error.CudaNotEnabled;
        if (!std.meta.eql(self.geometry, Geometry.of(protocol))) return error.ModelArtifactsChanging;
        var logits = forwardDevice(self, a, features, counts) catch |err| {
            _ = self.compute.releaseMemorillaRequestResources() catch {};
            return err;
        };
        errdefer logits.deinit();
        _ = try self.compute.releaseMemorillaRequestResources();
        return logits;
    }
};

fn insert(a: A, compute: anytype, key: []const u8, data: []const f32, shape: []const i64) !void {
    var tensor = try tensor_mod.Tensor.initFloat32(a, key, shape, data);
    defer tensor.deinit();
    const owned = try compute.allocator.dupe(u8, key);
    errdefer compute.allocator.free(owned);
    try compute.insertWeightFromTensor(owned, &tensor);
}

/// Resident names: `project`, `project_norm`, `blocks.{i}.{n1,q,k,v,out,n2,l1,l2}`,
/// `norm`, `out` (`.weight` / `.bias`, PyTorch `[out, in]` layout); the fused
/// `in_proj` is split into its q, k and v rows.
fn upload(a: A, compute: anytype, head: *const span.Head, control: Control) !void {
    const h: i64 = @intCast(head.hidden);
    const hu = head.hidden;
    try insert(a, compute, "project.weight", head.project_w, &.{ h, @intCast(head.width) });
    try insert(a, compute, "project.bias", head.project_b, &.{h});
    try insert(a, compute, "project_norm.weight", head.project_ln_w, &.{h});
    try insert(a, compute, "project_norm.bias", head.project_ln_b, &.{h});
    var buf: [64]u8 = undefined;
    for (head.blocks, 0..) |block, i| {
        try control.check();
        for ([_][]const u8{ "q", "k", "v" }, 0..) |name, part| {
            try insert(a, compute, try std.fmt.bufPrint(&buf, "blocks.{d}.{s}.weight", .{ i, name }), block.in_w[part * hu * hu ..][0 .. hu * hu], &.{ h, h });
            try insert(a, compute, try std.fmt.bufPrint(&buf, "blocks.{d}.{s}.bias", .{ i, name }), block.in_b[part * hu ..][0..hu], &.{h});
        }
        try insert(a, compute, try std.fmt.bufPrint(&buf, "blocks.{d}.out.weight", .{i}), block.out_w, &.{ h, h });
        try insert(a, compute, try std.fmt.bufPrint(&buf, "blocks.{d}.out.bias", .{i}), block.out_b, &.{h});
        try insert(a, compute, try std.fmt.bufPrint(&buf, "blocks.{d}.l1.weight", .{i}), block.l1_w, &.{ 4 * h, h });
        try insert(a, compute, try std.fmt.bufPrint(&buf, "blocks.{d}.l1.bias", .{i}), block.l1_b, &.{4 * h});
        try insert(a, compute, try std.fmt.bufPrint(&buf, "blocks.{d}.l2.weight", .{i}), block.l2_w, &.{ h, 4 * h });
        try insert(a, compute, try std.fmt.bufPrint(&buf, "blocks.{d}.l2.bias", .{i}), block.l2_b, &.{h});
        try insert(a, compute, try std.fmt.bufPrint(&buf, "blocks.{d}.n1.weight", .{i}), block.n1_w, &.{h});
        try insert(a, compute, try std.fmt.bufPrint(&buf, "blocks.{d}.n1.bias", .{i}), block.n1_b, &.{h});
        try insert(a, compute, try std.fmt.bufPrint(&buf, "blocks.{d}.n2.weight", .{i}), block.n2_w, &.{h});
        try insert(a, compute, try std.fmt.bufPrint(&buf, "blocks.{d}.n2.bias", .{i}), block.n2_b, &.{h});
    }
    try insert(a, compute, "norm.weight", head.norm_w, &.{h});
    try insert(a, compute, "norm.bias", head.norm_b, &.{h});
    try insert(a, compute, "out.weight", head.out_w, &.{ 2, h });
    try insert(a, compute, "out.bias", head.out_b, &.{2});
}

fn norm(cb: *const CB, prefix: []const u8, input: CT, width: usize) !CT {
    var buf: [64]u8 = undefined;
    const weight = try cb.getWeight(try std.fmt.bufPrint(&buf, "{s}.weight", .{prefix}));
    defer cb.free(weight);
    const bias = try cb.getWeight(try std.fmt.bufPrint(&buf, "{s}.bias", .{prefix}));
    defer cb.free(bias);
    return cb.layerNorm(input, weight, bias, width, span.layer_norm_eps);
}

fn linear(cb: *const CB, prefix: []const u8, input: CT, rows: usize, in_dim: usize, out_dim: usize) !CT {
    var buf: [64]u8 = undefined;
    const weight = try cb.getWeight(try std.fmt.bufPrint(&buf, "{s}.weight", .{prefix}));
    defer cb.free(weight);
    const bias = try cb.getWeight(try std.fmt.bufPrint(&buf, "{s}.bias", .{prefix}));
    defer cb.free(bias);
    return cb.linear(input, weight, bias, rows, in_dim, out_dim);
}

/// `x += f(x)` with ownership: frees the old stream and the delta.
fn residual(cb: *const CB, x: *CT, delta: CT) !void {
    defer cb.free(delta);
    const sum = try cb.add(x.*, delta);
    cb.free(x.*);
    x.* = sum;
}

fn shape2(rows: usize, cols: usize) ![2]i32 {
    return .{ std.math.cast(i32, rows) orelse return error.InvalidMemorillaStates, std.math.cast(i32, cols) orelse return error.InvalidMemorillaStates };
}

fn forwardDevice(owner: *Owner, a: A, features: []const f32, counts: []const usize) !span.Logits {
    const g = owner.geometry;
    var rows: usize = 0;
    for (counts) |count| rows = try std.math.add(usize, rows, count);
    if (rows == 0 or features.len != try std.math.mul(usize, rows, g.width)) return error.InvalidMemorillaStates;
    if (try scratchBytes(g, rows) > max_scratch_bytes) return error.MemorillaCollectionBudgetExceeded;
    const h = g.hidden;
    const n = rows + 1;

    // Host addend, added to [0; LN(project(features))] on the device: row 0 is
    // the no-answer row plus position 0; token rows get their document's
    // segment row plus their position row (both clamped to the tables),
    // summed in the host head's order (`x += segment + position`).
    const addend = try a.alloc(f32, n * h);
    defer a.free(addend);
    for (addend[0..h], owner.null_row, owner.position[0..h]) |*o, z, p| o.* = z + p;
    {
        var row: usize = 1;
        for (counts, 0..) |count, document| {
            const segment = owner.segment[@min(document, g.max_documents - 1) * h ..][0..h];
            for (0..count) |_| {
                const position = owner.position[@min(row, g.max_positions) * h ..][0..h];
                for (addend[row * h ..][0..h], segment, position) |*o, s, p| o.* = s + p;
                row += 1;
            }
        }
    }
    const zero = try a.alloc(f32, h);
    defer a.free(zero);
    @memset(zero, 0);

    const backend = owner.compute.computeBackend();
    const cb = &backend;
    if (cb.vtable.concatRows2D == null or cb.vtable.geluExact == null) return error.UnsupportedMemorillaBackend;

    var x = blk: {
        const feature_shape = try shape2(rows, g.width);
        const input = try cb.fromFloat32Shape(features, &feature_shape);
        defer cb.free(input);
        const projected = try linear(cb, "project", input, rows, g.width, h);
        defer cb.free(projected);
        const normed = try norm(cb, "project_norm", projected, h);
        defer cb.free(normed);
        const zero_shape = try shape2(1, h);
        const head_row = try cb.fromFloat32Shape(zero, &zero_shape);
        defer cb.free(head_row);
        const stacked = try cb.concatRows2D(a, head_row, normed, 1, rows, h);
        defer cb.free(stacked);
        const addend_shape = try shape2(n, h);
        const offsets = try cb.fromFloat32Shape(addend, &addend_shape);
        defer cb.free(offsets);
        break :blk try cb.add(stacked, offsets);
    };
    defer cb.free(x);

    var buf: [64]u8 = undefined;
    for (0..g.layers) |i| {
        // x += SelfAttention(LN1 x)
        {
            const normed = try norm(cb, try std.fmt.bufPrint(&buf, "blocks.{d}.n1", .{i}), x, h);
            defer cb.free(normed);
            const q = try linear(cb, try std.fmt.bufPrint(&buf, "blocks.{d}.q", .{i}), normed, n, h, h);
            defer cb.free(q);
            const k = try linear(cb, try std.fmt.bufPrint(&buf, "blocks.{d}.k", .{i}), normed, n, h, h);
            defer cb.free(k);
            const v = try linear(cb, try std.fmt.bufPrint(&buf, "blocks.{d}.v", .{i}), normed, n, h, h);
            defer cb.free(v);
            const attended = try owner.compute.memorillaFullAttention(q, k, v, n, g.heads);
            defer cb.free(attended);
            try residual(cb, &x, try linear(cb, try std.fmt.bufPrint(&buf, "blocks.{d}.out", .{i}), attended, n, h, h));
        }
        // x += W2 gelu(W1 LN2 x)
        {
            const normed = try norm(cb, try std.fmt.bufPrint(&buf, "blocks.{d}.n2", .{i}), x, h);
            defer cb.free(normed);
            const hidden = try linear(cb, try std.fmt.bufPrint(&buf, "blocks.{d}.l1", .{i}), normed, n, h, 4 * h);
            defer cb.free(hidden);
            const activated = (try cb.geluExact(hidden)) orelse return error.UnsupportedMemorillaBackend;
            defer cb.free(activated);
            try residual(cb, &x, try linear(cb, try std.fmt.bufPrint(&buf, "blocks.{d}.l2", .{i}), activated, n, 4 * h, h));
        }
    }
    const values = blk: {
        const normed = try norm(cb, "norm", x, h);
        defer cb.free(normed);
        const logits = try linear(cb, "out", normed, n, h, 2);
        defer cb.free(logits);
        break :blk try cb.toFloat32(logits, a);
    };
    defer a.free(values);
    if (values.len != n * 2) return error.InvalidMemorillaStates;
    const start = try a.alloc(f32, n);
    errdefer a.free(start);
    const end = try a.alloc(f32, n);
    errdefer a.free(end);
    for (0..n) |row| {
        start[row] = values[row * 2];
        end[row] = values[row * 2 + 1];
        if (!std.math.isFinite(start[row]) or !std.math.isFinite(end[row])) return error.NonFiniteMemorillaFeature;
    }
    return .{ .allocator = a, .start = start, .end = end };
}

test "Memorilla span head device geometry counts the resident tensors" {
    const g = Geometry{ .width = 5120, .hidden = 1024, .layers = 2, .heads = 8, .max_positions = 2048, .max_documents = 4 };
    const h: usize = 1024;
    // project (+ norm), 2 x (q, k, v, out, l1, l2, n1, n2), norm, out.
    const expected = @sizeOf(f32) * (h * 5120 + 3 * h + 2 * (12 * h * h + 13 * h) + 4 * h + 2);
    try std.testing.expectEqual(expected, g.deviceBytes());
    const protocol = span.Protocol{ .window_tokens = 32, .left_fraction = 0.65, .max_span_tokens = 30, .gate_threshold = 0, .gate_policy = .always, .width = 5120, .hidden = 1024, .layers = 2, .heads = 8, .max_positions = 2048, .max_documents = 4 };
    try std.testing.expect(span.Head.weightBytes(protocol) <= max_weight_bytes);
    try std.testing.expect(g.deviceBytes() < span.Head.weightBytes(protocol));
    // Four ~1,000-token documents fit the per-request scratch.
    try std.testing.expect(try scratchBytes(g, 4000) <= max_scratch_bytes);
}
