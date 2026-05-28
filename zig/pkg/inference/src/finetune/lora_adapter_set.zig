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

// LoRA adapter manager for ModernBERT (22 layers).
//
// Wraps the lora.zig primitives and manages the full set of adapters for the
// encoder during finetuning.  Each LoRALayer owns its weight and gradient
// buffers; LoRAAdapterSet owns a flat slice of LoRALayer values.

const std = @import("std");
const lora = @import("lora.zig");

// ----------------------------------------------------------------------------
// LoRAConfig
// ----------------------------------------------------------------------------

pub const LoRAConfig = struct {
    rank: u32 = 16,
    alpha: f32 = 32.0,
    /// Which linear projections in each layer to adapt.
    /// Valid names: "query_proj", "value_proj", "key_proj", "out_proj", "wi", "wo"
    target_modules: []const []const u8 = &.{ "query_proj", "value_proj", "key_proj", "out_proj" },
    num_layers: u32 = 22,
    /// Not used in the CPU path; reserved for future GPU dropout support.
    dropout: f32 = 0.0,
    /// LoRA+: multiply lora_B learning rate by this factor (16.0 recommended).
    /// 1.0 = disabled (standard LoRA).
    lora_plus_ratio: f32 = 1.0,
    /// Scaling mode: standard (alpha/rank) or rs_lora (alpha/sqrt(rank)).
    scaling: lora.ScalingMode = .standard,
    /// Enable DoRA (per-column magnitude decomposition). When true, the adapter
    /// set also allocates a magnitude vector per layer.
    use_dora: bool = false,
};

/// Convenience target-module preset: attention-only.
pub const attn_only_modules: []const []const u8 = &.{ "query_proj", "value_proj", "key_proj", "out_proj" };

/// Convenience target-module preset: MLP/feed-forward only.
pub const mlp_only_modules: []const []const u8 = &.{ "wi", "wo", "gate_proj", "up_proj", "down_proj" };

/// Convenience target-module preset: attention + feed-forward ("all linear").
/// Matches the Unsloth/Axolotl default for LLaMA/Gemma-family decoders.
pub const all_linear_modules: []const []const u8 = &.{ "query_proj", "key_proj", "value_proj", "out_proj", "wi", "wo", "gate_proj", "up_proj", "down_proj" };

/// Convenience target-module preset for common MoE expert parameters.
pub const moe_expert_modules: []const []const u8 = &.{ "experts", "gate_up_proj", "down_proj", "w1", "w2", "w3" };

// ----------------------------------------------------------------------------
// LoRALayer
// ----------------------------------------------------------------------------

pub const LoRALayer = struct {
    allocator: std.mem.Allocator,
    layer_idx: u32,
    /// Owned copy of the module name string.
    module_name: []const u8,
    in_features: usize,
    out_features: usize,

    /// A: [rank, in_features] — Kaiming uniform init
    A: []f32,
    /// B: [out_features, rank] — zero init
    B: []f32,

    /// Gradient accumulators (same shapes as A and B).
    grad_A: []f32,
    grad_B: []f32,

    /// Optional DoRA magnitude vector: [out_features]. null unless use_dora.
    magnitude: ?[]f32 = null,
    /// Gradient for the DoRA magnitude vector.
    grad_magnitude: ?[]f32 = null,

    pub fn init(
        allocator: std.mem.Allocator,
        layer_idx: u32,
        module_name: []const u8,
        in_features: usize,
        out_features: usize,
        rank: u32,
    ) !LoRALayer {
        return initWithDoRA(allocator, layer_idx, module_name, in_features, out_features, rank, false);
    }

    pub fn initWithDoRA(
        allocator: std.mem.Allocator,
        layer_idx: u32,
        module_name: []const u8,
        in_features: usize,
        out_features: usize,
        rank: u32,
        use_dora: bool,
    ) !LoRALayer {
        const r: usize = @intCast(rank);

        const A = try allocator.alloc(f32, r * in_features);
        errdefer allocator.free(A);
        const B = try allocator.alloc(f32, out_features * r);
        errdefer allocator.free(B);
        const grad_A = try allocator.alloc(f32, r * in_features);
        errdefer allocator.free(grad_A);
        const grad_B = try allocator.alloc(f32, out_features * r);
        errdefer allocator.free(grad_B);

        const owned_name = try allocator.dupe(u8, module_name);
        errdefer allocator.free(owned_name);

        var magnitude: ?[]f32 = null;
        var grad_magnitude: ?[]f32 = null;
        if (use_dora) {
            const m = try allocator.alloc(f32, out_features);
            errdefer allocator.free(m);
            const gm = try allocator.alloc(f32, out_features);
            errdefer allocator.free(gm);
            // Initial magnitude = 1.0 so the first forward equals the base weight
            // (B is zero-init, so V = base and ||V||_col is the base column norms;
            // callers that want the "reduces to W on step 0" property must instead
            // initialize m to the base column norms via `initMagnitudeFromBase`).
            for (m) |*v| v.* = 1.0;
            @memset(gm, 0);
            magnitude = m;
            grad_magnitude = gm;
        }

        // Kaiming uniform init for A: stddev = sqrt(2 / in_features).
        // Use a deterministic sin/cos pattern consistent with BoundaryHead.
        const stddev = @sqrt(2.0 / @as(f32, @floatFromInt(in_features)));
        for (0..r) |row| {
            for (0..in_features) |col| {
                const angle = @as(f32, @floatFromInt((row + 1) * (col + 5)));
                A[row * in_features + col] =
                    (@sin(angle * 0.11) + @cos(angle * 0.07)) * stddev;
            }
        }

        // B is zero-initialized.
        @memset(B, 0);
        @memset(grad_A, 0);
        @memset(grad_B, 0);

        return .{
            .allocator = allocator,
            .layer_idx = layer_idx,
            .module_name = owned_name,
            .in_features = in_features,
            .out_features = out_features,
            .A = A,
            .B = B,
            .grad_A = grad_A,
            .grad_B = grad_B,
            .magnitude = magnitude,
            .grad_magnitude = grad_magnitude,
        };
    }

    pub fn deinit(self: *LoRALayer) void {
        self.allocator.free(self.module_name);
        self.allocator.free(self.A);
        self.allocator.free(self.B);
        self.allocator.free(self.grad_A);
        self.allocator.free(self.grad_B);
        if (self.magnitude) |m| self.allocator.free(m);
        if (self.grad_magnitude) |gm| self.allocator.free(gm);
        self.* = undefined;
    }

    pub fn zeroGrads(self: *LoRALayer) void {
        @memset(self.grad_A, 0);
        @memset(self.grad_B, 0);
        if (self.grad_magnitude) |gm| @memset(gm, 0);
    }

    /// Returns a Matrix view of A: rank rows, in_features cols.
    pub fn asMatrixA(self: *const LoRALayer) lora.Matrix {
        const rank = self.A.len / self.in_features;
        return .{ .rows = rank, .cols = self.in_features, .data = self.A };
    }

    /// Returns a Matrix view of B: out_features rows, rank cols.
    pub fn asMatrixB(self: *const LoRALayer) lora.Matrix {
        const rank = self.B.len / self.out_features;
        return .{ .rows = self.out_features, .cols = rank, .data = self.B };
    }

    /// Initialize DoRA magnitude from a base weight stored as
    /// [out_features, in_features] row-major. This preserves the base function
    /// at step 0 because B is zero and m_j = ||W[:, j]|| in DoRA's column view.
    pub fn initMagnitudeFromBaseRowMajor(self: *LoRALayer, base_weight: []const f32) void {
        const magnitude = self.magnitude orelse return;
        std.debug.assert(base_weight.len == self.out_features * self.in_features);
        @memset(magnitude, 0);
        for (0..self.out_features) |out_idx| {
            const row = base_weight[out_idx * self.in_features .. (out_idx + 1) * self.in_features];
            var sum: f32 = 0;
            for (row) |v| sum += v * v;
            magnitude[out_idx] = @sqrt(sum + 1e-12);
        }
    }
};

// ----------------------------------------------------------------------------
// LoRAAdapterSet
// ----------------------------------------------------------------------------

pub const LoRAAdapterSet = struct {
    allocator: std.mem.Allocator,
    config: LoRAConfig,
    /// Flat slice of LoRALayer values.
    /// Length = num_layers * len(target_modules).
    layers: []LoRALayer,
    /// Adapter gate. When false, `applyAdapter` is a no-op so the base model
    /// runs as a reference policy. Enables the DPO/GRPO "disable adapter to
    /// get reference logprobs" trick without holding a second model in memory.
    enabled: bool = true,

    pub fn init(
        allocator: std.mem.Allocator,
        config: LoRAConfig,
        hidden_size: usize,
        intermediate_size: usize,
    ) !LoRAAdapterSet {
        const num_layers: usize = @intCast(config.num_layers);
        const num_modules = config.target_modules.len;
        const total = num_layers * num_modules;

        const layers = try allocator.alloc(LoRALayer, total);
        var initialized: usize = 0;
        errdefer {
            for (layers[0..initialized]) |*l| l.deinit();
            allocator.free(layers);
        }

        for (0..num_layers) |li| {
            for (config.target_modules, 0..) |mod_name, mi| {
                const in_features, const out_features = dimensionsForModule(
                    mod_name,
                    hidden_size,
                    intermediate_size,
                );
                layers[li * num_modules + mi] = try LoRALayer.initWithDoRA(
                    allocator,
                    @intCast(li),
                    mod_name,
                    in_features,
                    out_features,
                    config.rank,
                    config.use_dora,
                );
                initialized += 1;
            }
        }

        return .{
            .allocator = allocator,
            .config = config,
            .layers = layers,
            .enabled = true,
        };
    }

    /// Disable the LoRA delta so forward passes use the frozen base. Used to
    /// compute "reference policy" logprobs for DPO/GRPO without allocating a
    /// second model.
    pub fn disable(self: *LoRAAdapterSet) void {
        self.enabled = false;
    }

    /// Re-enable LoRA delta after a disabled forward pass.
    pub fn enable(self: *LoRAAdapterSet) void {
        self.enabled = true;
    }

    pub fn deinit(self: *LoRAAdapterSet) void {
        for (self.layers) |*l| l.deinit();
        self.allocator.free(self.layers);
        self.* = undefined;
    }

    /// Return a pointer to the LoRALayer for the given layer index and module name,
    /// or null if no such layer exists.
    pub fn get(self: *LoRAAdapterSet, layer_idx: u32, module_name: []const u8) ?*LoRALayer {
        for (self.layers) |*l| {
            if (l.layer_idx == layer_idx and std.mem.eql(u8, l.module_name, module_name)) {
                return l;
            }
        }
        return null;
    }

    /// Zero all gradient accumulators across every layer.
    pub fn zeroGrads(self: *LoRAAdapterSet) void {
        for (self.layers) |*l| l.zeroGrads();
    }

    /// Apply the LoRA delta for a single (layer_idx, module_name) pair to a
    /// flat hidden-state slice using lora.applyInPlace.
    ///
    /// hidden: [seq_len * in_features] — modified in-place.
    /// Does nothing if the adapter is not found or if the set is disabled
    /// (reference-policy mode).
    pub fn applyAdapter(
        self: *const LoRAAdapterSet,
        layer_idx: u32,
        module_name: []const u8,
        hidden: []f32,
    ) void {
        if (!self.enabled) return;
        const mutable: *LoRAAdapterSet = @constCast(self);
        const layer = mutable.get(layer_idx, module_name) orelse return;
        applyLayerDelta(layer, hidden, self.config.alpha, self.config.scaling);
    }

    // TODO(lora-io): save/load were written against Zig 0.15 file APIs
    // (file.writer(), file.reader(), std.fs.cwd().makeOpenPath) that the
    // 0.16 Io refactor removed. The canonical checkpoint path is
    // safetensors_checkpoint.zig; this binary format is unused outside its
    // own unit test. Disabled pending rewrite against std.Io.File.
};

fn applyLayerDelta(layer: *const LoRALayer, hidden: []f32, alpha: f32, scaling: lora.ScalingMode) void {
    const rank = layer.A.len / layer.in_features;
    if (rank == 0) return;
    const scale = lora.effectiveScaleMode(alpha, rank, scaling);
    if (scale == 0) return;
    const rows = hidden.len / layer.in_features;
    if (rows == 0) return;

    var stack = std.heap.stackFallback(4096, std.heap.page_allocator);
    const alloc = stack.get();
    const tmp = alloc.alloc(f32, rank) catch return;
    defer alloc.free(tmp);

    for (0..rows) |row_idx| {
        const row = hidden[row_idx * layer.in_features .. (row_idx + 1) * layer.in_features];
        @memset(tmp, 0);
        for (0..rank) |r| {
            const a_row = layer.A[r * layer.in_features .. (r + 1) * layer.in_features];
            var acc: f32 = 0;
            for (0..layer.in_features) |i| acc += row[i] * a_row[i];
            tmp[r] = acc;
        }
        for (0..layer.out_features) |o| {
            var delta: f32 = 0;
            for (0..rank) |r| delta += tmp[r] * layer.B[o * rank + r];
            if (o < row.len) row[o] += scale * delta;
        }
    }
}

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------

/// Return (in_features, out_features) for a named projection module.
fn dimensionsForModule(
    module_name: []const u8,
    hidden_size: usize,
    intermediate_size: usize,
) struct { usize, usize } {
    if (std.mem.eql(u8, module_name, "query_proj") or
        std.mem.eql(u8, module_name, "key_proj") or
        std.mem.eql(u8, module_name, "value_proj") or
        std.mem.eql(u8, module_name, "out_proj"))
    {
        return .{ hidden_size, hidden_size };
    }
    if (std.mem.eql(u8, module_name, "wi")) {
        return .{ hidden_size, intermediate_size };
    }
    if (std.mem.eql(u8, module_name, "wo")) {
        return .{ intermediate_size, hidden_size };
    }
    if (std.mem.eql(u8, module_name, "gate_proj") or std.mem.eql(u8, module_name, "up_proj")) {
        return .{ hidden_size, intermediate_size };
    }
    if (std.mem.eql(u8, module_name, "down_proj")) {
        return .{ intermediate_size, hidden_size };
    }
    // Unknown module: fall back to square hidden.
    return .{ hidden_size, hidden_size };
}

/// Write one tensor to a writer in the checkpoint binary format:
///   [name_len: u32 LE][name bytes][elem_count: u32 LE][f32 data as u32 LE...]
fn writeTensor(w: anytype, name: []const u8, data: []const f32) !void {
    try w.writeInt(u32, @intCast(name.len), .little);
    try w.writeAll(name);
    try w.writeInt(u32, @intCast(data.len), .little);
    for (data) |val| {
        try w.writeInt(u32, @bitCast(val), .little);
    }
}

/// Read one tensor from a reader and validate name + length, then overwrite dest.
fn readTensor(r: anytype, expected_name: []const u8, dest: []f32) !void {
    const name_len = try r.readInt(u32, .little);
    if (name_len > 256) return error.InvalidCheckpoint;
    var name_buf: [256]u8 = undefined;
    try r.readNoEof(name_buf[0..name_len]);
    const name = name_buf[0..name_len];
    if (!std.mem.eql(u8, name, expected_name)) return error.CheckpointNameMismatch;

    const elem_count = try r.readInt(u32, .little);
    if (elem_count != dest.len) return error.CheckpointSizeMismatch;
    for (dest) |*val| {
        val.* = @bitCast(try r.readInt(u32, .little));
    }
}

// ----------------------------------------------------------------------------
// Tests
// ----------------------------------------------------------------------------

test "LoRAAdapterSet init and deinit" {
    const allocator = std.testing.allocator;

    const config = LoRAConfig{
        .rank = 4,
        .alpha = 8.0,
        .target_modules = &.{ "query_proj", "value_proj" },
        .num_layers = 2,
    };

    var adapter_set = try LoRAAdapterSet.init(allocator, config, 64, 256);
    defer adapter_set.deinit();

    // 2 layers × 2 modules = 4 LoRALayer entries.
    try std.testing.expectEqual(@as(usize, 4), adapter_set.layers.len);

    // Each A matrix: [rank, in_features] = [4, 64] = 256 elements.
    try std.testing.expectEqual(@as(usize, 4 * 64), adapter_set.layers[0].A.len);
    // Each B matrix: [out_features, rank] = [64, 4] = 256 elements.
    try std.testing.expectEqual(@as(usize, 64 * 4), adapter_set.layers[0].B.len);

    // B must be zero-initialized.
    for (adapter_set.layers[0].B) |v| {
        try std.testing.expectEqual(@as(f32, 0.0), v);
    }

    // A must be non-zero (Kaiming init).
    var any_nonzero = false;
    for (adapter_set.layers[0].A) |v| {
        if (v != 0.0) {
            any_nonzero = true;
            break;
        }
    }
    try std.testing.expect(any_nonzero);
}

test "LoRALayer grad zero" {
    const allocator = std.testing.allocator;

    var layer = try LoRALayer.init(allocator, 0, "query_proj", 32, 32, 4);
    defer layer.deinit();

    // Dirty the grad buffers.
    for (layer.grad_A) |*v| v.* = 1.0;
    for (layer.grad_B) |*v| v.* = 2.0;

    layer.zeroGrads();

    for (layer.grad_A) |v| try std.testing.expectEqual(@as(f32, 0.0), v);
    for (layer.grad_B) |v| try std.testing.expectEqual(@as(f32, 0.0), v);
}

test "LoRAAdapterSet get" {
    const allocator = std.testing.allocator;

    const config = LoRAConfig{
        .rank = 2,
        .alpha = 4.0,
        .target_modules = &.{ "query_proj", "value_proj", "key_proj" },
        .num_layers = 3,
    };

    var adapter_set = try LoRAAdapterSet.init(allocator, config, 16, 64);
    defer adapter_set.deinit();

    // Should find a valid layer.
    const found = adapter_set.get(1, "value_proj");
    try std.testing.expect(found != null);
    try std.testing.expectEqual(@as(u32, 1), found.?.layer_idx);
    try std.testing.expectEqualStrings("value_proj", found.?.module_name);

    // Should return null for a non-existent module name.
    const missing = adapter_set.get(0, "out_proj");
    try std.testing.expectEqual(@as(?*LoRALayer, null), missing);

    // Should return null for an out-of-range layer index.
    const out_of_range = adapter_set.get(99, "query_proj");
    try std.testing.expectEqual(@as(?*LoRALayer, null), out_of_range);
}

test "LoRAAdapterSet applyAdapter uses graph LoRA tensor orientation" {
    const allocator = std.testing.allocator;
    const config = LoRAConfig{
        .rank = 1,
        .alpha = 2.0,
        .target_modules = &.{"query_proj"},
        .num_layers = 1,
    };
    var adapter_set = try LoRAAdapterSet.init(allocator, config, 2, 4);
    defer adapter_set.deinit();
    var layer = adapter_set.get(0, "query_proj").?;
    layer.A[0] = 1.0;
    layer.A[1] = 2.0;
    layer.B[0] = 3.0;
    layer.B[1] = 4.0;

    var hidden = [_]f32{ 5.0, 7.0 };
    adapter_set.applyAdapter(0, "query_proj", &hidden);

    // tmp = 5*1 + 7*2 = 19; scale = 2.0 / 1.
    try std.testing.expectApproxEqAbs(@as(f32, 5.0 + 2.0 * 19.0 * 3.0), hidden[0], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 7.0 + 2.0 * 19.0 * 4.0), hidden[1], 1e-5);
}

test "DoRA magnitude initializes from row-major base weight rows" {
    const allocator = std.testing.allocator;
    var layer = try LoRALayer.initWithDoRA(allocator, 0, "query_proj", 2, 2, 1, true);
    defer layer.deinit();
    const base = [_]f32{
        3, 4,
        5, 12,
    };
    layer.initMagnitudeFromBaseRowMajor(&base);
    try std.testing.expectApproxEqAbs(@as(f32, 5), layer.magnitude.?[0], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 13), layer.magnitude.?[1], 1e-5);
}

// The save/load round-trip test was removed alongside the save/load methods.
// See TODO(lora-io) at the LoRAAdapterSet end.
